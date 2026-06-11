# 串口传输 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Foundation 层（`TransportCore` + `FrameAssembler` + `ReceiveQueue`）之上实现串口传输（单个 `SerialImpl`），基于 `asio::serial_port` 的异步单线程 I/O 模型，以 pty 回环集成测试验证。

**Architecture:** `SerialImpl : public ITransport` **组合** `TransportCore core_`（接收交付+编解码，5 个接收方法转发）+ `FrameAssembler assembler_`（接收侧分帧，按 `config.framer` 建 `LengthFieldFramer`，无则透传）。自有 `io_context` + 1 后台 io 线程 + strand。`Open()` 打开串口并用 `set_option` 配置参数；`async_read_some` 循环切帧 → `core_.DeliverFrame`，`Send` 经 strand `async_write`。**无连接/无重连**：read 出错即 `OnDisconnect` 终态。结构上等于「自有 io 线程的 `TcpConnectionImpl`，去掉 connect/重连」。

**Tech Stack:** C++17、Standalone Asio（`asio::serial_port`）、GoogleTest 1.14、POSIX `openpty`（测试，链接 `util`）。

**依据 spec：** `docs/superpowers/specs/2026-06-11-serial-transport-design.md`。

---

## 文件结构

```
include/transport/serial/
├── SerialConfig.hpp     # 串口配置（device/baud/data_bits/stop_bits/parity/framer）
└── SerialImpl.hpp       # 单类实现
src/serial/
└── SerialImpl.cpp
tests/serial/
├── serial_interfaces_test.cpp   # 配置默认值
└── serial_transport_test.cpp    # pty 回环：透传/分帧/codec/断连/非法配置
（修改：CMakeLists.txt）
```

---

## Task 1: `SerialConfig` 头 + 配置测试

**Files:**
- Create: `include/transport/serial/SerialConfig.hpp`
- Test: `tests/serial/serial_interfaces_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/serial/serial_interfaces_test.cpp`:

```cpp
#include "transport/serial/SerialConfig.hpp"

#include <gtest/gtest.h>

TEST(SerialConfig, Defaults) {
  transport::SerialConfig c;
  EXPECT_EQ(c.baud_rate, 115200u);
  EXPECT_EQ(static_cast<int>(c.data_bits), 8);
  EXPECT_EQ(static_cast<int>(c.stop_bits), 1);
  EXPECT_EQ(c.parity, 'N');
  EXPECT_FALSE(c.framer.has_value());
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 列表追加 `tests/serial/serial_interfaces_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 `transport/serial/SerialConfig.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/serial/SerialConfig.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// SerialConfig.hpp — 串口配置
// device 路径 + 波特率/数据位/停止位/校验位，以及可选的接收侧分帧配置(framer)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct SerialConfig {
  std::string device;              // 例如 "/dev/ttyS0"
  uint32_t    baud_rate  = 115200;
  uint8_t     data_bits  = 8;
  uint8_t     stop_bits  = 1;      // 1 或 2
  char        parity     = 'N';    // 'N'（无）/ 'E'（偶）/ 'O'（奇）
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
};

}  // namespace transport
```

- [ ] **Step 5: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R SerialConfig`
Expected: `SerialConfig.Defaults` PASS；全量套件保持绿色。

- [ ] **Step 6: 提交**

```bash
git add include/transport/serial/SerialConfig.hpp tests/serial/serial_interfaces_test.cpp CMakeLists.txt
git commit -m "feat: 串口配置结构 SerialConfig"
```

---

## Task 2: `SerialImpl`（asio::serial_port + pty 回环测试）

**Files:**
- Create: `include/transport/serial/SerialImpl.hpp`
- Create: `src/serial/SerialImpl.cpp`
- Test: `tests/serial/serial_transport_test.cpp`
- Modify: `CMakeLists.txt`（加库源 + 测试源 + 链接 `util`）

- [ ] **Step 1: 写失败测试**

`tests/serial/serial_transport_test.cpp`:

```cpp
#include "transport/serial/SerialImpl.hpp"

#include <pty.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/framing/LengthFieldFramer.hpp"

using transport::ICodec;
using transport::LengthFieldFramerConfig;
using transport::Result;
using transport::SerialConfig;
using transport::SerialImpl;

namespace {

// 主从伪终端：SerialImpl 打开从端(by name)，测试代码直接读写主端 fd。
struct Pty {
  int master = -1;
  std::string slave_name;
  Pty() {
    int slave_fd = -1;
    char name[256] = {0};
    if (openpty(&master, &slave_fd, name, nullptr, nullptr) == 0) {
      slave_name = name;
      ::close(slave_fd);  // 让 SerialImpl 成为从端唯一打开者
    }
  }
  ~Pty() { if (master >= 0) ::close(master); }
  bool ok() const { return master >= 0 && !slave_name.empty(); }
  void WriteMaster(const std::vector<uint8_t>& d) {
    ASSERT_GE(::write(master, d.data(), d.size()), 0);
  }
  std::vector<uint8_t> ReadMaster(size_t n) {
    std::vector<uint8_t> buf(n);
    ssize_t r = ::read(master, buf.data(), n);
    buf.resize(r > 0 ? static_cast<size_t>(r) : 0);
    return buf;
  }
};

SerialConfig Cfg(const std::string& dev) {
  SerialConfig c;
  c.device = dev;
  c.baud_rate = 115200;
  return c;
}

LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.max_frame_size = 1024;
  return c;
}

std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
};

}  // namespace

TEST(SerialTransport, PassthroughReceive) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  pty.WriteMaster({10, 20, 30});
  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, pty.slave_name);
  s->Close();
}

TEST(SerialTransport, FramerAssemblesAcrossReads) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  SerialConfig cfg = Cfg(pty.slave_name);
  cfg.framer = BeConfig();
  auto s = std::make_shared<SerialImpl>(cfg);
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  auto frame = BuildFrame(5, 0xAB);  // 13 字节
  pty.WriteMaster(std::vector<uint8_t>(frame.begin(), frame.begin() + 6));
  pty.WriteMaster(std::vector<uint8_t>(frame.begin() + 6, frame.end()));

  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, frame);
  s->Close();
}

TEST(SerialTransport, SendWritesToPeer) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  ASSERT_TRUE(static_cast<bool>(s->Send({1, 2, 3, 4})));
  auto got = pty.ReadMaster(4);
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3, 4}));
  s->Close();
}

TEST(SerialTransport, CodecBothDirections) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  s->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  // 发：{1,2,3} 经 Encode(+1) → 主端读到 {2,3,4}
  ASSERT_TRUE(static_cast<bool>(s->Send({1, 2, 3})));
  auto got = pty.ReadMaster(3);
  EXPECT_EQ(got, (std::vector<uint8_t>{2, 3, 4}));

  // 收：主端写 {2,3,4} 经 Decode(-1) → payload {1,2,3}
  pty.WriteMaster({2, 3, 4});
  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  s->Close();
}

TEST(SerialTransport, PeerCloseTriggersDisconnect) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  std::string reason;
  s->OnDisconnect([&](const std::string& r) { reason = r; });
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  ::close(pty.master);  // 关主端 → 从端 read 出错
  pty.master = -1;
  auto r = s->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  for (int i = 0; i < 100 && reason.empty(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);
  s->Close();
}

TEST(SerialTransport, InvalidParityRejected) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  SerialConfig cfg = Cfg(pty.slave_name);
  cfg.parity = 'X';
  auto s = std::make_shared<SerialImpl>(cfg);
  auto st = s->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  s->Close();
}
```

- [ ] **Step 2: 把库源/测试源加入 CMake，并给测试链接 util**

在 `add_library(transport STATIC ...)` 追加 `src/serial/SerialImpl.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/serial/serial_transport_test.cpp`；
在 `CMakeLists.txt` 的 `if(TRANSPORT_BUILD_TESTS)` 块内、`gtest_discover_tests(transport_tests)` 之前追加（`openpty` 在 libutil）：

```cmake
  target_link_libraries(transport_tests PRIVATE util)
```

- [ ] **Step 3: 运行，确认失败**

Run: `cmake -S . -B build && cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 `SerialImpl.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/serial/SerialImpl.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// SerialImpl.hpp — 串口传输实现（ITransport）
// 组合 TransportCore + FrameAssembler；基于 asio::serial_port，自有 io_context +
// 1 后台 io 线程。流式（接收侧分帧同 TCP），无连接/无重连。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/framing/FrameAssembler.hpp"
#include "transport/serial/SerialConfig.hpp"

namespace transport {

class SerialImpl : public ITransport,
                   public std::enable_shared_from_this<SerialImpl> {
 public:
  explicit SerialImpl(SerialConfig config);
  ~SerialImpl() override;

  Status Open() override;   // 打开串口 + 配置参数 + 启动接收循环
  void Close() override;    // 关 port + core_.Close() + 停 io 线程
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;

  // 接收侧：一行转发给 core_
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

 private:
  void StartRead();
  void DoWrite();
  void HandleDisconnect(const std::string& reason);

  SerialConfig config_;
  TransportCore core_;
  FrameAssembler assembler_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::serial_port port_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> disconnected_{false};
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/serial/SerialImpl.cpp`:

```cpp
#include "transport/serial/SerialImpl.hpp"

#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

// SerialImpl.cpp — 串口传输实现（见 SerialImpl.hpp）。
// 并发：自有 io_context + 1 io 线程；读写经 strand_ 串行化；handler 用
// shared_from_this 保活。流式 → assembler_ 切帧后经 core_.DeliverFrame 交付。
// 无重连：read 出错即 HandleDisconnect 终态（disconnected_ 闩一次）。

namespace transport {

SerialImpl::SerialImpl(SerialConfig config)
    : config_(std::move(config)),
      assembler_(config_.framer
                     ? std::make_shared<LengthFieldFramer>(*config_.framer)
                     : nullptr),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      port_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

SerialImpl::~SerialImpl() { Close(); }

bool SerialImpl::IsOpen() const { return open_.load(); }

Status SerialImpl::Open() {
  if (config_.framer) {  // framer 配置校验
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }

  asio::error_code ec;
  port_.open(config_.device, ec);
  if (ec) return Status::Fail("config: open " + config_.device + ": " + ec.message());

  auto fail = [&](const std::string& msg) {
    asio::error_code ig;
    port_.close(ig);
    return Status::Fail(msg);
  };

  using sb = asio::serial_port_base;
  port_.set_option(sb::baud_rate(config_.baud_rate), ec);
  if (ec) return fail("config: baud_rate: " + ec.message());
  port_.set_option(sb::character_size(config_.data_bits), ec);
  if (ec) return fail("config: data_bits: " + ec.message());

  sb::stop_bits::type sbits;
  if (config_.stop_bits == 1) sbits = sb::stop_bits::one;
  else if (config_.stop_bits == 2) sbits = sb::stop_bits::two;
  else return fail("config: stop_bits must be 1 or 2");
  port_.set_option(sb::stop_bits(sbits), ec);
  if (ec) return fail("config: stop_bits: " + ec.message());

  sb::parity::type par;
  switch (config_.parity) {
    case 'N': par = sb::parity::none; break;
    case 'E': par = sb::parity::even; break;
    case 'O': par = sb::parity::odd; break;
    default: return fail("config: parity must be N/E/O");
  }
  port_.set_option(sb::parity(par), ec);
  if (ec) return fail("config: parity: " + ec.message());

  // 流控固定 none；best-effort（个别环境/pty 可能不支持，不致命）
  asio::error_code fc_ec;
  port_.set_option(sb::flow_control(sb::flow_control::none), fc_ec);

  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartRead(); });
  return Status::Success(std::monostate{});
}

void SerialImpl::StartRead() {
  auto self = shared_from_this();
  port_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect(std::string("conn: ") + ec.message());
              return;
            }
            auto frames = assembler_.Feed(read_buf_.data(), n);
            if (!frames) {
              HandleDisconnect(frames.error);  // frame: 错误
              return;
            }
            for (auto& f : frames.value) {
              core_.DeliverFrame(std::move(f), config_.device, "");
            }
            StartRead();
          }));
}

Status SerialImpl::Send(const std::vector<uint8_t>& data) {
  if (!open_.load()) return Status::Fail("config: serial not open");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(enc.value));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
  return Status::Success(std::monostate{});
}

void SerialImpl::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      port_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect(std::string("conn: ") + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) {
              DoWrite();
            } else {
              writing_ = false;
            }
          }));
}

void SerialImpl::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;  // 每个打开周期一次
  open_.store(false);
  asio::error_code ig;
  port_.close(ig);
  core_.DeliverError(reason);
  core_.Close();
  core_.NotifyDisconnect(reason);
}

void SerialImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    port_.close(ig);
  });
  core_.Close();
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

- [ ] **Step 6: 运行，确认通过**

Run:
```bash
cmake -S . -B build && cmake --build build -j && ctest --test-dir build --output-on-failure -R SerialTransport
```
Expected: `SerialTransport.*`（6 个）全部 PASS；全量套件保持绿色。

> 实测兜底：若 pty 上某条 `set_option`（如 `baud_rate`）返回错误导致 `PassthroughReceive` 等 Open 失败，按 spec §3.1 注记把该项设置改为 best-effort（像 `flow_control` 那样忽略 `ec`），保持真实串口仍配置、pty 测试可跑。`InvalidParityRejected` 依赖 parity 的 switch 判定（与 set_option 无关），不受影响。若 pty 二进制传输出现字节被改写（行规程问题），确认 asio 已置 raw（asio serial_port open 默认清 ICANON/OPOST）；仍有问题则在测试 helper 里对 master fd 也 `cfmakeraw`。

- [ ] **Step 7: 提交**

```bash
git add include/transport/serial/SerialImpl.hpp src/serial/SerialImpl.cpp \
        tests/serial/serial_transport_test.cpp CMakeLists.txt
git commit -m "feat: SerialImpl 串口传输（asio::serial_port，组合 TransportCore，pty 回环测试）"
```

---

## Task 3: 串口收尾验证 + README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 干净全量构建 + 串口套件稳定性**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
for i in $(seq 1 10); do ctest --test-dir build -R 'Serial' --output-on-failure || break; done
```
Expected: 全部 PASS；串口套件 10 次稳定（pty 回环不 flaky）。

- [ ] **Step 2: 更新 README 状态**

把 `README.md` 「状态」小节的串口一行改为完成：

```markdown
- [x] Foundation：核心接口、分帧、接收交付（`TransportCore`）、`Result`/`Message`
- [x] TCP（client / server）
- [x] UDP（单播 / 组播 / 广播）
- [x] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置
```

并把「特点」里「已实现传输」一处由「TCP、UDP」补上「串口」（如有该句）。

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README 标记串口完成"
```

---

## 后续（不在本计划范围）

DDS（主 spec §7）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。
