# TCP 传输 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Foundation 层之上实现 TCP 客户端与服务端，基于 Standalone Asio 的异步单线程 I/O 模型，客户端支持指数退避自动重连，全部以真实回环 socket 集成测试验证。

**Architecture:** `TcpConnectionImpl`（继承 `TransportBase`，包装已连接 socket，async_read 循环 + strand 串行 async_write + `FrameAssembler` 分帧）是客户端与服务端 accepted 连接的共用实现。`TcpClientImpl` 继承它，加 connect/超时/指数退避重连，自有 io_context + 1 线程。`TcpServerImpl` 实现 `ITcpServer`，自有 io_context + 1 线程，acceptor 每 accept 造一个共享该 io_context 的 `TcpConnectionImpl`，支持广播/客户端管理。所有 transport 以 `std::shared_ptr` 持有（async 处理器用 `shared_from_this` 保活）。

**Tech Stack:** C++17、Standalone Asio（FetchContent，`ASIO_STANDALONE`，header-only）、GoogleTest 1.14、Google C++ 风格。

**实现约定（锁定）：**
1. 所有 transport 均经 `std::make_shared` 创建（测试亦然），以支持 `shared_from_this`。
2. `TcpConnectionImpl` 不拥有 io 线程——由其 socket 所属的 io_context 驱动（client / server 各自拥有线程）。
3. 退避基数/封顶作为 `TcpClientImpl` 构造可注入参数（默认 1s/30s；测试用小值避免 flaky）。

---

## 文件结构

```
include/transport/tcp/
├── TcpClientConfig.hpp      # 客户端配置（复述自主 spec §5.1）
├── TcpServerConfig.hpp      # 服务端配置
├── ITcpServer.hpp           # 服务端扩展接口
├── TcpConnectionImpl.hpp        # 已连接 socket 收发循环（client/accepted 共用）
├── TcpClientImpl.hpp   # connect + 指数退避重连
└── TcpServerImpl.hpp   # acceptor + 每连接 + 广播
src/tcp/
├── TcpConnectionImpl.cpp
├── TcpClientImpl.cpp
└── TcpServerImpl.cpp
tests/tcp/
├── asio_smoke_test.cpp
├── tcp_connection_test.cpp
├── tcp_client_test.cpp
└── tcp_server_test.cpp
```

每个任务负责其中一组文件，并把新源文件加入 `CMakeLists.txt` 对应目标。

---

## Task 1: CMake 集成 Standalone Asio + 冒烟测试

**Files:**
- Modify: `CMakeLists.txt`
- Test: `tests/tcp/asio_smoke_test.cpp`

- [ ] **Step 1: 写冒烟测试**

`tests/tcp/asio_smoke_test.cpp`:

```cpp
#include <asio/version.hpp>

#include <gtest/gtest.h>

TEST(Asio, VersionMacroPresent) {
  EXPECT_GT(ASIO_VERSION, 0);
}
```

- [ ] **Step 2: CMake 拉取 Asio 并链接到 transport 库**

在 `CMakeLists.txt` 中，`target_include_directories(transport PUBLIC ...)` 之后、`if(TRANSPORT_BUILD_TESTS)` 之前，插入顶层 Asio 集成（transport 库本身需要 Asio，故放在 tests 块外）：

```cmake
include(FetchContent)
find_package(Threads REQUIRED)

FetchContent_Declare(
  asio
  GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
  GIT_TAG asio-1-30-2
)
FetchContent_MakeAvailable(asio)

add_library(asio_standalone INTERFACE)
target_include_directories(asio_standalone INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(asio_standalone INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio_standalone INTERFACE Threads::Threads)

target_link_libraries(transport PUBLIC asio_standalone)
```

在 `add_executable(transport_tests ...)` 列表中追加 `tests/tcp/asio_smoke_test.cpp`。

> 备注：`chriskohlhoff/asio` 仓库根目录无 CMakeLists，`FetchContent_MakeAvailable` 仅克隆；头文件位于 `${asio_SOURCE_DIR}/asio/include`。

- [ ] **Step 3: 配置（拉取 Asio）+ 构建 + 运行测试**

Run:
```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```
Expected: 配置阶段克隆 asio 成功；`Asio.VersionMacroPresent` PASS；既有测试保持绿色。

- [ ] **Step 4: 提交**

```bash
git add CMakeLists.txt tests/tcp/asio_smoke_test.cpp
git commit -m "build: 集成 Standalone Asio（FetchContent）+ 冒烟测试"
```

---

## Task 2: TCP 配置与 `ITcpServer` 接口头

**Files:**
- Create: `include/transport/tcp/TcpClientConfig.hpp`
- Create: `include/transport/tcp/TcpServerConfig.hpp`
- Create: `include/transport/tcp/ITcpServer.hpp`
- Test: `tests/tcp/tcp_interfaces_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/tcp/tcp_interfaces_test.cpp`:

```cpp
#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

#include <optional>

#include <gtest/gtest.h>

TEST(TcpConfig, ClientDefaults) {
  transport::TcpClientConfig c;
  EXPECT_EQ(c.port, 0);
  EXPECT_EQ(c.connect_timeout_ms, 5000u);
  EXPECT_TRUE(c.auto_reconnect);
  EXPECT_FALSE(c.framer.has_value());
}

TEST(TcpConfig, ServerDefaults) {
  transport::TcpServerConfig c;
  EXPECT_EQ(c.bind_addr, "0.0.0.0");
  EXPECT_EQ(c.max_clients, 10);
  EXPECT_FALSE(c.framer.has_value());
}

TEST(TcpConfig, ITcpServerIsAbstractTransport) {
  // 编译期确认 ITcpServer 继承自 ITransport
  EXPECT_TRUE((std::is_base_of<transport::ITransport, transport::ITcpServer>::value));
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 追加 `tests/tcp/tcp_interfaces_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败 —— 找不到这三个头文件。

- [ ] **Step 4: 写三个头文件**

`include/transport/tcp/TcpClientConfig.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct TcpClientConfig {
  std::string host;
  uint16_t    port               = 0;
  uint32_t    connect_timeout_ms = 5000;
  bool        auto_reconnect     = true;
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
};

}  // namespace transport
```

`include/transport/tcp/TcpServerConfig.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct TcpServerConfig {
  std::string bind_addr   = "0.0.0.0";
  uint16_t    port        = 0;     // 0 = 由 OS 分配临时端口
  int         max_clients = 10;
  std::optional<LengthFieldFramerConfig> framer;  // 应用于每个 accepted 连接的接收侧
};

}  // namespace transport
```

`include/transport/tcp/ITcpServer.hpp`:

```cpp
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"

namespace transport {

// TCP 服务端专属扩展。每个客户端连接通过 OnNewConnection 回调获得独立 client_transport。
class ITcpServer : public ITransport {
 public:
  using ConnectionCallback =
      std::function<void(std::shared_ptr<ITransport> client_transport)>;

  // 新客户端连接时触发；client_transport 是该连接的完整 ITransport 实例
  virtual void OnNewConnection(ConnectionCallback cb) = 0;

  // 返回当前所有已连接客户端的 transport 快照
  virtual std::vector<std::shared_ptr<ITransport>> GetClients() const = 0;

  // 根据 client_id（"ip:port"）主动断开指定客户端
  virtual void DisconnectClient(const std::string& client_id) = 0;
};

}  // namespace transport
```

测试还需 `<type_traits>`，在 `tcp_interfaces_test.cpp` 顶部补 `#include <type_traits>`。

- [ ] **Step 5: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R TcpConfig`
Expected: `TcpConfig.*` 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/tcp/TcpClientConfig.hpp include/transport/tcp/TcpServerConfig.hpp \
        include/transport/tcp/ITcpServer.hpp tests/tcp/tcp_interfaces_test.cpp CMakeLists.txt
git commit -m "feat: TCP 配置结构与 ITcpServer 接口"
```

---

## Task 3: `TcpConnectionImpl`（已连接 socket 收发循环）

**Files:**
- Create: `include/transport/tcp/TcpConnectionImpl.hpp`
- Create: `src/tcp/TcpConnectionImpl.cpp`
- Test: `tests/tcp/tcp_connection_test.cpp`
- Modify: `CMakeLists.txt`（加库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/tcp/tcp_connection_test.cpp`:

```cpp
#include "transport/tcp/TcpConnectionImpl.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/framing/LengthFieldFramer.hpp"

using transport::ICodec;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;
using transport::Result;
using transport::TcpConnectionImpl;

namespace {

// 在 127.0.0.1 建一对已连接 socket：返回 {server_side, client_side}
struct Pair {
  asio::ip::tcp::socket server;
  asio::ip::tcp::socket client;
};

Pair MakeConnectedPair(asio::io_context& ctx) {
  asio::ip::tcp::acceptor acc(ctx, asio::ip::tcp::endpoint(
                                       asio::ip::make_address("127.0.0.1"), 0));
  auto port = acc.local_endpoint().port();
  asio::ip::tcp::socket client(ctx);
  client.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  asio::ip::tcp::socket server = acc.accept();
  return Pair{std::move(server), std::move(client)};
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

void BlockingWriteAll(asio::ip::tcp::socket& s, const std::vector<uint8_t>& data) {
  asio::write(s, asio::buffer(data));
}

// 在每个字节上 +1/-1 的可逆 codec
class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
};

// 运行 io_context 的后台线程封装
struct IoRunner {
  asio::io_context ctx;
  asio::executor_work_guard<asio::io_context::executor_type> guard{ctx.get_executor()};
  std::thread th{[this] { ctx.run(); }};
  ~IoRunner() {
    guard.reset();
    ctx.stop();
    if (th.joinable()) th.join();
  }
};

}  // namespace

TEST(TcpConnectionImpl, PassthroughReceivesRawBytes) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->Open();

  BlockingWriteAll(pair.client, {10, 20, 30});
  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_FALSE(r.value.source.empty());  // "127.0.0.1:port"
  conn->Close();
}

TEST(TcpConnectionImpl, FramerAssemblesAcrossWrites) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), framer);
  conn->Open();

  auto frame = BuildFrame(5, 0xAB);  // 13 字节
  BlockingWriteAll(pair.client, std::vector<uint8_t>(frame.begin(), frame.begin() + 6));
  BlockingWriteAll(pair.client, std::vector<uint8_t>(frame.begin() + 6, frame.end()));

  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, frame);
  conn->Close();
}

TEST(TcpConnectionImpl, SendWritesToPeer) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->Open();

  auto st = conn->Send({1, 2, 3, 4});
  ASSERT_TRUE(static_cast<bool>(st));

  std::vector<uint8_t> got(4);
  asio::read(pair.client, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3, 4}));
  conn->Close();
}

TEST(TcpConnectionImpl, CodecAppliedBothDirections) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->SetCodec(std::make_shared<ShiftCodec>());
  conn->Open();

  // 发送：{1,2,3} 经 Encode(+1) → 对端应收 {2,3,4}
  ASSERT_TRUE(static_cast<bool>(conn->Send({1, 2, 3})));
  std::vector<uint8_t> got(3);
  asio::read(pair.client, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{2, 3, 4}));

  // 接收：对端发 {2,3,4} 经 Decode(-1) → payload {1,2,3}
  BlockingWriteAll(pair.client, {2, 3, 4});
  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  conn->Close();
}

TEST(TcpConnectionImpl, PeerCloseTriggersDisconnectAndConnError) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  std::string reason;
  conn->OnDisconnect([&](const std::string& r) { reason = r; });
  conn->Open();

  pair.client.close();  // 对端关闭
  auto r = conn->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  // OnDisconnect 应已触发（io 线程异步，给出充分超时）
  for (int i = 0; i < 100 && reason.empty(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);
}

TEST(TcpConnectionImpl, FrameErrorDeliversFrameError) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto c = BeConfig();
  c.max_frame_size = 4;  // 任何正常帧都会越界
  auto framer = std::make_shared<LengthFieldFramer>(c);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), framer);
  conn->Open();

  BlockingWriteAll(pair.client, BuildFrame(100, 0x44));  // 触发 frame: 错误
  auto r = conn->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
  conn->Close();
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/tcp/TcpConnectionImpl.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/tcp/tcp_connection_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败 —— 找不到 `TcpConnectionImpl.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/tcp/TcpConnectionImpl.hpp`:

```cpp
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportBase.hpp"
#include "transport/framing/FrameAssembler.hpp"

namespace transport {

// 已连接 socket 的收发循环：客户端连上后、服务端 accept 后均用它。
// io 由 socket 所属的 io_context 驱动（本类不拥有线程）。须以 shared_ptr 持有。
class TcpConnectionImpl : public TransportBase,
                      public std::enable_shared_from_this<TcpConnectionImpl> {
 public:
  TcpConnectionImpl(asio::ip::tcp::socket socket, std::shared_ptr<IFramer> framer);

  Status Open() override;   // 启动 async_read 循环
  void Close() override;    // 关闭 socket + CloseQueue()
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;

  const std::string& PeerId() const { return peer_id_; }

 protected:
  // 断连处理。基类：投递错误 + 关队列 + 通知（accepted 连接为终态）。
  // 子类（client）覆盖以触发重连而不关队列。每个连接周期只生效一次。
  virtual void HandleDisconnect(const std::string& reason);

  void StartRead();   // 启动一次 async_read_some；子类（client）连接成功后调用

  asio::ip::tcp::socket socket_;
  asio::strand<asio::any_io_executor> strand_;
  std::atomic<bool> open_{false};

 private:
  void DoWrite();

  FrameAssembler assembler_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::string peer_id_;
  std::atomic<bool> disconnected_{false};
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/tcp/TcpConnectionImpl.cpp`:

```cpp
#include "transport/tcp/TcpConnectionImpl.hpp"

#include <utility>

namespace transport {

namespace {
std::string EndpointId(const asio::ip::tcp::socket& s) {
  asio::error_code ec;
  auto ep = s.remote_endpoint(ec);
  if (ec) return "";
  return ep.address().to_string() + ":" + std::to_string(ep.port());
}
}  // namespace

TcpConnectionImpl::TcpConnectionImpl(asio::ip::tcp::socket socket,
                             std::shared_ptr<IFramer> framer)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())),
      assembler_(std::move(framer)),
      peer_id_(EndpointId(socket_)) {}

Status TcpConnectionImpl::Open() {
  bool expected = false;
  if (!open_.compare_exchange_strong(expected, true)) {
    return Status::Success(std::monostate{});  // 已打开
  }
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartRead(); });
  return Status::Success(std::monostate{});
}

bool TcpConnectionImpl::IsOpen() const { return open_.load(); }

void TcpConnectionImpl::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
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
              DeliverFrame(std::move(f), peer_id_, "");
            }
            StartRead();
          }));
}

Status TcpConnectionImpl::Send(const std::vector<uint8_t>& data) {
  if (!open_.load()) return Status::Fail("conn: not connected");
  auto enc = EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(enc.value));
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
  return Status::Success(std::monostate{});
}

void TcpConnectionImpl::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
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

void TcpConnectionImpl::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;  // 每周期一次
  open_.store(false);
  asio::error_code ec;
  socket_.close(ec);
  DeliverError(reason);   // 唤醒同步等待者 / 投递错误
  CloseQueue();           // accepted 连接终态：永久关闭接收队列
  NotifyDisconnect(reason);
}

void TcpConnectionImpl::Close() {
  open_.store(false);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    asio::error_code ec;
    socket_.close(ec);
  });
  CloseQueue();
}

}  // namespace transport
```

- [ ] **Step 6: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R TcpConnectionImpl`
Expected: `TcpConnectionImpl.*`（6 个）全部 PASS；全量套件保持绿色。

- [ ] **Step 7: 提交**

```bash
git add include/transport/tcp/TcpConnectionImpl.hpp src/tcp/TcpConnectionImpl.cpp \
        tests/tcp/tcp_connection_test.cpp CMakeLists.txt
git commit -m "feat: TcpConnectionImpl 已连接 socket 收发循环"
```

---

## Task 4: `TcpClientImpl`（connect + 指数退避重连）

**Files:**
- Create: `include/transport/tcp/TcpClientImpl.hpp`
- Create: `src/tcp/TcpClientImpl.cpp`
- Test: `tests/tcp/tcp_client_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/tcp/tcp_client_test.cpp`:

```cpp
#include "transport/tcp/TcpClientImpl.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientImpl;

namespace {

// 一个极简回环 echo/accept 服务器，仅用于测试客户端
class MiniServer {
 public:
  MiniServer()
      : acceptor_(ctx_, asio::ip::tcp::endpoint(
                            asio::ip::make_address("127.0.0.1"), 0)),
        guard_(ctx_.get_executor()) {
    port_ = acceptor_.local_endpoint().port();
    DoAccept();
    th_ = std::thread([this] { ctx_.run(); });
  }
  ~MiniServer() { Stop(); }

  uint16_t port() const { return port_; }

  // 向最近接受的连接写字节
  void WriteToPeer(const std::vector<uint8_t>& d) {
    std::lock_guard<std::mutex> lk(m_);
    if (peer_ && peer_->is_open()) asio::write(*peer_, asio::buffer(d));
  }

  void DropPeer() {
    std::lock_guard<std::mutex> lk(m_);
    if (peer_) { asio::error_code ec; peer_->close(ec); peer_.reset(); }
  }

  void Stop() {
    if (stopped_) return;
    stopped_ = true;
    asio::post(ctx_, [this] {
      asio::error_code ec;
      acceptor_.close(ec);
      if (peer_) { peer_->close(ec); peer_.reset(); }
    });
    guard_.reset();
    ctx_.stop();
    if (th_.joinable()) th_.join();
  }

 private:
  void DoAccept() {
    acceptor_.async_accept([this](asio::error_code ec, asio::ip::tcp::socket s) {
      if (ec) return;
      {
        std::lock_guard<std::mutex> lk(m_);
        peer_ = std::make_unique<asio::ip::tcp::socket>(std::move(s));
      }
      DoAccept();
    });
  }

  asio::io_context ctx_;
  asio::ip::tcp::acceptor acceptor_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  std::unique_ptr<asio::ip::tcp::socket> peer_;
  std::mutex m_;
  std::thread th_;
  uint16_t port_ = 0;
  bool stopped_ = false;
};

TcpClientConfig ClientCfg(uint16_t port, bool reconnect) {
  TcpClientConfig c;
  c.host = "127.0.0.1";
  c.port = port;
  c.connect_timeout_ms = 1000;
  c.auto_reconnect = reconnect;
  return c;
}

}  // namespace

TEST(TcpClient, ConnectAndReceive) {
  MiniServer server;
  // 退避参数无关紧要；用默认
  auto client = std::make_shared<TcpClientImpl>(ClientCfg(server.port(), true));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  server.WriteToPeer({7, 8, 9});
  auto r = client->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 8, 9}));
  client->Close();
}

TEST(TcpClient, ConnectRefusedReturnsError) {
  // 连接一个没有监听者的端口
  auto client = std::make_shared<TcpClientImpl>(ClientCfg(1, false));
  client->SetHost("127.0.0.1");
  auto st = client->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  // ECONNREFUSED → conn:，或极少数环境超时 → timeout:
  bool prefixed = st.error.rfind("conn:", 0) == 0 || st.error.rfind("timeout:", 0) == 0;
  EXPECT_TRUE(prefixed);
  client->Close();
}

TEST(TcpClient, AutoReconnectResumesAfterServerDrop) {
  MiniServer server;
  uint16_t port = server.port();
  // 用小退避基数（10ms）+ 小封顶（100ms）避免久等
  auto client = std::make_shared<TcpClientImpl>(
      ClientCfg(port, true), std::chrono::milliseconds(10),
      std::chrono::milliseconds(100));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  int drops = 0;
  client->OnDisconnect([&](const std::string&) { ++drops; });

  server.DropPeer();  // 触发掉线 + 重连
  // 等待重连：MiniServer 仍在 accept，client 会重新连上
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  EXPECT_GE(drops, 1);

  // 重连后应能继续收数据
  server.WriteToPeer({1, 2, 3});
  auto r = client->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  client->Close();
}

TEST(TcpClient, NoReconnectWhenDisabled) {
  MiniServer server;
  auto client = std::make_shared<TcpClientImpl>(ClientCfg(server.port(), false));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  bool disconnected = false;
  client->OnDisconnect([&](const std::string&) { disconnected = true; });

  server.DropPeer();
  auto r = client->Receive(1000);  // 掉线 → conn: 错误
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  for (int i = 0; i < 100 && !disconnected; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_TRUE(disconnected);
  client->Close();
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/tcp/TcpClientImpl.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/tcp/tcp_client_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败 —— 找不到 `TcpClientImpl.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/tcp/TcpClientImpl.hpp`:

```cpp
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <asio.hpp>

#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpConnectionImpl.hpp"

namespace transport {

namespace detail {
// 仅持有 io_context。作为 TcpClientImpl 的首个基类，保证 io_context 先于
// TcpConnectionImpl 基类构造——TcpConnectionImpl 的 socket_ 需绑定到这个 ctx。
struct IoContextHolder {
  asio::io_context ctx;
};
}  // namespace detail

// TCP 客户端：自有 io_context + 1 线程；connect 超时；指数退避自动重连。
// 须以 shared_ptr 持有（基类 TcpConnectionImpl 用 shared_from_this 保活）。
class TcpClientImpl : public detail::IoContextHolder, public TcpConnectionImpl {
 public:
  explicit TcpClientImpl(
      TcpClientConfig config,
      std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
      std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientImpl() override;

  Status Open() override;   // 同步连接（受 connect_timeout_ms 约束），成功后启动读
  void Close() override;    // 停重连 + 关连接 + 停 io 线程

  // 仅供测试/动态配置：覆盖目标 host
  void SetHost(const std::string& host) { config_.host = host; }

 protected:
  void HandleDisconnect(const std::string& reason) override;  // 触发重连

 private:
  // 在 io 线程上发起一次连接；prom 非空=初次 Open（设置结果），空=重连
  void StartConnect(std::shared_ptr<std::promise<Status>> prom);
  void ScheduleReconnect();

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_;
  std::chrono::milliseconds backoff_cap_;
  std::chrono::milliseconds backoff_cur_;

  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_;
  asio::steady_timer reconnect_timer_;
  std::thread io_thread_;
  std::atomic<bool> closing_{false};
  std::atomic<bool> link_up_{false};  // 连接建立标志；保证每次掉线只处理一次
};

}  // namespace transport
```

> 构造顺序要点：`TcpConnectionImpl`（基类）的 `socket_` 必须绑定到客户端自有的 `io_context`。C++ 基类按声明顺序构造，故让 `detail::IoContextHolder` 作为**首个**基类，其 `ctx` 即在 `TcpConnectionImpl` 基类之前构造完成，可在初始化列表里用 `asio::ip::tcp::socket(ctx)` 传给 `TcpConnectionImpl`。

- [ ] **Step 5: 写实现**

`src/tcp/TcpClientImpl.cpp`:

```cpp
#include "transport/tcp/TcpClientImpl.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

TcpClientImpl::TcpClientImpl(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : detail::IoContextHolder(),
      TcpConnectionImpl(asio::ip::tcp::socket(ctx),
                    config.framer
                        ? std::make_shared<LengthFieldFramer>(*config.framer)
                        : nullptr),
      config_(std::move(config)),
      backoff_base_(backoff_base),
      backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base),
      guard_(ctx.get_executor()),
      resolver_(ctx),
      connect_timer_(ctx),
      reconnect_timer_(ctx) {
  io_thread_ = std::thread([this] { ctx.run(); });
}

TcpClientImpl::~TcpClientImpl() { Close(); }

Status TcpClientImpl::Open() {
  // framer 配置校验（spec：非法则 config: 错误）
  if (config_.framer) {
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();  // 阻塞等待初次连接成败/超时
}

void TcpClientImpl::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());

  asio::error_code rec;
  auto endpoints =
      resolver_.resolve(config_.host, std::to_string(config_.port), rec);
  if (rec) {
    if (prom) prom->set_value(Status::Fail("conn: resolve: " + rec.message()));
    else ScheduleReconnect();
    return;
  }

  asio::error_code ig;
  socket_.close(ig);
  socket_ = asio::ip::tcp::socket(ctx);  // 新建 socket（重连时旧的已关）

  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(
      std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;  // 定时器被取消（连接已完成）
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);  // 取消进行中的 connect
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_,
          [this, self, prom, timed_out](asio::error_code ec,
                                        const asio::ip::tcp::endpoint&) {
            connect_timer_.cancel();
            if (!ec) {
              link_up_.store(true);
              open_.store(true);
              backoff_cur_ = backoff_base_;
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              StartRead();  // 启动读循环（基类 protected）
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
          }));
}

void TcpClientImpl::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

void TcpClientImpl::HandleDisconnect(const std::string& reason) {
  if (!link_up_.exchange(false)) return;  // 每个连接周期只处理一次
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  NotifyDisconnect(reason);
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();  // 复用接收队列，不 CloseQueue
  } else {
    DeliverError(reason);
    CloseQueue();
  }
}

void TcpClientImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  link_up_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
  });
  CloseQueue();
  guard_.reset();
  ctx.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

> 重连只复用接收队列、不 `CloseQueue`：`link_up_` 在连上时置 `true`、首次掉线时 `exchange(false)` 确保 read/write 同时报错也只处理一次。`StartRead()` 是基类 `protected` 方法（Task 3 已定义），连接成功后调用即开始读循环。

- [ ] **Step 6: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R TcpClient`
Expected: `TcpClient.*`（4 个）全部 PASS；全量套件保持绿色。若 `AutoReconnect` 偶发，确认退避用的是测试注入的小值（10ms/100ms）。

- [ ] **Step 7: 提交**

```bash
git add include/transport/tcp/TcpClientImpl.hpp src/tcp/TcpClientImpl.cpp \
        tests/tcp/tcp_client_test.cpp CMakeLists.txt
git commit -m "feat: TcpClientImpl connect + 指数退避重连"
```

---

## Task 5: `TcpServerImpl`（acceptor + 广播 + ITcpServer）

**Files:**
- Create: `include/transport/tcp/TcpServerImpl.hpp`
- Create: `src/tcp/TcpServerImpl.cpp`
- Test: `tests/tcp/tcp_server_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/tcp/tcp_server_test.cpp`:

```cpp
#include "transport/tcp/TcpServerImpl.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpConnectionImpl.hpp"

using transport::ITransport;
using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientImpl;
using transport::TcpServerConfig;
using transport::TcpServerImpl;

namespace {

TcpServerConfig ServerCfg() {
  TcpServerConfig c;
  c.bind_addr = "127.0.0.1";
  c.port = 0;  // OS 分配
  return c;
}

std::shared_ptr<TcpClientImpl> MakeClient(uint16_t port) {
  TcpClientConfig c;
  c.host = "127.0.0.1";
  c.port = port;
  c.connect_timeout_ms = 1000;
  c.auto_reconnect = false;
  return std::make_shared<TcpClientImpl>(c);
}

void WaitFor(std::function<bool()> pred, int ms = 1000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}

}  // namespace

TEST(TcpServer, AcceptsConnectionAndDeliversPerClient) {
  auto server = std::make_shared<TcpServerImpl>(ServerCfg());
  std::atomic<int> conns{0};
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    accepted = c;
    ++conns;
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto client = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });
  ASSERT_EQ(conns.load(), 1);
  ASSERT_TRUE(accepted != nullptr);

  // client → server：accepted 端收到
  ASSERT_TRUE(static_cast<bool>(client->Send({5, 6, 7})));
  auto r = accepted->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{5, 6, 7}));

  client->Close();
  server->Close();
}

TEST(TcpServer, BroadcastSendReachesAllClients) {
  auto server = std::make_shared<TcpServerImpl>(ServerCfg());
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto c1 = MakeClient(server->LocalPort());
  auto c2 = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(c1->Open()));
  ASSERT_TRUE(static_cast<bool>(c2->Open()));
  WaitFor([&] { return conns.load() >= 2; });
  ASSERT_EQ(conns.load(), 2);

  ASSERT_TRUE(static_cast<bool>(server->Send({9, 9})));
  auto r1 = c1->Receive(1000);
  auto r2 = c2->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r1));
  ASSERT_TRUE(static_cast<bool>(r2));
  EXPECT_EQ(r1.value.payload, (std::vector<uint8_t>{9, 9}));
  EXPECT_EQ(r2.value.payload, (std::vector<uint8_t>{9, 9}));

  c1->Close();
  c2->Close();
  server->Close();
}

TEST(TcpServer, GetClientsAndDisconnectClient) {
  auto server = std::make_shared<TcpServerImpl>(ServerCfg());
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) { accepted = c; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto client = MakeClient(server->LocalPort());
  bool dropped = false;
  client->OnDisconnect([&](const std::string&) { dropped = true; });
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return server->GetClients().size() >= 1 && accepted != nullptr; });
  ASSERT_EQ(server->GetClients().size(), 1u);

  // accepted 实为 TcpConnectionImpl；其 PeerId() 即 server 端登记的 client_id
  auto conn = std::dynamic_pointer_cast<transport::TcpConnectionImpl>(accepted);
  ASSERT_TRUE(conn != nullptr);
  server->DisconnectClient(conn->PeerId());

  WaitFor([&] { return server->GetClients().empty(); });
  EXPECT_TRUE(server->GetClients().empty());
  WaitFor([&] { return dropped; });
  EXPECT_TRUE(dropped);  // 被断开的客户端感知到 conn 断连

  client->Close();
  server->Close();
}

TEST(TcpServer, ReceiveOnServerReturnsConfigError) {
  auto server = std::make_shared<TcpServerImpl>(ServerCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  auto r = server->Receive(10);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u);
  auto fut = server->AsyncReceive();
  auto fr = fut.get();
  EXPECT_FALSE(static_cast<bool>(fr));
  EXPECT_EQ(fr.error.rfind("config:", 0), 0u);
  server->Close();
}

TEST(TcpServer, MaxClientsRejectsExtra) {
  auto cfg = ServerCfg();
  cfg.max_clients = 1;
  auto server = std::make_shared<TcpServerImpl>(cfg);
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto c1 = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(c1->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  auto c2 = MakeClient(server->LocalPort());
  (void)c2->Open();  // 连接可能 TCP 层成功，但会被立即关闭
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(conns.load(), 1);  // 第二个不触发 OnNewConnection

  c1->Close();
  c2->Close();
  server->Close();
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/tcp/TcpServerImpl.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/tcp/tcp_server_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败 —— 找不到 `TcpServerImpl.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/tcp/TcpServerImpl.hpp`:

```cpp
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpConnectionImpl.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

namespace transport {

// TCP 服务端：自有 io_context + 1 线程；acceptor 每 accept 造一个共享该 io_context
// 的 TcpConnectionImpl。须以 shared_ptr 持有。
class TcpServerImpl
    : public ITcpServer,
      public std::enable_shared_from_this<TcpServerImpl> {
 public:
  explicit TcpServerImpl(TcpServerConfig config);
  ~TcpServerImpl() override;

  Status Open() override;
  void Close() override;
  bool IsOpen() const override;

  Status Send(const std::vector<uint8_t>& data) override;  // 广播

  // 服务端不适用的接收方法（主 spec §5.3）
  Result<Message> Receive(uint32_t timeout_ms) override;
  void OnReceive(ReceiveCallback cb) override;
  std::future<Result<Message>> AsyncReceive() override;
  void OnDisconnect(DisconnectCallback cb) override;
  void SetCodec(std::shared_ptr<ICodec> codec) override;

  // ITcpServer
  void OnNewConnection(ConnectionCallback cb) override;
  std::vector<std::shared_ptr<ITransport>> GetClients() const override;
  void DisconnectClient(const std::string& client_id) override;

  uint16_t LocalPort() const { return local_port_; }

 private:
  void DoAccept();
  void RemoveClient(const std::string& id);

  TcpServerConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::ip::tcp::acceptor acceptor_;
  std::thread io_thread_;

  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<TcpConnectionImpl>> clients_;
  ConnectionCallback connection_cb_;
  DisconnectCallback disconnect_cb_;
  std::shared_ptr<ICodec> codec_;

  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/tcp/TcpServerImpl.cpp`:

```cpp
#include "transport/tcp/TcpServerImpl.hpp"

#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

TcpServerImpl::TcpServerImpl(TcpServerConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      acceptor_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpServerImpl::~TcpServerImpl() { Close(); }

Status TcpServerImpl::Open() {
  if (config_.framer) {  // framer 配置校验（spec：非法则 config: 错误）
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }
  asio::error_code ec;
  auto addr = asio::ip::make_address(config_.bind_addr, ec);
  if (ec) return Status::Fail("config: invalid bind_addr");
  asio::ip::tcp::endpoint ep(addr, config_.port);

  acceptor_.open(ep.protocol(), ec);
  if (ec) return Status::Fail("conn: open: " + ec.message());
  acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
  acceptor_.bind(ep, ec);
  if (ec) return Status::Fail("conn: bind: " + ec.message());
  acceptor_.listen(asio::socket_base::max_listen_connections, ec);
  if (ec) return Status::Fail("conn: listen: " + ec.message());

  local_port_ = acceptor_.local_endpoint().port();
  open_.store(true);
  DoAccept();
  return Status::Success(std::monostate{});
}

bool TcpServerImpl::IsOpen() const { return open_.load(); }

void TcpServerImpl::DoAccept() {
  auto self = shared_from_this();
  acceptor_.async_accept([this, self](asio::error_code ec,
                                      asio::ip::tcp::socket sock) {
    if (ec) {
      if (open_.load() && disconnect_cb_) disconnect_cb_("conn: acceptor: " + ec.message());
      return;
    }
    std::shared_ptr<TcpConnectionImpl> conn;
    ConnectionCallback cb_copy;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (static_cast<int>(clients_.size()) >= config_.max_clients) {
        asio::error_code ig;
        sock.close(ig);
      } else {
        std::shared_ptr<IFramer> framer;
        if (config_.framer)
          framer = std::make_shared<LengthFieldFramer>(*config_.framer);
        conn = std::make_shared<TcpConnectionImpl>(std::move(sock), framer);
        if (codec_) conn->SetCodec(codec_);
        const std::string id = conn->PeerId();
        clients_[id] = conn;
        std::weak_ptr<TcpServerImpl> wself = self;
        conn->OnDisconnect([wself, id](const std::string&) {
          if (auto s = wself.lock()) s->RemoveClient(id);
        });
        conn->Open();
        cb_copy = connection_cb_;
      }
    }
    if (conn && cb_copy) cb_copy(conn);  // 锁外回调
    DoAccept();
  });
}

void TcpServerImpl::RemoveClient(const std::string& id) {
  std::lock_guard<std::mutex> lock(mutex_);
  clients_.erase(id);
}

Status TcpServerImpl::Send(const std::vector<uint8_t>& data) {
  std::vector<std::shared_ptr<TcpConnectionImpl>> snapshot;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : clients_) snapshot.push_back(kv.second);
  }
  for (auto& c : snapshot) c->Send(data);
  return Status::Success(std::monostate{});
}

Result<Message> TcpServerImpl::Receive(uint32_t) {
  return Result<Message>::Fail(
      "config: 请使用 OnNewConnection 获取的 client_transport 进行接收");
}

void TcpServerImpl::OnReceive(ReceiveCallback) {}

std::future<Result<Message>> TcpServerImpl::AsyncReceive() {
  std::promise<Result<Message>> p;
  p.set_value(Result<Message>::Fail(
      "config: 请使用 OnNewConnection 获取的 client_transport 进行接收"));
  return p.get_future();
}

void TcpServerImpl::OnDisconnect(DisconnectCallback cb) {
  disconnect_cb_ = std::move(cb);
}

void TcpServerImpl::SetCodec(std::shared_ptr<ICodec> codec) {
  std::lock_guard<std::mutex> lock(mutex_);
  codec_ = std::move(codec);
}

void TcpServerImpl::OnNewConnection(ConnectionCallback cb) {
  std::lock_guard<std::mutex> lock(mutex_);
  connection_cb_ = std::move(cb);
}

std::vector<std::shared_ptr<ITransport>> TcpServerImpl::GetClients() const {
  std::vector<std::shared_ptr<ITransport>> out;
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& kv : clients_) out.push_back(kv.second);
  return out;
}

void TcpServerImpl::DisconnectClient(const std::string& client_id) {
  std::shared_ptr<TcpConnectionImpl> conn;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) return;
    conn = it->second;
    clients_.erase(it);
  }
  conn->Close();
}

void TcpServerImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::vector<std::shared_ptr<TcpConnectionImpl>> snapshot;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : clients_) snapshot.push_back(kv.second);
    clients_.clear();
  }
  asio::post(ctx_, [this]() {
    asio::error_code ig;
    acceptor_.close(ig);
  });
  for (auto& c : snapshot) c->Close();

  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

- [ ] **Step 6: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R TcpServer`
Expected: `TcpServer.*`（5 个）全部 PASS；全量套件保持绿色。

- [ ] **Step 7: 提交**

```bash
git add include/transport/tcp/TcpServerImpl.hpp src/tcp/TcpServerImpl.cpp \
        tests/tcp/tcp_server_test.cpp CMakeLists.txt
git commit -m "feat: TcpServerImpl acceptor + 广播 + ITcpServer"
```

---

## Task 6: TCP 收尾验证 + README 更新

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 全量干净构建 + 反复跑稳定性**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
# 网络测试稳定性：连续跑 10 次 TCP 套件
for i in $(seq 1 10); do ctest --test-dir build -R 'Tcp' --output-on-failure || break; done
```
Expected: 全部测试 PASS；TCP 套件 10 次均绿（无 flaky）。

- [ ] **Step 2: 更新 README 状态**

把 `README.md` 的「状态」小节中 TCP 一行改为已完成：

```markdown
- [x] Foundation：核心接口、分帧、接收交付、传输基类
- [x] TCP（client / server）
- [ ] UDP（单播 / 组播 / 广播）
- [ ] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置
```

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README 标记 TCP 完成"
```

---

## 后续计划（不在本计划范围）

UDP（主 spec §6）、串口（§7）、DDS（§8）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。
