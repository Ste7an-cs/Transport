# UDP 传输 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 先把 Foundation 的接收交付基座由「继承式 `TransportBase`」重构为「组合式组件 `TransportCore`」（消除与扩展接口的 `ITransport` 菱形），再在其上实现 UDP 单播/组播/广播传输（单个 `UdpImpl`，Standalone Asio，真实回环测试，组播/广播不支持则优雅跳过）。

**Architecture:** `TransportCore` 是不继承 `ITransport` 的「接收交付 + 编解码」内核，被各传输**持有**。`TcpConnectionImpl` 由「继承 TransportBase」改为「`: public ITransport` + 持有 `TransportCore core_` + 5 个接收方法转发」（`TcpClientImpl` 随之把 `core_.X` 调用接上）。`UdpImpl : public IUdpTransport` 同样组合 `TransportCore`，按 `mode` 在 `Open()` 分支配置 socket，无 framer（报文保边界）。

**Tech Stack:** C++17、Standalone Asio（已集成，`ASIO_STANDALONE`）、GoogleTest 1.14、Google C++ 风格。

**设计依据：** spec `docs/superpowers/specs/2026-06-10-udp-transport-design.md`（已锁定 B 方案 + `UdpImpl` 命名）。

---

## 文件结构

```
include/transport/core/
├── TransportCore.hpp        # 新增：接收交付+编解码内核（组件，非 ITransport）
└── TransportBase.hpp        # 删除
include/transport/udp/
├── UdpConfig.hpp            # 新增：UdpMode + UdpConfig
├── IUdpTransport.hpp        # 新增：IUdpTransport（+ SendTo）
└── UdpImpl.hpp              # 新增：单类多模式实现
src/udp/
└── UdpImpl.cpp              # 新增
tests/core/
├── transport_core_test.cpp  # 新增（替换 transport_base_test.cpp）
└── transport_base_test.cpp  # 删除
tests/udp/
├── udp_interfaces_test.cpp  # 新增
└── udp_transport_test.cpp   # 新增
（修改：TcpConnectionImpl.hpp/.cpp、TcpClientImpl.cpp、CMakeLists.txt）
```

---

## Task 1: Foundation 重构 — `TransportBase` → `TransportCore`（组合）

**这是纯重构：不引入新行为，「现存全部测试保持绿色」即为通过判据。** Foundation + TCP 既有测试在重构前为 57 个（重构后 `transport_base_test` 改写为 `transport_core_test`，总数不变）。

**Files:**
- Create: `include/transport/core/TransportCore.hpp`
- Modify: `include/transport/tcp/TcpConnectionImpl.hpp`
- Modify: `src/tcp/TcpConnectionImpl.cpp`
- Modify: `src/tcp/TcpClientImpl.cpp`
- Create: `tests/core/transport_core_test.cpp`
- Delete: `include/transport/core/TransportBase.hpp`
- Delete: `tests/core/transport_base_test.cpp`
- Modify: `CMakeLists.txt`（测试源 transport_base_test → transport_core_test）

- [ ] **Step 1: 新建 `TransportCore.hpp`**

`include/transport/core/TransportCore.hpp`（内容即原 `TransportBase` 的内脏，去掉 `: public ITransport`，方法全 public）：

```cpp
#pragma once

// -----------------------------------------------------------------------------
// TransportCore.hpp — 接收交付 + 编解码内核（被持有的组件，本身不是 ITransport）
// 会收数据的传输（TCP 连接 / UDP / DDS / 串口）持有它：把 ITransport 的接收侧方法
// (Receive/OnReceive/AsyncReceive/SetCodec/OnDisconnect) 转发给它；io 线程收到字节
// 调 DeliverFrame、发送前调 EncodeForSend。把 ITransport 留在具体传输上，避免
// 「TransportBase 与扩展接口同源 ITransport」的菱形。
// -----------------------------------------------------------------------------

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"  // 仅借用 ReceiveCallback / DisconnectCallback 类型
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/ReceiveQueue.hpp"

namespace transport {

class TransportCore {
 public:
  using ReceiveCallback = ITransport::ReceiveCallback;
  using DisconnectCallback = ITransport::DisconnectCallback;

  // —— 接收侧（持有者转发给 ITransport 同名方法）——
  void SetCodec(std::shared_ptr<ICodec> codec) { codec_ = std::move(codec); }
  Result<Message> Receive(uint32_t timeout_ms) { return queue_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) { queue_.SetCallback(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() { return queue_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) { disconnect_cb_ = std::move(cb); }

  // —— 生产侧（持有者在 io 线程调用）——
  // 发送前编码；无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(data);
    return codec_->Encode(data);
  }

  // 收到一帧/一报文：解码（无 codec 透传）后构造 Message 投递；解码失败投递 Fail。
  void DeliverFrame(std::vector<uint8_t> frame, const std::string& source,
                    const std::string& topic) {
    std::vector<uint8_t> payload;
    if (codec_) {
      auto decoded = codec_->Decode(frame);
      if (!decoded) {
        queue_.Push(Result<Message>::Fail(decoded.error));
        return;
      }
      payload = std::move(decoded.value);
    } else {
      payload = std::move(frame);
    }
    Message msg;
    msg.payload = std::move(payload);
    msg.source = source;
    msg.topic = topic;
    msg.timestamp = NowMicros();
    queue_.Push(Result<Message>::Success(std::move(msg)));
  }

  // 投递一个连接/IO 级错误到接收侧。
  void DeliverError(std::string error) {
    queue_.Push(Result<Message>::Fail(std::move(error)));
  }

  void NotifyDisconnect(const std::string& reason) {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

  // 关闭接收队列（唤醒等待者）。持有者 Close() 应调用。
  void Close() { queue_.Close(); }

  static int64_t NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

 private:
  std::shared_ptr<ICodec> codec_;
  ReceiveQueue queue_;
  DisconnectCallback disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 2: 改 `TcpConnectionImpl.hpp` 为组合**

把 `include/transport/tcp/TcpConnectionImpl.hpp` 改为不再继承 `TransportBase`，改为 `: public ITransport` + 持有 `TransportCore core_` + 5 个接收方法转发。具体替换：

(a) 头部 include：把
```cpp
#include "transport/core/TransportBase.hpp"
```
改为
```cpp
#include "transport/ITransport.hpp"
#include "transport/core/TransportCore.hpp"
```

(b) 类定义。把现有：
```cpp
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
```
替换为：
```cpp
class TcpConnectionImpl : public ITransport,
                          public std::enable_shared_from_this<TcpConnectionImpl> {
 public:
  TcpConnectionImpl(asio::ip::tcp::socket socket, std::shared_ptr<IFramer> framer);

  Status Open() override;   // 启动 async_read 循环
  void Close() override;    // 关闭 socket + core_.Close()
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;

  // 接收侧：转发给 core_
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }
  void SetCodec(std::shared_ptr<ICodec> codec) override { core_.SetCodec(std::move(codec)); }

  const std::string& PeerId() const { return peer_id_; }

 protected:
  // 断连处理。基类版：投递错误 + 关队列 + 通知（accepted 连接为终态）。
  // 子类（client）覆盖以触发重连而不关队列。每个连接周期只生效一次。
  virtual void HandleDisconnect(const std::string& reason);

  void StartRead();   // 启动一次 async_read_some；子类（client）连接成功后调用

  TransportCore core_;  // 接收交付 + 编解码（子类 TcpClientImpl 也用）
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
```
（须确保头部已 include `<future>` 与 `<memory>`——当前文件已含 `<memory>`；`<future>` 由 TransportCore.hpp 传递，但建议在本文件 include 区显式加 `#include <future>`。）

- [ ] **Step 3: 改 `TcpConnectionImpl.cpp` 的 5 处工具调用为 `core_.X`**

在 `src/tcp/TcpConnectionImpl.cpp` 做如下替换（仅这些方法名前加 `core_.`，`CloseQueue` 改为 `core_.Close`）：

| 原调用 | 改为 |
|--------|------|
| `DeliverFrame(std::move(f), peer_id_, "");` | `core_.DeliverFrame(std::move(f), peer_id_, "");` |
| `auto enc = EncodeForSend(data);` | `auto enc = core_.EncodeForSend(data);` |
| `DeliverError(reason);`（HandleDisconnect 内） | `core_.DeliverError(reason);` |
| `CloseQueue();`（HandleDisconnect 内） | `core_.Close();` |
| `NotifyDisconnect(reason);`（HandleDisconnect 内） | `core_.NotifyDisconnect(reason);` |
| `CloseQueue();`（Close() 内） | `core_.Close();` |

其余代码（socket_/strand_/open_/assembler_/write_queue_ 等）不变。

- [ ] **Step 4: 改 `TcpClientImpl.cpp` 的 3 处工具调用为 `core_.X`**

`src/tcp/TcpClientImpl.cpp` 的 `HandleDisconnect` 内：

| 原调用 | 改为 |
|--------|------|
| `NotifyDisconnect(reason);` | `core_.NotifyDisconnect(reason);` |
| `DeliverError(reason);` | `core_.DeliverError(reason);` |
| `CloseQueue();` | `core_.Close();` |

`StartRead()`（基类 protected 方法）与其它代码不变。`core_` 在 `TcpConnectionImpl` 中为 `protected`，子类可直接访问。

- [ ] **Step 5: 改写 TransportBase 测试为 TransportCore 测试**

新建 `tests/core/transport_core_test.cpp`（直接测 `TransportCore` 公共方法，不再需要 FakeTransport 子类）：

```cpp
#include "transport/core/TransportCore.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Result;
using transport::TransportCore;

namespace {

// 在每个字节上 +1（Encode）/ -1（Decode）的可逆 codec
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

// 始终失败的 Decode
class FailDecodeCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    return Result<std::vector<uint8_t>>::Success(d);
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>&) override {
    return Result<std::vector<uint8_t>>::Fail("codec: bad frame");
  }
};

}  // namespace

TEST(TransportCore, PassthroughDeliversRawBytes) {
  TransportCore core;
  core.DeliverFrame({10, 20, 30}, "1.2.3.4:5", "topicA");
  auto r = core.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, "1.2.3.4:5");
  EXPECT_EQ(r.value.topic, "topicA");
  EXPECT_GT(r.value.timestamp, 0);
}

TEST(TransportCore, EncodeForSendIsIdentityWithoutCodec) {
  TransportCore core;
  auto enc = core.EncodeForSend({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportCore, CodecAppliedOnSend) {
  TransportCore core;
  core.SetCodec(std::make_shared<ShiftCodec>());
  auto enc = core.EncodeForSend({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(TransportCore, CodecAppliedOnReceive) {
  TransportCore core;
  core.SetCodec(std::make_shared<ShiftCodec>());
  core.DeliverFrame({2, 3, 4}, "src", "");
  auto r = core.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportCore, DecodeFailureDeliversFail) {
  TransportCore core;
  core.SetCodec(std::make_shared<FailDecodeCodec>());
  core.DeliverFrame({9, 9}, "src", "");
  auto r = core.Receive(100);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(TransportCore, DisconnectCallbackInvoked) {
  TransportCore core;
  std::string reason;
  core.OnDisconnect([&](const std::string& r) { reason = r; });
  core.NotifyDisconnect("conn: peer closed");
  EXPECT_EQ(reason, "conn: peer closed");
}
```

- [ ] **Step 6: 删除旧文件 + 更新 CMake**

```bash
git rm include/transport/core/TransportBase.hpp tests/core/transport_base_test.cpp
```
在 `CMakeLists.txt` 的 `add_executable(transport_tests ...)` 列表里，把
`tests/core/transport_base_test.cpp` 改为 `tests/core/transport_core_test.cpp`。

顺带把三处 banner 注释里的 `TransportBase` 字样改为 `TransportCore`（保持文档准确，非必须）：`include/transport/ICodec.hpp`、`include/transport/core/ReceiveQueue.hpp`、`src/tcp/TcpServerImpl.cpp` 注释中的「TransportBase」→「TransportCore」。

- [ ] **Step 7: 干净构建 + 全量回归**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```
Expected: 编译通过；**57/57 全绿**（`TransportCore.*` 6 个替换了原 `TransportBase.*` 6 个，TCP 全部测试不变照绿）。若 TCP 测试有任何变红，说明转发或 `core_` 访问漏改，按错误定位修正。

- [ ] **Step 8: 提交**

```bash
git add include/transport/core/TransportCore.hpp include/transport/tcp/TcpConnectionImpl.hpp \
        src/tcp/TcpConnectionImpl.cpp src/tcp/TcpClientImpl.cpp \
        tests/core/transport_core_test.cpp CMakeLists.txt \
        include/transport/ICodec.hpp include/transport/core/ReceiveQueue.hpp src/tcp/TcpServerImpl.cpp
git rm -q include/transport/core/TransportBase.hpp tests/core/transport_base_test.cpp 2>/dev/null; true
git commit -m "refactor(core): TransportBase→TransportCore 组合化，TCP 改持有+转发（消除 ITransport 菱形，为 UDP/DDS 铺路）"
```

---

## Task 2: UDP 配置与 `IUdpTransport` 接口头

**Files:**
- Create: `include/transport/udp/UdpConfig.hpp`
- Create: `include/transport/udp/IUdpTransport.hpp`
- Test: `tests/udp/udp_interfaces_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/udp/udp_interfaces_test.cpp`:

```cpp
#include "transport/udp/IUdpTransport.hpp"
#include "transport/udp/UdpConfig.hpp"

#include <type_traits>

#include <gtest/gtest.h>

TEST(UdpConfig, Defaults) {
  transport::UdpConfig c;
  EXPECT_TRUE(c.mode == transport::UdpMode::kUnicast);
  EXPECT_EQ(c.local_addr, "0.0.0.0");
  EXPECT_EQ(c.local_port, 0);
  EXPECT_EQ(c.remote_port, 0);
  EXPECT_EQ(c.ttl, 1);
}

TEST(UdpConfig, IUdpTransportIsTransport) {
  EXPECT_TRUE((std::is_base_of<transport::ITransport,
                               transport::IUdpTransport>::value));
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 追加 `tests/udp/udp_interfaces_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 UDP 头文件。

- [ ] **Step 4: 写两个头文件**

`include/transport/udp/UdpConfig.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// UdpConfig.hpp — UDP 配置（UdpMode + UdpConfig）
// 单播/组播/广播三模式；本地绑定、默认发送目的地、组播组与 TTL。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>

namespace transport {

enum class UdpMode { kUnicast, kMulticast, kBroadcast };

struct UdpConfig {
  UdpMode     mode            = UdpMode::kUnicast;
  std::string local_addr      = "0.0.0.0";
  uint16_t    local_port      = 0;     // 0 = 由 OS 分配临时端口
  std::string remote_addr;             // Send() 默认目的地（单播/广播）
  uint16_t    remote_port     = 0;
  std::string multicast_group;         // 仅 kMulticast：Send() 默认目的地
  uint8_t     ttl             = 1;     // 组播 TTL（hops）
};

}  // namespace transport
```

`include/transport/udp/IUdpTransport.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// IUdpTransport.hpp — UDP 扩展接口（ITransport + 运行期指定目的地）
// 在 ITransport 基础上加 SendTo（发往运行期 ip:port，忽略 config 默认 remote）。
// 实现见 UdpImpl。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"

namespace transport {

class IUdpTransport : public ITransport {
 public:
  // 发往运行期指定的目的地；忽略 config 的默认 remote。
  virtual Status SendTo(const std::vector<uint8_t>& data,
                        const std::string& ip, uint16_t port) = 0;
};

}  // namespace transport
```

- [ ] **Step 5: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R UdpConfig`
Expected: `UdpConfig.*`（2 个）PASS；全量套件保持绿色。

- [ ] **Step 6: 提交**

```bash
git add include/transport/udp/UdpConfig.hpp include/transport/udp/IUdpTransport.hpp \
        tests/udp/udp_interfaces_test.cpp CMakeLists.txt
git commit -m "feat: UDP 配置结构与 IUdpTransport 接口"
```

---

## Task 3: `UdpImpl`（单类多模式 + 真实回环测试）

**Files:**
- Create: `include/transport/udp/UdpImpl.hpp`
- Create: `src/udp/UdpImpl.cpp`
- Test: `tests/udp/udp_transport_test.cpp`
- Modify: `CMakeLists.txt`（加库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/udp/udp_transport_test.cpp`:

```cpp
#include "transport/udp/UdpImpl.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Result;
using transport::UdpConfig;
using transport::UdpImpl;
using transport::UdpMode;

namespace {

UdpConfig UnicastCfg(const std::string& remote_ip, uint16_t remote_port) {
  UdpConfig c;
  c.mode = UdpMode::kUnicast;
  c.local_addr = "127.0.0.1";
  c.local_port = 0;          // OS 分配
  c.remote_addr = remote_ip; // 可空（只收时）
  c.remote_port = remote_port;
  return c;
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

TEST(UdpTransport, UnicastSendReceive) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));  // 只收，无默认 remote
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rport));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_FALSE(r.value.source.empty());  // "127.0.0.1:<tx port>"
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, SendToOverridesDefault) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("", 0));  // 无默认 remote
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->SendTo({4, 5}, "127.0.0.1", rport)));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 5}));
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, CodecAppliedBothDirections) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  rx->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rport));
  tx->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));  // Encode +1 → 线上 {2,3,4}
  auto r = rx->Receive(1000);                            // Decode -1 → {1,2,3}
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, SendBeforeOpenFails) {
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", 9999));
  auto st = tx->Send({1});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(UdpTransport, MulticastLoopbackOrSkip) {
  UdpConfig c;
  c.mode = UdpMode::kMulticast;
  c.multicast_group = "239.255.0.1";
  c.local_port = 0;
  auto m = std::make_shared<UdpImpl>(c);
  auto opened = m->Open();
  if (!opened) GTEST_SKIP() << "multicast not supported: " << opened.error;

  uint16_t port = m->LocalPort();
  // 经 enable_loopback 发给自身的组播组:本端口
  auto sent = m->SendTo({7, 8, 9}, "239.255.0.1", port);
  if (!sent) {
    m->Close();
    GTEST_SKIP() << "multicast send failed: " << sent.error;
  }
  auto r = m->Receive(500);
  if (!r) {
    m->Close();
    GTEST_SKIP() << "multicast loopback not delivered in this environment";
  }
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 8, 9}));
  m->Close();
}

TEST(UdpTransport, BroadcastLoopbackOrSkip) {
  // 接收端：普通单播绑 0.0.0.0:P
  UdpConfig rc;
  rc.mode = UdpMode::kUnicast;
  rc.local_addr = "0.0.0.0";
  rc.local_port = 0;
  auto rx = std::make_shared<UdpImpl>(rc);
  if (!rx->Open()) GTEST_SKIP() << "udp open failed";
  uint16_t port = rx->LocalPort();

  // 发送端：广播模式发 127.255.255.255:P
  UdpConfig bc;
  bc.mode = UdpMode::kBroadcast;
  bc.local_addr = "0.0.0.0";
  bc.local_port = 0;
  bc.remote_addr = "127.255.255.255";
  bc.remote_port = port;
  auto tx = std::make_shared<UdpImpl>(bc);
  auto opened = tx->Open();
  if (!opened) {
    rx->Close();
    GTEST_SKIP() << "broadcast not supported: " << opened.error;
  }
  auto sent = tx->Send({5, 5});
  if (!sent) {
    tx->Close();
    rx->Close();
    GTEST_SKIP() << "broadcast send failed: " << sent.error;
  }
  auto r = rx->Receive(500);
  if (!r) {
    tx->Close();
    rx->Close();
    GTEST_SKIP() << "broadcast not delivered in this environment";
  }
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{5, 5}));
  tx->Close();
  rx->Close();
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/udp/UdpImpl.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/udp/udp_transport_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 `UdpImpl.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/udp/UdpImpl.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// UdpImpl.hpp — UDP 传输实现（单类处理单播/组播/广播）
// 实现 IUdpTransport；组合 TransportCore（无 framer，每个 datagram 即一条 Message）。
// 自有 io_context + 1 后台 io 线程；Open() 按 mode 配置 socket。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/udp/IUdpTransport.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class UdpImpl : public IUdpTransport,
                public std::enable_shared_from_this<UdpImpl> {
 public:
  explicit UdpImpl(UdpConfig config);
  ~UdpImpl() override;

  Status Open() override;   // 按 mode 配置 socket，启动接收循环
  void Close() override;
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;             // 默认目的地
  Status SendTo(const std::vector<uint8_t>& data,
                const std::string& ip, uint16_t port) override;       // 运行期目的地

  // 接收侧：转发给 core_
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }
  void SetCodec(std::shared_ptr<ICodec> codec) override { core_.SetCodec(std::move(codec)); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void StartReceive();
  Status SendToEndpoint(const std::vector<uint8_t>& data,
                        const asio::ip::udp::endpoint& dest);

  UdpConfig config_;
  TransportCore core_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::udp::socket socket_;
  asio::ip::udp::endpoint default_dest_;
  asio::ip::udp::endpoint recv_from_;
  std::array<uint8_t, 65536> recv_buf_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/udp/UdpImpl.cpp`:

```cpp
#include "transport/udp/UdpImpl.hpp"

#include <utility>
#include <variant>

// UdpImpl.cpp — UDP 单播/组播/广播实现（见 UdpImpl.hpp）。
// 并发：自有 io_context + 1 io 线程；收/发都经 strand_ 串行化（async_receive_from
// 与 strand 上的同步 send_to 互不并发）。报文保边界 → 无分帧，每个 datagram 一条
// Message，经 core_.DeliverFrame 投递。

namespace transport {

UdpImpl::UdpImpl(UdpConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

UdpImpl::~UdpImpl() { Close(); }

bool UdpImpl::IsOpen() const { return open_.load(); }

Status UdpImpl::Open() {
  asio::error_code ec;
  socket_.open(asio::ip::udp::v4(), ec);
  if (ec) return Status::Fail("config: udp open: " + ec.message());

  if (config_.mode == UdpMode::kBroadcast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
    socket_.set_option(asio::socket_base::broadcast(true), ec);
    if (ec) return Status::Fail("config: broadcast option: " + ec.message());
  } else if (config_.mode == UdpMode::kMulticast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
  }

  // 绑定：组播绑 0.0.0.0 以收组；其余绑 local_addr。
  asio::ip::address local;
  if (config_.mode == UdpMode::kMulticast) {
    local = asio::ip::address_v4::any();
  } else {
    local = asio::ip::make_address(config_.local_addr, ec);
    if (ec) return Status::Fail("config: invalid local_addr");
  }
  socket_.bind(asio::ip::udp::endpoint(local, config_.local_port), ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());

  if (config_.mode == UdpMode::kMulticast) {
    auto group = asio::ip::make_address(config_.multicast_group, ec);
    if (ec) return Status::Fail("config: invalid multicast_group");
    socket_.set_option(asio::ip::multicast::join_group(group), ec);
    if (ec) return Status::Fail("config: join_group: " + ec.message());
    asio::error_code ig;
    socket_.set_option(asio::ip::multicast::hops(config_.ttl), ig);
    socket_.set_option(asio::ip::multicast::enable_loopback(true), ig);
    default_dest_ = asio::ip::udp::endpoint(group, config_.remote_port);
  } else if (!config_.remote_addr.empty()) {
    auto raddr = asio::ip::make_address(config_.remote_addr, ec);
    if (ec) return Status::Fail("config: invalid remote_addr");
    default_dest_ = asio::ip::udp::endpoint(raddr, config_.remote_port);
  }
  // 单播/广播 remote_addr 为空：default_dest_ 留默认（仅 SendTo/接收可用）。

  local_port_ = socket_.local_endpoint().port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartReceive(); });
  return Status::Success(std::monostate{});
}

void UdpImpl::StartReceive() {
  auto self = shared_from_this();
  socket_.async_receive_from(
      asio::buffer(recv_buf_), recv_from_,
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (!open_.load()) return;
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              core_.DeliverError("io: receive: " + ec.message());
              StartReceive();  // 单报文错误不致命，继续监听
              return;
            }
            std::string source = recv_from_.address().to_string() + ":" +
                                 std::to_string(recv_from_.port());
            std::vector<uint8_t> datagram(recv_buf_.begin(),
                                          recv_buf_.begin() + n);
            core_.DeliverFrame(std::move(datagram), source, "");
            StartReceive();
          }));
}

Status UdpImpl::SendToEndpoint(const std::vector<uint8_t>& data,
                               const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(enc.value));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);  // 在 strand 上同步发，避免与收竞争
    if (ec) core_.DeliverError("io: send: " + ec.message());
  });
  return Status::Success(std::monostate{});  // 已入队写出
}

Status UdpImpl::Send(const std::vector<uint8_t>& data) {
  return SendToEndpoint(data, default_dest_);
}

Status UdpImpl::SendTo(const std::vector<uint8_t>& data, const std::string& ip,
                       uint16_t port) {
  asio::error_code ec;
  auto addr = asio::ip::make_address(ip, ec);
  if (ec) return Status::Fail("config: invalid address");
  return SendToEndpoint(data, asio::ip::udp::endpoint(addr, port));
}

void UdpImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    socket_.close(ig);
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
cmake --build build -j && ctest --test-dir build --output-on-failure -R UdpTransport
```
Expected: `UdpTransport.UnicastSendReceive` / `SendToOverridesDefault` / `CodecAppliedBothDirections` / `SendBeforeOpenFails` PASS；`MulticastLoopbackOrSkip` / `BroadcastLoopbackOrSkip` 视环境 PASS 或 SKIP（不得 FAIL）。全量套件保持绿色。

- [ ] **Step 7: 提交**

```bash
git add include/transport/udp/UdpImpl.hpp src/udp/UdpImpl.cpp \
        tests/udp/udp_transport_test.cpp CMakeLists.txt
git commit -m "feat: UdpImpl 单播/组播/广播（组合 TransportCore，真实回环+优雅跳过）"
```

---

## Task 4: UDP 收尾验证 + README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 干净全量构建 + UDP 套件稳定性**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
for i in $(seq 1 10); do ctest --test-dir build -R 'Udp' --output-on-failure || break; done
```
Expected: 全部 PASS/SKIP；UDP 套件 10 次稳定（单播/SendTo/codec 不 flaky；组播/广播 PASS 或一致 SKIP）。

- [ ] **Step 2: 更新 README 状态**

把 `README.md` 「状态」小节的 UDP 一行改为完成：

```markdown
- [x] Foundation：核心接口、分帧、接收交付、传输基类
- [x] TCP（client / server）
- [x] UDP（单播 / 组播 / 广播）
- [ ] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置
```

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README 标记 UDP 完成"
```

---

## 后续（不在本计划范围）

串口（主 spec §7）、DDS（§8）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。DDS 实现时同样组合 `TransportCore`（并使用 `IDdsProvider`）。
