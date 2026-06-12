# Endpoint 统一寻址发送 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ITransport` 增加 `Send(data, Endpoint)` 统一寻址发送(基类默认实现),删除 `IUdpTransport`(整个接口)与 `IDdsTransport::Send(data, topic)`,基类句柄即可按目的地发送。

**Architecture:** 新值类型 `Endpoint`(Default/Net/Topic 三种 kind,命名工厂);`ITransport::Send(data, Endpoint)` 非纯虚——kDefault 退化调 `Send(data)`,其余 Fail;UdpImpl/DdsImpl 覆写。破坏性变更:`IUdpTransport` 删除(`UdpImpl` 直接继承 `ITransport`,Factory 签名改 `shared_ptr<ITransport>`)、`IDdsTransport::Send(data,topic)` 删除(改 DdsImpl 私有 `SendToTopic`)。

**Tech Stack:** C++17;依赖全部 vendored(third_party/);GoogleTest;测试经 `ctest`。

**Spec:** `docs/superpowers/specs/2026-06-12-endpoint-send-design.md`

**构建命令(全程用这个,离线零拉取):**
```bash
cmake -S /home/ubuntu/david/transport -B /home/ubuntu/david/transport/build -DTRANSPORT_BUILD_TESTS=ON
cmake --build /home/ubuntu/david/transport/build -j$(nproc)
ctest --test-dir /home/ubuntu/david/transport/build --output-on-failure
```

**错误前缀约定(贯穿全计划):** `config:` = 用法/配置错(给错 Endpoint 种类、socket 未开);`io:` = 该传输不支持寻址。`Status::Fail("...")` 构造失败,`Status::Success(std::monostate{})` 成功(或 `Ok()`,DDS 代码里已有)。

---

### Task 1: `Endpoint` 类型 + `ITransport::Send(data, Endpoint)` 基类默认实现

**Files:**
- Create: `include/transport/Endpoint.hpp`
- Modify: `include/transport/ITransport.hpp`
- Create: `tests/endpoint_send_test.cpp`
- Modify: `CMakeLists.txt`(测试源列表加一行)

- [ ] **Step 1: 写失败测试**

创建 `tests/endpoint_send_test.cpp`:

```cpp
// -----------------------------------------------------------------------------
// endpoint_send_test.cpp — Endpoint 统一寻址发送
// 覆盖:基类默认实现(kDefault 退化/其余 io: Fail)、UDP kNet、DDS kTopic、
// 基类句柄多态寻址、具体类型上重载不被名字隐藏。
// -----------------------------------------------------------------------------

#include "transport/Endpoint.hpp"
#include "transport/ITransport.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace transport;

namespace {
void WaitFor(const std::function<bool()>& pred, int ms = 2000) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
  while (!pred() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(Endpoint, FactoriesProduceCorrectKinds) {
  auto d = Endpoint::Default();
  EXPECT_EQ(d.kind, Endpoint::Kind::kDefault);
  auto n = Endpoint::Net("10.0.0.7", 9000);
  EXPECT_EQ(n.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(n.host, "10.0.0.7");
  EXPECT_EQ(n.port, 9000);
  auto t = Endpoint::Topic("cmd");
  EXPECT_EQ(t.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(t.topic, "cmd");
}

// TCP 不覆写寻址重载:kNet/kTopic → io: Fail;kDefault 经真实回环退化等价 Send(data)。
TEST(EndpointSend, TcpDefaultDegradesAndAddressedFails) {
  TcpServerConfig sc;
  sc.bind_addr = "127.0.0.1";
  sc.port = 0;
  auto server = std::make_shared<TcpServerImpl>(sc);
  std::atomic<int> conns{0};
  std::shared_ptr<ITransport> server_side;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    server_side = std::move(c);
    ++conns;
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = std::make_shared<TcpClientImpl>(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  // kDefault → 退化 Send(data),消息真实送达
  ASSERT_TRUE(static_cast<bool>(client->Send({1, 2, 3}, Endpoint::Default())));
  auto r = server_side->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));

  // 寻址种类 → io: Fail(基类默认实现)
  auto st_net = client->Send({1}, Endpoint::Net("127.0.0.1", 1));
  EXPECT_FALSE(static_cast<bool>(st_net));
  EXPECT_EQ(st_net.error.rfind("io:", 0), 0u);
  auto st_topic = client->Send({1}, Endpoint::Topic("x"));
  EXPECT_FALSE(static_cast<bool>(st_topic));
  EXPECT_EQ(st_topic.error.rfind("io:", 0), 0u);

  client->Close();
  server->Close();
}

// 基类句柄上的两个重载均可见可调(接口层面,不依赖具体传输)
TEST(EndpointSend, BothOverloadsCallableViaBaseHandle) {
  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = 1;  // 不连接,只验证可调性
  cc.auto_reconnect = false;
  std::shared_ptr<ITransport> t = std::make_shared<TcpClientImpl>(cc);
  EXPECT_FALSE(static_cast<bool>(t->Send({1})));                     // 未打开
  EXPECT_FALSE(static_cast<bool>(t->Send({1}, Endpoint::Topic("x"))));
}
```

- [ ] **Step 2: 在 CMakeLists.txt 测试源列表加入新文件**

`CMakeLists.txt` 中 `add_executable(transport_tests` 块,在 `tests/version_test.cpp` 之后加一行:

```cmake
    tests/endpoint_send_test.cpp
```

- [ ] **Step 3: 构建确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -5`
Expected: 编译错误 —— `transport/Endpoint.hpp: No such file or directory`。

- [ ] **Step 4: 创建 `include/transport/Endpoint.hpp`**

```cpp
#pragma once

// -----------------------------------------------------------------------------
// Endpoint.hpp — 统一寻址发送的中立目的地值类型
// 三种 kind:kDefault(用 config 默认目的地)/ kNet(UDP ip:port)/ kTopic(DDS)。
// 命名工厂 Default()/Net()/Topic() 让调用点自解释;与接收侧 Message.source/topic
// 对称——收到消息可据其构造 Endpoint 直接回发。零第三方依赖。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>

namespace transport {

struct Endpoint {
  enum class Kind { kDefault, kNet, kTopic };

  Kind kind = Kind::kDefault;
  std::string host;     // kNet: ip
  uint16_t port{0};     // kNet
  std::string topic;    // kTopic

  static Endpoint Default() { return {}; }
  static Endpoint Net(std::string ip, uint16_t p) {
    Endpoint e;
    e.kind = Kind::kNet;
    e.host = std::move(ip);
    e.port = p;
    return e;
  }
  static Endpoint Topic(std::string name) {
    Endpoint e;
    e.kind = Kind::kTopic;
    e.topic = std::move(name);
    return e;
  }
};

}  // namespace transport
```

- [ ] **Step 5: `ITransport.hpp` 加寻址重载(带默认实现)**

`include/transport/ITransport.hpp`:

① include 区加(`#include "transport/ICodec.hpp"` 之前):

```cpp
#include "transport/Endpoint.hpp"
```

② 在 `virtual Status Send(const std::vector<uint8_t>& data) = 0;` 之后加:

```cpp
  // 统一寻址发送(非纯虚,基类默认实现):
  //   kDefault → 退化调 Send(data);其余 kind → Fail("io: addressed send not supported")。
  // UdpImpl 覆写支持 kNet,DdsImpl 覆写支持 kTopic;TCP/串口继承默认行为。
  virtual Status Send(const std::vector<uint8_t>& data, const Endpoint& to) {
    if (to.kind == Endpoint::Kind::kDefault) return Send(data);
    return Status::Fail("io: addressed send not supported");
  }
```

③ 头注释第一段「发送(Send)」改为「发送(Send / Send+Endpoint 统一寻址)」。

- [ ] **Step 6: 给只覆写单参 Send 的实现类加 `using ITransport::Send;` 防名字隐藏**

C++ 规则:派生类声明任一 `Send` 覆写即隐藏基类其余 `Send` 重载(具体类型句柄上编译失败)。在以下 4 个类的 public 段(各自 `Status Send(...)` 声明上一行)加 `using ITransport::Send;`:

- `include/transport/tcp/TcpConnectionImpl.hpp`
- `include/transport/tcp/TcpClientImpl.hpp`
- `include/transport/tcp/TcpServerImpl.hpp`
- `include/transport/serial/SerialImpl.hpp`

(UdpImpl/DdsImpl 在 Task 2/3 覆写双参时一并处理。)

- [ ] **Step 7: 构建 + 运行新测试确认通过**

Run: `cmake --build build -j$(nproc) && ctest --test-dir build -R 'Endpoint' --output-on-failure`
Expected: `Endpoint.FactoriesProduceCorrectKinds`、`EndpointSend.TcpDefaultDegradesAndAddressedFails`、`EndpointSend.BothOverloadsCallableViaBaseHandle` PASS。

- [ ] **Step 8: 全量回归**

Run: `ctest --test-dir build --output-on-failure | tail -3`
Expected: `100% tests passed`(116 + 3 = 119)。

- [ ] **Step 9: Commit**

```bash
git add include/transport/Endpoint.hpp include/transport/ITransport.hpp \
        include/transport/tcp/TcpConnectionImpl.hpp include/transport/tcp/TcpClientImpl.hpp \
        include/transport/tcp/TcpServerImpl.hpp include/transport/serial/SerialImpl.hpp \
        tests/endpoint_send_test.cpp CMakeLists.txt
git commit -m "feat: Endpoint 类型 + ITransport::Send(data,Endpoint) 统一寻址(基类默认实现)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: UDP 迁移 —— 删除 `IUdpTransport`,`UdpImpl` 覆写 Endpoint 发送

**Files:**
- Delete: `include/transport/udp/IUdpTransport.hpp`
- Modify: `include/transport/udp/UdpImpl.hpp`
- Modify: `src/udp/UdpImpl.cpp`
- Modify: `include/transport/TransportFactory.hpp`、`src/TransportFactory.cpp`
- Modify: `tests/endpoint_send_test.cpp`(追加 UDP 用例)
- Modify: `tests/udp/udp_transport_test.cpp`、`tests/udp/udp_interfaces_test.cpp`

- [ ] **Step 1: 追加失败测试到 `tests/endpoint_send_test.cpp`**

文件顶部 include 区加:

```cpp
#include "transport/udp/UdpImpl.hpp"
```

文件末尾追加:

```cpp
namespace {
UdpConfig UnicastCfg(const std::string& remote, uint16_t rport) {
  UdpConfig c;
  c.local_addr = "127.0.0.1";
  c.local_port = 0;
  c.remote_addr = remote;
  c.remote_port = rport;
  return c;
}
}  // namespace

// 基类句柄 + Endpoint::Net 寻址,真实回环送达 —— 本设计的核心收益
TEST(EndpointSend, UdpNetAddressingViaBaseHandle) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  std::shared_ptr<ITransport> tx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(
      tx->Send({4, 5}, Endpoint::Net("127.0.0.1", rport))));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 5}));
  tx->Close();
  rx->Close();
}

TEST(EndpointSend, UdpRejectsTopicEndpoint) {
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", 9999));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1}, Endpoint::Topic("x"));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(EndpointSend, UdpDefaultEndpointUsesConfigRemote) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rx->LocalPort()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Send({7}, Endpoint::Default())));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7}));
  tx->Close();
  rx->Close();
}
```

- [ ] **Step 2: 构建确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -5`
Expected: 编译错误 —— `UdpImpl` 无 `Send(data, Endpoint)` 覆写时,基类默认实现对 kNet 返回 Fail → `UdpNetAddressingViaBaseHandle` 运行期 FAIL;(若 UdpImpl 已有 using 缺失则为编译错)。构建若通过则跑 `ctest -R EndpointSend` 确认 `UdpNetAddressingViaBaseHandle` FAIL。

- [ ] **Step 3: 删除 `IUdpTransport`,改 `UdpImpl` 继承与声明**

```bash
git rm include/transport/udp/IUdpTransport.hpp
```

`include/transport/udp/UdpImpl.hpp`:

① include 区:删 `#include "transport/udp/IUdpTransport.hpp"`,加 `#include "transport/ITransport.hpp"`;
② 类声明改:

```cpp
class UdpImpl : public ITransport,
                public std::enable_shared_from_this<UdpImpl> {
```

③ 替换 Send 声明区(原 `Status Send(...) override;` 与 `Status SendTo(...) override;` 两行)为:

```cpp
  using ITransport::Send;
  Status Send(const std::vector<uint8_t>& data) override;             // 默认目的地
  Status Send(const std::vector<uint8_t>& data,
              const Endpoint& to) override;                           // kNet 运行期目的地
```

④ 头注释「实现 IUdpTransport」改「实现 ITransport(Endpoint::Net 运行期寻址)」。

- [ ] **Step 4: `src/udp/UdpImpl.cpp` 用 Endpoint 覆写替换 SendTo**

替换现有 `Status UdpImpl::SendTo(...)` 函数体为:

```cpp
Status UdpImpl::Send(const std::vector<uint8_t>& data, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return Send(data);
    case Endpoint::Kind::kNet: {
      asio::error_code ec;
      auto addr = asio::ip::make_address(to.host, ec);
      if (ec) return Status::Fail("config: invalid address");
      return SendToEndpoint(data, asio::ip::udp::endpoint(addr, to.port));
    }
    case Endpoint::Kind::kTopic:
      return Status::Fail("config: udp expects net endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}
```

(私有 `SendToEndpoint(data, asio endpoint)` 保留不动。)

- [ ] **Step 5: Factory 签名改 `shared_ptr<ITransport>`**

`include/transport/TransportFactory.hpp`:
- 删 `#include "transport/udp/IUdpTransport.hpp"`(`UdpConfig.hpp` include 保留);
- `static std::shared_ptr<IUdpTransport> Create(const UdpConfig& config);` → `static std::shared_ptr<ITransport> Create(const UdpConfig& config);`

`src/TransportFactory.cpp` 对应定义同步改返回类型:

```cpp
std::shared_ptr<ITransport> TransportFactory::Create(const UdpConfig& config) {
  return std::make_shared<UdpImpl>(config);
}
```

- [ ] **Step 6: 既有 UDP 测试改写**

`tests/udp/udp_transport_test.cpp`:
- `TEST(UdpTransport, SendToOverridesDefault)` 内 `tx->SendTo({4, 5}, "127.0.0.1", rport)` → `tx->Send({4, 5}, Endpoint::Net("127.0.0.1", rport))`,测试名改 `EndpointNetOverridesDefault`;
- `TEST(UdpTransport, MulticastLoopbackOrSkip)` 内 `m->SendTo({7, 8, 9}, "239.255.0.1", port)` → `m->Send({7, 8, 9}, Endpoint::Net("239.255.0.1", port))`;
- include 区加 `#include "transport/Endpoint.hpp"`(若 `using namespace transport;` 已有则 `Endpoint::Net` 直接可用)。

`tests/udp/udp_interfaces_test.cpp`:
- `#include "transport/udp/IUdpTransport.hpp"` → `#include "transport/ITransport.hpp"` 与 `#include "transport/udp/UdpImpl.hpp"`;
- `TEST(UdpConfig, IUdpTransportIsTransport)` 整个替换为:

```cpp
TEST(UdpConfig, UdpImplIsTransport) {
  EXPECT_TRUE((std::is_base_of<transport::ITransport,
                               transport::UdpImpl>::value));
}
```

- [ ] **Step 7: 构建 + UDP/Endpoint/Factory 套件全过**

Run: `cmake --build build -j$(nproc) && ctest --test-dir build -R 'Endpoint|Udp|Factory' --output-on-failure | tail -5`
Expected: 全 PASS(含新 3 个 EndpointSend.Udp* 用例)。

- [ ] **Step 8: 全量回归**

Run: `ctest --test-dir build --output-on-failure | tail -3`
Expected: `100% tests passed`(119 + 3 = 122)。

- [ ] **Step 9: Commit**

```bash
git add -A include src tests
git commit -m "feat!: 删除 IUdpTransport,UdpImpl 以 Endpoint::Net 统一寻址(Factory 返回 ITransport)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: DDS 迁移 —— 删除 `IDdsTransport::Send(data, topic)`,`DdsImpl` 覆写 Endpoint 发送

**Files:**
- Modify: `include/transport/dds/IDdsTransport.hpp`
- Modify: `include/transport/dds/DdsImpl.hpp`、`src/dds/DdsImpl.cpp`
- Modify: `tests/endpoint_send_test.cpp`(追加 DDS 用例)
- Modify: `tests/dds/dds_impl_pubsub_test.cpp`、`tests/dds/dds_impl_reqresp_test.cpp`

- [ ] **Step 1: 追加失败测试到 `tests/endpoint_send_test.cpp`**

include 区加:

```cpp
#include "transport/dds/DdsImpl.hpp"
#include "tests/dds/FakeDdsProvider.hpp"  // 路径按仓库实际:tests/dds/ 下的 fake;若为
                                          // #include "FakeDdsProvider.hpp" + include dir,
                                          // 照搬 dds_impl_pubsub_test.cpp 顶部的写法
```

(**执行者注意**:`FakeDdsProvider` 的 include 写法与构造方式**必须照搬 `tests/dds/dds_impl_pubsub_test.cpp` 顶部**——它已经在用同一 fake,有现成的 `Make(bus, cfg)`/`PubSubCfg` 辅助;把该文件 1-55 行的辅助函数复制过来或等价重写。)

文件末尾追加:

```cpp
// 基类句柄 + Endpoint::Topic 寻址,经 FakeDdsProvider 总线送达订阅方
TEST(EndpointSend, DdsTopicAddressingViaBaseHandle) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<ITransport> tx = MakeDds(bus, DdsPubSubCfg({"a"}));
  auto rx = MakeDds(bus, DdsPubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({9}, Endpoint::Topic("b"))));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{9}));
}

TEST(EndpointSend, DdsRejectsNetEndpoint) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = MakeDds(bus, DdsPubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(t->Open()));
  auto st = t->Send({1}, Endpoint::Net("127.0.0.1", 1));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(EndpointSend, DdsDefaultEndpointUsesFirstTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = MakeDds(bus, DdsPubSubCfg({"t0"}));
  auto rx = MakeDds(bus, DdsPubSubCfg({"t0"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t0")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({3}, Endpoint::Default())));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.topic, "t0");
}
```

(`MakeDds`/`DdsPubSubCfg` 即照搬 pubsub 测试里 `Make`/`PubSubCfg` 的实现,换名避免与本文件其他辅助冲突。)

- [ ] **Step 2: 构建+运行确认失败**

Run: `cmake --build build -j$(nproc) && ctest --test-dir build -R 'EndpointSend.Dds' --output-on-failure | tail -5`
Expected: `DdsTopicAddressingViaBaseHandle` FAIL(基类默认实现对 kTopic 返回 io: Fail —— DdsImpl 尚未覆写)。

- [ ] **Step 3: `IDdsTransport.hpp` 删除 Send(data, topic)**

删除这两行(`---- pub-sub ----` 注释段内):

```cpp
  using ITransport::Send;  // 保留基类 Send(data)（发往默认 topic）
  virtual Status Send(const std::vector<uint8_t>& data,
                      const std::string& topic) = 0;
```

头注释「多 topic pub-sub」描述保留(Subscribe 等仍在);补一句「按 topic 发送统一走 `ITransport::Send(data, Endpoint::Topic(...))`」。

- [ ] **Step 4: `DdsImpl` 覆写 Endpoint 发送,原双参 Send 转私有 `SendToTopic`**

`include/transport/dds/DdsImpl.hpp`:

① `Status Send(const std::vector<uint8_t>& data) override;  // → topics[0]` 处改为:

```cpp
  using ITransport::Send;
  Status Send(const std::vector<uint8_t>& data) override;  // → topics[0]
  Status Send(const std::vector<uint8_t>& data,
              const Endpoint& to) override;                // kTopic 寻址
```

② 「IDdsTransport — pub-sub」段删掉 `Status Send(const std::vector<uint8_t>& data, const std::string& topic) override;` 一行;
③ private 段加:

```cpp
  Status SendToTopic(const std::vector<uint8_t>& data, const std::string& topic);
```

`src/dds/DdsImpl.cpp`:

① 原 `Status DdsImpl::Send(const std::vector<uint8_t>& data, const std::string& topic)` 改签名为 `Status DdsImpl::SendToTopic(const std::vector<uint8_t>& data, const std::string& topic)`(函数体不动);
② `Status DdsImpl::Send(const std::vector<uint8_t>& data)` 函数体改 `return SendToTopic(data, config_.topics[0]);`
③ 紧随其后新增:

```cpp
Status DdsImpl::Send(const std::vector<uint8_t>& data, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return Send(data);
    case Endpoint::Kind::kTopic:
      return SendToTopic(data, to.topic);
    case Endpoint::Kind::kNet:
      return Status::Fail("config: dds expects topic endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}
```

- [ ] **Step 5: 既有 DDS 测试改写**

`tests/dds/dds_impl_pubsub_test.cpp`(`TEST(DdsPubSub, SendToSpecificTopicAndMultiTopicRouting)` 内):

```cpp
  ASSERT_TRUE(static_cast<bool>(tx->Send({1}, Endpoint::Topic("a"))));
  ASSERT_TRUE(static_cast<bool>(tx->Send({2}, Endpoint::Topic("b"))));
```

`tests/dds/dds_impl_reqresp_test.cpp`(`ModeConstraintRejectsPubSubMethods` 内):

```cpp
  EXPECT_EQ(t->Send({1}, Endpoint::Topic("x")).error.rfind("config:", 0), 0u);
```

两文件 include 区各加 `#include "transport/Endpoint.hpp"`(有 `using namespace transport;` 即可直接用)。

- [ ] **Step 6: 构建 + DDS/Endpoint 套件全过**

Run: `cmake --build build -j$(nproc) && ctest --test-dir build -R 'Endpoint|Dds' --output-on-failure | tail -5`
Expected: 全 PASS(含 FastDDS 8 个用例——本机已装 Fast DDS)。

- [ ] **Step 7: 全量回归**

Run: `ctest --test-dir build --output-on-failure | tail -3`
Expected: `100% tests passed`(122 + 3 = 125)。

- [ ] **Step 8: Commit**

```bash
git add -A include src tests
git commit -m "feat!: 删除 IDdsTransport::Send(data,topic),DdsImpl 以 Endpoint::Topic 统一寻址

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 文档同步(README / 主 spec / as-built 架构)

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`
- Modify: `docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`

- [ ] **Step 1: README 同步**

- L27 特点行:`**UDP 运行期指定目的地** \`SendTo\`` → `**统一寻址发送** \`Send(data, Endpoint)\`(UDP \`Endpoint::Net\` / DDS \`Endpoint::Topic\`,基类句柄即可寻址)`;
- L38 表格行「多协议、一套用法」:`I*Transport\`(如 \`ITcpServer.OnNewConnection\`、\`IUdpTransport.SendTo\`)` → `I*Transport\`(如 \`ITcpServer.OnNewConnection\`、\`IDdsTransport.SendRequest\`);一次性寻址发送统一为 \`Send(data, Endpoint)\``;
- UDP 用法示例 L164:`udp->SendTo({4, 5, 6}, "10.0.0.7", 7000);   // 运行期指定目的地,忽略默认 remote` → `udp->Send({4, 5, 6}, Endpoint::Net("10.0.0.7", 7000));  // 运行期指定目的地,忽略默认 remote`;
- DDS 用法示例若有 `Send(data, "topic")` 形态,改 `Send(data, Endpoint::Topic("topic"))`(执行时 grep 确认);
- TransportFactory 一节若展示 `Create(UdpConfig)` 返回 `IUdpTransport`,改 `ITransport`。

- [ ] **Step 2: 主 spec 同步(`2026-06-09-transport-middleware-design.md`)**

- §3 类图(L161 附近)`IUdpTransport` 类框删除,`UdpImpl --|> ITransport`;`ITransport` 框加 `+Send(data, Endpoint)`;
- L353 `Send` 语义段:末句「运行期指定目的地见 §6(UDP \`SendTo\`)、§7(DDS \`Send(data, topic)\`)」→「运行期指定目的地统一用 \`Send(data, Endpoint)\`(UDP \`Endpoint::Net\`、DDS \`Endpoint::Topic\`,见 §5.x Endpoint)」,并在 §5(公共类型)补 `Endpoint` 定义(从 `include/transport/Endpoint.hpp` 复制);
- §6 UDP(L450-456):`IUdpTransport` 接口代码块与 `SendTo` 描述替换为「UDP 无专属扩展接口;运行期寻址 \`Send(data, Endpoint::Net(ip, port))\`」;
- §7 DDS:`IDdsTransport` 代码块删 `using ITransport::Send;` 与双参 `Send`;补注「按 topic 发送走 \`Endpoint::Topic\`」;
- §9 Factory(L676):「返回各传输的最具体接口,以便访问 \`SendTo\`/按 topic \`Send\` 等专属方法」→「返回各传输的最具体接口(UDP 已无专属扩展,返回 \`ITransport\`);寻址发送统一 \`Send(data, Endpoint)\`」,`Create(UdpConfig)` 签名同步;
- §13 测试(L850):「\`SendTo\` 动态目的地」→「\`Endpoint::Net\` 动态目的地」;L880 示例代码 `udp->SendTo(payload, "10.0.0.7", 6000);` → `udp->Send(payload, Endpoint::Net("10.0.0.7", 6000));`;
- changelog 加一条:`2026-06-12:Endpoint 统一寻址发送 —— ITransport 加 Send(data, Endpoint) 默认实现;删除 IUdpTransport(掏空)与 IDdsTransport::Send(data,topic)(破坏性)。理由:基类句柄无法寻址发送 + 各传输寻址 API 形态不一。详见 specs/2026-06-12-endpoint-send-design.md`。

- [ ] **Step 3: as-built 架构 spec 同步(`2026-06-10-foundation-tcp-architecture.md`)**

- 类图(L131-134):删 `IUdpTransport` 框(`+SendTo(data,ip,port)`),`UdpImpl` 改直接 `--|> ITransport`,其方法行 `+Open() +Close() +Send() +SendTo()` → `+Open() +Close() +Send() +Send(data,Endpoint)`;`ITransport` 框加 `+Send(data, Endpoint)`;
- L235 关系清单:`**\`IUdpTransport\` ──▷ \`ITransport\`**:UDP 扩展接口(加 \`SendTo\`...)` 行删除,补 `**\`UdpImpl\` ──▷ \`ITransport\`**:直接实现(Endpoint::Net 运行期寻址)`;
- L239 组合优于继承段:把「`IUdpTransport`(=`ITransport`+`SendTo`)」措辞改为「`ITransport`」,该段菱形论证结论不变(执行者改写为通顺中文即可,核心是删去对已不存在接口的引用)。

- [ ] **Step 4: 验证文档无残留旧 API 引用**

Run: `grep -rn 'SendTo\|IUdpTransport' README.md docs/superpowers/specs/ | grep -v '2026-06-12-endpoint-send-design\|udp-transport-design\|plans/'`
Expected: 无输出(历史 plan/旧实现 spec 是过程记录,不改;两个 2026-06-12 endpoint 文档本身提及旧 API 属设计陈述,排除项已涵盖)。
(注:`DdsImpl.cpp`/测试里的 `SendToTopic`、UdpImpl 私有 `SendToEndpoint` 是内部名,不在本 grep 范围,无需处理。)

- [ ] **Step 5: 最终全量回归 + Commit**

Run: `ctest --test-dir build --output-on-failure | tail -3`
Expected: `100% tests passed ... out of 125`。

```bash
git add README.md docs/superpowers/specs/
git commit -m "docs: Endpoint 统一寻址同步 README/主 spec/as-built(删 IUdpTransport 与 DDS 双参 Send 文档面)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
