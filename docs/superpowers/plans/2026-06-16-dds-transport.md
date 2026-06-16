# DDS 底层(pub-sub 字节传输 + provider 抽象)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在纯字节管道架构上补 DDS:`DdsTransport`(实现 `IDdsTransport : ITransport` + `Subscribe/Unsubscribe`)以统一 `ITransport` 用法做 pub-sub;真实 DDS 经 `IDdsProvider` 隔离(`FakeDdsProvider` 进程内总线默认/测试,`FastDdsProvider` 可选 `find_package`)。只 pub-sub,req-resp 留 System。

**Architecture:** `DdsTransport` 持有一个 `IDdsProvider`,`Send(bytes,Endpoint::Topic)→Publish`、`Subscribe(topic)→provider.Subscribe`、样本到达时**直接在 provider 的 listener 线程**调 `OnBytes(bytes, from=topic)`(同 topic 有序、跨 topic 并发)。`FakeDdsProvider` 用进程内总线让同进程同 domain 两个实例互通,DDS 逻辑零外部依赖可全测;`FastDdsProvider` 每 topic 懒建 writer/reader + 极简"只携带 `[]byte`"的自定义 `TopicDataType`,`find_package` 探测到才编译,未装构建照常绿。

**Tech Stack:** C++17;Standalone Asio(已 vendored,本件不强依赖);GoogleTest 1.14(已 vendored);Fast DDS 2.13+(可选,`find_package(fastrtps/fastcdr)`,不内置);不抛异常。

**配套 spec:** `docs/superpowers/specs/2026-06-16-dds-transport-design.md`

---

## 文件结构

**新建:**
- `include/transport/dds/DdsConfig.hpp` — DdsQos + DdsConfig。
- `include/transport/dds/IDdsProvider.hpp` — provider 抽象(pub-sub only)。
- `include/transport/dds/IDdsTransport.hpp` — `ITransport` + Subscribe/Unsubscribe。
- `include/transport/dds/DdsProviderRegistry.hpp` + `src/dds/DdsProviderRegistry.cpp` — name→工厂。
- `include/transport/dds/FakeDdsProvider.hpp` + `src/dds/FakeDdsProvider.cpp` — 进程内总线 provider。
- `include/transport/dds/DdsTransport.hpp` + `src/dds/DdsTransport.cpp` — 主类 + RegisterBuiltinProviders。
- `src/dds/FastDdsRawType.hpp` + `src/dds/FastDdsRawType.cpp` — 极简 `[]byte` TopicDataType(仅 FastDDS 编入)。
- `src/dds/FastDdsProvider.hpp` + `src/dds/FastDdsProvider.cpp` — Fast DDS 2.13 provider(仅 FastDDS 编入)。
- 测试:`tests/dds/dds_registry_test.cpp`、`tests/dds/fake_dds_provider_test.cpp`、`tests/dds/dds_transport_test.cpp`、`tests/dds/fastdds_provider_test.cpp`(仅 FastDDS)。

**修改:** `CMakeLists.txt`(DDS 核心源无条件编入;`find_package` 探测到则编 `FastDds*`+链接+加 FastDDS 测试)。

---

## Task 1: 类型 + 接口 + provider 注册表

**Files:** Create `include/transport/dds/DdsConfig.hpp`、`IDdsProvider.hpp`、`IDdsTransport.hpp`、`DdsProviderRegistry.hpp`、`src/dds/DdsProviderRegistry.cpp`;Test `tests/dds/dds_registry_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 写 `include/transport/dds/DdsConfig.hpp`**
```cpp
#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct DdsQos {
  enum class Reliability { kBestEffort, kReliable };
  enum class Durability  { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability  durability  = Durability::kVolatile;
  uint32_t    history_depth = 10;
};

struct DdsConfig {
  int         domain_id = 0;
  std::string default_topic;      // Send(bytes) 无 endpoint 时的目的 topic
  std::string provider = "fake";  // registry 名;真实互通用 "fastdds"
  DdsQos      qos;
};

}  // namespace transport
```

- [ ] **Step 2: 写 `include/transport/dds/IDdsProvider.hpp`**
```cpp
#pragma once

// IDdsProvider.hpp — 底层 DDS 库抽象(只管「按 topic 收发不透明字节」)。
// 实现:FakeDdsProvider(进程内总线)、FastDdsProvider(真实,可选)。

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;

  virtual Status Init(const DdsConfig& config) = 0;
  virtual void   Shutdown() = 0;

  virtual Status Publish(const std::string& topic,
                         const std::vector<uint8_t>& bytes) = 0;
  // cb 在样本到达时被 provider 调用(FastDDS=该 topic 的 listener 线程;Fake=发布线程)。
  virtual Status Subscribe(const std::string& topic,
                           std::function<void(const std::vector<uint8_t>&)> cb) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  virtual std::string Name() const = 0;
};

}  // namespace transport
```

- [ ] **Step 3: 写 `include/transport/dds/IDdsTransport.hpp`**
```cpp
#pragma once

// IDdsTransport.hpp — 在纯管道 ITransport 上加 DDS 的订阅能力(pub-sub)。

#include <string>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

namespace transport {

class IDdsTransport : public ITransport {
 public:
  virtual Status Subscribe(const std::string& topic) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `include/transport/dds/DdsProviderRegistry.hpp`**
```cpp
#pragma once

// DdsProviderRegistry.hpp — provider 注册表(name → 工厂)。

#include <functional>
#include <memory>
#include <string>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class DdsProviderRegistry {
 public:
  using Factory = std::function<std::unique_ptr<IDdsProvider>()>;
  static void RegisterProvider(const std::string& name, Factory factory);
  static std::unique_ptr<IDdsProvider> Create(const std::string& name);  // 未注册→nullptr
};

}  // namespace transport
```

- [ ] **Step 5: 写失败测试** `tests/dds/dds_registry_test.cpp`:
```cpp
#include "transport/dds/DdsProviderRegistry.hpp"

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsProviderRegistry;
using transport::IDdsProvider;
using transport::Status;

namespace {
class StubProvider : public IDdsProvider {
 public:
  Status Init(const DdsConfig&) override { return Status::Success(std::monostate{}); }
  void   Shutdown() override {}
  Status Publish(const std::string&, const std::vector<uint8_t>&) override {
    return Status::Success(std::monostate{});
  }
  Status Subscribe(const std::string&,
                   std::function<void(const std::vector<uint8_t>&)>) override {
    return Status::Success(std::monostate{});
  }
  Status Unsubscribe(const std::string&) override { return Status::Success(std::monostate{}); }
  std::string Name() const override { return "stub"; }
};
}  // namespace

TEST(DdsProviderRegistry, RegisterAndCreate) {
  DdsProviderRegistry::RegisterProvider(
      "stub-x", [] { return std::make_unique<StubProvider>(); });
  auto p = DdsProviderRegistry::Create("stub-x");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->Name(), "stub");
}

TEST(DdsProviderRegistry, UnknownReturnsNull) {
  EXPECT_EQ(DdsProviderRegistry::Create("no-such-provider"), nullptr);
}
```
需要 `#include <variant>` 吗?`std::monostate` 在 `<variant>`,但 `Result.hpp` 已包含;为稳妥在测试顶部加 `#include <variant>`。把测试加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`。

- [ ] **Step 6: 运行,确认失败** `cd /home/ubuntu/david/transport && cmake --build build -j$(nproc) 2>&1 | head -20`(build 不存在先 `cmake -S . -B build >/dev/null`)。Expected: 找不到 `DdsProviderRegistry.hpp`。

- [ ] **Step 7: 写 `src/dds/DdsProviderRegistry.cpp`**
```cpp
#include "transport/dds/DdsProviderRegistry.hpp"

#include <map>
#include <mutex>
#include <utility>

// DdsProviderRegistry.cpp — 静态注册表(函数局部 static 保证初始化顺序安全)。

namespace transport {

namespace {
std::mutex& RegistryMutex() { static std::mutex m; return m; }
std::map<std::string, DdsProviderRegistry::Factory>& RegistryMap() {
  static std::map<std::string, DdsProviderRegistry::Factory> m;
  return m;
}
}  // namespace

void DdsProviderRegistry::RegisterProvider(const std::string& name, Factory factory) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  RegistryMap()[name] = std::move(factory);
}

std::unique_ptr<IDdsProvider> DdsProviderRegistry::Create(const std::string& name) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  auto it = RegistryMap().find(name);
  if (it == RegistryMap().end()) return nullptr;
  return it->second();
}

}  // namespace transport
```
把 `src/dds/DdsProviderRegistry.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 8: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R DdsProviderRegistry 2>&1 | grep -iE "passed|failed"`。Expected: 2 个用例通过。

- [ ] **Step 9: 提交**
```bash
git add include/transport/dds/DdsConfig.hpp include/transport/dds/IDdsProvider.hpp include/transport/dds/IDdsTransport.hpp include/transport/dds/DdsProviderRegistry.hpp src/dds/DdsProviderRegistry.cpp tests/dds/dds_registry_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DDS 类型/接口/注册表(DdsConfig/IDdsProvider/IDdsTransport/DdsProviderRegistry)"
```

---

## Task 2: `FakeDdsProvider`(进程内总线)

**Files:** Create `include/transport/dds/FakeDdsProvider.hpp`、`src/dds/FakeDdsProvider.cpp`;Test `tests/dds/fake_dds_provider_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 写失败测试** `tests/dds/fake_dds_provider_test.cpp`:
```cpp
#include "transport/dds/FakeDdsProvider.hpp"

#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::FakeDdsProvider;

namespace {
DdsConfig Cfg(int domain) { DdsConfig c; c.domain_id = domain; return c; }
}  // namespace

TEST(FakeDdsProvider, SharedBusDeliversAcrossProviders) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus), rx(bus);
  ASSERT_TRUE(static_cast<bool>(tx.Init(Cfg(0))));
  ASSERT_TRUE(static_cast<bool>(rx.Init(Cfg(0))));

  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(
      rx.Subscribe("t", [&](const std::vector<uint8_t>& b) { got = b; })));
  ASSERT_TRUE(static_cast<bool>(tx.Publish("t", {1, 2, 3})));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(FakeDdsProvider, UnsubscribeStopsDelivery) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus), rx(bus);
  (void)tx.Init(Cfg(0)); (void)rx.Init(Cfg(0));
  int count = 0;
  (void)rx.Subscribe("t", [&](const std::vector<uint8_t>&) { ++count; });
  (void)tx.Publish("t", {1});
  EXPECT_EQ(count, 1);
  (void)rx.Unsubscribe("t");
  (void)tx.Publish("t", {2});
  EXPECT_EQ(count, 1);  // 不再投递
}

TEST(FakeDdsProvider, StaticBusIsolatesByDomain) {
  // 默认构造(无注入 Bus)→ Init 接入按 domain 的静态总线。
  FakeDdsProvider tx, rx_same, rx_other;
  (void)tx.Init(Cfg(7)); (void)rx_same.Init(Cfg(7)); (void)rx_other.Init(Cfg(8));
  int same = 0, other = 0;
  (void)rx_same.Subscribe("t", [&](const std::vector<uint8_t>&) { ++same; });
  (void)rx_other.Subscribe("t", [&](const std::vector<uint8_t>&) { ++other; });
  (void)tx.Publish("t", {1});
  EXPECT_EQ(same, 1);
  EXPECT_EQ(other, 0);  // 不同 domain 不可见
}
```
把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 2: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `FakeDdsProvider.hpp`。

- [ ] **Step 3: 写 `include/transport/dds/FakeDdsProvider.hpp`**
```cpp
#pragma once

// FakeDdsProvider.hpp — 进程内内存 topic 总线实现 IDdsProvider(零 FastDDS 依赖)。
// 多个 provider 共享一条 Bus 模拟多 participant 互通;Publish 同步分发。
// 两种获得方式:① 注入共享 Bus(测试 DI,完全隔离);② 默认构造→Init 接入按 domain 的静态 Bus。

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/Result.hpp"
#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class FakeDdsProvider : public IDdsProvider {
 public:
  using Sink = std::function<void(const std::vector<uint8_t>&)>;

  struct Bus {
    std::mutex m;
    uint64_t next_id = 1;
    std::map<std::string, std::map<uint64_t, Sink>> subs;  // topic → (id → sink)

    uint64_t Add(const std::string& topic, Sink sink) {
      std::lock_guard<std::mutex> lk(m);
      uint64_t id = next_id++;
      subs[topic][id] = std::move(sink);
      return id;
    }
    void Remove(const std::string& topic, uint64_t id) {
      std::lock_guard<std::mutex> lk(m);
      auto it = subs.find(topic);
      if (it != subs.end()) it->second.erase(id);
    }
    void Dispatch(const std::string& topic, const std::vector<uint8_t>& bytes) {
      std::vector<Sink> snapshot;
      {
        std::lock_guard<std::mutex> lk(m);
        auto it = subs.find(topic);
        if (it != subs.end())
          for (auto& kv : it->second) snapshot.push_back(kv.second);
      }
      for (auto& s : snapshot) s(bytes);  // 锁外同步分发
    }
  };

  FakeDdsProvider() = default;                                  // 默认:Init 接入静态 domain 总线
  explicit FakeDdsProvider(std::shared_ptr<Bus> bus) : bus_(std::move(bus)) {}  // DI

  Status Init(const DdsConfig& config) override;
  void   Shutdown() override;
  Status Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Status Subscribe(const std::string& topic, Sink cb) override;
  Status Unsubscribe(const std::string& topic) override;
  std::string Name() const override { return "fake"; }

 private:
  static std::shared_ptr<Bus> StaticBusForDomain(int domain);

  std::shared_ptr<Bus> bus_;
  std::mutex mine_m_;
  std::map<std::string, std::vector<uint64_t>> mine_;  // 本 provider 的订阅 id
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/dds/FakeDdsProvider.cpp`**
```cpp
#include "transport/dds/FakeDdsProvider.hpp"

#include <map>
#include <utility>
#include <variant>

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

std::shared_ptr<FakeDdsProvider::Bus> FakeDdsProvider::StaticBusForDomain(int domain) {
  static std::mutex m;
  static std::map<int, std::shared_ptr<Bus>> buses;
  std::lock_guard<std::mutex> lk(m);
  auto& b = buses[domain];
  if (!b) b = std::make_shared<Bus>();
  return b;
}

Status FakeDdsProvider::Init(const DdsConfig& config) {
  if (!bus_) bus_ = StaticBusForDomain(config.domain_id);  // 未注入 → 接入静态 domain 总线
  return Ok();
}

void FakeDdsProvider::Shutdown() {
  std::map<std::string, std::vector<uint64_t>> mine;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    mine.swap(mine_);
  }
  if (bus_)
    for (auto& kv : mine)
      for (auto id : kv.second) bus_->Remove(kv.first, id);
}

Status FakeDdsProvider::Publish(const std::string& topic,
                                const std::vector<uint8_t>& bytes) {
  if (!bus_) return Status::Fail("config: dds not initialized");
  bus_->Dispatch(topic, bytes);
  return Ok();
}

Status FakeDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  if (!bus_) return Status::Fail("config: dds not initialized");
  uint64_t id = bus_->Add(topic, std::move(cb));
  std::lock_guard<std::mutex> lk(mine_m_);
  mine_[topic].push_back(id);
  return Ok();
}

Status FakeDdsProvider::Unsubscribe(const std::string& topic) {
  std::vector<uint64_t> ids;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    auto it = mine_.find(topic);
    if (it != mine_.end()) { ids = it->second; mine_.erase(it); }
  }
  if (bus_)
    for (auto id : ids) bus_->Remove(topic, id);
  return Ok();
}

}  // namespace transport
```
把 `src/dds/FakeDdsProvider.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R FakeDdsProvider 2>&1 | grep -iE "passed|failed"`。Expected: 3 个用例通过。

- [ ] **Step 6: 提交**
```bash
git add include/transport/dds/FakeDdsProvider.hpp src/dds/FakeDdsProvider.cpp tests/dds/fake_dds_provider_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: FakeDdsProvider(进程内总线,DI 共享 Bus + 按 domain 静态总线)"
```

---

## Task 3: `DdsTransport`(主类 + 内建 provider 注册)

**Files:** Create `include/transport/dds/DdsTransport.hpp`、`src/dds/DdsTransport.cpp`;Test `tests/dds/dds_transport_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 写失败测试** `tests/dds/dds_transport_test.cpp`:
```cpp
#include "transport/dds/DdsTransport.hpp"
#include "transport/dds/FakeDdsProvider.hpp"

#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::Result;

namespace {
// 两个 DdsTransport 共享一条 Fake Bus(DI),完全隔离。
struct Pair {
  std::shared_ptr<FakeDdsProvider::Bus> bus = std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<DdsTransport> Make(DdsConfig cfg) {
    return std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
  }
};
DdsConfig Cfg(std::string def = "") { DdsConfig c; c.domain_id = 0; c.default_topic = std::move(def); return c; }
}  // namespace

TEST(DdsTransport, PublishSubscribeDelivery) {
  Pair p;
  auto tx = p.Make(Cfg()), rx = p.Make(Cfg());
  std::vector<uint8_t> got; std::string from;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (r) { got = r.value; from = f; }
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("cmd")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({4, 5, 6}, Endpoint::Topic("cmd"))));
  EXPECT_EQ(got, (std::vector<uint8_t>{4, 5, 6}));
  EXPECT_EQ(from, "cmd");
  tx->Close(); rx->Close();
}

TEST(DdsTransport, DefaultTopic) {
  Pair p;
  auto tx = p.Make(Cfg("telemetry")), rx = p.Make(Cfg("telemetry"));
  std::vector<uint8_t> got;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) { if (r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("telemetry")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({9})));  // 无 endpoint → default_topic
  EXPECT_EQ(got, (std::vector<uint8_t>{9}));
  tx->Close(); rx->Close();
}

TEST(DdsTransport, NetEndpointRejected) {
  Pair p;
  auto tx = p.Make(Cfg("d"));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1}, Endpoint::Net("127.0.0.1", 9000));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(DdsTransport, NoDefaultTopicFails) {
  Pair p;
  auto tx = p.Make(Cfg(""));  // 空 default_topic
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(DdsTransport, MultiTopicRouting) {
  Pair p;
  auto tx = p.Make(Cfg()), rx = p.Make(Cfg());
  std::vector<std::pair<std::string, std::vector<uint8_t>>> seen;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (r) seen.emplace_back(f, r.value);
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("a")));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({1}, Endpoint::Topic("a"))));
  ASSERT_TRUE(static_cast<bool>(tx->Send({2}, Endpoint::Topic("b"))));
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0].first, "a"); EXPECT_EQ(seen[0].second, (std::vector<uint8_t>{1}));
  EXPECT_EQ(seen[1].first, "b"); EXPECT_EQ(seen[1].second, (std::vector<uint8_t>{2}));
  tx->Close(); rx->Close();
}

TEST(DdsTransport, UnknownProviderFailsOpen) {
  DdsConfig cfg; cfg.provider = "no-such";
  auto t = std::make_shared<DdsTransport>(cfg);  // 不注入 → 走 registry
  auto st = t->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(DdsTransport, DefaultFakeProviderViaRegistry) {
  // 不注入 provider → Open 经 registry 建 "fake",接入按 domain 的静态总线。
  // 用独立 domain 避免与别的测试串扰。
  DdsConfig cfg; cfg.provider = "fake"; cfg.domain_id = 4242; cfg.default_topic = "x";
  auto tx = std::make_shared<DdsTransport>(cfg);
  auto rx = std::make_shared<DdsTransport>(cfg);
  std::vector<uint8_t> got;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) { if (r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("x")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({7})));
  EXPECT_EQ(got, (std::vector<uint8_t>{7}));
  tx->Close(); rx->Close();
}
```
把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 2: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `DdsTransport.hpp`。

- [ ] **Step 3: 写 `include/transport/dds/DdsTransport.hpp`**
```cpp
#pragma once

// DdsTransport.hpp — DDS pub-sub 字节传输(实现 IDdsTransport : ITransport)。
// 持有一个 IDdsProvider;Send(bytes,Endpoint::Topic)→Publish;Subscribe(topic)→provider.Subscribe;
// 样本到达直接在 provider 的 listener 线程调 OnBytes(bytes, from=topic)。须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

class DdsTransport : public IDdsTransport,
                     public std::enable_shared_from_this<DdsTransport> {
 public:
  // provider 为空 → Open() 经 registry 按 config.provider 建(默认路径);注入用于测试(DI)。
  explicit DdsTransport(DdsConfig config,
                        std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  Status SendToTopic(const std::string& topic, const std::vector<uint8_t>& bytes);

  DdsConfig config_;
  std::unique_ptr<IDdsProvider> provider_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex subs_m_;
  std::set<std::string> subscribed_;

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/dds/DdsTransport.cpp`**
```cpp
#include "transport/dds/DdsTransport.hpp"

#include <mutex>
#include <utility>
#include <variant>

#include "transport/dds/DdsProviderRegistry.hpp"
#include "transport/dds/FakeDdsProvider.hpp"

// DdsTransport.cpp — 见 .hpp。订阅回调捕获 weak_ptr 防引用环
// (provider→callback→weak(transport);transport→unique_ptr(provider))。
// OnBytes 直接在 provider listener 线程上调(同 topic 有序、跨 topic 并发)。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }

// 幂等注册内建 provider:"fake" 总注册;探测到 FastDDS 时注册 "fastdds"。
void RegisterBuiltinProviders() {
  static std::once_flag once;
  std::call_once(once, [] {
    DdsProviderRegistry::RegisterProvider(
        "fake", [] { return std::make_unique<FakeDdsProvider>(); });
    // FastDDS 注册在 Task 4 以 #ifdef TRANSPORT_HAS_FASTDDS 加入。
  });
}
}  // namespace

DdsTransport::DdsTransport(DdsConfig config, std::unique_ptr<IDdsProvider> provider)
    : config_(std::move(config)), provider_(std::move(provider)) {}

DdsTransport::~DdsTransport() { Close(); }

Status DdsTransport::Open() {
  if (open_.load()) return Ok();
  if (!provider_) {
    RegisterBuiltinProviders();
    provider_ = DdsProviderRegistry::Create(config_.provider);
    if (!provider_)
      return Status::Fail("config: provider not registered: " + config_.provider);
  }
  auto st = provider_->Init(config_);
  if (!st) return st;
  open_.store(true);
  if (connect_cb_) connect_cb_();
  return Ok();
}

void DdsTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  if (provider_) provider_->Shutdown();
}

Status DdsTransport::SendToTopic(const std::string& topic,
                                 const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  return provider_->Publish(topic, bytes);
}

Status DdsTransport::Send(const std::vector<uint8_t>& bytes) {
  if (config_.default_topic.empty())
    return Status::Fail("config: no default topic");
  return SendToTopic(config_.default_topic, bytes);
}

Status DdsTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault: return Send(bytes);
    case Endpoint::Kind::kTopic:   return SendToTopic(to.topic, bytes);
    case Endpoint::Kind::kNet:     return Status::Fail("config: dds expects topic endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

Status DdsTransport::Subscribe(const std::string& topic) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  std::weak_ptr<DdsTransport> wself = weak_from_this();
  auto st = provider_->Subscribe(
      topic, [wself, topic](const std::vector<uint8_t>& bytes) {
        auto s = wself.lock();
        if (!s) return;
        if (s->bytes_cb_)
          s->bytes_cb_(Result<std::vector<uint8_t>>::Success(bytes), topic);
      });
  if (st) {
    std::lock_guard<std::mutex> lk(subs_m_);
    subscribed_.insert(topic);
  }
  return st;
}

Status DdsTransport::Unsubscribe(const std::string& topic) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  auto st = provider_->Unsubscribe(topic);
  {
    std::lock_guard<std::mutex> lk(subs_m_);
    subscribed_.erase(topic);
  }
  return st;
}

}  // namespace transport
```
把 `src/dds/DdsTransport.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R DdsTransport 2>&1 | grep -iE "passed|failed"`。Expected: 7 个 `DdsTransport.*` 用例通过。

- [ ] **Step 6: 提交**
```bash
git add include/transport/dds/DdsTransport.hpp src/dds/DdsTransport.cpp tests/dds/dds_transport_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DdsTransport(pub-sub 字节传输,Send/Subscribe/OnBytes from=topic,内建 fake provider)"
```

---

## Task 4: `FastDdsProvider`(可选,find_package)

**Files:** Create `src/dds/FastDdsRawType.{hpp,cpp}`、`src/dds/FastDdsProvider.{hpp,cpp}`;Modify `src/dds/DdsTransport.cpp`(注册 fastdds)、`CMakeLists.txt`;Test `tests/dds/fastdds_provider_test.cpp`(仅 FastDDS)。

> 本机已装 Fast DDS 2.13(`find_package` 探测得到),故本任务可真实编译+运行。未装的机器上 `find_package` 失败 → 不编译 `FastDds*`、不加该测试,构建照常绿。

- [ ] **Step 1: CMake 探测 FastDDS + 条件编译**

在 `CMakeLists.txt` 中 `find_package(Threads REQUIRED)` 之后、`add_library` 之后(库目标已建好处)加入:
```cmake
find_package(fastcdr QUIET)
find_package(fastrtps QUIET)
if(fastrtps_FOUND AND fastcdr_FOUND)
  set(TRANSPORT_HAS_FASTDDS ON)
  message(STATUS "Fast DDS found: enabling FastDdsProvider")
  target_sources(transport PRIVATE src/dds/FastDdsRawType.cpp src/dds/FastDdsProvider.cpp)
  target_link_libraries(transport PUBLIC fastrtps fastcdr)
  target_compile_definitions(transport PUBLIC TRANSPORT_HAS_FASTDDS)
else()
  set(TRANSPORT_HAS_FASTDDS OFF)
  message(STATUS "Fast DDS NOT found: building without FastDdsProvider")
endif()
```
并在 `if(TRANSPORT_BUILD_TESTS)` 块内、`gtest_discover_tests(transport_tests)` 之前加:
```cmake
  if(TRANSPORT_HAS_FASTDDS)
    target_sources(transport_tests PRIVATE tests/dds/fastdds_provider_test.cpp)
  endif()
```

- [ ] **Step 2: 写 `src/dds/FastDdsRawType.hpp`**(极简,只携带 `[]byte`)
```cpp
#pragma once

// FastDdsRawType.hpp — "只携带 []byte" 的自定义 TopicDataType(Fast DDS 2.13)。
// 手写紧凑序列化(不经 CDR):payload 直接写入 SerializedPayload。版本敏感面之一。

#include <cstdint>
#include <vector>

#include <fastdds/dds/topic/TopicDataType.hpp>

namespace transport {

struct RawBytes { std::vector<uint8_t> payload; };

class FastDdsRawType : public eprosima::fastdds::dds::TopicDataType {
 public:
  FastDdsRawType();
  bool serialize(void* data,
                 eprosima::fastrtps::rtps::SerializedPayload_t* payload) override;
  bool deserialize(eprosima::fastrtps::rtps::SerializedPayload_t* payload,
                   void* data) override;
  std::function<uint32_t()> getSerializedSizeProvider(void* data) override;
  void* createData() override;
  void deleteData(void* data) override;
  bool getKey(void* data, eprosima::fastrtps::rtps::InstanceHandle_t* handle,
              bool force_md5) override;
};

}  // namespace transport
```

- [ ] **Step 3: 写 `src/dds/FastDdsRawType.cpp`**
```cpp
#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;
}  // namespace

FastDdsRawType::FastDdsRawType() {
  setName("RawBytes");
  m_typeSize = kPreallocSize;
  m_isGetKeyDefined = false;
}

bool FastDdsRawType::serialize(
    void* data, eprosima::fastrtps::rtps::SerializedPayload_t* payload) {
  auto* msg = static_cast<RawBytes*>(data);
  const uint32_t total = static_cast<uint32_t>(msg->payload.size());
  if (total > payload->max_size) return false;
  if (total > 0) std::memcpy(payload->data, msg->payload.data(), total);
  payload->length = total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastrtps::rtps::SerializedPayload_t* payload, void* data) {
  auto* msg = static_cast<RawBytes*>(data);
  msg->payload.assign(payload->data, payload->data + payload->length);
  return true;
}

std::function<uint32_t()> FastDdsRawType::getSerializedSizeProvider(void* data) {
  auto* msg = static_cast<RawBytes*>(data);
  const uint32_t total = static_cast<uint32_t>(msg->payload.size());
  return [total]() { return total; };
}

void* FastDdsRawType::createData() { return new RawBytes(); }
void FastDdsRawType::deleteData(void* data) { delete static_cast<RawBytes*>(data); }

bool FastDdsRawType::getKey(void*, eprosima::fastrtps::rtps::InstanceHandle_t*, bool) {
  return false;
}

}  // namespace transport
```

- [ ] **Step 4: 写 `src/dds/FastDdsProvider.hpp`**
```cpp
#pragma once

// FastDdsProvider.hpp — IDdsProvider 的 Fast DDS 2.13 实现(pub-sub only)。
// participant + RawBytes 类型注册 + topic→writer/reader 懒加载 + DdsQos 映射。

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class FastDdsProvider : public IDdsProvider {
 public:
  FastDdsProvider();
  ~FastDdsProvider() override;

  Status Init(const DdsConfig& config) override;
  void   Shutdown() override;
  Status Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Status Subscribe(const std::string& topic,
                   std::function<void(const std::vector<uint8_t>&)> cb) override;
  Status Unsubscribe(const std::string& topic) override;
  std::string Name() const override { return "fastdds"; }

 private:
  class ReaderListener;  // on_data_available → take → sink
  eprosima::fastdds::dds::Topic* GetOrCreateTopic(const std::string& name);

  DdsConfig config_;
  eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
  eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
  eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
  eprosima::fastdds::dds::TypeSupport type_;

  std::mutex mutex_;
  std::map<std::string, eprosima::fastdds::dds::Topic*> topics_;
  std::map<std::string, eprosima::fastdds::dds::DataWriter*> writers_;
  struct ReaderEntry {
    eprosima::fastdds::dds::DataReader* reader = nullptr;
    std::unique_ptr<ReaderListener> listener;
  };
  std::map<std::string, ReaderEntry> readers_;
};

}  // namespace transport
```

- [ ] **Step 5: 写 `src/dds/FastDdsProvider.cpp`**
```cpp
#include "FastDdsProvider.hpp"

#include <utility>
#include <variant>

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastrtps/types/TypesBase.h>

#include "FastDdsRawType.hpp"

// FastDdsProvider.cpp — Fast DDS 2.13 provider(pub-sub only)。
// 线程:reader listener 回调来自 FastDDS 内部线程 → sink(=DdsTransport 的交付)须线程安全。

namespace transport {

namespace dds = eprosima::fastdds::dds;
using eprosima::fastrtps::types::ReturnCode_t;
using Sink = std::function<void(const std::vector<uint8_t>&)>;

namespace {
Status Ok() { return Status::Success(std::monostate{}); }

void ApplyQos(const DdsQos& q, dds::DataWriterQos* wqos, dds::DataReaderQos* rqos) {
  const auto rel = (q.reliability == DdsQos::Reliability::kReliable)
                       ? dds::RELIABLE_RELIABILITY_QOS : dds::BEST_EFFORT_RELIABILITY_QOS;
  const auto dur = (q.durability == DdsQos::Durability::kTransientLocal)
                       ? dds::TRANSIENT_LOCAL_DURABILITY_QOS : dds::VOLATILE_DURABILITY_QOS;
  if (wqos) {
    wqos->reliability().kind = rel; wqos->durability().kind = dur;
    wqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    wqos->history().depth = static_cast<int32_t>(q.history_depth);
    wqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
  if (rqos) {
    rqos->reliability().kind = rel; rqos->durability().kind = dur;
    rqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    rqos->history().depth = static_cast<int32_t>(q.history_depth);
    rqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
}
}  // namespace

class FastDdsProvider::ReaderListener : public dds::DataReaderListener {
 public:
  explicit ReaderListener(Sink sink) : sink_(std::move(sink)) {}
  void on_data_available(dds::DataReader* reader) override {
    RawBytes msg;
    dds::SampleInfo info;
    while (reader->take_next_sample(&msg, &info) == ReturnCode_t::RETCODE_OK) {
      if (info.valid_data) sink_(msg.payload);
    }
  }
 private:
  Sink sink_;
};

FastDdsProvider::FastDdsProvider() = default;
FastDdsProvider::~FastDdsProvider() { Shutdown(); }

Status FastDdsProvider::Init(const DdsConfig& config) {
  config_ = config;
  auto* factory = dds::DomainParticipantFactory::get_instance();
  participant_ = factory->create_participant(config.domain_id, dds::PARTICIPANT_QOS_DEFAULT);
  if (!participant_)
    return Status::Fail("io: create_participant failed (domain " +
                        std::to_string(config.domain_id) + ")");
  type_ = dds::TypeSupport(new FastDdsRawType());
  if (type_.register_type(participant_) != ReturnCode_t::RETCODE_OK)
    return Status::Fail("io: register_type RawBytes failed");
  publisher_ = participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
  subscriber_ = participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
  if (!publisher_ || !subscriber_)
    return Status::Fail("io: create publisher/subscriber failed");
  return Ok();
}

dds::Topic* FastDdsProvider::GetOrCreateTopic(const std::string& name) {
  auto it = topics_.find(name);
  if (it != topics_.end()) return it->second;
  dds::Topic* t = participant_->create_topic(name, "RawBytes", dds::TOPIC_QOS_DEFAULT);
  if (t) topics_[name] = t;
  return t;
}

Status FastDdsProvider::Publish(const std::string& topic,
                                const std::vector<uint8_t>& bytes) {
  dds::DataWriter* writer = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = writers_.find(topic);
    if (it != writers_.end()) {
      writer = it->second;
    } else {
      dds::Topic* t = GetOrCreateTopic(topic);
      if (!t) return Status::Fail("io: create_topic failed: " + topic);
      dds::DataWriterQos wqos = dds::DATAWRITER_QOS_DEFAULT;
      ApplyQos(config_.qos, &wqos, nullptr);
      writer = publisher_->create_datawriter(t, wqos, nullptr);
      if (!writer) return Status::Fail("io: create_datawriter failed: " + topic);
      writers_[topic] = writer;
    }
  }
  RawBytes copy; copy.payload = bytes;
  // Fast DDS 2.13.1 的 DataWriter::write(void*) 单参重载返回 bool(成功 true)。
  if (!writer->write(&copy)) return Status::Fail("io: write failed: " + topic);
  return Ok();
}

Status FastDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (readers_.count(topic)) return Ok();  // 幂等
  dds::Topic* t = GetOrCreateTopic(topic);
  if (!t) return Status::Fail("io: create_topic failed: " + topic);
  auto listener = std::make_unique<ReaderListener>(std::move(cb));
  dds::DataReaderQos rqos = dds::DATAREADER_QOS_DEFAULT;
  ApplyQos(config_.qos, nullptr, &rqos);
  dds::DataReader* reader = subscriber_->create_datareader(t, rqos, listener.get());
  if (!reader) return Status::Fail("io: create_datareader failed: " + topic);
  readers_[topic] = ReaderEntry{reader, std::move(listener)};
  return Ok();
}

Status FastDdsProvider::Unsubscribe(const std::string& topic) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = readers_.find(topic);
  if (it == readers_.end()) return Ok();
  subscriber_->delete_datareader(it->second.reader);
  readers_.erase(it);
  return Ok();
}

void FastDdsProvider::Shutdown() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (!participant_) return;
  for (auto& kv : readers_) subscriber_->delete_datareader(kv.second.reader);
  readers_.clear();
  for (auto& kv : writers_) publisher_->delete_datawriter(kv.second);
  writers_.clear();
  for (auto& kv : topics_) participant_->delete_topic(kv.second);
  topics_.clear();
  if (subscriber_) participant_->delete_subscriber(subscriber_);
  if (publisher_) participant_->delete_publisher(publisher_);
  dds::DomainParticipantFactory::get_instance()->delete_participant(participant_);
  participant_ = nullptr; publisher_ = nullptr; subscriber_ = nullptr;
}

}  // namespace transport
```

- [ ] **Step 6: 在 `src/dds/DdsTransport.cpp` 注册 "fastdds"**

在文件顶部 include 区(`#include "transport/dds/FakeDdsProvider.hpp"` 之后)加:
```cpp
#ifdef TRANSPORT_HAS_FASTDDS
#include "dds/FastDdsProvider.hpp"
#endif
```
把 `RegisterBuiltinProviders` 的 `std::call_once` lambda 内、`fake` 注册之后(替换原"FastDDS 注册在 Task 4..."注释)改为:
```cpp
    DdsProviderRegistry::RegisterProvider(
        "fake", [] { return std::make_unique<FakeDdsProvider>(); });
#ifdef TRANSPORT_HAS_FASTDDS
    DdsProviderRegistry::RegisterProvider(
        "fastdds", [] { return std::make_unique<FastDdsProvider>(); });
#endif
```

- [ ] **Step 7: 写互通测试** `tests/dds/fastdds_provider_test.cpp`(仅在 FastDDS 编入时加入构建):
```cpp
#include "transport/dds/DdsTransport.hpp"

#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::Result;

namespace {
DdsConfig RealCfg(int domain) {
  DdsConfig c; c.provider = "fastdds"; c.domain_id = domain; return c;
}
}  // namespace

TEST(FastDdsIntegration, PubSubRoundtrip) {
  // 用一个不常用的 domain 降低与本机其它 DDS 流量冲突的概率。
  auto rx = std::make_shared<DdsTransport>(RealCfg(71));
  auto tx = std::make_shared<DdsTransport>(RealCfg(71));

  auto ro = rx->Open();
  auto to = tx->Open();
  if (!ro || !to) GTEST_SKIP() << "FastDDS participant unavailable";

  std::mutex m; std::vector<uint8_t> got; std::string from;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (!r) return;
    std::lock_guard<std::mutex> lk(m); got = r.value; from = f;
  });
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("icalc")));

  // pub-sub 在发现完成前发的样本可能丢 → 重复发布直到收到或超时(应对发现时延)。
  bool received = false;
  for (int i = 0; i < 50 && !received; ++i) {
    ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3}, Endpoint::Topic("icalc"))));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::lock_guard<std::mutex> lk(m);
    received = !got.empty();
  }
  {
    std::lock_guard<std::mutex> lk(m);
    ASSERT_TRUE(received) << "no sample received within timeout";
    EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(from, "icalc");
  }
  tx->Close(); rx->Close();
}
```

- [ ] **Step 8: 干净构建 + 运行(本机有 FastDDS)** 
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build 2>&1 | grep -iE "Fast DDS"
cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings (none)---"
ctest --test-dir build -R "FastDdsIntegration|DdsTransport" 2>&1 | grep -iE "passed|failed"
```
Expected: 配置阶段打印 `Fast DDS found`;无告警;`FastDdsIntegration.PubSubRoundtrip` + 7 个 `DdsTransport.*` 通过(互通用例若本机 DDS 环境异常会 `GTEST_SKIP`,不算失败)。

- [ ] **Step 9: 提交**
```bash
git add src/dds/FastDdsRawType.hpp src/dds/FastDdsRawType.cpp src/dds/FastDdsProvider.hpp src/dds/FastDdsProvider.cpp src/dds/DdsTransport.cpp tests/dds/fastdds_provider_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: FastDdsProvider(可选,find_package;极简 []byte TopicDataType;pub-sub 互通)"
```

---

## Task 5: 全量验证

**Files:** 无(仅验证)

- [ ] **Step 1: 干净构建零告警**
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings (none)---"
```
Expected: 无 `warning`/`error:`。

- [ ] **Step 2: 全量测试连跑两次(查 flaky)**
```bash
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 两次都 `100% tests passed`(原 33 + registry 2 + Fake 3 + DdsTransport 7 + FastDDS 1 = 46;FastDDS 互通若 SKIP 仍算通过)。

- [ ] **Step 3: 解耦检查**
```bash
grep -rn "ICodec\|Message" include/transport/dds src/dds || echo "(DDS 层不依赖 codec/Message —— 解耦保持)"
```
Expected: 无输出(DDS 底层只进出裸字节)。

> 本任务无新增文件;若前序均已提交且工作树干净,无需额外提交。

---

## 完成标准
- `DdsTransport`(`IDdsTransport : ITransport` + Subscribe)落地:`Send(bytes,Endpoint::Topic)` 发布、`Subscribe(topic)`、`OnBytes(bytes,from=topic)` 收;`kNet`/空 default_topic/未注册 provider 各报 `config:`。
- provider 抽象:`IDdsProvider` + registry + `FakeDdsProvider`(进程内总线,DI + 静态 domain)+ `FastDdsProvider`(可选,find_package)。
- DDS 逻辑零外部依赖经 Fake 全测;FastDDS 互通在本机真实编译运行(未装机器跳过、构建照常绿)。
- 干净构建零告警;全量测试稳定通过;DDS 层不引入 codec/Message(解耦保持)。
- 范围外(未做):req-resp(System)、QoS 高级项、Message/codec 接入。
