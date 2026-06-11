# DDS 传输 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 DDS 传输：provider 无关的 `DdsImpl`（pub-sub 多 topic 路由 + req-resp 关联/超时，对 `FakeDdsProvider` 单测）+ `FastDdsProvider`（系统 Fast DDS 2.13，自定义 `TopicDataType` 绕过 IDL/CDR）+ 真实互通集成测试。

**Architecture:** `DdsImpl : public IDdsTransport` 组合 `TransportCore core_`，**provider 经构造注入**（默认从 `DdsProviderRegistry` 按名创建；测试注入 `FakeDdsProvider`）。全部业务逻辑（路由/关联/超时/codec/模式约束）在 `DdsImpl`；全部 FastDDS 调用与版本敏感面封在 `FastDdsProvider`+`FastDdsRawType`。req-resp 超时用自有 io_context + per-request `asio::steady_timer`，`pending_` 互斥「取出再执行」保证 `on_reply` 恰好一次。CMake 以 `TRANSPORT_HAS_FASTDDS` 门控 FastDDS 部分（find 不到仍可构建其余 + Fake 测试）。

**Tech Stack:** C++17、系统 Fast DDS 2.13.1（`find_package(fastrtps)`+`fastcdr`）、Standalone Asio（已集成）、GoogleTest 1.14。

**依据 spec：** `docs/superpowers/specs/2026-06-11-dds-transport-design.md`（含主 spec §7 机制与 wire layout）。

---

## 文件结构

```
include/transport/dds/
├── DdsConfig.hpp          # DdsMode + DdsQos + DdsConfig
├── RawMessage.hpp         # 承载类 {request_id, reply_topic, payload}
├── IDdsTransport.hpp      # 扩展接口
├── IDdsProvider.hpp       # provider 抽象
├── DdsProviderRegistry.hpp
└── DdsImpl.hpp
src/dds/
├── DdsProviderRegistry.cpp
├── DdsImpl.cpp
├── FastDdsRawType.hpp / .cpp     # [FastDDS 门控] TopicDataType（wire layout）
└── FastDdsProvider.hpp / .cpp    # [FastDDS 门控] IDdsProvider 实现
tests/dds/
├── dds_interfaces_test.cpp
├── FakeDdsProvider.hpp           # 测试件：进程内 topic 总线
├── dds_registry_test.cpp
├── dds_impl_pubsub_test.cpp
├── dds_impl_reqresp_test.cpp
├── fastdds_rawtype_test.cpp      # [FastDDS 门控]
└── fastdds_provider_test.cpp     # [FastDDS 门控] 真实互通
（修改：CMakeLists.txt、include/transport/core/TransportCore.hpp[Task 4 加 DecodeForReceive]、tests/core/transport_core_test.cpp）
```

---

## Task 1: DDS 配置与接口头（5 个头文件）

**Files:**
- Create: `include/transport/dds/DdsConfig.hpp`
- Create: `include/transport/dds/RawMessage.hpp`
- Create: `include/transport/dds/IDdsTransport.hpp`
- Create: `include/transport/dds/IDdsProvider.hpp`
- Create: `include/transport/dds/DdsProviderRegistry.hpp`
- Test: `tests/dds/dds_interfaces_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/dds/dds_interfaces_test.cpp`:

```cpp
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"
#include "transport/dds/RawMessage.hpp"

#include <type_traits>

#include <gtest/gtest.h>

TEST(DdsConfig, Defaults) {
  transport::DdsConfig c;
  EXPECT_TRUE(c.mode == transport::DdsMode::kPubSub);
  EXPECT_TRUE(c.topics.empty());
  EXPECT_EQ(c.domain_id, 0);
  EXPECT_TRUE(c.qos.reliability == transport::DdsQos::Reliability::kReliable);
  EXPECT_TRUE(c.qos.durability == transport::DdsQos::Durability::kVolatile);
  EXPECT_EQ(c.qos.history_depth, 10u);
  EXPECT_EQ(c.provider, "FastDDS");
}

TEST(DdsConfig, RawMessageDefaults) {
  transport::RawMessage m;
  EXPECT_TRUE(m.request_id.empty());
  EXPECT_TRUE(m.reply_topic.empty());
  EXPECT_TRUE(m.payload.empty());
}

TEST(DdsConfig, IDdsTransportIsTransport) {
  EXPECT_TRUE((std::is_base_of<transport::ITransport,
                               transport::IDdsTransport>::value));
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 追加 `tests/dds/dds_interfaces_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 DDS 头文件。

- [ ] **Step 4: 写五个头文件**

`include/transport/dds/DdsConfig.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// DdsConfig.hpp — DDS 配置（DdsMode + DdsQos + DdsConfig）
// DdsQos 为简化 QoS 结构（借鉴 Apollo Cyber RT）：可枚举、可校验、provider 无关，
// 由 provider 映射到底层 DDS QoS 策略。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

enum class DdsMode { kPubSub, kReqResp };

struct DdsQos {
  enum class Reliability { kReliable, kBestEffort };
  enum class Durability { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability durability = Durability::kVolatile;
  uint32_t history_depth = 10;  // KEEP_LAST depth；0 非法
};

struct DdsConfig {
  DdsMode mode = DdsMode::kPubSub;
  std::vector<std::string> topics;  // topics[0] 为 Send(data) 的默认 topic
  int domain_id = 0;                // 一个实例 = 一个 DomainParticipant
  DdsQos qos;                       // writer/reader 共用
  std::string provider = "FastDDS"; // 从 DdsProviderRegistry 选择
};

}  // namespace transport
```

`include/transport/dds/RawMessage.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// RawMessage.hpp — DDS 承载类（普通 C++ 类，非 IDL；provider 无关）
// pub-sub 与 req-resp 共用：request_id/reply_topic 是框架级关联信息（pub-sub 为
// 空），payload 是 ICodec.Encode 输出的原始字节。wire layout 见主 spec §7.2。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

class RawMessage {
 public:
  std::string request_id;   // req-resp 关联 id；pub-sub 为空
  std::string reply_topic;  // req-resp 回包 topic；pub-sub 为空
  std::vector<uint8_t> payload;
};

}  // namespace transport
```

`include/transport/dds/IDdsTransport.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// IDdsTransport.hpp — DDS 扩展接口（ITransport + 多 topic pub-sub + req-resp）
// 一个实例对应一个 DomainParticipant，内部懒加载维护多个 topic。实现见 DdsImpl。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsTransport : public ITransport {
 public:
  // ---- pub-sub ----
  using ITransport::Send;  // 保留基类 Send(data)（发往默认 topic）
  virtual Status Send(const std::vector<uint8_t>& data,
                      const std::string& topic) = 0;
  virtual Status Subscribe(const std::string& topic) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  // ---- req-resp 客户端 ----
  virtual Status SendRequest(const std::vector<uint8_t>& data,
                             const std::string& topic,
                             std::function<void(Result<Message>)> on_reply,
                             uint32_t timeout_ms = 5000) = 0;

  // ---- req-resp 响应端 ----
  using ReplyFn = std::function<Status(const std::vector<uint8_t>&)>;
  using RequestHandler =
      std::function<void(const Message& request, ReplyFn reply)>;
  virtual Status OnRequest(const std::string& topic,
                           RequestHandler handler) = 0;

  virtual DdsMode Mode() const = 0;
  virtual std::string Provider() const = 0;
};

}  // namespace transport
```

`include/transport/dds/IDdsProvider.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// IDdsProvider.hpp — 底层 DDS 库抽象（provider 只管「按 RawMessage 在 topic 上
// 收发字节」；关联/超时/codec 全在 DdsImpl 层）。实现：FastDdsProvider（真实）、
// FakeDdsProvider（测试，进程内总线）。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;

  // 建立 participant、注册 RawMessage 类型
  virtual Status Init(const DdsConfig& config) = 0;

  // ---- pub-sub ----
  virtual Status Publish(const std::string& topic,
                         const std::vector<uint8_t>& data) = 0;
  virtual Status Subscribe(const std::string& topic,
                           ITransport::ReceiveCallback cb) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  // ---- req-resp 客户端 ----
  virtual Status SendRequest(const std::string& request_topic,
                             const std::string& request_id,
                             const std::string& reply_topic,
                             const std::vector<uint8_t>& data) = 0;
  using ReplySink = std::function<void(const std::string& request_id,
                                       const std::vector<uint8_t>& payload)>;
  virtual Status SubscribeReplies(const std::string& reply_topic,
                                  ReplySink sink) = 0;

  // ---- req-resp 响应端 ----
  using RequestSink = std::function<void(const std::vector<uint8_t>& payload,
                                         const std::string& request_id,
                                         const std::string& reply_topic)>;
  virtual Status ServeRequests(const std::string& request_topic,
                               RequestSink sink) = 0;
  virtual Status Reply(const std::string& reply_topic,
                       const std::string& request_id,
                       const std::vector<uint8_t>& data) = 0;

  virtual void Shutdown() = 0;
  virtual std::string ProviderName() const = 0;
};

}  // namespace transport
```

`include/transport/dds/DdsProviderRegistry.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// DdsProviderRegistry.hpp — provider 注册表（name → 工厂）。FastDDS 在
// TRANSPORT_HAS_FASTDDS 时自动注册；接入其它 DDS 实现调 RegisterProvider 即可。
// -----------------------------------------------------------------------------

#include <functional>
#include <memory>
#include <string>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class DdsProviderRegistry {
 public:
  using Factory = std::function<std::unique_ptr<IDdsProvider>()>;

  static void RegisterProvider(const std::string& name, Factory factory);
  // 未注册返回 nullptr（调用方报 config: 错误）
  static std::unique_ptr<IDdsProvider> Create(const std::string& name);
};

}  // namespace transport
```

- [ ] **Step 5: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R DdsConfig`
Expected: `DdsConfig.*`（3 个）PASS；全量套件保持绿色。

- [ ] **Step 6: 提交**

```bash
git add include/transport/dds/ tests/dds/dds_interfaces_test.cpp CMakeLists.txt
git commit -m "feat: DDS 配置/RawMessage/IDdsTransport/IDdsProvider/Registry 接口头"
```

---

## Task 2: `DdsProviderRegistry` 实现 + `FakeDdsProvider` 测试件

**Files:**
- Create: `src/dds/DdsProviderRegistry.cpp`
- Create: `tests/dds/FakeDdsProvider.hpp`
- Test: `tests/dds/dds_registry_test.cpp`
- Modify: `CMakeLists.txt`（库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/dds/dds_registry_test.cpp`:

```cpp
#include "transport/dds/DdsProviderRegistry.hpp"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"

using transport::DdsProviderRegistry;
using transport::FakeDdsProvider;
using transport::IDdsProvider;

TEST(DdsRegistry, UnknownNameReturnsNull) {
  EXPECT_EQ(DdsProviderRegistry::Create("NoSuchProvider"), nullptr);
}

TEST(DdsRegistry, RegisterAndCreate) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  DdsProviderRegistry::RegisterProvider(
      "FakeForRegistryTest", [bus] { return std::make_unique<FakeDdsProvider>(bus); });
  auto p = DdsProviderRegistry::Create("FakeForRegistryTest");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "Fake");
}

TEST(DdsRegistry, FakeBusRoundtrip) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider a(bus), b(bus);

  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(
      b.Subscribe("t", [&](transport::Result<transport::Message> m) {
        if (m) got = m.value.payload;
      })));
  ASSERT_TRUE(static_cast<bool>(a.Publish("t", {1, 2, 3})));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));

  // req-resp 原语
  std::string seen_id, seen_reply_topic;
  std::vector<uint8_t> seen_req;
  ASSERT_TRUE(static_cast<bool>(b.ServeRequests(
      "calc_Request", [&](const std::vector<uint8_t>& p, const std::string& id,
                          const std::string& rt) {
        seen_req = p; seen_id = id; seen_reply_topic = rt;
      })));
  std::string reply_id;
  std::vector<uint8_t> reply_payload;
  ASSERT_TRUE(static_cast<bool>(a.SubscribeReplies(
      "calc_Reply", [&](const std::string& id, const std::vector<uint8_t>& p) {
        reply_id = id; reply_payload = p;
      })));
  ASSERT_TRUE(static_cast<bool>(
      a.SendRequest("calc_Request", "id-1", "calc_Reply", {9})));
  EXPECT_EQ(seen_req, (std::vector<uint8_t>{9}));
  EXPECT_EQ(seen_id, "id-1");
  EXPECT_EQ(seen_reply_topic, "calc_Reply");
  ASSERT_TRUE(static_cast<bool>(b.Reply("calc_Reply", "id-1", {8})));
  EXPECT_EQ(reply_id, "id-1");
  EXPECT_EQ(reply_payload, (std::vector<uint8_t>{8}));

  // Unsubscribe 生效
  got.clear();
  ASSERT_TRUE(static_cast<bool>(b.Unsubscribe("t")));
  a.Publish("t", {7});
  EXPECT_TRUE(got.empty());
}
```

- [ ] **Step 2: 写 `FakeDdsProvider.hpp`（测试件，header-only）**

`tests/dds/FakeDdsProvider.hpp`:

```cpp
#pragma once

// FakeDdsProvider — 进程内内存 topic 总线实现 IDdsProvider（测试件，零 FastDDS
// 依赖）。多个 provider 共享一条 Bus 模拟多 participant 互通；Publish 同步分发。

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/RawMessage.hpp"

namespace transport {

class FakeDdsProvider : public IDdsProvider {
 public:
  struct Bus {
    using RawSink = std::function<void(const RawMessage&)>;
    std::mutex m;
    uint64_t next_id = 1;
    std::map<std::string, std::map<uint64_t, RawSink>> subs;

    uint64_t Add(const std::string& topic, RawSink sink) {
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
    void Dispatch(const std::string& topic, const RawMessage& msg) {
      std::vector<RawSink> snapshot;
      {
        std::lock_guard<std::mutex> lk(m);
        auto it = subs.find(topic);
        if (it != subs.end())
          for (auto& kv : it->second) snapshot.push_back(kv.second);
      }
      for (auto& s : snapshot) s(msg);  // 锁外同步分发
    }
  };

  explicit FakeDdsProvider(std::shared_ptr<Bus> bus) : bus_(std::move(bus)) {}

  Status Init(const DdsConfig&) override { return Ok(); }

  Status Publish(const std::string& topic,
                 const std::vector<uint8_t>& data) override {
    RawMessage m; m.payload = data;
    bus_->Dispatch(topic, m);
    return Ok();
  }

  Status Subscribe(const std::string& topic,
                   ITransport::ReceiveCallback cb) override {
    uint64_t id = bus_->Add(topic, [cb, topic](const RawMessage& m) {
      Message msg;
      msg.payload = m.payload;
      msg.topic = topic;
      msg.source = topic;
      cb(Result<Message>::Success(std::move(msg)));
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[topic].push_back(id);
    return Ok();
  }

  Status Unsubscribe(const std::string& topic) override {
    std::vector<uint64_t> ids;
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      auto it = mine_.find(topic);
      if (it != mine_.end()) { ids = it->second; mine_.erase(it); }
    }
    for (auto id : ids) bus_->Remove(topic, id);
    return Ok();
  }

  Status SendRequest(const std::string& request_topic,
                     const std::string& request_id,
                     const std::string& reply_topic,
                     const std::vector<uint8_t>& data) override {
    RawMessage m; m.request_id = request_id; m.reply_topic = reply_topic;
    m.payload = data;
    bus_->Dispatch(request_topic, m);
    return Ok();
  }

  Status SubscribeReplies(const std::string& reply_topic,
                          ReplySink sink) override {
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      if (!reply_subscribed_.insert(reply_topic).second) return Ok();  // 幂等
    }
    uint64_t id = bus_->Add(reply_topic, [sink](const RawMessage& m) {
      sink(m.request_id, m.payload);
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[reply_topic].push_back(id);
    return Ok();
  }

  Status ServeRequests(const std::string& request_topic,
                       RequestSink sink) override {
    uint64_t id = bus_->Add(request_topic, [sink](const RawMessage& m) {
      sink(m.payload, m.request_id, m.reply_topic);
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[request_topic].push_back(id);
    return Ok();
  }

  Status Reply(const std::string& reply_topic, const std::string& request_id,
               const std::vector<uint8_t>& data) override {
    RawMessage m; m.request_id = request_id; m.payload = data;
    bus_->Dispatch(reply_topic, m);
    return Ok();
  }

  void Shutdown() override {
    std::map<std::string, std::vector<uint64_t>> mine;
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      mine.swap(mine_);
      reply_subscribed_.clear();
    }
    for (auto& kv : mine)
      for (auto id : kv.second) bus_->Remove(kv.first, id);
  }

  std::string ProviderName() const override { return "Fake"; }

 private:
  static Status Ok() { return Status::Success(std::monostate{}); }

  std::shared_ptr<Bus> bus_;
  std::mutex mine_m_;
  std::map<std::string, std::vector<uint64_t>> mine_;  // 本 provider 的订阅 id
  std::set<std::string> reply_subscribed_;
};

}  // namespace transport
```

- [ ] **Step 3: 把库源/测试源加入 CMake，运行确认失败→实现→通过**

在 `add_library(transport STATIC ...)` 追加 `src/dds/DdsProviderRegistry.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/dds/dds_registry_test.cpp`。

`src/dds/DdsProviderRegistry.cpp`:

```cpp
#include "transport/dds/DdsProviderRegistry.hpp"

#include <map>
#include <mutex>

// DdsProviderRegistry.cpp — 静态注册表（函数局部 static 保证初始化顺序安全）。

namespace transport {

namespace {
std::mutex& RegistryMutex() {
  static std::mutex m;
  return m;
}
std::map<std::string, DdsProviderRegistry::Factory>& RegistryMap() {
  static std::map<std::string, DdsProviderRegistry::Factory> m;
  return m;
}
}  // namespace

void DdsProviderRegistry::RegisterProvider(const std::string& name,
                                           Factory factory) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  RegistryMap()[name] = std::move(factory);
}

std::unique_ptr<IDdsProvider> DdsProviderRegistry::Create(
    const std::string& name) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  auto it = RegistryMap().find(name);
  if (it == RegistryMap().end()) return nullptr;
  return it->second();
}

}  // namespace transport
```

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R DdsRegistry`
Expected: `DdsRegistry.*`（3 个）PASS；全量绿色。

- [ ] **Step 4: 提交**

```bash
git add src/dds/DdsProviderRegistry.cpp tests/dds/FakeDdsProvider.hpp \
        tests/dds/dds_registry_test.cpp CMakeLists.txt
git commit -m "feat: DdsProviderRegistry + FakeDdsProvider 测试件（进程内 topic 总线）"
```

---

## Task 3: `DdsImpl` pub-sub（对 Fake）

**Files:**
- Create: `include/transport/dds/DdsImpl.hpp`
- Create: `src/dds/DdsImpl.cpp`
- Test: `tests/dds/dds_impl_pubsub_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/dds/dds_impl_pubsub_test.cpp`:

```cpp
#include "transport/dds/DdsImpl.hpp"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"
#include "transport/ICodec.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::Result;

namespace {

DdsConfig PubSubCfg(std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = DdsMode::kPubSub;
  c.topics = std::move(topics);
  return c;
}

std::shared_ptr<DdsImpl> Make(std::shared_ptr<FakeDdsProvider::Bus> bus,
                              DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}

class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto o = d; for (auto& b : o) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto o = d; for (auto& b : o) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
};

}  // namespace

TEST(DdsPubSub, SendGoesToDefaultTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t0"}));
  auto rx = Make(bus, PubSubCfg({"t0"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t0")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2})));  // 默认 topic = topics[0]
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2}));
  EXPECT_EQ(r.value.topic, "t0");
  EXPECT_EQ(r.value.source, "t0");
}

TEST(DdsPubSub, SendToSpecificTopicAndMultiTopicRouting) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"a"}));
  auto rx = Make(bus, PubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("a")));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1}, "a")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({2}, "b")));
  auto r1 = rx->Receive(1000);
  auto r2 = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r1));
  ASSERT_TRUE(static_cast<bool>(r2));
  EXPECT_EQ(r1.value.topic, "a");
  EXPECT_EQ(r1.value.payload, (std::vector<uint8_t>{1}));
  EXPECT_EQ(r2.value.topic, "b");
  EXPECT_EQ(r2.value.payload, (std::vector<uint8_t>{2}));
}

TEST(DdsPubSub, UnsubscribeStopsDelivery) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t"}));
  auto rx = Make(bus, PubSubCfg({"t"}));
  tx->Open(); rx->Open();
  rx->Subscribe("t");
  ASSERT_TRUE(static_cast<bool>(rx->Unsubscribe("t")));
  tx->Send({9});
  auto r = rx->Receive(50);
  EXPECT_FALSE(static_cast<bool>(r));  // timeout:
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsPubSub, CodecAppliedBothDirections) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t"}));
  auto rx = Make(bus, PubSubCfg({"t"}));
  tx->SetCodec(std::make_shared<ShiftCodec>());
  rx->SetCodec(std::make_shared<ShiftCodec>());
  tx->Open(); rx->Open();
  rx->Subscribe("t");

  tx->Send({1, 2, 3});                    // Encode +1 → 总线上 {2,3,4}
  auto r = rx->Receive(1000);             // Decode -1 → {1,2,3}
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(DdsPubSub, ModeConstraintRejectsReqRespMethods) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = Make(bus, PubSubCfg({"t"}));
  t->Open();
  auto st = t->SendRequest({1}, "t", [](Result<transport::Message>) {}, 1000);
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  auto st2 = t->OnRequest("t", [](const transport::Message&,
                                  transport::IDdsTransport::ReplyFn) {});
  EXPECT_FALSE(static_cast<bool>(st2));
  EXPECT_EQ(st2.error.rfind("config:", 0), 0u);
}

TEST(DdsPubSub, OpenValidations) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  // 空 topics
  auto a = Make(bus, PubSubCfg({}));
  auto st = a->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  // history_depth = 0
  auto cfg = PubSubCfg({"t"});
  cfg.qos.history_depth = 0;
  auto b = Make(bus, cfg);
  auto st2 = b->Open();
  EXPECT_FALSE(static_cast<bool>(st2));
  // 未注册 provider（不注入）
  DdsConfig c3 = PubSubCfg({"t"});
  c3.provider = "NoSuch";
  auto d = std::make_shared<DdsImpl>(c3);
  auto st3 = d->Open();
  EXPECT_FALSE(static_cast<bool>(st3));
  EXPECT_EQ(st3.error.rfind("config:", 0), 0u);
}
```

- [ ] **Step 2: 把库源/测试源加入 CMake**

`add_library` 追加 `src/dds/DdsImpl.cpp`；`add_executable` 追加 `tests/dds/dds_impl_pubsub_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 `DdsImpl.hpp`。

- [ ] **Step 4: 写头文件（含 req-resp 成员声明，Task 4 填实现）**

`include/transport/dds/DdsImpl.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// DdsImpl.hpp — DDS 传输实现（IDdsTransport；provider 无关）
// 组合 TransportCore；provider 经构造注入（默认从 DdsProviderRegistry 创建）。
// pub-sub 路由 + req-resp 关联/超时 + codec 边界 + 模式约束全部在本层；
// 底层 DDS 调用全部经 IDdsProvider。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

class DdsImpl : public IDdsTransport,
                public std::enable_shared_from_this<DdsImpl> {
 public:
  explicit DdsImpl(DdsConfig config,
                   std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsImpl() override;

  // ITransport
  Status Open() override;
  void Close() override;
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;  // → topics[0]

  // 接收侧：转发 core_（仅 kPubSub 交付订阅消息）
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

  // IDdsTransport — pub-sub
  Status Send(const std::vector<uint8_t>& data, const std::string& topic) override;
  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  // IDdsTransport — req-resp
  Status SendRequest(const std::vector<uint8_t>& data, const std::string& topic,
                     std::function<void(Result<Message>)> on_reply,
                     uint32_t timeout_ms) override;
  Status OnRequest(const std::string& topic, RequestHandler handler) override;

  DdsMode Mode() const override { return config_.mode; }
  std::string Provider() const override;

 private:
  struct Pending {
    std::function<void(Result<Message>)> on_reply;
    std::shared_ptr<asio::steady_timer> timer;
  };

  Status RequireOpen() const;
  Status RequireMode(DdsMode m) const;
  std::string NextRequestId();
  void HandleReply(const std::string& request_id,
                   const std::vector<uint8_t>& payload);

  DdsConfig config_;
  std::unique_ptr<IDdsProvider> provider_;
  TransportCore core_;

  // req-resp 状态（mutex_ 保护）
  std::mutex mutex_;
  std::map<std::string, Pending> pending_;
  std::set<std::string> reply_subscribed_;
  uint64_t request_seq_ = 0;
  std::string request_prefix_;  // 构造时随机，保证跨实例/进程 id 唯一

  // 超时 timer 用 io 线程（与其它实现的「每实例一 io 线程」模式一致）
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  std::thread io_thread_;

  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
};

}  // namespace transport
```

- [ ] **Step 5: 写实现（本任务实现 pub-sub + 生命周期 + 模式约束；req-resp 四方法本任务给出完整代码——与 Task 4 联测）**

`src/dds/DdsImpl.cpp`（完整文件）:

```cpp
#include "transport/dds/DdsImpl.hpp"

#include <chrono>
#include <random>
#include <utility>
#include <variant>

#include "transport/dds/DdsProviderRegistry.hpp"

// DdsImpl.cpp — DDS 传输实现（见 DdsImpl.hpp）。
// 线程：provider 回调来自其内部线程（FastDDS listener / Fake 调用线程），进
// core_(ReceiveQueue 自带锁) 或 pending_(mutex_) 后即返回；req-resp 超时 timer
// 跑在自有 io 线程。pending_ 「取出再执行」：reply 与超时竞争同一条目，先取到者
// 兑现 on_reply，后到者发现条目不在即放弃 —— 保证恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

DdsImpl::DdsImpl(DdsConfig config, std::unique_ptr<IDdsProvider> provider)
    : config_(std::move(config)),
      provider_(std::move(provider)),
      guard_(ctx_.get_executor()) {
  std::random_device rd;
  request_prefix_ = std::to_string(rd()) + "-";
  io_thread_ = std::thread([this] { ctx_.run(); });
}

DdsImpl::~DdsImpl() { Close(); }

bool DdsImpl::IsOpen() const { return open_.load(); }

Status DdsImpl::Open() {
  if (config_.topics.empty())
    return Status::Fail("config: topics must not be empty");
  if (config_.qos.history_depth == 0)
    return Status::Fail("config: history_depth must be > 0");
  if (!provider_) {
    provider_ = DdsProviderRegistry::Create(config_.provider);
    if (!provider_)
      return Status::Fail("config: provider not registered: " +
                          config_.provider);
  }
  auto st = provider_->Init(config_);
  if (!st) return st;
  open_.store(true);
  return Ok();
}

Status DdsImpl::RequireOpen() const {
  if (!open_.load()) return Status::Fail("config: transport not open");
  return Ok();
}

Status DdsImpl::RequireMode(DdsMode m) const {
  if (config_.mode != m)
    return Status::Fail("config: method not available in this mode");
  return Ok();
}

// ---------- pub-sub ----------

Status DdsImpl::Send(const std::vector<uint8_t>& data) {
  return Send(data, config_.topics[0]);
}

Status DdsImpl::Send(const std::vector<uint8_t>& data,
                     const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  return provider_->Publish(topic, enc.value);
}

Status DdsImpl::Subscribe(const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  // 注意：provider 长期持有订阅回调——捕获 weak_ptr，避免
  // DdsImpl → provider_ → callback → shared_ptr<DdsImpl> 引用环（泄漏）。
  std::weak_ptr<DdsImpl> wself = shared_from_this();
  return provider_->Subscribe(topic, [wself, topic](Result<Message> m) {
    auto self = wself.lock();
    if (!self) return;
    if (!m) {
      self->core_.DeliverError(m.error);
      return;
    }
    // provider 给的是未解码 payload；DeliverFrame 内按 codec Decode
    self->core_.DeliverFrame(std::move(m.value.payload), topic, topic);
  });
}

Status DdsImpl::Unsubscribe(const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  return provider_->Unsubscribe(topic);
}

// ---------- req-resp ----------

std::string DdsImpl::NextRequestId() {
  std::lock_guard<std::mutex> lk(mutex_);
  return request_prefix_ + std::to_string(++request_seq_);
}

void DdsImpl::HandleReply(const std::string& request_id,
                          const std::vector<uint8_t>& payload) {
  Pending entry;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = pending_.find(request_id);
    if (it == pending_.end()) return;  // 不认识的 id（他人请求/已超时）：忽略
    entry = std::move(it->second);
    pending_.erase(it);
  }
  entry.timer->cancel();
  auto dec = core_.DecodeForReceive(payload);
  if (!dec) {
    entry.on_reply(Result<Message>::Fail(dec.error));
    return;
  }
  Message msg;
  msg.payload = std::move(dec.value);
  msg.timestamp = TransportCore::NowMicros();
  entry.on_reply(Result<Message>::Success(std::move(msg)));
}

Status DdsImpl::SendRequest(const std::vector<uint8_t>& data,
                            const std::string& topic,
                            std::function<void(Result<Message>)> on_reply,
                            uint32_t timeout_ms) {
  if (auto st = RequireMode(DdsMode::kReqResp); !st) return st;
  if (auto st = RequireOpen(); !st) return st;

  const std::string request_topic = topic + "_Request";
  const std::string reply_topic = topic + "_Reply";
  auto self = shared_from_this();
  std::weak_ptr<DdsImpl> wself = self;  // 长期回调用 weak，避免引用环

  // 幂等订阅 reply topic
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (reply_subscribed_.insert(reply_topic).second) {
      auto st = provider_->SubscribeReplies(
          reply_topic, [wself](const std::string& id,
                               const std::vector<uint8_t>& payload) {
            if (auto self = wself.lock()) self->HandleReply(id, payload);
          });
      if (!st) {
        reply_subscribed_.erase(reply_topic);
        return st;
      }
    }
  }

  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);

  const std::string id = NextRequestId();
  auto timer = std::make_shared<asio::steady_timer>(ctx_);
  timer->expires_after(std::chrono::milliseconds(timeout_ms));
  {
    std::lock_guard<std::mutex> lk(mutex_);
    pending_[id] = Pending{std::move(on_reply), timer};
  }
  timer->async_wait([self, id](asio::error_code ec) {
    if (ec) return;  // 被取消（reply 已到）
    Pending entry;
    {
      std::lock_guard<std::mutex> lk(self->mutex_);
      auto it = self->pending_.find(id);
      if (it == self->pending_.end()) return;  // reply 赢了
      entry = std::move(it->second);
      self->pending_.erase(it);
    }
    entry.on_reply(Result<Message>::Fail("timeout: request timed out"));
  });

  // 注意：登记 pending 之后再发布——Fake 总线同步分发，reply 可能在本调用内到达
  return provider_->SendRequest(request_topic, id, reply_topic, enc.value);
}

Status DdsImpl::OnRequest(const std::string& topic, RequestHandler handler) {
  if (auto st = RequireMode(DdsMode::kReqResp); !st) return st;
  if (auto st = RequireOpen(); !st) return st;

  // 长期持有的 sink 与可能被用户长期保存的 ReplyFn 均捕获 weak_ptr，避免引用环。
  std::weak_ptr<DdsImpl> wself = shared_from_this();
  return provider_->ServeRequests(
      topic + "_Request",
      [wself, handler](const std::vector<uint8_t>& payload,
                       const std::string& request_id,
                       const std::string& reply_topic) {
        auto self = wself.lock();
        if (!self) return;
        auto dec = self->core_.DecodeForReceive(payload);
        if (!dec) return;  // 解码失败：丢弃该请求（无法回传框架级错误）
        Message req;
        req.payload = std::move(dec.value);
        req.timestamp = TransportCore::NowMicros();
        ReplyFn reply = [wself, request_id,
                         reply_topic](const std::vector<uint8_t>& bytes) {
          auto self = wself.lock();
          if (!self || !self->open_.load())
            return Status::Fail("conn: transport closed");
          auto enc = self->core_.EncodeForSend(bytes);
          if (!enc) return Status::Fail(enc.error);
          return self->provider_->Reply(reply_topic, request_id, enc.value);
        };
        handler(req, std::move(reply));
      });
}

std::string DdsImpl::Provider() const {
  return provider_ ? provider_->ProviderName() : config_.provider;
}

// ---------- 生命周期 ----------

void DdsImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  // 取消全部未决请求（conn: 兑现），锁外调用 on_reply
  std::map<std::string, Pending> pend;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    pend.swap(pending_);
  }
  for (auto& kv : pend) {
    kv.second.timer->cancel();
    kv.second.on_reply(Result<Message>::Fail("conn: transport closed"));
  }
  if (provider_) provider_->Shutdown();
  core_.Close();
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

> 本任务同时给出了 req-resp 实现（与头文件一体），但 req-resp 行为由 Task 4 的测试驱动验证；本任务只要求 pub-sub 测试全绿。`core_.DecodeForReceive` 在 Task 4 加入 `TransportCore`——**为使本任务可编译**，本任务先在 `TransportCore.hpp` 的 `EncodeForSend` 旁加入该方法（见下），Task 4 为其补单测。

`include/transport/core/TransportCore.hpp` 中，在 `EncodeForSend` 之后加入：

```cpp
  // 接收侧解码；无 codec 时透传。供不经 ReceiveQueue 交付的路径
  //（如 DDS req-resp 回调）使用；常规路径走 DeliverFrame。
  Result<std::vector<uint8_t>> DecodeForReceive(
      const std::vector<uint8_t>& frame) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(frame);
    return codec_->Decode(frame);
  }
```

- [ ] **Step 6: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R DdsPubSub`
Expected: `DdsPubSub.*`（6 个）PASS；全量套件保持绿色。

- [ ] **Step 7: 提交**

```bash
git add include/transport/dds/DdsImpl.hpp src/dds/DdsImpl.cpp \
        include/transport/core/TransportCore.hpp \
        tests/dds/dds_impl_pubsub_test.cpp CMakeLists.txt
git commit -m "feat: DdsImpl pub-sub（provider 注入，对 FakeDdsProvider 测试）"
```

---

## Task 4: `DdsImpl` req-resp（对 Fake：关联/超时/并发/Close）

**Files:**
- Test: `tests/dds/dds_impl_reqresp_test.cpp`
- Modify: `tests/core/transport_core_test.cpp`（补 `DecodeForReceive` 单测）
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败（行为验证）测试**

`tests/dds/dds_impl_reqresp_test.cpp`:

```cpp
#include "transport/dds/DdsImpl.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::IDdsTransport;
using transport::Message;
using transport::Result;

namespace {

DdsConfig ReqRespCfg() {
  DdsConfig c;
  c.mode = DdsMode::kReqResp;
  c.topics = {"calc"};
  return c;
}

std::shared_ptr<DdsImpl> Make(std::shared_ptr<FakeDdsProvider::Bus> bus,
                              DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}

}  // namespace

TEST(DdsReqResp, RoundtripCorrelation) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "calc", [](const Message& req, IDdsTransport::ReplyFn reply) {
        auto out = req.payload;
        for (auto& b : out) b = static_cast<uint8_t>(b + 1);  // 业务：+1
        EXPECT_TRUE(static_cast<bool>(reply(out)));
      })));

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {1, 2, 3}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 1000)));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(DdsReqResp, ConcurrentRequestsDoNotCross) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  server->Open(); client->Open();
  // echo 服务
  server->OnRequest("calc", [](const Message& req, IDdsTransport::ReplyFn reply) {
    reply(req.payload);
  });

  std::promise<std::vector<uint8_t>> p1, p2;
  client->SendRequest({1}, "calc",
      [&](Result<Message> r) { p1.set_value(r ? r.value.payload
                                              : std::vector<uint8_t>{}); }, 1000);
  client->SendRequest({2}, "calc",
      [&](Result<Message> r) { p2.set_value(r ? r.value.payload
                                              : std::vector<uint8_t>{}); }, 1000);
  EXPECT_EQ(p1.get_future().get(), (std::vector<uint8_t>{1}));
  EXPECT_EQ(p2.get_future().get(), (std::vector<uint8_t>{2}));
}

TEST(DdsReqResp, TimeoutWhenNoServer) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  client->Open();
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {1}, "calc", [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/50)));
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsReqResp, AsyncReplyFromAnotherThread) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  server->Open(); client->Open();

  std::thread worker;
  server->OnRequest("calc", [&](const Message& req, IDdsTransport::ReplyFn reply) {
    auto payload = req.payload;
    worker = std::thread([reply, payload] {
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
      reply(payload);  // 异步回包
    });
  });

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  client->SendRequest({7}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 2000);
  auto r = fut.get();
  if (worker.joinable()) worker.join();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7}));
}

TEST(DdsReqResp, UnknownReplyIdIgnored) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  client->Open();
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  client->SendRequest({1}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 300);
  // 直接向 reply topic 投一个无关 id 的回复
  FakeDdsProvider stranger(bus);
  stranger.Reply("calc_Reply", "not-our-id", {9});
  auto r = fut.get();  // 仍按超时收场（无关回复被忽略）
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsReqResp, CloseCancelsPendingWithConnError) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  client->Open();
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  client->SendRequest({1}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/60000);
  client->Close();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}

TEST(DdsReqResp, ModeConstraintRejectsPubSubMethods) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = Make(bus, ReqRespCfg());
  t->Open();
  EXPECT_EQ(t->Send({1}).error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Send({1}, "x").error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Subscribe("x").error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Unsubscribe("x").error.rfind("config:", 0), 0u);
}
```

在 `tests/core/transport_core_test.cpp` 末尾追加：

```cpp
TEST(TransportCore, DecodeForReceivePassthroughAndCodec) {
  transport::TransportCore core;
  auto p = core.DecodeForReceive({1, 2});
  ASSERT_TRUE(static_cast<bool>(p));
  EXPECT_EQ(p.value, (std::vector<uint8_t>{1, 2}));
  core.SetCodec(std::make_shared<ShiftCodec>());
  auto d = core.DecodeForReceive({2, 3});
  ASSERT_TRUE(static_cast<bool>(d));
  EXPECT_EQ(d.value, (std::vector<uint8_t>{1, 2}));
}
```

- [ ] **Step 2: 把测试源加入 CMake，构建运行**

`add_executable` 追加 `tests/dds/dds_impl_reqresp_test.cpp`。

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R 'DdsReqResp|TransportCore'`
Expected: `DdsReqResp.*`（7 个）与 `TransportCore.*` 全 PASS（实现已在 Task 3 就位；若有行为缺陷按 TDD 修 `DdsImpl.cpp` 并报告）。全量绿色。

- [ ] **Step 3: 跑稳定性（含线程/timer 路径）**

Run: `for i in $(seq 1 8); do ctest --test-dir build -R 'DdsReqResp' --output-on-failure || break; done`
Expected: 8/8 clean。

- [ ] **Step 4: 提交**

```bash
git add tests/dds/dds_impl_reqresp_test.cpp tests/core/transport_core_test.cpp CMakeLists.txt
git commit -m "test: DdsImpl req-resp 行为验证（关联/超时/并发/异步回包/Close）+ TransportCore.DecodeForReceive 单测"
```

---

## Task 5: [FastDDS] `FastDdsRawType` + wire layout 测试 + CMake 门控

**Files:**
- Create: `src/dds/FastDdsRawType.hpp`
- Create: `src/dds/FastDdsRawType.cpp`
- Test: `tests/dds/fastdds_rawtype_test.cpp`
- Modify: `CMakeLists.txt`（FastDDS find_package + 门控）

- [ ] **Step 1: CMake 加 FastDDS 门控**

在 `CMakeLists.txt` 顶层（asio 集成之后）加：

```cmake
find_package(fastcdr QUIET)
find_package(fastrtps QUIET)
if(fastrtps_FOUND AND fastcdr_FOUND)
  set(TRANSPORT_HAS_FASTDDS ON)
  message(STATUS "Fast DDS found: enabling DDS provider")
else()
  set(TRANSPORT_HAS_FASTDDS OFF)
  message(STATUS "Fast DDS NOT found: building without FastDdsProvider")
endif()
```

在 `add_library(transport STATIC ...)` 之后加：

```cmake
if(TRANSPORT_HAS_FASTDDS)
  target_sources(transport PRIVATE src/dds/FastDdsRawType.cpp)
  target_link_libraries(transport PUBLIC fastrtps fastcdr)
  target_compile_definitions(transport PUBLIC TRANSPORT_HAS_FASTDDS)
endif()
```

在 tests 块内（`gtest_discover_tests` 之前）加：

```cmake
  if(TRANSPORT_HAS_FASTDDS)
    target_sources(transport_tests PRIVATE tests/dds/fastdds_rawtype_test.cpp)
  endif()
```

- [ ] **Step 2: 写失败测试（wire layout golden bytes + 往返）**

`tests/dds/fastdds_rawtype_test.cpp`:

```cpp
#include "../../src/dds/FastDdsRawType.hpp"

#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/RawMessage.hpp"

using transport::FastDdsRawType;
using transport::RawMessage;
using SerializedPayload = eprosima::fastrtps::rtps::SerializedPayload_t;

TEST(FastDdsRawType, GoldenBytes) {
  FastDdsRawType type;
  RawMessage msg;
  msg.request_id = "ab";
  msg.reply_topic = "cd";
  msg.payload = {0xDE, 0xAD};

  SerializedPayload payload(64);
  ASSERT_TRUE(type.serialize(&msg, &payload));
  // [u16 LE id_len][id][u16 LE reply_len][reply][payload...]
  const uint8_t expected[] = {0x02, 0x00, 'a', 'b',
                              0x02, 0x00, 'c', 'd', 0xDE, 0xAD};
  ASSERT_EQ(payload.length, sizeof(expected));
  EXPECT_EQ(0, memcmp(payload.data, expected, sizeof(expected)));
}

TEST(FastDdsRawType, RoundtripAndEmptyFields) {
  FastDdsRawType type;
  RawMessage in;
  in.payload = {1, 2, 3};  // pub-sub 形态：id/reply 为空（各 2 字节 0 前缀）

  SerializedPayload payload(64);
  ASSERT_TRUE(type.serialize(&in, &payload));
  EXPECT_EQ(payload.length, 4u + 3u);

  RawMessage out;
  ASSERT_TRUE(type.deserialize(&payload, &out));
  EXPECT_TRUE(out.request_id.empty());
  EXPECT_TRUE(out.reply_topic.empty());
  EXPECT_EQ(out.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(FastDdsRawType, SizeProviderMatchesSerializedLength) {
  FastDdsRawType type;
  RawMessage msg;
  msg.request_id = "x";
  msg.payload = std::vector<uint8_t>(100, 0xAA);
  auto size_fn = type.getSerializedSizeProvider(&msg);
  SerializedPayload payload(256);
  ASSERT_TRUE(type.serialize(&msg, &payload));
  EXPECT_EQ(size_fn(), payload.length);
}

TEST(FastDdsRawType, DeserializeRejectsTruncated) {
  FastDdsRawType type;
  SerializedPayload payload(8);
  payload.data[0] = 0x05; payload.data[1] = 0x00;  // 声称 id_len=5
  payload.length = 3;                              // 实际只有 3 字节
  RawMessage out;
  EXPECT_FALSE(type.deserialize(&payload, &out));
}
```

Run: `cmake -S . -B build && cmake --build build -j 2>&1 | head -20`
Expected: 配置输出 `Fast DDS found`；编译失败——找不到 `FastDdsRawType.hpp`。

- [ ] **Step 3: 写头与实现（FastDDS 2.13 签名）**

`src/dds/FastDdsRawType.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// FastDdsRawType.hpp — RawMessage 的自定义 TopicDataType（Fast DDS 2.13）
// 手写紧凑序列化（不经 CDR）：[u16 LE id_len][id][u16 LE reply_len][reply][payload]
// 版本敏感面之一（升 3.x 改本文件对的签名即可）。
// -----------------------------------------------------------------------------

#include <fastdds/dds/topic/TopicDataType.hpp>

#include "transport/dds/RawMessage.hpp"

namespace transport {

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

`src/dds/FastDdsRawType.cpp`:

```cpp
#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
constexpr uint32_t kHeaderBytes = 4;  // 两个 u16 长度前缀
// 预分配上限（payload 较大时由 PREALLOCATED_WITH_REALLOC 内存策略兜底）
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;

void WriteU16Le(uint8_t* p, uint16_t v) {
  p[0] = static_cast<uint8_t>(v & 0xFF);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
}
uint16_t ReadU16Le(const uint8_t* p) {
  return static_cast<uint16_t>(p[0] | (p[1] << 8));
}
}  // namespace

FastDdsRawType::FastDdsRawType() {
  setName("RawMessage");
  m_typeSize = kPreallocSize;
  m_isGetKeyDefined = false;
}

bool FastDdsRawType::serialize(
    void* data, eprosima::fastrtps::rtps::SerializedPayload_t* payload) {
  auto* msg = static_cast<RawMessage*>(data);
  if (msg->request_id.size() > 0xFFFF || msg->reply_topic.size() > 0xFFFF)
    return false;
  const uint32_t total = kHeaderBytes +
                         static_cast<uint32_t>(msg->request_id.size()) +
                         static_cast<uint32_t>(msg->reply_topic.size()) +
                         static_cast<uint32_t>(msg->payload.size());
  if (total > payload->max_size) return false;

  uint8_t* p = payload->data;
  WriteU16Le(p, static_cast<uint16_t>(msg->request_id.size()));
  p += 2;
  std::memcpy(p, msg->request_id.data(), msg->request_id.size());
  p += msg->request_id.size();
  WriteU16Le(p, static_cast<uint16_t>(msg->reply_topic.size()));
  p += 2;
  std::memcpy(p, msg->reply_topic.data(), msg->reply_topic.size());
  p += msg->reply_topic.size();
  std::memcpy(p, msg->payload.data(), msg->payload.size());
  payload->length = total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastrtps::rtps::SerializedPayload_t* payload, void* data) {
  auto* msg = static_cast<RawMessage*>(data);
  const uint8_t* p = payload->data;
  uint32_t remaining = payload->length;

  if (remaining < 2) return false;
  uint16_t id_len = ReadU16Le(p);
  p += 2; remaining -= 2;
  if (remaining < id_len) return false;
  msg->request_id.assign(reinterpret_cast<const char*>(p), id_len);
  p += id_len; remaining -= id_len;

  if (remaining < 2) return false;
  uint16_t reply_len = ReadU16Le(p);
  p += 2; remaining -= 2;
  if (remaining < reply_len) return false;
  msg->reply_topic.assign(reinterpret_cast<const char*>(p), reply_len);
  p += reply_len; remaining -= reply_len;

  msg->payload.assign(p, p + remaining);
  return true;
}

std::function<uint32_t()> FastDdsRawType::getSerializedSizeProvider(
    void* data) {
  auto* msg = static_cast<RawMessage*>(data);
  const uint32_t total = kHeaderBytes +
                         static_cast<uint32_t>(msg->request_id.size()) +
                         static_cast<uint32_t>(msg->reply_topic.size()) +
                         static_cast<uint32_t>(msg->payload.size());
  return [total]() { return total; };
}

void* FastDdsRawType::createData() { return new RawMessage(); }

void FastDdsRawType::deleteData(void* data) {
  delete static_cast<RawMessage*>(data);
}

bool FastDdsRawType::getKey(void*, eprosima::fastrtps::rtps::InstanceHandle_t*,
                            bool) {
  return false;  // 无 key
}

}  // namespace transport
```

- [ ] **Step 4: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R FastDdsRawType`
Expected: `FastDdsRawType.*`（4 个）PASS；全量绿色。

- [ ] **Step 5: 提交**

```bash
git add src/dds/FastDdsRawType.hpp src/dds/FastDdsRawType.cpp \
        tests/dds/fastdds_rawtype_test.cpp CMakeLists.txt
git commit -m "feat: FastDdsRawType 自定义 TopicDataType（wire layout + golden bytes 测试，CMake FastDDS 门控）"
```

---

## Task 6: [FastDDS] `FastDdsProvider` + 真实互通集成测试

**Files:**
- Create: `src/dds/FastDdsProvider.hpp`
- Create: `src/dds/FastDdsProvider.cpp`
- Test: `tests/dds/fastdds_provider_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试（两实例真实互通）**

`tests/dds/fastdds_provider_test.cpp`:

```cpp
#include "../../src/dds/FastDdsProvider.hpp"

#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/dds/DdsProviderRegistry.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FastDdsProvider;
using transport::IDdsTransport;
using transport::Message;
using transport::Result;

namespace {

constexpr int kTestDomain = 42;  // 避免与环境中其它 DDS 互扰

DdsConfig Cfg(DdsMode mode, std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = mode;
  c.topics = std::move(topics);
  c.domain_id = kTestDomain;
  // TransientLocal：晚匹配的 reader 仍可取到 depth 内历史样本，
  // 吸收 DDS 发现期（~百 ms 级），避免 sleep-flaky。
  c.qos.durability = transport::DdsQos::Durability::kTransientLocal;
  return c;
}

std::shared_ptr<DdsImpl> MakeReal(DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FastDdsProvider>());
}

}  // namespace

TEST(FastDdsIntegration, PubSubRoundtrip) {
  auto rx = MakeReal(Cfg(DdsMode::kPubSub, {"itopic"}));
  auto opened = rx->Open();
  if (!opened) GTEST_SKIP() << "FastDDS participant unavailable: " << opened.error;
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("itopic")));

  auto tx = MakeReal(Cfg(DdsMode::kPubSub, {"itopic"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));

  auto r = rx->Receive(3000);  // 容纳发现期
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(r.value.topic, "itopic");
  tx->Close();
  rx->Close();
}

TEST(FastDdsIntegration, ReqRespRoundtrip) {
  auto server = MakeReal(Cfg(DdsMode::kReqResp, {"icalc"}));
  auto opened = server->Open();
  if (!opened) GTEST_SKIP() << "FastDDS participant unavailable";
  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "icalc", [](const Message& req, IDdsTransport::ReplyFn reply) {
        auto out = req.payload;
        for (auto& b : out) b = static_cast<uint8_t>(b + 1);
        reply(out);
      })));

  auto client = MakeReal(Cfg(DdsMode::kReqResp, {"icalc"}));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {10, 20}, "icalc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/5000)));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{11, 21}));
  client->Close();
  server->Close();
}

TEST(FastDdsIntegration, RegistryProvidesFastDds) {
  transport::RegisterFastDdsProvider();  // 显式注册（静态库防裁剪）
  auto p = transport::DdsProviderRegistry::Create("FastDDS");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "FastDDS");
}
```

- [ ] **Step 2: 写头与实现**

`src/dds/FastDdsProvider.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// FastDdsProvider.hpp — IDdsProvider 的 Fast DDS 2.13 实现
// participant + RawMessage 类型注册 + topic→writer/reader 懒加载 + DdsQos 映射。
// 版本敏感面之一（升 3.x 仅改本文件对 + FastDdsRawType）。
// -----------------------------------------------------------------------------

#include <map>
#include <memory>
#include <mutex>
#include <set>
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
#include "transport/dds/RawMessage.hpp"

namespace transport {

class FastDdsProvider : public IDdsProvider {
 public:
  FastDdsProvider() = default;
  ~FastDdsProvider() override;

  Status Init(const DdsConfig& config) override;
  Status Publish(const std::string& topic,
                 const std::vector<uint8_t>& data) override;
  Status Subscribe(const std::string& topic,
                   ITransport::ReceiveCallback cb) override;
  Status Unsubscribe(const std::string& topic) override;
  Status SendRequest(const std::string& request_topic,
                     const std::string& request_id,
                     const std::string& reply_topic,
                     const std::vector<uint8_t>& data) override;
  Status SubscribeReplies(const std::string& reply_topic,
                          ReplySink sink) override;
  Status ServeRequests(const std::string& request_topic,
                       RequestSink sink) override;
  Status Reply(const std::string& reply_topic, const std::string& request_id,
               const std::vector<uint8_t>& data) override;
  void Shutdown() override;
  std::string ProviderName() const override { return "FastDDS"; }

 private:
  using RawSink = std::function<void(const RawMessage&)>;

  class ReaderListener;  // on_data_available → take → RawSink

  // 内部统一原语：写 RawMessage / 以 RawSink 订阅
  Status WriteRaw(const std::string& topic, const RawMessage& msg);
  Status SubscribeRaw(const std::string& topic, RawSink sink);
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
  std::set<std::string> reply_subscribed_;
};

// 显式注册入口（静态库下匿名注册器可能被链接器裁剪；TransportFactory/测试调用此函数）
void RegisterFastDdsProvider();

}  // namespace transport
```

`src/dds/FastDdsProvider.cpp`:

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
#include "transport/dds/DdsProviderRegistry.hpp"

// FastDdsProvider.cpp — Fast DDS 2.13 provider（见 FastDdsProvider.hpp）。
// 线程：reader listener 回调来自 FastDDS 内部线程 → RawSink 须线程安全（DdsImpl
// 侧由 ReceiveQueue/mutex 保证）。实体均挂在单一 participant 下，Shutdown 逆序清理。

namespace transport {

namespace dds = eprosima::fastdds::dds;
using eprosima::fastrtps::types::ReturnCode_t;

namespace {
Status Ok() { return Status::Success(std::monostate{}); }

void ApplyQos(const DdsQos& q, dds::DataWriterQos* wqos,
              dds::DataReaderQos* rqos) {
  const auto rel = (q.reliability == DdsQos::Reliability::kReliable)
                       ? dds::RELIABLE_RELIABILITY_QOS
                       : dds::BEST_EFFORT_RELIABILITY_QOS;
  const auto dur = (q.durability == DdsQos::Durability::kTransientLocal)
                       ? dds::TRANSIENT_LOCAL_DURABILITY_QOS
                       : dds::VOLATILE_DURABILITY_QOS;
  if (wqos) {
    wqos->reliability().kind = rel;
    wqos->durability().kind = dur;
    wqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    wqos->history().depth = static_cast<int32_t>(q.history_depth);
    wqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
  if (rqos) {
    rqos->reliability().kind = rel;
    rqos->durability().kind = dur;
    rqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    rqos->history().depth = static_cast<int32_t>(q.history_depth);
    rqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
}
}  // namespace

// reader listener：每条 sample 取出 RawMessage 转交 sink
class FastDdsProvider::ReaderListener : public dds::DataReaderListener {
 public:
  explicit ReaderListener(RawSink sink) : sink_(std::move(sink)) {}
  void on_data_available(dds::DataReader* reader) override {
    RawMessage msg;
    dds::SampleInfo info;
    while (reader->take_next_sample(&msg, &info) == ReturnCode_t::RETCODE_OK) {
      if (info.valid_data) sink_(msg);
    }
  }

 private:
  RawSink sink_;
};

FastDdsProvider::~FastDdsProvider() { Shutdown(); }

Status FastDdsProvider::Init(const DdsConfig& config) {
  config_ = config;
  auto* factory = dds::DomainParticipantFactory::get_instance();
  participant_ = factory->create_participant(config.domain_id,
                                             dds::PARTICIPANT_QOS_DEFAULT);
  if (!participant_)
    return Status::Fail("io: create_participant failed (domain " +
                        std::to_string(config.domain_id) + ")");
  type_ = dds::TypeSupport(new FastDdsRawType());
  if (type_.register_type(participant_) != ReturnCode_t::RETCODE_OK)
    return Status::Fail("io: register_type RawMessage failed");
  publisher_ = participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
  subscriber_ = participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
  if (!publisher_ || !subscriber_)
    return Status::Fail("io: create publisher/subscriber failed");
  return Ok();
}

dds::Topic* FastDdsProvider::GetOrCreateTopic(const std::string& name) {
  auto it = topics_.find(name);
  if (it != topics_.end()) return it->second;
  dds::Topic* t =
      participant_->create_topic(name, "RawMessage", dds::TOPIC_QOS_DEFAULT);
  if (t) topics_[name] = t;
  return t;
}

Status FastDdsProvider::WriteRaw(const std::string& topic,
                                 const RawMessage& msg) {
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
  // write 需要非 const 指针
  RawMessage copy = msg;
  if (writer->write(&copy) != ReturnCode_t::RETCODE_OK)
    return Status::Fail("io: write failed: " + topic);
  return Ok();
}

Status FastDdsProvider::SubscribeRaw(const std::string& topic, RawSink sink) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (readers_.count(topic)) return Ok();  // 已订阅（幂等）
  dds::Topic* t = GetOrCreateTopic(topic);
  if (!t) return Status::Fail("io: create_topic failed: " + topic);
  auto listener = std::make_unique<ReaderListener>(std::move(sink));
  dds::DataReaderQos rqos = dds::DATAREADER_QOS_DEFAULT;
  ApplyQos(config_.qos, nullptr, &rqos);
  dds::DataReader* reader =
      subscriber_->create_datareader(t, rqos, listener.get());
  if (!reader) return Status::Fail("io: create_datareader failed: " + topic);
  readers_[topic] = ReaderEntry{reader, std::move(listener)};
  return Ok();
}

Status FastDdsProvider::Publish(const std::string& topic,
                                const std::vector<uint8_t>& data) {
  RawMessage m;
  m.payload = data;
  return WriteRaw(topic, m);
}

Status FastDdsProvider::Subscribe(const std::string& topic,
                                  ITransport::ReceiveCallback cb) {
  return SubscribeRaw(topic, [cb, topic](const RawMessage& m) {
    Message msg;
    msg.payload = m.payload;
    msg.topic = topic;
    msg.source = topic;
    cb(Result<Message>::Success(std::move(msg)));
  });
}

Status FastDdsProvider::Unsubscribe(const std::string& topic) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = readers_.find(topic);
  if (it == readers_.end()) return Ok();
  subscriber_->delete_datareader(it->second.reader);
  readers_.erase(it);
  return Ok();
}

Status FastDdsProvider::SendRequest(const std::string& request_topic,
                                    const std::string& request_id,
                                    const std::string& reply_topic,
                                    const std::vector<uint8_t>& data) {
  RawMessage m;
  m.request_id = request_id;
  m.reply_topic = reply_topic;
  m.payload = data;
  return WriteRaw(request_topic, m);
}

Status FastDdsProvider::SubscribeReplies(const std::string& reply_topic,
                                         ReplySink sink) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!reply_subscribed_.insert(reply_topic).second) return Ok();
  }
  return SubscribeRaw(reply_topic, [sink](const RawMessage& m) {
    sink(m.request_id, m.payload);
  });
}

Status FastDdsProvider::ServeRequests(const std::string& request_topic,
                                      RequestSink sink) {
  return SubscribeRaw(request_topic, [sink](const RawMessage& m) {
    sink(m.payload, m.request_id, m.reply_topic);
  });
}

Status FastDdsProvider::Reply(const std::string& reply_topic,
                              const std::string& request_id,
                              const std::vector<uint8_t>& data) {
  RawMessage m;
  m.request_id = request_id;
  m.payload = data;
  return WriteRaw(reply_topic, m);
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
  dds::DomainParticipantFactory::get_instance()->delete_participant(
      participant_);
  participant_ = nullptr;
  publisher_ = nullptr;
  subscriber_ = nullptr;
}

void RegisterFastDdsProvider() {
  DdsProviderRegistry::RegisterProvider(
      "FastDDS", [] { return std::make_unique<FastDdsProvider>(); });
}

namespace {
// 静态注册器：动态库/直接链接对象时自动注册；静态库可能被裁剪 → 工厂/用户可显式
// 调 RegisterFastDdsProvider()（TransportFactory 落地时调用）。
struct FastDdsRegistrar {
  FastDdsRegistrar() { RegisterFastDdsProvider(); }
} g_fastdds_registrar;
}  // namespace

}  // namespace transport
```

- [ ] **Step 3: CMake 加 provider 源与集成测试（门控内）**

在 `if(TRANSPORT_HAS_FASTDDS)` 的 `target_sources(transport ...)` 中追加 `src/dds/FastDdsProvider.cpp`；
tests 门控块中追加 `tests/dds/fastdds_provider_test.cpp`。

- [ ] **Step 4: 运行，确认通过**

Run:
```bash
cmake --build build -j && ctest --test-dir build --output-on-failure -R FastDdsIntegration
for i in $(seq 1 5); do ctest --test-dir build -R 'FastDdsIntegration' --output-on-failure || break; done
```
Expected: 3 个集成测试 PASS（环境无 DDS 能力时 SKIP，不 FAIL）；5 次稳定；全量绿色。

> 实测兜底：若 `take_next_sample`/`ReturnCode_t` 的头路径或比较方式在 2.13.1 上有出入（如需 `ReturnCode_t::ReturnCodeValue` 或 `==` 重载差异），按编译器报错就地修正 include/比较写法——属机械适配，不改架构；报告偏差。若发现期 3s 仍偶发不够，把 `Receive(3000)` 改 5000 并报告。

- [ ] **Step 5: 提交**

```bash
git add src/dds/FastDdsProvider.hpp src/dds/FastDdsProvider.cpp \
        tests/dds/fastdds_provider_test.cpp CMakeLists.txt
git commit -m "feat: FastDdsProvider（FastDDS 2.13）+ 两实例真实互通集成测试"
```

---

## Task 7: DDS 收尾验证 + 文档同步

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`（§3.4 状态）
- Modify: `docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`（补 DDS）

- [ ] **Step 1: 干净全量构建 + DDS 套件稳定性**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
for i in $(seq 1 10); do ctest --test-dir build -R 'Dds|FastDds' --output-on-failure || break; done
```
Expected: 全部 PASS（FastDDS 集成测试 PASS 或一致 SKIP）；DDS 套件 10 次稳定。

- [ ] **Step 2: README 更新**

「状态」小节 DDS 行改 `- [x] DDS（Fast DDS，pub-sub / req-resp）`；「特点」的已实现传输补「DDS」；「用法」节在 UDP 示例之后插入：

````markdown
### DDS（pub-sub 多 topic + req-resp）

```cpp
#include "transport/dds/DdsImpl.hpp"
using namespace transport;

// pub-sub：一个实例 = 一个 DomainParticipant，内部多 topic
DdsConfig pc;
pc.mode = DdsMode::kPubSub;
pc.topics = {"cmd", "telemetry"};        // topics[0] 为 Send(data) 默认 topic
pc.domain_id = 0;
auto dds = std::make_shared<DdsImpl>(pc);
dds->Open();
dds->Subscribe("telemetry");
dds->OnReceive([](Result<Message> m) { /* m.value.topic 标识来源 */ });
dds->Send({1, 2, 3}, "cmd");             // 向指定 topic 发布

// req-resp：响应端
DdsConfig sc;
sc.mode = DdsMode::kReqResp;
sc.topics = {"calc"};
auto server = std::make_shared<DdsImpl>(sc);
server->Open();
server->OnRequest("calc", [](const Message& req, IDdsTransport::ReplyFn reply) {
  reply(Compute(req.payload));           // 可同步或异步调用 reply
});

// req-resp：客户端（框架自动生成 request_id、配对响应、超时）
auto client = std::make_shared<DdsImpl>(sc);
client->Open();
client->SendRequest(request_bytes, "calc",
    [](Result<Message> r) {
      if (!r) { /* r.error 形如 "timeout:..." */ return; }
      Use(r.value.payload);
    }, /*timeout_ms=*/3000);
```
````

- [ ] **Step 3: 文档同步**

- 主 spec §3.4 状态行：`IDdsTransport`/`DdsImpl` 移入已实现，仅剩 `TransportFactory` 规划中；
- as-built 架构文档（`2026-06-10-foundation-tcp-architecture.md`）：标题/引言补 DDS；类图加 `IDdsTransport`/`DdsImpl`/`IDdsProvider`/`FastDdsProvider`/`RawMessage`（`DdsImpl *-- TransportCore`、`DdsImpl o-- IDdsProvider`、`IDdsProvider <|.. FastDdsProvider`、`FastDdsProvider ..> RawMessage`）；§2 依赖说明补 DDS 小节；时序图可选补「req-resp 一次往返」。

- [ ] **Step 4: 提交**

```bash
git add README.md docs/superpowers/specs/2026-06-09-transport-middleware-design.md \
        docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md
git commit -m "docs: README/主 spec/as-built 架构标记 DDS 完成（类图补 DDS 部分）"
```

---

## 后续（不在本计划范围）

TransportFactory + JSON 配置（主 spec §9）——届时调用 `RegisterFastDdsProvider()` 解决静态库注册裁剪问题，并把 `DdsQos` 纳入 JSON 映射。
