# InteractionEngine + InteractionPolicy(交互引擎抽象)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把三节点各自重写的交互机制抽成一份 `InteractionEngine`(3 原语 + 挂起/超时/重发/分发/定时/并发纪律),协议差异收进声明式 `InteractionPolicy`;`DdsNode`/`ProtocolNode` 重做成薄壳,`CommNode` 移除。

**Architecture:** `InteractionEngine` 持 transport+codec+policy+executor,io→Decode→Post→单 worker Dispatch;`InteractionPolicy` 提供 `TagOf/SetTag/NewCorrelation/KeyOf/EchoCorrelation/ReplyTo/RouteUnmatched`;`RequestSpec` 配置 request-await(中间/终结 tag、auto_ack、超时、重发)。节点 = 命名模式 → 原语 + tag 常量。

**Tech Stack:** C++17;GoogleTest 1.14(vendored);不抛异常(`Result<T>`/`Status`,`[[nodiscard]]`);标准库线程/条件变量。

**配套 spec:** `docs/superpowers/specs/2026-06-24-interaction-engine-design.md`

## Global Constraints

- 不抛异常;`Result<T>`/`Status`,保留 `[[nodiscard]]`。C++17。不引入新第三方依赖;不动 transport/codec/DDS provider/`IExecutor`/`ThreadExecutor`/`Message`。
- `Key = std::string`;`FrameTag = int`(引擎只做相等比较)。
- **重发规则:** 挂起项记 `advanced`;超时时 `if (!advanced && retries < max_retries)` 重发(同 Message)+重排+`++retries`,否则失败 `timeout:`+erase;首个配上的帧(中间/终结)置 `advanced=true`。
- 并发纪律(仅引擎一处):posted 任务/回调捕获 `weak_ptr`;挂起表/periodic 表/计数器由 `mu_` 保护;`Encode`+`Send`(含 auto_ack/periodic/SendReply)一律锁外;中间帧 `on_intermediate` 拷贝调用、终结 `on_terminal` 移出调用(防双触发);`open_` 在 `transport_->Open()` 前置位+失败回滚;`Close` 幂等,先终结挂起(`conn:`)+取消全部定时器(`mu_` 下)→`executor_->Stop()`→`transport_->Close()`。
- `ProtocolPolicy`:`session_id` 滚动 0–255,**`message_id` = 调用方填的命令码(不动)**,`protocol_id` 配置;Key=`pack(session,message)`。
- `DdsNode` 行为不变(`dds_node_test` 仅去 CommNode 依赖:删 2 `using` + 删 1 个基类句柄 TEST,断言不变);`ProtocolNode` 发送方法加 `uint16_t cmd` 参数(`protocol_node_test` 据此补参数,其 Responder 已是嵌套类型)。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**不加 Co-Authored-By**;不提交 `build/`。

---

## 文件结构

**新建:** `include/transport/comm/InteractionPolicy.hpp`、`include/transport/comm/InteractionEngine.hpp` + `src/comm/InteractionEngine.cpp`、`include/transport/comm/ProtocolPolicy.hpp`、`include/transport/comm/DdsPolicy.hpp`、`tests/comm/interaction_engine_test.cpp`。
**改写:** `ProtocolNode.hpp/.cpp`、`DdsNode.hpp/.cpp`、`tests/comm/protocol_node_test.cpp`、`CMakeLists.txt`。
**移除:** `CommNode.hpp/.cpp`、`tests/comm/comm_node_test.cpp`。
**不动:** transport/codec/`DdsCodec`/`SystemCodec`/DDS provider/`IExecutor`/`ThreadExecutor`/`Message`/`dds_node_test.cpp`。

---

## Task 1: `InteractionPolicy` + `InteractionEngine` + 引擎测试

**Files:** Create `include/transport/comm/InteractionPolicy.hpp`、`include/transport/comm/InteractionEngine.hpp`、`src/comm/InteractionEngine.cpp`、`tests/comm/interaction_engine_test.cpp`;Modify `CMakeLists.txt`。

**Interfaces:**
- Produces: `transport::Key`(=`std::string`)、`transport::FrameTag`(=`int`)、`transport::InteractionPolicy`(7 虚法 + `Route` 枚举)、`transport::RequestSpec`、`transport::InteractionEngine`(ctor、Open/Close/IsOpen、On* 钩子、`Fire`/`RequestAwait`/`StartPeriodic`/`StopPeriodic`/`SendReply`)。

- [ ] **Step 1: 写 `include/transport/comm/InteractionPolicy.hpp`**
```cpp
#pragma once

// InteractionPolicy.hpp — 交互引擎的协议策略缝(声明式)。引擎只问、不解释 FrameTag/Key。

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

using Key = std::string;   // 统一匹配键;policy 打包协议字段成串
using FrameTag = int;      // 抽象判别符;policy 把 frm_type/kind 枚举 cast 成 int

class InteractionPolicy {
 public:
  virtual ~InteractionPolicy() = default;

  virtual FrameTag TagOf(const Message& m) = 0;
  virtual void     SetTag(Message& m, FrameTag tag) = 0;

  virtual Key  NewCorrelation(Message& out) = 0;     // 盖全新相关号进 out,返回挂起 key
  virtual Key  KeyOf(const Message& in) = 0;          // 取入站 key(须与应答相等)
  virtual void EchoCorrelation(Message& reply, const Message& request) = 0;

  virtual Endpoint ReplyTo(const Message& request) = 0;

  enum class Route { kInboundRequest, kDeliver, kDrop };
  virtual Route RouteUnmatched(const Message& in) = 0;
};

// request-await 配置。
struct RequestSpec {
  FrameTag request_tag = 0;
  std::optional<FrameTag> intermediate_tag;
  FrameTag terminal_tag = 0;
  std::optional<FrameTag> auto_ack_tag;
  std::function<void(const Message&)> on_intermediate;
  std::function<void(Result<Message>)> on_terminal;
  uint32_t timeout_ms = 1000;
  uint32_t max_retries = 0;
};

}  // namespace transport
```

- [ ] **Step 2: 写 `include/transport/comm/InteractionEngine.hpp`**
```cpp
#pragma once

// InteractionEngine.hpp — 通用交互引擎。3 原语(Fire/RequestAwait/StartPeriodic)+ 挂起/超时/重发/
// 分发/periodic/并发纪律一份。协议差异经 InteractionPolicy。须以 shared_ptr 持有。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class InteractionEngine : public std::enable_shared_from_this<InteractionEngine> {
 public:
  InteractionEngine(std::shared_ptr<ITransport> transport,
                    std::unique_ptr<ICodec> codec,
                    std::unique_ptr<InteractionPolicy> policy,
                    std::unique_ptr<IExecutor> executor = nullptr,
                    std::size_t queue_capacity = 1024);
  ~InteractionEngine();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  void OnInboundRequest(std::function<void(const Message&)> cb) { on_request_ = std::move(cb); }
  void OnInboundDeliver(std::function<void(const Message&)> cb) { on_deliver_ = std::move(cb); }
  void OnError(std::function<void(const std::string&)> cb) { on_error_ = std::move(cb); }

  Status   Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  Status   RequestAwait(Message out, RequestSpec spec, const Endpoint& to = Endpoint::Default());
  uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  void     StopPeriodic(uint32_t handle);

  Status   SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload);

 private:
  struct Pending {
    RequestSpec spec; Message out; Endpoint to;
    uint32_t retries = 0; IExecutor::TimerId timer = 0; bool advanced = false;
  };
  struct Periodic { Message out; FrameTag tag; Endpoint to; uint32_t interval_ms; IExecutor::TimerId timer = 0; };

  Status SendMessage(Message& m, const Endpoint& to);
  void Dispatch(Message msg);
  void OnTimeout(Key key);
  void HandleDisconnect(const std::string& reason);
  void FirePeriodic(uint32_t handle);

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<InteractionPolicy> policy_;
  std::unique_ptr<IExecutor> executor_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;
  std::map<Key, Pending> pending_;
  std::map<uint32_t, Periodic> periodics_;
  uint32_t periodic_next_ = 1;
  std::function<void(const Message&)> on_request_;
  std::function<void(const Message&)> on_deliver_;
  std::function<void(const std::string&)> on_error_;
};

}  // namespace transport
```

- [ ] **Step 3: 写失败测试 `tests/comm/interaction_engine_test.cpp`**
```cpp
#include "transport/comm/InteractionEngine.hpp"

#include "transport/codec/DdsCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <future>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

using transport::DdsCodec;
using transport::Endpoint;
using transport::FrameTag;
using transport::ICodec;
using transport::IExecutor;
using transport::InteractionEngine;
using transport::InteractionPolicy;
using transport::Key;
using transport::Message;
using transport::MessageKind;
using transport::RequestSpec;
using transport::Result;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
// 测试 policy:Key=correlation_id 字符串;tag=(int)kind;DDS 风格但用同管道(ReplyTo=Default)。
class TestPolicy : public InteractionPolicy {
 public:
  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.kind); }
  void SetTag(Message& m, FrameTag t) override { m.kind = static_cast<MessageKind>(t); }
  Key NewCorrelation(Message& out) override { out.correlation_id = "c" + std::to_string(++seq_); return out.correlation_id; }
  Key KeyOf(const Message& in) override { return in.correlation_id; }
  void EchoCorrelation(Message& reply, const Message& req) override { reply.correlation_id = req.correlation_id; }
  Endpoint ReplyTo(const Message&) override { return Endpoint::Default(); }
  Route RouteUnmatched(const Message& in) override {
    if (in.kind == MessageKind::kRequest) return Route::kInboundRequest;
    if (in.kind == MessageKind::kOneway || in.kind == MessageKind::kNotify) return Route::kDeliver;
    return Route::kDrop;
  }
 private:
  uint64_t seq_ = 0;
};

int T(MessageKind k) { return static_cast<int>(k); }
Message Pay(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

struct Net {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<InteractionEngine> a, b;
  InlineExecutor* exa = nullptr;
  Net() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::unique_ptr<IExecutor>(std::move(ea)));
    b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(InteractionEngine, FireDelivers) {
  Net n; std::vector<uint8_t> got; bool called = false;
  n.b->OnInboundDeliver([&](const Message& m) { got = m.payload; called = true; });
  n.Open();
  ASSERT_TRUE(static_cast<bool>(n.a->Fire(Pay({1, 2, 3}), T(MessageKind::kOneway))));
  EXPECT_TRUE(called); EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
  n.Close();
}

TEST(InteractionEngine, RequestAwaitTerminalCompletes) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    auto out = req.payload; out.push_back(0xEE);
    (void)n.b->SendReply(req, T(MessageKind::kReply), out);
  });
  n.Open();
  Result<Message> got = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.on_terminal = [&](Result<Message> r) { got = std::move(r); };
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xEE}));
  n.Close();
}

TEST(InteractionEngine, IntermediateThenTerminalKeepsPendingThenCompletes) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    (void)n.b->SendReply(req, T(MessageKind::kFeedback), {0x01});
    (void)n.b->SendReply(req, T(MessageKind::kReply), {0x02});
  });
  n.Open();
  std::vector<std::vector<uint8_t>> inter; Result<Message> fin = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.on_intermediate = [&](const Message& m) { inter.push_back(m.payload); };
  s.on_terminal = [&](Result<Message> r) { fin = std::move(r); };
  (void)n.a->RequestAwait(Pay({9}), s);
  ASSERT_EQ(inter.size(), 1u); EXPECT_EQ(inter[0], (std::vector<uint8_t>{0x01}));
  ASSERT_TRUE(static_cast<bool>(fin)); EXPECT_EQ(fin.value.payload, (std::vector<uint8_t>{0x02}));
  n.Close();
}

TEST(InteractionEngine, AutoAckOnTerminal) {
  Net n; int acks = 0;
  n.b->OnInboundRequest([&](const Message& req) { (void)n.b->SendReply(req, T(MessageKind::kReply), {0xAA}); });
  n.b->OnInboundDeliver([&](const Message& m) { if (m.kind == MessageKind::kNotify) ++acks; });  // ack = kNotify
  n.Open();
  Result<Message> fin = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.auto_ack_tag = T(MessageKind::kNotify); s.on_terminal = [&](Result<Message> r) { fin = std::move(r); };
  (void)n.a->RequestAwait(Pay({1}), s);
  ASSERT_TRUE(static_cast<bool>(fin)); EXPECT_EQ(acks, 1);  // b 收到发起方自动回的 ack
  n.Close();
}

TEST(InteractionEngine, TimeoutRetransmitsThenFailsAndFirstFrameStops) {
  Net n; int reqs = 0;
  n.b->OnInboundRequest([&](const Message&) { ++reqs; });  // 不回
  n.Open();
  Result<Message> got = Result<Message>::Success(Message{});
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 50; s.max_retries = 3; s.on_terminal = [&](Result<Message> r) { got = std::move(r); };
  (void)n.a->RequestAwait(Pay({1}), s);
  EXPECT_EQ(reqs, 1);
  n.exa->FireAll(); EXPECT_EQ(reqs, 2);
  n.exa->FireAll(); EXPECT_EQ(reqs, 3);
  n.exa->FireAll(); EXPECT_EQ(reqs, 4);
  n.exa->FireAll();                                  // 重发耗尽 → 失败
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  EXPECT_EQ(reqs, 4);
  n.Close();
}

TEST(InteractionEngine, PeriodicSendsUntilStopped) {
  Net n; int delivered = 0;
  n.b->OnInboundDeliver([&](const Message&) { ++delivered; });
  n.Open();
  uint32_t h = n.a->StartPeriodic(Pay({7}), T(MessageKind::kOneway), 100);
  EXPECT_EQ(delivered, 1);
  n.exa->FireAll(); EXPECT_EQ(delivered, 2);
  n.a->StopPeriodic(h);
  n.exa->FireAll(); EXPECT_EQ(delivered, 2);
  EXPECT_EQ(n.a->StartPeriodic(Pay({0}), T(MessageKind::kOneway), 0), 0u);  // 零间隔拒绝
  n.Close();
}

TEST(InteractionEngine, CloseFinalizesPendingNoDoubleInvoke) {
  Net n; int term = 0;
  n.b->OnInboundRequest([&](const Message& req) { (void)n.b->SendReply(req, T(MessageKind::kFeedback), {0x01}); });  // 只中间,不终结
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 100000; s.on_terminal = [&](Result<Message>) { ++term; };
  (void)n.a->RequestAwait(Pay({1}), s);
  n.a->Close();                       // 终结挂起一次
  EXPECT_EQ(term, 1);
  n.b->Close();
}

TEST(InteractionEngine, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  auto b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  b->OnInboundRequest([b](const Message& req) { auto o = req.payload; o.push_back(0xEE); (void)b->SendReply(req, static_cast<int>(MessageKind::kReply), o); });
  (void)b->Open(); (void)a->Open();
  std::promise<Result<Message>> prom; auto fut = prom.get_future();
  RequestSpec s; s.request_tag = static_cast<int>(MessageKind::kRequest); s.terminal_tag = static_cast<int>(MessageKind::kReply);
  s.timeout_ms = 2000; s.on_terminal = [&](Result<Message> r) { prom.set_value(std::move(r)); };
  (void)a->RequestAwait(Pay({3}), s);
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r)); EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xEE}));
  a->Close(); b->Close();
}
```
把 `tests/comm/interaction_engine_test.cpp` 加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`(在 `tests/comm/dds_node_test.cpp` 后)。

- [ ] **Step 4: 运行,确认失败** `cd /home/ubuntu/david/transport && cmake -S . -B build >/dev/null 2>&1; cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `transport/comm/InteractionEngine.hpp`。

- [ ] **Step 5: 写 `src/comm/InteractionEngine.cpp`**
```cpp
#include "transport/comm/InteractionEngine.hpp"

#include <chrono>
#include <utility>
#include <variant>

#include "transport/comm/ThreadExecutor.hpp"

// InteractionEngine.cpp — 见 .hpp。机制一份;并发纪律集中此处。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

InteractionEngine::InteractionEngine(std::shared_ptr<ITransport> transport,
                                     std::unique_ptr<ICodec> codec,
                                     std::unique_ptr<InteractionPolicy> policy,
                                     std::unique_ptr<IExecutor> executor,
                                     std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      policy_(std::move(policy)),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))) {}

InteractionEngine::~InteractionEngine() { Close(); }

Status InteractionEngine::Open() {
  executor_->Start();
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string& from) {
    auto s = wself.lock();
    if (!s) return;
    if (!r) {
      std::string e = r.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      return;
    }
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      return;
    }
    for (auto& m : msgs.value) {
      m.source = from;
      if (m.topic.empty()) m.topic = from;
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
    }
  });
  transport_->OnConnect([] {});
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  open_.store(true);
  auto st = transport_->Open();
  if (!st) { open_.store(false); executor_->Stop(); return st; }
  return Ok();
}

void InteractionEngine::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<Key, Pending> taken;
  {
    std::lock_guard<std::mutex> lk(mu_);
    taken.swap(pending_);
    for (auto& kv : periodics_) if (kv.second.timer) executor_->Cancel(kv.second.timer);
    periodics_.clear();
  }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();
  transport_->Close();
}

Status InteractionEngine::SendMessage(Message& m, const Endpoint& to) {
  if (!open_.load()) return Status::Fail("config: node not open");
  auto bytes = codec_->Encode(m);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value, to);
}

Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  return SendMessage(out, to);
}

Status InteractionEngine::RequestAwait(Message out, RequestSpec spec, const Endpoint& to) {
  if (!open_.load()) {
    if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  policy_->SetTag(out, spec.request_tag);
  Key key;
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {
      if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    key = policy_->NewCorrelation(out);
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(spec.timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p; p.spec = std::move(spec); p.out = out; p.to = to; p.timer = timer;
    pending_[key] = std::move(p);
  }
  Status st = SendMessage(out, to);
  if (!st) {
    std::function<void(Result<Message>)> cb;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.spec.on_terminal); pending_.erase(it); }
    }
    if (cb) cb(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}

void InteractionEngine::OnTimeout(Key key) {
  std::function<void(Result<Message>)> fail_cb;
  bool resend = false; Message rs; Endpoint rto;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;
    Pending& p = it->second;
    if (!p.advanced && p.retries < p.spec.max_retries) {
      ++p.retries;
      std::weak_ptr<InteractionEngine> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(p.spec.timeout_ms),
          [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
      resend = true; rs = p.out; rto = p.to;
    } else {
      fail_cb = std::move(p.spec.on_terminal);
      pending_.erase(it);
    }
  }
  if (resend) (void)SendMessage(rs, rto);
  if (fail_cb) fail_cb(Result<Message>::Fail("timeout: request timed out"));
}

void InteractionEngine::Dispatch(Message msg) {
  const Key key = policy_->KeyOf(msg);
  const FrameTag tag = policy_->TagOf(msg);
  std::function<void(const Message&)> inter_cb;
  std::function<void(Result<Message>)> term_cb;
  bool auto_ack = false; FrameTag ack_tag = 0; Message ack_req;
  bool matched = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it != pending_.end()) {
      matched = true;
      Pending& p = it->second;
      if (tag == p.spec.terminal_tag) {
        p.advanced = true;
        executor_->Cancel(p.timer);
        term_cb = std::move(p.spec.on_terminal);
        if (p.spec.auto_ack_tag.has_value()) { auto_ack = true; ack_tag = *p.spec.auto_ack_tag; ack_req = msg; }
        pending_.erase(it);
      } else if (p.spec.intermediate_tag.has_value() && tag == *p.spec.intermediate_tag) {
        p.advanced = true;
        inter_cb = p.spec.on_intermediate;  // 拷贝,保留挂起
      }
      // 否则:命中 key 但意外 tag → 忽略
    }
  }
  if (matched) {
    if (auto_ack) (void)SendReply(ack_req, ack_tag, {});  // 锁外
    if (term_cb) term_cb(Result<Message>::Success(std::move(msg)));
    else if (inter_cb) inter_cb(msg);
    return;
  }
  switch (policy_->RouteUnmatched(msg)) {
    case InteractionPolicy::Route::kInboundRequest: if (on_request_) on_request_(msg); break;
    case InteractionPolicy::Route::kDeliver:        if (on_deliver_) on_deliver_(msg); break;
    case InteractionPolicy::Route::kDrop:           break;
  }
}

Status InteractionEngine::SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload) {
  Message m; m.payload = std::move(payload);
  policy_->SetTag(m, tag);
  policy_->EchoCorrelation(m, request);
  Endpoint to = policy_->ReplyTo(request);
  return SendMessage(m, to);
}

uint32_t InteractionEngine::StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms, const Endpoint& to) {
  if (interval_ms == 0) return 0;
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = periodic_next_++;
    periodics_[handle] = Periodic{out, tag, to, interval_ms, 0};
  }
  (void)Fire(out, tag, to);  // 立即一帧
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
  return handle;
}

void InteractionEngine::FirePeriodic(uint32_t handle) {
  Message out; FrameTag tag = 0; Endpoint to; uint32_t interval = 0; bool alive = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { out = it->second.out; tag = it->second.tag; to = it->second.to; interval = it->second.interval_ms; alive = true; }
  }
  if (!alive || !open_.load()) return;
  (void)Fire(out, tag, to);
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
}

void InteractionEngine::StopPeriodic(uint32_t handle) {
  IExecutor::TimerId t = 0;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { t = it->second.timer; periodics_.erase(it); }
  }
  if (t) executor_->Cancel(t);
}

void InteractionEngine::HandleDisconnect(const std::string& reason) {
  std::map<Key, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail(reason));
  }
}

}  // namespace transport
```
把 `src/comm/InteractionEngine.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`(在 `src/comm/ThreadExecutor.cpp` 后)。

- [ ] **Step 6: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R InteractionEngine 2>&1 | grep -iE "passed|failed"`。Expected: 8 个 `InteractionEngine.*` 通过。

- [ ] **Step 7: 提交**
```bash
git add include/transport/comm/InteractionPolicy.hpp include/transport/comm/InteractionEngine.hpp src/comm/InteractionEngine.cpp tests/comm/interaction_engine_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: InteractionEngine + InteractionPolicy(3 原语/声明式策略;挂起·超时·重发·分发·periodic·并发纪律一份)"
```

---

## Task 2: `ProtocolPolicy` + `ProtocolNode` 薄壳

**Files:** Create `include/transport/comm/ProtocolPolicy.hpp`;Rewrite `include/transport/comm/ProtocolNode.hpp`、`src/comm/ProtocolNode.cpp`、`tests/comm/protocol_node_test.cpp`;Modify `CMakeLists.txt`(若 ProtocolPolicy 有 .cpp;本计划 header-only,无需改库)。

**Interfaces:**
- Consumes: `InteractionEngine`、`InteractionPolicy`、`RequestSpec`、`FrameType`(Message)、`SystemCodec`。
- Produces: `transport::ProtocolPolicy`(header-only);`ProtocolNode` 薄壳,发送方法签名 `SendNoResponse(uint16_t cmd, vector<uint8_t>)`、`Request(uint16_t cmd, vector<uint8_t>, ReplyFn)`、`RequestWithResult(uint16_t cmd, vector<uint8_t>, ReplyFn)`、`RequestNeedFeedback(uint16_t cmd, vector<uint8_t>, ReplyFn, ReplyFn)`、`StartRepeating(uint16_t cmd, vector<uint8_t>, uint32_t)`、`StopRepeating(uint32_t)`;嵌套 `Responder{Response,Result}`;钩子 `OnCommand`/`OnHeartbeat`/`OnError`。

- [ ] **Step 1: 写 `include/transport/comm/ProtocolPolicy.hpp`**
```cpp
#pragma once

// ProtocolPolicy.hpp — 外部协议策略(header-only)。Key=pack(session,message);tag=frm_type。
// session_id 滚动 0–255;message_id = 调用方填的命令码(不动);protocol_id 配置。

#include <cstdint>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class ProtocolPolicy : public InteractionPolicy {
 public:
  explicit ProtocolPolicy(uint8_t protocol_id) : protocol_id_(protocol_id) {}

  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.frm_type); }
  void SetTag(Message& m, FrameTag tag) override { m.frm_type = static_cast<FrameType>(tag); }

  Key NewCorrelation(Message& out) override {
    out.session_id = session_ctr_++;     // 滚动 0–255(uint8 自然回绕)
    out.protocol_id = protocol_id_;       // message_id 由调用方填(命令码),不动
    return Pack(out.session_id, out.message_id);
  }
  Key KeyOf(const Message& in) override { return Pack(in.session_id, in.message_id); }
  void EchoCorrelation(Message& reply, const Message& req) override {
    reply.session_id = req.session_id; reply.message_id = req.message_id; reply.protocol_id = req.protocol_id;
  }
  Endpoint ReplyTo(const Message&) override { return Endpoint::Default(); }
  Route RouteUnmatched(const Message& in) override {
    switch (in.frm_type) {
      case FrameType::kCommand: case FrameType::kState: return Route::kInboundRequest;
      case FrameType::kHeartbeat: return Route::kDeliver;
      default: return Route::kDrop;  // RESPONSE/RESULT(无挂起)/UNKNOWN
    }
  }

 private:
  static Key Pack(uint8_t s, uint16_t m) {
    Key k(3, '\0');
    k[0] = static_cast<char>(s);
    k[1] = static_cast<char>(m & 0xFF);
    k[2] = static_cast<char>((m >> 8) & 0xFF);
    return k;
  }
  uint8_t protocol_id_;
  uint8_t session_ctr_ = 0;
};

}  // namespace transport
```

- [ ] **Step 2: 改失败测试 `tests/comm/protocol_node_test.cpp` —— 发送调用补 `cmd` 参数**

测试里所有 `SendNoResponse(P({...}))` / `Request(P({...}), cb)` / `RequestWithResult(...)` / `RequestNeedFeedback(...)` / `StartRepeating(P({...}), iv)` 改为首参补一个命令码(用任意常量,如 `0x10`)。例如:
```bash
cd /home/ubuntu/david/transport
sed -i -E 's/SendNoResponse\(P\(/SendNoResponse(0x10, P(/g' tests/comm/protocol_node_test.cpp
sed -i -E 's/->Request\(P\(/->Request(0x10, P(/g' tests/comm/protocol_node_test.cpp
sed -i -E 's/RequestWithResult\(P\(/RequestWithResult(0x10, P(/g' tests/comm/protocol_node_test.cpp
sed -i -E 's/RequestNeedFeedback\(\s*P\(/RequestNeedFeedback(0x10, P(/g' tests/comm/protocol_node_test.cpp
sed -i -E 's/StartRepeating\(P\(/StartRepeating(0x10, P(/g' tests/comm/protocol_node_test.cpp
```
> 然后**人工核对** `protocol_node_test.cpp`:每个发送调用首参是命令码 `uint16_t`;`RequestNeedFeedback` 跨行的也补上;断言不变。`RepeatingZeroIntervalRejected` 的 `StartRepeating(P({0xAB}), 0)` 应变为 `StartRepeating(0x10, P({0xAB}), 0)`。若 sed 未覆盖到跨行调用,手动补全。

- [ ] **Step 3: 写 `include/transport/comm/ProtocolNode.hpp`(整体替换为薄壳)**
```cpp
#pragma once

// ProtocolNode.hpp — 外部协议节点(薄壳)。持 InteractionEngine + ProtocolPolicy;
// 命名模式翻译成引擎原语 + frm_type 常量。须以 shared_ptr 持有。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionEngine.hpp"

namespace transport {

struct ProtocolConfig {
  uint8_t  protocol_id = 0;
  uint32_t response_timeout_ms = 1000;
  uint32_t max_retries = 3;
  uint32_t heartbeat_interval_ms = 0;
};

class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  class Responder {
   public:
    Status Response(std::vector<uint8_t> payload);
    Status Result(std::vector<uint8_t> payload);
   private:
    friend class ProtocolNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;
    Message request_;
  };

  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,          // null → SystemCodec
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,
               std::size_t queue_capacity = 1024);
  virtual ~ProtocolNode();

  Status Open();
  void   Close();
  bool   IsOpen() const;

  Status SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload);
  Status Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response);
  Status RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result);
  Status RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                             ReplyFn on_response, ReplyFn on_result);
  uint32_t StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms);
  void     StopRepeating(uint32_t handle);

 protected:
  virtual void OnCommand(const Message& cmd, Responder responder) {}
  virtual void OnHeartbeat(const Message& hb) {}
  virtual void OnError(const std::string& error) {}

 private:
  void WireHandlers();
  std::shared_ptr<InteractionEngine> engine_;
  ProtocolConfig config_;
  uint32_t heartbeat_handle_ = 0;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/comm/ProtocolNode.cpp`(整体替换为薄壳)**
```cpp
#include "transport/comm/ProtocolNode.hpp"

#include <utility>

#include "transport/codec/SystemCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"

// ProtocolNode.cpp — 薄壳:命名模式 → 引擎原语 + frm_type 常量。

namespace transport {

namespace {
int Tag(FrameType t) { return static_cast<int>(t); }
Message Cmd(uint16_t cmd, std::vector<uint8_t> p) { Message m; m.message_id = cmd; m.payload = std::move(p); return m; }
}  // namespace

Status ProtocolNode::Responder::Response(std::vector<uint8_t> payload) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(FrameType::kResponse), std::move(payload));
}
Status ProtocolNode::Responder::Result(std::vector<uint8_t> payload) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(FrameType::kResult), std::move(payload));
}

ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                           ProtocolConfig config, std::unique_ptr<IExecutor> executor,
                           std::size_t queue_capacity)
    : engine_(std::make_shared<InteractionEngine>(
          std::move(transport),
          codec ? std::move(codec) : std::unique_ptr<ICodec>(new SystemCodec()),
          std::unique_ptr<InteractionPolicy>(new ProtocolPolicy(config.protocol_id)),
          std::move(executor), queue_capacity)),
      config_(config) {}

ProtocolNode::~ProtocolNode() { Close(); }

void ProtocolNode::WireHandlers() {
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  engine_->OnInboundRequest([wself, weng](const Message& req) {
    auto s = wself.lock(); if (!s) return;
    s->OnCommand(req, Responder(weng, req));
  });
  engine_->OnInboundDeliver([wself](const Message& m) {
    auto s = wself.lock(); if (!s) return;
    if (m.frm_type == FrameType::kHeartbeat) s->OnHeartbeat(m);
  });
  engine_->OnError([wself](const std::string& e) { if (auto s = wself.lock()) s->OnError(e); });
}

Status ProtocolNode::Open() {
  WireHandlers();
  auto st = engine_->Open();
  if (!st) return st;
  if (config_.heartbeat_interval_ms > 0)
    heartbeat_handle_ = engine_->StartPeriodic(Cmd(0, {}), Tag(FrameType::kHeartbeat),
                                               config_.heartbeat_interval_ms);
  return Status::Success(std::monostate{});
}

void ProtocolNode::Close() { engine_->Close(); }
bool ProtocolNode::IsOpen() const { return engine_->IsOpen(); }

Status ProtocolNode::SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload) {
  return engine_->Fire(Cmd(cmd, std::move(payload)), Tag(FrameType::kCommand));
}

Status ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResponse);
  s.on_terminal = std::move(on_response); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

Status ProtocolNode::RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResult);
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

Status ProtocolNode::RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand);
  s.intermediate_tag = Tag(FrameType::kResponse); s.terminal_tag = Tag(FrameType::kResult);
  s.auto_ack_tag = Tag(FrameType::kResponse);
  s.on_intermediate = [cb = std::move(on_response)](const Message& m) { if (cb) cb(Result<Message>::Success(m)); };
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms) {
  return engine_->StartPeriodic(Cmd(cmd, std::move(payload)), Tag(FrameType::kState), interval_ms);
}

void ProtocolNode::StopRepeating(uint32_t handle) { engine_->StopPeriodic(handle); }

}  // namespace transport
```
> 注:`RequestNeedFeedback` 的 `on_response`(中间回应)在协议里是 `ReplyFn`(收 `Result<Message>`),而引擎 `on_intermediate` 是 `function<void(const Message&)>` —— 故包一层把 `Message` 升成 `Result<Message>::Success`。

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:"; ctest --test-dir build -R ProtocolNode 2>&1 | grep -iE "passed|failed"`。Expected: 无 warning/error;`protocol_node_test` 全部 `ProtocolNode.*` 通过(补 cmd 参数后)。

- [ ] **Step 6: 提交**
```bash
git add include/transport/comm/ProtocolPolicy.hpp include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor: ProtocolNode 改为 InteractionEngine+ProtocolPolicy 薄壳;message_id=命令码(加 cmd 参数)"
```

---

## Task 3: `DdsPolicy` + `DdsNode` 薄壳

**Files:** Create `include/transport/comm/DdsPolicy.hpp`;Rewrite `include/transport/comm/DdsNode.hpp`、`src/comm/DdsNode.cpp`;Modify `tests/comm/dds_node_test.cpp`(仅去 CommNode 依赖:删 2 个 `using` + 删 1 个基类句柄 TEST;DDS 行为/其余断言不变)。

**Interfaces:**
- Consumes: `InteractionEngine`/`InteractionPolicy`/`RequestSpec`、`IDdsTransport`、`DdsCodec`、`MessageKind`。
- Produces: `transport::DdsPolicy`(header-only);`DdsNode` 薄壳(公开 API 与现状一致:ctor `(shared_ptr<IDdsTransport>, string inbox, unique_ptr<ICodec>=nullptr→DdsCodec, unique_ptr<IExecutor>=nullptr, size_t)`;`Open/Close/IsOpen`、`Subscribe`/`Unsubscribe`、`Send(Message, Endpoint=Default)`、`Request(Message, ReplyFn, uint32_t, Endpoint=Default)`、`Request(Message, FeedbackFn, ReplyFn, uint32_t, Endpoint=Default)`、`Request(Message, uint32_t, Endpoint=Default)→future`;嵌套 `Responder{Reply,Feedback}`;钩子 `OnMessage`/`OnRequest`)。

- [ ] **Step 1: 写 `include/transport/comm/DdsPolicy.hpp`**
```cpp
#pragma once

// DdsPolicy.hpp — DDS 策略(header-only)。Key=correlation_id;tag=kind;reply_to=inbox(请求)/路由(应答)。

#include <atomic>
#include <cstdint>
#include <random>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class DdsPolicy : public InteractionPolicy {
 public:
  explicit DdsPolicy(std::string inbox) : inbox_(std::move(inbox)) {
    std::random_device rd;
    prefix_ = std::to_string(rd()) + "-";
  }

  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.kind); }
  void SetTag(Message& m, FrameTag tag) override { m.kind = static_cast<MessageKind>(tag); }

  Key NewCorrelation(Message& out) override {
    out.correlation_id = prefix_ + std::to_string(++seq_);
    out.reply_to = inbox_;
    return out.correlation_id;
  }
  Key KeyOf(const Message& in) override { return in.correlation_id; }
  void EchoCorrelation(Message& reply, const Message& req) override { reply.correlation_id = req.correlation_id; }
  Endpoint ReplyTo(const Message& req) override {
    return req.reply_to.empty() ? Endpoint::Default() : Endpoint::Topic(req.reply_to);
  }
  Route RouteUnmatched(const Message& in) override {
    switch (in.kind) {
      case MessageKind::kRequest: return Route::kInboundRequest;
      case MessageKind::kOneway: case MessageKind::kNotify: return Route::kDeliver;
      default: return Route::kDrop;  // kReply/kFeedback(无挂起)
    }
  }

 private:
  std::string inbox_;
  std::string prefix_;
  uint64_t seq_ = 0;
};

}  // namespace transport
```

- [ ] **Step 2: 运行 dds_node_test,确认现状(基线)** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R DdsNode 2>&1 | grep -iE "passed|failed"`。Expected: 现有 DdsNode 测试通过(改写前基线)。

- [ ] **Step 3: 写 `include/transport/comm/DdsNode.hpp`(整体替换为薄壳)**
```cpp
#pragma once

// DdsNode.hpp — DDS 节点(薄壳)。持 InteractionEngine + DdsPolicy;发布=Fire(Topic),请求=RequestAwait(Topic);
// 另持 IDdsTransport 供 Subscribe;Open 自动订阅 inbox。须以 shared_ptr 持有。

#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionEngine.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

using ReplyFn    = std::function<void(Result<Message>)>;
using FeedbackFn = std::function<void(const Message&)>;

class DdsNode : public std::enable_shared_from_this<DdsNode> {
 public:
  class Responder {
   public:
    Status Reply(Message msg);
    Status Feedback(Message msg);
   private:
    friend class DdsNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;
    Message request_;
  };

  DdsNode(std::shared_ptr<IDdsTransport> transport,
          std::string inbox_topic,
          std::unique_ptr<ICodec> codec = nullptr,        // null → DdsCodec
          std::unique_ptr<IExecutor> executor = nullptr,
          std::size_t queue_capacity = 1024);
  virtual ~DdsNode();

  Status Open();
  void   Close();
  bool   IsOpen() const;

  Status Subscribe(const std::string& topic);
  Status Unsubscribe(const std::string& topic);

  Status Send(Message msg, const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms,
                                       const Endpoint& to = Endpoint::Default());

 protected:
  virtual void OnMessage(const Message& msg) {}
  virtual void OnRequest(const Message& req, Responder responder) {}

 private:
  void WireHandlers();
  std::shared_ptr<IDdsTransport> dds_;
  std::shared_ptr<InteractionEngine> engine_;
  std::string inbox_topic_;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/comm/DdsNode.cpp`(整体替换为薄壳)**
```cpp
#include "transport/comm/DdsNode.hpp"

#include <utility>

#include "transport/codec/DdsCodec.hpp"
#include "transport/comm/DdsPolicy.hpp"

// DdsNode.cpp — 薄壳:发布=Fire(kNotify,Topic),请求=RequestAwait(kRequest/kReply,Topic);Open 订阅 inbox。

namespace transport {

namespace {
int Tag(MessageKind k) { return static_cast<int>(k); }
}  // namespace

Status DdsNode::Responder::Reply(Message msg) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kReply), std::move(msg.payload));
}
Status DdsNode::Responder::Feedback(Message msg) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kFeedback), std::move(msg.payload));
}

DdsNode::DdsNode(std::shared_ptr<IDdsTransport> transport, std::string inbox_topic,
                 std::unique_ptr<ICodec> codec, std::unique_ptr<IExecutor> executor,
                 std::size_t queue_capacity)
    : dds_(transport),
      engine_(std::make_shared<InteractionEngine>(
          transport,
          codec ? std::move(codec) : std::unique_ptr<ICodec>(new DdsCodec()),
          std::unique_ptr<InteractionPolicy>(new DdsPolicy(inbox_topic)),
          std::move(executor), queue_capacity)),
      inbox_topic_(std::move(inbox_topic)) {}

DdsNode::~DdsNode() { Close(); }

void DdsNode::WireHandlers() {
  std::weak_ptr<DdsNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  engine_->OnInboundRequest([wself, weng](const Message& req) {
    if (auto s = wself.lock()) s->OnRequest(req, Responder(weng, req));
  });
  engine_->OnInboundDeliver([wself](const Message& m) {
    if (auto s = wself.lock()) s->OnMessage(m);
  });
}

Status DdsNode::Open() {
  WireHandlers();
  auto st = engine_->Open();
  if (!st) return st;
  return Subscribe(inbox_topic_);
}

void DdsNode::Close() { engine_->Close(); }
bool DdsNode::IsOpen() const { return engine_->IsOpen(); }

Status DdsNode::Subscribe(const std::string& topic)   { return dds_->Subscribe(topic); }
Status DdsNode::Unsubscribe(const std::string& topic) { return dds_->Unsubscribe(topic); }

Status DdsNode::Send(Message msg, const Endpoint& to) {
  return engine_->Fire(std::move(msg), Tag(MessageKind::kNotify), to);
}

Status DdsNode::Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms, const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_terminal = std::move(on_reply); s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

Status DdsNode::Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                        const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest);
  s.intermediate_tag = Tag(MessageKind::kFeedback); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_intermediate = std::move(on_feedback); s.on_terminal = std::move(on_final);
  s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

std::future<Result<Message>> DdsNode::Request(Message msg, uint32_t timeout_ms, const Endpoint& to) {
  auto prom = std::make_shared<std::promise<Result<Message>>>();
  auto fut = prom->get_future();
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_terminal = [prom](Result<Message> r) { prom->set_value(std::move(r)); };
  s.timeout_ms = timeout_ms; s.max_retries = 0;
  (void)engine_->RequestAwait(std::move(msg), std::move(s), to);
  return fut;
}

}  // namespace transport
```

- [ ] **Step 5: 去 `tests/comm/dds_node_test.cpp` 的 CommNode 依赖(3 处)**

DdsNode 不再继承 CommNode、其 `Responder` 改为嵌套 `DdsNode::Responder`,故测试去掉对 CommNode 的依赖:
```bash
cd /home/ubuntu/david/transport
sed -i '/using transport::CommNode;/d' tests/comm/dds_node_test.cpp
sed -i 's/using transport::Responder;/using Responder = transport::DdsNode::Responder;/' tests/comm/dds_node_test.cpp
```
再**删除整个 `TEST(DdsNode, OpenIsVirtualThroughBaseHandle) { ... }`**(连同其上一行注释 `// Open 已虚化:...`):它用 `std::shared_ptr<CommNode> a_base = a;`,依赖已不存在的继承关系;「Open 订阅 inbox」由其余 DDS 用例(如 `RequestReplyOverTopics`,应答经 inbox 回送)覆盖,无覆盖损失。
> 人工核对:`grep -n CommNode tests/comm/dds_node_test.cpp` 应为空;测试体里 `Responder` 用 `using` 别名自动解析到嵌套类型,其余断言不动。

- [ ] **Step 6: 运行,确认通过** `cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:"; ctest --test-dir build -R DdsNode 2>&1 | grep -iE "passed|failed"`。Expected: 无 warning/error;`dds_node_test` 全部通过(仅去 CommNode 依赖、删 1 个基类句柄用例,其余断言不变)。

- [ ] **Step 7: 提交**
```bash
git add include/transport/comm/DdsPolicy.hpp include/transport/comm/DdsNode.hpp src/comm/DdsNode.cpp tests/comm/dds_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor: DdsNode 改为 InteractionEngine+DdsPolicy 薄壳;dds_node_test 去 CommNode 依赖"
```

---

## Task 4: 移除 `CommNode` + 全量验证

**Files:** Remove `include/transport/comm/CommNode.hpp`、`src/comm/CommNode.cpp`、`tests/comm/comm_node_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 删文件 + 从 CMake 摘除**
```bash
cd /home/ubuntu/david/transport
git rm include/transport/comm/CommNode.hpp src/comm/CommNode.cpp tests/comm/comm_node_test.cpp
```
然后改 `CMakeLists.txt`:从 `add_library(transport STATIC ...)` 删除 `src/comm/CommNode.cpp` 那一行;从 `add_executable(transport_tests ...)` 删除 `tests/comm/comm_node_test.cpp` 那一行。确认无其它文件 `#include "transport/comm/CommNode.hpp"`:
```bash
grep -rn "CommNode" include src tests || echo "(无残留 CommNode 引用)"
```
Expected: 无残留(DdsNode 已不继承 CommNode)。

- [ ] **Step 2: 干净构建零告警**
```bash
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings (none)---"
```
Expected: 无 `warning`/`error:`。

- [ ] **Step 3: 全量测试连跑两次**
```bash
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 两次都 `0 tests failed`(移除 comm_node_test 7 条、新增 interaction_engine_test 8 条;ProtocolNode/DdsNode 测试仍全绿)。以 0 failed + 两次一致为准。

- [ ] **Step 4: 解耦/失败信号检查(引擎里无 per-node 特判)**
```bash
grep -rn "frm_type\|FrameType\|MessageKind\|ProtocolNode\|DdsNode\|kRequest\|kReply" include/transport/comm/InteractionEngine.hpp src/comm/InteractionEngine.cpp include/transport/comm/InteractionPolicy.hpp || echo "(引擎/策略接口不含任何具体协议判别符或节点名 —— 缝切对)"
```
Expected: 无输出(引擎只认抽象 `FrameTag`/`Key`,不提具体 frm_type/kind/节点)。

- [ ] **Step 5: 提交**
```bash
git add -A
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor: 移除 CommNode(交互机制已抽入 InteractionEngine);comm_node_test 转引擎级测试"
```

---

## 完成标准
- `InteractionEngine`(3 原语 + 挂起/超时/重发/分发/periodic/并发纪律)+ `InteractionPolicy`(声明式)落地;引擎级测试覆盖原 comm_node_test 的通用交互面。
- `ProtocolNode` 改薄壳(`message_id`=命令码、加 `cmd` 参数,`protocol_node_test` 仅补参数);`DdsNode` 改薄壳(`dds_node_test` 仅去 CommNode 依赖、断言不变,全绿)。
- `CommNode` 移除;引擎里无 per-node 特判(`FrameTag`/`Key` 抽象);并发纪律仅一处。
- 干净构建零告警;全量两次稳定 0 failed。
- 范围外(未做):第四原语、抽象 `INode`、新协议(= 新 policy)。
