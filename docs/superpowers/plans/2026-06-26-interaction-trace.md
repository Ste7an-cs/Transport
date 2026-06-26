# 可插拔结构化 Trace 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `InteractionEngine` 加一套可插拔、结构化、近零开销的 trace,让分发/超时/重发/auto-ack/periodic/收发/关闭全程可观测。

**Architecture:** 层中立 `ITraceSink`(`include/transport/ITraceSink.hpp`)+ 零分配 `TraceEvent` 视图结构 + 两个内置 sink。sink 只注入 `InteractionEngine`(咽喉点),节点透传一行。每个埋点 `if (trace_) ...` 守卫;无 sink ≈ 免费。transport/codec 本版不改(预留 category)。

**Tech Stack:** C++17,不抛异常,GoogleTest,CMake。配套 spec:`docs/superpowers/specs/2026-06-26-interaction-trace-design.md`。

## Global Constraints

- C++17,**不抛异常**;延续 `Result`/`Status` 风格,trace 只观测、不改错误模型。
- sink 与 `TraceEvent` **header-only**;接口层零第三方依赖。
- `ITraceSink.hpp` 放 `include/transport/` 根(层中立),**不放 `comm/`**。
- `TraceEvent` 全字段为 `std::string_view` + `int`/`long`,**构造零分配**;sink 实现须**线程安全**(io/worker/调用方线程并发调用)。
- 每个埋点用 `if (trace_)` 守卫,**无 sink 时不构造事件、不格式化**(近零开销)。
- 现有 86 个测试不回归;无 sink 时字节流逐字不变。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**无 Co-Authored-By trailer**。
- 不提交 `build/`。文档(SRS/SDD/README/CHANGELOG)同步留到实现后单独一轮。

---

## File Structure

| 文件 | 责任 | 任务 |
|---|---|---|
| `include/transport/ITraceSink.hpp`(新) | `TraceLevel`/`TraceEvent`/`ITraceSink` + `OstreamTraceSink` + `CapturingTraceSink`,全 header-only | T1 |
| `tests/comm/trace_sink_test.cpp`(新) | 两个内置 sink 的单元测试 | T1 |
| `include/transport/comm/InteractionEngine.hpp`(改) | 加 `trace_` 成员 + `SetTrace` + 私有 `Trace()` | T2 |
| `src/comm/InteractionEngine.cpp`(改) | 各咽喉点埋 trace | T2 |
| `tests/comm/fake_transport.hpp`(改) | 加 `InjectBytes`(测试用原始字节注入) | T2 |
| `tests/comm/interaction_trace_test.cpp`(新) | 引擎事件目录测试 | T2 |
| `include/transport/comm/DdsNode.hpp`(改) | 透传 `SetTrace` | T3 |
| `include/transport/comm/ProtocolNode.hpp`(改) | 透传 `SetTrace` | T3 |
| `tests/comm/protocol_node_test.cpp`(改) | 节点透传 trace 的回归测试 | T3 |
| `CMakeLists.txt`(改) | 注册两个新测试文件 | T1/T2 |

---

### Task 1: `ITraceSink.hpp`（接口 + 两个内置 sink）

**Files:**
- Create: `include/transport/ITraceSink.hpp`
- Create: `tests/comm/trace_sink_test.cpp`
- Modify: `CMakeLists.txt`(在 `add_executable(transport_tests ...)` 源列表加一行)

**Interfaces:**
- Produces:
  - `enum class transport::TraceLevel { kTrace, kDebug, kInfo, kWarn, kError };`
  - `constexpr int transport::kNoTag = -9999;` `constexpr long transport::kNoNum = -1;`
  - `struct transport::TraceEvent { TraceLevel level; std::string_view category, message, key, endpoint, error; int tag=kNoTag; long size=kNoNum; int attempt=-1; };`
  - `class transport::ITraceSink { virtual void OnTrace(const TraceEvent&) = 0; };`
  - `class transport::OstreamTraceSink : public ITraceSink`(构造 `(std::ostream& = std::cerr, TraceLevel min = kDebug)`)
  - `class transport::CapturingTraceSink : public ITraceSink`(`std::vector<Record> Records() const; void Clear();`,`Record` 含 `level` 与各字段的 `std::string`/`int`/`long` 深拷贝)

- [ ] **Step 1: 写失败测试** `tests/comm/trace_sink_test.cpp`

```cpp
#include "transport/ITraceSink.hpp"

#include <sstream>
#include <gtest/gtest.h>

using transport::CapturingTraceSink;
using transport::OstreamTraceSink;
using transport::TraceEvent;
using transport::TraceLevel;
using transport::kNoNum;
using transport::kNoTag;

TEST(OstreamTraceSink, FormatsOnlyNonEmptyFields) {
  std::ostringstream os;
  OstreamTraceSink sink(os, TraceLevel::kTrace);
  TraceEvent ev{TraceLevel::kDebug, "dispatch", "match-terminal", "01.0005", "", "", 3, kNoNum, -1};
  sink.OnTrace(ev);
  const std::string line = os.str();
  EXPECT_NE(line.find("[D]"), std::string::npos);
  EXPECT_NE(line.find("dispatch"), std::string::npos);
  EXPECT_NE(line.find("match-terminal"), std::string::npos);
  EXPECT_NE(line.find("key=01.0005"), std::string::npos);
  EXPECT_NE(line.find("tag=3"), std::string::npos);
  EXPECT_EQ(line.find("size="), std::string::npos);     // kNoNum 不打印
  EXPECT_EQ(line.find("attempt="), std::string::npos);  // -1 不打印
  EXPECT_EQ(line.find("ep="), std::string::npos);       // 空不打印
}

TEST(OstreamTraceSink, FiltersBelowMinLevel) {
  std::ostringstream os;
  OstreamTraceSink sink(os, TraceLevel::kWarn);
  sink.OnTrace(TraceEvent{TraceLevel::kDebug, "send", "", "", "", "", 1, 7, -1});
  EXPECT_TRUE(os.str().empty());                         // Debug < Warn,被过滤
  sink.OnTrace(TraceEvent{TraceLevel::kError, "error", "", "", "", "io: boom", kNoTag, kNoNum, -1});
  EXPECT_NE(os.str().find("[E]"), std::string::npos);
  EXPECT_NE(os.str().find("err=io: boom"), std::string::npos);
}

TEST(CapturingTraceSink, DeepCopiesEvents) {
  CapturingTraceSink cap;
  {
    std::string cat = "retransmit";
    std::string key = "k1";
    cap.OnTrace(TraceEvent{TraceLevel::kDebug, cat, "", key, "", "", kNoTag, kNoNum, 2});
  }  // cat/key 已析构 — 深拷贝必须仍有效
  ASSERT_EQ(cap.Records().size(), 1u);
  EXPECT_EQ(cap.Records()[0].category, "retransmit");
  EXPECT_EQ(cap.Records()[0].key, "k1");
  EXPECT_EQ(cap.Records()[0].attempt, 2);
  EXPECT_EQ(cap.Records()[0].level, TraceLevel::kDebug);
  cap.Clear();
  EXPECT_TRUE(cap.Records().empty());
}
```

- [ ] **Step 2: 注册测试,确认编译失败**

在 `CMakeLists.txt` 的 `add_executable(transport_tests` 源列表中,`tests/comm/interaction_engine_test.cpp` 行之后加:
```
    tests/comm/trace_sink_test.cpp
```
Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `transport/ITraceSink.hpp: No such file or directory`。

- [ ] **Step 3: 实现 `include/transport/ITraceSink.hpp`**

```cpp
#pragma once

// ITraceSink.hpp — 可插拔结构化 trace。层中立:放 transport 根,供任意层(当前仅
// InteractionEngine)注入。事件为零分配视图结构;sink 实现须线程安全。

#include <cstddef>
#include <iostream>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace transport {

enum class TraceLevel { kTrace, kDebug, kInfo, kWarn, kError };

inline constexpr int  kNoTag = -9999;  // TraceEvent.tag 哨兵:无判别符
inline constexpr long kNoNum = -1;     // size/attempt 哨兵:无该数值

// 轻量"视图"事件:string_view 指向调用点已存在的数据,构造零分配。
// sink 若需在 OnTrace 返回后留存,须自行拷贝(见 CapturingTraceSink)。
struct TraceEvent {
  TraceLevel       level;
  std::string_view category;   // 静态字面量:"send"/"recv"/"dispatch"/"timeout"/...
  std::string_view message;    // 短静态子原因(可空),如 "match-terminal"
  std::string_view key;        // 相关键(可空)
  std::string_view endpoint;   // endpoint / from / route 名(可空)
  std::string_view error;      // 错误串(可空)
  int  tag     = kNoTag;       // FrameTag
  long size    = kNoNum;       // 字节数 / 计数(按 category 解释)
  int  attempt = -1;           // 重发第几次(-1=无)
};

// 可能被 io / worker / 调用方线程并发调用 → 实现必须线程安全。
class ITraceSink {
 public:
  virtual ~ITraceSink() = default;
  virtual void OnTrace(const TraceEvent& ev) = 0;
};

// ---- 内置 sink:格式化到 ostream(默认 std::cerr) ----
class OstreamTraceSink : public ITraceSink {
 public:
  explicit OstreamTraceSink(std::ostream& os = std::cerr, TraceLevel min = TraceLevel::kDebug)
      : os_(os), min_(min) {}

  void OnTrace(const TraceEvent& ev) override {
    if (static_cast<int>(ev.level) < static_cast<int>(min_)) return;
    std::lock_guard<std::mutex> lk(mu_);
    os_ << '[' << Letter(ev.level) << "] " << ev.category;
    if (!ev.message.empty())  os_ << ' ' << ev.message;
    if (!ev.key.empty())      os_ << " key=" << ev.key;
    if (!ev.endpoint.empty()) os_ << " ep=" << ev.endpoint;
    if (ev.tag != kNoTag)     os_ << " tag=" << ev.tag;
    if (ev.size != kNoNum)    os_ << " size=" << ev.size;
    if (ev.attempt >= 0)      os_ << " attempt=" << ev.attempt;
    if (!ev.error.empty())    os_ << " err=" << ev.error;
    os_ << '\n';
  }

 private:
  static char Letter(TraceLevel l) {
    switch (l) {
      case TraceLevel::kTrace: return 'T';
      case TraceLevel::kDebug: return 'D';
      case TraceLevel::kInfo:  return 'I';
      case TraceLevel::kWarn:  return 'W';
      case TraceLevel::kError: return 'E';
    }
    return '?';
  }
  std::ostream& os_;
  TraceLevel min_;
  std::mutex mu_;
};

// ---- 内置 sink:深拷贝留存,供测试/内省 ----
class CapturingTraceSink : public ITraceSink {
 public:
  struct Record {
    TraceLevel level;
    std::string category, message, key, endpoint, error;
    int tag; long size; int attempt;
  };

  void OnTrace(const TraceEvent& ev) override {
    std::lock_guard<std::mutex> lk(mu_);
    records_.push_back(Record{ev.level, std::string(ev.category), std::string(ev.message),
                              std::string(ev.key), std::string(ev.endpoint), std::string(ev.error),
                              ev.tag, ev.size, ev.attempt});
  }

  std::vector<Record> Records() const {
    std::lock_guard<std::mutex> lk(mu_);
    return records_;
  }
  void Clear() {
    std::lock_guard<std::mutex> lk(mu_);
    records_.clear();
  }

 private:
  mutable std::mutex mu_;
  std::vector<Record> records_;
};

}  // namespace transport
```

- [ ] **Step 4: 跑测试,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R "OstreamTraceSink|CapturingTraceSink" --output-on-failure`
Expected: 3 tests PASS。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(原 86 + 新 3 = 89)。
```bash
git add include/transport/ITraceSink.hpp tests/comm/trace_sink_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ITraceSink 接口 + OstreamTraceSink/CapturingTraceSink(层中立 header-only)"
```

---

### Task 2: 引擎埋点（`SetTrace` + `Trace()` + 各咽喉点)

**Files:**
- Modify: `include/transport/comm/InteractionEngine.hpp`
- Modify: `src/comm/InteractionEngine.cpp`
- Modify: `tests/comm/fake_transport.hpp`(加 `InjectBytes`)
- Create: `tests/comm/interaction_trace_test.cpp`
- Modify: `CMakeLists.txt`(注册 `interaction_trace_test.cpp`)

**Interfaces:**
- Consumes(来自 T1):`transport::ITraceSink`、`TraceEvent`、`TraceLevel`、`kNoTag`、`kNoNum`。
- Produces:`void InteractionEngine::SetTrace(std::shared_ptr<ITraceSink>)`(**须在 Open() 前调用**)。

**埋点目录(实现依据,均为引擎在该点已持有的数据):**

| category / message | level | 站点 | 字段 |
|---|---|---|---|
| `open` | Info | Open() 成功末尾 | — |
| `close` | Info | Close() 取 taken 后 | size=taken.size() |
| `conn`/`connect` | Info | OnConnect 回调 | — |
| `conn`/`disconnect` | Info | OnDisconnect 回调 | error=reason |
| `recv` | Trace | OnBytes 收到(`r` ok) | size=字节, endpoint=from |
| `decode`/`decode-fail` | Warn | OnBytes `!msgs` 分支 | error |
| `error` | Error | OnBytes `!r` 分支(transport 读错误) | error |
| `send` | Debug | Fire 内 SetTag 后 | tag, size=payload, endpoint=to |
| `request` | Debug | RequestAwait 登记后(锁外) | key, tag, size=payload, endpoint=to |
| `reply` | Debug | SendReply 内 | key, tag, size=payload, endpoint |
| `periodic`/`start`,`fire`,`stop` | Trace | StartPeriodic/FirePeriodic/StopPeriodic | tag |
| `dispatch`/`match-terminal` | Debug | Dispatch 命中终结(锁外) | key, tag |
| `dispatch`/`match-intermediate` | Debug | Dispatch 命中中间(锁外) | key, tag |
| `dispatch`/`auto-ack` | Trace | Dispatch auto_ack(锁外) | key, tag=ack_tag |
| `unmatched`/`request`,`deliver`,`drop` | Trace | Dispatch 无主路由 | tag |
| `retransmit` | Debug | OnTimeout 重发(锁外) | key, attempt=retries |
| `timeout` | Warn | OnTimeout 失败(锁外) | key |

> **精化(相对 spec §5):** 解码失败只发 `decode/decode-fail`(Warn);`error`(Error)仅用于 transport 读错误分支(`!r`)。二者都仍触发现有 `on_error_` 业务回调(不变)。每个错误恰好一条 trace,不重复。

- [ ] **Step 1: 给 `fake_transport.hpp` 加 `InjectBytes`(测试用)**

在 `tests/comm/fake_transport.hpp` 的 `FakeTransport` public 区(`OnBytes` 定义之后)加:
```cpp
  // 测试用:把原始字节直接喂给本端 OnBytes(模拟对端发来,绕过编码)。
  void InjectBytes(std::vector<uint8_t> bytes) {
    if (bytes_cb_) bytes_cb_(transport::Result<std::vector<uint8_t>>::Success(std::move(bytes)), "inject");
  }
```
Run: `cmake --build build -j$(nproc) 2>&1 | tail -3`
Expected: 编译通过(纯新增方法,无调用方)。

- [ ] **Step 2: 写失败测试 `tests/comm/interaction_trace_test.cpp`**

```cpp
#include "transport/comm/InteractionEngine.hpp"
#include "transport/ITraceSink.hpp"

#include "transport/codec/DdsCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

using transport::CapturingTraceSink;
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
using transport::Status;
using transport::TraceLevel;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
// 与 interaction_engine_test 同形的最小 policy(此文件独立,故重声明)。
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

// 计某 category(+可选 message)在记录里出现的次数。
size_t Count(const CapturingTraceSink& c, const std::string& cat, const std::string& msg = "") {
  size_t n = 0;
  for (const auto& r : c.Records())
    if (r.category == cat && (msg.empty() || r.message == msg)) ++n;
  return n;
}

struct Net {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<InteractionEngine> a, b;
  InlineExecutor* exa = nullptr;
  std::shared_ptr<CapturingTraceSink> cap = std::make_shared<CapturingTraceSink>();
  Net() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::unique_ptr<IExecutor>(std::move(ea)));
    b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
    a->SetTrace(cap);                 // 仅 a 装 sink
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(InteractionTrace, FireEmitsSend) {
  Net n;
  n.b->OnInboundDeliver([](const Message&) {});
  n.Open();
  ASSERT_TRUE(static_cast<bool>(n.a->Fire(Pay({1, 2, 3}), T(MessageKind::kOneway))));
  ASSERT_EQ(Count(*n.cap, "send"), 1u);
  for (const auto& r : n.cap->Records())
    if (r.category == "send") { EXPECT_EQ(r.tag, T(MessageKind::kOneway)); EXPECT_EQ(r.size, 3); }
  n.Close();
}

TEST(InteractionTrace, RequestThenTerminal) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    auto out = req.payload; out.push_back(0xEE);
    (void)n.b->SendReply(req, T(MessageKind::kReply), out);
  });
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  EXPECT_EQ(Count(*n.cap, "request"), 1u);
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-terminal"), 1u);
  n.Close();
}

TEST(InteractionTrace, IntermediateThenTerminal) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    (void)n.b->SendReply(req, T(MessageKind::kFeedback), {1});  // 中间
    (void)n.b->SendReply(req, T(MessageKind::kReply), {2});     // 终结
  });
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.on_intermediate = [](const Message&) {}; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-intermediate"), 1u);
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-terminal"), 1u);
  n.Close();
}

TEST(InteractionTrace, RetransmitsThenTimeout) {
  Net n;
  n.b->OnInboundRequest([](const Message&) {});  // 不回应 → 超时
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 10; s.max_retries = 2; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  n.exa->FireAll();  // 第1次超时 → 重发1 + 重排
  n.exa->FireAll();  // 第2次超时 → 重发2 + 重排
  n.exa->FireAll();  // 第3次超时 → 达上限,失败
  EXPECT_EQ(Count(*n.cap, "retransmit"), 2u);
  EXPECT_EQ(Count(*n.cap, "timeout"), 1u);
  n.Close();
}

TEST(InteractionTrace, CloseEmitsFinalizedCount) {
  Net n;
  n.b->OnInboundRequest([](const Message&) {});  // 不回应,留一个挂起
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 10000; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  n.a->Close();
  bool found = false;
  for (const auto& r : n.cap->Records())
    if (r.category == "close") { found = true; EXPECT_EQ(r.size, 1); }
  EXPECT_TRUE(found);
  n.b->Close();
}

TEST(InteractionTrace, NoSinkIsSafe) {
  Net n;
  n.a->SetTrace(nullptr);  // 显式清空
  n.b->OnInboundDeliver([](const Message&) {});
  n.Open();
  EXPECT_TRUE(static_cast<bool>(n.a->Fire(Pay({9}), T(MessageKind::kOneway))));
  EXPECT_TRUE(n.cap->Records().empty());  // sink 已摘,零捕获
  n.Close();
}

// 解码失败:独立引擎 + 必败 codec + InjectBytes。
TEST(InteractionTrace, DecodeFailEmitsWarn) {
  class FailCodec : public ICodec {
   public:
    Result<std::vector<uint8_t>> Encode(const Message&) override {
      return Result<std::vector<uint8_t>>::Success({});
    }
    Result<std::vector<Message>> Decode(const uint8_t*, std::size_t) override {
      return Result<std::vector<Message>>::Fail("frame: bad");
    }
  };
  auto t = std::make_shared<FakeTransport>();
  auto cap = std::make_shared<CapturingTraceSink>();
  auto e = std::make_shared<InteractionEngine>(t, std::make_unique<FailCodec>(),
      std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
  e->SetTrace(cap);
  (void)e->Open();
  t->InjectBytes({0xFF, 0xFF});
  EXPECT_EQ(Count(*cap, "decode", "decode-fail"), 1u);
  e->Close();
}
```

注册到 `CMakeLists.txt`(`trace_sink_test.cpp` 行之后):
```
    tests/comm/interaction_trace_test.cpp
```
Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `InteractionEngine` 无 `SetTrace` 成员。

- [ ] **Step 3: 头文件加 `trace_` + `SetTrace` + `Trace()`**

`include/transport/comm/InteractionEngine.hpp`:
1. 顶部 include 区(`#include "transport/comm/InteractionPolicy.hpp"` 之后)加:
```cpp
#include "transport/ITraceSink.hpp"
```
2. public 区(`OnError(...)` 之后)加:
```cpp
  // 须在 Open() 前调用;Open 后埋点只读 trace_,设置期单线程,无竞争。
  void SetTrace(std::shared_ptr<ITraceSink> t) { trace_ = std::move(t); }
```
3. private 区(`FirePeriodic` 声明之后)加:
```cpp
  void Trace(const TraceEvent& ev) const { if (trace_) trace_->OnTrace(ev); }
```
4. private 成员区(`on_error_` 之后)加:
```cpp
  std::shared_ptr<ITraceSink> trace_;
```

> **注意:** 多数埋点需在调用 `Trace(...)` 前用 `if (trace_)` 守卫,以免无 sink 时仍构造事件/格式化 endpoint。`Trace()` 本身的 `if (trace_)` 是二次保险;**有 endpoint 格式化或非平凡构造的站点必须外层再套 `if (trace_)`**。

- [ ] **Step 4: `.cpp` 加 `EndpointStr` 辅助 + 各埋点**

`src/comm/InteractionEngine.cpp`:

(a) 匿名命名空间内(`Status Ok()` 之后)加格式化辅助:
```cpp
std::string EndpointStr(const Endpoint& e) {
  switch (e.kind) {
    case Endpoint::Kind::kNet:   return "net:" + e.host + ":" + std::to_string(e.port);
    case Endpoint::Kind::kTopic: return "topic:" + e.topic;
    default:                     return "default";
  }
}
```

(b) **Open 成功**:`return Ok();`(函数末)前加:
```cpp
  Trace({TraceLevel::kInfo, "open", "", "", "", "", kNoTag, kNoNum, -1});
```

(c) **OnBytes `!r` 分支**:在该分支 `s->executor_->Post(...)` 之后加(io 线程直接 trace):
```cpp
      s->Trace({TraceLevel::kError, "error", "", "", "", e, kNoTag, kNoNum, -1});
```

(d) **OnBytes `!msgs` 分支**:在该分支 `s->executor_->Post(...)` 之后加:
```cpp
      s->Trace({TraceLevel::kWarn, "decode", "decode-fail", "", "", e, kNoTag, kNoNum, -1});
```

(e) **recv**:`auto msgs = s->codec_->Decode(...)` 之前加:
```cpp
    if (s->trace_) s->Trace({TraceLevel::kTrace, "recv", "", "", from, "", kNoTag,
                             static_cast<long>(r.value.size()), -1});
```

(f) **OnConnect**:把 `transport_->OnConnect([] {});` 替换为:
```cpp
  transport_->OnConnect([wself] {
    if (auto s = wself.lock())
      s->Trace({TraceLevel::kInfo, "conn", "connect", "", "", "", kNoTag, kNoNum, -1});
  });
```

(g) **OnDisconnect**:在 `OnDisconnect` lambda 体首行(`if (auto s = wself.lock())` 之前)插入直接 trace —— 改为:
```cpp
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock()) {
      s->Trace({TraceLevel::kInfo, "conn", "disconnect", "", "", reason, kNoTag, kNoNum, -1});
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
    }
  });
```

(h) **Close**:在 `for (auto& kv : taken)` 循环**之前**加:
```cpp
  Trace({TraceLevel::kInfo, "close", "", "", "", "", kNoTag, static_cast<long>(taken.size()), -1});
```

(i) **Fire**:`SetTag` 后、`return SendMessage(...)` 前改为:
```cpp
Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  if (trace_) { std::string ep = EndpointStr(to);
    Trace({TraceLevel::kDebug, "send", "", "", ep, "", tag, static_cast<long>(out.payload.size()), -1}); }
  return SendMessage(out, to);
}
```

(j) **RequestAwait**:在锁块**之后**、`Status st = SendMessage(out, to);` **之前**加(此时 `key` 已得;`spec` 已被移入 Pending,故用预存的 tag/size):
```cpp
  if (trace_) { std::string ep = EndpointStr(to);
    Trace({TraceLevel::kDebug, "request", "", key, ep, "", req_tag, req_size, -1}); }
```
并在函数靠前(`policy_->SetTag(out, spec.request_tag);` 之后)预存:
```cpp
  const FrameTag req_tag = spec.request_tag;
  const long req_size = static_cast<long>(out.payload.size());
```

(k) **SendReply**:`Endpoint to = policy_->ReplyTo(request);` 之后、`return SendMessage(m, to);` 之前加:
```cpp
  if (trace_) { std::string ep = EndpointStr(to); Key rk = policy_->KeyOf(request);
    Trace({TraceLevel::kDebug, "reply", "", rk, ep, "", tag, static_cast<long>(m.payload.size()), -1}); }
```

(l) **StartPeriodic**:`(void)Fire(out, tag, to);  // 立即一帧` **之前**加:
```cpp
  Trace({TraceLevel::kTrace, "periodic", "start", "", "", "", tag, kNoNum, -1});
```

(m) **FirePeriodic**:`(void)Fire(out, tag, to);`(`if (!alive...) return;` 之后)**之前**加:
```cpp
  Trace({TraceLevel::kTrace, "periodic", "fire", "", "", "", tag, kNoNum, -1});
```

(n) **StopPeriodic**:把取 timer 的块改为同时取 tag 并 trace。将
```cpp
    if (it != periodics_.end()) { t = it->second.timer; periodics_.erase(it); }
```
改为:
```cpp
    if (it != periodics_.end()) { t = it->second.timer;
      Trace({TraceLevel::kTrace, "periodic", "stop", "", "", "", it->second.tag, kNoNum, -1});
      periodics_.erase(it); }
```

(o) **Dispatch**:把 `if (matched) { ... }` 块替换为(在锁外 trace 命中):
```cpp
  if (matched) {
    if (auto_ack) {
      (void)SendReply(ack_req, ack_tag, {});  // 锁外
      Trace({TraceLevel::kTrace, "dispatch", "auto-ack", key, "", "", ack_tag, kNoNum, -1});
    }
    if (term_cb) {
      Trace({TraceLevel::kDebug, "dispatch", "match-terminal", key, "", "", tag, kNoNum, -1});
      term_cb(Result<Message>::Success(std::move(msg)));
    } else if (inter_cb) {
      Trace({TraceLevel::kDebug, "dispatch", "match-intermediate", key, "", "", tag, kNoNum, -1});
      inter_cb(msg);
    }
    return;
  }
  switch (policy_->RouteUnmatched(msg)) {
    case InteractionPolicy::Route::kInboundRequest:
      Trace({TraceLevel::kTrace, "unmatched", "request", key, "", "", tag, kNoNum, -1});
      if (on_request_) on_request_(msg); break;
    case InteractionPolicy::Route::kDeliver:
      Trace({TraceLevel::kTrace, "unmatched", "deliver", key, "", "", tag, kNoNum, -1});
      if (on_deliver_) on_deliver_(msg); break;
    case InteractionPolicy::Route::kDrop:
      Trace({TraceLevel::kTrace, "unmatched", "drop", key, "", "", tag, kNoNum, -1});
      break;
  }
```

(p) **OnTimeout**:在锁内捕获 `retries` 到局部,锁外 trace。将函数尾部
```cpp
  if (resend) (void)SendMessage(rs, rto);
  if (fail_cb) fail_cb(Result<Message>::Fail("timeout: request timed out"));
```
改为:
```cpp
  if (resend) {
    (void)SendMessage(rs, rto);
    Trace({TraceLevel::kDebug, "retransmit", "", key, "", "", kNoTag, kNoNum, retries_now});
  }
  if (fail_cb) {
    Trace({TraceLevel::kWarn, "timeout", "", key, "", "", kNoTag, kNoNum, -1});
    fail_cb(Result<Message>::Fail("timeout: request timed out"));
  }
```
并在锁内 `resend = true; rs = p.out; rto = p.to;` 处补:
```cpp
      resend = true; rs = p.out; rto = p.to; retries_now = static_cast<int>(p.retries);
```
在函数顶部局部声明区(`bool resend = false; ...`)加:
```cpp
  int retries_now = -1;
```

- [ ] **Step 5: 跑测试,确认通过**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "InteractionTrace" --output-on-failure`
Expected: 8 个 InteractionTrace 用例全 PASS;零编译告警。

- [ ] **Step 6: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(89 + 8 = 97)。
```bash
git add include/transport/comm/InteractionEngine.hpp src/comm/InteractionEngine.cpp \
        tests/comm/fake_transport.hpp tests/comm/interaction_trace_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: InteractionEngine 埋 trace(收发/分发/超时/重发/periodic/关闭),SetTrace 注入"
```

---

### Task 3: 节点透传 `SetTrace`

**Files:**
- Modify: `include/transport/comm/DdsNode.hpp`
- Modify: `include/transport/comm/ProtocolNode.hpp`
- Modify: `tests/comm/protocol_node_test.cpp`(加一个透传回归用例)

**Interfaces:**
- Consumes:`InteractionEngine::SetTrace`(T2)、`transport::ITraceSink`/`CapturingTraceSink`(T1)。
- Produces:`void DdsNode::SetTrace(std::shared_ptr<ITraceSink>)`、`void ProtocolNode::SetTrace(std::shared_ptr<ITraceSink>)`。

- [ ] **Step 1: 写失败测试**(加到 `tests/comm/protocol_node_test.cpp` 末尾,`}` 命名空间前)

文件顶部 include 区补:
```cpp
#include "transport/ITraceSink.hpp"
```
末尾加用例(复用本文件既有的 `Pair` 夹具 + `P(...)` 助手):
```cpp
TEST(ProtocolNode, SetTraceForwardsToEngine) {
  Pair p;
  auto cap = std::make_shared<transport::CapturingTraceSink>();
  p.a->SetTrace(cap);               // 节点透传 → 引擎 SetTrace
  p.Open();
  (void)p.a->SendNoResponse(/*cmd=*/0x10, P({1}));
  bool sawSend = false;
  for (const auto& r : cap->Records()) if (r.category == "send") sawSend = true;
  EXPECT_TRUE(sawSend);              // 引擎在发起端发了 trace,证明透传生效
  p.Close();
}
```

Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `ProtocolNode` 无 `SetTrace`。

- [ ] **Step 2: `DdsNode.hpp` 透传**

`include/transport/comm/DdsNode.hpp`:include 区加 `#include "transport/ITraceSink.hpp"`;public 区(节点 `Open()` 声明附近)加:
```cpp
  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }
```

- [ ] **Step 3: `ProtocolNode.hpp` 透传**

`include/transport/comm/ProtocolNode.hpp`:include 区加 `#include "transport/ITraceSink.hpp"`;public 区加:
```cpp
  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }
```

- [ ] **Step 4: 跑测试,确认通过**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "ProtocolNode.SetTraceForwardsToEngine" --output-on-failure`
Expected: PASS。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(98)。
```bash
git add include/transport/comm/DdsNode.hpp include/transport/comm/ProtocolNode.hpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DdsNode/ProtocolNode 透传 SetTrace 到引擎"
```

---

## 实现后(计划外,单独一轮)

- 同步 SRS(加 trace FR)、SDD(§7 加 trace 小节 + 事件目录)、README(用法一行)、CHANGELOG(`[Unreleased]` 加条目)。
- 终态全分支 review(subagent-driven 的最后一步)→ finishing-a-development-branch。

## Self-Review 记录

- **Spec 覆盖:** §3 接口→T1;§4 两 sink→T1;§5 事件目录→T2(逐条埋点表);§6 注入/透传→T2(SetTrace)+T3(节点);§7 预留→spec 已述、本版不实现(T 无任务,符合);§8 文件/测试→T1/T2/T3 测试齐。
- **精化披露:** decode-fail 与 error 的分工(§Task2 表下注)相对 spec §5 更精确,已显式标注,留 review 裁决。
- **类型一致:** `SetTrace(std::shared_ptr<ITraceSink>)` 三处签名一致;`TraceEvent` 字段顺序 = T1 定义 = 各埋点 positional init 顺序(level,category,message,key,endpoint,error,tag,size,attempt)。
- **占位扫描:** 无占位。T3 Step1 测试复用本文件既有 `Pair` 夹具与 `P(...)` 助手(均为该文件已定义符号)。
