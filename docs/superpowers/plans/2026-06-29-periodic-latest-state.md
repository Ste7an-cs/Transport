# 周期发送取最新状态 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让周期发送每帧在发送前取最新状态——消息工厂(pull)+ `UpdatePeriodic`(push),`ProtocolNode`/`DdsNode` 同享。

**Architecture:** 引擎 `Periodic` 由固定 `Message` 改为 `std::function<Message()>` 工厂,`FirePeriodic` 每拍锁外调 `make()`;加 `UpdatePeriodic`;固定版 `StartPeriodic(Message,…)` 包装为工厂,行为不变。`ProtocolNode` 加 `StartRepeating(state_fn)` + `UpdateRepeating`;`DdsNode` 新增周期发布(固定/工厂/更新)。

**Tech Stack:** C++17,不抛异常,GoogleTest,CMake。配套 spec:`docs/superpowers/specs/2026-06-29-periodic-latest-state-design.md`。

## Global Constraints

- C++17,**不抛异常**;`Result`/`Status`。
- 并发纪律不破:`make()`/`state_fn` 在 executor 线程、每拍发送前、**锁外**调用;`UpdatePeriodic` 锁内换 `make`;`Close` 仍取消全部 periodic 定时器。
- **向后兼容**:固定版 `StartPeriodic(Message,…)` / `StartRepeating(cmd, payload,…)` 行为**逐字不变**(现有 periodic/心跳测试即回归证明);新增均为重载/新方法。
- `make()`/`state_fn` 契约:线程安全、非阻塞、快、不抛。
- 现有 111 测试不回归。提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**无 Co-Authored-By**。不提交 `build/`。
- 文档/demo 同步留到实现后。

---

## File Structure

| 文件 | 责任 | 任务 |
|---|---|---|
| `include/transport/comm/InteractionEngine.hpp`(改) | `Periodic.make`;工厂版+固定版 `StartPeriodic`;`UpdatePeriodic` 声明 | T1 |
| `src/comm/InteractionEngine.cpp`(改) | 两版 `StartPeriodic`、`UpdatePeriodic`、`FirePeriodic`(make 锁外调) | T1 |
| `tests/comm/interaction_engine_test.cpp`(改) | 工厂每拍取新 / Update 改后续 / 未知 handle | T1 |
| `include/transport/comm/ProtocolNode.hpp`(改) | `StartRepeating(state_fn)` + `UpdateRepeating` | T2 |
| `src/comm/ProtocolNode.cpp`(改) | 转发引擎工厂/Update | T2 |
| `tests/comm/protocol_node_test.cpp`(改) | pull / push 用例 | T2 |
| `include/transport/comm/DdsNode.hpp`(改) | `StartPublishing`(固定/工厂)、`UpdatePublishing`、`StopPublishing` | T3 |
| `src/comm/DdsNode.cpp`(改) | 转发引擎 | T3 |
| `tests/comm/dds_node_test.cpp`(改) | 发布 pull / push 用例 | T3 |

---

### Task 1: 引擎 —— 消息工厂 periodic + `UpdatePeriodic`

**Files:**
- Modify: `include/transport/comm/InteractionEngine.hpp`
- Modify: `src/comm/InteractionEngine.cpp`
- Modify: `tests/comm/interaction_engine_test.cpp`

**Interfaces:**
- Produces:
  - `uint32_t StartPeriodic(std::function<Message()> make, FrameTag tag, uint32_t interval_ms, const Endpoint& to = Endpoint::Default())`
  - `uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms, const Endpoint& to = Endpoint::Default())`(兼容)
  - `bool UpdatePeriodic(uint32_t handle, Message out)`

- [ ] **Step 1: 写失败测试**(加到 `tests/comm/interaction_engine_test.cpp` 末尾,命名空间 `}` 前;复用本文件已有的 `Net`/`T`/`Pay`/`MessageKind`)

```cpp
TEST(InteractionEngine, PeriodicFactoryFiresLatestEachTick) {
  Net n; std::vector<std::vector<uint8_t>> got;
  n.b->OnInboundDeliver([&](const Message& m) { got.push_back(m.payload); });
  n.Open();
  int ctr = 0;
  uint32_t h = n.a->StartPeriodic(
      [&ctr] { Message m; m.payload = {static_cast<uint8_t>(ctr++)}; return m; },
      T(MessageKind::kNotify), /*interval_ms=*/50);
  n.exa->FireAll();   // 第 2 拍
  n.exa->FireAll();   // 第 3 拍
  n.a->StopPeriodic(h);
  ASSERT_GE(got.size(), 3u);
  EXPECT_EQ(got[0], (std::vector<uint8_t>{0}));   // 立即一帧 = 最新
  EXPECT_EQ(got[1], (std::vector<uint8_t>{1}));   // 每拍重新 make()
  EXPECT_EQ(got[2], (std::vector<uint8_t>{2}));
  n.Close();
}

TEST(InteractionEngine, UpdatePeriodicChangesSubsequentFrames) {
  Net n; std::vector<std::vector<uint8_t>> got;
  n.b->OnInboundDeliver([&](const Message& m) { got.push_back(m.payload); });
  n.Open();
  uint32_t h = n.a->StartPeriodic(Pay({1}), T(MessageKind::kNotify), /*interval_ms=*/50);
  ASSERT_EQ(got.size(), 1u); EXPECT_EQ(got[0], (std::vector<uint8_t>{1}));  // 固定版立即一帧
  EXPECT_TRUE(n.a->UpdatePeriodic(h, Pay({2})));
  n.exa->FireAll();
  ASSERT_EQ(got.size(), 2u); EXPECT_EQ(got[1], (std::vector<uint8_t>{2}));   // 后续帧用新值
  EXPECT_FALSE(n.a->UpdatePeriodic(9999, Pay({3})));                          // 未知 handle → false
  n.a->StopPeriodic(h); n.Close();
}
```

Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `StartPeriodic` 无工厂重载 / 无 `UpdatePeriodic`。

- [ ] **Step 2: 改 `InteractionEngine.hpp`**

(a) `Periodic` 结构(把 `Message out` 换成工厂):
```cpp
  struct Periodic { std::function<Message()> make; FrameTag tag; Endpoint to; uint32_t interval_ms; IExecutor::TimerId timer = 0; };
```
(b) public 区把现有 `StartPeriodic` 声明替换为两个重载 + `UpdatePeriodic`:
```cpp
  uint32_t StartPeriodic(std::function<Message()> make, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  bool     UpdatePeriodic(uint32_t handle, Message out);
  void     StopPeriodic(uint32_t handle);
```
(`<functional>` 已包含——`on_request_` 等已用 `std::function`。)

- [ ] **Step 3: 改 `src/comm/InteractionEngine.cpp`**

把现有 `StartPeriodic` 与 `FirePeriodic` 两个函数整体替换为下面四段(`StopPeriodic` 不动):
```cpp
uint32_t InteractionEngine::StartPeriodic(std::function<Message()> make, FrameTag tag,
                                          uint32_t interval_ms, const Endpoint& to) {
  if (interval_ms == 0) return 0;
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = periodic_next_++;
    periodics_[handle] = Periodic{make, tag, to, interval_ms, 0};
  }
  Trace({TraceLevel::kTrace, "periodic", "start", "", "", "", tag, kNoNum, -1});
  Message out = make();        // 锁外:立即一帧 = 最新
  (void)Fire(out, tag, to);
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
  return handle;
}

uint32_t InteractionEngine::StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                                          const Endpoint& to) {
  return StartPeriodic(std::function<Message()>([m = std::move(out)]() { return m; }),
                       tag, interval_ms, to);
}

bool InteractionEngine::UpdatePeriodic(uint32_t handle, Message out) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it == periodics_.end()) return false;
  it->second.make = [m = std::move(out)]() { return m; };
  return true;
}

void InteractionEngine::FirePeriodic(uint32_t handle) {
  std::function<Message()> make; FrameTag tag = 0; Endpoint to; uint32_t interval = 0; bool alive = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { make = it->second.make; tag = it->second.tag; to = it->second.to; interval = it->second.interval_ms; alive = true; }
  }
  if (!alive || !open_.load()) return;
  Trace({TraceLevel::kTrace, "periodic", "fire", "", "", "", tag, kNoNum, -1});
  Message out = make();        // 锁外:每拍取最新
  (void)Fire(out, tag, to);
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
}
```

- [ ] **Step 4: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "InteractionEngine|InteractionTrace|ProtocolNode|DdsNode" --output-on-failure 2>&1 | tail -5`
Expected: 新 2 用例过;现有 periodic/心跳/trace periodic 全过(固定版行为不变)。零告警。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(111 + 2 = 113)。
```bash
git add include/transport/comm/InteractionEngine.hpp src/comm/InteractionEngine.cpp tests/comm/interaction_engine_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: 引擎 periodic 改消息工厂(每拍取最新)+ UpdatePeriodic(推送更新);固定版包装、行为不变"
```

---

### Task 2: `ProtocolNode` —— `StartRepeating(state_fn)` + `UpdateRepeating`

**Files:**
- Modify: `include/transport/comm/ProtocolNode.hpp`
- Modify: `src/comm/ProtocolNode.cpp`
- Modify: `tests/comm/protocol_node_test.cpp`

**Interfaces:**
- Consumes:`engine_->StartPeriodic(std::function<Message()>, FrameTag, interval, to)`、`engine_->UpdatePeriodic(handle, Message)`(T1)。
- Produces:
  - `uint32_t StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn, uint32_t interval_ms, const Endpoint& to = Endpoint::Default())`
  - `bool UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload)`

- [ ] **Step 1: 写失败测试**(加到 `tests/comm/protocol_node_test.cpp` 末尾,命名空间 `}` 前;复用 `Pair`/`P`/`TestNode.on_cmd`)

```cpp
TEST(ProtocolNode, RepeatingPullSendsLatestState) {
  Pair p; std::vector<std::vector<uint8_t>> got;
  p.b->on_cmd = [&](const Message& m, Responder) { got.push_back(m.payload); };
  p.Open();
  int ctr = 0;
  uint32_t h = p.a->StartRepeating(/*cmd=*/0x14,
      [&ctr] { return P({static_cast<uint8_t>(ctr++)}); }, /*interval_ms=*/50);
  p.exa->FireAll();   // 第 2 拍
  p.exa->FireAll();   // 第 3 拍
  p.a->StopRepeating(h);
  ASSERT_GE(got.size(), 3u);
  EXPECT_EQ(got[0], P({0}));
  EXPECT_EQ(got[1], P({1}));
  EXPECT_EQ(got[2], P({2}));
  p.Close();
}

TEST(ProtocolNode, UpdateRepeatingChangesState) {
  Pair p; std::vector<std::vector<uint8_t>> got;
  p.b->on_cmd = [&](const Message& m, Responder) { got.push_back(m.payload); };
  p.Open();
  uint32_t h = p.a->StartRepeating(/*cmd=*/0x14, P({1}), /*interval_ms=*/50);
  ASSERT_EQ(got.size(), 1u); EXPECT_EQ(got[0], P({1}));
  EXPECT_TRUE(p.a->UpdateRepeating(h, /*cmd=*/0x14, P({2})));
  p.exa->FireAll();
  ASSERT_EQ(got.size(), 2u); EXPECT_EQ(got[1], P({2}));
  p.a->StopRepeating(h); p.Close();
}
```

Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `StartRepeating` 无 `state_fn` 重载 / 无 `UpdateRepeating`。

- [ ] **Step 2: 改 `include/transport/comm/ProtocolNode.hpp`**

在现有 `StartRepeating(uint16_t, std::vector<uint8_t>, uint32_t, const Endpoint&)` 声明之后加:
```cpp
  uint32_t StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                          uint32_t interval_ms, const Endpoint& to = Endpoint::Default());
  bool     UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload);
```
(`<functional>` 已包含——`ReplyFn` 已用 `std::function`。)

- [ ] **Step 3: 改 `src/comm/ProtocolNode.cpp`**

在现有 `StartRepeating`(固定版)之后加两个定义:
```cpp
uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                                      uint32_t interval_ms, const Endpoint& to) {
  return engine_->StartPeriodic(
      [cmd, fn = std::move(state_fn)]() { return Cmd(cmd, fn()); },
      Tag(FrameType::kState), interval_ms, to);
}

bool ProtocolNode::UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload) {
  return engine_->UpdatePeriodic(handle, Cmd(cmd, std::move(payload)));
}
```
(`Cmd`/`Tag` 是本文件匿名命名空间内的自由函数;工厂 lambda 在 executor 线程被调时调用 `Cmd`/`fn`,均无状态,安全。)

- [ ] **Step 4: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "ProtocolNode" --output-on-failure 2>&1 | tail -5`
Expected: 新 2 用例过;现有 `ProtocolNode.*`(含固定 repeating/心跳)全过。零告警。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(113 + 2 = 115)。
```bash
git add include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolNode StartRepeating(state_fn) 每拍拉最新 + UpdateRepeating 推送更新"
```

---

### Task 3: `DdsNode` —— 周期发布(新增)

**Files:**
- Modify: `include/transport/comm/DdsNode.hpp`
- Modify: `src/comm/DdsNode.cpp`
- Modify: `tests/comm/dds_node_test.cpp`

**Interfaces:**
- Consumes:`engine_->StartPeriodic`(两版)、`engine_->UpdatePeriodic`、`engine_->StopPeriodic`(T1)。
- Produces:
  - `uint32_t StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to)`
  - `uint32_t StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to)`
  - `bool UpdatePublishing(uint32_t handle, Message msg)`
  - `void StopPublishing(uint32_t handle)`

- [ ] **Step 1: 写失败测试**(加到 `tests/comm/dds_node_test.cpp` 末尾,命名空间 `}` 前;复用 `Net`/`Msg`/`TestNode`)

```cpp
TEST(DdsNode, PublishingPullSendsLatestSample) {
  Net net; InlineExecutor* exa = nullptr;
  auto a = net.Make("A_in", &exa);   // 发布者
  auto b = net.Make("B_in", nullptr); // 订阅者
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("T")));
  int ctr = 0;
  uint32_t h = a->StartPublishing(
      [&ctr] { return Msg({static_cast<uint8_t>(ctr++)}); }, /*interval_ms=*/50, Endpoint::Topic("T"));
  exa->FireAll();   // 第 2 拍
  a->StopPublishing(h);
  ASSERT_GE(b->messages.size(), 2u);
  EXPECT_EQ(b->messages[0].payload, (std::vector<uint8_t>{0}));   // 立即一帧
  EXPECT_EQ(b->messages[1].payload, (std::vector<uint8_t>{1}));   // 每拍取最新
  a->Close(); b->Close();
}

TEST(DdsNode, UpdatePublishingChangesSample) {
  Net net; InlineExecutor* exa = nullptr;
  auto a = net.Make("A_in", &exa);
  auto b = net.Make("B_in", nullptr);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("T")));
  uint32_t h = a->StartPublishing(Msg({1}), /*interval_ms=*/50, Endpoint::Topic("T"));
  ASSERT_EQ(b->messages.size(), 1u); EXPECT_EQ(b->messages[0].payload, (std::vector<uint8_t>{1}));
  EXPECT_TRUE(a->UpdatePublishing(h, Msg({2})));
  exa->FireAll();
  ASSERT_EQ(b->messages.size(), 2u); EXPECT_EQ(b->messages[1].payload, (std::vector<uint8_t>{2}));
  EXPECT_FALSE(a->UpdatePublishing(9999, Msg({3})));
  a->StopPublishing(h); a->Close(); b->Close();
}
```

Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `DdsNode` 无 `StartPublishing`。

- [ ] **Step 2: 改 `include/transport/comm/DdsNode.hpp`**

(a) 顶部 include 区补(签名用 `uint32_t`):
```cpp
#include <cstdint>
```
(b) public 区(`Request` 三重载之后)加:
```cpp
  uint32_t StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to);
  uint32_t StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to);
  bool     UpdatePublishing(uint32_t handle, Message msg);
  void     StopPublishing(uint32_t handle);
```
(`<functional>` 已包含——`ReplyFn`/`FeedbackFn` 已用 `std::function`。)

- [ ] **Step 3: 改 `src/comm/DdsNode.cpp`**

在 `Send` 定义之后加四个定义(`Tag` 是本文件匿名命名空间的自由函数):
```cpp
uint32_t DdsNode::StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to) {
  return engine_->StartPeriodic(std::move(msg), Tag(MessageKind::kNotify), interval_ms, to);
}
uint32_t DdsNode::StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to) {
  return engine_->StartPeriodic(std::move(sample_fn), Tag(MessageKind::kNotify), interval_ms, to);
}
bool DdsNode::UpdatePublishing(uint32_t handle, Message msg) {
  return engine_->UpdatePeriodic(handle, std::move(msg));
}
void DdsNode::StopPublishing(uint32_t handle) { engine_->StopPeriodic(handle); }
```

- [ ] **Step 4: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "DdsNode" --output-on-failure 2>&1 | tail -5`
Expected: 新 2 用例过;现有 `DdsNode.*` 全过。零告警。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(115 + 2 = 117)。
```bash
git add include/transport/comm/DdsNode.hpp src/comm/DdsNode.cpp tests/comm/dds_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DdsNode 周期发布 StartPublishing(固定/工厂)+ UpdatePublishing/StopPublishing(每拍取最新样本)"
```

---

## 实现后(计划外,单独一轮)

- 同步 SRS(FR-7 repeating / 新 DDS 周期发布)、SDD(§7.2 引擎原语 periodic、§7.4/7.5 节点)、README、CHANGELOG;demo 可补「拉最新状态」示例。
- 终态全分支 review → finishing-a-development-branch。

## Self-Review 记录

- **Spec 覆盖:** §3 引擎工厂+Update → T1;§4 ProtocolNode pull+push → T2;§5 DdsNode 周期发布 → T3;§6 用法 = 三者组合(文档/demo 时演示)。
- **占位扫描:** 无。每步含完整代码与确切命令。
- **类型一致:** `StartPeriodic(std::function<Message()>,…)` / `UpdatePeriodic(uint32_t, Message)` 在 T1 声明=定义=T2/T3 调用一致;`StartRepeating(…, std::function<std::vector<uint8_t>()>,…)`、`UpdateRepeating(uint32_t,uint16_t,vector)`、`StartPublishing`/`UpdatePublishing` 各自 hpp 声明=cpp 定义一致。重载消歧:`Message` vs `std::function<Message()>` 类型不同,无歧义。
- **回归保障:** 固定版 `StartPeriodic`/`StartRepeating` 经包装行为不变,现有 periodic/心跳测试即证明;新增均为重载/新方法,默认调用点零改动。
