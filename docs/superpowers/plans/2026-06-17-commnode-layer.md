# CommNode 层 v1(交互模式基类 + IExecutor 执行器缝)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现用户继承的交互模式基类 `CommNode`(单向 / 请求-应答 / 请求-结果反馈 / 服务端),其执行模型经 `IExecutor` 抽象,v1 内置 `ThreadExecutor`(worker 线程 + 有界队列 + 定时器),为未来自研协程 `CoroExecutor` 预留。

**Architecture:** io 线程内联 `codec.Decode` → `executor->Post` 投递到业务上下文(满则阻塞=背压);worker 串行跑 `Dispatch`(按 `kind` 路由:kReply/kFeedback 配挂起请求、kRequest→OnRequest、kOneway/kNotify→OnMessage);请求超时用 `executor->ScheduleAt`。`CommNode` 与执行器无关,逻辑可在 ThreadExecutor / InlineExecutor / 未来 CoroExecutor 间不改动地换。v1 只做点对点双向管道。

**Tech Stack:** C++17;GoogleTest 1.14(已 vendored);不抛异常(`Result<T>`/`Status`);标准库线程/条件变量(本层不依赖 asio)。

**配套 spec:** `docs/superpowers/specs/2026-06-17-commnode-layer-design.md`

---

## 文件结构

**新建:**
- `include/transport/comm/IExecutor.hpp` — 执行器接口(Post/ScheduleAt/Cancel/Start/Stop)。
- `include/transport/comm/ThreadExecutor.hpp` + `src/comm/ThreadExecutor.cpp` — v1 线程执行器。
- `include/transport/comm/CommNode.hpp` + `src/comm/CommNode.cpp` — 基类 + Responder + 分发 + 挂起表。
- `tests/comm/inline_executor.hpp` — 确定性测试执行器(同步 Post,手动驱动定时器)。
- `tests/comm/fake_transport.hpp` — 进程内双向回环 ITransport(测试件)。
- `tests/comm/thread_executor_test.cpp`、`tests/comm/comm_node_test.cpp`。

**修改:** `CMakeLists.txt`。

**不动:** 所有 Transport / ICodec / 底层。

---

## Task 1: `IExecutor` + `ThreadExecutor`

**Files:** Create `include/transport/comm/IExecutor.hpp`、`include/transport/comm/ThreadExecutor.hpp`、`src/comm/ThreadExecutor.cpp`;Test `tests/comm/thread_executor_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 写 `include/transport/comm/IExecutor.hpp`**
```cpp
#pragma once

// IExecutor.hpp — 执行器缝:决定"业务回调在哪/怎么跑"+ 定时。
// CommNode 只依赖此接口;v1=ThreadExecutor(线程),将来=CoroExecutor(协程),测试=InlineExecutor。

#include <chrono>
#include <cstdint>
#include <functional>

namespace transport {

class IExecutor {
 public:
  using Task    = std::function<void()>;
  using TimerId = uint64_t;  // 0 = 无效

  virtual ~IExecutor() = default;
  virtual void Start() = 0;
  virtual void Stop()  = 0;  // 停止并 drain/join,确保无任务在 CommNode 析构后跑

  // 投递任务到业务上下文【串行】执行;容量满时【阻塞调用方】(背压)。
  virtual void Post(Task task) = 0;

  // 在 deadline 触发一次性 task(请求超时);task 也在业务上下文跑。
  virtual TimerId ScheduleAt(std::chrono::steady_clock::time_point deadline, Task task) = 0;
  virtual void    Cancel(TimerId id) = 0;  // 取消未触发定时器(幂等)
};

}  // namespace transport
```

- [ ] **Step 2: 写失败测试** `tests/comm/thread_executor_test.cpp`:
```cpp
#include "transport/comm/ThreadExecutor.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using transport::ThreadExecutor;
using namespace std::chrono_literals;

TEST(ThreadExecutor, PostRunsTasksSeriallyInOrder) {
  ThreadExecutor ex(64);
  ex.Start();
  std::vector<int> seen;
  std::mutex m;
  for (int i = 0; i < 100; ++i)
    ex.Post([&, i] { std::lock_guard<std::mutex> lk(m); seen.push_back(i); });
  // 等排空:投递一个置位任务并轮询
  std::atomic<bool> done{false};
  ex.Post([&] { done = true; });
  for (int i = 0; i < 100 && !done; ++i) std::this_thread::sleep_for(5ms);
  ex.Stop();
  ASSERT_EQ(seen.size(), 100u);
  for (int i = 0; i < 100; ++i) EXPECT_EQ(seen[i], i);  // 串行有序
}

TEST(ThreadExecutor, ScheduleAtFires) {
  ThreadExecutor ex(64);
  ex.Start();
  std::atomic<bool> fired{false};
  ex.ScheduleAt(std::chrono::steady_clock::now() + 30ms, [&] { fired = true; });
  for (int i = 0; i < 100 && !fired; ++i) std::this_thread::sleep_for(5ms);
  EXPECT_TRUE(fired.load());
  ex.Stop();
}

TEST(ThreadExecutor, CancelPreventsFire) {
  ThreadExecutor ex(64);
  ex.Start();
  std::atomic<bool> fired{false};
  auto id = ex.ScheduleAt(std::chrono::steady_clock::now() + 50ms, [&] { fired = true; });
  ex.Cancel(id);
  std::this_thread::sleep_for(120ms);
  EXPECT_FALSE(fired.load());
  ex.Stop();
}

TEST(ThreadExecutor, PostBlocksWhenFull) {
  ThreadExecutor ex(/*capacity=*/1);
  ex.Start();
  std::promise<void> gate;             // 阻住 worker 的第一个任务
  auto gate_fut = gate.get_future();
  ex.Post([&] { gate_fut.wait(); });   // worker 卡在这
  ex.Post([] {});                      // 占满容量(队列 size=1)
  std::atomic<bool> third_posted{false};
  std::thread t([&] { ex.Post([] {}); third_posted = true; });  // 应阻塞
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(third_posted.load());   // 满 → 第三个 Post 仍阻塞
  gate.set_value();                    // 放行 → 排空 → 第三个 Post 解除
  t.join();
  EXPECT_TRUE(third_posted.load());
  ex.Stop();
}
```
需要 `#include <future>`/`#include <mutex>`(promise/mutex)。把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 3: 运行,确认失败** `cd /home/ubuntu/david/transport && cmake --build build -j$(nproc) 2>&1 | head -20`(build 不存在先 `cmake -S . -B build >/dev/null`)。Expected: 找不到 `ThreadExecutor.hpp`。

- [ ] **Step 4: 写 `include/transport/comm/ThreadExecutor.hpp`**
```cpp
#pragma once

// ThreadExecutor.hpp — v1 执行器:1 条 worker 线程 + 有界任务队列(满则阻塞=背压)
// + 最小堆定时器(worker cv.wait_until 兼顾取任务/触发超时)。

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "transport/comm/IExecutor.hpp"

namespace transport {

class ThreadExecutor : public IExecutor {
 public:
  explicit ThreadExecutor(std::size_t capacity = 1024);
  ~ThreadExecutor() override;

  void Start() override;
  void Stop()  override;
  void Post(Task task) override;
  TimerId ScheduleAt(std::chrono::steady_clock::time_point deadline, Task task) override;
  void Cancel(TimerId id) override;

 private:
  void Run();

  struct TimerEntry {
    std::chrono::steady_clock::time_point deadline;
    TimerId id;
  };
  struct LaterDeadline {  // 小顶堆:最早 deadline 在 top
    bool operator()(const TimerEntry& a, const TimerEntry& b) const {
      return a.deadline > b.deadline;
    }
  };

  std::size_t capacity_;
  std::mutex m_;
  std::condition_variable work_cv_;   // worker 等待:有任务/到点/stop
  std::condition_variable space_cv_;  // 投递方等待:队列非满
  std::deque<Task> tasks_;
  std::priority_queue<TimerEntry, std::vector<TimerEntry>, LaterDeadline> heap_;
  std::map<TimerId, Task> timers_;    // 活跃定时器(id→task);取消即从此删,堆项懒跳过
  TimerId next_id_ = 1;
  std::thread worker_;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace transport
```

- [ ] **Step 5: 写 `src/comm/ThreadExecutor.cpp`**
```cpp
#include "transport/comm/ThreadExecutor.hpp"

#include <utility>

// ThreadExecutor.cpp — 见 .hpp。任务/超时回调一律在锁外运行(单 worker → 串行)。

namespace transport {

ThreadExecutor::ThreadExecutor(std::size_t capacity) : capacity_(capacity) {}

ThreadExecutor::~ThreadExecutor() { Stop(); }

void ThreadExecutor::Start() {
  std::lock_guard<std::mutex> lk(m_);
  if (started_) return;
  started_ = true;
  worker_ = std::thread([this] { Run(); });
}

void ThreadExecutor::Stop() {
  {
    std::lock_guard<std::mutex> lk(m_);
    if (stopping_) return;
    stopping_ = true;
  }
  work_cv_.notify_all();
  space_cv_.notify_all();  // 解除可能阻塞的投递方
  if (worker_.joinable()) worker_.join();
}

void ThreadExecutor::Post(Task task) {
  std::unique_lock<std::mutex> lk(m_);
  space_cv_.wait(lk, [this] { return tasks_.size() < capacity_ || stopping_; });
  if (stopping_) return;  // 停止中:丢弃
  tasks_.push_back(std::move(task));
  work_cv_.notify_one();
}

IExecutor::TimerId ThreadExecutor::ScheduleAt(
    std::chrono::steady_clock::time_point deadline, Task task) {
  std::lock_guard<std::mutex> lk(m_);
  const TimerId id = next_id_++;
  timers_[id] = std::move(task);
  heap_.push(TimerEntry{deadline, id});
  work_cv_.notify_one();
  return id;
}

void ThreadExecutor::Cancel(TimerId id) {
  std::lock_guard<std::mutex> lk(m_);
  timers_.erase(id);  // 堆项在 pop 时按 timers_ 缺失而跳过
}

void ThreadExecutor::Run() {
  std::unique_lock<std::mutex> lk(m_);
  while (true) {
    const auto now = std::chrono::steady_clock::now();
    if (!heap_.empty() && heap_.top().deadline <= now) {
      const TimerEntry e = heap_.top();
      heap_.pop();
      auto it = timers_.find(e.id);
      if (it == timers_.end()) continue;  // 已取消
      Task t = std::move(it->second);
      timers_.erase(it);
      lk.unlock();
      t();
      lk.lock();
      continue;
    }
    if (!tasks_.empty()) {
      Task t = std::move(tasks_.front());
      tasks_.pop_front();
      space_cv_.notify_one();
      lk.unlock();
      t();
      lk.lock();
      continue;
    }
    if (stopping_) break;
    if (heap_.empty())
      work_cv_.wait(lk);
    else
      work_cv_.wait_until(lk, heap_.top().deadline);
  }
}

}  // namespace transport
```
把 `src/comm/ThreadExecutor.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 6: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R ThreadExecutor 2>&1 | grep -iE "passed|failed"`。Expected: 4 个用例通过。

- [ ] **Step 7: 提交**
```bash
git add include/transport/comm/IExecutor.hpp include/transport/comm/ThreadExecutor.hpp src/comm/ThreadExecutor.cpp tests/comm/thread_executor_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: IExecutor 执行器缝 + ThreadExecutor(worker+有界队列+定时器)"
```

---

## Task 2: `CommNode` + `Responder` + 测试件 + 交互模式测试

**Files:** Create `include/transport/comm/CommNode.hpp`、`src/comm/CommNode.cpp`、`tests/comm/inline_executor.hpp`、`tests/comm/fake_transport.hpp`、`tests/comm/comm_node_test.cpp`;Modify `CMakeLists.txt`。

- [ ] **Step 1: 写测试件 `tests/comm/inline_executor.hpp`**(确定性执行器:同步 Post、手动驱动定时器)
```cpp
#pragma once
#include <chrono>
#include <map>
#include "transport/comm/IExecutor.hpp"

namespace testutil {
class InlineExecutor : public transport::IExecutor {
 public:
  void Start() override {}
  void Stop() override {}
  void Post(Task task) override { task(); }  // 同步,调用线程立即执行
  TimerId ScheduleAt(std::chrono::steady_clock::time_point dl, Task task) override {
    const TimerId id = ++next_;
    timers_[id] = {dl, std::move(task)};
    return id;
  }
  void Cancel(TimerId id) override { timers_.erase(id); }

  // 测试驱动:触发所有(或到点)定时器
  void FireAll() {
    auto t = std::move(timers_);
    timers_.clear();
    for (auto& kv : t) kv.second.second();
  }

 private:
  TimerId next_ = 0;
  std::map<TimerId, std::pair<std::chrono::steady_clock::time_point, Task>> timers_;
};
}  // namespace testutil
```

- [ ] **Step 2: 写测试件 `tests/comm/fake_transport.hpp`**(进程内双向回环 ITransport)
```cpp
#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

namespace testutil {
// 两个 FakeTransport 用 Link 互联:一端 Send → 另一端 OnBytes(同步,投递方线程)。
class FakeTransport : public transport::ITransport,
                      public std::enable_shared_from_this<FakeTransport> {
 public:
  static void Link(const std::shared_ptr<FakeTransport>& a,
                   const std::shared_ptr<FakeTransport>& b) {
    a->peer_ = b; b->peer_ = a;
  }

  transport::Status Open() override {
    open_.store(true);
    if (connect_cb_) connect_cb_();
    return transport::Status::Success(std::monostate{});
  }
  void Close() override {
    if (!open_.exchange(false)) return;
    if (auto p = peer_.lock())
      if (p->open_.load() && p->disconnect_cb_) p->disconnect_cb_("conn: peer closed");
  }
  bool IsOpen() const override { return open_.load(); }

  transport::Status Send(const std::vector<uint8_t>& bytes) override {
    auto p = peer_.lock();
    if (!p || !p->open_.load()) return transport::Status::Fail("conn: peer not open");
    if (p->bytes_cb_)
      p->bytes_cb_(transport::Result<std::vector<uint8_t>>::Success(bytes), "fake");
    return transport::Status::Success(std::monostate{});
  }
  transport::Status Send(const std::vector<uint8_t>& bytes,
                         const transport::Endpoint&) override {
    return Send(bytes);
  }

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  std::weak_ptr<FakeTransport> peer_;
  std::atomic<bool> open_{false};
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};
}  // namespace testutil
```

- [ ] **Step 3: 写失败测试** `tests/comm/comm_node_test.cpp`:
```cpp
#include "transport/comm/CommNode.hpp"

#include "transport/codec/SystemCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <chrono>
#include <memory>

#include <gtest/gtest.h>

using transport::CommNode;
using transport::Message;
using transport::MessageKind;
using transport::Responder;
using transport::Result;
using transport::SystemCodec;
using testutil::FakeTransport;
using testutil::InlineExecutor;
using namespace std::chrono_literals;

namespace {
Message Msg(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

// 测试用子类:把收到的请求/消息记录或回显。
class EchoNode : public CommNode {
 public:
  using CommNode::CommNode;
  std::vector<uint8_t> last_msg;
  void OnMessage(const Message& m) override { last_msg = m.payload; }
  void OnRequest(const Message& req, Responder r) override {
    auto out = req.payload; out.push_back(0xFF);
    (void)r.Reply(Msg(out));  // 应答 = 请求 payload + 0xFF
  }
};

// 把两个 EchoNode 经一对 FakeTransport + InlineExecutor(确定性)接起来。
struct Pair {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<EchoNode> a, b;
  InlineExecutor* exa = nullptr; InlineExecutor* exb = nullptr;
  Pair() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    auto eb = std::make_unique<InlineExecutor>(); exb = eb.get();
    a = std::make_shared<EchoNode>(ta, std::make_unique<SystemCodec>(), std::move(ea));
    b = std::make_shared<EchoNode>(tb, std::make_unique<SystemCodec>(), std::move(eb));
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(CommNode, OnewaySend) {
  Pair p; p.Open();
  ASSERT_TRUE(static_cast<bool>(p.a->Send(Msg({1, 2, 3}))));
  EXPECT_EQ(p.b->last_msg, (std::vector<uint8_t>{1, 2, 3}));  // InlineExecutor 同步交付
  p.Close();
}

TEST(CommNode, RequestReplyCallback) {
  Pair p; p.Open();
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->Request(Msg({5}), [&](Result<Message> r) { got = std::move(r); }, 1000)));
  ASSERT_TRUE(static_cast<bool>(got));                 // InlineExecutor 全同步:已回
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xFF}));
  p.Close();
}

TEST(CommNode, RequestReplyFuture) {
  Pair p; p.Open();
  auto fut = p.a->Request(Msg({7}), 1000);
  auto r = fut.get();                                  // 同步执行器下即时就绪
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 0xFF}));
  p.Close();
}

TEST(CommNode, RequestTimeout) {
  // server 不应答:用一个不回 reply 的子类
  class Silent : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder) override {} };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto ea = std::make_unique<InlineExecutor>(); auto* pea = ea.get();
  auto a = std::make_shared<Silent>(ta, std::make_unique<SystemCodec>(), std::move(ea));
  auto b = std::make_shared<Silent>(tb, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  Result<Message> got = Result<Message>::Success(Message{});
  (void)a->Request(Msg({1}), [&](Result<Message> r) { got = std::move(r); }, 50);
  EXPECT_FALSE(static_cast<bool>(got));   // 还没超时
  pea->FireAll();                          // 驱动 a 的超时定时器
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  a->Close(); b->Close();
}

TEST(CommNode, FeedbackThenFinal) {
  class Worker : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder r) override {
      (void)r.Feedback(Msg({0x01}));
      (void)r.Feedback(Msg({0x02}));
      (void)r.Reply(Msg({0xEE}));
    } };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<Worker>(ta, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  auto b = std::make_shared<Worker>(tb, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  std::vector<std::vector<uint8_t>> feedbacks; Result<Message> fin = Result<Message>::Fail("none");
  (void)a->Request(Msg({9}),
                   [&](const Message& m) { feedbacks.push_back(m.payload); },
                   [&](Result<Message> r) { fin = std::move(r); }, 1000);
  ASSERT_EQ(feedbacks.size(), 2u);
  EXPECT_EQ(feedbacks[0], (std::vector<uint8_t>{0x01}));
  EXPECT_EQ(feedbacks[1], (std::vector<uint8_t>{0x02}));
  ASSERT_TRUE(static_cast<bool>(fin));
  EXPECT_EQ(fin.value.payload, (std::vector<uint8_t>{0xEE}));
  a->Close(); b->Close();
}

TEST(CommNode, DisconnectFinalizesPending) {
  // server 持有请求不回(长超时),再关对端 → 挂起以 conn: 终结。
  Result<Message> got = Result<Message>::Success(Message{});
  class Hold : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder) override {} };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<Hold>(ta, std::make_unique<SystemCodec>(),
                                  std::make_unique<InlineExecutor>());
  auto b = std::make_shared<Hold>(tb, std::make_unique<SystemCodec>(),
                                  std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  (void)a->Request(Msg({1}), [&](Result<Message> r) { got = std::move(r); }, 100000);
  EXPECT_TRUE(static_cast<bool>(got)) << "still pending, untouched";  // 初值 Success,未被调
  b->Close();   // 对端断 → a 收 OnDisconnect → 终结挂起(conn:)
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("conn:", 0), 0u);
  a->Close();
}

// 执行器可换性:同一交互逻辑换 ThreadExecutor 也通(真实线程)。
TEST(CommNode, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<EchoNode>(ta, std::make_unique<SystemCodec>(), nullptr);  // 默认 ThreadExecutor
  auto b = std::make_shared<EchoNode>(tb, std::make_unique<SystemCodec>(), nullptr);
  (void)b->Open(); (void)a->Open();
  auto r = a->Request(Msg({3}), 2000).get();   // future 等回(真实线程)
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xFF}));
  a->Close(); b->Close();
}
```
需要 `#include <future>`/`#include <mutex>`/`#include <vector>`。把 `tests/comm/comm_node_test.cpp` 加入 `CMakeLists.txt` 的 `transport_tests`,并确保测试 include 目录能找到 `tests/comm/`(同目录头 `fake_transport.hpp`/`inline_executor.hpp` 用引号包含即可,gtest 源文件目录会被搜索)。

- [ ] **Step 4: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `CommNode.hpp`。

- [ ] **Step 5: 写 `include/transport/comm/CommNode.hpp`**
```cpp
#pragma once

// CommNode.hpp — 用户继承的交互模式基类。持有 Transport + ICodec + IExecutor;
// io 线程 Decode → executor.Post → 业务上下文 Dispatch(按 kind);Request 用 executor.ScheduleAt 超时。
// 须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
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

namespace transport {

using ReplyFn    = std::function<void(Result<Message>)>;
using FeedbackFn = std::function<void(const Message&)>;

class CommNode;

class Responder {  // 服务端应答句柄:绑定该请求 correlation_id + 回发目的地
 public:
  Status Feedback(Message msg);  // 发 kFeedback(可多次)
  Status Reply(Message msg);     // 发 kReply(终结,一次)

 private:
  friend class CommNode;
  Responder(std::weak_ptr<CommNode> node, std::string corr, Endpoint to)
      : node_(std::move(node)), corr_(std::move(corr)), to_(std::move(to)) {}
  std::weak_ptr<CommNode> node_;
  std::string corr_;
  Endpoint to_;
};

class CommNode : public std::enable_shared_from_this<CommNode> {
 public:
  CommNode(std::shared_ptr<ITransport> transport,
           std::unique_ptr<ICodec> codec,
           std::unique_ptr<IExecutor> executor = nullptr,
           std::size_t queue_capacity = 1024);
  virtual ~CommNode();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  Status Send(Message msg, const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms);
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms);
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms);

 protected:
  virtual void OnMessage(const Message& msg) {}
  virtual void OnRequest(const Message& req, Responder responder) {}
  virtual void OnConnected() {}
  virtual void OnDisconnected(const std::string& reason) {}
  virtual void OnError(const std::string& error) {}

 private:
  friend class Responder;
  Status SendKind(Message msg, MessageKind kind, const std::string& corr, const Endpoint& to);
  Status RequestImpl(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms);
  void Dispatch(Message msg);
  void HandleDisconnect(const std::string& reason);
  void FireTimeout(const std::string& corr);
  std::string NextCorrId();

  struct Pending { FeedbackFn on_feedback; ReplyFn on_final; IExecutor::TimerId timer; };

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<IExecutor> executor_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;                            // 保护 pending_ + seq_
  std::map<std::string, Pending> pending_;
  uint64_t seq_ = 0;
  std::string id_prefix_;
};

}  // namespace transport
```

- [ ] **Step 6: 写 `src/comm/CommNode.cpp`**
```cpp
#include "transport/comm/CommNode.hpp"

#include <chrono>
#include <random>
#include <utility>
#include <variant>

#include "transport/comm/ThreadExecutor.hpp"

// CommNode.cpp — 见 .hpp。posted 任务/transport 回调捕获 weak_ptr 防悬空;
// pending_ 由 mu_ 保护;超时与 Dispatch 同在业务上下文 → reply/超时恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
int64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

Status Responder::Feedback(Message msg) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendKind(std::move(msg), MessageKind::kFeedback, corr_, to_);
}
Status Responder::Reply(Message msg) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendKind(std::move(msg), MessageKind::kReply, corr_, to_);
}

CommNode::CommNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                   std::unique_ptr<IExecutor> executor, std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))) {
  std::random_device rd;
  id_prefix_ = std::to_string(rd()) + "-";
}

CommNode::~CommNode() { Close(); }

std::string CommNode::NextCorrId() {
  std::lock_guard<std::mutex> lk(mu_);
  return id_prefix_ + std::to_string(++seq_);
}

Status CommNode::Open() {
  executor_->Start();
  std::weak_ptr<CommNode> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string& from) {
    auto s = wself.lock();
    if (!s) return;
    if (!r) {
      std::string e = r.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    for (auto& m : msgs.value) {
      m.source = from;
      m.timestamp = NowMicros();
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
    }
  });
  transport_->OnConnect([wself] {
    if (auto s = wself.lock())
      s->executor_->Post([wself] { if (auto s2 = wself.lock()) s2->OnConnected(); });
  });
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  auto st = transport_->Open();
  if (!st) { executor_->Stop(); return st; }
  open_.store(true);
  return Ok();
}

void CommNode::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<std::string, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.on_final) kv.second.on_final(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();
  transport_->Close();
}

Status CommNode::SendKind(Message msg, MessageKind kind, const std::string& corr,
                          const Endpoint& to) {
  if (!open_.load()) return Status::Fail("config: node not open");
  msg.kind = kind;
  msg.correlation_id = corr;
  auto bytes = codec_->Encode(msg);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value, to);
}

Status CommNode::Send(Message msg, const Endpoint& to) {
  return SendKind(std::move(msg), MessageKind::kOneway, std::string(), to);
}

Status CommNode::RequestImpl(Message msg, FeedbackFn on_feedback, ReplyFn on_final,
                             uint32_t timeout_ms) {
  if (!open_.load()) {
    if (on_final) on_final(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  const std::string corr = NextCorrId();
  std::weak_ptr<CommNode> wself = weak_from_this();
  {  // 登记挂起 + 排超时(原子,防 reply/超时早于登记)
    std::lock_guard<std::mutex> lk(mu_);
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms),
        [wself, corr] { if (auto s = wself.lock()) s->FireTimeout(corr); });
    pending_[corr] = Pending{std::move(on_feedback), std::move(on_final), timer};
  }
  msg.kind = MessageKind::kRequest;
  msg.correlation_id = corr;
  auto bytes = codec_->Encode(msg);
  Status send_st = bytes ? transport_->Send(bytes.value) : Status::Fail(bytes.error);
  if (!send_st) {  // 编码/发送失败 → 回滚挂起 + 立即以该错误终结
    ReplyFn cb;
    { std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(corr);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.on_final); pending_.erase(it); } }
    if (cb) cb(Result<Message>::Fail(send_st.error));
    return send_st;
  }
  return Ok();
}

Status CommNode::Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms) {
  return RequestImpl(std::move(msg), nullptr, std::move(on_reply), timeout_ms);
}
Status CommNode::Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final,
                         uint32_t timeout_ms) {
  return RequestImpl(std::move(msg), std::move(on_feedback), std::move(on_final), timeout_ms);
}
std::future<Result<Message>> CommNode::Request(Message msg, uint32_t timeout_ms) {
  auto prom = std::make_shared<std::promise<Result<Message>>>();
  auto fut = prom->get_future();
  RequestImpl(std::move(msg), nullptr,
              [prom](Result<Message> r) { prom->set_value(std::move(r)); }, timeout_ms);
  return fut;
}

void CommNode::Dispatch(Message msg) {
  switch (msg.kind) {
    case MessageKind::kReply:
    case MessageKind::kFeedback: {
      const bool is_reply = (msg.kind == MessageKind::kReply);
      FeedbackFn fb; ReplyFn final_cb; bool found = false;
      {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(msg.correlation_id);
        if (it != pending_.end()) {
          found = true;
          if (is_reply) { final_cb = std::move(it->second.on_final); executor_->Cancel(it->second.timer); pending_.erase(it); }
          else { fb = it->second.on_feedback; }  // 拷贝,保留挂起
        }
      }
      if (!found) return;
      if (is_reply) { if (final_cb) final_cb(Result<Message>::Success(std::move(msg))); }
      else { if (fb) fb(msg); }
      break;
    }
    case MessageKind::kRequest:
      OnRequest(msg, Responder(weak_from_this(), msg.correlation_id, Endpoint::Default()));
      break;
    case MessageKind::kOneway:
    case MessageKind::kNotify:
      OnMessage(msg);
      break;
  }
}

void CommNode::FireTimeout(const std::string& corr) {
  ReplyFn cb;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(corr);
    if (it == pending_.end()) return;  // 已被应答
    cb = std::move(it->second.on_final);
    pending_.erase(it);
  }
  if (cb) cb(Result<Message>::Fail("timeout: request timed out"));
}

void CommNode::HandleDisconnect(const std::string& reason) {
  std::map<std::string, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.on_final) kv.second.on_final(Result<Message>::Fail(reason));
  }
  OnDisconnected(reason);
}

}  // namespace transport
```
把 `src/comm/CommNode.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 7: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R CommNode 2>&1 | grep -iE "passed|failed"`。Expected: 7 个 `CommNode.*` 用例通过。

- [ ] **Step 8: 提交**
```bash
git add include/transport/comm/CommNode.hpp src/comm/CommNode.cpp tests/comm/inline_executor.hpp tests/comm/fake_transport.hpp tests/comm/comm_node_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: CommNode(交互模式基类:Send/Request 回调+future/结果反馈/OnRequest;executor 可换)"
```

---

## Task 3: 全量验证

**Files:** 无(仅验证)

- [ ] **Step 1: 干净构建零告警**
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings (none)---"
```
Expected: 无 `warning`/`error:`。

- [ ] **Step 2: 全量测试连跑两次(查 flaky;含 ThreadExecutor 真实线程用例)**
```bash
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 两次都 `100% tests passed`(原 46 + ThreadExecutor 4 + CommNode 7 = 57)。

- [ ] **Step 3: 解耦检查(comm 层只依赖接口,不碰具体 transport/codec 实现)**
```bash
grep -rn "UdpTransport\|TcpClientTransport\|SerialTransport\|DdsTransport\|FastDds\|SystemCodec\|LengthFieldCodec\|DatagramCodec" include/transport/comm src/comm || echo "(comm 层只依赖 ITransport/ICodec/IExecutor 接口 —— 解耦保持)"
```
Expected: 无输出(`src/comm/CommNode.cpp` 仅 `#include ThreadExecutor.hpp` 作默认执行器,属同层;不依赖任何具体 transport/codec)。

> 本任务无新增文件;若前序均已提交且工作树干净,无需额外提交。

---

## 完成标准
- `IExecutor` 缝 + `ThreadExecutor`(worker+有界队列+定时器)落地、单测通过(含背压)。
- `CommNode` 交互模式全齐:`Send`、`Request`(回调 / future)、`Request`(结果反馈)、`OnRequest`+`Responder`;按 kind 统一分发;请求超时 / 断连终结挂起 / 恰好一次。
- **执行器可换性**:同一交互测试在 `InlineExecutor`(确定性)与 `ThreadExecutor`(真实线程)下都通过 —— 证明 `CommNode` 逻辑与执行器解耦,未来 `CoroExecutor` 可即插即换。
- 干净构建零告警;全量 57 测试稳定通过;comm 层只依赖 `ITransport`/`ICodec`/`IExecutor` 接口。
- 范围外(未做):`CoroExecutor`、DDS `DdsNode`、服务端 `ServerNode`、pull 三模式。
