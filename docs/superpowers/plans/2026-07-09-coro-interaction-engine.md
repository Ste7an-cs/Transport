# 协程版 InteractionEngine(第二期·核心)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个协程原生(AsyncTask/boost.fiber)的 `transport::coro::InteractionEngine` 及薄壳 `transport::coro::ProtocolNode`,把请求-应答从异步状态机变成线性的 `send(); r = await_for(timeout);`,复用第一期 QtNetwork `ITransport`。

**Architecture:** 引擎持一个 `ITransport`,把 `OnBytes` 桥接进按 `Key` 挂起的 `Coro::Awaitable<Message>`;`Request` 在 fiber 上 `await_for(timeout)`。协议差异仍外包给 `InteractionPolicy`(与异步引擎同缝)。全程单 fiber 调度线程 = Qt 事件循环线程,协作式、无锁。

**Tech Stack:** C++17;Qt5(Core/Network);AsyncTask(头文件式协程库,`ASYNC_HAS_QTCORE`);boost `fiber/context/thread/chrono`(已编译静态库,`/usr/local/lib`);GoogleTest。

## Global Constraints

- C++17,**不抛异常**;对外错误用 `transport::Result`/`transport::Status`,前缀分类 `config:`/`io:`/`conn:`/`timeout:` 不变。
- **纯加性**:新增 `transport::coro` 目标 + `InteractionPolicy::IsTerminal` 默认实现;**不改**异步栈(`InteractionEngine`/`IExecutor`/`ThreadExecutor`/异步 `ProtocolNode`/`DdsNode`)、**不改** `ITransport`、**不改**第一期传输。
- AsyncTask **不加修改**;单线程靠 `makeTask` 默认 affinity=当前线程 + 单 fiber 调度线程。
- **铁律**:`Request()`/`await_for` 挂起的是 fiber,**必须在 `Coro::makeTask` 内调用**,不可在裸非 fiber 线程调。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**无 Co-Authored-By**;不提交 `build/`。
- 命名空间 `transport::coro`;新文件在 `include/transport/coro/`、`src/coro/`、`tests/coro/`。
- 外部路径以 CMake 缓存变量给默认值:`ASYNCTASK_DIR=/home/david/zpj/Framework-dev/AsyncTask`,`BOOST_LOCAL_ROOT=/usr/local`。

## 关键接口(跨任务一致,逐字使用)

- `transport::Key = std::string`;`transport::FrameTag = int`。
- `InteractionPolicy`(现有,纯虚):`FrameTag TagOf(const Message&)`、`void SetTag(Message&, FrameTag)`、`Key NewCorrelation(Message&)`、`Key KeyOf(const Message&)`、`Route RouteUnmatched(const Message&)`;`enum class Route { kInboundRequest, kDeliver, kDrop }`。**本计划新增**非纯虚 `virtual bool IsTerminal(FrameType) const`(默认 `== kResult`)。
- `ICodec`:`Result<std::vector<uint8_t>> Encode(const Message&)`;`Result<std::vector<Message>> Decode(const uint8_t*, std::size_t)`。用 `transport::SystemDatagramCodec`(单帧,含 `Encode`/`Decode`,已验证)。
- `ITransport`:`Status Open()`、`void Close()`、`bool IsOpen() const`、`Status Send(bytes)`、`Status Send(bytes, const Endpoint&)`、`void OnBytes(BytesCallback)`、`void OnConnect(fn)`、`void OnDisconnect(fn)`;`BytesCallback = std::function<void(Result<std::vector<uint8_t>>, const std::string&)>`。
- `Coro::Awaitable<T>`:`std::shared_ptr<Coro::FiberChannel<T>> channel()`、`Result<T,std::error_code> await_for(dur)`、`bool resolve(const T&)`、`void close()`。`Coro::FiberChannel<T>`:`push(T)`、`bool is_closed()`、`close()`。`Coro::Result<T,E>`:`explicit operator bool()`、`T& value()`。
- 引擎(本计划产出):
  - `coro::InteractionEngine(std::shared_ptr<ITransport>, std::unique_ptr<ICodec>, std::unique_ptr<InteractionPolicy>)`
  - `Status Open()`;`void Close()`
  - `Status Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default())`
  - `Result<Message> Request(Message out, FrameTag tag, std::chrono::milliseconds timeout, const Endpoint& to = Endpoint::Default())`
  - `void OnInboundDeliver(std::function<void(const Message&)>)`;`void SetKeyFn(std::function<Key(const Message&)>)`
- 节点(本计划产出):`coro::ProtocolNode(std::shared_ptr<ITransport>, uint8_t protocol_id, bool reply_to_source=false)`;`Status Start()`;`void Stop()`;`Result<Message> Request(uint16_t cmd, std::vector<uint8_t> payload, std::chrono::milliseconds timeout)`。

## 文件结构

- `CMakeLists.txt`(改):新增 `option(TRANSPORT_BUILD_CORO ...)` 段——`asynctask` 静态库、`boost_fiber_local` 接口库、`transport_coro` 静态库、`transport_coro_tests`。
- `include/transport/comm/InteractionPolicy.hpp`(改):加 `IsTerminal` 默认虚函数。
- `include/transport/coro/InteractionEngine.hpp`、`src/coro/InteractionEngine.cpp`(新)。
- `include/transport/coro/ProtocolNode.hpp`、`src/coro/ProtocolNode.cpp`(新)。
- `tests/coro/coro_test_main.cpp`(新):fiber 测试 main。
- `tests/coro/coro_test_util.hpp`(新):`FakeTransport` + `pumpFiberUntil`。
- `tests/coro/*_test.cpp`(新):各任务测试。

---

### Task 1: 构建接入 AsyncTask + boost + fiber 测试夹具(hello-world)

**Files:**
- Modify: `CMakeLists.txt`
- Create: `tests/coro/coro_test_main.cpp`, `tests/coro/coro_harness_test.cpp`

**Interfaces:**
- Produces:CMake 目标 `asynctask`/`boost_fiber_local`/`transport_coro_tests`;可运行的 fiber 测试(`installFiberApplication`+`makeTask(RUN_ALL_TESTS)`+`exec`)。

- [ ] **Step 1: CMake 新增协程段**

在 `CMakeLists.txt` 末尾(`transport_tests` 之后)追加:
```cmake
# ---------------------------------------------------------------------------
# 协程栈(第二期):AsyncTask + boost.fiber。纯加性,可关。
# ---------------------------------------------------------------------------
option(TRANSPORT_BUILD_CORO "Build coroutine InteractionEngine (AsyncTask/boost.fiber)" ON)
if(TRANSPORT_BUILD_CORO)
  set(ASYNCTASK_DIR "/home/david/zpj/Framework-dev/AsyncTask" CACHE PATH "AsyncTask coroutine lib root")
  set(BOOST_LOCAL_ROOT "/usr/local" CACHE PATH "boost prefix with compiled fiber/context/thread/chrono")

  # 已编译 boost 静态库(顺序:fiber 依赖 context)。
  add_library(boost_fiber_local INTERFACE)
  target_include_directories(boost_fiber_local INTERFACE ${BOOST_LOCAL_ROOT}/include)
  target_link_libraries(boost_fiber_local INTERFACE
    ${BOOST_LOCAL_ROOT}/lib/libboost_fiber.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_context.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_thread.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_chrono.a
    Threads::Threads)

  # AsyncTask 编译单元(含 Qt QObject → 需 AUTOMOC)。
  add_library(asynctask STATIC
    ${ASYNCTASK_DIR}/coro/executor/qtfiberthread.cpp
    ${ASYNCTASK_DIR}/coro/executor/fiberpool.cpp
    ${ASYNCTASK_DIR}/coro/detail/asyncdefine.cpp
    ${ASYNCTASK_DIR}/coro/task/fiberapplication.cpp
    ${ASYNCTASK_DIR}/coro/task/fibertask.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fibertaskqueue.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/qtlocalfiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/qtfiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fiberthreadblock.cpp)
  set_target_properties(asynctask PROPERTIES AUTOMOC ON)
  target_include_directories(asynctask PUBLIC ${ASYNCTASK_DIR}/coro)
  target_compile_definitions(asynctask PUBLIC ASYNC_HAS_QTCORE)
  target_compile_features(asynctask PUBLIC cxx_std_17)
  target_link_libraries(asynctask PUBLIC Qt5::Core Qt5::Network boost_fiber_local)

  if(TRANSPORT_BUILD_TESTS)
    add_executable(transport_coro_tests
      tests/coro/coro_test_main.cpp
      tests/coro/coro_harness_test.cpp)
    target_include_directories(transport_coro_tests PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/tests/coro)
    target_link_libraries(transport_coro_tests PRIVATE asynctask GTest::gtest Qt5::Core)
    add_test(NAME transport_coro_tests COMMAND transport_coro_tests)
  endif()
endif()
```

- [ ] **Step 2: 写 fiber 测试 main `tests/coro/coro_test_main.cpp`**
```cpp
#include <QCoreApplication>
#include <gtest/gtest.h>

#include "task/fiberapplication.h"   // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"          // Coro::makeTask

// 协程测试须在 fiber 调度器内跑:主线程装本地调度器,断言体在 makeTask 的 fiber 中执行
//(这样 Request/await_for 有 fiber 上下文),跑完 quit() 让 exec() 返回。
int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int rc = 0;
  Coro::installFiberApplication();
  auto task = Coro::makeTask([&] {
    rc = RUN_ALL_TESTS();
    Coro::quit();
  });
  Coro::exec();
  (void)task;
  return rc;
}
```

- [ ] **Step 3: 写 hello-world 测试 `tests/coro/coro_harness_test.cpp`**
```cpp
#include <gtest/gtest.h>
#include "await/awaitable.hpp"  // Coro::Awaitable

// 证明 fiber 运行时 + channel + 链接均通:在 fiber 内 resolve 再 await 取回。
TEST(CoroHarness, AwaitableRoundtrip) {
  Coro::Awaitable<int> aw;
  ASSERT_TRUE(aw.resolve(42));       // 生产者投递
  auto r = aw.await();               // 同 fiber 消费(队列已有值,立即返回)
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value(), 42);
}
```

- [ ] **Step 4: 配置 + 构建 + 运行**

Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) --target transport_coro_tests 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build -R transport_coro_tests --output-on-failure 2>&1 | tail -6`
Expected: 零 error/warning;`transport_coro_tests` 通过(`AwaitableRoundtrip` 绿)。若 boost/AsyncTask 链接失败,先解决(见风险 §8.1:确认 `/usr/local/lib/libboost_*.a` 存在、`ASYNCTASK_DIR` 正确、AUTOMOC 生效)。

- [ ] **Step 5: 提交**
```bash
git add CMakeLists.txt tests/coro/coro_test_main.cpp tests/coro/coro_harness_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "build(coro): 接入 AsyncTask + boost.fiber,fiber 测试夹具 + hello-world"
```

---

### Task 2: `IsTerminal` 钩子 + Fake 传输 + 引擎骨架(ctor/Open/Close/Fire)

**Files:**
- Modify: `include/transport/comm/InteractionPolicy.hpp`
- Create: `include/transport/coro/InteractionEngine.hpp`, `src/coro/InteractionEngine.cpp`, `tests/coro/coro_test_util.hpp`, `tests/coro/engine_fire_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes:Task 1 的 fiber 夹具。
- Produces:`InteractionPolicy::IsTerminal`;`coro::InteractionEngine`(ctor/Open/Close/Fire);`testutil::FakeTransport`、`testutil::pumpFiberUntil`。

- [ ] **Step 1: 给 `InteractionPolicy` 加 `IsTerminal`(纯加性)**

`include/transport/comm/InteractionPolicy.hpp`,在 `RouteUnmatched` 声明之后、`};` 之前加:
```cpp
  // IsTerminal:该 frm_type 是否【终结】一个请求(协程引擎 Request 用;异步引擎不调它)。
  // 默认 RESULT 终结;需要别的终结规则的 policy 可覆写。纯加性,不影响异步引擎行为。
  virtual bool IsTerminal(FrameType t) const { return t == FrameType::kResult; }
```
确认该文件已 `#include "transport/Message.hpp"`(用到 `FrameType`);若无则加到 include 区。

- [ ] **Step 2: 写 `tests/coro/coro_test_util.hpp`(Fake 传输 + fiber 泵)**
```cpp
#pragma once
#include <chrono>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

#include "transport/ITransport.hpp"

namespace testutil {

// 可编程假传输:记录 Send 的字节;inject() 在当前(fiber)线程把一帧交给引擎 demux。
class FakeTransport : public transport::ITransport {
 public:
  std::vector<std::vector<uint8_t>> sent;

  transport::Status Open() override { open_ = true; return transport::Status::Success(std::monostate{}); }
  void Close() override { open_ = false; }
  bool IsOpen() const override { return open_; }
  transport::Status Send(const std::vector<uint8_t>& b) override {
    sent.push_back(b); return transport::Status::Success(std::monostate{});
  }
  transport::Status Send(const std::vector<uint8_t>& b, const transport::Endpoint&) override { return Send(b); }
  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

  // 注入一帧字节(触发引擎 demux)。
  void inject(const std::vector<uint8_t>& bytes, const std::string& from = "test") {
    if (bytes_cb_) bytes_cb_(transport::Result<std::vector<uint8_t>>::Success(bytes), from);
  }
  // 模拟对端断开。
  void dropPeer(const std::string& reason = "conn: peer reset") {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

 private:
  bool open_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

// 在 fiber 里让出并推进时间,直到 pred() 为真或超过 budget_ms。sleep_for 既让出其他 fiber、
// 又推进 fiber 调度器时钟(供 await_for 超时),避免忙等。
inline bool pumpFiberUntil(std::function<bool()> pred, int budget_ms = 3000) {
  for (int i = 0; i < budget_ms && !pred(); ++i)
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
  return pred();
}

}  // namespace testutil
```

- [ ] **Step 3: 写 `include/transport/coro/InteractionEngine.hpp`**
```cpp
#pragma once

// coro::InteractionEngine — 协程原生交互引擎(通用机制,协议差异外包给 InteractionPolicy)。
// 请求-应答 = 线性 send();await_for(timeout);。活在 fiber 调度线程 = Qt 事件循环线程,协作式无锁。

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/InteractionPolicy.hpp"

#include "await/awaitable.hpp"  // Coro::Awaitable / Coro::FiberChannel

namespace transport {
namespace coro {

class InteractionEngine {
 public:
  InteractionEngine(std::shared_ptr<ITransport> transport,
                    std::unique_ptr<ICodec> codec,
                    std::unique_ptr<InteractionPolicy> policy);
  ~InteractionEngine();

  Status Open();   // 接 OnBytes/OnDisconnect;不负责 transport->Open()
  void   Close();  // 幂等:唤醒并终结所有挂起(在途 Request 返回 conn:)

  // Fire:单向发。SetTag → Encode → Send。不登记、不等待。
  Status Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  // Request:发 out(tag)并按 key 挂起,await_for(timeout)。仅挂起【当前 fiber】。
  //   返回:应答(终结帧)/ "timeout:" / "conn:"(Close 或断连)。
  Result<Message> Request(Message out, FrameTag tag, std::chrono::milliseconds timeout,
                          const Endpoint& to = Endpoint::Default());

  // 无主入站帧(RouteUnmatched==kDeliver)交此钩子;未设则丢弃。
  void OnInboundDeliver(std::function<void(const Message&)> cb) { on_deliver_ = std::move(cb); }
  // 可插拔入站关联键(默认 policy.KeyOf);自定义配对算法在此注入。
  void SetKeyFn(std::function<Key(const Message&)> fn) { key_fn_ = std::move(fn); }

 private:
  void onBytes(Result<std::vector<uint8_t>> r, const std::string& from);
  void onDisconnect(const std::string& reason);

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<InteractionPolicy> policy_;
  std::function<Key(const Message&)> key_fn_;
  std::function<void(const Message&)> on_deliver_;
  std::map<Key, std::shared_ptr<Coro::FiberChannel<Message>>> pending_;
  bool closing_ = false;
};

}  // namespace coro
}  // namespace transport
```

- [ ] **Step 4: 写 `src/coro/InteractionEngine.cpp`(本任务仅 ctor/Open/Close/Fire;Request/demux 留 Task 3)**
```cpp
#include "transport/coro/InteractionEngine.hpp"

#include <utility>
#include <variant>

namespace transport {
namespace coro {

InteractionEngine::InteractionEngine(std::shared_ptr<ITransport> transport,
                                     std::unique_ptr<ICodec> codec,
                                     std::unique_ptr<InteractionPolicy> policy)
    : transport_(std::move(transport)), codec_(std::move(codec)), policy_(std::move(policy)) {
  key_fn_ = [this](const Message& m) { return policy_->KeyOf(m); };  // 默认键
}

InteractionEngine::~InteractionEngine() { Close(); }

Status InteractionEngine::Open() {
  transport_->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& from) {
    onBytes(std::move(r), from);
  });
  transport_->OnDisconnect([this](const std::string& reason) { onDisconnect(reason); });
  closing_ = false;
  return Status::Success(std::monostate{});
}

void InteractionEngine::Close() {
  if (closing_) return;
  closing_ = true;
  for (auto& kv : pending_) kv.second->close();  // 唤醒等待者(await_for 返回错误 → Request 判 conn:)
  pending_.clear();
}

Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  auto enc = codec_->Encode(out);
  if (!enc) return Status::Fail(enc.error);
  return transport_->Send(enc.value, to);
}

// Request / onBytes / onDisconnect 在 Task 3 实现(本任务先留空存根以便链接)。
Result<Message> InteractionEngine::Request(Message, FrameTag, std::chrono::milliseconds,
                                           const Endpoint&) {
  return Result<Message>::Fail("config: not implemented");  // Task 3 覆盖
}
void InteractionEngine::onBytes(Result<std::vector<uint8_t>>, const std::string&) {}
void InteractionEngine::onDisconnect(const std::string&) {}

}  // namespace coro
}  // namespace transport
```

- [ ] **Step 5: CMake 加 `transport_coro` 库并入测试**

在 `if(TRANSPORT_BUILD_CORO)` 段内、`add_library(asynctask ...)` 之后加:
```cmake
  add_library(transport_coro STATIC src/coro/InteractionEngine.cpp)
  target_include_directories(transport_coro PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_features(transport_coro PUBLIC cxx_std_17)
  target_link_libraries(transport_coro PUBLIC transport asynctask)
```
把测试目标改为链接 `transport_coro` 并加新测试源:
```cmake
    target_link_libraries(transport_coro_tests PRIVATE transport_coro asynctask GTest::gtest Qt5::Core)
    target_sources(transport_coro_tests PRIVATE tests/coro/engine_fire_test.cpp)
```
(即在 Task 1 的 `add_executable`/`target_link_libraries` 基础上:链接由 `asynctask` 换成 `transport_coro asynctask`,并 `target_sources` 追加 `engine_fire_test.cpp`。)

- [ ] **Step 6: 写失败测试 `tests/coro/engine_fire_test.cpp`**
```cpp
#include <memory>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "coro_test_util.hpp"

using transport::FrameTag;
using transport::FrameType;
using transport::Message;
using transport::ProtocolPolicy;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;

// Fire 单向发:编码后交给传输,字节可被解回同一条消息。
TEST(CoroEngine, FireEncodesAndSends) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(7));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Message m; m.message_id = 0x0055; m.payload = {0xAB};
  auto st = eng.Fire(m, static_cast<FrameTag>(FrameType::kCommand));
  ASSERT_TRUE(static_cast<bool>(st));
  ASSERT_EQ(tp->sent.size(), 1u);

  SystemDatagramCodec codec;
  auto back = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  ASSERT_TRUE(static_cast<bool>(back));
  ASSERT_EQ(back.value.size(), 1u);
  EXPECT_EQ(back.value[0].frm_type, FrameType::kCommand);   // SetTag 生效
  EXPECT_EQ(back.value[0].message_id, 0x0055);
  EXPECT_EQ(back.value[0].protocol_id, 0);                  // Fire 仅 SetTag,不调 NewCorrelation → protocol_id 保持默认 0
  EXPECT_EQ(back.value[0].payload, (std::vector<uint8_t>{0xAB}));
}
```
注:`Fire` 是纯单向发,只 `SetTag(frm_type)`,不滚 session_id、不盖 protocol_id(那些是 `Request`/`NewCorrelation` 的活)。故解回的帧 `protocol_id`/`session_id` 均为消息默认值 0。

- [ ] **Step 7: 构建 + 跑 + 提交**

Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build 2>&1 | tail -3`
Expected: 零 error/warning;全量绿(含现有 118 + 协程目标)。
```bash
git add include/transport/comm/InteractionPolicy.hpp include/transport/coro/InteractionEngine.hpp src/coro/InteractionEngine.cpp tests/coro/coro_test_util.hpp tests/coro/engine_fire_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat(coro): InteractionPolicy::IsTerminal + 引擎骨架(ctor/Open/Close/Fire)+ Fake 传输"
```

---

### Task 3: `Request` + demux(往返 / 超时 / 断连 / 中间帧丢弃)

**Files:**
- Modify: `src/coro/InteractionEngine.cpp`
- Create: `tests/coro/engine_request_test.cpp`
- Modify: `CMakeLists.txt`(测试源追加)

**Interfaces:**
- Consumes:Task 2 引擎骨架、Fake 传输、`pumpFiberUntil`。
- Produces:`Request` 与 demux 的真实实现。

- [ ] **Step 1: 写失败测试 `tests/coro/engine_request_test.cpp`**
```cpp
#include <chrono>
#include <memory>
#include <string>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "task/fibertask.h"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using transport::FrameTag;
using transport::FrameType;
using transport::Message;
using transport::ProtocolPolicy;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

namespace {
// 把 fake 已发出的命令帧解回,回显成一条 RESULT(终结)帧字节。
std::vector<uint8_t> MakeResultReply(const std::vector<uint8_t>& cmd_bytes,
                                     std::vector<uint8_t> payload) {
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(cmd_bytes.data(), cmd_bytes.size());
  Message reply = cmd.value[0];               // 回显 session_id/message_id/protocol_id
  reply.frm_type = FrameType::kResult;        // 终结
  reply.payload = std::move(payload);
  return codec.Encode(reply).value;
}
const FrameTag kCmd = static_cast<FrameTag>(FrameType::kCommand);
}  // namespace

TEST(CoroEngineRequest, Roundtrip) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42; m.payload = {1, 2, 3};
    got = eng.Request(m, kCmd, 1000ms);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));   // 等命令发出
  tp->inject(MakeResultReply(tp->sent[0], {9, 9}));                 // 注入终结应答
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{9, 9}));
  (void)req;
}

TEST(CoroEngineRequest, TimesOut) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 150ms);   // 不注入应答
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 2000));
  EXPECT_FALSE(static_cast<bool>(got));
  EXPECT_NE(got.error.find("timeout:"), std::string::npos);
  (void)req;
}

TEST(CoroEngineRequest, CloseFailsPendingWithConn) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 5000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));
  eng.Close();                                    // 关引擎 → 唤醒挂起
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  EXPECT_FALSE(static_cast<bool>(got));
  EXPECT_NE(got.error.find("conn:"), std::string::npos);
  (void)req;
}

TEST(CoroEngineRequest, IntermediateResponseDropped) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  // 先注入一条中间 RESPONSE(非终结)→ 不应终结请求。
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message resp = cmd.value[0]; resp.frm_type = FrameType::kResponse; resp.payload = {7};
  tp->inject(codec.Encode(resp).value);
  EXPECT_FALSE(pumpFiberUntil([&] { return done; }, 100));   // 仍未完成

  tp->inject(MakeResultReply(tp->sent[0], {8}));             // 再注入终结 RESULT
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{8}));
  (void)req;
}
```

- [ ] **Step 2: 加测试源到 CMake**

`if(TRANSPORT_BUILD_CORO)` 内测试 `target_sources` 追加:
```cmake
    target_sources(transport_coro_tests PRIVATE tests/coro/engine_request_test.cpp)
```

- [ ] **Step 3: 运行确认失败**

Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) --target transport_coro_tests 2>&1 | tail -3; ctest --test-dir build -R transport_coro_tests --output-on-failure 2>&1 | tail -8`
Expected: `CoroEngineRequest.*` 失败(Request 仍是 `config: not implemented` 存根)。

- [ ] **Step 4: 实现 `Request` + demux + `onDisconnect`(替换 Task 2 的存根)**

在 `src/coro/InteractionEngine.cpp` 里,把 Task 2 的三个存根实现替换为:
```cpp
Result<Message> InteractionEngine::Request(Message out, FrameTag tag,
                                           std::chrono::milliseconds timeout,
                                           const Endpoint& to) {
  using R = Result<Message>;
  Key k = policy_->NewCorrelation(out);          // 滚 session_id、盖 protocol_id;返回挂起键
  policy_->SetTag(out, tag);
  Coro::Awaitable<Message> aw;
  auto ch = aw.channel();
  pending_[k] = ch;                              // 登记挂起(单线程,无锁)

  auto enc = codec_->Encode(out);
  if (!enc) { pending_.erase(k); return R::Fail(enc.error); }
  auto st = transport_->Send(enc.value, to);
  if (!st) { pending_.erase(k); return R::Fail(st.error); }

  auto r = aw.await_for(timeout);               // 仅挂起当前 fiber
  pending_.erase(k);
  if (r) return R::Success(r.value());
  // channel 被 close(Close/断连)→ conn:;否则真超时 → timeout:
  if (ch->is_closed()) return R::Fail("conn: engine closed or disconnected");
  return R::Fail("timeout: request timed out");
}

void InteractionEngine::onBytes(Result<std::vector<uint8_t>> r, const std::string& from) {
  if (!r) return;                                // 传输层错误:本核心丢弃
  auto msgs = codec_->Decode(r.value.data(), r.value.size());
  if (!msgs) return;
  for (auto& m : msgs.value) {
    if (m.source.empty()) m.source = from;       // 缺省来源
    Key k = key_fn_(m);
    auto it = pending_.find(k);
    if (it != pending_.end()) {
      if (policy_->IsTerminal(m.frm_type))
        it->second->push(m);                     // 唤醒请求 fiber(它会摘挂起)
      // 中间(非终结)帧:本期丢弃
      continue;
    }
    switch (policy_->RouteUnmatched(m)) {        // 无主帧
      case InteractionPolicy::Route::kDeliver:
        if (on_deliver_) on_deliver_(m);
        break;
      case InteractionPolicy::Route::kInboundRequest:  // 服务端(下期)——本期丢弃
      case InteractionPolicy::Route::kDrop:
        break;
    }
  }
}

void InteractionEngine::onDisconnect(const std::string&) {
  for (auto& kv : pending_) kv.second->close();  // 断连:唤醒挂起 → Request 返回 conn:
  pending_.clear();
}
```
(删掉 Task 2 里 `Request`/`onBytes`/`onDisconnect` 的存根定义,勿重复定义。)

- [ ] **Step 5: 构建 + 跑 + 全量 + 提交**

Run: `cmake --build build -j$(nproc) 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build --output-on-failure 2>&1 | tail -4`
Expected: 零 error/warning;`CoroEngineRequest.*` 4 例全绿;全量绿。
```bash
git add src/coro/InteractionEngine.cpp tests/coro/engine_request_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat(coro): Request + demux(往返/超时/断连 conn/中间帧丢弃)"
```

---

### Task 4: 可插拔关联键(`SetKeyFn`)

**Files:**
- Create: `tests/coro/engine_keyfn_test.cpp`
- Modify: `CMakeLists.txt`(测试源追加)

**Interfaces:**
- Consumes:Task 3 的 `Request`/demux;`SetKeyFn`(Task 2 已声明并默认 `policy.KeyOf`)。
- Produces:自定义键匹配的测试证据(无需改引擎——`key_fn_` 已就位)。

- [ ] **Step 1: 写失败测试 `tests/coro/engine_keyfn_test.cpp`**
```cpp
#include <chrono>
#include <memory>
#include <string>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "task/fibertask.h"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using transport::FrameTag;
using transport::FrameType;
using transport::Key;
using transport::Message;
using transport::ProtocolPolicy;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

// 自定义键:仅按 session_id 配对(应答 message_id 变了也能匹配)。
TEST(CoroEngineKeyFn, SessionOnlyMatchesWhenMessageIdChanges) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  eng.SetKeyFn([](const Message& m) -> Key { return Key(1, static_cast<char>(m.session_id)); });
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, static_cast<FrameTag>(FrameType::kCommand), 1000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  // 造应答:回显 session_id,但 message_id 改成 0x99(响应码),frm_type=RESULT。
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message reply = cmd.value[0];
  reply.message_id = 0x99;                        // 变了
  reply.frm_type = FrameType::kResult;
  reply.payload = {5};
  tp->inject(codec.Encode(reply).value);

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));   // 默认键会因 message_id 变而超时;自定义键匹配
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5}));
  (void)req;
}
```
注:出站键仍由 `policy.NewCorrelation` 决定(挂起用 `NewCorrelation` 返回的 `(session,message)`);本用例的 `SetKeyFn` 只改**入站**键。**为使二者可比,挂起键也须与入站键同域**——见 Step 2 的实现说明。

- [ ] **Step 2: 让挂起键与入站键同源(小改引擎)**

为使 `SetKeyFn` 自定义键真正生效,`Request` 登记挂起时也须用 `key_fn_`(而非直接用 `NewCorrelation` 的返回值)。在 `src/coro/InteractionEngine.cpp` 的 `Request` 中,把:
```cpp
  Key k = policy_->NewCorrelation(out);
  policy_->SetTag(out, tag);
```
改为:
```cpp
  policy_->NewCorrelation(out);       // 仍滚 session_id、盖 protocol_id(副作用)
  policy_->SetTag(out, tag);
  Key k = key_fn_(out);               // 挂起键与入站键同源(默认=policy.KeyOf;可被 SetKeyFn 覆盖)
```
(默认 `key_fn_ = policy.KeyOf`,而 `NewCorrelation` 出站帧的 `KeyOf` 即 `(session,message)`,与旧行为**逐字一致**;Task 3 的四个用例仍应全绿。)

- [ ] **Step 3: 加测试源 + 构建 + 跑 + 全量 + 提交**

`if(TRANSPORT_BUILD_CORO)` 内 `target_sources` 追加 `tests/coro/engine_keyfn_test.cpp`。
Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build --output-on-failure 2>&1 | tail -4`
Expected: 零 error/warning;`CoroEngineKeyFn.*` 绿;Task 3 四例仍绿;全量绿。
```bash
git add src/coro/InteractionEngine.cpp tests/coro/engine_keyfn_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat(coro): 可插拔关联键 SetKeyFn(挂起键与入站键同源)"
```

---

### Task 5: 协程 `ProtocolNode` 薄壳 + 真 UDP 回环冒烟

**Files:**
- Create: `include/transport/coro/ProtocolNode.hpp`, `src/coro/ProtocolNode.cpp`, `tests/coro/protocol_node_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes:`coro::InteractionEngine`(Task 2–4);第一期 `UdpTransport`。
- Produces:`coro::ProtocolNode`(`Start`/`Stop`/`Request(cmd,payload,timeout)`)。

- [ ] **Step 1: 写 `include/transport/coro/ProtocolNode.hpp`**
```cpp
#pragma once

// coro::ProtocolNode — 协程 ProtocolNode 薄壳。持 ITransport + coro::InteractionEngine(ProtocolPolicy)。
// Request 线性:构一条命令 → engine.Request(kCommand)。活在 fiber 调度线程。

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/coro/InteractionEngine.hpp"

namespace transport {
namespace coro {

class ProtocolNode {
 public:
  ProtocolNode(std::shared_ptr<ITransport> transport, uint8_t protocol_id,
               bool reply_to_source = false);

  Status Start();  // transport->Open() + engine.Open()
  void   Stop();   // engine.Close() + transport->Close()

  // Request:发命令码 cmd + payload,等终结应答(或 timeout:/conn:)。须在 fiber 内调。
  Result<Message> Request(uint16_t cmd, std::vector<uint8_t> payload,
                          std::chrono::milliseconds timeout);

 private:
  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<InteractionEngine> engine_;
};

}  // namespace coro
}  // namespace transport
```

- [ ] **Step 2: 写 `src/coro/ProtocolNode.cpp`**
```cpp
#include "transport/coro/ProtocolNode.hpp"

#include <utility>

#include "transport/Message.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"

namespace transport {
namespace coro {

ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, uint8_t protocol_id,
                           bool reply_to_source)
    : transport_(transport) {
  engine_ = std::make_unique<InteractionEngine>(
      transport, std::make_unique<SystemDatagramCodec>(),
      std::make_unique<ProtocolPolicy>(protocol_id, reply_to_source));
}

Status ProtocolNode::Start() {
  auto st = transport_->Open();
  if (!st) return st;
  return engine_->Open();
}

void ProtocolNode::Stop() {
  engine_->Close();
  transport_->Close();
}

Result<Message> ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload,
                                      std::chrono::milliseconds timeout) {
  Message m; m.message_id = cmd; m.payload = std::move(payload);
  return engine_->Request(m, static_cast<FrameTag>(FrameType::kCommand), timeout);
}

}  // namespace coro
}  // namespace transport
```

- [ ] **Step 3: CMake 加 ProtocolNode.cpp + 测试源**

`transport_coro` 源列表加 `src/coro/ProtocolNode.cpp`:
```cmake
  add_library(transport_coro STATIC src/coro/InteractionEngine.cpp src/coro/ProtocolNode.cpp)
```
`transport_coro_tests` `target_sources` 追加 `tests/coro/protocol_node_test.cpp`。`transport_coro` 已 `PUBLIC transport`(含 `UdpTransport`/`Qt5::Network`),测试无需再加。

- [ ] **Step 4: 写测试 `tests/coro/protocol_node_test.cpp`(Fake 节点往返 + 真 UDP 回环冒烟)**
```cpp
#include <chrono>
#include <memory>
#include <string>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/coro/ProtocolNode.hpp"
#include "transport/udp/UdpTransport.hpp"
#include "task/fibertask.h"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using transport::FrameType;
using transport::Message;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using transport::coro::ProtocolNode;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

// 节点层往返(Fake 传输):Request → 手工回显 RESULT → 返回。
TEST(CoroProtocolNode, RequestRoundtripOverFake) {
  auto tp = std::make_shared<FakeTransport>();
  auto node = std::make_shared<ProtocolNode>(tp, /*protocol_id*/3);
  ASSERT_TRUE(static_cast<bool>(node->Start()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    got = node->Request(0x77, {1}, 1000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message reply = cmd.value[0]; reply.frm_type = FrameType::kResult; reply.payload = {2, 2};
  tp->inject(codec.Encode(reply).value);

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{2, 2}));
  node->Stop();
  (void)req;
}

// 真 UDP 回环冒烟:两个 UdpTransport,一端是节点,另一端手工当"服务端"回一条 RESULT。
TEST(CoroProtocolNode, RequestOverUdpLoopback) {
  // 服务端 UDP:收到命令帧 → 回显成 RESULT。
  UdpConfig sc; sc.mode = UdpMode::kUnicast; sc.local_addr = "127.0.0.1"; sc.local_port = 0;
  auto srv = std::make_shared<UdpTransport>(sc);
  SystemDatagramCodec codec;
  srv->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& from) {
    if (!r) return;
    auto ms = codec.Decode(r.value.data(), r.value.size());
    if (!ms || ms.value.empty()) return;
    Message reply = ms.value[0]; reply.frm_type = FrameType::kResult; reply.payload = {0xEE};
    // from = "ip:port";回给来源。
    auto colon = from.rfind(':');
    std::string ip = from.substr(0, colon);
    uint16_t port = static_cast<uint16_t>(std::stoi(from.substr(colon + 1)));
    srv->Send(codec.Encode(reply).value, transport::Endpoint::Net(ip, port));
  });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  uint16_t srv_port = srv->LocalPort();

  // 客户端节点 UDP:默认目的地 = 服务端。
  UdpConfig cc; cc.mode = UdpMode::kUnicast; cc.local_addr = "127.0.0.1"; cc.local_port = 0;
  cc.remote_addr = "127.0.0.1"; cc.remote_port = srv_port;
  auto ctp = std::make_shared<UdpTransport>(cc);
  auto node = std::make_shared<ProtocolNode>(ctp, /*protocol_id*/1);
  ASSERT_TRUE(static_cast<bool>(node->Start()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    got = node->Request(0x10, {0xAA}, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 3000));   // sleep_for 泵进 Qt 事件 + fiber
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.frm_type, FrameType::kResult);
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{0xEE}));
  node->Stop(); srv->Close();
  (void)req;
}
```
注:真 UDP 用例依赖 fiber 调度器同时推进 Qt 事件循环(`installFiberApplication` 装的 `QtLocalFiberScheduler` 即为此)。`pumpFiberUntil` 的 `sleep_for` 让出 fiber 时,Qt 事件(UDP `readyRead`)也被调度器推进。若本环境该联动不成立(冒烟失败但 Fake 用例全绿),记为环境限制并在报告中说明(不改核心逻辑)。

- [ ] **Step 5: 构建 + 跑 + 全量 + 提交**

Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build --output-on-failure 2>&1 | tail -4`
Expected: 零 error/warning;`CoroProtocolNode.*` 绿(真 UDP 冒烟如受环境限制则如上说明);全量绿。
```bash
git add include/transport/coro/ProtocolNode.hpp src/coro/ProtocolNode.cpp tests/coro/protocol_node_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat(coro): ProtocolNode 薄壳 + 真 UDP 回环冒烟"
```

---

## 实现后(计划外)

- 同步 SRS/SDD/README/CHANGELOG(新增协程栈、构建需 AsyncTask+boost、fiber 线程模型与用法约束);终态全分支 review → finishing-a-development-branch。
- **下一期**:服务端 responder / `SendReply`(含"是否单独 server node")、周期 `StartPeriodic`、心跳、ack;DDS 搬到协程引擎(listener→fiber 线程桥)。

## Self-Review 记录

- **Spec 覆盖**:§4 引擎(Fire/Request/demux/OnInboundDeliver/SetKeyFn)→ T2–T4;§4.5 `IsTerminal` → T2;§5 节点 → T5;§6 构建(AsyncTask+boost+ASYNC_HAS_QTCORE+AUTOMOC)→ T1;§7 测试(fiber 夹具/Fake 传输/往返·超时·断连·中间帧·自定义键/真 UDP 冒烟)→ T1–T5;§8 风险(boost 库/ fiber 纪律/流式切帧)→ T1 gating + 注释。
- **占位扫描**:无 TBD;每步含完整代码与命令。engine_fire_test 的 protocol_id 断言给了"按实际行为诚实修正"的明确指示(Fire 不盖 protocol_id)。
- **类型一致**:`InteractionEngine` ctor/`Fire`/`Request`/`SetKeyFn`/`OnInboundDeliver` 签名 T2 定义、T3–T5 使用一致;`Key=std::string`、`FrameTag=int`、`Route::{kInboundRequest,kDeliver,kDrop}`、`IsTerminal(FrameType)`、`Coro::Awaitable::{channel,await_for,resolve,close}`、`FiberChannel::{push,is_closed,close}`、`Coro::Result::{operator bool,value}` 均与 AsyncTask/现有头逐字对齐;`ProtocolNode` 用 `SystemDatagramCodec`+`ProtocolPolicy(protocol_id,reply_to_source)`。
- **潜在坑已标注**:(1) `conn:` vs `timeout:` 靠 `FiberChannel::is_closed()` 区分(非用易误的全局标志);(2) 挂起键与入站键同源(T4 Step 2)以让 `SetKeyFn` 真正生效,且默认逐字保持旧 `(session,message)`;(3) `makeTask` 返回值须存活(各测试 `auto req=...; (void)req;`);(4) AUTOMOC 对 AsyncTask 的 `Q_OBJECT`(`fiberapplication.h` 等);(5) ctest 用整二进制单条 `add_test`(不用 `gtest_discover_tests`,避免发现期跑 fiber app)。
