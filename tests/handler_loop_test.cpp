// -----------------------------------------------------------------------------
// handler_loop_test.cpp — HandlerLoop<Event> 契约单测(ADR-0006 D4 /
// RT_HANDLER_003 / RT_HANDLER_006 / RT_LIFECYCLE_006 / RT_DESIGN_008 / P5-3/P5-4)
//
// 覆盖:串行消费(出队一条跑完再出下一条)/ 逃逸异常被边界兜住并计数、不自关、继续下一条 /
// 队列满 tail-drop 归因 business-queue-overflow / `Join()` 等消费者**实际退出**(ADR-0005
// D2 的 FiberTask::get())/ 未 Spawn 时 Join 立即返回 / `CancelAndClose` 关队列 + 触发协作
// 取消(幂等)/ `DrainForClose()` 报未启动条数(归因归调用方)/ handler 调用起止 Trace 与
// 时长计量。
// 消费者在 fiber 调度器内跑(coro_test_main 范式):HandlerLoop 内部用 makeTask 起消费者
// fiber,主(测试)fiber 用 pumpFiberUntil 驱动让出/时钟。
//
// 协议无关证据(RT_DESIGN_008 / ADR-0003 D10):本文件不 include transport/Message.hpp,
// Event 全程为 int 或本地结构,字节计量经注入回调取得。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/node/BoundedQueue.hpp"
#include "transport/node/HandlerLoop.hpp"
#include "coro_test_util.hpp"

using testutil::pumpFiberUntil;
using transport::BoundedQueue;
using transport::CapturingTraceSink;
using transport::DropReason;
using transport::DropReasonName;
using transport::HandlerLoop;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 每个元素计 1 字节:令字节上界与事件上界解耦,便于单独驱动事件上界的 tail-drop 路径。
HandlerLoop<int>::ByteSizeOf UnitBytes() {
  return [](const int&) { return std::size_t{1}; };
}

// 默认上界的小件(事件/字节上界均不绑),供只关心消费语义的用例使用。
std::unique_ptr<HandlerLoop<int>> MakeLoop(transport::ITraceSink* sink = nullptr) {
  return std::make_unique<HandlerLoop<int>>(UnitBytes(),
                                            BoundedQueue<int>::kDefaultMaxEvents,
                                            BoundedQueue<int>::kMaxBytes, sink);
}

}  // namespace

// 串行消费(RT_HANDLER_003):出队一条 → 跑 consume 到完成(含其让出)→ 再出下一条。
// consume 内让出若干轮,若小件并发派发则会观察到 in_flight>1;实测恒为 1,且顺序 = FIFO。
TEST(HandlerLoop, ConsumesQueuedEventsSeriallyInFifoOrder) {
  auto loop = MakeLoop();

  std::vector<int> consumed;
  int in_flight = 0;
  int max_in_flight = 0;
  loop->Spawn([&](int&& value) {
    ++in_flight;
    max_in_flight = std::max(max_in_flight, in_flight);
    // 在 consume 内让出:严格串行意味着此时不得有第二条被启动。
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
    consumed.push_back(value);
    --in_flight;
  });

  EXPECT_TRUE(loop->Enqueue(1));
  EXPECT_TRUE(loop->Enqueue(2));
  EXPECT_TRUE(loop->Enqueue(3));

  EXPECT_TRUE(pumpFiberUntil([&] { return consumed.size() == 3u; }));
  EXPECT_EQ(consumed, (std::vector<int>{1, 2, 3}));
  EXPECT_EQ(max_in_flight, 1);

  loop->CancelAndClose();  // 关队列 → 在途 Pop 返 kClosed → 消费者退出。
  loop->Join();
}

// 逃逸异常(RT_HANDLER_006):边界兜住 → 计数 +1 → **不自关**,继续消费下一条。
TEST(HandlerLoop, ConsumeExceptionIsCountedAndLoopContinues) {
  auto loop = MakeLoop();

  std::vector<int> consumed;
  loop->Spawn([&](int&& value) {
    if (value == 1) {
      throw std::runtime_error("handler boom");  // 逃逸异常。
    }
    consumed.push_back(value);
  });

  EXPECT_TRUE(loop->Enqueue(1));  // 抛
  EXPECT_TRUE(loop->Enqueue(2));  // 仍应被消费

  EXPECT_TRUE(pumpFiberUntil([&] { return consumed.size() == 1u; }));
  EXPECT_EQ(consumed, (std::vector<int>{2}));
  EXPECT_EQ(loop->HandlerExceptionCount(), 1u);

  // 消费者未自关:再入队一条仍被处理。
  EXPECT_TRUE(loop->Enqueue(3));
  EXPECT_TRUE(pumpFiberUntil([&] { return consumed.size() == 2u; }));
  EXPECT_EQ(consumed.back(), 3);
  EXPECT_EQ(loop->HandlerExceptionCount(), 1u);

  loop->CancelAndClose();
  loop->Join();
}

// 队列满 tail-drop:Enqueue 返 kResourceExhausted,归因计数 business_queue_overflow +1
// (归因由队列自己做,见 HandlerLoop 文件头"归因分工")。
TEST(HandlerLoop, EnqueueTailDropsWhenQueueFullAndCountsOverflow) {
  HandlerLoop<int> loop(UnitBytes(), /*max_events=*/1, BoundedQueue<int>::kMaxBytes);

  EXPECT_TRUE(loop.Enqueue(1));
  auto dropped = loop.Enqueue(2);  // 满(未 Spawn,无人消费)→ tail-drop。
  ASSERT_FALSE(dropped);
  EXPECT_EQ(dropped.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(loop.BusinessQueueOverflowCount(), 1u);
}

// tail-drop 归因经可选 sink 上报为 category="drop" / message=business-queue-overflow
// (P5-3;RT_TRACE_002:计数与上文用例一致,不因有无 sink 而变)。
TEST(HandlerLoop, TailDropIsAttributedToBusinessQueueOverflowOnSink) {
  CapturingTraceSink sink;
  HandlerLoop<int> loop(UnitBytes(), /*max_events=*/1, BoundedQueue<int>::kMaxBytes,
                        &sink);

  EXPECT_TRUE(loop.Enqueue(1));
  EXPECT_FALSE(loop.Enqueue(2));
  EXPECT_EQ(loop.BusinessQueueOverflowCount(), 1u);

  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message,
            DropReasonName(DropReason::kBusinessQueueOverflow));
}

// Join 等消费者**实际退出**、不强杀(RT_LIFECYCLE_006 / ADR-0005 D2):consume 挂在
// 一个由测试控制的 await 上时,即便队列已 Close,Join 也必须挡住;consume 返回后才放行。
//
// 小件与状态一律放堆上并由各 fiber 按值持有 shared_ptr:即便某轮让出预算不够(断言失败),
// 残留 fiber 也不会触碰已析构的测试栈对象。
TEST(HandlerLoop, JoinWaitsUntilConsumerActuallyExits) {
  struct State {
    bool entered = false;
    bool consume_returned = false;
    bool joined = false;
  };
  auto loop = std::make_shared<HandlerLoop<int>>(UnitBytes(),
                                                 BoundedQueue<int>::kDefaultMaxEvents,
                                                 BoundedQueue<int>::kMaxBytes);
  auto state = std::make_shared<State>();
  auto gate = std::make_shared<Coro::Awaitable<void>>();

  // consume 不捕获 loop(否则 loop→消费者 fiber→loop 成环,永不释放)。
  loop->Spawn([state, gate](int&&) {
    state->entered = true;
    (void)gate->await();  // 挂起在此:消费者 fiber 尚未退出。
    state->consume_returned = true;
  });
  EXPECT_TRUE(loop->Enqueue(1));
  EXPECT_TRUE(pumpFiberUntil([&] { return state->entered; }));

  loop->CancelAndClose();  // 关队列 + 触发取消:但消费者仍卡在 consume 内。
  auto joiner = Coro::makeTask([loop, state] {
    loop->Join();
    state->joined = true;
  });

  // consume 未返回 ⇒ Join 必须仍在等(让出若干轮为证)。
  EXPECT_FALSE(pumpFiberUntil([&] { return state->joined; }, 20));
  EXPECT_FALSE(state->consume_returned);

  gate->resolve();  // 放行 consume → 消费者跑完本条、Pop 得 kClosed 后退出。
  EXPECT_TRUE(pumpFiberUntil([&] { return state->joined; }));
  EXPECT_TRUE(state->consume_returned);
  EXPECT_TRUE(joiner.get());
}

// 未 Spawn(没有消费者)⇒ 无可汇合者,Join 立即返回;Drain 仍报残留条数。
TEST(HandlerLoop, JoinWithoutSpawnReturnsImmediately) {
  auto loop = MakeLoop();
  loop->Join();  // 不得挂起。
  loop->CancelAndClose();
  loop->Join();  // Close 后再 Join 仍立即返回(幂等)。
  SUCCEED();
}

// CancelAndClose:关业务队列(此后 Enqueue 一律 kClosed)+ 触发 handler 协作取消令牌;
// 二者各自幂等,重复调用不改变可观察状态。
TEST(HandlerLoop, CancelAndCloseClosesQueueAndTriggersTokenIdempotently) {
  auto loop = MakeLoop();
  EXPECT_FALSE(loop->Token().IsCancellationRequested());

  loop->CancelAndClose();
  EXPECT_TRUE(loop->Token().IsCancellationRequested());

  auto rejected = loop->Enqueue(1);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), make_error_code(TransportErrc::kClosed));

  loop->CancelAndClose();  // 幂等。
  EXPECT_TRUE(loop->Token().IsCancellationRequested());
  EXPECT_FALSE(loop->Enqueue(2));
}

// DrainForClose:只报**未启动**的排队条数(不排空处理、不归因——close_drop 归调用方);
// 取尽后再调返 0。
TEST(HandlerLoop, DrainForCloseReturnsUnstartedEventCount) {
  auto loop = MakeLoop();  // 未 Spawn:入队的三条都"未启动"。
  EXPECT_TRUE(loop->Enqueue(1));
  EXPECT_TRUE(loop->Enqueue(2));
  EXPECT_TRUE(loop->Enqueue(3));

  loop->CancelAndClose();
  EXPECT_EQ(loop->DrainForClose(), 3u);
  EXPECT_EQ(loop->DrainForClose(), 0u);  // 已取尽。
}

// 已被消费者取走的事件不计入 DrainForClose(它只报"未启动"的)。
TEST(HandlerLoop, DrainForCloseExcludesAlreadyConsumedEvents) {
  auto loop = MakeLoop();
  std::size_t consumed = 0;
  loop->Spawn([&](int&&) { ++consumed; });

  EXPECT_TRUE(loop->Enqueue(1));
  EXPECT_TRUE(pumpFiberUntil([&] { return consumed == 1u; }));

  EXPECT_TRUE(loop->Enqueue(2));
  EXPECT_TRUE(pumpFiberUntil([&] { return consumed == 2u; }));

  loop->CancelAndClose();
  loop->Join();
  EXPECT_EQ(loop->DrainForClose(), 0u);  // 两条都已消费完,无残留。
}

// P5-4:单次 handler 调用时长(简单存最近值)与调用起止 Trace(category="handler",
// message="start"/"end";逃逸异常时止点 message="exception")。
TEST(HandlerLoop, RecordsHandlerDurationAndStartEndTrace) {
  CapturingTraceSink sink;
  auto loop = MakeLoop(&sink);
  EXPECT_EQ(loop->LastHandlerDuration(), HandlerLoop<int>::Clock::duration::zero());

  std::size_t consumed = 0;
  loop->Spawn([&](int&& value) {
    boost::this_fiber::sleep_for(std::chrono::milliseconds(5));
    if (value == 2) {
      throw std::runtime_error("handler boom");
    }
    ++consumed;
  });

  EXPECT_TRUE(loop->Enqueue(1));
  EXPECT_TRUE(pumpFiberUntil([&] { return consumed == 1u; }));
  EXPECT_GT(loop->LastHandlerDuration(), HandlerLoop<int>::Clock::duration::zero());

  EXPECT_TRUE(loop->Enqueue(2));  // 抛 → 止点为 "exception"。
  EXPECT_TRUE(pumpFiberUntil([&] { return loop->HandlerExceptionCount() == 1u; }));

  loop->CancelAndClose();
  loop->Join();

  std::vector<std::string> handler_messages;
  for (const auto& record : sink.Records()) {
    if (record.category == "handler") {
      handler_messages.push_back(record.message);
    }
  }
  EXPECT_EQ(handler_messages,
            (std::vector<std::string>{"start", "end", "start", "exception"}));
}

// 协议无关(RT_DESIGN_008):Event 换成本地自定义结构、字节计量取其 blob 大小——小件
// 不读 Event 任何字段,只经注入回调不透明地计量。
TEST(HandlerLoop, ProtocolAgnosticCustomEventType) {
  struct Event {
    std::string name;
    std::vector<std::uint8_t> blob;
  };
  // 每条 4 字节、字节上界 kMinBytes(64 KiB)不绑;事件上界 =1 → 第二条 tail-drop。
  HandlerLoop<Event> loop([](const Event& e) { return e.blob.size(); },
                          /*max_events=*/1, BoundedQueue<Event>::kMinBytes);

  std::vector<std::string> consumed;
  loop.Spawn([&](Event&& e) { consumed.push_back(e.name); });

  EXPECT_TRUE(loop.Enqueue(Event{"a", {1, 2, 3, 4}}));
  EXPECT_TRUE(pumpFiberUntil([&] { return consumed.size() == 1u; }));
  EXPECT_EQ(consumed, (std::vector<std::string>{"a"}));

  loop.CancelAndClose();
  loop.Join();
}
