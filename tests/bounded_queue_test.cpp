// -----------------------------------------------------------------------------
// bounded_queue_test.cpp — BoundedQueue<T> 契约单测(RT_HANDLER 3.1.5.4 /
// ADR-0002 D5-D6 / ADR-0003 D10 / RT_DESIGN_008 / RT_DATA_BUFFER)
//
// 覆盖:事件上限 tail-drop / 字节上限 tail-drop / FIFO / 空队列 await→Push 唤醒 /
// Close 唤醒 + 剩余可枚举 / 协议无关(用非 Message 的 int 与自定义结构测)。
// 消费者出队在 fiber 调度器内跑(coro_test_main 范式):消费 fiber 用 makeTask 起
// 独立 fiber 在 Pop 挂起,主(测试)fiber 用 pumpFiberUntil 驱动让出/时钟。
//
// 协议无关证据:本文件不 include transport/Message.hpp,T 全程为 int 或本地结构。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/node/BoundedQueue.hpp"
#include "coro_test_util.hpp"

using testutil::pumpFiberUntil;
using transport::BoundedQueue;
using transport::CancellationSource;
using transport::CapturingTraceSink;
using transport::DropReason;
using transport::DropReasonName;
using transport::OperationOptions;
using transport::Result;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 每个元素计 1 字节:令字节上界与事件上界解耦,便于分别驱动两条 tail-drop 路径。
BoundedQueue<int>::ByteSizeOf UnitBytes() {
  return [](const int&) { return std::size_t{1}; };
}

}  // namespace

// 事件数达上限 → Push tail-drop 返 kResourceExhausted,DroppedCount +1,已入队不受影响。
TEST(BoundedQueue, EventLimitTailDrops) {
  // 字节上界放到最大(不绑),仅事件上界=3 生效。
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/3,
                          BoundedQueue<int>::kMaxBytes);

  EXPECT_TRUE(queue.Push(10));
  EXPECT_TRUE(queue.Push(20));
  EXPECT_TRUE(queue.Push(30));
  EXPECT_EQ(queue.Size(), 3u);

  auto dropped = queue.Push(40);  // 满 → tail-drop 正到达的 40。
  ASSERT_FALSE(dropped);
  EXPECT_EQ(dropped.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(queue.DroppedCount(), 1u);
  EXPECT_EQ(queue.Size(), 3u);  // 已入队不受影响。

  // 已入队的仍是最初三个,FIFO。
  auto a = queue.Pop();
  auto b = queue.Pop();
  auto c = queue.Pop();
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  EXPECT_EQ(a.value(), 10);
  EXPECT_EQ(b.value(), 20);
  EXPECT_EQ(c.value(), 30);
}

// 字节数达上限(事件数未满)→ 同样 tail-drop。
TEST(BoundedQueue, ByteLimitTailDrops) {
  // 字节上界须 >= kMinBytes(64 KiB,钳制下界),故用大计量:每元素 32 KiB,
  // 字节上界 = 64 KiB(= kMinBytes)→ 容 2 个,第 3 个越界 tail-drop。
  constexpr std::size_t kPer = 32u * 1024u;
  BoundedQueue<int> queue(
      [](const int&) { return kPer; },
      /*max_events=*/BoundedQueue<int>::kMaxEvents,  // 事件上界不绑。
      /*max_bytes=*/BoundedQueue<int>::kMinBytes /* 64 KiB */);

  EXPECT_TRUE(queue.Push(1));  // 32 KiB
  EXPECT_TRUE(queue.Push(2));  // 64 KiB(达上界,仍容纳)
  EXPECT_EQ(queue.ByteSize(), 64u * 1024u);
  EXPECT_EQ(queue.Size(), 2u);

  auto dropped = queue.Push(3);  // 96 KiB 越界 → tail-drop。
  ASSERT_FALSE(dropped);
  EXPECT_EQ(dropped.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(queue.DroppedCount(), 1u);
  EXPECT_EQ(queue.Size(), 2u);
  EXPECT_EQ(queue.ByteSize(), 64u * 1024u);

  // 出队扣字节。
  auto a = queue.Pop();
  ASSERT_TRUE(a);
  EXPECT_EQ(a.value(), 1);
  EXPECT_EQ(queue.ByteSize(), 32u * 1024u);
}

// FIFO:出队顺序 = 入队顺序;容量内元素最终都可取出。
TEST(BoundedQueue, FifoOrderPreserved) {
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/8,
                          BoundedQueue<int>::kMaxBytes);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(queue.Push(i));
  }
  std::vector<int> out;
  for (int i = 0; i < 5; ++i) {
    auto v = queue.Pop();
    ASSERT_TRUE(v);
    out.push_back(v.value());
  }
  EXPECT_EQ(out, (std::vector<int>{0, 1, 2, 3, 4}));
  EXPECT_EQ(queue.Size(), 0u);
  EXPECT_EQ(queue.ByteSize(), 0u);
}

// 空队列消费者 await → Push 唤醒(fiber 调度器内)。
TEST(BoundedQueue, EmptyPopAwaitsThenPushWakes) {
  BoundedQueue<int> queue(UnitBytes());

  Result<int> observed{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto consumer = Coro::makeTask([&] {
    entered.resolve();
    observed = queue.Pop();  // 空 → 在此挂起等待。
  });
  ASSERT_TRUE(entered.await());

  // 消费者应已挂起在 Pop(尚未产出结果)。让出若干轮仍无结果为证。
  EXPECT_TRUE(pumpFiberUntil([&] { return queue.Size() == 0; }, 5));

  EXPECT_TRUE(queue.Push(77));  // 唤醒在途消费者。
  EXPECT_TRUE(consumer.get());

  ASSERT_TRUE(observed);
  EXPECT_EQ(observed.value(), 77);
  EXPECT_EQ(queue.Size(), 0u);
}

// Close:唤醒在途消费者(返 kClosed);剩余元素可 Drain 枚举供归因。
TEST(BoundedQueue, CloseWakesConsumerAndDrainEnumeratesRemainder) {
  BoundedQueue<int> queue(UnitBytes());

  Result<int> observed{42};
  Coro::Awaitable<void> entered;
  auto consumer = Coro::makeTask([&] {
    entered.resolve();
    observed = queue.Pop();  // 空 → 挂起。
  });
  ASSERT_TRUE(entered.await());

  queue.Close();  // 唤醒在途消费者。
  EXPECT_TRUE(consumer.get());
  ASSERT_FALSE(observed);
  EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kClosed));

  // Close 后 Push 拒绝。
  auto rejected = queue.Push(1);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), make_error_code(TransportErrc::kClosed));
}

// Close 不清空已入队元素:Drain 按 FIFO 枚举残留供 close_drop 归因。
TEST(BoundedQueue, CloseRetainsItemsForDrain) {
  BoundedQueue<int> queue(UnitBytes());
  EXPECT_TRUE(queue.Push(1));
  EXPECT_TRUE(queue.Push(2));
  EXPECT_TRUE(queue.Push(3));

  queue.Close();
  EXPECT_EQ(queue.Size(), 3u);  // 残留仍在。

  // Close 后 Pop 直接返 kClosed(即使有残留,消费循环应终止,残留交 node 归因)。
  auto after_close = queue.Pop();
  ASSERT_FALSE(after_close);
  EXPECT_EQ(after_close.error(), make_error_code(TransportErrc::kClosed));

  auto remainder = queue.Drain();
  EXPECT_EQ(remainder, (std::vector<int>{1, 2, 3}));
  EXPECT_EQ(queue.Size(), 0u);
  EXPECT_EQ(queue.ByteSize(), 0u);
}

// 协议无关:用非 Message 的自定义结构做 T,证明队列不依赖任何协议类型。
TEST(BoundedQueue, ProtocolAgnosticCustomPayload) {
  struct Event {
    std::string name;
    std::vector<std::uint8_t> blob;
  };
  // 字节计量取 blob 大小——队列不读该字段,只经回调不透明获取。
  BoundedQueue<Event> queue([](const Event& e) { return e.blob.size(); },
                            /*max_events=*/4, /*max_bytes=*/16);

  EXPECT_TRUE(queue.Push(Event{"a", {1, 2, 3}}));   // 3 字节
  EXPECT_TRUE(queue.Push(Event{"b", {4, 5, 6, 7}}));  // 4 字节
  EXPECT_EQ(queue.ByteSize(), 7u);

  auto first = queue.Pop();
  ASSERT_TRUE(first);
  EXPECT_EQ(first.value().name, "a");
  EXPECT_EQ(first.value().blob.size(), 3u);
  EXPECT_EQ(queue.ByteSize(), 4u);  // 出队按存档字节扣减。
}

// 取消令牌触发 → 挂起的 Pop 返 kCancelled。
TEST(BoundedQueue, CancellationUnblocksPop) {
  BoundedQueue<int> queue(UnitBytes());
  CancellationSource source;

  OperationOptions options;
  options.cancellation = source.token();
  Result<int> observed{7};
  Coro::Awaitable<void> entered;
  auto consumer = Coro::makeTask([&] {
    entered.resolve();
    observed = queue.Pop(options);
  });
  ASSERT_TRUE(entered.await());

  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(consumer.get());
  ASSERT_FALSE(observed);
  EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kCancelled));
}

// deadline 到 → 挂起的 Pop 返 kTimeout。
TEST(BoundedQueue, DeadlineTimesOutPop) {
  BoundedQueue<int> queue(UnitBytes());

  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + std::chrono::milliseconds(20);
  Result<int> observed{7};
  Coro::Awaitable<void> entered;
  auto consumer = Coro::makeTask([&] {
    entered.resolve();
    observed = queue.Pop(options);
  });
  ASSERT_TRUE(entered.await());

  EXPECT_TRUE(pumpFiberUntil([&] { return !observed; }));
  EXPECT_TRUE(consumer.get());
  ASSERT_FALSE(observed);
  EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kTimeout));
}

// 配置钳制:越界的 max_events/max_bytes 钳制到合法区间(取上界后仍能入队多于默认极小值)。
TEST(BoundedQueue, ConfigClampedToRange) {
  // max_events=0 → 钳制到 kMinEvents(1);字节上界给超大 → 钳制到 kMaxBytes。
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/0,
                          /*max_bytes=*/BoundedQueue<int>::kMaxBytes + 1);
  EXPECT_TRUE(queue.Push(1));
  auto dropped = queue.Push(2);  // 事件上界钳到 1 → 第二个丢弃。
  ASSERT_FALSE(dropped);
  EXPECT_EQ(dropped.error(), make_error_code(TransportErrc::kResourceExhausted));
}

// -----------------------------------------------------------------------------
// P5-3:tail-drop 归因(构造注入 DropReason + 可选 ITraceSink)。DroppedCount() 语义/
// 签名不变(见上文既有用例);这里只加验证归因原语接线正确的新用例。
// -----------------------------------------------------------------------------

// 未传 drop_reason/sink(沿用既有 3 参构造)→ 默认 kBusinessQueueOverflow,行为/计数
// 与改动前完全一致(RT_TRACE_002:未配 sink 不改变控制流/计数)。
TEST(BoundedQueue, DefaultDropReasonWithoutSinkBehavesUnchanged) {
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/1);
  EXPECT_TRUE(queue.Push(1));
  auto dropped = queue.Push(2);
  ASSERT_FALSE(dropped);
  EXPECT_EQ(dropped.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(queue.DroppedCount(), 1u);
}

// 构造注入 drop_reason=kDdsHandoffOverflow + sink → tail-drop 时 DroppedCount 仍恰好
// +1(getter 契约不变),且 sink 收到一条 category="drop"、message=该原因短名的 TraceEvent。
TEST(BoundedQueue, TailDropWithSinkEmitsRecordDropForInjectedReason) {
  CapturingTraceSink sink;
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/1,
                          BoundedQueue<int>::kMaxBytes,
                          DropReason::kDdsHandoffOverflow, &sink);

  EXPECT_TRUE(queue.Push(1));
  auto dropped = queue.Push(2);  // 满 → tail-drop。
  ASSERT_FALSE(dropped);
  EXPECT_EQ(queue.DroppedCount(), 1u);  // getter 行为不变。

  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kDdsHandoffOverflow));

  // 再触发一次:计数与 Trace 事件同步递增。
  auto dropped2 = queue.Push(3);
  ASSERT_FALSE(dropped2);
  EXPECT_EQ(queue.DroppedCount(), 2u);
  EXPECT_EQ(sink.Records().size(), 2u);
}

// 注入 sink 但从未溢出:不产生任何 drop 事件(RecordDrop 只在 tail-drop 分支调用)。
TEST(BoundedQueue, SinkConfiguredButNoOverflowEmitsNothing) {
  CapturingTraceSink sink;
  BoundedQueue<int> queue(UnitBytes(), /*max_events=*/4, BoundedQueue<int>::kMaxBytes,
                          DropReason::kBusinessQueueOverflow, &sink);
  EXPECT_TRUE(queue.Push(1));
  EXPECT_TRUE(queue.Push(2));
  EXPECT_EQ(queue.DroppedCount(), 0u);
  EXPECT_TRUE(sink.Records().empty());
}
