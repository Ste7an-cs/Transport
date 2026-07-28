// -----------------------------------------------------------------------------
// pending_table_test.cpp — PendingTable<Key, T> 契约单测(RT_IN_INTERFACE_004)
//
// 覆盖:唯一登记 / 恰好一次完成 / 超时 / 取消 / FailAll 收敛 / closed latch。
// 全部在 fiber 调度器内跑(coro_test_main 范式):请求 fiber 用 makeTask 起独立
// fiber 在 Handle::Wait 挂起,主(测试)fiber 用 pumpFiberUntil 驱动时钟/让出。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/Message.hpp"
#include "transport/PendingTable.hpp"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::CancellationSource;
using transport::Message;
using transport::OperationOptions;
using transport::PendingTable;
using transport::Result;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

Message MakeMessage(std::uint8_t tag) {
  Message m;
  m.payload = {tag};
  return m;
}

}  // namespace

// 唯一登记:同一 key 连续 Register 两次 → 第二次 kInvalidState。
TEST(PendingTable, DuplicateRegisterRejectedWithInvalidState) {
  PendingTable<std::uint32_t, Message> table;

  auto first = table.Register(1);
  ASSERT_TRUE(first);
  EXPECT_EQ(table.Size(), 1u);

  auto second = table.Register(1);
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(table.Size(), 1u);
}

// 恰好一次完成:Resolve 命中 → Wait 返值且 entry erase;再 Resolve 同 key → false。
TEST(PendingTable, ResolveDeliversValueThenLateResolveDropped) {
  PendingTable<std::uint32_t, Message> table;
  auto handle = table.Register(7);
  ASSERT_TRUE(handle);

  Result<Message> observed{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto waiter = Coro::makeTask([&] {
    entered.resolve();
    observed = handle.value().Wait();
  });
  ASSERT_TRUE(entered.await());

  EXPECT_TRUE(table.Resolve(7, MakeMessage(42)));
  EXPECT_TRUE(waiter.get());

  ASSERT_TRUE(observed);
  ASSERT_EQ(observed.value().payload.size(), 1u);
  EXPECT_EQ(observed.value().payload[0], 42);
  EXPECT_EQ(table.Size(), 0u);

  // 迟到应答:entry 已终结且被摘除 → false。
  EXPECT_FALSE(table.Resolve(7, MakeMessage(99)));
}

// 无匹配 key 的 Resolve 直接返 false。
TEST(PendingTable, ResolveUnknownKeyReturnsFalse) {
  PendingTable<std::uint32_t, Message> table;
  EXPECT_FALSE(table.Resolve(123, MakeMessage(1)));
}

// 超时:Wait 到 deadline → kTimeout 且 entry 终结;随后 Resolve → false。
TEST(PendingTable, DeadlineTimesOutAndTerminatesEntry) {
  PendingTable<std::uint32_t, Message> table;
  auto handle = table.Register(3);
  ASSERT_TRUE(handle);

  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 20ms;
  Result<Message> observed{Message{}};
  Coro::Awaitable<void> entered;
  auto waiter = Coro::makeTask([&] {
    entered.resolve();
    observed = handle.value().Wait(options);
  });
  ASSERT_TRUE(entered.await());

  // 推进 fiber 时钟直到请求 fiber 因超时返回(Size 归零 = entry 已摘除)。
  EXPECT_TRUE(pumpFiberUntil([&] { return table.Size() == 0; }));
  EXPECT_TRUE(waiter.get());

  ASSERT_FALSE(observed);
  EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kTimeout));
  // entry 已终结:迟到 Resolve 落空。
  EXPECT_FALSE(table.Resolve(3, MakeMessage(1)));
}

// 取消:cancellation token 触发 → Wait 返 kCancelled 且 entry 终结;随后 Resolve → false。
TEST(PendingTable, CancellationTerminatesEntry) {
  PendingTable<std::uint32_t, Message> table;
  CancellationSource source;
  auto handle = table.Register(5);
  ASSERT_TRUE(handle);

  OperationOptions options;
  options.cancellation = source.token();
  Result<Message> observed{Message{}};
  Coro::Awaitable<void> entered;
  auto waiter = Coro::makeTask([&] {
    entered.resolve();
    observed = handle.value().Wait(options);
  });
  ASSERT_TRUE(entered.await());

  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(waiter.get());

  ASSERT_FALSE(observed);
  EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kCancelled));
  EXPECT_EQ(table.Size(), 0u);
  EXPECT_FALSE(table.Resolve(5, MakeMessage(1)));
}

// FailAll(kConnection):全部在途 Wait 恰好一次返 kConnection。
TEST(PendingTable, FailAllConvergesAllInFlightWaiters) {
  PendingTable<std::uint32_t, Message> table;
  auto handle_a = table.Register(10);
  auto handle_b = table.Register(11);
  ASSERT_TRUE(handle_a);
  ASSERT_TRUE(handle_b);

  Result<Message> observed_a{Message{}};
  Result<Message> observed_b{Message{}};
  Coro::Awaitable<void> entered;
  auto waiter_a = Coro::makeTask([&] {
    entered.resolve();
    observed_a = handle_a.value().Wait();
  });
  auto waiter_b = Coro::makeTask([&] {
    entered.resolve();
    observed_b = handle_b.value().Wait();
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(entered.await());
  EXPECT_EQ(table.Size(), 2u);

  table.FailAll(make_error_code(TransportErrc::kConnection));

  EXPECT_TRUE(waiter_a.get());
  EXPECT_TRUE(waiter_b.get());
  ASSERT_FALSE(observed_a);
  ASSERT_FALSE(observed_b);
  EXPECT_EQ(observed_a.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(observed_b.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(table.Size(), 0u);
}

// closed latch:FailAll(kClosed) 后 Register → kClosed(幽灵在途堵截,场景 C)。
TEST(PendingTable, FailAllLatchesClosedForSubsequentRegister) {
  PendingTable<std::uint32_t, Message> table;
  table.FailAll(make_error_code(TransportErrc::kClosed));

  auto rejected = table.Register(1);
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), make_error_code(TransportErrc::kClosed));
}

// 四方竞争:读循环 fiber 的 Resolve 先胜,超时/析构不改写结果(恰好一次)。
TEST(PendingTable, ResolveWinsOverPendingDeadlineExactlyOnce) {
  PendingTable<std::uint32_t, Message> table;
  auto handle = table.Register(20);
  ASSERT_TRUE(handle);

  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 2s;  // 远期,不会先触发
  Result<Message> observed{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto waiter = Coro::makeTask([&] {
    entered.resolve();
    observed = handle.value().Wait(options);
  });
  ASSERT_TRUE(entered.await());

  // 独立"读循环"fiber 交付应答。
  auto reader = Coro::makeTask([&] { EXPECT_TRUE(table.Resolve(20, MakeMessage(7))); });
  EXPECT_TRUE(reader.get());
  EXPECT_TRUE(waiter.get());

  ASSERT_TRUE(observed);
  EXPECT_EQ(observed.value().payload[0], 7);
  EXPECT_EQ(table.Size(), 0u);
  // 第二次 Resolve 落空(恰好一次)。
  EXPECT_FALSE(table.Resolve(20, MakeMessage(8)));
}

// max_pending(协议无关纯计数上限):达上限 Register → kResourceExhausted;终结释放后
// 名额可复用。默认 0 = 无限(既有测试已覆盖不设限)。
TEST(PendingTable, MaxPendingRejectsRegisterBeyondCapacity) {
  PendingTable<std::uint32_t, Message> table(2);  // 上限 2。

  auto a = table.Register(1);
  auto b = table.Register(2);
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_EQ(table.Size(), 2u);

  // 第 3 个不同 key:达上限 → kResourceExhausted(不登记)。
  auto c = table.Register(3);
  ASSERT_FALSE(c);
  EXPECT_EQ(c.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(table.Size(), 2u);

  // 上限判定先于键语义:已在途 key 满员时也报上限(纯计数,不碰键语义)。
  auto dup = table.Register(1);
  ASSERT_FALSE(dup);
  EXPECT_EQ(dup.error(), make_error_code(TransportErrc::kResourceExhausted));

  // 释放一个在途(Handle 析构兜底摘除)→ 名额回收,可再 Register。
  { auto evict = std::move(a); }
  EXPECT_EQ(table.Size(), 1u);
  auto d = table.Register(4);
  ASSERT_TRUE(d);
  EXPECT_EQ(table.Size(), 2u);
}

// Handle 析构兜底:未 Wait 直接析构 → entry 从表摘除(取消纪律)。
TEST(PendingTable, HandleDestructorEvictsUnfinishedEntry) {
  PendingTable<std::uint32_t, Message> table;
  {
    auto handle = table.Register(30);
    ASSERT_TRUE(handle);
    EXPECT_EQ(table.Size(), 1u);
  }
  EXPECT_EQ(table.Size(), 0u);
  EXPECT_FALSE(table.Resolve(30, MakeMessage(1)));
}
