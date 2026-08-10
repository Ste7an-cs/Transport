#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"
#include "task/fibertask.h"

using namespace std::chrono_literals;
using transport::Endpoint;
using transport::CancellationSource;
using transport::Datagram;
using transport::LifecycleState;
using transport::OperationOptions;
using transport::Result;
using transport::SendUnit;
using transport::Status;
using transport::TransportErrc;
using transport::make_error_code;
using testutil::FakeCoroTransport;

TEST(CoroFakeTransport, StartIsIdempotentAndClosedCannotRestart) {
  FakeCoroTransport fake;
  EXPECT_TRUE(fake.Start());
  EXPECT_TRUE(fake.Start());
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(fake.WaitClosed());
  const auto restarted = fake.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
}

TEST(CoroFakeTransport, InjectCompletesReadWithSourceMetadata) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> received{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    received = fake.Read();
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveRead());
  fake.Inject(Datagram{{1, 2}, Endpoint::Net("127.0.0.1", 7001)});
  ASSERT_TRUE(reader.get());
  ASSERT_TRUE(received);
  EXPECT_EQ(received.value().bytes, (std::vector<std::uint8_t>{1, 2}));
  EXPECT_EQ(received.value().source.host, "127.0.0.1");
  EXPECT_EQ(received.value().source.port, 7001);
}

TEST(CoroFakeTransport, QueuedInjectionAndErrorAreConsumedInOrder) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.Inject(Datagram{{3}, Endpoint::Topic("queued")});
  fake.InjectError(make_error_code(TransportErrc::kConnection));

  const auto first = fake.Read();
  ASSERT_TRUE(first);
  EXPECT_EQ(first.value().bytes, (std::vector<std::uint8_t>{3}));
  EXPECT_EQ(first.value().source.topic, "queued");
  EXPECT_FALSE(fake.ActiveRead());

  OperationOptions no_block;
  no_block.deadline = OperationOptions::Clock::now() - 1ms;
  const auto second = fake.Read(no_block);
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_FALSE(fake.ActiveRead());
}

TEST(CoroFakeTransport, OnlyFirstInjectionClaimsAnActiveRead) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> first{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    first = fake.Read();
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveRead());

  fake.Inject(Datagram{{1}, Endpoint::Default()});
  fake.Inject(Datagram{{2}, Endpoint::Default()});

  ASSERT_TRUE(reader.get());
  ASSERT_TRUE(first);
  EXPECT_EQ(first.value().bytes, (std::vector<std::uint8_t>{1}));
  OperationOptions no_block;
  no_block.deadline = OperationOptions::Clock::now() - 1ms;
  const auto second = fake.Read(no_block);
  ASSERT_TRUE(second);
  EXPECT_EQ(second.value().bytes, (std::vector<std::uint8_t>{2}));
}

TEST(CoroFakeTransport, InjectionAfterCancellationIsPreservedForNextRead) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  CancellationSource source;
  OperationOptions options;
  options.cancellation = source.token();
  Result<Datagram> cancelled{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    cancelled = fake.Read(options);
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveRead());

  EXPECT_TRUE(source.Cancel());
  fake.Inject(Datagram{{7}, Endpoint::Default()});

  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error(), make_error_code(TransportErrc::kCancelled));
  OperationOptions no_block;
  no_block.deadline = OperationOptions::Clock::now() - 1ms;
  const auto next = fake.Read(no_block);
  ASSERT_TRUE(next);
  EXPECT_EQ(next.value().bytes, (std::vector<std::uint8_t>{7}));
}

TEST(CoroFakeTransport, InjectionWinsAfterAwaitTimeoutBeforeStateArbitration) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.SetBeforeTimeoutArbitration([&] {
    fake.Inject(Datagram{{8}, Endpoint::Topic("timeout-winner")});
  });
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() - 1ms;

  const auto result = fake.Read(options);

  ASSERT_TRUE(result);
  EXPECT_EQ(result.value().bytes, (std::vector<std::uint8_t>{8}));
  EXPECT_EQ(result.value().source.topic, "timeout-winner");
}

TEST(CoroFakeTransport, InjectedErrorWinsAfterAwaitTimeoutBeforeArbitration) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.SetBeforeTimeoutArbitration([&] {
    fake.InjectError(make_error_code(TransportErrc::kConnection));
  });
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() - 1ms;

  const auto result = fake.Read(options);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kConnection));
}

TEST(CoroFakeTransport, CloseWinsAfterAwaitTimeoutBeforeStateArbitration) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.SetBeforeTimeoutArbitration([&] { EXPECT_TRUE(fake.RequestClose()); });
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() - 1ms;

  const auto result = fake.Read(options);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(fake.WaitClosed());
}

TEST(CoroFakeTransport, CancellationWinsAfterAwaitTimeoutBeforeArbitration) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  CancellationSource source;
  fake.SetBeforeTimeoutArbitration([&] { EXPECT_TRUE(source.Cancel()); });
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() - 1ms;
  options.cancellation = source.token();

  const auto result = fake.Read(options);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kCancelled));
}

TEST(CoroFakeTransport, ConcurrentReadReturnsInvalidState) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> first{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    first = fake.Read();
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveRead());
  const auto second = fake.Read();
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(first);
  EXPECT_EQ(first.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_FALSE(fake.ActiveRead());
}

TEST(CoroFakeTransport, ReadDeadlineAndCancellationReleaseReadSlot) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  OperationOptions timeout;
  timeout.deadline = OperationOptions::Clock::now() - 1ms;
  const auto timed_out = fake.Read(timeout);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_FALSE(fake.ActiveRead());

  CancellationSource source;
  OperationOptions cancelled_options;
  cancelled_options.cancellation = source.token();
  Result<Datagram> cancelled{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    cancelled = fake.Read(cancelled_options);
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveRead());
  source.Cancel();
  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error(), make_error_code(TransportErrc::kCancelled));
  EXPECT_FALSE(fake.ActiveRead());
}

TEST(CoroFakeTransport, ConcurrentPhysicalWriteSerializes) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto writer_a = Coro::makeTask([&] {
    entered.resolve();
    first = fake.Write(SendUnit{{1}, Endpoint::Default()});
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveWrite());
  // 第二个并发写在第一个持有写槽时进入 → 排队(不拒绝),等待者深度升至 2。
  auto writer_b = Coro::makeTask(
      [&] { second = fake.Write(SendUnit{{2}, Endpoint::Default()}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return fake.SendWaiterDepth() == 2; }));
  fake.ReleaseWrite();
  EXPECT_TRUE(writer_a.get());
  EXPECT_TRUE(writer_b.get());
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
  EXPECT_FALSE(fake.ActiveWrite());
  // 先取得写槽者先上线;两帧各自完整、串行不交错。
  ASSERT_EQ(fake.sent().size(), 2U);
  EXPECT_EQ(fake.sent()[0].bytes, (std::vector<std::uint8_t>{1}));
  EXPECT_EQ(fake.sent()[1].bytes, (std::vector<std::uint8_t>{2}));
}

TEST(CoroFakeTransport, CompleteWriteFailureReleasesSlotWithoutClosing) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.FailNextWrite(make_error_code(TransportErrc::kIo), false);

  const auto failed = fake.Write(SendUnit{{1}, Endpoint::Default()});
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), make_error_code(TransportErrc::kIo));
  EXPECT_FALSE(fake.ActiveWrite());
  EXPECT_EQ(fake.state(), LifecycleState::kRunning);

  EXPECT_TRUE(fake.Write(SendUnit{{2}, Endpoint::Default()}));
  ASSERT_EQ(fake.sent().size(), 1U);
  EXPECT_EQ(fake.sent()[0].bytes, (std::vector<std::uint8_t>{2}));
}

TEST(CoroFakeTransport, PartialWriteFailureClosesTransport) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.FailNextWrite(make_error_code(TransportErrc::kIo), true);
  const auto written = fake.Write(SendUnit{{1}, Endpoint::Default()});
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kIo));
  EXPECT_FALSE(fake.ActiveWrite());
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_EQ(fake.state(), LifecycleState::kClosed);
  EXPECT_TRUE(fake.sent().empty());
}

TEST(CoroFakeTransport, CloseWakesReadAndAllClosedWaiters) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> read{make_error_code(TransportErrc::kInternal)};
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  Coro::Awaitable<void> read_entered;
  auto reader = Coro::makeTask([&] {
    read_entered.resolve();
    read = fake.Read();
  });
  auto one = Coro::makeTask([&] {
    entered.resolve();
    first = fake.WaitClosed();
  });
  auto two = Coro::makeTask([&] {
    entered.resolve();
    second = fake.WaitClosed();
  });
  ASSERT_TRUE(read_entered.await());
  ASSERT_TRUE(fake.ActiveRead());
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(entered.await());
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(reader.get());
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  ASSERT_FALSE(read);
  EXPECT_EQ(read.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
}

TEST(CoroFakeTransport, RequestCloseWakesHeldWriteAndCompletesAfterItExits) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();
  Status written{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto writer = Coro::makeTask([&] {
    entered.resolve();
    written = fake.Write(SendUnit{{1}, Endpoint::Default()});
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveWrite());

  EXPECT_TRUE(fake.RequestClose());
  EXPECT_EQ(fake.state(), LifecycleState::kClosing);
  EXPECT_TRUE(writer.get());
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_FALSE(fake.ActiveWrite());
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_EQ(fake.state(), LifecycleState::kClosed);
}

// 由 WaiterTimeoutAndCancellationAreLocal 拆出:取消部分随 WaitClosed 的取消令牌
// 支持一并移除(ADR-0006 D3),超时局部性在广播完成量上依然成立,故原样保留。
TEST(CoroFakeTransport, WaiterTimeoutIsLocal) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  OperationOptions timeout_options;
  timeout_options.deadline = OperationOptions::Clock::now() - 1ms;
  Status survivor{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto survivor_waiter = Coro::makeTask([&] {
    entered.resolve();
    survivor = fake.WaitClosed();
  });
  ASSERT_TRUE(entered.await());

  const auto timed_out = fake.WaitClosed(timeout_options);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));

  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(survivor_waiter.get());
  EXPECT_TRUE(survivor);
}

TEST(CoroFakeTransport, IllegalOperationsReflectEveryLifecycleState) {
  FakeCoroTransport fake;
  auto created_read = fake.Read();
  auto created_write = fake.Write(SendUnit{{1}, Endpoint::Default()});
  auto created_wait = fake.WaitClosed();
  ASSERT_FALSE(created_read);
  ASSERT_FALSE(created_write);
  ASSERT_FALSE(created_wait);
  EXPECT_EQ(created_read.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(created_write.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(created_wait.error(), make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();
  Status held_write{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto writer = Coro::makeTask([&] {
    entered.resolve();
    held_write = fake.Write(SendUnit{{2}, Endpoint::Default()});
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(fake.ActiveWrite());
  ASSERT_TRUE(fake.RequestClose());
  EXPECT_EQ(fake.state(), LifecycleState::kClosing);

  auto closing_read = fake.Read();
  auto closing_write = fake.Write(SendUnit{{3}, Endpoint::Default()});
  auto closing_start = fake.Start();
  ASSERT_FALSE(closing_read);
  ASSERT_FALSE(closing_write);
  ASSERT_FALSE(closing_start);
  EXPECT_EQ(closing_read.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(closing_write.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(closing_start.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_TRUE(writer.get());
  ASSERT_FALSE(held_write);
  EXPECT_EQ(held_write.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(fake.WaitClosed());

  auto closed_read = fake.Read();
  auto closed_write = fake.Write(SendUnit{{4}, Endpoint::Default()});
  auto closed_start = fake.Start();
  ASSERT_FALSE(closed_read);
  ASSERT_FALSE(closed_write);
  ASSERT_FALSE(closed_start);
  EXPECT_EQ(closed_read.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(closed_write.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(closed_start.error(), make_error_code(TransportErrc::kInvalidState));
}

TEST(CoroFakeTransport, RequestCloseAndWaitClosedAreIdempotent) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_TRUE(fake.RequestClose());
}

TEST(CoroFakeTransport, WriteSuccessUpdatesLastSendTimeAndReadSuccessUpdatesLastReceiveTime) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  EXPECT_FALSE(fake.LastSendTime().has_value());
  EXPECT_FALSE(fake.LastReceiveTime().has_value());
  EXPECT_FALSE(fake.LastError());

  const auto before_write = OperationOptions::Clock::now();
  ASSERT_TRUE(fake.Write(SendUnit{{1, 2}, Endpoint::Default()}));
  const auto after_write = OperationOptions::Clock::now();
  ASSERT_TRUE(fake.LastSendTime().has_value());
  EXPECT_GE(*fake.LastSendTime(), before_write);
  EXPECT_LE(*fake.LastSendTime(), after_write);
  EXPECT_FALSE(fake.LastReceiveTime().has_value());  // 未收前仍空。

  const auto before_read = OperationOptions::Clock::now();
  fake.Inject(Datagram{{3, 4}, Endpoint::Default()});
  const auto received = fake.Read();
  const auto after_read = OperationOptions::Clock::now();
  ASSERT_TRUE(received);
  ASSERT_TRUE(fake.LastReceiveTime().has_value());
  EXPECT_GE(*fake.LastReceiveTime(), before_read);
  EXPECT_LE(*fake.LastReceiveTime(), after_read);
}

TEST(CoroFakeTransport, WriteFailureUpdatesLastErrorWithoutTouchingLastSendTime) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.FailNextWrite(make_error_code(TransportErrc::kIo), false);

  const auto failed = fake.Write(SendUnit{{1}, Endpoint::Default()});
  ASSERT_FALSE(failed);
  EXPECT_EQ(fake.LastError(), make_error_code(TransportErrc::kIo));
  EXPECT_FALSE(fake.LastSendTime().has_value());
}

TEST(CoroFakeTransport, InjectedReadErrorUpdatesLastError) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.InjectError(make_error_code(TransportErrc::kConnection));

  OperationOptions no_block;
  no_block.deadline = OperationOptions::Clock::now() - 1ms;
  const auto result = fake.Read(no_block);
  ASSERT_FALSE(result);
  EXPECT_EQ(fake.LastError(), make_error_code(TransportErrc::kConnection));
  EXPECT_FALSE(fake.LastReceiveTime().has_value());
}

TEST(CoroFakeTransport, DestructionWakesAnActiveReadWithoutUsingObjectStorage) {
  auto fake = std::make_unique<FakeCoroTransport>();
  ASSERT_TRUE(fake->Start());
  auto* raw = fake.get();
  Result<Datagram> read{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    read = raw->Read();
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(raw->ActiveRead());

  fake.reset();
  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(read);
  EXPECT_EQ(read.error(), make_error_code(TransportErrc::kClosed));
}
