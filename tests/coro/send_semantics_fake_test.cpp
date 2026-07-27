// 发送语义契约测试(Fake 替身,确定性)——RT_TRANSPORT_007/008、RT_TRANSPORT_004/004.4。
// 只断言 ITransport 缝上的外部可观察行为:Write 何时完成(经闸门)、sent 的上线
// 顺序、发送等待者深度、部分写失败后的返回码与连接关闭。不断言内部缓冲/锁/循环。
#include <cstdint>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::yield
#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"
#include "task/fibertask.h"

using transport::Endpoint;
using transport::coro::LifecycleState;
using transport::coro::SendUnit;
using transport::coro::Status;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;
using testutil::FakeCoroTransport;

namespace {
SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}
}  // namespace

// RT_TRANSPORT_008:Write 只有在帧字节刷完(闸门释放)后才报告成功。
TEST(CoroSendSemantics, WriteDoesNotCompleteBeforeFlushGateReleases) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();
  Status written{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto writer = Coro::makeTask([&] {
    entered.resolve();
    written = fake.Write(Frame({1, 2, 3}));
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveWrite(); }));

  // 闸门未释放:发送尚未完成——既无上线记录,也仍占用有效写。
  boost::this_fiber::yield();
  EXPECT_TRUE(fake.sent().empty());
  EXPECT_TRUE(fake.ActiveWrite());

  fake.ReleaseWrite();
  EXPECT_TRUE(writer.get());
  EXPECT_TRUE(written);
  EXPECT_FALSE(fake.ActiveWrite());
  ASSERT_EQ(fake.sent().size(), 1U);
  EXPECT_EQ(fake.sent()[0].bytes, (std::vector<std::uint8_t>{1, 2, 3}));
}

// RT_TRANSPORT_007:单个 fiber 先后发起的发送必按其程序顺序上线。
TEST(CoroSendSemantics, SingleFiberWritesLandInProgramOrder) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  auto writer = Coro::makeTask([&] {
    EXPECT_TRUE(fake.Write(Frame({0xA})));
    EXPECT_TRUE(fake.Write(Frame({0xB})));
    EXPECT_TRUE(fake.Write(Frame({0xC})));
  });
  ASSERT_TRUE(writer.get());
  const auto sent = fake.sent();
  ASSERT_EQ(sent.size(), 3U);
  EXPECT_EQ(sent[0].bytes, (std::vector<std::uint8_t>{0xA}));
  EXPECT_EQ(sent[1].bytes, (std::vector<std::uint8_t>{0xB}));
  EXPECT_EQ(sent[2].bytes, (std::vector<std::uint8_t>{0xC}));
}

// RT_TRANSPORT_007/004:跨 fiber 并发发送被串行化,取得一致全序、单帧不交错。
// 后到者排队等待写槽(不拒绝),先到者刷完后依序上线。
TEST(CoroSendSemantics, ConcurrentWritesSerializeIntoConsistentOrderWithoutInterleave) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();

  Status a{make_error_code(TransportErrc::kInternal)};
  Status b{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> a_entered;
  auto first = Coro::makeTask([&] {
    a_entered.resolve();
    a = fake.Write(Frame({0x11, 0x11}));
  });
  ASSERT_TRUE(a_entered.await());
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveWrite(); }));

  // 第二个 fiber 在第一个持有写槽时并发进入 → 排队等待(不拒绝),深度升至 2。
  auto second = Coro::makeTask([&] { b = fake.Write(Frame({0x22, 0x22})); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return fake.SendWaiterDepth() == 2; }));

  fake.ReleaseWrite();  // 放行第一帧,第二帧随后取得写槽并刷完。
  EXPECT_TRUE(first.get());
  EXPECT_TRUE(second.get());
  EXPECT_TRUE(a);
  EXPECT_TRUE(b);

  const auto sent = fake.sent();
  ASSERT_EQ(sent.size(), 2U);
  // 先取得写槽者先上线;两帧各自完整、互不交错。
  EXPECT_EQ(sent[0].bytes, (std::vector<std::uint8_t>{0x11, 0x11}));
  EXPECT_EQ(sent[1].bytes, (std::vector<std::uint8_t>{0x22, 0x22}));
}

// 3.4.4:发送等待者深度反映背压积压——排队 + 在写的 fiber 数,并发时 >1。
TEST(CoroSendSemantics, SendWaiterDepthReflectsQueuedAndInFlightSenders) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  EXPECT_EQ(fake.SendWaiterDepth(), 0U);
  fake.HoldWrites();

  Coro::Awaitable<void> a_entered;
  auto a = Coro::makeTask([&] {
    a_entered.resolve();
    EXPECT_TRUE(fake.Write(Frame({7})));
  });
  ASSERT_TRUE(a_entered.await());
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveWrite(); }));
  EXPECT_EQ(fake.SendWaiterDepth(), 1U);  // 一个在写。

  auto b = Coro::makeTask([&] { EXPECT_TRUE(fake.Write(Frame({8}))); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return fake.SendWaiterDepth() == 2; }));
  EXPECT_EQ(fake.SendWaiterDepth(), 2U);  // 一个在写 + 一个排队 = 积压可见。

  fake.ReleaseWrite();
  EXPECT_TRUE(a.get());
  EXPECT_TRUE(b.get());
  EXPECT_EQ(fake.SendWaiterDepth(), 0U);
}

// RT_TRANSPORT_004.4:注入部分写失败 → 返回 Io、后续写被拒、连接关闭。
TEST(CoroSendSemantics, PartialWriteFailureReturnsIoRejectsFurtherWritesAndCloses) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.FailNextWrite(make_error_code(TransportErrc::kIo), /*partial=*/true);

  const auto failed = fake.Write(Frame({1}));
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), make_error_code(TransportErrc::kIo));
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_EQ(fake.state(), LifecycleState::kClosed);

  // 部分写失败后连接关闭,后续写被拒;残缺帧不上线。
  const auto rejected = fake.Write(Frame({2}));
  ASSERT_FALSE(rejected);
  EXPECT_EQ(rejected.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(fake.sent().empty());
}
