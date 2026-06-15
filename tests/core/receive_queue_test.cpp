#include "transport/core/ReceiveQueue.hpp"

#include <chrono>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

using transport::Message;
using transport::ReceiveQueue;
using transport::Result;

namespace {

Result<Message> MakeMsg(uint8_t tag) {
  Message m;
  m.payload = {tag};
  return Result<Message>::Success(std::move(m));
}

}  // namespace

TEST(ReceiveQueue, SyncPushThenReceive) {
  ReceiveQueue q;
  q.Push(MakeMsg(7));
  auto r = q.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.payload.size(), 1u);
  EXPECT_EQ(r.value.payload[0], 7);
}

TEST(ReceiveQueue, SyncReceiveTimesOutOnEmpty) {
  ReceiveQueue q;
  auto r = q.Receive(20);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(ReceiveQueue, CallbackInvokedOnPush) {
  ReceiveQueue q;
  std::vector<uint8_t> got;
  auto st = q.SetCallback([&](Result<Message> m) {
    if (m) got.push_back(m.value.payload[0]);
  });
  ASSERT_TRUE(static_cast<bool>(st));
  q.Push(MakeMsg(1));
  q.Push(MakeMsg(2));
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0], 1);
  EXPECT_EQ(got[1], 2);
}

TEST(ReceiveQueue, CallbackDrainsBacklog) {
  ReceiveQueue q;
  q.Push(MakeMsg(9));
  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(q.SetCallback([&](Result<Message> m) {
    if (m) got.push_back(m.value.payload[0]);
  })));
  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0], 9);
}

TEST(ReceiveQueue, FutureReadyWhenMessageAlreadyQueued) {
  ReceiveQueue q;
  q.Push(MakeMsg(5));
  auto fut = q.AsyncReceive();
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload[0], 5);
}

TEST(ReceiveQueue, FutureFulfilledOnLaterPush) {
  ReceiveQueue q;
  auto fut = q.AsyncReceive();
  q.Push(MakeMsg(8));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload[0], 8);
}

TEST(ReceiveQueue, FutureFifoOrder) {
  ReceiveQueue q;
  auto f1 = q.AsyncReceive();
  auto f2 = q.AsyncReceive();
  q.Push(MakeMsg(1));
  q.Push(MakeMsg(2));
  EXPECT_EQ(f1.get().value.payload[0], 1);
  EXPECT_EQ(f2.get().value.payload[0], 2);
}

TEST(ReceiveQueue, ModeExclusivitySyncThenCallbackFails) {
  ReceiveQueue q;
  (void)q.Receive(1);
  auto st = q.SetCallback([](Result<Message>) {});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(ReceiveQueue, ModeExclusivityCallbackThenAsyncFails) {
  ReceiveQueue q;
  ASSERT_TRUE(static_cast<bool>(q.SetCallback([](Result<Message>) {})));
  auto fut = q.AsyncReceive();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u);
}

TEST(ReceiveQueue, CloseUnblocksSyncReceive) {
  ReceiveQueue q;
  std::thread closer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.Close();
  });
  auto r = q.Receive(0);
  closer.join();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}

TEST(ReceiveQueue, CloseFulfillsPendingFutures) {
  ReceiveQueue q;
  auto fut = q.AsyncReceive();
  q.Close();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}
