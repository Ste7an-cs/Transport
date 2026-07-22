#include <atomic>
#include <gtest/gtest.h>

#include "task/fibertask.h"
#include "transport/coro/Cancellation.hpp"
#include "transport/coro/Error.hpp"

using transport::coro::CancellationSource;
using transport::coro::Status;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

TEST(CoroCancellation, CancelIsIdempotentAndRunsNotificationOnce) {
  CancellationSource source;
  std::atomic_int calls{0};
  auto registration = source.token().Register([&] { ++calls; });

  EXPECT_FALSE(source.token().IsCancellationRequested());
  EXPECT_TRUE(source.Cancel());
  EXPECT_FALSE(source.Cancel());
  EXPECT_TRUE(source.token().IsCancellationRequested());
  EXPECT_EQ(calls.load(), 1);
  EXPECT_TRUE(static_cast<bool>(registration));
}

TEST(CoroCancellation, ResetPreventsNotification) {
  CancellationSource source;
  int calls = 0;
  auto registration = source.token().Register([&] { ++calls; });
  registration.Reset();
  source.Cancel();
  EXPECT_EQ(calls, 0);
}

TEST(CoroCancellation, WaitWakesAllFibers) {
  CancellationSource source;
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};

  auto one = Coro::makeTask([&] { first = source.token().Wait(); });
  auto two = Coro::makeTask([&] { second = source.token().Wait(); });
  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
}

TEST(CoroCancellation, EmptyTokenWaitReturnsInvalidState) {
  transport::coro::CancellationToken token;
  const Status result = token.Wait();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kInvalidState));
}
