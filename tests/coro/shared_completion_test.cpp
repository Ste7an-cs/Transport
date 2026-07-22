#include <atomic>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <thread>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/coro/SharedCompletion.hpp"

using namespace std::chrono_literals;
using transport::coro::CancellationSource;
using transport::coro::OperationOptions;
using transport::coro::Result;
using transport::coro::SharedCompletion;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

TEST(CoroSharedCompletion, FirstCompletionWinsAndLateWaitSeesIt) {
  SharedCompletion<int> completion;

  EXPECT_TRUE(completion.Complete(Result<int>{7}));
  EXPECT_FALSE(completion.Complete(Result<int>{8}));

  auto result = completion.Wait();
  ASSERT_TRUE(result);
  EXPECT_EQ(result.value(), 7);
}

TEST(CoroSharedCompletion, WakesMultipleWaitersAfterBothEnterWait) {
  SharedCompletion<int> completion;
  Coro::Awaitable<void> entered;
  Result<int> first{make_error_code(TransportErrc::kInternal)};
  Result<int> second{make_error_code(TransportErrc::kInternal)};

  auto one = Coro::makeTask([&] {
    entered.resolve();
    first = completion.Wait();
  });
  auto two = Coro::makeTask([&] {
    entered.resolve();
    second = completion.Wait();
  });

  // resolve() does not yield. Each task therefore proceeds into Wait() and
  // blocks before this test fiber can consume its entered notification.
  EXPECT_TRUE(entered.await());
  EXPECT_TRUE(entered.await());
  EXPECT_TRUE(completion.Complete(Result<int>{9}));

  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_EQ(first.value(), 9);
  EXPECT_EQ(second.value(), 9);
}

TEST(CoroSharedCompletion, CancellationOnlyEndsTheCancelledWaiter) {
  SharedCompletion<int> completion;
  CancellationSource source;
  OperationOptions cancelled_options;
  cancelled_options.cancellation = source.token();
  Coro::Awaitable<void> entered;
  Result<int> cancelled{make_error_code(TransportErrc::kInternal)};
  Result<int> survivor{make_error_code(TransportErrc::kInternal)};

  auto cancelled_task = Coro::makeTask([&] {
    entered.resolve();
    cancelled = completion.Wait(cancelled_options);
  });
  auto survivor_task = Coro::makeTask([&] {
    entered.resolve();
    survivor = completion.Wait();
  });
  EXPECT_TRUE(entered.await());
  EXPECT_TRUE(entered.await());

  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(cancelled_task.get());
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error(), make_error_code(TransportErrc::kCancelled));

  EXPECT_TRUE(completion.Complete(Result<int>{11}));
  EXPECT_TRUE(survivor_task.get());
  ASSERT_TRUE(survivor);
  EXPECT_EQ(survivor.value(), 11);
}

TEST(CoroSharedCompletion, DeadlineOnlyEndsTheTimedOutWaiter) {
  SharedCompletion<int> completion;
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 10ms;
  Result<int> survivor{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto survivor_task = Coro::makeTask([&] {
    entered.resolve();
    survivor = completion.Wait();
  });
  EXPECT_TRUE(entered.await());

  auto timed_out = completion.Wait(options);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));

  EXPECT_TRUE(completion.Complete(Result<int>{13}));
  EXPECT_TRUE(survivor_task.get());
  ASSERT_TRUE(survivor);
  EXPECT_EQ(survivor.value(), 13);
}

TEST(CoroSharedCompletion, CompleteCancelRaceReturnsOneDefinedOutcome) {
  for (int iteration = 0; iteration < 32; ++iteration) {
    SharedCompletion<int> completion;
    CancellationSource source;
    OperationOptions options;
    options.cancellation = source.token();
    Coro::Awaitable<void> entered;
    Result<int> observed{make_error_code(TransportErrc::kInternal)};
    auto waiter = Coro::makeTask([&] {
      entered.resolve();
      observed = completion.Wait(options);
    });
    EXPECT_TRUE(entered.await());

    auto cancelling =
        std::async(std::launch::async, [&] { return source.Cancel(); });
    EXPECT_TRUE(completion.Complete(Result<int>{17}));
    (void)cancelling.get();
    EXPECT_TRUE(waiter.get());

    if (observed) {
      EXPECT_EQ(observed.value(), 17);
    } else {
      EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kCancelled));
    }
    ASSERT_TRUE(completion.Wait());
    EXPECT_EQ(completion.Wait().value(), 17);
  }
}

TEST(CoroSharedCompletion, CompleteTimeoutRaceReturnsOneDefinedOutcome) {
  for (int iteration = 0; iteration < 16; ++iteration) {
    SharedCompletion<int> completion;
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 1ms;
    Result<int> observed{make_error_code(TransportErrc::kInternal)};
    Coro::Awaitable<void> entered;
    auto waiter = Coro::makeTask([&] {
      entered.resolve();
      observed = completion.Wait(options);
    });
    EXPECT_TRUE(entered.await());

    auto completing = std::async(std::launch::async, [&] {
      std::this_thread::sleep_for(1ms);
      return completion.Complete(Result<int>{19});
    });
    EXPECT_TRUE(waiter.get());
    EXPECT_TRUE(completing.get());

    if (observed) {
      EXPECT_EQ(observed.value(), 19);
    } else {
      EXPECT_EQ(observed.error(), make_error_code(TransportErrc::kTimeout));
    }
    ASSERT_TRUE(completion.Wait());
    EXPECT_EQ(completion.Wait().value(), 19);
  }
}
