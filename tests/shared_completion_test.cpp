#include <chrono>
#include <future>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <type_traits>
#include <thread>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/core/SharedCompletion.hpp"

using namespace std::chrono_literals;
using transport::CancellationSource;
using transport::OperationOptions;
using transport::Result;
using transport::SharedCompletion;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

struct CopyProbe {
  std::shared_ptr<std::function<void()>> on_copy;

  CopyProbe() = default;
  explicit CopyProbe(std::shared_ptr<std::function<void()>> callback)
      : on_copy(std::move(callback)) {}
  CopyProbe(const CopyProbe& other) : on_copy(other.on_copy) {
    if (on_copy && *on_copy) {
      (*on_copy)();
    }
  }
  CopyProbe(CopyProbe&&) noexcept = default;
  CopyProbe& operator=(const CopyProbe&) = default;
  CopyProbe& operator=(CopyProbe&&) noexcept = default;
};

static_assert(std::is_default_constructible_v<SharedCompletion<CopyProbe>>,
              "copy-constructible values must be supported");
static_assert(std::is_default_constructible_v<SharedCompletion<void>>,
              "void completion must be supported");

}  // namespace

TEST(CoroSharedCompletion, FirstCompletionWinsAndLateWaitSeesIt) {
  SharedCompletion<int> completion;

  EXPECT_TRUE(completion.Complete(Result<int>{7}));
  EXPECT_FALSE(completion.Complete(Result<int>{8}));

  auto result = completion.Wait();
  ASSERT_TRUE(result);
  EXPECT_EQ(result.value(), 7);
}

TEST(CoroSharedCompletion, VoidFirstCompletionWinsAndLateWaitSucceeds) {
  SharedCompletion<void> completion;

  EXPECT_TRUE(completion.Complete(transport::Status{}));
  EXPECT_FALSE(completion.Complete(
      transport::Status{make_error_code(TransportErrc::kInternal)}));

  auto result = completion.Wait();
  EXPECT_TRUE(result);
}

TEST(CoroSharedCompletion, LateWaitCopiesValueOutsideStateMutex) {
  SharedCompletion<CopyProbe> completion;
  std::promise<void> start_reentry;
  std::promise<void> reentry_finished;
  auto reentry_done = reentry_finished.get_future();
  bool copy_observed_unblocked_reentry = false;
  auto callback = std::make_shared<std::function<void()>>([&] {
    start_reentry.set_value();
    copy_observed_unblocked_reentry =
        reentry_done.wait_for(100ms) == std::future_status::ready;
  });

  EXPECT_TRUE(completion.Complete(Result<CopyProbe>{CopyProbe{callback}}));
  std::thread reentrant_completer([&] {
    start_reentry.get_future().wait();
    EXPECT_FALSE(completion.Complete(Result<CopyProbe>{CopyProbe{}}));
    reentry_finished.set_value();
  });

  auto late = completion.Wait();
  reentrant_completer.join();

  EXPECT_TRUE(late);
  EXPECT_TRUE(copy_observed_unblocked_reentry);
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

TEST(CoroSharedCompletion, PreCancelledWaitReturnsCancelled) {
  SharedCompletion<int> completion;
  CancellationSource source;
  EXPECT_TRUE(source.Cancel());
  OperationOptions options;
  options.cancellation = source.token();

  auto result = completion.Wait(options);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kCancelled));
}

TEST(CoroSharedCompletion, CompletedLateWaitIgnoresCancellationAndPastDeadline) {
  SharedCompletion<int> completion;
  CancellationSource source;
  EXPECT_TRUE(source.Cancel());
  OperationOptions options;
  options.cancellation = source.token();
  options.deadline = OperationOptions::Clock::now() - 1ms;
  EXPECT_TRUE(completion.Complete(Result<int>{17}));

  auto result = completion.Wait(options);

  ASSERT_TRUE(result);
  EXPECT_EQ(result.value(), 17);
}

TEST(CoroSharedCompletion, PastDeadlineWithoutCompletionReturnsTimeout) {
  SharedCompletion<int> completion;
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() - 1ms;

  auto result = completion.Wait(options);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kTimeout));
}

TEST(CoroSharedCompletion, CompleteBeforeCancelReturnsCompletedValue) {
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

  EXPECT_TRUE(completion.Complete(Result<int>{19}));
  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(waiter.get());

  ASSERT_TRUE(observed);
  EXPECT_EQ(observed.value(), 19);
}

TEST(CoroSharedCompletion, CompleteBeforeDeadlineReturnsCompletedValue) {
  SharedCompletion<int> completion;
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 1s;
  Coro::Awaitable<void> entered;
  Result<int> observed{make_error_code(TransportErrc::kInternal)};
  auto waiter = Coro::makeTask([&] {
    entered.resolve();
    observed = completion.Wait(options);
  });
  EXPECT_TRUE(entered.await());

  EXPECT_TRUE(completion.Complete(Result<int>{23}));
  EXPECT_TRUE(waiter.get());

  ASSERT_TRUE(observed);
  EXPECT_EQ(observed.value(), 23);
}
