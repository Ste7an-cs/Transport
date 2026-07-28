#include <atomic>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <thread>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/Cancellation.hpp"
#include "transport/Error.hpp"

using transport::CancellationSource;
using transport::Status;
using transport::TransportErrc;
using transport::make_error_code;

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

TEST(CoroCancellation, ResetWaitsForRunningNotification) {
  using namespace std::chrono_literals;

  CancellationSource source;
  std::promise<void> callback_entered;
  std::promise<void> release_callback;
  auto release = release_callback.get_future().share();
  auto registration = source.token().Register([&] {
    callback_entered.set_value();
    release.wait();
  });

  auto cancelling =
      std::async(std::launch::async, [&] { return source.Cancel(); });
  callback_entered.get_future().wait();

  std::promise<void> reset_started;
  std::promise<void> reset_returned;
  auto returned = reset_returned.get_future();
  auto resetting = std::async(std::launch::async, [&] {
    reset_started.set_value();
    registration.Reset();
    reset_returned.set_value();
  });
  reset_started.get_future().wait();

  EXPECT_EQ(returned.wait_for(50ms), std::future_status::timeout);
  release_callback.set_value();
  EXPECT_TRUE(cancelling.get());
  resetting.get();
  EXPECT_EQ(returned.wait_for(0ms), std::future_status::ready);
}

TEST(CoroCancellation, NotificationCanResetItself) {
  CancellationSource source;
  int calls = 0;
  std::optional<transport::CancellationRegistration> registration;
  registration.emplace(source.token().Register([&] {
    registration->Reset();
    ++calls;
  }));

  EXPECT_TRUE(source.Cancel());
  EXPECT_EQ(calls, 1);
  EXPECT_FALSE(static_cast<bool>(*registration));
}

TEST(CoroCancellation, ThrowingNotificationDoesNotStopLaterNotifications) {
  CancellationSource source;
  int later_calls = 0;
  auto throwing = source.token().Register([] { throw std::runtime_error("boom"); });
  auto later = source.token().Register([&] { ++later_calls; });

  EXPECT_NO_THROW(EXPECT_TRUE(source.Cancel()));
  EXPECT_EQ(later_calls, 1);
}

TEST(CoroCancellation, MoveAssignmentUnregistersPreviousNotification) {
  CancellationSource first_source;
  CancellationSource second_source;
  int first_calls = 0;
  int second_calls = 0;
  auto registration = first_source.token().Register([&] { ++first_calls; });
  auto replacement = second_source.token().Register([&] { ++second_calls; });

  registration = std::move(replacement);
  first_source.Cancel();
  second_source.Cancel();

  EXPECT_EQ(first_calls, 0);
  EXPECT_EQ(second_calls, 1);
  EXPECT_FALSE(static_cast<bool>(replacement));
}

TEST(CoroCancellation, RegistrationDestructionPreventsNotification) {
  CancellationSource source;
  int calls = 0;
  {
    auto registration = source.token().Register([&] { ++calls; });
    EXPECT_TRUE(static_cast<bool>(registration));
  }

  source.Cancel();
  EXPECT_EQ(calls, 0);
}

TEST(CoroCancellation, WaitWakesAllFibers) {
  CancellationSource source;
  Coro::Awaitable<void> ready;
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};

  auto one = Coro::makeTask([&] {
    ready.resolve();
    first = source.token().Wait();
  });
  auto two = Coro::makeTask([&] {
    ready.resolve();
    second = source.token().Wait();
  });
  // The tasks share this cooperative scheduler. resolve() does not yield, so
  // each task proceeds into Wait() before this test fiber consumes its signal.
  EXPECT_TRUE(ready.await());
  EXPECT_TRUE(ready.await());
  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
}

TEST(CoroCancellation, WaitAfterCancellationReturnsImmediately) {
  CancellationSource source;
  EXPECT_TRUE(source.Cancel());

  const Status result = source.token().Wait();

  EXPECT_TRUE(result);
}

TEST(CoroCancellation, EmptyTokenWaitReturnsInvalidState) {
  transport::CancellationToken token;
  const Status result = token.Wait();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kInvalidState));
}
