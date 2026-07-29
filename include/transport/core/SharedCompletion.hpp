#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

template <typename T>
class SharedCompletion {
  static_assert(std::is_void_v<T> || std::is_copy_constructible_v<T>,
                "SharedCompletion<T> requires T to be void or "
                "copy-constructible because each waiter receives its own "
                "Result<T>");

 private:
  using StoredResult = std::shared_ptr<const Result<T>>;
  using Waiter = Coro::Awaitable<StoredResult>;

  struct State {
    std::mutex mutex;
    StoredResult completion;
    std::size_t next_waiter_id{0};
    std::map<std::size_t, std::weak_ptr<Waiter>> waiters;
  };

 public:
  SharedCompletion() : state_(std::make_shared<State>()) {}

  bool Complete(Result<T> result) {
    auto stored = std::make_shared<const Result<T>>(std::move(result));
    std::vector<std::shared_ptr<Waiter>> waiters;
    auto state = state_;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->completion) {
        return false;
      }
      state->completion = stored;
      waiters.reserve(state->waiters.size());
      for (auto& entry : state->waiters) {
        if (auto waiter = entry.second.lock()) {
          waiters.push_back(std::move(waiter));
        }
      }
      state->waiters.clear();
    }

    for (const auto& waiter : waiters) {
      waiter->resolve(stored);
      waiter->close();
    }
    return true;
  }

  Result<T> Wait(OperationOptions options = {}) const {
    auto state = state_;
    auto waiter = std::make_shared<Waiter>();
    StoredResult completed;
    std::size_t waiter_id = 0;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->completion) {
        completed = state->completion;
      } else {
        waiter_id = state->next_waiter_id++;
        state->waiters.emplace(waiter_id, waiter);
      }
    }
    if (completed) {
      return *completed;
    }

    auto registration = options.cancellation.Register([waiter] {
      waiter->close(make_error_code(TransportErrc::kCancelled));
    });

    Coro::Result<StoredResult, std::error_code> notification =
        options.deadline
            ? Coro::await_for(waiter, *options.deadline - OperationOptions::Clock::now())
            : Coro::await(waiter);

    if (!notification &&
        notification.error() == std::make_error_code(std::errc::timed_out)) {
      waiter->close(make_error_code(TransportErrc::kTimeout));
    }

    registration.Reset();
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->waiters.erase(waiter_id);
    }

    if (notification) {
      const auto& stored = notification.value();
      if (stored) {
        return *stored;
      }
      return make_error_code(TransportErrc::kInternal);
    }
    if (notification.error() == make_error_code(TransportErrc::kCancelled)) {
      return notification.error();
    }
    if (notification.error() == std::make_error_code(std::errc::timed_out)) {
      return make_error_code(TransportErrc::kTimeout);
    }
    return make_error_code(TransportErrc::kInternal);
  }

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
