#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"
#include "transport/coro/Error.hpp"
#include "transport/coro/Result.hpp"
#include "transport/coro/TransportTypes.hpp"

namespace transport::coro {

template <typename T>
class SharedCompletion {
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
    std::size_t waiter_id = 0;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->completion) {
        return *state->completion;
      }
      waiter_id = state->next_waiter_id++;
      state->waiters.emplace(waiter_id, waiter);
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

}  // namespace transport::coro
