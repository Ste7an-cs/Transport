#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"
#include "transport/coro/Error.hpp"
#include "transport/coro/ITransport.hpp"
#include "transport/coro/SharedCompletion.hpp"

namespace testutil {

class FakeCoroTransport final : public transport::coro::ITransport {
 private:
  using Datagram = transport::coro::Datagram;
  using LifecycleState = transport::coro::LifecycleState;
  using OperationOptions = transport::coro::OperationOptions;
  template <typename T>
  using Result = transport::coro::Result<T>;
  using SendUnit = transport::coro::SendUnit;
  using Status = transport::coro::Status;
  using TransportErrc = transport::coro::TransportErrc;

  struct State {
    std::mutex mutex;
    LifecycleState lifecycle{LifecycleState::kCreated};
    bool active_read{false};
    bool active_write{false};
    bool hold_writes{false};
    std::deque<Result<Datagram>> queued_reads;
    std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter;
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    std::optional<std::pair<std::error_code, bool>> next_write_error;
    std::function<void()> before_timeout_arbitration;
    std::size_t send_waiters{0};
    std::vector<SendUnit> sent;
    transport::coro::SharedCompletion<void> closed;
  };

 public:
  FakeCoroTransport() : state_(std::make_shared<State>()) {}
  ~FakeCoroTransport() override { BeginClose(state_); }

  Status Start() override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      state->lifecycle = LifecycleState::kRunning;
      return Status{};
    }
    if (state->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    return transport::coro::make_error_code(TransportErrc::kInvalidState);
  }

  Result<Datagram> Read(OperationOptions options = {}) override {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<Datagram>> waiter;
    std::optional<Result<Datagram>> queued;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kCreated) {
        return transport::coro::make_error_code(TransportErrc::kInvalidState);
      }
      if (state->lifecycle != LifecycleState::kRunning) {
        return transport::coro::make_error_code(TransportErrc::kClosed);
      }
      if (state->active_read) {
        return transport::coro::make_error_code(TransportErrc::kInvalidState);
      }
      state->active_read = true;
      if (!state->queued_reads.empty()) {
        queued.emplace(std::move(state->queued_reads.front()));
        state->queued_reads.pop_front();
      } else {
        waiter = std::make_shared<Coro::Awaitable<Datagram>>();
        state->read_waiter = waiter;
      }
    }

    if (queued) {
      auto result = std::move(*queued);
      FinishRead(state, nullptr);
      return result;
    }

    auto registration = options.cancellation.Register([state, waiter] {
      CloseReadWaiter(
          state, waiter,
          transport::coro::make_error_code(TransportErrc::kCancelled));
    });
    Result<Datagram> notification =
        options.deadline
            ? Coro::await_for(waiter,
                              *options.deadline - OperationOptions::Clock::now())
            : Coro::await(waiter);
    if (!notification &&
        notification.error() ==
            std::make_error_code(std::errc::timed_out)) {
      auto before_arbitration = TakeBeforeTimeoutArbitration(state);
      if (before_arbitration) {
        before_arbitration();
      }
      if (!CloseReadWaiter(
              state, waiter,
              transport::coro::make_error_code(TransportErrc::kTimeout))) {
        notification = Coro::await(waiter);
      }
    }
    registration.Reset();
    FinishRead(state, waiter);

    if (notification) {
      return notification;
    }
    if (notification.error() ==
        std::make_error_code(std::errc::timed_out)) {
      return transport::coro::make_error_code(TransportErrc::kTimeout);
    }
    if (notification.error().category() ==
        transport::coro::transport_error_category()) {
      return notification.error();
    }
    return transport::coro::make_error_code(TransportErrc::kInternal);
  }

  Status Write(SendUnit unit) override {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<void>> gate;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kCreated) {
        return transport::coro::make_error_code(TransportErrc::kInvalidState);
      }
      if (state->lifecycle != LifecycleState::kRunning) {
        return transport::coro::make_error_code(TransportErrc::kClosed);
      }
      if (state->active_write) {
        return transport::coro::make_error_code(TransportErrc::kInvalidState);
      }
      state->active_write = true;
      // 发送等待者:已获取有效写、正等待帧刷完(进入操作系统发送缓冲)的 fiber。
      // 单写约束下至多一个,是发送侧背压是否积压的可观测事实(3.4.4)。
      state->send_waiters += 1;
      if (state->hold_writes) {
        gate = std::make_shared<Coro::Awaitable<void>>();
        state->write_gate = gate;
      }
    }

    if (gate) {
      auto released = Coro::await(gate);
      if (!released) {
        auto status = released.error().category() ==
                              transport::coro::transport_error_category()
                          ? Status{released.error()}
                          : Status{transport::coro::make_error_code(
                                TransportErrc::kInternal)};
        FinishWrite(state);
        return status;
      }
    }

    Status result{};
    bool partial_failure = false;
    std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle != LifecycleState::kRunning) {
        result = Status{
            transport::coro::make_error_code(TransportErrc::kClosed)};
      } else if (state->next_write_error) {
        result = Status{state->next_write_error->first};
        partial_failure = state->next_write_error->second;
        state->next_write_error.reset();
        if (partial_failure) {
          state->lifecycle = LifecycleState::kClosing;
          read_waiter = state->read_waiter;
          state->read_waiter.reset();
        }
      } else {
        state->sent.push_back(std::move(unit));
      }
    }
    if (read_waiter) {
      read_waiter->close(
          transport::coro::make_error_code(TransportErrc::kClosed));
    }
    FinishWrite(state);
    return result;
  }

  Status RequestClose() override {
    BeginClose(state_);
    return Status{};
  }

  Status WaitClosed(OperationOptions options = {}) override {
    const auto state = state_;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kCreated) {
        return transport::coro::make_error_code(TransportErrc::kInvalidState);
      }
    }
    return state->closed.Wait(std::move(options));
  }

  void Inject(Datagram datagram) {
    DeliverRead(Result<Datagram>{std::move(datagram)});
  }

  void InjectError(std::error_code error) {
    DeliverRead(Result<Datagram>{error});
  }

  void HoldWrites() {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->hold_writes = true;
  }

  void ReleaseWrite() {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<void>> gate;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->hold_writes = false;
      gate = state->write_gate;
      state->write_gate.reset();
    }
    if (gate) {
      gate->resolve();
      gate->close();
    }
  }

  void FailNextWrite(std::error_code error, bool partial = false) {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->next_write_error = std::make_pair(error, partial);
  }

  void SetBeforeTimeoutArbitration(std::function<void()> hook) {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->before_timeout_arbitration = std::move(hook);
  }

  bool ActiveRead() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->active_read;
  }

  bool ActiveWrite() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->active_write;
  }

  LifecycleState state() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->lifecycle;
  }

  std::vector<SendUnit> sent() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->sent;
  }

  // 发送等待者深度:当前正等待帧刷完的发送 fiber 数(供上层判活/背压观测)。
  std::size_t SendWaiterDepth() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->send_waiters;
  }

 private:
  static void FinishRead(
      const std::shared_ptr<State>& state,
      const std::shared_ptr<Coro::Awaitable<Datagram>>& waiter) {
    bool complete_close = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->active_read = false;
      if (!waiter || state->read_waiter == waiter) {
        state->read_waiter.reset();
      }
      if (state->lifecycle == LifecycleState::kClosing &&
          !state->active_write) {
        state->lifecycle = LifecycleState::kClosed;
        complete_close = true;
      }
    }
    if (complete_close) {
      state->closed.Complete(Status{});
    }
  }

  static void FinishWrite(const std::shared_ptr<State>& state) {
    bool complete_close = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->active_write = false;
      if (state->send_waiters > 0) {
        state->send_waiters -= 1;
      }
      state->write_gate.reset();
      if (state->lifecycle == LifecycleState::kClosing &&
          !state->active_read) {
        state->lifecycle = LifecycleState::kClosed;
        complete_close = true;
      }
    }
    if (complete_close) {
      state->closed.Complete(Status{});
    }
  }

  static void BeginClose(const std::shared_ptr<State>& state) {
    std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter;
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    bool complete_close = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kClosed) {
        return;
      }
      state->lifecycle = LifecycleState::kClosing;
      read_waiter = state->read_waiter;
      write_gate = state->write_gate;
      state->read_waiter.reset();
      state->write_gate.reset();
      if (!state->active_read && !state->active_write) {
        state->lifecycle = LifecycleState::kClosed;
        complete_close = true;
      }
    }
    if (read_waiter) {
      read_waiter->close(
          transport::coro::make_error_code(TransportErrc::kClosed));
    }
    if (write_gate) {
      write_gate->close(
          transport::coro::make_error_code(TransportErrc::kClosed));
    }
    if (complete_close) {
      state->closed.Complete(Status{});
    }
  }

  void DeliverRead(Result<Datagram> result) {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<Datagram>> waiter;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle != LifecycleState::kRunning) {
        return;
      }
      waiter = state->read_waiter;
      if (!waiter) {
        state->queued_reads.push_back(std::move(result));
        return;
      }
      state->read_waiter.reset();
    }
    if (result) {
      waiter->resolve(std::move(result).value());
      waiter->close();
    } else {
      waiter->close(result.error());
    }
  }

  static bool CloseReadWaiter(
      const std::shared_ptr<State>& state,
      const std::shared_ptr<Coro::Awaitable<Datagram>>& waiter,
      std::error_code error) {
    bool claimed = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->read_waiter == waiter) {
        state->read_waiter.reset();
        claimed = true;
      }
    }
    if (claimed) {
      waiter->close(error);
    }
    return claimed;
  }

  static std::function<void()> TakeBeforeTimeoutArbitration(
      const std::shared_ptr<State>& state) {
    std::function<void()> hook;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      hook = std::move(state->before_timeout_arbitration);
    }
    return hook;
  }

  std::shared_ptr<State> state_;
};

}  // namespace testutil
