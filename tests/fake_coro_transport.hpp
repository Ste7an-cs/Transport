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
#include "transport/core/Error.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace testutil {

class FakeCoroTransport final : public transport::ITransport {
 private:
  using Datagram = transport::Datagram;
  using LifecycleState = transport::LifecycleState;
  using OperationOptions = transport::OperationOptions;
  using Clock = OperationOptions::Clock;
  template <typename T>
  using Result = transport::Result<T>;
  using SendUnit = transport::SendUnit;
  using Status = transport::Status;
  using TransportErrc = transport::TransportErrc;

  struct State {
    std::mutex mutex;
    LifecycleState lifecycle{LifecycleState::kCreated};
    bool active_read{false};
    bool active_write{false};
    bool hold_writes{false};
    std::deque<Result<Datagram>> queued_reads;
    std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter;
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    // 并发写按到达顺序排队等待写槽(RT_TRANSPORT_004/007 串行化,不拒绝)。
    std::deque<std::shared_ptr<Coro::Awaitable<void>>> write_queue;
    std::optional<std::pair<std::error_code, bool>> next_write_error;
    std::function<void()> before_timeout_arbitration;
    std::size_t send_waiters{0};
    std::vector<SendUnit> sent;
    transport::SharedCompletion<void> closed;
    // I/O 事实(ADR-0003 D13 / ITransport 契约):Write 成功记 last_send;Read 成功
    // 记 last_recv;Write/Read 失败记 last_error(与生产传输的记账口径对齐)。
    std::optional<Clock::time_point> last_send;
    std::optional<Clock::time_point> last_recv;
    std::error_code last_error;
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
    return transport::make_error_code(TransportErrc::kInvalidState);
  }

  Result<Datagram> Read(OperationOptions options = {}) override {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<Datagram>> waiter;
    std::optional<Result<Datagram>> queued;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kCreated) {
        return transport::make_error_code(TransportErrc::kInvalidState);
      }
      if (state->lifecycle != LifecycleState::kRunning) {
        return transport::make_error_code(TransportErrc::kClosed);
      }
      if (state->active_read) {
        return transport::make_error_code(TransportErrc::kInvalidState);
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
      RecordReadOutcome(state, result);
      return result;
    }

    auto registration = options.cancellation.Register([state, waiter] {
      CloseReadWaiter(
          state, waiter,
          transport::make_error_code(TransportErrc::kCancelled));
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
              transport::make_error_code(TransportErrc::kTimeout))) {
        notification = Coro::await(waiter);
      }
    }
    registration.Reset();
    FinishRead(state, waiter);

    Result<Datagram> outcome = std::move(notification);
    if (!outcome) {
      if (outcome.error() == std::make_error_code(std::errc::timed_out)) {
        outcome = transport::make_error_code(TransportErrc::kTimeout);
      } else if (outcome.error().category() !=
                 transport::transport_error_category()) {
        outcome = transport::make_error_code(TransportErrc::kInternal);
      }
    }
    RecordReadOutcome(state, outcome);
    return outcome;
  }

  Status Write(SendUnit unit) override {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<void>> slot_gate;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kCreated) {
        return transport::make_error_code(TransportErrc::kInvalidState);
      }
      if (state->lifecycle != LifecycleState::kRunning) {
        return transport::make_error_code(TransportErrc::kClosed);
      }
      // 发送等待者:自进入 Write 起计数(排队 + 在写),反映发送侧背压积压(3.4.4)。
      state->send_waiters += 1;
      if (!state->active_write) {
        state->active_write = true;  // 写槽空闲 → 立即取得。
      } else {
        // 写槽被占 → 按到达顺序排队等待(RT_TRANSPORT_004/007 串行化,不拒绝)。
        slot_gate = std::make_shared<Coro::Awaitable<void>>();
        state->write_queue.push_back(slot_gate);
      }
    }

    if (slot_gate) {
      auto acquired = Coro::await(slot_gate);
      if (!acquired) {
        // 关闭时被唤醒:从未取得写槽 → 仅回退等待者计数,不释放写槽。
        LeaveWriteQueue(state);
        Status failure = acquired.error().category() ==
                                 transport::transport_error_category()
                             ? Status{acquired.error()}
                             : Status{transport::make_error_code(
                                   TransportErrc::kClosed)};
        RecordWriteOutcome(state, failure);
        return failure;
      }
    }

    // —— 已持有写槽 ——(hold_writes 模拟帧尚未刷完的在写延迟)
    std::shared_ptr<Coro::Awaitable<void>> gate;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->hold_writes) {
        gate = std::make_shared<Coro::Awaitable<void>>();
        state->write_gate = gate;
      }
    }
    if (gate) {
      auto released = Coro::await(gate);
      if (!released) {
        auto status = released.error().category() ==
                              transport::transport_error_category()
                          ? Status{released.error()}
                          : Status{transport::make_error_code(
                                TransportErrc::kInternal)};
        ExitWrite(state);
        RecordWriteOutcome(state, status);
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
            transport::make_error_code(TransportErrc::kClosed)};
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
          transport::make_error_code(TransportErrc::kClosed));
    }
    ExitWrite(state);
    RecordWriteOutcome(state, result);
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
        return transport::make_error_code(TransportErrc::kInvalidState);
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

  // 发送等待者深度:当前处于 Write 中的 fiber 数(排队 + 在写),并发时可 >1,
  // 反映发送侧背压积压(供上层判活/背压观测,3.4.4)。
  std::size_t SendWaiterDepth() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->send_waiters;
  }

  /// @brief 最近一次 Write 成功完成的时刻(尚无则空)。
  std::optional<Clock::time_point> LastSendTime() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->last_send;
  }

  /// @brief 最近一次 Read 成功返回数据的时刻(尚无则空)。
  std::optional<Clock::time_point> LastReceiveTime() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->last_recv;
  }

  /// @brief 最近一次 Write/Read 操作错误(无则默认构造的 error_code)。
  std::error_code LastError() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->last_error;
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

  // 写槽持有者收尾:回退等待者计数,并把写槽移交队首等待者(FIFO 串行化);关闭中
  // 则唤醒全部排队者以 kClosed 收敛,不再移交。
  // 注:写槽 FIFO 语义(EnterWrite/ExitWrite/LeaveWriteQueue/BeginClose)与生产
  // TcpTransport 的同名逻辑对齐——改串行化行为需同步两处。
  static void ExitWrite(const std::shared_ptr<State>& state) {
    std::shared_ptr<Coro::Awaitable<void>> next_gate;
    std::deque<std::shared_ptr<Coro::Awaitable<void>>> closed_gates;
    bool complete_close = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->send_waiters > 0) {
        state->send_waiters -= 1;
      }
      state->write_gate.reset();
      if (state->lifecycle == LifecycleState::kClosing) {
        state->active_write = false;
        closed_gates.swap(state->write_queue);
        if (!state->active_read) {
          state->lifecycle = LifecycleState::kClosed;
          complete_close = true;
        }
      } else if (!state->write_queue.empty()) {
        next_gate = state->write_queue.front();  // 写槽移交队首,active_write 保持真。
        state->write_queue.pop_front();
      } else {
        state->active_write = false;
      }
    }
    for (const auto& gate : closed_gates) {
      gate->close(transport::make_error_code(TransportErrc::kClosed));
    }
    if (next_gate) {
      next_gate->resolve();
      next_gate->close();
    }
    if (complete_close) {
      state->closed.Complete(Status{});
    }
  }

  // 排队等待者在关闭时被唤醒(从未取得写槽):仅回退等待者计数。
  static void LeaveWriteQueue(const std::shared_ptr<State>& state) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->send_waiters > 0) {
      state->send_waiters -= 1;
    }
  }

  // I/O 事实记账(与生产传输口径对齐):Write 成功记 last_send,失败记
  // last_error。
  static void RecordWriteOutcome(const std::shared_ptr<State>& state,
                                 const Status& outcome) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (outcome) {
      state->last_send = Clock::now();
    } else {
      state->last_error = outcome.error();
    }
  }

  // I/O 事实记账:Read 成功记 last_recv,失败记 last_error。
  static void RecordReadOutcome(const std::shared_ptr<State>& state,
                                const Result<Datagram>& outcome) {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (outcome) {
      state->last_recv = Clock::now();
    } else {
      state->last_error = outcome.error();
    }
  }

  static void BeginClose(const std::shared_ptr<State>& state) {
    std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter;
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    std::deque<std::shared_ptr<Coro::Awaitable<void>>> queued_writes;
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
      queued_writes.swap(state->write_queue);  // 唤醒排队写等待者以 kClosed 收敛。
      if (!state->active_read && !state->active_write) {
        state->lifecycle = LifecycleState::kClosed;
        complete_close = true;
      }
    }
    if (read_waiter) {
      read_waiter->close(
          transport::make_error_code(TransportErrc::kClosed));
    }
    if (write_gate) {
      write_gate->close(
          transport::make_error_code(TransportErrc::kClosed));
    }
    for (const auto& gate : queued_writes) {
      gate->close(transport::make_error_code(TransportErrc::kClosed));
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
