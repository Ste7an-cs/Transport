#pragma once

#include <cstddef>
#include <deque>
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
  using LinkState = transport::LinkState;
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
    bool active_write{false};
    bool hold_writes{false};
    // 对外 read_queue(ADR-0007 D1/D4):Inject 是生产者,`Read()` 只交出本句柄。
    // 构造即建,故未 Inject 时读者在其上挂起;InjectError / 关闭以终止原因 close 它。
    // **无需转发泵**:本假件没有底层流,注入本身就是生产端(ADR-0007 D4 的"可不引入泵"情形)。
    std::shared_ptr<Coro::Awaitable<Datagram>> read_queue{
        std::make_shared<Coro::Awaitable<Datagram>>()};
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    // 并发写按到达顺序排队等待写槽(RT_TRANSPORT_004/007 串行化,不拒绝)。
    std::deque<std::shared_ptr<Coro::Awaitable<void>>> write_queue;
    std::optional<std::pair<std::error_code, bool>> next_write_error;
    std::size_t send_waiters{0};
    std::vector<SendUnit> sent;
    transport::SharedCompletion<void> closed;
    // I/O 事实(ADR-0003 D13 / ITransport 契约):Write 成功记 last_send;数据投入
    // read_queue 记 last_recv;Write 失败 / 注入的读错误记 last_error(与生产传输
    // 的泵侧记账口径对齐——生产传输同样在投队时记 last_recv,不等消费者取走)。
    std::optional<Clock::time_point> last_send;
    std::optional<Clock::time_point> last_recv;
    std::error_code last_error;
    // 链路可用性(RT_TRANSPORT_009):缺省由生命周期推导(Running → kUp,其余
    // kDown),SetLinkState 可注入任意取值以模拟"进程活着但链路不可用/建立中"。
    std::optional<LinkState> link_state_override;
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

  // 交出 read_queue 句柄(ADR-0007 D4):不返回数据,deadline/取消/扇出由调用方自理。
  // 未 Start 时给一个以 kInvalidState 关闭的句柄;关闭中/已关闭时 read_queue 已被
  // BeginClose 以 kClosed 关闭,await 即得终止原因。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> Read() override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return transport::ClosedDatagramQueue(
          transport::make_error_code(TransportErrc::kInvalidState));
    }
    return state->read_queue;
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
    std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
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
          read_queue = state->read_queue;  // 部分写失败即关传输:读侧以 kClosed 收敛。
        }
      } else {
        state->sent.push_back(std::move(unit));
      }
    }
    if (read_queue) {
      transport::CloseDatagramQueue(
          read_queue, transport::make_error_code(TransportErrc::kClosed));
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

  /// @brief 投一条数据进 read_queue(Running 期才生效);记 last_recv。
  ///        无消费者时留在队列里,由下一个 await 取走(channel FIFO 保序)。
  void Inject(Datagram datagram) {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle != LifecycleState::kRunning) {
        return;
      }
      read_queue = state->read_queue;
      state->last_recv = Clock::now();
    }
    (void)read_queue->channel()->push(std::move(datagram));
  }

  /// @brief 以 `error` 为终止原因关闭 read_queue(ADR-0007 D4 的终止表达);记 last_error。
  ///        已排队的数据仍先被取尽,之后消费者得到该终止错误。
  void InjectError(std::error_code error) {
    const auto state = state_;
    std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle != LifecycleState::kRunning) {
        return;
      }
      read_queue = state->read_queue;
      state->last_error = error;
    }
    read_queue->close(error);
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

  /// @brief 最近一次数据投入 read_queue 的时刻(尚无则空);与生产传输的泵侧
  ///        记账口径一致——投队即记,不等消费者取走。
  std::optional<Clock::time_point> LastReceiveTime() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->last_recv;
  }

  /// @brief 最近一次 Write 失败 / 注入的读终止原因(无则默认构造的 error_code)。
  std::error_code LastError() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->last_error;
  }

  /// @brief 当前链路可用性:默认与生命周期同调(Running → kUp,其余 kDown);
  ///        经 SetLinkState 注入后以注入值为准。
  LinkState CurrentLinkState() const override {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->link_state_override) {
      return *state->link_state_override;
    }
    return state->lifecycle == LifecycleState::kRunning ? LinkState::kUp
                                                       : LinkState::kDown;
  }

  /// @brief 注入链路可用性(测试钩子):此后 CurrentLinkState 恒返注入值。
  void SetLinkState(LinkState link_state) {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->link_state_override = link_state;
  }

  /// @brief 撤销注入,恢复"与生命周期同调"的缺省口径(测试钩子)。
  void ClearLinkState() {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->link_state_override.reset();
  }

 private:
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
        state->lifecycle = LifecycleState::kClosed;  // 读侧已无在途操作可等。
        complete_close = true;
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

  static void BeginClose(const std::shared_ptr<State>& state) {
    std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
    std::shared_ptr<Coro::Awaitable<void>> write_gate;
    std::deque<std::shared_ptr<Coro::Awaitable<void>>> queued_writes;
    bool complete_close = false;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->lifecycle == LifecycleState::kClosed) {
        return;
      }
      state->lifecycle = LifecycleState::kClosing;
      read_queue = state->read_queue;
      write_gate = state->write_gate;
      state->write_gate.reset();
      queued_writes.swap(state->write_queue);  // 唤醒排队写等待者以 kClosed 收敛。
      if (!state->active_write) {
        state->lifecycle = LifecycleState::kClosed;
        complete_close = true;
      }
    }
    // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因;我方关闭同时丢弃
    // 残留(改造前关闭后发起的读一律得 kClosed、取不到残留)。
    transport::CloseDatagramQueue(
        read_queue, transport::make_error_code(TransportErrc::kClosed));
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

  std::shared_ptr<State> state_;
};

}  // namespace testutil
