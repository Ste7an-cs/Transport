#include "transport/TcpClientTransport.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <utility>
#include <vector>

#include <QAbstractSocket>
#include <QPointer>
#include <QString>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"
#include "transport/Error.hpp"
#include "transport/SharedCompletion.hpp"
#include "transport/TcpTransport.hpp"

namespace transport {

using Clock = OperationOptions::Clock;

// -----------------------------------------------------------------------------
// 共享状态:connect-loop fiber 与外层 API 结构性并发访问(读循环 / 发起 fiber /
// 关闭控制),用 std::mutex 串行化(同 ADR-0003 D8)。State 以 shared_ptr 持有,故
// detached 的 loop fiber 在本类析构后仍可安全引用直至 join。
// -----------------------------------------------------------------------------
struct TcpClientTransport::Impl {
  TcpClientConfig config;
  mutable std::mutex mutex;

  LifecycleState lifecycle{LifecycleState::kCreated};
  ConnectionState conn{ConnectionState::kDisconnected};
  bool closing{false};

  // 诊断/观察面。
  std::uint64_t generation{0};
  std::uint64_t config_version{1};
  std::error_code last_failure;
  std::size_t attempt_count{0};
  std::optional<Clock::time_point> next_attempt_time;

  // 当前代际内层(纯字节管道);仅 Connected 期非空。
  std::shared_ptr<TcpTransport> inner;

  // connect-loop 各相位的中断句柄(RequestClose 据此掐断当前尝试)。
  QPointer<QAbstractSocket> connecting_socket;              // Connecting 期
  std::shared_ptr<Coro::Awaitable<void>> backoff_gate;     // Reconnecting 等待期

  // 状态跃迁广播:每个等待者持一 gate,跃迁时 resolve+close 全部并清空。
  std::size_t next_waiter_id{0};
  std::map<std::size_t, std::shared_ptr<Coro::Awaitable<void>>> waiters;

  // 退避账本:current_backoff 为当前退避级别(下一次失败后的等待基值)。
  Clock::duration current_backoff{};
  std::mt19937_64 rng;

  SharedCompletion<void> closed;  // WaitClosed 多等待者。
  std::shared_ptr<Coro::FiberTask<void>> loop_task;  // connect-loop 句柄(join 用)。
};

namespace {

using StatePtr = std::shared_ptr<TcpClientTransport::Impl>;

// 连接失败归类:await_for 超时归 kTimeout,其余 socket/未知故障归 kConnection
// (P3-1 无限重试,分类只供 LastFailure 观察)。
std::error_code ClassifyConnectFailure(std::error_code error) {
  if (error == std::make_error_code(std::errc::timed_out)) {
    return make_error_code(TransportErrc::kTimeout);
  }
  return make_error_code(TransportErrc::kConnection);
}

// 唤醒并清空全部状态等待者(在锁外 resolve/close,避免重入死锁)。
void NotifyWaiters(const StatePtr& s) {
  std::vector<std::shared_ptr<Coro::Awaitable<void>>> woken;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    woken.reserve(s->waiters.size());
    for (auto& entry : s->waiters) {
      woken.push_back(entry.second);
    }
    s->waiters.clear();
  }
  for (const auto& gate : woken) {
    gate->resolve();
    gate->close();
  }
}

// 记录状态并广播跃迁;同态则无副作用(避免把 loop 中重复的 Connecting 报为跃迁)。
void SetConnectionState(const StatePtr& s, ConnectionState next) {
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->conn == next) {
      return;
    }
    s->conn = next;
  }
  NotifyWaiters(s);
}

bool IsClosing(const StatePtr& s) {
  std::lock_guard<std::mutex> lock(s->mutex);
  return s->closing;
}

// 对退避基值施加 ±ratio 抖动(令并发客户端错峰);关闭抖动时原样返回。
Clock::duration ApplyJitter(TcpClientTransport::Impl& s, Clock::duration base) {
  if (!s.config.jitter_enabled || s.config.jitter_ratio <= 0.0) {
    return base;
  }
  std::uniform_real_distribution<double> dist(-s.config.jitter_ratio,
                                              s.config.jitter_ratio);
  const double factor = 1.0 + dist(s.rng);
  const auto scaled = static_cast<Clock::duration::rep>(base.count() * factor);
  return Clock::duration{std::max<Clock::duration::rep>(0, scaled)};
}

// 进入 Reconnecting 并退避等待。返回 true 表示退避正常结束(可继续下一次尝试),
// false 表示等待期间被关闭(loop 应退出)。deadline 由 await_for 承载,RequestClose
// 关闭 backoff_gate 提前唤醒。
bool BackoffWait(const StatePtr& s) {
  SetConnectionState(s, ConnectionState::kReconnecting);

  auto gate = std::make_shared<Coro::Awaitable<void>>();
  Clock::duration delay{};
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->closing) {
      return false;
    }
    if (s->current_backoff <= Clock::duration::zero()) {
      s->current_backoff = s->config.initial_backoff;
    }
    delay = ApplyJitter(*s, s->current_backoff);
    // 推进退避级别:×multiplier,不超过 max_backoff。
    const auto grown = static_cast<Clock::duration::rep>(
        s->current_backoff.count() * s->config.backoff_multiplier);
    s->current_backoff =
        std::min<Clock::duration>(Clock::duration{grown}, s->config.max_backoff);
    s->next_attempt_time = Clock::now() + delay;
    s->backoff_gate = gate;
  }

  Coro::await_for(gate, delay);

  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->backoff_gate.reset();
    s->next_attempt_time.reset();
  }
  return !IsClosing(s);
}

// connect-loop fiber 主体(节点执行域线程):Connecting→Connected→Reconnecting
// 状态机,无限退避重试直至 RequestClose。socket 在本 fiber 内创建(亲和纪律)。
void RunConnectLoop(StatePtr s) {
  const QString host = QString::fromStdString(s->config.host);
  const quint16 port = s->config.port;

  while (!IsClosing(s)) {
    SetConnectionState(s, ConnectionState::kConnecting);

    // 在本 fiber 内创建 socket(亲和纪律:socket 归属本执行域线程)。
    auto* sock = new QTcpSocket();
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->connecting_socket = sock;
      s->attempt_count += 1;
    }

    auto conn = Coro::coro(sock).connectToHost(host, port);
    Coro::Result<void, std::error_code> r =
        Coro::await_for(conn, s->config.connect_timeout);

    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->connecting_socket.clear();
    }

    if (!r) {
      // 失败/超时:await_for 超时不取消底层 → 显式 abort + deleteLater
      // (corosocket 摩擦 1),否则底层连接可能稍后才完成、socket 泄漏。
      sock->abort();
      sock->deleteLater();
      {
        std::lock_guard<std::mutex> lock(s->mutex);
        s->last_failure = ClassifyConnectFailure(r.error());
      }
      if (!BackoffWait(s)) {
        break;
      }
      continue;
    }

    if (IsClosing(s)) {
      sock->abort();
      sock->deleteLater();
      break;
    }

    // 成功物理连接:用 sock 造内层并 Start,切代际,进 Connected 并通知观察者。
    auto inner = std::make_shared<TcpTransport>(sock);
    Status started = inner->Start();
    if (!started) {
      inner->RequestClose();  // 内层析构会 deleteLater sock。
      {
        std::lock_guard<std::mutex> lock(s->mutex);
        s->last_failure = make_error_code(TransportErrc::kInternal);
      }
      if (!BackoffWait(s)) {
        break;
      }
      continue;
    }
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->inner = inner;
      s->generation += 1;
    }
    const auto connected_at = Clock::now();
    SetConnectionState(s, ConnectionState::kConnected);

    // Connected 期:监听断连(waitForDisconnected 覆盖对端断开;RequestClose 掐断
    // 内层 abort socket 亦令其唤醒)。
    Coro::await(Coro::coro(sock).waitForDisconnected());

    const bool was_stable =
        (Clock::now() - connected_at) >= s->config.stable_reset_after;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->inner.reset();
      if (was_stable) {
        s->current_backoff = s->config.initial_backoff;  // 稳定 → 下次断开重置级别。
      }
    }
    inner->RequestClose();  // 幂等:关闭当前代际内层、deleteLater sock。
    inner.reset();

    if (IsClosing(s)) {
      break;
    }
    if (!BackoffWait(s)) {
      break;
    }
  }

  // 终态收敛:弃内层、落 Disconnected、完成 closed(唤醒 WaitClosed)。
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->inner.reset();
    s->lifecycle = LifecycleState::kClosed;
  }
  SetConnectionState(s, ConnectionState::kDisconnected);
  s->closed.Complete(Status{});
}

// 在一个状态 gate 上等待,承载 deadline 与 cancellation。
enum class WaitOutcome { kWoken, kTimeout, kCancelled };

WaitOutcome AwaitGate(const std::shared_ptr<Coro::Awaitable<void>>& gate,
                      OperationOptions& options) {
  auto registration = options.cancellation.Register(
      [gate] { gate->close(make_error_code(TransportErrc::kCancelled)); });

  Coro::Result<void, std::error_code> r =
      options.deadline
          ? Coro::await_for(gate, *options.deadline - Clock::now())
          : Coro::await(gate);

  registration.Reset();

  if (r) {
    return WaitOutcome::kWoken;  // 跃迁 resolve 唤醒。
  }
  if (r.error() == std::make_error_code(std::errc::timed_out)) {
    return WaitOutcome::kTimeout;
  }
  if (r.error() == make_error_code(TransportErrc::kCancelled)) {
    return WaitOutcome::kCancelled;
  }
  return WaitOutcome::kWoken;  // 其它关闭(如跃迁广播的 no_message)→ 重新判定。
}

}  // namespace

TcpClientTransport::TcpClientTransport(TcpClientConfig config)
    : state_(std::make_shared<Impl>()) {
  state_->config = std::move(config);
  state_->current_backoff = state_->config.initial_backoff;
  if (state_->config.jitter_seed != 0) {
    state_->rng.seed(state_->config.jitter_seed);
  } else {
    std::random_device rd;
    state_->rng.seed((static_cast<std::uint64_t>(rd()) << 32) ^ rd());
  }
}

TcpClientTransport::~TcpClientTransport() {
  RequestClose();
  // join connect-loop:get() 在 fiber 内让出直至 loop 退出,保证 detached fiber 不再
  // 触碰 State。RequestClose 已掐断当前尝试,loop 会迅速收敛。
  auto task = state_->loop_task;
  if (task) {
    task->get();
  }
}

Status TcpClientTransport::Start() {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    if (state_->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    state_->lifecycle = LifecycleState::kRunning;
    state_->conn = ConnectionState::kConnecting;  // 立即进 Connecting(不等首次连上)。
  }
  auto s = state_;
  state_->loop_task = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([s] { RunConnectLoop(s); }));
  return Status{};
}

Result<Datagram> TcpClientTransport::Read(OperationOptions options) {
  const auto s = state_;
  // 透明跨重连(ADR-0003 D11 Q1①):Connected 期委托当前代际内层;断连/重连期阻塞等下一
  // 代际连上再委托新代际内层。node 读循环据此永不因 TCP 客户端断连而退出——只 Close 返
  // kClosed。调用方 deadline/取消只结束本次等待(透传 kTimeout/kCancelled),后台重连继续。
  for (;;) {
    std::shared_ptr<TcpTransport> inner;
    bool connected = false;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      if (s->lifecycle == LifecycleState::kCreated) {
        return make_error_code(TransportErrc::kInvalidState);
      }
      if (s->closing || s->lifecycle != LifecycleState::kRunning) {
        return make_error_code(TransportErrc::kClosed);  // 唯一退出:Close/RequestClose。
      }
      connected = (s->conn == ConnectionState::kConnected);
      if (connected) {
        inner = s->inner;  // 拆卸极短窗口内可能已 reset 为空。
      }
    }
    if (inner) {
      auto r = inner->Read(options);
      if (r) {
        return r;  // 拿到当前代际字节。
      }
      const auto err = r.error();
      // 调用方 deadline/取消只结束本次等待,透传(后台重连继续)。
      if (err == make_error_code(TransportErrc::kTimeout) ||
          err == make_error_code(TransportErrc::kCancelled)) {
        return r;
      }
      // 其余(kConnection / 内层因代际切换 RequestClose 返 kClosed / 映射 socket 错误)=
      // 当前代际已死。落到下面等代际推进(不 return kConnection,守透明续命)。
    } else if (!connected) {
      // 未连接:阻塞等进入 Connected 再委托(deadline 只界定本次等待,超时/取消透传)。
      Status waited = WaitForState(ConnectionState::kConnected, options);
      if (!waited) {
        return waited.error();  // kClosed(关闭)/ kTimeout / kCancelled。
      }
      continue;
    }
    // 到此:当前代际内层已死(inner 读失败)或拆卸瞬时窗口(Connected 但 inner 尚空)。
    // 用 WaitStateChange 正确挂起等代际推进(泵事件循环让 connect-loop 察觉断连并跃迁),
    // 绝不忙等自旋饿死 connect-loop。每次挂起前在锁下复检条件,避免丢失唤醒:仅当"仍 Connected
    // 且 s->inner 与刚失败者同一(含二者皆空)"时才等下一跃迁,否则回到顶部按新代际重判。
    for (;;) {
      bool wait_more = false;
      {
        std::lock_guard<std::mutex> lock(s->mutex);
        if (s->closing || s->lifecycle != LifecycleState::kRunning) {
          return make_error_code(TransportErrc::kClosed);
        }
        wait_more =
            (s->conn == ConnectionState::kConnected) && (s->inner == inner);
      }
      if (!wait_more) {
        break;  // 已推进(inner 换代 / 离开 Connected)→ 回顶部重判。
      }
      Result<ConnectionState> changed = WaitStateChange(options);
      if (!changed) {
        return changed.error();  // kTimeout / kCancelled 透传。
      }
    }
  }
}

Status TcpClientTransport::Write(SendUnit unit) {
  std::shared_ptr<TcpTransport> inner;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!state_->closing && state_->conn == ConnectionState::kConnected) {
      inner = state_->inner;
    }
  }
  if (!inner) {
    // 非 Connected 态立即返 kConnection,不缓存(RT_TCP_RECONNECT_003)。
    return make_error_code(TransportErrc::kConnection);
  }
  return inner->Write(std::move(unit));
}

Status TcpClientTransport::RequestClose() {
  QPointer<QAbstractSocket> connecting;
  std::shared_ptr<Coro::Awaitable<void>> backoff_gate;
  std::shared_ptr<TcpTransport> inner;
  bool never_started = false;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closing) {
      return Status{};  // 幂等。
    }
    state_->closing = true;
    if (state_->lifecycle == LifecycleState::kCreated) {
      state_->lifecycle = LifecycleState::kClosed;  // 从未 Start:无 loop 可停。
      never_started = true;
    } else {
      state_->lifecycle = LifecycleState::kClosing;
    }
    connecting = state_->connecting_socket;
    backoff_gate = state_->backoff_gate;
    inner = state_->inner;
  }
  // 掐断当前尝试的各相位:abort 连接中 socket、唤醒退避等待、关闭当前内层。
  if (connecting) {
    connecting->abort();
  }
  if (backoff_gate) {
    backoff_gate->close();
  }
  if (inner) {
    inner->RequestClose();
  }
  if (never_started) {
    NotifyWaiters(state_);
    state_->closed.Complete(Status{});
  }
  return Status{};
}

Status TcpClientTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

ConnectionState TcpClientTransport::State() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->conn;
}

Status TcpClientTransport::WaitForState(ConnectionState target,
                                        OperationOptions options) {
  const auto s = state_;
  for (;;) {
    std::shared_ptr<Coro::Awaitable<void>> gate;
    std::size_t id = 0;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      if (s->conn == target) {
        return Status{};  // 已满足即刻返回。
      }
      if (s->closing) {
        return make_error_code(TransportErrc::kClosed);  // 关闭前无法达成。
      }
      gate = std::make_shared<Coro::Awaitable<void>>();
      id = s->next_waiter_id++;
      s->waiters.emplace(id, gate);
    }
    const WaitOutcome outcome = AwaitGate(gate, options);
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->waiters.erase(id);
    }
    if (outcome == WaitOutcome::kTimeout) {
      return make_error_code(TransportErrc::kTimeout);
    }
    if (outcome == WaitOutcome::kCancelled) {
      return make_error_code(TransportErrc::kCancelled);
    }
    // 唤醒 → 重新判定(deadline 只结束等待,后台重连继续)。
  }
}

Result<ConnectionState> TcpClientTransport::WaitStateChange(
    OperationOptions options) {
  const auto s = state_;
  std::shared_ptr<Coro::Awaitable<void>> gate = std::make_shared<Coro::Awaitable<void>>();
  std::size_t id = 0;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    id = s->next_waiter_id++;
    s->waiters.emplace(id, gate);
  }
  const WaitOutcome outcome = AwaitGate(gate, options);
  ConnectionState now;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->waiters.erase(id);
    now = s->conn;
  }
  if (outcome == WaitOutcome::kTimeout) {
    return make_error_code(TransportErrc::kTimeout);
  }
  if (outcome == WaitOutcome::kCancelled) {
    return make_error_code(TransportErrc::kCancelled);
  }
  return Result<ConnectionState>{now};
}

std::uint64_t TcpClientTransport::Generation() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->generation;
}

std::uint64_t TcpClientTransport::ConfigVersion() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->config_version;
}

std::error_code TcpClientTransport::LastFailure() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_failure;
}

std::size_t TcpClientTransport::AttemptCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->attempt_count;
}

std::optional<TcpClientTransport::Clock::time_point>
TcpClientTransport::NextAttemptTime() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->next_attempt_time;
}

std::size_t TcpClientTransport::SendWaiterDepth() const {
  std::shared_ptr<TcpTransport> inner;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    inner = state_->inner;
  }
  return inner ? inner->SendWaiterDepth() : 0;
}

std::optional<TcpClientTransport::Clock::time_point>
TcpClientTransport::LastSendTime() const {
  std::shared_ptr<TcpTransport> inner;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    inner = state_->inner;
  }
  return inner ? inner->LastSendTime() : std::nullopt;
}

std::optional<TcpClientTransport::Clock::time_point>
TcpClientTransport::LastReceiveTime() const {
  std::shared_ptr<TcpTransport> inner;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    inner = state_->inner;
  }
  return inner ? inner->LastReceiveTime() : std::nullopt;
}

std::error_code TcpClientTransport::LastError() const {
  std::shared_ptr<TcpTransport> inner;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    inner = state_->inner;
  }
  return inner ? inner->LastError() : std::error_code{};
}

}  // namespace transport
