#include "transport/io/tcp/TcpClientTransport.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QNetworkProxy>
#include <QPointer>
#include <QString>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"
#include "transport/core/Error.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/SharedCompletion.hpp"
#include "transport/core/TraceCategories.hpp"
#include "transport/io/tcp/TcpTransport.hpp"

namespace transport {

using Clock = OperationOptions::Clock;

// -----------------------------------------------------------------------------
// 共享状态:connect-loop fiber 与外层 API 结构性并发访问(读方 / 发起 fiber /
// 关闭控制),用 std::mutex 串行化(同 ADR-0003 D8)。State 以 shared_ptr 持有,故
// detached 的 loop fiber 在本类析构后仍可安全引用直至 join。
// -----------------------------------------------------------------------------
struct TcpClientTransport::Impl {
  TcpClientConfig config;
  mutable std::mutex mutex;

  LifecycleState lifecycle{LifecycleState::kCreated};
  ConnectionState conn{ConnectionState::kDisconnected};
  bool closing{false};

  // 对外 read_queue(ADR-0004 D6 / ADR-0007 D1):connect-loop 的读泵是唯一生产者,
  // `Read()` 只交出本句柄(ADR-0007 D4)。整个生命周期只此一条——**跨重连不重建**,
  // 故断链前的残留字节保留并继续交付(ADR-0004 D4)。
  //
  // **已知缺口(本轮明确接受,留待性能硬化期)**:该通道**无界**(`FiberChannel` 底层
  // 为 `std::deque`)。若调用方消费慢于对端发送速率,通道可无界增长。字节流不能
  // tail-drop(丢中段即帧错乱),正解是"有界且满时阻塞生产者",使内核接收缓冲填满、
  // 背压经操作系统回传对端;本轮不实现(ADR-0004 D6 / SRS §3.4.4)。
  std::shared_ptr<Coro::Awaitable<Datagram>> rx{
      std::make_shared<Coro::Awaitable<Datagram>>()};

  // 诊断/观察面。配置版本与连接代际两轴独立递增(RT_DATA_STATE)。
  std::uint64_t generation{0};
  std::uint64_t config_version{1};
  std::uint64_t config_change_count{0};  // 规范化配置变更次数(RT_TCP_RECONFIG_006)。
  std::error_code last_failure;
  std::size_t attempt_count{0};
  std::optional<Clock::time_point> next_attempt_time;

  // I/O 事实(本类自己记账,跨代际连续):内层随每次断链消失,若委托内层则断链后回退
  // 为空,故收字节时刻由读泵记在本处,内层最后一次 I/O 错误在拆卸时抄存于此。
  std::optional<Clock::time_point> last_receive;
  std::error_code last_io_error;

  // P5-4:关闭时延(RequestClose 首次调用 → Closed 完成)。
  std::optional<Clock::time_point> close_requested_at;
  Clock::duration last_close_latency{};

  // 端点热更新信号:ApplyConfig 检出 host/port 变化时置位并掐断当前相位,connect-loop
  // 消费后立即以新端点重试(不先等重连间隔,RT_TCP_RECONFIG_005)。
  bool endpoint_reconfig_pending{false};

  // 当前代际内层(纯字节管道);仅 Connected 期非空。
  std::shared_ptr<TcpTransport> inner;

  // connect-loop 各相位的中断句柄(RequestClose 据此掐断当前尝试)。
  QPointer<QAbstractSocket> connecting_socket;            // Connecting 期
  std::shared_ptr<Coro::Awaitable<void>> reconnect_gate;  // 重连间隔等待期

  // 状态跃迁广播:每个等待者持一 gate,跃迁时 resolve+close 全部并清空。
  std::size_t next_waiter_id{0};
  std::map<std::size_t, std::shared_ptr<Coro::Awaitable<void>>> waiters;

  SharedCompletion<void> closed;  // WaitClosed 多等待者。
  std::shared_ptr<Coro::FiberTask<void>> loop_task;  // connect-loop 句柄(join 用)。
};

namespace {

using StatePtr = std::shared_ptr<TcpClientTransport::Impl>;

// 连接失败归类:await_for 超时归 kTimeout,其余 socket/未知故障归 kConnection
// (无限重试,分类只供 LastFailure 观察)。
std::error_code ClassifyConnectFailure(std::error_code error) {
  if (error == std::make_error_code(std::errc::timed_out)) {
    return make_error_code(TransportErrc::kTimeout);
  }
  return make_error_code(TransportErrc::kConnection);
}

// ConnectionState 的稳定短名,供 "connect" 类别 Trace 的 message 复用(P5-4)。
std::string_view ConnectionStateName(ConnectionState state) {
  switch (state) {
    case ConnectionState::kDisconnected:
      return "disconnected";
    case ConnectionState::kConnecting:
      return "connecting";
    case ConnectionState::kConnected:
      return "connected";
    case ConnectionState::kReconnecting:
      return "reconnecting";
  }
  return "unknown";
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
// "connect" 类别 Trace(P5-4):Connecting/Connected/Reconnecting 三态跃迁边界点上报;
// 终态 Disconnected 归 "lifecycle" 类别(生命周期终态,非连接管理态),此处不重复。
void SetConnectionState(const StatePtr& s, ConnectionState next) {
  ITraceSink* sink = nullptr;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->conn == next) {
      return;
    }
    s->conn = next;
    sink = s->config.trace_sink;
  }
  if (next != ConnectionState::kDisconnected) {
    RecordEvent(kTraceCategoryConnect, sink, ConnectionStateName(next));
  }
  NotifyWaiters(s);
}

bool IsClosing(const StatePtr& s) {
  std::lock_guard<std::mutex> lock(s->mutex);
  return s->closing;
}

// 完整校验一份配置快照(RT_TCP_RECONFIG_003):热更新范围内所有字段须自洽,任一非法
// 则整个更新失败。connect_timeout 落 [100ms,60s](SRS §3.1.7.4);重连间隔须为正
// (零间隔会退化为紧循环,ADR-0005 D4)。
bool IsConfigValid(const TcpClientConfig& c) {
  if (c.host.empty()) {
    return false;  // host 非空。
  }
  if (c.port == 0) {
    return false;  // 端口非 0。
  }
  if (c.connect_timeout < std::chrono::milliseconds{100} ||
      c.connect_timeout > std::chrono::milliseconds{60000}) {
    return false;  // 连接超时落 100ms–60s。
  }
  if (c.reconnect_interval <= TcpClientConfig::Duration::zero()) {
    return false;  // 重连间隔为正。
  }
  return true;
}

// 消费端点热更新信号:置位则清除并返 true(loop 应跳过重连等待、立即以新端点重试)。
bool ConsumeEndpointReconfig(const StatePtr& s) {
  std::lock_guard<std::mutex> lock(s->mutex);
  if (s->endpoint_reconfig_pending) {
    s->endpoint_reconfig_pending = false;
    return true;
  }
  return false;
}

// 进入 Reconnecting 并按**固定间隔**等待下一次尝试(ADR-0005 D4:指数退避已撤销)。
// 返回 true 表示等待正常结束(可继续下一次尝试),false 表示等待期间被关闭(loop 应
// 退出)。间隔由 await_for 承载,RequestClose / 端点热更新关闭 gate 提前唤醒。
bool ReconnectWait(const StatePtr& s) {
  SetConnectionState(s, ConnectionState::kReconnecting);

  auto gate = std::make_shared<Coro::Awaitable<void>>();
  Clock::duration delay{};
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->closing) {
      return false;
    }
    delay = s->config.reconnect_interval;
    s->next_attempt_time = Clock::now() + delay;
    s->reconnect_gate = gate;
  }

  Coro::await_for(gate, delay);

  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->reconnect_gate.reset();
    s->next_attempt_time.reset();
  }
  return !IsClosing(s);
}

// 读泵(ADR-0004 D6):反复从本代际内层的 read_queue 取一片字节投入对外 read_queue,
// 直至本代际读取终结。
//
// 内层 read_queue 的终止原因一律是 `kClosed`(ADR-0004 D1:已连接 socket 上的致命错误
// 与对端关闭同码,表达经 ADR-0007 D4 改写为"队列被 close 并带终止原因"),含义即"本物理
// 连接结束"——**此处不向调用方转发任何断链信号**,只退出读泵令 connect-loop 转入重连
// (断链完全透明)。对外通道无界,风险见 `Impl::rx` 注释。
void PumpReceivedBytes(const StatePtr& s, TcpTransport& inner) {
  const auto channel = s->rx->channel();
  const auto inner_rx = inner.Read();  // 取一次内层句柄,循环 await(ADR-0007 D4)。
  for (;;) {
    Coro::Result<Datagram, std::error_code> piece = Coro::await(inner_rx);
    if (!piece) {
      return;  // 本代际读取终结 → 拆代际、转重连(调用方无感)。
    }
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->last_receive = Clock::now();  // 跨代际连续的"最近收字节时刻"。
    }
    if (channel->push(std::move(piece).value()) !=
        boost::fibers::channel_op_status::success) {
      return;  // 对外通道已关闭(我方 RequestClose)→ 停止投递。
    }
  }
}

// connect-loop fiber 主体(节点执行域线程),本类唯一的内部工作单元:
// Connecting→Connected(读泵)→Reconnecting 状态机,固定间隔无限重试直至 RequestClose。
// socket 在本 fiber 内创建(亲和纪律)。
void RunConnectLoop(StatePtr s) {
  while (!IsClosing(s)) {
    // 每轮读取当前生效端点/超时(支持端点/超时热更新:下一次连接动作用新参数)。
    QString host;
    std::string host_str;
    quint16 port;
    Clock::duration connect_timeout{};
    ITraceSink* sink = nullptr;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      host_str = s->config.host;
      host = QString::fromStdString(host_str);
      port = s->config.port;
      connect_timeout = s->config.connect_timeout;
      sink = s->config.trace_sink;
    }
    // "reconnect" 类别 Trace(P5-4)的 endpoint 标识:host:port,不逐字节。
    const std::string endpoint = host_str + ":" + std::to_string(port);

    SetConnectionState(s, ConnectionState::kConnecting);

    // 在本 fiber 内创建 socket(亲和纪律:socket 归属本执行域线程)。
    auto* sock = new QTcpSocket();
    // 显式禁用代理(#123):部分发行版的 Qt 以 `QT_USE_SYSTEM_PROXIES` 构建(如 Ubuntu),
    // `QTcpSocket` 会解析 `http_proxy` / `all_proxy` 等环境变量,把本应直连 host:port 的
    // TCP 连接改走 HTTP CONNECT / SOCKS5 隧道。此时"连上"的是代理而非目标端点,连接
    // 成败、超时与失败归因全部失真(代理即刻接受再断开 → 空代际 + 无失败记账)。本层
    // 契约是"到指定端点的纯字节管道",不承载任何环境级代理策略,故一律直连。
    sock->setProxy(QNetworkProxy::NoProxy);
    std::size_t attempt_no = 0;
    bool closing_before_connect = false;
    {
      // 与内层登记同理:在同一把锁内判 closing 并登记连接中 socket,否则 RequestClose
      // 可能错过它(此刻 connecting_socket 尚空),本次尝试就得空等满一个连接超时。
      std::lock_guard<std::mutex> lock(s->mutex);
      if (s->closing) {
        closing_before_connect = true;
      } else {
        s->connecting_socket = sock;
        s->attempt_count += 1;
        attempt_no = s->attempt_count;
      }
    }
    if (closing_before_connect) {
      sock->abort();
      sock->deleteLater();
      break;
    }

    auto conn = Coro::coro(sock).connectToHost(host, port);
    Coro::Result<void, std::error_code> r =
        Coro::await_for(conn, connect_timeout);

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
      // "reconnect" 类别 Trace(P5-4):本次 connect 尝试失败(每次尝试成功/失败均上报)。
      RecordEvent(kTraceCategoryReconnect, sink, "attempt-failed", {}, endpoint,
                  r.error().message(), kNoNum, static_cast<int>(attempt_no));
      // 端点热更新(掐断本次 Connecting)→ 立即以新端点重试,不先等重连间隔。
      if (ConsumeEndpointReconfig(s)) {
        continue;
      }
      if (!ReconnectWait(s)) {
        break;
      }
      continue;
    }

    if (IsClosing(s)) {
      sock->abort();
      sock->deleteLater();
      break;
    }

    // 成功物理连接:用 sock 造内层并 Start,切代际,进 Connected。
    auto inner = std::make_shared<TcpTransport>(sock);
    Coro::Result<void> started = inner->Start();
    if (!started) {
      inner->RequestClose();  // 内层析构会 deleteLater sock。
      {
        std::lock_guard<std::mutex> lock(s->mutex);
        s->last_failure = make_error_code(TransportErrc::kInternal);
      }
      // "reconnect" 类别 Trace(P5-4):物理连接成功但内层启动失败,仍归本次尝试失败。
      RecordEvent(kTraceCategoryReconnect, sink, "start-failed", {}, endpoint,
                  started.error().message(), kNoNum, static_cast<int>(attempt_no));
      if (!ReconnectWait(s)) {
        break;
      }
      continue;
    }
    // 发布本代际内层与 RequestClose 的交接点:必须在**同一把锁**内判 closing 并登记
    // 内层,否则存在窗口——RequestClose 在登记前置 closing(它此刻看到的 inner 为空,
    // 不会关闭本内层),而 loop 随后进读泵,将永远挂在内层 Read 上。
    std::uint64_t new_generation = 0;
    bool closing_now = false;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      if (s->closing) {
        closing_now = true;
      } else {
        s->inner = inner;
        s->generation += 1;
        new_generation = s->generation;
      }
    }
    if (closing_now) {
      inner->RequestClose();  // 内层析构会 deleteLater sock。
      inner.reset();
      break;
    }
    // "generation" 类别 Trace(P5-4):新代际建立,与 connect 成功同点;size 载新代际号。
    RecordEvent(kTraceCategoryGeneration, sink, {}, {}, endpoint, {},
                static_cast<long>(new_generation));
    // "reconnect" 类别 Trace(P5-4):本次 connect 尝试成功。
    RecordEvent(kTraceCategoryReconnect, sink, "attempt-succeeded", {}, endpoint,
                {}, kNoNum,
                static_cast<int>(attempt_no));
    SetConnectionState(s, ConnectionState::kConnected);

    // Connected 期:读泵持续取字节投入对外通道,直至本代际读取终结(断链 / 我方关闭 /
    // 端点热更新掐断)。断链不产生任何面向调用方的信号(ADR-0004 D1)。
    PumpReceivedBytes(s, *inner);

    {
      std::lock_guard<std::mutex> lock(s->mutex);
      s->inner.reset();
      const std::error_code inner_error = inner->LastError();
      if (inner_error) {
        s->last_io_error = inner_error;  // 抄存:内层随代际消失,诊断事实需跨代际保留。
      }
    }
    inner->RequestClose();  // 幂等:关闭当前代际内层、deleteLater sock。
    inner.reset();

    if (IsClosing(s)) {
      break;
    }
    // 端点热更新导致的断连 → 立即以新端点重试新代际,不先等重连间隔。
    if (ConsumeEndpointReconfig(s)) {
      continue;
    }
    if (!ReconnectWait(s)) {
      break;
    }
  }

  // 终态收敛:弃内层、落 Disconnected、完成 closed(唤醒 WaitClosed)。
  ITraceSink* closing_sink = nullptr;
  Clock::duration close_latency{};
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->inner.reset();
    s->lifecycle = LifecycleState::kClosed;
    if (s->close_requested_at) {
      close_latency = Clock::now() - *s->close_requested_at;
      s->last_close_latency = close_latency;  // P5-4:关闭时延终点。
    }
    closing_sink = s->config.trace_sink;
  }
  // "lifecycle" 类别 Trace(P5-4,#98 改名):生命周期终态 Closed;size 载关闭时延(微秒)。
  RecordEvent(kTraceCategoryLifecycle, closing_sink, "closed", {}, {}, {},
              static_cast<long>(
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      close_latency)
                      .count()));
  SetConnectionState(s, ConnectionState::kDisconnected);
  s->closed.Complete(Coro::Result<void>{});
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

Coro::Result<void> TcpClientTransport::Start() {
  ITraceSink* sink = nullptr;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kRunning) {
      return Coro::Result<void>{};
    }
    if (state_->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    state_->lifecycle = LifecycleState::kRunning;
    state_->conn = ConnectionState::kConnecting;  // 立即进 Connecting(不等首次连上)。
    sink = state_->config.trace_sink;
  }
  // "lifecycle" 类别 Trace(P5-4,#98 改名):生命周期跃迁 Created→Running。
  RecordEvent(kTraceCategoryLifecycle, sink, "running");
  auto s = state_;
  state_->loop_task = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([s] { RunConnectLoop(s); }));
  return Coro::Result<void>{};
}

// 交出对外 read_queue 的句柄(ADR-0007 D4)——**无需转发泵**:connect-loop 的读泵本就
// 以本队列为出口,元素已是 `Datagram`,故直接交出即等价。
//
// 与连接状态机完全解耦——**断链完全透明**(ADR-0004 D1/D6):断链期间队列无数据故调用方
// 在句柄上挂起,重连后读泵投入新链路字节即被唤醒。本队列**不因断链关闭**;唯一的终止是
// 我方 RequestClose 后的 `close(kClosed)`。未 Start 时给出以 kInvalidState 关闭的句柄。
std::shared_ptr<Coro::Awaitable<Datagram>> TcpClientTransport::Read() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    return ClosedDatagramQueue(make_error_code(TransportErrc::kInvalidState));
  }
  return state_->rx;
}

Coro::Result<void> TcpClientTransport::Write(SendUnit unit) {
  std::shared_ptr<TcpTransport> inner;
  bool created = false;
  bool closed = false;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    created = state_->lifecycle == LifecycleState::kCreated;
    closed = !created && (state_->closing ||
                          state_->lifecycle != LifecycleState::kRunning);
    if (!created && !closed && state_->conn == ConnectionState::kConnected) {
      inner = state_->inner;
    }
  }
  if (created) {
    return make_error_code(TransportErrc::kInvalidState);  // 未 Start(同其它介质)。
  }
  if (closed) {
    return make_error_code(TransportErrc::kClosed);  // 我方已关闭:传输终结。
  }
  if (!inner) {
    // 链路不可用(未连上 / 重连中)→ 立即返 kConnection,不缓存(RT_TCP_RECONNECT_003)。
    return make_error_code(TransportErrc::kConnection);
  }
  Coro::Result<void> sent = inner->Write(std::move(unit));
  if (!sent && sent.error() == make_error_code(TransportErrc::kClosed)) {
    // 内层在本次发送期间随断链终结,而本传输仍在运行(还会重连):对调用方而言这是
    // "链路不可用",不是传输终结 → 归 kConnection(ADR-0004 D1:kClosed 仅表传输终结)。
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!state_->closing && state_->lifecycle == LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kConnection);
    }
  }
  return sent;
}

Coro::Result<void> TcpClientTransport::RequestClose() {
  QPointer<QAbstractSocket> connecting;
  std::shared_ptr<Coro::Awaitable<void>> reconnect_gate;
  std::shared_ptr<TcpTransport> inner;
  std::shared_ptr<Coro::Awaitable<Datagram>> rx;
  bool never_started = false;
  ITraceSink* sink = nullptr;
  Clock::time_point requested_at{};
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closing) {
      return Coro::Result<void>{};  // 幂等。
    }
    state_->closing = true;
    requested_at = Clock::now();
    state_->close_requested_at = requested_at;  // P5-4:关闭时延起点。
    sink = state_->config.trace_sink;
    if (state_->lifecycle == LifecycleState::kCreated) {
      state_->lifecycle = LifecycleState::kClosed;  // 从未 Start:无 loop 可停。
      never_started = true;
    } else {
      state_->lifecycle = LifecycleState::kClosing;
    }
    connecting = state_->connecting_socket;
    reconnect_gate = state_->reconnect_gate;
    inner = state_->inner;
    rx = state_->rx;
  }
  // "lifecycle" 类别 Trace(P5-4,#98 改名):生命周期跃迁 Running→Closing(从未 Start
  // 则直落 Closed)。
  RecordEvent(kTraceCategoryLifecycle, sink, never_started ? "closed" : "closing");
  // 掐断当前尝试的各相位:abort 连接中 socket、唤醒重连等待、关闭当前内层(读泵随之
  // 退出),并关闭对外通道令在途 Read 以 kClosed 收敛(唯一使 Read 失败终止的路径)。
  if (connecting) {
    connecting->abort();
  }
  if (reconnect_gate) {
    reconnect_gate->close();
  }
  if (inner) {
    inner->RequestClose();
  }
  if (rx) {
    // 终止表达(ADR-0007 D4):以 kClosed 关对外 read_queue 并丢弃残留——改造前
    // `Read` 在 closing 下先判生命周期返 kClosed,取不到残留,此处与之等价。
    CloseDatagramQueue(rx, make_error_code(TransportErrc::kClosed));
  }
  if (never_started) {
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->last_close_latency = Clock::now() - requested_at;  // P5-4:关闭时延终点。
    }
    NotifyWaiters(state_);
    state_->closed.Complete(Coro::Result<void>{});
  }
  return Coro::Result<void>{};
}

Coro::Result<void> TcpClientTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

Coro::Result<void> TcpClientTransport::ApplyConfig(TcpClientConfig config,
                                       std::uint64_t version) {
  // 端点变化时需在锁外掐断当前尝试/连接的相位句柄(避免持锁重入)。
  QPointer<QAbstractSocket> connecting;
  std::shared_ptr<Coro::Awaitable<void>> reconnect_gate;
  std::shared_ptr<TcpTransport> inner;
  bool endpoint_changed = false;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);

    // 生命周期非法:关闭中/已关闭不再接受重配置(可区分类别 kInvalidState)。
    if (state_->closing || state_->lifecycle == LifecycleState::kClosing ||
        state_->lifecycle == LifecycleState::kClosed) {
      return make_error_code(TransportErrc::kInvalidState);
    }

    // 先完整校验字段(RT_TCP_RECONFIG_003):任一非法 → 整个失败、旧配置旧连接不变、
    // 返 kConfiguration(参数非法类别)。
    if (!IsConfigValid(config)) {
      return make_error_code(TransportErrc::kConfiguration);
    }

    // 单调版本判定(RT_TCP_RECONFIG_004),失败用 kInvalidArgument(版本过期类别)。
    if (version < state_->config_version) {
      return make_error_code(TransportErrc::kInvalidArgument);  // 过期。
    }
    if (version == state_->config_version) {
      if (config == state_->config) {
        return Coro::Result<void>{};  // 同版同容 → 成功 no-op,不产生变更通知。
      }
      return make_error_code(TransportErrc::kInvalidArgument);  // 同版异容 → 乱序拒绝。
    }

    // version > current:原子应用(校验已过,此处不再可失败)。
    const bool content_changed = (config != state_->config);
    endpoint_changed = (config.host != state_->config.host) ||
                       (config.port != state_->config.port);

    state_->config = std::move(config);
    state_->config_version = version;  // 与连接代际两轴独立递增(RT_DATA_STATE)。
    if (content_changed) {
      // 每次非空成功应用产生一次规范化配置变更通知(RT_TCP_RECONFIG_006)。
      state_->config_change_count += 1;
    }

    // 仅策略参数(连接超时/重连间隔)变化:不打断正在进行的连接尝试或已开始的间隔等待
    // ——它们用旧快照,connect-loop 下一轮才读新参数;此处无需掐断。
    if (endpoint_changed && state_->lifecycle == LifecycleState::kRunning) {
      // 端点变化(RT_TCP_RECONFIG_005):置位热更新信号,随后掐断当前相位,令
      // connect-loop 立即以新端点重试新代际(不先等重连间隔)。
      state_->endpoint_reconfig_pending = true;
      connecting = state_->connecting_socket;
      reconnect_gate = state_->reconnect_gate;
      inner = state_->inner;
    }
  }

  // 锁外掐断当前相位(端点变化):abort 连接中 socket / 提前唤醒重连等待 / 关旧连接。
  // 旧连接的在途请求不在本层终结(ADR-0004 D3 已撤销代际隔离),由各自总超时收敛。
  if (endpoint_changed) {
    if (connecting) {
      connecting->abort();
    }
    if (reconnect_gate) {
      reconnect_gate->close();
    }
    if (inner) {
      inner->RequestClose();  // 停止接受新发送 + 关旧连接(读泵随之退出)。
    }
  }
  return Coro::Result<void>{};
}

ConnectionState TcpClientTransport::State() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->conn;
}

Coro::Result<void> TcpClientTransport::WaitForState(ConnectionState target,
                                        OperationOptions options) {
  const auto s = state_;
  for (;;) {
    std::shared_ptr<Coro::Awaitable<void>> gate;
    std::size_t id = 0;
    {
      std::lock_guard<std::mutex> lock(s->mutex);
      if (s->conn == target) {
        return Coro::Result<void>{};  // 已满足即刻返回。
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

Coro::Result<ConnectionState> TcpClientTransport::WaitStateChange(
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
  return Coro::Result<ConnectionState>{now};
}

std::uint64_t TcpClientTransport::Generation() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->generation;
}

std::uint64_t TcpClientTransport::ConfigVersion() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->config_version;
}

std::uint64_t TcpClientTransport::ConfigChangeCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->config_change_count;
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

TcpClientTransport::Clock::duration
TcpClientTransport::LastCloseLatency() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_close_latency;
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

// 收字节时刻由读泵记在本类(而非委托内层):内层随每次断链消失,委托会使断链后回退为
// 空,而"最近收到字节"是跨代际连续的 I/O 事实。
std::optional<TcpClientTransport::Clock::time_point>
TcpClientTransport::LastReceiveTime() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_receive;
}

// 当前代际内层的 I/O 错误优先;无内层(重连期)则给拆卸时抄存的最后一次。
std::error_code TcpClientTransport::LastError() const {
  std::shared_ptr<TcpTransport> inner;
  std::error_code fallback;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    inner = state_->inner;
    fallback = state_->last_io_error;
  }
  if (inner) {
    const std::error_code current = inner->LastError();
    if (current) {
      return current;
    }
  }
  return fallback;
}

// 链路可用性直接映射连接状态机(不委托内层):内层只在 Connected 期存在,而
// Connecting/Reconnecting 期须如实报 kEstablishing。生命周期先决——Closing/Closed
// 期连接状态机可能尚未收敛到 Disconnected,但链路对调用方已不可用。
LinkState TcpClientTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle != LifecycleState::kRunning) {
    return LinkState::kDown;
  }
  switch (state_->conn) {
    case ConnectionState::kConnected:
      return LinkState::kUp;
    case ConnectionState::kConnecting:
    case ConnectionState::kReconnecting:
      return LinkState::kEstablishing;
    case ConnectionState::kDisconnected:
      break;
  }
  return LinkState::kDown;
}

}  // namespace transport
