#include "transport/io/udp/UdpTransport.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QNetworkDatagram>
#include <QNetworkProxy>
#include <QUdpSocket>

#include "await/awaitable.hpp"
#include "await/coroudpsocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"  // Coro::makeTask —— 泵 / 写泵 fiber。
#include "transport/core/Error.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace transport {
namespace {

// bind 失败后的固定重试间隔(ADR-0007 D2):**内部常量、非配置项**。无限重试,唯一的
// 退出条件是我方 Close——退避由 close_signal 承载,故 Close 可提前打断(见 §6 四处打断)。
constexpr std::chrono::milliseconds kBindRetryInterval{3000};

// 把 Qt socket 错误映射到传输错误类别:网络层断裂归 Connection,报文过大归
// InvalidArgument(调用方给了不可发送的报文),其余读写故障归 Io。
std::error_code MapSocketError(std::error_code error) {
  if (error.category() == Coro::detail::socket_error_category()) {
    switch (static_cast<QAbstractSocket::SocketError>(error.value())) {
      case QAbstractSocket::NetworkError:
      case QAbstractSocket::ConnectionRefusedError:
        return make_error_code(TransportErrc::kConnection);
      case QAbstractSocket::DatagramTooLargeError:
        return make_error_code(TransportErrc::kInvalidArgument);
      default:
        return make_error_code(TransportErrc::kIo);
    }
  }
  return make_error_code(TransportErrc::kIo);
}

}  // namespace

// -----------------------------------------------------------------------------
// 共享状态(ADR-0007 D1 样板,设计见 docs/superpowers/specs/2026-08-12-udp-transport-
// pump-design.md §3):泵 fiber / 写泵 fiber / 外层 API 结构性并发访问,用 std::mutex
// 串行化。State 以 shared_ptr 持有,故两条 detached fiber 在本类析构后仍可安全引用
// 直至 join。
// -----------------------------------------------------------------------------
struct UdpTransport::State {
  mutable std::mutex mutex;

  // 启动后不变,不加锁读(#156 起将含 silence_timeout)。
  UdpConfig config;
  // **整个生命期只一个 socket 对象**:Start 建、析构销。QUdpSocket 支持
  // bind→close→再 bind,且 bind 失败过的对象仍可复用(实测),故无需每代换对象,
  // 判据只剩 `state() == BoundState`。设一次后只读,写泵持有的指针整个生命期稳定。
  QUdpSocket* socket{nullptr};

  LifecycleState lifecycle{LifecycleState::kCreated};
  SharedCompletion<void> closed;  // WaitClosed 多等待者(自守其锁)。

  // 对外数据面两条队列:**不随 socket 重建而更换**——这正是"重建对调用方透明"的载体。
  // 容量策略未定(TBD-009 / #152):沿用 AsyncTask 默认(有界 1024、静默丢最旧),
  // 本轮不处置,也不在别处加补偿逻辑。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  std::shared_ptr<Coro::Awaitable<SendUnit>> write_queue{
      std::make_shared<Coro::Awaitable<SendUnit>>()};
  // 写泵等 socket 就绪的信号(复用一个,不换代)。
  std::shared_ptr<Coro::Awaitable<void>> socket_ready{
      std::make_shared<Coro::Awaitable<void>>()};
  // 只为打断"未 bind 时的退避":未 bind 的 socket 上建读流会被当场关闭(实测
  // `await_for` 0ms 返回 no_message),故**不能**用读超时当重试间隔,退避须用独立
  // 延时原语,否则外层循环退化成不带间隔的紧转。
  std::shared_ptr<Coro::Awaitable<void>> close_signal{
      std::make_shared<Coro::Awaitable<void>>()};

  std::shared_ptr<Coro::FiberTask<void>> pump;        // 析构 join。
  std::shared_ptr<Coro::FiberTask<void>> write_pump;  // 泵收敛时先 join。

  // 观测面(ITransport 强制)。
  std::optional<OperationOptions::Clock::time_point> last_send;
  std::optional<OperationOptions::Clock::time_point> last_recv;
  std::error_code last_error;
  std::uint16_t local_port{0};  // 实际绑定端口。
};

namespace {

using StatePtr = std::shared_ptr<UdpTransport::State>;

bool IsClosing(const StatePtr& state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  return state->lifecycle >= LifecycleState::kClosing;
}

// 一次 bind 尝试(幂等前置 close:把可能处于任意状态的 socket 重置到可 bind 状态)。
// 成功则记下实际端口并(组播)重新入组;失败只记 last_error——**不是启动失败**,由
// 外层循环退避后无限重试(ADR-0007 D2)。
bool BindOnce(const StatePtr& state) {
  QUdpSocket* socket = state->socket;
  socket->close();

  const QHostAddress bind_addr(
      QString::fromStdString(state->config.local_addr));
  // ShareAddress|ReuseAddressHint:组播/多消费者场景可共享绑定;单播 loopback 无碍。
  const QAbstractSocket::BindMode bind_mode =
      state->config.mode == UdpMode::kMulticast
          ? (QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint)
          : QUdpSocket::DefaultForPlatform;
  if (!socket->bind(bind_addr, state->config.local_port, bind_mode)) {
    std::error_code bind_error =
        MapSocketError(Coro::detail::socket_error_code(socket->error()));
    if (!bind_error) {
      bind_error = make_error_code(TransportErrc::kIo);
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_error = bind_error;
    return false;
  }

  if (state->config.mode == UdpMode::kMulticast &&
      !state->config.multicast_group.empty()) {
    // 重建后须重新入组:close 会退组。
    const QHostAddress group(
        QString::fromStdString(state->config.multicast_group));
    socket->setSocketOption(QAbstractSocket::MulticastTtlOption,
                            static_cast<int>(state->config.ttl));
    socket->joinMulticastGroup(group);
  }

  std::lock_guard<std::mutex> lock(state->mutex);
  // 临时端口(local_port 配 0)每次重 bind 会拿到不同端口:对端若记着源端口会失联。
  // 是否"重 bind 时沿用上次端口"未定,本轮不处理(设计 §9)。
  state->local_port = socket->localPort();
  return true;
}

// 一条完整报文 → Datagram;source 填每条报文的发送方地址(RT_IF_UDP,from 可变)。
Datagram ToDatagram(const QNetworkDatagram& dg) {
  const QByteArray bytes = dg.data();
  Datagram out;
  out.bytes.assign(reinterpret_cast<const std::uint8_t*>(bytes.constData()),
                   reinterpret_cast<const std::uint8_t*>(bytes.constData()) +
                       bytes.size());
  out.source = Endpoint::Net(dg.senderAddress().toString().toStdString(),
                             static_cast<std::uint16_t>(dg.senderPort()));
  return out;
}

// 泵 fiber(ADR-0007 D1):外层管 socket 的创建/重建/重试,内层把收到的报文投入
// read_queue;退出后兼任收敛者。
void RunSocketPump(const StatePtr& state) {
  QUdpSocket* socket = state->socket;
  const auto read_channel = state->read_queue->channel();

  while (!IsClosing(state)) {
    if (socket->state() != QAbstractSocket::BoundState) {
      if (!BindOnce(state)) {
        // 退避——**不是**读超时:未 bind 上时不得建读流(见 State::close_signal)。
        // close_signal 被 Close 关闭时立即返回,故退避可提前打断。
        Coro::await_for(state->close_signal, kBindRetryInterval);
        continue;  // 无限重试,不自终(ADR-0007 D2)。
      }
    }
    // 通告"socket 就绪":写泵可能正停在阻塞点②等本信号(陈旧标记由它自行丢弃)。
    state->socket_ready->resolve();

    // 每代重建读流(旧流已随 close 死掉);建流时会 drain 订阅前已到达的报文,不丢首报。
    auto stream = Coro::coro(socket).receiveDatagram();
    for (;;) {
      // #156(静默超时)的取值点:配置为 0 时如下无限 await;非 0 时改
      // `Coro::await_for(stream, wait)`,超时即退出内层回外层重建。本轮不做。
      Coro::Result<QNetworkDatagram, std::error_code> datagram =
          Coro::await(stream);
      if (!datagram) {
        if (datagram.error().category() ==
            Coro::detail::socket_error_category()) {
          // socket 级 I/O 故障降为诊断事实(UDP 不自终):记 LastError 后回外层重建。
          const std::error_code mapped = MapSocketError(datagram.error());
          std::lock_guard<std::mutex> lock(state->mutex);
          state->last_error = mapped;
        }
        break;  // 流终止 / 被 Close 打断 → 回外层。
      }
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_recv = OperationOptions::Clock::now();
      }
      if (read_channel->push(ToDatagram(datagram.value())) !=
          boost::fibers::channel_op_status::success) {
        break;  // read_queue 已关闭(我方 Close)→ 停止投递,回外层判生命周期。
      }
    }
  }

  // 收敛(泵退出后兼任收敛者):先 join 写泵,确保它不再碰 socket,再关 read_queue、
  // 落 Closed、完成 closed。
  if (state->write_pump) {
    state->write_pump->get();
  }
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因。我方 Close 路径丢弃
  // 残留——改造前关闭后发起的读一律得 kClosed、取不到残留,此处与之等价。
  CloseDatagramQueue(state->read_queue,
                     make_error_code(TransportErrc::kClosed));
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->lifecycle = LifecycleState::kClosed;
  }
  state->closed.Complete(Status{});
}

// 实际发出一条报文。**不变式:状态检查到写出之间无挂起点**——writeDatagram 同步,故泵
// 不可能在中途重建 socket(只对 UDP 成立;有挂起点的介质须改用代际号校验)。
// 发送失败**不回传调用方**(fire-and-forget,ADR-0007 D3),只进 LastError()。
void SendOne(const StatePtr& state, QUdpSocket* socket, const SendUnit& unit) {
  const QHostAddress dest(QString::fromStdString(unit.destination.host));
  const qint64 n = socket->writeDatagram(
      reinterpret_cast<const char*>(unit.bytes.data()),
      static_cast<qint64>(unit.bytes.size()), dest, unit.destination.port);

  std::error_code failure;
  if (n < 0) {
    failure = MapSocketError(Coro::detail::socket_error_code(socket->error()));
  } else if (n != static_cast<qint64>(unit.bytes.size())) {
    // 报文未整发(过大被截断):报文语义下视为无效参数(RT_IF_UDP 一次一完整报文)。
    failure = make_error_code(TransportErrc::kInvalidArgument);
  }
  std::lock_guard<std::mutex> lock(state->mutex);
  if (failure) {
    state->last_error = failure;
  } else {
    state->last_send = OperationOptions::Clock::now();
  }
}

// 写泵 fiber(ADR-0007 D3):从 write_queue 取出并写 socket。两个阻塞点,**串行**
// (AsyncTask 无 select,也不需要多路等待)。单消费者天然保证写入串行化。
void RunWritePump(const StatePtr& state) {
  QUdpSocket* socket = state->socket;
  for (;;) {
    // ── 阻塞点①:等数据 ──(Close 关 write_queue 唤醒)
    Coro::Result<SendUnit, std::error_code> item =
        Coro::await(state->write_queue);
    if (!item) {
      return;  // 队列被关闭 → 退出(残留报文随之丢弃)。
    }
    for (;;) {
      // ── 阻塞点②:等 socket 就绪 ──(Close 关 socket_ready 唤醒)
      if (IsClosing(state)) {
        return;
      }
      if (socket->state() == QAbstractSocket::BoundState) {
        SendOne(state, socket, item.value());
        break;
      }
      // 清历史 resolve 的陈旧标记,再等下一次就绪通告。同线程协作(泵与写泵只在
      // await 点交错),故本行与 await 之间不会插入泵的 resolve,不丢唤醒。
      state->socket_ready->channel()->discard_pending();
      Coro::await(state->socket_ready);
    }
  }
}

}  // namespace

UdpTransport::UdpTransport(UdpConfig config)
    : state_(std::make_shared<State>()) {
  state_->config = std::move(config);
}

UdpTransport::~UdpTransport() {
  RequestClose();
  // join 泵:get() 在 fiber 内让出直至泵退出(泵内部已先 join 写泵),保证两条 detached
  // fiber 都不再触碰 socket / State。RequestClose 已四处打断,泵会迅速收敛。
  auto pump = state_->pump;
  if (pump) {
    pump->get();
  }
  if (state_->socket) {
    state_->socket->deleteLater();  // 整个生命期一个 socket,此处销毁。
  }
}

Status UdpTransport::Start() {
  const auto state = state_;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    if (state->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    // socket 在本 fiber(节点执行域)内创建,守亲和纪律(ADR-0003 D12/RT_IN_INTERFACE_005)。
    auto* socket = new QUdpSocket();
    // 显式禁用代理(#123):不继承 http_proxy/all_proxy 等环境级代理策略。
    socket->setProxy(QNetworkProxy::NoProxy);
    state->socket = socket;
    state->lifecycle = LifecycleState::kRunning;
  }

  // 首次 bind 就地做一次(不等泵被调度):`LocalPort()`/`CurrentLinkState()` 因此在
  // Start() 返回后即可如实观测。**失败不算启动失败**(ADR-0007 D2)——泵的首轮会立刻
  // 重试,随后按 kBindRetryInterval 无限退避重试。
  (void)BindOnce(state);

  // 起写泵与泵(句柄留在 State:泵收敛时 join 写泵,析构时 join 泵)。
  state->write_pump = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([state] { RunWritePump(state); }));
  state->pump = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([state] { RunSocketPump(state); }));
  return Status{};
}

// 交出 read_queue 句柄(ADR-0007 D4):不返回数据,deadline/取消/扇出由调用方在句柄上
// 自理。未 Start 时给一个以 kInvalidState 关闭的句柄(生命周期非法,await 立即得到它);
// 已关闭时 read_queue 已被泵以 kClosed 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> UdpTransport::Read() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    return ClosedDatagramQueue(make_error_code(TransportErrc::kInvalidState));
  }
  return state_->read_queue;
}

// 投入 write_queue 即返(ADR-0007 D3):**不等待实际发出**,返回值仅表示"已入队"。
// 发送失败不作为返回值,只进 LastError()。链路不可用时报文留在队列等恢复,不拒绝、
// 不丢弃;恢复后按序全部发出(接受对端可能收到过期数据,ADR-0007 D3 定案)。
Status UdpTransport::Write(SendUnit unit) {
  const auto state = state_;
  // 寻址(Endpoint 统一寻址语义)——**调用方参数**的合法性仍即时作答(与"发送失败不回传"
  // 无关:这是入队前就能判定的参数错误,不是 I/O 结果):
  //   kDefault → 解析为 config 默认目的地(UdpConfig.remote_addr / multicast_group +
  //     remote_port)。这让传输无关的调用方(如 ProtocolNode 恒发 Default)无缝跑在 UDP 上。
  //   kNet → 按 ip:port 发往不同地址(ADR-0003 D12)。
  //   其余(kTopic)→ 非法。
  if (unit.destination.kind == Endpoint::Kind::kDefault) {
    const std::string& host = state->config.mode == UdpMode::kMulticast
                                  ? state->config.multicast_group
                                  : state->config.remote_addr;
    if (host.empty() || state->config.remote_port == 0) {
      // config 未配默认目的地 → 无法解析 Default。
      return make_error_code(TransportErrc::kInvalidArgument);
    }
    unit.destination = Endpoint::Net(host, state->config.remote_port);
  } else if (unit.destination.kind != Endpoint::Kind::kNet) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  const QHostAddress dest(QString::fromStdString(unit.destination.host));
  if (dest.isNull()) {  // 非法目的地址(无法解析为 IP)。
    return make_error_code(TransportErrc::kInvalidArgument);
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
  }
  if (state->write_queue->channel()->push(std::move(unit)) !=
      boost::fibers::channel_op_status::success) {
    return make_error_code(TransportErrc::kClosed);  // 队列已关闭 = 传输终结。
  }
  return Status{};
}

// 请求关闭(幂等):**四处打断缺一不可**——泵可能停在退避或读等待,写泵可能停在两个
// 阻塞点之一,漏一处即一次收敛挂死。只发信号,不等收敛(收敛由泵自己跑完)。
Status UdpTransport::RequestClose() {
  const auto state = state_;
  bool never_started = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle >= LifecycleState::kClosing) {
      return Status{};  // 幂等。
    }
    if (state->lifecycle == LifecycleState::kCreated) {
      state->lifecycle = LifecycleState::kClosed;  // 从未 Start:无泵可停。
      never_started = true;
    } else {
      state->lifecycle = LifecycleState::kClosing;
    }
  }
  if (never_started) {
    CloseDatagramQueue(state->read_queue,
                       make_error_code(TransportErrc::kClosed));
    state->closed.Complete(Status{});
    return Status{};
  }

  const std::error_code closed = make_error_code(TransportErrc::kClosed);
  state->close_signal->close(closed);  // ① 打断"未 bind 时的退避"。
  state->socket->close();              // ② 打断活跃读流(实测有效)。
  state->write_queue->close(closed);   // ③ 唤醒写泵阻塞点①。
  // 已入队但未发出的报文随 write_queue 关闭而丢弃(设计 §8 用例 3 注)。
  state->write_queue->channel()->discard_pending();
  state->socket_ready->close(closed);  // ④ 唤醒写泵阻塞点②。
  return Status{};
}

Status UdpTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

std::uint16_t UdpTransport::LocalPort() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->local_port;
}

std::optional<UdpTransport::Clock::time_point> UdpTransport::LastSendTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_send;
}

std::optional<UdpTransport::Clock::time_point> UdpTransport::LastReceiveTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_recv;
}

std::error_code UdpTransport::LastError() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_error;
}

// UDP 的"链路可用"即 socket 此刻已绑定——判据只剩 `state() == BoundState`(socket 对象
// 整个生命期只一个)。bind 重试期间 Running 但未绑定 → kDown;UDP 无连接,故永不出现
// kEstablishing(退避是连接管理策略,不经本查询暴露)。
LinkState UdpTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle != LifecycleState::kRunning || !state_->socket) {
    return LinkState::kDown;
  }
  return state_->socket->state() == QAbstractSocket::BoundState
             ? LinkState::kUp
             : LinkState::kDown;
}

}  // namespace transport
