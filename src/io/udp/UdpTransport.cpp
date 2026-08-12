#include "transport/io/udp/UdpTransport.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QNetworkDatagram>
#include <QPointer>
#include <QUdpSocket>

#include "await/awaitable.hpp"
#include "await/coroudpsocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"  // Coro::makeTask —— 数据泵 fiber。
#include "transport/core/Error.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace transport {
namespace {

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

struct UdpTransport::State {
  mutable std::mutex mutex;
  UdpConfig config;
  QPointer<QUdpSocket> socket;
  // 唯一的 receiveDatagram 流:持有一条、反复 await 取下一条完整报文(报文边界保持)。
  std::shared_ptr<Coro::Awaitable<QNetworkDatagram>> read_stream;
  // 对外 read_queue(ADR-0007 D1/D4):数据泵是唯一生产者,`Read()` 只交出本句柄。
  // 整个生命周期只此一条(构造即建),故 Read() 恒能给出非空句柄。容量策略未定
  // (TBD-009):沿用 AsyncTask 默认(有界 1024、丢最旧),本轮不处置(#152)。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  LifecycleState lifecycle{LifecycleState::kCreated};
  std::uint16_t local_port{0};
  std::optional<Clock::time_point> last_send;
  std::optional<Clock::time_point> last_recv;
  std::error_code last_error;
  SharedCompletion<void> closed;
};

namespace {

// 关闭一次(幂等):关接收流(令数据泵退出)、以 kClosed 关 read_queue 唤醒全部读者,
// 并落 Closed、完成 closed。UDP 无连接、无写槽,读者不再持有"在途读"这一状态(单读
// 守卫已随 ADR-0007 D4 删除),故 Closing 只是本函数内的瞬时相位、一步收敛到 Closed
// ——与改造前"无在途读时 BeginClose 直接落 Closed"的常态路径等价。
void BeginClose(const std::shared_ptr<UdpTransport::State>& state,
                bool discard_residual = true) {
  std::shared_ptr<Coro::Awaitable<QNetworkDatagram>> read_stream;
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosed) {
      return;
    }
    read_stream = state->read_stream;
    read_queue = state->read_queue;
    state->lifecycle = LifecycleState::kClosed;
  }
  if (read_stream) {
    read_stream->close(make_error_code(TransportErrc::kClosed));  // 令数据泵退出。
  }
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因,调用方 await 得到它。
  // discard_residual:我方 Close 路径丢弃残留(改造前关闭后发起的读一律得 kClosed);
  // 泵因链路终结而收敛的路径不丢——残留先被取尽,再观察到终止原因(同改造前)。
  if (discard_residual) {
    CloseDatagramQueue(read_queue, make_error_code(TransportErrc::kClosed));
  } else {
    read_queue->close(make_error_code(TransportErrc::kClosed));
  }
  state->closed.Complete(Status{});
}

// 数据泵(ADR-0007 D1 内层循环):反复 await 接收流,把每条完整报文转成 Datagram 投入
// read_queue,直至流终止。UDP 尚未改成"外层 socket 管理循环 + 无限重试"(留给后续票),
// 故流终止即传输终结:关 read_queue 并 Closing→Closed,与改造前 `Read` 的收敛一致。
void RunReadPump(const std::shared_ptr<UdpTransport::State>& state,
                 const std::shared_ptr<Coro::Awaitable<QNetworkDatagram>>& stream,
                 const std::shared_ptr<Coro::Awaitable<Datagram>>& read_queue) {
  const auto channel = read_queue->channel();
  for (;;) {
    Coro::Result<QNetworkDatagram, std::error_code> datagram =
        Coro::await(stream);
    if (datagram) {
      const QNetworkDatagram& dg = datagram.value();
      const QByteArray bytes = dg.data();
      Datagram out;
      out.bytes.assign(
          reinterpret_cast<const std::uint8_t*>(bytes.constData()),
          reinterpret_cast<const std::uint8_t*>(bytes.constData()) +
              bytes.size());
      // from 可变:source 填每条报文的发送方地址(RT_IF_UDP,与 TCP 恒对端相反)。
      out.source = Endpoint::Net(dg.senderAddress().toString().toStdString(),
                                 static_cast<std::uint16_t>(dg.senderPort()));
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_recv = OperationOptions::Clock::now();
      }
      if (channel->push(std::move(out)) !=
          boost::fibers::channel_op_status::success) {
        return;  // read_queue 已关闭(我方 Close)→ 停止投递。
      }
      continue;
    }

    if (datagram.error().category() == Coro::detail::socket_error_category()) {
      // socket 级致命 I/O → 非重连介质,链路终结即传输终结:read_queue 以 kClosed
      // 收敛(RT_TRANSPORT_008 / ADR-0004 D1 经 ADR-0007 D4 改写终止表达),底层成因
      // (kConnection/kIo)降为诊断事实留在 LastError();并 Closing→Closed。
      // 注意:单次发送的可恢复失败(报文过大等)由 Write 自行返回,不经本路径。
      const std::error_code mapped = MapSocketError(datagram.error());
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_error = mapped;
      }
    }
    // 流终止:我方关闭、或底层正常终止(socket 失效)。二者都是传输终结,对调用方同一
    // 含义 → read_queue 以 kClosed 关闭(BeginClose 幂等,已关闭时直接返回)。残留不丢:
    // 消费者先取尽已投入的报文,再观察到终止原因(同改造前直接 await 接收流的语义)。
    BeginClose(state, /*discard_residual=*/false);
    return;
  }
}

}  // namespace

UdpTransport::UdpTransport(UdpConfig config)
    : state_(std::make_shared<State>()) {
  state_->config = std::move(config);
}

UdpTransport::~UdpTransport() {
  BeginClose(state_);
  // socket 由 State 持有(QPointer);此处请求删除,coroudpsocket 内部用 QPointer
  // 防悬空。detached 的收尾 fiber 也持有 State,故 socket 存活至最后一个引用释放。
  if (state_->socket) {
    state_->socket->deleteLater();
  }
}

Status UdpTransport::Start() {
  const auto state = state_;
  std::shared_ptr<Coro::Awaitable<QNetworkDatagram>> stream;
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    if (state->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }

    // socket 在本 fiber(节点执行域)内创建并绑定,守亲和纪律(ADR-0003 D12/RT_IN_INTERFACE_005)。
    auto* socket = new QUdpSocket();
    const QHostAddress bind_addr(
        QString::fromStdString(state->config.local_addr));
    // ShareAddress|ReuseAddressHint:组播/多消费者场景可共享绑定;单播 loopback 无碍。
    const QAbstractSocket::BindMode bind_mode =
        state->config.mode == UdpMode::kMulticast
            ? (QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint)
            : QUdpSocket::DefaultForPlatform;
    if (!socket->bind(bind_addr, state->config.local_port, bind_mode)) {
      std::error_code bind_error = MapSocketError(
          Coro::detail::socket_error_code(socket->error()));
      if (!bind_error) {
        bind_error = make_error_code(TransportErrc::kIo);
      }
      state->last_error = bind_error;
      socket->deleteLater();
      return Status{bind_error};
    }

    if (state->config.mode == UdpMode::kMulticast &&
        !state->config.multicast_group.empty()) {
      const QHostAddress group(
          QString::fromStdString(state->config.multicast_group));
      socket->setSocketOption(QAbstractSocket::MulticastTtlOption,
                              static_cast<int>(state->config.ttl));
      socket->joinMulticastGroup(group);
    }

    state->socket = socket;
    state->local_port = socket->localPort();
    // 建立唯一接收流(持有一条、反复 await);初始 drain 收下订阅前已到达的报文,不丢首报。
    state->read_stream = Coro::coro(socket).receiveDatagram();
    state->lifecycle = LifecycleState::kRunning;
    stream = state->read_stream;
    read_queue = state->read_queue;
  }

  // 起数据泵(ADR-0007 D1):socket 已就绪,泵在本执行域 fiber 内反复取报文投 read_queue。
  // 句柄不留存:泵只触碰以 shared_ptr 持有的 State / 流 / 队列,故本类析构后仍安全收敛。
  Coro::makeTask([state, stream, read_queue] {
    RunReadPump(state, stream, read_queue);
  });
  return Status{};
}

// 交出 read_queue 句柄(ADR-0007 D4):不返回数据,deadline/取消/扇出由调用方在句柄上
// 自理。未 Start 时给一个以 kInvalidState 关闭的句柄(生命周期非法,await 立即得到它);
// 关闭中/已关闭时 read_queue 已被 BeginClose 以 kClosed 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> UdpTransport::Read() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    return ClosedDatagramQueue(make_error_code(TransportErrc::kInvalidState));
  }
  return state_->read_queue;
}

Status UdpTransport::Write(SendUnit unit) {
  const auto state = state_;
  // 寻址(Endpoint 统一寻址语义):
  //   kDefault → 解析为 config 默认目的地(Endpoint::kDefault = "用 config 默认目的地";
  //     UdpConfig.remote_addr / multicast_group + remote_port)。这让传输无关的调用方
  //     (如 ProtocolNode 恒发 Default)无缝跑在 UDP 上,由 UdpConfig 提供对端地址。
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

  QPointer<QUdpSocket> socket;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    socket = state->socket;
  }
  if (!socket) {
    return make_error_code(TransportErrc::kClosed);
  }

  const QHostAddress dest(QString::fromStdString(unit.destination.host));
  if (dest.isNull()) {  // 非法目的地址(无法解析为 IP)。
    return make_error_code(TransportErrc::kInvalidArgument);
  }

  // writeDatagram 同步非阻塞:整报文原子进入操作系统发送缓冲或失败,无短写/背压。
  const qint64 n = socket->writeDatagram(
      reinterpret_cast<const char*>(unit.bytes.data()),
      static_cast<qint64>(unit.bytes.size()), dest, unit.destination.port);
  if (n < 0) {
    std::error_code mapped =
        MapSocketError(Coro::detail::socket_error_code(socket->error()));
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    return Status{mapped};
  }
  if (n != static_cast<qint64>(unit.bytes.size())) {
    // 报文未整发(过大被截断):报文语义下视为无效参数(RT_IF_UDP 一次一完整报文)。
    std::error_code mapped = make_error_code(TransportErrc::kInvalidArgument);
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    return Status{mapped};
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_send = Clock::now();
  }
  return Status{};
}

Status UdpTransport::RequestClose() {
  BeginClose(state_);
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

// UDP 的"链路可用"即 socket 已绑定:Start 只在 bind 成功后落 Running 并置 socket,
// 故 Running + socket 非空是充要判据(bind 失败不进 Running)。
LinkState UdpTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return (state_->lifecycle == LifecycleState::kRunning && state_->socket)
             ? LinkState::kUp
             : LinkState::kDown;
}

}  // namespace transport
