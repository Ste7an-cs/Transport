#include "transport/UdpTransport.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QNetworkDatagram>
#include <QPointer>
#include <QUdpSocket>

#include "await/coroudpsocket.hpp"
#include "await/detail/socketerror.hpp"
#include "transport/Error.hpp"
#include "transport/SharedCompletion.hpp"

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
  LifecycleState lifecycle{LifecycleState::kCreated};
  std::uint16_t local_port{0};
  bool active_read{false};
  std::optional<Clock::time_point> last_send;
  std::optional<Clock::time_point> last_recv;
  std::error_code last_error;
  SharedCompletion<void> closed;
};

namespace {

// 关闭一次:进入 Closing、关接收流唤醒在途读者;无在途读则直接落 Closed 并完成
// closed。有在途读则由其收尾(FinishRead)完成。UDP 无连接、无写槽,故只协调读侧。
void BeginClose(const std::shared_ptr<UdpTransport::State>& state) {
  std::shared_ptr<Coro::Awaitable<QNetworkDatagram>> read_stream;
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosed) {
      return;
    }
    state->lifecycle = LifecycleState::kClosing;
    read_stream = state->read_stream;
    if (!state->active_read) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (read_stream) {
    read_stream->close(make_error_code(TransportErrc::kClosed));  // 唤醒在途 Read。
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

void FinishRead(const std::shared_ptr<UdpTransport::State>& state) {
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->active_read = false;
    if (state->lifecycle == LifecycleState::kClosing) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (complete) {
    state->closed.Complete(Status{});
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
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kRunning) {
    return Status{};
  }
  if (state_->lifecycle != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }

  // socket 在本 fiber(节点执行域)内创建并绑定,守亲和纪律(ADR-0003 D12/RT_IN_INTERFACE_005)。
  auto* socket = new QUdpSocket();
  const QHostAddress bind_addr(
      QString::fromStdString(state_->config.local_addr));
  // ShareAddress|ReuseAddressHint:组播/多消费者场景可共享绑定;单播 loopback 无碍。
  const QAbstractSocket::BindMode bind_mode =
      state_->config.mode == UdpMode::kMulticast
          ? (QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint)
          : QUdpSocket::DefaultForPlatform;
  if (!socket->bind(bind_addr, state_->config.local_port, bind_mode)) {
    std::error_code bind_error = MapSocketError(
        Coro::detail::socket_error_code(socket->error()));
    if (!bind_error) {
      bind_error = make_error_code(TransportErrc::kIo);
    }
    state_->last_error = bind_error;
    socket->deleteLater();
    return Status{bind_error};
  }

  if (state_->config.mode == UdpMode::kMulticast &&
      !state_->config.multicast_group.empty()) {
    const QHostAddress group(
        QString::fromStdString(state_->config.multicast_group));
    socket->setSocketOption(QAbstractSocket::MulticastTtlOption,
                            static_cast<int>(state_->config.ttl));
    socket->joinMulticastGroup(group);
  }

  state_->socket = socket;
  state_->local_port = socket->localPort();
  // 建立唯一接收流(持有一条、反复 await);初始 drain 收下订阅前已到达的报文,不丢首报。
  state_->read_stream = Coro::coro(socket).receiveDatagram();
  state_->lifecycle = LifecycleState::kRunning;
  return Status{};
}

Result<Datagram> UdpTransport::Read(OperationOptions options) {
  const auto state = state_;
  std::shared_ptr<Coro::Awaitable<QNetworkDatagram>> stream;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    if (state->active_read) {  // 单读:同一时刻至多一个有效读(RT_TRANSPORT_004)。
      return make_error_code(TransportErrc::kInvalidState);
    }
    state->active_read = true;
    stream = state->read_stream;
  }

  if (!stream) {
    FinishRead(state);
    return make_error_code(TransportErrc::kInternal);
  }

  // 复用持有的接收流:每次 Read await 下一条完整报文(channel FIFO 保序)。读侧为
  // 最小能力:以 deadline 界定单次读,超时不停流、可再读;要中断在途 Read 请关闭传输。
  Coro::Result<QNetworkDatagram, std::error_code> datagram =
      options.deadline
          ? Coro::await_for(stream, *options.deadline - Clock::now())
          : Coro::await(stream);

  Result<Datagram> result{make_error_code(TransportErrc::kInternal)};
  if (datagram) {
    const QNetworkDatagram& dg = datagram.value();
    const QByteArray bytes = dg.data();
    Datagram out;
    out.bytes.assign(
        reinterpret_cast<const std::uint8_t*>(bytes.constData()),
        reinterpret_cast<const std::uint8_t*>(bytes.constData()) + bytes.size());
    // from 可变:source 填每条报文的发送方地址(RT_IF_UDP,与 TCP 恒对端相反)。
    out.source = Endpoint::Net(dg.senderAddress().toString().toStdString(),
                               static_cast<std::uint16_t>(dg.senderPort()));
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_recv = Clock::now();
    }
    result = Result<Datagram>{std::move(out)};
    FinishRead(state);
    return result;
  }

  if (datagram.error() == std::make_error_code(std::errc::timed_out)) {
    // 超时不停流、不改生命周期:可再次 Read。
    FinishRead(state);
    return make_error_code(TransportErrc::kTimeout);
  }

  if (datagram.error().category() == Coro::detail::socket_error_category()) {
    // 底层致命 I/O → 非重连:记录事实并 Closing→Closed(ADR-0002 D3′)。
    std::error_code mapped = MapSocketError(datagram.error());
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    result = mapped;
    FinishRead(state);
    BeginClose(state);
    return result;
  }

  // 流无错误关闭:我方关闭 → Closed;否则底层正常终止(socket 失效)→ Connection。
  bool closing;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    closing = state->lifecycle != LifecycleState::kRunning;
  }
  result = make_error_code(closing ? TransportErrc::kClosed
                                   : TransportErrc::kConnection);
  FinishRead(state);
  if (!closing) {
    BeginClose(state);
  }
  return result;
}

Status UdpTransport::Write(SendUnit unit) {
  const auto state = state_;
  // destination 必须是 kNet(UDP 按 ip:port 寻址,发往不同地址,ADR-0003 D12)。
  if (unit.destination.kind != Endpoint::Kind::kNet) {
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

}  // namespace transport
