#include "AcceptedTcpTransport.hpp"

#include <utility>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/corosocket.hpp"
#include "await/corotcpserver.hpp"

#include "transport/core/Error.hpp"

namespace perf {
namespace {

using transport::Datagram;
using transport::Endpoint;
using transport::LifecycleState;
using transport::LinkState;
using transport::TransportErrc;
using transport::make_error_code;

}  // namespace

AcceptedTcpTransport::AcceptedTcpTransport(std::string bind_addr,
                                           std::uint16_t port)
    : bind_addr_(std::move(bind_addr)), port_(port) {}

AcceptedTcpTransport::~AcceptedTcpTransport() {
  (void)Close();
  WaitClosed();
}

Coro::Result<void> AcceptedTcpTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};
  }
  if (lifecycle_ >= LifecycleState::kClosing) {
    return make_error_code(TransportErrc::kInvalidState);
  }

  server_ = std::make_unique<QTcpServer>();
  QHostAddress address;
  if (!address.setAddress(QString::fromStdString(bind_addr_))) {
    last_error_ = make_error_code(TransportErrc::kConfiguration);
    server_.reset();
    return last_error_;
  }
  if (!server_->listen(address, port_)) {
    last_error_ = make_error_code(TransportErrc::kConnection);
    server_.reset();
    return last_error_;
  }

  incoming_ = Coro::coro(server_.get()).nextConnection();
  lifecycle_ = LifecycleState::kRunning;
  accept_task_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunAcceptLoop(); }));
  return Coro::Result<void>{};
}

Coro::Result<void> AcceptedTcpTransport::Close() {
  if (lifecycle_ == LifecycleState::kCreated) {
    lifecycle_ = LifecycleState::kClosed;
    return Coro::Result<void>{};
  }
  if (lifecycle_ >= LifecycleState::kClosing) {
    return Coro::Result<void>{};
  }
  lifecycle_ = LifecycleState::kClosing;

  // 关掉两条在途等待:accept 流与当前读流。二者的 fiber 随即得到终止错误而退出。
  const std::error_code closed = make_error_code(TransportErrc::kClosed);
  if (incoming_) {
    incoming_->close(closed);
  }
  if (read_stream_) {
    read_stream_->close(closed);
  }
  if (server_) {
    server_->close();
  }
  return Coro::Result<void>{};
}

void AcceptedTcpTransport::WaitClosed() {
  if (lifecycle_ == LifecycleState::kClosed) {
    return;
  }
  if (accept_task_) {
    (void)accept_task_->get();
    accept_task_.reset();
  }
  for (auto& task : read_tasks_) {
    if (task) {
      (void)task->get();
    }
  }
  read_tasks_.clear();
  transport::CloseQueue(read_queue_, make_error_code(TransportErrc::kClosed));
  socket_ = nullptr;
  read_stream_.reset();
  incoming_.reset();
  server_.reset();
  lifecycle_ = LifecycleState::kClosed;
}

std::shared_ptr<Coro::Awaitable<Datagram>> AcceptedTcpTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    return transport::ClosedQueue<Datagram>(
        make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

Coro::Result<void> AcceptedTcpTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ >= LifecycleState::kClosing) {
    return make_error_code(TransportErrc::kClosed);
  }
  // `peer` 被忽略:TCP 点对点,一律发往当前这条已接受连接(与 `TcpTransport` D8 同形)。
  if (socket_ == nullptr ||
      socket_->state() != QAbstractSocket::ConnectedState) {
    // 写侧 fire-and-forget(ADR-0007 D3):没有对端就丢掉,只落 `LastError()`、不回传。
    last_error_ = make_error_code(TransportErrc::kConnection);
    return Coro::Result<void>{};
  }
  socket_->write(reinterpret_cast<const char*>(datagram.bytes.data()),
                 static_cast<qint64>(datagram.bytes.size()));
  return Coro::Result<void>{};
}

std::error_code AcceptedTcpTransport::LastError() const { return last_error_; }

LinkState AcceptedTcpTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning) {
    return LinkState::kDown;
  }
  if (socket_ != nullptr &&
      socket_->state() == QAbstractSocket::ConnectedState) {
    return LinkState::kUp;
  }
  return LinkState::kEstablishing;
}

std::uint16_t AcceptedTcpTransport::LocalPort() const {
  return server_ ? static_cast<std::uint16_t>(server_->serverPort()) : 0;
}

void AcceptedTcpTransport::RunAcceptLoop() {
  for (;;) {
    auto accepted = Coro::await(incoming_);
    if (!accepted) {
      break;  // 我方 Close,或 server 停止监听 / 销毁。
    }
    QTcpSocket* socket = accepted.value();
    if (socket == nullptr) {
      continue;
    }
    socket_ = socket;
    // 每条连接一条读 fiber。旧连接的读 fiber 随其流自然终止而退出,不必显式打断。
    read_tasks_.push_back(std::make_shared<Coro::FiberTask<void>>(
        Coro::makeTask([this, socket] { RunReadLoop(socket); })));
    if (lifecycle_ >= LifecycleState::kClosing) {
      break;
    }
  }
}

void AcceptedTcpTransport::RunReadLoop(QTcpSocket* socket) {
  auto stream = Coro::coro(socket).readAll();
  read_stream_ = stream;  // 供 `Close()` 打断当前这条。
  const Endpoint peer = Endpoint::Net(socket->peerAddress().toString().toStdString(),
                                      static_cast<std::uint16_t>(socket->peerPort()));
  while (lifecycle_ < LifecycleState::kClosing) {
    auto chunk = Coro::await(stream);
    if (!chunk) {
      break;  // 对端断开 / 我方 Close。
    }
    const QByteArray& bytes = chunk.value();
    const auto* first = reinterpret_cast<const std::uint8_t*>(bytes.constData());
    Datagram out{{first, first + bytes.size()}, peer};
    if (read_queue_->channel()->push(std::move(out)) !=
        boost::fibers::channel_op_status::success) {
      break;  // 读队列已关闭。
    }
  }
  if (socket_ == socket) {
    socket_ = nullptr;
  }
  if (read_stream_ == stream) {
    read_stream_.reset();
  }
}

}  // namespace perf
