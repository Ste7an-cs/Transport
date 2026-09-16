#include "ListeningTcpTransport.hpp"

#include <utility>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/corosocket.hpp"
#include "await/corotcpserver.hpp"

#include "transport/core/Error.hpp"

namespace example {
namespace {

using transport::Datagram;
using transport::Endpoint;
using transport::LifecycleState;
using transport::LinkState;
using transport::TransportErrc;
using transport::make_error_code;

}  // namespace

ListeningTcpTransport::ListeningTcpTransport(std::string bind_addr,
                                             std::uint16_t port)
    : bind_addr_(std::move(bind_addr)), port_(port) {}

ListeningTcpTransport::~ListeningTcpTransport() {
  // 传输也和节点一样:析构里 `Close()` + `WaitClosed()`,返回即没有 fiber 再碰本对象。
  (void)Close();
  WaitClosed();
}

Coro::Result<void> ListeningTcpTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};  // 幂等
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
  // `Start()` 起内部 fiber 后【即返回】,不等对端连上(与库内四种传输同形)。
  accept_task_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunAcceptLoop(); }));
  return Coro::Result<void>{};
}

Coro::Result<void> ListeningTcpTransport::Close() {
  if (lifecycle_ == LifecycleState::kCreated) {
    lifecycle_ = LifecycleState::kClosed;  // 没起过,没有 fiber 要汇合
    return Coro::Result<void>{};
  }
  if (lifecycle_ >= LifecycleState::kClosing) {
    return Coro::Result<void>{};  // 幂等
  }
  lifecycle_ = LifecycleState::kClosing;

  // ★ **只发信号,一个等待点都没有**:关掉两条在途等待(accept 流与当前读流),
  //   两条 fiber 随即得到终止错误而退出。真正的等待属于 `WaitClosed()`。
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

void ListeningTcpTransport::WaitClosed() {
  if (lifecycle_ == LifecycleState::kClosed) {
    return;
  }
  if (accept_task_) {
    (void)accept_task_->get();  // 让出式 join
    accept_task_.reset();
  }
  for (auto& task : read_tasks_) {
    if (task) {
      (void)task->get();
    }
  }
  read_tasks_.clear();
  // 我方关闭路径:关队列**并丢弃残留**,才与"关闭即停止交付"等价。
  transport::CloseQueue(read_queue_, make_error_code(TransportErrc::kClosed));
  socket_ = nullptr;
  read_stream_.reset();
  incoming_.reset();
  server_.reset();
  lifecycle_ = LifecycleState::kClosed;
}

std::shared_ptr<Coro::Awaitable<Datagram>> ListeningTcpTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    // 读侧是**句柄式**的,没有返回错误码的位置 —— 把错误作为队列的终止原因交出去。
    return transport::ClosedQueue<Datagram>(
        make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

Coro::Result<void> ListeningTcpTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ >= LifecycleState::kClosing) {
    return make_error_code(TransportErrc::kClosed);
  }
  // ★ `peer` **被忽略**:TCP 点对点,一律发往当前这条已接受连接(与 `TcpTransport`
  //   的 ADR-0011 D8 同形)。这正是 examples/tcp_server 里 `rsp.endpoint = req.endpoint`
  //   在 TCP 上"填了也不起作用、但照着写总是对的"的那一头。
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

std::error_code ListeningTcpTransport::LastError() const { return last_error_; }

LinkState ListeningTcpTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning) {
    return LinkState::kDown;
  }
  if (socket_ != nullptr &&
      socket_->state() == QAbstractSocket::ConnectedState) {
    return LinkState::kUp;
  }
  return LinkState::kEstablishing;  // 在听,还没人连上来
}

std::uint16_t ListeningTcpTransport::LocalPort() const {
  return server_ ? static_cast<std::uint16_t>(server_->serverPort()) : 0;
}

void ListeningTcpTransport::RunAcceptLoop() {
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

void ListeningTcpTransport::RunReadLoop(QTcpSocket* socket) {
  auto stream = Coro::coro(socket).readAll();
  read_stream_ = stream;  // 供 `Close()` 打断当前这条。
  // 入站 `peer` 是**发送方**(ADR-0020 D3);`ProtocolNode` 会把它填进
  // `Message::endpoint`,于是 examples/tcp_server 打印得出"请求从哪儿来"。
  const Endpoint peer =
      Endpoint::Net(socket->peerAddress().toString().toStdString(),
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

}  // namespace example
