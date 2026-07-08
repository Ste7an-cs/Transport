#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <variant>

#include <QHostAddress>
#include <QTcpSocket>
#include <QTimer>

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : config_(std::move(config)), backoff_base_(backoff_base), backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base) {}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  connect_timer_ = std::make_unique<QTimer>();  connect_timer_->setSingleShot(true);
  reconnect_timer_ = std::make_unique<QTimer>(); reconnect_timer_->setSingleShot(true);
  QObject::connect(reconnect_timer_.get(), &QTimer::timeout, [this] { startConnect(); });
  // connect_timer_ 的 timeout 只连一次(在这里),否则每次 startConnect 重连会累积重复 slot,
  // 一次超时触发全部累积 lambda → 退避被乘性推进。sock_ 是成员,重连换 socket 不影响本连接。
  QObject::connect(connect_timer_.get(), &QTimer::timeout, [this] {
    if (sock_ && sock_->state() != QAbstractSocket::ConnectedState) {
      sock_->abort();
      onDisconnected("timeout: connect timed out");
    }
  });
  open_ = true;
  startConnect();
  return Status::Success(std::monostate{});  // 连接结果经 OnConnect/OnDisconnect 上报
}

void TcpClientTransport::startConnect() {
  if (closing_) return;
  sock_ = std::make_unique<QTcpSocket>();
  QObject::connect(sock_.get(), &QTcpSocket::connected, [this] { onConnected(); });
  QObject::connect(sock_.get(), &QTcpSocket::readyRead, [this] { onReadyRead(); });
  QObject::connect(sock_.get(), &QTcpSocket::disconnected,
                   [this] { onDisconnected("conn: disconnected"); });
  QObject::connect(sock_.get(),
                   QOverload<QAbstractSocket::SocketError>::of(&QAbstractSocket::errorOccurred),
                   [this] { onDisconnected("conn: " + sock_->errorString().toStdString()); });
  connect_timer_->start(int(config_.connect_timeout_ms));
  sock_->connectToHost(QString::fromStdString(config_.host), config_.port);
}

void TcpClientTransport::onConnected() {
  connect_timer_->stop();
  backoff_cur_ = backoff_base_;
  peer_id_ = sock_->peerAddress().toString().toStdString() + ":" + std::to_string(sock_->peerPort());
  if (connect_cb_) connect_cb_();
}

void TcpClientTransport::onReadyRead() {
  if (!sock_) return;
  QByteArray d = sock_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_) bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), peer_id_);
}

void TcpClientTransport::onDisconnected(const std::string& reason) {
  connect_timer_->stop();
  if (sock_) { sock_->disconnect(); }  // 断开所有 connect,防重入
  if (config_.auto_reconnect && !closing_) {
    scheduleReconnect();               // 自动重连:不打扰用户
  } else if (disconnect_cb_) {
    disconnect_cb_(reason);
  }
}

void TcpClientTransport::scheduleReconnect() {
  if (closing_ || !config_.auto_reconnect) return;
  reconnect_timer_->start(int(backoff_cur_.count()));
  backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!sock_ || sock_->state() != QAbstractSocket::ConnectedState)
    return Status::Fail("config: tcp not open");
  sock_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size()));
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::Close() {
  closing_ = true;
  open_ = false;
  if (connect_timer_) connect_timer_->stop();
  if (reconnect_timer_) reconnect_timer_->stop();
  if (sock_) { sock_->abort(); sock_.reset(); }
}

}  // namespace transport
