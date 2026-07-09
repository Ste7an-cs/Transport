#include "transport/tcp/TcpConnection.hpp"

#include <variant>

#include <QHostAddress>
#include <QTcpSocket>

namespace transport {

TcpConnection::TcpConnection(QTcpSocket* sock) : sock_(sock) {}
TcpConnection::~TcpConnection() { Close(); }

Status TcpConnection::Open() {
  peer_id_ = sock_->peerAddress().toString().toStdString() + ":" +
             std::to_string(sock_->peerPort());
  QObject::connect(sock_.get(), &QTcpSocket::readyRead, [this] { onReadyRead(); });
  QObject::connect(sock_.get(), &QTcpSocket::disconnected,
                   [this] { handleDisconnect("conn: disconnected"); });
  QObject::connect(sock_.get(),
                   QOverload<QAbstractSocket::SocketError>::of(&QAbstractSocket::errorOccurred),
                   [this] { handleDisconnect("conn: " + sock_->errorString().toStdString()); });
  open_ = true;
  if (connect_cb_) connect_cb_();
  return Status::Success(std::monostate{});
}

void TcpConnection::onReadyRead() {
  if (!sock_) return;
  QByteArray d = sock_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_)
    bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), peer_id_);
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes) {
  if (!open_ || !sock_) return Status::Fail("config: connection not open");
  sock_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size()));
  return Status::Success(std::monostate{});  // QTcpSocket 内部有写缓冲,不代表对端已收
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpConnection::handleDisconnect(const std::string& reason) {
  if (disconnected_) return;   // 一次性闸(disconnected + errorOccurred 只报一次)
  disconnected_ = true;
  open_ = false;
  if (disconnect_cb_) disconnect_cb_(reason);
}

void TcpConnection::Close() {
  disconnected_ = true;        // 主动关不算断线,不回 OnDisconnect
  open_ = false;
  if (sock_) { sock_->abort(); sock_.reset(); }
}

}  // namespace transport
