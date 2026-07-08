#include "transport/udp/UdpTransport.hpp"

#include <variant>

#include <QAbstractSocket>
#include <QNetworkDatagram>
#include <QUdpSocket>

namespace transport {

UdpTransport::UdpTransport(UdpConfig config) : config_(std::move(config)) {}
UdpTransport::~UdpTransport() { Close(); }

Status UdpTransport::Open() {
  sock_ = std::make_unique<QUdpSocket>();
  QHostAddress local;
  QUdpSocket::BindMode mode = QUdpSocket::DefaultForPlatform;
  if (config_.mode == UdpMode::kMulticast) {
    local = QHostAddress::AnyIPv4;
    mode = QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint;
  } else {
    if (!local.setAddress(QString::fromStdString(config_.local_addr)))
      return Status::Fail("config: invalid local_addr");
  }
  if (config_.mode == UdpMode::kBroadcast) mode = QUdpSocket::ReuseAddressHint;
  if (!sock_->bind(local, config_.local_port, mode))
    return Status::Fail("config: bind: " + sock_->errorString().toStdString());

  if (config_.mode == UdpMode::kMulticast) {
    QHostAddress group(QString::fromStdString(config_.multicast_group));
    if (group.isNull()) return Status::Fail("config: invalid multicast_group");
    if (!sock_->joinMulticastGroup(group))
      return Status::Fail("config: join_group: " + sock_->errorString().toStdString());
    sock_->setSocketOption(QAbstractSocket::MulticastTtlOption, int(config_.ttl));
    sock_->setSocketOption(QAbstractSocket::MulticastLoopbackOption, 1);
    dest_addr_ = group;
    dest_port_ = config_.remote_port;
  } else if (!config_.remote_addr.empty()) {
    if (!dest_addr_.setAddress(QString::fromStdString(config_.remote_addr)))
      return Status::Fail("config: invalid remote_addr");
    dest_port_ = config_.remote_port;
  }

  local_port_ = sock_->localPort();
  QObject::connect(sock_.get(), &QUdpSocket::readyRead, [this] { onReadyRead(); });
  open_ = true;
  if (connect_cb_) connect_cb_();  // UDP 无连接,Open 成功即视作已连
  return Status::Success(std::monostate{});
}

void UdpTransport::onReadyRead() {
  while (sock_ && sock_->hasPendingDatagrams()) {
    QNetworkDatagram dg = sock_->receiveDatagram();
    QByteArray d = dg.data();
    std::vector<uint8_t> bytes(d.begin(), d.end());
    std::string from = dg.senderAddress().toString().toStdString() + ":" +
                       std::to_string(dg.senderPort());
    if (bytes_cb_)
      bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), from);
  }
}

Status UdpTransport::sendTo(const std::vector<uint8_t>& bytes, const QHostAddress& addr, quint16 port) {
  if (!open_ || !sock_) return Status::Fail("config: socket not open");
  QByteArray d(reinterpret_cast<const char*>(bytes.data()), int(bytes.size()));
  if (sock_->writeDatagram(d, addr, port) < 0)
    return Status::Fail("io: send: " + sock_->errorString().toStdString());
  return Status::Success(std::monostate{});
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes) {
  return sendTo(bytes, dest_addr_, dest_port_);
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return sendTo(bytes, dest_addr_, dest_port_);
    case Endpoint::Kind::kNet: {
      QHostAddress a;
      if (!a.setAddress(QString::fromStdString(to.host)))
        return Status::Fail("config: invalid address");
      return sendTo(bytes, a, static_cast<quint16>(to.port));
    }
    case Endpoint::Kind::kTopic:
      return Status::Fail("config: udp expects net endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

void UdpTransport::Close() {
  open_ = false;
  if (sock_) { sock_->close(); sock_.reset(); }  // 析构断开所有 connect
}

}  // namespace transport
