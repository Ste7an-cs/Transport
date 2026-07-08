#include "transport/tcp/TcpServerTransport.hpp"

#include <algorithm>
#include <variant>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "transport/tcp/TcpConnection.hpp"

namespace transport {

TcpServerTransport::TcpServerTransport(TcpServerConfig config) : config_(std::move(config)) {}
TcpServerTransport::~TcpServerTransport() { Close(); }

Status TcpServerTransport::Open() {
  server_ = std::make_unique<QTcpServer>();
  if (config_.backlog > 0) server_->setMaxPendingConnections(config_.backlog);
  QHostAddress addr;
  if (!addr.setAddress(QString::fromStdString(config_.bind_addr)))
    return Status::Fail("config: invalid bind_addr");
  if (!server_->listen(addr, config_.port))
    return Status::Fail("io: listen: " + server_->errorString().toStdString());
  local_port_ = server_->serverPort();
  QObject::connect(server_.get(), &QTcpServer::newConnection, [this] { onNewConnection(); });
  open_ = true;
  return Status::Success(std::monostate{});
}

void TcpServerTransport::onNewConnection() {
  while (server_ && server_->hasPendingConnections()) {
    QTcpSocket* s = server_->nextPendingConnection();  // 默认归 server 所有(child)
    s->setParent(nullptr);                             // 解除父子,交 TcpConnection 独占(防双删)
    auto conn = std::make_shared<TcpConnection>(s);
    // 清理失效 weak
    conns_.erase(std::remove_if(conns_.begin(), conns_.end(),
                   [](const std::weak_ptr<TcpConnection>& w){ return w.expired(); }), conns_.end());
    conns_.push_back(conn);                 // 只存 weak(保活是用户的事)
    if (accept_cb_) accept_cb_(conn);       // 用户须在回调里 Open + 保活
  }
}

void TcpServerTransport::Close() {
  open_ = false;
  if (server_) { server_->close(); server_.reset(); }
  for (auto& w : conns_) if (auto c = w.lock()) c->Close();
  conns_.clear();
}

}  // namespace transport
