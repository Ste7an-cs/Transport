#include "transport/tcp/TcpServerImpl.hpp"

#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

TcpServerImpl::TcpServerImpl(TcpServerConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      acceptor_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpServerImpl::~TcpServerImpl() { Close(); }

Status TcpServerImpl::Open() {
  if (config_.framer) {  // framer 配置校验（spec：非法则 config: 错误）
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }
  asio::error_code ec;
  auto addr = asio::ip::make_address(config_.bind_addr, ec);
  if (ec) return Status::Fail("config: invalid bind_addr");
  asio::ip::tcp::endpoint ep(addr, config_.port);

  acceptor_.open(ep.protocol(), ec);
  if (ec) return Status::Fail("conn: open: " + ec.message());
  acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
  acceptor_.bind(ep, ec);
  if (ec) return Status::Fail("conn: bind: " + ec.message());
  acceptor_.listen(asio::socket_base::max_listen_connections, ec);
  if (ec) return Status::Fail("conn: listen: " + ec.message());

  local_port_ = acceptor_.local_endpoint().port();
  open_.store(true);
  DoAccept();
  return Status::Success(std::monostate{});
}

bool TcpServerImpl::IsOpen() const { return open_.load(); }

void TcpServerImpl::DoAccept() {
  auto self = shared_from_this();
  acceptor_.async_accept([this, self](asio::error_code ec,
                                      asio::ip::tcp::socket sock) {
    if (ec) {
      DisconnectCallback dcb;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (open_.load()) dcb = disconnect_cb_;  // 锁内拷贝，避免与 OnDisconnect 数据竞争
      }
      if (dcb) dcb("conn: acceptor: " + ec.message());
      return;
    }
    std::shared_ptr<TcpConnectionImpl> conn;
    ConnectionCallback cb_copy;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (static_cast<int>(clients_.size()) >= config_.max_clients) {
        asio::error_code ig;
        sock.close(ig);
      } else {
        std::shared_ptr<IFramer> framer;
        if (config_.framer)
          framer = std::make_shared<LengthFieldFramer>(*config_.framer);
        conn = std::make_shared<TcpConnectionImpl>(std::move(sock), framer);
        if (codec_) conn->SetCodec(codec_);
        const std::string id = conn->PeerId();
        clients_[id] = conn;
        std::weak_ptr<TcpServerImpl> wself = self;
        conn->OnDisconnect([wself, id](const std::string&) {
          if (auto s = wself.lock()) s->RemoveClient(id);
        });
        conn->Open();
        cb_copy = connection_cb_;
      }
    }
    if (conn && cb_copy) cb_copy(conn);  // 锁外回调
    DoAccept();
  });
}

void TcpServerImpl::RemoveClient(const std::string& id) {
  std::lock_guard<std::mutex> lock(mutex_);
  clients_.erase(id);
}

Status TcpServerImpl::Send(const std::vector<uint8_t>& data) {
  std::vector<std::shared_ptr<TcpConnectionImpl>> snapshot;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : clients_) snapshot.push_back(kv.second);
  }
  for (auto& c : snapshot) c->Send(data);
  return Status::Success(std::monostate{});
}

Result<Message> TcpServerImpl::Receive(uint32_t) {
  return Result<Message>::Fail(
      "config: 请使用 OnNewConnection 获取的 client_transport 进行接收");
}

void TcpServerImpl::OnReceive(ReceiveCallback) {}

std::future<Result<Message>> TcpServerImpl::AsyncReceive() {
  std::promise<Result<Message>> p;
  p.set_value(Result<Message>::Fail(
      "config: 请使用 OnNewConnection 获取的 client_transport 进行接收"));
  return p.get_future();
}

void TcpServerImpl::OnDisconnect(DisconnectCallback cb) {
  std::lock_guard<std::mutex> lock(mutex_);
  disconnect_cb_ = std::move(cb);
}

void TcpServerImpl::SetCodec(std::shared_ptr<ICodec> codec) {
  std::lock_guard<std::mutex> lock(mutex_);
  codec_ = std::move(codec);
}

void TcpServerImpl::OnNewConnection(ConnectionCallback cb) {
  std::lock_guard<std::mutex> lock(mutex_);
  connection_cb_ = std::move(cb);
}

std::vector<std::shared_ptr<ITransport>> TcpServerImpl::GetClients() const {
  std::vector<std::shared_ptr<ITransport>> out;
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& kv : clients_) out.push_back(kv.second);
  return out;
}

void TcpServerImpl::DisconnectClient(const std::string& client_id) {
  std::shared_ptr<TcpConnectionImpl> conn;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end()) return;
    conn = it->second;
    clients_.erase(it);
  }
  conn->Close();
}

void TcpServerImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::vector<std::shared_ptr<TcpConnectionImpl>> snapshot;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : clients_) snapshot.push_back(kv.second);
    clients_.clear();
  }
  asio::post(ctx_, [this]() {
    asio::error_code ig;
    acceptor_.close(ig);
  });
  for (auto& c : snapshot) c->Close();

  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
