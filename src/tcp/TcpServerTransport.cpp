#include "transport/tcp/TcpServerTransport.hpp"

#include <algorithm>
#include <utility>
#include <variant>

// TcpServerTransport.cpp — 见 .hpp。
// Close:在 strand 上关 acceptor + 逐个 Close 在册连接(关其 socket),再释放 work guard,
// 不调 ctx_.stop() —— 让 io 线程把"关连接 → 读被 abort"的 pending 处理排空后自然返回,
// 保证对端确实看到连接关闭(EOF),再 join。

namespace transport {

TcpServerTransport::TcpServerTransport(TcpServerConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      acceptor_(ctx_),
      peer_socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpServerTransport::~TcpServerTransport() { Close(); }

Status TcpServerTransport::Open() {
  asio::error_code ec;
  auto addr = asio::ip::make_address(config_.bind_addr, ec);
  if (ec) return Status::Fail("config: invalid bind_addr");
  asio::ip::tcp::endpoint ep(addr, config_.port);

  acceptor_.open(ep.protocol(), ec);
  if (ec) return Status::Fail("io: acceptor open: " + ec.message());
  acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
  acceptor_.bind(ep, ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());
  const int backlog = config_.backlog > 0
                          ? config_.backlog
                          : asio::socket_base::max_listen_connections;
  acceptor_.listen(backlog, ec);
  if (ec) return Status::Fail("io: listen: " + ec.message());

  asio::error_code lec;
  auto le = acceptor_.local_endpoint(lec);
  if (lec) return Status::Fail("io: local_endpoint: " + lec.message());
  local_port_ = le.port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { DoAccept(); });
  return Status::Success(std::monostate{});
}

void TcpServerTransport::DoAccept() {
  auto self = shared_from_this();
  acceptor_.async_accept(
      peer_socket_,
      asio::bind_executor(strand_, [this, self](asio::error_code ec) {
        if (ec) {
          if (ec == asio::error::operation_aborted) return;
          if (error_cb_) error_cb_("io: accept: " + ec.message());
          if (open_.load()) DoAccept();
          return;
        }
        auto conn = std::make_shared<TcpConnection>(std::move(peer_socket_));
        peer_socket_ = asio::ip::tcp::socket(ctx_);
        conns_.erase(
            std::remove_if(conns_.begin(), conns_.end(),
                           [](const std::weak_ptr<TcpConnection>& w) { return w.expired(); }),
            conns_.end());
        conns_.push_back(conn);
        if (accept_cb_) accept_cb_(conn);
        (void)conn->Open();  // 已连接 socket 起读循环;失败经该连接接收侧暴露
        DoAccept();
      }));
}

void TcpServerTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    acceptor_.close(ig);
    for (auto& w : conns_)
      if (auto c = w.lock()) c->Close();
    conns_.clear();
  });
  guard_.reset();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
