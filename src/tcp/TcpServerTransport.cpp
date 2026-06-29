#include "transport/tcp/TcpServerTransport.hpp"

#include <algorithm>
#include <utility>
#include <variant>

// TcpServerTransport.cpp — TCP 服务端【接受器】(注意:它本身不是 ITransport,不收发字节)。
// 监听 bind_addr:port,每 accept 一个连接就造一个 TcpConnection 经 OnAccept 交给用户,
// 用户在每个连接上独立收发(典型:在其上构造一个节点)。服务端不广播、不持有"全体客户端"。
// conns_ 只存 weak_ptr,仅用于 Close 时通知关闭(不延长连接寿命)。
//
// Close 的讲究:在 strand 上关 acceptor + 逐个 Close 在册连接(关其 socket),再释放 work
// guard,但【不调 ctx_.stop()】—— 让 io 线程把"关连接→读被 abort"的 pending 处理排空后
// 自然返回,保证对端确实看到 EOF,再 join。

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
  local_port_ = le.port();                        // OS 分配端口(port=0 时取回)
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { DoAccept(); });  // 启动接受循环
  return Status::Success(std::monostate{});
}

// DoAccept:接受一个连接 → 造 TcpConnection → 经 OnAccept 交给用户 → 起其读循环 → 再接受下一个。
void TcpServerTransport::DoAccept() {
  auto self = shared_from_this();
  acceptor_.async_accept(
      peer_socket_,
      asio::bind_executor(strand_, [this, self](asio::error_code ec) {
        if (ec) {
          if (ec == asio::error::operation_aborted) return;   // Close 关 acceptor,正常退出
          if (error_cb_) error_cb_("io: accept: " + ec.message());  // 接受错误上报,继续
          if (open_.load()) DoAccept();
          return;
        }
        auto conn = std::make_shared<TcpConnection>(std::move(peer_socket_));  // 接管已连接 socket
        peer_socket_ = asio::ip::tcp::socket(ctx_);  // 复位备用(下次 accept 用)
        // 顺手清理已失效的 weak(用户没保活的连接)。
        conns_.erase(
            std::remove_if(conns_.begin(), conns_.end(),
                           [](const std::weak_ptr<TcpConnection>& w) { return w.expired(); }),
            conns_.end());
        conns_.push_back(conn);                    // 只存 weak,不延长寿命(保活是用户的事)
        if (accept_cb_) accept_cb_(conn);          // 交给用户(用户须在回调里同步设好 conn 的回调并保活)
        (void)conn->Open();                        // 起该连接的读循环
        DoAccept();                                // 接受下一个
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
