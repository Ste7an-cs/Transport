#include "transport/udp/UdpTransport.hpp"

#include <utility>
#include <variant>

// UdpTransport.cpp — UDP 纯字节管道。自有 io_context + 1 io 线程;收发经 strand。
// 报文保边界 → 每个 datagram 经 OnBytes 交付裸字节 + from("ip:port")。

namespace transport {

UdpTransport::UdpTransport(UdpConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

UdpTransport::~UdpTransport() { Close(); }

bool UdpTransport::IsOpen() const { return open_.load(); }

Status UdpTransport::Open() {
  asio::error_code ec;
  socket_.open(asio::ip::udp::v4(), ec);
  if (ec) return Status::Fail("config: udp open: " + ec.message());

  if (config_.mode == UdpMode::kBroadcast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
    socket_.set_option(asio::socket_base::broadcast(true), ec);
    if (ec) return Status::Fail("config: broadcast option: " + ec.message());
  } else if (config_.mode == UdpMode::kMulticast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
  }

  asio::ip::address local;
  if (config_.mode == UdpMode::kMulticast) {
    local = asio::ip::address_v4::any();
  } else {
    local = asio::ip::make_address(config_.local_addr, ec);
    if (ec) return Status::Fail("config: invalid local_addr");
  }
  socket_.bind(asio::ip::udp::endpoint(local, config_.local_port), ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());

  if (config_.mode == UdpMode::kMulticast) {
    auto group = asio::ip::make_address(config_.multicast_group, ec);
    if (ec) return Status::Fail("config: invalid multicast_group");
    socket_.set_option(asio::ip::multicast::join_group(group), ec);
    if (ec) return Status::Fail("config: join_group: " + ec.message());
    asio::error_code ig;
    socket_.set_option(asio::ip::multicast::hops(config_.ttl), ig);
    socket_.set_option(asio::ip::multicast::enable_loopback(true), ig);
    default_dest_ = asio::ip::udp::endpoint(group, config_.remote_port);
  } else if (!config_.remote_addr.empty()) {
    auto raddr = asio::ip::make_address(config_.remote_addr, ec);
    if (ec) return Status::Fail("config: invalid remote_addr");
    default_dest_ = asio::ip::udp::endpoint(raddr, config_.remote_port);
  }

  asio::error_code lec;
  auto le = socket_.local_endpoint(lec);
  if (lec) return Status::Fail("config: local_endpoint: " + lec.message());
  local_port_ = le.port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartReceive();
  });
  return Status::Success(std::monostate{});
}

void UdpTransport::StartReceive() {
  auto self = shared_from_this();
  socket_.async_receive_from(
      asio::buffer(recv_buf_), recv_from_,
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (!open_.load()) return;
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              if (bytes_cb_)
                bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: receive: " + ec.message()), "");
              StartReceive();
              return;
            }
            std::string from = recv_from_.address().to_string() + ":" +
                               std::to_string(recv_from_.port());
            std::vector<uint8_t> dg(recv_buf_.begin(), recv_buf_.begin() + n);
            if (bytes_cb_)
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(dg)), from);
            StartReceive();
          }));
}

Result<asio::ip::udp::endpoint> UdpTransport::ResolveDest(const Endpoint& to) {
  using R = Result<asio::ip::udp::endpoint>;
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return R::Success(default_dest_);
    case Endpoint::Kind::kNet: {
      asio::error_code ec;
      auto addr = asio::ip::make_address(to.host, ec);
      if (ec) return R::Fail("config: invalid address");
      return R::Success(asio::ip::udp::endpoint(addr, to.port));
    }
    case Endpoint::Kind::kTopic:
      return R::Fail("config: udp expects net endpoint");
  }
  return R::Fail("config: unknown endpoint kind");
}

Status UdpTransport::SendRaw(std::vector<uint8_t> bytes,
                             const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);
    if (ec && bytes_cb_)
      bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: send: " + ec.message()), "");
  });
  return Status::Success(std::monostate{});
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes) {
  return SendRaw(bytes, default_dest_);
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  auto dest = ResolveDest(to);
  if (!dest) return Status::Fail(dest.error);
  return SendRaw(bytes, dest.value);
}

void UdpTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    socket_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
