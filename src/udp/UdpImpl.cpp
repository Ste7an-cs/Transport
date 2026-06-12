#include "transport/udp/UdpImpl.hpp"

#include <utility>
#include <variant>

// UdpImpl.cpp — UDP 单播/组播/广播实现（见 UdpImpl.hpp）。
// 并发：自有 io_context + 1 io 线程；收/发都经 strand_ 串行化（async_receive_from
// 与 strand 上的同步 send_to 互不并发）。报文保边界 → 无分帧，每个 datagram 一条
// Message，经 core_.DeliverFrame 投递。

namespace transport {

UdpImpl::UdpImpl(UdpConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

UdpImpl::~UdpImpl() { Close(); }

bool UdpImpl::IsOpen() const { return open_.load(); }

Status UdpImpl::Open() {
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

  // 绑定：组播绑 0.0.0.0 以收组；其余绑 local_addr。
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
  // 单播/广播 remote_addr 为空：default_dest_ 留默认（仅 Endpoint::Net 寻址/接收可用）；
  // 此时调 Send() 会向 0.0.0.0:0 发出并经 core_.DeliverError 报 io: 错误。

  asio::error_code lec;  // 用 error_code 重载，遵守框架不抛异常约定
  auto le = socket_.local_endpoint(lec);
  if (lec) return Status::Fail("config: local_endpoint: " + lec.message());
  local_port_ = le.port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartReceive(); });
  return Status::Success(std::monostate{});
}

void UdpImpl::StartReceive() {
  auto self = shared_from_this();
  socket_.async_receive_from(
      asio::buffer(recv_buf_), recv_from_,
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (!open_.load()) return;
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              core_.DeliverError("io: receive: " + ec.message());
              StartReceive();  // 单报文错误不致命，继续监听
              return;
            }
            std::string source = recv_from_.address().to_string() + ":" +
                                 std::to_string(recv_from_.port());
            std::vector<uint8_t> datagram(recv_buf_.begin(),
                                          recv_buf_.begin() + n);
            core_.DeliverFrame(std::move(datagram), source, "");
            StartReceive();
          }));
}

Status UdpImpl::SendToEndpoint(const std::vector<uint8_t>& data,
                               const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(enc.value));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);  // 在 strand 上同步发，避免与收竞争
    if (ec) core_.DeliverError("io: send: " + ec.message());
  });
  return Status::Success(std::monostate{});  // 已入队写出
}

Status UdpImpl::Send(const std::vector<uint8_t>& data) {
  return SendToEndpoint(data, default_dest_);
}

Status UdpImpl::Send(const std::vector<uint8_t>& data, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return Send(data);
    case Endpoint::Kind::kNet: {
      asio::error_code ec;
      auto addr = asio::ip::make_address(to.host, ec);
      if (ec) return Status::Fail("config: invalid address");
      return SendToEndpoint(data, asio::ip::udp::endpoint(addr, to.port));
    }
    case Endpoint::Kind::kTopic:
      return Status::Fail("config: udp expects net endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

void UdpImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    socket_.close(ig);
  });
  core_.Close();
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
