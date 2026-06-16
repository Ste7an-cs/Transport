#include "transport/tcp/TcpConnection.hpp"

#include <utility>
#include <variant>

// TcpConnection.cpp — 见 .hpp。读到的字节切片经 OnBytes 直接交付(无分帧)。
// Close 与 HandleDisconnect:operation_aborted(主动关闭引起)在读写 handler 里被跳过,
// 故 Close 不会触发 OnDisconnect;真实对端断开(eof/reset)才经 HandleDisconnect 上报一次。

namespace transport {

TcpConnection::TcpConnection(asio::ip::tcp::socket socket)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())) {}

TcpConnection::~TcpConnection() {
  asio::error_code ig;
  socket_.close(ig);
}

Status TcpConnection::Open() {
  asio::error_code ec;
  auto ep = socket_.remote_endpoint(ec);
  if (!ec) peer_id_ = ep.address().to_string() + ":" + std::to_string(ep.port());
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartRead();
  });
  return Status::Success(std::monostate{});
}

void TcpConnection::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)), peer_id_);
            }
            StartRead();
          }));
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: connection not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpConnection::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void TcpConnection::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              if (ec == asio::error::operation_aborted) return;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void TcpConnection::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
}

void TcpConnection::Close() {
  if (closing_.exchange(true)) return;
  disconnected_.store(true);
  open_.store(false);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    asio::error_code ig;
    socket_.close(ig);
  });
}

}  // namespace transport
