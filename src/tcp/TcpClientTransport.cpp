#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

// TcpClientTransport.cpp — 见 .hpp。自有 io 线程;connect+超时+退避重连;
// 读到的字节切片经 OnBytes 直接交付(无分帧);写经 strand 串行 write_queue。

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : config_(std::move(config)),
      backoff_base_(backoff_base),
      backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_),
      resolver_(ctx_),
      connect_timer_(ctx_),
      reconnect_timer_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = shared_from_this();
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();
}

void TcpClientTransport::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = shared_from_this();
  asio::error_code rec;
  auto endpoints = resolver_.resolve(config_.host, std::to_string(config_.port), rec);
  if (rec) {
    if (prom) prom->set_value(Status::Fail("conn: resolve: " + rec.message()));
    else ScheduleReconnect();
    return;
  }

  asio::error_code ig;
  socket_.close(ig);
  socket_ = asio::ip::tcp::socket(ctx_);

  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_, [this, self, prom, timed_out](asio::error_code ec,
                                                 const asio::ip::tcp::endpoint& ep) {
            connect_timer_.cancel();
            if (!ec) {
              link_up_.store(true);
              open_.store(true);
              backoff_cur_ = backoff_base_;
              peer_id_ = ep.address().to_string() + ":" + std::to_string(ep.port());
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              if (connect_cb_) connect_cb_();
              StartRead();
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
          }));
}

void TcpClientTransport::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = shared_from_this();
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

void TcpClientTransport::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
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

void TcpClientTransport::HandleDisconnect(const std::string& reason) {
  if (!link_up_.exchange(false)) return;
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
  if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: tcp not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void TcpClientTransport::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void TcpClientTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  link_up_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
