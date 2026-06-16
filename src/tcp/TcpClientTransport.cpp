#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

// TcpClientTransport.cpp — 见 .hpp。connect 成功后用已连接 socket 造 TcpConnection,
// 接入用户回调并委托收发;断开经 conn_ 的 OnDisconnect 路由到 OnConnLost(退避重连)。
// conn_ 只在 strand_ 上访问(Send 也 post 到 strand_),避免与 io 线程上的重建竞争。

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
                                                 const asio::ip::tcp::endpoint&) {
            connect_timer_.cancel();
            if (!ec) {
              backoff_cur_ = backoff_base_;
              conn_ = std::make_shared<TcpConnection>(std::move(socket_));
              socket_ = asio::ip::tcp::socket(ctx_);  // 复位备用
              conn_->OnBytes(bytes_cb_);
              conn_->OnConnect(connect_cb_);
              std::weak_ptr<TcpClientTransport> wself = self;
              conn_->OnDisconnect([wself](const std::string& reason) {
                auto s = wself.lock();
                if (!s) return;
                asio::post(s->strand_, [s, reason]() { s->OnConnLost(reason); });
              });
              open_.store(true);
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              (void)conn_->Open();
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
          }));
}

void TcpClientTransport::OnConnLost(const std::string& reason) {
  open_.store(false);
  conn_.reset();
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();
  } else if (disconnect_cb_) {
    disconnect_cb_(reason);
  }
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

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: tcp not open");
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(bytes);
  asio::post(strand_, [this, self, buf]() {
    if (conn_) (void)conn_->Send(*buf);  // conn_ 只在 strand_ 上访问
  });
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
    if (conn_) conn_->Close();
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
