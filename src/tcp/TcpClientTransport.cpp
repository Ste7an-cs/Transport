#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

// TcpClientTransport.cpp — 客户端 TCP 字节管道(ITransport 实现)。
// 自有 io_context + io 线程。connect 成功后用已连接 socket 造一个 TcpConnection(conn_),
// 把用户回调接上去并委托收发;断开经 conn_ 的 OnDisconnect → OnConnLost → 指数退避重连
// (若 auto_reconnect)。Open() 同步等首次 connect 结果(promise/future)。
//
// 并发关键:conn_ 只在 strand_ 上读写(Send 也 post 到 strand_),所以"用旧 conn_ 发送"
// 与"io 线程上重建 conn_"不会竞争。退避:backoff_cur_ 每次 ×2 封顶 backoff_cap_,connect
// 成功复位 backoff_base_。

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

// Open:把 StartConnect 投到 strand,用 promise 同步等首次 connect 结果(成功/失败/超时)。
Status TcpClientTransport::Open() {
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = shared_from_this();
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();                              // 阻塞到 connect handler 兑现 promise
}

// StartConnect:解析 host:port → 起连接超时定时器 → async_connect。
// prom 非空时是首次 Open 的同步等待者(兑现它);prom 为空时是重连路径(失败则再排重连)。
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

  // 连接超时:到点就关 socket → 令 async_connect 以 aborted 失败;timed_out 区分超时 vs 普通失败。
  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;                          // 定时器被 cancel(连接已成/已败),正常
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_, [this, self, prom, timed_out](asio::error_code ec,
                                                 const asio::ip::tcp::endpoint&) {
            connect_timer_.cancel();             // 连接有结果了,撤超时
            if (!ec) {
              backoff_cur_ = backoff_base_;       // 连上 → 退避复位
              conn_ = std::make_shared<TcpConnection>(std::move(socket_));  // 已连接 socket 造连接
              socket_ = asio::ip::tcp::socket(ctx_);  // 复位备用(下次重连用)
              conn_->OnBytes(bytes_cb_);          // 把用户回调透传给底层连接
              conn_->OnConnect(connect_cb_);
              std::weak_ptr<TcpClientTransport> wself = self;
              conn_->OnDisconnect([wself](const std::string& reason) {  // 断开 → 退避重连
                auto s = wself.lock();
                if (!s) return;
                asio::post(s->strand_, [s, reason]() { s->OnConnLost(reason); });
              });
              open_.store(true);
              if (prom) prom->set_value(Status::Success(std::monostate{}));  // 兑现 Open 等待者
              (void)conn_->Open();                // 启动读循环 + 回 OnConnect
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));  // 首次 Open 失败 → 返回错误
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();  // 否则排重连
          }));
}

// OnConnLost(strand 上):连接断了。清掉 conn_;配了自动重连则排重连,否则把断开上报给用户。
void TcpClientTransport::OnConnLost(const std::string& reason) {
  open_.store(false);
  conn_.reset();
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();                          // 自动重连:不打扰用户(重连仍会触发 OnConnect)
  } else if (disconnect_cb_) {
    disconnect_cb_(reason);                        // 不重连:把断开通知用户
  }
}

// ScheduleReconnect:退避等待后再 StartConnect。每次退避 ×2 封顶(backoff_cur_)。
void TcpClientTransport::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = shared_from_this();
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);  // 指数退避,封顶
        StartConnect(nullptr);                    // 重连路径(prom=null)
      }));
}

// Send:委托给当前 conn_(在 strand 上),避免与重建竞争。未连接则拒发。
Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: tcp not open");
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(bytes);  // 异步期间持有
  asio::post(strand_, [this, self, buf]() {
    if (conn_) (void)conn_->Send(*buf);          // conn_ 只在 strand_ 上访问
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
