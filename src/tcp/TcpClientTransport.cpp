#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : detail::IoContextHolder(),
      TcpConnection(asio::ip::tcp::socket(ctx),
                    config.framer
                        ? std::make_shared<LengthFieldFramer>(*config.framer)
                        : nullptr),
      config_(std::move(config)),
      backoff_base_(backoff_base),
      backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base),
      guard_(ctx.get_executor()),
      resolver_(ctx),
      connect_timer_(ctx),
      reconnect_timer_(ctx) {
  io_thread_ = std::thread([this] { ctx.run(); });
}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  // framer 配置校验（spec：非法则 config: 错误）
  if (config_.framer) {
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = std::static_pointer_cast<TcpClientTransport>(shared_from_this());
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();  // 阻塞等待初次连接成败/超时
}

void TcpClientTransport::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = std::static_pointer_cast<TcpClientTransport>(shared_from_this());

  // 已知限制：resolver_.resolve 为同步阻塞 DNS 查询，不受 connect_timeout_ms 约束；
  // connect_timeout_ms 仅约束 TCP 连接阶段。host 为 IP（如 127.0.0.1）时解析瞬时返回；
  // 对慢速/黑洞 DNS 的主机名，Open() 可能阻塞超过 connect_timeout_ms。
  asio::error_code rec;
  auto endpoints =
      resolver_.resolve(config_.host, std::to_string(config_.port), rec);
  if (rec) {
    if (prom) prom->set_value(Status::Fail("conn: resolve: " + rec.message()));
    else ScheduleReconnect();
    return;
  }

  asio::error_code ig;
  socket_.close(ig);
  socket_ = asio::ip::tcp::socket(ctx);  // 新建 socket（重连时旧的已关）

  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(
      std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;  // 定时器被取消（连接已完成）
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);  // 取消进行中的 connect
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_,
          [this, self, prom, timed_out](asio::error_code ec,
                                        const asio::ip::tcp::endpoint&) {
            connect_timer_.cancel();
            if (!ec) {
              link_up_.store(true);
              open_.store(true);
              backoff_cur_ = backoff_base_;
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              StartRead();  // 启动读循环（基类 protected）
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
  auto self = std::static_pointer_cast<TcpClientTransport>(shared_from_this());
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

void TcpClientTransport::HandleDisconnect(const std::string& reason) {
  if (!link_up_.exchange(false)) return;  // 每个连接周期只处理一次
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  NotifyDisconnect(reason);
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();  // 复用接收队列，不 CloseQueue
  } else {
    DeliverError(reason);
    CloseQueue();
  }
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
  CloseQueue();
  guard_.reset();
  ctx.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
