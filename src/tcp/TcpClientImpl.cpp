#include "transport/tcp/TcpClientImpl.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

#include "transport/framing/LengthFieldFramer.hpp"

// TcpClientImpl.cpp — TCP 客户端实现（见 TcpClientImpl.hpp）。
//
// 自有 io_context + 1 后台 io 线程。连接生命周期：
//   Open()           同步发起首连（阻塞在 future 上等成败/超时）；
//   断线 → HandleDisconnect → 指数退避 ScheduleReconnect（复用接收队列，不关）；
//   Close()          停重连、关 socket、停 ctx 并 join 线程（幂等）。
// link_up_ 闩保证「每个连接周期只处理一次断连」（read/write 同时报错也只处理一次）。
// 构造顺序：detail::IoContextHolder 作首基类，使其 ctx 先于 TcpConnectionImpl 的
// socket 构造（socket 需绑定到 ctx）。

namespace transport {

TcpClientImpl::TcpClientImpl(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : detail::IoContextHolder(),
      TcpConnectionImpl(asio::ip::tcp::socket(ctx),
                    config.framer
                        ? std::make_shared<LengthFieldFramer>(*config.framer)
                        : nullptr,
                    config.enable_topic_routing),
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

TcpClientImpl::~TcpClientImpl() { Close(); }

// 同步发起首次连接：把连接动作 post 到 io 线程，阻塞等待 prom 被设置
// （成功/失败/超时三种结局都会 set_value），返回最终 Status。
Status TcpClientImpl::Open() {
  // framer 配置校验（spec：非法则 config: 错误）
  if (config_.framer) {
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();  // 阻塞等待初次连接成败/超时
}

// 在 io 线程上发起一次连接：resolve → 新建 socket → 起 connect 超时定时器 →
// async_connect。prom 非空=首连（设置结果），空=重连（失败则继续退避）。
// 超时定时器与 connect 完成互相取消：谁先触发，另一方在 strand 上看到取消/中止。
void TcpClientImpl::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());

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

// 退避重连：等待 backoff_cur_ 后再发起一次连接；每次失败退避翻倍并封顶到
// backoff_cap_；连接成功时在 StartConnect 里复位为 backoff_base_。Close 后不再排程。
void TcpClientImpl::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = std::static_pointer_cast<TcpClientImpl>(shared_from_this());
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

void TcpClientImpl::HandleDisconnect(const std::string& reason) {
  if (!link_up_.exchange(false)) return;  // 每个连接周期只处理一次
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  core_.NotifyDisconnect(reason);
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();  // 复用接收队列，不 core_.Close
  } else {
    core_.DeliverError(reason);
    core_.Close();
  }
}

void TcpClientImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  link_up_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
  });
  core_.Close();
  guard_.reset();
  ctx.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
