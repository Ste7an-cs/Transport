#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <asio.hpp>

#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpConnection.hpp"

namespace transport {

namespace detail {
// 仅持有 io_context。作为 TcpClientTransport 的首个基类，保证 io_context 先于
// TcpConnection 基类构造——TcpConnection 的 socket_ 需绑定到这个 ctx。
struct IoContextHolder {
  asio::io_context ctx;
};
}  // namespace detail

// TCP 客户端：自有 io_context + 1 线程；connect 超时；指数退避自动重连。
// 须以 shared_ptr 持有（基类 TcpConnection 用 shared_from_this 保活）。
class TcpClientTransport : public detail::IoContextHolder, public TcpConnection {
 public:
  explicit TcpClientTransport(
      TcpClientConfig config,
      std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
      std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;   // 同步连接（受 connect_timeout_ms 约束），成功后启动读
  void Close() override;    // 停重连 + 关连接 + 停 io 线程

  // 仅供测试/动态配置：覆盖目标 host
  void SetHost(const std::string& host) { config_.host = host; }

 protected:
  void HandleDisconnect(const std::string& reason) override;  // 触发重连

 private:
  // 在 io 线程上发起一次连接；prom 非空=初次 Open（设置结果），空=重连
  void StartConnect(std::shared_ptr<std::promise<Status>> prom);
  void ScheduleReconnect();

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_;
  std::chrono::milliseconds backoff_cap_;
  std::chrono::milliseconds backoff_cur_;

  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_;
  asio::steady_timer reconnect_timer_;
  std::thread io_thread_;
  std::atomic<bool> closing_{false};
  std::atomic<bool> link_up_{false};  // 连接建立标志；保证每次掉线只处理一次
};

}  // namespace transport
