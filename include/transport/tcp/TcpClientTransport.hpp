#pragma once

// TcpClientTransport.hpp — TCP 客户端纯字节管道。自有 io_context + 1 线程:
// connect + 连接超时 + 指数退避自动重连。连接建立后持有一个 TcpConnection,
// 收发委托给它(读写实现复用 TcpConnection,不再内联)。须以 shared_ptr 持有。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpConnection.hpp"

namespace transport {

class TcpClientTransport : public ITransport,
                           public std::enable_shared_from_this<TcpClientTransport> {
 public:
  explicit TcpClientTransport(
      TcpClientConfig config,
      std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
      std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  void StartConnect(std::shared_ptr<std::promise<Status>> prom);
  void ScheduleReconnect();
  void OnConnLost(const std::string& reason);

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_, backoff_cap_, backoff_cur_;

  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::socket socket_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_, reconnect_timer_;
  std::shared_ptr<TcpConnection> conn_;  // 当前连接;每次重连重建。仅在 strand_ 上访问。
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
