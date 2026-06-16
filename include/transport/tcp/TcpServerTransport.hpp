#pragma once

// TcpServerTransport.hpp — TCP 接受器(不是 ITransport)。自有 io_context + 1 线程:
// 监听 + 接受;每 accept 一个 socket 造一个 TcpConnection,先 OnAccept(conn)(用户在回调里
// 同步设好 conn 回调)再 conn->Open()。最小 API:无广播/GetClients/DisconnectClient。
// 须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpConnection.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

namespace transport {

class TcpServerTransport : public std::enable_shared_from_this<TcpServerTransport> {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  void OnAccept(std::function<void(std::shared_ptr<ITransport>)> cb) {
    accept_cb_ = std::move(cb);
  }
  void OnError(std::function<void(const std::string&)> cb) { error_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void DoAccept();

  TcpServerConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket peer_socket_;
  std::vector<std::weak_ptr<TcpConnection>> conns_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;

  std::function<void(std::shared_ptr<ITransport>)> accept_cb_;
  std::function<void(const std::string&)> error_cb_;
};

}  // namespace transport
