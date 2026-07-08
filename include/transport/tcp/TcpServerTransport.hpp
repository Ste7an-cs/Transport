#pragma once

// TcpServerTransport.hpp — TCP 服务端接受器(QtNetwork/QTcpServer)。非 ITransport。
// 每 accept 一个连接造一个 TcpConnection 经 OnAccept 交付,用户在其上独立收发。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

class QTcpServer;

namespace transport {

class TcpConnection;

class TcpServerTransport {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_; }

  void OnAccept(std::function<void(std::shared_ptr<ITransport>)> cb) { accept_cb_ = std::move(cb); }
  void OnError(std::function<void(const std::string&)> cb) { error_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void onNewConnection();

  TcpServerConfig config_;
  std::unique_ptr<QTcpServer> server_;
  bool open_ = false;
  uint16_t local_port_ = 0;
  std::vector<std::weak_ptr<TcpConnection>> conns_;
  std::function<void(std::shared_ptr<ITransport>)> accept_cb_;
  std::function<void(const std::string&)> error_cb_;
};

}  // namespace transport
