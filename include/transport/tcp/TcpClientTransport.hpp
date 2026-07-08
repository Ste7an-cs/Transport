#pragma once

// TcpClientTransport.hpp — 客户端 TCP 字节管道(QtNetwork/QTcpSocket)。
// connectToHost + 连接超时(QTimer)+ 可选指数退避自动重连。活在宿主 Qt 事件循环线程。

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"

class QTcpSocket;
class QTimer;

namespace transport {

class TcpClientTransport : public ITransport {
 public:
  explicit TcpClientTransport(TcpClientConfig config,
                              std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
                              std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

 private:
  void startConnect();
  void onConnected();
  void onReadyRead();
  void onDisconnected(const std::string& reason);
  void scheduleReconnect();

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_, backoff_cap_, backoff_cur_;
  std::unique_ptr<QTcpSocket> sock_;
  std::unique_ptr<QTimer> connect_timer_;
  std::unique_ptr<QTimer> reconnect_timer_;
  std::string peer_id_;
  bool open_ = false;
  bool closing_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
