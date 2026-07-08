#pragma once

// TcpConnection.hpp — 一条已连接的 TCP 字节管道(QtNetwork/QTcpSocket)。
// client 与 server-accepted 共用。字节流:一次 read 切片经 OnBytes 交付(切帧归上层)。
// 主动 Close 不报 OnDisconnect;真实断开报一次。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

class QTcpSocket;

namespace transport {

class TcpConnection : public ITransport {
 public:
  explicit TcpConnection(QTcpSocket* sock);  // 接管已连接 socket 的所有权
  ~TcpConnection() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

 private:
  void onReadyRead();
  void handleDisconnect(const std::string& reason);

  std::unique_ptr<QTcpSocket> sock_;
  std::string peer_id_;
  bool open_ = false;
  bool disconnected_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
