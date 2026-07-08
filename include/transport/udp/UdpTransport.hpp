#pragma once

// UdpTransport.hpp — UDP 纯字节管道(QtNetwork/QUdpSocket)。单播/组播/广播。
// 活在宿主 Qt 事件循环线程;每个 datagram 经 OnBytes 交付裸字节 + from("ip:port")。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <QHostAddress>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/udp/UdpConfig.hpp"

class QUdpSocket;

namespace transport {

class UdpTransport : public ITransport {
 public:
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void onReadyRead();
  Status sendTo(const std::vector<uint8_t>& bytes, const QHostAddress& addr, quint16 port);

  UdpConfig config_;
  std::unique_ptr<QUdpSocket> sock_;
  QHostAddress dest_addr_;
  quint16 dest_port_ = 0;
  bool open_ = false;
  uint16_t local_port_ = 0;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
