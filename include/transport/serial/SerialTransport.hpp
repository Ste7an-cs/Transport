#pragma once

// SerialTransport.hpp — 串口字节管道(QtNetwork/QSerialPort)。活在宿主 Qt 事件循环线程。
// 读到的切片经 OnBytes 交付(切帧归上层 codec);from=设备路径。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/serial/SerialConfig.hpp"

class QSerialPort;

namespace transport {

class SerialTransport : public ITransport {
 public:
  explicit SerialTransport(SerialConfig config);
  ~SerialTransport() override;

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

  SerialConfig config_;
  std::unique_ptr<QSerialPort> port_;
  bool open_ = false;
  bool disconnected_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
