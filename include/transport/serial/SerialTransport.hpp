#pragma once

// SerialTransport.hpp — 串口纯字节管道(改写自 SerialImpl,去 codec/分帧/topic)。
// 自有 io_context + 1 io 线程;读到的字节切片经 OnBytes 交付(from=设备路径)。

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/serial/SerialConfig.hpp"

namespace transport {

class SerialTransport : public ITransport,
                        public std::enable_shared_from_this<SerialTransport> {
 public:
  explicit SerialTransport(SerialConfig config);
  ~SerialTransport() override;

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
  void StartRead();
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);
  void HandleDisconnect(const std::string& reason);

  SerialConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::serial_port port_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> disconnected_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
