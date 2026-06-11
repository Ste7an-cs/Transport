#pragma once

// -----------------------------------------------------------------------------
// SerialImpl.hpp — 串口传输实现（ITransport）
// 组合 TransportCore + FrameAssembler；基于 asio::serial_port，自有 io_context +
// 1 后台 io 线程。流式（接收侧分帧同 TCP），无连接/无重连。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/framing/FrameAssembler.hpp"
#include "transport/serial/SerialConfig.hpp"

namespace transport {

class SerialImpl : public ITransport,
                   public std::enable_shared_from_this<SerialImpl> {
 public:
  explicit SerialImpl(SerialConfig config);
  ~SerialImpl() override;

  Status Open() override;   // 打开串口 + 配置参数 + 启动接收循环
  void Close() override;    // 关 port + core_.Close() + 停 io 线程
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;

  // 接收侧：一行转发给 core_
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

 private:
  void StartRead();
  void DoWrite();
  void HandleDisconnect(const std::string& reason);

  SerialConfig config_;
  TransportCore core_;
  FrameAssembler assembler_;
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
};

}  // namespace transport
