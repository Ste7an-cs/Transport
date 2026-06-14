#pragma once

// -----------------------------------------------------------------------------
// TcpConnectionImpl.hpp — 已连接 socket 的收发实现（ITransport）
// 持有 TransportCore；包装一个已连接的 asio tcp socket：async_read 循环经
// FrameAssembler 切帧 → core_.DeliverFrame，写经 strand 串行化。客户端与服务端
// accepted 连接共用它。不拥有 io 线程（由所属 io_context 驱动）。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "transport/IFramer.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TopicEnvelope.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/framing/FrameAssembler.hpp"

namespace transport {

// 已连接 socket 的收发循环：客户端连上后、服务端 accept 后均用它。
// io 由 socket 所属的 io_context 驱动（本类不拥有线程）。须以 shared_ptr 持有。
class TcpConnectionImpl : public ITransport,
                          public std::enable_shared_from_this<TcpConnectionImpl> {
 public:
  TcpConnectionImpl(asio::ip::tcp::socket socket, std::shared_ptr<IFramer> framer,
                    bool enable_topic_routing = false);

  Status Open() override;   // 启动 async_read 循环
  void Close() override;    // 关闭 socket + core_.Close()
  bool IsOpen() const override;
  using ITransport::Send;  // 保留基类 Send(data,Endpoint) 重载,避免名字隐藏
  using ITransport::SetCodec;  // 保留基类 SetCodec(codec) 重载,避免名字隐藏
  Status Send(const std::vector<uint8_t>& data) override;
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;

  // 接收侧：转发给 core_
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }
  void SetCodec(std::shared_ptr<ICodec> codec) override { core_.SetCodec(std::move(codec)); }
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }

  const std::string& PeerId() const { return peer_id_; }

 protected:
  // 断连处理。基类版：投递错误 + 关队列 + 通知（accepted 连接为终态）。
  // 子类（client）覆盖以触发重连而不关队列。每个连接周期只生效一次。
  virtual void HandleDisconnect(const std::string& reason);

  void StartRead();   // 启动一次 async_read_some；子类（client）连接成功后调用

  TransportCore core_;  // 接收交付 + 编解码（子类 TcpClientImpl 也用）
  asio::ip::tcp::socket socket_;
  asio::strand<asio::any_io_executor> strand_;
  std::atomic<bool> open_{false};

 private:
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);  // 入队 + 触发 DoWrite

  FrameAssembler assembler_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::string peer_id_;
  std::atomic<bool> disconnected_{false};
  bool enable_topic_routing_;
  TopicFrameAssembler topic_assembler_;
};

}  // namespace transport
