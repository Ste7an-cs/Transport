#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/ReceiveQueue.hpp"

namespace transport {

// ITransport 的通用实现：编解码挂载、三模式接收交付、断连通知。
// 子类只需实现 Open/Close/IsOpen/Send，并在收到字节时调用 DeliverFrame。
class TransportBase : public ITransport {
 public:
  void SetCodec(std::shared_ptr<ICodec> codec) override {
    codec_ = std::move(codec);
  }
  Result<Message> Receive(uint32_t timeout_ms) override {
    return queue_.Receive(timeout_ms);
  }
  void OnReceive(ReceiveCallback cb) override {
    queue_.SetCallback(std::move(cb));
  }
  std::future<Result<Message>> AsyncReceive() override {
    return queue_.AsyncReceive();
  }
  void OnDisconnect(DisconnectCallback cb) override {
    disconnect_cb_ = std::move(cb);
  }

 protected:
  // 发送前编码；无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(data);
    return codec_->Encode(data);
  }

  // 收到一帧/一报文：解码（无 codec 透传）后构造 Message 投递；解码失败投递 Fail。
  void DeliverFrame(std::vector<uint8_t> frame, const std::string& source,
                    const std::string& topic) {
    std::vector<uint8_t> payload;
    if (codec_) {
      auto decoded = codec_->Decode(frame);
      if (!decoded) {
        queue_.Push(Result<Message>::Fail(decoded.error));
        return;
      }
      payload = std::move(decoded.value);
    } else {
      payload = std::move(frame);
    }
    Message msg;
    msg.payload = std::move(payload);
    msg.source = source;
    msg.topic = topic;
    msg.timestamp = NowMicros();
    queue_.Push(Result<Message>::Success(std::move(msg)));
  }

  // 投递一个连接级错误（如对端断开）到接收侧。
  void DeliverError(std::string error) {
    queue_.Push(Result<Message>::Fail(std::move(error)));
  }

  void NotifyDisconnect(const std::string& reason) {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

  // 关闭接收队列（唤醒等待者）。子类 Close() 应调用。
  void CloseQueue() { queue_.Close(); }

  static int64_t NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::shared_ptr<ICodec> codec_;
  ReceiveQueue queue_;
  DisconnectCallback disconnect_cb_;
};

}  // namespace transport
