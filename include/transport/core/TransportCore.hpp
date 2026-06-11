#pragma once

// -----------------------------------------------------------------------------
// TransportCore.hpp — 接收交付 + 编解码内核（被持有的组件，本身不是 ITransport）
// 会收数据的传输（TCP 连接 / UDP / DDS / 串口）持有它：把 ITransport 的接收侧方法
// (Receive/OnReceive/AsyncReceive/SetCodec/OnDisconnect) 转发给它；io 线程收到字节
// 调 DeliverFrame、发送前调 EncodeForSend。把 ITransport 留在具体传输上，避免
// 「TransportBase 与扩展接口同源 ITransport」的菱形。
// -----------------------------------------------------------------------------

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"  // 仅借用 ReceiveCallback / DisconnectCallback 类型
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/ReceiveQueue.hpp"

namespace transport {

class TransportCore {
 public:
  using ReceiveCallback = ITransport::ReceiveCallback;
  using DisconnectCallback = ITransport::DisconnectCallback;

  // —— 接收侧（持有者转发给 ITransport 同名方法）——
  void SetCodec(std::shared_ptr<ICodec> codec) { codec_ = std::move(codec); }
  Result<Message> Receive(uint32_t timeout_ms) { return queue_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) { queue_.SetCallback(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() { return queue_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) { disconnect_cb_ = std::move(cb); }

  // —— 生产侧（持有者在 io 线程调用）——
  // 发送前编码；无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(data);
    return codec_->Encode(data);
  }

  // 接收侧解码；无 codec 时透传。供不经 ReceiveQueue 交付的路径
  //（如 DDS req-resp 回调）使用；常规路径走 DeliverFrame。
  Result<std::vector<uint8_t>> DecodeForReceive(
      const std::vector<uint8_t>& frame) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(frame);
    return codec_->Decode(frame);
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

  // 投递一个连接/IO 级错误到接收侧。
  void DeliverError(std::string error) {
    queue_.Push(Result<Message>::Fail(std::move(error)));
  }

  void NotifyDisconnect(const std::string& reason) {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

  // 关闭接收队列（唤醒等待者）。持有者 Close() 应调用。
  void Close() { queue_.Close(); }

  static int64_t NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

 private:
  std::shared_ptr<ICodec> codec_;
  ReceiveQueue queue_;
  DisconnectCallback disconnect_cb_;
};

}  // namespace transport
