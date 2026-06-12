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
#include <map>
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
  // 默认 codec：topic 未注册时的兜底；不设则透传。
  void SetCodec(std::shared_ptr<ICodec> codec) { default_codec_ = std::move(codec); }
  // 为某 topic 注册专用 codec。
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) {
    codecs_[topic] = std::move(codec);
  }
  // 选 codec：优先 topic 专用,回退默认(可能为 nullptr → 透传)。
  std::shared_ptr<ICodec> CodecFor(const std::string& topic) const {
    auto it = codecs_.find(topic);
    if (it != codecs_.end()) return it->second;
    return default_codec_;
  }
  Result<Message> Receive(uint32_t timeout_ms) { return queue_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) { queue_.SetCallback(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() { return queue_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) { disconnect_cb_ = std::move(cb); }

  // —— 生产侧（持有者在 io 线程调用）——
  // 发送前编码；无 topic 走默认 codec。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    return EncodeForSend(data, "");
  }
  // 按 topic 选 codec 编码；无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data,
                                             const std::string& topic) {
    auto codec = CodecFor(topic);
    if (!codec) return Result<std::vector<uint8_t>>::Success(data);
    return codec->Encode(data);
  }

  // 接收侧解码；无 codec 时透传。供不经 ReceiveQueue 交付的路径
  //（如 DDS req-resp 回调）使用；常规路径走 DeliverFrame。
  Result<std::vector<uint8_t>> DecodeForReceive(
      const std::vector<uint8_t>& frame) {
    auto codec = CodecFor("");
    if (!codec) return Result<std::vector<uint8_t>>::Success(frame);
    return codec->Decode(frame);
  }

  // 收到一帧/一报文：解码（无 codec 透传）后构造 Message 投递；解码失败投递 Fail。
  void DeliverFrame(std::vector<uint8_t> frame, const std::string& source,
                    const std::string& topic) {
    std::vector<uint8_t> payload;
    auto codec = CodecFor(topic);
    if (codec) {
      auto decoded = codec->Decode(frame);
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
  std::shared_ptr<ICodec> default_codec_;
  std::map<std::string, std::shared_ptr<ICodec>> codecs_;
  ReceiveQueue queue_;
  DisconnectCallback disconnect_cb_;
};

}  // namespace transport
