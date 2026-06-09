#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

class ITransport {
 public:
  virtual ~ITransport() = default;

  using ReceiveCallback = std::function<void(Result<Message>)>;
  using DisconnectCallback = std::function<void(const std::string& reason)>;

  // 生命周期
  virtual Status Open() = 0;
  virtual void Close() = 0;
  virtual bool IsOpen() const = 0;

  // 发送（若已设置 ICodec，自动 Encode 后传输）。data 自带长度。
  virtual Status Send(const std::vector<uint8_t>& data) = 0;

  // 同步接收（阻塞至收到数据或超时；timeout_ms == 0 表示永久阻塞）
  virtual Result<Message> Receive(uint32_t timeout_ms = 0) = 0;

  // 异步接收 —— 回调模式（回调在内部 I/O 线程执行，必须非阻塞）
  virtual void OnReceive(ReceiveCallback cb) = 0;

  // 异步接收 —— future 模式（每次调用消费一条到来的消息）
  virtual std::future<Result<Message>> AsyncReceive() = 0;

  // 断连通知（TCP 客户端、串口适用）
  virtual void OnDisconnect(DisconnectCallback cb) = 0;

  // 挂载编解码器；未设置时原始字节直接透传
  virtual void SetCodec(std::shared_ptr<ICodec> codec) = 0;
};

}  // namespace transport
