#pragma once

// -----------------------------------------------------------------------------
// ITransport.hpp — 所有传输的统一抽象接口
// 生命周期(Open/Close/IsOpen) + 发送(Send / Send+Endpoint 统一寻址) + 三模式接收(Receive/OnReceive/
// AsyncReceive) + 断连通知(OnDisconnect) + 编解码挂载(SetCodec)。
// 具体实现：TcpConnectionImpl / TcpClientImpl / TcpServerImpl(扩展 ITcpServer) 等。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
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

  // 统一寻址发送(非纯虚,基类默认实现):
  //   kDefault → 退化调 Send(data);其余 kind → Fail("io: addressed send not supported")。
  // UdpImpl 覆写支持 kNet,DdsImpl 覆写支持 kTopic;TCP/串口继承默认行为。
  virtual Status Send(const std::vector<uint8_t>& data, const Endpoint& to) {
    if (to.kind == Endpoint::Kind::kDefault) return Send(data);
    return Status::Fail("io: addressed send not supported");
  }

  // topic 路由发送(非纯虚,基类默认):
  //   topic 为空 → 退化 Send(payload, to);否则 → Fail(该实现不支持路由)。
  // TCP/UDP/串口/DDS 覆写以支持 topic→codec 路由。
  virtual Status Send(const Message& msg,
                      const Endpoint& to = Endpoint::Default()) {
    if (msg.topic.empty()) return Send(msg.payload, to);
    return Status::Fail("io: topic routing not supported");
  }

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

  // 为某 topic 注册 codec(topic 路由);基类 no-op,路由能力的实现覆写转发给
  // 自己的 TransportCore。
  virtual void SetCodec(const std::string& topic,
                        std::shared_ptr<ICodec> codec) {
    (void)topic;
    (void)codec;
  }
};

}  // namespace transport
