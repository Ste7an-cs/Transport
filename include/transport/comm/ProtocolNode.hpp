#pragma once

// ProtocolNode.hpp — 外部协议节点(薄壳)。持 InteractionEngine + ProtocolPolicy;
// 命名模式翻译成引擎原语 + frm_type 常量。须以 shared_ptr 持有。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/ICodec.hpp"
#include "transport/ITraceSink.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionEngine.hpp"

namespace transport {

struct ProtocolConfig {
  uint8_t  protocol_id = 0;
  uint32_t response_timeout_ms = 1000;
  uint32_t max_retries = 3;
  uint32_t heartbeat_interval_ms = 0;
  bool     reply_to_source = false;    // 1:多 UDP 置 true:应答/ack 回到入站来源 ip:port。
                                       // 不只服务端 Responder——客户端 needfeedback 的自动 ack
                                       // 也走此路:1:多 UDP 客户端同样须置 true,否则 ack 发往
                                       // default_dest_(错的对端)。TCP/串口/1:1 留 false。
};

class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  class Responder {
   public:
    Status Response(std::vector<uint8_t> payload);
    Status Result(std::vector<uint8_t> payload);
   private:
    friend class ProtocolNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;
    Message request_;
  };

  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,          // null → SystemCodec
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,
               std::size_t queue_capacity = 1024);
  virtual ~ProtocolNode();

  Status Open();
  void   Close();
  bool   IsOpen() const;

  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }

  Status SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload,
                        const Endpoint& to = Endpoint::Default());
  Status Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response,
                 const Endpoint& to = Endpoint::Default());
  Status RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result,
                           const Endpoint& to = Endpoint::Default());
  Status RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                             ReplyFn on_response, ReplyFn on_result,
                             const Endpoint& to = Endpoint::Default());
  uint32_t StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                          const Endpoint& to = Endpoint::Default());
  // state_fn() 在执行器线程上、每次发送前(含立即首拍)被调用;须线程安全、非阻塞、不抛。
  // null state_fn → 返回 0(不启动)。
  uint32_t StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                          uint32_t interval_ms, const Endpoint& to = Endpoint::Default());
  bool     UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload);
  void     StopRepeating(uint32_t handle);

 protected:
  virtual void OnCommand(const Message& /*cmd*/, Responder /*responder*/) {}
  virtual void OnHeartbeat(const Message& /*hb*/) {}
  virtual void OnError(const std::string& /*error*/) {}

 private:
  void WireHandlers();
  std::shared_ptr<InteractionEngine> engine_;
  ProtocolConfig config_;
  uint32_t heartbeat_handle_ = 0;
};

}  // namespace transport
