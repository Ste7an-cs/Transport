#pragma once

// ProtocolNode.hpp — 外部协议交互节点。复用 ITransport + ICodec(默认 SystemCodec)+ IExecutor。
// io 线程 Decode → executor.Post → 单 worker 串行 Dispatch(按 frm_type)。匹配键 (session_id, message_id)。
// 须以 shared_ptr 持有。Task 2:noresponse/needresponse/重发/接收角色;Task 3/4 续加其余模式。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"

namespace transport {

struct ProtocolConfig {
  uint8_t  protocol_id = 0;             // 出站帧的外部系统 id
  uint32_t response_timeout_ms = 1000;  // 等待回应/结果超时
  uint32_t max_retries = 3;             // 超时重发上限(超过即失败)
  uint32_t heartbeat_interval_ms = 0;   // 0 = 关闭心跳
};

class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  // 接收角色应答句柄:回填该请求 (session_id, message_id)。
  // 嵌套于 ProtocolNode,避免与 CommNode 的同名 transport::Responder 冲突(ODR)。
  class Responder {
   public:
    Status Response(std::vector<uint8_t> payload);  // 发 RESPONSE
    Status Result(std::vector<uint8_t> payload);     // 发 RESULT

   private:
    friend class ProtocolNode;
    Responder(std::weak_ptr<ProtocolNode> node, uint8_t session, uint16_t message)
        : node_(std::move(node)), session_(session), message_(message) {}
    std::weak_ptr<ProtocolNode> node_;
    uint8_t session_;
    uint16_t message_;
  };

  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,            // null → SystemCodec(DefaultCrc16)
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,  // null → ThreadExecutor
               std::size_t queue_capacity = 1024);
  virtual ~ProtocolNode();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  Status SendNoResponse(std::vector<uint8_t> payload);          // noresponse
  Status Request(std::vector<uint8_t> payload, ReplyFn on_response);  // needresponse

 protected:
  virtual void OnCommand(const Message& cmd, Responder responder) {}
  virtual void OnHeartbeat(const Message& hb) {}
  virtual void OnError(const std::string& error) {}

 private:
  enum class Mode { kNeedResponse };   // Task 3 续加 kWithResult / kNeedFeedback
  struct Pending {
    Mode mode;
    std::vector<uint8_t> payload;      // 重发用
    uint8_t session; uint16_t message;
    ReplyFn on_response;
    ReplyFn on_result;
    uint32_t retries = 0;
    IExecutor::TimerId timer = 0;
    bool got_response = false;
  };

  Status SendFrame(FrameType type, uint8_t session, uint16_t message,
                   const std::vector<uint8_t>& payload);
  std::pair<uint8_t, uint16_t> NextId();
  static uint32_t Key(uint8_t s, uint16_t m) {
    return (static_cast<uint32_t>(s) << 16) | m;
  }
  void Dispatch(Message msg);
  void OnTimeout(uint32_t key);
  void HandleDisconnect(const std::string& reason);

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<IExecutor> executor_;
  ProtocolConfig config_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;
  std::map<uint32_t, Pending> pending_;
  uint8_t session_ctr_ = 0;
  uint16_t message_ctr_ = 0;
};

}  // namespace transport
