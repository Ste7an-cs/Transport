#pragma once

// =============================================================================
// ProtocolNode.hpp — 对接外部系统协议帧的交互节点(薄壳)
//
// 它 = 一个 InteractionEngine + 一个 ProtocolPolicy + 默认 SystemCodec。本身几乎没有
// 逻辑:把"5 种发送交互模式 + 收发双角色 + 周期 + 心跳"这套【命名 API】翻译成引擎的
// 3 原语(Fire/RequestAwait/StartPeriodic)并配上正确的 frm_type 判别符。挂起表、超时、
// 重发、并发都在引擎里,这里看不到。
//
// 5 种发送模式(每条命令属其一,首参 cmd = message_id 命令码):
//   SendNoResponse      发后即完(Fire COMMAND)
//   Request             需回应:等 1 个 RESPONSE
//   RequestWithResult   需结果:等 1 个 RESULT
//   RequestNeedFeedback 收 RESPONSE(中间)→ 收 RESULT(终结),引擎自动回 RESPONSE ack
//   StartRepeating      周期发 STATE(可传 state_fn 每拍取最新)
// 接收角色:OnCommand(收 COMMAND/STATE)/ OnHeartbeat / OnError;经 Responder 回应。
//
// 用户【继承】本类、重写钩子。须以 std::shared_ptr 持有。
// =============================================================================

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

// 节点行为配置。
struct ProtocolConfig {
  uint8_t  protocol_id = 0;            // 外部系统 id(盖到出站帧)
  uint32_t response_timeout_ms = 1000; // 请求等回应的单次超时
  uint32_t max_retries = 3;           // 超时重发上限(达上限仍无回应 → timeout: 失败)
  uint32_t heartbeat_interval_ms = 0; // >0 → Open 后周期发 HEARTBEAT;0 = 不发心跳
  bool     reply_to_source = false;    // 1:多 UDP 置 true:应答/ack 回到入站来源 ip:port。
                                       // 不只服务端 Responder——客户端 needfeedback 的自动 ack
                                       // 也走此路:1:多 UDP 客户端同样须置 true,否则 ack 发往
                                       // default_dest_(错的对端)。TCP/串口/1:1 留 false。
};

class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  // 接收角色收到 COMMAND/STATE 时,OnCommand 拿到一个 Responder,用它回 RESPONSE/RESULT。
  // Responder 绑住原请求(里面有 session/message/来源),薄包 engine->SendReply。
  // 嵌套在节点内(与 DdsNode::Responder 同名),避免同一二进制里的 ODR 冲突。
  class Responder {
   public:
    Status Response(std::vector<uint8_t> payload);  // 回 RESPONSE 帧(回填请求的 session+message)
    Status Result(std::vector<uint8_t> payload);    // 回 RESULT 帧
   private:
    friend class ProtocolNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;       // weak:回应时引擎可能已关
    Message request_;                               // 原请求(供 EchoCorrelation/ReplyTo)
  };

  // codec 传 null → 默认 SystemCodec(外部协议帧,流式;UDP 应改注入 SystemDatagramCodec)。
  // executor 传 null → 默认 ThreadExecutor。
  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,          // null → SystemCodec
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,
               std::size_t queue_capacity = 1024);
  virtual ~ProtocolNode();                            // = Close()

  Status Open();        // 接好入站钩子 + engine->Open();heartbeat_interval_ms>0 时起心跳
  void   Close();       // engine->Close()(幂等)
  bool   IsOpen() const;

  // 把 trace sink 透传给引擎(须 Open 前调)。
  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }

  // ---- 5 种发送交互模式(发起角色)。首参 cmd = message_id 命令码;to 可选目的地(1:多 UDP)----
  Status SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload,     // 发后即完
                        const Endpoint& to = Endpoint::Default());
  Status Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response,  // 等 RESPONSE
                 const Endpoint& to = Endpoint::Default());
  Status RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result,  // 等 RESULT
                           const Endpoint& to = Endpoint::Default());
  Status RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,  // 收 RESPONSE→RESULT,自动回 ack
                             ReplyFn on_response, ReplyFn on_result,
                             const Endpoint& to = Endpoint::Default());
  // 周期发 STATE(固定快照版):每拍发同一份 payload。
  uint32_t StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                          const Endpoint& to = Endpoint::Default());
  // 周期发 STATE(拉最新版):state_fn() 在执行器线程上、每次发送前(含立即首拍)被调用,
  // 发的是实时状态而非启动快照。须线程安全、非阻塞、不抛。null state_fn → 返回 0(不启动)。
  uint32_t StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                          uint32_t interval_ms, const Endpoint& to = Endpoint::Default());
  // 推送更新某周期任务的当前 STATE(事件驱动);需再给 cmd 以重建帧。handle 未知 → false。
  bool     UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload);
  void     StopRepeating(uint32_t handle);

 protected:
  // 接收角色钩子(子类重写;在 worker 线程被调)。默认空实现。
  virtual void OnCommand(const Message& /*cmd*/, Responder /*responder*/) {}  // 收 COMMAND/STATE
  virtual void OnHeartbeat(const Message& /*hb*/) {}                          // 收 HEARTBEAT
  virtual void OnError(const std::string& /*error*/) {}                       // 传输/解码错误上报

 private:
  void WireHandlers();                            // 把引擎入站钩子接到本类的 On* 钩子
  std::shared_ptr<InteractionEngine> engine_;     // 持有的引擎(本节点独占)
  ProtocolConfig config_;
  uint32_t heartbeat_handle_ = 0;                 // 心跳 periodic 的 handle(0 = 未起)
};

}  // namespace transport
