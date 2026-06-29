#pragma once

// =============================================================================
// DdsNode.hpp — DDS 发布-订阅交互节点(薄壳)
//
// 它 = 一个 InteractionEngine + 一个 DdsPolicy + 默认 DdsCodec,另持 IDdsTransport
// 以提供订阅能力。把 DDS 习惯用法翻译成引擎原语:
//   发布 = Fire(kNotify, Endpoint::Topic(t))     —— 一发多收扇出
//   请求 = RequestAwait(kRequest→kReply, Topic)  —— 多路请求-应答
//   应答 = SendReply,经 reply_to 精确回送发起方 inbox(故多客户端打同一服务不串台)
// 周期发布、订阅/退订是 DdsNode 相对 ProtocolNode 多出的能力。
//
// 关键机制:每个节点有自己的 inbox topic;Open 时自动订阅它;发请求时 DdsPolicy 把
// inbox 写进 reply_to → 服务端据此把应答发回本 inbox。用户【继承】本类、重写
// OnMessage/OnRequest。须以 std::shared_ptr 持有。
// =============================================================================

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITraceSink.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionEngine.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

using ReplyFn    = std::function<void(Result<Message>)>;  // 终结回调(成功带应答 / 失败带错误)
using FeedbackFn = std::function<void(const Message&)>;   // 中间反馈回调

class DdsNode : public std::enable_shared_from_this<DdsNode> {
 public:
  // 服务端回应句柄。OnRequest 拿到它,用 Reply(终结)/ Feedback(中间)回送;
  // policy 据原请求的 reply_to 把应答发回发起方 inbox。嵌套在节点内避免与 ProtocolNode::Responder ODR 冲突。
  class Responder {
   public:
    Status Reply(Message msg);     // kReply:终结应答(精确回发起方 inbox)
    Status Feedback(Message msg);  // kFeedback:中间反馈(可多次)
   private:
    friend class DdsNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;
    Message request_;              // 原请求(带 reply_to/correlation_id)
  };

  // transport：DDS 字节管道(也是订阅句柄)。inbox_topic：本节点 inbox(应答回到这)。
  // codec：null → DdsCodec。executor：null → ThreadExecutor。
  DdsNode(std::shared_ptr<IDdsTransport> transport,
          std::string inbox_topic,
          std::unique_ptr<ICodec> codec = nullptr,        // null → DdsCodec
          std::unique_ptr<IExecutor> executor = nullptr,
          std::size_t queue_capacity = 1024);
  virtual ~DdsNode();

  Status Open();        // 接钩子 + engine->Open() + 自动订阅 inbox
  void   Close();
  bool   IsOpen() const;

  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }

  // 订阅/退订 topic(DdsNode 专有;转发给 DDS 传输)。
  Status Subscribe(const std::string& topic);
  Status Unsubscribe(const std::string& topic);

  // 发布:发到 Endpoint::Topic(t)。订了该 topic 的对端在 OnMessage 收到。
  Status Send(Message msg, const Endpoint& to = Endpoint::Default());
  // 请求(回调重载):等 1 个 kReply。to 给服务 topic。
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  // 请求(反馈重载):中间 kFeedback(可多次)+ 终结 kReply。
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  // 请求(future 重载):同步等结果(fut.get())。
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms,
                                       const Endpoint& to = Endpoint::Default());

  // 周期发布(固定样本版):每拍发同一份 msg。
  uint32_t StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to);
  // 周期发布(拉最新版):sample_fn() 在执行器线程上、每次发送前(含立即首拍)被调用,发的是
  // 实时样本。须线程安全、非阻塞、不抛。null sample_fn → 返回 0(不启动)。
  uint32_t StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to);
  bool     UpdatePublishing(uint32_t handle, Message msg);  // 推送更新当前样本;handle 未知→false
  void     StopPublishing(uint32_t handle);

 protected:
  // 接收钩子(子类重写;worker 线程被调)。默认空实现。
  virtual void OnMessage(const Message& /*msg*/) {}                       // 收到发布的样本
  virtual void OnRequest(const Message& /*req*/, Responder /*responder*/) {}  // 收到请求

 private:
  void WireHandlers();                            // 接引擎入站钩子到本类 On*
  std::shared_ptr<IDdsTransport> dds_;            // DDS 传输(订阅句柄;也是引擎的 ITransport)
  std::shared_ptr<InteractionEngine> engine_;     // 持有的引擎
  std::string inbox_topic_;                       // 本节点 inbox(Open 自动订阅)
};

}  // namespace transport
