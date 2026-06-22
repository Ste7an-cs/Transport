#pragma once

// CommNode.hpp — 用户继承的交互模式基类。持有 Transport + ICodec + IExecutor;
// io 线程 Decode → executor.Post → 业务上下文 Dispatch(按 kind);Request 用 executor.ScheduleAt 超时。
// 须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"

namespace transport {

using ReplyFn    = std::function<void(Result<Message>)>;
using FeedbackFn = std::function<void(const Message&)>;

class CommNode;

class Responder {  // 服务端应答句柄:绑定该请求 correlation_id + 回发目的地
 public:
  Status Feedback(Message msg);  // 发 kFeedback(可多次)
  Status Reply(Message msg);     // 发 kReply(终结,一次)

 private:
  friend class CommNode;
  Responder(std::weak_ptr<CommNode> node, std::string corr, Endpoint to)
      : node_(std::move(node)), corr_(std::move(corr)), to_(std::move(to)) {}
  std::weak_ptr<CommNode> node_;
  std::string corr_;
  Endpoint to_;
};

class CommNode : public std::enable_shared_from_this<CommNode> {
 public:
  CommNode(std::shared_ptr<ITransport> transport,
           std::unique_ptr<ICodec> codec,
           std::unique_ptr<IExecutor> executor = nullptr,
           std::size_t queue_capacity = 1024);
  virtual ~CommNode();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  Status Send(Message msg, const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms,
                                       const Endpoint& to = Endpoint::Default());

 protected:
  std::string reply_address_;  // 出站请求填入 msg.reply_to(默认空=p2p;DdsNode 设为 inbox topic)
  virtual void OnMessage(const Message& msg) {}
  virtual void OnRequest(const Message& req, Responder responder) {}
  virtual void OnConnected() {}
  virtual void OnDisconnected(const std::string& reason) {}
  virtual void OnError(const std::string& error) {}

 private:
  friend class Responder;
  Status SendKind(Message msg, MessageKind kind, const std::string& corr, const Endpoint& to);
  Status RequestImpl(Message msg, FeedbackFn on_feedback, ReplyFn on_final,
                     uint32_t timeout_ms, const Endpoint& to);
  void Dispatch(Message msg);
  void HandleDisconnect(const std::string& reason);
  void FireTimeout(const std::string& corr);
  std::string NextCorrId();

  struct Pending { FeedbackFn on_feedback; ReplyFn on_final; IExecutor::TimerId timer; };

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<IExecutor> executor_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;                            // 保护 pending_ + seq_
  std::map<std::string, Pending> pending_;
  uint64_t seq_ = 0;
  std::string id_prefix_;
};

}  // namespace transport
