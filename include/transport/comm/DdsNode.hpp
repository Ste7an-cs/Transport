#pragma once

// DdsNode.hpp — DDS 节点(薄壳)。持 InteractionEngine + DdsPolicy;发布=Fire(Topic),请求=RequestAwait(Topic);
// 另持 IDdsTransport 供 Subscribe;Open 自动订阅 inbox。须以 shared_ptr 持有。

#include <cstddef>
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

using ReplyFn    = std::function<void(Result<Message>)>;
using FeedbackFn = std::function<void(const Message&)>;

class DdsNode : public std::enable_shared_from_this<DdsNode> {
 public:
  class Responder {
   public:
    Status Reply(Message msg);
    Status Feedback(Message msg);
   private:
    friend class DdsNode;
    Responder(std::weak_ptr<InteractionEngine> engine, Message request)
        : engine_(std::move(engine)), request_(std::move(request)) {}
    std::weak_ptr<InteractionEngine> engine_;
    Message request_;
  };

  DdsNode(std::shared_ptr<IDdsTransport> transport,
          std::string inbox_topic,
          std::unique_ptr<ICodec> codec = nullptr,        // null → DdsCodec
          std::unique_ptr<IExecutor> executor = nullptr,
          std::size_t queue_capacity = 1024);
  virtual ~DdsNode();

  Status Open();
  void   Close();
  bool   IsOpen() const;

  void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }

  Status Subscribe(const std::string& topic);
  Status Unsubscribe(const std::string& topic);

  Status Send(Message msg, const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                 const Endpoint& to = Endpoint::Default());
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms,
                                       const Endpoint& to = Endpoint::Default());

 protected:
  virtual void OnMessage(const Message& msg) {}
  virtual void OnRequest(const Message& req, Responder responder) {}

 private:
  void WireHandlers();
  std::shared_ptr<IDdsTransport> dds_;
  std::shared_ptr<InteractionEngine> engine_;
  std::string inbox_topic_;
};

}  // namespace transport
