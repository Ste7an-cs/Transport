#include "transport/comm/DdsNode.hpp"

#include <utility>

#include "transport/codec/DdsCodec.hpp"
#include "transport/comm/DdsPolicy.hpp"

// DdsNode.cpp — 薄壳:发布=Fire(kNotify,Topic),请求=RequestAwait(kRequest/kReply,Topic);Open 订阅 inbox。

namespace transport {

namespace {
int Tag(MessageKind k) { return static_cast<int>(k); }
}  // namespace

Status DdsNode::Responder::Reply(Message msg) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kReply), std::move(msg.payload));
}
Status DdsNode::Responder::Feedback(Message msg) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kFeedback), std::move(msg.payload));
}

DdsNode::DdsNode(std::shared_ptr<IDdsTransport> transport, std::string inbox_topic,
                 std::unique_ptr<ICodec> codec, std::unique_ptr<IExecutor> executor,
                 std::size_t queue_capacity)
    : dds_(transport),
      engine_(std::make_shared<InteractionEngine>(
          transport,
          codec ? std::move(codec) : std::unique_ptr<ICodec>(new DdsCodec()),
          std::unique_ptr<InteractionPolicy>(new DdsPolicy(inbox_topic)),
          std::move(executor), queue_capacity)),
      inbox_topic_(std::move(inbox_topic)) {}

DdsNode::~DdsNode() { Close(); }

void DdsNode::WireHandlers() {
  std::weak_ptr<DdsNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  engine_->OnInboundRequest([wself, weng](const Message& req) {
    if (auto s = wself.lock()) s->OnRequest(req, Responder(weng, req));
  });
  engine_->OnInboundDeliver([wself](const Message& m) {
    if (auto s = wself.lock()) s->OnMessage(m);
  });
}

Status DdsNode::Open() {
  WireHandlers();
  auto st = engine_->Open();
  if (!st) return st;
  return Subscribe(inbox_topic_);
}

void DdsNode::Close() { engine_->Close(); }
bool DdsNode::IsOpen() const { return engine_->IsOpen(); }

Status DdsNode::Subscribe(const std::string& topic)   { return dds_->Subscribe(topic); }
Status DdsNode::Unsubscribe(const std::string& topic) { return dds_->Unsubscribe(topic); }

Status DdsNode::Send(Message msg, const Endpoint& to) {
  return engine_->Fire(std::move(msg), Tag(MessageKind::kNotify), to);
}

uint32_t DdsNode::StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to) {
  return engine_->StartPeriodic(std::move(msg), Tag(MessageKind::kNotify), interval_ms, to);
}
uint32_t DdsNode::StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to) {
  return engine_->StartPeriodic(std::move(sample_fn), Tag(MessageKind::kNotify), interval_ms, to);
}
bool DdsNode::UpdatePublishing(uint32_t handle, Message msg) {
  return engine_->UpdatePeriodic(handle, std::move(msg));
}
void DdsNode::StopPublishing(uint32_t handle) { engine_->StopPeriodic(handle); }

Status DdsNode::Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms, const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_terminal = std::move(on_reply); s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

Status DdsNode::Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                        const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest);
  s.intermediate_tag = Tag(MessageKind::kFeedback); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_intermediate = std::move(on_feedback); s.on_terminal = std::move(on_final);
  s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

std::future<Result<Message>> DdsNode::Request(Message msg, uint32_t timeout_ms, const Endpoint& to) {
  auto prom = std::make_shared<std::promise<Result<Message>>>();
  auto fut = prom->get_future();
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_terminal = [prom](Result<Message> r) { prom->set_value(std::move(r)); };
  s.timeout_ms = timeout_ms; s.max_retries = 0;
  (void)engine_->RequestAwait(std::move(msg), std::move(s), to);
  return fut;
}

}  // namespace transport
