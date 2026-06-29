#include "transport/comm/DdsNode.hpp"

#include <utility>

#include "transport/codec/DdsCodec.hpp"
#include "transport/comm/DdsPolicy.hpp"

// DdsNode.cpp — 薄壳:发布=Fire(kNotify,Topic),请求=RequestAwait(kRequest/kReply,Topic);Open 订阅 inbox。

namespace transport {

namespace {
// MessageKind 枚举 → 抽象 FrameTag。
int Tag(MessageKind k) { return static_cast<int>(k); }
}  // namespace

// ---- Responder:服务端回应。policy 据 req.reply_to 把应答精确回送发起方 inbox。----
Status DdsNode::Responder::Reply(Message msg) {        // 终结应答(kReply)
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kReply), std::move(msg.payload));
}
Status DdsNode::Responder::Feedback(Message msg) {     // 中间反馈(kFeedback,可多次)
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(MessageKind::kFeedback), std::move(msg.payload));
}

// 构造:transport 同时充当引擎的 ITransport(发布/收样本)与本节点订阅句柄 dds_(Subscribe)。
// codec 缺省 DdsCodec;policy 是 DdsPolicy(inbox)。
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

// 把引擎入站钩子接到本类虚钩子(捕获 weak_ptr,worker 线程跑)。
void DdsNode::WireHandlers() {
  std::weak_ptr<DdsNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  engine_->OnInboundRequest([wself, weng](const Message& req) {   // kRequest → OnRequest
    if (auto s = wself.lock()) s->OnRequest(req, Responder(weng, req));
  });
  engine_->OnInboundDeliver([wself](const Message& m) {           // 发布(kOneway/kNotify)→ OnMessage
    if (auto s = wself.lock()) s->OnMessage(m);
  });
}

// Open:接钩子 → 开引擎 → 自动订阅本节点 inbox(应答要回到这,必须先订上)。
Status DdsNode::Open() {
  WireHandlers();
  auto st = engine_->Open();
  if (!st) return st;
  return Subscribe(inbox_topic_);
}

void DdsNode::Close() { engine_->Close(); }
bool DdsNode::IsOpen() const { return engine_->IsOpen(); }

// 订阅/退订:DdsNode 比普通节点多出的能力,直接转发给 DDS 传输。
Status DdsNode::Subscribe(const std::string& topic)   { return dds_->Subscribe(topic); }
Status DdsNode::Unsubscribe(const std::string& topic) { return dds_->Unsubscribe(topic); }

// 发布:单向发到 Endpoint::Topic(t)(kNotify);订了该 topic 的对端在 OnMessage 收到(1 发多收扇出)。
Status DdsNode::Send(Message msg, const Endpoint& to) {
  return engine_->Fire(std::move(msg), Tag(MessageKind::kNotify), to);
}

// ---- 周期发布:同 Send(kNotify),交引擎 periodic。固定 / 每拍取最新 / 推送更新 / 停 ----
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

// ---- 多路请求-应答:发到服务 topic;DdsPolicy 自动把本节点 inbox 写进 reply_to → 应答回到本 inbox ----
// 回调重载:等 1 个 kReply。DDS 一般不重发(max_retries=0)。
Status DdsNode::Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms, const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_terminal = std::move(on_reply); s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

// 反馈重载:中间 kFeedback(可多次)+ 终结 kReply。
Status DdsNode::Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final, uint32_t timeout_ms,
                        const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(MessageKind::kRequest);
  s.intermediate_tag = Tag(MessageKind::kFeedback); s.terminal_tag = Tag(MessageKind::kReply);
  s.on_intermediate = std::move(on_feedback); s.on_terminal = std::move(on_final);
  s.timeout_ms = timeout_ms; s.max_retries = 0;
  return engine_->RequestAwait(std::move(msg), std::move(s), to);
}

// future 重载:把终结回调桥接到 promise,调用方 fut.get() 同步等。引擎在所有失败路径也会调
// on_terminal,故 promise 必被兑现(fut.get() 不会永久阻塞)。
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
