#include "transport/comm/ProtocolNode.hpp"

#include <utility>

#include "transport/codec/SystemCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"

// ProtocolNode.cpp — 薄壳:命名模式 → 引擎原语 + frm_type 常量。每个方法基本就是一行转发。

namespace transport {

namespace {
// frm_type 枚举 → 抽象 FrameTag(引擎只比较相等)。
int Tag(FrameType t) { return static_cast<int>(t); }
// 建一个出站消息:message_id = 命令码 cmd,payload = p。frm_type/session/protocol 由引擎/policy 盖。
Message Cmd(uint16_t cmd, std::vector<uint8_t> p) { Message m; m.message_id = cmd; m.payload = std::move(p); return m; }
}  // namespace

// ---- Responder:接收角色回应。薄包 engine->SendReply,policy 会按 request 回填 session+message。----

Status ProtocolNode::Responder::Response(std::vector<uint8_t> payload) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(FrameType::kResponse), std::move(payload));
}
Status ProtocolNode::Responder::Result(std::vector<uint8_t> payload) {
  auto e = engine_.lock();
  if (!e) return Status::Fail("conn: node gone");
  return e->SendReply(request_, Tag(FrameType::kResult), std::move(payload));
}

// 构造:建引擎 —— codec 缺省 SystemCodec、policy 是 ProtocolPolicy(带 reply_to_source)。
ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                           ProtocolConfig config, std::unique_ptr<IExecutor> executor,
                           std::size_t queue_capacity)
    : engine_(std::make_shared<InteractionEngine>(
          std::move(transport),
          codec ? std::move(codec) : std::unique_ptr<ICodec>(new SystemCodec()),
          std::unique_ptr<InteractionPolicy>(new ProtocolPolicy(config.protocol_id, config.reply_to_source)),
          std::move(executor), queue_capacity)),
      config_(config) {}

ProtocolNode::~ProtocolNode() { Close(); }

// 把引擎的入站钩子接到本类的虚钩子。都捕获 weak_ptr:回调在 worker 线程跑,节点可能已亡。
void ProtocolNode::WireHandlers() {
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  // 无主 COMMAND/STATE → 当请求:给子类 OnCommand 一个绑住 req 的 Responder。
  engine_->OnInboundRequest([wself, weng](const Message& req) {
    auto s = wself.lock(); if (!s) return;
    s->OnCommand(req, Responder(weng, req));
  });
  // 无主投递里只挑 HEARTBEAT 交 OnHeartbeat(policy 已把心跳路由为 kDeliver)。
  engine_->OnInboundDeliver([wself](const Message& m) {
    auto s = wself.lock(); if (!s) return;
    if (m.frm_type == FrameType::kHeartbeat) s->OnHeartbeat(m);
  });
  engine_->OnError([wself](const std::string& e) { if (auto s = wself.lock()) s->OnError(e); });
}

// Open:先接钩子再开引擎;配了心跳间隔则起一个发 HEARTBEAT 的周期任务(立即首拍)。
Status ProtocolNode::Open() {
  WireHandlers();
  auto st = engine_->Open();
  if (!st) return st;
  if (config_.heartbeat_interval_ms > 0)
    heartbeat_handle_ = engine_->StartPeriodic(Cmd(0, {}), Tag(FrameType::kHeartbeat),
                                               config_.heartbeat_interval_ms);
  return Status::Success(std::monostate{});
}

void ProtocolNode::Close() { engine_->Close(); }
bool ProtocolNode::IsOpen() const { return engine_->IsOpen(); }

// noresponse:单向发 COMMAND。
Status ProtocolNode::SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload, const Endpoint& to) {
  return engine_->Fire(Cmd(cmd, std::move(payload)), Tag(FrameType::kCommand), to);
}

// needresponse:发 COMMAND,等 1 个 RESPONSE(终结)。超时按 config 重发。
Status ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response,
                             const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResponse);
  s.on_terminal = std::move(on_response); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

// withfeedback:发 COMMAND,等 1 个 RESULT(终结)。
Status ProtocolNode::RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result,
                                       const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResult);
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

// needfeedback:发 COMMAND → 收 RESPONSE(中间,调 on_response、停重发)→ 收 RESULT(终结,调
// on_result)→ 引擎自动回一帧 RESPONSE ack(auto_ack_tag)。中间回调包成引擎的 on_intermediate。
Status ProtocolNode::RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result, const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand);
  s.intermediate_tag = Tag(FrameType::kResponse); s.terminal_tag = Tag(FrameType::kResult);
  s.auto_ack_tag = Tag(FrameType::kResponse);
  s.on_intermediate = [cb = std::move(on_response)](const Message& m) { if (cb) cb(Result<Message>::Success(m)); };
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

// repeating(固定快照):周期发 STATE,每拍发同一份 payload。
uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                                      const Endpoint& to) {
  return engine_->StartPeriodic(Cmd(cmd, std::move(payload)), Tag(FrameType::kState), interval_ms, to);
}

// repeating(拉最新):工厂每拍调 state_fn() 取最新 payload,包成 Cmd(cmd, …) 交引擎工厂版。
uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                                      uint32_t interval_ms, const Endpoint& to) {
  if (!state_fn) return 0;                         // 空 state_fn 防御(否则引擎每拍会 bad_function_call)
  return engine_->StartPeriodic(
      [cmd, fn = std::move(state_fn)]() { return Cmd(cmd, fn()); },
      Tag(FrameType::kState), interval_ms, to);
}

// 推送更新:重建 Cmd(cmd, payload) 交引擎 UpdatePeriodic。
bool ProtocolNode::UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload) {
  return engine_->UpdatePeriodic(handle, Cmd(cmd, std::move(payload)));
}

void ProtocolNode::StopRepeating(uint32_t handle) { engine_->StopPeriodic(handle); }

}  // namespace transport
