#include "transport/comm/ProtocolNode.hpp"

#include <utility>

#include "transport/codec/SystemCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"

// ProtocolNode.cpp — 薄壳:命名模式 → 引擎原语 + frm_type 常量。

namespace transport {

namespace {
int Tag(FrameType t) { return static_cast<int>(t); }
Message Cmd(uint16_t cmd, std::vector<uint8_t> p) { Message m; m.message_id = cmd; m.payload = std::move(p); return m; }
}  // namespace

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

void ProtocolNode::WireHandlers() {
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::weak_ptr<InteractionEngine> weng = engine_;
  engine_->OnInboundRequest([wself, weng](const Message& req) {
    auto s = wself.lock(); if (!s) return;
    s->OnCommand(req, Responder(weng, req));
  });
  engine_->OnInboundDeliver([wself](const Message& m) {
    auto s = wself.lock(); if (!s) return;
    if (m.frm_type == FrameType::kHeartbeat) s->OnHeartbeat(m);
  });
  engine_->OnError([wself](const std::string& e) { if (auto s = wself.lock()) s->OnError(e); });
}

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

Status ProtocolNode::SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload) {
  return engine_->Fire(Cmd(cmd, std::move(payload)), Tag(FrameType::kCommand));
}

Status ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResponse);
  s.on_terminal = std::move(on_response); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

Status ProtocolNode::RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResult);
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

Status ProtocolNode::RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand);
  s.intermediate_tag = Tag(FrameType::kResponse); s.terminal_tag = Tag(FrameType::kResult);
  s.auto_ack_tag = Tag(FrameType::kResponse);
  s.on_intermediate = [cb = std::move(on_response)](const Message& m) { if (cb) cb(Result<Message>::Success(m)); };
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s));
}

uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms) {
  return engine_->StartPeriodic(Cmd(cmd, std::move(payload)), Tag(FrameType::kState), interval_ms);
}

void ProtocolNode::StopRepeating(uint32_t handle) { engine_->StopPeriodic(handle); }

}  // namespace transport
