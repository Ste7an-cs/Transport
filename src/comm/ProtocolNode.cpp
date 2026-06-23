#include "transport/comm/ProtocolNode.hpp"

#include <chrono>
#include <utility>
#include <variant>

#include "transport/codec/SystemCodec.hpp"
#include "transport/comm/ThreadExecutor.hpp"

// ProtocolNode.cpp — 见 .hpp。posted 任务/transport 回调捕获 weak_ptr;
// pending_ 由 mu_ 保护;Encode+Send 在锁外;超时与 Dispatch 同在 worker → 恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

Status ProtocolNode::Responder::Response(std::vector<uint8_t> payload) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendFrame(FrameType::kResponse, session_, message_, payload);
}
Status ProtocolNode::Responder::Result(std::vector<uint8_t> payload) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendFrame(FrameType::kResult, session_, message_, payload);
}

ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                           ProtocolConfig config, std::unique_ptr<IExecutor> executor,
                           std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(codec ? std::move(codec) : std::unique_ptr<ICodec>(new SystemCodec())),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))),
      config_(config) {}

ProtocolNode::~ProtocolNode() { Close(); }

std::pair<uint8_t, uint16_t> ProtocolNode::NextId() {
  std::lock_guard<std::mutex> lk(mu_);
  uint8_t s = session_ctr_++;
  uint16_t m = message_ctr_++;
  return {s, m};
}

Status ProtocolNode::Open() {
  executor_->Start();
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string&) {
    auto s = wself.lock();
    if (!s) return;
    if (!r) {
      std::string e = r.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    for (auto& m : msgs.value)
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
  });
  transport_->OnConnect([] {});
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  open_.store(true);
  auto st = transport_->Open();
  if (!st) { open_.store(false); executor_->Stop(); return st; }
  if (config_.heartbeat_interval_ms > 0) ScheduleHeartbeat();
  return Ok();
}

void ProtocolNode::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<uint32_t, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    auto& p = kv.second;
    if (p.on_response) p.on_response(Result<Message>::Fail("conn: node closed"));
    if (p.on_result) p.on_result(Result<Message>::Fail("conn: node closed"));
  }
  { std::lock_guard<std::mutex> lk(mu_);
    if (heartbeat_timer_) { executor_->Cancel(heartbeat_timer_); heartbeat_timer_ = 0; }
    for (auto& kv : repeats_) if (kv.second.timer) executor_->Cancel(kv.second.timer);
    repeats_.clear(); }
  executor_->Stop();
  transport_->Close();
}

Status ProtocolNode::SendFrame(FrameType type, uint8_t session, uint16_t message,
                               const std::vector<uint8_t>& payload) {
  if (!open_.load()) return Status::Fail("config: node not open");
  Message m;
  m.frm_type = type;
  m.protocol_id = config_.protocol_id;
  m.session_id = session;
  m.message_id = message;
  m.payload = payload;
  auto bytes = codec_->Encode(m);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value);
}

Status ProtocolNode::SendNoResponse(std::vector<uint8_t> payload) {
  auto id = NextId();
  return SendFrame(FrameType::kCommand, id.first, id.second, payload);
}

Status ProtocolNode::Request(std::vector<uint8_t> payload, ReplyFn on_response) {
  return RequestImpl(Mode::kNeedResponse, std::move(payload), std::move(on_response), nullptr);
}
Status ProtocolNode::RequestWithResult(std::vector<uint8_t> payload, ReplyFn on_result) {
  return RequestImpl(Mode::kWithResult, std::move(payload), nullptr, std::move(on_result));
}
Status ProtocolNode::RequestNeedFeedback(std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result) {
  return RequestImpl(Mode::kNeedFeedback, std::move(payload),
                     std::move(on_response), std::move(on_result));
}

Status ProtocolNode::RequestImpl(Mode mode, std::vector<uint8_t> payload,
                                 ReplyFn on_response, ReplyFn on_result) {
  if (!open_.load()) {
    if (on_response) on_response(Result<Message>::Fail("config: node not open"));
    if (on_result) on_result(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  auto id = NextId();
  const uint32_t key = Key(id.first, id.second);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {
      if (on_response) on_response(Result<Message>::Fail("conn: node closing"));
      if (on_result) on_result(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p;
    p.mode = mode; p.payload = payload;
    p.session = id.first; p.message = id.second;
    p.on_response = std::move(on_response); p.on_result = std::move(on_result);
    p.timer = timer;
    pending_[key] = std::move(p);
  }
  Status st = SendFrame(FrameType::kCommand, id.first, id.second, payload);
  if (!st) {
    ReplyFn cr, cf;
    { std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) {
        executor_->Cancel(it->second.timer);
        cr = std::move(it->second.on_response); cf = std::move(it->second.on_result);
        pending_.erase(it);
      } }
    if (cr) cr(Result<Message>::Fail(st.error));
    if (cf) cf(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}

void ProtocolNode::OnTimeout(uint32_t key) {
  ReplyFn fail_cb;
  bool resend = false; FrameType rt = FrameType::kCommand;
  uint8_t s = 0; uint16_t m = 0; std::vector<uint8_t> payload;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;
    Pending& p = it->second;
    const bool no_retransmit = (p.mode == Mode::kNeedFeedback && p.got_response);
    if (!no_retransmit && p.retries < config_.max_retries) {
      ++p.retries;
      std::weak_ptr<ProtocolNode> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
          [wself, key] { if (auto s2 = wself.lock()) s2->OnTimeout(key); });
      resend = true; s = p.session; m = p.message; payload = p.payload;
    } else {
      fail_cb = p.on_result ? std::move(p.on_result) : std::move(p.on_response);
      pending_.erase(it);
    }
  }
  if (resend) (void)SendFrame(rt, s, m, payload);
  if (fail_cb) fail_cb(Result<Message>::Fail("timeout: request timed out"));
}

void ProtocolNode::Dispatch(Message msg) {
  switch (msg.frm_type) {
    case FrameType::kCommand:
    case FrameType::kState:
      OnCommand(msg, Responder(weak_from_this(), msg.session_id, msg.message_id));
      break;
    case FrameType::kResponse: {
      const uint32_t key = Key(msg.session_id, msg.message_id);
      ReplyFn cb;
      { std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
          Pending& p = it->second;
          if (p.mode == Mode::kNeedResponse) {
            cb = std::move(p.on_response); executor_->Cancel(p.timer); pending_.erase(it);
          } else if (p.mode == Mode::kNeedFeedback && !p.got_response) {
            // 中间回应:消费 on_response(保留挂起等 RESULT),清空防 Close/disconnect 二次触发。
            p.got_response = true; cb = std::move(p.on_response);
          }
        } }
      if (cb) cb(Result<Message>::Success(std::move(msg)));
      break;
    }
    case FrameType::kResult: {
      const uint32_t key = Key(msg.session_id, msg.message_id);
      ReplyFn cb; bool ack = false; uint8_t s = 0; uint16_t m = 0;
      { std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
          Pending& p = it->second;
          if (p.mode == Mode::kWithResult || p.mode == Mode::kNeedFeedback) {
            cb = std::move(p.on_result); executor_->Cancel(p.timer);
            ack = (p.mode == Mode::kNeedFeedback); s = p.session; m = p.message;
            pending_.erase(it);
          }
        } }
      if (ack) (void)SendFrame(FrameType::kResponse, s, m, {});  // needfeedback 自动回 RESPONSE
      if (cb) cb(Result<Message>::Success(std::move(msg)));
      break;
    }
    case FrameType::kHeartbeat:
      OnHeartbeat(msg);
      break;
    case FrameType::kUnknown:
      OnError("codec: unknown frame type");
      break;
  }
}

void ProtocolNode::HandleDisconnect(const std::string& reason) {
  std::map<uint32_t, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    auto& p = kv.second;
    if (p.on_response) p.on_response(Result<Message>::Fail(reason));
    if (p.on_result) p.on_result(Result<Message>::Fail(reason));
  }
}

void ProtocolNode::ScheduleHeartbeat() {
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  heartbeat_timer_ = executor_->ScheduleAt(
      std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.heartbeat_interval_ms),
      [wself] {
        auto s = wself.lock();
        if (!s || !s->open_.load()) return;
        auto id = s->NextId();
        (void)s->SendFrame(FrameType::kHeartbeat, id.first, id.second, {});
        s->ScheduleHeartbeat();  // 重排
      });
}

uint32_t ProtocolNode::StartRepeating(std::vector<uint8_t> payload, uint32_t interval_ms) {
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = repeat_next_++;
    repeats_[handle] = Repeat{payload, interval_ms, 0};
  }
  // 起始即发一帧,并排下一次。
  auto id = NextId();
  (void)SendFrame(FrameType::kState, id.first, id.second, payload);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = repeats_.find(handle);
  if (it != repeats_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FireRepeat(handle); });
  return handle;
}

void ProtocolNode::FireRepeat(uint32_t handle) {
  std::vector<uint8_t> payload; uint32_t interval = 0; bool alive = false;
  { std::lock_guard<std::mutex> lk(mu_);
    auto it = repeats_.find(handle);
    if (it != repeats_.end()) { payload = it->second.payload; interval = it->second.interval_ms; alive = true; } }
  if (!alive || !open_.load()) return;
  auto id = NextId();
  (void)SendFrame(FrameType::kState, id.first, id.second, payload);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = repeats_.find(handle);
  if (it != repeats_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FireRepeat(handle); });
}

void ProtocolNode::StopRepeating(uint32_t handle) {
  IExecutor::TimerId t = 0;
  { std::lock_guard<std::mutex> lk(mu_);
    auto it = repeats_.find(handle);
    if (it != repeats_.end()) { t = it->second.timer; repeats_.erase(it); } }
  if (t) executor_->Cancel(t);
}

}  // namespace transport
