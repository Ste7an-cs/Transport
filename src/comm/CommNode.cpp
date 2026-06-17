#include "transport/comm/CommNode.hpp"

#include <chrono>
#include <random>
#include <utility>
#include <variant>

#include "transport/comm/ThreadExecutor.hpp"

// CommNode.cpp — 见 .hpp。posted 任务/transport 回调捕获 weak_ptr 防悬空;
// pending_ 由 mu_ 保护;超时与 Dispatch 同在业务上下文 → reply/超时恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
int64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

Status Responder::Feedback(Message msg) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendKind(std::move(msg), MessageKind::kFeedback, corr_, to_);
}
Status Responder::Reply(Message msg) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendKind(std::move(msg), MessageKind::kReply, corr_, to_);
}

CommNode::CommNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                   std::unique_ptr<IExecutor> executor, std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))) {
  std::random_device rd;
  id_prefix_ = std::to_string(rd()) + "-";
}

CommNode::~CommNode() { Close(); }

std::string CommNode::NextCorrId() {
  std::lock_guard<std::mutex> lk(mu_);
  return id_prefix_ + std::to_string(++seq_);
}

Status CommNode::Open() {
  executor_->Start();
  std::weak_ptr<CommNode> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string& from) {
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
    for (auto& m : msgs.value) {
      m.source = from;
      m.timestamp = NowMicros();
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
    }
  });
  transport_->OnConnect([wself] {
    if (auto s = wself.lock())
      s->executor_->Post([wself] { if (auto s2 = wself.lock()) s2->OnConnected(); });
  });
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  auto st = transport_->Open();
  if (!st) { executor_->Stop(); return st; }
  open_.store(true);
  return Ok();
}

void CommNode::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<std::string, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.on_final) kv.second.on_final(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();
  transport_->Close();
}

Status CommNode::SendKind(Message msg, MessageKind kind, const std::string& corr,
                          const Endpoint& to) {
  if (!open_.load()) return Status::Fail("config: node not open");
  msg.kind = kind;
  msg.correlation_id = corr;
  auto bytes = codec_->Encode(msg);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value, to);
}

Status CommNode::Send(Message msg, const Endpoint& to) {
  return SendKind(std::move(msg), MessageKind::kOneway, std::string(), to);
}

Status CommNode::RequestImpl(Message msg, FeedbackFn on_feedback, ReplyFn on_final,
                             uint32_t timeout_ms) {
  if (!open_.load()) {
    if (on_final) on_final(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  const std::string corr = NextCorrId();
  std::weak_ptr<CommNode> wself = weak_from_this();
  {  // 登记挂起 + 排超时(原子,防 reply/超时早于登记)
    std::lock_guard<std::mutex> lk(mu_);
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms),
        [wself, corr] { if (auto s = wself.lock()) s->FireTimeout(corr); });
    pending_[corr] = Pending{std::move(on_feedback), std::move(on_final), timer};
  }
  msg.kind = MessageKind::kRequest;
  msg.correlation_id = corr;
  auto bytes = codec_->Encode(msg);
  Status send_st = bytes ? transport_->Send(bytes.value) : Status::Fail(bytes.error);
  if (!send_st) {  // 编码/发送失败 → 回滚挂起 + 立即以该错误终结
    ReplyFn cb;
    { std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(corr);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.on_final); pending_.erase(it); } }
    if (cb) cb(Result<Message>::Fail(send_st.error));
    return send_st;
  }
  return Ok();
}

Status CommNode::Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms) {
  return RequestImpl(std::move(msg), nullptr, std::move(on_reply), timeout_ms);
}
Status CommNode::Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final,
                         uint32_t timeout_ms) {
  return RequestImpl(std::move(msg), std::move(on_feedback), std::move(on_final), timeout_ms);
}
std::future<Result<Message>> CommNode::Request(Message msg, uint32_t timeout_ms) {
  auto prom = std::make_shared<std::promise<Result<Message>>>();
  auto fut = prom->get_future();
  (void)RequestImpl(std::move(msg), nullptr,
                    [prom](Result<Message> r) { prom->set_value(std::move(r)); }, timeout_ms);
  return fut;
}

void CommNode::Dispatch(Message msg) {
  switch (msg.kind) {
    case MessageKind::kReply:
    case MessageKind::kFeedback: {
      const bool is_reply = (msg.kind == MessageKind::kReply);
      FeedbackFn fb; ReplyFn final_cb; bool found = false;
      {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(msg.correlation_id);
        if (it != pending_.end()) {
          found = true;
          if (is_reply) { final_cb = std::move(it->second.on_final); executor_->Cancel(it->second.timer); pending_.erase(it); }
          else { fb = it->second.on_feedback; }  // 拷贝,保留挂起
        }
      }
      if (!found) return;
      if (is_reply) { if (final_cb) final_cb(Result<Message>::Success(std::move(msg))); }
      else { if (fb) fb(msg); }
      break;
    }
    case MessageKind::kRequest:
      OnRequest(msg, Responder(weak_from_this(), msg.correlation_id, Endpoint::Default()));
      break;
    case MessageKind::kOneway:
    case MessageKind::kNotify:
      OnMessage(msg);
      break;
  }
}

void CommNode::FireTimeout(const std::string& corr) {
  ReplyFn cb;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(corr);
    if (it == pending_.end()) return;  // 已被应答
    cb = std::move(it->second.on_final);
    pending_.erase(it);
  }
  if (cb) cb(Result<Message>::Fail("timeout: request timed out"));
}

void CommNode::HandleDisconnect(const std::string& reason) {
  std::map<std::string, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.on_final) kv.second.on_final(Result<Message>::Fail(reason));
  }
  OnDisconnected(reason);
}

}  // namespace transport
