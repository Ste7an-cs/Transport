#include "transport/comm/InteractionEngine.hpp"

#include <chrono>
#include <utility>
#include <variant>

#include "transport/comm/ThreadExecutor.hpp"

// InteractionEngine.cpp — 见 .hpp。机制一份;并发纪律集中此处。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

InteractionEngine::InteractionEngine(std::shared_ptr<ITransport> transport,
                                     std::unique_ptr<ICodec> codec,
                                     std::unique_ptr<InteractionPolicy> policy,
                                     std::unique_ptr<IExecutor> executor,
                                     std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      policy_(std::move(policy)),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))) {}

InteractionEngine::~InteractionEngine() { Close(); }

Status InteractionEngine::Open() {
  executor_->Start();
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string& from) {
    auto s = wself.lock();
    if (!s) return;
    if (!r) {
      std::string e = r.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      return;
    }
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      return;
    }
    for (auto& m : msgs.value) {
      m.source = from;
      if (m.topic.empty()) m.topic = from;
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
    }
  });
  transport_->OnConnect([] {});
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  open_.store(true);
  auto st = transport_->Open();
  if (!st) { open_.store(false); executor_->Stop(); return st; }
  return Ok();
}

void InteractionEngine::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<Key, Pending> taken;
  {
    std::lock_guard<std::mutex> lk(mu_);
    taken.swap(pending_);
    for (auto& kv : periodics_) if (kv.second.timer) executor_->Cancel(kv.second.timer);
    periodics_.clear();
  }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();
  transport_->Close();
}

Status InteractionEngine::SendMessage(Message& m, const Endpoint& to) {
  if (!open_.load()) return Status::Fail("config: node not open");
  auto bytes = codec_->Encode(m);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value, to);
}

Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  return SendMessage(out, to);
}

Status InteractionEngine::RequestAwait(Message out, RequestSpec spec, const Endpoint& to) {
  if (!open_.load()) {
    if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  policy_->SetTag(out, spec.request_tag);
  Key key;
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {
      if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    key = policy_->NewCorrelation(out);
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(spec.timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p; p.spec = std::move(spec); p.out = out; p.to = to; p.timer = timer;
    pending_[key] = std::move(p);
  }
  Status st = SendMessage(out, to);
  if (!st) {
    std::function<void(Result<Message>)> cb;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.spec.on_terminal); pending_.erase(it); }
    }
    if (cb) cb(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}

void InteractionEngine::OnTimeout(Key key) {
  std::function<void(Result<Message>)> fail_cb;
  bool resend = false; Message rs; Endpoint rto;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;
    Pending& p = it->second;
    if (!p.advanced && p.retries < p.spec.max_retries) {
      ++p.retries;
      std::weak_ptr<InteractionEngine> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(p.spec.timeout_ms),
          [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
      resend = true; rs = p.out; rto = p.to;
    } else {
      fail_cb = std::move(p.spec.on_terminal);
      pending_.erase(it);
    }
  }
  if (resend) (void)SendMessage(rs, rto);
  if (fail_cb) fail_cb(Result<Message>::Fail("timeout: request timed out"));
}

void InteractionEngine::Dispatch(Message msg) {
  const Key key = policy_->KeyOf(msg);
  const FrameTag tag = policy_->TagOf(msg);
  std::function<void(const Message&)> inter_cb;
  std::function<void(Result<Message>)> term_cb;
  bool auto_ack = false; FrameTag ack_tag = 0; Message ack_req;
  bool matched = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it != pending_.end()) {
      matched = true;
      Pending& p = it->second;
      if (tag == p.spec.terminal_tag) {
        p.advanced = true;
        executor_->Cancel(p.timer);
        term_cb = std::move(p.spec.on_terminal);
        if (p.spec.auto_ack_tag.has_value()) { auto_ack = true; ack_tag = *p.spec.auto_ack_tag; ack_req = msg; }
        pending_.erase(it);
      } else if (p.spec.intermediate_tag.has_value() && tag == *p.spec.intermediate_tag) {
        p.advanced = true;
        inter_cb = p.spec.on_intermediate;  // 拷贝,保留挂起
      }
      // 否则:命中 key 但意外 tag → 忽略
    }
  }
  if (matched) {
    if (auto_ack) (void)SendReply(ack_req, ack_tag, {});  // 锁外
    if (term_cb) term_cb(Result<Message>::Success(std::move(msg)));
    else if (inter_cb) inter_cb(msg);
    return;
  }
  switch (policy_->RouteUnmatched(msg)) {
    case InteractionPolicy::Route::kInboundRequest: if (on_request_) on_request_(msg); break;
    case InteractionPolicy::Route::kDeliver:        if (on_deliver_) on_deliver_(msg); break;
    case InteractionPolicy::Route::kDrop:           break;
  }
}

Status InteractionEngine::SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload) {
  Message m; m.payload = std::move(payload);
  policy_->SetTag(m, tag);
  policy_->EchoCorrelation(m, request);
  Endpoint to = policy_->ReplyTo(request);
  return SendMessage(m, to);
}

uint32_t InteractionEngine::StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms, const Endpoint& to) {
  if (interval_ms == 0) return 0;
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = periodic_next_++;
    periodics_[handle] = Periodic{out, tag, to, interval_ms, 0};
  }
  (void)Fire(out, tag, to);  // 立即一帧
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
  return handle;
}

void InteractionEngine::FirePeriodic(uint32_t handle) {
  Message out; FrameTag tag = 0; Endpoint to; uint32_t interval = 0; bool alive = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { out = it->second.out; tag = it->second.tag; to = it->second.to; interval = it->second.interval_ms; alive = true; }
  }
  if (!alive || !open_.load()) return;
  (void)Fire(out, tag, to);
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
}

void InteractionEngine::StopPeriodic(uint32_t handle) {
  IExecutor::TimerId t = 0;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { t = it->second.timer; periodics_.erase(it); }
  }
  if (t) executor_->Cancel(t);
}

void InteractionEngine::HandleDisconnect(const std::string& reason) {
  std::map<Key, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail(reason));
  }
}

}  // namespace transport
