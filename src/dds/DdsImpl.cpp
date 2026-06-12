#include "transport/dds/DdsImpl.hpp"

#include <chrono>
#include <random>
#include <utility>
#include <variant>

#include "transport/dds/DdsProviderRegistry.hpp"

// DdsImpl.cpp — DDS 传输实现（见 DdsImpl.hpp）。
// 线程：provider 回调来自其内部线程（FastDDS listener / Fake 调用线程），进
// core_(ReceiveQueue 自带锁) 或 pending_(mutex_) 后即返回；req-resp 超时 timer
// 跑在自有 io 线程。pending_ 「取出再执行」：reply 与超时竞争同一条目，先取到者
// 兑现 on_reply，后到者发现条目不在即放弃 —— 保证恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

DdsImpl::DdsImpl(DdsConfig config, std::unique_ptr<IDdsProvider> provider)
    : config_(std::move(config)),
      provider_(std::move(provider)),
      guard_(ctx_.get_executor()) {
  std::random_device rd;
  request_prefix_ = std::to_string(rd()) + "-";
  io_thread_ = std::thread([this] { ctx_.run(); });
}

DdsImpl::~DdsImpl() { Close(); }

bool DdsImpl::IsOpen() const { return open_.load(); }

Status DdsImpl::Open() {
  if (config_.topics.empty())
    return Status::Fail("config: topics must not be empty");
  if (config_.qos.history_depth == 0)
    return Status::Fail("config: history_depth must be > 0");
  if (!provider_) {
    provider_ = DdsProviderRegistry::Create(config_.provider);
    if (!provider_)
      return Status::Fail("config: provider not registered: " +
                          config_.provider);
  }
  auto st = provider_->Init(config_);
  if (!st) return st;
  open_.store(true);
  return Ok();
}

Status DdsImpl::RequireOpen() const {
  if (!open_.load()) return Status::Fail("config: transport not open");
  return Ok();
}

Status DdsImpl::RequireMode(DdsMode m) const {
  if (config_.mode != m)
    return Status::Fail("config: method not available in this mode");
  return Ok();
}

// ---------- pub-sub ----------

Status DdsImpl::Send(const std::vector<uint8_t>& data) {
  return SendToTopic(data, config_.topics[0]);
}

Status DdsImpl::Send(const std::vector<uint8_t>& data, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return Send(data);
    case Endpoint::Kind::kTopic:
      return SendToTopic(data, to.topic);
    case Endpoint::Kind::kNet:
      return Status::Fail("config: dds expects topic endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

Status DdsImpl::Send(const Message& msg, const Endpoint& to) {
  if (msg.topic.empty()) return Send(msg.payload, to);
  return SendToTopic(msg.payload, msg.topic);
}

Status DdsImpl::SendToTopic(const std::vector<uint8_t>& data,
                            const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  auto enc = core_.EncodeForSend(data, topic);  // 按 topic 选 codec
  if (!enc) return Status::Fail(enc.error);
  return provider_->Publish(topic, enc.value);
}

Status DdsImpl::Subscribe(const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  // 注意：provider 长期持有订阅回调——捕获 weak_ptr，避免
  // DdsImpl → provider_ → callback → shared_ptr<DdsImpl> 引用环（泄漏）。
  std::weak_ptr<DdsImpl> wself = shared_from_this();
  return provider_->Subscribe(topic, [wself, topic](Result<Message> m) {
    auto self = wself.lock();
    if (!self) return;
    if (!m) {
      self->core_.DeliverError(m.error);
      return;
    }
    // provider 给的是未解码 payload；DeliverFrame 内按 codec Decode
    self->core_.DeliverFrame(std::move(m.value.payload), topic, topic);
  });
}

Status DdsImpl::Unsubscribe(const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  return provider_->Unsubscribe(topic);
}

// ---------- req-resp ----------

std::string DdsImpl::NextRequestId() {
  std::lock_guard<std::mutex> lk(mutex_);
  return request_prefix_ + std::to_string(++request_seq_);
}

void DdsImpl::HandleReply(const std::string& request_id,
                          const std::vector<uint8_t>& payload) {
  Pending entry;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = pending_.find(request_id);
    if (it == pending_.end()) return;  // 不认识的 id（他人请求/已超时）：忽略
    entry = std::move(it->second);
    pending_.erase(it);
  }
  entry.timer->cancel();
  auto dec = core_.DecodeForReceive(payload);
  if (!dec) {
    entry.on_reply(Result<Message>::Fail(dec.error));
    return;
  }
  Message msg;
  msg.payload = std::move(dec.value);
  msg.timestamp = TransportCore::NowMicros();
  entry.on_reply(Result<Message>::Success(std::move(msg)));
}

Status DdsImpl::SendRequest(const std::vector<uint8_t>& data,
                            const std::string& topic,
                            std::function<void(Result<Message>)> on_reply,
                            uint32_t timeout_ms) {
  if (auto st = RequireMode(DdsMode::kReqResp); !st) return st;
  if (auto st = RequireOpen(); !st) return st;

  const std::string request_topic = topic + "_Request";
  const std::string reply_topic = topic + "_Reply";
  auto self = shared_from_this();
  std::weak_ptr<DdsImpl> wself = self;  // 长期回调用 weak，避免引用环

  // 幂等订阅 reply topic
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (reply_subscribed_.insert(reply_topic).second) {
      auto st = provider_->SubscribeReplies(
          reply_topic, [wself](const std::string& id,
                               const std::vector<uint8_t>& payload) {
            if (auto self = wself.lock()) self->HandleReply(id, payload);
          });
      if (!st) {
        reply_subscribed_.erase(reply_topic);
        return st;
      }
    }
  }

  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);

  const std::string id = NextRequestId();
  auto timer = std::make_shared<asio::steady_timer>(ctx_);
  timer->expires_after(std::chrono::milliseconds(timeout_ms));
  {
    std::lock_guard<std::mutex> lk(mutex_);
    pending_[id] = Pending{std::move(on_reply), timer};
  }
  // 注意：捕获 weak_ptr（而非 self）。strong 捕获会让本 handler 成为最后一个
  // DdsImpl 引用——其析构在 io 线程触发 ~DdsImpl→Close→io_thread_.join() 自我
  // join（EDEADLK，"Resource deadlock avoided" 终止进程）。weak 锁定后无此风险。
  timer->async_wait([wself, id](asio::error_code ec) {
    if (ec) return;  // 被取消（reply 已到）
    auto self = wself.lock();
    if (!self) return;
    Pending entry;
    {
      std::lock_guard<std::mutex> lk(self->mutex_);
      auto it = self->pending_.find(id);
      if (it == self->pending_.end()) return;  // reply 赢了
      entry = std::move(it->second);
      self->pending_.erase(it);
    }
    entry.on_reply(Result<Message>::Fail("timeout: request timed out"));
  });

  // 注意：登记 pending 之后再发布——Fake 总线同步分发，reply 可能在本调用内到达
  return provider_->SendRequest(request_topic, id, reply_topic, enc.value);
}

Status DdsImpl::OnRequest(const std::string& topic, RequestHandler handler) {
  if (auto st = RequireMode(DdsMode::kReqResp); !st) return st;
  if (auto st = RequireOpen(); !st) return st;

  // 长期持有的 sink 与可能被用户长期保存的 ReplyFn 均捕获 weak_ptr，避免引用环。
  std::weak_ptr<DdsImpl> wself = shared_from_this();
  return provider_->ServeRequests(
      topic + "_Request",
      [wself, handler](const std::vector<uint8_t>& payload,
                       const std::string& request_id,
                       const std::string& reply_topic) {
        auto self = wself.lock();
        if (!self) return;
        auto dec = self->core_.DecodeForReceive(payload);
        if (!dec) return;  // 解码失败：丢弃该请求（无法回传框架级错误）
        Message req;
        req.payload = std::move(dec.value);
        req.timestamp = TransportCore::NowMicros();
        ReplyFn reply = [wself, request_id,
                         reply_topic](const std::vector<uint8_t>& bytes) {
          auto self = wself.lock();
          if (!self || !self->open_.load())
            return Status::Fail("conn: transport closed");
          auto enc = self->core_.EncodeForSend(bytes);
          if (!enc) return Status::Fail(enc.error);
          return self->provider_->Reply(reply_topic, request_id, enc.value);
        };
        handler(req, std::move(reply));
      });
}

std::string DdsImpl::Provider() const {
  return provider_ ? provider_->ProviderName() : config_.provider;
}

// ---------- 生命周期 ----------

void DdsImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  // 取消全部未决请求（conn: 兑现），锁外调用 on_reply
  std::map<std::string, Pending> pend;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    pend.swap(pending_);
  }
  for (auto& kv : pend) {
    kv.second.timer->cancel();
    kv.second.on_reply(Result<Message>::Fail("conn: transport closed"));
  }
  if (provider_) provider_->Shutdown();
  core_.Close();
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
