#include "transport/dds/DdsTransport.hpp"

#include <mutex>
#include <utility>
#include <variant>

#include "transport/dds/DdsProviderRegistry.hpp"
#include "transport/dds/FakeDdsProvider.hpp"
#ifdef TRANSPORT_HAS_FASTDDS
#include "dds/FastDdsProvider.hpp"
#endif

// DdsTransport.cpp — 见 .hpp。订阅回调捕获 weak_ptr 防引用环
// (provider→callback→weak(transport);transport→unique_ptr(provider))。
// OnBytes 直接在 provider listener 线程上调(同 topic 有序、跨 topic 并发)。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }

// 幂等注册内建 provider:"fake" 总注册;探测到 FastDDS 时注册 "fastdds"。
void RegisterBuiltinProviders() {
  static std::once_flag once;
  std::call_once(once, [] {
    DdsProviderRegistry::RegisterProvider(
        "fake", [] { return std::make_unique<FakeDdsProvider>(); });
#ifdef TRANSPORT_HAS_FASTDDS
    DdsProviderRegistry::RegisterProvider(
        "fastdds", [] { return std::make_unique<FastDdsProvider>(); });
#endif
  });
}
}  // namespace

DdsTransport::DdsTransport(DdsConfig config, std::unique_ptr<IDdsProvider> provider)
    : config_(std::move(config)), provider_(std::move(provider)) {}

DdsTransport::~DdsTransport() { Close(); }

Status DdsTransport::Open() {
  if (open_.load()) return Ok();
  if (!provider_) {
    RegisterBuiltinProviders();
    provider_ = DdsProviderRegistry::Create(config_.provider);
    if (!provider_)
      return Status::Fail("config: provider not registered: " + config_.provider);
  }
  auto st = provider_->Init(config_);
  if (!st) return st;
  open_.store(true);
  if (connect_cb_) connect_cb_();
  return Ok();
}

void DdsTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  if (provider_) provider_->Shutdown();
}

Status DdsTransport::SendToTopic(const std::string& topic,
                                 const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  return provider_->Publish(topic, bytes);
}

Status DdsTransport::Send(const std::vector<uint8_t>& bytes) {
  if (config_.default_topic.empty())
    return Status::Fail("config: no default topic");
  return SendToTopic(config_.default_topic, bytes);
}

Status DdsTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault: return Send(bytes);
    case Endpoint::Kind::kTopic:   return SendToTopic(to.topic, bytes);
    case Endpoint::Kind::kNet:     return Status::Fail("config: dds expects topic endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

Status DdsTransport::Subscribe(const std::string& topic) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  std::weak_ptr<DdsTransport> wself = weak_from_this();
  auto st = provider_->Subscribe(
      topic, [wself, topic](const std::vector<uint8_t>& bytes) {
        auto s = wself.lock();
        if (!s) return;
        if (s->bytes_cb_)
          s->bytes_cb_(Result<std::vector<uint8_t>>::Success(bytes), topic);
      });
  if (st) {
    std::lock_guard<std::mutex> lk(subs_m_);
    subscribed_.insert(topic);
  }
  return st;
}

Status DdsTransport::Unsubscribe(const std::string& topic) {
  if (!open_.load()) return Status::Fail("config: dds not open");
  auto st = provider_->Unsubscribe(topic);
  {
    std::lock_guard<std::mutex> lk(subs_m_);
    subscribed_.erase(topic);
  }
  return st;
}

}  // namespace transport
