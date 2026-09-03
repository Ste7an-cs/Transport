#include "transport/io/dds/FakeDdsProvider.hpp"

#include <map>
#include <utility>

namespace transport {

std::shared_ptr<FakeDdsProvider::Bus> FakeDdsProvider::StaticBusForDomain(int domain) {
  static std::mutex m;
  static std::map<int, std::shared_ptr<Bus>> buses;
  std::lock_guard<std::mutex> lk(m);
  auto& b = buses[domain];
  if (!b) b = std::make_shared<Bus>();
  return b;
}

Coro::Result<void> FakeDdsProvider::Init(const DdsConfig& config) {
  if (!bus_) bus_ = StaticBusForDomain(config.domain_id);  // 未注入 → 接入静态 domain 总线
  return Coro::Result<void>{};
}

void FakeDdsProvider::Shutdown() {
  std::map<std::string, std::vector<uint64_t>> mine;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    mine.swap(mine_);
    declared_writers_.clear();  // 端点集合在这一刻整体拆除(D16)。
  }
  if (bus_)
    for (auto& kv : mine)
      for (auto id : kv.second) bus_->Remove(kv.first, id);
}

// 写侧端点声明(ADR-0013 D13)。Fake 的总线上没有"端点"这种东西,故本方法只登记
// 意图——但**判据与真实 provider 完全一致**:登记过的 topic 才发得出去,这样用例里的
// 调用序错误在 Fake 上也会当场暴露,而不是等换到 Fast DDS 才发作。
Coro::Result<void> FakeDdsProvider::DeclareWriter(const std::string& topic) {
  // 未 Init(无总线)即声明:调用序错误 → kInvalidState(与 Subscribe 一致)。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  std::lock_guard<std::mutex> lk(mine_m_);
  declared_writers_.insert(topic);  // 幂等:set 天然去重。
  return Coro::Result<void>{};
}

Coro::Result<void> FakeDdsProvider::Publish(const std::string& topic,
                                      const std::vector<uint8_t>& bytes) {
  // 未 Init(无总线)即发布:调用序错误 → kInvalidState。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    // 未声明即发布 → kConfiguration(**不惰性建**,同 FastDdsProvider)。
    if (declared_writers_.count(topic) == 0)
      return make_error_code(TransportErrc::kConfiguration);
  }
  bus_->Dispatch(topic, bytes);  // 锁外分发:回调可能反过来调本对象。
  return Coro::Result<void>{};
}

Coro::Result<void> FakeDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  // 未 Init(无总线)即订阅:调用序错误 → kInvalidState。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  uint64_t id = bus_->Add(topic, std::move(cb));
  std::lock_guard<std::mutex> lk(mine_m_);
  mine_[topic].push_back(id);
  return Coro::Result<void>{};
}

Coro::Result<void> FakeDdsProvider::Unsubscribe(const std::string& topic) {
  std::vector<uint64_t> ids;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    auto it = mine_.find(topic);
    if (it != mine_.end()) { ids = it->second; mine_.erase(it); }
  }
  if (bus_)
    for (auto id : ids) bus_->Remove(topic, id);
  return Coro::Result<void>{};
}

DdsMatchedCount FakeDdsProvider::MatchedCount() const {
  if (!bus_) return DdsMatchedCount{};  // 未 Init:没接上总线就谈不上匹配
  size_t mine = 0;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    for (const auto& kv : mine_) mine += kv.second.size();
  }
  const size_t total = bus_->TotalSinks();
  // 总线上的 sink 减去自己的即为对端。两次取数之间总线可能变动,故夹到非负。
  const auto peers = static_cast<int32_t>(total > mine ? total - mine : 0);
  return DdsMatchedCount{peers, peers};  // Fake 无独立判活:匹配即视为存活
}

}  // namespace transport
