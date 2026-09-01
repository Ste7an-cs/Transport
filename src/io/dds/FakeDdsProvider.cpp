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
  }
  if (bus_)
    for (auto& kv : mine)
      for (auto id : kv.second) bus_->Remove(kv.first, id);
}

Coro::Result<void> FakeDdsProvider::Publish(const std::string& topic,
                                      const std::vector<uint8_t>& bytes) {
  // 未 Init(无总线)即发布:调用序错误 → kInvalidState。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  bus_->Dispatch(topic, bytes);
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
