#include "transport/dds/FakeDdsProvider.hpp"

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

Status FakeDdsProvider::Init(const DdsConfig& config) {
  if (!bus_) bus_ = StaticBusForDomain(config.domain_id);  // 未注入 → 接入静态 domain 总线
  return Status{};
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

Status FakeDdsProvider::Publish(const std::string& topic,
                                      const std::vector<uint8_t>& bytes) {
  // 未 Init(无总线)即发布:调用序错误 → kInvalidState。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  bus_->Dispatch(topic, bytes);
  return Status{};
}

Status FakeDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  // 未 Init(无总线)即订阅:调用序错误 → kInvalidState。
  if (!bus_) return make_error_code(TransportErrc::kInvalidState);
  uint64_t id = bus_->Add(topic, std::move(cb));
  std::lock_guard<std::mutex> lk(mine_m_);
  mine_[topic].push_back(id);
  return Status{};
}

Status FakeDdsProvider::Unsubscribe(const std::string& topic) {
  std::vector<uint64_t> ids;
  {
    std::lock_guard<std::mutex> lk(mine_m_);
    auto it = mine_.find(topic);
    if (it != mine_.end()) { ids = it->second; mine_.erase(it); }
  }
  if (bus_)
    for (auto id : ids) bus_->Remove(topic, id);
  return Status{};
}

}  // namespace transport
