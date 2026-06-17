#include "transport/dds/FakeDdsProvider.hpp"

#include <map>
#include <utility>
#include <variant>

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

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
  return Ok();
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
  if (!bus_) return Status::Fail("config: dds not initialized");
  bus_->Dispatch(topic, bytes);
  return Ok();
}

Status FakeDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  if (!bus_) return Status::Fail("config: dds not initialized");
  uint64_t id = bus_->Add(topic, std::move(cb));
  std::lock_guard<std::mutex> lk(mine_m_);
  mine_[topic].push_back(id);
  return Ok();
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
  return Ok();
}

}  // namespace transport
