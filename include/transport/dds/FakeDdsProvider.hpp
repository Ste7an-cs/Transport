#pragma once

// FakeDdsProvider.hpp — 进程内内存 topic 总线实现 IDdsProvider(零 FastDDS 依赖)。
// 多个 provider 共享一条 Bus 模拟多 participant 互通;Publish 同步分发。
// 两种获得方式:① 注入共享 Bus(测试 DI,完全隔离);② 默认构造→Init 接入按 domain 的静态 Bus。

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class FakeDdsProvider : public IDdsProvider {
 public:
  using Sink = std::function<void(const std::vector<uint8_t>&)>;

  struct Bus {
    std::mutex m;
    uint64_t next_id = 1;
    std::map<std::string, std::map<uint64_t, Sink>> subs;  // topic → (id → sink)

    uint64_t Add(const std::string& topic, Sink sink) {
      std::lock_guard<std::mutex> lk(m);
      uint64_t id = next_id++;
      subs[topic][id] = std::move(sink);
      return id;
    }
    void Remove(const std::string& topic, uint64_t id) {
      std::lock_guard<std::mutex> lk(m);
      auto it = subs.find(topic);
      if (it != subs.end()) it->second.erase(id);
    }
    void Dispatch(const std::string& topic, const std::vector<uint8_t>& bytes) {
      std::vector<Sink> snapshot;
      {
        std::lock_guard<std::mutex> lk(m);
        auto it = subs.find(topic);
        if (it != subs.end())
          for (auto& kv : it->second) snapshot.push_back(kv.second);
      }
      for (auto& s : snapshot) s(bytes);  // 锁外同步分发
    }
  };

  FakeDdsProvider() = default;                                  // 默认:Init 接入静态 domain 总线
  explicit FakeDdsProvider(std::shared_ptr<Bus> bus) : bus_(std::move(bus)) {}  // DI

  Status Init(const DdsConfig& config) override;
  void         Shutdown() override;
  Status Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Status Subscribe(const std::string& topic, Sink cb) override;
  Status Unsubscribe(const std::string& topic) override;
  std::string Name() const override { return "fake"; }

 private:
  static std::shared_ptr<Bus> StaticBusForDomain(int domain);

  std::shared_ptr<Bus> bus_;
  std::mutex mine_m_;
  std::map<std::string, std::vector<uint64_t>> mine_;  // 本 provider 的订阅 id
};

}  // namespace transport
