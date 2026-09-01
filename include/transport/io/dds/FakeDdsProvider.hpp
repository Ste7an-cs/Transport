#pragma once

// FakeDdsProvider.hpp — 进程内内存 topic 总线实现 IDdsProvider(零 FastDDS 依赖)。
// 多个 provider 共享一条 Bus 模拟多 participant 互通;Publish 同步分发。
// 两种获得方式:① 注入共享 Bus(测试 DI,完全隔离);② 默认构造→Init 接入按 domain 的静态 Bus。

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "transport/io/dds/IDdsProvider.hpp"

namespace transport {

class FakeDdsProvider : public IDdsProvider {
 public:
  using Sink = std::function<void(const std::vector<uint8_t>&)>;

  struct Bus {
    mutable std::mutex m;
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
    /// 总线上现存的 sink 总数(跨全部 topic),供 `MatchedCount()` 扣掉自己后当对端数。
    size_t TotalSinks() const {
      std::lock_guard<std::mutex> lk(m);
      size_t n = 0;
      for (const auto& kv : subs) n += kv.second.size();
      return n;
    }
  };

  FakeDdsProvider() = default;                                  // 默认:Init 接入静态 domain 总线
  explicit FakeDdsProvider(std::shared_ptr<Bus> bus) : bus_(std::move(bus)) {}  // DI

  Coro::Result<void> Init(const DdsConfig& config) override;
  void         Shutdown() override;

  /// @brief 记下该 topic 的写侧端点(幂等)。真实 provider 在这一步建 `DataWriter`;
  ///        Fake 无端点可建,只登记——`Publish` 据此判"未声明"。未 `Init` 返 `kInvalidState`。
  Coro::Result<void> DeclareWriter(const std::string& topic) override;

  /// @brief 向 topic 同步分发。**该 topic 须已 `DeclareWriter`**,否则返 `kConfiguration`
  ///        (**D13** 补正:不惰性建,两个实现一致)。
  Coro::Result<void> Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Coro::Result<void> Subscribe(const std::string& topic, Sink cb) override;
  Coro::Result<void> Unsubscribe(const std::string& topic) override;

  /// @brief 匹配数 = 总线上**别人**的 sink 数;`alive` 与之相等。
  ///
  /// Fake 无发现、无判活机制,只能拿"同一条总线上还有没有别的订阅者"当对端数——
  /// 未 `Init`(无总线)或总线上只有自己时返 `{0, 0}`。**局限**:纯发布方(从不
  /// `Subscribe`)在总线上不留痕,故对端只订阅、我方只发布时,**对方**看不到我方。
  [[nodiscard]] DdsMatchedCount MatchedCount() const override;

  std::string Name() const override { return "fake"; }

 private:
  static std::shared_ptr<Bus> StaticBusForDomain(int domain);

  std::shared_ptr<Bus> bus_;
  mutable std::mutex mine_m_;
  std::map<std::string, std::vector<uint64_t>> mine_;  // 本 provider 的订阅 id
  /// 已声明的写侧 topic(与 `mine_` 同锁)。`Shutdown()` 整体清掉——端点集合只在那一刻
  /// 拆除,没有单独撤销一个 writer 的时机(**D16**,故不设 `UndeclareWriter`)。
  std::set<std::string> declared_writers_;
};

}  // namespace transport
