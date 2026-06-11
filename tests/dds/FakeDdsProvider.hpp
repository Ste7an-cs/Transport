#pragma once

// FakeDdsProvider — 进程内内存 topic 总线实现 IDdsProvider（测试件，零 FastDDS
// 依赖）。多个 provider 共享一条 Bus 模拟多 participant 互通；Publish 同步分发。

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/RawMessage.hpp"

namespace transport {

class FakeDdsProvider : public IDdsProvider {
 public:
  struct Bus {
    using RawSink = std::function<void(const RawMessage&)>;
    std::mutex m;
    uint64_t next_id = 1;
    std::map<std::string, std::map<uint64_t, RawSink>> subs;

    uint64_t Add(const std::string& topic, RawSink sink) {
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
    void Dispatch(const std::string& topic, const RawMessage& msg) {
      std::vector<RawSink> snapshot;
      {
        std::lock_guard<std::mutex> lk(m);
        auto it = subs.find(topic);
        if (it != subs.end())
          for (auto& kv : it->second) snapshot.push_back(kv.second);
      }
      for (auto& s : snapshot) s(msg);  // 锁外同步分发
    }
  };

  explicit FakeDdsProvider(std::shared_ptr<Bus> bus) : bus_(std::move(bus)) {}

  Status Init(const DdsConfig&) override { return Ok(); }

  Status Publish(const std::string& topic,
                 const std::vector<uint8_t>& data) override {
    RawMessage m; m.payload = data;
    bus_->Dispatch(topic, m);
    return Ok();
  }

  Status Subscribe(const std::string& topic,
                   ITransport::ReceiveCallback cb) override {
    uint64_t id = bus_->Add(topic, [cb, topic](const RawMessage& m) {
      Message msg;
      msg.payload = m.payload;
      msg.topic = topic;
      msg.source = topic;
      cb(Result<Message>::Success(std::move(msg)));
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[topic].push_back(id);
    return Ok();
  }

  Status Unsubscribe(const std::string& topic) override {
    std::vector<uint64_t> ids;
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      auto it = mine_.find(topic);
      if (it != mine_.end()) { ids = it->second; mine_.erase(it); }
    }
    for (auto id : ids) bus_->Remove(topic, id);
    return Ok();
  }

  Status SendRequest(const std::string& request_topic,
                     const std::string& request_id,
                     const std::string& reply_topic,
                     const std::vector<uint8_t>& data) override {
    RawMessage m; m.request_id = request_id; m.reply_topic = reply_topic;
    m.payload = data;
    bus_->Dispatch(request_topic, m);
    return Ok();
  }

  Status SubscribeReplies(const std::string& reply_topic,
                          ReplySink sink) override {
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      if (!reply_subscribed_.insert(reply_topic).second) return Ok();  // 幂等
    }
    uint64_t id = bus_->Add(reply_topic, [sink](const RawMessage& m) {
      sink(m.request_id, m.payload);
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[reply_topic].push_back(id);
    return Ok();
  }

  Status ServeRequests(const std::string& request_topic,
                       RequestSink sink) override {
    uint64_t id = bus_->Add(request_topic, [sink](const RawMessage& m) {
      sink(m.payload, m.request_id, m.reply_topic);
    });
    std::lock_guard<std::mutex> lk(mine_m_);
    mine_[request_topic].push_back(id);
    return Ok();
  }

  Status Reply(const std::string& reply_topic, const std::string& request_id,
               const std::vector<uint8_t>& data) override {
    RawMessage m; m.request_id = request_id; m.payload = data;
    bus_->Dispatch(reply_topic, m);
    return Ok();
  }

  void Shutdown() override {
    std::map<std::string, std::vector<uint64_t>> mine;
    {
      std::lock_guard<std::mutex> lk(mine_m_);
      mine.swap(mine_);
      reply_subscribed_.clear();
    }
    for (auto& kv : mine)
      for (auto id : kv.second) bus_->Remove(kv.first, id);
  }

  std::string ProviderName() const override { return "Fake"; }

 private:
  static Status Ok() { return Status::Success(std::monostate{}); }

  std::shared_ptr<Bus> bus_;
  std::mutex mine_m_;
  std::map<std::string, std::vector<uint64_t>> mine_;  // 本 provider 的订阅 id
  std::set<std::string> reply_subscribed_;
};

}  // namespace transport
