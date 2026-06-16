#pragma once

// DdsTransport.hpp — DDS pub-sub 字节传输(实现 IDdsTransport : ITransport)。
// 持有一个 IDdsProvider;Send(bytes,Endpoint::Topic)→Publish;Subscribe(topic)→provider.Subscribe;
// 样本到达直接在 provider 的 listener 线程调 OnBytes(bytes, from=topic)。须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

class DdsTransport : public IDdsTransport,
                     public std::enable_shared_from_this<DdsTransport> {
 public:
  explicit DdsTransport(DdsConfig config,
                        std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  Status SendToTopic(const std::string& topic, const std::vector<uint8_t>& bytes);

  DdsConfig config_;
  std::unique_ptr<IDdsProvider> provider_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex subs_m_;
  std::set<std::string> subscribed_;

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
