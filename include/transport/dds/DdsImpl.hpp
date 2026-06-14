#pragma once

// -----------------------------------------------------------------------------
// DdsImpl.hpp — DDS 传输实现（IDdsTransport；provider 无关）
// 组合 TransportCore；provider 经构造注入（默认从 DdsProviderRegistry 创建）。
// pub-sub 路由 + req-resp 关联/超时 + codec 边界 + 模式约束全部在本层；
// 底层 DDS 调用全部经 IDdsProvider。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

class DdsImpl : public IDdsTransport,
                public std::enable_shared_from_this<DdsImpl> {
 public:
  explicit DdsImpl(DdsConfig config,
                   std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsImpl() override;

  // ITransport
  Status Open() override;
  void Close() override;
  bool IsOpen() const override;
  using ITransport::Send;
  Status Send(const std::vector<uint8_t>& data) override;  // → topics[0]
  Status Send(const std::vector<uint8_t>& data,
              const Endpoint& to) override;                // kTopic 寻址
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;

  // 接收侧：转发 core_（仅 kPubSub 交付订阅消息）
  using ITransport::SetCodec;
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

  // IDdsTransport — pub-sub
  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  // IDdsTransport — req-resp
  Status SendRequest(const std::vector<uint8_t>& data, const std::string& topic,
                     std::function<void(Result<Message>)> on_reply,
                     uint32_t timeout_ms) override;
  Status OnRequest(const std::string& topic, RequestHandler handler) override;

  DdsMode Mode() const override { return config_.mode; }
  std::string Provider() const override;

 private:
  struct Pending {
    std::function<void(Result<Message>)> on_reply;
    std::shared_ptr<asio::steady_timer> timer;
  };

  Status SendToTopic(const std::vector<uint8_t>& data, const std::string& topic);

  Status RequireOpen() const;
  Status RequireMode(DdsMode m) const;
  std::string NextRequestId();
  void HandleReply(const std::string& request_id,
                   const std::vector<uint8_t>& payload);

  DdsConfig config_;
  std::unique_ptr<IDdsProvider> provider_;
  TransportCore core_;

  // req-resp 状态（mutex_ 保护）
  std::mutex mutex_;
  std::map<std::string, Pending> pending_;
  std::set<std::string> reply_subscribed_;
  uint64_t request_seq_ = 0;
  std::string request_prefix_;  // 构造时随机，保证跨实例/进程 id 唯一

  // 超时 timer 用 io 线程（与其它实现的「每实例一 io 线程」模式一致）
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  std::thread io_thread_;

  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
};

}  // namespace transport
