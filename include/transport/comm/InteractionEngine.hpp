#pragma once

// InteractionEngine.hpp — 通用交互引擎。3 原语(Fire/RequestAwait/StartPeriodic)+ 挂起/超时/重发/
// 分发/periodic/并发纪律一份。协议差异经 InteractionPolicy。须以 shared_ptr 持有。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionPolicy.hpp"
#include "transport/ITraceSink.hpp"

namespace transport {

class InteractionEngine : public std::enable_shared_from_this<InteractionEngine> {
 public:
  InteractionEngine(std::shared_ptr<ITransport> transport,
                    std::unique_ptr<ICodec> codec,
                    std::unique_ptr<InteractionPolicy> policy,
                    std::unique_ptr<IExecutor> executor = nullptr,
                    std::size_t queue_capacity = 1024);
  ~InteractionEngine();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  void OnInboundRequest(std::function<void(const Message&)> cb) { on_request_ = std::move(cb); }
  void OnInboundDeliver(std::function<void(const Message&)> cb) { on_deliver_ = std::move(cb); }
  void OnError(std::function<void(const std::string&)> cb) { on_error_ = std::move(cb); }

  // 须在 Open() 前调用;Open 后埋点只读 trace_,设置期单线程,无竞争。
  void SetTrace(std::shared_ptr<ITraceSink> t) { trace_ = std::move(t); }

  Status   Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  Status   RequestAwait(Message out, RequestSpec spec, const Endpoint& to = Endpoint::Default());
  uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  void     StopPeriodic(uint32_t handle);

  Status   SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload);

 private:
  struct Pending {
    RequestSpec spec; Message out; Endpoint to;
    uint32_t retries = 0; IExecutor::TimerId timer = 0; bool advanced = false;
  };
  struct Periodic { Message out; FrameTag tag; Endpoint to; uint32_t interval_ms; IExecutor::TimerId timer = 0; };

  Status SendMessage(Message& m, const Endpoint& to);
  void Dispatch(Message msg);
  void OnTimeout(Key key);
  void HandleDisconnect(const std::string& reason);
  void FirePeriodic(uint32_t handle);
  void Trace(const TraceEvent& ev) const { if (trace_) trace_->OnTrace(ev); }

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<InteractionPolicy> policy_;
  std::unique_ptr<IExecutor> executor_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;
  std::map<Key, Pending> pending_;
  std::map<uint32_t, Periodic> periodics_;
  uint32_t periodic_next_ = 1;
  std::function<void(const Message&)> on_request_;
  std::function<void(const Message&)> on_deliver_;
  std::function<void(const std::string&)> on_error_;
  std::shared_ptr<ITraceSink> trace_;
};

}  // namespace transport
