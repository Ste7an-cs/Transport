#pragma once

// coro::InteractionEngine — 协程原生交互引擎(通用机制,协议差异外包给 InteractionPolicy)。
// 请求-应答 = 线性 send();await_for(timeout);。活在 fiber 调度线程 = Qt 事件循环线程,协作式无锁。

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/InteractionPolicy.hpp"

#include "await/awaitable.hpp"  // Coro::Awaitable / Coro::FiberChannel

namespace transport {
namespace coro {

class InteractionEngine {
 public:
  InteractionEngine(std::shared_ptr<ITransport> transport,
                    std::unique_ptr<ICodec> codec,
                    std::unique_ptr<InteractionPolicy> policy);
  ~InteractionEngine();

  Status Open();   // 接 OnBytes/OnDisconnect;不负责 transport->Open()
  void   Close();  // 幂等:唤醒并终结所有挂起(在途 Request 返回 conn:)

  // Fire:单向发。SetTag → Encode → Send。不登记、不等待。
  Status Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  // Request:发 out(tag)并按 key 挂起,await_for(timeout)。仅挂起【当前 fiber】。
  //   返回:应答(终结帧)/ "timeout:" / "conn:"(Close 或断连)。
  Result<Message> Request(Message out, FrameTag tag, std::chrono::milliseconds timeout,
                          const Endpoint& to = Endpoint::Default());

  // 无主入站帧(RouteUnmatched==kDeliver)交此钩子;未设则丢弃。
  void OnInboundDeliver(std::function<void(const Message&)> cb) { on_deliver_ = std::move(cb); }
  // 可插拔入站关联键(默认 policy.KeyOf);自定义配对算法在此注入。
  void SetKeyFn(std::function<Key(const Message&)> fn) { key_fn_ = std::move(fn); }

 private:
  void onBytes(Result<std::vector<uint8_t>> r, const std::string& from);
  void onDisconnect(const std::string& reason);

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<InteractionPolicy> policy_;
  std::function<Key(const Message&)> key_fn_;
  std::function<void(const Message&)> on_deliver_;
  std::map<Key, std::shared_ptr<Coro::FiberChannel<Message>>> pending_;
  bool closing_ = false;
};

}  // namespace coro
}  // namespace transport
