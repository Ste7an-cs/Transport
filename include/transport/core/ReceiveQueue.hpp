#pragma once

// -----------------------------------------------------------------------------
// ReceiveQueue.hpp — FIFO 消息队列 + 三模式接收交付
// 同步(Receive)/回调(OnReceive)/future(AsyncReceive) 三种交付互斥，由首个消费
// 侧调用锁定。线程安全：I/O 线程 Push，应用线程消费。被 TransportBase 组合拥有。
// -----------------------------------------------------------------------------

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

// FIFO 消息队列 + 三种交付模式（同步/回调/future）。
// 模式互斥：由首个消费侧调用锁定，之后调用其它模式返回 config: 错误。
// 线程安全：生产侧 Push 由 I/O 线程调用，消费侧由应用线程调用。
class ReceiveQueue {
 public:
  using Callback = std::function<void(Result<Message>)>;
  enum class Mode { kNone, kSync, kCallback, kFuture };

  ReceiveQueue() = default;
  ~ReceiveQueue();

  ReceiveQueue(const ReceiveQueue&) = delete;
  ReceiveQueue& operator=(const ReceiveQueue&) = delete;

  // 生产侧：投递一条消息（成功或失败）
  void Push(Result<Message> msg);

  // 消费侧（三选一，互斥）
  Result<Message> Receive(uint32_t timeout_ms);  // 锁定 kSync
  Status SetCallback(Callback cb);               // 锁定 kCallback
  std::future<Result<Message>> AsyncReceive();   // 锁定 kFuture

  // 关闭：唤醒同步等待者、兑现未决 future，均以 conn: 错误结束
  void Close();

  Mode CurrentMode();

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  Mode mode_ = Mode::kNone;
  bool closed_ = false;

  std::deque<Result<Message>> messages_;               // kSync / kFuture 暂存
  Callback callback_;                                  // kCallback
  std::deque<std::promise<Result<Message>>> pending_;  // kFuture 未决
};

}  // namespace transport
