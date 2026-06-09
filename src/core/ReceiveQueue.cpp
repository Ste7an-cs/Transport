#include "transport/core/ReceiveQueue.hpp"

#include <chrono>
#include <utility>
#include <variant>

namespace transport {

ReceiveQueue::~ReceiveQueue() { Close(); }

void ReceiveQueue::Push(Result<Message> msg) {
  Callback cb_copy;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) return;

    if (mode_ == Mode::kFuture && !pending_.empty()) {
      auto promise = std::move(pending_.front());
      pending_.pop_front();
      lock.unlock();
      promise.set_value(std::move(msg));
      return;
    }
    if (mode_ == Mode::kCallback) {
      cb_copy = callback_;  // 锁外调用，避免回调内重入死锁
    } else {
      messages_.push_back(std::move(msg));
      cv_.notify_one();
      return;
    }
  }
  if (cb_copy) cb_copy(std::move(msg));
}

Result<Message> ReceiveQueue::Receive(uint32_t timeout_ms) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (mode_ != Mode::kNone && mode_ != Mode::kSync) {
    return Result<Message>::Fail(
        "config: receive mode already set to a different mode");
  }
  mode_ = Mode::kSync;

  auto ready = [this] { return !messages_.empty() || closed_; };
  if (timeout_ms == 0) {
    cv_.wait(lock, ready);
  } else if (!cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), ready)) {
    return Result<Message>::Fail("timeout: receive timed out");
  }

  if (messages_.empty()) {  // 被 Close 唤醒
    return Result<Message>::Fail("conn: receive queue closed");
  }
  Result<Message> msg = std::move(messages_.front());
  messages_.pop_front();
  return msg;
}

Status ReceiveQueue::SetCallback(Callback cb) {
  std::deque<Result<Message>> backlog;
  Callback local;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return Status::Fail("conn: receive queue closed");
    if (mode_ != Mode::kNone && mode_ != Mode::kCallback) {
      return Status::Fail("config: receive mode already set to a different mode");
    }
    mode_ = Mode::kCallback;
    callback_ = std::move(cb);
    local = callback_;
    backlog.swap(messages_);  // 设回调前积压的消息
  }
  for (auto& m : backlog) {
    if (local) local(std::move(m));
  }
  return Status::Success(std::monostate{});
}

std::future<Result<Message>> ReceiveQueue::AsyncReceive() {
  std::promise<Result<Message>> promise;
  auto fut = promise.get_future();

  std::lock_guard<std::mutex> lock(mutex_);
  if (mode_ != Mode::kNone && mode_ != Mode::kFuture) {
    promise.set_value(Result<Message>::Fail(
        "config: receive mode already set to a different mode"));
    return fut;
  }
  mode_ = Mode::kFuture;

  if (closed_) {
    promise.set_value(Result<Message>::Fail("conn: receive queue closed"));
  } else if (!messages_.empty()) {
    promise.set_value(std::move(messages_.front()));
    messages_.pop_front();
  } else {
    pending_.push_back(std::move(promise));
  }
  return fut;
}

void ReceiveQueue::Close() {
  std::deque<std::promise<Result<Message>>> pend;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return;
    closed_ = true;
    pend.swap(pending_);
    cv_.notify_all();
  }
  for (auto& p : pend) {
    p.set_value(Result<Message>::Fail("conn: receive queue closed"));
  }
}

ReceiveQueue::Mode ReceiveQueue::CurrentMode() {
  std::lock_guard<std::mutex> lock(mutex_);
  return mode_;
}

}  // namespace transport
