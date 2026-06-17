#include "transport/comm/ThreadExecutor.hpp"

#include <utility>

// ThreadExecutor.cpp — 见 .hpp。任务/超时回调一律在锁外运行(单 worker → 串行)。

namespace transport {

ThreadExecutor::ThreadExecutor(std::size_t capacity) : capacity_(capacity) {}

ThreadExecutor::~ThreadExecutor() { Stop(); }

void ThreadExecutor::Start() {
  std::lock_guard<std::mutex> lk(m_);
  if (started_) return;
  started_ = true;
  worker_ = std::thread([this] { Run(); });
}

void ThreadExecutor::Stop() {
  {
    std::lock_guard<std::mutex> lk(m_);
    if (stopping_) return;
    stopping_ = true;
  }
  work_cv_.notify_all();
  space_cv_.notify_all();  // 解除可能阻塞的投递方
  if (worker_.joinable()) worker_.join();
}

void ThreadExecutor::Post(Task task) {
  std::unique_lock<std::mutex> lk(m_);
  space_cv_.wait(lk, [this] { return tasks_.size() < capacity_ || stopping_; });
  if (stopping_) return;  // 停止中:丢弃
  tasks_.push_back(std::move(task));
  work_cv_.notify_one();
}

IExecutor::TimerId ThreadExecutor::ScheduleAt(
    std::chrono::steady_clock::time_point deadline, Task task) {
  std::lock_guard<std::mutex> lk(m_);
  const TimerId id = next_id_++;
  timers_[id] = std::move(task);
  heap_.push(TimerEntry{deadline, id});
  work_cv_.notify_one();
  return id;
}

void ThreadExecutor::Cancel(TimerId id) {
  std::lock_guard<std::mutex> lk(m_);
  timers_.erase(id);  // 堆项在 pop 时按 timers_ 缺失而跳过
}

void ThreadExecutor::Run() {
  std::unique_lock<std::mutex> lk(m_);
  while (true) {
    const auto now = std::chrono::steady_clock::now();
    if (!heap_.empty() && heap_.top().deadline <= now) {
      const TimerEntry e = heap_.top();
      heap_.pop();
      auto it = timers_.find(e.id);
      if (it == timers_.end()) continue;  // 已取消
      Task t = std::move(it->second);
      timers_.erase(it);
      lk.unlock();
      t();
      lk.lock();
      continue;
    }
    if (!tasks_.empty()) {
      Task t = std::move(tasks_.front());
      tasks_.pop_front();
      space_cv_.notify_one();
      lk.unlock();
      t();
      lk.lock();
      continue;
    }
    if (stopping_) break;
    if (heap_.empty())
      work_cv_.wait(lk);
    else
      work_cv_.wait_until(lk, heap_.top().deadline);
  }
}

}  // namespace transport
