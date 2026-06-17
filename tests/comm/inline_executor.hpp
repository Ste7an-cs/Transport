#pragma once
#include <chrono>
#include <map>
#include "transport/comm/IExecutor.hpp"

namespace testutil {
class InlineExecutor : public transport::IExecutor {
 public:
  void Start() override {}
  void Stop() override {}
  void Post(Task task) override { task(); }  // 同步,调用线程立即执行
  TimerId ScheduleAt(std::chrono::steady_clock::time_point dl, Task task) override {
    const TimerId id = ++next_;
    timers_[id] = {dl, std::move(task)};
    return id;
  }
  void Cancel(TimerId id) override { timers_.erase(id); }

  // 测试驱动:触发所有(或到点)定时器
  void FireAll() {
    auto t = std::move(timers_);
    timers_.clear();
    for (auto& kv : t) kv.second.second();
  }

 private:
  TimerId next_ = 0;
  std::map<TimerId, std::pair<std::chrono::steady_clock::time_point, Task>> timers_;
};
}  // namespace testutil
