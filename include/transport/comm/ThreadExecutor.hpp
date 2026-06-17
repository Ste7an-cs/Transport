#pragma once

// ThreadExecutor.hpp — v1 执行器:1 条 worker 线程 + 有界任务队列(满则阻塞=背压)
// + 最小堆定时器(worker cv.wait_until 兼顾取任务/触发超时)。

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "transport/comm/IExecutor.hpp"

namespace transport {

class ThreadExecutor : public IExecutor {
 public:
  explicit ThreadExecutor(std::size_t capacity = 1024);
  ~ThreadExecutor() override;

  void Start() override;
  void Stop()  override;
  void Post(Task task) override;
  TimerId ScheduleAt(std::chrono::steady_clock::time_point deadline, Task task) override;
  void Cancel(TimerId id) override;

 private:
  void Run();

  struct TimerEntry {
    std::chrono::steady_clock::time_point deadline;
    TimerId id;
  };
  struct LaterDeadline {  // 小顶堆:最早 deadline 在 top
    bool operator()(const TimerEntry& a, const TimerEntry& b) const {
      return a.deadline > b.deadline;
    }
  };

  std::size_t capacity_;
  std::mutex m_;
  std::condition_variable work_cv_;   // worker 等待:有任务/到点/stop
  std::condition_variable space_cv_;  // 投递方等待:队列非满
  std::deque<Task> tasks_;
  std::priority_queue<TimerEntry, std::vector<TimerEntry>, LaterDeadline> heap_;
  std::map<TimerId, Task> timers_;    // 活跃定时器(id→task);取消即从此删,堆项懒跳过
  TimerId next_id_ = 1;
  std::thread worker_;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace transport
