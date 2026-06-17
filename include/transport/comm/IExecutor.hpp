#pragma once

// IExecutor.hpp — 执行器缝:决定"业务回调在哪/怎么跑"+ 定时。
// CommNode 只依赖此接口;v1=ThreadExecutor(线程),将来=CoroExecutor(协程),测试=InlineExecutor。

#include <chrono>
#include <cstdint>
#include <functional>

namespace transport {

class IExecutor {
 public:
  using Task    = std::function<void()>;
  using TimerId = uint64_t;  // 0 = 无效

  virtual ~IExecutor() = default;
  virtual void Start() = 0;
  virtual void Stop()  = 0;  // 停止并 drain/join,确保无任务在 CommNode 析构后跑

  // 投递任务到业务上下文【串行】执行;容量满时【阻塞调用方】(背压)。
  virtual void Post(Task task) = 0;

  // 在 deadline 触发一次性 task(请求超时);task 也在业务上下文跑。
  virtual TimerId ScheduleAt(std::chrono::steady_clock::time_point deadline, Task task) = 0;
  virtual void    Cancel(TimerId id) = 0;  // 取消未触发定时器(幂等)
};

}  // namespace transport
