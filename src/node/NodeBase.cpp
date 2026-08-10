#include "transport/node/NodeBase.hpp"

#include <chrono>
#include <utility>

#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/TraceCategories.hpp"

// NodeBase.cpp — 见 .hpp。生命周期唯一的一份实现(ADR-0006 D1/D6):幂等 Start、关闭仲裁、
// 收敛(join → Drain 归因 → 置 Closed → 广播)。协议特有实事一律经虚钩子交给子类,本文件
// 不 include 任何协议 / 消息类型。

namespace transport {

NodeBase::NodeBase(ITraceSink* trace_sink) : trace_sink_(trace_sink) {}

NodeBase::~NodeBase() = default;

Status NodeBase::Start() {
  bool do_init = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    switch (lifecycle_) {
      case LifecycleState::kRunning:
        return Status{};  // 已 Running 再启 → 幂等成功。
      case LifecycleState::kClosing:
      case LifecycleState::kClosed:
        return make_error_code(TransportErrc::kInvalidState);
      case LifecycleState::kCreated:
        if (starting_) {
          break;  // 已有 Start 在初始化 → 出临界区 await 同一 start_done_,不重复 spawn。
        }
        // 首个 Start:先校验(失败停 Created、start_done_ 不 latch、可重试)。
        if (auto valid = ValidateConfig(); !valid) {
          return valid;
        }
        starting_ = true;
        do_init = true;
        break;
    }
  }
  if (!do_init) {
    return start_done_.Wait();  // 并发 Start:共享首个 Start 的结果,不重复创建资源。
  }

  Status started = DoStart();  // 子实事:失败时未 MarkRunning、仍在 Created。
  if (!started) {
    std::lock_guard<std::mutex> lock(mutex_);
    starting_ = false;  // 退回 Created 允许重试;并发 await 者共享此失败结果。
  }
  start_done_.Complete(started);
  return started;
}

void NodeBase::MarkRunning() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycle_ = LifecycleState::kRunning;
    starting_ = false;
  }
  // 生命周期跃迁 Trace(P5-4:Created→Running;类别原名 "close",#98 改 lifecycle)。
  RecordEvent(kTraceCategoryLifecycle, trace_sink_, "running");
}

Status NodeBase::SignalClose() {
  bool read_loop_converges = false;
  if (SignalCloseIfFirstCloser(&read_loop_converges) && !read_loop_converges) {
    // 从未 spawn 读循环:无收敛者,就地收敛。残留业务(理论上无)一并 close_drop 归因。
    // 本段不等待任何 fiber(只 Drain + 置 Closed + Complete),故仍是"不等待"入口。
    ConvergeToClosed();
  }
  return Status{};
}

Status NodeBase::Close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ == LifecycleState::kClosed) {
      return Status{};  // 已 Closed 再关直接成功(RT_LIFECYCLE_004)。
    }
  }
  (void)SignalClose();  // 幂等仲裁在内:本调用未必是首个关闭者。
  return closed_.Wait();  // 后续关闭者与外部调用者共享同一收敛结果(多等待者)。
}

Status NodeBase::WaitClosed(OperationOptions options) {
  return closed_.Wait(std::move(options));
}

bool NodeBase::IsRunning() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lifecycle_ == LifecycleState::kRunning;
}

std::size_t NodeBase::CloseDropCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return close_drop_count_;
}

NodeBase::Clock::duration NodeBase::LastCloseLatency() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return last_close_latency_;
}

bool NodeBase::SignalCloseIfFirstCloser(bool* read_loop_converges) {
  bool first_closer = false;
  bool converger_ready = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ != LifecycleState::kClosing &&
        lifecycle_ != LifecycleState::kClosed) {
      first_closer = true;
      // Running ⇒ DoStart 已 spawn 读循环 ⇒ 收敛者就位;否则(Created/starting)无人收敛。
      converger_ready = (lifecycle_ == LifecycleState::kRunning);
      lifecycle_ = LifecycleState::kClosing;
      close_requested_at_ = Clock::now();  // P5-4:关闭时延起点。
    }
  }
  if (read_loop_converges) {
    *read_loop_converges = converger_ready;
  }
  if (!first_closer) {
    return false;
  }
  // 生命周期跃迁 Trace(Running→Closing)。
  RecordEvent(kTraceCategoryLifecycle, trace_sink_, "closing");
  // 汇合信号(锁外,子实事):唤醒读循环 + 消费者 + 触发 handler 取消 + FailAll 在途请求。
  // 关闭一经置 Closing 即不可回滚,故基类不据其返回值分支(签名与 DoStart 对称)。
  (void)DoClose();
  // 最后一步:此时上述信号均已发出,读循环一旦被放行即可无条件走完收敛。**无条件**
  // Complete(即便此刻无收敛者):它是"首个关闭者已发完全部汇合信号"这一事实本身,
  // 无人等待时 Complete 亦无副作用;而漏发一次即等于读循环永久挂在 Wait 上。
  close_signalled_.Complete(Status{});
  return true;
}

void NodeBase::ConvergeAfterReadLoop() {
  (void)SignalCloseIfFirstCloser(nullptr);  // 仍 Running ⇒ 致命错误自终(D5)。
  close_signalled_.Wait();  // 信号已 Complete(我方 Close 或上一行自终),立即返回。
  // 结构化并发 join(ADR-0005 D2):`JoinHandler` 在本 fiber 内让出,直至 handler 消费者
  // 实际退出(RT_LIFECYCLE_006:不强制销毁 fiber,等其协作返回);未设 handler 时立即
  // 返回。返回即意味着 handler fiber 已不再运行,可安全置 Closed。
  JoinHandler();
  ConvergeToClosed();
}

void NodeBase::ConvergeToClosed() {
  Clock::duration latency{};
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // 归因留在基类(子类只报条数):close_drop 是关闭语义,与 close 时延、置 Closed 同一
    // 临界区(与拆件前的加锁范围逐字相同)。
    const std::size_t drained = DrainUnstartedBusiness();
    for (std::size_t i = 0; i < drained; ++i) {
      RecordDrop(DropReason::kCloseDrop, close_drop_count_, trace_sink_);
    }
    lifecycle_ = LifecycleState::kClosed;
    latency = Clock::now() - close_requested_at_;
    last_close_latency_ = latency;
  }
  RecordEvent(kTraceCategoryLifecycle, trace_sink_, "closed", {}, {}, {},
              static_cast<long>(std::chrono::duration_cast<
                                 std::chrono::microseconds>(latency)
                                     .count()));
  closed_.Complete(Status{});
}

}  // namespace transport
