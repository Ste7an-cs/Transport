#include "transport/node/NodeBase.hpp"

#include "transport/core/Error.hpp"

// NodeBase.cpp — 见 .hpp。生命周期唯一的一份实现:幂等 Start、只发信号的 Close、join 式
// WaitClosed。协议特有实事一律经三个钩子交给子类,本文件不 include 任何协议 / 消息类型。
//
// 全文只有一条同步纪律:**持 mutex_ 期间不调钩子、不挂起**。三个钩子都在锁外调用,故基类
// 不需要向子类下达"实现不得取锁、不得挂起"这类反向约束。

namespace transport {

NodeBase::~NodeBase() = default;

Coro::Result<void> NodeBase::Start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    switch (lifecycle_) {
      case LifecycleState::kRunning:
        return Coro::Result<void>{};  // 已 Running 再启 → 幂等成功。
      case LifecycleState::kClosing:
      case LifecycleState::kClosed:
        return make_error_code(TransportErrc::kInvalidState);
      case LifecycleState::kCreated:
        if (starting_) {
          // 另一次 Start 正在锁外跑 DoStart:不共享其结果(见 .hpp,#150)。
          return make_error_code(TransportErrc::kInvalidState);
        }
        starting_ = true;
        break;
    }
  }

  const Coro::Result<void> started = DoStart();  // 锁外:配置校验 + transport.Start + spawn。

  std::lock_guard<std::mutex> lock(mutex_);
  starting_ = false;
  if (!started) {
    return started;  // 停在 Created:未 spawn 任何 fiber,允许改配重试。
  }
  // 置 Running 由**基类**做(旧形态是子类在 DoStart 中途回调 MarkRunning())。DoStart
  // spawn 的 fiber 与本调用同线程亲和(Affinity::fixed),且 spawn 后至此无挂起点,故它们
  // 不可能先于本行运行、观察到"已 spawn 但尚未 Running"。
  lifecycle_ = LifecycleState::kRunning;
  return Coro::Result<void>{};
}

Coro::Result<void> NodeBase::Close() {
  bool has_fibers = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ == LifecycleState::kClosing ||
        lifecycle_ == LifecycleState::kClosed) {
      return Coro::Result<void>{};  // 幂等:信号已由首个关闭者发过(lifecycle_ 即唯一仲裁点)。
    }
    // 只有 Running 才有 spawn 出去的 fiber 需要发信号唤醒;Created(含 starting_ 期间)
    // 无 fiber、也无 transport 可关,直接落 Closed。
    has_fibers = (lifecycle_ == LifecycleState::kRunning);
    lifecycle_ =
        has_fibers ? LifecycleState::kClosing : LifecycleState::kClosed;
  }
  if (!has_fibers) {
    return Coro::Result<void>{};
  }
  // 锁外发出全部汇合信号。**本函数至此没有任何等待点**——这正是"读循环 / handler 可以
  // 直接调公开的 Close()"的全部依据(旧形态为此另设了受保护的 SignalClose())。
  // 关闭一经置 Closing 即不可回滚,故不据返回值分支(签名与 DoStart 对称)。
  (void)DoClose();
  return Coro::Result<void>{};
}

void NodeBase::WaitClosed() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (joined_ || lifecycle_ == LifecycleState::kCreated) {
      return;  // 已 join 过(get() 一次性),或从未 Start 过:无 fiber 可汇合。
    }
    joined_ = true;
  }
  // 锁外让出式 join 全部内部 fiber(FiberTask::get())。返回即意味着它们已不再运行、
  // 不再触碰本对象成员——这是"可安全析构"的唯一充分条件(ADR-0006 D6)。
  DoJoin();
  std::lock_guard<std::mutex> lock(mutex_);
  lifecycle_ = LifecycleState::kClosed;
}

bool NodeBase::IsRunning() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lifecycle_ == LifecycleState::kRunning;
}

// 供子类判"是否仍在 Created"(见 .hpp)。与 `IsRunning()` 同形:取一眼锁内的相位就走,
// 不构成任何同步保证。
LifecycleState NodeBase::CurrentLifecycle() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return lifecycle_;
}

}  // namespace transport
