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

  bool signal_close = false;  // 窗口内被 Close() 受理过:收敛信号由本次 Start 收尾发出。
  {
    std::lock_guard<std::mutex> lock(mutex_);
    starting_ = false;
    if (!started) {
      // 启动失败:未 spawn 任何 fiber,没有收敛信号可发(与 Created 直接 Close 同形)。
      if (close_pending_) {
        close_pending_ = false;
        lifecycle_ = LifecycleState::kClosed;  // 那次关闭已答应过,不许退回 Created。
      }
      // 无待关闭时停在 Created:允许改配重试。有待关闭则已落 Closed,重试自会得
      // kInvalidState——返回值仍报**启动失败的成因**,那是调用方在此处唯一问不到别处的事。
      return started;
    }
    if (close_pending_) {
      // ★ #220:`DoStart()` 跑在锁外,这期间 `Close()` 可能已受理过一次关闭。此处**不得**
      // 无条件写回 Running——那会把一个已答应关闭的节点复活,且那次关闭的收敛信号从未发出。
      close_pending_ = false;
      lifecycle_ = LifecycleState::kClosing;  // 已 spawn fiber,故走 Closing 而非 Closed。
      signal_close = true;
    } else {
      // 置 Running 由**基类**做。DoStart spawn 的 fiber 与本调用同线程亲和
      // (Affinity::fixed),且 spawn 后至此无挂起点,故
      // 它们不可能先于本行运行、观察到"已 spawn 但尚未 Running"。
      lifecycle_ = LifecycleState::kRunning;
    }
  }
  if (signal_close) {
    // 锁外补发那次被推迟的关闭信号(与 `Close()` 的后半段逐字同形)。**由本函数发**而不是
    // 由当初那次 `Close()` 发:`DoClose()` 要触碰的正是 `DoStart()` 刚建起来的东西
    // (读订阅句柄、Dispatcher),让它跑在**建好之后、且与 DoStart 同一条线程**上,才不会
    // 与仍在写这些成员的 `DoStart()` 撞车。恰好一次:那次 `Close()` 已直接返回,此后再调
    // 一律撞上 Closing 的幂等分支。
    (void)DoClose();
    // 关闭赢了这一局:节点不曾、也不会进入 Running,故本次启动不算成功。错误码取
    // `kInvalidState`,与开头"Closing/Closed 再启"那条分支同码——对调用方而言事实相同:
    // 这个节点的生命周期已经不容启动了。
    return make_error_code(TransportErrc::kInvalidState);
  }
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
    if (starting_) {
      // ★ #220:`DoStart()` 正在锁外跑,此刻的 `Created` 是"启动未完成",不是"没启动过"。
      // **只记账、不落相位**:等 `Start()` 收尾时才知道到底有没有 fiber 要收(见其尾部)。
      // 若在此直接落 Closed,就会对外宣告一个"已关闭"的节点,而它的 `DoStart()` 仍在写
      // 自己的成员、仍在 spawn——`WaitClosed()` 据此相位去 `DoJoin()` 会与之撞车,甚至在
      // fiber 尚未 spawn 出来时就宣告收敛完成。相位停在 `Created` 才是此刻的实话。
      close_pending_ = true;
      return Coro::Result<void>{};  // 已受理(与本方法一贯的语义一致:受理 ≠ 已关完)。
    }
    // 只有 Running 才有 spawn 出去的 fiber 需要发信号唤醒;Created(且不在启动中)
    // 无 fiber、也无 transport 可关,直接落 Closed。
    has_fibers = (lifecycle_ == LifecycleState::kRunning);
    lifecycle_ =
        has_fibers ? LifecycleState::kClosing : LifecycleState::kClosed;
  }
  if (!has_fibers) {
    return Coro::Result<void>{};
  }
  // 锁外发出全部汇合信号。**本函数至此没有任何等待点**——这正是“读循环 / 订阅消费者可以
  // 直接调公开的 Close()”的全部依据。
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
