#pragma once

/**
 * @file NodeBase.hpp
 * @brief 交互节点生命周期基类 NodeBase——只装"每个节点都有的那一件事":生命周期。
 *
 * **关闭拆成发信号与等收敛两半**,与 `ITransport` 同形:
 *
 * | | 语义 |
 * |---|---|
 * | `Start()` | 幂等启动;`DoStart()` 返回成功即 Running |
 * | `Close()` | **只发信号,不等待收敛**。幂等,**任何 fiber 都可调**(含节点自己的内部 fiber) |
 * | `WaitClosed()` | join 全部内部 fiber——返回即可安全析构。**单调用方** |
 * | `IsRunning()` | 交互前置判据(`Request`/`Send`/`Publish`) |
 *
 * 这个拆分消掉了旧形态里三样东西:
 *
 * 1. **第二个关闭入口**(旧 `SignalClose()`)。旧 `Close()` 结尾无条件等收敛,内部工作单元
 *    调它就是等自己退出、静默挂死,于是分裂出一个"只发信号"的受保护入口,并以**使用契约**
 *    约束谁能调哪个(ADR-0005 D6 想用重入守卫解决,ADR-0006 D8 又撤销)。现在 `Close()`
 *    本身就不等待,契约、守卫、那一整轮反复一并作废。
 * 2. **子类回调基类的三个动作**(旧 `MarkRunning()` / `ConvergeAfterReadLoop()` /
 *    `JoinHandler()`)。基类只向下调钩子,子类不再反向驱动基类状态机:置 Running 由基类在
 *    `DoStart()` 返回后自己做,收敛由 `WaitClosed()` 的调用方经 `DoJoin()` 自上而下完成。
 * 3. **手搓的同步件**(旧三个 `SharedCompletion<void>`)。`close_signalled_` 那个"信号已发完"
 *    的握手,只在"收敛者是个可能被提前唤醒的 fiber"时才需要;现在关闭方自己顺序执行
 *    「发完信号 → join」,顺序由调用栈保证,不需要握手对象。剩下的汇合一律用 AsyncTask
 *    自带的 `Coro::FiberTask<T>::get()`(见其 `doc/使用说明.md` §6.1:捕获对方句柄 `get()`
 *    即多协程 join)。
 *
 * **协议特有的实事经三个钩子交给子类**:`DoStart()`(含配置校验;transport.Start + spawn
 * 读循环 / handler)、`DoClose()`(发出全部汇合信号)、`DoJoin()`(join 自己 spawn 的全部
 * fiber)。基类一律不触及协议类型:本头文件不 include 任何协议 / 消息类型,也不持有任何
 * 队列——`HandlerLoop` 是**可选件**,由 node 持有。
 *
 * 同步纪律(ADR-0003 D8):生命周期状态由一把 `std::mutex` 守,**持锁期间不调任何钩子、
 * 不出现挂起点**;`DoStart()` / `DoClose()` / `DoJoin()` 全在锁外调用。
 */

#include "detail/result.hpp"

#include <mutex>

#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 交互节点生命周期基类:幂等 Start / 只发信号的 Close / join 式 WaitClosed。
 *
 * **库内实现基类**,不是应用扩展点:RT_IF_API「不要求应用继承节点类型」约束的是**应用**,
 * 宿主仍按组合方式使用 `ProtocolNode` / `DdsNode`。
 *
 * 不可拷贝、不可移动(内部 fiber 捕获 this,且持 `std::mutex`)。
 */
class NodeBase {
 public:
  /**
   * @brief 析构:**不**在此收敛。
   *
   * 收敛须调 `DoClose()` / `DoJoin()` 虚钩子,而基类析构时子类已析构完毕、虚派发已退回
   * 基类(纯虚 ⇒ UB)。故每个具体 node 在**自身**析构函数体内调 `Close()` 后再
   * `WaitClosed()`(彼时动态类型仍是该 node,且其成员尚未析构)。
   */
  virtual ~NodeBase();

  NodeBase(const NodeBase&) = delete;
  NodeBase& operator=(const NodeBase&) = delete;

  /**
   * @brief 幂等启动(RT_LIFECYCLE_003 / RT_LIFECYCLE_007)。
   *
   * 出临界区调 `DoStart()`(子实事:配置校验 + transport.Start + spawn 读循环 / handler),
   * 返回成功则由**基类**置 Running;失败退回 Created,允许宿主改配后重试(实现须保证失败
   * 时未 spawn 任何 fiber)。已 Running 再启幂等成功;Closing/Closed 返 `kInvalidState`。
   *
   * **不承诺并发调用可共享同一次初始化结果**:另一次 `Start()` 正在初始化时返
   * `kInvalidState`。旧形态用一个一次性 `SharedCompletion` 共享首次结果,而它 latch 后不再
   * 更新,第二轮重试的调用方会拿到上一轮的陈旧失败(#150);启动是宿主调一次的动作,
   * 不值得为此保留一个坏 latch。
   */
  Coro::Result<void> Start();

  /**
   * @brief 只发关闭汇合信号,**不等待收敛**。幂等。
   *
   * 首个关闭者:Running→Closing(立即拒新交互)→ 锁外调 `DoClose()` 发出全部汇合信号
   * (transport.Close + 业务队列 Close + handler 协作取消 + PendingTable.FailAll)。从未
   * `Start()` 过则直接落 Closed(无 fiber 可汇合,不调 `DoClose()`)。
   *
   * **不含任何等待点,故任何 fiber 都可调**——包括节点自己的读循环与业务处理器。读循环退出
   * 时无条件调本方法即可:我方 `Close` 所致时它是幂等空操作,底层致命错误所致时它就是自终
   * (ADR-0005 D5 的两条路径至此合并,不再需要"退出时是否仍 Running"的判据)。`DdsNode` 的
   * 业务处理器经 `DdsHandlerContext::RequestClose()` 转调之;`ProtocolNode` 的订阅者直接调
   * 本方法即可(ADR-0009 D4:订阅者 fiber 属宿主,不是节点的内部工作单元)。
   *
   * @return 仅表示**已受理**,不表示已关完。要确认收敛完成须由**节点外部**调 `WaitClosed()`。
   */
  Coro::Result<void> Close();

  /**
   * @brief 等待节点**完全收敛**:join 全部内部 fiber,返回即可安全析构。
   *
   * 经 `DoJoin()` 让出式 join(`Coro::FiberTask::get()`),**必须在 fiber 内调用**。
   * 未 `Start()` 或已收敛时立即返回。
   *
   * **单调用方**(节点的所有者),这是本方法与旧 `WaitClosed()` 唯一的能力回退:
   * `FiberTask::get()` 底层是 `boost::fibers::future::get()`,**取过一次即失效**,并发的第二
   * 个等待者会立刻拿到"已收敛"的假答案。旧形态的多等待者是被"`Close()` 要等收敛"逼出来
   * 的——每个 `Close()` 调用方都成了等待者;`Close()` 不再等待之后,真正需要等的只剩所有者
   * 一个。
   *
   * **无 deadline**:`Awaitable::close()` 只保证等待者被**唤醒**,不保证被唤醒的 fiber 已跑完
   * 并不再触碰 `this`,而安全释放要的恰是后者——只有 `get()` 给得了(ADR-0006 D6)。deadline
   * 与"安全析构"二选一,且超时返回后调用方什么也做不了(仍不能析构节点),故取后者。
   */
  void WaitClosed();

  /// @brief 当前是否处于 Running(node 的 Request/Send/Publish 前置状态判据)。
  ///        启动中、关闭中与已关闭一律 false。
  [[nodiscard]] bool IsRunning() const;

 protected:
  NodeBase() = default;

  // —— 子类钩子(协议特有实事;一律锁外调用)————————————————————————————

  /// @brief 启动的协议特有实事:**配置校验** → transport.Start → spawn 读-分发循环 /
  ///        (设了 handler 时)handler 消费者。返回非成功即启动失败,基类退回 Created 允许
  ///        改配重试——实现须保证此时未 spawn 任何 fiber。配置校验放在本钩子**开头**即可,
  ///        不再单设 `ValidateConfig()`:它旧时唯一的存在理由是绕开 `start_done_` 那个坏
  ///        latch,latch 删掉后二者行为一致。
  virtual Coro::Result<void> DoStart() = 0;

  /// @brief 关闭汇合信号的协议特有实事(恒由首个关闭者独占执行一次):按序发出
  ///        transport.Close → 业务队列 Close + handler 协作取消 → PendingTable.FailAll。
  ///        **只发信号、不得等待任何 fiber**(等待属于 `DoJoin()`)——本钩子可能在节点自己的
  ///        读循环 fiber 内被调用。返回 Coro::Result<void> 与 `DoStart` 对称;关闭一经置 Closing 即不可
  ///        回滚,基类不据此分支。
  virtual Coro::Result<void> DoClose() = 0;

  /// @brief join 本节点 spawn 的**全部** fiber(读-分发循环 + 可选 handler 消费者),等其
  ///        实际退出、不强杀(RT_LIFECYCLE_006)。以 `Coro::FiberTask::get()` 让出式实现;
  ///        返回即意味着这些 fiber 已不再运行、不再触碰节点成员。由 `WaitClosed()` 在调用方
  ///        fiber 内调用,**节点自己的 fiber 不得调**(等自己退出)。
  virtual void DoJoin() = 0;

 private:
  mutable std::mutex mutex_;  ///< 守生命周期状态(ADR-0003 D8)。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool starting_{false};  ///< `DoStart()` 正在锁外执行;并发 Start 据此返 kInvalidState。
  bool joined_{false};    ///< `DoJoin()` 已执行过;`WaitClosed()` 据此幂等(get() 一次性)。
};

}  // namespace transport
