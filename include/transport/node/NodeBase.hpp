#pragma once

/**
 * @file NodeBase.hpp
 * @brief 交互节点生命周期基类 NodeBase——只装"每个节点都有的那一件事":生命周期。
 *
 * **关闭拆成发信号与等收敛两半**,与 `ITransport` 同形:
 *
 * | | 语义 |
 * |---|---|
 * | `Start()` | 幂等启动;`DoStart()` 返回成功即 Running(除非这期间 `Close()` 已受理,#220) |
 * | `Close()` | **只发信号,不等待收敛**。幂等,**任何 fiber 都可调**(含节点自己的内部 fiber) |
 * | `WaitClosed()` | join 全部内部 fiber——返回即可安全析构。**单调用方** |
 * | `IsRunning()` | 交互前置判据(`Request`/`Send`/`Publish`) |
 *
 * **协议特有的实事经三个钩子交给子类**:`DoStart()`(含配置校验;取读订阅 + spawn 读-分发
 * 循环)、`DoClose()`(发出全部汇合信号)、`DoJoin()`(join 自己 spawn 的全部
 * fiber)。基类一律不触及协议类型:本头文件不 include 任何协议 / 消息类型,也不持有任何
 * 队列——入站业务的排队与消费由订阅侧承载(ADR-0009),由 node 自己持有。
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
   * 出临界区调 `DoStart()`(子实事:配置校验 + 取读订阅 + spawn 读-分发循环),
   * 返回成功则由**基类**置 Running;失败退回 Created,允许宿主改配后重试(实现须保证失败
   * 时未 spawn 任何 fiber)。已 Running 再启幂等成功;Closing/Closed 返 `kInvalidState`。
   *
   * **`DoStart()` 期间来过 `Close()` 则关闭赢**(#220):置 Running 前复查这一笔,
   * `DoStart()` 成功即置 Closing 并**在此补发 `DoClose()`**(那次 `Close()` 只受理、未发
   * 信号),返 `kInvalidState`——节点不曾也不会进入 Running;`DoStart()` 失败则直接落
   * Closed(无 fiber 可收),返 `DoStart()` 的错误码,且不再可重试。**不无条件写回 Running**:
   * 那会把一个已答应关闭的节点复活,而那次关闭的收敛信号从未发出。
   *
   * **不承诺并发调用可共享同一次初始化结果**:另一次 `Start()` 正在初始化时返
   * `kInvalidState`。启动是宿主调一次的动作。
   */
  Coro::Result<void> Start();

  /**
   * @brief 只发关闭汇合信号,**不等待收敛**。幂等。
   *
   * 首个关闭者:Running→Closing(立即拒新交互)→ 锁外调 `DoClose()` 发出全部汇合信号。
   * 从未 `Start()` 过则直接落 Closed(无 fiber 可汇合,不调 `DoClose()`)。
   *
   * **`Start()` 正跑在 `DoStart()` 里时只记账**(#220):此刻的 `Created` 是"启动未完成",
   * 有没有 fiber 要收还不知道,故本方法受理即返回、不落相位、不调 `DoClose()`,处置由
   * `Start()` 收尾统一做(见其说明)。对调用方而言语义不变:返回即"已受理",要确认收敛
   * 仍须 `WaitClosed()`。
   *
   * **不含任何等待点,故任何 fiber 都可调**——包括节点自己的读循环与宿主的订阅消费 fiber
   * (ADR-0009 D4:订阅者 fiber 属宿主,不是节点的内部工作单元)。读循环退出时无条件调本
   * 方法即可:我方 `Close` 所致时它是幂等空操作,底层致命错误所致时它就是自终。
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
   * **单调用方**(节点的所有者):`FiberTask::get()` 底层是 `boost::fibers::future::get()`,
   * **取过一次即失效**,并发的第二个等待者会立刻拿到“已收敛”的假答案。
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

  /**
   * @brief 当前生命周期相位——**供子类判"是否仍在 `Created`"**,不进公开面。
   *
   * `IsRunning()` 分不出 `Created` 与 `Closed`(两者都是 false),而 `DdsNode` 的四个注册
   * 方法要求"**只允许 `Start()` 之前注册**"(ADR-0013 **D16**),非 `Created` 一律返
   * `kInvalidState`——这个判据只有基类给得了:注册表是子类的,但相位是基类的,且
   * "从未 `Start()` 就 `Close()`"这条路径根本不调 `DoClose()`,子类自持一份标志必然漂移。
   *
   * 只读一眼当下的相位,**不构成任何同步保证**:返回后相位可能立即变化。故它只适合
   * "拒绝非法调用序"这类用途,不适合当临界区判据。
   */
  [[nodiscard]] LifecycleState CurrentLifecycle() const;

  // —— 子类钩子(协议特有实事;一律锁外调用)————————————————————————————

  /// @brief 启动的协议特有实事:**配置校验** → 取读订阅 → spawn 读-分发循环。返回非成功即
  ///        启动失败,基类退回 Created 允许改配重试——实现须保证此时未 spawn 任何 fiber。
  ///        配置校验放在本钩子**开头**做,不单设 `ValidateConfig()`。
  virtual Coro::Result<void> DoStart() = 0;

  /// @brief 关闭汇合信号的协议特有实事(恒由首个关闭者独占执行一次):关闭本节点的读订阅,
  ///        并关闭全部订阅信箱(即订阅者的协作取消信号)。**只发信号、不得等待任何 fiber**
  ///        (等待属于 `DoJoin()`)——本钩子可能在节点自己的读循环 fiber 内被调用。返回
  ///        Coro::Result<void> 与 `DoStart` 对称;关闭一经置 Closing 即不可回滚,基类不据此
  ///        分支。
  virtual Coro::Result<void> DoClose() = 0;

  /// @brief join 本节点 spawn 的**全部** fiber(读-分发循环),等其
  ///        实际退出、不强杀(RT_LIFECYCLE_006)。以 `Coro::FiberTask::get()` 让出式实现;
  ///        返回即意味着这些 fiber 已不再运行、不再触碰节点成员。由 `WaitClosed()` 在调用方
  ///        fiber 内调用,**节点自己的 fiber 不得调**(等自己退出)。
  virtual void DoJoin() = 0;

 private:
  mutable std::mutex mutex_;  ///< 守生命周期状态(ADR-0003 D8)。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool starting_{false};  ///< `DoStart()` 正在锁外执行;并发 Start 据此返 kInvalidState。
  /// `starting_` 期间来过 `Close()`(已受理、尚未落相位),由 `Start()` 收尾处置(#220)。
  bool close_pending_{false};
  bool joined_{false};  ///< `DoJoin()` 已执行过;`WaitClosed()` 据此幂等(get() 一次性)。
};

}  // namespace transport
