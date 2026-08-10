#pragma once

/**
 * @file NodeBase.hpp
 * @brief 交互节点生命周期基类 NodeBase(ADR-0006 D1/D2/D6 / RT_LIFECYCLE / RT_DESIGN_008)。
 *
 * NodeBase 以**模板方法**承载每个交互节点都有的那一件事——生命周期:
 *
 *   1. 状态机 Created→Running→Closing→Closed + 并发幂等 `Start`(共享一次结果、不重复
 *      spawn)+ 关闭仲裁(恒只有一个首个关闭者)+ 多等待者 `WaitClosed`;
 *   2. **收敛**(ADR-0006 D6,不可下放):读循环退出后 join 内部工作单元 → Drain 未启动
 *      业务归因 close_drop → 置 Closed → 广播唤醒全部等待者。`Awaitable::close()` 只保证
 *      等待者被**唤醒**,不保证被唤醒的 fiber 已跑完并不再触碰 `this`;安全释放要的是后者,
 *      只能靠 join,故这段留在基类只此一份;
 *   3. **致命错误自终**(ADR-0005 D5 / RT_LIFECYCLE_008):读循环退出时若节点仍 Running,
 *      由读循环自行发出与 `Close` **完全相同**的一组汇合信号再走**同一段**收敛;
 *   4. **无重入守卫**(ADR-0006 D8 撤销 ADR-0005 D6):`Close()` 结尾无条件等收敛,不比对
 *      fiber id、不设"半执行"分支;"内部工作单元不等待本节点关闭"由**使用契约**保证——
 *      处理器走只发信号的 `SignalClose()`,读循环收敛走内部的 `ConvergeAfterReadLoop()`。
 *
 * **协议特有的实事由子类经钩子提供**(ADR-0006 D1):`ValidateConfig()`(配置校验)、
 * `DoStart()`(transport.Start + spawn 读循环 / handler)、`DoClose()`(发出全部汇合信号:
 * transport.RequestClose + 业务队列 Close + handler 协作取消 + PendingTable.FailAll)、
 * `JoinHandler()` / `DrainUnstartedBusiness()`(收敛时对可选 handler 小件的两个叶子
 * 动作)。
 * 基类一律不触及协议类型:本头文件不 include 任何协议/消息类型,也不持有 handler 队列
 * ——`HandlerLoop` 是**可选件**,由 node 持有(ADR-0006 D4),基类只装每个节点都有的东西。
 *
 * **公开接口返 `Status` 而非 `bool`**(ADR-0006 D2):`bool` 会把「已 `Running`(成功)」与
 * 「已 `Closing`/`Closed`(RT_LIFECYCLE_003 要求 `kInvalidState`)」压成同一个 `false`,二者
 * 对调用方含义相反;且 RT_LIFECYCLE_007 要求宿主据错误改配置重试。"本次调用是否真正执行
 * 了转换"只在基类内部作为仲裁结果使用,不外露。
 *
 * 同步纪律(ADR-0003 D8):生命周期状态(lifecycle/starting/关闭计数与时延)由一把
 * std::mutex 守;运行时 await 只出现在 fiber 体内的挂起点,唤醒/回调在锁外调用。
 */

#include <cstddef>
#include <mutex>

#include "transport/core/ITraceSink.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/SharedCompletion.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 交互节点生命周期基类:幂等 Start / 关闭仲裁 / 收敛,协议特有实事下沉纯虚钩子。
 *
 * **库内实现基类**,不是应用扩展点:RT_IF_API「不要求应用继承节点类型」约束的是**应用**,
 * 宿主仍按组合方式使用 `ProtocolNode` / `DdsNode`。
 *
 * 不可拷贝、不可移动(读循环 / handler fiber 捕获 this,且持 std::mutex)。
 */
class NodeBase {
 public:
  using Clock = OperationOptions::Clock;

  /**
   * @brief 析构:**不**在此收敛。
   *
   * 收敛须调 `DoClose()` / `JoinHandler()` / `DrainUnstartedBusiness()` 等虚钩子,而基类
   * 析构时子类已析构完毕、虚派发已退回基类(纯虚 ⇒ UB)。故每个具体 node 在**自身**析构
   * 函数体内调 `Close()`(彼时动态类型仍是该 node,且其成员尚未析构)。
   */
  virtual ~NodeBase();

  NodeBase(const NodeBase&) = delete;
  NodeBase& operator=(const NodeBase&) = delete;

  /**
   * @brief 并发安全幂等启动(RT_LIFECYCLE_003 / RT_LIFECYCLE_007)。
   *
   * 首个 Start 先跑 `ValidateConfig()`(协议特有配置校验):失败原样返回、停在 Created、
   * 不置 starting、不 latch `start_done_`(允许改配 / 重建重试)。通过则置 starting、**出
   * 临界区**调 `DoStart()`(子实事:transport.Start + `MarkRunning()` + spawn 读循环 /
   * handler);`DoStart` 失败退回 Created。首个 Start 的结果经 `start_done_` 共享给并发进来
   * 的 Start(不重复 spawn)。已 Running 再启幂等成功;Closing/Closed 返 kInvalidState。
   */
  Status Start();

  /**
   * @brief 并发安全幂等关闭(RT_LIFECYCLE_004/005/006):发汇合信号 + **等**收敛结果。
   *
   * **Close 只发汇合信号 + 等收敛结果**(ADR-0005 D1),不亲自收敛;即
   * `SignalClose()` + `closed_.Wait()`。首个关闭者:Running→Closing(立即拒新交互)→
   * 汇合信号(`DoClose()`:transport.RequestClose + 业务队列 Close + handler 协作取消 +
   * PendingTable.FailAll)→ Complete `close_signalled_`(读循环据此放行收敛)。收敛由
   * **读循环兼任**:读循环退出后 join handler → Drain 未启动业务归因 close_drop → 置
   * Closed → `closed_.Complete`(见 `ConvergeAfterReadLoop`)。从未 spawn 读循环
   * (Created/starting)时无收敛者,由 `SignalClose` 就地收敛。后续关闭者共享 `closed_`
   * (多等待者);已 Closed 再关直接成功。**读循环因致命错误自终**(D5)时本函数即"后续
   * 关闭者",一样等 `closed_`。
   *
   * **无重入守卫**(ADR-0006 D8):结尾无条件 `closed_.Wait()`。内部工作单元(读-分发循环、
   * 入站业务处理器)**不得**调用本方法——那是等自己退出,会静默挂死;这是**使用契约**
   * (RT_LIFECYCLE_005),处理器请求关闭须走只发信号的 `SignalClose()`。
   */
  Status Close();

  /// @brief 等待节点收敛到 Closed(多等待者;支持 deadline)。**只应由节点外部调用**
  ///        (RT_LIFECYCLE_005 使用契约:内部工作单元等待收敛 = 等自己退出,静默挂死;
  ///        无运行时守卫,ADR-0006 D8)。
  ///        取消能力已随 SharedCompletion 轻量化移除(ADR-0006 D3,#137)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {});

  /// @brief 当前是否处于 Running(node 的 Request/Send/Publish 前置状态判据)。
  [[nodiscard]] bool IsRunning() const;

  /// @brief 观测:Close 时业务队列内未启动、被 Drain 丢弃归因的业务事件累计数(close_drop)。
  [[nodiscard]] std::size_t CloseDropCount() const;

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const;

 protected:
  /**
   * @brief 构造基类。
   *
   * @param trace_sink 可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。
   *                   用于生命周期跃迁 `RecordEvent` 与 Close 时的 close_drop 批量归因。
   *                   RT_TRACE_002:为空时不改变任何控制流/计数,仅一次判空。
   */
  explicit NodeBase(ITraceSink* trace_sink = nullptr);

  // —— 子类钩子(协议特有实事)——————————————————————————————————————————

  /// @brief 协议特有配置校验(RT_LIFECYCLE_007):非成功即拒绝启动、停 Created 可重试,
  ///        且**不** latch `start_done_`。基类在首个 Start 的临界区内调用(实现不得取节点
  ///        自身的锁,也不得挂起)。默认无配置可校验。
  virtual Status ValidateConfig() const { return Status{}; }

  /// @brief 首个 Start 的协议特有实事(锁外调用):transport.Start → `MarkRunning()` →
  ///        spawn 读-分发循环 /(设了 handler 时)handler 消费者。返回非成功即启动失败,
  ///        基类退回 Created 允许重试(实现须保证此时未 `MarkRunning`、未 spawn)。
  virtual Status DoStart() = 0;

  /// @brief 关闭汇合信号的协议特有实事(锁外调用,恒由首个关闭者独占执行一次):按序发出
  ///        transport.RequestClose → 业务队列 Close + handler 协作取消 →
  ///        PendingTable.FailAll。
  ///        **顺序即契约**:transport.RequestClose 一执行读循环就可能被唤醒退出,故其余信号
  ///        必须在同一段内发完,基类随后才 Complete `close_signalled_` 放行收敛。
  ///        返回 Status 与 `DoStart` 对称;关闭一经置 Closing 即不可回滚,基类不据此分支。
  virtual Status DoClose() = 0;

  /// @brief 收敛叶子动作:让出式 join 入站业务处理器(等其实际退出,不强杀,
  ///        RT_LIFECYCLE_006 / ADR-0005 D2 的 `FiberTask::get()`)。未设 handler 的节点无可
  ///        汇合者,默认空实现。基类在**读循环自身 fiber 内**、锁外调用。
  virtual void JoinHandler() {}

  /// @brief 收敛叶子动作:Drain 业务队列内未启动的排队业务(不排空处理),返回其条数;
  ///        归因 close_drop 由基类做(与置 Closed 同一临界区)。默认无队列可 Drain。
  ///        基类**持生命周期锁**调用:实现不得取节点自身的锁、不得挂起。
  virtual std::size_t DrainUnstartedBusiness() { return 0; }

  // —— 供子类驱动的生命周期动作 ——————————————————————————————————————

  /**
   * @brief 置 Running(`DoStart` 内、spawn 各 fiber 之前调):lifecycle=Running、清 starting。
   *        与 lifecycle_ 同锁置位,令并发 Close 一致地观察到 Running(据此判定收敛者就位)。
   */
  void MarkRunning();

  /**
   * @brief 只发关闭汇合信号、**不等待**收敛(RT_LIFECYCLE_005 / ADR-0006 D8)。
   *
   * `Close` 的前半段:并发安全幂等地做首个关闭者仲裁并发出全部汇合信号,随即返回;收敛由
   * **读循环**完成(`ConvergeAfterReadLoop`)。命名与 `ITransport::RequestClose()`(发信号)
   * / `WaitClosed()`(等待)的仓内既有约定一致——本方法即 node 侧"发信号"那一半。
   *
   * **这是内部工作单元唯一被授权的关闭入口**:它不含任何等待点,故处理器 fiber 内调用
   * 也不自锁,框架因此不需要运行时重入守卫(ADR-0006 D8 撤销 ADR-0005 D6)。node 经
   * `HandlerContext::RequestClose()` / `DdsHandlerContext::RequestClose()` 暴露之。
   *
   * @return 仅表示**已受理**(关闭已发起或此前已发起/已完成),**不表示已关完**。要确认
   *         收敛完成须由**节点外部**调 `WaitClosed()`(内部工作单元不得等,见
   *         RT_LIFECYCLE_005)。
   */
  Status SignalClose();

  /**
   * @brief 读循环退出后的收敛尾段(ADR-0005 D1 / ADR-0006 D6):读循环兼任收敛者,在其自身
   *        fiber 内跑完整个收敛,故**不得调公开的 `Close()`**(那会等 closed_ = 等自己退出)。
   *
   * 依次:自终判定 → 等关闭汇合信号 →(设 handler 时)join handler → 收敛到 Closed。
   *
   * **致命错误自终(ADR-0005 D5 / RT_LIFECYCLE_008)**:读循环退出有两种成因——我方
   * `Close`(信号已在退出前发出),或不具重连能力的传输发生底层致命错误而节点仍 `Running`
   * (ADR-0004 D1 后二者同为 kClosed、读循环无从区分)。后者由本 fiber **自行**置 Closing
   * 并发出与 `Close` 完全相同的一组汇合信号,再走下面同一段收敛代码;不这样做则业务队列未
   * Close、handler 未取消,节点将停在 `Running` 而收发已终止,`WaitClosed` 的等待者永不被
   * 唤醒(僵尸节点)。我方 `Close` 所致时 lifecycle 已是 Closing/Closed,自终判定原样退让,
   * `Wait` 至多挂起到 `Close` 把余下的汇合信号发完(读循环可能在 transport.RequestClose 后、
   * 其余信号发出前就已退出);自终时则是本 fiber 自己刚刚 Complete 的,`Wait` 立即返回。
   * **TCP 客户端天然落不到自终分支**:它无限重连,`Read` 只在我方 `Close` 后返 kClosed,
   * 彼时 lifecycle 已非 Running——无需按介质分支,判据只是"读循环退出时是否仍 Running"。
   * **与 RT_LIFECYCLE_005 的边界**:自终的收敛驱动者是读循环,而 D1 之后**无人等待读循环
   * 退出**,故它不是"被收敛所等待的内部工作单元",不构成自等待。
   */
  void ConvergeAfterReadLoop();

 private:
  /**
   * @brief 置 Closing 并发出**全部**关闭汇合信号——`Close` 与致命错误自终共用的同一段发起
   *        代码(ADR-0005 D5:正常关闭与自终合并为一条路径,区别仅在"谁先置的 Closing")。
   *
   * 首个关闭者(lifecycle 尚非 Closing/Closed)独占地:置 Closing(立即拒新交互)→ 记关闭
   * 时延起点 → 锁外调 `DoClose()` 发出全部汇合信号 → 最后 Complete `close_signalled_` 放行
   * 收敛者。并发的 `Close` 与自终以 lifecycle_ 为唯一仲裁点,故**恒只有一个发起者**,
   * `close_signalled_` 也只被 Complete 一次。
   *
   * @param[out] read_loop_converges 非空时写出"收敛者是否已就位":自 Running 进入 Closing
   *             ⇒ `DoStart` 已 spawn 读循环 ⇒ 由它收敛;否则(Created/starting)无人收敛,
   *             调用者须就地 `ConvergeToClosed`。非首个关闭者时写 false(其无收敛职责)。
   * @return 本调用是否为首个关闭者。
   */
  bool SignalCloseIfFirstCloser(bool* read_loop_converges);

  /// @brief 收敛到 Closed 的共用尾段(读循环收敛路径与"从未 spawn 读循环"直接收敛分支
  ///        共用):Drain 未启动的排队业务逐条归因 close_drop(不排空处理)→ 置 Closed +
  ///        记关闭时延(P5-4)→ lifecycle Trace → `closed_.Complete` 广播唤醒全部等待者。
  void ConvergeToClosed();

  /// 可选 Trace 出口(非拥有,P5-3/P5-4):生命周期 `RecordEvent` 与 close_drop 归因。
  /// 写一次(构造)不再变,读不需持锁。
  ITraceSink* trace_sink_{nullptr};

  SharedCompletion<void> start_done_;  ///< 首个 Start 初始化结果(并发 Start 共享)。
  /// 首个关闭者(外部 `Close` 或致命错误自终的读循环自身,D5)已发出全部汇合信号
  /// (读循环退出后据此放行收敛;D1)。
  SharedCompletion<void> close_signalled_;
  SharedCompletion<void> closed_;  ///< 节点 Closed 通知(Close/WaitClosed 等待点)。

  mutable std::mutex mutex_;  ///< 守生命周期状态与关闭归因(ADR-0003 D8)。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool starting_{false};  ///< 首个 Start 正在初始化(并发 Start 据此 await start_done_)。
  std::size_t close_drop_count_{0};
  Clock::time_point close_requested_at_{};  ///< P5-4:Close 首个调用者置 Closing 的时刻。
  Clock::duration last_close_latency_{};    ///< P5-4:最近一次关闭时延(简单存最近值)。
};

}  // namespace transport
