#pragma once

/**
 * @file NodeRuntime.hpp
 * @brief 协议无关的节点运行时机制件 NodeRuntime<Event>(ADR-0003 D10/D12 收口 /
 *        RT_DESIGN_008 / RT_NODE_003 / RT_LIFECYCLE / RT_HANDLER)。
 *
 * NodeRuntime 把多个交互节点(ProtocolNode / 后续 DdsNode)共享的**协议无关机制**收成
 * 一个可组合薄件,像 `BoundedQueue<T>` / `PendingTable<Key,T>` 一样独立、协议无关:
 *
 *   1. 生命周期状态机(Created→Running→Closing→Closed)+ 并发幂等 Start(共享一次结果、
 *      不重复 spawn)+ **收敛并入读循环**(ADR-0005 D1:读循环退出后兼任收敛者,等 handler
 *      退出 → Drain 归因 → 置 Closed;Close 只发汇合信号并等结果)+ **致命错误自终**
 *      (ADR-0005 D5 / RT_LIFECYCLE_008:读循环退出时若节点仍 Running 即非我方 Close 所致,
 *      由读循环自行置 Closing 并发同一组汇合信号,再走同一段收敛)+ 多等待者 WaitClosed +
 *      重入自锁防护(比对 handler fiber id)。
 *   2. handler 消费者的**持有与驱动**:消费者 fiber + `BoundedQueue<Event>` + 协作取消 +
 *      异常隔离 + 时长计量已整体下沉到可选小件 `HandlerLoop<Event>`(ADR-0006 D4),
 *      runtime 持有一个并在生命周期各段驱动它(bring-up 时 Spawn、汇合时 CancelAndClose、
 *      收敛时 Join + DrainForClose),对外的 handler 面(Enqueue / 取消令牌 / 计数)为转发;
 *      **close_drop 归因留在 runtime**——"为什么丢"属关闭语义,非 HandlerLoop 之责。
 *   3. 读-分发循环**骨架**:`SpawnReadLoop(decodeAndDispatchFn)` —— runtime 跑
 *      `Read → 错误分类(仅 kClosed 退出、其余继续)→ 调 node 提供的 decodeAndDispatch`
 *      (ADR-0004 D1:读取终止语义单值化,三介质同一段读循环、无介质分支)。
 *
 * **纪律(RT_NODE_003 / RT_DESIGN_008 / D10)**:NodeRuntime 是**机制**件,不是回调
 * `KeyOf`/`IsTerminal`/`RouteUnmatched` 的共享引擎——node **组合并驱动**它(node 调
 * `runtime.Enqueue`/`runtime.MarkRunning`/`runtime.SpawnReadLoop(fn)`),协议特有语义(key
 * 派生、frm_type 盖章、session_id 分配、Dispatch 分类、终结判别、寻址)全内联在 node 的
 * 回调体里,runtime 一律不触及。Event 全程不透明:runtime 不读其任何字段
 * (字节计量靠构造时注入 byte_size_of 回调),不含 frm_type/session_id/correlation/连接概念。
 *
 * 同步纪律(D8):生命周期状态(lifecycle/starting/关闭计数与时延)由一把 std::mutex 守;
 * handler 侧状态(fiber 句柄/ id /异常计数/时长)由 `HandlerLoop` 自守其锁;运行时 await
 * 只出现在 fiber 体内的挂起点,唤醒/回调在锁外调用。
 */

#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <utility>

#include "task/fibertask.h"  // Coro::makeTask —— 读循环 fiber。

#include "transport/node/HandlerLoop.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/TraceCategories.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/SharedCompletion.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 协议无关的节点运行时机制件:生命周期 + handler 消费者 + 读循环骨架。
 *
 * @tparam Event 入站业务事件载荷类型(node 传 Message);runtime 对其不透明,仅经构造时
 *               注入的 byte_size_of 回调计量字节,不读任何字段(RT_DESIGN_008 / D10)。
 *
 * 不可拷贝、不可移动(读循环 / handler fiber 捕获 this,且持 std::mutex)。
 */
template <typename Event>
class NodeRuntime {
 public:
  using Clock = OperationOptions::Clock;

  /**
   * @brief 构造运行时。
   *
   * @param transport     非拥有的字节管道指针(node 持 unique_ptr,runtime 只用于读循环
   *                      Read 与收敛 RequestClose);须在 runtime 之上存活(node 成员序保证)。
   * @param byte_size_of  业务队列的字节计量回调(node 传 payload.size());runtime 不读字段。
   * @param max_events    业务队列事件数上界(越界由 BoundedQueue 钳制)。
   * @param max_bytes     业务队列字节数上界(越界由 BoundedQueue 钳制)。
   * @param trace_sink    可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。
   *                      转交 HandlerLoop(业务队列 kBusinessQueueOverflow 归因 + handler
   *                      调用起止点),并用于 Close 时的 close_drop 批量归因与生命周期
   *                      `RecordEvent`。RT_TRACE_002:为空时不改变任何控制流/计数,
   *                      `RecordEvent`/`RecordDrop` 仅一次判空。
   */
  NodeRuntime(ITransport* transport,
              typename HandlerLoop<Event>::ByteSizeOf byte_size_of,
              std::size_t max_events, std::size_t max_bytes,
              ITraceSink* trace_sink = nullptr)
      : transport_(transport),
        handler_loop_(std::move(byte_size_of), max_events, max_bytes, trace_sink),
        trace_sink_(trace_sink) {}

  NodeRuntime(const NodeRuntime&) = delete;
  NodeRuntime& operator=(const NodeRuntime&) = delete;

  // —— 生命周期机制 ————————————————————————————————————————————————————————

  /**
   * @brief 并发安全幂等启动(RT_LIFECYCLE_003 / RT_LIFECYCLE_007)。
   *
   * 首个 Start 先跑 node 的 @p validate(协议特有配置校验):失败原样返回、停在 Created、
   * 不置 starting、不 latch start_done_(允许改配/重建重试)。通过则置 starting、出临界区
   * 调 node 的 @p bring_up(node 侧:transport.Start + MarkRunning +
   * SpawnReadLoop / SpawnHandlerLoop);bring_up 失败退回 Created。首个 Start 的结果经
   * start_done_ 共享给并发进来的 Start(不重复 spawn)。已 Running 再启幂等成功;Closing/
   * Closed 返 kInvalidState。
   *
   * @param validate  协议特有配置校验(返回非成功即拒绝、停 Created 可重试)。
   * @param bring_up  首个 Start 的实事(transport 启动 + 置 Running + spawn 各 fiber)。
   */
  Status Start(const std::function<Status()>& validate,
               const std::function<Status()>& bring_up) {
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
          if (validate) {
            if (auto valid = validate(); !valid) {
              return valid;
            }
          }
          starting_ = true;
          do_init = true;
          break;
      }
    }
    if (!do_init) {
      return start_done_.Wait();  // 并发 Start:共享首个 Start 的结果,不重复创建资源。
    }

    Status started = bring_up();  // node 实事:失败时未 MarkRunning、仍在 Created。
    if (!started) {
      std::lock_guard<std::mutex> lock(mutex_);
      starting_ = false;  // 退回 Created 允许重试;并发 await 者共享此失败结果。
    }
    start_done_.Complete(started);
    return started;
  }

  /**
   * @brief 置 Running(bring_up 内、spawn 各 fiber 之前调):lifecycle=Running、清 starting。
   *        与 lifecycle_ 同锁置位,令并发 Close 一致地观察到 Running(据此判定收敛者就位)。
   */
  void MarkRunning() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      lifecycle_ = LifecycleState::kRunning;
      starting_ = false;
    }
    // 生命周期跃迁 Trace(P5-4:Created→Running;类别原名 "close",#98 改 lifecycle)。
    RecordEvent(kTraceCategoryLifecycle, trace_sink_, "running");
  }

  /**
   * @brief 登记 node 侧协议特有收敛信号(典型为 `PendingTable.FailAll(kClosed)`)。
   *
   * 由 node 在**构造期**登记一次:关闭汇合有两个发起点——外部 `Close` 与读循环的致命错误
   * 自终(ADR-0005 D5),二者须发出**完全相同**的一组汇合信号,故该回调不能再作为 `Close`
   * 的入参(自终路径无从取得),改由 runtime 持有。
   *
   * 同步纪律:与 `trace_sink_` 同——写一次(node 构造期,节点尚未发布给任何 fiber)、
   * 此后只读不再变,故读取不需持锁。
   *
   * @param signal 协议特有收敛信号(锁外调用,可为空)。
   */
  void SetNodeConvergenceSignal(std::function<void()> signal) {
    node_convergence_signal_ = std::move(signal);
  }

  /**
   * @brief 并发安全幂等关闭(RT_LIFECYCLE_004/005/006)。
   *
   * **Close 只发汇合信号 + 等收敛结果**(ADR-0005 D1),不亲自收敛。首个关闭者:
   * Running→Closing(立即拒新交互)→ 汇合信号(见 SignalCloseIfFirstCloser:runtime 侧
   * transport.RequestClose + 业务队列 Close + handler 协作取消,node 侧已登记的收敛信号)
   * → Complete close_signalled_(读循环据此收敛)。收敛由**读循环兼任**:读循环 Read 返
   * kClosed 退出后等 handler 退出 → Drain 未启动业务归因 close_drop → 置 Closed →
   * closed_.Complete(见 ConvergeAfterReadLoop)。
   * 从未 spawn 读循环(Created/starting)时无收敛者,由本函数就地收敛。
   * 后续关闭者共享 closed_(多等待者);已 Closed 再关直接成功。**读循环因致命错误自终**
   * (D5)时本函数即"后续关闭者",一样等 closed_。当前若就是 handler 消费者 fiber(重入)
   * → 只发起拆卸、跳过对 closed_ 的自等待(避自锁),节点由读循环收敛。
   */
  Status Close() {
    // 重入判据自成一体:问的是"**当前 fiber** 是不是那个消费者",与 lifecycle_ 无关,故
    // 不必与之同锁(HandlerLoop 自守其 fiber id 锁)。且消费者 fiber 一进入 fiber 体、
    // 早于任何一次 Pop 就已登记其 id,故它调到这里时判据恒已就绪。
    const bool in_handler_fiber = handler_loop_.IsCurrentFiber();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (lifecycle_ == LifecycleState::kClosed) {
        return Status{};  // 已 Closed 再关直接成功(RT_LIFECYCLE_004)。
      }
    }

    bool read_loop_converges = false;
    if (SignalCloseIfFirstCloser(&read_loop_converges) && !read_loop_converges) {
      // 从未 spawn 读循环:无收敛者,就地收敛。残留业务(理论上无)一并 close_drop 归因。
      ConvergeToClosed();
    }

    // 重入自锁防护:当前若就是 handler 消费者 fiber,等 closed_ = 等自己退出 = 自锁。
    // 只发起(上文已做)不自等,立即返回;节点由读循环收敛(RT_LIFECYCLE_005)。
    if (in_handler_fiber) {
      return Status{};
    }
    return closed_.Wait();  // 后续关闭者与外部调用者共享同一收敛结果(多等待者)。
  }

  /// @brief 等待节点收敛到 Closed(多等待者;支持 deadline)。handler 消费者 fiber 内且
  ///        未 Closed 时返 kInvalidState(等自己 = 自锁,RT_LIFECYCLE_005)。
  ///        取消能力已随 SharedCompletion 轻量化移除(ADR-0006 D3,#137)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {}) {
    const bool in_handler_fiber = handler_loop_.IsCurrentFiber();  // 见 Close 的同一注解。
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (in_handler_fiber && lifecycle_ != LifecycleState::kClosed) {
        return make_error_code(TransportErrc::kInvalidState);
      }
    }
    return closed_.Wait(std::move(options));
  }

  /// @brief 当前是否处于 Running(node 的 Request/Send 前置状态判据)。
  [[nodiscard]] bool IsRunning() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lifecycle_ == LifecycleState::kRunning;
  }

  // —— 读-分发循环骨架 ——————————————————————————————————————————————————————

  /**
   * @brief spawn 读-分发循环 fiber(骨架):Read → 错误分类 → 调 node 的 decode+dispatch。
   *
   * **三介质同一段读循环、无介质分支、无能力探测**(ADR-0004 D1 / RT_TRANSPORT_008):
   *
   * ```
   * Read() → 成功    → 解码分发
   *        → kClosed → 退出读循环
   *        → 其它     → 瞬时错误,继续
   * ```
   *
   * `kClosed` 是**唯一**的传输终结信号(我方关闭,或不具重连能力的传输发生底层致命错误);
   * 其余失败一律视为可继续的瞬时错误。具备自动重连的传输在内部透明处理链路中断——`Read`
   * 在重连期间挂起、重连后于新链路继续交付,读循环**看不到任何链路中断事件**,故此处不再
   * 有 `kConnection` 分支。
   *
   * **退出后本 fiber 兼任收敛者**(ADR-0005 D1,见 ConvergeAfterReadLoop):两条内部工作
   * 单元中读循环恒是第一个退出的,故它天然是收敛的正确位置——无需独立 finalizer fiber,
   * 也无人再等"读循环已退出"这一事件。
   */
  void SpawnReadLoop(std::function<void(Datagram)> decode_and_dispatch) {
    Coro::makeTask([this, fn = std::move(decode_and_dispatch)]() mutable {
      while (true) {
        auto datagram = transport_->Read();  // 裸读,无 deadline。
        if (!datagram) {
          if (datagram.error() == make_error_code(TransportErrc::kClosed)) {
            break;  // 传输终结(唯一终止语义)→ 退出读循环。
          }
          continue;  // 其它(瞬时错误):丢弃继续。
        }
        fn(std::move(datagram).value());  // node 内联 decode + 分发(协议特有)。
      }
      ConvergeAfterReadLoop();  // 读循环兼任收敛者(D1);不得调公开的 Close(会自等)。
    });
  }

  // —— handler 消费者 + 业务队列(转发至可选小件 HandlerLoop,ADR-0006 D4)——————

  /**
   * @brief spawn 单消费者 handler fiber(串行消费业务队列):出队一条 → 跑 @p consume 到
   *        完成(含其 await)→ 再出下一条(严格串行,RT_HANDLER_003)。
   *
   * 机制全部在 `HandlerLoop<Event>::Spawn` 内(异常隔离、时长计量、fiber 句柄登记);
   * 本方法只是 bring-up 期的驱动点。**未调本方法即"未设 handler"**:HandlerLoop 保持无
   * 消费者状态,收敛时无可汇合者。
   */
  void SpawnHandlerLoop(std::function<void(Event&&)> consume) {
    handler_loop_.Spawn(std::move(consume));
  }

  /// @brief 入站业务事件入有界队列(满/已 Close 均丢弃、不阻塞);读循环分发业务帧时调。
  Status Enqueue(Event event) { return handler_loop_.Enqueue(std::move(event)); }

  /// @brief handler 协作取消令牌:Close 时被触发,node 经 HandlerContext 暴露给 handler。
  [[nodiscard]] CancellationToken HandlerCancellationToken() const {
    return handler_loop_.Token();
  }

  // —— 观测计数(机制归因,协议无关)——————————————————————————————————————

  /// @brief 业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const {
    return handler_loop_.BusinessQueueOverflowCount();  // HandlerLoop/队列自守其锁。
  }

  /// @brief handler consume 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const {
    return handler_loop_.HandlerExceptionCount();  // HandlerLoop 自守其锁。
  }

  /// @brief Close 时业务队列内未启动、被 Drain 丢弃归因的业务事件累计数(close_drop)。
  [[nodiscard]] std::size_t CloseDropCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return close_drop_count_;
  }

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。简单存最近值(非直方图,分布分析留 P6)。
  [[nodiscard]] Clock::duration LastHandlerDuration() const {
    return handler_loop_.LastHandlerDuration();  // HandlerLoop 自守其锁。
  }

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_close_latency_;
  }

 private:
  /**
   * @brief 置 Closing 并发出**全部**关闭汇合信号——`Close` 与致命错误自终共用的同一段发起
   *        代码(ADR-0005 D5:正常关闭与自终合并为一条路径,区别仅在"谁先置的 Closing")。
   *
   * 首个关闭者(lifecycle 尚非 Closing/Closed)独占地:置 Closing(立即拒新交互)→ 记关闭
   * 时延起点 → 锁外发汇合信号(transport.RequestClose + 业务队列 Close + handler 协作取消
   * + node 侧已登记的收敛信号)→ 最后 Complete close_signalled_ 放行收敛者。并发的 `Close`
   * 与自终以 lifecycle_ 为唯一仲裁点,故**恒只有一个发起者**,close_signalled_ 也只被
   * Complete 一次。
   *
   * @param[out] read_loop_converges 非空时写出"收敛者是否已就位":自 Running 进入 Closing
   *             ⇒ bring_up 已 spawn 读循环 ⇒ 由它收敛;否则(Created/starting)无人收敛,
   *             调用者须就地 ConvergeToClosed。非首个关闭者时写 false(其无收敛职责)。
   * @return 本调用是否为首个关闭者。
   */
  bool SignalCloseIfFirstCloser(bool* read_loop_converges) {
    bool first_closer = false;
    bool converger_ready = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (lifecycle_ != LifecycleState::kClosing &&
          lifecycle_ != LifecycleState::kClosed) {
        first_closer = true;
        // Running ⇒ bring_up 已 spawn 读循环 ⇒ 收敛者就位;否则(Created/starting)无人收敛。
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
    // 汇合信号(锁外):runtime 侧唤醒读循环 + 消费者 + 触发 handler 取消。
    transport_->RequestClose();
    handler_loop_.CancelAndClose();  // 业务队列 Close + handler 协作取消(同一顺序)。
    if (node_convergence_signal_) {
      node_convergence_signal_();  // node 侧:PendingTable.FailAll 等。
    }
    // 最后一步:此时上述信号均已发出,读循环一旦被放行即可无条件走完收敛。**无条件**
    // Complete(即便此刻无收敛者):它是"首个关闭者已发完全部汇合信号"这一事实本身,
    // 无人等待时 Complete 亦无副作用;而漏发一次即等于读循环永久挂在 Wait 上。
    close_signalled_.Complete(Status{});
    return true;
  }

  /**
   * @brief 读循环退出后的收敛尾段(ADR-0005 D1):读循环兼任收敛者,在其自身 fiber 内跑完
   *        整个收敛,故**不得调公开的 `Close()`**(那会等 closed_ = 等自己退出 = 自锁)。
   *
   * 依次:自终判定 → 等关闭汇合信号 →(设 handler 时)等 handler 退出 → ConvergeToClosed。
   *
   * **致命错误自终(ADR-0005 D5 / RT_LIFECYCLE_008)**:读循环退出有两种成因——我方
   * `Close`(信号已在退出前发出),或不具重连能力的传输发生底层致命错误而节点仍 `Running`
   * (ADR-0004 D1 后二者同为 kClosed、读循环无从区分)。后者由本 fiber **自行**置 Closing
   * 并发出与 `Close` 完全相同的一组汇合信号(SignalCloseIfFirstCloser),再走下面同一段
   * 收敛代码;不这样做则业务队列未 Close、handler 未取消,节点将停在 `Running` 而收发已
   * 终止,`WaitClosed` 的等待者永不被唤醒(僵尸节点)。
   * 我方 `Close` 所致时 lifecycle 已是 Closing/Closed,自终判定原样退让,`Wait` 至多挂起到
   * `Close` 把余下的汇合信号发完(读循环可能在 transport.RequestClose 后、其余信号发出前
   * 就已退出);自终时则是本 fiber 自己刚刚 Complete 的,`Wait` 立即返回。
   * **TCP 客户端天然落不到自终分支**:它无限重连,`Read` 只在我方 `Close` 后返 kClosed,
   * 彼时 lifecycle 已非 Running——无需按介质分支,判据只是"读循环退出时是否仍 Running"。
   * **与 RT_LIFECYCLE_005 的边界**:自终的收敛驱动者是读循环,而 D1 之后**无人等待读循环
   * 退出**(`loop_done_` 已删),故它不是"被收敛所等待的内部工作单元",不构成自等待。
   */
  void ConvergeAfterReadLoop() {
    (void)SignalCloseIfFirstCloser(nullptr);  // 仍 Running ⇒ 致命错误自终(D5)。
    close_signalled_.Wait();  // 信号已 Complete(我方 Close 或上一行自终),立即返回。
    // 结构化并发 join(ADR-0005 D2):HandlerLoop::Join 在本 fiber 内让出,直至 handler
    // 消费者实际退出(RT_LIFECYCLE_006:不强制销毁 fiber,等其协作返回);未设 handler
    // 时立即返回。返回即意味着 handler fiber 已不再运行,可安全置 Closed。
    handler_loop_.Join();
    ConvergeToClosed();
  }

  /// @brief 收敛到 Closed 的共用尾段(#98 收口,读循环收敛路径与"从未 spawn 读循环"
  ///        直接收敛分支共用):Drain 未启动的排队业务逐条归因 close_drop(不排空处理)
  ///        → 置 Closed + 记关闭时延(P5-4)→ lifecycle Trace → closed_.Complete。
  void ConvergeToClosed() {
    Clock::duration latency{};
    {
      std::lock_guard<std::mutex> lock(mutex_);
      // 归因留在 runtime(HandlerLoop 只报条数):close_drop 是关闭语义,与 close 时延、
      // 置 Closed 同一临界区(与拆件前的加锁范围逐字相同)。
      const std::size_t drained = handler_loop_.DrainForClose();
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

  ITransport* transport_;             ///< 非拥有字节管道(读循环 Read + 收敛 RequestClose)。
  /// handler 消费者小件(ADR-0006 D4):业务队列 + 消费者 fiber 句柄 + 协作取消 + 异常隔离
  /// + 时长计量。**可选**——未 SpawnHandlerLoop 时它只是个没人消费的空队列。自守其锁。
  HandlerLoop<Event> handler_loop_;
  /// 可选 Trace 出口(非拥有,P5-3/P5-4):Close 时 close_drop 归因与生命周期 RecordEvent
  /// (handler 侧那份已转交 HandlerLoop)。写一次(构造)不再变,读不需持锁。
  ITraceSink* trace_sink_{nullptr};
  /// node 侧协议特有收敛信号(PendingTable.FailAll 等):`Close` 与致命错误自终(D5)共用。
  /// 与 trace_sink_ 同纪律:node 构造期写一次(节点尚未发布)、此后只读,读不需持锁。
  std::function<void()> node_convergence_signal_;

  SharedCompletion<void> start_done_;    ///< 首个 Start 初始化结果(并发 Start 共享)。
  /// 首个关闭者(外部 `Close` 或致命错误自终的读循环自身,D5)已发出全部汇合信号
  /// (读循环退出后据此放行收敛;D1)。
  SharedCompletion<void> close_signalled_;
  SharedCompletion<void> closed_;  ///< 节点 Closed 通知(Close/WaitClosed 等待点)。

  mutable std::mutex mutex_;  ///< 守生命周期状态与关闭归因(D8);handler 侧由 HandlerLoop 自守。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool starting_{false};  ///< 首个 Start 正在初始化(并发 Start 据此 await start_done_)。
  std::size_t close_drop_count_{0};
  Clock::time_point close_requested_at_{};  ///< P5-4:Close 首个调用者置 Closing 的时刻。
  Clock::duration last_close_latency_{};    ///< P5-4:最近一次关闭时延(简单存最近值)。
};

}  // namespace transport
