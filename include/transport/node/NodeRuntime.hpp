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
 *      不重复 spawn)+ **三方汇合**(读循环 fiber + handler fiber + 收敛回调,独立 finalizer
 *      fiber 收敛到 Closed)+ 多等待者 WaitClosed + 重入自锁防护(比对 handler fiber id)。
 *   2. handler 消费者 fiber + `BoundedQueue<Event>` 集成:入队(Enqueue)/串行消费/异常
 *      隔离(consume 逃逸异常兜住 → 记 handler_exception,不自关)/ close_drop 归因
 *      (Close 时 Drain 未启动业务计数)。
 *   3. 读-分发循环**骨架**:`SpawnReadLoop(decodeAndDispatchFn)` —— runtime 跑
 *      `Read → 错误分类(kClosed/kConnection 退出、其它继续)→ 调 node 提供的 decodeAndDispatch`。
 *
 * **纪律(RT_NODE_003 / RT_DESIGN_008 / D10)**:NodeRuntime 是**机制**件,不是回调
 * `KeyOf`/`IsTerminal`/`RouteUnmatched` 的共享引擎——node **组合并驱动**它(node 调
 * `runtime.Enqueue`/`runtime.MarkRunning`/`runtime.SpawnReadLoop(fn)`),协议特有语义(key
 * 派生、frm_type 盖章、session_id 分配、Dispatch 分类、终结判别、寻址、reactor 连接观察)
 * 全内联在 node 的回调体里,runtime 一律不触及。Event 全程不透明:runtime 不读其任何字段
 * (字节计量靠构造时注入 byte_size_of 回调),不含 frm_type/session_id/correlation/连接概念。
 *
 * 同步纪律(D8):生命周期状态(lifecycle/starting/handler fiber id/计数/finalizer joins)由
 * 一把 std::mutex 守;运行时 await 只出现在 fiber 体内的挂起点,唤醒/回调在锁外调用。
 */

#include <boost/fiber/operations.hpp>

#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include "task/fibertask.h"  // Coro::makeTask —— 读循环 / handler / finalizer fiber

#include "transport/node/BoundedQueue.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Observability.hpp"
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
 * 不可拷贝、不可移动(finalizer / 读循环 / handler fiber 捕获 this,且持 std::mutex)。
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
   *                      传给业务队列(归因 kBusinessQueueOverflow)、Close 时的 close_drop
   *                      批量归因,以及生命周期/handler 的 `RecordEvent`。RT_TRACE_002:为空
   *                      时不改变任何控制流/计数,`RecordEvent`/`RecordDrop` 仅一次判空。
   */
  NodeRuntime(ITransport* transport,
              typename BoundedQueue<Event>::ByteSizeOf byte_size_of,
              std::size_t max_events, std::size_t max_bytes,
              ITraceSink* trace_sink = nullptr)
      : transport_(transport),
        business_queue_(std::move(byte_size_of), max_events, max_bytes,
                        DropReason::kBusinessQueueOverflow, trace_sink),
        trace_sink_(trace_sink) {}

  NodeRuntime(const NodeRuntime&) = delete;
  NodeRuntime& operator=(const NodeRuntime&) = delete;

  // —— 生命周期机制 ————————————————————————————————————————————————————————

  /**
   * @brief 并发安全幂等启动(RT_LIFECYCLE_003 / RT_LIFECYCLE_007)。
   *
   * 首个 Start 先跑 node 的 @p validate(协议特有配置校验):失败原样返回、停在 Created、
   * 不置 starting、不 latch start_done_(允许改配/重建重试)。通过则置 starting、出临界区
   * 调 node 的 @p bring_up(node 侧:transport.Start + 检测 + MarkRunning + SpawnReadLoop/
   * SpawnHandlerLoop + 起 reactor 等);bring_up 失败退回 Created。首个 Start 的结果经
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
   *        与 lifecycle_ 同锁置位,令并发 Close 的 finalizer 一致地观察到 Running/汇合点。
   */
  void MarkRunning() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      lifecycle_ = LifecycleState::kRunning;
      starting_ = false;
    }
    RecordEvent("close", trace_sink_, "running");  // 生命周期跃迁(P5-4:Created→Running)。
  }

  /**
   * @brief 登记一个 finalizer 追加汇合点(node 侧协议特有 fiber 的退出等待,如 reactor)。
   *
   * finalizer 在读循环退出后、handler 汇合前依序运行全部追加 join;runtime 对其内容不透明
   * (只是"等某 fiber 退出"),连接/reactor 概念不下沉 runtime(守 RT_DESIGN_008)。
   * bring_up 内(单 fiber 同步、无挂起)在 spawn 对应 fiber 时登记。
   */
  void AddFinalizerJoin(std::function<void()> join) {
    std::lock_guard<std::mutex> lock(mutex_);
    finalizer_joins_.push_back(std::move(join));
  }

  /**
   * @brief 并发安全幂等关闭(RT_LIFECYCLE_004/005/006)。
   *
   * 首个关闭者:Running→Closing(立即拒新交互)→ 三方汇合信号:runtime 侧
   * (transport.RequestClose + 业务队列 Close + handler 协作取消)+ node 侧
   * @p signal_node_convergence(协议特有:reactor 取消、PendingTable.FailAll 等)。Running
   * 则起独立 finalizer fiber:等读循环退出 → 依序等追加 join(reactor 等)→(设 handler 时)
   * 观察 handler 协作取消 ~500ms、超时记 kInternal 不强杀仍等实退出 → Drain 未启动业务归因
   * close_drop → 置 Closed → closed_.Complete。从未 spawn 读循环(Created/starting)则直接收敛。
   * 后续关闭者共享 closed_(多等待者);已 Closed 再关直接成功。当前若就是 handler 消费者
   * fiber(重入)→ 只发起拆卸、跳过对 closed_ 的自等待(避自锁),节点由 finalizer 收敛。
   *
   * @param signal_node_convergence node 侧协议特有收敛信号(锁外调用,可为空)。
   */
  Status Close(const std::function<void()>& signal_node_convergence) {
    bool first_closer = false;
    bool spawn_finalizer = false;
    bool in_handler_fiber = false;
    const bool has_handler = has_handler_;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      in_handler_fiber = InHandlerFiberLocked();
      if (lifecycle_ == LifecycleState::kClosed) {
        return Status{};  // 已 Closed 再关直接成功(RT_LIFECYCLE_004)。
      }
      if (lifecycle_ != LifecycleState::kClosing) {
        first_closer = true;
        spawn_finalizer = (lifecycle_ == LifecycleState::kRunning);
        lifecycle_ = LifecycleState::kClosing;
        close_requested_at_ = Clock::now();  // P5-4:关闭时延起点。
      }
    }

    if (first_closer) {
      RecordEvent("close", trace_sink_, "closing");  // 生命周期跃迁(Running→Closing)。
      // 三方汇合信号(锁外):runtime 侧唤醒读循环 + 消费者 + 触发 handler 取消。
      transport_->RequestClose();
      business_queue_.Close();
      handler_cancellation_.Cancel();
      if (signal_node_convergence) {
        signal_node_convergence();  // node 侧:reactor 取消 + PendingTable.FailAll 等。
      }
      if (spawn_finalizer) {
        std::vector<std::function<void()>> joins;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          joins = finalizer_joins_;
        }
        // 独立 finalizer fiber:不在调用者 fiber 内自等,天然规避 handler 消费者自等待自锁
        // (RT_LIFECYCLE_005),并让多个 Close/WaitClosed 等待者共享同一 closed_。
        Coro::makeTask([this, has_handler, joins = std::move(joins)] {
          loop_done_.Wait();  // 读循环收到终结后退出并 Complete。
          for (auto& join : joins) {
            join();  // 追加汇合(reactor 等):不早收敛致其触碰已收敛的 node 状态。
          }
          if (has_handler) {
            // handler 协作取消观测:~500ms 内应返回;超时只记 kInternal(不强杀)仍等实退出。
            OperationOptions observe;
            observe.deadline =
                OperationOptions::Clock::now() + kHandlerCancelObservation;
            if (auto within = handler_done_.Wait(observe);
                !within &&
                within.error() == make_error_code(TransportErrc::kTimeout)) {
              {
                std::lock_guard<std::mutex> lock(mutex_);
                ++handler_cancel_overrun_count_;
              }
              handler_done_.Wait();  // 仍等 handler 实际退出(RT_LIFECYCLE_006)。
            }
          }
          Clock::duration latency{};
          {
            std::lock_guard<std::mutex> lock(mutex_);
            // 未启动的排队业务归因 close_drop(不排空处理):逐条 RecordDrop(与原地
            // += size() 同一临界区宽度,只是把裸算术换成归因原语;取舍见 PR 说明)。
            auto drained = business_queue_.Drain();
            for (std::size_t i = 0; i < drained.size(); ++i) {
              RecordDrop(DropReason::kCloseDrop, close_drop_count_, trace_sink_);
            }
            lifecycle_ = LifecycleState::kClosed;
            latency = Clock::now() - close_requested_at_;
            last_close_latency_ = latency;  // P5-4:关闭时延终点。
          }
          RecordEvent("close", trace_sink_, "closed", {}, {}, {},
                      static_cast<long>(std::chrono::duration_cast<
                                         std::chrono::microseconds>(latency)
                                             .count()));
          closed_.Complete(Status{});
        });
      } else {
        // 从未 spawn 读循环:直接收敛。残留业务(理论上无)一并 close_drop 归因。
        Clock::duration latency{};
        {
          std::lock_guard<std::mutex> lock(mutex_);
          auto drained = business_queue_.Drain();
          for (std::size_t i = 0; i < drained.size(); ++i) {
            RecordDrop(DropReason::kCloseDrop, close_drop_count_, trace_sink_);
          }
          lifecycle_ = LifecycleState::kClosed;
          latency = Clock::now() - close_requested_at_;
          last_close_latency_ = latency;  // P5-4:关闭时延终点。
        }
        RecordEvent("close", trace_sink_, "closed", {}, {}, {},
                    static_cast<long>(std::chrono::duration_cast<
                                       std::chrono::microseconds>(latency)
                                           .count()));
        closed_.Complete(Status{});
      }
    }

    // 重入自锁防护:当前若就是 handler 消费者 fiber,等 closed_/handler_done_ = 等自己退出 =
    // 自锁。只发起(上文已做)不自等,立即返回;节点由 finalizer 收敛(RT_LIFECYCLE_005)。
    if (in_handler_fiber) {
      return Status{};
    }
    return closed_.Wait();  // 后续关闭者与外部调用者共享同一收敛结果(多等待者)。
  }

  /// @brief 等待节点收敛到 Closed(多等待者;支持 deadline/取消)。handler 消费者 fiber 内且
  ///        未 Closed 时返 kInvalidState(等自己 = 自锁,RT_LIFECYCLE_005)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {}) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (InHandlerFiberLocked() && lifecycle_ != LifecycleState::kClosed) {
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
   * runtime 跑 `transport_->Read()`;传输终结(kClosed/kConnection)退出循环,其它错误
   * (瞬时/坏帧)丢弃继续;成功则把整帧 Datagram 交 @p decode_and_dispatch(node 内联
   * decode + 协议特有分类/寻址)。退出后 Complete loop_done_(finalizer 汇合点)。
   */
  void SpawnReadLoop(std::function<void(Datagram)> decode_and_dispatch) {
    Coro::makeTask([this, fn = std::move(decode_and_dispatch)]() mutable {
      while (true) {
        auto datagram = transport_->Read();  // 裸读,无 deadline。
        if (!datagram) {
          const auto error = datagram.error();
          if (error == make_error_code(TransportErrc::kClosed) ||
              error == make_error_code(TransportErrc::kConnection)) {
            break;  // 传输终结 → 退出读循环。
          }
          continue;  // 其它(瞬时错误):丢弃继续。
        }
        fn(std::move(datagram).value());  // node 内联 decode + 分发(协议特有)。
      }
      loop_done_.Complete(Status{});
    });
  }

  // —— handler 消费者 + 业务队列机制 ————————————————————————————————————————

  /**
   * @brief spawn 单消费者 handler fiber(串行消费业务队列):出队一条 → 跑 @p consume 到
   *        完成(含其 await)→ 再出下一条(严格串行,RT_HANDLER_003)。
   *
   * consume 逃逸异常被边界兜住 → 记 handler_exception(转 kInternal 隔离当前事件、不自关
   * node、继续下一条,RT_HANDLER_006);这是运行时唯一授权的 catch。队列 Close(Pop 返
   * kClosed)→ 消费者退出、Complete handler_done_。记录本 fiber id 供重入自锁检测。
   */
  void SpawnHandlerLoop(std::function<void(Event&&)> consume) {
    has_handler_ = true;
    Coro::makeTask([this, consume = std::move(consume)]() mutable {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        handler_fiber_id_ = boost::this_fiber::get_id();
        handler_fiber_id_set_ = true;
      }
      for (;;) {
        auto item = business_queue_.Pop();  // 空则协作 await;Close → kClosed 唤醒退出。
        if (!item) {
          break;  // kClosed / kCancelled:队列收敛 → 消费者退出。
        }
        Event event = std::move(item).value();
        RecordEvent("handler", trace_sink_, "start");  // P5-4:调用起点。
        const Clock::time_point started_at = Clock::now();
        bool threw = false;
        try {
          consume(std::move(event));  // 出队一条跑完再出下一条 = 严格串行。
        } catch (...) {
          threw = true;
          std::lock_guard<std::mutex> lock(mutex_);
          ++handler_exception_count_;  // RT_HANDLER_006:边界兜住逃逸异常 → kInternal 隔离。
        }
        const Clock::duration duration = Clock::now() - started_at;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          last_handler_duration_ = duration;  // P5-4:处理器时长(简单存最近值)。
        }
        RecordEvent("handler", trace_sink_, threw ? "exception" : "end", {}, {}, {},
                    static_cast<long>(std::chrono::duration_cast<
                                       std::chrono::microseconds>(duration)
                                           .count()));  // P5-4:调用止点。
      }
      handler_done_.Complete(Status{});
    });
  }

  /// @brief 入站业务事件入有界队列(满/已 Close 均丢弃、不阻塞);读循环分发业务帧时调。
  Status Enqueue(Event event) { return business_queue_.Push(std::move(event)); }

  /// @brief 取出并清空业务队列全部残留事件(供 node 代际隔离归因等);字节计量归零。
  [[nodiscard]] std::vector<Event> DrainBusinessQueue() {
    return business_queue_.Drain();
  }

  /// @brief handler 协作取消令牌:Close 时被触发,node 经 HandlerContext 暴露给 handler。
  [[nodiscard]] CancellationToken HandlerCancellationToken() const {
    return handler_cancellation_.token();
  }

  // —— 观测计数(机制归因,协议无关)——————————————————————————————————————

  /// @brief 业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const {
    return business_queue_.DroppedCount();  // BoundedQueue 自守其锁。
  }

  /// @brief handler consume 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return handler_exception_count_;
  }

  /// @brief Close 时业务队列内未启动、被 Drain 丢弃归因的业务事件累计数(close_drop)。
  [[nodiscard]] std::size_t CloseDropCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return close_drop_count_;
  }

  /// @brief Close 时 handler 协作取消超 ~500ms 观测阈值记 kInternal 的累计次数(TBD-007)。
  [[nodiscard]] std::size_t HandlerCancelOverrunCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return handler_cancel_overrun_count_;
  }

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。简单存最近值(非直方图,分布分析留 P6)。
  [[nodiscard]] Clock::duration LastHandlerDuration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_handler_duration_;
  }

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_close_latency_;
  }

 private:
  /// handler 协作取消观测阈值(TBD-007):Close 后 handler 应在此内协作返回;超时只记
  /// kInternal(不强杀)仍等其实际退出(RT_LIFECYCLE_006)。
  static constexpr std::chrono::milliseconds kHandlerCancelObservation{500};

  /// @brief 是否当前 fiber 即 handler 消费者 fiber(重入自锁检测)。自持锁调用。
  [[nodiscard]] bool InHandlerFiberLocked() const {
    return handler_fiber_id_set_ &&
           boost::this_fiber::get_id() == handler_fiber_id_;
  }

  ITransport* transport_;             ///< 非拥有字节管道(读循环 Read + 收敛 RequestClose)。
  BoundedQueue<Event> business_queue_;  ///< 入站业务事件队列(协议无关):读循环 Push、消费者 Pop。
  /// 可选 Trace 出口(非拥有,P5-3/P5-4):业务队列 kBusinessQueueOverflow 归因、Close 时
  /// close_drop 归因,以及生命周期/handler 的 RecordEvent。写一次(构造)不再变,读不需持锁。
  ITraceSink* trace_sink_{nullptr};
  CancellationSource handler_cancellation_;  ///< handler 协作取消源:Close 时 Cancel。

  SharedCompletion<void> start_done_;    ///< 首个 Start 初始化结果(并发 Start 共享)。
  SharedCompletion<void> loop_done_;     ///< 读循环退出通知(finalizer 等待点)。
  SharedCompletion<void> handler_done_;  ///< 消费者 fiber 退出通知(finalizer 等待点)。
  SharedCompletion<void> closed_;        ///< 节点 Closed 通知(Close/WaitClosed 等待点)。

  mutable std::mutex mutex_;  ///< 守生命周期状态(D8)。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool starting_{false};  ///< 首个 Start 正在初始化(并发 Start 据此 await start_done_)。
  boost::fibers::fiber::id handler_fiber_id_;  ///< handler 消费者 fiber id(重入自锁检测)。
  bool handler_fiber_id_set_{false};
  bool has_handler_{false};  ///< 是否 spawn 了 handler 消费者 fiber(finalizer 据此汇合)。
  std::vector<std::function<void()>> finalizer_joins_;  ///< 追加汇合点(node 协议特有 fiber)。
  std::size_t handler_exception_count_{0};
  std::size_t close_drop_count_{0};
  std::size_t handler_cancel_overrun_count_{0};
  Clock::time_point close_requested_at_{};  ///< P5-4:Close 首个调用者置 Closing 的时刻。
  Clock::duration last_close_latency_{};    ///< P5-4:最近一次关闭时延(简单存最近值)。
  Clock::duration last_handler_duration_{};  ///< P5-4:最近一次 handler 调用时长(简单存最近值)。
};

}  // namespace transport
