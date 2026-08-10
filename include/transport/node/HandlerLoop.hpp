#pragma once

/**
 * @file HandlerLoop.hpp
 * @brief 协议无关的 handler 消费者小件 HandlerLoop<Event>(ADR-0006 D4 /
 *        RT_HANDLER_003 / RT_HANDLER_006 / RT_DESIGN_008)。
 *
 * HandlerLoop 把"入站业务事件的单消费者串行处理"收成一个独立小件,拥有该职责所需的
 * **全部**状态:
 *
 *   - `BoundedQueue<Event>` —— 入站业务队列(满即 tail-drop,归因
 *     `kBusinessQueueOverflow`);
 *   - `Coro::FiberTask<void>` 句柄 —— 消费者 fiber 的结构化并发句柄,供收敛者让出式
 *     join(ADR-0005 D2);
 *   - `CancellationSource` —— handler 协作取消源,关闭时触发,令牌经 node 的
 *     HandlerContext 交到业务 handler 手上;
 *   - 逃逸异常隔离与计数(RT_HANDLER_006)+ 单次调用时长计量(P5-4)。
 *
 * **它是可选件**(ADR-0006 D4):未设 handler 的节点根本没有消费者 fiber——故它由 node
 * 持有,不进节点基类(基类只装每个节点都有的东西)。未 `Spawn` 时本对象仍可安全地被
 * `Enqueue`/`Join`/`CancelAndClose`/`DrainForClose`(分别为:入队后无人消费、无可汇合者
 * 立即返回、只关队列、Drain 全部残留)。
 *
 * **协议无关**(RT_DESIGN_008 / ADR-0003 D10):头文件不 include 任何协议类型,Event 全程
 * 不透明——字节计量靠构造时注入的 `byte_size_of` 回调,本件不读 Event 的任何字段。
 *
 * **归因分工**:队列满的 tail-drop 由队列自己归因(`kBusinessQueueOverflow`);关闭时
 * Drain 掉的未启动业务由**调用方**归因 `close_drop`——`DrainForClose()` 只返回条数,
 * 因为"这些事件为什么被丢"属于关闭语义,归 node/收敛者管(与 `BoundedQueue` 把
 * `drop_reason` 交由构造方注入是同一条纪律)。
 *
 * 同步纪律(ADR-0003 D8):消费者 fiber 句柄 / 异常计数 / 时长由一把 std::mutex 守;队列
 * 与取消源各自守其内部状态。**运行时 await 只出现在 fiber 体内的挂起点**——`consume`
 * 一律在锁外调用(其内部可挂起、可回调进 node,持锁调用会与生命周期锁交叉)。
 */

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "task/fibertask.h"  // Coro::makeTask / Coro::FiberTask —— handler 消费者 fiber。

#include "transport/core/Cancellation.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TraceCategories.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/node/BoundedQueue.hpp"

namespace transport {

/**
 * @brief 单消费者 handler 循环:有界业务队列 + 消费者 fiber + 协作取消 + 异常隔离。
 *
 * @tparam Event 入站业务事件载荷类型(node 传 Message);本件对其不透明,仅经构造时注入
 *               的 byte_size_of 回调计量字节(RT_DESIGN_008 / ADR-0003 D10)。
 *
 * 不可拷贝、不可移动(消费者 fiber 捕获 this,且持 std::mutex)。
 */
template <typename Event>
class HandlerLoop {
 public:
  using Clock = OperationOptions::Clock;
  /// 字节计量回调(转发自 BoundedQueue):node 传 payload.size()。
  using ByteSizeOf = typename BoundedQueue<Event>::ByteSizeOf;

  /**
   * @brief 构造 handler 循环(仅建队列;消费者 fiber 待 `Spawn` 时才起)。
   *
   * @param byte_size_of 业务队列的字节计量回调(node 传 payload.size());本件不读字段。
   * @param max_events   业务队列事件数上界(越界由 BoundedQueue 钳制)。
   * @param max_bytes    业务队列字节数上界(越界由 BoundedQueue 钳制)。
   * @param trace_sink   可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。
   *                     传给业务队列(归因 kBusinessQueueOverflow)与 handler 调用起止点的
   *                     `RecordEvent`。RT_TRACE_002:为空时不改变任何控制流/计数。
   */
  HandlerLoop(ByteSizeOf byte_size_of, std::size_t max_events,
              std::size_t max_bytes, ITraceSink* trace_sink = nullptr)
      : queue_(std::move(byte_size_of), max_events, max_bytes,
               DropReason::kBusinessQueueOverflow, trace_sink),
        trace_sink_(trace_sink) {}

  HandlerLoop(const HandlerLoop&) = delete;
  HandlerLoop& operator=(const HandlerLoop&) = delete;

  /**
   * @brief spawn 单消费者 fiber(串行消费业务队列):出队一条 → 跑 @p consume 到完成
   *        (含其 await)→ 再出下一条(严格串行,RT_HANDLER_003)。
   *
   * consume 逃逸异常被边界兜住 → 记 handler_exception(转 kInternal 隔离当前事件、不自关
   * node、继续下一条,RT_HANDLER_006);这是本件唯一授权的 catch。队列 Close(Pop 返
   * kClosed)→ 消费者退出。
   *
   * **保留 `FiberTask` 句柄供收敛者结构化 join**(ADR-0005 D2):`Join()` 以
   * `FiberTask::get()` 等 handler 退出,不再手写完成量——完成由运行时保证(fiber 体走异常
   * 路径时 makeTask 边界仍会置结果),避免"漏 Complete → 收敛永久挂死"。
   */
  void Spawn(std::function<void(Event&&)> consume) {
    auto body = [this, consume = std::move(consume)]() mutable {
      for (;;) {
        auto item = queue_.Pop();  // 空则协作 await;Close → kClosed 唤醒退出。
        if (!item) {
          break;  // kClosed / kCancelled:队列收敛 → 消费者退出。
        }
        Event event = std::move(item).value();
        RecordEvent(kTraceCategoryHandler, trace_sink_, "start");  // P5-4:调用起点。
        const Clock::time_point started_at = Clock::now();
        bool threw = false;
        try {
          consume(std::move(event));  // 出队一条跑完再出下一条 = 严格串行。
        } catch (...) {
          threw = true;
          std::lock_guard<std::mutex> lock(mutex_);
          ++exception_count_;  // RT_HANDLER_006:边界兜住逃逸异常 → kInternal 隔离。
        }
        const Clock::duration duration = Clock::now() - started_at;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          last_duration_ = duration;  // P5-4:处理器时长(简单存最近值)。
        }
        RecordEvent(kTraceCategoryHandler, trace_sink_, threw ? "exception" : "end",
                    {}, {}, {},
                    static_cast<long>(std::chrono::duration_cast<
                                       std::chrono::microseconds>(duration)
                                          .count()));  // P5-4:调用止点。
      }
    };
    // 句柄登记先于 fiber 首次得以运行:makeTask 的 fiber 与本调用同线程亲和
    // (Affinity::fixed(当前线程)),而 bring-up 自置 Running 至此无挂起点,故收敛者
    // (读循环 fiber,同线程)不可能观察到"已 spawn handler 但句柄未登记"。同锁发布保留
    // (#98):不依赖该时序做跨线程可见性保证。
    auto task =
        std::make_shared<Coro::FiberTask<void>>(Coro::makeTask(std::move(body)));
    std::lock_guard<std::mutex> lock(mutex_);
    task_ = std::move(task);
  }

  /// @brief 入站业务事件入有界队列(满/已 Close 均丢弃、不阻塞);读循环分发业务帧时调。
  [[nodiscard]] Status Enqueue(Event event) {
    return queue_.Push(std::move(event));
  }

  /**
   * @brief 让出式 join 消费者 fiber:等其**实际退出**,不强杀(RT_LIFECYCLE_006)。
   *
   * 未 `Spawn`(句柄为空)⇒ 无消费者可汇合,立即返回。`get()` 在调用者 fiber 内让出,直至
   * 消费者协作返回;其返回值仅表征任务是否正常完成(异常/取消 → interrupted),调用方不据
   * 此分支——无论哪条路径,`get()` 返回即意味着消费者 fiber 已不再运行。
   */
  void Join() {
    std::shared_ptr<Coro::FiberTask<void>> task;
    {
      // 与写方(Spawn)同锁,不依赖 bring-up 时序(#98)。
      std::lock_guard<std::mutex> lock(mutex_);
      task = task_;
    }
    if (task) {
      (void)task->get();
    }
  }

  /**
   * @brief 关闭汇合信号:关业务队列(唤醒在途 Pop → 消费者退出)+ 触发 handler 协作取消。
   *
   * 幂等(二者各自幂等)。锁外调用:两个被调件各守其内部状态,本件的 mutex_ 不参与。
   * 队列内**已入队但未启动**的业务不在此清理——留给 `DrainForClose()` 供调用方归因。
   */
  void CancelAndClose() {
    queue_.Close();
    cancellation_.Cancel();
  }

  /// @brief handler 协作取消令牌:`CancelAndClose` 时被触发,node 经 HandlerContext 暴露
  ///        给业务 handler。
  [[nodiscard]] CancellationToken Token() const { return cancellation_.token(); }

  /**
   * @brief Drain 队列内未启动的排队业务(不排空处理),返回其条数。
   *
   * 供收敛者在置 Closed 前逐条归因 `close_drop`——归因由**调用方**做(见文件头"归因
   * 分工"):本件只报"有多少条被丢掉了"。可在任意时刻调用;返回 0 即无残留。
   *
   * @return 被 Drain 掉的事件条数。
   */
  [[nodiscard]] std::size_t DrainForClose() { return queue_.Drain().size(); }

  // —— 观测计数(机制归因,协议无关)——————————————————————————————————————

  /// @brief 业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const {
    return queue_.DroppedCount();  // BoundedQueue 自守其锁。
  }

  /// @brief consume 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return exception_count_;
  }

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。简单存最近值(非直方图,分布分析留 P6)。
  [[nodiscard]] Clock::duration LastHandlerDuration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_duration_;
  }

 private:
  BoundedQueue<Event> queue_;  ///< 入站业务事件队列:生产者 Push、消费者 Pop。
  /// 可选 Trace 出口(非拥有,P5-3/P5-4):队列 kBusinessQueueOverflow 归因 + handler 调用
  /// 起止点 RecordEvent。写一次(构造)不再变,读不需持锁。
  ITraceSink* trace_sink_{nullptr};
  CancellationSource cancellation_;  ///< handler 协作取消源:CancelAndClose 时 Cancel。

  mutable std::mutex mutex_;  ///< 守 fiber 句柄 / 异常计数 / 时长(ADR-0003 D8)。
  /// 消费者 fiber 的结构化并发句柄(ADR-0005 D2);为空即未 Spawn。`Join()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> task_;
  std::size_t exception_count_{0};
  Clock::duration last_duration_{};  ///< P5-4:最近一次 handler 调用时长(简单存最近值)。
};

}  // namespace transport
