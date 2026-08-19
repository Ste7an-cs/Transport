#pragma once

/**
 * @file HandlerLoop.hpp
 * @brief 协议无关的 handler 消费者小件 HandlerLoop<Event>(RT_HANDLER_003 /
 *        RT_HANDLER_006 / RT_DESIGN_008)。
 *
 * HandlerLoop 把"入站业务事件的单消费者串行处理"收成一个独立小件,拥有该职责所需的
 * **全部**状态:
 *
 *   - `Coro::Awaitable<Event>` —— 入站业务队列。**直接用 AsyncTask 的队列**,不再自造:
 *     `FiberChannel` 本身就是"有界 FIFO + 空则消费者协作等待 + `close(error)` 唤醒在途
 *     消费者",这正是本件需要的全部;
 *   - `Coro::FiberTask<void>` 句柄 —— 消费者 fiber 的结构化并发句柄,供收敛者让出式
 *     join;
 *   - `CancellationSource` —— handler 协作取消源,关闭时触发,令牌经 node 的
 *     HandlerContext 交到业务 handler 手上;
 *   - 逃逸异常隔离(RT_HANDLER_006)。
 *
 * **它是可选件**:未设 handler 的节点根本没有消费者 fiber——故它由 node 持有,不进节点
 * 基类。未 `Spawn` 时本对象仍可安全地被 `Enqueue`/`Join`/`CancelAndClose`(分别为:
 * 入队后无人消费、无可汇合者立即返回、只关队列)。
 *
 * **协议无关**(RT_DESIGN_008 / ADR-0003 D10):头文件不 include 任何协议类型,Event 全程
 * 不透明。
 *
 * @warning **队列满的行为是"丢最旧"且静默**(`FiberChannel` 的语义:超过容量时 `push`
 *          丢弃队首最旧的值,不阻塞、不失败、无任何提示)。这与旧 `BoundedQueue` 的
 *          tail-drop(拒绝正到达的元素并归因计数)不同,且**丢弃不可观测**——AsyncTask
 *          不提供丢弃计数,容量是唯一的调节手段。字节数上界也随之消失,只剩事件数。
 *          见 #152。
 *
 * 同步纪律(ADR-0003 D8):消费者 fiber 句柄由一把 std::mutex 守;队列与取消源各自守其
 * 内部状态。**运行时 await 只出现在 fiber 体内的挂起点**——`consume` 一律在锁外调用
 * (其内部可挂起、可回调进 node,持锁调用会与生命周期锁交叉)。
 */

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include "await/awaitable.hpp"
#include "task/fibertask.h"  // Coro::makeTask / Coro::FiberTask —— handler 消费者 fiber。
#include "detail/result.hpp"

#include "transport/core/Cancellation.hpp"
#include "transport/core/Error.hpp"

namespace transport {

/**
 * @brief 单消费者 handler 循环:业务队列 + 消费者 fiber + 协作取消 + 异常隔离。
 *
 * @tparam Event 入站业务事件载荷类型(node 传 Message);本件对其不透明。
 *
 * 不可拷贝、不可移动(消费者 fiber 捕获 this,且持 std::mutex)。
 */
template <typename Event>
class HandlerLoop {
 public:
  /// 业务队列默认容量(事件数)。沿用 AsyncTask `FiberChannel` 的默认值。
  static constexpr std::uint32_t kDefaultCapacity = 1024;

  /**
   * @brief 构造 handler 循环(仅建队列;消费者 fiber 待 `Spawn` 时才起)。
   *
   * @param capacity 业务队列容量(事件数);0 = 无上限(自行保证消费速度)。超限时
   *                 `push` **静默丢弃队首最旧的值**——见类文档的 @warning。
   */
  explicit HandlerLoop(std::uint32_t capacity = kDefaultCapacity) {
    queue_->setCapacity(capacity);
  }

  HandlerLoop(const HandlerLoop&) = delete;
  HandlerLoop& operator=(const HandlerLoop&) = delete;

  /**
   * @brief spawn 单消费者 fiber(串行消费业务队列):出队一条 → 跑 @p consume 到完成
   *        (含其 await)→ 再出下一条(严格串行,RT_HANDLER_003)。
   *
   * consume 逃逸异常被边界兜住 → 隔离当前事件、不自关 node、继续下一条
   * (RT_HANDLER_006);这是本件唯一授权的 catch。队列被 `close` → 消费者退出。
   *
   * **保留 `FiberTask` 句柄供收敛者结构化 join**:`Join()` 以 `FiberTask::get()` 等
   * handler 退出,不再手写完成量——完成由运行时保证,避免"漏 Complete → 收敛永久挂死"。
   */
  void Spawn(std::function<void(Event&&)> consume) {
    auto body = [this, consume = std::move(consume)]() mutable {
      for (;;) {
        Coro::Result<Event, std::error_code> item = Coro::await(queue_);
        if (!item) {
          break;  // 队列被 close(CancelAndClose)→ 消费者退出。
        }
        try {
          consume(std::move(item).value());  // 出队一条跑完再出下一条 = 严格串行。
        } catch (...) {
          // RT_HANDLER_006:边界兜住逃逸异常 → 隔离本条事件,继续下一条。
        }
      }
    };
    // 句柄登记先于 fiber 首次得以运行:makeTask 的 fiber 与本调用同线程亲和
    // (Affinity::fixed(当前线程)),而至此无挂起点,故收敛者不可能观察到"已 spawn 但
    // 句柄未登记"。同锁发布保留:不依赖该时序做跨线程可见性保证。
    auto task =
        std::make_shared<Coro::FiberTask<void>>(Coro::makeTask(std::move(body)));
    std::lock_guard<std::mutex> lock(mutex_);
    task_ = std::move(task);
  }

  /// @brief 入站业务事件入队(不阻塞);读循环分发业务帧时调。
  /// @return 已入队;队列已 close 返 kClosed。**注意**:队列满时返回的仍是成功——满是
  ///         靠丢弃队首最旧的值来消化的,不体现在返回值里(见类文档 @warning)。
  [[nodiscard]] Coro::Result<void> Enqueue(Event event) {
    if (queue_->channel()->push(std::move(event)) !=
        boost::fibers::channel_op_status::success) {
      return make_error_code(TransportErrc::kClosed);
    }
    return Coro::Result<void>{};
  }

  /**
   * @brief 让出式 join 消费者 fiber:等其**实际退出**,不强杀(RT_LIFECYCLE_006)。
   *
   * 未 `Spawn`(句柄为空)⇒ 无消费者可汇合,立即返回。`get()` 在调用者 fiber 内让出,直至
   * 消费者协作返回;其返回值仅表征任务是否正常完成,调用方不据此分支——无论哪条路径,
   * `get()` 返回即意味着消费者 fiber 已不再运行。
   */
  void Join() {
    std::shared_ptr<Coro::FiberTask<void>> task;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      task = task_;
    }
    if (task) {
      (void)task->get();
    }
  }

  /**
   * @brief 关闭汇合信号:关业务队列(唤醒在途消费者 → 其退出)+ 触发 handler 协作取消。
   *
   * 幂等。锁外调用:两个被调件各守其内部状态,本件的 mutex_ 不参与。队列内**已入队但
   * 未启动**的业务随之丢弃——关闭即停止交付。
   */
  void CancelAndClose() {
    queue_->close(make_error_code(TransportErrc::kClosed));
    queue_->channel()->discard_pending();
    cancellation_.Cancel();
  }

  /// @brief handler 协作取消令牌:`CancelAndClose` 时被触发,node 经 HandlerContext 暴露
  ///        给业务 handler。
  [[nodiscard]] CancellationToken Token() const { return cancellation_.token(); }

 private:
  /// 入站业务事件队列:生产者 push、消费者 await。用 AsyncTask 的队列,不自造。
  std::shared_ptr<Coro::Awaitable<Event>> queue_{
      std::make_shared<Coro::Awaitable<Event>>()};
  CancellationSource cancellation_;  ///< handler 协作取消源:CancelAndClose 时 Cancel。

  mutable std::mutex mutex_;  ///< 守 fiber 句柄(ADR-0003 D8)。
  /// 消费者 fiber 的结构化并发句柄;为空即未 Spawn。`Join()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> task_;
};

}  // namespace transport
