#pragma once

/**
 * @file BoundedQueue.hpp
 * @brief 协议无关有界业务队列薄件 BoundedQueue<T>(ADR-0003 D10 / RT_DESIGN_008 /
 *        RT_HANDLER 3.1.5.4 / ADR-0002 D5-D6 / RT_DATA_BUFFER)。
 *
 * BoundedQueue 只做四件事:双上界入队(Push,满即 tail-drop)、协作出队(Pop,空
 * 队列消费者 await 唤醒)、收敛(Close,唤醒在途消费者)、剩余枚举(Drain,供关闭
 * 时 close_drop 归因)。它像 PendingTable 一样独立、可单测、供 P4 DdsNode 复用。
 *
 * 协议无关(D10):头文件不 include Message.hpp / 任何协议类型,T 全程不透明。字节
 * 计量靠构造时注入的 byte_size_of 回调(node 用时传 payload.size()),队列自身不读
 * T 的任何字段。入队时按回调计量并随元素存档,出队按存档字节精确扣减(计量稳定)。
 *
 * 双上界(D5):事件数 max_events 与字节数 max_bytes,任一达到即"满"。满时 Push 拒绝
 * 正到达的元素(tail-drop),返 kResourceExhausted 并经 `RecordDrop`(P5-3)以构造时注入
 * 的 `drop_reason` 归因原地 +1 DroppedCount(+ 可选 Trace)。Push 不阻塞。
 *
 * 归因(P5-3,ADR-0003 D13):`drop_reason`/`sink` 由调用方在构造时注入——本类不知道
 * "为什么是这个原因"(业务队列满 vs DDS 交接满等语义在调用方),只把裸计数换成
 * `RecordDrop` 原语;`DroppedCount()` 语义/签名不变,仍是该原因的累计丢弃数。
 *
 * 同步纪律(D8):表结构(deque / 计数 / waiter 队列)由一把 std::mutex 守;运行时
 * await 只出现在 Pop 的消费者挂起点,唤醒回调(resolve/close)在锁外调用。RecordDrop
 * 在 tail-drop 分支内、持锁调用(与原地 `++dropped` 同一临界区,不引入新的锁外时序)。
 */

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 双上界、协议无关的有界业务队列:FIFO 保序、满即 tail-drop、空即协作等待。
 *
 * @tparam T 业务事件载荷类型;要求可移动构造(每个元素唯一持有)。队列对 T 不透明,
 *           只经构造时注入的 byte_size_of 回调获取其字节计量,不读任何 T 字段
 *           (RT_DESIGN_008 协议可扩展性 / ADR-0003 D10)。
 */
template <typename T>
class BoundedQueue {
 public:
  /// 默认事件数上限(ADR-0002 D5)。
  static constexpr std::size_t kDefaultMaxEvents = 1024;
  /// 事件数上限可配下界。
  static constexpr std::size_t kMinEvents = 1;
  /// 事件数上限可配上界。
  static constexpr std::size_t kMaxEvents = 65536;
  /// 默认字节数上限:16 MiB(ADR-0002 D5)。
  static constexpr std::size_t kDefaultMaxBytes = 16u * 1024u * 1024u;
  /// 字节数上限可配下界:64 KiB。
  static constexpr std::size_t kMinBytes = 64u * 1024u;
  /// 字节数上限可配上界:256 MiB。
  static constexpr std::size_t kMaxBytes = 256u * 1024u * 1024u;

  /// 字节计量回调:入队时对元素求其计量字节(node 传 payload.size())。
  using ByteSizeOf = std::function<std::size_t(const T&)>;

  /**
   * @brief 构造有界队列。
   *
   * @param byte_size_of 字节计量回调(不透明地测 T 的字节数);为空则字节上界永不触发。
   * @param max_events   事件数上限,越界钳制到 [kMinEvents, kMaxEvents](默认 1024)。
   * @param max_bytes    字节数上限,越界钳制到 [kMinBytes, kMaxBytes](默认 16 MiB)。
   * @param drop_reason  tail-drop 时的命名归因(P5-3,ADR-0003 D13 Q3);调用方按其归属
   *                     语义传入(如 `kBusinessQueueOverflow`/`kDdsHandoffOverflow`),
   *                     默认 `kBusinessQueueOverflow`(最常见的调用方,HandlerLoop 业务队列)。
   * @param sink         可选 Trace 出口(RT_TRACE_002:为空时行为/计数不受影响,只少一次
   *                     判空之外的开销)。
   */
  explicit BoundedQueue(ByteSizeOf byte_size_of,
                        std::size_t max_events = kDefaultMaxEvents,
                        std::size_t max_bytes = kDefaultMaxBytes,
                        DropReason drop_reason = DropReason::kBusinessQueueOverflow,
                        ITraceSink* sink = nullptr)
      : shared_(std::make_shared<Shared>()) {
    shared_->byte_size_of = std::move(byte_size_of);
    shared_->max_events = std::min(std::max(max_events, kMinEvents), kMaxEvents);
    shared_->max_bytes = std::min(std::max(max_bytes, kMinBytes), kMaxBytes);
    shared_->drop_reason = drop_reason;
    shared_->sink = sink;
  }

  /**
   * @brief 入队一个元素;不阻塞。
   *
   * 满(事件数达上限 或 追加后字节数超上限)→ tail-drop 拒绝正到达的这个元素,
   * DroppedCount 加一,已入队元素不受影响。入队成功则唤醒一个在途消费者(若有)。
   *
   * @param value 待入队元素(移动进队)。
   * @return 成功返回 Status{};满返 kResourceExhausted;已 Close 返 kClosed。
   */
  [[nodiscard]] Status Push(T value) {
    auto shared = shared_;
    std::shared_ptr<Waiter> waiter;
    {
      std::lock_guard<std::mutex> lock(shared->mutex);
      if (shared->closed) {
        return make_error_code(TransportErrc::kClosed);
      }
      const std::size_t bytes =
          shared->byte_size_of ? shared->byte_size_of(value) : 0;
      if (shared->items.size() >= shared->max_events ||
          shared->byte_size + bytes > shared->max_bytes) {
        // 归因(P5-3):RecordDrop 原地 +1 dropped(DroppedCount 语义不变)+ 可选 Trace。
        RecordDrop(shared->drop_reason, shared->dropped, shared->sink);
        return make_error_code(TransportErrc::kResourceExhausted);
      }
      shared->items.push_back(Item{std::move(value), bytes});
      shared->byte_size += bytes;
      if (!shared->waiters.empty()) {
        waiter = std::move(shared->waiters.front());  // FIFO 唤醒最久等待的消费者。
        shared->waiters.pop_front();
      }
    }
    if (waiter) {  // 唤醒在锁外:消费者 resume 不应在表临界区内运行(D8)。
      waiter->resolve();
      waiter->close();
    }
    return Status{};
  }

  /**
   * @brief 出队一个元素,FIFO;空队列时消费者协作 await 直到 Push 唤醒或收敛。
   *
   * 有元素则立即返回并按存档字节扣减;空且未关闭则挂起等待,被 Push 唤醒后重试。
   * 结果与错误类别:命中元素 → 值;Close → kClosed;deadline 到 → kTimeout;取消令牌
   * 触发 → kCancelled。
   *
   * @param options 截止时间与取消令牌。
   * @return 出队值或机器可判别的错误类别。
   */
  [[nodiscard]] Result<T> Pop(OperationOptions options = {}) {
    auto shared = shared_;
    for (;;) {
      std::shared_ptr<Waiter> waiter;
      {
        std::lock_guard<std::mutex> lock(shared->mutex);
        // Close 先于取元素:关闭后消费循环即终止,残留元素交 Drain 归因 close_drop,
        // 不再经 Pop 交付(RT_HANDLER 3.1.5.4 收敛语义)。
        if (shared->closed) {
          return make_error_code(TransportErrc::kClosed);
        }
        if (!shared->items.empty()) {
          Item item = std::move(shared->items.front());
          shared->items.pop_front();
          shared->byte_size -= item.bytes;  // 按入队存档字节精确扣减。
          return Result<T>(std::move(item.value));
        }
        waiter = std::make_shared<Waiter>();
        shared->waiters.push_back(waiter);
      }

      auto registration = options.cancellation.Register(
          [waiter] { waiter->close(make_error_code(TransportErrc::kCancelled)); });

      Coro::Result<void, std::error_code> notification =
          options.deadline
              ? Coro::await_for(waiter,
                                *options.deadline - OperationOptions::Clock::now())
              : Coro::await(waiter);

      if (!notification &&
          notification.error() == std::make_error_code(std::errc::timed_out)) {
        waiter->close(make_error_code(TransportErrc::kTimeout));
      }
      registration.Reset();
      RemoveWaiter(shared, waiter);

      if (notification) {
        continue;  // 被 Push 唤醒:回到循环取元素(可能被他者抢先则重新等待)。
      }
      if (notification.error() == std::make_error_code(std::errc::timed_out)) {
        return make_error_code(TransportErrc::kTimeout);
      }
      if (notification.error().category() == transport_error_category()) {
        return notification.error();  // kCancelled / kClosed。
      }
      return make_error_code(TransportErrc::kInternal);
    }
  }

  /**
   * @brief latch 关闭:唤醒全部在途消费者(返 kClosed),之后 Push/Pop 拒绝。
   *
   * 不清空已入队元素——留待 Drain 枚举供 node 归因 close_drop。幂等。
   */
  void Close() {
    auto shared = shared_;
    std::deque<std::shared_ptr<Waiter>> waiters;
    {
      std::lock_guard<std::mutex> lock(shared->mutex);
      if (shared->closed) {
        return;
      }
      shared->closed = true;
      waiters.swap(shared->waiters);
    }
    for (auto& waiter : waiters) {  // 锁外唤醒。
      waiter->close(make_error_code(TransportErrc::kClosed));
    }
  }

  /**
   * @brief 取出并清空全部剩余元素(FIFO 序),字节计量归零。
   *
   * 供 node 在 Close 后枚举残留元素以 close_drop 归因;可在任意时刻调用。
   *
   * @return 剩余元素向量(入队顺序)。
   */
  [[nodiscard]] std::vector<T> Drain() {
    auto shared = shared_;
    std::vector<T> out;
    std::lock_guard<std::mutex> lock(shared->mutex);
    out.reserve(shared->items.size());
    for (auto& item : shared->items) {
      out.push_back(std::move(item.value));
    }
    shared->items.clear();
    shared->byte_size = 0;
    return out;
  }

  /// @brief 当前在队事件数。
  [[nodiscard]] std::size_t Size() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->items.size();
  }

  /// @brief 当前在队字节数(按入队存档字节累计)。
  [[nodiscard]] std::size_t ByteSize() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->byte_size;
  }

  /// @brief 累计 tail-drop 丢弃数(命名归因见构造时注入的 `drop_reason`)。
  [[nodiscard]] std::size_t DroppedCount() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->dropped;
  }

 private:
  using Waiter = Coro::Awaitable<void>;

  /// 在队元素:载荷 + 入队时存档的字节计量(出队按此精确扣减)。
  struct Item {
    T value;
    std::size_t bytes;
  };

  /// 队列共享状态:deque + 计数 + waiter 队列 + closed latch,由一把 mutex 守(D8)。
  struct Shared {
    mutable std::mutex mutex;
    std::deque<Item> items;
    std::deque<std::shared_ptr<Waiter>> waiters;
    std::size_t byte_size{0};
    std::size_t dropped{0};
    std::size_t max_events{kDefaultMaxEvents};
    std::size_t max_bytes{kDefaultMaxBytes};
    bool closed{false};
    ByteSizeOf byte_size_of;
    DropReason drop_reason{DropReason::kBusinessQueueOverflow};  ///< tail-drop 命名归因(P5-3)。
    ITraceSink* sink{nullptr};  ///< 可选 Trace 出口(非拥有,P5-3)。
  };

  /// 从 waiter 队列按身份摘除本消费者的 waiter(超时/取消/唤醒后收尾)。幂等。
  static void RemoveWaiter(const std::shared_ptr<Shared>& shared,
                           const std::shared_ptr<Waiter>& waiter) {
    std::lock_guard<std::mutex> lock(shared->mutex);
    for (auto it = shared->waiters.begin(); it != shared->waiters.end(); ++it) {
      if (*it == waiter) {
        shared->waiters.erase(it);
        return;
      }
    }
  }

  std::shared_ptr<Shared> shared_;
};

}  // namespace transport
