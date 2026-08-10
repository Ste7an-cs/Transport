#pragma once

/**
 * @file PendingTable.hpp
 * @brief 纯挂起-应答薄基座 PendingTable<Key, T>(RT_IN_INTERFACE_004 / ADR-0001 D2)。
 *
 * PendingTable 只做四件事:唯一登记(Register)、恰好一次完成(Resolve)、全部收敛
 * (FailAll)、取消纪律(Handle 析构兜底)。它不 decode、不算 key、不判终结、不跑读
 * 循环——那些内联在 node(P1-T3)。可独立于 transport/node 用 Fake 契约单测验证。
 *
 * 仲裁(单一点):每个在途 entry 只有一个等待者(当前 (session_id,message_id) /
 * correlation_id 键空间下不需同 key 并发多待,ADR-0001)。四方(Resolve / 超时 / 取消 /
 * FailAll)统一经**表锁内 find+erase 抢占终结权**——谁先摘除该 entry 谁胜(唯一仲裁点,
 * 恰好一次,RT_REQUEST_003);entry 的信箱退化为一个裸的 `Coro::Awaitable<T>`(底层一条
 * FiberChannel),只承载"唤醒 + 带值/带错",不再自持仲裁锁。胜方在锁外对信箱 push 值 +
 * close(Resolve)或 close(error)(超时/取消/FailAll);消费者 `Handle::Wait` 在信箱上
 * `await`,`Awaitable` 的"关闭后先取尽已入队值再报 close_error"语义天然裁决"值抢在 close
 * 前落地"的竞态。表结构(map find/erase)是 std::mutex 守的同步临界区(ADR-0003 D8);
 * 运行时 await 只出现在 Handle::Wait 的信箱挂起点。
 *
 * 简化沿革(#98 后续):此前每个 entry 背一个 `SharedCompletion<T>`(自带第二把 mutex +
 * 多等待者 waiter-map + 广播)。单等待者场景用不到多等待者机制;改用裸 `Awaitable<T>` +
 * 表锁作唯一仲裁点后,去掉该层。`SharedCompletion` 仍供 NodeBase 等多等待者 void 事件用。
 */

#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"  // Coro::Awaitable —— entry 信箱(单等待者一次性 channel)
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TraceCategories.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 挂起-应答登记表:唯一登记的在途请求与其应答信箱的薄基座。
 *
 * @tparam Key 关联键;要求可拷贝且 LessThanComparable(内部以 std::map 索引)。P1 实例化为
 *             uint32;模板保留供 P4 DDS correlation_id string 键复用。自定义协议键若非天然
 *             有序,需自备 operator< 或比较器(RT_DESIGN_008 协议可扩展性)。
 * @tparam T   应答载荷类型;要求**可默认构造且可拷贝**(`Coro::Awaitable<T>` 的信箱语义:
 *             出队处 `T value{}` + 生产侧按值投递)。P1/P4 实例化为 Message(二者皆满足)。
 */
template <typename Key, typename T>
class PendingTable {
 public:
  using Clock = OperationOptions::Clock;

 private:
  /// 单个在途 entry:一次性信箱(裸 Awaitable)+ 登记时刻(供终结时计请求时延,P5-4)。
  struct Entry {
    std::shared_ptr<Coro::Awaitable<T>> mailbox{
        std::make_shared<Coro::Awaitable<T>>()};
    Clock::time_point registered_at{};
  };

  /// 表的共享状态:map + closed latch + 可选 Trace 出口 + 最近请求时延,由一把 mutex 守。
  /// Handle 与表共享它。
  struct Shared {
    std::mutex mutex;
    std::map<Key, std::shared_ptr<Entry>> entries;
    bool closed{false};
    ITraceSink* sink{nullptr};             ///< 可选 Trace 出口(P5-4);为空则 RT_TRACE_002。
    Clock::duration last_request_latency{};  ///< 最近一次终结请求的时延(简单存最近值)。
  };

  /// @brief 抢占 (key,entry) 的终结权:本 entry 仍在表则摘除并返回 true(本方胜,唯一仲裁点)。
  ///        自持表锁;Resolve/超时/取消/FailAll 皆经此裁决"谁先终结"。
  static bool ClaimTerminal(const std::shared_ptr<Shared>& shared, const Key& key,
                            const std::shared_ptr<Entry>& entry) {
    std::lock_guard<std::mutex> lock(shared->mutex);
    auto it = shared->entries.find(key);
    if (it != shared->entries.end() && it->second == entry) {
      shared->entries.erase(it);
      return true;
    }
    return false;
  }

 public:
  /**
   * @brief 登记句柄:持有一个在途 entry,在其上等待应答;析构兜底摘除未终结 entry。
   *
   * 移动语义(不可拷贝):句柄唯一持有该 entry 的等待权。
   */
  class Handle {
   public:
    Handle() = default;
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;

    Handle(Handle&& other) noexcept
        : shared_(std::move(other.shared_)),
          key_(std::move(other.key_)),
          entry_(std::move(other.entry_)) {
      other.entry_.reset();
    }

    Handle& operator=(Handle&& other) noexcept {
      if (this != &other) {
        Evict();
        shared_ = std::move(other.shared_);
        key_ = std::move(other.key_);
        entry_ = std::move(other.entry_);
        other.entry_.reset();
      }
      return *this;
    }

    /// 析构兜底:未终结则从表摘除该 key(取消纪律)。
    ~Handle() { Evict(); }

    /**
     * @brief 在该 entry 的信箱上等待应答,四方仲裁在此收敛(RT_REQUEST_002/003/004)。
     *
     * 结果与错误类别:Resolve 命中 → 值;deadline 到 → kTimeout;cancel 触发 →
     * kCancelled;FailAll → 其传入 error(kConnection / kClosed)。仲裁经"表锁内抢占
     * (find+erase)"裁决:
     *  - 信箱收到值 → Resolve 抢先并 push,直接取值;
     *  - 本地超时(await_for 报 timed_out)→ 抢占终结:抢到即 kTimeout;抢输(他方已在途)
     *    则在信箱上 `await` 一次 drain 出对方的值/错误(信箱"关闭后先取尽值再报错"语义);
     *  - 信箱被 close(error) 唤醒 → cancel/FailAll 已抢占,采其 error。
     * 终结后 entry 恒已被抢占方从表摘除,本函数只需复位句柄。
     *
     * @param options 截止时间与取消令牌。
     * @return 应答值或机器可判别的错误类别。
     */
    [[nodiscard]] Result<T> Wait(OperationOptions options = {}) {
      if (!entry_) {
        return make_error_code(TransportErrc::kInvalidState);
      }
      const std::shared_ptr<Shared> shared = shared_;
      const Key key = key_;
      const std::shared_ptr<Entry> entry = entry_;
      const std::shared_ptr<Coro::Awaitable<T>> mailbox = entry_->mailbox;

      // 取消:令牌触发 → 抢占终结后 close(kCancelled) 唤醒本等待者。
      auto registration = options.cancellation.Register([shared, key, entry] {
        if (ClaimTerminal(shared, key, entry)) {
          entry->mailbox->close(make_error_code(TransportErrc::kCancelled));
        }
      });

      Result<T> notification =
          options.deadline
              ? Coro::await_for(*mailbox, *options.deadline - Clock::now())
              : Coro::await(*mailbox);
      registration.Reset();

      Result<T> outcome = make_error_code(TransportErrc::kInternal);
      if (notification) {
        outcome = std::move(notification);  // Resolve 抢先并 push 了值。
      } else if (notification.error() ==
                 std::make_error_code(std::errc::timed_out)) {
        // 本地超时:抢占终结(唯一仲裁点)。
        if (ClaimTerminal(shared, key, entry)) {
          mailbox->close(make_error_code(TransportErrc::kTimeout));
          outcome = make_error_code(TransportErrc::kTimeout);
        } else {
          // 抢输:Resolve/cancel/FailAll 已在途,drain 出其值/错误(信箱先取尽值再报错)。
          outcome = mailbox->await();
        }
      } else {
        // 信箱 close(error):cancel(kCancelled)/FailAll(kConnection/kClosed) 已抢占并摘除。
        outcome = std::move(notification);
      }

      // 终结点(P5-4,RT_DATA_BUFFER 请求时延):记 Register→本次终结耗时(简单存最近值,
      // 分布分析留 P6)+ 按结果分类 Trace——match(成功)/timeout/cancel 三类
      // (RT_REQUEST_003 四类终结之二 + 成功一类);第四类 FailAll(kConnection/kClosed)
      // 不在本三类之列,不发 Trace,但仍计入时延。
      const Clock::duration latency = Clock::now() - entry->registered_at;
      ITraceSink* sink = nullptr;
      {
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->last_request_latency = latency;
        sink = shared->sink;
      }
      std::string_view category;
      if (outcome) {
        category = kTraceCategoryMatch;
      } else if (outcome.error() == make_error_code(TransportErrc::kTimeout)) {
        category = kTraceCategoryTimeout;
      } else if (outcome.error() == make_error_code(TransportErrc::kCancelled)) {
        category = kTraceCategoryCancel;
      }
      if (!category.empty()) {
        RecordEvent(category, sink, /*message=*/{}, /*key=*/{}, /*endpoint=*/{},
                    /*error=*/{},
                    static_cast<long>(std::chrono::duration_cast<
                                       std::chrono::microseconds>(latency)
                                           .count()));
      }
      entry_.reset();  // 句柄已消费(entry 已被终结方摘除;~Handle 的 Evict 成空操作)。
      return outcome;
    }

    /// @brief 句柄是否仍持有一个在途 entry。
    [[nodiscard]] explicit operator bool() const noexcept {
      return entry_ != nullptr;
    }

   private:
    friend class PendingTable;

    Handle(std::shared_ptr<Shared> shared, Key key, std::shared_ptr<Entry> entry)
        : shared_(std::move(shared)), key_(std::move(key)), entry_(std::move(entry)) {}

    /// 从表摘除本句柄的 entry(身份匹配,避免误删同 key 的新登记)。幂等。未 Wait 即析构
    /// (取消纪律)时经此摘除;已 Wait 消费后 entry_ 为空,直接返回。
    void Evict() noexcept {
      if (!entry_ || !shared_) {
        return;
      }
      std::lock_guard<std::mutex> lock(shared_->mutex);
      auto it = shared_->entries.find(key_);
      if (it != shared_->entries.end() && it->second == entry_) {
        shared_->entries.erase(it);
      }
      entry_.reset();
    }

    std::shared_ptr<Shared> shared_;
    Key key_{};
    std::shared_ptr<Entry> entry_;
  };

  /**
   * @brief 构造挂起-应答表。
   *
   * @param max_pending 在途 entry 的可选纯计数上限(协议无关,RT_DESIGN_008):0 = 无限;
   *                    >0 时 Register 在 Size() 已达上限时返 kResourceExhausted。此上限只
   *                    数 entry、不碰键语义,协议特有的容量语义(如 session_id 空间)由
   *                    调用方 node 另行内联(ADR-0003 D10)。
   * @param sink        可选 Trace 出口(P5-4);为空则 `Handle::Wait` 终结点的
   *                    `RecordEvent` 仅一次判空(RT_TRACE_002)。
   */
  explicit PendingTable(std::size_t max_pending = 0, ITraceSink* sink = nullptr)
      : shared_(std::make_shared<Shared>()), max_pending_(max_pending) {
    shared_->sink = sink;
  }

  /**
   * @brief 唯一登记一个在途请求。
   *
   * @param key 关联键。
   * @return 成功返回持有该 entry 的 Handle;key 已在途返 kInvalidState(不发送);
   *         在途数已达 max_pending 返 kResourceExhausted(不发送);FailAll 已 latch
   *         closed 返 kClosed(堵幽灵在途)。
   */
  [[nodiscard]] Result<Handle> Register(const Key& key) {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    if (shared_->closed) {
      return make_error_code(TransportErrc::kClosed);
    }
    if (max_pending_ != 0 && shared_->entries.size() >= max_pending_) {
      return make_error_code(TransportErrc::kResourceExhausted);
    }
    if (shared_->entries.find(key) != shared_->entries.end()) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    auto entry = std::make_shared<Entry>();  // 信箱在成员初值处自动创建。
    entry->registered_at = Clock::now();
    shared_->entries.emplace(key, entry);
    return Handle(shared_, key, std::move(entry));
  }

  /**
   * @brief 向某 key 的在途 entry 交付应答并终结(抢占仲裁,恰好一次)。
   *
   * 表锁内 find+erase 抢占终结权:抢到 → 锁外对信箱 push 值 + close(消费者 await 先取值
   * 再见 close_error);未找到(他方已终结 / 无此 key)→ 返 false。
   *
   * @param key   关联键。
   * @param value 应答载荷。
   * @return 命中未终结 entry 并交付返 true;无 entry / 已终结返 false(调用方据此归因
   *         迟到 / 重复 / 无匹配,RT_REQUEST_004)。
   */
  bool Resolve(const Key& key, T value) {
    std::shared_ptr<Entry> entry;
    {
      std::lock_guard<std::mutex> lock(shared_->mutex);
      auto it = shared_->entries.find(key);
      if (it == shared_->entries.end()) {
        return false;  // 无 entry / 已被他方抢占终结(迟到 / 重复 / 无匹配)。
      }
      entry = it->second;
      shared_->entries.erase(it);  // 抢占终结(唯一仲裁点)。
    }
    // 锁外投递:push 值不应在表临界区内唤醒等待者(ADR-0003 D8)。
    entry->mailbox->resolve(value);
    entry->mailbox->close();  // 锁存:消费者 await 先取尽该值,再见 close_error。
    return true;
  }

  /**
   * @brief 全部在途 entry 恰好一次以 error 终结;可选是否 latch closed。
   *
   * 表锁内摘除全部在途 entry(抢占终结),锁外逐个 `mailbox->close(error)`。已被超时/取消/
   * Resolve 先抢占摘除的 entry 不在本批,故不受影响(恰好一次)。
   *
   * @param error 终结用错误类别(典型 kConnection / kClosed)。
   * @param latch_closed 是否 latch closed:
   *        - true(默认,Close 语义):之后 Register 返 kClosed(堵幽灵在途),表永久收敛。
   *        - false(连接断连语义,ADR-0003 D11 / RT_TCP_RECONNECT_002):只清空当前在途、
   *          不 latch,表继续可用——新连接代际的请求仍可 Register。靠"FailAll 清空 ⇒ 在途
   *          恒属当前代际"不变式隔离代际,连接概念不下沉本协议无关基座(守 RT_DESIGN_008)。
   *          绝不 un-latch:若表已被 latch(Close),本调用不会重开。
   */
  void FailAll(std::error_code error, bool latch_closed = true) {
    std::vector<std::shared_ptr<Entry>> victims;
    {
      std::lock_guard<std::mutex> lock(shared_->mutex);
      if (latch_closed) {
        shared_->closed = true;  // 只单向 latch;false 时保持原 closed 值(不 un-latch)。
      }
      victims.reserve(shared_->entries.size());
      for (auto& item : shared_->entries) {
        victims.push_back(item.second);
      }
      shared_->entries.clear();  // 抢占全部在途终结。
    }
    for (auto& entry : victims) {
      entry->mailbox->close(error);  // 首次 close 胜出;唤醒该 entry 的等待者。
    }
  }

  /// @brief 当前在途 entry 数。
  [[nodiscard]] std::size_t Size() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->entries.size();
  }

  /// @brief 观测:最近一次终结请求的时延(Register→终结,P5-4,RT_DATA_BUFFER)。尚无
  ///        已终结请求时为 0。简单存最近值(非直方图,分布分析留 P6)。
  [[nodiscard]] Clock::duration LastRequestLatency() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->last_request_latency;
  }

 private:
  std::shared_ptr<Shared> shared_;
  std::size_t max_pending_{0};  ///< 在途 entry 计数上限;0 = 无限(协议无关)。
};

}  // namespace transport
