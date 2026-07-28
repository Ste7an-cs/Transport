#pragma once

/**
 * @file PendingTable.hpp
 * @brief 纯挂起-应答薄基座 PendingTable<Key, T>(RT_IN_INTERFACE_004 / ADR-0001 D2)。
 *
 * PendingTable 只做四件事:唯一登记(Register)、恰好一次完成(Resolve)、全部收敛
 * (FailAll)、取消纪律(Handle 析构兜底)。它不 decode、不算 key、不判终结、不跑读
 * 循环——那些内联在 node(P1-T3)。可独立于 transport/node 用 Fake 契约单测验证。
 *
 * 仲裁:每个在途 entry 复用一个 SharedCompletion<T> 作值信箱,取其原子首胜 Complete。
 * 四方(Resolve / 超时 / 取消 / FailAll)抢同一个 Complete → 恰好一次(RT_REQUEST_003)。
 * 表结构(map 的 find/insert/erase)是 std::mutex 守的同步临界区(ADR-0003 D8);运行时
 * await 只出现在 Handle::Wait 的挂起点。
 */

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <system_error>
#include <utility>
#include <vector>

#include "transport/Error.hpp"
#include "transport/Result.hpp"
#include "transport/SharedCompletion.hpp"
#include "transport/TransportTypes.hpp"

namespace transport {

/**
 * @brief 挂起-应答登记表:唯一登记的在途请求与其应答信箱的薄基座。
 *
 * @tparam Key 关联键;要求可拷贝且 LessThanComparable(内部以 std::map 索引)。P1 实例化为
 *             uint32;模板保留供 P4 DDS correlation_id string 键复用。自定义协议键若非天然
 *             有序,需自备 operator< 或比较器(RT_DESIGN_008 协议可扩展性)。
 * @tparam T   应答载荷类型;要求可拷贝(每个等待者持有独立 Result)。P1 实例化为 Message。
 */
template <typename Key, typename T>
class PendingTable {
 private:
  /// 单个在途 entry:仅含其值信箱。以 shared_ptr 存,身份用于摘除时的精确匹配。
  struct Entry {
    SharedCompletion<T> completion;
  };

  /// 表的共享状态:map + closed latch,由一把 mutex 守。Handle 与表共享它。
  struct Shared {
    std::mutex mutex;
    std::map<Key, std::shared_ptr<Entry>> entries;
    bool closed{false};
  };

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
          entry_(std::move(other.entry_)),
          finalized_(other.finalized_) {
      other.entry_.reset();
    }

    Handle& operator=(Handle&& other) noexcept {
      if (this != &other) {
        Evict();
        shared_ = std::move(other.shared_);
        key_ = std::move(other.key_);
        entry_ = std::move(other.entry_);
        finalized_ = other.finalized_;
        other.entry_.reset();
      }
      return *this;
    }

    /// 析构兜底:未终结则从表摘除该 key(取消纪律)。
    ~Handle() { Evict(); }

    /**
     * @brief 在该 entry 上等待应答,四方仲裁在此收敛(RT_REQUEST_002/003/004)。
     *
     * 结果与错误类别:resolve 命中 → 值;deadline 到 → kTimeout;cancel 触发 →
     * kCancelled;FailAll → 其传入 error(kConnection / kClosed)。超时/取消也穿过
     * entry 的 Complete 把 entry 置终结(修 SharedCompletion::Wait 本地超时不置终结的
     * 缺口),故随后 Resolve 必失败。返回前从表摘除该 entry。
     *
     * @param options 截止时间与取消令牌。
     * @return 应答值或机器可判别的错误类别。
     */
    [[nodiscard]] Result<T> Wait(OperationOptions options = {}) {
      if (!entry_) {
        return make_error_code(TransportErrc::kInvalidState);
      }
      Result<T> outcome = entry_->completion.Wait(std::move(options));
      if (!outcome) {
        // 超时/取消胜出时先让 entry 的完成原语 Complete(该 error) 置终结,再摘除;
        // 若已被他方(Resolve / FailAll)先胜,则采用那个真正的终结结果。
        if (!entry_->completion.Complete(Result<T>(outcome.error()))) {
          outcome = entry_->completion.Wait();
        }
      }
      Evict();
      finalized_ = true;
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

    /// 从表摘除本句柄的 entry(身份匹配,避免误删同 key 的新登记)。幂等。
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
    bool finalized_{false};
  };

  /**
   * @brief 构造挂起-应答表。
   *
   * @param max_pending 在途 entry 的可选纯计数上限(协议无关,RT_DESIGN_008):0 = 无限;
   *                    >0 时 Register 在 Size() 已达上限时返 kResourceExhausted。此上限只
   *                    数 entry、不碰键语义,协议特有的容量语义(如 session_id 空间)由
   *                    调用方 node 另行内联(ADR-0003 D10)。
   */
  explicit PendingTable(std::size_t max_pending = 0)
      : shared_(std::make_shared<Shared>()), max_pending_(max_pending) {}

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
    auto entry = std::make_shared<Entry>();
    shared_->entries.emplace(key, entry);
    return Handle(shared_, key, std::move(entry));
  }

  /**
   * @brief 向某 key 的在途 entry 交付应答并终结(原子首胜)。
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
        return false;
      }
      entry = it->second;
    }
    // Complete 在锁外:其唤醒等待者的回调不应在表临界区内运行。
    if (!entry->completion.Complete(Result<T>(std::move(value)))) {
      return false;  // 已被他方终结(迟到 / 重复)。
    }
    {
      std::lock_guard<std::mutex> lock(shared_->mutex);
      auto it = shared_->entries.find(key);
      if (it != shared_->entries.end() && it->second == entry) {
        shared_->entries.erase(it);
      }
    }
    return true;
  }

  /**
   * @brief 全部在途 entry 恰好一次以 error 终结,并 latch closed。
   *
   * 之后 Register 返 kClosed(堵幽灵在途)。已被超时/取消先终结的 entry 保持其原有结果
   * (Complete 首胜),FailAll 对其为 no-op。
   *
   * @param error 终结用错误类别(典型 kConnection / kClosed)。
   */
  void FailAll(std::error_code error) {
    std::vector<std::shared_ptr<Entry>> victims;
    {
      std::lock_guard<std::mutex> lock(shared_->mutex);
      shared_->closed = true;
      victims.reserve(shared_->entries.size());
      for (auto& item : shared_->entries) {
        victims.push_back(item.second);
      }
      shared_->entries.clear();
    }
    for (auto& entry : victims) {
      entry->completion.Complete(Result<T>(error));
    }
  }

  /// @brief 当前在途 entry 数。
  [[nodiscard]] std::size_t Size() const {
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->entries.size();
  }

 private:
  std::shared_ptr<Shared> shared_;
  std::size_t max_pending_{0};  ///< 在途 entry 计数上限;0 = 无限(协议无关)。
};

}  // namespace transport
