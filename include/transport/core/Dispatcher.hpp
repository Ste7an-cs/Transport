#pragma once

/**
 * @file Dispatcher.hpp
 * @brief 按键分配的消息路由器 Dispatcher<T, Fields...>(协议无关)。
 *
 * 本件负责入站消息与等待者之间的关联:订阅者按键登记一个信箱,消息到达时由本件按键定位
 * 并投递,等待者在信箱上以协程方式取值。
 *
 * ## 使用
 *
 * 构造时提供**键提取函数**,给出一条消息各匹配字段的具体值:
 *
 * ```cpp
 * Dispatcher<Message, std::uint8_t, std::uint16_t, FrameType> d(
 *     [](const Message& m) {
 *       return std::make_tuple(m.session_id, m.message_id, m.frm_type);
 *     });
 * ```
 *
 * 订阅时逐字段给出约束,不参与匹配的字段填 `kAny`:
 *
 * ```cpp
 * auto mine  = d.Subscribe({7,    10,   FrameType::kResponse});  // 三字段全约束
 * auto audit = d.Subscribe({kAny, kAny, FrameType::kResult});    // 仅约束帧类型
 * auto reply = mine.Wait(std::chrono::seconds(1));               // 让出式等待
 * ```
 *
 * 部分匹配由本件实现,调用方无需提供通配逻辑、哨兵值或字段组合枚举。
 *
 * `kAny` 表示"该字段不参与匹配",与"该字段须等于 0"是两种不同的约束,二者以
 * `std::optional` 的持值状态区分而不占用字段值域;因此值域已被占满的字段(如
 * `std::uint8_t` 的 0..255)同样可以通配。
 *
 * ## 投递语义
 *
 * 一条消息投递给**全部**键匹配的订阅者,每人一份副本。不同粒度的订阅(精确等待与旁路
 * 监听)可同时命中同一条消息,彼此不构成竞争。
 *
 * 本件不提供"独占投递"模式:投递唯一性应由协议在键的设计上保证(例如以会话标识区分并发
 * 交互)。单个订阅只关联一个键,故同一订阅者不会因一条消息收到多份。
 *
 * ## 复杂度
 *
 * 单条消息的投递成本为 O(在用 mask 种数 + 收件人数),与订阅者总数无关。内部按"哪些字段
 * 被约束"(mask)分层索引,投递时仅探测存在订阅的 mask,既不枚举 2ⁿ 种字段组合,也不逐个
 * 求值谓词。在用 mask 的种数由订阅行为决定,无需声明。
 *
 * ## 线程安全(ADR-0016)
 *
 * 本件**跨线程、跨 fiber 安全**:`Subscribe` / `Dispatch` / `CloseAll` / `~Ticket` 可从任意
 * 线程、任意 fiber 并发调用。索引由 `State` 内的 `boost::fibers::mutex` 保护——取的是 fiber
 * 版互斥量而非 `std::mutex`:等待时让出 fiber 而不阻塞线程,故同线程的其他 fiber 不受牵连。
 *
 * **临界区内不投递**:`Dispatch` 与 `CloseAll` 在锁内只做索引快照(纯内存操作、无挂起点),
 * `resolve` / `close` 一律在锁外执行——二者会取信箱内部的 fiber 互斥量、可能让出,放在锁内
 * 既拖长索引的持锁期,又会在被唤醒方回调本件时自锁(`fibers::mutex` 不可重入)。
 *
 * 由此带来一个**刻意的窗口**:快照与投递之间注销的凭据,仍可能收到最后一条消息。快照持
 * `shared_ptr<Awaitable>`,信箱必然存活,故不构成悬垂——只是投进了无人再读的信箱。
 *
 * ## 约束
 *
 * - `T` 须可拷贝(多订阅者各得一份副本);`Fields...` 须可默认构造、可哈希、可相等比较。
 * - **键提取函数内不得回调本件**:它在锁外求值,但重入路径会撞上同一把不可重入的互斥量。
 * - `~Ticket` / `Ticket::Reset()` 会取锁,故**可能让出 fiber**(此前无挂起点)。持锁期只有
 *   一次索引摘除,不含挂起点,故等待必然有界;但析构期间让出意味着别的 fiber 可能观察到
 *   半析构的宿主对象,宿主须自行留意。
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/fiber/mutex.hpp>

#include "await/awaitable.hpp"
#include "detail/result.hpp"
#include "transport/core/Error.hpp"

namespace transport {

/**
 * @brief 通配标记:标注该字段不参与匹配。
 *
 * 采用具名标记而非空初值 `{}`,一是使调用点自解释,二是使调用方与字段的内部表示解耦——
 * 更换 `Pattern` 的存储形式不影响 `kAny` 的用法。
 */
struct AnyTag {
  template <typename T>
  constexpr operator std::optional<T>() const {  // NOLINT(google-explicit-constructor)
    return std::nullopt;
  }
};
inline constexpr AnyTag kAny{};

/**
 * @brief 订阅模式:逐字段给出具体值或 `kAny`。
 *
 * 不直接使用 `std::tuple<std::optional<Fields>...>`:该模板存在
 * `tuple(allocator_arg_t, ...)` 重载,当首字段以 `{}` 或 `kAny` 初始化时会优先匹配到该
 * 重载并导致编译失败。本包装同时承载 mask 推导与哈希所需的成员。
 */
template <typename... Fields>
struct Pattern {
  std::tuple<std::optional<Fields>...> fields;

  Pattern() = default;
  // NOLINTNEXTLINE(google-explicit-constructor) —— 订阅点要能直接写 {7, 10, kResponse}
  Pattern(std::optional<Fields>... values) : fields(std::move(values)...) {}
};

namespace detail {

/// 元组哈希:以 FNV-1a 混合各字段的 std::hash 结果。
template <typename Tuple>
struct TupleHash {
  std::size_t operator()(const Tuple& value) const {
    std::size_t hash = 1469598103934665603ULL;
    Mix(value, std::make_index_sequence<std::tuple_size_v<Tuple>>{}, hash);
    return hash;
  }

 private:
  template <std::size_t... I>
  static void Mix(const Tuple& value, std::index_sequence<I...>,
                  std::size_t& hash) {
    ((hash = (hash ^ std::hash<std::tuple_element_t<I, Tuple>>{}(
                         std::get<I>(value))) *
                    1099511628211ULL),
     ...);
  }
};

}  // namespace detail

/**
 * @brief 按键分配的消息路由器。见文件头。
 *
 * @tparam T      消息类型(可拷贝);本件对其不透明。
 * @tparam Fields 参与匹配的字段类型,顺序即订阅模式里的位置。
 */
template <typename T, typename... Fields>
class Dispatcher {
  static_assert(sizeof...(Fields) > 0, "至少要有一个匹配字段");
  static_assert(sizeof...(Fields) <= 32, "mask 用 uint32_t，字段不超过 32 个");

 public:
  /// 订阅模式:字段可写具体值或 `kAny`。
  using Key = Pattern<Fields...>;
  /// 键提取函数:给出一条消息各匹配字段的具体值,由调用方提供。
  using KeyOf = std::function<std::tuple<Fields...>(const T&)>;

 private:
  using Values = std::tuple<Fields...>;
  using Mask = std::uint32_t;
  static constexpr std::size_t kFieldCount = sizeof...(Fields);

  struct Entry {
    std::uint64_t id;
    std::shared_ptr<Coro::Awaitable<T>> mailbox;
  };
  /// 同一 mask 下按投影后的键值分桶;桶内可存在多个订阅者,各得一份副本。
  using Bucket = std::unordered_map<Values, std::vector<Entry>,
                                    detail::TupleHash<Values>>;

  struct State {
    explicit State(KeyOf key) : key_of(std::move(key)) {}

    /// 构造后只读,故不受 `mutex` 保护;其内不得回调本件(锁不可重入)。
    const KeyOf key_of;

    /// 守以下三项。fiber 版互斥量:等待时让出 fiber 而不阻塞线程。持锁期内**无挂起点**
    /// ——投递与关闭信箱一律在锁外做,见文件头「线程安全」。
    boost::fibers::mutex mutex;

    /// 仅保留存在订阅的 mask,使 `Dispatch` 的探测次数随订阅情况自动收敛。
    std::unordered_map<Mask, Bucket> by_mask;
    std::uint64_t next_id{1};
    std::error_code closed;  ///< 非空表示已 CloseAll,此后订阅直接返回已关闭的信箱。
  };

  /// 投递/关闭用的信箱快照:在锁内采集,在锁外逐个 resolve/close。
  using Mailboxes = std::vector<std::shared_ptr<Coro::Awaitable<T>>>;

 public:
  /**
   * @brief 订阅凭据:持有一个信箱,并在析构时注销该订阅。仅可移动。
   *
   * 析构即从索引中摘除对应条目。内部以弱引用持有索引,故本类允许在 `Dispatcher` 析构
   * 之后再析构;索引锁也在 `State` 内,`weak_ptr::lock()` 成功即为其续命,故与 `~Dispatcher`
   * 并发亦安全。
   *
   * 本类**不是**线程安全的容器:同一张凭据不得被两个线程同时使用(移动、`Reset`、`Wait`);
   * 不同凭据之间、以及凭据与 `Dispatcher` 之间的并发则是安全的。
   */
  class Ticket {
   public:
    Ticket() = default;
    ~Ticket() { Reset(); }

    Ticket(Ticket&& other) noexcept { *this = std::move(other); }
    Ticket& operator=(Ticket&& other) noexcept {
      if (this != &other) {
        Reset();
        state_ = std::move(other.state_);
        mask_ = other.mask_;
        values_ = std::move(other.values_);
        id_ = other.id_;
        mailbox_ = std::move(other.mailbox_);
        other.id_ = 0;
      }
      return *this;
    }
    Ticket(const Ticket&) = delete;
    Ticket& operator=(const Ticket&) = delete;

    /**
     * @brief 等待一条匹配的消息;让出所在线程,不阻塞。
     *
     * 信箱为队列语义,同一凭据可多次等待,按到达顺序取值。
     *
     * @param timeout 等待时限;零值表示不设时限。
     * @return 匹配到的消息;超时返回 `kTimeout`;`CloseAll` 之后返回其终止原因(通常为
     *         `kClosed`);空凭据返回 `kInvalidState`。
     */
    [[nodiscard]] Coro::Result<T> Wait(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) {
      if (!mailbox_) {
        return make_error_code(TransportErrc::kInvalidState);
      }
      Coro::Result<T, std::error_code> got =
          timeout > std::chrono::milliseconds::zero()
              ? Coro::await_for(mailbox_, timeout)
              : Coro::await(mailbox_);
      if (got) {
        return Coro::Result<T>{std::move(got).value()};
      }
      if (got.error() == std::make_error_code(std::errc::timed_out)) {
        return make_error_code(TransportErrc::kTimeout);
      }
      return got.error();  // 终止原因原样透出
    }

    /// @brief 信箱句柄:供调用方自行 `await_for` / `generate` / `shared()`。
    [[nodiscard]] const std::shared_ptr<Coro::Awaitable<T>>& mailbox() const {
      return mailbox_;
    }

    /// @brief 提前注销;析构时亦会执行。注销后本凭据不再接收消息——但与 `Dispatch` 并发
    ///        时,已进入投递快照的那一条仍会送达信箱(见文件头「线程安全」)。
    ///
    /// 取索引锁,故**可能让出 fiber**;持锁期只有一次索引摘除,等待有界。
    void Reset() {
      if (id_ == 0) {
        return;
      }
      if (auto state = state_.lock()) {
        Unsubscribe(*state, mask_, values_, id_);
      }
      state_.reset();
      mailbox_.reset();
      id_ = 0;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
      return mailbox_ != nullptr;
    }

   private:
    friend class Dispatcher;
    std::weak_ptr<State> state_;
    Mask mask_{0};
    Values values_{};
    std::uint64_t id_{0};
    std::shared_ptr<Coro::Awaitable<T>> mailbox_;
  };

  /// @param key_of 键提取函数:给出一条消息各匹配字段的具体值;其内不得回调本件。
  explicit Dispatcher(KeyOf key_of)
      : state_(std::make_shared<State>(std::move(key_of))) {}

  Dispatcher(const Dispatcher&) = delete;
  Dispatcher& operator=(const Dispatcher&) = delete;

  /**
   * @brief 按键登记一个订阅,不参与匹配的字段填 `kAny`。
   * @return 订阅凭据,析构时自动注销。若已 `CloseAll`,返回的凭据其信箱处于已关闭状态,
   *         `Wait` 将立即得到该终止原因,语义等同于"关闭后不再受理订阅"。
   */
  [[nodiscard]] Ticket Subscribe(Key key) {
    Ticket ticket;
    ticket.mailbox_ = std::make_shared<Coro::Awaitable<T>>();
    // 键的推导与信箱的构造都不碰共享索引，放在锁外，压缩持锁期。
    const Mask mask = MaskOf(key, std::make_index_sequence<kFieldCount>{});
    Values values = ValuesOf(key, std::make_index_sequence<kFieldCount>{});

    std::error_code closed;
    {
      std::lock_guard<boost::fibers::mutex> lock(state_->mutex);
      if (!state_->closed) {
        ticket.state_ = state_;
        ticket.mask_ = mask;
        ticket.values_ = std::move(values);
        ticket.id_ = state_->next_id++;
        state_->by_mask[mask][ticket.values_].push_back(
            Entry{ticket.id_, ticket.mailbox_});
        return ticket;
      }
      closed = state_->closed;
    }
    // close 会取信箱内部的 fiber 互斥量、可能让出，故在锁外做。
    ticket.mailbox_->close(closed);
    return ticket;  // id_ 保持 0，表示未进入索引，无需注销
  }

  /**
   * @brief 将一条消息投递给全部键匹配的订阅者。
   *
   * @return 实际投递的份数。返回 0 表示无匹配订阅,此时 `value` 未被修改,调用方可自行
   *         处置(转交处理器、归因丢弃等)。
   */
  std::size_t Dispatch(const T& value) {
    // key_of 构造后只读且约定不回调本件，故在锁外求值。
    const Values full = state_->key_of(value);

    Mailboxes recipients;
    {
      std::lock_guard<boost::fibers::mutex> lock(state_->mutex);
      for (auto& [mask, bucket] : state_->by_mask) {
        auto found = bucket.find(
            Project(full, mask, std::make_index_sequence<kFieldCount>{}));
        if (found == bucket.end()) {
          continue;
        }
        for (auto& entry : found->second) {
          recipients.push_back(entry.mailbox);
        }
      }
    }

    // 锁外投递：resolve 会取信箱内部的 fiber 互斥量、可能让出。快照持 shared_ptr，信箱
    // 必然存活；期间被注销的凭据仍可能收到这一条，见文件头「线程安全」。
    std::size_t delivered = 0;
    for (auto& mailbox : recipients) {
      if (mailbox->resolve(value)) {
        ++delivered;
      }
    }
    return delivered;
  }

  /**
   * @brief 关闭全部信箱并置终止标记,供节点关闭时使用。
   *
   * 在途的 `Wait` 因此恰好终结一次;此后的 `Subscribe` 一律返回信箱已关闭的凭据。
   */
  void CloseAll(std::error_code error) {
    Mailboxes mailboxes;
    {
      std::lock_guard<boost::fibers::mutex> lock(state_->mutex);
      if (state_->closed) {
        return;  // 幂等：首次终止原因不被覆盖
      }
      state_->closed = error;
      for (auto& [mask, bucket] : state_->by_mask) {
        for (auto& [values, entries] : bucket) {
          for (auto& entry : entries) {
            mailboxes.push_back(entry.mailbox);
          }
        }
      }
      state_->by_mask.clear();
    }
    // 锁外关闭：close 会取信箱内部的 fiber 互斥量、可能让出。
    for (auto& mailbox : mailboxes) {
      mailbox->close(error);
    }
  }

  /// @brief 当前在册的订阅数,供诊断与测试使用。
  [[nodiscard]] std::size_t Size() const {
    // shared_ptr 的 operator-> 给出非 const 的 State&，故 const 成员函数里可直接取锁，
    // 无需把 mutex 标 mutable。
    std::lock_guard<boost::fibers::mutex> lock(state_->mutex);
    std::size_t total = 0;
    for (const auto& [mask, bucket] : state_->by_mask) {
      for (const auto& [values, entries] : bucket) {
        total += entries.size();
      }
    }
    return total;
  }

  /// @brief 当前在用的 mask 种数,即单条消息的探测次数,供诊断与测试使用。
  [[nodiscard]] std::size_t ProbeCount() const {
    std::lock_guard<boost::fibers::mutex> lock(state_->mutex);
    return state_->by_mask.size();
  }

 private:
  /// 由订阅模式推导 mask:标记哪些字段被约束。
  template <std::size_t... I>
  static Mask MaskOf(const Key& key, std::index_sequence<I...>) {
    Mask mask = 0;
    ((mask |= std::get<I>(key.fields).has_value() ? (Mask{1} << I) : Mask{0}), ...);
    return mask;
  }

  /// 提取订阅模式的键值:被约束的字段取其值,未约束的字段取值初始化的占位值。键值仅在
  /// 同一 mask 内比较,故占位值的取值不影响匹配结果。
  template <std::size_t... I>
  static Values ValuesOf(const Key& key, std::index_sequence<I...>) {
    Values out{};
    ((std::get<I>(out) =
          std::get<I>(key.fields).value_or(std::tuple_element_t<I, Values>{})),
     ...);
    return out;
  }

  /// 将消息的完整键按 mask 投影:未约束的字段置为同一占位值,使其可与订阅键直接比较。
  template <std::size_t... I>
  static Values Project(const Values& full, Mask mask, std::index_sequence<I...>) {
    Values out{};
    ((std::get<I>(out) = (mask & (Mask{1} << I))
                             ? std::get<I>(full)
                             : std::tuple_element_t<I, Values>{}),
     ...);
    return out;
  }

  static void Unsubscribe(State& state, Mask mask, const Values& values,
                          std::uint64_t id) {
    std::lock_guard<boost::fibers::mutex> lock(state.mutex);
    auto bucket = state.by_mask.find(mask);
    if (bucket == state.by_mask.end()) {
      return;
    }
    auto entries = bucket->second.find(values);
    if (entries == bucket->second.end()) {
      return;
    }
    auto& list = entries->second;
    for (auto it = list.begin(); it != list.end(); ++it) {
      if (it->id == id) {
        list.erase(it);
        break;
      }
    }
    // 清理空桶与空 mask，避免 Dispatch 持续探测已无订阅的 mask
    if (list.empty()) {
      bucket->second.erase(entries);
    }
    if (bucket->second.empty()) {
      state.by_mask.erase(bucket);
    }
  }

  std::shared_ptr<State> state_;
};

}  // namespace transport
