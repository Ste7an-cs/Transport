#pragma once

#include <memory>
#include <mutex>
#include <system_error>
#include <type_traits>
#include <utility>

#include "await/awaitable.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 一次性广播完成量:首个 `Complete` 定结果,全部等待者共享该结果(ADR-0006 D3)。
 *
 * 结构为"存结果 + 一条共享 `Awaitable` + `close()` 广播":
 * - `Complete` 持锁存结果,**锁外** `close()`——`Awaitable::close()` 的语义即
 *   "唤醒并收敛**所有**等待者",故一次调用即广播,无须逐等待者投递。
 * - `Wait` 先持锁查已存结果:**迟到者**(`Complete` 之后才来)直接返回,不进等待;
 *   否则在共享通道上等待,醒来后再读存好的结果。
 * - `deadline` 无须为每等待者分配独立通道:`Awaitable::await_for` 超时走
 *   `pop_wait_for` 直接返回 `timed_out` 而**不关闭 channel**,天然不殃及其余等待者。
 *
 * @note `Wait(OperationOptions)` **不支持取消令牌**(ADR-0006 D3 明确接受的能力移除);
 *       传入的 `cancellation` 被静默忽略,只有 `deadline` 生效。
 * @tparam T 完成结果的负载类型,为 `void` 或可拷贝构造。
 */
template <typename T>
class SharedCompletion {
  static_assert(std::is_void_v<T> || std::is_copy_constructible_v<T>,
                "SharedCompletion<T> requires void or copy-constructible T: "
                "each waiter receives its own Result<T> copy");

 public:
  SharedCompletion() : state_(std::make_shared<State>()) {}

  /**
   * @brief 定结果并广播唤醒全部等待者;仅首次调用生效。
   * @param result 本次完成结果。
   * @return 本次调用定下了结果返回 true;已完成过返回 false(结果不被覆盖)。
   */
  bool Complete(Result<T> result) {
    auto stored = std::make_shared<const Result<T>>(std::move(result));
    const auto state = state_;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      if (state->completion) {
        return false;
      }
      state->completion = std::move(stored);
    }
    state->broadcast.close();  // 锁外广播,避免在 std::mutex 内取 fiber 互斥量。
    return true;
  }

  /**
   * @brief 等待完成结果;已完成则立即返回,否则等到广播或 `deadline` 到期。
   * @param options 仅 `deadline` 生效;`cancellation` 被忽略(能力已移除)。
   * @return 已完成返回该结果的副本;`deadline` 到期未完成返回 `kTimeout`。
   */
  Result<T> Wait(OperationOptions options = {}) const {
    const auto state = state_;
    if (const auto stored = Peek(*state)) {
      return *stored;  // 迟到者直读;副本在锁外构造,不阻塞并发 Complete。
    }
    if (options.deadline) {
      (void)Coro::await_for(state->broadcast,
                            *options.deadline - OperationOptions::Clock::now());
    } else {
      (void)Coro::await(state->broadcast);
    }
    // 唤醒成因不作判据:结果存在即完成,否则只可能是 deadline 到期。
    if (const auto stored = Peek(*state)) {
      return *stored;
    }
    return make_error_code(TransportErrc::kTimeout);
  }

 private:
  using StoredResult = std::shared_ptr<const Result<T>>;

  struct State {
    mutable std::mutex mutex;
    StoredResult completion;          ///< 非空即"已完成";首个 Complete 后不再变。
    Coro::Awaitable<void> broadcast;  ///< 共享通知通道,`close()` 即广播。
  };

  /// @brief 持锁取结果句柄(不解引用),使值副本落在锁外。
  static StoredResult Peek(const State& state) {
    std::lock_guard<std::mutex> lock(state.mutex);
    return state.completion;
  }

  std::shared_ptr<State> state_;
};

}  // namespace transport
