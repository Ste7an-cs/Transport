#pragma once

/**
 * @file IConnectionObservable.hpp
 * @brief 连接管理传输的可选观察面——状态机快照 + 拉模型状态等待 + 诊断 getter。
 */

#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 连接管理状态机可观察态(RT_LIFECYCLE_002,TCP 客户端 Running 内子状态)。
 *
 * 与 base 生命周期 `LifecycleState` 正交:连接代际churn 在 Running 内进行,`Close`
 * 走 `Closing→Closed` 终态,不在此枚举内。
 */
enum class ConnectionState {
  kDisconnected,  ///< 未连接(初始 / 关闭后)。
  kConnecting,    ///< 正在发起一次连接尝试。
  kConnected,     ///< 已建立物理连接,内层传输就绪。
  kReconnecting,  ///< 断连后退避等待下一次尝试。
};

/**
 * @brief 连接管理传输的可选观察面(ADR-0003 D11 Q2,ADR-0002 D3′)。
 *
 * 不进 base `ITransport`——连接概念不下沉纯字节管道(守 RT_DESIGN_008)。node 检测
 * 本接口后 spawn reactor fiber 订阅状态跃迁(拉模型非回调,契合 fiber + RT_HANDLER_002)。
 * 等待方法支持多 fiber 并发等待;`OperationOptions.deadline` 只结束本次等待、不停后台
 * 重连。
 */
class IConnectionObservable {
 public:
  using Clock = OperationOptions::Clock;

  virtual ~IConnectionObservable() = default;

  /// @brief 当前连接状态快照。
  [[nodiscard]] virtual ConnectionState State() const = 0;

  /// @brief 等待进入目标状态(多等待者);已满足即刻返回。
  /// @param options deadline 只结束本次等待(超时 kTimeout),后台重连继续;取消 kCancelled;
  ///        目标在关闭前无法达成返回 kClosed。
  [[nodiscard]] virtual Status WaitForState(ConnectionState target,
                                            OperationOptions options = {}) = 0;

  /// @brief 等待下一次状态跃迁,返回跃迁后的新状态(多等待者)。
  /// @param options deadline 超时 kTimeout;取消 kCancelled。
  [[nodiscard]] virtual Result<ConnectionState> WaitStateChange(
      OperationOptions options = {}) = 0;

  /// @brief 连接代际:单调 uint64,每次成功物理连接 +1。
  [[nodiscard]] virtual std::uint64_t Generation() const = 0;
  /// @brief 配置版本:单调 uint64(P3-1 固定初值,运行时 ApplyConfig 属 P3-3)。
  [[nodiscard]] virtual std::uint64_t ConfigVersion() const = 0;
  /// @brief 最近一次连接失败的错误类别(无则默认构造的 error_code)。
  [[nodiscard]] virtual std::error_code LastFailure() const = 0;
  /// @brief 迄今累计的连接尝试次数。
  [[nodiscard]] virtual std::size_t AttemptCount() const = 0;
  /// @brief 退避中下次尝试的预定时刻(非退避期为空)。
  [[nodiscard]] virtual std::optional<Clock::time_point> NextAttemptTime()
      const = 0;
};

}  // namespace transport
