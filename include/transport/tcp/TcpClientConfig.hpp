#pragma once

/**
 * @file TcpClientConfig.hpp
 * @brief TcpClientTransport 连接管理策略配置——连接超时、退避、稳定重置阈值。
 */

#include <chrono>
#include <cstdint>
#include <string>

namespace transport {

/**
 * @brief TCP 客户端连接管理配置(ADR-0003 D11 Q4)。
 *
 * 端点(host/port)+ 连接尝试与退避策略参数。所有时长/阈值均可注入,测试可用小值
 * 确定化退避序列;`jitter_enabled=false` 关闭抖动以断言基础序列,`jitter_seed`
 * 令抖动确定可复现。P3-1 只读取本配置一次(构造时);运行时 `ApplyConfig` 属 P3-3。
 */
struct TcpClientConfig {
  using Duration = std::chrono::milliseconds;

  /// @brief 目标主机名或 IP。
  std::string host = "127.0.0.1";
  /// @brief 目标端口。
  std::uint16_t port = 0;

  /// @brief 单次连接尝试超时;超时后显式 abort 底层 socket(corosocket 摩擦 1)。
  Duration connect_timeout{5000};

  /// @brief 首次退避时长(退避级别初值)。
  Duration initial_backoff{1000};
  /// @brief 退避时长上限。
  Duration max_backoff{30000};
  /// @brief 退避倍增因子(每次失败后 ×multiplier,不超过 max_backoff)。
  double backoff_multiplier = 2.0;

  /// @brief 抖动比例(±ratio);Connected 期抖动令并发客户端错峰重连。
  double jitter_ratio = 0.2;
  /// @brief 是否启用抖动;测试关闭以断言确定退避序列。
  bool jitter_enabled = true;
  /// @brief 抖动随机源种子;非 0 时确定可复现,0 时用 random_device 随机播种。
  std::uint64_t jitter_seed = 0;

  /// @brief 连接稳定持续 ≥ 本阈值后,下次断开重置退避级别为 initial_backoff。
  Duration stable_reset_after{60000};
};

/// @brief 内容相等:比较全部热更新字段,用于 `ApplyConfig` 同版同容 no-op 判定
///        (RT_TCP_RECONFIG_004)。
inline bool operator==(const TcpClientConfig& a, const TcpClientConfig& b) {
  return a.host == b.host && a.port == b.port &&
         a.connect_timeout == b.connect_timeout &&
         a.initial_backoff == b.initial_backoff &&
         a.max_backoff == b.max_backoff &&
         a.backoff_multiplier == b.backoff_multiplier &&
         a.jitter_ratio == b.jitter_ratio &&
         a.jitter_enabled == b.jitter_enabled &&
         a.jitter_seed == b.jitter_seed &&
         a.stable_reset_after == b.stable_reset_after;
}

inline bool operator!=(const TcpClientConfig& a, const TcpClientConfig& b) {
  return !(a == b);
}

}  // namespace transport
