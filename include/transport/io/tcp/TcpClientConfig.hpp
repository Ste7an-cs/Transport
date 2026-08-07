#pragma once

/**
 * @file TcpClientConfig.hpp
 * @brief TcpClientTransport 连接管理策略配置——端点、连接超时、固定重连间隔。
 */

#include <chrono>
#include <cstdint>
#include <string>

#include "transport/core/ITraceSink.hpp"

namespace transport {

/**
 * @brief TCP 客户端连接管理配置(ADR-0003 D11 Q4;退避部分由 ADR-0005 D4 取代)。
 *
 * 端点(host/port)+ 单次连接超时 + **固定重连间隔**。指数退避已撤销(ADR-0005 D4):
 * 倍率、上限、抖动、稳定重置四套参数一并作废,重连按固定间隔无限重试直至连上或关闭。
 * 所有时长可注入,测试用小值确定化时序。热更新范围(RT_TCP_RECONFIG_002)恰为本结构
 * 的三个策略字段:端点 / 连接超时 / 重连间隔。
 */
struct TcpClientConfig {
  using Duration = std::chrono::milliseconds;

  /// @brief 目标主机名或 IP。
  std::string host = "127.0.0.1";
  /// @brief 目标端口。
  std::uint16_t port = 0;

  /// @brief 单次连接尝试超时;超时后显式 abort 底层 socket(corosocket 摩擦 1)。
  ///        有效范围 100ms–60s(SRS §3.1.7.4)。
  Duration connect_timeout{5000};

  /// @brief 相邻两次连接尝试之间的**固定**间隔(SRS §3.1.7.4 缺省 1s,ADR-0005 D4)。
  ///
  /// **须为正**:对端主机在而端口未监听时内核立即回 RST,`connect` 微秒级失败,零间隔
  /// 重连会退化为紧循环(烧 CPU 且向对端刷 SYN)——这正是保留非零间隔的唯一理由。
  Duration reconnect_interval{1000};

  /// @brief 可选 Trace 出口(P5-4,RT_TRACE_001/002):非空则在连接/代际/重连/生命周期
  ///        跃迁边界点上报 `connect`/`generation`/`reconnect`/`close` 事件;为空时
  ///        `RecordEvent` 仅一次判空,不产生任何其它开销。不属于热更新范围,不参与
  ///        `operator==`/`ApplyConfig` 同版同容判定。
  ITraceSink* trace_sink = nullptr;
};

/// @brief 内容相等:比较全部热更新字段,用于 `ApplyConfig` 同版同容 no-op 判定
///        (RT_TCP_RECONFIG_004)。
inline bool operator==(const TcpClientConfig& a, const TcpClientConfig& b) {
  return a.host == b.host && a.port == b.port &&
         a.connect_timeout == b.connect_timeout &&
         a.reconnect_interval == b.reconnect_interval;
}

inline bool operator!=(const TcpClientConfig& a, const TcpClientConfig& b) {
  return !(a == b);
}

}  // namespace transport
