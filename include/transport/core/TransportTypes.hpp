#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

#include "transport/core/Endpoint.hpp"
#include "transport/core/Cancellation.hpp"

namespace transport {

struct Datagram {
  std::vector<std::uint8_t> bytes;
  Endpoint source;
};

struct SendUnit {
  std::vector<std::uint8_t> bytes;
  Endpoint destination;
};

enum class LifecycleState { kCreated, kRunning, kClosing, kClosed };

/**
 * @brief 链路可用性——所有介质同形的当前 I/O 事实(RT_TRANSPORT_009,ADR-0004 D2)。
 *
 * 与 `LifecycleState` 正交:后者是传输实例自身的生命周期,前者是**此刻能否收发字节**。
 * 它与 `LastSendTime`/`LastReceiveTime`/`LastError` 同类——每种介质都答得出、**不含
 * 连接管理策略**(退避参数、重连决策不经此暴露);调用方不据其推断介质是否会重连。
 */
enum class LinkState {
  kDown,          ///< 链路不可用(未启动 / 已关闭 / 设备未打开 / 未连接)。
  kEstablishing,  ///< 正在建立(TCP 连接中 / 退避重连中);仅具连接管理的传输会给出。
  kUp,            ///< 链路可用,可收发字节。
};

struct OperationOptions {
  using Clock = std::chrono::steady_clock;
  std::optional<Clock::time_point> deadline;
  CancellationToken cancellation;
};

}  // namespace transport
