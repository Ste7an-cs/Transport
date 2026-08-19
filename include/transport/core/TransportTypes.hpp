#pragma once

#include <cstdint>
#include <vector>

#include "transport/core/Endpoint.hpp"

namespace transport {

/// 一份字节及其**对端**。读写共用:读侧 `peer` 是发送方,写侧是目的地——方向由用它的
/// 接口决定,不需要两个结构相同的类型来编码(原 `SendUnit` 已删)。
struct Datagram {
  std::vector<std::uint8_t> bytes;
  Endpoint peer;
};

enum class LifecycleState { kCreated, kRunning, kClosing, kClosed };

/**
 * @brief 链路可用性——所有介质同形的当前 I/O 事实(RT_TRANSPORT_009,ADR-0004 D2)。
 *
 * 与 `LifecycleState` 正交:后者是传输实例自身的生命周期,前者是**此刻能否收发字节**。
 * 它与 `LastError()` 同类——每种介质都答得出、**不含
 * 连接管理策略**(退避参数、重连决策不经此暴露);调用方不据其推断介质是否会重连。
 */
enum class LinkState {
  kDown,          ///< 链路不可用(未启动 / 已关闭 / 设备未打开 / 未连接)。
  kEstablishing,  ///< 正在建立(TCP 连接中 / 退避重连中);仅具连接管理的传输会给出。
  kUp,            ///< 链路可用,可收发字节。
};

}  // namespace transport
