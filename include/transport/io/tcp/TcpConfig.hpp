#pragma once

/**
 * @file TcpConfig.hpp
 * @brief TCP 客户端传输配置——对端端点 + 唯一的时间量(ADR-0011 D5/D14)。
 */

#include <chrono>
#include <cstdint>
#include <string>

namespace transport {

/**
 * @brief `TcpTransport` 的配置。
 *
 * **只有一个时间量**(D5):三处用途——等连上 / 读静默判链路坏 / 连不上时的重连退避
 * ——共用它,不设三个旋钮。
 */
struct TcpConfig {
  /// 对端主机(非空,否则 `Start()` 返 kConfiguration)。
  std::string host = "127.0.0.1";
  /// 对端端口(非 0,否则 `Start()` 返 kConfiguration)。
  std::uint16_t port = 0;

  /// 唯一的时间量,三处共用:等连上 / 读静默判链路坏 / 连不上时的重连间隔。
  ///
  /// **须为正**(D14)——它同时是退避间隔,零值退化为紧循环(对端主机在而端口未监听时
  /// 内核立即回 RST,`connect` 微秒级失败,烧 CPU 且向对端刷 SYN)。故 TCP **不设**
  /// UDP 那档"0 = 禁用静默判活",非正值由 `Start()` 直接拒绝。
  std::chrono::milliseconds silence_timeout{5000};
};

}  // namespace transport
