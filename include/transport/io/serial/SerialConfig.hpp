#pragma once

/**
 * @file SerialConfig.hpp
 * @brief 串口传输配置——设备参数 + 唯一的时间量(ADR-0012 D3/D12)。
 */

#include <chrono>
#include <cstdint>
#include <string>

namespace transport {

/**
 * @brief `SerialTransport` 的配置。
 *
 * **只有一个时间量**(D3):`silence_timeout` 两处共用——读静默判链路坏 / 重开退避间隔。
 * **比 TCP 少一处用途**:串口的 `open()` 是**同步**的,没有"等连上"那一处,这一点上回到
 * `UdpConfig` 的形态(其 `bind()` 同样同步)。
 *
 * 全部字段在 `Start()` 时一次性校验(D12),非法返 `kConfiguration` 并**停在 `Created`**。
 */
struct SerialConfig {
  /// 设备路径(非空,否则 `Start()` 返 kConfiguration),如 `/dev/ttyUSB0`、`/dev/pts/7`。
  std::string device;
  /// 波特率(非 0)。
  std::uint32_t baud_rate = 115200;
  /// 数据位:5 / 6 / 7 / 8。
  std::uint8_t data_bits = 8;
  /// 停止位:1 或 2。
  std::uint8_t stop_bits = 1;
  /// 校验:'N' / 'E' / 'O'(大小写均可)。
  char parity = 'N';

  /// 唯一的时间量,两处共用:读静默判链路坏 / 重开退避间隔。
  ///
  /// **须为正**(D12)——它同时是退避间隔,零值退化为紧循环(设备不存在时 `open()`
  /// 微秒级失败,烧 CPU)。故串口**不设** `UdpConfig` 那档"0 = 禁用静默判活":D4 已定
  /// 静默超时是串口"链路坏了"的**唯一**主动判据(串口没有断开事件),禁用它等于放弃判活。
  ///
  /// **调用方须按协议特征配置**(ADR-0012 代价 4):缺省 5s 对"周期性上报"类协议足够;
  /// "长时间静默、偶发指令"类协议须调大,否则会周期性无谓重开设备。
  std::chrono::milliseconds silence_timeout{5000};
};

}  // namespace transport
