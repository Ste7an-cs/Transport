#pragma once

/**
 * @file PerfStats.hpp
 * @brief 统计与报表——**与 Fast DDS 3.6.1 逐列同名**(ADR-0018 **D2** / **D3**)。
 *
 * ## 时延(**D2**)
 *
 * ```
 * 延迟 = RTT / 2 − 时钟开销        (回显型:Send / 发布-订阅)
 * 延迟 = RTT     − 时钟开销        (请求型:一次交互本身就是一个往返)
 * ```
 *
 * **时钟开销由 1001 次时钟读取标定**(照 `LatencyTestPublisher.cpp:373-378`),且
 * **不减半**——一次往返读两次时钟,开销本就是两次读取之和。
 *
 * 报表列与 Fast DDS 逐一同名:`Bytes, Samples, stdev, mean, min, 50%, 90%, 99%,
 * 99.99%, max`(µs)。百分位取法亦照抄(`size * p` 向下取整后减一,越界给 NaN)。
 *
 * ## 吞吐(**D3**)
 *
 * 两侧分列,列名与 Fast DDS 的 `print_results()` 逐一同名。**发送侧的 `Sent Samples`
 * 是"入队条数"**——写侧 fire-and-forget、队列满时静默丢最旧且返回成功(ADR-0007 D3),
 * 它**不等于上线条数**;`Rec Samples` / `Lost Samples` 由接收侧按 payload 内嵌的自增
 * 序号自行算出(ADR-0014:框架无观测面)。**解读时以接收侧为准。**
 */

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace perf {

/// @brief 标定时钟读取开销(微秒)——**1001 次读取**,照 Fast DDS 的做法(**D2**)。
[[nodiscard]] double MeasureClockOverheadUs();

/// 一行时延矩阵的统计量。字段名与 Fast DDS `TimeStats` 对应。
struct LatencyStats {
  std::uint64_t bytes = 0;
  std::uint32_t samples = 0;
  double stdev = 0.0;
  double mean = 0.0;
  double minimum = 0.0;
  double percentile_50 = 0.0;
  double percentile_90 = 0.0;
  double percentile_99 = 0.0;
  double percentile_9999 = 0.0;
  double maximum = 0.0;
};

/// @brief 由一组往返时长(µs,**已扣时钟开销、已按口径取半**)算出统计量。
///
/// @param times 样本;**本函数会就地排序**(与 Fast DDS 同)。空集返回全 0 的统计量。
[[nodiscard]] LatencyStats ComputeLatencyStats(std::uint64_t bytes,
                                               std::vector<double>& times);

/// @brief 打印时延表头(逐字同 Fast DDS 的两行)。
///
/// @param title 本表的口径说明行,先于表头打印。
void PrintLatencyHeader(const std::string& title);

/// @brief 打印一行时延统计(格式串逐字同 Fast DDS)。
void PrintLatencyRow(const LatencyStats& stats);

/// 一行吞吐矩阵的两侧数字。
struct ThroughputRow {
  std::uint32_t payload_bytes = 0;
  std::uint32_t demand = 0;
  std::uint32_t recovery_time_ms = 0;
  // 发送侧——**只能报入队条数**(ADR-0007 D3)。
  double sent_samples = 0.0;
  double send_time_us = 0.0;
  // 接收侧——由 payload 内嵌序号算出。
  double recv_samples = 0.0;
  double lost_samples = 0.0;
  double recv_time_us = 0.0;
};

/// @brief 打印吞吐表头(三行,逐字同 Fast DDS `print_results()`)+ 一行口径提醒。
void PrintThroughputHeader(const std::string& title);

/// @brief 打印一行吞吐统计;`Packs/sec` 与 `MBits/sec` 按 Fast DDS 的算式当场算出。
void PrintThroughputRow(const ThroughputRow& row);

}  // namespace perf
