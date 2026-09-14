#include "PerfStats.hpp"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <numeric>

namespace perf {
namespace {

using Clock = std::chrono::steady_clock;

/// 百分位取法照抄 Fast DDS:`elem = size * p`,落在 (0, size] 内则取 `at(elem - 1)`,
/// 否则给 NaN(样本太少时该百分位没有意义,**不外插**)。
double Percentile(const std::vector<double>& sorted, double fraction) {
  const std::size_t elem = static_cast<std::size_t>(
      static_cast<double>(sorted.size()) * fraction);
  if (elem > 0 && elem <= sorted.size()) {
    return sorted.at(elem - 1);
  }
  return NAN;
}

}  // namespace

double MeasureClockOverheadUs() {
  // 照 `LatencyTestPublisher.cpp:373-378`:读 1000 次、除以 1001(首次取时刻也算一次)。
  const Clock::time_point start = Clock::now();
  Clock::time_point end = start;
  for (int i = 0; i < 1000; ++i) {
    end = Clock::now();
  }
  return std::chrono::duration<double, std::micro>(end - start).count() / 1001.0;
}

LatencyStats ComputeLatencyStats(std::uint64_t bytes, std::vector<double>& times) {
  LatencyStats stats;
  stats.bytes = bytes;
  if (times.empty()) {
    return stats;
  }
  stats.samples = static_cast<std::uint32_t>(times.size());
  stats.minimum = *std::min_element(times.begin(), times.end());
  stats.maximum = *std::max_element(times.begin(), times.end());
  stats.mean = std::accumulate(times.begin(), times.end(), 0.0) /
               static_cast<double>(times.size());

  double aux = 0.0;
  for (double value : times) {
    aux += std::pow(value - stats.mean, 2);
  }
  stats.stdev = std::sqrt(aux / static_cast<double>(times.size()));

  std::sort(times.begin(), times.end());
  stats.percentile_50 = Percentile(times, 0.5);
  stats.percentile_90 = Percentile(times, 0.9);
  stats.percentile_99 = Percentile(times, 0.99);
  stats.percentile_9999 = Percentile(times, 0.9999);
  return stats;
}

void PrintLatencyHeader(const std::string& title) {
  std::printf("\n%s\n", title.c_str());
  std::printf(
      "   Bytes, Samples,   stdev,    mean,     min,     50%%,     90%%,"
      "     99%%,  99.99%%,     max\n");
  std::printf(
      "--------,--------,--------,--------,--------,--------,--------,"
      "--------,--------,--------,\n");
  std::fflush(stdout);
}

void PrintLatencyRow(const LatencyStats& stats) {
  std::printf("%8" PRIu64 ",%8u,%8.3f,%8.3f,%8.3f,%8.3f,%8.3f,%8.3f,%8.3f,%8.3f \n",
              stats.bytes, stats.samples, stats.stdev, stats.mean, stats.minimum,
              stats.percentile_50, stats.percentile_90, stats.percentile_99,
              stats.percentile_9999, stats.maximum);
  std::fflush(stdout);
}

void PrintThroughputHeader(const std::string& title) {
  std::printf("\n%s\n", title.c_str());
  std::printf(
      "注:PUBLISHER 的 Sent Samples 是【入队条数】,不等于上线条数"
      "(写侧 fire-and-forget、队列满静默丢最旧且返回成功,ADR-0007 D3);"
      "实收与丢失以 SUBSCRIBER 侧为准(序号自算,ADR-0018 D3)。\n");
  std::printf(
      "[            TEST           ][                    PUBLISHER                      ]"
      "[                            SUBSCRIBER                        ]\n");
  std::printf(
      "[ Bytes,Demand,Recovery Time][Sent Samples,Send Time(us),   Packs/sec,  MBits/sec]"
      "[Rec Samples,Lost Samples,Rec Time(us),   Packs/sec,  MBits/sec]\n");
  std::printf(
      "[------,------,-------------][------------,-------------,------------,-----------]"
      "[-----------,------------,------------,-----------]\n");
  std::fflush(stdout);
}

void PrintThroughputRow(const ThroughputRow& row) {
  // 算式照 Fast DDS `TroughputResults::compute()`。
  const double send_time = row.send_time_us > 0.0 ? row.send_time_us : 1.0;
  const double recv_time = row.recv_time_us > 0.0 ? row.recv_time_us : 1.0;
  const double pub_packs = row.sent_samples * 1000000.0 / send_time;
  const double pub_mbits =
      row.sent_samples * static_cast<double>(row.payload_bytes) * 8.0 / send_time;
  const double sub_packs = row.recv_samples * 1000000.0 / recv_time;
  const double sub_mbits =
      row.recv_samples * static_cast<double>(row.payload_bytes) * 8.0 / recv_time;

  std::printf("%7u,%6u,%13u,%13.0f,%13.0f,%12.3f,%11.3f,%12.0f,%12.0f,%12.0f,%12.3f,%11.3f\n",
              row.payload_bytes, row.demand, row.recovery_time_ms,
              row.sent_samples, row.send_time_us, pub_packs, pub_mbits,
              row.recv_samples, row.lost_samples, row.recv_time_us, sub_packs,
              sub_mbits);
  std::fflush(stdout);
}

}  // namespace perf
