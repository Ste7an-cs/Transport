#pragma once

/**
 * @file DdsConfig.hpp
 * @brief DDS provider 的配置与 QoS——**统一一套,不按模式分**(ADR-0013 D4/D10)。
 */

#include <chrono>
#include <cstdint>
#include <string>

namespace transport {

/**
 * @brief 转达给 DDS 的 QoS(ADR-0013 **D4**:对所有 topic 一视同仁,声明端点时不再带 QoS 参数)。
 *
 * 五项**全部是 DDS 自己定义语义的参数**,我方只做转达(**D10**)。DDS 无 connect、
 * 无退避、无静默判活,故这里**没有**三介质那种"我方定义的时间量"(如 `silence_timeout`)。
 */
struct DdsQos {
  enum class Reliability { kBestEffort, kReliable };
  enum class Durability  { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability  durability  = Durability::kVolatile;
  uint32_t    history_depth = 10;

  /// `ReliabilityQosPolicy::max_blocking_time`——`RELIABLE` 下 writer 准入满时
  /// `Publish` **park 调用线程**的上限(ADR-0013 D3/D10)。**须为正**。
  ///
  /// 100ms 即 Fast DDS 的默认值。它同时是 `Shutdown()` 落在一次阻塞写上时
  /// 最坏要等的时长(「明确接受的代价」7)。
  std::chrono::milliseconds max_blocking_time{100};

  /// `LivelinessQosPolicy::lease_duration`,kind 固定为 `AUTOMATIC`(ADR-0013 D9/D10)。
  /// **须为正**。
  ///
  /// **不可省**:仅靠 `matched` 时对端被硬杀要等 participant lease(默认 20s)才检出,
  /// 期间谎报 `kUp`;配 2s 后 2.0s 检出。
  std::chrono::milliseconds liveliness_lease{2000};
};

/// @brief DDS 传输的配置。topic 不在这里——它在**注册那一刻**判(ADR-0013 D16)。
struct DdsConfig {
  int         domain_id = 0;
  std::string default_topic;      // Send(bytes) 无 endpoint 时的目的 topic
  std::string provider = "fake";  // registry 名;真实互通用 "fastdds"
  DdsQos      qos;
};

}  // namespace transport
