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
  /// 100ms 即 Fast DDS 的默认值。
  ///
  /// ⚠ **它不是关闭路径的最坏等待**(ADR-0013「明确接受的代价」7):`Publish` 的阻塞主要
  /// 来自**同进程订阅方的交付回调在发布线程上同步执行**(默认 `INTRAPROCESS_FULL`),
  /// 本项**根本不参与**。关闭时的最坏等待**没有上界**,由同进程内最慢的那个订阅回调决定。
  std::chrono::milliseconds max_blocking_time{100};

  /// `LivelinessQosPolicy::lease_duration`,kind 固定为 `AUTOMATIC`(ADR-0013 D9/D10)。
  /// **须为正**。
  ///
  /// **不可省**:仅靠 `matched` 时对端被硬杀要等 participant lease(默认 20s)才检出,
  /// 期间谎报 `kUp`。
  std::chrono::milliseconds liveliness_lease{2000};
};

/// @brief DDS 传输的配置——**只有三项,不含任何 topic**(ADR-0013 D12/D16)。
///
/// topic 不在这里:它由 `DdsNode` 的注册接口给出,在**注册那一刻**判合法性;传输层则由
/// `DdsTransport::DeclareWriter` / `DeclareReader` 逐项声明端点(**D15**)。
///
/// 由此 DDS **没有"默认对端"**——`AsyncWrite` 的 `Endpoint::Default()` 在本介质上无从
/// 解析,写线程会丢该条并落 `kInvalidArgument` 到 `LastError()`。
struct DdsConfig {
  /// DDS domain 编号,合法区间 `[0, 232]`(**D12**,`Start()` 时校验)。
  int         domain_id = 0;
  /// `DdsProviderRegistry` 里的实现名;**须非空且已注册**(**D12**)。内建两个:
  /// `"fake"`(进程内总线,恒可用)与 `"fastdds"`(真实互通,仅在装了 Fast DDS 时存在)。
  std::string provider = "fake";
  DdsQos      qos;
};

}  // namespace transport
