#pragma once

// -----------------------------------------------------------------------------
// DdsConfig.hpp — DDS 配置（DdsMode + DdsQos + DdsConfig）
// DdsQos 为简化 QoS 结构（借鉴 Apollo Cyber RT）：可枚举、可校验、provider 无关，
// 由 provider 映射到底层 DDS QoS 策略。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

enum class DdsMode { kPubSub, kReqResp };

struct DdsQos {
  enum class Reliability { kReliable, kBestEffort };
  enum class Durability { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability durability = Durability::kVolatile;
  uint32_t history_depth = 10;  // KEEP_LAST depth；0 非法
};

struct DdsConfig {
  DdsMode mode = DdsMode::kPubSub;
  std::vector<std::string> topics;  // topics[0] 为 Send(data) 的默认 topic
  int domain_id = 0;                // 一个实例 = 一个 DomainParticipant
  DdsQos qos;                       // writer/reader 共用
  std::string provider = "FastDDS"; // 从 DdsProviderRegistry 选择
};

}  // namespace transport
