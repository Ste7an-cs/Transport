#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct DdsQos {
  enum class Reliability { kBestEffort, kReliable };
  enum class Durability  { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability  durability  = Durability::kVolatile;
  uint32_t    history_depth = 10;
};

struct DdsConfig {
  int         domain_id = 0;
  std::string default_topic;      // Send(bytes) 无 endpoint 时的目的 topic
  std::string provider = "fake";  // registry 名;真实互通用 "fastdds"
  DdsQos      qos;
};

}  // namespace transport
