#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/coro/Cancellation.hpp"

namespace transport::coro {

struct Datagram {
  std::vector<std::uint8_t> bytes;
  Endpoint source;
};

struct SendUnit {
  std::vector<std::uint8_t> bytes;
  Endpoint destination;
};

enum class LifecycleState { kCreated, kRunning, kClosing, kClosed };

struct OperationOptions {
  using Clock = std::chrono::steady_clock;
  std::optional<Clock::time_point> deadline;
  CancellationToken cancellation;
};

}  // namespace transport::coro
