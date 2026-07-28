#pragma once
#include <chrono>
#include <functional>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

namespace testutil {

// 在 fiber 里让出并推进时间,直到 pred() 为真或超过 budget_ms。sleep_for 既让出其他 fiber、
// 又推进 fiber 调度器时钟(供 await_for 超时),避免忙等。
inline bool pumpFiberUntil(std::function<bool()> pred, int budget_ms = 3000) {
  for (int i = 0; i < budget_ms && !pred(); ++i)
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
  return pred();
}

}  // namespace testutil
