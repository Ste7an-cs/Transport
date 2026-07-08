#pragma once
#include <QCoreApplication>
#include <QDeadlineTimer>
#include <QEventLoop>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <vector>

namespace qtutil {
// 泵 Qt 事件直到 pred() 为真或超时;返回 pred 最终是否满足。
inline bool pumpUntil(std::function<bool()> pred, int timeout_ms = 2000) {
  QDeadlineTimer dl(timeout_ms);
  while (!pred() && !dl.hasExpired())
    QCoreApplication::processEvents(QEventLoop::AllEvents, 10);
  return pred();
}
inline std::vector<uint8_t> B(std::initializer_list<uint8_t> l) { return std::vector<uint8_t>(l); }
}  // namespace qtutil
