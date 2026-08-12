#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <system_error>
#include <utility>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

#include "await/awaitable.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"

namespace testutil {

// 在 fiber 里让出并推进时间,直到 pred() 为真或超过 budget_ms。sleep_for 既让出其他 fiber、
// 又推进 fiber 调度器时钟(供 await_for 超时),避免忙等。
inline bool pumpFiberUntil(std::function<bool()> pred, int budget_ms = 3000) {
  for (int i = 0; i < budget_ms && !pred(); ++i)
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
  return pred();
}

// 在 read_queue 句柄上取一次(测试便利件,ADR-0007 D4 后的读法)。
//
// `ITransport::Read()` 只交出句柄,deadline 由调用方自理;本 helper 把"取一次 + 把
// AsyncTask 的 timed_out 折算成 kTimeout"这段样板收口,使各传输用例保持逐条断言不变。
// 队列终止(被 close 并带终止原因)时原样透出该终止错误(通常是 kClosed)。
inline transport::Result<transport::Datagram> AwaitRead(
    const std::shared_ptr<Coro::Awaitable<transport::Datagram>>& queue,
    transport::OperationOptions options = {}) {
  Coro::Result<transport::Datagram, std::error_code> r =
      options.deadline
          ? Coro::await_for(queue, *options.deadline -
                                       transport::OperationOptions::Clock::now())
          : Coro::await(queue);
  if (r) {
    return transport::Result<transport::Datagram>{std::move(r).value()};
  }
  if (r.error() == std::make_error_code(std::errc::timed_out)) {
    return transport::make_error_code(transport::TransportErrc::kTimeout);
  }
  if (r.error().category() == transport::transport_error_category()) {
    return r.error();  // 终止原因原样透出(kClosed / kInvalidState 等)。
  }
  return transport::make_error_code(transport::TransportErrc::kInternal);
}

// 便利重载:直接对传输取一次(等价于 `AwaitRead(t.Read(), options)`)。
inline transport::Result<transport::Datagram> ReadOnce(
    transport::ITransport& transport,
    transport::OperationOptions options = {}) {
  return AwaitRead(transport.Read(), std::move(options));
}

}  // namespace testutil
