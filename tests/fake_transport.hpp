#pragma once

// 测试用假传输:实现重设计后的 ITransport(七个方法),把 I/O 换成可直接驱动的内存队列。
//
// 与真实传输的差别仅在于**数据从哪来、到哪去**:入站由 `Deliver()` 直接投进读队列,
// 出站落在 `sent()` 里供断言。生命周期语义与真实传输一致——`Close()` 只发信号并关闭读
// 队列,`WaitClosed()` 因无内部 fiber 而立即返回。
//
// 它同时用于验证一条新契约:**节点不管传输的生命周期**。宿主启停传输,节点只借用它;
// 故 `Start()` / `Close()` 的调用次数在此可观测。

#include <cstdint>
#include <memory>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/fiber/channel_op_status.hpp>

#include "await/awaitable.hpp"
#include "detail/result.hpp"

#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"

namespace testutil {

class FakeTransport final : public transport::ITransport {
 public:
  Coro::Result<void> Start() override {
    if (lifecycle_ == transport::LifecycleState::kRunning) {
      return Coro::Result<void>{};
    }
    if (lifecycle_ != transport::LifecycleState::kCreated) {
      return transport::make_error_code(transport::TransportErrc::kInvalidState);
    }
    ++start_count_;
    lifecycle_ = transport::LifecycleState::kRunning;
    return Coro::Result<void>{};
  }

  std::shared_ptr<Coro::Awaitable<transport::Datagram>> AsyncRead() override {
    if (lifecycle_ == transport::LifecycleState::kCreated) {
      return transport::ClosedQueue<transport::Datagram>(
          transport::make_error_code(transport::TransportErrc::kInvalidState));
    }
    return read_queue_;
  }

  Coro::Result<void> AsyncWrite(transport::Datagram datagram) override {
    if (lifecycle_ == transport::LifecycleState::kCreated) {
      return transport::make_error_code(transport::TransportErrc::kInvalidState);
    }
    if (lifecycle_ != transport::LifecycleState::kRunning) {
      return transport::make_error_code(transport::TransportErrc::kClosed);
    }
    sent_.push_back(std::move(datagram));
    return Coro::Result<void>{};
  }

  Coro::Result<void> Close() override {
    if (lifecycle_ >= transport::LifecycleState::kClosing) {
      return Coro::Result<void>{};
    }
    ++close_count_;
    lifecycle_ = transport::LifecycleState::kClosed;
    transport::CloseQueue(
        read_queue_, transport::make_error_code(transport::TransportErrc::kClosed));
    return Coro::Result<void>{};
  }

  /// 无内部 fiber,故无可汇合者。
  void WaitClosed() override {}

  [[nodiscard]] std::error_code LastError() const override { return last_error_; }

  [[nodiscard]] transport::LinkState CurrentLinkState() const override {
    return lifecycle_ == transport::LifecycleState::kRunning
               ? transport::LinkState::kUp
               : transport::LinkState::kDown;
  }

  // —— 测试驱动面 ————————————————————————————————————————————————————

  /// 把一段字节当作一条入站报文投进读队列。
  bool Deliver(std::vector<std::uint8_t> bytes) {
    return read_queue_->channel()->push(
               transport::Datagram{std::move(bytes), transport::Endpoint::Default()}) ==
           boost::fibers::channel_op_status::success;
  }

  /// 已受理的出站报文,按发出顺序。
  [[nodiscard]] const std::vector<transport::Datagram>& sent() const { return sent_; }
  [[nodiscard]] std::size_t start_count() const { return start_count_; }
  [[nodiscard]] std::size_t close_count() const { return close_count_; }
  [[nodiscard]] bool running() const {
    return lifecycle_ == transport::LifecycleState::kRunning;
  }

 private:
  transport::LifecycleState lifecycle_{transport::LifecycleState::kCreated};
  std::shared_ptr<Coro::Awaitable<transport::Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<transport::Datagram>>()};
  std::vector<transport::Datagram> sent_;
  std::error_code last_error_;
  std::size_t start_count_{0};
  std::size_t close_count_{0};
};

}  // namespace testutil
