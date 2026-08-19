#include <chrono>
#include <memory>
#include <type_traits>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "transport/io/ITransport.hpp"

using transport::Endpoint;
using transport::Datagram;
using transport::ITransport;
using transport::LifecycleState;
using transport::OperationOptions;
using transport::SendUnit;

TEST(CoroTransportContract, DataUnitsOwnBytesAndAddressing) {
  Datagram incoming{{1, 2, 3}, Endpoint::Net("127.0.0.1", 9000)};
  EXPECT_EQ(incoming.bytes, (std::vector<std::uint8_t>{1, 2, 3}));
  EXPECT_EQ(incoming.peer.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(incoming.peer.port, 9000);

  SendUnit outgoing{{4, 5}, Endpoint::Topic("events")};
  EXPECT_EQ(outgoing.destination.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(outgoing.destination.topic, "events");
}

TEST(CoroTransportContract, OptionsUseSteadyClockDeadline) {
  static_assert(std::is_same_v<OperationOptions::Clock, std::chrono::steady_clock>);

  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + std::chrono::seconds(1);
  EXPECT_TRUE(options.deadline.has_value());
  EXPECT_FALSE(options.cancellation);
}

TEST(CoroTransportContract, InterfaceIsInternalPolymorphicSeam) {
  static_assert(std::has_virtual_destructor_v<ITransport>);
  static_assert(std::is_same_v<decltype(&ITransport::Start),
                               Coro::Result<void> (ITransport::*)()>);
  // Read 交出 read_queue 的等待器句柄,且**不接受 OperationOptions**(ADR-0007 D4)。
  static_assert(
      std::is_same_v<decltype(&ITransport::Read),
                     std::shared_ptr<Coro::Awaitable<Datagram>> (
                         ITransport::*)()>);
  static_assert(std::is_same_v<decltype(&ITransport::Write),
                               Coro::Result<void> (ITransport::*)(SendUnit)>);
  static_assert(std::is_same_v<decltype(&ITransport::RequestClose),
                               Coro::Result<void> (ITransport::*)()>);
  static_assert(std::is_same_v<decltype(&ITransport::WaitClosed),
                               Coro::Result<void> (ITransport::*)(OperationOptions)>);
  EXPECT_NE(LifecycleState::kCreated, LifecycleState::kClosed);
}
