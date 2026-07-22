#include <chrono>
#include <type_traits>

#include <gtest/gtest.h>

#include "transport/coro/ITransport.hpp"

using transport::Endpoint;
using transport::coro::Datagram;
using transport::coro::ITransport;
using transport::coro::LifecycleState;
using transport::coro::OperationOptions;
using transport::coro::SendUnit;

TEST(CoroTransportContract, DataUnitsOwnBytesAndAddressing) {
  Datagram incoming{{1, 2, 3}, Endpoint::Net("127.0.0.1", 9000)};
  EXPECT_EQ(incoming.bytes, (std::vector<std::uint8_t>{1, 2, 3}));
  EXPECT_EQ(incoming.source.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(incoming.source.port, 9000);

  SendUnit outgoing{{4, 5}, Endpoint::Topic("events")};
  EXPECT_EQ(outgoing.destination.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(outgoing.destination.topic, "events");
}

TEST(CoroTransportContract, OptionsUseSteadyClockDeadline) {
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + std::chrono::seconds(1);
  EXPECT_TRUE(options.deadline.has_value());
  EXPECT_FALSE(options.cancellation);
}

TEST(CoroTransportContract, InterfaceIsInternalPolymorphicSeam) {
  static_assert(std::has_virtual_destructor_v<ITransport>);
  EXPECT_NE(LifecycleState::kCreated, LifecycleState::kClosed);
}
