#include "transport/ITransport.hpp"
#include "transport/udp/UdpConfig.hpp"
#include "transport/udp/UdpImpl.hpp"

#include <type_traits>

#include <gtest/gtest.h>

TEST(UdpConfig, Defaults) {
  transport::UdpConfig c;
  EXPECT_TRUE(c.mode == transport::UdpMode::kUnicast);
  EXPECT_EQ(c.local_addr, "0.0.0.0");
  EXPECT_EQ(c.local_port, 0);
  EXPECT_EQ(c.remote_port, 0);
  EXPECT_EQ(c.ttl, 1);
}

TEST(UdpConfig, UdpImplIsTransport) {
  EXPECT_TRUE((std::is_base_of<transport::ITransport,
                               transport::UdpImpl>::value));
}
