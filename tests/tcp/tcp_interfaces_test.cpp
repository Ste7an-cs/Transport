#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

#include <optional>
#include <type_traits>

#include <gtest/gtest.h>

TEST(TcpConfig, ClientDefaults) {
  transport::TcpClientConfig c;
  EXPECT_EQ(c.port, 0);
  EXPECT_EQ(c.connect_timeout_ms, 5000u);
  EXPECT_TRUE(c.auto_reconnect);
  EXPECT_FALSE(c.framer.has_value());
}

TEST(TcpConfig, ServerDefaults) {
  transport::TcpServerConfig c;
  EXPECT_EQ(c.bind_addr, "0.0.0.0");
  EXPECT_EQ(c.max_clients, 10);
  EXPECT_FALSE(c.framer.has_value());
}

TEST(TcpConfig, ITcpServerIsAbstractTransport) {
  // 编译期确认 ITcpServer 继承自 ITransport
  EXPECT_TRUE((std::is_base_of<transport::ITransport, transport::ITcpServer>::value));
}
