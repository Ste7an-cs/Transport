#include "transport/TransportFactory.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/dds/DdsProviderRegistry.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"

using namespace transport;

namespace {
void WaitFor(std::function<bool()> pred, int ms = 2000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(FactoryCreate, TypedOverloadsReturnConcreteImpls) {
  TcpClientConfig tc;
  tc.host = "127.0.0.1";
  tc.port = 1;
  auto t1 = TransportFactory::Create(tc);
  ASSERT_NE(t1, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpClientImpl>(t1), nullptr);

  TcpServerConfig sc;
  auto t2 = TransportFactory::Create(sc);
  ASSERT_NE(t2, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpServerImpl>(t2), nullptr);

  UdpConfig uc;
  auto t3 = TransportFactory::Create(uc);
  ASSERT_NE(t3, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<UdpImpl>(t3), nullptr);

  DdsConfig dc;
  dc.topics = {"t"};
  auto t4 = TransportFactory::Create(dc);
  ASSERT_NE(t4, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<DdsImpl>(t4), nullptr);

  SerialConfig se;
  se.device = "/dev/null";
  auto t5 = TransportFactory::Create(se);
  ASSERT_NE(t5, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SerialImpl>(t5), nullptr);
}

TEST(FactoryCreate, TcpLoopbackSmoke) {
  TcpServerConfig sc;
  sc.bind_addr = "127.0.0.1";
  sc.port = 0;
  auto server_i = TransportFactory::Create(sc);
  auto server = std::dynamic_pointer_cast<TcpServerImpl>(server_i);
  ASSERT_NE(server, nullptr);
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = TransportFactory::Create(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  ASSERT_TRUE(static_cast<bool>(server->Send({4, 2})));  // 广播
  auto r = client->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 2}));
  client->Close();
  server->Close();
}

#ifdef TRANSPORT_HAS_FASTDDS
TEST(FactoryCreate, DdsCreateEnsuresFastDdsRegistered) {
  DdsConfig dc;
  dc.topics = {"t"};
  (void)TransportFactory::Create(dc);  // 应触发 RegisterFastDdsProvider()
  auto p = DdsProviderRegistry::Create("FastDDS");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "FastDDS");
}
#endif
