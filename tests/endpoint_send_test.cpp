// -----------------------------------------------------------------------------
// endpoint_send_test.cpp — Endpoint 统一寻址发送
// 覆盖:基类默认实现(kDefault 退化/其余 io: Fail)、UDP kNet、DDS kTopic、
// 基类句柄多态寻址、具体类型上重载不被名字隐藏。
// -----------------------------------------------------------------------------

#include "transport/Endpoint.hpp"
#include "transport/ITransport.hpp"
#include "transport/dds/DdsImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"

#include "dds/FakeDdsProvider.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace transport;

namespace {
void WaitFor(const std::function<bool()>& pred, int ms = 2000) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
  while (!pred() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(Endpoint, FactoriesProduceCorrectKinds) {
  auto d = Endpoint::Default();
  EXPECT_EQ(d.kind, Endpoint::Kind::kDefault);
  auto n = Endpoint::Net("10.0.0.7", 9000);
  EXPECT_EQ(n.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(n.host, "10.0.0.7");
  EXPECT_EQ(n.port, 9000);
  auto t = Endpoint::Topic("cmd");
  EXPECT_EQ(t.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(t.topic, "cmd");
}

// TCP 不覆写寻址重载:kNet/kTopic → io: Fail;kDefault 经真实回环退化等价 Send(data)。
TEST(EndpointSend, TcpDefaultDegradesAndAddressedFails) {
  TcpServerConfig sc;
  sc.bind_addr = "127.0.0.1";
  sc.port = 0;
  auto server = std::make_shared<TcpServerImpl>(sc);
  std::atomic<int> conns{0};
  std::shared_ptr<ITransport> server_side;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    server_side = std::move(c);
    ++conns;
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = std::make_shared<TcpClientImpl>(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  // kDefault → 退化 Send(data),消息真实送达
  ASSERT_TRUE(static_cast<bool>(client->Send({1, 2, 3}, Endpoint::Default())));
  auto r = server_side->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));

  // 寻址种类 → io: Fail(基类默认实现)
  auto st_net = client->Send({1}, Endpoint::Net("127.0.0.1", 1));
  EXPECT_FALSE(static_cast<bool>(st_net));
  EXPECT_EQ(st_net.error.rfind("io:", 0), 0u);
  auto st_topic = client->Send({1}, Endpoint::Topic("x"));
  EXPECT_FALSE(static_cast<bool>(st_topic));
  EXPECT_EQ(st_topic.error.rfind("io:", 0), 0u);

  client->Close();
  server->Close();
}

// 基类句柄上的两个重载均可见可调(接口层面,不依赖具体传输)
TEST(EndpointSend, BothOverloadsCallableViaBaseHandle) {
  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = 1;  // 不连接,只验证可调性
  cc.auto_reconnect = false;
  std::shared_ptr<ITransport> t = std::make_shared<TcpClientImpl>(cc);
  EXPECT_FALSE(static_cast<bool>(t->Send({1})));                     // 未打开
  EXPECT_FALSE(static_cast<bool>(t->Send({1}, Endpoint::Topic("x"))));
}

namespace {
UdpConfig UnicastCfg(const std::string& remote, uint16_t rport) {
  UdpConfig c;
  c.local_addr = "127.0.0.1";
  c.local_port = 0;
  c.remote_addr = remote;
  c.remote_port = rport;
  return c;
}
}  // namespace

// 基类句柄 + Endpoint::Net 寻址,真实回环送达 —— 本设计的核心收益
TEST(EndpointSend, UdpNetAddressingViaBaseHandle) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  std::shared_ptr<ITransport> tx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(
      tx->Send({4, 5}, Endpoint::Net("127.0.0.1", rport))));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 5}));
  tx->Close();
  rx->Close();
}

TEST(EndpointSend, UdpRejectsTopicEndpoint) {
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", 9999));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1}, Endpoint::Topic("x"));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(EndpointSend, UdpDefaultEndpointUsesConfigRemote) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rx->LocalPort()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Send({7}, Endpoint::Default())));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7}));
  tx->Close();
  rx->Close();
}

namespace {
DdsConfig DdsPubSubCfg(std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = DdsMode::kPubSub;
  c.topics = std::move(topics);
  return c;
}
std::shared_ptr<DdsImpl> MakeDds(std::shared_ptr<FakeDdsProvider::Bus> bus,
                                 DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}
}  // namespace

// 基类句柄 + Endpoint::Topic 寻址,经 FakeDdsProvider 总线送达订阅方
TEST(EndpointSend, DdsTopicAddressingViaBaseHandle) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<ITransport> tx = MakeDds(bus, DdsPubSubCfg({"a"}));
  auto rx = MakeDds(bus, DdsPubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({9}, Endpoint::Topic("b"))));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{9}));
}

TEST(EndpointSend, DdsRejectsNetEndpoint) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = MakeDds(bus, DdsPubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(t->Open()));
  auto st = t->Send({1}, Endpoint::Net("127.0.0.1", 1));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(EndpointSend, DdsDefaultEndpointUsesFirstTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = MakeDds(bus, DdsPubSubCfg({"t0"}));
  auto rx = MakeDds(bus, DdsPubSubCfg({"t0"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t0")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({3}, Endpoint::Default())));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.topic, "t0");
}
