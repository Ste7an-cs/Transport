#include "transport/tcp/TcpServerTransport.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/tcp/TcpClientTransport.hpp"
#include "transport/tcp/TcpConnection.hpp"

using transport::ITransport;
using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpServerConfig;
using transport::TcpServerTransport;

namespace {

TcpServerConfig ServerCfg() {
  TcpServerConfig c;
  c.bind_addr = "127.0.0.1";
  c.port = 0;  // OS 分配
  return c;
}

std::shared_ptr<TcpClientTransport> MakeClient(uint16_t port) {
  TcpClientConfig c;
  c.host = "127.0.0.1";
  c.port = port;
  c.connect_timeout_ms = 1000;
  c.auto_reconnect = false;
  return std::make_shared<TcpClientTransport>(c);
}

void WaitFor(std::function<bool()> pred, int ms = 1000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}

}  // namespace

TEST(TcpServer, AcceptsConnectionAndDeliversPerClient) {
  auto server = std::make_shared<TcpServerTransport>(ServerCfg());
  std::atomic<int> conns{0};
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    accepted = c;
    ++conns;
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto client = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });
  ASSERT_EQ(conns.load(), 1);
  ASSERT_TRUE(accepted != nullptr);

  // client → server：accepted 端收到
  ASSERT_TRUE(static_cast<bool>(client->Send({5, 6, 7})));
  auto r = accepted->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{5, 6, 7}));

  client->Close();
  server->Close();
}

TEST(TcpServer, BroadcastSendReachesAllClients) {
  auto server = std::make_shared<TcpServerTransport>(ServerCfg());
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto c1 = MakeClient(server->LocalPort());
  auto c2 = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(c1->Open()));
  ASSERT_TRUE(static_cast<bool>(c2->Open()));
  WaitFor([&] { return conns.load() >= 2; });
  ASSERT_EQ(conns.load(), 2);

  ASSERT_TRUE(static_cast<bool>(server->Send({9, 9})));
  auto r1 = c1->Receive(1000);
  auto r2 = c2->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r1));
  ASSERT_TRUE(static_cast<bool>(r2));
  EXPECT_EQ(r1.value.payload, (std::vector<uint8_t>{9, 9}));
  EXPECT_EQ(r2.value.payload, (std::vector<uint8_t>{9, 9}));

  c1->Close();
  c2->Close();
  server->Close();
}

TEST(TcpServer, GetClientsAndDisconnectClient) {
  auto server = std::make_shared<TcpServerTransport>(ServerCfg());
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) { accepted = c; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto client = MakeClient(server->LocalPort());
  bool dropped = false;
  client->OnDisconnect([&](const std::string&) { dropped = true; });
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return server->GetClients().size() >= 1 && accepted != nullptr; });
  ASSERT_EQ(server->GetClients().size(), 1u);

  // accepted 实为 TcpConnection；其 PeerId() 即 server 端登记的 client_id
  auto conn = std::dynamic_pointer_cast<transport::TcpConnection>(accepted);
  ASSERT_TRUE(conn != nullptr);
  server->DisconnectClient(conn->PeerId());

  WaitFor([&] { return server->GetClients().empty(); });
  EXPECT_TRUE(server->GetClients().empty());
  WaitFor([&] { return dropped; });
  EXPECT_TRUE(dropped);  // 被断开的客户端感知到 conn 断连

  client->Close();
  server->Close();
}

TEST(TcpServer, ReceiveOnServerReturnsConfigError) {
  auto server = std::make_shared<TcpServerTransport>(ServerCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  auto r = server->Receive(10);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u);
  auto fut = server->AsyncReceive();
  auto fr = fut.get();
  EXPECT_FALSE(static_cast<bool>(fr));
  EXPECT_EQ(fr.error.rfind("config:", 0), 0u);
  server->Close();
}

TEST(TcpServer, MaxClientsRejectsExtra) {
  auto cfg = ServerCfg();
  cfg.max_clients = 1;
  auto server = std::make_shared<TcpServerTransport>(cfg);
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  auto c1 = MakeClient(server->LocalPort());
  ASSERT_TRUE(static_cast<bool>(c1->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  auto c2 = MakeClient(server->LocalPort());
  (void)c2->Open();  // 连接可能 TCP 层成功，但会被立即关闭
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(conns.load(), 1);  // 第二个不触发 OnNewConnection

  c1->Close();
  c2->Close();
  server->Close();
}
