#include "transport/tcp/TcpClientTransport.hpp"
#include "transport/tcp/TcpServerTransport.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <vector>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpServerConfig;
using transport::TcpServerTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(TcpClientTransport, ClientServerRoundtrip) {
  TcpServerConfig scfg; scfg.bind_addr = "127.0.0.1"; scfg.port = 0;
  auto server = std::make_shared<TcpServerTransport>(scfg);
  std::vector<std::shared_ptr<ITransport>> keep;
  std::vector<uint8_t> srv_got;
  server->OnAccept([&](std::shared_ptr<ITransport> c) {
    // 内层按值捕获 c(shared_ptr):OnAccept 的 c 形参在回调返回后即销毁,
    // 按引用捕获会在字节到达时悬垂 → 段错误。
    c->OnBytes([&srv_got, c](Result<std::vector<uint8_t>> r, const std::string&){ if(r){ srv_got = r.value; (void)c->Send(r.value); } });  // 回显
    (void)c->Open(); keep.push_back(c);
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig ccfg; ccfg.host = "127.0.0.1"; ccfg.port = server->LocalPort();
  auto client = std::make_shared<TcpClientTransport>(ccfg);
  std::vector<uint8_t> cli_got; bool connected = false;
  client->OnConnect([&]{ connected = true; });
  client->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) cli_got = r.value; });
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  EXPECT_TRUE(pumpUntil([&]{ return connected; }));

  ASSERT_TRUE(static_cast<bool>(client->Send(B({7, 8}))));
  EXPECT_TRUE(pumpUntil([&]{ return srv_got.size() == 2 && cli_got.size() == 2; }));
  EXPECT_EQ(srv_got, B({7, 8}));
  EXPECT_EQ(cli_got, B({7, 8}));  // 回显收到
  client->Close(); server->Close();
}

TEST(TcpClientTransport, ConnectRefusedFails) {
  TcpClientConfig ccfg; ccfg.host = "127.0.0.1"; ccfg.port = 1;  // 极可能拒连
  ccfg.connect_timeout_ms = 800; ccfg.auto_reconnect = false;
  auto client = std::make_shared<TcpClientTransport>(ccfg);
  bool disc = false;
  client->OnDisconnect([&](const std::string&){ disc = true; });
  auto st = client->Open();
  // Open 返回失败,或稍后经 OnDisconnect 上报;两者其一即可
  if (!st) { SUCCEED(); } else { EXPECT_TRUE(pumpUntil([&]{ return disc; }, 2000)); }
  client->Close();
}
