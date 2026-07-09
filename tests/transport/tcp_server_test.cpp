#include "transport/tcp/TcpServerTransport.hpp"
#include "transport/tcp/TcpConnection.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <vector>
#include <QHostAddress>
#include <QTcpSocket>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpServerConfig;
using transport::TcpServerTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(TcpServerTransport, AcceptsAndEchoes) {
  TcpServerConfig cfg; cfg.bind_addr = "127.0.0.1"; cfg.port = 0;
  auto server = std::make_shared<TcpServerTransport>(cfg);
  std::vector<std::shared_ptr<ITransport>> accepted;
  std::vector<uint8_t> srv_got;
  server->OnAccept([&](std::shared_ptr<ITransport> conn) {
    conn->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) srv_got = r.value; });
    (void)conn->Open();
    accepted.push_back(conn);  // 保活
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  uint16_t port = server->LocalPort();

  QTcpSocket client;
  client.connectToHost(QHostAddress::LocalHost, port);
  ASSERT_TRUE(pumpUntil([&]{ return !accepted.empty() &&
                                    client.state() == QAbstractSocket::ConnectedState; }));
  client.write("ab", 2); client.flush();
  EXPECT_TRUE(pumpUntil([&]{ return srv_got.size() == 2; }));
  EXPECT_EQ(srv_got, B({'a', 'b'}));
  server->Close();
}
