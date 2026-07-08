#include "transport/tcp/TcpConnection.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <QTcpServer>
#include <QTcpSocket>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpConnection;
using qtutil::pumpUntil; using qtutil::B;

// 用一个 QTcpServer 造一对已连接 socket,验证 TcpConnection 收发。
TEST(TcpConnection, LoopbackRoundtrip) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  quint16 port = server.serverPort();

  QTcpSocket* client = new QTcpSocket();
  client->connectToHost(QHostAddress::LocalHost, port);
  ASSERT_TRUE(pumpUntil([&]{ return server.hasPendingConnections() &&
                                     client->state() == QAbstractSocket::ConnectedState; }));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);

  auto conn = std::make_shared<TcpConnection>(accepted);
  std::vector<uint8_t> got;
  conn->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));

  client->write("hi", 2); client->flush();
  EXPECT_TRUE(pumpUntil([&]{ return got.size() == 2; }));
  EXPECT_EQ(got, B({'h', 'i'}));

  // conn->Send 回去,client 收到
  QByteArray back;
  QObject::connect(client, &QTcpSocket::readyRead, [&]{ back += client->readAll(); });
  ASSERT_TRUE(static_cast<bool>(conn->Send(B({'y', 'o'}))));
  EXPECT_TRUE(pumpUntil([&]{ return back.size() == 2; }));
  EXPECT_EQ(back, QByteArray("yo"));

  conn->Close();
  client->close(); client->deleteLater();
}

TEST(TcpConnection, PeerCloseTriggersDisconnect) {
  QTcpServer server; ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  QTcpSocket* client = new QTcpSocket();
  client->connectToHost(QHostAddress::LocalHost, server.serverPort());
  ASSERT_TRUE(pumpUntil([&]{ return server.hasPendingConnections(); }));
  auto conn = std::make_shared<TcpConnection>(server.nextPendingConnection());
  bool disc = false;
  conn->OnDisconnect([&](const std::string&){ disc = true; });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  client->disconnectFromHost();               // 对端主动断开
  EXPECT_TRUE(pumpUntil([&]{ return disc; }));  // 真断开 → OnDisconnect 一次
  client->deleteLater();
}
