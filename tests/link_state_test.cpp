// 链路可用性(RT_TRANSPORT_009 / ADR-0004 D2 / SDD DD-7)跨介质同形契约测试。
//
// 每种介质在**同一组相位**上取值:Start 前 → kDown;Running 且链路就绪 → kUp;
// 关闭后 → kDown。具连接管理的 TcpClientTransport 另有建立中相位 → kEstablishing。
// 各介质用与其既有测试相同的确定化手段:TCP/UDP 走本机回环、串口走 PTY、DDS 走
// FakeDdsProvider 进程内总线、交互层替身走 FakeCoroTransport。
#include <pty.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/serial/SerialConfig.hpp"
#include "transport/io/serial/SerialTransport.hpp"
#include "transport/io/tcp/TcpClientConfig.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/io/udp/UdpConfig.hpp"
#include "transport/io/udp/UdpTransport.hpp"

using namespace std::chrono_literals;
using transport::ConnectionState;
using transport::DdsConfig;
using transport::DdsTransport;
using transport::FakeDdsProvider;
using transport::LinkState;
using transport::OperationOptions;
using transport::SerialConfig;
using transport::SerialTransport;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpTransport;
using transport::UdpConfig;
using transport::UdpTransport;
using Clock = OperationOptions::Clock;

namespace {

constexpr char kLoopback[] = "127.0.0.1";

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = Clock::now() + d;
  return o;
}

// 一对已连接的回环 socket(客户端 + 服务端已接受),同 tcp_transport_read_test。
bool MakeConnectedPair(QTcpServer& server, QTcpSocket*& client,
                       QTcpSocket*& accepted) {
  if (!server.listen(QHostAddress::LocalHost, 0)) {
    return false;
  }
  client = new QTcpSocket();
  auto connected =
      Coro::coro(client).connectToHost(QHostAddress::LocalHost,
                                       server.serverPort());
  if (!Coro::await_for(connected, 2s)) {
    return false;
  }
  if (!testutil::pumpFiberUntil([&] { return server.hasPendingConnections(); },
                                2000)) {
    return false;
  }
  accepted = server.nextPendingConnection();
  if (!accepted) {
    return false;
  }
  accepted->setParent(nullptr);  // 交由 TcpTransport 管理生命周期。
  return true;
}

// 一对 PTY:master 为测试侧对端 fd,slave_name 为被测串口打开的 /dev/pts/N。
struct PtyPair {
  int master = -1;
  std::string slave_name;
  bool ok = false;
};

PtyPair MakePty() {
  PtyPair p;
  int slave = -1;
  char name[256] = {0};
  if (openpty(&p.master, &slave, name, nullptr, nullptr) != 0) {
    return p;
  }
  ::close(slave);  // 由 QSerialPort 按名字重新打开 slave。
  p.slave_name = name;
  p.ok = true;
  return p;
}

}  // namespace

// —— TcpTransport(已接受/已连接 socket):无建立相位,连接存续即 kUp ——

TEST(LinkState, TcpTransportDownBeforeStartUpWhileRunningDownAfterClose) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport transport(accepted);
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);  // 未 Start。
  ASSERT_TRUE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(transport.RequestClose());
  ASSERT_TRUE(transport.WaitClosed(Deadline(2000ms)));
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);

  client->deleteLater();
}

// 对端断开(非我方关闭):生命周期仍在 Running(本类不重连也不自闭),但链路已不
// 存续,须如实报 kDown。
TEST(LinkState, TcpTransportDownAfterPeerDisconnect) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport transport(accepted);
  ASSERT_TRUE(transport.Start());
  ASSERT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  client->abort();  // 对端消失。
  client->deleteLater();
  EXPECT_TRUE(testutil::pumpFiberUntil(
      [&] { return transport.CurrentLinkState() == LinkState::kDown; }, 3000));
}

// —— TcpClientTransport(连接管理):Connecting/Reconnecting → kEstablishing ——

TEST(LinkState, TcpClientEstablishingThenUpThenDownOnClose) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientConfig cfg;
  cfg.host = kLoopback;
  cfg.port = server.serverPort();
  cfg.connect_timeout = 400ms;
  cfg.reconnect_interval = 30ms;

  TcpClientTransport client(cfg);
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kDown);  // 未 Start。

  ASSERT_TRUE(client.Start());
  // Start 立即进 Connecting(不等首次连上)→ 建立中。
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kEstablishing);

  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(client.RequestClose());
  ASSERT_TRUE(client.WaitClosed(Deadline(3000ms)));
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kDown);
}

// 连不上的端点:反复 Connecting/Reconnecting——链路始终"建立中",既非 kUp 亦非
// kDown(策略本身——退避多久、还试几次——不经本查询暴露)。
TEST(LinkState, TcpClientStaysEstablishingWhileReconnecting) {
  QTcpServer probe;
  ASSERT_TRUE(probe.listen(QHostAddress::LocalHost, 0));
  const quint16 dead_port = probe.serverPort();
  probe.close();  // 端口空出:后续连接被拒。

  TcpClientConfig cfg;
  cfg.host = kLoopback;
  cfg.port = dead_port;
  cfg.connect_timeout = 200ms;
  cfg.reconnect_interval = 20ms;

  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());
  // 至少经历一次失败后的退避(Reconnecting),期间仍报 kEstablishing。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.State() == ConnectionState::kReconnecting; }, 3000));
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kEstablishing);

  ASSERT_TRUE(client.RequestClose());
  ASSERT_TRUE(client.WaitClosed(Deadline(3000ms)));
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kDown);
}

// —— UdpTransport:已绑定即可用 ——

TEST(LinkState, UdpDownBeforeStartUpWhenBoundDownAfterClose) {
  UdpConfig cfg;
  cfg.mode = transport::UdpMode::kUnicast;
  cfg.local_addr = kLoopback;
  cfg.local_port = 0;  // OS 分配临时端口。

  UdpTransport transport(cfg);
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);  // 未绑定。
  ASSERT_TRUE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(transport.RequestClose());
  ASSERT_TRUE(transport.WaitClosed(Deadline(2000ms)));
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
}

// bind 失败(端口被独占绑定)不进 Running:链路仍不可用。
TEST(LinkState, UdpDownWhenBindFails) {
  UdpConfig first;
  first.mode = transport::UdpMode::kUnicast;
  first.local_addr = kLoopback;
  first.local_port = 0;
  UdpTransport holder(first);
  ASSERT_TRUE(holder.Start());

  UdpConfig conflicting = first;
  conflicting.local_port = holder.LocalPort();  // 与已绑定端口冲突。
  UdpTransport transport(conflicting);
  EXPECT_FALSE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);

  ASSERT_TRUE(holder.RequestClose());
}

// —— SerialTransport:设备已打开即可用 ——

TEST(LinkState, SerialDownBeforeStartUpWhenOpenDownAfterClose) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);

  SerialConfig cfg;
  cfg.device = pty.slave_name;
  cfg.baud_rate = 115200;

  SerialTransport transport(cfg);
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);  // 设备未打开。
  ASSERT_TRUE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(transport.RequestClose());
  ASSERT_TRUE(transport.WaitClosed(Deadline(2000ms)));
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);

  ::close(pty.master);
}

// 设备不存在 → Start 失败,链路不可用。
TEST(LinkState, SerialDownWhenDeviceCannotOpen) {
  SerialConfig cfg;
  cfg.device = "/dev/does-not-exist-transport-test";
  SerialTransport transport(cfg);
  EXPECT_FALSE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
}

// —— DdsTransport:provider 已 Init 且订阅完成即可用 ——

TEST(LinkState, DdsDownBeforeStartUpWhenSubscribedDownAfterClose) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  DdsConfig cfg;
  cfg.domain_id = 0;
  DdsTransport transport(std::make_unique<FakeDdsProvider>(bus), cfg,
                         std::vector<std::string>{"topic/a"});

  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);  // 未 Init/订阅。
  ASSERT_TRUE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(transport.RequestClose());
  ASSERT_TRUE(transport.WaitClosed(Deadline(2000ms)));
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
}

// —— FakeCoroTransport(测试替身):缺省与生命周期同调 + 可注入 ——

TEST(LinkState, FakeTransportFollowsLifecycleByDefault) {
  testutil::FakeCoroTransport transport;
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
  ASSERT_TRUE(transport.Start());
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);
  ASSERT_TRUE(transport.RequestClose());
  ASSERT_TRUE(transport.WaitClosed(Deadline(1000ms)));
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
}

TEST(LinkState, FakeTransportLinkStateIsInjectable) {
  testutil::FakeCoroTransport transport;
  ASSERT_TRUE(transport.Start());
  ASSERT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  // 注入:模拟"实例仍在 Running,但链路建立中/已不可用"。
  transport.SetLinkState(LinkState::kEstablishing);
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kEstablishing);
  transport.SetLinkState(LinkState::kDown);
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);

  transport.ClearLinkState();  // 撤销注入 → 恢复缺省口径。
  EXPECT_EQ(transport.CurrentLinkState(), LinkState::kUp);

  ASSERT_TRUE(transport.RequestClose());
}
