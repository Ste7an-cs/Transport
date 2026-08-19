// 协程原生 TcpTransport 读侧契约真实回环集成测试。
// 在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证读侧可观察契约:
// Datagram.peer 为对端地址、对端断开 → Closed(不重连,终止语义单一)、
// 我方 RequestClose → Closed、
// 带 deadline 的读超时 → Timeout 且不停流(可再读)、单读守卫已删除(并发第二个读者
// 不再被拒,ADR-0007 D4)。连接建立由测试夹具完成(非本类职责)。
// 逐读 cancellation 为 out-of-scope(循环级中断靠 RequestClose),本文件不覆盖。
#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/tcp/TcpTransport.hpp"

using namespace std::chrono_literals;
using transport::Datagram;
using transport::Endpoint;
using transport::OperationOptions;
using transport::SendUnit;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 建立一对已连接的回环 socket(客户端 + 服务端已接受);连接建立不在被测类职责。
bool MakeConnectedPair(QTcpServer& server, QTcpSocket*& client,
                       QTcpSocket*& accepted) {
  if (!server.listen(QHostAddress::LocalHost, 0)) {
    return false;
  }
  const quint16 port = server.serverPort();
  client = new QTcpSocket();
  auto connected =
      Coro::coro(client).connectToHost(QHostAddress::LocalHost, port);
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

SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}

}  // namespace

// 读侧契约:Datagram.peer 填为对端 Endpoint::Net(ip, port)(取自已连接 socket 的
// peerAddress/peerPort;TCP from 恒为对端)。
TEST(CoroTcpTransportRead, SourceIsPeerEndpoint) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  // receiver 接管服务端 accepted socket,其对端即客户端 client。
  const std::string expected_host = accepted->peerAddress().toString().toStdString();
  const std::uint16_t expected_port = accepted->peerPort();
  ASSERT_GT(expected_port, 0U);
  TcpTransport sender(client);
  TcpTransport receiver(accepted);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> frame(16, 0x5A);
  ASSERT_TRUE(sender.Write(Frame(frame)));

  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 3s;
  auto r = testutil::ReadOnce(receiver, options);
  ASSERT_TRUE(r) << r.error().message();
  EXPECT_EQ(r.value().peer.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(r.value().peer.host, expected_host);
  EXPECT_EQ(r.value().peer.port, expected_port);
}

// 读侧契约(RT_TRANSPORT_008 / ADR-0004 D1):本类不重连,连接终结即传输终结,故
// 对端断开 → 在途 Read 以 Closed 收敛(与我方关闭同一终止语义,调用方停止读取)。
TEST(CoroTcpTransportRead, PeerDisconnectYieldsClosed) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport receiver(accepted);
  ASSERT_TRUE(receiver.Start());

  std::error_code read_err;
  bool read_ok = true;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    auto r = testutil::ReadOnce(receiver, options);
    read_ok = static_cast<bool>(r);
    if (!r) {
      read_err = r.error();
    }
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让 Read 挂起在流上。
  client->disconnectFromHost();  // 对端正常关闭。

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kClosed));
  client->deleteLater();
}

// 读侧契约:我方 RequestClose → 在途 Read 以 Closed 收敛。
TEST(CoroTcpTransportRead, RequestCloseWakesPendingReadWithClosed) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport receiver(accepted);
  ASSERT_TRUE(receiver.Start());

  std::error_code read_err;
  bool read_ok = true;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    auto r = testutil::ReadOnce(receiver, options);
    read_ok = static_cast<bool>(r);
    if (!r) {
      read_err = r.error();
    }
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让 Read 挂起在流上。
  EXPECT_TRUE(receiver.RequestClose());  // 我方关闭。

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kClosed));
  client->deleteLater();
}

// 读侧契约:带 deadline 的 Read 超时 → Timeout,且不停流——超时后再次 Read 仍可拿到
// 后续到达的数据。
TEST(CoroTcpTransportRead, DeadlineTimeoutDoesNotStopStream) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport sender(client);
  TcpTransport receiver(accepted);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  // 无数据到达 → 短 deadline 的 Read 超时。
  OperationOptions timeout_opts;
  timeout_opts.deadline = OperationOptions::Clock::now() + 60ms;
  auto timed_out = testutil::ReadOnce(receiver, timeout_opts);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));

  // 流未停:后续数据到达后再次 Read 成功拿到。
  const std::vector<std::uint8_t> frame(16, 0x77);
  ASSERT_TRUE(sender.Write(Frame(frame)));
  OperationOptions read_opts;
  read_opts.deadline = OperationOptions::Clock::now() + 3s;
  auto again = testutil::ReadOnce(receiver, read_opts);
  ASSERT_TRUE(again) << again.error().message();
  std::vector<std::uint8_t> got(again.value().bytes.begin(),
                                again.value().bytes.end());
  // readAll 可能一次不满整帧,补读至满。
  while (got.size() < frame.size()) {
    OperationOptions more;
    more.deadline = OperationOptions::Clock::now() + 3s;
    auto r = testutil::ReadOnce(receiver, more);
    ASSERT_TRUE(r) << r.error().message();
    got.insert(got.end(), r.value().bytes.begin(), r.value().bytes.end());
  }
  EXPECT_EQ(got, frame);
  client->deleteLater();
}

// 读侧契约(ADR-0007 D4):单读守卫已删除——已有在途读者时并发第二个读者**不再被拒**,
// 两者同挂在 read_queue 上抢占式共读,无数据则各按自己的 deadline 以 kTimeout 收敛。
TEST(CoroTcpTransportRead, ConcurrentSecondReadIsNotRejected) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport receiver(accepted);
  ASSERT_TRUE(receiver.Start());

  // 第一个读者挂起在流上(无数据、带较短 deadline 便于收尾)。
  Coro::Awaitable<void> entered;
  bool first_ok = true;
  std::error_code first_err;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 300ms;
    auto r = testutil::ReadOnce(receiver, options);
    first_ok = static_cast<bool>(r);
    if (!r) {
      first_err = r.error();
    }
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让第一个读者先挂到 read_queue 上。

  // 并发第二个读者:不再返 kInvalidState,而是同样挂起、按自己的 deadline 超时。
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 200ms;
  auto second = testutil::ReadOnce(receiver, options);
  ASSERT_FALSE(second);
  EXPECT_NE(second.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kTimeout));

  // 收尾:第一个读者超时返回,不影响流语义。
  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(first_ok);
  EXPECT_EQ(first_err, make_error_code(TransportErrc::kTimeout));
  client->deleteLater();
}
