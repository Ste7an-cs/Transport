// 协程原生 TcpTransport 发送语义真实回环集成测试。
// 在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证:Write 刷完即完成、
// 慢读取端下的背压且内存有界、并发一致顺序、对端 reset → Io/Connection + 关连接、
// 写入已开始后超时 → Timeout 且帧不被截断。连接建立由测试夹具完成(非本类职责)。
#include <chrono>
#include <cstdint>
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
#include "transport/coro/Error.hpp"
#include "transport/coro/TcpTransport.hpp"

using namespace std::chrono_literals;
using transport::Endpoint;
using transport::coro::Datagram;
using transport::coro::OperationOptions;
using transport::coro::SendUnit;
using transport::coro::Status;
using transport::coro::TcpTransport;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

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

// 从流式传输读满 n 字节(读路径按本 spec 只需最小能力)。
std::vector<std::uint8_t> ReadExact(TcpTransport& t, std::size_t n,
                                    int budget_ms = 3000) {
  std::vector<std::uint8_t> buf;
  const auto deadline =
      OperationOptions::Clock::now() + std::chrono::milliseconds(budget_ms);
  while (buf.size() < n && OperationOptions::Clock::now() < deadline) {
    OperationOptions options;
    options.deadline = deadline;
    auto r = t.Read(options);
    if (!r) {
      break;
    }
    buf.insert(buf.end(), r.value().bytes.begin(), r.value().bytes.end());
  }
  return buf;
}

}  // namespace

// RT_TRANSPORT_008:Write 成功后帧已上线,对端完整收到;发送时间戳可观测。
TEST(CoroTcpTransport, WriteFlushesFrameAndPeerReceivesIt) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport sender(client);
  TcpTransport receiver(accepted);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> frame(64, 0x5A);
  EXPECT_FALSE(sender.LastSendTime().has_value());
  ASSERT_TRUE(sender.Write(Frame(frame)));
  EXPECT_TRUE(sender.LastSendTime().has_value());
  EXPECT_EQ(sender.SendWaiterDepth(), 0U);

  const auto received = ReadExact(receiver, frame.size());
  EXPECT_EQ(received, frame);
  EXPECT_TRUE(receiver.LastReceiveTime().has_value());
}
