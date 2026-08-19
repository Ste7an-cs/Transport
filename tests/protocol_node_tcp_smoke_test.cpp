// -----------------------------------------------------------------------------
// protocol_node_tcp_smoke_test.cpp — P2-4 真实 TCP 回环冒烟(折入本票 = 原 P2-5)
//
// 端到端(真实单机 TCP 回环,复用 tcp_transport_send/read 的 socket 对范式):
//   · 请求方 = 真 ProtocolNode(真 TcpTransport(client) + SystemCodec)配入站 handler。
//   · 对端  = 裸 harness(真 TcpTransport(accepted) + SystemCodec),主动发一个业务帧过来。
//   路径:对端发业务帧 → node handler 收到 → handler 经 ctx.Send 回 noresponse 帧 → 对端
//   收到;再优雅 node.Close() → 端到端收敛、物理连接撕掉(对端 Read 观测 kConnection 退出)。
// 全部在 coro_test_main 单 fiber 调度器内协作,主 fiber 以 pumpFiberUntil 驱动。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
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
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::Endpoint;
using transport::FrameType;
using transport::HandlerContext;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SendUnit;
using transport::SystemCodec;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 建立一对已连接的回环 socket(客户端 + 服务端已接受);复用 loopback 夹具范式。
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
  if (!pumpFiberUntil([&] { return server.hasPendingConnections(); }, 2000)) {
    return false;
  }
  accepted = server.nextPendingConnection();
  if (!accepted) {
    return false;
  }
  accepted->setParent(nullptr);  // 交由 TcpTransport 管理生命周期。
  return true;
}

}  // namespace

// RT_HANDLER / RT_LIFECYCLE:真实 TCP 上 handler 收对端业务帧、ctx.Send 回帧被对端收到、
// 优雅 Close 端到端收敛、物理连接撕掉。
TEST(ProtocolNodeTcpSmoke, HandlerReceivesBusinessRepliesAndClosesEndToEnd) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  // 请求方 node:配 handler,对收到的业务帧经 ctx.Send 回一帧 noresponse 应答(回显 payload,
  // 打上判别性 message_id)。
  ProtocolNodeConfig config;
  config.handler = [](const Message& msg, HandlerContext& ctx) -> Coro::Result<void> {
    Message reply;
    reply.frm_type = FrameType::kState;
    reply.message_id = 0x00BB;
    reply.payload = msg.payload;  // 回显对端发来的 payload。
    return ctx.Send(std::move(reply));
  };
  auto node = std::make_unique<ProtocolNode>(
      std::make_unique<TcpTransport>(client), std::make_unique<SystemCodec>(),
      std::move(config));
  ASSERT_TRUE(node->Start());

  // 裸对端:发一个业务帧给 node,并起一条读 fiber 捕获 node 的应答;传输终结则退出。
  TcpTransport peer(accepted);
  ASSERT_TRUE(peer.Start());
  SystemCodec peer_codec;

  Message business;
  business.frm_type = FrameType::kState;  // 业务帧(非 kResponse/kResult)。
  business.session_id = 42;
  business.message_id = 0x0007;
  business.payload = {0xC0, 0xDE};
  auto encoded = peer_codec.Encode(business);
  ASSERT_TRUE(encoded);
  ASSERT_TRUE(peer.Write(SendUnit{std::move(encoded).value(), Endpoint::Default()}));

  std::mutex reply_mutex;
  std::vector<Message> replies;
  bool peer_ended = false;
  auto peer_reader = Coro::makeTask([&] {
    // 取一次 read_queue 句柄,循环 await(ADR-0007 D4)。
    const auto peer_rx = peer.Read();
    while (true) {
      auto datagram = testutil::AwaitRead(peer_rx);
      if (!datagram) {
        const auto error = datagram.error();
        if (error == make_error_code(TransportErrc::kClosed) ||
            error == make_error_code(TransportErrc::kConnection)) {
          break;  // 物理连接撕掉 → 对端读退出(端到端收敛可观测)。
        }
        continue;
      }
      const auto& bytes = datagram.value().bytes;
      auto decoded = peer_codec.Decode(bytes.data(), bytes.size());
      if (!decoded) {
        continue;
      }
      std::lock_guard<std::mutex> lock(reply_mutex);
      for (auto& m : decoded.value()) {
        replies.push_back(std::move(m));
      }
    }
    peer_ended = true;
  });

  // node handler 收到业务帧 → ctx.Send 回帧 → 对端收到一帧应答。
  ASSERT_TRUE(pumpFiberUntil([&] {
    std::lock_guard<std::mutex> lock(reply_mutex);
    return !replies.empty();
  }));
  {
    std::lock_guard<std::mutex> lock(reply_mutex);
    ASSERT_EQ(replies.size(), 1u);
    EXPECT_EQ(replies.front().message_id, 0x00BB);
    EXPECT_EQ(replies.front().payload, (std::vector<std::uint8_t>{0xC0, 0xDE}));
  }

  // 优雅 Close:端到端收敛、物理连接撕掉 → 对端读观测 kConnection 退出。
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed());
  EXPECT_TRUE(pumpFiberUntil([&] { return peer_ended; }));
  EXPECT_TRUE(peer_reader.get());
  client->deleteLater();
}
