// -----------------------------------------------------------------------------
// protocol_node_tcp_loopback_test.cpp — P1-T4 真实 TCP 回环端到端集成测试
//
// 在单机真实 TCP 回环上跑通一次 needresponse 请求-响应,证实 P1 最大架构赌注:
// **无共享引擎、语义内联各 node**(RT_DESIGN_003 / ADR-0001 D1-D2 / ADR-0003 D3/D7)。
//
// 拓扑(沿用 #19 双实现范式、复用 tcp_transport_send/read_test 的真实 socket 对范式):
//   · 请求方 = 真 ProtocolNode(真 TcpTransport(client) + SystemCodec)。
//   · 对端  = **裸** echo harness(不是 node):真 TcpTransport(accepted) + SystemCodec,
//            一条 makeTask fiber 循环 Read → Decode → 对每个 kCommand 帧调 responder →
//            Encode → Write。handler/队列是 P2,对端不做。
// 全部在 coro_test_main 的单 fiber 调度器内协作:请求方读循环 fiber + echo fiber +
// Request fiber 并发,主(测试)fiber 以 pumpFiberUntil 驱动时钟/让出。
// -----------------------------------------------------------------------------
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
#include "task/fibertask.h"
#include "transport/Endpoint.hpp"
#include "transport/Error.hpp"
#include "transport/Message.hpp"
#include "transport/ProtocolNode.hpp"
#include "transport/TcpTransport.hpp"
#include "transport/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::Result;
using transport::SendUnit;
using transport::Status;
using transport::SystemCodec;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 建立一对已连接的回环 socket(客户端 + 服务端已接受);连接建立不在被测类职责,
// 复用 tcp_transport_send/read_test 的夹具范式。
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

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

// 标准 echo 响应:frm_type=kResponse、session_id 原样、message_id = 请求码 | 0x1000、
// payload echo(与 DefaultProtocolKeyStrategy 的配对规则一致)。
Message EchoResponse(const Message& command) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.protocol_id = command.protocol_id;
  resp.session_id = command.session_id;
  resp.message_id = static_cast<std::uint16_t>(command.message_id | 0x1000);
  resp.payload = command.payload;
  return resp;
}

// 标准 echo responder:一条命令回恰好一帧标准响应(happy path 对端行为)。
std::vector<Message> EchoResponder(const Message& command) {
  return {EchoResponse(command)};
}

// 裸 echo harness fiber:真 TcpTransport + SystemCodec,收 kCommand → responder → 回帧。
// responder 决定对一条命令回多少帧(happy=1;乱序测试可回错 key / 重复)。传输终结
// (对端撕连接 kConnection / 我方 kClosed)→ 退出并置 ended=true(端到端连接撕掉可观测)。
template <typename Responder>
auto SpawnEcho(TcpTransport& transport, Responder responder, bool& ended) {
  return Coro::makeTask([&transport, responder, &ended] {
    SystemCodec codec;
    while (true) {
      auto datagram = transport.Read();  // 裸读,无 deadline。
      if (!datagram) {
        const auto error = datagram.error();
        if (error == make_error_code(TransportErrc::kClosed) ||
            error == make_error_code(TransportErrc::kConnection)) {
          break;  // 传输终结 → echo 退出。
        }
        continue;  // 其它瞬时错误:丢弃继续。
      }
      const auto& bytes = datagram.value().bytes;
      auto decoded = codec.Decode(bytes.data(), bytes.size());
      if (!decoded) {
        continue;
      }
      for (const auto& command : decoded.value()) {
        if (command.frm_type != FrameType::kCommand) {
          continue;  // 对端只应答命令帧。
        }
        for (const auto& resp : responder(command)) {
          auto encoded = codec.Encode(resp);
          if (!encoded) {
            continue;
          }
          SendUnit unit{std::move(encoded).value(), Endpoint::Default()};
          transport.Write(std::move(unit));
        }
      }
    }
    ended = true;
  });
}

// 便捷:构造一个真 ProtocolNode 请求方(接管 client 端 TcpTransport + SystemCodec)。
std::unique_ptr<ProtocolNode> MakeRequesterNode(QTcpSocket* client) {
  return std::make_unique<ProtocolNode>(std::make_unique<TcpTransport>(client),
                                        std::make_unique<SystemCodec>());
}

}  // namespace

// RT_REQUEST / RT_DESIGN_003:真实 TCP 回环 happy path。请求方 Request → 裸 echo 对端回
// 一帧响应 → Request 恰好一次完成、返响应 Message(payload 与 echo 一致)、关联清理
// (UnmatchedResponseCount 保持 0;归 0 的强观测见下一 LateAndWrongKey 用例)。
TEST(ProtocolNodeTcpLoopback, RequestResolvedByRealEchoPeer) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport echo_transport(accepted);
  ASSERT_TRUE(echo_transport.Start());
  bool echo_ended = false;
  auto echo = SpawnEcho(echo_transport, EchoResponder, echo_ended);

  auto node = MakeRequesterNode(client);
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node->Request(MakeRequest(0x0002, {0x11, 0x22, 0x33}), options);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(outcome.value().session_id, 0);  // 首个滚动 session_id。
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(outcome.value().payload,
            (std::vector<std::uint8_t>{0x11, 0x22, 0x33}));
  EXPECT_EQ(node->UnmatchedResponseCount(), 0u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);

  // 收尾:关请求方 → 撕连接 → echo 观测到 kConnection 退出。
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
  client->deleteLater();
}

// RT_REQUEST_004:乱序/迟到/无匹配。对端对一条命令回 [错 key 响应, 正确响应, 重复正确
// 响应] 三帧 → 正确响应恰好一次完成 Request;错 key 与重复响应均无匹配在途 → 被丢弃并
// 观测(UnmatchedResponseCount 升到 2),不二次完成、不进业务流。重复正确响应被丢弃即
// 证实 Request 完成后关联已清理(PendingTable 归 0)。
TEST(ProtocolNodeTcpLoopback, LateAndWrongKeyResponsesDroppedAndCounted) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport echo_transport(accepted);
  ASSERT_TRUE(echo_transport.Start());
  bool echo_ended = false;
  // responder:错 key(session 偏移,配不上任何在途键)+ 正确响应 + 重复正确响应。
  auto responder = [](const Message& command) {
    Message wrong_key = EchoResponse(command);
    wrong_key.session_id = static_cast<std::uint8_t>(command.session_id + 7);
    Message correct = EchoResponse(command);
    Message duplicate = EchoResponse(command);  // 与 correct 同 key。
    return std::vector<Message>{wrong_key, correct, duplicate};
  };
  auto echo = SpawnEcho(echo_transport, responder, echo_ended);

  auto node = MakeRequesterNode(client);
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node->Request(MakeRequest(0x0005, {0xAA}), options);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();  // 恰好一次完成。
  EXPECT_EQ(outcome.value().message_id, 0x1005);
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0xAA}));

  // 错 key + 重复(迟到)两帧无匹配在途 → 丢弃计数升到 2,不二次完成、不进业务流。
  ASSERT_TRUE(pumpFiberUntil([&] { return node->UnmatchedResponseCount() == 2u; }));
  EXPECT_EQ(node->UnmatchedResponseCount(), 2u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);  // 响应帧未误入业务流。

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
  client->deleteLater();
}

// RT_TRANSPORT(依赖 T1 读侧契约):请求方在真实回环上读到的响应 Datagram 的来源标识
// 为对端地址(Endpoint::Net = 对端 ip:port)。ProtocolNode 内联读循环不外露 source,故本
// 用例用裸 TcpTransport 作请求方读取一帧真 echo 响应,直接断言 source == 对端。
TEST(ProtocolNodeTcpLoopback, ResponseSourceIsPeerEndpoint) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport echo_transport(accepted);
  ASSERT_TRUE(echo_transport.Start());
  bool echo_ended = false;
  auto echo = SpawnEcho(echo_transport, EchoResponder, echo_ended);

  // 裸请求方:手写一条命令帧发出,读回 echo 响应,验证其来源为对端。
  TcpTransport requester(client);
  ASSERT_TRUE(requester.Start());
  const std::string expected_host =
      client->peerAddress().toString().toStdString();
  const std::uint16_t expected_port = client->peerPort();
  ASSERT_GT(expected_port, 0U);

  SystemCodec codec;
  Message command;
  command.frm_type = FrameType::kCommand;
  command.session_id = 0;
  command.message_id = 0x0009;
  command.payload = {0x5A, 0x5A};
  auto encoded = codec.Encode(command);
  ASSERT_TRUE(encoded);
  ASSERT_TRUE(requester.Write(
      SendUnit{std::move(encoded).value(), Endpoint::Default()}));

  // 读回 echo 响应(阻塞 Read 让出 → echo fiber 处理命令回帧;readAll 可能分片,补读
  // 至能 Decode 出一帧)。每片都携带对端来源标识(T1 读侧契约,TCP from 恒为对端)。
  std::vector<Message> responses;
  const auto deadline = OperationOptions::Clock::now() + 3s;
  while (responses.empty() && OperationOptions::Clock::now() < deadline) {
    OperationOptions options;
    options.deadline = deadline;
    auto datagram = requester.Read(options);
    ASSERT_TRUE(datagram) << datagram.error().message();
    EXPECT_EQ(datagram.value().source.kind, Endpoint::Kind::kNet);
    EXPECT_EQ(datagram.value().source.host, expected_host);
    EXPECT_EQ(datagram.value().source.port, expected_port);
    const auto& bytes = datagram.value().bytes;
    auto decoded = codec.Decode(bytes.data(), bytes.size());
    ASSERT_TRUE(decoded);
    for (auto& m : decoded.value()) {
      responses.push_back(std::move(m));
    }
  }
  ASSERT_FALSE(responses.empty());
  EXPECT_EQ(responses.front().frm_type, FrameType::kResponse);
  EXPECT_EQ(responses.front().message_id, 0x1009);
  EXPECT_EQ(responses.front().payload,
            (std::vector<std::uint8_t>{0x5A, 0x5A}));

  ASSERT_TRUE(requester.RequestClose());
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));
  EXPECT_TRUE(echo.get());
  client->deleteLater();
}

// RT_LIFECYCLE:Close 端到端收敛。对端沉默(不回响应)→ 在途 Request 挂起 → 关闭请求方
// → 在途 Request 恰好一次返 kClosed、WaitClosed 完成、物理连接撕掉(对端 echo 观测到
// kConnection 退出)。关闭后再 Request 仍 kClosed。
TEST(ProtocolNodeTcpLoopback, CloseConvergesEndToEnd) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));

  TcpTransport echo_transport(accepted);
  ASSERT_TRUE(echo_transport.Start());
  bool echo_ended = false;
  // 沉默对端:收命令但不回任何响应帧。
  auto silent = [](const Message&) { return std::vector<Message>{}; };
  auto echo = SpawnEcho(echo_transport, silent, echo_ended);

  auto node = MakeRequesterNode(client);
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0002, {0x01}));  // 无 deadline,靠 Close 收敛。
    done = true;
  });
  // 让 Request 把命令写出并挂起在应答等待点(对端沉默,不会回应);Request 保持在途。
  boost::this_fiber::sleep_for(50ms);
  EXPECT_FALSE(done);

  // 关请求方:在途 Request 恰好一次以 kClosed 收敛。
  ASSERT_TRUE(node->Close());
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kClosed));

  // WaitClosed 在读循环退出后完成。
  EXPECT_TRUE(node->WaitClosed());

  // 物理连接撕掉:对端 echo 观测到连接终结而退出。
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));

  // 关闭后再 Request → 一律 kClosed。
  auto after = node->Request(MakeRequest(0x0003, {}));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));

  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
  client->deleteLater();
}
