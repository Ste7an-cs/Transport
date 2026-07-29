// -----------------------------------------------------------------------------
// protocol_node_reconnect_test.cpp — P3-2 节点集成断连:reactor fiber + Read 透明跨重连
//
// 承接 ADR-0003 D11 Q1/Q3、RT_TCP_RECONNECT_002/004、RT_DESIGN_008。验证 ProtocolNode 在
// 自动重连传输(TcpClientTransport,实现 IConnectionObservable)上"观察连接状态但不管理
// churn":
//   ① Read 透明跨重连 → node 读循环永不因 TCP 客户端断连而退出(只 Close 退出)。
//   ② reactor fiber 遇代际结束 → PendingTable.FailAll(kConnection) 令在途请求恰好一次收敛
//      + Drain 未启动旧代际业务归因 连接代际隔离丢弃 + 运行中 handler 跑完 + node 保持 Running。
//   ③ 旧代际迟到响应到达新代际 → 归因丢弃(UnmatchedResponseCount),在途已 FailAll 清空不误配。
//   ④ 非 IConnectionObservable 传输(Fake)→ node 无 reactor,行为同 P2(回归)。
//
// 拓扑沿用 tcp_client_transport_test / protocol_node_tcp_loopback_test 的真实可控 server 范式:
// 请求方 = 真 ProtocolNode(真 TcpClientTransport(client) + SystemCodec);对端 = 裸 QTcpServer
// + 真 TcpTransport(accepted) + SystemCodec,测试 abort accepted socket 模拟物理断连,再 accept
// 新连接模拟自动重连。所有退避/超时用小值注入以确定化单条时长。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
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
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::Endpoint;
using transport::FrameType;
using transport::HandlerContext;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::SendUnit;
using transport::Status;
using transport::SystemCodec;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

// 小值确定化配置:短连接超时、小退避、关抖动 → 断连后毫秒级自动重连。
TcpClientConfig FastClientConfig(quint16 port) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 400ms;
  cfg.initial_backoff = 20ms;
  cfg.max_backoff = 80ms;
  cfg.backoff_multiplier = 2.0;
  cfg.jitter_enabled = false;
  cfg.stable_reset_after = 10s;
  return cfg;
}

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = Clock::now() + d;
  return o;
}

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

// 接受下一个入站连接(pump 到就绪),交出所有权(setParent nullptr)供 TcpTransport 管理。
QTcpSocket* AcceptNext(QTcpServer& server, int budget_ms = 3000) {
  if (!pumpFiberUntil([&] { return server.hasPendingConnections(); }, budget_ms)) {
    return nullptr;
  }
  QTcpSocket* s = server.nextPendingConnection();
  if (s) {
    s->setParent(nullptr);
  }
  return s;
}

// 标准 echo 响应:frm_type=kResponse、session_id 原样、message_id=请求码|0x1000、payload echo
//(与 DefaultProtocolKeyStrategy 配对规则一致)。
Message EchoResponse(const Message& command) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.protocol_id = command.protocol_id;
  resp.session_id = command.session_id;
  resp.message_id = static_cast<std::uint16_t>(command.message_id | 0x1000);
  resp.payload = command.payload;
  return resp;
}

std::vector<Message> EchoResponder(const Message& command) {
  return {EchoResponse(command)};
}

// 裸 echo harness fiber:真 TcpTransport + SystemCodec,收 kCommand → responder → 回帧。
// 传输终结(对端撕连接 kConnection / 我方 kClosed)→ 退出并置 ended=true。
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
          break;
        }
        continue;
      }
      const auto& bytes = datagram.value().bytes;
      auto decoded = codec.Decode(bytes.data(), bytes.size());
      if (!decoded) {
        continue;
      }
      for (const auto& command : decoded.value()) {
        if (command.frm_type != FrameType::kCommand) {
          continue;
        }
        for (const auto& resp : responder(command)) {
          auto encoded = codec.Encode(resp);
          if (!encoded) {
            continue;
          }
          transport.Write(SendUnit{std::move(encoded).value(), Endpoint::Default()});
        }
      }
    }
    ended = true;
  });
}

// 服务端主动发一帧(不经 echo responder):供注入业务帧 / 旧代际迟到响应。
void ServerSend(TcpTransport& transport, const Message& msg) {
  SystemCodec codec;
  auto encoded = codec.Encode(msg);
  ASSERT_TRUE(encoded);
  ASSERT_TRUE(
      transport.Write(SendUnit{std::move(encoded).value(), Endpoint::Default()}));
}

std::unique_ptr<ProtocolNode> MakeClientNode(quint16 port,
                                             ProtocolNodeConfig config = {}) {
  return std::make_unique<ProtocolNode>(
      std::make_unique<TcpClientTransport>(FastClientConfig(port)),
      std::make_unique<SystemCodec>(), std::move(config));
}

}  // namespace

// —— ① + node 保持 Running 跨断连-重连:在途请求断连恰好一次 kConnection,读循环透明续命,
// 重连后新代际 Request 正常完成,仅 Close 使 node 收敛。 ——
// 覆盖 RT_TCP_RECONNECT_002/004、验收 1/3/6。
TEST(ProtocolNodeReconnect, InFlightRequestFailsOnceThenReconnectResumes) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto node = MakeClientNode(port);
  ASSERT_TRUE(node->Start());

  // 代际1:接受连接 + 沉默对端(收命令不回响应)→ 在途 Request 挂起。
  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());
  bool echo1_ended = false;
  auto silent = [](const Message&) { return std::vector<Message>{}; };
  auto echo1 = SpawnEcho(*server_txp1, silent, echo1_ended);

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0002, {0x01}));  // 无 deadline,靠断连收敛。
    done = true;
  });
  boost::this_fiber::sleep_for(60ms);
  EXPECT_FALSE(done);                     // 在途挂起。
  EXPECT_EQ(node->PendingCount(), 1u);    // 恰一在途。

  // 物理断连:abort 服务端 socket → 客户端 TcpClientTransport 检测断连 → reactor 触发。
  accepted1->abort();

  // 在途请求恰好一次以 kConnection 收敛(不跨代际重放,RT_TCP_RECONNECT_002)。
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 4000));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kConnection));
  // 在途已 FailAll 清空(session_id 亦释放)。
  EXPECT_EQ(node->PendingCount(), 0u);

  // node 未 Closed(读循环透明续命未退出):WaitClosed 短 deadline → kTimeout。
  auto wc = node->WaitClosed(Deadline(50ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  // 代际2:客户端自动重连 → 服务端接受新连接,这次挂 echo 响应对端。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);

  // 新代际请求正常完成 → 证读循环跨重连续命(响应经同一未退出的读循环路由)。
  Result<Message> outcome2{make_error_code(TransportErrc::kInternal)};
  bool done2 = false;
  auto request2 = Coro::makeTask([&] {
    outcome2 = node->Request(MakeRequest(0x0004, {0xAB}), Deadline(4000ms));
    done2 = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done2; }, 6000));
  ASSERT_TRUE(outcome2) << outcome2.error().message();
  EXPECT_EQ(outcome2.value().message_id, 0x1004);
  EXPECT_EQ(outcome2.value().payload, (std::vector<std::uint8_t>{0xAB}));

  // 仅 Close 使 node Closing→Closed。
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));

  // 收敛所有 spawn 的 fiber(server 端两代际 echo + 两个请求 fiber),避免跨用例泄漏。
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended && echo2_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(echo2.get());
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(request2.get());
}

// —— ② 断连时未启动排队业务 → 连接代际隔离丢弃 计数;正在运行 handler 跑完(不强杀)。 ——
// 覆盖 RT_TCP_RECONNECT 3.1.7.4、验收 2。
TEST(ProtocolNodeReconnect, QueuedOldGenerationBusinessDroppedRunningHandlerCompletes) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  // handler:首帧进入即 ++started 并阻塞在 gate(模拟"运行中"),释放后 ++completed。
  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int started = 0;
  int completed = 0;
  ProtocolNodeConfig cfg;
  cfg.handler = [&started, &completed, gate](const Message&,
                                             HandlerContext&) -> Status {
    ++started;
    Coro::await(gate);  // 阻塞到测试释放:一帧"正在运行"的 handler。
    ++completed;
    return Status{};
  };

  auto node = MakeClientNode(port, std::move(cfg));
  ASSERT_TRUE(node->Start());

  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());

  // 对端主动推 4 帧业务(kState,非响应帧 → node 入业务队列)。handler 取首帧后阻塞在 gate,
  // 其余 3 帧滞留队列未启动。
  constexpr int kBusinessFrames = 4;
  for (int i = 0; i < kBusinessFrames; ++i) {
    Message biz;
    biz.frm_type = FrameType::kState;
    biz.message_id = static_cast<std::uint16_t>(0x0030 + i);
    biz.payload = {static_cast<std::uint8_t>(i)};
    ServerSend(*server_txp1, biz);
  }

  // 等首帧进入 handler 并阻塞;再 pump 让其余 3 帧全部到达并入队(handler 阻塞 → 不会出队)。
  ASSERT_TRUE(pumpFiberUntil([&] { return started == 1; }, 3000));
  pumpFiberUntil([] { return false; }, 250);  // 让滞留帧全部到达入队(确定化)。

  // 物理断连:reactor 遇代际结束 → Drain 未启动的 3 帧 → 连接代际隔离丢弃。
  accepted1->abort();
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return node->GenerationIsolationDropCount() >= 1; }, 4000));
  EXPECT_EQ(node->GenerationIsolationDropCount(),
            static_cast<std::size_t>(kBusinessFrames - 1));
  // 隔离丢弃不误记为 close_drop / overflow / dropped_no_handler。
  EXPECT_EQ(node->CloseDropCount(), 0u);
  EXPECT_EQ(node->BusinessQueueOverflowCount(), 0u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);

  // 运行中 handler 未被强杀:释放 gate → 首帧跑完清理。
  EXPECT_EQ(started, 1);
  EXPECT_EQ(completed, 0);
  gate->resolve();
  gate->close();
  ASSERT_TRUE(pumpFiberUntil([&] { return completed == 1; }, 3000));
  EXPECT_EQ(completed, 1);

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
}

// —— ③ 旧代际迟到响应到达新代际 → 归因丢弃、不误配(在途已 FailAll 清空)。 ——
// 覆盖 RT_TCP_RECONNECT_004、验收 4。
TEST(ProtocolNodeReconnect, StaleOldGenerationResponseAttributedNotMisrouted) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto node = MakeClientNode(port);
  ASSERT_TRUE(node->Start());

  // 代际1:沉默对端 → 在途 Request(session 0)挂起。
  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0007, {0x01}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node->PendingCount() == 1u; }, 3000));

  // 断连 → 在途 Request 恰好一次 kConnection、在途清空。
  accepted1->abort();
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 4000));
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(node->PendingCount(), 0u);

  // 代际2:重连;对端在新连接上补发旧代际请求(session 0,message_id 0x1007|0x1000)的迟到响应。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());

  Message stale;
  stale.frm_type = FrameType::kResponse;
  stale.session_id = 0;
  stale.message_id = static_cast<std::uint16_t>(0x0007 | 0x1000);
  stale.payload = {0xDE, 0xAD};
  ServerSend(*server_txp2, stale);

  // 迟到旧代际响应无匹配在途(已 FailAll 清空)→ 归因丢弃、不误配。
  ASSERT_TRUE(
      pumpFiberUntil([&] { return node->UnmatchedResponseCount() == 1u; }, 4000));
  EXPECT_EQ(node->UnmatchedResponseCount(), 1u);
  EXPECT_EQ(node->PendingCount(), 0u);

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(request.get());
  server_txp1->RequestClose();
  server_txp2->RequestClose();
}

// —— ④ 非 IConnectionObservable 传输(Fake)→ node 无 reactor,行为同 P2(回归)。 ——
// 覆盖验收 5。
TEST(ProtocolNodeReconnect, NonObservableTransportHasNoReactor) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  // 无 reactor:代际隔离计数恒 0。
  EXPECT_EQ(node.GenerationIsolationDropCount(), 0u);

  // P2 请求-响应回归:发 Request → 注入匹配响应 → 恰好一次完成。
  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {0x11}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));

  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.session_id = 0;
  resp.message_id = 0x1002;
  resp.payload = {0xAB};
  SystemCodec wire;
  auto bytes = wire.Encode(resp);
  ASSERT_TRUE(bytes);
  Datagram dg;
  dg.bytes = std::move(bytes).value();
  fake->Inject(std::move(dg));

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(node.GenerationIsolationDropCount(), 0u);  // 全程无代际隔离。

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(request.get());
}
