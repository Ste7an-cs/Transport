// -----------------------------------------------------------------------------
// protocol_node_reconnect_test.cpp — 节点集成断连:重连对交互层完全透明
//
// 承接 **ADR-0004 D1/D2/D3**、RT_TCP_RECONNECT_002/004、RT_TRANSPORT_008、RT_DESIGN_008。
// 交互层不再感知连接管理:无能力探测、无 reactor 协程、无连接代际概念,读循环仅区分
// `kClosed`(终结)与其余(瞬时错误,继续)。本文件验证由此得到的对外可观察语义:
//   ① 断链**不终结**在途请求:请求继续在途,直至自己的总超时;读循环透明续命,node 保持
//      Running;重连后新请求正常收发(RT_TCP_RECONNECT_002 改写)。
//   ② 断链**不清空**排队业务:未启动的旧链路业务事件保留,释放运行中 handler 后照常全部
//      处理完;运行中 handler 不被强杀(D3 撤销代际隔离丢弃)。
//   ③ 断链全程**不产生任何丢弃归因 Trace**(D3:不再有"旧代际业务被丢弃"这一事件)。
//   ④ 在途请求在终结前不释放其关联标识 → 重连后的新请求取不到同一关联键、不被旧链路迟到
//      响应误配;在途请求终结后,迟到响应无匹配 → 归因丢弃(RT_TCP_RECONNECT_004)。
//
// 拓扑沿用 tcp_client_transport_test / protocol_node_tcp_loopback_test 的真实可控 server 范式:
// 请求方 = 真 ProtocolNode(真 TcpClientTransport(client) + SystemCodec);对端 = 裸 QTcpServer
// + 真 TcpTransport(accepted) + SystemCodec,测试 abort accepted socket 模拟物理断连,再 accept
// 新连接模拟自动重连。所有重连间隔/超时用小值注入以确定化单条时长。
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
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::CapturingTraceSink;
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

// 过滤出 category=="drop" 的记录:sink 同时收 P5-3 的丢弃事件与 P5-4 的 send/recv/
// decode/handler/close/reconnect 等事件(共用同一 trace_sink),按 category 过滤才是
// "这次丢弃恰好一条 Trace"断言的正确写法,不能假设 sink 总记录数等于丢弃数。
std::vector<CapturingTraceSink::Record> DropRecords(
    const std::vector<CapturingTraceSink::Record>& records) {
  std::vector<CapturingTraceSink::Record> out;
  for (const auto& rec : records) {
    if (rec.category == "drop") {
      out.push_back(rec);
    }
  }
  return out;
}

// 小值确定化配置:短连接超时、小退避、关抖动 → 断连后毫秒级自动重连。
TcpClientConfig FastClientConfig(quint16 port) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 400ms;
  cfg.reconnect_interval = 20ms;
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
    // 取一次 read_queue 句柄,循环 await(ADR-0007 D4)。
    const auto rx = transport.Read();
    while (true) {
      auto datagram = testutil::AwaitRead(rx);
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

// —— ① 断链不终结在途请求;读循环透明续命,node 保持 Running;重连后新请求正常收发;
// 旧请求最终由**自己的总超时**终结(唯一剩下的终结源)。 ——
// 覆盖 ADR-0004 D1/D3、RT_TCP_RECONNECT_002(改写)、RT_TRANSPORT_008。
TEST(ProtocolNodeReconnect, InFlightRequestSurvivesDisconnectThenReconnectResumes) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto node = MakeClientNode(port);
  ASSERT_TRUE(node->Start());

  // 链路1:接受连接 + 沉默对端(收命令不回响应)→ 在途 Request 挂起。
  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());
  bool echo1_ended = false;
  auto silent = [](const Message&) { return std::vector<Message>{}; };
  auto echo1 = SpawnEcho(*server_txp1, silent, echo1_ended);

  // 显式总超时:断链已不再终结在途请求,总超时是它唯一的终结源(ADR-0004 D3)。
  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0002, {0x01}), Deadline(2500ms));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node->PendingCount() == 1u; }, 3000));
  EXPECT_FALSE(done);  // 在途挂起。

  // 物理断连:abort 服务端 socket。交互层收不到任何链路中断信号(D1 完全透明)。
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 300);  // 让断链充分传导(确定化)。

  // 断链**不终结**在途请求:仍在途,未收敛。
  EXPECT_FALSE(done) << "断链不得终结在途请求(ADR-0004 D3)";
  EXPECT_EQ(node->PendingCount(), 1u);

  // node 未 Closed(读循环仅 kClosed 退出,断链不是终结):WaitClosed 短 deadline → kTimeout。
  auto wc = node->WaitClosed(Deadline(50ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  // 链路2:客户端自动重连 → 服务端接受新连接,这次挂 echo 响应对端。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);

  // 重连后新请求正常完成 → 证读循环跨重连续命(响应经同一未退出的读循环路由)。
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
  // 旧请求此刻仍在途(其响应永远不会到:发在已死的链路1上)。
  EXPECT_FALSE(done);
  EXPECT_EQ(node->PendingCount(), 1u);

  // 旧请求最终由自己的总超时恰好一次终结(RT_REQUEST_003)。
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 4000));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node->PendingCount(), 0u);  // 终结后 session_id 亦释放。

  // 仅 Close 使 node Closing→Closed。
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));

  // 收敛所有 spawn 的 fiber(server 端两条链路的 echo + 两个请求 fiber),避免跨用例泄漏。
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended && echo2_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(echo2.get());
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(request2.get());
}

// —— ② 断链**不清空**排队业务:未启动的业务事件不被丢弃,释放运行中 handler 后照常全部
// 处理完;运行中 handler 不被强杀。 ——
// 覆盖 ADR-0004 D3(撤销代际隔离丢弃)。
TEST(ProtocolNodeReconnect, QueuedBusinessSurvivesDisconnectRunningHandlerCompletes) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  // handler:首帧进入即阻塞在 gate(模拟"正在运行"),其余帧释放后依次跑完。
  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int started = 0;
  int completed = 0;
  ProtocolNodeConfig cfg;
  cfg.handler = [&started, &completed, gate](const Message&,
                                             HandlerContext&) -> Status {
    ++started;
    if (started == 1) {
      Coro::await(gate);  // 仅首帧挂住:一帧"正在运行"的 handler。
    }
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

  // 物理断连:交互层无任何动作——不清队列、不终结请求、不改生命周期。
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 300);  // 让断链充分传导(确定化)。

  // 运行中 handler 未被强杀,滞留帧也未被丢弃(无任何丢弃归因)。
  EXPECT_EQ(started, 1);
  EXPECT_EQ(completed, 0);
  EXPECT_EQ(node->CloseDropCount(), 0u);
  EXPECT_EQ(node->BusinessQueueOverflowCount(), 0u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);
  EXPECT_EQ(node->UnmatchedResponseCount(), 0u);

  // node 保持 Running(读循环未因断链退出)。
  auto wc = node->WaitClosed(Deadline(50ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  // 释放 gate → 首帧跑完清理,滞留的 3 帧照常全部被处理(断链未清空业务队列)。
  gate->resolve();
  gate->close();
  ASSERT_TRUE(pumpFiberUntil([&] { return completed == kBusinessFrames; }, 4000));
  EXPECT_EQ(started, kBusinessFrames);
  EXPECT_EQ(node->CloseDropCount(), 0u);

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
}

// —— ③ P5-3 Trace 面(issue #88 的反向断言):同一断连拓扑配置 trace_sink,断链全程
// **不产生任何 category=="drop" 的 Trace 事件**——"旧代际业务被丢弃"这一事件已不存在
// (ADR-0004 D3);业务帧全部被处理,直到 Close 也无 close_drop。 ——
TEST(ProtocolNodeReconnect, DisconnectEmitsNoDropTraceEvents) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int started = 0;
  int completed = 0;
  CapturingTraceSink sink;
  ProtocolNodeConfig cfg;
  cfg.trace_sink = &sink;
  cfg.handler = [&started, &completed, gate](const Message&,
                                             HandlerContext&) -> Status {
    ++started;
    if (started == 1) {
      Coro::await(gate);  // 仅首帧挂住:一帧"正在运行"的 handler。
    }
    ++completed;
    return Status{};
  };

  auto node = MakeClientNode(port, std::move(cfg));
  ASSERT_TRUE(node->Start());

  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());

  constexpr int kBusinessFrames = 4;
  for (int i = 0; i < kBusinessFrames; ++i) {
    Message biz;
    biz.frm_type = FrameType::kState;
    biz.message_id = static_cast<std::uint16_t>(0x0040 + i);
    biz.payload = {static_cast<std::uint8_t>(i)};
    ServerSend(*server_txp1, biz);
  }

  ASSERT_TRUE(pumpFiberUntil([&] { return started == 1; }, 3000));
  pumpFiberUntil([] { return false; }, 250);  // 让滞留帧全部到达入队。

  accepted1->abort();
  pumpFiberUntil([] { return false; }, 300);  // 让断链充分传导(确定化)。

  // 断链一条丢弃 Trace 都不产生(sink 仍收 send/recv/decode/handler 等非丢弃事件,
  // 故按 category 过滤而非断言 Records() 整体为空)。
  EXPECT_TRUE(DropRecords(sink.Records()).empty())
      << "断链不得产生任何丢弃归因(ADR-0004 D3)";

  // 释放 gate:滞留业务照常全部处理完 → 直到 Close 仍无 close_drop。
  gate->resolve();
  gate->close();
  ASSERT_TRUE(pumpFiberUntil([&] { return completed == kBusinessFrames; }, 4000));

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(DropRecords(sink.Records()).empty());
  server_txp1->RequestClose();
}

// —— ④ 关联标识在终结前不释放 → 重连后的新请求取不到同一关联键(RT_REQUEST_004 由物理
// 事实 + 标识不复用共同保证,ADR-0004 D3 正确性依据);旧请求终结后其迟到响应无匹配 →
// 归因丢弃、不误配。 ——
// 覆盖 RT_TCP_RECONNECT_004、RT_REQUEST_004/005。
TEST(ProtocolNodeReconnect, StaleOldGenerationResponseAttributedNotMisrouted) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto node = MakeClientNode(port);
  ASSERT_TRUE(node->Start());

  // 链路1:沉默对端(不挂 echo)→ 在途 Request(session 0)挂起,总超时 2500ms。
  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0007, {0x01}), Deadline(2500ms));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node->PendingCount() == 1u; }, 3000));

  // 断连:在途 Request 继续在途(不再被断链终结)。
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 300);
  EXPECT_FALSE(done);
  EXPECT_EQ(node->PendingCount(), 1u);

  // 链路2:重连 + echo 对端。此时发**同一命令码**的新请求:因旧请求尚未终结、其 session_id
  // 未归还空闲集,新请求必得另一个 session_id → 两者关联键不同,旧链路的迟到响应无从完成
  // 新请求(这正是撤销代际隔离后 RT_REQUEST_004 仍成立的机制依据)。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);

  Result<Message> fresh{make_error_code(TransportErrc::kInternal)};
  bool fresh_done = false;
  auto request2 = Coro::makeTask([&] {
    fresh = node->Request(MakeRequest(0x0007, {0x02}), Deadline(4000ms));
    fresh_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return fresh_done; }, 6000));
  ASSERT_TRUE(fresh) << fresh.error().message();
  EXPECT_EQ(fresh.value().message_id, 0x1007);
  EXPECT_EQ(fresh.value().payload, (std::vector<std::uint8_t>{0x02}));
  EXPECT_NE(fresh.value().session_id, 0)
      << "旧请求未终结前其 session_id 不得被复用(否则旧响应可误配新请求)";
  EXPECT_FALSE(done);  // 旧请求仍在途,未被新链路的响应误配。

  // 旧请求由自己的总超时终结,session 0 归还空闲集。
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 4000));
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node->PendingCount(), 0u);
  EXPECT_EQ(node->UnmatchedResponseCount(), 0u);

  // 对端在新链路上补发旧请求(session 0,message_id 0x1007)的迟到响应:无匹配在途 →
  // 归因丢弃、不误配。
  Message stale;
  stale.frm_type = FrameType::kResponse;
  stale.session_id = 0;
  stale.message_id = static_cast<std::uint16_t>(0x0007 | 0x1000);
  stale.payload = {0xDE, 0xAD};
  ServerSend(*server_txp2, stale);
  ASSERT_TRUE(
      pumpFiberUntil([&] { return node->UnmatchedResponseCount() == 1u; }, 4000));
  EXPECT_EQ(node->PendingCount(), 0u);

  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo2_ended; }));
  EXPECT_TRUE(echo2.get());
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(request2.get());
}

// —— ⑤ 致命错误自终的**反向用例**(RT_LIFECYCLE_008 / ADR-0005 D5):TCP 客户端**永不
// 自终**。它无限重连,`Read` 只在我方 Close 时返 kClosed(ADR-0004 D1),故断链期读循环
// 挂起而非退出,节点保持 Running(WaitClosed 短 deadline → kTimeout、无 close_drop),
// 重连后照常收发;唯有显式 Close 才使其 Closing→Closed。 ——
// 实现上无按介质分支:判据是"读循环退出时 lifecycle 是否仍 Running",TCP 客户端天然落不到。
TEST(ProtocolNodeReconnect, TcpClientDisconnectDoesNotSelfTerminate) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto node = MakeClientNode(port);
  ASSERT_TRUE(node->Start());

  // 链路1:接受连接后立即物理断连。
  QTcpSocket* accepted1 = AcceptNext(server, 4000);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 300);  // 让断链充分传导(确定化)。

  // 断链**不触发**自终:节点仍 Running(WaitClosed 超时而非返回成功),无关闭归因。
  auto wc = node->WaitClosed(Deadline(200ms));
  ASSERT_FALSE(wc) << "TCP 客户端断链不得自终(它无限重连,读循环未退出)";
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node->CloseDropCount(), 0u);

  // 链路2:重连后照常收发(同一条未退出的读循环继续交付)。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node->Request(MakeRequest(0x0009, {0x5A}), Deadline(4000ms));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().message_id, 0x1009);

  // 唯有显式 Close 使其收敛。
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(node->WaitClosed(Deadline(2000ms)));

  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo2_ended; }));
  EXPECT_TRUE(echo2.get());
  EXPECT_TRUE(request.get());
}
