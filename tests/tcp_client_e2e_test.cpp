// -----------------------------------------------------------------------------
// tcp_client_e2e_test.cpp — 真实 TCP 断连-重连回环端到端(ProtocolNode + 连接泵)
//
// 承接 ADR-0004 D1/D3/D6、ADR-0005 D4、RT_TCP_RECONNECT 全 / RT_TCP_RECONFIG 全 /
// RT_LIFECYCLE_002 / RT_DATA_STATE。在 fiber 调度器(coro_test_main)内用本机真实 TCP
// 回环把 `ProtocolNode` + `TcpClientTransport` 的完整链路端到端跑通:
//
//   请求方 = 真 ProtocolNode(真 TcpClientTransport(client) + SystemCodec)。
//   对端    = 可控 echo server:裸 QTcpServer + 真 TcpTransport(accepted) + SystemCodec,
//             收 kCommand → 回 kResponse(session_id 原样、message_id|0x1000、payload echo);
//             测试可令其静默(不回帧)、abort(模拟物理断连)、下线/上线(拒绝/接受连接)。
//
// 覆盖:
//   ① 真实断连-重连回环:Request 恰好一次完成(echo)→ server 断开 → **在途请求不被
//      框架终结**(ADR-0004 D3),由其总超时收敛 → 自动重连(Generation 递增)→ 新代际
//      Request 成功;node 全程 Running、读循环透明续命未退出(ADR-0004 D1)。
//   ② 代际隔离已撤销:断连**不丢弃**未开始处理的业务事件、**不终结**在途请求;隔离丢弃
//      计数恒为 0;释放 handler 后滞留业务照常处理完。
//   ③ 重连节奏:断开后按**固定间隔**重试(ADR-0005 D4:无指数增长)。
//   ④ 重配置端点切换:ApplyConfig 改 host/port → 立即切新代际连第二个 server(不等剩余
//      间隔)→ 新 Request 成功;旧在途请求由总超时收敛;Generation 与 ConfigVersion 各自递增。
//   ⑤ Close 端到端收敛:Connected / 重连等待中 Close → node Closing→Closed,WaitClosed
//      完成,连接撕掉。
//
// 确定化 / 防 flake 手段:所有连接超时/重连间隔用毫秒级小值注入;在途请求一律给显式短
// deadline(总超时是断链后唯一的收敛手段);端口用 FreePort(listen 后即关)确定"被拒绝";
// 时序断言采用宽松容差(容调度抖动)。所有 spawn 的 fiber 均在用例末 join,避免跨用例泄漏。
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
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::ConnectionState;
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

// 小值确定化配置:短连接超时、小固定重连间隔 → 断连后毫秒级自动重连。
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

// 取一个当前空闲端口(listen 后立即关闭,后续连接将被拒绝)。
quint16 FreePort() {
  QTcpServer probe;
  probe.listen(QHostAddress::LocalHost, 0);
  const quint16 port = probe.serverPort();
  probe.close();
  return port;
}

// 接受下一个入站连接(pump 到就绪),交出所有权(setParent nullptr)供 TcpTransport 管理。
QTcpSocket* AcceptNext(QTcpServer& server, int budget_ms = 4000) {
  if (!pumpFiberUntil([&] { return server.hasPendingConnections(); }, budget_ms)) {
    return nullptr;
  }
  QTcpSocket* s = server.nextPendingConnection();
  if (s) {
    s->setParent(nullptr);
  }
  return s;
}

// 标准 echo 响应:frm_type=kResponse、session_id 原样、message_id=请求码|0x1000、payload
// echo(与 DefaultProtocolKeyStrategy 配对规则一致)。
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
// 传输终结(对端撕连接 / 我方关闭,均为 kClosed)→ 退出并置 ended=true。
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

// 服务端主动发一帧(不经 echo responder)。
void ServerSend(TcpTransport& transport, const Message& msg) {
  SystemCodec codec;
  auto encoded = codec.Encode(msg);
  ASSERT_TRUE(encoded);
  ASSERT_TRUE(
      transport.Write(SendUnit{std::move(encoded).value(), Endpoint::Default()}));
}

// 组装:node(经 TcpClientTransport)+ 返回裸客户端指针(用于观察 Generation/ConfigVersion/
// AttemptCount 与调 ApplyConfig,类似 Fake 传输取原始指针后 move 入 node)。
struct NodeWithClient {
  std::unique_ptr<ProtocolNode> node;
  TcpClientTransport* client;  // 生命周期由 node 拥有;仅作观察/重配置句柄。
};

NodeWithClient MakeNodeWithClient(const TcpClientConfig& cfg,
                                  ProtocolNodeConfig config = {}) {
  auto owner = std::make_unique<TcpClientTransport>(cfg);
  TcpClientTransport* raw = owner.get();
  auto node = std::make_unique<ProtocolNode>(
      std::move(owner), std::make_unique<SystemCodec>(), std::move(config));
  return NodeWithClient{std::move(node), raw};
}

}  // namespace

// —— ① 真实断连-重连回环 ——
// 连上 → Request 恰好一次完成(echo)→ server 断开 → **在途 Request 不被框架终结**
// (ADR-0004 D3),按其总超时收敛 → 自动重连(Generation 递增)→ server 恢复 → 新代际
// Request 成功;node 全程 Running、读循环透明续命未退出。覆盖 RT_TCP_RECONNECT_002/003/004。
TEST(TcpClientE2E, DisconnectReconnectLoopRequestsSucceedAcrossGenerations) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  auto nc = MakeNodeWithClient(FastClientConfig(port));
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  // 代际1:接受连接,silent 标志可控的 echo 对端(先应答,后静默)。
  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());
  auto silent = std::make_shared<bool>(false);
  bool echo1_ended = false;
  auto responder1 = [silent](const Message& c) -> std::vector<Message> {
    return *silent ? std::vector<Message>{} : EchoResponder(c);
  };
  auto echo1 = SpawnEcho(*server_txp1, responder1, echo1_ended);

  // 首个 Request 恰好一次完成(echo 回帧)。
  Result<Message> a{make_error_code(TransportErrc::kInternal)};
  bool a_done = false;
  auto req_a = Coro::makeTask([&] {
    a = node.Request(MakeRequest(0x0001, {0x11}), Deadline(4000ms));
    a_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return a_done; }, 6000));
  ASSERT_TRUE(a) << a.error().message();
  EXPECT_EQ(a.value().message_id, 0x1001);
  EXPECT_EQ(a.value().payload, (std::vector<std::uint8_t>{0x11}));
  const std::uint64_t gen1 = nc.client->Generation();
  EXPECT_EQ(gen1, 1u);

  // 令对端静默 → 起一个在途 Request(总超时 900ms:断链后唯一的收敛手段)。
  *silent = true;
  Result<Message> b{make_error_code(TransportErrc::kInternal)};
  bool b_done = false;
  auto req_b = Coro::makeTask([&] {
    b = node.Request(MakeRequest(0x0002, {0x22}), Deadline(900ms));
    b_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }, 3000));
  EXPECT_FALSE(b_done);

  // server 断开(abort accepted socket):在途 Request **不被** 断链终结(D3 撤销代际隔离)。
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 250);  // 让断链充分传播。
  EXPECT_FALSE(b_done) << "断链不得终结在途请求(ADR-0004 D3)";
  EXPECT_EQ(node.PendingCount(), 1u);

  // 它由自己的总超时收敛(RT_TCP_RECONNECT_002 改写后的唯一路径)。
  ASSERT_TRUE(pumpFiberUntil([&] { return b_done; }, 3000));
  ASSERT_FALSE(b);
  EXPECT_EQ(b.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node.PendingCount(), 0u);

  // node 全程 Running(读循环透明续命未退出):WaitClosed 短 deadline → kTimeout。
  auto wc = node.WaitClosed(Deadline(50ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  // 代际2:客户端自动重连 → server 接受新连接(echo 恢复),Generation 递增。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() >= 2u; }, 5000));
  EXPECT_GT(nc.client->Generation(), gen1);

  // 新代际 Request 成功(证读循环跨重连续命 + 新代际物理隔离连上)。
  Result<Message> c{make_error_code(TransportErrc::kInternal)};
  bool c_done = false;
  auto req_c = Coro::makeTask([&] {
    c = node.Request(MakeRequest(0x0004, {0xAB}), Deadline(4000ms));
    c_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return c_done; }, 6000));
  ASSERT_TRUE(c) << c.error().message();
  EXPECT_EQ(c.value().message_id, 0x1004);
  EXPECT_EQ(c.value().payload, (std::vector<std::uint8_t>{0xAB}));

  // 仅 Close 使 node 收敛。
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));

  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended && echo2_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(echo2.get());
  EXPECT_TRUE(req_a.get());
  EXPECT_TRUE(req_b.get());
  EXPECT_TRUE(req_c.get());
}

// —— ② 代际隔离已撤销(ADR-0004 D3)——
// 断连时:未开始处理的业务事件**不丢弃**(隔离丢弃计数恒 0)、在途请求**不终结**;释放
// handler 后滞留业务照常全部处理完。重连后旧代际迟到响应无匹配在途 → 归因丢弃不误配。
TEST(TcpClientE2E, DisconnectKeepsQueuedBusinessAndInFlightRequests) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  // handler 首帧进入即阻塞在 gate(模拟"运行中"),其余帧滞留业务队列未启动。
  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int started = 0;
  int completed = 0;
  ProtocolNodeConfig cfg;
  cfg.handler = [&started, &completed, gate](const Message&,
                                             HandlerContext&) -> Status {
    ++started;
    if (started == 1) {
      Coro::await(gate);  // 仅首帧挂住,后续帧释放后依次跑完。
    }
    ++completed;
    return Status{};
  };

  auto nc = MakeNodeWithClient(FastClientConfig(port), std::move(cfg));
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());

  // 在途 Request(session 0):sink 对端(不回),总超时 1200ms。
  bool echo1_ended = false;
  auto sink = [](const Message&) { return std::vector<Message>{}; };
  auto echo1 = SpawnEcho(*server_txp1, sink, echo1_ended);
  Result<Message> req{make_error_code(TransportErrc::kInternal)};
  bool req_done = false;
  auto request = Coro::makeTask([&] {
    req = node.Request(MakeRequest(0x0007, {0x01}), Deadline(1200ms));
    req_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }, 3000));

  // 对端推 4 帧业务(kState);handler 取首帧阻塞在 gate,其余 3 帧滞留队列。
  constexpr int kBusinessFrames = 4;
  for (int i = 0; i < kBusinessFrames; ++i) {
    Message biz;
    biz.frm_type = FrameType::kState;
    biz.message_id = static_cast<std::uint16_t>(0x0030 + i);
    biz.payload = {static_cast<std::uint8_t>(i)};
    ServerSend(*server_txp1, biz);
  }
  ASSERT_TRUE(pumpFiberUntil([&] { return started == 1; }, 3000));
  pumpFiberUntil([] { return false; }, 250);  // 让滞留帧全部到达入队(确定化)。

  // 物理断连:在途 Request 不被终结、未启动的 3 帧不被丢弃(代际隔离已撤销)。
  accepted1->abort();
  pumpFiberUntil([] { return false; }, 250);
  EXPECT_FALSE(req_done) << "断链不得终结在途请求(ADR-0004 D3)";
  EXPECT_EQ(node.PendingCount(), 1u);
  // 代际隔离丢弃已撤销(ADR-0004 D3),其访问器随 #110 一并删除:改由"业务事件一条不少地
  // 被处理完 + 其余丢弃计数恒 0"反证断链未丢弃任何业务。
  EXPECT_EQ(node.CloseDropCount(), 0u);
  EXPECT_EQ(node.BusinessQueueOverflowCount(), 0u);
  EXPECT_EQ(node.DroppedNoHandlerCount(), 0u);

  // 释放 gate:滞留的业务帧照常全部处理完(断链未清空业务队列)。
  gate->resolve();
  gate->close();
  ASSERT_TRUE(pumpFiberUntil([&] { return completed == kBusinessFrames; }, 4000));
  EXPECT_EQ(started, kBusinessFrames);

  // 在途请求最终由自己的总超时收敛。
  ASSERT_TRUE(pumpFiberUntil([&] { return req_done; }, 3000));
  EXPECT_EQ(req.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node.PendingCount(), 0u);

  // 代际2:重连;对端在新连接(新 socket 物理隔离)上补发旧代际迟到响应(session 0,
  // message_id 0x1007)。无匹配在途(已超时收敛)→ 归因丢弃、不误配。
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
  ASSERT_TRUE(
      pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }, 4000));
  EXPECT_EQ(node.PendingCount(), 0u);

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(request.get());
}

// —— ③ 重连节奏:固定间隔(ADR-0005 D4)——
// 确定化:小固定间隔;连上 echo 后 server 下线,相邻尝试间隔稳定在配置值附近、**不随
// 失败次数增长**(旧指数退避会 1×→2×→4× 上升)。node 全程 Running(读循环透明续命)。
TEST(TcpClientE2E, ReconnectUsesFixedIntervalWithoutGrowth) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  TcpClientConfig cfg = FastClientConfig(port);
  cfg.reconnect_interval = 120ms;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() == 1u; }, 4000));
  const std::size_t attempts_connected = nc.client->AttemptCount();

  // server 下线 + 断连:客户端进入固定间隔重试(端口被拒绝,近乎瞬时失败)。
  server.close();
  accepted1->abort();
  accepted1->deleteLater();

  std::vector<Clock::time_point> attempt_times;
  std::size_t last = attempts_connected;
  const auto budget_end = Clock::now() + 6s;
  while (attempt_times.size() < 6 && Clock::now() < budget_end) {
    const std::size_t now_count = nc.client->AttemptCount();
    if (now_count > last) {
      last = now_count;
      attempt_times.push_back(Clock::now());
    }
    boost::this_fiber::sleep_for(2ms);
  }
  ASSERT_GE(attempt_times.size(), 5u);
  std::vector<Clock::duration> intervals;
  for (std::size_t i = 1; i < attempt_times.size(); ++i) {
    intervals.push_back(attempt_times[i] - attempt_times[i - 1]);
  }
  for (const auto& d : intervals) {
    EXPECT_GT(d, 50ms) << "间隔不得塌陷(零间隔会退化为紧循环)";
    EXPECT_LT(d, 400ms) << "间隔应稳定在配置的 120ms 附近(容调度抖动)";
  }
  EXPECT_LT(intervals.back(), intervals.front() + 100ms)
      << "固定间隔重连不得随失败次数增长";

  // node 未 Closed(读循环透明续命跨重连):WaitClosed 短 deadline → kTimeout。
  auto wc = node.WaitClosed(Deadline(30ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
}

// —— ④ 重配置端点切换 ——
// ApplyConfig 改 host/port(指向第二个 server)→ 立即切新代际(不等剩余间隔)→ 连上
// server_b、新 Request 成功;旧在途请求由总超时收敛;Generation 与 ConfigVersion 各自
// 递增。覆盖 RT_TCP_RECONFIG_005、RT_DATA_STATE。
TEST(TcpClientE2E, ReconfigEndpointSwitchConnectsNewServer) {
  QTcpServer server_a;
  ASSERT_TRUE(server_a.listen(QHostAddress::LocalHost, 0));
  QTcpServer server_b;
  ASSERT_TRUE(server_b.listen(QHostAddress::LocalHost, 0));

  // 大重连间隔确保"切端点立即尝试(不等剩余间隔)"可判别。
  TcpClientConfig cfg = FastClientConfig(server_a.serverPort());
  cfg.reconnect_interval = 1500ms;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  // 代际1:连到 server_a,silent 对端(在途 Request 挂起)。
  QTcpSocket* accepted_a = AcceptNext(server_a);
  ASSERT_NE(accepted_a, nullptr);
  auto server_txp_a = std::make_shared<TcpTransport>(accepted_a);
  ASSERT_TRUE(server_txp_a->Start());
  bool echo_a_ended = false;
  auto sink = [](const Message&) { return std::vector<Message>{}; };
  auto echo_a = SpawnEcho(*server_txp_a, sink, echo_a_ended);
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() == 1u; }, 4000));
  EXPECT_EQ(nc.client->ConfigVersion(), 1u);

  Result<Message> a{make_error_code(TransportErrc::kInternal)};
  bool a_done = false;
  auto req_a = Coro::makeTask([&] {
    a = node.Request(MakeRequest(0x0005, {0x55}), Deadline(900ms));
    a_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }, 3000));

  // ApplyConfig 切端点到 server_b(新版本 2)。
  TcpClientConfig to_b = FastClientConfig(server_b.serverPort());
  to_b.reconnect_interval = 1500ms;
  const auto t0 = Clock::now();
  ASSERT_TRUE(nc.client->ApplyConfig(to_b, 2));
  EXPECT_EQ(nc.client->ConfigVersion(), 2u);
  EXPECT_EQ(nc.client->ConfigChangeCount(), 1u);

  // 新代际立即尝试新端点(不等 1500ms 间隔)→ 迅速连上 server_b。
  QTcpSocket* accepted_b = AcceptNext(server_b, 3000);
  ASSERT_NE(accepted_b, nullptr);
  EXPECT_LT(Clock::now() - t0, 1000ms) << "端点变化应立即重试,不等剩余重连间隔";
  auto server_txp_b = std::make_shared<TcpTransport>(accepted_b);
  ASSERT_TRUE(server_txp_b->Start());
  bool echo_b_ended = false;
  auto echo_b = SpawnEcho(*server_txp_b, EchoResponder, echo_b_ended);

  // 旧在途请求由其总超时收敛(切端点不再主动终结,ADR-0004 D3)。
  ASSERT_TRUE(pumpFiberUntil([&] { return a_done; }, 4000));
  ASSERT_FALSE(a);
  EXPECT_EQ(a.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(node.PendingCount(), 0u);

  // Generation 与 ConfigVersion 各自递增(两轴独立)。
  EXPECT_GE(nc.client->Generation(), 2u);
  EXPECT_EQ(nc.client->ConfigVersion(), 2u);

  // 新 Request 在 server_b 上成功。
  Result<Message> b{make_error_code(TransportErrc::kInternal)};
  bool b_done = false;
  auto req_b = Coro::makeTask([&] {
    b = node.Request(MakeRequest(0x0006, {0x66}), Deadline(4000ms));
    b_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return b_done; }, 6000));
  ASSERT_TRUE(b) << b.error().message();
  EXPECT_EQ(b.value().message_id, 0x1006);
  EXPECT_EQ(b.value().payload, (std::vector<std::uint8_t>{0x66}));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  server_txp_a->RequestClose();
  server_txp_b->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_a_ended && echo_b_ended; }));
  EXPECT_TRUE(echo_a.get());
  EXPECT_TRUE(echo_b.get());
  EXPECT_TRUE(req_a.get());
  EXPECT_TRUE(req_b.get());
}

// —— ⑤a Close 端到端收敛(Connected)——
// 连上后 Close → node Closing→Closed、WaitClosed 完成;物理连接撕掉(对端 echo 读观测
// 传输终结退出)。覆盖 RT_LIFECYCLE。
TEST(TcpClientE2E, CloseWhileConnectedConvergesAndTearsConnection) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  auto nc = MakeNodeWithClient(FastClientConfig(server.serverPort()));
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  QTcpSocket* accepted = AcceptNext(server);
  ASSERT_NE(accepted, nullptr);
  auto server_txp = std::make_shared<TcpTransport>(accepted);
  ASSERT_TRUE(server_txp->Start());
  bool echo_ended = false;
  auto echo = SpawnEcho(*server_txp, EchoResponder, echo_ended);
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() == 1u; }, 4000));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  // 物理连接撕掉 → 对端 echo 读观测传输终结退出。
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }, 3000));
  EXPECT_TRUE(echo.get());
  server_txp->RequestClose();
}

// —— ⑤b Close 端到端收敛(重连等待中)——
// 重连等待中 Close → 掐断当前尝试/重连,node 迅速收敛到 Closed,连接状态归 Disconnected。
TEST(TcpClientE2E, CloseDuringReconnectWaitCutsReconnectAndConverges) {
  TcpClientConfig cfg = FastClientConfig(FreePort());
  cfg.reconnect_interval = 800ms;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  // 进入重连等待(第一次失败后)。
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return nc.client->State() == ConnectionState::kReconnecting; }, 3000));

  const auto t0 = Clock::now();
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(1500ms)));
  EXPECT_LT(Clock::now() - t0, 500ms) << "应掐断间隔等待、迅速收敛";
  // 链路对调用方立即不可用;传输自身的 connect-loop 随后收敛到 Disconnected(node 的
  // 收敛不再等待传输连接状态机——读循环由对外通道关闭直接唤醒,ADR-0004 D6)。
  EXPECT_EQ(nc.client->CurrentLinkState(), transport::LinkState::kDown);
  EXPECT_TRUE(pumpFiberUntil(
      [&] { return nc.client->State() == ConnectionState::kDisconnected; }, 2000));
}
