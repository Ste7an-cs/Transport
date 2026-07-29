// -----------------------------------------------------------------------------
// tcp_client_e2e_test.cpp — P3-4 真实 TCP 断连-重连回环 + 代际隔离端到端(P3 收官)
//
// 承接 ADR-0003 D11、SDD §4 P3 验收、RT_TCP_RECONNECT 全 / RT_TCP_RECONFIG 全 /
// RT_LIFECYCLE_002 / RT_DATA_STATE(seed #20)。在 fiber 调度器(coro_test_main)内用
// 本机真实 TCP 回环把 `ProtocolNode` + `TcpClientTransport` 的完整链路端到端跑通:
//
//   请求方 = 真 ProtocolNode(真 TcpClientTransport(client) + SystemCodec)。
//   对端    = 可控 echo server:裸 QTcpServer + 真 TcpTransport(accepted) + SystemCodec,
//             收 kCommand → 回 kResponse(session_id 原样、message_id|0x1000、payload echo);
//             测试可令其静默(不回帧)、abort(模拟物理断连)、下线/上线(拒绝/接受连接)。
//
// 覆盖五条 SDD §4 P3 验收:
//   ① 真实断连-重连回环:Request 恰好一次完成(echo)→ server 断开 → 在途恰好一次
//      kConnection → 自动重连(退避,Generation 递增)→ 新代际 Request 成功;node 全程
//      Running、读循环透明续命未退出。
//   ② 代际隔离:新 socket 物理隔离 + FailAll 清在途;旧代际迟到响应归因丢弃不误配;断连
//      时未启动排队业务 → GenerationIsolationDropCount 计数。
//   ③ 重连退避:断开后按退避序列重试(小参数 + 关 jitter,断言时序上升到 cap);稳定阈值
//      缩短验证重置语义。
//   ④ 重配置端点切换:ApplyConfig 改 host/port → 旧在途 kConnection + 新代际立即尝试
//      (不等退避)→ 连上第二个 server、新 Request 成功;Generation 与 ConfigVersion 各自递增。
//   ⑤ Close 端到端收敛:Connected / 退避中 Close → node Closing→Closed,WaitClosed 完成,
//      连接撕掉。
//
// 确定化 / 防 flake 手段:所有连接超时/退避/稳定阈值用毫秒级小值注入;jitter 关闭以断言
// 确定退避序列;沉默对端 + 无 deadline 在途请求靠断连收敛(而非计时);端口用 FreePort
// (listen 后即关)确定"被拒绝";退避时序断言采用"相邻间隔递增/上界"的宽松容差(容调度
// 抖动)。所有 spawn 的 fiber 均在用例末 join,避免跨用例泄漏。
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
#include "transport/Endpoint.hpp"
#include "transport/Error.hpp"
#include "transport/Message.hpp"
#include "transport/ProtocolNode.hpp"
#include "transport/TcpClientTransport.hpp"
#include "transport/TcpTransport.hpp"
#include "transport/TransportTypes.hpp"
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

// 服务端主动发一帧(不经 echo responder):供注入旧代际迟到响应。
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

// —— ① 真实断连-重连回环(SDD §4 P3 验收 1)——
// 连上 → Request 恰好一次完成(echo)→ server 断开 → 在途 Request 恰好一次 kConnection →
// 自动重连(Generation 递增)→ server 恢复 → 新代际 Request 成功;node 全程 Running、读循环
// 透明续命未退出(响应经同一未退出读循环路由)。覆盖 RT_TCP_RECONNECT_002/003/004。
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

  // 令对端静默 → 起一个在途 Request(无 deadline,靠断连收敛)。
  *silent = true;
  Result<Message> b{make_error_code(TransportErrc::kInternal)};
  bool b_done = false;
  auto req_b = Coro::makeTask([&] {
    b = node.Request(MakeRequest(0x0002, {0x22}));
    b_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }, 3000));
  EXPECT_FALSE(b_done);

  // server 断开(abort accepted socket):在途 Request 恰好一次 kConnection,在途清空。
  accepted1->abort();
  ASSERT_TRUE(pumpFiberUntil([&] { return b_done; }, 4000));
  ASSERT_FALSE(b);
  EXPECT_EQ(b.error(), make_error_code(TransportErrc::kConnection));
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

// —— ② 代际隔离(SDD §4 P3 验收 2)——
// 断连时未启动排队业务 → GenerationIsolationDropCount 计数;新 socket 物理隔离 + FailAll 清
// 在途;重连后旧代际迟到响应(旧 session 键)无匹配在途 → 归因丢弃(UnmatchedResponseCount)
// 不误配。覆盖 RT_TCP_RECONNECT_004、3.1.7.4。
TEST(TcpClientE2E, GenerationIsolationDropsQueuedAndAttributesStaleResponse) {
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
    Coro::await(gate);
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

  // 在途 Request(session 0):sink 对端(不回),使 FailAll 清在途可观测。
  bool echo1_ended = false;
  auto sink = [](const Message&) { return std::vector<Message>{}; };
  auto echo1 = SpawnEcho(*server_txp1, sink, echo1_ended);
  Result<Message> req{make_error_code(TransportErrc::kInternal)};
  bool req_done = false;
  auto request = Coro::makeTask([&] {
    req = node.Request(MakeRequest(0x0007, {0x01}));
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

  // 物理断连:在途 Request 恰好一次 kConnection;未启动 3 帧 → 代际隔离丢弃。
  accepted1->abort();
  ASSERT_TRUE(pumpFiberUntil([&] { return req_done; }, 4000));
  EXPECT_EQ(req.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(node.PendingCount(), 0u);
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return node.GenerationIsolationDropCount() >= 1; }, 4000));
  EXPECT_EQ(node.GenerationIsolationDropCount(),
            static_cast<std::size_t>(kBusinessFrames - 1));
  // 隔离丢弃不误记为 close_drop / overflow / dropped_no_handler。
  EXPECT_EQ(node.CloseDropCount(), 0u);
  EXPECT_EQ(node.BusinessQueueOverflowCount(), 0u);
  EXPECT_EQ(node.DroppedNoHandlerCount(), 0u);

  // 代际2:重连;对端在新连接(新 socket 物理隔离)上补发旧代际迟到响应(session 0,
  // message_id 0x1007)。无匹配在途(已 FailAll 清空)→ 归因丢弃、不误配。
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
  EXPECT_EQ(node.UnmatchedResponseCount(), 1u);
  EXPECT_EQ(node.PendingCount(), 0u);

  // 运行中 handler 未被强杀:释放 gate → 首帧跑完。
  EXPECT_EQ(started, 1);
  EXPECT_EQ(completed, 0);
  gate->resolve();
  gate->close();
  ASSERT_TRUE(pumpFiberUntil([&] { return completed == 1; }, 3000));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(request.get());
}

// —— ③a 重连退避:断开后按退避序列重试,间隔递增到 cap(SDD §4 P3 验收 3)——
// 确定化:jitter 关闭 + 小 initial/cap;连上 echo 后 server 下线,相邻尝试间隔 1×→2×→cap
// 递增(容调度抖动的宽松容差)。node 全程 Running(读循环透明续命)。
TEST(TcpClientE2E, ReconnectBackoffSequenceRisesToCap) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  TcpClientConfig cfg = FastClientConfig(port);
  cfg.initial_backoff = 60ms;
  cfg.max_backoff = 240ms;
  cfg.backoff_multiplier = 2.0;
  cfg.jitter_enabled = false;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() == 1u; }, 4000));
  const std::size_t attempts_connected = nc.client->AttemptCount();

  // server 下线 + 断连:客户端进入退避重试(端口被拒绝,近乎瞬时失败,间隔≈退避)。
  server.close();
  accepted1->abort();
  accepted1->deleteLater();

  // 采集断连后前若干次尝试时刻(端口被拒绝近乎瞬时失败,相邻间隔≈退避时长)。多采几个
  // 样本以覆盖"从低于 cap 上升到 cap"整段(采样起点落在升级中途亦稳健)。
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
  const auto first_interval = intervals.front();
  const auto last_interval = intervals.back();
  // 退避序列从低于 cap(240ms)上升并触顶 cap:首个间隔明显低于 cap,末间隔逼近 cap,
  // 且整体清晰上升(容调度抖动的宽松容差)。
  EXPECT_LT(first_interval, 180ms) << "起始退避应低于 cap(初值 60ms / 一次升级 120ms)";
  EXPECT_GT(last_interval, 180ms) << "退避应上升触顶 cap(240ms)";
  EXPECT_GT(last_interval, first_interval + 40ms) << "退避序列应清晰上升到 cap";
  EXPECT_LT(last_interval, 240ms + 160ms);  // 未超过 cap 太多。

  // node 未 Closed(读循环透明续命跨退避):WaitClosed 短 deadline → kTimeout。
  auto wc = node.WaitClosed(Deadline(30ms));
  ASSERT_FALSE(wc);
  EXPECT_EQ(wc.error(), make_error_code(TransportErrc::kTimeout));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
}

// —— ③b 稳定阈值重置:稳定连接 ≥ stable_reset_after 后断开,退避从 initial 重置(SDD §4
// P3 验收 3)——
// 确定化:先 server 下线令退避升到 cap,再上线连上并稳定 > 阈值 → 下次断开首个退避 = initial
// (远小于 cap)。以"断连到重连的时延"区分重置(<150ms,initial 60ms)vs 未重置(cap 240ms)。
TEST(TcpClientE2E, StableConnectionResetsBackoffLevel) {
  const quint16 port = FreePort();  // 先无 server → 连接被拒绝。

  TcpClientConfig cfg = FastClientConfig(port);
  cfg.initial_backoff = 60ms;
  cfg.max_backoff = 240ms;
  cfg.backoff_multiplier = 2.0;
  cfg.jitter_enabled = false;
  cfg.stable_reset_after = 250ms;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  // 退避升级:多次失败后退避级别攀到 cap。
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->AttemptCount() >= 4; }, 5000));

  // server 上线 → 连上;稳定 > stable_reset_after(250ms)→ 下次断开重置退避级别。
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, port));
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() == 1u; }, 5000));
  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);
  auto server_txp1 = std::make_shared<TcpTransport>(accepted1);
  ASSERT_TRUE(server_txp1->Start());
  bool echo1_ended = false;
  auto echo1 = SpawnEcho(*server_txp1, EchoResponder, echo1_ended);
  boost::this_fiber::sleep_for(350ms);  // 稳定连接 > 阈值。

  // 断开:退避级别应已重置为 initial(60ms)→ 下次尝试快速重连(server 仍在)。
  const auto t0 = Clock::now();
  accepted1->abort();
  ASSERT_TRUE(pumpFiberUntil([&] { return nc.client->Generation() >= 2u; }, 4000));
  const auto reconnect_latency = Clock::now() - t0;
  EXPECT_LT(reconnect_latency, 150ms)
      << "稳定 ≥ 阈值后重置为 initial(60ms);未重置则应停在 cap(240ms)";

  // 收尾:接受重连出的新连接以清理(重置后已重连,server 有 pending 连接)。
  QTcpSocket* accepted2 = AcceptNext(server, 3000);
  ASSERT_NE(accepted2, nullptr);
  auto server_txp2 = std::make_shared<TcpTransport>(accepted2);
  ASSERT_TRUE(server_txp2->Start());
  bool echo2_ended = false;
  auto echo2 = SpawnEcho(*server_txp2, EchoResponder, echo2_ended);

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  server_txp1->RequestClose();
  server_txp2->RequestClose();
  EXPECT_TRUE(pumpFiberUntil([&] { return echo1_ended && echo2_ended; }));
  EXPECT_TRUE(echo1.get());
  EXPECT_TRUE(echo2.get());
}

// —— ④ 重配置端点切换(SDD §4 P3 验收 4)——
// ApplyConfig 改 host/port(指向第二个 server)→ 旧在途 kConnection + 新代际立即尝试(不等
// 退避)→ 连上新 server、新 Request 成功;Generation 与 ConfigVersion 各自递增。覆盖
// RT_TCP_RECONFIG_005、RT_DATA_STATE。
TEST(TcpClientE2E, ReconfigEndpointSwitchFailsInFlightAndConnectsNewServer) {
  QTcpServer server_a;
  ASSERT_TRUE(server_a.listen(QHostAddress::LocalHost, 0));
  QTcpServer server_b;
  ASSERT_TRUE(server_b.listen(QHostAddress::LocalHost, 0));

  // 大退避确保"切端点立即尝试(不等退避)"可判别:仅端点变化应立即以新端点重试。
  TcpClientConfig cfg = FastClientConfig(server_a.serverPort());
  cfg.initial_backoff = 1500ms;
  cfg.max_backoff = 1500ms;
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
    a = node.Request(MakeRequest(0x0005, {0x55}));  // 无 deadline,靠切端点断连收敛。
    a_done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }, 3000));

  // ApplyConfig 切端点到 server_b(新版本 2)。
  TcpClientConfig to_b = FastClientConfig(server_b.serverPort());
  to_b.initial_backoff = 1500ms;
  to_b.max_backoff = 1500ms;
  const auto t0 = Clock::now();
  ASSERT_TRUE(nc.client->ApplyConfig(to_b, 2));
  EXPECT_EQ(nc.client->ConfigVersion(), 2u);
  EXPECT_EQ(nc.client->ConfigChangeCount(), 1u);

  // 旧在途以 kConnection 终结(端点变化掐断当前连接)。
  ASSERT_TRUE(pumpFiberUntil([&] { return a_done; }, 4000));
  ASSERT_FALSE(a);
  EXPECT_EQ(a.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(node.PendingCount(), 0u);

  // 新代际立即尝试新端点(不等 1500ms 退避)→ 迅速连上 server_b。
  QTcpSocket* accepted_b = AcceptNext(server_b, 3000);
  ASSERT_NE(accepted_b, nullptr);
  EXPECT_LT(Clock::now() - t0, 1000ms) << "端点变化应立即重试,不等剩余退避";
  auto server_txp_b = std::make_shared<TcpTransport>(accepted_b);
  ASSERT_TRUE(server_txp_b->Start());
  bool echo_b_ended = false;
  auto echo_b = SpawnEcho(*server_txp_b, EchoResponder, echo_b_ended);

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

// —— ⑤a Close 端到端收敛(Connected)——(SDD §4 P3 验收 5)
// 连上后 Close → node Closing→Closed、WaitClosed 完成;物理连接撕掉(对端 echo 读观测
// kConnection 退出)。覆盖 RT_LIFECYCLE。
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
  // 物理连接撕掉 → 对端 echo 读观测 kConnection 退出。
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }, 3000));
  EXPECT_TRUE(echo.get());
  server_txp->RequestClose();
}

// —— ⑤b Close 端到端收敛(退避中)——(SDD §4 P3 验收 5)
// 退避重连中 Close → 掐断当前尝试/重连,node 迅速收敛到 Closed,连接状态归 Disconnected。
TEST(TcpClientE2E, CloseDuringBackoffCutsReconnectAndConverges) {
  TcpClientConfig cfg = FastClientConfig(FreePort());
  cfg.initial_backoff = 300ms;
  cfg.max_backoff = 300ms;
  auto nc = MakeNodeWithClient(cfg);
  ProtocolNode& node = *nc.node;
  ASSERT_TRUE(node.Start());

  // 进入退避(第一次失败后)。
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return nc.client->State() == ConnectionState::kReconnecting; }, 3000));

  const auto t0 = Clock::now();
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(1500ms)));
  EXPECT_LT(Clock::now() - t0, 300ms) << "应掐断退避等待、迅速收敛";
  EXPECT_EQ(nc.client->State(), ConnectionState::kDisconnected);
}
