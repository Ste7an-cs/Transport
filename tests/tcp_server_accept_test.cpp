// -----------------------------------------------------------------------------
// tcp_server_accept_test.cpp — P4-6 TCP 服务端 accept 层端到端(真实回环)
//
// 承接 ADR-0003 D12 Q6、RT_DESIGN_004(每连接一 node)、RT_IF_TCP、ADR-0002 D3′
// (服务端连接非重连,连接生命=节点生命)。在 fiber 调度器(coro_test_main)内用本机真实
// TCP 回环把 `TcpServer` accept 层跑通:
//
//   服务端 = 真 TcpServer(每接受一条连接 → 工厂装配 ProtocolNode(内层 TcpTransport 直接
//            接管已接受 socket + SystemCodec + echo handler)+ Start + 记入子 node 列表)。
//   客户端 = 真 ProtocolNode(真 TcpClientTransport(连到 server 端口)+ SystemCodec),
//            经 Request 发 kCommand、收 server handler 经 ctx.Send 回的 kResponse。
//
// 相关性说明:server handler 经 ProtocolNode::Send 回帧会盖新 session_id(协议语义),故
// 客户端用**命令码关联键策略**(忽略 session_id,仅 message_id 命令码配对,D9 允许的 node
// 级可注入),使 echo 回帧仍与请求配对成立。每客户端至多一条在途请求,键不相撞。
//
// 覆盖验收:① 单连接请求-响应;② 多连接每连接独立 node 互不影响;③ 对端断开 → 对应子 node
// Closing→Closed 并从列表注销、其余不受影响;④ server Close → 停 accept + 全部子 node 收敛;
// ⑤ 监听失败(端口占用)可观测。确定化:短 deadline + pumpFiberUntil 轮询,防 flake。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QHostAddress>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "detail/result.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/io/tcp/TcpServer.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "transport/io/tcp/TcpClientConfig.hpp"
#include "transport/io/tcp/TcpServerConfig.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::ConnectionState;
using transport::FrameType;
using transport::HandlerContext;
using transport::ITransport;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolKey;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::SystemCodec;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpServer;
using transport::TcpServerConfig;
using transport::TransportErrc;
using transport::kResponseMarker;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

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

// 命令码关联键策略(忽略 session_id):请求键=命令码;响应键=命令码去 marker 位归一化。
// server handler 经 ctx.Send 回帧会盖新 session_id,故客户端不能以 session_id 入键。
ProtocolNodeConfig ClientNodeConfig() {
  ProtocolNodeConfig cfg;
  cfg.key_strategy.request_key = [](const Message& m) {
    return static_cast<ProtocolKey>(m.message_id);
  };
  cfg.key_strategy.response_key = [](const Message& m) {
    return static_cast<ProtocolKey>(m.message_id & ~kResponseMarker);
  };
  return cfg;
}

// 客户端连接配置:短连接超时/小退避,毫秒级连上本机 server。
TcpClientConfig FastClientConfig(std::uint16_t port) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 500ms;
  cfg.reconnect_interval = 20ms;
  return cfg;
}

// server 每连接 node 工厂:内层 TcpTransport(已接受 socket)+ SystemCodec + echo handler。
// handler 收 kCommand 业务帧 → ctx.Send 回一帧 kResponse(message_id|marker、payload echo),
// 与客户端命令码关联键配对成立。
TcpServer::NodeFactory EchoFactory() {
  return [](std::unique_ptr<ITransport> transport)
             -> std::unique_ptr<ProtocolNode> {
    ProtocolNodeConfig cfg;
    cfg.handler = [](const Message& msg, HandlerContext& ctx) -> Coro::Result<void> {
      Message reply;
      reply.frm_type = FrameType::kResponse;  // 终结帧(客户端 IsTerminal 认)。
      reply.message_id = static_cast<std::uint16_t>(msg.message_id | kResponseMarker);
      reply.payload = msg.payload;  // 回显 payload。
      return ctx.Send(std::move(reply));
    };
    return std::make_unique<ProtocolNode>(
        std::move(transport), std::make_unique<SystemCodec>(), std::move(cfg));
  };
}

TcpServerConfig LoopbackServerConfig() {
  TcpServerConfig cfg;
  cfg.bind_addr = "127.0.0.1";
  cfg.port = 0;  // OS 分配。
  return cfg;
}

// 组装:客户端 node(经 TcpClientTransport 连到 port)+ 裸客户端指针(观察连接态,生命由
// node 拥有;类似 e2e 的 NodeWithClient)。
struct ClientNode {
  std::unique_ptr<ProtocolNode> node;
  TcpClientTransport* client;  // 观察 State()/Generation();生命由 node 拥有。
};

ClientNode MakeClientNode(std::uint16_t port) {
  auto owner = std::make_unique<TcpClientTransport>(FastClientConfig(port));
  TcpClientTransport* raw = owner.get();
  auto node = std::make_unique<ProtocolNode>(
      std::move(owner), std::make_unique<SystemCodec>(), ClientNodeConfig());
  return ClientNode{std::move(node), raw};
}

// 起客户端 node 并等其连上(TcpClientTransport.Write 非 Connected 态立即返 kConnection,
// 故须先连上再发 Request)。
void StartAndAwaitConnected(ClientNode& c) {
  ASSERT_TRUE(c.node->Start());
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return c.client->State() == ConnectionState::kConnected; }, 4000));
}

// 客户端发一次请求并断言拿到 echo 响应(命令码 message_id、payload 回显)。
void ExpectRequestEcho(ProtocolNode& client, std::uint16_t message_id,
                       std::vector<std::uint8_t> payload) {
  Coro::Result<Message> resp = client.Request(MakeRequest(message_id, payload),
                                        Deadline(4000ms));
  ASSERT_TRUE(resp) << resp.error().message();
  EXPECT_EQ(resp.value().message_id,
            static_cast<std::uint16_t>(message_id | kResponseMarker));
  EXPECT_EQ(resp.value().payload, payload);
}

}  // namespace

// —— ① 单连接请求-响应(验收 1)——
// client 连入 → server 派生 node → node 经该连接完成一次请求-响应(ProtocolNode + echo）。
TEST(TcpServerAccept, SingleConnectionRequestResponse) {
  TcpServer server(LoopbackServerConfig(), EchoFactory());
  ASSERT_TRUE(server.Start()) << server.LastError().message();
  ASSERT_TRUE(server.IsListening());
  const std::uint16_t port = server.LocalPort();
  ASSERT_NE(port, 0);

  auto client = MakeClientNode(port);
  StartAndAwaitConnected(client);

  ExpectRequestEcho(*client.node, 0x0007, {0xC0, 0xDE});

  // server 已派生恰好一个子 node。
  ASSERT_TRUE(pumpFiberUntil([&] { return server.AcceptedCount() == 1u; }, 3000));
  EXPECT_EQ(server.ActiveNodeCount(), 1u);
  EXPECT_EQ(server.AcceptErrorCount(), 0u);

  ASSERT_TRUE(client.node->Close());
  EXPECT_TRUE(client.node->WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(server.Close());
  EXPECT_TRUE(server.WaitClosed(Deadline(2000ms)));
}

// —— ② 多连接:每连接一独立 node,互不影响(验收 2)——
TEST(TcpServerAccept, MultipleIndependentConnections) {
  TcpServer server(LoopbackServerConfig(), EchoFactory());
  ASSERT_TRUE(server.Start());
  const std::uint16_t port = server.LocalPort();

  auto c1 = MakeClientNode(port);
  auto c2 = MakeClientNode(port);
  auto c3 = MakeClientNode(port);
  StartAndAwaitConnected(c1);
  StartAndAwaitConnected(c2);
  StartAndAwaitConnected(c3);

  // 各连接独立完成请求-响应(不同 payload,证互不串扰)。
  ExpectRequestEcho(*c1.node, 0x0011, {0x01});
  ExpectRequestEcho(*c2.node, 0x0022, {0x02, 0x02});
  ExpectRequestEcho(*c3.node, 0x0033, {0x03, 0x03, 0x03});

  ASSERT_TRUE(pumpFiberUntil([&] { return server.AcceptedCount() == 3u; }, 4000));
  EXPECT_EQ(server.ActiveNodeCount(), 3u);

  // 再各发一次,证连接仍独立可用。
  ExpectRequestEcho(*c1.node, 0x0011, {0xAA});
  ExpectRequestEcho(*c2.node, 0x0022, {0xBB});

  ASSERT_TRUE(c1.node->Close());
  ASSERT_TRUE(c2.node->Close());
  ASSERT_TRUE(c3.node->Close());
  EXPECT_TRUE(server.Close());
  EXPECT_TRUE(server.WaitClosed(Deadline(2000ms)));
}

// —— ③ 对端断开 → 对应子 node Closing→Closed 并从列表注销;其余不受影响(验收 3)——
TEST(TcpServerAccept, PeerDisconnectDeregistersChildOthersUnaffected) {
  TcpServer server(LoopbackServerConfig(), EchoFactory());
  ASSERT_TRUE(server.Start());
  const std::uint16_t port = server.LocalPort();

  auto c1 = MakeClientNode(port);
  auto c2 = MakeClientNode(port);
  StartAndAwaitConnected(c1);
  StartAndAwaitConnected(c2);
  ExpectRequestEcho(*c1.node, 0x0041, {0x41});
  ExpectRequestEcho(*c2.node, 0x0042, {0x42});
  ASSERT_TRUE(pumpFiberUntil([&] { return server.ActiveNodeCount() == 2u; }, 4000));

  // c1 完全关闭 → 撕物理连接 → server 侧对应连接对端断开 → 子 node 自终注销。
  ASSERT_TRUE(c1.node->Close());
  EXPECT_TRUE(c1.node->WaitClosed(Deadline(2000ms)));

  ASSERT_TRUE(pumpFiberUntil([&] { return server.ActiveNodeCount() == 1u; }, 4000));
  EXPECT_EQ(server.ClosedConnectionCount(), 1u);
  EXPECT_EQ(server.AcceptedCount(), 2u);  // 累计接受数不回退。
  EXPECT_TRUE(server.IsListening());       // server 仍监听。

  // 其余连接(c2)不受影响,仍可请求-响应。
  ExpectRequestEcho(*c2.node, 0x0042, {0x99});

  ASSERT_TRUE(c2.node->Close());
  EXPECT_TRUE(server.Close());
  EXPECT_TRUE(server.WaitClosed(Deadline(2000ms)));
}

// —— ④ server Close → 停 accept + 全部子 node 收敛(验收 4)——
TEST(TcpServerAccept, ServerCloseStopsAcceptAndConvergesChildren) {
  TcpServer server(LoopbackServerConfig(), EchoFactory());
  ASSERT_TRUE(server.Start());
  const std::uint16_t port = server.LocalPort();

  auto c1 = MakeClientNode(port);
  auto c2 = MakeClientNode(port);
  StartAndAwaitConnected(c1);
  StartAndAwaitConnected(c2);
  ExpectRequestEcho(*c1.node, 0x0051, {0x51});
  ExpectRequestEcho(*c2.node, 0x0052, {0x52});
  ASSERT_TRUE(pumpFiberUntil([&] { return server.ActiveNodeCount() == 2u; }, 4000));

  // server Close:停监听 + 全部子 node 收敛。
  EXPECT_TRUE(server.Close());
  EXPECT_TRUE(server.WaitClosed(Deadline(3000ms)));
  EXPECT_EQ(server.ActiveNodeCount(), 0u);
  EXPECT_FALSE(server.IsListening());

  // 停 accept 后新连接被拒绝(端口已关)。
  auto* probe = new QTcpSocket();
  auto connecting =
      Coro::coro(probe).connectToHost(QHostAddress::LocalHost, port);
  EXPECT_FALSE(Coro::await_for(connecting, 600ms));
  probe->abort();
  probe->deleteLater();

  // 幂等再关成功。
  EXPECT_TRUE(server.Close());

  c1.node->Close();
  c2.node->Close();
}

// —— ⑤ 监听失败(端口占用)可观测(验收 5)——
TEST(TcpServerAccept, ListenFailureIsObservable) {
  TcpServer occupier(LoopbackServerConfig(), EchoFactory());
  ASSERT_TRUE(occupier.Start());
  const std::uint16_t port = occupier.LocalPort();
  ASSERT_NE(port, 0);

  // 第二个 server 绑同一端口 → 监听失败可观测。
  TcpServerConfig conflict;
  conflict.bind_addr = "127.0.0.1";
  conflict.port = port;
  TcpServer clash(conflict, EchoFactory());

  Coro::Result<void> started = clash.Start();
  EXPECT_FALSE(started);
  EXPECT_FALSE(clash.IsListening());
  EXPECT_TRUE(clash.LastError()) << "监听失败应可观测(LastError 非空)";
  EXPECT_EQ(clash.LocalPort(), 0);

  EXPECT_TRUE(occupier.Close());
  EXPECT_TRUE(occupier.WaitClosed(Deadline(2000ms)));
}
