// -----------------------------------------------------------------------------
// protocol_node_tcp_e2e_test.cpp — ProtocolNode + SystemCodec 跑在真实 TCP 上的端到端
//                                   (#181 迁移件,由 `tcp_client_e2e_test.cpp` 重写)
//
// 旧文件建在 `TcpClientTransport`(已删,ADR-0011 **D1**)+ `ProtocolNodeConfig::handler`
// (已废止,ADR-0009 **D1**:入站只剩订阅一条通路)+ 一堆已删除的计数器之上。其五组用例
// 在新形态下只剩两条事实仍然成立且**未被别处覆盖**——它们的共同点是:**只有真链路才测
// 得出来**,`protocol_node_test.cpp` 的假传输给不了。
//
//   ① **断链对交互层透明**:物理断连既不终结在途交互、也不关闭节点;泵自行重连,新交互
//      在新连接上照常成功(DD-11 / ADR-0007 D2 / ADR-0011 D2)。假传输没有"链路"这一层,
//      这条只能在真 TCP 上验。
//   ② **入站方向端到端**:对端推来的业务帧经真 socket → 泵 → read_queue → 节点读循环 →
//      订阅信箱,宿主回的一帧再经 `Send()` 原路出去被对端收到(RT_INBOUND_001)。
//
// 其余三组的处置见 #181 的判定表(② 代际隔离/handler 一组已随 ADR-0009 与 ADR-0008 D10
// 撤销;③ 固定间隔已迁至 `tcp_transport_reconnect_test.cpp`;④ `ApplyConfig` 待 **D11**
// 裁决;⑤ "node Close 撕掉物理连接"已被撤销——节点**不管传输的生命周期**)。
//
// 对端形态:**裸 `QTcpSocket` + `SystemCodec`**,不再用 `TcpTransport(accepted)`——那个
// 构造已随 D1 删除,服务端侧的泵形态属 `TcpServer`,本轮不做(**D10**)。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <QByteArray>
#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/codec/SystemCodec.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/tcp/TcpConfig.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::AnyOfType;
using transport::FrameType;
using transport::LinkState;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemCodec;
using transport::TcpConfig;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";
constexpr std::uint8_t kProtocolId = 0x2A;

TcpConfig ConfigFor(std::uint16_t port, std::chrono::milliseconds silence) {
  TcpConfig config;
  config.host = kLoopback;
  config.port = port;
  config.silence_timeout = silence;
  return config;
}

ProtocolNodeConfig NodeConfig() {
  ProtocolNodeConfig config;
  config.protocol_id = kProtocolId;
  return config;
}

class LoopbackServer {
 public:
  LoopbackServer() { EXPECT_TRUE(server_.listen(QHostAddress::LocalHost, 0)); }

  std::uint16_t port() const {
    return static_cast<std::uint16_t>(server_.serverPort());
  }

  QTcpSocket* Accept(int budget_ms = 4000) {
    if (!pumpFiberUntil([this] { return server_.hasPendingConnections(); },
                        budget_ms)) {
      return nullptr;
    }
    return server_.nextPendingConnection();
  }

 private:
  QTcpServer server_;
};

// 见 #132:服务端完成握手 ≠ 客户端的泵已连上、已建好读流。
bool WaitLinkUp(const TcpTransport& t, int budget_ms = 4000) {
  return pumpFiberUntil([&t] { return t.CurrentLinkState() == LinkState::kUp; },
                        budget_ms);
}

Message Command(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message msg;
  msg.message_id = message_id;
  msg.payload = std::move(payload);
  return msg;
}

// 标准 echo 应答:kResponse,**session_id 与 message_id 原样**,payload 回显。
//
// 【与旧 e2e 用例的一处实质差异】旧对端回的是 `message_id | 0x1000`,因为旧节点对"响应
// 命令码"做过归一化。重设计后配对键就是 `(session_id, message_id, frm_type)` 三字段的
// 直接比对(`ProtocolNode.cpp` 的 `ResponseTo`),**不再有任何命令码映射规则**——响应帧
// 须与请求帧同码。故此处照新规则回帧,不是放松断言。
Message EchoResponse(const Message& command) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.protocol_id = command.protocol_id;
  resp.session_id = command.session_id;
  resp.message_id = command.message_id;
  resp.payload = command.payload;
  return resp;
}

/// 裸对端会话:一条 fiber 挂在 `readAll()` 流上,解出的每条 kCommand 交给 responder,
/// 回帧经同一个 socket 写回。**无轮询**——流被关(我方 `Stop()`)或 socket 断开时退出。
class PeerSession {
 public:
  using Responder = std::function<std::vector<Message>(const Message&)>;

  PeerSession(QTcpSocket* socket, Responder responder)
      : socket_(socket),
        responder_(std::move(responder)),
        stream_(Coro::coro(socket).readAll()) {
    task_ = std::make_shared<Coro::FiberTask<void>>(
        Coro::makeTask([this] { Run(); }));
  }

  ~PeerSession() { Stop(); }

  PeerSession(const PeerSession&) = delete;
  PeerSession& operator=(const PeerSession&) = delete;

  /// 关流 + join 自己的 fiber(幂等)。
  void Stop() {
    if (!task_) {
      return;
    }
    stream_->close(make_error_code(TransportErrc::kClosed));
    (void)task_->get();
    task_.reset();
  }

  void Send(const Message& msg) {
    auto encoded = codec_.Encode(msg);
    ASSERT_TRUE(encoded);
    const auto& bytes = encoded.value();
    socket_->write(reinterpret_cast<const char*>(bytes.data()),
                   static_cast<qint64>(bytes.size()));
    socket_->flush();
  }

  /// 收到的全部帧(含命令与节点回的业务帧)——供事件驱动的等待条件使用。
  [[nodiscard]] const std::vector<Message>& received() const { return received_; }
  [[nodiscard]] std::size_t command_count() const { return command_count_; }

 private:
  void Run() {
    for (;;) {
      auto chunk = Coro::await(stream_);  // 无限期等:流被关或 socket 断开才返回。
      if (!chunk) {
        return;
      }
      const QByteArray& bytes = chunk.value();
      auto decoded = codec_.Decode(
          reinterpret_cast<const std::uint8_t*>(bytes.constData()),
          static_cast<std::size_t>(bytes.size()));
      if (!decoded) {
        continue;  // 坏帧:codec 自行重同步,对端不做别的。
      }
      for (const Message& msg : decoded.value()) {
        received_.push_back(msg);
        if (msg.frm_type != FrameType::kCommand) {
          continue;
        }
        ++command_count_;
        for (const Message& reply : responder_(msg)) {
          Send(reply);
        }
      }
    }
  }

  QTcpSocket* socket_;
  Responder responder_;
  SystemCodec codec_;
  std::shared_ptr<Coro::Awaitable<QByteArray>> stream_;
  std::shared_ptr<Coro::FiberTask<void>> task_;
  std::vector<Message> received_;
  std::size_t command_count_ = 0;
};

}  // namespace

// —— ① 断链对交互层透明 ————————————————————————————————————————————————
//
// AC(DD-11 / ADR-0007 D2 / ADR-0011 D2):真实 TCP 上,
//   · 连上 → 一次交互恰好一次完成(裸 echo 对端);
//   · 对端物理断连 → **在途交互不被断链终结**、**节点不被断链关闭**(读队列不随断链
//     终止,故节点的读循环不退出);在途交互只由**它自己的重发次数耗尽**收敛;
//   · 泵自动重连 → 新连接上的交互照常成功,调用方全程没有任何切换动作。
TEST(ProtocolNodeTcpE2E, InFlightInteractionSurvivesDisconnectAndNodeKeepsRunning) {
  LoopbackServer server;
  TcpTransport transport(ConfigFor(server.port(), 3000ms));
  ASSERT_TRUE(transport.Start());
  ProtocolNode node(transport, std::make_unique<SystemCodec>(), NodeConfig());
  ASSERT_TRUE(node.Start());

  // —— 代际 1:连上,对端先应答后静默 ——
  QTcpSocket* first = server.Accept();
  ASSERT_NE(first, nullptr);
  ASSERT_TRUE(WaitLinkUp(transport));
  bool silent = false;
  auto responder = [&silent](const Message& c) -> std::vector<Message> {
    return silent ? std::vector<Message>{} : std::vector<Message>{EchoResponse(c)};
  };
  auto echo1 = std::make_unique<PeerSession>(first, responder);

  auto first_reply = node.RequestForResponse(Command(0x0001, {0x11}), {2000ms, 1});
  ASSERT_TRUE(first_reply) << first_reply.error().message();
  EXPECT_EQ(first_reply.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(first_reply.value().message_id, 0x0001);
  EXPECT_EQ(first_reply.value().payload, (std::vector<std::uint8_t>{0x11}));

  // —— 在途交互 + 物理断连 ——
  silent = true;
  Coro::Result<Message> pending = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    pending = node.RequestForResponse(Command(0x0002, {0x22}), {1200ms, 1});
    done = true;
  });
  // 事件驱动:等对端确实收到了第二条命令,才谈得上"在途"。
  ASSERT_TRUE(
      pumpFiberUntil([&] { return echo1->command_count() >= 2; }, 3000));

  first->abort();  // 对端强制断连(RST)。

  EXPECT_FALSE(pumpFiberUntil([&] { return done; }, 250))
      << "断链终结了在途交互(应只由其自身的重发次数耗尽收敛)";
  EXPECT_TRUE(node.IsRunning())
      << "断链关闭了节点(重连对交互层应完全透明,DD-11)";

  // 它由自己的受理阶段次数耗尽收敛——**kNotAccepted**,不是 kTimeout(ADR-0010 D12)。
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 4000));
  (void)caller.get();
  ASSERT_FALSE(pending);
  EXPECT_EQ(pending.error(), make_error_code(TransportErrc::kNotAccepted));
  EXPECT_TRUE(node.IsRunning());

  // —— 代际 2:泵自动重连,新交互在新连接上成功 ——
  QTcpSocket* second = server.Accept(5000);
  ASSERT_NE(second, nullptr) << "泵未自动重连";
  ASSERT_TRUE(WaitLinkUp(transport));
  silent = false;
  auto echo2 = std::make_unique<PeerSession>(second, responder);

  auto after = node.RequestForResponse(Command(0x0004, {0xAB}), {3000ms, 3});
  ASSERT_TRUE(after) << after.error().message();
  EXPECT_EQ(after.value().message_id, 0x0004);
  EXPECT_EQ(after.value().payload, (std::vector<std::uint8_t>{0xAB}));

  echo1->Stop();
  echo2->Stop();
  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(transport.Close());  // 传输由**宿主**关(节点不管它的生命周期)。
  transport.WaitClosed();
}

// —— ② 入站方向端到端 ——————————————————————————————————————————————————
//
// AC(RT_INBOUND_001 / ADR-0009 D1):对端主动推来的业务帧穿过真 socket → 泵 → read_queue
// → 节点读循环 → **订阅信箱**;宿主在自己的 fiber 里取到它,回的一帧经 `Send()` 原路出去
// 被对端收到。入站只有订阅这一条通路——内建 handler 通道已废止。
TEST(ProtocolNodeTcpE2E, InboundBusinessFrameReachesSubscriberAndReplyGoesOut) {
  LoopbackServer server;
  TcpTransport transport(ConfigFor(server.port(), 3000ms));
  ASSERT_TRUE(transport.Start());
  ProtocolNode node(transport, std::make_unique<SystemCodec>(), NodeConfig());
  ASSERT_TRUE(node.Start());

  QTcpSocket* accepted = server.Accept();
  ASSERT_NE(accepted, nullptr);
  ASSERT_TRUE(WaitLinkUp(transport));
  auto sink = [](const Message&) { return std::vector<Message>{}; };
  auto peer = std::make_unique<PeerSession>(accepted, sink);

  // **登记须先于报文到达**(ADR-0009 D1):先订阅,再让对端推帧。
  auto ticket = node.Subscribe(AnyOfType(FrameType::kState));
  auto mailbox = ticket.mailbox();

  Message business;
  business.frm_type = FrameType::kState;
  business.protocol_id = kProtocolId;
  business.session_id = 42;
  business.message_id = 0x0007;
  business.payload = {0xC0, 0xDE};
  peer->Send(business);

  auto inbound = Coro::await_for(mailbox, 3000ms);
  ASSERT_TRUE(inbound) << "业务帧未经真实 TCP 抵达订阅信箱";
  EXPECT_EQ(inbound.value().frm_type, FrameType::kState);
  EXPECT_EQ(inbound.value().session_id, 42);
  EXPECT_EQ(inbound.value().message_id, 0x0007);
  EXPECT_EQ(inbound.value().payload, (std::vector<std::uint8_t>{0xC0, 0xDE}));

  // 宿主回一帧(noresponse 出站):原路出去,对端收到。
  Message reply;
  reply.frm_type = FrameType::kState;
  reply.message_id = 0x00BB;
  reply.payload = inbound.value().payload;
  ASSERT_TRUE(node.Send(std::move(reply)));

  ASSERT_TRUE(pumpFiberUntil([&] { return !peer->received().empty(); }, 3000))
      << "宿主的回帧未抵达对端";
  ASSERT_EQ(peer->received().size(), 1u);
  EXPECT_EQ(peer->received().front().frm_type, FrameType::kState);
  EXPECT_EQ(peer->received().front().message_id, 0x00BB);
  EXPECT_EQ(peer->received().front().payload,
            (std::vector<std::uint8_t>{0xC0, 0xDE}));

  peer->Stop();
  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(transport.Close());
  transport.WaitClosed();
}
