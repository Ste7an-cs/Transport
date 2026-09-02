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

  // **登记须先于报文到达**(ADR-0009 D1):先订阅,再让对端推帧。节点已 `Start()`,
  // 订阅这才受理(D1′:`Subscribe` 只在 `Running` 放行,返 `Coro::Result<Ticket>`)。
  auto sub = node.Subscribe(AnyOfType(FrameType::kState));
  ASSERT_TRUE(static_cast<bool>(sub)) << sub.error().message();
  auto ticket = std::move(sub).value();
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

// =============================================================================
// #187 追加:两个多段交互(RequestForResult / RequestForResultDirect)与跨 TCP 切片
//         的分帧,全部跑在真实链路上。
//
// `protocol_node_test.cpp` 已在 `FakeTransport` 上验过同样的语义,但那里的传输**交付的是
// 完整帧**;`TcpTransport` 交付的是**任意字节切片**(RT_TRANSPORT_003)。故 codec 的跨切片
// 拼帧与交互状态机的相互作用只可能在真链路上发生——⑨ 两条是本组的核心,fake 传输在结构
// 上给不了。其余各条则是把 ADR-0010 里三处"易做错点"(D4 的登记时序、D5 的注销时机、
// D2 与 D13 两条相反的重发规则)的证据从 fake 层搬到真实链路上。
// =============================================================================

namespace {

/// 交互模式用例的共用装配:真实 TCP 链路 + 已连上的裸对端 + 已启动的节点。
///
/// 拆解顺序与既有两条用例一致:先停对端 fiber,再关节点,最后由**宿主**关传输
/// (节点不管传输的生命周期)。
class E2ELink {
 public:
  E2ELink() = default;
  ~E2ELink() { Teardown(); }

  E2ELink(const E2ELink&) = delete;
  E2ELink& operator=(const E2ELink&) = delete;

  [[nodiscard]] bool Setup(PeerSession::Responder responder) {
    transport_ =
        std::make_unique<TcpTransport>(ConfigFor(server_.port(), 3000ms));
    if (!transport_->Start()) {
      return false;
    }
    node_ = std::make_unique<ProtocolNode>(
        *transport_, std::make_unique<SystemCodec>(), NodeConfig());
    if (!node_->Start()) {
      return false;
    }
    socket_ = server_.Accept();
    if (socket_ == nullptr || !WaitLinkUp(*transport_)) {
      return false;
    }
    peer_ = std::make_unique<PeerSession>(socket_, std::move(responder));
    return true;
  }

  void Teardown() {
    if (peer_) {
      peer_->Stop();
      peer_.reset();
    }
    if (node_) {
      (void)node_->Close();
      node_->WaitClosed();
      node_.reset();
    }
    if (transport_) {
      (void)transport_->Close();
      transport_->WaitClosed();
      transport_.reset();
    }
  }

  ProtocolNode& node() { return *node_; }
  PeerSession& peer() { return *peer_; }
  QTcpSocket* socket() { return socket_; }

 private:
  LoopbackServer server_;
  std::unique_ptr<TcpTransport> transport_;
  std::unique_ptr<ProtocolNode> node_;
  QTcpSocket* socket_ = nullptr;
  std::unique_ptr<PeerSession> peer_;
};

transport::RetryPolicy Retry(std::chrono::milliseconds timeout, int attempts) {
  transport::RetryPolicy retry;
  retry.timeout = timeout;
  retry.max_attempts = attempts;
  return retry;
}

/// 结果帧:**命令码与请求帧不同**(协议知识,由调用方给出,D7),session_id 沿用。
Message ResultFor(const Message& command, std::uint16_t result_message_id,
                  std::vector<std::uint8_t> payload) {
  Message result;
  result.frm_type = FrameType::kResult;
  result.protocol_id = command.protocol_id;
  result.session_id = command.session_id;
  result.message_id = result_message_id;
  result.payload = std::move(payload);
  return result;
}

/// 绕开 `PeerSession::Send`(它整帧发出)直接往 socket 上灌裸字节——切片用例专用。
void WriteRaw(QTcpSocket* socket, const std::uint8_t* data, std::size_t len) {
  socket->write(reinterpret_cast<const char*>(data), static_cast<qint64>(len));
  socket->flush();
}

}  // namespace

// —— ③ RequestForResult 完整链路:命令 → kResponse → kResult → 我方回一帧派生 kResponse ——
//
// AC(ADR-0010 D8 / RT_NODE_002_f):回应结果是本模型**固有的最后一步**。该帧完全由收到的
// kResult 派生:payload 原样、session_id / message_id 沿用,**仅**帧类型改为 kResponse
// (CRC 由 Encode 重算)。这里断言的是**对端在线缆上实际收到的那一帧**——它穿过了真实的
// 编码、socket 与对端 codec,而非 fake 层的字节直读。
TEST(ProtocolNodeTcpE2E, RequestForResultCompletesAndRepliesWithDerivedResponse) {
  constexpr std::uint16_t kResultId = 0x03F2;
  const std::vector<std::uint8_t> kResultPayload{4, 2, 0};
  E2ELink link;
  ASSERT_TRUE(link.Setup([&](const Message& c) {
    return std::vector<Message>{EchoResponse(c),
                                ResultFor(c, kResultId, kResultPayload)};
  }));

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    outcome = link.node().RequestForResult(Command(0x0010, {0x01}),
                                           Retry(2000ms, 3), kResultId, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000))
      << "RequestForResult 在真实链路上未收敛";
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResult) << "返回的是结果那一帧";
  EXPECT_EQ(outcome.value().message_id, kResultId);
  EXPECT_EQ(outcome.value().payload, kResultPayload);

  // 派生回应帧原路出去,对端收到:命令 + 回应,恰两帧。
  ASSERT_TRUE(
      pumpFiberUntil([&] { return link.peer().received().size() >= 2u; }, 4000))
      << "我方未回发派生的 kResponse(D8 的末步)";
  ASSERT_EQ(link.peer().received().size(), 2u);
  EXPECT_EQ(link.peer().command_count(), 1u) << "首次即受理,不应有重发";
  const Message& command = link.peer().received()[0];
  const Message& derived = link.peer().received()[1];
  EXPECT_EQ(command.frm_type, FrameType::kCommand);
  EXPECT_EQ(derived.frm_type, FrameType::kResponse) << "仅此一处相对 kResult 有改动";
  EXPECT_EQ(derived.session_id, command.session_id) << "session_id 沿用";
  EXPECT_EQ(derived.message_id, kResultId) << "message_id 沿用结果帧的";
  EXPECT_EQ(derived.payload, kResultPayload) << "payload 原样回显";
  EXPECT_EQ(derived.protocol_id, kProtocolId);
}

// —— ④ kResult **先于** kResponse 到达仍能成功 ————————————————————————————
//
// AC(ADR-0010 D4):两个订阅一起在**发命令之前**登记。若改成"收到受理再登记结果订阅",
// 先到的结果帧会因无匹配而被按 kUnmatchedOrLateResponse 丢弃,本用例必失败(等结果阶段
// 又不重发,只能超时)。对端此处**先发结果、后发受理**。
TEST(ProtocolNodeTcpE2E, RequestForResultAcceptsResultArrivingBeforeAckOverTcp) {
  constexpr std::uint16_t kResultId = 0x03F2;
  E2ELink link;
  ASSERT_TRUE(link.Setup([&](const Message& c) {
    return std::vector<Message>{ResultFor(c, kResultId, {0x08}),
                                EchoResponse(c)};
  }));

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    outcome = link.node().RequestForResult(Command(0x0010, {0x01}),
                                           Retry(2000ms, 3), kResultId, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000))
      << "结果先于受理到达时交互未收敛——结果订阅可能登记晚了(D4)";
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0x08}));
  ASSERT_TRUE(
      pumpFiberUntil([&] { return link.peer().received().size() >= 2u; }, 4000));
  ASSERT_EQ(link.peer().received().size(), 2u);
  EXPECT_EQ(link.peer().received()[1].frm_type, FrameType::kResponse);
  EXPECT_EQ(link.peer().received()[1].message_id, kResultId);
}

// —— ⑤ RequestForResultDirect 完整链路:命令 → kResult → 成功,且**我方不回发任何帧** ——
//
// AC(ADR-0010 D13):与 ③ 的对照。本交互没有受理阶段,也没有 D8 的末步回应。
TEST(ProtocolNodeTcpE2E, RequestForResultDirectSucceedsAndSendsNoReplyOverTcp) {
  constexpr std::uint16_t kResultId = 0x03F2;
  const std::vector<std::uint8_t> kResultPayload{4, 2, 0};
  E2ELink link;
  ASSERT_TRUE(link.Setup([&](const Message& c) {
    return std::vector<Message>{ResultFor(c, kResultId, kResultPayload)};
  }));

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    outcome = link.node().RequestForResultDirect(Command(0x0010, {0x01}),
                                                 Retry(2000ms, 3), kResultId);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000))
      << "RequestForResultDirect 在真实链路上未收敛";
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResult);
  EXPECT_EQ(outcome.value().message_id, kResultId);
  EXPECT_EQ(outcome.value().payload, kResultPayload);

  // sleep_for 只用于**构造前提**:给任何(本不该存在的)回帧留出穿过 socket 的时间,
  // 使随后的"只收到一帧"成为一条有分量的否定断言,而不是抢跑。
  boost::this_fiber::sleep_for(150ms);
  ASSERT_EQ(link.peer().received().size(), 1u)
      << "只有那一条命令帧:收到 kResult 后**不回应任何帧**(与 RequestForResult 相反)";
  EXPECT_EQ(link.peer().received()[0].frm_type, FrameType::kCommand);
  EXPECT_EQ(link.peer().command_count(), 1u);
}

// —— ⑥ RequestForResultDirect **在等结果阶段重发** ————————————————————————
//
// AC(ADR-0010 D13 / RT_NODE_002_g):本交互唯一的等待阶段就是等结果,不重发则命令帧一旦
// 丢失即彻底失败。前 N−1 次不回结果,最后一次回 → 成功;对端实际收到 N 帧,且逐帧
// session_id 相同(D3:重发的是原帧,session_id 不变,故订阅横跨全部重发继续有效)。
// **与 ⑦ 并排即两条相反重发规则的行为分界证据。**
TEST(ProtocolNodeTcpE2E,
     RequestForResultDirectRetransmitsWhileAwaitingResultOverTcp) {
  constexpr std::uint16_t kResultId = 0x03F2;
  constexpr int kAttempts = 3;
  E2ELink link;
  int seen = 0;
  ASSERT_TRUE(link.Setup([&](const Message& c) -> std::vector<Message> {
    if (++seen < kAttempts) {
      return {};  // 前 N−1 次静默,逼出重发。
    }
    return {ResultFor(c, kResultId, {0x33})};
  }));

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    outcome = link.node().RequestForResultDirect(
        Command(0x0010, {0x01}), Retry(300ms, kAttempts), kResultId);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000))
      << "等结果阶段未重发,或重发后的结果未被认领";
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0x33}));
  ASSERT_EQ(link.peer().command_count(), static_cast<std::size_t>(kAttempts))
      << "总发送次数应恰为 max_attempts——等结果阶段确实重发了";
  ASSERT_EQ(link.peer().received().size(), static_cast<std::size_t>(kAttempts))
      << "线缆上全是命令帧:收到结果后不回应";
  const Message& first = link.peer().received()[0];
  for (std::size_t i = 0; i < link.peer().received().size(); ++i) {
    const Message& attempt = link.peer().received()[i];
    EXPECT_EQ(attempt.frm_type, FrameType::kCommand) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.session_id, first.session_id)
        << "逐帧同一 session_id,第 " << i << " 帧";
    EXPECT_EQ(attempt.message_id, first.message_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.payload, first.payload) << "第 " << i << " 帧";
  }
}

// —— ⑦ RequestForResult 受理后等结果**不重发**,超时返 kTimeout ————————————
//
// AC(ADR-0010 D2 / D12):kResult 未达意味着对端**正在执行**,重发有使其重复执行的风险,
// 故本阶段一帧都不再发;失败点在第二阶段,故返 kTimeout 而**非** kNotAccepted。
// 与 ⑥ 并排:同为"等结果超时",一个重发到次数耗尽、一个一帧不发。
TEST(ProtocolNodeTcpE2E, RequestForResultDoesNotRetransmitAfterAckOverTcp) {
  E2ELink link;
  ASSERT_TRUE(link.Setup([](const Message& c) {
    return std::vector<Message>{EchoResponse(c)};  // 只受理,**永不出结果**。
  }));

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    // 受理阶段时限远大于结果时限:若真发生了重发,对端会收到第二帧命令。
    outcome = link.node().RequestForResult(Command(0x0010, {0x01}),
                                           Retry(2000ms, 3), 0x03F2, 300ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000));
  (void)caller.get();

  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_NE(outcome.error(), make_error_code(TransportErrc::kNotAccepted))
      << "已受理,失败点在第二阶段(D12)";

  // sleep_for 只用于**构造前提**:再等一个结果时限,若实现会重发,这段窗口足够让第二帧
  // 命令抵达对端;之后的计数断言才是有分量的否定断言。
  boost::this_fiber::sleep_for(400ms);
  EXPECT_EQ(link.peer().command_count(), 1u)
      << "等结果阶段不得重发(D2),与 RequestForResultDirect 恰好相反";
  EXPECT_EQ(link.peer().received().size(), 1u)
      << "失败时也不回应结果——线缆上只有那一条命令帧";
}

// —— ⑧ RequestForResponse 的重发(真实链路)————————————————————————————————
//
// AC(ADR-0010 D3):前 N−1 次不回,最后一次回 → 成功;对端实际收到 N 帧,逐帧
// session_id / message_id / payload 相同——重发的是**同一条原帧**,不是新交互。
TEST(ProtocolNodeTcpE2E, RequestForResponseRetransmitsUntilAcceptedOverTcp) {
  constexpr int kAttempts = 3;
  E2ELink link;
  int seen = 0;
  ASSERT_TRUE(link.Setup([&](const Message& c) -> std::vector<Message> {
    if (++seen < kAttempts) {
      return {};
    }
    return {EchoResponse(c)};
  }));

  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    reply = link.node().RequestForResponse(Command(0x0010, {0xAB}),
                                           Retry(300ms, kAttempts));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 6000))
      << "受理阶段未重发,或重发后的受理帧未被认领";
  (void)caller.get();

  ASSERT_TRUE(reply) << reply.error().message();
  EXPECT_EQ(reply.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(reply.value().payload, (std::vector<std::uint8_t>{0xAB}));
  ASSERT_EQ(link.peer().command_count(), static_cast<std::size_t>(kAttempts))
      << "总发送次数应恰为 max_attempts";
  ASSERT_FALSE(link.peer().received().empty());
  const Message& first = link.peer().received()[0];
  for (std::size_t i = 0; i < link.peer().received().size(); ++i) {
    const Message& attempt = link.peer().received()[i];
    EXPECT_EQ(attempt.frm_type, FrameType::kCommand) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.session_id, first.session_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.message_id, first.message_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.payload, first.payload) << "第 " << i << " 帧";
  }
}

// —— ⑨ ⭐ 跨 TCP 切片的分帧 ————————————————————————————————————————————————
//
// **本票的核心,且是 `FakeTransport` 结构上测不到的那一类**:fake 交付完整帧,`TcpTransport`
// 交付任意字节切片(RT_TRANSPORT_003)。对端把一个 kResponse 帧拆成两段、中间加延迟发出,
// 节点侧的 `SystemCodec::Decode` 必须把前半段留在 `buffer_` 里、与后半段拼接后才解出该帧,
// 而在途交互必须**横跨这两次交付继续有效**——两者的相互作用只在真链路上发生。
//
// 帧布局(SystemCodec):[0..3] 4 字节标志 | [4] frm_type | [5] protocol_id | [6] session_id |
// [7..10] 保留 | [11..12] CRC | [13..14] 长度 | [15..] body。下面两条用例各取一个切分点。
namespace {

void RunSplitResponseCase(std::size_t split_at) {
  E2ELink link;
  Message captured;
  bool got_command = false;
  // 对端不在自己的 fiber 里回帧——回帧由用例手工切成两段发出。
  ASSERT_TRUE(link.Setup([&](const Message& c) -> std::vector<Message> {
    captured = c;
    got_command = true;
    return {};
  }));

  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto caller = Coro::makeTask([&] {
    // 单次尝试、长时限:切分期间不得发生重发,否则计数断言失去意义。
    reply = link.node().RequestForResponse(Command(0x0021, {0x5A, 0x5B}),
                                           Retry(4000ms, 1));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return got_command; }, 4000))
      << "命令帧未抵达对端";

  SystemCodec peer_codec;
  auto encoded = peer_codec.Encode(EchoResponse(captured));
  ASSERT_TRUE(encoded);
  const std::vector<std::uint8_t>& bytes = encoded.value();
  ASSERT_GT(split_at, 0u);
  ASSERT_LT(split_at, bytes.size());

  WriteRaw(link.socket(), bytes.data(), split_at);
  // sleep_for 只用于**构造前提**:确保前半段作为**独立一次交付**抵达节点(否则两次 write
  // 可能被合成一个 TCP 段,切片就没发生过)。随后的 EXPECT_FALSE 是确定性的——完整帧还
  // 没发全,交互不可能终结。
  boost::this_fiber::sleep_for(150ms);
  EXPECT_FALSE(done) << "半个帧终结了交互";
  EXPECT_EQ(link.peer().command_count(), 1u) << "时限未到,不应有重发";

  WriteRaw(link.socket(), bytes.data() + split_at, bytes.size() - split_at);
  EXPECT_TRUE(pumpFiberUntil([&] { return done; }, 4000))
      << "跨切片的帧未被拼回:codec 的残留缓冲与交互状态机没配合上";
  (void)caller.get();

  ASSERT_TRUE(reply) << reply.error().message();
  EXPECT_EQ(reply.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(reply.value().session_id, captured.session_id);
  EXPECT_EQ(reply.value().message_id, 0x0021);
  EXPECT_EQ(reply.value().payload, (std::vector<std::uint8_t>{0x5A, 0x5B}));
}

}  // namespace

// ⑨-a 切分点落在**4 字节帧头标志中间**(2/4):节点先收到 0xAA 0xBB 两字节——连一次标志
//     匹配都做不完,`ScanSystemFrames` 一字节都不消费,全部留待下批拼接。
TEST(ProtocolNodeTcpE2E, ResponseSplitInsideHeadFlagIsReassembled) {
  RunSplitResponseCase(2);
}

// ⑨-b 切分点落在**长度字段中间**([13] 与 [14] 之间):节点先收到 14 字节——标志匹配上了,
//     但不足一个 15 字节的帧头,长度字段只到手一半,同样一字节不消费。这条比 ⑨-a 更靠后,
//     覆盖"已识别帧起点、但头部未收全"这一支。
TEST(ProtocolNodeTcpE2E, ResponseSplitInsideLengthFieldIsReassembled) {
  RunSplitResponseCase(14);
}
