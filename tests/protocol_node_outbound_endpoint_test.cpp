// -----------------------------------------------------------------------------
// protocol_node_outbound_endpoint_test.cpp — `Message::endpoint` 的【发送方向】语义
//                                             (ADR-0021,#250)
//
// `endpoint` 与 `Datagram::peer` 逐字同义:**发送时是目的地、接收时是来源**。接收半边由
// `protocol_node_endpoint_test.cpp` 守着(ADR-0020 D3);**发送半边**此前是断的——
// `ProtocolNode::EncodeAndWrite` 把目的地写死成 `Endpoint::Default()`,字段有值也被丢弃,
// 于是 `UdpTransport` 写泵早就具备的"按报文指定目的地"(ADR-0003 D12)对 node 层不可达。
// **`ProtocolNode` 是 UDP 一对多的唯一堵点**,ADR-0021 D1 把它打通。
//
// 本文件一律用**真实 UDP / TCP 回环**,不用假传输:"这一帧到底发去了哪个 ip:port"这件事
// 只有真 socket 给得出来。
//
// 四条用例各自证明的事实:
//   ① `OneNodeFansOutToTwoDistinctPeers` —— **本票的核心**:一个节点、一条 socket,
//      按 `msg.endpoint` 把两帧发往两个不同的 `ip:port`,各收各的。
//   ② `UnsetEndpointStillGoesToTheConfiguredDefaultPeer` —— D2 向后兼容:不填即
//      `Endpoint::Default()`,仍发往 `UdpConfig` 的默认对端,与本 ADR 之前逐字相同。
//   ③ `ServerRepliesToTheRequesterOverUdpAndTerminatesTheRequest` —— **D4**:服务端只用
//      公开面(`rsp.session_id = req.session_id`(ADR-0019)+ `rsp.endpoint = req.endpoint`
//      (本 ADR)+ `Send`)就能在 UDP 上把应答送回**真正的请求方**,客户端的
//      `RequestForResponse` 由它终结。**两个 ADR 合起来才闭环。**
//   ④ `TcpIgnoresTheOutboundEndpoint` —— 代价 3:TCP 写泵一律忽略 `peer`
//      (ADR-0011 D8),填了 `endpoint` 也照旧发往固定对端,本 ADR 对它是空操作。
//
// **反向证据一并写死**:①②③ 都让 `UdpConfig` 配一个**诱饵默认对端**(decoy),并断言它
// **一帧都收不到**。这是把改动还原成 `Endpoint::Default()` 时会变红的那一条——否则用例
// 只要"收到了就算过",诱饵吃掉全部流量也照样绿。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
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
#include "message_test_util.hpp"
#include "task/fibertask.h"
#include "transport/codec/SystemCodec.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/tcp/TcpConfig.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/io/udp/UdpConfig.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::Pay;
using testutil::ToVec;
using testutil::pumpFiberUntil;
using transport::AnyOfType;
using transport::Endpoint;
using transport::FrameType;
using transport::LinkState;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemCodec;
using transport::SystemDatagramCodec;
using transport::TcpConfig;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";
constexpr std::uint8_t kProtocolId = 0x2A;

/// 没有默认对端的 UDP 配置(纯收方用)。
UdpConfig LoopbackConfig() {
  UdpConfig config;
  config.mode = transport::UdpMode::kUnicast;
  config.local_addr = kLoopback;
  config.local_port = 0;  // OS 分配临时端口。
  return config;
}

/// 配了默认对端的 UDP 配置——`kDefault` 的解析目标(ADR-0003 D12)。
UdpConfig ConfigWithDefaultPeer(std::uint16_t remote_port) {
  UdpConfig config = LoopbackConfig();
  config.remote_addr = kLoopback;
  config.remote_port = remote_port;
  return config;
}

ProtocolNodeConfig NodeConfig() {
  ProtocolNodeConfig config;
  config.protocol_id = kProtocolId;
  return config;
}

/// 从一条 UDP 报文里解出恰一帧(报文版 codec:一个报文恰一帧,残留丢弃)。
/// 读不到报文、或解不出恰一帧时返回空 —— 调用方一律先 `ASSERT_TRUE(got.has_value())`。
struct ReceivedFrame {
  Message msg;
  Endpoint from;
};

bool ReadOneFrame(UdpTransport& transport, ReceivedFrame& out,
                  std::chrono::milliseconds budget = 3000ms) {
  auto datagram = testutil::ReadOnce(transport, budget);
  if (!datagram) {
    return false;
  }
  SystemDatagramCodec codec;
  auto decoded = codec.Decode(datagram.value().bytes.data(),
                              datagram.value().bytes.size());
  if (!decoded || decoded.value().size() != 1u) {
    return false;
  }
  out.msg = decoded.value().front();
  out.from = datagram.value().peer;
  return true;
}

/// 在给定预算内**一条报文都没收到**。预算刻意短:它只用来证否,不用来等。
bool NothingArrives(UdpTransport& transport,
                    std::chrono::milliseconds budget = 300ms) {
  return !testutil::ReadOnce(transport, budget);
}

/// 一条待发的业务帧(kState:不期待应答,`Send` 原样保留该帧类型)。
Message StateTo(Endpoint to, std::uint16_t message_id,
                std::vector<std::uint8_t> payload) {
  Message msg;
  msg.frm_type = FrameType::kState;
  msg.message_id = message_id;
  msg.payload = Pay(payload);
  msg.endpoint = std::move(to);
  return msg;
}

}  // namespace

// —— ① UDP 一对多:本票的核心 ————————————————————————————————————————————
//
// AC(ADR-0021 **D1** / RT_TRANSPORT_006):**一个** `ProtocolNode` + **一条** `UdpTransport`,
// 按 `msg.endpoint` 向两个不同的 `ip:port` 各发一帧,两个独立的接收 socket **各自只收到
// 发给自己的那一帧**。
//
// 本 ADR 之前这做不到:两帧都会被写死的 `Endpoint::Default()` 送去 `UdpConfig` 配的默认
// 对端。故此处刻意把默认对端配成**诱饵**,并断言它一帧都收不到——这正是把改动还原回
// `Endpoint::Default()` 时三处断言一起变红的原因(诱饵收到两帧、A 与 B 各收到零帧)。
TEST(ProtocolNodeOutboundEndpoint, OneNodeFansOutToTwoDistinctPeers) {
  UdpTransport peer_a(LoopbackConfig());
  UdpTransport peer_b(LoopbackConfig());
  UdpTransport decoy(LoopbackConfig());  // 配置的默认对端——本用例里它应当颗粒无收。
  ASSERT_TRUE(peer_a.Start());
  ASSERT_TRUE(peer_b.Start());
  ASSERT_TRUE(decoy.Start());

  UdpTransport sender(ConfigWithDefaultPeer(decoy.LocalPort()));
  ASSERT_TRUE(sender.Start());
  ProtocolNode node(sender, std::make_unique<SystemDatagramCodec>(), NodeConfig());
  ASSERT_TRUE(node.Start());

  const Endpoint to_a = Endpoint::Net(kLoopback, peer_a.LocalPort());
  const Endpoint to_b = Endpoint::Net(kLoopback, peer_b.LocalPort());
  ASSERT_NE(peer_a.LocalPort(), peer_b.LocalPort());

  ASSERT_TRUE(node.Send(StateTo(to_a, 0x00A0, {0xA1, 0xA2})));
  ASSERT_TRUE(node.Send(StateTo(to_b, 0x00B0, {0xB1})));

  // A 只收到发给 A 的那一帧。
  ReceivedFrame at_a;
  ASSERT_TRUE(ReadOneFrame(peer_a, at_a)) << "发给 A 的那一帧没到 A";
  EXPECT_EQ(at_a.msg.message_id, 0x00A0);
  EXPECT_EQ(ToVec(at_a.msg.payload), (std::vector<std::uint8_t>{0xA1, 0xA2}));
  EXPECT_EQ(at_a.from.port, sender.LocalPort()) << "两帧出自同一条 socket";

  // B 只收到发给 B 的那一帧。
  ReceivedFrame at_b;
  ASSERT_TRUE(ReadOneFrame(peer_b, at_b)) << "发给 B 的那一帧没到 B";
  EXPECT_EQ(at_b.msg.message_id, 0x00B0);
  EXPECT_EQ(ToVec(at_b.msg.payload), (std::vector<std::uint8_t>{0xB1}));
  EXPECT_EQ(at_b.from.port, sender.LocalPort());

  // **各自只收到自己那一帧**:没有把对方的也收了(广播/串台),也没有重复。
  EXPECT_TRUE(NothingArrives(peer_a)) << "A 收到了不该它收的报文";
  EXPECT_TRUE(NothingArrives(peer_b)) << "B 收到了不该它收的报文";
  // 诱饵(配置的默认对端)一帧都不该有——`endpoint` 若被丢弃,两帧都会落在这里。
  EXPECT_TRUE(NothingArrives(decoy))
      << "出站目的地仍被写死成 Endpoint::Default():两帧都去了配置的默认对端";

  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(sender.Close());
  sender.WaitClosed();
  ASSERT_TRUE(peer_a.Close());
  peer_a.WaitClosed();
  ASSERT_TRUE(peer_b.Close());
  peer_b.WaitClosed();
  ASSERT_TRUE(decoy.Close());
  decoy.WaitClosed();
}

// —— ② 向后兼容 ————————————————————————————————————————————————————————
//
// AC(ADR-0021 **D2**):`Message::endpoint` 的默认值就是 `Endpoint::Default()`,故**不填的
// 调用方行为与本 ADR 之前逐字相同**——仍发往 `UdpConfig` 配的默认对端。不设开关、无迁移期。
//
// 反向那半边也验掉:另有一个对端在监听,不填 `endpoint` 时它什么也收不到(即"默认"不是
// 广播、也不是"最近一次用过的地址")。
TEST(ProtocolNodeOutboundEndpoint, UnsetEndpointStillGoesToTheConfiguredDefaultPeer) {
  UdpTransport default_peer(LoopbackConfig());
  UdpTransport other(LoopbackConfig());
  ASSERT_TRUE(default_peer.Start());
  ASSERT_TRUE(other.Start());

  UdpTransport sender(ConfigWithDefaultPeer(default_peer.LocalPort()));
  ASSERT_TRUE(sender.Start());
  ProtocolNode node(sender, std::make_unique<SystemDatagramCodec>(), NodeConfig());
  ASSERT_TRUE(node.Start());

  Message msg;
  msg.frm_type = FrameType::kState;
  msg.message_id = 0x00C0;
  msg.payload = Pay({0xC0, 0xDE});
  ASSERT_EQ(msg.endpoint.kind, Endpoint::Kind::kDefault) << "默认值即 kDefault,前提";
  ASSERT_TRUE(node.Send(std::move(msg)));

  ReceivedFrame at_default;
  ASSERT_TRUE(ReadOneFrame(default_peer, at_default))
      << "不填 endpoint 时未发往 UdpConfig 的默认对端(向后兼容被打破)";
  EXPECT_EQ(at_default.msg.message_id, 0x00C0);
  EXPECT_EQ(ToVec(at_default.msg.payload), (std::vector<std::uint8_t>{0xC0, 0xDE}));

  EXPECT_TRUE(NothingArrives(other)) << "kDefault 不是广播";

  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(sender.Close());
  sender.WaitClosed();
  ASSERT_TRUE(default_peer.Close());
  default_peer.WaitClosed();
  ASSERT_TRUE(other.Close());
  other.WaitClosed();
}

// —— ③ 服务端在 UDP 上闭环 ————————————————————————————————————————————
//
// AC(ADR-0021 **D4** + ADR-0019 **D1**):两个 `ProtocolNode` 对接,服务端收到 `kCommand`
// 后**只用公开面**回应答:
//
//     rsp.session_id = req.session_id;   // ADR-0019:Send 透传,不再被覆盖
//     rsp.endpoint   = req.endpoint;     // ★ ADR-0021:回到真正的请求方
//     (void)node.Send(std::move(rsp));
//
// 断言客户端的 `RequestForResponse` **被它终结**。
//
// **两个 ADR 缺一不可**:少了 ADR-0019,`Send` 会盖掉 `session_id`,应答帧关联不上;少了
// 本 ADR,那帧只会发往服务端 `UdpConfig` 配的默认对端——故此处同样配了**诱饵**默认对端,
// 并在末尾断言它颗粒无收。这是本票最有价值的一条。
TEST(ProtocolNodeOutboundEndpoint,
     ServerRepliesToTheRequesterOverUdpAndTerminatesTheRequest) {
  // 诱饵:服务端 `UdpConfig` 的默认对端。应答若丢了 `endpoint` 就会落到这里。
  UdpTransport decoy(LoopbackConfig());
  ASSERT_TRUE(decoy.Start());

  // —— 服务端 ——(先起,客户端才知道该往哪儿发)
  UdpTransport server_io(ConfigWithDefaultPeer(decoy.LocalPort()));
  ASSERT_TRUE(server_io.Start());
  ProtocolNode server(server_io, std::make_unique<SystemDatagramCodec>(),
                      NodeConfig());
  ASSERT_TRUE(server.Start());
  ASSERT_NE(server_io.LocalPort(), 0);

  // —— 客户端 ——:默认对端就是服务端,故请求本身不必填 `endpoint`(一对一照旧)。
  UdpTransport client_io(ConfigWithDefaultPeer(server_io.LocalPort()));
  ASSERT_TRUE(client_io.Start());
  ProtocolNode client(client_io, std::make_unique<SystemDatagramCodec>(),
                      NodeConfig());
  ASSERT_TRUE(client.Start());

  // 服务端的入站通路只有订阅一条(ADR-0009 D1);**登记须先于报文到达**。
  auto sub = server.Subscribe(AnyOfType(FrameType::kCommand));
  ASSERT_TRUE(static_cast<bool>(sub)) << sub.error().message();
  auto ticket = std::move(sub).value();
  auto mailbox = ticket.mailbox();

  // 服务端 fiber:**整段只用公开面**——这正是「写一个外部协议服务端」的全部代码。
  bool replied = false;
  Coro::Result<void> reply_sent = make_error_code(TransportErrc::kInternal);
  Endpoint requester_seen;
  auto responder = Coro::makeTask([&] {
    auto req = Coro::await_for(mailbox, 5000ms);
    if (!req) {
      return;
    }
    requester_seen = req.value().endpoint;  // 入站即来源(ADR-0020 D3)。

    Message rsp;
    rsp.frm_type = FrameType::kResponse;
    rsp.session_id = req.value().session_id;  // ADR-0019:透传,Send 不再盖。
    rsp.message_id = req.value().message_id;
    rsp.endpoint = req.value().endpoint;      // ★ ADR-0021:回到请求方。
    rsp.payload = req.value().OwnedPayload();  // 回显(要活过 req,故深拷贝,D8)。
    reply_sent = server.Send(std::move(rsp));
    replied = true;
  });

  Message cmd;
  cmd.message_id = 0x0042;
  cmd.payload = Pay({0x11, 0x22});
  auto answer = client.RequestForResponse(std::move(cmd), {3000ms, 2});

  ASSERT_TRUE(pumpFiberUntil([&] { return replied; }, 5000));
  (void)responder.get();
  ASSERT_TRUE(reply_sent) << reply_sent.error().message();

  ASSERT_TRUE(answer) << "应答没回到请求方,客户端的 RequestForResponse 未被终结: "
                      << answer.error().message();
  EXPECT_EQ(answer.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(answer.value().message_id, 0x0042);
  EXPECT_EQ(ToVec(answer.value().payload), (std::vector<std::uint8_t>{0x11, 0x22}));
  // 应答是从服务端那条 socket 来的——客户端这一侧也看得见来源(ADR-0020 D3)。
  EXPECT_EQ(answer.value().endpoint.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(answer.value().endpoint.port, server_io.LocalPort());
  // 服务端看到的请求方就是客户端那条 socket——`rsp.endpoint = req.endpoint` 的落点。
  EXPECT_EQ(requester_seen.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(requester_seen.port, client_io.LocalPort());

  EXPECT_TRUE(NothingArrives(decoy))
      << "应答去了服务端配置的默认对端而非请求方:出站目的地仍被丢弃";

  ASSERT_TRUE(client.Close());
  client.WaitClosed();
  ASSERT_TRUE(client_io.Close());
  client_io.WaitClosed();
  ASSERT_TRUE(server.Close());
  server.WaitClosed();
  ASSERT_TRUE(server_io.Close());
  server_io.WaitClosed();
  ASSERT_TRUE(decoy.Close());
  decoy.WaitClosed();
}

// —— ④ TCP 不受影响 ————————————————————————————————————————————————————
//
// AC(ADR-0021 代价 3 / ADR-0011 **D8**):TCP 写泵**一律忽略** `Datagram::peer`——它只有
// 一条连接。故填了 `msg.endpoint`(这里刻意填一个**别处**的 `ip:port`)也照旧发往配置的
// 固定对端,本 ADR 对 TCP 是**空操作**。串口同理(ADR-0012 D9),写泵结构逐字同构。
TEST(ProtocolNodeOutboundEndpoint, TcpIgnoresTheOutboundEndpoint) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const auto server_port = static_cast<std::uint16_t>(server.serverPort());

  // 另起一个 UDP socket 只为借一个**确实存在但无关**的端口号填进 endpoint:
  // 若写泵不忽略 peer,这一帧就不会出现在下面那条 TCP 连接上。
  UdpTransport elsewhere(LoopbackConfig());
  ASSERT_TRUE(elsewhere.Start());
  ASSERT_NE(elsewhere.LocalPort(), server_port);

  TcpConfig config;
  config.host = kLoopback;
  config.port = server_port;
  config.silence_timeout = 3000ms;
  TcpTransport transport(config);
  ASSERT_TRUE(transport.Start());
  ProtocolNode node(transport, std::make_unique<SystemCodec>(), NodeConfig());
  ASSERT_TRUE(node.Start());

  ASSERT_TRUE(pumpFiberUntil([&] { return server.hasPendingConnections(); }, 4000));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return transport.CurrentLinkState() == LinkState::kUp; }, 4000));
  auto stream = Coro::coro(accepted).readAll();

  ASSERT_TRUE(node.Send(StateTo(Endpoint::Net(kLoopback, elsewhere.LocalPort()),
                                0x00D0, {0xD1, 0xD2})));

  auto chunk = Coro::await_for(stream, 3000ms);
  ASSERT_TRUE(chunk) << "填了 endpoint 之后 TCP 帧没到固定对端(写泵不该解释 peer)";
  SystemCodec codec;
  auto decoded = codec.Decode(
      reinterpret_cast<const std::uint8_t*>(chunk.value().constData()),
      static_cast<std::size_t>(chunk.value().size()));
  ASSERT_TRUE(static_cast<bool>(decoded));
  ASSERT_EQ(decoded.value().size(), 1u);
  EXPECT_EQ(decoded.value().front().message_id, 0x00D0);
  EXPECT_EQ(ToVec(decoded.value().front().payload),
            (std::vector<std::uint8_t>{0xD1, 0xD2}));

  // 那个"别处"的地址一条报文也没收到——endpoint 没有把帧引去别处。
  EXPECT_TRUE(NothingArrives(elsewhere));

  stream->close(make_error_code(TransportErrc::kClosed));
  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(transport.Close());
  transport.WaitClosed();
  ASSERT_TRUE(elsewhere.Close());
  elsewhere.WaitClosed();
}
