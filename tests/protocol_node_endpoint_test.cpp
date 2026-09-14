// -----------------------------------------------------------------------------
// protocol_node_endpoint_test.cpp — `Message::endpoint` 的【接收方向】语义(ADR-0020 D3)
//
// `endpoint` 与 `Datagram::peer` 逐字同义:**发送时是目的地、接收时是来源**。
// `ProtocolNode` 此前**根本不填**这一项——UDP 收到的报文,业务层看不到发送方是谁;
// ADR-0020 **D3** 之后 `DecodeAndDispatch` 填 `msg.endpoint = datagram.peer`,这是白拿的
// 改进,故单列一个用例守住它。
//
// 本文件用**真实 UDP 回环**,不是假传输:"发送方地址"这件事只有真 socket 给得出来
// ——假传输的 `Deliver()` 恒填 `Endpoint::Default()`,拿它验不出 `ip:port` 有没有传到位。
//
// 顺带把入站 `frame` / payload 视图在**整条链路**(socket → 泵 → 读队列 → 读循环 →
// 订阅信箱)末端仍然成立这一条也验掉:信箱里那一条是 `Dispatcher` 发的**副本**。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "message_test_util.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/udp/UdpConfig.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::Pay;
using testutil::PayloadIsViewOfFrame;
using testutil::ToVec;
using transport::AnyOfType;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemDatagramCodec;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";
constexpr std::uint8_t kProtocolId = 0x2A;

UdpConfig LoopbackConfig() {
  UdpConfig config;
  config.mode = transport::UdpMode::kUnicast;
  config.local_addr = kLoopback;
  config.local_port = 0;  // OS 分配临时端口。
  return config;
}

ProtocolNodeConfig NodeConfig() {
  ProtocolNodeConfig config;
  config.protocol_id = kProtocolId;
  return config;
}

/// 造一帧 kState 业务帧的线缆字节(报文版 codec:一个报文恰一帧)。
std::vector<std::uint8_t> StateFrame(std::uint8_t session_id,
                                     std::uint16_t message_id,
                                     std::vector<std::uint8_t> payload) {
  Message msg;
  msg.frm_type = FrameType::kState;
  msg.protocol_id = kProtocolId;
  msg.session_id = session_id;
  msg.message_id = message_id;
  msg.payload = Pay(payload);
  SystemDatagramCodec codec;
  auto bytes = codec.Encode(msg);
  EXPECT_TRUE(static_cast<bool>(bytes));
  return std::move(bytes).value();
}

}  // namespace

// ⭐ ADR-0020 **D3**:`ProtocolNode` 收到 UDP 报文后,`msg.endpoint` 是**发送方**。
//
// 两个不同的发送方各发一帧,节点这一侧从 `endpoint` 就能分辨出各自的 `ip:port`——这在
// 本 ADR 之前**做不到**(两个旧字段 `topic` / `source` 一个都不填)。
TEST(ProtocolNodeEndpoint, InboundEndpointIsTheUdpSenderAddress) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender_a(LoopbackConfig());
  UdpTransport sender_b(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender_a.Start());
  ASSERT_TRUE(sender_b.Start());

  ProtocolNode node(receiver, std::make_unique<SystemDatagramCodec>(),
                    NodeConfig());
  ASSERT_TRUE(node.Start());

  auto ticket = node.Subscribe(AnyOfType(FrameType::kState));
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  auto mailbox = ticket.value().mailbox();

  const Endpoint to_receiver = Endpoint::Net(kLoopback, receiver.LocalPort());
  ASSERT_TRUE(sender_a.AsyncWrite({StateFrame(1, 0x0011, {0xA1, 0xA2}), to_receiver}));

  auto first = Coro::await_for(mailbox, 3000ms);
  ASSERT_TRUE(first) << "业务帧未经真实 UDP 抵达订阅信箱";
  EXPECT_EQ(first.value().endpoint.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(first.value().endpoint.host, std::string(kLoopback));
  EXPECT_EQ(first.value().endpoint.port, sender_a.LocalPort())
      << "endpoint 必须是【这一帧的】发送方——回帧就靠它";
  EXPECT_EQ(ToVec(first.value().payload), (std::vector<std::uint8_t>{0xA1, 0xA2}));

  // 换一个发送方:同一个订阅上,`endpoint` 随之改变(它是每帧一份的事实,不是节点配置)。
  ASSERT_TRUE(sender_b.AsyncWrite({StateFrame(2, 0x0011, {0xB1}), to_receiver}));
  auto second = Coro::await_for(mailbox, 3000ms);
  ASSERT_TRUE(second);
  EXPECT_EQ(second.value().endpoint.port, sender_b.LocalPort());
  EXPECT_NE(second.value().endpoint.port, first.value().endpoint.port);

  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(receiver.Close());
  receiver.WaitClosed();
  ASSERT_TRUE(sender_a.Close());
  sender_a.WaitClosed();
  ASSERT_TRUE(sender_b.Close());
  sender_b.WaitClosed();
}

// 入站 `frame` 与 payload 视图在**整条真实链路**的末端仍然成立:信箱里那一条是
// `Dispatcher` 发出的**副本**(ADR-0020 D2 的拷贝安全性),`frame` 是线缆上那一整帧。
TEST(ProtocolNodeEndpoint, InboundFrameAndPayloadViewSurviveTheWholePipeline) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());

  ProtocolNode node(receiver, std::make_unique<SystemDatagramCodec>(),
                    NodeConfig());
  ASSERT_TRUE(node.Start());

  auto ticket = node.Subscribe(AnyOfType(FrameType::kState));
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  auto mailbox = ticket.value().mailbox();

  const std::vector<std::uint8_t> wire = StateFrame(7, 0x00BB, {0xC0, 0xDE});
  ASSERT_TRUE(sender.AsyncWrite(
      {wire, Endpoint::Net(kLoopback, receiver.LocalPort())}));

  auto got = Coro::await_for(mailbox, 3000ms);
  ASSERT_TRUE(got) << "业务帧未抵达订阅信箱";
  const Message& msg = got.value();

  EXPECT_EQ(ToVec(msg.frame), wire) << "frame 应是线缆上那一整帧(帧头 → payload 末)";
  EXPECT_TRUE(PayloadIsViewOfFrame(msg))
      << "经过读队列、读循环与 Dispatcher 的副本之后,payload 仍须是 frame 的视图";
  EXPECT_EQ(ToVec(msg.payload), (std::vector<std::uint8_t>{0xC0, 0xDE}));

  // 需要活得比这条 `Message` 久时才调 `OwnedPayload()`(D8)——它是深拷贝,落在 frame 外。
  const QByteArray owned = msg.OwnedPayload();
  const char* begin = msg.frame.constData();
  EXPECT_TRUE(owned.constData() < begin ||
              owned.constData() >= begin + msg.frame.size());
  EXPECT_EQ(ToVec(owned), (std::vector<std::uint8_t>{0xC0, 0xDE}));

  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  ASSERT_TRUE(receiver.Close());
  receiver.WaitClosed();
  ASSERT_TRUE(sender.Close());
  sender.WaitClosed();
}
