#include <chrono>
#include <memory>
#include <string>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/coro/ProtocolNode.hpp"
#include "transport/udp/UdpTransport.hpp"
#include "task/fibertask.h"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using transport::FrameType;
using transport::Message;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using transport::coro::ProtocolNode;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

// 节点层往返(Fake 传输):Request → 手工回显 RESULT → 返回。
TEST(CoroProtocolNode, RequestRoundtripOverFake) {
  auto tp = std::make_shared<FakeTransport>();
  auto node = std::make_shared<ProtocolNode>(tp, /*protocol_id*/3);
  ASSERT_TRUE(static_cast<bool>(node->Start()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    got = node->Request(0x77, {1}, 1000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message reply = cmd.value[0]; reply.frm_type = FrameType::kResult; reply.payload = {2, 2};
  tp->inject(codec.Encode(reply).value);

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{2, 2}));
  node->Stop();
  (void)req;
}

// 真 UDP 回环冒烟:两个 UdpTransport,一端是节点,另一端手工当"服务端"回一条 RESULT。
TEST(CoroProtocolNode, RequestOverUdpLoopback) {
  // 服务端 UDP:收到命令帧 → 回显成 RESULT。
  UdpConfig sc; sc.mode = UdpMode::kUnicast; sc.local_addr = "127.0.0.1"; sc.local_port = 0;
  auto srv = std::make_shared<UdpTransport>(sc);
  SystemDatagramCodec codec;
  srv->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& from) {
    if (!r) return;
    auto ms = codec.Decode(r.value.data(), r.value.size());
    if (!ms || ms.value.empty()) return;
    Message reply = ms.value[0]; reply.frm_type = FrameType::kResult; reply.payload = {0xEE};
    // from = "ip:port";回给来源。
    auto colon = from.rfind(':');
    std::string ip = from.substr(0, colon);
    uint16_t port = static_cast<uint16_t>(std::stoi(from.substr(colon + 1)));
    (void)srv->Send(codec.Encode(reply).value, transport::Endpoint::Net(ip, port));
  });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  uint16_t srv_port = srv->LocalPort();

  // 客户端节点 UDP:默认目的地 = 服务端。
  UdpConfig cc; cc.mode = UdpMode::kUnicast; cc.local_addr = "127.0.0.1"; cc.local_port = 0;
  cc.remote_addr = "127.0.0.1"; cc.remote_port = srv_port;
  auto ctp = std::make_shared<UdpTransport>(cc);
  auto node = std::make_shared<ProtocolNode>(ctp, /*protocol_id*/1);
  ASSERT_TRUE(static_cast<bool>(node->Start()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    got = node->Request(0x10, {0xAA}, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 3000));   // sleep_for 泵进 Qt 事件 + fiber
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.frm_type, FrameType::kResult);
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{0xEE}));
  node->Stop(); srv->Close();
  (void)req;
}
