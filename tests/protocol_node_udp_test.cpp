// -----------------------------------------------------------------------------
// protocol_node_udp_test.cpp — ProtocolNode 无缝跑在 UdpTransport 上的端到端验证
//
// 目的:证实 ITransport 缝可无缝切换。请求方是**与 TCP 回环测试同一个 ProtocolNode**
// (逻辑零改动),只把注入的 ITransport 由 TcpTransport 换成 UdpTransport、codec 由流式
// SystemCodec 换成报文式 SystemDatagramCodec(UDP 报文边界=帧边界)。ProtocolNode 的
// Request/读-分发循环/关联匹配一字未改。
//
// 拓扑(对照 protocol_node_tcp_loopback_test 的裸 echo 范式,换 UDP):
//   · 请求方 = 真 ProtocolNode(UdpTransport(remote=对端) + SystemDatagramCodec)。
//     ProtocolNode 恒发 Endpoint::Default() → UdpTransport 解析为 UdpConfig 默认目的地
//     (remote_addr:remote_port)——这是"无缝"的关键:传输无关的 node 不需要知道对端地址,
//     由 UdpConfig 提供(Endpoint::kDefault 语义)。
//   · 对端 = 裸 UdpTransport + SystemDatagramCodec echo fiber:Read 报文(source=请求方地址)
//     → Decode → 对 kCommand 回一帧标准响应 → Write 回 source(kNet,发往请求方)。
// 全部在 coro_test_main 单 fiber 调度器内协作。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/io/udp/UdpConfig.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::Result;
using transport::SendUnit;
using transport::SystemDatagramCodec;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

namespace {

const char* kLoopback = "127.0.0.1";

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

// 标准 echo 响应:frm_type=kResponse、session_id 原样、message_id = 请求码 | 0x1000、
// payload echo(与 DefaultProtocolKeyStrategy 的配对规则一致,与 TCP 用例同)。
Message EchoResponse(const Message& command) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.protocol_id = command.protocol_id;
  resp.session_id = command.session_id;
  resp.message_id = static_cast<std::uint16_t>(command.message_id | 0x1000);
  resp.payload = command.payload;
  return resp;
}

// 裸 UDP echo fiber:收 kCommand 报文 → 回一帧标准响应到发送方地址(kNet=报文 source)。
// 传输终结(我方 kClosed)→ 退出并置 ended=true。
template <typename Responder>
auto SpawnUdpEcho(UdpTransport& transport, Responder responder, bool& ended) {
  return Coro::makeTask([&transport, responder, &ended] {
    SystemDatagramCodec codec;
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
      const Endpoint from = datagram.value().source;  // 请求方地址(kNet,发往它回帧)。
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
          transport.Write(SendUnit{std::move(encoded).value(), from});
        }
      }
    }
    ended = true;
  });
}

std::vector<Message> EchoResponder(const Message& command) {
  return {EchoResponse(command)};
}

}  // namespace

// 核心验证:ProtocolNode 无缝跑在 UDP 上。请求方 = 真 ProtocolNode(UdpTransport +
// SystemDatagramCodec),对端 = 裸 UDP echo。一次 needresponse 请求-响应恰好一次完成、
// 关联清理——ProtocolNode 逻辑与 TCP 用例完全一致,仅传输/codec 注入不同。
TEST(ProtocolNodeUdp, RequestResolvedOverUdpSeamlessSwap) {
  // 对端:裸 UdpTransport,绑临时端口。
  UdpConfig echo_cfg;
  echo_cfg.local_addr = kLoopback;
  echo_cfg.local_port = 0;  // OS 分配。
  UdpTransport echo_transport(echo_cfg);
  ASSERT_TRUE(echo_transport.Start());
  const std::uint16_t echo_port = echo_transport.LocalPort();
  ASSERT_GT(echo_port, 0u);

  bool echo_ended = false;
  auto echo = SpawnUdpEcho(echo_transport, EchoResponder, echo_ended);

  // 请求方:ProtocolNode over UdpTransport,remote 指向对端(Default 由此解析)。
  UdpConfig req_cfg;
  req_cfg.local_addr = kLoopback;
  req_cfg.local_port = 0;
  req_cfg.remote_addr = kLoopback;
  req_cfg.remote_port = echo_port;  // ProtocolNode 恒发 Default → 解析到这里。
  auto node = std::make_unique<ProtocolNode>(
      std::make_unique<UdpTransport>(req_cfg),
      std::make_unique<SystemDatagramCodec>());
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node->Request(MakeRequest(0x0002, {0x11, 0x22, 0x33}), options);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(outcome.value().session_id, 0);  // 首个滚动 session_id,与 TCP 用例一致。
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(outcome.value().payload,
            (std::vector<std::uint8_t>{0x11, 0x22, 0x33}));
  EXPECT_EQ(node->UnmatchedResponseCount(), 0u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);

  ASSERT_TRUE(node->Close());
  ASSERT_TRUE(echo_transport.RequestClose());
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
}

// RT_REQUEST_004 over UDP:乱序/迟到/无匹配响应被丢弃并观测——同一 ProtocolNode 的关联
// 纪律在 UDP 上同样成立(与 TCP 用例对照)。对端回 [错 key, 正确, 重复] 三帧。
TEST(ProtocolNodeUdp, LateAndWrongKeyResponsesDroppedOverUdp) {
  UdpConfig echo_cfg;
  echo_cfg.local_addr = kLoopback;
  echo_cfg.local_port = 0;
  UdpTransport echo_transport(echo_cfg);
  ASSERT_TRUE(echo_transport.Start());
  const std::uint16_t echo_port = echo_transport.LocalPort();
  ASSERT_GT(echo_port, 0u);

  bool echo_ended = false;
  auto responder = [](const Message& command) {
    Message wrong_key = EchoResponse(command);
    wrong_key.session_id = static_cast<std::uint8_t>(command.session_id + 7);
    Message correct = EchoResponse(command);
    Message duplicate = EchoResponse(command);
    return std::vector<Message>{wrong_key, correct, duplicate};
  };
  auto echo = SpawnUdpEcho(echo_transport, responder, echo_ended);

  UdpConfig req_cfg;
  req_cfg.local_addr = kLoopback;
  req_cfg.local_port = 0;
  req_cfg.remote_addr = kLoopback;
  req_cfg.remote_port = echo_port;
  auto node = std::make_unique<ProtocolNode>(
      std::make_unique<UdpTransport>(req_cfg),
      std::make_unique<SystemDatagramCodec>());
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node->Request(MakeRequest(0x0005, {0xAA}), options);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();  // 恰好一次完成。
  EXPECT_EQ(outcome.value().message_id, 0x1005);

  ASSERT_TRUE(pumpFiberUntil([&] { return node->UnmatchedResponseCount() == 2u; }));
  EXPECT_EQ(node->UnmatchedResponseCount(), 2u);
  EXPECT_EQ(node->DroppedNoHandlerCount(), 0u);

  ASSERT_TRUE(node->Close());
  ASSERT_TRUE(echo_transport.RequestClose());
  EXPECT_TRUE(pumpFiberUntil([&] { return echo_ended; }));
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
}
