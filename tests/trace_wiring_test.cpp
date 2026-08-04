// -----------------------------------------------------------------------------
// trace_wiring_test.cpp — P5-4:9 类 Trace 事件类别接入 + 4 项新增指标 getter
//
// 承接 ADR-0003 D13 Q5、RT_TRACE_001/002、RT_DATA_BUFFER。验证:
//   · 9 类 Trace(connect/generation/send/recv/decode/match/timeout/cancel/handler/
//     reconnect/close)各至少产生一条可采集事件(CapturingTraceSink)。
//   · 4 项新增指标 getter 数值合理:LastRequestLatency/LastHandlerDuration/
//     LastCloseLatency(TcpClientTransport 已有 AttemptCount,本票只补 Trace)。
//   · 未配 sink(RT_TRACE_002):控制流/结果与配 sink 时一致,指标 getter 仍正常记录
//     (latency 追踪与 Trace 上报解耦,互不依赖)。
//
// 拓扑:TCP 类别(connect/generation/reconnect/close)用真实 TcpClientTransport +
// QTcpServer 回环(同 tcp_client_transport_test 范式);send/recv/decode/match/
// timeout/cancel/handler/close 用 ProtocolNode + FakeCoroTransport(同
// protocol_node_handler_test 范式)。DdsNode 复用 dds_node_test 的 FakeDdsProvider
// 共享 Bus 拓扑,验证 trace_sink 装配对 DDS 侧同样生效(send/recv/decode/match)。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <QHostAddress>
#include <QTcpServer>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"
#include "task/fibertask.h"
#include "transport/codec/DdsCodec.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::CancellationSource;
using transport::CapturingTraceSink;
using transport::ConnectionState;
using transport::Datagram;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsHandlerContext;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::FrameType;
using transport::HandlerContext;
using transport::ITraceSink;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::Status;
using transport::SystemCodec;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = Clock::now() + d;
  return o;
}

bool HasCategory(const std::vector<CapturingTraceSink::Record>& records,
                 const std::string& category) {
  return std::any_of(records.begin(), records.end(),
                      [&](const auto& r) { return r.category == category; });
}

TcpClientConfig FastConfig(quint16 port, ITraceSink* sink) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 400ms;
  cfg.initial_backoff = 20ms;
  cfg.max_backoff = 80ms;
  cfg.backoff_multiplier = 2.0;
  cfg.jitter_enabled = false;
  cfg.stable_reset_after = 10s;
  cfg.trace_sink = sink;
  return cfg;
}

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

Datagram MakeBusinessDatagram(FrameType frm_type, std::uint8_t session_id,
                              std::uint16_t message_id) {
  Message msg;
  msg.frm_type = frm_type;
  msg.session_id = session_id;
  msg.message_id = message_id;
  SystemCodec wire;
  auto bytes = wire.Encode(msg);
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

Datagram MakeResponseDatagram(std::uint8_t session_id, std::uint16_t message_id,
                              std::vector<std::uint8_t> payload) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.session_id = session_id;
  resp.message_id = message_id;
  resp.payload = std::move(payload);
  SystemCodec wire;
  auto bytes = wire.Encode(resp);
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

}  // namespace

// —— TCP 侧四类:connect / generation / reconnect / close(+ LastCloseLatency)。——
TEST(TraceWiring, TcpClientCapturesConnectGenerationReconnectAndClose) {
  CapturingTraceSink sink;
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort(), &sink));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.Generation(), 1u);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));

  const auto records = sink.Records();
  EXPECT_TRUE(HasCategory(records, "connect"));
  EXPECT_TRUE(HasCategory(records, "generation"));
  EXPECT_TRUE(HasCategory(records, "reconnect"));
  EXPECT_TRUE(HasCategory(records, "close"));
  EXPECT_GT(client.LastCloseLatency().count(), 0);
}

// —— ProtocolNode 六类:send / recv / decode / match / handler / close
//    (+ LastRequestLatency / LastHandlerDuration / LastCloseLatency)。——
TEST(TraceWiring, ProtocolNodeCapturesSendRecvDecodeMatchHandlerAndClose) {
  CapturingTraceSink sink;
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  config.handler = [](const Message&, HandlerContext&) -> Status { return Status{}; };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 请求-响应回合:Write 完成(send)→ 对端注入响应 → Decode 成功(decode)→ 解出消息
  // (recv)→ PendingTable 匹配(match)。
  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {0xAB}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  fake->Inject(MakeResponseDatagram(0, 0x1002, {0xCD}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_GT(node.LastRequestLatency().count(), 0);

  // 业务帧 → handler 单次调用起止(handler)。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0010));
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return node.LastHandlerDuration().count() > 0; }));

  // Close:生命周期跃迁(close)+ 关闭时延。
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(request.get());
  EXPECT_GT(node.LastCloseLatency().count(), 0);

  const auto records = sink.Records();
  EXPECT_TRUE(HasCategory(records, "send"));
  EXPECT_TRUE(HasCategory(records, "recv"));
  EXPECT_TRUE(HasCategory(records, "decode"));
  EXPECT_TRUE(HasCategory(records, "match"));
  EXPECT_TRUE(HasCategory(records, "handler"));
  EXPECT_TRUE(HasCategory(records, "close"));
}

// —— PendingTable::Handle::Wait 的另两类终结:timeout / cancel(+ LastRequestLatency)。——
TEST(TraceWiring, ProtocolNodeCapturesTimeout) {
  CapturingTraceSink sink;
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 无响应注入,短 deadline → 超时终结。
  Result<Message> outcome{Message{}};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {}), Deadline(30ms));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 500));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_GT(node.LastRequestLatency().count(), 0);

  node.Close();
  EXPECT_TRUE(HasCategory(sink.Records(), "timeout"));
}

TEST(TraceWiring, ProtocolNodeCapturesCancel) {
  CapturingTraceSink sink;
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  CancellationSource source;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  OperationOptions options;
  options.cancellation = source.token();
  Result<Message> outcome{Message{}};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {}), options);
    done = true;
  });
  pumpFiberUntil([] { return false; }, 20);  // 让请求 fiber 跑到 Wait() 挂起。
  EXPECT_FALSE(done);
  EXPECT_TRUE(source.Cancel());
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 500));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kCancelled));
  EXPECT_GT(node.LastRequestLatency().count(), 0);

  node.Close();
  EXPECT_TRUE(HasCategory(sink.Records(), "cancel"));
}

// —— DdsNode:trace_sink 装配同样生效(send/recv/decode/match)。——
TEST(TraceWiring, DdsNodeCapturesSendRecvDecodeAndMatch) {
  DdsConfig dds_cfg;
  dds_cfg.domain_id = 0;
  auto bus = std::make_shared<FakeDdsProvider::Bus>();

  CapturingTraceSink server_sink;
  auto server_provider = std::make_unique<FakeDdsProvider>(bus);
  auto server_transport = std::make_unique<DdsTransport>(
      std::move(server_provider), dds_cfg,
      std::vector<std::string>{"svc", "srv_inbox"});
  DdsNodeConfig server_cfg;
  server_cfg.inbox_topic = "srv_inbox";
  server_cfg.node_id = "srv";
  server_cfg.trace_sink = &server_sink;
  server_cfg.handler = [](const Message& msg, DdsHandlerContext& ctx) -> Status {
    Message reply;
    reply.payload = msg.payload;
    return ctx.Reply(msg, std::move(reply));
  };
  DdsNode server(std::move(server_transport), std::make_unique<DdsCodec>(),
                std::move(server_cfg));
  ASSERT_TRUE(static_cast<bool>(server.Start()));

  CapturingTraceSink client_sink;
  auto client_provider = std::make_unique<FakeDdsProvider>(bus);
  auto client_transport = std::make_unique<DdsTransport>(
      std::move(client_provider), dds_cfg, std::vector<std::string>{"cli_inbox"});
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  client_cfg.trace_sink = &client_sink;
  DdsNode client(std::move(client_transport), std::make_unique<DdsCodec>(),
                std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(client.Start()));

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m;
    m.payload = {0x01, 0x02};
    outcome = client.Request(std::move(m), Endpoint::Topic("svc"), Deadline(3000ms));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_GT(client.LastRequestLatency().count(), 0);

  client.Close();
  server.Close();

  const auto client_records = client_sink.Records();
  const auto server_records = server_sink.Records();
  EXPECT_TRUE(HasCategory(client_records, "send"));    // client Request 写出。
  EXPECT_TRUE(HasCategory(server_records, "recv"));    // server 收到 kRequest。
  EXPECT_TRUE(HasCategory(server_records, "decode"));  // server decode 成功。
  EXPECT_TRUE(HasCategory(client_records, "match"));   // client 收到匹配 kReply。
}

// —— RT_TRACE_002:未配 sink 时控制流/结果不受影响;指标 getter 仍正常记录
//    (latency 追踪与 Trace 上报解耦,互不依赖)。——
TEST(TraceWiring, NullSinkLeavesControlFlowAndMetricsUnaffected) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;  // trace_sink 默认 nullptr。
  config.handler = [](const Message&, HandlerContext&) -> Status { return Status{}; };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {0xAB}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  fake->Inject(MakeResponseDatagram(0, 0x1002, {0xCD}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0xCD}));
  EXPECT_GT(node.LastRequestLatency().count(), 0);

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0010));
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return node.LastHandlerDuration().count() > 0; }));

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed(Deadline(2000ms)));
  EXPECT_TRUE(request.get());
  EXPECT_GT(node.LastCloseLatency().count(), 0);
}

// —— RT_TRACE_002:TcpClientTransport 未配 sink 时同样零副作用,连接/重连行为不变。——
TEST(TraceWiring, TcpClientNullSinkStillConnectsAndReportsCloseLatency) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort(), nullptr));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.Generation(), 1u);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  EXPECT_GT(client.LastCloseLatency().count(), 0);
}
