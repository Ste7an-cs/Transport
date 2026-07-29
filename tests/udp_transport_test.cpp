// 协程原生 UdpTransport 真实 UDP 回环集成测试。
// 在 fiber 调度器(coro_test_main)内用本机 UDP loopback 验证:报文边界保持、
// source 填发送方地址(from 可变)、按 destination 发往不同地址、非法目的地/过大
// 报文 → 结构化错误、非重连生命周期(RequestClose→Closing→Closed、单读者约束),
// 以及经 DatagramCodec 的裸端到端收发。短超时确定化。
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/codec/DatagramCodec.hpp"
#include "transport/io/udp/UdpConfig.hpp"

using namespace std::chrono_literals;
using transport::DatagramCodec;
using transport::Datagram;
using transport::Endpoint;
using transport::Message;
using transport::OperationOptions;
using transport::SendUnit;
using transport::Status;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";

UdpConfig LoopbackConfig() {
  UdpConfig config;
  config.mode = transport::UdpMode::kUnicast;
  config.local_addr = kLoopback;
  config.local_port = 0;  // OS 分配临时端口。
  return config;
}

SendUnit ToPort(std::vector<std::uint8_t> bytes, std::uint16_t port) {
  return SendUnit{std::move(bytes), Endpoint::Net(kLoopback, port)};
}

// 以短 deadline 读一条报文;超时返回错误 Result(不永久挂起)。
transport::Result<Datagram> ReadOne(UdpTransport& t, int budget_ms = 2000) {
  OperationOptions options;
  options.deadline =
      OperationOptions::Clock::now() + std::chrono::milliseconds(budget_ms);
  return t.Read(options);
}

}  // namespace

// AC:发一报文 → 收到完整报文 + source==发送方地址(ip+port);报文边界保持。
TEST(CoroUdpTransport, DeliversDatagramWithSenderAddress) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());
  ASSERT_NE(receiver.LocalPort(), 0);

  const std::vector<std::uint8_t> payload{1, 2, 3, 4, 5, 6, 7, 8};
  ASSERT_TRUE(sender.Write(ToPort(payload, receiver.LocalPort())));
  EXPECT_TRUE(sender.LastSendTime().has_value());

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
  EXPECT_EQ(got.value().source.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(got.value().source.host, std::string(kLoopback));
  EXPECT_EQ(got.value().source.port, sender.LocalPort());
  EXPECT_TRUE(receiver.LastReceiveTime().has_value());
}

// AC:1200B 不分片报文完整送达,边界保持(一次 Read 恰好一条完整报文)。
TEST(CoroUdpTransport, PreservesDatagramBoundariesForBaselinePayload) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> first(1200, 0xAB);
  const std::vector<std::uint8_t> second(37, 0xCD);
  ASSERT_TRUE(sender.Write(ToPort(first, receiver.LocalPort())));
  ASSERT_TRUE(sender.Write(ToPort(second, receiver.LocalPort())));

  auto r1 = ReadOne(receiver);
  ASSERT_TRUE(r1) << r1.error().message();
  EXPECT_EQ(r1.value().bytes, first);  // 一次 Read 恰好第一条完整报文,不拼接。
  auto r2 = ReadOne(receiver);
  ASSERT_TRUE(r2) << r2.error().message();
  EXPECT_EQ(r2.value().bytes, second);
}

// AC:同一 sender 按 destination 发往不同地址,各达对应对端。
TEST(CoroUdpTransport, RoutesToDistinctDestinations) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport peer_a(LoopbackConfig());
  UdpTransport peer_b(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(peer_a.Start());
  ASSERT_TRUE(peer_b.Start());

  const std::vector<std::uint8_t> to_a{0xA1, 0xA2};
  const std::vector<std::uint8_t> to_b{0xB1, 0xB2, 0xB3};
  ASSERT_TRUE(sender.Write(ToPort(to_a, peer_a.LocalPort())));
  ASSERT_TRUE(sender.Write(ToPort(to_b, peer_b.LocalPort())));

  auto ra = ReadOne(peer_a);
  auto rb = ReadOne(peer_b);
  ASSERT_TRUE(ra) << ra.error().message();
  ASSERT_TRUE(rb) << rb.error().message();
  EXPECT_EQ(ra.value().bytes, to_a);
  EXPECT_EQ(rb.value().bytes, to_b);
}

// AC:非 kNet destination → kInvalidArgument。
TEST(CoroUdpTransport, RejectsNonNetDestination) {
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(sender.Start());

  const auto def = sender.Write(SendUnit{{1, 2, 3}, Endpoint::Default()});
  ASSERT_FALSE(def);
  EXPECT_EQ(def.error(), make_error_code(TransportErrc::kInvalidArgument));

  const auto topic =
      sender.Write(SendUnit{{1, 2, 3}, Endpoint::Topic("some/topic")});
  ASSERT_FALSE(topic);
  EXPECT_EQ(topic.error(), make_error_code(TransportErrc::kInvalidArgument));
}

// AC:发送失败 → 结构化错误。过大报文(超 UDP payload 上限)→ kInvalidArgument。
TEST(CoroUdpTransport, RejectsOversizedDatagram) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> too_big(70000, 0xEE);  // > 65507 UDP 上限。
  const auto sent = sender.Write(ToPort(too_big, receiver.LocalPort()));
  ASSERT_FALSE(sent);
  EXPECT_EQ(sent.error(), make_error_code(TransportErrc::kInvalidArgument));
}

// AC:非重连生命周期——RequestClose → Closing→Closed;在途 Read 以 Closed 唤醒。
TEST(CoroUdpTransport, RequestCloseWakesPendingReadAndReachesClosed) {
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());

  std::error_code read_err;
  bool read_ok = true;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    auto r = receiver.Read(options);
    read_ok = static_cast<bool>(r);
    if (!r) read_err = r.error();
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让 Read 挂起在接收流上。

  EXPECT_TRUE(receiver.RequestClose());
  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kClosed));

  EXPECT_TRUE(receiver.WaitClosed());
  // 关闭后不得继续发送/读。
  const auto after = receiver.Write(ToPort({1}, 65000));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
  const auto read_after = receiver.Read();
  ASSERT_FALSE(read_after);
  EXPECT_EQ(read_after.error(), make_error_code(TransportErrc::kClosed));
}

// AC:单读者约束——在途 Read 时并发第二 Read 立即返回 InvalidState。
TEST(CoroUdpTransport, RejectsConcurrentSecondReader) {
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());

  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 500ms;
    (void)receiver.Read(options);  // 挂起(无人发)→ 超时收敛。
  });
  ASSERT_TRUE(entered.await());
  ASSERT_TRUE(testutil::pumpFiberUntil([&] {
    const auto r = receiver.Read();  // 已有在途读者 → 立即拒。
    return !r && r.error() == make_error_code(TransportErrc::kInvalidState);
  }));
  EXPECT_TRUE(reader.get());
}

// AC(可选):裸 node 经 UdpTransport + DatagramCodec 端到端收发。
TEST(CoroUdpTransport, EndToEndThroughDatagramCodec) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  DatagramCodec codec;
  Message out;
  out.payload = {0xDE, 0xAD, 0xBE, 0xEF};
  auto encoded = codec.Encode(out);
  ASSERT_TRUE(encoded);
  ASSERT_TRUE(sender.Write(ToPort(encoded.value(), receiver.LocalPort())));

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  auto decoded = codec.Decode(got.value().bytes.data(), got.value().bytes.size());
  ASSERT_TRUE(decoded);
  ASSERT_EQ(decoded.value().size(), 1u);
  EXPECT_EQ(decoded.value().front().payload, out.payload);
}
