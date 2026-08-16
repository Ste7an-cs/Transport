// 协程原生 UdpTransport 真实 UDP 回环集成测试。
// 在 fiber 调度器(coro_test_main)内用本机 UDP loopback 验证:报文边界保持、
// source 填发送方地址(from 可变)、按 destination 发往不同地址、非法目的地 →
// 结构化错误、生命周期(RequestClose→Closing→Closed;单读守卫已随 ADR-0007 D4
// 删除,改验多消费者抢占),以及经 DatagramCodec 的裸端到端收发。短超时确定化。
//
// **socket 管理泵 + 读写双队列(ADR-0007 D1/D2/D3)** 的形态用例集中在文件末尾:
// bind 失败无限重试(Start 仍成功)、链路不可用时 Write 排队待恢复后按序全发、
// Close 在**四种停留位置**(bind 退避中 / 读等待中 / 写泵阻塞点① / 写泵阻塞点②)
// 各自都能干净收敛。
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>
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

// 绑定固定端口的回环配置(用于"端口被占 → bind 失败 → 释放后重试成功"的确定化构造)。
UdpConfig PortConfig(std::uint16_t port) {
  UdpConfig config = LoopbackConfig();
  config.local_port = port;
  return config;
}

SendUnit ToPort(std::vector<std::uint8_t> bytes, std::uint16_t port) {
  return SendUnit{std::move(bytes), Endpoint::Net(kLoopback, port)};
}

// 在 read_queue 句柄上以短 deadline 取一条报文;超时返回错误 Result(不永久挂起)。
transport::Result<Datagram> ReadOne(UdpTransport& t, int budget_ms = 2000) {
  OperationOptions options;
  options.deadline =
      OperationOptions::Clock::now() + std::chrono::milliseconds(budget_ms);
  return testutil::ReadOnce(t, options);
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

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  // LastSendTime 由**写泵**在实际发出后记账(ADR-0007 D3:Write 只入队即返),故在
  // 报文送达后才断言——Write 返回的那一刻它尚未发出。
  EXPECT_TRUE(sender.LastSendTime().has_value());
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

// AC:发送失败 → 结构化错误,但**不回传调用方**(ADR-0007 D3 fire-and-forget):
// 过大报文(超 UDP payload 上限)入队成功,写泵发出失败后只进 LastError()
// (kInvalidArgument),传输不终结。
// 【因 ADR-0007 D3 改写】原用例断言 `Write` 同步返 kInvalidArgument。
TEST(CoroUdpTransport, OversizedDatagramFailsInWritePumpNotInWrite) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> too_big(70000, 0xEE);  // > 65507 UDP 上限。
  ASSERT_TRUE(sender.Write(ToPort(too_big, receiver.LocalPort())));  // 仅"已入队"。

  const std::error_code too_large =
      make_error_code(TransportErrc::kInvalidArgument);
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return sender.LastError() == too_large; }));
  EXPECT_EQ(sender.LastError(), too_large);

  // 传输未终结:仍可发、仍可收(单次发送失败不是致命错误)。
  const std::vector<std::uint8_t> payload{0x5A};
  ASSERT_TRUE(sender.Write(ToPort(payload, receiver.LocalPort())));
  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
}

// AC(ADR-0007 D2:UDP 不自终):socket 级致命 I/O **不再终结传输**——泵把它降为诊断
// 事实(LastError),回外层重建 socket 后继续收发;read_queue **不关闭**,故读者只是
// 继续等,不会看到 kClosed。**唯一的终止条件是我方 Close**。
// 【因 ADR-0007 D2 改写】原用例 `FatalSocketErrorYieldsClosed` 断言致命 I/O →
// 读取以 kClosed 收敛、生命周期自行 Closing→Closed(ADR-0005 D5 的自终结论,已被推翻)。
// 确定化触发:socket 绑在 IPv4 回环,向 IPv6 地址发一报文 → 内核以 socket 级错误
// (地址族不匹配,归 NetworkError)拒绝,接收流随之被该 socket 错误终止。这与"拔网线
// 后 sendto 返 ENETUNREACH"是同一条错误通路,只是可在回环上确定化复现(不依赖路由表)。
TEST(CoroUdpTransport, FatalSocketErrorDoesNotTerminateTransport) {
  UdpTransport t(LoopbackConfig());
  UdpTransport peer(LoopbackConfig());
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(peer.Start());
  ASSERT_EQ(t.CurrentLinkState(), transport::LinkState::kUp);

  // 写路径:fire-and-forget,入队即成功;底层故障只进 LastError()(非 kClosed)。
  ASSERT_TRUE(t.Write(SendUnit{{1, 2, 3}, Endpoint::Net("::1", 9)}));
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.LastError() != std::error_code{}; }))
      << "预期 socket 级拒绝:IPv4 socket 发往 IPv6 地址";
  EXPECT_NE(t.LastError(), make_error_code(TransportErrc::kClosed));

  // 读路径:**不终止**——本次读只是超时(kTimeout),不是 kClosed。
  auto not_fatal = ReadOne(t, 300);
  ASSERT_FALSE(not_fatal);
  EXPECT_EQ(not_fatal.error(), make_error_code(TransportErrc::kTimeout));

  // 泵重建 socket 后链路自行恢复,继续收数(重建对调用方透明)。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.CurrentLinkState() == transport::LinkState::kUp; }, 6000));
  const std::vector<std::uint8_t> payload{0x77, 0x88};
  ASSERT_TRUE(peer.Write(ToPort(payload, t.LocalPort())));
  auto got = ReadOne(t);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);

  // 终结只由我方 Close 触发。
  ASSERT_TRUE(t.RequestClose());
  EXPECT_TRUE(t.WaitClosed());
  const auto after = testutil::ReadOnce(t);
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(t.CurrentLinkState(), transport::LinkState::kDown);
}

// AC(边界守卫):单次操作的可恢复错误**不是**致命——不得被误改为 kClosed,亦不得
// 终结传输。非法 destination 只影响本次 Write,传输随后照常收发(SRS §3.1.2.4)。
TEST(CoroUdpTransport, RecoverableWriteErrorDoesNotTerminateTransport) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const auto bad = sender.Write(SendUnit{{9, 9}, Endpoint::Topic("t")});
  ASSERT_FALSE(bad);
  EXPECT_EQ(bad.error(), make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_NE(bad.error(), make_error_code(TransportErrc::kClosed));

  // 传输未终结:仍可发、仍可收。
  EXPECT_EQ(sender.CurrentLinkState(), transport::LinkState::kUp);
  const std::vector<std::uint8_t> payload{0x11, 0x22};
  ASSERT_TRUE(sender.Write(ToPort(payload, receiver.LocalPort())));
  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
}

// AC:生命周期——RequestClose → Closing→Closed;在途 Read 以 Closed 唤醒。
// 【四处打断之二:泵停在**读等待**中】Close 关 socket 打断活跃读流,泵回外层见
// Closing 即收敛(设计 §6/§8-4)。
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
    auto r = testutil::ReadOnce(receiver, options);
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
  const auto read_after = testutil::ReadOnce(receiver);
  ASSERT_FALSE(read_after);
  EXPECT_EQ(read_after.error(), make_error_code(TransportErrc::kClosed));
}

// AC(ADR-0007 D4):单读守卫已删除——两个消费者共读同一 read_queue **天然抢占**,
// 第二个读者不再被拒(不返 kInvalidState),两条报文各归其一、不重复不丢失。
TEST(CoroUdpTransport, ConcurrentReadersPreemptInsteadOfBeingRejected) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  transport::Result<Datagram> first{make_error_code(TransportErrc::kInternal)};
  transport::Result<Datagram> second{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> entered;
  auto reader_a = Coro::makeTask([&] {
    entered.resolve();
    first = ReadOne(receiver);
  });
  ASSERT_TRUE(entered.await());
  auto reader_b = Coro::makeTask([&] { second = ReadOne(receiver); });

  ASSERT_TRUE(sender.Write(ToPort({0xA1}, receiver.LocalPort())));
  ASSERT_TRUE(sender.Write(ToPort({0xA2}, receiver.LocalPort())));

  EXPECT_TRUE(reader_a.get());
  EXPECT_TRUE(reader_b.get());
  ASSERT_TRUE(first) << first.error().message();
  ASSERT_TRUE(second) << second.error().message();
  std::vector<std::vector<std::uint8_t>> got{first.value().bytes,
                                             second.value().bytes};
  std::sort(got.begin(), got.end());  // 谁先取到由调度决定,只断言集合。
  EXPECT_EQ(got, (std::vector<std::vector<std::uint8_t>>{{0xA1}, {0xA2}}));
}

// -----------------------------------------------------------------------------
// socket 管理泵 + 读写双队列(ADR-0007 D1/D2/D3;设计 §8 的 5 条)
// -----------------------------------------------------------------------------

// AC(设计 §8-1/§8-2):bind 失败(端口被占)→ **Start() 仍返成功**、泵进无限重试;
// 端口释放后自动 bind 成功并继续收数(重建对调用方透明)。
TEST(CoroUdpTransport, StartSucceedsWhenBindFailsThenBindsAfterPortIsFreed) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());
  const std::uint16_t port = holder.LocalPort();
  ASSERT_NE(port, 0);

  UdpTransport t(PortConfig(port));  // 与 holder 冲突 → 首次 bind 必失败。
  ASSERT_TRUE(t.Start()) << "首次 bind 未成不算启动失败(ADR-0007 D2)";
  boost::this_fiber::sleep_for(50ms);  // 让泵重试一轮后停进 3 秒退避。
  EXPECT_EQ(t.CurrentLinkState(), transport::LinkState::kDown);
  EXPECT_NE(t.LastError(), std::error_code{});  // bind 失败降为诊断事实。
  EXPECT_EQ(t.LocalPort(), 0);                  // 尚未绑上。

  // 释放端口 → 泵在下一次退避到期后 bind 成功(固定 3 秒重试间隔)。
  ASSERT_TRUE(holder.RequestClose());
  ASSERT_TRUE(holder.WaitClosed());
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.CurrentLinkState() == transport::LinkState::kUp; }, 6000));
  EXPECT_EQ(t.LocalPort(), port);

  // 继续收数:read_queue 从未更换,读者无感。
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  const std::vector<std::uint8_t> payload{0xC1, 0xC2, 0xC3};
  ASSERT_TRUE(sender.Write(ToPort(payload, port)));
  auto got = ReadOne(t);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
}

// AC(设计 §8-3 / ADR-0007 D3):链路不可用时 Write **不拒绝**,报文留在 write_queue;
// 链路恢复后**按序全部发出**。
TEST(CoroUdpTransport, WriteQueuesWhileLinkDownAndFlushesInOrderAfterRecovery) {
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  const std::uint16_t receiver_port = receiver.LocalPort();

  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());
  const std::uint16_t busy_port = holder.LocalPort();

  UdpTransport sender(PortConfig(busy_port));  // bind 失败 → 链路不可用。
  ASSERT_TRUE(sender.Start());
  ASSERT_EQ(sender.CurrentLinkState(), transport::LinkState::kDown);

  // 链路不可用时入队三条:不拒绝、不丢弃,也不产生 LastSendTime。
  for (std::uint8_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(sender.Write(ToPort({i}, receiver_port)))
        << "第 " << int(i) << " 条";
  }
  boost::this_fiber::sleep_for(50ms);
  EXPECT_FALSE(sender.LastSendTime().has_value());
  EXPECT_FALSE(ReadOne(receiver, 100));  // 确实一条都没发出。

  // 释放端口 → sender bind 成功 → 写泵被 socket_ready 唤醒,按序灌出全部积压。
  ASSERT_TRUE(holder.RequestClose());
  ASSERT_TRUE(holder.WaitClosed());
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return sender.CurrentLinkState() == transport::LinkState::kUp; },
      6000));

  for (std::uint8_t i = 1; i <= 3; ++i) {
    auto got = ReadOne(receiver);
    ASSERT_TRUE(got) << "第 " << int(i) << " 条:" << got.error().message();
    EXPECT_EQ(got.value().bytes, (std::vector<std::uint8_t>{i}));  // 按序。
  }
  EXPECT_TRUE(sender.LastSendTime().has_value());
}

// AC(设计 §8-4)【四处打断之一:泵停在 **bind 退避**中】退避由 close_signal 承载,
// Close 关它即提前唤醒——收敛必须**远快于** 3 秒的重试间隔,否则外层循环会挂到退避到期。
TEST(CoroUdpTransport, CloseDuringBindBackoffConvergesPromptly) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());

  UdpTransport t(PortConfig(holder.LocalPort()));
  ASSERT_TRUE(t.Start());
  boost::this_fiber::sleep_for(50ms);  // 让泵首轮重试失败后停进退避。
  ASSERT_EQ(t.CurrentLinkState(), transport::LinkState::kDown);

  ASSERT_TRUE(t.RequestClose());
  // 1.5s < 3s 退避间隔:能在此期限内收敛即证明退避被打断,而非等它自然到期。
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 1500ms;
  EXPECT_TRUE(t.WaitClosed(options));
  const auto after = testutil::ReadOnce(t);
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
  ASSERT_TRUE(holder.RequestClose());
}

// AC(设计 §8-4)【四处打断之三:写泵停在**阻塞点①**(等 write_queue 来数据)】
// 先发出一条(写泵因此确已回到阻塞点①),再 Close:关 write_queue 唤醒它。
TEST(CoroUdpTransport, CloseWhileWritePumpWaitsForDataConverges) {
  UdpTransport sender(LoopbackConfig());
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  ASSERT_TRUE(sender.Write(ToPort({0x01}, receiver.LocalPort())));
  ASSERT_TRUE(ReadOne(receiver));  // 已发出 → 写泵已回到阻塞点①等下一条。
  ASSERT_TRUE(sender.LastSendTime().has_value());

  ASSERT_TRUE(sender.RequestClose());
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 1500ms;
  EXPECT_TRUE(sender.WaitClosed(options));
  // 关闭后不再接受发送。
  const auto after = sender.Write(ToPort({0x02}, receiver.LocalPort()));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
}

// AC(设计 §8-4)【四处打断之四:写泵停在**阻塞点②**(等 socket 就绪)】
// 链路不可用时入队一条:写泵取出后卡在等 socket_ready;Close 关它即唤醒。
TEST(CoroUdpTransport, CloseWhileWritePumpWaitsForSocketReadyConverges) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());

  UdpTransport sender(PortConfig(holder.LocalPort()));  // bind 失败 → 未就绪。
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(sender.Write(ToPort({0xFF}, holder.LocalPort())));
  boost::this_fiber::sleep_for(50ms);  // 让写泵取出该条并停在阻塞点②。
  ASSERT_EQ(sender.CurrentLinkState(), transport::LinkState::kDown);
  ASSERT_FALSE(sender.LastSendTime().has_value());  // 确实卡在"等 socket 就绪"。

  ASSERT_TRUE(sender.RequestClose());
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 1500ms;
  EXPECT_TRUE(sender.WaitClosed(options));  // 泵先 join 写泵才落 Closed。
  ASSERT_TRUE(holder.RequestClose());
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
