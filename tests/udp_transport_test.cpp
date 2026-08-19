// 协程原生 UdpTransport 真实 UDP 回环集成测试(重设计后的 ITransport 面)。
//
// 在 fiber 调度器(coro_test_main)内用本机 UDP loopback 验证四组事实:
//
//   1. 数据面:报文边界保持、`Datagram::peer` 填发送方地址(from 可变)、按 destination
//      发往不同地址、`Endpoint::Default()` 解析为 config 默认对端;
//   2. 同步作答的参数错误:kTopic / 解析不出的地址 / config 未配默认对端 —— 这三样在
//      `AsyncWrite` 里入队前判定并返回,**不被 fire-and-forget 吞掉**;
//   3. socket 管理泵(ADR-0007 D1/D2):bind 失败无限重试(Start 仍成功)、链路不可用时
//      写入排队待恢复后按序全发、静默超时判链路坏并重建;
//   4. 生命周期:`Close()` 只发信号、`WaitClosed()` join 泵,且 Close 停在**四种位置**
//      (bind 退避中 / 读等待中 / 写泵等数据 / 写泵等 socket 就绪)都能干净收敛。
#include <chrono>
#include <cstdint>
#include <initializer_list>
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
#include "transport/io/udp/UdpConfig.hpp"
#include "transport/io/udp/UdpTransport.hpp"

using namespace std::chrono_literals;
using transport::Datagram;
using transport::Endpoint;
using transport::LinkState;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";

// 默认静默超时 5s:短用例跑不到,故不会有意外重建。
UdpConfig LoopbackConfig() {
  UdpConfig config;
  config.mode = transport::UdpMode::kUnicast;
  config.local_addr = kLoopback;
  config.local_port = 0;  // OS 分配临时端口。
  return config;
}

UdpConfig PortConfig(std::uint16_t port) {
  UdpConfig config = LoopbackConfig();
  config.local_port = port;
  return config;
}

// 借一个临时端口拿到号随即释放——供"固定端口"的确定化构造(重建后端口可断言)。
std::uint16_t GrabFreePort() {
  UdpTransport probe(LoopbackConfig());
  EXPECT_TRUE(probe.Start());
  const std::uint16_t port = probe.LocalPort();
  EXPECT_TRUE(probe.Close());
  probe.WaitClosed();
  return port;
}

std::vector<std::uint8_t> Bytes(std::initializer_list<std::uint8_t> v) {
  return std::vector<std::uint8_t>(v);
}

Coro::Result<Datagram> ReadOne(UdpTransport& t, int budget_ms = 2000) {
  return testutil::ReadOnce(t, std::chrono::milliseconds(budget_ms));
}

}  // namespace

// —— 1. 数据面 ——————————————————————————————————————————————————————————

// AC(RT_IF_UDP):一次读恰好一条完整报文,`peer` 填该报文的发送方地址。
TEST(CoroUdpTransport, DeliversDatagramWithSenderAddress) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());

  const auto payload = Bytes({0xA1, 0xA2, 0xA3});
  ASSERT_TRUE(
      sender.AsyncWrite({payload, Endpoint::Net(kLoopback, receiver.LocalPort())}));

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
  EXPECT_EQ(got.value().peer.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(got.value().peer.host, std::string(kLoopback));
  EXPECT_EQ(got.value().peer.port, sender.LocalPort());  // 回帧就靠它。
}

// AC:报文边界由内核保持——两次写出两条,不粘连、不切分。
TEST(CoroUdpTransport, PreservesDatagramBoundaries) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());
  const Endpoint to = Endpoint::Net(kLoopback, receiver.LocalPort());

  ASSERT_TRUE(sender.AsyncWrite({Bytes({1, 2, 3}), to}));
  ASSERT_TRUE(sender.AsyncWrite({Bytes({4, 5}), to}));

  auto first = ReadOne(receiver);
  ASSERT_TRUE(first) << first.error().message();
  EXPECT_EQ(first.value().bytes, Bytes({1, 2, 3}));
  auto second = ReadOne(receiver);
  ASSERT_TRUE(second) << second.error().message();
  EXPECT_EQ(second.value().bytes, Bytes({4, 5}));
}

// AC(ADR-0003 D12):同一条传输按 destination 发往不同地址。
TEST(CoroUdpTransport, RoutesToDistinctDestinations) {
  UdpTransport a(LoopbackConfig());
  UdpTransport b(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(a.Start());
  ASSERT_TRUE(b.Start());
  ASSERT_TRUE(sender.Start());

  ASSERT_TRUE(
      sender.AsyncWrite({Bytes({0xAA}), Endpoint::Net(kLoopback, a.LocalPort())}));
  ASSERT_TRUE(
      sender.AsyncWrite({Bytes({0xBB}), Endpoint::Net(kLoopback, b.LocalPort())}));

  auto ra = ReadOne(a);
  ASSERT_TRUE(ra) << ra.error().message();
  EXPECT_EQ(ra.value().bytes, Bytes({0xAA}));
  auto rb = ReadOne(b);
  ASSERT_TRUE(rb) << rb.error().message();
  EXPECT_EQ(rb.value().bytes, Bytes({0xBB}));
}

// AC:`Endpoint::Default()` 解析为 config 的默认对端 —— 这让恒发 Default 的传输无关
// 调用方(ProtocolNode)无缝跑在 UDP 上。
TEST(CoroUdpTransport, ResolvesDefaultDestinationFromConfig) {
  UdpTransport receiver(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());

  UdpConfig config = LoopbackConfig();
  config.remote_addr = kLoopback;
  config.remote_port = receiver.LocalPort();
  UdpTransport sender(config);
  ASSERT_TRUE(sender.Start());

  ASSERT_TRUE(sender.AsyncWrite({Bytes({7, 7, 7})}));  // 不给 destination = Default。

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, Bytes({7, 7, 7}));
}

// —— 2. 写侧的错误分工 ————————————————————————————————————————————————
//
// `AsyncWrite` 只判两件事:生命周期允不允许写、有没有真的入队。目的地能不能解析是**写泵**
// 才知道的事,解析不了就丢那一条并记 `LastError()`——fire-and-forget,不回传调用方。
TEST(CoroUdpTransport, UnsendableDestinationIsDroppedAndRecordedInLastError) {
  UdpTransport t(LoopbackConfig());  // 未配 remote_addr/remote_port。
  ASSERT_TRUE(t.Start());

  // 三种发不出去的目的地,入队一律成功。
  ASSERT_TRUE(t.AsyncWrite({Bytes({1}), Endpoint::Topic("some/topic")}));  // UDP 无此语义
  ASSERT_TRUE(t.AsyncWrite({Bytes({1})}));                          // Default 但 config 未配
  ASSERT_TRUE(t.AsyncWrite({Bytes({1}), Endpoint::Net("not-an-ip", 9)}));  // 解析不出 IP

  // 结果只出现在诊断面上。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] {
        return t.LastError() == make_error_code(TransportErrc::kInvalidArgument);
      },
      1000))
      << "写泵未记录目的地错误:LastError=" << t.LastError().message();
}

// AC:发不出去的一条**不阻塞后续**——它在出队时就被判掉,不会占着队头等 socket 就绪。
TEST(CoroUdpTransport, UnsendableDatagramDoesNotBlockLaterOnes) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());

  ASSERT_TRUE(sender.AsyncWrite({Bytes({0xEE}), Endpoint::Topic("bad")}));
  ASSERT_TRUE(sender.AsyncWrite(
      {Bytes({0x11}), Endpoint::Net(kLoopback, receiver.LocalPort())}));

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, Bytes({0x11}));
}

// AC:未 `Start()` 时,读句柄以 kInvalidState **已关闭**交出(句柄式接口没有返回错误码
// 的位置,故把该错误作为队列的终止原因),写直接返 kInvalidState。
TEST(CoroUdpTransport, BeforeStartReadHandleIsClosedAndWriteRejected) {
  UdpTransport t(LoopbackConfig());

  auto rx = t.AsyncRead();
  ASSERT_NE(rx, nullptr);
  auto got = testutil::AwaitRead(rx, 100ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kInvalidState));

  EXPECT_EQ(t.AsyncWrite({Bytes({1}), Endpoint::Net(kLoopback, 9)}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
}

// AC:`Start()` 幂等;关闭后再 `Start()` 拒绝(生命周期不可回滚)。
TEST(CoroUdpTransport, StartIsIdempotentAndRejectedAfterClose) {
  UdpTransport t(LoopbackConfig());
  ASSERT_TRUE(t.Start());
  const std::uint16_t port = t.LocalPort();
  ASSERT_TRUE(t.Start()) << "已 Running 时重复 Start 应为成功的空操作";
  EXPECT_EQ(t.LocalPort(), port) << "重复 Start 不应重新绑定";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  const auto restarted = t.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
}

// AC:未 `Start()` 时无绑定端口,链路不可用。
TEST(CoroUdpTransport, LocalPortIsZeroBeforeStart) {
  UdpTransport t(LoopbackConfig());
  EXPECT_EQ(t.LocalPort(), 0);
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  EXPECT_EQ(t.LastError(), std::error_code{});
}

// AC:超长报文入队成功、写出失败——这正是 fire-and-forget 的分工,失败只落 `LastError()`。
// UDP 单报文上限约 65507 字节,取 70000 必然发不出去。
TEST(CoroUdpTransport, OversizedDatagramFailsInWritePumpNotInAsyncWrite) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());

  std::vector<std::uint8_t> huge(70000, 0xAB);
  ASSERT_TRUE(sender.AsyncWrite(
      {std::move(huge), Endpoint::Net(kLoopback, receiver.LocalPort())}))
      << "入队本身不判报文大小";

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return sender.LastError() != std::error_code{}; }, 1000))
      << "写泵未记录发送失败";
  EXPECT_FALSE(ReadOne(receiver, 100)) << "超长报文不应被对端收到";
}

// AC:超长报文之后的正常报文仍能发出——单条写失败不影响写泵继续消费队列。
TEST(CoroUdpTransport, WritePumpContinuesAfterFailedDatagram) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());
  const Endpoint to = Endpoint::Net(kLoopback, receiver.LocalPort());

  ASSERT_TRUE(sender.AsyncWrite({std::vector<std::uint8_t>(70000, 0xAB), to}));
  ASSERT_TRUE(sender.AsyncWrite({Bytes({0x5A}), to}));

  auto got = ReadOne(receiver);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, Bytes({0x5A}));
}

// —— 3. socket 管理泵 ————————————————————————————————————————————————

// AC(ADR-0007 D2):首次 bind 失败**不算启动失败**,泵按 silence_timeout 无限重试;
// 端口释放后自愈,而 read_queue 从未更换 —— 重建对调用方透明。
TEST(CoroUdpTransport, StartSucceedsWhenBindFailsThenBindsAfterPortIsFreed) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());
  const std::uint16_t port = holder.LocalPort();
  ASSERT_NE(port, 0);

  UdpConfig config = PortConfig(port);
  config.silence_timeout = 200ms;  // 重试间隔与读超时同一个旋钮。
  UdpTransport t(config);
  ASSERT_TRUE(t.Start()) << "首次 bind 未成不算启动失败(ADR-0007 D2)";
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  EXPECT_NE(t.LastError(), std::error_code{});  // bind 失败降为诊断事实。
  EXPECT_EQ(t.LocalPort(), 0);                  // 尚未绑上。

  ASSERT_TRUE(holder.Close());
  holder.WaitClosed();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.CurrentLinkState() == LinkState::kUp; }, 3000));
  EXPECT_EQ(t.LocalPort(), port);

  // 继续收数:读者拿的是同一个 read_queue 句柄,对重建无感。
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  const auto payload = Bytes({0xC1, 0xC2});
  ASSERT_TRUE(sender.AsyncWrite({payload, Endpoint::Net(kLoopback, port)}));
  auto got = ReadOne(t);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, payload);
}

// AC(ADR-0007 D3):链路不可用时 `AsyncWrite` **不拒绝**,报文留在内部队列;链路恢复后
// **按序全部发出**。
TEST(CoroUdpTransport, WriteQueuesWhileLinkDownAndFlushesInOrderAfterRecovery) {
  // 接收方用固定端口 + 默认 5s 静默超时:整个用例期间不会重建、端口不会变号。
  UdpTransport receiver(PortConfig(GrabFreePort()));
  ASSERT_TRUE(receiver.Start());
  const std::uint16_t receiver_port = receiver.LocalPort();
  ASSERT_NE(receiver_port, 0);

  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());

  UdpConfig config = PortConfig(holder.LocalPort());  // 端口被占 → bind 必失败。
  config.silence_timeout = 200ms;
  UdpTransport sender(config);
  ASSERT_TRUE(sender.Start());
  ASSERT_EQ(sender.CurrentLinkState(), LinkState::kDown);

  // 链路不可用时入队三条:不拒绝、不丢弃。
  const Endpoint to = Endpoint::Net(kLoopback, receiver_port);
  for (std::uint8_t i = 1; i <= 3; ++i) {
    ASSERT_TRUE(sender.AsyncWrite({Bytes({i}), to})) << "第 " << int(i) << " 条";
  }
  boost::this_fiber::sleep_for(50ms);
  EXPECT_FALSE(ReadOne(receiver, 100));  // 确实一条都没发出。

  // 释放端口 → sender bind 成功 → 写泵被 socket_ready 唤醒,按序灌出全部积压。
  ASSERT_TRUE(holder.Close());
  holder.WaitClosed();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return sender.CurrentLinkState() == LinkState::kUp; }, 3000));

  for (std::uint8_t i = 1; i <= 3; ++i) {
    auto got = ReadOne(receiver);
    ASSERT_TRUE(got) << "第 " << int(i) << " 条:" << got.error().message();
    EXPECT_EQ(got.value().bytes, Bytes({i}));  // 按序。
  }
}

// AC:超过 silence_timeout 无任何报文 → 泵判链路已坏 → 解绑重建。固定端口才能断言
// "重建后仍是同一个绑定"。
TEST(CoroUdpTransport, SilenceTimeoutRebuildsSocketOnSamePort) {
  const std::uint16_t port = GrabFreePort();
  UdpConfig config = PortConfig(port);
  config.silence_timeout = 150ms;
  UdpTransport t(config);
  ASSERT_TRUE(t.Start());
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kUp);
  ASSERT_EQ(t.LastError(), std::error_code{});

  // 一条报文都不发 → 内层 await_for 超时 → 归因 kTimeout,回外层重建。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.LastError() == make_error_code(TransportErrc::kTimeout); },
      2000))
      << "静默超时未触发:LastError=" << t.LastError().message();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return t.CurrentLinkState() == LinkState::kUp; }, 2000));
  EXPECT_EQ(t.LocalPort(), port);

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(判别力):有数据持续流动时**不得**判静默超时——每条报文都重置读等待。
// 8 轮 × 80ms = 640ms,远超 300ms 阈值,但每轮都有报文,故一次都不该超时。
TEST(CoroUdpTransport, SilenceTimeoutDoesNotFireWhileDataKeepsArriving) {
  UdpConfig config = LoopbackConfig();
  config.silence_timeout = 300ms;
  UdpTransport t(config);
  ASSERT_TRUE(t.Start());
  const std::uint16_t port = t.LocalPort();

  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(sender.Start());
  const Endpoint to = Endpoint::Net(kLoopback, port);
  for (std::uint8_t i = 1; i <= 8; ++i) {
    ASSERT_TRUE(sender.AsyncWrite({Bytes({i}), to}));
    auto got = ReadOne(t);
    ASSERT_TRUE(got) << "第 " << int(i) << " 条:" << got.error().message();
    EXPECT_EQ(got.value().bytes, Bytes({i}));
    boost::this_fiber::sleep_for(80ms);
  }
  EXPECT_EQ(t.LastError(), std::error_code{}) << "有数据流动时不应判静默超时";
  // 临时端口一旦重建就会换号,故它没变即"从未重建"。
  EXPECT_EQ(t.LocalPort(), port);
}

// —— 4. 生命周期 ————————————————————————————————————————————————————

// AC(ADR-0007 D4):`Close()` 关 read_queue 并携带终止原因,在途的读随即得到 kClosed;
// `WaitClosed()` join 泵后返回,此时可安全析构。
TEST(CoroUdpTransport, CloseWakesPendingReadAndWaitClosedReturns) {
  UdpTransport t(LoopbackConfig());
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();

  ASSERT_TRUE(t.Close());
  auto got = testutil::AwaitRead(rx, 1000ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));

  t.WaitClosed();
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  // 关后再读仍是终止原因,不是超时。
  auto again = testutil::AwaitRead(t.AsyncRead(), 100ms);
  ASSERT_FALSE(again);
  EXPECT_EQ(again.error(), make_error_code(TransportErrc::kClosed));
}

// AC:`Close()` 幂等、`WaitClosed()` 幂等(`FiberTask::get()` 是一次性的,第二次调用
// 必须立即返回而不是挂死或拿到假结果)。
TEST(CoroUdpTransport, CloseAndWaitClosedAreIdempotent) {
  UdpTransport t(LoopbackConfig());
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(t.Close());
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  t.WaitClosed();  // 不得挂死。
  EXPECT_EQ(t.AsyncWrite({Bytes({1}), Endpoint::Net(kLoopback, 9)}).error(),
            make_error_code(TransportErrc::kClosed));
}

// AC:从未 Start 就 Close/WaitClosed —— 无泵可停,立即返回。
TEST(CoroUdpTransport, CloseWithoutStartConverges) {
  UdpTransport t(LoopbackConfig());
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC【四处打断之一:泵停在 **bind 退避**中】退避由 close_signal 承载,Close 关它即提前
// 唤醒——收敛必须**远快于**重试间隔,否则外层循环会挂到退避到期。
TEST(CoroUdpTransport, CloseDuringBindBackoffConvergesPromptly) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());

  UdpConfig config = PortConfig(holder.LocalPort());
  config.silence_timeout = 3000ms;  // 退避 3s:若不打断,收敛就要等这么久。
  UdpTransport t(config);
  ASSERT_TRUE(t.Start());
  boost::this_fiber::sleep_for(50ms);  // 让泵首轮重试失败后停进退避。
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kDown);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(std::chrono::steady_clock::now() - began, 1000ms)
      << "Close 未打断 bind 退避";

  ASSERT_TRUE(holder.Close());
  holder.WaitClosed();
}

// AC【四处打断之二:写泵停在**等数据**】Close 关 write_queue 唤醒它。
TEST(CoroUdpTransport, CloseWhileWritePumpWaitsForDataConverges) {
  UdpTransport t(LoopbackConfig());
  ASSERT_TRUE(t.Start());
  boost::this_fiber::sleep_for(50ms);  // 写泵停在"等数据"。

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(std::chrono::steady_clock::now() - began, 1000ms);
}

// AC【四处打断之三:写泵停在**等 socket 就绪**】链路不可用时它取出了一条却发不出去,
// 停在第二个阻塞点;Close 关 socket_ready 唤醒它。
TEST(CoroUdpTransport, CloseWhileWritePumpWaitsForSocketReadyConverges) {
  UdpTransport holder(LoopbackConfig());
  ASSERT_TRUE(holder.Start());

  UdpConfig config = PortConfig(holder.LocalPort());  // bind 失败 → 链路不可用。
  config.silence_timeout = 3000ms;
  UdpTransport sender(config);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(sender.AsyncWrite({Bytes({0xFF}), Endpoint::Net(kLoopback, 9)}));
  boost::this_fiber::sleep_for(50ms);  // 让写泵取出该条并停在阻塞点②。
  ASSERT_EQ(sender.CurrentLinkState(), LinkState::kDown);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(sender.Close());
  sender.WaitClosed();
  EXPECT_LT(std::chrono::steady_clock::now() - began, 1000ms);

  ASSERT_TRUE(holder.Close());
  holder.WaitClosed();
}

// AC(ADR-0007 D4):读句柄不设单读守卫——多个消费者直接 await 同一句柄是**抢占**关系
// (socket 的读本就是抢占式的);要扇出由调用方自己 `shared()`。
TEST(CoroUdpTransport, ConcurrentReadersPreemptInsteadOfBeingRejected) {
  UdpTransport receiver(LoopbackConfig());
  UdpTransport sender(LoopbackConfig());
  ASSERT_TRUE(receiver.Start());
  ASSERT_TRUE(sender.Start());

  auto rx = receiver.AsyncRead();
  int delivered = 0;
  auto reader = [&] {
    if (testutil::AwaitRead(rx, 2000ms)) ++delivered;
  };
  auto a = Coro::makeTask(reader);
  auto b = Coro::makeTask(reader);

  const Endpoint to = Endpoint::Net(kLoopback, receiver.LocalPort());
  ASSERT_TRUE(sender.AsyncWrite({Bytes({1}), to}));
  ASSERT_TRUE(sender.AsyncWrite({Bytes({2}), to}));
  (void)a.get();
  (void)b.get();
  EXPECT_EQ(delivered, 2) << "两条报文被两个读者各取一条(抢占,不重复投递)";
}
