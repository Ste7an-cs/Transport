// -----------------------------------------------------------------------------
// tcp_transport_test.cpp — 协程原生 TcpTransport 读侧真实回环集成测试(ADR-0011)
//
// 在 fiber 调度器(coro_test_main)内用本机 TCP loopback 验证五组事实:
//
//   1. 配置校验(D14):host 空 / port 为 0 / silence_timeout 非正 → `Start()` 返
//      kConfiguration 且**停在 Created**(未起泵);
//   2. `Start()` **不等首连**:连不上的地址上它立即返回,链路报 kEstablishing;
//   3. 数据面:回环收到的字节切片原样入队,`peer` 为**固定对端**(D8);
//   4. 透明重连(①'):对端断开后泵自动重连,新连接上的数据**照常从同一个 `AsyncRead()`
//      句柄**取到——队列不随连接重建而更换;不自终(D2),连不上时无限重试;
//   5. `Close()` 的三处打断(**D15 的回归证据**):连接窗口 / 读等待 / 退避中关闭,
//      都须在**远小于 silence_timeout** 的时间内收敛。
//
// 第 5 组是本文件的核心,是 D15 的**回归证据**(原始探针已随 ADR 定稿删除)。本次实现
// 时逐条做过负向对照(去掉对应的一处打断后重跑,silence_timeout = 3s / 2s):
//
//   | 打断     | 三处齐备 | 去掉该处 |
//   |----------|----------|----------|
//   | ② 等连上 | 0 ms     | 2900 ms  |
//   | ③ 读等待 | 0 ms     | 2949 ms  |
//   | ① 退避   | 0 ms     | 1800 ms  |
//
// **一处不可省**。另附一条实测细节(与 D15 的表述有出入,已如实记录、未改文档):把
// `Close()` 里的 ②③ 换成 `socket_->abort()` 时,② 照样挂满 2900ms(与探针 1/2 一致),
// 但 ③ 却 0ms 收敛——因为此时 socket **已连上**,`abort()` 会发 disconnected/error,
// `readAll()` 流随之终止。D15 的"abort 唤不醒 readAll"成立的前提是 socket 仍在
// **ConnectingState**。结论不变(**②③ 仍须持句柄 close**),只是 ③ 的失效场景更窄。
//
// 写侧(`AsyncWrite`)本轮是返 kUnsupported 的占位,归 #180,故不在此测。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QByteArray>
#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/tcp/TcpConfig.hpp"
#include "transport/io/tcp/TcpTransport.hpp"

using namespace std::chrono_literals;
using testutil::AwaitRead;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::Endpoint;
using transport::LinkState;
using transport::TcpConfig;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";
// TEST-NET-1(RFC 5737):不可路由的黑洞地址,SYN 不会被应答——用来把泵**钉在连接窗口**里。
constexpr char kBlackhole[] = "192.0.2.1";

TcpConfig ConfigFor(const char* host, std::uint16_t port,
                    std::chrono::milliseconds silence) {
  TcpConfig config;
  config.host = host;
  config.port = port;
  config.silence_timeout = silence;
  return config;
}

// 借一个临时端口拿到号随即释放:此后连它必被内核立即 RST(ECONNREFUSED),用来把泵
// **钉在退避**里(连接窗口只有微秒级)。
std::uint16_t GrabFreePort() {
  QTcpServer probe;
  EXPECT_TRUE(probe.listen(QHostAddress::LocalHost, 0));
  const std::uint16_t port = static_cast<std::uint16_t>(probe.serverPort());
  probe.close();
  return port;
}

// 真实回环服务端:监听 + 取已接受连接(接受的 socket 归 server 所有,随其析构)。
class LoopbackServer {
 public:
  LoopbackServer() { EXPECT_TRUE(server_.listen(QHostAddress::LocalHost, 0)); }

  std::uint16_t port() const {
    return static_cast<std::uint16_t>(server_.serverPort());
  }

  QTcpSocket* Accept(int budget_ms = 3000) {
    if (!pumpFiberUntil([this] { return server_.hasPendingConnections(); },
                        budget_ms)) {
      return nullptr;
    }
    return server_.nextPendingConnection();
  }

 private:
  QTcpServer server_;
};

std::vector<std::uint8_t> Bytes(const char* text) {
  const std::string s(text);
  return std::vector<std::uint8_t>(s.begin(), s.end());
}

std::chrono::milliseconds Since(std::chrono::steady_clock::time_point began) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - began);
}

}  // namespace

// —— 1. 配置校验(D14)——————————————————————————————————————————————————

// AC:三项非法配置一律返 kConfiguration,且**停在 Created**——未起泵(`IsRunning()` 假)、
// 读句柄仍以 kInvalidState 关闭(该错误只在 Created 才给,Running 给活队列、Closed 给
// kClosed),故实例并未被这次失败推进到任何后续状态。
TEST(CoroTcpTransport, RejectsInvalidConfigAndStaysCreated) {
  const std::uint16_t port = GrabFreePort();
  const TcpConfig bad[] = {
      ConfigFor("", port, 1000ms),          // host 空。
      ConfigFor(kLoopback, 0, 1000ms),      // port 为 0。
      ConfigFor(kLoopback, port, 0ms),      // silence_timeout 为 0(无"禁用"这一档)。
      ConfigFor(kLoopback, port, -5ms),     // 负值同理。
  };
  for (const TcpConfig& config : bad) {
    TcpTransport t(config);
    auto started = t.Start();
    ASSERT_FALSE(started);
    EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
    EXPECT_FALSE(t.IsRunning());
    EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
    auto handle = t.AsyncRead();
    auto got = AwaitRead(handle, 0ms);
    ASSERT_FALSE(got);
    EXPECT_EQ(got.error(), make_error_code(TransportErrc::kInvalidState))
        << "配置校验失败后未停在 Created";
  }

  // 配置合法即可 Start——校验是配置的属性,不是实例的死刑。
  // (无配置热更新 API:`ApplyConfig` 待定,ADR-0011 D11,故"改配"以新实例表达。)
  TcpTransport good(ConfigFor(kLoopback, port, 1000ms));
  EXPECT_TRUE(good.Start());
  EXPECT_TRUE(good.IsRunning());
  EXPECT_TRUE(good.Close());
  good.WaitClosed();
}

// —— 2. `Start()` 不等首连 ————————————————————————————————————————————

// AC(SDD §5.6.1):TCP 的 connect 是异步的,`Start()` **不就地等**——否则它会变成一个
// 最长一个 silence_timeout 的阻塞调用。返回后链路报 kEstablishing(泵正在连/将要连)。
TEST(CoroTcpTransport, StartDoesNotWaitForFirstConnect) {
  TcpTransport t(ConfigFor(kBlackhole, 9, 3000ms));
  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Start());
  EXPECT_LT(Since(began), 500ms) << "Start() 等了首连";
  EXPECT_TRUE(t.IsRunning());
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kEstablishing);

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 3. 回环收字节 ——————————————————————————————————————————————————

// AC(RT_TRANSPORT_003 / D8):服务端发出的字节原样入队(切片,不是帧),`peer` 是
// **固定对端**——TCP 点对点,不从 socket 上取来源。
TEST(CoroTcpTransport, DeliversBytesWithFixedPeer) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());

  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  peer->write(QByteArray("hello-tcp"));
  peer->flush();

  auto got = testutil::ReadOnce(t, 2000ms);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().bytes, Bytes("hello-tcp"));
  EXPECT_EQ(got.value().peer.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(got.value().peer.host, std::string(kLoopback));
  EXPECT_EQ(got.value().peer.port, server.port());

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 4. 透明重连与不自终 ————————————————————————————————————————————

// AC(①' / D2):对端断开 → 泵自动重连 → 新连接上的数据**照常从同一个句柄**取到。
// 断链既不关队列、也不向调用方报错(DD-11):句柄在整个重连期间一直有效。
TEST(CoroTcpTransport, ReconnectsTransparentlyOnSameReadHandle) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());

  QTcpSocket* first = server.Accept();
  ASSERT_NE(first, nullptr);
  auto rx = t.AsyncRead();  // 【全程只取这一次句柄】

  first->write(QByteArray("gen-1"));
  first->flush();
  auto before = AwaitRead(rx, 2000ms);
  ASSERT_TRUE(before) << before.error().message();
  EXPECT_EQ(before.value().bytes, Bytes("gen-1"));

  // 服务端主动断开:泵经读流的自然终止发现,随即重连(连接立即成功,不进退避)。
  first->disconnectFromHost();
  first->close();

  QTcpSocket* second = server.Accept();
  ASSERT_NE(second, nullptr) << "泵未自动重连";
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  second->write(QByteArray("gen-2"));
  second->flush();
  auto after = AwaitRead(rx, 2000ms);  // 同一个句柄。
  ASSERT_TRUE(after) << after.error().message();
  EXPECT_EQ(after.value().bytes, Bytes("gen-2"));

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(ADR-0007 D2 / D14):连不上**不自终**——泵无限重试,传输仍 Running、读句柄仍活着,
// 底层成因只降为 `LastError()` 的诊断事实。
TEST(CoroTcpTransport, NeverSelfTerminatesWhilePeerIsUnreachable) {
  const std::uint16_t dead = GrabFreePort();
  TcpTransport t(ConfigFor(kLoopback, dead, 200ms));
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();

  boost::this_fiber::sleep_for(1000ms);  // 数个 silence_timeout(≈5 轮重试)。

  EXPECT_TRUE(t.IsRunning());
  EXPECT_TRUE(t.LastError()) << "连不上却没留下诊断事实";
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kEstablishing);
  auto got = AwaitRead(rx, 50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kTimeout))
      << "读队列被提前终结(应只有我方 Close 才终止)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 5. `Close()` 的三处打断(D15 的回归证据)————————————————————————————

// 【打断之一:泵停在**等连上**】连黑洞地址,SYN 无应答,泵钉在 `await_for(connect_waiter_,
// 3s)` 上。AC:`Close()` 关 `connect_waiter_` 即刻唤醒 —— 实测 `socket_->abort()` 在此
// 唤不醒,会挂满 3000ms。
TEST(CoroTcpTransport, CloseDuringConnectWindowConvergesPromptly) {
  TcpTransport t(ConfigFor(kBlackhole, 9, 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kEstablishing; }, 500));
  boost::this_fiber::sleep_for(100ms);  // 让泵真正进到 connect 等待里。
  ASSERT_FALSE(t.LastError())
      << "连接尚未失败才说明我们钉在连接窗口内(而非已进退避)";

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断'等连上'(abort() 唤不醒,须 close 句柄)";
}

// 【打断之二:泵停在**读等待**】连上后对端不发数据,泵钉在 `await_for(read_stream_, 3s)`。
// AC:`Close()` 关 `read_stream_` 即刻唤醒 —— 同样不能靠 abort()。
TEST(CoroTcpTransport, CloseDuringReadWaitConvergesPromptly) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));
  boost::this_fiber::sleep_for(50ms);  // 泵已建流并进入读等待(对端一字节未发)。

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断读等待";
}

// 【打断之三:泵停在**退避**】连一个无人监听的端口,内核立即 RST,连接窗口只有微秒级,
// 泵绝大部分时间钉在 `await_for(close_signal_, 2s)` 上。等到**第二轮退避**再关。
// AC:`close_signal_->close()` 即刻唤醒。
TEST(CoroTcpTransport, CloseDuringSecondBackoffConvergesPromptly) {
  const std::uint16_t dead = GrabFreePort();
  TcpTransport t(ConfigFor(kLoopback, dead, 2000ms));
  ASSERT_TRUE(t.Start());
  // 第一轮:连接立即失败 → 退避 2s;2.2s 时第二轮已失败并进入第二次退避。
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); }, 1000));
  boost::this_fiber::sleep_for(2200ms);
  ASSERT_TRUE(t.IsRunning());

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断重连退避";
}

// —— 6. 生命周期 ——————————————————————————————————————————————————

// AC(ADR-0007 D4):未 Start 时读句柄以 kInvalidState 关闭;Close/WaitClosed 幂等;
// 从未 Start 也能干净收敛。
TEST(CoroTcpTransport, LifecycleBeforeStartAndIdempotentClose) {
  TcpTransport t(ConfigFor(kLoopback, GrabFreePort(), 1000ms));
  auto handle = t.AsyncRead();
  auto got = AwaitRead(handle, 0ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_FALSE(t.AsyncWrite({}));  // 写侧本轮是占位(#180)。

  ASSERT_TRUE(t.Close());  // 从未 Start:无泵可停。
  ASSERT_TRUE(t.Close());  // 幂等。
  t.WaitClosed();
  t.WaitClosed();  // 不得挂死。
}

// AC:`Close()` 关 read_queue 并携带终止原因,在途的读随即得到 kClosed;`WaitClosed()`
// 返回后 Start 不再受理,且析构安全。
TEST(CoroTcpTransport, CloseTerminatesReadQueueAndBlocksRestart) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_NE(server.Accept(), nullptr);
  auto rx = t.AsyncRead();

  ASSERT_TRUE(t.Close());
  auto got = AwaitRead(rx, 1000ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));

  t.WaitClosed();
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  auto restarted = t.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
}
