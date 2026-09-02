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
//      都须在**远小于 silence_timeout** 的时间内收敛;外加**「建完即复查」**
//      (**D15 补正**,#200):`Close()` 跑完之后泵**不得再建一个没人关的等待器**——
//      三处打断只关得到已存在的句柄,这一条管的是"未来才出现的那一个"。
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
// 写侧(#180)另有两组,见文件后半:数据面(回环发字节 / `peer` 被忽略(D8)/ 按序
// (RT_TRANSPORT_004)/ 链路不可用时入队即返(RT_TCP_RECONNECT_003))与写侧生命周期 +
// **另两处打断**(④⑤)+ **不等刷出**(D13)。
//
// 写侧两处打断同样做了负向对照(silence_timeout = 3s):
//
//   | 打断           | 五处齐备 | 去掉该处   |
//   |----------------|----------|-----------|
//   | ④ 写泵等数据   | 0 ms     | **永久挂死**(>30s 被 timeout 杀) |
//   | ⑤ 写泵等就绪   | 0 ms     | **永久挂死**(同上)              |
//
// **写侧比读侧更严**:读侧三处用的是 `await_for`,漏一处最坏挂满一个 silence_timeout;
// 写侧两处是**无限期** `await`(写没有"超时该干什么"的语义),漏一处就是**永久挂死**
// ——`WaitClosed()` join 管理泵,而管理泵收尾时要先 join 写泵。
//
// D13(不等刷出)也有负向对照:在 `write()` 后加 `while(bytesToWrite()>0)
// waitForBytesWritten(3000)`,`CloseDoesNotWaitForFlush` 由 **11 ms 涨到 165 460 ms**
// (4 MiB 灌给一个不读的对端)。**不等刷出**这一条是可观测的,不只是风格选择。
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
  // port 传 0 = 让内核挑一个;传具体端口用于"先连不上、后起服务"的重连用例。
  explicit LoopbackServer(std::uint16_t port = 0) {
    EXPECT_TRUE(server_.listen(QHostAddress::LocalHost, port));
  }

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

Datagram Unit(const char* text, Endpoint peer = Endpoint::Default()) {
  return Datagram{Bytes(text), std::move(peer)};
}

// 在服务端侧收满 want 字节(或超时)。`bytesAvailable()` 由事件循环推进,`pumpFiberUntil`
// 的 fiber sleep 既让出调度器又跑 Qt 事件,故不必 waitForReadyRead。
QByteArray RecvAtLeast(QTcpSocket* peer, int want, int budget_ms = 3000) {
  QByteArray got;
  pumpFiberUntil(
      [&] {
        got += peer->readAll();
        return got.size() >= want;
      },
      budget_ms);
  return got;
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
  auto written = t.AsyncWrite({});  // 未 Start:写侧同样报非法生命周期。
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kInvalidState));

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

// —— 7. 写侧数据面(#180)——————————————————————————————————————————————

// AC(RT_TRANSPORT_003):`AsyncWrite` 的字节原样出现在对端,一字不差。
TEST(CoroTcpTransport, WritesBytesToPeer) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  ASSERT_TRUE(t.AsyncWrite(Unit("hello-write")));
  EXPECT_EQ(RecvAtLeast(peer, 11), QByteArray("hello-write"));
  EXPECT_FALSE(t.LastError()) << "正常写出不该留下诊断事实";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(D8):`peer` **一律忽略**——填一个与配置完全不同的 `Endpoint::Net`,报文仍发往配置的
// 固定对端,且**不返 kInvalidArgument**。这正是"传输无关的调用方换传输即可运行"的落点;
// 与 `UdpTransport` 相反(它按 peer 解析目的地、解析不了就丢该条并记 LastError)。
TEST(CoroTcpTransport, IgnoresPeerAndAlwaysSendsToConfiguredEndpoint) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  // 三种 peer:默认 / 黑洞地址 / 一个本介质无此语义的 Topic —— 一视同仁。
  ASSERT_TRUE(t.AsyncWrite(Unit("a", Endpoint::Default())));
  ASSERT_TRUE(t.AsyncWrite(Unit("b", Endpoint::Net(kBlackhole, 9))));
  ASSERT_TRUE(t.AsyncWrite(Unit("c", Endpoint::Topic("no-such-medium"))));

  EXPECT_EQ(RecvAtLeast(peer, 3), QByteArray("abc"))
      << "peer 未被忽略:某一条没发到配置的固定对端";
  EXPECT_NE(t.LastError(), make_error_code(TransportErrc::kInvalidArgument))
      << "TCP 不得因 peer 非默认而判非法(D8)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(RT_TRANSPORT_004):单消费者写泵保证串行化——连发 N 条,对端收到的字节序列与发出
// 顺序**逐字一致**、两条不交错。
TEST(CoroTcpTransport, PreservesWriteOrder) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  QByteArray expected;
  for (int i = 0; i < 32; ++i) {
    const std::string unit = "<" + std::to_string(i) + ">";
    ASSERT_TRUE(t.AsyncWrite(Unit(unit.c_str())));
    expected += QByteArray::fromStdString(unit);
  }
  EXPECT_EQ(RecvAtLeast(peer, expected.size()), expected) << "写出乱序或字节交错";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(RT_TCP_RECONNECT_003):**链路不可用时 `AsyncWrite` 照常入队即返、返回成功**
// ——不阻塞、不返错;写泵停在阻塞点②等 `socket_ready_`,链路恢复后按序发出积压。
TEST(CoroTcpTransport, QueuesWhileLinkDownAndFlushesAfterReconnect) {
  const std::uint16_t port = GrabFreePort();  // 先无人监听:连必被 RST。
  TcpTransport t(ConfigFor(kLoopback, port, 200ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); }, 1000))
      << "未进入'连不上'的状态,后续断言无意义";
  ASSERT_NE(t.CurrentLinkState(), LinkState::kUp);

  // 链路不可用时写:立即返回成功(入队即返),不是 kClosed、也不阻塞。
  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.AsyncWrite(Unit("queued-1")));
  ASSERT_TRUE(t.AsyncWrite(Unit("queued-2")));
  EXPECT_LT(Since(began), 100ms) << "AsyncWrite 阻塞了(应入队即返)";

  // 现在把服务端起在同一端口:泵下一轮即连上,积压按序发出。
  LoopbackServer server(port);
  ASSERT_EQ(server.port(), port);
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr) << "泵未重连上";
  EXPECT_EQ(RecvAtLeast(peer, 16), QByteArray("queued-1queued-2"))
      << "链路恢复后积压未按序发出";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 8. 写侧生命周期与 `Close()` 的两处打断(#180)——————————————————————

// AC:未 `Start()` → kInvalidState;`Close()` 后 → kClosed(队列已终结)。
TEST(CoroTcpTransport, WriteRejectsOutsideRunning) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 1000ms));

  auto before = t.AsyncWrite(Unit("too-early"));
  ASSERT_FALSE(before);
  EXPECT_EQ(before.error(), make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(t.Start());
  ASSERT_NE(server.Accept(), nullptr);
  ASSERT_TRUE(t.AsyncWrite(Unit("ok")));  // Running:成功。

  ASSERT_TRUE(t.Close());
  auto closing = t.AsyncWrite(Unit("too-late"));  // Closing。
  ASSERT_FALSE(closing);
  EXPECT_EQ(closing.error(), make_error_code(TransportErrc::kClosed));

  t.WaitClosed();
  auto after = t.AsyncWrite(Unit("way-too-late"));  // Closed。
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
}

// 【打断之四:写泵停在**等数据**】连上后一字节不写,写泵钉在 `await(write_queue_)` 上
// ——那是**无限期**等待(不是 await_for),漏了 `write_queue_->close()` 就是**永久挂死**,
// 而不只是挂满一个 silence_timeout。AC:`Close()` 关写队列即刻唤醒。
TEST(CoroTcpTransport, CloseWhileWritePumpWaitsForDataConvergesPromptly) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));
  boost::this_fiber::sleep_for(50ms);  // 写泵已进到"等数据"里(队列始终为空)。

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断写泵的'等数据'";
}

// 【打断之五:写泵停在**等连接就绪**】连黑洞地址(SYN 无应答)且**有待发数据**:写泵取到
// 数据后判 socket 未连上,钉在 `await(socket_ready_)` 上——同样是无限期等待。
// AC:`socket_ready_->close()` 即刻唤醒(注意管理泵此刻钉在"等连上",靠打断 ② 收敛)。
TEST(CoroTcpTransport, CloseWhileWritePumpWaitsForLinkConvergesPromptly) {
  TcpTransport t(ConfigFor(kBlackhole, 9, 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(t.AsyncWrite(Unit("never-sent")));  // 入队即返,链路永不就绪。
  boost::this_fiber::sleep_for(100ms);            // 写泵已进到"等连接就绪"里。
  ASSERT_NE(t.CurrentLinkState(), LinkState::kUp);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断写泵的'等连接就绪'";
}

// 【D13 的行为证据:不等刷出】连发一大批后**立即** `Close()`。AC:`WaitClosed()` 迅速
// 返回——写泵不等 `bytesWritten`、不等 `bytesToWrite() == 0`,已交给 Qt 内部写缓冲而未
// 刷出的部分随 `abort()` 丢弃。**故意不断言"全部送达"**:丢弃是预期行为(D7/D13)。
TEST(CoroTcpTransport, CloseDoesNotWaitForFlush) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }));

  // 一大批(对端一字节不读),内核 socket 缓冲很快填满,余下的全压在 Qt 内部写缓冲里
  // ——那正是**有界的 write_queue_ 挡不住的那一段**(D13 明确接受的代价)。
  const std::string blob(64 * 1024, 'x');
  for (int i = 0; i < 64; ++i) {  // 4 MiB
    ASSERT_TRUE(t.AsyncWrite(Unit(blob.c_str())));
  }
  // 【关键】让出,好让写泵真的跑起来把这批交给 Qt——否则本用例只是在测"空队列上 Close"。
  boost::this_fiber::sleep_for(100ms);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 等了刷出(D13:写出后不得再 await)";
  // 未刷出的部分随 `abort()` 丢弃——**不断言"全部送达"**,丢弃是预期行为(D7/D13)。
}

// —— 9. 「建完即复查」:Close() 之后泵不得再建一个没人关的等待器(D15 补正,#200)——

// 【D15 补正的回归证据】`Close()` 只关得到**它跑的那一刻已经存在**的句柄。泵停在
// `await_for(connect_waiter_, 3s)` 上被"连上"唤醒(runnable)、却尚未被调度时,`Close()`
// 可能整个跑完——那一刻 `read_stream_` 还是 null,它关了个空;泵随后建出的读流**没有任何
// 人会唤醒**,挂满一个 `silence_timeout`,而读队列只在泵退出循环后才关。于是
// `CloseTerminatesReadQueueAndBlocksRestart` 偶发拿到 kTimeout 而非 kClosed(#200)。
//
// **该窗口只有几百微秒宽,靠单次用例撞不到**:整组 40 轮才命中 1 次(#200 实测),单独跑
// 那一条 40 轮 0 次——这正是它当初 57 轮没复现、连名字都丢了的原因。本用例改为**扫过**
// 它:把 `Close()` 的时刻按微秒推进,覆盖"连上"前后 0..3000us 的整段。实测修复前每轮
// 201 个采样点里**稳定**有 10~11 个挂满 3s(连跑三轮:10 / 11 / 10),修复后 0 个。
//
// **不与 `CloseDuringConnectWindowConvergesPromptly` 重复**:那一条钉的是"泵**正停在**
// 连接等待里"(黑洞地址,连接永不完成),证的是 `Close()` 关得到**已存在**的句柄;本条
// 钉的是"连接**刚刚完成**、泵尚未被调度",证的是泵不会再建一个**新的、关不到**的句柄。
//
// AC:每个采样点上,`Close()` 之后读句柄都在远小于 `silence_timeout` 的预算内拿到
// kClosed,且 `WaitClosed()` 当场收敛。
TEST(CoroTcpTransport, CloseInterruptsWaitersBornAfterItAcrossConnectHandoff) {
  constexpr int kSpanUs = 3000;  // 覆盖回环连上前后的整段(实测命中区 ~960..1200us)。
  constexpr int kStepUs = 15;    // 201 个采样点;全绿时总开销就是那些 sleep 之和(~0.3s)。
  for (int slack_us = 0; slack_us <= kSpanUs; slack_us += kStepUs) {
    LoopbackServer server;  // 不必 Accept():QTcpServer 自动收进 pending 队列即已连上。
    TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
    ASSERT_TRUE(t.Start());
    boost::this_fiber::sleep_for(std::chrono::microseconds(slack_us));

    auto rx = t.AsyncRead();
    const auto began = std::chrono::steady_clock::now();
    ASSERT_TRUE(t.Close());
    auto got = AwaitRead(rx, 1000ms);
    ASSERT_FALSE(got) << "slack=" << slack_us << "us";
    EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed))
        << "slack=" << slack_us << "us:Close() 之后泵又建了一个没人关的等待器";
    t.WaitClosed();
    EXPECT_LT(Since(began), 500ms) << "slack=" << slack_us << "us:收敛慢于一次让出";
  }
}
