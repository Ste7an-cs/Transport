// -----------------------------------------------------------------------------
// tcp_transport_reconnect_test.cpp — 重连的三条**节奏与归因**事实(#181 迁移件)
//
// 本文件由 `tcp_client_transport_test.cpp` 重写而来(`TcpClientTransport` /
// `TcpClientConfig` 已随 ADR-0011 **D1** 删除,三件收成一个 `TcpTransport`)。旧文件的
// 十三条用例中只有三条在新形态下仍然成立、且 `tcp_transport_test.cpp`(#179/#180)
// **未覆盖**,即本文件的三条;其余的处置逐条见 #181 的判定表,其中删除的多数是因为
// 它们断言的是已被删除的诊断面(`Generation()` / `AttemptCount()` / `LastFailure()` /
// `State()` / `WaitForState()`,**D9/D12**)或已被撤销的写侧语义(链路不可用即返
// `kConnection` → 现为入队等待,RT_TCP_RECONNECT_003)。
//
// 三条的分工:
//   1. **队列跨代际存活**——断链前已入队、尚未取走的字节在重连后照常取到。它与
//      `ReconnectsTransparentlyOnSameReadHandle`(那条验证的是"新连接的数据从同一句柄
//      取到")互补:本条管的是**旧连接的数据不因换连接而丢**。
//   2. **连接窗口超时的归因是 kTimeout**——`NeverSelfTerminatesWhilePeerIsUnreachable`
//      走的是"内核立即 RST"那条路(归因 kConnection),连接窗口挂满超时这条路
//      (**D5**:等连上用的就是 `silence_timeout`)此前无人覆盖。
//   3. **退避是固定间隔、不随失败次数增长**(ADR-0005 **D4**,ADR-0011 **D5** 沿用)。
//
// 【#132 与 #148 的处置】本文件不含任何"固定时长 sleep 后断言"的时间预算断言:等待
// 一律 `pumpFiberUntil` 到条件成立;仅有的两处 `sleep_for` 是**构造前提**(让泵跑过若干
// 轮失败),不是被断言的对象。`Accept()` 之后一律先等到 `kUp` 再操作传输——那正是 #132
// 的根因(服务端握手完成 ≠ 客户端泵已连上)。
// -----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QByteArray>
#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/tcp/TcpConfig.hpp"
#include "transport/io/tcp/TcpTransport.hpp"

using namespace std::chrono_literals;
using testutil::AwaitRead;
using testutil::pumpFiberUntil;
using transport::LinkState;
using transport::TcpConfig;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

constexpr char kLoopback[] = "127.0.0.1";
// TEST-NET-1(RFC 5737):不可路由的黑洞地址,SYN 不会被应答 —— 把泵钉在连接窗口里。
constexpr char kBlackhole[] = "192.0.2.1";

TcpConfig ConfigFor(const char* host, std::uint16_t port,
                    std::chrono::milliseconds silence) {
  TcpConfig config;
  config.host = host;
  config.port = port;
  config.silence_timeout = silence;
  return config;
}

// 借一个临时端口拿到号随即释放:此后连它必被内核立即 RST(连接窗口只有微秒级)。
std::uint16_t GrabFreePort() {
  QTcpServer probe;
  EXPECT_TRUE(probe.listen(QHostAddress::LocalHost, 0));
  const std::uint16_t port = static_cast<std::uint16_t>(probe.serverPort());
  probe.close();
  return port;
}

class LoopbackServer {
 public:
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

// 见文件头【#132】:握手在服务端侧完成不等于客户端泵已连上。
bool WaitLinkUp(const TcpTransport& t, int budget_ms = 3000) {
  return pumpFiberUntil([&t] { return t.CurrentLinkState() == LinkState::kUp; },
                        budget_ms);
}

std::chrono::milliseconds Since(std::chrono::steady_clock::time_point began) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - began);
}

}  // namespace

// AC(ADR-0011 **D2**:两条队列**不随连接重建而更换**):断链前已投进 read_queue、调用方
// 尚未取走的字节,在泵重连之后照常从同一个句柄取到——重连既不清队列,也不丢残留。
//
// 确定化手段:对端用 `disconnectFromHost()` **优雅**关闭而非 `abort()`。`corosocket` 的
// `readAll()` 在关流前先 `drain()`,故"先写后 FIN"这一序保证那 12 个字节必已入队
// (见 `TcpTransport.hpp` 的 D4 注释"尾字节不丢")——不需要靠 sleep 去赌传播完成。
TEST(CoroTcpTransportReconnect, BytesQueuedBeforeDisconnectSurviveReconnect) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(kLoopback, server.port(), 3000ms));
  ASSERT_TRUE(t.Start());

  QTcpSocket* first = server.Accept();
  ASSERT_NE(first, nullptr);
  ASSERT_TRUE(WaitLinkUp(t));
  auto rx = t.AsyncRead();  // 【全程只取这一次句柄】

  // 先发数据、随即优雅断开;调用方此刻**尚未读**。
  first->write(QByteArray("pre-disconnect"));
  first->flush();
  first->disconnectFromHost();

  // 等泵重连上(它经读流的自然终止发现断链,立即转下一圈;不进退避)。
  QTcpSocket* second = server.Accept();
  ASSERT_NE(second, nullptr) << "泵未自动重连";
  ASSERT_TRUE(WaitLinkUp(t));

  // 断链前的残留仍在队列里,且排在新连接的数据之前。
  auto leftover = AwaitRead(rx, 2000ms);
  ASSERT_TRUE(leftover) << leftover.error().message();
  EXPECT_EQ(leftover.value().bytes, Bytes("pre-disconnect"))
      << "重连丢掉了断链前已入队的字节(队列不该随连接重建而更换)";

  second->write(QByteArray("post-reconnect"));
  second->flush();
  auto fresh = AwaitRead(rx, 2000ms);
  ASSERT_TRUE(fresh) << fresh.error().message();
  EXPECT_EQ(fresh.value().bytes, Bytes("post-reconnect"));

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(**D5** + ADR-0007 D2):连接窗口挂满 `silence_timeout` 后,泵**不自终**、回外层继续
// 重试,底层成因降为 `LastError()` 的诊断事实且归因为 **kTimeout**——区别于"内核立即
// RST"那条路(归因 kConnection,见 `NeverSelfTerminatesWhilePeerIsUnreachable`)。
// 这两条归因的区分正是"等连上用的就是那一个量"的可观测证据。
TEST(CoroTcpTransportReconnect, ConnectWindowTimeoutIsAttributedAndKeepsRetrying) {
  TcpTransport t(ConfigFor(kBlackhole, 9, 300ms));
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();

  // 黑洞地址的 SYN 无应答 → 第一轮必然挂满 300ms 的连接窗口后超时。
  ASSERT_TRUE(pumpFiberUntil(
      [&t] {
        return t.LastError() == make_error_code(TransportErrc::kTimeout);
      },
      5000))
      << "连接窗口超时未归因为 kTimeout(实得:" << t.LastError().message() << ")";

  // 不自终:传输仍 Running、链路报"正在建立"、读句柄仍活着。
  EXPECT_TRUE(t.IsRunning());
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kEstablishing);
  auto got = AwaitRead(rx, 50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kTimeout))
      << "读队列被提前终结(应只有我方 Close 才终止)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(ADR-0005 **D4**,ADR-0011 **D5** 沿用):重连退避是**固定间隔**,不随失败次数增长。
//
// 新形态下 `AttemptCount()` 已随 **D9** 删除,重试节奏不再能直接计数,故改为**事件驱动
// 的判别式观测**:先让泵跑过约八轮失败(连一个无人监听的端口,内核立即 RST,故每轮
// ≈ 一个退避间隔),然后把服务端起在同一端口,量"从服务端上线到被接受"的时长。
//   · 固定间隔:泵至多再等一个未走完的退避 → ≈ ≤ 200ms;
//   · 指数退避:第九轮的间隔已是 200ms × 2⁸ ≈ 51 s → 必然超出预算。
// 二者相差两个数量级,故 1000ms 的判据既有区分度、又远离调度抖动。
TEST(CoroTcpTransportReconnect, BackoffIsFixedAndDoesNotGrowWithFailures) {
  constexpr auto kBackoff = 200ms;
  const std::uint16_t port = GrabFreePort();  // 先无人监听:连必被 RST。

  TcpTransport t(ConfigFor(kLoopback, port, kBackoff));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); }, 2000))
      << "未进入'连不上'的状态,后续断言无意义";

  // 【构造前提,不是被断言的量】跑过约八轮失败——指数退避在此已涨到分钟量级。
  boost::this_fiber::sleep_for(8 * kBackoff);
  ASSERT_TRUE(t.IsRunning());

  LoopbackServer server(port);
  ASSERT_EQ(server.port(), port);
  const auto began = std::chrono::steady_clock::now();
  QTcpSocket* peer = server.Accept(3000);
  ASSERT_NE(peer, nullptr) << "服务端上线后泵未在预算内连上";
  const auto waited = Since(began);
  EXPECT_LT(waited, 1000ms)
      << "重连间隔随失败次数增长了(固定间隔应 ≤ 一个退避,实得 " << waited.count()
      << " ms)";
  EXPECT_TRUE(WaitLinkUp(t));

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}
