// 协程原生 TcpClientTransport 连接管理核心真实回环集成测试(ADR-0004 D1/D6/D7、
// ADR-0005 D4)。在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证:连接状态机、
// 代际、**固定间隔**重连、连接诊断面(具体方法,非多态观察面)与 ITransport 契约——
// 尤以**断链完全透明**(Read 不返回任何断链错误、重连后继续交付)为核心。
// 所有超时/间隔用小值注入以确定化单条时长。连接超时→abort 路径用不响应端点 + 小
// connect timeout 确定化。
#include <chrono>
#include <cstdint>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/tcp/TcpClientTransport.hpp"

using namespace std::chrono_literals;
using transport::ConnectionState;
using transport::Datagram;
using transport::Endpoint;
using transport::LinkState;
using transport::OperationOptions;
using transport::Result;
using transport::SendUnit;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}

// 小值确定化配置:短连接超时、小重连间隔(固定,无退避)。
TcpClientConfig FastConfig(quint16 port) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 400ms;
  cfg.reconnect_interval = 60ms;
  return cfg;
}

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = Clock::now() + d;
  return o;
}

// 取一个当前空闲端口(listen 后立即关闭,后续连接将被拒绝)。
quint16 FreePort() {
  QTcpServer probe;
  probe.listen(QHostAddress::LocalHost, 0);
  const quint16 port = probe.serverPort();
  probe.close();
  return port;
}

// 接受下一个入站连接(pump 到就绪),交出所有权。
QTcpSocket* AcceptNext(QTcpServer& server, int budget_ms = 4000) {
  if (!testutil::pumpFiberUntil([&] { return server.hasPendingConnections(); },
                                budget_ms)) {
    return nullptr;
  }
  QTcpSocket* s = server.nextPendingConnection();
  if (s) {
    s->setParent(nullptr);
  }
  return s;
}

// 累计读满 want 字节(每次读一片,deadline 只界定单次等待)。
std::vector<std::uint8_t> ReadExactly(TcpClientTransport& client,
                                      std::size_t want,
                                      std::chrono::milliseconds budget) {
  std::vector<std::uint8_t> got;
  const auto end = Clock::now() + budget;
  while (got.size() < want && Clock::now() < end) {
    OperationOptions o;
    o.deadline = end;
    auto r = testutil::ReadOnce(client, o);
    if (!r) {
      break;
    }
    got.insert(got.end(), r.value().bytes.begin(), r.value().bytes.end());
  }
  return got;
}

}  // namespace

// 缺省重连间隔为 1 s(SRS §3.1.7.4 / ADR-0005 D4:固定间隔,非指数退避)。
TEST(CoroTcpClientTransport, DefaultReconnectIntervalIsOneSecond) {
  TcpClientConfig cfg;
  EXPECT_EQ(cfg.reconnect_interval, 1000ms);
  EXPECT_EQ(cfg.connect_timeout, 5000ms);
}

// 连上后 State==kConnected、Generation 递增;Read/Write 正常收发。
TEST(CoroTcpClientTransport, ConnectDelegatesReadWrite) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.State(), ConnectionState::kConnected);
  EXPECT_EQ(client.Generation(), 1u);

  QTcpSocket* accepted = AcceptNext(server, 2000);
  ASSERT_NE(accepted, nullptr);

  // outer.Write → 服务端读到。
  const std::vector<std::uint8_t> out(16, 0x5A);
  ASSERT_TRUE(client.Write(Frame(out)));
  auto in_stream = Coro::coro(accepted).readAll();
  std::vector<std::uint8_t> got;
  while (got.size() < out.size()) {
    auto chunk = Coro::await_for(in_stream, 3s);
    ASSERT_TRUE(chunk) << chunk.error().message();
    got.insert(got.end(), chunk.value().begin(), chunk.value().end());
  }
  EXPECT_EQ(got, out);

  // 服务端 write → outer.Read 读到。
  const QByteArray reply(16, 0x77);
  accepted->write(reply);
  const auto recv =
      ReadExactly(client, static_cast<std::size_t>(reply.size()), 3000ms);
  EXPECT_EQ(recv.size(), static_cast<std::size_t>(reply.size()));

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  accepted->deleteLater();
}

// 未 Start → kInvalidState;Running 但链路不可用(连不上/重连中)→ kConnection
// (不缓存等待重连,RT_TCP_RECONNECT_003);已关闭 → kClosed。
TEST(CoroTcpClientTransport, WriteWhenLinkUnavailable) {
  TcpClientTransport client(FastConfig(FreePort()));
  // 未 Start:非法生命周期。
  auto before = client.Write(Frame(std::vector<std::uint8_t>(4, 0x1)));
  ASSERT_FALSE(before);
  EXPECT_EQ(before.error(), make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(client.Start());
  // 端口无人监听:连接反复失败,链路不可用 → kConnection。
  auto during = client.Write(Frame(std::vector<std::uint8_t>(4, 0x1)));
  ASSERT_FALSE(during);
  EXPECT_EQ(during.error(), make_error_code(TransportErrc::kConnection));

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  auto after = client.Write(Frame(std::vector<std::uint8_t>(4, 0x1)));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
}

// —— 断链完全透明(ADR-0004 D1/D6,本票核心)——
// 断链期间 Read **不返回任何断链错误**(短 deadline 只得 kTimeout),重连后于新链路
// 继续交付;调用方全程无需感知链路中断。
TEST(CoroTcpClientTransport, ReadIsTransparentAcrossDisconnectAndReconnect) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);

  accepted1->write(QByteArray(8, 0x11));
  const auto first = ReadExactly(client, 8, 3000ms);
  ASSERT_EQ(first.size(), 8u);
  EXPECT_EQ(first[0], 0x11);

  // 对端物理断连:读取**不得**因此失败。
  accepted1->abort();
  accepted1->deleteLater();
  for (int i = 0; i < 3; ++i) {
    auto during = testutil::ReadOnce(client, Deadline(120ms));
    ASSERT_FALSE(during) << "断链期间不应有数据";
    EXPECT_EQ(during.error(), make_error_code(TransportErrc::kTimeout))
        << "断链只使读取挂起(kTimeout 由调用方 deadline 产生),不得暴露断链错误";
  }
  // 传输仍在运行(未终结),链路在重建中。
  EXPECT_NE(client.CurrentLinkState(), LinkState::kDown);

  // 重连后新链路继续交付(同一个 Read 契约,调用方无任何切换动作)。
  QTcpSocket* accepted2 = AcceptNext(server, 5000);
  ASSERT_NE(accepted2, nullptr);
  EXPECT_GE(client.Generation(), 2u);
  accepted2->write(QByteArray(8, 0x22));
  const auto second = ReadExactly(client, 8, 4000ms);
  ASSERT_EQ(second.size(), 8u);
  EXPECT_EQ(second[0], 0x22);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  accepted2->deleteLater();
}

// 断链前已进入对外通道、尚未被取走的字节保留并继续交付(ADR-0004 D6;其与新链路首
// 字节可能拼成错帧,由编解码器重同步处置——D4 已决定不做重置)。
TEST(CoroTcpClientTransport, BytesBufferedBeforeDisconnectAreStillDelivered) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  QTcpSocket* accepted1 = AcceptNext(server);
  ASSERT_NE(accepted1, nullptr);

  // 对端发数据后立即断连;调用方此刻尚未读。
  accepted1->write(QByteArray(12, 0x33));
  accepted1->flush();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.LastReceiveTime().has_value(); }, 3000));
  accepted1->abort();
  accepted1->deleteLater();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.Generation() >= 2u; }, 5000));

  // 断链后仍取得断链前的残留字节。
  const auto leftover = ReadExactly(client, 12, 2000ms);
  ASSERT_EQ(leftover.size(), 12u);
  EXPECT_EQ(leftover[0], 0x33);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 重连期 Write 立即返 kConnection(不缓存等待重连,RT_TCP_RECONNECT_003)。
TEST(CoroTcpClientTransport, WriteDuringReconnectIsConnection) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();

  TcpClientTransport client(FastConfig(port));
  ASSERT_TRUE(client.Start());
  QTcpSocket* accepted = AcceptNext(server);
  ASSERT_NE(accepted, nullptr);
  ASSERT_TRUE(client.Write(Frame(std::vector<std::uint8_t>(4, 0x9))));

  // 服务端下线并断连 → 客户端进入重连。
  server.close();
  accepted->abort();
  accepted->deleteLater();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.State() != ConnectionState::kConnected; }, 3000));

  auto w = client.Write(Frame(std::vector<std::uint8_t>(4, 0x9)));
  ASSERT_FALSE(w);
  EXPECT_EQ(w.error(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kEstablishing);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 关闭路径:RequestClose 使**在途** Read 返 kClosed,connect-loop 干净退出(join 到)。
TEST(CoroTcpClientTransport, RequestCloseWakesInFlightReadWithClosed) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  QTcpSocket* accepted = AcceptNext(server);
  ASSERT_NE(accepted, nullptr);

  Coro::Result<Datagram> out{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    out = testutil::ReadOnce(client);  // 无 deadline:唯有关闭能使其失败返回。
    done = true;
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);
  EXPECT_FALSE(done) << "无数据时应挂起,不得自行返回";

  client.RequestClose();
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return done; }, 3000));
  ASSERT_FALSE(out);
  EXPECT_EQ(out.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(reader.get());  // 读方 fiber 干净退出。
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));  // connect-loop 已收敛。
  EXPECT_EQ(client.CurrentLinkState(), LinkState::kDown);
  accepted->deleteLater();
}

// ADR-0007 D4:单读守卫已删除——并发第二个读者**不再被拒**(不返 kInvalidState),
// 与第一个读者同挂在对外 read_queue 上抢占式共读,无数据则按自己的 deadline 超时。
TEST(CoroTcpClientTransport, ConcurrentReadIsNotRejected) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  QTcpSocket* accepted = AcceptNext(server);
  ASSERT_NE(accepted, nullptr);

  bool done = false;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    (void)testutil::ReadOnce(client);
    done = true;
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);

  auto second = testutil::ReadOnce(client, Deadline(200ms));
  ASSERT_FALSE(second);
  EXPECT_NE(second.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kTimeout));

  client.RequestClose();
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return done; }, 3000));
  EXPECT_TRUE(reader.get());
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  accepted->deleteLater();
}

// WaitForState 目标已满足立即返;deadline 超时只结束等待,后台重连继续。
TEST(CoroTcpClientTransport, WaitForStateImmediateAndDeadlineDoesNotStopLoop) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));

  // 已满足即刻返回。
  auto t0 = Clock::now();
  EXPECT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_LT(Clock::now() - t0, 50ms);

  // 等待一个不会达成的目标(kDisconnected),短 deadline → 只结束本次等待。
  auto w = client.WaitForState(ConnectionState::kDisconnected, Deadline(80ms));
  ASSERT_FALSE(w);
  EXPECT_EQ(w.error(), make_error_code(TransportErrc::kTimeout));
  // 后台连接仍在:仍 Connected。
  EXPECT_EQ(client.State(), ConnectionState::kConnected);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 断连 → Reconnecting → 自动重连 → 新 Generation;WaitStateChange 观察到跃迁。
TEST(CoroTcpClientTransport, ReconnectAfterDisconnectBumpsGeneration) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.Generation(), 1u);

  QTcpSocket* accepted = AcceptNext(server, 2000);
  ASSERT_NE(accepted, nullptr);

  // 观察下一次跃迁(应为 Connected→Reconnecting)。
  bool change_ok = false;
  ConnectionState observed{};
  Coro::Awaitable<void> entered;
  auto watcher = Coro::makeTask([&] {
    entered.resolve();
    auto c = client.WaitStateChange(Deadline(3000ms));
    change_ok = static_cast<bool>(c);
    if (c) observed = c.value();
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(20ms);

  accepted->abort();  // 服务端断连。
  accepted->deleteLater();

  EXPECT_TRUE(watcher.get());
  EXPECT_TRUE(change_ok);
  EXPECT_NE(observed, ConnectionState::kConnected);

  // 自动重连:Generation 到 2、再次 Connected。
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_GE(client.Generation(), 2u);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// connect 超时 → abort → 分类可重试 → Reconnecting 后重试(AttemptCount 递增)。
// 确定化手段:连接 100.64.0.1(RFC6598 CGNAT 保留段),本环境下其 TCP 握手静默悬挂,
// connect 在小 connect_timeout 内确定性超时(而 127.0.0.1 及多数保留段本环境会即时接受)。
TEST(CoroTcpClientTransport, ConnectTimeoutAbortsAndRetries) {
  TcpClientConfig cfg = FastConfig(9);
  cfg.host = "100.64.0.1";  // 握手悬挂 → await_for 超时 → 显式 abort + deleteLater。
  cfg.connect_timeout = 200ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());

  // 超时→abort→固定间隔→再尝试:AttemptCount 跨过 ≥2 次尝试。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 2; }, 5000));
  EXPECT_NE(client.State(), ConnectionState::kConnected);
  // 失败分类为 kTimeout(超时路径,非快速拒绝)。
  EXPECT_EQ(client.LastFailure(), make_error_code(TransportErrc::kTimeout));

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 先无 server(连接被拒绝)→ 固定间隔重试;server 上线后最终 Connected。
TEST(CoroTcpClientTransport, EventuallyConnectsWhenServerAppears) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();
  server.close();  // 先关闭 → 连接被拒绝。

  TcpClientTransport client(FastConfig(port));
  ASSERT_TRUE(client.Start());
  // 至少经历一次失败后的间隔等待。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 2; }, 3000));
  EXPECT_NE(client.State(), ConnectionState::kConnected);

  // server 重新上线 → 最终连上。
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, port));
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(4000ms)));
  EXPECT_GE(client.Generation(), 1u);

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 重连节奏为**固定间隔**(ADR-0005 D4):相邻尝试间隔稳定在配置值附近,不随失败次数
// 增长(旧指数退避会呈 1×→2×→4× 上升)。连接被拒绝近乎瞬时,故间隔≈重连间隔。
TEST(CoroTcpClientTransport, ReconnectIntervalIsFixedAndDoesNotGrow) {
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.reconnect_interval = 120ms;
  TcpClientTransport client(cfg);

  std::vector<Clock::time_point> attempt_times;
  std::size_t last = 0;
  ASSERT_TRUE(client.Start());
  // 采集前 5 次尝试的时刻。
  const auto budget_end = Clock::now() + 5s;
  while (attempt_times.size() < 5 && Clock::now() < budget_end) {
    const std::size_t now_count = client.AttemptCount();
    if (now_count > last) {
      last = now_count;
      attempt_times.push_back(Clock::now());
    }
    boost::this_fiber::sleep_for(2ms);
  }
  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));

  ASSERT_GE(attempt_times.size(), 5u);
  std::vector<Clock::duration> intervals;
  for (std::size_t i = 1; i < attempt_times.size(); ++i) {
    intervals.push_back(attempt_times[i] - attempt_times[i - 1]);
  }
  for (const auto& d : intervals) {
    EXPECT_GT(d, 60ms) << "间隔不得塌陷(零间隔会退化为紧循环)";
    EXPECT_LT(d, 400ms) << "间隔应稳定在配置的 120ms 附近(容调度抖动)";
  }
  // 无指数增长:末间隔不显著大于首间隔(旧退避此处应为 ≈8×)。
  EXPECT_LT(intervals.back(), intervals.front() + 100ms)
      << "固定间隔重连不得随失败次数增长";
}

// RequestClose 停 loop、掐断当前尝试(重连间隔等待中亦立即收敛)。
TEST(CoroTcpClientTransport, RequestCloseStopsLoopDuringReconnectWait) {
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.reconnect_interval = 800ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());
  // 进入重连等待(第一次失败后)。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.State() == ConnectionState::kReconnecting; }, 2000));
  EXPECT_TRUE(client.NextAttemptTime().has_value());

  auto t0 = Clock::now();
  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(1000ms)));
  EXPECT_LT(Clock::now() - t0, 400ms) << "应掐断间隔等待、迅速收敛";
  EXPECT_EQ(client.State(), ConnectionState::kDisconnected);
}
