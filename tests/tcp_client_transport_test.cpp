// 协程原生 TcpClientTransport 连接管理核心真实回环集成测试(ADR-0003 D11)。
// 在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证连接状态机、代际、退避重连、
// 观察面(IConnectionObservable)与 ITransport 委托。所有退避/超时/稳定阈值用小值注入
// 以确定化单条时长。连接超时→abort 路径用 TEST-NET(RFC5737)不响应端点 + 小 connect
// timeout 确定化;退避序列用 jitter 关闭 + 小 initial/cap 确定化。
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
using transport::OperationOptions;
using transport::SendUnit;
using transport::Status;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}

// 小值确定化配置:短连接超时、小退避、关抖动。
TcpClientConfig FastConfig(quint16 port) {
  TcpClientConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = port;
  cfg.connect_timeout = 400ms;
  cfg.initial_backoff = 30ms;
  cfg.max_backoff = 120ms;
  cfg.backoff_multiplier = 2.0;
  cfg.jitter_enabled = false;
  cfg.stable_reset_after = 10s;
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

}  // namespace

// 连上后 State==kConnected、Generation 递增;Read/Write 委托内层正常收发。
TEST(CoroTcpClientTransport, ConnectDelegatesReadWrite) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.State(), ConnectionState::kConnected);
  EXPECT_EQ(client.Generation(), 1u);

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return server.hasPendingConnections(); }, 2000));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);
  accepted->setParent(nullptr);

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
  std::vector<std::uint8_t> recv;
  while (recv.size() < static_cast<std::size_t>(reply.size())) {
    auto r = client.Read(Deadline(3000ms));
    ASSERT_TRUE(r) << r.error().message();
    recv.insert(recv.end(), r.value().bytes.begin(), r.value().bytes.end());
  }
  EXPECT_EQ(recv.size(), static_cast<std::size_t>(reply.size()));

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  accepted->deleteLater();
}

// 非 Connected 态 Write → kConnection(不缓存)。
TEST(CoroTcpClientTransport, WriteWhenNotConnectedIsConnection) {
  TcpClientTransport client(FastConfig(FreePort()));
  // 未 Start:kDisconnected。
  EXPECT_EQ(client.State(), ConnectionState::kDisconnected);
  auto r = client.Write(Frame(std::vector<std::uint8_t>(4, 0x1)));
  ASSERT_FALSE(r);
  EXPECT_EQ(r.error(), make_error_code(TransportErrc::kConnection));
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

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return server.hasPendingConnections(); }, 2000));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);
  accepted->setParent(nullptr);

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

// connect 超时 → abort → 分类可重试 → Reconnecting 退避重试(AttemptCount 递增)。
// 确定化手段:连接 100.64.0.1(RFC6598 CGNAT 保留段),本环境下其 TCP 握手静默悬挂,
// connect 在小 connect_timeout 内确定性超时(而 127.0.0.1 及多数保留段本环境会即时接受)。
TEST(CoroTcpClientTransport, ConnectTimeoutAbortsAndRetries) {
  TcpClientConfig cfg = FastConfig(9);
  cfg.host = "100.64.0.1";  // 握手悬挂 → await_for 超时 → 显式 abort + deleteLater。
  cfg.connect_timeout = 200ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());

  // 超时→abort→退避→再尝试:AttemptCount 跨过 ≥2 次尝试。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 2; }, 5000));
  EXPECT_NE(client.State(), ConnectionState::kConnected);
  // 失败分类为 kTimeout(超时路径,非快速拒绝)。
  EXPECT_EQ(client.LastFailure(), make_error_code(TransportErrc::kTimeout));

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 先无 server(连接被拒绝)→ 退避重试;server 上线后最终 Connected。
TEST(CoroTcpClientTransport, EventuallyConnectsWhenServerAppears) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  const quint16 port = server.serverPort();
  server.close();  // 先关闭 → 连接被拒绝。

  TcpClientTransport client(FastConfig(port));
  ASSERT_TRUE(client.Start());
  // 至少经历一次失败退避。
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

// 退避序列:jitter 关闭 + 小 initial/cap,相邻尝试间隔按 1×→2×→…→cap 递增。
TEST(CoroTcpClientTransport, BackoffSequenceDoublesToCap) {
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.initial_backoff = 60ms;
  cfg.max_backoff = 240ms;
  cfg.jitter_enabled = false;
  TcpClientTransport client(cfg);

  std::vector<Clock::time_point> attempt_times;
  std::size_t last = 0;
  ASSERT_TRUE(client.Start());
  // 采集前 5 次尝试的时刻(连接被拒绝近乎瞬时,间隔≈退避时长)。
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

  ASSERT_GE(attempt_times.size(), 4u);
  const auto d1 = attempt_times[1] - attempt_times[0];  // ≈ initial(60ms)
  const auto d2 = attempt_times[2] - attempt_times[1];  // ≈ 2×(120ms)
  const auto d3 = attempt_times[3] - attempt_times[2];  // ≈ 4×→cap(240ms)
  // 递增(容忍调度抖动):d2 明显大于 d1、d3 大于 d2 或触顶。
  EXPECT_GT(d2, d1 + 20ms);
  EXPECT_GT(d3, d2 + 20ms) << "d3 应继续增大直至触顶 cap";
  EXPECT_LT(d3, 240ms + 120ms);  // 未超过 cap 太多。
}

// jitter 启用(固定 seed 确定复现)时,首次退避落在 ±ratio 抖动带内。
TEST(CoroTcpClientTransport, JitterKeepsDelayWithinBand) {
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.initial_backoff = 120ms;
  cfg.max_backoff = 480ms;
  cfg.jitter_ratio = 0.2;
  cfg.jitter_enabled = true;
  cfg.jitter_seed = 12345;  // 确定复现。
  TcpClientTransport client(cfg);

  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 1; }, 2000));
  auto t_first = Clock::now();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 2; }, 3000));
  auto d1 = Clock::now() - t_first;
  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));

  // 抖动带 [96ms,144ms],加宽调度容差断言递增有界(不塌成 0、不超 initial 两倍)。
  EXPECT_GT(d1, 60ms);
  EXPECT_LT(d1, 240ms);
}

// RequestClose 停 loop、掐断当前尝试(退避中亦立即收敛)。
TEST(CoroTcpClientTransport, RequestCloseStopsLoopDuringBackoff) {
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.initial_backoff = 200ms;
  cfg.max_backoff = 200ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());
  // 进入退避(第一次失败后)。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.State() == ConnectionState::kReconnecting; }, 2000));

  auto t0 = Clock::now();
  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(1000ms)));
  EXPECT_LT(Clock::now() - t0, 150ms) << "应掐断退避等待、迅速收敛";
  EXPECT_EQ(client.State(), ConnectionState::kDisconnected);
}
