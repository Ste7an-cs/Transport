// 协程原生 TcpClientTransport 运行时重配置(ApplyConfig)集成测试(ADR-0003 D11 Q5,
// RT_TCP_RECONFIG)。在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证:单调版本、
// 先校验后原子应用、端点变化(切新代际 + 立即尝试新端点)vs 仅策略变化(不打断当前尝试
// /退避、下次用新参数)、配置版本与连接代际两轴独立递增。退避/超时/稳定阈值用小值注入
// 以确定化单条时长。
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "coro_test_util.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/TcpClientTransport.hpp"

using namespace std::chrono_literals;
using transport::ConnectionState;
using transport::OperationOptions;
using transport::Status;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

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

// 过期/乱序版本 → 拒绝(kInvalidArgument),当前配置/版本不变;同版同容 → 成功 no-op。
TEST(CoroTcpClientReconfig, StaleAndSameVersionSemantics) {
  TcpClientTransport client(FastConfig(FreePort()));
  EXPECT_EQ(client.ConfigVersion(), 1u);
  EXPECT_EQ(client.ConfigChangeCount(), 0u);

  // 过期版本(< 当前 1)→ 拒绝,版本不变。
  TcpClientConfig older = FastConfig(FreePort());
  auto r_old = client.ApplyConfig(older, 0);
  ASSERT_FALSE(r_old);
  EXPECT_EQ(r_old.error(), make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(client.ConfigVersion(), 1u);
  EXPECT_EQ(client.ConfigChangeCount(), 0u);

  // 同版(== 当前 1)但内容不同 → 乱序拒绝,版本不变。
  TcpClientConfig conflict = FastConfig(FreePort());
  conflict.connect_timeout = 999ms;
  auto r_conf = client.ApplyConfig(conflict, 1);
  ASSERT_FALSE(r_conf);
  EXPECT_EQ(r_conf.error(), make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(client.ConfigVersion(), 1u);

  // 先应用一个合法新版本 2(内容变化)使 ChangeCount=1。
  TcpClientConfig v2 = FastConfig(FreePort());
  v2.connect_timeout = 350ms;
  ASSERT_TRUE(client.ApplyConfig(v2, 2));
  EXPECT_EQ(client.ConfigVersion(), 2u);
  EXPECT_EQ(client.ConfigChangeCount(), 1u);

  // 同版同容(== 当前 2,内容相同)→ 成功 no-op,不计变更。
  auto r_noop = client.ApplyConfig(v2, 2);
  ASSERT_TRUE(r_noop);
  EXPECT_EQ(client.ConfigVersion(), 2u);
  EXPECT_EQ(client.ConfigChangeCount(), 1u);
}

// 非法字段(端口 0 / 超时越界 / host 空)→ kConfiguration,旧配置旧版本不变;失败原因
// 与版本过期(kInvalidArgument)、生命周期非法(kInvalidState)可区分。
TEST(CoroTcpClientReconfig, InvalidFieldsRejectedAndDistinct) {
  TcpClientTransport client(FastConfig(FreePort()));

  // 端口 0。
  TcpClientConfig bad_port = FastConfig(0);
  auto r1 = client.ApplyConfig(bad_port, 5);
  ASSERT_FALSE(r1);
  EXPECT_EQ(r1.error(), make_error_code(TransportErrc::kConfiguration));

  // 超时越界(< 100ms)。
  TcpClientConfig bad_timeout = FastConfig(FreePort());
  bad_timeout.connect_timeout = 10ms;
  auto r2 = client.ApplyConfig(bad_timeout, 5);
  ASSERT_FALSE(r2);
  EXPECT_EQ(r2.error(), make_error_code(TransportErrc::kConfiguration));

  // 超时越界(> 60s)。
  TcpClientConfig big_timeout = FastConfig(FreePort());
  big_timeout.connect_timeout = 61s;
  auto r3 = client.ApplyConfig(big_timeout, 5);
  ASSERT_FALSE(r3);
  EXPECT_EQ(r3.error(), make_error_code(TransportErrc::kConfiguration));

  // host 空。
  TcpClientConfig empty_host = FastConfig(FreePort());
  empty_host.host = "";
  auto r4 = client.ApplyConfig(empty_host, 5);
  ASSERT_FALSE(r4);
  EXPECT_EQ(r4.error(), make_error_code(TransportErrc::kConfiguration));

  // 全部失败:配置版本不变(旧配置不变)。
  EXPECT_EQ(client.ConfigVersion(), 1u);
  EXPECT_EQ(client.ConfigChangeCount(), 0u);

  // 参数非法(kConfiguration)≠ 版本过期(kInvalidArgument)。
  auto r_stale = client.ApplyConfig(FastConfig(FreePort()), 0);
  ASSERT_FALSE(r_stale);
  EXPECT_NE(r1.error(), r_stale.error());
  EXPECT_EQ(r_stale.error(), make_error_code(TransportErrc::kInvalidArgument));
}

// 生命周期非法:关闭后 ApplyConfig → kInvalidState(与参数非法/版本过期可区分)。
TEST(CoroTcpClientReconfig, ApplyAfterCloseIsInvalidState) {
  TcpClientTransport client(FastConfig(FreePort()));
  ASSERT_TRUE(client.Start());
  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));

  auto r = client.ApplyConfig(FastConfig(FreePort()), 9);
  ASSERT_FALSE(r);
  EXPECT_EQ(r.error(), make_error_code(TransportErrc::kInvalidState));
}

// 端点变化 → 切新代际 + 立即尝试新端点(连到新 server);Generation 与 ConfigVersion
// 各自递增。旧连接被关闭(断连触发)。
TEST(CoroTcpClientReconfig, EndpointChangeSwitchesGenerationAndConnectsNewServer) {
  QTcpServer server_a;
  ASSERT_TRUE(server_a.listen(QHostAddress::LocalHost, 0));
  QTcpServer server_b;
  ASSERT_TRUE(server_b.listen(QHostAddress::LocalHost, 0));

  TcpClientTransport client(FastConfig(server_a.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.Generation(), 1u);
  EXPECT_EQ(client.ConfigVersion(), 1u);

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return server_a.hasPendingConnections(); }, 2000));
  QTcpSocket* accepted_a = server_a.nextPendingConnection();
  ASSERT_NE(accepted_a, nullptr);
  accepted_a->setParent(nullptr);

  // 端点切到 server_b(新版本 2)。
  TcpClientConfig to_b = FastConfig(server_b.serverPort());
  ASSERT_TRUE(client.ApplyConfig(to_b, 2));
  EXPECT_EQ(client.ConfigVersion(), 2u);
  EXPECT_EQ(client.ConfigChangeCount(), 1u);

  // 旧连接被关闭(server_a 侧读到断连)。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] {
        return accepted_a->state() == QAbstractSocket::UnconnectedState;
      },
      3000));

  // 立即尝试新端点 → 连到 server_b、代际递增到 2。
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(4000ms)));
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return server_b.hasPendingConnections(); }, 3000));
  QTcpSocket* accepted_b = server_b.nextPendingConnection();
  ASSERT_NE(accepted_b, nullptr);
  accepted_b->setParent(nullptr);

  EXPECT_GE(client.Generation(), 2u);      // 连接代际递增。
  EXPECT_EQ(client.ConfigVersion(), 2u);   // 配置版本独立递增。

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
  accepted_a->deleteLater();
  accepted_b->deleteLater();
}

// 端点变化在退避期发生 → 立即以新端点重试,不等剩余退避;连到新 server。
TEST(CoroTcpClientReconfig, EndpointChangeDuringBackoffRetriesImmediately) {
  // 起始端点为被拒绝端口(进入退避),大退避确保停在退避里。
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.initial_backoff = 1500ms;
  cfg.max_backoff = 1500ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.State() == ConnectionState::kReconnecting; }, 3000));

  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  TcpClientConfig to_server = FastConfig(server.serverPort());
  to_server.initial_backoff = 1500ms;
  to_server.max_backoff = 1500ms;

  auto t0 = Clock::now();
  ASSERT_TRUE(client.ApplyConfig(to_server, 2));
  // 立即尝试新端点(不等 1500ms 退避)→ 迅速连上。
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(1000ms)));
  EXPECT_LT(Clock::now() - t0, 1000ms) << "端点变化应立即重试,不等剩余退避";

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 仅退避/超时变化(端点不变)→ 不打断当前退避;下次连接动作用新退避参数(可从退避序列
// 变化观察)。同时 ConfigVersion 递增而 Generation 不变(两轴独立)。
TEST(CoroTcpClientReconfig, PolicyOnlyChangeAppliesToNextActionWithoutInterrupt) {
  // 起始:小退避(30ms)且端点被拒绝,持续退避重试。
  TcpClientConfig cfg = FastConfig(FreePort());
  cfg.initial_backoff = 40ms;
  cfg.max_backoff = 40ms;
  TcpClientTransport client(cfg);
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.AttemptCount() >= 2; }, 3000));
  const auto gen_before = client.Generation();

  // 仅策略变化:同端点(host/port 不变),放大退避到 300ms;新版本 2。
  TcpClientConfig slower = cfg;
  slower.initial_backoff = 300ms;
  slower.max_backoff = 300ms;
  ASSERT_TRUE(client.ApplyConfig(slower, 2));
  EXPECT_EQ(client.ConfigVersion(), 2u);
  EXPECT_EQ(client.ConfigChangeCount(), 1u);
  // 仅策略变化不切代际:Generation 不因 ApplyConfig 改变。
  EXPECT_EQ(client.Generation(), gen_before);

  // 应用后连续采样若干相邻尝试间隔:退避账本保留(不打断),但新的 max_backoff(300ms)
  // 令后续退避可攀升越过旧上限(40ms)——旧参数下任何间隔都不可能 > 100ms。
  std::size_t last = client.AttemptCount();
  Clock::time_point prev = Clock::now();
  Clock::duration max_interval{};
  const auto budget_end = Clock::now() + 3s;
  std::size_t sampled = 0;
  while (sampled < 5 && Clock::now() < budget_end) {
    const std::size_t now_count = client.AttemptCount();
    if (now_count > last) {
      const auto now = Clock::now();
      max_interval = std::max(max_interval, now - prev);
      prev = now;
      last = now_count;
      ++sampled;
    }
    boost::this_fiber::sleep_for(5ms);
  }
  EXPECT_GT(max_interval, 100ms)
      << "下次连接动作应使用新的(更大)退避参数,间隔应越过旧上限 40ms";

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}

// 配置版本与连接代际独立递增:一次策略重配置(仅 ConfigVersion++),一次断连重连
// (仅 Generation++),二者互不牵连。
TEST(CoroTcpClientReconfig, ConfigVersionAndGenerationEvolveIndependently) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  TcpClientTransport client(FastConfig(server.serverPort()));
  ASSERT_TRUE(client.Start());
  ASSERT_TRUE(client.WaitForState(ConnectionState::kConnected, Deadline(3000ms)));
  EXPECT_EQ(client.Generation(), 1u);
  EXPECT_EQ(client.ConfigVersion(), 1u);

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return server.hasPendingConnections(); }, 2000));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);
  accepted->setParent(nullptr);

  // 仅策略重配置(端点不变):ConfigVersion 1→2,Generation 不变。
  TcpClientConfig policy = FastConfig(server.serverPort());
  policy.connect_timeout = 350ms;
  ASSERT_TRUE(client.ApplyConfig(policy, 2));
  EXPECT_EQ(client.ConfigVersion(), 2u);
  EXPECT_EQ(client.Generation(), 1u);

  // 断连触发一次自动重连(端点不变):Generation 1→≥2,ConfigVersion 仍 2。
  accepted->abort();
  accepted->deleteLater();
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return client.Generation() >= 2u; }, 4000));
  EXPECT_EQ(client.ConfigVersion(), 2u) << "断连重连不改变配置版本(两轴独立)";

  client.RequestClose();
  EXPECT_TRUE(client.WaitClosed(Deadline(2000ms)));
}
