// -----------------------------------------------------------------------------
// tcp_transport_read_test.cpp — 读句柄的两条**调用方侧**契约(#181 迁移件)
//
// 本文件是旧同名文件按 ADR-0011 的新接口面重写后的残余。旧文件建在
// `TcpTransport(QTcpSocket*)`(已连接的裸管道)+ `OperationOptions` / `RequestClose()`
// 之上,其五条用例只有两条在新形态下仍然成立、且 `tcp_transport_test.cpp`(#179/#180)
// **未覆盖**,即本文件的两条;另三条的处置见 #181 的判定表:
//
//   · `SourceIsPeerEndpoint`  → 已被 `CoroTcpTransport.DeliversBytesWithFixedPeer` 取代
//     (且 peer 语义已由 **D8** 改为固定对端,不再取自 socket 的 peerAddress);
//   · `PeerDisconnectYieldsClosed` → **行为已撤销**(**D1/D2**:断链不再终结传输,泵透明
//     重连;反向事实见 `CoroTcpTransport.ReconnectsTransparentlyOnSameReadHandle`);
//   · `RequestCloseWakesPendingReadWithClosed` → 已被
//     `CoroTcpTransport.CloseTerminatesReadQueueAndBlocksRestart` 取代。
//
// 这两条讲的都是**读句柄对调用方的性质**(超时不是终结、句柄不独占),与
// `tcp_transport_test.cpp` 里"泵/链路/打断"那七组是两个层面的事,故独立成文件。
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
#include "task/fibertask.h"
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

TcpConfig ConfigFor(std::uint16_t port, std::chrono::milliseconds silence) {
  TcpConfig config;
  config.host = kLoopback;
  config.port = port;
  config.silence_timeout = silence;
  return config;
}

// 真实回环服务端;`Accept()` 交出已接受的 socket(归 server 所有,随其析构)。
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

// 【#132 的修法】`Accept()` 只说明**服务端**完成了三次握手,不说明**客户端**的泵已跑到
// "连上"、建好读流。此后立刻操作传输是 #132 那条竞态的原型,故一律先等到 kUp 再动手。
bool WaitLinkUp(const TcpTransport& t, int budget_ms = 3000) {
  return pumpFiberUntil([&t] { return t.CurrentLinkState() == LinkState::kUp; },
                        budget_ms);
}

}  // namespace

// AC:**调用方的 deadline 到期不是流的终结**——`AwaitRead` 超时返 kTimeout 后,同一个
// 句柄照常交付后续到达的字节。超时属于调用方一侧的等待策略(ADR-0007 D4:句柄交出,
// deadline 由调用方自理),队列**只有我方 `Close` 才终止**。
TEST(CoroTcpTransportRead, CallerTimeoutDoesNotTerminateTheHandle) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  QTcpSocket* peer = server.Accept();
  ASSERT_NE(peer, nullptr);
  ASSERT_TRUE(WaitLinkUp(t));

  auto rx = t.AsyncRead();  // 【全程只取这一次句柄】

  // 对端一字未发 → 短 deadline 的等待以 kTimeout 收敛。
  auto timed_out = AwaitRead(rx, 60ms);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));

  // 流未停:后续字节照常从**同一个句柄**取到。
  peer->write(QByteArray("after-timeout"));
  peer->flush();
  auto again = AwaitRead(rx, 2000ms);
  ASSERT_TRUE(again) << again.error().message();
  EXPECT_EQ(again.value().bytes, Bytes("after-timeout"))
      << "调用方超时把读流终结了(超时只应结束这一次等待)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// AC(ADR-0007 D4):**单读守卫已删除**——已有在途读者时,并发的第二个读者不再被拒
// (不返 kInvalidState),而是同挂在同一条 read_queue 上,无数据则各按自己的 deadline
// 以 kTimeout 收敛。
TEST(CoroTcpTransportRead, ConcurrentSecondReaderIsNotRejected) {
  LoopbackServer server;
  TcpTransport t(ConfigFor(server.port(), 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_NE(server.Accept(), nullptr);
  ASSERT_TRUE(WaitLinkUp(t));

  // 第一个读者先挂到队列上(带较短 deadline 便于收尾)。
  Coro::Awaitable<void> entered;
  bool first_ok = true;
  std::error_code first_error;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    auto r = testutil::ReadOnce(t, 400ms);
    first_ok = static_cast<bool>(r);
    if (!r) {
      first_error = r.error();
    }
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让第一个读者真正挂到队列上。

  // 并发第二个读者:**不得**被拒。
  auto second = testutil::ReadOnce(t, 200ms);
  ASSERT_FALSE(second);
  EXPECT_NE(second.error(), make_error_code(TransportErrc::kInvalidState))
      << "第二个读者被单读守卫拒了(该守卫已随 ADR-0007 D4 删除)";
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kTimeout));

  // 收尾:第一个读者按自己的 deadline 超时返回,读流语义不受影响。
  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(first_ok);
  EXPECT_EQ(first_error, make_error_code(TransportErrc::kTimeout));

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}
