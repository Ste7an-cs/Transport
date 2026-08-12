// 协程原生 TcpTransport 发送语义真实回环集成测试。
// 在 fiber 调度器(coro_test_main)内用本机 TCP 回环验证:Write 刷完即完成、
// 慢读取端下的背压且内存有界、并发一致顺序、对端 reset → Io/Connection + 关连接、
// 写入已开始后超时 → Timeout 且帧不被截断。连接建立由测试夹具完成(非本类职责)。
#include <algorithm>
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
#include "transport/core/Error.hpp"
#include "transport/io/tcp/TcpTransport.hpp"

using namespace std::chrono_literals;
using transport::Endpoint;
using transport::Datagram;
using transport::OperationOptions;
using transport::SendUnit;
using transport::Status;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 建立一对已连接的回环 socket(客户端 + 服务端已接受);连接建立不在被测类职责。
bool MakeConnectedPair(QTcpServer& server, QTcpSocket*& client,
                       QTcpSocket*& accepted) {
  if (!server.listen(QHostAddress::LocalHost, 0)) {
    return false;
  }
  const quint16 port = server.serverPort();
  client = new QTcpSocket();
  auto connected =
      Coro::coro(client).connectToHost(QHostAddress::LocalHost, port);
  if (!Coro::await_for(connected, 2s)) {
    return false;
  }
  if (!testutil::pumpFiberUntil([&] { return server.hasPendingConnections(); },
                                2000)) {
    return false;
  }
  accepted = server.nextPendingConnection();
  if (!accepted) {
    return false;
  }
  accepted->setParent(nullptr);  // 交由 TcpTransport 管理生命周期。
  return true;
}

SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}

// 收紧内核收发缓冲,让慢读取端能确定性地把内核发送缓冲填满(制造背压)。
// 只收紧内核缓冲不够:QAbstractSocket::readBufferSize() 默认 0 = 用户态读缓冲无上限,
// 事件循环一转就把内核接收缓冲抽干、灌进无界的用户态缓冲,"慢读取端"前提失效、背压
// 提前释放。故同时给用户态读缓冲设上限(夹具前提,非产品行为)。
void Throttle(QTcpSocket* client, QTcpSocket* accepted, int bytes = 16384) {
  client->setSocketOption(QAbstractSocket::SendBufferSizeSocketOption, bytes);
  accepted->setSocketOption(QAbstractSocket::ReceiveBufferSizeSocketOption,
                            bytes);
  accepted->setReadBufferSize(bytes);
}

// 串行排队下并发写不再被拒:直接发,断言成功(后到者内部排队等待写槽)。
void WriteFrame(TcpTransport& t, std::vector<std::uint8_t> bytes) {
  ASSERT_TRUE(t.Write(Frame(bytes)));
}

// 从流式传输读满 n 字节(读路径按本 spec 只需最小能力)。
std::vector<std::uint8_t> ReadExact(TcpTransport& t, std::size_t n,
                                    int budget_ms = 3000) {
  std::vector<std::uint8_t> buf;
  const auto deadline =
      OperationOptions::Clock::now() + std::chrono::milliseconds(budget_ms);
  while (buf.size() < n && OperationOptions::Clock::now() < deadline) {
    OperationOptions options;
    options.deadline = deadline;
    auto r = testutil::ReadOnce(t, options);
    if (!r) {
      break;
    }
    buf.insert(buf.end(), r.value().bytes.begin(), r.value().bytes.end());
  }
  return buf;
}

}  // namespace

// RT_TRANSPORT_008:Write 成功后帧已上线,对端完整收到;发送时间戳可观测。
TEST(CoroTcpTransport, WriteFlushesFrameAndPeerReceivesIt) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport sender(client);
  TcpTransport receiver(accepted);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  const std::vector<std::uint8_t> frame(64, 0x5A);
  EXPECT_FALSE(sender.LastSendTime().has_value());
  ASSERT_TRUE(sender.Write(Frame(frame)));
  EXPECT_TRUE(sender.LastSendTime().has_value());
  EXPECT_EQ(sender.SendWaiterDepth(), 0U);

  const auto received = ReadExact(receiver, frame.size());
  EXPECT_EQ(received, frame);
  EXPECT_TRUE(receiver.LastReceiveTime().has_value());
}

// RT_TRANSPORT_008:慢读取端填满内核发送缓冲 → 生产者 Write 挂起(背压),且框架
// 用户态发送缓冲至多驻留一个在写帧(第二个并发写被拒 → 内存有界)。排空后完成。
TEST(CoroTcpTransport, SlowReaderAppliesBackpressureAndBoundsInFlightFrames) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  Throttle(client, accepted);
  TcpTransport sender(client);
  // 读取端**暂不接**:TcpTransport::Start() 会建立 readAll 流,该流在每次 readyRead
  // 上把 socket 抽干 → 就不再是"慢读取端"。慢读取端的构造 = 背压窗口内无人读
  //(内核接收缓冲 + 上了限的 Qt 用户态读缓冲一起被填满),排空阶段再接上读取端。
  ASSERT_TRUE(sender.Start());

  const std::vector<std::uint8_t> big(256u * 1024u, 0x33);  // 256 KiB,远超收紧后的缓冲。
  Status write_status{make_error_code(TransportErrc::kInternal)};
  auto writer = Coro::makeTask([&] { write_status = sender.Write(Frame(big)); });

  // 内核发送缓冲填满 → Write 在刷完循环中挂起,发送等待者深度为 1。
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return sender.SendWaiterDepth() == 1; }));
  // 读取端不读:背压持续,Write 不完成。
  boost::this_fiber::sleep_for(80ms);
  EXPECT_EQ(sender.SendWaiterDepth(), 1U);
  EXPECT_FALSE(sender.LastSendTime().has_value());
  // 并发第二写不再被拒,而是排队等待写槽 → 深度升至 2;但至多一个在写帧真正进入
  // 内核(队首持有者),框架用户态发送缓冲仍有界。
  Status second_status{make_error_code(TransportErrc::kInternal)};
  auto writer_b =
      Coro::makeTask([&] { second_status = sender.Write(Frame({0x99})); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return sender.SendWaiterDepth() == 2; }));

  // 接上读取端并排空 → 背压释放,两帧依序完成、串行上线(先 big,后 0x99)。
  // readAll 建流时会先 drain 已到达的字节,故背压窗口内被缓冲的字节不会丢。
  TcpTransport receiver(accepted);
  ASSERT_TRUE(receiver.Start());
  const auto received = ReadExact(receiver, big.size() + 1, 8000);
  EXPECT_TRUE(writer.get());
  EXPECT_TRUE(writer_b.get());
  EXPECT_TRUE(write_status);
  EXPECT_TRUE(second_status);
  EXPECT_EQ(sender.SendWaiterDepth(), 0U);
  EXPECT_TRUE(sender.LastSendTime().has_value());
  ASSERT_EQ(received.size(), big.size() + 1);
  EXPECT_TRUE(std::equal(big.begin(), big.end(), received.begin()));
  EXPECT_EQ(received.back(), 0x99);
}

// RT_TRANSPORT_007/004:并发发送者被串行化为一致全序,单帧不交错(先获取有效写者
// 先整帧上线,对端绝不看到两帧交错的半截字节)。
TEST(CoroTcpTransport, ConcurrentSendersProduceConsistentUninterleavedOrder) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  Throttle(client, accepted);
  TcpTransport sender(client);
  TcpTransport receiver(accepted);
  ASSERT_TRUE(sender.Start());
  ASSERT_TRUE(receiver.Start());

  constexpr std::size_t kFrame = 128u * 1024u;
  const std::vector<std::uint8_t> frame_a(kFrame, 0xAA);
  const std::vector<std::uint8_t> frame_b(kFrame, 0xBB);

  // 不预先安排谁持有写槽:两个 fiber 并发进入,由串行排队涌现一致全序。
  auto first = Coro::makeTask([&] { WriteFrame(sender, frame_a); });
  auto second = Coro::makeTask([&] { WriteFrame(sender, frame_b); });

  const auto received = ReadExact(receiver, 2 * kFrame, 8000);
  EXPECT_TRUE(first.get());
  EXPECT_TRUE(second.get());

  ASSERT_EQ(received.size(), 2 * kFrame);
  // 恰好一次值跳变 = 两帧各自连续、互不交错(串行化);顺序为涌现的一致全序。
  std::size_t transitions = 0;
  for (std::size_t i = 1; i < received.size(); ++i) {
    if (received[i] != received[i - 1]) {
      ++transitions;
    }
  }
  EXPECT_EQ(transitions, 1U);
  EXPECT_EQ(std::count(received.begin(), received.end(), 0xAA), kFrame);
  EXPECT_EQ(std::count(received.begin(), received.end(), 0xBB), kFrame);
  // 一帧完整地先于另一帧;哪一帧在前是涌现的(不预设)。
  EXPECT_TRUE((received.front() == 0xAA && received.back() == 0xBB) ||
              (received.front() == 0xBB && received.back() == 0xAA));
}

// RT_TRANSPORT_004.4:刷完途中对端 reset → 本次 Write 返回 Io/Connection,本物理
// 连接关闭,不在其上继续发送、不重发残缺帧。
TEST(CoroTcpTransport, PeerResetDuringFlushFailsWriteAndClosesConnection) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  Throttle(client, accepted);
  TcpTransport sender(client);
  ASSERT_TRUE(sender.Start());

  const std::vector<std::uint8_t> big(256u * 1024u, 0x44);
  Status write_status{make_error_code(TransportErrc::kInternal)};
  auto writer = Coro::makeTask([&] { write_status = sender.Write(Frame(big)); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return sender.SendWaiterDepth() == 1; }));

  accepted->abort();  // 对端强制断开(reset),刷完中途失败。
  EXPECT_TRUE(writer.get());
  ASSERT_FALSE(write_status);
  const auto err = write_status.error();
  EXPECT_TRUE(err == make_error_code(TransportErrc::kConnection) ||
              err == make_error_code(TransportErrc::kIo))
      << "unexpected: " << err.message();
  // 本物理连接被关闭。
  EXPECT_TRUE(sender.WaitClosed());
  EXPECT_NE(sender.LastError(), std::error_code{});
  // 关闭后不得继续发送。
  const auto after = sender.Write(Frame({1}));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));

  accepted->deleteLater();
}

// 读侧:对端正常关闭时在途 Read 以 Closed 收敛——本类不重连,连接终结即传输终结
// (RT_TRANSPORT_008 / ADR-0004 D1)。
TEST(CoroTcpTransport, PeerCloseWakesPendingReadWithClosed) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  TcpTransport receiver(accepted);
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
    if (!r) {
      read_err = r.error();
    }
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让 Read 挂起在流上。
  client->disconnectFromHost();  // 对端正常关闭。

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kClosed));
  client->deleteLater();
}

// RT_REQUEST_004.4:写入已开始后的总超时是发起方(请求层)语义。ITransport::Write
// 无取消入口(契约固定签名),故传输侧的刷完 fiber 不会被请求层超时打断——超时只让
// 一个独立 awaiter 放弃等待,在写帧由构造保证刷完到底、健康连接上绝不截断。本测试
// 验证:请求层 awaiter 120ms 超时后,底层仍把整帧刷完,对端完整收到、无截断。
TEST(CoroTcpTransport, TimeoutAfterWriteStartedDoesNotTruncateHealthyFrame) {
  QTcpServer server;
  QTcpSocket* client = nullptr;
  QTcpSocket* accepted = nullptr;
  ASSERT_TRUE(MakeConnectedPair(server, client, accepted));
  Throttle(client, accepted);
  TcpTransport sender(client);
  // 读取端暂不接:本用例要求在写帧在超时窗口内仍未刷完(背压持续),故超时窗口内
  // 不能有人抽干 socket——Start() 建立的 readAll 流会在每次 readyRead 上抽干。
  ASSERT_TRUE(sender.Start());

  const std::vector<std::uint8_t> big(256u * 1024u, 0x77);
  Status write_status{make_error_code(TransportErrc::kInternal)};
  Coro::Awaitable<void> completion;
  auto writer = Coro::makeTask([&] {
    write_status = sender.Write(Frame(big));
    completion.resolve();
  });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return sender.SendWaiterDepth() == 1; }));

  // 发起方(请求层)总超时:独立 awaiter 本地判 Timeout,不撕连接、不打断在写帧。
  const auto waited = completion.await_for(120ms);
  ASSERT_FALSE(waited);
  EXPECT_EQ(waited.error(), std::make_error_code(std::errc::timed_out));
  EXPECT_EQ(sender.SendWaiterDepth(), 1U);  // 底层仍在尽力刷完。

  // 接上读取端并排空:帧未被截断,对端完整收到,且底层刷完最终成功。
  TcpTransport receiver(accepted);
  ASSERT_TRUE(receiver.Start());
  const auto received = ReadExact(receiver, big.size(), 8000);
  EXPECT_TRUE(writer.get());
  EXPECT_TRUE(write_status);
  ASSERT_EQ(received.size(), big.size());
  EXPECT_EQ(received, big);
}
