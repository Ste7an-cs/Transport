// 协程原生 SerialTransport 契约真实 PTY 回环集成测试。
//
// 确定化手段:用 openpty() 造一对 master/slave 伪终端;SerialTransport 以 slave 的
// /dev/pts/N 路径打开真实 QSerialPort(QSerialPort 在 PTY 上工作,已实测 open +
// setBaudRate 生效、双向字节流、拔线时连发 ResourceError)。测试侧直接用 master fd
// 的原始 read/write 充当串口对端,在 fiber 调度器内 pump 推进 Qt 事件循环。
// 覆盖:字节流回环(切片任意)、配置(非默认波特率 + 非法配置/设备)、断开致命
// (Closing→Closed 不重连)、单读者、超时不停流、我方 RequestClose。
// 逐读 cancellation 为 out-of-scope(循环级中断靠 RequestClose),同 TcpTransport。
#include <fcntl.h>
#include <pty.h>
#include <unistd.h>

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
#include <memory>

#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/io/SerialTransport.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "transport/serial/SerialConfig.hpp"

using namespace std::chrono_literals;
using transport::Datagram;
using transport::Endpoint;
using transport::OperationOptions;
using transport::SendUnit;
using transport::SerialConfig;
using transport::SerialTransport;
using transport::Status;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::Result;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 一对 PTY:master 为原始 fd(测试侧对端),slave_name 为 /dev/pts/N(被测串口打开)。
struct PtyPair {
  int master = -1;
  std::string slave_name;
  bool ok = false;
};

PtyPair MakePty() {
  PtyPair p;
  int slave = -1;
  char name[256] = {0};
  if (openpty(&p.master, &slave, name, nullptr, nullptr) != 0) {
    return p;
  }
  ::close(slave);  // 由 QSerialPort 按名字重新打开 slave。
  ::fcntl(p.master, F_SETFL, ::fcntl(p.master, F_GETFL, 0) | O_NONBLOCK);
  p.slave_name = name;
  p.ok = true;
  return p;
}

SendUnit Frame(std::vector<std::uint8_t> bytes) {
  return SendUnit{std::move(bytes), Endpoint::Default()};
}

// 在 fiber 内 pump,直到从 master 读满 n 字节或超时;返回已读字节(字节流,切片任意)。
std::vector<std::uint8_t> ReadMaster(int master, std::size_t n, int budget_ms) {
  std::vector<std::uint8_t> got;
  testutil::pumpFiberUntil(
      [&] {
        std::uint8_t buf[256];
        for (;;) {
          const ssize_t r = ::read(master, buf, sizeof(buf));
          if (r > 0) {
            got.insert(got.end(), buf, buf + r);
          } else {
            break;  // EAGAIN / 无更多数据。
          }
        }
        return got.size() >= n;
      },
      budget_ms);
  return got;
}

// 在 fiber 内 pump,通过被测传输读满 n 字节(字节流,切片任意)。
std::vector<std::uint8_t> ReadTransport(SerialTransport& t, std::size_t n,
                                        int budget_ms) {
  std::vector<std::uint8_t> got;
  const auto stop = OperationOptions::Clock::now() + std::chrono::milliseconds(budget_ms);
  while (got.size() < n && OperationOptions::Clock::now() < stop) {
    OperationOptions opts;
    opts.deadline = OperationOptions::Clock::now() + 200ms;
    auto r = t.Read(opts);
    if (r) {
      got.insert(got.end(), r.value().bytes.begin(), r.value().bytes.end());
    } else if (r.error() != make_error_code(TransportErrc::kTimeout)) {
      break;  // 非超时错误(断开/关闭)→ 停止。
    }
  }
  return got;
}

}  // namespace

// 回环:被测传输 Write → master 读到相同字节(字节流)。source 为中立默认目的地。
TEST(CoroSerialTransport, TransportWriteReachesPeer) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  const std::vector<std::uint8_t> frame(32, 0x5A);
  ASSERT_TRUE(t.Write(Frame(frame)));
  const std::vector<std::uint8_t> got = ReadMaster(pty.master, frame.size(), 3000);
  EXPECT_EQ(got, frame);

  ::close(pty.master);
}

// 回环:master 写 → 被测传输 Read 读到相同字节;Datagram.source 为默认目的地。
TEST(CoroSerialTransport, PeerWriteReachesTransport) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  const std::vector<std::uint8_t> frame(48, 0x3C);
  ASSERT_EQ(::write(pty.master, frame.data(), frame.size()),
            static_cast<ssize_t>(frame.size()));

  OperationOptions opts;
  opts.deadline = OperationOptions::Clock::now() + 3s;
  auto first = t.Read(opts);
  ASSERT_TRUE(first) << first.error().message();
  EXPECT_EQ(first.value().source.kind, Endpoint::Kind::kDefault);
  std::vector<std::uint8_t> got(first.value().bytes.begin(),
                                first.value().bytes.end());
  while (got.size() < frame.size()) {
    auto more = ReadTransport(t, frame.size() - got.size(), 2000);
    got.insert(got.end(), more.begin(), more.end());
    if (more.empty()) break;
  }
  EXPECT_EQ(got, frame);

  ::close(pty.master);
}

// 配置生效:非默认波特率(9600)+ 完整参数应用后双向回环仍通。
TEST(CoroSerialTransport, NonDefaultBaudConfigApplied) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  cfg.baud_rate = 9600;
  cfg.data_bits = 8;
  cfg.stop_bits = 1;
  cfg.parity = 'N';
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  const std::vector<std::uint8_t> frame(16, 0xA5);
  ASSERT_TRUE(t.Write(Frame(frame)));
  const std::vector<std::uint8_t> got = ReadMaster(pty.master, frame.size(), 3000);
  EXPECT_EQ(got, frame);

  ::close(pty.master);
}

// 配置非法:数据位越界 → Configuration(参数应用路径可判别)。
TEST(CoroSerialTransport, InvalidConfigYieldsConfiguration) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  cfg.data_bits = 99;  // 非法。
  SerialTransport t(cfg);
  auto s = t.Start();
  EXPECT_FALSE(s);
  EXPECT_EQ(s.error(), make_error_code(TransportErrc::kConfiguration));
  ::close(pty.master);
}

// 打开失败:不存在的设备路径 → Connection。
TEST(CoroSerialTransport, OpenFailureYieldsConnection) {
  SerialConfig cfg;
  cfg.device = "/dev/tty-nonexistent-serial-xyz";
  SerialTransport t(cfg);
  auto s = t.Start();
  EXPECT_FALSE(s);
  EXPECT_EQ(s.error(), make_error_code(TransportErrc::kConnection));
}

// 断开致命:对端(master)关闭 → 在途 Read 以 Connection 收敛,传输 Closing→Closed
// (不自动重连);WaitClosed 完成。
TEST(CoroSerialTransport, PeerDisconnectIsFatalClosingToClosed) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  std::error_code read_err;
  bool read_ok = true;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions opts;
    opts.deadline = OperationOptions::Clock::now() + 3s;
    auto r = t.Read(opts);
    read_ok = static_cast<bool>(r);
    if (!r) read_err = r.error();
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让 Read 挂起在流上。
  ::close(pty.master);                  // 拔线:slave 端 hangup。

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(t.LastError(), make_error_code(TransportErrc::kConnection));

  // 致命 → 已 Closing→Closed:WaitClosed 立即完成,后续 Read 返回 Closed(不重连)。
  OperationOptions wopts;
  wopts.deadline = OperationOptions::Clock::now() + 2s;
  EXPECT_TRUE(t.WaitClosed(wopts));
  auto after = t.Read();
  EXPECT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
}

// 我方 RequestClose → 在途 Read 以 Closed 收敛(区别于设备断开的 Connection)。
TEST(CoroSerialTransport, RequestCloseWakesPendingReadWithClosed) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  std::error_code read_err;
  bool read_ok = true;
  Coro::Awaitable<void> entered;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions opts;
    opts.deadline = OperationOptions::Clock::now() + 3s;
    auto r = t.Read(opts);
    read_ok = static_cast<bool>(r);
    if (!r) read_err = r.error();
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);
  EXPECT_TRUE(t.RequestClose());

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(read_ok);
  EXPECT_EQ(read_err, make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(t.WaitClosed());
  ::close(pty.master);
}

// 单读者:已有在途 Read 时并发第二个 Read → InvalidState。
TEST(CoroSerialTransport, ConcurrentSecondReadIsInvalidState) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  Coro::Awaitable<void> entered;
  bool first_ok = true;
  std::error_code first_err;
  auto reader = Coro::makeTask([&] {
    entered.resolve();
    OperationOptions opts;
    opts.deadline = OperationOptions::Clock::now() + 300ms;
    auto r = t.Read(opts);
    first_ok = static_cast<bool>(r);
    if (!r) first_err = r.error();
  });
  ASSERT_TRUE(entered.await());
  boost::this_fiber::sleep_for(30ms);  // 让第一个 Read 占住读槽。

  OperationOptions opts;
  opts.deadline = OperationOptions::Clock::now() + 3s;
  auto second = t.Read(opts);
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kInvalidState));

  EXPECT_TRUE(reader.get());
  EXPECT_FALSE(first_ok);
  EXPECT_EQ(first_err, make_error_code(TransportErrc::kTimeout));
  ::close(pty.master);
}

// 带 deadline 的 Read 超时 → Timeout,且不停流:后续数据到达后再次 Read 仍拿到。
TEST(CoroSerialTransport, DeadlineTimeoutDoesNotStopStream) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  SerialConfig cfg;
  cfg.device = pty.slave_name;
  SerialTransport t(cfg);
  ASSERT_TRUE(t.Start()) << t.LastError().message();

  OperationOptions timeout_opts;
  timeout_opts.deadline = OperationOptions::Clock::now() + 60ms;
  auto timed_out = t.Read(timeout_opts);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));

  const std::vector<std::uint8_t> frame(16, 0x77);
  ASSERT_EQ(::write(pty.master, frame.data(), frame.size()),
            static_cast<ssize_t>(frame.size()));
  const std::vector<std::uint8_t> got = ReadTransport(t, frame.size(), 3000);
  EXPECT_EQ(got, frame);

  ::close(pty.master);
}

namespace {

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

// 标准 echo 响应(与 DefaultProtocolKeyStrategy 配对规则一致):frm_type=kResponse、
// session_id 原样、message_id = 请求码 | 0x1000、payload echo。
Message EchoResponse(const Message& command) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.protocol_id = command.protocol_id;
  resp.session_id = command.session_id;
  resp.message_id = static_cast<std::uint16_t>(command.message_id | 0x1000);
  resp.payload = command.payload;
  return resp;
}

// master 侧裸 echo harness fiber:直接在 master fd 上收发原始字节 + SystemCodec 组帧
// (SystemCodec.Decode 内部缓冲跨切片,故可逐片喂入)。收 kCommand → 回一帧标准
// 响应。stop 置真后退出。充当串口对端(节点在 slave 侧)。
auto SpawnMasterEcho(int master, bool& ended, const bool& stop) {
  return Coro::makeTask([master, &ended, &stop] {
    SystemCodec codec;
    while (!stop) {
      std::uint8_t buf[256];
      const ssize_t r = ::read(master, buf, sizeof(buf));
      if (r > 0) {
        auto decoded = codec.Decode(buf, static_cast<std::size_t>(r));
        if (decoded) {
          for (const auto& command : decoded.value()) {
            if (command.frm_type != FrameType::kCommand) {
              continue;
            }
            auto encoded = codec.Encode(EchoResponse(command));
            if (encoded) {
              const auto& out = encoded.value();
              ssize_t off = 0;
              while (off < static_cast<ssize_t>(out.size())) {
                const ssize_t w = ::write(master, out.data() + off,
                                          out.size() - off);
                if (w > 0) {
                  off += w;
                } else {
                  boost::this_fiber::sleep_for(1ms);
                }
              }
            }
          }
        }
      } else {
        boost::this_fiber::sleep_for(1ms);  // EAGAIN → 让出推进调度器。
      }
    }
    ended = true;
  });
}

}  // namespace

// (可选)端到端:真 ProtocolNode(SerialTransport + 流式 SystemCodec)经 PTY 回环向
// master 侧裸 echo 对端发一次请求-应答,Request 恰好一次完成、payload 与 echo 一致。
// 证实串口传输可无改动组合进节点栈(RT_NODE_006 / D12 复用实证)。
TEST(CoroSerialTransport, NodeRequestResponseEndToEnd) {
  PtyPair pty = MakePty();
  ASSERT_TRUE(pty.ok);
  bool echo_ended = false;
  bool stop = false;
  auto echo = SpawnMasterEcho(pty.master, echo_ended, stop);

  SerialConfig cfg;
  cfg.device = pty.slave_name;
  auto node = std::make_unique<ProtocolNode>(
      std::make_unique<SerialTransport>(cfg), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node->Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node->Request(MakeRequest(0x0002, {0x11, 0x22, 0x33}), options);
    done = true;
  });

  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return done; }, 4000));
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(outcome.value().payload,
            (std::vector<std::uint8_t>{0x11, 0x22, 0x33}));

  stop = true;
  ASSERT_TRUE(node->Close());
  EXPECT_TRUE(testutil::pumpFiberUntil([&] { return echo_ended; }, 2000));
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(echo.get());
  ::close(pty.master);
}

// 生命周期:Start 前 Read/Write → InvalidState。
TEST(CoroSerialTransport, OperationsBeforeStartAreInvalidState) {
  SerialConfig cfg;
  cfg.device = "/dev/null";
  SerialTransport t(cfg);
  auto r = t.Read();
  EXPECT_FALSE(r);
  EXPECT_EQ(r.error(), make_error_code(TransportErrc::kInvalidState));
  auto w = t.Write(Frame({1, 2, 3}));
  EXPECT_FALSE(w);
  EXPECT_EQ(w.error(), make_error_code(TransportErrc::kInvalidState));
}
