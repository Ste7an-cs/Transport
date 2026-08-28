// -----------------------------------------------------------------------------
// serial_transport_test.cpp — 协程原生 SerialTransport 真实 PTY 回环集成测试(ADR-0012)
//
// 确定化手段:用 `openpty()` 造一对 master/slave 伪终端;被测传输以 slave 的
// `/dev/pts/N` 路径打开真实 `QSerialPort`,测试侧直接用 master fd 的原始 read/write 充当
// 串口对端,在 fiber 调度器(coro_test_main)内 pump 推进 Qt 事件循环。
//
// **一个关键的对端观测手段**(本文件多处依赖):Linux 上 slave 的最后一个 fd 关闭后,
// master 的 `read()` 返 **EIO**;slave 被重新打开后又回到 EAGAIN。故"设备被关了"与
// "设备又开了"在**对端侧直接可观测**——`Close→退避→Open` 这一整圈不必靠固定 sleep 猜,
// 全部断言因此是**事件驱动**的(`pumpFiberUntil`)。
//
// 覆盖九组事实:
//   1. 配置校验(D12):六项非法配置 → kConfiguration 且**停在 Created**;改配可再 Start;
//   2. 回环收字节:切片入队,`peer` 为**固定设备端点**(D9);
//   3. 回环发字节:`peer` **被忽略**、任何值都写往那一个设备(D9,不判 kInvalidArgument);
//   4. ⭐ **空切片被跳过**(**D5** 的回归证据):制造设备重开,断言调用方一侧**从未**取到
//      0 字节的 `Datagram`;
//   5. **静默超时触发重开**(**D4**):对端不发数据 → 对端观测到设备被关、随即重开,
//      **同一个 `AsyncRead()` 句柄**照常收到重开后的数据(重开对调用方透明);
//   6. **不自终**(**D1**,TBD-005 关闭的行为证据):设备不存在时 `Start()` 成功,数个
//      `silence_timeout` 后仍 Running、`LastError()` 有值、读队列未被终结;
//   7. **`Close()` 的四处打断**(**D6**):退避中 / 读等待中 / 写泵等数据 / 写泵等设备
//      就绪,四条一条不能少,均须**远小于 `silence_timeout`** 收敛;
//   8. `CurrentLinkState()` **只有两值**(**D10**):退避重开期间恒为 kDown,
//      **不出现 kEstablishing**;
//   9. 生命周期:未 Start 的读写、`Close()` 幂等、`WaitClosed()` 后可安全析构。
//
// 第 4 与第 7 组是本文件的核心。两组都做过负向对照(实测数字见 PR 正文):
//
//   | 打断(silence_timeout = 3s) | 四处齐备 | 去掉该处          |
//   |------------------------------|----------|-------------------|
//   | ① 退避                       | 0 ms     | 2900 ms           |
//   | ② 读等待                     | 0 ms     | 2950 ms           |
//   | ③ 写泵等数据                 | 0 ms     | **永久挂死**(>40s 被 timeout 杀) |
//   | ④ 写泵等设备就绪             | 0 ms     | **永久挂死**(同上) |
//
// **写侧比读侧更严**:读侧两处用的是 `await_for`,漏一处最坏挂满一个 silence_timeout;
// 写侧两处是**无限期** `await`,漏一处就是**永久挂死**——`WaitClosed()` join 管理泵,
// 而管理泵收尾时要先 join 写泵。
//
// 注:**② 有两条独立有效的路径**——`read_stream_->close()` 与 `Close()` 里的
// `port_->close()`(走 `aboutToClose`,ADR-0012 D6 的实测结论)。逐个去掉时另一条会顶上,
// 各自仍 0 ms 收敛;上表 ② 的 2950 ms 是**同时去掉两者**测得的。这与 TCP 恰好相反
// (其 `abort()` 在连接窗口内唤不醒任何等待,故 D15 要求必须持句柄 close)。
//
// **全部实测建立在 PTY 上,真实硬件未验**——这是 ADR-0012 已登记的代价③,本票不改变。
// -----------------------------------------------------------------------------
#include <errno.h>
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
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/serial/SerialConfig.hpp"
#include "transport/io/serial/SerialTransport.hpp"

using namespace std::chrono_literals;
using testutil::AwaitRead;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::Endpoint;
using transport::LinkState;
using transport::SerialConfig;
using transport::SerialTransport;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 必不存在的设备路径:用来把泵**钉在退避**里(`open()` 恒失败)。
constexpr char kMissingDevice[] = "/dev/tty-no-such-serial-193";

// 一对 PTY:master 为原始 fd(测试侧对端),slave 为 `/dev/pts/N`(被测传输打开)。
class PtyPair {
 public:
  PtyPair() {
    int slave = -1;
    char name[256] = {0};
    ok_ = ::openpty(&master_, &slave, name, nullptr, nullptr) == 0;
    if (!ok_) {
      return;
    }
    ::close(slave);  // 由 QSerialPort 按名字重新打开 slave(故"最后一个 slave fd"归它)。
    ::fcntl(master_, F_SETFL, ::fcntl(master_, F_GETFL, 0) | O_NONBLOCK);
    slave_name_ = name;
  }
  ~PtyPair() {
    if (master_ >= 0) {
      ::close(master_);
    }
  }
  PtyPair(const PtyPair&) = delete;
  PtyPair& operator=(const PtyPair&) = delete;

  bool ok() const { return ok_; }
  int master() const { return master_; }
  /// 交出 master fd 的所有权(此后析构不再关它)——供"拔线"用例自行关闭。
  int detach_master() {
    const int fd = master_;
    master_ = -1;
    return fd;
  }
  const std::string& slave() const { return slave_name_; }

 private:
  int master_ = -1;
  std::string slave_name_;
  bool ok_ = false;
};

// 对端侧一次非阻塞排空(EAGAIN 即无更多数据;slave 全部 fd 关闭时为 EIO,同样返回)。
void DrainPeer(int master, std::vector<std::uint8_t>* sink) {
  std::uint8_t buf[512];
  for (;;) {
    const ssize_t r = ::read(master, buf, sizeof(buf));
    if (r <= 0) {
      return;
    }
    sink->insert(sink->end(), buf, buf + r);
  }
}

SerialConfig ConfigFor(const std::string& device,
                       std::chrono::milliseconds silence) {
  SerialConfig config;
  config.device = device;
  config.silence_timeout = silence;
  return config;
}

std::vector<std::uint8_t> Bytes(const char* text) {
  const std::string s(text);
  return std::vector<std::uint8_t>(s.begin(), s.end());
}

Datagram Unit(const char* text, Endpoint peer = Endpoint::Default()) {
  return Datagram{Bytes(text), std::move(peer)};
}

// 在对端(master)侧收满 want 字节(或超时);字节流,切片任意。
std::vector<std::uint8_t> RecvAtLeast(int master, std::size_t want,
                                      int budget_ms = 3000) {
  std::vector<std::uint8_t> got;
  pumpFiberUntil([&] { DrainPeer(master, &got); return got.size() >= want; },
                 budget_ms);
  return got;
}

// 把读队列里此刻**已积压**的全部切片取空(逐次短等待,直到一次超时为止)。
// 用于第 4 组:空切片若被投出来,一定还躺在有界队列里。
std::vector<Coro::Result<Datagram>> DrainRead(
    const std::shared_ptr<Coro::Awaitable<Datagram>>& rx,
    std::chrono::milliseconds quiet = 200ms) {
  std::vector<Coro::Result<Datagram>> out;
  for (;;) {
    auto got = AwaitRead(rx, quiet);
    if (!got) {
      return out;  // 队列已空(kTimeout)或已终结(kClosed)。
    }
    out.push_back(std::move(got));
  }
}

std::chrono::milliseconds Since(std::chrono::steady_clock::time_point began) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - began);
}

}  // namespace

// —— 1. 配置校验(D12)——————————————————————————————————————————————————

// AC:六项非法配置一律返 kConfiguration,且**停在 Created**——未起泵(`IsRunning()` 假)、
// 读句柄仍以 kInvalidState 关闭(该错误只在 Created 才给);改配后可再 Start 成功。
TEST(CoroSerialTransport, RejectsInvalidConfigAndStaysCreated) {
  SerialConfig bad[6];
  bad[0] = ConfigFor("", 1000ms);                     // device 空。
  bad[1] = ConfigFor(kMissingDevice, 1000ms);
  bad[1].baud_rate = 0;                               // 波特率为 0。
  bad[2] = ConfigFor(kMissingDevice, 0ms);            // 无"0 = 禁用"这一档(与 UDP 异)。
  bad[3] = ConfigFor(kMissingDevice, -5ms);           // 负值同理。
  bad[4] = ConfigFor(kMissingDevice, 1000ms);
  bad[4].data_bits = 99;                              // 数据位越界。
  bad[5] = ConfigFor(kMissingDevice, 1000ms);
  bad[5].stop_bits = 3;                               // 停止位越界。

  for (const SerialConfig& config : bad) {
    SerialTransport t(config);
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

  // 校验非法的 parity 单列一条(char 值域):'X' 不在 N/E/O 内。
  SerialConfig bad_parity = ConfigFor(kMissingDevice, 1000ms);
  bad_parity.parity = 'X';
  SerialTransport rejected(bad_parity);
  auto parity_started = rejected.Start();
  ASSERT_FALSE(parity_started);
  EXPECT_EQ(parity_started.error(),
            make_error_code(TransportErrc::kConfiguration));

  // 配置合法即可 Start——校验是配置的属性,不是实例的死刑。
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport good(ConfigFor(pty.slave(), 1000ms));
  EXPECT_TRUE(good.Start());
  EXPECT_TRUE(good.IsRunning());
  EXPECT_EQ(good.CurrentLinkState(), LinkState::kUp)
      << "open() 是同步的,Start() 返回后链路即应可观测(与 TCP 的差异)";
  EXPECT_TRUE(good.Close());
  good.WaitClosed();
}

// —— 2. 回环收字节 ——————————————————————————————————————————————————

// AC(RT_TRANSPORT_003 / D9):对端发出的字节原样入队(切片,不是帧),`peer` 是
// **固定设备端点**——串口无 peer 概念,不从设备上取来源,且每一片都相同。
TEST(CoroSerialTransport, DeliversBytesWithFixedDevicePeer) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kUp);
  auto rx = t.AsyncRead();

  const std::string payload = "hello-serial";
  ASSERT_EQ(::write(pty.master(), payload.data(), payload.size()),
            static_cast<ssize_t>(payload.size()));

  std::vector<std::uint8_t> got;
  Endpoint first_peer;
  bool first = true;
  while (got.size() < payload.size()) {
    auto chunk = AwaitRead(rx, 2000ms);
    ASSERT_TRUE(chunk) << chunk.error().message();
    EXPECT_FALSE(chunk.value().bytes.empty()) << "投出了空切片(D5)";
    if (first) {
      first_peer = chunk.value().peer;
      first = false;
    } else {
      EXPECT_EQ(chunk.value().peer.kind, first_peer.kind)
          << "同一设备的切片给出了不同的 peer";
    }
    got.insert(got.end(), chunk.value().bytes.begin(),
               chunk.value().bytes.end());
  }
  EXPECT_EQ(got, Bytes("hello-serial"));
  // 固定设备端点(D9):`Endpoint` 现有三 kind 里只有 kDefault 能如实表达单设备介质
  // ——既非 kNet 的 ip:port,也非 kTopic 的 DDS 主题。
  EXPECT_EQ(first_peer.kind, Endpoint::Kind::kDefault);

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 3. 回环发字节 ——————————————————————————————————————————————————

// AC(RT_TRANSPORT_003 / D9):`AsyncWrite` 的字节原样出现在设备对端;`peer` **一律
// 忽略**——填 kNet / kTopic 也照样写往那一个设备,且**不返 kInvalidArgument**。
// 这与 `UdpTransport` 相反(它按 peer 解析目的地、解析不了就丢该条并记 LastError)。
TEST(CoroSerialTransport, WritesBytesIgnoringPeer) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(t.Start());

  ASSERT_TRUE(t.AsyncWrite(Unit("a", Endpoint::Default())));
  ASSERT_TRUE(t.AsyncWrite(Unit("b", Endpoint::Net("192.0.2.1", 9))));
  ASSERT_TRUE(t.AsyncWrite(Unit("c", Endpoint::Topic("no-such-medium"))));

  EXPECT_EQ(RecvAtLeast(pty.master(), 3), Bytes("abc"))
      << "peer 未被忽略:某一条没写到配置的那个设备";
  EXPECT_NE(t.LastError(), make_error_code(TransportErrc::kInvalidArgument))
      << "串口不得因 peer 非默认而判非法(D9)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 4. ⭐ 空切片被跳过(D5 的回归证据)————————————————————————————————

// AC(**D5**):`coroiodevice::readAll()` 的 push **不判空**(`ch->push(dev->readAll())`),
// 而 `corosocket::readAll()` 有 `if(!bytes.isEmpty())` 守卫——故**串口读泵必须显式
// `if (chunk->isEmpty()) continue;`**,UDP/TCP 都不需要这一行。
//
// 本用例制造真实的设备重开(静默超时驱动),然后把读队列**整个取空**:断言取到的每一个
// `Datagram` 都非空,且取到的字节**恰好只有对端真发的那些**。若重开吐出 0 字节切片而
// 实现漏了那一行,该切片会躺在有界队列里,本用例当场失败。
//
// **如实登记一处未复现**:本环境(Qt 5.15 / Linux PTY)下做过负向对照——把实现里那一行
// `if (bytes.isEmpty()) continue;` 去掉后本用例**仍然通过**,且在读泵里加计数探针跑完
// 整个串口用例集,**一次空切片都没观测到**(包括拔线之后)。即 #186 记录的"设备重开后
// 读流立刻吐一个 0 字节切片"**在本环境未能复现**。那一行**仍然保留**:它是 ADR-0012
// **D5** 的明文决策、成本一行、且 `coroiodevice::readAll()` 的 `ch->push(dev->readAll())`
// 确实**不判空**(源码级事实,与 `corosocket` 的 `if(!bytes.isEmpty())` 守卫相对),
// 空切片能否出现只取决于 Qt 在何种条件下发一次"无字节可读的 `readyRead`"。
// 本用例因此是**契约断言**(调用方永不取到空 `Datagram`),而非一条已复现故障的回归。
TEST(CoroSerialTransport, NeverDeliversEmptyChunksAcrossDeviceReopen) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 200ms));  // 短静默:快速逼出重开。
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();  // 【全程只取这一次句柄】

  // 逼出一整圈"关设备 → 重开设备"(对端不发一个字节,静默超时是唯一驱动)。
  // 门槛是**事件**而非固定 sleep:`LastError()` 变 kTimeout 即静默判据已触发,那正是
  // `break → port_->close() → 下一轮 Open()` 这一圈的入口(归因就落在 break 之前)。
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.LastError() == make_error_code(TransportErrc::kTimeout); },
      3000))
      << "静默超时未触发(重开这一圈根本没发生,后续断言无意义)";

  // 重开之后设备照常工作:对端发一批真数据。**数据能到达本身就是重开成功的证据**
  // ——泵在上一步已把设备关掉,不重开就一个字节也收不到。
  const std::string payload = "after-reopen";
  ASSERT_EQ(::write(pty.master(), payload.data(), payload.size()),
            static_cast<ssize_t>(payload.size()));

  // 把队列整个取空:每一片都必须非空,且拼起来恰好只有真数据。
  std::vector<std::uint8_t> assembled;
  std::size_t empty_chunks = 0;
  ASSERT_TRUE(pumpFiberUntil(
      [&] {
        for (auto& chunk : DrainRead(rx, 50ms)) {
          if (chunk.value().bytes.empty()) {
            ++empty_chunks;
          }
          assembled.insert(assembled.end(), chunk.value().bytes.begin(),
                           chunk.value().bytes.end());
        }
        return assembled.size() >= payload.size();
      },
      3000))
      << "重开后的数据没有从同一个句柄取到";
  EXPECT_EQ(empty_chunks, 0u)
      << "读泵投出了 0 字节切片——D5 的那一行 `if (chunk->isEmpty()) continue;` 丢了";
  EXPECT_EQ(assembled, Bytes("after-reopen"))
      << "取到的字节不止对端真发的那些(混入了空切片或残留)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 5. 静默超时触发重开(D4)————————————————————————————————————————

// AC(**D4**,反转 ADR-0011 D4):串口**没有断开事件**,`silence_timeout` 是"链路坏了"
// 的**唯一**主动判据。对端长时间不发数据 → 泵判链路坏 → `close()` → 重开。
//
// **"关掉了"这一步怎么证**:`close()` 与紧接着的 `open()` 之间**没有任何挂起点**(串口的
// `open()` 是同步的),故对端那个"slave 全部 fd 已关 → read 返 EIO"的窗口**宽度为零**,
// 测试 fiber 根本调度不到——直接观测行不通。本用例改用一条**决定性**的判据:
//
//   先让静默超时打一圈(`LastError()` 变 kTimeout,且重开后的设备照常收字节),
//   再**拔线**(关掉 pty master,`/dev/pts/N` 随之消失),然后断言链路变 `kDown`。
//
// 这一步之所以决定性:据 ADR-0012 背景 ①(实测),拔线后 `readAll()` 流**完全不终止**、
// `isOpen()` **仍为 true**——即**如果泵不主动 `close()`,链路永远不会变 kDown**。故
// kDown 只可能来自"静默超时 → 泵主动关设备 → 重开失败(设备已不在)"这一整圈。
// 顺带也证了**不自终**(D1):此后仍 Running,只是退避重试。
TEST(CoroSerialTransport, SilenceTimeoutClosesAndReopensDevice) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 200ms));
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();  // 【全程只取这一次句柄】
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kUp);

  // ① 静默超时触发,并归因到 LastError(它是串口判活的唯一主动判据)。
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.LastError() == make_error_code(TransportErrc::kTimeout); },
      3000))
      << "静默超时未触发/未归因";

  // ② 重开成功且**对调用方透明**:重开后的数据照常从**同一个句柄**取到——泵刚把设备
  //    关过,不重开就一个字节也收不到。
  const std::string payload = "post-reopen";
  ASSERT_EQ(::write(pty.master(), payload.data(), payload.size()),
            static_cast<ssize_t>(payload.size()));
  std::vector<std::uint8_t> got;
  ASSERT_TRUE(pumpFiberUntil(
      [&] {
        for (auto& chunk : DrainRead(rx, 20ms)) {
          got.insert(got.end(), chunk.value().bytes.begin(),
                     chunk.value().bytes.end());
        }
        return got.size() >= payload.size();
      },
      3000))
      << "重开后数据未从同一个 AsyncRead 句柄取到(重开未对调用方透明)";
  EXPECT_EQ(got, Bytes("post-reopen"));
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kUp);

  // ③ 拔线:设备消失。**没有泵的主动 close(),链路永远不会变 kDown**(拔线后
  //    readAll() 不终止、isOpen() 仍为 true——ADR-0012 背景 ①)。故 kDown 就是
  //    "静默超时 → 关设备 → 重开"这一整圈的决定性证据。
  ::close(pty.detach_master());
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kDown; }, 3000))
      << "设备已消失而链路仍报 kUp:泵没有在静默超时后主动关闭设备(D4)";
  EXPECT_TRUE(t.IsRunning()) << "串口自终了(D1 要求不自终)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 6. 不自终(D1,TBD-005 关闭的行为证据)——————————————————————————

// AC(**D1**):设备根本不存在 → `Start()` **成功**(打开失败不算启动失败),泵无限退避
// 重试;数个 `silence_timeout` 后仍 Running、`LastError()` 有值、**读队列未被终结**。
// 这正是 RT_LIFECYCLE_008 的串口一支被推翻后的行为:**只有我方 Close 才终止**。
TEST(CoroSerialTransport, NeverSelfTerminatesWhileDeviceIsMissing) {
  SerialTransport t(ConfigFor(kMissingDevice, 200ms));
  ASSERT_TRUE(t.Start()) << "打开失败被当成了启动失败(D1)";
  auto rx = t.AsyncRead();
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); },
                             1000))
      << "打不开设备却没留下诊断事实";

  boost::this_fiber::sleep_for(1000ms);  // 数个 silence_timeout(≈5 轮退避重试)。

  EXPECT_TRUE(t.IsRunning()) << "串口自终了(D1 要求不自终)";
  EXPECT_EQ(t.LastError(), make_error_code(TransportErrc::kConnection));
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  auto got = AwaitRead(rx, 50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kTimeout))
      << "读队列被提前终结(应只有我方 Close 才终止)";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
}

// —— 7. `Close()` 的四处打断(D6 的回归证据)————————————————————————————

// 【打断之一:管理泵停在**退避**】设备不存在,`open()` 恒失败,泵钉在
// `await_for(close_signal_, 3s)` 上。AC:`close_signal_->close()` 即刻唤醒。
TEST(CoroSerialTransport, CloseDuringBackoffConvergesPromptly) {
  SerialTransport t(ConfigFor(kMissingDevice, 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); },
                             500))
      << "未进入'打不开'的状态,后续断言无意义";
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kDown);
  // 让泵**真的**跑进退避里:`Start()` 就地试开过一次(故 LastError 立刻有值),但泵
  // fiber 此刻可能一步都还没跑——不让出的话本用例测的是"泵还没起就 Close",不是打断。
  boost::this_fiber::sleep_for(100ms);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断重开退避";
}

// 【打断之二:管理泵停在**读等待**】设备打开、对端一字节不发,泵钉在
// `await_for(read_stream_, 3s)` 上。AC:`read_stream_->close()`(以及 `port_->close()`
// 这条串口独有的有效路径,D6)即刻唤醒。
TEST(CoroSerialTransport, CloseDuringReadWaitConvergesPromptly) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }, 500));
  boost::this_fiber::sleep_for(50ms);  // 泵已建流并进入读等待(对端一字节未发)。

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断读等待";
}

// 【打断之三:写泵停在**等数据**】设备打开、一字节不写,写泵钉在 `await(write_queue_)`
// 上——那是**无限期**等待(不是 await_for),漏了 `write_queue_->close()` 就是**永久
// 挂死**,而不只是挂满一个 silence_timeout。AC:关写队列即刻唤醒。
//
// 注:此刻管理泵同时钉在读等待上,故本用例的收敛同样依赖打断之二;两者的区分靠**负向
// 对照**(逐个去掉打断重跑)完成,见文件头的表。
TEST(CoroSerialTransport, CloseWhileWritePumpWaitsForDataConvergesPromptly) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil(
      [&t] { return t.CurrentLinkState() == LinkState::kUp; }, 500));
  boost::this_fiber::sleep_for(50ms);  // 写泵已进到"等数据"里(队列始终为空)。

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断写泵的'等数据'";
}

// 【打断之四:写泵停在**等设备就绪**】设备不存在且**有待发数据**:写泵取到数据后判设备
// 未打开,钉在 `await(device_ready_)` 上——同样是无限期等待。AC:`device_ready_->close()`
// 即刻唤醒(注意管理泵此刻钉在退避上,靠打断之一收敛)。
TEST(CoroSerialTransport, CloseWhileWritePumpWaitsForDeviceConvergesPromptly) {
  SerialTransport t(ConfigFor(kMissingDevice, 3000ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(t.AsyncWrite(Unit("never-sent")))
      << "设备不可用时 AsyncWrite 应照常入队即返";
  boost::this_fiber::sleep_for(100ms);  // 写泵已进到"等设备就绪"里。
  ASSERT_EQ(t.CurrentLinkState(), LinkState::kDown);

  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_LT(Since(began), 500ms) << "Close 未打断写泵的'等设备就绪'";
}

// —— 8. `CurrentLinkState()` 只有两值(D10)————————————————————————————

// AC(**D10**):串口只给 `kDown` / `kUp`,**永不出现 `kEstablishing`**——退避重开期间
// 报 kDown 更诚实(此刻确实收发不了字节);且退避是连接管理策略,不经本查询暴露。
// 这一点与 `UdpTransport` 一致、与 `TcpTransport` 分歧(后者退避重连期间报
// kEstablishing)。
TEST(CoroSerialTransport, LinkStateHasOnlyTwoValues) {
  SerialTransport t(ConfigFor(kMissingDevice, 200ms));
  ASSERT_TRUE(t.Start());
  ASSERT_TRUE(pumpFiberUntil([&t] { return static_cast<bool>(t.LastError()); },
                             1000));

  // 整个退避重开期间**逐次**采样:恒为 kDown,一次 kEstablishing 都不许出现。
  int samples = 0;
  pumpFiberUntil(
      [&] {
        EXPECT_NE(t.CurrentLinkState(), LinkState::kEstablishing)
            << "串口给出了 kEstablishing(D10 只许两值)";
        EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
        return ++samples > 600;  // ≈600ms,跨过数轮退避。
      },
      1500);
  EXPECT_GT(samples, 100) << "采样太少,断言不充分";

  ASSERT_TRUE(t.Close());
  t.WaitClosed();
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);

  // 设备打开时的另一支:kUp。
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport up(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(up.Start());
  EXPECT_EQ(up.CurrentLinkState(), LinkState::kUp);
  ASSERT_TRUE(up.Close());
  up.WaitClosed();
}

// —— 9. 生命周期 ——————————————————————————————————————————————————

// AC(ADR-0007 D4):未 Start 时读句柄以 kInvalidState 关闭、写返 kInvalidState;
// `Close()` / `WaitClosed()` 幂等;从未 Start 也能干净收敛。
TEST(CoroSerialTransport, LifecycleBeforeStartAndIdempotentClose) {
  SerialTransport t(ConfigFor(kMissingDevice, 1000ms));
  auto handle = t.AsyncRead();
  auto got = AwaitRead(handle, 0ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kInvalidState));
  auto written = t.AsyncWrite(Unit("too-early"));
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(t.Close());  // 从未 Start:无泵可停。
  ASSERT_TRUE(t.Close());  // 幂等。
  t.WaitClosed();
  t.WaitClosed();  // 不得挂死。
}

// AC:`Close()` 关 read_queue 并携带终止原因,在途的读随即得到 kClosed;`WaitClosed()`
// 返回后 Start 不再受理、写返 kClosed,且析构安全。
TEST(CoroSerialTransport, CloseTerminatesQueuesAndBlocksRestart) {
  PtyPair pty;
  ASSERT_TRUE(pty.ok());
  SerialTransport t(ConfigFor(pty.slave(), 3000ms));
  ASSERT_TRUE(t.Start());
  auto rx = t.AsyncRead();

  ASSERT_TRUE(t.Close());
  auto closing = t.AsyncWrite(Unit("too-late"));  // Closing。
  ASSERT_FALSE(closing);
  EXPECT_EQ(closing.error(), make_error_code(TransportErrc::kClosed));
  auto got = AwaitRead(rx, 1000ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));

  t.WaitClosed();
  EXPECT_FALSE(t.IsRunning());
  EXPECT_EQ(t.CurrentLinkState(), LinkState::kDown);
  auto restarted = t.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
  // 析构在此处发生:`WaitClosed()` 已 join 两条泵,故可安全释放。
}
