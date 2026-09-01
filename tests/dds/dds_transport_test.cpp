// -----------------------------------------------------------------------------
// dds_transport_test.cpp — 协程原生 DdsTransport 双队列样板测试(ADR-0013
// D1/D2/D3/D9/D11/D12/D15)。在 fiber 调度器(coro_test_main)内跑。
//
// 确定化手段:被测传输的 provider 由**每个用例自己注册的一条独立 Fake 总线**给出——
// `DdsProviderRegistry` 是进程级的静态表,故每个 fixture 生成一个唯一名字并绑一条新
// `Bus`,`--gtest_repeat` 多轮之间不串。对端用一个直接挂在同一条总线上的裸
// `FakeDdsProvider`,它的 `Publish` 是**同步分发**的,故"发布方线程 = 订阅回调线程"这一
// 条与 Fast DDS 默认 `INTRAPROCESS_FULL` 的实测形态一致,写侧的两个契约(入队即返 /
// 关闭要等在途写)可以在 Fake 上如实复现。
//
// 覆盖九组事实:
//   1. 配置校验(**D12**):五类非法配置 → kConfiguration 且**停在 Created**,改配可再 Start;
//   2. 读侧(**D2**):listener 直推,`peer` = `Endpoint::Topic(来源 topic)`,多 topic 各自带出;
//   3. ⭐ `read_queue_` 溢出(**D11**):有界 1024 + **静默丢最旧**,且 listener **从不阻塞**;
//   4. 写侧(**D3**):`AsyncWrite` **入队即返**——provider 阻塞 400ms 也不拖住调用方;
//   5. 写侧目的地:非 `kTopic` 的 `peer` **不回传错误**,只落 `LastError()`(kInvalidArgument);
//   6. `Declare*` **幂等**(**D15**):重复声明 reader 不会重复投递;
//   7. `CurrentLinkState()` **三态**(**D9**):kDown / kEstablishing / kUp 逐条;
//   8. ⭐ 关闭路径(「明确接受的代价」7):`Close()` **打不断**在途 `Publish`,
//      `WaitClosed()` 得等它自己跑完——最坏等待由**那次回调多慢**决定,不是 max_blocking_time;
//   9. 生命周期:未 Start / 已 Close 的读写、`Close()` 与 `WaitClosed()` 幂等。
//
// 第 3 与第 8 组是本文件的核心。
// -----------------------------------------------------------------------------
#include "transport/io/dds/DdsTransport.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsProviderRegistry.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/dds/IDdsProvider.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::DdsConfig;
using transport::DdsMatchedCount;
using transport::DdsProviderRegistry;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::IDdsProvider;
using transport::LinkState;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

using Bytes = std::vector<std::uint8_t>;
using Clock = std::chrono::steady_clock;

int NextFixtureId() {
  static std::atomic<int> counter{0};
  return ++counter;
}

std::chrono::milliseconds ElapsedSince(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                               start);
}

Bytes Enc(int value) {
  return {static_cast<std::uint8_t>(value & 0xFF),
          static_cast<std::uint8_t>((value >> 8) & 0xFF)};
}
int Dec(const Bytes& bytes) {
  return static_cast<int>(bytes[0]) | (static_cast<int>(bytes[1]) << 8);
}

// 一条独立的 Fake 总线 + 一个只属于本用例的 provider 注册名。
struct Fixture {
  std::shared_ptr<FakeDdsProvider::Bus> bus =
      std::make_shared<FakeDdsProvider::Bus>();
  std::string provider_name = "fake-bus-" + std::to_string(NextFixtureId());

  Fixture() {
    DdsProviderRegistry::RegisterProvider(provider_name, [bus = bus] {
      return std::unique_ptr<IDdsProvider>(new FakeDdsProvider(bus));
    });
  }

  DdsConfig Cfg() const {
    DdsConfig config;
    config.provider = provider_name;
    return config;
  }

  std::unique_ptr<DdsTransport> MakeTransport() const {
    return std::make_unique<DdsTransport>(Cfg());
  }
};

// 挂在同一条总线上的对端:发样本给被测传输,或收被测传输发出的样本。
// 回调可能跑在**被测传输的写线程**上(Fake 同步分发),故收到的东西一律加锁存。
class Peer {
 public:
  Peer(const Fixture& fixture) : provider_(fixture.bus) {
    (void)provider_.Init(fixture.Cfg());
  }
  ~Peer() { provider_.Shutdown(); }

  Peer(const Peer&) = delete;
  Peer& operator=(const Peer&) = delete;

  void Publish(const std::string& topic, const Bytes& bytes) {
    EXPECT_TRUE(static_cast<bool>(provider_.Publish(topic, bytes)));
  }

  void Watch(const std::string& topic) {
    EXPECT_TRUE(static_cast<bool>(
        provider_.Subscribe(topic, [this](const Bytes& bytes) {
          std::lock_guard<std::mutex> lock(mutex_);
          received_.push_back(bytes);
        })));
  }

  std::size_t Count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return received_.size();
  }
  std::vector<Bytes> Received() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return received_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<Bytes> received_;
  FakeDdsProvider provider_;
};

// 会把发布线程 park 住的 provider——复现「`Publish` 的阻塞是线程级、且没有上界」这一
// 实测事实(ADR-0013 背景:同进程订阅回调跑在发布线程上,`max_blocking_time` 不参与)。
// 状态放在共享块里,使工厂造出的实例与用例共用同一份计数。
struct BlockingState {
  std::atomic<int> entered{0};    ///< 已进入 Publish 的次数。
  std::atomic<int> completed{0};  ///< **跑完**的次数(用来证明它没被截断)。
  std::chrono::milliseconds delay{400};
};

class BlockingPublishProvider : public IDdsProvider {
 public:
  explicit BlockingPublishProvider(std::shared_ptr<BlockingState> state)
      : state_(std::move(state)) {}

  Coro::Result<void> Init(const DdsConfig&) override {
    return Coro::Result<void>{};
  }
  void Shutdown() override {}
  Coro::Result<void> Publish(const std::string&, const Bytes&) override {
    state_->entered.fetch_add(1);
    std::this_thread::sleep_for(state_->delay);  // 打不断的一段(同真实 write())。
    state_->completed.fetch_add(1);
    return Coro::Result<void>{};
  }
  Coro::Result<void> Subscribe(const std::string&,
                               std::function<void(const Bytes&)>) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Unsubscribe(const std::string&) override {
    return Coro::Result<void>{};
  }
  [[nodiscard]] DdsMatchedCount MatchedCount() const override { return {}; }
  std::string Name() const override { return "blocking-publish"; }

 private:
  std::shared_ptr<BlockingState> state_;
};

// 注册一个 BlockingPublishProvider 工厂,返回配置 + 共享状态。
std::pair<DdsConfig, std::shared_ptr<BlockingState>> MakeBlockingConfig() {
  auto state = std::make_shared<BlockingState>();
  DdsConfig config;
  config.provider = "blocking-publish-" + std::to_string(NextFixtureId());
  DdsProviderRegistry::RegisterProvider(config.provider, [state] {
    return std::unique_ptr<IDdsProvider>(new BlockingPublishProvider(state));
  });
  return {config, state};
}

}  // namespace

// ── 1. 配置校验(D12):非法 → kConfiguration 且停在 Created ──────────────

TEST(DdsTransport, InvalidConfigIsConfigurationAndStaysCreated) {
  Fixture fixture;

  const auto expect_rejected = [&fixture](DdsConfig config) {
    DdsTransport transport(std::move(config));
    auto started = transport.Start();
    ASSERT_FALSE(static_cast<bool>(started));
    EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
    // **停在 Created**:读句柄仍报 kInvalidState(而不是 kClosed),写同理。
    EXPECT_FALSE(transport.IsRunning());
    EXPECT_EQ(testutil::ReadOnce(transport, 50ms).error(),
              make_error_code(TransportErrc::kInvalidState));
    EXPECT_EQ(transport.AsyncWrite(Datagram{}).error(),
              make_error_code(TransportErrc::kInvalidState));
    EXPECT_EQ(transport.CurrentLinkState(), LinkState::kDown);
  };

  DdsConfig below = fixture.Cfg();
  below.domain_id = -1;
  expect_rejected(below);

  DdsConfig above = fixture.Cfg();
  above.domain_id = 233;  // 合法区间是 [0, 232]。
  expect_rejected(above);

  DdsConfig no_provider = fixture.Cfg();
  no_provider.provider.clear();
  expect_rejected(no_provider);

  DdsConfig unregistered = fixture.Cfg();
  unregistered.provider = "no-such-provider";  // 非空但未注册。
  expect_rejected(unregistered);

  DdsConfig zero_blocking = fixture.Cfg();
  zero_blocking.qos.max_blocking_time = 0ms;  // 须为正,没有"0 = 禁用"这一档。
  expect_rejected(zero_blocking);

  DdsConfig negative_lease = fixture.Cfg();
  negative_lease.qos.liveliness_lease = -1ms;
  expect_rejected(negative_lease);
}

TEST(DdsTransport, StartAfterFixingConfigSucceeds) {
  Fixture fixture;
  DdsConfig bad = fixture.Cfg();
  bad.domain_id = 999;
  DdsTransport transport(bad);
  ASSERT_FALSE(static_cast<bool>(transport.Start()));
  // 停在 Created 的意义就在这里:同一个对象改不了配,但同一份配置改好后可再建再起。
  DdsTransport fixed(fixture.Cfg());
  ASSERT_TRUE(static_cast<bool>(fixed.Start()));
  EXPECT_TRUE(fixed.IsRunning());
  ASSERT_TRUE(static_cast<bool>(fixed.Close()));
  fixed.WaitClosed();
}

TEST(DdsTransport, StartIsIdempotentAndRejectedAfterClose) {
  Fixture fixture;
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  EXPECT_TRUE(static_cast<bool>(transport->Start()));  // Running 时是成功 no-op。
  ASSERT_TRUE(static_cast<bool>(transport->Close()));
  EXPECT_EQ(transport->Start().error(),
            make_error_code(TransportErrc::kInvalidState));
  transport->WaitClosed();
}

// ── 2. 读侧(D2):listener 直推,peer 带出来源 topic ──────────────────────

TEST(DdsTransport, DeclaredReaderDeliversDatagramCarryingSourceTopic) {
  Fixture fixture;
  Peer peer(fixture);
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));

  peer.Publish("t", {1, 2, 3});

  auto datagram = testutil::ReadOnce(*transport, 2000ms);
  ASSERT_TRUE(static_cast<bool>(datagram));
  EXPECT_EQ(datagram.value().bytes, (Bytes{1, 2, 3}));
  // topic **不上线缆**(D5):入站只能靠 listener 闭包捕获的 topic 带出。
  EXPECT_EQ(datagram.value().peer.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(datagram.value().peer.topic, "t");
}

TEST(DdsTransport, MultipleReadersEachCarryOwnTopicAndKeepOrder) {
  Fixture fixture;
  Peer peer(fixture);
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("a")));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("b")));

  for (int i = 0; i < 5; ++i) {
    peer.Publish("a", Enc(i));
    peer.Publish("b", Enc(100 + i));
  }

  std::vector<int> from_a;
  std::vector<int> from_b;
  auto queue = transport->AsyncRead();
  for (int i = 0; i < 10; ++i) {
    auto datagram = testutil::AwaitRead(queue, 2000ms);
    ASSERT_TRUE(static_cast<bool>(datagram));
    if (datagram.value().peer.topic == "a") {
      from_a.push_back(Dec(datagram.value().bytes));
    } else {
      from_b.push_back(Dec(datagram.value().bytes));
    }
  }
  EXPECT_EQ(from_a, (std::vector<int>{0, 1, 2, 3, 4}));
  EXPECT_EQ(from_b, (std::vector<int>{100, 101, 102, 103, 104}));
}

TEST(DdsTransport, ReaderPushFromForeignThreadIsSafeUnderLoad) {
  // 跨线程确证(#190 Q1 的回归):listener 在**非 fiber 线程**上 push,fiber 侧 pop。
  Fixture fixture;
  Peer peer(fixture);
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));

  constexpr int kCount = 800;  // < 1024,fiber 跟得上则无丢弃。
  std::atomic<bool> go{false};
  std::thread publisher([&] {
    while (!go.load()) {
    }
    for (int i = 0; i < kCount; ++i) {
      peer.Publish("t", Enc(i));
    }
  });

  go.store(true);
  std::vector<int> sequence;
  sequence.reserve(kCount);
  auto queue = transport->AsyncRead();
  for (int i = 0; i < kCount; ++i) {
    auto datagram = testutil::AwaitRead(queue, 5000ms);
    ASSERT_TRUE(static_cast<bool>(datagram))
        << "read #" << i << ": " << datagram.error().message();
    sequence.push_back(Dec(datagram.value().bytes));
  }
  publisher.join();

  ASSERT_EQ(sequence.size(), static_cast<std::size_t>(kCount));
  for (int i = 0; i < kCount; ++i) {
    EXPECT_EQ(sequence[i], i);  // 严格 FIFO,无空洞。
  }
}

// ── 3. ⭐ read_queue_ 溢出(D11):有界 1024 + 静默丢最旧,listener 不阻塞 ──

TEST(DdsTransport, ReadQueueOverflowSilentlyDropsOldestAndNeverBlocksListener) {
  Fixture fixture;
  Peer peer(fixture);
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));

  // 一条都不读,连发 1200 条(> 1024)。`Peer::Publish` 内已断言每一条都成功——
  // 队列满**不会**把失败回传给 listener,这正是「`RELIABLE` 被本地队列架空」的形态。
  constexpr int kSent = 1200;
  const auto start = Clock::now();
  for (int i = 0; i < kSent; ++i) {
    peer.Publish("t", Enc(i));
  }
  const auto elapsed = ElapsedSince(start);
  // listener 只做 lock + push_back + notify_all,**无等待路径**:1200 条必须瞬时跑完。
  EXPECT_LT(elapsed.count(), 1000) << "listener 被阻塞了:" << elapsed.count() << "ms";

  // 丢的是**最旧**的那批(不是三介质之外的 tail-drop),且**静默**——没有计数器、
  // 没有归因,唯一可观测的证据就是"取出来的第一条不是 0 号"。
  auto queue = transport->AsyncRead();
  std::vector<int> drained;
  for (;;) {
    auto datagram = testutil::AwaitRead(queue, 200ms);
    if (!datagram) {
      break;  // 取空 → kTimeout。
    }
    drained.push_back(Dec(datagram.value().bytes));
  }
  ASSERT_FALSE(drained.empty());
  EXPECT_EQ(drained.size(), 1024u) << "read_queue_ 的界不是 1024";
  EXPECT_EQ(drained.front(), kSent - 1024);  // 保留最新的 1024 条 ⇒ 首条是 176 号。
  EXPECT_EQ(drained.back(), kSent - 1);
  for (std::size_t i = 1; i < drained.size(); ++i) {
    EXPECT_EQ(drained[i], drained[i - 1] + 1);  // 保留段内部严格连续。
  }
  EXPECT_FALSE(transport->LastError());  // 丢弃**不落 LastError**,与三介质逐字相同。
}

// ── 4/5. 写侧(D3):入队即返;非 topic 目的地只落 LastError ────────────────

TEST(DdsTransport, AsyncWritePublishesToPeerTopic) {
  Fixture fixture;
  Peer peer(fixture);
  peer.Watch("out");
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareWriter("out")));

  ASSERT_TRUE(static_cast<bool>(
      transport->AsyncWrite(Datagram{{9, 8, 7}, Endpoint::Topic("out")})));

  ASSERT_TRUE(pumpFiberUntil([&] { return peer.Count() == 1; }));
  EXPECT_EQ(peer.Received().front(), (Bytes{9, 8, 7}));
  EXPECT_FALSE(transport->LastError());
}

TEST(DdsTransport, AsyncWriteReturnsImmediatelyWhilePublishBlocks) {
  // **写侧的核心契约**(D3):`Publish` park 的是那条专属 OS 线程,业务侧早已返回。
  auto [config, state] = MakeBlockingConfig();
  DdsTransport transport(config);
  ASSERT_TRUE(static_cast<bool>(transport.Start()));
  ASSERT_TRUE(static_cast<bool>(transport.DeclareWriter("t")));

  const auto start = Clock::now();
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(static_cast<bool>(
        transport.AsyncWrite(Datagram{Enc(i), Endpoint::Topic("t")})));
  }
  const auto elapsed = ElapsedSince(start);
  // 三次入队 + 一次 notify,远小于一次 Publish 的 400ms。
  EXPECT_LT(elapsed.count(), 100) << "AsyncWrite 被写侧阻塞拖住了";
  EXPECT_TRUE(pumpFiberUntil([&s = *state] { return s.entered.load() >= 1; }));

  (void)transport.Close();
  transport.WaitClosed();
}

TEST(DdsTransport, NonTopicDestinationIsDroppedIntoLastErrorNotReturned) {
  Fixture fixture;
  Peer peer(fixture);
  peer.Watch("out");
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareWriter("out")));

  // 写是 fire-and-forget:`AsyncWrite` 只判生命周期与入队,故这两条**都返回成功**。
  // DDS 没有"默认对端"(配置里没有 topic,D16),故它们在写线程上被丢弃。
  ASSERT_TRUE(static_cast<bool>(
      transport->AsyncWrite(Datagram{{1}, Endpoint::Default()})));
  ASSERT_TRUE(static_cast<bool>(
      transport->AsyncWrite(Datagram{{2}, Endpoint::Net("127.0.0.1", 5000)})));

  ASSERT_TRUE(pumpFiberUntil([&] {
    return transport->LastError() ==
           make_error_code(TransportErrc::kInvalidArgument);
  }));
  // 一条都没发出去,且随后的合法目的地照常发得出(丢的是那一条,不是整条链路)。
  ASSERT_TRUE(static_cast<bool>(
      transport->AsyncWrite(Datagram{{3}, Endpoint::Topic("out")})));
  ASSERT_TRUE(pumpFiberUntil([&] { return peer.Count() == 1; }));
  EXPECT_EQ(peer.Received().front(), (Bytes{3}));
}

// ── 6. Declare* 幂等(D15)────────────────────────────────────────────────

TEST(DdsTransport, DeclareIsIdempotentAndValidatesTopicAndLifecycle) {
  Fixture fixture;
  Peer peer(fixture);
  auto transport = fixture.MakeTransport();

  // 未 Start:两个方法都 kInvalidState(端点只能在 provider 已 Init 之后建)。
  EXPECT_EQ(transport->DeclareReader("t").error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(transport->DeclareWriter("t").error(),
            make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  EXPECT_EQ(transport->DeclareReader("").error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(transport->DeclareWriter("").error(),
            make_error_code(TransportErrc::kConfiguration));

  // 幂等:注册里可能重复(同一 topic 既是订阅项、又是某条 client 的应答 topic)。
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareWriter("t")));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareWriter("t")));

  // 重复声明**不能**变成两个 reader,否则一条样本会被投递两次。
  peer.Publish("t", {7});
  auto queue = transport->AsyncRead();
  auto first = testutil::AwaitRead(queue, 2000ms);
  ASSERT_TRUE(static_cast<bool>(first));
  EXPECT_EQ(first.value().bytes, (Bytes{7}));
  EXPECT_EQ(testutil::AwaitRead(queue, 200ms).error(),
            make_error_code(TransportErrc::kTimeout));  // 没有第二份。

  ASSERT_TRUE(static_cast<bool>(transport->Close()));
  EXPECT_EQ(transport->DeclareReader("t").error(),
            make_error_code(TransportErrc::kInvalidState));
  transport->WaitClosed();
}

// ── 7. CurrentLinkState 三态(D9)────────────────────────────────────────

TEST(DdsTransport, LinkStateIsDownEstablishingUpByMatchedAndAlive) {
  Fixture fixture;
  auto transport = fixture.MakeTransport();
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kDown);  // 未 Start。

  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  // 已 Running 但一个端点都没声明:此刻确实收发不了字节,报 kDown 更诚实。
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kDown);

  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));
  // 端点已建、还没发现对端 —— 约 240ms 的发现窗口,**无 DDS 原生事件**,由我方推出。
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kEstablishing);

  {
    Peer peer(fixture);
    peer.Watch("t");  // 总线上多了一个别人的 sink ⇒ matched/alive 均 > 0。
    EXPECT_EQ(transport->CurrentLinkState(), LinkState::kUp);
  }
  // 对端退出 ⇒ 回到 kEstablishing(端点还在,只是又没有对端了)。
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kEstablishing);

  ASSERT_TRUE(static_cast<bool>(transport->Close()));
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kDown);  // 关闭中/已关闭。
  transport->WaitClosed();
  EXPECT_EQ(transport->CurrentLinkState(), LinkState::kDown);
}

// ── 8. ⭐ 关闭路径:Close() 打不断在途 Publish ──────────────────────────

TEST(DdsTransport, CloseReturnsAtOnceAndWaitClosedOutlastsInFlightPublish) {
  auto [config, state] = MakeBlockingConfig();
  auto* raw_state = state.get();
  DdsTransport transport(config);
  ASSERT_TRUE(static_cast<bool>(transport.Start()));
  ASSERT_TRUE(static_cast<bool>(transport.DeclareWriter("t")));

  // 三条:第一条会把写线程 park 住,后两条留在队列里。
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(static_cast<bool>(
        transport.AsyncWrite(Datagram{Enc(i), Endpoint::Topic("t")})));
  }
  ASSERT_TRUE(pumpFiberUntil([raw_state] { return raw_state->entered.load() >= 1; }));
  EXPECT_EQ(raw_state->completed.load(), 0);  // 确认此刻确实卡在途中。

  // `Close()` **只发信号**:落在一次阻塞写上也必须当场返回。
  const auto close_start = Clock::now();
  ASSERT_TRUE(static_cast<bool>(transport.Close()));
  const auto close_elapsed = ElapsedSince(close_start);
  EXPECT_LT(close_elapsed.count(), 50) << "Close() 等收敛了:" << close_elapsed.count() << "ms";

  // `WaitClosed()` 才是收敛点,而在途的那次 `Publish` **打不断**——只能等它自己跑完。
  const auto wait_start = Clock::now();
  transport.WaitClosed();
  const auto wait_elapsed = ElapsedSince(wait_start);

  // 它**跑完了**,不是被截断:这正是「最坏等待没有上界」的机理——界由那次调用自己
  // 多久返回决定(真实 DDS 上则由同进程内最慢的那个订阅回调决定)。
  EXPECT_EQ(raw_state->completed.load(), 1);
  // 队列里剩下的两条**随 Close 丢弃**,不等刷出(同三介质)。
  EXPECT_EQ(raw_state->entered.load(), 1);
  // 等待时长下界:至少是这次 Publish 剩余的那一段(留足调度余量,只断言"确实等了")。
  EXPECT_GE(wait_elapsed.count(), 100) << "WaitClosed 没有等在途写";
}

TEST(DdsTransport, CloseAndWaitClosedAreIdempotent) {
  Fixture fixture;
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));

  ASSERT_TRUE(static_cast<bool>(transport->Close()));
  EXPECT_TRUE(static_cast<bool>(transport->Close()));  // 幂等。
  transport->WaitClosed();
  transport->WaitClosed();  // 幂等:join 只做一次。
}

TEST(DdsTransport, CloseBeforeStartClosesReadQueueWithoutHangingWaitClosed) {
  Fixture fixture;
  auto transport = fixture.MakeTransport();
  ASSERT_TRUE(static_cast<bool>(transport->Close()));  // 从未 Start:无线程可停。
  EXPECT_EQ(testutil::ReadOnce(*transport, 200ms).error(),
            make_error_code(TransportErrc::kClosed));
  transport->WaitClosed();  // 必须立即返回。
}

// ── 9. 生命周期:未 Start / 已 Close 的读写 ──────────────────────────────

TEST(DdsTransport, ReadAndWriteBeforeStartAreInvalidStateAfterCloseAreClosed) {
  Fixture fixture;
  auto transport = fixture.MakeTransport();
  EXPECT_EQ(testutil::ReadOnce(*transport, 200ms).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(transport->AsyncWrite(Datagram{{1}, Endpoint::Topic("t")}).error(),
            make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(static_cast<bool>(transport->Start()));
  ASSERT_TRUE(static_cast<bool>(transport->Close()));

  EXPECT_EQ(testutil::ReadOnce(*transport, 200ms).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(transport->AsyncWrite(Datagram{{1}, Endpoint::Topic("t")}).error(),
            make_error_code(TransportErrc::kClosed));
  transport->WaitClosed();
}

TEST(DdsTransport, LateSampleAfterCloseTouchesNothing) {
  Fixture fixture;
  Peer peer(fixture);
  {
    auto transport = fixture.MakeTransport();
    ASSERT_TRUE(static_cast<bool>(transport->Start()));
    ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));
    ASSERT_TRUE(static_cast<bool>(transport->Close()));
    // 迟到样本只会 push 进一条已关闭的队列:listener 捕获的是分发端,不是 `this`。
    peer.Publish("t", {1});
    transport->WaitClosed();
    peer.Publish("t", {2});
  }
  peer.Publish("t", {3});  // 析构之后:provider 已 Shutdown,总线上无该订阅,不崩。
}

// 析构不经 Close/WaitClosed 也必须收敛(析构里自己补上这两步)。
TEST(DdsTransport, DestructorClosesAndJoins) {
  Fixture fixture;
  Peer peer(fixture);
  {
    auto transport = fixture.MakeTransport();
    ASSERT_TRUE(static_cast<bool>(transport->Start()));
    ASSERT_TRUE(static_cast<bool>(transport->DeclareReader("t")));
    ASSERT_TRUE(static_cast<bool>(transport->DeclareWriter("t")));
    ASSERT_TRUE(static_cast<bool>(
        transport->AsyncWrite(Datagram{{5}, Endpoint::Topic("t")})));
  }
  peer.Publish("t", {6});
}
