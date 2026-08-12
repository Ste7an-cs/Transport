// 协程原生 DdsTransport 契约与**跨线程交接边界**测试(ADR-0003 D12 Q1 /
// RT_NODE_004/005/007)。在 fiber 调度器(coro_test_main)内跑:
//   * 收侧:FakeDdsProvider Publish → DdsTransport.Read 收 Datagram{bytes, kTopic(topic)}。
//   * 多 topic 各自到达、同 topic 保接受顺序(FIFO)。
//   * 交接满 → tail-drop 丢最新 + dds_handoff_overflow 计数,listener 不阻塞。
//   * 发侧:Write → provider.Publish 到 destination.topic;非 topic → kInvalidArgument。
//   * Unsubscribe/Close 后迟到样本丢弃、不碰已销毁对象。
//   * ★ 跨线程确证:listener(std::thread)Push、fiber Pop,压测无崩溃/无丢唤醒/无死锁。
//   * I/O 观测面(ADR-0003 D13/RT_NODE_006):Publish 成功记 LastSendTime;出队样本记
//     LastReceiveTime;Publish/Read 操作失败记 LastError。
#include "transport/io/dds/DdsTransport.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/dds/IDdsProvider.hpp"

using namespace std::chrono_literals;
using transport::CapturingTraceSink;
using transport::Datagram;
using transport::DdsConfig;
using transport::DdsTransport;
using transport::DropReason;
using transport::DropReasonName;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::OperationOptions;
using transport::SendUnit;
using transport::Status;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

DdsConfig Cfg() { DdsConfig c; c.domain_id = 0; return c; }

std::vector<std::uint8_t> Enc(int i) {
  return {static_cast<std::uint8_t>(i & 0xFF),
          static_cast<std::uint8_t>((i >> 8) & 0xFF)};
}
int Dec(const std::vector<std::uint8_t>& b) {
  return static_cast<int>(b[0]) | (static_cast<int>(b[1]) << 8);
}

// 以共享 Bus 造被测 DdsTransport(rx)与一个独立发布方 provider(tx)。
struct Fixture {
  std::shared_ptr<FakeDdsProvider::Bus> bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx{bus};

  std::unique_ptr<DdsTransport> MakeRx(std::vector<std::string> topics,
                                       std::size_t max_samples = DdsTransport::kDefaultMaxSamples,
                                       std::size_t max_bytes = DdsTransport::kDefaultMaxBytes,
                                       transport::ITraceSink* trace_sink = nullptr) {
    return std::make_unique<DdsTransport>(std::make_unique<FakeDdsProvider>(bus),
                                          Cfg(), std::move(topics), max_samples,
                                          max_bytes, trace_sink);
  }
  Fixture() { (void)tx.Init(Cfg()); }
};

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = OperationOptions::Clock::now() + d;
  return o;
}

// 最小 IDdsProvider 替身,专供 LastError 测试:Init/Subscribe/Unsubscribe/Shutdown
// 恒成功(不驱动任何真实收发),Publish 恒失败(模拟 provider 侧发布故障)。
using transport::IDdsProvider;
class FailingPublishProvider : public IDdsProvider {
 public:
  Status Init(const DdsConfig&) override { return Status{}; }
  void Shutdown() override {}
  Status Publish(const std::string&, const std::vector<uint8_t>&) override {
    return make_error_code(TransportErrc::kIo);
  }
  Status Subscribe(const std::string&,
                   std::function<void(const std::vector<uint8_t>&)>) override {
    return Status{};
  }
  Status Unsubscribe(const std::string&) override { return Status{}; }
  std::string Name() const override { return "failing-publish"; }
};

}  // namespace

TEST(DdsTransport, PublishToTopicDeliversDatagramWithTopicSource) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  ASSERT_TRUE(static_cast<bool>(f.tx.Publish("t", {1, 2, 3})));

  auto dg = testutil::ReadOnce(*rx, Deadline(2000ms));
  ASSERT_TRUE(static_cast<bool>(dg));
  EXPECT_EQ(dg.value().bytes, (std::vector<std::uint8_t>{1, 2, 3}));
  EXPECT_EQ(dg.value().source.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(dg.value().source.topic, "t");
}

TEST(DdsTransport, MultiTopicEachArrivesAndSameTopicKeepsOrder) {
  Fixture f;
  auto rx = f.MakeRx({"a", "b"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  // 交替发布两 topic,验证各自都到达且同 topic 保接受顺序。
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(static_cast<bool>(f.tx.Publish("a", Enc(i))));
    ASSERT_TRUE(static_cast<bool>(f.tx.Publish("b", Enc(100 + i))));
  }

  std::vector<int> a_seq, b_seq;
  for (int k = 0; k < 10; ++k) {
    auto dg = testutil::ReadOnce(*rx, Deadline(2000ms));
    ASSERT_TRUE(static_cast<bool>(dg));
    if (dg.value().source.topic == "a")
      a_seq.push_back(Dec(dg.value().bytes));
    else
      b_seq.push_back(Dec(dg.value().bytes));
  }
  EXPECT_EQ(a_seq, (std::vector<int>{0, 1, 2, 3, 4}));
  EXPECT_EQ(b_seq, (std::vector<int>{100, 101, 102, 103, 104}));
}

TEST(DdsTransport, HandoffFullTailDropsAndCountsWithoutBlockingListener) {
  Fixture f;
  // 极小交接容量:3 样本(字节上界给足,只让样本数触顶)。
  auto rx = f.MakeRx({"t"}, /*max_samples=*/3, /*max_bytes=*/DdsTransport::kDefaultMaxBytes);
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  // 不读的情况下连发 10 条:满即 tail-drop 丢最新,listener(Publish)不阻塞、始终成功。
  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(static_cast<bool>(f.tx.Publish("t", Enc(i))));  // 不阻塞。
  }
  EXPECT_EQ(rx->DdsHandoffOverflowCount(), 7u);  // 收下前 3,丢后 7。

  // 保留的是最早的 3 条(FIFO,tail-drop 丢新不丢旧)。
  for (int i = 0; i < 3; ++i) {
    auto dg = testutil::ReadOnce(*rx, Deadline(2000ms));
    ASSERT_TRUE(static_cast<bool>(dg));
    EXPECT_EQ(Dec(dg.value().bytes), i);
  }
}

// P5-3(issue #88):kDdsHandoffOverflow 定义点在 BoundedQueue::Push 内(交接边界经
// DdsTransport 构造函数透传可选 trace_sink)——配置 trace_sink 时,交接满 tail-drop
// 应逐条产生可辨识的 TraceEvent,且 DdsHandoffOverflowCount()(代理
// BoundedQueue::DroppedCount())不变。
TEST(DdsTransport, HandoffOverflowWithSinkEmitsDropTraceForEachDrop) {
  Fixture f;
  CapturingTraceSink sink;
  auto rx = f.MakeRx({"t"}, /*max_samples=*/3,
                     /*max_bytes=*/DdsTransport::kDefaultMaxBytes, &sink);
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(static_cast<bool>(f.tx.Publish("t", Enc(i))));
  }
  EXPECT_EQ(rx->DdsHandoffOverflowCount(), 7u);

  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 7u);  // 逐条归因:7 次 tail-drop → 7 条 TraceEvent。
  for (const auto& rec : records) {
    EXPECT_EQ(rec.category, "drop");
    EXPECT_EQ(rec.message, DropReasonName(DropReason::kDdsHandoffOverflow));
  }
}

// RT_TRACE_002:未配置 trace_sink(默认 nullptr)时,交接满 tail-drop 计数不受影响
// (行为与既有 HandoffFullTailDropsAndCountsWithoutBlockingListener 一致,不重复断言细节)。
TEST(DdsTransport, HandoffOverflowNoSinkConfiguredCountUnaffected) {
  Fixture f;
  auto rx = f.MakeRx({"t"}, /*max_samples=*/3);  // trace_sink 缺省 nullptr。
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(static_cast<bool>(f.tx.Publish("t", Enc(i))));
  }
  EXPECT_EQ(rx->DdsHandoffOverflowCount(), 2u);
}

TEST(DdsTransport, WritePublishesToDestinationTopic) {
  Fixture f;
  auto rx = f.MakeRx({"in"});  // 被测对象只订阅 "in"
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  // 独立订阅方监听 "out",验证 Write 发到 destination.topic。
  std::vector<std::uint8_t> got;
  FakeDdsProvider sink{f.bus};
  (void)sink.Init(Cfg());
  ASSERT_TRUE(static_cast<bool>(
      sink.Subscribe("out", [&](const std::vector<std::uint8_t>& b) { got = b; })));

  SendUnit unit;
  unit.bytes = {9, 8, 7};
  unit.destination = Endpoint::Topic("out");
  ASSERT_TRUE(static_cast<bool>(rx->Write(std::move(unit))));
  EXPECT_EQ(got, (std::vector<std::uint8_t>{9, 8, 7}));
}

TEST(DdsTransport, WriteNonTopicDestinationIsInvalidArgument) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  SendUnit def;
  def.bytes = {1};
  def.destination = Endpoint::Default();
  auto r1 = rx->Write(std::move(def));
  EXPECT_EQ(r1.error(), make_error_code(TransportErrc::kInvalidArgument));

  SendUnit net;
  net.bytes = {1};
  net.destination = Endpoint::Net("127.0.0.1", 5000);
  auto r2 = rx->Write(std::move(net));
  EXPECT_EQ(r2.error(), make_error_code(TransportErrc::kInvalidArgument));
}

TEST(DdsTransport, ReadBeforeStartIsInvalidStateAndAfterCloseIsClosed) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  auto before = testutil::ReadOnce(*rx, Deadline(200ms));
  EXPECT_EQ(before.error(), make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(static_cast<bool>(rx->Start()));
  ASSERT_TRUE(static_cast<bool>(rx->RequestClose()));
  auto after = testutil::ReadOnce(*rx, Deadline(200ms));
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
  // WaitClosed 立即完成。
  EXPECT_TRUE(static_cast<bool>(rx->WaitClosed(Deadline(2000ms))));
}

TEST(DdsTransport, LateSampleAfterCloseIsDiscardedAndNoUseAfterFree) {
  Fixture f;
  {
    auto rx = f.MakeRx({"t"});
    ASSERT_TRUE(static_cast<bool>(rx->Start()));
    ASSERT_TRUE(static_cast<bool>(rx->RequestClose()));
    // 关闭后再发:回调已被 Shutdown 摘除;即便迟到回调只 Push 存活的交接队列(返 kClosed)。
    EXPECT_TRUE(static_cast<bool>(f.tx.Publish("t", {1})));
    EXPECT_EQ(rx->DdsHandoffOverflowCount(), 0u);  // 关闭队列 Push 记 kClosed 而非 overflow。
  }
  // 析构后再发:总线上已无该 provider 的订阅,不触碰已销毁对象(无崩溃)。
  EXPECT_TRUE(static_cast<bool>(f.tx.Publish("t", {2})));
}

// —— ★ 跨线程交接确证:listener(std::thread)Push、fiber Pop ——
// FakeDdsProvider.Publish 同步 Dispatch → 订阅回调在**调用线程**触发,故在独立
// std::thread 上 Publish 即在非 fiber 线程上 Push 交接边界。fiber 侧 Pop 等待被
// 该线程唤醒。压测确认无崩溃、无丢唤醒(每次 Read 有界 deadline,丢唤醒会转成
// kTimeout 失败而非无限挂起)、无死锁。--gtest_repeat 多轮加压。
TEST(DdsTransport, CrossThreadListenerPushFiberPopStress) {
  Fixture f;
  constexpr int kN = 800;  // < 默认 1024 交接容量:fiber 跟得上则无丢弃。
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  std::atomic<bool> go{false};
  std::thread listener([&] {
    while (!go.load()) { /* 等 fiber 就绪再开压 */ }
    for (int i = 0; i < kN; ++i) {
      (void)f.tx.Publish("t", Enc(i));  // 非 fiber 线程上触发回调 → Push。
    }
  });

  go.store(true);
  std::vector<int> seq;
  seq.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    auto dg = testutil::ReadOnce(*rx, Deadline(5000ms));
    ASSERT_TRUE(static_cast<bool>(dg))  // 丢唤醒会在此转成 kTimeout。
        << "Read #" << i << " failed: " << dg.error().message();
    seq.push_back(Dec(dg.value().bytes));
  }
  listener.join();

  // 无丢弃(容量足)、严格 FIFO 保接受顺序。
  EXPECT_EQ(rx->DdsHandoffOverflowCount(), 0u);
  ASSERT_EQ(seq.size(), static_cast<std::size_t>(kN));
  for (int i = 0; i < kN; ++i) EXPECT_EQ(seq[i], i);
}

// 跨线程并发 Push 与 Close 竞争:一个 std::thread 持续 Push,fiber 侧 RequestClose,
// 在途 Read 被唤醒返 kClosed,无崩溃/无死锁(确证唤醒原语两路唤醒安全)。
TEST(DdsTransport, CrossThreadCloseRacesWithListenerPush) {
  Fixture f;
  auto rx = f.MakeRx({"t"}, /*max_samples=*/8);
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  std::atomic<bool> stop{false};
  std::thread listener([&] {
    for (int i = 0; !stop.load(); ++i) {
      (void)f.tx.Publish("t", Enc(i & 0xFFFF));
    }
  });

  // 先取几条(fiber 与 listener 真并发),再关闭,验证 Read 收敛到 kClosed。
  for (int k = 0; k < 5; ++k) {
    auto dg = testutil::ReadOnce(*rx, Deadline(5000ms));
    ASSERT_TRUE(static_cast<bool>(dg)) << "drain read failed: " << dg.error().message();
  }
  ASSERT_TRUE(static_cast<bool>(rx->RequestClose()));
  stop.store(true);
  listener.join();

  // 关闭后 Read 返 kClosed(不挂起)。
  auto after = testutil::ReadOnce(*rx, Deadline(2000ms));
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
}

// —— I/O 观测面(ADR-0003 D13/RT_NODE_006「所有介质如实报」)——

TEST(DdsTransport, PublishSuccessUpdatesLastSendTime) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));
  EXPECT_FALSE(rx->LastSendTime().has_value());  // 未发送前为空。

  const auto before = OperationOptions::Clock::now();
  SendUnit unit;
  unit.bytes = {1, 2, 3};
  unit.destination = Endpoint::Topic("out");
  ASSERT_TRUE(static_cast<bool>(rx->Write(std::move(unit))));
  const auto after = OperationOptions::Clock::now();

  ASSERT_TRUE(rx->LastSendTime().has_value());
  EXPECT_GE(*rx->LastSendTime(), before);
  EXPECT_LE(*rx->LastSendTime(), after);
}

TEST(DdsTransport, HandoffSampleUpdatesLastReceiveTime) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));
  EXPECT_FALSE(rx->LastReceiveTime().has_value());  // 未收到前为空。

  const auto before = OperationOptions::Clock::now();
  ASSERT_TRUE(static_cast<bool>(f.tx.Publish("t", {1, 2, 3})));
  auto dg = testutil::ReadOnce(*rx, Deadline(2000ms));
  const auto after = OperationOptions::Clock::now();
  ASSERT_TRUE(static_cast<bool>(dg));

  ASSERT_TRUE(rx->LastReceiveTime().has_value());
  EXPECT_GE(*rx->LastReceiveTime(), before);
  EXPECT_LE(*rx->LastReceiveTime(), after);
}

TEST(DdsTransport, PublishFailureUpdatesLastErrorWithoutTouchingLastSendTime) {
  auto rx = std::make_unique<DdsTransport>(
      std::make_unique<FailingPublishProvider>(), Cfg(),
      std::vector<std::string>{});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));
  EXPECT_FALSE(rx->LastError());  // 初始无错误。

  SendUnit unit;
  unit.bytes = {9};
  unit.destination = Endpoint::Topic("t");
  auto written = rx->Write(std::move(unit));

  ASSERT_FALSE(static_cast<bool>(written));
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kIo));
  EXPECT_EQ(rx->LastError(), make_error_code(TransportErrc::kIo));
  EXPECT_FALSE(rx->LastSendTime().has_value());  // 失败不应记为一次成功发送。
}

TEST(DdsTransport, ReadTimeoutDoesNotUpdateLastError) {
  Fixture f;
  auto rx = f.MakeRx({"t"});
  ASSERT_TRUE(static_cast<bool>(rx->Start()));

  // 空队列、短 deadline:handoff.Pop 必然超时。kTimeout 是正常操作结果(无数据
  // 到达),不是故障事实——同 TCP/UDP/Serial 惯例,不计入 LastError,保持它作为
  // "真故障"信号不被正常控制流结果稀释(ADR-0003 D13、RT_NODE_006)。
  auto dg = testutil::ReadOnce(*rx, Deadline(20ms));
  ASSERT_FALSE(static_cast<bool>(dg));
  EXPECT_EQ(dg.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_FALSE(rx->LastError());
}
