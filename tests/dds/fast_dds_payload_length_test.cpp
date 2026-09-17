// -----------------------------------------------------------------------------
// fast_dds_payload_length_test.cpp — **能看见 #258 的那条用例**(ADR-0023 **D4**)。
// 只在 TRANSPORT_HAS_FASTDDS 时编入(CMakeLists / qmake/tests/tests.pro)。
//
// ⭐ **既有的 `dds_node_fastdds_e2e_test` 对这个缺陷完全盲**,两处叠加:
//   ① 两端在同一进程,Fast DDS 默认 `INTRAPROCESS_FULL`,同进程交付**根本不走序列化**
//      (ADR-0013 **D3**),故不产生 RTPS 的 4 字节对齐填充;
//   ② 那两条用例的样本长度**恰好是 4 的倍数**,即便走了序列化也看不出差别。
//   缺陷之所以活到今天,正是因为测试形态对它盲——**本文件补的就是这一口**。
//
// 本文件的每条用例都同时满足 **D4** 的三个条件:
//   | 强制走序列化 | `IntraprocessOff` 把库设置压成 `INTRAPROCESS_OFF`(线缆用例);
//   |              | 或直接调 `FastDdsRawType` 并**手工模拟** RTPS 的对齐填充(类型用例)。
//   | 长度非 4 的倍数 | `kOddSizes` = {1, 2, 3, 5, 9, 63},覆盖四种对齐余数;9 即
//   |                | #258 现场那条 `"echo:ping"`。
//   | 逐字节 + 长度都断言 | 只比前缀会漏掉**尾部多出的零**——那正是本缺陷的形态。
//
// ⚠ **样本一律以 `0x00` 结尾**:合法 payload 本来就可以以零结尾,故"剥掉尾部零字节"
//   不可行(ADR-0023 备选方案)。这些用例同时钉死了那条路。
//
// ⚠ **`set_library_settings` 是进程级设置**,且 Fast DDS 要求调用时**没有已 enable 的
//   participant**。故 `IntraprocessOff` 写成 RAII:进作用域压成 `OFF`,出作用域**原样
//   还原**,同一进程内其它 DDS 用例的行为一个字不改。
//
// domain 取 96 / 97,与 `fast_dds_provider_test`(88–93)、`dds_node_fastdds_e2e_test`
// (94 / 95)错开,避免同一台机器上互串。
// -----------------------------------------------------------------------------
#include "io/dds/FastDdsProvider.hpp"
#include "io/dds/FastDdsRawType.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fastdds/LibrarySettings.hpp>
#include <fastdds/dds/core/ReturnCode.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "message_test_util.hpp"
#include "transport/codec/DdsCodec.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/DdsNode.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FastDdsProvider;
using transport::FastDdsRawType;
using transport::LinkState;
using transport::Message;
using transport::MessageKind;
using transport::RawBytes;

// 别名取 `fdds` / `frtps`:`dds` / `rtps` 这两个名字与 Fast DDS 自己的命名空间
// 在全局作用域相撞(`eprosima::fastdds::dds` 已由其头文件引入可见)。
namespace fdds = eprosima::fastdds::dds;
namespace frtps = eprosima::fastdds::rtps;

namespace {

using Clock = std::chrono::steady_clock;

/// 非 4 倍数的多档长度,覆盖 `len % 4 ∈ {1, 2, 3}` 三种余数各至少一次。
/// 63 让"填充 1 字节"也走一遍较大的样本;9 即 #258 现场的 `"echo:ping"`。
constexpr std::size_t kOddSizes[] = {1, 2, 3, 5, 9, 63};

/// 本类型不经 CDR 封装,`DataRepresentationId_t` 对它无意义;取默认值以示意。
constexpr fdds::DataRepresentationId_t kRepr = fdds::DEFAULT_DATA_REPRESENTATION;

int MsSince(Clock::time_point t) {
  return static_cast<int>(
      std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t).count());
}

/// 长度为 `n` 的可判别样本:**末字节恒为 `0x00`**(见文件头的"尾部零"一条),
/// 其余字节各不相同,任何一处错位都能被逐字节比较逮住。
std::vector<std::uint8_t> Pattern(std::size_t n) {
  std::vector<std::uint8_t> bytes(n);
  for (std::size_t i = 0; i < n; ++i) {
    bytes[i] = static_cast<std::uint8_t>((i * 7u + 1u) & 0xFFu);
  }
  if (n > 0) bytes[n - 1] = 0x00;
  return bytes;
}

/// RTPS 对 DATA 子消息序列化载荷的 **4 字节对齐**:补零,并把 `length` 一并撑大——
/// 接收侧 `SerializedPayload_t::length` 反映的正是**含填充**的长度,精确长度就是在这里
/// 丢掉的(ADR-0023「机理 ①」)。类型层用例用它把线缆那一步**原地复现**。
void SimulateRtpsAlignmentPadding(frtps::SerializedPayload_t& payload) {
  const std::uint32_t padded = (payload.length + 3u) & ~3u;
  for (std::uint32_t i = payload.length; i < padded; ++i) payload.data[i] = 0x00;
  payload.length = padded;
}

/// `set_library_settings(INTRAPROCESS_OFF)` 的 RAII 包装:**强制走序列化**(D4 条件一)。
///
/// 进程级设置,故出作用域必还原。Fast DDS 要求调用时没有已 enable 的 participant,
/// 因此**必须在建出任何 participant 之前构造**;`ok()` 为假即说明这条前提没满足,
/// 用例当场 fail 而不是默默退化成"其实没关掉 intraprocess"的假绿。
class IntraprocessOff {
 public:
  IntraprocessOff() {
    auto* factory = fdds::DomainParticipantFactory::get_instance();
    if (factory->get_library_settings(saved_) != fdds::RETCODE_OK) return;
    eprosima::fastdds::LibrarySettings off = saved_;
    off.intraprocess_delivery = eprosima::fastdds::INTRAPROCESS_OFF;
    ok_ = factory->set_library_settings(off) == fdds::RETCODE_OK;
    restore_ = ok_;
  }

  ~IntraprocessOff() {
    if (!restore_) return;
    (void)fdds::DomainParticipantFactory::get_instance()->set_library_settings(saved_);
  }

  IntraprocessOff(const IntraprocessOff&) = delete;
  IntraprocessOff& operator=(const IntraprocessOff&) = delete;

  /// 是否真的压成了 `INTRAPROCESS_OFF`(假 ⇒ 调用时已有 enable 的 participant)。
  [[nodiscard]] bool ok() const { return ok_; }

 private:
  eprosima::fastdds::LibrarySettings saved_{};
  bool ok_ = false;
  bool restore_ = false;
};

/// Init 失败也要能收拾干净:RAII 保证 --gtest_repeat 下不留 participant。
struct Pair {
  FastDdsProvider tx, rx;
  ~Pair() { tx.Shutdown(); rx.Shutdown(); }
};

DdsConfig Cfg(int domain) {
  DdsConfig c;
  c.domain_id = domain;
  c.provider = "fastdds";
  c.qos.history_depth = 10;
  c.qos.max_blocking_time = 200ms;
  c.qos.liveliness_lease = 1000ms;
  return c;
}

/// 等到 provider 至少匹配上一个对端(发现窗口约 240ms,ADR-0013 D9);超时返 false。
bool WaitMatched(const FastDdsProvider& p, std::chrono::milliseconds budget = 10s) {
  auto t0 = Clock::now();
  while (MsSince(t0) < budget.count()) {
    if (p.MatchedCount().matched > 0) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

}  // namespace

// ============================ 一、类型层:不起 DDS ============================
// 这一组**直接调 `FastDdsRawType`**,用 `SimulateRtpsAlignmentPadding` 原地复现线缆那一
// 步。它不受发现窗口/调度影响,是本缺陷最硬的那道闸:线缆用例若因环境退化而变绿,这一
// 组照样咬住。

// 线缆布局就是 `[len:4 BE][payload]`——前缀**是大端**,与 `DdsCodec` 的
// `corr_len:2 BE` / `reply_len:2 BE` 同一字节序(ADR-0023 D1)。
TEST(FastDdsRawTypePrefix, SerializeWritesBigEndianLengthPrefix) {
  FastDdsRawType type;
  RawBytes msg;
  msg.payload = Pattern(258);  // 258 = 0x0102:高低两个字节都非零,大小端一眼可辨。

  frtps::SerializedPayload_t payload(type.calculate_serialized_size(&msg, kRepr));
  ASSERT_TRUE(type.serialize(&msg, payload, kRepr));

  EXPECT_EQ(payload.length, 4u + 258u);
  EXPECT_EQ(payload.data[0], 0x00);
  EXPECT_EQ(payload.data[1], 0x00);
  EXPECT_EQ(payload.data[2], 0x01);  // 大端:高位在前。
  EXPECT_EQ(payload.data[3], 0x02);
  EXPECT_EQ(std::vector<std::uint8_t>(payload.data + 4, payload.data + payload.length),
            msg.payload);
}

// `calculate_serialized_size` 必须把前缀算进去,否则 payload 缓冲区差 4 字节。
TEST(FastDdsRawTypePrefix, CalculateSerializedSizeCountsPrefix) {
  FastDdsRawType type;
  for (std::size_t n : kOddSizes) {
    RawBytes msg;
    msg.payload = Pattern(n);
    EXPECT_EQ(type.calculate_serialized_size(&msg, kRepr), 4u + n) << "size=" << n;
  }
  RawBytes empty;
  EXPECT_EQ(type.calculate_serialized_size(&empty, kRepr), 4u);
}

// ⭐ **本票的核心断言**:`payload.length` 被 RTPS 的对齐填充撑大之后,`deserialize`
// 仍须交出**长度、内容都逐字节一致**的原样字节。
//
// 还原 D1(`deserialize` 改回 `assign(data, data + payload.length)`)时,本条对每个
// 非 4 倍数的长度都会失败:收到的 vector 比原样长 1–3 个零字节。
TEST(FastDdsRawTypePrefix, DeserializeIgnoresRtpsAlignmentPadding) {
  FastDdsRawType type;
  // 0 与 4 的倍数一并跑:证明修复没把"本来就对"的那些弄坏。
  for (std::size_t n : {std::size_t{0}, std::size_t{1}, std::size_t{2}, std::size_t{3},
                        std::size_t{4}, std::size_t{5}, std::size_t{8}, std::size_t{9},
                        std::size_t{63}, std::size_t{64}}) {
    SCOPED_TRACE("payload size = " + std::to_string(n));
    RawBytes sent;
    sent.payload = Pattern(n);

    // 缓冲区按对齐后的上限要,好让填充有地方写(真实 RTPS 的缓冲同样是对齐的)。
    const std::uint32_t exact = type.calculate_serialized_size(&sent, kRepr);
    frtps::SerializedPayload_t payload((exact + 3u) & ~3u);
    ASSERT_TRUE(type.serialize(&sent, payload, kRepr));
    ASSERT_EQ(payload.length, exact);  // 发送侧写的是**精确值**,信息不在这里丢。

    SimulateRtpsAlignmentPadding(payload);  // ← 线缆这一步:length 被撑大。

    RawBytes got;
    ASSERT_TRUE(type.deserialize(payload, &got));
    EXPECT_EQ(got.payload.size(), n);       // 长度断言:尾部多出的零就栽在这里。
    EXPECT_EQ(got.payload, sent.payload);   // 逐字节断言。
  }
}

// D3:长度不够装下前缀 ⇒ 返 `false`(Fast DDS 丢弃该 sample),**不读越界**。
TEST(FastDdsRawTypePrefix, DeserializeRejectsSampleShorterThanPrefix) {
  FastDdsRawType type;
  frtps::SerializedPayload_t payload(16);
  for (std::uint32_t len : {0u, 1u, 2u, 3u}) {
    SCOPED_TRACE("payload.length = " + std::to_string(len));
    for (std::uint32_t i = 0; i < len; ++i) payload.data[i] = 0xFF;
    payload.length = len;
    RawBytes got;
    got.payload = {0xAA};  // 失败路径**不得**改动出参之外的东西,这里只求返 false。
    EXPECT_FALSE(type.deserialize(payload, &got));
  }
}

// D3:前缀声称的长度越过 `payload.length` ⇒ 返 `false`。这正是**不带前缀的旧版本对端**
// 发来的样本会落到的分支(ADR-0023「代价 1」:线缆不兼容,两端必须一起升级),
// 也是损坏样本的兜底——**明确失败,而不是读越界**。
TEST(FastDdsRawTypePrefix, DeserializeRejectsLengthPrefixBeyondPayload) {
  FastDdsRawType type;
  frtps::SerializedPayload_t payload(32);
  payload.data[0] = 0x00;
  payload.data[1] = 0x00;
  payload.data[2] = 0x00;
  payload.data[3] = 0x09;  // 声称 9 字节……
  payload.length = 4 + 8;  // ……实际只有 8。
  RawBytes got;
  EXPECT_FALSE(type.deserialize(payload, &got));

  payload.length = 4 + 9;  // 刚好装得下 ⇒ 放行(边界取 `<=`,不是 `<`)。
  EXPECT_TRUE(type.deserialize(payload, &got));
  EXPECT_EQ(got.payload.size(), 9u);
}

// ======================= 二、线缆层:真的走一遍序列化 =======================

// ⭐ provider 层跨越真实 RTPS 线缆:`INTRAPROCESS_OFF` ⇒ 样本必经 serialize /
// deserialize,4 字节对齐填充**真的会发生**。六档非 4 倍数长度各跑一遍。
//
// 一次发现窗口跑完全部长度(而不是每档各建一对 participant):既快,也让"第 n 条样本"
// 的顺序性一并受检(RELIABLE + history_depth 10)。
TEST(FastDdsSerializedWire, OddLengthPayloadsSurviveRtpsAlignmentPadding) {
  IntraprocessOff serialization_forced;  // ← 必须在任何 participant 之前构造。
  ASSERT_TRUE(serialization_forced.ok())
      << "set_library_settings(INTRAPROCESS_OFF) 失败:调用时已有 enable 的 participant。"
         "本用例若在 INTRAPROCESS_FULL 下跑就根本不走序列化,会假绿——故直接判失败。";

  Pair p;
  ASSERT_TRUE(static_cast<bool>(p.tx.Init(Cfg(96))));
  ASSERT_TRUE(static_cast<bool>(p.rx.Init(Cfg(96))));

  std::mutex mu;
  std::vector<std::vector<std::uint8_t>> received;
  ASSERT_TRUE(static_cast<bool>(
      p.rx.Subscribe("odd-length", [&](const std::vector<std::uint8_t>& bytes) {
        std::lock_guard<std::mutex> lock(mu);
        received.push_back(bytes);
      })));
  ASSERT_TRUE(static_cast<bool>(p.tx.DeclareWriter("odd-length")));
  ASSERT_TRUE(WaitMatched(p.tx)) << "发布侧一直没匹配上对端";

  std::size_t expected_count = 0;
  for (std::size_t n : kOddSizes) {
    SCOPED_TRACE("payload size = " + std::to_string(n));
    const std::vector<std::uint8_t> sent = Pattern(n);
    ASSERT_TRUE(static_cast<bool>(p.tx.Publish("odd-length", sent)));
    ++expected_count;

    auto t0 = Clock::now();
    for (;;) {
      {
        std::lock_guard<std::mutex> lock(mu);
        if (received.size() >= expected_count) break;
      }
      ASSERT_LT(MsSince(t0), 10000) << "样本没在预算内到达";
      std::this_thread::sleep_for(5ms);
    }

    std::lock_guard<std::mutex> lock(mu);
    const std::vector<std::uint8_t>& got = received[expected_count - 1];
    // ⭐ 长度与内容**都**断言:只比前缀会漏掉尾部多出的那 1–3 个零字节。
    EXPECT_EQ(got.size(), n);
    EXPECT_EQ(got, sent);
  }
}

// ⭐ 整条链路 `DdsNode` → `DdsCodec` → `FastDdsProvider` 上的同一件事——#258 的现场形态:
// 头部 43 + payload 9 = 52,整帧被 RTPS 补到 56,那 4 个填充字节此前被当成了 payload,
// 宿主拿到 `"echo:ping\0\0\0\0"`。
//
// 与 `dds_node_fastdds_e2e_test` 的两条用例形态相同,**只差两处**:这里 `INTRAPROCESS_OFF`
// 强制走序列化,且 payload 长度不是 4 的倍数——正是那两处让既有用例对本缺陷全盲。
TEST(FastDdsSerializedWire, DdsNodePublishSubscribePreservesOddLengthPayload) {
  IntraprocessOff serialization_forced;
  ASSERT_TRUE(serialization_forced.ok())
      << "set_library_settings(INTRAPROCESS_OFF) 失败:调用时已有 enable 的 participant。";

  DdsTransport publisher_transport(Cfg(97));
  DdsTransport subscriber_transport(Cfg(97));
  auto publisher = std::make_unique<DdsNode>(publisher_transport,
                                             std::make_unique<DdsCodec>());
  auto subscriber = std::make_unique<DdsNode>(subscriber_transport,
                                              std::make_unique<DdsCodec>());

  // 析构序:先收敛节点,再关传输(与 dds_node_fastdds_e2e_test 的 Host 同规矩)。
  struct Closer {
    DdsTransport& a;
    DdsTransport& b;
    std::unique_ptr<DdsNode>& na;
    std::unique_ptr<DdsNode>& nb;
    ~Closer() {
      na.reset();
      nb.reset();
      (void)a.Close();
      a.WaitClosed();
      (void)b.Close();
      b.WaitClosed();
    }
  } closer{publisher_transport, subscriber_transport, publisher, subscriber};

  ASSERT_TRUE(static_cast<bool>(publisher_transport.Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber_transport.Start()));
  ASSERT_TRUE(static_cast<bool>(publisher->RegisterPublishers({"odd.news"})));
  ASSERT_TRUE(static_cast<bool>(subscriber->RegisterSubscribers({"odd.news"})));
  ASSERT_TRUE(static_cast<bool>(publisher->Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber->Start()));

  auto ticket = subscriber->Subscribe(std::string("odd.news"), MessageKind::kNotify);
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  DdsNode::Ticket mailbox = std::move(ticket).value();

  ASSERT_TRUE(pumpFiberUntil(
      [&] { return publisher_transport.CurrentLinkState() == LinkState::kUp; }, 10000))
      << "发布侧一直没匹配上对端";
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return subscriber_transport.CurrentLinkState() == LinkState::kUp; }, 10000))
      << "订阅侧一直没匹配上对端";

  // 9 字节,且**末字节为 `0x00`**——#258 现场的长度,外加"合法 payload 可以以零结尾"。
  const std::vector<std::uint8_t> sent = Pattern(9);
  Message out;
  out.payload = testutil::Pay(sent);
  out.endpoint = Endpoint::Topic("odd.news");
  ASSERT_TRUE(static_cast<bool>(publisher->Publish(std::move(out))));

  auto got = mailbox.Wait(10000ms);
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  // ⭐ 长度先断言:缺陷在时这里是 13 != 9(尾部 4 个 `\0`)。
  EXPECT_EQ(static_cast<std::size_t>(got.value().payload.size()), sent.size());
  EXPECT_EQ(testutil::ToVec(got.value().payload), sent);  // 逐字节。
}
