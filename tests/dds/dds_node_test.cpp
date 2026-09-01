// -----------------------------------------------------------------------------
// dds_node_test.cpp — DdsNode 的注册接口 + 两种交互模式(ADR-0013 D5/D6/D7/D8/D12/D15/D16)。
// 在 fiber 调度器(coro_test_main)内跑。
//
// 确定化手段与 dds_transport_test 相同:每个 fixture 生成一个唯一的 provider 注册名并绑
// 一条独立的 Fake 总线,`--gtest_repeat` 多轮之间不串。节点之间**真的经传输 + codec +
// 总线**通信——不打桩 transport,故请求-响应的每一条断言都是端到端的。
//
// 覆盖七组事实:
//   1. 注册接口(**D16**):只在 Created 受理、批量累加、幂等去重、**整批生效或整批不生效**、
//      `Start()` 失败**不清空注册表**;
//   2. 注册期校验:空串 / 键值相同 / **唯一要拦的方向冲突**(Clients 键 ∩ Services 键);
//      其余"同一 topic 既有 writer 又有 reader"的组合**不拦**;
//   3. 启动(**D12**/**D15**):四组全空 → kConfiguration;端点在 `DoStart()` 一次性建出,
//      **服务的第一次应答不会丢**;
//   4. 发布-订阅:`Publish` / `Subscribe(topic, kNotify)` 端到端;调用与注册的对应校验;
//   5. `Subscribe` 返 **`Coro::Result<Ticket>`**(**D8**),topic 传 `kAny` 时**跳过**校验;
//   6. ⭐ 请求-响应(**D7**):单阶段、等结果时重发、耗尽返 **kTimeout**、不回应;
//      corr 两段式且 `uuid_override` 可注入;**重发复用同一份字节**(corr 不变);
//   7. ⭐ 共用应答 topic 靠**内部登记的具体 corr** 区分客户端(**D6** 那处承重的区别);
//      `Reply` 查自己的 `Services` 表、`reply_to` 作一致性交叉校验(**D15**)。
//
// 第 6、7 组是本文件的核心。
// -----------------------------------------------------------------------------
#include "transport/node/DdsNode.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/codec/DdsCodec.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TraceCategories.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsProviderRegistry.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/dds/IDdsProvider.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsProviderRegistry;
using transport::DdsTransport;
using transport::DropReason;
using transport::DropReasonName;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::IDdsProvider;
using transport::ITraceSink;
using transport::kAny;
using transport::Message;
using transport::MessageKind;
using transport::RetryPolicy;
using transport::TraceEvent;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

using Bytes = std::vector<std::uint8_t>;

int NextFixtureId() {
  static std::atomic<int> counter{0};
  return ++counter;
}

/// 一条独立的 Fake 总线 + 一个只属于本用例的 provider 注册名。
struct Fixture {
  std::shared_ptr<FakeDdsProvider::Bus> bus =
      std::make_shared<FakeDdsProvider::Bus>();
  std::string provider_name = "fake-node-bus-" + std::to_string(NextFixtureId());

  Fixture() {
    DdsProviderRegistry::RegisterProvider(provider_name, [bus = bus] {
      return std::unique_ptr<IDdsProvider>(new FakeDdsProvider(bus));
    });
  }

  [[nodiscard]] DdsConfig Cfg() const {
    DdsConfig config;
    config.provider = provider_name;
    return config;
  }
};

/// 只数事件的 Trace 出口:本类没有观测计数器,丢弃归因**只经 sink 一条出口**。
class CountingSink : public ITraceSink {
 public:
  void OnTrace(const TraceEvent& ev) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ev.category == transport::kTraceCategoryDrop) {
      ++drops_[std::string(ev.message)];
    }
  }
  [[nodiscard]] std::size_t Drops(DropReason reason) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = drops_.find(std::string(DropReasonName(reason)));
    return found == drops_.end() ? 0 : found->second;
  }

 private:
  mutable std::mutex mutex_;
  std::map<std::string, std::size_t> drops_;
};

/// 一个"节点 + 它借用的那条传输"的组合体。
///
/// **传输归宿主**(ADR-0009 / ADR-0013 D15 的连带):本件模拟宿主——它建传输、`Start()`
/// 它,把引用交给节点,并在析构里**先收敛节点、再关传输**(顺序不能反:节点借着它)。
class Host {
 public:
  explicit Host(const Fixture& fixture, std::string uuid = {},
                ITraceSink* sink = nullptr)
      : transport_(fixture.Cfg()) {
    DdsNodeConfig config;
    config.uuid_override = std::move(uuid);
    config.trace_sink = sink;
    node_ = std::make_unique<DdsNode>(transport_, std::make_unique<DdsCodec>(),
                                      std::move(config));
  }

  ~Host() {
    node_.reset();  // 节点先收敛(它只关自己那一路读订阅,不触碰传输的生命周期)。
    (void)transport_.Close();
    transport_.WaitClosed();
  }

  Host(const Host&) = delete;
  Host& operator=(const Host&) = delete;

  /// 宿主先把传输启起来——节点 `Start()` 里的 `Declare*` 只能落在已 `Init` 的 provider 上。
  void StartTransport() {
    ASSERT_TRUE(static_cast<bool>(transport_.Start()));
  }

  [[nodiscard]] DdsNode& node() { return *node_; }
  [[nodiscard]] DdsTransport& transport() { return transport_; }

 private:
  DdsTransport transport_;
  std::unique_ptr<DdsNode> node_;  ///< 后声明 ⇒ 先析构,恰是需要的顺序。
};

/// 订阅消费小件:一条**调用方自己的** fiber 顺序消费自己的信箱(ADR-0009 D2 的样板)。
/// 串行、异常隔离与 join 全由调用方负责,节点不代管。
class Subscriber {
 public:
  Subscriber(DdsNode::Ticket ticket, std::function<void(const Message&)> on_message)
      : ticket_(std::move(ticket)), mailbox_(ticket_.mailbox()) {
    task_ = std::make_shared<Coro::FiberTask<void>>(
        Coro::makeTask([this, on_message = std::move(on_message)] {
          for (;;) {
            Coro::Result<Message, std::error_code> msg = Coro::await(mailbox_);
            if (!msg) {
              break;  // 信箱被关(节点 Close 或本件 Join)→ 退出。
            }
            on_message(msg.value());
          }
        }));
  }

  ~Subscriber() { Join(); }

  Subscriber(const Subscriber&) = delete;
  Subscriber& operator=(const Subscriber&) = delete;

  void Join() {
    if (!task_) {
      return;
    }
    mailbox_->close(make_error_code(TransportErrc::kClosed));
    (void)task_->get();  // 让出式 join:返回即消费 fiber 已退出。
    task_.reset();
  }

 private:
  DdsNode::Ticket ticket_;
  std::shared_ptr<Coro::Awaitable<Message>> mailbox_;
  std::shared_ptr<Coro::FiberTask<void>> task_;
};

/// 取一张订阅凭据并断言拿到了(`Subscribe` 返 Result,忘了注册会在这里当场炸)。
DdsNode::Ticket MustSubscribe(DdsNode& node, DdsNode::TopicKey topic,
                              DdsNode::KindKey kind) {
  auto ticket = node.Subscribe(std::move(topic), std::move(kind));
  EXPECT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  return std::move(ticket).value();
}

Message Payload(std::string text) {
  Message msg;
  msg.payload.assign(text.begin(), text.end());
  return msg;
}

std::string Text(const Message& msg) {
  return std::string(msg.payload.begin(), msg.payload.end());
}

constexpr auto kCaseTimeout = 300ms;

}  // namespace

// ── 1. 注册接口:相位、累加、幂等、整批生效(D16)───────────────────────

TEST(DdsNode, RegistrationIsAcceptedOnlyInCreated) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // Created:四个方法都受理。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"pub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"sub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({{"own", "own.reply"}})));

  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // Running:一律 kInvalidState——端点集合"启动即定型、运行期恒定",本设计不引入运行期
  // 动态端点。
  EXPECT_EQ(node.RegisterPublishers({"late"}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterSubscribers({"late"}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterClients({{"late", "late.reply"}}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterServices({{"late", "late.reply"}}).error(),
            make_error_code(TransportErrc::kInvalidState));

  ASSERT_TRUE(static_cast<bool>(node.Close()));
  node.WaitClosed();
  // Closing / Closed 同理。
  EXPECT_EQ(node.RegisterPublishers({"later"}).error(),
            make_error_code(TransportErrc::kInvalidState));
}

TEST(DdsNode, RegistrationAccumulatesAcrossCallsAndDeduplicates) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // 多次调用**累加**;重复项**幂等去重**,不报错。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"a", "b"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"b", "c"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 三个都注册上了才发得出去(未注册为 Publishers 返 kConfiguration)。
  for (const char* topic : {"a", "b", "c"}) {
    EXPECT_TRUE(static_cast<bool>(node.Publish(topic, Payload("x"))));
  }
  EXPECT_EQ(node.Publish("d", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
}

TEST(DdsNode, InvalidItemRollsBackTheWholeBatch) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // 一批里只要有一项非法,**整批回滚、一项都不落**。
  EXPECT_EQ(node.RegisterPublishers({"good", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterSubscribers({"", "good"}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterClients({{"ok", "ok.reply"}, {"bad", "bad"}}).error(),
            make_error_code(TransportErrc::kInvalidArgument));

  // 三批都没落下任何一项 ⇒ 四组仍全空 ⇒ Start 返 kConfiguration(D12)。
  EXPECT_EQ(node.Start().error(), make_error_code(TransportErrc::kConfiguration));
}

TEST(DdsNode, PairRegistrationRejectsSameKeyAndValue) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // 请求与应答同 topic 必然自收自答。
  EXPECT_EQ(node.RegisterClients({{"t", "t"}}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterServices({{"t", "t"}}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // 空串同样不收。
  EXPECT_EQ(node.RegisterClients({{"", "r"}}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterServices({{"q", ""}}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
}

// ⭐ **唯一要拦的方向冲突**:同一 topic 既是 Clients 的键、又是 Services 的键——自己请求
// 自己,且 corr 由自己生成、Dispatcher **会真的匹配上**,形成毫无察觉的自问自答。
TEST(DdsNode, ClientKeyAndServiceKeyOnSameTopicIsRejectedBothOrders) {
  Fixture fixture;
  {
    Host host(fixture);
    host.StartTransport();
    ASSERT_TRUE(static_cast<bool>(
        host.node().RegisterServices({{"svc", "svc.reply"}})));
    EXPECT_EQ(host.node().RegisterClients({{"svc", "svc.reply"}}).error(),
              make_error_code(TransportErrc::kInvalidArgument));
  }
  {
    Host host(fixture);  // 反过来注册,同样拦下——两侧各查对方的表。
    host.StartTransport();
    ASSERT_TRUE(static_cast<bool>(
        host.node().RegisterClients({{"svc", "svc.reply"}})));
    EXPECT_EQ(host.node().RegisterServices({{"svc", "svc.reply"}}).error(),
              make_error_code(TransportErrc::kInvalidArgument));
  }
}

// 其余"同一 topic 上既有 writer 又有 reader"的组合**不拦**:只造成自收白干、不会误配,
// 且可能是调用方有意为之(本地回环自测)。
TEST(DdsNode, OtherWriterReaderOverlapsAreNotRejected) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // Publishers ∩ Subscribers:本地回环自测,合法。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"loop"})));
  // Publishers ∩ Clients 的值:往某应答 topic 发布、同时又是该服务的客户端,合法。
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"svc", "loop"}})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 回环自测确实通:发出去的通知自己收得到。
  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(MustSubscribe(node, std::string("loop"), MessageKind::kNotify),
                 [seen](const Message& msg) { seen->push_back(Text(msg)); });
  ASSERT_TRUE(static_cast<bool>(node.Publish("loop", Payload("echo"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 1; }));
  ASSERT_EQ(seen->size(), 1u);
  EXPECT_EQ(seen->front(), "echo");
}

// ── 2. 启动:四组全空 → kConfiguration,且**失败不清空注册表**(D12/D16)──

TEST(DdsNode, StartWithNoRegistrationIsConfigurationAndKeepsRegistry) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // 一个什么都不收不发的节点必是漏了注册。
  auto empty = node.Start();
  ASSERT_FALSE(static_cast<bool>(empty));
  EXPECT_EQ(empty.error(), make_error_code(TransportErrc::kConfiguration));
  EXPECT_FALSE(node.IsRunning());

  // **停在 Created ⇒ 还能接着注册**——补上漏项再 Start 一次即可,不必把全部注册重做一遍。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"a"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"b"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));
  EXPECT_TRUE(node.IsRunning());
  // 两批分别注册、跨调用累加,启动后都生效。
  EXPECT_TRUE(static_cast<bool>(node.Publish("a", Payload("x"))));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("b"), MessageKind::kNotify)));
}

// 传输还没 Running 就 Start 节点:`Declare*` 返 kInvalidState,本次启动失败并停在
// Created;把传输启起来再 Start 一次即成——**注册表原样保留**,这正是 D16 要的形态。
TEST(DdsNode, StartBeforeTransportIsRunningFailsThenSucceedsAfterRetry) {
  Fixture fixture;
  Host host(fixture);  // 故意**不** StartTransport()
  DdsNode& node = host.node();

  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"a"})));
  EXPECT_EQ(node.Start().error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_FALSE(node.IsRunning());

  host.StartTransport();
  ASSERT_TRUE(static_cast<bool>(node.Start()));  // 注册表没被清掉,一次就成。
  EXPECT_TRUE(node.IsRunning());
  EXPECT_TRUE(static_cast<bool>(node.Publish("a", Payload("x"))));
}

// ── 3. 发布-订阅 + 调用与注册的对应校验(D16)──────────────────────────

TEST(DdsNode, PublishAndSubscribeDeliverAcrossNodes) {
  Fixture fixture;
  Host publisher(fixture);
  Host subscriber(fixture);
  publisher.StartTransport();
  subscriber.StartTransport();

  ASSERT_TRUE(static_cast<bool>(publisher.node().RegisterPublishers({"news"})));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().RegisterSubscribers({"news"})));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().Start()));

  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(
      MustSubscribe(subscriber.node(), std::string("news"), MessageKind::kNotify),
      [seen](const Message& msg) { seen->push_back(Text(msg)); });

  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("news", Payload("one"))));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("news", Payload("two"))));

  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 2; }));
  ASSERT_EQ(seen->size(), 2u);
  EXPECT_EQ((*seen)[0], "one");
  EXPECT_EQ((*seen)[1], "two");
}

TEST(DdsNode, PublishRequiresPublisherRegistrationAndRunning) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"ok"})));

  // 未启动:**调用序错误先于配置错误**——即便 topic 没注册也先报 kClosed。
  EXPECT_EQ(node.Publish("ok", Payload("x")).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Publish("nope", Payload("x")).error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  EXPECT_TRUE(static_cast<bool>(node.Publish("ok", Payload("x"))));
  // **不猜、不回落、不懒补**:忘了注册是显式错误,不是静默无效。
  EXPECT_EQ(node.Publish("nope", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));

  ASSERT_TRUE(static_cast<bool>(node.Close()));
  EXPECT_EQ(node.Publish("ok", Payload("x")).error(),
            make_error_code(TransportErrc::kClosed));
  node.WaitClosed();
}

// ── 4. Subscribe 返 Result(D8);topic 传 kAny 跳过校验(D16)─────────────

TEST(DdsNode, SubscribeReturnsConfigurationWhenTopicNotRegisteredForThatRole) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"news"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"far", "far.reply"}})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 对应关系:kNotify → Subscribers;kRequest → Services 的键。
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("news"), MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("svc"), MessageKind::kRequest)));

  // 角色不对 ⇒ kConfiguration。**错误码就是 kConfiguration**,而不是推迟到第一次 `Wait`
  // 才冒出来的 kInvalidState——这正是"返 Result 而不是裸 Ticket"要拿到的东西(D8)。
  EXPECT_EQ(node.Subscribe(std::string("svc"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Subscribe(std::string("news"), MessageKind::kRequest).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Subscribe(std::string("unknown"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kConfiguration));

  // topic 传 `kAny` ⇒ **跳过该校验**:kAny 不对应任何一个具体 topic,拿它查注册表必然落空。
  EXPECT_TRUE(static_cast<bool>(node.Subscribe(kAny, MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(node.Subscribe(kAny, kAny)));

  // 其余 kind + 具体 topic:至少得在读侧集合内(Subscribers ∪ Services 键 ∪ Clients 值)。
  EXPECT_TRUE(static_cast<bool>(node.Subscribe(std::string("far.reply"), kAny)));
  EXPECT_EQ(node.Subscribe(std::string("far"), kAny).error(),
            make_error_code(TransportErrc::kConfiguration));
}

// `Subscribe(kAny, kNotify)` 的实际语义是「**已注册为 reader 的 topic 的全部**」,
// 不是"本 domain 上的全部"——未注册的 topic 其消息根本不会到达本进程。
TEST(DdsNode, SubscribeAnyTopicCoversOnlyRegisteredReaderTopics) {
  Fixture fixture;
  Host publisher(fixture);
  Host subscriber(fixture);
  publisher.StartTransport();
  subscriber.StartTransport();

  ASSERT_TRUE(static_cast<bool>(publisher.node().RegisterPublishers({"in", "out"})));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().RegisterSubscribers({"in"})));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().Start()));

  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(MustSubscribe(subscriber.node(), kAny, MessageKind::kNotify),
                 [seen](const Message& msg) { seen->push_back(msg.topic); });

  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("out", Payload("x"))));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("in", Payload("y"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return !seen->empty(); }));
  // 只收得到 "in"——"out" 上本节点没建 reader,那条样本压根不进本进程。
  EXPECT_EQ(seen->size(), 1u);
  EXPECT_EQ(seen->front(), "in");
}

// ── 5. ⭐ 请求-响应:单阶段、corr 两段式、重发、耗尽 kTimeout(D6/D7)─────

namespace {

/// 一个跑在自己 fiber 上的服务端:收 `kRequest` → 调 `Reply` 回一条 `kReply`。
/// `skip` 指定**前几条请求直接吞掉不回**,用来逼出客户端的重发。
class Service {
 public:
  Service(DdsNode& node, const std::string& request_topic, int skip = 0)
      : node_(node), skip_(skip) {
    sub_ = std::make_unique<Subscriber>(
        MustSubscribe(node, request_topic, MessageKind::kRequest),
        [this](const Message& request) { OnRequest(request); });
  }

  void Join() { sub_->Join(); }

  [[nodiscard]] std::vector<std::string> seen_correlations() const {
    return seen_correlations_;
  }
  [[nodiscard]] int received() const { return received_; }

 private:
  void OnRequest(const Message& request) {
    ++received_;
    seen_correlations_.push_back(request.correlation_id);
    if (received_ <= skip_) {
      return;  // 吞掉:客户端会在 retry.timeout 之后重发。
    }
    // 回一条把请求 payload 原样带回来的应答,便于客户端断言"这份确实是给我的"。
    (void)node_.Reply(request, Payload(Text(request)));
  }

  DdsNode& node_;
  int skip_;
  int received_ = 0;
  std::vector<std::string> seen_correlations_;
  std::unique_ptr<Subscriber> sub_;
};

/// 建一对"客户端 + 服务端"的注册:两侧**传一模一样的实参**,各自按角色建各自那一侧。
void RegisterPair(DdsNode& client, DdsNode& service, const std::string& request_topic,
                  const std::string& reply_topic) {
  ASSERT_TRUE(static_cast<bool>(client.RegisterClients({{request_topic, reply_topic}})));
  ASSERT_TRUE(static_cast<bool>(service.RegisterServices({{request_topic, reply_topic}})));
}

}  // namespace

TEST(DdsNode, RequestForResultDirectRoundTripsAndStampsTwoPartCorrelationId) {
  Fixture fixture;
  Host client(fixture, "node-a");  // uuid_override:保住确定性可测。
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc", "svc.reply");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc");

  EXPECT_EQ(client.node().uuid(), "node-a");

  auto first = client.node().RequestForResultDirect("svc", Payload("ping"),
                                                    RetryPolicy{kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(first)) << first.error().message();
  EXPECT_EQ(Text(first.value()), "ping");
  EXPECT_EQ(first.value().kind, MessageKind::kReply);
  EXPECT_EQ(first.value().topic, "svc.reply");  // 应答落在该服务的应答 topic 上。

  auto second = client.node().RequestForResultDirect("svc", Payload("pong"),
                                                     RetryPolicy{kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(second));
  EXPECT_EQ(Text(second.value()), "pong");

  // corr **两段式**:`<uuid>#<request_seq>`,request_seq 从 0 起、每请求一个。
  const auto correlations = service.seen_correlations();
  ASSERT_EQ(correlations.size(), 2u);
  EXPECT_EQ(correlations[0], "node-a#0");
  EXPECT_EQ(correlations[1], "node-a#1");
  service.Join();
}

// 等结果阶段**就重发**(与 ProtocolNode::RequestForResult 恰好相反),且重发的是**字节
// 完全相同的原帧**——corr 不变,故订阅横跨全部重发继续有效,最先到达者即终结。
TEST(DdsNode, RetriesInTheResultPhaseReusingTheSameCorrelationId) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc", "svc.reply");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc", /*skip=*/2);  // 前两条吞掉。

  auto got = client.node().RequestForResultDirect("svc", Payload("retry"),
                                                  RetryPolicy{100ms, 5});
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "retry");

  const auto correlations = service.seen_correlations();
  ASSERT_GE(correlations.size(), 3u);
  for (const auto& correlation : correlations) {
    EXPECT_EQ(correlation, "node-a#0") << "重发换了 correlation_id";
  }
  service.Join();
}

TEST(DdsNode, RetryExhaustionReturnsTimeoutNotNotAccepted) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc", "svc.reply");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc", /*skip=*/100);  // 永不回应。

  auto got = client.node().RequestForResultDirect("svc", Payload("void"),
                                                  RetryPolicy{60ms, 3});
  ASSERT_FALSE(static_cast<bool>(got));
  // **kTimeout,不是 kNotAccepted**:后者的语义是"对端没有受理",而本模型根本不存在
  // 受理这一步。
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_EQ(service.received(), 3) << "总发送次数应为 max_attempts(含首发)";
  service.Join();
}

TEST(DdsNode, RequestValidatesLifecycleRetryPolicyAndClientRegistration) {
  Fixture fixture;
  Host host(fixture, "node-a");
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({{"svc", "svc.reply"}})));

  // 未启动:kClosed(先于一切)。
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  // 策略非法:零时限(不接受"零即永不超时")与零次数(一帧都不发)。
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {0ms, 1}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {kCaseTimeout, 0}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // topic 未注册为 Clients 的键:**查不到即 kConfiguration,不猜、不回落**。
  EXPECT_EQ(node.RequestForResultDirect("other", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kConfiguration));
}

// ── 6. ⭐ 共用应答 topic 靠内部登记的**具体 corr** 区分客户端(D6)────────

TEST(DdsNode, SharedReplyTopicDiscriminatesClientsByConcreteCorrelationId) {
  Fixture fixture;
  CountingSink sink_a;
  CountingSink sink_b;
  Host client_a(fixture, "node-a", &sink_a);
  Host client_b(fixture, "node-b", &sink_b);
  Host server(fixture, "node-s");
  client_a.StartTransport();
  client_b.StartTransport();
  server.StartTransport();

  // **同一个应答 topic,两个客户端共用**——这正是 D6 那句"每服务一个、全体客户端共用"。
  ASSERT_TRUE(static_cast<bool>(
      client_a.node().RegisterClients({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(
      client_b.node().RegisterClients({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(
      server.node().RegisterServices({{"svc", "svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(client_a.node().Start()));
  ASSERT_TRUE(static_cast<bool>(client_b.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  // 服务端收满两条请求后**按到达的反序**回应:先回后到的 B、再回先到的 A。
  //
  // ★ **这个排序是本用例的全部要害**:它使 B 的应答**先**落到共用的 `svc.reply` 上。
  //   客户端 A 此刻正等在那条 topic 上——若它内部登记的 corr 是 `kAny`,先到的那份
  //   (**别人的**)就会当场把它的等待终结掉,A 拿到的将是 "from-b"。唯有内部登记用
  //   **具体 corr**,B 的那份才会在 A 的 `Dispatcher` 处落空,A 继续等到自己的那份。
  auto pending = std::make_shared<std::vector<Message>>();
  Subscriber svc(
      MustSubscribe(server.node(), std::string("svc"), MessageKind::kRequest),
      [&server, pending](const Message& request) {
        pending->push_back(request);
        if (pending->size() < 2) {
          return;
        }
        for (auto it = pending->rbegin(); it != pending->rend(); ++it) {
          (void)server.node().Reply(*it, Payload(Text(*it)));
        }
      });

  Coro::Result<Message> reply_a = make_error_code(TransportErrc::kInternal);
  Coro::Result<Message> reply_b = make_error_code(TransportErrc::kInternal);
  auto task_a = Coro::makeTask([&] {
    reply_a = client_a.node().RequestForResultDirect("svc", Payload("from-a"),
                                                     RetryPolicy{2000ms, 1});
  });
  // 等 A 的请求确实到了服务端,再让 B 发——这样"后到的先回"才是确定的,不靠调度巧合。
  ASSERT_TRUE(pumpFiberUntil([pending] { return pending->size() == 1; }));
  auto task_b = Coro::makeTask([&] {
    reply_b = client_b.node().RequestForResultDirect("svc", Payload("from-b"),
                                                     RetryPolicy{2000ms, 1});
  });
  (void)task_a.get();
  (void)task_b.get();

  ASSERT_TRUE(static_cast<bool>(reply_a)) << reply_a.error().message();
  ASSERT_TRUE(static_cast<bool>(reply_b)) << reply_b.error().message();
  // ★ 各拿各的:A 先等、B 的应答先到,A 仍然拿到 "from-a"。
  EXPECT_EQ(Text(reply_a.value()), "from-a");
  EXPECT_EQ(Text(reply_b.value()), "from-b");

  // **读入放大是明确接受的代价**(代价 8):别人的那份应答确实一路进到了本节点、被解码,
  // 然后才在 Dispatcher 处因 corr 不匹配而落空 —— 这里就是它留下的唯一痕迹。
  //
  // 两个方向都要等:B 的应答先发出、A 那边先落空,而 A 的应答后发出——`task_b.get()` 一
  // 返回只说明 B 收到了**自己**那份,别人那份还可能没走完 B 的读循环。故两个计数一起等。
  EXPECT_TRUE(pumpFiberUntil([&sink_a, &sink_b] {
    return sink_a.Drops(DropReason::kUnmatchedOrLateResponse) >= 1 &&
           sink_b.Drops(DropReason::kUnmatchedOrLateResponse) >= 1;
  }));
  EXPECT_GE(sink_a.Drops(DropReason::kUnmatchedOrLateResponse), 1u);
  EXPECT_GE(sink_b.Drops(DropReason::kUnmatchedOrLateResponse), 1u);
  svc.Join();
}

// ── 7. Reply:查自己的 Services 表 + reply_to 交叉校验(D15)──────────────

TEST(DdsNode, ReplyLooksUpItsOwnServicesTableAndCrossChecksReplyTo) {
  Fixture fixture;
  Host server(fixture, "node-s");
  server.StartTransport();
  DdsNode& node = server.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({{"svc", "svc.reply"}})));

  Message request;
  request.topic = "svc";
  request.kind = MessageKind::kRequest;
  request.correlation_id = "node-a#0";

  // 未启动:kClosed。
  EXPECT_EQ(node.Reply(request, Payload("r")).error(),
            make_error_code(TransportErrc::kClosed));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 本节点根本不服务这个 topic ⇒ kConfiguration。
  Message foreign = request;
  foreign.topic = "other";
  EXPECT_EQ(node.Reply(foreign, Payload("r")).error(),
            make_error_code(TransportErrc::kConfiguration));

  // `reply_to` 为空:**不取信于线缆**,照样按自己注册的应答 topic 发得出去。
  EXPECT_TRUE(static_cast<bool>(node.Reply(request, Payload("r"))));

  // `reply_to` 与自己注册的一致:通过。
  Message matching = request;
  matching.reply_to = "svc.reply";
  EXPECT_TRUE(static_cast<bool>(node.Reply(matching, Payload("r"))));

  // ★ `reply_to` 非空且不等 ⇒ kInvalidArgument。两侧注册实参写歪时(客户端在
  //   `svc.reply` 上等、服务端注册成别的往外发),**不带它这种偏差完全不可见**,客户端
  //   只会一路超时、看起来像对端没响应。
  Message skewed = request;
  skewed.reply_to = "cfg.reply";
  EXPECT_EQ(node.Reply(skewed, Payload("r")).error(),
            make_error_code(TransportErrc::kInvalidArgument));
}

// 应答 topic 的 writer 早在 `DoStart()` 就建好了(D15/D13 补正),故**服务的第一次应答
// 不会丢**。Fake provider 不惰性建 writer——未声明就 Publish 直接 kConfiguration,故这条
// 断言在 Fake 上如实成立。
TEST(DdsNode, FirstReplyIsNotLostBecauseWritersAreDeclaredAtStart) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-s");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc", "svc.reply");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc");

  // 只发**一次**(max_attempts = 1):首答一旦丢,这条就直接 kTimeout。
  auto got = client.node().RequestForResultDirect("svc", Payload("first"),
                                                  RetryPolicy{1000ms, 1});
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "first");
  EXPECT_FALSE(server.transport().LastError())
      << "应答走了未声明的 writer:" << server.transport().LastError().message();
  service.Join();
}

// ── 8. 生命周期:关闭令在途请求恰好终结一次 ─────────────────────────────

TEST(DdsNode, CloseTerminatesInFlightRequestExactlyOnce) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-s");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc", "svc.reply");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc", /*skip=*/100);  // 永不回应。

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome = client.node().RequestForResultDirect("svc", Payload("x"),
                                                   RetryPolicy{5000ms, 1});
  });
  EXPECT_TRUE(pumpFiberUntil([&service] { return service.received() >= 1; }));

  ASSERT_TRUE(static_cast<bool>(client.node().Close()));
  (void)caller.get();
  ASSERT_FALSE(static_cast<bool>(outcome));
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kClosed));
  client.node().WaitClosed();
  service.Join();
}

// 传输终结 ⇒ 节点自终(读循环退出时无条件调公开的 Close())。
TEST(DdsNode, TransportTerminationClosesTheNode) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"t"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));
  EXPECT_TRUE(node.IsRunning());

  ASSERT_TRUE(static_cast<bool>(host.transport().Close()));
  EXPECT_TRUE(pumpFiberUntil([&node] { return !node.IsRunning(); }));
  EXPECT_FALSE(node.IsRunning());
  node.WaitClosed();
}

// 坏样本归因 kBadFrame:codec 解不出来的字节被丢弃,读循环照常继续。
TEST(DdsNode, UndecodableSampleIsDroppedAsBadFrame) {
  Fixture fixture;
  CountingSink sink;
  Host host(fixture, {}, &sink);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"t"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 直接往总线上灌一条 kind 判别符非法的样本(0xFF > kNotify)。
  FakeDdsProvider peer(fixture.bus);
  ASSERT_TRUE(static_cast<bool>(peer.Init(fixture.Cfg())));
  ASSERT_TRUE(static_cast<bool>(peer.DeclareWriter("t")));
  ASSERT_TRUE(static_cast<bool>(peer.Publish("t", Bytes{0xFF, 0, 0, 0, 0})));

  EXPECT_TRUE(pumpFiberUntil(
      [&sink] { return sink.Drops(DropReason::kBadFrame) >= 1; }));
  EXPECT_GE(sink.Drops(DropReason::kBadFrame), 1u);
  peer.Shutdown();
}
