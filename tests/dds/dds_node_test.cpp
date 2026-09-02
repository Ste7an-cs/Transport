// -----------------------------------------------------------------------------
// dds_node_test.cpp — DdsNode 的注册接口 + 两种交互模式(ADR-0013 D5/D6/D7/D8/D12/D15/D16)。
// 在 fiber 调度器(coro_test_main)内跑。
//
// 确定化手段与 dds_transport_test 相同:每个 fixture 生成一个唯一的 provider 注册名并绑
// 一条独立的 Fake 总线,`--gtest_repeat` 多轮之间不串。节点之间**真的经传输 + codec +
// 总线**通信——不打桩 transport,故请求-响应的每一条断言都是端到端的。
//
// 覆盖九组事实:
//   1. 注册接口(**D16**):只在 Created 受理、批量累加、幂等去重、**整批生效或整批不生效**
//      (且**回滚不动先前批次**)、`Start()` 失败**不清空注册表**;
//   2. 注册期校验(派生化后只剩两条):空串 / **唯一要拦的方向冲突**(同一服务名既是
//      Clients 又是 Services);其余"同一 topic 既有 writer 又有 reader"的组合**不拦**;
//   3. ⭐ **服务名派生**(**D6**):`请求 = cfg.<名>.request`、`应答 = cfg.<名>.response`,
//      端点确实建在这两条上,两侧**只凭同一个服务名**即可通信;
//   4. 启动(**D12**/**D15**):四组全空 → kConfiguration;端点在 `DoStart()` 一次性建出,
//      **服务的第一次应答不会丢**;
//   5. 发布-订阅:`Publish` / `Subscribe(topic, kNotify)` 端到端;调用与注册的对应校验;
//   6. `Subscribe` 返 **`Coro::Result<Ticket>`**(**D8**),topic 传 `kAny` 时**跳过**校验;
//      **第一参永远是 topic**,`ServeRequests(名)` 是它的服务名封装;相位:**只在 `Running`
//      放行**——非 `Running` 一律 `kClosed`(判据与另外几个交互方法同一个 `IsRunning()`),
//      `Created` 期订阅是**禁用法**;
//   7. ⭐ 请求-响应(**D7**):单阶段、等结果时重发(**线缆上字节完全相同**)、耗尽返
//      **kTimeout**、首个到达即成功且**不回任何帧**、策略非法返 kInvalidArgument、
//      **服务名未注册返 kConfiguration**;
//   8. ⭐ corr 两段式(**D6**):`uuid_override` 可注入;**两个节点各自的 corr 不撞**;
//      共用应答 topic 靠**内部登记的具体 corr** 区分客户端(那处承重的区别);
//      `Reply` 反查自己注册的服务、`reply_to` 作一致性交叉校验(**D15**);
//   9. 生命周期:关闭令在途请求恰好终结一次,关闭之后五个交互方法一律**返** `kClosed`。
//
// 第 3、7、8 组是本文件的核心。
//
// **派生出的 topic 一律在用例里写死字面量**(`"cfg.svc.request"` 等),不复用被测的派生
// 函数——否则派生规则若被改坏,用例会跟着一起歪,什么都测不出来。
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

/// 挂在同一条 Fake 总线上的**线缆探针**:按 topic 收原始字节。
///
/// 「重发的是**字节完全相同**的原帧」与「成功之后**一帧都不再发**」这两条只在线缆这一层
/// 看得见——解码之后的 `Message` 已经看不出"这两帧的字节是不是同一份"。回调可能跑在被测
/// 传输的写线程上,故收到的东西一律加锁存(与 dds_transport_test 的 `Peer` 同形)。
class WireTap {
 public:
  WireTap(const Fixture& fixture, const std::string& topic)
      : provider_(fixture.bus) {
    EXPECT_TRUE(static_cast<bool>(provider_.Init(fixture.Cfg())));
    EXPECT_TRUE(static_cast<bool>(
        provider_.Subscribe(topic, [this](const Bytes& bytes) {
          std::lock_guard<std::mutex> lock(mutex_);
          frames_.push_back(bytes);
        })));
  }
  ~WireTap() { provider_.Shutdown(); }

  WireTap(const WireTap&) = delete;
  WireTap& operator=(const WireTap&) = delete;

  [[nodiscard]] std::size_t Count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frames_.size();
  }
  [[nodiscard]] std::vector<Bytes> Frames() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frames_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<Bytes> frames_;
  FakeDdsProvider provider_;
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

  // Created:四个方法都受理。后两个**只收服务名**(D6)。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"pub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"sub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"own"})));

  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // Running:一律 kInvalidState——端点集合"启动即定型、运行期恒定",本设计不引入运行期
  // 动态端点。
  EXPECT_EQ(node.RegisterPublishers({"late"}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterSubscribers({"late"}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterClients({"late"}).error(),
            make_error_code(TransportErrc::kInvalidState));
  EXPECT_EQ(node.RegisterServices({"late"}).error(),
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
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"svc", "svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"svc"})));
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
  EXPECT_EQ(node.RegisterClients({"ok", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));

  // 三批都没落下任何一项 ⇒ 四组仍全空 ⇒ Start 返 kConfiguration(D12)。
  EXPECT_EQ(node.Start().error(), make_error_code(TransportErrc::kConfiguration));
}

// 上一条测的是"从空注册表开始,非法批一项都不落";本条测的是**回滚的另一半**——非法批
// 不得动**先前已落**的那些。
//
// 全部断言都走公开面:注册表没有 getter,"某项在不在里面"只能由 `Publish` / `Subscribe` /
// `ServeRequests` / `RequestForResultDirect` 的对应校验反推——这也正是调用方能观察到的那一层。
//
// **原用例名里的"一个请求 topic 只能配一个应答 topic"那半段随派生化删除**:该校验(#204
// 复核时补的运行期校验)所拦的输入现在**根本表达不出来**——注册只收服务名,同名必派生出
// 同一对 topic。测一个不可能发生的事没有意义,故连同其断言一并去掉。
TEST(DdsNode, RollbackLeavesEarlierBatchesIntact) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  // 先落三批合法的,其中两批**跨调用累加**、且带重复项(幂等去重,不报错)。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"p1"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"s1"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"s1", "s2"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"cli"})));

  // 每批都含一项合法 + 一项非法:**合法的那项也不许落下**。
  EXPECT_EQ(node.RegisterPublishers({"p2", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterSubscribers({"s3", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterServices({"svc2", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // ★ 非法项是方向冲突(`svc` 已注册为 Services):同批的 `cli2` 也一并不落。
  EXPECT_EQ(node.RegisterClients({"cli2", "svc"}).error(),
            make_error_code(TransportErrc::kInvalidArgument));

  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 先前那三批**原样还在**。
  EXPECT_TRUE(static_cast<bool>(node.Publish("p1", Payload("x"))));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("s1"), MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("s2"), MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(node.ServeRequests("svc")));
  EXPECT_TRUE(
      static_cast<bool>(node.Subscribe(std::string("cfg.cli.response"), kAny)));

  // 非法批里的**合法项**一个都没落下。
  EXPECT_EQ(node.Publish("p2", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Subscribe(std::string("s3"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.ServeRequests("svc2").error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.RequestForResultDirect("cli2", Payload("x"), {kCaseTimeout, 1})
                .error(),
            make_error_code(TransportErrc::kConfiguration));
}

// 派生化之后成对注册的校验**只剩"服务名非空"一条**(另两条见 `ValidateServiceNameBatch`
// 的注释:键值相同、跨批次改应答 topic,均已不可能发生)。
//
// 原用例名 `PairRegistrationRejectsSameKeyAndValue` 里的"键值相同"那半段随之删除。
TEST(DdsNode, ServiceNameRegistrationRejectsEmptyName) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  EXPECT_EQ(node.RegisterClients({""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RegisterServices({""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // 一批里混着一个空名同样整批拒。
  EXPECT_EQ(node.RegisterClients({"ok", ""}).error(),
            make_error_code(TransportErrc::kInvalidArgument));

  // **除"非空"外不限制字符**:点、斜杠、空格、`#` 一律照收——`cfg.<名>.request` 与
  // `cfg.<名>.response` 对任意非空名都互不相同,拼接是单射的,派生不出歧义。
  ASSERT_TRUE(static_cast<bool>(
      node.RegisterClients({"log.tail", "a/b", "with space", "has#hash"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));
  // 每个都按同一条规则派生,各自独立可用。
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("cfg.log.tail.response"), kAny)));
  EXPECT_TRUE(
      static_cast<bool>(node.Subscribe(std::string("cfg.a/b.response"), kAny)));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("cfg.with space.response"), kAny)));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("cfg.has#hash.response"), kAny)));
}

// ⭐ **唯一要拦的方向冲突**:同一**服务名**既注册为 Clients、又注册为 Services——自己请求
// 自己,且 corr 由自己生成、Dispatcher **会真的匹配上**,形成毫无察觉的自问自答。
//
// 派生化之后这条比先前更严实:先前拦的是"请求 topic 撞上",而两侧的应答 topic 是分别配
// 的;现在服务名一撞,**两个 topic 全撞**。
TEST(DdsNode, SameServiceNameAsBothClientAndServiceIsRejectedBothOrders) {
  Fixture fixture;
  {
    Host host(fixture);
    host.StartTransport();
    ASSERT_TRUE(static_cast<bool>(host.node().RegisterServices({"svc"})));
    EXPECT_EQ(host.node().RegisterClients({"svc"}).error(),
              make_error_code(TransportErrc::kInvalidArgument));
  }
  {
    Host host(fixture);  // 反过来注册,同样拦下——两侧各查对方那一组。
    host.StartTransport();
    ASSERT_TRUE(static_cast<bool>(host.node().RegisterClients({"svc"})));
    EXPECT_EQ(host.node().RegisterServices({"svc"}).error(),
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
  // Publishers ∩ 某客户端的应答 topic:往 `cfg.svc.response` 发布、同时又是 `svc` 的
  // 客户端,合法。**派生化之后这种重叠只能这样写出来**——得直接把派生 topic 当普通
  // topic 注册,这正是"框架侵占 cfg.* 命名空间"那条代价的具体形状(D6 明确接受,不拦)。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"cfg.svc.response"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"svc"})));
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
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"far"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // 对应关系:kNotify → Subscribers;kRequest → 某已注册服务的 `cfg.<名>.request`。
  // **`Subscribe` 的第一参永远是 topic**,故这里写的是派生出来的那条,不是服务名。
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("news"), MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(
      node.Subscribe(std::string("cfg.svc.request"), MessageKind::kRequest)));
  // 服务名本身**不是** topic:直接拿它当 topic 订阅必然落空。
  EXPECT_EQ(node.Subscribe(std::string("svc"), MessageKind::kRequest).error(),
            make_error_code(TransportErrc::kConfiguration));

  // 角色不对 ⇒ kConfiguration。**错误码就是 kConfiguration**,而不是推迟到第一次 `Wait`
  // 才冒出来的 kInvalidState——这正是"返 Result 而不是裸 Ticket"要拿到的东西(D8)。
  EXPECT_EQ(
      node.Subscribe(std::string("cfg.svc.request"), MessageKind::kNotify).error(),
      make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Subscribe(std::string("news"), MessageKind::kRequest).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Subscribe(std::string("unknown"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kConfiguration));

  // topic 传 `kAny` ⇒ **跳过该校验**:kAny 不对应任何一个具体 topic,拿它查注册表必然落空。
  EXPECT_TRUE(static_cast<bool>(node.Subscribe(kAny, MessageKind::kNotify)));
  EXPECT_TRUE(static_cast<bool>(node.Subscribe(kAny, kAny)));

  // 其余 kind + 具体 topic:至少得在读侧集合内(Subscribers ∪ 各服务的 cfg.<名>.request
  // ∪ 各客户端的 cfg.<名>.response)。
  EXPECT_TRUE(
      static_cast<bool>(node.Subscribe(std::string("cfg.far.response"), kAny)));
  // ★ 客户端在 `far` 上只有 writer(发请求),故它的请求 topic **不在读侧**。
  EXPECT_EQ(node.Subscribe(std::string("cfg.far.request"), kAny).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 服务端反之:请求 topic 在读侧,应答 topic 只有 writer。
  EXPECT_EQ(node.Subscribe(std::string("cfg.svc.response"), kAny).error(),
            make_error_code(TransportErrc::kConfiguration));
}

// ⭐ **派生规则**(**D6**):注册服务名 `get` 之后,端点确实建在 `cfg.get.request` /
// `cfg.get.response` 上,且**客户端与服务端只凭同一个服务名就通得了**。
//
// 证据分三层,都不依赖被测的派生函数(topic 字面量写死):
//   ① **线缆层**——`WireTap` 挂在 `cfg.get.request` 上,收得到客户端发出的请求帧;
//   ② **分发层**——服务端用 `ServeRequests("get")` 就收得到,且它与显式
//      `Subscribe("cfg.get.request", kRequest)` **是同一条订阅**(后者此刻同样合法);
//   ③ **返回值层**——应答的 `topic` 是 `cfg.get.response`。
//
// 两侧从头到尾**只说过 "get" 这一个词**:客户端 `RegisterClients({"get"})` +
// `RequestForResultDirect("get", …)`,服务端 `RegisterServices({"get"})` +
// `ServeRequests("get")`。这正是本轮改动要买的东西。
TEST(DdsNode, DerivedTopicsAreCfgNameRequestAndResponse) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-s");
  client.StartTransport();
  server.StartTransport();

  ASSERT_TRUE(static_cast<bool>(client.node().RegisterClients({"get"})));
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"get"})));
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  // ② `ServeRequests("get")` ≡ `Subscribe("cfg.get.request", kRequest)`:两种写法此刻
  //    都合法,**且指的是同一条 topic**——服务端从没说过 "cfg.get.request"。
  EXPECT_TRUE(static_cast<bool>(server.node().Subscribe(
      std::string("cfg.get.request"), MessageKind::kRequest)));

  WireTap requests(fixture, "cfg.get.request");   // ① 请求确实走这条。
  WireTap replies(fixture, "cfg.get.response");   // ① 应答确实走那条。

  auto pending = std::make_shared<std::vector<Message>>();
  auto serve = server.node().ServeRequests("get");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(),
                 [&server, pending](const Message& request) {
                   pending->push_back(request);
                   (void)server.node().Reply(request, Payload("pong"));
                 });

  auto got = client.node().RequestForResultDirect("get", Payload("ping"),
                                                  RetryPolicy{1000ms, 1});
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "pong");
  // ③ 应答落在派生出的应答 topic 上。
  EXPECT_EQ(got.value().topic, "cfg.get.response");

  // ① 线缆上两条 topic 各自都真的过了帧。
  EXPECT_GE(requests.Count(), 1u) << "请求没走 cfg.get.request";
  EXPECT_GE(replies.Count(), 1u) << "应答没走 cfg.get.response";

  // 服务端看到的请求 topic 同样是派生出来的那条(它是 `Reply` 反查服务的唯一输入)。
  ASSERT_EQ(pending->size(), 1u);
  EXPECT_EQ(pending->front().topic, "cfg.get.request");
  EXPECT_EQ(pending->front().reply_to, "cfg.get.response");
  svc.Join();
}

// `ServeRequests` 的两道校验都落在 `Subscribe` 里(它是后者的服务名封装,不是另一套机制):
// 相位先于配置,未注册的服务名返 `kConfiguration`。
TEST(DdsNode, ServeRequestsValidatesLifecycleAndServiceRegistration) {
  Fixture fixture;
  Host host(fixture, "node-s");
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"cli"})));

  // 未启动:kClosed 先于一切(`Subscribe` 的相位规则,#218/#219)。
  EXPECT_EQ(node.ServeRequests("svc").error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  EXPECT_TRUE(static_cast<bool>(node.ServeRequests("svc")));
  // **未注册的服务名 ⇒ kConfiguration**,不猜、不回落。
  EXPECT_EQ(node.ServeRequests("other").error(),
            make_error_code(TransportErrc::kConfiguration));
  // 只注册为客户端的服务名同样不行——它在那条服务上没有请求侧的 reader。
  EXPECT_EQ(node.ServeRequests("cli").error(),
            make_error_code(TransportErrc::kConfiguration));
  // 空服务名:`cfg..request` 永远注册不上,故同样 kConfiguration(不是 kInvalidArgument
  // ——那是**注册面**对空串的答案)。
  EXPECT_EQ(node.ServeRequests("").error(),
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

/// 一个跑在自己 fiber 上的服务端:`ServeRequests(服务名)` 收 `kRequest` → 调 `Reply` 回一条
/// `kReply`。**只说服务名**,请求 topic 由框架派生。
/// `skip` 指定**前几条请求直接吞掉不回**,用来逼出客户端的重发。
class Service {
 public:
  Service(DdsNode& node, const std::string& service_name, int skip = 0)
      : node_(node), skip_(skip) {
    auto ticket = node.ServeRequests(service_name);
    EXPECT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
    sub_ = std::make_unique<Subscriber>(
        std::move(ticket).value(),
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

/// 建一对"客户端 + 服务端"的注册:两侧**传一模一样的服务名**,各自按角色建各自那一侧。
/// 派生化之后这个小件只剩一个参数——先前它要收请求 topic 与应答 topic 两个。
void RegisterPair(DdsNode& client, DdsNode& service,
                  const std::string& service_name) {
  ASSERT_TRUE(static_cast<bool>(client.RegisterClients({service_name})));
  ASSERT_TRUE(static_cast<bool>(service.RegisterServices({service_name})));
}

}  // namespace

TEST(DdsNode, RequestForResultDirectRoundTripsAndStampsTwoPartCorrelationId) {
  Fixture fixture;
  Host client(fixture, "node-a");  // uuid_override:保住确定性可测。
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc");

  EXPECT_EQ(client.node().uuid(), "node-a");

  auto first = client.node().RequestForResultDirect("svc", Payload("ping"),
                                                    RetryPolicy{kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(first)) << first.error().message();
  EXPECT_EQ(Text(first.value()), "ping");
  EXPECT_EQ(first.value().kind, MessageKind::kReply);
  // 应答落在**派生出的**应答 topic 上——两侧从头到尾只说过 "svc" 这个服务名。
  EXPECT_EQ(first.value().topic, "cfg.svc.response");

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

  RegisterPair(client.node(), server.node(), "svc");
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

// 上一条在**解码之后**证了 corr 不变;本条在**线缆上**证"重发的是字节完全相同的原帧"
// (ADR-0010 D3 / **D7**)——实现只 `Encode` 一次、重发复用同一份字节,这是唯一能把它钉死
// 的层面:若哪天改成每次重新 Encode,corr 仍然一样,但线缆上的字节就可能不再逐字节相同
// (时间戳、序号、任何编码期取值都会漏进来),而"订阅横跨全部重发继续有效"的依据正是
// **这一份字节从头到尾没变过**。
TEST(DdsNode, ResentFramesAreByteIdenticalOnTheWire) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc", /*skip=*/2);  // 逼出两次重发。
  // 客户端往**派生出的**请求 topic 上发的每一帧原样收下。
  WireTap requests(fixture, "cfg.svc.request");

  auto got = client.node().RequestForResultDirect("svc", Payload("same-bytes"),
                                                  RetryPolicy{100ms, 5});
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();

  const auto frames = requests.Frames();
  ASSERT_GE(frames.size(), 3u) << "没发生重发,本例测不到东西";
  ASSERT_FALSE(frames.front().empty());
  for (std::size_t i = 1; i < frames.size(); ++i) {
    EXPECT_EQ(frames[i], frames.front())
        << "第 " << i << " 帧与首发不是同一份字节";
  }
  service.Join();
}

// **首个到达即成功,不回应**(**D7**):本模型没有受理阶段、也没有确认帧,拿到应答就完事。
// 后半段是它的另一面——那条内部登记的订阅随调用返回而注销,故**迟到的第二份应答**无人
// 认领,归因 `kUnmatchedOrLateResponse`(共用应答 topic 之下这条归因本来就会很吵,代价 8)。
TEST(DdsNode, SuccessfulRequestSendsNothingBackAndTheLateDuplicateReplyIsUnmatched) {
  Fixture fixture;
  CountingSink sink;
  Host client(fixture, "node-a", &sink);
  Host server(fixture, "node-s");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc");
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  WireTap requests(fixture, "cfg.svc.request");
  auto seen = std::make_shared<std::vector<Message>>();
  auto serve = server.node().ServeRequests("svc");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(), [&server, seen](const Message& request) {
    seen->push_back(request);
    (void)server.node().Reply(request, Payload("done"));
  });

  auto got = client.node().RequestForResultDirect("svc", Payload("once"),
                                                  RetryPolicy{1000ms, 3});
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "done");
  ASSERT_EQ(seen->size(), 1u);

  // 拿到应答之后客户端**一帧都不再发**:既没有确认帧,也不会再重发一次。
  EXPECT_FALSE(pumpFiberUntil([&requests] { return requests.Count() > 1; }, 200))
      << "成功之后客户端又往请求 topic 上发了帧";
  EXPECT_EQ(requests.Count(), 1u);

  // 服务端补发一份**同 corr** 的应答:此刻客户端已无对应登记 ⇒ 落到"无人认领"。
  ASSERT_TRUE(static_cast<bool>(server.node().Reply(seen->front(), Payload("dup"))));
  EXPECT_TRUE(pumpFiberUntil([&sink] {
    return sink.Drops(DropReason::kUnmatchedOrLateResponse) >= 1;
  }));
  EXPECT_GE(sink.Drops(DropReason::kUnmatchedOrLateResponse), 1u);
}

TEST(DdsNode, RetryExhaustionReturnsTimeoutNotNotAccepted) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-b");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc");
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
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"own"})));

  // 未启动:kClosed(先于一切)。
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  // 策略非法:零时限(不接受"零即永不超时")与零次数(一帧都不发)。
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {0ms, 1}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {kCaseTimeout, 0}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // 负值同判:`timeout <= 0` 与 `max_attempts < 1`(两个字段都是有符号类型,负值进得来)。
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {-1ms, 1}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_EQ(node.RequestForResultDirect("svc", Payload("x"), {kCaseTimeout, -1}).error(),
            make_error_code(TransportErrc::kInvalidArgument));
  // **服务名未注册为 Clients:查不到即 kConfiguration,不猜、不回落**。
  EXPECT_EQ(node.RequestForResultDirect("other", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 只注册为服务端的服务名同样不行——那一侧没有发请求的 writer。
  EXPECT_EQ(node.RequestForResultDirect("own", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 派生出的 topic **不是**服务名:拿它当第一参一样查不到(第一参永远是服务名,D8)。
  EXPECT_EQ(node.RequestForResultDirect("cfg.svc.request", Payload("x"),
                                        {kCaseTimeout, 1})
                .error(),
            make_error_code(TransportErrc::kConfiguration));
}

// ── 6. ⭐ corr 的 uuid 半段:跨节点不撞(D6)──────────────────────────────

// 前面几条都注入了 `uuid_override`(为了断言具体值);本条**故意不注入**,走
// `QUuid::createUuid()` 那条生产路径,证的是 uuid 半段真正的用途:
// **两个节点的 corr 不撞**。`request_seq` 是**各节点自己**从 0 起的,故若没有 uuid 半段,
// 这两条本该互不相干的请求会带着同一个 "0" 上线缆——而它们此刻正共用一个应答 topic。
TEST(DdsNode, CorrelationIdsOfDistinctNodesNeverCollide) {
  Fixture fixture;
  Host client_a(fixture);  // 不注入 uuid。
  Host client_b(fixture);
  Host server(fixture);
  client_a.StartTransport();
  client_b.StartTransport();
  server.StartTransport();

  RegisterPair(client_a.node(), server.node(), "svc");
  ASSERT_TRUE(static_cast<bool>(client_b.node().RegisterClients({"svc"})));
  ASSERT_TRUE(static_cast<bool>(client_a.node().Start()));
  ASSERT_TRUE(static_cast<bool>(client_b.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));
  Service service(server.node(), "svc");

  const std::string uuid_a = client_a.node().uuid();
  const std::string uuid_b = client_b.node().uuid();
  EXPECT_FALSE(uuid_a.empty());
  EXPECT_NE(uuid_a, uuid_b) << "两个节点拿到了同一个 uuid";
  // uuid 半段里不能有 '#':两段式靠它切,uuid 自带一个就切出歧义了。
  EXPECT_EQ(uuid_a.find('#'), std::string::npos);
  EXPECT_EQ(uuid_b.find('#'), std::string::npos);

  // 两条请求依次发(调用是阻塞的,故服务端看到的顺序确定)。
  auto from_a = client_a.node().RequestForResultDirect("svc", Payload("a"),
                                                       RetryPolicy{kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(from_a)) << from_a.error().message();
  auto from_b = client_b.node().RequestForResultDirect("svc", Payload("b"),
                                                       RetryPolicy{kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(from_b)) << from_b.error().message();
  EXPECT_EQ(Text(from_a.value()), "a");
  EXPECT_EQ(Text(from_b.value()), "b");

  const auto correlations = service.seen_correlations();
  ASSERT_EQ(correlations.size(), 2u);
  // ★ 两条都是本节点的第 0 号请求,**只有 uuid 半段把它们分开**。
  EXPECT_EQ(correlations[0], uuid_a + "#0");
  EXPECT_EQ(correlations[1], uuid_b + "#0");
  EXPECT_NE(correlations[0], correlations[1]);
  service.Join();
}

// ── 7. ⭐ 共用应答 topic 靠内部登记的**具体 corr** 区分客户端(D6)────────

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
  // 派生化之后共用是**自动的**:两个客户端说的都是 "svc",派生出的 `cfg.svc.response`
  // 必然是同一条,调用方无从把它们配到两条不同的应答 topic 上。
  ASSERT_TRUE(static_cast<bool>(client_a.node().RegisterClients({"svc"})));
  ASSERT_TRUE(static_cast<bool>(client_b.node().RegisterClients({"svc"})));
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(client_a.node().Start()));
  ASSERT_TRUE(static_cast<bool>(client_b.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  // 服务端收满两条请求后**按到达的反序**回应:先回后到的 B、再回先到的 A。
  //
  // ★ **这个排序是本用例的全部要害**:它使 B 的应答**先**落到共用的 `cfg.svc.response` 上。
  //   客户端 A 此刻正等在那条 topic 上——若它内部登记的 corr 是 `kAny`,先到的那份
  //   (**别人的**)就会当场把它的等待终结掉,A 拿到的将是 "from-b"。唯有内部登记用
  //   **具体 corr**,B 的那份才会在 A 的 `Dispatcher` 处落空,A 继续等到自己的那份。
  auto pending = std::make_shared<std::vector<Message>>();
  auto serve = server.node().ServeRequests("svc");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(), [&server, pending](const Message& request) {
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

// ── 8. Reply:由 request.topic 反查自己注册的服务 + reply_to 交叉校验(D15)──

TEST(DdsNode, ReplyResolvesTheServiceFromRequestTopicAndCrossChecksReplyTo) {
  Fixture fixture;
  Host server(fixture, "node-s");
  server.StartTransport();
  DdsNode& node = server.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));

  // 一条"收到的请求"。它的 `topic` 是**派生出的请求 topic**——`Reply` 的全部输入就是它,
  // 反查走的是同一个派生规则。
  Message request;
  request.topic = "cfg.svc.request";
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
  // **服务名本身不是请求 topic**:反查同样落空。
  Message bare_name = request;
  bare_name.topic = "svc";
  EXPECT_EQ(node.Reply(bare_name, Payload("r")).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 应答 topic 也不是请求 topic——两个后缀不同,反查不会张冠李戴。
  Message reply_side = request;
  reply_side.topic = "cfg.svc.response";
  EXPECT_EQ(node.Reply(reply_side, Payload("r")).error(),
            make_error_code(TransportErrc::kConfiguration));

  // `reply_to` 为空:**不取信于线缆**,照样按派生出的应答 topic 发得出去。
  EXPECT_TRUE(static_cast<bool>(node.Reply(request, Payload("r"))));

  // `reply_to` 与派生出的一致:通过。
  Message matching = request;
  matching.reply_to = "cfg.svc.response";
  EXPECT_TRUE(static_cast<bool>(node.Reply(matching, Payload("r"))));

  // ★ `reply_to` 非空且不等 ⇒ kInvalidArgument。**派生化之后两侧配歪已不可能**,这条校验
  //   剩下的用途是对**版本不一致的对端**(派生规则将来若变更)当场报出偏差——否则客户端
  //   只会一路超时、看起来像对端没响应。
  Message skewed = request;
  skewed.reply_to = "svc.reply";  // 旧派生规则(或写歪了)的对端会带这个。
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

  RegisterPair(client.node(), server.node(), "svc");
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

// ── 9. 生命周期:关闭令在途请求恰好终结一次,此后一律终结于 kClosed ──────

TEST(DdsNode, CloseTerminatesInFlightRequestExactlyOnce) {
  Fixture fixture;
  Host client(fixture, "node-a");
  Host server(fixture, "node-s");
  client.StartTransport();
  server.StartTransport();

  RegisterPair(client.node(), server.node(), "svc");
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

// 关闭之后五个交互方法**一律在返回值上给出 kClosed**,且不因 topic / 服务名有没有注册而
// 改口:调用序错误先于配置错误。
//
// `Subscribe` 与另外几个**同一个判据** `IsRunning()`(未启动一侧同样 `kClosed`,见下一例),
// 关闭一侧自然也同一个答案:
// `kClosed` 由 `Subscribe` **返回**给出,不再推迟到第一次 `Wait`——`Dispatcher::CloseAll`
// 之后"交出一张信箱已关闭的凭据"那条既定语义(Dispatcher.hpp)在本节点的公开面上够不着了。
TEST(DdsNode, EveryInteractionAfterCloseEndsInClosed) {
  Fixture fixture;
  Host host(fixture, "node-a");
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"pub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"sub"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterClients({"cli"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  Message request;  // 一条"关闭之前收到的"请求,用来试 Reply。
  request.topic = "cfg.svc.request";
  request.kind = MessageKind::kRequest;
  request.correlation_id = "peer#0";

  ASSERT_TRUE(static_cast<bool>(node.Close()));
  node.WaitClosed();

  EXPECT_EQ(node.Publish("pub", Payload("x")).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.RequestForResultDirect("cli", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Reply(request, Payload("r")).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Subscribe(std::string("sub"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kClosed));
  // `ServeRequests` 同样——它的相位判定落在它封装的 `Subscribe` 里。
  EXPECT_EQ(node.ServeRequests("svc").error(),
            make_error_code(TransportErrc::kClosed));
  // 未注册的 topic / 服务名也一样报 kClosed,不报 kConfiguration。
  EXPECT_EQ(node.Publish("nope", Payload("x")).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Subscribe(std::string("nope"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.ServeRequests("nope").error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.RequestForResultDirect("nope", Payload("x"), {kCaseTimeout, 1})
                .error(),
            make_error_code(TransportErrc::kClosed));
  // `kAny` 也不例外:相位判定在 `kAny` 的跳过校验**之前**。
  EXPECT_EQ(node.Subscribe(kAny, kAny).error(),
            make_error_code(TransportErrc::kClosed));
}

// `Subscribe` **只在 `Running` 受理**:`Created` 期订阅是**禁用法**,返 `kClosed`。
//
// 判据与另外三个交互方法**同一个** `IsRunning()`——`kClosed` 一并覆盖"未启动 / 关闭中 /
// 已关闭",这是本库已写进公开 `@return` 的既有约定(见 `ProtocolNode.hpp`),`Subscribe`
// 不为"没启动"单开一个错误码。注册面与订阅面仍互不重叠:注册只在 `Created`,订阅只在
// `Running`。相位判定**先于**注册校验:连已注册为 `Subscribers` 的 topic 在此相位也报
// `kClosed`,而不是放行。
//
// 尾部那段吸收了原 `SubscribeReturnsClosedAfterCloseFromCreated`:改判之后两者**返回值与
// 代码路径都相同**(同一条 `if (!IsRunning())`,`Created` 与 `Closed` 不再分叉),留两条
// 独立用例只会让读者去找一个已经不存在的区别;但"从未 `Start()` 就 `Close()`"这条路径
// (`NodeBase` 不调 `DoClose()`,正是 #217 的那条)值得留一行,故并进来而不是删掉。
TEST(DdsNode, SubscribeBeforeStartIsClosed) {
  Fixture fixture;
  Host host(fixture, "node-a");
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"loop"})));

  // ★ 尚未 Start():注册得再全也订阅不了。
  EXPECT_EQ(node.Subscribe(std::string("loop"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kClosed));
  // 未注册的 topic 也一样报 kClosed,不报 kConfiguration(相位先于配置)。
  EXPECT_EQ(node.Subscribe(std::string("unknown"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kClosed));
  // `kAny` 不例外:相位判定在 `kAny` 的跳过校验之前。
  EXPECT_EQ(node.Subscribe(kAny, kAny).error(),
            make_error_code(TransportErrc::kClosed));

  // 放弃启动、从 `Created` 直接 `Close()`:相位直落 `Closed`,答案不变。
  ASSERT_TRUE(static_cast<bool>(node.Close()));
  EXPECT_EQ(node.Subscribe(std::string("loop"), MessageKind::kNotify).error(),
            make_error_code(TransportErrc::kClosed));
}

// 推荐写法「注册 → `Start()` → 订阅」:`Start()` 之后订阅照常收得到消息。
//
// 这样不漏收启动初期的消息——`DataReader` 建于 `DoStart()`,而 DDS 发现约需 ~240ms,
// `Start()` 返回后的头 ~240ms 对端还没 match,一条样本也到不了;紧挨着的两句(中间连一次
// 让出都没有)离那个边界差着几个数量级。
TEST(DdsNode, SubscribeRightAfterStartReceivesTheFirstMessage) {
  Fixture fixture;
  Host host(fixture, "node-a");
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"loop"})));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  auto ticket = node.Subscribe(std::string("loop"), MessageKind::kNotify);
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();

  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(std::move(ticket).value(),
                 [seen](const Message& msg) { seen->push_back(Text(msg)); });
  ASSERT_TRUE(static_cast<bool>(node.Publish("loop", Payload("first"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 1; }));
  ASSERT_EQ(seen->size(), 1u);
  EXPECT_EQ(seen->front(), "first");

  ASSERT_TRUE(static_cast<bool>(node.Close()));
  node.WaitClosed();
  sub.Join();
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
