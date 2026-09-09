// -----------------------------------------------------------------------------
// dds_node_dynamic_registration_test.cpp — `DdsNode` 的**运行期动态注册与注销**
// (ADR-0015 D1/D2/D3/D6/D7)。在 fiber 调度器(coro_test_main)内跑。
//
// 与 `dds_node_test.cpp` 分家的理由:那个文件守的是 ADR-0013 的注册面与两种交互模式,
// 本文件守的是 ADR-0015 放开之后**多出来的那半边**——端点随注册当场增删。确定化手段相同
// (每个 fixture 一条独立 Fake 总线 + 独占的 provider 注册名),节点之间**真的经传输 +
// codec + 总线**通信。
//
// 覆盖的事实,每条都**真正观测到效果**,不只是"没崩":
//   1. `Running` 期注册后**立刻收发能通**(Fake 无发现窗口,故这条是确定的);
//   2. ⭐ 注销后该 topic **确实不再收发**,且**注销再重新注册收发恢复正常**——读侧尤其
//      承重:**读路径根本不查注册表**(`Dispatcher` 之外无判据),故"收不到了"只可能是
//      `DataReader` 真被拆了,而不是某处多判了一下注册表;
//   3. ⭐ 同一 topic 同时在 `Publishers` 与 `Subscribers`(方向不同):注销其一,**另一侧
//      仍正常工作**——这是"一条端点恰有一个注册项负责"(**D3**)在两个方向上的体现;
//   4. ⭐ 注销一个**正在被 `ServeRequests` 服务**的服务名(**D6**):已持有的 `Ticket`
//      **仍有效**(它挂在 `Dispatcher` 上,与注册表无关),`Reply()` 返 `kConfiguration`;
//   5. ⭐ `Running` 期批量注册中途建端点失败的**回滚**(**D7**):注册表一项不落,且**本批
//      已建的端点已被拆掉**;
//   6. 全空 `Start()` 之后动态注册,端到端收发照常(**D5** 的下半场);
//   7. `Created` 期注销只从集合移除;不在册的项是幂等空操作。
//
// **派生出的 topic 一律写死字面量**(`"cfg.svc.request"` 等),不复用被测的派生函数。
// -----------------------------------------------------------------------------
#include "transport/node/DdsNode.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/codec/DdsCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsProviderRegistry.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/io/dds/IDdsProvider.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsProviderRegistry;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::IDdsProvider;
using transport::kAny;
using transport::Message;
using transport::MessageKind;
using transport::RetryPolicy;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

using Bytes = std::vector<std::uint8_t>;

int NextFixtureId() {
  static std::atomic<int> counter{0};
  return ++counter;
}

/// provider 侧的可控件:哪几条 topic 上**建端点会失败**,以及拆过哪些。
///
/// 回滚(**D7**)必须能被观测到"**本批已建的端点已被拆掉**",而这件事在节点与传输那一层
/// 都看不见——只有 provider 知道自己被摘过。故把它记在这里。
struct ProviderTrace {
  mutable std::mutex m;
  std::set<std::string> fail_writers;  ///< 这些 topic 上 `DeclareWriter` 返 kIo。
  std::set<std::string> fail_readers;  ///< 这些 topic 上 `Subscribe` 返 kIo。
  std::vector<std::string> undeclared;  ///< 被 `UndeclareWriter` / `Unsubscribe` 拆过的。

  [[nodiscard]] bool WasUndeclared(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(m);
    for (const auto& seen : undeclared) {
      if (seen == topic) {
        return true;
      }
    }
    return false;
  }
};

/// `FakeDdsProvider` 加两件事:**按 topic 令建端点失败**、**记下拆过谁**。
///
/// 只覆盖这四个方法,其余行为(总线、Publish 的"未声明即 kConfiguration"判据)原样沿用
/// ——回滚用例要的是"建到一半失败",不是另造一套总线语义。
class TracingProvider : public FakeDdsProvider {
 public:
  TracingProvider(std::shared_ptr<Bus> bus, std::shared_ptr<ProviderTrace> trace)
      : FakeDdsProvider(std::move(bus)), trace_(std::move(trace)) {}

  Coro::Result<void> DeclareWriter(const std::string& topic) override {
    {
      std::lock_guard<std::mutex> lock(trace_->m);
      if (trace_->fail_writers.count(topic) != 0) {
        return make_error_code(TransportErrc::kIo);
      }
    }
    return FakeDdsProvider::DeclareWriter(topic);
  }

  Coro::Result<void> UndeclareWriter(const std::string& topic) override {
    {
      std::lock_guard<std::mutex> lock(trace_->m);
      trace_->undeclared.push_back(topic);
    }
    return FakeDdsProvider::UndeclareWriter(topic);
  }

  Coro::Result<void> Subscribe(const std::string& topic, Sink cb) override {
    {
      std::lock_guard<std::mutex> lock(trace_->m);
      if (trace_->fail_readers.count(topic) != 0) {
        return make_error_code(TransportErrc::kIo);
      }
    }
    return FakeDdsProvider::Subscribe(topic, std::move(cb));
  }

  Coro::Result<void> Unsubscribe(const std::string& topic) override {
    {
      std::lock_guard<std::mutex> lock(trace_->m);
      trace_->undeclared.push_back(topic);
    }
    return FakeDdsProvider::Unsubscribe(topic);
  }

 private:
  std::shared_ptr<ProviderTrace> trace_;
};

/// 一条独立的 Fake 总线 + 一个只属于本用例的 provider 注册名(与 dds_node_test 同形)。
struct Fixture {
  std::shared_ptr<FakeDdsProvider::Bus> bus =
      std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<ProviderTrace> trace = std::make_shared<ProviderTrace>();
  std::string provider_name = "fake-dyn-bus-" + std::to_string(NextFixtureId());

  Fixture() {
    DdsProviderRegistry::RegisterProvider(
        provider_name, [bus = bus, trace = trace] {
          return std::unique_ptr<IDdsProvider>(new TracingProvider(bus, trace));
        });
  }

  [[nodiscard]] DdsConfig Cfg() const {
    DdsConfig config;
    config.provider = provider_name;
    return config;
  }
};

/// "节点 + 它借用的那条传输"(与 dds_node_test 的 `Host` 同形)。
class Host {
 public:
  explicit Host(const Fixture& fixture, std::string uuid = {})
      : transport_(fixture.Cfg()) {
    DdsNodeConfig config;
    config.uuid_override = std::move(uuid);
    node_ = std::make_unique<DdsNode>(transport_, std::make_unique<DdsCodec>(),
                                      std::move(config));
  }

  ~Host() {
    node_.reset();  // 节点先收敛,再关传输(节点借着它)。
    (void)transport_.Close();
    transport_.WaitClosed();
  }

  Host(const Host&) = delete;
  Host& operator=(const Host&) = delete;

  void StartTransport() {
    ASSERT_TRUE(static_cast<bool>(transport_.Start()));
  }

  [[nodiscard]] DdsNode& node() { return *node_; }
  [[nodiscard]] DdsTransport& transport() { return transport_; }

 private:
  DdsTransport transport_;
  std::unique_ptr<DdsNode> node_;  ///< 后声明 ⇒ 先析构。
};

/// 订阅消费小件(与 dds_node_test 的 `Subscriber` 同形)。
class Subscriber {
 public:
  Subscriber(DdsNode::Ticket ticket, std::function<void(const Message&)> on_message)
      : ticket_(std::move(ticket)), mailbox_(ticket_.mailbox()) {
    task_ = std::make_shared<Coro::FiberTask<void>>(
        Coro::makeTask([this, on_message = std::move(on_message)] {
          for (;;) {
            Coro::Result<Message, std::error_code> msg = Coro::await(mailbox_);
            if (!msg) {
              break;
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
    (void)task_->get();
    task_.reset();
  }

 private:
  DdsNode::Ticket ticket_;
  std::shared_ptr<Coro::Awaitable<Message>> mailbox_;
  std::shared_ptr<Coro::FiberTask<void>> task_;
};

/// 挂在同一条 Fake 总线上的线缆探针:按 topic 收原始字节(与 dds_node_test 同形)。
class WireTap {
 public:
  WireTap(const Fixture& fixture, const std::string& topic)
      : provider_(fixture.bus) {
    EXPECT_TRUE(static_cast<bool>(provider_.Init(fixture.Cfg())));
    EXPECT_TRUE(static_cast<bool>(
        provider_.Subscribe(topic, [this](const Bytes& bytes) {
          std::lock_guard<std::mutex> lock(mutex_);
          ++count_;
          (void)bytes;
        })));
  }
  ~WireTap() { provider_.Shutdown(); }

  WireTap(const WireTap&) = delete;
  WireTap& operator=(const WireTap&) = delete;

  [[nodiscard]] std::size_t Count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
  }

 private:
  mutable std::mutex mutex_;
  std::size_t count_ = 0;
  FakeDdsProvider provider_;
};

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

/// "本该收不到"这类否定断言:泵一小会儿让该到的都到齐,再看计数。
///
/// Fake 总线是**同步分发**的(`Publish` 在写线程上直接调 sink),故一条本该到的消息只需
/// 泵到读循环跑一轮;泵满上限意味着它确实没来。
void PumpAWhile() {
  (void)pumpFiberUntil([] { return false; }, 120);
}

}  // namespace

// ── 1. Running 期注册:端点当场建出,立刻收发能通(D1)────────────────────

TEST(DdsNodeDynamicRegistration, RegisteringWhileRunningDeliversImmediately) {
  Fixture fixture;
  Host publisher(fixture);
  Host subscriber(fixture);
  publisher.StartTransport();
  subscriber.StartTransport();

  // 两个节点都**一项注册都没有**就启动——D5 撤销了"四组全空即 kConfiguration"。
  ASSERT_TRUE(static_cast<bool>(publisher.node().Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().Start()));

  // 启动之后才知道要收发哪条 topic:端点在注册的**当场**建出。
  ASSERT_TRUE(static_cast<bool>(publisher.node().RegisterPublishers({"late"})));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().RegisterSubscribers({"late"})));

  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(
      MustSubscribe(subscriber.node(), std::string("late"), MessageKind::kNotify),
      [seen](const Message& msg) { seen->push_back(Text(msg)); });

  // Fake 总线没有发现窗口,故这条是确定的(真实 DDS 上首帧会丢——ADR-0015
  // 「明确接受的代价」①,风险由宿主自行评估处置)。
  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("late", Payload("hi"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 1; }));
  ASSERT_EQ(seen->size(), 1u);
  EXPECT_EQ(seen->front(), "hi");
}

// 请求-响应两侧也一样:两个节点全空启动,运行期各自补上自己那一侧的注册即通。
TEST(DdsNodeDynamicRegistration, RequestResponseWorksAfterRunningRegistration) {
  Fixture fixture;
  Host client(fixture, "dyn-cli");
  Host server(fixture, "dyn-srv");
  client.StartTransport();
  server.StartTransport();
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  // 补注册之前:服务名查不到 ⇒ kConfiguration(不猜、不回落)。
  EXPECT_EQ(
      client.node().RequestForResultDirect("get", Payload("ping"), {kCaseTimeout, 1})
          .error(),
      make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(server.node().ServeRequests("get").error(),
            make_error_code(TransportErrc::kConfiguration));

  ASSERT_TRUE(static_cast<bool>(client.node().RegisterClients({"get"})));
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"get"})));

  auto serve = server.node().ServeRequests("get");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(), [&server](const Message& request) {
    (void)server.node().Reply(request, Payload("pong"));
  });

  auto reply = client.node().RequestForResultDirect("get", Payload("ping"),
                                                    {kCaseTimeout, 3});
  ASSERT_TRUE(static_cast<bool>(reply)) << reply.error().message();
  EXPECT_EQ(Text(reply.value()), "pong");
  // 应答确实走的是派生出来的那条(字面量写死,不复用被测的派生函数)。
  EXPECT_EQ(reply.value().topic, "cfg.get.response");
}

// ── 2. ⭐ 注销:该 topic 确实不再收发;重新注册后恢复正常(D2)─────────────

// **读侧承重**:`DecodeAndDispatch` / `Dispatch` 完全不触碰四个注册集合,故"注销之后收不
// 到了"**只可能**是 `DataReader` 真被拆了——注册表里少一项对读路径毫无影响。
//
// 手里的 `Ticket` 全程不换,重新注册之后**还是它**收到消息:注销拆的是端点,不是订阅。
TEST(DdsNodeDynamicRegistration, UnregisterStopsReceivingAndReRegisterRestoresIt) {
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
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 1; }));

  // ── 注销:reader 当场拆掉 ──
  ASSERT_TRUE(static_cast<bool>(subscriber.node().UnregisterSubscribers({"news"})));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("news", Payload("two"))));
  PumpAWhile();
  EXPECT_EQ(seen->size(), 1u) << "注销之后这条不该再到达";
  // 订阅面也随之收紧:`Subscribe` 查不到该 topic 的读侧角色了。
  EXPECT_EQ(
      subscriber.node().Subscribe(std::string("news"), MessageKind::kNotify).error(),
      make_error_code(TransportErrc::kConfiguration));

  // ── 重新注册:reader 再建出来,收发恢复正常 ──
  ASSERT_TRUE(static_cast<bool>(subscriber.node().RegisterSubscribers({"news"})));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Publish("news", Payload("three"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 2; }));
  ASSERT_EQ(seen->size(), 2u);
  EXPECT_EQ((*seen)[0], "one");
  // ★ "two" 是真丢了,不是迟到——中间那条**不在**序列里。
  EXPECT_EQ((*seen)[1], "three");
}

// 写侧的对称面。注册表这一层的效果(`Publish` 返 kConfiguration)是浅的,故还要看**端点
// 那一层**:绕过节点、直接往传输上写这条 topic,provider 因"未声明"报 kConfiguration 并落
// 到 `LastError()`——writer 确实没了。
TEST(DdsNodeDynamicRegistration, UnregisterTearsDownTheWriterNotJustTheRegistryEntry) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"telemetry"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  WireTap tap(fixture, "telemetry");
  ASSERT_TRUE(static_cast<bool>(node.Publish("telemetry", Payload("x"))));
  EXPECT_TRUE(pumpFiberUntil([&tap] { return tap.Count() == 1; }));

  ASSERT_TRUE(static_cast<bool>(node.UnregisterPublishers({"telemetry"})));
  // ① 注册面:与"从没注册过"完全一样。
  EXPECT_EQ(node.Publish("telemetry", Payload("y")).error(),
            make_error_code(TransportErrc::kConfiguration));
  // ② 端点面:绕过节点直接写,写线程上得到"未声明"的 kConfiguration。
  ASSERT_TRUE(static_cast<bool>(host.transport().AsyncWrite(
      Datagram{Bytes{1, 2, 3}, Endpoint::Topic("telemetry")})));
  EXPECT_TRUE(pumpFiberUntil([&host] {
    return host.transport().LastError() ==
           make_error_code(TransportErrc::kConfiguration);
  }));
  EXPECT_EQ(tap.Count(), 1u) << "writer 已拆,这一帧发不出去";

  // 重新注册即恢复。
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"telemetry"})));
  ASSERT_TRUE(static_cast<bool>(node.Publish("telemetry", Payload("z"))));
  EXPECT_TRUE(pumpFiberUntil([&tap] { return tap.Count() == 2; }));
}

// ── 3. ⭐ 同一 topic 的两个方向互不牵连(D3 的可观测形状)──────────────────

// `loop` 同时在 `Publishers`(W)与 `Subscribers`(R):这是**两个注册项、两条端点**,
// 各自恰有一个注册项负责。故注销其一之后另一侧**仍正常工作**——注销路径直接拆自己那条,
// 既不误伤对面,也不需要回头重算"这条 topic 还有别人要吗"。
TEST(DdsNodeDynamicRegistration, UnregisteringOneDirectionLeavesTheOtherWorking) {
  Fixture fixture;
  Host host(fixture);
  Host peer(fixture);
  host.StartTransport();
  peer.StartTransport();

  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(peer.node().RegisterPublishers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));
  ASSERT_TRUE(static_cast<bool>(peer.node().Start()));

  auto seen = std::make_shared<std::vector<std::string>>();
  Subscriber sub(MustSubscribe(node, std::string("loop"), MessageKind::kNotify),
                 [seen](const Message& msg) { seen->push_back(Text(msg)); });

  WireTap tap(fixture, "loop");
  ASSERT_TRUE(static_cast<bool>(node.Publish("loop", Payload("a"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 1; }));

  // ── 只注销读侧:写侧照常发得出去 ──
  ASSERT_TRUE(static_cast<bool>(node.UnregisterSubscribers({"loop"})));
  const std::size_t before = tap.Count();
  ASSERT_TRUE(static_cast<bool>(node.Publish("loop", Payload("b"))))
      << "写侧不该被读侧的注销牵连";
  EXPECT_TRUE(pumpFiberUntil([&tap, before] { return tap.Count() == before + 1; }));
  PumpAWhile();
  EXPECT_EQ(seen->size(), 1u) << "读侧已拆,自己发的也收不到了";

  // ── 换个方向:恢复读侧、只注销写侧 ──
  ASSERT_TRUE(static_cast<bool>(node.RegisterSubscribers({"loop"})));
  ASSERT_TRUE(static_cast<bool>(node.UnregisterPublishers({"loop"})));
  EXPECT_EQ(node.Publish("loop", Payload("c")).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 读侧仍好好的:别人发的照收。
  ASSERT_TRUE(static_cast<bool>(peer.node().Publish("loop", Payload("d"))));
  EXPECT_TRUE(pumpFiberUntil([seen] { return seen->size() == 2; }));
  ASSERT_EQ(seen->size(), 2u);
  EXPECT_EQ((*seen)[1], "d");
}

// 请求-响应两组也各占自己的两条端点:同一个服务名分别注册在两个节点上,一侧注销与另一侧
// 无关(这条守的是"注销只动自己节点的注册表",跨节点没有隐含耦合)。
TEST(DdsNodeDynamicRegistration, UnregisteringClientDoesNotDisturbTheServer) {
  Fixture fixture;
  Host client(fixture, "dyn-c");
  Host server(fixture, "dyn-s");
  client.StartTransport();
  server.StartTransport();
  ASSERT_TRUE(static_cast<bool>(client.node().RegisterClients({"get"})));
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"get"})));
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  auto serve = server.node().ServeRequests("get");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(), [&server](const Message& request) {
    (void)server.node().Reply(request, Payload("pong"));
  });

  ASSERT_TRUE(static_cast<bool>(client.node().RequestForResultDirect(
      "get", Payload("ping"), {kCaseTimeout, 3})));

  // 客户端注销:它自己那两条端点没了,服务端一无所感。
  ASSERT_TRUE(static_cast<bool>(client.node().UnregisterClients({"get"})));
  EXPECT_EQ(client.node()
                .RequestForResultDirect("get", Payload("ping"), {kCaseTimeout, 1})
                .error(),
            make_error_code(TransportErrc::kConfiguration));
  // 服务端照旧能服务——重新注册的客户端立刻又通了。
  ASSERT_TRUE(static_cast<bool>(client.node().RegisterClients({"get"})));
  auto again = client.node().RequestForResultDirect("get", Payload("ping"),
                                                    {kCaseTimeout, 3});
  EXPECT_TRUE(static_cast<bool>(again)) << again.error().message();
}

// ── 4. ⭐ 在途交互不检测:Ticket 仍有效,Reply 返 kConfiguration(D6)────────

TEST(DdsNodeDynamicRegistration, UnregisteringAServedServiceKeepsTheTicketButFailsReply) {
  Fixture fixture;
  Host client(fixture, "dyn-c2");
  Host server(fixture, "dyn-s2");
  client.StartTransport();
  server.StartTransport();
  ASSERT_TRUE(static_cast<bool>(client.node().RegisterClients({"get"})));
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"get"})));
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  // 服务端把收到的请求攒下来,**先不应答**——注销要在"手里正拿着一条请求"时发生。
  auto inbox = std::make_shared<std::vector<Message>>();
  auto serve = server.node().ServeRequests("get");
  ASSERT_TRUE(static_cast<bool>(serve)) << serve.error().message();
  Subscriber svc(std::move(serve).value(),
                 [inbox](const Message& request) { inbox->push_back(request); });

  // 客户端只发一次(不等结果:没人应答,等下去只会超时)。
  auto request_task = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([&client] {
    (void)client.node().RequestForResultDirect("get", Payload("ping"),
                                               {kCaseTimeout, 1});
  }));
  EXPECT_TRUE(pumpFiberUntil([inbox] { return inbox->size() == 1; }));
  ASSERT_EQ(inbox->size(), 1u);
  const Message in_hand = inbox->front();

  // ── 注销一个**正在被服务**的服务名:不被拒绝、不等在途结束 ──
  ASSERT_TRUE(static_cast<bool>(server.node().UnregisterServices({"get"})));

  // ① 对已注销服务的 `Reply()` 返 kConfiguration(反查落空,与"从没注册过"一样)。
  EXPECT_EQ(server.node().Reply(in_hand, Payload("pong")).error(),
            make_error_code(TransportErrc::kConfiguration));
  (void)request_task->get();  // 客户端那一路自行超时终结。

  // ② 已持有的 `Ticket` **仍有效**——它挂在 `Dispatcher` 上,与注册表无关。
  //    证据不是"没崩",而是:重新注册把 request reader 建回来之后,**还是这张原票**
  //    收到了新请求(全程没有再调过一次 `ServeRequests`)。
  ASSERT_TRUE(static_cast<bool>(server.node().RegisterServices({"get"})));
  auto second = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([&client, &server, inbox] {
    (void)client.node().RequestForResultDirect("get", Payload("ping2"),
                                               {kCaseTimeout, 1});
    (void)server;
    (void)inbox;
  }));
  EXPECT_TRUE(pumpFiberUntil([inbox] { return inbox->size() == 2; }));
  ASSERT_EQ(inbox->size(), 2u);
  EXPECT_EQ(Text(inbox->back()), "ping2");
  // 服务名回来了,`Reply` 也随之恢复。
  EXPECT_TRUE(static_cast<bool>(server.node().Reply(inbox->back(), Payload("pong"))));
  (void)second->get();
}

// ── 5. ⭐ Running 期批量注册中途失败的回滚(D7)────────────────────────────

// 次序是**校验 → 逐项建端点 → 提交集合**,故中途失败时:注册表**一项不落**(它还没被动过),
// 且**本批已建的端点已被拆掉**(后者只有 provider 看得见,故由 `ProviderTrace` 作证)。
TEST(DdsNodeDynamicRegistration, RunningBatchRollsBackEndpointsBuiltSoFar) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"kept"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  {
    std::lock_guard<std::mutex> lock(fixture.trace->m);
    fixture.trace->fail_writers.insert("boom");
  }

  // 一批三项,第二项建端点失败 ⇒ 整批不生效,错误原样透出(provider 报的 kIo)。
  EXPECT_EQ(node.RegisterPublishers({"first", "boom", "third"}).error(),
            make_error_code(TransportErrc::kIo));

  // ① 注册表一项不落——连**失败之前**那项也没落。
  EXPECT_EQ(node.Publish("first", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Publish("third", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
  // ② 本批已建的端点已被拆掉(不是"留在那儿反正没人用")。
  EXPECT_TRUE(fixture.trace->WasUndeclared("first"));
  // ③ **先前批次的端点毫发无伤**:回滚清单只记本批真正新建的项。
  EXPECT_FALSE(fixture.trace->WasUndeclared("kept"));
  EXPECT_TRUE(static_cast<bool>(node.Publish("kept", Payload("x"))));

  // 去掉故障源再来一次,整批照常落地。
  {
    std::lock_guard<std::mutex> lock(fixture.trace->m);
    fixture.trace->fail_writers.clear();
  }
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"first", "boom", "third"})));
  EXPECT_TRUE(static_cast<bool>(node.Publish("first", Payload("x"))));
  EXPECT_TRUE(static_cast<bool>(node.Publish("boom", Payload("x"))));
  EXPECT_TRUE(static_cast<bool>(node.Publish("third", Payload("x"))));
}

// 成对派生的那两组回滚要连**半边**一起拆:`Clients` 的一项是 request 的 W + response 的 R,
// 后者失败时前者也不许留下——否则会剩一条谁也不负责的端点。
TEST(DdsNodeDynamicRegistration, RollbackAlsoTearsDownTheHalfBuiltPairedItem) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  // `cfg.b.response` 的 reader 建不出来 ⇒ 第二项 `b` 建到一半失败。
  {
    std::lock_guard<std::mutex> lock(fixture.trace->m);
    fixture.trace->fail_readers.insert("cfg.b.response");
  }

  EXPECT_EQ(node.RegisterClients({"a", "b"}).error(),
            make_error_code(TransportErrc::kIo));

  // 注册表一项不落。
  EXPECT_EQ(node.RequestForResultDirect("a", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.RequestForResultDirect("b", Payload("x"), {kCaseTimeout, 1}).error(),
            make_error_code(TransportErrc::kConfiguration));
  // 已建成的整项 `a` 两条端点都拆了;失败项 `b` 建了一半的那条 writer 也拆了。
  EXPECT_TRUE(fixture.trace->WasUndeclared("cfg.a.request"));
  EXPECT_TRUE(fixture.trace->WasUndeclared("cfg.a.response"));
  EXPECT_TRUE(fixture.trace->WasUndeclared("cfg.b.request"));
}

// ── 6. Created 期的注销 + 幂等(D2)───────────────────────────────────────

TEST(DdsNodeDynamicRegistration, UnregisterInCreatedOnlyRemovesFromTheRegistry) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();

  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"a", "b"})));
  ASSERT_TRUE(static_cast<bool>(node.RegisterServices({"svc"})));
  // `Created` 期本就没建过端点,故这里只从集合移除——**provider 上一次拆除都不该发生**。
  ASSERT_TRUE(static_cast<bool>(node.UnregisterPublishers({"a"})));
  ASSERT_TRUE(static_cast<bool>(node.UnregisterServices({"svc"})));
  EXPECT_FALSE(fixture.trace->WasUndeclared("a"));
  EXPECT_FALSE(fixture.trace->WasUndeclared("cfg.svc.request"));

  ASSERT_TRUE(static_cast<bool>(node.Start()));
  EXPECT_EQ(node.Publish("a", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
  EXPECT_TRUE(static_cast<bool>(node.Publish("b", Payload("x"))));
  EXPECT_EQ(node.ServeRequests("svc").error(),
            make_error_code(TransportErrc::kConfiguration));
}

// 不在册的项是**幂等空操作**:注销的语义是"确保它不在",不是"它此刻必须在"。
TEST(DdsNodeDynamicRegistration, UnregisteringSomethingNeverRegisteredIsANoOp) {
  Fixture fixture;
  Host host(fixture);
  host.StartTransport();
  DdsNode& node = host.node();
  ASSERT_TRUE(static_cast<bool>(node.RegisterPublishers({"a"})));
  ASSERT_TRUE(static_cast<bool>(node.Start()));

  EXPECT_TRUE(static_cast<bool>(node.UnregisterPublishers({"never"})));
  EXPECT_TRUE(static_cast<bool>(node.UnregisterSubscribers({"never", ""})));
  EXPECT_TRUE(static_cast<bool>(node.UnregisterClients({"never"})));
  EXPECT_TRUE(static_cast<bool>(node.UnregisterServices({"never"})));
  // 连拆都没往 provider 上落——不在册的项不是本节点建的,不能替别人拆。
  EXPECT_FALSE(fixture.trace->WasUndeclared("never"));
  // 重复注销同一项也一样。
  EXPECT_TRUE(static_cast<bool>(node.UnregisterPublishers({"a"})));
  EXPECT_TRUE(static_cast<bool>(node.UnregisterPublishers({"a"})));
  EXPECT_EQ(node.Publish("a", Payload("x")).error(),
            make_error_code(TransportErrc::kConfiguration));
}
