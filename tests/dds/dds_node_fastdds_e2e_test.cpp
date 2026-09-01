// -----------------------------------------------------------------------------
// dds_node_fastdds_e2e_test.cpp — **真实 Fast DDS 上**的 DdsNode 端到端(ADR-0013)。
// 只在 TRANSPORT_HAS_FASTDDS 时编入(CMakeLists);Fast DDS 缺席时整份不参与构建。
//
// 其余 DDS 用例一律跑在 `FakeDdsProvider` 上——那是有意的:Fake 同步分发、无发现窗口,
// 逐条断言才立得住。但整条链路 `DdsNode` → `DdsTransport` → `FastDdsProvider` 从未在
// **真实介质**上合起来跑过一次:`fast_dds_provider_test` 只到 provider 面为止,Fake 又
// 恰好抹掉了真实 DDS 唯一的那处形态差异——**约 240ms 的发现窗口**(**D9**)。本文件补的
// 就是这一口:两种交互模式各一条烟雾用例。
//
// ⚠ **时间断言的纪律**:发现窗口不是常量,故本文件**不写任何"多久之内必须完成"的断言**,
//   一切等待都写成「轮询到条件成立,或耗尽预算」(`pumpFiberUntil`,它让出 fiber 而不是
//   park 线程——节点的读-分发循环就跑在同一条线程的另一条 fiber 上,睡线程会把它一起睡死)。
//   请求-响应侧则直接用 `RetryPolicy` 的重发吸收窗口:首帧若抢在匹配之前发出会永久丢失
//   (VOLATILE 不补发),这正是重发要救的那一段。
//
// domain 取 94 / 95,与 `fast_dds_provider_test`(88–93)错开,避免同一台机器上互串。
// -----------------------------------------------------------------------------
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "task/fibertask.h"  // Coro::makeTask —— 服务端那条消费 fiber。
#include "transport/codec/DdsCodec.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/RetryPolicy.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsTransport;
using transport::LinkState;
using transport::Message;
using transport::MessageKind;
using transport::RetryPolicy;

namespace {

/// 真实 provider 上的等待预算:发现窗口约 240ms,给足一个数量级的余量。
/// **只当预算用,不当断言用**——没有任何一条断言依赖它有多大。
constexpr int kDiscoveryBudgetMs = 10000;

DdsConfig Cfg(int domain) {
  DdsConfig config;
  config.domain_id = domain;
  config.provider = "fastdds";
  config.qos.history_depth = 10;
  config.qos.max_blocking_time = 200ms;
  config.qos.liveliness_lease = 1000ms;
  return config;
}

/// 宿主:建传输、启传输、把引用交给节点;析构里**先收敛节点、再关传输**。
/// 与 dds_node_test 的 `Host` 同形,只是配置换成真实 provider。
class Host {
 public:
  explicit Host(int domain) : transport_(Cfg(domain)) {
    node_ = std::make_unique<DdsNode>(transport_, std::make_unique<DdsCodec>());
  }
  ~Host() {
    node_.reset();
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

std::string Text(const Message& msg) {
  return std::string(msg.payload.begin(), msg.payload.end());
}

Message Payload(const std::string& text) {
  Message msg;
  msg.payload.assign(text.begin(), text.end());
  return msg;
}

/// 走完发现窗口:等到该传输看得见对端(`CurrentLinkState() == kUp`,由 provider 的
/// `MatchedCount()` 推出,**D9**)。轮询式,不写死 sleep。
bool WaitLinkUp(DdsTransport& transport) {
  return pumpFiberUntil(
      [&transport] { return transport.CurrentLinkState() == LinkState::kUp; },
      kDiscoveryBudgetMs);
}

}  // namespace

// 发布-订阅:`RegisterPublishers` / `RegisterSubscribers` → `Publish` → 订阅方收到。
//
// 先等到链路 kUp 再发:writer 已在 `DoStart()` 建出(**D15**/**D13** 补正),但匹配仍需
// 走完发现窗口,抢在匹配之前发出的那一帧 VOLATILE 不会补发。
TEST(DdsNodeFastDds, PublishSubscribeRoundTripOverRealDds) {
  Host publisher(94);
  Host subscriber(94);
  publisher.StartTransport();
  subscriber.StartTransport();

  ASSERT_TRUE(static_cast<bool>(
      publisher.node().RegisterPublishers({"e2e.news"})));
  ASSERT_TRUE(static_cast<bool>(
      subscriber.node().RegisterSubscribers({"e2e.news"})));
  ASSERT_TRUE(static_cast<bool>(publisher.node().Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber.node().Start()));

  auto ticket = subscriber.node().Subscribe(std::string("e2e.news"),
                                            MessageKind::kNotify);
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  DdsNode::Ticket mailbox = std::move(ticket).value();

  ASSERT_TRUE(WaitLinkUp(publisher.transport())) << "发布侧一直没匹配上对端";
  ASSERT_TRUE(WaitLinkUp(subscriber.transport())) << "订阅侧一直没匹配上对端";

  ASSERT_TRUE(static_cast<bool>(
      publisher.node().Publish("e2e.news", Payload("over-the-wire"))));

  auto got = mailbox.Wait(std::chrono::milliseconds(kDiscoveryBudgetMs));
  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "over-the-wire");
  EXPECT_EQ(got.value().kind, MessageKind::kNotify);
  // topic **不上线缆**(**D5**):它由入站 `Datagram.peer` 带出、在读循环里填回。
  EXPECT_EQ(got.value().topic, "e2e.news");
  EXPECT_FALSE(publisher.transport().LastError())
      << publisher.transport().LastError().message();
}

// 请求-响应:`RegisterClients` / `RegisterServices` → `RequestForResultDirect` →
// 服务端 `Subscribe(topic, kRequest)` 收到 → `Reply` → 客户端拿到结果。
//
// 这里**不等 kUp**,而是让 `RetryPolicy` 去吸收发现窗口——这正是 D7 那句"丢的不是网络,
// 是队列……重发是对这一段的补救"在真实介质上的形态:首帧若抢在匹配之前发出就永久丢失,
// 一秒后的重发落在匹配之后,当场成事。
TEST(DdsNodeFastDds, RequestResponseRoundTripOverRealDds) {
  Host client(95);
  Host server(95);
  client.StartTransport();
  server.StartTransport();

  // 两侧**传一模一样的实参**,各自按角色建各自那一侧(**D16**)。
  ASSERT_TRUE(static_cast<bool>(
      client.node().RegisterClients({{"e2e.svc", "e2e.svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(
      server.node().RegisterServices({{"e2e.svc", "e2e.svc.reply"}})));
  ASSERT_TRUE(static_cast<bool>(client.node().Start()));
  ASSERT_TRUE(static_cast<bool>(server.node().Start()));

  auto ticket =
      server.node().Subscribe(std::string("e2e.svc"), MessageKind::kRequest);
  ASSERT_TRUE(static_cast<bool>(ticket)) << ticket.error().message();
  DdsNode::Ticket requests = std::move(ticket).value();

  // 服务端跑在自己的 fiber 上:收一条请求就回一条应答(应答 topic 从自己的 Services
  // 表查,**不取信于线缆**,**D15**)。**每条重发都照答**——首答本身也可能抢在匹配之前
  // 发出而丢失,那就等客户端的下一次重发。
  int served = 0;
  bool stop = false;
  auto service = Coro::makeTask([&] {
    while (!stop) {
      // 短时限轮询:让出 fiber 的同时也给收工标志一个观察点(**不 park 线程**)。
      auto request = requests.Wait(50ms);
      if (!request) {
        if (request.error() ==
            transport::make_error_code(transport::TransportErrc::kTimeout)) {
          continue;
        }
        break;  // 信箱关闭 → 收工。
      }
      ++served;
      (void)server.node().Reply(request.value(),
                                Payload("echo:" + Text(request.value())));
    }
  });

  auto got = client.node().RequestForResultDirect(
      "e2e.svc", Payload("ping"), RetryPolicy{1000ms, 8});
  stop = true;
  (void)service.get();  // 让出式 join:返回即服务端 fiber 已退出。

  ASSERT_TRUE(static_cast<bool>(got)) << got.error().message();
  EXPECT_EQ(Text(got.value()), "echo:ping");
  EXPECT_EQ(got.value().kind, MessageKind::kReply);
  EXPECT_EQ(got.value().topic, "e2e.svc.reply");
  // corr 两段式(**D6**):应答沿用请求那一份,故它必是本节点的 `<uuid>#0`。
  EXPECT_EQ(got.value().correlation_id, client.node().uuid() + "#0");
  EXPECT_GE(served, 1);
}
