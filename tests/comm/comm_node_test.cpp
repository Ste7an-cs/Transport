#include "transport/comm/CommNode.hpp"

#include "transport/codec/SystemCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include <gtest/gtest.h>

using transport::CommNode;
using transport::Message;
using transport::MessageKind;
using transport::Responder;
using transport::Result;
using transport::SystemCodec;
using testutil::FakeTransport;
using testutil::InlineExecutor;
using namespace std::chrono_literals;

namespace {
Message Msg(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

// 测试用子类:把收到的请求/消息记录或回显。
class EchoNode : public CommNode {
 public:
  using CommNode::CommNode;
  std::vector<uint8_t> last_msg;
  void OnMessage(const Message& m) override { last_msg = m.payload; }
  void OnRequest(const Message& req, Responder r) override {
    auto out = req.payload; out.push_back(0xFF);
    (void)r.Reply(Msg(out));  // 应答 = 请求 payload + 0xFF
  }
};

// 把两个 EchoNode 经一对 FakeTransport + InlineExecutor(确定性)接起来。
struct Pair {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<EchoNode> a, b;
  InlineExecutor* exa = nullptr; InlineExecutor* exb = nullptr;
  Pair() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    auto eb = std::make_unique<InlineExecutor>(); exb = eb.get();
    a = std::make_shared<EchoNode>(ta, std::make_unique<SystemCodec>(), std::move(ea));
    b = std::make_shared<EchoNode>(tb, std::make_unique<SystemCodec>(), std::move(eb));
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(CommNode, OnewaySend) {
  Pair p; p.Open();
  ASSERT_TRUE(static_cast<bool>(p.a->Send(Msg({1, 2, 3}))));
  EXPECT_EQ(p.b->last_msg, (std::vector<uint8_t>{1, 2, 3}));  // InlineExecutor 同步交付
  p.Close();
}

TEST(CommNode, RequestReplyCallback) {
  Pair p; p.Open();
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->Request(Msg({5}), [&](Result<Message> r) { got = std::move(r); }, 1000)));
  ASSERT_TRUE(static_cast<bool>(got));                 // InlineExecutor 全同步:已回
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xFF}));
  p.Close();
}

TEST(CommNode, RequestReplyFuture) {
  Pair p; p.Open();
  auto fut = p.a->Request(Msg({7}), 1000);
  auto r = fut.get();                                  // 同步执行器下即时就绪
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 0xFF}));
  p.Close();
}

TEST(CommNode, RequestTimeout) {
  // server 不应答:用一个不回 reply 的子类
  class Silent : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder) override {} };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto ea = std::make_unique<InlineExecutor>(); auto* pea = ea.get();
  auto a = std::make_shared<Silent>(ta, std::make_unique<SystemCodec>(), std::move(ea));
  auto b = std::make_shared<Silent>(tb, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  Result<Message> got = Result<Message>::Fail("none");
  (void)a->Request(Msg({1}), [&](Result<Message> r) { got = std::move(r); }, 50);
  EXPECT_FALSE(static_cast<bool>(got));   // 还没超时(未应答,仍为初始 Fail)
  pea->FireAll();                          // 驱动 a 的超时定时器
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  a->Close(); b->Close();
}

TEST(CommNode, FeedbackThenFinal) {
  class Worker : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder r) override {
      (void)r.Feedback(Msg({0x01}));
      (void)r.Feedback(Msg({0x02}));
      (void)r.Reply(Msg({0xEE}));
    } };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<Worker>(ta, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  auto b = std::make_shared<Worker>(tb, std::make_unique<SystemCodec>(),
                                    std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  std::vector<std::vector<uint8_t>> feedbacks; Result<Message> fin = Result<Message>::Fail("none");
  (void)a->Request(Msg({9}),
                   [&](const Message& m) { feedbacks.push_back(m.payload); },
                   [&](Result<Message> r) { fin = std::move(r); }, 1000);
  ASSERT_EQ(feedbacks.size(), 2u);
  EXPECT_EQ(feedbacks[0], (std::vector<uint8_t>{0x01}));
  EXPECT_EQ(feedbacks[1], (std::vector<uint8_t>{0x02}));
  ASSERT_TRUE(static_cast<bool>(fin));
  EXPECT_EQ(fin.value.payload, (std::vector<uint8_t>{0xEE}));
  a->Close(); b->Close();
}

TEST(CommNode, DisconnectFinalizesPending) {
  // server 持有请求不回(长超时),再关对端 → 挂起以 conn: 终结。
  Result<Message> got = Result<Message>::Success(Message{});
  class Hold : public CommNode { public: using CommNode::CommNode;
    void OnRequest(const Message&, Responder) override {} };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<Hold>(ta, std::make_unique<SystemCodec>(),
                                  std::make_unique<InlineExecutor>());
  auto b = std::make_shared<Hold>(tb, std::make_unique<SystemCodec>(),
                                  std::make_unique<InlineExecutor>());
  (void)b->Open(); (void)a->Open();
  (void)a->Request(Msg({1}), [&](Result<Message> r) { got = std::move(r); }, 100000);
  EXPECT_TRUE(static_cast<bool>(got)) << "still pending, untouched";  // 初值 Success,未被调
  b->Close();   // 对端断 → a 收 OnDisconnect → 终结挂起(conn:)
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("conn:", 0), 0u);
  a->Close();
}

// FIX 1:用户 hook 在 worker 线程内调 Close() → ThreadExecutor::Stop() 不能 join 自身。
// 自连接守卫:改 detach,进程不得 terminate;调用须返回。
TEST(CommNode, HookCallsCloseFromWorkerNoSelfJoinCrash) {
  class SelfCloser : public CommNode {
   public:
    using CommNode::CommNode;
    std::promise<void> closed;
    void OnMessage(const Message&) override {
      this->Close();          // worker 线程内 → 触发 ThreadExecutor::Stop() 自连接路径
      closed.set_value();
    }
  };
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  // a 用真实 ThreadExecutor(executor=nullptr);b 同步交付 oneway 给 a。
  auto a = std::make_shared<SelfCloser>(ta, std::make_unique<SystemCodec>(), nullptr);
  auto b = std::make_shared<EchoNode>(tb, std::make_unique<SystemCodec>(), nullptr);
  auto fut = a->closed.get_future();
  (void)a->Open(); (void)b->Open();
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({1, 2, 3}))));  // → a 的 worker 跑 OnMessage→Close()
  // 确定性等待:hook 完成(不靠纯 sleep)。守卫生效则按时就绪;否则进程早已 terminate。
  ASSERT_EQ(fut.wait_for(2s), std::future_status::ready) << "hook 未完成(疑自连接 crash)";
  EXPECT_FALSE(a->IsOpen());
  b->Close();
}

// 执行器可换性:同一交互逻辑换 ThreadExecutor 也通(真实线程)。
TEST(CommNode, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<EchoNode>(ta, std::make_unique<SystemCodec>(), nullptr);  // 默认 ThreadExecutor
  auto b = std::make_shared<EchoNode>(tb, std::make_unique<SystemCodec>(), nullptr);
  (void)b->Open(); (void)a->Open();
  auto r = a->Request(Msg({3}), 2000).get();   // future 等回(真实线程)
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xFF}));
  a->Close(); b->Close();
}
