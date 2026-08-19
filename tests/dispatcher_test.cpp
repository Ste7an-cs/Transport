// Dispatcher<T, Fields...> 单元测试:按键分配、部分匹配、多订阅者投递与生命周期。
//
// 采用一个与协议无关的最小消息类型,以验证本件不依赖任何具体协议:
//   Ev{ level, code, tag } —— 前两个字段参与匹配,tag 仅用于辨识收到的是哪一条消息。
#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>

#include <gtest/gtest.h>

#include "coro_test_util.hpp"
#include "task/fibertask.h"
#include "transport/core/Dispatcher.hpp"
#include "transport/core/Error.hpp"

using namespace std::chrono_literals;
using transport::Dispatcher;
using transport::kAny;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

struct Ev {
  std::uint8_t level = 0;
  std::uint16_t code = 0;
  std::string tag;
};

using EvDispatcher = Dispatcher<Ev, std::uint8_t, std::uint16_t>;

// 键提取函数:给出这条消息各匹配字段的具体值;通配由 Dispatcher 内部处理。
EvDispatcher MakeDispatcher() {
  return EvDispatcher(
      [](const Ev& e) { return std::make_tuple(e.level, e.code); });
}

}  // namespace

// —— 基本投递 ——————————————————————————————————————————————————————————

TEST(Dispatcher, ExactKeyDeliversToSubscriber) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "hit"}), 1u);
  auto got = ticket.Wait(100ms);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().tag, "hit");
}

TEST(Dispatcher, NonMatchingMessageIsNotDelivered) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});

  EXPECT_EQ(d.Dispatch(Ev{7, 11, "wrong-code"}), 0u);
  EXPECT_EQ(d.Dispatch(Ev{8, 10, "wrong-level"}), 0u);
  EXPECT_FALSE(ticket.Wait(50ms));  // 信箱中无任何消息
}

// 无匹配订阅时返回 0 且不修改 value,调用方据此转交处理器或归因丢弃。
TEST(Dispatcher, NoSubscriberReturnsZero) {
  auto d = MakeDispatcher();
  EXPECT_EQ(d.Dispatch(Ev{1, 2, "nobody"}), 0u);
}

// —— 部分匹配 ————————————————————————————————————————————————————————

TEST(Dispatcher, WildcardFieldMatchesAnyValue) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({kAny, 10});  // 只认 code==10,不问 level

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "a"}), 1u);
  EXPECT_EQ(d.Dispatch(Ev{99, 10, "b"}), 1u);
  EXPECT_EQ(d.Dispatch(Ev{7, 11, "c"}), 0u);  // code 不匹配，不投递

  auto first = ticket.Wait(100ms);
  ASSERT_TRUE(first);
  EXPECT_EQ(first.value().tag, "a");
  auto second = ticket.Wait(100ms);
  ASSERT_TRUE(second);
  EXPECT_EQ(second.value().tag, "b");  // 信箱为队列语义，同一凭据可多次等待
}

TEST(Dispatcher, AllWildcardMatchesEverything) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({kAny, kAny});

  EXPECT_EQ(d.Dispatch(Ev{1, 2, "x"}), 1u);
  EXPECT_EQ(d.Dispatch(Ev{200, 60000, "y"}), 1u);
}

// kAny 与数值 0 是两种不同的约束:通配以 optional 的持值状态表达,不占用字段值域,故
// level 这类取值范围已被占满(0..255)的字段同样可以通配,无需保留哨兵值。
TEST(Dispatcher, AnyIsNotTheZeroValue) {
  auto d = MakeDispatcher();
  auto zero = d.Subscribe({0, 10});     // level 必须等于 0
  auto any = d.Subscribe({kAny, 10});   // level 任意

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "level-7"}), 1u);  // 仅 any 命中
  EXPECT_FALSE(zero.Wait(50ms));
  auto got = any.Wait(50ms);
  ASSERT_TRUE(got);
  EXPECT_EQ(got.value().tag, "level-7");

  EXPECT_EQ(d.Dispatch(Ev{0, 10, "level-0"}), 2u);  // 两个订阅均命中
  auto z = zero.Wait(50ms);
  ASSERT_TRUE(z);
  EXPECT_EQ(z.value().tag, "level-0");
  auto a = any.Wait(50ms);
  ASSERT_TRUE(a);
  EXPECT_EQ(a.value().tag, "level-0");
}

// —— 多消费者 ————————————————————————————————————————————————————————

// 同一个键被多个订阅者登记时,各得一份副本,彼此不构成竞争。
TEST(Dispatcher, MultipleSubscribersOnSameKeyAllReceive) {
  auto d = MakeDispatcher();
  auto a = d.Subscribe({7, 10});
  auto b = d.Subscribe({7, 10});
  auto c = d.Subscribe({7, 10});

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "broadcast"}), 3u);
  for (auto* t : {&a, &b, &c}) {
    auto got = t->Wait(100ms);
    ASSERT_TRUE(got) << got.error().message();
    EXPECT_EQ(got.value().tag, "broadcast");
  }
}

// 不同粒度的订阅同时命中同一条消息:精确等待者与旁路监听者各得一份副本。
TEST(Dispatcher, DifferentGranularitiesBothReceive) {
  auto d = MakeDispatcher();
  auto precise = d.Subscribe({7, 10});
  auto audit = d.Subscribe({kAny, 10});

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "both"}), 2u);
  ASSERT_TRUE(precise.Wait(100ms));
  ASSERT_TRUE(audit.Wait(100ms));
}

// —— 索引行为(复杂度承诺的可观测面)——————————————————————————————————

// 单条消息的探测次数等于在用 mask 的种数,与订阅者总数无关。
TEST(Dispatcher, ProbeCountTracksDistinctMasksNotSubscribers) {
  auto d = MakeDispatcher();
  EXPECT_EQ(d.ProbeCount(), 0u);

  std::vector<EvDispatcher::Ticket> many;
  for (std::uint16_t code = 0; code < 200; ++code) {
    many.push_back(d.Subscribe({7, code}));  // 200 个订阅，同属一种 mask
  }
  EXPECT_EQ(d.Size(), 200u);
  EXPECT_EQ(d.ProbeCount(), 1u) << "同一 mask 下的订阅不应增加探测次数";

  auto wide = d.Subscribe({kAny, 10});  // 引入第二种 mask
  EXPECT_EQ(d.ProbeCount(), 2u);
  auto all = d.Subscribe({kAny, kAny});  // 引入第三种 mask
  EXPECT_EQ(d.ProbeCount(), 3u);
}

// 注销后 mask 须从索引中移除,否则 Dispatch 将持续探测已无订阅的 mask。
TEST(Dispatcher, TicketDestructionUnsubscribesAndPrunesIndex) {
  auto d = MakeDispatcher();
  {
    auto ticket = d.Subscribe({7, 10});
    EXPECT_EQ(d.Size(), 1u);
    EXPECT_EQ(d.ProbeCount(), 1u);
  }
  EXPECT_EQ(d.Size(), 0u);
  EXPECT_EQ(d.ProbeCount(), 0u) << "空 mask 未被清理";
  EXPECT_EQ(d.Dispatch(Ev{7, 10, "gone"}), 0u);
}

TEST(Dispatcher, ResetUnsubscribesEarly) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});
  ticket.Reset();
  EXPECT_FALSE(static_cast<bool>(ticket));
  EXPECT_EQ(d.Size(), 0u);
  EXPECT_EQ(d.Dispatch(Ev{7, 10, "gone"}), 0u);
}

TEST(Dispatcher, MovedTicketKeepsReceiving) {
  auto d = MakeDispatcher();
  auto original = d.Subscribe({7, 10});
  auto moved = std::move(original);
  EXPECT_EQ(d.Size(), 1u) << "移动不应产生第二次注销";

  EXPECT_EQ(d.Dispatch(Ev{7, 10, "moved"}), 1u);
  auto got = moved.Wait(100ms);
  ASSERT_TRUE(got);
  EXPECT_EQ(got.value().tag, "moved");
}

// —— 等待与终结 ————————————————————————————————————————————————————

TEST(Dispatcher, WaitTimesOut) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});
  auto got = ticket.Wait(50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kTimeout));
}

// CloseAll 使在途等待恰好终结一次,并透出终止原因。
TEST(Dispatcher, CloseAllWakesPendingWaiterWithReason) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});

  Coro::Result<Ev> got = make_error_code(TransportErrc::kInternal);
  auto waiter = Coro::makeTask([&] { got = ticket.Wait(2000ms); });
  boost::this_fiber::sleep_for(20ms);  // 令其停在等待点上

  d.CloseAll(make_error_code(TransportErrc::kClosed));
  (void)waiter.get();

  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(d.Size(), 0u);
}

// 关闭后再行订阅:返回的凭据其信箱已关闭,Wait 立即得到该终止原因,等同于不再受理订阅。
TEST(Dispatcher, SubscribeAfterCloseGivesClosedTicket) {
  auto d = MakeDispatcher();
  d.CloseAll(make_error_code(TransportErrc::kClosed));

  auto ticket = d.Subscribe({7, 10});
  auto got = ticket.Wait(50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(d.Dispatch(Ev{7, 10, "after-close"}), 0u);
}

TEST(Dispatcher, CloseAllIsIdempotent) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});
  d.CloseAll(make_error_code(TransportErrc::kClosed));
  d.CloseAll(make_error_code(TransportErrc::kInternal));  // 不覆盖首次终止原因
  auto got = ticket.Wait(50ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));
}

// 空凭据:默认构造的 Ticket 可安全析构,Wait 返回 kInvalidState。
TEST(Dispatcher, DefaultTicketIsInert) {
  EvDispatcher::Ticket ticket;
  EXPECT_FALSE(static_cast<bool>(ticket));
  auto got = ticket.Wait(10ms);
  ASSERT_FALSE(got);
  EXPECT_EQ(got.error(), make_error_code(TransportErrc::kInvalidState));
}

// Ticket 生存期长于 Dispatcher 的情形:内部以弱引用持有索引,注销时不得出现悬垂访问。
TEST(Dispatcher, TicketOutlivingDispatcherIsSafe) {
  EvDispatcher::Ticket ticket;
  {
    auto d = MakeDispatcher();
    ticket = d.Subscribe({7, 10});
  }
  EXPECT_TRUE(static_cast<bool>(ticket));
  ticket.Reset();  // 不得崩溃
}

// —— 一问两答:两段等待各自设定时限 ————————————————————————————————

TEST(Dispatcher, TwoPhaseInteractionUsesTwoTickets) {
  auto d = MakeDispatcher();
  constexpr std::uint8_t kSession = 7;
  auto ack = d.Subscribe({kSession, 10});       // 第一段:等待回应
  auto result = d.Subscribe({kSession, 1010});  // 第二段:等待结果

  EXPECT_EQ(d.Dispatch(Ev{kSession, 10, "ack"}), 1u);
  EXPECT_EQ(d.Dispatch(Ev{kSession, 1010, "result"}), 1u);

  auto a = ack.Wait(100ms);
  ASSERT_TRUE(a);
  EXPECT_EQ(a.value().tag, "ack");
  auto r = result.Wait(100ms);
  ASSERT_TRUE(r);
  EXPECT_EQ(r.value().tag, "result");
}
