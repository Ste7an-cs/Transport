// -----------------------------------------------------------------------------
// protocol_node_capacity_test.cpp — ProtocolNode 256 并发在途上限 + session_id 空闲集
// LRU 退休窗口(RT_REQUEST_005/006 / ADR-0003 D10)。
//
// 复用 protocol_node_test.cpp 的 Fake 范式:FakeCoroTransport 作传输、真实 SystemCodec
// 走 encode/decode;请求 fiber 用 makeTask 起,主(测试)fiber 用 pumpFiberUntil 驱动。
// 响应帧的 session_id 由 node 盖章、须从已发送帧解码回来才能对上——故先解码 fake.sent()
// 里的请求帧拿到 (session_id, message_id),再注入 message_id|0x1000 的匹配响应。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/node/ProtocolNode.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"

using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::Datagram;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::Result;
using transport::Status;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

constexpr std::uint16_t kResponseBit = 0x1000;

Message MakeRequest(std::uint16_t message_id) {
  Message req;
  req.message_id = message_id;
  return req;
}

// 把一个已发送的请求帧字节解码回 Message(取 node 盖的 session_id / message_id)。
Message DecodeSent(const std::vector<std::uint8_t>& bytes) {
  SystemCodec wire;
  auto decoded = wire.Decode(bytes.data(), bytes.size());
  EXPECT_TRUE(decoded);
  EXPECT_EQ(decoded.value().size(), 1u);
  return decoded && decoded.value().size() == 1 ? decoded.value().front()
                                                : Message{};
}

// 为某请求帧造匹配响应 Datagram:同 session_id、message_id|0x1000、frm_type=kResponse。
Datagram MakeMatchingResponse(const Message& request,
                              std::vector<std::uint8_t> payload = {}) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.session_id = request.session_id;
  resp.message_id = request.message_id | kResponseBit;
  resp.payload = std::move(payload);
  SystemCodec wire;
  auto bytes = wire.Encode(resp);
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

}  // namespace

// 256 并发在途 Request + 乱序注入各自响应 → 每个恰好一次完成、关联清理、PendingCount 归 0。
TEST(ProtocolNodeCapacity, MaxInflightConcurrentRequestsAllResolveExactlyOnce) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  constexpr int kN = 256;
  std::vector<Result<Message>> outcomes(
      kN, Result<Message>{make_error_code(TransportErrc::kInternal)});
  std::vector<Coro::FiberTask<void>> requests;
  requests.reserve(kN);
  int done = 0;
  for (int i = 0; i < kN; ++i) {
    requests.push_back(Coro::makeTask([&, i] {
      outcomes[i] = node.Request(MakeRequest(0x0002));
      ++done;
    }));
  }

  // 256 个请求 fiber 各分配一个 session_id 并写出帧(空闲集恰好耗尽)。
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return fake->sent().size() == static_cast<std::size_t>(kN); }));
  EXPECT_EQ(node.PendingCount(), static_cast<std::size_t>(kN));

  // 解码 256 帧拿各自 session_id,校验 0..255 全被分配(无重号)。
  auto sent = fake->sent();
  std::vector<bool> seen(kN, false);
  std::vector<Message> reqs;
  for (const auto& unit : sent) {
    Message m = DecodeSent(unit.bytes);
    ASSERT_FALSE(seen[m.session_id]) << "重复分配 session_id " << int(m.session_id);
    seen[m.session_id] = true;
    reqs.push_back(m);
  }

  // 乱序注入(逆序)各自匹配响应 → 每个 fiber 恰好一次完成。
  for (auto it = reqs.rbegin(); it != reqs.rend(); ++it) {
    fake->Inject(MakeMatchingResponse(*it, {it->session_id}));
  }

  ASSERT_TRUE(pumpFiberUntil([&] { return done == kN; }));
  for (int i = 0; i < kN; ++i) {
    EXPECT_TRUE(outcomes[i]) << "请求 " << i << " 未完成";
  }
  // 关联全清理:在途归 0、无误配丢弃。
  EXPECT_EQ(node.PendingCount(), 0u);
  EXPECT_EQ(node.UnmatchedResponseCount(), 0u);

  node.Close();
  for (auto& r : requests) EXPECT_TRUE(r.get());
}

// 第 257 个并发 Request:256 全在途 → 发送前返 kResourceExhausted(不占用、不发送)。
TEST(ProtocolNodeCapacity, RequestBeyondSessionSpaceExhaustsWithoutSending) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  constexpr int kN = 256;
  std::vector<Coro::FiberTask<void>> requests;
  requests.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    requests.push_back(
        Coro::makeTask([&] { (void)node.Request(MakeRequest(0x0002)); }));
  }
  ASSERT_TRUE(pumpFiberUntil(
      [&] { return fake->sent().size() == static_cast<std::size_t>(kN); }));

  // 第 257 个:空闲集空 → kResourceExhausted,且未新增发送帧。
  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto overflow = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kResourceExhausted));
  EXPECT_EQ(fake->sent().size(), static_cast<std::size_t>(kN));  // 未发送。
  EXPECT_TRUE(overflow.get());

  node.Close();
  for (auto& r : requests) EXPECT_TRUE(r.get());
}

// 上限恢复:某请求终结释放 session_id 后,后续 Request 可复用该名额(容量回收)。
TEST(ProtocolNodeCapacity, ReleasedSessionIsReusableAfterCompletion) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  // 先跑一个请求-响应,session_id 释放回空闲集。
  bool done1 = false;
  auto r1 = Coro::makeTask([&] {
    (void)node.Request(MakeRequest(0x0002));
    done1 = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  fake->Inject(MakeMatchingResponse(DecodeSent(fake->sent().at(0).bytes)));
  ASSERT_TRUE(pumpFiberUntil([&] { return done1; }));
  EXPECT_TRUE(r1.get());
  EXPECT_EQ(node.PendingCount(), 0u);

  // 名额已回收:再发一个请求应成功登记(在途 +1)并可完成。
  bool done2 = false;
  auto r2 = Coro::makeTask([&] {
    (void)node.Request(MakeRequest(0x0003));
    done2 = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() == 2u; }));
  EXPECT_EQ(node.PendingCount(), 1u);
  fake->Inject(MakeMatchingResponse(DecodeSent(fake->sent().at(1).bytes)));
  ASSERT_TRUE(pumpFiberUntil([&] { return done2; }));
  EXPECT_TRUE(r2.get());
  EXPECT_EQ(node.PendingCount(), 0u);

  node.Close();
}

// LRU / FIFO 退休窗口:连续 分配-释放 取值为 0,1,2...(最久释放者最后复用),而非
// 立即复用刚释放的 0 —— 退休窗口最大化(RT_REQUEST_005)。
TEST(ProtocolNodeCapacity, SessionIdReuseIsFifoForMaxRetirementWindow) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  // 三轮串行 请求-响应;每轮 node 分配一个 session_id,终结后 push_back 释放。
  for (std::uint8_t expected = 0; expected < 3; ++expected) {
    bool done = false;
    auto req = Coro::makeTask([&] {
      (void)node.Request(MakeRequest(0x0002));
      done = true;
    });
    const std::size_t idx = expected;  // 第 idx 个已发送帧。
    ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() == idx + 1; }));
    Message sent = DecodeSent(fake->sent().at(idx).bytes);
    // FIFO:第 0/1/2 轮分别取 0/1/2,而非立即复用上轮释放的值。
    EXPECT_EQ(sent.session_id, expected) << "第 " << int(expected) << " 轮 session_id";
    fake->Inject(MakeMatchingResponse(sent));
    ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
    EXPECT_TRUE(req.get());
  }

  node.Close();
}

// #98:noresponse Send 只读空闲集尾部(最新释放者)盖帧、不出队——不扰动 Request 的
// FIFO 退休窗口(RT_REQUEST_005),也不占 256 在途预算。每轮 Request 前穿插一条 Send:
// Request 的 session 序仍为 0,1,2(若 Send 仍借道 pop/push,序会被搅乱);Send 盖的
// id 依次为 255(初始尾部)、0、1(上一轮 Request 释放的最新者)。
TEST(ProtocolNodeCapacity, NoresponseSendDoesNotDisturbSessionFifoOrBudget) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  const std::uint8_t expect_send_id[] = {255, 0, 1};  // 各轮空闲集尾部。
  std::size_t frames = 0;  // fake->sent() 游标(Send 帧与 Request 帧交错)。
  for (std::uint8_t expected = 0; expected < 3; ++expected) {
    // 穿插 noresponse Send:盖空闲集尾部 id,不出队。
    Status send_result = make_error_code(TransportErrc::kInternal);
    bool sent_done = false;
    auto sender = Coro::makeTask([&] {
      send_result = node.Send(MakeRequest(0x0033));
      sent_done = true;
    });
    ASSERT_TRUE(pumpFiberUntil([&] { return sent_done; }));
    ASSERT_TRUE(send_result);
    ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() == frames + 1; }));
    Message oneway = DecodeSent(fake->sent().at(frames).bytes);
    EXPECT_EQ(oneway.session_id, expect_send_id[expected])
        << "第 " << int(expected) << " 轮 Send 应盖空闲集尾部";
    ++frames;
    EXPECT_TRUE(sender.get());

    // Request:FIFO 序不被穿插的 Send 扰动,仍依次取 0/1/2。
    bool done = false;
    auto req = Coro::makeTask([&] {
      (void)node.Request(MakeRequest(0x0002));
      done = true;
    });
    ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() == frames + 1; }));
    Message sent = DecodeSent(fake->sent().at(frames).bytes);
    EXPECT_EQ(sent.session_id, expected)
        << "Send 穿插后第 " << int(expected) << " 轮 Request session_id";
    fake->Inject(MakeMatchingResponse(sent));
    ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
    ++frames;
    EXPECT_TRUE(req.get());
  }

  node.Close();
}

// 迟到响应不误配:退休 session 的迟到响应不会命中复用后落到别处的在途请求。
// A(session 0)终结释放后,B 因 FIFO 拿 session 1(非 0);注入 A 的迟到响应(session 0)
// → 无匹配丢弃、B 不被误配(仍在途),再正常收 B。
TEST(ProtocolNodeCapacity, LateResponseOfRetiredSessionIsNotMisattributed) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  // A:session 0,收响应后终结,session 0 释放回空闲集尾。
  bool doneA = false;
  auto a = Coro::makeTask([&] {
    (void)node.Request(MakeRequest(0x0002));
    doneA = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  Message reqA = DecodeSent(fake->sent().at(0).bytes);
  ASSERT_EQ(reqA.session_id, 0);
  fake->Inject(MakeMatchingResponse(reqA));
  ASSERT_TRUE(pumpFiberUntil([&] { return doneA; }));
  EXPECT_TRUE(a.get());

  // B:FIFO → 拿 session 1(退休窗口令 0 不被立即复用)。B 保持在途(不注入其响应)。
  Result<Message> outcomeB{make_error_code(TransportErrc::kInternal)};
  bool doneB = false;
  auto b = Coro::makeTask([&] {
    outcomeB = node.Request(MakeRequest(0x0002));
    doneB = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() == 2u; }));
  Message reqB = DecodeSent(fake->sent().at(1).bytes);
  EXPECT_EQ(reqB.session_id, 1) << "FIFO 复用应跳过刚释放的 0";

  // A 的迟到响应(session 0):无在途匹配 → 丢弃计数 +1,绝不误配到 B。
  fake->Inject(MakeMatchingResponse(reqA));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }));
  EXPECT_FALSE(doneB);  // B 未被迟到响应误终结。
  EXPECT_EQ(node.PendingCount(), 1u);

  // B 正常收自己的响应 → 恰好一次完成。
  fake->Inject(MakeMatchingResponse(reqB));
  ASSERT_TRUE(pumpFiberUntil([&] { return doneB; }));
  EXPECT_TRUE(outcomeB);
  EXPECT_TRUE(b.get());

  node.Close();
}
