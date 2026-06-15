#include "transport/dds/DdsImpl.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"
#include "transport/Endpoint.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::IDdsTransport;
using transport::Message;
using transport::Result;

namespace {

DdsConfig ReqRespCfg() {
  DdsConfig c;
  c.mode = DdsMode::kReqResp;
  c.topics = {"calc"};
  return c;
}

std::shared_ptr<DdsImpl> Make(std::shared_ptr<FakeDdsProvider::Bus> bus,
                              DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}

}  // namespace

TEST(DdsReqResp, RoundtripCorrelation) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "calc", [](const Message& req, IDdsTransport::ReplyFn reply) {
        auto out = req.payload;
        for (auto& b : out) b = static_cast<uint8_t>(b + 1);  // 业务：+1
        EXPECT_TRUE(static_cast<bool>(reply(out)));
      })));

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {1, 2, 3}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 1000)));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(DdsReqResp, ConcurrentRequestsDoNotCross) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  // echo 服务
  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "calc", [](const Message& req, IDdsTransport::ReplyFn reply) {
        (void)reply(req.payload);
      })));

  std::promise<std::vector<uint8_t>> p1, p2;
  ASSERT_TRUE(static_cast<bool>(client->SendRequest({1}, "calc",
      [&](Result<Message> r) { p1.set_value(r ? r.value.payload
                                              : std::vector<uint8_t>{}); }, 1000)));
  ASSERT_TRUE(static_cast<bool>(client->SendRequest({2}, "calc",
      [&](Result<Message> r) { p2.set_value(r ? r.value.payload
                                              : std::vector<uint8_t>{}); }, 1000)));
  EXPECT_EQ(p1.get_future().get(), (std::vector<uint8_t>{1}));
  EXPECT_EQ(p2.get_future().get(), (std::vector<uint8_t>{2}));
}

TEST(DdsReqResp, TimeoutWhenNoServer) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {1}, "calc", [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/50)));
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsReqResp, AsyncReplyFromAnotherThread) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto server = Make(bus, ReqRespCfg());
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  std::thread worker;
  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "calc", [&](const Message& req, IDdsTransport::ReplyFn reply) {
    auto payload = req.payload;
    worker = std::thread([reply, payload] {
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
      (void)reply(payload);  // 异步回包
    });
  })));

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest({7}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 2000)));
  auto r = fut.get();
  if (worker.joinable()) worker.join();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7}));
}

TEST(DdsReqResp, UnknownReplyIdIgnored) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest({1}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); }, 300)));
  // 直接向 reply topic 投一个无关 id 的回复
  FakeDdsProvider stranger(bus);
  ASSERT_TRUE(static_cast<bool>(stranger.Reply("calc_Reply", "not-our-id", {9})));
  auto r = fut.get();  // 仍按超时收场（无关回复被忽略）
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsReqResp, CloseCancelsPendingWithConnError) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto client = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest({1}, "calc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/60000)));
  client->Close();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}

TEST(DdsReqResp, ModeConstraintRejectsPubSubMethods) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = Make(bus, ReqRespCfg());
  ASSERT_TRUE(static_cast<bool>(t->Open()));
  EXPECT_EQ(t->Send({1}).error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Send({1}, transport::Endpoint::Topic("x")).error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Subscribe("x").error.rfind("config:", 0), 0u);
  EXPECT_EQ(t->Unsubscribe("x").error.rfind("config:", 0), 0u);
}
