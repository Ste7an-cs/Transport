#include "transport/ITransport.hpp"

#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::ICodec;
using transport::ITransport;
using transport::Message;
using transport::Result;
using transport::Status;
using Bytes = std::vector<uint8_t>;

namespace {
// 最小 ITransport:只记录最后一次 Send(payload) 的入参,其余接收侧空实现。
// 不覆写 Send(Message)/SetCodec(topic,codec) → 走 ITransport 基类默认。
class RecordingTransport : public ITransport {
 public:
  // 保留基类 Send(Message)/SetCodec(topic,codec) 重载,避免被同名 override 隐藏
  // —— 与各具体传输一致的 using 约定。
  using ITransport::Send;
  using ITransport::SetCodec;

  Status Open() override { return Status::Success({}); }
  void Close() override {}
  bool IsOpen() const override { return true; }
  Status Send(const std::vector<uint8_t>& data) override {
    last_payload = data;
    ++send_calls;
    return Status::Success({});
  }
  Result<Message> Receive(uint32_t) override {
    return Result<Message>::Fail("io: not supported");
  }
  void OnReceive(ReceiveCallback) override {}
  std::future<Result<Message>> AsyncReceive() override {
    std::promise<Result<Message>> p;
    p.set_value(Result<Message>::Fail("io: not supported"));
    return p.get_future();
  }
  void OnDisconnect(DisconnectCallback) override {}
  void SetCodec(std::shared_ptr<ICodec>) override {}

  Bytes last_payload;
  int send_calls = 0;
};
}  // namespace

TEST(SendMessageBaseDefault, EmptyTopicDegradesToSendPayload) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1, 2, 3};  // topic 空
  auto st = t.Send(m);
  EXPECT_TRUE(st.ok);
  EXPECT_EQ(t.send_calls, 1);
  EXPECT_EQ(t.last_payload, (Bytes{1, 2, 3}));
}

TEST(SendMessageBaseDefault, NonEmptyTopicNotSupported) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = t.Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "io: topic routing not supported");
  EXPECT_EQ(t.send_calls, 0);
}

TEST(SendMessageBaseDefault, SetCodecTopicIsNoopOnBase) {
  RecordingTransport t;
  // 基类 no-op:不抛、不崩,纯粹忽略。
  t.SetCodec("x", nullptr);
  SUCCEED();
}

// ---- TCP topic 路由(回环 client+server) ----
#include <atomic>
#include <chrono>
#include <functional>
#include <thread>

#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerConfig.hpp"
#include "transport/tcp/TcpServerImpl.hpp"

namespace {
// 在 body 前加 tag 字节;Decode 校验剥除。证明「按 topic 选对了 codec」。
// 此 helper 同时供下方 UDP / 串口段复用(后续任务),勿重复定义。
class TagCodec2 : public ICodec {
 public:
  explicit TagCodec2(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};

void WaitFor(std::function<bool()> pred, int ms = 1000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(TcpTopicRouting, TwoTopicsOverOneConnection) {
  using namespace transport;
  TcpServerConfig scfg;
  scfg.bind_addr = "127.0.0.1";
  scfg.port = 0;  // OS 分配
  scfg.enable_topic_routing = true;
  auto server = std::make_shared<TcpServerImpl>(scfg);

  std::shared_ptr<ITransport> accepted;
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    accepted = c;
    ++conns;
  });
  ASSERT_TRUE(server->Open().ok);

  TcpClientConfig ccfg;
  ccfg.host = "127.0.0.1";
  ccfg.port = server->LocalPort();
  ccfg.connect_timeout_ms = 1000;
  ccfg.auto_reconnect = false;
  ccfg.enable_topic_routing = true;
  auto client = std::make_shared<TcpClientImpl>(ccfg);
  ASSERT_TRUE(client->Open().ok);
  WaitFor([&] { return conns.load() >= 1 && accepted != nullptr; });
  ASSERT_NE(accepted, nullptr);

  // 发送端(client)编码、接收端(accepted)解码,两端都要注册同名 codec。
  for (ITransport* t : {static_cast<ITransport*>(client.get()), accepted.get()}) {
    t->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
    t->SetCodec("b", std::make_shared<TagCodec2>(0xBB));
  }

  Message ma;
  ma.payload = Bytes{1, 2};
  ma.topic = "a";
  Message mb;
  mb.payload = Bytes{3, 4, 5};
  mb.topic = "b";
  ASSERT_TRUE(client->Send(ma).ok);
  ASSERT_TRUE(client->Send(mb).ok);

  auto r1 = accepted->Receive(2000);
  ASSERT_TRUE(r1.ok);
  EXPECT_EQ(r1.value.topic, "a");
  EXPECT_EQ(r1.value.payload, (Bytes{1, 2}));
  auto r2 = accepted->Receive(2000);
  ASSERT_TRUE(r2.ok);
  EXPECT_EQ(r2.value.topic, "b");
  EXPECT_EQ(r2.value.payload, (Bytes{3, 4, 5}));

  client->Close();
  server->Close();
}

TEST(TcpTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  TcpClientConfig ccfg;
  ccfg.host = "127.0.0.1";
  ccfg.port = 1;  // 不连接;routing-off 分支在 open 检查前先因 topic 非空返回
  ccfg.auto_reconnect = false;
  ccfg.enable_topic_routing = false;
  auto client = std::make_shared<TcpClientImpl>(ccfg);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = client->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
}

// ---- UDP topic 路由(回环) ----
#include "transport/udp/UdpConfig.hpp"
#include "transport/udp/UdpImpl.hpp"

TEST(UdpTopicRouting, RoundTripSelectsCodecByTopic) {
  using namespace transport;
  UdpConfig rcfg;
  rcfg.mode = UdpMode::kUnicast;
  rcfg.local_addr = "127.0.0.1";
  rcfg.local_port = 0;
  rcfg.enable_topic_routing = true;
  auto rx = std::make_shared<UdpImpl>(rcfg);
  ASSERT_TRUE(rx->Open().ok);
  rx->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
  rx->SetCodec("b", std::make_shared<TagCodec2>(0xBB));

  UdpConfig scfg;
  scfg.mode = UdpMode::kUnicast;
  scfg.local_addr = "127.0.0.1";
  scfg.local_port = 0;
  scfg.remote_addr = "127.0.0.1";
  scfg.remote_port = rx->LocalPort();
  scfg.enable_topic_routing = true;
  auto tx = std::make_shared<UdpImpl>(scfg);
  ASSERT_TRUE(tx->Open().ok);
  tx->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
  tx->SetCodec("b", std::make_shared<TagCodec2>(0xBB));

  Message mb;
  mb.payload = Bytes{9, 8, 7};
  mb.topic = "b";
  ASSERT_TRUE(tx->Send(mb).ok);

  auto r = rx->Receive(2000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{9, 8, 7}));

  tx->Close();
  rx->Close();
}

TEST(UdpTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  UdpConfig cfg;
  cfg.mode = UdpMode::kUnicast;
  cfg.local_addr = "127.0.0.1";
  cfg.local_port = 0;
  cfg.enable_topic_routing = false;
  auto u = std::make_shared<UdpImpl>(cfg);
  ASSERT_TRUE(u->Open().ok);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = u->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
  u->Close();
}

// ---- 串口 topic 路由(开关行为,离线可测) ----
#include "transport/serial/SerialConfig.hpp"
#include "transport/serial/SerialImpl.hpp"

TEST(SerialTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  SerialConfig cfg;
  cfg.device = "/dev/null";  // 不依赖真实串口;routing-off 分支在 open 检查前返回
  cfg.enable_topic_routing = false;
  auto s = std::make_shared<SerialImpl>(cfg);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = s->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
}
