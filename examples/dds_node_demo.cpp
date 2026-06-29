// =============================================================================
// dds_node_demo.cpp — DdsNode 完整用法演示(DDS 发布-订阅 + 多路请求-应答)
//
// 演示 DDS 交互节点 DdsNode 的全部用法,三个节点经**进程内 DDS 总线**(FakeDdsProvider,
// 零 FastDDS 依赖)互联:
//
//   1. 构造与生命周期   —— DdsNode(transport, inbox_topic, codec=null→DdsCodec) / Open / Close
//                          Open 自动订阅自身 inbox topic
//   2. 发布-订阅(扇出) —— Subscribe(topic);Send(msg, Endpoint::Topic(t)) 发布;
//                          一次发布 → 所有订阅者 OnMessage(1 对多扇出)
//   3. 多路请求-应答     —— Request(msg, cb, timeout, Endpoint::Topic(svc));请求自动带本节点
//                          inbox 入 reply_to,服务端 Responder.Reply 据此**精确回送发起方 inbox**
//                          —— 多个客户端打同一服务 topic,应答各回各家、不串台
//   4. 反馈 + 终结       —— Request 的反馈重载:服务端 Responder.Feedback(中间)→ Reply(终结)
//
// 关键:DDS 是发布-订阅范式,DdsNode 用 Endpoint::Topic 寻址、correlation_id 配对、reply_to
// 路由(随 DdsCodec 上线缆)。本 demo 用 FakeDdsProvider 的共享 Bus 在一个进程里跑;换真实
// FastDDS 只需把 provider 换成 "fastdds"(节点代码不变)。
//
// 构建运行(需 -DTRANSPORT_BUILD_EXAMPLES=ON):
//   cmake -S . -B build -DTRANSPORT_BUILD_EXAMPLES=ON
//   cmake --build build -j --target dds_node_demo
//   ./build/dds_node_demo
// =============================================================================

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "transport/comm/DdsNode.hpp"
#include "transport/dds/DdsTransport.hpp"
#include "transport/dds/FakeDdsProvider.hpp"

using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::IDdsTransport;
using transport::Message;
using transport::Result;
using transport::Status;

std::mutex g_io;
void log(const std::string& who, const std::string& msg) {
  std::lock_guard<std::mutex> lk(g_io);
  std::cout << "[" << who << "] " << msg << "\n";
}
std::string Hex(const std::vector<uint8_t>& b) {
  static const char* d = "0123456789abcdef";
  std::string s;
  for (uint8_t x : b) { s += d[x >> 4]; s += d[x & 0xF]; s += ' '; }
  return s.empty() ? "(空)" : s;
}
Message Msg(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

// ── 通用节点:打印订阅到的发布;OnRequest 委托给可设回调 ──
class Node : public DdsNode {
 public:
  Node(std::shared_ptr<IDdsTransport> t, std::string inbox, std::string name)
      : DdsNode(std::move(t), std::move(inbox)), name_(std::move(name)) {}

  std::function<void(const Message&, Responder)> on_request;
  const std::string& name() const { return name_; }

 protected:
  void OnMessage(const Message& m) override {
    log(name_, "📡 收到发布(topic=" + m.topic + ")payload=" + Hex(m.payload));
  }
  void OnRequest(const Message& req, Responder r) override {
    if (on_request) on_request(req, std::move(r));
  }

 private:
  std::string name_;
};

int main() {
  using namespace std::chrono_literals;

  // 共享进程内 DDS 总线(DI 注入 FakeDdsProvider);真实部署换 provider="fastdds"。
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto make = [&bus](const std::string& inbox, const std::string& name) {
    DdsConfig cfg; cfg.domain_id = 0;
    auto tp = std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
    return std::make_shared<Node>(tp, inbox, name);  // 默认 DdsCodec + ThreadExecutor
  };

  auto svc = make("svc_in", "service");   // 服务端:订阅 "calc"
  auto c1  = make("c1_in",  "client1");   // 客户端 1:订阅 "telemetry"
  auto c2  = make("c2_in",  "client2");   // 客户端 2:订阅 "telemetry"

  // 服务端处理请求:先 Feedback(已受理),再 Reply(把每字节翻倍)。
  svc->on_request = [](const Message& req, DdsNode::Responder r) {
    log("service", "收到请求(reply_to=" + req.reply_to + ")payload=" + Hex(req.payload));
    (void)r.Feedback(Msg({0x06}));                       // 中间:受理 ack
    std::vector<uint8_t> out;
    for (uint8_t b : req.payload) out.push_back(static_cast<uint8_t>(b * 2));
    (void)r.Reply(Msg(out));                             // 终结:结果(经 reply_to 回发起方 inbox)
  };

  for (auto& n : {svc, c1, c2})
    if (Status s = n->Open(); !s) { std::cerr << "open: " << s.error << "\n"; return 1; }
  (void)svc->Subscribe("calc");
  (void)c1->Subscribe("telemetry");
  (void)c2->Subscribe("telemetry");
  log("main", "3 节点已 Open(service 订 calc;client1/2 订 telemetry)");
  std::this_thread::sleep_for(100ms);

  // ── (1) 发布-订阅扇出:service 在 telemetry 上发一条,两个客户端都收到 ──
  log("main", "[1] service 发布 telemetry → 扇出到所有订阅者");
  (void)svc->Send(Msg({0xAB, 0xCD}), Endpoint::Topic("telemetry"));
  std::this_thread::sleep_for(200ms);

  // ── (2) 多路请求-应答 + 反馈:两个客户端各打 calc,应答各回各家 inbox ──
  log("main", "[2] client1、client2 各向 calc 发请求(反馈+终结)");
  (void)c1->Request(Msg({10}),
      [](const Message& fb) { log("client1", "  ↳ FEEDBACK payload=" + Hex(fb.payload)); },
      [](Result<Message> r) { if (r) log("client1", "  ↳ REPLY payload=" + Hex(r.value.payload)); },
      /*timeout_ms=*/1000, Endpoint::Topic("calc"));
  (void)c2->Request(Msg({21}),
      [](const Message& fb) { log("client2", "  ↳ FEEDBACK payload=" + Hex(fb.payload)); },
      [](Result<Message> r) { if (r) log("client2", "  ↳ REPLY payload=" + Hex(r.value.payload)); },
      /*timeout_ms=*/1000, Endpoint::Topic("calc"));
  std::this_thread::sleep_for(300ms);

  log("main", "Close");
  c1->Close(); c2->Close(); svc->Close();
  return 0;
}
