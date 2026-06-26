// =============================================================================
// protocol_udp_demo.cpp — ProtocolNode 跑 UDP(1:多)演示
//
// 演示外部协议(SystemCodec 帧)经 UDP 与多个对端通信,覆盖 UDP 适配的三件套:
//
//   1. SystemDatagramCodec —— 无状态【报文版】外部帧 codec(每 datagram 独立解码,
//      残留丢弃、不跨报文)。UDP 是报文边界语义,必须用它,而非有状态流式 SystemCodec
//      (流式滚动缓冲在多对端会串台、半截报文污染下一报文)。
//   2. ProtocolConfig.reply_to_source = true —— 应答/ack 回到【入站消息的来源 ip:port】。
//      服务端 Responder 回应、客户端 needfeedback 自动 ack 都走这条;一个 socket 服务/
//      面对多个对端时必须置 true(否则回到固定 default_dest_ = 错的对端)。
//   3. 发送方法的目的地参数 const Endpoint& to —— 客户端经一个 UDP socket 向多个设备
//      分别发命令:node->Request(cmd, payload, cb, Endpoint::Net(ip, port))。
//
// 拓扑:1 个控制器(:7000)向 2 个设备(:7001 / :7002)各发命令;每个设备 reply_to_source
// 回到控制器来源;控制器靠匹配键 (session_id, message_id) 区分两设备的回应(session 每请求滚动)。
//
// 构建运行(需 -DTRANSPORT_BUILD_EXAMPLES=ON):
//   cmake -S . -B build -DTRANSPORT_BUILD_EXAMPLES=ON
//   cmake --build build -j --target protocol_udp_demo
//   ./build/protocol_udp_demo
// =============================================================================

#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolNode.hpp"
#include "transport/udp/UdpConfig.hpp"
#include "transport/udp/UdpTransport.hpp"

using transport::Endpoint;
using transport::Message;
using transport::ProtocolConfig;
using transport::ProtocolNode;
using transport::Result;
using transport::Status;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpTransport;

namespace cmd {
constexpr uint16_t kGetStatus = 0x0002;  // 需回应(RESPONSE)
constexpr uint16_t kStartJob  = 0x0004;  // 需反馈(RESPONSE 然后 RESULT)
}  // namespace cmd

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

// ── 设备端:回应里带自身 id,便于控制器分辨是哪个设备答的 ──
class Device : public ProtocolNode {
 public:
  Device(std::shared_ptr<transport::ITransport> t, std::unique_ptr<transport::ICodec> c,
         ProtocolConfig cfg, uint8_t id)
      : ProtocolNode(std::move(t), std::move(c), cfg), id_(id) {}

 protected:
  void OnCommand(const Message& m, Responder r) override {
    const std::string me = "device" + std::to_string(id_);
    switch (m.message_id) {
      case cmd::kGetStatus:
        log(me, "收到 GET_STATUS(来源 " + m.source + ")→ 回 RESPONSE");
        (void)r.Response({id_, 0xA1});                  // 经 reply_to_source 回到控制器
        break;
      case cmd::kStartJob:
        log(me, "收到 START_JOB → RESPONSE(受理)+ RESULT(完成)");
        (void)r.Response({id_, 0x06});
        (void)r.Result({id_, 0xDA});
        break;
      default:
        log(me, "未知命令 " + std::to_string(m.message_id));
        break;
    }
  }
  void OnError(const std::string& e) override { log("device" + std::to_string(id_), "OnError: " + e); }

 private:
  uint8_t id_;
};

class Controller : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;
 protected:
  void OnError(const std::string& e) override { log("controller", "OnError: " + e); }
};

// 造一个绑定到 127.0.0.1:port 的 UDP 传输(无固定 remote —— 发送时显式指定目的地)。
std::shared_ptr<UdpTransport> Udp(uint16_t port) {
  UdpConfig c;
  c.local_addr = "127.0.0.1";
  c.local_port = port;
  return std::make_shared<UdpTransport>(c);
}

int main() {
  using namespace std::chrono_literals;

  // 设备配置:reply_to_source=true(回到请求来源);无心跳(1:多 UDP 无固定对端)。
  ProtocolConfig dev_cfg;
  dev_cfg.protocol_id = 1;
  dev_cfg.reply_to_source = true;

  auto dev1 = std::make_shared<Device>(Udp(7001), std::make_unique<SystemDatagramCodec>(), dev_cfg, 1);
  auto dev2 = std::make_shared<Device>(Udp(7002), std::make_unique<SystemDatagramCodec>(), dev_cfg, 2);

  // 控制器:reply_to_source=true(供 needfeedback 自动 ack 回到结果来源);无心跳。
  ProtocolConfig ctl_cfg;
  ctl_cfg.protocol_id = 1;
  ctl_cfg.response_timeout_ms = 500;
  ctl_cfg.reply_to_source = true;
  auto controller = std::make_shared<Controller>(Udp(7000), std::make_unique<SystemDatagramCodec>(), ctl_cfg);

  for (auto* n : {static_cast<ProtocolNode*>(dev1.get()), static_cast<ProtocolNode*>(dev2.get()),
                  static_cast<ProtocolNode*>(controller.get())}) {
    if (Status s = n->Open(); !s) { std::cerr << "open: " << s.error << "\n"; return 1; }
  }
  log("main", "3 个节点已 Open(控制器 :7000,设备 :7001 / :7002)");
  std::this_thread::sleep_for(100ms);

  const Endpoint to1 = Endpoint::Net("127.0.0.1", 7001);
  const Endpoint to2 = Endpoint::Net("127.0.0.1", 7002);

  // (1) 经一个 socket 向两个设备各发 GET_STATUS,回应经 reply_to_source 回到控制器。
  log("main", "[1] 向 device1、device2 各发 GET_STATUS");
  (void)controller->Request(cmd::kGetStatus, {}, [](Result<Message> r) {
    if (r) log("controller", "  ↳ 来自 device" + std::to_string(r.value.payload.empty() ? 0 : r.value.payload[0]) +
                             " 的 RESPONSE payload=" + Hex(r.value.payload));
  }, to1);
  (void)controller->Request(cmd::kGetStatus, {}, [](Result<Message> r) {
    if (r) log("controller", "  ↳ 来自 device" + std::to_string(r.value.payload.empty() ? 0 : r.value.payload[0]) +
                             " 的 RESPONSE payload=" + Hex(r.value.payload));
  }, to2);
  std::this_thread::sleep_for(300ms);

  // (2) 对 device1 走 needfeedback:RESPONSE → RESULT,控制器自动回 ack(经 reply_to_source 回到 device1)。
  log("main", "[2] 对 device1 发 START_JOB(needfeedback)");
  (void)controller->RequestNeedFeedback(cmd::kStartJob, {},
      [](Result<Message> r) { if (r) log("controller", "  ↳ RESPONSE payload=" + Hex(r.value.payload)); },
      [](Result<Message> r) { if (r) log("controller", "  ↳ RESULT payload=" + Hex(r.value.payload)); },
      to1);
  std::this_thread::sleep_for(300ms);

  log("main", "Close");
  controller->Close();
  dev1->Close();
  dev2->Close();
  return 0;
}
