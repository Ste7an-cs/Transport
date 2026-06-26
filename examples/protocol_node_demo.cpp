// =============================================================================
// protocol_node_demo.cpp — ProtocolNode 完整用法演示
//
// 演示「控制器(controller)」与「设备(device)」两个 ProtocolNode 经 TCP 对接外部
// 协议(SystemCodec 帧),覆盖 ProtocolNode 的全部用法:
//
//   1. 构造与生命周期         —— ctor(transport, codec=null→SystemCodec, config) / Open / Close
//   2. 接收角色钩子           —— 继承 OnCommand / OnHeartbeat / OnError
//   3. 应答                   —— Responder.Response() / Result()
//   4. 5 种发送交互模式:
//        SendNoResponse        发后即完(无回应)
//        Request               需回应:等 1 个 RESPONSE
//        RequestWithResult     需结果:等 1 个 RESULT
//        RequestNeedFeedback   收 RESPONSE(中间) → 收 RESULT(终结),引擎自动回 ack
//        StartRepeating/Stop   周期发 STATE
//   5. 周期心跳               —— config.heartbeat_interval_ms > 0,对端 OnHeartbeat
//   6. 可观测 trace           —— SetTrace(OstreamTraceSink) 打印全量交互流
//
// ── 为什么用 TCP(而不是 UDP)? ────────────────────────────────────────────
//   SystemCodec 是【有状态的流式 codec】:内部维护滚动缓冲 buffer_,每次 Decode 把新
//   字节 append 进去,扫描 AA BB CC DD 同步头,取出能凑齐的完整帧,半截帧留到下次。
//   这套「跨多次读拼帧 + 坏帧 resync」正是为【字节流】(TCP / 串口)设计的——一次 read
//   可能只到半个帧、也可能含多个帧,靠滚动缓冲与同步头/CRC 切分。TCP 与它天然匹配。
//
//   UDP 是【报文边界】语义(一次收 = 一个完整 datagram),把有状态流式 codec 套在 UDP 上
//   是语义错配,且有真实隐患:
//     · 多对端:设备一个 UDP socket 收多个控制器 → 各家 datagram 混进【同一个】buffer_,
//       无法按对端区分 → 帧被拼接污染。(流式 codec 没有「按来源分缓冲」的概念。)
//     · 半截/截断报文:某个 datagram 不是整帧(frm_len 异常等)→ 残留字节滞留 buffer_,
//       被【拼到下一个本来正常的 datagram 前面】→ 污染,需 resync 才(可能)恢复。
//   仅当「单一对端 + 每个 datagram 恰好整帧 + 不乱序丢半截」时它才碰巧能跑(回环即如此)。
//   若确需 UDP,应让每个 datagram 承载整帧并配【无状态】codec(按报文独立解码),而不是
//   SystemCodec。本 demo 因此用 TCP——让有状态切帧真正发挥作用,不教易错的用法。
//   (同一套 ProtocolNode/SystemCodec 也适用串口,串口同为字节流,与 TCP 同理。)
//
// 构建运行(需 -DTRANSPORT_BUILD_EXAMPLES=ON):
//   cmake -S . -B build -DTRANSPORT_BUILD_EXAMPLES=ON
//   cmake --build build -j --target protocol_node_demo
//   ./build/protocol_node_demo
// =============================================================================

#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "transport/ITraceSink.hpp"
#include "transport/comm/ProtocolNode.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpClientTransport.hpp"
#include "transport/tcp/TcpServerConfig.hpp"
#include "transport/tcp/TcpServerTransport.hpp"

using transport::ITransport;
using transport::Message;
using transport::OstreamTraceSink;
using transport::ProtocolConfig;
using transport::ProtocolNode;
using transport::Result;
using transport::Status;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpServerConfig;
using transport::TcpServerTransport;
using transport::TraceLevel;

// ---- 命令码(message_id):每个命令一个固定值,不自增 ----
namespace cmd {
constexpr uint16_t kSetLed     = 0x0001;  // 无回应
constexpr uint16_t kGetStatus  = 0x0002;  // 需回应(RESPONSE)
constexpr uint16_t kSelfTest   = 0x0003;  // 需结果(RESULT)
constexpr uint16_t kStartJob   = 0x0004;  // 需反馈(RESPONSE 然后 RESULT)
constexpr uint16_t kTelemetry  = 0x0005;  // 周期 STATE
}  // namespace cmd

// 跨线程打印用一把锁(回调在 worker 线程跑,避免输出交错)。
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

// ============================================================================
// 设备端:接收角色。继承 ProtocolNode,重写钩子。
// ============================================================================
class Device : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;

 protected:
  // 收到 COMMAND 或 STATE 帧 → 这里。按命令码(msg.message_id)分派,经 Responder 回应。
  void OnCommand(const Message& msg, Responder r) override {
    switch (msg.message_id) {
      case cmd::kSetLed:
        log("device", "收到 SET_LED payload=" + Hex(msg.payload) + " (无需回应)");
        break;

      case cmd::kGetStatus:
        log("device", "收到 GET_STATUS → 回 RESPONSE");
        (void)r.Response({0xA0, 0xA1});  // 即时回应:状态字节
        break;

      case cmd::kSelfTest:
        log("device", "收到 SELF_TEST → 回 RESULT");
        (void)r.Result({0x00});          // 终结结果:0x00=通过
        break;

      case cmd::kStartJob:
        log("device", "收到 START_JOB → 先 RESPONSE(已受理) 再 RESULT(完成)");
        (void)r.Response({0x06});        // 中间:已受理 ack
        // 真实场景这里是异步干活;demo 直接给最终结果。
        (void)r.Result({0xDA, 0x7A});    // 终结:产出
        break;

      case cmd::kTelemetry:
        log("device", "收到 STATE(telemetry) payload=" + Hex(msg.payload));
        break;

      default:
        log("device", "未知命令 message_id=" + std::to_string(msg.message_id));
        break;
    }
  }

  void OnHeartbeat(const Message&) override { log("device", "♥ 收到心跳"); }
  void OnError(const std::string& e) override { log("device", "OnError: " + e); }
};

// ============================================================================
// 控制器端:发起角色。这里也继承一下,演示它同样能收(双角色),但本 demo 只发。
// ============================================================================
class Controller : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;
 protected:
  void OnError(const std::string& e) override { log("controller", "OnError: " + e); }
};

// 设备节点在 server 的 OnAccept 回调里(accept 线程)创建,需在外层持住保活。
std::mutex g_dev_mu;
std::shared_ptr<Device> g_device;

int main() {
  using namespace std::chrono_literals;

  // ---- 1. 设备端:TCP 服务端监听 127.0.0.1:6000;每接受一个连接,在该连接上建一个 Device ----
  TcpServerConfig srv_cfg;
  srv_cfg.bind_addr = "127.0.0.1";
  srv_cfg.port = 6000;
  auto server = std::make_shared<TcpServerTransport>(srv_cfg);

  ProtocolConfig dev_pc;
  dev_pc.protocol_id = 1;

  server->OnAccept([dev_pc](std::shared_ptr<ITransport> conn) {
    // conn 是独立 ITransport(一条 TCP 连接,字节流);在其上建设备节点(默认 SystemCodec)。
    auto dev = std::make_shared<Device>(conn, /*codec=*/nullptr, dev_pc);
    (void)dev->Open();
    std::lock_guard<std::mutex> lk(g_dev_mu);
    g_device = dev;                       // 持住保活
    log("device", "已接受连接并 Open");
  });
  if (Status s = server->Open(); !s) { std::cerr << "server open: " << s.error << "\n"; return 1; }

  // ---- 2. 控制器端:TCP 客户端连接设备 ----
  TcpClientConfig cli_cfg;
  cli_cfg.host = "127.0.0.1";
  cli_cfg.port = 6000;

  ProtocolConfig ctl_pc;
  ctl_pc.protocol_id = 1;
  ctl_pc.response_timeout_ms = 500;     // 请求超时
  ctl_pc.max_retries = 3;               // 超时重发上限(达上限仍无回应 → timeout: 失败)
  ctl_pc.heartbeat_interval_ms = 1000;  // >0 → Open 后每 1s 自动发 HEARTBEAT 给对端

  auto controller = std::make_shared<Controller>(
      std::make_shared<TcpClientTransport>(cli_cfg), /*codec=*/nullptr, ctl_pc);

  // ---- 3. 可观测:给控制器挂一个打印 sink,看全量交互流(send/request/dispatch/retransmit/...) ----
  controller->SetTrace(std::make_shared<OstreamTraceSink>(std::cerr, TraceLevel::kDebug));

  // ---- 4. Open 控制器(发起连接)。等连接建立 + 设备节点就绪。 ----
  if (Status s = controller->Open(); !s) { std::cerr << "controller open: " << s.error << "\n"; return 1; }
  std::this_thread::sleep_for(300ms);
  log("main", "已连接(controller 心跳已启动)");

  // ---- 5. 五种发送交互模式 ----

  // (a) 无回应:发后即完。
  log("main", "[a] SendNoResponse(SET_LED)");
  (void)controller->SendNoResponse(cmd::kSetLed, {0x01});
  std::this_thread::sleep_for(200ms);

  // (b) 需回应:等 1 个 RESPONSE(回调在 worker 线程触发)。
  log("main", "[b] Request(GET_STATUS) — 等 RESPONSE");
  (void)controller->Request(cmd::kGetStatus, {}, [](Result<Message> r) {
    if (r) log("controller", "  ↳ RESPONSE payload=" + Hex(r.value.payload));
    else   log("controller", "  ↳ 失败: " + r.error);
  });
  std::this_thread::sleep_for(200ms);

  // (c) 需结果:等 1 个 RESULT。
  log("main", "[c] RequestWithResult(SELF_TEST) — 等 RESULT");
  (void)controller->RequestWithResult(cmd::kSelfTest, {}, [](Result<Message> r) {
    if (r) log("controller", "  ↳ RESULT payload=" + Hex(r.value.payload));
    else   log("controller", "  ↳ 失败: " + r.error);
  });
  std::this_thread::sleep_for(200ms);

  // (d) 需反馈:收 RESPONSE(中间)→ 收 RESULT(终结),引擎自动回 RESPONSE ack。
  log("main", "[d] RequestNeedFeedback(START_JOB) — 收 RESPONSE 再收 RESULT");
  (void)controller->RequestNeedFeedback(cmd::kStartJob, {0x2A},
      [](Result<Message> r) {  // on_response(中间)
        if (r) log("controller", "  ↳ RESPONSE(已受理) payload=" + Hex(r.value.payload));
      },
      [](Result<Message> r) {  // on_result(终结)
        if (r) log("controller", "  ↳ RESULT(完成) payload=" + Hex(r.value.payload));
        else   log("controller", "  ↳ 失败: " + r.error);
      });
  std::this_thread::sleep_for(200ms);

  // (e) 周期发送:每 300ms 发一帧 STATE,发几拍后停。
  log("main", "[e] StartRepeating(TELEMETRY, 300ms)");
  uint32_t h = controller->StartRepeating(cmd::kTelemetry, {0xEE}, /*interval_ms=*/300);
  std::this_thread::sleep_for(1000ms);   // 期间约 3~4 拍 STATE + 1 次心跳
  controller->StopRepeating(h);
  log("main", "StopRepeating");

  // ---- 6. 演示超时+重发:发一个设备不回应的命令,看 trace 里 retransmit×3 后 timeout ----
  log("main", "[f] Request(未知命令) — 设备不回应 → 重发后超时");
  (void)controller->Request(/*cmd=*/0x00FF, {}, [](Result<Message> r) {
    log("controller", std::string("  ↳ ") + (r ? "RESPONSE" : ("失败: " + r.error)));
  });
  std::this_thread::sleep_for(2500ms);   // 500ms 超时 × (1+3 重发) ≈ 2s

  // ---- 7. Close(幂等;终结所有挂起请求、停定时器、停执行器、关传输) ----
  log("main", "Close");
  controller->Close();
  {
    std::lock_guard<std::mutex> lk(g_dev_mu);
    if (g_device) g_device->Close();
  }
  server->Close();
  return 0;
}
