/**
 * @file examples/udp_fanout/main.cpp
 * @brief 示例 ③:UDP **一对多**(ADR-0021)+ **入站 `endpoint` 即发送方**(ADR-0020 D3)。
 *
 * 一个进程两种角色:`--role peer`(回声端)与 `--role fanout`(一对多发起端)。
 *
 * 本例覆盖的易错点:
 *  - **codec 与介质匹配**:UDP 是**报文**介质,配**无状态报文式**的 `SystemDatagramCodec`
 *    ——同一套帧格式的报文版,**零跨报文状态**,故多对端安全。
 *    **把有状态的 `SystemCodec` 装到 UDP 上是错的**:跨报文残留会污染下一个对端的解码。
 *  - **一对多靠 `Message::endpoint`**(ADR-0021 D1):**一个节点、一条 socket** 就够,
 *    在出站 `Message` 里填目的地即可,不必每个对端开一条传输。
 *  - **入站的 `msg.endpoint` 就是发送方**(ADR-0020 D3),故 `rsp.endpoint = req.endpoint;`
 *    这一行就把应答送回了**真正的请求方**。本例的回声端**刻意不配默认对端**
 *    (`UdpConfig::remote_addr` / `remote_port` 留空)——不回带 `endpoint` 的应答根本发不出去,
 *    这正是"UDP 上不可省"的直接证据。
 *  - **应答三项**:`session_id`(ADR-0019)+ `message_id` + `endpoint`(ADR-0021),缺一不可。
 */

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <QCoreApplication>

#include "detail/asyncdefine.h"     // Coro::msleep
#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "ExampleCommon.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Hex16;
using example::Log;
using example::Pay;
using example::Printable;
using example::Text;
using transport::AnyOfType;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::RetryPolicy;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;

namespace {

constexpr std::uint16_t kMidNote = 0x0001;  ///< 单向通报:回声端**不回**
constexpr std::uint16_t kMidPing = 0x0010;  ///< 请求:回声端回一帧 kResponse

/// 把一个 `Endpoint` 打成人看得懂的样子。
std::string Show(const Endpoint& endpoint) {
  switch (endpoint.kind) {
    case Endpoint::Kind::kNet:
      return endpoint.host + ":" + std::to_string(endpoint.port);
    case Endpoint::Kind::kTopic:
      return "topic(" + endpoint.topic + ")";
    case Endpoint::Kind::kService:
      return "service(" + endpoint.topic + ")";
    case Endpoint::Kind::kDefault:
      break;
  }
  return "<默认对端>";
}

// ───────────────────────────── 回声端 ─────────────────────────────────────

void RunPeer(const std::string& bind_addr, std::uint16_t port, int requests) {
  UdpConfig cfg;
  cfg.mode = UdpMode::kUnicast;
  cfg.local_addr = bind_addr;
  cfg.local_port = port;
  // ★★ **刻意不配 remote_addr / remote_port**:本端没有"默认对端"。
  //    于是不回带 `endpoint` 的应答**根本无处可去**——`Endpoint::Default()` 在这里解析
  //    不出目的地,写泵会丢掉该条并只落 `LastError()`(不回传)。这就是 ADR-0021
  //    「UDP 上 `rsp.endpoint` 不可省」最直接的证据。
  cfg.silence_timeout = 5000ms;

  UdpTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("UDP 启动失败:" + Err(started.error()));
    return;
  }

  ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x07;
  // ★ UDP = 报文介质 ⇒ **无状态报文式** codec。装 `SystemCodec` 是错的(跨报文残留)。
  ProtocolNode node(transport, std::make_unique<SystemDatagramCodec>(), ncfg);
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  Log("[回声端] 绑在 " + bind_addr + ":" + std::to_string(transport.LocalPort()) +
      ",无默认对端(应答只能靠 endpoint 回)");

  auto sub = node.Subscribe(AnyOfType(FrameType::kCommand));
  if (!sub) {
    Log("订阅失败:" + Err(sub.error()));
    node.Close();
    node.WaitClosed();
    transport.Close();
    transport.WaitClosed();
    return;
  }
  auto ticket = std::move(sub).value();

  int served = 0;
  auto serving = Coro::makeTask([&] {
    for (;;) {
      auto got = ticket.Wait();
      if (!got) {
        break;  // 信箱被节点关闭 → 退出
      }
      const Message& req = got.value();
      // ★ **入站的 endpoint 就是发送方**(ADR-0020 D3):UDP 路径由此把发送方
      //   ip:port 交到业务层手里,"回给谁"一目了然。
      Log("[收到] 来自 " + Show(req.endpoint) + " message_id=" + Hex16(req.message_id) +
          " session_id=" + std::to_string(req.session_id) +
          " payload=\"" + Printable(req.payload) + "\"");  // 作用域内直接用视图,零拷贝

      if (req.message_id == kMidPing) {
        Message rsp;
        rsp.frm_type = FrameType::kResponse;
        rsp.session_id = req.session_id;  // ★ ADR-0019:Send 原样透传,不回带就匹配不上
        rsp.message_id = req.message_id;  // ★ 键的另一半
        rsp.endpoint = req.endpoint;      // ★★ ADR-0021:回到**真正的请求方**
        rsp.payload = Pay("pong@" + std::to_string(transport.LocalPort()) +
                          ":" + Text(req.payload));
        auto sent = node.Send(std::move(rsp));
        Log(sent ? "   → 已回 kResponse 到 " + Show(req.endpoint)
                 : "   !! 应答入队失败:" + Err(sent.error()));
      } else {
        Log("   → 单向通报,不回");
      }

      ++served;
      if (requests > 0 && served >= requests) {
        Log("[收工] 已服务 " + std::to_string(served) + " 条,按 --requests 退出");
        break;
      }
    }
  });

  (void)serving.get();  // ★ 宿主自己 join 自己的 fiber
  Coro::msleep(300);    // 让最后那帧应答真的写出去(写侧 fire-and-forget)

  Log("[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

// ───────────────────────────── 一对多发起端 ───────────────────────────────

void RunFanout(const std::vector<Endpoint>& peers) {
  UdpConfig cfg;
  cfg.mode = UdpMode::kUnicast;
  cfg.local_addr = "0.0.0.0";
  cfg.local_port = 0;  // 0 = 由 OS 分配临时端口
  // ★ 同样不配默认对端:本例的每一帧目的地都显式写在 `Message::endpoint` 里。
  cfg.silence_timeout = 5000ms;

  UdpTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("UDP 启动失败:" + Err(started.error()));
    return;
  }

  ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x07;
  ProtocolNode node(transport, std::make_unique<SystemDatagramCodec>(), ncfg);
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  Log("[发起端] 本地 socket 端口 " + std::to_string(transport.LocalPort()) +
      ",**一个节点一条 socket** 发往 " + std::to_string(peers.size()) + " 个对端\n");

  // ── ① 一对多的单向通报:一条 socket,两个不同的 ip:port ─────────────────
  //
  // 本 ADR 之前这做不到:出站目的地被写死成 `Endpoint::Default()`,两帧都会去
  // `UdpConfig` 配的那**一个**默认对端。
  Log("① 一对多 Send:目的地写进 msg.endpoint,一帧一个地址");
  std::uint8_t session = 1;
  for (const Endpoint& peer : peers) {
    Message note;
    note.message_id = kMidNote;
    note.session_id = session++;  // `Send` 不盖 session_id,原样透传(ADR-0019 D1)
    note.payload = Pay("note-to-" + Show(peer));
    note.endpoint = peer;         // ★★ 目的地
    auto queued = node.Send(std::move(note));
    Log(queued ? "   → 已发往 " + Show(peer) : "   !! " + Err(queued.error()));
  }

  // ── ② 一对多的请求-响应:各问各的,各收各的 ────────────────────────────
  //
  // ⚠ 请求-响应的关联键**不含对端**(`session_id` + `message_id` + `frm_type`)。
  //   `session_id` 是**节点全局**自增,故并发发往不同对端的请求天然不撞;但**行为异常
  //   或恶意的对端**若用了别人的 `session_id` 作答,框架无从分辨。不可信网络上跑一对多
  //   请求-响应,须自行校验 `rsp.endpoint` 的来源(README「一对多」的那条警告)。
  Log("\n② 一对多 RequestForResponse:同一个节点依次问每个对端");
  for (const Endpoint& peer : peers) {
    Message req;
    req.message_id = kMidPing;   // session_id 由框架盖(三个 RequestFor* 自己分配)
    req.payload = Pay("ping");
    req.endpoint = peer;         // ★★ 目的地
    auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
    if (!rsp) {
      Log("   ← " + Show(peer) + " 没答:" + Err(rsp.error()) +
          "(kNotAccepted = 对端始终没有受理)");
      continue;
    }
    // ★ 入站 `endpoint` 是**来源**:这就是"谁答的"。上面那条警告要校验的正是它。
    Log("   ← 来自 " + Show(rsp.value().endpoint) + ":\"" + Printable(rsp.value().payload) +
        "\" session_id=" + std::to_string(rsp.value().session_id));
    const bool from_the_one_we_asked =
        rsp.value().endpoint.port == peer.port;
    Log(std::string("      来源校验:") +
        (from_the_one_we_asked ? "就是我问的那个对端 ✔" : "**不是**我问的那个对端 ✘"));
  }

  Log("\n[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

}  // namespace

int main(int argc, char** argv) {
  const std::string role = example::OptionOr(argc, argv, "--role", "peer");
  const std::string bind_addr = example::OptionOr(argc, argv, "--bind", "0.0.0.0");
  const auto port =
      static_cast<std::uint16_t>(example::IntOptionOr(argc, argv, "--port", 9101));
  const int requests = example::IntOptionOr(argc, argv, "--requests", 2);
  const std::vector<std::string> peer_texts = example::OptionAll(argc, argv, "--peer");

  example::PrintBanner(
      "示例 ③ udp_fanout —— UDP 一对多 + 入站 endpoint 即发送方",
      {"演示:一个 ProtocolNode + 一条 UdpTransport,按 msg.endpoint 发往多个 ip:port;",
       "      回声端用 rsp.endpoint = req.endpoint 把应答送回真正的请求方(ADR-0020 + ADR-0021)。",
       "",
       "怎么跑(三个终端,**先起两个回声端**):",
       "  终端 A:  ./transport_example_udp_fanout --role peer --port 9101",
       "  终端 B:  ./transport_example_udp_fanout --role peer --port 9102",
       "  终端 C:  ./transport_example_udp_fanout --role fanout \\",
       "               --peer 127.0.0.1:9101 --peer 127.0.0.1:9102",
       "",
       "选项:--requests N 回声端服务满 N 条即退出(默认 2);0 则常驻,Ctrl-C 退出。",
       "",
       "本次参数:--role " + role});

  std::vector<Endpoint> peers;
  if (role == "fanout") {
    for (const std::string& text : peer_texts) {
      std::string host;
      std::uint16_t peer_port = 0;
      if (!example::ParseHostPort(text, &host, &peer_port)) {
        Log("--peer 解析不了:" + text + "(要 ip:port)");
        return 2;
      }
      peers.push_back(Endpoint::Net(host, peer_port));
    }
    if (peers.empty()) {
      Log("--role fanout 至少要给一个 --peer ip:port");
      return 2;
    }
  } else if (role != "peer") {
    Log("--role 只能是 peer 或 fanout");
    return 2;
  }

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环
  Coro::installFiberApplication();   // ② 装 fiber 调度器
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    if (role == "fanout") {
      RunFanout(peers);
    } else {
      RunPeer(bind_addr, port, requests);
    }
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来
  (void)task;
  return 0;
}
