/**
 * @file examples/tcp_server/main.cpp
 * @brief 示例 ②:**只用公开面**写出来的外部协议服务端。
 *
 * 对端是 `examples/tcp_client`(**本例先起**)。
 *
 * 本例覆盖的易错点:
 *  - **应答必须回带三项**(README「写一个外部协议服务端」):
 *    ```cpp
 *    rsp.session_id = req.session_id;   // ★ ADR-0019:否则客户端 Dispatcher 匹配不上
 *    rsp.message_id = req.message_id;   // ★ 同上(键是 session_id + message_id + frm_type)
 *    rsp.endpoint   = req.endpoint;     // ★ ADR-0021:UDP 上否则发往默认对端、请求方收不到
 *    ```
 *    TCP 忽略 `endpoint`(点对点),**但照着写总是对的**——同一段服务端代码换到 UDP
 *    (见 examples/udp_fanout)才不必改。
 *  - **三种请求-响应的应答形态不同**,服务端须按客户端所用的模式回;框架对协议语义
 *    不透明、不校验,回错形态不会报错,只会让客户端一路超时。
 *  - **入站只有订阅一条通路**(ADR-0009 D1),消费在**宿主自己的 fiber** 上,
 *    **宿主自己 `get()` join**——`WaitClosed()` 只 join 框架内部的 fiber。
 *  - **`payload` 是指进 `frame` 的视图**(ADR-0020):回显时直接用(作用域内,零拷贝),
 *    要留进审计表才 `OwnedPayload()`。
 *
 * ## 一处必须说明的脚手架:监听侧传输不在库里
 *
 * 库里的 `TcpTransport` 是**客户端**传输(连出去 + 内建透明重连);`TcpServer` 本轮不做
 * (ADR-0011 **D10**,其 `.cpp` 不在任一构建的源清单里)。而 `ProtocolNode` 只接
 * `ITransport&`,于是服务端角色缺一个"监听 + 接受 + 收发字节"的实现。
 *
 * 这件事 `tools/perf` 早就遇到过并写了一个(`perf::AcceptedTcpTransport`),本示例**直接
 * 复用它**而不再抄一份——同一个事实存两份必然漂移。**它是工具/示例代码,不是库代码**:
 * 不进 `libtransport.a`、不进公共头。
 *
 * **除这一个构造语句外,本文件全程只用公开面**:`Subscribe` / `Ticket::Wait` / `Send`。
 */

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <QByteArray>
#include <QCoreApplication>

#include "detail/asyncdefine.h"     // Coro::msleep
#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "transport/codec/SystemCodec.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "AcceptedTcpTransport.hpp"  // 见文件头:监听侧传输,复用 tools/perf 的那一个。
#include "ExampleCommon.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Hex16;
using example::Log;
using example::Pay;
using example::Text;
using transport::AnyOfType;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemCodec;

namespace {

// 与 examples/tcp_client 约定的命令码(协议知识,框架不猜)。
constexpr std::uint16_t kMidNotify = 0x0001;       ///< ① 客户端 Send:**不回**
constexpr std::uint16_t kMidEcho = 0x0010;         ///< ② RequestForResponse:回一帧 kResponse
constexpr std::uint16_t kMidJob = 0x0020;          ///< ③ RequestForResult:先 kResponse 后 kResult
constexpr std::uint16_t kMidJobResult = 0x0021;    ///< ③ 的结果帧命令码
constexpr std::uint16_t kMidQuery = 0x0030;        ///< ④ RequestForResultDirect:**只回** kResult
constexpr std::uint16_t kMidQueryResult = 0x0031;  ///< ④ 的结果帧命令码

/// ★★★ **本例的核心三行就在这里**:一条应答帧的全部必填项。
///
/// `Send` 原样透传 `session_id`(ADR-0019 D1)——**这正是服务端只用公开面就能回出合法
/// 应答帧的依据**;目的地取自 `msg.endpoint`(ADR-0021 D1)。两个 ADR 合起来才闭环。
void Respond(ProtocolNode& node, const Message& req, FrameType type,
             std::uint16_t message_id, const std::string& body) {
  Message rsp;
  rsp.frm_type = type;                 // 服务端自己决定帧类型(Send 只在 kUnknown 时补 kCommand)
  rsp.session_id = req.session_id;     // ★ ADR-0019:不回带,客户端的 Dispatcher 匹配不上
  rsp.message_id = message_id;         // ★ 键的另一半
  rsp.endpoint = req.endpoint;         // ★ ADR-0021:回给**真正的请求方**(TCP 忽略,UDP 命根子)
  rsp.payload = Pay(body);             // 出站 payload 是**拥有型**的,随手构造即可
  auto sent = node.Send(std::move(rsp));
  if (!sent) {
    Log("   !! 应答入队失败:" + Err(sent.error()));
  }
}

/// 按**客户端所用的模式**回应答——三种模式的应答形态不同,回错了不会报错,只会让客户端
/// 一路超时(框架对协议语义不透明)。
void Handle(ProtocolNode& node, const Message& req,
            std::vector<QByteArray>& audit) {
  // ★ `req` 还活着,`payload` 这个指进 `frame` 的视图此刻有效 —— 直接用,零拷贝。
  const std::string body = Text(req.payload);
  Log("[收到] frm_type=kCommand message_id=" + Hex16(req.message_id) +
      " session_id=" + std::to_string(req.session_id) + " payload=\"" + body + "\"");

  switch (req.message_id) {
    case kMidNotify:
      // ① noresponse:发了不管,**服务端也不回**。
      Log("   → ① noresponse:不回任何帧");
      break;

    case kMidEcho:
      // ② needresponse:一帧 kResponse 即终结客户端的 `RequestForResponse`。
      Log("   → ② 回一帧 kResponse(回显 payload)");
      Respond(node, req, FrameType::kResponse, req.message_id, "echo:" + body);
      break;

    case kMidJob:
      // ③ withfeedback:**先受理、后结果**。末尾那帧 kResponse 由**框架自动补发**
      //    (ADR-0010 D8),服务端不管、也不该自己再回。
      Log("   → ③ 先回 kResponse(受理),再回 kResult(结果帧命令码 " +
          Hex16(kMidJobResult) + ")");
      Respond(node, req, FrameType::kResponse, req.message_id, "accepted:" + body);
      Coro::msleep(300);  // 假装干了点活(fiber 版休眠,不阻塞线程)。
      Respond(node, req, FrameType::kResult, kMidJobResult, "done:" + body);
      break;

    case kMidQuery:
      // ④ 另一种协议(直取结果):**只回 kResult,不回受理帧**。
      Log("   → ④ 只回一帧 kResult(结果帧命令码 " + Hex16(kMidQueryResult) + ")");
      Respond(node, req, FrameType::kResult, kMidQueryResult, "answer:42");
      break;

    default:
      Log("   → 未知命令码,不回(框架不校验协议语义)");
      break;
  }

  // ★ 审计表会活过 `req` 这条 `Message`,视图届时悬垂 —— 故这里**必须**深拷贝。
  //   判据只有这一句:「需要活得比 Message 久」。不是「不确定就调」。
  audit.push_back(req.OwnedPayload());
}

/// ★★ 全部业务代码都在**这一个函数**里,而它**身处 fiber 内**(见 main 的 ③)。
void RunServer(const std::string& bind_addr, std::uint16_t port, int requests) {
  // —— 装配三件套 ————————————————————————————————————————————————————
  // 见文件头:监听侧传输不在库里,这里复用 tools/perf 的那一个。
  perf::AcceptedTcpTransport transport(bind_addr, port);
  if (auto started = transport.Start(); !started) {
    Log("监听失败(" + bind_addr + ":" + std::to_string(port) + "):" +
        Err(started.error()));
    return;
  }
  Log("[监听] " + bind_addr + ":" + std::to_string(transport.LocalPort()) + ",等客户端连上……");

  ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x01;
  // ★ TCP = 字节流 ⇒ **有状态流式** `SystemCodec`(两端必须同一套帧格式与 CRC)。
  ProtocolNode node(transport, std::make_unique<SystemCodec>(), ncfg);
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }

  // —— 入站只有订阅一条通路(ADR-0009 D1);**须在 `Start()` 之后** ——————
  auto sub = node.Subscribe(AnyOfType(FrameType::kCommand));
  if (!sub) {
    Log("订阅失败:" + Err(sub.error()) + "(kClosed:未启动 / 已关闭)");
    node.Close();
    node.WaitClosed();
    transport.Close();
    transport.WaitClosed();
    return;
  }
  auto ticket = std::move(sub).value();

  std::vector<QByteArray> audit;  // 见 Handle():要活过 Message,故存 OwnedPayload()。
  int served = 0;

  // —— 消费在**宿主自己的 fiber** 上,节点不代管 ————————————————————
  auto serving = Coro::makeTask([&] {
    for (;;) {
      auto req = ticket.Wait();  // 不设时限 = 一直等
      if (!req) {
        break;  // 信箱被节点关闭 → 退出(`Ticket::Wait` 返回终止原因)
      }
      // 消费代码的逃逸异常须**自行隔离**——框架不再兜住(ADR-0009 D3)。
      Handle(node, req.value(), audit);
      ++served;
      if (requests > 0 && served >= requests) {
        Log("\n[收工] 已服务 " + std::to_string(served) + " 条命令,按 --requests 退出");
        break;
      }
    }
  });

  // ★ **宿主自己 join 自己的 fiber**,勿依赖 `WaitClosed()`——后者只 join 框架内部的
  //   那条读-分发循环(ADR-0009 D4 / RT_LIFECYCLE_006)。
  (void)serving.get();

  // 最后一帧应答刚交给写侧(fire-and-forget:返回成功只表示已入队),给它一点时间真的
  // 写出去再拆 socket。**这不是框架的要求,是本示例为了输出好看**。
  Coro::msleep(300);

  Log("[审计] OwnedPayload() 存下的 " + std::to_string(audit.size()) +
      " 条 payload 在各自的 Message 析构后仍然可用");

  // —— 正常收尾:先节点、后传输,顺序不能反 ————————————————————————
  Log("[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

}  // namespace

int main(int argc, char** argv) {
  const std::string bind_addr = example::OptionOr(argc, argv, "--bind", "0.0.0.0");
  const auto port =
      static_cast<std::uint16_t>(example::IntOptionOr(argc, argv, "--port", 9000));
  const int requests = example::IntOptionOr(argc, argv, "--requests", 4);

  example::PrintBanner(
      "示例 ② tcp_server —— 只用公开面写的外部协议服务端",
      {"演示:Subscribe(AnyOfType(kCommand)) → 按客户端所用的交互模式回应答;",
       "      应答必须回带 session_id / message_id / endpoint 三项(ADR-0019 + ADR-0021)。",
       "",
       "怎么跑(两个终端,**先起本端**):",
       "  终端 A:  ./transport_example_tcp_server --bind 0.0.0.0 --port 9000",
       "  终端 B:  ./transport_example_tcp_client --host 127.0.0.1 --port 9000",
       "",
       "选项:--requests N 服务满 N 条命令即退出(默认 4,正好是 tcp_client 的四步);",
       "      --requests 0 则常驻,按 Ctrl-C 退出。",
       "",
       "本次参数:--bind " + bind_addr + " --port " + std::to_string(port) +
           " --requests " + std::to_string(requests)});

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环
  Coro::installFiberApplication();   // ② 装 fiber 调度器(须在任何 makeTask 之前)
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    RunServer(bind_addr, port, requests);
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来
  (void)task;
  return 0;
}
