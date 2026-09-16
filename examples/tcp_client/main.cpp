/**
 * @file examples/tcp_client/main.cpp
 * @brief 示例 ①:`ProtocolNode` + `TcpTransport`,依次演示**四种交互模式**。
 *
 * 对端是 `examples/tcp_server`(**先起服务端**)。
 *
 * 本例覆盖的易错点:
 *  - **fiber 运行时装配**:`QCoreApplication` + `installFiberApplication()` + `makeTask` +
 *    `exec()`。**一切等待语义必须在 fiber 内**——裸线程上调 `Coro::await` 会崩,不是返错。
 *  - **codec 与介质匹配**:TCP 是**字节流**介质,配**有状态流式**的 `SystemCodec`
 *    (README「选 codec」)。
 *  - **接收到的 `payload` 是指进 `frame` 的视图**(ADR-0020 D2):作用域内直接用是零拷贝,
 *    要存起来才调 `OwnedPayload()`。判据是「**需要活得比 `Message` 久才调**」,
 *    **不是**「不确定就调」——处处防御性地拷,零拷贝收益全部消失,比不做这套还差。
 *  - **正常收尾**:`node.Close(); node.WaitClosed(); transport.Close(); transport.WaitClosed();`
 *    ——先关节点再关传输,顺序不能反(README「快速开始」)。
 */

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <QByteArray>
#include <QCoreApplication>

#include "detail/asyncdefine.h"     // Coro::msleep —— fiber 版休眠(不阻塞线程)
#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "transport/codec/SystemCodec.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "ExampleCommon.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Log;
using example::Pay;
using example::Text;
using transport::LinkState;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::RetryPolicy;
using transport::SystemCodec;
using transport::TcpConfig;
using transport::TcpTransport;

namespace {

// 本例与 examples/tcp_server 约定的命令码(**协议知识,框架不猜**:ADR-0010 D7 明确
// 「结果帧的命令码与请求帧不同,其对应关系由调用方给出」)。
constexpr std::uint16_t kMidNotify = 0x0001;         ///< ① Send:发了不管
constexpr std::uint16_t kMidEcho = 0x0010;           ///< ② RequestForResponse
constexpr std::uint16_t kMidJob = 0x0020;            ///< ③ RequestForResult:命令
constexpr std::uint16_t kMidJobResult = 0x0021;      ///< ③ 的**结果帧**命令码
constexpr std::uint16_t kMidQuery = 0x0030;          ///< ④ RequestForResultDirect:命令
constexpr std::uint16_t kMidQueryResult = 0x0031;    ///< ④ 的**结果帧**命令码

/// 等链路真的连上再发——`Start()` **不等连上**(README「生命周期」),首次连不上也不算
/// 启动失败,泵会按 `silence_timeout` 退避后无限重试。这里只是为了让示例输出好看:
/// 不等也不会丢数据(写侧在链路恢复前留在内部队列)。
bool WaitLinkUp(TcpTransport& transport, int budget_ms) {
  for (int waited = 0; waited < budget_ms; waited += 50) {
    if (transport.CurrentLinkState() == LinkState::kUp) {
      return true;
    }
    Coro::msleep(50);  // ★ fiber 版休眠:让出线程,传输的泵 fiber 才跑得动。
  }
  return transport.CurrentLinkState() == LinkState::kUp;
}

/// ★★ 全部业务代码都在**这一个函数**里,而它**身处 fiber 内**(见 main 的 ③)。
void RunClient(const std::string& host, std::uint16_t port) {
  // —— 装配三件套:传输 + codec + node ————————————————————————————————
  TcpConfig cfg;
  cfg.host = host;
  cfg.port = port;
  cfg.silence_timeout = 5000ms;  // 一个量三处用:等连上 / 读静默 / 重连退避。

  TcpTransport transport(cfg);  // 传输由**宿主**创建与启停,节点只按引用借用。
  if (auto started = transport.Start(); !started) {
    Log("传输启动失败(配置非法):" + Err(started.error()));
    return;
  }

  ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x01;  // 节点盖在每一帧上的外部系统 id。
  // ★ TCP = 字节流 ⇒ **有状态流式** codec。UDP 那种报文介质要换 `SystemDatagramCodec`,
  //   把有状态的装到 UDP 上,跨报文残留会污染下一个对端的解码(README「选 codec」)。
  ProtocolNode node(transport, std::make_unique<SystemCodec>(), ncfg);
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }

  Log(WaitLinkUp(transport, 5000) ? "[链路] 已连上 " + host + ":" + std::to_string(port)
                                  : "[链路] 尚未连上(泵会继续重试;数据先留在写队列)");

  // 需要**活得比 `Message` 久**的 payload 攒在这里——见 ③ 那一段。
  std::vector<QByteArray> backlog;

  // ── ① Send —— noresponse,发了不管 ────────────────────────────────────
  //
  // 盖章规则按方法分两档(ADR-0019):`Send` 只盖 protocol_id 与 frm_type(仅当留
  // kUnknown 时补 kCommand),**session_id 由调用方填、原样透传**——这正是服务端能用
  // 公开面回出合法应答帧的依据(见 examples/tcp_server)。
  {
    Log("\n① Send(noresponse) —— 只入队,不等任何回应");
    Message msg;
    msg.message_id = kMidNotify;
    msg.payload = Pay("hello-from-client");
    msg.session_id = 0xFF;  // 不填即恒为 0,框架**不校验**:对端按它区分帧就必须自己填。
    auto queued = node.Send(std::move(msg));
    // ★ 返回成功**只表示已入队**,不表示已发出:实际写出与失败归因都在传输的写泵里,
    //   框架不回传(fire-and-forget,ADR-0007 D3)。
    Log(queued ? "   已入队(不代表已发出)" : "   入队失败:" + Err(queued.error()));
  }

  // ── ② RequestForResponse —— needresponse,等受理 ──────────────────────
  //
  // → kCommand;等 kResponse,超时重发;次数耗尽返 **kNotAccepted**(对端**始终没有
  // 受理**),不是 kTimeout。
  {
    Log("\n② RequestForResponse(needresponse) —— 等一帧 kResponse");
    Message req;
    req.message_id = kMidEcho;
    req.payload = Pay("echo-me");
    auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
    if (rsp) {
      // ★ 用法 A(零拷贝):`rsp` 还活着,`payload` 这个**指进 frame 的视图**此刻有效,
      //   直接用即可,**不要**多此一举地 OwnedPayload()。
      Log("   收到 kResponse:payload=\"" + Text(rsp.value().payload) + "\"" +
          " session_id=" + std::to_string(rsp.value().session_id) +
          " message_id=" + example::Hex16(rsp.value().message_id));
      // 接收路径还白送一件事:**完整的原始帧**(ADR-0020 D2 必填),排障/透传都靠它。
      Log("   整帧 frame(" + std::to_string(rsp.value().frame.size()) +
          " 字节):" + example::Hex(rsp.value().frame));
    } else {
      Log("   失败:" + Err(rsp.error()) + "(kNotAccepted = 对端始终没受理)");
    }
  }

  // ── ③ RequestForResult —— withfeedback,两阶段 ────────────────────────
  //
  // → kCommand;等 kResponse(受理,超时重发,耗尽 kNotAccepted);再等 kResult
  // (**不重发**——对端正在执行,重发有重复执行的风险,超时直接 kTimeout);
  // 收到 kResult 后**框架自动回一帧 kResponse**,调用方不参与、也不该自己再回。
  {
    Log("\n③ RequestForResult(withfeedback) —— 先等受理、再等结果");
    Message req;
    req.message_id = kMidJob;
    req.payload = Pay("job-42");
    auto result = node.RequestForResult(std::move(req),
                                        RetryPolicy{2000ms, 3},  // 仅【受理阶段】
                                        kMidJobResult,           // 结果帧的命令码
                                        /*result_timeout=*/15000ms);
    if (result) {
      // ★ 用法 B(要存起来):`backlog` 会活过 `result` 这条 `Message`,视图届时悬垂,
      //   故**必须**取一份拥有型深拷贝。判据就是这一句——「要活得比 Message 久」。
      backlog.push_back(result.value().OwnedPayload());
      Log("   收到 kResult:message_id=" + example::Hex16(result.value().message_id) +
          "(已 OwnedPayload() 存进 backlog,稍后在 Message 死后再打印)");
      Log("   框架已自动补发末尾那帧 kResponse —— 本模型固有的最后一步,不是可选项");
    } else {
      Log("   失败:" + Err(result.error()) +
          "(kNotAccepted=没受理 / kTimeout=受理了但没出结果)");
    }
  }

  // ── ④ RequestForResultDirect —— **另一种协议**,直取结果 ──────────────
  //
  // 与 ③ 恰好相反的三条:就在等结果阶段重发 / 耗尽返 **kTimeout** / 收到结果后**不回应**。
  // 它**不是**外部系统协议的第五种交互,二者并存于同一节点;用哪个由调用方保证与对端
  // 协议匹配,框架对协议语义不透明、不校验(ADR-0010 D13)。
  {
    Log("\n④ RequestForResultDirect(另一种协议) —— 只等 kResult,不回应");
    Message req;
    req.message_id = kMidQuery;
    req.payload = Pay("query-now");
    auto result = node.RequestForResultDirect(std::move(req),
                                              RetryPolicy{2000ms, 3},  // 唯一等待阶段
                                              kMidQueryResult);
    if (result) {
      Log("   收到 kResult:payload=\"" + Text(result.value().payload) + "\"");
    } else {
      Log("   失败:" + Err(result.error()) + "(本交互耗尽返 kTimeout,非 kNotAccepted)");
    }
  }

  // `Message` 早已析构,但 backlog 里那份是**拥有型**的,照样可用。
  for (const QByteArray& owned : backlog) {
    Log("\n[backlog] Message 已析构,OwnedPayload() 的那份仍然有效:\"" + Text(owned) + "\"");
  }

  // ── 正常收尾:先节点、后传输,顺序不能反 ──────────────────────────────
  //
  // 节点借用传输的读流:先关节点再关传输。反过来节点的读循环会先看到流终止而自行收敛,
  // 也能收干净,但错误码会变成"传输终止"而非"我方关闭"。
  Log("\n[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();      // 返回即节点的内部 fiber 已跑完,可安全析构。
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

}  // namespace

int main(int argc, char** argv) {
  const std::string host = example::OptionOr(argc, argv, "--host", "127.0.0.1");
  const auto port =
      static_cast<std::uint16_t>(example::IntOptionOr(argc, argv, "--port", 9000));

  example::PrintBanner(
      "示例 ① tcp_client —— ProtocolNode + TcpTransport 的四种交互模式",
      {"演示:Send / RequestForResponse / RequestForResult / RequestForResultDirect,每步打印结果;",
       "      顺带演示接收 payload 的两种用法(作用域内零拷贝 vs OwnedPayload)与正常收尾顺序。",
       "",
       "怎么跑(两个终端,**先起服务端**):",
       "  终端 A:  ./transport_example_tcp_server --bind 0.0.0.0 --port 9000",
       "  终端 B:  ./transport_example_tcp_client --host 127.0.0.1 --port 9000",
       "",
       "本次参数:--host " + host + " --port " + std::to_string(port)});

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环(TCP/UDP/串口都靠它)
  Coro::installFiberApplication();   // ② 在主线程装 fiber 调度器(须在任何 makeTask 之前)
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    RunClient(host, port);
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来(内部即 app.exec())
  (void)task;
  return 0;
}
