/**
 * @file examples/dds_service/main.cpp
 * @brief 示例 ⑤:`DdsNode` 的**请求-响应**(单阶段 Direct),`--role client|service`。
 *
 * 本例覆盖的易错点:
 *  - **请求-响应只说服务名**:两个 topic 由框架按同一个派生函数算出
 *    (`cfg.<服务名>.request` / `cfg.<服务名>.response`),两侧**传一模一样的服务名**,
 *    派生规则不外泄到调用方,故不可能配歪。`cfg.` 是**框架保留前缀**。
 *  - **寻址取自 `req.endpoint` 且须是 `kService`**(ADR-0020 D5/D6):服务名**不是 topic**,
 *    填 `Endpoint::Topic(...)` 直接返 `kInvalidArgument`(本例故意犯一次)。
 *  - **相位规则**:`RegisterClients` / `RegisterServices` 在 `Created` 与 `Running` 都受理;
 *    `RequestForResultDirect` / `ServeRequests` / `Reply` **只在 `Running`**(本例也各犯一次)。
 *  - **服务端只用 `Reply(request, result)` 一个方法**:本模型**没有受理阶段**,故没有
 *    `Accept()`;应答目的地由**服务端自己注册的内容**决定,不取信于线缆。
 *  - **重发要求对端能容忍重复请求**:`RELIABLE` 的 DDS 上仍要重发——丢的不是网络,是
 *    **队列**(读队列有界 1024、满时静默丢最旧),而首帧还可能掉进约 240ms 的发现窗口。
 *    本例**不等 `kUp`**,直接让 `RetryPolicy` 去吸收这段窗口。
 *  - **接收 `payload` 是指进 `frame` 的视图**(ADR-0020):作用域内直接用,要存才 `OwnedPayload()`。
 *
 * ⚠ **跑真实 Fast DDS 且两端在不同进程时,收到的 `payload` 末尾可能多出 1..3 个零字节。**
 *   实测事实,成因见 `examples/dds_pubsub/main.cpp` 文件头的同名说明(RTPS 的 4 字节对齐
 *   填充被当成了 payload)。本例把 payload 的字节数与整帧字节数都打出来,方便对照
 *   ——`整帧 % 4 == 0` 时就没有填充。示例不兜这个底。
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

#include "transport/codec/DdsCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/DdsNode.hpp"

#include "ExampleCommon.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Log;
using example::Pay;
using example::Printable;
using example::Text;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::Message;
using transport::MessageKind;
using transport::RetryPolicy;
using transport::TransportErrc;

namespace {

DdsConfig MakeConfig(int domain, const std::string& provider) {
  DdsConfig cfg;
  cfg.domain_id = domain;
  cfg.provider = provider;
  cfg.qos.reliability = transport::DdsQos::Reliability::kReliable;
  cfg.qos.durability = transport::DdsQos::Durability::kVolatile;
  cfg.qos.history_depth = 10;
  cfg.qos.max_blocking_time = 200ms;  // 须为正
  cfg.qos.liveliness_lease = 1000ms;  // 须为正;不可省
  return cfg;
}

// ───────────────────────────── 服务端 ─────────────────────────────────────

void RunService(const DdsConfig& cfg, const std::string& service_name, int seconds) {
  // ★ 传输由宿主创建并**先启动**:节点 Start() 要在已 Init 的 provider 上声明端点。
  DdsTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("DDS 传输启动失败:" + Err(started.error()));
    return;
  }

  DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

  // —— 相位演示:`ServeRequests` 只在 `Running` 受理 ————————————————
  {
    auto denied = node.ServeRequests(service_name);
    Log("[相位] Start() 之前 ServeRequests → " + Err(denied.error()) +
        "(kClosed **先于**注册校验)");
  }

  // ★ **只说服务名**:`cfg.<名>.request` 建 Reader(收请求)、`cfg.<名>.response`
  //   建 Writer(发应答)。客户端传一模一样的名字,各自按角色建各自那一侧。
  if (auto ok = node.RegisterServices({service_name}); !ok) {
    Log("注册服务失败:" + Err(ok.error()) +
        "(kInvalidArgument:空串,或该名已注册为 Clients —— 会变成自问自答)");
    transport.Close();
    transport.WaitClosed();
    return;
  }
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }

  auto serving = node.ServeRequests(service_name);  // = Subscribe(cfg.<名>.request, kRequest)
  if (!serving) {
    Log("ServeRequests 失败:" + Err(serving.error()) +
        "(kConfiguration:该服务名没注册为 Services)");
    node.Close();
    node.WaitClosed();
    transport.Close();
    transport.WaitClosed();
    return;
  }
  DdsNode::Ticket requests = std::move(serving).value();
  Log("[服务端] domain=" + std::to_string(cfg.domain_id) + " 服务名=\"" + service_name +
      "\" → 请求 topic cfg." + service_name + ".request / 应答 topic cfg." +
      service_name + ".response");
  Log("[服务端] 开张,最多待 " + std::to_string(seconds) + " 秒……");

  std::vector<QByteArray> journal;  // 要活过各自的 Message ⇒ OwnedPayload()
  int served = 0;

  auto worker = Coro::makeTask([&] {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (std::chrono::steady_clock::now() < deadline) {
      // 短时限轮询:**让出 fiber** 的同时也给收工判据一个观察点。
      // 不 park 线程 —— 节点的读-分发循环就跑在同一条线程的另一条 fiber 上。
      auto request = requests.Wait(200ms);
      if (!request) {
        if (request.error() == transport::make_error_code(TransportErrc::kTimeout)) {
          continue;  // 只是这一轮没请求
        }
        break;       // 信箱被节点关闭 → 收工
      }
      const Message& req = request.value();
      // ★ 作用域内直接用视图,零拷贝。
      Log("[请求] corr=" + req.correlation_id + " 来源 topic=\"" + req.endpoint.topic +
          "\" payload=\"" + Printable(req.payload) + "\"(" +
          std::to_string(req.payload.size()) + " 字节,整帧 " +
          std::to_string(req.frame.size()) + " 字节)");
      journal.push_back(req.OwnedPayload());  // ★ 要留到循环之外 ⇒ 深拷贝

      // ★ 服务端**唯一**的方法:`Reply`。本模型没有受理阶段,故没有 Accept()。
      //   应答 topic 由服务端**自己注册的内容**反查派生,**不取信于线缆**;线缆上的
      //   `reply_to` 只作一致性交叉校验,不等即返 kInvalidArgument。
      Message result;
      result.payload = Pay("echo:" + Text(req.payload));  // kind / corr / endpoint 由节点盖
      auto replied = node.Reply(req, std::move(result));
      Log(replied ? "   → 已回 kReply" : "   !! Reply 失败:" + Err(replied.error()));

      // ⚠ **每条重发都照答**:重发要求对端能容忍重复请求(幂等,或自行按
      //   correlation_id 去重)——协议层假设,框架不校验。
      ++served;
    }
  });
  (void)worker.get();  // ★ 宿主自己 join 自己的 fiber

  Log("\n[收工] 共服务 " + std::to_string(served) + " 条请求;journal 里 " +
      std::to_string(journal.size()) + " 条 OwnedPayload() 仍然有效");
  Coro::msleep(300);  // 让最后那帧应答真的写出去(fire-and-forget)

  Log("[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

// ───────────────────────────── 客户端 ─────────────────────────────────────

void RunClient(const DdsConfig& cfg, const std::string& service_name,
               const std::string& question) {
  DdsTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("DDS 传输启动失败:" + Err(started.error()));
    return;
  }

  DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

  // —— 相位演示:`RequestForResultDirect` 只在 `Running` 受理 ————————
  {
    Message early;
    early.payload = Pay(question);
    early.endpoint = Endpoint::Service(service_name);
    auto denied = node.RequestForResultDirect(std::move(early), RetryPolicy{100ms, 1});
    Log("[相位] Start() 之前 RequestForResultDirect → " + Err(denied.error()));
  }

  // ★ 与服务端**传一模一样的服务名**:`cfg.<名>.request` 建 Writer(发请求)、
  //   `cfg.<名>.response` 建 Reader(收应答)。
  if (auto ok = node.RegisterClients({service_name}); !ok) {
    Log("注册客户端失败:" + Err(ok.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  if (auto started = node.Start(); !started) {
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  Log("[客户端] domain=" + std::to_string(cfg.domain_id) + " 服务名=\"" + service_name +
      "\" uuid=" + node.uuid());

  // —— 寻址演示 ①:endpoint **须是 `kService`**,不是 `kTopic` ——————————
  {
    Message wrong;
    wrong.payload = Pay(question);
    wrong.endpoint = Endpoint::Topic(service_name);  // ✘ 服务名不是 topic(ADR-0020 D5)
    auto denied = node.RequestForResultDirect(std::move(wrong), RetryPolicy{100ms, 1});
    Log("[寻址] 用 Endpoint::Topic 发请求 → " + Err(denied.error()) +
        "(须 Endpoint::Service;Publish 那边才是 kTopic)");
  }

  // —— 寻址演示 ②:没注册过的服务名 → `kConfiguration`,**不猜、不回落** ——
  {
    Message unknown;
    unknown.payload = Pay(question);
    unknown.endpoint = Endpoint::Service(service_name + ".not-registered");
    auto denied = node.RequestForResultDirect(std::move(unknown), RetryPolicy{100ms, 1});
    Log("[寻址] 用没注册过的服务名 → " + Err(denied.error()));
  }

  // —— 真正的一次请求-响应 ————————————————————————————————————————
  //
  // ```
  // → kRequest
  // ⏱ 等 kReply ──超时──▶ 重发 ──次数耗尽──▶ kTimeout
  // ← kReply                                ⇒ 成功(返回该帧,**不回应**)
  // ```
  // 签名里**没有** result_timeout:本交互只有一个等待阶段,其时限即 `retry.timeout`;
  // 耗尽返 `kTimeout`(本模型没有受理阶段,故不用 kNotAccepted)。
  //
  // **这里刻意不等链路 kUp**:让重发去吸收约 240ms 的发现窗口 —— 首帧若抢在匹配之前
  // 发出会永久丢失(VOLATILE 不补发),一秒后的重发落在匹配之后,当场成事。
  Message req;
  req.payload = Pay(question);
  req.endpoint = Endpoint::Service(service_name);  // ★ 服务名放进 endpoint
  Log("\n[请求] \"" + question + "\" → service(" + service_name +
      "),RetryPolicy{1000ms, 8} 吸收发现窗口");
  auto reply = node.RequestForResultDirect(std::move(req), RetryPolicy{1000ms, 8});
  if (reply) {
    // ★ 作用域内直接用 payload 视图,零拷贝。
    Log("[应答] payload=\"" + Printable(reply.value().payload) + "\"(" +
        std::to_string(reply.value().payload.size()) + " 字节,整帧 " +
        std::to_string(reply.value().frame.size()) + " 字节)");
    Log("       kind=kReply corr=" + reply.value().correlation_id +
        "(两段式:<uuid>#<自增序号>)");
    // ★ 应答落在**派生出的**应答 topic 上——服务名 → cfg.<名>.response。
    Log("       来源 topic=\"" + reply.value().endpoint.topic + "\"");
  } else {
    Log("[应答] 失败:" + Err(reply.error()) +
        "(kTimeout=重发耗尽 / kConfiguration=服务名没注册为 Clients)");
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
  const std::string role = example::OptionOr(argc, argv, "--role", "service");
  const std::string service_name = example::OptionOr(argc, argv, "--service", "get");
  const std::string provider = example::OptionOr(argc, argv, "--provider", "fastdds");
  const std::string question = example::OptionOr(argc, argv, "--ask", "ping");
  const int domain = example::IntOptionOr(argc, argv, "--domain", 43);
  const int seconds = example::IntOptionOr(argc, argv, "--seconds", 15);

  example::PrintBanner(
      "示例 ⑤ dds_service —— DdsNode 的请求-响应(单阶段 Direct)",
      {"演示:RegisterClients / RegisterServices(两侧传一模一样的服务名)→ Start →",
       "      RequestForResultDirect / ServeRequests + Reply;两个 topic 由框架派生。",
       "      顺带把相位与寻址的三条规则各犯一次给你看。",
       "",
       "怎么跑(两个终端,**先起服务端**):",
       "  终端 A:  ./transport_example_dds_service --role service --domain 43 --service get",
       "  终端 B:  ./transport_example_dds_service --role client  --domain 43 --service get",
       "",
       "选项:--provider fastdds(默认)/ fake(**进程内**总线,跨不了进程);",
       "      --domain N(默认 43)、--service 名(默认 get)、--ask 文本、",
       "      --seconds N 服务端最多待多久(默认 15)。",
       "",
       "本次参数:--role " + role + " --domain " + std::to_string(domain) +
           " --provider " + provider + " --service " + service_name});

  if (role != "client" && role != "service") {
    Log("--role 只能是 client 或 service");
    return 2;
  }

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环
  Coro::installFiberApplication();   // ② 装 fiber 调度器
  const DdsConfig cfg = MakeConfig(domain, provider);
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    if (role == "client") {
      RunClient(cfg, service_name, question);
    } else {
      RunService(cfg, service_name, seconds);
    }
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来
  (void)task;
  return 0;
}
