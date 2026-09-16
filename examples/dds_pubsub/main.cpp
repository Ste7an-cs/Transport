/**
 * @file examples/dds_pubsub/main.cpp
 * @brief 示例 ④:`DdsNode` 的**发布-订阅**,`--role pub|sub`。
 *
 * 本例覆盖的易错点:
 *  - **`DdsNode` 的相位规则**(README「生命周期与相位规则」/ ADR-0015 D1):
 *    八个注册 / 注销方法在 **`Created` 与 `Running`** 都受理;而
 *    `Subscribe` / `Publish` / `RequestForResultDirect` / `ServeRequests` / `Reply`
 *    **只在 `Running`** 受理,`Created` 期调是**禁用法**、返 `kClosed`。
 *    本例**故意各犯一次**再打印错误码,把这条规则演示成可见的事实。
 *  - **寻址取自 `msg.endpoint`**(ADR-0020 D6):`Publish` 的 endpoint **须是 `kTopic`**,
 *    填 `Endpoint::Service(...)` 直接返 `kInvalidArgument`(本例也故意犯一次)。
 *  - **codec 与介质匹配**:DDS 是报文式、每 sample 一条完整消息 ⇒ 无状态的 `DdsCodec`。
 *  - **接收 `payload` 是指进 `frame` 的视图**(ADR-0020 D2):作用域内直接用(零拷贝),
 *    要存进容器才 `OwnedPayload()`。
 *  - **传输必须先于节点 `Start()`**:`DdsNode::Start()` 要在已 `Init` 的 provider 上声明
 *    端点,传输没起来时声明一律返 `kInvalidState`。
 *
 * ⚠ **`Publish` 无重发,首帧丢了就是永久丢失**:DDS 的 writer 与对端 reader 之间有约
 *   **240ms 的发现窗口**,窗口内写出的帧直接丢失而 `Publish` 照样返回成功。框架**不提供**
 *   "何时可安全发送"的判据。本例的处置是:**先等链路 `kUp` 再发**(轮询
 *   `CurrentLinkState()`),这只是宿主自选的一种处置,不是框架承诺。
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
#include "transport/core/Message.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/DdsNode.hpp"

#include "ExampleCommon.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Log;
using example::Pay;
using example::Text;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::LinkState;
using transport::Message;
using transport::MessageKind;

namespace {

DdsConfig MakeConfig(int domain, const std::string& provider) {
  DdsConfig cfg;
  cfg.domain_id = domain;    // [0, 232],`Start()` 时校验
  cfg.provider = provider;   // "fastdds"(真实互通)/ "fake"(**进程内**总线,跨不了进程)
  // QoS 统一一套,声明端点时不再带 QoS 参数。
  cfg.qos.reliability = transport::DdsQos::Reliability::kReliable;
  cfg.qos.durability = transport::DdsQos::Durability::kVolatile;
  cfg.qos.history_depth = 10;
  cfg.qos.max_blocking_time = 200ms;   // 须为正
  cfg.qos.liveliness_lease = 1000ms;   // 须为正;**不可省**,否则对端被硬杀要等 20s 才检出
  return cfg;
}

/// 轮询到链路 `kUp`(由 provider 的 `MatchedCount()` 推出)或耗尽预算。
/// **不是框架承诺的"何时安全"判据**,只是本示例自选的一种处置,见文件头的 ⚠。
bool WaitLinkUp(DdsTransport& transport, int budget_ms) {
  for (int waited = 0; waited < budget_ms; waited += 50) {
    if (transport.CurrentLinkState() == LinkState::kUp) {
      return true;
    }
    Coro::msleep(50);  // fiber 版休眠:让出线程,别的 fiber 才跑得动
  }
  return transport.CurrentLinkState() == LinkState::kUp;
}

// ───────────────────────────── 发布方 ─────────────────────────────────────

void RunPublisher(const DdsConfig& cfg, const std::string& topic, int count) {
  // ★ **传输由宿主创建并先启动**:节点 `Start()` 要在已 Init 的 provider 上声明端点。
  DdsTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("DDS 传输启动失败:" + Err(started.error()) +
        "(kConfiguration:domain 越界 / provider 名未注册)");
    return;
  }

  DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

  // —— 相位演示 ①:`Publish` 只在 `Running` 受理 ——————————————————————
  {
    Message early;
    early.payload = Pay("too-early");
    early.endpoint = Endpoint::Topic(topic);
    auto denied = node.Publish(std::move(early));
    Log("[相位] Start() 之前 Publish → " + Err(denied.error()) +
        "(kClosed 一码覆盖 未启动 / 关闭中 / 已关闭)");
  }

  // —— 注册:**`Created` 期只落注册表**,端点由 `Start()` 统一建 ————————
  //    (注册在 `Running` 期同样受理,落表并**当场**建端点;但那样建出来的 writer
  //     其首帧会静默丢失 —— 240ms 发现窗口,ADR-0015。)
  if (auto ok = node.RegisterPublishers({topic}); !ok) {
    Log("注册发布 topic 失败:" + Err(ok.error()) +
        "(kInvalidArgument:空串,或以框架保留前缀 `cfg.` 开头)");
    transport.Close();
    transport.WaitClosed();
    return;
  }
  if (auto started = node.Start(); !started) {  // 端点在此一次性建出
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  Log("[发布方] domain=" + std::to_string(cfg.domain_id) + " provider=" + cfg.provider +
      " topic=\"" + topic + "\"");

  // —— 寻址演示:`Publish` 的 endpoint **须是 `kTopic`** —————————————
  {
    Message wrong;
    wrong.payload = Pay("wrong-endpoint-kind");
    wrong.endpoint = Endpoint::Service(topic);  // ✘ 服务名不是 topic(ADR-0020 D5)
    auto denied = node.Publish(std::move(wrong));
    Log("[寻址] Publish 用 Endpoint::Service → " + Err(denied.error()) +
        "(Publish 须 kTopic;RequestForResultDirect 才须 kService)");
  }

  Log(WaitLinkUp(transport, 10000)
          ? "[发现] 链路 kUp —— 对端 reader 已匹配上,现在发不会掉进发现窗口"
          : "[发现] 预算内没等到 kUp(订阅方还没起?);照发,但首帧可能永久丢失");

  for (int i = 0; i < count; ++i) {
    Message msg;
    msg.payload = Pay("telemetry#" + std::to_string(i));
    msg.endpoint = Endpoint::Topic(topic);  // ★ 目的 topic 放进 endpoint(ADR-0020 D6)
    auto ok = node.Publish(std::move(msg));
    // 本节点盖 `kind = kNotify` 并清空 correlation_id / reply_to。
    Log(ok ? "[发布] telemetry#" + std::to_string(i) + "(已入队,不代表已送达)"
           : "[发布] 失败:" + Err(ok.error()));
    Coro::msleep(500);
  }

  Log("\n[收尾] node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  node.Close();
  node.WaitClosed();
  transport.Close();
  transport.WaitClosed();
  Log("[收尾] 完成");
}

// ───────────────────────────── 订阅方 ─────────────────────────────────────

void RunSubscriber(const DdsConfig& cfg, const std::string& topic, int count) {
  DdsTransport transport(cfg);
  if (auto started = transport.Start(); !started) {
    Log("DDS 传输启动失败:" + Err(started.error()));
    return;
  }

  DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

  // —— 相位演示 ②:`Subscribe` 只在 `Running` 受理 ————————————————
  {
    auto denied = node.Subscribe(DdsNode::TopicKey{topic},
                                 DdsNode::KindKey{MessageKind::kNotify});
    Log("[相位] Start() 之前 Subscribe → " + Err(denied.error()) +
        "(`Created` 期订阅是禁用法,不是「早一点也行」)");
  }

  if (auto ok = node.RegisterSubscribers({topic}); !ok) {  // `Created` 期:只落表
    Log("注册订阅 topic 失败:" + Err(ok.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }
  if (auto started = node.Start(); !started) {  // reader 在此建出
    Log("节点启动失败:" + Err(started.error()));
    transport.Close();
    transport.WaitClosed();
    return;
  }

  // ★ 推荐写法是紧挨着的两句:`Start()` 之后立刻订阅。DataReader 建于 `DoStart()`,
  //   而 DDS 发现约需 ~240ms,故这样不会漏收启动初期的消息。
  auto sub = node.Subscribe(DdsNode::TopicKey{topic},
                            DdsNode::KindKey{MessageKind::kNotify});
  if (!sub) {
    Log("订阅失败:" + Err(sub.error()) +
        "(kConfiguration:该 topic 没注册为 Subscribers —— 它的消息根本到不了本进程)");
    node.Close();
    node.WaitClosed();
    transport.Close();
    transport.WaitClosed();
    return;
  }
  DdsNode::Ticket ticket = std::move(sub).value();
  Log("[订阅方] domain=" + std::to_string(cfg.domain_id) + " provider=" + cfg.provider +
      " topic=\"" + topic + "\",等 " + std::to_string(count) + " 条……");

  std::vector<QByteArray> archive;  // 要活过各自的 Message ⇒ 存 OwnedPayload()
  int received = 0;

  // 消费在**宿主自己的 fiber** 上,节点不代管;宿主自己 join。
  auto worker = Coro::makeTask([&] {
    while (received < count) {
      auto got = ticket.Wait(20000ms);  // 不设时限 = 一直等;这里给个预算好收工
      if (!got) {
        Log("[等待结束] " + Err(got.error()) + "(kTimeout=没等到 / kClosed=信箱被关)");
        break;
      }
      const Message& msg = got.value();
      // ★ 用法 A:`msg` 还活着,payload 这个指进 frame 的视图此刻有效 —— 直接用。
      Log("[收到] topic=\"" + msg.endpoint.topic + "\" kind=kNotify payload=\"" +
          Text(msg.payload) + "\" 整帧 " + std::to_string(msg.frame.size()) + " 字节");
      // ★ 用法 B:要留到循环之外 ⇒ 必须深拷贝(判据:要活得比 Message 久)。
      archive.push_back(msg.OwnedPayload());
      ++received;
    }
  });
  (void)worker.get();  // ★ 宿主自己 join,勿依赖 WaitClosed

  Log("\n[归档] " + std::to_string(archive.size()) +
      " 条 OwnedPayload(),在各自的 Message 析构之后仍然有效:");
  for (const QByteArray& owned : archive) {
    Log("   \"" + Text(owned) + "\"");
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
  const std::string role = example::OptionOr(argc, argv, "--role", "sub");
  const std::string topic = example::OptionOr(argc, argv, "--topic", "telemetry");
  const std::string provider = example::OptionOr(argc, argv, "--provider", "fastdds");
  const int domain = example::IntOptionOr(argc, argv, "--domain", 42);
  const int count = example::IntOptionOr(argc, argv, "--count", 3);

  example::PrintBanner(
      "示例 ④ dds_pubsub —— DdsNode 的发布-订阅",
      {"演示:RegisterPublishers / RegisterSubscribers → Start → Publish / Subscribe;",
       "      顺带把 DdsNode 的相位规则与「Publish 的 endpoint 须是 kTopic」各犯一次给你看。",
       "",
       "怎么跑(两个终端,**先起订阅方**):",
       "  终端 A:  ./transport_example_dds_pubsub --role sub --domain 42 --topic telemetry",
       "  终端 B:  ./transport_example_dds_pubsub --role pub --domain 42 --topic telemetry",
       "",
       "选项:--provider fastdds(默认,真实互通)/ fake(**进程内**总线,跨不了进程);",
       "      --domain N(默认 42)、--topic 名(默认 telemetry)、--count N(默认 3)。",
       "",
       "本次参数:--role " + role + " --domain " + std::to_string(domain) +
           " --provider " + provider + " --topic " + topic +
           " --count " + std::to_string(count)});

  if (role != "pub" && role != "sub") {
    Log("--role 只能是 pub 或 sub");
    return 2;
  }

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环
  Coro::installFiberApplication();   // ② 装 fiber 调度器
  const DdsConfig cfg = MakeConfig(domain, provider);
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    if (role == "pub") {
      RunPublisher(cfg, topic, count);
    } else {
      RunSubscriber(cfg, topic, count);
    }
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来
  (void)task;
  return 0;
}
