# transport — C++ 通信中间件

C++17 通信中间件库，把**传输**、**编解码**、**交互**三层彻底解耦，以 **AsyncTask 协程运行时**（boost.fiber）为异步执行环境。

| 层 | 职责 |
|---|---|
| **Transport**（纯字节管道） | 跨 TCP / UDP / 串口 / DDS 搬运原始字节，不解释消息类型、请求关联或 payload 语义。内部一条管理泵负责链路的建立、重建与重试，对外只交出读队列的等待器句柄与写入口。 |
| **ICodec**（线缆格式） | 在收发边界把逻辑 `Message` ↔ 线缆字节（分帧 + 序列化 + 校验 + 重同步）。流式跨切片拼帧、报文式保边界。**应用可自行提供并装配。** |
| **node**（交互层） | 组合前两层，交付请求-响应与发布-订阅。协议语义内联各 node，不设共享交互引擎。 |

**非目标**：不解析 payload 业务语义；不是消息代理或通用路由守护进程；不内建加密、认证与访问控制。

---

## 目录

- [运行时：一切都在 fiber 里跑](#运行时一切都在-fiber-里跑) —— **先读这一节**
- [快速开始](#快速开始)
- [装配三件套](#装配三件套)：[选传输](#1-选传输) · [选 codec](#2-选-codec) · [选 node](#3-选-node)
- [`ProtocolNode`：四种交互模式](#protocolnode四种交互模式)
- [订阅入站消息](#订阅入站消息)
- [`DdsNode`：发布-订阅与请求-响应](#ddsnode发布-订阅与请求-响应)
- [`Message` 的字段归属](#message-的字段归属)
- [生命周期与相位规则](#生命周期与相位规则)
- [错误码](#错误码)
- [扩展：自定义 codec](#扩展自定义-codec)
- [扩展：新建一个 node](#扩展新建一个-node) —— 交互方式变了的时候
- [内部传输契约（`ITransport`）](#内部传输契约itransport)
- [构建](#构建)
- [关键约束](#关键约束)

---

## 运行时：一切都在 fiber 里跑

**这是最容易踩的一步。** 本库的全部等待语义（`RequestFor*`、`await`、`Ticket::Wait`）都建立在 **AsyncTask 的 boost.fiber 协程**之上。它们**必须在 fiber 上下文里调用**——在裸线程上调用 `Coro::await` 会直接崩，不是返回错误。

所以宿主的 `main` 长这样：

```cpp
#include <QCoreApplication>
#include "task/fiberapplication.h"   // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"          // Coro::makeTask

int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);     // ① Qt 事件循环（TCP/UDP/串口都靠它）

  Coro::installFiberApplication();      // ② 在主线程装 fiber 调度器

  auto task = Coro::makeTask([&] {      // ③ 业务代码写在 fiber 里
    RunApplication();                   //    —— 创建传输/节点、收发、关闭都在这里面
    Coro::quit();                       // ④ 干完让 exec() 返回
  });

  Coro::exec();                         // ⑤ 跑起来（内部即 app.exec()）
  (void)task;
  return 0;
}
```

| 记号 | 含义 |
|---|---|
| `installFiberApplication()` | 把当前线程变成 fiber 调度器宿主。**在任何 `makeTask` 之前调用一次。** |
| `makeTask(fn)` | 起一条 fiber 跑 `fn`，返回 `FiberTask<T>` 句柄。**不阻塞。** |
| `FiberTask::get()` | 让出式 join：等这条 fiber 真正跑完。**这是"可安全析构"的唯一充分条件。** |
| `Coro::exec()` / `Coro::quit()` | 启动 / 退出事件循环，对应 `QCoreApplication::exec/quit`。 |

**下文所有代码片段都默认身处 ③ 那个 fiber 内。**

> 已有 Qt 事件循环的宿主（GUI 程序等）同样调 `installFiberApplication()`，之后用 `makeTask` 起 fiber 即可；`Coro::exec()` 换成你原本的 `app.exec()`。

---

## 快速开始

编程主入口是**交互层 node**。传输由**宿主**创建、启动、关闭，节点按引用借用。

```cpp
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace transport;
using namespace std::chrono_literals;

void RunApplication() {                // ← 身处 fiber 内（见上一节）
  TcpConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 9000;
  cfg.silence_timeout = 5000ms;        // 唯一的时间量：等连上 / 读静默 / 重连间隔

  TcpTransport transport(cfg);
  if (!transport.Start()) return;      // 宿主启动传输；配置非法在此返 kConfiguration

  ProtocolNode node(transport, std::make_unique<SystemCodec>(), ProtocolNodeConfig{});
  if (!node.Start()) return;

  Message req;
  req.payload    = {0x01, 0x02};
  req.message_id = 0x10;               // 命令码由调用方给
  auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
  if (rsp) { /* 用 rsp.value().payload */ }

  node.Close();        node.WaitClosed();      // 先关节点
  transport.Close();   transport.WaitClosed(); // 再关传输
}
```

**关闭顺序是有讲究的**：节点借用传输的读流，先关节点再关传输；反过来节点的读循环会先看到流终止而自行收敛，虽然也能收干净，但错误码会变成"传输终止"而非"我方关闭"。

---

## 装配三件套

一个可用的通信端 = **传输** + **codec** + **node**。三者独立选，但有匹配约束。

### 1. 选传输

四种介质，全部实现同一个 `ITransport`。**共性**：`Start()` 起内部泵后即返回（首次连不上/绑不上/打不开**不算启动失败**，泵会无限重试）；**都不自终**，唯一的退出条件是宿主 `Close()`。

#### `TcpTransport` —— 流式，客户端，自动重连

```cpp
#include "transport/io/tcp/TcpTransport.hpp"

TcpConfig cfg;
cfg.host            = "192.168.1.10";  // 非空，否则 Start() 返 kConfiguration
cfg.port            = 9000;            // 非 0，否则 kConfiguration
cfg.silence_timeout = 5000ms;          // 须为正，否则 kConfiguration
TcpTransport transport(cfg);
```

`silence_timeout` **一个量三处用**：等连上 / 读静默判链路坏 / 连不上时的重连退避。重连对上层**完全透明**——节点保持 `Running`，读循环没有断链分支，在途交互不被批量终结。

> **服务端**见 `TcpServer`（`include/transport/io/tcp/TcpServer.hpp`）：每条接受的连接派生一个独立节点。

#### `UdpTransport` —— 报文式，单播 / 组播 / 广播

```cpp
#include "transport/io/udp/UdpTransport.hpp"

UdpConfig cfg;
cfg.mode        = UdpMode::kUnicast;   // kUnicast / kMulticast / kBroadcast
cfg.local_addr  = "0.0.0.0";
cfg.local_port  = 8000;                // 0 = 由 OS 分配临时端口
cfg.remote_addr = "192.168.1.255";     // Send() 的默认目的地（单播/广播）
cfg.remote_port = 9000;
// cfg.multicast_group = "239.0.0.1";  // 仅 kMulticast：Send() 的默认目的地
// cfg.ttl             = 1;            // 组播 TTL（hops）
cfg.silence_timeout = 5000ms;
UdpTransport transport(cfg);
```

**保报文边界**，`Datagram::peer` 是**可变**的：读到的是发送方地址，写出的是目的地——故一条 UDP 传输可对多个对端收发。

> ⚠ **UDP 是唯一不校验配置的传输**：它没有 `kConfiguration` 这条路径，`silence_timeout` 非正时**静默兜底为 5s**。TCP 与串口则在 `Start()` 直接拒绝并停在 `Created`。

#### `SerialTransport` —— 流式，单设备，自动重开

```cpp
#include "transport/io/serial/SerialTransport.hpp"

SerialConfig cfg;
cfg.device          = "/dev/ttyUSB0";  // 非空，否则 kConfiguration
cfg.baud_rate       = 115200;          // 非 0
cfg.data_bits       = 8;               // 5 / 6 / 7 / 8
cfg.stop_bits       = 1;               // 1 或 2
cfg.parity          = 'N';             // 'N' / 'E' / 'O'（大小写均可）
cfg.silence_timeout = 5000ms;          // 须为正
SerialTransport transport(cfg);
```

> ⚠ **`silence_timeout` 必须按协议特征配**。串口**没有断开事件**，读静默超时是判"链路坏了"的**唯一**主动判据。缺省 5s 适合"周期性上报"类协议；**"长时间静默、偶发指令"类协议必须调大**，否则会周期性地无谓重开设备。

`peer` **恒为固定设备端点**（`Endpoint::Default()`），写侧忽略调用方填的 `peer`。

#### `DdsTransport` —— 按 topic 收发

```cpp
#include "transport/io/dds/DdsTransport.hpp"

DdsConfig cfg;
cfg.domain_id = 0;                     // [0, 232]
cfg.provider  = "fastdds";             // "fake"（进程内，恒可用）/ "fastdds"（需装 Fast DDS）
cfg.qos.reliability      = DdsQos::Reliability::kReliable;
cfg.qos.durability       = DdsQos::Durability::kVolatile;
cfg.qos.history_depth    = 10;
cfg.qos.max_blocking_time = 100ms;     // 须为正
cfg.qos.liveliness_lease  = 2000ms;    // 须为正；不可省，否则对端被硬杀要等 20s 才检出
DdsTransport transport(cfg);
```

**QoS 统一一套**，声明端点时不再带 QoS 参数。测试里把 `provider` 换成 `"fake"` 即可全程离线跑，不需要装 Fast DDS。

> ⚠ **`max_blocking_time` 不是关闭路径的最坏等待。** `Publish` 的阻塞主要来自**同进程订阅方的交付回调在发布线程上同步执行**（Fast DDS 默认 `INTRAPROCESS_FULL`），该项根本不参与。关闭时的最坏等待**没有上界**，由同进程内最慢的那个订阅回调决定。

### 2. 选 codec

**codec 必须和介质的分帧特性匹配**——把流式 codec 装到 UDP 上会出错。

| codec | 形态 | 配哪种介质 | 干什么 |
|---|---|---|---|
| `SystemCodec` | **有状态·流式** | TCP / 串口 | 外部协议完整帧：头标志 + 帧类型 + CRC + 长度。跨切片拼帧，坏帧逐字节重同步。 |
| `SystemDatagramCodec` | 无状态·报文 | UDP | 同一套帧格式的**报文版**：只解本报文内的整帧，残留直接丢弃，**零跨报文状态**（多对端安全）。 |
| `LengthFieldCodec` | 有状态·流式 | TCP / 串口 | 通用「固定 header + 长度字段」分帧，`payload` 透传。不解释帧内语义。 |
| `DatagramCodec` | 无状态·报文 | UDP | 直通：整段字节即一条 `kOneway` 消息。 |
| `DdsCodec` | 无状态 | DDS | 每 sample 一条完整消息，携带 `kind` / `correlation_id` / `reply_to`。 |

**选择规则**：字节流介质（TCP / 串口）→ 流式 codec；报文介质（UDP / DDS）→ 无状态 codec。**把有状态的 `SystemCodec` 装到 UDP 上，跨报文残留会污染下一个对端的解码。**

```cpp
// 外部协议 over TCP
ProtocolNode node(tcp,  std::make_unique<SystemCodec>(),         ProtocolNodeConfig{});
// 外部协议 over UDP
ProtocolNode node(udp,  std::make_unique<SystemDatagramCodec>(), ProtocolNodeConfig{});
```

`SystemCodec` / `SystemDatagramCodec` 的 CRC 算法经构造注入（`CrcFn`），**默认是占位的 CRC16-CCITT**；真实对接时须替换成外部协议规定的算法，两端一致。`FrameType` 的枚举值同样是**占位字节值**，须改成协议规定的真实值。

### 3. 选 node

| node | 交互 | 配哪种传输 |
|---|---|---|
| `ProtocolNode` | 外部协议：`Send` + 三种请求-响应 | TCP / UDP / 串口 |
| `DdsNode` | 发布-订阅 + 按服务名的请求-响应 | DDS |

```cpp
ProtocolNodeConfig ncfg;
ncfg.protocol_id = 0x01;               // 节点盖在每一帧上的外部系统 id
ProtocolNode node(transport, std::make_unique<SystemCodec>(), ncfg);
```

---

## `ProtocolNode`：四种交互模式

`RetryPolicy{timeout, max_attempts}` **逐次传参**，节点配置面上没有任何时限缺省值；`timeout` 须为正、`max_attempts` 须 ≥ 1（**含首发**），否则返 `kInvalidArgument`。

**盖章规则**：调用方填 `payload` 与 `message_id`，节点盖 `frm_type` / `protocol_id` / `session_id`。

---

#### ① `Send(msg)` —— noresponse，发了不管

```cpp
Message msg;
msg.payload = {0x01, 0x02};
auto ok = node.Send(std::move(msg));      // 不登记任何订阅
```

**返回成功只表示已入队，不表示已发出**——实际写出与失败归因都在传输的写泵里，框架不回传。

---

#### ② `RequestForResponse(req, retry)` —— needresponse，等受理

```
→ kCommand
⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
← kResponse                                ⇒ 成功（返回该帧）
```

```cpp
auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
```

耗尽次数返 **`kNotAccepted`**（对端**始终没有受理**）而**不是** `kTimeout`。

---

#### ③ `RequestForResult(req, retry, result_mid, result_timeout)` —— withfeedback / needfeedback，两阶段

```
→ kCommand
⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
← kResponse（受理）
⏱ 等 kResult   ──超时──▶ kTimeout（【不重发】）
← kResult
→ kResponse（回应结果）                     ⇒ 成功（返回 kResult 那一帧）
```

```cpp
auto result = node.RequestForResult(std::move(req),
                                    RetryPolicy{2000ms, 3},   // 仅【受理阶段】的重发策略
                                    /*result_message_id=*/0x82,
                                    /*result_timeout=*/30s);
```

三点要注意：

- **`retry` 只管受理阶段**；等结果的时限是独立的 `result_timeout`。
- **第二阶段不重发**：`kResult` 未达意味着对端**正在执行**，重发命令有使其重复执行的风险，故超时直接以 `kTimeout` 终结。
- **末尾那帧回应是本模型固有的最后一步，不是可选项**——框架自动发出，完全由收到的 `kResult` 派生（payload 原样回显、`session_id`/`message_id` 沿用，仅把帧类型改为 `kResponse`）。调用方不参与，也不该自己再回一帧。

`result_message_id` 是**结果帧的命令码**，与请求帧不同；其对应关系是协议知识，**框架不猜、不做映射**，须由调用方给出。

---

#### ④ `RequestForResultDirect(req, retry, result_mid)` —— 另一种协议，直取结果

```
→ kCommand
⏱ 等 kResult ──超时──▶ 重发 ──次数耗尽──▶ kTimeout
← kResult                                ⇒ 成功（返回该帧，【不回应】）
```

```cpp
auto result = node.RequestForResultDirect(std::move(req),
                                          RetryPolicy{2000ms, 3},   // 唯一的等待阶段
                                          /*result_message_id=*/0x82);
```

> ⚠ **它不是外部系统协议的第五种交互。** `Send` / `RequestForResponse` / `RequestForResult` 属**外部系统协议**，本方法属**另一种协议**，二者并存于同一节点。**调用方须自行确保所用方法与对端协议匹配——框架对协议语义不透明，不校验。**

**与 ③ 恰好相反的三条，勿混：**

| | `RequestForResult`（③） | `RequestForResultDirect`（④） |
|---|---|---|
| 等结果阶段 | **不重发**（重发有使对端重复执行的风险） | **就在这一阶段重发**（没有受理阶段，不重发则命令帧一旦丢包即彻底失败、无任何补救） |
| 耗尽返回 | `kNotAccepted` | **`kTimeout`**——本交互根本不存在"受理"这一步 |
| 收到结果后 | **自动回一帧** `kResponse` | **不回应任何帧** |

签名里**没有**独立的 `result_timeout`：本交互只有一个等待阶段，其时限即 `RetryPolicy::timeout`。

---

#### 重发的共同纪律（②④）

重发的是**字节完全相同的原帧**、`session_id` 不变，故原订阅横跨全部重发继续有效，**最先到达的那一帧即终结本次交互**，框架不区分它对应第几次尝试。

由此**要求对端能容忍重复命令**（幂等，或自行按 `session_id` 去重）。这是**协议层假设，框架不校验**。

---

## 订阅入站消息

节点只交出**凭据**，消费在调用方自己的 fiber 上：

```cpp
auto sub = node.Subscribe(AnyOfType(FrameType::kCommand));   // 须在 Start() 之后
if (!sub) { /* kClosed：未启动 / 已关闭 */ }
auto ticket = std::move(sub).value();

auto worker = Coro::makeTask([&] {
  for (;;) {
    auto m = Coro::await(ticket.mailbox());
    if (!m) break;                    // 信箱被节点关闭 → 退出
    try { Handle(m.value()); }
    catch (...) { /* 自行隔离 */ }
  }
});
...
(void)worker.get();                   // 宿主自己 join，勿依赖 WaitClosed
```

订阅键的具名工厂：

| 工厂 | 匹配 |
|---|---|
| `ResponseTo(request)` | 该请求的应答帧（`session_id` + `message_id` + `kResponse`） |
| `FrameOf(session_id, message_id, type)` | 三字段精确匹配；任一字段可填 `kAny` |
| `AnyOfType(type)` | 只按帧类型，另两字段通配 |

不参与匹配的字段填 **`kAny`**——它以 `std::optional` 的空状态表达，**不占用字段值域**，故 `session_id` 这种 0..255 全用满的字段照样能通配；`kAny` 与"该字段须等于 0"是两种不同的约束。

**一条消息投给全部键匹配的订阅者，各得一份副本**——故支持多消费者与旁路监听。投递份数为 0 时静默丢弃。

`Ticket` 是 **move-only 的 RAII 句柄**：析构即注销订阅。同一凭据可多次 `Wait()`，故一次交互需分段等待多条报文时，各段各自设定时限。

> ⚠ **信箱满时静默丢弃队首最旧值**，且**不可观测**。消费 fiber 要跟得上投递速度。

---

## `DdsNode`：发布-订阅与请求-响应

topic 由**注册接口**给出，且只在 `Start()` 之前受理：

```cpp
DdsTransport transport(dds_config);
(void)transport.Start();

DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

(void)node.RegisterPublishers ({"telemetry"});   // 发布：topic
(void)node.RegisterSubscribers({"telemetry"});   // 订阅：topic
(void)node.RegisterClients    ({"get"});         // 请求-响应客户端：服务名
(void)node.RegisterServices   ({"get"});         // 请求-响应服务端：服务名

(void)node.Start();                              // 端点在此一次性建出
```

**请求-响应只说服务名**，两个 topic 由框架派生（`cfg.` 为固定前缀）：

```
请求 topic = cfg.<服务名>.request        应答 topic = cfg.<服务名>.response
```

两侧用同一个派生函数，故不可能配歪。客户端与服务端**传一模一样的服务名**。

```cpp
// —— 客户端 ——
auto result = node.RequestForResultDirect("get", req, RetryPolicy{2000ms, 3});
if (result) { /* 用 result.value().payload */ }

// —— 服务端 ——
auto serving = node.ServeRequests("get");
if (!serving) { /* kConfiguration：未注册该服务名 */ }
auto tickets = std::move(serving).value();

auto worker = Coro::makeTask([&] {
  for (;;) {
    auto req = tickets.Wait();            // 不设时限 = 一直等
    if (!req) break;                      // 信箱被节点关闭 → 退出
    (void)node.Reply(req.value(), Handle(req.value()));
  }
});
...
(void)worker.get();                       // 宿主自己 join

// —— 发布-订阅 ——
(void)node.Publish("telemetry", msg);

auto sub = node.Subscribe(DdsNode::TopicKey{"telemetry"},   // 两个键都是 DdsNode 的嵌套别名
                          DdsNode::KindKey{MessageKind::kNotify});
auto notes = std::move(sub).value();      // 消费同上：自己起 fiber 循环 Wait
```

`Reply` 的应答目的地由**服务端自己注册的内容**决定，不取信于线缆；线缆上的 `reply_to` 只作一致性交叉校验，不等即返 `kInvalidArgument`。

**相位规则**：四个注册方法**只在 `Created`** 受理，`Subscribe` / `Publish` / `RequestForResultDirect` / `ServeRequests` / `Reply` **只在 `Running`** 受理。全流程即「注册 → `Start()` → 订阅/收发」。

> ⚠ 框架占用 `cfg.*.request` / `cfg.*.response` 这一命名空间：它与 `RegisterPublishers` / `RegisterSubscribers` 收的普通 topic 处在同一平面，`RegisterSubscribers({"cfg.get.request"})` 与 `RegisterServices({"get"})` 指的是同一条 topic。框架不拦。

**同一个服务名不能同时注册为 client 和 service**（两个方向都会被拒），否则节点会自己收自己的请求。

`DdsNodeConfig::uuid_override` 只为**测试**而设：`correlation_id = "<uuid>#<序号>"`，生产留空即每节点随机一个 uuid；测试填固定值才能断言具体的 `correlation_id`。

### 换 provider

```cpp
#include "transport/io/dds/DdsProviderRegistry.hpp"

// 内建两个："fake"（进程内总线，恒可用）与 "fastdds"（仅在装了 Fast DDS 时存在）
DdsProviderRegistry::RegisterProvider("mine", []{ return std::make_unique<MyProvider>(); });
cfg.provider = "mine";
```

单元测试用 `"fake"` 可全程离线跑，且**同一进程内的多个 `DdsNode` 通过共享总线互通**——不需要真实 DDS 也能测完整的请求-响应链路。

---

## `Message` 的字段归属

一条 `Message` 同时携带 payload 与交互元数据。**元数据分两套，按路径取用，互不干扰**——用错路径的字段不会报错，只会不起作用。

```cpp
struct Message {
  std::vector<uint8_t> payload;   // 应用字节，框架不解读其语义
  std::string topic;              // 操作/通道名（DDS = topic）
  std::string source;             // 来源标识，【由框架填】：UDP = "ip:port"、DDS = topic
  int64_t     timestamp = 0;      // 预留，本库未用

  // ── DDS 路径（DdsCodec 上线缆）──
  MessageKind kind = MessageKind::kOneway;
  std::string correlation_id;     // 配对请求↔应答；【框架盖】
  std::string reply_to;           // 应答回送目的 topic；【框架盖】

  // ── 外部协议路径（SystemCodec 上线缆）──
  FrameType frm_type    = FrameType::kUnknown;  // 【框架盖】
  uint8_t   protocol_id = 0;                    // 【框架盖】取自 ProtocolNodeConfig
  uint8_t   session_id  = 0;                    // 【框架盖】滚动 0–255
  uint16_t  message_id  = 0;                    // 【调用方填】命令码
};
```

**调用方只需要填 `payload` 与 `message_id`**（DDS 路径则是 `payload`）；标了【框架盖】的字段由节点填，手填会被覆盖。

`MessageKind`（DDS）：`kOneway` / `kRequest` / `kReply` / `kFeedback` / `kNotify`。
`FrameType`（外部协议）：`kCommand` / `kResponse` / `kResult` / `kState` / `kHeartbeat`——**枚举值是占位的**，真实对接时改成协议规定的字节值。

> ⚠ `session_id` 是 `uint8`，**滚动复用、不做冲突检测**。在途交互超过 256 条时标识会重复，一条应答将同时投给两个订阅者。协议若有此量级并发，须在关联键里引入更宽的区分字段。

---

## 生命周期与相位规则

传输与节点共用同一套三段式：

```cpp
Start();        // 起内部 fiber 后【即返回】，不等首次连上
Close();        // 【只发信号，不等待】。幂等，任何 fiber 都可调（含节点自己的读循环）
WaitClosed();   // join 全部内部 fiber。返回即【可安全析构】
```

- **`Start()` 不等连上**：首次 connect/bind/open 失败**不算启动失败**，泵会退避后无限重试。真正的启动失败只有"配置非法"（TCP/串口/DDS 返 `kConfiguration` 并**停在 `Created`**，允许改配重试）。
- **`Close()` 不含等待点**，故订阅消费 fiber 可以直接调它关掉自己所属的节点。
- **`WaitClosed()` 不设时限也不返回结果**：`Awaitable::close()` 只保证唤醒等待者，而"可安全释放"要求 fiber 已跑完，只有 `FiberTask::get()` 给得了。
- **`WaitClosed()` 不保证你自己的消费 fiber 已退出**——它只 join 框架内部的 fiber。宿主起的 worker 须自己 `get()`。

**方法的相位要求**：

| 方法 | 只在此相位受理 | 否则返回 |
|---|---|---|
| `DdsNode::RegisterPublishers` / `Subscribers` / `Clients` / `Services` | `Created`（即 `Start()` **之前**） | `kInvalidState` |
| `ProtocolNode::Subscribe` / 三个 `RequestFor*` / `Send` | `Running` | `kClosed` |
| `DdsNode::Subscribe` / `Publish` / `RequestForResultDirect` / `ServeRequests` / `Reply` | `Running` | `kClosed` |

`kClosed` 一码覆盖**未启动 / 关闭中 / 已关闭**三种情形——对调用方而言事实相同：这个节点现在不接活。

> **一条传输可被多个节点共用**，各得全量副本；但**任一节点关闭即终结整条读流**（`Awaitable::close()` 整流传播，有意为之），不支持独立关停——共用的诸节点须一起关。

---

## 错误码

预期失败一律走 `Coro::Result<T>`（`[[nodiscard]]`）**不抛异常**。`Result` 转 `bool` 即成败，`.value()` 取值，`.error()` 取 `std::error_code`。

```cpp
auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
if (!rsp) {
  if (rsp.error() == make_error_code(TransportErrc::kNotAccepted)) { /* 对端没受理 */ }
  else if (rsp.error() == make_error_code(TransportErrc::kTimeout)) { /* 受理了但没出结果 */ }
}
```

| 错误码 | 调用方该怎么理解 |
|---|---|
| `kInvalidArgument` | 参数不合法（`timeout` 非正、`max_attempts` < 1、`reply_to` 交叉校验不符）。**改参数重试。** |
| `kInvalidState` | 相位不对（如 `Start()` 之后再注册 topic）。 |
| `kConfiguration` | 配置非法，或用了未注册的 topic / 服务名。传输**停在 `Created`**，改配可重试。 |
| `kConnection` | 链路层失败。**通常不用处理**——泵会自己重连/重开，这只是诊断事实。 |
| `kClosed` | 节点或传输已关闭 / 未启动。 |
| `kTimeout` | **已受理但没等到结果**；或链路读静默超时。 |
| `kNotAccepted` | **对端始终没有受理**：重发次数耗尽仍无受理帧。与 `kTimeout` 语义相对。 |
| `kFrame` / `kCodec` | 分帧或编解码失败。 |
| `kIo` | 读写故障、线路噪声。 |
| `kResourceExhausted` / `kUnsupported` / `kCancelled` / `kInternal` | 分别为资源耗尽、不支持的操作、已取消、内部不变量破坏。 |

**`kNotAccepted` 与 `kTimeout` 的分野是本库最要紧的一对**：前者说明命令根本没被对端接住（可以安全重发），后者说明对端**正在执行或已执行**（重发有重复执行的风险）。

---

## 扩展：自定义 codec

codec 是**公共扩展点**。实现两个方法即可：

```cpp
#include "transport/codec/ICodec.hpp"

class MyCodec : public transport::ICodec {
 public:
  // 一条 Message → 一帧线缆字节
  Coro::Result<std::vector<uint8_t>> Encode(const transport::Message& msg) override {
    std::vector<uint8_t> out = BuildFrame(msg);
    if (out.empty()) return transport::make_error_code(transport::TransportErrc::kCodec);
    return out;
  }

  // 一段收到的字节 → 0..N 条完整 Message
  Coro::Result<std::vector<transport::Message>> Decode(const uint8_t* data,
                                                       std::size_t len) override {
    buffer_.insert(buffer_.end(), data, data + len);   // 流式：自行维护滚动缓冲
    std::vector<transport::Message> out;
    ScanCompleteFrames(buffer_, out);                  // 扫出整帧，残留留在 buffer_
    return out;                                        // 【扫不出整帧就返回空成功】
  }

 private:
  std::vector<uint8_t> buffer_;
};
```

三条纪律：

1. **`Decode` 扫不出完整帧时返回空成功，不是错误**——字节流本来就会切在半帧处。
2. **返回错误意味着"这段字节坏了"**，节点会静默丢弃并继续读；坏帧的重同步（前移重扫）由 codec 自己负责。
3. **报文式介质（UDP）的 codec 不得跨报文保留状态**——多对端场景下，上一个对端的残留会污染下一个对端的解码。

装配时 `std::make_unique<MyCodec>()` 传给节点构造函数，节点取得所有权。

---

## 扩展：新建一个 node

### 先判断：你真的需要新 node 吗？

| 你的情况 | 该做什么 |
|---|---|
| 线缆格式不同，但交互仍是"发命令 → 等受理 → 等结果" | **只写 codec**，复用 `ProtocolNode` |
| 换了介质（TCP → 串口 → UDP） | **什么都不用写**，换个传输实例即可 |
| 关联键不同（不是 `session_id`+`message_id`+`frm_type`） | **新建 node** |
| 交互步数/终结条件不同（如三段式、需中间反馈、需显式 ACK 才算完成） | **新建 node** |
| 寻址方式不同（如按服务名派生 topic） | **新建 node**（`DdsNode` 即这一类） |

判据是**交互语义**，不是线缆格式，也不是介质。

### 三个基座直接复用，不要重写

| 基座 | 给你什么 | 你不用再操心 |
|---|---|---|
| `NodeBase` | 幂等 `Start()` / 只发信号的 `Close()` / join 式 `WaitClosed()` / `IsRunning()` | 相位机、启动与关闭的竞态（含 `DoStart()` 期间 `Close()` 到来这一格）、幂等性 |
| `Dispatcher<T, Fields...>` | 按键的多字段部分匹配、`kAny` 通配、一条消息投给全部匹配者、move-only `Ticket` | 挂起表、恰好一次完成、关闭时唤醒全部等待者 |
| `ITransport` | 四种介质同一个契约 | 连接管理、重连/重开、读写队列 |

**这三样都是协议无关的**，新协议不需要、也不应该改动它们。`NodeBase` 本身不 include 任何协议或消息类型。

### 你要写的四件

#### ① 关联键：字段 + 提取函数

选出"哪几个字段决定一条入站消息该投给谁"，实例化 `Dispatcher`：

```cpp
// 例：某协议按 (设备地址, 事务号, 操作码) 关联
using MyDispatcher = Dispatcher<Message, std::uint16_t, std::uint32_t, std::uint8_t>;

MyDispatcher dispatcher{[](const Message& m) {           // KeyOf：给出各字段的具体值
  return std::make_tuple(DeviceOf(m), TxnOf(m), OpOf(m));
}};
```

字段顺序即键的顺序；订阅时不参与匹配的字段填 `std::nullopt`（即 `kAny`）。

#### ② 具名键工厂

别让调用方手拼 `std::optional`，给出语义化的工厂（对照 `ResponseTo` / `FrameOf` / `AnyOfType`）：

```cpp
MyDispatcher::Key ReplyTo(const Message& req) {
  return {DeviceOf(req), TxnOf(req), kReplyOp};
}
MyDispatcher::Key AnyFrom(std::uint16_t device) {
  return {device, std::nullopt, std::nullopt};   // 后两字段通配
}
```

#### ③ 三个生命周期钩子

照抄这个形状即可——**协议无关的部分它们已经替你处理了**：

```cpp
Coro::Result<void> MyNode::DoStart() {
  // 配置校验放在【本钩子开头】，不单设 ValidateConfig()。
  if (!ConfigOk(config_)) return make_error_code(TransportErrc::kConfiguration);

  rx_ = transport_.AsyncRead()->shared();   // 取本节点自己的读订阅
  SpawnReadLoop();                          // spawn 读-分发循环
  return {};                                // 返回成功后由【基类】置 Running
}

Coro::Result<void> MyNode::DoClose() {      // 【只发信号，一个等待点都不许有】
  if (rx_) {
    rx_->close(make_error_code(TransportErrc::kClosed));
    rx_->channel()->discard_pending();
  }
  dispatcher_.CloseAll(make_error_code(TransportErrc::kClosed));
  return {};
}

void MyNode::DoJoin() {                     // 等自己 spawn 的每一条 fiber 真正退出
  if (read_task_) (void)read_task_->get();
}
```

**`DoStart()` 失败时必须保证一条 fiber 都没 spawn**——基类会退回 `Created` 让宿主改配重试。

#### ④ 读-分发循环 + 交互方法

读循环是固定形状，跟着抄：

```cpp
void MyNode::SpawnReadLoop() {
  read_task_ = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([this] {
    while (true) {
      auto datagram = Coro::await(rx_);
      if (!datagram) break;                 // 我方 Close，或传输终结 —— 都该收敛
      DecodeAndDispatch(std::move(datagram).value());
    }
    (void)Close();   // 【无条件】调公开的 Close()：我方关闭时是幂等空操作，
                     // 传输终结时即自终。Close 不含等待点，故在本 fiber 内调用安全。
  }));
}

void MyNode::DecodeAndDispatch(Datagram d) {
  auto decoded = codec_->Decode(d.bytes.data(), d.bytes.size());
  if (!decoded) return;                     // 坏帧：静默丢弃，继续读
  for (const auto& m : decoded.value()) {
    (void)dispatcher_.Dispatch(m);          // 返 0 = 无人认领 → 静默丢弃
  }
}
```

交互方法则是你的协议语义所在。以"发命令 → 等回执"为例：

```cpp
Coro::Result<Message> MyNode::Invoke(Message req, RetryPolicy retry) {
  if (!IsRunning()) return make_error_code(TransportErrc::kClosed);      // 相位判定
  if (retry.timeout <= 0ms || retry.max_attempts < 1)
    return make_error_code(TransportErrc::kInvalidArgument);

  Stamp(req);                                          // 盖上协议字段
  auto ticket = dispatcher_.Subscribe(ReplyTo(req));   // 【先登记订阅、再发出请求】

  for (int attempt = 0; attempt < retry.max_attempts; ++attempt) {
    if (auto sent = EncodeAndWrite(req); !sent) return sent.error();
    auto reply = ticket.Wait(retry.timeout);
    if (reply) return reply;                           // 命中即终结
    if (reply.error() == make_error_code(TransportErrc::kClosed))
      return reply.error();                            // 节点关闭：立刻退出，别再重发
  }
  return make_error_code(TransportErrc::kNotAccepted);
}
```

### 必须守住的纪律

这几条是踩过的坑，新 node 一条都不能漏：

1. **先登记订阅，再发出请求。** 反过来则回应可能先于订阅到达而被丢弃。
2. **`DoClose()` 里不许有任何等待点。** 它可能在节点**自己的读循环 fiber** 内被调用（读循环末尾那句 `Close()`），有等待点就会自己等自己。等待属于 `DoJoin()`。
3. **析构函数必须在「本类」函数体内 `Close()` + `WaitClosed()`。**
   ```cpp
   MyNode::~MyNode() { (void)Close(); WaitClosed(); }
   ```
   不能指望 `NodeBase` 的析构——那时子类已析构完毕、虚派发已退回基类（纯虚 ⇒ UB）。
4. **读循环结束后无条件调公开的 `Close()`**，让"传输终结"能自动收敛节点。
5. **相位判定用 `IsRunning()`**，未启动/关闭中/已关闭一律返 `kClosed`（三者对调用方是同一件事：这个节点现在不接活）。需要区分 `Created` 与 `Closed` 时（如"只在 `Start()` 之前受理"的注册接口）才用 protected 的 `CurrentLifecycle()`。
6. **节点不管传输的生命周期**：`transport_` 是**借用**的引用，宿主创建、`Start()`、`Close()`，节点绝不碰。宿主须保证传输寿命长于节点。
7. **不要往 `NodeBase` 里加协议类型。** 协议语义内联在你自己的 node 里，不下沉为框架级共享 policy——这是本库刻意不设"共享交互引擎"的原因。

### 装到框架里

新 node 只需继承 `NodeBase`，无需注册到任何地方；宿主直接构造使用。测试可用 `FakeDdsProvider` 那种进程内替身，或直接拿两个节点对接同一条 loopback 传输。

`ProtocolNode`（`src/node/ProtocolNode.cpp`，~400 行）与 `DdsNode`（`src/node/DdsNode.cpp`）是两个完整样板：前者是"外部协议 + 三段交互"，后者是"服务名派生 topic + 两段式 `correlation_id`"。**两者共用的只有 `NodeBase` 与 `Dispatcher`，协议语义各自内联**——这正是新增协议时该复制的关系，而不是去抽一个公共交互层。

---

## 内部传输契约（`ITransport`）

介质无关的内部缝，**非用户 API**——宿主只需创建、`Start()`、`Close()` / `WaitClosed()`。

```cpp
class ITransport {
 public:
  virtual Coro::Result<void> Start()      = 0;   // 起内部管理泵后即返回
  virtual Coro::Result<void> Close()      = 0;   // 只发信号，不等待收敛。幂等
  virtual void               WaitClosed() = 0;   // join 全部内部工作单元

  virtual std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead()          = 0;
  virtual Coro::Result<void>                        AsyncWrite(Datagram) = 0;

  virtual std::error_code LastError()        const = 0;
  virtual LinkState       CurrentLinkState() const = 0;
};
```

**读写刻意不对称。** 读是"数据什么时候来"，只能交出等待器句柄，超时、取消与是否 `shared()` 扇出由调用方自理——不设单读守卫，多个消费者直接 await 同一句柄是抢占关系。写是"把这份数据发到那里去"，调用方给完即返回。

**写为彻底的 fire-and-forget**：`AsyncWrite` 只判生命周期与入队，返回成功仅表示已受理；目的地能否解析、socket 是否写成一律不回传，只落 `LastError()`。链路不可用时数据留在内部队列等待恢复，不拒绝、不丢弃。**由此不提供背压。**

`Datagram{bytes, peer}` 读写共用：读到的 `peer` 是发送方，写出的 `peer` 是目的地（`Endpoint::Default()` 表示"发往本传输配置的默认对端"，故传输无关的调用方恒可传它）。

`Datagram` 的读写不对称与 fire-and-forget 语义见上；生命周期三段式见[生命周期与相位规则](#生命周期与相位规则)。

---

## 构建

**两套构建并存，产物等价**：单一 `transport` 静态库 + 单一 `transport_tests` 可执行文件。C++17，目标平台 Linux。

```bash
git submodule update --init --recursive third_party/AsyncTask
```

### CMake

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

显式禁用 DDS：`cmake -S . -B build -DCMAKE_DISABLE_FIND_PACKAGE_fastdds=ON`

### qmake

推荐 shadow build（qmake 默认 in-source，会把中间产物撒进源码树）：

```bash
mkdir build-qmake && cd build-qmake
qmake ../transport.pro && make -j$(nproc)
./bin/transport_tests
```

可选开关：

```bash
qmake FASTDDS_ROOT=/opt/fastdds ../transport.pro   # 换 Fast DDS 探测前缀（默认 /usr/local）
qmake CONFIG+=no_fastdds ../transport.pro          # 强制不编 FastDdsProvider
qmake CONFIG+=debug ../transport.pro               # Debug（qmake 默认 release）
```

工程文件收在 `qmake/` 下，根 `transport.pro` 是 Qt Creator 的入口。

> ⚠ **源文件清单有两份**（`CMakeLists.txt` 与 `qmake/*/*.pro`），增删 `.cpp` 须同时改。两边的清单顺序与注释逐字一致，便于肉眼 diff 发现漂移。

### 前置依赖

- **Qt5**（5.12+；Core / Network / SerialPort，如 `libqt5serialport5-dev`）
- **AsyncTask**（boost.fiber 协程运行时，`third_party/AsyncTask` 子模块）+ 已编译 boost `fiber` / `context` / `thread` / `chrono`。子模块未初始化时构建直接报错
- **Fast DDS 3.6.1**（**唯一可选**外部依赖）：未装时自动跳过 `FastDdsProvider`，其余能力照常构建可测（用例数 236 → 225）；装后自动启用并定义 `TRANSPORT_HAS_FASTDDS`
- **GoogleTest** 已 vendored 在 `third_party/`，无需自备

### 链接到你的工程

```cmake
add_subdirectory(path/to/transport)
target_link_libraries(your_app PRIVATE transport)
```

`transport` 已 PUBLIC 传递 AsyncTask、Qt5 与 boost 的用法要求，无需重复声明。

---

## 关键约束

- **三层解耦**：传输不依赖逻辑消息或协议语义；codec 是公共扩展点；node 组合三者。〔RT_IN_INTERFACE_001/002〕
- **AsyncTask 强制运行时**：不设独立业务调度体系；M:N 协作式，同一节点的状态与关联串行，不同节点可并行。〔RT_DESIGN_002、RT_CORO_RUNTIME〕
- **无共享交互引擎**：协议语义归各 node，公共只复用协议无关的 `Dispatcher` 与生命周期基类。〔RT_DESIGN_003、RT_NODE_003〕
- **不抛异常**：预期失败用 `Coro::Result<T>`（`[[nodiscard]]`）+ 机器可判别的 `TransportErrc`。〔RT_ERROR_001/002/003〕
- **节点不管传输的生命周期**：宿主创建、启动、关闭传输，节点按引用借用。
- **框架不提供可观测性**：内部丢弃（队列满丢最旧 / 坏帧 / 迟到·无匹配响应）完全静默，排障须由宿主在 codec 或订阅侧自行加日志。
- **底层回调不碰节点状态**：Qt I/O 与 DDS listener 回调安全转交节点执行域，不在回调线程执行业务处理。〔RT_NODE_004〕

---

## 文档

- **需求规格说明书（SRS）**：[`docs/需求规格说明书-协程原生.md`](docs/需求规格说明书-协程原生.md) —— 可观察/可验收行为，标识前缀 `RT_`
- **软件设计说明（SDD, GJB438C）**：[`docs/软件设计说明-GJB438C.md`](docs/软件设计说明-GJB438C.md) —— 部件、接口、详细设计与追溯矩阵
- **架构决策记录（ADR）**：[`docs/adr/`](docs/adr/) —— 每条决策的依据、否决的备选与明确接受的代价
- **项目术语（单一权威）**：[`CONTEXT.md`](CONTEXT.md)
- **编码规范**：[`CODING_STANDARDS.md`](CODING_STANDARDS.md)
- **变更日志**：[`CHANGELOG.md`](CHANGELOG.md)
