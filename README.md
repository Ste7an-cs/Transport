# transport — C++ 通信中间件

一个 C++17 通信中间件库,把**传输**、**编解码**、**交互模式**三层彻底解耦:

- **Transport（纯字节管道）** —— 跨 TCP / UDP / 串口 / DDS 搬运原始字节,不知 Message/格式/交互。
- **ICodec（线缆格式）** —— 在收发边界把 `Message` ↔ 线缆字节(分帧 + 序列化 + 承载交互元数据)。
- **Comm（交互节点）** —— 在前两层之上实现请求-应答 / 结果反馈 / 发布-订阅 / 外部协议等交互模式;**用户继承节点即获得通信能力**。线程模型经 `IExecutor` 可换。

> **非目标：** 不解析/解释 payload 语义,不是消息代理或路由守护进程。

> **架构基线（0.2.0 开发线）：** 当前为三层解耦架构,取代 0.1.0 的富 `ITransport` + `TransportCore` + 三模式接收 + `IFramer` + topic 路由 + `TransportFactory`。`v0.1.0` 标签保留旧实现;0.2.0 与 0.1.0 **不 API 兼容**。

---

## 三层一览

| 层 | 接口 | 内置实现 |
|---|---|---|
| **Transport** 纯字节管道 | `ITransport`(`Open/Close/Send(bytes[,Endpoint])/OnBytes/OnConnect/OnDisconnect`) | `UdpTransport`、`TcpClientTransport`/`TcpConnection`/`TcpServerTransport`、`SerialTransport`、`DdsTransport`(`IDdsTransport`+`Subscribe`,经 `IDdsProvider`/Fake/FastDDS) |
| **ICodec** 线缆格式 | `ICodec`(`Encode(Message)→bytes` / `Decode(bytes)→0..N Message`) | `SystemCodec`(外部协议帧)、`DdsCodec`(无状态,带交互元数据)、`LengthFieldCodec`、`DatagramCodec` |
| **Comm** 交互节点 | `IExecutor`(线程模型缝)+ 节点基类 | `ThreadExecutor`;`CommNode`(通用 req-resp)、`DdsNode`(DDS pub-sub + 多路 req-resp)、`ProtocolNode`(外部协议栈) |

> **用户面定位:** **Comm 节点是编程主入口** —— 继承 `CommNode`/`DdsNode`/`ProtocolNode` 重写交互钩子。`ITransport` **不是直接收发面**(对比 0.1.0),而是 ① **装配缝**(你实例化一个具体 transport 注入节点构造,换协议只换 transport)+ ② **裸字节逃生口**(只要字节管道时直接持 `shared_ptr<ITransport>` 用 `OnBytes`/`Send`,如下方 UDP 例)。它保持公共干净接口,是分层、可测(`FakeTransport`)、可替换的支点。

- **统一寻址 `Endpoint`**:发布即 `Send(msg, Endpoint::Topic(t))`、UDP 寻址即 `Send(bytes, Endpoint::Net(ip,port))`,基类句柄即可寻址,无形态不一的 `SendTo`。
- **不抛异常**:所有可失败操作返回 `Result<T>`(标 `[[nodiscard]]`,忽略错误返回值即编译期告警),错误串前缀分类 `timeout:`/`conn:`/`codec:`/`frame:`/`io:`/`config:`。
- **可换线程模型**:`IExecutor` 缝使同一交互逻辑在确定性 `InlineExecutor`(测试)与真实 `ThreadExecutor` 下都跑,未来 `CoroExecutor`(自研协程)即插即换。
- **接口层零第三方依赖**:`include/transport/` 只含纯接口 + 数据结构;asio / Fast DDS / termios 关在实现层。

---

## 构建

```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

**离线自包含**:Standalone Asio 1.30.2、nlohmann/json 3.11.3、GoogleTest 1.14.0 已 vendored 到 `third_party/`,CMake **不联网拉取**,`git clone` 后即可构建。C++17。

**唯一可选外部依赖 Fast DDS 2.13.x**:未装时自动跳过 `FastDdsProvider` 及其真实互通测试(DDS 逻辑仍可用 `FakeDdsProvider` 全测);装后 `find_package` 自动启用。详见 [`third_party/README.md`](third_party/README.md)。

---

## 用法

> 所有传输/节点须以 `std::shared_ptr` 持有。

### 通用请求-应答(`CommNode`,适用 TCP/UDP/串口)

```cpp
#include "transport/comm/CommNode.hpp"
#include "transport/codec/DdsCodec.hpp"
#include "transport/tcp/TcpClientTransport.hpp"
using namespace transport;

Message Msg(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

// 继承 CommNode,重写交互钩子获得通信能力
class MyNode : public CommNode {
 public:
  using CommNode::CommNode;
  void OnMessage(const Message& m) override { /* 单向消息 */ }
  void OnRequest(const Message& req, Responder r) override {
    auto out = req.payload; out.push_back(0xFF);
    (void)r.Reply(Msg(out));                       // 服务端应答
  }
};

TcpClientConfig cfg; cfg.host = "127.0.0.1"; cfg.port = 9000; cfg.auto_reconnect = true;
auto node = std::make_shared<MyNode>(
    std::make_shared<TcpClientTransport>(cfg),
    std::make_unique<DdsCodec>());                // 通用线缆格式(带 kind/correlation_id)
(void)node->Open();

node->Send(Msg({1, 2, 3}));                       // 单向
// 请求-应答(回调)
(void)node->Request(Msg({5}),
    [](Result<Message> r) { if (r) use(r.value.payload); }, /*timeout_ms=*/1000);
// 请求-应答(future)
auto fut = node->Request(Msg({7}), /*timeout_ms=*/1000);
Result<Message> r = fut.get();
node->Close();
```

### DDS 发布-订阅 + 多路请求-应答(`DdsNode`)

```cpp
#include "transport/comm/DdsNode.hpp"
#include "transport/dds/DdsTransport.hpp"
using namespace transport;

class Svc : public DdsNode {
 public:
  using DdsNode::DdsNode;
  void OnMessage(const Message& m) override { /* m.topic = 来源 topic */ }
  void OnRequest(const Message& req, Responder r) override {
    Message rep; rep.payload = compute(req.payload);
    (void)r.Reply(rep);                               // 经 reply_to 精确回送发起方
  }
};

DdsConfig cfg; cfg.domain_id = 0; cfg.provider = "fastdds";   // 或默认 "fake"(进程内)
auto node = std::make_shared<Svc>(
    std::make_shared<DdsTransport>(cfg), /*inbox_topic=*/"A_in");  // 默认 DdsCodec
(void)node->Open();                                   // 自动订阅自身 inbox
(void)node->Subscribe("telemetry");                   // 订阅(可多个)
Message pub; pub.payload = {1, 2, 3};
(void)node->Send(pub, Endpoint::Topic("telemetry"));  // 发布
// 向某服务 topic 发请求,应答经本节点 inbox 回来
Message req; req.payload = {9};
(void)node->Request(req,
    [](Result<Message> r) { /* ... */ }, 1000, Endpoint::Topic("svc"));
```

### 对接外部系统(`ProtocolNode` + `SystemCodec` 协议帧)

帧格式 `[head_flag:4=AA BB CC DD][frm_type:1][protocol_id:1][session_id:1][reserve:4][crc:2][frm_len:2][message_id:2|payload]`(小端);CRC 经 `CrcFn` 注入,匹配键 (session_id, message_id)。

```cpp
#include "transport/comm/ProtocolNode.hpp"
#include "transport/serial/SerialTransport.hpp"
using namespace transport;

class Link : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;
  void OnCommand(const Message& cmd, Responder r) override {   // 接收角色
    (void)r.Response(ack(cmd.payload));      // 即时回应
    (void)r.Result(compute(cmd.payload));    // 最终结果
  }
  void OnHeartbeat(const Message&) override {}
};

SerialConfig sc; sc.device = "/dev/ttyUSB0"; sc.baud_rate = 115200;
ProtocolConfig pc; pc.protocol_id = 1; pc.response_timeout_ms = 200;
pc.max_retries = 3; pc.heartbeat_interval_ms = 1000;
auto link = std::make_shared<Link>(
    std::make_shared<SerialTransport>(sc), /*codec=*/nullptr, pc);  // 默认 SystemCodec
(void)link->Open();

// 5 种发送交互模式(发起角色):
(void)link->SendNoResponse({0x01});                                  // 不需回应
(void)link->Request({0x02}, [](Result<Message> r){/*RESPONSE*/});    // 需回应,超时重发≤3
(void)link->RequestWithResult({0x03}, [](Result<Message> r){/*RESULT*/});  // 需结果
(void)link->RequestNeedFeedback({0x04},
    [](Result<Message> r){/*RESPONSE*/}, [](Result<Message> r){/*RESULT,自动回 ack*/});
uint32_t h = link->StartRepeating({0x05}, /*interval_ms=*/500);      // 定时发 STATE
link->StopRepeating(h);
```

> **接入前需替换的外部常量**:`SystemCodec` 的 `CrcFn`(真实 CRC16 算法,构造注入)与 `FrameType` 六类的真实字节值(枚举占位)。两端一致即可。

### TCP 服务端(每连接独立节点)

```cpp
#include "transport/tcp/TcpServerTransport.hpp"
using namespace transport;

TcpServerConfig cfg; cfg.bind_addr = "0.0.0.0"; cfg.port = 9000;
auto server = std::make_shared<TcpServerTransport>(cfg);
server->OnAccept([](std::shared_ptr<ITransport> conn) {
  // 每个被接受连接是独立 ITransport:在其上构造一个节点收发
  auto node = std::make_shared<MyNode>(conn, std::make_unique<DdsCodec>());
  (void)node->Open();
  keep_alive(node);                  // 持有 shared_ptr,勿让其析构
});
(void)server->Open();
```

### UDP(单播 / 组播 / 广播)

```cpp
#include "transport/udp/UdpTransport.hpp"
using namespace transport;

UdpConfig cfg; cfg.mode = UdpMode::kUnicast;
cfg.local_port = 5000; cfg.remote_addr = "127.0.0.1"; cfg.remote_port = 6000;
auto udp = std::make_shared<UdpTransport>(cfg);          // 纯字节管道(也可套节点)
udp->OnBytes([](Result<std::vector<uint8_t>> b, const std::string& from) { /* ... */ });
(void)udp->Open();
(void)udp->Send({1, 2, 3});                              // 默认目的地
(void)udp->Send({4, 5, 6}, Endpoint::Net("10.0.0.7", 7000));  // 运行期寻址
```

`Result<T>` 用法:`if (r) use(r.value); else log(r.error);`(`operator bool` == `r.ok`)。

---

## 状态(0.2.0 开发线)

- [x] **Transport 层**:UDP、TCP(client/connection/server)、串口、DDS(纯字节 pub-sub + provider 抽象 / Fake / 可选 FastDDS)
- [x] **Codec 层**:`SystemCodec`(外部协议帧)、`DdsCodec`、`LengthFieldCodec`、`DatagramCodec`
- [x] **Comm 层**:`IExecutor`/`ThreadExecutor`、`CommNode`(通用 req-resp)、`DdsNode`(DDS pub-sub + 多路 req-resp)、`ProtocolNode`(外部协议栈:5 模式 + 重发 + repeating + 心跳 + 双角色)
- [ ] 后续:自研协程 `CoroExecutor`、外部协议真实 frm_type/CRC 接入、正式发布 0.2.0

---

## 文档

权威参考(基于当前实现汇总):

- **需求规格说明书(SRS)**:[`docs/需求规格说明书.md`](docs/需求规格说明书.md) —— 功能需求 FR-1~13 + 非功能需求 + 约束。
- **设计说明书(SDD)**:[`docs/设计说明书.md`](docs/设计说明书.md) —— 三层架构 / UML / 协议帧格式 / 数据流时序 / 执行器与并发模型 / 设计依据。
- **变更日志**:[`CHANGELOG.md`](CHANGELOG.md) —— 按 PR/里程碑汇总(含破坏性变更标注)。
- 过程历史(逐特性 spec / plan,备查):`docs/superpowers/specs/`、`docs/superpowers/plans/`。

### 关键约束(详见 SRS/SDD)

- **三层解耦**:Transport 不依赖 Message/ICodec;ICodec 不依赖具体 transport;Comm 节点只依赖 `ITransport`/`ICodec`/`IExecutor` 接口(+ 同层默认 `SystemCodec`/`ThreadExecutor`)。〔SRS NFR-2、SDD §2.2〕
- **不抛异常**:`Result<T>`/`Status`(`[[nodiscard]]`),错误前缀分类。〔SRS FR-13、SDD §11〕
- **回调式交付 + 单 worker 串行**:Transport 经 `OnBytes` 回调交付(io 线程,非阻塞);Comm 层 io 线程内联 `Decode` → `executor.Post` → 单 worker 串行业务回调,背压在 `Post`。〔SRS FR-1.2/NFR-3、SDD §10〕
- **线程模型可换**:经 `IExecutor` 缝换线程/协程/确定性测试而不改节点逻辑。〔SRS FR-4/NFR-4、SDD §7.1〕
- **统一寻址 `Endpoint`**:发布=`Send(Topic)`、UDP 寻址=`Send(Net)`,基类句柄即可寻址。〔SRS FR-2、SDD §4.3〕
- **节点以 shared_ptr 持有**:`enable_shared_from_this` + weak_ptr 保活;`Close` 先终结挂起/取消定时器再停执行器再关传输。〔SRS NFR-3、SDD §10〕
- **离线自包含构建**:第三方依赖 vendored;Fast DDS 为唯一可选外部依赖,`find_package` 自动探测。〔SRS NFR-7/NFR-8、SDD §12〕
- **外部协议常量注入**:`SystemCodec` 的 CRC(`CrcFn` 注入)与 `frm_type` 真值(枚举占位)接入前替换,不改结构。〔SRS FR-12、SDD §8〕
