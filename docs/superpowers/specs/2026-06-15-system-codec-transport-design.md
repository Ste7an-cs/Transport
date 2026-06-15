# 底层通信框架重构:Transport + ICodec 两层解耦 — 设计 spec

> **大版本重构(0.2.0,破坏性),分阶段推进。** 目标三层架构:
> **Transport(纯字节管道)/ ICodec(线缆格式)/ System(用户继承的交互模式基类)**。
> **本轮(第一阶段)只实现底层两层 `Transport` + `ICodec`,且两层尽量解耦、各自独立可测**;
> 上层 `System`(消息收发编排 + 三模式交付 + 请求-应答/结果反馈/订阅等交互模式)见 §8 蓝图,**本轮不实现**。
> 本轮**只覆盖点对点传输**(TCP 客户端 / 串口 / UDP);DDS、TCP 服务端 留作后续。
> 0.1.0 已打标签 `v0.1.0` 留存,不受影响。

**本轮 Goal:** 把"搬字节"与"线缆格式"彻底拆成两个互不依赖的底层组件:`Transport` 只收发裸字节;`ICodec` 只做 `Message ↔ 线缆字节`(分帧 + 序列化 + 承载交互元数据)。二者解耦,均可单独构造与测试;并让 `Message`/`ICodec` 已携带未来 `System` 实现交互模式所需的元数据(`kind`/`correlation_id`)。

**Tech Stack:** C++17;Standalone Asio(已 vendored);GoogleTest 1.14(已 vendored);不抛异常。

---

## 1. 动机

当前(0.1.0)富 `ITransport` 一个接口同时管:I/O、三模式接收、`ICodec` 挂载、按 topic 选 codec、`Send(Message)`、topic 信封分帧,且交互模式(请求-应答)只在 DDS 上。耦合重、模式无法跨传输复用。

目标三层(自底向上):

```
┌─────────────────────────────────────────────┐
│ System(后续阶段:用户继承的交互模式基类)     │  §8 蓝图,本轮不做
├─────────────────────────────────────────────┤
│ ICodec(本轮:线缆格式 = 分帧+序列化+交互元数据) │  ← 本轮
├─────────────────────────────────────────────┤
│ Transport(本轮:纯字节管道)                   │  ← 本轮
└─────────────────────────────────────────────┘
```

**本轮解耦原则:** `Transport` 只依赖 `Result`/`Endpoint` + 裸字节,**不知道** `Message`/`ICodec`;`ICodec` 只依赖 `Result`/`Message`,**不知道** `Transport`。两者唯一交集是共享值类型,彼此可独立替换、独立测试。

---

## 2. 共享数据类型

### 2.1 `Result<T>` / `Status`(不变)
`{ T value; bool ok; std::string error; }`,`Success`/`Fail("prefix: msg")`,`explicit operator bool`,`[[nodiscard]]`。前缀:`timeout:`/`conn:`/`codec:`/`frame:`/`io:`/`config:`。

### 2.2 `Message`(本轮定型,含交互元数据)
```cpp
enum class MessageKind {
  kOneway,    // 单向(无需应答)
  kRequest,   // 请求(期待应答/反馈)
  kReply,     // 终结应答(请求-应答的应答,或请求-结果反馈的最终结果)
  kFeedback,  // 中间结果反馈(可多次,非终结)
  kNotify,    // 订阅通知(主动推送)
};

struct Message {
  std::vector<uint8_t> payload;       // 应用字节
  std::string topic;                  // 操作/通道名(如 "calc");否则空
  std::string source;                 // 来源标识;本轮由调用方留空,后续 System 从 transport from 填
  int64_t     timestamp = 0;          // 本轮留 0,后续 System 填(微秒)
  MessageKind kind = MessageKind::kOneway;
  std::string correlation_id;         // 配对请求↔应答/反馈;非请求流量为空
};
```
> `kind`/`correlation_id` 本轮由 `ICodec` 负责编进/解出线缆(见 §4);其**消费**(配对、分发)是后续 `System` 的职责。`source`/`timestamp` 本轮不在底层填(留给 System)。

### 2.3 `Endpoint`(不变)
`Kind{kDefault,kNet,kTopic}` + `Net/Topic/Default`。本轮点对点主要用 `kDefault`(默认目的地)与 `kNet`(UDP 运行期寻址)。

---

## 3. 第一层:`Transport` 纯字节管道(本轮)

```cpp
// include/transport/ITransport.hpp
class ITransport {
 public:
  virtual ~ITransport() = default;

  // bytes = 收到的字节(失败时为错误);from = 来源标识字符串
  //   (UDP/TCP 为 "ip:port",串口为设备路径),失败时为空。
  using BytesCallback =
      std::function<void(Result<std::vector<uint8_t>> bytes, const std::string& from)>;

  virtual Status Open() = 0;
  virtual void   Close() = 0;
  virtual bool   IsOpen() const = 0;

  virtual Status Send(const std::vector<uint8_t>& bytes) = 0;
  virtual Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) = 0;

  // 收裸字节(io 线程,经 strand 串行化):
  //   流式(TCP/串口)= 本次 read 到的切片;报文式(UDP)= 一个完整 datagram。
  virtual void OnBytes(BytesCallback cb) = 0;

  virtual void OnConnect(std::function<void()> cb) = 0;                       // 建连(含重连)
  virtual void OnDisconnect(std::function<void(const std::string&)> cb) = 0; // 断开
};
```

**要点:**
- **不依赖** `Message`/`ICodec`/topic/分帧/交互模式;只进出 `std::vector<uint8_t>` + `from` 字符串。
- 各回调单消费者(注册一次);回调在 transport io 线程 + strand 上,串行不并发。
- 连接生命周期(connect/超时/指数退避重连/断开)是 transport I/O 职责;重连透明恢复字节流并触发 `OnConnect`。

**`OnBytes` 语义(何时被谁调用):**
- `OnBytes(cb)` 仅**注册**回调(单消费者,后注册覆盖先注册);本身不触发任何事,应在 `Open()` **之前**注册以免漏掉最早到达的数据。
- 回调由 **transport 自己的 io 线程**在**异步读完成**时调用,绑定在 `strand_` 上 → 多次回调**严格串行、绝不并发**;回调在 io 线程执行,**必须非阻塞**,且**不可在其中 `Close()` 本对象**(自我 join 死锁)。
- **每次回调对应**:报文式(UDP)= 一个完整 datagram;流式(TCP/串口)= 本次 `read` 到的字节切片(可能半条/一条/多条粘连)。**「一次回调 ≠ 一条消息」**,切帧由上层 `ICodec` 负责。
- **`from`**:成功时为来源标识(UDP=发送方 `"ip:port"`,逐包可变;TCP=对端 `"ip:port"`;串口=设备路径),失败时为空。
- **错误分流**:UDP 的收/发 I/O 错误经 `OnBytes` 投 `Result::Fail`(单包出错不致命,继续监听);TCP/串口的连接级读写错误改走 `OnDisconnect`(其 `OnBytes` 只会收到成功字节)。`OnConnect` 在建连(含重连成功)时调,UDP/串口在 `Open()` 成功后调一次。

**内置实现(本轮):** `TcpClientTransport` / `SerialTransport` / `UdpTransport`,由现有 `TcpClientImpl` / `SerialImpl` / `UdpImpl` 剥离 `TransportCore`/codec/framing 改造——接收路径改吐裸字节 + `from`,删 `SetCodec`/`Send(Message)`/topic/三模式接收。

---

## 4. 第二层:`ICodec` 线缆格式(本轮)

```cpp
// include/transport/ICodec.hpp
class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发:一条消息(含 kind/correlation_id/topic/payload)→ 一段线缆字节。一对一。
  virtual Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  // 收:喂入一段字节切片 → 切出 0..N 条完整消息(内部维护滚动缓冲),
  //   还原每条的 kind/correlation_id/topic/payload。
  //   流式:可能 0 条(半包)/1 条/多条(粘包);报文式:通常 1 条。
  //   解析错误(坏帧头/越界)→ Fail("frame: ..." / "codec: ...")。
  virtual Result<std::vector<Message>> Decode(const uint8_t* data,
                                              std::size_t len) = 0;
};
```

**要点:**
- **不依赖** `Transport`;纯 `Message ↔ bytes` 变换。
- **有状态**:`Decode` 持滚动缓冲 → 未来每个 `System`(连接)一个 codec 实例,不可跨连接共享。本轮单独测时同样按"喂切片→出整条"语义。
- 由(未来)transport io 线程单线程喂,故 **无需线程安全**。
- `correlation_id`/`kind` 必须传到对端才能配对,**故归 codec 上线缆**;codec 只搬运不解释其语义。

**内置实现(本轮):**
- **`SystemCodec`(默认线缆格式,为未来交互模式备齐元数据):**
  `[frame_len:4 BE][kind:1][corr_len:2 BE][corr_id][topic_len:2 BE][topic][payload...]`
  `frame_len` 不含自身 4 字节;流式按 `frame_len` 切帧,报文式整段一条。往返保 `kind`/`correlation_id`/`topic`/`payload`。
- **`LengthFieldCodec`:** 吸收现 `LengthFieldFramer`+`FrameAssembler`(固定 header+长度字段切帧),body→`payload`;不带元数据 → `kind=kOneway`、`correlation_id` 空。
- **`DatagramCodec`:** 报文直通(UDP 无分帧);整段一条,`kind=kOneway`。
- 用户对接外部协议:继承 `ICodec`,把对方协议字段映射到 `Message` 各字段。

**被删除/合并:** `IFramer`、`LengthFieldFramer`、`FrameAssembler`、`TopicEnvelope`、`StreamSend` → 逻辑并入各 `ICodec` 实现,不再是 transport 层概念。

---

## 5. 两层如何协作(本轮不固化为代码,仅约定契约)

本轮**不**提供把 Transport 与 ICodec 接起来的编排件(那是 System 的活)。但二者契约必须能无缝组合,后续 System 将这样用:
```
发: System 将 Message → codec.Encode → transport.Send(bytes[,to])
收: transport.OnBytes(bytes,from) → codec.Decode(bytes) → 0..N 条 Message
     → System 填 source=from / timestamp,再按 kind 分发(后续阶段)
```
本轮以一个**组合冒烟测试**验证该契约(见 §7):手动把某 transport 收到的字节喂给 codec,断言还原出的 `Message` 等于发送端 `Encode` 的输入——证明两层可拼,但不产出 System 类。

---

## 6. 错误处理(本轮)
- 全程 `Result`/`Status`,不抛异常,前缀分类不变。
- `transport`:连接错误经 `OnBytes` 投 `Fail` / `OnDisconnect` 上报;`Send` 写失败返回/经回调上报(沿用现有实现语义)。
- `codec`:坏帧 → `Decode` 返回 `frame:`;解码单条失败 → `codec:`。**如何处置(致命关闭 vs 丢弃续收)是 System 的策略,本轮 codec 只如实返回错误。**

---

## 7. 测试策略(本轮)
- **`ICodec` 单元(核心):**
  - `SystemCodec` 编解码往返:覆盖每种 `kind` + 非空/空 `correlation_id` + `topic` + `payload`;流式半包/粘包/跨切片/超长/坏帧头/`frame_len` 边界。
  - `LengthFieldCodec` 表驱动(迁移现 framer 用例:半包/粘包/跨读/超长/非法头/大小端/`length_size∈{2,4,8}`)。
  - `DatagramCodec` 一段一条往返。
- **`Transport` 单元/回环:** TCP 客户端↔回环 server(测试内裸 asio acceptor)收发裸字节 + 重连;串口(`openpty`)裸字节;UDP(回环 + `Endpoint::Net`)裸 datagram;`OnBytes` 的 `from` 正确、`Close` 幂等、无自我 join 死锁。
- **两层组合冒烟(§5):** transport 收到的字节喂 codec,断言还原 `Message` == 发送端 `Encode` 输入。
- **解耦验证:** `ICodec` 测试不链接任何 transport 实现;`Transport` 测试不引入 `ICodec`/`Message`(除组合冒烟外)。

---

## 8. 蓝图:`System` 交互模式基类(后续阶段,本轮不实现)

> 列此节是为确保本轮底层"够用"——`Message` 的 `kind/correlation_id` 与 `SystemCodec` 的承载即为支撑下列模式而预置。

后续 `System`(用户继承的基类)将持有一个 `Transport` + 一个 `ICodec`,内置交互模式:
- `Send(Message)`:单向(kOneway)。
- `Request(Message, on_reply, timeout)`:请求-应答(分配 `correlation_id`、登记挂起、配 kReply、超时)。
- `Request(Message, on_feedback, on_final, timeout)`:请求-结果反馈(期间多条 kFeedback + 终结 kReply)。
- 三种接收原语 `Receive`/`OnReceive`/`AsyncReceive`(内置 `ReceiveQueue`)承接非请求消息(kOneway/kNotify)。
- 子类覆写 `OnRequest(req, responder)` 做服务端;`OnConnected/OnDisconnected/OnError`。
- 统一接收分发:`codec.Decode` 出的消息按 `kind` 分流——kReply/kFeedback 配挂起请求、kRequest→OnRequest、kOneway/kNotify→三模式。
- 线程:回调走 transport io 线程(非阻塞约定);请求超时用 System 自持的小 `io_context`+线程(取出再执行恰好一次 + `weak_ptr` 防自我 join)。

这些**全部留到后续 spec/plan**,本轮不写任何 System 代码。

---

## 9. 迁移与影响(0.1.0 → 0.2.0,本轮部分)

| 0.1.0 | 0.2.0 本轮 |
|---|---|
| 富 `ITransport`(Send(Message)/SetCodec/topic/三模式) | 纯管道 `ITransport`(只 bytes) |
| `TransportCore` | 删除(其职责拆到 codec 与未来 System) |
| `IFramer`/`LengthFieldFramer`/`FrameAssembler`/`TopicEnvelope`/`StreamSend` | 并入 `ICodec` 实现(`SystemCodec`/`LengthFieldCodec`/`DatagramCodec`) |
| `ICodec`(无状态 bytes↔bytes) | `ICodec`(有状态;Encode(Message)/Decode→0..N;承载 kind+corr_id) |
| `ReceiveQueue` + 三模式 | 本轮**移出 transport**,暂不归位(留给后续 System) |
| `TcpClientImpl`/`SerialImpl`/`UdpImpl` | 剥成纯管道 `TcpClientTransport`/`SerialTransport`/`UdpTransport` |
| `TcpConnectionImpl`/`TcpServerImpl`/`ITcpServer` | **本轮移除**(后续设计) |
| `DdsImpl`/`IDdsTransport`/`IDdsProvider`/`FastDds*` | **本轮移除**(后续设计) |
| `IDdsTransport::SendRequest/OnRequest` | 后续泛化进 `System`(本轮不做) |
| `TransportFactory`(5 类型化 Create + JSON) | 收窄为创建点对点纯管道(`Tcp/Serial/Udp`);JSON 后续按需 |

**说明:** 服务端/DDS、以及 `System`/`ReceiveQueue` 相关代码本轮从构建移除或不引入,放到后续 spec。0.1.0 标签保留完整旧实现备查。

---

## 10. 不做什么(YAGNI / 本轮范围外)
- **不做** `System`(交互模式 + 消息收发编排 + 三模式交付)——后续阶段。
- **不做** TCP 服务端(多连接)与 DDS(pub-sub)——后续阶段。
- **不做** Transport↔ICodec 的固化编排件(本轮仅以组合冒烟测试验证契约)。
- **不做** JSON 配置文件重建——先稳定底层 API。
- **不引入** 新第三方依赖。

---

## 11. 命名备注
- `ICodec` 现兼管分帧 + 交互元数据,语义已是"线缆格式";名保留(亦可 `IProtocol`)。
- `MessageKind` 取值若需对齐业务术语(请求/应答/结果反馈),实现期可调整命名。
- 上层基类暂称 `System`(后续阶段定);本轮不出现。
