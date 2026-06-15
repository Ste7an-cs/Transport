# System / ICodec / Transport 三层重构 — 设计 spec

> **大版本重构(0.2.0,破坏性)。** 把当前"transport 内建分帧+编解码+接收队列"的富 `ITransport`
> 拆成三层:**Transport 退化为纯字节管道**、**ICodec 吸收分帧+编解码(线缆格式)**、
> 新增 **`System` 作为用户继承的通信基类**(子类写业务)。本轮**只做点对点**
> (TCP 客户端 / 串口 / UDP);DDS(pub-sub/req-resp)与 TCP 服务端(多连接)留作后续设计。
> 0.1.0 已打标签 `v0.1.0` 留存,不受影响。

**Goal:** 让 transport 只负责搬字节,线缆格式(分帧+序列化)归 `ICodec`,通信编排与业务挂载归用户继承的 `System` 基类;职责单一、可独立测试、可自由组合。

**Tech Stack:** C++17;Standalone Asio(已 vendored);GoogleTest 1.14(已 vendored);不抛异常。

---

## 1. 动机与现状问题

当前(0.1.0)`ITransport` 一个接口同时承担:生命周期、发送、三模式接收、`ICodec` 挂载、按 topic 选 codec、`Send(Message)`、topic 信封分帧。分帧(`IFramer`/`FrameAssembler`)与编解码(`ICodec`)由 `TransportCore` 在 transport 内部驱动。结果:**transport 接口臃肿、与编解码/topic 强耦合,"搬字节"的本质被稀释**。

本设计把关注点彻底分层:

```
┌──────────────────────────────────────────────┐
│ System(用户继承的通信基类)                   │ 编排 + 业务钩子(子类实现)
│  持有 Transport + ICodec,驱动收发,推 OnMessage │
├──────────────────────────────────────────────┤
│ ICodec(线缆格式 = 分帧 + 序列化,有状态)      │ 库内置 + 用户自定义
├──────────────────────────────────────────────┤
│ Transport(纯字节管道)                         │ 库提供 TCP 客户端/串口/UDP
└──────────────────────────────────────────────┘
```

**关键非对称(本设计的依据):** 编解码是 `bytes↔bytes` 的纯变换、可分离;而流式(TCP/串口)分帧需要**跨多次 read 的滚动缓冲**,本质有状态。把两者都收进 `ICodec` 后:transport 彻底无状态于"消息"概念,只吐裸字节;ICodec 成为唯一掌管线缆格式的有状态对象。

---

## 2. 核心数据类型(基本沿用)

### 2.1 `Result<T>` / `Status`
不变:`{ T value; bool ok; std::string error; }`,`Success`/`Fail("prefix: msg")`,`explicit operator bool`,`[[nodiscard]]`。前缀:`timeout:`/`conn:`/`codec:`/`frame:`/`io:`/`config:`。

### 2.2 `Message`
不变:`{ vector<uint8_t> payload; string topic; string source; int64_t timestamp; }`。
- `payload`:`Decode` 后的应用字节。
- `topic`:若线缆格式带通道,由 **codec** 填;否则空(不再是 transport 概念)。
- `source`:由 **System** 从 transport 回调的 `from` 字符串直接填(UDP 发送方 ip:port,TCP 对端 ip:port,串口设备路径)。
- `timestamp`:由 **System** 填(微秒)。

### 2.3 `Endpoint`
不变:`Kind{kDefault,kNet,kTopic}` + `Net/Topic/Default`。本轮点对点主要用 `kDefault`(默认目的地)与 `kNet`(UDP 运行期寻址)。

---

## 3. 第一层:`Transport` 纯字节管道

```cpp
// include/transport/ITransport.hpp
class ITransport {
 public:
  virtual ~ITransport() = default;

  virtual Status Open() = 0;
  virtual void   Close() = 0;
  virtual bool   IsOpen() const = 0;

  // 发裸字节;to=默认目的地或(UDP)运行期寻址
  virtual Status Send(const std::vector<uint8_t>& bytes) = 0;
  virtual Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) = 0;

  // 收裸字节(io 线程,经 strand 串行化):
  //   流式(TCP/串口)= 本次 read 到的切片;报文式(UDP)= 一个完整 datagram。
  //   from = 来源标识字符串(UDP 发送方 "ip:port";TCP 对端 "ip:port";串口设备路径),
  //          直接填入 Message.source;失败时 from 为空。
  virtual void OnBytes(
      std::function<void(Result<std::vector<uint8_t>> bytes,
                         const std::string& from)> cb) = 0;

  // 连接建立(含重连)/ 断开 通知
  virtual void OnConnect(std::function<void()> cb) = 0;
  virtual void OnDisconnect(std::function<void(const std::string& reason)> cb) = 0;
};
```

**要点:**
- transport **不知道** `Message`/`ICodec`/topic/分帧的存在。
- 每个回调单消费者(由 System 注册一次);回调在 transport 的 io 线程 + strand 上,串行不并发。
- 连接生命周期(connect/超时/指数退避重连/断开)仍是 transport 的 I/O 职责;重连透明恢复字节流并触发 `OnConnect`。
- `Send` 语义同今:入队/写出即返回 `Status`;写失败经……(见 §6 错误)。

**内置实现(本轮):** `TcpClientTransport` / `SerialTransport` / `UdpTransport`,由现有 `TcpClientImpl` / `SerialImpl` / `UdpImpl` 剥离 `TransportCore`/codec/framing 改造而来——接收路径改为吐裸字节 + `from`,删除 `SetCodec`/`Send(Message)`/topic。

---

## 4. 第二层:`ICodec` 线缆格式(分帧 + 序列化)

```cpp
// include/transport/ICodec.hpp
class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发:一条消息 → 一段线缆字节(序列化 + 加帧头)。一对一。
  virtual Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  // 收:喂入一段字节切片 → 切出 0..N 条完整消息(内部维护滚动缓冲)。
  //   流式:可能 0 条(半包)、1 条、多条(粘包);报文式:通常 1 条。
  //   解析错误(坏帧头/越界)→ Fail("frame: ..." / "codec: ...")。
  virtual Result<std::vector<Message>> Decode(const uint8_t* data,
                                              std::size_t len) = 0;
};
```

**要点:**
- **有状态**:`Decode` 持有滚动缓冲 → **每个 `System` 一个 codec 实例,不可跨连接共享**。
- 只被 transport 的 io 线程(经 OnBytes)单线程调用,故 codec **无需线程安全**。
- `Encode` 只产 `payload`(及可选 topic)对应的字节;`source`/`timestamp` 不参与编码。

**内置实现(本轮):**
- `LengthFieldCodec`:吸收现 `LengthFieldFramer` + `FrameAssembler` 逻辑(固定 header + 长度字段切帧),body 透传为 `payload`。可配 `header_size/length_offset/length_size∈{2,4,8}/big_endian/length_includes_header/max_frame_size`。
- `DatagramCodec`:报文式直通——`Decode` 把整段字节当一条 `Message`,`Encode` 原样输出(UDP 无分帧时用)。
- 用户继承 `ICodec` 实现任意完整线缆格式(header+body+topic+校验等一体)。

**被删除/合并:** `IFramer`、`LengthFieldFramer`、`FrameAssembler`、`TopicEnvelope`(topic 信封)、`StreamSend` → 逻辑并入各 `ICodec` 实现,不再作为 transport 层概念。

---

## 5. 第三层:`System` 用户继承的通信基类

```cpp
// include/transport/System.hpp
class System {
 public:
  System(std::shared_ptr<ITransport> transport, std::shared_ptr<ICodec> codec);
  virtual ~System();

  Status Open();   // 注册 transport 回调 → transport->Open()
  void   Close();  // transport->Close()
  bool   IsOpen() const;

  // 发:codec->Encode(msg) → transport->Send(bytes, to)
  Status Send(const Message& msg, const Endpoint& to = Endpoint::Default());

 protected:
  // —— 业务钩子(子类覆写),全部在 io 线程被调,须非阻塞 ——
  virtual void OnMessage(const Message& msg) = 0;             // 必填:收到一条完整消息
  virtual void OnConnected() {}                               // 可选
  virtual void OnDisconnected(const std::string& reason) {}   // 可选
  virtual void OnError(const std::string& error) {}           // 可选:解码失败/IO 错误

 private:
  std::shared_ptr<ITransport> transport_;
  std::shared_ptr<ICodec> codec_;
  // Open() 内:
  //   transport_->OnBytes([this](Result<bytes> r, const std::string& from){
  //       if(!r){ OnError(r.error); return; }
  //       auto msgs = codec_->Decode(r.value.data(), r.value.size());
  //       if(!msgs){ OnError(msgs.error); return; }
  //       for(auto& m : msgs.value){ m.source=from; m.timestamp=now; OnMessage(m); }
  //   });
  //   transport_->OnConnect([this]{ OnConnected(); });
  //   transport_->OnDisconnect([this](auto& r){ OnDisconnected(r); });
};
```

**要点:**
- **交付:push-only**。子类覆写 `OnMessage`;**无接收队列、无三模式**——`ReceiveQueue` 被彻底删除(它原为支撑 pull 三模式而存在)。收到即在 io 线程逐条推给 `OnMessage`。
- **线程:** 所有钩子在 transport io 线程被调,**约定非阻塞**;耗时业务由子类自行派发到别处。
- **保活:** `System` 以 `shared_ptr` 持有;回调捕获 `weak_ptr`(或由 transport 的 `shared_from_this` 链保活),避免"最后引用在 io 线程析构 → 自我 join 死锁"(沿用现有约束)。
- `Send` 线程安全:转发给 transport 的 strand 串行写出。

**用户用法:**
```cpp
class MyApp : public System {
 public:
  using System::System;                  // 继承构造
 protected:
  void OnMessage(const Message& msg) override {
    Send(BuildReply(msg));               // 业务在子类;可直接回发
  }
  void OnDisconnected(const std::string& r) override { /* ... */ }
};

auto app = std::make_shared<MyApp>(
    TransportFactory::Tcp(tcp_cfg),                      // 纯管道
    std::make_shared<LengthFieldCodec>(lf_cfg));         // 线缆格式
app->Open();
app->Send(Message{ .payload = bytes });
```

---

## 6. 错误处理

- 全程 `Result`/`Status`,不抛异常,前缀分类不变。
- `codec_->Decode` 失败(坏帧)→ `System` 调 `OnError("frame:/codec: ...")`;**该连接是否断开由 codec 语义决定**:流式坏帧通常不可恢复 → System 在 `OnError` 后可选 `Close()`(本设计:Decode 返回 `frame:` 视为致命,System 关闭;`codec:` 仅丢弃当前批,继续)。
- transport 断开 → `OnDisconnected(reason)`;`OnBytes` 投递 `Fail` → `OnError`。
- `Send` 前 `Encode` 失败 → 返回 `Status::Fail`,不触达 transport。

---

## 7. 数据流总览

```
发送: 子类 Send(msg, to)
      → codec_->Encode(msg) → Result<bytes>
      → transport_->Send(bytes, to)        // strand 串行写出

接收: transport io线程读到字节/收到 datagram
      → OnBytes(bytes, from)               // strand 上
      → codec_->Decode(bytes) → 0..N 条 Message
      → 逐条: 填 source(from)/timestamp → OnMessage(msg)   // 仍在 io 线程
```

---

## 8. 迁移与影响(0.1.0 → 0.2.0)

| 0.1.0 | 0.2.0 |
|---|---|
| 富 `ITransport`(含 Send(Message)/SetCodec/topic) | 纯管道 `ITransport`(只 bytes) |
| `TransportCore`(codec 注册表 + 接收队列 + 分帧编排) | **删除**:codec 调用上移 System、分帧并入 codec、队列删除 |
| `ReceiveQueue` + 三模式接收 | **删除**(push-only) |
| `IFramer`/`LengthFieldFramer`/`FrameAssembler` | 并入 `LengthFieldCodec` |
| `TopicEnvelope`/`StreamSend`/topic 路由 | **删除**(topic 由 codec 自定义格式承载) |
| `ICodec`(无状态 bytes↔bytes) | `ICodec`(有状态,Encode(Message)/Decode→0..N) |
| `TcpClientImpl`/`SerialImpl`/`UdpImpl` | 剥离为纯管道 `TcpClientTransport`/`SerialTransport`/`UdpTransport` |
| `TcpConnectionImpl`/`TcpServerImpl`/`ITcpServer` | **本轮移除**(TCP 服务端=多连接,后续设计) |
| `DdsImpl`/`IDdsTransport`/`IDdsProvider`/`FastDds*` | **本轮移除**(DDS=pub-sub/req-resp,后续设计) |
| `TransportFactory`(5 类型化 Create + JSON) | 收窄为创建点对点纯管道(`Tcp/Serial/Udp`);JSON 配置后续按需 |

**说明:** 服务端/DDS 相关代码在本轮从构建中移除(它们无法对接纯 `ITransport`),其设计与重新落地放到后续 spec(大概率为 `System` 的特化子类 `ServerSystem`/`DdsSystem`)。0.1.0 标签保留完整旧实现备查。

---

## 9. 测试策略

- **`ICodec` 单元(核心):** `LengthFieldCodec` 表驱动——半包/粘包/跨切片/超长/坏帧头/大小端/`length_size∈{2,4,8}`;`DatagramCodec` 一段一条。
- **`System` 单元(Mock transport):** 用 `FakeTransport`(进程内,可手动注入 OnBytes/OnConnect/OnDisconnect)验证:Decode→逐条 OnMessage、source/timestamp 填充、Encode→Send 透传、Decode 失败→OnError、断开→OnDisconnected。零真实 I/O。
- **回环集成:** TCP 客户端↔回环 server(测试内起裸 asio acceptor)、串口(`openpty`)、UDP(回环 + `Endpoint::Net`):继承一个测试用 `System` 子类,断言端到端收发与重连。
- **线程约定:** 验证 OnMessage 在 io 线程、Close 幂等、无自我 join 死锁。

---

## 10. 不做什么(YAGNI / 本轮范围外)

- **不做** TCP 服务端(多连接)与 DDS(pub-sub/req-resp)——后续 spec。
- **不做** pull 接收(同步 `Receive`/future)——push-only;确有需要再加可选适配。
- **不做** 独立业务线程——默认 io 线程 + 非阻塞约定;可选 worker 后续按需。
- **不做** codec 链式组合(多个 codec 串联)——单 `ICodec` 承载完整格式;链式后续按需。
- **不做** JSON 配置文件重建——先稳定代码 API,配置后续按需。
- **不引入** 新第三方依赖。

---

## 11. 命名备注

- `System` 名字较泛;候选 `CommSystem`/`Node`/`Peer`/`Link`/`Session`。**本 spec 暂用 `System`**(沿用讨论),实现期可再定。
- `ICodec` 现兼管分帧,语义已是"线缆格式";名字保留(亦可考虑 `IProtocol`)。
