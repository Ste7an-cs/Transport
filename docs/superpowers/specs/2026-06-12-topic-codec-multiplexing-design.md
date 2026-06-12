# Topic 路由编解码多路复用 — 设计 spec

> 在已合并的 Endpoint 统一寻址(`2026-06-12-endpoint-send-design.md`)之上扩展:
> 此前各传输的发送都是裸字节流 `Send(vector<uint8_t>)`,一条连接只能挂一个
> `ICodec`(单帧格式)。本设计引入 **topic = 编解码通道** 的概念:一条连接/传输上
> 按 topic 多路复用多种帧格式,框架按 topic 自动选 codec 编解码,并提供
> `Send(const Message&)` 让发送侧与接收侧的 `Message` 对称——字面实现"发送 Message
> (元数据 + payload)"。

**Goal:** 单条连接上承载多种帧格式,用 topic 区分并自动选对 codec;发送侧 `Send(Message)`
与接收侧 `Result<Message>` 对称,echo/relay 可直接往返。

**Tech Stack:** C++17,无新第三方依赖;GoogleTest 1.14(vendored)。

---

## 1. 核心概念:topic 与寻址正交

- **topic(做什么)** = 逻辑通道名,决定用哪个 `ICodec` 编解码 `payload`。收发两侧都携带,
  落在 `Message.topic`。
- **Endpoint(发到哪)** = 地址。TCP/串口地址即连接(隐式);UDP 是 `ip:port`;DDS 的 topic
  既是地址又天然可作 codec 键。

| 传输 | 地址 | topic 怎么走线 | codec 选择 |
|---|---|---|---|
| TCP 连接 | 隐式(该连接) | in-band 帧体:`[topic_len][topic][encoded]` | 按 topic 查注册表 |
| 串口 | 隐式(该口) | 同 TCP | 同上 |
| UDP | `Endpoint::Net` / 默认目的 | in-band 报文:`[topic_len][topic][encoded]` | 同上 |
| DDS | topic 即地址 | **不嵌**,用 DDS 原生 topic 通道 | 按发布 topic 查 |

DDS 有带外 topic 通道,故 **不把 topic 塞进字节流**;in-band envelope 只用于 TCP/UDP/串口。

## 2. 关键不变式

**`Message.payload` 在收发两侧恒为"应用原始字节(codec 之前/之后)";codec 永远由框架
在中间层施加,用户从不直接调 codec。** 这让 echo/relay 自洽:

```cpp
auto m = t->Receive();   // m.payload = 解码后原始字节, m.topic = "x"
t->Send(m.value);        // 框架按 topic="x" 重新 Encode 后发出
```

## 3. codec 注册表(`TransportCore`)

单 codec 升级为 `topic → ICodec` 注册表 + 一个默认 codec:

```cpp
// 保留:默认 codec —— topic 未注册时的兜底;不设则透传(原样字节)。
void SetCodec(std::shared_ptr<ICodec> codec);
// 新增:为某 topic 注册专用 codec。
void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec);

// 发送侧:按 topic 选 codec 编码(查不到回退默认;默认也无则透传)。
Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data,
                                           const std::string& topic);
```

解码侧沿用既有 `DeliverFrame(frame, source, topic)`,内部由"用单 codec 解码"改为
"按传入 topic 选 codec 解码(回退默认/透传)"。注册表为空且无默认时,行为与今天逐字节一致。

`ITransport` 相应新增带 topic 的 `SetCodec(topic, codec)` 重载,各实现按其既有
单参 `SetCodec` 的同样方式转发给自己的 `TransportCore`;每个 `*Impl` 加
`using ITransport::SetCodec;` 防 C++ 名字隐藏(与 `Send` 同)。

## 4. 发送 API:`Send(const Message&)`

`ITransport` 新增(非纯虚,基类默认实现,与既有 Endpoint 重载同风格):

```cpp
virtual Status Send(const Message& msg, const Endpoint& to = Endpoint::Default()) {
  if (msg.topic.empty()) return Send(msg.payload, to);   // 无 topic → 退化为既有发送
  return Status::Fail("io: topic routing not supported"); // 默认不支持 topic 路由
}
```

- 用 `msg.topic` 选 codec + 作 in-band 通道,`msg.payload` 作数据;`source/timestamp`
  在发送侧忽略(接收侧才填)。
- `to` 仅 UDP 使用(`Net` 定向);TCP/串口要求 `Default`(地址即连接);DDS 以 `msg.topic`
  为目的 topic 并忽略 `to`。
- 各实现覆写见 §5;每个 `*Impl` 加 `using ITransport::Send;` 防 C++ 名字隐藏。
- 现有 `Send(data)` / `Send(data, Endpoint)` 保留不变(无 topic、默认 codec、不加 envelope)。

## 5. opt-in 与各传输行为

**DDS:始终启用注册表,零 wire 改动。** DDS topic 本就在原生通道,`Send(Message)` 发往
`msg.topic`、按其选 codec;无 in-band envelope。无需开关。

**TCP/UDP/串口:envelope 由 config 开关 `enable_topic_routing`(默认 false)控制。**
- 关(默认)→ 与今天逐字节一致;`Send(Message)` 若 `msg.topic` 非空则
  `Fail("config: topic routing not enabled")`,空 topic 退化为 `Send(payload, to)`。
- 开 → 帧体/报文加 `[topic_len][topic][encoded]`;收端解析 topic 后选 codec 解码。

开关下放到 `TcpConfig` / `UdpConfig` / `SerialConfig`(`bool enable_topic_routing = false;`)。
两端必须一致(wire 约定),显式 config 便于审查。

各实现行为矩阵(`Send(Message)`):

| 实现 | topic 空 | topic 非空 + 路由开 | topic 非空 + 路由关 |
|---|---|---|---|
| TcpConnImpl/串口 | = `Send(payload)` | envelope 发出 | `Fail("config: topic routing not enabled")` |
| UdpImpl | = `Send(payload, to)` | envelope 发往 `to`/默认 | 同上 |
| DdsImpl | 发默认 topic | 发往 `msg.topic` | (DDS 无此开关,始终可路由) |

## 6. wire envelope 格式

**前提:既有 framer 是接收侧专用的。** 发送侧 TCP/串口直接写出 codec 输出字节,无框架长度前缀
——现有约定是「codec 自己产出自带长度的帧,接收侧 `LengthFieldFramer` 按 codec 内嵌的长度字段切帧」。
topic 必须在解码前可读,故必须位于 codec body 之外,这要求路由模式在 TCP/串口上由**框架自行分帧**。

定义两个 envelope 形态:

**topic envelope(UDP 报文体 / 流帧的内层)**
```
[topic_len : uint16 BE][topic : topic_len 字节 UTF-8][codec body : 余下全部]
```

**stream frame(TCP/串口,路由开启时框架自有分帧)**
```
[frame_len : uint32 BE][topic envelope]
其中 frame_len = topic envelope 字节数(不含自身 4 字节)
```

- `topic_len` 上限 65535;允许 0(开启路由但本帧 topic 为空)。`frame_len` 上限取一较大常量(如 16 MiB)。
- **路由模式下 codec 只编解码 body**(不再负责自带长度);框架的 `frame_len` 负责流上分帧。
  即:开启 `enable_topic_routing` 时,TCP/串口的接收分帧改用框架内置的 4 字节长度前缀帧器,
  **不使用** 用户在 config 里配的 `LengthFieldFramer`(路由模式下该 framer 配置被忽略)。
- UDP:topic envelope 即报文负载(报文边界天然分帧,无 `frame_len`)。
- DDS:不加任何 envelope,codec body 直接经原生 topic 发布。
- 打包/解包为纯函数,独立成 `include/transport/core/TopicEnvelope.hpp`,集中可测;
  流式累积复用同一长度分帧逻辑。

## 7. 测试策略

新增 `tests/topic_routing_test.cpp`:

- **TCP 多路复用**:一条回环连接,`enable_topic_routing=true`,注册 `"a"→CodecA`、
  `"b"→CodecB`;交错发两种 topic,收端各自用对应 codec 解码、`Message.topic` 正确;
- **默认回退**:发未注册的 topic → 走默认 codec;无默认 → 透传;
- **echo 往返**:`Send(Receive())` 同 topic 重新编码、往返一致;
- **opt-in 关闭**:`enable_topic_routing=false` 时 `Send(Message{topic="x",...})` →
  `Fail("config: topic routing not enabled")`;空 topic 退化等价 `Send(payload)`;
  且 wire 与旧格式逐字节相同(回归:旧单 codec 用例不变);
- **UDP**:`Send(Message, Endpoint::Net(...))` envelope 定向送达并按 topic 解码;
- **DDS**:`Send(Message{topic="t"})` 发往原生 topic `t`、按 t 选 codec,**不**加 in-band
  前缀(断言收到的原生 payload 无 topic_len 头);
- **基类默认**:未启用/不支持路由的句柄上 `Send(Message{topic 非空})` →
  `Fail("io: topic routing not supported")`(非 TCP/UDP/串口/DDS 的假想实现);
- **名字隐藏回归**:具体类型句柄上 `Send(data)` / `Send(data,Endpoint)` / `Send(Message)`
  三个重载均可见可调。

既有测试:`DeliverFrame` 相关、单 codec 收发用例保持断言不变(路由关闭默认行为)。

## 8. 文档同步

- 主 spec `2026-06-09`:`ITransport` 加 `Send(Message)` 与带 topic 的 `SetCodec`;新增
  §topic 路由与 envelope wire 格式;config 三处加 `enable_topic_routing`;changelog 记录;
- as-built 架构 spec `2026-06-10`:`TransportCore` 由单 codec 改 topic→codec 注册表;
- README:新增"单连接多帧格式(topic 路由)"用法示例(注册多 codec + `Send(Message)`)。

## 9. 不做什么(YAGNI)

- 不做 topic 通配/正则匹配(精确字符串查表);
- 不做每 topic 独立的 QoS/可靠性/分片策略;
- 不把 `source/timestamp` 做成发送侧可控字段(仍由接收侧框架填充);
- 不为纯中继提供"原样转发不重编码"专用路径(可用未注册 codec 的 topic 走透传);
- 不引入 topic 的数字别名/压缩(envelope 直接走字符串,与 `Message.topic` 同一字段)。
