# 外部协议跑 UDP(无状态帧 codec + 1:多寻址)— 设计

**日期：** 2026-06-26
**状态：** 设计已确认,待写 plan
**配套：** `docs/superpowers/specs/2026-06-23-protocol-node.md`(外部协议栈)、《设计说明书》§7–8

## 1. 背景与目标

外部协议(`SystemCodec` 帧:`AA BB CC DD`/CRC/`(session_id,message_id)` 匹配)需经 **UDP** 与外部系统通信,且是 **1:多**(一个 UDP socket 同时对多个外部端,收发两方向都要)。

现状问题:`SystemCodec` 是**有状态流式**解码(持久滚动缓冲 `buffer_` 跨 `Decode` 累积),为字节流(TCP/串口)设计。套在 UDP 上有真实隐患——多对端时各家 datagram 混进同一 `buffer_` → 串台;半截/坏报文残留污染下一报文。UDP 是报文边界语义,应**无状态按报文解码**。

**目标:** 外部协议在 UDP 上正确跑 1:多,收发两方向:
- **收多家、回多家**(我们作服务端):应答回到请求来源 ip:port。
- **发多家**(我们作客户端):每次发送可指定目的设备;`needfeedback` 自动 ack 回到结果来源。

**非目标(YAGNI):** 改 DDS/TCP/串口现有行为;IPv6 带方括号语法解析(按最后一个 `:` 切分,`::1` 形式可用);可靠性/重传跨 datagram 拼帧(UDP 不跨报文拼帧)。

## 2. 关键决策

| 决策 | 选择 |
|---|---|
| UDP codec | 新增 `SystemDatagramCodec`(`SystemCodec` 的**无状态孪生**),不就地改通用 `DatagramCodec` |
| 复用 | 抽出共享帧逻辑 `EncodeSystemFrame` / `ScanSystemFrames`,流式版与报文版**共用、不复制** |
| 应答寻址 | `ProtocolConfig.reply_to_source` 开关 → `ProtocolPolicy.ReplyTo` 回 `Net(来源 ip:port)`;默认 false(TCP/串口/1:1 不变) |
| 主动多发 | `ProtocolNode` 5 个发送方法加可选尾参 `const Endpoint& to = Default()`,转发引擎原语 |

## 3. 组件一:`SystemDatagramCodec`(无状态报文帧 codec)

### 3.1 共享帧核(DRY)

把 `SystemCodec.cpp` 现有逻辑抽成两个自由函数(声明放 `SystemCodec.hpp`,定义放 `SystemCodec.cpp`),流式版与报文版共用:

- `Result<std::vector<uint8_t>> EncodeSystemFrame(const Message&, const CrcFn&)` —— 现 `SystemCodec::Encode` 的函数体原样搬出(一条消息 → 一个帧)。
- `std::size_t ScanSystemFrames(const uint8_t* data, std::size_t len, const CrcFn&, std::vector<Message>& out)` —— 现 `SystemCodec::Decode` 的 while 扫描循环(找同步头 / 头-体完整性 / CRC resync / 拆帧)原样搬出,返回**消费字节数**(off)。

重构后:
- `SystemCodec::Encode` = `return EncodeSystemFrame(msg, crc_);`
- `SystemCodec::Decode` = append 到 `buffer_` → `off = ScanSystemFrames(buffer_.data(), buffer_.size(), crc_, out)` → erase 前 off 字节(**保留**残留)。**行为逐字不变**,现有 `system_codec_test` 即回归证明。

### 3.2 `SystemDatagramCodec`(新,header-only)

```
class SystemDatagramCodec : public ICodec {
  explicit SystemDatagramCodec(CrcFn crc = DefaultCrc16);
  Encode(msg)      → EncodeSystemFrame(msg, crc_)          // 与流式版完全相同
  Decode(data,len) → ScanSystemFrames(data, len, crc_, out); 返回值(残留)丢弃
};
```

- **无状态**:每次 `Decode` 只对**这一个 datagram** 扫描,吐出其中所有整帧;尾部残留(消费数 < len 的部分)**直接丢弃、零保留**。本报文内坏帧仍可 resync(前移),只是不跨 datagram。
- 多对端天然安全(每报文独立解,无共享缓冲);坏/半截报文只丢自己,不污染他人。
- 与流式版同样构造注入 `CrcFn`;两端、两 codec 须同一套 `CrcFn`/`frm_type` 常量才互通。

## 4. 组件二:应答寻址 `reply_to_source`(收发两方向都需要)

引擎所有「对入站消息的回送」都经 `ProtocolPolicy.ReplyTo(request)` 取目的地:
- 服务端 `Responder.Response/Result` → `SendReply(命令, …)` → `ReplyTo(命令)`。
- 客户端 `needfeedback` 自动 ack → `SendReply(结果, …)` → `ReplyTo(结果)`。

两者都应回到「该入站消息的来源」。引擎已把来源放进 `Message.source`(UDP = `"ip:port"`)。故:

- **`ProtocolConfig`** 加 `bool reply_to_source = false;`(注:1:多 UDP 置 true——应答/ack 回到入站消息来源 ip:port;TCP/串口/1:1 留 false,因它们对非 Default Endpoint 会 `io: addressed send not supported`)。
- **`ProtocolPolicy`** 构造加 `bool reply_to_source = false`(存成员);`ReplyTo(req)`:
  - 若 `reply_to_source_` 且 `req.source` 可解析为 `host:port`(按**最后一个 `:`** 切分,兼容 IPv4 与 `::1` 形式)→ `Endpoint::Net(host, port)`;
  - 否则 → `Endpoint::Default()`(解析失败/空也回退 Default,安全)。
- **`ProtocolNode`** 构造把 `config.reply_to_source` 透传进 `ProtocolPolicy`。

> 一个开关覆盖两方向:服务端回 Responder、客户端回 needfeedback ack,本质都是「回到入站消息的 source」,在 1:多 UDP 下都需 `Net(source)`。

## 5. 组件三:主动多发 `const Endpoint& to`

`ProtocolNode` 发送方法目前不透传目的地(一律 `Default`)。引擎原语 `Fire`/`RequestAwait`/`StartPeriodic` 本就收 `const Endpoint& to`。给 5 个方法加可选尾参并转发:

```
SendNoResponse(uint16_t cmd, payload, const Endpoint& to = Default())
Request(uint16_t cmd, payload, ReplyFn on_response, const Endpoint& to = Default())
RequestWithResult(uint16_t cmd, payload, ReplyFn on_result, const Endpoint& to = Default())
RequestNeedFeedback(uint16_t cmd, payload, ReplyFn on_response, ReplyFn on_result, const Endpoint& to = Default())
StartRepeating(uint16_t cmd, payload, uint32_t interval_ms, const Endpoint& to = Default())
```

- 客户端经一个 UDP socket 向多个设备发:`node->Request(cmd, p, cb, Endpoint::Net("10.0.0.7", 7000))`。
- 匹配键 `(session_id, message_id)` 每请求唯一(session 滚动 0–255),多设备并发回应不串(≤256 并发);引擎重发用 `Pending.to`(已存),自动重发到同一设备。
- 默认 `Default` → TCP/串口/1:1 调用点零改动。

## 6. 装配:UDP 节点

```
ProtocolConfig pc; pc.protocol_id = 1; pc.reply_to_source = true;   // 1:多 UDP
auto node = std::make_shared<MyNode>(
    std::make_shared<UdpTransport>(udp_cfg),
    std::make_unique<SystemDatagramCodec>(),                        // 无状态报文帧
    pc);
// 收:OnCommand 照常,Responder 回到来源
// 发:node->Request(cmd, payload, cb, Endpoint::Net(device_ip, device_port));
```

引擎/`InteractionEngine`/`InteractionPolicy` 接口不动;DDS/TCP/串口路径不变。

## 7. 文件

**新增**
- `include/transport/codec/SystemDatagramCodec.hpp`(header-only)。
- `tests/codec/system_datagram_codec_test.cpp`:单帧/多帧一报文/坏帧丢弃不跨报文/CRC 不符丢/与 `SystemCodec` Encode 字节一致/空报文。

**修改**
- `include/transport/codec/SystemCodec.hpp` + `src/codec/SystemCodec.cpp`:声明+定义 `EncodeSystemFrame`/`ScanSystemFrames`,两方法改为调用(行为不变)。
- `include/transport/comm/ProtocolNode.hpp`:`ProtocolConfig` 加 `reply_to_source`;5 个发送方法加 `to` 尾参。
- `src/comm/ProtocolNode.cpp`:发送方法转发 `to`;构造把 `reply_to_source` 传入 `ProtocolPolicy`。
- `include/transport/comm/ProtocolPolicy.hpp`:构造加 `reply_to_source`;`ReplyTo` 解析 source。
- `tests/comm/protocol_node_test.cpp`:补 `to` 透传 + `reply_to_source` 回送来源用例(可用两 UDP 或 fake 注入带 source 的入站)。
- CMake 注册新测试。

**不回归:** 现有 `system_codec_test`(流式行为不变)+ 全部 comm 测试;默认 `reply_to_source=false`、`to=Default` → 旧调用点零改动。

## 8. 约束

- C++17,不抛异常,`Result`/`Status`。`SystemDatagramCodec` header-only;接口层零第三方依赖。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,无 Co-Authored-By。
- 文档(SRS/SDD/README/CHANGELOG)+ demo 同步留到实现后。
