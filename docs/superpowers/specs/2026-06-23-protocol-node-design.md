# 外部协议栈(帧 codec + ProtocolNode)设计

> 面向某外部系统的具体通信协议:把 `SystemCodec` 改造成该协议的帧格式,并新建 `ProtocolNode` 实现其交互模式(5 种发送模式 + 重发 + repeating + 心跳 + 收发双角色)。**`CommNode` / `DdsNode` / `DdsCodec` 完全不动**(DDS 不走此协议)。

**适用传输:** TCP / UDP / 串口(点对点)。DDS 仍走 `DdsNode`/`DdsCodec`,与本协议无关。

---

## 1. 目标与范围

- 把 `SystemCodec` 改造为外部协议帧 codec(`AA BB CC DD` 同步头 + 协议字段 + 可注入 CRC + 流式 resync)。
- 新建 `ProtocolNode`:实现该协议的交互模式,复用 `IExecutor`(定时/执行) + `ITransport` + 改造后的 `SystemCodec`。
- `Message` 加性扩展协议字段。

**架构决定(已定):**
- **不新建 codec**:改 `SystemCodec`。**新建 `ProtocolNode`**(独立节点,不继承也不改 `CommNode`)。
- DDS 不兼容本协议:`CommNode`/`DdsNode`/`DdsCodec` 零改动。
- `SystemCodec` 被改成协议专用后,原先用它测 `CommNode` 通用 req-resp 的 `comm_node_test` 改用 `DdsCodec`(带 kind/corr,够通用)。

**范围外(YAGNI):** `noresponsewithcheck` 模式(暂不做)、QoS、分片(body>65535 直接报错)、多外部系统路由表(`protocol_id` 按节点配置一个)。不引入新第三方依赖。

**实现前补充(外部常量,spec 占位):** `frm_type` 六类的真实字节值;CRC16 自定义算法(经注入接口提供)。

---

## 2. 帧格式(小端)

```
偏移  字段        长度  说明
0     head_flag    4    固定 AA BB CC DD(字节序列,逐字节匹配)
4     frm_type     1    帧类型枚举(见 §4)
5     protocol_id  1    外部系统 id(按节点配置)
6     session_id   1    会话 id
7     reserve      4    保留,固定 0x00000000
11    crc          2    CRC16,小端,经注入算法校验 frm_body
13    frm_len      2    frm_body 长度,小端,≤ 65535
15    frm_body     N    = [message_id:2 小端][payload:0..65533]
```
- 帧头固定 **15 字节**,其后 `frm_len` 字节 body。
- `frm_len = 2 + payload_len`;`payload_len > 65533` → 编码报错 `frame: payload too long`。
- **匹配键 = (session_id, message_id)**;RESPONSE/RESULT 原样回填请求帧的 session_id + message_id。
- CRC 覆盖整个 `frm_body`(含 message_id 两字节)。

---

## 3. `Message` 加性扩展

`include/transport/Message.hpp` 增加协议字段(generic 字段 kind/correlation_id/topic/reply_to 保留不变,本协议不用):

```cpp
enum class FrameType : uint8_t {   // 占位值,实现前替换为外部真实字节值
  kUnknown   = 0,
  kCommand   = 1,
  kResponse  = 2,
  kResult    = 3,
  kState     = 4,
  kHeartbeat = 5,
};

struct Message {
  // ... 现有 generic 字段不变 ...
  FrameType frm_type    = FrameType::kUnknown;  // 协议:帧类型
  uint8_t   protocol_id = 0;                    // 协议:外部系统 id
  uint8_t   session_id  = 0;                    // 协议:会话 id
  uint16_t  message_id  = 0;                    // 协议:帧唯一 id
};
```
`FrameType` 定义在 `Message.hpp`(与 `MessageKind` 并列)。`ProtocolNode`/`SystemCodec` 读写这些字段;`CommNode`/`DdsNode` 不碰。

---

## 4. `SystemCodec` 改造(协议帧 codec)

`include/transport/codec/SystemCodec.hpp` + `src/codec/SystemCodec.cpp`。

### CRC 注入接口
```cpp
using CrcFn = std::function<uint16_t(const uint8_t* body, std::size_t len)>;
```
- 构造:`explicit SystemCodec(CrcFn crc = DefaultCrc);`
- `DefaultCrc` = **占位实现**(spec 阶段给一个可用的 CRC16-CCITT 占位;真实算法实现前经构造注入替换)。两端用同一 `CrcFn` 即可自洽。

### Encode(`const Message&`) → 帧字节(一对一)
1. `payload.size() > 65533` → `Fail("frame: payload too long")`。
2. `body = [message_id:2 LE][payload]`;`crc = crc_(body)`。
3. 拼头:`AA BB CC DD` + `frm_type 字节` + `protocol_id` + `session_id` + `00 00 00 00` + `crc:2 LE` + `frm_len:2 LE` + body。
4. `frm_type` → 字节:`static_cast<uint8_t>(msg.frm_type)`(枚举底值即外部字节值)。

### Decode(`const uint8_t*`, `len`) → 0..N `Message`(有状态、流式)
滚动缓冲 `buffer_`,循环:
1. **同步**:在缓冲中找 `AA BB CC DD`。找不到 → 保留末尾 ≤3 字节(可能半个同步头),其余丢弃,返回。
2. 同步头后不足 15 字节头 → 等更多数据(break)。
3. 读 `frm_len`;`15 + frm_len` 超过当前可用 → 等更多数据(break)。
4. **校验**:`crc_(body)` 与帧内 crc 不符 → **跳过该同步头首字节(前移 1),重新同步**(resync,不报错不拆连接)。
5. 通过:`frm_type` 字节 → 已知枚举值则取之,否则 `kUnknown`;提取 `protocol_id`/`session_id`/`message_id`/`payload` → 组 `Message`;offset 前移 `15 + frm_len`。
- 解码不产生 `frame:` 致命错误(坏帧一律 resync 跳过);仅编码越界报 `frame:`。

> 改造后 `SystemCodec` 不再携带 kind/corr/topic;`comm_node_test` 改用 `DdsCodec`。

---

## 5. `ProtocolNode`(交互层)

`include/transport/comm/ProtocolNode.hpp` + `src/comm/ProtocolNode.cpp`。独立类(`enable_shared_from_this`),复用 `ITransport`+`ICodec`(默认 `SystemCodec`)+`IExecutor`(默认 `ThreadExecutor`)。须以 `shared_ptr` 持有。

### 配置
```cpp
struct ProtocolConfig {
  uint8_t  protocol_id = 0;            // 本节点对接的外部系统 id(贴到出站帧)
  uint32_t response_timeout_ms = 1000; // 等待回应/结果超时
  uint32_t max_retries = 3;            // 超时重发上限(超过即失败)
  uint32_t heartbeat_interval_ms = 0;  // 0 = 关闭心跳
};
```

### 接口
```cpp
class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,          // null → SystemCodec(DefaultCrc)
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,
               std::size_t queue_capacity = 1024);

  Status Open(); void Close(); bool IsOpen() const;

  // 发送 5 模式(payload = 业务字节):
  Status SendNoResponse(std::vector<uint8_t> payload);                          // noresponse
  Status Request(std::vector<uint8_t> payload, ReplyFn on_response);            // needresponse
  Status RequestWithResult(std::vector<uint8_t> payload, ReplyFn on_result);    // withfeedback
  Status RequestNeedFeedback(std::vector<uint8_t> payload,
                             ReplyFn on_response, ReplyFn on_result);           // needfeedback
  uint32_t StartRepeating(std::vector<uint8_t> payload, uint32_t interval_ms);  // repeating→handle
  void     StopRepeating(uint32_t handle);

 protected:
  virtual void OnCommand(const Message& cmd, Responder responder) {}  // 收到 COMMAND(接收角色)
  virtual void OnHeartbeat(const Message& hb) {}
  virtual void OnError(const std::string& error) {}
};

class Responder {                       // 接收角色应答句柄(回填 session+message)
 public:
  Status Response(std::vector<uint8_t> payload);  // 发 RESPONSE
  Status Result(std::vector<uint8_t> payload);    // 发 RESULT
};
```

### 发送方状态机(每事务键 = (session_id, message_id))
节点维护滚动 `session_id`(uint8)+`message_id`(uint16)计数器,每次发送分配一对、登记挂起。

| 模式 | 发送帧 | 完成条件 | 超时行为 |
|---|---|---|---|
| `noresponse` | COMMAND | 发出即完成 | — |
| `needresponse` | COMMAND | 收到配对 RESPONSE → `on_response(Success)` | 重发 COMMAND;`max_retries` 后 `on_response(Fail "timeout:")` |
| `withfeedback` | COMMAND | 收到配对 RESULT → `on_result(Success)` | 同上(`on_result` 失败) |
| `needfeedback` | COMMAND | 收 RESPONSE(中间)→ 收 RESULT → **自动回 RESPONSE(ack)** → `on_result(Success)` 完成 | 在收到首个 RESPONSE 前:重发 COMMAND ≤`max_retries`;收到 RESPONSE 后等 RESULT 超时 → 失败(不再重发) |
| `repeating` | STATE | 不完成(持续) | 每 `interval_ms` 发一帧 STATE(各自新 message_id);`StopRepeating(handle)` 或 `Close()` 停 |

- 超时用 `executor_->ScheduleAt`;**收到任一推进事务的帧重置计时**。重发即用同一 (session_id, message_id) 重发原 COMMAND。
- `needfeedback` 的自动 ack RESPONSE 回填该事务的 (session_id, message_id),`protocol_id` 不变。

### 接收方(双角色)
- 入站 `frm_type` 分发(在 worker 串行):
  - `COMMAND` → `OnCommand(msg, Responder{session_id,message_id})`;应用用 `Responder.Response()/Result()` 回(回填键)。
  - `RESPONSE`/`RESULT` → 按 (session_id, message_id) 命中挂起事务,推进其状态机;未命中则丢弃。
  - `HEARTBEAT` → `OnHeartbeat`。
  - `STATE` → 交 `OnCommand`(被动接收的状态帧;`msg.frm_type` 可辨,本版不单列 `OnState`,避免接口膨胀)。
  - `UNKNOWN`/无法识别 → `OnError("codec: unknown frame type")`。

### 心跳
`heartbeat_interval_ms > 0` 时,`Open()` 起一个周期定时器发 HEARTBEAT(自身 session/message 计数);入站 HEARTBEAT → `OnHeartbeat`。

---

## 6. 数据流与生命周期

- `Open()`:注册 transport 回调(OnBytes→Decode→`executor_->Post`→worker 分发;OnConnect/OnDisconnect)→ `executor_->Start()` → `open_=true` → `transport_->Open()`;失败回滚。心跳定时器(若开)在此启动。
- io/listener 线程内联 `Decode`(SystemCodec 有状态、单线程喂)→ 每条 `Message` `Post` 到单 worker 串行分发(背压在 Post)。
- `Close()`:幂等;终结所有挂起事务(`Fail("conn: node closed")`)→ 停心跳/repeating 定时器 → `executor_->Stop()` → `transport_->Close()`。
- posted 任务/transport 回调捕获 `weak_ptr`。

---

## 7. 错误处理

- 未 Open 时发送 → `config: node not open`。
- 编码 body 越界 → `frame: payload too long`。
- 解码坏帧(同步头/CRC 不符)→ resync 跳过,不报致命错(可选 `OnError` 计数,本版静默跳过)。
- 事务超时且超 `max_retries` → 该事务回调 `Fail("timeout: ...")`。
- 入站 `UNKNOWN` 帧 → `OnError`。
- 全程不抛异常;`Result<T>`/`Status`,`[[nodiscard]]`。

---

## 8. 并发要点

- `SystemCodec` 仍是有状态流式 codec,由单一 io 线程喂 `Decode`(同现状);`Encode` 只读 `Message`(CRC 注入函数须无副作用/线程安全)。
- 挂起事务表 `pending_`(键 (session,message))由 `mu_` 保护;`Encode`+`Send` 在锁外(避免与同步执行器回环死锁)。
- 重发/超时/repeating/心跳定时器经 `executor_->ScheduleAt`;事务推进与超时同在 worker → 恰好一次。
- repeating/心跳的取消在 `Close` 前完成,保证无定时器在析构后触发。

---

## 9. 文件结构

**修改:**
- `include/transport/Message.hpp`(加 `FrameType` + 4 协议字段)。
- `include/transport/codec/SystemCodec.hpp` + `src/codec/SystemCodec.cpp`(协议帧 + CRC 注入 + resync)。
- `tests/codec/system_codec_test.cpp`(重写为协议帧字节级测试)。
- `tests/comm/comm_node_test.cpp`(把 `SystemCodec` 换成 `DdsCodec`,保持测 CommNode 通用 req-resp)。
- `CMakeLists.txt`(加 `src/comm/ProtocolNode.cpp`、新测试)。

**新建:**
- `include/transport/comm/ProtocolNode.hpp` + `src/comm/ProtocolNode.cpp`(节点 + `Responder` + `ProtocolConfig`)。
- `tests/comm/protocol_node_test.cpp`。

**不动:** `CommNode`/`DdsNode`/`DdsCodec`/`IExecutor`/`ThreadExecutor`/transport 层、DDS 测试。

---

## 10. 测试(TDD)

### `SystemCodec`(`tests/codec/system_codec_test.cpp` 重写)
- `EncodeProducesProtocolFrame`:字节级校验 head_flag/frm_type/protocol_id/session_id/reserve=0/crc/frm_len/body 布局(注入确定性 CRC)。
- `EncodeDecodeRoundtrip`:frm_type/protocol_id/session_id/message_id/payload 往返一致。
- `DecodeSplitAcrossReads`:帧被拆成多次喂入,半包缓冲、齐了再出。
- `DecodeMultipleFramesOneRead`:粘包切多帧。
- `ResyncOnBadHeadFlag`:前面塞垃圾字节 → 扫描到下一个 head_flag 正常解出。
- `ResyncOnCrcMismatch`:CRC 不符 → 跳过该帧、不致命,后续好帧仍解出。
- `EncodeRejectsOversizePayload`:payload>65533 → `Fail("frame:")`。

### `ProtocolNode`(`tests/comm/protocol_node_test.cpp`,FakeTransport 双向回环 + InlineExecutor / ThreadExecutor)
- `NoResponseSends`:`SendNoResponse` 对端 `OnCommand` 收到 COMMAND。
- `NeedResponseCompletesOnResponse`:`Request` → 对端 `Responder.Response` → `on_response(Success)`,(session,message) 配对。
- `WithFeedbackCompletesOnResult`:`RequestWithResult` → 对端 `Responder.Result` → `on_result(Success)`。
- `NeedFeedbackResponseThenResultThenAck`:收 RESPONSE→收 RESULT→自动回 RESPONSE(对端可观察到 ack)→ `on_result(Success)`。
- `TimeoutRetransmitsUpToThreeThenFails`:对端不回 → 驱动定时器,观察重发 ≤3 次(可用计数 transport)后 `Fail("timeout:")`。
- `RepeatingSendsPeriodicallyUntilStopped`:`StartRepeating` → 驱动定时器发 N 帧 STATE → `StopRepeating` 后停。
- `HeartbeatPeriodic`:配 `heartbeat_interval_ms` → 定时发 HEARTBEAT;入站 HEARTBEAT → `OnHeartbeat`。
- `ReceiverRoleHandlesIncomingCommand`:作为接收方收 COMMAND 并回 RESPONSE/RESULT。
- `WorksWithThreadExecutor`:真实线程,`Request` future/promise 同步完成,非 flaky。

### 回归
- DDS 测试不变全绿;`comm_node_test`(改用 DdsCodec)全绿。

**完成标准:** 协议帧编解码/resync 全绿;5 种交互模式 + 重发(≤3)+ repeating + 心跳 + 双角色全绿;干净构建零告警;`ProtocolNode` 只依赖 `ITransport`/`ICodec`/`IExecutor` 接口。

---

## 11. 命名与待补

- `FrameType` 占位值,实现前替换为外部真实字节;`CrcFn` 注入,实现前提供真实 CRC16。
- `protocol_id` 按节点配置(对接的外部系统)。
- 后续:`noresponsewithcheck` 模式、多外部系统、QoS。
