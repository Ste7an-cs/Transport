# 可插拔结构化 Trace（InteractionEngine 咽喉点）— 设计

**日期：** 2026-06-26
**状态：** 设计已确认,待写 plan
**配套：**《设计说明书》§7、`docs/superpowers/specs/2026-06-24-interaction-engine-design.md`

## 1. 背景与目标

当前库零日志:唯一可观测点是 `InteractionEngine` 的单个 `on_error_(std::string)` 回调。引擎里最难调的行为——分发决策(命中终结/中间/无主路由)、超时、重发、auto-ack、periodic、关闭终结、解码成败——全程不可见,出问题只能盲调。

**目标:** 一套**可插拔、结构化、近零开销**的 trace 机制,同时服务两类用途:
- **开发期**:库作者排查中间件自身行为(挂一个 stderr sink 看全量协作流)。
- **集成期**:库使用者把 trace 接到自己的日志后端(注入自定义 sink)。

**非目标(YAGNI):** 全局 logger、宏式编译期日志、采样/限流、trace-id 传播、本版不改 codec/transport(见 §7 预留)。

## 2. 关键决策（来自 brainstorming)

| 决策 | 选择 | 理由 |
|---|---|---|
| 形态 | 可注入 sink 接口(非全局/非宏) | 与现有 `ITransport`/`ICodec`/`IExecutor` 依赖注入风格同构;集成方可换后端 |
| 事件结构 | 分级 + 短文本 + 少量结构字段 | 机器可过滤又不臃肿;多路 req-resp/重发调试最需要按 key 过滤 |
| 覆盖范围 | 仅 `InteractionEngine` 咽喉点 | 引擎已注册 OnBytes/OnConnect/OnDisconnect 且亲自调 Encode/Decode → 一处注入拿到 ~90% 价值,transport/codec 零改动 |
| 开销 | 无 sink 时一次空指针分支 | 生产默认不挂 sink ≈ 免费;事件用 string_view+int 视图结构,零分配 |

## 3. sink 接口（新文件 `include/transport/ITraceSink.hpp`,header-only,层中立)

放在 `include/transport/` 根(与 `ITransport.hpp` 同级),**不放 `comm/`** —— 使将来 codec/transport 采用它时不必反向依赖 comm 层(见 §7)。

```cpp
namespace transport {

enum class TraceLevel { kTrace, kDebug, kInfo, kWarn, kError };

inline constexpr int  kNoTag  = -9999;   // TraceEvent.tag 哨兵:无判别符
inline constexpr long kNoNum  = -1;      // size/attempt 哨兵:无该数值

// 轻量"视图"事件:全部 string_view + int,构造零分配。
// string_view 指向调用点已存在的数据(引擎的 Key、错误串、静态字面量);
// sink 若需留存须自行拷贝。
struct TraceEvent {
  TraceLevel       level;
  std::string_view category;   // 静态字面量,见 §5 目录
  std::string_view message;    // 短静态子原因(可空),如 "match-terminal"
  std::string_view key;        // 相关键(可空)
  std::string_view endpoint;   // endpoint / from / route 名(可空)
  std::string_view error;      // 错误串,前缀分类(可空)
  int  tag     = kNoTag;       // FrameTag
  long size    = kNoNum;       // 字节数,或计数(decode=条数 / close=终结挂起数);按 category 解释
  int  attempt = -1;           // 重发第几次(-1=无)
};

// 实现可能被 io 线程、worker 线程、调用方线程**并发**调用 → 必须线程安全。
class ITraceSink {
 public:
  virtual ~ITraceSink() = default;
  virtual void OnTrace(const TraceEvent& ev) = 0;
};

}  // namespace transport
```

**开销契约:** 每个埋点形如 `if (trace_) trace_->OnTrace({...});`。级别过滤是 sink 的职责(事件已极廉价,不在引擎侧做 virtual 级别门控以保持埋点代码简洁)。无 sink = 单分支。

## 4. 内置 sink（同文件,header-only)

- **`OstreamTraceSink`** —— 开发调试用。
  - 构造 `OstreamTraceSink(std::ostream& os = std::cerr, TraceLevel min = TraceLevel::kDebug)`。
  - `OnTrace`:`level < min` 直接 return;否则在内部 `std::mutex` 保护下格式化成单行,仅打非空/非哨兵字段:
    `[D] dispatch match-terminal key=01.0005 tag=3`
  - level 字母:T/D/I/W/E。
- **`CapturingTraceSink`** —— 测试/内省用。
  - `OnTrace` 在 mutex 下把事件**深拷贝**(string_view→string)进 `std::vector<Record>`;`Records()` 取回快照、`Clear()` 清空。
  - 供 TDD 断言事件目录。

## 5. 事件目录（引擎埋点;数据均为引擎在该点已持有)

| category | message(子原因) | level | 触发点 | 字段 |
|---|---|---|---|---|
| `conn` | `connect` / `disconnect` | Info | transport OnConnect/OnDisconnect 回调 | endpoint=from |
| `open` | — | Info | `Open()` 成功 | — |
| `close` | — | Info | `Close()` | size=终结的挂起数 |
| `send` | — | Debug | `Fire`(单向) | tag,size=字节,endpoint |
| `request` | — | Debug | `RequestAwait` 登记后 | key,tag,size=字节,endpoint |
| `reply` | — | Debug | `SendReply` | key,tag,size=字节 |
| `periodic` | `start`/`fire`/`stop` | Trace | StartPeriodic/FirePeriodic/StopPeriodic | tag |
| `recv` | — | Trace | `OnBytes` 收到 | size=字节,endpoint=from |
| `decode` | — | Trace | `Decode` 成功 | size=消息条数 |
| `decode` | `decode-fail` | Warn | `Decode` 返回错误 | error |
| `dispatch` | `match-terminal` | Debug | 命中挂起、终结帧 | key,tag |
| `dispatch` | `match-intermediate` | Debug | 命中挂起、中间帧 | key,tag |
| `dispatch` | `auto-ack` | Trace | 终结后自动回 ack | key,tag |
| `unmatched` | `request`/`deliver`/`drop` | Trace | 无主入站经 RouteUnmatched | tag,endpoint=route |
| `retransmit` | — | Debug | 超时且未推进、未达上限 | key,attempt |
| `timeout` | — | Warn | 超时失败(达上限/已推进) | key |
| `error` | — | Error | 现有 `on_error_` 触发处 | error |

> `error` category 与现有 `on_error_` 回调**并存**:on_error_ 是用户业务回调(不变),trace 是观测面;同一错误两处都发。

## 6. 注入与穿透

- `InteractionEngine`:加私有成员 `std::shared_ptr<ITraceSink> trace_;` + 公有 `void SetTrace(std::shared_ptr<ITraceSink> t) { trace_ = std::move(t); }`。
  - **契约:Open() 之前调用**(设置期单线程,无数据竞争);Open 后埋点在多线程读 `trace_`,但指针在 Open 前已固定,只读安全。
  - 一个私有内联辅助 `void Trace(const TraceEvent&) const`(封 `if (trace_) ...`)让埋点处一行。
- `DdsNode` / `ProtocolNode`:各加一行透传 `void SetTrace(std::shared_ptr<ITraceSink> t) { engine_->SetTrace(std::move(t)); }`。**构造签名不变**(零破坏)。

**用法:**
```cpp
auto link = std::make_shared<Link>(transport, nullptr, cfg);
link->SetTrace(std::make_shared<OstreamTraceSink>(std::cerr, TraceLevel::kTrace));
link->Open();   // 此后全量协作流打到 stderr
```

## 7. 预留口子（codec / transport 后续采用,本版不实现）

trace 词汇表设计为**层中立**,为后续扩展预留,但本版**不改** codec/transport:

- **位置中立**:`ITraceSink.hpp` 在 `include/transport/` 根,codec/transport 将来可直接依赖,不引入对 comm 层的反向依赖。
- **保留 category**(本版不发,文档登记,供后续直接用,无需改 `ITraceSink`/`TraceEvent`):
  - `resync` —— codec 坏帧重同步;size=跳过字节数。
  - `frame-drop` —— codec CRC/帧长不符丢帧;error=原因。
  - `io` —— transport 裸读写错误;error=原因,endpoint=对端。
  - `reconnect` —— transport 重连尝试;attempt=第几次。
- **采用方式(将来)**:给需要的具体 codec/transport 加一个**可选** `std::shared_ptr<ITraceSink>` 构造参数,在其内部发上述 category。`ITraceSink`、`TraceEvent`、`InteractionEngine` **均不改**。

## 8. 文件与测试

**新增**
- `include/transport/ITraceSink.hpp` —— `TraceLevel`、`TraceEvent`、`ITraceSink`、`OstreamTraceSink`、`CapturingTraceSink`(全 header-only)。
- `tests/comm/interaction_trace_test.cpp` —— 用 `CapturingTraceSink` + 现有 `TestPolicy`/`FakeTransport`/`InlineExecutor` 断言事件目录:
  - Fire → 一条 `send`。
  - RequestAwait 命中终结 → `request` 后 `dispatch/match-terminal`。
  - 中间+终结 → `match-intermediate` 后 `match-terminal`(+auto-ack)。
  - 超时 → `retransmit`×N 后 `timeout`。
  - Decode 失败 → `decode/decode-fail`(Warn)。
  - Close 有挂起 → `close` 带 size=终结数。
  - 无 sink(trace_ 空)→ 不崩、零调用(默认路径)。

**修改**
- `include/transport/comm/InteractionEngine.hpp` + `src/comm/InteractionEngine.cpp` —— 成员 + `SetTrace` + `Trace()` 辅助 + §5 埋点。
- `include/transport/comm/DdsNode.hpp`、`include/transport/comm/ProtocolNode.hpp` —— 各透传一行 `SetTrace`。

**不回归:** 现有 86 测试不动、全绿;无 sink 时行为与字节流逐字不变。

## 9. 约束

- C++17,不抛异常,延续 `Result`/`Status` 风格(trace 不改错误模型,只观测)。
- header-only sink;接口层零第三方依赖。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,无 Co-Authored-By。
- 文档(SRS/SDD/README/CHANGELOG)同步留到实现后单独一轮。
