# InteractionEngine + InteractionPolicy 设计(交互引擎抽象)

> 把当前 `CommNode`/`DdsNode`/`ProtocolNode` 各自重写的交互机制(挂起表/超时/重发/分发/定时/并发纪律)抽成**一份通用 `InteractionEngine`**;协议差异收进**声明式 `InteractionPolicy`**。`DdsNode`/`ProtocolNode` 在引擎上重新实现为薄壳;通用 corr+kind 的 **`CommNode` 移除**(不被实用)。

**动机:** `ProtocolNode.cpp` 与 `CommNode.cpp` 各自实现了同一套 io→Decode→Post→worker Dispatch、挂起表、超时、并发纪律(且各被评审抓出过一个并发 Critical:自连接、needfeedback 双触发)。三个真实交互节点 = 足够样本,合并机制为一份、最难写对的并发生命周期只写一处。

**配套:** 取代 `2026-06-17-commnode-layer-design.md` 的 CommNode 部分;DDS/协议节点行为不变(纯内部重构)。

---

## 1. 目标与范围

- **抽 `InteractionEngine`(通用机制,一份)+ `InteractionPolicy`(每协议声明式策略)。**
- **`DdsNode`/`ProtocolNode` 在引擎上重新实现**:`DdsNode` 公开 API 与行为**不变**(纯内部重构,`dds_node_test` 不改);`ProtocolNode` 行为不变,但发送方法**新增命令码参数**(纠正 `message_id`=命令码的语义,见 §7),`protocol_node_test` 据此小改。
- **移除 `CommNode`**(字符串 corr + 5 值 kind 的通用点对点节点不被实用);其 `comm_node_test` 转为引擎级测试(`InteractionEngine` + 测试 policy)。

**范围外(YAGNI):** 不为「不合 fire / request-await / periodic 三原语」的假想协议预留第四原语(届时再加);不引入抽象 `INode`;不动 transport / codec / DDS provider 层。

---

## 2. 三层归位(机制 / 策略 / 薄壳)

| 关注点 | 归属 | 说明 |
|---|---|---|
| io→Decode→Post→单 worker Dispatch、挂起表、超时、重发、periodic/心跳定时器、并发纪律 | **InteractionEngine** | 通用、一份、复用 |
| 匹配键取法、判别符读写、应答回填+寻址、无主入站路由、tag 配置 | **InteractionPolicy** | 每协议一个,**声明式纯函数 + 配置** |
| 命名模式(`SendNoResponse`/`Request`/…)、用户可重写钩子、DDS 订阅 | **节点薄壳** | 翻译成原语调用 + tag 常量;用户继承 |

**核心洞察:** DDS 与外部协议的全部模式都落在 **3 个引擎原语**上 —— `Fire` / `RequestAwait` / `StartPeriodic`(见 §6)。

---

## 3. 抽象类型

```cpp
namespace transport {
using Key = std::string;   // 统一匹配键(引擎不模板化);policy 把 (session,message) 打包成串,DDS 用 correlation_id
using FrameTag = int;      // 抽象判别符;policy 把自己的 frm_type/kind 枚举 cast 成 int;引擎只做相等比较,从不解释
}
```
**Key=string 取舍:** 每键一次小字符串分配 ≪ 网络 I/O,可忽略;换来引擎非模板化(普通虚接口、可拆 .cpp)、`pending_=map<string,Pending>` 一份。模板化 `Engine<Key>` 会让 policy 接口也带 Key 类型 → 模板传染、难拆 .cpp,不取。

---

## 4. `InteractionPolicy`(声明式策略接口)

```cpp
class InteractionPolicy {
 public:
  virtual ~InteractionPolicy() = default;

  // 判别符(读/写"这是哪类帧")
  virtual FrameTag TagOf(const Message& m) = 0;          // protocol→frm_type, dds→kind
  virtual void     SetTag(Message& m, FrameTag tag) = 0;

  // 匹配键 + 相关
  virtual Key  NewCorrelation(Message& out) = 0;          // 出站请求:盖全新相关号进 out,返回挂起 key
  virtual Key  KeyOf(const Message& in) = 0;              // 入站:取 key(须与应答的 NewCorrelation 返回相等)
  virtual void EchoCorrelation(Message& reply, const Message& request) = 0;  // 应答回填,使 KeyOf 命中

  // 应答寻址
  virtual Endpoint ReplyTo(const Message& request) = 0;   // dds→Topic(req.reply_to);protocol→Default

  // 无主入站路由(没配上任何挂起请求时)
  enum class Route { kInboundRequest, kDeliver, kDrop };
  virtual Route RouteUnmatched(const Message& in) = 0;
};
```

**policy 状态与线程:** policy 是有状态对象(持 session/message 计数器或 corr 序号、inbox topic、protocol_id),由节点造、移交引擎持有。变异方法(`NewCorrelation`)由引擎在 `mu_` 下调用(只盖字段+返回 key,无 I/O),其后 `Encode`+`Send` 在锁外 → policy 自身无需再加锁。

### 两个具体 policy

**`ProtocolPolicy`**(`SystemCodec` 帧;持 `protocol_id` + 滚动 `session_id`/`message_id` 计数器):
- `TagOf`=`(int)m.frm_type`;`SetTag`:`m.frm_type=(FrameType)tag`。
- `NewCorrelation(out)`:`out.session_id = session_ctr_++ (0–255 滚动); out.protocol_id = protocol_id_;` **不动 `out.message_id`** —— 它是**调用方已填的命令码**(每命令固定值,区分不同命令,非自增);`return pack(out.session_id, out.message_id)`(3 字节串)。
- `KeyOf(in)`=`pack(in.session_id, in.message_id)`;`EchoCorrelation`:拷 `session_id`+`message_id`+`protocol_id`。
- **匹配/并发:** `session_id` 滚动是实例唯一,故**同一命令并发 ≤256**;`message_id`(命令码)+`session_id` 合成键,不同命令各自独立。
- `ReplyTo`=`Endpoint::Default()`(同管道)。
- `RouteUnmatched`:`COMMAND`/`STATE`→`kInboundRequest`;`HEARTBEAT`→`kDeliver`;`RESPONSE`/`RESULT`(无挂起)→`kDrop`;`UNKNOWN`→`kDrop`。

**`DdsPolicy`**(`DdsCodec`;持 `inbox` topic + corr 序号):
- `TagOf`=`(int)m.kind`;`SetTag`:`m.kind=(MessageKind)tag`。
- `NewCorrelation(out)`:`out.correlation_id=next_corr(); out.reply_to=inbox_; return out.correlation_id`。
- `KeyOf(in)`=`in.correlation_id`;`EchoCorrelation`:拷 `correlation_id`(目的地由 `ReplyTo` 给)。
- `ReplyTo`=`in.reply_to.empty()?Default():Topic(in.reply_to)`。
- `RouteUnmatched`:`kRequest`→`kInboundRequest`;`kOneway`/`kNotify`→`kDeliver`;`kReply`/`kFeedback`(无挂起)→`kDrop`。

---

## 5. `RequestSpec`(request-await 配置)

```cpp
struct RequestSpec {
  FrameTag request_tag;                       // 出站请求帧 tag
  std::optional<FrameTag> intermediate_tag;   // 推进但不终结(needfeedback 的 RESPONSE / DDS kFeedback)
  FrameTag terminal_tag;                       // 终结帧
  std::optional<FrameTag> auto_ack_tag;        // 非空:终结时自动回一帧此 tag(EchoCorrelation 回填,ReplyTo 寻址)
  std::function<void(const Message&)> on_intermediate;   // 每中间帧(可空)
  std::function<void(Result<Message>)> on_terminal;      // 终结/超时失败,恰好一次
  uint32_t timeout_ms;
  uint32_t max_retries;                        // 超时重发请求帧上限
};
```
**重发规则(消除 god-config):** 挂起项记 `bool advanced`。超时:`if (!advanced && retries < max_retries)` 重发请求帧(同相关号)+重排定时器、`++retries`;否则 `on_terminal(Fail("timeout:..."))` + erase。**首个配上的帧(中间或终结)置 `advanced=true`** → 此后超时只失败不重发。一条规则覆盖 needresponse/withfeedback(首帧即终结)与 needfeedback(首帧中间 RESPONSE 停重发、续等 RESULT),无协议专属 flag。

---

## 6. `InteractionEngine`(通用机制)

```cpp
class InteractionEngine : public std::enable_shared_from_this<InteractionEngine> {
 public:
  InteractionEngine(std::shared_ptr<ITransport>, std::unique_ptr<ICodec>,
                    std::unique_ptr<InteractionPolicy>,
                    std::unique_ptr<IExecutor> = nullptr, std::size_t queue_capacity = 1024);
  Status Open();  void Close();  bool IsOpen() const;

  void OnInboundRequest(std::function<void(const Message&)>);  // RouteUnmatched==kInboundRequest
  void OnInboundDeliver(std::function<void(const Message&)>);  // ==kDeliver(节点内按 tag 再分)
  void OnError(std::function<void(const std::string&)>);

  // 原语收 Message(调用方预填语义字段:payload,以及协议需要的如 message_id 命令码);
  // 引擎只补 tag + 经 policy 补滚动相关号。
  Status   Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  Status   RequestAwait(Message out, RequestSpec spec, const Endpoint& to = Endpoint::Default());
  uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms, const Endpoint& to = Endpoint::Default());
  void     StopPeriodic(uint32_t handle);

  Status   SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload);  // 供节点 Responder
};
```

**三原语(`out` 为调用方预填的 Message):**
- `Fire(out, tag, to)`:`policy_->SetTag(out, tag); Encode; transport_->Send(bytes, to)`。无追踪。
- `RequestAwait(out, spec, to)`:`policy_->SetTag(out, spec.request_tag);` 在 `mu_` 下 `Key k=policy_->NewCorrelation(out); pending_[k]=Pending{spec, out(留作重发), timer=ScheduleAt(超时,FireTimeout(k)), advanced=false};` 锁外 `Encode`+`Send(out_bytes, to)`(失败回滚+`on_terminal(Fail)`)。
- `StartPeriodic(out, tag, interval, to)`:登记 handle,立即 `Fire(out, tag, to)` 一帧并排自重排定时器;`StopPeriodic` 取消。(repeating 与心跳共用)

**入站 Dispatch(单 worker):**
```
m = decode 出的一条;m.source=from; if m.topic.empty(): m.topic=from
k = policy_->KeyOf(m)
若 pending_ 命中 p:
   tag = policy_->TagOf(m)
   if tag == p.spec.terminal_tag:
        p.advanced=true; cancel timer; erase
        if p.spec.auto_ack_tag: SendReply(m, *auto_ack_tag, {})   // 锁外
        p.spec.on_terminal(Success(m))                            // 移出回调防双触发
   elif p.spec.intermediate_tag && tag == *p.spec.intermediate_tag:
        p.advanced=true; (留挂起) p.spec.on_intermediate(m)        // 拷贝,不移出
   else: 忽略(对该挂起意外的 tag)
否则按 policy_->RouteUnmatched(m):
   kInboundRequest → OnInboundRequest 钩子(节点据 m 造 Responder 调用户 OnRequest/OnCommand)
   kDeliver        → OnInboundDeliver 钩子(节点据 tag 再分 OnMessage/OnHeartbeat)
   kDrop           → 忽略
```

**`SendReply(request, tag, payload)`:** `Message m; m.payload=payload; policy_->SetTag(m, tag); policy_->EchoCorrelation(m, request); Endpoint to=policy_->ReplyTo(request); Encode; transport_->Send(bytes, to)`。

**并发纪律(只写这一处):** posted 任务/transport 回调捕获 `weak_ptr`;挂起表/计数器/periodic 表由 `mu_` 保护,`Encode`+`Send`(含 auto_ack、periodic、SendReply)一律锁外;终结/超时同在单 worker → 恰好一次;中间帧 `on_intermediate` 拷贝调用、终结 `on_terminal` 移出调用 → 防 `Close`/断连双触发;`open_` 在 `transport_->Open()` 前置位+失败回滚;`Close` 幂等,先终结全部挂起(`conn:`)+取消全部定时器(在 `mu_` 下)→ `executor_->Stop()` → `transport_->Close()`;`ThreadExecutor::Stop` 自连接守卫。

---

## 7. 节点薄壳(用户继承面,公开 API 不变)

**`ProtocolNode`**(持 `engine_` + `ProtocolConfig`;codec 默认 `SystemCodec`,policy=`ProtocolPolicy(protocol_id)`)。发送方法**新增命令码 `cmd`(=`message_id`)参数**;记 `Msg(cmd,p)` = `Message m; m.message_id=cmd; m.payload=p; m`:
- `SendNoResponse(uint16_t cmd, p)` = `Fire(Msg(cmd,p), COMMAND)`;
- `Request(uint16_t cmd, p, cb)` = `RequestAwait(Msg(cmd,p), {request=COMMAND, terminal=RESPONSE, on_terminal=cb, timeout, max=cfg.max_retries})`;
- `RequestWithResult(cmd,p,cb)` → `terminal=RESULT`;`RequestNeedFeedback(cmd,p,…)` → `intermediate=RESPONSE, terminal=RESULT, auto_ack=RESPONSE`;
- `StartRepeating(uint16_t cmd, p, iv)`=`StartPeriodic(Msg(cmd,p), STATE, iv)`;`StopRepeating`=`StopPeriodic`;
- > **API 纠正:** 原 `ProtocolNode` 自增 `message_id` 的语义不符实际(`message_id` 实为命令码);新增 `cmd` 参数,`protocol_node_test` 据此小改(非"一字不改")。
- 嵌套 `Responder{ Response(p)=SendReply(req,RESPONSE,p); Result(p)=SendReply(req,RESULT,p); }`;
- 钩子 `OnCommand(msg,Responder)`/`OnHeartbeat(msg)`/`OnError`;`Open()` 接钩子(`OnInboundRequest`→造 Responder 调 `OnCommand`;`OnInboundDeliver`→按 `frm_type` 分 `OnHeartbeat`)+ `engine_->Open()` + 若 `heartbeat_interval_ms>0` `StartPeriodic(HEARTBEAT,hb)`。

**`DdsNode`**(另持 `shared_ptr<IDdsTransport> dds_` 供订阅;policy=`DdsPolicy(inbox)`,codec 默认 `DdsCodec`):
- `Subscribe(t)`/`Unsubscribe(t)` = `dds_->Subscribe/Unsubscribe`;
- `Send(msg, Endpoint::Topic(t))` = `Fire(msg, kNotify, Topic(t))`(发布);
- `Request(msg, cb, timeout, Endpoint::Topic(req_t))` = `RequestAwait(msg, {request=kRequest, terminal=kReply, on_terminal=cb, timeout, max=0}, Topic(req_t))`;反馈重载 +`intermediate=kFeedback`;
- 嵌套 `Responder{ Reply(p)=SendReply(req,kReply,p); Feedback(p)=SendReply(req,kFeedback,p); }`;
- 钩子 `OnMessage`/`OnRequest`;`Open()` 接钩子 + `engine_->Open()` + `dds_->Subscribe(inbox)`(reply_to=inbox 由 `DdsPolicy.NewCorrelation` 每请求盖)。

> 嵌套 `Responder`(`ProtocolNode::Responder` / `DdsNode::Responder`)避免与彼此/历史 `transport::Responder` 的 ODR 冲突。

---

## 8. 文件结构

**新建:**
- `include/transport/comm/InteractionPolicy.hpp`(`Key`/`FrameTag`/`InteractionPolicy`/`RequestSpec`)。
- `include/transport/comm/InteractionEngine.hpp` + `src/comm/InteractionEngine.cpp`。
- `include/transport/comm/ProtocolPolicy.hpp` + `src/comm/ProtocolPolicy.cpp`(或 header-only)。
- `include/transport/comm/DdsPolicy.hpp` + `src/comm/DdsPolicy.cpp`。
- `tests/comm/interaction_engine_test.cpp`(引擎 + 测试 policy)。

**改写:**
- `include/transport/comm/ProtocolNode.hpp` + `src/comm/ProtocolNode.cpp`(改为持 `engine_`+`ProtocolPolicy` 的薄壳;公开 API 与测试不变)。
- `include/transport/comm/DdsNode.hpp` + `src/comm/DdsNode.cpp`(同上;不再继承 CommNode)。
- `CMakeLists.txt`。

**移除:**
- `include/transport/comm/CommNode.hpp` + `src/comm/CommNode.cpp`;`tests/comm/comm_node_test.cpp`(其覆盖转入 `interaction_engine_test.cpp`)。

**不动:** transport / codec(`SystemCodec`/`DdsCodec`/…)/ DDS provider / `IExecutor`/`ThreadExecutor` / `Message`(协议字段已在)。

---

## 9. 错误处理 / 并发
同 §6:不抛异常,`Result<T>`/`Status`(`[[nodiscard]]`),前缀 `config:`/`timeout:`/`conn:`/`codec:`/`frame:`。并发纪律集中在引擎一份;policy 声明式无并发面;节点薄壳无挂起/定时逻辑。

---

## 10. 测试(TDD)

- **`InteractionEngine`(`interaction_engine_test.cpp`,`FakeTransport`+`InlineExecutor`+一个简单测试 policy)**:`Fire` 交付;`RequestAwait` 终结完成;中间帧推进保留挂起;`auto_ack` 终结时自动回;超时重发 ≤max 后失败、首帧停重发;`StartPeriodic`/`StopPeriodic`;`RouteUnmatched` 三路;`Close` 终结挂起不双触发;`WorksWithThreadExecutor`。**覆盖原 comm_node_test 的通用交互面。**
- **`ProtocolNode`**:`protocol_node_test.cpp` **小改**——发送调用补命令码 `cmd` 参数(纠正 `message_id`=命令码语义);其余断言不变,全绿。
- **`DdsNode`**:现有 `dds_node_test.cpp` **不改**,全绿(纯内部重构验证)。
- **回归**:全量两次连跑、干净构建零告警;移除 `comm_node_test` 后总数相应变化,以 0 failed + 稳定为准。

**完成标准:** `InteractionEngine`+两 policy 落地;`DdsNode` 改为薄壳且 `dds_node_test` **一字不改保持绿**;`ProtocolNode` 改为薄壳,`protocol_node_test` 仅补命令码参数、其余不变保持绿;`CommNode` 移除;引擎里无 per-node 特判(失败信号);并发纪律仅一处。

---

## 11. 设计依据 / 护栏
- **机制集中、策略声明式**:最难写对、出过 Critical 的并发生命周期只写一处复用;policy 退成纯函数+配置,易写对。
- **首帧停重发**一条规则覆盖三模式,免 god-config。
- **护栏**:若引擎 `RequestAwait`/Dispatch 长出 per-node 特判(`if dds…/if protocol…`),即缝切错的信号 → 停手,退回「policy 自写状态机」思路。
- **范围**:纯内部重构,DDS/Protocol 公开 API 与现有测试不变;新协议 = 新 policy;不合三原语的协议 = 届时加第四原语。
