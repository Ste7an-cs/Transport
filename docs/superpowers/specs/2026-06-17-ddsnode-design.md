# DdsNode 设计(DDS pub-sub + 多路 req-resp 节点)

> 三层架构最上层(`comm/`)的 DDS 节点:**`DdsNode : CommNode`** —— 复用 `CommNode` 的全部交互引擎(请求-应答 / 结果反馈 / 单向 / 超时 / 挂起表),只**加性**补上 DDS 独有的订阅能力与基于 topic 的应答路由。对称于 Layer 1 的 `IDdsTransport : ITransport (+Subscribe)`。

**配套底层 spec:** `IDdsTransport`/`DdsTransport`/`IDdsProvider`(已落地);`IExecutor`/`ThreadExecutor` 与 `CommNode` 见 `2026-06-17-commnode-layer-design.md`。

---

## 1. 目标与范围

一个 `DdsNode` 同时支持:
- **发布**到一个或多个 topic(复用 `Send` + `Endpoint::Topic`,**不新增 Publish 方法**)。
- **订阅**一个或多个 topic(`Subscribe`/`Unsubscribe`,DDS 独有能力)。
- **一路或多路请求-应答**(复用 `CommNode` 的 `correlation_id` 挂起表 + 超时;应答经 `reply_to` topic 精确回送发起方)。
- 单向 / 结果反馈(`kFeedback`)等交互模式 —— 全部继承自 `CommNode`。

**关键设计取舍(已定):**
- **不引入抽象 `INode` 接口**:`DdsNode : CommNode`,`CommNode` 自然是公共基类型。
- **不引入工厂**(YAGNI):手工装配 `transport + codec + node` 即可;真有配置驱动需求时后续再加薄 `Build(config)→{transport,codec}`,且只装配底层、不接管 node 消息入口。
- **保留 `CommNode` 继承模型**:用户继承 `DdsNode` 重写 `OnMessage`/`OnRequest`;`CommNode` 入口不改回调,只做**加性泛化**。
- **DDS codec 无状态**:DDS 每个 sample 即一条完整消息,无需滚动缓冲;故并发多 topic 解码天然安全,**无需每 topic 一份 codec、无需 `CommNode` 解码缝**。

**范围外(YAGNI):** QoS 细配、topic 通配/分区、pull 接收、请求取消、`ServerNode`(TCP 服务端多连接)、配置驱动工厂、抽象 `INode`。不引入新第三方依赖。

---

## 2. 架构与复用

`DdsNode` 持有一个 `shared_ptr<IDdsTransport>`(既作为 `CommNode` 的 `ITransport`,又保留带订阅能力的类型用于 `Subscribe`)。`CommNode` 既有管道照常工作:io/listener 线程 `Decode` → `executor->Post` → 单 worker 串行 `Dispatch`(按 `kind` 路由)。`DdsNode` 不重写这条管道,只:
1. 提供 `Subscribe`/`Unsubscribe`(转发给 `IDdsTransport`)。
2. `Open()` 时把自身 **inbox topic** 自动订阅,并把 `CommNode` 的"应答地址"设为该 inbox topic(使继承来的 `Request` 出站时自动带 `reply_to`)。

须以 `shared_ptr` 持有(posted 任务 / transport 回调捕获 `weak_ptr`)。

---

## 3. CommNode 的加性泛化(p2p 行为不变)

为让 DDS 多 topic / 多对等体下的 req-resp **复用 `CommNode` 而不改交互逻辑**,对 `CommNode` 做三处加性修改;p2p 路径全程 `reply_to` 为空、目的 `Endpoint` 默认 `Default()` → **现有行为零变化**。

### 3.1 `Message` 加 `reply_to`
```cpp
struct Message {
  std::vector<uint8_t> payload;
  std::string topic;
  std::string source;
  int64_t     timestamp = 0;
  MessageKind kind = MessageKind::kOneway;
  std::string correlation_id;
  std::string reply_to;   // 新增:请求方期望收应答的目的(DDS=topic);非 DDS 留空
};
```
`SystemCodec` 不动(p2p 应答走原双向管道,无需该字段);仅 `DdsCodec` 携带它。

### 3.2 `Request` 加可选目的 `Endpoint`,出站填 `reply_to`
```cpp
// CommNode —— 与 Send 对称地接收目的地;新增 protected 应答地址。
Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms,
               const Endpoint& to = Endpoint::Default());
Status Request(Message msg, FeedbackFn on_fb, ReplyFn on_final, uint32_t timeout_ms,
               const Endpoint& to = Endpoint::Default());
std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms,
               const Endpoint& to = Endpoint::Default());
protected:
  std::string reply_address_;   // 默认空(p2p);DdsNode 设为自己的 inbox topic
```
`RequestImpl` 在编码前设 `msg.reply_to = reply_address_`,并把请求 `Send` 到传入的 `to`(原先固定 `transport_->Send(bytes)` 无目的 → 改为 `transport_->Send(bytes, to)`,`Default()` 时等价原行为)。

### 3.3 `Responder.Reply`/`Feedback` 按 `reply_to` 回送
`Dispatch` 处理 `kRequest` 时,用解码出的 `msg.reply_to` 构造 `Responder` 的目的地:
```cpp
Endpoint to = msg.reply_to.empty() ? Endpoint::Default()
                                   : Endpoint::Topic(msg.reply_to);
OnRequest(msg, Responder(weak_from_this(), msg.correlation_id, to));
```
`Responder` 现有的 `to_` 字段(此前恒为 `Default()`)正好承接;`Reply`/`Feedback` 经 `SendKind(..., to_)` 回送 → DDS 下即发布到发起方 inbox topic,p2p 下即原双向管道。

---

## 4. 新增 `DdsCodec`(无状态、带元数据)

```
线缆格式(单 sample = 单消息,无长度前缀):
  [kind:1][corr_len:2 BE][corr_id][reply_len:2 BE][reply_to][payload]
```
- `Encode(msg)` → 上述字节(一对一)。
- `Decode(data, len)` → **恰好一条** `Message`(整段即一条;`len==0` 返回空)。无成员缓冲、无状态 → 多 topic 并发解码安全。
- 不携带 `topic`:接收侧 `topic`/`source` 由 DDS 投递的 `from`(来源 topic)填,见 §5。
- 解析越界 → `Fail("codec: ...")`。

`include/transport/codec/DdsCodec.hpp`(可 header-only,仿 `DatagramCodec`)。

---

## 5. 接口与数据流

### 5.1 DdsNode 接口
```cpp
namespace transport {

class DdsNode : public CommNode {
 public:
  // inbox: 本节点接收应答/点对点回送的 topic(唯一);codec 默认 DdsCodec。
  DdsNode(std::shared_ptr<IDdsTransport> transport,
          std::string inbox_topic,
          std::unique_ptr<ICodec> codec = nullptr,         // null → DdsCodec
          std::unique_ptr<IExecutor> executor = nullptr,   // null → ThreadExecutor
          std::size_t queue_capacity = 1024);

  Status Subscribe(const std::string& topic);     // DDS 独有能力
  Status Unsubscribe(const std::string& topic);

  // 继承自 CommNode 并直接可用:
  //   Send(msg, Endpoint::Topic(t))                 —— 发布到 t
  //   Request(msg, cb/future, timeout, Endpoint::Topic(req_t)) —— 向 req_t 发请求
  //   OnMessage / OnRequest+Responder               —— 用户重写

 private:
  std::shared_ptr<IDdsTransport> dds_;
  std::string inbox_topic_;
};

}  // namespace transport
```

### 5.2 发布 / 订阅
- **发布**:`Send(msg, Endpoint::Topic("t"))` → `CommNode` 编码 → `DdsTransport::Send(bytes, Topic)` → `provider.Publish("t", ...)`。
- **订阅**:`Subscribe("t")` → `dds_->Subscribe("t")` → 样本到达 → `OnBytes(bytes, from="t")`(listener 线程)→ `CommNode` `DdsCodec.Decode` → 设 `m.topic = m.source = from` → `Post` → 单 worker `OnMessage(m)`。

### 5.3 多路请求-应答(reply_to 精确回送)
```
节点 A(inbox="A_in"),节点 B(inbox="B_in"),业务请求 topic "svc"
A.Open → Subscribe("A_in"); reply_address_="A_in"
B.Open → Subscribe("B_in"); Subscribe("svc")

A.Request(msg, cb, 1000, Endpoint::Topic("svc"))
  └ msg.reply_to="A_in", kind=kRequest, corr=唯一 → publish "svc"
       └ B.Subscribe("svc") 收 → Decode → Dispatch(kRequest)
            → OnRequest(req, Responder{to=Topic("A_in")})
                 └ Responder.Reply(rep) → publish "A_in"
                      └ A.Subscribe("A_in") 收 → Dispatch(kReply) → corr 配对 → cb(rep) ✓
```
多个并发请求(不同 corr)、跨多个请求 topic、跨多个对等体 —— 全由 `CommNode` 的 `pending_`(corr→挂起)承载,应答经各自 `reply_to` 回各自 inbox,不串台。无关应答(corr 未命中)由 `Dispatch` 既有逻辑丢弃。

### 5.4 生命周期
- `DdsNode::Open()`:先 `CommNode::Open()`(注册回调 + `executor.Start` + `transport.Open`);成功后 `Subscribe(inbox_topic_)` 并设 `reply_address_ = inbox_topic_`。失败回滚同 `CommNode`。
- `Close()`:继承 `CommNode::Close()`(终结挂起 → `executor.Stop` → `transport.Close`)。

---

## 6. 错误处理

- 未 Open 时 `Send`/`Request` → `config: node not open`(继承)。
- `DdsCodec.Decode` 失败(`codec:` 前缀):**丢弃该样本 + `OnError`,继续** —— DDS 各 topic 独立、每样本自成完整帧,单坏样本不致命,也无"连接"可拆(区别于 `CommNode` 流式 `frame:` → 终结)。需要 `CommNode` 的入站解码错误分流已按前缀区分(`frame:` 终结 / `codec:` 继续);DDS 只产 `codec:` → 自然走"继续"。
- 全程不抛异常;`Result<T>`/`Status`,`[[nodiscard]]`。

---

## 7. 并发要点

- DDS codec 无状态 → 并发 listener 线程同时 `Decode` 安全(无共享缓冲)。
- 入站 `Decode` 在 listener 线程(跨 topic 并发),`OnMessage`/`Dispatch` 在单 worker(串行);背压点在 `executor->Post`。
- posted 任务 / transport 回调捕获 `weak_ptr`;`Close` 先终结挂起再停执行器再关传输(继承 `CommNode` 既有顺序)。
- `reply_to` 经线缆传递,应答路由无需节点间共享状态。

---

## 8. 文件结构

**新建:**
- `include/transport/codec/DdsCodec.hpp`(无状态 codec,可 header-only)。
- `include/transport/comm/DdsNode.hpp` + `src/comm/DdsNode.cpp`。
- `tests/comm/dds_node_test.cpp`、`tests/codec/dds_codec_test.cpp`。

**修改:**
- `include/transport/Message.hpp`(加 `reply_to`)。
- `include/transport/comm/CommNode.hpp` + `src/comm/CommNode.cpp`(`Request` 目的 `Endpoint` + `reply_address_` + 出站填 `reply_to` + `Responder` 按 `reply_to` 路由)。
- `CMakeLists.txt`(加 `src/comm/DdsNode.cpp`、新测试)。

**复用(不动):** `IDdsTransport`/`DdsTransport`/`FakeDdsProvider`、`IExecutor`/`ThreadExecutor`、`SystemCodec`、`Endpoint`、`tests/comm/inline_executor.hpp`。

---

## 9. 测试(TDD)

**`DdsCodec`(`tests/codec/dds_codec_test.cpp`):**
- `EncodeDecodeRoundtripCarriesMetadata`:kind/corr/reply_to/payload 往返一致。
- `DecodeWholeBufferAsOneMessage`、`DecodeEmptyYieldsNone`、`DecodeTruncatedFailsCodecPrefix`。
- `StatelessConcurrentDecodeSafe`:同一 codec 实例多线程 `Decode` 不同 sample,结果正确(无数据竞争)。

**`DdsNode`(`tests/comm/dds_node_test.cpp`,`FakeDdsProvider` 共享 `Bus` DI + `InlineExecutor` 确定性):**
- `PublishSubscribeRoundtrip`:A 订 `T`、B `Send(Endpoint::Topic("T"))` → A `OnMessage` 收到,`topic=="T"`。
- `MultiTopicPublishSubscribe`:订 `T1`/`T2` 分流正确;未订阅 topic 不投递。
- `UnsubscribeStopsDelivery`。
- `RequestReplyOverTopics`:A `Request(..., Endpoint::Topic("svc"))`、B `OnRequest`→`Reply` → 经 `reply_to` 回 A inbox,`cb` 命中。
- `ConcurrentMultiRouteRequests`:A 同时对多个/同一 svc 发多请求(不同 corr),各自应答正确配对。
- `FeedbackThenFinalOverTopics`:中间 `kFeedback` 多次 + 终结 `kReply` 经 inbox 回送。
- `RequestTimeoutOverTopics`:服务端不应答 → `timeout:`。
- `WorksWithThreadExecutor`:真实线程,`promise/future` 同步(非 sleep)。
- 可选 `FastDdsIntegration`:`provider="fastdds"`,participant 不可用则 `GTEST_SKIP`(仿 `tests/dds/fastdds_provider_test.cpp`)。

**回归:** 既有 58 测试全绿(`CommNode` 加性泛化不破坏 p2p:`reply_to` 空 / 目的 `Default()`)。

**完成标准:** pub-sub 多 topic 收发分流、多路 req-resp 经 reply_to 精确回送、退订生效、超时、可换执行器全绿;干净构建零告警;comm 层仍只依赖 `IDdsTransport`/`ICodec`/`IExecutor` 接口。

---

## 10. 命名备注

- `DdsNode : CommNode`,`comm/` 目录;`CommNode` 为公共基类型(无抽象 `INode`)。
- `DdsCodec`:无状态、带交互元数据的 DDS 线缆格式;`Message.reply_to`:topic-based 应答路由地址。
- **发布即 `Send(Endpoint::Topic)`**、**请求即 `Request(..., Endpoint::Topic)`** —— 复用既有寻址,不增冗余方法。
- 后续:配置驱动 `Build(config)`、`ServerNode`(TCP 服务端多连接)。
