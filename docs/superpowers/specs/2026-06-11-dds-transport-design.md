# DDS 传输 — 实现设计 spec

> 本 spec 是主设计文档 `2026-06-09-transport-middleware-design.md` §7（DDS 传输）的**实现层细化**。主 spec 已锁定公共 API 与机制（`RawMessage` 承载类与 wire layout、`IDdsTransport`、req-resp = pub-sub + 关联 id、`IDdsProvider`/`DdsProviderRegistry`）；本 spec 锁定实现细节：Fast DDS 版本与依赖策略、类分解与职责边界、`DdsQos`（替代 `qos_profile` 字符串）、req-resp 超时机制、线程模型、分阶段测试策略，供后续 plan 直接落地。
>
> **本 spec 同步修改主 spec 两处：** §7.1 `qos_profile: string` → `DdsQos` 结构体；§12 Fast DDS 版本 3.6 → 2.13+（理由见 §2）。

**Goal:** 在 Foundation 层（`TransportCore` + `ReceiveQueue`）之上实现 DDS 传输：provider 无关的 `DdsImpl`（pub-sub 多 topic 路由 + req-resp 关联/超时，全部业务逻辑对 `FakeDdsProvider` 单测）+ `FastDdsProvider`（Fast DDS 2.13，自定义 `TopicDataType` 绕过 IDL/CDR）+ 真实互通集成测试。

**Tech Stack:** C++17、系统 Fast DDS 2.13.1（`find_package(fastrtps)`）、Standalone Asio（已集成，用于 req-resp 超时 timer）、GoogleTest 1.14、Google C++ 风格。

**设计借鉴（Apollo Cyber RT 对照结论）：** Cyber RT 跨主机走 RTPS 时用极简承载类型装载上层序列化字节（同我们 `RawMessage`），req-resp 同为 pub-sub + 关联——互相印证，机制不改。借鉴其 **QoS 简化结构**：`qos_profile` 字符串改为可枚举、可校验、provider 无关的 `DdsQos`。其 SHM 同主机后端不借（Fast DDS 2.13 自带 SHM transport 已覆盖同机场景；自研 SHM 传输列入远期 roadmap）；拓扑发现/协程调度/组件模型属运行时框架，超出本库非目标。

---

## 1. Fast DDS 版本与依赖策略

- **采用系统已装 Fast DDS 2.13.1**（`find_package(fastrtps)` + `find_package(fastcdr)`），不经 FetchContent 拉源码（3.x 全源码构建过重）。
- **主 spec「3.6」改为「2.13+」**：我们只用 DDS-PIM 实体 API（DomainParticipant/Publisher/Subscriber/Topic/DataWriter/DataReader）与自定义 `TopicDataType`——两版相同；版本差异仅在 `TopicDataType` 虚函数签名、库名/头路径、fastcdr 依赖，**全部封在 `FastDdsProvider` + `FastDdsRawType` 一对文件内**。将来升 3.x 只改这对文件 + CMake 两行；RTPS 线协议 2.x/3.x 互通，不影响与 3.x 系统通信。
- CMake：DDS 相关源文件与测试**仅在 `find_package(fastrtps QUIET)` 成功时**加入构建（`TRANSPORT_HAS_FASTDDS`）；找不到的环境仍可构建库的其余部分与 provider 无关的 DDS 逻辑测试（`DdsImpl`+`FakeDdsProvider` 不依赖 Fast DDS）。

---

## 2. 配置结构（含 `DdsQos`，修订主 spec §7.1）

```cpp
enum class DdsMode { kPubSub, kReqResp };

// QoS 简化结构（借鉴 Cyber RT）：可枚举、可校验、provider 无关。
// provider 负责映射到底层 QoS（FastDDS: ReliabilityQosPolicy/DurabilityQosPolicy/HistoryQosPolicy）。
struct DdsQos {
  enum class Reliability { kReliable, kBestEffort };
  enum class Durability  { kVolatile, kTransientLocal };
  Reliability reliability   = Reliability::kReliable;
  Durability  durability    = Durability::kVolatile;
  uint32_t    history_depth = 10;   // KEEP_LAST depth；0 非法（config: 错误）
};

struct DdsConfig {
  DdsMode                  mode      = DdsMode::kPubSub;
  std::vector<std::string> topics;          // 实例关注的 topic 列表；topics[0] 为默认 topic
  int                      domain_id = 0;   // 一个实例 = 一个 DomainParticipant
  DdsQos                   qos;             // writer/reader 共用（原 qos_profile 字符串废除）
  std::string              provider  = "FastDDS";  // 从 DdsProviderRegistry 选择
};
```

校验（`Open()` 时）：`topics` 非空（`Send(data)` 需要默认 topic）；`history_depth > 0`；`provider` 已注册。

---

## 3. 类分解与职责边界

```
DdsImpl (provider 无关，全部业务逻辑)          FastDdsProvider (版本敏感面，全部 FastDDS 调用)
┌─────────────────────────────────┐          ┌──────────────────────────────────┐
│ IDdsTransport 实现                │  调用    │ IDdsProvider 实现                  │
│ · 组合 TransportCore core_       │ ──────→ │ · participant + type 注册          │
│ · pub-sub 路由 (Send/Subscribe)  │          │ · topic→writer/reader 懒加载 map   │
│ · req-resp 关联/超时/id 生成      │          │ · DdsQos→FastDDS QoS 映射         │
│ · codec 边界 (Encode/Decode)     │          │ · FastDdsRawType (wire layout)    │
│ · 模式约束 (config: 错误)         │          └──────────────────────────────────┘
└─────────────────────────────────┘          FakeDdsProvider (tests/) 同实现 IDdsProvider
```

### 3.1 `DdsImpl`（实现 `IDdsTransport`，组合 `TransportCore`）

```cpp
class DdsImpl : public IDdsTransport,
                public std::enable_shared_from_this<DdsImpl> {
 public:
  // provider 注入：为空则按 config.provider 从 DdsProviderRegistry 创建。
  // 测试直接注入 FakeDdsProvider —— 可测性的关键。
  explicit DdsImpl(DdsConfig config,
                   std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsImpl() override;

  // ITransport
  Status Open() override;    // 校验 config → provider->Init → kReqResp 时按需准备
  void Close() override;     // 取消未决请求(conn: 错误兑现) → provider->Shutdown → core_.Close → 停 io 线程
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;   // → Send(data, topics[0])
  // 接收侧 5 方法一行转发 core_（同 Udp/Serial 模式）

  // IDdsTransport — pub-sub
  Status Send(const std::vector<uint8_t>& data, const std::string& topic) override;
  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  // IDdsTransport — req-resp
  Status SendRequest(const std::vector<uint8_t>& data, const std::string& topic,
                     std::function<void(Result<Message>)> on_reply,
                     uint32_t timeout_ms = 5000) override;
  Status OnRequest(const std::string& topic, RequestHandler handler) override;

  DdsMode Mode() const override;
  std::string Provider() const override;

 private:
  // pending_ 表项：{on_reply, asio::steady_timer}；mutex 保护
  // 自有 io_context + 1 线程：仅驱动 req-resp 超时 timer（DDS 收发线程归 provider）
};
```

行为约定：
- **pub-sub：** `Send(data, topic)` → `core_.EncodeForSend` → `provider->Publish(topic, bytes)`（writer 懒加载在 provider 内）。`Subscribe(topic)` → `provider->Subscribe(topic, cb)`，cb 内 `core_.DeliverFrame(payload, /*source=*/topic, /*topic=*/topic)`（DDS 的 `Message.source` 与 `topic` 均为 topic 名）。`Unsubscribe` 转发。
- **req-resp 客户端（主 spec §7.4 机制照抄）：** `SendRequest` → 生成唯一 `request_id`（自增计数 + 随机前缀，进程内/跨进程均不碰撞）→ 幂等 `provider->SubscribeReplies(topic+"_Reply", sink)` → 登记 `pending_[id] = {on_reply, timer(timeout_ms)}` → `provider->SendRequest(topic+"_Request", id, topic+"_Reply", Encode(data))`。reply sink 命中 id → 取消 timer、`Decode` 后 `on_reply(Success)`、清理；timer 到期 → `on_reply(Fail("timeout: ..."))`、清理。不认识的 id 静默忽略（同一 reply topic 多客户端各取所需）。
- **req-resp 响应端：** `OnRequest(topic, handler)` → `provider->ServeRequests(topic+"_Request", sink)`；sink 把 payload `Decode` 成 `Message`（topic 填逻辑 topic 名）、构造绑定 `{request_id, reply_topic}` 的 `ReplyFn`（内部 `provider->Reply(reply_topic, id, Encode(bytes))`），调用 handler。`ReplyFn` 可同步或异步调用（持有 provider 的 weak/shared 引用，Close 后调用返回 `conn:` 错误）。
- **模式约束：** `kPubSub` 实例调 `SendRequest/OnRequest`、`kReqResp` 实例调 `Subscribe/Unsubscribe`/按 topic `Send` → 返回 `Fail("config: method not available in this mode")`。`Receive/OnReceive/AsyncReceive` 仅在 `kPubSub` 模式交付订阅消息；`kReqResp` 模式的消息全部经 `on_reply`/`RequestHandler` 回调交付，`Receive` 系列不投递（与主 spec §16.2 用法一致）。
- **codec 边界（主 spec §7.5 注）：** Encode/Decode 全部在 `DdsImpl` 层；provider 只见原始字节。
- **错误前缀：** `config:`（校验失败/模式不符/provider 未注册）、`timeout:`（请求超时）、`conn:`（Close 后的未决请求/回复）、`io:`（provider 发布/订阅失败透传）、`codec:`。

### 3.2 `FastDdsProvider`（实现 `IDdsProvider`，Fast DDS 2.13）

- `Init(config)`：`DomainParticipantFactory::get_instance()->create_participant(domain_id, ...)` → `TypeSupport(new FastDdsRawType()).register_type(participant)`（type 名 `"RawMessage"`）→ `create_publisher` / `create_subscriber`。失败 → `io:`/`config:` 错误。
- **懒加载 map：** `topic → {Topic*, DataWriter*}` 与 `topic → {Topic*, DataReader*, listener}`，mutex 保护；Topic 实体按名复用（同名 topic 的 writer/reader 共享一个 `Topic*`）。
- **QoS 映射：** `DdsQos.reliability` → `RELIABLE_RELIABILITY_QOS`/`BEST_EFFORT_...`；`durability` → `VOLATILE_DURABILITY_QOS`/`TRANSIENT_LOCAL_...`；`history_depth` → `KEEP_LAST_HISTORY_QOS, depth`。writer/reader 同配。
- `Publish(topic, bytes)`：get-or-create writer → `RawMessage{"", "", bytes}` → `writer->write(&msg)`。
- `Subscribe(topic, cb)`：get-or-create reader（listener `on_data_available` → `take_next_sample` → 组 `Result<Message>{payload=msg.payload, topic=topic, source=topic}` → cb）。**listener 回调在 FastDDS 线程**——cb 须线程安全（`DdsImpl` 侧由 `ReceiveQueue`/mutex 保证）。
- req-resp 四方法 = 「带 id/reply 字段的 RawMessage 收发」薄封装，复用同一 writer/reader 机制：`SendRequest` 写 `RawMessage{id, reply_topic, bytes}` 到 request topic；`SubscribeReplies` 的 sink 回调 `(msg.request_id, msg.payload)`；`ServeRequests` 回调 `(msg.payload, msg.request_id, msg.reply_topic)`；`Reply` 写 `RawMessage{id, "", bytes}` 到 reply topic。
- `Shutdown()`：逆序销毁 readers/writers/topics/publisher/subscriber/participant；幂等。

### 3.3 `FastDdsRawType`（自定义 `TopicDataType`，主 spec §7.2 wire layout）

手写紧凑序列化（不经 CDR），2.13 签名（`serialize(void*, SerializedPayload_t*)` 等）：

```
[uint16 LE: id_len][id_len 字节 request_id]
[uint16 LE: reply_len][reply_len 字节 reply_topic]
[payload 字节 ... 到 sample 末尾]
```

`calculate_serialized_size` = 4 + id_len + reply_len + payload.size()；`create_data`/`delete_data` new/delete `RawMessage`。id/reply 长度上限 65535（超出 → serialize 失败）。**wire layout 单测**：序列化↔反序列化往返 + golden bytes 断言（保障主 spec §7.2 跨系统互通约定）。

### 3.4 `DdsProviderRegistry`

主 spec §7.6 原样：`RegisterProvider(name, factory)` / `Create(name)`（未注册返回 nullptr → `DdsImpl::Open` 报 `config:`）。静态 map + mutex。**FastDDS 在 `TRANSPORT_HAS_FASTDDS` 时由库初始化注册**（匿名命名空间静态注册器对象）。

---

## 4. 线程模型

- **DDS 收发线程归 provider**（FastDDS listener 线程）：`Subscribe`/reply/request sink 回调均在其上 → 进 `core_.DeliverFrame`（ReceiveQueue 自带锁）或 `pending_` 处理（mutex）后即返回，不阻塞。
- **`DdsImpl` 自有 `io_context` + 1 线程**：只跑 req-resp 超时 `asio::steady_timer`（与 TCP/UDP/串口的「每实例一 io 线程」模式一致，构造即启动，结构统一；kPubSub 模式该线程空闲，开销可忽略）。
- timer 到期回调与 reply sink 竞争同一 `pending_` 条目：mutex 下「取出再执行」，保证 `on_reply` 恰好调用一次（先到先得，后者发现条目已不在即放弃）。
- 用户回调（`on_reply`/`RequestHandler`/`OnReceive`）须非阻塞（与全库约定一致）。

---

## 5. 测试策略（分阶段；业务逻辑零 FastDDS 依赖）

**`FakeDdsProvider`（tests/dds/，header-only 测试件）：** 进程内内存 topic 总线实现完整 `IDdsProvider`——`map<topic, vector<sink>>` + mutex，`Publish/SendRequest/Reply` 同步分发给订阅者。多个 `FakeDdsProvider` 实例可共享一条总线（构造传入 `shared_ptr<Bus>`），模拟两个 participant 互通。

- **阶段 A（零 FastDDS）—— `dds_interfaces_test` + `dds_impl_test`：**
  - 配置默认值、`IDdsTransport` 继承关系；
  - pub-sub：`Send(data)` 走默认 topic、按 topic `Send`、`Subscribe` 路由（`Message.topic` 正确）、多 topic 互不串、`Unsubscribe` 生效、codec 双向；
  - req-resp：请求-响应往返（id 关联）、并发多请求互不串扰、超时触发（注入短 timeout，如 50ms）、`ReplyFn` 异步调用、不认识的 id 被忽略、Close 取消未决请求（`conn:`）；
  - 模式约束：错误模式调用返回 `config:`；
  - provider 未注册 → `Open` 返回 `config:`。
- **阶段 B（真实 FastDDS）—— `fastdds_rawtype_test` + `fastdds_provider_test`：**
  - wire layout 往返 + golden bytes（含空 id/reply 的 pub-sub 形态、64KB payload 边界）；
  - 两个 `DdsImpl`（FastDDS provider，同 domain_id=42 避撞）：pub-sub 互通（订阅→发布→收到，`Receive(timeout=3000)` 容纳发现期）、req-resp 互通（含超时路径）；QoS kReliable+TransientLocal 晚加入订阅者收到历史（depth 内）；
  - FastDDS 测试在 `TRANSPORT_HAS_FASTDDS` 未定义时整体不编译；运行期 participant 创建失败 → `GTEST_SKIP`。
- 不 sleep-flaky：互通等待一律 `Receive(timeout)`/带超时轮询配对状态。

---

## 6. 与 Foundation / 主 spec 的衔接

- 组合 `TransportCore`（编解码、三模式接收交付、时间戳）；**不**用 `IFramer`/`FrameAssembler`（DDS sample 保边界）。
- 满足主 spec §7 全部公共 API；req-resp 机制照 §7.4；wire layout 照 §7.2（跨系统互通约定不变）。
- 主 spec 同步修改：§7.1 `qos_profile` → `DdsQos`（本 spec §2）；§12 Fast DDS `3.6` → `2.13+`；§3.1 层次图底层库标注同步；changelog 增补记录（DdsQos 借鉴 Cyber RT、版本策略、SHM 不自研之 roadmap 备注）。
- 命名遵循 `*Impl` 约定：实现类为 `DdsImpl`（主 spec 旧文中的 `DdsTransport` 同义）。

## 7. 后续（不在本 spec 范围）

TransportFactory + JSON 配置（主 spec §9，将把 `DdsQos` 纳入 JSON 映射）；自研 SHM 传输（远期 roadmap，Fast DDS 内建 SHM 已覆盖同机场景）；协程调度层（已留档：升 C++20 + asio awaitable 方案，独立于传输库）。
