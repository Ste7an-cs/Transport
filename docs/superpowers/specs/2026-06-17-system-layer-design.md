# System 层 v1(点对点交互模式基类)— 设计 spec

> 0.2.0 三层架构的最上层:**`System` —— 用户继承的交互模式基类**。持有一个 **Transport(纯字节管道)**
> + 一个 **ICodec(线缆格式)**,把"收发字节"提升为**交互模式**:单向 / 请求-应答 / 请求-结果反馈 /
> 服务端处理。**v1 只做点对点双向管道**(TCP 客户端 / 串口 / UDP);DDS pub-sub/双 topic req-resp
> (`DdsSystem`)、TCP 服务端多连接(`ServerSystem`)留作后续。
> 底层 Transport(TCP 客户端/服务端、UDP、串口、DDS)与 ICodec(System/LengthField/Datagram)均已就位。

**Goal:** 用户继承 `System`、在子类填业务,即获得统一的通信交互模式:`Send`(单向)、`Request`(请求-应答,回调或 future)、`Request`(请求-结果反馈)、`OnRequest`(服务端)。底层字节收发、分帧解码、关联/超时、线程与背压由基类承担。

**Tech Stack:** C++17;Standalone Asio(已 vendored,本层不强依赖);GoogleTest 1.14(已 vendored);不抛异常(`Result<T>`/`Status`)。

**配套:** 三层架构 spec `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`(§8 蓝图);本 spec 是其 System 部分的落地细化。

---

## 1. 定位与线程模型(核心)

`System` 把三层接起来,关键是**线程/背压**布局(已定):

```
io 线程(transport 自有): transport.OnBytes(bytes,from)
                          → codec.Decode(bytes)        ← 内联,便宜、有序、天然背压
                          → 逐条 Message 入【有界队列】 ← 满则阻塞(=背压旋钮)
worker 线程(System 自有): 出队 → 按 kind 路由 → 调业务钩子/挂起回调
                          → 同一线程用 cv wait_until 兼做【请求超时】
用户线程: Send / Request 发起;或 future.get() 过程式等回
```

**要点:**
- **`Decode` 内联在 io 线程**(不另起队列在它之前)——单流解码本就串行,队列放它之前只平移串行点、还丢 TCP 背压。
- **业务全在一条 worker 线程串行跑** —— 慢业务绝不阻塞 io 线程;心智统一:"你的回调永远在 System 这一条线程上,串行,不碰 I/O"。
- **有界队列 = 背压** —— 队满时 io 线程入队阻塞,经传输流控传导到对端(可配容量)。

---

## 2. 数据类型(沿用底层)

- `Result`/`Status`(不抛异常,前缀分类)。
- `Message{ payload, topic, source, timestamp, kind, correlation_id }`,`MessageKind{kOneway,kRequest,kReply,kFeedback,kNotify}`(底层已定;`SystemCodec` 负责上线缆)。
- `Endpoint`(发送寻址;点对点通常 `kDefault`)。
- **codec 选择:** v1 用 `SystemCodec`(携带 kind+correlation_id,交互模式必需)。用裸 codec(无 corr_id)只能 `Send`/`OnMessage`,Request/OnRequest 不可用。

---

## 3. 用户面:`System` 基类

```cpp
// include/transport/system/System.hpp
using ReplyFn    = std::function<void(Result<Message>)>;   // 终结:成功/超时/断开
using FeedbackFn = std::function<void(const Message&)>;     // 中间反馈,可多次

class Responder {  // 服务端应答句柄:绑定该请求的 correlation_id + 来源
 public:
  Status Feedback(Message msg);  // 发 kFeedback(可多次)
  Status Reply(Message msg);     // 发 kReply(终结,一次)
  // 内部持 System* + correlation_id + Endpoint(回发目的地)
};

class System {
 public:
  System(std::shared_ptr<ITransport> transport,
         std::unique_ptr<ICodec> codec,
         std::size_t queue_capacity = 1024);
  virtual ~System();

  Status Open();   // 注册 transport 回调 + 起 worker 线程 + transport->Open()
  void   Close();  // 停 worker、以 conn: 终结所有挂起、transport->Close()(幂等)
  bool   IsOpen() const;

  // —— 交互模式:主动发起(用户线程)——
  Status Send(Message msg, const Endpoint& to = Endpoint::Default());            // 单向(kOneway)
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms);            // 请求-应答(回调)
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final,          // 请求-结果反馈
                 uint32_t timeout_ms);
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms);        // 请求-应答(future)

 protected:
  // —— 业务钩子:子类覆写,全部在 worker 线程串行被调,须非阻塞(可在内部再派发) ——
  virtual void OnMessage(const Message& msg) {}                  // kOneway / kNotify
  virtual void OnRequest(const Message& req, Responder responder) {}  // kRequest
  virtual void OnConnected() {}
  virtual void OnDisconnected(const std::string& reason) {}
  virtual void OnError(const std::string& error) {}              // 解码失败 / IO 错误

 private:
  // 见 §4/§5:io→入队、worker 出队分发+超时、挂起表。
};
```

**说明:**
- 须以 `shared_ptr` 持有(`enable_shared_from_this`);transport 回调捕获 `weak_ptr` 防环。
- `Send`/`Request` 在用户线程:`codec_->Encode(msg)` → `transport_->Send(bytes[,to])`(`Encode` 一对一、不碰 `Decode` 的滚动缓冲,内置 codec 与并发 `Decode` 安全)。
- 钩子默认空实现;子类按需覆写。

---

## 4. 收侧统一分发(worker 线程)

**队列承载 work item:** 入站 `Message`(io 线程 `Decode` 出,已填 `source=from`、`timestamp`),或控制事件(连接建立 / 断开 / IO 错误)。worker 出队后:控制事件 → 对应钩子(断开还会终结挂起);`Message` → 按 `kind`:

```
kReply / kFeedback → 按 correlation_id 配挂起请求:
    kFeedback → on_feedback(msg)         (不移除挂起)
    kReply    → on_reply/on_final(msg)   (移除挂起、取消其超时;future 重载则兑现 promise)
    无匹配 id(他人/已超时)→ 丢弃
kRequest → OnRequest(msg, Responder{correlation_id, source})   (子类业务)
kOneway / kNotify → OnMessage(msg)                              (子类业务)
```

---

## 5. 请求-应答状态机 + 超时(worker 兼顾)

- **挂起表** `correlation_id → Pending{ on_reply/on_final, on_feedback(可空), deadline }`。
- **单一互斥** 同时保护:有界队列、挂起表、worker 唤醒条件变量(避免锁序问题)。
- **`Request`(用户线程)**:分配 `correlation_id`、`msg.kind=kRequest`、**先在锁内登记挂起**(算出 deadline)、`notify` worker(重算最近超时)、**再** `Encode`+`transport->Send`。"先登记后发送"保证应答(经 io→队列,必晚于发送)不会先于登记被处理。
- **worker 循环**:
  ```
  锁定;deadline = 挂起表最近 deadline(无则永久)
  cv.wait_until(deadline, 直到 队列非空 / 有挂起到期 / stop)
  唤醒后:取出到期挂起 → on_reply(Fail("timeout: request timed out")) + 移除
          排空队列 → 逐条按 §4 分发
  ```
- **`future` 重载**:内部 = 登记一个"兑现 `promise` 的 `on_reply`",返回其 `future`。
- **终结一致性**:`Close`/断连 → 所有挂起 `on_reply(Fail("conn: ..."))` 并清空。
- **恰好一次**:reply 与超时竞争同一条挂起 —— 先取到者(从表中移除)兑现,后者发现不在即放弃。

---

## 6. 背压(有界队列)

- 队列容量 `queue_capacity`(构造参数,默认 1024 条 work item;入站消息走背压,控制事件可不计入上限以免断连/错误被卡)。
- **入队(io 线程)**:满 → 在"非满"条件变量上等待,直到 worker 取走;由此把背压经 transport 传导(TCP 收窗收紧 → 对端减速)。
- **出队(worker)**:取走后 `notify` 非满条件。
- 这是"慢消费者"下的内存上限与流控点。

---

## 7. 生命周期

- **`Open()`**:`transport_->OnBytes(...)`/`OnConnect(→排 OnConnected)`/`OnDisconnect(→排 OnDisconnected+终结挂起)`;起 worker 线程;`transport_->Open()`(失败则停 worker 并返回其错误)。
- **`Close()`**(幂等):停 worker(置 stop + notify + join);锁内以 `conn:` 终结所有挂起;`transport_->Close()`。
- **析构**:`Close()`。
- **连接事件**:`OnConnected`/`OnDisconnected` 也经 worker 串行交付(与其它钩子同线程,顺序一致);断连额外触发"终结所有挂起请求"。

---

## 8. 错误处理

- 全程 `Result`/`Status`,前缀分类。
- `Decode` 返回 `frame:`(坏帧)→ `OnError` 且(流式)视为致命 → `Close`;`codec:`(单条解码失败)→ 丢弃该批 + `OnError`,继续。
- 请求超时 → `timeout:`;`Close`/断连终结挂起 → `conn:`。
- `Send`/`Request` 前 `Encode` 失败 → 返回 `Status::Fail`,不入挂起、不发送。

---

## 9. 测试策略

- **`FakeTransport`(进程内双向回环,测试件)**:一对 `FakeTransport` 互联(写一端 → 另一端 `OnBytes`),让两个 `System`(client/server)经 `SystemCodec` 全栈互通,零真实 I/O、确定性。
- **覆盖**(继承测试用 `System` 子类):
  - 单向 `Send` → 对端 `OnMessage`;
  - 请求-应答(回调):client `Request(on_reply)` → server `OnRequest`→`responder.Reply` → on_reply 收到;
  - 请求-应答(future):`auto r = client.Request(msg, 1000).get();` 断言;
  - 请求-结果反馈:server `responder.Feedback` 多次 + `Reply` 终结 → client `on_feedback` 多次 + `on_final`;
  - 超时:无人应答 → `on_reply(Fail("timeout:"))`;
  - 断连终结:`Close`/对端断 → 挂起以 `conn:` 终结;
  - 背压:容量设小、塞满 → 入队阻塞(可控放行后排空,无丢失);
  - worker 串行:并发到达的消息在钩子里串行可见(无数据竞争)。
- **解耦/分层**:System 测试只经 `ITransport`+`ICodec` 接口,不碰具体 transport 实现(用 FakeTransport)。

---

## 10. 文件结构

**新建:**
- `include/transport/system/System.hpp` + `src/system/System.cpp`(基类 + worker + 挂起表 + 有界队列)。
- `include/transport/system/Responder.hpp`(或并入 System.hpp)。
- `tests/system/fake_transport.hpp`(测试件:进程内双向回环 ITransport)。
- `tests/system/system_test.cpp`(全部交互模式 + 超时 + 断连 + 背压)。

**修改:** `CMakeLists.txt`(加 `src/system/System.cpp` 到库;加 system 测试)。

**不动:** 所有 Transport / ICodec / 底层。

---

## 11. 不做什么(YAGNI / 范围外)
- **不做** DDS pub-sub / 双 topic req-resp —— 后续 `DdsSystem`(System 之上按 topic 约定 + per-topic codec)。
- **不做** TCP 服务端多连接 —— 后续 `ServerSystem`(接受器 → 每连接一个 System)。
- **不做** pull 三模式接收(同步 `Receive`/`OnReceive`/`AsyncReceive`)—— 已被 push 钩子 + `Request` 的 `future` 重载取代。
- **不做** 独立可配的"业务线程数 / 线程池"——固定一条 worker(单流有序);并行后续按需。
- **不做** 请求取消、流式应答背压细粒度控制 —— 按需后续。
- **不引入** 新第三方依赖。

---

## 12. 命名备注
- `System` 名字较泛;候选 `CommNode`/`Peer`/`Session`/`Endpoint`(占用)。**本 spec 暂用 `System`**(沿用讨论);实现期可再定。
- `Responder` 服务端应答句柄;`ReplyFn`/`FeedbackFn` 回调别名。
- `kReply` 为终结(请求-应答的应答 / 请求-结果反馈的最终结果);`kFeedback` 为中间反馈。
