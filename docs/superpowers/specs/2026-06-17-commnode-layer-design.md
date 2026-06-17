# CommNode 层 v1(点对点交互模式基类)— 设计 spec

> 0.2.0 三层架构的最上层:**`CommNode` —— 用户继承的交互模式基类**(原讨论中暂称 "System",定名 `CommNode`)。
> 持有一个 **Transport(纯字节管道)** + 一个 **ICodec(线缆格式)** + 一个 **IExecutor(执行器:决定业务在哪跑)**,
> 把"收发字节"提升为**交互模式**:单向 / 请求-应答 / 请求-结果反馈 / 服务端处理。
> **v1 只做点对点双向管道**(TCP 客户端 / 串口 / UDP);DDS pub-sub/双 topic req-resp(`DdsNode`)、
> TCP 服务端多连接(`ServerNode`)留作后续。
> **执行模型可换**:v1 内置 `ThreadExecutor`(worker 线程 + 有界队列 + 定时器);将来**自研协程库 `CoroExecutor`** 实现同一 `IExecutor` 即插即换,`CommNode` 不动。

**Goal:** 用户继承 `CommNode`、在子类填业务,即获得统一通信交互模式(`Send`/`Request` 回调或 future/`Request` 结果反馈/`OnRequest`);底层字节收发、分帧解码、关联/超时由基类承担;**线程/执行模型经 `IExecutor` 抽象,可从 v1 的线程版平滑换成未来协程版**。

**Tech Stack:** C++17;Standalone Asio(已 vendored,本层不强依赖);GoogleTest 1.14(已 vendored);不抛异常(`Result<T>`/`Status`)。

**配套:** 三层架构 spec `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`(§8 蓝图,其中 "System 层" 即本层,类定名 `CommNode`)。

---

## 1. 定位与线程模型(核心)

`CommNode` 把三层接起来,**执行模型经 `IExecutor` 抽象**:

```
io 线程(transport 自有): transport.OnBytes(bytes,from)
                          → codec.Decode(bytes)              ← 内联,便宜、有序、天然背压
                          → executor->Post([msg]{Dispatch})  ← 投递到业务上下文(满则阻塞=背压)
业务上下文(executor 决定): Dispatch:按 kind 路由 → 业务钩子/挂起回调(串行)
                          请求超时:executor->ScheduleAt(deadline, …)
用户线程: Send / Request 发起;或 future.get() 过程式等回
```

**要点:**
- **`Decode` 内联在 io 线程**(不在它之前放队列)——单流解码本就串行,队列前置只平移串行点、还丢 TCP 背压。
- **业务在"业务上下文"串行跑**(v1 = 一条 worker 线程;未来 = 协程调度)—— 慢业务绝不阻塞 io 线程。
- **`CommNode` 与执行器无关**:它只调 `executor->Post / ScheduleAt / Cancel`;线程 vs 协程的差异全在 `IExecutor` 实现里。
- **背压在 `executor->Post`**:实现(ThreadExecutor)队满则阻塞投递方(io 线程)→ 经 transport 流控传导到对端。

---

## 2. 执行器缝 `IExecutor`(线程模型可换 —— 为未来协程库预留)

```cpp
// include/transport/comm/IExecutor.hpp
class IExecutor {
 public:
  using Task    = std::function<void()>;
  using TimerId = uint64_t;                       // 0 = 无效

  virtual ~IExecutor() = default;
  virtual void Start() = 0;
  virtual void Stop()  = 0;                        // 停止并 drain/join,确保无任务在 CommNode 析构后跑

  // 投递任务到业务上下文【串行】执行;容量满时【阻塞调用方】(背压)。
  virtual void Post(Task task) = 0;

  // 在 deadline 触发一次性 task(用于请求超时);task 也在业务上下文跑。
  virtual TimerId ScheduleAt(std::chrono::steady_clock::time_point deadline, Task task) = 0;
  virtual void    Cancel(TimerId id) = 0;          // 取消未触发的定时器(幂等)
};
```

- **`CommNode` 的全部业务回调与超时,都经此接口调度** —— 它不知道背后是线程还是协程。
- **v1 内置 `ThreadExecutor`(`include/transport/comm/ThreadExecutor.{hpp,cpp}`)**:1 条 worker 线程;`Post` 入有界任务队列(满则阻塞=背压);定时器用最小堆 + worker 的 `cv.wait_until(最近 deadline)` 兼顾"取任务 / 触发超时";`Stop` 唤醒+join。**这正是原设计的那套并发机制,封装成可复用执行器。**
- **将来 `CoroExecutor`(自研协程库,本轮不做)**:实现同一 `IExecutor`,`Post` = 恢复/调度一个协程,`ScheduleAt` = 协程定时器。**替换它,`CommNode` 与所有交互模式逻辑零改动。**
- **测试可注入 `InlineExecutor`**(可选):`Post` 即时在调用线程执行、`ScheduleAt` 由测试手动驱动 —— 让交互模式逻辑可做确定性单测(不依赖真实线程时序)。

---

## 3. 数据类型(沿用底层)

- `Result`/`Status`(不抛异常,前缀分类)。
- `Message{ payload, topic, source, timestamp, kind, correlation_id }`,`MessageKind{kOneway,kRequest,kReply,kFeedback,kNotify}`(底层已定;`SystemCodec` 负责上线缆)。
- `Endpoint`(发送寻址;点对点通常 `kDefault`)。
- **codec 选择:** v1 用 `SystemCodec`(携带 kind+correlation_id,交互模式必需)。裸 codec(无 corr_id)只能 `Send`/`OnMessage`,Request/OnRequest 不可用。

---

## 4. 用户面:`CommNode` 基类

```cpp
// include/transport/comm/CommNode.hpp
using ReplyFn    = std::function<void(Result<Message>)>;   // 终结:成功/超时/断开
using FeedbackFn = std::function<void(const Message&)>;     // 中间反馈,可多次

class Responder {  // 服务端应答句柄:绑定该请求的 correlation_id + 来源
 public:
  Status Feedback(Message msg);  // 发 kFeedback(可多次)
  Status Reply(Message msg);     // 发 kReply(终结,一次)
  // 内部持 CommNode* + correlation_id + Endpoint(回发目的地)
};

class CommNode {
 public:
  // executor 为空 → 默认建 ThreadExecutor(queue_capacity);注入用于换执行器/测试。
  CommNode(std::shared_ptr<ITransport> transport,
           std::unique_ptr<ICodec> codec,
           std::unique_ptr<IExecutor> executor = nullptr,
           std::size_t queue_capacity = 1024);
  virtual ~CommNode();

  Status Open();   // executor->Start();注册 transport 回调;transport->Open()
  void   Close();  // 以 conn: 终结挂起;executor->Stop();transport->Close()(幂等)
  bool   IsOpen() const;

  // —— 交互模式:主动发起(用户线程)——
  Status Send(Message msg, const Endpoint& to = Endpoint::Default());            // 单向(kOneway)
  Status Request(Message msg, ReplyFn on_reply, uint32_t timeout_ms);            // 请求-应答(回调)
  Status Request(Message msg, FeedbackFn on_feedback, ReplyFn on_final,          // 请求-结果反馈
                 uint32_t timeout_ms);
  std::future<Result<Message>> Request(Message msg, uint32_t timeout_ms);        // 请求-应答(future)

 protected:
  // —— 业务钩子:子类覆写,全部在业务上下文串行被调,须非阻塞(可在内部再派发) ——
  virtual void OnMessage(const Message& msg) {}                  // kOneway / kNotify
  virtual void OnRequest(const Message& req, Responder responder) {}  // kRequest
  virtual void OnConnected() {}
  virtual void OnDisconnected(const std::string& reason) {}
  virtual void OnError(const std::string& error) {}              // 解码失败 / IO 错误

 private:
  void Dispatch(Message msg);   // 业务上下文:按 kind 路由(§5)
  // transport_/codec_/executor_/挂起表(mutex)/...
};
```

**说明:**
- 须以 `shared_ptr` 持有(`enable_shared_from_this`);transport 回调捕获 `weak_ptr` 防环;executor 任务捕获 `this`(`executor->Stop()` 在析构前 join,保证不悬空)。
- `Send`/`Request` 在用户线程:`codec_->Encode(msg)` → `transport_->Send(bytes[,to])`(`Encode` 一对一、不碰 `Decode` 的滚动缓冲,内置 codec 与并发 `Decode` 安全)。
- 钩子默认空实现;子类按需覆写。

---

## 5. 收侧统一分发 + 请求-应答状态机

**io 线程**:`Decode` 出每条 `Message`(填 `source=from`、`timestamp`)→ `executor->Post([self,msg]{ self->Dispatch(msg); })`;连接事件(建连/断开/错误)同样 `Post` 一个对应任务。

**`Dispatch(msg)`(业务上下文,串行)** 按 `kind`:
```
kReply / kFeedback → 按 correlation_id 配挂起请求:
    kFeedback → on_feedback(msg)         (不移除挂起)
    kReply    → on_reply/on_final(msg)   (移除挂起、Cancel 其超时定时器;future 重载兑现 promise)
    无匹配 id(他人/已超时)→ 丢弃
kRequest → OnRequest(msg, Responder{correlation_id, source})   (子类业务)
kOneway / kNotify → OnMessage(msg)                              (子类业务)
```

**挂起表** `correlation_id → Pending{ on_reply/on_final, on_feedback(可空), TimerId }`,mutex 保护(`Request` 用户线程登记;`Dispatch`/超时在业务上下文)。
- **`Request`(用户线程)**:分配 `correlation_id`、`msg.kind=kRequest`、**先登记挂起**、`timer = executor->ScheduleAt(now+timeout, [self,id]{ self->FireTimeout(id); })` 存入挂起、**再** `Encode`+`transport->Send`。"先登记后发送"保证应答(经 io→Post,必晚于发送)不会先于登记被处理。
- **超时 `FireTimeout(id)`(业务上下文)**:取挂起;在 → 移除 + `on_reply(Fail("timeout: request timed out"))`;不在(已被应答)→ 放弃。**与 `Dispatch` 同在业务上下文串行 → reply 与超时天然"恰好一次"。**
- **`future` 重载** = 登记一个"兑现 `promise` 的 `on_reply`",返回其 `future`。
- **终结一致性**:`Close`/断连 → 所有挂起 `on_reply(Fail("conn: ..."))`、`Cancel` 其定时器、清空。

---

## 6. 背压

- 背压点在 **`executor->Post`**:`ThreadExecutor` 的任务队列有界(容量 `queue_capacity`),满时 `Post` 阻塞投递方(io 线程)→ 不再读 → transport 流控 → 对端减速。
- 连接/错误等控制任务可不计入上限(以免断连/错误被卡)。
- 这是"慢消费者"下的内存上限与流控点;换 `CoroExecutor` 时由其自行定义背压策略。

---

## 7. 生命周期

- **`Open()`**:`executor_->Start()`;`transport_->OnBytes/OnConnect/OnDisconnect(...)`;`transport_->Open()`(失败 → `executor_->Stop()` 并返回其错误)。
- **`Close()`**(幂等):锁内以 `conn:` 终结所有挂起 + `Cancel` 定时器;`executor_->Stop()`(drain/join);`transport_->Close()`。
- **析构**:`Close()`。
- **连接事件**:`OnConnected`/`OnDisconnected` 经 executor 串行交付(与其它钩子同上下文、顺序一致);断连额外终结所有挂起请求。

---

## 8. 错误处理

- 全程 `Result`/`Status`,前缀分类。
- `Decode` 返回 `frame:`(坏帧)→ `OnError` 且(流式)视为致命 → `Close`;`codec:`(单条解码失败)→ 丢弃该批 + `OnError`,继续。
- 请求超时 → `timeout:`;`Close`/断连终结挂起 → `conn:`。
- `Send`/`Request` 前 `Encode` 失败 → 返回 `Status::Fail`,不入挂起、不发送。

---

## 9. 测试策略

- **`FakeTransport`(进程内双向回环,测试件)**:一对 `FakeTransport` 互联(写一端 → 另一端 `OnBytes`),让两个 `CommNode`(client/server)经 `SystemCodec` + `ThreadExecutor` 全栈互通,零真实 I/O。
- **`ThreadExecutor` 单测**:`Post` 串行执行 + 满则阻塞(背压);`ScheduleAt` 到点触发、`Cancel` 生效;`Stop` drain/join。
- **`CommNode` 交互模式**(继承测试用 `CommNode` 子类):
  - 单向 `Send` → 对端 `OnMessage`;
  - 请求-应答(回调):client `Request(on_reply)` → server `OnRequest`→`responder.Reply` → on_reply 收到;
  - 请求-应答(future):`auto r = client.Request(msg, 1000).get();` 断言;
  - 请求-结果反馈:server `responder.Feedback` 多次 + `Reply` 终结 → client `on_feedback` 多次 + `on_final`;
  - 超时:无人应答 → `on_reply(Fail("timeout:"))`;
  - 断连终结:`Close`/对端断 → 挂起以 `conn:` 终结;
  - 背压:`queue_capacity` 设小、塞满 → io 入队阻塞(放行后排空,无丢失);
  - 串行:并发到达在钩子里串行可见(无数据竞争)。
- **执行器可换性验证**:同一套 `CommNode` 交互测试,注入 `InlineExecutor`(确定性、无线程)再跑一遍关键用例 —— 证明逻辑与执行器解耦。
- **解耦/分层**:`CommNode` 测试只经 `ITransport`+`ICodec`+`IExecutor` 接口,不碰具体 transport 实现。

---

## 10. 文件结构

**新建:**
- `include/transport/comm/IExecutor.hpp` —— 执行器接口。
- `include/transport/comm/ThreadExecutor.hpp` + `src/comm/ThreadExecutor.cpp` —— v1 线程版执行器(worker + 有界队列 + 定时器)。
- `include/transport/comm/CommNode.hpp` + `src/comm/CommNode.cpp` —— 基类 + 分发 + 挂起表。
- `include/transport/comm/Responder.hpp`(或并入 CommNode.hpp)。
- `tests/comm/fake_transport.hpp`、`tests/comm/inline_executor.hpp`(测试件)。
- `tests/comm/thread_executor_test.cpp`、`tests/comm/comm_node_test.cpp`。

**修改:** `CMakeLists.txt`(加 `src/comm/ThreadExecutor.cpp`、`src/comm/CommNode.cpp` 到库;加 comm 测试)。

**不动:** 所有 Transport / ICodec / 底层。

---

## 11. 不做什么(YAGNI / 范围外)
- **不做** 自研协程库 `CoroExecutor` —— 本轮只**预留 `IExecutor` 缝** + 内置 `ThreadExecutor`;协程版后续实现同接口替换。
- **不做** DDS pub-sub / 双 topic req-resp —— 后续 `DdsNode`。
- **不做** TCP 服务端多连接 —— 后续 `ServerNode`(接受器 → 每连接一个 CommNode)。
- **不做** pull 三模式接收 —— 已被 push 钩子 + `Request` 的 `future` 重载取代。
- **不做** 多 worker / 线程池 —— `ThreadExecutor` 固定单线程(单流有序);并行由未来执行器按需提供。
- **不做** 请求取消、流式应答细粒度背压 —— 按需后续。
- **不引入** 新第三方依赖。

---

## 12. 命名备注
- **类定名 `CommNode`**(通信节点),所在层目录 `comm/`,即三层架构最上层(早先暂称 "System")。
- **执行器缝 `IExecutor`** 是线程模型可换的关键:v1 `ThreadExecutor`,未来 `CoroExecutor`(自研协程),测试 `InlineExecutor`。
- 后续特化:`DdsNode`(DDS pub-sub)、`ServerNode`(服务端多连接)。
- `Responder` 服务端应答句柄;`ReplyFn`/`FeedbackFn` 回调别名;`kReply` 终结、`kFeedback` 中间反馈。
