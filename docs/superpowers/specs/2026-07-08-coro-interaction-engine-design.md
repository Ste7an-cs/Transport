# 协程版 InteractionEngine(协程化第二期·核心)— 设计

**日期：** 2026-07-08
**状态：** 设计中,待用户确认
**背景大局：** 交互层协程原生化的两期迁移之**第二期**。第一期(UDP/TCP/串口传输迁 QtNetwork、去 asio)见 `2026-07-08-qtnetwork-transport-design.md`,已完成(PR #12)。本期在其上做**协程原生的交互引擎**。

## 1. 背景与目标

现有 `InteractionEngine` 是**异步状态机**:请求-应答靠"挂起表 + `steady_timer` 排超时 + `IExecutor` 线程 + 终结/中间回调"驱动。第二期把这套机制换成**协程原生**:一个在 fiber 调度线程上跑的同步引擎,请求-应答退化为线性的 `send(); r = await_for(timeout);`,**消除挂起表 / ScheduleAt / Cancel / OnTimeout / IExecutor**。

**本期目标(核心)——客户端请求/应答:**
- 新增 `transport::coro::InteractionEngine`:**通用机制**,协议差异仍外包给 `InteractionPolicy`(与异步引擎同一缝),使 `ProtocolNode`(本期)、DDS(设计留口)、未来节点都能在其上实现自己的交互模式。
- 新增 `transport::coro::ProtocolNode`:薄壳,证明引擎端到端(tcp/udp/serial `ITransport`)。
- 复用第一期 QtNetwork `ITransport` 传输(**方案 A**:`OnBytes` 回调桥接进协程 channel);复用现有 `ICodec`(流式 `SystemCodec` 负责跨读切帧)与 `ProtocolPolicy`。
- 用 **AsyncTask**(boost.fiber 协程库)**不加修改**;单调度线程 = fiber 调度线程 = Qt 事件循环线程。

**本期不做(范围外,下一期另立 spec):**
- 服务端 responder / `SendReply`(以及"是否需要单独一个 server node"的讨论)。
- 周期发送 `StartPeriodic` / 心跳 / 自动 ack。
- 把 DDS 节点搬到协程引擎(需"provider listener 线程 → fiber 线程"桥;本期仅**设计留口**,不实现)。
- **异步栈一律不动**:`InteractionEngine`(异步)/`IExecutor`/`ThreadExecutor`/`ProtocolNode`(异步)/`DdsNode`/DDS 传输 —— 全部保留,两栈并存。

## 2. 关键决策

| 决策 | 选择 |
|---|---|
| 传输缝 | **方案 A:复用第一期 `ITransport`**。协程引擎持一个 `ITransport`,把 `OnBytes` 桥接进按 Key 挂起的协程 `Awaitable`。单一传输层同时服务异步(DDS)与协程两栈 |
| 协程库 | **AsyncTask 不改**(boost.fiber)。`Coro::installFiberApplication()` + `exec()` 在主线程装本地 fiber 调度器,**同一线程推进 fiber 与 Qt 事件**。`makeTask` 默认 affinity=当前线程,天然单线程 |
| 线程模型 | 引擎、demux、等待中的请求 fiber、QtNetwork 传输的 `OnBytes` —— **全在这一条 fiber 调度线程上**,协作式调度,**无跨线程交接、锁极少** |
| 关联键 | 默认 `(session_id, message_id)`(复用 `ProtocolPolicy`);**可插拔自定义键算法**(`std::function<Key(const Message&)>` 注入,默认保持现行为) |
| 终结语义 | `Request` 阻塞到**终结帧**;`RESULT` 为终结,中间 `RESPONSE` 本期丢弃。经 `policy.IsTerminal(frm_type)` 钩子配置(默认 `RESULT` 终结),不硬编码 |
| 分层 | 引擎独立成类(通用机制缝);`ProtocolNode` 是薄壳。与异步"引擎/节点/策略"三分一致 |
| DDS | **设计留口、本期不搬**。引擎按 `InteractionPolicy` 通用,`DdsPolicy` 未来可插;引擎假定 `OnBytes` 在 fiber 线程触发(tcp/udp/serial 成立),DDS 的跨线程 listener 交付留待搬迁时加桥 |

## 3. 架构与线程模型

一条线程跑全部。宿主(应用/测试)在主线程 `Coro::installFiberApplication()` 再 `exec()`;该调度器**同时推进 fiber 与 Qt 事件**。第一期 QtNetwork `ITransport` 的 `OnBytes` 也在这条线程触发。于是:

- **读回调(`OnBytes`)与等待中的请求 fiber 是同一线程**,协作式调度 → 无跨线程交接。
- `await_for(timeout)` 挂起的是**当前 fiber**,不阻塞线程(其他 fiber、Qt 事件照常推进)。

**铁律(须写进注释并在测试中体现):** `Request()` 挂起的是 fiber,**必须在 `Coro::makeTask([]{ ... })` 内调用**,不可在裸的非 fiber 线程上调(fiber channel 在非协程线程上 pop 会崩)。这条替代了异步引擎的整套挂起表/定时器/执行器。

## 4. 协程 `InteractionEngine`(通用机制)

文件:`include/transport/coro/InteractionEngine.hpp`、`src/coro/InteractionEngine.cpp`,命名空间 `transport::coro`。

### 4.1 构造与生命周期
- 构造:`InteractionEngine(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec, std::unique_ptr<InteractionPolicy> policy)`。**无 `IExecutor`**。
- `Status Open()`:把 `transport->OnBytes(...)` 接到内部 demux;把 `transport->OnDisconnect(...)` 接到"断连即终结所有挂起(回 `conn:`)"。不负责 `transport->Open()`(由节点/宿主开)。
- `void Close()`:幂等。终结所有在途挂起请求(逐个 `resolve` 一个 `conn:` 失败)→ 解开所有挂起 → 断 `OnBytes`。析构即 `Close()`。

### 4.2 核心原语
- `Status Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default())`
  单向发:`policy.SetTag(out, tag)` → `codec.Encode` → `transport.Send`。不登记、不等待。
- `Result<Message> Request(Message out, std::chrono::milliseconds timeout, const Endpoint& to = Endpoint::Default())`
  **核心**。步骤:
  1. `Key k = policy.NewCorrelation(out)`(滚动 session_id、盖 protocol_id;可插拔键算法在此生效)。
  2. 在挂起表 `pending_[k]` 登记一个 `Coro::Awaitable<Message>`。
  3. `policy.SetTag(out, kCommand)` → `codec.Encode` → `transport.Send(bytes, to)`;发送失败 → 摘挂起、返回该 `io:`/`config:` 错误。
  4. `auto r = awaitable.await_for(timeout)`(挂起当前 fiber)。
  5. 摘挂起表;`r` 有值 → `Result<Message>::Success(帧)`;超时 → `Result<Message>::Fail("timeout: request timed out")`;`Close`/断连中途 → `Fail("conn: ...")`。
- `void OnInboundDeliver(std::function<void(const Message&)> cb)`
  无主入站帧(无匹配挂起、且 `policy.RouteUnmatched` 判为投递)交此钩子;本期默认无钩子即**丢弃**(记 trace)。

### 4.3 demux(收帧分发)
`OnBytes(bytes, from)`(fiber 线程)→ `codec.Decode(bytes)`(流式 `SystemCodec` 内部缓冲,跨读拼帧;UDP 一 datagram 一或多帧)→ 对每个 `Message m`:
1. 填 `m.source = from`(缺省来源)。
2. `Key k = key_fn(m)`(默认 `policy.KeyOf(m)`)。
3. `pending_` 命中且 `policy.IsTerminal(m.frm_type)` → `resolve(m)` 唤醒该请求 fiber。
4. 命中但**非终结**(中间 `RESPONSE`)→ 本期丢弃(记 trace;下一期做流式回应)。
5. 未命中 → `policy.RouteUnmatched(m)`:判 deliver → `OnInboundDeliver`;否则丢弃。

### 4.4 关联键可插拔
- 引擎持 `std::function<Key(const Message&)> key_fn_`,默认 `[this](const Message& m){ return policy_->KeyOf(m); }`。
- 出站侧默认走 `policy.NewCorrelation`(滚 session_id)。自定义算法:`void SetKeyFn(std::function<Key(const Message&)>)` 覆盖入站键;出站键仍由 policy 决定(若需完全自定义配对,自定义 policy)。
- **默认逐字保持现 `(session_id, message_id)` 行为。**

### 4.5 终结钩子
- `InteractionPolicy` 增虚函数 `virtual bool IsTerminal(FrameType t) const`,默认实现 `return t == FrameType::kResult;`(基类给默认,`ProtocolPolicy` 可覆写)。**纯加性**,不动现有异步引擎行为(异步引擎不调它)。

## 5. 协程 `ProtocolNode`(薄壳)

文件:`include/transport/coro/ProtocolNode.hpp`、`src/coro/ProtocolNode.cpp`,命名空间 `transport::coro`。

- 构造:`ProtocolNode(std::shared_ptr<ITransport> transport, uint8_t protocol_id, bool reply_to_source = false)`(参数对齐 `ProtocolPolicy`);内部造 `coro::InteractionEngine`(codec=`SystemCodec` 流式,policy=`ProtocolPolicy(protocol_id, reply_to_source)`)。
- `Status Start()`:`transport->Open()` + `engine.Open()`。
- `Result<Message> Request(uint16_t cmd, std::vector<uint8_t> payload, std::chrono::milliseconds timeout)`
  内部:`Message m; m.message_id = cmd; m.payload = ...;` → `engine.Request(m, timeout)`。**线性**。
- `void Stop()`:`engine.Close()` + `transport->Close()`。

用法(宿主):
```cpp
Coro::installFiberApplication();
auto node = std::make_shared<coro::ProtocolNode>(transport, /*protocol_id*/1);
Coro::makeTask([node]{
  node->Start();
  auto r = node->Request(0x42, {1,2,3}, std::chrono::milliseconds(500));
  if (r) use(r.value); else handle(r.error);   // "timeout:"/"conn:"
  node->Stop();
  Coro::quit();
});
Coro::exec();
```

## 6. 构建(CMake 接入 AsyncTask)

AsyncTask 为头文件式(`#include "all.hpp"`),仅带 qmake `.pri`,无 CMake;Qt 能力经 `ASYNC_HAS_QTCORE` 开关;依赖**已编译的 boost** `context/fiber/thread/chrono`(vendored 于 `AsyncTask/3dParty/boost`)。接入:

- 新增 CMake 目标(仅协程引擎+节点+协程测试用),**不动现有 `transport` 库**:
  - include AsyncTask `coro/` 头目录。
  - `target_compile_definitions(... ASYNC_HAS_QTCORE)`(需 `installFiberApplication`/`coro()`)。
  - 链接 boost `fiber context thread chrono`(优先系统 `libboost-*-dev`;不可用则用 AsyncTask vendored boost)。
  - 链接第一期 `transport`(拿 `ITransport`/传输)+ `Qt5::Core`。
- 协程代码可放独立静态库 `transport_coro`(依赖 `transport` + AsyncTask + boost),或作为可选目标由开关 `TRANSPORT_BUILD_CORO` 控制。

## 7. 测试(GoogleTest + fiber 运行时)

- 测试须在 fiber 调度器内跑:测试 `main` 装 `installFiberApplication()`,断言体放进 `makeTask`,末尾 `quit()`;或用 AsyncTask `test/` 同款轻量夹具。
- **Fake `ITransport`**(可编程):记录 `Send` 的字节;暴露"在 fiber 线程注入一帧"的方法(直接调其保存的 `bytes_cb_`)。用它:
  - **往返**:`makeTask` 里 `Request(...)`;另一 fiber/定时喂一条终结 `RESULT` 帧(相同 session_id/message_id)→ 断言 `Request` 返回该帧。
  - **超时**:不喂帧 → `await_for` 返回 `timeout:`。
  - **断连**:请求在途时 `engine.Close()`/`OnDisconnect` → 返回 `conn:`。
  - **中间帧丢弃**:喂一条 `RESPONSE`(非终结)→ `Request` 不返回;再喂 `RESULT` → 返回。
  - **可插拔键**:`SetKeyFn` 后按自定义键匹配。
- 也可加一条**真传输冒烟**(UDP 回环 + `SystemDatagramCodec`)在 fiber 内跑一次 `Request`↔手工应答。

## 8. 风险与待办

1. **boost.fiber/context 编译库**:context 含架构相关汇编,须已编译库。优先系统 `libboost-fiber-dev`;不可用则编 AsyncTask vendored boost。**列为计划第 1 任务**(先让一个 hello-world fiber 编译+链接通过再写引擎)。
2. **fiber 线程纪律**:`Request` 在非 fiber 线程上调用是误用 → 注释明确 + 尽量加防护(检测/文档)。
3. **流式切帧**:TCP/串口一次 read 是任意切片;由**有状态 `SystemCodec`** 跨读拼帧(复用第一期已验证逻辑),UDP 由 `SystemDatagramCodec` 或流式皆可。
4. **DDS 跨线程**:DDS `OnBytes` 在 provider listener 线程,非 fiber 线程;本期不搬,搬时需 listener→fiber 线程 channel 桥。引擎设计不得预设 DDS 直连。
5. **破坏性**:本期**纯加性**(新增 `coro::` 目标 + `InteractionPolicy::IsTerminal` 默认实现);不改异步栈,不改 `ITransport`。版本按加性小步(0.3.x)。

## 9. 约束

- C++17,不抛异常,`Result`/`Status` 前缀分类(`config:`/`io:`/`conn:`/`timeout:`)不变。
- AsyncTask **不改**;单线程靠 `makeTask` 默认 affinity + 单 fiber 调度线程达成。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,无 Co-Authored-By。不提交 `build/`。
- 文档(SRS/SDD/README/CHANGELOG)同步留到实现后。
