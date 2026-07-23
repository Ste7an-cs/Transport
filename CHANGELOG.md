# 变更日志（Changelog）

本项目的所有重要变更记录于此。格式借鉴 [Keep a Changelog](https://keepachangelog.com/)，版本遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

权威参考：[需求规格说明书（SRS）](docs/需求规格说明书.md) · [设计说明书（SDD）](docs/设计说明书.md)。

---

## [Unreleased]

> 面向**协程原生目标架构**(见 `docs/adr/0001-*`、`docs/adr/0002-*`、`docs/需求规格说明书-协程原生.md`)的文档与地基工作;尚未构成发布版本,as-built 交互层仍为 0.3.0 的异步栈 + 第二期 `coro::InteractionEngine`。目标与 as-built 的差异在本节、迁移计划与测试追溯矩阵中维护。

### 文档:协程原生 SRS v2→v3 —— grilling 收敛 + ADR-0002 — 2026-07-22
> 经 `/grill-with-docs` 逐条盘问,把 SRS v2 的多处可验收性缺口收敛为可验证需求,关闭 TBD-002 与 TBD-006;决策记入新建 ADR-0002 并回指 SRS 需求号。
- **新增** `RT_TRANSPORT_007` 定义并发发送排序 = **节点执行域到达顺序**(单 fiber 程序序保持;跨 fiber 取得某个一致全序,但不保证可由调用方墙钟时序预测),**关闭 TBD-002**;`RT_TRANSPORT_008` 定义**发送完成 = 帧字节全部离开框架用户态发送缓冲(进内核)+ 协程背压**,否决 fire-and-forget(经 corosocket 代码核对:用现有 `write()`+`waitForBytesWritten()` 循环可实现,不改 AsyncTask、不暴露 corosocket 给用户)。
- **新增** `RT_NODE_006`(无连接介质判活归协议/交互层,transport 只暴露最近收发时间戳/计数/单操作错误等 I/O 事实)、`RT_NODE_007`(DDS provider 交接有界队列 + 满时丢最新 tail-drop,默认 1024 样本/16 MiB 可配),**关闭 TBD-006 的 DDS 部分**;`RT_LIFECYCLE_002` 补「连接状态是 TCP 客户端在 `Running` 内的子状态,非对等第二生命周期;非重连节点致命错误→`Closing→Closed`,无 `Reconnecting`」。
- **变更** 业务队列字节上限可配范围定为 **64 KiB~256 MiB**(默认 16 MiB)+ 聚合内存(Σ 业务队列+DDS 交接)归宿主保证落在 RSS 预算,**关闭 TBD-006 剩余部分**;3.6.2 把「框架导致丢失/重复 = 0」改为**容量内可验证判据 + 逐因归因**(每次本地丢弃归因到恰好一个命名计数原因,Σ 命名原因计数 = 总丢弃,无静默丢失;迟到/重复/坏帧属正确过滤而非丢失),RT_DATA_BUFFER/RT_TESTABILITY 同步。
- **新增** `docs/adr/0002-send-completion-drop-attribution-and-lifecycle-refinements.md`(决策 D1–D6 + corosocket 可行性核对 F1);`docs/adr/0001-*` 的「尚未解决」标注 DDS 交接容量/溢出、并发发送排序两项已由 ADR-0002 关闭;`CONTEXT.md` 增术语 **发送完成语义 / 发送排序 / 丢弃归因**。
- **说明** 本节为**目标需求**收敛,不含实现;SRS 版本 v2→v3。

### 地基:协程传输契约 + 协作取消 + 结构化错误 + 共享完成原语 — 2026-07-21 ~ 07-22(PR #17)
> 协程原生传输层的地基件,尚未接入节点。AsyncTask 以 `third_party/AsyncTask` **git 子模块**纳入并补充 socket awaitable(新增 `CoroUdpSocket::receiveDatagram()` 保留 datagram 边界与地址 metadata 等)。
- **新增** 协程传输契约(`ITransport` 协程 await 式定义 + 契约消歧)、**结构化传输错误**、**协作取消**(含取消竞态加固)、**共享完成原语 `SharedCompletion`**(支持 `void`、值拷贝前释放完成锁)。
- **新增** 确定性 **Fake 传输**测试替身 + fake 读超时仲裁(用 AsyncTask `await_for`)。
- **构建/杂项** AsyncTask 转 git 子模块并更新 socket awaitables;忽略本地 worktree 与 subagent 进度账本。
- **设计** `docs/` 增协程传输地基的 plan/design 文档。

### 文档:协程原生架构 SRS(GJB 精简模板)+ agent-skills 配置 — 2026-07-17 ~ 07-21(PR #15/#16)
- **新增** 目标架构 SRS `docs/需求规格说明书-协程原生.md`(GJB 精简模板,标识前缀 `RT`);ADR-0001(协程原生交互架构,多轮 grilling 决策 D1–D21 + 事实修正)。
- **新增** agent-skills 配置:issue tracker(GitHub / `gh`)与 domain docs(single-context)约定,落 `docs/agents/`、`CLAUDE.md`/`AGENTS.md` 的 `## Agent skills`。
- **变更** SRS v1→v2「完善协程原生需求规格」;`CONTEXT.md` 术语表标注 [as-built]/[target]。

## [0.3.0] - 2026-07-09

> **⚠️ 破坏性次版本 —— 传输层从 standalone Asio 迁至 QtNetwork(去 asio、需 Qt5);并新增协程原生交互引擎(第二期)。** 与 0.2.x **不构建兼容**:整仓构建从此需 Qt5,所有传输不再自持 io 线程 → 用它们的宿主须运行 Qt 事件循环。

### ⚠️ 特性:UDP/TCP/串口传输迁移至 QtNetwork(去 asio)— 2026-07-09(PR #12)
> `ITransport` 接口与回调契约**不变**;仅把 5 个具体传输的底层实现从 standalone Asio 换成 QtNetwork,**整仓移除 asio**。为第二期协程原生引擎(跑在 Qt 事件循环线程)铺路。
- **变更** `UdpTransport`→`QUdpSocket`、`TcpConnection`→`QTcpSocket`、`TcpServerTransport`→`QTcpServer`、`TcpClientTransport`→`QTcpSocket`+`QTimer`(连接超时/指数退避重连)、`SerialTransport`→`QSerialPort`。用 functor `connect`(lambda 捕获 `this`),传输类**不加 `Q_OBJECT`**(无需 moc)。
- **⚠️ 线程模型变更** 传输**不再自持 io 线程/strand**;活在**宿主 Qt 事件循环线程**上,`OnBytes`/`OnConnect`/`OnDisconnect` 经 Qt 信号在该线程串行触发。宿主须运行 Qt 事件循环(测试用 `QCoreApplication`+事件泵)。报文/流式语义照旧(UDP 一 datagram 一回调;TCP/串口一次 read 切片一回调),断连语义照旧(主动 `Close` 不报 `OnDisconnect`,真断报一次)。
- **⚠️ 移除** vendored `third_party/asio`;CMake 删 `asio_standalone`(保留 `Threads::Threads`,`ThreadExecutor` 仍用)。
- **构建** `find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)`;`transport` 库链接 `Qt5::Core/Network/SerialPort`。前置依赖:`libqt5serialport5-dev`(串口编译前提)+ `socat`(串口回环测试)。测试改自定义 `QCoreApplication` main + 事件泵助手 `qtutil::pumpUntil`。
- **DDS 不动** 异步 `InteractionEngine`/`IExecutor`/`ThreadExecutor`/`DdsNode`/`DdsTransport`/`DdsCodec` 全保留(DDS 不走 QtNetwork)。
- **验证** 全新构建零告警,118/118(TCP 用例合并后总数微调);串口 socat pty 回环实跑通过。终审 1 Important(客户端 `connect_timer_` 每次重连累积 timeout slot → 退避被乘性推进)+ 3 Minor(再 Open 安全 / UDP 错误经 `OnBytes(Fail)` 上报契约 / 广播冒烟)已修。

### 特性:协程原生交互引擎(`coro::InteractionEngine` + `coro::ProtocolNode`)— 2026-07-09(PR #13)
> 交互层协程原生化第二期(核心)。把请求-应答从异步状态机变成线性的 `send(); r = await_for(timeout);`。基于 **AsyncTask**(boost.fiber,不加修改),复用 QtNetwork `ITransport`。**纯加性**,异步栈/`ITransport`/第一期传输不动。
- **新增** `transport::coro::InteractionEngine`(`include/transport/coro/`,通用机制,协议差异仍外包给 `InteractionPolicy`):`Request(out, tag, timeout, to)→Result<Message>`(`NewCorrelation`→按 `key_fn_(out)` 登记 `Coro::Awaitable`→`Send`→`await_for`,仅挂起**当前 fiber**,消除挂起表/ScheduleAt/Cancel/OnTimeout/IExecutor)、`Fire`(单向)、demux(`OnBytes`→`Decode`→`KeyOf`→命中且终结则唤醒 fiber)、`OnInboundDeliver`、**可插拔关联键 `SetKeyFn`**(默认逐字 `(session_id, message_id)`)。`conn:` vs `timeout:` 由 `channel->is_closed()` 区分。
- **新增** `transport::coro::ProtocolNode`:薄壳,`Start`/`Stop`/`Request(cmd, payload, timeout)`。
- **新增** `InteractionPolicy::IsTerminal(FrameType)`(非纯虚,默认 `== kResult`;异步引擎从不调用它 → 纯加性)。
- **线程模型** 宿主 `Coro::installFiberApplication()`+`exec()`,fiber 调度器**同时推进 fiber 与 Qt 事件**;传输 `OnBytes`、demux、等待 fiber 全在这条线程,协作式无锁。`Request` **须在 fiber(固定于传输 I/O 线程)内调用**。
- **构建** 新增 `option(TRANSPORT_BUILD_CORO)`(默认 ON):`asynctask` 静态库(AsyncTask 编译单元 + `ASYNC_HAS_QTCORE` + AUTOMOC)+ 已编译 boost `fiber/context/thread/chrono`;新 `transport_coro` 库 + `transport_coro_tests`。缓存变量 `ASYNCTASK_DIR`/`BOOST_LOCAL_ROOT`(当前为机器绝对路径,可 `-D` 覆盖;后续改可移植发现)。
- **验证** 119/119 零告警;含真 UDP 回环冒烟(协程节点经两个真 `UdpTransport` 线性 `Request` 拿应答 ~2ms,验证 fiber↔Qt↔UDP 端到端)。终审 Ready-to-merge,2 Important(独立/复用引擎场景潜在隐患,当前无在用调用触达:`Close` 摘传输回调防 UAF、`Request` fiber 亲和注记)+ 1 Minor 已修。
- **待办(本期外)** 服务端 responder/`SendReply`、周期 `StartPeriodic`、心跳、ack;DDS 搬到协程引擎(listener→fiber 线程桥);构建路径可移植化。

## [0.2.1] - 2026-06-29

### 特性:周期发送取最新状态(消息工厂 + 推送更新)— 2026-06-29(PR #10)
> 周期发送此前把 payload 冻结在启动时刻;现支持每帧**发送前取最新状态**。纯加性,固定/心跳路径逐字不变。
- **变更** 引擎 `Periodic` 载体由固定 `Message` 改为 `std::function<Message()>` 工厂;`FirePeriodic` 每拍**锁外**调 `make()` 取最新。`StartPeriodic` 加工厂重载;固定版包装、行为不变;新增 `UpdatePeriodic(handle, Message)`(推送换值)。空工厂 → 返回 0(不崩)。
- **新增** `ProtocolNode::StartRepeating(cmd, state_fn, …)`(拉)+ `UpdateRepeating(handle, cmd, payload)`(推)。
- **新增** `DdsNode` 周期发布(此前无):`StartPublishing`(固定/`sample_fn` 工厂)+ `UpdatePublishing`/`StopPublishing`(tag `kNotify`)。
- **契约** `state_fn`/`sample_fn`/`make` 在 executor 线程、每拍发送前调用,须线程安全、非阻塞、不抛;`Update*` 下一拍生效、会把 pull 周期转为固定。
- **示例** `protocol_node_demo` 周期段改用 `state_fn` 演示每拍拉最新温度。
- **验证** 119/119,零告警;`-Wall -Wextra` 清洁。

- **示例** `examples/dds_node_demo.cpp`:`DdsNode` 完整 demo —— 进程内 DDS 总线上发布-订阅扇出 + 多路请求-应答(reply_to 各回各家 inbox)+ 反馈/终结。

## [0.2.0] - 2026-06-29

> **0.2.0 开发线 —— 三层架构(Transport 纯管道 / ICodec 线缆格式 / Comm 交互层)。** 自 2026-06-15 的两层解耦起,逐 PR 自底向上补齐:TCP 服务端 → DDS 底层 → CommNode 交互层 → DdsNode → 外部协议栈 ProtocolNode → InteractionEngine 抽象 → ITraceSink 可观测 → 外部协议 UDP 1:多。`v0.1.0` 标签保留完整旧实现(`TransportCore`/三模式接收/`TransportFactory`/`RawMessage` 等)备查;0.2.0 与 0.1.0 **不 API 兼容**。

### 特性:外部协议跑 UDP(`SystemDatagramCodec` + 1:多寻址)— 2026-06-26(PR #9)
> 外部协议(`SystemCodec` 帧)此前只配 TCP/串口(有状态流式切帧)。让它正确跑 UDP 1:多——UDP 是报文边界语义,有状态滚动缓冲在多对端会串台、半截报文污染下一报文。**纯加性,默认值使 TCP/串口/流式路径逐字不变。**
- **新增** `SystemDatagramCodec`(header-only,无状态报文版外部帧):每 `Decode` 只解一个 datagram、残留丢弃、不跨报文(多对端不串台);与流式 `SystemCodec` **共用帧核** `EncodeSystemFrame`/`ScanSystemFrames`(抽出、不复制),编码字节一致。`SystemCodec` 重构为调用共享核,行为不变。
- **新增** `ProtocolConfig.reply_to_source`(默认 false):置 true 时 `ProtocolPolicy.ReplyTo` 经 `std::from_chars` 解析入站 `Message.source`(按最后一个 `:` 切,兼容 IPv4 与 `::1`)→ `Endpoint::Net`;**一个开关覆盖服务端 `Responder` 回应 + 客户端 `needfeedback` 自动 ack**(都回到入站来源)。
- **变更** `ProtocolNode` 5 个发送方法加可选尾参 `const Endpoint& to = Default()`(转发引擎原语):客户端经一个 UDP socket 向多设备分别发,重发回同一 `Pending.to`。默认 `Default` → 旧调用点/TCP/串口零改动。
- **示例** `examples/protocol_udp_demo.cpp`:1 控制器对 2 设备的 UDP 1:多 req-resp。
- **验证** 干净构建零告警,111/111(新增 `system_datagram_codec_test` 6 + `protocol_policy_test` 4 + 发送目的地透传 1);`system_codec_test` 不变即证明共享核重构行为保持。

### 特性:可插拔结构化 Trace(`ITraceSink`)— 2026-06-26(PR #8)
> 此前全库零日志,唯一可观测点是引擎的 `on_error_(string)`。给交互层加一套**观测-only、近零开销**的 trace,让分发/超时/重发/auto-ack/periodic/收发/关闭全程可见。
- **新增** `ITraceSink`(层中立 `include/transport/ITraceSink.hpp`,header-only):`TraceLevel{kTrace,kDebug,kInfo,kWarn,kError}` + 零分配 `TraceEvent`(`string_view`+`int` 视图)+ 两个内置 sink `OstreamTraceSink`(→ ostream,默认 stderr)/`CapturingTraceSink`(深拷贝,测试用)。
- **新增** `InteractionEngine::SetTrace`(Open 前注入)+ 全咽喉点埋点:`open`/`close`/`conn`/`send`/`request`/`reply`/`recv`/`decode`(成功=条数 / `decode-fail`)/`dispatch`(match-terminal·match-intermediate·auto-ack)/`unmatched`/`retransmit`/`timeout`/`periodic`(start·fire·stop)/`error`。
- **新增** `DdsNode`/`ProtocolNode` 透传 `SetTrace`。
- **观测-only**:无 sink 时单分支、字节流逐字不变;所有 trace 在 `mu_` 外发(不在锁内回调用户 sink → 杜绝慢 sink 阻塞/重入死锁);sink 须线程安全。
- **预留**:`ITraceSink` 层中立 + 保留 category(`resync`/`frame-drop`/`io`/`reconnect`),后续 codec/transport 加可选 sink 参数即接,接口零改。
- **验证** 干净构建零告警,100/100(新增 trace_sink + interaction_trace + 节点透传);评审抓出并修掉「StopPeriodic 锁内回调 sink」与「缺 spec §5 decode 成功事件」。

### 重构:交互机制抽象(`InteractionEngine` + `InteractionPolicy`)— 2026-06-24(PR #7)
> `CommNode` 与 `ProtocolNode` 曾各自重写同一套交互机制(且各被评审抓出过一个并发 Critical)。三个真实交互节点 = 足够样本,合并机制为一份、最难写对的并发生命周期只写一处。**纯内部重构,DDS/协议行为保持。**
- **新增** `InteractionEngine`(通用机制一份):3 原语 `Fire`(单向)/ `RequestAwait`(发+等回应,带超时/重发/自动 ack)/ `StartPeriodic`(定时发);挂起表、超时、重发、periodic 定时器、io→Decode→Post→单 worker Dispatch、并发纪律(weak_ptr / `Encode`+`Send` 锁外 / 中间帧拷贝调用·终结帧移出调用防双触发 / `Close` 先终结挂起再停定时器再停执行器再关传输 / 恰好一次)全部集中此处。
- **新增** `InteractionPolicy`(声明式协议策略,7 法 `TagOf`/`SetTag`/`NewCorrelation`/`KeyOf`/`EchoCorrelation`/`ReplyTo`/`RouteUnmatched`):匹配键 `Key`=`std::string`、判别符 `FrameTag`=`int`,**引擎只做相等比较、从不解释**(零 per-protocol 泄漏)。`RequestSpec` 配置 request-await;**「首帧停重发」一条规则**(首个配上的中间/终结帧停重发)覆盖各模式,免协议专属 flag。实现 `ProtocolPolicy`(`Key`=(session,message)、`session_id` 滚动、`message_id`=命令码)、`DdsPolicy`(`Key`=correlation_id、`reply_to`=inbox 路由)。
- **变更** `DdsNode` 重做成 `InteractionEngine`+`DdsPolicy` 薄壳(**不再继承 `CommNode`**);`dds_node_test` 仅去 CommNode 依赖、断言不变。
- **变更** `ProtocolNode` 重做成 `InteractionEngine`+`ProtocolPolicy` 薄壳;发送方法**新增首参 `uint16_t cmd`(=`message_id` 命令码)**(纠正旧实现 `message_id` 自增的语义);心跳改为首拍即发。
- **⚠️ 移除** 通用 corr+kind 的 `CommNode`(其机制即引擎,不被实用);`comm_node_test` 转为引擎级 `interaction_engine_test`(覆盖原通用交互面 + worker 内 Close 自连接守卫)。
- **验证** 干净构建零告警,86/86 测试两次连跑稳定;引擎/策略代码零 `frm_type`/`kind`/节点名(缝切对的护栏)。

### 特性:外部协议栈(`SystemCodec` 协议帧 + `ProtocolNode`)— 2026-06-23(PR #6)
> 面向某外部系统的具体通信协议。`CommNode`/`DdsNode`/`DdsCodec` 不动(DDS 不走此协议)。
- **变更** `SystemCodec` 由通用线缆格式**改造为外部协议帧 codec**:`[head_flag:4=AA BB CC DD][frm_type:1][protocol_id:1][session_id:1][reserve:4=0][crc:2 LE][frm_len:2 LE][frm_body: message_id:2 LE | payload]`,小端、有状态流式解码 + 坏帧 **resync**(同步头/CRC 不符 → 前移 1 字节重扫)。**CRC 经 `CrcFn` 注入**(默认占位 `DefaultCrc16`);`frm_type` 枚举占位值 —— 真实外部字节值/算法实现前替换。
- **新增** `Message` 协议字段 `frm_type`/`protocol_id`/`session_id`/`message_id`(加性,通用字段不变)。
- **新增** `ProtocolNode`(独立节点,复用 `ITransport`+`ICodec`+`IExecutor`):5 种发送交互模式 `noresponse` / `needresponse`(等 RESPONSE)/ `withfeedback`(等 RESULT)/ `needfeedback`(RESPONSE→RESULT→自动回 RESPONSE ack)/ `repeating`(定时发 STATE,可停)+ 周期心跳 + 收发双角色(`OnCommand` + `Responder.Response()/Result()`);匹配键 **(session_id, message_id)**;超时**重发 ≤3** 后失败。
- **变更** `SystemCodec` 改为协议专用后,`comm_node_test`/`combination_smoke_test` 改用 `DdsCodec`(仍覆盖 CommNode 通用 req-resp)。
- **验证** 干净构建零告警,86/86 测试两次连跑稳定(含确定性 `InlineExecutor` 与真实 `ThreadExecutor` 双跑)。

### 特性:`DdsNode`(DDS pub-sub + 多路 req-resp)— 2026-06-17(PR #5)
- **新增** `DdsNode : CommNode`,复用交互引擎,加性补 DDS 订阅能力 + 基于 `reply_to` topic 的精确应答路由:多 topic 发布(即 `Send(Endpoint::Topic)`)/ 订阅(`Subscribe`/`Unsubscribe`)/ 多路请求-应答(请求带自身 inbox topic 入 `reply_to`,服务端 `Responder.Reply` 据此精确回送,`correlation_id` 配对)。
- **新增** `DdsCodec`(无状态、带交互元数据 `kind`/`correlation_id`/`reply_to` 的 DDS 线缆格式;DDS 每 sample 即一条完整消息,无滚动缓冲 → 多 topic 并发解码安全);`Message.reply_to` 字段。
- **变更** `CommNode` 加性泛化(`Request` 收可选目的 `Endpoint`、出站填 `reply_to`、`Responder` 按 `reply_to` 回送、入站 `topic` 缺省取来源);p2p 行为不变。

### 特性:`CommNode` 交互层(`IExecutor` + 交互模式基类)— 2026-06-17(PR #4)
- **新增** `IExecutor` 执行器缝(`Post`/`ScheduleAt`/`Cancel`/`Start`/`Stop`)+ 内置 `ThreadExecutor`(1 worker 线程 + 有界队列背压 + 最小堆定时器);为未来自研协程 `CoroExecutor` 预留可换线程模型。
- **新增** `CommNode`(用户继承的交互模式基类,复用任一 `ITransport`):单向 `Send`、请求-应答 `Request`(回调 / `std::future`)、请求-结果反馈、服务端 `OnRequest`+`Responder`;io 线程内联 `Decode` → `executor.Post` → 单 worker 串行 `Dispatch`(按 `kind`),背压在队列;请求超时 / 断连终结挂起 / 恰好一次。
- **新增** `Message` 交互元数据补 `reply_to`;错误前缀新增 `timeout:`(请求超时)。

### 特性:DDS 底层(`IDdsTransport` + `DdsTransport` + provider 抽象)— 2026-06-16(PR #3)
- **新增** `IDdsTransport : ITransport`(加 `Subscribe`/`Unsubscribe`);`DdsTransport` 持有 `IDdsProvider`:`Send(bytes, Endpoint::Topic)` → publish,`Subscribe(topic)` → 样本到达直接在 provider listener 线程调 `OnBytes(bytes, from=topic)`(同 topic 有序、跨 topic 并发)。
- **新增** provider 抽象 `IDdsProvider` + `DdsProviderRegistry`(按名)+ `FakeDdsProvider`(进程内内存 topic 总线,DI 共享 `Bus`,DDS 业务零 FastDDS 依赖即可全测)+ 可选 `FastDdsProvider`(Fast DDS 2.13,手写 `FastDdsRawType` 绕过 IDL/CDR)。
- **变更** `DdsConfig` 精简为 `{domain_id, default_topic, provider, qos}`;`DdsTransport` 是**纯字节管道**(req-resp/关联/超时移交 `DdsNode`,不再在传输层)。

### 特性:TCP 服务端 `TcpServerTransport` — 2026-06-16(PR #2)
- **新增** `TcpServerTransport`(独立接受器,非 `ITransport`):监听 `bind_addr:port`,每 accept 造一个 `TcpConnection`(实现 `ITransport`,client/accepted 共用),经 `OnAccept(shared_ptr<ITransport>)` 交付给用户在每连接上独立收发;`OnError` 通知接受错误;`LocalPort()` 取回 OS 分配端口。
- **设计** 服务端不再继承 `ITransport`/不广播(广播由用户在各连接 transport 上自行实现);与旧 `ITcpServer` 的「服务端即 transport」模型解耦。

### ⚠️ 重大重构:底层分层(Transport + ICodec 两层解耦)— 2026-06-15
> 面向 0.2.0 的破坏性重构第一阶段(PR #1)。把臃肿的富 `ITransport` 拆成两个**解耦的底层层**;上层 `System` 交互模式基类、TCP 服务端、DDS 留作后续阶段(设计见 `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`)。`v0.1.0` 标签保留完整旧实现备查。

- **新增** `ITransport` 纯字节管道:仅 `Open/Close/IsOpen` + `Send(bytes)` / `Send(bytes, Endpoint)` + `OnBytes` / `OnConnect` / `OnDisconnect`,不再涉及 Message/codec/分帧/topic。实现 `UdpTransport`、`TcpClientTransport`(合并旧 client+connection 为单类)、`SerialTransport`。
- **新增** `ICodec` 线缆格式(分帧 + 序列化 + 交互元数据,有状态):`Encode(Message) → bytes` / `Decode(bytes) → 0..N Message`。实现 `SystemCodec`(默认线缆格式,承载 `kind`+`correlation_id`+`topic`+`payload`)、`LengthFieldCodec`(吸收旧 `LengthFieldFramer`+`FrameAssembler`)、`DatagramCodec`(报文直通,header-only)。
- **新增** `Message` 交互元数据:`MessageKind{kOneway,kRequest,kReply,kFeedback,kNotify}` + `correlation_id`,为后续 `System` 的请求-应答/请求-结果反馈/订阅预置。
- **⚠️ 破坏性移除** 旧 `TransportCore`、`IFramer`/`LengthFieldFramer`/`FrameAssembler`、`TopicEnvelope`/`StreamSend`、`ReceiveQueue` 与三模式接收(`Receive`/`OnReceive`/`AsyncReceive`)、topic 路由、`TcpConnectionImpl`/`TcpServerImpl`/`ITcpServer`、整个 DDS(`DdsImpl`/`IDdsProvider`/`FastDdsProvider` 等)、`TransportFactory`。`ICodec` 由无状态 `bytes↔bytes` 改为有状态 `Message↔bytes`。
- **说明** 本次移除使上一条「健壮性优化」中的 `DdsImpl::Send` 空 `topics` 防护、`TransportFactory::ParseSubFramer` 随相关文件一并删除(对当前代码不再适用);`Result<T>`/`Status` 的 `[[nodiscard]]` 保留。
- **解耦** Transport 不依赖 `Message`/`ICodec`,`ICodec` 不依赖任何 transport(唯一交叉点是组合冒烟测试)。
- **验证** 干净构建零告警,27/27 测试通过(TDD,11 commits;含 UDP/TCP 回环、串口 openpty、两层组合冒烟)。

### 健壮性与可维护性优化 — 2026-06-15
- **变更** `Result<T>`/`Status` 标 `[[nodiscard]]`：框架不抛异常、靠返回值传错，忽略 `Open`/`Send` 等返回值改为**编译期告警**；同步清理由此暴露的全部静默丢错（生产代码有意忽略处显式 `(void)` 标注，测试中期望成功的调用升级为 `ASSERT_TRUE` 断言）。
- **修复** `DdsImpl::Send(data)` 增加空 `topics` 前置校验（返回 `config: no topic configured`），堵住先于 `Open` 调用导致 `topics[0]` 越界的未定义行为。
- **重构** `TransportFactory` 中 `tcp_client`/`tcp_server`/`serial` 三处重复的 framer 子解析合并为 `ParseSubFramer` 辅助函数。
- **验证** 干净构建零告警，150/150 测试通过。

## [0.1.0] - 2026-06-14

**首个发布版本**，聚合下列全部里程碑。`transport::LibraryVersion()` 返回 `"0.1.0"`，对应 git 标签 `v0.1.0`。本节内各里程碑的 **⚠️ 破坏性** 均为 0.1.0 发布**之前**的开发期内部变更（彼时尚无已发布版本可破坏）；自 0.1.0 起，破坏性变更将触发主/次版本号递增。

里程碑按倒序排列（最新在前）。

### 文档整理 — 2026-06-14
- **新增** 需求规格说明书（SRS）`docs/需求规格说明书.md` 与 设计说明书（SDD）`docs/设计说明书.md`：基于当前实现，把分散的逐特性 spec 汇总为两份对外权威参考（需求 vs 设计）；新增 `CHANGELOG.md`。
- **新增** README「关键约束」小节，8 条核心约束反向链接 SRS/SDD。
- **变更** as-built 架构文档升级为总结版：§3 运行期协作扩为覆盖**四种传输的收发**（发送统一 / 流式接收 / 报文接收 / DDS+req-resp）；类图同步至最新（`Send(Message)`、删 `IDdsTransport::Send(data,topic)`、`TransportCore` 真实成员名、补 `StreamSend`）。

### 重构：流式发送决策去重 — 2026-06-14
- **变更** 抽出 header-only 自由函数 `BuildStreamFrame(core, routing, payload, topic)`（`core/StreamSend.hpp`），统一 TCP 与串口完全相同的「routing 分支 + `TopicFitsEnvelope` 守卫 + 按 topic 编码 + `FrameStream`」决策；各传输只保留自身 open 检查 + 入队。行为保持，149 测试全绿。

### 特性：Topic 路由编解码多路复用 — 2026-06-12 ~ 06-14
一条连接上承载多种帧格式，按 `topic` 选 codec 编解码；发送侧与接收侧 `Message` 对称。
- **新增** `ITransport::Send(const Message&, Endpoint=Default())`（基类默认实现）与 `SetCodec(topic, codec)`（基类 no-op，各传输覆写）。
- **新增** `TransportCore` 升级为 `topic→codec` 注册表 + 默认 codec（`EncodeForSend(data, topic)`、`DeliverFrame` 按 topic 选 codec）。
- **新增** header-only `TopicEnvelope`：in-band 信封 `PackTopic`/`UnpackTopic`（报文 `[topic_len:2][topic][body]`）、`FrameStream`/`TopicFrameAssembler`（流式 `[frame_len:4][topic_len:2][topic][body]`）、`TopicFitsEnvelope`。
- **新增** TCP/UDP/串口经各自 config 的 `enable_topic_routing`（默认**关**）opt-in；DDS 用原生 topic（无开关、无信封、线格不变）。
- **行为** 路由关闭时与旧帧格式**逐字节一致**；关闭时 `Send(Message{topic 非空})` 返回 `config: topic routing not enabled`；topic 上限 65535 字节，超限返回 `frame: topic too long`。

### 特性：Endpoint 统一寻址发送 — 2026-06-12
- **新增** 中立寻址值类型 `Endpoint`（`Default`/`Net(ip,port)`/`Topic(name)`）与 `ITransport::Send(data, Endpoint)`（基类默认实现，UDP 覆写 `kNet`、DDS 覆写 `kTopic`）。基类句柄即可寻址发送，无需向下转型。
- **⚠️ 破坏性** 删除 `IUdpTransport`（`UdpImpl` 直接实现 `ITransport`，`Factory::Create(UdpConfig)` 返回 `shared_ptr<ITransport>`）。
- **⚠️ 破坏性** 删除 `IDdsTransport::Send(data, topic)`（改用 `Send(data, Endpoint::Topic(...))`）。

### 构建：离线自包含 — 2026-06-12
- **变更** 第三方依赖（asio 1.30.2 / nlohmann json 3.11.3 / googletest 1.14）vendored 进 `third_party/`，构建**不再联网拉取**（移除 FetchContent）。
- **变更** Fast DDS 2.13+ 确立为**唯一可选外部依赖**，`find_package` 自动探测；未装跳过 `FastDdsProvider` 及真实互通测试。

### 里程碑：TransportFactory（主 spec 全部规划范围完成）— 2026-06-12
- **新增** `TransportFactory`：5 个类型化 `Create(config)`（返回各传输最具体接口）+ `CreateFromFile(path) → Result<vector<...>>`（JSON 配置数组）。
- **新增** JSON **严格校验**：未知 type/字段、类型不符、枚举非法、必填缺失 → `config:` 错误（含条目序号+字段），任一条目失败整体失败。
- **修复** `Create(DdsConfig)` 显式注册 FastDDS provider，根治静态库下匿名注册器被链接器裁剪问题。

### 里程碑：DDS 传输 — 2026-06-11
- **新增** `DdsImpl`（实现 `IDdsTransport`，组合 `TransportCore`）：pub-sub 多 topic 懒加载路由 + req-resp（`request_id` 关联、per-request 超时、take-then-invoke 恰好一次）。
- **新增** 内置承载 `RawMessage` + 自定义 `FastDdsRawType`（手写 wire layout，绕过 IDL/Fast DDS-Gen/CDR）。
- **新增** `IDdsProvider` 抽象 + `DdsProviderRegistry` + `FastDdsProvider`（Fast DDS 2.13）；`FakeDdsProvider`（进程内 topic 总线）使 DDS 业务逻辑零 FastDDS 依赖即可全测。
- **变更** `DdsConfig`：`qos_profile` 字符串 → `DdsQos` 结构体（借鉴 Apollo Cyber RT）；移除 `type_name`（承载统一为不透明字节流）。

### 里程碑：串口传输 — 2026-06-11
- **新增** `SerialImpl`（`asio::serial_port`，组合 `TransportCore`+`FrameAssembler`）：波特率/数据位/停止位/校验配置；流式分帧同 TCP；pty 回环测试。

### 里程碑：UDP 传输 + 组合化重构 — 2026-06-10
- **新增** `UdpImpl`（单类支持单播/组播/广播，组合 `TransportCore`，无分帧）；组播 join_group + TTL。
- **⚠️ 破坏性（架构）** 接收基座 `TransportBase`（继承式）→ `TransportCore`（组合式组件）：会收数据的传输改为**持有**它并转发接收侧方法，从根上消除「接收基座与扩展接口同源 `ITransport`」的菱形。

### 里程碑：TCP 传输 — 2026-06-10
- **新增** `TcpConnectionImpl`（已连接 socket 收发，client/accepted 共用）、`TcpClientImpl`（connect + 连接超时 + 指数退避自动重连）、`TcpServerImpl`（acceptor + 每客户端独立 `ITransport` + 广播 + `ITcpServer`）；standalone Asio 集成 + 真实回环集成测试。
- **变更** 具体实现类统一加 `*Impl` 后缀（`TcpConnection→TcpConnectionImpl` 等），区分接口。
- **修复** 锁内拷贝 `disconnect_cb_` 消除数据竞争。

### 里程碑：Foundation 基座 — 2026-06-09 ~ 06-10
- **新增** 核心类型 `Result<T>`/`Status`（不抛异常 + 前缀分类错误）、`Message`、`Endpoint` 雏形。
- **新增** 扩展点 `ICodec`（用户编解码）、`IFramer` ← `LengthFieldFramer`（长度字段分帧）、`FrameAssembler`（滚动缓冲切帧）、`ReceiveQueue`（FIFO，三模式互斥交付）。
- **新增** 统一抽象 `ITransport`（生命周期 / 发送 / 三模式接收 / 编解码挂载）；CMake 骨架 + GoogleTest + 版本冒烟测试。
- **设计基线** 抽象层（`include/`，零第三方依赖）+ 实现层（`src/`，第三方库封在 `*Impl`/provider 内）；`I*` 接口 + `*Impl` 实现；分帧仅流式接收侧；不抛异常；每实例单 io 线程 + strand 串行化。

---

[Unreleased]: https://github.com/Ste7an-cs/Transport/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.3.0
[0.2.1]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.2.1
[0.2.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.2.0
[0.1.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.1.0
