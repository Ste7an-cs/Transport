# CONTEXT — 通信中间件框架 术语表（ubiquitous language）

本项目的规范术语。issue 标题、重构提案、假设、测试名等一律用此处定义的词,不要漂移到同义词。
标注 **[as-built]** = 当前实现(v0.3.0);**[target]** = 协程原生目标架构(见 `docs/adr/0001-*`、`docs/需求规格说明书-协程原生.md`)。无标注 = 两者通用。

## 三层

- **Transport（传输）** —— 纯字节收发能力,不知 `Message`、线缆格式或交互模式。
  - **[as-built]** 回调式:`OnBytes(bytes, from)` 交付、`Send()` 立即返回;底层 QtNetwork。
  - **[target]** 面向 node 提供介质无关的协程式收发；内部以 `ITransport` 组织实现，但 `ITransport` **不是用户公共 API**，其名称和方法属于设计说明。
- **编解码器（Codec）** —— 用户可扩展的公共线缆格式能力，在逻辑消息与线缆字节之间转换；流式格式可跨接收片段拼帧，报文式格式保持报文边界。公共抽象在设计中命名为 `ICodec`，具体 C++ 签名属于 API/设计说明。
- **node（交互节点）** —— 交互编程主入口:`ProtocolNode`(外部协议)、`DdsNode`(DDS)。
  - **[as-built]** = 共享 `InteractionEngine` + 声明式 `InteractionPolicy` 的薄壳;用户**继承**重写钩子。
  - **[target]** = 应用以**组合**方式配置和调用节点，不需继承框架节点类型；具体 handler 和请求 API 属于设计说明。

## 数据

- **逻辑消息** —— 编解码边界上的消息语义：`payload` + 当前协议所需的类别、关联、来源和目的信息；不要求 TCP/UDP/串口/DDS 共用一个包含所有字段的 C++ 结构。
- **Endpoint** —— 中立寻址值类型:`Default` / `Net(ip,port)` / `Topic(name)`。
- **Datagram** —— **[target]** `ITransport::Read()` 的返回负载 `{bytes, from}`。UDP/DDS 一次一完整报文(`from` 可变);TCP/串口一次一任意字节切片(`from` 恒为对端)。
- **错误类别** —— 用户可机器判别的稳定失败语义：`InvalidArgument`、`InvalidState`、`Configuration`、`Connection`、`Closed`、`Timeout`、`Cancelled`、`Io`、`Frame`、`Codec`、`ResourceExhausted`、`Unsupported`、`Internal`；可附带诊断文本。具体错误码、枚举和结果载体属于 API/设计说明，调用方不得依靠解析文本前缀分类。
- **匹配键（Key）** —— 请求↔应答配对键:外部协议 = `(session_id, message_id)`;DDS = `correlation_id`。
- **判别符** —— 帧类型:外部协议 `FrameType`(COMMAND/RESPONSE/RESULT/STATE/HEARTBEAT);DDS `MessageKind`。

## 机制

- **InteractionEngine / InteractionPolicy** —— **[as-built]** 共享交互引擎(挂起表/超时/重发/分发)+ 声明式协议策略(TagOf/KeyOf/NewCorrelation/EchoCorrelation/ReplyTo/RouteUnmatched)。**[target] 均去除**——语义内联进各 node。
- **IExecutor / ThreadExecutor** —— **[as-built]** 执行器缝 + 单 worker 线程池(决定业务回调在哪跑 + 定时)。**[target] 去除**——并发交给协程运行时。
- **协程运行时** —— **[target]** 框架统一的 M:N 协作式并发与时序环境；**AsyncTask 是需求级强制技术约束，不是可替换的推荐实现**。不同 node 可并行，同一 node 的消息分发和业务处理保持串行语义；具体 affinity、线程分配、调度和构建方式属于设计说明。
- **节点所属执行域** —— **[target]** 节点首次成功启动后绑定的稳定 AsyncTask 执行上下文。不同 node 可位于不同运行线程；同一 node 的可变状态在该域内串行访问，节点运行期间不迁移。
- **入站业务处理器** —— **[target]** 应用以组合方式注册、在 AsyncTask fiber 中处理普通入站业务消息的逻辑。它不是 Qt/DDS 底层回调；同一 node 同时只运行一个，其他 node 可并行。
- **请求等待 fiber** —— **[target]** 发起请求并等待唯一响应、超时、取消或连接错误的调用方协程。
- **连接状态观察器** —— **[target]** 观察 TCP 物理连接状态变化的应用能力，不承载入站业务处理。
- **连接代际** —— **[target]** 每次成功建立 TCP 物理连接后形成的隔离标识；旧代际的迟到数据和事件不得影响新代际。
- **配置版本** —— **[target]** 宿主提交的单调 TCP 连接管理配置快照版本；与连接代际是两个独立概念。
- **coro socket** —— **[target]** AsyncTask 封装的协程 I/O:`corosocket`(QAbstractSocket,**仅流式 readAll**)、`coroiodevice`(QIODevice,串口)、`corotcpserver`(accept)。**UDP 报文语义 corosocket 不支持** → `UdpTransport` 自桥 `receiveDatagram`。
- **DDS provider 交接边界** —— **[target]** DDS provider listener 样本安全进入节点所属执行域的行为边界；必须有界、非阻塞 listener、同 topic 保持框架接受顺序。它不要求存在名为 `DdsBridge` 的组件；具体投递机制、容量和溢出策略属于设计说明及尚未关闭的需求项。本地已经丢弃的样本不能由 DDS Reliable 自动恢复。
- **provider** —— DDS 底层库的抽象适配(`IDdsProvider`):Fast DDS / 进程内 `FakeDdsProvider`。

## 边界与非目标

- 框架**不解释 payload 业务语义**、**不抛异常**、**不是消息代理/路由守护进程**。
- `ITransport` 是库内部设计缝；编解码器则是用户可实现和注入的公共扩展点。
- `frm_type` 真实字节值与 CRC 算法为**外部常量**,经枚举占位 + `CrcFn` 注入预留,接入前替换、两端一致。
