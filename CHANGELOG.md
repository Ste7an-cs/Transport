# 变更日志（Changelog）

本项目的所有重要变更记录于此。格式借鉴 [Keep a Changelog](https://keepachangelog.com/)，版本遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

权威参考：[需求规格说明书（SRS）](docs/需求规格说明书.md) · [设计说明书（SDD）](docs/设计说明书.md)。

---

## [Unreleased]

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

[Unreleased]: https://github.com/Ste7an-cs/Transport/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.1.0
