# transport — C++ 通信中间件（协程原生目标架构）

一个 C++17 通信中间件库,把**传输**、**编解码**、**交互**三层彻底解耦,以 **AsyncTask 协程运行时**为强制异步运行环境:

- **Transport（纯字节管道）** —— 跨 TCP / UDP / 串口 / DDS 搬运原始字节或样本,不解释消息类型、请求关联或 payload 语义。介质无关的协程拉模型:`Read` 一次一片(流式任意切片 / 报文一整报)。
- **ICodec（线缆格式）** —— 在收发边界把逻辑 `Message` ↔ 线缆字节(分帧 + 序列化 + 校验 + 重同步);流式跨切片拼帧、报文式保边界。**应用可提供并装配的公共扩展点。**
- **node（交互层）** —— 在前两层之上组合请求关联、入站分发、超时、连接状态与协议交互。**薄壳组合,不共享交互引擎**:协议可观察语义由各 node 自实现,公共只复用协议无关的挂起-应答纪律与生命周期收敛。

> **非目标:** 不解析/解释 payload 业务语义,不是消息代理、通用路由守护进程,本版不内建加密/认证/访问控制。

---

## 当前状态（诚实说明）

本仓库正按 SDD 路线图(`docs/设计说明书-协程原生.md` §4)做**协程原生清洁重建**。

已交付 **P0–P5**(tag `v0.4.0`–`v0.4.5`);全量 270 tests 全绿。逐条 RT_* 追溯见 SDD §7 追溯矩阵。

- **P0 —— 目标骨架落位(`v0.4.0`):**
  - `transport::TcpTransport`(已建立连接的 TCP 字节管道):发送完成语义(帧字节全部进内核发送缓冲才报成功 + 协程背压 —— RT_TRANSPORT_008)、并发写按节点执行域到达顺序串行化(RT_TRANSPORT_007)、复用 `readAll` 流的读路径。
  - 统一的机器可判别错误模型 `transport::TransportErrc`(RT_ERROR_002/003),不靠解析字符串前缀分类;协作取消 `CancellationToken`、共享完成原语 `SharedCompletion`。
  - 抢救沿用的线缆 **codec**(`transport::codec::`)与 **DDS provider 适配**(`transport::dds::`);命名空间统一 `transport::` 顶层,单一 `transport` 库 + 单一测试目标,AsyncTask 强制依赖。
- **P1 —— TCP 最小请求-响应(`v0.4.1`):** 协议无关 `PendingTable<Key,T>` 挂起-应答薄基座(唯一登记/恰好一次完成/`FailAll`/取消纪律)、最小 `transport::ProtocolNode`(组合 transport+codec+PendingTable + 读-分发循环 + 可注入 `CorrelationKeyStrategy` + needresponse `Request`)、`TcpTransport` 读侧契约;真实 TCP 回环证实"无共享引擎、语义内联各 node"架构赌注(RT_DESIGN_003)。
- **P2 —— 节点加厚(`v0.4.2`):** 协议无关 `BoundedQueue<T>`(双上界/tail-drop/命名归因)、入站业务处理器(组合注册/单消费者 fiber 串行/异常隔离 —— RT_HANDLER 全)、`noresponse` `Send`、256 并发在途 + `session_id` LRU 退休、生命周期硬化(并发幂等 Start/多等待者/三方汇合/重入自锁防护 —— RT_LIFECYCLE 全)。
- **P3 —— 连接管理(`v0.4.3`):** `TcpClientTransport`(状态机 + 自动重连退避 + 连接代际 + `IConnectionObservable`,组合 P1 内层)、节点集成断连(reactor fiber + Read 透明跨重连)、运行时重配置(`ApplyConfig` 单调版本/校验原子/端点切换 —— RT_TCP_RECONNECT/RECONFIG 全);真实 TCP 断连-重连回环。
- **P4 —— 其余介质(`v0.4.4`):** `UdpTransport`(报文+地址)/`SerialTransport`(串口字节流)/`DdsTransport`(provider 跨线程有界交接,复用 `BoundedQueue`)、`DdsNode`(pub-sub + 多路请求-应答,correlation_id 键)、`TcpServer` accept(每连接一 node)、`NodeRuntime`(ProtocolNode/DdsNode 共享的协议无关机制)。统一寻址靠 `Endpoint`。**D10 复用证实**(DdsNode 复用 PendingTable 仅一行改动)、**跨线程交接闭合 ADR-0001 未决项**。
- **P5 —— 观测 + 完整性归因(`v0.4.5`):** 可插拔结构化 Trace(`ITraceSink`,push,9 类 category)+ 命名计数(pull);`DropReason` **六项**(P5 交付时为七项,「连接代际隔离丢弃」随 ADR-0004 D3 移除)+ `RecordDrop`/`RecordEvent` 协议无关观测原语;I/O 事实(`LastSendTime`/`LastReceiveTime`/`LastError`)统一为 base `ITransport` 强制接口;补齐请求时延/处理器时长/重连/关闭时延指标。**loss=0 harness** 断言"无静默丢失"结构性可验证(`Σ命名=总丢弃`)。

- **尚未实现(按路线图 P6 推进,勿当作已有):** 五种交互模式精确状态机与 `kFeedback`(TBD-001)、DDS 动态 Subscribe / 判活 QoS、串口自动重连(TBD-005)、性能/容量/两机验收与稳定性/时延基线固化(P6,TBD-004)。

> **as-built 归档:** 0.3.0 的异步交互栈(`comm/` 引擎/执行器/策略、第二期 `coro::InteractionEngine`、回调式传输)及其文档已在 P0 从 master 删除,完整存档于 git tag **`v0.3.0`**。

---

## 内部传输契约（`ITransport`,内部缝 —— 非用户 API）

`ITransport` 是介质无关的内部缝(RT_IN_INTERFACE_002),协程 await 式拉模型:

```cpp
class ITransport {
 public:
  virtual Status          Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;  // 一次一片
  virtual Status          Write(SendUnit unit) = 0;                  // 帧进内核才成功
  virtual Status          RequestClose() = 0;
  virtual Status          WaitClosed(OperationOptions options = {}) = 0;
};
```

- `Read` 返回一片 `Datagram{bytes, source}`;`Write` 收 `SendUnit{bytes, destination}`,并发写在节点执行域串行化,单帧字节不与另一帧交错。
- 同一实例同一时刻至多一个有效读操作;`OperationOptions` 携带可选 `deadline` 与 `CancellationToken`。
- 可失败操作一律返回 `Result<T>` / `Status`(标 `[[nodiscard]]`),不抛异常表达预期失败。

> **用户面定位:** 编程主入口将是**交互层 node**(组合装配,P1+ 落地),而非 `ITransport`;codec 是应用可提供的公共扩展点。

---

## 构建

```bash
git submodule update --init third_party/AsyncTask     # AsyncTask 为强制运行时(git 子模块)
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

**前置依赖:**

- **Qt5**(5.12+;Core / Network / SerialPort,系统安装,如 `libqt5serialport5-dev`)。
- **AsyncTask**(boost.fiber 协程运行时,`third_party/AsyncTask` 子模块)+ 已编译 boost `fiber/context/thread/chrono`;经缓存变量 `ASYNCTASK_DIR` / `BOOST_LOCAL_ROOT` 指定(当前为绝对路径默认,可 `-D` 覆盖)。子模块未初始化时 configure 直接 `FATAL_ERROR`。
- **Fast DDS 2.13.x(唯一可选外部依赖)**:未装时 `find_package` 自动跳过 `FastDdsProvider`,其余能力照常构建;装后自动启用(编译带 `TRANSPORT_HAS_FASTDDS`)。

GoogleTest 仍 vendored 到 `third_party/`。整个仓库合并为**单一 `transport` 静态库**(链接 `asynctask` + Qt + 可选 fastrtps)与**单一 `transport_tests` 可执行文件**(全部 `tests/*` 在 AsyncTask fiber 调度器内跑)。C++17,目标平台 Linux。

---

## 文档

权威参考(目标架构):

- **需求规格说明书(SRS)**:[`docs/需求规格说明书-协程原生.md`](docs/需求规格说明书-协程原生.md) —— 可观察/可验收行为,标识前缀 `RT_`。
- **设计说明书 + 分期路线图(SDD)**:[`docs/设计说明书-协程原生.md`](docs/设计说明书-协程原生.md) —— 三层与缝、node 组合、线程/执行域模型、P0–P6 路线图。
- **架构决策记录(ADR)**:[`docs/adr/`](docs/adr/) —— 0001 协程原生架构总纲、0002 发送/丢弃/生命周期、0003 SDD 与路线图。
- **项目术语(单一权威)**:[`CONTEXT.md`](CONTEXT.md)。
- **变更日志**:[`CHANGELOG.md`](CHANGELOG.md)。
- **as-built 存档**:git tag [`v0.3.0`](https://github.com/Ste7an-cs/Transport/releases/tag/v0.3.0) —— 0.3.0 的 as-built SRS/SDD 与实现。

### 关键约束（详见 SRS/SDD）

- **三层解耦**:传输不依赖逻辑消息或协议语义;codec 是公共扩展点;node 组合三者。〔RT_IN_INTERFACE_001/002〕
- **AsyncTask 强制运行时**:不设 `IExecutor`/`ThreadExecutor` 等独立业务调度体系;M:N 协作式,同一节点状态/关联/入站处理串行,不同节点可并行。〔RT_DESIGN_002、RT_CORO_RUNTIME〕
- **无共享交互引擎**:不设独立 `InteractionEngine`/`InteractionPolicy` 层;协议语义归各 node。〔RT_DESIGN_003、RT_NODE_003〕
- **不抛异常**:预期失败用 `Result<T>`/`Status`(`[[nodiscard]]`)+ 机器可判别的 `TransportErrc` 类别。〔RT_ERROR_001/002/003〕
- **发送完成语义 + 背压**:一次发送在帧字节全部离开框架用户态缓冲(进内核)后才报成功,背压经协程等待自然传导;不采用 fire-and-forget。〔RT_TRANSPORT_008〕
- **底层回调不碰节点状态**:Qt I/O / DDS listener 回调须安全转交节点所属执行域,不在回调线程执行业务处理器。〔RT_HANDLER_002、RT_NODE_004〕
