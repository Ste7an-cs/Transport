# transport — C++ 通信中间件（协程原生目标架构）

一个 C++17 通信中间件库,把**传输**、**编解码**、**交互**三层彻底解耦,以 **AsyncTask 协程运行时**为强制异步运行环境:

- **Transport（纯字节管道）** —— 跨 TCP / UDP / 串口 / DDS 搬运原始字节或样本,不解释消息类型、请求关联或 payload 语义。介质无关的协程拉模型:`Read` 一次一片(流式任意切片 / 报文一整报)。
- **ICodec（线缆格式）** —— 在收发边界把逻辑 `Message` ↔ 线缆字节(分帧 + 序列化 + 校验 + 重同步);流式跨切片拼帧、报文式保边界。**应用可提供并装配的公共扩展点。**
- **node（交互层）** —— 在前两层之上组合请求关联、入站分发、超时、连接状态与协议交互。**薄壳组合,不共享交互引擎**:协议可观察语义由各 node 自实现,公共只复用协议无关的挂起-应答纪律与生命周期收敛。

> **非目标:** 不解析/解释 payload 业务语义,不是消息代理、通用路由守护进程,本版不内建加密/认证/访问控制。

---

## 当前状态（诚实说明）

本仓库正按 SDD 路线图(`docs/设计说明书-协程原生.md` §4)做**协程原生清洁重建**。

- **P0 已完成 —— 目标骨架落位:**
  - `transport::TcpTransport`(已建立连接的 TCP 字节管道):发送完成语义(帧字节全部进内核发送缓冲才报成功 + 协程背压 —— RT_TRANSPORT_008)、并发写按节点执行域到达顺序串行化(RT_TRANSPORT_007)、复用 `readAll` 流的读路径。
  - 统一的机器可判别错误模型 `transport::TransportErrc`(`InvalidArgument`/`InvalidState`/`Connection`/`Closed`/`Timeout`/`Cancelled`/`Io`/`Frame`/`Codec`/`ResourceExhausted`/`Unsupported`/`Internal` 等 —— RT_ERROR_002/003),不靠解析字符串前缀分类。
  - 协作取消 `CancellationToken`、共享完成原语 `SharedCompletion`。
  - 抢救沿用的线缆 **codec**(`transport::codec::` 下 `SystemCodec`/`LengthFieldCodec`/`DatagramCodec`/`SystemDatagramCodec`/`DdsCodec` 的帧布局 / 流式扫描 / 重同步 / 可注入 `CrcFn`)与 **DDS provider 适配**(`transport::dds::` 下 `IDdsProvider` / `FakeDdsProvider` / 可选 `FastDdsProvider`)。
  - 命名空间统一到 `transport::` 顶层;CMake 合并为**单一 `transport` 库 + 单一测试目标**,AsyncTask 为强制依赖。

- **尚未实现(按路线图 P1–P6 推进,勿当作已有):** 交互节点(`ProtocolNode` / `DdsNode`)、内联 `PendingTable` 请求关联、入站有界业务队列、节点生命周期、`TcpClientTransport` 连接管理与自动重连、UDP/串口/DDS 传输、结构化 Trace 与丢弃归因、性能验收。

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
