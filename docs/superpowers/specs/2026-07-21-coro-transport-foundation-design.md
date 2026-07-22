# 协程传输基础层设计

**日期：** 2026-07-21<br>
**状态：** 已确认，待实施<br>
**范围：** 协程原生迁移方案 1 / 第一阶段<br>
**需求基线：** `docs/需求规格说明书-协程原生.md`<br>
**架构基线：** `docs/adr/0001-coroutine-native-interaction-architecture.md`

## 1. 目标

本阶段建立后续 TCP、UDP、串口、DDS 和 node 协程化共同依赖的最小基础层：

- 统一使用 AsyncTask 的 `Coro::Result` 表达协程栈成功或失败；
- 提供稳定、机器可判别的 Transport 错误类别；
- 定义库内部的协程式纯传输契约及其数据类型；
- 提供协作取消和一次完成、多 fiber 等待的基础原语；
- 提供无需真实介质的 `FakeTransport` 测试替身；
- 保持现有回调栈、公共 API 和测试不变。

本阶段不实现真实介质、node、Codec 重构、请求关联、重连、热加载或五种协议交互行为。

## 2. 依赖基线

AsyncTask 子模块更新到 `67b71a7e6e143952693bf845c1e032debb22a342`。该版本提供：

- TCP、UDP datagram、local socket 和 SSL awaitable；
- socket awaitable 的共享 handle；
- 确定性的正常关闭、错误关闭和订阅清理；
- 跨线程安全并在关闭时唤醒全部等待者的 `FiberChannel`；
- `await_for` 超时只终止本次等待、不隐式取消底层操作的明确契约。

因此后续真实传输直接组合这些 awaitable，不再重复实现 Qt signal 到 fiber 的通用桥。DDS provider 的非 Qt listener 仍需单独设计有界线程交接边界。

## 3. 兼容与构建边界

迁移采用双栈渐进方式：

```text
现有调用方 ── transport（回调栈，保持不变）

新协程 node ── transport_coro（协程栈，逐阶段替换）
                    │
                    └── AsyncTask / Qt awaitable
```

- 现有 `transport::ITransport`、`transport::Result<T>`、`InteractionEngine`、`InteractionPolicy` 和 `ThreadExecutor` 暂不删除。
- 新类型位于 `transport::coro`，避免与旧栈名称和 ABI 冲突。
- `transport_coro` 继续作为可开关目标；非协程和 Fast DDS 未安装场景仍可构建。
- 只有完成相应替代能力和回归测试后，才在后续阶段讨论旧栈删除。

## 4. 结果与错误模型

### 4.1 结果类型

协程栈不实现新的 expected/result 容器，而是为 AsyncTask 类型提供项目内别名：

```cpp
namespace transport::coro {

template<typename T>
using Result = Coro::Result<T, std::error_code>;

using Status = Coro::Result<void, std::error_code>;

}
```

旧栈继续使用 `transport::Result<T>`。新代码不得依赖旧结果中的字符串前缀。

### 4.2 Transport 错误类别

定义 `TransportErrc`，至少包含：

```text
InvalidArgument     InvalidState       Configuration
Connection          Closed             Timeout
Cancelled           Io                 Frame
Codec               ResourceExhausted  Unsupported
Internal
```

并提供：

- `transport_error_category()`；
- `make_error_code(TransportErrc)`；
- `std::is_error_code_enum<TransportErrc>` 特化；
- 每个错误码稳定的诊断文本。

公共协程边界统一返回 `transport.error` category。Qt、串口、DDS provider 和系统错误在最接近发生处映射；原始 category/value、操作名和对象信息进入结构化 Trace，而不是要求调用方解析文本。

映射原则：

| 来源 | 公共错误 |
|---|---|
| 非法参数/目的地址 | `InvalidArgument` |
| 生命周期或并发契约违规 | `InvalidState` |
| 配置校验失败 | `Configuration` |
| 远端断开、连接拒绝、连接代际失效 | `Connection` |
| 用户主动关闭 | `Closed` |
| 本次等待超时 | `Timeout` |
| 调用方协作取消 | `Cancelled` |
| 其他介质读写失败 | `Io` |
| 第三方异常或内部不变量破坏 | `Internal` |

AsyncTask stream 的正常终止 `std::errc::no_message` 是运行时内部信号；映射层根据上下文将其转换为成功结束或 `Closed`，不直接作为公共错误暴露。

## 5. 数据类型

### 5.1 Datagram

`Datagram` 是介质无关的接收单元：

```cpp
struct Datagram {
  std::vector<std::uint8_t> bytes;
  Endpoint source;
};
```

- TCP 和串口：一次 `Read` 返回任意字节切片，不代表完整业务帧；`source` 在连接存续期间固定。
- UDP：一次 `Read` 返回一个完整数据报及发送方地址和端口。
- DDS：一次 `Read` 返回一个完整 sample 及足以路由的 topic 信息。

### 5.2 SendUnit

`SendUnit` 是介质无关的发送单元：

```cpp
struct SendUnit {
  std::vector<std::uint8_t> bytes;
  Endpoint destination;
};
```

payload 在异步写入完成前由发送单元拥有，避免悬空引用。UDP/DDS 实现必须以一个完整单元发送，不得拆分或截断。

### 5.3 OperationOptions

`OperationOptions` 携带可选 deadline 和取消 token。deadline 使用 `std::chrono::steady_clock`，避免系统时钟调整影响超时。

未配置 deadline 表示不设置本次等待时限；空 token 表示调用方不单独取消。关闭仍可独立唤醒操作。

## 6. 协作原语

### 6.1 CancellationSource / CancellationToken

取消对象共享一个小型状态：

- `CancellationSource::Cancel()` 幂等；
- token 可查询是否已取消，并可让 fiber 等待取消；
- 内部操作可注册轻量取消通知，并通过 RAII registration 在操作结束时注销；
- 取消唤醒全部等待者；
- token 不拥有或强杀任务，不等同于 `FiberTask::cancel()`；
- 已取消 token 上的新等待立即完成。

共享状态用原子标志完成快速查询，用短时互斥保护通知登记；`Cancel()` 在锁外调用已取出的内部通知，避免重入死锁。通知只用于关闭本次操作的 awaitable 或唤醒内部等待，不直接执行用户业务代码。等待实现复用 AsyncTask `FiberChannel` 的关闭广播语义。

### 6.2 SharedCompletion

`SharedCompletion<T>` 表示一次完成、多 fiber 观察：

- 首次完成保存 `Result<T>`，后续完成尝试无效；
- 完成后唤醒全部等待者；
- 完成前和完成后加入的等待者取得同一结果；
- 每个可取消等待者使用自己的 awaitable；单个等待者超时或取消不得关闭其他等待者的通知源；
- 完成者在保存结果后取出当前等待者列表，在锁外逐个完成；
- 内部同步必须支持不同 AsyncTask worker 上的 fiber。

该原语用于关闭完成等广播场景，不用于传输数据流。

## 7. 协程传输契约

`transport::coro::ITransport` 是库内部装配缝，不属于用户公共扩展 API。概念接口为：

```cpp
class ITransport {
 public:
  virtual ~ITransport() = default;

  virtual Status Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;
  virtual Status Write(SendUnit unit) = 0;
  virtual Status RequestClose() = 0;
  virtual Status WaitClosed(OperationOptions options = {}) = 0;
};
```

确切参数传值方式可在实施时根据所有权和编译诊断微调，但不得改变以下语义。

### 7.1 Start

- 启动成功后可读写。
- 已启动时再次启动成功。
- `Closing` 或 `Closed` 时返回 `InvalidState`。
- 配置校验必须先于底层资源创建；失败不产生半初始化资源。

传输自身的初始化状态是内部资源状态，不取代 SRS 规定的 node 生命周期。

### 7.2 Read

- 每实例最多一个活动 `Read`。
- 第二个并发 `Read` 立即返回 `InvalidState`。
- deadline 到达返回 `Timeout`；token 取消返回 `Cancelled`。
- 读取超时或取消只终止本次调用，不隐式关闭持久底层接收源。
- AsyncTask `await_for` 超时后，Transport 必须显式关闭本次 read awaitable 以注销 Qt 订阅，但不得关闭 socket、串口或 DDS reader 本身。
- `RequestClose` 唤醒活动读取并返回 `Closed`。
- 读取成功后释放活动读取资格。

### 7.3 Write

传输层不维护写入队列。`Write` 只执行一个发送单元的物理写入：

- 每实例最多一个活动物理写入；内部错误地并发调用返回 `InvalidState`。
- 并发公共发送由后续 node 发送协调器串行化，避免 node 与 transport 双重排队。
- node 发送协调器在调用 `Write` 前检查总 deadline/取消；已经失效的发送不得进入 Transport。
- 一旦调用 `Write`，专用发送协调 fiber 等待其物理完成。调用方请求可先以 `Timeout`/`Cancelled` 完成，但不得取消或遗弃正在执行的 `Write`。
- 流式部分写失败返回 `Io` 或 `Connection` 并关闭当前物理连接。
- UDP/DDS 在发送前检查单元大小和地址，不得静默截断。

跨 fiber 公共发送排序仍为 SRS TBD；本设计只保证单个发送单元不交错。

### 7.4 Close

- `RequestClose` 幂等，只发起关闭，不等待当前调用者退出。
- 首次调用使新读写返回 `Closed`，并唤醒活动读取。
- `WaitClosed` 可由多个 fiber 同时调用。
- 只有内部 I/O 和资源清理完成后，`WaitClosed` 才成功。
- 已完全关闭时再次请求关闭或等待关闭直接成功。

node 的 `Created → Running → Closing → Closed`、并发 Start 共享结果和 handler 自关闭规则在 node 阶段实现；本阶段不提前建立通用生命周期控制器。

## 8. FakeTransport

`FakeTransport` 位于协程测试支持目录，完整实现 `coro::ITransport`，提供确定性控制点：

- 注入接收单元或接收错误；
- 记录成功开始的发送单元；
- 暂停/放行物理写入，用于验证活动写约束；
- 配置启动失败、完整写失败和部分写失败；
- 主动模拟远端断开或本地关闭；
- 查询活动读写数和关闭状态，仅供测试断言。

Fake 不启动系统线程，不依赖真实时钟或真实网络；所有等待都在 AsyncTask fiber 中完成。

## 9. 测试策略

实施采用 TDD，每个行为先写失败测试，再写最小实现。第一阶段测试至少覆盖：

1. 13 个 `TransportErrc` 的 category、数值比较和非空文本；
2. AsyncTask `Coro::Result` 别名的成功、失败和 `void` 结果；
3. 取消前、取消后及多个取消等待者；
4. `SharedCompletion` 首次完成胜出、多等待者和完成后等待；
5. Fake 启动幂等与关闭后不可启动；
6. 单活动读取、读取成功、超时、取消、关闭唤醒；
7. 单活动写入、错误并发写、完整写和部分写失败；
8. `RequestClose`/`WaitClosed` 幂等和多等待者；
9. 现有 `transport_tests` 与 `transport_coro_tests` 全部回归。

涉及本机 TCP/UDP/Fast DDS 的测试必须在允许本机网络资源的环境执行；沙箱内 bind 权限失败不得误判为代码回归。

## 10. 预期文件布局

```text
include/transport/coro/
  Error.hpp
  Result.hpp
  Cancellation.hpp
  SharedCompletion.hpp
  TransportTypes.hpp
  ITransport.hpp

src/coro/
  Error.cpp
  Cancellation.cpp              # 仅在非头文件实现确有必要时

tests/coro/
  error_test.cpp
  cancellation_test.cpp
  shared_completion_test.cpp
  fake_transport.hpp
  fake_transport_test.cpp
```

最终文件拆分可为减少模板链接复杂度做小幅调整，但公共依赖方向必须保持 `node → coro ITransport → AsyncTask`，传输层不得依赖 Codec、逻辑 Message 或协议状态机。

## 11. 需求追踪

| 设计部分 | 主要需求 |
|---|---|
| AsyncTask Result 与等待 | `RT_CORO_RUNTIME_001`、`RT_ERROR_001` |
| 稳定 error_code | `RT_ERROR_002`、`RT_ERROR_003` |
| Datagram/SendUnit | `RT_TRANSPORT_002`、`003`、`005` |
| 单活动读写 | `RT_TRANSPORT_004` |
| 取消和 deadline | `RT_TRANSPORT` 输入输出、`RT_LIFECYCLE` |
| Close 多等待者 | `RT_LIFECYCLE_004`、`005`、`006` |
| FakeTransport | `RT_IN_INTERFACE_005` |
| 双栈迁移 | ADR D7、D8、D13 |

## 12. 后续阶段

第一阶段完成并验收后，按既定顺序推进：

1. TCP、UDP、串口、DDS 协程传输；
2. node 执行域、发送协调器、请求关联、handler 队列和关闭；
3. TCP 自动重连、连接代际和配置热加载；
4. `ProtocolNode`、`DdsNode`，届时逐项确认五种协议交互细节；
5. 可观测性、容量、性能和稳定性验收。
