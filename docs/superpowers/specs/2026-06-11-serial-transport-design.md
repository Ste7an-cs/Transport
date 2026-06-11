# 串口传输 — 实现设计 spec

> 本 spec 是主设计文档 `2026-06-09-transport-middleware-design.md` §8（串口传输）的**实现层细化**。主 spec 已锁定 `SerialConfig` 与「串口为字节流、接收侧分帧规则同 TCP」；本 spec 锁定实现细节：I/O 后端、类结构、串口参数配置、错误语义与测试策略，供后续 plan 直接落地。

**Goal:** 在 Foundation 层（`TransportCore` + `FrameAssembler` + `ReceiveQueue`）之上实现串口传输（单个 `SerialImpl`），基于 `asio::serial_port` 的异步单线程 I/O 模型，以 pty 回环集成测试验证。

**Tech Stack:** C++17、Standalone Asio（已集成；`asio::serial_port`）、GoogleTest 1.14、POSIX `openpty`（测试）、Google C++ 风格。

**与 TCP 的关系：** 串口与 TCP 连接同为**流式传输**——接收侧同样用 `FrameAssembler` + `IFramer` 切帧、组合 `TransportCore` 交付。区别只在：底层是 `asio::serial_port`（而非 tcp socket）、**无连接/无 connect/无重连**、`Open()` 需配置串口参数（波特率等）。`SerialImpl` 结构上近似「自有 io 线程的 `TcpConnectionImpl`，去掉 connect/重连」。

---

## 1. 范围与文件布局

单个 `SerialImpl : public ITransport`（串口无扩展接口，是普通 `ITransport`）。

```
include/transport/serial/
├── SerialConfig.hpp     # 串口配置（复述自主 spec §8）
└── SerialImpl.hpp       # 单类实现
src/serial/
└── SerialImpl.cpp
tests/serial/
├── serial_interfaces_test.cpp   # 配置默认值
└── serial_transport_test.cpp    # pty 回环：透传/分帧/codec/断连
```

第三方依赖 Standalone Asio 已集成（`asio::serial_port` 随 Asio 提供，无需新增）；测试用 POSIX `<pty.h>` 的 `openpty`（链接 `util` 库，仅测试目标需要）。

---

## 2. 配置结构（复述自主 spec §8）

```cpp
struct SerialConfig {
  std::string device;              // 例如 "/dev/ttyS0"
  uint32_t    baud_rate  = 115200;
  uint8_t     data_bits  = 8;
  uint8_t     stop_bits  = 1;      // 1 或 2
  char        parity     = 'N';    // 'N'（无）/ 'E'（偶）/ 'O'（奇）
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
};
```

实现补充约定：
- `framer` 提供时构造 `LengthFieldFramer`（构造前 `ValidateConfig`，非法则 `Open()` 返回 `config:` 错误）；不提供则 `FrameAssembler(nullptr)` 透传。
- `parity` 映射 `asio::serial_port_base::parity`：`'N'→none`、`'E'→even`、`'O'→odd`；其它字符 → `config:` 错误。
- `data_bits` → `character_size`；`stop_bits` 1/2 → `stop_bits::one`/`two`（其它 → `config:` 错误）；流控固定 `flow_control::none`。

---

## 3. `SerialImpl`（单类，组合 `TransportCore`）

```cpp
class SerialImpl : public ITransport,
                   public std::enable_shared_from_this<SerialImpl> {
 public:
  explicit SerialImpl(SerialConfig config);
  ~SerialImpl() override;

  Status Open() override;   // 打开串口 + 配置参数 + 启动接收循环
  void Close() override;    // 关 port + core_.Close() + 停 io 线程
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;  // core_.EncodeForSend → strand async_write

  // 接收侧：一行转发给 core_
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

 private:
  void StartRead();
  void DoWrite();
  void HandleDisconnect(const std::string& reason);

  SerialConfig config_;
  TransportCore core_;
  FrameAssembler assembler_;            // 按 config.framer 构造
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::serial_port port_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> disconnected_{false};  // 断连只处理一次
};
```

### 3.1 `Open()`：打开 + 配置参数

1. `framer` 非空则先 `LengthFieldFramer::ValidateConfig`，非法 → `config:` 错误。
2. `port_.open(config_.device, ec)`；失败 → `config: open <device>: ...`。
3. 用 `set_option` 配置（任一失败 → `config:` 错误并关 port）：
   - `serial_port_base::baud_rate(config_.baud_rate)`
   - `serial_port_base::character_size(config_.data_bits)`
   - `serial_port_base::stop_bits(...)`（1→one / 2→two，否则 config:）
   - `serial_port_base::parity(...)`（N/E/O 映射，否则 config:）
   - `serial_port_base::flow_control(none)`
4. `open_ = true`，`asio::post(strand_, StartReceive)`，返回成功。

> 注：在 pty 上，个别 `set_option`（如某些波特率）可能不被支持。测试使用 pty 默认接受的配置（如 9600/115200、8N1）；若实测某项在 pty 上失败，plan 中对该项设置采取 best-effort（容错继续，不影响数据收发），真实串口仍严格配置。该细节在 plan 的测试步骤中按实测确定。

### 3.2 收 / 发 / 断连（与 TCP 连接同构）

- **收：** io 线程 `port_.async_read_some(buffer(read_buf_), bind_executor(strand_, ...))`。回调：
  - 无错误：`assembler_.Feed(read_buf_, n)` 切帧（透传则整段为一帧），逐帧 `core_.DeliverFrame(frame, config_.device, "")`（source = 设备路径，topic 为空）。Feed 返回 `frame:` 错误 → `HandleDisconnect(error)`。再 `StartRead()`。
  - 错误（设备拔出/eof）：`HandleDisconnect("conn: " + ec.message())`。
- **发：** `Send` → `core_.EncodeForSend` → 经 `strand_` 串行化 `async_write`（同 TCP 写队列）。返回 `Status` 表示「已入队写出」。未打开（`open_==false`）→ `config: serial not open` 错误。
- **断连：** `HandleDisconnect`（`disconnected_` 闩保证一次）：`open_=false`、关 port、`core_.DeliverError(reason)`、`core_.Close()`、`core_.NotifyDisconnect(reason)`。**不重连**（串口 config 无 `auto_reconnect`；设备消失即终态）。

### 3.3 生命周期

- 自有 `io_context` + 1 后台 io 线程（构造时起，`Close()`/析构时停）。
- `Close()`（幂等，`closing_` 闩）：`open_=false`；strand 上关 port；`core_.Close()`；`guard_.reset()` + `ctx_.stop()` + join。

---

## 4. 线程模型与错误语义

- 并发与 TCP/UDP 一致：每实例一个 `io_context` + 一个 io 线程；async 经 strand；handler 用 `shared_from_this` 保活；`make_shared` 持有。
- 接收落入线程安全的 `ReceiveQueue`（`core_` 内），应用线程用三模式取。

错误前缀（沿用主 spec §3）：

| 前缀 | 串口场景 |
|------|----------|
| `config:` | device 打不开、串口参数非法/设置失败、framer 配置非法、未打开时操作 |
| `frame:` | 接收侧分帧非法（同 TCP） |
| `conn:` / `io:` | read/write 运行期错误、设备消失 |
| `codec:` | 挂载 codec 的 Encode/Decode 失败 |

---

## 5. 测试策略（pty 回环）

用 `openpty(&master_fd, &slave_fd, name, nullptr, nullptr)` 造一对主从伪终端；取从端路径 `ptsname(master_fd)`，`SerialImpl` 以该路径为 `device`；测试代码直接读写**主端 fd**（裸 `::read`/`::write`）。用 `ReceiveQueue.Receive(timeout_ms)` 同步等待保证确定。

**`serial_interfaces_test`：** `SerialConfig` 默认值（baud 115200、8N1、framer 空）。

**`serial_transport_test`（pty 回环）：**
- **透传收发**：主端 `write` 若干字节 → `SerialImpl.Receive` 收到，`Message.source == device`。
- **带 `LengthFieldFramer` 跨读分帧**：主端分两次 write 一帧的两半 → 切出整帧；一次 write 两帧 → 切出两帧。
- **codec 双向**：`ShiftCodec` 收发各 +1/-1 正确。
- **`Send` 写到对端**：`SerialImpl.Send` → 主端 `read` 收到（含 codec 时为 Encode 后字节）。
- **对端关闭/出错 → 断连**：关主端 fd → `SerialImpl.OnDisconnect` 触发、`Receive` 返回 `conn:`。
- **非法配置**：`parity='X'` 或 `stop_bits=3` → `Open()` 返回 `config:` 错误。

> 若某条 `set_option` 在 CI/沙箱的 pty 上不被支持，测试改用该环境可接受的参数；数据收发与分帧断言不依赖具体波特率。

---

## 6. 与 Foundation / 主 spec 的衔接

- 组合 `TransportCore`（编解码、三模式接收交付、断连通知、时间戳）、`FrameAssembler` + `LengthFieldFramer`（接收侧分帧）、`ReceiveQueue`。
- 满足主 spec §8 全部公共 API（`SerialConfig` + 普通 `ITransport`）。
- 无新增对外接口。`SerialImpl` 与 `TcpConnectionImpl` 同为「组合 `TransportCore` + `FrameAssembler` 的流式传输」，可对照实现。

## 7. 后续（不在本 spec 范围）

DDS（主 spec §7）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。
