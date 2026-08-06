#include "transport/io/serial/SerialTransport.hpp"

#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>

#include <QByteArray>
#include <QPointer>
#include <QSerialPort>
#include <QString>

#include "await/awaitable.hpp"
#include "await/coroiodevice.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace transport {
namespace {

// 判定某 QSerialPort 错误是否为运行期致命(应触发 Closing→Closed)。TimeoutError
// 不截断流(同 TCP 读超时不停流);NoError 是清除信号。
// 其余(ResourceError 设备移除、ReadError/WriteError 读写故障、DeviceNotFound/
// Permission 等)在本传输"任一设备错误即致命断开"模型下均视为链路断裂。
bool IsFatalSerialError(QSerialPort::SerialPortError error) {
  return error != QSerialPort::NoError && error != QSerialPort::TimeoutError;
}

// 把 SerialConfig 的数据位/停止位/校验映射为 QSerialPort 枚举。
bool ApplyDataBits(QSerialPort& port, std::uint8_t bits) {
  switch (bits) {
    case 5: return port.setDataBits(QSerialPort::Data5);
    case 6: return port.setDataBits(QSerialPort::Data6);
    case 7: return port.setDataBits(QSerialPort::Data7);
    case 8: return port.setDataBits(QSerialPort::Data8);
    default: return false;
  }
}

bool ApplyStopBits(QSerialPort& port, std::uint8_t bits) {
  switch (bits) {
    case 1: return port.setStopBits(QSerialPort::OneStop);
    case 2: return port.setStopBits(QSerialPort::TwoStop);
    default: return false;
  }
}

bool ApplyParity(QSerialPort& port, char parity) {
  switch (parity) {
    case 'N': case 'n': return port.setParity(QSerialPort::NoParity);
    case 'E': case 'e': return port.setParity(QSerialPort::EvenParity);
    case 'O': case 'o': return port.setParity(QSerialPort::OddParity);
    default: return false;
  }
}

}  // namespace

struct SerialTransport::State {
  mutable std::mutex mutex;
  SerialConfig config;
  QPointer<QSerialPort> port;
  // 唯一的 readAll 流:持有一条、反复 await 取下一片(RT_TRANSPORT_003 流式一次一
  // 切片)。在 Start 建立,复用 coroiodevice 读原语。
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream;
  LifecycleState lifecycle{LifecycleState::kCreated};
  bool active_read{false};
  bool active_write{false};
  std::size_t send_waiters{0};
  // 并发写按到达顺序排队等待写槽(RT_TRANSPORT_004/007 串行化,不拒绝)。
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> write_queue;
  // 在途写的刷空等待者:设备致命错误时由 BeginClose 唤醒,避免写 fiber 永挂。
  std::shared_ptr<Coro::Awaitable<void>> active_flush;
  // 设备致命断开错误(非我方 RequestClose):作为**写路径**的失败原因(链路已断,
  // ADR-0004 D1 保留 kConnection 于写路径)与诊断事实;读路径不再区分成因,一律以
  // kClosed 收敛。
  std::error_code disconnect_error;
  std::optional<Clock::time_point> last_send;
  std::optional<Clock::time_point> last_recv;
  std::error_code last_error;
  SharedCompletion<void> closed;
};

namespace {

// 关闭一次:进入 Closing、关设备(止住错误风暴与设备句柄)、唤醒在途读/写等待者;
// 无在途操作时直接落 Closed 并完成 closed。有在途操作则由其收尾时落 Closed。
void BeginClose(const std::shared_ptr<SerialTransport::State>& state) {
  QPointer<QSerialPort> port;
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream;
  std::shared_ptr<Coro::Awaitable<void>> flush;
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> queued_writes;
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosed) {
      return;
    }
    state->lifecycle = LifecycleState::kClosing;
    port = state->port;
    read_stream = state->read_stream;
    flush = state->active_flush;
    queued_writes.swap(state->write_queue);  // 唤醒排队写等待者以 kClosed 收敛。
    if (!state->active_read && !state->active_write) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (port) {
    port->close();  // 关设备:止住 errorOccurred 风暴,断句柄。
  }
  if (read_stream) {
    read_stream->close(make_error_code(TransportErrc::kClosed));  // 唤醒在途 Read。
  }
  if (flush) {
    flush->close(make_error_code(TransportErrc::kClosed));  // 唤醒在途写刷空等待者。
  }
  for (const auto& gate : queued_writes) {
    gate->close(make_error_code(TransportErrc::kClosed));
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

// 设备致命错误回调(运行在 Qt 事件循环):记录断开原因并触发 Closing→Closed。
// 仅首个致命错误生效(errorOccurred 在断开时会连发,见实测);TimeoutError 忽略。
void OnDeviceError(const std::shared_ptr<SerialTransport::State>& state,
                   QSerialPort::SerialPortError error) {
  if (!IsFatalSerialError(error)) {
    return;
  }
  bool act = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle != LifecycleState::kRunning || state->disconnect_error) {
      return;  // 已在关闭或已记录首个断开。
    }
    // 任一设备致命错误 = 链路断裂(等同 TCP 对端断开),成因统一归 Connection
    // (写路径失败原因 + LastError 诊断);并触发 Closing→Closed(不重连,D3′),
    // 在途 Read 随流关闭以 kClosed 收敛(ADR-0004 D1)。
    const std::error_code mapped = make_error_code(TransportErrc::kConnection);
    state->disconnect_error = mapped;
    state->last_error = mapped;
    act = true;
  }
  if (act) {
    BeginClose(state);
  }
}

void FinishRead(const std::shared_ptr<SerialTransport::State>& state) {
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->active_read = false;
    if (state->lifecycle == LifecycleState::kClosing && !state->active_write) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

// 写槽持有者收尾:回退等待者计数,并把写槽移交队首等待者(FIFO 串行化);关闭中
// 则唤醒全部排队者以 kClosed 收敛。若正在关闭且无其他在途操作则落 Closed。
void ExitWrite(const std::shared_ptr<SerialTransport::State>& state) {
  std::shared_ptr<Coro::Awaitable<void>> next_gate;
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> closed_gates;
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->send_waiters > 0) {
      state->send_waiters -= 1;
    }
    if (state->lifecycle == LifecycleState::kClosing) {
      state->active_write = false;
      closed_gates.swap(state->write_queue);
      if (!state->active_read) {
        state->lifecycle = LifecycleState::kClosed;
        complete = true;
      }
    } else if (!state->write_queue.empty()) {
      next_gate = state->write_queue.front();  // 写槽移交队首,active_write 保持真。
      state->write_queue.pop_front();
    } else {
      state->active_write = false;
    }
  }
  for (const auto& gate : closed_gates) {
    gate->close(make_error_code(TransportErrc::kClosed));
  }
  if (next_gate) {
    next_gate->resolve();
    next_gate->close();
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

// 排队等待者在关闭时被唤醒(从未取得写槽):仅回退等待者计数。
void LeaveWriteQueue(const std::shared_ptr<SerialTransport::State>& state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->send_waiters > 0) {
    state->send_waiters -= 1;
  }
}

}  // namespace

SerialTransport::SerialTransport(SerialConfig config)
    : state_(std::make_shared<State>()) {
  state_->config = std::move(config);
}

SerialTransport::~SerialTransport() {
  BeginClose(state_);
  // port 由 State 持有;detached 的收尾 fiber 也持有 State,故 port 存活至最后一个
  // 引用释放。此处只请求删除,coroiodevice 内部用 QPointer 防悬空。
  if (state_->port) {
    state_->port->deleteLater();
  }
}

Status SerialTransport::Start() {
  const auto state = state_;
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->lifecycle == LifecycleState::kRunning) {
    return Status{};
  }
  if (state->lifecycle != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }

  // 在节点执行域 fiber 内创建并打开设备(同 TCP 的 socket 于执行域内建立)。
  auto* port = new QSerialPort();
  port->setPortName(QString::fromStdString(state->config.device));
  if (!port->open(QIODevice::ReadWrite)) {
    port->deleteLater();
    return make_error_code(TransportErrc::kConnection);
  }
  // 应用参数:任一失败即视为配置错误,关设备回退。
  if (!port->setBaudRate(static_cast<qint32>(state->config.baud_rate)) ||
      !ApplyDataBits(*port, state->config.data_bits) ||
      !ApplyStopBits(*port, state->config.stop_bits) ||
      !ApplyParity(*port, state->config.parity)) {
    port->close();
    port->deleteLater();
    return make_error_code(TransportErrc::kConfiguration);
  }

  state->port = port;
  // 断开检测:coroiodevice 只连 readyRead/aboutToClose,设备致命错误(拔线)只经
  // errorOccurred 暴露,故此处显式监听(实测断开时连发 ResourceError)。
  QObject::connect(port, &QSerialPort::errorOccurred, port,
                   [state](QSerialPort::SerialPortError error) {
                     OnDeviceError(state, error);
                   });
  state->lifecycle = LifecycleState::kRunning;
  // 建立唯一 readAll 流(持有一条、反复 await);初始 drain 收下订阅前已到达字节。
  // coroiodevice.readAll() 按值返回 Awaitable(区别于 corosocket 的 shared_ptr),
  // 故包一层 shared_ptr 以便被 State 长期持有、被 BeginClose 关闭。
  state->read_stream = std::make_shared<Coro::Awaitable<QByteArray>>(
      Coro::coro(port).readAll());
  return Status{};
}

Result<Datagram> SerialTransport::Read(OperationOptions options) {
  const auto state = state_;
  std::shared_ptr<Coro::Awaitable<QByteArray>> stream;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    if (state->active_read) {  // 单读:同一时刻至多一个有效读(RT_TRANSPORT_004)。
      return make_error_code(TransportErrc::kInvalidState);
    }
    state->active_read = true;
    stream = state->read_stream;
  }

  if (!stream) {
    FinishRead(state);
    return make_error_code(TransportErrc::kInternal);
  }

  // 复用持有的 readAll 流:每次 Read await 下一片(channel FIFO 保序)。读侧最小
  // 能力:以 deadline 界定单次读;要中断在途 Read 请 RequestClose(不逐读取消,
  // 与超时不停流不对称,同 TcpTransport)。
  Coro::Result<QByteArray, std::error_code> chunk =
      options.deadline
          ? Coro::await_for(stream, *options.deadline - Clock::now())
          : Coro::await(stream);

  Result<Datagram> result{make_error_code(TransportErrc::kInternal)};
  if (chunk) {
    const QByteArray& bytes = chunk.value();
    Datagram datagram;
    datagram.bytes.assign(
        reinterpret_cast<const std::uint8_t*>(bytes.constData()),
        reinterpret_cast<const std::uint8_t*>(bytes.constData()) + bytes.size());
    // 单设备无寻址:source 用中立默认目的地(destination 亦被忽略,收发同一设备)。
    datagram.source = Endpoint::Default();
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_recv = Clock::now();
    }
    result = Result<Datagram>{std::move(datagram)};
  } else if (chunk.error() == std::make_error_code(std::errc::timed_out)) {
    result = make_error_code(TransportErrc::kTimeout);  // 超时不停流,可再读。
  } else {
    // 流关闭:设备致命断开(disconnect_error 已记)或我方关闭。串口不重连,二者对
    // 调用方同一含义——传输终结、停止读取 → kClosed(RT_TRANSPORT_008 / ADR-0004
    // D1)。底层成因降为诊断事实,留在 LastError()(见 OnDeviceError)。
    result = make_error_code(TransportErrc::kClosed);
  }
  FinishRead(state);
  return result;
}

Status SerialTransport::Write(SendUnit unit) {
  const auto state = state_;
  std::shared_ptr<Coro::Awaitable<void>> slot_gate;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    state->send_waiters += 1;  // 自进入 Write 起计数(排队 + 在写),反映背压。
    if (!state->active_write) {
      state->active_write = true;  // 写槽空闲 → 立即取得。
    } else {
      slot_gate = std::make_shared<Coro::Awaitable<void>>();
      state->write_queue.push_back(slot_gate);
    }
  }

  if (slot_gate) {
    if (!Coro::await(slot_gate)) {
      LeaveWriteQueue(state);  // 关闭时被唤醒:从未取得写槽 → 仅回退计数。
      return make_error_code(TransportErrc::kClosed);
    }
  }

  // —— 已持有写槽 ——
  QPointer<QSerialPort> port;
  bool running = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    running = state->lifecycle == LifecycleState::kRunning;
    port = state->port;
  }
  if (!running) {  // 等待写槽期间已进入关闭。
    ExitWrite(state);
    return make_error_code(TransportErrc::kClosed);
  }

  // 失败即关设备,不自动重发:先进入关闭(排队写等待者以 kClosed 收敛),再释放写槽。
  auto fail = [&state](std::error_code mapped) -> Status {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    BeginClose(state);
    ExitWrite(state);
    return Status{mapped};
  };

  // 整帧全部交给 Qt 用户态发送缓冲并等其刷入设备(bytesToWrite()==0)才报告成功。
  // 处理短写:write() 返回 0≤n<total 时循环写剩余。背压经 await 传导。
  const char* data = reinterpret_cast<const char*>(unit.bytes.data());
  const qint64 total = static_cast<qint64>(unit.bytes.size());
  qint64 offset = 0;
  for (;;) {
    if (!port) {
      return fail(make_error_code(TransportErrc::kConnection));
    }
    const qint64 remaining = total - offset;
    if (remaining <= 0 && port->bytesToWrite() <= 0) {
      break;  // 整帧字节已全部进入设备发送缓冲 → 发送完成。
    }
    // waiter 先于本轮 write 创建,避免快速的 bytesWritten 被漏掉;登记到 State 使
    // BeginClose 能在设备断开时唤醒它(否则永挂——close 不触发 bytesWritten)。
    auto flushed = std::make_shared<Coro::Awaitable<void>>(
        Coro::coro(port.data()).waitForBytesWritten());
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->active_flush = flushed;
    }
    if (remaining > 0) {
      const qint64 n = port->write(data + offset, remaining);
      if (n < 0) {
        {
          std::lock_guard<std::mutex> lock(state->mutex);
          state->active_flush.reset();
        }
        return fail(make_error_code(TransportErrc::kIo));
      }
      offset += n;
    }
    Coro::Result<void, std::error_code> drained = Coro::await(flushed);
    std::error_code disconnect;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->active_flush.reset();
      disconnect = state->disconnect_error;
    }
    if (!drained) {
      // 刷空途中设备断裂 = 流式部分写失败(RT_TRANSPORT_004.4)。
      return fail(disconnect ? disconnect : make_error_code(TransportErrc::kIo));
    }
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_send = Clock::now();
  }
  ExitWrite(state);
  return Status{};
}

Status SerialTransport::RequestClose() {
  BeginClose(state_);
  return Status{};
}

Status SerialTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

std::size_t SerialTransport::SendWaiterDepth() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->send_waiters;
}

std::optional<SerialTransport::Clock::time_point> SerialTransport::LastSendTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_send;
}

std::optional<SerialTransport::Clock::time_point>
SerialTransport::LastReceiveTime() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_recv;
}

std::error_code SerialTransport::LastError() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_error;
}

// 串口的"链路可用"即设备已打开:Start 只在 open + 参数应用成功后落 Running;设备
// 致命断开(拔线)经 OnDeviceError→BeginClose 关设备并离开 Running。isOpen() 一并
// 核对设备句柄本身,避免 lifecycle 与设备实态脱节时误报可用。
LinkState SerialTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return (state_->lifecycle == LifecycleState::kRunning && state_->port &&
          state_->port->isOpen())
             ? LinkState::kUp
             : LinkState::kDown;
}

}  // namespace transport
