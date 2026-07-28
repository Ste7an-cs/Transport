#include "transport/TcpTransport.hpp"

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <utility>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QPointer>

#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "transport/Error.hpp"
#include "transport/SharedCompletion.hpp"

namespace transport {
namespace {

// 把 Qt socket 错误映射到传输错误类别:对端主动关闭 / 网络层断裂归 Connection,
// 其余读写故障归 Io(RT_TRANSPORT_004.4 允许 Io 或 Connection)。
std::error_code MapSocketError(std::error_code error) {
  if (error.category() == Coro::detail::socket_error_category()) {
    switch (static_cast<QAbstractSocket::SocketError>(error.value())) {
      case QAbstractSocket::RemoteHostClosedError:
      case QAbstractSocket::NetworkError:
      case QAbstractSocket::ConnectionRefusedError:
        return make_error_code(TransportErrc::kConnection);
      default:
        return make_error_code(TransportErrc::kIo);
    }
  }
  return make_error_code(TransportErrc::kIo);
}

}  // namespace

struct TcpTransport::State {
  mutable std::mutex mutex;
  QPointer<QAbstractSocket> socket;
  // 唯一的 readAll 流(channel 支撑):持有一条、反复 await 取下一片(RT_TRANSPORT_003
  // 流式一次一切片)。在 Start 建立,复用修好的 corosocket 读原语。
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream;
  LifecycleState lifecycle{LifecycleState::kCreated};
  // 对端地址,Start 时从已连接 socket 缓存(对已连接 TCP 恒定):Read 返回的
  // Datagram.source 恒填为对端 Endpoint::Net(TCP from 恒为对端,见 CONTEXT.md)。
  std::string peer_host;
  std::uint16_t peer_port{0};
  bool active_read{false};
  bool active_write{false};  // 写槽是否被占。
  std::size_t send_waiters{0};
  // 并发写按到达顺序排队等待写槽(RT_TRANSPORT_004/007 串行化,不拒绝)。
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> write_queue;
  std::optional<Clock::time_point> last_send;
  std::optional<Clock::time_point> last_recv;
  std::error_code last_error;
  SharedCompletion<void> closed;
};

namespace {

// 关闭一次:进入 Closing、撕 socket 唤醒在途读/写等待者;无在途操作时直接落到
// Closed 并完成 closed。有在途操作则由其收尾时(FinishRead/FinishWrite)完成。
void BeginClose(const std::shared_ptr<TcpTransport::State>& state) {
  QPointer<QAbstractSocket> socket;
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream;
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> queued_writes;
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosed) {
      return;
    }
    state->lifecycle = LifecycleState::kClosing;
    socket = state->socket;
    read_stream = state->read_stream;
    queued_writes.swap(state->write_queue);  // 唤醒排队写等待者以 kClosed 收敛。
    if (!state->active_read && !state->active_write) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (socket) {
    socket->abort();  // 立即唤醒在途读写等待者(以错误收敛)。
  }
  if (read_stream) {
    read_stream->close(make_error_code(TransportErrc::kClosed));  // 唤醒在途 Read。
  }
  for (const auto& gate : queued_writes) {
    gate->close(make_error_code(TransportErrc::kClosed));
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

void FinishRead(const std::shared_ptr<TcpTransport::State>& state) {
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
// 则唤醒全部排队者以 kClosed 收敛,不再移交。若正在关闭且无其他在途操作则落 Closed。
void ExitWrite(const std::shared_ptr<TcpTransport::State>& state) {
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
void LeaveWriteQueue(const std::shared_ptr<TcpTransport::State>& state) {
  std::lock_guard<std::mutex> lock(state->mutex);
  if (state->send_waiters > 0) {
    state->send_waiters -= 1;
  }
}

}  // namespace

TcpTransport::TcpTransport(QAbstractSocket* connected_socket)
    : state_(std::make_shared<State>()) {
  state_->socket = connected_socket;
}

TcpTransport::~TcpTransport() {
  BeginClose(state_);
  // socket 由 State 持有;detached 的刷完 fiber 也持有 State,故 socket 存活至最后
  // 一个引用释放。此处只请求删除,corosocket 内部用 QPointer 防悬空。
  if (state_->socket) {
    state_->socket->deleteLater();
  }
}

Status TcpTransport::Start() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    if (!state_->socket) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    state_->lifecycle = LifecycleState::kRunning;
    // 缓存对端地址(已连接 TCP 恒定):Read 返回的 Datagram.source 恒填为对端。
    state_->peer_host = state_->socket->peerAddress().toString().toStdString();
    state_->peer_port = state_->socket->peerPort();
    // 建立唯一 readAll 流(持有一条、反复 await);初始 drain 会收下订阅前已到达的
    // 字节,故不丢首片。await_for 超时不停止该流,可再次 Read。
    state_->read_stream = Coro::coro(state_->socket.data()).readAll();
    return Status{};
  }
  if (state_->lifecycle == LifecycleState::kRunning) {
    return Status{};
  }
  return make_error_code(TransportErrc::kInvalidState);
}

Result<Datagram> TcpTransport::Read(OperationOptions options) {
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

  // 复用持有的 readAll 流:每次 Read await 下一片(channel FIFO 保序、单发自清理)。
  // 读侧为最小能力(见 spec out-of-scope):以 deadline 界定单次读;要中断在途 Read
  // 请关闭传输(RequestClose→关流→Read 返回 Closed)。不逐次响应 cancellation token
  // ——持久单流被逐读取消 close 会永久终止整条读流,与超时(不停流、可再读)不对称。
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
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_recv = Clock::now();
      // TCP from 恒为对端:source 填 Start 时缓存的对端地址(CONTEXT.md 读-分发循环)。
      datagram.source = Endpoint::Net(state->peer_host, state->peer_port);
    }
    result = Result<Datagram>{std::move(datagram)};
  } else if (chunk.error() == std::make_error_code(std::errc::timed_out)) {
    result = make_error_code(TransportErrc::kTimeout);
  } else if (chunk.error().category() == Coro::detail::socket_error_category()) {
    std::error_code mapped = MapSocketError(chunk.error());
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    result = mapped;
  } else {
    // channel 无错误关闭(对端正常关闭)或我方关闭:关闭中 → Closed,否则对端断
    // 开 → Connection。
    bool closing;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      closing = state->lifecycle != LifecycleState::kRunning;
    }
    result = make_error_code(closing ? TransportErrc::kClosed
                                     : TransportErrc::kConnection);
  }
  FinishRead(state);
  return result;
}

Status TcpTransport::Write(SendUnit unit) {
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
    // 发送等待者:自进入 Write 起计数(排队 + 在写),反映发送侧背压积压(3.4.4)。
    state->send_waiters += 1;
    if (!state->active_write) {
      state->active_write = true;  // 写槽空闲 → 立即取得。
    } else {
      // 写槽被占 → 按到达顺序排队等待(RT_TRANSPORT_004/007 串行化,不拒绝)。
      slot_gate = std::make_shared<Coro::Awaitable<void>>();
      state->write_queue.push_back(slot_gate);
    }
  }

  if (slot_gate) {
    if (!Coro::await(slot_gate)) {
      // 关闭时被唤醒:从未取得写槽 → 仅回退等待者计数。
      LeaveWriteQueue(state);
      return make_error_code(TransportErrc::kClosed);
    }
  }

  // —— 已持有写槽 ——
  QPointer<QAbstractSocket> socket;
  bool running = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    running = state->lifecycle == LifecycleState::kRunning;
    socket = state->socket;
  }
  if (!running) {  // 等待写槽期间已进入关闭。
    ExitWrite(state);
    return make_error_code(TransportErrc::kClosed);
  }

  // 失败即关本物理连接,不自动重发:先进入关闭(排队写等待者以 kClosed 收敛、不
  // 移交写槽),再释放写槽。
  auto fail = [&state](std::error_code mapped) -> Status {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    BeginClose(state);
    ExitWrite(state);
    return Status{mapped};
  };

  // 把整帧全部交给 Qt 用户态发送缓冲、并等待其刷入操作系统发送缓冲:只有整帧字节
  // 全部离开框架用户态缓冲(bytesToWrite()==0)才报告成功(RT_TRANSPORT_008)。
  // 处理短写:socket->write() 返回 0≤n<total 时循环写剩余字节。背压经 await 传导。
  const char* data = reinterpret_cast<const char*>(unit.bytes.data());
  const qint64 total = static_cast<qint64>(unit.bytes.size());
  qint64 offset = 0;
  for (;;) {
    if (!socket) {
      return fail(make_error_code(TransportErrc::kConnection));
    }
    const qint64 remaining = total - offset;
    if (remaining <= 0 && socket->bytesToWrite() <= 0) {
      break;  // 整帧字节已全部进入操作系统发送缓冲 → 发送完成。
    }
    // waiter 先于本轮 write 创建,避免快速的 bytesWritten 被漏掉(见 AsyncTask
    // socket_pingpong 示例);已刷空时循环顶部的守卫保证不会空等。
    auto flushed = Coro::coro(socket.data()).waitForBytesWritten();
    if (remaining > 0) {
      const qint64 n = socket->write(data + offset, remaining);
      if (n < 0) {
        return fail(make_error_code(TransportErrc::kIo));
      }
      offset += n;
    }
    Coro::Result<void, std::error_code> drained = Coro::await(flushed);
    if (!drained) {
      // 刷完途中连接断裂 = 流式部分写失败(RT_TRANSPORT_004.4)。
      return fail(MapSocketError(drained.error()));
    }
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_send = Clock::now();
  }
  ExitWrite(state);
  return Status{};
}

Status TcpTransport::RequestClose() {
  BeginClose(state_);
  return Status{};
}

Status TcpTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

std::size_t TcpTransport::SendWaiterDepth() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->send_waiters;
}

std::optional<TcpTransport::Clock::time_point> TcpTransport::LastSendTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_send;
}

std::optional<TcpTransport::Clock::time_point> TcpTransport::LastReceiveTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_recv;
}

std::error_code TcpTransport::LastError() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_error;
}

}  // namespace transport
