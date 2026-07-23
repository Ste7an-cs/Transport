#include "transport/coro/TcpTransport.hpp"

#include <mutex>
#include <utility>

#include <QAbstractSocket>
#include <QByteArray>
#include <QPointer>

#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "transport/coro/Error.hpp"
#include "transport/coro/SharedCompletion.hpp"

namespace transport::coro {
namespace {

// 把 Qt socket 错误映射到传输错误类别:对端主动关闭 / 网络层断裂归 Connection,
// 其余读写故障归 Io(RT_TRANSPORT_004.4 允许 Io 或 Connection)。
std::error_code MapFlushError(std::error_code error) {
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
  LifecycleState lifecycle{LifecycleState::kCreated};
  bool active_read{false};
  bool active_write{false};
  std::size_t send_waiters{0};
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
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosed) {
      return;
    }
    state->lifecycle = LifecycleState::kClosing;
    socket = state->socket;
    if (!state->active_read && !state->active_write) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (socket) {
    socket->abort();  // 立即唤醒在途 readAll/waitForBytesWritten(以错误收敛)
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

// 收尾一次写:释放有效写与发送等待者计数;若正在关闭且无其他在途操作则落 Closed。
void FinishWrite(const std::shared_ptr<TcpTransport::State>& state) {
  bool complete = false;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->active_write = false;
    if (state->send_waiters > 0) {
      state->send_waiters -= 1;
    }
    if (state->lifecycle == LifecycleState::kClosing && !state->active_read) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (complete) {
    state->closed.Complete(Status{});
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
    return Status{};
  }
  if (state_->lifecycle == LifecycleState::kRunning) {
    return Status{};
  }
  return make_error_code(TransportErrc::kInvalidState);
}

Result<Datagram> TcpTransport::Read(OperationOptions options) {
  const auto state = state_;
  QPointer<QAbstractSocket> socket;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    if (state->active_read) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    state->active_read = true;
    socket = state->socket;
  }

  auto reader = Coro::coro(socket.data()).readAll();
  auto registration = options.cancellation.Register(
      [reader] { reader->close(make_error_code(TransportErrc::kCancelled)); });
  Coro::Result<QByteArray, std::error_code> notification =
      options.deadline
          ? Coro::await_for(reader, *options.deadline - Clock::now())
          : Coro::await(reader);
  registration.Reset();

  Result<Datagram> result{make_error_code(TransportErrc::kInternal)};
  if (notification) {
    const QByteArray& bytes = notification.value();
    Datagram datagram;
    datagram.bytes.assign(
        reinterpret_cast<const std::uint8_t*>(bytes.constData()),
        reinterpret_cast<const std::uint8_t*>(bytes.constData()) + bytes.size());
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_recv = Clock::now();
    }
    result = Result<Datagram>{std::move(datagram)};
  } else if (notification.error() == std::make_error_code(std::errc::timed_out)) {
    result = make_error_code(TransportErrc::kTimeout);
  } else if (notification.error() ==
             make_error_code(TransportErrc::kCancelled)) {
    result = notification.error();
  } else {
    std::error_code mapped = MapFlushError(notification.error());
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    result = mapped;
  }
  FinishRead(state);
  return result;
}

Status TcpTransport::Write(SendUnit unit) {
  const auto state = state_;
  QPointer<QAbstractSocket> socket;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    if (state->active_write) {
      // 单写不交错(RT_TRANSPORT_004):并发违规写立即被拒,绝不与在写帧交错。
      return make_error_code(TransportErrc::kInvalidState);
    }
    state->active_write = true;
    state->send_waiters += 1;
    socket = state->socket;
  }

  // 裸 write 把整帧交给 Qt 用户态发送缓冲(RT_TRANSPORT_008 的"离开框架缓冲"以
  // 刷入操作系统缓冲为准,下面的刷完循环负责等待其排空)。
  const qint64 total = static_cast<qint64>(unit.bytes.size());
  const qint64 written =
      socket ? socket->write(
                   reinterpret_cast<const char*>(unit.bytes.data()), total)
             : -1;
  if (written < 0) {
    std::error_code io = make_error_code(TransportErrc::kIo);
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = io;
    }
    FinishWrite(state);
    BeginClose(state);  // 写失败即关本物理连接,不自动重发。
    return io;
  }

  // 刷完循环:守卫 bytesToWrite()>0 为强制——已刷空时 waitForBytesWritten 不会
  // 再触发信号,若无守卫会挂死。背压在此经 await 传导回发起方。
  for (;;) {
    qint64 pending = socket ? socket->bytesToWrite() : 0;
    if (pending <= 0) {
      break;  // 整帧字节已进入操作系统发送缓冲 → 发送完成。
    }
    auto flushed = Coro::coro(socket.data()).waitForBytesWritten();
    Coro::Result<void, std::error_code> drained = Coro::await(flushed);
    if (!drained) {
      // 刷完途中连接断裂 = 流式部分写失败(RT_TRANSPORT_004.4)。
      std::error_code mapped = MapFlushError(drained.error());
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_error = mapped;
      }
      FinishWrite(state);
      BeginClose(state);  // 关本物理连接,不在此连接继续发送、不重发残缺帧。
      return mapped;
    }
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->last_send = Clock::now();
  }
  FinishWrite(state);
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

}  // namespace transport::coro
