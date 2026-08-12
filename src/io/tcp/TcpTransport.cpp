#include "transport/io/tcp/TcpTransport.hpp"

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QPointer>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"  // Coro::makeTask —— 数据泵 fiber。
#include "transport/core/Error.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace transport {
namespace {

// 把 Qt socket 错误映射到传输错误类别:对端主动关闭 / 网络层断裂归 Connection,
// 其余读写故障归 Io(RT_TRANSPORT_004.4 允许 Io 或 Connection)。
// 用途:**写路径**的失败原因(ADR-0004 D1:kConnection 此后仅存于写路径)与
// LastError() 诊断;读路径的终止一律以 kClosed 呈现,不用本映射作返回值。
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
  // 对外 read_queue(ADR-0007 D1/D4):数据泵是唯一生产者,`Read()` 只交出本句柄。
  // 构造即建、整个生命周期只此一条。容量策略未定(TBD-009):沿用 AsyncTask 默认;
  // **字节流介质丢中段即帧错乱**,该默认是已登记的活跃隐患(#152 / ADR-0007 D6),
  // 本轮不处置——泵一取到切片就立刻转投,常态队深约 1。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  LifecycleState lifecycle{LifecycleState::kCreated};
  // 对端地址,Start 时从已连接 socket 缓存(对已连接 TCP 恒定):投入 read_queue 的
  // Datagram.source 恒填为对端 Endpoint::Net(TCP from 恒为对端,见 CONTEXT.md)。
  std::string peer_host;
  std::uint16_t peer_port{0};
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

// 关闭一次:进入 Closing、撕 socket、关读流令数据泵退出、以 kClosed 关 read_queue 唤醒
// 全部读者与排队写等待者;无在途写时直接落到 Closed 并完成 closed(有在途写则由
// ExitWrite 完成)。读侧不再有"在途读"这一状态(单读守卫随 ADR-0007 D4 删除),故不等读者。
void BeginClose(const std::shared_ptr<TcpTransport::State>& state) {
  QPointer<QAbstractSocket> socket;
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream;
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
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
    read_queue = state->read_queue;
    queued_writes.swap(state->write_queue);  // 唤醒排队写等待者以 kClosed 收敛。
    if (!state->active_write) {
      state->lifecycle = LifecycleState::kClosed;
      complete = true;
    }
  }
  if (socket) {
    socket->abort();  // 立即唤醒在途读写等待者(以错误收敛)。
  }
  if (read_stream) {
    read_stream->close(make_error_code(TransportErrc::kClosed));  // 令数据泵退出。
  }
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因,调用方 await 得到它;
  // 我方关闭同时丢弃残留(改造前关闭后发起的读一律得 kClosed、取不到残留)。
  CloseDatagramQueue(read_queue, make_error_code(TransportErrc::kClosed));
  for (const auto& gate : queued_writes) {
    gate->close(make_error_code(TransportErrc::kClosed));
  }
  if (complete) {
    state->closed.Complete(Status{});
  }
}

// 数据泵(ADR-0007 D1 内层循环):反复 await readAll 流,把每片字节转成 Datagram 投入
// read_queue,直至流终止。本类不重连(连接终结即传输终结),故流终止即以 kClosed 关
// read_queue——与改造前"Read 一律以 kClosed 收敛"等价;底层成因仍降为 LastError() 诊断。
// **不改生命周期**:对端断开时本类保持 Running(链路可用性问 socket),与改造前一致。
void RunReadPump(const std::shared_ptr<TcpTransport::State>& state,
                 const std::shared_ptr<Coro::Awaitable<QByteArray>>& stream,
                 const std::shared_ptr<Coro::Awaitable<Datagram>>& read_queue) {
  const auto channel = read_queue->channel();
  for (;;) {
    Coro::Result<QByteArray, std::error_code> chunk = Coro::await(stream);
    if (chunk) {
      const QByteArray& bytes = chunk.value();
      Datagram datagram;
      datagram.bytes.assign(
          reinterpret_cast<const std::uint8_t*>(bytes.constData()),
          reinterpret_cast<const std::uint8_t*>(bytes.constData()) +
              bytes.size());
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_recv = OperationOptions::Clock::now();
        // TCP from 恒为对端:source 填 Start 时缓存的对端地址(CONTEXT.md 读-分发循环)。
        datagram.source = Endpoint::Net(state->peer_host, state->peer_port);
      }
      if (channel->push(std::move(datagram)) !=
          boost::fibers::channel_op_status::success) {
        return;  // read_queue 已关闭(我方 Close)→ 停止投递。
      }
      continue;
    }
    if (chunk.error().category() == Coro::detail::socket_error_category()) {
      // 已连接 socket 上的底层致命错误(对端 reset、网络断裂):本类不重连,连接终结
      // 即传输终结,底层成因降为诊断事实留在 LastError()。
      const std::error_code mapped = MapSocketError(chunk.error());
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_error = mapped;
    }
    // channel 无错误关闭:对端正常关闭,或我方关闭。本类不重连,二者对调用方同一
    // 含义——传输终结、停止读取(ADR-0004 D1 终止语义单一化,表达经 ADR-0007 D4 改写)。
    read_queue->close(make_error_code(TransportErrc::kClosed));
    return;
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
      state->lifecycle = LifecycleState::kClosed;  // 读侧已无在途操作可等。
      complete = true;
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
  const auto state = state_;
  std::shared_ptr<Coro::Awaitable<QByteArray>> stream;
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    if (state->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (!state->socket) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    state->lifecycle = LifecycleState::kRunning;
    // 缓存对端地址(已连接 TCP 恒定):投入 read_queue 的 Datagram.source 恒填为对端。
    state->peer_host = state->socket->peerAddress().toString().toStdString();
    state->peer_port = state->socket->peerPort();
    // 建立唯一 readAll 流(持有一条、反复 await);初始 drain 会收下订阅前已到达的
    // 字节,故不丢首片。
    state->read_stream = Coro::coro(state->socket.data()).readAll();
    stream = state->read_stream;
    read_queue = state->read_queue;
  }
  // 起数据泵(ADR-0007 D1):在本执行域 fiber 内反复取字节片投 read_queue。句柄不留存:
  // 泵只触碰以 shared_ptr 持有的 State / 流 / 队列,故本类析构后仍安全收敛。
  Coro::makeTask([state, stream, read_queue] {
    RunReadPump(state, stream, read_queue);
  });
  return Status{};
}

// 交出 read_queue 句柄(ADR-0007 D4):不返回数据,deadline/取消/扇出由调用方在句柄上
// 自理。未 Start 时给一个以 kInvalidState 关闭的句柄;关闭中/已关闭或对端断开时,
// read_queue 已被关闭并携带 kClosed,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> TcpTransport::Read() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    return ClosedDatagramQueue(make_error_code(TransportErrc::kInvalidState));
  }
  return state_->read_queue;
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

// 链路可用性以 socket 的当前连接态为准而非以生命周期推断:对端断开时本类不改
// lifecycle(读侧只以 kClosed 收敛,不重连),故 Running 期仍须问 socket 才能
// 如实报告"连接已不存续"。
LinkState TcpTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle != LifecycleState::kRunning || !state_->socket) {
    return LinkState::kDown;
  }
  return state_->socket->state() == QAbstractSocket::ConnectedState
             ? LinkState::kUp
             : LinkState::kDown;
}

}  // namespace transport
