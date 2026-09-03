#include "transport/io/tcp/TcpTransport.hpp"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QByteArray>
#include <QNetworkProxy>
#include <QString>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"
#include "transport/core/Error.hpp"

// TcpTransport.cpp — 见 .hpp。两条 fiber:管理泵(管连接 + 收字节)与写泵(发字节)。
// 单线程 fiber 协作,成员不加锁(见 .hpp 的"单线程,不加锁")。

namespace transport {
namespace {

// 把 Qt socket 错误映射到传输错误类别:连接与链路断裂归 Connection,其余读写故障归 Io。
// **一律不自终**(ADR-0007 D2 / ADR-0011 D14)——这里产出的只是 `LastError()` 的诊断
// 事实,包括"致命 socket I/O 错误",泵照样无限重连。
std::error_code MapSocketError(std::error_code error) {
  if (error.category() == Coro::detail::socket_error_category()) {
    switch (static_cast<QAbstractSocket::SocketError>(error.value())) {
      case QAbstractSocket::ConnectionRefusedError:
      case QAbstractSocket::RemoteHostClosedError:
      case QAbstractSocket::HostNotFoundError:
      case QAbstractSocket::NetworkError:
        return make_error_code(TransportErrc::kConnection);
      case QAbstractSocket::SocketTimeoutError:
        return make_error_code(TransportErrc::kTimeout);
      default:
        return make_error_code(TransportErrc::kIo);
    }
  }
  return make_error_code(TransportErrc::kIo);
}

// 归因:只落 `LastError()`,不改控制流。三条成因里**我方 Close 不记**——它不是故障
// (`Awaitable::close()` 无错误码时以 `no_message` 收敛,故此处恰好落在两支之外)。
void Attribute(std::error_code cause, std::error_code& sink) {
  if (cause == std::make_error_code(std::errc::timed_out)) {
    sink = make_error_code(TransportErrc::kTimeout);
  } else if (cause.category() == Coro::detail::socket_error_category()) {
    sink = MapSocketError(cause);
  }
}

}  // namespace

TcpTransport::TcpTransport(TcpConfig config)
    : config_(std::move(config)),
      // 固定对端,构造时算出一次(D8):读侧每个切片的 peer 都是它。
      peer_(Endpoint::Net(config_.host, config_.port)) {}

TcpTransport::~TcpTransport() {
  (void)Close();
  WaitClosed();  // join 两条泵:返回即它们都不再触碰本对象,可安全销毁 socket。
  if (socket_) {
    socket_->deleteLater();
  }
}

// 配置校验(D14):**在 `Start()` 里一次性做**,这是 SRS「不可重试失败」清单中"无效配置"
// 一项的落点——非法配置根本不进入重连循环。
Coro::Result<void> TcpTransport::ValidateConfig() const {
  if (config_.host.empty() || config_.port == 0) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // **须为正,没有“0 = 禁用”这一档**:合一之后它同时是等连上的时限与重连退避间隔,零值
  // 直接退化为紧循环(端口未监听时内核立即回 RST,connect 微秒级失败,烧 CPU 且向对端
  // 刷 SYN)。
  if (config_.silence_timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Coro::Result<void>{};
}

Coro::Result<void> TcpTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};
  }
  if (lifecycle_ != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidateConfig(); !valid) {
    // **停在 `Created`**:未建 socket、未起泵,允许改配后重试(RT_LIFECYCLE_007)。
    last_error_ = valid.error();
    return valid.error();
  }
  // socket 在本 fiber(节点执行域)内创建,守 Qt 对象亲和纪律。
  socket_ = new QTcpSocket();
  socket_->setProxy(QNetworkProxy::NoProxy);  // #123:不继承环境级代理策略。
  lifecycle_ = LifecycleState::kRunning;

  // **不就地 connect**:TCP 的 connect 是异步的,就地等会把 `Start()` 变成一个最长一个
  // silence_timeout 的阻塞调用。故 `Start()` 返回时 `CurrentLinkState()` 通常是
  // kEstablishing,“首连未成不算启动失败”(ADR-0007 D2)——调用方可以 Start() 后立即
  // 发送,链路不可用时报文在写队列里等(RT_TCP_RECONNECT_003)。
  write_pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunWritePump(); }));
  pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunSocketPump(); }));
  return Coro::Result<void>{};
}

// 泵 fiber(ADR-0011 D2,与 `UdpTransport::RunSocketPump()` 同构):
//
//   while (!closing) {
//     connect_waiter = connectToHost;              ← 句柄存成员,供 Close 打断 (D15)
//     if (closing) break;                          ← 【建完即复查】(D15 补正)
//     if (await_for(connect_waiter, timeout)) {
//       socket_ready 先清后发;
//       read_stream = readAll();                   ← 同样存成员 (D15)
//       while (!closing) { r = await_for(read_stream, timeout);  ← 同上,判据即复查
//                          if (r) push else break }
//     } else { 归因; await_for(close_signal, timeout) }   ← 退避
//     abort()                                      ← 每轮末尾无条件清理,不是打断手段
//   }
//
// **重连不是独立机制**:它就是本 while 转第二圈;首连与重连走同一段代码,不作区分。
// 三处的 timeout 是**同一个量**(D5):等连上 / 多久没数据算链路坏 / 多久重试一次。
//
// ★ **两处「建完即复查」是不变式,不是防御性冗余**(D15 补正,#200)。循环顶部的判据与
//   等待器的创建之间**不是原子的**:`Close()` 可能恰在其间跑完——它置了 kClosing、也
//   `close()` 了两个句柄,但那一刻新句柄还是 null,**它关不到未来才出现的句柄**。此后
//   泵接着建出一个**没有任何人会唤醒**的等待器,挂满一个 `silence_timeout`。
//   **`corosocket::readAll()` 的出生守卫救不了这一格**:它只在 socket 处于
//   `UnconnectedState` 时当场关流,而此处 socket 刚刚**连上**。
void TcpTransport::RunSocketPump() {
  const std::chrono::milliseconds timeout = config_.silence_timeout;

  while (lifecycle_ < LifecycleState::kClosing) {
    // 发起连接并等待。用的就是那一个量——**不设单独的 connect_timeout**(D5)。
    // 句柄**存成成员**,因为 `Close()` 要靠 `close()` 它来打断:实测 `abort()` 在连接
    // 窗口内唤不醒任何等待(D15)。
    connect_waiter_ = Coro::coro(socket_).connectToHost(
        QString::fromStdString(config_.host), config_.port);
    // 【建完即复查】(D15 补正):已 kClosing 说明 `Close()` 在本行之前跑完、关的是一个
    // 当时还是 null 的 `connect_waiter_`。**就地终结,不进 await**;轮末的两个清理动作
    // 在此原样补做,故 socket 仍回到确定状态。
    if (lifecycle_ >= LifecycleState::kClosing) {
      connect_waiter_.reset();
      socket_->abort();
      break;
    }
    auto connected = Coro::await_for(connect_waiter_, timeout);
    if (connected) {
      // 通告写泵(#180 的消费者;本轮无人取)。**先清后发**:每轮外层都是一次真实的
      // down→up 跃迁,而写泵停在"等数据"时没人来取,不清就会一直堆积。信号因此恒定
      // 只有 0 或 1 个 token。
      socket_ready_->channel()->discard_pending();
      socket_ready_->resolve();

      // 每轮重建读流(旧流已随上一轮 abort 死掉);建流时会 drain 订阅前已到的字节。
      // 同样**存成成员**供 Close 打断(D15)。
      read_stream_ = Coro::coro(socket_).readAll();
      // 【建完即复查】(D15 补正,#200):**本循环的判据就是那次复查**——它在第一次
      // `await_for` 之前求值,故“`Close()` 已跑完、关的是一个当时还是 null 的
      // `read_stream_`”这一格在此当场终结,**不进 await**。
      while (lifecycle_ < LifecycleState::kClosing) {
        auto chunk = Coro::await_for(read_stream_, timeout);
        if (!chunk) {
          // 三条成因**不作区分**,一律 break 回外层重连:
          //   ① 对端断开 —— **主判据**(D4),经流的自然终止到达(`readAll()` 在
          //      disconnected 与 socket error 上都先 `drain()` 再关流,**尾字节不丢**,
          //      故**不需要**另订阅 `waitForDisconnected()`);
          //   ② 静默超时 —— **辅助判据**(D4),只在半开连接(FIN 未达)时才轮到它;
          //   ③ 我方 Close —— 由 while 判据接住,不记归因。
          Attribute(chunk.error(), last_error_);
          break;
        }
        // chunk 是**任意字节切片**,不是一个完整帧(RT_TRANSPORT_003);组帧归 ICodec。
        // `readAll()` 只推送非空切片,故此处无须判空。
        const QByteArray& bytes = chunk.value();
        const auto* first =
            reinterpret_cast<const std::uint8_t*>(bytes.constData());
        Datagram out{{first, first + bytes.size()}, peer_};  // peer 固定对端(D8)。
        if (read_queue_->channel()->push(std::move(out)) !=
            boost::fibers::channel_op_status::success) {
          // read_queue 已关闭 → 停止投递,回外层判生命周期。两种成因:我方 Close,
          // 或**某个订阅者调了 close()**——后者是整流传播的(AsyncTask 417790c 起),
          // 会连本队列一并关掉,系有意为之。
          break;
        }
      }
      read_stream_.reset();
    } else {
      // 连不上:归因后退避。**必须用独立的延时原语**——拿"在未连接的 socket 上建读流"
      // 当退避会被当场关闭、退化为紧转(`UdpTransport` 的注释记着同一个坑)。
      // close_signal_ 被 Close 关闭时立即返回,故退避可提前打断。
      Attribute(connected.error(), last_error_);
      Coro::await_for(close_signal_, timeout);
    }
    connect_waiter_.reset();
    // 每轮末尾**无条件 abort()**(D3):socket 回 UnconnectedState,状态、缓冲与挂起的
    // 信号一并清除,下一轮从这一个确定状态重建——不必区分上轮怎么结束的,也**不需要
    // 新建 socket 对象**。注意它是**清理动作,不是打断手段**(D15)。
    socket_->abort();
  }

  // 收尾:**先 join 写泵**(确保它不再碰 socket),再关读队列、落 Closed。**没有完成量**
  // ——外部的 `WaitClosed()` 直接 join 本 fiber,那才是"可安全释放"的充分条件。
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因,残留一并丢弃。
  if (write_pump_) {
    (void)write_pump_->get();
  }
  CloseQueue(read_queue_, make_error_code(TransportErrc::kClosed));
  lifecycle_ = LifecycleState::kClosed;
}

// 写泵 fiber(ADR-0007 D3):两个阻塞点,**串行**(AsyncTask 无 select,也不需要多路等待)。
// 单消费者天然保证写入串行化(RT_TRANSPORT_004:两帧字节不交错)。
//
// 不变式:**取到 socket 到写出之间没有挂起点**——`write()` 交给 Qt 内部写缓冲即返回,而
// 我们**不等刷出**(D13),故写泵 fiber 不可能在“判 Connected”与“write”之间被调度走、
// 让管理泵把 socket abort 掉。
void TcpTransport::RunWritePump() {
  for (;;) {
    // ── 阻塞点①:等数据 ──(Close 关 write_queue 唤醒)
    auto item = Coro::await(write_queue_);
    if (!item) {
      return;  // 队列被关闭 → 退出(残留报文随之丢弃)。
    }
    const Datagram& unit = item.value();
    // `unit.peer` **被忽略**(D8):TCP 点对点,一律发往 config 里的固定对端。**不判
    // kInvalidArgument**——那会让恒发 Endpoint::Default()(或填了别的目的地)的“传输无关
    // 调用方”在 TCP 上跑不起来。

    // ── 阻塞点②:等连接就绪 ──(Close 关 socket_ready 唤醒)
    // 链路不可用时就停在这里,报文留在队列里等,**不丢弃、不回传错误**
    // (RT_TCP_RECONNECT_003);恢复后管理泵 resolve() 叫醒本泵,按序发出积压。
    bool connected = false;
    while (lifecycle_ < LifecycleState::kClosing) {
      if (socket_->state() == QAbstractSocket::ConnectedState) {
        connected = true;
        break;
      }
      socket_ready_->channel()->discard_pending();  // 清陈旧标记,再等下次就绪通告。
      if (!Coro::await(socket_ready_)) {
        break;  // 信号被 Close 关闭 → 退出。
      }
    }
    if (!connected) {
      return;
    }

    // ── 写出 ──【同步,无挂起点】(D13)
    // `setWriteBufferSize` 全仓未设(Qt 默认 0 = 无上限),故 write() 接受全部数据后立即
    // 返回。**不等 bytesWritten、不等 bytesToWrite() == 0**:写本就是 fire-and-forget
    // (ADR-0007 D3),等刷出不改变该语义、只让写泵多挂起一次。
    const auto size = static_cast<qint64>(unit.bytes.size());
    const qint64 n =
        socket_->write(reinterpret_cast<const char*>(unit.bytes.data()), size);
    if (n < 0) {
      // 写失败 → **放弃本条**,只落诊断事实(不自终、不回传调用方)。
      last_error_ =
          MapSocketError(Coro::detail::socket_error_code(socket_->error()));
    } else if (n != size) {
      // 短写视为链路异常:**放弃残余,不循环重试**——既然不等刷出,短写就没有可等的
      // 东西。残余丢失落在 D7 的既定范围内:写了半条即半条,由对端重同步。
      last_error_ = make_error_code(TransportErrc::kIo);
    }
    // 回阻塞点①取下一条。
  }
}

// 交出 read_queue 句柄(ADR-0007 D4):未 Start 时给一个以 kInvalidState 关闭的句柄;
// 已关闭时 read_queue 已被泵以 kClosed 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> TcpTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    return ClosedQueue<Datagram>(make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

// 送入写队列即返(ADR-0007 D3):**不等待实际发出**,返回成功仅表示"已入队"。写出的一切
// 结果(socket 写失败、短写)都在写泵里,只落 LastError(),不回传。
//
// **链路不可用时同样返回成功**(RT_TCP_RECONNECT_003):报文在队列里等,写泵停在阻塞点②。
// 但队列有界 1024 且满时**静默丢最旧**(D6),故那句“不拒绝、不丢弃”的“不丢弃”只在未超界
// 时成立,此处**不作归因**。
Coro::Result<void> TcpTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kClosed);
  }
  // `datagram.peer` 不在此处解析,也不校验(D8):写泵一律忽略它。
  if (write_queue_->channel()->push(std::move(datagram)) !=
      boost::fibers::channel_op_status::success) {
    return make_error_code(TransportErrc::kClosed);  // 队列已关闭 = 传输终结。
  }
  return Coro::Result<void>{};
}

// 请求关闭(幂等):**五处打断缺一不可**——漏一处,`Close()` 落在对应窗口时就要挂满一个
// `silence_timeout`。只发信号,不等收敛(收尾由泵自己跑完)。
//
// | # | 阻塞点                                  | 打断者                    |
// |---|------------------------------------------|---------------------------|
// | ① | 泵:`await_for(close_signal_, timeout)` 退避 | `close_signal_->close()`  |
// | ② | 泵:`await_for(connect_waiter_, timeout)` 等连上 | `connect_waiter_->close()` |
// | ③ | 泵:`await_for(read_stream_, timeout)` 读等待 | `read_stream_->close()`   |
// | ④ | 写泵:`await(write_queue_)` 等数据         | `write_queue_->close()` + `discard_pending()` |
// | ⑤ | 写泵:`await(socket_ready_)` 等连接就绪    | `socket_ready_->close()`  |
//
// ②③ **不能用 `socket_->abort()` 代替**(D15):Qt 的 `abort()` 在连接中的 socket 上不发
// `errorOccurred`,而 corosocket 的 waitForSignal / readAll 都靠 socket error 或
// disconnected 终结,两处都唤不醒;持句柄 `close()` 才唤得醒。UDP 没有“连接中”这个
// 窗口,故其 `socket_->close()` 打断读流有效,**不可照搬**。
//
// ★ ②③ 只能关到**此刻已经存在**的句柄。“泵在本函数跑完之后才建出来的那一个”由泵侧的
//   **两处「建完即复查」**接住(D15 补正,#200),见 `RunSocketPump()`。
Coro::Result<void> TcpTransport::Close() {
  if (lifecycle_ >= LifecycleState::kClosing) {
    return Coro::Result<void>{};  // 幂等。
  }
  const std::error_code closed = make_error_code(TransportErrc::kClosed);
  if (lifecycle_ == LifecycleState::kCreated) {
    lifecycle_ = LifecycleState::kClosed;  // 从未 Start:无泵可停。
    CloseQueue(read_queue_, closed);
    return Coro::Result<void>{};
  }
  lifecycle_ = LifecycleState::kClosing;

  close_signal_->close(closed);  // ① 打断重连退避。
  if (connect_waiter_) {
    connect_waiter_->close(closed);  // ② 打断"等连上"。
  }
  if (read_stream_) {
    read_stream_->close(closed);  // ③ 打断读等待。
  }
  write_queue_->close(closed);                 // ④ 唤醒写泵阻塞点①(等数据)。
  write_queue_->channel()->discard_pending();  // 未发出的残留随之丢弃(不等刷出,D13)。
  socket_ready_->close(closed);                // ⑤ 唤醒写泵阻塞点②(等连接就绪)。
  return Coro::Result<void>{};
}

// join 管理泵 fiber(它内部已先 join 写泵)。返回即两条 fiber 都不再触碰本对象——这是
// "可安全释放"的充分条件,
// `Awaitable::close()` 给不了(它只保证等待者被唤醒)。`get()` 是一次性的(底层 boost
// future 取过即失效),故用 joined_ 闩保证幂等。
void TcpTransport::WaitClosed() {
  if (joined_ || !pump_) {
    return;  // 已 join 过,或从未 Start:无可汇合者。
  }
  joined_ = true;
  (void)pump_->get();
}

bool TcpTransport::IsRunning() const {
  return lifecycle_ == LifecycleState::kRunning;
}

std::error_code TcpTransport::LastError() const { return last_error_; }

// **无状态成员,当场算出**(D12)。**退避重连期间报 `kEstablishing`**——泵仍会重试,
// 链路是“正在建立”,不是“没了”。
LinkState TcpTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning || !socket_) {
    return LinkState::kDown;
  }
  switch (socket_->state()) {
    case QAbstractSocket::ConnectedState:
      return LinkState::kUp;
    case QAbstractSocket::ConnectingState:
    case QAbstractSocket::HostLookupState:
      return LinkState::kEstablishing;
    default:
      return LinkState::kEstablishing;  // 未连接但泵仍会重试。
  }
}

}  // namespace transport
