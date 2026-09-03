#include "transport/io/udp/UdpTransport.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QAbstractSocket>
#include <QByteArray>
#include <QHostAddress>
#include <QNetworkDatagram>
#include <QNetworkProxy>
#include <QUdpSocket>

#include "await/awaitable.hpp"
#include "await/coroudpsocket.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"
#include "transport/core/Error.hpp"

// UdpTransport.cpp — 见 .hpp。两条 fiber:泵(管 socket + 收报文)与写泵(发报文)。
// 单线程 fiber 协作,成员不加锁(见 .hpp 的"单线程,不加锁")。

namespace transport {
namespace {

// 读超时 / 重试间隔的兜底值:`silence_timeout` 非正时按此处理。它同时承载 bind 失败后的
// 退避,配 0 会让外层循环退化成不带间隔的紧转,故不接受 0(见 UdpConfig)。
constexpr std::chrono::milliseconds kDefaultTimeout{5000};

// 把 Qt socket 错误映射到传输错误类别:网络层断裂归 Connection,报文过大归
// InvalidArgument(调用方给了不可发送的报文),其余读写故障归 Io。
std::error_code MapSocketError(std::error_code error) {
  if (error.category() == Coro::detail::socket_error_category()) {
    switch (static_cast<QAbstractSocket::SocketError>(error.value())) {
      case QAbstractSocket::NetworkError:
      case QAbstractSocket::ConnectionRefusedError:
        return make_error_code(TransportErrc::kConnection);
      case QAbstractSocket::DatagramTooLargeError:
        return make_error_code(TransportErrc::kInvalidArgument);
      default:
        return make_error_code(TransportErrc::kIo);
    }
  }
  return make_error_code(TransportErrc::kIo);
}

QHostAddress ToAddress(const std::string& host) {
  return QHostAddress(QString::fromStdString(host));
}

}  // namespace

UdpTransport::UdpTransport(UdpConfig config) : config_(std::move(config)) {}

UdpTransport::~UdpTransport() {
  (void)Close();
  WaitClosed();  // join 两条泵:返回即它们都不再触碰本对象,可安全销毁 socket。
  if (socket_) {
    socket_->deleteLater();
  }
}

Coro::Result<void> UdpTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};
  }
  if (lifecycle_ != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  // socket 在本 fiber(节点执行域)内创建,守 Qt 对象亲和纪律。
  socket_ = new QUdpSocket();
  socket_->setProxy(QNetworkProxy::NoProxy);  // #123:不继承环境级代理策略。
  lifecycle_ = LifecycleState::kRunning;

  // 首次 bind 就地做一次(不等泵被调度):`LocalPort()` / `CurrentLinkState()` 因此在
  // Start() 返回后即可如实观测。**失败不算启动失败**(ADR-0007 D2)——泵会无限重试。
  (void)Bind();

  write_pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunWritePump(); }));
  pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunSocketPump(); }));
  return Coro::Result<void>{};
}

bool UdpTransport::Bind() {
  socket_->close();  // 幂等前置:把 socket 重置到可 bind 状态。

  // ShareAddress|ReuseAddressHint:组播/多消费者场景可共享绑定;单播 loopback 无碍。
  const QAbstractSocket::BindMode mode =
      config_.mode == UdpMode::kMulticast
          ? (QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint)
          : QUdpSocket::DefaultForPlatform;
  if (!socket_->bind(ToAddress(config_.local_addr), config_.local_port, mode)) {
    last_error_ =
        MapSocketError(Coro::detail::socket_error_code(socket_->error()));
    if (!last_error_) {
      last_error_ = make_error_code(TransportErrc::kIo);
    }
    return false;
  }
  if (config_.mode == UdpMode::kMulticast && !config_.multicast_group.empty()) {
    socket_->setSocketOption(QAbstractSocket::MulticastTtlOption,
                             static_cast<int>(config_.ttl));
    socket_->joinMulticastGroup(ToAddress(config_.multicast_group));  // close 会退组。
  }
    // 临时端口(local_port 配 0)每次重 bind 会拿到不同端口:对端若记着源端口会失联。
  local_port_ = socket_->localPort();
  return true;
}

// 泵 fiber(ADR-0007 D1):**读数据与超时判断同在本 fiber 内**。
//
//   while (!closing) {
//     bind
//     if (bound) { stream = receiveDatagram; for(;;) { r = await_for(stream, timeout);
//                                                      if (r) push else break } }
//     else       { await_for(close_signal, timeout) }
//     unbind                    ← 每轮末尾无条件解绑,下轮从干净状态重建
//   }
//
// 两处的 timeout 是**同一个量**:有链路时它是"多久没数据算坏",没链路时它是"多久试一次
// bind"。三条成因(静默超时 / 流终止 / 我方 Close)在内层不作区分,一律 break 回外层:
// 前两者由外层重新 bind 消化(无限重试、不自终,ADR-0007 D2),后者由 while 判据接住。
void UdpTransport::RunSocketPump() {
  const std::chrono::milliseconds timeout =
      config_.silence_timeout > std::chrono::milliseconds::zero()
          ? config_.silence_timeout
          : kDefaultTimeout;

  while (lifecycle_ < LifecycleState::kClosing) {
    // 已绑定则跳过——本条**只在首轮**为真(Start 已就地 bind 过一次,重 bind 会换掉它刚
    // 报出的临时端口)。第二轮起底部的解绑已把 socket 置为未绑定,本条恒为假。
    if (socket_->state() == QAbstractSocket::BoundState || Bind()) {
      // 通告 socket 就绪。**先清后发**:每轮外层都是一次真实的 down→up 跃迁,而写泵停在
      // "等数据"时没人来取,不清就会一直堆积。写泵若正停在"等就绪",队列本就是空的,清是
      // 空操作、随后的 resolve 照常叫醒它——信号因此恒定只有 0 或 1 个 token。
      socket_ready_->channel()->discard_pending();
      socket_ready_->resolve();

      // 每代重建读流(旧流已随上一轮解绑死掉);建流时会 drain 订阅前已到的报文,不丢首报。
      auto stream = Coro::coro(socket_).receiveDatagram();
      for (;;) {
        // 读超时**不关闭 stream**(await_for 只结束本次等待),但本轮已判链路坏、要重建
        // socket,故仍丢弃该句柄。
        auto received = Coro::await_for(stream, timeout);
        if (!received) {
          // 归因只落 LastError,不改控制流。我方 Close 所致的 no_message 不记——不是故障。
          if (received.error() == std::make_error_code(std::errc::timed_out)) {
            last_error_ = make_error_code(TransportErrc::kTimeout);  // 判链路已坏。
          } else if (received.error().category() ==
                     Coro::detail::socket_error_category()) {
            last_error_ = MapSocketError(received.error());  // 不自终,降为诊断事实。
          }
          break;
        }
        const QNetworkDatagram& dg = received.value();
        const QByteArray bytes = dg.data();
        const auto* first =
            reinterpret_cast<const std::uint8_t*>(bytes.constData());
        Datagram out{{first, first + bytes.size()},
                     Endpoint::Net(dg.senderAddress().toString().toStdString(),
                                   static_cast<std::uint16_t>(dg.senderPort()))};
        if (read_queue_->channel()->push(std::move(out)) !=
            boost::fibers::channel_op_status::success) {
          // read_queue 已关闭 → 停止投递,回外层判生命周期。两种成因:我方 Close,
          // 或**某个订阅者调了 close()**——后者是整流传播的(AsyncTask 417790c 起),
          // 会连本队列一并关掉。ProtocolNode::DoClose() 走的正是后一条,系有意为之。
          break;
        }
      }
    } else {
      // bind 失败的退避,间隔与读超时同值。**必须用独立的延时原语**:未 bind 的 socket 上
      // 建读流会被当场关闭(实测 await_for 0ms 返回 no_message),拿它当退避会紧转。
      // close_signal 被 Close 关闭时立即返回,故退避可提前打断。
      Coro::await_for(close_signal_, timeout);
    }
    // 每轮末尾无条件解绑:下一轮从"未绑定"这一个确定状态重建,不必区分上轮怎么结束的。
    // 对未绑定的 socket 调 close() 是空操作;`CurrentLinkState()` 据实时状态作答,故此刻
    // 起如实报 kDown。
    socket_->close();
  }

  // 收尾:先 join 写泵(确保它不再碰 socket),再关读队列、落 Closed。**没有完成量**——
  // 外部的 WaitClosed() 直接 join 本 fiber,那才是"可安全释放"的充分条件。
  if (write_pump_) {
    (void)write_pump_->get();
  }
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因,残留一并丢弃——句柄式读
  // 没有"逐次判生命周期"的位置,须显式丢弃才与"关闭即停止交付"等价。
  CloseQueue(read_queue_, make_error_code(TransportErrc::kClosed));
  lifecycle_ = LifecycleState::kClosed;
}

// 写泵 fiber(ADR-0007 D3):两个阻塞点,**串行**(AsyncTask 无 select,也不需要多路等待)。
// 单消费者天然保证写入串行化。
//
// 不变式:**取到 socket 到写出之间没有挂起点**——writeDatagram 同步非阻塞,故泵 fiber
// 不可能在中间被调度起来解绑 socket,由此不需要代际号校验。
//
// **该不变式对 TCP 与串口同样成立**(ADR-0011 D13 / ADR-0012 D8):两者的写泵同样
// 写完不等刷出,写出段无挂起点。**三个写泵在这一点上结构完全同构**。
void UdpTransport::RunWritePump() {
  for (;;) {
    // ── 阻塞点①:等数据 ──(Close 关 write_queue 唤醒)
    auto item = Coro::await(write_queue_);
    if (!item) {
      return;  // 队列被关闭 → 退出(残留报文随之丢弃)。
    }
    const Datagram& unit = item.value();
    // 解析目的地(出队即判,不必等 socket 就绪——发不出去的一条不该占着队头):
    //   kDefault → config 默认对端(让恒发 Default 的传输无关调用方跑在 UDP 上);
    //   kNet     → 按 ip:port(ADR-0003 D12);其余(kTopic)→ 本介质无此语义。
    // 解析不了就丢这一条并记 LastError:fire-and-forget,不回传调用方(ADR-0007 D3)。
    QHostAddress address;
    std::uint16_t port = 0;
    if (unit.peer.kind == Endpoint::Kind::kDefault) {
      const std::string& host = config_.mode == UdpMode::kMulticast
                                    ? config_.multicast_group
                                    : config_.remote_addr;
      address = ToAddress(host);
      port = config_.remote_port;
    } else if (unit.peer.kind == Endpoint::Kind::kNet) {
      address = ToAddress(unit.peer.host);
      port = unit.peer.port;
    }
    if (address.isNull() || port == 0) {
      last_error_ = make_error_code(TransportErrc::kInvalidArgument);
      continue;
    }

    for (;;) {
      // ── 阻塞点②:等 socket 就绪 ──(Close 关 socket_ready 唤醒)
      if (lifecycle_ >= LifecycleState::kClosing) {
        return;
      }
      if (socket_->state() == QAbstractSocket::BoundState) {
        const auto size = static_cast<qint64>(unit.bytes.size());
        const qint64 n = socket_->writeDatagram(
            reinterpret_cast<const char*>(unit.bytes.data()), size, address, port);
        if (n < 0) {
          last_error_ =
              MapSocketError(Coro::detail::socket_error_code(socket_->error()));
        } else if (n != size) {
          // 未整发(过大被截断):报文语义下视为无效参数(RT_IF_UDP 一次一完整报文)。
          last_error_ = make_error_code(TransportErrc::kInvalidArgument);
        }
        break;  // 本条处理完,回阻塞点①。
      }
      socket_ready_->channel()->discard_pending();  // 清陈旧标记,再等下次就绪通告。
      Coro::await(socket_ready_);
    }
  }
}

// 交出 read_queue 句柄(ADR-0007 D4):不返回数据,超时/取消/扇出由调用方在句柄上自理。
// 未 Start 时给一个以 kInvalidState 关闭的句柄;已关闭时 read_queue 已被泵以 kClosed
// 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> UdpTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    return ClosedQueue<Datagram>(make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

// 送入写队列即返(ADR-0007 D3):**不等待实际发出**,返回成功仅表示"已入队"。写出的一切
// 结果(目的地能不能解析、socket 写成没写成)都在写泵里,只落 LastError(),不回传。
Coro::Result<void> UdpTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kClosed);
  }
  if (write_queue_->channel()->push(std::move(datagram)) !=
      boost::fibers::channel_op_status::success) {
    return make_error_code(TransportErrc::kClosed);  // 队列已关闭 = 传输终结。
  }
  return Coro::Result<void>{};
}

// 请求关闭(幂等):**四处打断缺一不可**——泵可能停在退避或读等待,写泵可能停在两个阻塞点
// 之一,漏一处即一次收敛挂死。只发信号,不等收敛(收尾由泵自己跑完)。
Coro::Result<void> UdpTransport::Close() {
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

  close_signal_->close(closed);  // ① 打断"未 bind 时的退避"。
  socket_->close();              // ② 打断活跃读流(实测有效)。
  write_queue_->close(closed);   // ③ 唤醒写泵阻塞点①。
  write_queue_->channel()->discard_pending();  // 未发出的残留随之丢弃。
  socket_ready_->close(closed);  // ④ 唤醒写泵阻塞点②。
  return Coro::Result<void>{};
}

// join 泵 fiber(它内部已先 join 写泵)。返回即两条 fiber 都不再触碰本对象——这是"可安全
// 释放"的充分条件,`Awaitable::close()` 给不了(它只保证等待者被唤醒)。
// `get()` 是一次性的(底层 boost future 取过即失效),故用 joined_ 闩保证幂等。
void UdpTransport::WaitClosed() {
  if (joined_ || !pump_) {
    return;  // 已 join 过,或从未 Start:无可汇合者。
  }
  joined_ = true;
  (void)pump_->get();
}

std::uint16_t UdpTransport::LocalPort() const { return local_port_; }

std::error_code UdpTransport::LastError() const { return last_error_; }

// UDP 的"链路可用"即 socket 此刻已绑定。bind 重试期间 Running 但未绑定 → kDown;UDP 无
// 连接,故永不出现 kEstablishing(退避是连接管理策略,不经本查询暴露)。
LinkState UdpTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning || !socket_) {
    return LinkState::kDown;
  }
  return socket_->state() == QAbstractSocket::BoundState ? LinkState::kUp
                                                         : LinkState::kDown;
}

}  // namespace transport
