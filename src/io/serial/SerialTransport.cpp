#include "transport/io/serial/SerialTransport.hpp"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include <QByteArray>
#include <QIODevice>
#include <QSerialPort>
#include <QString>

#include "await/awaitable.hpp"
#include "await/coroiodevice.hpp"
#include "task/fibertask.h"
#include "transport/core/Error.hpp"

// SerialTransport.cpp — 见 .hpp。两条 fiber:管理泵(管设备 + 收字节)与写泵(发字节)。
// 单线程 fiber 协作,成员不加锁(见 .hpp 的"单线程,不加锁")。
//
// 与 `UdpTransport` / `TcpTransport` 逐段同构;三处**串口独有**的差异都在下面就地标出:
//   ① 读泵显式跳过空切片(D5)——`coroiodevice` 的 push 不判空;
//   ② 判活只认静默超时(D4)——串口没有断开事件;
//   ③ 设备错误一律只落 LastError(D11 + D4)——`errorOccurred` 是噪声而非事件。

namespace transport {
namespace {

// 把 QSerialPort 错误映射到传输错误类别:设备不在/无权限/被移除归 Connection,读超时归
// Timeout,其余(含线路噪声 Parity/Framing/Break 与读写故障)归 Io。
//
// **一律不自终、也不触发重建**(D1 + D4 + D11):这里产出的只是 `LastError()` 的诊断
// 事实。设备重建**只由静默超时驱动**——串口在流层面拿不到"设备已死"的信号,拿噪声当
// 断链判据会让一段线路噪声把链路整个重建,反而丢掉更多在途字节。
std::error_code MapSerialError(QSerialPort::SerialPortError error) {
  switch (error) {
    case QSerialPort::NoError:
      return {};
    case QSerialPort::DeviceNotFoundError:
    case QSerialPort::PermissionError:
    case QSerialPort::OpenError:
    case QSerialPort::NotOpenError:
    case QSerialPort::ResourceError:
      return make_error_code(TransportErrc::kConnection);
    case QSerialPort::TimeoutError:
      return make_error_code(TransportErrc::kTimeout);
    default:
      // ParityError / FramingError / BreakConditionError / ReadError / WriteError /
      // UnsupportedOperationError / UnknownError。
      return make_error_code(TransportErrc::kIo);
  }
}

// 归因:只落 `LastError()`,不改控制流。读流终止的成因里**我方 Close 不记**——它不是
// 故障(`Awaitable::close()` 无错误码时以 `no_message` 收敛,恰好落在本判据之外)。
//
// **串口只有一条归因支**(与 TCP 的差异):TCP 还要把 socket 错误类别翻译进来,而
// `coroiodevice` 的读流根本不订阅错误信号(D4),流终止只可能来自 `aboutToClose` 或
// 我方 `close()`,两者都以 `no_message` 收敛。
void Attribute(std::error_code cause, std::error_code& sink) {
  if (cause == std::make_error_code(std::errc::timed_out)) {
    sink = make_error_code(TransportErrc::kTimeout);  // 静默超时 = 判链路已坏(D4)。
  }
}

// 把 SerialConfig 的数据位/停止位/校验映射为 QSerialPort 枚举(值域已由 ValidateConfig
// 保证,此处的 default 分支只是防御)。
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

bool IsValidDataBits(std::uint8_t bits) { return bits >= 5 && bits <= 8; }
bool IsValidStopBits(std::uint8_t bits) { return bits == 1 || bits == 2; }
bool IsValidParity(char parity) {
  switch (parity) {
    case 'N': case 'n': case 'E': case 'e': case 'O': case 'o': return true;
    default: return false;
  }
}

}  // namespace

SerialTransport::SerialTransport(SerialConfig config)
    : config_(std::move(config)),
      // 固定设备端点,构造时算出一次(D9):读侧每个切片的 peer 都是它,写侧忽略调用方
      // 填的 peer 一律写往它。`Endpoint::Default()` 的语义即“本传输配置的默认对端”,
      // 对单设备介质就是 `config.device` 那一个设备。
      peer_(Endpoint::Default()) {}

SerialTransport::~SerialTransport() {
  (void)Close();
  WaitClosed();  // join 两条泵:返回即它们都不再触碰本对象,可安全销毁设备对象。
  if (port_) {
    port_->deleteLater();
  }
}

// 配置校验(D12):**在 `Start()` 里一次性做**,这是 SRS「不可重试失败」清单中"无效配置"
// 一项的落点——非法配置根本不进入重开循环。
Coro::Result<void> SerialTransport::ValidateConfig() const {
  if (config_.device.empty() || config_.baud_rate == 0) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (!IsValidDataBits(config_.data_bits) ||
      !IsValidStopBits(config_.stop_bits) || !IsValidParity(config_.parity)) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // **须为正,没有“0 = 禁用”这一档**:它同时是退避间隔,零值直接退化为紧循环(设备不
  // 存在时 `open()` 微秒级失败,烧 CPU);且静默超时是串口判活的**唯一**判据(D4),
  // 禁用它等于放弃判活。
  if (config_.silence_timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Coro::Result<void>{};
}

Coro::Result<void> SerialTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};
  }
  if (lifecycle_ != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidateConfig(); !valid) {
    // **停在 `Created`**:未建设备、未起泵,允许改配后重试(RT_LIFECYCLE_007)。
    last_error_ = valid.error();
    return valid.error();
  }
  // 设备对象在本 fiber(节点执行域)内创建,守 Qt 对象亲和纪律。
  port_ = new QSerialPort();
  lifecycle_ = LifecycleState::kRunning;

  // **首次打开就地做一次**(不等泵被调度)——串口的 `open()` 是**同步**的,故
  // `CurrentLinkState()` 在 `Start()` 返回后即可如实观测。**打开失败不算启动失败**
  // (D1)——泵会退避后无限重试。
  (void)Open();

  write_pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunWritePump(); }));
  pump_ = std::make_shared<Coro::FiberTask<void>>(
      Coro::makeTask([this] { RunDevicePump(); }));
  return Coro::Result<void>{};
}

// 一次打开尝试。**同步**,无挂起点(这正是串口比 TCP 少一处时间量用途的原因)。
bool SerialTransport::Open() {
  port_->close();  // 幂等前置:把设备重置到可 open 状态(未打开时是空操作)。
  port_->clearError();
  port_->setPortName(QString::fromStdString(config_.device));
  if (!port_->open(QIODevice::ReadWrite)) {
    last_error_ = MapSerialError(port_->error());
    if (!last_error_) {
      last_error_ = make_error_code(TransportErrc::kConnection);
    }
    return false;
  }
  // 参数应用:值域已由 `ValidateConfig()` 保过,此处失败是**设备侧**的事实(如 PTY 不
  // 支持某项),按 I/O 错误归因并放弃本轮——**不是配置错误**,故不改变生命周期。
  if (!port_->setBaudRate(static_cast<qint32>(config_.baud_rate)) ||
      !ApplyDataBits(*port_, config_.data_bits) ||
      !ApplyStopBits(*port_, config_.stop_bits) ||
      !ApplyParity(*port_, config_.parity)) {
    last_error_ = MapSerialError(port_->error());
    if (!last_error_) {
      last_error_ = make_error_code(TransportErrc::kIo);
    }
    port_->close();
    return false;
  }
  return true;
}

// 采样设备错误:**只落 `LastError()`,绝不改控制流**(D11 + D4)。
//
// 这是“不订阅 `errorOccurred`”的替代路径:在读泵每轮就地**采样一次**当前错误位并清除,
// 代价恒定,且天生不会被错误风暴淹没。
//
// 线路噪声类(`ParityError`/`FramingError`/`BreakConditionError`)**不触发设备重建**
// ——噪声帧本就该由 codec 的 CRC 与重同步处置(DD-15 的第一层补救),重建反而丢掉更多
// 在途字节。其余类别同样不触发重建:**重建只由静默超时驱动**(D4)。
void SerialTransport::AbsorbDeviceError() {
  if (port_->error() == QSerialPort::NoError) {
    return;
  }
  last_error_ = MapSerialError(port_->error());
  port_->clearError();  // 清位,下轮才能观测到"新"的错误。
}

// 管理泵 fiber(ADR-0012 D2,与 `UdpTransport::RunSocketPump()` 同构):
//
//   while (!closing) {
//     if (已打开 || Open()) {
//       device_ready 先清后发;
//       read_stream = readAll();                  ← 存成员,供 Close 打断 (D6)
//       while (!closing) { r = await_for(read_stream, timeout);  ← 判据即「建完即复查」
//                          if (!r) { 归因; break }
//                          if (r->isEmpty()) continue      ← 【D5:串口独有的一行】
//                          push(切片) }
//     } else { await_for(close_signal, timeout) }  ← 退避
//     port->close()                                ← 每轮末尾无条件清理
//   }
//
// **重开不是独立机制**:它就是本 while 转第二圈;首开与重开走同一段代码,不作区分。
// 两处的 timeout 是**同一个量**(D3):多久没数据算链路坏 / 多久试一次 open。**比 TCP
// 少一处用途**——`open()` 同步,没有"等连上"。
//
// ★ **「建完即复查」**(ADR-0011 D15 补正,#200):`Close()` 只关得到它跑的那一刻**已经
//   存在**的句柄;泵之后才建出的读流没有任何人会唤醒,会挂满一个 `silence_timeout`。
//   故内层循环的判据是复查 `lifecycle_`,而不是 `for(;;)`。
void SerialTransport::RunDevicePump() {
  const std::chrono::milliseconds timeout = config_.silence_timeout;

  while (lifecycle_ < LifecycleState::kClosing) {
    // 已打开则跳过——本条**只在首轮**为真(Start 已就地 open 过一次)。第二轮起底部的
    // `close()` 已把设备置为未打开,本条恒为假。
    if (port_->isOpen() || Open()) {
      // 通告写泵。**先清后发**:每轮外层都是一次真实的 down→up 跃迁,而写泵停在"等
      // 数据"时没人来取,不清就会一直堆积。信号因此恒定只有 0 或 1 个 token。
      device_ready_->channel()->discard_pending();
      device_ready_->resolve();

      // 每代重建读流(旧流已随上一轮 close 死掉);建流时会 drain 订阅前已到的字节,
      // 不丢首片。**存成成员**供 `Close()` 打断(D6)。
      // `coroiodevice::readAll()` **按值**返回 `Awaitable`(与 `corosocket::readAll()`
      // 返回 `shared_ptr` 不同),故此处自行装箱——句柄要持为成员供 `Close()` 打断(D6)。
      read_stream_ = std::make_shared<Coro::Awaitable<QByteArray>>(
          Coro::coro(static_cast<QIODevice*>(port_)).readAll());
      // 【建完即复查】(ADR-0011 D15 补正,#200):**本循环的判据就是那次复查**——它在第
      // 一次 `await_for` 之前求值,故"`Close()` 已跑完、关的是一个当时还是 null 的
      // `read_stream_`"这一格当场终结,**不进 await**。
      while (lifecycle_ < LifecycleState::kClosing) {
        auto chunk = Coro::await_for(read_stream_, timeout);
        if (!chunk) {
          // 三条成因**不作区分**,一律 break 回外层重开:
          //   ① 静默超时 —— **唯一的主动判据**(D4,反转 ADR-0011 D4):串口没有断开
          //      事件,`coroiodevice::readAll()` 只订阅 readyRead 与 aboutToClose;
          //   ② 读流被 `aboutToClose` 终结 —— 设备被关(含我方 Close 的 port->close());
          //   ③ 我方 `Close` 关了 read_stream_ —— 由 while 判据接住,不记归因。
          Attribute(chunk.error(), last_error_);
          break;
        }
        // 设备错误只作为诊断事实吸收,**不改控制流**(D11)。
        AbsorbDeviceError();
        const QByteArray& bytes = chunk.value();
        // ★★ 【D5,串口独有的一行】`coroiodevice::readAll()` 的 push **不判空**
        // (`ch->push(dev->readAll())`),而 `corosocket::readAll()` 有
        // `if(!bytes.isEmpty())` 守卫、其初次 drain 亦有 `bytesAvailable() > 0` 检查。
        // 少了这一行,调用方就可能在 `read_queue` 上取到空 `Datagram`——UDP/TCP 都不
        // 需要它,这是“照抄样板就会漏”的典型。
        if (bytes.isEmpty()) {
          continue;
        }
        // chunk 是**任意字节切片**,不是一个完整帧(RT_TRANSPORT_003);组帧归 ICodec。
        const auto* first =
            reinterpret_cast<const std::uint8_t*>(bytes.constData());
        Datagram out{{first, first + bytes.size()}, peer_};  // 固定设备端点(D9)。
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
      // 打不开:退避。**必须用独立的延时原语**——拿"在未打开的设备上建读流"当退避会被
      // 当场关闭、退化为紧转(`UdpTransport` 的注释记着同一个坑)。close_signal_ 被
      // Close 关闭时立即返回,故退避可提前打断。
      Coro::await_for(close_signal_, timeout);
    }
    // 每轮末尾**无条件 close()**:设备回到"未打开"这一个确定状态,下一轮从它重建——
    // 不必区分上轮怎么结束的,也**不需要新建 `QSerialPort` 对象**(D2)。它同时是
    // `errorOccurred` 风暴的止血手段(实测 `port->close()` **0ms** 止住,D11)。
    port_->close();
  }

  // 收尾:**先 join 写泵**(确保它不再碰设备),再关读队列、落 Closed。**没有完成量**
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
// 不变式:**取到设备到写出之间没有挂起点**(D8)——`write()` 交给 Qt 内部写缓冲即返回,
// 而我们**不等刷出**,故写泵 fiber 不可能在“判 isOpen”与“write”之间被调度走、让管理泵
// 把设备关掉。
void SerialTransport::RunWritePump() {
  for (;;) {
    // ── 阻塞点①:等数据 ──(Close 关 write_queue 唤醒)
    auto item = Coro::await(write_queue_);
    if (!item) {
      return;  // 队列被关闭 → 退出(残留数据随之丢弃)。
    }
    const Datagram& unit = item.value();
    // `unit.peer` **被忽略**(D9):串口只有一个设备,一律写往它。**不判
    // kInvalidArgument**——那会让恒发 `Endpoint::Default()`(或填了别的目的地)的“传输
    // 无关调用方”在串口上跑不起来。

    // ── 阻塞点②:等设备就绪 ──(Close 关 device_ready 唤醒)
    // 设备不可用(重开退避中)时就停在这里,数据留在队列里等,**不丢弃、不回传错误**;
    // 恢复后管理泵 resolve() 叫醒本泵,按序发出积压。
    bool ready = false;
    while (lifecycle_ < LifecycleState::kClosing) {
      if (port_->isOpen()) {
        ready = true;
        break;
      }
      device_ready_->channel()->discard_pending();  // 清陈旧标记,再等下次就绪通告。
      if (!Coro::await(device_ready_)) {
        break;  // 信号被 Close 关闭 → 退出。
      }
    }
    if (!ready) {
      return;
    }

    // ── 写出 ──【同步,无挂起点】(D8)
    // **不等 bytesWritten、不等 bytesToWrite() == 0**:写本就是 fire-and-forget
    // (ADR-0007 D3),等刷出不改变该语义、只让写泵多挂起一次。
    const auto size = static_cast<qint64>(unit.bytes.size());
    const qint64 n =
        port_->write(reinterpret_cast<const char*>(unit.bytes.data()), size);
    if (n < 0) {
      // 写失败 → **放弃本条**,只落诊断事实(不自终、不回传调用方)。
      last_error_ = MapSerialError(port_->error());
      if (!last_error_) {
        last_error_ = make_error_code(TransportErrc::kIo);
      }
    } else if (n != size) {
      // 短写视为链路异常:**放弃残余,不循环重试**——既然不等刷出,短写就没有可等的
      // 东西。残余丢失落在 DD-15 的既定范围内:写了半条即半条,由对端重同步。
      last_error_ = make_error_code(TransportErrc::kIo);
    }
    // 回阻塞点①取下一条。
  }
}

// 交出 read_queue 句柄(ADR-0007 D4):未 Start 时给一个以 kInvalidState 关闭的句柄;
// 已关闭时 read_queue 已被泵以 kClosed 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> SerialTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    return ClosedQueue<Datagram>(make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

// 送入写队列即返(ADR-0007 D3):**不等待实际发出**,返回成功仅表示"已入队"。写出的一切
// 结果(写失败、短写)都在写泵里,只落 LastError(),不回传。
//
// **设备不可用时同样返回成功**:数据在队列里等,写泵停在阻塞点②。但队列有界 1024 且满时
// **静默丢最旧**(DD-15),故"不拒绝、不丢弃"的"不丢弃"只在未超界时成立——已知且已接受
// (#176),此处**不作归因**。
Coro::Result<void> SerialTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kClosed);
  }
  // `datagram.peer` 不在此处解析,也不校验(D9):写泵一律忽略它。
  if (write_queue_->channel()->push(std::move(datagram)) !=
      boost::fibers::channel_op_status::success) {
    return make_error_code(TransportErrc::kClosed);  // 队列已关闭 = 传输终结。
  }
  return Coro::Result<void>{};
}

// 请求关闭(幂等):**四处打断缺一不可**——漏一处,`Close()` 落在对应窗口时就要挂满一个
// `silence_timeout`(写泵那两处是**无限期** `await`,漏了就是**永久挂死**)。只发信号,
// 不等收敛(收尾由泵自己跑完)。
//
// | # | 阻塞点                                        | 打断者                    |
// |---|-----------------------------------------------|---------------------------|
// | ① | 管理泵:`await_for(close_signal_, timeout)` 退避 | `close_signal_->close()`  |
// | ② | 管理泵:`await_for(read_stream_, timeout)` 读等待 | `read_stream_->close()`   |
// | ③ | 写泵:`await(write_queue_)` 等数据              | `write_queue_->close()` + `discard_pending()` |
// | ④ | 写泵:`await(device_ready_)` 等设备就绪         | `device_ready_->close()`  |
//
// **只有四处,不是 TCP 的五处**(D6):串口没有“连接窗口”,ADR-0011 **D15** 的第五处
// (`connect_waiter_`)不适用。`port_->close()` 也能打断活跃读流(走 `aboutToClose`);
// `read_stream_->close()` 仍保留,它不依赖 Qt 的信号时序。
Coro::Result<void> SerialTransport::Close() {
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

  close_signal_->close(closed);  // ① 打断重开退避。
  if (read_stream_) {
    read_stream_->close(closed);  // ② 打断读等待。
  }
  if (port_) {
    port_->close();  // 同上,另一条有效路径(走 aboutToClose);兼止错误风暴(D11)。
  }
  write_queue_->close(closed);                 // ③ 唤醒写泵阻塞点①(等数据)。
  write_queue_->channel()->discard_pending();  // 未发出的残留随之丢弃(不等刷出,D8)。
  device_ready_->close(closed);                // ④ 唤醒写泵阻塞点②(等设备就绪)。
  return Coro::Result<void>{};
}

// join 管理泵 fiber(它内部已先 join 写泵)。返回即两条 fiber 都不再触碰本对象——这是
// "可安全释放"的充分条件,`Awaitable::close()` 给不了(它只保证等待者被唤醒)。`get()`
// 是一次性的(底层 boost future 取过即失效),故用 joined_ 闩保证幂等。
void SerialTransport::WaitClosed() {
  if (joined_ || !pump_) {
    return;  // 已 join 过,或从未 Start:无可汇合者。
  }
  joined_ = true;
  (void)pump_->get();
}

bool SerialTransport::IsRunning() const {
  return lifecycle_ == LifecycleState::kRunning;
}

std::error_code SerialTransport::LastError() const { return last_error_; }

// **无状态成员,当场算出**(D10)。**只有两值**:退避重开期间报 `kDown`(此刻确实收发
// 不了字节),`kEstablishing` **永不出现**。
LinkState SerialTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning || !port_) {
    return LinkState::kDown;
  }
  return port_->isOpen() ? LinkState::kUp : LinkState::kDown;
}

}  // namespace transport
