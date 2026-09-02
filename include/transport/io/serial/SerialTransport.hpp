#pragma once

/**
 * @file SerialTransport.hpp
 * @brief 协程原生串口传输——设备管理泵 + 内建透明重开的字节流管道(ADR-0012)。
 */

#include <memory>
#include <system_error>

#include "await/awaitable.hpp"  // Coro::Awaitable —— 队列与信号。
#include "task/fibertask.h"     // Coro::FiberTask —— 泵 fiber 的结构化并发句柄。

#include "transport/core/Endpoint.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/io/serial/SerialConfig.hpp"

class QByteArray;
class QSerialPort;

namespace transport {

/**
 * @brief 协程原生串口传输——面向字节流、内建透明设备重开(ITransport 实现)。
 *
 * **形态与 `UdpTransport` / `TcpTransport` 同构**(ADR-0012 D2,ADR-0007 D1/D2/D3/D4 的
 * 样板):外层设备管理泵负责 `open` / `close` / 退避重开,内层数据泵把读到的字节切片投入
 * `read_queue`;两条队列**不随设备重开而更换**,故重开对调用方完全透明(DD-11)。
 *
 * **串口比 TCP 更像 UDP**,差异集中在三点(ADR-0012,SDD §4.2.14 图 4-16):
 *
 * | | UDP | TCP | **串口** |
 * |---|---|---|---|
 * | 外层动作 | `bind` 同步 | `connectToHost` **异步**,要等 | **`open` 同步**,故**无"等连上"这一处** |
 * | 判活主判据 | `silence_timeout`(唯一) | 对端断开事件(主)+ 静默(辅) | **`silence_timeout`(唯一)** |
 * | 空切片 | 不会出现 | 不会出现 | **必须显式跳过** |
 *
 * - **不自终**(**D1**,TBD-005 关闭,推翻 ADR-0002 D3′ 与 RT_LIFECYCLE_008 的串口一支):
 *   `open()` 失败、读流终止与静默超时一律回外层重开、无限重试,唯一的退出条件是我方
 *   `Close`;底层故障降为诊断事实留在 `LastError()`。
 *   **依据是技术的**:自终要求一个"致命错误"判据,而串口在流层面**拿不到**——见下条。
 * - **判活:`silence_timeout` 是唯一主动判据**(**D4**,**反转 ADR-0011 D4**):串口
 *   **没有断开事件**——`coroiodevice::readAll()` **只订阅 `readyRead` 与 `aboutToClose`**
 *   (`corosocket::readAll()` 订阅五个,含 socket error 与 `disconnected`,TCP 的断链正是
 *   靠它们到达)。实测:关掉 pty master 后读流**完全不终止**、挂满 1500ms 报 `timed_out`,
 *   `isOpen()` 仍为 `true`。写路径**连判活都做不了**(设备消失后 `write()` 照样成功、
 *   `bytesToWrite()` 永不下降),故判活只能落在读侧。
 * - **`errorOccurred` 是噪声而非事件**(**D11**):实测拔线后以 **~950 次/秒**风暴式连发。
 *   故**不订阅它当断链判据**;线路噪声类(`ParityError`/`FramingError`/
 *   `BreakConditionError`)只落 `LastError()`,**不触发设备重建**——重建**只由静默超时
 *   驱动**。噪声帧本就该由 codec 的 CRC 与重同步处置(DD-15 的第一层补救)。
 * - **唯一的时间量**(**D3**):`SerialConfig::silence_timeout` 两处共用——读静默判链路坏 /
 *   重开退避间隔。**比 TCP 少一处**(无"等连上"),**须为正**(**D12**)。
 * - `AsyncRead()` 交出 `read_queue` 句柄(ADR-0007 D4),每个元素是**任意字节切片**、不是
 *   一个完整帧(RT_TRANSPORT_003,组帧归 `ICodec`);`peer` 一律填固定设备端点(**D9**)。
 * - `AsyncWrite()` **入队即返**(ADR-0007 D3),`peer` **被忽略**(**D9**,不判
 *   `kInvalidArgument`);写出的一切结果只落 `LastError()`、不回传。
 * - **写不等刷出**(**D8**,与 TCP 同构):实测 `QSerialPort::write(4096)` 返 4096
 *   (**不短写**)、`bytesToWrite()` 立刻查为 4096(**不同步刷出**)、50ms 后归 0——与
 *   `QTcpSocket::write()` 逐条一致。故写泵**写出段无挂起点**,三个写泵结构完全同构。
 * - **整个生命期一个 `QSerialPort`**(**D2**,概念同 ADR-0011 D3):每轮末尾 `close()`,
 *   下轮在同一对象上 `open()`,不新建对象。
 *
 * **`Close()` 是四处打断,不是 TCP 的五处**(**D6**):串口**没有"连接窗口"**,故
 * ADR-0011 **D15**(须持 `connect_waiter_` 句柄打断)**不适用**。且实测 `port->close()`
 * **能**打断活跃读流(50ms 唤醒,走 `aboutToClose`)——这与 TCP 在 `ConnectingState` 下
 * `abort()` 唤不醒任何等待**恰好相反**。`read_stream_` 仍持为成员并在 `Close()` 中
 * `close()`:成本一行,且与 TCP 形态一致。
 *
 * **「建完即复查」同样适用**(ADR-0011 **D15 补正**,#200):`Close()` 只关得到**调用时
 * 已经存在**的句柄,泵之后才建出的读流没有任何人会唤醒。串口**未复现**该竞态(`Open()`
 * 同步,循环顶部判据到建流之间没有挂起点,窗口是空的),但句柄形态与 TCP 同构、且
 * `coroiodevice::readAll()` 没有"设备已关就当场关流"的出生守卫,窗口一旦被打开就是挂满
 * 一个 `silence_timeout`。故读循环的判据即是那次复查:`while (lifecycle_ < kClosing)`。
 *
 * **单线程(fiber 协作式),不加锁**:两条泵由 `Start()` 用 `Coro::makeTask` 起,默认亲和
 * 是 `fixed(调用线程)`,故它们与本对象的全部公开方法跑在**同一个线程**上、只在 await 点
 * 交错。代价是**公开方法必须在起它的那个执行域内调用**,这也正是 Qt 对象亲和的要求。
 *
 * 析构 `Close()` + `WaitClosed()`,后者 join 管理泵(管理泵内部已先 join 写泵)——故两条
 * fiber 都不可能活过本对象。
 */
class SerialTransport final : public ITransport {
 public:
  /// @brief 以 SerialConfig 构造(尚未创建设备对象);`QSerialPort` 在 `Start()` 内创建。
  explicit SerialTransport(SerialConfig config);
  /// @brief 析构:请求关闭、join 两条泵,再销毁设备对象。
  ~SerialTransport() override;

  SerialTransport(const SerialTransport&) = delete;
  SerialTransport& operator=(const SerialTransport&) = delete;

  /// @brief 校验配置、创建 `QSerialPort`、起两条泵,进入 Running(须在节点执行域 fiber
  ///        内调用)。
  ///
  /// **就地试开一次**(与 UDP 同、与 TCP 异):串口的 `open()` 是**同步**的,故
  /// `CurrentLinkState()` 在 `Start()` 返回后即可如实观测,不会把 `Start()` 变成一个
  /// 阻塞调用。**但打开失败不算启动失败**(**D1**):泵会退避后无限重试,直至我方
  /// `Close`。
  ///
  /// @return 成功;配置非法(device 空 / baud_rate 为 0 / data_bits、stop_bits、parity
  ///         越界 / silence_timeout 非正)返 `kConfiguration` 并**停在 `Created`**(未建
  ///         设备、未起泵,可改配后重试,RT_LIFECYCLE_007);非法生命周期(关闭中/已关闭)
  ///         返 `kInvalidState`。已 Running 时重复调用为成功 no-op。
  Coro::Result<void> Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是**任意字节切片**,
  ///        `peer` 一律为固定设备端点(**D9**)。**空切片不会出现**(**D5**:读泵已显式
  ///        跳过)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理。
  ///         **传输终结**表现为队列被 `close(kClosed)`,而**只有我方 `Close` 才终止**
  ///         ——设备打开失败与静默超时均由泵内部无限重开消化,不向调用方暴露(**D1**);
  ///         底层成因经 `LastError()` 诊断。未 `Start()` 时给出以 `kInvalidState` 关闭
  ///         的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead() override;

  /// @brief 送入写队列即返(ADR-0007 D3):**不等待实际发出**,更**不等刷出**(**D8**)。
  ///
  /// `datagram.peer` **被忽略**(**D9**):串口无 peer 概念,任何值都写往配置的那一个
  /// 设备,**不判 `kInvalidArgument`**——那会让"传输无关的调用方"在串口上跑不起来。
  ///
  /// 设备不可用(重开退避中)时**照常入队、返回成功**:数据留在队列里等,写泵停在
  /// "等设备就绪"上,恢复后按序发出积压。**"不丢弃"有限定**:`write_queue_` 默认有界
  /// 1024 且**静默丢最旧**(DD-15),积压超界即丢、`push` 仍报成功。
  ///
  /// @return 成功仅表示**已入队**;未 `Start()` 返 `kInvalidState`,关闭中/已关闭返
  ///         `kClosed`。写出的结果(写失败、短写)只落 `LastError()`,不回传。
  [[nodiscard]] Coro::Result<void> AsyncWrite(Datagram datagram) override;

  /// @brief 请求关闭(幂等,**只发信号不等收敛**):打断管理泵的两处(退避 / 读等待)与
  ///        写泵的两处(等数据 / 等设备就绪),**四处缺一不可**(**D6**);随后由管理泵
  ///        自行跑完收尾(join 写泵、关 `read_queue`、落 Closed)。
  Coro::Result<void> Close() override;

  /// @brief join 管理泵 fiber(它内部已先 join 写泵),返回即两条 fiber 都不再触碰本对象。
  ///        未 `Start()` 或已 join 过时立即返回。
  void WaitClosed() override;

  /// @brief 是否处于 Running(泵在跑;设备是否打开另见 `CurrentLinkState()`)。
  [[nodiscard]] bool IsRunning() const;

  // 观测面——I/O 事实。`LastSendTime()` / `LastReceiveTime()` / `SendWaiterDepth()`
  // 已随 ADR-0008 D10 与 ADR-0007 D3 删除,不在本类重建。

  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性——**无状态成员,当场由 `lifecycle_` 与 `port_->isOpen()`
  ///        算出**(**D10**)。设备已打开 → `kUp`;未 `Start()`、打开失败、**退避重开
  ///        期间**与已关闭 → `kDown`。
  ///
  /// **只有两值,永不出现 `kEstablishing`**(**D10**):这一点与 `UdpTransport` 一致、
  /// 与 `TcpTransport` 分歧。退避重开期间报 `kDown` 更诚实——此刻**确实收发不了字节**;
  /// 且退避是连接管理策略,不经本查询暴露。
  ///
  /// **定位同 ADR-0011 D12**:统一的 I/O 事实查询,**不面向业务调用方**,仅供诊断与测试
  /// 观测——重开对交互层完全透明,设备不可用时发送入队等待,调用方不必先查链路。
  [[nodiscard]] LinkState CurrentLinkState() const override;

 private:
  /// @brief `Start()` 的一次性配置校验(D12):非法返 `kConfiguration`。
  [[nodiscard]] Coro::Result<void> ValidateConfig() const;
  /// @brief 一次打开尝试:`open()` + 应用波特率/数据位/停止位/校验;失败只记
  ///        `last_error_` ——**不是启动失败**,由泵退避后无限重试(D1)。
  bool Open();
  /// @brief 采样设备错误并清除:**只落 `LastError()`,绝不改控制流**(**D11** + **D4**)。
  void AbsorbDeviceError();
  /// @brief 管理泵 fiber:外层管设备的打开/关闭/退避重开,内层把字节切片投入
  ///        `read_queue_`;退出后 join 写泵、关读队列、落 Closed。
  void RunDevicePump();
  /// @brief 写泵 fiber:从 `write_queue_` 取出并写设备。两个阻塞点(等数据 / 等设备
  ///        就绪),串行;**写出段无挂起点**(D8)。
  void RunWritePump();

  SerialConfig config_;
  QSerialPort* port_{nullptr};  ///< 整个生命期只一个(D2);Start 建、析构销。
  Endpoint peer_;               ///< 固定设备端点,构造时由 config 算出(D9)。

  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool joined_{false};  ///< WaitClosed 已 join:`FiberTask::get()` 是一次性的。
  std::error_code last_error_;

  /// 对外读队列:**不随设备重开而更换**——这正是"重开对调用方透明"的载体。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 内部写队列。元素的 `peer` **不读作目的地**(同 TCP、与 UDP 异):串口只有一个设备,
  /// 写泵一律忽略它(**D9**)。有界 1024 且满时静默丢最旧(DD-15)。
  std::shared_ptr<Coro::Awaitable<Datagram>> write_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 管理泵 → 写泵的"设备可以写了"通告(复用一个,不换代;**先清后发**恒定 0 或 1 个
  /// token)。
  std::shared_ptr<Coro::Awaitable<void>> device_ready_{
      std::make_shared<Coro::Awaitable<void>>()};
  /// 只为打断重开退避:退避须用独立的延时原语(拿"在未打开的设备上建读流"当退避会被
  /// 当场关闭、退化为紧转,`UdpTransport` 的注释记着同一个坑)。
  std::shared_ptr<Coro::Awaitable<void>> close_signal_{
      std::make_shared<Coro::Awaitable<void>>()};
  /// 【每轮重建】读流句柄——持为成员供 `Close()` 打断(D6)。
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream_;

  std::shared_ptr<Coro::FiberTask<void>> pump_;        ///< WaitClosed join。
  std::shared_ptr<Coro::FiberTask<void>> write_pump_;  ///< 管理泵收尾时先 join。
};

}  // namespace transport
