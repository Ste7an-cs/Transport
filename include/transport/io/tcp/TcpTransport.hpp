#pragma once

/**
 * @file TcpTransport.hpp
 * @brief 协程原生 TCP 客户端传输——socket 管理泵 + 内建透明重连的字节流管道。
 */

#include <cstdint>
#include <memory>
#include <system_error>

#include "await/awaitable.hpp"  // Coro::Awaitable —— 队列与信号。
#include "task/fibertask.h"     // Coro::FiberTask —— 泵 fiber 的结构化并发句柄。

#include "transport/core/Endpoint.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/io/tcp/TcpConfig.hpp"

class QByteArray;
class QTcpSocket;

namespace transport {

/**
 * @brief 协程原生 TCP 客户端传输——面向字节流、内建透明重连(ITransport 实现)。
 *
 * **形态与 `UdpTransport` 同构**(ADR-0011 D2,ADR-0007 D1/D2/D3/D4 的样板):外层
 * socket 管理泵负责连接/重连,内层数据泵把读到的字节切片投入 `read_queue`;队列
 * **不随连接重建而更换**,故重连对调用方完全透明(DD-11)。三件套(`TcpTransport` /
 * `TcpClientTransport` / `TcpClientConfig`)已收成本类一件(**D1**)——重连是 TCP 客户端的
 * 固定语义,做成外层泵的一部分即可,不需要单设"重连外壳"一层。
 *
 * - **重连不是独立机制**(SDD §5.6.1 ①'):它就是外层 `while` 转第二圈。首连与重连
 *   不作区分、走同一段代码;没有重连状态、没有重连专用 fiber。
 * - **不自终**(ADR-0007 D2):连接失败、读流终止与静默超时一律回外层重试、无限重连,
 *   唯一的退出条件是我方 `Close`;底层故障降为诊断事实留在 `LastError()`。
 * - **唯一的时间量**(**D5**):`TcpConfig::silence_timeout` 三处共用——等连上 / 读静默
 *   判链路坏 / 连不上时的退避。**不设** `connect_timeout`,也不为退避另设间隔。
 * - **判活**(**D4**):对端断开事件是**主判据**(经 `readAll()` 流的自然终止到达,且
 *   `corosocket` 在关流前先 `drain()`,**尾字节不丢**);静默超时降为**辅助**判据,只在
 *   半开连接(对端进程消失、FIN 未达)时才轮得到。**不另订阅 `waitForDisconnected()`**
 *   ——那是多余的第二条路径。
 * - `AsyncRead()` 交出 `read_queue` 句柄(ADR-0007 D4),每个元素是**任意字节切片**、
 *   不是一个完整帧(RT_TRANSPORT_003,组帧归 `ICodec`);`peer` 一律填固定对端(**D8**)。
 * - `AsyncWrite()` **入队即返**(ADR-0007 D3):返回成功仅表示"已入队",写出的一切结果
 *   只落 `LastError()`、不回传。链路不可用时照常入队,写泵停在"等连接就绪"上,恢复后
 *   按序发出积压(RT_TCP_RECONNECT_003)——**但积压超过队列上界时静默丢最旧**(**D6**)。
 * - **写不等刷出**(**D13**):`socket_->write()` 把字节交给 Qt 内部写缓冲即返回,写泵
 *   **不等 `bytesWritten`、不等 `bytesToWrite() == 0`**。写本就是 fire-and-forget,等刷出
 *   不改变该语义、只让写泵多挂起一次。代价是 **Qt 内部写缓冲无上限**(`setWriteBufferSize`
 *   未设,Qt 默认 0 = 无上限),它是有界 `write_queue_` 挡不住的那一段;要给它设上界会让
 *   `write()` 真的开始短写,须连同 D13 与 D7 重新评审,**本轮不设**。
 * - 由此**写泵没有任何挂起点**:`UdpTransport` 写泵那条"取到 socket 到写出之间没有挂起点"
 *   的不变式**对本类同样成立**,两个写泵在结构上完全同构(TCP 侧只是把"等 bind 就绪"换成
 *   "等连接就绪")。**单消费者写泵**保证 `RT_TRANSPORT_004`(并发写串行化、两帧字节不交错);
 *   断链把一帧截断**不属于交错**,半条即半条,由对端重同步(**D7**)。
 * - **整个生命期一个 `QTcpSocket`**(**D3**):每轮末尾 `abort()` 使其回到
 *   `UnconnectedState`,下轮在同一对象上重连,不新建 socket 对象。
 *
 * **`socket_->abort()` 是清理动作,不是打断手段**(**D15**,实测结论):Qt 的 `abort()`
 * 在**连接中**的 socket 上不发 `errorOccurred`,而 `corosocket` 的 `waitForSignal` /
 * `readAll` 都靠 socket error 或 `disconnected` 终结——故它在连接窗口内**唤不醒任何
 * 等待**(实测挂满整个超时)。因此 `connect_waiter_` 与 `read_stream_` **持为成员**,
 * `Close()` 逐个 `close()` 它们。UDP 没有这个窗口(其 `bind()` 同步),故其 `close()`
 * 打断活跃读流是有效的——**这条不可照搬**。
 *
 * **单线程(fiber 协作式),不加锁**:泵由 `Start()` 用 `Coro::makeTask` 起,默认亲和是
 * `fixed(调用线程)`,故它与本对象的全部公开方法跑在**同一个线程**上、只在 await 点交错。
 * 代价是**公开方法必须在起它的那个执行域内调用**,这也正是 Qt 对象亲和的要求。
 *
 * 析构 `Close()` + `WaitClosed()`,后者 join 管理泵(管理泵内部已先 join 写泵)——故两条
 * fiber 都不可能活过本对象。
 */
class TcpTransport final : public ITransport {
 public:
  /// @brief 以 TcpConfig 构造(尚未创建 socket);socket 在 `Start()` 内创建。
  explicit TcpTransport(TcpConfig config);
  /// @brief 析构:请求关闭、join 泵,再销毁 socket。
  ~TcpTransport() override;

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;

  /// @brief 校验配置、创建 `QTcpSocket`、起泵,进入 Running(须在节点执行域 fiber 内调用)。
  ///
  /// **不等首连成功**(SDD §5.6.1):`connect` 是异步的,就地等会把 `Start()` 变成一个
  /// 最长一个 `silence_timeout` 的阻塞调用。故返回时 `CurrentLinkState()` 通常是
  /// `kEstablishing`,首连未成**不算启动失败**(ADR-0007 D2)。
  ///
  /// @return 成功;配置非法(host 空 / port 为 0 / silence_timeout 非正)返
  ///         `kConfiguration` 并**停在 `Created`**(未建 socket、未起泵,可改配后重试,
  ///         RT_LIFECYCLE_007);非法生命周期(关闭中/已关闭)返 `kInvalidState`。
  ///         已 Running 时重复调用为成功 no-op。
  Coro::Result<void> Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是**任意字节切片**,
  ///        `peer` 一律为固定对端(`Endpoint::Net(config.host, config.port)`,D8)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理。
  ///         **传输终结**表现为队列被 `close(kClosed)`,而**只有我方 `Close` 才终止**
  ///         ——断链由泵内部透明重连消化,不向调用方暴露(DD-11);底层成因经
  ///         `LastError()` 诊断。未 `Start()` 时给出以 `kInvalidState` 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead() override;

  /// @brief 送入写队列即返(ADR-0007 D3):**不等待实际发出**,更**不等刷出**(D13)。
  ///
  /// `datagram.peer` **被忽略**(**D8**):TCP 点对点,任何值都发往配置的固定对端,
  /// **不判 `kInvalidArgument`**——那会让"传输无关的调用方"(恒发 `Endpoint::Default()`
  /// 或填了别的目的地)在 TCP 上跑不起来。这与 `UdpTransport` 不同,后者解析不了目的地
  /// 会丢该条并记 `LastError()`。
  ///
  /// 链路不可用时**照常入队、返回成功**(RT_TCP_RECONNECT_003:"投入发送队列等待链路
  /// 恢复,不拒绝、不丢弃")。**"不丢弃"有限定**:`write_queue_` 默认有界 1024 且
  /// **静默丢最旧**(**D6**),积压超界即丢,`push` 仍报成功——已知且已接受(见 #176)。
  ///
  /// @return 成功仅表示**已入队**;未 `Start()` 返 `kInvalidState`,关闭中/已关闭返
  ///         `kClosed`。写出的一切结果(socket 写失败、短写)只落 `LastError()`,不回传。
  [[nodiscard]] Coro::Result<void> AsyncWrite(Datagram datagram) override;

  /// @brief 请求关闭(幂等,**只发信号不等收敛**):打断管理泵的三处(退避 / 等连上 /
  ///        读等待)与写泵的两处(等数据 / 等连接就绪),**五处缺一不可**;随后由管理泵
  ///        自行跑完收尾(join 写泵、关 `read_queue`、落 Closed)。
  Coro::Result<void> Close() override;

  /// @brief join 管理泵 fiber(它内部已先 join 写泵),返回即两条 fiber 都不再触碰本对象。
  ///        未 `Start()` 或已 join 过时立即返回。
  void WaitClosed() override;

  /// @brief 是否处于 Running(泵在跑;链路是否连上另见 `CurrentLinkState()`)。
  [[nodiscard]] bool IsRunning() const;

  // 观测面——I/O 事实。三个 TCP 独有的诊断方法(`Generation()` / `AttemptCount()` /
  // `LastFailure()`)与 `State()` / `WaitForState()` 已随 ADR-0011 D9/D12 删除。

  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性——**无状态成员,当场由 `lifecycle_` 与 `socket_->state()`
  ///        算出**(D12)。已连接 → `kUp`;未 `Start()` / 已关闭 → `kDown`;连接中、
  ///        主机名解析中与**退避重连中**一律 `kEstablishing`。
  ///
  /// 最后一支是 TCP 与 UDP 的**真正分歧**(UDP 未绑定即报 `kDown`,永不出现
  /// `kEstablishing`)。**定位**:统一的 I/O 事实查询,**不面向业务调用方**,仅供诊断与
  /// 测试观测——重连对交互层完全透明,链路不可用时发送入队等待,调用方不必先查链路。
  [[nodiscard]] LinkState CurrentLinkState() const override;

 private:
  /// @brief `Start()` 的一次性配置校验(D14):非法返 `kConfiguration`。
  [[nodiscard]] Coro::Result<void> ValidateConfig() const;
  /// @brief 泵 fiber:外层管连接的建立/重建/退避,内层把字节切片投入 `read_queue_`;
  ///        退出后 join 写泵、关读队列、落 Closed。
  void RunSocketPump();
  /// @brief 写泵 fiber:从 `write_queue_` 取出并写 socket。两个阻塞点(等数据 / 等连接
  ///        就绪),串行;**写出段无挂起点**(D13)。
  void RunWritePump();

  TcpConfig config_;
  QTcpSocket* socket_{nullptr};  ///< 整个生命期只一个(D3);Start 建、析构销。
  Endpoint peer_;                ///< 固定对端,构造时由 config 算出(D8)。

  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool joined_{false};  ///< WaitClosed 已 join:`FiberTask::get()` 是一次性的。
  std::error_code last_error_;
  /// 每次连上 +1。**纯内部记账**(D9/D12):不对外暴露、不驱动任何控制流、不做代际隔离,
  /// 唯一用途是 Trace 事件的归类与内部判重。
  std::uint32_t generation_{0};

  /// 对外读队列:**不随连接重建而更换**——这正是"重连对调用方透明"的载体。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 内部写队列。元素的 `peer` **不读作目的地**(与 UDP 的一处分歧):TCP 点对端固定,
  /// 写泵一律忽略它(**D8**)。有界 1024 且满时静默丢最旧(**D6**)。
  std::shared_ptr<Coro::Awaitable<Datagram>> write_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 管理泵 → 写泵的"可以写了"通告(复用一个,不换代;**先清后发**恒定 0 或 1 个 token)。
  std::shared_ptr<Coro::Awaitable<void>> socket_ready_{
      std::make_shared<Coro::Awaitable<void>>()};
  /// 只为打断重连退避:退避须用独立的延时原语(拿"在未连接的 socket 上建读流"当退避会
  /// 被当场关闭、退化为紧转,`UdpTransport` 的注释记着同一个坑)。
  std::shared_ptr<Coro::Awaitable<void>> close_signal_{
      std::make_shared<Coro::Awaitable<void>>()};
  /// 【每轮重建】等连上的句柄——**持为成员只为供 `Close()` 打断**(D15)。
  std::shared_ptr<Coro::Awaitable<void>> connect_waiter_;
  /// 【每轮重建】读流句柄——同上(D15)。
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream_;

  std::shared_ptr<Coro::FiberTask<void>> pump_;        ///< WaitClosed join。
  std::shared_ptr<Coro::FiberTask<void>> write_pump_;  ///< 管理泵收尾时先 join。
};

}  // namespace transport
