#pragma once

/**
 * @file UdpTransport.hpp
 * @brief 协程原生 UDP 传输——报文式收发,保留报文边界与发送方地址,socket 管理泵形态。
 */

#include <cstdint>
#include <memory>
#include <system_error>
#include <vector>

#include "task/fibertask.h"  // Coro::FiberTask —— 两条泵 fiber 的结构化并发句柄。

#include "transport/io/ITransport.hpp"
#include "transport/io/udp/UdpConfig.hpp"

class QUdpSocket;

namespace transport {

/**
 * @brief 协程原生 UDP 传输——无连接、报文式字节管道(ITransport 实现)。
 *
 * **socket 管理泵 + 读写双队列**(ADR-0007 D1/D2/D3 的样板实现):`Start()` 起两条
 * fiber 后即返回——**泵**(外层管 socket 的 bind/重建/重试,内层把收到的报文投入
 * `read_queue`)与**写泵**(从 `write_queue` 取出并写 socket)。两条队列**不随 socket
 * 重建而更换**,故重建对调用方完全透明。
 *
 * - **不自终**(ADR-0007 D2):bind 失败、读流终止与**静默超时**一律回外层重建、无限重试,
 *   唯一的退出条件是我方 `Close`;底层致命 I/O 降为诊断事实留在 `LastError()`。
 * - **静默超时**(`UdpConfig::silence_timeout`,0 禁用):读等待与超时判断**同在泵这一条
 *   fiber 内**——`await_for(stream, silence)` 超时即判链路已坏,解绑重建。UDP 无连接、
 *   无对端断开事件,这是"链路坏了"唯一的主动判据。
 * - `AsyncRead()` 只**交出 `read_queue` 的等待器句柄**(ADR-0007 D4),每个元素是一条完整
 *   报文、`peer` 填发送方地址(from 可变);终止表现为队列被 `close(kClosed)`。
 * - `AsyncWrite(datagram)` 只**入队即返**(ADR-0007 D3,fire-and-forget);其 `peer` 读作
 *   目的地,由**写泵**解析(`kDefault` → config 默认对端)。写出的一切结果不回传,只落
 *   `LastError()`。
 *
 * UDP 无连接:不构造伪连接状态,只暴露 I/O 事实(`LastError()` / `CurrentLinkState()`)。
 * **整个生命期只一个 socket 对象**(bind→close→再 bind 复用)。
 *
 * **单线程(fiber 协作式),不加锁**:两条泵由 `Start()` 用 `Coro::makeTask` 起,默认亲和
 * 是 `fixed(调用线程)`,故它们与本对象的全部公开方法都跑在**同一个线程**上、只在 await
 * 点交错——普通成员因此不需要互斥量,队列本身(`Coro::Awaitable`)另有其内部同步。代价是
 * **公开方法必须在起它的那个执行域内调用**,这也正是 Qt 对象亲和的要求(socket 建在该
 * 线程上)。
 *
 * 析构 `Close()` + `WaitClosed()`,后者 join 两条泵——故 fiber 不可能活过本对象,成员
 * 直接内联,不需要共享状态那层间接。
 *
 * 不在本类范围:队列容量与丢弃归因(TBD-009 / #152,沿用 AsyncTask 默认「有界 1024 +
 * 静默丢最旧」)。
 */
// 与 TCP 的差异:UDP `writeDatagram` 是同步非阻塞的单报文原子发送——要么整报文
// 进入操作系统发送缓冲、要么失败,无短写/部分写、无背压刷缓冲循环。写泵是
// `write_queue` 的**单消费者**,写入串行化由此天然保证(RT_TRANSPORT_004 保留的那半);
// 且"状态检查 → 写出"之间无挂起点,故不需要代际号校验(该不变式只对 UDP 成立)。
// 报文边界由内核保持,一次读恰好一条报文(RT_IF_UDP)。
class UdpTransport final : public ITransport {
 public:
  /// @brief 以 UdpConfig 构造(尚未创建 socket);socket 在 Start() 内创建。
  explicit UdpTransport(UdpConfig config);
  /// @brief 析构:请求关闭、join 泵(泵内部已先 join 写泵),再销毁 socket。
  ~UdpTransport() override;

  UdpTransport(const UdpTransport&) = delete;
  UdpTransport& operator=(const UdpTransport&) = delete;

  /// @brief 创建 QUdpSocket、起泵与写泵,进入 Running(须在节点执行域 fiber 内调用)。
  ///
  /// 首次 bind 就地尝试一次(故 `LocalPort()`/`CurrentLinkState()` 返回后即可观测),
  /// **但 bind 失败不算启动失败**(ADR-0007 D2):泵会等 `silence_timeout` 后无限重试,
  /// 直至我方 `Close`。已 Running 时重复调用为成功 no-op。
  /// @return 成功;非法生命周期(关闭中/已关闭)InvalidState。
  Coro::Result<void> Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是一条完整报文,
  ///        `peer` 填发送方地址(from 可变)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理,
  ///         传输层不设单读守卫。**传输终结**表现为队列被 `close(kClosed)`,而
  ///         **只有我方 `Close` 才终止**——bind 失败与 socket 级致命 I/O 均由泵内部
  ///         无限重试消化,不向调用方暴露(ADR-0007 D2),底层成因经 `LastError()` 诊断。
  ///         未 Start 时给出以 kInvalidState 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead() override;

  /// @brief 把一份报文送入内部写队列即返(ADR-0007 D3,fire-and-forget)。
  ///
  /// **不等待实际发出**,返回成功仅表示"已入队"。`datagram.peer` 读作目的地:`kDefault`
  /// → 由写泵解析为 UdpConfig 的默认对端(`remote_addr`/`multicast_group` +
  /// `remote_port`),`kNet` → 按 ip:port。无法解析的目的地(含 `kTopic`)由写泵丢弃该条
  /// 并记 `LastError()`——**不回传**。链路不可用(bind 重试中)时报文留在队列等待恢复,
  /// 不拒绝、不丢弃,恢复后按序全部发出;我方 `Close` 时未发出的残留随队列关闭而丢弃。
  ///
  /// @return 已入队;未 Start kInvalidState;关闭中/已关闭 kClosed。
  [[nodiscard]] Coro::Result<void> AsyncWrite(Datagram datagram) override;

  /// @brief 请求关闭(幂等,**只发信号不等收敛**):打断 bind 退避与活跃读流,唤醒写泵的
  ///        两个阻塞点,随后由泵自行跑完收尾(关 `read_queue`、落 Closed)。
  Coro::Result<void> Close() override;

  /// @brief join 泵 fiber(泵内部已先 join 写泵),返回即两条 fiber 都不再触碰本对象。
  ///        未 `Start()` 或已 join 过时立即返回。
  void WaitClosed() override;

  // 观测面——I/O 事实,非"连接健康"裁决(无连接介质判活留给协议层)。

  /// @brief 本地实际绑定端口(config 端口为 0 时由 OS 分配;未 Start / 尚未 bind 上
  ///        则为 0)。注意 config 端口为 0 时每次重 bind 会拿到**不同**的临时端口。
  [[nodiscard]] std::uint16_t LocalPort() const;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):socket 此刻已绑定 → `kUp`;未 Start、
  ///        bind 重试期间或已关闭 → `kDown`。UDP 无连接,故永不出现 `kEstablishing`
  ///        (bind 退避属连接管理策略,不经本查询暴露)。
  [[nodiscard]] LinkState CurrentLinkState() const override;

 private:
  /// @brief 一次 bind 尝试:成功记下实际端口并(组播)重新入组;失败只记 `last_error_`
  ///        ——**不是启动失败**,由泵退避后无限重试(ADR-0007 D2)。
  bool Bind();
  /// @brief 泵 fiber:外层管 socket 的绑定/解绑/重试,内层把收到的报文投入 `read_queue_`;
  ///        退出后 join 写泵、关读队列、落 Closed。
  void RunSocketPump();
  /// @brief 写泵 fiber:从 `write_queue_` 取出并写 socket。两个阻塞点,串行。
  void RunWritePump();

  UdpConfig config_;
  QUdpSocket* socket_{nullptr};  ///< 整个生命期只一个;Start 建、析构销。

  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool joined_{false};           ///< WaitClosed 已 join:`FiberTask::get()` 是一次性的。
  std::error_code last_error_;
  std::uint16_t local_port_{0};  ///< 最近一次成功绑定的端口。

  /// 对外读队列:**不随 socket 重建而更换**——这正是"重建对调用方透明"的载体。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 内部写队列;元素就是 `Datagram`,其 `peer` 读作**目的地**(已在 AsyncWrite 里解析好)。
  std::shared_ptr<Coro::Awaitable<Datagram>> write_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  /// 写泵等 socket 就绪的信号(复用一个,不换代)。
  std::shared_ptr<Coro::Awaitable<void>> socket_ready_{
      std::make_shared<Coro::Awaitable<void>>()};
  /// 只为打断"未 bind 时的退避":未 bind 的 socket 上建读流会被当场关闭(实测 `await_for`
  /// 0ms 返回 no_message),故不能用读超时当重试间隔,退避须用独立延时原语。
  std::shared_ptr<Coro::Awaitable<void>> close_signal_{
      std::make_shared<Coro::Awaitable<void>>()};

  std::shared_ptr<Coro::FiberTask<void>> pump_;        ///< WaitClosed join。
  std::shared_ptr<Coro::FiberTask<void>> write_pump_;  ///< 泵收尾时先 join。
};

}  // namespace transport
