#pragma once

/**
 * @file UdpTransport.hpp
 * @brief 协程原生 UDP 传输——报文式收发,保留报文边界与发送方地址,socket 管理泵形态。
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/io/ITransport.hpp"
#include "transport/io/udp/UdpConfig.hpp"

namespace transport {

/**
 * @brief 协程原生 UDP 传输——无连接、报文式字节管道(ITransport 实现)。
 *
 * **socket 管理泵 + 读写双队列**(ADR-0007 D1/D2/D3 的样板实现):`Start()` 起两条
 * fiber 后即返回——**泵**(外层管 socket 的 bind/重建/重试,内层把收到的报文投入
 * `read_queue`)与**写泵**(从 `write_queue` 取出并写 socket)。两条队列**不随 socket
 * 重建而更换**,故重建对调用方完全透明。
 *
 * - **不自终**(ADR-0007 D2):bind 失败或读流终止一律**等固定 3 秒重试、无限重试**,
 *   唯一的退出条件是我方 `Close`;底层致命 I/O 降为诊断事实留在 `LastError()`。
 * - `Read()` 只**交出 `read_queue` 的等待器句柄**(ADR-0007 D4),每个元素是一条完整
 *   报文、`source` 填发送方地址(from 可变);终止表现为队列被 `close(kClosed)`。
 * - `Write(SendUnit)` 只**入队即返**(ADR-0007 D3,fire-and-forget)。
 *
 * UDP 无连接:不构造伪连接状态,只暴露 I/O 事实(最近收发时间戳、最近错误,
 * RT_NODE_006/ADR-0002 D3)。socket 在调用 `Start()` 的 fiber(节点执行域)内创建,
 * 守亲和纪律;**整个生命期只一个 socket 对象**(bind→close→再 bind 复用)。
 *
 * 不在本类范围:静默超时(#156)、队列容量与丢弃归因(TBD-009 / #152,沿用 AsyncTask
 * 默认「有界 1024 + 静默丢最旧」)。
 */
// 与 TCP 的差异:UDP `writeDatagram` 是同步非阻塞的单报文原子发送——要么整报文
// 进入操作系统发送缓冲、要么失败,无短写/部分写、无背压刷缓冲循环。写泵是
// `write_queue` 的**单消费者**,写入串行化由此天然保证(RT_TRANSPORT_004 保留的那半);
// 且"状态检查 → 写出"之间无挂起点,故不需要代际号校验(该不变式只对 UDP 成立)。
// 报文边界由内核保持,一次 Read 恰好一条报文(RT_IF_UDP)。
class UdpTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 以 UdpConfig 构造(尚未创建 socket);socket 在 Start() 内创建。
  explicit UdpTransport(UdpConfig config);
  /// @brief 析构:请求关闭、join 泵(泵内部已先 join 写泵),再销毁 socket。
  ~UdpTransport() override;

  UdpTransport(const UdpTransport&) = delete;
  UdpTransport& operator=(const UdpTransport&) = delete;

  /// @brief 创建 QUdpSocket、起泵与写泵,进入 Running(须在节点执行域 fiber 内调用)。
  ///
  /// 首次 bind 就地尝试一次(故 `LocalPort()`/`CurrentLinkState()` 返回后即可观测),
  /// **但 bind 失败不算启动失败**(ADR-0007 D2):泵会等固定 3 秒后无限重试,直至我方
  /// `Close`。已 Running 时重复调用为成功 no-op。
  /// @return 成功;非法生命周期(关闭中/已关闭)InvalidState。
  Status Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是一条完整报文,
  ///        `source` 填发送方地址(from 可变)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理,
  ///         传输层不设单读守卫。**传输终结**表现为队列被 `close(kClosed)`,而
  ///         **只有我方 `Close` 才终止**——bind 失败与 socket 级致命 I/O 均由泵内部
  ///         无限重试消化,不向调用方暴露(ADR-0007 D2),底层成因经 `LastError()` 诊断。
  ///         未 Start 时给出以 kInvalidState 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> Read() override;

  /// @brief 把一条完整报文投入 `write_queue` 即返(ADR-0007 D3,fire-and-forget)。
  ///
  /// **不等待实际发出**,返回值仅表示"已入队";发送失败不作为返回值,只进 `LastError()`。
  /// 链路不可用(bind 重试中)时报文留在队列等待恢复,不拒绝、不丢弃,恢复后按序全部
  /// 发出(接受对端可能收到过期数据);我方 `Close` 时未发出的残留随队列关闭而丢弃。
  /// 寻址:`kDefault` → 解析为 UdpConfig 默认目的地(remote_addr/multicast_group +
  /// remote_port,让 ProtocolNode 等恒发 Default 的传输无关调用方无缝跑在 UDP 上);
  /// `kNet` → 按 ip:port;其余(kTopic)非法。**调用方参数**的合法性仍即时作答。
  /// @param unit 待发送报文;无法解析的 destination / config 未配默认目的地 → kInvalidArgument。
  /// @return 已入队;未 Start kInvalidState;关闭中/已关闭 kClosed;
  ///         destination 非法 kInvalidArgument。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等,只发信号不等收敛):打断 bind 退避与活跃读流,唤醒写泵的
  ///        两个阻塞点,随后由泵跑完收敛(关 `read_queue`、落 Closed、完成 `WaitClosed`)。
  Status RequestClose() override;

  /// @brief 等待完全关闭(支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  // 观测面——I/O 事实,非"连接健康"裁决(无连接介质判活留给协议层)。

  /// @brief 本地实际绑定端口(config 端口为 0 时由 OS 分配;未 Start / 尚未 bind 上
  ///        则为 0)。注意 config 端口为 0 时每次重 bind 会拿到**不同**的临时端口。
  [[nodiscard]] std::uint16_t LocalPort() const;
  /// @brief 最近一次发送完成的时刻(尚无则空);由写泵在实际发出后记账。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次收到报文的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):socket 此刻已绑定 → `kUp`;未 Start、
  ///        bind 重试期间或已关闭 → `kDown`。UDP 无连接,故永不出现 `kEstablishing`
  ///        (bind 退避属连接管理策略,不经本查询暴露)。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
