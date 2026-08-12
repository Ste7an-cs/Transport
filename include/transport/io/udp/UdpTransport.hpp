#pragma once

/**
 * @file UdpTransport.hpp
 * @brief 协程原生 UDP 传输——报文式收发,保留报文边界与发送方地址,非重连。
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
 * 基于 AsyncTask `coroudpsocket`:`Start()` 起**数据泵** fiber,反复取报文投入内部
 * `read_queue`(ADR-0007 D1);`Read()` 只**交出该队列的等待器句柄**,每个元素是一条
 * 完整报文、`source` 填发送方地址(from 可变);`Write(SendUnit)` 按 `destination`
 * (须为 `kNet`)发往不同地址,一次一报文。UDP 无连接:不构造伪连接状态,只暴露 I/O
 * 事实(最近收发时间戳、单操作错误,RT_NODE_006/ADR-0002 D3);底层致命 I/O 使生命
 * 周期 `Closing→Closed` 而非重连(ADR-0002 D3′),终止表现为 `read_queue` 被
 * `close(kClosed)`(ADR-0004 D1,表达经 ADR-0007 D4 改写)。socket 在调用 `Start()`
 * 的 fiber(节点执行域)内创建并绑定,守亲和纪律。
 *
 * 本轮只做读侧句柄化:外层 socket 管理循环(bind 失败无限重试、UDP 不自终)与
 * `write_queue` 见 ADR-0007 D2/D3,留后续票。
 */
// 与 TCP 的差异:UDP `writeDatagram` 是同步非阻塞的单报文原子发送——要么整报文
// 进入操作系统发送缓冲、要么失败,无短写/部分写、无背压刷缓冲循环、无写槽串行化。
// 报文过大 → kInvalidArgument;发送失败 → kIo。报文边界由内核保持,一次 Read
// 恰好一条报文(RT_IF_UDP)。
class UdpTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 以 UdpConfig 构造(尚未创建 socket);bind 在 Start() 内完成。
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  UdpTransport(const UdpTransport&) = delete;
  UdpTransport& operator=(const UdpTransport&) = delete;

  /// @brief 创建并绑定 QUdpSocket、建立接收流,进入 Running(须在节点执行域 fiber 内调用)。
  /// @return 成功;非法生命周期 InvalidState;bind 失败 Io。
  Status Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是一条完整报文,
  ///        `source` 填发送方地址(from 可变)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理,
  ///         传输层不设单读守卫。**传输终结**表现为队列被 `close(kClosed)`——UDP
  ///         不重连,故 socket 级致命 I/O 与我方关闭同以 kClosed 收敛,调用方
  ///         `await` 得到它后应停止读取(RT_TRANSPORT_008 / ADR-0004 D1 经 D4 改写);
  ///         底层成因经 LastError() 诊断。单次发送的可恢复失败不经本路径(见 Write)。
  ///         未 Start 时给出以 kInvalidState 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> Read() override;

  /// @brief 发一条报文;一次一完整报文。寻址:`kDefault` → 解析为 UdpConfig 默认目的地
  ///        (remote_addr/multicast_group + remote_port,让 ProtocolNode 等恒发 Default 的
  ///        传输无关调用方无缝跑在 UDP 上);`kNet` → 按 ip:port;其余(kTopic)非法。
  /// @param unit 待发送报文;无法解析的 destination / config 未配默认目的地 → kInvalidArgument。
  /// @return 成功;报文过大/地址非法 kInvalidArgument;发送失败 Io;关闭 Closed。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):关接收流、唤醒在途读者。
  Status RequestClose() override;

  /// @brief 等待完全关闭(支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  // 观测面——I/O 事实,非"连接健康"裁决(无连接介质判活留给协议层)。

  /// @brief 本地实际绑定端口(config 端口为 0 时由 OS 分配;未 Start 前为 0)。
  [[nodiscard]] std::uint16_t LocalPort() const;
  /// @brief 最近一次发送完成的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次收到报文的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):socket 已绑定 → `kUp`;未 Start、
  ///        bind 失败或已关闭 → `kDown`。UDP 无连接,故永不出现 `kEstablishing`。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
