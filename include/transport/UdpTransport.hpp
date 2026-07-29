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

#include "transport/ITransport.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

/**
 * @brief 协程原生 UDP 传输——无连接、报文式字节管道(ITransport 实现)。
 *
 * 基于 AsyncTask `coroudpsocket`:`Read()` 收一条完整报文,`source` 填发送方地址
 * (from 可变);`Write(SendUnit)` 按 `destination`(须为 `kNet`)发往不同地址,
 * 一次一报文。UDP 无连接:不构造伪连接状态,只暴露 I/O 事实(最近收发时间戳、
 * 单操作错误,RT_NODE_006/ADR-0002 D3);底层致命 I/O 使生命周期 `Closing→Closed`
 * 而非重连(ADR-0002 D3′)。socket 在调用 `Start()` 的 fiber(节点执行域)内创建
 * 并绑定,守亲和纪律。
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

  /// @brief 收一条完整报文(拉模型,单一顺序读者);source 填发送方地址(from 可变)。
  /// @param options 截止时间(取消见 TCP 读侧同理:持久单流不逐读取消)。
  /// @return 一条 Datagram;超时 Timeout、底层致命 Io/Connection、关闭 Closed。
  Result<Datagram> Read(OperationOptions options = {}) override;

  /// @brief 发一条报文到 `unit.destination`(须为 kNet);一次一完整报文。
  /// @param unit 待发送报文;destination 非 kNet → kInvalidArgument。
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
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime() const;
  /// @brief 最近一次收到报文的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime() const;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
