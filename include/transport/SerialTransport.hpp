#pragma once

/**
 * @file SerialTransport.hpp
 * @brief 协程原生串口传输——串口设备上的字节流收发与发送完成语义。
 */

#include <cstddef>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/ITransport.hpp"
#include "transport/serial/SerialConfig.hpp"

class QSerialPort;

namespace transport {

/**
 * @brief 协程原生串口传输——只覆盖已打开设备上的字节收发与发送完成语义。
 *
 * 基于 AsyncTask `coroiodevice`(QSerialPort 是 QIODevice)。语义与 TcpTransport
 * 对称:**字节流,一次一任意切片**(非报文,RT_IF_SERIAL);只暴露 I/O 事实,
 * 无连接状态、无自动重连(D3′)。串口设备在节点执行域 fiber 内于 Start() 时按
 * SerialConfig 打开并应用参数(波特率/数据位/停止位/校验)。
 *
 * **断开 = 过渡默认致命**(TBD-005):底层设备致命错误(如设备移除 ResourceError)
 * 使传输 `Closing→Closed`,**不自动重连**;复用靠宿主重建(ADR-0002 D3′)。
 */
class SerialTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 以配置构造;设备在 Start() 时打开并应用参数(须在节点执行域调用)。
  /// @param config 串口设备路径/波特率/数据位/停止位/校验。
  explicit SerialTransport(SerialConfig config);
  ~SerialTransport() override;

  SerialTransport(const SerialTransport&) = delete;
  SerialTransport& operator=(const SerialTransport&) = delete;

  /// @brief 打开设备、应用参数并进入 Running。
  /// @return 成功;打开失败 Connection、参数应用失败 Configuration、非法生命周期
  ///         InvalidState。
  Status Start() override;

  /// @brief 读一片已到达字节(拉模型,单一顺序读者;流式,一次一任意切片)。
  /// @param options 截止时间(cancellation 逐读 out-of-scope,同 TcpTransport)。
  /// @return 一片 Datagram(source 为单设备中立目的地);超时 Timeout、设备断开
  ///         Connection、我方关闭 Closed。
  Result<Datagram> Read(OperationOptions options = {}) override;

  /// @brief 发送一帧;仅在整帧字节全部离开框架用户态缓冲、进入设备发送缓冲后才
  ///        报告成功。并发写按到达顺序排队串行化(destination 忽略,单设备)。
  /// @param unit 待发送帧。
  /// @return 成功;部分写失败 Io/Connection 并关闭设备;关闭中 Closed。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):关设备、唤醒在途读写等待者。
  Status RequestClose() override;

  /// @brief 等待完全关闭(支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  // 发送侧可观测——I/O 事实,非"连接健康"裁决(判活留给协议层)。

  /// @brief 当前处于 Write 中的 fiber 数(排队 + 在写);反映背压积压。
  [[nodiscard]] std::size_t SendWaiterDepth() const;
  /// @brief 最近一次发送完成的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime() const;
  /// @brief 最近一次收到字节的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime() const;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
