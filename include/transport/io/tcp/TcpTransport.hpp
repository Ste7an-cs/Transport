#pragma once

/**
 * @file TcpTransport.hpp
 * @brief 协程原生 TCP 传输——已建立连接上的字节收发与发送完成语义。
 */

#include <cstddef>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/io/ITransport.hpp"

class QAbstractSocket;

namespace transport {

/**
 * @brief 协程原生 TCP 传输——只覆盖已建立连接上的字节收发与发送完成语义。
 *
 * 连接建立、自动重连、运行时重配置不在本类职责(见连接管理 spec);构造时接管一个
 * 已连接的 QAbstractSocket,客户端与服务端已接受连接共享本实现,不向用户暴露
 * corosocket。发送路径遵守 ITransport 契约的可观察发送完成语义。
 *
 * 读侧为泵形态(ADR-0007 D1/D4):`Start()` 起数据泵 fiber 把 readAll 流的每片字节转成
 * `Datagram` 投入内部 `read_queue`,`Read()` 只交出该队列句柄。写侧仍是同步背压语义
 * (`write_queue` 见 ADR-0007 D3,留后续票)。
 */
// 发送路径可观察语义细则:
//   * RT_TRANSPORT_008 一次发送在帧字节全部离开框架用户态发送缓冲、进入操作系统
//     发送缓冲后才报告成功;背压经协程 await 自然传导回发起方,不 fire-and-forget。
//   * RT_TRANSPORT_007 同一 fiber 的先后发送按程序序上线;跨 fiber 并发发送被
//     串行化为一致全序(按到达节点执行域的顺序)。
//   * RT_TRANSPORT_004 并发写按到达顺序排队串行化(不拒绝),同一时刻至多一个在写
//     帧,单帧字节不与另一帧交错。
//   * RT_TRANSPORT_004.4 流式部分写失败返回 Io/Connection 并关闭本物理连接,
//     不自动重发残缺帧。
//   * RT_REQUEST_004.4 写入已开始后被取消/超时,底层尽力把帧写完(健康连接不
//     截断);取消/超时的本地返回码由发起方(请求层)裁决,不由本类截断连接。
class TcpTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 接管一个已建立连接的 socket(须与调用方处于同一执行域/线程)。
  /// @param connected_socket 已连接的 QAbstractSocket,生命周期由本类接管。
  explicit TcpTransport(QAbstractSocket* connected_socket);
  ~TcpTransport() override;

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;

  /// @brief 进入 Running;须已持有已连接 socket。
  /// @return 成功;非法生命周期或无 socket 返回 InvalidState。
  Coro::Result<void> Start() override;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4);每个元素是一片已到达字节
  ///        (流式,一次一任意切片)。
  /// @return `read_queue` 句柄:deadline/取消/是否 `shared()` 扇出由调用方自理,
  ///         传输层不设单读守卫。**传输终结**表现为队列被 `close(kClosed)`——本类
  ///         不重连,故对端断开/底层致命错误与我方关闭同以 kClosed 收敛,调用方
  ///         `await` 得到它后应停止读取(RT_TRANSPORT_008 / ADR-0004 D1 经 D4 改写);
  ///         底层成因经 LastError() 诊断。未 Start 时给出以 kInvalidState 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> Read() override;

  /// @brief 发送一帧;仅在帧字节全部离开框架用户态缓冲、进入操作系统发送缓冲后
  ///        才报告成功(RT_TRANSPORT_008)。并发写按到达顺序排队串行化。
  /// @param unit 待发送帧(目的地址在已连接 TCP 上被忽略)。
  /// @return 成功;部分写失败 Io/Connection 并关闭本连接;关闭中 Closed。
  Coro::Result<void> Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):撕连接、唤醒在途读写等待者。
  Coro::Result<void> RequestClose() override;

  /// @brief 等待完全关闭(支持多等待者)。
  /// @param options 仅 `deadline` 生效;取消令牌已随共享完成量轻量化移除(ADR-0006 D3)。
  Coro::Result<void> WaitClosed(OperationOptions options = {}) override;

  // 发送侧可观测——I/O 事实,非"连接健康"裁决(判活留给协议层)。

  /// @brief 当前处于 Write 中的 fiber 数(排队 + 在写);并发时可 >1,反映背压积压。
  [[nodiscard]] std::size_t SendWaiterDepth() const;
  /// @brief 最近一次发送完成的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次收到字节的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):已接管连接存续 → `kUp`;未 Start、
  ///        对端断开或已关闭 → `kDown`。本类不管理连接,故永不出现 `kEstablishing`。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
