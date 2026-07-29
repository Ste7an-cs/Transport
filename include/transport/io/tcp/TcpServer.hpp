#pragma once

/**
 * @file TcpServer.hpp
 * @brief 协程原生 TCP 服务端 accept 层——每接受一条连接派生一个 ProtocolNode
 *        (ADR-0003 D12 Q6 / RT_DESIGN_004 / RT_IF_TCP)。
 */

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/io/ITransport.hpp"
#include "transport/io/tcp/TcpServerConfig.hpp"

namespace transport {

class ProtocolNode;

/**
 * @brief TCP 服务端 accept 层:包 AsyncTask `corotcpserver.nextConnection()` 生成器,
 *        每接受一条已连接 socket 派生一个 `ProtocolNode`(内层 `TcpTransport` 直接接管
 *        已接受 socket,**非重连** D3′,连接生命=节点生命 RT_DESIGN_004)。
 *
 * server owns 已接受子 node 列表:`Start()` 监听并 spawn 一条 accept 循环 fiber;每得一条
 * 连接经**应用提供的 node 工厂**装配 codec/handler/config 造 node,`Start` 后记入子 node
 * 列表,并为其 spawn 一条 supervisor fiber 监听该连接断开。**服务端连接 node = 非重连**
 * (D3′):对端断开 → supervisor 驱动该 node `Closing→Closed` 并从列表注销(连接生命=节点
 * 生命)。`Close()` → 停 accept + 逐个关闭全部子 node + 回收。监听/接受失败/连接关闭可观测
 * (RT_IF_TCP)。
 *
 * 不可拷贝、不可移动(accept 循环 / supervisor fiber 捕获共享状态)。
 */
class TcpServer {
 public:
  using Clock = OperationOptions::Clock;

  /**
   * @brief 每连接 node 工厂(RT_HANDLER_001 装配点):server 交出一个已接管已接受 socket 的
   *        内层 `TcpTransport`(以 `ITransport` 形态),应用装配 codec/handler/config 造
   *        `ProtocolNode` 返回。返回 nullptr = 拒绝该连接(server 丢弃 socket)。
   *
   * server 负责对返回的 node 调 `Start`、记入子 node 列表并管理其生命周期;工厂只装配、
   * 不启动。
   */
  using NodeFactory =
      std::function<std::unique_ptr<ProtocolNode>(std::unique_ptr<ITransport>)>;

  /// @brief 用监听配置 + 每连接 node 工厂构造(尚未监听;须 Start)。
  TcpServer(TcpServerConfig config, NodeFactory factory);
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  /**
   * @brief 监听 `config.bind_addr:port` 并 spawn accept 循环 fiber。
   * @return 成功;监听失败(端口占用/地址非法)返 kConnection(见 LastError),停在 Created
   *         可改配重试;非法生命周期返 kInvalidState。
   */
  Status Start();

  /**
   * @brief 请求关闭(幂等):停 accept(停监听)+ 逐个关闭全部子 node 并等其收敛 + 回收。
   * @return 全部子 node 收敛后成功。
   */
  Status Close();

  /// @brief 等待 server 完全关闭(accept 循环退出 + 全部子 node 收敛;支持多等待者)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {});

  /// @brief 当前是否在监听。
  [[nodiscard]] bool IsListening() const;

  /// @brief 实际监听端口(config.port=0 时由 OS 分配后经此取回);未监听为 0。
  [[nodiscard]] std::uint16_t LocalPort() const;

  /// @brief 最近一次监听/接受失败的错误类别(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const;

  /// @brief 累计接受(并成功派生 node)的连接数。
  [[nodiscard]] std::size_t AcceptedCount() const;

  /// @brief 当前活跃子 node 数(已接受未注销)。
  [[nodiscard]] std::size_t ActiveNodeCount() const;

  /// @brief 累计因对端断开而自终注销的子 node 数(连接生命=节点生命,D3′)。
  [[nodiscard]] std::size_t ClosedConnectionCount() const;

  /// @brief 累计接受失败/派生失败次数(RT_IF_TCP:acceptError / node 启动失败)。
  [[nodiscard]] std::size_t AcceptErrorCount() const;

  struct State;  // 不透明:定义在 .cpp,accept 循环 / supervisor fiber 与本类共享。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
