#pragma once

/**
 * @file PerfLink.hpp
 * @brief 介质装配——**三种介质,一处装配**(ADR-0018 **D1** / **D5**)。
 *
 * 把"选一种介质 + 选一个角色"变成"一条已启动的传输 + 一个已启动的真节点"。两端都是
 * **真节点**(`ProtocolNode` / `DdsNode`),服务端只用公开面应答(ADR-0019 之后
 * `Send` 不再盖 `session_id`,服务端因此与客户端对称)。
 *
 * **介质维度是可扩展形状**(**D5**):将来接入串口,只需在这里补一个装配分支与其寻址
 * 参数——方法学、控制协议与报表都不必动。
 */

#include <memory>
#include <string>

#include "detail/result.hpp"

#include "transport/io/ITransport.hpp"

#include "PerfOptions.hpp"

namespace transport {
class DdsNode;
class DdsTransport;
class ProtocolNode;
}  // namespace transport

namespace perf {

/// @brief 一条按选项装配好的链路:传输 + 节点(**宿主**,负责二者的生命周期)。
///
/// 析构顺序是**先收敛节点、再关传输**——节点借用传输,其寿命须短于传输。
class PerfLink {
 public:
  explicit PerfLink(const PerfOptions& options);
  ~PerfLink();

  PerfLink(const PerfLink&) = delete;
  PerfLink& operator=(const PerfLink&) = delete;

  /// @brief 起传输 → (DDS:按角色注册四组端点) → 起节点。
  [[nodiscard]] Coro::Result<void> Start();

  /// @brief 收敛节点、关传输(幂等)。析构时亦会执行。
  void Stop();

  [[nodiscard]] bool IsDds() const { return options_.medium == MediumKind::kDds; }

  /// @brief `ProtocolNode` 句柄;仅 TCP / UDP 有效(DDS 上为 `nullptr`)。
  [[nodiscard]] transport::ProtocolNode* protocol_node() const {
    return protocol_node_.get();
  }
  /// @brief `DdsNode` 句柄;仅 DDS 有效(TCP / UDP 上为 `nullptr`)。
  [[nodiscard]] transport::DdsNode* dds_node() const { return dds_node_.get(); }

  /// @brief 数据帧的出站目的地(ADR-0021 **D1**)。
  ///
  /// UDP 上必须是 `Endpoint::Net(host, port)`——不填会发往传输配置的默认对端;TCP 上写泵
  /// 一律忽略它,填 `Default()` 即可。DDS 路径不经本方法(topic / 服务名各自填)。
  [[nodiscard]] transport::Endpoint DataPeer() const;

  /// @brief 轮询等到链路可用(`kUp`)或耗尽预算。**让出 fiber,不 park 线程**。
  [[nodiscard]] bool WaitLinkUp(int budget_ms) const;

  /// @brief 实际本地端口(TCP 服务端在 `--port 0` 时由 OS 分配);无则 0。
  [[nodiscard]] std::uint16_t LocalPort() const;

 private:
  PerfOptions options_;

  std::unique_ptr<transport::ITransport> byte_transport_;  ///< TCP / UDP。
  std::unique_ptr<transport::DdsTransport> dds_transport_; ///< DDS。
  std::unique_ptr<transport::ProtocolNode> protocol_node_;
  std::unique_ptr<transport::DdsNode> dds_node_;
  bool stopped_{false};
};

}  // namespace perf
