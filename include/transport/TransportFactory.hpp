#pragma once

// -----------------------------------------------------------------------------
// TransportFactory.hpp — 所有传输实例的统一创建入口
// 类型化 Create：构造不失败（配置校验在 Open()），返回最具体接口。
// CreateFromFile：JSON 配置文件 → 实例数组；解析/校验失败返回 config: 错误
//（含条目定位，如 "config: transports[2].port: ..."），任一条目失败整体失败。
// 本头零第三方类型；JSON 解析（nlohmann）封在 TransportFactory.cpp。
// -----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsTransport.hpp"
#include "transport/serial/SerialConfig.hpp"
#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpServerConfig.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class TransportFactory {
 public:
  // ---- 代码配置方式 ----
  static std::shared_ptr<ITransport> Create(const TcpClientConfig& config);
  static std::shared_ptr<ITcpServer> Create(const TcpServerConfig& config);
  static std::shared_ptr<ITransport> Create(const UdpConfig& config);
  static std::shared_ptr<IDdsTransport> Create(const DdsConfig& config);
  static std::shared_ptr<ITransport> Create(const SerialConfig& config);

  // ---- 配置文件方式（JSON）----
  static Result<std::vector<std::shared_ptr<ITransport>>> CreateFromFile(
      const std::string& path);
};

}  // namespace transport
