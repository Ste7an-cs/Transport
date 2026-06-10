#pragma once

// -----------------------------------------------------------------------------
// ITcpServer.hpp — TCP 服务端扩展接口（ITransport + 连接管理）
// 在 ITransport 基础上加 OnNewConnection（每客户端独立 ITransport）/GetClients/
// DisconnectClient；继承的 Send=广播、Receive 不适用。实现见 TcpServerImpl。
// -----------------------------------------------------------------------------

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"

namespace transport {

// TCP 服务端专属扩展。每个客户端连接通过 OnNewConnection 回调获得独立 client_transport。
class ITcpServer : public ITransport {
 public:
  using ConnectionCallback =
      std::function<void(std::shared_ptr<ITransport> client_transport)>;

  // 新客户端连接时触发；client_transport 是该连接的完整 ITransport 实例
  virtual void OnNewConnection(ConnectionCallback cb) = 0;

  // 返回当前所有已连接客户端的 transport 快照
  virtual std::vector<std::shared_ptr<ITransport>> GetClients() const = 0;

  // 根据 client_id（"ip:port"）主动断开指定客户端
  virtual void DisconnectClient(const std::string& client_id) = 0;
};

}  // namespace transport
