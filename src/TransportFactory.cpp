#include "transport/TransportFactory.hpp"

#include <utility>
#include <variant>

#include "transport/dds/DdsImpl.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"
#ifdef TRANSPORT_HAS_FASTDDS
#include "dds/FastDdsProvider.hpp"  // RegisterFastDdsProvider（src 内部头）
#endif

// TransportFactory.cpp — 统一创建入口（见 TransportFactory.hpp）。
// 类型化 Create = make_shared<对应 *Impl>；DDS 路径显式注册 FastDDS provider
//（静态库下匿名静态注册器可能被链接器裁剪，工厂是确定被引用的符号，在此根治）。

namespace transport {

std::shared_ptr<ITransport> TransportFactory::Create(
    const TcpClientConfig& config) {
  return std::make_shared<TcpClientImpl>(config);
}

std::shared_ptr<ITcpServer> TransportFactory::Create(
    const TcpServerConfig& config) {
  return std::make_shared<TcpServerImpl>(config);
}

std::shared_ptr<IUdpTransport> TransportFactory::Create(
    const UdpConfig& config) {
  return std::make_shared<UdpImpl>(config);
}

std::shared_ptr<IDdsTransport> TransportFactory::Create(
    const DdsConfig& config) {
#ifdef TRANSPORT_HAS_FASTDDS
  RegisterFastDdsProvider();  // 幂等
#endif
  return std::make_shared<DdsImpl>(config);
}

std::shared_ptr<ITransport> TransportFactory::Create(
    const SerialConfig& config) {
  return std::make_shared<SerialImpl>(config);
}

}  // namespace transport
