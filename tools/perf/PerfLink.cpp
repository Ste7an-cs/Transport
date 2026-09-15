#include "PerfLink.hpp"

#include <chrono>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

#include "transport/codec/DdsCodec.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/tcp/TcpConfig.hpp"
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/io/udp/UdpConfig.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "AcceptedTcpTransport.hpp"
#include "PerfWire.hpp"

namespace perf {
namespace {

using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsTransport;
using transport::Endpoint;
using transport::LinkState;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemCodec;
using transport::SystemDatagramCodec;
using transport::TcpConfig;
using transport::TcpTransport;
using transport::TransportErrc;
using transport::UdpConfig;
using transport::UdpTransport;
using transport::make_error_code;

/// 外部协议 id:两端一致即可,取一个不易与真实系统撞的值。
constexpr std::uint8_t kProtocolId = 0x5A;

}  // namespace

PerfLink::PerfLink(const PerfOptions& options) : options_(options) {}

PerfLink::~PerfLink() { Stop(); }

Coro::Result<void> PerfLink::Start() {
  const bool is_server = options_.role == Role::kServer;
  const std::chrono::milliseconds silence{options_.silence_ms};

  switch (options_.medium) {
    case MediumKind::kTcp: {
      if (is_server) {
        // 库里的 `TcpTransport` 只有客户端形态、`TcpServer` 本轮不做(ADR-0011 D10),
        // 故监听侧用工具自带的字节管道(见 AcceptedTcpTransport.hpp 的文件头)。
        byte_transport_ = std::make_unique<AcceptedTcpTransport>(
            options_.bind_host, options_.port);
      } else {
        TcpConfig config;
        config.host = options_.host;
        config.port = options_.port;
        config.silence_timeout = silence;
        byte_transport_ = std::make_unique<TcpTransport>(config);
      }
      break;
    }
    case MediumKind::kUdp: {
      UdpConfig config;
      config.mode = transport::UdpMode::kUnicast;
      config.silence_timeout = silence;
      if (is_server) {
        config.local_addr = options_.bind_host;
        config.local_port = options_.port;
        // 服务端不配默认对端:应答一律回带 `req.endpoint`(ADR-0021 D4)。
      } else {
        config.local_addr = "0.0.0.0";
        config.local_port = options_.local_port;
        config.remote_addr = options_.host;
        config.remote_port = options_.port;
      }
      byte_transport_ = std::make_unique<UdpTransport>(config);
      break;
    }
    case MediumKind::kDds: {
      DdsConfig config;
      config.domain_id = options_.domain;
      config.provider = "fastdds";
      dds_transport_ = std::make_unique<DdsTransport>(config);
      break;
    }
  }

  if (dds_transport_) {
    auto started = dds_transport_->Start();
    if (!started) {
      return started;
    }
    dds_node_ = std::make_unique<DdsNode>(*dds_transport_,
                                          std::make_unique<DdsCodec>());
    // 两侧**传一模一样的服务名**,各自按角色建各自那一侧(ADR-0013 D16)。
    const std::vector<std::string> services{kCtrlService, kDataService};
    Coro::Result<void> registered = Coro::Result<void>{};
    if (is_server) {
      registered = dds_node_->RegisterServices(services);
      if (registered) { registered = dds_node_->RegisterSubscribers({kDataTopic}); }
      if (registered) { registered = dds_node_->RegisterPublishers({kEchoTopic}); }
    } else {
      registered = dds_node_->RegisterClients(services);
      if (registered) { registered = dds_node_->RegisterPublishers({kDataTopic}); }
      if (registered) { registered = dds_node_->RegisterSubscribers({kEchoTopic}); }
    }
    if (!registered) {
      return registered;
    }
    return dds_node_->Start();
  }

  auto started = byte_transport_->Start();
  if (!started) {
    return started;
  }
  ProtocolNodeConfig node_config;
  node_config.protocol_id = kProtocolId;
  std::unique_ptr<transport::ICodec> codec;
  if (options_.medium == MediumKind::kUdp) {
    // 报文式介质用无状态报文版 codec:每次 Decode 只解这一个 datagram、零跨报文保留。
    codec = std::make_unique<SystemDatagramCodec>();
  } else {
    codec = std::make_unique<SystemCodec>();
  }
  protocol_node_ = std::make_unique<ProtocolNode>(*byte_transport_,
                                                  std::move(codec), node_config);
  return protocol_node_->Start();
}

void PerfLink::Stop() {
  if (stopped_) {
    return;
  }
  stopped_ = true;
  // 先收敛节点(它借用传输),再关传输。
  if (protocol_node_) {
    (void)protocol_node_->Close();
    protocol_node_->WaitClosed();
    protocol_node_.reset();
  }
  if (dds_node_) {
    (void)dds_node_->Close();
    dds_node_->WaitClosed();
    dds_node_.reset();
  }
  if (byte_transport_) {
    (void)byte_transport_->Close();
    byte_transport_->WaitClosed();
    byte_transport_.reset();
  }
  if (dds_transport_) {
    (void)dds_transport_->Close();
    dds_transport_->WaitClosed();
    dds_transport_.reset();
  }
}

Endpoint PerfLink::DataPeer() const {
  if (options_.medium == MediumKind::kUdp && options_.role == Role::kClient) {
    return Endpoint::Net(options_.host, options_.port);
  }
  return Endpoint::Default();
}

bool PerfLink::WaitLinkUp(int budget_ms) const {
  const transport::ITransport* transport = byte_transport_
                                               ? byte_transport_.get()
                                               : static_cast<const transport::ITransport*>(
                                                     dds_transport_.get());
  if (transport == nullptr) {
    return false;
  }
  for (int i = 0; i < budget_ms; ++i) {
    if (transport->CurrentLinkState() == LinkState::kUp) {
      return true;
    }
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
  }
  return transport->CurrentLinkState() == LinkState::kUp;
}

std::uint16_t PerfLink::LocalPort() const {
  if (options_.medium == MediumKind::kTcp && options_.role == Role::kServer) {
    const auto* accepted =
        static_cast<const AcceptedTcpTransport*>(byte_transport_.get());
    return accepted != nullptr ? accepted->LocalPort() : 0;
  }
  return options_.port;
}

}  // namespace perf
