#pragma once

// DdsNode.hpp — DDS 节点:DdsNode : CommNode,复用交互引擎,加 DDS 订阅能力 +
// 基于 inbox topic 的应答路由。发布=Send(Endpoint::Topic);请求=Request(...,Endpoint::Topic)。
// 须以 shared_ptr 持有。

#include <cstddef>
#include <memory>
#include <string>

#include "transport/Result.hpp"
#include "transport/comm/CommNode.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/dds/IDdsTransport.hpp"

namespace transport {

class DdsNode : public CommNode {
 public:
  DdsNode(std::shared_ptr<IDdsTransport> transport,
          std::string inbox_topic,
          std::unique_ptr<ICodec> codec = nullptr,        // null → DdsCodec
          std::unique_ptr<IExecutor> executor = nullptr,  // null → ThreadExecutor
          std::size_t queue_capacity = 1024);

  Status Open() override;                         // CommNode::Open + 订阅 inbox + 设应答地址
  Status Subscribe(const std::string& topic);     // DDS 独有能力
  Status Unsubscribe(const std::string& topic);

 private:
  std::shared_ptr<IDdsTransport> dds_;
  std::string inbox_topic_;
};

}  // namespace transport
