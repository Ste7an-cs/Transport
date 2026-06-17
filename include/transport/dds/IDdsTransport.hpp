#pragma once

// IDdsTransport.hpp — 在纯管道 ITransport 上加 DDS 的订阅能力(pub-sub)。

#include <string>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

namespace transport {

class IDdsTransport : public ITransport {
 public:
  virtual Status Subscribe(const std::string& topic) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;
};

}  // namespace transport
