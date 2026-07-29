#pragma once

#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

class ITransport {
 public:
  virtual ~ITransport() = default;
  virtual Status Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;
  virtual Status Write(SendUnit unit) = 0;
  virtual Status RequestClose() = 0;
  virtual Status WaitClosed(OperationOptions options = {}) = 0;
};

}  // namespace transport
