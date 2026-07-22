#pragma once

#include "transport/coro/Result.hpp"
#include "transport/coro/TransportTypes.hpp"

namespace transport::coro {

class ITransport {
 public:
  virtual ~ITransport() = default;
  virtual Status Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;
  virtual Status Write(SendUnit unit) = 0;
  virtual Status RequestClose() = 0;
  virtual Status WaitClosed(OperationOptions options = {}) = 0;
};

}  // namespace transport::coro
