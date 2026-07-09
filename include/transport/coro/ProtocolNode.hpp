#pragma once

// coro::ProtocolNode — 协程 ProtocolNode 薄壳。持 ITransport + coro::InteractionEngine(ProtocolPolicy)。
// Request 线性:构一条命令 → engine.Request(kCommand)。活在 fiber 调度线程。

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/coro/InteractionEngine.hpp"

namespace transport {
namespace coro {

class ProtocolNode {
 public:
  ProtocolNode(std::shared_ptr<ITransport> transport, uint8_t protocol_id,
               bool reply_to_source = false);

  Status Start();  // transport->Open() + engine.Open()
  void   Stop();   // engine.Close() + transport->Close()

  // Request:发命令码 cmd + payload,等终结应答(或 timeout:/conn:)。须在 fiber 内调。
  Result<Message> Request(uint16_t cmd, std::vector<uint8_t> payload,
                          std::chrono::milliseconds timeout);

 private:
  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<InteractionEngine> engine_;
};

}  // namespace coro
}  // namespace transport
