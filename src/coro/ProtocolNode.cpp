#include "transport/coro/ProtocolNode.hpp"

#include <utility>

#include "transport/Message.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"

namespace transport {
namespace coro {

ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, uint8_t protocol_id,
                           bool reply_to_source)
    : transport_(transport) {
  engine_ = std::make_unique<InteractionEngine>(
      transport, std::make_unique<SystemDatagramCodec>(),
      std::make_unique<ProtocolPolicy>(protocol_id, reply_to_source));
}

Status ProtocolNode::Start() {
  auto st = transport_->Open();
  if (!st) return st;
  return engine_->Open();
}

void ProtocolNode::Stop() {
  engine_->Close();
  transport_->Close();
}

Result<Message> ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload,
                                      std::chrono::milliseconds timeout) {
  Message m; m.message_id = cmd; m.payload = std::move(payload);
  return engine_->Request(m, static_cast<FrameTag>(FrameType::kCommand), timeout);
}

}  // namespace coro
}  // namespace transport
