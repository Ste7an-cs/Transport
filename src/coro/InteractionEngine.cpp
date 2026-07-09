#include "transport/coro/InteractionEngine.hpp"

#include <utility>
#include <variant>

namespace transport {
namespace coro {

InteractionEngine::InteractionEngine(std::shared_ptr<ITransport> transport,
                                     std::unique_ptr<ICodec> codec,
                                     std::unique_ptr<InteractionPolicy> policy)
    : transport_(std::move(transport)), codec_(std::move(codec)), policy_(std::move(policy)) {
  key_fn_ = [this](const Message& m) { return policy_->KeyOf(m); };  // 默认键
}

InteractionEngine::~InteractionEngine() { Close(); }

Status InteractionEngine::Open() {
  transport_->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& from) {
    onBytes(std::move(r), from);
  });
  transport_->OnDisconnect([this](const std::string& reason) { onDisconnect(reason); });
  closing_ = false;
  return Status::Success(std::monostate{});
}

void InteractionEngine::Close() {
  if (closing_) return;
  closing_ = true;
  for (auto& kv : pending_) kv.second->close();  // 唤醒等待者(await_for 返回错误 → Request 判 conn:)
  pending_.clear();
}

Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  auto enc = codec_->Encode(out);
  if (!enc) return Status::Fail(enc.error);
  return transport_->Send(enc.value, to);
}

// Request / onBytes / onDisconnect 在 Task 3 实现(本任务先留空存根以便链接)。
Result<Message> InteractionEngine::Request(Message, FrameTag, std::chrono::milliseconds,
                                           const Endpoint&) {
  return Result<Message>::Fail("config: not implemented");  // Task 3 覆盖
}
void InteractionEngine::onBytes(Result<std::vector<uint8_t>>, const std::string&) {}
void InteractionEngine::onDisconnect(const std::string&) {}

}  // namespace coro
}  // namespace transport
