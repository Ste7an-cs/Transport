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

Result<Message> InteractionEngine::Request(Message out, FrameTag tag,
                                           std::chrono::milliseconds timeout,
                                           const Endpoint& to) {
  using R = Result<Message>;
  policy_->NewCorrelation(out);                  // 仍滚 session_id、盖 protocol_id(副作用)
  policy_->SetTag(out, tag);
  Key k = key_fn_(out);                           // 挂起键与入站键同源(默认=policy.KeyOf;可被 SetKeyFn 覆盖)
  Coro::Awaitable<Message> aw;
  auto ch = aw.channel();
  pending_[k] = ch;                              // 登记挂起(单线程,无锁)

  auto enc = codec_->Encode(out);
  if (!enc) { pending_.erase(k); return R::Fail(enc.error); }
  auto st = transport_->Send(enc.value, to);
  if (!st) { pending_.erase(k); return R::Fail(st.error); }

  auto r = aw.await_for(timeout);               // 仅挂起当前 fiber
  pending_.erase(k);
  if (r) return R::Success(r.value());
  // channel 被 close(Close/断连)→ conn:;否则真超时 → timeout:
  if (ch->is_closed()) return R::Fail("conn: engine closed or disconnected");
  return R::Fail("timeout: request timed out");
}

void InteractionEngine::onBytes(Result<std::vector<uint8_t>> r, const std::string& from) {
  if (!r) return;                                // 传输层错误:本核心丢弃
  auto msgs = codec_->Decode(r.value.data(), r.value.size());
  if (!msgs) return;
  for (auto& m : msgs.value) {
    if (m.source.empty()) m.source = from;       // 缺省来源
    Key k = key_fn_(m);
    auto it = pending_.find(k);
    if (it != pending_.end()) {
      if (policy_->IsTerminal(m.frm_type))
        it->second->push(m);                     // 唤醒请求 fiber(它会摘挂起)
      // 中间(非终结)帧:本期丢弃
      continue;
    }
    switch (policy_->RouteUnmatched(m)) {        // 无主帧
      case InteractionPolicy::Route::kDeliver:
        if (on_deliver_) on_deliver_(m);
        break;
      case InteractionPolicy::Route::kInboundRequest:  // 服务端(下期)——本期丢弃
      case InteractionPolicy::Route::kDrop:
        break;
    }
  }
}

void InteractionEngine::onDisconnect(const std::string&) {
  for (auto& kv : pending_) kv.second->close();  // 断连:唤醒挂起 → Request 返回 conn:
  pending_.clear();
}

}  // namespace coro
}  // namespace transport
