#pragma once

// -----------------------------------------------------------------------------
// StreamSend.hpp — 流式传输(TCP/串口)发送侧的「待写字节」决策(header-only)
// 把 routing 分支 + topic 守卫 + 按 topic 编码 + FrameStream 这段两类流式传输完全
// 相同的逻辑抽出来集中;不含 open 检查与入队 —— 那些与具体 socket / 错误串绑定,
// 留在各传输自身。UDP(报文 PackTopic+寻址)与 DDS(原生 topic)不走本路径。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

#include "transport/Result.hpp"
#include "transport/core/TopicEnvelope.hpp"
#include "transport/core/TransportCore.hpp"

namespace transport {

// 决定一次流式发送要写出的字节:
//   !routing && topic 非空 → Fail("config: topic routing not enabled")
//   !routing && topic 空   → core.EncodeForSend(payload)           (旧 plain 路径)
//   routing  && topic 过长 → Fail("frame: topic too long")
//   routing                → FrameStream(topic, EncodeForSend(payload, topic))
inline Result<std::vector<uint8_t>> BuildStreamFrame(
    TransportCore& core, bool routing, const std::vector<uint8_t>& payload,
    const std::string& topic) {
  using R = Result<std::vector<uint8_t>>;
  if (!routing) {
    if (!topic.empty()) return R::Fail("config: topic routing not enabled");
    return core.EncodeForSend(payload);
  }
  if (!TopicFitsEnvelope(topic)) return R::Fail("frame: topic too long");
  auto enc = core.EncodeForSend(payload, topic);
  if (!enc) return R::Fail(enc.error);
  return R::Success(FrameStream(topic, enc.value));
}

}  // namespace transport
