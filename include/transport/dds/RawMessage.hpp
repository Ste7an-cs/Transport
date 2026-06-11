#pragma once

// -----------------------------------------------------------------------------
// RawMessage.hpp — DDS 承载类（普通 C++ 类，非 IDL；provider 无关）
// pub-sub 与 req-resp 共用：request_id/reply_topic 是框架级关联信息（pub-sub 为
// 空），payload 是 ICodec.Encode 输出的原始字节。wire layout 见主 spec §7.2。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

class RawMessage {
 public:
  std::string request_id;   // req-resp 关联 id；pub-sub 为空
  std::string reply_topic;  // req-resp 回包 topic；pub-sub 为空
  std::vector<uint8_t> payload;
};

}  // namespace transport
