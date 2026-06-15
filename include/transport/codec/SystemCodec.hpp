#pragma once

// SystemCodec.hpp — 默认线缆格式,承载完整交互元数据。
// [frame_len:4 BE][kind:1][corr_len:2 BE][corr_id][topic_len:2 BE][topic][payload]
// frame_len = 其后全部字节数(不含自身 4 字节)。流式按 frame_len 切帧,报文式整段一条。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

class SystemCodec : public ICodec {
 public:
  static constexpr std::size_t kMaxFrame = 16 * 1024 * 1024;

  Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
