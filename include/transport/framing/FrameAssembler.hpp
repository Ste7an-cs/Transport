#pragma once

// -----------------------------------------------------------------------------
// FrameAssembler.hpp — 接收侧分帧装配器（header-only）
// 把字节追加进滚动缓冲，用持有的 IFramer 循环切出完整帧；framer 为 nullptr 时
// 透传（每次 Feed 的数据原样作为一帧）。流式传输(TcpConnectionImpl)接收侧使用。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"

namespace transport {

// 接收侧装配器：把字节追加进滚动缓冲，用 IFramer 循环切出完整帧。
// framer 为 nullptr 时进入透传模式：每次 Feed 的数据原样作为一帧返回。
class FrameAssembler {
 public:
  using Frames = std::vector<std::vector<uint8_t>>;

  explicit FrameAssembler(std::shared_ptr<IFramer> framer)
      : framer_(std::move(framer)) {}

  // 返回本次喂入后切出的所有完整帧；遇 frame: 错误返回 Fail（调用方应断开）。
  Result<Frames> Feed(const uint8_t* data, size_t len) {
    Frames frames;

    if (!framer_) {  // 透传模式
      if (len > 0) frames.emplace_back(data, data + len);
      return Result<Frames>::Success(std::move(frames));
    }

    buffer_.insert(buffer_.end(), data, data + len);
    size_t offset = 0;
    while (offset < buffer_.size()) {
      auto r = framer_->TryExtract(buffer_.data() + offset, buffer_.size() - offset);
      if (!r) {
        return Result<Frames>::Fail(r.error);
      }
      if (!r.value.has_frame) {
        break;
      }
      const size_t consumed = r.value.consumed;
      frames.emplace_back(buffer_.begin() + offset,
                          buffer_.begin() + offset + consumed);
      offset += consumed;
    }
    if (offset > 0) {
      buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
    }
    return Result<Frames>::Success(std::move(frames));
  }

 private:
  std::shared_ptr<IFramer> framer_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
