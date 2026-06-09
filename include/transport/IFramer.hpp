#pragma once

#include <cstddef>
#include <cstdint>

#include "transport/Result.hpp"

namespace transport {

struct FrameResult {
  size_t consumed = 0;     // 本次消耗字节数；帧 = buf[0 .. consumed)
  bool has_frame = false;  // 是否切出一整帧
};

// 流式传输（TCP/串口）接收侧分帧：从滚动缓冲识别并切出一帧。
class IFramer {
 public:
  virtual ~IFramer() = default;

  // 返回值约定：
  //   ok == false                  -> frame: 错误（帧头非法/帧长越界），调用方应断开
  //   ok == true, has_frame == true -> 成功切出一帧 buf[0 .. consumed)
  //   ok == true, has_frame == false-> 数据不足，等待更多字节（consumed = 0）
  virtual Result<FrameResult> TryExtract(const uint8_t* buf, size_t len) = 0;
};

}  // namespace transport
