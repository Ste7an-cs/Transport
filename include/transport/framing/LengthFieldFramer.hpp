#pragma once

#include <cstddef>
#include <cstdint>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"

namespace transport {

// 适配「固定长 header + header 内长度字段」协议。
struct LengthFieldFramerConfig {
  size_t header_size = 0;             // 固定 header 总长（字节）
  size_t length_offset = 0;           // 长度字段在 header 内的偏移
  size_t length_size = 4;             // 长度字段字节数（2 / 4 / 8）
  bool big_endian = true;             // 长度字段字节序
  bool length_includes_header = false;// 长度值是否已包含 header 本身
  size_t max_frame_size = 16 * 1024 * 1024;  // 帧长上限，超出报 frame: 错误
};

class LengthFieldFramer : public IFramer {
 public:
  // 配置校验（供 TransportFactory 在创建时调用）
  static Status ValidateConfig(const LengthFieldFramerConfig& config);

  explicit LengthFieldFramer(LengthFieldFramerConfig config);

  Result<FrameResult> TryExtract(const uint8_t* buf, size_t len) override;

 private:
  LengthFieldFramerConfig config_;
};

}  // namespace transport
