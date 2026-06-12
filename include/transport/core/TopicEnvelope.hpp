#pragma once

// -----------------------------------------------------------------------------
// TopicEnvelope.hpp — topic 路由的 wire 封装(纯函数 + 流式装配器,header-only)
// UDP 报文体 / 流帧内层:[topic_len:2 BE][topic][body]
// TCP/串口流帧:[frame_len:4 BE][topic envelope] —— 路由模式框架自有分帧。
// codec 只编解码 body;本层只负责 topic 与分帧。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "transport/Result.hpp"

namespace transport {

struct TopicFrame {
  std::string topic;
  std::vector<uint8_t> body;
};

// [topic_len:2 BE][topic][body]
inline std::vector<uint8_t> PackTopic(const std::string& topic,
                                      const std::vector<uint8_t>& body) {
  std::vector<uint8_t> out;
  out.reserve(2 + topic.size() + body.size());
  const uint16_t n = static_cast<uint16_t>(topic.size());
  out.push_back(static_cast<uint8_t>((n >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(n & 0xFF));
  out.insert(out.end(), topic.begin(), topic.end());
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

// 解析 [topic_len:2 BE][topic][body];越界返回 Fail("frame: ...")
inline Result<TopicFrame> UnpackTopic(const uint8_t* data, size_t len) {
  if (len < 2)
    return Result<TopicFrame>::Fail("frame: topic envelope too short");
  const size_t n = (static_cast<size_t>(data[0]) << 8) | data[1];
  if (2 + n > len)
    return Result<TopicFrame>::Fail("frame: topic length exceeds envelope");
  TopicFrame f;
  f.topic.assign(reinterpret_cast<const char*>(data + 2), n);
  f.body.assign(data + 2 + n, data + len);
  return Result<TopicFrame>::Success(std::move(f));
}

// topic 是否能放进 2 字节长度字段(上限 65535)
inline bool TopicFitsEnvelope(const std::string& topic) {
  return topic.size() <= 0xFFFF;
}

// [frame_len:4 BE][PackTopic 输出]  —— frame_len 不含自身 4 字节
inline std::vector<uint8_t> FrameStream(const std::string& topic,
                                        const std::vector<uint8_t>& body) {
  auto env = PackTopic(topic, body);
  std::vector<uint8_t> out;
  out.reserve(4 + env.size());
  const uint32_t n = static_cast<uint32_t>(env.size());
  out.push_back(static_cast<uint8_t>((n >> 24) & 0xFF));
  out.push_back(static_cast<uint8_t>((n >> 16) & 0xFF));
  out.push_back(static_cast<uint8_t>((n >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(n & 0xFF));
  out.insert(out.end(), env.begin(), env.end());
  return out;
}

// 流式装配器:累积字节,按 frame_len 切出 (topic, body)。镜像 FrameAssembler。
class TopicFrameAssembler {
 public:
  static constexpr size_t kMaxFrame = 16 * 1024 * 1024;

  Result<std::vector<TopicFrame>> Feed(const uint8_t* data, size_t len) {
    std::vector<TopicFrame> out;
    buffer_.insert(buffer_.end(), data, data + len);
    size_t offset = 0;
    while (buffer_.size() - offset >= 4) {
      const uint8_t* p = buffer_.data() + offset;
      const size_t flen = (static_cast<size_t>(p[0]) << 24) |
                          (static_cast<size_t>(p[1]) << 16) |
                          (static_cast<size_t>(p[2]) << 8) |
                          static_cast<size_t>(p[3]);
      if (flen > kMaxFrame)
        return Result<std::vector<TopicFrame>>::Fail(
            "frame: frame length exceeds max");
      if (buffer_.size() - offset - 4 < flen) break;  // 不足一帧,等更多
      auto tf = UnpackTopic(p + 4, flen);
      if (!tf) return Result<std::vector<TopicFrame>>::Fail(tf.error);
      out.push_back(std::move(tf.value));
      offset += 4 + flen;
    }
    if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
    return Result<std::vector<TopicFrame>>::Success(std::move(out));
  }

 private:
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
