#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
constexpr uint32_t kHeaderBytes = 4;  // 两个 u16 长度前缀
// 预分配上限（payload 较大时由 PREALLOCATED_WITH_REALLOC 内存策略兜底）
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;

void WriteU16Le(uint8_t* p, uint16_t v) {
  p[0] = static_cast<uint8_t>(v & 0xFF);
  p[1] = static_cast<uint8_t>((v >> 8) & 0xFF);
}
uint16_t ReadU16Le(const uint8_t* p) {
  return static_cast<uint16_t>(p[0] | (p[1] << 8));
}
}  // namespace

FastDdsRawType::FastDdsRawType() {
  setName("RawMessage");
  m_typeSize = kPreallocSize;
  m_isGetKeyDefined = false;
}

bool FastDdsRawType::serialize(
    void* data, eprosima::fastrtps::rtps::SerializedPayload_t* payload) {
  auto* msg = static_cast<RawMessage*>(data);
  if (msg->request_id.size() > 0xFFFF || msg->reply_topic.size() > 0xFFFF)
    return false;
  const uint32_t total = kHeaderBytes +
                         static_cast<uint32_t>(msg->request_id.size()) +
                         static_cast<uint32_t>(msg->reply_topic.size()) +
                         static_cast<uint32_t>(msg->payload.size());
  if (total > payload->max_size) return false;

  uint8_t* p = payload->data;
  WriteU16Le(p, static_cast<uint16_t>(msg->request_id.size()));
  p += 2;
  std::memcpy(p, msg->request_id.data(), msg->request_id.size());
  p += msg->request_id.size();
  WriteU16Le(p, static_cast<uint16_t>(msg->reply_topic.size()));
  p += 2;
  std::memcpy(p, msg->reply_topic.data(), msg->reply_topic.size());
  p += msg->reply_topic.size();
  std::memcpy(p, msg->payload.data(), msg->payload.size());
  payload->length = total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastrtps::rtps::SerializedPayload_t* payload, void* data) {
  auto* msg = static_cast<RawMessage*>(data);
  const uint8_t* p = payload->data;
  uint32_t remaining = payload->length;

  if (remaining < 2) return false;
  uint16_t id_len = ReadU16Le(p);
  p += 2; remaining -= 2;
  if (remaining < id_len) return false;
  msg->request_id.assign(reinterpret_cast<const char*>(p), id_len);
  p += id_len; remaining -= id_len;

  if (remaining < 2) return false;
  uint16_t reply_len = ReadU16Le(p);
  p += 2; remaining -= 2;
  if (remaining < reply_len) return false;
  msg->reply_topic.assign(reinterpret_cast<const char*>(p), reply_len);
  p += reply_len; remaining -= reply_len;

  msg->payload.assign(p, p + remaining);
  return true;
}

std::function<uint32_t()> FastDdsRawType::getSerializedSizeProvider(
    void* data) {
  auto* msg = static_cast<RawMessage*>(data);
  const uint32_t total = kHeaderBytes +
                         static_cast<uint32_t>(msg->request_id.size()) +
                         static_cast<uint32_t>(msg->reply_topic.size()) +
                         static_cast<uint32_t>(msg->payload.size());
  return [total]() { return total; };
}

void* FastDdsRawType::createData() { return new RawMessage(); }

void FastDdsRawType::deleteData(void* data) {
  delete static_cast<RawMessage*>(data);
}

bool FastDdsRawType::getKey(void*, eprosima::fastrtps::rtps::InstanceHandle_t*,
                            bool) {
  return false;  // 无 key
}

}  // namespace transport
