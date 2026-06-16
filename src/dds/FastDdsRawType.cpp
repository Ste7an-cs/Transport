#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;
}  // namespace

FastDdsRawType::FastDdsRawType() {
  setName("RawBytes");
  m_typeSize = kPreallocSize;
  m_isGetKeyDefined = false;
}

bool FastDdsRawType::serialize(
    void* data, eprosima::fastrtps::rtps::SerializedPayload_t* payload) {
  auto* msg = static_cast<RawBytes*>(data);
  const uint32_t total = static_cast<uint32_t>(msg->payload.size());
  if (total > payload->max_size) return false;
  if (total > 0) std::memcpy(payload->data, msg->payload.data(), total);
  payload->length = total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastrtps::rtps::SerializedPayload_t* payload, void* data) {
  auto* msg = static_cast<RawBytes*>(data);
  msg->payload.assign(payload->data, payload->data + payload->length);
  return true;
}

std::function<uint32_t()> FastDdsRawType::getSerializedSizeProvider(void* data) {
  auto* msg = static_cast<RawBytes*>(data);
  const uint32_t total = static_cast<uint32_t>(msg->payload.size());
  return [total]() { return total; };
}

void* FastDdsRawType::createData() { return new RawBytes(); }
void FastDdsRawType::deleteData(void* data) { delete static_cast<RawBytes*>(data); }

bool FastDdsRawType::getKey(void*, eprosima::fastrtps::rtps::InstanceHandle_t*, bool) {
  return false;
}

}  // namespace transport
