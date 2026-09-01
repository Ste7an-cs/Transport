#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
// 仅是 history 预分配的**提示值**(is_bounded() 为 false,真正的每样本大小由
// calculate_serialized_size 现算)。沿用 2.13 版本的取值,不改变既有内存足迹。
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;
}  // namespace

FastDdsRawType::FastDdsRawType() {
  set_name("RawBytes");
  max_serialized_type_size = kPreallocSize;
  is_compute_key_provided = false;
}

bool FastDdsRawType::serialize(
    const void* const data,
    eprosima::fastdds::rtps::SerializedPayload_t& payload,
    eprosima::fastdds::dds::DataRepresentationId_t /*data_representation*/) {
  // 无 CDR 封装:字节原样落进 payload,故 data_representation 对本类型无意义。
  const auto* msg = static_cast<const RawBytes*>(data);
  const auto total = static_cast<uint32_t>(msg->payload.size());
  if (total > payload.max_size) return false;
  if (total > 0) std::memcpy(payload.data, msg->payload.data(), total);
  payload.length = total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastdds::rtps::SerializedPayload_t& payload, void* data) {
  auto* msg = static_cast<RawBytes*>(data);
  msg->payload.assign(payload.data, payload.data + payload.length);
  return true;
}

uint32_t FastDdsRawType::calculate_serialized_size(
    const void* const data,
    eprosima::fastdds::dds::DataRepresentationId_t /*data_representation*/) {
  const auto* msg = static_cast<const RawBytes*>(data);
  return static_cast<uint32_t>(msg->payload.size());
}

void* FastDdsRawType::create_data() { return new RawBytes(); }
void FastDdsRawType::delete_data(void* data) { delete static_cast<RawBytes*>(data); }

bool FastDdsRawType::compute_key(eprosima::fastdds::rtps::SerializedPayload_t&,
                                 eprosima::fastdds::rtps::InstanceHandle_t&, bool) {
  return false;  // 无键类型
}

bool FastDdsRawType::compute_key(const void* const,
                                 eprosima::fastdds::rtps::InstanceHandle_t&, bool) {
  return false;  // 无键类型
}

}  // namespace transport
