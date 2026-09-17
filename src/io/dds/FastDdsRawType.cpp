#include "FastDdsRawType.hpp"

#include <cstring>

namespace transport {

namespace {
// 仅是 history 预分配的**提示值**(is_bounded() 为 false,真正的每样本大小由
// calculate_serialized_size 现算)。沿用 2.13 版本的取值,不改变既有内存足迹;
// 长度前缀那 4 字节另加在上面,预留量才与 calculate_serialized_size 对得上。
constexpr uint32_t kPreallocSize = 64 * 1024 + 512;

/// 大端写入 32 位长度(与 `DdsCodec` 的 `corr_len:2 BE` / `reply_len:2 BE` 同一字节序)。
void WriteBe32(uint8_t* dst, uint32_t value) {
  dst[0] = static_cast<uint8_t>((value >> 24) & 0xFFu);
  dst[1] = static_cast<uint8_t>((value >> 16) & 0xFFu);
  dst[2] = static_cast<uint8_t>((value >> 8) & 0xFFu);
  dst[3] = static_cast<uint8_t>(value & 0xFFu);
}

/// 大端读出 32 位长度。
uint32_t ReadBe32(const uint8_t* src) {
  return (static_cast<uint32_t>(src[0]) << 24) |
         (static_cast<uint32_t>(src[1]) << 16) |
         (static_cast<uint32_t>(src[2]) << 8) | static_cast<uint32_t>(src[3]);
}
}  // namespace

FastDdsRawType::FastDdsRawType() {
  set_name("RawBytes");
  max_serialized_type_size = kPreallocSize + kLengthPrefixBytes;
  is_compute_key_provided = false;
}

bool FastDdsRawType::serialize(
    const void* const data,
    eprosima::fastdds::rtps::SerializedPayload_t& payload,
    eprosima::fastdds::dds::DataRepresentationId_t /*data_representation*/) {
  // 无 CDR 封装:字节原样落进 payload,故 data_representation 对本类型无意义。
  //
  // **载荷前先写 4 字节大端真实长度**(ADR-0023 D1):RTPS 把 DATA 子消息的序列化载荷按
  // 4 字节对齐,接收侧 `SerializedPayload_t::length` **含填充**,精确长度就是在那一层丢的。
  // 自带长度 ⇒ 反序列化不必信 `payload.length`。
  const auto* msg = static_cast<const RawBytes*>(data);
  const auto total = static_cast<uint32_t>(msg->payload.size());
  if (total > payload.max_size || kLengthPrefixBytes > payload.max_size - total) {
    return false;
  }
  WriteBe32(payload.data, total);
  if (total > 0) {
    std::memcpy(payload.data + kLengthPrefixBytes, msg->payload.data(), total);
  }
  payload.length = kLengthPrefixBytes + total;
  return true;
}

bool FastDdsRawType::deserialize(
    eprosima::fastdds::rtps::SerializedPayload_t& payload, void* data) {
  // **按前缀取,不信 `payload.length`**(ADR-0023 D1):后者含 RTPS 的 4 字节对齐填充。
  // 前缀本身须先校验(**D3**):长度不够装下前缀、或前缀声称的长度越过 `payload.length`,
  // 一律返 `false` —— Fast DDS 会丢弃该 sample。遇到旧版本对端(无前缀)或损坏样本时
  // **明确失败,不读越界**。
  if (payload.length < kLengthPrefixBytes) return false;
  const uint32_t declared = ReadBe32(payload.data);
  if (declared > payload.length - kLengthPrefixBytes) return false;

  auto* msg = static_cast<RawBytes*>(data);
  msg->payload.assign(payload.data + kLengthPrefixBytes,
                      payload.data + kLengthPrefixBytes + declared);
  return true;
}

uint32_t FastDdsRawType::calculate_serialized_size(
    const void* const data,
    eprosima::fastdds::dds::DataRepresentationId_t /*data_representation*/) {
  const auto* msg = static_cast<const RawBytes*>(data);
  return kLengthPrefixBytes + static_cast<uint32_t>(msg->payload.size());
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
