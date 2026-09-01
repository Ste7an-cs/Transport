#pragma once

// FastDdsRawType.hpp — "只携带 []byte" 的自定义 TopicDataType(Fast DDS 3.x)。
// 手写紧凑序列化(不经 CDR):payload 直接写入 SerializedPayload。版本敏感面之一。
//
// 3.x 相对 2.13.x 的断裂(ADR-0013 D14):命名空间由 eprosima::fastrtps::rtps 改为
// eprosima::fastdds::rtps;serialize/deserialize 由收指针改收引用且 serialize 增
// DataRepresentationId_t;getSerializedSizeProvider → calculate_serialized_size
// (由"返回一个求值闭包"改为直接返回大小);createData/deleteData → create_data/
// delete_data;getKey → compute_key(两个重载:按 payload、按 data);setName →
// set_name;m_typeSize → max_serialized_type_size;m_isGetKeyDefined →
// is_compute_key_provided。

#include <cstdint>
#include <vector>

#include <fastdds/dds/core/policy/QosPolicies.hpp>
#include <fastdds/dds/topic/TopicDataType.hpp>
#include <fastdds/rtps/common/InstanceHandle.hpp>
#include <fastdds/rtps/common/SerializedPayload.hpp>

namespace transport {

/// 线缆上的样本:一段不透明字节,别无他物(topic 不上线缆,ADR-0013 D6)。
struct RawBytes { std::vector<uint8_t> payload; };

/// @brief `RawBytes` 的 TopicDataType:序列化就是 memcpy,无 CDR 封装头。
class FastDdsRawType : public eprosima::fastdds::dds::TopicDataType {
 public:
  FastDdsRawType();

  bool serialize(
      const void* const data,
      eprosima::fastdds::rtps::SerializedPayload_t& payload,
      eprosima::fastdds::dds::DataRepresentationId_t data_representation) override;

  bool deserialize(eprosima::fastdds::rtps::SerializedPayload_t& payload,
                   void* data) override;

  uint32_t calculate_serialized_size(
      const void* const data,
      eprosima::fastdds::dds::DataRepresentationId_t data_representation) override;

  void* create_data() override;
  void delete_data(void* data) override;

  // 无键类型:两个 compute_key 重载都是纯虚,须双双给出,一律返 false。
  bool compute_key(eprosima::fastdds::rtps::SerializedPayload_t& payload,
                   eprosima::fastdds::rtps::InstanceHandle_t& ihandle,
                   bool force_md5) override;
  bool compute_key(const void* const data,
                   eprosima::fastdds::rtps::InstanceHandle_t& ihandle,
                   bool force_md5) override;
};

}  // namespace transport
