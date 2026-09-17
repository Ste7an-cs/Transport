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

/// @brief `RawBytes` 的 TopicDataType:序列化就是 memcpy,无 CDR 封装头,
///        但**载荷前带一个 4 字节大端长度前缀**(ADR-0023 D1)。
///
/// 线缆布局:`[len:4 BE][payload]`。前缀在此的理由:RTPS 把 DATA 子消息的序列化载荷按
/// 4 字节对齐,接收侧 `SerializedPayload_t::length` **含填充**,精确长度在那一层丢失
/// (#258 的数据损坏)。自带长度即在与 RTPS 打交道的**这一层**把信息补回来,对任何
/// codec 都生效;`DdsCodec` 的线缆格式一个字节不改(**D2**)。
///
/// @warning **线缆不兼容**:不带前缀的旧版本对端发来的样本会被 `deserialize` 拒掉
///          (**D3**),两端必须一起升级。
class FastDdsRawType : public eprosima::fastdds::dds::TopicDataType {
 public:
  /// 长度前缀的字节数(大端)。
  static constexpr uint32_t kLengthPrefixBytes = 4;

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
