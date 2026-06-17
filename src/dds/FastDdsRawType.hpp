#pragma once

// FastDdsRawType.hpp — "只携带 []byte" 的自定义 TopicDataType(Fast DDS 2.13)。
// 手写紧凑序列化(不经 CDR):payload 直接写入 SerializedPayload。版本敏感面之一。

#include <cstdint>
#include <functional>
#include <vector>

#include <fastdds/dds/topic/TopicDataType.hpp>

namespace transport {

struct RawBytes { std::vector<uint8_t> payload; };

class FastDdsRawType : public eprosima::fastdds::dds::TopicDataType {
 public:
  FastDdsRawType();
  bool serialize(void* data,
                 eprosima::fastrtps::rtps::SerializedPayload_t* payload) override;
  bool deserialize(eprosima::fastrtps::rtps::SerializedPayload_t* payload,
                   void* data) override;
  std::function<uint32_t()> getSerializedSizeProvider(void* data) override;
  void* createData() override;
  void deleteData(void* data) override;
  bool getKey(void* data, eprosima::fastrtps::rtps::InstanceHandle_t* handle,
              bool force_md5) override;
};

}  // namespace transport
