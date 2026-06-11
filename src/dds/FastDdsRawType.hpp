#pragma once

// -----------------------------------------------------------------------------
// FastDdsRawType.hpp — RawMessage 的自定义 TopicDataType（Fast DDS 2.13）
// 手写紧凑序列化（不经 CDR）：[u16 LE id_len][id][u16 LE reply_len][reply][payload]
// 版本敏感面之一（升 3.x 改本文件对的签名即可）。
// -----------------------------------------------------------------------------

#include <fastdds/dds/topic/TopicDataType.hpp>

#include "transport/dds/RawMessage.hpp"

namespace transport {

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
