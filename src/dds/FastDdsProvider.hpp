#pragma once

// FastDdsProvider.hpp — IDdsProvider 的 Fast DDS 2.13 实现(pub-sub only)。
// participant + RawBytes 类型注册 + topic→writer/reader 懒加载 + DdsQos 映射。

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class FastDdsProvider : public IDdsProvider {
 public:
  FastDdsProvider();
  ~FastDdsProvider() override;

  Status Init(const DdsConfig& config) override;
  void   Shutdown() override;
  Status Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Status Subscribe(const std::string& topic,
                   std::function<void(const std::vector<uint8_t>&)> cb) override;
  Status Unsubscribe(const std::string& topic) override;
  std::string Name() const override { return "fastdds"; }

 private:
  class ReaderListener;
  eprosima::fastdds::dds::Topic* GetOrCreateTopic(const std::string& name);

  DdsConfig config_;
  eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
  eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
  eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
  eprosima::fastdds::dds::TypeSupport type_;

  std::mutex mutex_;
  std::map<std::string, eprosima::fastdds::dds::Topic*> topics_;
  std::map<std::string, eprosima::fastdds::dds::DataWriter*> writers_;
  struct ReaderEntry {
    eprosima::fastdds::dds::DataReader* reader = nullptr;
    std::unique_ptr<ReaderListener> listener;
  };
  std::map<std::string, ReaderEntry> readers_;
};

}  // namespace transport
