#pragma once

// FastDdsProvider.hpp — IDdsProvider 的 Fast DDS 3.x 实现(pub-sub only)。
// participant + RawBytes 类型注册 + topic→writer/reader(**由 DeclareWriter / Subscribe
// 显式建出,不惰性建**,ADR-0013 D13)+ DdsQos 映射。
//
// 3.x 相对 2.13.x 的断裂(ADR-0013 D14):CMake 包名 fastrtps → fastdds;
// eprosima::fastrtps::rtps → eprosima::fastdds::rtps;fastrtps/types/TypesBase.h
// 已不存在(ReturnCode_t 移入 fastdds/dds/core/ReturnCode.hpp,且由 class 退化成
// int32_t 的 typedef —— 见 .cpp 里 Publish 的返回值判定)。

#include <condition_variable>
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

#include "transport/io/dds/IDdsProvider.hpp"

namespace transport {

class FastDdsProvider : public IDdsProvider {
 public:
  FastDdsProvider();
  ~FastDdsProvider() override;

  Coro::Result<void> Init(const DdsConfig& config) override;
  void         Shutdown() override;
  /// 当场建出该 topic 的 `DataWriter`(D13);幂等。
  Coro::Result<void> DeclareWriter(const std::string& topic) override;
  /// **可阻塞**:RELIABLE 准入满时 park 调用线程至多一个 `max_blocking_time`(D13)。
  /// topic 须已 `DeclareWriter`,否则返 `kConfiguration`(**不惰性建**)。
  Coro::Result<void> Publish(const std::string& topic, const std::vector<uint8_t>& bytes) override;
  Coro::Result<void> Subscribe(const std::string& topic,
                         std::function<void(const std::vector<uint8_t>&)> cb) override;
  Coro::Result<void> Unsubscribe(const std::string& topic) override;
  [[nodiscard]] DdsMatchedCount MatchedCount() const override;
  std::string Name() const override { return "fastdds"; }

 private:
  class ReaderListener;
  eprosima::fastdds::dds::Topic* GetOrCreateTopic(const std::string& name);

  DdsConfig config_;
  eprosima::fastdds::dds::DomainParticipant* participant_ = nullptr;
  eprosima::fastdds::dds::Publisher* publisher_ = nullptr;
  eprosima::fastdds::dds::Subscriber* subscriber_ = nullptr;
  eprosima::fastdds::dds::TypeSupport type_;

  mutable std::mutex mutex_;
  /// 在途 `Publish` 归零的信号:`write()` 在锁外阻塞,`Shutdown` 删 writer 前须等它。
  std::condition_variable idle_cv_;
  int  in_flight_ = 0;
  bool closing_ = false;
  std::map<std::string, eprosima::fastdds::dds::Topic*> topics_;
  std::map<std::string, eprosima::fastdds::dds::DataWriter*> writers_;
  struct ReaderEntry {
    eprosima::fastdds::dds::DataReader* reader = nullptr;
    std::unique_ptr<ReaderListener> listener;
  };
  std::map<std::string, ReaderEntry> readers_;
};

}  // namespace transport
