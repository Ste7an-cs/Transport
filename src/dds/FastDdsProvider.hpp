#pragma once

// -----------------------------------------------------------------------------
// FastDdsProvider.hpp — IDdsProvider 的 Fast DDS 2.13 实现
// participant + RawMessage 类型注册 + topic→writer/reader 懒加载 + DdsQos 映射。
// 版本敏感面之一（升 3.x 仅改本文件对 + FastDdsRawType）。
// -----------------------------------------------------------------------------

#include <map>
#include <memory>
#include <mutex>
#include <set>
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
#include "transport/dds/RawMessage.hpp"

namespace transport {

class FastDdsProvider : public IDdsProvider {
 public:
  // ctor/dtor 均出线定义于 .cpp：ReaderListener 在头中仅前置声明，而
  // make_unique<FastDdsProvider>（测试 TU）会实例化构造的异常清理路径，
  // 触及含 unique_ptr<ReaderListener> 的成员析构——须在 ReaderListener 完整处生成。
  FastDdsProvider();
  ~FastDdsProvider() override;

  Status Init(const DdsConfig& config) override;
  Status Publish(const std::string& topic,
                 const std::vector<uint8_t>& data) override;
  Status Subscribe(const std::string& topic,
                   ITransport::ReceiveCallback cb) override;
  Status Unsubscribe(const std::string& topic) override;
  Status SendRequest(const std::string& request_topic,
                     const std::string& request_id,
                     const std::string& reply_topic,
                     const std::vector<uint8_t>& data) override;
  Status SubscribeReplies(const std::string& reply_topic,
                          ReplySink sink) override;
  Status ServeRequests(const std::string& request_topic,
                       RequestSink sink) override;
  Status Reply(const std::string& reply_topic, const std::string& request_id,
               const std::vector<uint8_t>& data) override;
  void Shutdown() override;
  std::string ProviderName() const override { return "FastDDS"; }

 private:
  using RawSink = std::function<void(const RawMessage&)>;

  class ReaderListener;  // on_data_available → take → RawSink

  // 内部统一原语：写 RawMessage / 以 RawSink 订阅
  Status WriteRaw(const std::string& topic, const RawMessage& msg);
  Status SubscribeRaw(const std::string& topic, RawSink sink);
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
  std::set<std::string> reply_subscribed_;
};

// 显式注册入口（静态库下匿名注册器可能被链接器裁剪；TransportFactory/测试调用此函数）
void RegisterFastDdsProvider();

}  // namespace transport
