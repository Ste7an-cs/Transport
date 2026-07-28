#include "FastDdsProvider.hpp"

#include <utility>

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastrtps/types/TypesBase.h>

#include "FastDdsRawType.hpp"

// FastDdsProvider.cpp — Fast DDS 2.13 provider(pub-sub only)。
// 线程:reader listener 回调来自 FastDDS 内部线程 → sink(=DdsTransport 的交付)须线程安全。

namespace transport {

namespace dds = eprosima::fastdds::dds;
using eprosima::fastrtps::types::ReturnCode_t;
using Sink = std::function<void(const std::vector<uint8_t>&)>;

namespace {
void ApplyQos(const DdsQos& q, dds::DataWriterQos* wqos, dds::DataReaderQos* rqos) {
  const auto rel = (q.reliability == DdsQos::Reliability::kReliable)
                       ? dds::RELIABLE_RELIABILITY_QOS : dds::BEST_EFFORT_RELIABILITY_QOS;
  const auto dur = (q.durability == DdsQos::Durability::kTransientLocal)
                       ? dds::TRANSIENT_LOCAL_DURABILITY_QOS : dds::VOLATILE_DURABILITY_QOS;
  if (wqos) {
    wqos->reliability().kind = rel; wqos->durability().kind = dur;
    wqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    wqos->history().depth = static_cast<int32_t>(q.history_depth);
    wqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
  if (rqos) {
    rqos->reliability().kind = rel; rqos->durability().kind = dur;
    rqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    rqos->history().depth = static_cast<int32_t>(q.history_depth);
    rqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
}
}  // namespace

class FastDdsProvider::ReaderListener : public dds::DataReaderListener {
 public:
  explicit ReaderListener(Sink sink) : sink_(std::move(sink)) {}
  void on_data_available(dds::DataReader* reader) override {
    RawBytes msg;
    dds::SampleInfo info;
    while (reader->take_next_sample(&msg, &info) == ReturnCode_t::RETCODE_OK) {
      if (info.valid_data) sink_(msg.payload);
    }
  }
 private:
  Sink sink_;
};

FastDdsProvider::FastDdsProvider() = default;
FastDdsProvider::~FastDdsProvider() { Shutdown(); }

Status FastDdsProvider::Init(const DdsConfig& config) {
  config_ = config;
  auto* factory = dds::DomainParticipantFactory::get_instance();
  participant_ = factory->create_participant(config.domain_id, dds::PARTICIPANT_QOS_DEFAULT);
  // participant 建不出通常源于 domain_id 等配置不合法 → kConfiguration。
  if (!participant_)
    return make_error_code(TransportErrc::kConfiguration);
  type_ = dds::TypeSupport(new FastDdsRawType());
  if (type_.register_type(participant_) != ReturnCode_t::RETCODE_OK)
    return make_error_code(TransportErrc::kIo);
  publisher_ = participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
  subscriber_ = participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
  if (!publisher_ || !subscriber_)
    return make_error_code(TransportErrc::kIo);
  return Status{};
}

dds::Topic* FastDdsProvider::GetOrCreateTopic(const std::string& name) {
  auto it = topics_.find(name);
  if (it != topics_.end()) return it->second;
  dds::Topic* t = participant_->create_topic(name, "RawBytes", dds::TOPIC_QOS_DEFAULT);
  if (t) topics_[name] = t;
  return t;
}

Status FastDdsProvider::Publish(const std::string& topic,
                                      const std::vector<uint8_t>& bytes) {
  dds::DataWriter* writer = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = writers_.find(topic);
    if (it != writers_.end()) {
      writer = it->second;
    } else {
      dds::Topic* t = GetOrCreateTopic(topic);
      if (!t) return make_error_code(TransportErrc::kIo);
      dds::DataWriterQos wqos = dds::DATAWRITER_QOS_DEFAULT;
      ApplyQos(config_.qos, &wqos, nullptr);
      writer = publisher_->create_datawriter(t, wqos, nullptr);
      if (!writer) return make_error_code(TransportErrc::kIo);
      writers_[topic] = writer;
    }
  }
  RawBytes copy; copy.payload = bytes;
  // Fast DDS 2.13.1 的 DataWriter::write(void*) 单参重载返回 bool(成功 true)。
  if (!writer->write(&copy)) return make_error_code(TransportErrc::kIo);
  return Status{};
}

Status FastDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (readers_.count(topic)) return Status{};  // 幂等
  dds::Topic* t = GetOrCreateTopic(topic);
  if (!t) return make_error_code(TransportErrc::kIo);
  auto listener = std::make_unique<ReaderListener>(std::move(cb));
  dds::DataReaderQos rqos = dds::DATAREADER_QOS_DEFAULT;
  ApplyQos(config_.qos, nullptr, &rqos);
  dds::DataReader* reader = subscriber_->create_datareader(t, rqos, listener.get());
  if (!reader) return make_error_code(TransportErrc::kIo);
  readers_[topic] = ReaderEntry{reader, std::move(listener)};
  return Status{};
}

Status FastDdsProvider::Unsubscribe(const std::string& topic) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = readers_.find(topic);
  if (it == readers_.end()) return Status{};
  subscriber_->delete_datareader(it->second.reader);
  readers_.erase(it);
  return Status{};
}

void FastDdsProvider::Shutdown() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (!participant_) return;
  for (auto& kv : readers_) subscriber_->delete_datareader(kv.second.reader);
  readers_.clear();
  for (auto& kv : writers_) publisher_->delete_datawriter(kv.second);
  writers_.clear();
  for (auto& kv : topics_) participant_->delete_topic(kv.second);
  topics_.clear();
  if (subscriber_) participant_->delete_subscriber(subscriber_);
  if (publisher_) participant_->delete_publisher(publisher_);
  dds::DomainParticipantFactory::get_instance()->delete_participant(participant_);
  participant_ = nullptr; publisher_ = nullptr; subscriber_ = nullptr;
}

}  // namespace transport
