#include "FastDdsProvider.hpp"

#include <utility>
#include <variant>

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastrtps/types/TypesBase.h>

#include "FastDdsRawType.hpp"
#include "transport/dds/DdsProviderRegistry.hpp"

// FastDdsProvider.cpp — Fast DDS 2.13 provider（见 FastDdsProvider.hpp）。
// 线程：reader listener 回调来自 FastDDS 内部线程 → RawSink 须线程安全（DdsImpl
// 侧由 ReceiveQueue/mutex 保证）。实体均挂在单一 participant 下，Shutdown 逆序清理。

namespace transport {

namespace dds = eprosima::fastdds::dds;
using eprosima::fastrtps::types::ReturnCode_t;

namespace {
Status Ok() { return Status::Success(std::monostate{}); }

void ApplyQos(const DdsQos& q, dds::DataWriterQos* wqos,
              dds::DataReaderQos* rqos) {
  const auto rel = (q.reliability == DdsQos::Reliability::kReliable)
                       ? dds::RELIABLE_RELIABILITY_QOS
                       : dds::BEST_EFFORT_RELIABILITY_QOS;
  const auto dur = (q.durability == DdsQos::Durability::kTransientLocal)
                       ? dds::TRANSIENT_LOCAL_DURABILITY_QOS
                       : dds::VOLATILE_DURABILITY_QOS;
  if (wqos) {
    wqos->reliability().kind = rel;
    wqos->durability().kind = dur;
    wqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    wqos->history().depth = static_cast<int32_t>(q.history_depth);
    wqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
  if (rqos) {
    rqos->reliability().kind = rel;
    rqos->durability().kind = dur;
    rqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    rqos->history().depth = static_cast<int32_t>(q.history_depth);
    rqos->endpoint().history_memory_policy =
        eprosima::fastrtps::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
}
}  // namespace

// reader listener：每条 sample 取出 RawMessage 转交 sink
class FastDdsProvider::ReaderListener : public dds::DataReaderListener {
 public:
  explicit ReaderListener(RawSink sink) : sink_(std::move(sink)) {}
  void on_data_available(dds::DataReader* reader) override {
    RawMessage msg;
    dds::SampleInfo info;
    while (reader->take_next_sample(&msg, &info) == ReturnCode_t::RETCODE_OK) {
      if (info.valid_data) sink_(msg);
    }
  }

 private:
  RawSink sink_;
};

FastDdsProvider::FastDdsProvider() = default;

FastDdsProvider::~FastDdsProvider() { Shutdown(); }

Status FastDdsProvider::Init(const DdsConfig& config) {
  config_ = config;
  auto* factory = dds::DomainParticipantFactory::get_instance();
  participant_ = factory->create_participant(config.domain_id,
                                             dds::PARTICIPANT_QOS_DEFAULT);
  if (!participant_)
    return Status::Fail("io: create_participant failed (domain " +
                        std::to_string(config.domain_id) + ")");
  type_ = dds::TypeSupport(new FastDdsRawType());
  if (type_.register_type(participant_) != ReturnCode_t::RETCODE_OK)
    return Status::Fail("io: register_type RawMessage failed");
  publisher_ = participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
  subscriber_ = participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
  if (!publisher_ || !subscriber_)
    return Status::Fail("io: create publisher/subscriber failed");
  return Ok();
}

dds::Topic* FastDdsProvider::GetOrCreateTopic(const std::string& name) {
  auto it = topics_.find(name);
  if (it != topics_.end()) return it->second;
  dds::Topic* t =
      participant_->create_topic(name, "RawMessage", dds::TOPIC_QOS_DEFAULT);
  if (t) topics_[name] = t;
  return t;
}

Status FastDdsProvider::WriteRaw(const std::string& topic,
                                 const RawMessage& msg) {
  dds::DataWriter* writer = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = writers_.find(topic);
    if (it != writers_.end()) {
      writer = it->second;
    } else {
      dds::Topic* t = GetOrCreateTopic(topic);
      if (!t) return Status::Fail("io: create_topic failed: " + topic);
      dds::DataWriterQos wqos = dds::DATAWRITER_QOS_DEFAULT;
      ApplyQos(config_.qos, &wqos, nullptr);
      writer = publisher_->create_datawriter(t, wqos, nullptr);
      if (!writer) return Status::Fail("io: create_datawriter failed: " + topic);
      writers_[topic] = writer;
    }
  }
  // write 需要非 const 指针
  RawMessage copy = msg;
  // 机械适配：Fast DDS 2.13.1 的 DataWriter::write(void*) 单参重载返回 bool
  // （成功 true），而非 ReturnCode_t；按 bool 判定（计划注释已预告此偏差）。
  if (!writer->write(&copy))
    return Status::Fail("io: write failed: " + topic);
  return Ok();
}

Status FastDdsProvider::SubscribeRaw(const std::string& topic, RawSink sink) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (readers_.count(topic)) return Ok();  // 已订阅（幂等）
  dds::Topic* t = GetOrCreateTopic(topic);
  if (!t) return Status::Fail("io: create_topic failed: " + topic);
  auto listener = std::make_unique<ReaderListener>(std::move(sink));
  dds::DataReaderQos rqos = dds::DATAREADER_QOS_DEFAULT;
  ApplyQos(config_.qos, nullptr, &rqos);
  dds::DataReader* reader =
      subscriber_->create_datareader(t, rqos, listener.get());
  if (!reader) return Status::Fail("io: create_datareader failed: " + topic);
  readers_[topic] = ReaderEntry{reader, std::move(listener)};
  return Ok();
}

Status FastDdsProvider::Publish(const std::string& topic,
                                const std::vector<uint8_t>& data) {
  RawMessage m;
  m.payload = data;
  return WriteRaw(topic, m);
}

Status FastDdsProvider::Subscribe(const std::string& topic,
                                  ITransport::ReceiveCallback cb) {
  return SubscribeRaw(topic, [cb, topic](const RawMessage& m) {
    Message msg;
    msg.payload = m.payload;
    msg.topic = topic;
    msg.source = topic;
    cb(Result<Message>::Success(std::move(msg)));
  });
}

Status FastDdsProvider::Unsubscribe(const std::string& topic) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = readers_.find(topic);
  if (it == readers_.end()) return Ok();
  subscriber_->delete_datareader(it->second.reader);
  readers_.erase(it);
  return Ok();
}

Status FastDdsProvider::SendRequest(const std::string& request_topic,
                                    const std::string& request_id,
                                    const std::string& reply_topic,
                                    const std::vector<uint8_t>& data) {
  RawMessage m;
  m.request_id = request_id;
  m.reply_topic = reply_topic;
  m.payload = data;
  return WriteRaw(request_topic, m);
}

Status FastDdsProvider::SubscribeReplies(const std::string& reply_topic,
                                         ReplySink sink) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!reply_subscribed_.insert(reply_topic).second) return Ok();
  }
  return SubscribeRaw(reply_topic, [sink](const RawMessage& m) {
    sink(m.request_id, m.payload);
  });
}

Status FastDdsProvider::ServeRequests(const std::string& request_topic,
                                      RequestSink sink) {
  return SubscribeRaw(request_topic, [sink](const RawMessage& m) {
    sink(m.payload, m.request_id, m.reply_topic);
  });
}

Status FastDdsProvider::Reply(const std::string& reply_topic,
                              const std::string& request_id,
                              const std::vector<uint8_t>& data) {
  RawMessage m;
  m.request_id = request_id;
  m.payload = data;
  return WriteRaw(reply_topic, m);
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
  dds::DomainParticipantFactory::get_instance()->delete_participant(
      participant_);
  participant_ = nullptr;
  publisher_ = nullptr;
  subscriber_ = nullptr;
}

void RegisterFastDdsProvider() {
  DdsProviderRegistry::RegisterProvider(
      "FastDDS", [] { return std::make_unique<FastDdsProvider>(); });
}

namespace {
// 静态注册器：动态库/直接链接对象时自动注册；静态库可能被裁剪 → 工厂/用户可显式
// 调 RegisterFastDdsProvider()（TransportFactory 落地时调用）。
struct FastDdsRegistrar {
  FastDdsRegistrar() { RegisterFastDdsProvider(); }
} g_fastdds_registrar;
}  // namespace

}  // namespace transport
