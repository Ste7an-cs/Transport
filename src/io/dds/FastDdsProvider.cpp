#include "FastDdsProvider.hpp"

#include <utility>

#include <fastdds/dds/core/ReturnCode.hpp>
#include <fastdds/dds/core/status/LivelinessChangedStatus.hpp>
#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

#include "FastDdsRawType.hpp"

// FastDdsProvider.cpp — Fast DDS 3.x provider(pub-sub only)。
// 线程:reader listener 回调来自 FastDDS 内部线程 → sink(=DdsTransport 的交付)须线程安全。

namespace transport {

namespace dds = eprosima::fastdds::dds;
using Sink = std::function<void(const std::vector<uint8_t>&)>;

namespace {

/// 我方的 `std::chrono::milliseconds` → DDS 的 `Duration_t`(秒 + 纳秒)。
dds::Duration_t ToDdsDuration(std::chrono::milliseconds ms) {
  const auto count = ms.count();
  return dds::Duration_t(static_cast<int32_t>(count / 1000),
                         static_cast<uint32_t>((count % 1000) * 1000000));
}

/// QoS **统一一套、不按模式分**(ADR-0013 D4):同一份 `DdsQos` 同时铺到 writer 与 reader。
void ApplyQos(const DdsQos& q, dds::DataWriterQos* wqos, dds::DataReaderQos* rqos) {
  const auto rel = (q.reliability == DdsQos::Reliability::kReliable)
                       ? dds::RELIABLE_RELIABILITY_QOS : dds::BEST_EFFORT_RELIABILITY_QOS;
  const auto dur = (q.durability == DdsQos::Durability::kTransientLocal)
                       ? dds::TRANSIENT_LOCAL_DURABILITY_QOS : dds::VOLATILE_DURABILITY_QOS;
  const dds::Duration_t lease = ToDdsDuration(q.liveliness_lease);
  // AUTOMATIC 下 announcement_period 须 < lease,且建议 < 0.7*lease(Fast DDS 头文件的告诫)。
  const dds::Duration_t announce = ToDdsDuration(q.liveliness_lease / 3);
  if (wqos) {
    wqos->reliability().kind = rel; wqos->durability().kind = dur;
    // D3/D10:RELIABLE 准入满时 write() park 调用线程的上限。
    wqos->reliability().max_blocking_time = ToDdsDuration(q.max_blocking_time);
    wqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    wqos->history().depth = static_cast<int32_t>(q.history_depth);
    // D9:必须配 AUTOMATIC_LIVELINESS。只靠 matched 时对端被硬杀要等 participant
    // lease(默认 20s)才检出,期间谎报 kUp。
    wqos->liveliness().kind = dds::AUTOMATIC_LIVELINESS_QOS;
    wqos->liveliness().lease_duration = lease;
    wqos->liveliness().announcement_period = announce;
    wqos->endpoint().history_memory_policy =
        eprosima::fastdds::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
  if (rqos) {
    rqos->reliability().kind = rel; rqos->durability().kind = dur;
    rqos->history().kind = dds::KEEP_LAST_HISTORY_QOS;
    rqos->history().depth = static_cast<int32_t>(q.history_depth);
    // reader 侧请求的 lease 须 >= writer 侧提供的,否则 QoS 不兼容、根本匹配不上;
    // 两侧同源于一份 DdsQos,故恒等。判活的实际收益也在这一侧(alive_count)。
    rqos->liveliness().kind = dds::AUTOMATIC_LIVELINESS_QOS;
    rqos->liveliness().lease_duration = lease;
    rqos->endpoint().history_memory_policy =
        eprosima::fastdds::rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
  }
}

}  // namespace

class FastDdsProvider::ReaderListener : public dds::DataReaderListener {
 public:
  explicit ReaderListener(Sink sink) : sink_(std::move(sink)) {}
  void on_data_available(dds::DataReader* reader) override {
    RawBytes msg;
    dds::SampleInfo info;
    while (reader->take_next_sample(&msg, &info) == dds::RETCODE_OK) {
      if (info.valid_data) sink_(msg.payload);
    }
  }
 private:
  Sink sink_;
};

FastDdsProvider::FastDdsProvider() = default;
FastDdsProvider::~FastDdsProvider() { Shutdown(); }

Coro::Result<void> FastDdsProvider::Init(const DdsConfig& config) {
  config_ = config;
  auto* factory = dds::DomainParticipantFactory::get_instance();
  participant_ = factory->create_participant(config.domain_id, dds::PARTICIPANT_QOS_DEFAULT);
  // participant 建不出通常源于 domain_id 等配置不合法 → kConfiguration。
  if (!participant_)
    return make_error_code(TransportErrc::kConfiguration);
  type_ = dds::TypeSupport(new FastDdsRawType());
  if (type_.register_type(participant_) != dds::RETCODE_OK)
    return make_error_code(TransportErrc::kIo);
  publisher_ = participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT);
  subscriber_ = participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT);
  if (!publisher_ || !subscriber_)
    return make_error_code(TransportErrc::kIo);
  return Coro::Result<void>{};
}

dds::Topic* FastDdsProvider::GetOrCreateTopic(const std::string& name) {
  auto it = topics_.find(name);
  if (it != topics_.end()) return it->second;
  dds::Topic* t = participant_->create_topic(name, "RawBytes", dds::TOPIC_QOS_DEFAULT);
  if (t) topics_[name] = t;
  return t;
}

Coro::Result<void> FastDdsProvider::Publish(const std::string& topic,
                                      const std::vector<uint8_t>& bytes) {
  dds::DataWriter* writer = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    // 未 Init / 正在 Shutdown:调用序错误 → kInvalidState(与 FakeDdsProvider 一致)。
    if (!participant_ || closing_) return make_error_code(TransportErrc::kInvalidState);
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
    // write() 在锁外跑(它会阻塞,不能占着锁),故须记在途数——Shutdown 得等它跑完
    // 才能删 writer,否则是 use-after-free。
    ++in_flight_;
  }

  RawBytes copy; copy.payload = bytes;
  // ⚠ 3.x 的返回值语义与 2.13.x 相反,此处**不能**写成 `if (!writer->write(&copy))`:
  //   2.13.x 的单参 write() 返回 bool(成功 = true);
  //   3.x   的单参 write() 返回 ReturnCode_t —— 它是 `int32_t` 的 typedef 且
  //         `RETCODE_OK == 0`,于是"成功"取反为真、"失败"取反为假,判定整个颠倒
  //         (而且零告警照常编译)。故一律显式与 RETCODE_OK 比。
  const dds::ReturnCode_t rc = writer->write(&copy);

  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (--in_flight_ == 0) idle_cv_.notify_all();
  }
  // RETCODE_TIMEOUT(准入等满 max_blocking_time)也走这条:对调用方就是一次写失败。
  if (rc != dds::RETCODE_OK) return make_error_code(TransportErrc::kIo);
  return Coro::Result<void>{};
}

Coro::Result<void> FastDdsProvider::Subscribe(const std::string& topic, Sink cb) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (!participant_ || closing_) return make_error_code(TransportErrc::kInvalidState);
  if (readers_.count(topic)) return Coro::Result<void>{};  // 幂等
  dds::Topic* t = GetOrCreateTopic(topic);
  if (!t) return make_error_code(TransportErrc::kIo);
  auto listener = std::make_unique<ReaderListener>(std::move(cb));
  dds::DataReaderQos rqos = dds::DATAREADER_QOS_DEFAULT;
  ApplyQos(config_.qos, nullptr, &rqos);
  dds::DataReader* reader = subscriber_->create_datareader(t, rqos, listener.get());
  if (!reader) return make_error_code(TransportErrc::kIo);
  readers_[topic] = ReaderEntry{reader, std::move(listener)};
  return Coro::Result<void>{};
}

Coro::Result<void> FastDdsProvider::Unsubscribe(const std::string& topic) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (!participant_) return make_error_code(TransportErrc::kInvalidState);
  auto it = readers_.find(topic);
  if (it == readers_.end()) return Coro::Result<void>{};
  subscriber_->delete_datareader(it->second.reader);
  readers_.erase(it);
  return Coro::Result<void>{};
}

DdsMatchedCount FastDdsProvider::MatchedCount() const {
  std::lock_guard<std::mutex> lk(mutex_);
  DdsMatchedCount out;
  if (!participant_) return out;
  for (const auto& kv : writers_) {
    dds::PublicationMatchedStatus st;
    if (kv.second->get_publication_matched_status(st) != dds::RETCODE_OK) continue;
    out.matched += st.current_count;
    // writer 侧的对端(reader)**没有**判活手段:DDS 的 liveliness 是 writer → reader
    // 的单向断言,reader 从不向 writer 断言存活。故匹配上的 reader 一律计为存活,
    // 其消失只能靠 matched 归零检出(即 participant lease)。判活的收益在 reader 侧。
    out.alive += st.current_count;
  }
  for (const auto& kv : readers_) {
    dds::SubscriptionMatchedStatus st;
    if (kv.second.reader->get_subscription_matched_status(st) == dds::RETCODE_OK)
      out.matched += st.current_count;
    dds::LivelinessChangedStatus ls;
    if (kv.second.reader->get_liveliness_changed_status(ls) == dds::RETCODE_OK)
      out.alive += ls.alive_count;
  }
  return out;
}

void FastDdsProvider::Shutdown() {
  std::unique_lock<std::mutex> lk(mutex_);
  if (!participant_) return;
  // 先拦住新的 Publish,再**等**在途的写跑完——Fast DDS 3.6.1 没有中断在途 write()
  // 的手段(DataWriter 上无 cancel/abort 之类),所以这里只能等,而这一等**没有上界**:
  // 阻塞来自同进程订阅方的交付回调在发布线程上同步执行,`max_blocking_time` 不参与
  // (实测它设 300ms 而 Publish 跑满 2000ms)。界由同进程内最慢的那个订阅回调决定
  // (ADR-0013「明确接受的代价」7,2026-09-01 改判)。
  closing_ = true;
  idle_cv_.wait(lk, [this] { return in_flight_ == 0; });

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
  closing_ = false;  // 复位,以便同一对象重新 Init
}

}  // namespace transport
