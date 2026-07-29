#include "transport/io/dds/DdsTransport.hpp"

#include <mutex>
#include <utility>

#include "transport/node/BoundedQueue.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/SharedCompletion.hpp"

namespace transport {

// 交接边界的实现状态。交接队列 handoff_ 与 provider 同置于一个 shared_ptr<State>,
// 但 provider 的订阅回调**只捕获 handoff_ 的共享句柄(BoundedQueue 内部 shared_ptr),
// 不捕获 State**——这样回调与 provider 之间无引用环(State→provider→回调→State),
// 且迟到回调在 State 销毁后仍只触碰存活的交接队列共享状态,不碰已销毁对象。
struct DdsTransport::State {
  explicit State(std::unique_ptr<IDdsProvider> p, DdsConfig cfg,
                 std::vector<std::string> topics, std::size_t max_samples,
                 std::size_t max_bytes)
      : provider(std::move(p)),
        config(std::move(cfg)),
        subscribe_topics(std::move(topics)),
        handoff([](const Sample& s) { return s.bytes.size(); }, max_samples,
                max_bytes) {}

  mutable std::mutex mutex;
  std::unique_ptr<IDdsProvider> provider;
  DdsConfig config;
  std::vector<std::string> subscribe_topics;
  // 跨线程有界交接边界:listener 线程 Push、Read 侧 fiber Pop(BoundedQueue 内部
  // std::mutex 守表 + Awaitable 唤醒,底层 boost.fiber channel 跨线程安全)。
  BoundedQueue<Sample> handoff;
  LifecycleState lifecycle{LifecycleState::kCreated};
  SharedCompletion<void> closed;
};

namespace {

// 关闭一次(幂等):进入 Closing → 先 Unsubscribe 停投递 → Shutdown 释放 provider 侧
// 回调(连带释放其持有的交接队列共享句柄)→ 交接边界 Close 唤醒在途 Read → 落 Closed
// 并完成 closed。迟到的在途回调(Dispatch 已取快照)仍只对交接队列 Push,返 kClosed 丢弃,
// 不触碰 State(RT_NODE_005 防碰已销毁对象)。
void BeginClose(const std::shared_ptr<DdsTransport::State>& state) {
  std::vector<std::string> topics;
  IDdsProvider* provider = nullptr;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosing ||
        state->lifecycle == LifecycleState::kClosed) {
      return;  // 已在关闭:幂等。
    }
    state->lifecycle = LifecycleState::kClosing;
    topics = state->subscribe_topics;
    provider = state->provider.get();
  }
  if (provider) {
    for (const auto& topic : topics) {
      (void)provider->Unsubscribe(topic);  // 先停投递。
    }
    provider->Shutdown();  // 释放回调(连带其交接队列共享句柄)。
  }
  state->handoff.Close();  // 唤醒在途 Read(以 kClosed 收敛)。
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->lifecycle = LifecycleState::kClosed;
  }
  state->closed.Complete(Status{});
}

}  // namespace

DdsTransport::DdsTransport(std::unique_ptr<IDdsProvider> provider,
                           DdsConfig config, std::vector<std::string> topics,
                           std::size_t max_samples, std::size_t max_bytes)
    : state_(std::make_shared<State>(std::move(provider), std::move(config),
                                     std::move(topics), max_samples,
                                     max_bytes)) {}

DdsTransport::~DdsTransport() { BeginClose(state_); }

Status DdsTransport::Start() {
  const auto state = state_;
  IDdsProvider* provider = nullptr;
  DdsConfig config;
  std::vector<std::string> topics;
  BoundedQueue<Sample> handoff = state->handoff;  // 共享句柄:供回调捕获(不捕获 State)。
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kRunning) {
      return Status{};  // 幂等。
    }
    if (state->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (!state->provider) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    provider = state->provider.get();
    config = state->config;
    topics = state->subscribe_topics;
  }

  if (Status init = provider->Init(config); !init) {
    return init;
  }
  // 逐 topic 订阅:回调在 listener 线程构造 Sample 并**非阻塞** Push 交接边界。回调只
  // 捕获 handoff(BoundedQueue 共享句柄)与 topic,不引用 State——无引用环、迟到安全。
  for (const auto& topic : topics) {
    Status sub = provider->Subscribe(
        topic, [handoff, topic](const std::vector<std::uint8_t>& bytes) mutable {
          // 满即 tail-drop(BoundedQueue 内部计数 dds_handoff_overflow),不阻塞 listener。
          (void)handoff.Push(Sample{bytes, topic});
        });
    if (!sub) {
      return sub;  // 订阅失败:上层可 RequestClose 收敛已订阅的 topic。
    }
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->lifecycle = LifecycleState::kRunning;
  }
  return Status{};
}

Result<Datagram> DdsTransport::Read(OperationOptions options) {
  const auto state = state_;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
  }

  // 出队交接边界:空则 fiber 协作 await,被 listener 线程 Push 或 Close 唤醒。错误类别
  // (kClosed/kTimeout/kCancelled)由 BoundedQueue 直接透传。
  Result<Sample> sample = state->handoff.Pop(std::move(options));
  if (!sample) {
    return sample.error();
  }
  Datagram datagram;
  datagram.bytes = std::move(sample.value().bytes);
  datagram.source = Endpoint::Topic(std::move(sample.value().topic));
  return Result<Datagram>{std::move(datagram)};
}

Status DdsTransport::Write(SendUnit unit) {
  const auto state = state_;
  IDdsProvider* provider = nullptr;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
    if (state->lifecycle != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
    provider = state->provider.get();
  }
  // 统一寻址:DDS 每条消息经 destination.topic 发往不同 topic;非 topic 目的地
  // (kDefault/kNet)对 DDS 无意义 → kInvalidArgument。
  if (unit.destination.kind != Endpoint::Kind::kTopic) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  if (!provider) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  return provider->Publish(unit.destination.topic, unit.bytes);
}

Status DdsTransport::RequestClose() {
  BeginClose(state_);
  return Status{};
}

Status DdsTransport::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

std::size_t DdsTransport::DdsHandoffOverflowCount() const {
  return state_->handoff.DroppedCount();
}

}  // namespace transport
