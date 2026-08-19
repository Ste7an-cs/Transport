#include "transport/io/dds/DdsTransport.hpp"

#include <mutex>
#include <utility>

#include <boost/fiber/channel_op_status.hpp>

#include "await/awaitable.hpp"
#include "task/fibertask.h"  // Coro::makeTask —— 转发泵 fiber。
#include "transport/node/BoundedQueue.hpp"
#include "transport/core/DropReason.hpp"
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
                 std::size_t max_bytes, ITraceSink* trace_sink)
      : provider(std::move(p)),
        config(std::move(cfg)),
        subscribe_topics(std::move(topics)),
        // 归因(P5-3):交接边界满 tail-drop 命名 kDdsHandoffOverflow,trace_sink 透传。
        handoff([](const Sample& s) { return s.bytes.size(); }, max_samples,
                max_bytes, DropReason::kDdsHandoffOverflow, trace_sink) {}

  mutable std::mutex mutex;
  std::unique_ptr<IDdsProvider> provider;
  DdsConfig config;
  std::vector<std::string> subscribe_topics;
  // 跨线程有界交接边界:listener 线程 Push、转发泵 fiber Pop(BoundedQueue 内部
  // std::mutex 守表 + Awaitable 唤醒,底层 boost.fiber channel 跨线程安全)。
  BoundedQueue<Sample> handoff;
  // 对外 read_queue(ADR-0007 D1/D4):转发泵是唯一生产者,`Read()` 只交出本句柄。
  // 交接边界(有界 + kDdsHandoffOverflow 归因)仍在其上游不变;本队列容量策略未定
  // (TBD-009),沿用 AsyncTask 默认,本轮不处置(#152)。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue{
      std::make_shared<Coro::Awaitable<Datagram>>()};
  LifecycleState lifecycle{LifecycleState::kCreated};
  SharedCompletion<void> closed;

  // I/O 事实(ADR-0003 D13):Publish 成功记 last_send;出队样本记 last_recv;
  // Publish/Pop 操作失败记 last_error(RT_NODE_006「所有介质如实报」)。
  std::optional<Clock::time_point> last_send;
  std::optional<Clock::time_point> last_recv;
  std::error_code last_error;
};

namespace {

// 关闭一次(幂等):进入 Closing → 先 Unsubscribe 停投递 → Shutdown 释放 provider 侧
// 回调(连带释放其持有的交接队列共享句柄)→ 交接边界 Close(令转发泵退出)→ read_queue
// 以 kClosed 关闭唤醒全部读者 → 落 Closed 并完成 closed。迟到的在途回调(Dispatch 已取
// 快照)仍只对交接队列 Push,返 kClosed 丢弃,不触碰 State(RT_NODE_005 防碰已销毁对象)。
void BeginClose(const std::shared_ptr<DdsTransport::State>& state) {
  std::vector<std::string> topics;
  IDdsProvider* provider = nullptr;
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kClosing ||
        state->lifecycle == LifecycleState::kClosed) {
      return;  // 已在关闭:幂等。
    }
    state->lifecycle = LifecycleState::kClosing;
    topics = state->subscribe_topics;
    provider = state->provider.get();
    read_queue = state->read_queue;
  }
  if (provider) {
    for (const auto& topic : topics) {
      (void)provider->Unsubscribe(topic);  // 先停投递。
    }
    provider->Shutdown();  // 释放回调(连带其交接队列共享句柄)。
  }
  state->handoff.Close();  // 令转发泵退出。
  // 终止表达(ADR-0007 D4):read_queue 被 close 并携带终止原因,调用方 await 得到它;
  // 同时丢弃残留——与改造前 BoundedQueue「Close 先于取元素、残留不再经 Read 交付」
  // 逐字对齐(残留样本的归因口径亦不变:本就不计入任何丢弃计数)。
  CloseDatagramQueue(read_queue, make_error_code(TransportErrc::kClosed));
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->lifecycle = LifecycleState::kClosed;
  }
  state->closed.Complete(Coro::Result<void>{});
}

// 转发泵(ADR-0007 D1 的 DDS 形态):反复从交接边界 Pop 一条 Sample,转成 Datagram 投入
// read_queue,直至交接边界 Close。DDS 无 socket,故只有内层数据泵、无外层管理循环。
//
// 错误处置与改造前 `Read` 逐字对齐:kClosed = 我方关闭 → 关 read_queue 并退出;其余
// (kInternal 等)是可继续的瞬时错误 → 记 LastError 后继续下一轮(改造前由调用方的读
// 循环 `continue`,现由泵就地消化,不外泄到 read_queue)。kTimeout/kCancelled 不会出现
// ——泵不带 deadline、不接令牌(它们是调用方在句柄上自理的事,ADR-0007 D4)。
void RunReadPump(const std::shared_ptr<DdsTransport::State>& state,
                 const std::shared_ptr<Coro::Awaitable<Datagram>>& read_queue) {
  const auto channel = read_queue->channel();
  for (;;) {
    Coro::Result<Sample> sample = state->handoff.Pop();
    if (!sample) {
      const auto error = sample.error();
      if (error == make_error_code(TransportErrc::kClosed)) {
        read_queue->close(make_error_code(TransportErrc::kClosed));
        return;
      }
      // 非终止失败:记为故障事实(kTimeout/kClosed/kCancelled 才是正常控制流结果,
      // 不稀释 LastError——同改造前 Read 的口径,ADR-0003 D13、RT_NODE_006)。
      if (error != make_error_code(TransportErrc::kTimeout) &&
          error != make_error_code(TransportErrc::kCancelled)) {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->last_error = error;
      }
      continue;
    }
    Datagram datagram;
    datagram.bytes = std::move(sample.value().bytes);
    datagram.peer = Endpoint::Topic(std::move(sample.value().topic));
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_recv = OperationOptions::Clock::now();
    }
    if (channel->push(std::move(datagram)) !=
        boost::fibers::channel_op_status::success) {
      return;  // read_queue 已关闭(我方 Close)→ 停止投递。
    }
  }
}

}  // namespace

DdsTransport::DdsTransport(std::unique_ptr<IDdsProvider> provider,
                           DdsConfig config, std::vector<std::string> topics,
                           std::size_t max_samples, std::size_t max_bytes,
                           ITraceSink* trace_sink)
    : state_(std::make_shared<State>(std::move(provider), std::move(config),
                                     std::move(topics), max_samples,
                                     max_bytes, trace_sink)) {}

DdsTransport::~DdsTransport() { BeginClose(state_); }

Coro::Result<void> DdsTransport::Start() {
  const auto state = state_;
  IDdsProvider* provider = nullptr;
  DdsConfig config;
  std::vector<std::string> topics;
  BoundedQueue<Sample> handoff = state->handoff;  // 共享句柄:供回调捕获(不捕获 State)。
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->lifecycle == LifecycleState::kRunning) {
      return Coro::Result<void>{};  // 幂等。
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

  if (Coro::Result<void> init = provider->Init(config); !init) {
    return init;
  }
  // 逐 topic 订阅:回调在 listener 线程构造 Sample 并**非阻塞** Push 交接边界。回调只
  // 捕获 handoff(BoundedQueue 共享句柄)与 topic,不引用 State——无引用环、迟到安全。
  for (const auto& topic : topics) {
    Coro::Result<void> sub = provider->Subscribe(
        topic, [handoff, topic](const std::vector<std::uint8_t>& bytes) mutable {
          // 满即 tail-drop(BoundedQueue 内部计数 dds_handoff_overflow),不阻塞 listener。
          (void)handoff.Push(Sample{bytes, topic});
        });
    if (!sub) {
      return sub;  // 订阅失败:上层可 RequestClose 收敛已订阅的 topic。
    }
  }

  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->lifecycle = LifecycleState::kRunning;
    read_queue = state->read_queue;
  }
  // 起转发泵(ADR-0007 D1):把交接边界的样本转成 Datagram 投 read_queue。句柄不留存:
  // 泵只触碰以 shared_ptr 持有的 State 与队列,故本类析构后仍安全收敛。
  Coro::makeTask([state, read_queue] { RunReadPump(state, read_queue); });
  return Coro::Result<void>{};
}

// 交出 read_queue 句柄(ADR-0007 D4):不返回数据,deadline/取消/扇出由调用方在句柄上
// 自理。未 Start 时给一个以 kInvalidState 关闭的句柄;关闭后 read_queue 已被以 kClosed
// 关闭,await 即得终止原因。
std::shared_ptr<Coro::Awaitable<Datagram>> DdsTransport::Read() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->lifecycle == LifecycleState::kCreated) {
    return ClosedDatagramQueue(make_error_code(TransportErrc::kInvalidState));
  }
  return state_->read_queue;
}

Coro::Result<void> DdsTransport::Write(SendUnit unit) {
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
  // (kDefault/kNet)对 DDS 无意义 → kInvalidArgument(调用契约错误,不算一次
  // Publish 尝试,不计入 LastError,同 UdpTransport 对早期寻址校验的处理)。
  if (unit.destination.kind != Endpoint::Kind::kTopic) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  if (!provider) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  Coro::Result<void> result = provider->Publish(unit.destination.topic, unit.bytes);
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (result) {
      state->last_send = Clock::now();
    } else {
      state->last_error = result.error();
    }
  }
  return result;
}

Coro::Result<void> DdsTransport::RequestClose() {
  BeginClose(state_);
  return Coro::Result<void>{};
}

Coro::Result<void> DdsTransport::WaitClosed(OperationOptions options) {
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

std::optional<DdsTransport::Clock::time_point> DdsTransport::LastSendTime()
    const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_send;
}

std::optional<DdsTransport::Clock::time_point>
DdsTransport::LastReceiveTime() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_recv;
}

std::error_code DdsTransport::LastError() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_error;
}

// DDS 的"链路可用"即 provider 已初始化且订阅全部完成:Start 只在 Init 与逐 topic
// Subscribe 全部成功后才落 Running(任一失败中途返回、不进 Running),故 Running +
// provider 存活是充要判据。
LinkState DdsTransport::CurrentLinkState() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return (state_->lifecycle == LifecycleState::kRunning && state_->provider)
             ? LinkState::kUp
             : LinkState::kDown;
}

}  // namespace transport
