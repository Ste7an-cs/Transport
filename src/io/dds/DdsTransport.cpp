#include "transport/io/dds/DdsTransport.hpp"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <utility>
#include <vector>

#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/io/dds/DdsProviderRegistry.hpp"

// DdsTransport.cpp — 见 .hpp。双队列样板的第四次跟进(ADR-0013),两处与三介质不同:
//   ① 读侧**没有泵 fiber**——provider 的 listener 在外来线程上直推 read_queue_(D2);
//   ② 写侧是**一条专属 OS 线程**而不是写泵 fiber——`Publish` park 的是线程(D3)。
//
// 全文只有一处允许阻塞:写线程里的 `provider_->Publish()`。其余每一处(尤其 listener)
// 都必须是"拿锁—动一下—放锁"的。

namespace transport {

namespace {

/// `domain_id` 的合法上界(**D12**)。DDS 规范下 domain 编号 0..232。
constexpr int kMaxDomainId = 232;

}  // namespace

DdsTransport::DdsTransport(DdsConfig config) : config_(std::move(config)) {}

DdsTransport::~DdsTransport() {
  (void)Close();
  WaitClosed();  // join 写线程 + Shutdown provider:返回即无人再触碰本对象。
}

// 配置校验(D12):**在 `Start()` 里一次性做**,非法一律 kConfiguration 并停在 Created。
// topic 不在这里判——配置里根本没有 topic(D16),端点合法性落在 `Declare*` 上。
Coro::Result<void> DdsTransport::ValidateConfig() const {
  if (config_.domain_id < 0 || config_.domain_id > kMaxDomainId) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (config_.provider.empty()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // 两项都是**转达给 DDS 的 QoS 参数**(D10),但零值/负值在 DDS 侧无意义:
  // `max_blocking_time` 为零 = `RELIABLE` 准入满时立刻失败,`liveliness_lease` 为零则
  // 判活恒判死。故**须为正**(D12),不设"0 = 禁用"这一档。
  if (config_.qos.max_blocking_time <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (config_.qos.liveliness_lease <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Coro::Result<void>{};
}

Coro::Result<void> DdsTransport::Start() {
  if (lifecycle_ == LifecycleState::kRunning) {
    return Coro::Result<void>{};  // 幂等 no-op,同三介质。
  }
  if (lifecycle_ != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidateConfig(); !valid) {
    SetLastError(valid.error());
    return valid.error();  // **停在 Created**:可改配后重试(RT_LIFECYCLE_007)。
  }
  // provider **按名从注册表取**(D12 的"已注册"就是这一步)。未注册即 kConfiguration:
  // 这不是 I/O 故障,是配置里写了个不存在的实现名。
  auto provider = DdsProviderRegistry::Create(config_.provider);
  if (!provider) {
    const std::error_code error = make_error_code(TransportErrc::kConfiguration);
    SetLastError(error);
    return error;
  }
  if (auto inited = provider->Init(config_); !inited) {
    // Init 失败同样**停在 Created**,且**不留半个 provider**——DDS 没有重连相位
    // (与三介质的"首次链路就绪失败不算启动失败"分歧:那三个有泵可以无限重试,
    // 而 participant 建不出来多半是配置问题,留着只会让后续每次 Publish 都失败)。
    SetLastError(inited.error());
    return inited.error();
  }
  provider_ = std::move(provider);
  lifecycle_ = LifecycleState::kRunning;

  // **端点一个都不建**:topic 由调用方在启动时逐项 Declare*(D15/D16)。
  write_thread_ = std::thread([this] { RunWriteThread(); });
  return Coro::Result<void>{};
}

// 专属写线程(D3)。**这是全文唯一允许阻塞的地方**:`Publish` 会 park 本线程,且**没有
// 上界**——Fast DDS 默认 `INTRAPROCESS_FULL`,同进程订阅方的 `on_data_available` 就跑在
// 本线程上,`max_blocking_time` 根本不参与。
//
// 由此本线程实际**兼跑同进程内所有对端的交付回调**;我方 listener 只做一次 push 故满足
// "快且不阻塞",但同进程内非本框架的慢订阅方会卡住整条写队列——那是部署面的约束。
void DdsTransport::RunWriteThread() {
  for (;;) {
    Datagram item;
    {
      std::unique_lock<std::mutex> lock(write_mutex_);
      write_cv_.wait(lock,
                     [this] { return write_stop_ || !write_queue_.empty(); });
      if (write_stop_) {
        return;  // Close 已置位并清空残留(不等刷出,同三介质)。
      }
      item = std::move(write_queue_.front());
      write_queue_.pop_front();
    }
    // 目的地只能是 topic:DDS 的寻址维度就是它,而配置里**没有默认 topic**(D16),
    // 故 `Endpoint::Default()` / `kNet` 在本介质上无从解析。**丢该条并只落 LastError**
    // ——与 UDP 解析不出目的地时的处置同形,不回传调用方(写是 fire-and-forget)。
    if (item.peer.kind != Endpoint::Kind::kTopic || item.peer.topic.empty()) {
      SetLastError(make_error_code(TransportErrc::kInvalidArgument));
      continue;
    }
    // 写出的一切结果(含 `RETCODE_TIMEOUT`)**不回传,只落 LastError()**——与三介质
    // 逐字相同。代价:背压信号被丢弃,调用方无从知道"这条因对端消费不过来而没发出去"。
    if (auto published = provider_->Publish(item.peer.topic, item.bytes);
        !published) {
      SetLastError(published.error());
    }
  }
}

// 交出 read_queue_ 句柄(ADR-0007 D4),与三介质完全一致。
std::shared_ptr<Coro::Awaitable<Datagram>> DdsTransport::AsyncRead() {
  if (lifecycle_ == LifecycleState::kCreated) {
    return ClosedQueue<Datagram>(make_error_code(TransportErrc::kInvalidState));
  }
  return read_queue_;
}

// 入队即返(ADR-0007 D3):**只判生命周期与入队**。目的地是否可解析由写线程判——契约只
// 允许本方法判这两件事,提前判会让"传输无关的调用方"拿到一个三介质都不会给的错误码。
//
// 队列**有界 1024 且满时静默丢最旧**(与三介质的 `write_queue_` 逐字相同)。写侧尤其需要
// 这个界:在途 `Publish` 的阻塞无上界,不设界则积压无上界。
Coro::Result<void> DdsTransport::AsyncWrite(Datagram datagram) {
  if (lifecycle_ == LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kClosed);
  }
  {
    std::lock_guard<std::mutex> lock(write_mutex_);
    if (write_stop_) {
      return make_error_code(TransportErrc::kClosed);
    }
    write_queue_.push_back(std::move(datagram));
    while (write_queue_.size() > kWriteQueueCapacity) {
      write_queue_.pop_front();  // 丢最旧,静默(DD-15):不计数、不归因。
    }
  }
  write_cv_.notify_one();
  return Coro::Result<void>{};
}

// 请求关闭(幂等,只发信号不等收敛):两处。
//
// | # | 停止的东西                    | 手段                                   |
// |---|-------------------------------|----------------------------------------|
// | ① | 交付(listener → read_queue_) | `CloseQueue` —— 此后 push 返 closed    |
// | ② | 写线程(停在 cv 上等数据)      | `write_stop_ = true` + `notify_all`     |
//
// **不在这里 `Shutdown()` provider**:它要等在途 `Publish` 跑完(无上界),而本方法契约是
// 受理即返。收敛整个落在 `WaitClosed()`。
Coro::Result<void> DdsTransport::Close() {
  if (lifecycle_ >= LifecycleState::kClosing) {
    return Coro::Result<void>{};  // 幂等。
  }
  const std::error_code closed = make_error_code(TransportErrc::kClosed);
  if (lifecycle_ == LifecycleState::kCreated) {
    lifecycle_ = LifecycleState::kClosed;  // 从未 Start:无线程可停、无 provider 可关。
    CloseQueue(read_queue_, closed);
    return Coro::Result<void>{};
  }
  lifecycle_ = LifecycleState::kClosing;

  // ① 停止交付。迟到的 listener 回调只会 push 进一条已关闭的队列(返 closed),它捕获的
  //    是分发端而**不是 `this`**,故不触碰已销毁的对象。
  CloseQueue(read_queue_, closed);
  {
    std::lock_guard<std::mutex> lock(write_mutex_);
    write_stop_ = true;    // ② 唤醒写线程的唯一阻塞点(等数据)。
    write_queue_.clear();  // 未发出的残留随之丢弃——同三介质,不等刷出。
  }
  write_cv_.notify_all();
  return Coro::Result<void>{};
}

// join 写线程,再 Shutdown provider。
//
// **最坏等待没有上界**(ADR-0013「明确接受的代价」7):`Close()` 落在一次在途 `Publish`
// 上时,那次写**打不断**——Fast DDS 3.6.1 的 `DataWriter` 上没有任何中止 `write()` 的
// 入口(实测 5 轮,`Shutdown()` 一次也没截断过它)。故等待时长 = 那次 `Publish` 自己跑完
// 所需的时间,而它由**同进程内最慢的那个订阅回调**决定,**不是**一个 `max_blocking_time`
// (实测 `max_blocking_time` 设 300ms 而 `Publish` 跑满 2000ms)。
//
// **顺序不能反**:先 join 才 `Shutdown()`。provider 内部已按在途计数守着 writer 的删除
// (`write()` 在锁外跑,直接删 writer 就是 use-after-free),我方 join 在前是同一条纪律的
// 外层保证——两道闩都要,不绕开任何一道。
void DdsTransport::WaitClosed() {
  if (joined_ || lifecycle_ == LifecycleState::kCreated) {
    return;  // 已汇合过,或从未 Start:无可汇合者。
  }
  joined_ = true;
  if (write_thread_.joinable()) {
    write_thread_.join();  // ← 无上界的那一等就在这里。
  }
  if (provider_) {
    provider_->Shutdown();  // 摘 reader/writer、销 participant;此刻已无在途写。
  }
  lifecycle_ = LifecycleState::kClosed;
}

// 端点声明(D15)。两个方法**都幂等**:注册里可能重复(同一 topic 既是订阅项、又是某条
// client 的应答 topic),幂等让调用方不必先去重。
//
// **一处已知缺口**:`IDdsProvider` 上没有与 `Subscribe` 对称的 writer 声明钩子(D13 明确
// 不增删该接口),故 `DeclareWriter` 只登记意图,真正的 `DataWriter` 仍由 provider 在首次
// `Publish` 时惰性建——D15「运行期无 DDS 端点创建」在**写侧尚未真正兑现**。
Coro::Result<void> DdsTransport::DeclareWriter(const std::string& topic) {
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (topic.empty()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  declared_writers_.insert(topic);
  return Coro::Result<void>{};
}

Coro::Result<void> DdsTransport::DeclareReader(const std::string& topic) {
  if (lifecycle_ != LifecycleState::kRunning) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (topic.empty()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (declared_readers_.count(topic) != 0) {
    return Coro::Result<void>{};  // 幂等:同 topic 重复声明直接成功。
  }
  // ★★ listener:**跑在 provider 的外来线程上,且可能就是别人的发布线程**(默认
  // `INTRAPROCESS_FULL`)。故这里**只做一次 push**——`lock` + `push_back` + `notify_all`,
  // 无等待路径,满时丢最旧也不阻塞。
  //
  // **不要在这里加任何东西**:不解码(`Message` 是 codec 之后的产物,而这里在 codec 之下)、
  // 不加锁等待、不打日志到慢 sink。任何一处阻塞都会当场卡住对端的整条写队列。
  //
  // 捕获的是**读队列的分发端**与 **topic 的副本**,**不捕获 `this`**:前者让迟到回调不触碰
  // 已销毁的对象,后者用来填 `Datagram.peer`——topic 不上线缆(D5),入站只能靠它带出。
  auto subscribed = provider_->Subscribe(
      topic, [channel = read_queue_->channel(), topic](
                 const std::vector<std::uint8_t>& bytes) {
        (void)channel->push(Datagram{bytes, Endpoint::Topic(topic)});
      });
  if (!subscribed) {
    SetLastError(subscribed.error());
    return subscribed.error();
  }
  declared_readers_.insert(topic);
  return Coro::Result<void>{};
}

bool DdsTransport::IsRunning() const {
  return lifecycle_ == LifecycleState::kRunning;
}

void DdsTransport::SetLastError(std::error_code error) {
  std::lock_guard<std::mutex> lock(error_mutex_);
  last_error_ = error;
}

std::error_code DdsTransport::LastError() const {
  std::lock_guard<std::mutex> lock(error_mutex_);
  return last_error_;
}

// 判活(D9)。**无状态成员,当场向 provider 取数算出**——与串口/TCP 的 `CurrentLinkState()`
// 同定位:统一的 I/O 事实查询,不面向业务调用方。
//
// `kEstablishing` 那约 240ms 的发现窗口**没有 DDS 原生事件**,是由我方状态(端点已声明 +
// matched 尚未 > 0)推出来的,这是 ADR-0013 已登记的代价 5。
LinkState DdsTransport::CurrentLinkState() const {
  if (lifecycle_ != LifecycleState::kRunning || !provider_) {
    return LinkState::kDown;  // 未 Start / 关闭中 / 已关闭。
  }
  if (declared_writers_.empty() && declared_readers_.empty()) {
    // 一个端点都没声明:此刻**确实收发不了任何字节**,报 kDown 比 kEstablishing 诚实
    // ——没有任何东西"正在建立"。
    return LinkState::kDown;
  }
  const DdsMatchedCount count = provider_->MatchedCount();
  if (count.matched > 0 && count.alive > 0) {
    return LinkState::kUp;
  }
  if (count.matched == 0) {
    return LinkState::kEstablishing;  // 端点已建,还没发现对端。
  }
  // matched > 0 但 alive == 0:对端端点还在,但已被 `AUTOMATIC_LIVELINESS` 判死。
  // **必须配 liveliness**——只靠 matched 时对端被硬杀要等 participant lease(默认 20s)
  // 才检出,期间谎报 kUp;配 2s 后 2.0s 检出(D9)。
  return LinkState::kDown;
}

}  // namespace transport
