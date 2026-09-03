#include "transport/node/DdsNode.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <QUuid>  // QUuid::createUuid —— correlation_id 的 uuid 半段(ADR-0013 D6)。

#include "await/awaitable.hpp"

#include "task/fibertask.h"  // Coro::makeTask —— 读-分发循环 fiber。

#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"

// DdsNode.cpp — 见 .hpp。DDS 特有语义内联于此(D10 红线):四组注册表、服务名 → 两个
// topic 的派生、两段式 correlation_id、Dispatcher 键 (topic, corr, kind)、单阶段
// 请求-响应、应答寻址。**派生只此一处实现**(`DeriveServiceTopics`)。生命周期
// (幂等 Start / 关闭仲裁 / join)由基类 NodeBase 承载,本类只填三个钩子。
//
// 入站只有一条通路——`Dispatcher` 按键投递(ADR-0009 D1)。本类不持有业务队列与 handler
// 消费者 fiber:入站业务由宿主 `Subscribe` 后在自己的 fiber 上消费。
//
// 本类**不触碰 transport 的生命周期**:不 Start、不 Close、不 WaitClosed,只在 `DoStart()`
// 里逐项 `Declare*`(D15:唯一建端点的地方),此外只借它的两条队列。

namespace transport {
namespace {

/// 节点 uuid(**D6**):`uuid_override` 非空则用它,为空才 `QUuid::createUuid()`。
///
/// **不自搓**(`random_device` 在 WSL 下质量存疑,且要自行论证碰撞率),**不引第三方 uuid
/// 库**(为一个字段引依赖不划算);`Qt5::Core` 本就 `PUBLIC` 链进 `transport`,不引入新依赖。
std::string MakeUuid(const std::string& uuid_override) {
  if (!uuid_override.empty()) {
    return uuid_override;  // 测试注入固定值,保住确定性可测。
  }
  return QUuid::createUuid().toString(QUuid::WithoutBraces).toStdString();
}

/// 单值型注册批(`Publishers` / `Subscribers`)的校验:**只有"topic 非空"一条**。
///
/// **先整批校验、再整批落地**——"整批生效或整批不生效"(**D16**)由这个顺序天然保证,
/// 不需要回滚代码:校验没过就一个字节都没写进注册表。
[[nodiscard]] Coro::Result<void> ValidatePlainBatch(
    const std::vector<std::string>& batch) {
  for (const auto& topic : batch) {
    if (topic.empty()) {
      return make_error_code(TransportErrc::kInvalidArgument);
    }
  }
  return Coro::Result<void>{};
}

/// 服务名派生出的两个 topic(**D6**)。
struct ServiceTopics {
  std::string request;  ///< `cfg.<服务名>.request`
  std::string reply;    ///< `cfg.<服务名>.response`
};

/// `cfg.` 是**固定字面前缀,不可配**(**D6**)——不是配置项,不许做成可注入的。
constexpr char kServicePrefix[] = "cfg.";
constexpr char kRequestSuffix[] = ".request";
constexpr char kReplySuffix[] = ".response";

/// 服务名 → 两个 topic 的**唯一**派生实现(**D6**)。
///
/// ★ **一处实现、处处调用**:客户端与服务端、注册面(`DoStart` 建端点)与调用面
///   (`RequestForResultDirect` / `ServeRequests` / `Reply` / `Subscribe` 的校验)**全都从
///   这一个函数取值**。「两侧不可能算歪」这个保证**全部依赖于此**——任何一处另写一遍
///   字符串拼接,保证当场失效。
///
/// **拼接是单射的,故不需要限制服务名的字符**:`.request` 与 `.response` 首字符不同、长度
/// 差 1,两者拼不到一起;不同服务名派生出的 topic 亦必不同。空串在注册处已拦。
[[nodiscard]] ServiceTopics DeriveServiceTopics(const std::string& service_name) {
  return ServiceTopics{kServicePrefix + service_name + kRequestSuffix,
                       kServicePrefix + service_name + kReplySuffix};
}

/// 反查:`request_topic` 是不是 `service_names` 里某个服务派生出来的请求 topic。
///
/// **仍走 `DeriveServiceTopics`,不另写"剥前缀去后缀"的解析器**——派生与反查一旦分成两份
/// 实现,就又有了两边算不到一处去的余地,而这正是本轮设计要根除的东西。
[[nodiscard]] std::optional<ServiceTopics> FindServiceByRequestTopic(
    const std::set<std::string>& service_names,
    const std::string& request_topic) {
  for (const auto& name : service_names) {
    ServiceTopics topics = DeriveServiceTopics(name);
    if (topics.request == request_topic) {
      return topics;
    }
  }
  return std::nullopt;
}

/// 服务名注册批(`Clients` / `Services`)的校验。派生化之后**只剩两条**:
///
/// | 检查 | 依据 |
/// |---|---|
/// | 服务名为空串 | 空名派生出的 `cfg..request` 无从表达"哪个服务" |
/// | 服务名已注册为**反向角色** | **唯一要拦的方向冲突**——自己请求自己,且 `corr` 由自己生成、`Dispatcher` **会真的匹配上**,形成毫无察觉的自问自答 |
///
/// **另外两条随派生化从根上消失**(**D16**,2026-09-02):"请求与应答同 topic"——派生出来
/// 必然不同;"同一请求 topic 跨批次配了两个不同应答 topic"——派生确定、同名必同值。留着
/// 是死代码。
///
/// **只拦这一种方向冲突。** 其余"同一 topic 上既有 writer 又有 reader"的组合只造成自收
/// 白干、不会误配,且可能是调用方有意为之(本地回环自测),**不拦**(**D16** / 代价 9)。
[[nodiscard]] Coro::Result<void> ValidateServiceNameBatch(
    const std::vector<std::string>& batch,
    const std::set<std::string>& opposite) {
  for (const auto& service_name : batch) {
    if (service_name.empty()) {
      return make_error_code(TransportErrc::kInvalidArgument);
    }
    if (opposite.count(service_name) != 0) {
      return make_error_code(TransportErrc::kInvalidArgument);
    }
  }
  return Coro::Result<void>{};
}

}  // namespace

DdsNode::DdsNode(DdsTransport& transport, std::unique_ptr<ICodec> codec,
                 DdsNodeConfig config)
    : transport_(transport),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // uuid **构造时生成一次、此后不变**(D6):它是"共用应答 topic 也能区分客户端"的
      // 全部根据,故不能每次请求重取。
      uuid_(MakeUuid(config_.uuid_override)),
      // 键提取函数:给出一条消息在三个匹配字段上的具体值。部分匹配(kAny)由 Dispatcher
      // 实现,本类不需要提供通配逻辑。`topic` 由读循环按来源填(D5:topic 不上线缆)。
      dispatcher_([](const Message& msg) {
        return std::make_tuple(msg.topic, msg.correlation_id, msg.kind);
      }) {}

// 析构即关闭并汇合:必须在**本类**析构体内做——基类析构时虚钩子已退回纯虚。
// Close 只发信号,WaitClosed 才 join,故两句缺一不可。
DdsNode::~DdsNode() {
  (void)Close();
  WaitClosed();
}

// ── 注册接口(D16)────────────────────────────────────────────────────────
//
// 四个方法同一副骨架:**判相位 → 整批校验 → 整批落地**。
//
// - **只允许 `Start()` 之前注册**:端点集合"启动即定型、运行期恒定",本设计**不引入
//   运行期动态端点**。故非 `Created` 一律 `kInvalidState`,不分"正在启动"与"已关闭"。
// - **可多次调用累加、重复项幂等去重**:落地用的是 `set`/`map` 的 insert,天然如此。
// - **整批生效或整批不生效**:校验在落地之前整批做完,故失败时一项都没落。

Coro::Result<void> DdsNode::RegisterPublishers(std::vector<std::string> topics) {
  if (CurrentLifecycle() != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidatePlainBatch(topics); !valid) {
    return valid;
  }
  publishers_.insert(topics.begin(), topics.end());
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::RegisterSubscribers(std::vector<std::string> topics) {
  if (CurrentLifecycle() != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidatePlainBatch(topics); !valid) {
    return valid;
  }
  subscribers_.insert(topics.begin(), topics.end());
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::RegisterClients(
    std::vector<std::string> service_names) {
  if (CurrentLifecycle() != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  // 反向角色是 `services_`:同一服务名既注册为 Clients 又注册为 Services = 自己请求自己。
  if (auto valid = ValidateServiceNameBatch(service_names, services_); !valid) {
    return valid;
  }
  clients_.insert(service_names.begin(), service_names.end());
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::RegisterServices(
    std::vector<std::string> service_names) {
  if (CurrentLifecycle() != LifecycleState::kCreated) {
    return make_error_code(TransportErrc::kInvalidState);
  }
  if (auto valid = ValidateServiceNameBatch(service_names, clients_); !valid) {
    return valid;
  }
  services_.insert(service_names.begin(), service_names.end());
  return Coro::Result<void>{};
}

// ── NodeBase 钩子 ────────────────────────────────────────────────────────

Coro::Result<void> DdsNode::DoStart() {
  // **四组全空由 `Start()` 判**(D12):topic 的合法性在注册那一刻就判完了,只剩这一条要
  // 等注册全部结束才知道——一个什么都不收不发的节点必是漏了注册。
  if (publishers_.empty() && subscribers_.empty() && clients_.empty() &&
      services_.empty()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // **唯一建端点的地方**(D15)。按四组注册逐项建**对应方向**的端点:一个 topic 上通常
  // 只需要一侧,建成对是浪费、还会招来自收。`Declare*` 幂等,故这里不必先去重
  // (同一 topic 可能既是订阅项、又是某条 client 的应答 topic)。
  //
  // **不启动 transport**:宿主已经启过。它若还没 Running,`Declare*` 会返 kInvalidState,
  // 本次 Start 随之失败并停在 Created ——注册表原样保留,启好传输再来一次即可。
  for (const auto& topic : publishers_) {
    if (auto declared = transport_.DeclareWriter(topic); !declared) {
      return declared;
    }
  }
  for (const auto& topic : subscribers_) {
    if (auto declared = transport_.DeclareReader(topic); !declared) {
      return declared;
    }
  }
  // 请求-响应两组存的是**服务名**,两个 topic 在此由**同一个派生函数**算出(D6)。
  for (const auto& service_name : clients_) {
    const ServiceTopics topics = DeriveServiceTopics(service_name);
    if (auto declared = transport_.DeclareWriter(topics.request); !declared) {
      return declared;  // cfg.<名>.request → Writer(发请求)。
    }
    if (auto declared = transport_.DeclareReader(topics.reply); !declared) {
      return declared;  // cfg.<名>.response → Reader(收应答)。
    }
  }
  for (const auto& service_name : services_) {
    const ServiceTopics topics = DeriveServiceTopics(service_name);
    if (auto declared = transport_.DeclareReader(topics.request); !declared) {
      return declared;  // cfg.<名>.request → Reader(收请求)。
    }
    if (auto declared = transport_.DeclareWriter(topics.reply); !declared) {
      return declared;  // cfg.<名>.response → Writer(发应答)。**首次应答不会丢**靠这一行。
    }
  }

  // 读侧取自己的一路订阅——各订阅者各得全量副本;写侧无句柄可取,直接调 AsyncWrite。
  rx_ = transport_.AsyncRead()->shared();
  // 本节点只 spawn 这一条 fiber。入站业务的消费 fiber 属宿主,由其自行 spawn 与 join。
  SpawnReadLoop();
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::DoClose() {
  // close 本节点这一路读订阅 → 读循环的 await 立即得到终止错误而退出。
  // **close 是整流传播的**(AsyncTask 417790c 起):源读队列与同一条传输上的其它订阅者
  // 一并终结。有意为之——节点关闭即读侧终结,宿主随后关传输。
  if (rx_) {
    rx_->close(make_error_code(TransportErrc::kClosed));
    rx_->channel()->discard_pending();
  }
  // 关闭全部订阅信箱并置终止标记。一举两得:令在途 `RequestForResultDirect` 恰好终结
  // 一次,同时**即入站订阅者的协作取消信号**(ADR-0009 D4)。
  dispatcher_.CloseAll(make_error_code(TransportErrc::kClosed));
  return Coro::Result<void>{};
}

void DdsNode::DoJoin() {
  // 让出式 join(FiberTask::get()):返回即意味着读循环已不再运行、不再触碰本对象。
  // 这里 join 的是本节点**全部**的内部工作单元——只此一条。**不 join transport 的写线程**:
  // 那是宿主的事,且它的最坏等待无上界,在 fiber 里 join 会阻塞整条线程。
  if (read_task_) {
    (void)read_task_->get();
  }
}

void DdsNode::SpawnReadLoop() {
  read_task_ = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([this] {
    while (true) {
      Coro::Result<Datagram, std::error_code> datagram = Coro::await(rx_);
      if (!datagram) {
        // 两种成因:我方 Close 关了订阅,或传输终结关了源队列。二者都该让节点关闭。
        break;
      }
      DecodeAndDispatch(datagram.value());
    }
    // 无条件调**公开的** Close():我方 Close 所致时是幂等空操作,传输终结所致时即自终。
    (void)Close();
  }));
}

void DdsNode::DecodeAndDispatch(const Datagram& datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    // 坏样本 / codec 语义错误:**丢弃**。观测面撤销后不再归因、不再记录,丢弃动作本身
    // 不变(ADR-0014 D1/D4)。
    return;
  }
  for (auto& msg : decoded.value()) {
    // **topic 不上线缆**(D5):它是 DDS 的寻址维度,入站只能由 `Datagram.peer` 带出。
    // 这两个字段同时也是 `Dispatcher` 键的第一位,故这一行是分发能成立的前提。
    msg.source = datagram.peer.topic;
    msg.topic = datagram.peer.topic;
    Dispatch(msg);
  }
}

void DdsNode::Dispatch(const Message& msg) {
  // **唯一投递路径**:交由 Dispatcher 按键投递,命中的订阅者各得一份副本(ADR-0009 D1)。
  if (dispatcher_.Dispatch(msg) > 0) {
    return;
  }
  // 无人认领的 `kReply` 是迟到、乱序,或**别人的应答**——共用应答 topic 之下,同一服务的
  // 每个客户端都会收到该服务的全部应答,自己那份只是其中之一(代价 8);业务消息无人订阅
  // 则是宿主的正常选择(ADR-0009 D5)。**两者的处置相同:丢弃。** 观测面随 ADR-0014 D1
  // 撤销后框架已无处记录二者之别,故此处不再分支;代价(D4)是丢弃完全不可见,已明确接受。
}

// ── 公开面:两种交互模式(D8)──────────────────────────────────────────

Coro::Result<DdsNode::Ticket> DdsNode::Subscribe(TopicKey topic, KindKey kind) {
  // **相位判定先于配置校验**,与另外三个交互方法同序(调用序错误先于配置错误);判据也
  // **与它们同一个** `IsRunning()`——`kClosed` 一并覆盖"未启动 / 关闭中 / 已关闭",这是
  // `ProtocolNode` 已经写进公开 `@return` 的既有约定,本方法不单开一份。
  //
  // - **`Created` 也返 `kClosed`**:`Subscribe` **只在 `Running` 受理**,还没 `Start()` 就
  //   订阅是**禁用法**,不是"早一点也行"。放行它有一处真实危害:`NodeBase::Close()` 从
  //   `Created` 走时**不调 `DoClose()`**,`dispatcher_.CloseAll` 因此从不执行——宿主若在
  //   此相位订阅并 spawn 了消费 fiber、随后放弃启动,那条 fiber 的信箱**永远等不到关闭
  //   信号**,join 时挂住。
  //   而它本要换来的"不漏收启动初期消息"是空的:`DataReader` 建于 `DoStart()`,**DDS 发现
  //   约 240ms**,`Start()` 返回之后的头 ~240ms 对端根本还没 match,一条样本也到不了。
  //   宿主得在 `Start()` 与 `Subscribe()` 之间干超过 240 毫秒的事才谈得上丢,而
  //   `Start(); Subscribe();` 这样的正常写法离那个边界差着几个数量级。
  // - **`Closing` / `Closed` 同样 `kClosed`**:此时 `DoClose()` 已 `CloseAll`,再登记只能
  //   得到一张信箱已关闭的凭据。让它**在返回处**就说清楚,而不是推迟到第一次 `Wait`——D8
  //   把本方法从裸 `Ticket` 改成 `Coro::Result<Ticket>` 的理由原样适用于相位。
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);  // 未启动 / 关闭中 / 已关闭。
  }
  // **topic 传 `kAny` 时跳过校验**(D16):`kAny` 不对应任何一个具体 topic,拿它去查注册表
  // 必然落空。这不是网开一面——它的作用域本就已由注册天然限定("已注册为 reader 的
  // topic 的全部",而不是"本 domain 上的全部")。
  if (topic.has_value()) {
    const std::string& name = topic.value();
    bool registered = false;
    if (kind.has_value() && kind.value() == MessageKind::kNotify) {
      registered = subscribers_.count(name) != 0;  // 发布-订阅的订阅侧。
    } else if (kind.has_value() && kind.value() == MessageKind::kRequest) {
      // 请求-响应的服务端收请求:第一参**仍然是 topic**,须是某个已注册服务派生出的请求
      // topic。`ServeRequests(名)` 正是从这条路进来的——**本方法不为 kind 改参数含义**。
      registered = FindServiceByRequestTopic(services_, name).has_value();
    } else {
      // 其余 kind(含 kind 传 kAny):**至少**得在读侧集合内——不在读侧的 topic 其消息
      // 根本不会到达本进程,订阅它必然是静默无效,正是 D16 要消灭的那种失败。
      registered = IsReaderSideTopic(name);
    }
    if (!registered) {
      return make_error_code(TransportErrc::kConfiguration);
    }
  }
  // ★ **交出去的订阅其 corr 位恒为 `kAny`**(D6)——与 `RequestForResultDirect` 内部登记的
  // 那一条(用具体 corr)恰成对照。`correlation_id` 不进公开接口。
  return Coro::Result<Ticket>{
      dispatcher_.Subscribe({std::move(topic), kAny, std::move(kind)})};
}

Coro::Result<void> DdsNode::Publish(const std::string& topic, Message msg) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // **调用序错误先于配置错误**:上面先判了生命周期,这里才判注册。
  if (publishers_.count(topic) == 0) {
    return make_error_code(TransportErrc::kConfiguration);  // 不猜、不回落、不懒补。
  }
  msg.kind = MessageKind::kNotify;
  msg.topic = topic;
  // D6 之后 `correlation_id` **只有框架生成的关联符一个来源**,发布路径上它没有第二种
  // 用法("应用自定义子通道"已裁决为不需要);`reply_to` 同理——本调用不期待应答。
  msg.correlation_id.clear();
  msg.reply_to.clear();
  return EncodeAndWrite(msg, topic);
}

Coro::Result<Message> DdsNode::RequestForResultDirect(
    const std::string& service_name, Message req, RetryPolicy retry) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 时限是在途交互唯一的兜底终结源(写出是 fire-and-forget),故不接受"零即永不超时";
  // 次数含首发,少于一次意味着一帧都不发。
  if (retry.max_attempts < 1 ||
      retry.timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  // **第一参是服务名**(D8):查它有没有注册为 `Clients`,**查不到即 kConfiguration,不猜、
  // 不回落**(D6)。这让"忘了注册"从一个静默无效变成一个显式错误。
  if (clients_.count(service_name) == 0) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // 两个 topic 在此派生——**与服务端建端点时调的是同一个函数**,故两侧必然算到一处去。
  const ServiceTopics topics = DeriveServiceTopics(service_name);
  const std::string& request_topic = topics.request;
  const std::string& reply_topic = topics.reply;

  const std::string correlation_id = NextCorrelationId();
  req.kind = MessageKind::kRequest;
  req.correlation_id = correlation_id;
  // `reply_to` 上线缆,供服务端做**一致性交叉校验**(D15)。派生化之后两侧配歪已不可能,
  // 它剩下的用途是对**版本不一致的对端**(派生规则将来若变更)当场报出偏差。
  req.reply_to = reply_topic;
  req.topic = request_topic;

  // **编码一次**,重发复用同一份字节(ADR-0010 D3:重发的是字节完全相同的原帧,
  // `correlation_id` 不变,故订阅横跨全部重发继续有效)。
  auto encoded = codec_->Encode(req);
  if (!encoded) {
    return encoded.error();
  }
  const std::vector<std::uint8_t> bytes = std::move(encoded).value();

  // **先登记订阅、再发出**——这是 `Dispatcher` 用法的固有要求:反之则应答可能先于订阅
  // 登记到达而被丢弃。
  //
  // ★ 这条登记的 corr 用的是**具体值**,不是 `kAny`。共用应答 topic 之所以能区分客户端,
  //   全靠这一点:该 topic 上别人的应答带着别人的 corr,与本条不匹配,落到"无订阅者"
  //   而被丢弃。若这里也用 `kAny`,本客户端会匹配上该 topic 上**所有人**的应答。
  auto result = dispatcher_.Subscribe(
      {reply_topic, correlation_id, MessageKind::kReply});

  for (int attempt = 0; attempt < retry.max_attempts; ++attempt) {
    if (auto queued = WriteEncoded(bytes, request_topic); !queued) {
      return queued.error();  // 生命周期非法——不属超时,不重试。
    }
    auto got = result.Wait(retry.timeout);
    if (got) {
      return got;  // 首个到达者即终结本次交互,**不回应任何帧**(D7)。
    }
    if (got.error() != make_error_code(TransportErrc::kTimeout)) {
      return got.error();  // kClosed 等终止原因直接透出,重试无意义。
    }
    // 超时 → 重发。**本模型恰恰要在等结果阶段重发**(D7):丢的不是网络(DDS 是
    // RELIABLE 的),是我方或对端的**本地队列**——那一段 RELIABLE 覆盖不到。
  }
  // **耗尽返 kTimeout,不是 kNotAccepted**(D7 / ADR-0010 D12):后者的语义是"对端没有
  // 受理",而本模型根本不存在受理这一步。
  return make_error_code(TransportErrc::kTimeout);
}

Coro::Result<DdsNode::Ticket> DdsNode::ServeRequests(
    const std::string& service_name) {
  // **是 `Subscribe` 在服务名一侧的封装,不是另一套机制**(D8):派生出请求 topic 之后原样
  // 交给它,相位与注册两道校验都落在那里。`Subscribe` 由此得以**保持通用**——它的第一参
  // 永远是 topic,不需要为 `kind` 分叉出"这个参数其实是服务名"的分支。
  //
  // 空服务名走到这里也无妨:`cfg..request` 永远注册不上,`Subscribe` 报 kConfiguration。
  return Subscribe(DeriveServiceTopics(service_name).request,
                   MessageKind::kRequest);
}

Coro::Result<void> DdsNode::Reply(const Message& request, Message result) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // **应答目的地由自己注册的服务反查,不取信于线缆、不建端点**(D15):`request.topic` 是
  // 派生出来的 `cfg.<名>.request`,反查同样走 `DeriveServiceTopics`(不另写解析器)。
  const auto service = FindServiceByRequestTopic(services_, request.topic);
  if (!service.has_value()) {
    return make_error_code(TransportErrc::kConfiguration);  // 我根本不服务这个 topic。
  }
  const std::string& reply_topic = service->reply;
  // 线缆上的 `reply_to` 降为**一致性交叉校验**:非空且与查出的不等即报错。这 20 来个
  // 字节买的是"两侧注册实参写歪"这一类部署错误的可诊断性。
  if (!request.reply_to.empty() && request.reply_to != reply_topic) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  result.kind = MessageKind::kReply;
  result.correlation_id = request.correlation_id;  // 关联符沿用请求那一份。
  result.reply_to.clear();                         // 应答不再期待应答。
  result.topic = reply_topic;
  return EncodeAndWrite(result, reply_topic);
}

// ── 私有 ────────────────────────────────────────────────────────────────

std::string DdsNode::NextCorrelationId() {
  // 两段式(D6):uuid 保证**跨节点**不撞,自增半段保证**节点内**不撞。
  // `uint32` 回绕(约 42.9 亿次请求后)**明确接受**——届时重复的是本节点很久以前用过的
  // 值,那条订阅早已注销,`Dispatcher` 里已无对应登记,不会误配。
  return uuid_ + "#" + std::to_string(request_seq_++);
}

bool DdsNode::IsReaderSideTopic(const std::string& topic) const {
  // 读侧 = Subscribers ∪ 各服务的 cfg.<名>.request ∪ 各客户端的 cfg.<名>.response
  // (D16 的判据,与代价 9 的 "reader 侧"逐字相同,只是两个 topic 现在是派生出来的)。
  if (subscribers_.count(topic) != 0) {
    return true;
  }
  if (FindServiceByRequestTopic(services_, topic).has_value()) {
    return true;
  }
  for (const auto& service_name : clients_) {
    if (DeriveServiceTopics(service_name).reply == topic) {
      return true;
    }
  }
  return false;
}

Coro::Result<void> DdsNode::EncodeAndWrite(const Message& msg,
                                            const std::string& topic) {
  auto encoded = codec_->Encode(msg);
  if (!encoded) {
    return encoded.error();
  }
  return WriteEncoded(std::move(encoded).value(), topic);
}

Coro::Result<void> DdsNode::WriteEncoded(std::vector<std::uint8_t> bytes,
                                          const std::string& topic) {
  // fire-and-forget:返回成功只表示"已入队",不表示已发出;写出的一切结果不回传,只落
  // 传输的 `LastError()`。这里能拿到的错误只有生命周期非法一种。
  //
  // 目的地恒是 `Endpoint::Topic`——DDS 的寻址维度就是 topic,配置里也没有默认对端。
  if (auto queued =
          transport_.AsyncWrite(Datagram{std::move(bytes), Endpoint::Topic(topic)});
      !queued) {
    return queued;
  }
  return Coro::Result<void>{};
}

}  // namespace transport
