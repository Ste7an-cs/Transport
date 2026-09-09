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
// 入站只有一条通路——`Dispatcher` 按键投递(ADR-0009 D1):入站业务由宿主 `Subscribe`
// 后在自己的 fiber 上消费。
//
// 本类**不触碰 transport 的生命周期**:不 Start、不 Close、不 WaitClosed,只在 `DoStart()`
// 里逐项 `Declare*`(D15:唯一建端点的地方),此外只借它的两条队列。

namespace transport {
namespace {

/// 节点 uuid(**D6**):`uuid_override` 非空则用它,为空才 `QUuid::createUuid()`。
///
/// **不自搓**、**不引第三方 uuid 库**;`Qt5::Core` 本就 `PUBLIC` 链进 `transport`,不引入新依赖。
std::string MakeUuid(const std::string& uuid_override) {
  if (!uuid_override.empty()) {
    return uuid_override;  // 测试注入固定值,保住确定性可测。
  }
  return QUuid::createUuid().toString(QUuid::WithoutBraces).toStdString();
}

/// 服务名派生出的两个 topic(**D6**)。
struct ServiceTopics {
  std::string request;  ///< `cfg.<服务名>.request`
  std::string reply;    ///< `cfg.<服务名>.response`
};

/// `cfg.` 是**固定字面前缀,不可配**(**D6**)。
constexpr char kServicePrefix[] = "cfg.";
constexpr char kRequestSuffix[] = ".request";
constexpr char kReplySuffix[] = ".response";

/// 单值型注册批(`Publishers` / `Subscribers`)的校验:**topic 非空**,且**不得以 `cfg.`
/// 开头**(ADR-0015 **D3**)。
///
/// ★ 后一条是「注销可以直接拆端点、不必重算」的**全部依据**。派生项之间不可能相撞(**D6**
///   的单射性论证:`.request` / `.response` 后缀天然分开两侧,同侧同后缀则服务名不同即不同,
///   同名既 client 又 service 已被 `ValidateServiceNameBatch` 拒),故端点重叠的**唯一**
///   来源就是调用方往这两个方法里手写一个 `cfg.` 开头的字符串。拦掉它,「一条端点恰有一个
///   注册项负责」就从"靠调用方守约定"变成**可证的结构性质**——不是"约定它不会发生所以省掉
///   重算",而是"它不可能发生所以不需要重算"。
///
/// **这是破坏性变更**:`RegisterSubscribers({"cfg.get.request"})` 此前合法(ADR-0013 D16
/// 只在文档里标注该命名空间被占用、框架不拦),现在返 `kInvalidArgument`。代价见 ADR-0015
/// 「明确接受的代价」④。**服务名不受此限**——`RegisterClients({"cfg.x"})` 仍合法,它派生出
/// 的是 `cfg.cfg.x.request`,与任何普通 topic 都撞不上。
///
/// **先整批校验、再整批落地**——"整批生效或整批不生效"(**D16**)由这个顺序保证。
[[nodiscard]] Coro::Result<void> ValidatePlainBatch(
    const std::vector<std::string>& batch) {
  const std::string prefix = kServicePrefix;
  for (const auto& topic : batch) {
    if (topic.empty()) {
      return make_error_code(TransportErrc::kInvalidArgument);
    }
    if (topic.compare(0, prefix.size(), prefix) == 0) {
      return make_error_code(TransportErrc::kInvalidArgument);  // D3。
    }
  }
  return Coro::Result<void>{};
}

/// 服务名 → 两个 topic 的**唯一**派生实现(**D6**)。
///
/// ★ **一处实现、处处调用**:客户端与服务端、注册面(`DoStart` 建端点)与调用面
///   (`RequestForResultDirect` / `ServeRequests` / `Reply` / `Subscribe` 的校验)**全都从
///   这一个函数取值**——任何一处另写一遍字符串拼接,“两侧不可能算歪”当场失效。
///
/// **拼接是单射的,故不需要限制服务名的字符**;空串在注册处已拦。
[[nodiscard]] ServiceTopics DeriveServiceTopics(const std::string& service_name) {
  return ServiceTopics{kServicePrefix + service_name + kRequestSuffix,
                       kServicePrefix + service_name + kReplySuffix};
}

/// 反查:`request_topic` 是不是 `service_names` 里某个服务派生出来的请求 topic。
///
/// **仍走 `DeriveServiceTopics`,不另写“剥前缀去后缀”的解析器**——派生与反查分成两份实现
/// 就又有了两边算不到一处去的余地。
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

/// 服务名注册批(`Clients` / `Services`)的校验,**只有两条**:
///
/// | 检查 | 依据 |
/// |---|---|
/// | 服务名为空串 | 空名派生出的 `cfg..request` 无从表达“哪个服务” |
/// | 服务名已注册为**反向角色** | 自己请求自己,且 `corr` 由自己生成、`Dispatcher` **会真的匹配上**,形成毫无察觉的自问自答 |
///
/// **只拦这一种方向冲突。** 其余“同一 topic 上既有 writer 又有 reader”的组合只造成自收
/// 白干、不会误配,且可能是调用方有意为之(本地回环自测),**不拦**(**D16**)。
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

// ── 端点派生:**唯一**一份实现,`DoStart()` 与运行期动态路径共用(ADR-0015 D1)────
//
// D1 要求"不写两份派生逻辑":`DoStart()` 里原来那四段 `for` 的循环体就是下面这个
// `DeclareEndpointsFor`,`DoStart()` 现在只负责遍历,动态注册直接对单项调它。

/// 四组注册各自代表的角色——决定该项派生出哪个方向的端点。
enum class RegistrationKind {
  kPublisher,   ///< topic → Writer。
  kSubscriber,  ///< topic → Reader。
  kClient,      ///< 服务名 → request 的 Writer(发请求)+ response 的 Reader(收应答)。
  kService,     ///< 服务名 → request 的 Reader(收请求)+ response 的 Writer(发应答)。
};

/// 拆掉某个注册项派生出的全部端点(**幂等**,故也可以拿来清理建了一半的项)。
///
/// **不做任何"是否仍被需要"的重算**(**D3**):端点归属唯一是可证性质,该项负责的端点
/// 只有该项要。
void UndeclareEndpointsFor(DdsTransport& transport, RegistrationKind kind,
                           const std::string& item) {
  switch (kind) {
    case RegistrationKind::kPublisher:
      (void)transport.UndeclareWriter(item);
      break;
    case RegistrationKind::kSubscriber:
      (void)transport.UndeclareReader(item);
      break;
    case RegistrationKind::kClient: {
      const ServiceTopics topics = DeriveServiceTopics(item);
      (void)transport.UndeclareWriter(topics.request);
      (void)transport.UndeclareReader(topics.reply);
      break;
    }
    case RegistrationKind::kService: {
      const ServiceTopics topics = DeriveServiceTopics(item);
      (void)transport.UndeclareReader(topics.request);
      (void)transport.UndeclareWriter(topics.reply);
      break;
    }
  }
}

/// 建出某个注册项对应方向的端点。**端点方向的派生规则一字不改**(ADR-0013 D15/D6)。
///
/// 失败时**先拆掉本项已建的那半边**再返错:一个注册项要么整项在、要么整项不在,免得留下
/// 一条谁也不负责的端点。`Declare*` 幂等,故重试无副作用。
[[nodiscard]] Coro::Result<void> DeclareEndpointsFor(DdsTransport& transport,
                                                     RegistrationKind kind,
                                                     const std::string& item) {
  switch (kind) {
    case RegistrationKind::kPublisher:
      return transport.DeclareWriter(item);
    case RegistrationKind::kSubscriber:
      return transport.DeclareReader(item);
    case RegistrationKind::kClient: {
      // 请求-响应两组存的是**服务名**,两个 topic 由**同一个派生函数**算出(D6)。
      const ServiceTopics topics = DeriveServiceTopics(item);
      if (auto declared = transport.DeclareWriter(topics.request); !declared) {
        return declared;  // cfg.<名>.request → Writer(发请求)。
      }
      if (auto declared = transport.DeclareReader(topics.reply); !declared) {
        UndeclareEndpointsFor(transport, kind, item);  // 半边不留。
        return declared;  // cfg.<名>.response → Reader(收应答)。
      }
      return Coro::Result<void>{};
    }
    case RegistrationKind::kService: {
      const ServiceTopics topics = DeriveServiceTopics(item);
      if (auto declared = transport.DeclareReader(topics.request); !declared) {
        return declared;  // cfg.<名>.request → Reader(收请求)。
      }
      if (auto declared = transport.DeclareWriter(topics.reply); !declared) {
        UndeclareEndpointsFor(transport, kind, item);
        return declared;  // cfg.<名>.response → Writer(发应答)。
      }
      return Coro::Result<void>{};
    }
  }
  return Coro::Result<void>{};  // 不可达:枚举已穷举。
}

/// 一批**已通过校验**的注册项落地(**D7**)。
///
/// | 相位 | 做什么 |
/// |---|---|
/// | `Created` | 只往 `target` 里插——端点仍由 `DoStart()` 统一建(与 ADR-0015 之前逐字相同) |
/// | `Running` | **逐项建端点 → 提交集合**;中途失败**拆掉本批已建的**并返错,`target` 一项不落 |
///
/// **回滚清单只记"本批真正新建的项"**:批里那些早已在册的项,其端点是先前建的、且此刻仍被
/// 那份注册需要,拆掉就是误伤。
[[nodiscard]] Coro::Result<void> CommitBatch(DdsTransport& transport,
                                             bool running,
                                             RegistrationKind kind,
                                             const std::vector<std::string>& batch,
                                             std::set<std::string>* target) {
  if (!running) {
    target->insert(batch.begin(), batch.end());  // Created 期:只落表。
    return Coro::Result<void>{};
  }
  std::set<std::string> created;  // 本批新建了端点的项 —— 回滚只拆这些。
  for (const auto& item : batch) {
    if (target->count(item) != 0 || created.count(item) != 0) {
      continue;  // 已在册 / 批内重复:端点已经有了,不重复建、更不进回滚清单。
    }
    if (auto declared = DeclareEndpointsFor(transport, kind, item); !declared) {
      for (const auto& done : created) {
        UndeclareEndpointsFor(transport, kind, done);
      }
      return declared;  // 注册表一项不落——`target` 到这一步还没被动过。
    }
    created.insert(item);
  }
  target->insert(batch.begin(), batch.end());  // 全部建成了才提交集合。
  return Coro::Result<void>{};
}

/// 一批注销项落地:从集合摘除,`Running` 期同时拆端点(**D2**)。
///
/// **不在册的项是幂等空操作**,不报错——注销的语义是"确保它不在",而不是"它此刻必须在"。
/// **不检测在途交互**(**D6**):已发出的 `Ticket` 挂在 `Dispatcher` 上、与注册表无关,继续
/// 有效;新请求不再到达;此后对该服务的 `Reply` / `RequestForResultDirect` 返
/// `kConfiguration`(那两处本就查注册表)。
void ApplyUnregister(DdsTransport& transport, bool running,
                     RegistrationKind kind, const std::vector<std::string>& batch,
                     std::set<std::string>* target) {
  for (const auto& item : batch) {
    if (target->erase(item) == 0) {
      continue;  // 本就不在册:什么都不做(端点也不是本节点这一项建的)。
    }
    if (running) {
      UndeclareEndpointsFor(transport, kind, item);
    }
  }
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

// ── 注册 / 注销接口(D16 + ADR-0015 D1/D2)────────────────────────────────
//
// 八个方法同一副骨架:**判相位 → 整批校验 → 整批落地**。
//
// - **相位是 `Created ∪ Running`**(ADR-0015 **D1**):`Created` 期只落表(端点仍由
//   `DoStart()` 统一建,与放开之前逐字相同),`Running` 期落表**并当场建端点**;
//   `Closing` / `Closed` 返 **`kClosed`**(注意:不是 `kInvalidState`——已关闭的节点上
//   注册是"关了"而不是"时候不对",与五个交互方法同一口径)。
// - **可多次调用累加、重复项幂等去重**:落地用的是 `set` 的 insert,天然如此。
// - **整批生效或整批不生效**:`Created` 期由"校验先于落地"保证;`Running` 期还多一步
//   **拆掉本批已建端点**的回滚(**D7**,见 `CommitBatch`)。

/// 八个方法共用的相位判据(**D1**):`Created` / `Running` 放行并告知是不是 `Running`,
/// `Closing` / `Closed` 返 `kClosed`。
Coro::Result<bool> DdsNode::RegistrationPhase() const {
  switch (CurrentLifecycle()) {
    case LifecycleState::kCreated:
      return Coro::Result<bool>{false};
    case LifecycleState::kRunning:
      return Coro::Result<bool>{true};
    default:
      return make_error_code(TransportErrc::kClosed);
  }
}

Coro::Result<void> DdsNode::RegisterPublishers(std::vector<std::string> topics) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  if (auto valid = ValidatePlainBatch(topics); !valid) {
    return valid;
  }
  return CommitBatch(transport_, running.value(), RegistrationKind::kPublisher,
                     topics, &publishers_);
}

Coro::Result<void> DdsNode::RegisterSubscribers(std::vector<std::string> topics) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  if (auto valid = ValidatePlainBatch(topics); !valid) {
    return valid;
  }
  return CommitBatch(transport_, running.value(), RegistrationKind::kSubscriber,
                     topics, &subscribers_);
}

Coro::Result<void> DdsNode::RegisterClients(
    std::vector<std::string> service_names) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  // 反向角色是 `services_`:同一服务名既注册为 Clients 又注册为 Services = 自己请求自己。
  if (auto valid = ValidateServiceNameBatch(service_names, services_); !valid) {
    return valid;
  }
  return CommitBatch(transport_, running.value(), RegistrationKind::kClient,
                     service_names, &clients_);
}

Coro::Result<void> DdsNode::RegisterServices(
    std::vector<std::string> service_names) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  if (auto valid = ValidateServiceNameBatch(service_names, clients_); !valid) {
    return valid;
  }
  return CommitBatch(transport_, running.value(), RegistrationKind::kService,
                     service_names, &services_);
}

// 四个注销方法(**D2**)。与对应的注册方法同形、同相位;**没有校验**——不在册即幂等空
// 操作,故也没有"整批不生效"可言(摘除不会失败)。
//
// **不检测在途交互**(**D6**):不拒绝、不等待。见 `ApplyUnregister` 的注释。

Coro::Result<void> DdsNode::UnregisterPublishers(std::vector<std::string> topics) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  ApplyUnregister(transport_, running.value(), RegistrationKind::kPublisher,
                  topics, &publishers_);
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::UnregisterSubscribers(
    std::vector<std::string> topics) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  ApplyUnregister(transport_, running.value(), RegistrationKind::kSubscriber,
                  topics, &subscribers_);
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::UnregisterClients(
    std::vector<std::string> service_names) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  ApplyUnregister(transport_, running.value(), RegistrationKind::kClient,
                  service_names, &clients_);
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::UnregisterServices(
    std::vector<std::string> service_names) {
  auto running = RegistrationPhase();
  if (!running) {
    return running.error();
  }
  ApplyUnregister(transport_, running.value(), RegistrationKind::kService,
                  service_names, &services_);
  return Coro::Result<void>{};
}

// ── NodeBase 钩子 ────────────────────────────────────────────────────────

Coro::Result<void> DdsNode::DoStart() {
  // **不再有"四组全空即 kConfiguration"这条判据**(ADR-0015 **D5** 撤销 ADR-0013 D12):
  // 它当初的依据是"一个什么都不收不发的节点必是漏了注册",而注册可以在启动之后补上之后
  // 该依据不再成立——"启动时还不知道有哪些 topic"正是动态注册要支持的主要场景。
  //
  // 按四组注册逐项建**对应方向**的端点:一个 topic 上通常只需要一侧,建成对是浪费、还会
  // 招来自收。`Declare*` 幂等,故这里不必先去重。
  //
  // ★ **与运行期动态注册共用同一个 `DeclareEndpointsFor`**(**D1**):本函数只负责遍历,
  //   方向派生一份实现,两条路径不可能走歪到两处去。
  //
  // **不启动 transport**:宿主已经启过。它若还没 Running,`Declare*` 会返 kInvalidState,
  // 本次 Start 随之失败并停在 Created ——注册表原样保留,启好传输再来一次即可。
  for (const auto& topic : publishers_) {
    if (auto declared = DeclareEndpointsFor(transport_,
                                            RegistrationKind::kPublisher, topic);
        !declared) {
      return declared;
    }
  }
  for (const auto& topic : subscribers_) {
    if (auto declared = DeclareEndpointsFor(transport_,
                                            RegistrationKind::kSubscriber, topic);
        !declared) {
      return declared;
    }
  }
  // 请求-响应两组存的是**服务名**,两个 topic 由**同一个派生函数**算出(D6)。
  for (const auto& service_name : clients_) {
    if (auto declared = DeclareEndpointsFor(transport_, RegistrationKind::kClient,
                                            service_name);
        !declared) {
      return declared;
    }
  }
  for (const auto& service_name : services_) {
    // `cfg.<名>.response` 的 Writer 也在这里建出——**启动时就注册了的服务,其第一次应答
    // 不会丢**(D15 的那条实测事实)。动态注册出来的服务没有这条保障,见 ADR-0015
    // 「明确接受的代价」①:风险由宿主自行评估处置。
    if (auto declared = DeclareEndpointsFor(transport_,
                                            RegistrationKind::kService, service_name);
        !declared) {
      return declared;
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
    // 坏样本 / codec 语义错误:**丢弃**,不归因、不记录(ADR-0014 D1/D4)。
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
  // 每个客户端都会收到该服务的全部应答,自己那份只是其中之一;业务消息无人订阅则是宿主
  // 的正常选择(ADR-0009 D5)。**两者的处置相同:丢弃,且不作记录**(ADR-0014 D1/D4)。
}

// ── 公开面:两种交互模式(D8)──────────────────────────────────────────

Coro::Result<DdsNode::Ticket> DdsNode::Subscribe(TopicKey topic, KindKey kind) {
  // **相位判定先于配置校验**,与另外三个交互方法同序(调用序错误先于配置错误);判据也
  // **与它们同一个** `IsRunning()`——`kClosed` 一并覆盖“未启动 / 关闭中 / 已关闭”。
  // `Subscribe` **只在 `Running` 受理**,还没 `Start()` 就订阅是**禁用法**;`Closing` /
  // `Closed` 期 `DoClose()` 已 `CloseAll`,再登记只能得到一张信箱已关闭的凭据。
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
  // `correlation_id` **只有框架生成的关联符一个来源**(D6),发布路径上不使用它;
  // `reply_to` 同理——本调用不期待应答。
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
  // 两个 topic 在此派生——与服务端建端点时调的是**同一个函数**。
  const ServiceTopics topics = DeriveServiceTopics(service_name);
  const std::string& request_topic = topics.request;
  const std::string& reply_topic = topics.reply;

  const std::string correlation_id = NextCorrelationId();
  req.kind = MessageKind::kRequest;
  req.correlation_id = correlation_id;
  // `reply_to` 上线缆,供服务端做**一致性交叉校验**(D15)。
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
  // **耗尽返 kTimeout,不是 kNotAccepted**(D7 / ADR-0010 D12):本模型没有受理这一步。
  return make_error_code(TransportErrc::kTimeout);
}

Coro::Result<DdsNode::Ticket> DdsNode::ServeRequests(
    const std::string& service_name) {
  // **是 `Subscribe` 在服务名一侧的封装,不是另一套机制**(D8):派生出请求 topic 之后原样
  // 交给它,相位与注册两道校验都落在那里。
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
  // 线缆上的 `reply_to` 降为**一致性交叉校验**:非空且与查出的不等即报错——对版本不
  // 一致的对端,它是唯一能当场报出偏差的手段。
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
  // `uint32` 回绕(约 42.9 亿次请求后)**明确接受**,不加防回绕逻辑。
  return uuid_ + "#" + std::to_string(request_seq_++);
}

bool DdsNode::IsReaderSideTopic(const std::string& topic) const {
  // 读侧 = Subscribers ∪ 各服务的 cfg.<名>.request ∪ 各客户端的 cfg.<名>.response(D16)。
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
