#include "transport/node/ProtocolNode.hpp"

#include <chrono>
#include <utility>

#include "transport/core/DropReason.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/TraceCategories.hpp"

// ProtocolNode.cpp — 见 .hpp。协议特有语义内联于此(D9/D10 红线):key 派生、frm_type
// 盖章、session_id 分配、Dispatch 分类、终结判别、寻址。生命周期(幂等 Start / 关闭仲裁 /
// 收敛)由基类 NodeBase 承载,本类只填协议特有钩子(ADR-0006 D1);读循环骨架与 handler
// 消费者小件仍由过渡件 NodeRuntime 持有(ADR-0006 D5,#140 下放本类)。

namespace transport {

CorrelationKeyStrategy DefaultProtocolKeyStrategy(std::uint16_t response_marker) {
  CorrelationKeyStrategy strategy;
  strategy.request_key = [](const Message& msg) -> ProtocolKey {
    // 请求键:会话 id 上移 16 位 | 命令码原样。
    return (static_cast<ProtocolKey>(msg.session_id) << 16) |
           static_cast<ProtocolKey>(msg.message_id);
  };
  strategy.response_key = [response_marker](const Message& msg) -> ProtocolKey {
    // 响应键:清掉响应标记位,把命令码归一化回请求命令码,再与会话 id 合成。
    const auto normalized = static_cast<ProtocolKey>(msg.message_id) &
                            ~static_cast<ProtocolKey>(response_marker);
    return (static_cast<ProtocolKey>(msg.session_id) << 16) | normalized;
  };
  return strategy;
}

// session_id 空间宽度:线缆 uint8 硬顶 = 256 个并发在途上限(协议特有值,D10)。
constexpr std::size_t kSessionIdSpace = 256;

ProtocolNode::ProtocolNode(std::unique_ptr<ITransport> transport,
                           std::unique_ptr<ICodec> codec, ProtocolNodeConfig config)
    // 生命周期基类:共用同一可选 trace_sink(生命周期跃迁 + close_drop 归因)。
    : NodeBase(config.trace_sink),
      transport_(std::move(transport)),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // P5-4:与 runtime_ 共用同一可选 trace_sink(未配则两处 RecordEvent 均一次判空)。
      // max_pending=256 与 session 空闲集是同一上限的双重执法(#98 注):默认键策略下
      // AllocateSession 先行拒绝,此纯计数上限不可达——保留仅为防**自定义键策略**绕过
      // session 预算造出超额在途(RT_DESIGN_008 自定义键开放后的兜底)。
      pending_(kSessionIdSpace, config_.trace_sink),
      // 过渡件组合业务队列:字节计量注入 payload.size()(D10:runtime 不读 Message 任何
      // 其它字段);transport 裸指针供读循环 Read(node 持 unique_ptr)。trace_sink 透传
      // (P5-3):业务队列满(business_queue_overflow)经它可选上报。
      runtime_(transport_.get(),
               [](const Message& msg) { return msg.payload.size(); },
               config_.business_queue_max_events, config_.business_queue_max_bytes,
               config_.trace_sink) {
  // 空闲集初值 0..255:分配 pop_front、释放 push_back → FIFO 复用最久释放者(退休窗口
  // 最大化,RT_REQUEST_005)。
  for (std::size_t id = 0; id < kSessionIdSpace; ++id) {
    free_sessions_.push_back(static_cast<std::uint8_t>(id));
  }
}

// 析构即关闭:必须在**本类**析构体内做——基类析构时虚钩子已退回纯虚(见 NodeBase 文档)。
ProtocolNode::~ProtocolNode() { Close(); }

Status ProtocolNode::ValidateConfig() const {
  // 队列上界须落合法区间(RT_LIFECYCLE_007 / 3.1.5.4);构造时 BoundedQueue 虽会钳制,
  // 但 Start 前显式校验原始 config 以便非法时停 Created、允许宿主改配重试。
  using Q = BoundedQueue<Message>;
  if (config_.business_queue_max_events < Q::kMinEvents ||
      config_.business_queue_max_events > Q::kMaxEvents) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (config_.business_queue_max_bytes < Q::kMinBytes ||
      config_.business_queue_max_bytes > Q::kMaxBytes) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // 关联键策略两支必须非空——否则请求↔响应无从配对(交互契约破坏)。
  if (!config_.key_strategy.request_key || !config_.key_strategy.response_key) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  // 默认请求超时须为正(SRS §3.1.4.4):零或负值等价于"永不超时"或立即超时,前者会让
  // 断链后的无 deadline 请求失去全部终结源(ADR-0004 D3 撤销代际隔离后交互层不再终结在途
  // 请求),破坏 RT_REQUEST_003;故在 Start 前拒绝,停 Created 允许改配重试。
  if (config_.default_request_timeout <= Clock::duration::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Status{};
}

Status ProtocolNode::DoStart() {
  // 基类已做完幂等仲裁与配置校验,这里只做协议特有实事。**无能力探测、无按介质分支的
  // 第二条启动路径**(ADR-0004 D2)。
  Status started = transport_->Start();
  if (!started) {
    return started;  // 传输启动失败:基类退回 Created 允许重试(此时未 MarkRunning)。
  }
  MarkRunning();
  // 读-分发循环:runtime 跑 Read 骨架,node 内联 decode + 协议特有分类/寻址;循环退出后
  // 本 fiber 兼任收敛者,走基类内部路径 ConvergeAfterReadLoop(ADR-0005 D1)。
  runtime_.SpawnReadLoop(
      [this](Datagram datagram) { DecodeAndDispatch(std::move(datagram)); },
      [this] { ConvergeAfterReadLoop(); });
  // 设了 handler → spawn 单消费者 handler fiber(串行消费业务队列);node 在 consume
  // 回调内构造 HandlerContext 并跑业务 handler(协议特有的能力面 + 语义)。
  if (config_.handler) {
    runtime_.SpawnHandlerLoop([this](Message&& msg) {
      HandlerContext ctx(this, runtime_.HandlerCancellationToken());
      // 返回 Status 仅记录:框架不据此自动应答(避 TBD-001)。逃逸异常由 HandlerLoop
      // 边界兜住并归因 handler_exception(RT_HANDLER_006)。
      (void)config_.handler(msg, ctx);
    });
  }
  return Status{};
}

Status ProtocolNode::DoClose() {
  // 关闭汇合信号,**顺序即契约**(见 NodeBase::DoClose 文档):transport.RequestClose 一
  // 执行读循环就可能被唤醒退出,余下信号必须在本段内发完,基类随后才放行收敛。
  transport_->RequestClose();
  runtime_.CancelAndCloseHandler();  // 业务队列 Close + handler 协作取消(同一顺序)。
  // 协议特有收敛信号(ADR-0005 D5):PendingTable.FailAll(kClosed) 令在途请求恰好一次
  // 收敛。**外部 Close 与读循环致命错误自终共用本段**——故它是钩子而非 Close 的入参。
  // 断链**不是**收敛信号(ADR-0004 D3:在途请求只由总超时/取消/关闭终结)。
  pending_.FailAll(make_error_code(TransportErrc::kClosed));
  return Status{};
}

void ProtocolNode::JoinHandler() { runtime_.JoinHandlerLoop(); }

std::size_t ProtocolNode::DrainUnstartedBusiness() {
  return runtime_.DrainHandlerForClose();
}

Result<Message> ProtocolNode::Request(Message req, OperationOptions options) {
  if (!IsRunning()) {
    // 未启动 / 关闭中 / 已关闭:一律 kClosed(PendingTable closed latch 亦兜底)。
    return make_error_code(TransportErrc::kClosed);
  }

  // 总超时缺省值(SRS §3.1.4.4):调用方没给 deadline 就补上默认请求超时,且**在此处**
  // 补——总超时"从节点接受请求时开始",含分配 session、登记、编码、写出与等响应的全部
  // 时间。节点不接受"永不超时"的请求:断链已不再终结在途请求(ADR-0004 D3),无 deadline
  // 者将挂到节点关闭为止。显式给出的 deadline 是调用方意图,一律不覆盖。
  if (!options.deadline) {
    options.deadline = Clock::now() + config_.default_request_timeout;
  }

  // 从空闲集 LRU 分配 session_id;256 个全在途 → 发送前返 kResourceExhausted(不登记、
  // 不发送,RT_REQUEST_006)。
  auto session = AllocateSession();
  if (!session) {
    return make_error_code(TransportErrc::kResourceExhausted);
  }
  // RAII 租约(#98):此后任一返回路径(登记冲突 / 编码失败 / 写失败 / 正常终结)均经
  // lease 析构自动归还 session_id——不再靠手工 ReleaseSession 纪律。lease 先于 handle
  // 构造 → 后于 handle 析构:归还必发生在 entry 摘除之后。
  SessionLease lease(this, *session);
  // node 盖章:命令帧、默认协议 id、分配到的 session_id。
  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = lease.id();
  const ProtocolKey key = config_.key_strategy.request_key(req);

  auto registration = pending_.Register(key);
  if (!registration) {
    return registration.error();  // 重复键 kInvalidState / closed kClosed 透传。
  }
  auto handle = std::move(registration).value();

  auto encoded = codec_->Encode(req);
  if (!encoded) {
    return encoded.error();  // handle 析构兜底摘除未终结 entry(取消纪律)。
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = Endpoint::Default();
  const std::size_t sent_bytes = unit.bytes.size();
  if (auto written = transport_->Write(std::move(unit)); !written) {
    return written.error();
  }
  // Write 完成边界(P5-4):不逐字节,一次性记本帧大小。
  RecordEvent(kTraceCategorySend, config_.trace_sink, "request", {}, {}, {},
              static_cast<long>(sent_bytes));

  // 等唯一响应;请求终结(值/超时/取消/FailAll)后 lease 析构释放 session_id 回空闲集。
  return handle.Wait(std::move(options));
}

std::optional<std::uint8_t> ProtocolNode::AllocateSession() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (free_sessions_.empty()) {
    return std::nullopt;  // 256 全在途。
  }
  const std::uint8_t id = free_sessions_.front();
  free_sessions_.pop_front();
  return id;
}

void ProtocolNode::ReleaseSession(std::uint8_t session_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  free_sessions_.push_back(session_id);
}

std::optional<std::uint8_t> ProtocolNode::PeekIdleSession() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (free_sessions_.empty()) {
    return std::nullopt;  // 256 全在途。
  }
  return free_sessions_.back();  // 只读尾部(最新释放者),不出队(#98)。
}

void ProtocolNode::DecodeAndDispatch(Datagram datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    // 坏帧 / codec 错误:丢弃(codec 内部 resync)。归因 kBadFrame(P5-3)。
    std::lock_guard<std::mutex> lock(mutex_);
    RecordDrop(DropReason::kBadFrame, bad_frame_count_, config_.trace_sink);
    return;
  }
  // Decode 成功边界(P5-4):一次 Decode 调用一条事件,不逐条消息重复。
  RecordEvent(kTraceCategoryDecode, config_.trace_sink, {}, {}, {}, {},
              static_cast<long>(bytes.size()));
  for (auto& msg : decoded.value()) {
    // Read 解出消息边界(P5-4):按解出的消息计,不逐字节。
    RecordEvent(kTraceCategoryRecv, config_.trace_sink, {}, {}, {}, {},
                static_cast<long>(msg.payload.size()));
    Dispatch(std::move(msg));
  }
}

void ProtocolNode::Dispatch(Message msg) {
  // IsTerminal 内联锁死:kResponse / kResult = 响应帧(D9)。
  if (msg.frm_type == FrameType::kResponse || msg.frm_type == FrameType::kResult) {
    const ProtocolKey key = config_.key_strategy.response_key(msg);
    if (!pending_.Resolve(key, std::move(msg))) {
      // RouteUnmatched 内联锁死:无匹配在途请求(迟到 / 乱序 / 无匹配)→ 归因丢弃。
      std::lock_guard<std::mutex> lock(mutex_);
      RecordDrop(DropReason::kUnmatchedOrLateResponse, unmatched_response_count_,
                config_.trace_sink);
    }
    return;
  }
  // 非响应业务帧:设了 handler → 入运行时有界队列交单消费者串行处理;满则 tail-drop
  // (business_queue_overflow),读循环不阻塞、响应匹配照常(RT_HANDLER_004)。未设 handler
  // → 维持 P1 行为归因 dropped_no_handler。
  if (config_.handler) {
    (void)runtime_.Enqueue(std::move(msg));  // 满 / 已 Close 均丢弃,不阻塞。
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  RecordDrop(DropReason::kNoHandlerConfigured, dropped_no_handler_count_,
            config_.trace_sink);
}

Status ProtocolNode::Send(Message msg) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 盖帧只需一个"当前不在途"的 session_id,不需要独占预算:只读空闲集尾部(最新释放者,
  // #98)盖帧——不出队,不与 Request 争 LRU 头部,不扰动 FIFO 退休窗口(RT_REQUEST_005
  // 的迟到误配防护不被高频 Send 削弱)。空闲集空(256 全在途)仍拒绝:与 Request 一致的
  // 既定边界策略(P2-3),且此时任何可盖的 id 都正被某在途请求占用,盖上即有误配面。
  auto session = PeekIdleSession();
  if (!session) {
    return make_error_code(TransportErrc::kResourceExhausted);
  }
  // 盖章:调用方给的业务类型优先,否则默认命令帧;默认协议 id;尾部空闲 session_id。
  if (msg.frm_type == FrameType::kUnknown) {
    msg.frm_type = FrameType::kCommand;
  }
  msg.protocol_id = config_.protocol_id;
  msg.session_id = *session;

  auto encoded = codec_->Encode(msg);
  if (!encoded) {
    return encoded.error();
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = Endpoint::Default();
  const std::size_t sent_bytes = unit.bytes.size();
  auto written = transport_->Write(std::move(unit));  // 遵 RT_TRANSPORT_008 背压。
  if (written) {
    // Write 完成边界(P5-4):不逐字节,一次性记本帧大小。
    RecordEvent(kTraceCategorySend, config_.trace_sink, "fire-and-forget", {}, {},
                {}, static_cast<long>(sent_bytes));
  }
  return written;
}

Status HandlerContext::Send(Message msg) { return node_->Send(std::move(msg)); }

Status HandlerContext::RequestClose() {
  // 只发汇合信号、不等待(ADR-0006 D8):当前即 handler 消费者 fiber,任何等待收敛的入口
  // 都等于等自己退出。返回仅表示已受理;节点由读循环在汇合完成后收敛到 Closed(ADR-0005 D1)。
  return node_->SignalClose();  // NodeBase 的受保护入口(本类是 ProtocolNode 的友元)。
}

std::size_t ProtocolNode::UnmatchedResponseCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return unmatched_response_count_;
}

std::size_t ProtocolNode::DroppedNoHandlerCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return dropped_no_handler_count_;
}

std::size_t ProtocolNode::BusinessQueueOverflowCount() const {
  return runtime_.BusinessQueueOverflowCount();
}

std::size_t ProtocolNode::HandlerExceptionCount() const {
  return runtime_.HandlerExceptionCount();
}

std::size_t ProtocolNode::PendingCount() const { return pending_.Size(); }

std::size_t ProtocolNode::BadFrameCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bad_frame_count_;
}

ProtocolNode::Clock::duration ProtocolNode::LastRequestLatency() const {
  return pending_.LastRequestLatency();
}

ProtocolNode::Clock::duration ProtocolNode::LastHandlerDuration() const {
  return runtime_.LastHandlerDuration();
}

}  // namespace transport
