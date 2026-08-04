#include "transport/node/ProtocolNode.hpp"

#include <chrono>
#include <utility>

#include "task/fibertask.h"  // Coro::makeTask —— reactor fiber(其余 fiber 由 NodeRuntime 起)
#include "transport/core/Observability.hpp"

// ProtocolNode.cpp — 见 .hpp。协议特有语义内联于此(D9/D10 红线):key 派生、frm_type
// 盖章、session_id 分配、Dispatch 分类、终结判别、寻址、reactor 连接观察。协议无关机制
// (生命周期三方汇合、handler 消费者、业务队列、读循环骨架)组合并驱动 NodeRuntime。

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
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // P5-4:与 runtime_ 共用同一可选 trace_sink(未配则两处 RecordEvent 均一次判空)。
      pending_(kSessionIdSpace, config_.trace_sink),
      // 运行时组合业务队列:字节计量注入 payload.size()(D10:runtime 不读 Message 任何
      // 其它字段);transport 裸指针供读循环 Read + 收敛 RequestClose(node 持 unique_ptr)。
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
  return Status{};
}

Status ProtocolNode::Start() {
  // 组合并驱动 NodeRuntime 的幂等 Start:runtime 管状态机 / 共享结果 / 三方汇合骨架;
  // node 提供协议特有的配置校验与首次 bring-up(transport 启动 + 检测连接观察面 + 置
  // Running + spawn 读循环/handler 消费者/reactor)。
  return runtime_.Start(
      [this] { return ValidateConfig(); },
      [this]() -> Status {
        Status started = transport_->Start();
        if (!started) {
          return started;  // 传输启动失败:runtime 退回 Created 允许重试。
        }
        // 传输若为可选连接观察面(=自动重连 TCP 客户端)→ 稍后 spawn reactor fiber。介质
        // 无关:非 IConnectionObservable(Fake/UDP/串口/DDS)→ 不 spawn,行为同 P2。检测在
        // MarkRunning 前完成。
        auto* observable = dynamic_cast<IConnectionObservable*>(transport_.get());
        runtime_.MarkRunning();
        // 读-分发循环:runtime 跑 Read 骨架,node 内联 decode + 协议特有分类/寻址。
        runtime_.SpawnReadLoop(
            [this](Datagram datagram) { DecodeAndDispatch(std::move(datagram)); });
        // 设了 handler → runtime spawn 单消费者 handler fiber(串行消费业务队列);node 在
        // consume 回调内构造 HandlerContext 并跑业务 handler(协议特有的能力面 + 语义)。
        if (config_.handler) {
          runtime_.SpawnHandlerLoop([this](Message&& msg) {
            HandlerContext ctx(this, runtime_.HandlerCancellationToken());
            // 返回 Status 仅记录:框架不据此自动应答(避 TBD-001)。逃逸异常由 runtime
            // 边界兜住并归因 handler_exception(RT_HANDLER_006)。
            (void)config_.handler(msg, ctx);
          });
        }
        // reactor fiber:订阅连接状态跃迁,遇代际结束做代际隔离(ADR-0003 D11 Q1③/Q3④)。
        // 登记 finalizer 追加汇合点,令关闭时确保 reactor 退出后节点方收敛(不触碰已收敛态)。
        if (observable) {
          runtime_.AddFinalizerJoin([this] { reactor_done_.Wait(); });
          Coro::makeTask([this, observable] { RunReactorLoop(observable); });
        }
        return Status{};
      });
}

Status ProtocolNode::Close() {
  // 驱动 runtime 收敛;node 侧协议特有收敛信号:触发 reactor 取消(WaitStateChange 返
  // kCancelled 干净退出)+ PendingTable.FailAll(kClosed) 令在途请求恰好一次收敛。
  return runtime_.Close([this] {
    reactor_cancellation_.Cancel();
    pending_.FailAll(make_error_code(TransportErrc::kClosed));
  });
}

Status ProtocolNode::WaitClosed(OperationOptions options) {
  return runtime_.WaitClosed(std::move(options));
}

Result<Message> ProtocolNode::Request(Message req, OperationOptions options) {
  if (!runtime_.IsRunning()) {
    // 未启动 / 关闭中 / 已关闭:一律 kClosed(PendingTable closed latch 亦兜底)。
    return make_error_code(TransportErrc::kClosed);
  }

  // 从空闲集 LRU 分配 session_id;256 个全在途 → 发送前返 kResourceExhausted(不登记、
  // 不发送,RT_REQUEST_006)。
  auto session = AllocateSession();
  if (!session) {
    return make_error_code(TransportErrc::kResourceExhausted);
  }
  // node 盖章:命令帧、默认协议 id、分配到的 session_id。
  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = *session;
  const ProtocolKey key = config_.key_strategy.request_key(req);

  // 任一提前返回都须归还 session_id(否则在途预算泄漏,终致假 kResourceExhausted)。
  auto registration = pending_.Register(key);
  if (!registration) {
    ReleaseSession(*session);
    return registration.error();  // 重复键 kInvalidState / closed kClosed 透传。
  }
  auto handle = std::move(registration).value();

  auto encoded = codec_->Encode(req);
  if (!encoded) {
    ReleaseSession(*session);  // handle 析构兜底摘除未终结 entry(取消纪律)。
    return encoded.error();
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = Endpoint::Default();
  const std::size_t sent_bytes = unit.bytes.size();
  if (auto written = transport_->Write(std::move(unit)); !written) {
    ReleaseSession(*session);
    return written.error();
  }
  // Write 完成边界(P5-4):不逐字节,一次性记本帧大小。
  RecordEvent("send", config_.trace_sink, "request", {}, {}, {},
              static_cast<long>(sent_bytes));

  // 等唯一响应;请求终结(值/超时/取消/FailAll)后释放 session_id 回空闲集。
  auto outcome = handle.Wait(std::move(options));
  ReleaseSession(*session);
  return outcome;
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

void ProtocolNode::DecodeAndDispatch(Datagram datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    return;  // 坏帧 / codec 错误:丢弃(kBadFrame 已由 P5-3 覆盖,本票不重复)。
  }
  // Decode 成功边界(P5-4):一次 Decode 调用一条事件,不逐条消息重复。
  RecordEvent("decode", config_.trace_sink, {}, {}, {}, {},
              static_cast<long>(bytes.size()));
  for (auto& msg : decoded.value()) {
    // Read 解出消息边界(P5-4):按解出的消息计,不逐字节。
    RecordEvent("recv", config_.trace_sink, {}, {}, {}, {},
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
      ++unmatched_response_count_;
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
  ++dropped_no_handler_count_;
}

void ProtocolNode::RunReactorLoop(IConnectionObservable* observable) {
  // node "观察连接状态但不管理 churn"(ADR-0003 D11 Q1③/Q3④):拉模型订阅状态跃迁,
  // 遇代际结束(离开 kConnected)做代际隔离。node 保持 Running(≠Close 终态)。
  OperationOptions options;
  options.cancellation = reactor_cancellation_.token();  // Close 时取消 → 干净退出。
  // 代际不进 PendingTable(守 RT_DESIGN_008):以"是否处于 Connected"边沿甄别代际结束——
  // P3-1 状态机断连时 Connected→Reconnecting(非 kDisconnected,后者仅终态),故用"曾 Connected
  // 且现非 Connected"的下降沿触发,恰好一次隔离旧代际,契合 D11「遇代际结束」。
  bool was_connected = (observable->State() == ConnectionState::kConnected);
  for (;;) {
    auto changed = observable->WaitStateChange(options);
    if (!changed) {
      break;  // kCancelled(Close)→ 退出;无 deadline 不会 kTimeout。
    }
    const bool now_connected = (changed.value() == ConnectionState::kConnected);
    if (was_connected && !now_connected) {
      // —— 代际结束:隔离旧代际 ——
      // 在途请求恰好一次 kConnection(不 latch:node 保持 Running,新代际仍可 Register,
      // RT_TCP_RECONNECT_002)。断连时释放对应在途 session_id 由 Request 的 FailAll 收敛
      // 路径(handle.Wait 返回后 ReleaseSession)完成,不泄漏。
      pending_.FailAll(make_error_code(TransportErrc::kConnection),
                       /*latch_closed=*/false);
      // 未启动处理的旧代际排队业务 → Drain 丢弃、归因 连接代际隔离丢弃(RT_TCP_RECONNECT
      // 3.1.7.4)。正在运行的 handler 让其跑完清理(不强杀);其 ctx.Send 重连期返 Connection。
      const std::size_t dropped = runtime_.DrainBusinessQueue().size();
      if (dropped != 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        generation_isolation_drop_count_ += dropped;
      }
    }
    was_connected = now_connected;
  }
  reactor_done_.Complete(Status{});
}

Status ProtocolNode::Send(Message msg) {
  if (!runtime_.IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 分配 session_id 盖帧后立即释放:不登记 PendingTable、不占 256 在途预算。全在途无空闲
  // → kResourceExhausted(边界策略:与 Request 一致地拒绝,不与在途请求争 session_id)。
  auto session = AllocateSession();
  if (!session) {
    return make_error_code(TransportErrc::kResourceExhausted);
  }
  // 盖章:调用方给的业务类型优先,否则默认命令帧;默认协议 id;分配到的 session_id。
  if (msg.frm_type == FrameType::kUnknown) {
    msg.frm_type = FrameType::kCommand;
  }
  msg.protocol_id = config_.protocol_id;
  msg.session_id = *session;
  ReleaseSession(*session);  // 盖帧完毕立即归还(fire-and-forget,不占预算)。

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
    RecordEvent("send", config_.trace_sink, "fire-and-forget", {}, {}, {},
                static_cast<long>(sent_bytes));
  }
  return written;
}

Status HandlerContext::Send(Message msg) { return node_->Send(std::move(msg)); }

Status HandlerContext::RequestClose() {
  // 发起完整关闭拆卸;因当前即 handler 消费者 fiber,runtime.Close 内重入自锁防护只发起、
  // 不自等,立即返回(RT_LIFECYCLE_005)。节点由 finalizer fiber 在三方汇合后收敛到 Closed。
  return node_->Close();
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

std::size_t ProtocolNode::CloseDropCount() const {
  return runtime_.CloseDropCount();
}

std::size_t ProtocolNode::HandlerCancelOverrunCount() const {
  return runtime_.HandlerCancelOverrunCount();
}

std::size_t ProtocolNode::GenerationIsolationDropCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return generation_isolation_drop_count_;
}

ProtocolNode::Clock::duration ProtocolNode::LastRequestLatency() const {
  return pending_.LastRequestLatency();
}

ProtocolNode::Clock::duration ProtocolNode::LastHandlerDuration() const {
  return runtime_.LastHandlerDuration();
}

ProtocolNode::Clock::duration ProtocolNode::LastCloseLatency() const {
  return runtime_.LastCloseLatency();
}

}  // namespace transport
