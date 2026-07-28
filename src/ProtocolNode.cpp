#include "transport/ProtocolNode.hpp"

#include <utility>

#include "task/fibertask.h"  // Coro::makeTask —— 读循环 fiber

// ProtocolNode.cpp — 见 .hpp。协议特有语义内联于此(D9 红线)。

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
      pending_(kSessionIdSpace),
      // 字节计量注入 payload.size()(D10:队列不读 Message 任何其它字段)。
      business_queue_([](const Message& msg) { return msg.payload.size(); },
                      config_.business_queue_max_events,
                      config_.business_queue_max_bytes) {
  // 空闲集初值 0..255:分配 pop_front、释放 push_back → FIFO 复用最久释放者(退休窗口
  // 最大化,RT_REQUEST_005)。
  for (std::size_t id = 0; id < kSessionIdSpace; ++id) {
    free_sessions_.push_back(static_cast<std::uint8_t>(id));
  }
}

ProtocolNode::~ProtocolNode() { Close(); }

Status ProtocolNode::Start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  if (auto started = transport_->Start(); !started) {
    return started;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycle_ = LifecycleState::kRunning;
  }
  // spawn 读-分发循环 fiber;fiber 自持(detach),经 loop_done_ 与 Close 汇合。
  Coro::makeTask([this] { RunReadLoop(); });
  // 设了 handler → 另 spawn 单消费者 handler fiber(串行消费业务队列,RT_HANDLER_003);
  // 与读循环独立,响应匹配 / 关闭不被 handler await 阻塞(RT_HANDLER_004)。
  if (config_.handler) {
    Coro::makeTask([this] { RunHandlerLoop(); });
  }
  return Status{};
}

Status ProtocolNode::Close() {
  bool wait_loop = false;
  bool has_handler = (config_.handler != nullptr);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ == LifecycleState::kClosed) {
      return Status{};
    }
    wait_loop = (lifecycle_ == LifecycleState::kRunning);
    // Created(读循环从未 spawn)直接收敛;Running 进 Closing 等读循环 + 消费者退出。
    lifecycle_ =
        wait_loop ? LifecycleState::kClosing : LifecycleState::kClosed;
  }
  // 三方汇合:唤醒读循环(RequestClose)+ 消费者(队列 Close → Pop 返 kClosed)+ 触发
  // handler 取消;FailAll 让在途请求以 kClosed 收敛。
  SignalConvergence();
  pending_.FailAll(make_error_code(TransportErrc::kClosed));
  if (wait_loop) {
    loop_done_.Wait();  // 读循环 Read 收到 kClosed 后退出并 Complete(此挂起点让其推进)。
    if (has_handler) {
      handler_done_.Wait();  // 消费者 Pop 收到 kClosed 后退出并 Complete。
    }
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycle_ = LifecycleState::kClosed;
  }
  closed_.Complete(Status{});
  return Status{};
}

void ProtocolNode::SignalConvergence() {
  transport_->RequestClose();
  business_queue_.Close();     // 唤醒消费者 Pop 返 kClosed → 消费者 fiber 退出。
  handler_cancellation_.Cancel();  // handler 经 ctx.cancellation() 观测到取消。
}

Status ProtocolNode::WaitClosed(OperationOptions options) {
  return closed_.Wait(std::move(options));
}

Result<Message> ProtocolNode::Request(Message req, OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ != LifecycleState::kRunning) {
      // 未启动 / 关闭中 / 已关闭:一律 kClosed(PendingTable closed latch 亦兜底)。
      return make_error_code(TransportErrc::kClosed);
    }
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
  if (auto written = transport_->Write(std::move(unit)); !written) {
    ReleaseSession(*session);
    return written.error();
  }

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

void ProtocolNode::RunReadLoop() {
  while (true) {
    auto datagram = transport_->Read();  // 裸读,无 deadline。
    if (!datagram) {
      const auto error = datagram.error();
      if (error == make_error_code(TransportErrc::kClosed) ||
          error == make_error_code(TransportErrc::kConnection)) {
        break;  // 传输终结 → 退出读循环。
      }
      continue;  // 其它(如注入的瞬时错误):丢弃继续。
    }
    const auto& bytes = datagram.value().bytes;
    auto decoded = codec_->Decode(bytes.data(), bytes.size());
    if (!decoded) {
      continue;  // 坏帧 / codec 错误:丢弃(codec 内部 resync)。
    }
    for (auto& msg : decoded.value()) {
      Dispatch(std::move(msg));
    }
  }
  loop_done_.Complete(Status{});
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
  // 非响应业务帧:设了 handler → 入有界队列交单消费者串行处理;满则 tail-drop
  // (business_queue_overflow,BoundedQueue 内部计数),读循环不阻塞、响应匹配照常
  // (RT_HANDLER_004)。未设 handler → 维持 P1 行为归因 dropped_no_handler。
  if (config_.handler) {
    (void)business_queue_.Push(std::move(msg));  // 满 / 已 Close 均丢弃,不阻塞。
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  ++dropped_no_handler_count_;
}

void ProtocolNode::RunHandlerLoop() {
  // 消费者上下文:节点执行域内(RT_HANDLER_002),露 Send / RequestClose / cancellation。
  HandlerContext ctx(this, handler_cancellation_.token());
  for (;;) {
    auto item = business_queue_.Pop();  // 空则协作 await;Close → kClosed 唤醒退出。
    if (!item) {
      break;  // kClosed / kCancelled:队列收敛 → 消费者退出。
    }
    const Message msg = std::move(item).value();
    // 出队一条跑完(含其 await)再出下一条 = 严格串行(RT_HANDLER_003)。
    try {
      // 返回 Status 仅记录:框架不据此自动应答(避 TBD-001)。
      (void)config_.handler(msg, ctx);
    } catch (...) {
      // RT_HANDLER_006 / RT_ERROR_001:边界兜住逃逸异常 → 转 kInternal 隔离当前事件,
      // 不自关 node,继续下一条。这是框架唯一授权的 catch(框架自身不抛)。
      std::lock_guard<std::mutex> lock(mutex_);
      ++handler_exception_count_;
    }
  }
  handler_done_.Complete(Status{});
}

Status ProtocolNode::Send(Message msg) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ != LifecycleState::kRunning) {
      return make_error_code(TransportErrc::kClosed);
    }
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
  return transport_->Write(std::move(unit));  // 遵 RT_TRANSPORT_008 背压。
}

Status HandlerContext::Send(Message msg) { return node_->Send(std::move(msg)); }

Status HandlerContext::RequestClose() {
  // 非阻塞收敛信号:唤醒读循环 + 消费者,不在 handler fiber 内等待终结(避自等自锁)。
  node_->SignalConvergence();
  return Status{};
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
  return business_queue_.DroppedCount();  // BoundedQueue 自守其锁。
}

std::size_t ProtocolNode::HandlerExceptionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return handler_exception_count_;
}

std::size_t ProtocolNode::PendingCount() const { return pending_.Size(); }

}  // namespace transport
