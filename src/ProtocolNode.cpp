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

ProtocolNode::ProtocolNode(std::unique_ptr<ITransport> transport,
                           std::unique_ptr<ICodec> codec, ProtocolNodeConfig config)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      config_(std::move(config)) {}

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
  return Status{};
}

Status ProtocolNode::Close() {
  bool wait_loop = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ == LifecycleState::kClosed) {
      return Status{};
    }
    wait_loop = (lifecycle_ == LifecycleState::kRunning);
    // Created(读循环从未 spawn)直接收敛;Running 进 Closing 等读循环退出。
    lifecycle_ =
        wait_loop ? LifecycleState::kClosing : LifecycleState::kClosed;
  }
  transport_->RequestClose();
  pending_.FailAll(make_error_code(TransportErrc::kClosed));
  if (wait_loop) {
    loop_done_.Wait();  // 读循环 Read 收到 kClosed 后退出并 Complete(此挂起点让其推进)。
    std::lock_guard<std::mutex> lock(mutex_);
    lifecycle_ = LifecycleState::kClosed;
  }
  closed_.Complete(Status{});
  return Status{};
}

Status ProtocolNode::WaitClosed(OperationOptions options) {
  return closed_.Wait(std::move(options));
}

Result<Message> ProtocolNode::Request(Message req, OperationOptions options) {
  ProtocolKey key;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (lifecycle_ != LifecycleState::kRunning) {
      // 未启动 / 关闭中 / 已关闭:一律 kClosed(PendingTable closed latch 亦兜底)。
      return make_error_code(TransportErrc::kClosed);
    }
    // node 盖章:命令帧、默认协议 id、滚动分配 session_id(uint8 单调滚动)。
    req.frm_type = FrameType::kCommand;
    req.protocol_id = config_.protocol_id;
    req.session_id = next_session_++;
    key = config_.key_strategy.request_key(req);
  }

  auto registration = pending_.Register(key);
  if (!registration) {
    return registration.error();  // 重复键 kInvalidState / closed kClosed 透传。
  }
  auto handle = std::move(registration).value();

  auto encoded = codec_->Encode(req);
  if (!encoded) {
    return encoded.error();
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = Endpoint::Default();
  if (auto written = transport_->Write(std::move(unit)); !written) {
    return written.error();
  }

  return handle.Wait(std::move(options));
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
  // 业务帧:P1 无 handler / 无队列 → 归因丢弃(留 P2)。
  std::lock_guard<std::mutex> lock(mutex_);
  ++dropped_no_handler_count_;
}

std::size_t ProtocolNode::UnmatchedResponseCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return unmatched_response_count_;
}

std::size_t ProtocolNode::DroppedNoHandlerCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return dropped_no_handler_count_;
}

}  // namespace transport
