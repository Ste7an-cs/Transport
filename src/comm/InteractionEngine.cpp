#include "transport/comm/InteractionEngine.hpp"

#include <chrono>
#include <utility>
#include <variant>

#include "transport/comm/ThreadExecutor.hpp"

// InteractionEngine.cpp — 见 .hpp 的总览。机制一份;并发纪律集中于此。
//
// 阅读提示:每个公有方法都遵循同一套并发纪律——
//   ① 改共享表(pending_/periodics_)时持 mu_;
//   ② Encode/Send/用户回调/工厂 一律在 mu_ 之外(持锁时先拷贝/移出到局部);
//   ③ posted 任务与定时器都捕获 weak_ptr,引擎没了就跳过;
//   ④ 终结回调【移出】调用(恰好一次),中间回调【拷贝】调用(保留挂起)。

namespace transport {

namespace {
// 统一的"成功无值"返回(Status = Result<std::monostate>)。
Status Ok() { return Status::Success(std::monostate{}); }

// 把 Endpoint 格式化成一行供 trace 显示(仅在挂了 sink 时才调用)。
std::string EndpointStr(const Endpoint& e) {
  switch (e.kind) {
    case Endpoint::Kind::kNet:   return "net:" + e.host + ":" + std::to_string(e.port);
    case Endpoint::Kind::kTopic: return "topic:" + e.topic;
    default:                     return "default";
  }
}
}  // namespace

InteractionEngine::InteractionEngine(std::shared_ptr<ITransport> transport,
                                     std::unique_ptr<ICodec> codec,
                                     std::unique_ptr<InteractionPolicy> policy,
                                     std::unique_ptr<IExecutor> executor,
                                     std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      policy_(std::move(policy)),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))) {}

InteractionEngine::~InteractionEngine() { Close(); }  // 幂等;保证停线程/定时器后才析构成员

// Open:接好 transport 的三个回调,启动 executor 与 transport。
// 注意 OnBytes 在 io/listener 线程跑:它只做"解码 + Post 到 worker",绝不在 io 线程上
// 碰挂起表或调业务回调——把这些都推到单 worker 串行执行(背压点在 Post)。
Status InteractionEngine::Open() {
  executor_->Start();
  std::weak_ptr<InteractionEngine> wself = weak_from_this();  // 回调存活期可能长于引擎 → weak
  // ---- 收包路径(io 线程):一次回调 = 流式的一段 read / 报文式的一个完整 datagram ----
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string& from) {
    auto s = wself.lock();
    if (!s) return;                           // 引擎已亡,丢弃这次回调
    if (!r) {                                 // 传输层读错误(如 UDP 单包 I/O 失败)
      std::string e = r.error;
      // 业务回调要在 worker 线程串行跑 → Post(不在 io 线程直接调 on_error_)。
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      s->Trace({TraceLevel::kError, "error", "", "", "", e, kNoTag, kNoNum, -1});
      return;
    }
    if (s->trace_) s->Trace({TraceLevel::kTrace, "recv", "", "", from, "", kNoTag,
                             static_cast<long>(r.value.size()), -1});
    // Decode 可有状态(SystemCodec 的滚动缓冲由这唯一的 io 线程喂)或无状态(DdsCodec)。
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      // 有意:仅上报解码错误,不把 frame: 失败升级为断连(旧 CommNode 会)。
      // 在用编解码器(SystemCodec 自重同步、DdsCodec)从不从 Decode 返回致命 frame:,故升级为死代码。
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) if (s2->on_error_) s2->on_error_(e); });
      s->Trace({TraceLevel::kWarn, "decode", "decode-fail", "", "", e, kNoTag, kNoNum, -1});
      return;
    }
    s->Trace({TraceLevel::kTrace, "decode", "", "", "", "", kNoTag, static_cast<long>(msgs.value.size()), -1});
    // 把每条解出的消息逐条 Post 到 worker 做 Dispatch(单线程串行 → 分发无需再加锁)。
    for (auto& m : msgs.value) {
      m.source = from;                        // 记录来源(UDP="ip:port"、DDS=topic),供应答寻址
      if (m.topic.empty()) m.topic = from;    // topic 缺省取来源
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
    }
  });
  transport_->OnConnect([wself] {
    if (auto s = wself.lock())
      s->Trace({TraceLevel::kInfo, "conn", "connect", "", "", "", kNoTag, kNoNum, -1});
  });
  // 断连:终结挂起请求要在 worker 线程串行做(与 Dispatch 不并发) → Post HandleDisconnect。
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock()) {
      s->Trace({TraceLevel::kInfo, "conn", "disconnect", "", "", reason, kNoTag, kNoNum, -1});
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
    }
  });
  // open_ 在 transport_->Open() 之前置位:某些 transport 的 OnConnect 可能同步回调,
  // 那时 IsOpen() 须已为真。打开失败则回滚(置回 false 并停 executor),不留半开状态。
  open_.store(true);
  auto st = transport_->Open();
  if (!st) { open_.store(false); executor_->Stop(); return st; }
  Trace({TraceLevel::kInfo, "open", "", "", "", "", kNoTag, kNoNum, -1});
  return Ok();
}

// Close:幂等关闭。次序很关键——先在锁内夺走挂起表/取消定时器,放锁后终结回调,
// 再 executor_->Stop()(drain+join,阻塞到 worker 退出),最后关 transport。
// Stop 的 join 是【生命周期屏障】:在途的 Dispatch/FirePeriodic 必先跑完,之后调用方
// (节点析构)才会继续 —— 所以 worker 上的回调不会碰到已析构的节点状态。
void InteractionEngine::Close() {
  if (closing_.exchange(true)) return;        // 多次/重入 Close 只生效一次
  open_.store(false);                         // 之后所有 SendMessage 都拒发(config: not open)
  std::map<Key, Pending> taken;
  {
    std::lock_guard<std::mutex> lk(mu_);
    taken.swap(pending_);                     // 夺走挂起表(锁内),放锁后再回调
    for (auto& kv : periodics_) if (kv.second.timer) executor_->Cancel(kv.second.timer);
    periodics_.clear();
  }
  Trace({TraceLevel::kInfo, "close", "", "", "", "", kNoTag, static_cast<long>(taken.size()), -1});
  for (auto& kv : taken) {                    // 锁外:终结每个挂起请求(恰好一次,回 conn:)
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();                          // join worker —— 屏障(见上)
  transport_->Close();
}

// SendMessage:一切出站的唯一收口(Fire/RequestAwait/SendReply/重发/periodic 都经此)。
// 始终在 mu_ 之外被调。未 Open 直接拒发。
Status InteractionEngine::SendMessage(Message& m, const Endpoint& to) {
  if (!open_.load()) return Status::Fail("config: node not open");
  auto bytes = codec_->Encode(m);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value, to);
}

// Fire(原语1):单向发。盖 tag → (可选 trace) → 发。不登记、不等待。
Status InteractionEngine::Fire(Message out, FrameTag tag, const Endpoint& to) {
  policy_->SetTag(out, tag);
  if (trace_) { std::string ep = EndpointStr(to);
    Trace({TraceLevel::kDebug, "send", "", "", ep, "", tag, static_cast<long>(out.payload.size()), -1}); }
  return SendMessage(out, to);
}

// RequestAwait(原语2):发请求并登记挂起。
// 次序:盖 tag → 锁内 {生成相关号、排超时定时器、登记挂起} → 锁外发送 → 发送失败则回滚。
// 为何"先登记再发送":若反过来,应答可能在登记前就回到 Dispatch 而找不到挂起项(竞态)。
Status InteractionEngine::RequestAwait(Message out, RequestSpec spec, const Endpoint& to) {
  if (!open_.load()) {                         // 早退:未 Open 直接以错误回调,不登记
    if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  policy_->SetTag(out, spec.request_tag);
  // 预存 tag/size:下面 spec 会被 move 进 Pending,move 后不能再读它。
  const FrameTag req_tag = spec.request_tag;
  const long req_size = static_cast<long>(out.payload.size());
  Key key;
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {    // 与 Close 竞争时的二次检查(持锁内)
      if (spec.on_terminal) spec.on_terminal(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    key = policy_->NewCorrelation(out);        // 盖全新相关号,得挂起 Key
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(spec.timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p; p.spec = std::move(spec); p.out = out; p.to = to; p.timer = timer;
    pending_[key] = std::move(p);              // out/to 留作超时重发用
  }
  if (trace_) { std::string ep = EndpointStr(to);
    Trace({TraceLevel::kDebug, "request", "", key, ep, "", req_tag, req_size, -1}); }
  Status st = SendMessage(out, to);            // 锁外发送
  if (!st) {
    // 发送失败:回滚刚登记的挂起(取消定时器、移出终结回调),并以发送错误终结。
    // 但要小心:应答/超时可能已先一步处理了它 → find 可能已为空(那就别重复回调)。
    std::function<void(Result<Message>)> cb;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.spec.on_terminal); pending_.erase(it); }
    }
    if (cb) cb(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}

// OnTimeout(在 worker 线程):某挂起请求的超时定时器到点。
// 「首帧停重发」:advanced=true(已收到任意中间/终结帧)则不再重发,直接失败。
void InteractionEngine::OnTimeout(Key key) {
  std::function<void(Result<Message>)> fail_cb;   // 失败终结回调(锁外调)
  bool resend = false; Message rs; Endpoint rto;  // 重发的消息/目的地(锁内拷出)
  int retries_now = -1;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;             // 已被应答/Close 处理掉 → 这次超时作废
    Pending& p = it->second;
    if (!p.advanced && p.retries < p.spec.max_retries) {
      // 还能重发:计数 +1,排下一个超时定时器,拷出要重发的消息(锁外发)。
      ++p.retries;
      std::weak_ptr<InteractionEngine> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(p.spec.timeout_ms),
          [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
      resend = true; rs = p.out; rto = p.to; retries_now = static_cast<int>(p.retries);
    } else {
      // 已推进或已达重发上限:移出终结回调、删挂起,锁外以 timeout: 失败。
      fail_cb = std::move(p.spec.on_terminal);
      pending_.erase(it);
    }
  }
  if (resend) {                                   // 锁外重发
    (void)SendMessage(rs, rto);
    Trace({TraceLevel::kDebug, "retransmit", "", key, "", "", kNoTag, kNoNum, retries_now});
  }
  if (fail_cb) {                                  // 锁外终结(恰好一次)
    Trace({TraceLevel::kWarn, "timeout", "", key, "", "", kNoTag, kNoNum, -1});
    fail_cb(Result<Message>::Fail("timeout: request timed out"));
  }
}

// Dispatch(在 worker 线程,串行):对一条入站消息做分发决策。
// 这是请求-应答状态机的落点:命中挂起 → 终结/中间;无主 → 按 policy 路由。
// 锁内只做"查表 + 决定干什么 + 把回调/ack 拷贝或移出到局部",所有实际回调/发送都放锁后做。
void InteractionEngine::Dispatch(Message msg) {
  const Key key = policy_->KeyOf(msg);          // 这条消息的匹配键
  const FrameTag tag = policy_->TagOf(msg);     // 它的判别符
  // —— 以下局部变量是"锁内决定、锁外执行"的载体 ——
  std::function<void(const Message&)> inter_cb; // 命中中间帧 → 要调的中间回调(拷贝)
  std::function<void(Result<Message>)> term_cb; // 命中终结帧 → 要调的终结回调(移出)
  bool auto_ack = false; FrameTag ack_tag = 0; Message ack_req;  // 终结后是否自动回 ack
  bool matched = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // 仅按 Key 配对,不校验回应来源。1:多 UDP 下注意:并发挂起请求受 Key 空间限制
    // (ProtocolPolicy 为 session 0–255,≤256 并发),且来源不符的回应若 Key 偶合也会被接受。
    auto it = pending_.find(key);
    if (it != pending_.end()) {
      matched = true;
      Pending& p = it->second;
      if (tag == p.spec.terminal_tag) {
        // 【终结帧】:取消超时、移出终结回调、删挂起。回调"移出"确保恰好一次
        // (Close/断连即便也想终结它,move 后这里的 on_terminal 已空)。
        p.advanced = true;
        executor_->Cancel(p.timer);
        term_cb = std::move(p.spec.on_terminal);
        if (p.spec.auto_ack_tag.has_value()) { auto_ack = true; ack_tag = *p.spec.auto_ack_tag; ack_req = msg; }
        pending_.erase(it);
      } else if (p.spec.intermediate_tag.has_value() && tag == *p.spec.intermediate_tag) {
        // 【中间帧】(如 needfeedback 的 RESPONSE):置 advanced(停重发)、拷贝中间回调、
        // 但【保留】挂起继续等终结帧。回调"拷贝"——因为还要等下一帧。
        p.advanced = true;
        inter_cb = p.spec.on_intermediate;
      }
      // 否则:命中 key 但 tag 既非终结也非中间(意外)→ 忽略
    }
  }
  if (matched) {                                // —— 锁外执行刚才的决定 ——
    if (auto_ack) {                             // 收到终结后自动回一帧 ack(needfeedback)
      (void)SendReply(ack_req, ack_tag, {});
      Trace({TraceLevel::kTrace, "dispatch", "auto-ack", key, "", "", ack_tag, kNoNum, -1});
    }
    if (term_cb) {                              // 终结:成功带应答(移动出去)
      Trace({TraceLevel::kDebug, "dispatch", "match-terminal", key, "", "", tag, kNoNum, -1});
      term_cb(Result<Message>::Success(std::move(msg)));
    } else if (inter_cb) {                      // 中间:回调,挂起仍在
      Trace({TraceLevel::kDebug, "dispatch", "match-intermediate", key, "", "", tag, kNoNum, -1});
      inter_cb(msg);
    }
    return;
  }
  // 无主入站帧:问 policy 该当请求、当投递、还是丢弃。
  switch (policy_->RouteUnmatched(msg)) {
    case InteractionPolicy::Route::kInboundRequest:
      Trace({TraceLevel::kTrace, "unmatched", "request", key, "", "", tag, kNoNum, -1});
      if (on_request_) on_request_(msg);
      break;
    case InteractionPolicy::Route::kDeliver:
      Trace({TraceLevel::kTrace, "unmatched", "deliver", key, "", "", tag, kNoNum, -1});
      if (on_deliver_) on_deliver_(msg);
      break;
    case InteractionPolicy::Route::kDrop:
      Trace({TraceLevel::kTrace, "unmatched", "drop", key, "", "", tag, kNoNum, -1});
      break;
  }
}

// SendReply:服务端应答 / 客户端自动 ack。policy 据 request 回填相关号(EchoCorrelation,
// 使发起方 KeyOf 命中)+ 算目的地(ReplyTo)。SetTag 盖应答类型,发出。
Status InteractionEngine::SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload) {
  Message m; m.payload = std::move(payload);
  policy_->SetTag(m, tag);
  policy_->EchoCorrelation(m, request);         // 回填 corr,使发起方配对
  Endpoint to = policy_->ReplyTo(request);      // DDS→Topic(reply_to);协议→Default 或 Net(来源)
  if (trace_) { std::string ep = EndpointStr(to); Key rk = policy_->KeyOf(request);
    Trace({TraceLevel::kDebug, "reply", "", rk, ep, "", tag, static_cast<long>(m.payload.size()), -1}); }
  return SendMessage(m, to);
}

// StartPeriodic(原语3,工厂版):立即发一帧,然后每 interval_ms 一帧;每拍 make() 取最新。
uint32_t InteractionEngine::StartPeriodic(std::function<Message()> make, FrameTag tag,
                                          uint32_t interval_ms, const Endpoint& to) {
  if (interval_ms == 0 || !make) return 0;      // 防御:无效参数 → 不启动(空工厂会 bad_function_call)
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = periodic_next_++;
    periodics_[handle] = Periodic{make, tag, to, interval_ms, 0};  // 存的是工厂副本
  }
  Trace({TraceLevel::kTrace, "periodic", "start", "", "", "", tag, kNoNum, -1});
  Message out = make();                         // 锁外:立即首拍 = 最新(make 是用户代码,不可持锁调)
  (void)Fire(out, tag, to);
  // 排第一个周期定时器。注意持锁前先 weak_from_this(),定时器到点回 FirePeriodic。
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);            // 可能已被并发 Stop/Close 移除
  if (it != periodics_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
  return handle;
}

// StartPeriodic(固定版):把固定 out 包成工厂 [m]{return m;},复用工厂版。行为与历史一致。
uint32_t InteractionEngine::StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                                          const Endpoint& to) {
  return StartPeriodic(std::function<Message()>([m = std::move(out)]() { return m; }),
                       tag, interval_ms, to);
}

// UpdatePeriodic:推送更新(push 模型)。锁内把工厂换成"返回新 out"。下一拍生效。
// 注意:这会把一个【工厂启动】的 periodic 永久变成固定消息(见 .hpp 说明)。
bool InteractionEngine::UpdatePeriodic(uint32_t handle, Message out) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it == periodics_.end()) return false;     // handle 未知(已 Stop/Close 或从未存在)
  it->second.make = [m = std::move(out)]() { return m; };
  return true;
}

// FirePeriodic(在 worker 线程):一拍到点。锁内取出工厂/参数,锁外 make()+Fire,再排下一拍。
void InteractionEngine::FirePeriodic(uint32_t handle) {
  std::function<Message()> make; FrameTag tag = 0; Endpoint to; uint32_t interval = 0; bool alive = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);          // 可能已被 Stop/Close 移除
    if (it != periodics_.end()) { make = it->second.make; tag = it->second.tag; to = it->second.to; interval = it->second.interval_ms; alive = true; }
  }
  if (!alive || !open_.load()) return;          // 已停或引擎正在关 → 不发、不再排
  Trace({TraceLevel::kTrace, "periodic", "fire", "", "", "", tag, kNoNum, -1});
  Message out = make();                         // 锁外:每拍取最新(工厂是用户代码)
  (void)Fire(out, tag, to);
  std::weak_ptr<InteractionEngine> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = periodics_.find(handle);
  if (it != periodics_.end())                   // 自重排:排下一拍
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FirePeriodic(handle); });
}

// StopPeriodic:取消并移除某 periodic。tag 在锁内取出仅供 trace;Cancel 放锁外。
void InteractionEngine::StopPeriodic(uint32_t handle) {
  IExecutor::TimerId t = 0; FrameTag tg = 0; bool found = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = periodics_.find(handle);
    if (it != periodics_.end()) { t = it->second.timer; tg = it->second.tag; periodics_.erase(it); found = true; }
  }
  if (found) Trace({TraceLevel::kTrace, "periodic", "stop", "", "", "", tg, kNoNum, -1});
  if (t) executor_->Cancel(t);                  // 锁外(不在锁内回调 executor,保持纪律一致)
}

// HandleDisconnect(在 worker 线程):断连时终结全部挂起请求(回 reason,如 "conn: ...")。
// 与 Close 同构:锁内夺走挂起表,锁外逐个取消定时器 + 终结回调(恰好一次)。
void InteractionEngine::HandleDisconnect(const std::string& reason) {
  std::map<Key, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    if (kv.second.spec.on_terminal) kv.second.spec.on_terminal(Result<Message>::Fail(reason));
  }
}

}  // namespace transport
