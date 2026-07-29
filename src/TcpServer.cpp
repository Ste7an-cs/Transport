#include "transport/TcpServer.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <QAbstractSocket>
#include <QHostAddress>
#include <QPointer>
#include <QString>
#include <QTcpServer>
#include <QTcpSocket>

#include "await/awaitable.hpp"
#include "await/corosocket.hpp"
#include "await/corotcpserver.hpp"
#include "await/detail/socketerror.hpp"
#include "task/fibertask.h"
#include "transport/Error.hpp"
#include "transport/ProtocolNode.hpp"
#include "transport/SharedCompletion.hpp"
#include "transport/TcpTransport.hpp"

namespace transport {

// -----------------------------------------------------------------------------
// 一条已接受连接的子 node:server owns node 与其接受 socket 守卫;supervisor_done 供
// server Close 等待该连接的 supervisor fiber 实际退出(不留悬空引用)。
// -----------------------------------------------------------------------------
struct Child {
  std::unique_ptr<ProtocolNode> node;
  QPointer<QAbstractSocket> socket;
  std::shared_ptr<SharedCompletion<void>> supervisor_done;
};

// 共享状态:accept 循环 fiber、每连接 supervisor fiber、外层 API 结构性并发访问,用
// std::mutex 串行化(同 ADR-0003 D8)。以 shared_ptr 持有,故 detached 的 accept /
// supervisor fiber 在本类析构后仍可安全引用直至各自退出。
struct TcpServer::State {
  mutable std::mutex mutex;
  TcpServerConfig config;
  TcpServer::NodeFactory factory;

  QPointer<QTcpServer> server;  // owned;Close/析构 deleteLater。
  LifecycleState lifecycle{LifecycleState::kCreated};
  bool accept_spawned{false};
  std::uint16_t local_port{0};
  std::error_code last_error;

  std::uint64_t next_child_id{0};
  std::map<std::uint64_t, Child> children;

  std::size_t accepted_count{0};
  std::size_t closed_connection_count{0};
  std::size_t accept_error_count{0};

  SharedCompletion<void> accept_done;  // accept 循环退出通知(Close 汇合点)。
  SharedCompletion<void> closed;       // server 收敛通知(Close/WaitClosed 多等待者)。
};

namespace {

using StatePtr = std::shared_ptr<TcpServer::State>;

// 一条已接受连接的生命 supervisor:等该连接断开(对端主动断开;server Close 时内层
// TcpTransport abort socket 亦触发本等待),然后驱动其 node 收敛并从列表注销(自终路径,
// 连接生命=节点生命 D3′)。若 server Close 已接管本 node(从列表移走)→ find 落空、退让,
// 由 Close 负责收敛。无论哪条路径,退出前 Complete supervisor_done 供 Close 汇合。
void SuperviseChild(StatePtr s, std::uint64_t id, QPointer<QAbstractSocket> sock,
                    std::shared_ptr<SharedCompletion<void>> done) {
  if (sock) {
    Coro::await(Coro::coro(sock.data()).waitForDisconnected());
  }
  std::unique_ptr<ProtocolNode> node;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    auto it = s->children.find(id);
    if (it != s->children.end()) {
      node = std::move(it->second.node);
      s->children.erase(it);
      ++s->closed_connection_count;  // 对端断开自终注销(server Close 路径不经此)。
    }
  }
  if (node) {
    node->Close();  // 幂等:对端断开后驱动 Closing→Closed(非重连,D3′)。
    node->WaitClosed();
    node.reset();
  }
  done->Complete(Status{});
}

// 处理一条新接受的连接:setParent(nullptr) 交出所有权 → 内层 TcpTransport 接管 → 工厂
// 装配 node → Start → 记入子 node 列表 + spawn supervisor。工厂拒绝/启动失败计接受错误。
void HandleAccepted(const StatePtr& s, QTcpSocket* sock) {
  sock->setParent(nullptr);  // 交由 TcpTransport 管理生命周期(同回环夹具范式)。
  auto transport = std::make_unique<TcpTransport>(sock);
  std::unique_ptr<ProtocolNode> node = s->factory(std::move(transport));
  if (!node) {
    // 工厂拒绝:transport 已析构(RequestClose + deleteLater sock)。
    std::lock_guard<std::mutex> lock(s->mutex);
    ++s->accept_error_count;
    return;
  }
  Status started = node->Start();
  if (!started) {
    node->Close();  // 显式撕连接(node 析构亦兜底)。
    std::lock_guard<std::mutex> lock(s->mutex);
    ++s->accept_error_count;
    s->last_error = started.error();
    return;
  }

  auto done = std::make_shared<SharedCompletion<void>>();
  QPointer<QAbstractSocket> guard(sock);
  std::uint64_t id = 0;
  bool registered = false;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->lifecycle == LifecycleState::kRunning) {
      id = s->next_child_id++;
      Child child;
      child.node = std::move(node);
      child.socket = guard;
      child.supervisor_done = done;
      s->children.emplace(id, std::move(child));
      ++s->accepted_count;
      registered = true;
    }
  }
  if (!registered) {
    // 关闭竞态:不登记,就地收敛。
    node->Close();
    return;
  }
  Coro::makeTask([s, id, guard, done] { SuperviseChild(s, id, guard, done); });
}

// accept 循环 fiber:直接消费 nextConnection 流(而非 generate,以能观测 acceptError)。
// server 停监听(Close 调 srv->close())时流以 10ms 定时器检出并正常结束 → 循环退出;
// acceptError 以 qt.socket category 结束流 → 计接受错误。
void RunAcceptLoop(StatePtr s) {
  QPointer<QTcpServer> srv;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    srv = s->server;
  }
  if (srv) {
    auto stream = Coro::coro(srv.data()).nextConnection();
    for (;;) {
      Coro::Result<QTcpSocket*, std::error_code> r = Coro::await(stream);
      if (!r) {
        if (r.error().category() == Coro::detail::socket_error_category()) {
          std::lock_guard<std::mutex> lock(s->mutex);
          ++s->accept_error_count;
          s->last_error = make_error_code(TransportErrc::kConnection);
        }
        break;  // 停监听正常结束,或 acceptError → 退出 accept 循环。
      }
      QTcpSocket* sock = r.value();
      bool running = false;
      {
        std::lock_guard<std::mutex> lock(s->mutex);
        running = (s->lifecycle == LifecycleState::kRunning);
      }
      if (!running) {
        if (sock) sock->deleteLater();
        break;
      }
      if (sock) {
        HandleAccepted(s, sock);
      }
    }
  }
  s->accept_done.Complete(Status{});
}

}  // namespace

TcpServer::TcpServer(TcpServerConfig config, NodeFactory factory)
    : state_(std::make_shared<State>()) {
  state_->config = std::move(config);
  state_->factory = std::move(factory);
}

TcpServer::~TcpServer() { Close(); }

Status TcpServer::Start() {
  const auto s = state_;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->lifecycle == LifecycleState::kRunning) {
      return Status{};
    }
    if (s->lifecycle != LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }

  // 在调用者 fiber(节点执行域线程)内创建 QTcpServer(亲和纪律)。
  auto* srv = new QTcpServer();
  if (s->config.backlog > 0) {
    srv->setMaxPendingConnections(s->config.backlog);
  }
  QHostAddress addr;
  if (!addr.setAddress(QString::fromStdString(s->config.bind_addr))) {
    srv->deleteLater();
    std::lock_guard<std::mutex> lock(s->mutex);
    s->last_error = make_error_code(TransportErrc::kConfiguration);
    return s->last_error;
  }
  if (!srv->listen(addr, s->config.port)) {
    srv->deleteLater();
    const auto err = make_error_code(TransportErrc::kConnection);  // 监听失败(端口占用)。
    std::lock_guard<std::mutex> lock(s->mutex);
    s->last_error = err;
    return err;
  }

  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->server = srv;
    s->local_port = srv->serverPort();
    s->lifecycle = LifecycleState::kRunning;
    s->accept_spawned = true;
  }
  Coro::makeTask([s] { RunAcceptLoop(s); });
  return Status{};
}

Status TcpServer::Close() {
  const auto s = state_;
  QPointer<QTcpServer> srv;
  bool first_closer = false;
  bool accept_spawned = false;
  std::vector<Child> reaped;
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    if (s->lifecycle == LifecycleState::kClosed) {
      return Status{};
    }
    if (s->lifecycle == LifecycleState::kCreated) {
      s->lifecycle = LifecycleState::kClosed;  // 从未 Start:无 accept/子 node 可停。
      s->closed.Complete(Status{});
      return Status{};
    }
    if (s->lifecycle == LifecycleState::kRunning) {
      first_closer = true;
      s->lifecycle = LifecycleState::kClosing;
      srv = s->server;
      accept_spawned = s->accept_spawned;
      // 接管全部子 node 所有权(从共享列表移走 → supervisor 唤醒后见 id 不存在,退让)。
      for (auto& kv : s->children) {
        reaped.push_back(std::move(kv.second));
      }
      s->children.clear();
    }
  }
  if (!first_closer) {
    return s->closed.Wait();  // 后续关闭者共享同一收敛结果(多等待者)。
  }

  // 停监听 → accept 循环流结束、退出。
  if (srv) {
    srv->close();
  }
  if (accept_spawned) {
    s->accept_done.Wait();
  }
  // 逐个关闭子 node(阻塞至各自收敛;内层 abort socket 唤醒对应 supervisor)。
  for (auto& child : reaped) {
    if (child.node) {
      child.node->Close();
      child.node->WaitClosed();
    }
  }
  // 等每条 supervisor fiber 实际退出后再释放引用(不留悬空)。
  for (auto& child : reaped) {
    if (child.supervisor_done) {
      child.supervisor_done->Wait();
    }
  }
  reaped.clear();  // 析构已收敛 node。

  if (srv) {
    srv->deleteLater();
  }
  {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->server = nullptr;
    s->lifecycle = LifecycleState::kClosed;
  }
  s->closed.Complete(Status{});
  return Status{};
}

Status TcpServer::WaitClosed(OperationOptions options) {
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->lifecycle == LifecycleState::kCreated) {
      return make_error_code(TransportErrc::kInvalidState);
    }
  }
  return state_->closed.Wait(std::move(options));
}

bool TcpServer::IsListening() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->server && state_->server->isListening();
}

std::uint16_t TcpServer::LocalPort() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->local_port;
}

std::error_code TcpServer::LastError() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->last_error;
}

std::size_t TcpServer::AcceptedCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->accepted_count;
}

std::size_t TcpServer::ActiveNodeCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->children.size();
}

std::size_t TcpServer::ClosedConnectionCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->closed_connection_count;
}

std::size_t TcpServer::AcceptErrorCount() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->accept_error_count;
}

}  // namespace transport
