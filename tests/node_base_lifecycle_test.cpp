// -----------------------------------------------------------------------------
// node_base_lifecycle_test.cpp — `NodeBase` 的 Start / Close 交错(#220)
//
// 直接以一个最小 `NodeBase` 子类为被测对象:这里要验的是**基类的状态机**,不是任何协议
// 的实事。真实子类(`ProtocolNode` / `DdsNode`)的 `DoStart()` 全程无挂起点,拿它们做不出
// 这个交错——夹具把 `DoStart()` 停在一道闸上,窗口才张得开、才可判定。
//
// **可达性(判定于 #220,勿再退化为"防御性加固")**:
//   · **同一执行域内不可达**。两个真实 `DoStart()` 都是纯同步的(`DdsTransport::Declare*`
//     与 `AsyncRead()` 同步返回,spawn 走 `boost::fibers::launch::post` 不切换),协作调度下
//     `Close()` 插不进这段窗口。
//   · **跨 OS 线程今天就成立**。运行时是 M:N 的(SRS §1.3),`Close()` 的使用契约
//     (RT_LIFECYCLE_005)只要求"由节点外部调用",不要求同线程;`NodeBase` 用 `std::mutex`
//     守生命周期(ADR-0003 D8)正是为此。故本组用例按**跨线程**写:`Start()` 跑在一条
//     `std::thread` 上(宿主线程),`Close()` 由测试 fiber 所在线程发出。
//
// 修前的表现:窗口内的 `Close()` 看到 `Created` → 落 `Closed`、**不调 `DoClose()`**、报
// 关闭成功;`Start()` 回来无条件写回 `Running` —— 一个已答应关闭的节点被复活,而那次关闭
// 的收敛信号从未发出。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "transport/core/Error.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/node/NodeBase.hpp"

using transport::LifecycleState;
using transport::NodeBase;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

/// 最小 `NodeBase` 子类:三个钩子只记账,`DoStart()` 停在一道闸上把交错窗口张开。
///
/// 闸用 `std::mutex` + `std::condition_variable`(不是 fiber 同步件):两侧本就分处两条 OS
/// 线程,而被测的 `NodeBase` 也只用 `std::mutex` —— 全程不需要调度器推进,故测试 fiber 所在
/// 线程在闸上阻塞是安全的。
class GatedNode : public NodeBase {
 public:
  explicit GatedNode(bool start_succeeds) : start_succeeds_(start_succeeds) {}

  /// 等到 `DoStart()` 确实进来了——交错点由此**确定**,不靠 sleep 撞运气。
  void WaitEnteredDoStart() {
    std::unique_lock<std::mutex> lock(gate_mutex_);
    gate_cv_.wait(lock, [this] { return entered_do_start_; });
  }

  /// 放行 `DoStart()`,让 `Start()` 走它的收尾段。
  void ReleaseDoStart() {
    {
      std::lock_guard<std::mutex> lock(gate_mutex_);
      released_ = true;
    }
    gate_cv_.notify_all();
  }

  [[nodiscard]] int do_close_calls() const { return do_close_calls_.load(); }
  [[nodiscard]] int do_join_calls() const { return do_join_calls_.load(); }
  [[nodiscard]] LifecycleState Phase() const { return CurrentLifecycle(); }

 protected:
  Coro::Result<void> DoStart() override {
    {
      std::lock_guard<std::mutex> lock(gate_mutex_);
      entered_do_start_ = true;
    }
    gate_cv_.notify_all();
    {
      std::unique_lock<std::mutex> lock(gate_mutex_);
      gate_cv_.wait(lock, [this] { return released_; });
    }
    if (!start_succeeds_) {
      return make_error_code(TransportErrc::kConfiguration);
    }
    return Coro::Result<void>{};  // 真实子类在此已 spawn 了读循环。
  }

  Coro::Result<void> DoClose() override {
    do_close_calls_.fetch_add(1);
    return Coro::Result<void>{};
  }

  void DoJoin() override { do_join_calls_.fetch_add(1); }

 private:
  const bool start_succeeds_;
  std::mutex gate_mutex_;
  std::condition_variable gate_cv_;
  bool entered_do_start_{false};
  bool released_{false};
  std::atomic<int> do_close_calls_{0};
  std::atomic<int> do_join_calls_{0};
};

}  // namespace

// ★ #220 的那条交错:`DoStart()` 还在锁外跑,另一条线程把节点关了。
//
// 三条断言合起来才说明问题被修掉:节点**没被复活**(不是 Running)、那次关闭的**收敛信号
// 真的发出去了**(`DoClose()` 恰好一次)、`Start()` **不报成功**(它确实没启起来)。
TEST(NodeBaseLifecycle, CloseDuringDoStartWinsAndNodeIsNotResurrected) {
  GatedNode node(/*start_succeeds=*/true);

  Coro::Result<void> start_result{};
  std::thread host([&] { start_result = node.Start(); });
  node.WaitEnteredDoStart();

  // 交错点:`Start()` 卡在 `DoStart()` 里,宿主(另一条线程)发起关闭。
  EXPECT_EQ(node.Phase(), LifecycleState::kCreated);  // 启动未完成 ≠ 已关闭。
  EXPECT_TRUE(static_cast<bool>(node.Close()));       // 受理成功。
  EXPECT_TRUE(static_cast<bool>(node.Close()));       // 再关幂等,仍只受理一次。
  EXPECT_EQ(node.do_close_calls(), 0);  // 此刻还没发信号:有没有 fiber 要收尚且不知道。

  node.ReleaseDoStart();
  host.join();

  // 关闭赢:`Start()` 返 kInvalidState,节点从未进入 Running。
  ASSERT_FALSE(static_cast<bool>(start_result));
  EXPECT_EQ(start_result.error(), make_error_code(TransportErrc::kInvalidState));
  EXPECT_FALSE(node.IsRunning());
  EXPECT_EQ(node.Phase(), LifecycleState::kClosing);

  // ★ 收敛信号恰好发出一次——修前它一次都没发。
  EXPECT_EQ(node.do_close_calls(), 1);

  // 其后再 Close 撞上 Closing 的幂等分支,不会二次发信号。
  EXPECT_TRUE(static_cast<bool>(node.Close()));
  EXPECT_EQ(node.do_close_calls(), 1);

  // 收敛可正常完成:`WaitClosed()` join 到底并落 Closed。
  node.WaitClosed();
  EXPECT_EQ(node.do_join_calls(), 1);
  EXPECT_EQ(node.Phase(), LifecycleState::kClosed);

  // 已关闭的节点不许再启(相位不可回滚)。
  const Coro::Result<void> restart = node.Start();
  ASSERT_FALSE(static_cast<bool>(restart));
  EXPECT_EQ(restart.error(), make_error_code(TransportErrc::kInvalidState));
}

// 同一交错,但 `DoStart()` 失败:没 spawn 任何 fiber,故**不发**收敛信号,直接落 Closed。
//
// 返回值仍报**启动失败的成因**(kConfiguration)——那是调用方在别处问不到的事;"不可重试"
// 由相位表达:下一次 `Start()` 得 kInvalidState,而不是像无待关闭时那样退回 Created 重试。
TEST(NodeBaseLifecycle, CloseDuringFailedDoStartLandsClosedWithoutSignal) {
  GatedNode node(/*start_succeeds=*/false);

  Coro::Result<void> start_result{};
  std::thread host([&] { start_result = node.Start(); });
  node.WaitEnteredDoStart();
  EXPECT_TRUE(static_cast<bool>(node.Close()));
  node.ReleaseDoStart();
  host.join();

  ASSERT_FALSE(static_cast<bool>(start_result));
  EXPECT_EQ(start_result.error(), make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Phase(), LifecycleState::kClosed);
  EXPECT_EQ(node.do_close_calls(), 0);  // 无 fiber 可收,不该发信号。

  const Coro::Result<void> retry = node.Start();
  ASSERT_FALSE(static_cast<bool>(retry));
  EXPECT_EQ(retry.error(), make_error_code(TransportErrc::kInvalidState));
}

// 对照组:窗口内**没有**关闭,`Start()` 照旧置 Running,不因本次改动多发一次 `DoClose()`。
TEST(NodeBaseLifecycle, StartWithoutCloseStillReachesRunning) {
  GatedNode node(/*start_succeeds=*/true);

  Coro::Result<void> start_result{};
  std::thread host([&] { start_result = node.Start(); });
  node.WaitEnteredDoStart();
  node.ReleaseDoStart();
  host.join();

  EXPECT_TRUE(static_cast<bool>(start_result));
  EXPECT_TRUE(node.IsRunning());
  EXPECT_EQ(node.do_close_calls(), 0);

  EXPECT_TRUE(static_cast<bool>(node.Close()));
  EXPECT_EQ(node.do_close_calls(), 1);
  EXPECT_EQ(node.Phase(), LifecycleState::kClosing);
  node.WaitClosed();
  EXPECT_EQ(node.Phase(), LifecycleState::kClosed);
}

// 启动失败且**没有**待关闭:仍退回 Created,改配重试这条路不被本次改动堵上
// (RT_LIFECYCLE_007)。
TEST(NodeBaseLifecycle, FailedStartWithoutCloseStaysCreated) {
  GatedNode node(/*start_succeeds=*/false);

  Coro::Result<void> start_result{};
  std::thread host([&] { start_result = node.Start(); });
  node.WaitEnteredDoStart();
  node.ReleaseDoStart();
  host.join();

  ASSERT_FALSE(static_cast<bool>(start_result));
  EXPECT_EQ(start_result.error(), make_error_code(TransportErrc::kConfiguration));
  EXPECT_EQ(node.Phase(), LifecycleState::kCreated);
}
