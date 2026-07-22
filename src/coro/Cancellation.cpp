#include "transport/coro/Cancellation.hpp"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>

#include "await/awaitable.hpp"
#include "transport/coro/Error.hpp"

namespace transport::coro::detail {

struct CancellationCallback {
  enum class Phase { kActive, kExecuting, kFinished };

  boost::fibers::mutex mutex;
  boost::fibers::condition_variable condition;
  Phase phase{Phase::kActive};
  std::thread::id executing_thread;
  boost::fibers::fiber::id executing_fiber;
  std::function<void()> function;
};

struct CancellationState {
  std::atomic_bool cancelled{false};
  std::mutex mutex;
  std::vector<std::shared_ptr<CancellationCallback>> callbacks;
};

}  // namespace transport::coro::detail

namespace transport::coro {
namespace {

bool IsExecutingHere(const detail::CancellationCallback& callback) {
  return callback.executing_thread == std::this_thread::get_id() &&
         callback.executing_fiber == boost::this_fiber::get_id();
}

void RunCallback(
    const std::shared_ptr<detail::CancellationCallback>& callback) noexcept {
  {
    std::lock_guard<boost::fibers::mutex> lock(callback->mutex);
    if (callback->phase != detail::CancellationCallback::Phase::kActive) return;
    callback->phase = detail::CancellationCallback::Phase::kExecuting;
    callback->executing_thread = std::this_thread::get_id();
    callback->executing_fiber = boost::this_fiber::get_id();
  }

  try {
    if (callback->function) callback->function();
  } catch (...) {
    // One notification must not escape Cancel() or prevent later callbacks.
  }

  {
    std::lock_guard<boost::fibers::mutex> lock(callback->mutex);
    callback->phase = detail::CancellationCallback::Phase::kFinished;
    callback->executing_thread = {};
    callback->executing_fiber = {};
  }
  callback->condition.notify_all();
}

}  // namespace

CancellationRegistration::CancellationRegistration(
    std::weak_ptr<detail::CancellationState> state,
    std::shared_ptr<detail::CancellationCallback> callback)
    : state_(std::move(state)), callback_(std::move(callback)) {}

CancellationRegistration::~CancellationRegistration() { Reset(); }

CancellationRegistration::CancellationRegistration(
    CancellationRegistration&& other) noexcept
    : state_(std::move(other.state_)), callback_(std::move(other.callback_)) {}

CancellationRegistration& CancellationRegistration::operator=(
    CancellationRegistration&& other) noexcept {
  if (this != &other) {
    Reset();
    state_ = std::move(other.state_);
    callback_ = std::move(other.callback_);
  }
  return *this;
}

void CancellationRegistration::Reset() noexcept {
  auto callback = std::move(callback_);
  if (!callback) return;
  {
    std::unique_lock<boost::fibers::mutex> lock(callback->mutex);
    if (callback->phase == detail::CancellationCallback::Phase::kActive) {
      callback->phase = detail::CancellationCallback::Phase::kFinished;
    } else if (callback->phase ==
                   detail::CancellationCallback::Phase::kExecuting &&
               !IsExecutingHere(*callback)) {
      callback->condition.wait(lock, [&] {
        return callback->phase !=
               detail::CancellationCallback::Phase::kExecuting;
      });
    }
  }
  if (auto state = state_.lock()) {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->callbacks.erase(
        std::remove(state->callbacks.begin(), state->callbacks.end(), callback),
        state->callbacks.end());
  }
  state_.reset();
}

bool CancellationToken::IsCancellationRequested() const noexcept {
  return state_ && state_->cancelled.load(std::memory_order_acquire);
}

CancellationRegistration CancellationToken::Register(
    std::function<void()> function) const {
  if (!state_ || !function) return {};
  auto callback = std::make_shared<detail::CancellationCallback>();
  callback->function = std::move(function);
  bool invoke_now = state_->cancelled.load(std::memory_order_acquire);
  if (!invoke_now) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    invoke_now = state_->cancelled.load(std::memory_order_relaxed);
    if (!invoke_now) state_->callbacks.push_back(callback);
  }
  if (invoke_now) RunCallback(callback);
  return invoke_now ? CancellationRegistration{}
                    : CancellationRegistration{state_, std::move(callback)};
}

Status CancellationToken::Wait() const {
  if (!state_) return make_error_code(TransportErrc::kInvalidState);
  auto event = std::make_shared<Coro::Awaitable<void>>();
  auto registration = Register([event] {
    event->resolve();
    event->close();
  });
  auto result = event->await();
  if (result) return Status{};
  return result.error();
}

CancellationSource::CancellationSource()
    : state_(std::make_shared<detail::CancellationState>()) {}

bool CancellationSource::Cancel() noexcept {
  bool expected = false;
  if (!state_->cancelled.compare_exchange_strong(expected, true)) return false;
  std::vector<std::shared_ptr<detail::CancellationCallback>> callbacks;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    callbacks.swap(state_->callbacks);
  }
  for (const auto& callback : callbacks) {
    RunCallback(callback);
  }
  return true;
}

}  // namespace transport::coro
