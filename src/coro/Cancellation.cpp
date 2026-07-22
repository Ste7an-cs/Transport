#include "transport/coro/Cancellation.hpp"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"
#include "transport/coro/Error.hpp"

namespace transport::coro::detail {

struct CancellationCallback {
  std::atomic_bool active{true};
  std::function<void()> function;
};

struct CancellationState {
  std::atomic_bool cancelled{false};
  std::mutex mutex;
  std::vector<std::shared_ptr<CancellationCallback>> callbacks;
};

}  // namespace transport::coro::detail

namespace transport::coro {

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
  callback->active.store(false, std::memory_order_release);
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
  if (invoke_now && callback->active.exchange(false)) callback->function();
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
    if (callback->active.exchange(false) && callback->function)
      callback->function();
  }
  return true;
}

}  // namespace transport::coro
