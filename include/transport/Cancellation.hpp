#pragma once

#include <functional>
#include <memory>

#include "transport/Result.hpp"

namespace transport {
namespace detail {
struct CancellationState;
struct CancellationCallback;
}  // namespace detail

class CancellationRegistration {
 public:
  CancellationRegistration() = default;
  ~CancellationRegistration();
  CancellationRegistration(CancellationRegistration&&) noexcept;
  CancellationRegistration& operator=(CancellationRegistration&&) noexcept;
  CancellationRegistration(const CancellationRegistration&) = delete;
  CancellationRegistration& operator=(const CancellationRegistration&) = delete;

  void Reset() noexcept;
  explicit operator bool() const noexcept { return callback_ != nullptr; }

 private:
  friend class CancellationToken;
  CancellationRegistration(std::weak_ptr<detail::CancellationState> state,
                           std::shared_ptr<detail::CancellationCallback> callback);
  std::weak_ptr<detail::CancellationState> state_;
  std::shared_ptr<detail::CancellationCallback> callback_;
};

class CancellationToken {
 public:
  CancellationToken() = default;
  bool IsCancellationRequested() const noexcept;
  CancellationRegistration Register(std::function<void()> callback) const;
  Status Wait() const;
  explicit operator bool() const noexcept { return state_ != nullptr; }

 private:
  friend class CancellationSource;
  explicit CancellationToken(std::shared_ptr<detail::CancellationState> state)
      : state_(std::move(state)) {}
  std::shared_ptr<detail::CancellationState> state_;
};

class CancellationSource {
 public:
  CancellationSource();
  CancellationToken token() const { return CancellationToken{state_}; }
  bool Cancel() noexcept;

 private:
  std::shared_ptr<detail::CancellationState> state_;
};

}  // namespace transport
