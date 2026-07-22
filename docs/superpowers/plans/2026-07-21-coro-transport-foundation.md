# Coroutine Transport Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first independently testable slice of the coroutine-native stack: AsyncTask results, structured transport errors, cooperative cancellation, the internal coroutine transport contract, shared completion, and a deterministic fake transport.

**Architecture:** Keep the callback stack unchanged and add new types under `transport::coro`. The coroutine API aliases `Coro::Result<T, std::error_code>`; transport-specific failures use one stable `std::error_category`. `ITransport` performs one physical read/write at a time and owns no send queue; later node code will serialize public sends.

**Tech Stack:** C++17, CMake 3.16+, Qt 5.12+, AsyncTask `67b71a7`, boost.fiber, GoogleTest.

## Global Constraints

- Work only in `/home/ubuntu/david/transport/.worktrees/coro-foundation` on `feat/coro-foundation`.
- Preserve the existing callback `transport` target, APIs, and tests.
- New expected failures return AsyncTask `Coro::Result`; they do not throw.
- Do not add a transport write queue or a replacement thread executor.
- Do not implement concrete TCP, UDP, serial, DDS, node, codec, reconnect, hot-reload, or protocol-mode behavior in this plan.
- Run each coroutine test inside the existing AsyncTask test harness.
- Use `apply_patch` for edits and commit after every task.
- Run network-binding tests outside the restricted sandbox.

---

### Task 1: AsyncTask Result Alias and Structured Transport Errors

**Files:**
- Create: `include/transport/coro/Result.hpp`
- Create: `include/transport/coro/Error.hpp`
- Create: `src/coro/Error.cpp`
- Create: `tests/coro/error_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces: `transport::coro::Result<T>`, `transport::coro::Status`.
- Produces: `TransportErrc`, `transport_error_category()`, and `make_error_code(TransportErrc)`.
- Error category name is exactly `transport.error`; enumerator values start at 1 and remain stable.

- [ ] **Step 1: Write the failing error tests**

Create `tests/coro/error_test.cpp`:

```cpp
#include <array>
#include <system_error>
#include <gtest/gtest.h>

#include "transport/coro/Error.hpp"
#include "transport/coro/Result.hpp"

using transport::coro::Result;
using transport::coro::Status;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

TEST(CoroResult, UsesAsyncTaskValueAndVoidResults) {
  Result<int> value{42};
  ASSERT_TRUE(value);
  EXPECT_EQ(value.value(), 42);

  Status ok;
  EXPECT_TRUE(ok);

  Result<int> failed{make_error_code(TransportErrc::kIo)};
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), make_error_code(TransportErrc::kIo));
}

TEST(CoroError, AllRequiredErrorsAreStableAndDiagnostic) {
  constexpr std::array<TransportErrc, 13> errors{
      TransportErrc::kInvalidArgument, TransportErrc::kInvalidState,
      TransportErrc::kConfiguration, TransportErrc::kConnection,
      TransportErrc::kClosed, TransportErrc::kTimeout,
      TransportErrc::kCancelled, TransportErrc::kIo,
      TransportErrc::kFrame, TransportErrc::kCodec,
      TransportErrc::kResourceExhausted, TransportErrc::kUnsupported,
      TransportErrc::kInternal};

  int expected = 1;
  for (const auto error : errors) {
    const std::error_code code = make_error_code(error);
    EXPECT_STREQ(code.category().name(), "transport.error");
    EXPECT_EQ(code.value(), expected++);
    EXPECT_FALSE(code.message().empty());
  }
}

TEST(CoroError, SupportsStandardErrorCodeConstruction) {
  const std::error_code code = TransportErrc::kClosed;
  EXPECT_EQ(code, make_error_code(TransportErrc::kClosed));
}
```

Add `tests/coro/error_test.cpp` to `transport_coro_tests` in `CMakeLists.txt`.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON -DTRANSPORT_BUILD_CORO=ON
cmake --build build -j4
```

Expected: compilation fails because `transport/coro/Error.hpp` and `Result.hpp` do not exist.

- [ ] **Step 3: Implement the Result alias and error category**

Create `include/transport/coro/Result.hpp`:

```cpp
#pragma once

#include <system_error>
#include "detail/result.hpp"

namespace transport::coro {

template <typename T>
using Result = Coro::Result<T, std::error_code>;

using Status = Result<void>;

}  // namespace transport::coro
```

Create `include/transport/coro/Error.hpp`:

```cpp
#pragma once

#include <system_error>

namespace transport::coro {

enum class TransportErrc {
  kInvalidArgument = 1,
  kInvalidState,
  kConfiguration,
  kConnection,
  kClosed,
  kTimeout,
  kCancelled,
  kIo,
  kFrame,
  kCodec,
  kResourceExhausted,
  kUnsupported,
  kInternal,
};

const std::error_category& transport_error_category() noexcept;
std::error_code make_error_code(TransportErrc error) noexcept;

}  // namespace transport::coro

namespace std {
template <>
struct is_error_code_enum<transport::coro::TransportErrc> : true_type {};
}  // namespace std
```

Create `src/coro/Error.cpp`:

```cpp
#include "transport/coro/Error.hpp"

#include <string>

namespace transport::coro {
namespace {

class TransportErrorCategory final : public std::error_category {
 public:
  const char* name() const noexcept override { return "transport.error"; }

  std::string message(int value) const override {
    switch (static_cast<TransportErrc>(value)) {
      case TransportErrc::kInvalidArgument: return "invalid argument";
      case TransportErrc::kInvalidState: return "invalid state";
      case TransportErrc::kConfiguration: return "configuration error";
      case TransportErrc::kConnection: return "connection error";
      case TransportErrc::kClosed: return "closed";
      case TransportErrc::kTimeout: return "operation timed out";
      case TransportErrc::kCancelled: return "operation cancelled";
      case TransportErrc::kIo: return "I/O error";
      case TransportErrc::kFrame: return "frame error";
      case TransportErrc::kCodec: return "codec error";
      case TransportErrc::kResourceExhausted: return "resource exhausted";
      case TransportErrc::kUnsupported: return "unsupported operation";
      case TransportErrc::kInternal: return "internal error";
    }
    return "unknown transport error";
  }
};

}  // namespace

const std::error_category& transport_error_category() noexcept {
  static const TransportErrorCategory category;
  return category;
}

std::error_code make_error_code(TransportErrc error) noexcept {
  return {static_cast<int>(error), transport_error_category()};
}

}  // namespace transport::coro
```

Add `src/coro/Error.cpp` to `transport_coro` in `CMakeLists.txt`.

- [ ] **Step 4: Build and run the focused tests**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests --gtest_filter='CoroResult.*:CoroError.*'
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt include/transport/coro/Result.hpp include/transport/coro/Error.hpp src/coro/Error.cpp tests/coro/error_test.cpp
git commit -m "feat(coro): add structured transport errors"
```

---

### Task 2: Cooperative Cancellation

**Files:**
- Create: `include/transport/coro/Cancellation.hpp`
- Create: `src/coro/Cancellation.cpp`
- Create: `tests/coro/cancellation_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `Status` and `TransportErrc` from Task 1.
- Produces: move-only `CancellationRegistration`, copyable `CancellationToken`, and `CancellationSource`.
- `Cancel()` is idempotent and invokes internal notifications outside the state mutex.

- [ ] **Step 1: Write cancellation tests**

Create `tests/coro/cancellation_test.cpp`:

```cpp
#include <atomic>
#include <gtest/gtest.h>

#include "task/fibertask.h"
#include "transport/coro/Cancellation.hpp"
#include "transport/coro/Error.hpp"

using transport::coro::CancellationSource;
using transport::coro::Status;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

TEST(CoroCancellation, CancelIsIdempotentAndRunsNotificationOnce) {
  CancellationSource source;
  std::atomic_int calls{0};
  auto registration = source.token().Register([&] { ++calls; });

  EXPECT_FALSE(source.token().IsCancellationRequested());
  EXPECT_TRUE(source.Cancel());
  EXPECT_FALSE(source.Cancel());
  EXPECT_TRUE(source.token().IsCancellationRequested());
  EXPECT_EQ(calls.load(), 1);
  EXPECT_TRUE(static_cast<bool>(registration));
}

TEST(CoroCancellation, ResetPreventsNotification) {
  CancellationSource source;
  int calls = 0;
  auto registration = source.token().Register([&] { ++calls; });
  registration.Reset();
  source.Cancel();
  EXPECT_EQ(calls, 0);
}

TEST(CoroCancellation, WaitWakesAllFibers) {
  CancellationSource source;
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};

  auto one = Coro::makeTask([&] { first = source.token().Wait(); });
  auto two = Coro::makeTask([&] { second = source.token().Wait(); });
  EXPECT_TRUE(source.Cancel());
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
}

TEST(CoroCancellation, EmptyTokenWaitReturnsInvalidState) {
  transport::coro::CancellationToken token;
  const Status result = token.Wait();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kInvalidState));
}
```

Add the test source to `transport_coro_tests`.

- [ ] **Step 2: Build to verify the tests fail**

Run `cmake --build build -j4`.

Expected: compilation fails because `Cancellation.hpp` does not exist.

- [ ] **Step 3: Implement cancellation state and registration**

Create `include/transport/coro/Cancellation.hpp` with these exact public declarations:

```cpp
#pragma once

#include <functional>
#include <memory>

#include "transport/coro/Result.hpp"

namespace transport::coro {
namespace detail { struct CancellationState; struct CancellationCallback; }

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

}  // namespace transport::coro
```

Implement the state and race-sensitive methods in `src/coro/Cancellation.cpp` with this structure:

```cpp
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
```

Define the registration constructor, destructor, and move operations so that move assignment calls `Reset()` before taking the other record. `CancellationSource()` constructs one shared `CancellationState`; `IsCancellationRequested()` checks the atomic flag.

Implement `Wait()` as follows:

```cpp
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
```

Include `await/awaitable.hpp` and `transport/coro/Error.hpp` in the implementation. Add `src/coro/Cancellation.cpp` to `transport_coro`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests --gtest_filter='CoroCancellation.*'
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt include/transport/coro/Cancellation.hpp src/coro/Cancellation.cpp tests/coro/cancellation_test.cpp
git commit -m "feat(coro): add cooperative cancellation"
```

---

### Task 3: Transport Data Types and Internal Contract

**Files:**
- Create: `include/transport/coro/TransportTypes.hpp`
- Create: `include/transport/coro/ITransport.hpp`
- Create: `tests/coro/transport_contract_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `CancellationToken`, `Result<T>`, and the existing `transport::Endpoint`.
- Produces: `Datagram`, `SendUnit`, `OperationOptions`, `LifecycleState`, and internal `ITransport`.
- `Write` deliberately has no operation options and no queue.

- [ ] **Step 1: Write the compile-time/value contract tests**

Create `tests/coro/transport_contract_test.cpp`:

```cpp
#include <chrono>
#include <type_traits>
#include <gtest/gtest.h>

#include "transport/coro/ITransport.hpp"

using transport::Endpoint;
using transport::coro::Datagram;
using transport::coro::ITransport;
using transport::coro::LifecycleState;
using transport::coro::OperationOptions;
using transport::coro::SendUnit;

TEST(CoroTransportContract, DataUnitsOwnBytesAndAddressing) {
  Datagram incoming{{1, 2, 3}, Endpoint::Net("127.0.0.1", 9000)};
  EXPECT_EQ(incoming.bytes, (std::vector<std::uint8_t>{1, 2, 3}));
  EXPECT_EQ(incoming.source.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(incoming.source.port, 9000);

  SendUnit outgoing{{4, 5}, Endpoint::Topic("events")};
  EXPECT_EQ(outgoing.destination.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(outgoing.destination.topic, "events");
}

TEST(CoroTransportContract, OptionsUseSteadyClockDeadline) {
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + std::chrono::seconds(1);
  EXPECT_TRUE(options.deadline.has_value());
  EXPECT_FALSE(options.cancellation);
}

TEST(CoroTransportContract, InterfaceIsInternalPolymorphicSeam) {
  static_assert(std::has_virtual_destructor_v<ITransport>);
  EXPECT_NE(LifecycleState::kCreated, LifecycleState::kClosed);
}
```

- [ ] **Step 2: Build to verify the test fails**

Run `cmake --build build -j4`.

Expected: compilation fails because the two transport headers do not exist.

- [ ] **Step 3: Add the data and interface headers**

Create `include/transport/coro/TransportTypes.hpp`:

```cpp
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/coro/Cancellation.hpp"

namespace transport::coro {

struct Datagram {
  std::vector<std::uint8_t> bytes;
  Endpoint source;
};

struct SendUnit {
  std::vector<std::uint8_t> bytes;
  Endpoint destination;
};

enum class LifecycleState { kCreated, kRunning, kClosing, kClosed };

struct OperationOptions {
  using Clock = std::chrono::steady_clock;
  std::optional<Clock::time_point> deadline;
  CancellationToken cancellation;
};

}  // namespace transport::coro
```

Create `include/transport/coro/ITransport.hpp`:

```cpp
#pragma once

#include "transport/coro/Result.hpp"
#include "transport/coro/TransportTypes.hpp"

namespace transport::coro {

class ITransport {
 public:
  virtual ~ITransport() = default;
  virtual Status Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;
  virtual Status Write(SendUnit unit) = 0;
  virtual Status RequestClose() = 0;
  virtual Status WaitClosed(OperationOptions options = {}) = 0;
};

}  // namespace transport::coro
```

Add `tests/coro/transport_contract_test.cpp` to the coroutine test target.

- [ ] **Step 4: Build and run the focused tests**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests --gtest_filter='CoroTransportContract.*'
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt include/transport/coro/TransportTypes.hpp include/transport/coro/ITransport.hpp tests/coro/transport_contract_test.cpp
git commit -m "feat(coro): define coroutine transport contract"
```

---

### Task 4: Multi-Waiter Shared Completion

**Files:**
- Create: `include/transport/coro/SharedCompletion.hpp`
- Create: `tests/coro/shared_completion_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `Result<T>`, `OperationOptions`, and `CancellationToken::Register`.
- Produces: `SharedCompletion<T>::Complete(Result<T>)` and `Wait(OperationOptions)`.
- One waiter's timeout/cancellation only closes that waiter's local awaitable.

- [ ] **Step 1: Write shared-completion tests**

Create `tests/coro/shared_completion_test.cpp` with four tests:

```cpp
#include <chrono>
#include <gtest/gtest.h>

#include "task/fibertask.h"
#include "transport/coro/SharedCompletion.hpp"

using namespace std::chrono_literals;
using transport::coro::CancellationSource;
using transport::coro::OperationOptions;
using transport::coro::Result;
using transport::coro::SharedCompletion;
using transport::coro::TransportErrc;
using transport::coro::make_error_code;

TEST(CoroSharedCompletion, FirstCompletionWinsAndLateWaitSeesIt) {
  SharedCompletion<int> completion;
  EXPECT_TRUE(completion.Complete(Result<int>{7}));
  EXPECT_FALSE(completion.Complete(Result<int>{8}));
  auto result = completion.Wait();
  ASSERT_TRUE(result);
  EXPECT_EQ(result.value(), 7);
}

TEST(CoroSharedCompletion, WakesMultipleWaiters) {
  SharedCompletion<int> completion;
  Result<int> first{make_error_code(TransportErrc::kInternal)};
  Result<int> second{make_error_code(TransportErrc::kInternal)};
  auto one = Coro::makeTask([&] { first = completion.Wait(); });
  auto two = Coro::makeTask([&] { second = completion.Wait(); });
  EXPECT_TRUE(completion.Complete(Result<int>{9}));
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_EQ(first.value(), 9);
  EXPECT_EQ(second.value(), 9);
}

TEST(CoroSharedCompletion, CancellationOnlyEndsOneWaiter) {
  SharedCompletion<int> completion;
  CancellationSource source;
  OperationOptions cancelled_options;
  cancelled_options.cancellation = source.token();
  Result<int> cancelled{make_error_code(TransportErrc::kInternal)};
  auto task = Coro::makeTask([&] { cancelled = completion.Wait(cancelled_options); });
  source.Cancel();
  EXPECT_TRUE(task.get());
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error(), make_error_code(TransportErrc::kCancelled));
  EXPECT_TRUE(completion.Complete(Result<int>{11}));
  EXPECT_EQ(completion.Wait().value(), 11);
}

TEST(CoroSharedCompletion, DeadlineReturnsTransportTimeout) {
  SharedCompletion<int> completion;
  OperationOptions options;
  options.deadline = OperationOptions::Clock::now() + 10ms;
  auto result = completion.Wait(options);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), make_error_code(TransportErrc::kTimeout));
}
```

- [ ] **Step 2: Build to verify failure**

Run `cmake --build build -j4`.

Expected: compilation fails because `SharedCompletion.hpp` does not exist.

- [ ] **Step 3: Implement SharedCompletion as a header-only template**

Create `include/transport/coro/SharedCompletion.hpp`. Use a `State` containing a short-held `std::mutex`, a stored `shared_ptr<const Result<T>>`, a monotonically increasing waiter id, and a map from id to `weak_ptr<Coro::Awaitable<shared_ptr<const Result<T>>>>`.

The implementation must follow this exact race discipline:

```cpp
bool Complete(Result<T> result) {
  auto stored = std::make_shared<const Result<T>>(std::move(result));
  std::vector<std::shared_ptr<Waiter>> waiters;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->completion) return false;
    state_->completion = stored;
    for (auto& entry : state_->waiters)
      if (auto waiter = entry.second.lock()) waiters.push_back(std::move(waiter));
    state_->waiters.clear();
  }
  for (const auto& waiter : waiters) {
    waiter->resolve(stored);
    waiter->close();
  }
  return true;
}
```

`Wait()` must:

1. Return the stored result immediately when already complete.
2. Create and register one local waiter while holding the state mutex.
3. Register a cancellation callback that closes only that waiter with `kCancelled`.
4. Await indefinitely or until `deadline - Clock::now()`.
5. Convert AsyncTask `timed_out` to `TransportErrc::kTimeout` and explicitly close the local waiter.
6. Erase its waiter id under the state mutex before returning.
7. Return `kInternal` for an unexpected empty notification or `no_message` termination.

Include `await/awaitable.hpp`, plus the Task 1–3 headers. Do not hold `std::mutex` while awaiting or invoking an awaitable.

- [ ] **Step 4: Build and run focused tests**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests --gtest_filter='CoroSharedCompletion.*'
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt include/transport/coro/SharedCompletion.hpp tests/coro/shared_completion_test.cpp
git commit -m "feat(coro): add shared completion primitive"
```

---

### Task 5: Deterministic Coroutine FakeTransport

**Files:**
- Create: `tests/coro/fake_coro_transport.hpp`
- Create: `tests/coro/fake_coro_transport_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `ITransport`, `SharedCompletion<void>`, and AsyncTask `Awaitable`.
- Produces test-only controls: `Inject`, `InjectError`, `HoldWrites`, `ReleaseWrite`, `FailNextWrite`, `ActiveRead`, `ActiveWrite`, `state`, and `sent`.
- Does not start an OS thread or own a send queue.

- [ ] **Step 1: Write FakeTransport behavior tests**

Create `tests/coro/fake_coro_transport_test.cpp` with these test bodies (plus the necessary aliases and includes for `task/fibertask.h`, `coro_test_util.hpp`, and `fake_coro_transport.hpp`):

```cpp
TEST(CoroFakeTransport, StartIsIdempotentAndClosedCannotRestart) {
  FakeCoroTransport fake;
  EXPECT_TRUE(fake.Start());
  EXPECT_TRUE(fake.Start());
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(fake.WaitClosed());
  const auto restarted = fake.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
}

TEST(CoroFakeTransport, InjectCompletesReadWithSourceMetadata) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> received{make_error_code(TransportErrc::kInternal)};
  auto reader = Coro::makeTask([&] { received = fake.Read(); });
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveRead(); }));
  fake.Inject(Datagram{{1, 2}, Endpoint::Net("127.0.0.1", 7001)});
  ASSERT_TRUE(reader.get());
  ASSERT_TRUE(received);
  EXPECT_EQ(received.value().bytes, (std::vector<std::uint8_t>{1, 2}));
  EXPECT_EQ(received.value().source.host, "127.0.0.1");
  EXPECT_EQ(received.value().source.port, 7001);
}

TEST(CoroFakeTransport, ConcurrentReadReturnsInvalidState) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> first{make_error_code(TransportErrc::kInternal)};
  auto reader = Coro::makeTask([&] { first = fake.Read(); });
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveRead(); }));
  const auto second = fake.Read();
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kInvalidState));
  fake.RequestClose();
  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(first);
  EXPECT_EQ(first.error(), make_error_code(TransportErrc::kClosed));
}

TEST(CoroFakeTransport, ReadDeadlineAndCancellationReleaseReadSlot) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  OperationOptions timeout;
  timeout.deadline = OperationOptions::Clock::now() + 10ms;
  const auto timed_out = fake.Read(timeout);
  ASSERT_FALSE(timed_out);
  EXPECT_EQ(timed_out.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_FALSE(fake.ActiveRead());

  CancellationSource source;
  OperationOptions cancelled_options;
  cancelled_options.cancellation = source.token();
  Result<Datagram> cancelled{make_error_code(TransportErrc::kInternal)};
  auto reader = Coro::makeTask(
      [&] { cancelled = fake.Read(cancelled_options); });
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveRead(); }));
  source.Cancel();
  EXPECT_TRUE(reader.get());
  ASSERT_FALSE(cancelled);
  EXPECT_EQ(cancelled.error(), make_error_code(TransportErrc::kCancelled));
  EXPECT_FALSE(fake.ActiveRead());
}

TEST(CoroFakeTransport, ConcurrentPhysicalWriteReturnsInvalidState) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.HoldWrites();
  Status first{make_error_code(TransportErrc::kInternal)};
  auto writer = Coro::makeTask(
      [&] { first = fake.Write(SendUnit{{1}, Endpoint::Default()}); });
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveWrite(); }));
  const auto second = fake.Write(SendUnit{{2}, Endpoint::Default()});
  ASSERT_FALSE(second);
  EXPECT_EQ(second.error(), make_error_code(TransportErrc::kInvalidState));
  fake.ReleaseWrite();
  EXPECT_TRUE(writer.get());
  EXPECT_TRUE(first);
  ASSERT_EQ(fake.sent().size(), 1U);
  EXPECT_EQ(fake.sent()[0].bytes, (std::vector<std::uint8_t>{1}));
}

TEST(CoroFakeTransport, PartialWriteFailureClosesTransport) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  fake.FailNextWrite(make_error_code(TransportErrc::kIo), true);
  const auto written = fake.Write(SendUnit{{1}, Endpoint::Default()});
  ASSERT_FALSE(written);
  EXPECT_EQ(written.error(), make_error_code(TransportErrc::kIo));
  EXPECT_TRUE(fake.WaitClosed());
  EXPECT_EQ(fake.state(), LifecycleState::kClosed);
}

TEST(CoroFakeTransport, CloseWakesReadAndAllClosedWaiters) {
  FakeCoroTransport fake;
  ASSERT_TRUE(fake.Start());
  Result<Datagram> read{make_error_code(TransportErrc::kInternal)};
  Status first{make_error_code(TransportErrc::kInternal)};
  Status second{make_error_code(TransportErrc::kInternal)};
  auto reader = Coro::makeTask([&] { read = fake.Read(); });
  auto one = Coro::makeTask([&] { first = fake.WaitClosed(); });
  auto two = Coro::makeTask([&] { second = fake.WaitClosed(); });
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return fake.ActiveRead(); }));
  EXPECT_TRUE(fake.RequestClose());
  EXPECT_TRUE(reader.get());
  EXPECT_TRUE(one.get());
  EXPECT_TRUE(two.get());
  ASSERT_FALSE(read);
  EXPECT_EQ(read.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(first);
  EXPECT_TRUE(second);
}
```

Add the test source to `transport_coro_tests`.

- [ ] **Step 2: Build to verify failure**

Run `cmake --build build -j4`.

Expected: compilation fails because `fake_coro_transport.hpp` does not exist.

- [ ] **Step 3: Implement the fake with explicit single-operation slots**

Create `tests/coro/fake_coro_transport.hpp` as a header-only final class. Its private state is:

```cpp
mutable std::mutex mutex_;
LifecycleState state_{LifecycleState::kCreated};
bool active_read_{false};
bool active_write_{false};
bool hold_writes_{false};
std::deque<Result<Datagram>> queued_reads_;
std::shared_ptr<Coro::Awaitable<Datagram>> read_waiter_;
std::shared_ptr<Coro::Awaitable<void>> write_gate_;
std::optional<std::pair<std::error_code, bool>> next_write_error_;
std::vector<SendUnit> sent_;
SharedCompletion<void> closed_;
```

Implement behavior in this order:

- `Start`: `Created → Running`; repeated `Running` succeeds; `Closing/Closed` returns `kInvalidState`.
- `Read`: reject non-running or an occupied read slot; consume a queued result immediately or install one per-call awaitable. Register cancellation on that awaitable. On deadline, call `await_for`, then explicitly close the per-call awaitable with `kTimeout`. Always release the read slot before returning.
- `Inject`/`InjectError`: under the mutex either take the active waiter or append a `Result<Datagram>` to `queued_reads_`; resolve/close the waiter outside the mutex.
- `Write`: reject non-running or occupied write slot; optionally install and wait on `write_gate_`; after release, consume `next_write_error_` or append the unit to `sent_`; release the write slot on every exit.
- `FailNextWrite(error, partial)`: configure the next write. When `partial` is true, transition through closing and complete `closed_` after the active write slot is released.
- `RequestClose`: transition to closing, take the current read waiter/write gate under the mutex, close them with `kClosed` outside the mutex, and complete `closed_` when no active operation remains.
- `WaitClosed`: delegate to `closed_.Wait(options)`.
- Query helpers copy state under the mutex.

Use one private `FinishWrite(Status)` helper so success, error, gate close, and partial failure all release the active slot and complete close exactly once. Never invoke `SharedCompletion::Complete`, `Awaitable::resolve`, or `Awaitable::close` while holding `mutex_`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests --gtest_filter='CoroFakeTransport.*'
```

Expected: 7 tests pass with no hang.

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt tests/coro/fake_coro_transport.hpp tests/coro/fake_coro_transport_test.cpp
git commit -m "test(coro): add deterministic fake transport"
```

---

### Task 6: Full Regression and Requirement Check

**Files:**
- Verify only; modify implementation or tests only when a preceding requirement is not met.

**Interfaces:**
- Consumes all Task 1–5 deliverables.
- Produces a clean, independently testable first-stage branch.

- [ ] **Step 1: Run formatting and placeholder checks**

Run:

```bash
git diff origin/master --check
rg -n 'timeout:|conn:|codec:|frame:|io:|config:' include/transport/coro src/coro tests/coro/error_test.cpp tests/coro/cancellation_test.cpp tests/coro/shared_completion_test.cpp tests/coro/fake_coro_transport_test.cpp
```

Expected: `git diff --check` prints nothing. The prefix scan prints no new coroutine-foundation error classification.

- [ ] **Step 2: Run the coroutine test executable**

Run:

```bash
cmake --build build -j4
./build/transport_coro_tests
```

Expected: all existing and newly added coroutine tests pass.

- [ ] **Step 3: Run the full suite with local network permission**

Run:

```bash
ctest --test-dir build --output-on-failure
```

Expected: 100% tests pass. If restricted execution prevents TCP/UDP bind or Fast DDS network discovery, rerun the same built suite with local-network permission; do not change production code to accommodate sandbox denial.

- [ ] **Step 4: Verify architecture boundaries**

Run:

```bash
rg -n 'InteractionEngine|InteractionPolicy|ThreadExecutor|QThread|std::thread' include/transport/coro/Error.hpp include/transport/coro/Result.hpp include/transport/coro/Cancellation.hpp include/transport/coro/SharedCompletion.hpp include/transport/coro/TransportTypes.hpp include/transport/coro/ITransport.hpp src/coro/Error.cpp src/coro/Cancellation.cpp tests/coro/fake_coro_transport.hpp
```

Expected: no matches. Verify `ITransport::Write` has no `OperationOptions` parameter and `FakeTransport` has no send queue.

- [ ] **Step 5: Record final status**

Run:

```bash
git status --short --branch
git log --oneline origin/master..HEAD
```

Expected: clean `feat/coro-foundation` worktree and the five implementation commits after the design/dependency commits.
