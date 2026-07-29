#pragma once

#include <system_error>

#include "detail/result.hpp"

namespace transport {

template <typename T>
using Result = Coro::Result<T, std::error_code>;

using Status = Result<void>;

}  // namespace transport
