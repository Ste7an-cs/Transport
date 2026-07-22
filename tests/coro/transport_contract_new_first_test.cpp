#include <memory>
#include <type_traits>

#include "transport/coro/ITransport.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "transport/coro/ProtocolNode.hpp"

static_assert(std::is_constructible_v<transport::coro::InteractionEngine,
                                      std::shared_ptr<::transport::ITransport>,
                                      std::unique_ptr<::transport::ICodec>,
                                      std::unique_ptr<::transport::InteractionPolicy>>);
static_assert(std::is_constructible_v<transport::coro::ProtocolNode,
                                      std::shared_ptr<::transport::ITransport>, std::uint8_t, bool>);
