cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)

add_library(transport STATIC
  src/core/version.cpp
  src/core/ReceiveQueue.cpp
  src/framing/LengthFieldFramer.cpp
  src/tcp/TcpConnectionImpl.cpp
  src/tcp/TcpClientImpl.cpp
  src/tcp/TcpServerImpl.cpp
  src/udp/UdpImpl.cpp
)
target_include_directories(transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(transport PUBLIC cxx_std_17)

include(FetchContent)
find_package(Threads REQUIRED)

FetchContent_Declare(
  asio
  GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
  GIT_TAG asio-1-30-2
)
FetchContent_MakeAvailable(asio)

add_library(asio_standalone INTERFACE)
target_include_directories(asio_standalone INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(asio_standalone INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio_standalone INTERFACE Threads::Threads)

target_link_libraries(transport PUBLIC asio_standalone)

if(TRANSPORT_BUILD_TESTS)
  enable_testing()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  include(GoogleTest)

  add_executable(transport_tests
    tests/version_test.cpp
    tests/result_test.cpp
    tests/interfaces_test.cpp
    tests/framing/length_field_framer_test.cpp
    tests/framing/frame_assembler_test.cpp
    tests/core/receive_queue_test.cpp
    tests/core/transport_core_test.cpp
    tests/tcp/asio_smoke_test.cpp
    tests/tcp/tcp_interfaces_test.cpp
    tests/tcp/tcp_connection_test.cpp
    tests/tcp/tcp_client_test.cpp
    tests/tcp/tcp_server_test.cpp
    tests/udp/udp_interfaces_test.cpp
    tests/udp/udp_transport_test.cpp
    tests/serial/serial_interfaces_test.cpp
  )
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  find_package(Threads REQUIRED)
  target_link_libraries(transport PUBLIC Threads::Threads)
  gtest_discover_tests(transport_tests)
endif()
