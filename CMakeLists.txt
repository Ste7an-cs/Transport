cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)
option(TRANSPORT_BUILD_EXAMPLES "Build example programs" OFF)

add_library(transport STATIC
  src/core/version.cpp
  src/codec/LengthFieldCodec.cpp
  src/codec/SystemCodec.cpp
  src/udp/UdpTransport.cpp
  src/tcp/TcpClientTransport.cpp
  src/tcp/TcpConnection.cpp
  src/tcp/TcpServerTransport.cpp
  src/serial/SerialTransport.cpp
  src/dds/DdsProviderRegistry.cpp
  src/dds/FakeDdsProvider.cpp
  src/dds/DdsTransport.cpp
  src/comm/ThreadExecutor.cpp
  src/comm/InteractionEngine.cpp
  src/comm/DdsNode.cpp
  src/comm/ProtocolNode.cpp
)
target_include_directories(transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(transport PUBLIC cxx_std_17)

find_package(Threads REQUIRED)

find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)
target_link_libraries(transport PUBLIC Qt5::Core Qt5::Network Qt5::SerialPort)

target_link_libraries(transport PUBLIC Threads::Threads)  # ThreadExecutor 仍用线程

find_package(fastcdr QUIET)
find_package(fastrtps QUIET)
if(fastrtps_FOUND AND fastcdr_FOUND)
  set(TRANSPORT_HAS_FASTDDS ON)
  message(STATUS "Fast DDS found: enabling FastDdsProvider")
  target_sources(transport PRIVATE src/dds/FastDdsRawType.cpp src/dds/FastDdsProvider.cpp)
  target_link_libraries(transport PUBLIC fastrtps fastcdr)
  target_compile_definitions(transport PUBLIC TRANSPORT_HAS_FASTDDS)
else()
  set(TRANSPORT_HAS_FASTDDS OFF)
  message(STATUS "Fast DDS NOT found: building without FastDdsProvider")
endif()

if(TRANSPORT_BUILD_TESTS)
  enable_testing()
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  add_subdirectory(third_party/googletest)
  include(GoogleTest)

  add_executable(transport_tests
    tests/test_main.cpp
    tests/version_test.cpp
    tests/result_test.cpp
    tests/codec/message_test.cpp
    tests/codec/length_field_codec_test.cpp
    tests/codec/system_codec_test.cpp
    tests/codec/datagram_codec_test.cpp
    tests/codec/system_datagram_codec_test.cpp
    tests/codec/dds_codec_test.cpp
    tests/transport/udp_transport_test.cpp
    tests/transport/tcp_transport_test.cpp
    tests/transport/serial_transport_test.cpp
    tests/transport/tcp_connection_test.cpp
    tests/transport/tcp_server_test.cpp
    tests/transport/combination_smoke_test.cpp
    tests/dds/dds_registry_test.cpp
    tests/dds/fake_dds_provider_test.cpp
    tests/dds/dds_transport_test.cpp
    tests/comm/thread_executor_test.cpp
    tests/comm/dds_node_test.cpp
    tests/comm/interaction_engine_test.cpp
    tests/comm/trace_sink_test.cpp
    tests/comm/interaction_trace_test.cpp
    tests/comm/protocol_node_test.cpp
    tests/comm/protocol_policy_test.cpp
  )
  target_include_directories(transport_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/comm)
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest)
  target_link_libraries(transport_tests PRIVATE util)
  if(TRANSPORT_HAS_FASTDDS)
    target_sources(transport_tests PRIVATE tests/dds/fastdds_provider_test.cpp)
  endif()
  gtest_discover_tests(transport_tests)
endif()

if(TRANSPORT_BUILD_EXAMPLES)
  add_executable(protocol_node_demo examples/protocol_node_demo.cpp)
  target_link_libraries(protocol_node_demo PRIVATE transport Threads::Threads)
  add_executable(protocol_udp_demo examples/protocol_udp_demo.cpp)
  target_link_libraries(protocol_udp_demo PRIVATE transport Threads::Threads)
  add_executable(dds_node_demo examples/dds_node_demo.cpp)
  target_link_libraries(dds_node_demo PRIVATE transport Threads::Threads)
endif()

# ---------------------------------------------------------------------------
# 协程栈(第二期):AsyncTask + boost.fiber。纯加性,可关。
# ---------------------------------------------------------------------------
option(TRANSPORT_BUILD_CORO "Build coroutine InteractionEngine (AsyncTask/boost.fiber)" ON)
if(TRANSPORT_BUILD_CORO)
  # AsyncTask 为 git 子模块(third_party/AsyncTask);首次构建前 `git submodule update --init`。
  set(ASYNCTASK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/AsyncTask" CACHE PATH "AsyncTask coroutine lib root (git submodule)")
  set(BOOST_LOCAL_ROOT "/usr/local" CACHE PATH "boost prefix with compiled fiber/context/thread/chrono")
  if(NOT EXISTS "${ASYNCTASK_DIR}/coro/all.hpp")
    message(FATAL_ERROR "AsyncTask 子模块未初始化:请运行 `git submodule update --init third_party/AsyncTask`(或 -DTRANSPORT_BUILD_CORO=OFF)。")
  endif()

  # 已编译 boost 静态库(顺序:fiber 依赖 context)。
  add_library(boost_fiber_local INTERFACE)
  target_include_directories(boost_fiber_local INTERFACE ${BOOST_LOCAL_ROOT}/include)
  target_link_libraries(boost_fiber_local INTERFACE
    ${BOOST_LOCAL_ROOT}/lib/libboost_fiber.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_context.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_thread.a
    ${BOOST_LOCAL_ROOT}/lib/libboost_chrono.a
    Threads::Threads)

  # AsyncTask 编译单元(含 Qt QObject → 需 AUTOMOC)。
  add_library(asynctask STATIC
    ${ASYNCTASK_DIR}/coro/executor/qtfiberthread.cpp
    ${ASYNCTASK_DIR}/coro/executor/fiberpool.cpp
    ${ASYNCTASK_DIR}/coro/detail/asyncdefine.cpp
    ${ASYNCTASK_DIR}/coro/task/fiberapplication.cpp
    ${ASYNCTASK_DIR}/coro/task/fibertask.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fibertaskqueue.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/qtlocalfiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/qtfiberscheduler.cpp
    ${ASYNCTASK_DIR}/coro/executor/scheduler/fiberthreadblock.cpp)
  set_target_properties(asynctask PROPERTIES AUTOMOC ON)
  target_include_directories(asynctask PUBLIC ${ASYNCTASK_DIR}/coro)
  target_compile_definitions(asynctask PUBLIC ASYNC_HAS_QTCORE)
  target_compile_features(asynctask PUBLIC cxx_std_17)
  target_link_libraries(asynctask PUBLIC Qt5::Core Qt5::Network boost_fiber_local)

  add_library(transport_coro STATIC
    src/coro/Error.cpp
    src/coro/Cancellation.cpp
    src/coro/InteractionEngine.cpp
    src/coro/ProtocolNode.cpp)
  target_include_directories(transport_coro PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_features(transport_coro PUBLIC cxx_std_17)
  target_link_libraries(transport_coro PUBLIC transport asynctask)

  if(TRANSPORT_BUILD_TESTS)
    add_executable(transport_coro_tests
      tests/coro/coro_test_main.cpp
      tests/coro/coro_harness_test.cpp)
    target_include_directories(transport_coro_tests PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/tests/coro)
    target_link_libraries(transport_coro_tests PRIVATE transport_coro asynctask GTest::gtest Qt5::Core)
    target_sources(transport_coro_tests PRIVATE tests/coro/engine_fire_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/engine_request_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/engine_keyfn_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/protocol_node_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/error_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/cancellation_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/transport_contract_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/transport_contract_new_first_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/transport_contract_legacy_first_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/shared_completion_test.cpp)
    target_sources(transport_coro_tests PRIVATE tests/coro/fake_coro_transport_test.cpp)
    add_test(NAME transport_coro_tests COMMAND transport_coro_tests)
  endif()
endif()
