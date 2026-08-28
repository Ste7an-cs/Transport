cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)

find_package(Threads REQUIRED)
find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)

# ---------------------------------------------------------------------------
# AsyncTask 协程运行时（强制依赖 —— RT_DESIGN_002）。
# AsyncTask 为 git 子模块（third_party/AsyncTask）；首次构建前须
# `git submodule update --init third_party/AsyncTask`。
# ---------------------------------------------------------------------------
set(ASYNCTASK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/AsyncTask" CACHE PATH "AsyncTask coroutine lib root (git submodule)")
set(BOOST_LOCAL_ROOT "/usr/local" CACHE PATH "boost prefix with compiled fiber/context/thread/chrono")
if(NOT EXISTS "${ASYNCTASK_DIR}/coro/all.hpp")
  message(FATAL_ERROR "AsyncTask 子模块未初始化:请运行 `git submodule update --init third_party/AsyncTask`。")
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

# ---------------------------------------------------------------------------
# 单一 transport 库(协程原生目标骨架):纯字节传输 + codec + DDS provider,
# 以 AsyncTask 为强制运行时。
# ---------------------------------------------------------------------------
add_library(transport STATIC
  src/core/version.cpp
  src/core/Error.cpp
  src/core/Cancellation.cpp
  src/node/NodeBase.cpp
  src/node/ProtocolNode.cpp
  src/io/udp/UdpTransport.cpp
  src/io/tcp/TcpTransport.cpp
  src/io/serial/SerialTransport.cpp
  src/codec/LengthFieldCodec.cpp
  src/codec/SystemCodec.cpp
)
target_include_directories(transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(transport PUBLIC cxx_std_17)
target_link_libraries(transport PUBLIC asynctask Qt5::Core Qt5::Network Qt5::SerialPort Threads::Threads)

find_package(fastcdr QUIET)
# redesign 分支:工程收窄到 UDP + ProtocolNode,DDS provider 一并排除。
find_package(fastrtps QUIET)
if(FALSE)  # redesign: 暂时禁用 FastDDS(DdsTransport 已排除)
  set(TRANSPORT_HAS_FASTDDS ON)
  message(STATUS "Fast DDS found: enabling FastDdsProvider")
  target_sources(transport PRIVATE src/io/dds/FastDdsRawType.cpp src/io/dds/FastDdsProvider.cpp)
  target_link_libraries(transport PUBLIC fastrtps fastcdr)
  target_compile_definitions(transport PUBLIC TRANSPORT_HAS_FASTDDS)
else()
  set(TRANSPORT_HAS_FASTDDS OFF)
  message(STATUS "Fast DDS NOT found: building without FastDdsProvider")
endif()

# ---------------------------------------------------------------------------
# 单一测试目标:全部 tests/* 纳入一个可执行文件,在 AsyncTask fiber 调度器内跑。
# ---------------------------------------------------------------------------
if(TRANSPORT_BUILD_TESTS)
  enable_testing()
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  add_subdirectory(third_party/googletest)

  add_executable(transport_tests
    tests/coro_test_main.cpp
    # —— 重设计(redesign 分支)后仍成立的用例 ——
    tests/version_test.cpp
    tests/error_test.cpp
    tests/cancellation_test.cpp
    tests/observability_test.cpp
    tests/dispatcher_test.cpp
    tests/coro_harness_test.cpp
    tests/udp_transport_test.cpp
    tests/tcp_transport_test.cpp
    tests/tcp_transport_read_test.cpp
    tests/tcp_transport_reconnect_test.cpp
    tests/serial_transport_test.cpp
    tests/protocol_node_test.cpp
    tests/protocol_node_tcp_e2e_test.cpp
    # —— 以下两个 TCP 用例继续停摆，各有明确的待决依据（#181 的判定）——
    #   tcp_client_reconfig_test（`ApplyConfig` 热更新的去留待 ADR-0011 D11 裁决）
    #   tcp_server_accept_test（`TcpServer` 本轮不做，ADR-0011 D10）
    # —— 以下用例仍在旧 ITransport / NodeBase 面上，随重设计逐个迁移后再放回 ——
    #   protocol_node_handler_test / protocol_node_lifecycle_test
    #   protocol_node_capacity_test / protocol_node_udp_test / transport_contract_test
    #   fake_coro_transport_test / send_semantics_fake_test
    #   shared_completion_test（SharedCompletion 已随手搓同步件一并删除）
    #   handler_loop_test（HandlerLoop 已随 ADR-0009 废止内建 handler 通道而删除）
    tests/codec/message_test.cpp
    tests/codec/length_field_codec_test.cpp
    tests/codec/system_codec_test.cpp
    tests/codec/datagram_codec_test.cpp
    tests/codec/system_datagram_codec_test.cpp
  )
  target_include_directories(transport_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  # 供 observability_test.cpp 的协议无关自审读取 core/ 头文件源码文本核验(而非硬编码 grep)。
  target_compile_definitions(transport_tests PRIVATE
    TRANSPORT_INCLUDE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/include")
  target_link_libraries(transport_tests PRIVATE transport asynctask GTest::gtest Qt5::Core Qt5::Network util)
  add_test(NAME transport_tests COMMAND transport_tests)
endif()
