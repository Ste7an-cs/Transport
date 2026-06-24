cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)

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
  src/comm/CommNode.cpp
  src/comm/DdsNode.cpp
  src/comm/ProtocolNode.cpp
)
target_include_directories(transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(transport PUBLIC cxx_std_17)

find_package(Threads REQUIRED)

# asio (standalone, header-only) —— vendored
add_library(asio_standalone INTERFACE)
target_include_directories(asio_standalone INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include)
target_compile_definitions(asio_standalone INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio_standalone INTERFACE Threads::Threads)
target_link_libraries(transport PUBLIC asio_standalone)
target_link_libraries(transport PUBLIC Threads::Threads)

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
    tests/version_test.cpp
    tests/result_test.cpp
    tests/codec/message_test.cpp
    tests/codec/length_field_codec_test.cpp
    tests/codec/system_codec_test.cpp
    tests/codec/datagram_codec_test.cpp
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
    tests/comm/comm_node_test.cpp
    tests/comm/dds_node_test.cpp
    tests/comm/interaction_engine_test.cpp
    tests/comm/protocol_node_test.cpp
  )
  target_include_directories(transport_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests/comm)
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  target_link_libraries(transport_tests PRIVATE util)
  if(TRANSPORT_HAS_FASTDDS)
    target_sources(transport_tests PRIVATE tests/dds/fastdds_provider_test.cpp)
  endif()
  gtest_discover_tests(transport_tests)
endif()
