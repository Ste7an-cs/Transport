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
  src/serial/SerialImpl.cpp
  src/dds/DdsProviderRegistry.cpp
  src/dds/DdsImpl.cpp
  src/TransportFactory.cpp
)
target_include_directories(transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_include_directories(transport PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
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

find_package(fastcdr QUIET)
find_package(fastrtps QUIET)
if(fastrtps_FOUND AND fastcdr_FOUND)
  set(TRANSPORT_HAS_FASTDDS ON)
  message(STATUS "Fast DDS found: enabling DDS provider")
else()
  set(TRANSPORT_HAS_FASTDDS OFF)
  message(STATUS "Fast DDS NOT found: building without FastDdsProvider")
endif()

if(TRANSPORT_HAS_FASTDDS)
  target_sources(transport PRIVATE src/dds/FastDdsRawType.cpp
                                   src/dds/FastDdsProvider.cpp)
  target_link_libraries(transport PUBLIC fastrtps fastcdr)
  target_compile_definitions(transport PUBLIC TRANSPORT_HAS_FASTDDS)
endif()

find_package(nlohmann_json 3.11 QUIET)
if(NOT nlohmann_json_FOUND)
  FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  )
  FetchContent_MakeAvailable(json)
endif()
target_link_libraries(transport PRIVATE nlohmann_json::nlohmann_json)

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
    tests/serial/serial_transport_test.cpp
    tests/dds/dds_interfaces_test.cpp
    tests/dds/dds_registry_test.cpp
    tests/dds/dds_impl_pubsub_test.cpp
    tests/dds/dds_impl_reqresp_test.cpp
    tests/factory/factory_create_test.cpp
    tests/factory/factory_json_test.cpp
  )
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  target_link_libraries(transport_tests PRIVATE util)
  find_package(Threads REQUIRED)
  target_link_libraries(transport PUBLIC Threads::Threads)
  if(TRANSPORT_HAS_FASTDDS)
    target_sources(transport_tests PRIVATE tests/dds/fastdds_rawtype_test.cpp
                                            tests/dds/fastdds_provider_test.cpp)
  endif()
  gtest_discover_tests(transport_tests)
endif()
