cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)

add_library(transport STATIC
  src/core/version.cpp
  src/framing/LengthFieldFramer.cpp
)
target_include_directories(transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(transport PUBLIC cxx_std_17)

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
  )
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  gtest_discover_tests(transport_tests)
endif()
