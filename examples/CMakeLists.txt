# ---------------------------------------------------------------------------
# examples —— 六个完整可运行的示例程序(#255)。
#
# 只在根 CMakeLists.txt 的 `TRANSPORT_BUILD_EXAMPLES=ON` 时进入本文件;
# **不进 libtransport.a、不进 transport_tests**。
#
# ⚠ 源文件清单有两份(本文件与 `qmake/examples/*/*.pro`),增删示例须**同时改**
#   ——这是本仓库既有的双清单义务(README「构建」)。
#
# 各示例一律只链 `transport` 这一个库目标(它已 PUBLIC 传递 AsyncTask / Qt5 / boost 的
# 用法要求),另加 `examples/common` 的头路径。
# ---------------------------------------------------------------------------

# 一个示例 = 一个可执行,形状完全一致,收口到这个函数里。
function(transport_add_example name)
  set(target transport_example_${name})
  add_executable(${target} ${ARGN})
  target_include_directories(${target} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/${name})
  target_link_libraries(${target} PRIVATE transport)
  set_target_properties(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/examples)
endfunction()

# ① ProtocolNode + TcpTransport:四种交互模式。
transport_add_example(tcp_client tcp_client/main.cpp)
