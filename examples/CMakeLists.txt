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

# ② 只用公开面写的外部协议服务端。
#
# 多编一个 `tools/perf/AcceptedTcpTransport.cpp`:库里的 `TcpTransport` 是**客户端**
# 传输,`TcpServer` 本轮不做(ADR-0011 **D10**,其 .cpp 不在任一构建的源清单里),
# 而 `ProtocolNode` 只接 `ITransport&` —— 服务端角色缺一个"监听 + 接受"的实现。
# `tools/perf` 早已为同一原因写了一个,**直接复用而不再抄一份**(同一事实存两份必然漂移)。
# 它是工具/示例代码,不进 libtransport.a、不进公共头。
transport_add_example(tcp_server tcp_server/main.cpp
  ${CMAKE_SOURCE_DIR}/tools/perf/AcceptedTcpTransport.cpp)
target_include_directories(transport_example_tcp_server PRIVATE
  ${CMAKE_SOURCE_DIR}/tools/perf)

# ③ UDP 一对多 + 入站 endpoint 即发送方。
transport_add_example(udp_fanout udp_fanout/main.cpp)

# ④ DdsNode 发布-订阅。
transport_add_example(dds_pubsub dds_pubsub/main.cpp)
