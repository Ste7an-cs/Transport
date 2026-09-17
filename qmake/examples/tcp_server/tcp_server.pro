# 示例 ② tcp_server —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(tcp_server tcp_server/main.cpp common/ListeningTcpTransport.cpp)`。
#
# 多编一个 examples/common/ListeningTcpTransport.cpp:库里的 TcpTransport 是**客户端**
# 传输,TcpServer 本轮不做(ADR-0011 D10),服务端角色缺一个"监听 + 接受"的 ITransport
# 实现,故示例自带一个。**examples 自包含**:不引用 tools/ 下的任何源文件。

include(../example.pri)

TARGET = transport_example_tcp_server

SOURCES += \
    $$TRANSPORT_ROOT/examples/tcp_server/main.cpp \
    $$TRANSPORT_ROOT/examples/common/ListeningTcpTransport.cpp

HEADERS += \
    $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp \
    $$TRANSPORT_ROOT/examples/common/ListeningTcpTransport.hpp
