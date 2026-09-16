# 示例 ⑤ dds_service —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(dds_service dds_service/main.cpp)`。

include(../example.pri)

TARGET = transport_example_dds_service

SOURCES += $$TRANSPORT_ROOT/examples/dds_service/main.cpp

HEADERS += $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp
