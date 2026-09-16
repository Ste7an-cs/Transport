# 示例 ⑥ custom_codec —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(custom_codec custom_codec/main.cpp)`。
# LineCodec 是 header-only,故源清单里只有一个 main.cpp。

include(../example.pri)

TARGET = transport_example_custom_codec

SOURCES += $$TRANSPORT_ROOT/examples/custom_codec/main.cpp

HEADERS += \
    $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp \
    $$TRANSPORT_ROOT/examples/custom_codec/LineCodec.hpp
