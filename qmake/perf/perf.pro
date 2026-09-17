# ---------------------------------------------------------------------------
# transport_perf —— 对应 CMakeLists.txt 里的 add_executable(transport_perf ...)。
# 性能基准测试工具(ADR-0018 **D7**:独立可执行,不进 transport_tests),须**同时**登记
# CMake 与 qmake 两套清单——这是仓库既有的双清单义务。
#
# 依赖本库的那一整段(QT / INCLUDEPATH / AsyncTask 的头与 ASYNC_HAS_QTCORE /
# Fast DDS 探测 / -ltransport / boost 的链接顺序 / PRE_TARGETDEPS)全在根上的
# Transport.pri 里,由 TRANSPORT_LINK_STATIC = 1 选到「链预编静态库」那一支
# (ADR-0022 D3)。本文件只留 perf 独有的东西。
#
# 与 tests.pro 的两处差异:
#   * 不链 gtest、不链 -lutil(本工具不用 openpty);
#   * SOURCES 全部在 tools/perf/ 下,且 INCLUDEPATH 要含该目录(工具内部头用引号包含)。
# ---------------------------------------------------------------------------

include(../build_layout.pri)

TRANSPORT_LINK_STATIC = 1
include($$TRANSPORT_ROOT/Transport.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
TARGET = transport_perf
DESTDIR = $$TRANSPORT_BIN_DIR

INCLUDEPATH += $$TRANSPORT_ROOT/tools/perf

SOURCES += \
    $$TRANSPORT_ROOT/tools/perf/main.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfOptions.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfStats.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfLink.cpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfRun.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfServer.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfClient.cpp

HEADERS += \
    $$TRANSPORT_ROOT/tools/perf/PerfOptions.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfStats.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfWire.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfLink.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfSuites.hpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.hpp
