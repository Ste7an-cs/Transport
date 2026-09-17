# ---------------------------------------------------------------------------
# 六个示例 .pro 的公共部分 —— 对应 examples/CMakeLists.txt 里的
# `transport_add_example()` 函数。
#
# 依赖本库的那一整段(QT / INCLUDEPATH / AsyncTask 的头与 ASYNC_HAS_QTCORE /
# Fast DDS 探测 / -ltransport / boost 的链接顺序 / PRE_TARGETDEPS)全在根上的
# Transport.pri 里,由 TRANSPORT_LINK_STATIC = 1 选到「链预编静态库」那一支
# (ADR-0022 D3)。本文件只留示例独有的东西:examples/common 的头路径。
#
# 对照:examples/downstream_qmake/ 那个最小样例走的是 Transport.pri 的**默认模式**
# (源码级并入),那才是下游的接入路径(ADR-0022 D5)。这里的六个链静态库。
# ---------------------------------------------------------------------------

include($$PWD/../build_layout.pri)

# build_layout.pri 按「子工程在 qmake/<sub>/」算 TRANSPORT_BUILD_ROOT(OUT_PWD/../..);
# 示例多嵌了一层(qmake/examples/<name>/),故在此多退一级,产物仍汇到 build 根的
# lib/ 与 bin/,与 libtransport.a 对得上。
# ⚠ 这三行必须在 include Transport.pri **之前** —— 静态模式要用 TRANSPORT_LIB_DIR。
TRANSPORT_BUILD_ROOT = $$clean_path($$OUT_PWD/../../..)
TRANSPORT_LIB_DIR = $$TRANSPORT_BUILD_ROOT/lib
TRANSPORT_BIN_DIR = $$TRANSPORT_BUILD_ROOT/bin

TRANSPORT_LINK_STATIC = 1
include($$TRANSPORT_ROOT/Transport.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
DESTDIR = $$TRANSPORT_BIN_DIR

INCLUDEPATH += $$TRANSPORT_ROOT/examples/common
