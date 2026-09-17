# ---------------------------------------------------------------------------
# libtransport.a —— 对应 CMakeLists.txt 里的 add_library(transport STATIC ...)。
#
# 源清单、INCLUDEPATH、QT、Fast DDS 探测与 AsyncTask 全在根上的 Transport.pri 里
# (ADR-0022 D2):那**同一份**清单既供本工程编静态库,也供 qmake 下游源码级并入,
# 两条路径不会漂移。本文件只剩「编成什么、叫什么、放哪」。
#
# 本目标保留的理由见 ADR-0022 D3:tests / perf / examples 链这个静态库,多个可执行
# 共用一次编译结果,比每个都重编一遍库快得多。
# ---------------------------------------------------------------------------

include(../common.pri)
include($$TRANSPORT_ROOT/Transport.pri)

TEMPLATE = lib
CONFIG += staticlib
TARGET = transport
DESTDIR = $$TRANSPORT_LIB_DIR
