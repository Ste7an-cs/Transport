# ---------------------------------------------------------------------------
# ADR-0022 D5 —— 最小的 **qmake 下游**样例工程。
#
# 下游接入本库需要的**全部内容就是下面那一行 include**:源清单、INCLUDEPATH、
# QT、C++17、Fast DDS 的可选降级、AsyncTask 协程运行时,全在 Transport.pri 里。
# 下游不必知道 AsyncTask 的存在,也不必先构建本库。
#
# 本文件**刻意不 include `qmake/build_layout.pri`**,也**刻意不设
# `TRANSPORT_LINK_STATIC`**:那两样是仓库内部的构建设施,下游拿不到、也不该需要。
# 本例走的是 `Transport.pri` 的**默认模式 —— 源码级并入**(ADR-0022 D1/D3),
# 链接命令里**不应出现 `-ltransport`** —— 否则它就什么也没验证。
#
# 开关照常可用(ADR-0022 D4):
#   qmake CONFIG+=examples CONFIG+=no_fastdds ../transport.pro
#   qmake CONFIG+=examples FASTDDS_ROOT=/opt/fastdds ../transport.pro
# ---------------------------------------------------------------------------

include($$PWD/../../Transport.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
TARGET = transport_downstream_qmake

SOURCES += $$PWD/main.cpp
