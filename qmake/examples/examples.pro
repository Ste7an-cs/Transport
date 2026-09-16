# ---------------------------------------------------------------------------
# examples —— 六个示例程序的 qmake 汇总入口(对应 examples/CMakeLists.txt)。
#
# 只有 `qmake CONFIG+=examples ../transport.pro` 时根 transport.pro 才把本目录纳入
# SUBDIRS;不加开关时整段不生效。
#
# 每个示例一个子目录一个 .pro:**不能**把六个 .pro 平铺在本目录里用 `.file` 指,
# 那样六个子工程共享同一个 OUT_PWD,六个 `main.cpp` 的 `.o` 会互相覆盖。
#
# ⚠ 双清单义务:增删示例须同时改 `examples/CMakeLists.txt` 与本文件。
# ---------------------------------------------------------------------------

TEMPLATE = subdirs
CONFIG += ordered

SUBDIRS = tcp_client tcp_server udp_fanout

tcp_client.subdir = tcp_client
tcp_server.subdir = tcp_server
udp_fanout.subdir = udp_fanout
