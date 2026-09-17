# ---------------------------------------------------------------------------
# Transport.pri —— 本库在 qmake 下的**唯一依赖声明**(ADR-0022 D1/D2/D4)。
#
# 一处声明,两种消费方式:
#
#   ① 源码级并入(**默认**,下游走这条)—— 形状照
#      third_party/AsyncTask/AsyncTask.pri:SOURCES / INCLUDEPATH / QT / DEFINES /
#      LIBS 直接并进消费者的 target,不建中间库、不需要构建顺序、不需要
#      PRE_TARGETDEPS。qmake 没有 CMake 那套 target 依赖传递,这是 qmake 生态的常规
#      做法,也是我们消费 AsyncTask 时已经在用的做法。
#
#        include($$PWD/path/to/transport/Transport.pri)      # 下游只此一行
#
#   ② 链接预编的 libtransport.a(**仅本仓库内部**,ADR-0022 D3)—— 在 include
#      之前设 TRANSPORT_LINK_STATIC = 1:
#
#        include(../build_layout.pri)          # 提供 TRANSPORT_LIB_DIR
#        TRANSPORT_LINK_STATIC = 1
#        include($$TRANSPORT_ROOT/Transport.pri)
#
#      tests / perf / examples 用这条:多个可执行共用一次编译结果,比每个都重编
#      一遍库快得多。两种模式**共用同一份** QT / INCLUDEPATH / ASYNC_HAS_QTCORE /
#      Fast DDS 探测 / boost 链接顺序,故内部与下游的编译面不会漂移。
#
# 开关(ADR-0022 D4,对下游照常可用):
#   qmake CONFIG+=no_fastdds ...          # 强制不编 FastDdsProvider
#   qmake FASTDDS_ROOT=/opt/fastdds ...   # 换 Fast DDS 探测前缀(默认 /usr/local)
#
# 明确接受的代价(ADR-0022):① 下游每个 target 都重编一遍库的源码;② 源码并入
# 模式下 src/ 会进下游的 INCLUDEPATH(FastDdsProvider.hpp 等私有头在 src/ 下),
# 故公共/私有的边界在这条路径上**只剩约定**;③ CMake 与 qmake 仍是两份源清单。
#
# 注:qmake 不支持在 `\` 续行的赋值中间插注释(会把后半截一并注掉),故源清单按
# 注释切成数段 `SOURCES +=`,内容与顺序仍与 CMakeLists.txt 逐字对应。
#
# ⚠ 本文件**绝不**碰 QMAKE_CXXFLAGS_WARN_ON —— 关告警是本仓库内部的口径对齐
#   (见 qmake/build_layout.pri),下游不该因为 include 了本文件就被无声关掉全部
#   编译告警。
# ---------------------------------------------------------------------------

# 本文件位于仓库根,故 $$PWD 即仓库根。qmake 为每个被 include 的文件各自设 PWD,
# 下游 include 时它仍指向本仓库,不会跟着消费者的工程目录跑。
TRANSPORT_PRI_ROOT = $$clean_path($$PWD)

# 库要求 C++17(对应 CMakeLists.txt 的 target_compile_features(transport PUBLIC
# cxx_std_17));下游不必自己知道这件事。
CONFIG += c++17
CONFIG -= app_bundle

# serialport 是本库的公开依赖(CMake 里 Qt5::SerialPort 是 PUBLIC):即便消费者
# 只用 TCP,SerialTransport.o 里的 QSerialPort 符号也得有人解。
QT += core network serialport
QT -= gui

INCLUDEPATH += $$TRANSPORT_PRI_ROOT/include

# ---------------------------------------------------------------------------
# Fast DDS(可选依赖 —— SRS RT_IF_DDS)的探测,对应 CMakeLists.txt 里的
# find_package(fastcdr/fastdds QUIET) 那一段:找不到就**只**丢掉 FastDdsProvider,
# 框架其余能力照常构建可用。3.x 起包名由 fastrtps 改为 fastdds(ADR-0013 D14)。
#
# 这是优雅降级,不是报错 —— 缺席时仅 message() 提示。
#
# 输出(对 include 者可见):
#   TRANSPORT_HAS_FASTDDS = 1 / 空   —— tests.pro 靠它决定要不要加三个 Fast DDS
#                                       用例的源文件
#   命中时追加 DEFINES += TRANSPORT_HAS_FASTDDS 与 -lfastdds -lfastcdr
#
# CMake 里 TRANSPORT_HAS_FASTDDS 是 PUBLIC 的编译定义,故**链静态库的消费者也要
# 走这段探测**,两边的编译面才一致 —— 这正是两种模式共用本段的原因。
# ---------------------------------------------------------------------------
isEmpty(FASTDDS_ROOT): FASTDDS_ROOT = /usr/local

TRANSPORT_HAS_FASTDDS =
TRANSPORT_FASTDDS_LIBS =

contains(CONFIG, no_fastdds) {
    message("Fast DDS disabled by CONFIG+=no_fastdds: building without FastDdsProvider")
} else {
    exists($$FASTDDS_ROOT/include/fastdds):exists($$FASTDDS_ROOT/include/fastcdr) {
        TRANSPORT_HAS_FASTDDS = 1
        message("Fast DDS found at $$FASTDDS_ROOT: enabling FastDdsProvider")
        DEFINES += TRANSPORT_HAS_FASTDDS
        INCLUDEPATH += $$FASTDDS_ROOT/include
        # -l 不在这里追加:libtransport.a 引用 Fast DDS 的符号,链接模式下这两个
        # -l 必须排在 -ltransport **之后**(见文件末尾的链接顺序一段)。
        TRANSPORT_FASTDDS_LIBS = -L$$FASTDDS_ROOT/lib -lfastdds -lfastcdr
    } else {
        message("Fast DDS NOT found under $$FASTDDS_ROOT: 未找到 Fast DDS,不编 FastDdsProvider")
    }
}

# ---------------------------------------------------------------------------
# AsyncTask 协程运行时(强制依赖 —— RT_DESIGN_002)的**头与宏**,两种模式共用。
# 子模块须先 `git submodule update --init third_party/AsyncTask`。
# ---------------------------------------------------------------------------
!exists($$TRANSPORT_PRI_ROOT/third_party/AsyncTask/coro/all.hpp) {
    error("AsyncTask 子模块未初始化:请运行 `git submodule update --init third_party/AsyncTask`。")
}
INCLUDEPATH += $$TRANSPORT_PRI_ROOT/third_party/AsyncTask/coro
DEFINES += ASYNC_HAS_QTCORE

isEmpty(TRANSPORT_LINK_STATIC) {
    # -----------------------------------------------------------------------
    # 模式 ①:源码级并入(默认;下游走这条)。
    # -----------------------------------------------------------------------

    # 私有头在 src/ 下(FastDdsProvider.hpp 等),源码并入必须能见到它们。
    # 后果见文件头「明确接受的代价 ②」。
    INCLUDEPATH += $$TRANSPORT_PRI_ROOT/src

    SOURCES += \
        $$TRANSPORT_PRI_ROOT/src/core/version.cpp \
        $$TRANSPORT_PRI_ROOT/src/core/Error.cpp \
        $$TRANSPORT_PRI_ROOT/src/node/NodeBase.cpp \
        $$TRANSPORT_PRI_ROOT/src/node/ProtocolNode.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/udp/UdpTransport.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/tcp/TcpTransport.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/serial/SerialTransport.cpp \
        $$TRANSPORT_PRI_ROOT/src/codec/SystemCodec.cpp

    # FakeDdsProvider 零 FastDDS 依赖,恒在编译面内。DdsTransport 与 provider 注册表随
    # #203 的双队列重写回到编译面;DdsNode 随 #204 的整体重写(注册接口 / Dispatcher 键 /
    # 两段式 corr / 四个交互方法)一并回来。DDS 至此整条链路都在面内。
    SOURCES += \
        $$TRANSPORT_PRI_ROOT/src/io/dds/FakeDdsProvider.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/dds/DdsProviderRegistry.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/dds/DdsTransport.cpp \
        $$TRANSPORT_PRI_ROOT/src/node/DdsNode.cpp

    # Fast DDS 命中才把真实 provider 编进来。
    !isEmpty(TRANSPORT_HAS_FASTDDS) {
        SOURCES += \
            $$TRANSPORT_PRI_ROOT/src/io/dds/FastDdsRawType.cpp \
            $$TRANSPORT_PRI_ROOT/src/io/dds/FastDdsProvider.cpp
    }

    # 与 CMake 的一处结构差异:CMake 把 AsyncTask 单独编成 asynctask 静态库再 PUBLIC
    # 链给 transport;qmake 这边直接 include 上游的 AsyncTask.pri,让 AsyncTask 的
    # 目标文件一并进消费者的 target,链接面等价。AsyncTask.pri 已列全它自己的
    # HEADERS/SOURCES 与 INCLUDEPATH,清单由上游维护,此处不重抄。下游因此不必知道
    # AsyncTask 的存在(ADR-0022 D1)。
    #
    # ⚠ 必须放在 `QT += core network serialport` **之后**:AsyncTask.pri 内部有
    #   contains(QT, core) 判断,提前 include 会漏掉它的 Qt 那半边源文件与宏。
    include($$TRANSPORT_PRI_ROOT/third_party/AsyncTask/AsyncTask.pri)
} else {
    # -----------------------------------------------------------------------
    # 模式 ②:链接预编的 libtransport.a(仅本仓库内部,ADR-0022 D3)。
    #
    # AsyncTask 的**源码不能再并入** —— 它的目标文件已经在 libtransport.a 里,
    # 再编一遍就是重复符号。上面给过的 INCLUDEPATH 与 ASYNC_HAS_QTCORE 仍然有效。
    # -----------------------------------------------------------------------
    isEmpty(TRANSPORT_LIB_DIR) {
        error("TRANSPORT_LINK_STATIC 模式需要 TRANSPORT_LIB_DIR:请先 include qmake/build_layout.pri。")
    }
    LIBS += -L$$TRANSPORT_LIB_DIR -ltransport
    # 库变更触发重链。
    PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libtransport.a
}

# ---------------------------------------------------------------------------
# 链接顺序(务必保持):
#   [-ltransport] → Fast DDS → boost(fiber/context/thread/chrono) → -lpthread
#
# libtransport.a 里含 AsyncTask 的目标文件,而 AsyncTask 引用 boost 符号;boost 只有
# 静态归档,静态归档是左到右单遍解析,故 boost 必须排在 -ltransport **之后**。
# 源码并入模式下没有 -ltransport,目标文件直接在链接行最前面,结论不变。
#
# (源码并入模式里 AsyncTask.pri 自己也列了这四个 boost 的 -l;重复出现对静态归档
#  无害,只是链接命令长一点。)
# ---------------------------------------------------------------------------
LIBS += $$TRANSPORT_FASTDDS_LIBS
LIBS += -L/usr/local/lib -lboost_fiber -lboost_context -lboost_thread -lboost_chrono
LIBS += -lpthread
