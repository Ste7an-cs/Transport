# ADR-0022：提供 `Transport.pri`，qmake 下游按 AsyncTask 的方式一行接入

**状态：** Accepted
**日期：** 2026-09-15
**关联：** 补齐 **#232**（qmake 与 CMake 并存）留下的消费者侧缺口；形态参照本仓库已依赖的 **`third_party/AsyncTask/AsyncTask.pri`**；ADR-0018 **D7**（`transport_perf` 独立可执行——本 ADR 使其与 tests 共用同一份源清单）。

## 背景（Context）

#232 给仓库加了 qmake 构建，但**只解决了"我们自己怎么构建"，没解决"下游怎么用"**。

现状是一个 subdirs 工程：`transport.pro` → `qmake/lib/lib.pro`（编出 `libtransport.a`）+ gtest + tests + perf。一个想用本库的 qmake 工程，**没有任何受支持的接入方式**——它得自己摸清楚：`INCLUDEPATH` 要加哪两个目录、要不要 `include` AsyncTask 的 `.pri`、boost 四个库的链接顺序、Fast DDS 在不在、`TRANSPORT_HAS_FASTDDS` 该不该定义、`libtransport.a` 从哪来。

README 的「链接到你的工程」**只写了 CMake**（`add_subdirectory` + `target_link_libraries`），qmake 那栏是空的。

### 我们依赖的 AsyncTask 给出了现成的样板

```pro
# AsyncTask/example/basic/basic.pro —— 消费者只需一行
include($$PWD/../../AsyncTask.pri)
```

`AsyncTask.pri` 把 `HEADERS` / `SOURCES` / `INCLUDEPATH` / `DEFINES` / `LIBS` 直接并进**消费者的 target**——不建中间库、不需要构建顺序、不需要 `PRE_TARGETDEPS`。本仓库的 `qmake/lib/lib.pro` 正是这么用它的。

**我们对下游，应当提供同一形状。**

## 决策（Decision）

- **D1（根上提供 `Transport.pri`，源码级并入消费者 target）：** 形状与 `AsyncTask.pri` 一致：

  ```pro
  # 下游工程
  include($$PWD/path/to/transport/Transport.pri)
  ```

  它负责：库的全部 `SOURCES`、`INCLUDEPATH`（`include/` 与 `src/`）、`QT += core network serialport`、Fast DDS 的可选探测与 `TRANSPORT_HAS_FASTDDS`、以及**递归 include `AsyncTask.pri`**（下游因此不必知道 AsyncTask 的存在）。

  **不采用"链接预编静态库"的形态**：那要求下游先构建本库、处理 `PRE_TARGETDEPS` 与产物路径，而 qmake 没有 CMake 那套 target 依赖传递。源码级并入是 qmake 生态里的常规做法，也是我们已经在用的做法。

- **D2（`Transport.pri` 是唯一的源清单，`lib.pro` 改为 include 它）：** 现在 `qmake/lib/lib.pro` 自带一份源清单。本 ADR 之后它只剩：

  ```pro
  include(../build_layout.pri)     # 补正：原文写的是 common.pri，见 D6
  include($$TRANSPORT_ROOT/Transport.pri)
  TEMPLATE = lib
  CONFIG  += staticlib
  TARGET   = transport
  DESTDIR  = $$TRANSPORT_LIB_DIR
  ```

  **这是本 ADR 的实质收益**：qmake 侧的源清单由两份（若再加下游文档里的示例则三份）收敛为**一份**。仓库既有的"CMake 与 qmake 两份清单须手工同步"那条代价**不变**（仍是两份），但**不再增加第三份**。

- **D3（保留 `libtransport.a` 这个目标）：** 尽管下游走源码级并入，本仓库自己的 `tests` / `perf` / `examples` 仍链静态库——多个可执行共用一次编译结果，比每个都重编一遍库快得多。

  **由此形成两条路径**：仓库内部走 `lib.pro` 编出的静态库，下游走 `Transport.pri` 源码并入。**两者的源清单是同一份**（**D2**），不会漂移。

- **D4（Fast DDS 探测对下游同样生效，且可被下游覆盖）：** 下游同样享有"装了就编、没装就跳过"的降级；`FASTDDS_ROOT=` 与 `CONFIG+=no_fastdds` 两个开关对下游照常可用。

  > **补正（2026-09-17，随 D6 一并落地）：** 原文写的是"`Transport.pri` 内部 include `qmake/fastdds.pri`"。**实际做法改为内联**——探测逻辑整段并入 `Transport.pri`，`qmake/fastdds.pri` **删除**。原因见 **D6**：`fastdds.pri` 的四个消费者里有三个（tests / perf / examples）要的是"依赖而非源码"，只要 `Transport.pri` 支持这种模式（**D6**），单独留一个 `fastdds.pri` 就没有消费者了。`TRANSPORT_HAS_FASTDDS` 仍对外可见——`tests.pro` 靠它决定加不加三个 Fast DDS 测试源文件。

- **D5（提供一个最小可跑的下游样例并纳入 CI 口径）：** 在 `examples/` 下放一个**纯 qmake** 的最小工程，其 `.pro` 只有 `include(../../Transport.pri)` 与一个 `main.cpp`。

  > **理由**：`Transport.pri` 是给别人用的，**本仓库自己的构建不会经过它**（内部走静态库，**D3**）。没有这个样例，它一旦写错**没有任何信号**——这正是本仓库反复吃过亏的那类问题（README 的 `TopicKey` 曾编译不过、服务端示例曾缺 `endpoint`）。

- **D6（补充于 2026-09-17：`Transport.pri` 一个文件两种模式，`.pri` 由三个收敛为两个）：**

  **起因**：D1～D5 落地后，`qmake/` 下仍是三个 `.pri`（`common.pri` / `fastdds.pri` / `examples/example.pri`）加根上的 `Transport.pri`。清点后发现真正的问题不是"文件多"，而是 `tests.pro`、`perf.pro`、`example.pri` **三份逐字重复**同一段依赖声明：

  ```pro
  QT += core network serialport
  QT -= gui
  INCLUDEPATH += $$TRANSPORT_ROOT/include
  INCLUDEPATH += $$TRANSPORT_ROOT/third_party/AsyncTask/coro
  DEFINES += ASYNC_HAS_QTCORE
  LIBS += -L$$TRANSPORT_LIB_DIR -ltransport
  include(../fastdds.pri)
  LIBS += -L/usr/local/lib -lboost_fiber -lboost_context -lboost_thread -lboost_chrono
  LIBS += -lpthread
  PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libtransport.a
  ```

  这段与 `Transport.pri` 要给下游的东西**只差一件事**：给源码，还是给 `-ltransport`。

  **决策**：`Transport.pri` 支持两种模式，**共用同一份** `QT` / `INCLUDEPATH` / `DEFINES` / Fast DDS 探测 / boost+pthread：

  | | 设置 | 得到 |
  |---|---|---|
  | **默认（下游）** | 直接 `include` | 库的 `SOURCES`/`HEADERS` 并入消费者 target（**D1**） |
  | **静态（仓库内部）** | include 前置 `TRANSPORT_LINK_STATIC = 1` | 不加源码，改为 `-ltransport` + `PRE_TARGETDEPS`（**D3**） |

  静态模式下 **AsyncTask 的源码亦不并入**（已编进 `libtransport.a`，否则重复符号），但其 `INCLUDEPATH` / `DEFINES` 照给。静态模式依赖 `TRANSPORT_LIB_DIR`，若消费者未先 include `build_layout.pri`，`Transport.pri` 以 `error()` 明确报错，而非留给链接期。

  **连带**：`qmake/common.pri` 更名为 **`qmake/build_layout.pri`**——原名没说清它是什么，而它装的全是**仓库内部的构建布局**：输出目录（`OBJECTS_DIR`/`MOC_DIR`/`RCC_DIR`/`UI_DIR`）、产物路径（`TRANSPORT_LIB_DIR`/`TRANSPORT_BIN_DIR`）、以及 `QMAKE_CXXFLAGS_WARN_ON =`。其中**下游也需要**的（`CONFIG += c++17` 等）移入 `Transport.pri`。

  > ⚠ **`QMAKE_CXXFLAGS_WARN_ON =` 必须留在 `build_layout.pri`，不得进 `Transport.pri`。** 它是我们压第三方头文件噪音的内部手段；一旦进了对外的 `.pri`，**下游只要 include 就会被无声关掉全部编译告警**——既隐蔽又难查。这条是"内部设置"与"对外契约"必须分家的最好例证，也是本 ADR 不把三个 `.pri` 合成一个的原因。

  **结果**：`.pri` 由 3 个（+`Transport.pri`）变为 2 个（+`Transport.pri`）——`fastdds.pri` 并入（**D4** 补正），`common.pri` 更名并瘦身，`example.pri` 只剩 app 模板。三处重复的依赖块消除。

## 明确接受的代价

1. **下游每个 target 都会重编一遍库的源码。** 源码级并入的固有代价：下游若有多个可执行，库会被编多次。`AsyncTask.pri` 同理，是 qmake 生态的常态；要避免，下游可自行把它包成一个 staticlib 子工程（我们的 `lib.pro` 就是范例）。

2. **`src/` 会进下游的 `INCLUDEPATH`。** 源码级并入必须如此（`FastDdsProvider.hpp` 等私有头在 `src/` 下）。**后果是下游能 `#include` 到本库的私有头**——公共/私有的边界在 qmake 这条路径上**只剩约定，没有强制**。CMake 那条路径不受影响（`src/` 是 `PRIVATE`）。

3. **仍是两份源清单（CMake 与 qmake），本 ADR 不解决。** 它只保证 qmake 这一侧不再分裂成多份。

4. **（随 D6）`Transport.pri` 里多了一个模式分支。** 一个文件承担两种用途，读起来不如单一用途的文件直白；`TRANSPORT_LINK_STATIC` 必须在 include **之前**设置，这是 qmake 的求值顺序决定的、容易踩的坑。接受它，是因为另一条路（两个文件各写一份）恰恰会把 **D6** 消掉的那三份重复原样养回来。

## 影响（Consequences）

- **正面：** ① qmake 下游首次有受支持的接入方式，且是一行；② qmake 侧的源清单由两份收敛为一份；③ 下游自动获得 Fast DDS 的可选降级与两个开关；④ `Transport.pri` 有了持续验证的样例（**D5**）；⑤ 三处逐字重复的依赖块消除，`.pri` 由三个减为两个，且"对外契约"与"内部构建布局"不再混在同一个文件里（**D6**）。
- **负面（明确接受）：** 见上四条，其中 **代价 2（私有头对下游可见）是这条路径独有的**。
- **对 README：** 「链接到你的工程」补 qmake 一栏；「构建」一节说明 `Transport.pri` 的定位与两条路径的区别。
- **对 SDD：** 构建形态一节补 `Transport.pri`。

## 备选方案（Alternatives considered）

- **让下游链接预编的 `libtransport.a`**（形态对应 CMake 的 `add_subdirectory`）。**否决理由：** qmake 没有 target 依赖传递，下游要自己处理构建顺序、产物路径、`PRE_TARGETDEPS`、以及 boost/Qt/Fast DDS 的传递性链接——把我们内部已经解决过的复杂度原样推给下游。而 AsyncTask 已经示范了更简单的形态，我们又恰好是它的消费者。
- **`Transport.pri` 只导出 `INCLUDEPATH` + `LIBS`，源码仍由下游自行构建。** **否决理由：** 那等于什么都没解决——下游仍要知道编哪些文件、Fast DDS 在不在。
- **不提供 `.pri`，在 README 里写一段"qmake 下游该怎么配"的说明。** **否决理由：** 文档会漂移，而 `.pri` 是可执行的、且有 **D5** 的样例持续验证。本仓库已多次吃过"文档与代码漂移"的亏。
- **`lib.pro` 与 `Transport.pri` 各自维护一份源清单。** **否决理由：** 见 **D2**——那正是本 ADR 要消除的东西。
- **（D6 时）把三个 `.pri` 全部合进 `Transport.pri`，只留一个文件。** **否决理由：** 剩下两个 `.pri` 装的是**仓库内部的构建布局**，与"给下游的契约"性质相反，合进去会**泄漏**给每一个下游：输出目录（`OBJECTS_DIR`/`MOC_DIR`）会覆盖下游自己的布局；`QMAKE_CXXFLAGS_WARN_ON =` 更严重——下游 include 即被无声关掉全部编译告警。文件数从 2 减到 1 的收益，换不来这个风险。**"少一个文件"不是目标，"消掉重复、把内外分清"才是**——D6 两者都做到了。
- **（D6 时）让六个 `examples/` 也改走 `Transport.pri` 源码并入，从而让 `example.pri` 消失。** **否决理由：** 收益确实诱人（六个示例全变成真实下游，天天替我们验 `Transport.pri`，远强于 **D5** 那一个最小样例），但代价是每个示例都要全量重编一遍库（约 60 个源文件 × 6），qmake 全量构建显著变慢。**D5** 的样例已经够守住"写错就有信号"这条底线。
