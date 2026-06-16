# 借助 Claude Code 完成通信中间件:设计 → 实现 → 测试 → 迭代

> 本文复盘 `transport`(C++17 通信中间件)从零到现在,如何借助 **Claude Code** + **Superpowers SDD 工作流**
> 完成需求设计、编码实现、测试验证与持续迭代。所有结论可在仓库的 `docs/superpowers/specs/`、
> `docs/superpowers/plans/`、`CHANGELOG.md`、git 历史与 GitHub PR 中对照。

---

## 1. 项目与协作模式一句话

- **项目:** 把"怎么传"(TCP/UDP/串口/DDS 的连接、分帧、收发、线程)与"传什么"(协议帧格式)彻底解耦的 C++17 通信中间件,不抛异常、接口零第三方依赖、离线自包含构建。
- **协作模式:** 我(开发者)用自然语言提出意图与决策,Claude Code 以 **Superpowers SDD(Spec-Driven Development)** 的固定工作流推进:**头脑风暴定设计 → 写计划 → 子代理逐任务实现并双重审查 → 收尾合并**。每一步都有"人来拍板、AI 来落地与把关"的清晰分工。

---

## 2. 方法论核心:Superpowers SDD 四段闭环

每一个特性/重构,都走同一条闭环。这正是"设计/实现/测试/迭代"四件事被结构化的方式:

```
brainstorming(设计)
   └─ 一次一个问题地澄清需求 → 提 2~3 个方案权衡 → 给出推荐 → 我拍板
   └─ 产出:docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md(我 review 后定稿)
        ↓
writing-plans(把设计拆成可执行)
   └─ 拆成 bite-sized TDD 任务:每步含失败测试 + 完整实现代码 + 运行命令 + 提交
   └─ 无占位符;写完自审(spec 覆盖 / 占位扫描 / 类型一致)
   └─ 产出:docs/superpowers/plans/YYYY-MM-DD-<feature>.md
        ↓
subagent-driven-development(实现 + 测试 + 审查)
   └─ 每个任务派一个全新子代理(Opus)在隔离上下文里实现(严格 TDD)
   └─ 每任务两段审查:① spec 符合性(不多不少做对) ② 代码质量(并发/生命周期/泄漏)
   └─ 全部任务完成后,再做一次"整分支最终审查"
        ↓
finishing-a-development-branch(收尾)
   └─ 先验证测试 → 给出 合并/PR/保留/丢弃 四选项 → 执行 → 清理分支
```

**为什么有效:**
- **人只在"决策点"介入**(选方案、定边界、拍板),不陷进实现细节;
- **AI 把"机制"做满**(完整代码、测试、审查),且**每个任务用全新子代理**,上下文互不污染;
- **两道质量闸**(spec 符合 + 代码质量)+ **最终整分支审查**,把问题挡在合并前。

---

## 3. 四件事分别怎么用 Claude Code

### 3.1 设计(brainstorming → spec)
- **一次只问一个问题**,多用多选题让我快速拍板(例:"DDS 怎么映射到现架构?"→ ITransport+Subscribe 扩展 / 独立 pub-sub 抽象 / 先看代码再定)。
- **每个决策都先摆权衡再给推荐**,而不是替我做主。典型例子:
  - 把 `ITransport` 砍成纯字节管道 vs 保留富接口 → 选"纯字节管道";
  - DDS `OnBytes` 全局串行 vs 按 topic 并行 → 我质疑"为什么非要串行",AI 收回全局串行,改"同 topic 有序、跨 topic 并发,直接在 listener 线程交付"。
- **设计落成 spec 文档**并提交,我 review 后才进入下一步(User Review Gate)。
- 这一阶段产出了:总体架构 spec、Endpoint 统一寻址、topic 路由、底层两层重构、TCP Server、DDS 等多份设计文档,以及对外的《需求规格说明书(SRS)》《设计说明书(SDD)》。

### 3.2 实现(writing-plans → 子代理 TDD)
- **计划即代码**:writing-plans 把设计拆成一连串 2~5 分钟的小步,每步都给**完整可粘贴的代码**(不是"自行实现"),并标注"先写会失败的测试、跑一遍确认失败、再实现、再确认通过、提交"。
- **严格 TDD**:子代理先写测试看它失败,再写最小实现让它通过 —— 红→绿→提交,每个任务一串小提交。
- **新鲜子代理 + 精准上下文**:控制方(主对话)把"任务全文 + 场景上下文"喂给子代理,子代理不继承历史对话,专注把一个任务做对。

### 3.3 测试(贯穿始终,不是事后补)
- 测试是计划里的**第一步**,不是收尾补的。每个组件都自带:
  - **单元测试**(表驱动:半包/粘包/越界/大小端…);
  - **回环集成**(UDP 回环、TCP 裸 asio echo server、串口 `openpty` 虚拟串口对、DDS 进程内总线);
  - **解耦验证**(codec 测试不链接 transport,transport 测试不引入 codec)。
- **离线、确定性**:第三方依赖(asio/json/googletest)vendored 进 `third_party/`,零联网即可全测;DDS 逻辑用 `FakeDdsProvider` 进程内总线测试,**不依赖真实 Fast DDS** 也能全绿。
- **抗 flaky**:并发/回环用例由审查子代理**连跑多次压测**(如 TCP 用例 30 次零失败)确认稳定。

### 3.4 迭代(两段审查 + 最终审查 + 收尾)
- **每个任务两段审查**(都由独立子代理做,且"不信任实现者报告、亲自读代码+跑构建"):
  1. **spec 符合性**:做的是不是正好是要求的(不缺、不多、没误解);
  2. **代码质量**:并发竞争、生命周期/泄漏、UB、命名、单一职责。
- 审查发现问题 → 同一实现者修 → 复审,直到通过才进下一个任务。
- **最终整分支审查**:干净从零构建零告警、全量测试连跑、解耦/残留检查、提交规范,给出 READY TO MERGE 结论。
- **收尾**:验证测试 → 选择 合并/PR → 用 `gh` 建 PR → 合并后同步 master、删本地+远程分支、prune。

---

## 4. 开发历程(里程碑时间线)

### 第一阶段:0.1.0 —— 从基座搭到完整可用(CHANGELOG 有完整记录)
1. **Foundation 基座** —— 核心接口、`Result`/`Message`、长度字段分帧、滚动缓冲切帧、FIFO 三模式接收、CMake+GoogleTest 骨架。
2. **TCP**(客户端连接+超时+指数退避重连 / 服务端 acceptor+每客户端独立 transport)。
3. **UDP**(单播/组播/广播单类)、**串口**(termios+流式分帧)。
4. **DDS**(Fast DDS,pub-sub 多 topic + req-resp 关联超时;`IDdsProvider` 抽象 + `FakeDdsProvider` 让逻辑零依赖可测)。
5. **TransportFactory**(类型化创建 + JSON 配置严格校验)。
6. **离线自包含构建** —— 三方依赖 vendored,移除 FetchContent;Fast DDS 作唯一可选外部依赖(`find_package` 自动探测)。
7. **Endpoint 统一寻址** —— 引入中立 `Endpoint`,基类句柄即可寻址发送,删除形态不一的旧 `SendTo`/`Send(data,topic)`。
8. **topic 路由**、**`TransportBase`→`TransportCore` 组合化重构**(用组合消除接口菱形)。
9. **文档整理** —— SRS/SDD/CHANGELOG;**release 0.1.0**(打 `v0.1.0` 标签)。

### 第二阶段:0.2.0 —— 大版本重构(本轮,尚未发版)
- **健壮性优化**:`Result`/`Status` 加 `[[nodiscard]]`(忽略错误改为编译期告警)+ 清理全部静默丢错;DDS 空 topics 防越界;`ParseSubFramer` 去重。
- **底层两层重构(PR #1)**:把臃肿的富 `ITransport` 拆成 **Transport(纯字节管道)+ ICodec(线缆格式,承载 `kind`/`correlation_id`)** 两个解耦层;破坏性移除旧 `TransportCore`/topic 路由/req-resp/服务端/DDS;3 个 codec + 3 个纯管道 transport;`v0.1.0` 留底。**11 任务 TDD、最终审查 27/27 绿、零告警。**
- **TCP Server 底层(PR #2)**:接受器 `TcpServerTransport` 产出 per-connection `ITransport`;抽出共享 `TcpConnection`;重构客户端复用之。**4 任务、33/33 绿 + TCP 用例 30 次压测零 flaky。**
- **DDS 底层(进行中)**:设计已定(ITransport+Subscribe 扩展 + provider 抽象 + 按 topic 并行交付,只 pub-sub),正在写实现计划。
- 约定:**DDS 底层完成前不正式发 0.2.0**;之后再做 System 交互模式层。

---

## 5. 贯穿全程的工程实践

| 实践 | 做法 | 价值 |
|---|---|---|
| **Spec 先行** | 任何特性先 brainstorming 出 spec、我 review 定稿,才动代码 | 避免"想当然实现",决策留痕可回溯 |
| **TDD** | 计划第一步就是会失败的测试,红→绿→提交 | 测试即规格,实现被测试钉死 |
| **子代理隔离** | 每任务全新 Opus 子代理,只给任务全文+上下文 | 上下文不污染、专注、可并行安全 |
| **两段审查 + 最终审查** | spec 符合性 + 代码质量,均"不信报告、亲自验证" | 把缺陷挡在合并前 |
| **特性分支 + PR** | 不在 master 直接开发;`gh` 建 PR,合并后清理分支 | 主干始终可发布,改动可评审可回滚 |
| **离线自包含** | 依赖 vendored 到 `third_party/`,零联网构建+测试 | 新环境 clone 即编译,CI 稳定 |
| **文档同步** | 每次变更后同步 CHANGELOG / SRS / SDD / README,并主动消歧(如 spec 与实现的 `OnBytes` 签名对齐、把 `OnBytes` 语义写进头注释) | 文档与代码不漂移 |
| **持久记忆** | 把"SDD 子代理用 Opus""提交不加 Co-Authored-By""gh 安装位置"等写入项目 memory | 跨会话保持一致约定 |
| **不抛异常 + 前缀错误** | 全程 `Result<T>`/`Status`,错误串带 `config:/io:/conn:/timeout:` 等前缀 | 错误可分类、可被 `[[nodiscard]]` 强制处理 |

---

## 6. 一个端到端的具体例子:底层两层重构(PR #1)

把"四件事"串起来看一个真实特性:

1. **设计(brainstorming)**:从"transport 管太多,想更纯粹"出发,一次次澄清边界 —— 是否连分帧也移出?(讲清"流式分帧本质有状态、是 transport 形状的"非对称)→ 定为 Transport 纯字节管道 + ICodec 吸收分帧+编解码 + Message 带交互元数据,System 留后续。产出并提交 spec,我 review 定稿。
2. **计划(writing-plans)**:拆成 11 个 TDD 任务 —— 先"拆除旧架构+最小 CMake 绿",再逐个 codec、逐个 transport,最后两层组合冒烟。每任务含完整代码、命令、提交。自审过 spec 覆盖与类型一致。
3. **实现+测试+审查(subagent-driven)**:11 个全新 Opus 子代理逐任务实现(严格 TDD);每任务 spec + 质量两段审查;期间审查发现两处 `[[nodiscard]]` 告警、一处 header include 不一致,均当即修掉并复审。
4. **收尾(finishing)**:最终整分支审查 → 干净构建零告警、27/27 连跑稳定、解耦验证、零残留 → READY TO MERGE → 推分支、`gh` 建 PR #1 → 合并后同步 master、删分支、prune。
5. **文档同步**:CHANGELOG 记入 0.2.0 重构;SDD/README 同步;并把 `OnBytes` 的线程语义补进头注释与 spec。

---

## 7. 体会:这套协作给了什么

- **质量内建,而非事后补**:测试是第一步、审查是每步,缺陷在合并前被挡下;最终分支多次都是"零告警、全绿、稳定"。
- **决策清晰、节奏可控**:人只在方案/边界处拍板,AI 负责把机制做满;每个决策都有 spec/plan/PR 留痕。
- **大重构敢做**:有 spec + 计划 + 子代理 + 双审查 + `v0.1.0` 留底兜底,才敢对刚发布的版本做破坏性两层重构而不慌。
- **可持续**:离线自包含 + 文档同步 + 持久记忆,让项目在多次会话、多人/多代理协作下仍保持一致与可回溯。

---

*生成于通信中间件 `transport` 0.2.0 开发期(DDS 底层设计阶段)。对照物:`docs/superpowers/specs/`、`docs/superpowers/plans/`、`CHANGELOG.md`、`docs/需求规格说明书.md`、`docs/设计说明书.md`、GitHub PR #1 / #2、`v0.1.0` 标签。*
