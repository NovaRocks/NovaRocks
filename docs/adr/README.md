# 架构决策记录（ADR）

本目录是 NovaRocks 的架构决策知识库：每条 ADR 固化一个**已裁决的设计问题**——背景、考虑过的选项、裁决、**接受的妥协**与**何时重新评估**。检索单位是「架构师会问的问题」，不是「当年做过的任务」。改动某子系统的架构级行为前，先在下方领域索引里找到它。

**范围检验（写之前先过这一关）**：ADR 记录的是**代码里长期成立的设计**，不是**某一次修复的裁决**。反事实检验——假设引发这次调查的那个故障从未发生，这条 ADR 还值得存在吗？值得则是 ADR；不值得，那就是一条案例（写进知识库的 `ops/cases/`），换类型。个案的原话、日志、定位路径、走过的弯路、那次修复选了哪个方案，全部归案例；ADR 正文**不叙述个案**，个案只以 `provenance` 的 PR / 讨论日期与 `related` 的案例链接出现。

**怎么写**：用 `workbench` plugin 的 `$ops-capture` skill（`.agents/skills/workbench/`，Codex 直接发现、Claude Code 经 `.claude/skills` → `.agents/skills` 软链发现）新建、supersede 或重编号——其契约 `ops-contract.md` §7 内嵌模板与全部规则；人工手写请复制下方模板。正文中文；id/slug/tags/frontmatter 键用英文。

> **本 README 是本目录的权威**：`ops-contract.md` 与本文冲突时以本文为准（该契约自身也如此规定）。

**核心规则**：

- **不可变，永不删除**：ADR 合入后不改实质内容。立场变化时写新 ADR 标 `supersedes`，旧条标 `superseded-by` 并移入领域节末尾「历史」小节。被完全覆盖的 ADR 也**不删除**——它记录的是「曾经那样做过、为什么、后来为什么改了」，防止未来（人或 AI）循环重提已否决方案，并保住 provenance 链。允许物理删除的仅有例外：误创建/重复、从未定稿的草稿、泄密与合规问题。允许的原地修改仅限：状态字段、锚点链接的机械修正、错别字、编号冲突重编号。
- **自包含**：结论、选项、妥协必须在文件内完整可读，不依赖外部文档才能理解；设计工坊（vault spec）、PR、讨论日期仅作 provenance 字段。
- **编号**：`ADR-NNNN` = 现存最大编号 + 1，四位零填充。并行 PR 撞号时，**后合入者在 rebase 后重新编号**，并同步修改四处：①文件名、②frontmatter `id`、③本 README 索引行、④代码中全部 `Design: ADR-NNNN` 锚点（含其它 ADR 的交叉引用）。
- **代码锚点**：承重代码处放一行英文注释 `// Design: ADR-NNNN (docs/adr/ADR-NNNN-<slug>.md)`，只放「改这里之前必须读」的位置，不铺开。
- **引用持久性**：ADR 随仓库存续，正文与 provenance **不得依赖不随仓库维护的外部设计文档编号**（如设计工坊里的 RFD-* 任务号——那些文档后期可能删除失联）。指称某项工作用**机制名 + GitHub PR 号**（如「build-frontier proof 全局验环，PR #726」）或**其它 ADR 的编号**；尚未合入的工作用机制描述，PR 合入后回填编号（属允许的机械修正）。
- **否决必须标明性质**，三类不得混写：**设计否决**（与系统的基本能力或约束冲突，永不回头，除非那个能力本身被取消）、**成本否决**（方案更优但当下改造代价不可接受，代价变化时回头）、**待评估**（本条 ADR 没有裁决它，只是记录它存在）。后两类**必须在「何时重新评估」里有对应触发条件**。不得把成本否决写成设计否决——这是最常见的自我美化，且会让后人误以为此路不通。
- **部分替代**：旧条仍有有效规则时保留 `status: active` / `superseded-by: null`，以 `partially-superseded-by` 与新条的 `partially-supersedes` 双向记录范围；正文历史决定不改写，索引说明仍有效的部分。
- **谱系**：业界标准 ADR（Architecture Decision Record，Nygard 2011 谱系）；本库模板为 MADR 风格的六节增强变体——「接受的妥协」与「何时重新评估」为必填节（标准模板中它们名义上属于 Consequences，实践中最常被敷衍，而它们恰是本库存在的意义）。

## 模板

```markdown
---
id: ADR-NNNN
title: "English one-line title"
domain: [domain-tag]
status: active            # active | superseded
supersedes: []
superseded-by: null
date: YYYY-MM-DD
provenance:
  - "PR: <link>"
  - "discussion: <date + topic>"
code-anchors:
  - "<path> (<symbol>)"
---

## 问题
（未来架构师会问的那个问题，一句话。检索入口。
检验：它应该是**任何人第一次接触这块代码时就会问**的问题；出现具体故障、具体修复、PR 号
或某个函数的缺陷就是跑偏了，哪怕它语法上是个问句。）

## 背景与执行事实
（**模型说明书**，不是调查记录：只写「下一个改这块代码的人必须先知道」的事实。
优先用表格承载对照关系——有哪些实体、各自扛什么、谁按什么键、哪个 API 认哪种输入、
名字与实际是否一致、失败模式是什么形状。带符号名锚点；不写会腐烂的行号。
能量化这类错误为什么反复发生的写上（例如「该转换被手写 81 次，没有 helper」）；
代码里有做对了的地方，点名它作为「改这类代码前先读它」的正例锚点。）

## 考虑过的选项
（选项必须针对**这一整类问题**，不是针对那一次修复。每个选项一段：机制、优势、代价，
并标明否决性质（设计 / 成本 / 待评估）。对照过外部系统的写明对照结论。）

## 裁决
（选了什么，以及**它固化成哪几条可复用规则**——规则要具名、能直接贴进 code review；
读者身份不同时分组，例如「开发规则」与「调试规则」。）

## 接受的妥协（诚实记录）
（为此放弃了什么、真实理由是什么——「因改动成本而非因更优」这类必须如实写。）

## 何时重新评估
（触发条件清单：负载形态、依赖成熟度、指标阈值。
每一条成本否决与待评估的选项都应在这里有对应触发条件。）
```

## 领域索引

### test-capabilities

领域哲学：实际 debug assertions 决定开发接缝是否编入，默认开发共用主测试图与产品装配，标准 release 排除接缝；编入的能力仍须经每个实际角色启动的有限授权，准确绑定产品 owner 签发的工作身份。来源只在薄 CLI 冻结。构建事实、权限、控制结束、helper join、角色退出与资源收敛分别取证。共享测试层只提供机械能力，领域解释和生命周期仍属于原 owner。

- [ADR-0188](ADR-0188-optional-pr-validation-and-formal-admission.md) — PR 完整反馈为何以独立非必需检查展示真实状态，并与正式准入共用执行步骤（active；仅替代 ADR-0189 的 PR 只做快速预检这一成本取舍）
- [ADR-0189](ADR-0189-merge-queue-admission-and-current-attempt-evidence.md) — 合入队列准入为何绑定指定执行上下文、显式启用范围与当前批次证据（active；部分替代 ADR-0186；PR 只做快速预检的成本取舍由 ADR-0188 替代，正式来源、启用范围与证据规则保留）
- [ADR-0186](ADR-0186-layered-verification-and-read-only-evidence-composition.md) — 验证为何按开发、准入、全量分层，并由原生 owner 采集事实、只读消费者按同输入与完整生命周期组合证据（active）
- [ADR-0184](ADR-0184-native-test-execution-and-process-monitor-ownership.md) — 原生逐测试进程为何与独立 payload monitor、懒建调用租约和封闭材料汇总分工（active；部分替代 ADR-0069 的进程内 reader / Drop 所有权，机械叶子与领域边界保留）
- [ADR-0181](ADR-0181-unified-development-capabilities-and-executable-provenance.md) — 开发接缝为何按实际 cfg 编入、产品装配与主测试图共用，来源为何只在薄程序入口冻结（active；部分替代测试控制 ADR-0175 的编译排除、独立普通/控制产物与选择规则）
- [ADR-0175](ADR-0175-test-capabilities-and-finite-role-authorization.md) — 测试控制为何分离编译能力、准确角色启动授权与实际退出（active；编译与分产物规则由 ADR-0181 部分替代，准确授权、有限绑定、原期限与实际退出保留）

### runtime-filter

领域哲学：runtime filter 是**纯性能优化**——任何 activation、等待、降级策略都不得改变 SQL 结果语义（RF 是保守预过滤，join 本体兜底）。数据面走 query-global `RuntimeFilterGraph + DeploymentCompiler + Service`；静态层**宁严勿宽**（证明不了安全就在 fragment submission 前 fail-fast），运行时 timeout + PassThrough 只是生产可用性兜底、不是语义权威。planner 与 deployment 双侧独立验证，二者之间不传裸布尔结论。

- ADR-0001 — runtime filter 等待环为何静态 strict-fail，而不是靠运行时 timeout 兜底（active）
- ADR-0002 — multicast 反压为何保持消费者耦合（active）
- ADR-0003 — RF consumer 为何默认 BlockingSnapshot、NonBlockingLive 只做定点降级（active）
- ADR-0027 — Runtime Filter 规划为何由 SQL 私有拥有、角色之间只交换封存 wire facts（active）
- ADR-0043 — Runtime Filter row/scan evaluator 为何统一由 Execution 拥有、Backend 只提供 artifact query（active）
- ADR-0044 — Runtime Filter participant 物理生命周期为何由 Backend 拥有、Execution 保留语义值与 evaluator（active）
- ADR-0128 — Lifecycle canonical engine is private behind typed digest APIs（active）
- ADR-0159 — connector scan 为何由 driver 直接 poll page stream、在 scan 分支内扇出，并以观察到的退出为结束（active）

#### 历史

- ADR-0114 — participant 分类为何以载荷为唯一权威表示，删除自证式派生的 participant_roles 字段（superseded → ADR-0135）
- ADR-0041 — Runtime Filter scan-domain 评估为何由 Execution 拥有、Core 只提供中立 artifact capability（superseded → ADR-0043）
- ADR-0076 — Runtime Filter terminal observation 为何由 Backend participant 有界聚合、并仅经 typed QLC contribution 出域（superseded → ADR-0078）
- ADR-0078 — Runtime Filter terminal observation 为何只作观测，且以 P0/P1/P2 查询终止契约交付（superseded → ADR-0106）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（superseded → ADR-0126）
- ADR-0126 — Terminal delivery 为何以 participant attempt identity 而非 payload content identity 定位（superseded → ADR-0127）
- ADR-0127 — participant attempt identity 如何围栏 immutable Stage admission（superseded → ADR-0128）
- ADR-0053 — MV snapshot change window 复用 exact-generation scan planning（superseded → ADR-0114）
- ADR-0039 — scan unit 的 immutable、bounded physical domain facts（superseded → ADR-0114）
- ADR-0034 — cluster composite split 与 Backend local scan unit 的两级生命周期（superseded → ADR-0114）

### join-execution

领域哲学：join 执行核心是 purpose-built 的——join 的拓扑（key → 枚举该 key 全部 build 行 + gather 物化）与聚合（key → 单份累加态）不同，不共享聚合的 KeyTable 形状。速度来自算法与数据布局（直接寻址、合并列存、选择向量、membership 零枚举、整列直发），不来自手写 SIMD。任何新档位/快路径合入时，全部 join 类型 + null-safe 等值 + 残差谓词的全套件必须每一步全绿——正确性绝不为分层或向量化让步。

- ADR-0004 — hash join 执行核心为何与聚合 KeyTable 分家、自建 purpose-built join_hash_map，且不写显式 SIMD（active）

### pipeline-execution

领域哲学：查询 pipeline 的 CPU 推进只有一个执行者——driver 池。任何算子（包括 scan 这类 I/O 源）等待时都以 Pending 与可观察的唤醒交还线程，而不是占着线程阻塞或另建执行器；远端 I/O 由独立 I/O runtime 执行，driver 只 await 受管结果。让出靠共享的轮次预算完成，不承诺抢占 kernel。结束以观察到的真实退出为准：close 只观察，丢弃观察者不转移责任，所有 pending finish 只有一个事件入口。并行度声明必须是事实——声明的 DOP 与实际不符时在声明处兑现，而不是让下游逐处补边界。

- [ADR-0197](ADR-0197-logical-write-counts-and-strict-stream-delivery.md) — 逻辑写计数、物理分布与严格消费为何各守独立契约，summary与writer结果为何公平推进（active；完整宿主/FE接线仍待交付）

- [ADR-0192](ADR-0192-provider-work-units-share-one-runtime-split-path.md) — Provider 工作单元为何统一走 runtime split，单例提示为何只影响放置，分布聚合与恢复为何各守原 owner（active）
- [ADR-0191](ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md) — 折叠为何冻结真实 binding 并复用 compiled 单节点核心，COW 赋值为何属于选择查询，兼容材料为何只保留真实 owner（active；部分替代 ADR-0100 的请求词汇与旧实现映射，其余有效规则保留）
- [ADR-0193](ADR-0193-task-conclusion-proposals-and-settled-points.md) — Task 结论为何只交接提议、以结算屏障支撑定局，并由唯一 owner 推进终止（active；仅部分替代 ADR-0187 规则 5 的隐含同步收据，单一接受事务、细化及其余规则保留）
- [ADR-0187](ADR-0187-receiver-owned-steps-and-single-task-conclusion.md) — 接收方为何接受后准备、领域失败为何贯穿唯一 Task 结论，并由原 owner 继续观察真实退出（active；仅部分替代 ADR-0095 对已知非 Analyze 执行事实的不透明化，Analyze code/span 与历史 MV 边界保留；规则 5 的隐含同步收据由 ADR-0193 部分替代，单一接受事务、细化及其余规则保留）
- [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) — 静态计划为何只走 compiled package，能力缺口为何以准确拒绝与完整失败账交接，最终为何仍须全量清账（active；部分替代 ADR-0124 的兼容材料，exact-island 与 ingress 规则保留）
- ADR-0159 — connector scan 为何由 driver 直接 poll page stream、在 scan 分支内扇出，并以观察到的退出为结束（active）

### low-cardinality

领域哲学：编码是执行载体的自描述物理属性，不是 plan 必须背书的正确性契约。correctness 由载体保证——`Dictionary(Int32, Utf8)` 自描述 + 算子入口 hydrate 兜底，算子不认识编码时的默认后果必须是「慢」而非「错」（fail-safe，不是 fail-open）；plan/元数据层只声明快路径资格，误判最坏是少一次加速。lake-native（不拥有数据、无内表）是前提约束：native 侧不建表级全局字典，FE-compatible 全局字典执行是隔离的协议侧支。

- ADR-0005 — 低基数编码为何运行时载体优先：DictionaryArray 是 correctness owner、plan 层只是加速器（active）
- [ADR-0195](ADR-0195-public-value-access-and-construction.md) — 公共值算法为何分离解码计算与存储、保持原始坐标，并由实际调用 owner 掌握转换生命周期和资源控制（active）

### provider-spi

领域哲学：SPI 只承载 NovaRocks 产品架构明确支持的可替换 provider 契约，不吸收所有跨 crate API。系统定义契约语义，provider 与 consumer 共同依赖统一 SPI，host 负责选择、装配与生命周期；domain API、consumer port、transport 和实现策略保持各自 owner。稳定性由原子演进、conformance 与真实分布式验证保证，不靠兼容 bridge 或 service locator。

- [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) — 各分布式意图为何共用逻辑执行 owner，分离计算就绪、累计效果、publication 与真实收敛，并复用唯一实际资源预算和可取消背压（active；部分替代 lake publication ADR-0110、后端自注册 ADR-0111、ADR-0133/0161/0171；不承诺永久阻塞隔离；架构已实施，验证范围、失败处置与验收资格独立核对）
- [ADR-0196](ADR-0196-hadoop-test-catalog-complete-version-installation.md) — Hadoop 测试 catalog 为何共用完整不替换安装、只读版本发现与完整清单建删表，普通回收为何保护发现根（active；部分替代 ADR-0077/0118/0179 的 Hadoop 发现、存在性、DROP 与旧 UPDATE 协议，其余 owner 和生命周期保留）
- [ADR-0192](ADR-0192-provider-work-units-share-one-runtime-split-path.md) — Provider 工作单元为何统一走 runtime split，单例提示为何只影响放置，分布聚合与恢复为何各守原 owner（active）
- [ADR-0182](ADR-0182-iceberg-recursive-nominal-field-identities.md) — Iceberg 递归名义领域为何由 Provider 按稳定 field ID 持久声明，并在当前、历史与所选 schema 上严格验证（active）
- [ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md) — Iceberg 发布为何由冻结请求、唯一 journal 与独立证据期限拥有，正面核查为何只追加准确归属与收尾事实（active；部分替代 ADR-0118/0171 的 Transaction 与 late carrier、ADR-0175 的会话/生命周期、ADR-0174 的 P2/U4 退出路由；保留认证/路由及 IRU-6b/6c 边界；永久依赖图守卫由 ADR-0190 部分替代，发布生命周期裁决保留）
- [ADR-0178](ADR-0178-iceberg-write-type-boundary-and-variant-layering.md) — Iceberg 写出为何冻结完整存储 schema、在 Provider 适配执行载体，并按逻辑类型判定 Variant（active；部分替代 ADR-0174 的 U8/Variant 读取暂缓条款与 ADR-0055 的 Variant 写入载体分歧；其余来源、退出与策略所有权保留）

- ADR-0006 — 可替换 provider 契约为何统一进入一个系统 SPI，而普通跨 crate port 不进入（active；其中「Connector 与 StateStore 共用同一个物理 SPI package」这一条前提已由 ADR-0140 替换，其余原则仍有效）
- ADR-0014 — 共享文件访问与 Parquet/ORC 物理解码为何属于无 Connector identity 的独立基础（active）
- ADR-0015 — table-format Connector 为何拥有 read correctness，native fragment 只绑定已安装真实 instance（active）
- ADR-0164 — Iceberg 删除成员为何由 FE 冻结闭包、BE 以 task-local union 执行，并分离逻辑集合、加载视图与完整端点（active）
- ADR-0016 — Connector 为何共享逻辑 identity、但不共享 FE control 与 BE execution runtime（active）
- ADR-0017 — Connector catalog mutation 为何使用 FE-only lease 与三态 external outcome（active）
- ADR-0018 — 静态 Connector predicate 为何以 Exact/PruningOnly/Unsupported 协商、而不扩展 native wire（active）
- [ADR-0133](ADR-0133-dataflow-connector-writer-and-frontend-commit.md) — 分布式 writer 为何是普通执行算子、产物走数据流、且只有 FE 能提交（active；全参与者成功门及缺失输出的清理假设由 ADR-0198 部分替代，dataflow writer、单 root 有界聚合、FE 提交与语句累计上界保留）
- ADR-0024 — 无需 BE staging 的 data mutation 为何使用 FE-only frozen plan 与 marker-only reconcile（active）
- ADR-0028 — metadata maintenance 为何由 FE 以 exact lease、durable plan 与 marker reconcile 执行（active；持久计划、恢复、私有产物与回收条款由 ADR-0177 部分替代，FE-only capability、准确 generation 与原子 marker 保留）
- [ADR-0177](ADR-0177-attempt-local-maintenance-inputs-and-single-rewrite-plan.md) — 维护输入为何只驻留当前尝试，且重写读取、writer 与提交为何共用同一份冻结计划（active；部分替代 ADR-0028）
- ADR-0128 — Lifecycle canonical engine is private behind typed digest APIs（active）
- ADR-0127 — participant attempt identity 如何围栏 immutable Stage admission（active）
- ADR-0130 — Connector role binding 为何每个进程角色只发布一个 complete generation（active）
- ADR-0139 — Server 为何以唯一封闭 manifest 定义 active provider 并拒绝退役配置（active）
- ADR-0137 — 多 provider 的私有 Connector wire 为何归 provider 自有、Native 只承载 opaque envelope（active）
- ADR-0138 — Paimon 0.3.0 为何仅为有界授权读取接缝而维护可追溯 vendor patch（active）
- ADR-0125 — Backend catalog runtime、Frontend effect owner 与 provider-private proof 为何使用三种不互换 identity（active）
- ADR-0049 — row mutation 的 strategy、identity、route 与 cohort 为何由 Provider 签发并拥有（active）
- ADR-0052 — SHOW CREATE 为何以 exact lease 的有界 table-definition facts 取代 concrete table decode（active）
- ADR-0055 — row-DML 调用方为何只读 Provider 签发的 strategy，而 SQL 谓词合法性为何留在 Core（active；妥协 5 的 forward written-version 预测由 ADR-0171 部分取代，源 facts 与策略归属仍有效）（Variant 写入载体分歧由 ADR-0178 部分替代；策略与 source facts 所有权保留）
- ADR-0056 — 摘除 Core 对 provider 的测试依赖时，无法用冻结 SPI facts 表达的断言为何归位到实现旁而非复刻或删除（active）
- ADR-0063 — Copy-on-Write row mutation 的match与rewrite读源为何由Provider按exact base签发（active）
- ADR-0077 — Hadoop catalog 创建表为何以 storage 条件创建 v1 metadata 作为线性化点（active；完整安装与物理发现由 ADR-0196 部分替代）
- ADR-0085 — durable caller 为何通过既有 exact metadata lease capture/rebind 物理表对象，而不依赖统计能力或新增平行 authority（active）
- ADR-0080 — 统计证据为何拆成 collection 级覆盖度与 per-metric 基准版本/来源/数值性质/集合关系四个独立维度（active）
- ADR-0081 — 统计为何是带版本、允许陈旧、由读侧逐 metric 决定可用性的估计事实（active）
- ADR-0136 — 统计为何复用普通聚合与写入数据流，并由provider single-use session发布（active）
- ADR-0111 — Frontend maintenance/statistics job 为何是 ProcessRuntime，而 GC first-observation 是 Accelerator（active）
- ADR-0097 — durable MV 与维护基表 identity 为何保持 opaque、只在 provider-local 边界解释（active）
- ADR-0089 — Predicate-driven Parquet page pruning 为何只在 FS reader-open 按实际 physical leaf 计算（active）
- [ADR-0110](ADR-0110-lake-publication-crash-only-contract.md) — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active；本地期限推导外部效果上界及无条件年龄窗回收由 ADR-0198 部分替代，target OCC、autocommit 与无持久接管规则保留）
- ADR-0154 — MV 领域文档与准确发布附着为何是湖上权威（active；存放与附着条款由 ADR-0180 部分替代，湖上权威、领域分工与 Accelerator 边界保留）
- ADR-0123 — TaskUpdate split delivery 为何使用 sequence watermark 与 unknown-outcome retry（active；Create 的身份幂等与本条水位幂等的区分见 ADR-0161）
- ADR-0118 — Iceberg catalog 语义为何收敛到一个 provider-private owner，并以 operation-shaped admission 取代能力表（active；SDK transaction/action 准备复用许可由 ADR-0171 部分取代，catalog owner 与 operation admission 仍有效；统一 Transaction 与 late TableCommit carrier 由 ADR-0179 替代）
- [ADR-0171](ADR-0171-commit-operation-model.md) — Iceberg 提交为何由不可变意图、依赖验证、attempt staging 与产物 owner 共同完成（active；部分取代 ADR-0118、ADR-0055，细化 lake publication ADR-0110；文档请求形状由 ADR-0180 部分替代，不可变操作、依赖验证与产物所有权保留；late SDK carrier、transaction outcome 与可复用 rewrite manifest 生命周期由 ADR-0179 部分替代；同步等待及 finalize/cleanup 完成边界、无条件年龄窗延续由 ADR-0198 部分替代，原有 Unknown 保护保留）
- [ADR-0175](ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md) — Iceberg REST 为何每个 generation 只有一份会话、认证头只由角色本地 owner 写、BE 按冻结路由上下文取得，并把 NovaRocks 发出的每个变更请求归为可归属的证据（active；部分替代 ADR-0151 的取得路径含义与 ADR-0174 的 REST 补丁清单、临时结果收窄和 SDK 认证妥协；「core 中只为 REST 服务的补丁仍在」这一妥协由 ADR-0176 部分替代，会话建立、SDK facade、派发后期限与 FE stage 结果投影由 ADR-0179 替代；认证、路由、状态码与脱敏规则仍有效）
- [ADR-0176](ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md) — Iceberg REST 请求词汇为何归连接器、`add-schema` 为何不带 `last-column-id`（支持下限为 Iceberg Java 1.9 语义），view 替换为何总是新增带操作身份的版本并以 `-1` 选择、语义校验为何在派发之前（active；部分替代 ADR-0174 的 core U2/P8/View API 保留项与 ADR-0175 的 core REST 补丁妥协）
- ADR-0169 — catalog owner 为何在零副作用点统一准入、HMS 永久只读，并让单写者作业留在提交语句生命周期内（active；延伸 ADR-0118）
- ADR-0140 — StateStore 契约为何从统一 SPI package 物理独立、测试机制为何单独成 crate（active；守卫形态与依赖边界检查由 ADR-0190 部分替代，契约/testkit 拆分保留；替换 ADR-0006 的统一物理 SPI 前提）
- ADR-0143 — StateStore 为何只回答自己签发过的 attempt，并删除跨重启 receipt 查询与公共 change feed（active；替换 ADR-0122 的 schema 版本、history 保留与 commit-resolution 三项承诺）
- ADR-0158 — Parquet 扫描为何由共享 Range 服务派发，并由单一 B/N 后继窗口维持顺序与责任（active；其「同步 reader 占用扫描线程」的妥协已由 ADR-0159 兑现，Range 服务与 B/N 规则不变）
- ADR-0159 — connector scan 为何由 driver 直接 poll page stream、在 scan 分支内扇出，并以观察到的退出为结束（active）
- [ADR-0180](ADR-0180-inline-keyed-documents-and-snapshot-records.md) — 应用文档为何按完整语义内联寻址、槽变更推进 main，且快照记录与生命周期回收分工（active；部分替代 ADR-0154、ADR-0171）

#### 历史

- ADR-0132 — provider 为何拥有 role-binding factory、而 Server 只组装 role-local resource（superseded → ADR-0139）
- ADR-0129 — vended credential 为何必须在 metadata materialization 前按 attempt 收集（superseded → ADR-0145）
- ADR-0022 — Connector statistics capability 为何保持 FE-only、generation-fenced 且不进入 BE binding（superseded → ADR-0136）
- ADR-0082 — 同一快照上的统计发布为何以覆盖度排序、且冲突重试必须重新判定（superseded → ADR-0136）

- ADR-0048 — distributed write为何以 Provider-signed preparation、exact lease 与中立 durable terminal fact 收敛 caller authority（superseded → ADR-0051）
- ADR-0023 — distributed writer 为何以 operation/cohort/execution/writer 分层，并由 FE 聚合外部提交（superseded → ADR-0133）
- ADR-0030 — distributed rewrite 为何以 frozen groups、C1 cohorts 与 FE aggregate commit 实现单 snapshot（superseded → ADR-0133）
- ADR-0051 — distributed write 为何在 preparation 与 planning 之间强制 exact-generation Provider activation（superseded → ADR-0133）
- ADR-0062 — Copy-on-Write row mutation 的读源为何由 Provider 按 cohort 冻结并以中立 recipe 签发（superseded → ADR-0063）
- ADR-0060 — MV refresh base pin 为何必须从同一 exact metadata 投影 UUID 与 current snapshot（superseded → ADR-0086）
- ADR-0047 — catalog/read admission 为何以 exact Connector generation 与中立 native carrier 封存（superseded → ADR-0103）
- ADR-0103 — 中央 Provider wire authority 与同构 Native build admission 为何统一由 Protocol 和 Frontend topology 拥有（superseded → ADR-0105）
- ADR-0068 — 分布式 DML 的 external write fence 为何做成 catalog 原子条件更新里的线性化点（superseded → ADR-0110）
- ADR-0070 — CTAS takeover 为何使用 catalog-native absent-target fence，并对未广告能力的 catalog 提前拒绝（superseded → ADR-0110）
- ADR-0084 — durable statistics job target binding（superseded → ADR-0111）
- ADR-0061 — MV repartition 为何由 Provider 在单次原子 write commit 中同时切换 partition spec 与 snapshot（superseded → ADR-0112）
- ADR-0112 — MV 运行态为何只属于当前进程、StateStore 为何只保留 lake-source Accelerator（superseded → ADR-0154）
- ADR-0114 — Connector read 为何改用 Trino 对齐的 typed handle/split/page-source 与运行时 split 投递（superseded → ADR-0119）
- ADR-0119 — Connector read 内部 runtime SPI 与 wire codec 为何分离（superseded → ADR-0123）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0104 — Connector execution binding 为何使用 SPI domain declaration、sealed Host 与可重放失败状态机（superseded → ADR-0120）
- ADR-0120 — Frontend restart 后为何仅由 Backend 回收无 query lease 的旧 connector generation（superseded → ADR-0125）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（superseded → ADR-0126）
- ADR-0126 — Terminal delivery 为何以 participant attempt identity 而非 payload content identity 定位（superseded → ADR-0127）
- ADR-0127 — participant attempt identity 如何围栏 immutable Stage admission（superseded → ADR-0128）
- ADR-0131 — Server 为何从 role-local resource 组装 provider binding（superseded → ADR-0132）
### distributed-query-lifecycle

领域哲学：Query Application 的逻辑执行 owner 统一管理各分布式意图的 attempts、恢复与计算结果；Frontend Task 驱动报告协议事实，BE Worker 拥有本地执行与资源。外层语句/job 拥有累计业务结果，领域/Provider 拥有 publication。计算就绪、提交、诊断和实际收敛分别取证；内部安全交接后可立即推进领域业务。角色之间只通过版本化 wire 交换事实，FE 内共享契约仅含实际使用的身份、消息与中立事实，不携带 runtime 或领域 policy。取消、丢失、过期 ownership 与进程失败不能抹去已发生效果或未退出责任。

资源沿用原 executor、Transport 和结果预算；普通 Full 是可取消背压。完整依赖事实约束来源与责任，不构成另一套工作类别或依赖域配额。计算位置、仍活数据和残留责任分别退出，超时不证明实际退出。公平推进以资源可用、依赖能够返回为条件；共享 worker 全部永久阻塞时，健康请求与结算也可能不可用，不据此承诺故障隔离或新增 worker 最低数。

- [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) — 各分布式意图为何共用逻辑执行 owner，分离计算就绪、累计效果、publication 与真实收敛，并复用唯一实际资源预算和可取消背压（active；部分替代 lake publication ADR-0110、后端自注册 ADR-0111、ADR-0133/0161/0171；不承诺永久阻塞隔离；架构已实施，验证范围、失败处置与验收资格独立核对）
- [ADR-0191](ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md) — 折叠为何冻结真实 binding 并复用 compiled 单节点核心，COW 赋值为何属于选择查询，兼容材料为何只保留真实 owner（active；部分替代 ADR-0100 的请求词汇与旧实现映射，其余有效规则保留）
- [ADR-0193](ADR-0193-task-conclusion-proposals-and-settled-points.md) — Task 结论为何只交接提议、以结算屏障支撑定局，并由唯一 owner 推进终止（active；仅部分替代 ADR-0187 规则 5 的隐含同步收据，单一接受事务、细化及其余规则保留）
- [ADR-0187](ADR-0187-receiver-owned-steps-and-single-task-conclusion.md) — 接收方为何接受后准备、领域失败为何贯穿唯一 Task 结论，并由原 owner 继续观察真实退出（active；仅部分替代 ADR-0095 对已知非 Analyze 执行事实的不透明化，Analyze code/span 与历史 MV 边界保留；规则 5 的隐含同步收据由 ADR-0193 部分替代，单一接受事务、细化及其余规则保留）
- [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) — 静态计划为何只走 compiled package，能力缺口为何以准确拒绝与完整失败账交接，最终为何仍须全量清账（active；部分替代 ADR-0124 的兼容材料，exact-island 与 ingress 规则保留）
- ADR-0007 — FE 全局协调与 BE 本地查询生命周期为何保持进程和状态机分离（active）
- ADR-0011 — 请求执行为何使用 immutable context、一次 topology capture 并拒绝 ambient fallback（active）
- ADR-0012 — Query session admission 与 router 为何由 frontend 拥有、core 只保留 wire/compiler kernel（active）
- ADR-0102 — MySQL KILL 为何经 exact generation token 与 protocol-owned connection lifecycle 实现（active）
- [ADR-0172](ADR-0172-mysql-query-deadlines-and-active-delivery-work-budgets.md) — MySQL 查询期限、累计30秒活跃交付工作与绝对5秒Closing为何由不同owner约束（active）
- ADR-0128 — Lifecycle canonical engine is private behind typed digest APIs（active）
- ADR-0092 — 查询 execution identity 为何以 process-local namespace 与连续 sequence 保持既有 wire 形状（active）
- ADR-0123 — TaskUpdate split delivery 为何使用 sequence watermark 与 unknown-outcome retry（active；Create 的身份幂等与本条水位幂等的区分见 ADR-0161）
- [ADR-0124](ADR-0124-native-compatibility-islands-and-ingress-admission.md) — Native compatibility island 与 ingress admission 为何以 exact identity 闭合，而不协商 wire（active；兼容材料由 ADR-0185 部分替代，exact-island 与 ingress 规则保留）
- [ADR-0201](ADR-0201-role-local-node-credential-sources.md) — 节点身份为何使用独立 NodeLocal 来源、各角色本地取得并复用原 authority 续期，而不冒充 Static 或 Vended（active；部分替代 ADR-0151 的 NodeLocal principal 含义）
- ADR-0151 — 凭据续期为何由用它签名的那个消费者驱动、材料为何不再跨进程（active；supersedes ADR-0149；取得路径的含义由 ADR-0175 部分替代：两条路径都携带 FE 冻结的公开路由上下文、执行节点不建会话、vended 读取准入要求 BE 首次取得；消费侧续期与材料不跨进程仍有效）
- [ADR-0175](ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md) — Iceberg REST 为何每个 generation 只有一份会话、认证头只由角色本地 owner 写、BE 按冻结路由上下文取得，并把 NovaRocks 发出的每个变更请求归为可归属的证据（active；部分替代 ADR-0151 的取得路径含义与 ADR-0174 的 REST 补丁清单、临时结果收窄和 SDK 认证妥协；「core 中只为 REST 服务的补丁仍在」这一妥协由 ADR-0176 部分替代，会话建立、SDK facade、派发后期限与 FE stage 结果投影由 ADR-0179 替代；认证、路由、状态码与脱敏规则仍有效）

- [ADR-0161](ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md) — 纯本地程序为何分离 Task 接管、安装、覆盖观察与实际收敛（active；读取封口与逻辑业务结论边界由 ADR-0198 扩为通用计算就绪及独立语句结果，准确身份、Accepted/Installed、覆盖观察与真实收敛保留）

#### 历史

- ADR-0146 — 逻辑执行为何拥有 attempts、结果可见性与残余收敛，并在其下保留 Task 唯一生命周期权威（superseded → ADR-0161）
- ADR-0157 — Native RPC 接收保护为何分布在认证后入口、方法尺寸门和 codec（superseded → ADR-0161）
- ADR-0158 — Task 创建为何只冻结一次、按准确 identity 与生命周期重放，逻辑执行为何只激活一次计划；准确文件 ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md（superseded → ADR-0161）

- ADR-0149 — vended 凭据续期失败为何只终结本轮、失效判定为何收归访问解析点（superseded → ADR-0151）
- ADR-0010 — 显式 query cancellation surface 为何以 MySQL KILL QUERY 和 frontend session owner 实现（superseded → ADR-0102）
- ADR-0076 — Runtime Filter terminal observation 为何由 Backend participant 有界聚合、并仅经 typed QLC contribution 出域（superseded → ADR-0078）
- ADR-0047 — catalog/read admission 为何以 exact Connector generation 与中立 native carrier 封存（superseded → ADR-0103）
- ADR-0103 — 中央 Provider wire authority 与同构 Native build admission 为何统一由 Protocol 和 Frontend topology 拥有（superseded → ADR-0105）
- ADR-0078 — Runtime Filter terminal observation 为何只作观测，且以 P0/P1/P2 查询终止契约交付（superseded → ADR-0106）
- ADR-0079 — IDL/proto 为何是 FE/BE 中立 query lifecycle 契约的规范形式、Protocol 如何同时拥有 schema 与已验证值（superseded → ADR-0106）
- ADR-0098 — native DTO 的字段路径与验证错误为何由 Protocol 独占、而不保留 Core family 门面（superseded → ADR-0106）
- ADR-0105 — Provider wire authority 为何与 SPI domain carrier 分离、但仍保持单一 Protocol digest（superseded → ADR-0106）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（superseded → ADR-0126）
- ADR-0126 — Terminal delivery 为何以 participant attempt identity 而非 payload content identity 定位（superseded → ADR-0127）
- ADR-0127 — participant attempt identity 如何围栏 immutable Stage admission（superseded → ADR-0128）
- ADR-0008 — 分布式查询为何使用 Init/Stage/Start 三阶段启动（superseded → ADR-0135）
- ADR-0114 — participant 分类为何以载荷为唯一权威表示，删除自证式派生的 participant_roles 字段（superseded → ADR-0135）
- ADR-0135 — 分布式工作为何以 task 为单位创建、驱动与终结，而不是 query-wide participant 状态机（superseded → ADR-0146）
### sql-compiler

领域哲学：SQL compiler 只消费一次 statement admission 冻结的 SQL 事实，并只产出 SQL facts；application owner 保留 session、view rewrite、topology、native encoding 与 lifecycle assembly。catalog、statistics 与 scan preparation 对同一表必须复用同一 exact Connector binding，缺失事实必须显式失败或保守降级，不能重新读取 latest。

- [ADR-0192](ADR-0192-provider-work-units-share-one-runtime-split-path.md) — Provider 工作单元为何统一走 runtime split，单例提示为何只影响放置，分布聚合与恢复为何各守原 owner（active）
- [ADR-0191](ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md) — 折叠为何冻结真实 binding 并复用 compiled 单节点核心，COW 赋值为何属于选择查询，兼容材料为何只保留真实 owner（active；部分替代 ADR-0100 的请求词汇与旧实现映射，其余有效规则保留）
- [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) — 静态计划为何只走 compiled package，能力缺口为何以准确拒绝与完整失败账交接，最终为何仍须全量清账（active；部分替代 ADR-0124 的兼容材料，exact-island 与 ingress 规则保留）
- ADR-0073 — SQL compiler 为何先完成全部 binding 分析物化、再冻结 statistics 并以无 catalog 的第二阶段优化封存（active）
- ADR-0040 — SQL compiler 为何先完成依赖倒置闭包、再进行独立 crate 物理迁移（active）
- ADR-0153 — 完成的 PhysicalPlan 为何是唯一静态执行权威，首次 Task 提交为何关闭替换窗口（active；其中「首次提交关闭替换窗口 / DispatchSeal」规则已由 ADR-0161 的一次激活与固定版本恢复替换，其余仍有效）
- ADR-0100 — 常量折叠为何经注入端口复用执行 kernel，并对无法一致表示的结果拒绝折叠（active；请求词汇与旧实现映射由 ADR-0191 部分替代，端口、无状态生命周期、optional-fold 与原拒折门继续有效）
- ADR-0145 — 查询语义为何先于 per-attempt execution access 冻结，重试为何不得重新规划或携带秘密（active）

- [ADR-0161](ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md) — 纯本地程序为何分离 Task 接管、安装、覆盖观察与实际收敛（active；读取封口与逻辑业务结论边界由 ADR-0198 扩为通用计算就绪及独立语句结果，准确身份、Accepted/Installed、覆盖观察与真实收敛保留）

- ADR-0176 — 递归执行语义为何与准确布局、实际编码、来源字段及独立构造责任分离（active）

#### 历史

- ADR-0158 — Task 创建为何只冻结一次、按准确 identity 与生命周期重放，逻辑执行为何只激活一次计划；准确文件 ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md（superseded → ADR-0161）

- ADR-0050 — sealed DistributedPlan 为何以 logical mutation effect 与 opaque provider route 服务跨 owner encoder（superseded → ADR-0153）
- ADR-0025 — SQL compiler 为何以显式 request、immutable snapshots 与 post-compile binding context 形成唯一入口（superseded → ADR-0073）
- ADR-0042 — sealed DistributedPlan 为何以单一只读契约服务跨 owner encoder（superseded → ADR-0050）
### sql-language

领域哲学：SQL语言事实由自有parser及其source span定义；持久化定义保存用户有效原文与创建时解析上下文，不保存
normalizer、AST mutation或printer生成的内部表示。运行期可以按请求派生typed/canonical facts，但派生物不能反向成为
语言或durable authority。

- ADR-0090 — MV/View定义为何持久化用户有效SQL原文与解析上下文，而不是AST Display（active）
- ADR-0101 — native SQL language authority 为何分离 parser、SQL semantic value 与 Frontend application request（active）

### map-semantics

领域哲学：Map 键域在构造时成立，不能由写出路径或运行时字段放宽来补救。非空且唯一是值语义；构造拒绝与具有明确优先级的合并分开，唯一性复用 GROUP BY 规范键等价。实施覆盖必须如实记录，局部入口的校验不代表所有生产者已经收敛。

- [ADR-0183](ADR-0183-map-keys-are-non-null-and-unique.md) — Map 键为何非空且唯一，构造拒绝为何与合并优先级分开（active；当前字面量与数组构造落实 NULL key 拒绝，重复键与其他入口待收敛）

### runtime-role

领域哲学：生产运行时只由 native FE/BE 角色组成。FE 拥有 SQL admission 和
全局协调，BE 拥有本地执行；all-in-one 仅为测试便利且不得改变这条边界。
外部系统经 Connector 接入，不以 inbound 兼容 server、daemon 或全局 bridge
混入 native lifecycle。当前 Server 的封闭 provider 集合是 Iceberg 与 Paimon；
StarRocks 已废弃且没有 active read capability。


- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）
- ADR-0118 — Iceberg catalog 语义为何收敛到一个 provider-private owner，并以 operation-shaped admission 取代能力表（active）
- ADR-0121 — FE serving lifecycle 为何用单向 admission drain、而不是 connection shutdown 或远程 management mutation（active）
- [ADR-0124](ADR-0124-native-compatibility-islands-and-ingress-admission.md) — Native compatibility island 与 ingress admission 为何保持 role-local、无 all-in-one 旁路（active；兼容材料由 ADR-0185 部分替代，其余规则保留）
- ADR-0147 — 进程本地工作治理为何分离责任、准入、资源与结果信用，并由 Application Host 持有唯一 owner（active）
- ADR-0152 — 后端如何告诉前端它还记得哪些准入决定：凭据为何是账本下界而不是一个要相等匹配的当前值（active）
- ADR-0156 — Connector 操作控制与执行资源为何分离，并在 BE 准入后绑定（active）
- ADR-0158 — Parquet 扫描为何由共享 Range 服务派发，并由单一 B/N 后继窗口维持顺序与责任（active；其「同步 reader 占用扫描线程」的妥协已由 ADR-0159 兑现，Range 服务与 B/N 规则不变）

#### 历史

- ADR-0026 — 为何退役 StarRocks-compatible backend runtime role，并把 StarRocks 限定为外部 Connector（superseded → ADR-0108）
- ADR-0108 — native FE/BE role launch、management surface 与双配置 all-in-one（superseded → ADR-0112）

### native-transport-security

领域哲学：Native transport 先证明 caller 属于 deployment，再由 TLS 选择性提供 channel confidentiality、
integrity 与 server identity。transport proof 不取代 topology、lifecycle、Protocol 或 SQL authorization；
plaintext 的残余风险必须可见，TLS profile 与证书/endpoint identity 必须是 closed contract，不能以 fallback、
system default 或 role-local source 偷偷扩张。

- ADR-0110 — Native caller authentication 为何采用 mandatory JWT、authenticated plaintext 默认与 optional TLS（active）

### backend-architecture

领域哲学：Backend 的目录应表达真实 owner，而不是已退役的协议分类。领域 adapter 紧邻其状态、验证和 execution host；
跨领域的 RPC 基础设施仅保留 generated surface、codec、transport、data-plane 与 composition，不能成为第二业务 authority。
目录不是 crate 边界，跨 crate 隔离仍由依赖图强制；物理归位不改变既有 wire 或 execution contract。

- ADR-0091 — Backend 模块为何按领域 owner 归位，并保留窄 RPC 基础设施（active）

### cluster-membership

领域哲学：外部 orchestrator 是 backend desired lifecycle 的唯一权威；Frontend 只保存可丢失的
announce/heartbeat/compatibility observation，并从中派生可调度 topology。core 只消费稳定的
`BackendTopologyPort`，不保留 metadata bridge、global registry、seed 或 durable membership fallback；多 FE
fencing/takeover 仍须单独裁决。

- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（active）
- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）
- [ADR-0111](ADR-0111-backend-self-registration-and-pre-ready-replanning.md) — Backend 为何以自注册、精确 heartbeat 与 pre-ready 完整重规划取代 durable membership（active；ControlReady 前整体重规划/变更执行重试由 ADR-0198 部分替代，外部成员管理、自注册、精确 heartbeat 与进程身份保留）
- [ADR-0124](ADR-0124-native-compatibility-islands-and-ingress-admission.md) — Backend compatibility island 为何以 exact identity 派生 eligibility，而不持久化升级 membership（active；兼容材料由 ADR-0185 部分替代，eligibility 规则保留）

#### 历史

- ADR-0013 — backend membership 为何由 frontend StateStore 单独持久化（superseded → ADR-0111）
- ADR-0103 — 中央 Provider wire authority 与同构 Native build admission 为何统一由 Protocol 和 Frontend topology 拥有（superseded → ADR-0105）
- ADR-0105 — Provider wire authority 为何与 SPI domain carrier 分离、但仍保持单一 Protocol digest（superseded → ADR-0106）

### catalog-attachment

领域哲学：「集群挂了哪些外部 catalog」是一份**精确全量的期望态快照**，来自三个互斥 source mode 之一
（静态文件 / StateStore / 未来的 managed controller）；frontend 是它唯一的 owner，每个 FE 把同一份快照派生成
自己的只读运行时投影，查询热路径只读内存、绝不逐次访问共享存储。StateStore 只是其中一个 mode，而不是通用真源。
枚举不完整是全局失败（绝不退化成空快照），单个 catalog 物化失败只隔离该 catalog。change hint 只是唤醒信号，
拿到提示后一律重读权威记录，丢通知与 retention gap 都退化为有界全量重建。DDL 以 durable commit /
exact-version delete 为线性化点，发布本地 control generation 才让 SQL catalog 名可解析、撤销发布先于退役
generation；`Absent`（未知）与 `Unavailable`（本机未物化）永远分开，store 不可用时 DDL 与超预算的 read
admission 一律 fail closed，不存在内存 fallback 或 legacy 双写。跨 family 的同事务约束不可用：被读的一侧若是
可清除的加速态，那个「保证」会在缓存被清空时静默消失。

- ADR-0115 — catalog 期望态为何收敛为单一 typed 快照 + 三个互斥 source mode（active；其继承自 ADR-0066 的 durable change-hint 机制已由 ADR-0143 换成进程内唤醒 + 周期 sweep，快照与重读裁决不变）
- ADR-0116 — `DROP CATALOG` 的 MV 引用检查为何降级为 best-effort 运维保护（active；裁决第 4 条点名的 `resolve_commit` 已由 ADR-0143 换成 attempt 观察，未知即三态的语义不变）

历史：

- ADR-0066 — 外部 catalog attachment 为何由 StateStore 单一持久化、各 FE 只派生只读投影（superseded → ADR-0115）

### frontend-state-families

领域哲学：湖是用户数据与已发布语义的唯一共享真源，因此每一份 frontend 本地状态只可能是三类之一：外部期望态的
投影、只属于当前进程/attempt 的运行态、可从外部权威确定性重建的加速态。第四类（既无外部真源、又要求持久）
不允许存在。分类是编译期穷尽的闭合枚举，运行态在类型上无法携带持久前缀，持久前缀与记录版本只在 manifest 中
定义一次；完成态的判据是扫描真实 store 内容，而不是检查源码里还有没有某个字面量。

- ADR-0114 — frontend 本地状态为何采用闭合三分类 manifest、且持久前缀只在 manifest 定义（active）
- ADR-0117 — 本地 view registry 为何是进程运行态而非 frontend durable 真源（active）

### frontend-durable-records

领域哲学：Frontend durable record 的正确性以最终可持久化的完整表示为准，而不是以 wire payload 或字段的局部长度为准。
不透明字节必须有界、使用单一 canonical 表示并在日志中脱敏；所有记录写入在外部副作用前经过实际编码预算校验。
StateStore 的全局单值限制保持公共契约，record owner 负责自己的 schema、状态机与错误映射，索引和控制值保持独立小值路径。

- ADR-0074 — Frontend durable record 为何统一采用有界 canonical 编码与整记录预算（active）
- ADR-0143 — StateStore 为何只回答自己签发过的 attempt，并把提交结果收敛为不可翻转的三态（active）

### frontend-dml

领域哲学：frontend 的受监督语句任务拥有 DML 编排与累计结果，原始 publication custody 不依附可丢弃的会话等待栈；Provider 保留准确外部效果与必要收尾真相。StateStore 不承担 DML operation、recovery 或 coordination authority，也不因异步化新增跨崩溃接管。typed engine port 连接领域执行，计算就绪与提交、诊断、回收分别推进。native persistent INSERT 当前只支持
Iceberg；Paimon 仅支持已声明的只读快照，StarRocks 已废弃且不能恢复内部表或 server runtime。每次写入必须复用 admission 冻结的 immutable request identity；跨 crate 只传中立 DTO 与 opaque
handles，不以 service locator、core callback、metadata fallback 或公共 SPI 模糊 owner。

- [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) — 各分布式意图为何共用逻辑执行 owner，分离计算就绪、累计效果、publication 与真实收敛，并复用唯一实际资源预算和可取消背压（active；部分替代 lake publication ADR-0110、后端自注册 ADR-0111、ADR-0133/0161/0171；不承诺永久阻塞隔离；架构已实施，验证范围、失败处置与验收资格独立核对）
- [ADR-0191](ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md) — 折叠为何冻结真实 binding 并复用 compiled 单节点核心，COW 赋值为何属于选择查询，兼容材料为何只保留真实 owner（active；部分替代 ADR-0100 的请求词汇与旧实现映射，其余有效规则保留）
- ADR-0020 — DELETE/equality-delete application flow 为何由 frontend 拥有、core 只保留过渡性 typed engine port（active）
- ADR-0021 — native frontend INSERT 为何只支持 Iceberg，并与 external StarRocks connector 隔离（active）
- [ADR-0110](ADR-0110-lake-publication-crash-only-contract.md) — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active；本地期限推导外部效果上界及无条件年龄窗回收由 ADR-0198 部分替代，target OCC、autocommit 与无持久接管规则保留）
- ADR-0033 — UPDATE/MERGE 为何由 frontend 拥有 application lifecycle、core 保留 opaque mutation reverse port（active）
- ADR-0063 — Copy-on-Write row mutation 的match与rewrite读源为何由Provider按exact base签发（active）

#### 历史

- ADR-0045 — change-stream 为何由 SQL 绑定 layout、Iceberg Connector 拥有 provider binding，DML/MV 只共享该 binding（superseded → ADR-0049）
- ADR-0062 — Copy-on-Write row mutation 的读源为何由 Provider 按 cohort 冻结并以中立 recipe 签发（superseded → ADR-0063）
- ADR-0032 — Frontend CTAS 为何使用 provider-owned staged publication，而不对可见表做破坏性补偿（superseded → ADR-0110）
- ADR-0046 — ADD FILES 为何以 provider canonical source scope 保护 frontend durable ownership（superseded → ADR-0110）
- ADR-0054 — Frontend DML 为何使用 operation-scoped StateStore authority、且不把它宣称为 external commit fencing（superseded → ADR-0110）
- ADR-0068 — 分布式 DML 的 external write fence 为何做成 catalog 原子条件更新里的线性化点（superseded → ADR-0110）
- ADR-0070 — CTAS takeover 为何使用 catalog-native absent-target fence，并对未广告能力的 catalog 提前拒绝（superseded → ADR-0110）

### frontend-mv

领域哲学：MV 领域拥有 D/L/P/C 的持久语义；provider 拥有准确物理事实、附着与提交。Frontend 拥有当前 MV refresh attempt 的准入、效果结算、query orchestration 与用户结果；StateStore 仅保存按湖上来源重建的 Accelerator，不承担 durable ledger。commit truth 使用 typed provider evidence，不从错误文本猜测，也不通过双 journal 或 aggregate facade 掩盖 owner。

- [ADR-0110](ADR-0110-lake-publication-crash-only-contract.md) — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active；本地期限推导外部效果上界及无条件年龄窗回收由 ADR-0198 部分替代，target OCC、autocommit 与无持久接管规则保留）
- ADR-0086 — MV storage observation 为何以中立 SPI facts 连接 provider 与 Frontend durable contracts（active）
- ADR-0154 — MV 领域文档与准确发布附着为何是湖上权威（active；存放与附着条款由 ADR-0180 部分替代，湖上权威、领域分工与 Accelerator 边界保留）
- [ADR-0180](ADR-0180-inline-keyed-documents-and-snapshot-records.md) — 应用文档为何按完整语义内联寻址、槽变更推进 main，且快照记录与生命周期回收分工（active；部分替代 ADR-0154、ADR-0171）

#### 历史

- ADR-0019 — INSERT application flow 为何由 frontend 拥有、core 只保留过渡性 typed engine port（superseded by ADR-0021）
- ADR-0036 — MV refresh 为什么由Frontend拥有 lifecycle，并以 provider-neutral committed version 保持receipt隔离（superseded → ADR-0110）
- ADR-0037 — 历史 MV refresh 为什么只能跨 incarnation 做 lake inspection 与 guarded cleanup（superseded → ADR-0110）
- ADR-0064 — MV publication 为何需要 lake 上专用的 fence ref，并在推进 main 的同一 commit 中做四方 exact 比较（superseded → ADR-0110）
- ADR-0038 — Frontend 为何拥有 MV background worker lifecycle 与 per-target activity gate（superseded → ADR-0112）
- ADR-0096 — MV refresh 所有权为何按 target 上锁、且必须在每个事务内校验（superseded → ADR-0112）
- ADR-0075 — ledger 丢失后 MV attempt 为何以 lake-first 有界发现 + 保守分类收敛，而非按时间/ID 猜 winner（superseded → ADR-0112）
- ADR-0109 — MV lake descriptor 为何是 desired semantics 的重建 authority，StateStore definition 仅为可重建 projection（superseded → ADR-0112）
- ADR-0112 — MV 运行态为何只属于当前进程、StateStore 为何只保留 lake-source Accelerator（superseded → ADR-0154）

### table-maintenance

领域哲学：表维护的 application/lifecycle 与 execution port 由 frontend host 统一拥有；connector 保留 catalog、snapshot、file 与 commit 等外部事实。job 是 ProcessRuntime，GC first-observation 是可重建 Accelerator；观察记录不赋予跨重启删除权。必要 finalize 与可延后维护分别监督，扫描回收的 SQL/自动入口共用 Provider 证明准入，缺证明确限制；不以 SPI、service locator、双写或内存 fallback 模糊 owner。

- [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) — 各分布式意图为何共用逻辑执行 owner，分离计算就绪、累计效果、publication 与真实收敛，并复用唯一实际资源预算和可取消背压（active；部分替代 lake publication ADR-0110、后端自注册 ADR-0111、ADR-0133/0161/0171；不承诺永久阻塞隔离；架构已实施，验证范围、失败处置与验收资格独立核对）
- [ADR-0196](ADR-0196-hadoop-test-catalog-complete-version-installation.md) — Hadoop 测试 catalog 为何共用完整不替换安装、只读版本发现与完整清单建删表，普通回收为何保护发现根（active；部分替代 ADR-0077/0118/0179 的 Hadoop 发现、存在性、DROP 与旧 UPDATE 协议，其余 owner 和生命周期保留）
- [ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md) — Iceberg 发布为何由冻结请求、唯一 journal 与独立证据期限拥有，正面核查为何只追加准确归属与收尾事实（active；部分替代 ADR-0118/0171 的 Transaction 与 late carrier、ADR-0175 的会话/生命周期、ADR-0174 的 P2/U4 退出路由；保留认证/路由及 IRU-6b/6c 边界；永久依赖图守卫由 ADR-0190 部分替代，发布生命周期裁决保留）
- ADR-0083 — 表维护 execution port 为何必须随 query assembly 归 Frontend，不能以 Core cohort bridge 留存（active）
- [ADR-0110](ADR-0110-lake-publication-crash-only-contract.md) — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active；本地期限推导外部效果上界及无条件年龄窗回收由 ADR-0198 部分替代，target OCC、autocommit 与无持久接管规则保留）
- ADR-0111 — Frontend maintenance/statistics job 为何是 ProcessRuntime，而 GC first-observation 是 Accelerator（active）
- [ADR-0177](ADR-0177-attempt-local-maintenance-inputs-and-single-rewrite-plan.md) — 维护输入为何只驻留当前尝试，且重写读取、writer 与提交为何共用同一份冻结计划（active；部分替代 ADR-0028）
- ADR-0057 — MV 维护事实为何按「是否需要 provider runtime IO」切成观测口投影与 SPI capability 两条通道（active）

#### 历史

- ADR-0009 — 表维护为何由 frontend 拥有 application/lifecycle，并通过 core domain port 调用 connector truth（superseded → ADR-0083）
- ADR-0035 — Connector orphan cleanup 为何使用 immutable manifest、逐 batch receipt 与 reconcile-only unknown（superseded → ADR-0110）
- ADR-0065 — 同一张表的维护为何以单个 per-table lease attempt 为唯一派发权威、并在同事务内校验 fence（superseded → ADR-0111）
- ADR-0067 — 收敛已死 generation 的维护为何是独立 provider capability，而不是放宽 exact-generation reconcile（superseded → ADR-0111）

### memory-governance

领域哲学：Rust 的普通分配失败会终止进程，所以可恢复的容量控制只能建立在**比单次分配更粗的事前授权**上——先取额度、再分配、最后结算，且授予额度内的结算不会因容量失败。一个进程一个权威，账户成严格树，账户对父级持有的份额**就是**它的承诺；已下放的独立域内 funded 步骤不触父/root；补额修改在足够的父级余额停止，祖先增长资格门仍须握手。配置目标与历史债务独立。归属绑定在稳定 allocation origin，真实退出后残留交同分支祖先，物理释放以前仍计费。覆盖是**两级**且诚实的：硬治理覆盖声明的对象集合，其余由进程分配观测按 headroom 预算测量并列出盲区——不冒充按查询精确归属，也不承诺避免所有物理 OOM。授权、实际分配、共享持有、回收估计与物理压力是五个必须分别表达、永不相加的事实。本领域与 `runtime-role` 的工作治理是同一进程内的两半：ADR-0147 拥有责任、scope、阶段准入与取消，本领域拥有容量机制本身，并回答 ADR-0147 重评估条件 2 点名的「扩展真实内存来源与强制接入」。

- ADR-0148 — 为何采用唯一的非等待进程容量权威（active；旧余额、Arrow charge 与仲裁框架由 ADR-0166 替换，观察责任保留；全局 allocator 归属由 ADR-0167 部分替换）
- ADR-0167 — 尺寸分段归属为何用尾部身份与计数/槽 pin 保活，并分开来源、当前责任与资金授权（active；整体替换 ADR-0166，部分替换 ADR-0148/0163 的全局计数器前提；观测 owner 的覆盖失败、容量分区、future 作用域与角色诊断由 ADR-0197 部分替代，资金与存活协议保留）
- [ADR-0197](ADR-0197-observation-owner-shards-and-coverage-shadow.md) — 观测 owner 为何整组发布分片、在覆盖不可用时遮蔽外层，并隔离资金记录容量（active；部分替代 ADR-0167 规则 1、3、5、10）
- ADR-0162 — 查询中间计算状态为何保持内存内，并完整退出外溢能力（active；收敛 ADR-0148 的回收动作范围，不改变容量与谱系规则）
- ADR-0156 — Connector FE 操作控制为何不授予容量，以及 BE 为何只接受准入后的真实资源（active；不改变 ADR-0148 的 BE 容量权威）
- ADR-0163 — 进程为何由构建期选定的 jemalloc 分配，并从本进程的 cgroup 路径得知内存上限与用量（active；物理 allocator/cgroup 合同保留，外层归属由 ADR-0167 部分替换）
- ADR-0170 — 为何只用公开配置、库外准入与测量约束第三方库内部，而不为资源计量 vendor/fork 第三方 crate（active；patch allowlist 与 Iceberg vendor 基线/锚点由 ADR-0174 部分替代，其余规则保留）

#### 历史

- ADR-0166 — 分层资金与稳定分配来源（superseded → ADR-0167）
- ADR-0160 — Arrow 留存谱系与 Reservation 共享叶子（superseded → ADR-0166）

### crate-boundary

领域哲学：crate 结构表达依赖事实，编译器拒绝未声明的 crate 使用；哪些边可以新增、optional/feature/target 与传递能力是否符合 owner 边界，由 review 判断。CI 保留供应链与通用 lint，不增加依赖边界、源码形状或产品前提检查。迁移期临时检查随迁移完成一并删除。

- [ADR-0200](ADR-0200-advisory-discovery-is-independent-of-admission.md) — 公告发现为何独立于准入、保留真实失败与动态输入追溯，并报告传递 unsound（active）
- [ADR-0190](ADR-0190-dependency-direction-by-structure-and-review.md) — 依赖方向为何由 crate 结构与 review 维持、CI 为何保留通用检查，第三方资源预留为何按准入估计理解（active；部分替代 ADR-0140/0150/0168/0179 的具名检查与验收门规则；全局 Clippy warning-only 现状由 ADR-0194 替代，依赖治理与资源估计仍有效）
- [ADR-0194](ADR-0194-strict-clippy-ci-baseline.md) — Clippy 为何只在 CI 阶段严格阻止警告、哪些放宽项与局部例外构成最小基线（active；仅替代 ADR-0190 的全局 Clippy warning-only 现状）

- [ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md) — Iceberg 发布为何由冻结请求、唯一 journal 与独立证据期限拥有，正面核查为何只追加准确归属与收尾事实（active；部分替代 ADR-0118/0171 的 Transaction 与 late carrier、ADR-0175 的会话/生命周期、ADR-0174 的 P2/U4 退出路由；保留认证/路由及 IRU-6b/6c 边界；永久依赖图守卫由 ADR-0190 部分替代，发布生命周期裁决保留）
- ADR-0058 — 架构隔离为何由 crate 边界强制，而不用硬编码的 source-shape guard（active）
- [ADR-0184](ADR-0184-native-test-execution-and-process-monitor-ownership.md) — 原生逐测试进程为何与独立 payload monitor、懒建调用租约和封闭材料汇总分工（active；部分替代 ADR-0069 的进程内 reader / Drop 所有权，机械叶子与领域边界保留）
- ADR-0069 — 共享测试机械能力为何使用零产品依赖的独立叶子 crate，而领域断言仍跟随其 owner（active；进程内 reader / Drop 所有权由 ADR-0184 部分替代，机械叶子与领域边界保留）
- ADR-0071 — 分布式测试编排为何由唯一 cluster harness 拥有、SQL runner 只作 frontend adapter（active；其中 SQL runner 独立 workspace/profile 前提由 ADR-0142 替换）
- ADR-0138 — Paimon 0.3.0 为何仅为有界授权读取接缝而维护可追溯 vendor patch（active）
- ADR-0128 — Lifecycle canonical engine is private behind typed digest APIs（active）
- ADR-0094 — 空 catalog crate 为何在真实 owner 收敛后删除，而不保留 facade（active）
- ADR-0144 — 应用领域为何沿稳定 owner 接缝物理化，依赖为何指向政策而移动接缝留在模块内（active）
- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）
- ADR-0122 — SQLite 为何是唯一 production StateStore、远程 provider 仅保留实验 leaf crate（active；其中 schema 版本、history 保留与 commit-resolution 三项承诺已由 ADR-0143 替换，产品裁决仍有效）
- ADR-0140 — StateStore 契约为何从统一 SPI package 物理独立、测试机制为何单独成 crate（active；守卫形态与依赖边界检查由 ADR-0190 部分替代，契约/testkit 拆分保留；替换 ADR-0006 的统一物理 SPI 前提）
- ADR-0142 — NovaRocks 自有 packages 为何共享一个 Cargo workspace、resolver 与 lock authority（active）
- ADR-0150 — 概率结构为何精确锁定 registry DataSketches 正式版、并以版本无关的升级门管辖后续变化（active；版本/checksum 字面值规则由 ADR-0168 替换；永久图/lockfile 守卫与第 5 道结构性 benchmark 验收门由 ADR-0190 部分替代；精确正式版锁定、最小 feature 由 review 维持，格式所有权与第 1–4 道门保留；第 5 道门按当前阶段政策不作为验收门，恢复性能工作时重评）
- ADR-0168 — CI 依赖守卫为何从 Cargo 权威派生版本与 checksum、同时保护来源身份和封闭依赖面（active；部分替代 ADR-0150；身份与测试权威守卫规则由 ADR-0190 部分替代，版本仅由根 manifest 与 Cargo.lock 表达的规则保留）
- ADR-0170 — 为何只用公开配置、库外准入与测量约束第三方库内部，而不为资源计量 vendor/fork 第三方 crate（active；patch allowlist 与 Iceberg vendor 基线/锚点由 ADR-0174 部分替代，其余规则保留）
- [ADR-0174](ADR-0174-vendored-iceberg-baseline-and-patch-policy.md) — vendored Iceberg 的基线为何可重建、协议补丁为何由消费 owner 负责退出（active；部分替代 ADR-0170；REST crate 补丁清单、裁决 4 的临时结果收窄与 SDK 认证妥协由 ADR-0175 部分替代，core U2/P8/View API 保留项由 ADR-0176 部分替代，U8 与 Variant 读取暂缓由 ADR-0178 部分替代，P2 退出和 U4 交接由 ADR-0179 部分替代，基线可重建与其余 core 补丁的消费方退出仍有效）
- [ADR-0175](ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md) — Iceberg REST 为何每个 generation 只有一份会话、认证头只由角色本地 owner 写、BE 按冻结路由上下文取得，并把 NovaRocks 发出的每个变更请求归为可归属的证据（active；部分替代 ADR-0151 的取得路径含义与 ADR-0174 的 REST 补丁清单、临时结果收窄和 SDK 认证妥协；「core 中只为 REST 服务的补丁仍在」这一妥协由 ADR-0176 部分替代，会话建立、SDK facade、派发后期限与 FE stage 结果投影由 ADR-0179 替代；认证、路由、状态码与脱敏规则仍有效）
- [ADR-0176](ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md) — Iceberg REST 请求词汇为何归连接器、`add-schema` 为何不带 `last-column-id`（支持下限为 Iceberg Java 1.9 语义），view 替换为何总是新增带操作身份的版本并以 `-1` 选择、语义校验为何在派发之前（active；部分替代 ADR-0174 的 core U2/P8/View API 保留项与 ADR-0175 的 core REST 补丁妥协）

#### 历史

- ADR-0134 — 概率结构为何精确依赖 registry DataSketches 预发布包、由上游唯一拥有标准格式与集合运算（superseded → ADR-0150）
- ADR-0098 — native DTO 的字段路径与验证错误为何由 Protocol 独占、而不保留 Core family 门面（superseded → ADR-0106）
- ADR-0099 — 聚合 Core 退场后为何按真实 owner 物理切断依赖（superseded → ADR-0108）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（superseded → ADR-0126）
- ADR-0126 — Terminal delivery 为何以 participant attempt identity 而非 payload content identity 定位（superseded → ADR-0127）
- ADR-0127 — participant attempt identity 如何围栏 immutable Stage admission（superseded → ADR-0128）
- ADR-0093 — StateStore provider 为何作为 leaf crate、Frontend 直接拥有 consumer runtime（superseded → ADR-0122）

### configuration

领域哲学：Server composition root 唯一拥有完整应用 TOML wire、默认值、加载与跨 section 校验，并在启动时一次性投影为
各 owner 的 resolved typed input；domain crate 不重复解析 TOML，也不接收完整根配置。不存在进程级 `config()` 单例，
debug/test 开关归启动进程环境。跨域 wire section 不因名字相似而整体归某一 kernel crate，Core 不拥有 application schema。

- ADR-0087 — 进程 data runtime 为何由 Server 创建并经 role-local adapter 注入 FE/BE（active）
- ADR-0107 — 静态 startup secret 为何由 Server exact resolve，并向 provider 投影 direct credential（active）
- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）

#### 历史

- ADR-0059 — 配置为何由组合根注入，而不从进程全局读取（superseded → ADR-0072）
- ADR-0072 — 完整应用配置 wire 为何由 Server 唯一拥有，并投影为各 domain 的 resolved typed input（superseded → ADR-0108）

### error-contracts

领域哲学：用户可见 SQL error 必须由最早拥有完整语义的 domain 产生。Parser 不把已识别的语法能力边界伪装为 catalog
NotFound；Connector 不让通用 field lookup 遮蔽 table-format invariant；Frontend router 不根据文本或错误字符串重猜 SQL
statement family。边界层只能传递或编码 owner 的事实，测试只能验证既定契约，不能成为第二错误 authority。

- [ADR-0193](ADR-0193-task-conclusion-proposals-and-settled-points.md) — Task 结论为何只交接提议、以结算屏障支撑定局，并由唯一 owner 推进终止（active；仅部分替代 ADR-0187 规则 5 的隐含同步收据，单一接受事务、细化及其余规则保留）
- [ADR-0187](ADR-0187-receiver-owned-steps-and-single-task-conclusion.md) — 接收方为何接受后准备、领域失败为何贯穿唯一 Task 结论，并由原 owner 继续观察真实退出（active；仅部分替代 ADR-0095 对已知非 Analyze 执行事实的不透明化，Analyze code/span 与历史 MV 边界保留；规则 5 的隐含同步收据由 ADR-0193 部分替代，单一接受事务、细化及其余规则保留）
- ADR-0088 — SQL、Iceberg 与 MV 的错误为何必须在各自 owner 内、跨域 fallback 前收敛（active）
- ADR-0095 — SQL analyze 错误为何以 typed code/span 穿过 Frontend、由 MySQL 边界映射（active；已知非 Analyze 执行事实的不透明化由 ADR-0187 部分替代，Analyze 与历史 MV 规则保留）

### test-fixtures

领域哲学：测试运行不获取外部产物。镜像、Maven artifact 和 derived image 的身份由锁定输入与本机 READY BOM
共同证明；网络获取与 Docker build 只属于显式 provisioning。verify 与测试消费者只读取本机证据，缺失是 BLOCKED
而不是下载或普通测试失败。

- ADR-0155 — fixture 输入为何以锁定 provision/BOM 供给、并在离线 verify 阶段消费（active）
- [ADR-0191](ADR-0191-machine-shared-fixture-and-bounded-scratch.md) — 每个daemon为何只有一组共享fixture服务，维护显式、scratch有界、原owner确认实际writer退出（active；替代ADR-0165运行层，保留ADR-0155供给与ADR-0173 check-only消费）
- [ADR-0199](ADR-0199-readonly-observation-and-mutation-settlement.md) — 只读观察失败为何与变更结算分开，由原owner显式处置准确观察而保留原失败及未知退出（active；补充ADR-0191，原writer退出与scratch屏障保留）
- [ADR-0202](ADR-0202-benchmark-ready-after-complete-data.md) — 标准数据为何只在完整后普通发布 READY，删除条件覆盖与原地重建而保留准确只读消费（active；部分替代 ADR-0173 的条件发布与 rebuild）
- [ADR-0173](ADR-0173-prepared-dataset-consumption-is-check-only.md) — 已有数据消费者为何仅check准确READY绑定，而实际producer核验与数据写入保留给显式供给（active；部分替代ADR-0165条9；条件发布与原地 rebuild 由 ADR-0202 替代）

#### 历史

- ADR-0165 — fixture运行实例按输入版本化及显式绑定/删除的历史模型（superseded → ADR-0191；已有READY消费裁决继续由ADR-0173承载）
- ADR-0141 — fixture 镜像为何只从本机镜像库按 digest 解析、不可变性校验为何移入显式预检（superseded → ADR-0155）
