# 架构决策记录（ADR）

本目录是 NovaRocks 的架构决策知识库：每条 ADR 固化一个**已裁决的设计问题**——背景、考虑过的选项、裁决、**接受的妥协**与**何时重新评估**。检索单位是「架构师会问的问题」，不是「当年做过的任务」。改动某子系统的架构级行为前，先在下方领域索引里找到它。

**怎么写**：用 `workbench` plugin 的 `$ops-capture` skill（`.agents/skills/workbench/`，Codex 直接发现、Claude Code 经 `.claude/skills` → `.agents/skills` 软链发现）新建、supersede 或重编号——其契约 `ops-contract.md` §7 内嵌模板与全部规则；人工手写请复制下方模板。正文中文；id/slug/tags/frontmatter 键用英文。

> **本 README 是本目录的权威**：`ops-contract.md` 与本文冲突时以本文为准（该契约自身也如此规定）。

**核心规则**：

- **不可变，永不删除**：ADR 合入后不改实质内容。立场变化时写新 ADR 标 `supersedes`，旧条标 `superseded-by` 并移入领域节末尾「历史」小节。被完全覆盖的 ADR 也**不删除**——它记录的是「曾经那样做过、为什么、后来为什么改了」，防止未来（人或 AI）循环重提已否决方案，并保住 provenance 链。允许物理删除的仅有例外：误创建/重复、从未定稿的草稿、泄密与合规问题。允许的原地修改仅限：状态字段、锚点链接的机械修正、错别字、编号冲突重编号。
- **自包含**：结论、选项、妥协必须在文件内完整可读，不依赖外部文档才能理解；设计工坊（vault spec）、PR、讨论日期仅作 provenance 字段。
- **编号**：`ADR-NNNN` = 现存最大编号 + 1，四位零填充。并行 PR 撞号时，**后合入者在 rebase 后重新编号**，并同步修改四处：①文件名、②frontmatter `id`、③本 README 索引行、④代码中全部 `Design: ADR-NNNN` 锚点（含其它 ADR 的交叉引用）。
- **代码锚点**：承重代码处放一行英文注释 `// Design: ADR-NNNN (docs/adr/ADR-NNNN-<slug>.md)`，只放「改这里之前必须读」的位置，不铺开。
- **引用持久性**：ADR 随仓库存续，正文与 provenance **不得依赖不随仓库维护的外部设计文档编号**（如设计工坊里的 RFD-* 任务号——那些文档后期可能删除失联）。指称某项工作用**机制名 + GitHub PR 号**（如「build-frontier proof 全局验环，PR #726」）或**其它 ADR 的编号**；尚未合入的工作用机制描述，PR 合入后回填编号（属允许的机械修正）。
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
（未来架构师会问的那个问题，一句话。检索入口。）

## 背景与执行事实
（成立此决策的客观事实，带符号名锚点；不写会腐烂的行号。）

## 考虑过的选项
（每个选项一段：机制、优势、代价。对照过外部系统的写明对照结论。）

## 裁决
（选了什么。）

## 接受的妥协（诚实记录）
（为此放弃了什么、真实理由是什么——「因改动成本而非因更优」这类必须如实写。）

## 何时重新评估
（触发条件清单：负载形态、依赖成熟度、指标阈值。）
```

## 领域索引

### runtime-filter

领域哲学：runtime filter 是**纯性能优化**——任何 activation、等待、降级策略都不得改变 SQL 结果语义（RF 是保守预过滤，join 本体兜底）。数据面走 query-global `RuntimeFilterGraph + DeploymentCompiler + Service`；静态层**宁严勿宽**（证明不了安全就在 fragment submission 前 fail-fast），运行时 timeout + PassThrough 只是生产可用性兜底、不是语义权威。planner 与 deployment 双侧独立验证，二者之间不传裸布尔结论。

- ADR-0001 — runtime filter 等待环为何静态 strict-fail，而不是靠运行时 timeout 兜底（active）
- ADR-0002 — multicast 反压为何保持消费者耦合（active）
- ADR-0003 — RF consumer 为何默认 BlockingSnapshot、NonBlockingLive 只做定点降级（active）
- ADR-0027 — Runtime Filter 规划为何由 SQL 私有拥有、角色之间只交换封存 wire facts（active）
- ADR-0043 — Runtime Filter row/scan evaluator 为何统一由 Execution 拥有、Backend 只提供 artifact query（active）
- ADR-0044 — Runtime Filter participant 物理生命周期为何由 Backend 拥有、Execution 保留语义值与 evaluator（active）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（active）
- ADR-0114 — participant 分类为何以载荷为唯一权威表示，删除自证式派生的 participant_roles 字段（active）

#### 历史

- ADR-0041 — Runtime Filter scan-domain 评估为何由 Execution 拥有、Core 只提供中立 artifact capability（superseded → ADR-0043）
- ADR-0076 — Runtime Filter terminal observation 为何由 Backend participant 有界聚合、并仅经 typed QLC contribution 出域（superseded → ADR-0078）
- ADR-0078 — Runtime Filter terminal observation 为何只作观测，且以 P0/P1/P2 查询终止契约交付（superseded → ADR-0106）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0053 — MV snapshot change window 复用 exact-generation scan planning（superseded → ADR-0114）
- ADR-0039 — scan unit 的 immutable、bounded physical domain facts（superseded → ADR-0114）
- ADR-0034 — cluster composite split 与 Backend local scan unit 的两级生命周期（superseded → ADR-0114）

### join-execution

领域哲学：join 执行核心是 purpose-built 的——join 的拓扑（key → 枚举该 key 全部 build 行 + gather 物化）与聚合（key → 单份累加态）不同，不共享聚合的 KeyTable 形状。速度来自算法与数据布局（直接寻址、合并列存、选择向量、membership 零枚举、整列直发），不来自手写 SIMD。任何新档位/快路径合入时，全部 join 类型 + null-safe 等值 + 残差谓词的全套件必须每一步全绿——正确性绝不为分层或向量化让步。

- ADR-0004 — hash join 执行核心为何与聚合 KeyTable 分家、自建 purpose-built join_hash_map，且不写显式 SIMD（active）

### low-cardinality

领域哲学：编码是执行载体的自描述物理属性，不是 plan 必须背书的正确性契约。correctness 由载体保证——`Dictionary(Int32, Utf8)` 自描述 + 算子入口 hydrate 兜底，算子不认识编码时的默认后果必须是「慢」而非「错」（fail-safe，不是 fail-open）；plan/元数据层只声明快路径资格，误判最坏是少一次加速。lake-native（不拥有数据、无内表）是前提约束：native 侧不建表级全局字典，FE-compatible 全局字典执行是隔离的协议侧支。

- ADR-0005 — 低基数编码为何运行时载体优先：DictionaryArray 是 correctness owner、plan 层只是加速器（active）

### provider-spi

领域哲学：SPI 只承载 NovaRocks 产品架构明确支持的可替换 provider 契约，不吸收所有跨 crate API。系统定义契约语义，provider 与 consumer 共同依赖统一 SPI，host 负责选择、装配与生命周期；domain API、consumer port、transport 和实现策略保持各自 owner。稳定性由原子演进、conformance 与真实分布式验证保证，不靠兼容 bridge 或 service locator。

- ADR-0006 — 可替换 provider 契约为何统一进入一个系统 SPI，而普通跨 crate port 不进入（active）
- ADR-0014 — 共享文件访问与 Parquet/ORC 物理解码为何属于无 Connector identity 的独立基础（active）
- ADR-0015 — table-format Connector 为何拥有 read correctness，native fragment 只绑定已安装真实 instance（active）
- ADR-0016 — Connector 为何共享逻辑 identity、但不共享 FE control 与 BE execution runtime（active）
- ADR-0017 — Connector catalog mutation 为何使用 FE-only lease 与三态 external outcome（active）
- ADR-0018 — 静态 Connector predicate 为何以 Exact/PruningOnly/Unsupported 协商、而不扩展 native wire（active）
- ADR-0022 — Connector statistics capability 为何保持 FE-only、generation-fenced 且不进入 BE binding（active）
- ADR-0023 — distributed writer 为何以 operation/cohort/execution/writer 分层，并由 FE 聚合外部提交（active）
- ADR-0024 — 无需 BE staging 的 data mutation 为何使用 FE-only frozen plan 与 marker-only reconcile（active）
- ADR-0028 — metadata maintenance 为何由 FE 以 exact lease、durable plan 与 marker reconcile 执行（active）
- ADR-0029 — distributed rewrite 为何以 frozen groups、C1 cohorts 与 FE aggregate commit 实现单 snapshot（active）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（active）
- ADR-0104 — Connector execution binding 为何使用 SPI domain declaration、sealed Host 与可重放失败状态机（active）
- ADR-0049 — row mutation 的 strategy、identity、route 与 cohort 为何由 Provider 签发并拥有（active）
- ADR-0051 — distributed write 为何在 preparation 与 planning 之间强制 exact-generation Provider activation（active）
- ADR-0052 — SHOW CREATE 为何以 exact lease 的有界 table-definition facts 取代 concrete table decode（active）
- ADR-0055 — row-DML 调用方为何只读 Provider 签发的 strategy，而 SQL 谓词合法性为何留在 Core（active）
- ADR-0056 — 摘除 Core 对 provider 的测试依赖时，无法用冻结 SPI facts 表达的断言为何归位到实现旁而非复刻或删除（active）
- ADR-0063 — Copy-on-Write row mutation 的match与rewrite读源为何由Provider按exact base签发（active）
- ADR-0077 — Hadoop catalog 创建表为何以 storage 条件创建 v1 metadata 作为线性化点（active）
- ADR-0085 — durable caller 为何通过既有 exact metadata lease capture/rebind 物理表对象，而不依赖统计能力或新增平行 authority（active）
- ADR-0080 — 统计证据为何拆成 collection 级覆盖度与 per-metric 基准版本/来源/数值性质/集合关系四个独立维度（active）
- ADR-0081 — 统计为何是带版本、允许陈旧、由读侧逐 metric 决定可用性的估计事实（active）
- ADR-0082 — 同一快照上的统计发布为何以覆盖度排序、且冲突重试必须重新判定（active）
- ADR-0111 — Frontend maintenance/statistics job 为何是 ProcessRuntime，而 GC first-observation 是 Accelerator（active）
- ADR-0097 — durable MV 与维护基表 identity 为何保持 opaque、只在 provider-local 边界解释（active）
- ADR-0089 — Predicate-driven Parquet page pruning 为何只在 FS reader-open 按实际 physical leaf 计算（active）
- ADR-0110 — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active）
- ADR-0112 — MV 运行态为何只属于当前进程、StateStore为何只保留 lake-source Accelerator（active）
- ADR-0119 — Connector read 内部 runtime SPI 与 wire codec 为何分离（active）
- ADR-0118 — Iceberg catalog 语义为何收敛到一个 provider-private owner，并以 operation-shaped admission 取代能力表（active）

#### 历史

- ADR-0048 — distributed write为何以 Provider-signed preparation、exact lease 与中立 durable terminal fact 收敛 caller authority（superseded → ADR-0051）
- ADR-0062 — Copy-on-Write row mutation 的读源为何由 Provider 按 cohort 冻结并以中立 recipe 签发（superseded → ADR-0063）
- ADR-0060 — MV refresh base pin 为何必须从同一 exact metadata 投影 UUID 与 current snapshot（superseded → ADR-0086）
- ADR-0047 — catalog/read admission 为何以 exact Connector generation 与中立 native carrier 封存（superseded → ADR-0103）
- ADR-0103 — 中央 Provider wire authority 与同构 Native build admission 为何统一由 Protocol 和 Frontend topology 拥有（superseded → ADR-0105）
- ADR-0068 — 分布式 DML 的 external write fence 为何做成 catalog 原子条件更新里的线性化点（superseded → ADR-0110）
- ADR-0070 — CTAS takeover 为何使用 catalog-native absent-target fence，并对未广告能力的 catalog 提前拒绝（superseded → ADR-0110）
- ADR-0084 — durable statistics job target binding（superseded → ADR-0111）
- ADR-0061 — MV repartition 为何由 Provider 在单次原子 write commit 中同时切换 partition spec 与 snapshot（superseded → ADR-0112）
- ADR-0114 — Connector read 为何改用 Trino 对齐的 typed handle/split/page-source 与运行时 split 投递（superseded → ADR-0119）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
### distributed-query-lifecycle

领域哲学：FE coordinator 拥有全局编排，BE query lifecycle 拥有本地执行与资源；两者是独立进程、故障域与状态机，
只通过版本化 wire protocol 交换事实。共享面仅限 immutable wire/value contract、codec 与 pure validation，
不得以 all-in-one 便利、feature-specific flow 或共享 runtime state 绕过边界。协议必须正面处理重复、延迟、乱序、
丢失、过期 ownership 与进程失败，使分布式生命周期可测试、可观察并可独立演进。

- ADR-0007 — FE 全局协调与 BE 本地查询生命周期为何保持进程和状态机分离（active）
- ADR-0008 — 分布式查询为何使用 Init/Stage/Start 三阶段启动（active）
- ADR-0011 — 请求执行为何使用 immutable context、一次 topology capture 并拒绝 ambient fallback（active）
- ADR-0012 — Query session admission 与 router 为何由 frontend 拥有、core 只保留 wire/compiler kernel（active）
- ADR-0102 — MySQL KILL 为何经 exact generation token 与 protocol-owned connection lifecycle 实现（active）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（active）
- ADR-0114 — participant 分类为何以载荷为唯一权威表示，删除自证式派生的 participant_roles 字段（active）
- ADR-0092 — 查询 execution identity 为何以 process-local namespace 与连续 sequence 保持既有 wire 形状（active）

#### 历史

- ADR-0010 — 显式 query cancellation surface 为何以 MySQL KILL QUERY 和 frontend session owner 实现（superseded → ADR-0102）
- ADR-0076 — Runtime Filter terminal observation 为何由 Backend participant 有界聚合、并仅经 typed QLC contribution 出域（superseded → ADR-0078）
- ADR-0047 — catalog/read admission 为何以 exact Connector generation 与中立 native carrier 封存（superseded → ADR-0103）
- ADR-0103 — 中央 Provider wire authority 与同构 Native build admission 为何统一由 Protocol 和 Frontend topology 拥有（superseded → ADR-0105）
- ADR-0078 — Runtime Filter terminal observation 为何只作观测，且以 P0/P1/P2 查询终止契约交付（superseded → ADR-0106）
- ADR-0079 — IDL/proto 为何是 FE/BE 中立 query lifecycle 契约的规范形式、Protocol 如何同时拥有 schema 与已验证值（superseded → ADR-0106）
- ADR-0098 — native DTO 的字段路径与验证错误为何由 Protocol 独占、而不保留 Core family 门面（superseded → ADR-0106）
- ADR-0105 — Provider wire authority 为何与 SPI domain carrier 分离、但仍保持单一 Protocol digest（superseded → ADR-0106）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）

### sql-compiler

领域哲学：SQL compiler 只消费一次 statement admission 冻结的 SQL 事实，并只产出 SQL facts；application owner 保留 session、view rewrite、topology、native encoding 与 lifecycle assembly。catalog、statistics 与 scan preparation 对同一表必须复用同一 exact Connector binding，缺失事实必须显式失败或保守降级，不能重新读取 latest。

- ADR-0073 — SQL compiler 为何先完成全部 binding 分析物化、再冻结 statistics 并以无 catalog 的第二阶段优化封存（active）
- ADR-0040 — SQL compiler 为何先完成依赖倒置闭包、再进行独立 crate 物理迁移（active）
- ADR-0050 — sealed DistributedPlan 为何以 logical mutation effect 与 opaque provider route 服务跨 owner encoder（active）
- ADR-0100 — 常量折叠为何经注入端口复用执行 kernel，并对无法一致表示的结果拒绝折叠（active）

#### 历史

- ADR-0025 — SQL compiler 为何以显式 request、immutable snapshots 与 post-compile binding context 形成唯一入口（superseded → ADR-0073）
- ADR-0042 — sealed DistributedPlan 为何以单一只读契约服务跨 owner encoder（superseded → ADR-0050）

### sql-language

领域哲学：SQL语言事实由自有parser及其source span定义；持久化定义保存用户有效原文与创建时解析上下文，不保存
normalizer、AST mutation或printer生成的内部表示。运行期可以按请求派生typed/canonical facts，但派生物不能反向成为
语言或durable authority。

- ADR-0090 — MV/View定义为何持久化用户有效SQL原文与解析上下文，而不是AST Display（active）
- ADR-0101 — native SQL language authority 为何分离 parser、SQL semantic value 与 Frontend application request（active）

### runtime-role

领域哲学：生产运行时只由 native FE/BE 角色组成。FE 拥有 SQL admission 和
全局协调，BE 拥有本地执行；all-in-one 仅为测试便利且不得改变这条边界。
外部系统经 Connector 接入，不以 inbound 兼容 server、daemon 或全局 bridge
混入 native lifecycle。StarRocks 仅是只读 Connector：RPC 覆盖所有拓扑，direct
永久只支持 shared-data。


- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）
- ADR-0118 — Iceberg catalog 语义为何收敛到一个 provider-private owner，并以 operation-shaped admission 取代能力表（active）
- ADR-0119 — FE serving lifecycle 为何用单向 admission drain、而不是 connection shutdown 或远程 management mutation（active）

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
- ADR-0111 — Backend 为何以自注册、精确 heartbeat 与 pre-ready 完整重规划取代 durable membership（active）

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

- ADR-0115 — catalog 期望态为何收敛为单一 typed 快照 + 三个互斥 source mode（active）
- ADR-0116 — `DROP CATALOG` 的 MV 引用检查为何降级为 best-effort 运维保护（active）

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

### frontend-dml

领域哲学：frontend 拥有 DML 的 statement application flow 与 production routing；每个 publication attempt 仅存活于
当前 request stack，StateStore 不再保存 DML operation、recovery 或 coordination authority。core 只通过一对一 typed
engine port 保留 query、connector 和 external commit truth。native persistent INSERT 当前只支持
Iceberg；StarRocks 只作为 read-only external connector，不能恢复内部 StarRocks 表或 server runtime。每次写入必须复用 admission 冻结的 immutable request identity；跨 crate 只传中立 DTO 与 opaque
handles，不以 service locator、core callback、metadata fallback 或公共 SPI 模糊 owner。

- ADR-0020 — DELETE/equality-delete application flow 为何由 frontend 拥有、core 只保留过渡性 typed engine port（active）
- ADR-0021 — native frontend INSERT 为何只支持 Iceberg，并与 external StarRocks connector 隔离（active）
- ADR-0110 — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active）
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

领域哲学：Frontend 拥有当前 MV refresh attempt 的 application state、durable ledger、query orchestration 与用户结果；SQL、provider 和 Backend 各自只承担其真实职责。commit truth使用typed provider evidence，不从错误文本猜测，不通过双journal或aggregate facade掩盖owner。

- ADR-0110 — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active）
- ADR-0086 — MV storage observation 为何以中立 SPI facts 连接 provider 与 Frontend durable contracts（active）
- ADR-0112 — MV 运行态为何只属于当前进程、StateStore为何只保留 lake-source Accelerator（active）

#### 历史

- ADR-0019 — INSERT application flow 为何由 frontend 拥有、core 只保留过渡性 typed engine port（superseded by ADR-0021）
- ADR-0036 — MV refresh 为什么由Frontend拥有 lifecycle，并以 provider-neutral committed version 保持receipt隔离（superseded → ADR-0110）
- ADR-0037 — 历史 MV refresh 为什么只能跨 incarnation 做 lake inspection 与 guarded cleanup（superseded → ADR-0110）
- ADR-0064 — MV publication 为何需要 lake 上专用的 fence ref，并在推进 main 的同一 commit 中做四方 exact 比较（superseded → ADR-0110）
- ADR-0038 — Frontend 为何拥有 MV background worker lifecycle 与 per-target activity gate（superseded → ADR-0112）
- ADR-0096 — MV refresh 所有权为何按 target 上锁、且必须在每个事务内校验（superseded → ADR-0112）
- ADR-0075 — ledger 丢失后 MV attempt 为何以 lake-first 有界发现 + 保守分类收敛，而非按时间/ID 猜 winner（superseded → ADR-0112）
- ADR-0109 — MV lake descriptor 为何是 desired semantics 的重建 authority，StateStore definition 仅为可重建 projection（superseded → ADR-0112）

### table-maintenance

领域哲学：表维护的 application/lifecycle 与 execution port 都由 frontend host 统一拥有；connector 保留 catalog、
snapshot、file 与 commit 等 external-system truth，聚合 core 仅保留中立 DTO。Optimize job 以 StateStore 为唯一
durable truth；单 FE 恢复与未来多 FE lease/fence/takeover 分阶段决策，不以 SPI、service locator、双写或内存
fallback 模糊 owner 和故障语义。

- ADR-0083 — 表维护 execution port 为何必须随 query assembly 归 Frontend，不能以 Core cohort bridge 留存（active）
- ADR-0110 — lake publication 为何采用 crash-only outcome、target OCC 与年龄窗 GC（active）
- ADR-0111 — Frontend maintenance/statistics job 为何是 ProcessRuntime，而 GC first-observation 是 Accelerator（active）
- ADR-0057 — MV 维护事实为何按「是否需要 provider runtime IO」切成观测口投影与 SPI capability 两条通道（active）

#### 历史

- ADR-0009 — 表维护为何由 frontend 拥有 application/lifecycle，并通过 core domain port 调用 connector truth（superseded → ADR-0083）
- ADR-0035 — Connector orphan cleanup 为何使用 immutable manifest、逐 batch receipt 与 reconcile-only unknown（superseded → ADR-0110）
- ADR-0065 — 同一张表的维护为何以单个 per-table lease attempt 为唯一派发权威、并在同事务内校验 fence（superseded → ADR-0111）
- ADR-0067 — 收敛已死 generation 的维护为何是独立 provider capability，而不是放宽 exact-generation reconcile（superseded → ADR-0111）

### crate-boundary

领域哲学：架构隔离由 crate 依赖图强制——一个 crate 不能命名它没有依赖的 crate，
这条约束由编译器保证，不会因目录改名或模块搬迁而静默失效。当一条隔离约束只能靠扫描
源码形状表达时，正确的反应是把边界物理化成 crate，而不是写扫描器：扫描器描述的是当前
文件布局，crate 图描述的是依赖事实。迁移期的临时检查随迁移完成一并删除，不留作完成态
永久 guard。

- ADR-0058 — 架构隔离为何由 crate 边界强制，而不用硬编码的 source-shape guard（active）
- ADR-0069 — 共享测试机械能力为何使用零产品依赖的独立叶子 crate，而领域断言仍跟随其 owner（active）
- ADR-0071 — 分布式测试编排为何由唯一 cluster harness 拥有、SQL runner 只作 frontend adapter（active）
- ADR-0113 — Native wire 为何删除消息自证 digest、只保留跨消息引用与格式边界 fence（active）
- ADR-0094 — 空 catalog crate 为何在真实 owner 收敛后删除，而不保留 facade（active）
- ADR-0112 — native FE/BE role launch、management surface 与 ephemeral backend membership 为何保持同一启动路径（active）
- ADR-0119 — SQLite 为何是唯一 production StateStore、远程 provider 仅保留实验 leaf crate（active）

#### 历史

- ADR-0098 — native DTO 的字段路径与验证错误为何由 Protocol 独占、而不保留 Core family 门面（superseded → ADR-0106）
- ADR-0099 — 聚合 Core 退场后为何按真实 owner 物理切断依赖（superseded → ADR-0108）
- ADR-0106 — Native wire 分层、terminal content identity 与 Backend RF correctness owner（superseded → ADR-0113）
- ADR-0093 — StateStore provider 为何作为 leaf crate、Frontend 直接拥有 consumer runtime（superseded → ADR-0119）

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

- ADR-0088 — SQL、Iceberg 与 MV 的错误为何必须在各自 owner 内、跨域 fallback 前收敛（active）
- ADR-0095 — SQL analyze 错误为何以 typed code/span 穿过 Frontend、由 MySQL 边界映射（active）
