# 架构决策记录（ADR）

本目录是 NovaRocks 的架构决策知识库：每条 ADR 固化一个**已裁决的设计问题**——背景、考虑过的选项、裁决、**接受的妥协**与**何时重新评估**。检索单位是「架构师会问的问题」，不是「当年做过的任务」。改动某子系统的架构级行为前，先在下方领域索引里找到它。

**怎么写**：用仓库内置的 `/adr` skill（`.agents/skills/adr/`，Codex 直接发现、Claude Code 经 `.claude/skills/adr` 软链发现）新建、supersede 或重编号——skill 内嵌模板与全部规则；人工手写请复制下方模板。正文中文；id/slug/tags/frontmatter 键用英文。

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

### distributed-query-lifecycle

领域哲学：FE coordinator 拥有全局编排，BE query lifecycle 拥有本地执行与资源；两者是独立进程、故障域与状态机，
只通过版本化 wire protocol 交换事实。共享面仅限 immutable wire/value contract、codec 与 pure validation，
不得以 all-in-one 便利、feature-specific flow 或共享 runtime state 绕过边界。协议必须正面处理重复、延迟、乱序、
丢失、过期 ownership 与进程失败，使分布式生命周期可测试、可观察并可独立演进。

- ADR-0007 — FE 全局协调与 BE 本地查询生命周期为何保持进程和状态机分离（active）
- ADR-0008 — 分布式查询为何使用 Init/Stage/Start 三阶段启动（active）
- ADR-0010 — 显式 query cancellation surface 为何以 MySQL KILL QUERY 和 frontend session owner 实现（active）
- ADR-0011 — 请求执行为何使用 immutable context、一次 topology capture 并拒绝 ambient fallback（active）
- ADR-0012 — Query session admission 与 router 为何由 frontend 拥有、core 只保留 wire/compiler kernel（active）

### cluster-membership

领域哲学：backend membership 的 durable desired state 与 heartbeat/live/generation 等运行期 observation 必须分离。
frontend 的 `ClusterBackendService` 通过 StateStore 成为唯一 membership authority；core 只消费稳定的
`BackendTopologyPort`，不保留 metadata bridge、global registry 或内存 durable fallback。配置的 backend 是 additive
seeds，动态 ADD/DROP 的结果跨 FE 重启恢复；单 FE writer 与未来多 FE fencing/takeover 分阶段裁决。

- ADR-0013 — backend membership 为何由 frontend StateStore 单独持久化（active）

### frontend-dml

领域哲学：frontend 拥有 DML 的 statement application flow、durable operation lifecycle 与 production routing；
core 只通过一对一 typed engine port 保留 query、connector 和 external commit truth。每次写入必须复用 admission
冻结的 immutable request identity；跨 crate 只传中立 DTO 与 opaque handles，不以 service locator、core callback、
metadata fallback 或公共 SPI 模糊 owner。

- ADR-0017 — INSERT application flow 为何由 frontend 拥有、core 只保留过渡性 typed engine port（active）

### table-maintenance

领域哲学：表维护的 application/lifecycle 由 frontend host 统一拥有，core 只提供一对一、consumer-owned 的 typed
engine port，connector 保留 catalog、snapshot、file 与 commit 等 external-system truth。Optimize job 以 StateStore
为唯一 durable truth；单 FE 恢复与未来多 FE lease/fence/takeover 分阶段决策，不以 SPI、service locator、双写或
内存 fallback 模糊 owner 和故障语义。

- ADR-0009 — 表维护为何由 frontend 拥有 application/lifecycle，并通过 core domain port 调用 connector truth（active）
