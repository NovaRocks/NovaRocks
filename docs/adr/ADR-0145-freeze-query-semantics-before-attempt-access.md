---
id: ADR-0145
title: "Freeze query semantics before acquiring per-attempt execution access"
domain: [sql-compiler, provider-spi, distributed-query-lifecycle]
status: active
supersedes: [ADR-0129]
superseded-by: null
date: 2026-09-11
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-11 frozen query semantics and per-attempt access"
code-anchors:
  - "novarocks/query-application/src/preparation/description.rs (FrozenExecutionDescription)"
  - "novarocks/query-application/src/preparation/driver.rs (PreparationDriver)"
  - "novarocks/frontend/src/query_execution/completion.rs (PreparedLogicalRead)"
  - "novarocks/frontend/src/query_execution/native_execution_adapter.rs (FrontendNativeLogicalReadLauncher)"
  - "novarocks/frontend/src/query_execution/attempt_initialization.rs (attempt access materialization)"
  - "novarocks/spi/src/connector/binding/role.rs (ConnectorReadAttemptAccess)"
---

## 问题

查询准备得到的语义事实与每次执行 attempt 所需的拓扑、split source 和短期凭据，应在什么边界冻结和取得，才能让失败 attempt 重启而不重新规划，也不把秘密或可变资源放进可复用计划？

## 背景与执行事实

一次逻辑查询与一次执行 attempt 不是同一个生命周期。逻辑查询拥有 SQL 语义、准确对象绑定、Connector 协商结论、输出契约和恢复政策；attempt 才拥有 `QueryExecutionId`、placement、Task identity、运行凭据、split source、Runtime Filter deployment 与 transport owner。拓扑变化或 Worker 失败会替换 attempt，但不自动改变用户请求和已经证明的输入绑定。

准备并非“先做完全部 I/O，再调用一次纯 compiler”的单向流水线。谓词、投影和 Limit 只有在形成候选计划后才能与 Connector 协商；Connector 的回答又决定 residual predicate 和最终计划。观察、纯编译与协商因此可以在同一有界准备作用域内交替，直到语义稳定或预算耗尽。最终 `FrozenExecutionDescription` 只保存不可变且可验证的语义结果，不保存 endpoint、attempt identity、credential、lease、split manager 或可执行 I/O callback。

| 生命周期 | 允许持有 | 不允许持有 |
|---|---|---|
| 查询观察与准备 | exact object binding、occurrence、统计和 MV 候选事实、协商 request/receipt、FE metadata principal | Worker placement、attempt credential、Task identity、运行 split source |
| 冻结逻辑描述 | sealed plan、输出契约、恢复模式、资源需求、准确 binding/provenance、静态 attempt recipe | secret、Catalog lease、endpoint、运行时 owner、重新协商 callback |
| attempt 实例化 | 本 attempt 的 topology、admission evidence、data credential、split source、Task/RF/Exchange manifest | 改写已冻结输出、残余谓词、对象版本或恢复承诺 |

FE 为 metadata、manifest 和统计读取使用进程自己的 metadata principal；BE 的数据访问能力按 attempt 单独取得。不同 Catalog 的能力形状不同：静态 data credential 可引用进程本地受控对象；支持独立 credentials endpoint 的 Catalog 按 attempt 签发；只能通过 `load_table` 返回 delegation 的 Catalog 在 attempt 实例化时重新调用该端点，但只消费 delegation，并校验 table UUID、metadata location 和授权资源覆盖冻结绑定，忽略它附带的 current snapshot。无法形成上述闭合证明时 fail closed。

`PreparedLogicalRead` 将 frozen description、静态 Native template 与解析后的执行选项作为一个 move-only carrier 交给 launcher。launcher 在创建 `QueryExecutionRequest` 前验证 plan seal 与 template/request seal，防止把一个查询的语义描述与另一个查询的可执行模板拼接。每个 attempt 再从同一模板生成自己的运行 owner；replacement factory 无法命名 compiler、候选发现或 Connector negotiation。

ADR-0129 曾通过“在 metadata materialization 前先 mint candidate attempt”解决 `load_table` 同时返回 metadata 与 vended credential 的问题。独立 FE metadata principal 和 per-attempt data access 取消了这个前提：准备不再需要借用未来 attempt 的数据凭据。它关于秘密不进入 plan/cache、作用域精确、confidential transport、失败清理和不猜测 credential 的要求继续有效，并在本 ADR 中完整重述。

## 考虑过的选项

**选项一：准备前创建 candidate attempt，并把 metadata response 中的凭据直接收集给它。** 这能保持一次 `load_table` response 的 metadata/credential 配对，也是 ADR-0129 的选择。但准备失败会消耗 attempt 身份，重试拓扑会触发重新规划，metadata principal 与 data principal 被迫相同。独立 FE metadata access 已使该耦合不再必要，因此此选项被本 ADR **设计否决**。

**选项二：把 vended credential、table runtime 或 split manager保存在 plan/cache 中。** 它减少 attempt 启动请求，但把短期 capability 扩展为可跨查询、跨 attempt 或可观察对象，且无法由 plan digest安全表达。由于违反秘密隔离和生命周期精确性，这是**设计否决**。

**选项三：每个 attempt 重新执行 analyze、optimize、候选发现和 Connector 协商。** 实现可复用现有 compiler 入口，但 topology/credential 故障会重新观察 Current，可能改变输入、MV 选择、residual predicate 或输出语义；业务层也无法知道“重试”是否仍是同一逻辑执行。这是**设计否决**。

**选项四：冻结语义描述和 access recipe，每个 attempt 只实例化运行能力。** 准备有界交替到稳定后一次冻结；attempt 用准确 recipe 获取 topology、credentials 和 split source，并验证它们仍覆盖冻结事实。采纳。

**选项五：把所有结果完整物化后才允许执行 owner 取得描述。** 它能简化 handoff，却把准备延迟和存储成本强加给短查询，也不能替代 attempt access 的秘密生命周期。这是**成本否决**；需要可恢复 exchange 或结果缓存时应作为独立执行策略重新评估。

## 裁决

1. **Semantic-freeze rule。** 观察、编译与 Connector planning negotiation 可以在共享预算内有界交替；只有输出契约、执行保证、residual responsibility、exact binding 和资源需求全部确定后才能生成 `FrozenExecutionDescription`。
2. **Topology-free rule。** 冻结描述和静态 Native template 不包含 endpoint、backend count、placement、Task 数量映射或 attempt identity。它们不能通过 callback 重新观察 topology。
3. **Resource-free preparation rule。** 准备不持有运行 credential、Catalog lease、split source、attempt owner 或执行资源 reservation。FE metadata I/O 只使用明确的 FE metadata principal。
4. **Per-attempt access rule。** 每个 attempt 根据冻结 recipe 独立取得 data credential、split source、admission evidence、Task/RF/Exchange identity 与 transport owner；失败或取消由该 attempt owner 精确释放。
5. **No-renegotiation-on-retry rule。** replacement 不回调 compiler、Catalog observation、MV candidate discovery、statistics provider 或 Connector semantic negotiation，也不改变 binding、residual predicate、输出 schema和恢复政策。
6. **Capability-matrix rule。** 静态 credential、credentials endpoint 和 delegation-only `load_table` 是封闭的支持形状；后者必须校验 object identity、metadata location 和资源覆盖并忽略新 snapshot。无法证明时 fail closed，不退化为重新规划或复用 metadata credential。
7. **Secret-isolation rule。** secret 不进入 frozen description、plan、digest、cache、log 或公开 handle；只在受控 attempt owner 与要求 confidentiality 的 Native carrier 中移动。planning error、cancel、attempt terminal 和 owner Drop 必须清理 material。
8. **Atomic-pairing rule。** description、static template 和 resolved options 由一个 move-only carrier 原子交接；launcher 必须验证相同 plan/request seal 后才能构造执行请求，调用者不能拆开重组。

本 ADR 完整替代 ADR-0129。被替代的是“为了收集 vended credential，必须在 metadata materialization 前创建 attempt”的时序；继续保留并加强的是秘密隔离、准确授权范围、confidential transport、失败清理和无法证明时拒绝。

## 接受的妥协（诚实记录）

每个 attempt 可能多一次 credentials 或 delegation 请求，也可能重新创建 split source。我们接受额外 Catalog 往返和首行延迟，以换取准备与 attempt 生命周期分离；不通过缓存 secret 或重做语义协商隐藏这笔成本。

delegation-only `load_table` 会返回一份当前 metadata，但 attempt 只读取 credential 部分。实现必须额外校验 UUID、metadata location 与授权资源覆盖，且明确忽略 snapshot；这比独立 credentials endpoint 更复杂，也受外部 Catalog response 形状约束。

冻结语义不表示所有物理决策都固定。placement、Task/driver 数、bucket 到 Worker 的映射和运行期 split 批次可以随 attempt 改变，只要不改变 sealed plan 的语义责任。这要求 reviewer 区分 semantic plan 与 runtime schedule，边界比“一份最终计划包办全部”更精细。

当前 carrier 依靠 move-only ownership、私有构造和 seals，而不是可跨进程持久化的逻辑执行格式。FE 进程退出后不能从磁盘恢复该 description；本裁决只支持同一 FE 进程内的 attempt replacement。

## 何时重新评估

1. Catalog 提供原子 metadata token，可在不重新观察 Current 的前提下由独立 credentials endpoint 换取数据能力时，可简化 delegation-only 校验，但仍不得把 secret 放入 frozen description。
2. 多 FE 接管或 FE 重启恢复成为产品要求时，需要为 frozen description、access recipe 和 seal 定义持久格式、版本兼容与新 owner fencing；当前进程内 carrier 不足。
3. 每 attempt 的 credential/split-source 建立在生产 profile 中成为可测瓶颈时，评估绑定 token、批量签发或安全复用；优化不能重新耦合 compiler 或扩大 secret 生命周期。
4. Connector negotiation 需要依赖运行期 Worker 能力且无法提前冻结时，重新裁决 semantic negotiation 与 placement negotiation 的边界，不能把隐式 callback 塞进 attempt factory。
5. 外部效果查询需要在 attempt 间复用 access 时，必须先有产物隔离、提交证明和业务恢复策略；本 ADR 不授予写入透明重试。
