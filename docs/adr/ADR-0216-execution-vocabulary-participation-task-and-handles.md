---
id: ADR-0216
title: "Execution vocabulary: participation, task, and handle names"
domain: [execution, memory]
status: active
supersedes: []
superseded-by: null
date: 2026-11-03
provenance:
  - "discussion: 2026-11-02 vocabulary acceptance; 2026-11-03 parallel with TRC-3"
code-anchors: []
---

## 问题

同一批英文名字同时指协议记录、查询资源、计划片段和调度单元。后续内存账本如果沿这些名字接线，会把不同寿命的对象记成同一个账户。

## 选项

1. 继续沿用 fragment、context、lease、role 的现有混用。
2. 按 Trino 的查询级对象改名，把参与也叫 Query。
3. 冻结一套执行词汇：协议记录叫 Participation，计划片段仍叫 fragment，协议实例叫 Task，限时授予才叫 Lease，义务和看管按 Drop 契约命名。

## 裁决

采用选项 3。

定稿名字：

- `ParticipationRef` 是一台 BE 参与一次查询尝试的协议记录。Worker 侧是 `ParticipationActor` 与 `ParticipationCore`，FE 侧是 `ParticipationOwner`。
- `QueryContext` 仍是这台 BE 上、这一次尝试的查询资源上下文，不改名。
- `TaskIdentity` 是协议 task 的规范身份。stage 内序号仍叫 `TaskId`。内核、exchange 与 runtime filter 用的紧凑键是 `TaskKey`，字段名 `task_key`。它与 `TaskIdentity` 在同一次尝试内一一对应，但不由它派生。`TaskKey` 是 SHA-256 的前 16 字节，输入是域分隔、query id、attempt id、fragment id 和 instance index，不含 backend process id。结果若是全零，把低 64 位改成 1。
- `TaskContext` 是一个 task 内 driver 与算子共享的运行时上下文。`RuntimeState` 与它谁拥有身份和内存绑定，不在这次改名里决定。
- plan fragment 与 commit fragment 保持 fragment。
- 在 execution、worker、control-runtime 里，driver 只指 pipeline driver。调度单元叫 `DriverRunner`。
- `WorkObligation` 是受检工作义务。`ReservedInputObligation` 与 `StandingInputObligation` 是未受检输入义务。`ExitCustody` 是共享退出看管，`ExitObligation` 是唯一的退出义务。
- Lease 只用于按时间结束、可续约的授予。`InstalledLease` 与 `ParticipationLeaseGrant` 是参与租约。
- Handle 只用于惰性引用。共享 RAII 用它拥有的东西命名。两类义务都以 Obligation 结尾。
- Role 不用于义务。FE/BE 角色、交互角色和数据模型里的结构角色保留 Role。
- Task 只指协议 task。其余工作项叫 Step，异步作业叫 Job。
- domain 仍指协议里一块独立版本化的共享状态。runtime filter 的值集合写作 value domain。

与 Trino、Impala 的四处偏离：

1. `QueryContext` 的作用域是一次尝试。Trino 的 `QueryContext` 覆盖整个查询。
2. `TaskId` 是 stage 内序号，相当于 Trino 的 partitionId。规范身份是 `TaskIdentity`，不是 `TaskId`。
3. Lease 必须会到期。Trino 的 `NodeLease` 与 `LookupSourceLease` 不会到期，那些对象在这里不叫 Lease。
4. domain 不是 Trino 的值域。值域写成 value domain。

重复身份只记录、不合并：`BackendParticipantIdentity` 与 `ParticipationRef` 指同一个参与；`TaskKey` 与 `TaskIdentity` 一一对应但不是派生关系。

历史别名按概念记，不复制全量清单：协议 Context 改为 Participation；fragment instance 改为 Task；`WorkLease` 改为 `WorkObligation`；`ExitRole` 改为 `ExitCustody`；`ExitAuthority` 改为 `ExitObligation`。

旧文档里的叫法按同一张表读，不改那些 ADR 正文：

- ADR-0135 的 participant 状态机没有被选为生命周期权威；现在的协议记录叫 Participation。`InitQuery`、`UpdateQueryContext`、`ReleaseQueryContext` 改为 Participation 上的操作。fragment instance 与 finst 改为 Task 与 `task_key`。
- ADR-0126、ADR-0127 的 `ParticipantAttemptRef` 是当时 post-Init 的身份。它不改写成 `ParticipationRef`；当前协议记录是 `ParticipationRef`。
- ADR-0146 的 query execution lease 是参与租约，Context 的显式 `ReleaseQueryContext` 改为 `ReleaseParticipation`。
- ADR-0159 的 driver 仍是 pipeline driver，不改。

## 接受的妥协

改名不改变对象布局、协议语义和 Drop 行为。`RuntimeState` 与 `TaskContext` 的归属另案决定。两个已经没有读取方的测试环境变量保持原名，避免旧名逃出拒绝列表。TRC-3 仍会删掉的连接层名字这次不改；后合入的一方 rebase。

## 何时重新评估

出现第二种协议记录、第二种 task 身份，或 Lease 被用于不会到期的对象时，重新开一条 ADR，不在调用点另起别名。
