# Iceberg Operation Lifecycle Design

日期：2026-06-05

## 背景

NovaRocks 现在有两条相邻的 Iceberg 写入主线：

- Iceberg v3 IMV 的 Refresh lifecycle hardening（roadmap 13）
- Iceberg Distributed Write Pipeline 的 Commit Service 与 cancel/recovery semantics（IW-5 / IW-6）

这两条线看起来分别属于 MV refresh 和 distributed writer，但它们面对的是同一类问题：一次 Iceberg 操作进入 metadata commit 临界区前后，系统如何表达事实状态、如何区分 definite failure / commit unknown / known committed finalize failure、如何保留恢复证据、如何执行 cleanup，并且如何让用户和 scheduler 看到明确的 next action。

当前代码里已经有部分能力：

- `src/connector/iceberg/commit/run.rs` 统一调度 `CommitOpKind` 对应的 commit action，并在错误时用字符串分类 commit unknown。
- `src/engine/mv/iceberg_refresh.rs` 为 branch-staged MV refresh 记录 intent、staging commit、publish commit、finalize，并实现了 MV 专用 recovery。
- `src/meta/repository/mv.rs` 持久化 MV refresh state 和 active refresh guard。
- `src/runtime/write_coordinator.rs` 收齐 distributed writer final reports，并生成 `WriteCommitInput` / `WriteAbortInput`。
- PR #257 已经把 IMV refresh-time dispatch 从 legacy `RefreshStrategy` 枚举迁到 `RefreshCapabilities`，后续 lifecycle 不应该再围绕 shape/strategy 分支。

缺口是这些能力还没有收敛成一个共享的 operation lifecycle。MV refresh、INSERT/OVERWRITE、RowDelta、maintenance 等路径仍然各自解释 commit unknown、cleanup 和 recovery，后续支持 distributed write cutover 时会重复实现同一套失败语义。

## 目标

1. 设计一个共享的 Iceberg operation lifecycle，覆盖 MV refresh 和 distributed write 的共同事务语义。
2. 把 IW-5 的 commit service 作为 shared lifecycle 的底座之一纳入设计，而不是独立设计成一个只调 commit action 的 wrapper。
3. 让 13 和 IW-6 共享同一套状态机、typed commit outcome/error、recovery evidence 和 cleanup outcome。
4. 保持 MV refresh、distributed writer、DML 的领域逻辑分离：共享层只表达 Iceberg operation 的事实状态，不接管 SQL rewrite、writer 产物生成或 scheduler 决策。
5. 分阶段落地，先建立 typed commit service 和 lifecycle skeleton，再接入 MV refresh，最后接入 IW-6 writer lifecycle。

## 非目标

- 不启动 partition MV 相关任务。
- 不一次性迁移 DELETE / UPDATE / MERGE / RowDelta / MV refresh 的所有 writer 产物生成路径。
- 不把 MV scheduler 合并进 write coordinator。
- 不实现后台自动 recovery daemon。第一阶段只要求 startup recovery 和 manual inspect/recover 能有明确状态与证据。
- 不改变 Iceberg commit action 的表语义，例如 append、overwrite、row delta 的 Iceberg metadata 更新规则。

## 核心概念

### IcebergOperationIntent

一次可能产生 Iceberg metadata commit 的持久化操作意图。它是 shared lifecycle 的入口，而不是某个具体上层 statement 的私有状态。

第一版字段：

- `operation_id`
- `operation_kind`: `InsertAppend | InsertOverwrite | RowDelta | MvRefresh | Maintenance`
- `target`: catalog / namespace / table / ref
- `base_snapshot_id` 或 `base_snapshot_map`
- `attempt_id`
- `commit_strategy`
- `validation_policy`
- `cleanup_policy`
- `created_at_ms`

MV refresh 保留自己的 `refresh_id`，并通过 `operation_id` 关联 operation intent。当前 MV refresh 与 operation 是一对一，但保留两个 id 可以避免把 MV 领域事件和 Iceberg 写事务强行合并。未来 metadata-only refresh、partition refresh、rebuild catchup 可能需要更复杂关系。

### IcebergOperationState

统一事实状态机。它描述 Iceberg operation 事实，不描述 writer 内部 collection 状态，也不描述 MV logical rewrite 状态。

第一版状态：

- `Preparing`
- `Writing`
- `Collecting`
- `Committing`
- `Committed`
- `CommitUnknown`
- `Finalizing`
- `Finalized`
- `Aborting`
- `Aborted`
- `FailedKnownUncommitted`
- `FinalizeFailedKnownCommitted`

状态机允许 adapter 声明哪些 phase 适用。例如 standalone synchronous INSERT 可能不需要 `Collecting`；MV metadata-only refresh 可能没有 `Writing`；distributed write 必然有 `Collecting`。

### IcebergCommitService

执行唯一 metadata commit 的组件。它接收 typed request，选择 commit strategy，返回 typed outcome/error。

输入：

- target catalog/table/ref
- commit strategy
- base snapshot guard
- sequence guard
- written data/delete files
- stats/sketches
- snapshot properties
- validation policy
- cleanup policy
- recovery marker

输出：

- `CommitServiceOutcome::Committed { snapshot_id, commit_id, written_manifest_paths }`
- `CommitServiceError::KnownUncommitted { message, cleanup_attempted }`
- `CommitServiceError::Unknown { message, recovery_evidence }`

当前 `run_iceberg_commit` 不应该再通过字符串 `"iceberg commit unknown ("` 向外传播语义。commit unknown 的分类要收敛成 typed error，调用方只能基于 enum 决策。

### RecoveryEvidence

commit unknown 或 crash recovery 时用于判断事实状态的证据。

通用证据：

- target catalog/namespace/table/ref
- base snapshot guard
- attempt id
- commit strategy
- staged data/delete files
- manifest paths
- expected snapshot marker / snapshot property
- commit request summary

MV 特有证据：

- refresh id
- refresh snapshot marker token
- staging branch
- expected main snapshot id
- staging snapshot id
- published snapshot id
- base snapshot map

Writer 特有证据：

- write id
- writer final reports
- completed writer outputs
- incomplete writer keys
- client cancel / timeout reason

如果 recovery evidence 不足以证明 committed 或 uncommitted，operation 必须保持 `CommitUnknown`，并给出 `next_action=ManualInspect`，不能自动清理 staged artifacts。

### OperationAdapter

共享层不直接理解 MV rewrite 或 distributed execution。各领域通过 adapter 接入 lifecycle。

MV adapter 负责：

- 创建 refresh intent 并关联 operation id。
- 提供 refresh marker、base snapshot map、staging branch。
- 在 operation `Committed` 后执行 MV metadata finalize。
- 为 scheduler / SHOW / diagnostic 映射 refresh state 和 operation state。

Writer adapter 负责：

- 从 `WriteCoordinator` 取得 `WriteCommitInput` / `WriteAbortInput`。
- 将 writer failure、timeout、client disconnect 映射成 pre-commit abort。
- 将 writer outputs 转成 commit service request。
- 持久化 writer recovery evidence。

DML adapter 后续负责：

- DELETE / UPDATE / MERGE / RowDelta writer outputs。
- RowDelta validation policy。
- DML-specific cleanup diagnostics。

## 状态机语义

主路径：

```text
Preparing
  -> Writing
  -> Collecting
  -> Committing
  -> Committed
  -> Finalizing
  -> Finalized
```

失败路径：

```text
Preparing/Writing/Collecting
  -> Aborting
  -> Aborted

Preparing/Writing/Collecting
  -> FailedKnownUncommitted

Committing
  -> Committed
  -> CommitUnknown
  -> FailedKnownUncommitted

Finalizing
  -> Finalized
  -> FinalizeFailedKnownCommitted
  -> CommitUnknown
```

关键规则：

1. Commit 临界区之前，cancel、timeout、writer failure 都是 known-uncommitted。可以进入 `Aborting` 清理 staged files / staging branch，清理完成后进入 `Aborted`；如果没有可清理产物或清理失败后需要保留失败事实，则进入 `FailedKnownUncommitted` 并记录 cleanup outcome。cleanup failure 不能覆盖主错误事实。
2. Commit 临界区之中，网络错误、catalog unexpected、process crash 不得被简化成 failed。进入 `CommitUnknown` 后必须保留 recovery evidence，不能直接删除 staged artifacts，也不能清除 MV active refresh。
3. Commit 已确认之后，metadata finalize、cache invalidation、scheduler bookkeeping 失败属于 known-committed。用户错误信息必须明确 Iceberg commit 已成功但 finalize/recovery required。
4. Cleanup 是 best effort outcome。cleanup 成功或失败都不改变 `Committed`、`CommitUnknown`、`FailedKnownUncommitted` 这些事实状态。
5. Recovery 只基于证据判断。没有证据时保持 `CommitUnknown`。

## 持久化模型

新增通用 operation record，不把 shared lifecycle 塞进 MV repository。

第一版 record：

- `operation_id`
- `operation_kind`
- `target`
- `state`
- `attempt_id`
- `base_snapshot_id`
- `base_snapshot_map`
- `staged_artifacts`
- `commit_request`
- `commit_outcome`
- `cleanup_outcome`
- `failure`
- `created_at_ms`
- `updated_at_ms`
- `finished_at_ms`

MV repository 保留 MV 领域字段：

- `mv_id`
- `refresh_id`
- `active_refresh_id`
- refresh base snapshots
- refresh rows
- target snapshot
- scheduler state
- `operation_id`

迁移策略：

1. 已经 `Finalized` / `Aborted` 的旧 refresh 不强制回填 operation record。
2. Active / non-final refresh 在 migration 或 startup recovery 时合成 operation record。
3. 旧 refresh 如果处于 commit unknown 且证据不足，迁移为 `CommitUnknown` + `next_action=ManualInspect`。
4. `SHOW MATERIALIZED VIEWS` 继续展示 MV 状态；diagnostic 命令展示 operation id、operation state、failure kind、next action。

## 与当前代码的映射

### `src/connector/iceberg/commit/run.rs`

当前 `run_iceberg_commit` 负责选择 `CommitOpKind` 并在失败时按字符串判断 commit unknown。设计后它会成为 `IcebergCommitService` 的内部实现或第一版 facade：

- 输入从 `RunInput` 演进为 typed `CommitServiceRequest`。
- 错误从 `String` 演进为 `CommitServiceError`。
- commit unknown 分类从字符串包含判断变成 typed classifier。
- cleanup outcome 明确返回给 lifecycle。

### `src/engine/mv/iceberg_refresh.rs`

当前 MV refresh 自己负责 begin intent、record staging commit、publish、finalize、reconcile。设计后：

- MV adapter 创建 `IcebergOperationIntent`。
- staging commit / publish commit 通过 operation lifecycle 记录。
- `reconcile_iceberg_mv_refresh` 的事实判断下沉到 shared recovery evaluator。
- MV 文件保留 rewrite、base snapshot pin、target apply、refresh summary 等 MV 领域逻辑。

### `src/meta/repository/mv.rs`

当前 MV repo 同时表达 MV definition 状态和 refresh operation 状态。设计后：

- MV repo 保留 MV definition、refresh summary、scheduler fields。
- active refresh 通过 `operation_id` 指向 shared operation record。
- `clear_refresh_progress` 不允许绕过 operation state 清理 commit unknown。

### `src/runtime/write_coordinator.rs`

当前 `WriteCoordinator` 的 `Pending/Running/Finished/Failed/Canceled` 是 writer collection 内部状态。设计后：

- 它不替代 `IcebergOperationState`。
- `commit_input()` 成功后由 writer adapter 推进 operation 到 `Committing`。
- `abort_input()` 由 writer adapter 推进 operation 到 `Aborting` / `FailedKnownUncommitted`。

## 分阶段实现

### Phase 1: typed commit service

目标：建立 IW-5 的共享底座。

工作：

- 新增 `IcebergCommitService` 或等价模块。
- 定义 `CommitStrategy`、`CommitServiceRequest`、`CommitServiceOutcome`、`CommitServiceError`。
- 将 `run_iceberg_commit` 的字符串错误升级为 typed error。
- 现有 INSERT、DELETE、UPDATE、MV refresh commit 调用先通过 facade 迁移，保持行为不变。

验收：

- commit unknown 不再依赖字符串 `"iceberg commit unknown ("`。
- 单测覆盖 known-uncommitted / unknown 分类。
- cleanup failure 被记录，但不覆盖主错误类型。

### Phase 2: shared operation lifecycle skeleton

目标：建立状态机和持久化骨架。

工作：

- 新增 operation record repository。
- 新增 state transition helper。
- 新增 recovery evidence 数据结构。
- 给 commit service outcome/error 到 operation state 的映射加单测。

验收：

- 单测覆盖合法/非法状态转换。
- `CommitUnknown` 不允许直接进入 `Aborted`。
- `Committed` 后 finalize failure 进入 known-committed failure。

### Phase 3: MV refresh 接入 shared lifecycle

目标：完成 roadmap 13 的核心 lifecycle hardening。

工作：

- MV refresh intent 关联 operation id。
- Branch-staged refresh 的 staging commit、publish、finalize 通过 lifecycle 记录。
- MV recovery 迁到 shared recovery evaluator。
- SHOW / diagnostic 输出 operation state、target snapshot、base snapshot map、failure reason。

验收：

- MV refresh 任意阶段 crash 后可以恢复或明确拒绝继续。
- commit unknown 不会清掉 active refresh。
- stale refresh 不会 silently overwrite active target state。
- 新增 MV recovery SQL/集成 case。

### Phase 4: IW-6 writer lifecycle 接入

目标：让 distributed writer 使用同一套 failure semantics。

工作：

- `WriteCommitInput` 创建 operation commit request。
- writer failure / timeout / cancel 映射为 pre-commit abort。
- commit unknown 持久化 operation record 和 writer recovery evidence。
- cleanup outcome 进入 logs / metrics / diagnostic。

验收：

- writer operation 的 cancel / timeout / failure 状态转换有单测。
- commit unknown 不被误报为 definite failure。
- cleanup failure 有日志和 metrics。
- IW-7 / IW-8 cutover 前可通过 harness 验证 writer output -> coordinator -> commit service -> lifecycle 闭环。

## 测试策略

Unit tests:

- Commit error classifier：known-uncommitted、unknown、known-committed finalize failure。
- Operation state transitions：合法转移、非法转移、idempotent replay。
- Recovery evaluator：MV marker match、staging branch exists/missing、main ref changed、evidence insufficient。
- Cleanup outcome：cleanup success/failure 不覆盖主状态。
- Writer adapter：writer failure、missing final report、duplicate final report、cancel after finished writers。

SQL / integration tests:

- Iceberg MV branch-staged refresh recovery。
- Commit unknown guard：active refresh 不能被 scheduler 继续刷新。
- Metadata finalize failure 诊断输出。
- 后续 IW-6 writer harness：timeout/cancel 前后表可见性一致。

Migration tests:

- Old finalized refresh 不强制创建 operation record。
- Old active refresh 能合成 operation record。
- Old commit-unknown refresh 证据不足时进入 manual inspect。

## 观测与诊断

每个 operation 至少要能看到：

- operation id / operation kind
- target table/ref
- state
- attempt id
- writer count / completed writer count（writer operation）
- refresh id / mv id（MV operation）
- commit strategy
- commit outcome snapshot id
- cleanup outcome
- failure kind / message / next action

`SHOW MATERIALIZED VIEWS` 保持面向 MV 的摘要；更细的 operation state 通过 diagnostic 命令或 dedicated system table 暴露。第一版可以先在 logs 和 existing SHOW 扩展字段中落地，不要求完整产品化 UI。

## 已确认的设计决策

- IW-5 纳入本 spec，一起设计 shared lifecycle 和 commit service。
- 采用 shared operation lifecycle + adapter 设计。
- `refresh_id` 和 `operation_id` 分离。
- Phase 1 先做 typed commit service，Phase 2 做 lifecycle skeleton，Phase 3 接入 MV refresh，Phase 4 接入 IW-6 writer lifecycle。
- 不启动 partition MV。
- 不实现后台自动 recovery daemon。

## 风险

1. Scope 过大：通过 phase 切分控制。Phase 1 / Phase 2 可以独立提交，不要求立即完成 MV/writer 全量迁移。
2. 双状态源冲突：MV repo 和 operation repo 必须明确 ownership。operation repo owns fact state，MV repo owns MV summary/scheduler state。
3. Recovery 误判：所有 automatic recovery 必须基于 marker / snapshot / ref 证据。证据不足时保持 `CommitUnknown`。
4. 兼容旧 refresh：migration 不能自动清理 old active refresh，尤其不能清理 commit unknown。
5. Error type 迁移影响面广：先通过 facade 兼容现有 `String` 调用点，再逐步改调用方。
