# Unified IVM and RowDelta Write Lifecycle 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

NovaRocks 已经有 distributed write coordinator、operation lifecycle、Iceberg commit
collector 等基础，但 append、overwrite、RowDelta、MV refresh 仍然容易在各自 flow 中
保存局部逻辑。短期为了让 case 走通，某些路径会直接写 data/delete files，再把结果
注入 collector；distributed writer lifecycle 则是另一条线。

用户已经明确不需要保留旧路径 fallback。长期设计应以一套 engine-owned write transaction
runner 收束所有用户级 Iceberg 写入，包括 IVM refresh 和 RowDelta-family DML。

## 2. 目标

- append、insert overwrite、RowDelta、DELETE、UPDATE、MERGE、MV refresh 共用同一
  write lifecycle。
- runtime coordinator 只负责 writer lifecycle 和 report 收集，不拥有 Iceberg metadata。
- engine 层负责 transaction spec、commit policy、operation record、post-commit finalization。
- 每个 flow 切换时不提供回旧路径的 session/config fallback。
- commit/abort/recovery 状态机统一。

## 3. 非目标

- 不在本 spec 中实现 BE crash retry 或 exactly-once reschedule。
- 不把 optimizer rewrite、MV delta derivation 和 write transaction 混成一个模块。
- 不改变 Iceberg snapshot commit 的 correctness policy。
- 不要求一次 PR 切换所有 flow。

## 4. 架构

```text
SQL flow / MV refresh planner
  -> IcebergWriteTransactionSpec
  -> IcebergWriteTransactionRunner
      -> create operation record
      -> coordinate writer fragments
      -> collect WriteCommitInput / WriteAbortInput
      -> build commit collector input
      -> run typed commit service
      -> finalize cache/dictionary/lifecycle
```

`IcebergWriteTransactionSpec` 表达：

- target catalog/db/table/ref。
- operation kind：append、overwrite、overwrite partitions、row delta、rewrite、MV refresh。
- base snapshot guard、sequence guard、schema/spec snapshot。
- source plan 或 mutation plan。
- writer sink mode。
- commit action type。
- finalization policy。

## 5. 组件边界

### Runtime coordinator

保留职责：

- 注册 expected writers。
- 接收 writer final report。
- 生成 `WriteCommitInput` / `WriteAbortInput`。
- 任一 writer 失败时 cancel 其它 fragment。

禁止职责：

- 读取 Iceberg catalog。
- 创建 operation lifecycle record。
- 调用 commit service。
- 做 cache invalidation。

### Engine transaction runner

新增或扩展职责：

- 从 SQL flow 接收 write spec。
- 创建 operation record。
- 调用 distributed execution。
- 将 writer descriptor 注入 commit collector。
- 调用 typed commit service。
- 统一处理 commit unknown、known uncommitted、finalize failure。

### SQL flow / MV flow

保留职责：

- 语义分析和 flow-specific validation。
- 构造 source/mutation plan。
- 构造 write spec。

迁出职责：

- 直接提交 Iceberg metadata。
- 直接维护 operation lifecycle。
- flow-local cache invalidation。

## 6. IVM / RowDelta 特殊要求

### IVM refresh

MV refresh 只提供：

- source delta plan。
- target layout。
- refresh operation id。
- snapshot/ref guard。

它不拥有自己的 writer lifecycle。refresh 成功只在 write transaction finalized 后更新 MV
metadata 状态。

### RowDelta-family DML

DELETE/UPDATE/MERGE 可能产生 data files、position delete files、equality delete files、
DV/Puffin files。runner 不理解 SQL match 语义，但必须承载多个 file content type 的
descriptor，并把它们传给对应 commit action。

### Empty input

empty input 仍创建 operation record，覆盖 writing 阶段恢复语义。writer 产出为零时，
runner 根据 operation kind 决定是否提交空 snapshot 或直接 finalized no-op。

## 7. 状态机

```text
Preparing
  -> Writing
  -> Collecting
  -> Committing
  -> Committed
  -> Finalizing
  -> Finalized

Failure terminals:
  FailedKnownUncommitted
  CommitUnknown
  FinalizeFailedKnownCommitted
  Aborted
```

`Committed` 是 metadata 已提交但 finalization 未完成的中间态。用户成功返回以
`Finalized` 为准。

## 8. 落地顺序

1. 保持现有 PR #295 后 coordinator outcome，补齐 writer metadata descriptor。
2. 引入 runner skeleton，先接 append。
3. 接 insert overwrite / overwrite partitions。
4. 接 RowDelta DELETE。
5. 接 UPDATE / MERGE。
6. 接 MV refresh。
7. 删除旧默认入口或降级为内部 helper。

每个 flow 的切换 PR 都必须在同一 PR 内移除该 flow 的 fallback。

## 9. 验证

- Unit tests 覆盖状态机转换和 error classification。
- Integration tests 覆盖 append/overwrite/delete/update/merge/MV refresh 的 commit/abort。
- Fault tests 覆盖 writer final error、missing writer、duplicate final report、commit unknown。
- SQL suites 覆盖 `iceberg-dml`、`iceberg-ivm`、`iceberg-rest` 1FE+3BE。

## 10. 成功标准

- 用户级 Iceberg 写入只有一条默认 lifecycle。
- MV refresh 与 RowDelta 不绕过 writer coordinator。
- 每个 commit failure 都能落到明确 operation terminal state。
- 不存在 session/config fallback 回旧同步 writer。
