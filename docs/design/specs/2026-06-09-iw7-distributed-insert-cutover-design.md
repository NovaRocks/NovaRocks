# IW-7: INSERT INTO Iceberg Distributed Sink Cutover

## 状态

- 日期：2026-06-09
- 目标：完成 IW-7，直接把 standalone `INSERT INTO <iceberg> SELECT/VALUES`
  从 coordinator 本地同步写切到 distributed async sink + write coordinator +
  Append commit。
- 参考：PR #275 提供了 early stage 参考，但本设计不沿用它的
  optimizer-first 边界。

## 背景

IW-1 到 IW-6 已经把 async sink contract、Iceberg staged writer kernel、
distributed write coordinator、typed commit service、write transaction runner
和 operation lifecycle 建起来。当前缺口是用户级 `INSERT INTO` append 仍
在 coordinator 进程内执行：

1. SELECT 先执行并收集为 chunks。
2. coordinator 进程本地写 Iceberg data files。
3. 用 `synthetic_write_commit_input()` 构造单 writer 结果。
4. 再交给 commit service。

这条路径不是 IW-7 要求的 distributed write pipeline。IW-7 的完成标准是：
多 BE sink fragment 写 staged files，FE/coordinator 收齐真实 writer report，
然后只做一次 Iceberg Append metadata commit。

## 设计结论

本轮采用 **engine-owned transaction, codegen-owned sink contract**。

长期终态上，INSERT 应该进入 optimizer：这样 DML sink 的 distribution、
writer parallelism、EXPLAIN、cost/rule 优化都能统一表达。但当前 IW-7 不把
完整 INSERT AST 纳入 optimizer，因为那会把 DML planner 重构和 append cutover
叠在一起。

过渡方案是：engine 继续负责 INSERT 语义和事务边界；codegen 接收一个清晰的
`IcebergWriteSinkSpec`，把 SELECT physical plan 编成带 `ICEBERG_TABLE_SINK`
的 coordinated write plan。未来如果实现 optimizer-owned INSERT，只需要让
logical/physical INSERT sink 产出同一个 sink spec，后续 codegen/coordinator/
commit 路径不需要重写。

## 范围

包含：

- `INSERT INTO <iceberg> SELECT ...`
- `INSERT INTO <iceberg> VALUES ...`
- branch/ref INSERT INTO 的既有 format-v3 校验
- 分区表和非分区表
- all-in-one 和 cross-process 1FE+2BE
- real `WriteCommitInput` 到 `CommitStrategy::Append`

不包含：

- `INSERT OVERWRITE`，留给 IW-8。
- DELETE / UPDATE / MERGE / MV refresh distributed cutover，留给 IW-9。
- 完整 DML optimizer 重构。
- 小文件合并或 compaction 策略优化。
- 用 flag 保留旧 append 路径。

## Direct Cutover

IW-7 直接切换 `INSERT INTO` append，不保留旧同步 append 回退路径。

实现完成后：

- `INSERT INTO` 不再走 `run_select_to_chunks` 后本地写 data files。
- `INSERT INTO` 不再为 append 使用 `synthetic_write_commit_input()`。
- 旧 append 专用本地写代码应删除或收窄到仍被非 IW-7 路径需要的公共 helper。
- `INSERT OVERWRITE` 暂时保留现状，直到 IW-8 单独 cutover。

这个边界让代码更干净，也让验收更硬：append 只有一条生产写入路径。

## 架构

```text
INSERT INTO iceberg SELECT/VALUES
  -> statement routing and target validation
  -> build SELECT logical/physical plan
  -> build IcebergWriteSinkSpec
  -> codegen SELECT as coordinated plan with ICEBERG_TABLE_SINK
  -> sink fragments write staged data files on BE processes
  -> exec_status_report carries sink_commit_infos
  -> WriteCoordinator builds real WriteCommitInput
  -> IcebergWriteTransactionRunner commits Append once
  -> finalize invalidates catalog/cache/dictionary state
```

The engine owns transaction semantics. The sink owns staged file production.
The coordinator owns writer result collection. The commit service owns Iceberg
metadata mutation.

## Component Design

### Engine Entry

`src/engine/iceberg_writer.rs` remains the INSERT routing owner:

- Resolve target table/catalog/ref.
- Validate INSERT column list and target schema alignment.
- Rewrite or wrap the SELECT/VALUES query so its output is already in target
  column order, including default-value materialization for omitted columns.
- Preserve branch/ref format-v3 checks.
- Select `CommitOpKind::FastAppend` and `IcebergOperationKind::InsertAppend`.
- Create and run `IcebergWriteTransactionRunner`.

For append, `InsertOrOverwriteWriteExecutor::run_coordinated_write` should run
the distributed write plan and return the real `CoordinatedQueryResult`.

It should not write local files itself for append.

The old path performed INSERT column/default alignment after collecting chunks.
IW-7 cannot do that because rows flow directly into sink fragments. Therefore
target alignment must happen before codegen, either by inserting an explicit
projection above the SELECT plan or by generating sink output expressions that
include the target-order/default-value mapping.

### IcebergWriteSinkSpec

Introduce a small spec passed from engine to codegen. It should contain only
the sink/codegen contract, not commit semantics.

Required contents:

- target table id used by `TIcebergTableSink.target_table_id`
- target `TableDef` and `IcebergTableInfo`
- target column order after INSERT column alignment
- output expressions in target column order, including default literals for
  omitted target columns
- sink output tuple id and slot layout contract
- Iceberg table location and data location
- object store/cloud configuration
- file format and compression
- Iceberg partition metadata needed to build `TIcebergTable.partition_info`

Do not put `CommitOpKind`, operation state, retry policy, or metadata commit
logic in this spec.

### Descriptor and Partition Contract

PR #275's `partition_key_column_ids` is not enough as the durable abstraction.
It only models source columns and can mislead transform-partitioned tables
such as `bucket`, `truncate`, `day`, `month`, or `hour`.

The sink lowering path already expects descriptor-level Iceberg metadata:

- `TIcebergTable` in desc table.
- `TIcebergTable.partition_info`.
- each partition field's source column name, partition column name,
  transform expression, and partition expression.
- `TIcebergTableSink.tuple_id` so `update_partition_expr_slot_refs` can map
  partition source columns to sink output slot refs.

IW-7 must make standalone codegen emit that contract for write targets.
If partition metadata is missing or cannot be represented, codegen must fail
fast instead of silently falling back to a weaker contract.

Partition distribution has two valid implementation levels:

1. Prefer shuffle by lowered partition expressions, so one logical Iceberg
   partition converges to one writer.
2. If the first cut shuffles by source columns only, correctness is still valid,
   but the implementation must not claim same-partition single-writer layout for
   transform partitions. Tests must document this limitation.

Correctness does not require same-partition single writer; file layout quality
does. The spec and tests must keep those separate.

### Codegen Sink Plan

Add a write-plan codegen entry that takes:

- the optimized SELECT physical plan
- `IcebergWriteSinkSpec`

The builder should:

1. Build the SELECT fragments using existing scan/join/agg/distribution logic.
2. Replace the root result sink with `TDataSinkType::ICEBERG_TABLE_SINK`.
3. Populate `TIcebergTableSink` with target table id, location, data location,
   file format, compression, cloud configuration, and tuple id.
4. Register the target Iceberg table in the descriptor table with full schema
   and partition info.
5. Ensure sink output expressions match target column order.
6. Insert or preserve the distribution fragment needed by the sink contract.

The sink fragment must be recognized by existing `is_write_sink`, which causes
`ExecutionCoordinator` to register expected writers and wait for write reports.

### Coordinator and Commit

Reuse existing IW-4/IW-6 infrastructure:

- `ExecutionCoordinator::execute_with_write_outcome`
- `WriteCoordinator`
- `exec_status_report`
- `runtime::sink_commit`
- `IcebergWriteTransactionRunner`
- `IcebergWriteCommitExecutor::commit_write_input`

The only required behavioral change is that append now receives real
`WriteCommitInput` from writer reports instead of synthetic commit input.

## Error Handling

- Missing sink metadata: fail during codegen or sink lowering with an explicit
  error.
- Output expr count or type mismatch: fail during sink factory construction.
- Missing object store credentials for object-store data location: fail during
  sink factory construction.
- Writer failure: coordinator observes failed report, cancels peers, and runner
  records known-uncommitted failure.
- Timeout or client disconnect: coordinator cancels fragments; runner does not
  enter commit.
- Commit unknown and finalize failure: handled by the existing transaction
  runner state machine.
- Empty input: writer reports with no `sink_commit_infos` must be normalized to
  the existing no-op/aborted transaction outcome, with no metadata commit. It is
  not enough to check that `WriteCommitInput.writers` is non-empty.
- Staged files written before a writer failure or cancel must be cleaned by the
  existing abort cleanup path. If current distributed sink abort coverage is
  insufficient, IW-7 must add it.

## Testing

Unit and integration coverage should include:

- codegen emits `ICEBERG_TABLE_SINK` with complete `TIcebergTableSink` fields.
- descriptor table includes Iceberg schema and partition info for the target.
- sink output tuple slots map target source column names for partition expr
  rewrite.
- append no longer uses `synthetic_write_commit_input()`.
- INSERT column/default alignment happens before sink execution, including
  omitted nullable/default columns.
- all-in-one `INSERT INTO ... VALUES` for partitioned and unpartitioned tables.
- all-in-one `INSERT INTO ... SELECT` for partitioned and unpartitioned tables.
- cross-process 1FE+2BE append under `iceberg-rest`, proving multiple writer
  reports can produce one Append commit.
- empty INSERT SELECT commits nothing and leaves table contents unchanged.
- writer failure or timeout cancels peers and does not commit.
- transform-partition table coverage. If implementation shuffles only by source
  column in the first cut, the test should assert correctness but not
  same-partition single-writer layout.

The D2 overwrite hang guard from PR #275 is useful as a regression guard, but
it is not IW-7 completion evidence because IW-7 is append distributed cutover.

## Relationship to PR #275

Keep from PR #275:

- The recognition that INSERT should eventually become a plan with a terminal
  Iceberg sink.
- The observation that `ICEBERG_TABLE_SINK` lowering, WriteCoordinator, and
  transaction runner are already mostly reusable.

Do not keep as the main design boundary:

- A standalone optimizer-first stage that adds a physical sink but does not
  connect engine/codegen/coordinator.
- `partition_key_column_ids` as the long-term sink distribution contract.
- A stage-1 PR that claims IW-7 progress without generating a sink fragment or
  replacing the append write path.

## Future Optimizer-Owned INSERT

The current design is deliberately shaped for migration:

- `IcebergWriteSinkSpec` is the future payload of a logical/physical INSERT
  sink.
- codegen and coordinator should not care whether the spec came from engine
  routing or optimizer planning.
- transaction and commit semantics stay outside optimizer rules.

When DML enters optimizer, the new work should focus on producing the same spec
from a DML logical plan, not on rebuilding distributed sink execution.
