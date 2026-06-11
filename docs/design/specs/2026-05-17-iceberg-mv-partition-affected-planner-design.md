# Iceberg MV affected partition planner design

- 状态：待用户 review
- 日期：2026-05-17
- 范围：Iceberg-backed materialized view target、单 base projection/filter refresh planning、affected MV partition 输出
- 依赖：
  - Iceberg MV partition contract：CREATE MV 时持久化 target partition spec 与 source column/transform 映射
  - A2/A3 snapshot pin：refresh planner 已能拿到 base `[previous, current]` snapshot range
  - Iceberg change batch：`IcebergChangeBatch` 已包含 inserted/deleted data file 的 partition metadata

## 1. 背景

PR1 已经把 Iceberg MV target partition 从 DDL/metadata 层引入系统：CREATE MV 能为 Iceberg
target 建分区，并在 MV schema contract 中持久化 `MvPartitionContract`。但 refresh planner
目前仍然没有 affected partition 的概念：

- `RefreshPlan` 只表达 refresh mode、base refs、snapshot pins 和 backend plan；
- refresh 执行路径无法从 planner 侧看到本轮 base change 影响了哪些 MV partition；
- 后续 target locator pruning 或 partition-scoped rebuild 没有统一输入；
- 当 planner 无法证明 affected partitions 时，没有显式的 `Unknown(reason)` 语义。

PR2 的目标不是改变 refresh 执行范围，而是把 affected partitions 做成 refresh planner 的一等输出。
这让后续 PR 可以基于同一结构实现 locator pruning、partition scoped rebuild、debug/explain 展示和性能优化。

## 2. 目标

1. 为 Iceberg MV refresh planner 增加结构化 affected partition 输出。
2. 首版只支持 single-base projection/filter MV。
3. 对 unpartitioned MV 明确返回 `Unpartitioned`。
4. 对 metadata 能证明的变更返回 `Known { new_partitions, old_partitions }`。
5. 对无法证明的场景返回 `Unknown(reason)`，不隐式退化为 whole-table。
6. 在 `RefreshPlan`、refresh debug logs 或测试断言中暴露 affected partition 结果。
7. 为后续 row-evaluation fallback 预留接口边界，但 PR2 不读取 changed rows 计算 partition。

## 3. 非目标

- 不改变实际 refresh 执行范围。
- 不实现 target locator partition pruning。
- 不实现 partition-scoped rebuild。
- 不实现真实 row-evaluation fallback。
- 不支持 join MV 的 affected partition 推导；join MV 在 PR2 返回 `Unknown(reason)`。
- 不支持 aggregate MV 的 affected partition 推导。
- 不重新设计 `IcebergChangeBatch` 的 lineage 语义。
- 不为 managed-lake target MV 引入 partition planner。

## 4. 术语

**affected MV partition**：本轮 refresh 可能需要写入、删除、替换或重算的 MV target partition。

**metadata fast path**：只使用 Iceberg manifest/data-file metadata 和 MV partition contract 推导
affected partitions，不读取实际 changed rows。

**row-evaluation fallback**：metadata 无法证明时，读取或构造 changed rows，并执行 MV partition
expression 来计算 affected partitions。PR2 只保留接口和 `Unknown(reason)` 行为，不实现真实计算。

**new partition**：inserted data files 或 update new rows 会落入的 MV partition。

**old partition**：deleted data files 或 update old rows 需要撤销的 MV partition。

## 5. 数据模型

新增 `src/engine/mv/partition/` 模块，首版建议包含：

```text
src/engine/mv/partition/
  mod.rs
  key.rs
  mapping.rs
  planner.rs
```

核心输出结构：

```text
AffectedMvPartitions
  Unpartitioned
  Known {
    new_partitions: BTreeSet<MvPartitionKey>,
    old_partitions: BTreeSet<MvPartitionKey>,
  }
  Unknown {
    reason: String,
  }
```

`MvPartitionKey` 需要是稳定、可排序、可展示的结构，而不是裸字符串：

```text
MvPartitionKey {
  spec_id: i32,
  fields: Vec<MvPartitionKeyField>,
}

MvPartitionKeyField {
  field_name: String,
  value: MvPartitionValue,
}
```

`MvPartitionValue` 首版可以采用保守表示：

- metadata fast path 从 Iceberg partition metadata 得到的值保留为 normalized string；
- 缺失值、无法解析值、transform 不匹配时不构造 partial key，而是返回 `Unknown(reason)`；
- `MvPartitionKey` 的 `Display` 仅用于 logs/tests，不作为持久化格式。

`RefreshPlan` 增加字段：

```text
affected_partitions: AffectedMvPartitions
```

字段放在顶层 `RefreshPlan`，而不是只放在 `IcebergRefreshPlan`，原因是：

- affected partitions 是 refresh planner 的公共概念；
- managed-lake target 首版可返回 `Unknown("managed-lake MV partition planning is not implemented")`
  或 `Unpartitioned`；
- 后续 locator/rebuild 只需要读 `RefreshPlan`，不需要先拆 backend payload。

## 6. Planner 输入

`MvAffectedPartitionPlanner` 的输入保持纯 planning 数据：

```text
MvAffectedPartitionPlanner::plan(input) -> AffectedMvPartitions

MvAffectedPartitionPlanInput {
  query_shape,
  target_partition_contract,
  base_change_batch,
  fallback_strategy,
}
```

输入来源：

- `query_shape`：来自现有 single-base projection/filter classifier。
- `target_partition_contract`：来自 PR1 的 `MvSchemaContract.target.partition`。
- `base_change_batch`：来自 `IcebergChangeBatch`。
- `fallback_strategy`：PR2 默认是 no-op fallback，只返回 `Unknown(reason)`。

planner 不依赖 catalog、table commit、object store 或 execution pipeline。这样它可以用单元测试覆盖。

## 7. Metadata fast path

metadata fast path 只在 planner 能证明 base file partition metadata 与 MV partition contract 等价时返回
`Known`。

首版支持条件：

1. MV target 有 partition contract。
2. 每个 MV target partition field 都能映射到单个 source output column。
3. source output column 能回溯到单个 base field。
4. MV partition transform 与 base file partition metadata 的 transform 可证明一致。
5. changed data file 带有完整 `partition_spec_id` 和 `partition_key`。
6. 没有无法归属到 file-level partition 的 row-level delete。

首版可先实现 identity-compatible path：

- base table 按同一列 identity partition；
- MV target 也按同一列 identity partition；
- `IcebergChangeBatch.inserts` 的 partition metadata 转成 `new_partitions`；
- `IcebergChangeBatch.deleted_data_files` 的 partition metadata 转成 `old_partitions`。

当 Iceberg metadata 暂时只暴露 formatted partition key string 时，mapping 层可以先要求
`partition_key` 的字段集合和顺序与 `MvPartitionContract.fields` 完全一致。无法验证时返回
`Unknown(reason)`，不尝试字符串猜测。

## 8. Delete / update 语义

Iceberg COW update 或 overwrite diff 通常表现为：

- added data files：进入 `new_partitions`
- deleted data files：进入 `old_partitions`

因此 metadata fast path 能同时表达 update 的 old/new affected partitions。planner 只负责输出集合，
不在 PR2 中消费这些集合来改变 apply 范围。

row-level deletes 的处理规则：

- position delete / equality delete 如果没有足够 old row partition metadata，返回 `Unknown(reason)`；
- 如果未来能从 delete file 或 referenced data file metadata 证明 old partition，再扩展 metadata path；
- PR2 不通过扫描 old rows 来补齐 delete partition。

## 9. Unknown 边界

以下场景必须返回 `Unknown(reason)`：

- query shape 不是 single-base projection/filter；
- join MV、aggregate MV、union/subquery/CTE 等复杂形态；
- MV target partition contract 缺失但 target 实际是 partitioned；
- partition transform 无法与 base metadata 建立等价映射；
- changed data file 缺少 `partition_spec_id` 或 `partition_key`；
- equality delete 或 position delete 需要 row-level old partition 推导；
- `IcebergChangeBatch` 表示的变更包含 planner 暂不理解的组合；
- row-evaluation fallback 被需要但 PR2 默认 fallback 未实现。

`Unknown` 不是错误，也不是 whole-table。它表示 planner 无法证明 affected partitions。当前 refresh
执行路径保持原行为，后续 locator/rebuild 可以根据 `Unknown` 选择 full target scan 或报出更精确的诊断。

## 10. 数据流

```text
plan_iceberg_mv_refresh
  -> classify single-base projection/filter
  -> load StoredMvDefinition and MvSchemaContract
  -> capture RefreshSnapshotPin
  -> plan IcebergChangeBatch for base snapshot range
  -> MvAffectedPartitionPlanner::plan(...)
  -> RefreshPlan.affected_partitions
  -> refresh logs / focused tests expose affected partitions
```

对 no-op refresh：

- unpartitioned MV 返回 `Unpartitioned`；
- partitioned MV 返回 `Known { new_partitions: {}, old_partitions: {} }`，表示本轮没有 affected partition；
- 如果 contract 本身不可验证，仍返回 `Unknown(reason)`。

对 full refresh 或 first refresh：

- PR2 可以返回 `Unknown("full refresh affected partition planning is not implemented")`；
- 不阻塞实际 full/first refresh 的现有行为；
- 后续 partition-scoped rebuild 再定义 full refresh 的 target partition 枚举策略。

## 11. Observability

PR2 需要让 affected partition result 可以被测试和人工确认：

- `RefreshPlan` 的 debug 输出包含 `affected_partitions`；
- refresh planning logs 输出三态结果和 partition 数量；
- `Unknown` logs 必须包含 reason；
- 单元测试直接断言 `AffectedMvPartitions`；
- 集成测试可以通过 planner-level helper 或 refresh plan debug path 验证结果。

日志只用于诊断，不作为 SQL 用户可见 contract。

## 12. 测试计划

单元测试：

1. `AffectedMvPartitions::Known` 对 new/old partitions 去重并保持稳定排序。
2. `Unpartitioned` 的 display/debug 输出稳定。
3. `Unknown(reason)` 保留具体 reason。
4. identity-compatible metadata fast path 能把 inserted data files 映射到 `new_partitions`。
5. overwrite/delete data files 能映射到 `old_partitions`。
6. 同时有 inserts 和 deleted data files 时，old/new 集合都正确。
7. 缺少 partition metadata 时返回 `Unknown(reason)`。
8. equality delete / position delete 需要 row fallback 时返回 `Unknown(reason)`。
9. transform 不匹配时返回 `Unknown(reason)`。

refresh planner 测试：

1. single-base projection/filter append-only refresh 的 `RefreshPlan` 带 `Known { new_partitions }`。
2. COW update / overwrite diff 场景的 `RefreshPlan` 能表达 old/new partitions，前提是 metadata 可证明。
3. unpartitioned MV 返回 `Unpartitioned`。
4. join MV 或复杂 shape 返回 `Unknown(reason)`。
5. PR2 不改变 refresh 执行结果和行数。

## 13. 验收标准

1. `RefreshPlan` 有结构化 `affected_partitions` 字段。
2. Iceberg-backed single-base projection/filter refresh planning 会填充该字段。
3. append-only insert 能通过 metadata fast path 得到 affected MV partitions。
4. metadata 可证明的 delete/update 能表达 old/new affected partitions。
5. 不能证明时返回 `Unknown(reason)`，不隐式 whole-table。
6. join/aggregate/复杂 MV 在 PR2 明确返回 `Unknown(reason)`。
7. PR2 不改变实际 refresh 执行范围。
8. 测试覆盖 planner 三态、metadata fast path 和 unknown 边界。

## 14. 后续 PR 方向

PR2 完成后，后续工作可以按依赖顺序推进：

1. Target locator partition pruning：消费 `RefreshPlan.affected_partitions`，缩小 target read/delete 范围。
2. Partition-scoped rebuild：对 `Known` partitions 只重算局部 target partition。
3. Row-evaluation fallback：metadata 无法证明时，通过 changed rows 计算 MV partition key。
4. Transform coverage：补齐 day/month/year/hour/bucket/truncate 等可证明 metadata path。
5. Join/aggregate MV affected partition planner：基于多 base change batch 和更复杂的 output lineage 推导。
