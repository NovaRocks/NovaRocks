# IVM-P1 后续 · Partition-pruned touched-group state lookup design

- 状态：待用户 review
- 日期：2026-05-20
- 范围：Iceberg-backed materialized view target、aggregate / join aggregate IMV 增量 apply、touched-group state lookup、aggregate-delta affected partition derivation
- 依赖：
  - Iceberg MV partition contract：`MvSchemaContract.target.partition` 已持久化 target partition spec、source target field id、partition transform
  - Iceberg MV partition planner P2：`RefreshPlan.affected_partitions`、`AffectedMvPartitions` 三态、`MvPartitionKey`、identity transform mapping
  - Iceberg target aggregate / join aggregate IMV（#143）：`apply_iceberg_aggregate_delta_chunks`、`AggregateMvLayout`、`load_current_aggregate_target_state`
  - Iceberg row lineage v3：apply key `__row_id__` 已在 aggregate MV target schema 中持久化
- 非目标：managed-lake target、projection/filter MV、partition full recompute、`MIN/MAX` retract、partition lifecycle、`__row_id__` 反解 partition

## 1. 背景

#143 把 Iceberg target aggregate / join aggregate IMV 的增量 refresh 跑通，apply 阶段已经只 delete / insert touched groups。但 apply 之前还要把整个 target aggregate state 拉回来做合并：

```rust
let old_chunks = load_current_aggregate_target_state(target_table, layout)?;
let merge     = merge_aggregate_target_state(layout, &old_chunks, delta_chunks)?;
```

`load_current_aggregate_target_state` 走 `target_table.scan().select(physical_cols).build()`，没有 partition 谓词、没有 row id filter。等价行为是：

```text
O(target MV rows + delta rows)
```

当 target 行数大、每次 delta 只命中少量 group 时，refresh 单次成本不可接受。`locate_target_rows_by_apply_key_impl` 在 delete 阶段同样是全表扫拿 `_file / _pos / apply_key`，是同一个性能瓶颈的另一面。

partition planner P2 已经把 affected partition 抽象成 `AffectedMvPartitions` 三态，但只在 single-base projection/filter refresh 中消费；aggregate / join aggregate refresh 还没有 affected partition 输入，更不会用它做 pruning。

本 spec 把 aggregate / join aggregate IMV 的 incremental apply 改造成两级过滤：第一层用 partition 裁剪 target 扫描范围，第二层用 group row id 精确选 touched 行，把单次 refresh 成本降到：

```text
O(touched partitions target rows + touched group rows + delta rows)
```

## 2. 目标

1. 新增一套 partition-aware 的 aggregate target state lookup API，供 aggregate / join aggregate apply 调用。
2. 新增 aggregate-delta affected partition derivation，从 signed aggregate delta chunks 里的 visible group key 计算 affected target partition 集合（包含 dim-side 移动后的新旧两端）。
3. 在 `apply_iceberg_aggregate_delta_chunks` 里串起 derivation → lookup → merge → delete/insert，替换现有全量 `load_current_aggregate_target_state`。
4. 同时扩展 target locator scan，使它也能消费 affected partition 集合做 pruning。
5. 严格 fail-fast：partition derivation / mapping / scan 任何一步无法证明结果，refresh 必须报错，禁止悄悄退化成 unpartitioned 全表扫描。non-partitioned MV 仍然走合法的 full target + row-id filter 路径。
6. 在 partition mapping 中扩展支持 Iceberg 一类 transform（identity / year / month / day / hour / bucket / truncate），让 derivation 能复用 `iceberg::transform::create_transform_function`，保持 client 与 manifest 两条路径产出一致的 `MvPartitionKey`。
7. 暴露结构化 observability：touched group count、affected partition count、planned/scanned target file count、scanned target row count、derivation 失败 reason。
8. SQL 覆盖 partitioned aggregate、partitioned join aggregate、non-partitioned aggregate fallback、partition 移动场景。

## 3. 非目标

1. 不动 projection/filter MV、不引入 managed-lake target 支持。
2. 不实现 partition full recompute。aggregate state 仍然是 signed-delta IVM，与 partition recompute 是两种 refresh strategy。
3. 不在 signed aggregate delta path 里支持 `MIN/MAX`；`MIN/MAX` 仍然要求 partition full recompute fallback，作为独立 refresh strategy 单独立项。
4. 不修改 base-side delta 计算逻辑、不动 `IcebergChangeBatch`、不动 `aggregate_shape_for_layout` / `build_aggregate_mv_layout`。
5. 不实现 file/position 精确的 state row read（"O(touched group rows + delta rows)"）。本任务终点是 partition-pruned scan + client-side row-id filter；file/position level 是后续优化方向。
6. 不实现 partition lifecycle（drop / replace / evolve partition spec）。target spec drift 仍然由现有 contract guard 拦截。
7. 不引入新的可观测 metric exporter；observability 限定在 tracing 结构化字段。

## 4. 术语

- **aggregate target state**：target Iceberg table 中持久化的 aggregate physical rows（`__row_id__` + visible group key + visible aggregate output + hidden agg state columns）。
- **touched group**：本轮 delta 命中的 group row。由 signed aggregate delta chunks 中的 `__row_id__` 集合决定。
- **affected target partition**：本轮 refresh 需要写入或撤销的 target Iceberg partition。来源同时包含 delta 中新 group 的 partition 和 dim-side 移动前旧 group 的 partition。
- **partition derivation**：从 visible group key column 出发，用 partition contract + Iceberg transform 计算 `MvPartitionKey` 的过程。
- **derivation fail-fast**：partition derivation 中任何一步无法证明结果（contract 缺失、transform 不支持、值非法、metadata drift）时，lookup API 返回错误，整次 refresh 失败。
- **partition-pruned target scan**：在 Iceberg scan plan 阶段只保留 partition 命中的 file scan task。
- **row-id filter**：从 partition-pruned scan 读出的 Arrow batch 上，按 `__row_id__` 集合做 client-side filter。

## 5. 设计原则

1. **两级独立过滤**：partition pruning 与 group row id filter 是两层互不依赖的过滤。一层失效不会让另一层错误剪枝。
2. **partition 是优化，row id 是正确性**：apply key 永远是 `__row_id__`；partition 只用来减小扫描范围。derivation 失败要 fail fast，不允许悄悄"按 row id 验证、按 partition 误剪"。
3. **derivation 由 visible group key 驱动，不反解 `__row_id__`**：encoded row id 是 group identity hash，不是 partition 来源。
4. **两条路径产出同一个 `MvPartitionKey`**：
   - PR2 已建立的 file-metadata path：从 `IcebergChangeBatch.{inserts,deleted_data_files}` 的 `partition_values`（Iceberg manifest 已经存了 transform 后的值）映射到 `MvPartitionKey`。
   - 本任务新增的 delta-chunk path：从 delta chunk 的 visible group key column（pre-transform 值）调用 `iceberg::transform::create_transform_function` 计算 transform 后的值，再走同一序列化规则。
   两条路径必须输出可比较的 key，否则会出现"按 base file 算的 partition 和按 delta group key 算的 partition 不重合"。
5. **不重新实现 Iceberg transform**：所有 transform 计算都通过 `iceberg::transform::create_transform_function(&Transform) -> BoxedTransformFunction` 调用，再统一序列化。serialization 沿用 PR2 `change_partition_value_string` 的规则（year/month/day/hour → 整数十进制；bucket → 整数十进制；truncate → primitive Display；identity → primitive Display；NULL → `MvPartitionValue::Null`）。
6. **target locator 与 state loader 共享 partition filter 入参**：避免出现"state loader 已剪枝但 locator 仍全表扫"的非对称。

## 6. 整体架构

```text
plan_iceberg_mv_refresh
  -> (existing) plan single-base / join / aggregate / join aggregate refresh
  -> (existing) RefreshPlan { ..., affected_partitions: AffectedMvPartitions }
       * single-base projection/filter 已填充
       * aggregate / join aggregate 这一字段保留为 Unknown(reason) 直到本任务接入

refresh_aggregate_iceberg_mv  /  refresh_join_aggregate_iceberg_mv
  -> compute signed aggregate delta chunks
  -> apply_iceberg_aggregate_delta_chunks(layout, delta_chunks, schema_contract, ...)
        |
        |-- 1) extract touched group row ids (existing delta_row_ids)
        |-- 2) derive affected target partitions from delta chunks
        |       partition_derivation::derive_from_aggregate_delta(
        |           layout, schema_contract, delta_chunks)
        |       -> AffectedAggregateTargetPartitions
        |          { Unpartitioned, Known { partitions } }
        |          // fail fast on derivation error
        |-- 3) partition-pruned + row-id-filtered state lookup
        |       iceberg_aggregate_state::load_touched_aggregate_target_state(
        |           target_table, layout,
        |           touched_row_ids,
        |           target_partition_filter)
        |       -> { chunks, AggregateStateLookupStats }
        |-- 4) merge_aggregate_target_state(layout, touched_chunks, delta_chunks)
        |-- 5) target locator
        |       iceberg_target_apply::locate_target_rows_by_string_apply_key(
        |           target_table, apply_key_col,
        |           delete_row_ids,
        |           existing_deletes_by_file,
        |           referenced_data_file_partitions,
        |           target_partition_filter)
        |-- 6) write data files, commit ref, publish (existing pipeline)
        `-- 7) tracing::info!(structured stats)
```

新增模块只在 `src/engine/mv/` 子树内，连接现有 partition / aggregate / target apply 三块。

## 7. 模块边界与新增 API

### 7.1 `src/engine/mv/partition/`：扩展 aggregate delta derivation

新增子模块 `aggregate_delta.rs`：

```text
src/engine/mv/partition/
  mod.rs
  key.rs           (PR2)
  mapping.rs       (extend transform coverage in this task)
  planner.rs       (PR2, base-file metadata derivation)
  aggregate_delta.rs  (NEW)
```

`aggregate_delta.rs` 暴露：

```text
pub(crate) struct AggregateDeltaPartitionInput<'a> {
    pub(crate) layout: &'a AggregateMvLayout,
    pub(crate) schema_contract: &'a MvSchemaContract,
    pub(crate) delta_chunks: &'a [Chunk],
}

pub(crate) enum AffectedAggregateTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
}

pub(crate) fn derive_from_aggregate_delta(
    input: &AggregateDeltaPartitionInput<'_>,
) -> Result<AffectedAggregateTargetPartitions, AffectedPartitionError>;
```

为什么不复用 `AffectedMvPartitions` 三态：
- aggregate apply 是正确性路径，本任务约定 fail-fast；`Unknown(reason)` 在这一层会被立即转换为 `Err`。把 enum 收窄成两态（Unpartitioned / Known）能在类型层禁止"悄悄退化"。
- `Known { new, old }` 在 base file metadata 路径有意义（COW added vs deleted files），在 delta chunk 路径里 new 和 old 是同源的（每个 signed delta row 同时贡献一个旧 group 和新 group 的位置，但 `__row_id__` 才负责区分），因此合并成单一 partition 集合更直接，避免在 lookup 时再做并集。

`AffectedPartitionError` 是新增 enum，至少包含：

```text
pub(crate) enum AffectedPartitionError {
    ContractMissing(String),
    TransformUnsupported { field: String, transform: String },
    GroupKeyColumnMissing(String),
    GroupKeyTypeMismatch { field: String, want: String, got: String },
    TransformFailed { field: String, source: String },
    OutputLineageNotPureColumn(String),
}
```

实现要点：
- 从 `MvSchemaContract.target.partition` 列表出发；contract 缺失但 layout 是 partitioned 视为 fail-fast（`ContractMissing`）。contract 显示无 partition → 返回 `Unpartitioned`，不再触碰 delta。
- 对每个 partition field：
  1. 反查 `MvSchemaContract.target.visible_columns` 找到 target field id 对应的 visible column index；
  2. 反查 `MvSchemaContract.output.columns[index].expression`，要求 `ExpressionKind::Column` 且 `referenced_base_field_ids.len() == 1`，否则 `OutputLineageNotPureColumn`；
  3. 反查 `AggregateMvLayout.group_key_source_indexes`，确保该 output index 是 group key，否则 `OutputLineageNotPureColumn`（partition 字段引用非 group 列违反 PR1 约束，但作为 defense-in-depth 仍要 check）；
  4. 从 `AggregateMvLayout.visible_columns[index]` 得到 Arrow data type；
  5. 调用 `iceberg::transform::create_transform_function(&iceberg_transform)`，把 delta chunk 该列作为输入，得到 transformed Arrow column；
  6. 把 transformed 值用 PR2 `change_partition_value_string` 同款序列化规则转成 `MvPartitionValue::{String, Null}`。
- partition contract 的 `MvPartitionTransformContract` 已经覆盖 identity / year / month / day / hour / bucket / truncate / void。本任务把 mapping 端从"只 identity"升级到完整一类 transform；void 仍报 unsupported（语义上写入端不会生成有效 partition value）。
- delta chunks 可能有多个；对每个 chunk 调用 transform 一次得到 partition column，再行级生成 `MvPartitionKey` 推入 `BTreeSet` 自动去重。
- 空 delta（所有 chunk row_count = 0）允许返回 `Known { partitions: {} }`。

### 7.2 `src/engine/mv/partition/mapping.rs`：扩展 transform 覆盖

PR2 仅支持 identity。本任务在 mapping 层面同样扩展到完整一类 transform，原因有二：

1. file-metadata path 在 transform = `day("ts")` 等场景需要把 manifest 的 transformed 值（如 `19500`）直接当作 `MvPartitionKey` 值；只要 transform 与 contract 一致就允许 mapping，无需在 client 重新计算。
2. 两条路径都要走完整 transform 才能让"base file partition 与 delta chunk partition 比对"成立。

具体改动：
- `partition_transform_name` 已经能 stringify 所有变体。
- mapping 主循环：在比较 `file_partition_value.transform`（来自 manifest，存的是 transform 文本，由 `change_partition_transform_name` 生成）和 `partition_field.transform`（contract 持久化的 enum）时，按 enum 等价 + `num_buckets` / `width` 完全匹配；不再硬编码 "identity"。
- 对 transform 不匹配返回 `Err`（推荐改成 `AffectedPartitionError`，但为了不破坏 PR2 既有签名，第一版用 `String` 错误，后续 PR 在 apply path 入口统一映射）。

### 7.3 `src/engine/mv/iceberg_aggregate_state.rs`：touched-group state lookup API

新增公共函数：

```text
pub(crate) struct AggregateStateLookupStats {
    pub(crate) planned_file_count: usize,
    pub(crate) pruned_file_count: usize,
    pub(crate) scanned_row_count: usize,
    pub(crate) matched_row_count: usize,
}

pub(crate) fn load_touched_aggregate_target_state(
    target_table: &iceberg::table::Table,
    layout: &AggregateMvLayout,
    touched_row_ids: &BTreeSet<String>,
    partition_filter: &TargetPartitionFilter,
) -> Result<(Vec<Chunk>, AggregateStateLookupStats), String>;
```

`TargetPartitionFilter` 是新引入类型，统一表达"partition 不裁剪 / partition 命中给定集合"，并供 locator 共用：

```text
pub(crate) enum TargetPartitionFilter {
    None,                    // Unpartitioned MV; do not prune
    AllowList(BTreeSet<MvPartitionKey>),  // Partitioned; keep only matching files
}
```

行为：
- 当 `touched_row_ids` 为空：直接返回 `(vec![], stats { all zeros })`，不发起 scan。
- 构造 `target_table.scan().select(physical_cols)` 后，在 `plan_files()` 流上做 client-side filter：对每个 `FileScanTask`，按 `task.partition_spec_id` 找 `target_table.metadata().partition_spec_by_id(...)`，把 `task.data_file_partition`（`iceberg::spec::Struct`）通过 `change_partition_field_values` 的同款规则转成 `Vec<ChangePartitionFieldValue>`，再调用 `mapping::map_file_partition_to_mv_key(schema_contract, spec_id, &values)` 得到 `MvPartitionKey`，只保留 partition_filter 中命中的 task。统计 `planned_file_count` / `pruned_file_count`。
- 第一版不调用 `target_table.scan().with_filter(...)`：Iceberg-rust 当前对 partition-level predicate 支持有限，client-side filter 已经足够把 file 级别的开销裁掉，且不会引入 predicate API 兼容性问题。后续在 derivation 稳定后再考虑 push down 到 scan builder。
- Arrow reader 与现有 `load_current_aggregate_target_state` 一致：`with_row_group_filtering_enabled(false)`，并在读出 batch 后 `validate_physical_aggregate_schema`。
- 在 batch 层面用 `__row_id__` 列做 client-side BooleanArray filter，只保留 `touched_row_ids` 命中的行；统计 `scanned_row_count` 与 `matched_row_count`。
- 不在 `partition_filter == None` 时跳过 row-id filter——non-partitioned MV 同样要享受 row-id filter 收益（这是 TODO 明确要求的 "non-partitioned fallback"）。
- `partition_filter == AllowList(empty)` 的语义：partition 集合空，但 touched_row_ids 非空，说明 derivation 与 row id 不一致。这是 fail-fast 触发条件之一（见 §9 错误边界 E5）。

保留 `load_current_aggregate_target_state` 不删除：它在测试 fixture / 调试 path 里仍有用，且 PR1 期间 apply path 切换会跨越多个 PR；切换完成后再独立清理。

### 7.4 `src/engine/mv/iceberg_target_apply.rs`：locator partition pruning

`locate_target_rows_by_apply_key_impl` 新增一个 `partition_filter: &TargetPartitionFilter` 参数，行为与 §7.3 中 partition-pruning 段一致：

- 在 `plan_files()` 流上按 partition_filter 做 client-side filter。
- 不影响 `_file / _pos / apply_key` 列的读取与 visibility 校验。
- 返回的 `PositionDeleteGroup` 不变。

为避免改动调用面过大，所有公共 wrapper（`locate_target_rows_by_apply_key`、`locate_target_rows_by_string_apply_key`、`locate_target_rows_by_apply_key_string`）保持现有名称，但新增 `partition_filter` 形参（join-row-key wrapper 透传即可）。调用方在 `apply_iceberg_aggregate_delta_chunks` 中传入与 state lookup 相同的 `TargetPartitionFilter`；其它现存调用方（`iceberg_join_coalesce`、`iceberg_merge_sink`）一律传 `TargetPartitionFilter::None`，保留行为不变。它们各自迁移到 partition-pruned locator 不在本任务范围。

### 7.5 `src/engine/mv/iceberg_refresh.rs`：apply path 串联

新增内部辅助函数（命名建议）：

```text
fn build_aggregate_target_partition_filter(
    layout: &AggregateMvLayout,
    schema_contract: &MvSchemaContract,
    delta_chunks: &[Chunk],
) -> Result<TargetPartitionFilter, String>;
```

它把 `partition::aggregate_delta::derive_from_aggregate_delta` 结果转成 `TargetPartitionFilter`，并把 `AffectedPartitionError` 转换为 refresh 顶层 `String` 错误。错误信息要包含 mv id、target fqn、partition contract field、transform、failing chunk index。

`apply_iceberg_aggregate_delta_chunks` 改造：

1. metadata-only 短路保留（`delta_chunks` 全空仍然走 `finalize_iceberg_mv_metadata_only_refresh`）。
2. `let touched_row_ids = delta_row_ids(layout, delta_chunks)?;`（提到 merge 之前，便于复用）。
3. `let partition_filter = build_aggregate_target_partition_filter(layout, schema_contract, delta_chunks)?;`
4. `let (old_chunks, lookup_stats) = load_touched_aggregate_target_state(target_table, layout, &touched_row_ids, &partition_filter)?;`
5. `let merge = merge_aggregate_target_state(layout, &old_chunks, delta_chunks)?;`
6. 后续 staging branch / locator / commit / publish 保留；locator 调用追加 `&partition_filter` 参数。
7. refresh 末尾 `tracing::info!(...)` 增加结构化字段（见 §11）。

`merge_aggregate_target_state` 的现有签名不变：它消费 old_chunks 与 delta_chunks，不需要知道 partition。`build_old_state_map` 仅按 row_id 索引——touched_row_ids 已经在 lookup 阶段裁剪，merge 阶段在小集合上工作。

`refresh_aggregate_iceberg_mv` 与 `refresh_join_aggregate_iceberg_mv` 走的是同一个 `apply_iceberg_aggregate_delta_chunks`，因此 join aggregate 自动受益。**唯一需要额外注意的是 dim-side 的 delta 已经在 `execute_join_aggregate_delta_branch` 阶段被翻译成 signed aggregate delta chunks（带 group key + 状态列 + retract sign），所以 dim-side update 导致的 group partition 移动会同时贡献"旧 partition 的 retract row"和"新 partition 的 append row"两个 delta 物理行，derivation 自动把两端 partition 都纳入** `AffectedAggregateTargetPartitions::Known`。这是 §5 第 3 条原则在 join aggregate 上的具体体现，不需要新增 dim-side 专用代码路径。

### 7.6 `RefreshPlan.affected_partitions` 暂不接入 aggregate path

aggregate / join aggregate 现在依然把 `RefreshPlan.affected_partitions` 填成 `Unknown("aggregate MV affected partition planning is not wired")`。原因：

- `RefreshPlan.affected_partitions` 当前面向 base-file metadata（PR2），其语义与 aggregate delta derivation 不同（前者描述"哪些 target partition 可能被 refresh 写入"，后者描述"哪些 target partition 必须被读取/裁剪"）。强行让 aggregate path 填回去会污染 planner 输入语义。
- 后续若需要在 planner 层就给出 aggregate affected partition（例如做 partition lifecycle 时），再设计独立字段，不与本任务耦合。

## 8. 数据流

### 8.1 partitioned aggregate MV（single base）

```text
base delta files
  -> base delta scan (existing)
  -> SQL: project visible group key + signed agg state delta + __row_id__
  -> delta_chunks: Chunk[ row_id, group key cols..., visible agg cols..., __agg_state_* cols ]

derive_from_aggregate_delta(layout, schema_contract, delta_chunks):
  for each partition field:
    output_index   = visible_columns.find(target_field_id)
    base_field_id  = output.columns[output_index].expression.referenced_base_field_ids[0]
    arrow_input    = delta_chunk.column(group_key_source_indexes[output_index])
    transform_fn   = iceberg::transform::create_transform_function(&iceberg_transform)
    arrow_output   = transform_fn.transform(arrow_input)
  for each delta row:
    key = MvPartitionKey { spec_id, fields: zip(field_name, value_string) }
  return Known { partitions: BTreeSet<MvPartitionKey> }

apply path:
  touched_row_ids = delta_row_ids(layout, delta_chunks)
  partition_filter = AllowList(partitions)
  (old_chunks, stats) = load_touched_aggregate_target_state(
        target_table, layout, touched_row_ids, partition_filter)
  merge = merge_aggregate_target_state(layout, old_chunks, delta_chunks)
  delete_row_ids = merge.delete_row_ids
  insert_chunks  = merge.insert_chunks
  delete_groups = locate_target_rows_by_string_apply_key(
        target_table, apply_key_col,
        delete_row_ids,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter)
  data_files = write_chunks_as_iceberg_data_files(target_table, insert_chunks)
  commit_iceberg_mv_apply_with_ref(...)
```

### 8.2 partitioned join aggregate MV

差异点：
- delta_chunks 来自 `execute_join_aggregate_delta_branch` 的左侧/右侧 branch，已经合并成 signed aggregate delta。
- dim-side update 在 SQL 层就被展开成"对旧 group 的 retract row + 对新 group 的 append row"（aggregate delta convention）；二者各自带不同 group key，因此 derive_from_aggregate_delta 自动产出包含两端 partition 的集合。
- 不需要 dim-side 专用 detect / merge path。`AffectedAggregateTargetPartitions::Known` 中的 partition 集合是新旧的并集，无需区分。

### 8.3 non-partitioned MV fallback

`schema_contract.target.partition.is_none()` 时：

- derivation 直接返回 `Unpartitioned`，不读取 delta；
- `TargetPartitionFilter::None` 让 lookup / locator 走 full target scan；
- row-id filter 仍然在 batch 层面生效；
- observability 字段 `partition_filter = "none"`，`planned_file_count == pruned_file_count`。

## 9. fallback / error handling 边界

本任务采用**严格 fail-fast**：所有 derivation / mapping / lookup 阶段无法证明的状态都让整次 refresh 失败，错误信息必须包含 mv id 与 target fqn 便于定位。non-partitioned MV 是合法状态（contract 显示无 partition），不算 derivation 失败。

错误清单与触发位置：

| 编号 | 触发位置 | 现象 | 错误来源 |
|---|---|---|---|
| E1 | derivation | target partition contract 缺失但 layout 真的有 partition（spec evolution drift） | `AffectedPartitionError::ContractMissing` |
| E2 | derivation / mapping | transform 是 Void 或未来扩展 transform 但 mapping 未覆盖 | `TransformUnsupported` |
| E3 | derivation | partition field 引用的 output column 不在 layout.visible_columns / 不是 group key / 表达式不是纯 column | `OutputLineageNotPureColumn` |
| E4 | derivation | delta chunk 中缺少 group key 列 / 类型与 Iceberg transform 期望类型不匹配 | `GroupKeyColumnMissing` / `GroupKeyTypeMismatch` |
| E5 | lookup | derivation 给出 partition 空集合但 touched_row_ids 非空 | `String`: `"aggregate target lookup: empty partition allow-list with non-empty touched groups"` |
| E6 | lookup | `validate_physical_aggregate_schema` 失败 | 原有错误 |
| E7 | locator | `locate_target_rows_by_string_apply_key` 不能为所有 delete_row_ids 找到 target row | 现有 `"iceberg MV target row not found for apply key ..."` |
| E8 | locator | target snapshot 包含 equality deletes（compaction 未跑） | 现有 `"target row locator cannot apply on a target snapshot with equality deletes"` |
| E9 | mapping | manifest transform 文本与 contract transform enum 不匹配（spec drift） | `"MV partition field X file metadata transform Y mismatches contract Z"` |

E1 / E2 / E3 / E4 / E9 都属于 contract 与 metadata 错位，应由 DDL 端 guard，本任务不修复其根因，仅保证 refresh 不会"按错误的 partition 集合做剪枝"。E5 是 derivation 与 row id 内部一致性的最后一道兜底，正常情况下不会触发；触发即代码 bug 或 IcebergChangeBatch 与 delta SQL 输出脱节，必须 fail。

阈值 / 软退化策略**不在本任务实现**。如果未来 touched group / partition 数量过大需要兜底，应在后续单独 follow-up 中加入"超过阈值则跳过 partition pruning 退化为 row-id-only 的 full scan + filter"模式，并由 config 控制。本任务先保持简单。

## 10. transform 覆盖与 normalization

支持范围：**identity, year, month, day, hour, bucket(N), truncate(W)**。`Void` 与 `Unknown` 报 unsupported。

normalization 规则（client-side derivation 和 file-metadata mapping 共用）：

| Transform | Iceberg-rust 输出 Arrow 类型 | 序列化为 `MvPartitionValue` |
|---|---|---|
| Identity | 原 schema 类型 | 同 PR2 `change_partition_value_string`：primitive Display；NULL → `Null` |
| Year | Int32（years since 1970） | 整数十进制字符串 |
| Month | Int32（months since 1970-01） | 整数十进制字符串 |
| Day | Int32（days since 1970-01-01） | 整数十进制字符串 |
| Hour | Int32（hours since 1970-01-01T00） | 整数十进制字符串 |
| Bucket(N) | Int32 | 整数十进制字符串 |
| Truncate(W) | 原 type | primitive Display |

NULL 处理：Iceberg transform function 对 NULL 输入返回 NULL，序列化为 `MvPartitionValue::Null`。

实现位置：normalization 收敛在 `src/connector/iceberg/changes.rs::change_partition_value_string`（PR2 已有）；client-side derivation 增加一个 `arrow_value_to_partition_value(array: &dyn Array, row: usize) -> Result<MvPartitionValue, AffectedPartitionError>` helper，直接复用同一个序列化分支表（year/month/day/hour/bucket = Int32 → decimal；truncate / identity = 走类型分发）。两条 path 共享同一组 unit test fixtures（见 §12.1.4）。

manifest 端的 transform 文本与 contract enum 的对应关系（PR2 `change_partition_transform_name` + `mv_partition_transform_contract`）：

| Contract enum | Manifest 文本 |
|---|---|
| Identity | `identity` |
| Year | `year` |
| Month | `month` |
| Day | `day` |
| Hour | `hour` |
| Bucket { num_buckets } | `bucket[{num_buckets}]` |
| Truncate { width } | `truncate[{width}]` |
| Void | `void` |

`mapping.rs` 比对 transform 时按 enum 等价，不依赖文本相等（避免 `bucket(N)` vs `bucket[N]` 写法不一致引起的 false negative）。

## 11. Observability

apply path 增加一条结构化 `tracing::info!`，字段：

```text
event = "iceberg_aggregate_mv.apply"
mv_id, target_fqn
partition_filter            // "none" | "allow_list"
affected_partition_count    // usize
touched_group_count         // usize
planned_file_count          // from AggregateStateLookupStats
pruned_file_count           // 实际进入 scan 的 file 数
scanned_target_row_count    // batch 总行数（pre row-id filter）
matched_target_row_count    // post row-id filter 行数
delete_row_count            // merge.delete_row_ids.len()
insert_chunk_row_count      // sum(insert_chunks.batch.num_rows())
new_total_rows
iceberg_snapshot
```

derivation 失败的错误信息（fail-fast 路径）单独 `tracing::error!` 一条：

```text
event = "iceberg_aggregate_mv.partition_derivation_failed"
mv_id, target_fqn
field, transform, reason
```

SQL test 通过 `RUST_LOG=info`（或 fixture 提供的 log capture）抓取这两条事件做断言。

## 12. 测试计划

### 12.1 单元测试（`cargo test --lib`）

12.1.1 `engine::mv::partition::aggregate_delta` 新建 module-level test：
- identity transform，single group key，单个 delta chunk → Known 包含 1 个 partition。
- day transform on timestamp，跨两天的 delta chunk → Known 包含 2 个 partition；transformed 值用 Int32 十进制字符串断言。
- bucket(8) transform → Known 中 partition value 是 `0..7` 区间的整数字符串，断言 Iceberg 与 client-side 输出一致（feeding 同样 Arrow column 进 transform fn）。
- contract 无 partition → `Unpartitioned`。
- partition contract 引用不存在的 target field id → `Err(ContractMissing)`.
- output 表达式不是纯 column → `Err(OutputLineageNotPureColumn)`.
- delta chunk 缺少 group key 列 → `Err(GroupKeyColumnMissing)`.
- transform = void → `Err(TransformUnsupported)`.

12.1.2 `engine::mv::partition::mapping` 扩展：
- year / month / day / hour / bucket / truncate 各一个 happy path。
- manifest transform 文本与 contract enum 不匹配 → `Err`。

12.1.3 `engine::mv::iceberg_aggregate_state::load_touched_aggregate_target_state`：
- 在内存 Iceberg memory catalog 上准备 2 个 partition（`region=a`, `region=b`）的 target，touched_row_ids 仅指向 a 的某行，AllowList = {a} → `pruned_file_count == 1`，`matched_row_count == 1`。
- partition_filter = None 且 touched_row_ids = {r1, r2} → 走 full file scan 但只匹配 r1, r2。
- partition_filter = AllowList({}) 且 touched_row_ids 非空 → fail with E5。
- touched_row_ids = {} → 立即返回空 chunks，`planned_file_count == 0`。

12.1.4 `connector::iceberg::changes::change_partition_value_string` 与 client-side `arrow_value_to_partition_value` 在相同 transform + 相同输入 Arrow 值上输出相等 string（property test 风格的 6 个 transform × 多个边界值）。

12.1.5 `iceberg_target_apply::locate_target_rows_by_apply_key_impl` 增加 `partition_filter = AllowList(...)` 测试：仅命中 allow list 中 partition 的 PositionDeleteGroup 出现在结果。

### 12.2 Rust 集成测试

- `refresh_iceberg_aggregate_mv` end-to-end：partitioned aggregate MV 上做 incremental insert + update + delete（不含 MIN/MAX），断言：
  - 结果与 base query 对齐；
  - `tracing` 字段 `affected_partition_count` 与预期 partition 集合大小相等；
  - 后续 refresh 在没有变更时短路（metadata-only path）保持不变。
- `refresh_iceberg_join_aggregate_mv`：dim-side 把一行 group key 从 `region=a` 改到 `region=b`，断言：
  - 最终 aggregate 结果正确；
  - `affected_partition_count == 2`，且 partition 集合包含两端；
  - target locator 只触碰 a / b 两个 partition 的 data file。

### 12.3 SQL regression（`sql-tests`）

新增 suite case（推荐落在已有 `iceberg-rest` 套件中扩展，沿用 `docker/iceberg-rest/runtime/current/env.sh` 生成的 NovaRocks standalone server + REST catalog + MinIO）：

12.3.1 partitioned single-base aggregate MV
- 基表按 `region` identity partition。
- MV：`SELECT region, COUNT(*) FROM t GROUP BY region PARTITION BY (region)`。
- 操作序列：first refresh → insert into region=a → refresh → update one row's measure in region=b → refresh → delete one row from region=c → refresh。
- 每次 refresh 后 `SELECT * FROM mv` 与 `SELECT region, COUNT(*) FROM t GROUP BY region` 对比。

12.3.2 partitioned join aggregate MV
- fact `s` partitioned by base column, dim `d`。
- MV：`SELECT d.region, SUM(s.amount) FROM s JOIN d ON s.id = d.id GROUP BY d.region PARTITION BY (region)`。
- 操作：dim-side update 把某 id 的 region 从 a 改到 b → refresh → aggregate 结果应正确把旧 group 减完、新 group 加上。

12.3.3 partitioned aggregate MV with day transform
- 基表带 `ts TIMESTAMP` 列，MV `PARTITION BY (day(ts))`。
- 操作：跨 day 的 insert → refresh → 验证 partition Pruning 命中两个 day。

12.3.4 non-partitioned aggregate MV fallback
- MV 无 `PARTITION BY`。
- 操作：insert / update / delete 各一次。
- 断言：结果正确；tracing 字段 `partition_filter = "none"` 且 `matched_target_row_count <= total target rows`。

### 12.4 失败路径

- 单元：构造 transform=void 的 contract → derivation fail；构造 partition contract 引用非 group output column → derivation fail。
- Rust 集成：手动 patch target table partition spec（spec drift）后跑 refresh → expect Err，错误信息包含 `partition_derivation_failed`。

## 13. PR 切片

四个 PR 按 TODO 推荐顺序排列。每个 PR 自带单测与可独立 land 的契约。

### PR 1 ─ Aggregate target state lookup API + locator partition filter

`src/engine/mv/iceberg_aggregate_state.rs`:
- `pub(crate) fn load_touched_aggregate_target_state(...)`。
- `pub(crate) struct AggregateStateLookupStats { ... }`。
- `pub(crate) enum TargetPartitionFilter { None, AllowList(BTreeSet<MvPartitionKey>) }`（建议放 `src/engine/mv/partition/key.rs` 紧邻 `MvPartitionKey`，导出给 lookup 与 locator 共享）。

`src/engine/mv/iceberg_target_apply.rs`:
- `locate_target_rows_by_apply_key_impl` 增加 `partition_filter: &TargetPartitionFilter`。
- 公共 wrapper `locate_target_rows_by_apply_key` / `locate_target_rows_by_string_apply_key` 追加形参。
- 现有非 aggregate apply 调用方在本 PR 一并迁移到 `TargetPartitionFilter::None`，保留行为不变。

`src/engine/mv/partition/mapping.rs`:
- 扩展到完整一类 transform，确保 manifest 端 partition value 能映射到 `MvPartitionKey`。
- 单元测试覆盖 §12.1.2。

测试：§12.1.3、§12.1.5、§12.1.2。

不接入 `apply_iceberg_aggregate_delta_chunks`；PR 末尾代码可编译，运行行为与现状等价。

### PR 2 ─ Aggregate-delta affected partition derivation

`src/engine/mv/partition/aggregate_delta.rs`:
- `AggregateDeltaPartitionInput`、`AffectedAggregateTargetPartitions`、`AffectedPartitionError`、`derive_from_aggregate_delta`。
- `arrow_value_to_partition_value` helper 与 client-side transform 序列化（§10）。

测试：§12.1.1、§12.1.4。

不调用 lookup API；仅返回 partition 集合并打通单测。

### PR 3 ─ Apply path 接入

`src/engine/mv/iceberg_refresh.rs`:
- `build_aggregate_target_partition_filter` 内部 helper。
- 改写 `apply_iceberg_aggregate_delta_chunks`：替换 `load_current_aggregate_target_state` → `load_touched_aggregate_target_state`；locator 调用追加 partition filter。
- error 信息添加 mv id / target fqn。

测试：§12.2 全部、§12.4 spec drift。

### PR 4 ─ SQL regression + observability

`src/engine/mv/iceberg_refresh.rs`:
- apply path 末尾 `tracing::info!(...)` 结构化字段。
- derivation 失败入口 `tracing::error!(...)`。

`tests/sql-test-runner/.../iceberg-rest/`:
- 新增 §12.3.1 - 12.3.4 四个 case。
- 必要时新增 expected output。

观测断言：在 §12.2 集成测试中（不是 SQL 测试）使用 `tracing_subscriber::fmt::test::TestWriter`（或现有 fixture）抓取 event 字段。

## 14. 风险与缓解

1. **client-side transform 与 manifest transform 不一致**
   - 风险：`change_partition_value_string` 与 `arrow_value_to_partition_value` 序列化偏差会让 file metadata path 与 delta-chunk path 比较得出不重合 partition，导致 lookup 漏行（fail with E5）。
   - 缓解：§12.1.4 property test 强制两个路径在同一 transform + Arrow value 上输出相等 string；序列化规则统一收敛到 §10 表格。

2. **Iceberg-rust transform function 行为变更**
   - 风险：iceberg crate 升级后 transform 输出 Arrow 类型变化。
   - 缓解：normalization 用 `match` Arrow type 而不是字面假设 Int32；transform fail 报 `TransformFailed` 而非 panic；CI 锁版本，crate 升级走单独 PR。

3. **partition spec 漂移**
   - 风险：base / target 的 partition spec id 在生命周期内出现 evolution，contract 仍指向旧 spec。
   - 缓解：本任务保持 PR2 既定行为，`map_file_partition_to_mv_key` 直接以 file spec id 构造 key；contract 与 manifest mismatch 走 E1 / E9 fail。

4. **derivation 与 row id 的内部一致性**
   - 风险：partition 集合空但 touched_row_ids 非空（理论上不该发生）会让 lookup 返回空 → merge 把 delta 误判为新增 group 全量写入。
   - 缓解：E5 显式 fail。

5. **touched group 列表很大**
   - 风险：`BTreeSet<String>` 在百万级 touched group 时内存放大；同时 partition filter 集合也可能很大。
   - 缓解：本任务暂不引入阈值；§12.2 集成测试保留中等规模 fixture（数千 group），数据集级别压测放在后续 perf follow-up。

6. **`locate_target_rows_by_string_apply_key` 与 lookup partition filter 偏差**
   - 风险：两者用不同 partition filter 派生，导致 delete 与 read 不对齐。
   - 缓解：apply path 中只构造一次 `TargetPartitionFilter` 并同时传给两处。

7. **join aggregate dim-side**
   - 风险：dim-side change 在某些 SQL 形态下未在 delta chunk 体现旧/新 group 双行，导致旧 partition 漏掉。
   - 缓解：当前 `execute_join_aggregate_delta_branch` 已经按 signed convention 翻译；§12.2 dim-side update 集成测试做 end-to-end 验证；如果发现 SQL convention 不够，回到 join aggregate SQL 生成阶段补齐，本任务不在 derivation 层兜底。

## 15. 验收标准

1. partitioned aggregate / join aggregate IMV 的增量 refresh 在所有 §12.3 SQL case 上 verify 通过。
2. non-partitioned aggregate IMV 的增量 refresh 仍走 row-id filter，结果与 baseline 一致。
3. partitioned MV 在 touched group < 全量的 case 中，`pruned_file_count < planned_file_count` 且 `matched_target_row_count < scanned_target_row_count`（用 tracing 字段断言）。
4. 任何 derivation / mapping / spec drift 失败导致 refresh 退出 `Err`，错误信息包含 mv id / target fqn / failing field。
5. `RefreshPlan.affected_partitions` 与本任务的 aggregate 路径互不污染。
6. `cargo fmt` / `cargo clippy` / `cargo test --lib` / 相关 SQL suite 全部通过。
7. `load_current_aggregate_target_state` 调用面被 `apply_iceberg_aggregate_delta_chunks` 移除（其他 caller / fixture 可保留）。

## 16. 与 IVM-P1 roadmap 的关系

本任务是 IVM-P1 roadmap 上 "Target locator pruning" 与 "join/aggregate 局部刷新" 的具体落地。它不替代后续的：

- partition lifecycle（drop/replace partition spec）：本任务保持现有 contract guard 行为不变。
- partition full recompute（`MIN/MAX` 与 delete-bearing aggregate）：作为独立 refresh strategy。
- file/position 精确 state row read：本任务结束时仍是 partition-pruned scan + client-side filter；file/position level read 是下一阶段优化。
- row-evaluation fallback（base-file metadata 推不出 partition 时回到 row 读）：本任务不消费该 fallback，因为 derivation 由 delta chunk visible column 驱动，已经具备等价能力。

## 17. 方案对比与推荐

### 17.1 Lookup API 入参形态

| 选项 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| A | `Option<&BTreeSet<MvPartitionKey>>` | 解耦 partition 模块；调用方自己决定如何把三态转成 Option | 调用方需在多处把"没有 partition"与"derivation fail"区分清楚，容易把 Unknown 误转为 None |
| **B（推荐）** | `TargetPartitionFilter { None, AllowList(set) }` | 在类型层把 fail-fast 与"无 partition"区分；locator 与 lookup 共享同一类型 | 多一个新类型 |
| C | 直接传 `AffectedMvPartitions` 三态 | 复用现有 enum | `Unknown` 在 aggregate apply 是非法状态，类型上没禁止；调用方需要在外层做 unwrap |

选 B：把"fail-fast"与"功能性无 partition"区分写进类型，避免新人误用。

### 17.2 Partition derivation 入口

| 选项 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| A | 复用 `partition::planner::plan_affected_partitions` | 单一入口 | 现有 planner 输入是 `IcebergChangeBatch`，与 delta chunk 输入语义不同；强行复用会让 enum / 错误处理混乱 |
| **B（推荐）** | 新增 `partition::aggregate_delta::derive_from_aggregate_delta` | 不同来源不同函数；错误类型可以专用化为 fail-fast | 多一个模块 |
| C | 在 `iceberg_aggregate_state` 模块内做 derivation | 调用近，少跨模块 | 把 partition 派生与 state lookup 紧耦合，单测难写、被 base-file path 复用难 |

选 B：partition 模块统一持有 derivation 知识；aggregate state 模块只关心 lookup。

### 17.3 Apply path 改造粒度

| 选项 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| A | 引入一个 `AggregateApplyContext` 结构包住 layout / contract / partition_filter / stats，把 apply path 重写 | 调用面整洁 | 改面太大，难分 PR；与现有 staging branch / commit / publish 逻辑混在一起 |
| **B（推荐）** | 只在 `apply_iceberg_aggregate_delta_chunks` 内部串新参数，不增加结构 | 改动可逐 PR land；保留现有 commit/publish 流程 | 函数签名稍长，但仍局部 |
| C | 在 `iceberg_aggregate_state` 内做完所有 lookup + locator + merge 调度 | 复杂度集中 | 责任错配：state 模块不应该 own staging branch / commit lifecycle |

选 B：与现有 staging branch / commit / publish 责任划分一致，PR 也更容易切片。

## 18. Spec self-review

- 无 TBD / TODO 占位符；transform 覆盖、错误清单、observability 字段、PR 切片均逐条列出。
- 与现有 partition planner P2 的边界明确：本任务新增独立模块（`aggregate_delta.rs`），不污染既有 `planner.rs` 行为；`RefreshPlan.affected_partitions` 在 aggregate path 保持 `Unknown(reason)`。
- fail-fast 在 §5、§9、§15 三处描述一致；non-partitioned MV 的合法 fallback 与"derivation 失败"在类型 (`TargetPartitionFilter` vs `AffectedPartitionError`) 层就被区分。
- transform normalization 在 §5 / §10 给出共享规则，并通过 §12.1.4 property test 保证两条路径输出一致。
- join aggregate 路径在 §7.5 / §8.2 / §12.3.2 / §14.7 形成闭环：dim-side 移动由 signed delta convention 在 SQL 层翻译，derivation 自动取到新旧两端。
- PR 切片满足"API → derivation → apply 接入 → SQL+obs"四步且每个 PR 自带可验证契约。
