# Iceberg IVM row-lineage bug fix design

- 状态：待用户 review
- 日期：2026-05-20
- 范围：Iceberg V3 row-lineage 在 IVM (`IcebergDeltaScan` / CoW UPDATE / Overwrite-snapshot 增量) 路径上的合规性修复
- 非范围：MoR UPDATE 路径；非 join MV 的 IVM；Iceberg V2 表

## 1. 背景

`sql-tests/iceberg-ivm/sql/iceberg_ivm_join_key_update_multiplicity.sql` 在 main 上稳定失败,错误为：

```
ERROR 1105 (HY000): join coalesce multiple pending payloads for key v1:...: inserts=0, deletes=2
```

该 case 测试 join IVM 在 base 表同时存在 join-key 更新 + 非 key 列更新 + 删除 + 插入时的多重性正确性。失败根因不在 join coalesce 自身,而在 NovaRocks 对 Iceberg V3 row-lineage 规范的实现存在三个互相叠加的 bug,导致同一物理 row 在 IVM 的两条 branch 看到不一致的 `_row_id`,致使 join coalesce 无法抵消 +/- pairs。

Iceberg V3 row-lineage 规范明确规定 reader 计算 row_id 时：
1. 优先使用 parquet 文件中 stored `_row_id` 列 (field id = `i32::MAX - 107`) 中非 NULL 的值；
2. 当该列不存在或值为 NULL 时,fallback 到 `manifest entry.first_row_id + row_position` 公式。

iceberg-rust 上游 reader (`vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs:851-877`) 严格按此实现。NovaRocks 在普通 base scan 路径 (`src/exec/operators/scan/runner.rs::synthesize_row_lineage_columns`) 也实现了该规则；但 IVM 增量路径 `IcebergDeltaScan` 没有,造成 IVM delta 与 base scan 看到的 row_id 不一致。

## 2. 三个 bug

### Bug 1：`IcebergDeltaScan` 忽略 stored `_row_id`

代码：`src/exec/operators/iceberg_delta_scan.rs::append_data_file_lineage_columns`

`append_data_file_lineage_columns` 无条件用 `first_row_id + pos` 填充 `_row_id` 与 `_last_updated_sequence_number` 虚拟列,**不查 parquet 文件中可能存在的 stored `_row_id` / `_last_updated_sequence_number` 列**。

V3 row-lineage 规范要求 reader 优先使用 stored 非 NULL 值。`IcebergDeltaScan` 是 NovaRocks IVM 路径上的 reader,违反规范。

后果：CoW UPDATE 写出的 replacement file 的 stored `_row_id` 是从旧 row 继承来的（保持 row identity）,但 `IcebergDeltaScan` 视而不见,改用按物理位置算出的 row_id。同一逻辑 row 在 IVM delta view 和 base snapshot view 的 row_id 不同。

### Bug 2：CoW UPDATE replacement manifest `first_row_id` 选 `min(row_ids)`

代码：`src/connector/iceberg/commit/update_cow.rs::replacement_manifest_first_row_id`

CoW UPDATE 写 replacement file 时,把 manifest entry 的 `first_row_id` 设置为 `min(touched_row_ids)`,目的是让"`first_row_id + pos`"公式与 stored 列在最小 row 上巧合相等。

V3 spec 不要求 manifest `first_row_id` 与 row 实际 row_id 有任何关系（stored 列已决定逐 row 的 row_id）。CoW rewrite 本该使用新分配的 `next_row_id` (因为 row_range.added_rows = 0,实际 next_row_id 不前进,只是给 manifest 一个无碰撞的占位)。

后果：
- 单行 CoW rewrite 时 `min = stored = computed`,Bug 1 表现不出来；
- 多行 CoW rewrite 时 stored 与 computed 必然 mismatch,Bug 1 全面爆发。

该选择本身不违反 V3 spec,但是 misleading：它伪装成"row_id 一致"的保证,实际上一致性该由 stored 列保证。修了 Bug 1 之后,Bug 2 不再造成正确性问题,但仍值得清理以避免后续 reader 实现误以为"first_row_id+pos 等于 stored"。

### Bug 3：CoW UPDATE 把 unchanged rows 也当作 +Insert

代码：`src/connector/iceberg/changes.rs::collect_added_data_files_for_manifest_list` 与 `collect_deleted_data_files_for_manifest_list`（对 `CollectOverwriteDiff` action）

CoW UPDATE 的 commit 语义是"整个文件被替换"：File B 被标记 `Deleted`,File C 标记 `Added`。`IcebergChangeBatch.inserts` 收下 File C 整个 record 集合,`deleted_data_files` 收下 File B 整个 record 集合。

但 File C 中 unchanged rows（仅因 CoW 物理重写而改变文件位置）的 stored `_row_id` 与 File B 中对应 rows 相同。IVM 把这些 unchanged rows 当作"新 insert + 旧 delete",在 Bug 1 修复后能正确通过 coalesce 抵消,但浪费 IO + 计算。

该行为不违反 V3 spec,是 Overwrite 语义自然结果,但 IVM 可以基于 stored row_id 做行级跳过优化。在大 base 表 CoW UPDATE 场景下 IO 节省显著。

## 3. 修复架构

三个 bug 拆为三个独立 commit,每 commit 独立通过测试,独立可回滚。

### 3.1 Commit 1：Bug 1 — `IcebergDeltaScan` 读 stored `_row_id`

#### 新增 helper

新增 `src/connector/iceberg/row_lineage_synth.rs`,包含：

```rust
pub(crate) fn synthesize_row_id(
    schema: &arrow::datatypes::Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: i64,
    positions: Option<&[i64]>,    // None 表示用 0..num_rows
) -> Result<Vec<i64>, String>

pub(crate) fn synthesize_last_updated_sequence_number(
    schema: &arrow::datatypes::Schema,
    columns: &[ArrayRef],
    num_rows: usize,
    data_sequence_number: i64,
) -> Result<Vec<i64>, String>

/// 返回 stored row-lineage 字段在输入 schema 中的索引位置（若存在）。
/// 用于在调用 synthesize_* 之后从输出 batch 中移除冗余 stored 列。
pub(crate) fn stored_row_lineage_indices(
    schema: &arrow::datatypes::Schema,
) -> StoredRowLineageIndices
```

`synthesize_row_id` 行为：
- 通过 field metadata `PARQUET_FIELD_ID_META_KEY` 查找 stored `_row_id` 列（field id = `ICEBERG_RESERVED_FIELD_ID_ROW_ID`）；
- 逐行：stored 列存在且非 NULL → 取 stored 值；否则 `first_row_id + position`；
- 与 iceberg-rust 上游 `RecordBatchTransformer::create_row_id_column` 行为一致。

`synthesize_last_updated_sequence_number` 行为：
- 通过 field id `ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER` 查找 stored 列；
- stored 非 NULL → stored；否则 `data_sequence_number` fallback。

#### 改造 `IcebergDeltaScan::append_data_file_lineage_columns`

- 取代当前直接 `first_row_id + pos` 的计算,调用新 helper 拿到逐行 row_id 与 last_updated_seq；
- 计算完毕后,**从输出 batch 的 fields 中移除 stored `_row_id` 与 stored `_last_updated_sequence_number` 列**（用 `arrow::compute::take` 重建 batch,丢弃这两列）；
- 之后按现有逻辑 push 四个虚拟列（`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`）到 batch 尾部。

输出 batch schema 因此与 `iceberg_row_lineage_metadata_columns`（codegen 期望）保持一致 —— user fields + 4 lineage virtual cols,无重复 `_row_id`。

#### `IcebergDeltaScan::open_deleted_data_file_scanner` 同步改造

`scan_one_deleted_data_file` 走 `scan_deleted_data_file_rows_with_visibility_and_v3_lineage`,内部也可能拼装 `_row_id`。实施步骤：

1. 在实施 Commit 1 之初先 grep `scan_deleted_data_file_rows_with_visibility_and_v3_lineage` 与其调用栈,确认是否同样无条件用 `first_row_id + pos` 计算 `_row_id`。
2. 如是,把该路径同步改用新 helper,作为 Commit 1 的一部分（避免 deleted-side reader 仍违规导致 case 1 仍 fail）。
3. 如不是（已合规或不涉及 row_id 合成）,在 spec 评审环节回写本节说明,无需改造。

该项的具体改/不改决定不影响整体设计,只是 Commit 1 实施范围的边界。

#### 不动 `scan/runner.rs::synthesize_row_lineage_columns`

该路径已经合规。可选地把它重构为调用新 helper（避免两份实现）,但不是本 commit 必需。本 commit 仅保证 IVM 路径合规,普通 scan 不变。

#### 单元测试

新增 `src/connector/iceberg/row_lineage_synth.rs` 内联测试：
- `synthesize_row_id_uses_stored_when_present`
- `synthesize_row_id_falls_back_when_stored_null`
- `synthesize_row_id_falls_back_when_stored_column_absent`
- `synthesize_last_updated_seq_uses_stored_when_present`

`src/exec/operators/iceberg_delta_scan.rs` 内联测试：
- `append_data_file_lineage_columns_removes_stored_columns_from_output`
- `output_batch_schema_matches_iceberg_row_lineage_metadata_columns_order`

### 3.2 Commit 2：Bug 2 — CoW UPDATE 用新分配 first_row_id

#### 改 `replacement_manifest_first_row_id`

把签名从

```rust
fn replacement_manifest_first_row_id(rewrite_file: &CowUpdateTouchedFile) -> Result<u64, String>
```

改为：

```rust
fn replacement_manifest_first_row_id(metadata: &TableMetadata) -> Result<u64, String>
```

实现：返回 `effective_next_row_id(metadata)`,即"如果有 fallback row_id 需要分配,从哪儿开始"。CoW UPDATE 的 `with_row_range(first_row_id, 0)` 不变（snapshot 不实际分配 row,next_row_id 不前进）。stored 列覆盖所有 row → fallback 路径永不触发 → 该值的实际数值无意义,只要无碰撞。

#### `CowUpdateTxnAction::commit` 调用方更新

`update_cow.rs:275-278` 当前调用：

```rust
new_manifests.push(mark_replacement_manifest_row_id_assigned(
    data_manifest,
    replacement_manifest_first_row_id(rewrite_file)?,
));
```

改为：

```rust
new_manifests.push(mark_replacement_manifest_row_id_assigned(
    data_manifest,
    row_lineage_first_row_id,  // 已在该函数顶部从 metadata.next_row_id() 取
));
```

实际上 commit 函数顶部已有 `let row_lineage_first_row_id = m.next_row_id();`,直接复用。

#### 删除现已无用的辅助函数

- `replacement_manifest_first_row_id` 函数本身可移除（或保留为 `effective_next_row_id` 的轻包装,但保留 misleading 命名风险大,建议删）。

#### 单元测试

`src/connector/iceberg/commit/update_cow.rs` 单元测试：
- `multi_row_cow_rewrite_manifest_first_row_id_uses_next_row_id`
- `cow_rewrite_round_trip_preserves_row_id_via_stored_field`（写一次 CoW UPDATE 后,读出来的 row_id 等于 stored 值,与 manifest first_row_id 无关）

### 3.3 Commit 3：Bug 3 — IVM 跳过 CoW unchanged rows

#### 数据结构扩展

`src/connector/iceberg/changes.rs::DataFileRef` 扩展：

```rust
pub(crate) struct DataFileRef {
    // ... 现有字段
    /// IVM-specific: row-id allow list. When Some, the IVM scanner must only
    /// emit rows whose stored `_row_id` (or computed fallback) is in this set.
    /// None means "emit all rows" (current behavior).
    pub row_id_allow_list: Option<BTreeSet<i64>>,
}
```

`IcebergFileForQuery` 同步扩展该字段,在 `build_delta_source_files` 中从 `DataFileRef` propagate。

#### 新增 Overwrite-diff 分析

新增 `src/connector/iceberg/changes.rs::compute_overwrite_unchanged_rows`：

输入：Overwrite snapshot 的 (added_files, deleted_files)。
输出：对每个 added_file,生成 `row_id_allow_list` = added_file 中所有 stored row_id 减去 (在某个 deleted_file 中也出现的 stored row_id)。

具体逻辑：
1. 扫描 deleted_files,收集每个 partition 的 stored row_id 集合（仅扫 stored 列,不读 user data）；
2. 扫描 added_files,逐 row 检查 stored row_id 是否在 deleted 集合中：
   - 在 → 标记为 unchanged → 从 added file 的 allow list 剔除；同时从 deleted file 的"需要 reverse-project 删除"集合中剔除；
   - 不在 → 真新增,保留在 allow list。

仅在 added_file 与 deleted_file **同 partition_spec_id 且同 partition_key** 时配对。partition evolution 跨 spec / 不同 partition_key 的文件对不做 unchanged 识别,保留当前 over-counting 行为（保守正确）。

#### IcebergDeltaScan 消费 allow list

`open_data_file_scanner` 与 `open_deleted_data_file_scanner` 接收 `row_id_allow_list: Option<&BTreeSet<i64>>`。在 batch 输出前用该 set 过滤行：

```rust
if let Some(allow) = row_id_allow_list {
    let row_ids = synthesize_row_id(...);
    let keep_mask: BooleanArray = row_ids.iter()
        .map(|rid| Some(allow.contains(rid)))
        .collect();
    batch = arrow::compute::filter_record_batch(&batch, &keep_mask)?;
}
```

#### 单元测试

`src/connector/iceberg/changes.rs::compute_overwrite_unchanged_rows`：
- `same_stored_row_id_in_added_and_deleted_marked_unchanged`
- `different_partition_key_not_paired`
- `multi_row_file_partial_unchanged_filters_only_unchanged_rows`

`IcebergDeltaScan` 单元测试：
- `delta_scan_respects_row_id_allow_list_for_added_file`

#### SQL 集成 sanity（非性能测试）

不新增 dedicated SQL test。依赖现有 iceberg-ivm 全套通过即可：CoW UPDATE 类 case 正确性不变,case 1 通过验证 unchanged-row 跳过逻辑不破坏 changed-row 处理。

## 4. 测试计划

### 4.1 单元测试

- 各 commit 各自加单元测试如上。三 commit 完成后,执行 `cargo test --lib`,所有现有测试通过。

### 4.2 SQL 集成测试

- 跑 `iceberg-ivm` 全套 32 个 case：
  - 修复前：30 pass + 2 fail（PR #144 之前是 31 pass + 1 fail）
  - 修复后：32 pass + 0 fail
- 跑 `iceberg` 套件的 CoW UPDATE 相关 case：`iceberg_v3_update_cow`、`iceberg_v3_merge_cow`、`iceberg_v3_overwrite_partitions`、`iceberg_v3_remove_orphan_files`,无回归
- 跑 `iceberg-ivm` 的 join 和 aggregate cases：`iceberg_ivm_join_aggregate`、`iceberg_ivm_aggregate_target` 等,无回归

### 4.3 不在范围

- 不做 perf benchmark：Bug 3 在大 base 表 IO 节省的量化由后续独立 perf 任务。

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Bug 1 修复后,IcebergDeltaScan 输出 batch schema 与下游 codegen 期望的 scan tuple 列顺序不匹配 | 新增单元测试 `output_batch_schema_matches_iceberg_row_lineage_metadata_columns_order`；运行 iceberg-ivm 全套作为 schema 合规 sanity |
| Bug 2 改 manifest `first_row_id` 语义影响 Iceberg manifest list 的 row_range 不变性 | 现有 commit 路径已经 `with_row_range(first_row_id, 0)`,added_rows=0 保证 next_row_id 不前进；新增单元测试验证 round-trip read 后 row_id 仍等于 stored |
| Bug 3 的 row_id allow list 在 partition evolution 跨 spec 文件配对场景产生错误结果 | 仅在同 partition_spec_id + 同 partition_key 时做配对,其余 fallback 到当前 over-counting 行为 |
| 三 commit 累积后某 IVM 场景回归 | 每 commit 单独跑全套；最终再统一跑一次 |

## 6. 回滚

- Commit 3 撤：Bug 3 失效,IVM 退回 over-counting（正确性不变）；Bug 1/2 修复保留。
- Commit 2 撤：Bug 2 回到 `min(row_ids)` hack,正确性靠 Bug 1 保证。
- Commit 1 撤：case 1 重新失败。所有 commit 都依赖 Commit 1。

## 7. 成功标准

1. `iceberg_ivm_join_key_update_multiplicity` 在 `iceberg-ivm` suite verify 模式通过。
2. `iceberg-ivm` 全套 32 个 case 32 pass 0 fail（含 PR #144 后新增的 7 个 case）。
3. `iceberg` 套件 CoW UPDATE 相关 case（4-5 个）不回归。
4. 新增 Rust 单元测试全部通过。
5. 三 commit 各自独立通过测试,独立可回滚。
