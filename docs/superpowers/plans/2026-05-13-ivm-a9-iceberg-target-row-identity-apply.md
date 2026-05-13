# IVM A9 Iceberg Target Row Identity Apply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 Iceberg-backed projection/filter MV 在 base table delete/update 后不再 fallback full refresh，而是用 base Iceberg v3 `_row_id` 定位 target 行并通过 target Iceberg v3 Puffin DV 精确 apply。

**Architecture:** 当前阶段只支持单 base table、projection/filter MV。`TargetApplyKey = base._row_id`，写入 target 的内部普通 data column `__nova_base_row_id BIGINT NOT NULL`。用户 catalog 永远过滤这个列；refresh 内部用它把 logical delete row 转换成 target `PositionDeleteGroup`，再通过现有 `RowDeltaDvCommit` 在同一个 staging refresh 中完成 delete+append。

**Tech Stack:** Rust, Arrow `RecordBatch`/`Chunk`, Iceberg v3 row-lineage metadata columns, existing NovaRocks standalone SQL analyzer/codegen, Iceberg `RowDeltaDvCommit`, MV staged refresh metadata.

---

## Current Baseline

A4 change-op delta source code已经在当前树中存在：`src/exec/change_op.rs`、`IcebergFileForQuery.change_op`、scan runner 的 `__change_op` 虚拟列、以及 `materialize_changes`。A9 不重新实现 change-op，而是在此基础上补齐 target apply identity。

当前 A9 缺口：

- `create_iceberg_mv` 只用 visible output columns 建 target schema，没有 `__nova_base_row_id`。
- `refresh_iceberg_mv` 遇到 base uuid 变化或 delete-bearing delta 会自动 `rebuild_iceberg_mv`。
- full/first/append refresh 运行原始 `select_sql`，没有把 base `_row_id` 写入 target。
- delete-side reverse projection 没有把 base `_row_id` 带到内部 temp table。
- 没有从 `__nova_base_row_id` 定位 target physical row 并提交 Puffin DV 的 apply sink。

## File Structure

- Create `src/engine/mv/iceberg_target_apply.rs`
  - Owns A9 constants, schema helpers, physical SELECT rewrite, projected-change helpers, target row locator, and apply-key chunk utilities.

- Modify `src/engine/mv/mod.rs`
  - Export the new `iceberg_target_apply` module to sibling MV code.

- Modify `src/meta/repository/mv.rs`
  - Persist target apply-key metadata in `StoredMvDefinition`.

- Modify `src/engine/mv/iceberg_refresh.rs`
  - Create target schema with hidden apply key.
  - Enforce base/target row-lineage guards.
  - Use physical SELECT for full/first/incremental writes.
  - Replace delete-bearing fallback with A9 target apply.
  - Change base uuid mismatch from rebuild to fail-fast.

- Modify `src/connector/iceberg/catalog/backend.rs`
  - Hide NovaRocks MV apply-key columns from user-facing `TableDef.columns` when Iceberg table properties mark the table as an MV target.

- Modify `src/connector/iceberg/changes.rs`
  - Preserve base row-lineage identity on delete-side materialization and tag inserted files with `CHANGE_OP_INSERT`.

- Modify `src/connector/iceberg/scan_deletes.rs`
  - Add row-position-aware reverse projection helpers that append `_row_id`.

- Modify `src/engine/mv_flow.rs`
  - Let delete-side MV temp tables expose `_row_id` as a real internal column so physical MV SQL can alias it into `__nova_base_row_id`.

- Modify `src/engine/query_prep.rs`
  - Preserve inherited `_row_id` / `_last_updated_sequence_number` metadata when stamping `__change_op`.

- Create SQL tests:
  - `sql-tests/iceberg-rest/sql/iceberg_rest_ivm_a9_target_apply.sql`
  - `sql-tests/iceberg-rest/result/iceberg_rest_ivm_a9_target_apply.result`

## Task 1: Add A9 Apply-Key Contract Metadata

**Files:**
- Create: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/mod.rs`
- Modify: `src/meta/repository/mv.rs`

- [ ] **Step 1: Add failing metadata round-trip tests**

Add tests in `src/meta/repository/mv.rs`:

```rust
#[test]
fn mv_target_apply_key_metadata_round_trips() {
    // CreateMvDefinitionRequest carries __nova_base_row_id / field id / BASE_ROW_ID.
    // StoredMvDefinition reload returns exactly the same apply-key metadata.
}

#[test]
fn mv_target_apply_key_defaults_to_none_for_old_records() {
    // JSON without target_apply_key deserializes successfully with None.
}
```

Run:

```bash
cargo test mv_target_apply_key_metadata_round_trips mv_target_apply_key_defaults_to_none_for_old_records
```

Expected now: tests fail before implementation.

- [ ] **Step 2: Define the repository contract**

Add to `src/meta/repository/mv.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetApplyKey {
    pub column_name: String,
    pub field_id: i32,
    pub source: MvTargetApplyKeySource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MvTargetApplyKeySource {
    BaseRowId,
}
```

Extend `StoredMvDefinition` with a backward-compatible field:

```rust
#[serde(default)]
pub target_apply_key: Option<MvTargetApplyKey>,
```

Extend `CreateMvDefinitionRequest` with:

```rust
pub target_apply_key: Option<MvTargetApplyKey>,
```

Wire `create_definition_with_id` to copy it into `StoredMvDefinition`.

- [ ] **Step 3: Add A9 constants and field helpers**

Create `src/engine/mv/iceberg_target_apply.rs`:

```rust
pub(crate) const ICEBERG_MV_APPLY_KEY_COLUMN: &str = "__nova_base_row_id";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID: &str = "base._row_id";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_COLUMN: &str =
    "novarocks.mv.apply-key.column";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_SOURCE: &str =
    "novarocks.mv.apply-key.source";
pub(crate) const ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID: &str =
    "novarocks.mv.apply-key.field-id";

pub(crate) fn apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        data_type: crate::sql::parser::ast::SqlType::BigInt,
        nullable: false,
        aggregation: None,
        default: None,
    }
}
```

Add `pub(crate) mod iceberg_target_apply;` in `src/engine/mv/mod.rs`.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test mv_target_apply_key_metadata_round_trips mv_target_apply_key_defaults_to_none_for_old_records
```

Expected: both tests pass.

## Task 2: Create Target Schema with Hidden Apply Key

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [ ] **Step 1: Add red tests for target schema and catalog hiding**

Add tests near existing Iceberg MV create tests in `src/engine/mv/iceberg_refresh.rs`:

```rust
#[test]
fn iceberg_mv_target_schema_appends_base_row_id_apply_key() {
    // CREATE MV target schema includes visible output columns plus
    // __nova_base_row_id as the last required BIGINT field.
}

#[test]
fn iceberg_mv_target_hides_apply_key_column_from_user_catalog() {
    // register_iceberg_mv_target_in_catalog exposes visible columns only.
    // SELECT * metadata must not contain __nova_base_row_id.
}

#[test]
fn iceberg_mv_rejects_visible_apply_key_name_collision() {
    // SELECT id AS __nova_base_row_id FROM base is rejected at CREATE MV.
}
```

Run:

```bash
cargo test iceberg_mv_target_schema_appends_base_row_id_apply_key iceberg_mv_target_hides_apply_key_column_from_user_catalog iceberg_mv_rejects_visible_apply_key_name_collision
```

Expected now: tests fail.

- [ ] **Step 2: Append apply-key column during CREATE MV**

In `create_iceberg_mv`, after mapping `analysis.output_columns`, reject any output column whose name equals `ICEBERG_MV_APPLY_KEY_COLUMN` case-insensitively. Then append `apply_key_table_column()` to the physical `columns` passed to `create_table`.

Set target table properties:

```rust
(
    ICEBERG_MV_PROP_APPLY_KEY_COLUMN.to_string(),
    ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
),
(
    ICEBERG_MV_PROP_APPLY_KEY_SOURCE.to_string(),
    ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID.to_string(),
),
(
    ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID.to_string(),
    expected_apply_key_field_id.to_string(),
),
```

`expected_apply_key_field_id` is `analysis.output_columns.len() + 1`, matching the current sequential top-level Iceberg schema builder. After creation, reload the target table and validate the actual Iceberg schema field id before persisting repository metadata.

- [ ] **Step 3: Hide apply-key columns on user-facing catalog load**

In `src/connector/iceberg/catalog/backend.rs`, add a helper:

```rust
fn hide_novarocks_mv_apply_key_columns(
    metadata: &iceberg::spec::TableMetadata,
    columns: Vec<ColumnDef>,
) -> Result<Vec<ColumnDef>, String>
```

Behavior:

- If `novarocks.mv.apply-key.column` is absent, return columns unchanged.
- If present, remove exactly one matching top-level column.
- If the property is present but the column is absent, return an error because the MV target schema is corrupted.
- Do not remove Iceberg reserved `_row_id`; only remove the ordinary data column named by the NovaRocks property.

Call it before constructing `TableDef { columns, ... }`.

- [ ] **Step 4: Persist target apply-key metadata**

When creating `CreateMvDefinitionRequest`, fill:

```rust
target_apply_key: Some(MvTargetApplyKey {
    column_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
    field_id: actual_apply_key_field_id,
    source: MvTargetApplyKeySource::BaseRowId,
}),
```

- [ ] **Step 5: Verify**

Run:

```bash
cargo test iceberg_mv_target_schema_appends_base_row_id_apply_key iceberg_mv_target_hides_apply_key_column_from_user_catalog iceberg_mv_rejects_visible_apply_key_name_collision
```

Expected: all pass.

## Task 3: Enforce Base and Target Refresh Guards

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add red guard tests**

Add tests:

```rust
#[test]
fn create_iceberg_mv_rejects_v2_base_for_a9() {
    // Error contains "Iceberg v3 with write.row-lineage=true".
}

#[test]
fn create_iceberg_mv_rejects_v3_base_without_row_lineage_property() {
    // A9 requires explicit write.row-lineage=true.
}

#[test]
fn refresh_iceberg_mv_rejects_base_uuid_change_without_rebuild() {
    // Existing code rebuilds; A9 must return an error instead.
}

#[test]
fn refresh_iceberg_mv_rejects_target_apply_key_schema_mismatch() {
    // Missing column, non-BIGINT column, or field-id mismatch fail fast.
}
```

Run:

```bash
cargo test create_iceberg_mv_rejects_v2_base_for_a9 create_iceberg_mv_rejects_v3_base_without_row_lineage_property refresh_iceberg_mv_rejects_base_uuid_change_without_rebuild refresh_iceberg_mv_rejects_target_apply_key_schema_mismatch
```

Expected now: tests fail.

- [ ] **Step 2: Add guard helpers**

In `iceberg_target_apply.rs`, add:

```rust
pub(crate) fn ensure_base_row_lineage_contract(
    table: &iceberg::table::Table,
    base_fqn: &str,
) -> Result<(), String>

pub(crate) fn ensure_target_apply_key_contract(
    table: &iceberg::table::Table,
    expected: &crate::meta::repository::mv::MvTargetApplyKey,
) -> Result<(), String>
```

Base contract:

- `table.metadata().format_version() == FormatVersion::V3`
- `write.row-lineage` property equals `true` case-insensitively
- error text: `iceberg MV incremental refresh requires base table <fqn> to be Iceberg v3 with write.row-lineage=true`

Target contract:

- target is format-version 3
- target has `write.row-lineage=true`
- target schema has top-level `__nova_base_row_id`
- type is required BIGINT / Iceberg long
- field id equals repository metadata

- [ ] **Step 3: Apply guards in CREATE and REFRESH**

In `create_iceberg_mv`, after loading the single base table, call `ensure_base_row_lineage_contract`. A9 still keeps the existing optional PRIMARY KEY validation for compatibility, but A9 correctness does not depend on it.

In `refresh_iceberg_mv`:

- call base guard after loading the base table.
- call target guard after reloading the target table and before planning deltas.
- replace the current base uuid mismatch rebuild path with:

```rust
return Err(format!(
    "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
    base_ref.fqn()
));
```

Do not begin a staged refresh intent for this error.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test create_iceberg_mv_rejects_v2_base_for_a9 create_iceberg_mv_rejects_v3_base_without_row_lineage_property refresh_iceberg_mv_rejects_base_uuid_change_without_rebuild refresh_iceberg_mv_rejects_target_apply_key_schema_mismatch
```

Expected: all pass.

## Task 4: Generate Physical MV SELECT with Base `_row_id`

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add red SQL rewrite tests**

Add tests:

```rust
#[test]
fn iceberg_mv_physical_select_appends_base_row_id() {
    // input:  SELECT id, amount FROM ice.ns.orders WHERE amount > 0
    // output: SELECT id, amount, _row_id AS __nova_base_row_id FROM ...
}

#[test]
fn iceberg_mv_physical_select_preserves_projection_filter_shape() {
    // WHERE, aliases, casts, and ORDER-independent output column order remain stable.
}

#[test]
fn iceberg_mv_physical_select_rejects_star_projection() {
    // A9 requires explicit output columns so hidden apply key order is stable.
}
```

Run:

```bash
cargo test iceberg_mv_physical_select_appends_base_row_id iceberg_mv_physical_select_preserves_projection_filter_shape iceberg_mv_physical_select_rejects_star_projection
```

Expected now: tests fail.

- [ ] **Step 2: Implement physical SELECT helper**

Add:

```rust
pub(crate) fn iceberg_mv_physical_select_sql(select_sql: &str) -> Result<String, String>
```

Implementation rules:

- Parse with the existing NovaRocks SQL parser.
- Require a `SELECT` query whose select list has explicit expressions only.
- Append `SelectItem::ExprWithAlias { expr: Expr::Identifier("_row_id"), alias: "__nova_base_row_id" }` as the final item.
- Use unqualified `_row_id`; the current A9 shape has exactly one base table, and this avoids alias-resolution drift when users alias the base relation.
- Reject visible output column collision with `__nova_base_row_id`.
- Return normalized SQL from the AST.

- [ ] **Step 3: Use physical SELECT in target data writes**

Replace calls to:

```rust
run_mv_full_select_chunks(state, current_database, &mv_definition.select_sql)
```

with:

```rust
let physical_sql = iceberg_mv_physical_select_sql(&mv_definition.select_sql)?;
run_mv_full_select_chunks(state, current_database, &physical_sql)
```

in both `first_refresh_iceberg_mv` and `rebuild_iceberg_mv`.

For insert-only incremental refresh, pass the same `physical_sql` into `execute_query_for_mv_incremental_refresh`.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test iceberg_mv_physical_select_appends_base_row_id iceberg_mv_physical_select_preserves_projection_filter_shape iceberg_mv_physical_select_rejects_star_projection
```

Expected: all pass.

## Task 5: Preserve Base `_row_id` in Delta Materialization

**Files:**
- Modify: `src/engine/query_prep.rs`
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/iceberg/scan_deletes.rs`
- Modify: `src/engine/mv_flow.rs`

- [ ] **Step 1: Add red tests for insert-side metadata preservation**

Update existing `query_prep` tests:

```rust
#[test]
fn delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op() {
    // Existing _row_id / _last_updated_sequence_number metadata remains,
    // and __change_op is appended once.
}
```

Run:

```bash
cargo test delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op
```

Expected now: fails because `stamp_delta_table_def_change_ops` clears row-lineage metadata.

- [ ] **Step 2: Preserve inherited row-lineage metadata**

In `stamp_delta_table_def_change_ops`, remove only an existing `__change_op` conflict; do not clear `_file`, `_pos`, `_row_id`, or `_last_updated_sequence_number`. Append `change_op_field()` to the metadata column list after validating no name conflict.

In `materialize_changes` and `incremental_refresh_iceberg_mv`, set insert files to:

```rust
change_op: Some(crate::exec::change_op::CHANGE_OP_INSERT)
```

- [ ] **Step 3: Add red tests for delete-side row ids**

Add tests:

```rust
#[test]
fn position_delete_reverse_projection_appends_base_row_id() {
    // first_row_id=100, positions [2, 4] -> _row_id [102, 104].
}

#[test]
fn deleted_data_file_reverse_projection_appends_base_row_id_sequence() {
    // whole deleted data file first_row_id=200, three rows -> _row_id [200, 201, 202].
}

#[test]
fn equality_delete_reverse_projection_appends_matching_base_row_ids() {
    // matching rows keep first_row_id + original file position.
}

#[test]
fn mv_delete_temp_table_exposes_row_id_as_internal_column() {
    // execute_query_for_mv_incremental_deletes can resolve SELECT _row_id AS __nova_base_row_id.
}
```

Run:

```bash
cargo test position_delete_reverse_projection_appends_base_row_id deleted_data_file_reverse_projection_appends_base_row_id_sequence equality_delete_reverse_projection_appends_matching_base_row_ids mv_delete_temp_table_exposes_row_id_as_internal_column
```

Expected now: tests fail.

- [ ] **Step 4: Add row-id enriched reverse projection**

In `scan_deletes.rs`, add helpers that return batches with an appended `_row_id BIGINT NOT NULL` column:

```rust
pub(crate) fn append_base_row_id_column(
    batch: &RecordBatch,
    first_row_id: i64,
    positions: &[u64],
) -> Result<RecordBatch, ChangeError>
```

Rules:

- `positions.len() == batch.num_rows()`.
- every row id is `first_row_id + position`.
- reject missing `first_row_id` for A9 with an action-oriented error.
- preserve existing fields and append `_row_id` as the last field.

Extend position-delete reverse projection to look up the referenced data file's `first_row_id` from the base table's manifest data-file index, then call `append_base_row_id_column`.

Extend deleted-data-file reverse projection to use `DeletedDataFileRef.first_row_id` and row positions `0..num_rows`.

Extend equality-delete reverse projection to keep original row positions for matched rows and append `_row_id` from the matching data file's `first_row_id + position`. Equality-delete support is part of A9; do not silently fallback rebuild for equality-delete deltas.

- [ ] **Step 5: Register delete temp table from batch schema**

In `execute_query_for_mv_incremental_deletes`, stop rebuilding the temp table through `build_iceberg_table_def_with_files` for A9 delete materialization. Instead, build a local internal `TableDef` from the deleted-row `RecordBatch` schema:

- `columns` are all batch schema fields, including `_row_id`.
- `iceberg_row_lineage_metadata_columns` is empty because `_row_id` is a real temp column here.
- `storage` points to the temp parquet path.

This lets physical SQL resolve unqualified `_row_id` in the delete branch without exposing target `__nova_base_row_id` to users.

- [ ] **Step 6: Verify**

Run:

```bash
cargo test delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op
cargo test position_delete_reverse_projection_appends_base_row_id deleted_data_file_reverse_projection_appends_base_row_id_sequence equality_delete_reverse_projection_appends_matching_base_row_ids mv_delete_temp_table_exposes_row_id_as_internal_column
```

Expected: all pass.

## Task 6: Materialize A9 Projected Changes

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add red tests for projected change chunks**

Add tests:

```rust
#[test]
fn projected_changes_split_insert_and_delete_apply_keys() {
    // insert branch returns physical target chunks with __nova_base_row_id.
    // delete branch returns only keys needed for target locate.
}

#[test]
fn projected_filter_delete_without_old_match_produces_no_delete_key() {
    // old row failing filter yields no target delete.
}

#[test]
fn projected_filter_update_enter_leave_and_replace_are_distinct() {
    // enter -> insert only, leave -> delete only, replace -> delete + insert.
}
```

Run:

```bash
cargo test projected_changes_split_insert_and_delete_apply_keys projected_filter_delete_without_old_match_produces_no_delete_key projected_filter_update_enter_leave_and_replace_are_distinct
```

Expected now: tests fail.

- [ ] **Step 2: Add A9 projected-change types**

In `iceberg_target_apply.rs`:

```rust
pub(crate) struct ProjectedIcebergMvChanges {
    pub(crate) insert_chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) delete_base_row_ids: Vec<i64>,
    pub(crate) projected_row_count: i64,
}
```

Add helpers:

```rust
pub(crate) fn extract_apply_key_values_from_chunks(
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<i64>, String>

pub(crate) fn remove_apply_key_column_from_chunks(
    chunks: Vec<crate::exec::chunk::Chunk>,
) -> Result<Vec<crate::exec::chunk::Chunk>, String>
```

For inserts, keep `__nova_base_row_id` in the chunks because target physical data files need it. For deletes, extract key values and discard visible projection payload.

- [ ] **Step 3: Wire materialization**

In `incremental_refresh_iceberg_mv`:

- Plan change batch.
- Use `iceberg_mv_physical_select_sql`.
- Use insert branch for added files.
- Use delete branch for position/equality/deleted-data-file changes.
- Convert query results to chunks.
- Build `ProjectedIcebergMvChanges`.

Delete-bearing deltas no longer call `rebuild_iceberg_mv`.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test projected_changes_split_insert_and_delete_apply_keys projected_filter_delete_without_old_match_produces_no_delete_key projected_filter_update_enter_leave_and_replace_are_distinct
```

Expected: all pass.

## Task 7: Implement Target Row Locator

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add red locator tests**

Add tests:

```rust
#[test]
fn target_locator_maps_apply_key_to_one_position_delete_group() {
    // one live target row for key 7 -> one PositionDeleteGroup with one position.
}

#[test]
fn target_locator_rejects_missing_apply_key() {
    // delete key not found is corruption, not noop.
}

#[test]
fn target_locator_rejects_duplicate_apply_key() {
    // two live target rows with same __nova_base_row_id fail fast.
}

#[test]
fn target_locator_uses_live_target_rows_only() {
    // rows already deleted by target delete files are ignored by the scan.
}
```

Run:

```bash
cargo test target_locator_maps_apply_key_to_one_position_delete_group target_locator_rejects_missing_apply_key target_locator_rejects_duplicate_apply_key target_locator_uses_live_target_rows_only
```

Expected now: tests fail.

- [ ] **Step 2: Add an internal target scan loader**

Add a helper that builds an internal `TableDef` for the target snapshot with physical columns included, including `__nova_base_row_id`. This helper is only used by the locator; user-facing catalog loading remains hidden by Task 2.

The internal scan SQL shape is:

```sql
SELECT _file, _pos, __nova_base_row_id
FROM <internal_target_table>
```

The implementation may scan the live target snapshot and filter the requested key set in Rust. This keeps the first A9 implementation correct and avoids generating huge SQL `IN (...)` lists. Future optimization can push the key set into scan predicates without changing the locator contract.

- [ ] **Step 3: Convert live rows to delete groups**

Implement:

```rust
pub(crate) fn locate_target_rows_by_apply_key(
    state: &Arc<StandaloneState>,
    current_database: &str,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String>
```

Rules:

- Empty input returns empty groups.
- Each requested key must match exactly one live row.
- 0 matches returns `iceberg MV target row not found for base row id <id>`.
- More than 1 match returns `iceberg MV target has duplicate rows for base row id <id>`.
- Group output by referenced data file and partition metadata.
- Use target `_file` and `_pos` from Iceberg row-lineage metadata columns, not target reserved `_row_id`.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test target_locator_maps_apply_key_to_one_position_delete_group target_locator_rejects_missing_apply_key target_locator_rejects_duplicate_apply_key target_locator_uses_live_target_rows_only
```

Expected: all pass.

## Task 8: Commit Apply Changes Through FastAppend or RowDeltaDv

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [ ] **Step 1: Add red apply sink tests**

Add tests:

```rust
#[test]
fn apply_sink_insert_only_uses_fast_append_and_preserves_apply_key() {
    // target data file contains visible columns plus __nova_base_row_id.
}

#[test]
fn apply_sink_delete_only_uses_row_delta_dv() {
    // no data files, delete groups injected, Puffin DV commit action used.
}

#[test]
fn apply_sink_mixed_update_commits_deletes_and_appends_together() {
    // one RowDeltaDv refresh commit contains delete groups and new data files.
}

#[test]
fn apply_sink_empty_delta_finalizes_without_new_target_snapshot() {
    // no insert chunks and no delete keys advances base lineage only.
}
```

Run:

```bash
cargo test apply_sink_insert_only_uses_fast_append_and_preserves_apply_key apply_sink_delete_only_uses_row_delta_dv apply_sink_mixed_update_commits_deletes_and_appends_together apply_sink_empty_delta_finalizes_without_new_target_snapshot
```

Expected now: tests fail.

- [ ] **Step 2: Add a mixed apply commit helper**

Extend or add:

```rust
async fn commit_iceberg_mv_apply_with_ref(
    table: &iceberg::table::Table,
    catalog: &Arc<dyn iceberg::Catalog>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    ident: &iceberg::TableIdent,
    data_files: Vec<iceberg::spec::DataFile>,
    delete_groups: Vec<crate::connector::iceberg::commit::PositionDeleteGroup>,
    target_ref: &str,
    snapshot_properties: BTreeMap<String, String>,
) -> Result<CommitOutcome, String>
```

Rules:

- If `delete_groups.is_empty()`, use existing `CommitOpKind::FastAppend`.
- If delete groups exist, create an `IcebergCommitCollector` with `CommitOpKind::RowDeltaDv`.
- Inject `data_files` as written data files and `delete_groups` as delete groups.
- `RowDeltaDvCommit` already accepts data files and rejects non-data content; keep that invariant.
- Preserve the existing branch commit recovery logic from `commit_iceberg_mv_target_files_with_ref`.

- [ ] **Step 3: Replace incremental refresh fallback**

In `incremental_refresh_iceberg_mv`:

- Begin staged refresh intent after the projected change result is known to require a target commit.
- For insert chunks, call `write_chunks_as_iceberg_data_files`.
- For delete keys, call `locate_target_rows_by_apply_key`.
- Commit through `commit_iceberg_mv_apply_with_ref`.
- Publish staging branch, drop branch, and finalize repository metadata using the same metadata flow as first/rebuild refresh.

No target snapshot is written when projected inserts and delete keys are both empty; finalize base snapshot only.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test apply_sink_insert_only_uses_fast_append_and_preserves_apply_key apply_sink_delete_only_uses_row_delta_dv apply_sink_mixed_update_commits_deletes_and_appends_together apply_sink_empty_delta_finalizes_without_new_target_snapshot
```

Expected: all pass.

## Task 9: Add SQL End-to-End Coverage

**Files:**
- Create: `sql-tests/iceberg-rest/sql/iceberg_rest_ivm_a9_target_apply.sql`
- Create: `sql-tests/iceberg-rest/result/iceberg_rest_ivm_a9_target_apply.result`

- [ ] **Step 1: Prepare the REST fixture**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh || docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Expected: the shared REST/MinIO services are running or reused.

- [ ] **Step 2: Build the debug binary**

Run:

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 3: Start standalone-server with readiness gating**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-ivm-a9.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || {
  echo "timed out waiting for NOVAROCKS_READY" >&2
  kill -9 "$SRV_PID"
  exit 1
}
```

Expected: log contains `NOVAROCKS_READY mysql_port=<port>`.

- [ ] **Step 4: Add the SQL case**

The SQL case must cover:

- Create base Iceberg v3 table with `write.row-lineage=true`.
- Create projection/filter Iceberg-backed MV without `PRIMARY KEY`.
- First refresh populates visible rows.
- `SELECT * FROM mv` does not include `__nova_base_row_id`.
- Explicit `SELECT __nova_base_row_id FROM mv` returns an unknown-column error.
- Base append then refresh appends the new projected row.
- Base delete then refresh removes the target row without full rebuild.
- Base update represented as delete+insert produces replace semantics.
- Filter leave/enter cases are covered.

Use stable ordering in every result query.

- [ ] **Step 5: Record and verify the case**

Record after implementation:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_ivm_a9_target_apply \
  --mode record
```

Verify:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_ivm_a9_target_apply \
  --mode verify
```

Expected: the new case passes.

## Task 10: Full Validation and Commit Hygiene

**Files:** all modified files from Tasks 1-9.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: no formatting diff remains except intentional generated result files.

- [ ] **Step 2: Focused Rust tests**

Run:

```bash
cargo test mv_target_apply_key_metadata_round_trips
cargo test iceberg_mv_target_schema_appends_base_row_id_apply_key
cargo test create_iceberg_mv_rejects_v2_base_for_a9
cargo test iceberg_mv_physical_select_appends_base_row_id
cargo test delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op
cargo test position_delete_reverse_projection_appends_base_row_id
cargo test target_locator_maps_apply_key_to_one_position_delete_group
cargo test apply_sink_mixed_update_commits_deletes_and_appends_together
```

Expected: all pass.

- [ ] **Step 3: Build**

Run:

```bash
cargo build
```

Expected: debug build succeeds.

- [ ] **Step 4: SQL suite target case**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_ivm_a9_target_apply \
  --mode verify
```

Expected: A9 SQL case passes.

- [ ] **Step 5: Regression sweep**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --mode verify
```

Expected: `iceberg-rest` suite passes. Any pre-existing object-store skip must be recorded with the exact skip text.

- [ ] **Step 6: Final diff review**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; status shows only intentional A9 implementation and SQL result files.

## Completion Criteria

- CREATE MV fails fast unless the single base table is Iceberg v3 with `write.row-lineage=true`.
- Target schema contains physical `__nova_base_row_id BIGINT NOT NULL`, and user-facing MV queries do not expose it.
- First refresh, rebuild, append-only incremental, and delete-bearing incremental all fill `__nova_base_row_id`.
- Base uuid mismatch fails fast; it never rebuilds automatically and never becomes part of the target key.
- Delete-bearing deltas use base `_row_id` to locate target live rows and commit Puffin DV through `RowDeltaDvCommit`.
- Missing or duplicate target apply-key matches fail fast.
- Insert-only incremental remains efficient through FastAppend.
- Mixed update commits delete groups and appended data files in one staged refresh publish.
- SQL coverage proves append, delete, update, filter enter/leave, hidden-column behavior, and no PRIMARY KEY requirement.
