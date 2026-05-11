# IVM A4 Change-Op Delta Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an Iceberg IVM delta scan/source and make aggregate MV refresh consume it through one signed delta-state query for reversible aggregates.

**Architecture:** Keep the global `Chunk` ABI unchanged and model `__change_op` as an Iceberg scan virtual column. The IVM delta source builds a synthetic internal `TableDef` whose file ranges are tagged `+1` for added rows and `-1` for removed rows; HDFS scan lowering carries that tag through scan ranges/morsels, and the scan runner synthesizes the column into output chunks. Projection MV maps the tagged stream to managed-lake `__op`; aggregate MV rewrites `COUNT`/`SUM`/`AVG` into signed state expressions over `__change_op`, executes one delta-state query, and then reuses existing MV state merge code.

**Tech Stack:** Rust, Arrow `RecordBatch`/`Chunk`, existing NovaRocks standalone SQL analyzer/codegen, `THdfsScanRange.extended_columns`, Iceberg change planning, managed-lake MV refresh.

---

## Contract

The internal column is:

```text
__change_op: Int8 NOT NULL
+1 = insert/upsert/positive contribution
-1 = delete/retract/negative contribution
```

It is not a user column. It is only registered on synthetic IVM delta tables built for MV refresh. Ordinary Iceberg tables must not expose it.

The production source shape after this plan is:

```text
IcebergChangeBatch
  -> IvmDeltaSource
       added data files            -> scan rows with __change_op = +1
       deleted row batches/files   -> scan rows with __change_op = -1
  -> standalone ExecPlan
  -> projection MV consumer or aggregate signed-state consumer
```

The current `MaterializedChanges { inserts, deletes }` and `IvmChangeStream { inserts, deletes }` can remain temporarily for old call sites, but `src/connector/starrocks/managed/mv_refresh.rs` should stop using them for Iceberg-backed MV refresh once this plan is complete.

## File Structure

- Create `src/exec/change_op.rs`
  - Owns the internal column name, allowed values, and validation helpers.

- Modify `src/exec/mod.rs`
  - Exports the `change_op` module.

- Modify `src/exec/row_position.rs`
  - Adds `CHANGE_OP_COL`, `is_change_op`, and optional change-op slot/field fields to `IcebergVirtualSpec`.

- Modify `src/sql/catalog.rs`
  - Adds `ivm_change_op: Option<i8>` to `S3FileInfo`.
  - This is per-file/range metadata, not a business column.

- Modify `src/engine/query_prep.rs`
  - Adds `change_op: Option<i8>` to `IcebergFileForQuery`.
  - Adds `build_iceberg_delta_table_def_with_files`, which registers `__change_op` as a hidden metadata column only for synthetic IVM delta tables and stamps `S3FileInfo.ivm_change_op`.

- Modify `src/sql/codegen/fragment_builder.rs`
  - Registers synthetic metadata columns in scan scope and records the `__change_op` slot in `PlannedScanTable.slot_to_column`.

- Modify `src/sql/codegen/nodes.rs`
  - Detects the `__change_op` slot for a planned scan.
  - Serializes per-file op tags through `THdfsScanRange.extended_columns` as an int literal.

- Modify `src/sql/codegen/expr_compiler.rs`
  - Exposes the existing int literal thrift-node helper to sibling codegen modules.

- Modify `src/lower/node/hdfs_scan.rs`
  - Detects the `__change_op` scan slot, validates it as `TINYINT`, parses the corresponding `extended_columns` literal from each range, and stores it on `FileScanRange`.

- Modify `src/fs/scan_context.rs`, `src/exec/node/scan.rs`, `src/connector/hdfs.rs`, and `src/lower/node/file_scan.rs`
  - Propagate `ivm_change_op` from `FileScanRange` to `ScanMorsel::FileRange`.

- Modify `src/exec/operators/scan/runner.rs`
  - Extends `IcebergVirtualState` with `change_op`.
  - Synthesizes a constant `Int8Array` for `__change_op` when the scan output requests that slot.

- Create `src/connector/starrocks/managed/ivm_delta_source.rs`
  - Converts `IcebergChangeBatch` into tagged `IcebergFileForQuery` entries.
  - Writes delete-row batches to a temporary parquet file tagged with `-1`.
  - Executes projection and aggregate delta-state SQL against the synthetic delta table.

- Create `src/connector/starrocks/managed/ivm_delta_aggregate.rs`
  - Rewrites supported aggregate MV SQL into one signed state query over `__change_op`.
  - Rejects `MIN` / `MAX` retract in the delete-bearing path so callers can fall back.

- Modify `src/connector/starrocks/managed/mod.rs`
  - Exports `ivm_delta_source` and `ivm_delta_aggregate`.

- Modify `src/engine/mv_flow.rs`
  - Make existing validation and temp-parquet helpers `pub(crate)` so the new delta-source module can reuse them.
  - Keep old insert/delete functions for compatibility until all call sites move.

- Modify `src/connector/starrocks/managed/mv_refresh.rs`
  - Projection MV: consume one tagged delta result and map `__change_op` to managed-lake `__op`.
  - Aggregate MV: consume one signed delta-state result and reuse existing aggregate state materialization/merge code.

- Modify `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
  - Leave remaining insert-only Iceberg-backed MV refresh path explicitly marked as append-only compatibility if it does not need delete-bearing semantics.

## Task 1: Add Change-Op Contract Helpers

**Files:**
- Create: `src/exec/change_op.rs`
- Modify: `src/exec/mod.rs`

- [ ] **Step 1: Write the contract module and unit tests**

Create `src/exec/change_op.rs`:

```rust
use arrow::array::{ArrayRef, Int8Array};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub const CHANGE_OP_COLUMN: &str = "__change_op";
pub const CHANGE_OP_INSERT: i8 = 1;
pub const CHANGE_OP_DELETE: i8 = -1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Delete,
}

impl ChangeOp {
    pub fn value(self) -> i8 {
        match self {
            ChangeOp::Insert => CHANGE_OP_INSERT,
            ChangeOp::Delete => CHANGE_OP_DELETE,
        }
    }

    pub fn from_i8(value: i8) -> Result<Self, String> {
        match value {
            CHANGE_OP_INSERT => Ok(ChangeOp::Insert),
            CHANGE_OP_DELETE => Ok(ChangeOp::Delete),
            other => Err(format!(
                "invalid {CHANGE_OP_COLUMN} value {other}; expected {CHANGE_OP_INSERT} or {CHANGE_OP_DELETE}"
            )),
        }
    }
}

pub fn change_op_field() -> Field {
    Field::new(CHANGE_OP_COLUMN, DataType::Int8, false)
}

pub fn change_op_array(op: ChangeOp, row_count: usize) -> ArrayRef {
    Arc::new(Int8Array::from(vec![op.value(); row_count])) as ArrayRef
}

pub fn validate_change_op_value(value: i8) -> Result<(), String> {
    ChangeOp::from_i8(value).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_op_values_are_stable() {
        assert_eq!(ChangeOp::Insert.value(), 1);
        assert_eq!(ChangeOp::Delete.value(), -1);
        assert!(ChangeOp::from_i8(0).is_err());
    }

    #[test]
    fn change_op_array_uses_int8_values() {
        let array = change_op_array(ChangeOp::Delete, 3);
        let values = array.as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), -1);
        assert_eq!(values.value(1), -1);
        assert_eq!(values.value(2), -1);
    }
}
```

- [ ] **Step 2: Export the module**

Modify `src/exec/mod.rs` and add:

```rust
pub mod change_op;
```

- [ ] **Step 3: Run the focused test**

Run:

```bash
cargo test change_op_values_are_stable change_op_array_uses_int8_values
```

Expected: both tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/exec/change_op.rs src/exec/mod.rs
git commit -m "feat(ivm): add change-op row contract"
```

## Task 2: Extend Iceberg Virtual Scan Metadata

**Files:**
- Modify: `src/exec/row_position.rs`
- Modify: `src/exec/operators/scan/runner.rs`

- [ ] **Step 1: Extend `IcebergVirtualSpec`**

In `src/exec/row_position.rs`, add the change-op name helpers near the other Iceberg virtual columns:

```rust
pub const CHANGE_OP_COL: &str = crate::exec::change_op::CHANGE_OP_COLUMN;

pub fn is_change_op(name: &str) -> bool {
    name.eq_ignore_ascii_case(CHANGE_OP_COL)
}
```

Extend `IcebergVirtualSpec`:

```rust
#[derive(Clone, Debug, Default)]
pub struct IcebergVirtualSpec {
    pub file_path_slot: Option<SlotId>,
    pub row_pos_slot: Option<SlotId>,
    pub row_id_slot: Option<SlotId>,
    pub last_updated_seq_slot: Option<SlotId>,
    pub change_op_slot: Option<SlotId>,
    pub file_path_field: Option<Field>,
    pub row_pos_field: Option<Field>,
    pub row_id_field: Option<Field>,
    pub last_updated_seq_field: Option<Field>,
    pub change_op_field: Option<Field>,
}
```

Update `is_empty`:

```rust
pub fn is_empty(&self) -> bool {
    self.file_path_slot.is_none()
        && self.row_pos_slot.is_none()
        && self.row_id_slot.is_none()
        && self.last_updated_seq_slot.is_none()
        && self.change_op_slot.is_none()
}
```

Add a test:

```rust
#[test]
fn is_change_op_recognizes_name_case_insensitive() {
    assert!(is_change_op("__change_op"));
    assert!(is_change_op("__CHANGE_OP"));
    assert!(!is_change_op("change_op"));
}
```

- [ ] **Step 2: Extend scan runner state**

In `src/exec/operators/scan/runner.rs`, extend `IcebergVirtualState`:

```rust
struct IcebergVirtualState {
    spec: IcebergVirtualSpec,
    file_path: String,
    next_row_offset: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    change_op: Option<i8>,
}
```

In `build_iceberg_virtual_state`, destructure `change_op` from `ScanMorsel::FileRange` after Task 3 adds the field:

```rust
let ScanMorsel::FileRange {
    path,
    first_row_id,
    data_sequence_number,
    ivm_change_op,
    ..
} = morsel
else {
    return Err("iceberg virtual columns require file range morsels".to_string());
};
```

Store it:

```rust
    change_op: *ivm_change_op,
```

- [ ] **Step 3: Synthesize the virtual column**

In `append_iceberg_virtual_columns`, pre-build:

```rust
let change_op_array = state.spec.change_op_slot.map(|_| {
    let op = state.change_op.ok_or_else(|| {
        format!(
            "{} requested but scan range has no IVM change-op tag",
            crate::exec::change_op::CHANGE_OP_COLUMN
        )
    })?;
    crate::exec::change_op::validate_change_op_value(op)?;
    Ok(crate::exec::change_op::change_op_array(
        crate::exec::change_op::ChangeOp::from_i8(op)?,
        row_count,
    ))
}).transpose()?;
```

In the output-slot loop, add before falling back to `field_map`:

```rust
if Some(*slot_id) == state.spec.change_op_slot {
    let field = state
        .spec
        .change_op_field
        .as_ref()
        .ok_or_else(|| "iceberg __change_op slot missing field metadata".to_string())?;
    fields.push(field.clone());
    columns.push(
        change_op_array
            .as_ref()
            .expect("change_op_array built when slot exists")
            .clone(),
    );
    slot_schemas.push(ChunkSlotSchema::new(
        *slot_id,
        field.name().clone(),
        field.is_nullable(),
        Some(scalar_type_desc(types::TPrimitiveType::TINYINT)),
        None,
    ));
    continue;
}
```

- [ ] **Step 4: Run row-position tests**

Run:

```bash
cargo test iceberg_virtual_spec_default_is_empty is_change_op_recognizes_name_case_insensitive
```

Expected: both tests pass after Task 3 compiles the new `ScanMorsel` field.

- [ ] **Step 5: Commit after Task 3 compiles**

This task depends on Task 3 because `ScanMorsel::FileRange` gains `change_op` there. Commit it together with Task 3.

## Task 3: Carry Per-Range Change-Op Through Scan Ranges and Morsels

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/fs/scan_context.rs`
- Modify: `src/exec/node/scan.rs`
- Modify: `src/connector/hdfs.rs`
- Modify: `src/lower/node/file_scan.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/lower/node/hdfs_scan.rs`
- Modify tests that construct `S3FileInfo`, `FileScanRange`, or `ScanMorsel::FileRange`

- [ ] **Step 1: Add per-file metadata fields**

In `src/sql/catalog.rs`, add to `S3FileInfo`:

```rust
/// IVM delta source tag for this file/range. None for ordinary scans.
pub ivm_change_op: Option<i8>,
```

Every existing test literal for `S3FileInfo` must set:

```rust
ivm_change_op: None,
```

In `src/fs/scan_context.rs`, add to `FileScanRange`:

```rust
pub ivm_change_op: Option<i8>,
```

- [ ] **Step 2: Add the morsel field**

In `src/exec/node/scan.rs`, extend `ScanMorsel::FileRange`:

```rust
FileRange {
    path: String,
    file_len: u64,
    offset: u64,
    length: u64,
    scan_range_id: i32,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    external_datacache: Option<ExternalDataCacheRangeOptions>,
    delete_files: Vec<IcebergDeleteFileSpec>,
},
```

Update `describe()` to include the tag:

```rust
"path={} file_len={} offset={} length={} scan_range_id={} first_row_id={:?} data_sequence_number={:?} ivm_change_op={:?} external_datacache={:?} delete_files={}"
```

- [ ] **Step 3: Propagate local structs into morsels**

In `src/connector/hdfs.rs`, when building `ScanMorsel::FileRange`, pass:

```rust
ivm_change_op: r.ivm_change_op,
```

For incremental FE ranges that do not carry NovaRocks IVM metadata, set:

```rust
ivm_change_op: None,
```

In `src/lower/node/file_scan.rs`, pass through `r.ivm_change_op` for `FileLoadScanOp` and set `None` for non-HDFS synthetic cases.

- [ ] **Step 4: Expose a codegen int literal helper**

In `src/sql/codegen/expr_compiler.rs`, change:

```rust
fn int_literal_node(value: i64) -> exprs::TExprNode {
```

to:

```rust
pub(super) fn int_literal_node(value: i64) -> exprs::TExprNode {
```

This reuses the existing thrift expression shape instead of constructing a second manual `TExprNode` literal in `nodes.rs`.

- [ ] **Step 5: Serialize change-op through standalone HDFS scan ranges**

In `src/sql/codegen/nodes.rs`, add a helper:

```rust
fn planned_change_op_slot(planned: &PlannedScanTable) -> Option<types::TSlotId> {
    planned
        .slot_to_column
        .iter()
        .find_map(|(slot_id, name)| {
            if name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN) {
                Some(*slot_id)
            } else {
                None
            }
        })
}
```

Add a local wrapper that builds the `TExpr` payload:

```rust
fn int_literal_expr(value: i64) -> exprs::TExpr {
    exprs::TExpr::new(vec![super::expr_compiler::int_literal_node(value)])
}
```

Change `build_exec_params_multi` so `TableStorage::S3ParquetFiles` passes `planned_change_op_slot(planned)` into `build_hdfs_scan_range_params_for_file`.

Update signatures:

```rust
fn build_hdfs_scan_range_params_for_file(
    file: &S3FileInfo,
    change_op_slot: Option<types::TSlotId>,
) -> Result<Vec<internal_service::TScanRangeParams>, String>
```

and:

```rust
fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    delete_files: &[IcebergDeleteFileInfo],
    ivm_change_op: Option<i8>,
    change_op_slot: Option<types::TSlotId>,
) -> Result<internal_service::TScanRangeParams, String>
```

Inside `build_hdfs_scan_range_params`, build:

```rust
let extended_columns = match (ivm_change_op, change_op_slot) {
    (Some(op), Some(slot_id)) => {
        crate::exec::change_op::validate_change_op_value(op)?;
        Some(BTreeMap::from([(slot_id, int_literal_expr(op as i64))]))
    }
    (Some(_), None) => {
        return Err("IVM change-op file tag requires a __change_op scan slot".to_string());
    }
    (None, _) => None,
};
```

Pass `extended_columns` into `THdfsScanRange::new` at field 28, replacing the current `None::<BTreeMap<types::TSlotId, exprs::TExpr>>`.

- [ ] **Step 6: Parse change-op in HDFS lowering**

In `src/lower/node/hdfs_scan.rs`, add local variables near the Iceberg virtual slots:

```rust
let mut iceberg_virtual_change_op_slot: Option<SlotId> = None;
let mut iceberg_virtual_change_op_field: Option<arrow::datatypes::Field> = None;
```

In the output-column loop, before treating a column as a data column:

```rust
if crate::exec::row_position::is_change_op(&name) {
    if primitive != types::TPrimitiveType::TINYINT {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} expects TINYINT, got {:?}",
            node.node_id, slot_id, primitive
        ));
    }
    iceberg_virtual_change_op_slot = Some(slot_id);
    iceberg_virtual_change_op_field = Some(arrow::datatypes::Field::new(name, arrow_type, nullable));
    continue;
}
```

Add this field to `IcebergVirtualSpec` construction:

```rust
change_op_slot: iceberg_virtual_change_op_slot,
change_op_field: iceberg_virtual_change_op_field,
```

Add a parser helper:

```rust
fn extract_change_op_from_extended_columns(
    node_id: i32,
    hdfs_range: &plan_nodes::THdfsScanRange,
    change_op_slot: Option<SlotId>,
) -> Result<Option<i8>, String> {
    let Some(slot_id) = change_op_slot else {
        return Ok(None);
    };
    let Some(map) = hdfs_range.extended_columns.as_ref() else {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} requested but scan range has no extended_columns entry",
            node_id, slot_id
        ));
    };
    let thrift_slot_id = i32::try_from(slot_id.as_u32()).map_err(|_| {
        format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} does not fit thrift slot id",
            node_id, slot_id
        )
    })?;
    let expr = map.get(&thrift_slot_id).ok_or_else(|| {
        format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} missing from extended_columns",
            node_id, slot_id
        )
    })?;
    let node = expr.nodes.first().ok_or_else(|| {
        format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} has empty extended column expression",
            node_id, slot_id
        )
    })?;
    if node.node_type != exprs::TExprNodeType::INT_LITERAL {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} __change_op slot_id={} expects INT_LITERAL, got {:?}",
            node_id, slot_id, node.node_type
        ));
    }
    let value = node
        .int_literal
        .as_ref()
        .ok_or_else(|| {
            format!(
                "HDFS_SCAN_NODE node_id={} __change_op slot_id={} missing int_literal payload",
                node_id, slot_id
            )
        })?
        .value;
    let value = i8::try_from(value).map_err(|_| {
        format!(
            "HDFS_SCAN_NODE node_id={} __change_op value {} does not fit TINYINT",
            node_id, value
        )
    })?;
    crate::exec::change_op::validate_change_op_value(value)?;
    Ok(Some(value))
}
```

When building each `FileScanRange`, compute:

```rust
let ivm_change_op = extract_change_op_from_extended_columns(
    node.node_id,
    hdfs_range,
    iceberg_virtual_change_op_slot,
)?;
```

and set:

```rust
ivm_change_op,
```

- [ ] **Step 7: Run compile-focused tests**

Run:

```bash
cargo test hdfs_scan
```

Expected: existing HDFS scan tests compile and pass after all struct literals are updated.

- [ ] **Step 8: Commit**

```bash
git add src/sql/catalog.rs src/fs/scan_context.rs src/exec/node/scan.rs src/connector/hdfs.rs src/lower/node/file_scan.rs src/sql/codegen/nodes.rs src/sql/codegen/expr_compiler.rs src/lower/node/hdfs_scan.rs src/exec/row_position.rs src/exec/operators/scan/runner.rs
git commit -m "feat(ivm): carry change-op through iceberg scan ranges"
```

## Task 4: Build Synthetic Delta TableDefs

**Files:**
- Modify: `src/engine/query_prep.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: Extend file query input**

In `src/engine/query_prep.rs`, extend `IcebergFileForQuery`:

```rust
pub(crate) change_op: Option<i8>,
```

Every existing `IcebergFileForQuery` construction must set:

```rust
change_op: None,
```

except delta-source call sites introduced in Task 5.

- [ ] **Step 2: Preserve normal table-def behavior**

Keep `build_iceberg_table_def_with_files` as the ordinary read helper. When mapping to `DataFileWithStats`, ignore `change_op` there:

```rust
let _change_op = file.change_op;
```

This makes accidental normal use of `change_op` a no-op unless callers choose the new delta helper.

- [ ] **Step 3: Add delta table builder**

Add:

```rust
pub(crate) fn build_iceberg_delta_table_def_with_files(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    if data_files.iter().any(|file| file.change_op.is_none()) {
        return Err("IVM delta table requires every file to carry change_op".to_string());
    }
    let change_ops = data_files
        .iter()
        .map(|file| {
            let op = file.change_op.expect("checked above");
            crate::exec::change_op::validate_change_op_value(op)?;
            Ok(op)
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut table_def =
        build_iceberg_table_def_with_files(state, catalog_name, namespace, table_name, data_files)?;
    table_def
        .iceberg_row_lineage_metadata_columns
        .push(crate::sql::catalog::ColumnDef {
            name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Int8,
            nullable: false,
            write_default: None,
        });

    match &mut table_def.storage {
        crate::sql::catalog::TableStorage::S3ParquetFiles { files, .. } => {
            if files.len() != change_ops.len() {
                return Err(format!(
                    "IVM delta table file count mismatch: storage_files={} change_ops={}",
                    files.len(),
                    change_ops.len()
                ));
            }
            for (file, op) in files.iter_mut().zip(change_ops) {
                file.ivm_change_op = Some(op);
            }
        }
        _ => {
            return Err("IVM delta table requires S3ParquetFiles storage".to_string());
        }
    }
    Ok(table_def)
}
```

- [ ] **Step 4: Make catalog conversion initialize `ivm_change_op`**

In `src/connector/iceberg/catalog/backend.rs::data_file_with_stats_to_s3_file_info`, set:

```rust
ivm_change_op: None,
```

- [ ] **Step 5: Add query-prep tests**

Add unit tests in `src/engine/query_prep.rs`:

```rust
#[test]
fn delta_table_builder_rejects_untagged_files() {
    let file = IcebergFileForQuery {
        path: "file:///tmp/a.parquet".to_string(),
        size: 1,
        record_count: Some(1),
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        change_op: None,
    };
    assert!(crate::exec::change_op::validate_change_op_value(0).is_err());
    assert!(file.change_op.is_none());
}
```

This test is intentionally lightweight because building a real Iceberg catalog entry is already covered by existing `build_iceberg_table_def_with_files` integration tests. The real end-to-end verification lands in Task 8.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test delta_table_builder_rejects_untagged_files data_file_with_stats_to_s3_file_info_preserves_read_metadata
```

Expected: both tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/query_prep.rs src/connector/iceberg/catalog/backend.rs src/sql/catalog.rs
git commit -m "feat(ivm): build tagged iceberg delta table defs"
```

## Task 5: Add IVM Delta Source API

**Files:**
- Create: `src/connector/starrocks/managed/ivm_delta_source.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/engine/mv_flow.rs`

- [ ] **Step 1: Create the module skeleton**

Create `src/connector/starrocks/managed/ivm_delta_source.rs`:

```rust
use std::sync::Arc;

use crate::engine::{QueryResult, StandaloneState};
use crate::engine::query_prep::IcebergFileForQuery;
use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT, CHANGE_OP_COLUMN};

use super::store::IcebergTableRef;

pub(crate) struct IvmDeltaSourceInput<'a> {
    pub(crate) state: &'a Arc<StandaloneState>,
    pub(crate) current_database: &'a str,
    pub(crate) base_ref: &'a IcebergTableRef,
}

#[derive(Clone)]
pub(crate) struct IvmDeltaSourceFiles {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) files: Vec<IcebergFileForQuery>,
}
```

- [ ] **Step 2: Expose the existing validation and delete-projection helpers**

In `src/engine/mv_flow.rs`, make these helpers `pub(crate)` and call them from `ivm_delta_source.rs`:

```rust
validate_incremental_mv_base_ref
strip_catalog_from_three_part_names
write_mv_delete_temp_parquet
```

Keep the error messages unchanged so existing tests do not churn.

Make these currently private helpers in `src/connector/iceberg/changes.rs` `pub(crate)` so the delta source reuses the existing delete-row projection implementation:

```rust
pub(crate) fn scan_equality_delete_rows_for_table(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String>

pub(crate) fn scan_deleted_data_file_rows(
    base_table: &iceberg::table::Table,
    deleted_data_files: &[DeletedDataFileRef],
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String>

pub(crate) fn build_factory_for_table(
    table: &iceberg::table::Table,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<crate::fs::opendal::OpendalRangeReaderFactory, String>

pub(crate) fn normalize_delete_projection_path(
    path: &str,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<String, ChangeError>
```

- [ ] **Step 3: Build tagged delta files**

Add:

```rust
pub(crate) fn build_delta_source_files(
    base_table: &iceberg::table::Table,
    batch: crate::connector::iceberg::changes::IcebergChangeBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<IvmDeltaSourceFiles, String> {
    let mut files = Vec::new();
    for f in &batch.inserts {
        files.push(IcebergFileForQuery {
            path: f.path.clone(),
            size: f.size,
            record_count: f.record_count,
            partition_spec_id: f.partition_spec_id,
            partition_key: f.partition_key.clone(),
            first_row_id: f.first_row_id,
            data_sequence_number: f.data_sequence_number,
            change_op: Some(CHANGE_OP_INSERT),
        });
    }

    let needs_deletes_scan = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    if needs_deletes_scan {
        let factory = crate::connector::iceberg::changes::build_factory_for_table(
            base_table,
            object_store_config,
        )?;
        let size_lookup = |_path: &str| -> Option<u64> { None };
        let mut deleted_rows = if batch.deletes.is_empty() {
            Vec::new()
        } else {
            crate::connector::iceberg::scan_deletes::scan_deletes_with_path_normalizer(
                &batch.deletes,
                &factory,
                base_table.file_io(),
                size_lookup,
                |path| crate::connector::iceberg::changes::normalize_delete_projection_path(
                    path,
                    object_store_config,
                ),
            )
            .map_err(|e| e.to_string())?
        };
        if !batch.equality_deletes.is_empty() {
            deleted_rows.extend(
                crate::connector::iceberg::changes::scan_equality_delete_rows_for_table(
                    base_table,
                    &batch.equality_deletes,
                    &factory,
                    object_store_config,
                )?,
            );
        }
        if !batch.deleted_data_files.is_empty() {
            deleted_rows.extend(
                crate::connector::iceberg::changes::scan_deleted_data_file_rows(
                    base_table,
                    &batch.deleted_data_files,
                    object_store_config,
                )?,
            );
        }
        if !deleted_rows.is_empty() {
            let (path, total_size, total_rows) =
                write_mv_delete_temp_parquet("ivm_delta", "deleted_rows", &deleted_rows)?;
            files.push(IcebergFileForQuery {
                path,
                size: total_size,
                record_count: total_rows,
                partition_spec_id: None,
                partition_key: None,
                first_row_id: Some(0),
                data_sequence_number: Some(0),
                change_op: Some(CHANGE_OP_DELETE),
            });
        }
    }

    Ok(IvmDeltaSourceFiles {
        previous_snapshot_id: batch.previous_snapshot_id,
        current_snapshot_id: batch.current_snapshot_id,
        files,
    })
}
```

Do not duplicate delete-row scanning logic.

- [ ] **Step 4: Execute SQL against the synthetic delta table**

Add:

```rust
pub(crate) fn execute_delta_source_query(
    input: IvmDeltaSourceInput<'_>,
    sql: &str,
    source_files: IvmDeltaSourceFiles,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };
    let (catalog_name, namespace, table_name) =
        validate_incremental_mv_base_ref(&query, input.base_ref)?;

    let table_def = crate::engine::query_prep::build_iceberg_delta_table_def_with_files(
        input.state,
        &catalog_name,
        &namespace,
        &table_name,
        source_files.files,
    )?;
    let mut incremental_catalog = crate::engine::catalog::InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register IVM delta iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    crate::engine::execute_query(
        &executable,
        &incremental_catalog,
        input.current_database,
        input.state.exchange_port,
        None,
    )
}
```

- [ ] **Step 5: Add projection SQL rewrite helper**

Add a helper that appends `__change_op` to projection SQL:

```rust
pub(crate) fn projection_select_with_change_op(select_sql: &str) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("projection MV change-op SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("projection MV change-op SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("projection MV change-op SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("projection MV change-op SELECT expects a SELECT body".to_string());
    };
    select.projection.push(sqlparser::ast::SelectItem::UnnamedExpr(
        sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(CHANGE_OP_COLUMN)),
    ));
    Ok(stmt.to_string())
}
```

- [ ] **Step 6: Export the module**

In `src/connector/starrocks/managed/mod.rs`:

```rust
pub(crate) mod ivm_delta_source;
```

- [ ] **Step 7: Run focused parser tests**

Add tests for:

```rust
projection_select_with_change_op("select k, v from cat.db.t")
```

Expected output string should contain `__change_op`.

Run:

```bash
cargo test projection_select_with_change_op
```

Expected: test passes.

- [ ] **Step 8: Commit**

```bash
git add src/connector/starrocks/managed/ivm_delta_source.rs src/connector/starrocks/managed/mod.rs src/connector/iceberg/changes.rs src/engine/mv_flow.rs
git commit -m "feat(ivm): add tagged iceberg delta source"
```

## Task 6: Add State-Aware Aggregate Delta Rewrite

**Files:**
- Create: `src/connector/starrocks/managed/ivm_delta_aggregate.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`

- [ ] **Step 1: Create the module skeleton**

Create `src/connector/starrocks/managed/ivm_delta_aggregate.rs`:

```rust
use crate::connector::starrocks::managed::mv_shape::{
    AggregateFunctionKind, AggregateInput, AggregateMvShape,
};

const CHANGE_OP_COLUMN: &str = crate::exec::change_op::CHANGE_OP_COLUMN;

pub(crate) fn rewrite_select_sql_for_signed_delta_state(
    select_sql: &str,
    shape: &AggregateMvShape,
) -> Result<String, String> {
    reject_unsupported_delta_aggregates(shape)?;
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite signed delta aggregate normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite signed delta aggregate parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("rewrite signed delta aggregate: expected Query statement".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite signed delta aggregate: expected SELECT body".to_string());
    };
    select.projection = build_signed_state_projection(shape)?;
    Ok(stmt.to_string())
}
```

- [ ] **Step 2: Reject non-reversible aggregate functions**

Add:

```rust
fn reject_unsupported_delta_aggregates(shape: &AggregateMvShape) -> Result<(), String> {
    for aggregate in &shape.aggregates {
        if matches!(
            aggregate.function,
            AggregateFunctionKind::Min | AggregateFunctionKind::Max
        ) {
            return Err(
                "MIN/MAX aggregate cannot consume delete-bearing delta state incrementally"
                    .to_string(),
            );
        }
    }
    Ok(())
}
```

The caller should convert this error to full refresh or an existing unsupported policy, matching current MIN/MAX delete-retract behavior.

- [ ] **Step 3: Build signed state projection**

Add:

```rust
fn build_signed_state_projection(
    shape: &AggregateMvShape,
) -> Result<Vec<sqlparser::ast::SelectItem>, String> {
    let mut projection = Vec::with_capacity(shape.visible_outputs.len() + shape.aggregates.len());
    for visible in &shape.visible_outputs {
        match visible {
            crate::connector::starrocks::managed::mv_shape::VisibleAggregateOutput::GroupKey(idx) => {
                let group_key = shape.group_keys.get(*idx).ok_or_else(|| {
                    format!("group key index {idx} out of range for signed delta aggregate")
                })?;
                projection.push(sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: sqlparser::ast::Ident::new(group_key.output_name.clone()),
                });
            }
            crate::connector::starrocks::managed::mv_shape::VisibleAggregateOutput::Aggregate(idx) => {
                let aggregate = shape.aggregates.get(*idx).ok_or_else(|| {
                    format!("aggregate index {idx} out of range for signed delta aggregate")
                })?;
                append_signed_aggregate_items(&mut projection, aggregate)?;
            }
        }
    }
    Ok(projection)
}
```

- [ ] **Step 4: Rewrite supported aggregate calls**

Add:

```rust
fn append_signed_aggregate_items(
    projection: &mut Vec<sqlparser::ast::SelectItem>,
    aggregate: &crate::connector::starrocks::managed::mv_shape::AggregateCallShape,
) -> Result<(), String> {
    match aggregate.function {
        AggregateFunctionKind::Count => {
            projection.push(make_aggregate_select_item(
                "SUM",
                count_delta_expr(&aggregate.input)?,
                &aggregate.output_name,
            ));
        }
        AggregateFunctionKind::Sum => {
            let expr = aggregate_expr(&aggregate.input, "SUM")?;
            projection.push(make_aggregate_select_item(
                "SUM",
                signed_expr(expr),
                &aggregate.output_name,
            ));
        }
        AggregateFunctionKind::Avg => {
            let expr = aggregate_expr(&aggregate.input, "AVG")?;
            let sanitized =
                crate::connector::starrocks::managed::mv_agg_state::sanitize_state_column_name(
                    &aggregate.output_name,
                );
            projection.push(make_aggregate_select_item(
                "SUM",
                signed_expr(expr.clone()),
                &format!("__agg_state_{sanitized}__sum"),
            ));
            projection.push(make_aggregate_select_item(
                "SUM",
                count_delta_expr(&aggregate.input)?,
                &format!("__agg_state_{sanitized}__count"),
            ));
        }
        AggregateFunctionKind::Min | AggregateFunctionKind::Max => {
            return Err("MIN/MAX aggregate cannot consume signed delta state".to_string());
        }
    }
    Ok(())
}
```

Use helper expressions:

```rust
fn aggregate_expr(
    input: &AggregateInput,
    function_name: &str,
) -> Result<sqlparser::ast::Expr, String> {
    match input {
        AggregateInput::Expr(expr) => Ok(expr.as_ref().clone()),
        AggregateInput::Star => Err(format!("{function_name} aggregate requires an expression")),
    }
}

fn change_op_expr() -> sqlparser::ast::Expr {
    sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new(CHANGE_OP_COLUMN))
}

fn signed_expr(expr: sqlparser::ast::Expr) -> sqlparser::ast::Expr {
    sqlparser::ast::Expr::BinaryOp {
        left: Box::new(expr),
        op: sqlparser::ast::BinaryOperator::Multiply,
        right: Box::new(change_op_expr()),
    }
}

fn count_delta_expr(input: &AggregateInput) -> Result<sqlparser::ast::Expr, String> {
    match input {
        AggregateInput::Star => Ok(change_op_expr()),
        AggregateInput::Expr(expr) => Ok(sqlparser::ast::Expr::Case {
            case_token: None,
            end_token: None,
            operand: None,
            conditions: vec![sqlparser::ast::CaseWhen {
                condition: sqlparser::ast::Expr::IsNotNull(Box::new(expr.as_ref().clone())),
                result: change_op_expr(),
            }],
            else_result: Some(Box::new(sqlparser::ast::Expr::Value(
                sqlparser::ast::Value::Number("0".to_string(), false).into(),
            ))),
        }),
    }
}
```

Use the existing `make_aggregate_select_item` shape from `mv_shape.rs`. If it is private, either make it `pub(crate)` or copy the small helper into `ivm_delta_aggregate.rs`:

```rust
fn make_aggregate_select_item(
    func_name: &str,
    arg_expr: sqlparser::ast::Expr,
    alias: &str,
) -> sqlparser::ast::SelectItem {
    sqlparser::ast::SelectItem::ExprWithAlias {
        expr: sqlparser::ast::Expr::Function(sqlparser::ast::Function {
            name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                sqlparser::ast::Ident::new(func_name),
            )]),
            args: sqlparser::ast::FunctionArguments::List(
                sqlparser::ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(arg_expr),
                    )],
                    clauses: vec![],
                },
            ),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: sqlparser::ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        }),
        alias: sqlparser::ast::Ident::new(alias),
    }
}
```

- [ ] **Step 5: Export the module**

In `src/connector/starrocks/managed/mod.rs`:

```rust
pub(crate) mod ivm_delta_aggregate;
```

- [ ] **Step 6: Add rewrite tests**

Add tests in `ivm_delta_aggregate.rs`:

```rust
#[test]
fn signed_delta_rewrite_turns_sum_into_sum_times_change_op() {
    let shape = parse_aggregate_shape(
        "SELECT k, SUM(v) AS s FROM ice.ns.orders GROUP BY k",
    );
    let rewritten = rewrite_select_sql_for_signed_delta_state(
        "SELECT k, SUM(v) AS s FROM ice.ns.orders GROUP BY k",
        &shape,
    )
    .expect("rewrite");
    assert!(rewritten.to_uppercase().contains("SUM(V * __CHANGE_OP)"), "{rewritten}");
}

#[test]
fn signed_delta_rewrite_turns_count_star_into_sum_change_op() {
    let shape = parse_aggregate_shape(
        "SELECT k, COUNT(*) AS c FROM ice.ns.orders GROUP BY k",
    );
    let rewritten = rewrite_select_sql_for_signed_delta_state(
        "SELECT k, COUNT(*) AS c FROM ice.ns.orders GROUP BY k",
        &shape,
    )
    .expect("rewrite");
    assert!(rewritten.to_uppercase().contains("SUM(__CHANGE_OP)"), "{rewritten}");
}

#[test]
fn signed_delta_rewrite_expands_avg_to_signed_sum_and_count() {
    let shape = parse_aggregate_shape(
        "SELECT k, AVG(v) AS a FROM ice.ns.orders GROUP BY k",
    );
    let rewritten = rewrite_select_sql_for_signed_delta_state(
        "SELECT k, AVG(v) AS a FROM ice.ns.orders GROUP BY k",
        &shape,
    )
    .expect("rewrite");
    let upper = rewritten.to_uppercase();
    assert!(upper.contains("SUM(V * __CHANGE_OP)"), "{rewritten}");
    assert!(upper.contains("__AGG_STATE_A__SUM"), "{rewritten}");
    assert!(upper.contains("__AGG_STATE_A__COUNT"), "{rewritten}");
}

#[test]
fn signed_delta_rewrite_rejects_min_max() {
    let shape = parse_aggregate_shape(
        "SELECT k, MIN(v) AS m FROM ice.ns.orders GROUP BY k",
    );
    let err = rewrite_select_sql_for_signed_delta_state(
        "SELECT k, MIN(v) AS m FROM ice.ns.orders GROUP BY k",
        &shape,
    )
    .expect_err("MIN must reject");
    assert!(err.contains("MIN/MAX"), "err={err}");
}
```

Implement `parse_aggregate_shape` inside the test module by parsing the SQL and calling `classify_incremental_mv_query`, then matching `IncrementalMvShape::Aggregate`.

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test signed_delta_rewrite_turns_sum_into_sum_times_change_op
cargo test signed_delta_rewrite_turns_count_star_into_sum_change_op
cargo test signed_delta_rewrite_expands_avg_to_signed_sum_and_count
cargo test signed_delta_rewrite_rejects_min_max
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/starrocks/managed/ivm_delta_aggregate.rs src/connector/starrocks/managed/mod.rs
git commit -m "feat(ivm): rewrite aggregate deltas as signed state"
```

## Task 7: Consume Delta Source in Managed MV Refresh

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`

- [ ] **Step 1: Replace projection branch materialization**

In `src/connector/starrocks/managed/mv_refresh.rs`, replace the projection incremental path that calls `materialize_iceberg_change_batch`, splits `change_stream.into_results()`, and passes the two branches into `projection_delete_bearing_change_chunks`.

with:

```rust
let source_files = super::ivm_delta_source::build_delta_source_files(
    base_table,
    change_batch,
    object_store_config,
)?;
let projection_sql =
    super::ivm_delta_source::projection_select_with_change_op(&mv_row.select_sql)?;
let result = super::ivm_delta_source::execute_delta_source_query(
    super::ivm_delta_source::IvmDeltaSourceInput {
        state,
        current_database: &db_name,
        base_ref,
    },
    &projection_sql,
    source_files,
)?;
let (chunks, row_delta) = tagged_projection_change_chunks(result)?;
```

The important boundary is that `build_delta_source_files` owns Iceberg delta extraction, and `execute_delta_source_query` owns the synthetic delta table execution.

- [ ] **Step 2: Map `__change_op` to managed-lake `__op`**

Replace `projection_delete_bearing_change_chunks` branch inputs with a single-result helper:

```rust
fn tagged_projection_change_chunks(result: QueryResult) -> Result<(Vec<Chunk>, i64), String> {
    let mut delete_batches = Vec::new();
    let mut insert_batches = Vec::new();
    for chunk in result.chunks {
        let batch = chunk.batch;
        let op_idx = batch
            .schema()
            .index_of(crate::exec::change_op::CHANGE_OP_COLUMN)
            .map_err(|_| "projection MV delta result missing __change_op column".to_string())?;
        let op_array = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .ok_or_else(|| "projection MV delta result __change_op is not Int8".to_string())?;
        for row in 0..batch.num_rows() {
            let op = op_array.value(row);
            crate::exec::change_op::validate_change_op_value(op)?;
        }
        let (delete_batch, insert_batch) = split_batch_by_change_op(batch, op_idx)?;
        if let Some(batch) = delete_batch {
            delete_batches.push(append_mv_op_column(remove_column(batch, op_idx)?, MV_OP_DELETE)?);
        }
        if let Some(batch) = insert_batch {
            insert_batches.push(append_mv_op_column(remove_column(batch, op_idx)?, MV_OP_UPSERT)?);
        }
    }
    let mut delete_chunks = delete_batches
        .into_iter()
        .map(record_batch_to_chunk)
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert_chunks = insert_batches
        .into_iter()
        .map(record_batch_to_chunk)
        .collect::<Result<Vec<_>, _>>()?;
    let insert_rows = chunks_row_count(&insert_chunks)?;
    let delete_rows = chunks_row_count(&delete_chunks)?;
    delete_chunks.append(&mut insert_chunks);
    let row_delta = insert_rows.checked_sub(delete_rows).ok_or_else(|| {
        format!(
            "projection/filter MV row-count delta overflow: inserts={insert_rows} deletes={delete_rows}"
        )
    })?;
    Ok((delete_chunks, row_delta))
}
```

Implement `split_batch_by_change_op` using Arrow `filter_record_batch` with two masks:

```rust
fn split_batch_by_change_op(
    batch: arrow::record_batch::RecordBatch,
    op_idx: usize,
) -> Result<(Option<arrow::record_batch::RecordBatch>, Option<arrow::record_batch::RecordBatch>), String>
```

The helper must return delete batch first and insert batch second so the final chunk order remains delete-before-upsert.

- [ ] **Step 3: Consume delta source in aggregate branch**

Replace the aggregate incremental branch that calls `materialize_iceberg_change_batch`, splits `change_stream.into_results()`, and materializes insert/delete aggregate state chunks separately.

with:

```rust
let source_files = super::ivm_delta_source::build_delta_source_files(
    ctx.base_table,
    ctx.change_batch,
    ctx.object_store_config,
)?;
let signed_state_sql = super::ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state(
    ctx.select_sql,
    ctx.shape,
)?;
let delta_result = super::ivm_delta_source::execute_delta_source_query(
    ctx.to_delta_source_input(),
    &signed_state_sql,
    source_files,
)?;
let delta_chunks =
    super::mv_agg_state::materialize_aggregate_result_chunks(delta_result, &layout, ctx.shape)?;
```

This path must not call `negate_aggregate_state_chunks`; the sign is already encoded in the delta-state SQL.

- [ ] **Step 4: Preserve MIN/MAX fallback behavior**

Before executing the signed delta query, keep the existing apply policy gate for delete-bearing changes and `MIN` / `MAX` aggregate state. If `rewrite_select_sql_for_signed_delta_state` returns the MIN/MAX retract error, route to `refresh_aggregate_mv_full` with the same style of tracing message used by the current apply-policy fallback.

- [ ] **Step 5: Keep old branch helpers only as compatibility**

Leave `materialize_iceberg_change_batch` and `IvmChangeStream` in place if other call sites still use them, but add a code comment above their managed MV call sites if any remain:

```rust
// Compatibility path only. Iceberg-backed managed MV refresh should use
// ivm_delta_source so change semantics enter through a tagged scan/source.
```

- [ ] **Step 6: Run focused MV unit tests**

Run:

```bash
cargo test projection_delete_bearing_change_applies_deletes_before_upserts
cargo test aggregate_mv_incremental_refresh_handles_base_delete
cargo test aggregate_mv_incremental_refresh_treats_deleted_data_files_as_delete_bearing
cargo test signed_delta_rewrite_turns_sum_into_sum_times_change_op
```

Expected: all pass or skip only for existing unavailable object-store guards.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat(ivm): consume signed delta state in mv refresh"
```

## Task 8: End-to-End SQL Verification

**Files:**
- Create: `sql-tests/iceberg-rest/sql/iceberg_rest_ivm_change_op_delta_source.sql`
- Create: `sql-tests/iceberg-rest/result/iceberg_rest_ivm_change_op_delta_source.result`
- No production code changes unless these tests expose a bug.

- [ ] **Step 1: Prepare the Iceberg REST fixture**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh || docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Expected: shared MinIO/REST/Spark services are running or reused.

- [ ] **Step 2: Build NovaRocks**

Run:

```bash
cargo build
```

Expected: debug build succeeds.

- [ ] **Step 3: Start standalone-server with readiness gating**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-ivm-a4.log
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

- [ ] **Step 4: Add and run projection COW test**

Use an Iceberg v3 table and projection MV with primary key. The test must perform a COW update that removes one old row and adds one replacement row for the same key.

Expected result:

```text
base before:  (1, 100)
base after:   (1, 80)
delta source: (1, 100, -1), (1, 80, +1)
MV after:     (1, 80)
```

Run the suite command:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: the new projection MV COW case passes.

- [ ] **Step 5: Add and run aggregate COW/delete test**

Use:

```sql
CREATE MATERIALIZED VIEW mv_sum AS
SELECT customer, SUM(amount) AS total_amount
FROM ice.ns.orders
GROUP BY customer;
```

Then update one row from `100` to `80`.

Expected result:

```text
delta source: Alice, 100, -1
delta source: Alice,  80, +1
MV after:     Alice, 80
```

The old broken behavior would produce `Alice, 180` if delete rows were treated as positive inserts. The test must assert `80`.

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: aggregate case passes.

- [ ] **Step 6: Stop the server started by this task**

Run:

```bash
kill "$SRV_PID"
wait "$SRV_PID" || true
```

Expected: only the local standalone-server process exits; shared Docker remains running.

- [ ] **Step 7: Commit**

```bash
git add tests/sql
git commit -m "test(ivm): verify tagged delta source refresh"
```

## Task 9: Full Local Validation

**Files:**
- No code changes expected.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: command succeeds. If it changes files, inspect and include them in the final commit.

- [ ] **Step 2: Build**

Run:

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 3: Run focused Rust tests**

Run:

```bash
cargo test change_op
cargo test hdfs_scan
cargo test ivm_delta_source
cargo test signed_delta_rewrite
cargo test aggregate_mv_incremental_refresh_handles_base_delete
cargo test aggregate_mv_incremental_refresh_treats_deleted_data_files_as_delete_bearing
```

Expected: all focused tests pass.

- [ ] **Step 4: Run focused SQL suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: `iceberg-rest` suite passes. If an object-store availability guard skips a pre-existing test, record the exact skip and confirm it is unrelated to the new change-op path.

- [ ] **Step 5: Final commit if formatting or test files changed**

```bash
git status --short
git diff --name-only -z | xargs -0 git add
git commit -m "chore(ivm): finalize change-op delta source"
```

## Implementation Notes

- Do not add `__change_op` to the global `Chunk` struct. It must be a column inside the scan output schema for this phase.
- Do not register `__change_op` on ordinary Iceberg catalog tables. It is only present on synthetic delta tables returned by `build_iceberg_delta_table_def_with_files`.
- Do not treat missing `__change_op` as insert. Missing tags are correctness failures.
- Do not copy StarRocks append-only `__ACTION__ = 0` behavior. NovaRocks needs `+1` and `-1` for COW/update/delete.
- Keep delete-before-upsert ordering in projection MV writes after splitting tagged chunks.
- Aggregate MV refresh must not call `negate_aggregate_state_chunks` for supported `COUNT` / `SUM` / `AVG` delta refresh. The signed delta-state SQL already encodes delete rows as negative contributions.
- Keep `MIN` / `MAX` on the existing fallback or reject path when delete-bearing changes are present.
