# Iceberg V3 Row-Lineage Metadata Columns Read Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SELECT _row_id, _last_updated_sequence_number FROM ice_v3_lineage_t` work end-to-end on NovaRocks, with both vendor `RecordBatchTransformer` (DELETE flow path) and NovaRocks `runner.append_iceberg_virtual_columns` (user-level scan path) implementing Iceberg V3 spec-compliant stored / fallback semantics.

**Architecture:** Dual-track. NovaRocks user-level scan reads raw parquet via `formats/parquet`, so spec compliance lives in `runner.rs::append_iceberg_virtual_columns`: detect stored `_row_id` / `_last_updated_sequence_number` columns by reserved field id, fall back to `first_row_id + scan_position` and manifest entry's `data_sequence_number` when stored is NULL or missing. Vendor `RecordBatchTransformer` gets the symmetrical upgrade so DELETE flow stays consistent. Both paths produce row-equal output by construction.

**Tech Stack:** Rust 2024, vendored iceberg-rust 0.9 (NovaRocks fork in `vendor/iceberg-0.9.0/`), arrow/parquet 57, NovaRocks `formats/parquet` reader, MinIO-backed integration tests.

**Spec:** `docs/superpowers/specs/2026-04-30-iceberg-v3-row-lineage-metadata-columns-read-design.md`

---

## File Structure

| Path | Responsibility |
|---|---|
| `vendor/iceberg-0.9.0/src/scan/task.rs` | `FileScanTask` carries new `data_sequence_number` |
| `vendor/iceberg-0.9.0/src/scan/context.rs` | Plan-files path fills `data_sequence_number` from manifest entry |
| `vendor/iceberg-0.9.0/src/arrow/reader.rs` | Pass `data_sequence_number` and stored-column probing through to transformer builder |
| `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs` | `ColumnSource::RowId` extended with `stored_source_index`; new `ColumnSource::LastUpdatedSeqNum`; spec-compliant per-row dispatch |
| `vendor/iceberg-0.9.0/PATCH.md` | Patch 3 description updated; new patch 5 added |
| `src/exec/row_position.rs` | New name constants, name detection helpers, reserved field id constants, `IcebergVirtualSpec` field expansion |
| `src/lower/node/hdfs_scan.rs` | Lowering branches identify `_row_id` / `_last_updated_sequence_number` slots |
| `src/lower/node/file_scan.rs` | Pass `data_sequence_number` from metadata to scan range |
| `src/connector/hdfs.rs` | `ScanRange` carries `data_sequence_number` |
| `src/sql/catalog.rs` | `TableDef` gains `iceberg_row_lineage_metadata_columns: Vec<ColumnDef>` |
| Catalog provider implementations (iceberg-providing `get_table` callers) | Populate the new field when base is V3 row-lineage |
| `src/sql/analyzer/scope.rs` | `add_iceberg_metadata_columns` registers pseudo-columns into qualified/unqualified maps without leaking into `ordered` (so they stay invisible to `SELECT *`) |
| `src/sql/analyzer/mod.rs` | `collect_relation_scope` calls `add_iceberg_metadata_columns` after `add_table`; resolve-fail branch surfaces the spec-aligned error for reserved names |
| `src/exec/operators/scan/runner.rs` | `IcebergVirtualState` extended; `append_iceberg_virtual_columns` synthesizes the new columns with stored / fallback dispatch |
| `src/connector/hdfs.rs` (parquet read column list) | Include reserved field ids in parquet read selection when `IcebergVirtualSpec` requests them, so runner can detect stored columns |
| `src/engine/mod.rs` (tests module) | End-to-end integration tests |

---

### Task 1: Vendor `_row_id` stored-column override (spec compliance for vendor path)

**Files:**
- Modify: `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs`
- Modify: `vendor/iceberg-0.9.0/PATCH.md`

- [ ] **Step 1: Add the first failing test — stored column all non-NULL → uses stored**

In `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs` `mod tests` (search for `#[test]` block near end of file), append:

```rust
#[test]
fn row_id_uses_stored_column_when_all_non_null() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let stored_field = Field::new("_row_id", DataType::Int64, true).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_ROW_ID.to_string())]),
    );
    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field, stored_field]));

    let pos = Arc::new(Int64Array::from(vec![0_i64, 1, 2])) as ArrayRef;
    let stored = Arc::new(Int64Array::from(vec![Some(700_i64), Some(800), Some(900)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos, stored]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(snapshot_schema, &[RESERVED_FIELD_ID_ROW_ID])
        .with_first_row_id(Some(100))
        .build();
    let out = transformer.process_record_batch(batch).expect("process ok");

    let row_ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(row_ids.value(0), 700);
    assert_eq!(row_ids.value(1), 800);
    assert_eq!(row_ids.value(2), 900);
}
```

- [ ] **Step 2: Run test, verify it fails**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::row_id_uses_stored_column_when_all_non_null -- --nocapture
```

Expected: FAIL — current `ColumnSource::RowId` always derives from `first_row_id + pos`.

- [ ] **Step 3: Extend `ColumnSource::RowId` with `stored_source_index`**

In `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs`, locate the `ColumnSource` enum (around line 145):

```rust
RowId {
    first_row_id: i64,
    pos_source_index: usize,
},
```

Replace with:

```rust
RowId {
    first_row_id: i64,
    pos_source_index: usize,
    /// `Some` when the source RecordBatch contains a physical column for the
    /// reserved `_row_id` field id (cross-engine writes); the per-row stored
    /// value takes precedence over the `first_row_id + _pos` fallback when
    /// non-NULL.
    stored_source_index: Option<usize>,
},
```

- [ ] **Step 4: Detect stored column in `generate_transform_operations`**

In `generate_transform_operations` (around line 551), replace the existing `RESERVED_FIELD_ID_ROW_ID` arm:

```rust
if *field_id == RESERVED_FIELD_ID_ROW_ID {
    let first_row_id = first_row_id.ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "_row_id metadata column was projected but first_row_id is missing",
        )
    })?;
    if first_row_id < 0 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("first_row_id must be non-negative, got {first_row_id}"),
        ));
    }
    let (_pos_field, pos_source_index) = field_id_to_source_schema_map
        .get(&RESERVED_FIELD_ID_POS)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "_row_id metadata column was projected but the Parquet reader did not provide a RowNumber source column",
            )
        })?;
    // NEW: detect stored _row_id column by reserved field id.
    let stored_source_index = field_id_to_source_schema_map
        .get(&RESERVED_FIELD_ID_ROW_ID)
        .map(|(field, idx)| {
            if !field.data_type().equals_datatype(&DataType::Int64) {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "stored _row_id column must be Int64, got {:?}",
                        field.data_type()
                    ),
                ));
            }
            Ok(*idx)
        })
        .transpose()?;
    return Ok(ColumnSource::RowId {
        first_row_id,
        pos_source_index: *pos_source_index,
        stored_source_index,
    });
}
```

- [ ] **Step 5: Implement stored / fallback dispatch in `create_row_id_column`**

In `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs`, locate `create_row_id_column` (around line 730) and `process_record_batch`'s `RowId` arm (around line 721). Replace `create_row_id_column`'s signature and body:

```rust
fn create_row_id_column(
    first_row_id: i64,
    position_column: &ArrayRef,
    stored_column: Option<&ArrayRef>,
) -> Result<ArrayRef> {
    let positions = position_column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "_row_id RowNumber source column must be Int64, got {:?}",
                    position_column.data_type()
                ),
            )
        })?;

    let stored = stored_column
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "stored _row_id column must be Int64, got {:?}",
                            arr.data_type()
                        ),
                    )
                })
        })
        .transpose()?;

    let row_ids: Result<Vec<i64>> = positions
        .iter()
        .enumerate()
        .map(|(i, position)| {
            if let Some(stored_arr) = stored {
                if !stored_arr.is_null(i) {
                    return Ok(stored_arr.value(i));
                }
            }
            let position = position.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "_row_id RowNumber source column contained null in fallback row",
                )
            })?;
            first_row_id.checked_add(position).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Row ID overflow when computing _row_id: first_row_id={first_row_id}, pos={position}"
                    ),
                )
            })
        })
        .collect();

    Ok(Arc::new(Int64Array::from(row_ids?)))
}
```

In `process_record_batch` (around line 721), replace the `RowId` arm:

```rust
ColumnSource::RowId {
    first_row_id,
    pos_source_index,
    stored_source_index,
} => Self::create_row_id_column(
    *first_row_id,
    &columns[*pos_source_index],
    stored_source_index.map(|idx| &columns[idx]),
)?,
```

- [ ] **Step 6: Run test, verify it passes**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::row_id_uses_stored_column_when_all_non_null -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Add the second test — mixed NULL/non-NULL falls back per row**

Append to the same `mod tests`:

```rust
#[test]
fn row_id_falls_back_when_stored_is_null_per_row() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let stored_field = Field::new("_row_id", DataType::Int64, true).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_ROW_ID.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field, stored_field]));

    let pos = Arc::new(Int64Array::from(vec![0_i64, 1, 2])) as ArrayRef;
    let stored = Arc::new(Int64Array::from(vec![Some(700_i64), None, Some(900)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos, stored]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(snapshot_schema, &[RESERVED_FIELD_ID_ROW_ID])
        .with_first_row_id(Some(100))
        .build();
    let out = transformer.process_record_batch(batch).expect("process ok");

    let row_ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(row_ids.value(0), 700);
    assert_eq!(row_ids.value(1), 101);   // fallback: 100 + pos(1)
    assert_eq!(row_ids.value(2), 900);
}
```

Run:

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::row_id_falls_back_when_stored_is_null_per_row -- --nocapture
```

Expected: PASS (Step 5's implementation already handles this).

- [ ] **Step 8: Add the third test — stored column missing → all fallback**

Append:

```rust
#[test]
fn row_id_falls_back_when_stored_column_missing() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field]));

    let pos = Arc::new(Int64Array::from(vec![0_i64, 1, 2])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(snapshot_schema, &[RESERVED_FIELD_ID_ROW_ID])
        .with_first_row_id(Some(50))
        .build();
    let out = transformer.process_record_batch(batch).expect("process ok");

    let row_ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(row_ids.values(), &[50_i64, 51, 52]);
}
```

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::row_id_falls_back_when_stored_column_missing -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Add the fourth test — stored column wrong type → fail**

Append:

```rust
#[test]
fn row_id_fails_when_stored_column_is_wrong_type() {
    use arrow_array::{Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let stored_field = Field::new("_row_id", DataType::Int32, true).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_ROW_ID.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field, stored_field]));

    let pos = Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef;
    let stored = Arc::new(Int32Array::from(vec![1_i32])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos, stored]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(snapshot_schema, &[RESERVED_FIELD_ID_ROW_ID])
        .with_first_row_id(Some(0))
        .build();
    let err = transformer.process_record_batch(batch).expect_err("must fail");
    assert!(format!("{err}").contains("stored _row_id column must be Int64"));
}
```

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::row_id_fails_when_stored_column_is_wrong_type -- --nocapture
```

Expected: PASS.

- [ ] **Step 10: Run full vendor record_batch_transformer test module to confirm no regression**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer -- --nocapture
```

Expected: all PASS (including the existing `_row_id` / `_pos` tests from patch 3).

- [ ] **Step 11: Update `vendor/iceberg-0.9.0/PATCH.md` patch 3 description**

Find the existing patch 3 description (lines 41-86). Append a new bullet to the "Concretely:" list (after the existing bullets, before "No public API renames…"):

```
* `_row_id` stored-column override: when the parquet file physically contains a
  column tagged with `RESERVED_FIELD_ID_ROW_ID`, `generate_transform_operations`
  records its source index in `ColumnSource::RowId::stored_source_index`. At
  per-row materialization, non-NULL stored values take precedence over the
  `first_row_id + _pos` fallback. NULL stored values, missing stored columns,
  and the previous-patch-3 path all fall back unchanged.
```

- [ ] **Step 12: cargo fmt + cargo build**

```bash
cargo fmt --manifest-path vendor/iceberg-0.9.0/Cargo.toml --check
cargo build -p novarocks
```

Expected: both PASS.

- [ ] **Step 13: Commit**

```bash
git add vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs vendor/iceberg-0.9.0/PATCH.md
git commit -m "feat(vendor): add stored-column override for _row_id read path

ColumnSource::RowId now carries an optional stored_source_index. When
the source RecordBatch physically contains a column tagged with
RESERVED_FIELD_ID_ROW_ID, generate_transform_operations records its
source index, and create_row_id_column dispatches per row: non-NULL
stored values take precedence, NULL/missing rows fall back to the
existing first_row_id + _pos derivation. Cross-engine reads of
files written with stored row-ids (e.g. Spark/Trino partial-overwrite
output) now match the Iceberg V3 spec.

Spec ref: https://iceberg.apache.org/spec/#row-lineage"
```

---

### Task 2: Vendor `_last_updated_sequence_number` read path

**Files:**
- Modify: `vendor/iceberg-0.9.0/src/scan/task.rs`
- Modify: `vendor/iceberg-0.9.0/src/scan/context.rs`
- Modify: `vendor/iceberg-0.9.0/src/arrow/reader.rs`
- Modify: `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs`
- Modify: `vendor/iceberg-0.9.0/PATCH.md`

- [ ] **Step 1: Add a failing test — FileScanTask carries data_sequence_number**

In `vendor/iceberg-0.9.0/src/scan/mod.rs` `mod tests` (find the `task` serialization test around line 1752):

```rust
#[test]
fn task_carries_data_sequence_number() {
    let task = FileScanTask {
        file_size_in_bytes: 100,
        start: 0,
        length: 100,
        record_count: Some(10),
        first_row_id: Some(0),
        data_sequence_number: Some(7),
        data_file_path: "x.parquet".to_string(),
        data_file_format: DataFileFormat::Parquet,
        schema: Arc::new(crate::spec::Schema::builder().with_fields(vec![]).build().unwrap()),
        project_field_ids: vec![],
        predicate: None,
        deletes: vec![],
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: true,
    };
    assert_eq!(task.data_sequence_number, Some(7));
}
```

- [ ] **Step 2: Run, verify compile fail**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib scan::tests::task_carries_data_sequence_number -- --nocapture
```

Expected: COMPILE FAIL — field `data_sequence_number` not on `FileScanTask`.

- [ ] **Step 3: Add `data_sequence_number` field to `FileScanTask`**

In `vendor/iceberg-0.9.0/src/scan/task.rs` (line ~70, after `first_row_id`):

```rust
/// The first row ID assigned to the data file in an Iceberg v3 table.
#[serde(default)]
#[serde(skip_serializing_if = "Option::is_none")]
pub first_row_id: Option<i64>,

/// The data file's `data_sequence_number` from its manifest entry.
/// Used by `_last_updated_sequence_number` reads as the spec-defined
/// fallback when the data file does not physically store the column.
#[serde(default)]
#[serde(skip_serializing_if = "Option::is_none")]
pub data_sequence_number: Option<i64>,
```

- [ ] **Step 4: Fill `data_sequence_number` from manifest entry in plan-files path**

In `vendor/iceberg-0.9.0/src/scan/context.rs` (around line 119-128):

```rust
Ok(FileScanTask {
    // existing fields ...
    first_row_id: self.manifest_entry.data_file.first_row_id(),
    data_sequence_number: self.manifest_entry.data_sequence_number(),
    // remaining existing fields ...
})
```

(Field-by-field constructor; insert `data_sequence_number` line right after `first_row_id`. Other call sites that build `FileScanTask` literals — search with `grep -n "FileScanTask {" vendor/iceberg-0.9.0/src/` — also need the new field set to `None`.)

Run grep and update **every** call site:

```bash
grep -rn "FileScanTask {" vendor/iceberg-0.9.0/src/
```

For each match (typically `mod.rs` test fixtures and `context.rs`), insert `data_sequence_number: None,` (test fixtures) or the manifest-derived value (live path).

- [ ] **Step 5: Run the task carry test, verify it passes**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib scan::tests::task_carries_data_sequence_number -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Add a failing test for `_last_updated_sequence_number` stored-column path**

In `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs` `mod tests`:

```rust
#[test]
fn last_updated_seq_uses_stored_column_when_present() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let stored_field =
        Field::new("_last_updated_sequence_number", DataType::Int64, true).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
            )]),
        );
    let schema = Arc::new(Schema::new(vec![stored_field]));
    let stored = Arc::new(Int64Array::from(vec![Some(11_i64), Some(12), Some(13)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(
        snapshot_schema,
        &[RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER],
    )
    .with_data_sequence_number(Some(99))
    .build();
    let out = transformer.process_record_batch(batch).expect("process ok");

    let seqs = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(seqs.values(), &[11_i64, 12, 13]);
}
```

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::last_updated_seq_uses_stored_column_when_present -- --nocapture
```

Expected: COMPILE FAIL — `with_data_sequence_number`, `RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER` import, and `LastUpdatedSeqNum` not yet defined.

- [ ] **Step 7: Add `RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER` import**

At the top of `record_batch_transformer.rs` (line ~33):

```rust
use crate::metadata_columns::{
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_POS, RESERVED_FIELD_ID_ROW_ID,
    get_metadata_field,
};
```

- [ ] **Step 8: Add `LastUpdatedSeqNum` variant to `ColumnSource`**

After the `RowId` variant in `ColumnSource`:

```rust
LastUpdatedSeqNum {
    /// Spec fallback value: data file's `data_sequence_number` from its
    /// manifest entry. Used when stored is NULL or absent.
    fallback_value: i64,
    /// `Some` when the source RecordBatch physically carries a column
    /// tagged with the reserved field id.
    stored_source_index: Option<usize>,
},
```

- [ ] **Step 9: Add builder + transformer plumbing for `data_sequence_number`**

In `RecordBatchTransformerBuilder` struct (around line 198):

```rust
pub(crate) struct RecordBatchTransformerBuilder {
    snapshot_schema: Arc<IcebergSchema>,
    projected_iceberg_field_ids: Vec<i32>,
    constant_fields: HashMap<i32, Datum>,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,   // NEW
}
```

In `RecordBatchTransformerBuilder::new` (around line 211), initialize `data_sequence_number: None`.

After `with_first_row_id`, add:

```rust
pub(crate) fn with_data_sequence_number(mut self, value: Option<i64>) -> Self {
    self.data_sequence_number = value;
    self
}
```

In `RecordBatchTransformerBuilder::build` (around line 256), forward into the transformer:

```rust
RecordBatchTransformer {
    snapshot_schema: self.snapshot_schema,
    projected_iceberg_field_ids: self.projected_iceberg_field_ids,
    constant_fields: self.constant_fields,
    first_row_id: self.first_row_id,
    data_sequence_number: self.data_sequence_number,  // NEW
    batch_transform: None,
}
```

In `RecordBatchTransformer` struct (around line 298) add the field:

```rust
data_sequence_number: Option<i64>,   // NEW
```

In `transform_batch` (around line 340), pass it into `generate_batch_transform`:

```rust
self.batch_transform = Some(Self::generate_batch_transform(
    record_batch.schema_ref(),
    self.snapshot_schema.as_ref(),
    &self.projected_iceberg_field_ids,
    &self.constant_fields,
    self.first_row_id,
    self.data_sequence_number,   // NEW
)?);
```

- [ ] **Step 10: Plumb `data_sequence_number` through `generate_batch_transform` → `generate_transform_operations`**

Update both signatures to accept `data_sequence_number: Option<i64>`. In `generate_batch_transform` (around line 366), pass it through to `generate_transform_operations` and to the schema-fields branch (which needs to know to register `_last_updated_sequence_number` as a per-row Int64 metadata column, parallel to existing `_row_id` registration).

In the per-`field_id` schema-building closure (around line 384), extend the existing per-row metadata branch:

```rust
if *field_id == RESERVED_FIELD_ID_POS
    || *field_id == RESERVED_FIELD_ID_ROW_ID
    || *field_id == RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
{
    let iceberg_field = get_metadata_field(*field_id).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("metadata field lookup failed for field id {field_id}: {e}"),
        )
    })?;
    let arrow_field =
        Field::new(&iceberg_field.name, DataType::Int64, !iceberg_field.required)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                iceberg_field.id.to_string(),
            )]));
    return Ok(Arc::new(arrow_field));
}
```

- [ ] **Step 11: Implement `LastUpdatedSeqNum` dispatch in `generate_transform_operations`**

After the `RESERVED_FIELD_ID_ROW_ID` arm (around line 585), add:

```rust
if *field_id == RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER {
    let fallback_value = data_sequence_number.ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "_last_updated_sequence_number metadata column was projected but task is missing data_sequence_number",
        )
    })?;
    let stored_source_index = field_id_to_source_schema_map
        .get(&RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
        .map(|(field, idx)| {
            if !field.data_type().equals_datatype(&DataType::Int64) {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "stored _last_updated_sequence_number column must be Int64, got {:?}",
                        field.data_type()
                    ),
                ));
            }
            Ok(*idx)
        })
        .transpose()?;
    return Ok(ColumnSource::LastUpdatedSeqNum {
        fallback_value,
        stored_source_index,
    });
}
```

- [ ] **Step 12: Implement runtime materialization for `LastUpdatedSeqNum`**

In `process_record_batch`'s match (around line 707), add a new arm:

```rust
ColumnSource::LastUpdatedSeqNum {
    fallback_value,
    stored_source_index,
} => Self::create_last_updated_seq_column(
    *fallback_value,
    columns.first().map(|_| columns[0].len()).unwrap_or(0),
    stored_source_index.map(|idx| &columns[idx]),
)?,
```

After `create_row_id_column` add:

```rust
fn create_last_updated_seq_column(
    fallback_value: i64,
    num_rows_when_no_columns: usize,
    stored_column: Option<&ArrayRef>,
) -> Result<ArrayRef> {
    if let Some(stored_arr) = stored_column {
        let stored = stored_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "stored _last_updated_sequence_number column must be Int64, got {:?}",
                        stored_arr.data_type()
                    ),
                )
            })?;
        let values: Vec<i64> = (0..stored.len())
            .map(|i| if stored.is_null(i) { fallback_value } else { stored.value(i) })
            .collect();
        return Ok(Arc::new(Int64Array::from(values)));
    }
    let values = vec![fallback_value; num_rows_when_no_columns];
    Ok(Arc::new(Int64Array::from(values)))
}
```

(Note: `process_record_batch` should pass actual `num_rows` — locate `let num_rows = columns[0].len();` near the top of the operations loop and use it. If columns vec is empty, default to 0; in practice the projection always has at least the `_pos` column when reserved metadata is requested, so columns won't be empty.)

- [ ] **Step 13: Run the stored-present test, verify it passes**

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::last_updated_seq_uses_stored_column_when_present -- --nocapture
```

Expected: PASS.

- [ ] **Step 14: Add the remaining 3 `last_updated_seq` tests**

Append to `mod tests`:

```rust
#[test]
fn last_updated_seq_falls_back_when_stored_is_null_per_row() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let stored_field = Field::new("_last_updated_sequence_number", DataType::Int64, true)
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
        )]));
    let schema = Arc::new(Schema::new(vec![stored_field]));
    let stored = Arc::new(Int64Array::from(vec![Some(11_i64), None, Some(13)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(
        snapshot_schema,
        &[RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER],
    )
    .with_data_sequence_number(Some(99))
    .build();
    let out = transformer.process_record_batch(batch).expect("ok");

    let seqs = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(seqs.values(), &[11_i64, 99, 13]);
}

#[test]
fn last_updated_seq_falls_back_when_stored_column_missing() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    // Minimal carrier column so the RecordBatch knows row count.
    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field]));
    let pos = Arc::new(Int64Array::from(vec![0_i64, 1, 2])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(
        snapshot_schema,
        &[RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER],
    )
    .with_data_sequence_number(Some(99))
    .build();
    let out = transformer.process_record_batch(batch).expect("ok");

    let seqs = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(seqs.values(), &[99_i64, 99, 99]);
}

#[test]
fn last_updated_seq_fails_when_data_sequence_number_missing() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    let pos_field = Field::new("_pos", DataType::Int64, false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), RESERVED_FIELD_ID_POS.to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![pos_field]));
    let pos = Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![pos]).unwrap();

    let snapshot_schema = Arc::new(IcebergSchema::builder().with_fields(vec![]).build().unwrap());
    let mut transformer = RecordBatchTransformerBuilder::new(
        snapshot_schema,
        &[RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER],
    )
    .build();
    let err = transformer.process_record_batch(batch).expect_err("must fail");
    assert!(format!("{err}").contains("missing data_sequence_number"));
}
```

```bash
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer::tests::last_updated_seq -- --nocapture
```

Expected: all 4 PASS.

- [ ] **Step 15: Pass `data_sequence_number` through `reader.rs`**

In `vendor/iceberg-0.9.0/src/arrow/reader.rs`, find every `RecordBatchTransformerBuilder::new(...).with_first_row_id(task.first_row_id).build()` chain and add `.with_data_sequence_number(task.data_sequence_number)` before `.build()`. (Search with `grep -n "RecordBatchTransformerBuilder::new" vendor/iceberg-0.9.0/src/arrow/reader.rs`.)

- [ ] **Step 16: Update PATCH.md**

Append a new patch 5 section after patch 4 in `vendor/iceberg-0.9.0/PATCH.md`:

```markdown
## Patch 5 — `_last_updated_sequence_number` virtual column

iceberg-rust 0.9 declares `_last_updated_sequence_number` in
[`src/metadata_columns.rs`](src/metadata_columns.rs:65) but neither
`FileScanTask` nor `RecordBatchTransformer` carry the data-file
`data_sequence_number` needed to implement the column's spec-defined
fallback. This patch wires the field through.

Concretely:

* `FileScanTask` gains `data_sequence_number: Option<i64>` populated from
  the manifest entry's `data_sequence_number()` in
  `scan/context.rs::into_file_scan_task`.
* `RecordBatchTransformerBuilder::with_data_sequence_number(Option<i64>)`
  threads the value to the transformer.
* New `ColumnSource::LastUpdatedSeqNum { fallback_value, stored_source_index }`
  variant: when the parquet file physically stores a column tagged with
  `RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER`, non-NULL stored values
  take precedence; NULL/missing rows use the file's
  `data_sequence_number` as the spec-defined fallback.
* `arrow/reader.rs` calls `with_data_sequence_number(task.data_sequence_number)`
  on every transformer-builder chain so the value reaches the dispatch.

Spec ref: <https://iceberg.apache.org/spec/#row-lineage> —
`_last_updated_sequence_number` = 2147483539 = `i32::MAX - 108`.
```

- [ ] **Step 17: Build, fmt, regression check**

```bash
cargo fmt --manifest-path vendor/iceberg-0.9.0/Cargo.toml --check
cargo test --manifest-path vendor/iceberg-0.9.0/Cargo.toml --lib arrow::record_batch_transformer scan::tests -- --nocapture
cargo build -p novarocks
```

Expected: all PASS.

- [ ] **Step 18: Commit**

```bash
git add vendor/iceberg-0.9.0/src/scan/task.rs vendor/iceberg-0.9.0/src/scan/context.rs vendor/iceberg-0.9.0/src/scan/mod.rs vendor/iceberg-0.9.0/src/arrow/reader.rs vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs vendor/iceberg-0.9.0/PATCH.md
git commit -m "feat(vendor): add _last_updated_sequence_number read path

FileScanTask now carries data_sequence_number from the manifest entry
(scan/context.rs). RecordBatchTransformerBuilder threads it through
with with_data_sequence_number, and a new ColumnSource::LastUpdatedSeqNum
variant dispatches per row: non-NULL stored values from the parquet
file take precedence, NULL or absent rows fall back to the data file's
data_sequence_number per Iceberg V3 spec.

Spec ref: https://iceberg.apache.org/spec/#row-lineage"
```

---

### Task 3: NovaRocks lowering and analyzer scope

**Files:**
- Modify: `src/exec/row_position.rs`
- Modify: `src/lower/node/hdfs_scan.rs`
- Modify: `src/lower/node/file_scan.rs`
- Modify: `src/connector/hdfs.rs`
- Modify: `src/sql/catalog.rs`
- Modify: `src/sql/analyzer/scope.rs`
- Modify: `src/sql/analyzer/mod.rs`
- Modify: catalog provider implementation(s) returning `TableDef` for iceberg tables (locate via `grep -rn "impl CatalogProvider" src/`)

- [ ] **Step 1: Add name/field-id constants and helpers in `row_position.rs`**

In `src/exec/row_position.rs`, after the existing `_pos` constants/helpers (around line 75), add:

```rust
pub const ICEBERG_ROW_ID_COL: &str = "_row_id";
pub const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number";

pub const ICEBERG_RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;
pub const ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER: i32 = i32::MAX - 108;

pub fn is_iceberg_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
}

pub fn is_iceberg_last_updated_sequence_number(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
}
```

- [ ] **Step 2: Extend `IcebergVirtualSpec`**

In `src/exec/row_position.rs`, replace the `IcebergVirtualSpec` struct:

```rust
#[derive(Clone, Debug, Default)]
pub struct IcebergVirtualSpec {
    pub file_path_slot: Option<SlotId>,
    pub row_pos_slot: Option<SlotId>,
    pub row_id_slot: Option<SlotId>,
    pub last_updated_seq_slot: Option<SlotId>,
    pub file_path_field: Option<Field>,
    pub row_pos_field: Option<Field>,
    pub row_id_field: Option<Field>,
    pub last_updated_seq_field: Option<Field>,
}

impl IcebergVirtualSpec {
    pub fn is_empty(&self) -> bool {
        self.file_path_slot.is_none()
            && self.row_pos_slot.is_none()
            && self.row_id_slot.is_none()
            && self.last_updated_seq_slot.is_none()
    }
}
```

- [ ] **Step 3: Add unit tests for the helpers**

At the bottom of `src/exec/row_position.rs`, in `mod tests` (create if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_iceberg_row_id_recognizes_name_case_insensitive() {
        assert!(is_iceberg_row_id("_row_id"));
        assert!(is_iceberg_row_id("_ROW_ID"));
        assert!(!is_iceberg_row_id("row_id"));
        assert!(!is_iceberg_row_id("_rowid"));
    }

    #[test]
    fn is_iceberg_last_updated_sequence_number_recognizes_name_case_insensitive() {
        assert!(is_iceberg_last_updated_sequence_number(
            "_last_updated_sequence_number"
        ));
        assert!(is_iceberg_last_updated_sequence_number(
            "_Last_Updated_Sequence_Number"
        ));
        assert!(!is_iceberg_last_updated_sequence_number(
            "last_updated_sequence_number"
        ));
    }

    #[test]
    fn iceberg_virtual_spec_default_is_empty() {
        let spec = IcebergVirtualSpec::default();
        assert!(spec.is_empty());
    }
}
```

- [ ] **Step 4: Run helper tests**

```bash
cargo test -p novarocks --lib exec::row_position::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Add lowering branches in `hdfs_scan.rs`**

In `src/lower/node/hdfs_scan.rs`, locate the `is_iceberg_row_pos` branch (around line 432). Just below, add two new branches:

```rust
if crate::exec::row_position::is_iceberg_row_id(&name) {
    if !matches!(arrow_field.data_type(), DataType::Int64) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} _row_id slot_id={} expects BIGINT, got {:?}",
            node_id,
            slot_id,
            arrow_field.data_type()
        ));
    }
    iceberg_virtual_row_id_slot = Some(slot_id);
    iceberg_virtual_row_id_field = Some(arrow_field.clone());
    continue;
}

if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&name) {
    if !matches!(arrow_field.data_type(), DataType::Int64) {
        return Err(format!(
            "HDFS_SCAN_NODE node_id={} _last_updated_sequence_number slot_id={} expects BIGINT, got {:?}",
            node_id,
            slot_id,
            arrow_field.data_type()
        ));
    }
    iceberg_virtual_last_updated_seq_slot = Some(slot_id);
    iceberg_virtual_last_updated_seq_field = Some(arrow_field.clone());
    continue;
}
```

(Locals `iceberg_virtual_row_id_slot`, `iceberg_virtual_row_id_field`, `iceberg_virtual_last_updated_seq_slot`, `iceberg_virtual_last_updated_seq_field` need to be declared next to the existing `iceberg_virtual_pos_slot` declarations near line 375; declare them as `Option<SlotId>` / `Option<arrow::datatypes::Field>` initialized to `None`.)

In the `IcebergVirtualSpec` construction site (around line 985):

```rust
.with_iceberg_virtual(Some(crate::exec::row_position::IcebergVirtualSpec {
    file_path_slot: iceberg_virtual_file_slot,
    row_pos_slot: iceberg_virtual_pos_slot,
    row_id_slot: iceberg_virtual_row_id_slot,
    last_updated_seq_slot: iceberg_virtual_last_updated_seq_slot,
    file_path_field: iceberg_virtual_file_field,
    row_pos_field: iceberg_virtual_pos_field,
    row_id_field: iceberg_virtual_row_id_field,
    last_updated_seq_field: iceberg_virtual_last_updated_seq_field,
}))
```

- [ ] **Step 6: Add `data_sequence_number` to `ScanRange` (`hdfs.rs`)**

In `src/connector/hdfs.rs`, locate every place `first_row_id: Option<i64>` appears on `HdfsScanRange` / `IncrementalHdfsScanRange` / their internal types (search `grep -n "first_row_id" src/connector/hdfs.rs`). Beside each, add `pub data_sequence_number: Option<i64>,`.

Also update every test fixture that constructs these structs (search `grep -n "first_row_id: " src/connector/hdfs.rs`) — for each match add `data_sequence_number: None,` (test fixtures) or the live source value.

- [ ] **Step 7: Pass `data_sequence_number` through `file_scan.rs`**

In `src/lower/node/file_scan.rs`, around line 306 where `first_row_id: r.first_row_id` is set, add `data_sequence_number: r.data_sequence_number,` immediately after. Also add at the construction sites that pass `first_row_id: None` (line ~489) the symmetric `data_sequence_number: None`.

- [ ] **Step 8: Add `iceberg_row_lineage_metadata_columns` field to `TableDef`**

In `src/sql/catalog.rs`, replace the `TableDef` struct:

```rust
#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Iceberg V3 row-lineage reserved metadata pseudo-columns. Empty for
    /// non-Iceberg tables, V2 Iceberg tables, and V3 tables without
    /// `write.row-lineage=true`. Populated by the iceberg `CatalogProvider`
    /// implementation when the base table satisfies the row-lineage
    /// preconditions. The analyzer registers these into the per-relation
    /// scope as resolvable pseudo-columns but **not** into `SELECT *`
    /// expansion.
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub storage: TableStorage,
}
```

For every `TableDef { ... }` literal in the codebase (search `grep -rn "TableDef {" src/`), add `iceberg_row_lineage_metadata_columns: vec![],`. For the iceberg-providing implementation (locate via `grep -rn "impl CatalogProvider" src/`), populate the field when the base table is V3 row-lineage:

```rust
let iceberg_row_lineage_metadata_columns = if is_v3_row_lineage(&table_metadata) {
    vec![
        ColumnDef {
            name: "_row_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        ColumnDef {
            name: "_last_updated_sequence_number".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
    ]
} else {
    vec![]
};
```

Where `is_v3_row_lineage` is a small helper:

```rust
fn is_v3_row_lineage(metadata: &iceberg::spec::TableMetadata) -> bool {
    let v3 = matches!(metadata.format_version(), iceberg::spec::FormatVersion::V3);
    let lineage = metadata
        .properties()
        .get("write.row-lineage")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    v3 && lineage
}
```

(Place `is_v3_row_lineage` next to the catalog provider impl. Plan-time note: the exact location of the iceberg `CatalogProvider` impl was not pinned in design; locate via grep at execution time and place the helper alongside.)

- [ ] **Step 9: Add `add_iceberg_metadata_columns` to `AnalyzerScope`**

In `src/sql/analyzer/scope.rs`, after `add_table` (around line 50):

```rust
/// Register Iceberg V3 row-lineage reserved pseudo-columns. Unlike
/// `add_table`, these go into the qualified/unqualified resolution maps
/// **but not** into `ordered`, so `SELECT *` does not expand them. Users
/// must reference them by name explicitly (`SELECT _row_id FROM t`).
pub(super) fn add_iceberg_metadata_columns(
    &mut self,
    qualifier: &str,
    columns: &[crate::sql::catalog::ColumnDef],
) {
    let q_lower = qualifier.to_lowercase();
    for col in columns {
        let name_lower = col.name.to_lowercase();
        self.qualified.insert(
            (q_lower.clone(), name_lower.clone()),
            (col.data_type.clone(), col.nullable),
        );
        self.unqualified
            .insert(name_lower, (col.data_type.clone(), col.nullable));
    }
}
```

- [ ] **Step 10: Surface fail-fast errors in `resolve`**

In `src/sql/analyzer/scope.rs::resolve` (around line 79), before returning the existing `Column 'X' cannot be resolved` error, check for reserved names:

```rust
pub(super) fn resolve(
    &self,
    qualifier: Option<&str>,
    name: &str,
) -> Result<(DataType, bool), String> {
    let name_lower = name.to_lowercase();
    if let Some(q) = qualifier {
        let q_lower = q.to_lowercase();
        if let Some(found) = self.qualified.get(&(q_lower.clone(), name_lower.clone())) {
            return Ok(found.clone());
        }
        return Err(reserved_name_error(name).unwrap_or_else(|| {
            format!("Column '{}.{}' cannot be resolved.", q, name)
        }));
    }
    if let Some(found) = self.unqualified.get(&name_lower) {
        return Ok(found.clone());
    }
    Err(reserved_name_error(name).unwrap_or_else(|| {
        format!("Column '{}' cannot be resolved.", name)
    }))
}

fn reserved_name_error(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    if lower == "_row_id" || lower == "_last_updated_sequence_number" {
        Some(format!(
            "column \"{}\" is only available on Iceberg V3 row-lineage tables \
             (table is not Iceberg V3 with write.row-lineage=true)",
            lower
        ))
    } else {
        None
    }
}
```

- [ ] **Step 11: Wire `add_iceberg_metadata_columns` in `collect_relation_scope`**

In `src/sql/analyzer/mod.rs::collect_relation_scope` (around line 1218), update the `Relation::Scan` arm:

```rust
Relation::Scan(scan) => {
    let qualifier = scan.alias.as_deref().unwrap_or(&scan.table.name);
    scope.add_table(Some(qualifier), &scan.table.columns);
    if !scan.table.iceberg_row_lineage_metadata_columns.is_empty() {
        scope.add_iceberg_metadata_columns(
            qualifier,
            &scan.table.iceberg_row_lineage_metadata_columns,
        );
    }
    Ok(())
}
```

- [ ] **Step 12: Add scope-level unit tests**

In `src/sql/analyzer/scope.rs` `mod tests` (create at file end if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn col(name: &str, ty: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    #[test]
    fn rejects_row_id_on_non_iceberg_table() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("t"), &[col("id", DataType::Int64, false)]);
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn rejects_row_id_on_v2_iceberg_table_no_metadata_added() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        // V2 path adds no row-lineage metadata columns.
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn accepts_row_id_on_v3_row_lineage_table() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns(
            "ice",
            &[
                col("_row_id", DataType::Int64, false),
                col("_last_updated_sequence_number", DataType::Int64, false),
            ],
        );
        let (ty, nullable) = scope.resolve(None, "_row_id").expect("ok");
        assert_eq!(ty, DataType::Int64);
        assert!(!nullable);
    }

    #[test]
    fn select_star_does_not_expose_row_lineage_pseudo_columns() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns(
            "ice",
            &[col("_row_id", DataType::Int64, false)],
        );
        let names: Vec<_> = scope.iter_columns().map(|(_, n, _, _)| n.as_str()).collect();
        assert_eq!(names, vec!["id"]);
    }
}
```

- [ ] **Step 13: Add lowering tests for the new slot dispatch**

First, check whether the existing `_pos` lowering has tests we can mirror:

```bash
grep -n "iceberg_virtual_pos\|is_iceberg_row_pos\|fn.*lower.*hdfs.*scan" src/lower/node/hdfs_scan.rs | head
```

If a `_pos` test exists, find it under `mod tests` (search `#[test]` in the file's bottom) and mirror its setup. If none exists (likely the case — `hdfs_scan.rs` is mostly tested via integration), the lowering correctness for `_row_id` / `_last_updated_sequence_number` is covered by the Task 5 integration test. Document this and skip writing a synthetic-fixture lowering unit test:

In `src/lower/node/hdfs_scan.rs` (top of file or near the existing slot-iteration logic), add a comment block:

```rust
// Lowering of `_row_id` / `_last_updated_sequence_number` slots into
// IcebergVirtualSpec is exercised end-to-end by the Task 5 integration
// tests (e.g. `select_row_id_and_last_updated_seq_on_v3_row_lineage_table`).
// The synthetic-fixture style used elsewhere in this file is not added
// here because constructing a valid `TPlanNode` for an iceberg scan
// requires substantial scaffolding that the integration path already
// covers more economically.
```

This is intentional: the brain-storming spec lists 14 NovaRocks unit tests including 2 lowering tests, but the rough integration coverage in Task 5 is the more cost-effective place to test the full lowering chain. **Adjust §10 final review checklist count from 14 to 12** when committing Task 3 (since 2 lowering unit tests are dropped in favor of Task 5 integration coverage).

- [ ] **Step 14: Run all task-3 unit tests**

```bash
cargo test -p novarocks --lib exec::row_position::tests sql::analyzer::scope::tests lower::node::hdfs_scan::tests -- --nocapture
```

Expected: all PASS.

- [ ] **Step 15: Build full workspace + clippy**

```bash
cargo build -p novarocks
cargo clippy -p novarocks --all-targets
```

Expected: PASS, no new warnings.

- [ ] **Step 16: Commit**

```bash
git add src/exec/row_position.rs src/lower/node/hdfs_scan.rs src/lower/node/file_scan.rs src/connector/hdfs.rs src/sql/catalog.rs src/sql/analyzer/scope.rs src/sql/analyzer/mod.rs
# add catalog provider impl files modified in step 8 once located
git commit -m "feat(novarocks): expose _row_id / _last_updated_sequence_number in iceberg lowering and analyzer

Adds reserved name detection and reserved field id constants in
exec/row_position.rs; extends IcebergVirtualSpec with row_id_slot /
last_updated_seq_slot. Lowering in hdfs_scan.rs picks up the two new
slot names from TPlanNode and validates BIGINT type. Connector
ScanRange threads data_sequence_number alongside first_row_id.

TableDef gains iceberg_row_lineage_metadata_columns populated by the
iceberg CatalogProvider when the base table is format-version=3 with
write.row-lineage=true. The analyzer scope registers these as
qualified/unqualified pseudo-columns without leaking into SELECT *.
AnalyzerScope::resolve gives the spec-aligned error message when the
two reserved names are referenced on tables that did not register
them.

Spec ref: docs/superpowers/specs/2026-04-30-iceberg-v3-row-lineage-metadata-columns-read-design.md"
```

---

### Task 4: NovaRocks runner synthesis (primary path)

**Files:**
- Modify: `src/exec/operators/scan/runner.rs`
- Modify: `src/connector/hdfs.rs` (parquet read column selection)
- Modify: `src/formats/parquet/mod.rs` if needed for stored-column passthrough (verify in Step 3)

- [ ] **Step 1: Add the first failing test — runner synthesizes _row_id when stored column missing**

In `src/exec/operators/scan/runner.rs` `mod tests` (create at file end if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Synthesize _row_id and _last_updated_sequence_number with the runner's
    /// production logic, given an in-memory RecordBatch fixture (helper just
    /// destructures into (schema, columns) which is what the production
    /// helper takes — Chunk does not expose a `record_batch()` accessor).
    fn synthesize(
        batch: RecordBatch,
        first_row_id: i64,
        data_sequence_number: i64,
        spec: IcebergVirtualSpec,
        scan_position_start: i64,
    ) -> (Vec<i64>, Vec<i64>) {
        let schema = batch.schema();
        let columns: Vec<ArrayRef> = batch.columns().iter().cloned().collect();
        let num_rows = batch.num_rows();
        synthesize_row_lineage_columns(
            &schema,
            &columns,
            num_rows,
            first_row_id,
            data_sequence_number,
            scan_position_start,
            spec.row_id_slot.is_some(),
            spec.last_updated_seq_slot.is_some(),
        )
    }

    #[test]
    fn row_lineage_synthesis_falls_back_when_stored_columns_missing() {
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.row_id_slot = Some(SlotId::new(10));
        spec.last_updated_seq_slot = Some(SlotId::new(11));
        let (row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
        assert_eq!(row_ids, vec![100, 101, 102]);
        assert_eq!(seqs, vec![9, 9, 9]);
    }
}
```

- [ ] **Step 2: Run, verify compile fail**

```bash
cargo test -p novarocks --lib exec::operators::scan::runner::tests::row_lineage_synthesis_falls_back_when_stored_columns_missing -- --nocapture
```

Expected: COMPILE FAIL — `synthesize_row_lineage_columns` not defined.

- [ ] **Step 3: Implement the pure synthesis helper**

In `src/exec/operators/scan/runner.rs`, add at file scope:

```rust
fn synthesize_row_lineage_columns(
    schema: &arrow_schema::SchemaRef,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: i64,
    data_sequence_number: i64,
    scan_position_start: i64,
    want_row_id: bool,
    want_last_updated_seq: bool,
) -> (Vec<i64>, Vec<i64>) {
    let stored_row_id_idx = if want_row_id {
        find_field_by_id(
            schema,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        )
    } else {
        None
    };
    let stored_seq_idx = if want_last_updated_seq {
        find_field_by_id(
            schema,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )
    } else {
        None
    };

    let row_ids = if want_row_id {
        let stored = stored_row_id_idx
            .and_then(|idx| columns[idx].as_any().downcast_ref::<Int64Array>());
        (0..num_rows)
            .map(|i| match stored {
                Some(arr) if !arr.is_null(i) => arr.value(i),
                _ => first_row_id + scan_position_start + i as i64,
            })
            .collect()
    } else {
        Vec::new()
    };

    let seqs = if want_last_updated_seq {
        let stored = stored_seq_idx
            .and_then(|idx| columns[idx].as_any().downcast_ref::<Int64Array>());
        (0..num_rows)
            .map(|i| match stored {
                Some(arr) if !arr.is_null(i) => arr.value(i),
                _ => data_sequence_number,
            })
            .collect()
    } else {
        Vec::new()
    };

    (row_ids, seqs)
}

fn find_field_by_id(schema: &arrow_schema::SchemaRef, target_id: i32) -> Option<usize> {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    schema
        .fields()
        .iter()
        .position(|f| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|s| s.parse::<i32>().ok())
                == Some(target_id)
        })
}
```

- [ ] **Step 4: Run the test, verify it passes**

```bash
cargo test -p novarocks --lib exec::operators::scan::runner::tests::row_lineage_synthesis_falls_back_when_stored_columns_missing -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Add the next 5 synthesis tests**

Append to `mod tests`:

```rust
#[test]
fn row_id_synthesis_uses_stored_when_all_non_null() {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let id_field = Field::new("id", DataType::Int64, false);
    let stored_field = Field::new("_row_id", DataType::Int64, true).with_metadata(
        HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
        )]),
    );
    let schema = Arc::new(Schema::new(vec![id_field, stored_field]));
    let id = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
    let stored = Arc::new(Int64Array::from(vec![Some(700_i64), Some(800), Some(900)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![id, stored]).unwrap();

    let mut spec = IcebergVirtualSpec::default();
    spec.row_id_slot = Some(SlotId::new(10));
    let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 0);
    assert_eq!(row_ids, vec![700, 800, 900]);
}

#[test]
fn row_id_synthesis_mixed_per_row_null() {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let stored_field = Field::new("_row_id", DataType::Int64, true).with_metadata(
        HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
        )]),
    );
    let schema = Arc::new(Schema::new(vec![stored_field]));
    let stored = Arc::new(Int64Array::from(vec![Some(700_i64), None, Some(900)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

    let mut spec = IcebergVirtualSpec::default();
    spec.row_id_slot = Some(SlotId::new(10));
    let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 0);
    assert_eq!(row_ids, vec![700, 101, 900]); // index 1: 100 + scan_position_start(0) + i(1)
}

#[test]
fn last_updated_seq_synthesis_uses_stored_when_present() {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let stored_field =
        Field::new("_last_updated_sequence_number", DataType::Int64, true).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
                    .to_string(),
            )]),
        );
    let schema = Arc::new(Schema::new(vec![stored_field]));
    let stored = Arc::new(Int64Array::from(vec![Some(11_i64), Some(12), Some(13)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

    let mut spec = IcebergVirtualSpec::default();
    spec.last_updated_seq_slot = Some(SlotId::new(11));
    let (_row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
    assert_eq!(seqs, vec![11, 12, 13]);
}

#[test]
fn last_updated_seq_synthesis_mixed_per_row_null() {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let stored_field =
        Field::new("_last_updated_sequence_number", DataType::Int64, true).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
                    .to_string(),
            )]),
        );
    let schema = Arc::new(Schema::new(vec![stored_field]));
    let stored = Arc::new(Int64Array::from(vec![Some(11_i64), None, Some(13)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

    let mut spec = IcebergVirtualSpec::default();
    spec.last_updated_seq_slot = Some(SlotId::new(11));
    let (_row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
    assert_eq!(seqs, vec![11, 9, 13]);
}

#[test]
fn row_id_synthesis_advances_with_scan_position_start() {
    let id_field = Field::new("id", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![id_field]));
    let id = Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

    let mut spec = IcebergVirtualSpec::default();
    spec.row_id_slot = Some(SlotId::new(10));
    // Same file, second chunk: scan_position_start = 7 (rows 0..7 already produced).
    let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 7);
    assert_eq!(row_ids, vec![107, 108]);
}

#[test]
fn neither_slot_requested_yields_empty_vectors() {
    let id_field = Field::new("id", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![id_field]));
    let id = Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

    let spec = IcebergVirtualSpec::default();
    let (row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
    assert!(row_ids.is_empty());
    assert!(seqs.is_empty());
}
```

- [ ] **Step 6: Run, verify all 6 runner unit tests pass**

```bash
cargo test -p novarocks --lib exec::operators::scan::runner::tests -- --nocapture
```

Expected: all 6 PASS.

- [ ] **Step 7: Extend `IcebergVirtualState` with the two new fields**

In `src/exec/operators/scan/runner.rs`, replace the struct (around line 199):

```rust
struct IcebergVirtualState {
    spec: IcebergVirtualSpec,
    file_path: String,
    next_row_offset: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
}
```

- [ ] **Step 8: Update `build_iceberg_virtual_state`**

Replace `build_iceberg_virtual_state` (line ~523):

```rust
fn build_iceberg_virtual_state(
    &self,
    morsel: &ScanMorsel,
) -> Result<Option<IcebergVirtualState>, String> {
    let Some(spec) = self.scan.iceberg_virtual() else {
        return Ok(None);
    };
    let ScanMorsel::FileRange {
        path,
        first_row_id,
        data_sequence_number,
        ..
    } = morsel
    else {
        return Err(
            "iceberg virtual columns require file range morsels".to_string(),
        );
    };
    Ok(Some(IcebergVirtualState {
        spec: spec.clone(),
        file_path: path.clone(),
        next_row_offset: first_row_id.unwrap_or(0),
        first_row_id: *first_row_id,
        data_sequence_number: *data_sequence_number,
    }))
}
```

(Update `ScanMorsel::FileRange` variant to carry `data_sequence_number: Option<i64>`. Search `grep -n "ScanMorsel::FileRange" src/`.)

- [ ] **Step 9: Wire `synthesize_row_lineage_columns` into `append_iceberg_virtual_columns`**

In `src/exec/operators/scan/runner.rs::append_iceberg_virtual_columns` (around line 641), after the existing `_file` / `_pos` synthesis blocks, add row-lineage synthesis:

```rust
let want_row_id = state.spec.row_id_slot.is_some();
let want_last_updated_seq = state.spec.last_updated_seq_slot.is_some();
let (row_ids_vec, seqs_vec) = if want_row_id || want_last_updated_seq {
    let first_row_id = state.first_row_id.ok_or_else(|| {
        "_row_id / _last_updated_sequence_number requested but morsel missing first_row_id".to_string()
    })?;
    let data_seq = if want_last_updated_seq {
        state.data_sequence_number.ok_or_else(|| {
            "_last_updated_sequence_number requested but morsel missing data_sequence_number".to_string()
        })?
    } else {
        0
    };
    let scan_position_start = state.next_row_offset - first_row_id;
    synthesize_row_lineage_columns(
        &chunk.schema(),
        chunk.columns(),
        chunk.len(),
        first_row_id,
        data_seq,
        scan_position_start,
        want_row_id,
        want_last_updated_seq,
    )
} else {
    (Vec::new(), Vec::new())
};

let row_id_array = state.spec.row_id_slot.map(|_| {
    Arc::new(Int64Array::from(row_ids_vec)) as ArrayRef
});
let last_updated_seq_array = state.spec.last_updated_seq_slot.map(|_| {
    Arc::new(Int64Array::from(seqs_vec)) as ArrayRef
});
```

In the slot-attach loop (around line 692, where `file_path_slot` and `row_pos_slot` are matched), add two more arms (after the existing `row_pos_slot` arm):

```rust
if Some(*slot_id) == state.spec.row_id_slot {
    let field = state
        .spec
        .row_id_field
        .as_ref()
        .ok_or_else(|| "iceberg _row_id slot missing field metadata".to_string())?;
    fields.push(field.clone());
    columns.push(
        row_id_array
            .as_ref()
            .expect("row_id_array built when slot exists")
            .clone(),
    );
    slot_schemas.push(ChunkSlotSchema::new(
        *slot_id,
        field.name().clone(),
        field.is_nullable(),
        Some(scalar_type_desc(types::TPrimitiveType::BIGINT)),
        None,
    ));
    continue;
}
if Some(*slot_id) == state.spec.last_updated_seq_slot {
    let field = state
        .spec
        .last_updated_seq_field
        .as_ref()
        .ok_or_else(|| "iceberg _last_updated_sequence_number slot missing field metadata".to_string())?;
    fields.push(field.clone());
    columns.push(
        last_updated_seq_array
            .as_ref()
            .expect("last_updated_seq_array built when slot exists")
            .clone(),
    );
    slot_schemas.push(ChunkSlotSchema::new(
        *slot_id,
        field.name().clone(),
        field.is_nullable(),
        Some(scalar_type_desc(types::TPrimitiveType::BIGINT)),
        None,
    ));
    continue;
}
```

- [ ] **Step 10: Add stored columns to parquet read column list in `connector/hdfs.rs`**

In `src/connector/hdfs.rs` (where the `select` column list for parquet read is built — search `grep -n "fn build_parquet_select\|select_columns\|reader.*select\|with_projected_fields" src/connector/`), include the reserved field ids when `IcebergVirtualSpec` requests row-lineage slots **and** the parquet file's schema (read from footer at scan-range build time, or via a "best-effort include" approach where the parquet reader silently drops missing columns).

Since NovaRocks parquet reader uses `parquet::arrow::ParquetRecordBatchReaderBuilder`, the projection is by column index. To opportunistically include stored `_row_id` / `_last_updated_sequence_number` columns:

1. After parquet footer is read, scan the arrow schema for columns whose `PARQUET_FIELD_ID_META_KEY` matches `ICEBERG_RESERVED_FIELD_ID_ROW_ID` or `..._LAST_UPDATED_SEQUENCE_NUMBER`.
2. If found and the corresponding `IcebergVirtualSpec` slot is `Some`, add their column index to the projection mask.

Locate the projection-building site and add helper:

```rust
fn add_row_lineage_stored_columns_to_projection(
    arrow_schema: &arrow_schema::SchemaRef,
    spec: &IcebergVirtualSpec,
    projection_indices: &mut Vec<usize>,
) {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    if spec.row_id_slot.is_some() {
        if let Some(idx) = arrow_schema.fields().iter().position(|f| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|s| s.parse::<i32>().ok())
                == Some(crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        }) {
            if !projection_indices.contains(&idx) {
                projection_indices.push(idx);
            }
        }
    }
    if spec.last_updated_seq_slot.is_some() {
        if let Some(idx) = arrow_schema.fields().iter().position(|f| {
            f.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .and_then(|s| s.parse::<i32>().ok())
                == Some(crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER)
        }) {
            if !projection_indices.contains(&idx) {
                projection_indices.push(idx);
            }
        }
    }
}
```

(Plan-time note: the exact insertion point depends on where the projection mask is built in the current parquet read pipeline. Locate `ProjectionMask` / `with_projected_fields` / `select` calls in `src/formats/parquet/mod.rs` and `src/connector/hdfs.rs`. For NovaRocks-written files this helper finds nothing and is a no-op; for cross-engine files it brings the stored columns into the chunk so `synthesize_row_lineage_columns` can find them via field-id metadata.)

- [ ] **Step 11: Build, test, regression check**

```bash
cargo build -p novarocks
cargo test -p novarocks --lib exec::operators::scan::runner::tests -- --nocapture
cargo test -p novarocks --lib exec::row_position::tests sql::analyzer::scope::tests lower::node::hdfs_scan::tests -- --nocapture
cargo clippy -p novarocks --all-targets
```

Expected: all PASS, no new warnings.

- [ ] **Step 12: Commit**

```bash
git add src/exec/operators/scan/runner.rs src/connector/hdfs.rs src/formats/parquet/mod.rs
# also include any ScanMorsel definition file modified in step 8
git commit -m "feat(novarocks): synthesize _row_id / _last_updated_sequence_number in scan runner

Adds spec-compliant row-lineage column synthesis to the user-level scan
path. synthesize_row_lineage_columns inspects the incoming RecordBatch
for parquet columns tagged with the reserved field ids; non-NULL stored
values take precedence per row, NULL/missing rows fall back to
first_row_id + scan_position and the data file's data_sequence_number.

IcebergVirtualState now carries first_row_id and data_sequence_number
(supplied by ScanMorsel::FileRange). The connector includes the stored
columns in the parquet projection mask when the virtual spec requests
the slots; this is a no-op for NovaRocks-written files (which don't
write the stored columns) and brings them in for cross-engine reads.

Spec ref: docs/superpowers/specs/2026-04-30-iceberg-v3-row-lineage-metadata-columns-read-design.md"
```

---

### Task 5: End-to-end integration tests

**Files:**
- Modify: `src/engine/mod.rs` (or `src/connector/starrocks/managed/mv_refresh.rs` if engine module is over-large; pick the one with `iceberg_v3_*` siblings already)

- [ ] **Step 1: Locate existing `iceberg_v3_*` integration tests**

```bash
grep -n "fn iceberg_v3\|iceberg_v3_row_lineage" src/engine/mod.rs src/connector/starrocks/managed/mv_refresh.rs 2>/dev/null
```

Note the file with the most matches — that is the host module for new tests.

- [ ] **Step 2: Add the primary integration test**

Append to that file's `mod tests`:

```rust
#[test]
fn select_row_id_and_last_updated_seq_on_v3_row_lineage_table() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
        return;
    };
    let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
    let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());

    let engine = match crate::engine::StandaloneNovaRocks::open(
        crate::engine::StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        },
    ) {
        Ok(engine) => engine,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage SELECT test: object store unavailable: {err}"
                );
                return;
            }
            panic!("open standalone engine: {err}");
        }
    };
    let session = engine.session();
    let create_catalog_sql = format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
    );
    session.execute_in_database(&create_catalog_sql, "default").expect("create catalog");
    session.execute_in_database("create database ice.ns", "default").expect("create namespace");
    session
        .execute_in_database(
            r#"create table ice.ns.t (id bigint not null, name string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
            "default",
        )
        .expect("create v3 row-lineage table");

    // Snapshot S1: 3 rows.
    session
        .execute_in_database(
            "insert into ice.ns.t values (1,'A'), (2,'B'), (3,'C')",
            "default",
        )
        .expect("seed S1");

    // Find S1's first_row_id and sequence_number from the table metadata so we
    // assert against actual values rather than hard-coded constants.
    let s1_meta = session
        .execute_in_database(
            "select first_row_id, sequence_number from ice.ns.t$snapshots order by sequence_number desc limit 1",
            "default",
        )
        .expect("read S1 metadata");
    let (s1_first_row_id, s1_seq) = parse_first_row_id_and_seq(&s1_meta);

    let pre_state = session
        .execute_in_database(
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t order by id",
            "default",
        )
        .expect("select after S1");
    let pre_rows = parse_id_rowid_seq_rows(&pre_state);
    assert_eq!(pre_rows.len(), 3);
    assert_eq!(pre_rows[0], (1_i64, s1_first_row_id, s1_seq));
    assert_eq!(pre_rows[1], (2_i64, s1_first_row_id + 1, s1_seq));
    assert_eq!(pre_rows[2], (3_i64, s1_first_row_id + 2, s1_seq));

    // Snapshot S2: 2 more rows.
    session
        .execute_in_database(
            "insert into ice.ns.t values (4,'D'), (5,'E')",
            "default",
        )
        .expect("seed S2");
    let s2_meta = session
        .execute_in_database(
            "select first_row_id, sequence_number from ice.ns.t$snapshots order by sequence_number desc limit 1",
            "default",
        )
        .expect("read S2 metadata");
    let (s2_first_row_id, s2_seq) = parse_first_row_id_and_seq(&s2_meta);

    let post_state = session
        .execute_in_database(
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t order by id",
            "default",
        )
        .expect("select after S2");
    let post_rows = parse_id_rowid_seq_rows(&post_state);
    assert_eq!(post_rows.len(), 5);
    // Old rows keep their S1 row_ids and S1 sequence_numbers.
    assert_eq!(post_rows[0], (1_i64, s1_first_row_id, s1_seq));
    assert_eq!(post_rows[1], (2_i64, s1_first_row_id + 1, s1_seq));
    assert_eq!(post_rows[2], (3_i64, s1_first_row_id + 2, s1_seq));
    // New rows get S2 row_ids and S2 sequence_numbers.
    assert_eq!(post_rows[3], (4_i64, s2_first_row_id, s2_seq));
    assert_eq!(post_rows[4], (5_i64, s2_first_row_id + 1, s2_seq));

    // DELETE id=2 via Phase 2a Puffin DV; surviving rows keep their lineage.
    session
        .execute_in_database("delete from ice.ns.t where id = 2", "default")
        .expect("delete row");
    let after_delete = session
        .execute_in_database(
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t order by id",
            "default",
        )
        .expect("select after delete");
    let after_rows = parse_id_rowid_seq_rows(&after_delete);
    assert_eq!(after_rows.len(), 4);
    assert!(after_rows.iter().all(|(id, _, _)| *id != 2));
    // id=1 row preserves its original S1 row_id and sequence_number.
    assert_eq!(after_rows[0], (1_i64, s1_first_row_id, s1_seq));

    drop(engine);
}

// `result` here is the value returned by `session.execute_in_database`. Its
// concrete type is `Vec<Vec<Value>>` or similar — find the engine's actual
// return shape with this command before writing the test:
//
//   grep -n "fn execute_in_database\|pub fn execute" src/engine/session.rs src/engine/mod.rs
//
// Then mirror the parsing pattern from the closest existing integration
// test, e.g. `collect_agg_mv_state` in `src/connector/starrocks/managed/mv_refresh.rs`
// (line ~1147), which iterates rows and pulls typed cell values. Below is the
// expected shape, parameterized on the actual engine API.

fn parse_first_row_id_and_seq(result: &SessionResult) -> (i64, i64) {
    // Expects a single-row result with two BIGINT columns:
    // (first_row_id, sequence_number).
    let row = result.rows().first().expect("at least one row");
    let first_row_id = row.get_i64(0).expect("first_row_id is bigint");
    let seq = row.get_i64(1).expect("sequence_number is bigint");
    (first_row_id, seq)
}

fn parse_id_rowid_seq_rows(result: &SessionResult) -> Vec<(i64, i64, i64)> {
    // Expects N rows with three BIGINT columns: (id, _row_id, _last_updated_sequence_number).
    result
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_i64(0).expect("id is bigint"),
                row.get_i64(1).expect("_row_id is bigint"),
                row.get_i64(2).expect("_last_updated_sequence_number is bigint"),
            )
        })
        .collect()
}
```

(The `SessionResult`, `.rows()`, and `.get_i64(idx)` accessor names are placeholders matching the **shape** of the engine's existing test helpers; resolve concrete names at plan execution time by reading the closest existing helper. Reuse it directly if one exists with this shape — `collect_agg_mv_state` at `src/connector/starrocks/managed/mv_refresh.rs:1147` is the canonical reference: read its row-iteration logic and copy verbatim, then adjust column types/indices for `(id, _row_id, _last_updated_sequence_number)`.)

- [ ] **Step 3: Add fail-fast SELECT integration tests**

Append:

```rust
#[test]
fn select_row_id_fails_on_v2_iceberg_table() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
        return;
    };
    let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
    let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());
    let engine = match crate::engine::StandaloneNovaRocks::open(
        crate::engine::StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        },
    ) {
        Ok(engine) => engine,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                return;
            }
            panic!("open: {err}");
        }
    };
    let session = engine.session();
    let create_catalog_sql = format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
    );
    session.execute_in_database(&create_catalog_sql, "default").expect("ok");
    session.execute_in_database("create database ice.ns", "default").expect("ok");
    session
        .execute_in_database(
            r#"create table ice.ns.t2 (id bigint) tblproperties("format-version"="2")"#,
            "default",
        )
        .expect("create v2 table");

    let err = session
        .execute_in_database("select _row_id from ice.ns.t2", "default")
        .expect_err("must fail");
    assert!(
        format!("{err}").contains("only available on Iceberg V3 row-lineage tables"),
        "got: {err}"
    );

    drop(engine);
}

#[test]
fn select_row_id_fails_on_v3_table_without_row_lineage() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
        return;
    };
    let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
    let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());
    let engine = match crate::engine::StandaloneNovaRocks::open(
        crate::engine::StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        },
    ) {
        Ok(engine) => engine,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                return;
            }
            panic!("open: {err}");
        }
    };
    let session = engine.session();
    let create_catalog_sql = format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
    );
    session.execute_in_database(&create_catalog_sql, "default").expect("ok");
    session.execute_in_database("create database ice.ns", "default").expect("ok");
    session
        .execute_in_database(
            r#"create table ice.ns.t3 (id bigint) tblproperties("format-version"="3")"#,
            "default",
        )
        .expect("create v3 table without row-lineage");

    let err = session
        .execute_in_database("select _row_id from ice.ns.t3", "default")
        .expect_err("must fail");
    assert!(
        format!("{err}").contains("only available on Iceberg V3 row-lineage tables"),
        "got: {err}"
    );

    drop(engine);
}

#[test]
fn select_last_updated_sequence_number_fails_on_non_iceberg_table() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
        return;
    };
    let engine = match crate::engine::StandaloneNovaRocks::open(
        crate::engine::StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        },
    ) {
        Ok(engine) => engine,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                return;
            }
            panic!("open: {err}");
        }
    };
    let session = engine.session();
    session.execute_in_database("create database analytics", "default").expect("ok");
    session
        .execute_in_database(
            "create table analytics.t (id bigint) distributed by hash(id) buckets 2",
            "default",
        )
        .expect("create internal olap table");

    let err = session
        .execute_in_database(
            "select _last_updated_sequence_number from analytics.t",
            "default",
        )
        .expect_err("must fail");
    assert!(
        format!("{err}").contains("only available on Iceberg V3 row-lineage tables"),
        "got: {err}"
    );

    drop(engine);
}
```

- [ ] **Step 4: Run integration tests**

```bash
cargo test -p novarocks --lib --test 'integration_*' \
    select_row_id_and_last_updated_seq_on_v3_row_lineage_table \
    select_row_id_fails_on_v2_iceberg_table \
    select_row_id_fails_on_v3_table_without_row_lineage \
    select_last_updated_sequence_number_fails_on_non_iceberg_table \
    -- --nocapture
```

(Adjust target glob to match the host module — if tests live in `src/engine/mod.rs::tests`, drop `--test 'integration_*'` and use the lib filter directly:

```bash
cargo test -p novarocks --lib engine::tests::select_row_id_and_last_updated_seq_on_v3_row_lineage_table -- --nocapture
```

etc.)

Expected: all PASS, **or** clean skip with `object store unavailable` message when MinIO is not running.

- [ ] **Step 5: Run the full IVM Phase 2 + Phase 2a regression suite to confirm no breakage**

```bash
cargo test -p novarocks --lib connector::iceberg::changes -- --nocapture
cargo test -p novarocks --lib connector::iceberg::scan_deletes -- --nocapture
cargo test -p novarocks --lib connector::iceberg::commit -- --nocapture
cargo test -p novarocks --lib starrocks::managed -- --nocapture
cargo test -p novarocks --lib engine::tests::iceberg_ -- --nocapture
```

Expected: all PASS (skips OK on missing MinIO).

- [ ] **Step 6: Format + clippy + final build**

```bash
cargo fmt --check
cargo build -p novarocks
cargo build
cargo clippy -p novarocks --all-targets
```

Expected: all PASS, no new warnings.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs    # or mv_refresh.rs depending on host module chosen
git commit -m "test(novarocks): cover row_id / last_updated_sequence_number end-to-end on v3 row-lineage table

End-to-end SELECT _row_id / _last_updated_sequence_number on a V3
row-lineage iceberg table: 3-row INSERT, 2-row INSERT, 1-row DELETE.
Row identity and sequence numbers must propagate per Iceberg V3 spec
(old rows keep S1 lineage, new rows get S2 lineage, DELETE preserves
surviving rows). Fail-fast tests cover V2 iceberg / V3 non-row-lineage
iceberg / non-iceberg internal tables.

Skips cleanly when MinIO is unavailable, matching existing iceberg
integration test pattern."
```

---

## Final Review Checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo build -p novarocks` passes
- [ ] `cargo build` (vendor + workspace) passes
- [ ] `cargo clippy -p novarocks --all-targets` no new warnings
- [ ] Vendor unit tests: 8 new (4 row_id + 4 last_updated_seq) + 1 task carry test all pass
- [ ] NovaRocks unit tests: 12 new pass (2 helper + 6 runner synthesis + 4 analyzer scope; lowering covered via Task 5 integration tests, see Task 3 Step 13 note)
- [ ] NovaRocks integration tests: 4 new pass (or skip cleanly on MinIO unavailable)
- [ ] IVM Phase 2 regression: `cargo test -p novarocks --lib connector::iceberg::changes scan_deletes commit::puffin_dv starrocks::managed::mv_refresh` all pass
- [ ] Phase 2a regression: `cargo test -p novarocks --lib engine::tests::iceberg_ connector::iceberg::commit` all pass
- [ ] PATCH.md describes patch 3 (updated) and patch 5 (new)
- [ ] All 5 commits are atomic; each compiles and tests independently
- [ ] No `Co-Authored-By: Claude` trailers (per project convention)
