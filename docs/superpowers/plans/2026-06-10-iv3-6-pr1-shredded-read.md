# IV3-6 PR-1: Shredded Variant Read Correctness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** NovaRocks correctly reads Spark/iceberg-java-shredded variant parquet files (typed_value subtree) instead of silently nulling them out, by reconstructing the unshredded form via the upstream `unshred_variant` kernel.

**Architecture:** All variant read-side collapse logic moves into a new focused module `src/formats/parquet/variant_read.rs`. The struct-shape gate `is_variant_struct_data_type` learns the shredded layout (metadata + value and/or typed_value); `collapse_variant_struct_to_largebinary` becomes by-name and shredded-aware (detect `typed_value` → `VariantArray::try_new` → `unshred_variant` → existing metadata+value → `[size|metadata|value]` LargeBinary serialization). Silent-null on corrupt rows becomes a fail-fast error. Both read paths (iceberg schema-evolution align path and the non-iceberg `convert_variant_columns` path) delegate to the same function (DRY — today the logic is duplicated).

**Tech Stack:** Rust; `parquet::variant` re-exports (`VariantArray`, `unshred_variant`, `json_to_variant`, `shred_variant`, `ShreddedSchemaBuilder` — already available via `parquet = { version = "58.2.0", features = ["arrow", "variant_experimental"] }`, no Cargo.toml change). Existing `VariantValue` (`src/exec/variant.rs`) for the internal serialization.

**Background you must know (verified, with refs):**
- Internal variant form: `LargeBinary` rows `[size:u32 LE | metadata | value]`; `VariantValue::create(metadata, value)` / `.serialize()` build it (`src/exec/variant.rs:185-244`).
- Spark-shredded parquet variant = struct `{metadata: Binary, value: Binary (nullable), typed_value: <subtree> (nullable)}`. For shredded rows `value` is NULL and the data lives in `typed_value` — today's code turns those rows into variant-null (**silent data loss**, `src/formats/parquet/mod.rs:2060-2064`) on the non-iceberg path, and fails an opaque generic cast on the iceberg path because `is_variant_struct_data_type` requires exactly 2 fields (`mod.rs:1934-1947`).
- The two call paths into the collapse logic:
  1. Iceberg align path: `align_iceberg_array_to_field` special-case at `mod.rs:1608-1612` → `collapse_variant_struct_to_largebinary` (`mod.rs:1953-1985`, assumes positional children 0/1).
  2. Non-iceberg path: `convert_variant_columns` (`mod.rs:1987-2093`), called from `Iterator::next` at `mod.rs:1014-1030`, triggered by `cfg.slot_types[i] == types::TPrimitiveType::VARIANT`; duplicates the same by-name collapse inline.
- `unshred_variant(&VariantArray) -> Result<VariantArray>` removes all typed_value columns, merging values back into the `value` column (upstream `parquet-variant-compute-58.2.0/src/unshred_variant.rs:61`). Output `value` child is `BinaryView` — binary accessors must handle `Binary | LargeBinary | BinaryView`.
- `VariantArray::try_new(inner: &dyn Array)` requires a StructArray with a `metadata` child; `value`/`typed_value` optional. Accessors: `metadata_field() -> &ArrayRef`, `value_field() -> Option<&ArrayRef>`, `typed_value_field() -> Option<&ArrayRef>`, `into_inner() -> StructArray`.
- Spike-verified (2026-06-10): `ArrowWriter` writes a 3-child variant struct with `LogicalType::Variant`; reader schema inference (`with_skip_arrow_metadata(true)` — NovaRocks' existing config, `mod.rs:388`) yields the full shredded struct; kernel extraction over shredded vs unshredded input is row-identical.

**Commit policy:** every task ends in its own commit. Run targeted tests per step; run the full gate (`cargo fmt && cargo clippy && cargo build && cargo test`) in the final task.

---

### Task 1: New module `variant_read.rs` with shredded-aware `is_variant_struct_data_type`

**Files:**
- Create: `src/formats/parquet/variant_read.rs`
- Modify: `src/formats/parquet/mod.rs` (add `mod variant_read;`, remove the old `is_variant_struct_data_type` at lines ~1934-1947, import from the new module)

- [ ] **Step 1: Create the module with the moved+extended gate and failing-by-construction tests**

Create `src/formats/parquet/variant_read.rs` (Apache license header copied from any sibling file, e.g. `src/formats/parquet/row_group_selector.rs` lines 1-16):

```rust
// (Apache 2.0 license header — copy verbatim from row_group_selector.rs)

//! Read-side variant column handling: collapse parquet variant structs
//! (unshredded `{metadata,value}` or shredded `{metadata,value,typed_value}`)
//! into the engine-internal LargeBinary form `[size:u32 LE | metadata | value]`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeBinaryBuilder, RecordBatch, StructArray};
use arrow::datatypes::DataType;
use parquet::variant::{VariantArray, unshred_variant};

use crate::exec::variant::VariantValue;
use crate::service::starrocks_thrift::types;

use super::ParquetScanConfig;

fn is_binary_like(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

/// True when `data_type` is a parquet variant struct layout we can collapse:
/// a `metadata` binary child plus at least one of `value` (binary) /
/// `typed_value` (shredded subtree). Any other child name disqualifies.
pub(crate) fn is_variant_struct_data_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.is_empty() {
        return false;
    }
    let mut has_metadata = false;
    let mut has_value = false;
    let mut has_typed_value = false;
    for f in fields {
        match f.name().as_str() {
            "metadata" if is_binary_like(f.data_type()) => has_metadata = true,
            "value" if is_binary_like(f.data_type()) => has_value = true,
            "typed_value" => has_typed_value = true,
            _ => return false,
        }
    }
    has_metadata && (has_value || has_typed_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    #[test]
    fn variant_struct_gate_accepts_unshredded() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_shredded_three_child() {
        let typed_value = struct_of(vec![Field::new(
            "a",
            struct_of(vec![
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", DataType::Int64, true),
            ]),
            false,
        )]);
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("typed_value", typed_value, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_metadata_plus_typed_value_only() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("typed_value", DataType::Int64, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_binary_view_children() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_rejects_non_variant_shapes() {
        // missing metadata
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "value",
            DataType::Binary,
            true
        )])));
        // unknown extra field
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("extra", DataType::Int32, true),
        ])));
        // metadata only
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "metadata",
            DataType::Binary,
            false
        )])));
        // metadata wrong type
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Int32, false),
            Field::new("value", DataType::Binary, true),
        ])));
        // not a struct
        assert!(!is_variant_struct_data_type(&DataType::LargeBinary));
    }
}
```

(`unshred_variant`, `VariantArray`, `LargeBinaryBuilder`, `StructArray`, `RecordBatch`, `ParquetScanConfig`, `types`, `VariantValue`, `Arc` imports are used by Tasks 2-3; rustc will warn unused until then — acceptable within the same PR, or add `#[allow(unused_imports)]` temporarily and remove it in Task 3.)

- [ ] **Step 2: Wire the module and delete the old gate**

In `src/formats/parquet/mod.rs`:
1. Add `mod variant_read;` next to the existing `mod row_group_selector;`-style declarations (top of file).
2. Add `use variant_read::is_variant_struct_data_type;` (alongside other `use` items).
3. Delete the old `fn is_variant_struct_data_type` (lines ~1934-1947). The call site at ~1609 (`align_iceberg_array_to_field`) now resolves to the imported one.

- [ ] **Step 3: Run the new tests and the existing parquet tests**

Run: `cargo test --lib formats::parquet::variant_read -- --nocapture`
Expected: all `variant_struct_gate_*` tests PASS.

Run: `cargo test --lib formats::parquet`
Expected: PASS (no existing test relied on the 2-children-exactly restriction; if one does, it is asserting the bug — update it to expect acceptance and note it in the commit message).

- [ ] **Step 4: Commit**

```bash
git add src/formats/parquet/variant_read.rs src/formats/parquet/mod.rs
git commit -m "refactor(parquet): extract shredded-aware variant struct gate into variant_read module"
```

---

### Task 2: Shredded-aware `collapse_variant_struct_to_largebinary` (by-name, fail-fast)

**Files:**
- Modify: `src/formats/parquet/variant_read.rs` (add collapse + helpers + tests)

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module in `variant_read.rs`:

```rust
    use arrow::array::{LargeBinaryArray, StringArray};
    use parquet::variant::{ShreddedSchemaBuilder, json_to_variant, shred_variant};

    use crate::exec::variant::{parse_variant_path, variant_query, variant_to_i64};

    /// Build the canonical 5-row test column from JSON, optionally shredded
    /// on path `a` as Int64. Rows: shredded int, shredded int, missing `a`,
    /// wrong-typed `a`, SQL NULL.
    fn test_variant_struct(shredded: bool) -> ArrayRef {
        let json: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"a": 1, "b": "x"}"#),
            Some(r#"{"a": 99, "b": "y"}"#),
            Some(r#"{"b": "no-a"}"#),
            Some(r#"{"a": "not-an-int"}"#),
            None,
        ]));
        let unshredded = json_to_variant(&json).expect("json_to_variant");
        let variant = if shredded {
            let shred_type = ShreddedSchemaBuilder::new()
                .with_path("a", &DataType::Int64)
                .expect("with_path")
                .build();
            shred_variant(&unshredded, &shred_type).expect("shred_variant")
        } else {
            unshredded
        };
        Arc::new(variant.into_inner()) as ArrayRef
    }

    fn get_a_int(serialized: &[u8]) -> Option<i64> {
        let v = VariantValue::from_serialized(serialized).ok()?;
        let path = parse_variant_path("$.a").ok()?;
        let sub = variant_query(&v, &path).ok()?;
        variant_to_i64(&sub).ok()
    }

    #[test]
    fn collapse_unshredded_struct_round_trips() {
        let col = test_variant_struct(false);
        let out = collapse_variant_struct_to_largebinary(&col, "v").expect("collapse");
        let out = out
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("LargeBinary");
        assert_eq!(out.len(), 5);
        assert_eq!(get_a_int(out.value(0)), Some(1));
        assert_eq!(get_a_int(out.value(1)), Some(99));
        assert_eq!(get_a_int(out.value(2)), None); // `a` missing
        assert!(out.is_null(4)); // SQL NULL preserved
    }

    #[test]
    fn collapse_shredded_struct_matches_unshredded() {
        let shredded = collapse_variant_struct_to_largebinary(&test_variant_struct(true), "v")
            .expect("collapse shredded");
        let plain = collapse_variant_struct_to_largebinary(&test_variant_struct(false), "v")
            .expect("collapse plain");
        let shredded = shredded
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let plain = plain.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        for row in 0..5 {
            assert_eq!(shredded.is_null(row), plain.is_null(row), "row {row} nullness");
            if shredded.is_null(row) {
                continue;
            }
            // Byte-equality is NOT required (field order inside the rebuilt
            // object may differ); semantic equality on every field is.
            assert_eq!(
                get_a_int(shredded.value(row)),
                get_a_int(plain.value(row)),
                "row {row} $.a"
            );
        }
    }

    #[test]
    fn collapse_rejects_missing_bytes_instead_of_silent_null() {
        use arrow::array::BinaryArray;
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{Field, Fields};
        // metadata present, value NULL, no typed_value: previously this row
        // silently became variant-null; it must now be a hard error.
        let metadata: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![Some(&[0x01u8][..])]));
        let value: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![None]));
        let fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        let struct_arr: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![metadata, value],
            None::<NullBuffer>,
        ));
        let err = collapse_variant_struct_to_largebinary(&struct_arr, "v")
            .expect_err("must fail fast");
        assert!(err.contains("v"), "error names the column: {err}");
        assert!(
            err.contains("missing metadata/value"),
            "error says what is wrong: {err}"
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib formats::parquet::variant_read -- --nocapture`
Expected: FAIL to compile — `collapse_variant_struct_to_largebinary` not yet defined in this module.

- [ ] **Step 3: Implement the collapse**

Add to `variant_read.rs` (above the tests module):

```rust
fn binary_value_at_any(arr: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    use arrow::array::{BinaryArray, BinaryViewArray, LargeBinaryArray};
    if arr.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(a.value(row)));
    }
    Err(format!(
        "expected a binary array for variant metadata/value, got {:?}",
        arr.data_type()
    ))
}

/// Collapse a parquet variant struct column (unshredded or shredded) into
/// the engine-internal LargeBinary `[size|metadata|value]` form.
///
/// Shredded inputs (a `typed_value` child present) are first folded back to
/// plain metadata+value via the upstream `unshred_variant` kernel, so both
/// layouts produce identical engine-visible rows. Corrupt rows (missing
/// metadata or value bytes on a non-null row) are a hard error — never a
/// silent variant-null.
pub(crate) fn collapse_variant_struct_to_largebinary(
    source_array: &ArrayRef,
    column_name: &str,
) -> Result<ArrayRef, String> {
    let struct_arr = source_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| format!("expected StructArray for variant column `{column_name}`"))?;

    let has_typed_value = struct_arr
        .fields()
        .iter()
        .any(|f| f.name() == "typed_value");

    // Holds the unshredded form alive for the borrow below.
    let unshredded_holder;
    let (metadata_col, value_col): (ArrayRef, ArrayRef) = if has_typed_value {
        let variant = VariantArray::try_new(source_array.as_ref()).map_err(|e| {
            format!("variant column `{column_name}`: invalid shredded layout: {e}")
        })?;
        unshredded_holder = unshred_variant(&variant)
            .map_err(|e| format!("variant column `{column_name}`: unshred failed: {e}"))?;
        let value = unshredded_holder.value_field().ok_or_else(|| {
            format!("variant column `{column_name}`: unshred produced no value column")
        })?;
        (unshredded_holder.metadata_field().clone(), value.clone())
    } else {
        let mut metadata_idx = None;
        let mut value_idx = None;
        for (i, f) in struct_arr.fields().iter().enumerate() {
            match f.name().as_str() {
                "metadata" => metadata_idx = Some(i),
                "value" => value_idx = Some(i),
                _ => {}
            }
        }
        let metadata_idx = metadata_idx.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing metadata field")
        })?;
        let value_idx = value_idx.ok_or_else(|| {
            format!("variant column `{column_name}`: struct missing value field")
        })?;
        (
            struct_arr.column(metadata_idx).clone(),
            struct_arr.column(value_idx).clone(),
        )
    };

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..struct_arr.len() {
        if struct_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let metadata = binary_value_at_any(&metadata_col, row)?;
        let value = binary_value_at_any(&value_col, row)?;
        match (metadata, value) {
            (Some(m), Some(v)) => {
                let serialized = VariantValue::create(m, v)
                    .map_err(|e| format!("variant column `{column_name}` row {row}: {e}"))?
                    .serialize();
                builder.append_value(serialized.as_slice());
            }
            _ => {
                return Err(format!(
                    "variant column `{column_name}` row {row}: missing metadata/value bytes \
                     (corrupt file or unsupported variant encoding)"
                ));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib formats::parquet::variant_read -- --nocapture`
Expected: all 3 new `collapse_*` tests PASS (plus the Task 1 gate tests).

- [ ] **Step 5: Commit**

```bash
git add src/formats/parquet/variant_read.rs
git commit -m "feat(parquet): shredded-aware variant struct collapse via unshred_variant kernel"
```

---

### Task 3: Rewire both read paths onto the shared collapse (delete duplicated logic)

**Files:**
- Modify: `src/formats/parquet/variant_read.rs` (add `convert_variant_columns`)
- Modify: `src/formats/parquet/mod.rs` (delete old `collapse_variant_struct_to_largebinary` ~1953-1985 and old `convert_variant_columns` ~1987-2093; update the align call site and `Iterator::next`)

- [ ] **Step 1: Write the failing test for the moved `convert_variant_columns`**

The current `convert_variant_columns(cfg, batch)` only uses `cfg.slot_types` — narrow the signature to `(slot_types, batch)` so it is unit-testable without a full `ParquetScanConfig`. Append to `variant_read.rs` tests:

```rust
    use arrow::datatypes::{Field, Schema};

    fn batch_with_variant_struct(shredded: bool) -> RecordBatch {
        let col = test_variant_struct(shredded);
        let field = Field::new("v", col.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).expect("batch")
    }

    #[test]
    fn convert_variant_columns_handles_shredded_struct() {
        let batch = batch_with_variant_struct(true);
        let out = convert_variant_columns(&[types::TPrimitiveType::VARIANT], batch)
            .expect("convert");
        assert_eq!(out.column(0).data_type(), &DataType::LargeBinary);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(get_a_int(col.value(0)), Some(1));
        assert_eq!(get_a_int(col.value(1)), Some(99));
        assert!(col.is_null(4));
    }

    #[test]
    fn convert_variant_columns_passes_through_non_variant() {
        use arrow::array::Int64Array;
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        let field = Field::new("x", DataType::Int64, false);
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).expect("batch");
        let out = convert_variant_columns(&[types::TPrimitiveType::BIGINT], batch.clone())
            .expect("convert");
        assert_eq!(out.column(0).as_ref(), batch.column(0).as_ref());
    }
```

Run: `cargo test --lib formats::parquet::variant_read -- --nocapture`
Expected: FAIL to compile — `convert_variant_columns` not defined here yet.

- [ ] **Step 2: Implement `convert_variant_columns` in `variant_read.rs`**

This is the old `mod.rs:1987-2093` body with (a) the narrowed signature, (b) the inline struct collapse replaced by a delegate to `collapse_variant_struct_to_largebinary`:

```rust
/// Replace every VARIANT-typed slot whose column arrived as a parquet
/// variant struct with the engine-internal LargeBinary form. Non-variant
/// slots and already-LargeBinary variant slots pass through untouched.
pub(crate) fn convert_variant_columns(
    slot_types: &[types::TPrimitiveType],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if slot_types.is_empty() {
        return Ok(batch);
    }
    if !slot_types
        .iter()
        .any(|t| *t == types::TPrimitiveType::VARIANT)
    {
        return Ok(batch);
    }

    if batch.num_columns() != slot_types.len() {
        return Err(format!(
            "parquet scan slot_types mismatch: columns={} slot_types={}",
            batch.num_columns(),
            slot_types.len()
        ));
    }

    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(idx);
        if slot_types[idx] != types::TPrimitiveType::VARIANT {
            new_fields.push(field.clone());
            new_columns.push(col.clone());
            continue;
        }

        match col.data_type() {
            DataType::LargeBinary => {
                new_fields.push(field.clone());
                new_columns.push(col.clone());
            }
            DataType::Struct(_) => {
                if !is_variant_struct_data_type(col.data_type()) {
                    return Err(format!(
                        "VARIANT column `{}` has unsupported struct layout: {:?}",
                        field.name(),
                        col.data_type()
                    ));
                }
                let collapsed = collapse_variant_struct_to_largebinary(col, field.name())?;
                let meta = field.metadata().clone();
                let new_field = Arc::new(
                    arrow::datatypes::Field::new(
                        field.name(),
                        DataType::LargeBinary,
                        field.is_nullable(),
                    )
                    .with_metadata(meta),
                );
                new_fields.push(new_field);
                new_columns.push(collapsed);
            }
            other => {
                return Err(format!("VARIANT column has unsupported type: {:?}", other));
            }
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e: arrow::error::ArrowError| e.to_string())
}
```

- [ ] **Step 3: Update `mod.rs` — delete duplicates, fix call sites**

In `src/formats/parquet/mod.rs`:
1. Delete the old `fn collapse_variant_struct_to_largebinary` (~1953-1985) and the old `fn convert_variant_columns` (~1987-2093).
2. Extend the import: `use variant_read::{collapse_variant_struct_to_largebinary, convert_variant_columns, is_variant_struct_data_type};`
3. In `align_iceberg_array_to_field` (~1608-1612), the call changes from `collapse_variant_struct_to_largebinary(&source_array, target_field)` to:

```rust
        if matches!(target_field.data_type(), DataType::LargeBinary)
            && is_variant_struct_data_type(source_field.data_type())
        {
            return collapse_variant_struct_to_largebinary(&source_array, target_field.name());
        }
```

4. In `Iterator::next` (~1015), the call changes from `convert_variant_columns(&self.cfg, b)` to:

```rust
            .and_then(|b| convert_variant_columns(&self.cfg.slot_types, b))
```

5. If `binary_value_at` in `mod.rs` is now unused (its only users were the two deleted functions), delete it too; if other call sites remain, leave it.

- [ ] **Step 4: Run the module tests**

Run: `cargo test --lib formats::parquet -- --nocapture`
Expected: PASS — new variant_read tests plus all existing parquet tests.

- [ ] **Step 5: Commit**

```bash
git add src/formats/parquet/variant_read.rs src/formats/parquet/mod.rs
git commit -m "feat(parquet): route both variant read paths through shared shredded-aware collapse"
```

---

### Task 4: File-level round-trip test (write shredded parquet → read → engine form)

**Files:**
- Modify: `src/formats/parquet/variant_read.rs` (one integration-style test using a real file)

- [ ] **Step 1: Write the test**

Append to the tests module. This mirrors the design spike: write a shredded file with `ArrowWriter`, read it back with the same reader options production uses (`with_skip_arrow_metadata(true)`), and push the batch through `convert_variant_columns`.

```rust
    #[test]
    fn shredded_parquet_file_round_trips_to_engine_form() {
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
        use std::collections::HashMap;
        use std::fs::File;

        let inner = test_variant_struct(true);
        let mut md = HashMap::new();
        md.insert(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        );
        md.insert("ARROW:extension:metadata".to_string(), String::new());
        md.insert("PARQUET:field_id".to_string(), "2".to_string());
        let field = Field::new("v", inner.data_type().clone(), true).with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![inner]).expect("batch");

        let dir = std::env::temp_dir().join(format!(
            "nova_variant_read_test_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("t").len()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("shredded.parquet");
        {
            let f = File::create(&path).expect("create");
            let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
            w.write(&batch).expect("write");
            w.close().expect("close");
        }

        let opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(File::open(&path).expect("open"), opts)
                .expect("builder");
        let mut reader = builder.build().expect("build");
        let read_batch = reader.next().expect("one batch").expect("batch ok");

        // The file's variant column comes back as the shredded struct; the
        // engine conversion must reconstruct full variant rows from it.
        let out = convert_variant_columns(&[types::TPrimitiveType::VARIANT], read_batch)
            .expect("convert from file");
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(col.len(), 5);
        assert_eq!(get_a_int(col.value(0)), Some(1));
        assert_eq!(get_a_int(col.value(1)), Some(99));
        assert_eq!(get_a_int(col.value(2)), None);
        assert!(col.is_null(4));

        let _ = std::fs::remove_dir_all(&dir);
    }
```

- [ ] **Step 2: Run it**

Run: `cargo test --lib formats::parquet::variant_read::tests::shredded_parquet_file_round_trips_to_engine_form -- --nocapture`
Expected: PASS. If the reader returns the struct with `BinaryView` children (schema-inference variation across parquet-rs minor versions), `binary_value_at_any` already covers it; the test must still pass unchanged.

- [ ] **Step 3: Commit**

```bash
git add src/formats/parquet/variant_read.rs
git commit -m "test(parquet): shredded variant parquet file round-trips to engine LargeBinary form"
```

---

### Task 5: Docs, quality gate, final commit

**Files:**
- Modify: `docs/iceberg-v3/variant.md` (shredding section, ~line 76)

- [ ] **Step 1: Update the support matrix doc**

In `docs/iceberg-v3/variant.md`, replace the `## ❌ Variant shredding（typed_value）` section body:

```markdown
## 🟡 Variant shredding（typed_value）

Spec optional 能力：在 parquet 物理结构里把"已知 schema 部分"提到 `typed_value` 子树以加速点查 / pushdown。

- **读：✅ 已支持。** 读端检测 `typed_value` 子树后经上游 `unshred_variant` 内核重建完整
  variant（`src/formats/parquet/variant_read.rs`），shredded 与非 shredded 文件给出逐行一致的
  查询结果。损坏行（非空行缺 metadata/value 字节）显式报错，不再静默置 null。
- **写：❌ 未支持**（IV3-6 PR-6 计划内，显式表属性 `write.parquet.variant-shredding.<col>`）。
```

Also fix the now-stale claim at ~line 78 ("读端遇到 cross-engine 写出的 shredded variant 退化按 metadata + value 解码") — it is replaced by the text above.

- [ ] **Step 2: Full quality gate**

Run, in order:
```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo build
cargo test --lib formats::parquet
cargo test
```
Expected: all green. (`cargo test` is the full suite — budget ~10+ minutes.)

- [ ] **Step 3: Run the variant SQL regression to prove no behavior change for unshredded data**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
# wait for the NOVAROCKS_READY line per CLAUDE.md §7.3 before continuing
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --mode verify
```
Expected: suite PASS, including `variant_insert` and `variant_unsupported` unchanged.

- [ ] **Step 4: Commit**

```bash
git add docs/iceberg-v3/variant.md
git commit -m "docs: variant shredded read is now supported (IV3-6 PR-1)"
```

---

## Self-review checklist (completed at plan-write time)

- Spec coverage: PR-1 scope of the design doc (§3 reader 正确性地基, §8 fail-fast 行, §9 单元/差分+file round-trip) — Tasks 1-5 cover gate extension, kernel unshred, fail-fast on corrupt rows, both call paths, file round-trip, doc update. The design's "transition fail-fast commit" is subsumed: this PR lands real support directly.
- No placeholders: every step has complete code or an exact command + expected outcome.
- Type consistency: `collapse_variant_struct_to_largebinary(&ArrayRef, &str)` used identically in Tasks 2/3; `convert_variant_columns(&[TPrimitiveType], RecordBatch)` in Tasks 3/4.
- Known risk to verify at execution time: exact arrow import paths (`arrow::buffer::NullBuffer`, `BinaryArray::from_opt_vec`) may need minor adjustment to the crate's 58.2 surface — the compiler will say so; semantics are pinned by the tests.
