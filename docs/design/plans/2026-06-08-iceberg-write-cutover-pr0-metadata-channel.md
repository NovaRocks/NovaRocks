# Iceberg Write Cutover PR-0: Lossless Writer→Commit Metadata Channel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the distributed writer→coordinator→commit metadata channel lossless so that a `WrittenFile` reconstructed from a reported `TSinkCommitInfo` is field-for-field identical to the one the in-process inject path produces — the parity gate that unblocks every later flow cutover.

**Architecture:** The committed `IcebergCommitCollector` has two feed paths: the full-fidelity `inject_written_file` path (today's SQL writes) and the lossy `TSinkCommitInfo → convert()` path (the distributed coordinator path the cutover must use). This PR closes the gap by (1) extending the `TIcebergDataFile` thrift with `first_row_id`/`equality_ids`/`key_metadata`, (2) populating them at the sink-side serializer, (3) wiring `convert()` to read `column_stats` and the new fields (and stop hard-erroring on equality-delete content), and (4) adding a parity test that round-trips a real `DataFile` through both paths. No user-facing write routing changes in this PR.

**Tech Stack:** Rust, NovaRocks standalone engine, vendored `iceberg` 0.9.0 (`vendor/iceberg-0.9.0`), Apache Thrift IDL (`idl/thrift/Types.thrift`, regenerated at build time), Cargo unit tests.

---

## Background facts (verified against the worktree)

- Thrift `TIcebergDataFile` (`idl/thrift/Types.thrift:591`) has tags 1–10 and **no** `first_row_id`/`equality_ids`/`key_metadata`. `TIcebergColumnStats` (`idl/thrift/Types.thrift`) already carries `column_sizes`/`value_counts`/`null_value_counts`/`nan_value_counts`/`lower_bounds`/`upper_bounds`.
- The forward serializer `to_sink_commit_info` (`src/connector/iceberg/data_writer.rs:350`) builds the `TIcebergDataFile` literal inline and already calls `iceberg_data_file_to_column_stats(df)` — so `column_stats` is already on the wire. Bounds are serialized via `Datum::to_bytes()` (`data_writer.rs:438`).
- `collector.rs::convert()` (`src/connector/iceberg/commit/collector.rs:242`) currently: sets all stats maps to `Default::default()` (ignores `df.column_stats`), sets `key_metadata`/`equality_ids`/`first_row_id` to `None`, and **returns `Err` for `EQUALITY_DELETES` content** (`collector.rs:254`).
- Ground-truth inject converter: `crate::engine::iceberg_writer::data_file_to_written_file(df, spec_id)` (`src/engine/iceberg_writer.rs:300`) carries the full `WrittenFile` field set.
- Inverse of `Datum::to_bytes()` is `Datum::try_from_bytes(bytes: &[u8], data_type: PrimitiveType)` (`vendor/iceberg-0.9.0/src/spec/values/datum.rs:369`).
- iceberg `DataFile` accessors: `equality_ids() -> Option<Vec<i32>>`, `first_row_id() -> Option<i64>`, `key_metadata() -> Option<&[u8]>` (`vendor/iceberg-0.9.0/src/spec/manifest/data_file.rs:246-262`).
- `WrittenFile` (`src/connector/iceberg/commit/types.rs:99`) derives only `Clone, Debug` — it needs `PartialEq` for the parity assertion. Field types support it: `Struct`, `DataContentType`, `DataFileFormat` all derive `PartialEq`; `Datum` derives `PartialEq`. `WrittenFile` has **no** `nan_value_counts` field, so dropping NaN counts is consistent on both paths and out of scope.
- `parse_partition_path` (`collector.rs:309`) decodes **identity** transforms only and returns an explicit `Err` for any other transform. Transform-partition round-trip is therefore a known boundary handled in a later cutover PR; this PR covers identity/unpartitioned and asserts the explicit error for transforms.
- Sketches (Puffin/NDV) are **not** part of `WrittenFile`. They already ride an out-of-band channel: `to_sink_commit_info` returns `Option<FileSketchSet>` separately, and `IcebergCommitCollector::take_sketch_sets` (`collector.rs:161`) drains both injected sketches and `crate::runtime::sink_commit::take_sketch_sets(finst_id)` (in-process). Cross-node sketch transport is out of scope for PR-0 (no routing change; single-node standalone uses the in-process channel) and is flagged in Task 4.

## File Structure

- `idl/thrift/Types.thrift` — add 3 optional fields to `TIcebergDataFile`. Regenerated into `target/.../thrift_rs/types.rs` at build time; the generated struct keeps `Default`, so the `..Default::default()` test sites stay green.
- `src/connector/iceberg/data_writer.rs` — extract a single, unit-testable forward serializer `data_file_to_iceberg_thrift(df, …) -> TIcebergDataFile` (currently inline in `to_sink_commit_info`) and populate the 3 new fields there.
- `src/connector/iceberg/sink.rs` — the position-delete `TIcebergDataFile` literal (`sink.rs:494`) must set the 3 new fields (`None`) to keep compiling.
- `src/connector/iceberg/commit/types.rs` — add `PartialEq` to `WrittenFile`.
- `src/connector/iceberg/commit/collector.rs` — rewrite `convert()` to read `column_stats` + the 3 new fields, map `EQUALITY_DELETES` to `DataContentType::EqualityDeletes`; add `i64_map_to_u64` and `decode_bounds` helpers; add the parity tests; document the sketch and transform-partition boundaries.

Only two production `TIcebergDataFile` literals use explicit field lists (`data_writer.rs` and `sink.rs:494`); all other sites use `..Default::default()` and need no change.

---

## Task 1: Extend the thrift schema and add a testable forward serializer

**Files:**
- Modify: `idl/thrift/Types.thrift:591-602`
- Modify: `src/connector/iceberg/data_writer.rs:350-392` (extract `data_file_to_iceberg_thrift`)
- Modify: `src/connector/iceberg/sink.rs:494-505` (set new fields to `None`)
- Test: `src/connector/iceberg/data_writer.rs` (tests module)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `src/connector/iceberg/data_writer.rs`:

```rust
#[test]
fn data_file_to_iceberg_thrift_carries_lineage_equality_and_key_metadata() {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    let mut b = DataFileBuilder::default();
    b.content(DataContentType::Data)
        .file_path("file:///t/data-1.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(10)
        .file_size_in_bytes(100);
    b.key_metadata(Some(vec![1u8, 2, 3]));
    b.first_row_id(Some(42));
    let df = b.build().expect("data file");

    let thrift = data_file_to_iceberg_thrift(
        &df,
        String::new(),
        String::new(),
        "PARQUET".to_string(),
        crate::types::TIcebergFileContent::DATA,
    )
    .expect("thrift");

    assert_eq!(thrift.first_row_id, Some(42));
    assert_eq!(thrift.key_metadata, Some(vec![1u8, 2, 3]));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -q data_file_to_iceberg_thrift_carries`
Expected: FAIL — compile error (`data_file_to_iceberg_thrift` not found, and `thrift.first_row_id` / `thrift.key_metadata` fields do not exist on the generated struct).

- [ ] **Step 3: Add the optional fields to the thrift struct**

In `idl/thrift/Types.thrift`, change `TIcebergDataFile` (currently ending at tag 10) to:

```thrift
struct TIcebergDataFile {
    1: optional string path
    2: optional string format
    3: optional i64 record_count
    4: optional i64 file_size_in_bytes
    5: optional string partition_path;
    6: optional list<i64> split_offsets;
    7: optional TIcebergColumnStats column_stats;
    8: optional string partition_null_fingerprint;
    9: optional TIcebergFileContent file_content;
    10: optional string referenced_data_file;
    // NovaRocks-internal writer->coordinator carriage for Iceberg commit
    // fidelity. Optional => backward-compatible for the FE-compatible path.
    11: optional i64 first_row_id;
    12: optional list<i32> equality_ids;
    13: optional binary key_metadata;
}
```

- [ ] **Step 4: Extract `data_file_to_iceberg_thrift` and populate the new fields**

In `src/connector/iceberg/data_writer.rs`, replace the inline `TIcebergDataFile { … }` construction inside `to_sink_commit_info` (lines 364-375) with a call to a new extracted function. The body of `to_sink_commit_info` becomes:

```rust
    let df = &staged.data_file;
    let data_file = data_file_to_iceberg_thrift(df, partition_path, null_fingerprint, format, content)?;
    let commit_info = crate::types::TSinkCommitInfo {
        iceberg_data_file: Some(data_file),
        hive_file_info: None,
        is_overwrite: None,
        staging_dir: None,
        is_rewrite: None,
    };
```

Add the new function immediately above `iceberg_data_file_to_column_stats`:

```rust
/// Serialize an iceberg-rust `DataFile` into the thrift `TIcebergDataFile`
/// carried over the writer report channel. This is the single forward
/// serialization site; keep it lossless against
/// `engine::iceberg_writer::data_file_to_written_file` (the inject path) so
/// the distributed coordinator path reconstructs the same `WrittenFile`.
pub(crate) fn data_file_to_iceberg_thrift(
    df: &DataFile,
    partition_path: String,
    null_fingerprint: String,
    format: String,
    content: crate::types::TIcebergFileContent,
) -> Result<crate::types::TIcebergDataFile, String> {
    Ok(crate::types::TIcebergDataFile {
        path: Some(df.file_path().to_string()),
        format: Some(format),
        record_count: Some(u64_to_i64(df.record_count(), "record_count")?),
        file_size_in_bytes: Some(u64_to_i64(df.file_size_in_bytes(), "file_size_in_bytes")?),
        partition_path: Some(partition_path),
        split_offsets: df.split_offsets().map(|offsets| offsets.to_vec()),
        column_stats: iceberg_data_file_to_column_stats(df)?,
        partition_null_fingerprint: Some(null_fingerprint),
        file_content: Some(content),
        referenced_data_file: df.referenced_data_file(),
        first_row_id: df.first_row_id(),
        equality_ids: df.equality_ids(),
        key_metadata: df.key_metadata().map(|k| k.to_vec()),
    })
}
```

- [ ] **Step 5: Set the new fields at the position-delete sink site**

In `src/connector/iceberg/sink.rs`, the `TIcebergDataFile { … }` literal at line 494 (position-delete files) must set the new fields so it compiles. Add these three lines before the closing `}` of that struct literal (after `referenced_data_file,`):

```rust
                first_row_id: None,
                equality_ids: None,
                key_metadata: None,
```

(Position-delete files carry none of these; data-file lineage/equality come through `data_file_to_iceberg_thrift`.)

- [ ] **Step 6: Build and run the test to verify it passes**

Run: `cargo build && cargo test -q data_file_to_iceberg_thrift_carries`
Expected: build succeeds (thrift regenerates with the new fields); test PASSES.

- [ ] **Step 7: Commit**

```bash
git add idl/thrift/Types.thrift src/connector/iceberg/data_writer.rs src/connector/iceberg/sink.rs
git commit -m "iceberg: carry first_row_id/equality_ids/key_metadata in TIcebergDataFile"
```

---

## Task 2: Wire `convert()` to reconstruct full `WrittenFile` fidelity

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs:98` (derive `PartialEq`)
- Modify: `src/connector/iceberg/commit/collector.rs:29-40` (imports), `:242-292` (`convert`), add helpers
- Test: `src/connector/iceberg/commit/collector.rs` (tests module)

- [ ] **Step 1: Write the failing parity test**

Add a `#[cfg(test)] mod tests` block at the end of `src/connector/iceberg/commit/collector.rs` (or extend the existing one if present):

```rust
#[cfg(test)]
mod parity_tests {
    use super::*;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, NestedField, PrimitiveType,
        Schema, Struct, Type,
    };
    use std::collections::HashMap;

    fn int_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "k1",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema"),
        )
    }

    fn unpartitioned_collector(schema: SchemaRef) -> IcebergCommitCollector {
        IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            Arc::new(iceberg::spec::PartitionSpec::unpartition_spec()),
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        )
    }

    #[test]
    fn convert_reproduces_inject_path_for_data_file_stats() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1000)
            .file_size_in_bytes(2048);
        b.column_sizes(HashMap::from([(1, 4000u64)]));
        b.value_counts(HashMap::from([(1, 1000u64)]));
        b.null_value_counts(HashMap::from([(1, 0u64)]));
        b.lower_bounds(HashMap::from([(1, Datum::int(1))]));
        b.upper_bounds(HashMap::from([(1, Datum::int(1000))]));
        let df = b.build().expect("data file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_roundtrips_equality_delete_files() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::EqualityDeletes)
            .file_path("file:///t/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(3)
            .file_size_in_bytes(64)
            .equality_ids(Some(vec![1]));
        let df = b.build().expect("eq delete file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::EQUALITY_DELETES,
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected, actual);
        assert_eq!(actual.equality_ids, Some(vec![1]));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -q convert_reproduces_inject_path_for_data_file_stats convert_roundtrips_equality_delete_files`
Expected: FAIL — `WrittenFile` does not implement `PartialEq` (compile error), and once that is added, `convert` would still drop stats and return `Err` for `EQUALITY_DELETES`.

- [ ] **Step 3: Derive `PartialEq` on `WrittenFile`**

In `src/connector/iceberg/commit/types.rs`, change the derive on `WrittenFile` (line 98) from:

```rust
#[derive(Clone, Debug)]
pub struct WrittenFile {
```

to:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct WrittenFile {
```

- [ ] **Step 4: Add imports and rewrite `convert()` with helpers**

In `src/connector/iceberg/commit/collector.rs`, extend the iceberg import (line 33) to include `Datum`, and add a `std::collections` import near the top imports:

```rust
use iceberg::spec::{
    Datum, Literal, PartitionSpecRef, PrimitiveType, SchemaRef, Struct, Transform, Type,
};
use std::collections::{BTreeMap, HashMap};
```

Replace the body of `convert` (lines 242-292) with:

```rust
    fn convert(&self, df: crate::types::TIcebergDataFile) -> Result<WrittenFile, String> {
        use iceberg::spec::{DataContentType, DataFileFormat};

        let path = df
            .path
            .ok_or_else(|| "TIcebergDataFile missing path".to_string())?;
        let content = match df
            .file_content
            .unwrap_or(crate::types::TIcebergFileContent::DATA)
        {
            crate::types::TIcebergFileContent::DATA => DataContentType::Data,
            crate::types::TIcebergFileContent::POSITION_DELETES => DataContentType::PositionDeletes,
            crate::types::TIcebergFileContent::EQUALITY_DELETES => DataContentType::EqualityDeletes,
            other => {
                return Err(format!(
                    "unexpected TIcebergFileContent variant {other:?} in sink_commit_info"
                ));
            }
        };

        let partition_values = parse_partition_path(
            df.partition_path.as_deref().unwrap_or(""),
            &self.partition_spec,
            &self.schema,
        )?;

        let stats = df.column_stats.unwrap_or_default();
        let column_sizes = i64_map_to_u64(stats.column_sizes, "column_sizes")?;
        let value_counts = i64_map_to_u64(stats.value_counts, "value_counts")?;
        let null_value_counts = i64_map_to_u64(stats.null_value_counts, "null_value_counts")?;
        let lower_bounds = self.decode_bounds(stats.lower_bounds, "lower_bounds")?;
        let upper_bounds = self.decode_bounds(stats.upper_bounds, "upper_bounds")?;

        Ok(WrittenFile {
            path,
            format: DataFileFormat::Parquet,
            content,
            partition_values,
            partition_spec_id: self.partition_spec.spec_id(),
            record_count: df.record_count.unwrap_or(0).max(0) as u64,
            file_size_in_bytes: df.file_size_in_bytes.unwrap_or(0).max(0) as u64,
            split_offsets: df.split_offsets.unwrap_or_default(),
            column_sizes,
            value_counts,
            null_value_counts,
            lower_bounds,
            upper_bounds,
            key_metadata: df.key_metadata,
            referenced_data_file: df.referenced_data_file,
            equality_ids: df.equality_ids,
            first_row_id: df.first_row_id,
        })
    }

    /// Decode per-column bound bytes (Iceberg single-value binary encoding)
    /// back into `Datum`s, using the table schema to resolve each field id's
    /// primitive type. Inverse of `data_writer::datum_bounds_to_bytes`.
    fn decode_bounds(
        &self,
        bounds: Option<BTreeMap<i32, Vec<u8>>>,
        field: &str,
    ) -> Result<HashMap<i32, Datum>, String> {
        let mut out = HashMap::new();
        for (field_id, bytes) in bounds.unwrap_or_default() {
            let schema_field = self.schema.field_by_id(field_id).ok_or_else(|| {
                format!("column stat {field} references field id {field_id} not present in schema")
            })?;
            let prim = match &schema_field.field_type {
                Type::Primitive(p) => p.clone(),
                other => {
                    return Err(format!(
                        "column stat {field} field id {field_id} has non-primitive type {other:?}"
                    ));
                }
            };
            let datum = Datum::try_from_bytes(&bytes, prim)
                .map_err(|e| format!("decode column stat {field}[{field_id}] failed: {e}"))?;
            out.insert(field_id, datum);
        }
        Ok(out)
    }
```

Add this free function near `parse_partition_path` (outside the `impl` block):

```rust
/// Convert a thrift `map<i32, i64>` column-stat map into the `WrittenFile`
/// `HashMap<i32, u64>` representation. Inverse of `data_writer::u64_stats_to_i64`.
fn i64_map_to_u64(
    map: Option<BTreeMap<i32, i64>>,
    field: &str,
) -> Result<HashMap<i32, u64>, String> {
    map.unwrap_or_default()
        .into_iter()
        .map(|(field_id, value)| {
            u64::try_from(value)
                .map(|value| (field_id, value))
                .map_err(|_| format!("iceberg column stat {field}[{field_id}] value {value} is negative"))
        })
        .collect()
}
```

- [ ] **Step 5: Build and run the tests to verify they pass**

Run: `cargo build && cargo test -q convert_reproduces_inject_path_for_data_file_stats convert_roundtrips_equality_delete_files`
Expected: both PASS. (If `convert_reproduces_inject_path_for_data_file_stats` fails on `partition_spec_id`, confirm the collector was built with `unpartition_spec()` whose `spec_id()` is 0 and `data_file_to_written_file(&df, 0)` was passed 0.)

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/collector.rs
git commit -m "iceberg: reconstruct full WrittenFile fidelity in collector convert()"
```

---

## Task 3: Partition parity coverage and the transform boundary

**Files:**
- Test: `src/connector/iceberg/commit/collector.rs` (`parity_tests` module)

- [ ] **Step 1: Write the failing identity-partition parity test**

Add to the `parity_tests` module in `src/connector/iceberg/commit/collector.rs`:

```rust
    fn identity_partition_spec(schema: &SchemaRef) -> PartitionSpecRef {
        Arc::new(
            iceberg::spec::PartitionSpec::builder((**schema).clone())
                .with_spec_id(0)
                .add_partition_field("k1", "k1", Transform::Identity)
                .expect("add partition field")
                .build()
                .expect("partition spec"),
        )
    }

    #[test]
    fn convert_reproduces_identity_partition_values() {
        use iceberg::spec::Literal;

        let schema = int_schema();
        let spec = identity_partition_spec(&schema);

        let partition = Struct::from_iter([Some(Literal::int(5))]);
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/k1=5/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(partition.clone())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(64);
        let df = b.build().expect("data file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            "k1=5".to_string(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
        )
        .expect("thrift");

        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            spec,
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        );
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected.partition_values, actual.partition_values);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_rejects_unsupported_transform_partition_paths() {
        let schema = int_schema();
        let spec = Arc::new(
            iceberg::spec::PartitionSpec::builder((*schema).clone())
                .with_spec_id(0)
                .add_partition_field("k1", "k1_bucket", Transform::Bucket(4))
                .expect("add partition field")
                .build()
                .expect("partition spec"),
        );

        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/k1_bucket=2/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(iceberg::spec::Literal::int(2))]))
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(64);
        let df = b.build().expect("data file");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            "k1_bucket=2".to_string(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
        )
        .expect("thrift");

        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            spec,
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        );
        let err = collector.convert(thrift).expect_err("transform must be rejected");
        assert!(
            err.contains("transform"),
            "expected an explicit transform-not-supported error, got: {err}"
        );
    }
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -q convert_reproduces_identity_partition_values convert_rejects_unsupported_transform_partition_paths`
Expected:
- `convert_reproduces_identity_partition_values` PASSES (identity partition path round-trips through `parse_partition_path`).
- `convert_rejects_unsupported_transform_partition_paths` PASSES (documents the known transform boundary: convert returns the explicit "transform … not yet supported" error). If the `PartitionSpec::builder` API differs in this iceberg vendor, adjust the builder calls to match `vendor/iceberg-0.9.0/src/spec/partition.rs` while preserving the test intent.

- [ ] **Step 3: Commit**

```bash
git add src/connector/iceberg/commit/collector.rs
git commit -m "iceberg: parity-test identity partitions, assert transform boundary"
```

---

## Task 4: Document scope boundaries and run full verification

**Files:**
- Modify: `src/connector/iceberg/commit/collector.rs` (module doc / `convert` doc comment)

- [ ] **Step 1: Document the sketch and transform boundaries at the code**

In `src/connector/iceberg/commit/collector.rs`, add a doc comment directly above `fn convert` describing what the lossless channel does and does not carry, so a later cutover does not silently assume full coverage:

```rust
    /// Reconstruct a [`WrittenFile`] from a writer-reported `TIcebergDataFile`.
    ///
    /// As of PR-0 this is lossless against the inject path
    /// (`engine::iceberg_writer::data_file_to_written_file`) for data and
    /// delete files: column statistics (`column_stats`), `first_row_id`,
    /// `equality_ids`, and `key_metadata` all round-trip. Two boundaries
    /// remain and are handled by later cutover PRs:
    ///
    /// - Partition values are decoded from `partition_path` and currently
    ///   support identity transforms only; non-identity transforms return an
    ///   explicit error (see `parse_partition_path`).
    /// - Puffin/NDV sketches are not part of `WrittenFile`; they ride the
    ///   out-of-band sketch channel (`take_sketch_sets` /
    ///   `runtime::sink_commit::take_sketch_sets`), which is in-process today.
    ///   Cross-node sketch transport is required only when multi-BE append is
    ///   cut over and is out of scope for PR-0.
```

- [ ] **Step 2: Format and lint**

Run: `cargo fmt && cargo clippy --all-targets 2>&1 | tail -20`
Expected: no formatting diff; clippy reports no new warnings in `data_writer.rs`, `sink.rs`, or `collector.rs`.

- [ ] **Step 3: Run the focused parity and regression suites**

Run:
```bash
cargo test -q data_file_to_iceberg_thrift_carries
cargo test -q convert_reproduces_inject_path_for_data_file_stats
cargo test -q convert_roundtrips_equality_delete_files
cargo test -q convert_reproduces_identity_partition_values
cargo test -q convert_rejects_unsupported_transform_partition_paths
cargo test -q write_commit
cargo test -q writer_abort
cargo test -q staged_artifacts
cargo test -q write_coordinator
cargo test -q report_exec_status
git diff --check
```
Expected: all parity tests pass; the existing IW-4/IW-6 regression suites stay green (the new thrift fields are additive and default to `None`); `git diff --check` reports no whitespace errors.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/commit/collector.rs
git commit -m "iceberg: document PR-0 metadata channel scope boundaries"
```

---

## Self-Review

**Spec coverage (against `docs/design/specs/2026-06-08-iceberg-write-lifecycle-cutover-design.md`, Phase 0 + PR-0):**
- "extend `TIcebergDataFile` thrift (`first_row_id`/`equality_ids`/`key_metadata`)" → Task 1.
- "sink 侧填充新字段" → Task 1 (`data_file_to_iceberg_thrift`) + Task 1 Step 5 (position-delete site).
- "`convert()` 接通 `column_stats` + 新字段 + 移除 equality-delete 硬报错" → Task 2.
- "parity 测试做 gate" → Task 2 (data stats + equality delete) + Task 3 (identity partition + transform boundary).
- "sketch out-of-band" → documented in Task 4 as the existing in-process channel; cross-node deferred and flagged (consistent with PR-0 "no routing change").
- "顺带修 `convert()` 列统计 bug" → Task 2 (column_stats now read).
- "No user-facing write routing change in PR-0" → no engine/coordinator routing files touched.

**Placeholder scan:** No TBD/TODO; every code step shows the exact code; every test step shows the exact assertion and command.

**Type consistency:** `data_file_to_iceberg_thrift` defined in Task 1 and called by the same signature in Tasks 2 and 3. `convert` is a private method used by tests in the same module (`collector.rs`). `data_file_to_written_file(df, spec_id)` matches its real signature (`iceberg_writer.rs:300`). `Datum::try_from_bytes(&[u8], PrimitiveType)` and the `DataFile` accessor return types match the vendored iceberg API. `WrittenFile` gains `PartialEq` before any `assert_eq!` on it.

**Known boundary (not a gap):** transform-partition round-trip and cross-node sketch transport are intentionally out of PR-0 and asserted/documented rather than silently skipped — they are gated work for the partitioned-table and multi-BE append cutovers respectively.
