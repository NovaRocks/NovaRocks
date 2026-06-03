# IV3-8: `$files` / `$manifests` / `$entries` Iceberg Metadata Tables — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the three remaining Iceberg metadata tables — `t$files`, `t$manifests`, `t$entries` — so governance/debug tooling can query a table's physical layout (data + delete files with column stats and DVs; manifest inventory; full manifest-entry log).

**Architecture:** One **resolution-time** async manifest walk (server thread, where `block_on` is safe — the sync pipeline scan op cannot do I/O) reads the current snapshot's manifest list → manifests → entries directly off iceberg-rust `DataFile`/`ManifestFile`/`ManifestEntry`, projects per-table rows, and serializes them to JSON. The JSON rides on a new `IcebergTableInfo.serialized_metadata_rows` field → planner forwards it as the scan op's `metadata_payload`/`serialized_predicate` (the exact path `$partitions` already uses) → the sync `IcebergMetadataScanOp` deserializes and builds Arrow columns. This reuses the shipped 4-table builder pattern (`build_snapshot_array`/`build_partition_array`) and adds Map/List/List-of-Struct column builders.

**Tech Stack:** Rust, vendored `iceberg-rust 0.9.0` (`Snapshot::load_manifest_list`, `ManifestFile::load_manifest`, `DataFile`/`ManifestEntry` accessors), Arrow (`MapBuilder`, `ListBuilder`, `StructBuilder`), the standalone MySQL encoder (Map/Struct/List already supported), SQL regression + iceberg-compatibility suites.

**Scope (per approved spec D1):** Iceberg-spec columns for `$files`/`$manifests`; a **flat** `$entries` (entry-level columns + flattened file columns, no nested `data_file` struct, no `readable_metrics`). `lower_bounds`/`upper_bounds` are `Map<Int32, Binary>` (D6). `$files`/`$entries` `partition` is surfaced as the stable `Utf8` string form (the `iceberg_partition_key` representation already used elsewhere); promoting it to a typed dynamic struct is a follow-up. Prerequisite: none (independent of Plan 1 / 1b), but shares `metadata.rs` and `iceberg_metadata.rs` with them — no edit conflicts.

**Spec ref:** `docs/superpowers/specs/2026-06-03-iv3-2-iv3-8-iceberg-metadata-design.md` Part B.

---

## Column schemas (target)

**`$files`** — one row per data file (`content=0`) and per delete file (`content=1` position / `2` equality):
```
content INT, file_path STRING, file_format STRING, spec_id INT, record_count BIGINT,
file_size_in_bytes BIGINT, column_sizes MAP<INT,BIGINT>, value_counts MAP<INT,BIGINT>,
null_value_counts MAP<INT,BIGINT>, nan_value_counts MAP<INT,BIGINT>,
lower_bounds MAP<INT,BINARY>, upper_bounds MAP<INT,BINARY>, split_offsets ARRAY<BIGINT>,
equality_ids ARRAY<INT>, sort_order_id INT(null), key_metadata BINARY(null),
first_row_id BIGINT(null), partition STRING
```
**`$manifests`**:
```
content INT, path STRING, length BIGINT, partition_spec_id INT, added_snapshot_id BIGINT,
added_data_files_count INT, existing_data_files_count INT, deleted_data_files_count INT,
added_rows_count BIGINT, existing_rows_count BIGINT, deleted_rows_count BIGINT,
partition_summaries ARRAY<STRUCT<contains_null BOOL, contains_nan BOOL, lower_bound STRING, upper_bound STRING>>
```
**`$entries`** — entry columns + the full `$files` file columns:
```
status INT, snapshot_id BIGINT, sequence_number BIGINT, file_sequence_number BIGINT(null),
first_row_id BIGINT(null), + all $files columns (content … partition)
```

---

## File Structure

| File | Change |
|---|---|
| `src/sql/parser/dialect/mod.rs` | Whitelist `files`/`manifests`/`entries` suffixes (line ~1355) |
| `src/connector/iceberg/metadata.rs` | `parse()` accept `"ENTRIES"`; **add** 3 row structs + `load_*_rows` + `build_*_array` + Map/List/List-Struct builders; un-reject in `new()` + `execute_iter` |
| `src/sql/analyzer/iceberg_metadata.rs` | Real schemas for `Files`/`Manifests`/`LogicalIcebergMetadata` (replace `Vec::new()` at 87–93) |
| `src/sql/catalog.rs` | **Add** `IcebergTableInfo.serialized_metadata_rows: Option<String>` |
| `src/connector/iceberg/metadata_read.rs` (new) | Async manifest walk → per-table JSON rows |
| `src/connector/backend.rs` + `src/connector/iceberg/catalog/backend.rs` | `TableSource::build_metadata_rows_table_def(resolved, ty)` |
| `src/engine/catalog_mgr/provider.rs` | Route `Files`/`Manifests`/`Entries` lookups to the new builder (line ~63) |
| `src/sql/planner/mod.rs` | `build_iceberg_metadata_payload` branches for the 3 types |
| `sql-tests/iceberg/{sql,result}/iceberg_metadata_{files,manifests,entries}.*` | New goldens |
| `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_metadata_tables.sql` | Extend with cross-engine `$files`/`$manifests`/`$entries` checks |

---

## Task 1: Parser whitelist + enum `"ENTRIES"` parse

**Files:** `src/sql/parser/dialect/mod.rs` (~1354), `src/connector/iceberg/metadata.rs` (`parse`, lines 55–66)

- [ ] **Step 1: Write the failing test**

In `metadata.rs` tests (the `#[cfg(test)] mod tests` near line 790), add:

```rust
    #[test]
    fn parse_accepts_entries_files_manifests() {
        assert_eq!(IcebergMetadataTableType::parse("entries").unwrap(), IcebergMetadataTableType::LogicalIcebergMetadata);
        assert_eq!(IcebergMetadataTableType::parse("files").unwrap(), IcebergMetadataTableType::Files);
        assert_eq!(IcebergMetadataTableType::parse("manifests").unwrap(), IcebergMetadataTableType::Manifests);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks --lib connector::iceberg::metadata::tests::parse_accepts_entries 2>&1 | tail -15`
Expected: FAIL — `parse("entries")` returns `Err`.

- [ ] **Step 3: Add the `"ENTRIES"` arm**

In `IcebergMetadataTableType::parse` (metadata.rs:56–65), add before the `other =>` arm:

```rust
            "ENTRIES" => Ok(Self::LogicalIcebergMetadata),
```

- [ ] **Step 4: Extend the parser whitelist**

In `src/sql/parser/dialect/mod.rs`, function `rewrite_iceberg_metadata_suffix`, replace the match arm (line ~1355):

```rust
                "snapshots" | "history" | "refs" | "partitions" => {}
```

with:

```rust
                "snapshots" | "history" | "refs" | "partitions" | "files" | "manifests"
                | "entries" => {}
```

and update the adjacent error message to read `expected one of snapshots/history/refs/partitions/files/manifests/entries`.

- [ ] **Step 5: Run to verify it passes + commit**

Run: `cargo test -p novarocks --lib connector::iceberg::metadata::tests::parse_accepts_entries 2>&1 | tail -15`
Expected: PASS.

```bash
git add src/sql/parser/dialect/mod.rs src/connector/iceberg/metadata.rs
git commit -m "feat(iceberg-meta): accept files/manifests/entries metadata-table suffixes"
```

---

## Task 2: Analyzer column schemas

**Files:** `src/sql/analyzer/iceberg_metadata.rs` (replace the `Vec::new()` arm at 87–93)

- [ ] **Step 1: Write the failing test**

Replace the existing `out_of_scope_metatypes_produce_empty_schema` test with:

```rust
    #[test]
    fn files_schema_has_expected_columns() {
        let names: Vec<String> = metadata_table_schema(IcebergMetadataTableType::Files)
            .iter().map(|c| c.name.clone()).collect();
        for col in ["content", "file_path", "file_format", "spec_id", "record_count",
                    "file_size_in_bytes", "column_sizes", "lower_bounds", "split_offsets",
                    "equality_ids", "first_row_id", "partition"] {
            assert!(names.contains(&col.to_string()), "missing {col}");
        }
    }

    #[test]
    fn manifests_schema_has_partition_summaries() {
        let names: Vec<String> = metadata_table_schema(IcebergMetadataTableType::Manifests)
            .iter().map(|c| c.name.clone()).collect();
        assert!(names.contains(&"partition_summaries".to_string()));
        assert!(names.contains(&"added_snapshot_id".to_string()));
    }

    #[test]
    fn entries_schema_superset_of_files() {
        let names: Vec<String> = metadata_table_schema(IcebergMetadataTableType::LogicalIcebergMetadata)
            .iter().map(|c| c.name.clone()).collect();
        for col in ["status", "snapshot_id", "sequence_number", "file_sequence_number",
                    "file_path", "record_count"] {
            assert!(names.contains(&col.to_string()), "missing {col}");
        }
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks --lib analyzer::iceberg_metadata 2>&1 | tail -15`
Expected: FAIL (empty schema + the deleted out-of-scope test).

- [ ] **Step 3: Implement the schemas**

Add a Map-type helper near the top of `iceberg_metadata.rs`:

```rust
use arrow::datatypes::Field;
use std::sync::Arc;

/// Build an Arrow `Map<Int32, value>` data type matching the metadata-scan
/// `MapBuilder` output (keys non-nullable). Mirrors `iceberg_map_field_names`
/// usage in `metadata.rs`.
fn map_int_to(value: DataType) -> DataType {
    let entries = DataType::Struct(
        vec![
            Arc::new(Field::new("keys", DataType::Int32, false)),
            Arc::new(Field::new("values", value, true)),
        ]
        .into(),
    );
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

fn list_of(value: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", value, true)))
}
```

Replace the `T::Files | T::Manifests | T::LogicalIcebergMetadata => { Vec::new() }` arm with three arms. The `$files` file-column list is reused by `$entries`, so factor it into a local closure:

```rust
        T::Files => files_columns(),
        T::Manifests => vec![
            MetadataColumn::new("content", DataType::Int32, false),
            MetadataColumn::new("path", DataType::Utf8, false),
            MetadataColumn::new("length", DataType::Int64, false),
            MetadataColumn::new("partition_spec_id", DataType::Int32, false),
            MetadataColumn::new("added_snapshot_id", DataType::Int64, true),
            MetadataColumn::new("added_data_files_count", DataType::Int32, false),
            MetadataColumn::new("existing_data_files_count", DataType::Int32, false),
            MetadataColumn::new("deleted_data_files_count", DataType::Int32, false),
            MetadataColumn::new("added_rows_count", DataType::Int64, false),
            MetadataColumn::new("existing_rows_count", DataType::Int64, false),
            MetadataColumn::new("deleted_rows_count", DataType::Int64, false),
            MetadataColumn::new(
                "partition_summaries",
                list_of(DataType::Struct(
                    vec![
                        Arc::new(Field::new("contains_null", DataType::Boolean, true)),
                        Arc::new(Field::new("contains_nan", DataType::Boolean, true)),
                        Arc::new(Field::new("lower_bound", DataType::Utf8, true)),
                        Arc::new(Field::new("upper_bound", DataType::Utf8, true)),
                    ]
                    .into(),
                )),
                true,
            ),
        ],
        T::LogicalIcebergMetadata => {
            let mut cols = vec![
                MetadataColumn::new("status", DataType::Int32, false),
                MetadataColumn::new("snapshot_id", DataType::Int64, true),
                MetadataColumn::new("sequence_number", DataType::Int64, true),
                MetadataColumn::new("file_sequence_number", DataType::Int64, true),
                MetadataColumn::new("first_row_id", DataType::Int64, true),
            ];
            cols.extend(files_columns());
            cols
        }
```

and define `files_columns()` as a free function in the file:

```rust
fn files_columns() -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::new("content", DataType::Int32, false),
        MetadataColumn::new("file_path", DataType::Utf8, false),
        MetadataColumn::new("file_format", DataType::Utf8, false),
        MetadataColumn::new("spec_id", DataType::Int32, false),
        MetadataColumn::new("record_count", DataType::Int64, false),
        MetadataColumn::new("file_size_in_bytes", DataType::Int64, false),
        MetadataColumn::new("column_sizes", map_int_to(DataType::Int64), true),
        MetadataColumn::new("value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("null_value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("nan_value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("lower_bounds", map_int_to(DataType::Binary), true),
        MetadataColumn::new("upper_bounds", map_int_to(DataType::Binary), true),
        MetadataColumn::new("split_offsets", list_of(DataType::Int64), true),
        MetadataColumn::new("equality_ids", list_of(DataType::Int32), true),
        MetadataColumn::new("sort_order_id", DataType::Int32, true),
        MetadataColumn::new("key_metadata", DataType::Binary, true),
        MetadataColumn::new("first_row_id", DataType::Int64, true),
        MetadataColumn::new("partition", DataType::Utf8, true),
    ]
}
```

- [ ] **Step 4: Run to verify it passes + commit**

Run: `cargo test -p novarocks --lib analyzer::iceberg_metadata 2>&1 | tail -15`
Expected: PASS.

```bash
git add src/sql/analyzer/iceberg_metadata.rs
git commit -m "feat(iceberg-meta): analyzer column schemas for files/manifests/entries"
```

---

## Task 3: Add `IcebergTableInfo.serialized_metadata_rows`

**Files:** `src/sql/catalog.rs` (`IcebergTableInfo`, line 110), plus every construction site.

- [ ] **Step 1: Add the field**

After `pub serialized_metadata: Option<String>,` (line 130) add:

```rust
    /// JSON-serialized per-row payload for the `$files` / `$manifests` /
    /// `$entries` metadata tables, produced by the resolution-time manifest
    /// walk (`metadata_read.rs`). `None` for all other tables. The
    /// `IcebergMetadataScanOp` deserializes this as its row source.
    pub serialized_metadata_rows: Option<String>,
```

- [ ] **Step 2: Fix every construction site**

Run: `cargo build -p novarocks --lib 2>&1 | grep -A2 "missing field serialized_metadata_rows" | head -40`
This lists every `IcebergTableInfo { ... }` literal. Add `serialized_metadata_rows: None,` to each (the only site that sets it non-None is the new builder in Task 5). Known sites: `src/connector/iceberg/catalog/backend.rs` (~595), `empty_iceberg_scan_source` helpers, and test fixtures in `provider.rs`/`query_prep.rs`.

- [ ] **Step 3: Build clean + commit**

Run: `cargo build -p novarocks --lib 2>&1 | tail -5`
Expected: builds.

```bash
git add -A
git commit -m "feat(iceberg-meta): add IcebergTableInfo.serialized_metadata_rows carry field"
```

---

## Task 4: Resolution-time manifest walk (`metadata_read.rs`)

**Files:** Create `src/connector/iceberg/metadata_read.rs`; register `mod metadata_read;` in `src/connector/iceberg/mod.rs`.

This is the data source for all three tables. It walks the current snapshot's manifest list once and emits the per-table rows as JSON `serde_json::Value`s, returning a `{ "version": 1, "rows": [...] }` payload string (same envelope the `$partitions` payload uses). Row field names match the analyzer columns exactly.

- [ ] **Step 1: Write the walk + a unit test**

Create `src/connector/iceberg/metadata_read.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See NOTICE. (Apache-2.0)

//! Resolution-time manifest walk that materialises the `$files`,
//! `$manifests`, and `$entries` metadata-table rows as a JSON payload.
//! Runs on the server thread (async) during catalog resolution — the sync
//! pipeline scan op cannot perform manifest I/O.

use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, ManifestStatus};
use iceberg::table::Table;
use serde_json::{json, Value};

use crate::connector::iceberg::IcebergMetadataTableType;

/// Stable string form of a partition struct (mirrors `iceberg_partition_key`).
fn partition_string(df: &DataFile) -> String {
    crate::connector::iceberg::read::iceberg_partition_key(df.partition())
        .unwrap_or_else(|| "Struct([])".to_string())
}

fn content_code(df: &DataFile) -> i32 {
    match df.content_type() {
        DataContentType::Data => 0,
        DataContentType::PositionDeletes => 1,
        DataContentType::EqualityDeletes => 2,
    }
}

fn file_format_str(fmt: DataFileFormat) -> &'static str {
    match fmt {
        DataFileFormat::Parquet => "PARQUET",
        DataFileFormat::Orc => "ORC",
        DataFileFormat::Avro => "AVRO",
        DataFileFormat::Puffin => "PUFFIN",
    }
}

/// field-id-keyed `{int -> int}` map as a JSON array of `[k, v]` pairs (the
/// scan-op builder reconstructs the Arrow map from these pairs, key order
/// sorted for determinism).
fn int_map(m: &std::collections::HashMap<i32, u64>) -> Value {
    let mut pairs: Vec<(i32, u64)> = m.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by_key(|(k, _)| *k);
    json!(pairs.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>())
}

fn bytes_map(m: &std::collections::HashMap<i32, Vec<u8>>) -> Value {
    let mut pairs: Vec<(i32, Vec<u8>)> = m.iter().map(|(k, v)| (*k, v.clone())).collect();
    pairs.sort_by_key(|(k, _)| *k);
    json!(pairs.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>())
}

/// Build the `$files`-shaped JSON object for one `DataFile`. `entry_cols`,
/// when Some, prepends the `$entries` columns (status/snapshot_id/seq/...).
fn file_row(df: &DataFile, entry_cols: Option<Value>) -> Result<Value, String> {
    // lower/upper bounds: iceberg-rust returns Datum; serialize via to_bytes.
    let mut lower = std::collections::HashMap::new();
    for (k, datum) in df.lower_bounds() {
        if let Ok(b) = datum.to_bytes() {
            lower.insert(*k, b.to_vec());
        }
    }
    let mut upper = std::collections::HashMap::new();
    for (k, datum) in df.upper_bounds() {
        if let Ok(b) = datum.to_bytes() {
            upper.insert(*k, b.to_vec());
        }
    }
    let base = json!({
        "content": content_code(df),
        "file_path": df.file_path(),
        "file_format": file_format_str(df.file_format()),
        "spec_id": df.partition_spec_id(),
        "record_count": df.record_count(),
        "file_size_in_bytes": df.file_size_in_bytes(),
        "column_sizes": int_map(df.column_sizes()),
        "value_counts": int_map(df.value_counts()),
        "null_value_counts": int_map(df.null_value_counts()),
        "nan_value_counts": int_map(df.nan_value_counts()),
        "lower_bounds": bytes_map(&lower),
        "upper_bounds": bytes_map(&upper),
        "split_offsets": df.split_offsets(),
        "equality_ids": df.equality_ids(),
        "sort_order_id": df.sort_order_id(),
        "key_metadata": df.key_metadata().map(|b| b.to_vec()),
        "first_row_id": df.first_row_id(),
        "partition": partition_string(df),
    });
    match entry_cols {
        None => Ok(base),
        Some(Value::Object(mut entry)) => {
            if let Value::Object(b) = base {
                entry.extend(b);
            }
            Ok(Value::Object(entry))
        }
        Some(_) => Err("entry columns must be a JSON object".to_string()),
    }
}

/// Walk the current snapshot's manifest list and produce the `{version,rows}`
/// payload for the requested metadata-table type.
pub async fn read_metadata_table_rows(
    table: &Table,
    file_io: &FileIO,
    ty: IcebergMetadataTableType,
) -> Result<String, String> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(json!({ "version": 1, "rows": [] }).to_string());
    };
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| format!("load manifest list: {e}"))?;

    let mut rows: Vec<Value> = Vec::new();
    for mf in manifest_list.entries() {
        if ty == IcebergMetadataTableType::Manifests {
            rows.push(json!({
                "content": match mf.content { iceberg::spec::ManifestContentType::Data => 0, _ => 1 },
                "path": mf.manifest_path,
                "length": mf.manifest_length,
                "partition_spec_id": mf.partition_spec_id,
                "added_snapshot_id": mf.added_snapshot_id,
                "added_data_files_count": mf.added_files_count,
                "existing_data_files_count": mf.existing_files_count,
                "deleted_data_files_count": mf.deleted_files_count,
                "added_rows_count": mf.added_rows_count,
                "existing_rows_count": mf.existing_rows_count,
                "deleted_rows_count": mf.deleted_rows_count,
                "partition_summaries": mf.partitions.iter().map(|p| json!({
                    "contains_null": p.contains_null,
                    "contains_nan": p.contains_nan,
                    "lower_bound": p.lower_bound.as_ref().map(|b| format!("{b:?}")),
                    "upper_bound": p.upper_bound.as_ref().map(|b| format!("{b:?}")),
                })).collect::<Vec<_>>(),
            }));
            continue;
        }
        // $files / $entries need entry-level walk.
        let manifest = mf
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {}: {e}", mf.manifest_path))?;
        for entry in manifest.entries() {
            let df = entry.data_file();
            match ty {
                IcebergMetadataTableType::Files => {
                    // live entries only (skip deleted).
                    if entry.status() == ManifestStatus::Deleted {
                        continue;
                    }
                    rows.push(file_row(df, None)?);
                }
                IcebergMetadataTableType::LogicalIcebergMetadata => {
                    // all entries incl. deleted.
                    let status = match entry.status() {
                        ManifestStatus::Existing => 0,
                        ManifestStatus::Added => 1,
                        ManifestStatus::Deleted => 2,
                    };
                    let entry_cols = json!({
                        "status": status,
                        "snapshot_id": entry.snapshot_id(),
                        "sequence_number": entry.sequence_number(),
                        "file_sequence_number": entry.file_sequence_number,
                        "first_row_id": df.first_row_id(),
                    });
                    rows.push(file_row(df, Some(entry_cols))?);
                }
                _ => unreachable!("manifests handled above"),
            }
        }
    }
    Ok(json!({ "version": 1, "rows": rows }).to_string())
}
```

> NOTE during impl: confirm the exact `ManifestFile` field names (`manifest_length`, `added_files_count`, `existing_files_count`, `deleted_files_count`, `added_rows_count`, `existing_rows_count`, `deleted_rows_count`, `partitions`) and `FieldSummary` field names (`contains_null`, `contains_nan`, `lower_bound`, `upper_bound`) against `vendor/iceberg-0.9.0/src/spec/manifest_list.rs` (the `ManifestFile` struct is at line 698; `read.rs` already reads `mf.partition_spec_id`, `mf.sequence_number`, `mf.first_row_id`, `mf.content`, `mf.manifest_path`). Also confirm `iceberg_partition_key` is `pub` in `read.rs` (make it `pub(crate)` if not). Add a unit test `read_metadata_table_rows_empty_snapshot_returns_empty` driving a no-snapshot `Table` (reuse `commit::test_helpers::empty_v3_iceberg_table` via `#[cfg(test)]`) and asserting the payload is `{"version":1,"rows":[]}`.

- [ ] **Step 2: Run the unit test to verify it fails, then passes**

Run (after registering the module): `cargo test -p novarocks --lib connector::iceberg::metadata_read 2>&1 | tail -20`
Iterate until the new test passes and the file compiles (resolve any field-name mismatches flagged by the NOTE).

- [ ] **Step 3: Commit**

```bash
git add src/connector/iceberg/metadata_read.rs src/connector/iceberg/mod.rs
git commit -m "feat(iceberg-meta): resolution-time manifest walk for files/manifests/entries rows"
```

---

## Task 5: `TableSource::build_metadata_rows_table_def` + provider routing

**Files:** `src/connector/backend.rs` (trait), `src/connector/iceberg/catalog/backend.rs` (impl), `src/engine/catalog_mgr/provider.rs` (routing)

- [ ] **Step 1: Add the trait method**

In `src/connector/backend.rs`, on the `TableSource` trait (next to `build_table_def` / `build_schema_table_def`), add:

```rust
    /// Build a metadata-table `TableDef` whose `IcebergTableInfo` carries the
    /// serialized `$files`/`$manifests`/`$entries` rows. Default: same as the
    /// schema-only def (non-iceberg sources have no manifest rows).
    fn build_metadata_rows_table_def(
        &self,
        resolved: &ResolvedTable,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<crate::sql::catalog::TableDef, String> {
        let _ = metadata_table_type;
        self.build_schema_table_def(resolved)
    }
```

- [ ] **Step 2: Implement it for the iceberg source**

In `src/connector/iceberg/catalog/backend.rs`, override `build_metadata_rows_table_def` on the iceberg `TableSource` impl: start from `build_schema_table_def(resolved)`, run the walk via `block_on_iceberg`, and stash the JSON onto the returned `TableDef`'s `IcebergTableInfo.serialized_metadata_rows`:

```rust
    fn build_metadata_rows_table_def(
        &self,
        resolved: &ResolvedTable,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<crate::sql::catalog::TableDef, String> {
        let mut def = self.build_schema_table_def(resolved)?;
        let file_io = /* obtain FileIO from `resolved` exactly as load_table does */;
        let rows = crate::connector::iceberg::commit::run::block_on_iceberg(async {
            crate::connector::iceberg::metadata_read::read_metadata_table_rows(
                &resolved.table, // the iceberg-rust Table on ResolvedTable
                &file_io,
                metadata_table_type,
            )
            .await
        })?;
        if let crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. } = &mut def.source {
            table.serialized_metadata_rows = Some(rows);
        }
        Ok(def)
    }
```

> NOTE during impl: `build_schema_table_def` returns a `ScanSource::IcebergDataFiles { files: vec![], table, .. }` (the empty-files schema shape). Obtain `FileIO` and the iceberg `Table` from `ResolvedTable` the same way `load_table`/`build_read_snapshot_at` do (grep `ResolvedTable` fields + how `read.rs` gets its `file_io`). `block_on_iceberg` lives in `commit/run.rs` (confirm the path/visibility; it is already used in `registry.rs`/`schema_update.rs`).

- [ ] **Step 3: Route the three types in the provider**

In `src/engine/catalog_mgr/provider.rs`, the `iceberg_table_def` method, replace the catch-all `TableLookupMode::IcebergMetadata { .. }` arm (lines 63–68) so Files/Manifests/Entries use the new builder:

```rust
            TableLookupMode::IcebergMetadata { metadata_table_type }
                if matches!(
                    metadata_table_type,
                    crate::connector::iceberg::IcebergMetadataTableType::Files
                        | crate::connector::iceberg::IcebergMetadataTableType::Manifests
                        | crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata
                ) =>
            {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_metadata_rows_table_def(&resolved, metadata_table_type.clone())
            }
            TableLookupMode::IcebergMetadata { .. } => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_schema_table_def(&resolved)
            }
```

(Keep the existing `Partitions` arm above it unchanged.)

- [ ] **Step 4: Build + provider tests + commit**

Run: `cargo test -p novarocks --lib catalog_mgr::provider 2>&1 | tail -15`
Expected: existing provider tests pass (the new arm only affects Files/Manifests/Entries).

```bash
git add src/connector/backend.rs src/connector/iceberg/catalog/backend.rs src/engine/catalog_mgr/provider.rs
git commit -m "feat(iceberg-meta): resolve files/manifests/entries rows at catalog lookup"
```

---

## Task 6: Planner payload branches

**Files:** `src/sql/planner/mod.rs` (`build_iceberg_metadata_payload`, lines 2122–2140)

- [ ] **Step 1: Add the three branches**

Replace the `_ => Ok(None)` arm with:

```rust
        IcebergMetadataTableType::Files
        | IcebergMetadataTableType::Manifests
        | IcebergMetadataTableType::LogicalIcebergMetadata => {
            let table_info = crate::sql::planner::iceberg_table_info(storage).ok_or_else(|| {
                "iceberg files/manifests/entries metadata table requires iceberg table identity"
                    .to_string()
            })?;
            table_info
                .serialized_metadata_rows
                .clone()
                .map(Some)
                .ok_or_else(|| {
                    "iceberg metadata rows were not resolved at catalog lookup time".to_string()
                })
        }
        IcebergMetadataTableType::Snapshots
        | IcebergMetadataTableType::History
        | IcebergMetadataTableType::Refs => Ok(None),
```

(`iceberg_table_info` is the existing helper at planner/mod.rs:2307; reference it via the in-scope path. The `Partitions` arm stays unchanged.)

- [ ] **Step 2: Build + commit**

Run: `cargo build -p novarocks --lib 2>&1 | tail -5`
Expected: builds.

```bash
git add src/sql/planner/mod.rs
git commit -m "feat(iceberg-meta): forward serialized metadata rows as scan payload"
```

---

## Task 7: `$files` scan-op row builder + un-reject

**Files:** `src/connector/iceberg/metadata.rs`

- [ ] **Step 1: Add the row struct + loader + builders**

Add (near the partition equivalents):

```rust
#[derive(Clone, Debug, serde::Deserialize)]
struct FilesMetadataPayload {
    version: i32,
    rows: Vec<serde_json::Value>,
}

fn load_files_rows(cfg: &IcebergMetadataScanConfig) -> Result<Vec<serde_json::Value>, String> {
    if cfg.serialized_predicate.trim().is_empty() {
        return Err("iceberg files metadata scan missing payload".to_string());
    }
    let payload: FilesMetadataPayload = serde_json::from_str(&cfg.serialized_predicate)
        .map_err(|e| format!("parse iceberg files metadata payload failed: {e}"))?;
    if payload.version != 1 {
        return Err(format!("unsupported files metadata payload version {}", payload.version));
    }
    Ok(payload.rows)
}
```

(`$manifests` and `$entries` reuse the same `version/rows` envelope — Task 8/9 add `load_manifests_rows`/`load_entries_rows` that are identical except the error string; or generalize `load_files_rows` into a shared `load_json_rows(cfg, label)`.)

Add the column builder. Scalars follow the exact `build_snapshot_array` pattern (Int32Array/Int64Array/StringArray/from JSON via `row.get(name)`). The novel builders are the field-id maps, the lists, and the binary maps:

```rust
fn build_files_array(
    column: &IcebergMetadataOutputColumn,
    rows: &[serde_json::Value],
) -> Result<ArrayRef, String> {
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    };
    match column.name.as_str() {
        "content" | "spec_id" | "sort_order_id" => {
            let mut b = Int32Builder::new();
            for r in rows { b.append_option(r.get(&column.name).and_then(|v| v.as_i64()).map(|v| v as i32)); }
            Ok(Arc::new(b.finish()))
        }
        "record_count" | "file_size_in_bytes" | "first_row_id" => {
            let mut b = Int64Builder::new();
            for r in rows { b.append_option(r.get(&column.name).and_then(|v| v.as_i64())); }
            Ok(Arc::new(b.finish()))
        }
        "file_path" | "file_format" | "partition" => {
            let mut b = StringBuilder::new();
            for r in rows { b.append_option(r.get(&column.name).and_then(|v| v.as_str())); }
            Ok(Arc::new(b.finish()))
        }
        "key_metadata" => {
            let mut b = BinaryBuilder::new();
            for r in rows {
                match r.get("key_metadata").and_then(|v| v.as_array()) {
                    Some(bytes) => b.append_value(json_u8_array(bytes)?),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "column_sizes" | "value_counts" | "null_value_counts" | "nan_value_counts" => {
            build_int_int_map_array(rows, &column.name)
        }
        "lower_bounds" | "upper_bounds" => build_int_binary_map_array(rows, &column.name),
        "split_offsets" => {
            let mut b = ListBuilder::new(Int64Builder::new());
            for r in rows {
                match r.get("split_offsets").and_then(|v| v.as_array()) {
                    Some(items) => { for it in items { b.values().append_option(it.as_i64()); } b.append(true); }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        "equality_ids" => {
            let mut b = ListBuilder::new(Int32Builder::new());
            for r in rows {
                match r.get("equality_ids").and_then(|v| v.as_array()) {
                    Some(items) => { for it in items { b.values().append_option(it.as_i64().map(|x| x as i32)); } b.append(true); }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(format!("unsupported iceberg files metadata column: {other}")),
    }
}
```

Add the shared helpers (`build_int_int_map_array`, `build_int_binary_map_array`, `json_u8_array`) — `build_int_int_map_array` mirrors `build_string_string_map_array` (metadata.rs:471) but with `Int32Builder` keys / `Int64Builder` values, reading each row's `[[k,v],...]` pairs; `build_int_binary_map_array` uses `BinaryBuilder` values; `json_u8_array(&[Value]) -> Result<Vec<u8>,String>` maps `as_u64` → `u8`.

- [ ] **Step 2: Un-reject `Files` in constructor + execute_iter**

In `IcebergMetadataScanOp::new` (metadata.rs:122–137), move `Files` out of the reject arm into the allowed arm. In `execute_iter` (metadata.rs:240–292), replace the `Files` reject branch with:

```rust
            IcebergMetadataTableType::Files => {
                let rows = load_files_rows(&self.cfg)?;
                build_files_chunks(&rows, &self.cfg.output_columns, &self.output_schema, &self.output_chunk_schema, self.cfg.batch_size)?
            }
```

and add `build_files_chunks` mirroring `build_partition_chunks` (metadata.rs:739) but calling `build_files_array`.

- [ ] **Step 3: Unit test (type normalization round-trips through the builder)**

Add a test feeding a hand-built `rows` JSON (one data-file row with a `column_sizes` of `[[1,100]]`, `split_offsets` `[0,128]`) through `build_files_array` for each column and asserting the Arrow array types/values.

- [ ] **Step 4: Build + test + commit**

Run: `cargo test -p novarocks --lib connector::iceberg::metadata 2>&1 | tail -20`
Expected: PASS.

```bash
git add src/connector/iceberg/metadata.rs
git commit -m "feat(iceberg-meta): materialize \$files rows (stats maps, lists, DV delete rows)"
```

---

## Task 8: `$manifests` scan-op row builder + un-reject

**Files:** `src/connector/iceberg/metadata.rs`

- [ ] **Step 1:** Add `load_manifests_rows` (identical envelope to `load_files_rows`) + `build_manifests_array`. Scalars (content/path/length/partition_spec_id/added_snapshot_id/the counts) follow the same scalar pattern as Task 7. The novel column is `partition_summaries` → `ListBuilder<StructBuilder>`:

```rust
        "partition_summaries" => {
            use arrow::array::StructBuilder;
            let fields = match column.data_type { DataType::List(ref f) => match f.data_type() {
                DataType::Struct(fs) => fs.clone(), _ => return Err("partition_summaries inner not struct".into()) },
                _ => return Err("partition_summaries not list".into()) };
            let mut b = ListBuilder::new(StructBuilder::from_fields(fields.clone(), 0));
            for r in rows {
                match r.get("partition_summaries").and_then(|v| v.as_array()) {
                    Some(items) => {
                        for it in items {
                            let sb = b.values();
                            sb.field_builder::<BooleanBuilder>(0).unwrap().append_option(it.get("contains_null").and_then(|v| v.as_bool()));
                            sb.field_builder::<BooleanBuilder>(1).unwrap().append_option(it.get("contains_nan").and_then(|v| v.as_bool()));
                            sb.field_builder::<StringBuilder>(2).unwrap().append_option(it.get("lower_bound").and_then(|v| v.as_str()));
                            sb.field_builder::<StringBuilder>(3).unwrap().append_option(it.get("upper_bound").and_then(|v| v.as_str()));
                            sb.append(true);
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Ok(Arc::new(b.finish()))
        }
```

- [ ] **Step 2:** Un-reject `Manifests` in `new()` + add the `Manifests` arm to `execute_iter` (mirror Task 7) + `build_manifests_chunks`.

- [ ] **Step 3:** Unit test feeding a hand-built manifest row (with one partition summary) through `build_manifests_array`.

- [ ] **Step 4:** Build + test + commit:

Run: `cargo test -p novarocks --lib connector::iceberg::metadata 2>&1 | tail -20` (PASS)

```bash
git add src/connector/iceberg/metadata.rs
git commit -m "feat(iceberg-meta): materialize \$manifests rows (partition summaries list-of-struct)"
```

---

## Task 9: `$entries` scan-op row builder + un-reject

**Files:** `src/connector/iceberg/metadata.rs`

- [ ] **Step 1:** Add `load_entries_rows` + `build_entries_array`. The entry-level columns (`status`/`snapshot_id`/`sequence_number`/`file_sequence_number`) are scalar Int32/Int64; **all file columns delegate to `build_files_array`** (since the JSON row already carries them under the same names):

```rust
fn build_entries_array(column: &IcebergMetadataOutputColumn, rows: &[serde_json::Value]) -> Result<ArrayRef, String> {
    use arrow::array::{Int32Builder, Int64Builder};
    match column.name.as_str() {
        "status" => { let mut b = Int32Builder::new(); for r in rows { b.append_option(r.get("status").and_then(|v| v.as_i64()).map(|v| v as i32)); } Ok(Arc::new(b.finish())) }
        "snapshot_id" | "sequence_number" | "file_sequence_number" => {
            let mut b = Int64Builder::new(); for r in rows { b.append_option(r.get(&column.name).and_then(|v| v.as_i64())); } Ok(Arc::new(b.finish()))
        }
        // first_row_id + every $files column reuse the files builder.
        _ => build_files_array(column, rows),
    }
}
```

- [ ] **Step 2:** Un-reject `LogicalIcebergMetadata` in `new()` + add its `execute_iter` arm + `build_entries_chunks`.

- [ ] **Step 3:** Unit test (one added + one deleted entry → assert `status` 1/2 and file columns present).

- [ ] **Step 4:** Build + test + commit:

Run: `cargo test -p novarocks --lib connector::iceberg::metadata 2>&1 | tail -20` (PASS)

```bash
git add src/connector/iceberg/metadata.rs
git commit -m "feat(iceberg-meta): materialize \$entries rows (flat entry + file columns)"
```

---

## Task 10: SQL self-consistency goldens

**Files:** `sql-tests/iceberg/sql/iceberg_metadata_files.sql` (+ result), `iceberg_metadata_manifests.sql`, `iceberg_metadata_entries.sql`

- [ ] **Step 1: Write the fixtures** (mirror `iceberg_metadata_partitions.sql` structure). Example for `$files` (includes the IV3-2↔IV3-8 mutual check — file count vs inserted batches):

```sql
-- @order_sensitive=true
-- IV3-8: $files lists data files with stats; cross-checks file count.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};
-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");
-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES (1,10);
-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES (2,20);
-- query 5
-- two appends -> two data files, all content=0
SELECT count(*) AS n_data_files
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0}$files
  WHERE content = 0;
-- query 6
-- total record_count across files == 2 rows inserted
SELECT sum(record_count) AS total_records
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0}$files
  WHERE content = 0;
-- query 7
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};
```

`$manifests`: assert `count(*) >= 1` and `sum(added_data_files_count) >= 1`. `$entries`: assert `count(*) >= 2` and that `sequence_number IS NOT NULL` for added entries.

- [ ] **Step 2: Record + verify** (start standalone-server per CLAUDE.md readiness gate, then):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start server, wait for NOVAROCKS_READY (see CLAUDE.md §7.3), then:
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg \
  --only iceberg_metadata_files,iceberg_metadata_manifests,iceberg_metadata_entries --mode record
# inspect the .result files, then re-run with --mode verify
```

Expected: query 5 → `2`; query 6 → `2`; verify mode PASS.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_metadata_{files,manifests,entries}.sql \
        sql-tests/iceberg/result/iceberg_metadata_{files,manifests,entries}.result
git commit -m "test(iceberg): \$files/\$manifests/\$entries self-consistency goldens"
```

---

## Task 11: Cross-engine Spark compatibility extension

**Files:** `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_metadata_tables.sql` (+ result)

- [ ] **Step 1: Extend the existing test** — after the current query 3 (`$history` count), before the `DROP TABLE`, add queries that read the Spark-written v3 table's new metadata tables and assert against Spark's known writes (2 inserts → ≥2 data files, partitioned by region):

```sql
-- query 4
-- NovaRocks reads $files over the Spark-written v3 table.
SELECT count(*) AS n_data_files
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$files
WHERE content = 0;

-- query 5
-- $manifests is non-empty.
SELECT count(*) > 0 AS has_manifests
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$manifests;

-- query 6
-- $entries exposes sequence numbers for the Spark-added files.
SELECT count(*) > 0 AS has_added_entries
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$entries
WHERE status = 1 AND sequence_number IS NOT NULL;
```

Renumber the old `DROP TABLE` to query 7.

- [ ] **Step 2: Record + verify** (requires Docker fixture + Spark):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start server (readiness gate), then:
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-compatibility \
  --only spark_rest_minio_v3_metadata_tables --mode record
# inspect results (query 4 >= 2; query 5 = 1/true; query 6 = 1/true), then --mode verify
```

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_metadata_tables.sql \
        sql-tests/iceberg-compatibility/result/spark_rest_minio_v3_metadata_tables.result
git commit -m "test(iceberg-compat): cross-engine \$files/\$manifests/\$entries vs Spark"
```

---

## Task 12: Full build + fmt/clippy + suite sweep

- [ ] **Step 1:** `cargo fmt && cargo clippy -p novarocks --lib 2>&1 | tail -30` — no fmt diff, no new warnings.
- [ ] **Step 2:** `cargo test -p novarocks --lib connector::iceberg::metadata 2>&1 | tail -20` — all metadata-table unit tests pass.
- [ ] **Step 3:** Commit any fmt fixes:

```bash
git add -A && git commit -m "chore(iceberg-meta): fmt after files/manifests/entries" || echo "nothing to commit"
```

---

## Self-Review (completed during planning)

**Spec coverage (vs design Part B / §11 IV3-8):**
- B2 schemas (files/manifests/entries, D1 flat entries, D6 binary bounds) → Task 2 ✓
- B1 data source (resolution-time walk, $partitions-style payload) → Tasks 4–6 ✓
- B3 framework接线 (parser whitelist, enum, analyzer, metadata.rs builders, un-reject, planner) → Tasks 1,2,5,6,7,8,9 ✓
- B4 multi-spec/schema-evolution (field-id-keyed maps; each row carries its own spec_id; partition as stable string) → Task 4 `file_row` ✓
- B5 encoding → **no change needed** (Map/Struct/List already routed through `format_mysql_container_value_with_schema`; we use `List` not `LargeList`) — covered by goldens (Task 10) ✓
- Verification: self-consistency goldens + IV3-2↔IV3-8 mutual file-count check (Task 10) ✓; cross-engine Spark (Task 11) ✓
- Acceptance #1 (rows match Spark on key columns), #2 (`$entries` first_row_id/sequence_number), #3 (DV delete rows in `$files`, content=1, PUFFIN) → Tasks 7/9 build content+file_format; Task 11 cross-checks ✓
- **Deferred:** typed dynamic `partition` struct on `$files`/`$entries` (surfaced as stable `Utf8` string here — documented in scope); the cross-engine `total-*` consistency check (IV3-2 acceptance #2) rides the existing `$snapshots` golden once Plan 1 lands.

**Placeholder scan:** Two `NOTE during impl` markers (Task 4 manifest-field names; Task 5 FileIO acquisition + `block_on_iceberg` path) point the executing agent at the exact verbatim sources to confirm — these are field-name/visibility confirmations against open files, not design gaps. All structs, schemas, builders, routing, and wiring are concrete. Scalar array-builders in Tasks 8–9 reference the fully-shown Task 7 pattern (same file, same helpers).

**Type/name consistency:** JSON row field names produced by `metadata_read.rs::file_row`/manifest rows (Task 4) match the analyzer column names (Task 2) and the `build_*_array` match arms (Tasks 7–9) exactly: `content`, `file_path`, `file_format`, `spec_id`, `record_count`, `file_size_in_bytes`, `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts`, `lower_bounds`, `upper_bounds`, `split_offsets`, `equality_ids`, `sort_order_id`, `key_metadata`, `first_row_id`, `partition`; entries add `status`, `snapshot_id`, `sequence_number`, `file_sequence_number`. `IcebergMetadataTableType::LogicalIcebergMetadata` is the `entries` variant throughout.
