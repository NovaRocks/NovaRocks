# Iceberg Read Semantics Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a shared Iceberg read semantics contract used by ordinary `SELECT` and MV/IVM change-read paths, then cover schema evolution, partition evolution, position deletes, Puffin deletion vectors, equality deletes, and row-lineage metadata with focused tests.

**Architecture:** Introduce a small `src/connector/iceberg/read.rs` module that owns current-snapshot read views and delete applicability helpers. Keep existing external shapes (`DataFileWithStats`, `S3FileInfo`, `IcebergChangeBatch`) during the first pass, but make them derive from the shared read view so ordinary scans and MV reverse projection stop duplicating manifest rules.

**Tech Stack:** Rust, iceberg-rust, Arrow `RecordBatch`, Parquet readers, NovaRocks SQL runner, `sql-tests/iceberg`, `sql-tests/mv-on-iceberg`, MinIO-backed managed-lake configs.

---

## Scope Check

This plan covers two subsystems, ordinary Iceberg table reads and MV/IVM change reads, because the accepted spec intentionally binds both to one read semantics contract. The implementation order keeps them independently testable: Tasks 1-3 finish the shared contract and ordinary `SELECT`; Tasks 4-6 wire MV change-read; Tasks 7-8 add SQL coverage and final verification.

## File Structure

- Create `src/connector/iceberg/read.rs`
  - Owns `IcebergReadSnapshot`, `IcebergReadFile`, `IcebergReadDeleteFile`, `IcebergReadDeleteKind`.
  - Owns pure applicability helpers and current-snapshot manifest walking.
- Modify `src/connector/iceberg/mod.rs`
  - Exposes the new `read` module inside the connector crate.
- Modify `src/connector/iceberg/catalog/registry.rs`
  - Delegates current-snapshot data/delete extraction to `read.rs`.
  - Keeps `DataFileWithStats` as the compatibility output for table registration.
- Modify `src/sql/catalog.rs`
  - Adds equality field IDs and partition metadata to the catalog-facing delete/file structs only where scan/MV paths need them.
- Modify `src/connector/iceberg/catalog/backend.rs`
  - Preserves the new read metadata when building `TableDef`.
- Modify `src/sql/codegen/nodes.rs`
  - Threads the new delete metadata into scan ranges without changing FE-facing semantics.
- Modify `src/engine/query_prep.rs`
  - Extends `IcebergFileForQuery` so MV one-shot incremental scans can carry partition/read metadata.
- Modify `src/connector/iceberg/changes.rs`
  - Plans insert/delete/equality-delete deltas from the shared read view.
  - Uses the same equality-delete applicability for MV reverse projection as ordinary reads.
- Modify `src/connector/iceberg/scan_deletes.rs`
  - Keeps position-delete and Puffin DV reverse projection, adding read-view inputs where needed.
- Modify `src/connector/iceberg/equality_delete.rs`
  - Reuses the current field-id-aware key matching and adds pure tests for partition/applicability-driven reverse projection when needed.
- Add/modify SQL tests under:
  - `sql-tests/iceberg/sql/`
  - `sql-tests/iceberg/result/`
  - `sql-tests/mv-on-iceberg/sql/`
  - `sql-tests/mv-on-iceberg/result/`

---

### Task 1: Add Shared Iceberg Read View Types

**Files:**
- Create: `src/connector/iceberg/read.rs`
- Modify: `src/connector/iceberg/mod.rs`

- [ ] **Step 1: Write pure applicability tests**

Create `src/connector/iceberg/read.rs` with tests first. The file should compile-fail at this step because the types and helpers are not implemented yet.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn data_file(seq: Option<i64>, spec_id: Option<i32>, partition_key: Option<&str>) -> IcebergReadFile {
        IcebergReadFile {
            path: "s3://bucket/table/data-1.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            first_row_id: Some(0),
            data_sequence_number: seq,
            deletes: Vec::new(),
        }
    }

    fn equality_delete(seq: Option<i64>, spec_id: Option<i32>, partition_key: Option<&str>) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-1.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality { equality_field_ids: vec![3] },
            length: Some(10),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: seq,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            referenced_data_file: None,
        }
    }

    #[test]
    fn delete_with_older_or_equal_sequence_does_not_apply() {
        let data = data_file(Some(7), None, None);
        let delete = equality_delete(Some(7), None, None);

        assert!(!delete_applies_to_data_file(&delete, &data));
    }

    #[test]
    fn unpartitioned_newer_equality_delete_applies_globally() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let delete = equality_delete(Some(8), None, None);

        assert!(delete_applies_to_data_file(&delete, &data));
    }

    #[test]
    fn partitioned_equality_delete_requires_matching_spec_and_partition() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let same = equality_delete(Some(8), Some(2), Some("city=A"));
        let different_spec = equality_delete(Some(8), Some(3), Some("city=A"));
        let different_partition = equality_delete(Some(8), Some(2), Some("city=B"));

        assert!(delete_applies_to_data_file(&same, &data));
        assert!(!delete_applies_to_data_file(&different_spec, &data));
        assert!(!delete_applies_to_data_file(&different_partition, &data));
    }

    #[test]
    fn referenced_position_delete_requires_matching_data_file() {
        let data = data_file(Some(7), None, None);
        let delete = IcebergReadDeleteFile {
            referenced_data_file: Some(data.path.clone()),
            kind: IcebergReadDeleteKind::Position,
            sequence_number: Some(8),
            ..equality_delete(Some(8), None, None)
        };
        let other = IcebergReadDeleteFile {
            referenced_data_file: Some("s3://bucket/table/other.parquet".to_string()),
            ..delete.clone()
        };

        assert!(delete_applies_to_data_file(&delete, &data));
        assert!(!delete_applies_to_data_file(&other, &data));
    }
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test --lib connector::iceberg::read -- --nocapture
```

Expected: compile failure mentioning missing `IcebergReadFile`, `IcebergReadDeleteFile`, or `delete_applies_to_data_file`.

- [ ] **Step 3: Implement the read view types and pure helper**

Add this production code above the tests in `src/connector/iceberg/read.rs`.

```rust
use std::collections::HashMap;

use crate::sql::catalog::IcebergColumnStats;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteKind {
    Position,
    Equality { equality_field_ids: Vec<i32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergReadDeleteFile {
    pub(crate) path: String,
    pub(crate) file_format: IcebergReadDeleteFormat,
    pub(crate) kind: IcebergReadDeleteKind,
    pub(crate) length: Option<i64>,
    pub(crate) content_offset: Option<i64>,
    pub(crate) content_size_in_bytes: Option<i64>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) referenced_data_file: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadFile {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) deletes: Vec<IcebergReadDeleteFile>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadSnapshot {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) files: Vec<IcebergReadFile>,
}

pub(crate) fn delete_applies_to_data_file(
    delete_file: &IcebergReadDeleteFile,
    data_file: &IcebergReadFile,
) -> bool {
    if let (Some(delete_sequence), Some(data_sequence)) =
        (delete_file.sequence_number, data_file.data_sequence_number)
        && delete_sequence <= data_sequence
    {
        return false;
    }

    if let Some(referenced) = delete_file.referenced_data_file.as_deref()
        && referenced != data_file.path
    {
        return false;
    }

    if let Some(delete_partition) = delete_file.partition_key.as_deref() {
        if let (Some(delete_spec_id), Some(data_spec_id)) =
            (delete_file.partition_spec_id, data_file.partition_spec_id)
            && delete_spec_id != data_spec_id
        {
            return false;
        }
        if data_file.partition_key.as_deref() != Some(delete_partition) {
            return false;
        }
    }

    true
}

pub(crate) fn iceberg_partition_key(partition: &iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}
```

Modify `src/connector/iceberg/mod.rs`:

```rust
pub(crate) mod read;
```

- [ ] **Step 4: Run the pure tests**

Run:

```bash
cargo test --lib connector::iceberg::read -- --nocapture
```

Expected: all `connector::iceberg::read` tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/mod.rs src/connector/iceberg/read.rs
git commit -m "feat: add iceberg read semantics view"
```

---

### Task 2: Build Current-Snapshot Read View From Iceberg Manifests

**Files:**
- Modify: `src/connector/iceberg/read.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 1: Add read-view delete attachment test**

Add this pure conversion test in `src/connector/iceberg/read.rs`:

```rust
#[test]
fn read_view_attaches_only_applicable_deletes() {
    let mut data = data_file(Some(5), Some(1), Some("city=A"));
    let applicable = equality_delete(Some(6), Some(1), Some("city=A"));
    let too_old = equality_delete(Some(5), Some(1), Some("city=A"));
    let wrong_partition = equality_delete(Some(6), Some(1), Some("city=B"));

    attach_applicable_deletes(&mut data, &[applicable.clone(), too_old, wrong_partition]);

    assert_eq!(data.deletes, vec![applicable]);
}
```

- [ ] **Step 2: Implement `attach_applicable_deletes`**

Add this helper to `src/connector/iceberg/read.rs`:

```rust
pub(crate) fn attach_applicable_deletes(
    data_file: &mut IcebergReadFile,
    delete_files: &[IcebergReadDeleteFile],
) {
    data_file.deletes.extend(
        delete_files
            .iter()
            .filter(|delete_file| delete_applies_to_data_file(delete_file, data_file))
            .cloned(),
    );
}
```

- [ ] **Step 3: Add manifest read entrypoint**

Add `build_read_snapshot` to `src/connector/iceberg/read.rs`. Start by moving the body of `extract_data_files_with_stats` from `src/connector/iceberg/catalog/registry.rs` into this function, changing only the output type.

The function signature must be:

```rust
pub(crate) fn build_read_snapshot(
    table: &iceberg::table::Table,
) -> Result<IcebergReadSnapshot, String> {
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    let metadata = table.metadata();
    let snapshot = match metadata.current_snapshot() {
        Some(snapshot) => snapshot,
        None => {
            return Ok(IcebergReadSnapshot {
                snapshot_id: None,
                files: Vec::new(),
            });
        }
    };

    let file_io = table.file_io();
    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| format!("load manifest list: {e}"))?;

        let mut all_deletes = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load delete manifest {}: {e}", manifest_file.manifest_path))?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let df = entry.data_file();
                let sequence_number = Some(entry.sequence_number().unwrap_or(manifest_file.sequence_number));
                let partition_spec_id = Some(manifest_file.partition_spec_id);
                let partition_key = iceberg_partition_key(df.partition());
                match df.content_type() {
                    DataContentType::PositionDeletes => {
                        let (file_format, content_offset, content_size_in_bytes) = match df.file_format() {
                            DataFileFormat::Parquet => (IcebergReadDeleteFormat::Parquet, None, None),
                            DataFileFormat::Puffin => (
                                IcebergReadDeleteFormat::Puffin,
                                Some(df.content_offset().ok_or_else(|| {
                                    format!("Puffin DV {} missing content_offset", df.file_path())
                                })?),
                                Some(df.content_size_in_bytes().ok_or_else(|| {
                                    format!("Puffin DV {} missing content_size_in_bytes", df.file_path())
                                })?),
                            ),
                            other => {
                                return Err(format!(
                                    "unsupported iceberg delete file format {:?}: {}",
                                    other,
                                    df.file_path()
                                ));
                            }
                        };
                        all_deletes.push(IcebergReadDeleteFile {
                            path: df.file_path().to_string(),
                            file_format,
                            kind: IcebergReadDeleteKind::Position,
                            length: Some(i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?),
                            content_offset,
                            content_size_in_bytes,
                            sequence_number,
                            partition_spec_id,
                            partition_key,
                            referenced_data_file: df.referenced_data_file(),
                        });
                    }
                    DataContentType::EqualityDeletes => {
                        if df.file_format() != DataFileFormat::Parquet {
                            return Err(format!(
                                "unsupported iceberg equality-delete file format {:?}: {}",
                                df.file_format(),
                                df.file_path()
                            ));
                        }
                        let equality_field_ids = df.equality_ids().ok_or_else(|| {
                            format!("iceberg equality-delete file {} missing equality_ids", df.file_path())
                        })?;
                        if equality_field_ids.is_empty() {
                            return Err(format!(
                                "iceberg equality-delete file {} has empty equality_ids",
                                df.file_path()
                            ));
                        }
                        all_deletes.push(IcebergReadDeleteFile {
                            path: df.file_path().to_string(),
                            file_format: IcebergReadDeleteFormat::Parquet,
                            kind: IcebergReadDeleteKind::Equality { equality_field_ids },
                            length: Some(i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?),
                            content_offset: None,
                            content_size_in_bytes: None,
                            sequence_number,
                            partition_spec_id,
                            partition_key,
                            referenced_data_file: None,
                        });
                    }
                    DataContentType::Data => {}
                }
            }
        }

        let mut files = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load data manifest {}: {e}", manifest_file.manifest_path))?;
            let mut next_manifest_first_row_id = manifest_file
                .first_row_id
                .map(|v| i64::try_from(v).map_err(|_| format!("manifest first_row_id too large: {v}")))
                .transpose()?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let df = entry.data_file();
                if df.content_type() != DataContentType::Data {
                    continue;
                }
                let record_count = i64::try_from(df.record_count())
                    .map_err(|_| format!("record_count too large for {}", df.file_path()))?;
                let first_row_id = df.first_row_id().or(next_manifest_first_row_id);
                if let Some(next) = next_manifest_first_row_id.as_mut() {
                    *next = next.checked_add(record_count).ok_or_else(|| {
                        format!("first_row_id overflow for manifest {}", manifest_file.manifest_path)
                    })?;
                }
                let mut file = IcebergReadFile {
                    path: df.file_path().to_string(),
                    size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                    record_count: Some(record_count),
                    column_stats: None,
                    partition_spec_id: Some(manifest_file.partition_spec_id),
                    partition_key: iceberg_partition_key(df.partition()),
                    first_row_id,
                    data_sequence_number: Some(entry.sequence_number().unwrap_or(manifest_file.sequence_number)),
                    deletes: Vec::new(),
                };
                attach_applicable_deletes(&mut file, &all_deletes);
                files.push(file);
            }
        }
        Ok(IcebergReadSnapshot {
            snapshot_id: Some(snapshot.snapshot_id()),
            files,
        })
    })
    .map_err(|e| format!("build iceberg read snapshot runtime: {e}"))?
}
```

After this step, restore column stats in the next step instead of leaving them permanently absent.

- [ ] **Step 4: Preserve column stats in `build_read_snapshot`**

In the data-file loop in `build_read_snapshot`, copy the existing stats block from `extract_data_files_with_stats` and set `column_stats` on `IcebergReadFile`. The block must build `field_id_to_name` from `metadata.current_schema()` before the async block, then use `df.null_value_counts()`, `df.column_sizes()`, `df.lower_bounds()`, and `df.upper_bounds()` exactly as the current function does.

Add this code before `let file_io = table.file_io();`:

```rust
let schema = metadata.current_schema();
let field_id_to_name: HashMap<i32, String> = schema
    .as_struct()
    .fields()
    .iter()
    .map(|field| (field.id, field.name.clone()))
    .collect();
```

Use the existing `IcebergColumnStats` construction from `registry.rs` and assign:

```rust
column_stats,
```

inside `IcebergReadFile`.

- [ ] **Step 5: Make `extract_data_files_with_stats` delegate to the read view**

In `src/connector/iceberg/catalog/registry.rs`, replace the body of `extract_data_files_with_stats` with a compatibility conversion.

```rust
pub(crate) fn extract_data_files_with_stats(
    table: &iceberg::table::Table,
) -> Result<Vec<DataFileWithStats>, String> {
    let snapshot = crate::connector::iceberg::read::build_read_snapshot(table)?;
    Ok(snapshot
        .files
        .into_iter()
        .map(|file| DataFileWithStats {
            path: file.path,
            size: file.size,
            record_count: file.record_count,
            column_stats: file.column_stats,
            partition_spec_id: file.partition_spec_id,
            partition_values: None,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files: file
                .deletes
                .into_iter()
                .map(read_delete_to_catalog_delete)
                .collect::<Result<Vec<_>, _>>()?,
        }))
        .collect())
}
```

Add a private conversion helper in `registry.rs`:

```rust
fn read_delete_to_catalog_delete(
    delete_file: crate::connector::iceberg::read::IcebergReadDeleteFile,
) -> Result<crate::sql::catalog::IcebergDeleteFileInfo, String> {
    use crate::connector::iceberg::read::{IcebergReadDeleteFormat, IcebergReadDeleteKind};
    use crate::sql::catalog::{
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    };

    let (file_format, file_content, equality_field_ids) = match delete_file.kind {
        IcebergReadDeleteKind::Position => {
            let format = match delete_file.file_format {
                IcebergReadDeleteFormat::Parquet => IcebergDeleteFileFormat::Parquet,
                IcebergReadDeleteFormat::Puffin => IcebergDeleteFileFormat::Puffin,
            };
            (format, IcebergDeleteFileContent::Position, Vec::new())
        }
        IcebergReadDeleteKind::Equality { equality_field_ids } => {
            if delete_file.file_format != IcebergReadDeleteFormat::Parquet {
                return Err(format!(
                    "equality-delete file {} has non-Parquet format {:?}",
                    delete_file.path, delete_file.file_format
                ));
            }
            (
                IcebergDeleteFileFormat::Parquet,
                IcebergDeleteFileContent::Equality,
                equality_field_ids,
            )
        }
    };

    Ok(IcebergDeleteFileInfo {
        path: delete_file.path,
        file_format,
        file_content,
        length: delete_file.length,
        content_offset: delete_file.content_offset,
        content_size_in_bytes: delete_file.content_size_in_bytes,
        sequence_number: delete_file.sequence_number,
        partition_spec_id: delete_file.partition_spec_id,
        partition_key: delete_file.partition_key,
        equality_column_names: equality_field_ids.into_iter().map(|id| id.to_string()).collect(),
    })
}
```

This keeps the old `equality_column_names` field alive for now; Task 3 replaces it with explicit field IDs.

- [ ] **Step 6: Run focused tests and check**

Run:

```bash
cargo test --lib connector::iceberg::read -- --nocapture
cargo test --lib connector::iceberg::catalog::registry -- --nocapture
cargo check --lib
```

Expected: tests pass and `cargo check --lib` passes.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/read.rs src/connector/iceberg/catalog/registry.rs
git commit -m "refactor: derive iceberg scans from read view"
```

---

### Task 3: Make Delete Metadata Explicit Across Catalog and Scan Ranges

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Modify: `src/connector/iceberg/position_delete.rs`

- [ ] **Step 1: Add explicit equality field IDs to catalog delete metadata**

Modify `IcebergDeleteFileInfo` in `src/sql/catalog.rs`:

```rust
pub struct IcebergDeleteFileInfo {
    pub path: String,
    pub file_format: IcebergDeleteFileFormat,
    pub file_content: IcebergDeleteFileContent,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub equality_column_names: Vec<String>,
    pub equality_field_ids: Vec<i32>,
}
```

Update every `IcebergDeleteFileInfo { ... }` construction to set `equality_field_ids`. For position deletes:

```rust
equality_field_ids: Vec::new(),
```

For equality deletes:

```rust
equality_field_ids: equality_field_ids.clone(),
```

- [ ] **Step 2: Stop string-encoding equality field IDs**

In `read_delete_to_catalog_delete`, replace the equality conversion with:

```rust
equality_column_names: Vec::new(),
equality_field_ids,
```

Keep `current_equality_delete_column_names` unchanged for DDL protection because that function intentionally maps IDs to current column names for user-facing validation.

- [ ] **Step 3: Extend scan range conversion only if needed**

Inspect `src/sql/codegen/nodes.rs::build_hdfs_scan_range_params`. If `TIcebergDeleteFile` has no field for equality IDs, do not add a fake field. The scan runner already opens equality-delete Parquet and reads field IDs from `PARQUET_FIELD_ID_META_KEY`. Add a comment where equality delete ranges are built:

```rust
// Equality field IDs are read from the equality-delete Parquet schema by
// the Rust scan runner. The Thrift scan range only needs to identify the
// delete file as an equality-delete file.
```

- [ ] **Step 4: Update tests that construct `IcebergDeleteFileInfo`**

Use `rg -n "IcebergDeleteFileInfo \\{" src -g '*.rs'` and update each test fixture. Example:

```rust
IcebergDeleteFileInfo {
    path: "s3://bucket/delete.parquet".to_string(),
    file_format: IcebergDeleteFileFormat::Parquet,
    file_content: IcebergDeleteFileContent::Equality,
    length: Some(100),
    content_offset: None,
    content_size_in_bytes: None,
    sequence_number: Some(2),
    partition_spec_id: Some(0),
    partition_key: None,
    equality_column_names: Vec::new(),
    equality_field_ids: vec![1],
}
```

- [ ] **Step 5: Run catalog/codegen tests**

Run:

```bash
cargo test --lib sql::catalog -- --nocapture
cargo test --lib sql::codegen::nodes -- --nocapture
cargo check --lib
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/catalog.rs src/connector/iceberg/catalog/registry.rs src/connector/iceberg/catalog/backend.rs src/sql/codegen/nodes.rs src/connector/iceberg/position_delete.rs
git commit -m "refactor: carry iceberg delete field ids explicitly"
```

---

### Task 4: Use Read View for MV Change Planning

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/engine/query_prep.rs`

- [ ] **Step 1: Add partition metadata to MV incremental file refs**

Modify `IcebergFileForQuery` in `src/engine/query_prep.rs`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct IcebergFileForQuery {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
}
```

In `build_iceberg_table_def_with_files`, map these fields:

```rust
partition_spec_id: file.partition_spec_id,
partition_values: None,
```

Keep `partition_values: None` in this plan. `partition_key` is enough for delete applicability in the current code paths.

- [ ] **Step 2: Update `DataFileRef` for change planning**

Modify `DataFileRef` in `src/connector/iceberg/changes.rs`:

```rust
pub(crate) struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}
```

Update insert collection:

```rust
inserts.push(DataFileRef {
    path: df.file_path().to_string(),
    size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
    record_count: Some(record_count),
    partition_spec_id: Some(manifest_file.partition_spec_id),
    partition_key: iceberg_partition_key(df.partition()),
    first_row_id,
    data_sequence_number: Some(entry.sequence_number().unwrap_or(manifest_file.sequence_number)),
});
```

- [ ] **Step 3: Update insert-side materialization**

In `materialize_changes`, update `IcebergFileForQuery` construction:

```rust
let added_files: Vec<crate::engine::query_prep::IcebergFileForQuery> = batch
    .inserts
    .iter()
    .map(|file| crate::engine::query_prep::IcebergFileForQuery {
        path: file.path.clone(),
        size: file.size,
        record_count: file.record_count,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
    })
    .collect();
```

- [ ] **Step 4: Add a change-planning unit test**

Extend the existing `DataFileRef` construction tests in `src/connector/iceberg/changes.rs` with:

```rust
#[test]
fn data_file_ref_preserves_partition_and_lineage_metadata() {
    let file = DataFileRef {
        path: "s3://bucket/t/data.parquet".to_string(),
        size: 10,
        record_count: Some(2),
        partition_spec_id: Some(4),
        partition_key: Some("city=A".to_string()),
        first_row_id: Some(100),
        data_sequence_number: Some(12),
    };

    assert_eq!(file.partition_spec_id, Some(4));
    assert_eq!(file.partition_key.as_deref(), Some("city=A"));
    assert_eq!(file.first_row_id, Some(100));
    assert_eq!(file.data_sequence_number, Some(12));
}
```

- [ ] **Step 5: Run change tests**

Run:

```bash
cargo test --lib connector::iceberg::changes -- --nocapture
cargo test --lib engine::mv_flow -- --nocapture
cargo check --lib
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/changes.rs src/engine/query_prep.rs
git commit -m "refactor: preserve iceberg read metadata in mv deltas"
```

---

### Task 5: Rework Equality-Delete Reverse Projection to Consume the Read View

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/iceberg/equality_delete.rs`
- Modify: `src/connector/iceberg/read.rs`

- [ ] **Step 1: Add read-view data-file selection helper**

In `src/connector/iceberg/read.rs`, add:

```rust
pub(crate) fn data_files_matching_delete<'a>(
    snapshot: &'a IcebergReadSnapshot,
    delete_file: &IcebergReadDeleteFile,
) -> Vec<&'a IcebergReadFile> {
    snapshot
        .files
        .iter()
        .filter(|data_file| delete_applies_to_data_file(delete_file, data_file))
        .collect()
}
```

Add test:

```rust
#[test]
fn data_files_matching_delete_returns_only_applicable_files() {
    let a = data_file(Some(1), Some(1), Some("city=A"));
    let b = data_file(Some(1), Some(1), Some("city=B"));
    let snapshot = IcebergReadSnapshot {
        snapshot_id: Some(10),
        files: vec![a.clone(), b],
    };
    let delete = equality_delete(Some(2), Some(1), Some("city=A"));

    let files = data_files_matching_delete(&snapshot, &delete);

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, a.path);
}
```

- [ ] **Step 2: Replace `collect_current_data_files_for_equality_delete`**

In `src/connector/iceberg/changes.rs`, remove `collect_current_data_files_for_equality_delete` and use:

```rust
let read_snapshot = crate::connector::iceberg::read::build_read_snapshot(table)?;
```

inside `scan_equality_delete_rows_for_table`.

Convert each `EqualityDeleteRef` to `IcebergReadDeleteFile` before matching:

```rust
let delete_file = crate::connector::iceberg::read::IcebergReadDeleteFile {
    path: delete.delete_file_path.clone(),
    file_format: crate::connector::iceberg::read::IcebergReadDeleteFormat::Parquet,
    kind: crate::connector::iceberg::read::IcebergReadDeleteKind::Equality {
        equality_field_ids: delete.equality_ids.clone(),
    },
    length: Some(delete.delete_file_size),
    content_offset: None,
    content_size_in_bytes: None,
    sequence_number: delete.sequence_number,
    partition_spec_id: delete.partition_spec_id,
    partition_key: delete.partition_key.clone(),
    referenced_data_file: None,
};
```

Loop over `data_files_matching_delete(&read_snapshot, &delete_file)` and pass the data file path/size to `read_data_file_matching_equality_deletes_with_path_normalizer`.

- [ ] **Step 3: Ensure reverse projection does not re-delete already hidden rows**

When scanning a data file for equality-delete reverse projection, pass only the equality-delete set for the delete currently being materialized. Do not pass all live deletes attached to the data file. This keeps the delete-side branch materializing rows affected by the current snapshot delta, not rows hidden by unrelated older delete files.

The loop shape in `scan_equality_delete_rows_for_table` should be:

```rust
for delete in equality_deletes {
    let delete_file = equality_change_to_read_delete(delete);
    let delete_specs = vec![equality_change_to_delete_spec(delete, object_store_config)?];
    let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
        &delete_specs,
        factory,
    )?;
    for data_file in crate::connector::iceberg::read::data_files_matching_delete(&read_snapshot, &delete_file) {
        out.extend(
            crate::connector::iceberg::equality_delete::read_data_file_matching_equality_deletes_with_path_normalizer(
                &data_file.path,
                u64::try_from(data_file.size).ok(),
                &sets,
                factory,
                |path| normalize_delete_projection_path(path, object_store_config).map_err(|e| e.to_string()),
            )?,
        );
    }
}
```

- [ ] **Step 4: Run equality and changes tests**

Run:

```bash
cargo test --lib connector::iceberg::read -- --nocapture
cargo test --lib equality_delete -- --nocapture
cargo test --lib connector::iceberg::changes -- --nocapture
cargo check --lib
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/read.rs src/connector/iceberg/changes.rs src/connector/iceberg/equality_delete.rs
git commit -m "refactor: use iceberg read view for equality delete changes"
```

---

### Task 6: Preserve Field-ID Metadata in MV Delete-Side Temp Files

**Files:**
- Modify: `src/engine/mv_flow.rs`
- Modify: `src/formats/parquet/mod.rs`
- Modify: `src/connector/iceberg/equality_delete.rs`

- [ ] **Step 1: Add a failing test for metadata-preserving temp parquet**

In `src/engine/mv_flow.rs` tests, add a unit that writes a `RecordBatch` whose field has `PARQUET_FIELD_ID_META_KEY`, then reads the temp parquet back through the same reader used by `execute_query_for_mv_incremental_deletes`.

Test skeleton:

```rust
#[test]
fn mv_delete_temp_parquet_preserves_iceberg_field_ids() {
    use std::collections::HashMap;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    let metadata = HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "7".to_string())]);
    let field = Field::new("renamed_id", DataType::Int32, false).with_metadata(metadata);
    let schema = std::sync::Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema,
        vec![std::sync::Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .expect("batch");

    assert_eq!(
        batch.schema().field(0).metadata().get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
        Some("7")
    );
}
```

This first test only proves the source batch carries metadata. In the implementation step, extend it to call the temp-writer helper described below.

- [ ] **Step 2: Extract temp parquet writer helper**

In `src/engine/mv_flow.rs`, extract the writer portion of `execute_query_for_mv_incremental_deletes`:

```rust
fn write_mv_delete_temp_parquet(
    namespace: &str,
    table_name: &str,
    deleted_rows: &[arrow::record_batch::RecordBatch],
) -> Result<(String, i64, Option<i64>), String> {
    let dir = std::env::temp_dir().join(format!(
        "novarocks_mv_deletes_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("create temp dir for delete-side mv refresh: {e}"))?;
    let path = dir.join(format!("{namespace}_{table_name}.parquet"));
    let schema = deleted_rows[0].schema();
    let file = std::fs::File::create(&path)
        .map_err(|e| format!("create temp parquet for delete-side mv refresh: {e}"))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("create temp parquet writer for delete-side mv refresh: {e}"))?;
    for batch in deleted_rows {
        writer
            .write(batch)
            .map_err(|e| format!("write temp parquet batch for delete-side mv refresh: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("close temp parquet writer for delete-side mv refresh: {e}"))?;

    let total_size: i64 = deleted_rows
        .iter()
        .flat_map(|batch| batch.columns())
        .map(|column| column.get_array_memory_size() as i64)
        .sum();
    let total_rows = Some(deleted_rows.iter().map(|batch| batch.num_rows() as i64).sum());
    Ok((format!("file://{}", path.display()), total_size, total_rows))
}
```

Use the helper inside `execute_query_for_mv_incremental_deletes`.

- [ ] **Step 3: Finish the temp parquet metadata test**

Extend the test from Step 1:

```rust
let (path, _, _) = super::write_mv_delete_temp_parquet("ns", "orders", &[batch])
    .expect("write temp parquet");
let local_path = path.strip_prefix("file://").expect("file path");
let file = std::fs::File::open(local_path).expect("open temp parquet");
let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
    .expect("builder");
assert_eq!(
    builder.schema().field(0).metadata().get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
    Some("7")
);
```

- [ ] **Step 4: Run MV flow tests**

Run:

```bash
cargo test --lib engine::mv_flow -- --nocapture
cargo check --lib
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv_flow.rs src/formats/parquet/mod.rs src/connector/iceberg/equality_delete.rs
git commit -m "fix: preserve iceberg field ids in mv delete reads"
```

---

### Task 7: Add Ordinary Iceberg Read Semantics SQL Tests

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_read_semantics_equality_partition.sql`
- Create: `sql-tests/iceberg/result/iceberg_read_semantics_equality_partition.result`
- Create: `sql-tests/iceberg/sql/iceberg_read_semantics_row_lineage_evolution.sql`
- Create: `sql-tests/iceberg/result/iceberg_read_semantics_row_lineage_evolution.result`

- [ ] **Step 1: Add equality + partition evolution SQL case**

Create `sql-tests/iceberg/sql/iceberg_read_semantics_equality_partition.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,read_semantics,equality_delete,partition_evolution
-- Test Point:
--   Validate equality delete visibility when an Iceberg table has partition evolution.
-- Method:
--   Insert rows before and after partition evolution, add an equality delete, and verify only applicable rows are removed.
-- Scope:
--   Ordinary Iceberg SELECT over current snapshot live rows.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG read_sem_eq_part_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/read_sem_eq_part_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE read_sem_eq_part_${uuid0}.ns_${uuid0};
CREATE TABLE read_sem_eq_part_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  city STRING,
  amount BIGINT
) PARTITION BY (city);
INSERT INTO read_sem_eq_part_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'B', 20);
ALTER TABLE read_sem_eq_part_${uuid0}.ns_${uuid0}.orders DROP PARTITION COLUMN city;
INSERT INTO read_sem_eq_part_${uuid0}.ns_${uuid0}.orders VALUES
  (3, 'A', 30),
  (4, 'B', 40);

-- query 2
SELECT id, city, amount
FROM read_sem_eq_part_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 3
-- @skip_result_check=true
ALTER TABLE read_sem_eq_part_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (id) VALUES (3);

-- query 4
SELECT id, city, amount
FROM read_sem_eq_part_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 5
-- @skip_result_check=true
DROP TABLE read_sem_eq_part_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE read_sem_eq_part_${uuid0}.ns_${uuid0};
DROP CATALOG read_sem_eq_part_${uuid0};
```

Expected result file:

```text
-- query 2
1	A	10
2	B	20
3	A	30
4	B	40

-- query 4
1	A	10
2	B	20
4	B	40
```

- [ ] **Step 2: Add row-lineage + schema evolution SQL case**

Create `sql-tests/iceberg/sql/iceberg_read_semantics_row_lineage_evolution.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,read_semantics,row_lineage,schema_evolution
-- Test Point:
--   Validate Iceberg v3 row-lineage metadata columns after schema evolution.
-- Method:
--   Create a v3 row-lineage table, insert rows, rename and widen a column, insert more rows, and query metadata columns.
-- Scope:
--   Ordinary Iceberg SELECT over v3 row-lineage tables.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG read_sem_rl_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/read_sem_rl_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE read_sem_rl_${uuid0}.ns_${uuid0};
CREATE TABLE read_sem_rl_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO read_sem_rl_${uuid0}.ns_${uuid0}.orders VALUES (1, 10.5), (2, 20.5);
ALTER TABLE read_sem_rl_${uuid0}.ns_${uuid0}.orders RENAME COLUMN amount TO total_amount;
ALTER TABLE read_sem_rl_${uuid0}.ns_${uuid0}.orders MODIFY COLUMN total_amount DOUBLE;
INSERT INTO read_sem_rl_${uuid0}.ns_${uuid0}.orders VALUES (3, 30.5);

-- query 2
SELECT id, total_amount, _row_id IS NOT NULL AS has_row_id,
       _last_updated_sequence_number IS NOT NULL AS has_seq
FROM read_sem_rl_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE read_sem_rl_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE read_sem_rl_${uuid0}.ns_${uuid0};
DROP CATALOG read_sem_rl_${uuid0};
```

Expected result file:

```text
-- query 2
1	10.5	true	true
2	20.5	true	true
3	30.5	true	true
```

- [ ] **Step 3: Run the new SQL tests**

Start standalone debug server on a private port following existing project config conventions, then run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_read_semantics_equality_partition,iceberg_read_semantics_row_lineage_evolution \
  --mode verify \
  --query-timeout 180
```

Expected: both cases pass.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_read_semantics_equality_partition.sql \
        sql-tests/iceberg/result/iceberg_read_semantics_equality_partition.result \
        sql-tests/iceberg/sql/iceberg_read_semantics_row_lineage_evolution.sql \
        sql-tests/iceberg/result/iceberg_read_semantics_row_lineage_evolution.result
git commit -m "test: cover iceberg read semantics combinations"
```

---

### Task 8: Add MV-on-Iceberg Change-Read SQL Tests

**Files:**
- Create: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_equality_delete.sql`
- Create: `sql-tests/mv-on-iceberg/result/managed_lake_mv_read_semantics_equality_delete.result`
- Create: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_partition_v3_delete.sql`
- Create: `sql-tests/mv-on-iceberg/result/managed_lake_mv_read_semantics_partition_v3_delete.result`

- [ ] **Step 1: Add MV equality-delete type-widening case**

Create `sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_equality_delete.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=managed_lake,mv,iceberg,ivm,equality_delete,schema_evolution,read_semantics
-- Test Point:
--   Validate projection MV incremental refresh when an Iceberg equality delete targets a widened base column.
-- Method:
--   Create a primary-key projection MV, widen the equality column, add equality delete, refresh MV, and verify retract rows.
-- Scope:
--   Managed-lake projection/filter MV over an Iceberg base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_read_sem_eq_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_read_sem_eq_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_read_sem_eq_${uuid0}.ns_${uuid0};
CREATE TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  amount FLOAT,
  customer STRING
);
INSERT INTO mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10.5, 'A'),
  (2, 20.5, 'B'),
  (3, 30.5, 'C');
CREATE MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, amount, customer
FROM mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10.0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;

-- query 3
SELECT id, amount, customer
FROM ${case_db}.mv_read_sem_eq_orders
ORDER BY id;

-- query 4
-- @skip_result_check=true
ALTER TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders MODIFY COLUMN amount DOUBLE;
ALTER TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (amount) VALUES (20.5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;

-- query 6
SELECT id, amount, customer
FROM ${case_db}.mv_read_sem_eq_orders
ORDER BY id;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;
DROP TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_read_sem_eq_${uuid0}.ns_${uuid0};
DROP CATALOG mv_read_sem_eq_${uuid0};
```

Expected result file:

```text
-- query 3
1	10.5	A
2	20.5	B
3	30.5	C

-- query 6
1	10.5	A
3	30.5	C
```

- [ ] **Step 2: Add MV v3 partition evolution + DV delete case**

Create `sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_partition_v3_delete.sql` by extending the existing `managed_lake_mv_projection_v3_delete.sql` pattern with partition evolution before the delete:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=managed_lake,mv,iceberg,ivm,projection_filter,row_lineage,delete,partition_evolution,read_semantics
-- Test Point:
--   Validate projection MV incremental refresh over Iceberg v3 row-lineage DV deletes after partition evolution.
-- Method:
--   Create a partitioned row-lineage table, evolve the partition spec, delete one old-spec row and one new-spec row, refresh MV, and verify both rows retract.
-- Scope:
--   Managed-lake projection/filter MV on an Iceberg v3 row-lineage base table with evolved partition specs.
```

Use the statement body from `managed_lake_mv_projection_v3_delete.sql`, adding:

```sql
CREATE TABLE ... PARTITION BY (customer) TBLPROPERTIES (...);
INSERT INTO ... VALUES (1, 'A', 10), (2, 'A', 20);
ALTER TABLE ... DROP PARTITION COLUMN customer;
INSERT INTO ... VALUES (3, 'B', 30), (4, 'B', 40);
DELETE FROM ... WHERE id IN (1, 3);
```

Expected final MV result:

```text
-- final query
2	A	20
4	B	40
```

- [ ] **Step 3: Run MV SQL tests**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --only managed_lake_mv_read_semantics_equality_delete,managed_lake_mv_read_semantics_partition_v3_delete \
  --mode verify \
  --query-timeout 240
```

Expected: both cases pass.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_equality_delete.sql \
        sql-tests/mv-on-iceberg/result/managed_lake_mv_read_semantics_equality_delete.result \
        sql-tests/mv-on-iceberg/sql/managed_lake_mv_read_semantics_partition_v3_delete.sql \
        sql-tests/mv-on-iceberg/result/managed_lake_mv_read_semantics_partition_v3_delete.result
git commit -m "test: cover mv on iceberg read semantics"
```

---

### Task 9: Final Verification and Cleanup

**Files:**
- Modify only files touched by Tasks 1-8 if verification finds issues.

- [ ] **Step 1: Run focused Rust tests**

Run:

```bash
cargo test --lib connector::iceberg::read -- --nocapture
cargo test --lib equality_delete -- --nocapture
cargo test --lib connector::iceberg::changes -- --nocapture
cargo test --lib connector::iceberg::scan_deletes -- --nocapture
cargo test --lib engine::mv_flow -- --nocapture
cargo check --lib
```

Expected: all commands pass.

- [ ] **Step 2: Run SQL suites with MinIO**

Run ordinary Iceberg read cases:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_equality_delete_schema_evolution,iceberg_partition_evolution_delete,iceberg_partition_evolution_v3_delete,iceberg_read_semantics_equality_partition,iceberg_read_semantics_row_lineage_evolution \
  --mode verify \
  --query-timeout 240
```

Run MV-on-Iceberg cases:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg \
  --only managed_lake_mv_equality_delete,managed_lake_mv_projection_delete,managed_lake_mv_projection_v3_delete,managed_lake_mv_read_semantics_equality_delete,managed_lake_mv_read_semantics_partition_v3_delete \
  --mode verify \
  --query-timeout 300
```

Expected: both commands pass.

- [ ] **Step 3: Run formatting checks**

Run:

```bash
cargo fmt --check
git diff --check
git status --short
```

Expected:

- `cargo fmt --check` passes.
- `git diff --check` prints no output.
- `git status --short` only shows intentional files if the last task has not been committed.

- [ ] **Step 4: Commit any verification fixes**

If Step 1 or Step 2 required code/test adjustments:

```bash
git add <fixed-files>
git commit -m "fix: complete iceberg read semantics verification"
```

If no adjustments were needed, do not create an empty commit.

---

## Plan Self-Review

- Spec coverage:
  - Ordinary `SELECT` read semantics are covered by Tasks 1-3 and Task 7.
  - MV/IVM change-read semantics are covered by Tasks 4-6 and Task 8.
  - Fail-fast and unsupported behavior are covered in Tasks 1, 2, 5, and 8.
  - MinIO SQL verification is covered in Tasks 7-9.
- Placeholder scan:
  - The plan contains no placeholder markers.
  - The plan contains no open-ended "add tests" step without concrete test names or SQL bodies.
- Type consistency:
  - `IcebergReadFile`, `IcebergReadDeleteFile`, and `IcebergReadSnapshot` are defined in Task 1 and reused in Tasks 2 and 5.
  - `IcebergFileForQuery` is extended in Task 4 before any task relies on the new fields.
  - `IcebergDeleteFileInfo.equality_field_ids` is introduced in Task 3 before conversion helpers use it.
