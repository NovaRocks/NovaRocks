# Iceberg Standard Overwrite Diff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove NovaRocks private Iceberg COW UPDATE sidecar from the correctness path and drive MV incremental refresh from standard Iceberg snapshot / manifest / manifest-list diff.

**Architecture:** COW UPDATE writes only standard `operation=overwrite` metadata: old data files as `DELETED` manifest entries and replacement data files as `ADDED` entries. `changes.rs` treats every overwrite snapshot as a standard diff, materializes deleted data files as delete rows, and lets existing MV apply policy choose incremental or full refresh. NovaRocks update markers, sidecar JSON, `cow_updates`, and `mor_updates` are removed from change planning.

**Tech Stack:** Rust, iceberg-rust 0.9 vendor APIs, Arrow `RecordBatch`, Parquet, NovaRocks standalone managed-lake MV refresh, cargo unit/integration tests.

---

## File Structure

- `src/connector/iceberg/changes.rs`
  - Owns snapshot lineage classification, manifest diff collection, and materialization of insert/delete branches.
  - Add `DeletedDataFileRef`, `LineageAction::CollectOverwriteDiff`, `collect_deleted_data_files_for_manifest_list`, and `scan_deleted_data_file_rows`.
  - Remove `CowUpdateRef`, `MorUpdateRef`, sidecar readers, and marker-based classification.

- `src/connector/iceberg/commit/types.rs`
  - Keep generic commit kinds, write modes, and `WrittenFile`.
  - Remove persisted sidecar constants and sidecar structs.

- `src/connector/iceberg/commit/run.rs`
  - Rename `RunInput.cow_update_sidecar` to `cow_update_rewrite`.
  - Dispatch `CommitOpKind::CowUpdate` with the in-process rewrite set.

- `src/connector/iceberg/commit/update_cow.rs`
  - Keep the COW update commit action and manifest rewrite logic.
  - Replace `MutationSidecar` with `CowUpdateRewriteSet` / `CowUpdateTouchedFile`.
  - Stop writing sidecar JSON and stop writing NovaRocks snapshot summary markers.

- `src/engine/mutation_flow.rs`
  - Build the in-process COW rewrite set from matched rows and replacement files.
  - Pass it to `run_iceberg_commit`; do not serialize it.

- `src/connector/starrocks/managed/ivm_change_stream.rs`
  - Update `IcebergChangeBatch` construction and tests for `deleted_data_files`.

- `src/connector/starrocks/managed/mv_refresh.rs`
  - Count `deleted_data_files` as delete-bearing change.
  - Add the end-to-end COW UPDATE MV refresh regression.

- `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
  - Treat delete-bearing overwrite diff as a full refresh path for Iceberg-backed MV, because its current incremental writer is insert-only.

- `docs/design/specs/2026-05-09-iceberg-standard-overwrite-diff-design.md`
  - Reference spec. Do not edit unless implementation reveals a spec bug.

---

### Task 1: Change Planner Data Model and Classification

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

- [ ] **Step 1: Add failing classification tests**

Append these tests inside `changes.rs` `#[cfg(test)] mod tests` near the existing `classify_snapshot_*` tests:

```rust
#[test]
fn classify_snapshot_overwrite_emits_collect_overwrite_diff() {
    let s = snap(7, Some(6), Operation::Overwrite, &[], 0);

    assert_eq!(
        classify_snapshot(&s, None).expect("classify overwrite"),
        Some(LineageAction::CollectOverwriteDiff { snapshot_id: 7 })
    );
}

#[test]
fn classify_snapshot_ignores_novarocks_update_markers() {
    let s = snap(
        7,
        Some(6),
        Operation::Overwrite,
        &[
            ("novarocks.row-level-op", "update"),
            ("novarocks.update.mode", "copy-on-write"),
            ("novarocks.update.sidecar", "file:///tmp/obsolete-sidecar.json"),
        ],
        0,
    );

    assert_eq!(
        classify_snapshot(&s, None).expect("classify overwrite"),
        Some(LineageAction::CollectOverwriteDiff { snapshot_id: 7 })
    );
}
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::classify_snapshot_overwrite_emits_collect_overwrite_diff connector::iceberg::changes::tests::classify_snapshot_ignores_novarocks_update_markers
```

Expected: FAIL. The first failure should mention that `LineageAction::CollectOverwriteDiff` does not exist, or that ordinary overwrite still returns `UnsupportedOperation`.

- [ ] **Step 3: Implement the planner model changes**

In `changes.rs`, add the deleted data-file reference next to `DataFileRef`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeletedDataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}
```

Change `IcebergChangeBatch` to include deleted data files and remove marker vectors:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
    pub equality_deletes: Vec<EqualityDeleteRef>,
    pub deleted_data_files: Vec<DeletedDataFileRef>,
}
```

Replace the marker-based lineage actions with a standard overwrite action:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LineageAction {
    CollectInserts { snapshot_id: i64 },
    CollectDeletes { snapshot_id: i64 },
    CollectOverwriteDiff { snapshot_id: i64 },
}
```

Change `classify_snapshot` to:

```rust
fn classify_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: Option<&iceberg::spec::Snapshot>,
) -> Result<Option<LineageAction>, ChangeError> {
    use iceberg::spec::Operation;
    let snapshot_id = snapshot.snapshot_id();
    match &snapshot.summary().operation {
        Operation::Append => Ok(Some(LineageAction::CollectInserts { snapshot_id })),
        Operation::Delete => Ok(Some(LineageAction::CollectDeletes { snapshot_id })),
        Operation::Replace => {
            let parent = parent.ok_or_else(|| ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason: "REPLACE snapshot has no parent reachable for compaction validation"
                    .to_string(),
            })?;
            validate_replace_snapshot(snapshot, parent)?;
            Ok(None)
        }
        Operation::Overwrite => Ok(Some(LineageAction::CollectOverwriteDiff { snapshot_id })),
    }
}
```

Remove `update_marker_mode`, `CowUpdateRef`, and `MorUpdateRef` from `changes.rs`.

- [ ] **Step 4: Update empty batch constructors**

Every literal that starts with `IcebergChangeBatch {` must include:

```rust
deleted_data_files: Vec::new(),
```

Remove these fields from every literal:

```rust
cow_updates: Vec::new(),
mor_updates: Vec::new(),
```

- [ ] **Step 5: Run classification tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::classify_snapshot_overwrite_emits_collect_overwrite_diff connector::iceberg::changes::tests::classify_snapshot_ignores_novarocks_update_markers
```

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "refactor(iceberg): classify overwrite snapshots by standard diff"
```

---

### Task 2: Collect Deleted Data Files from Overwrite Manifests

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

- [ ] **Step 1: Add failing overwrite diff planning test**

Add this test in `changes.rs` after `plan_changes_collects_inserts_after_previous_snapshot`:

```rust
#[test]
fn plan_changes_collects_overwrite_added_and_deleted_data_files() {
    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::{
        CommitCtx, CommitOpKind, IcebergCommitAction, IcebergCommitCollector, OverwriteCommit,
        WrittenFile,
    };
    use iceberg::spec::{DataContentType, DataFileFormat, Struct};
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("file://{}", dir.path().join("warehouse").display());
    let entry = test_hadoop_catalog_entry("ice", &warehouse);
    create_namespace(&entry, "ns").expect("namespace");
    create_table(
        &entry,
        "ns",
        "orders",
        &[TableColumnDef {
            name: "k1".to_string(),
            data_type: SqlType::Int,
            nullable: true,
            aggregation: None,
            default: None,
        }],
        None,
        &[],
        &[],
    )
    .expect("table");
    insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]])
        .expect("first insert");

    let loaded = load_table(&entry, "ns", "orders").expect("load first");
    let previous = loaded
        .table
        .metadata()
        .current_snapshot()
        .expect("snapshot")
        .snapshot_id();
    let metadata = loaded.table.metadata();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::Overwrite,
        loaded.table.identifier().clone(),
        Some(previous),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        format!("{}/data/_staging/test-overwrite", metadata.location()),
        UniqueId { hi: 0, lo: 0 },
    ));
    collector.inject_written_file(WrittenFile {
        path: format!("{}/data/replacement.parquet", metadata.location()),
        format: DataFileFormat::Parquet,
        content: DataContentType::Data,
        partition_values: Struct::empty(),
        partition_spec_id: metadata.default_partition_spec_id(),
        record_count: 2,
        file_size_in_bytes: 2048,
        split_offsets: vec![],
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        key_metadata: None,
        referenced_data_file: None,
        equality_ids: None,
        first_row_id: None,
    });
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
        .expect("catalog");
    crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        let file_io = loaded.table.file_io().clone();
        let ctx = CommitCtx {
            collector: &collector,
            table: &loaded.table,
            catalog: catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle: collector.abort_log.clone(),
            target_ref: "main",
        };
        OverwriteCommit.commit(ctx).await
    })
    .expect("runtime")
    .expect("overwrite commit");

    entry.invalidate_table_cache("ns", "orders");
    let loaded = load_table(&entry, "ns", "orders").expect("load overwritten");
    let batch = plan_changes(&loaded.table, previous, &[]).expect("plan overwrite diff");

    assert_eq!(batch.previous_snapshot_id, previous);
    assert_eq!(batch.inserts.len(), 1, "replacement file must be insert delta");
    assert_eq!(
        batch.deleted_data_files.len(),
        1,
        "old file must be delete delta"
    );
    assert!(batch.deletes.is_empty());
    assert!(batch.equality_deletes.is_empty());
}
```

- [ ] **Step 2: Run the new planning test and confirm it fails**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::plan_changes_collects_overwrite_added_and_deleted_data_files
```

Expected: FAIL. The failure should mention `deleted_data_files` collection is empty, or the collector tuple does not include deleted data files.

- [ ] **Step 3: Implement deleted data-file collection**

Change `collect_files` to return deleted data files:

```rust
async fn collect_files(
    metadata: &iceberg::spec::TableMetadata,
    file_io: &iceberg::io::FileIO,
    actions: &[LineageAction],
) -> Result<
    (
        Vec<DataFileRef>,
        Vec<PositionDeleteRef>,
        Vec<EqualityDeleteRef>,
        Vec<DeletedDataFileRef>,
    ),
    ChangeError,
> {
    let mut inserts = Vec::new();
    let mut deletes = Vec::new();
    let mut equality_deletes = Vec::new();
    let mut deleted_data_files = Vec::new();

    for action in actions {
        let snapshot_id = match action {
            LineageAction::CollectInserts { snapshot_id }
            | LineageAction::CollectDeletes { snapshot_id }
            | LineageAction::CollectOverwriteDiff { snapshot_id } => *snapshot_id,
        };
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "collect_files: snapshot {snapshot_id} no longer in metadata"
            ))
        })?;
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "load manifest list for snapshot {snapshot_id}: {e}"
                ))
            })?;

        match action {
            LineageAction::CollectInserts { .. } => {
                collect_added_data_files_for_manifest_list(
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut inserts,
                )
                .await?;
            }
            LineageAction::CollectDeletes { .. } => {
                collect_added_delete_files_for_manifest_list(
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deletes,
                    &mut equality_deletes,
                )
                .await?;
            }
            LineageAction::CollectOverwriteDiff { .. } => {
                collect_added_data_files_for_manifest_list(
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut inserts,
                )
                .await?;
                collect_deleted_data_files_for_manifest_list(
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deleted_data_files,
                )
                .await?;
            }
        }
    }

    Ok((inserts, deletes, equality_deletes, deleted_data_files))
}
```

Add the deleted-data collector below `collect_added_data_files_for_manifest_list`:

```rust
async fn collect_deleted_data_files_for_manifest_list(
    snapshot_id: i64,
    file_io: &iceberg::io::FileIO,
    manifest_list: &iceberg::spec::ManifestList,
    deleted_data_files: &mut Vec<DeletedDataFileRef>,
) -> Result<(), ChangeError> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        if !manifest_file.has_deleted_files() {
            continue;
        }
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "load data manifest {} for deleted entries in snapshot {snapshot_id}: {e}",
                manifest_file.manifest_path
            ))
        })?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Deleted {
                continue;
            }
            if entry.snapshot_id() != Some(snapshot_id) {
                continue;
            }
            let df = entry.data_file();
            if df.content_type() != DataContentType::Data {
                continue;
            }
            deleted_data_files.push(DeletedDataFileRef {
                path: df.file_path().to_string(),
                size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(i64::try_from(df.record_count()).unwrap_or(i64::MAX)),
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: iceberg_partition_key(df.partition()),
                first_row_id: df.first_row_id(),
                data_sequence_number: Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                ),
            });
        }
    }
    Ok(())
}
```

Update `plan_changes` destructuring and result construction:

```rust
let (inserts, deletes, equality_deletes, deleted_data_files) =
    crate::connector::iceberg::catalog::registry::block_on_iceberg(collect)
        .map_err(|e| ChangeError::InternalInconsistency(format!("plan_changes runtime: {e}")))??;

Ok(IcebergChangeBatch {
    previous_snapshot_id,
    current_snapshot_id,
    inserts,
    deletes,
    equality_deletes,
    deleted_data_files,
})
```

- [ ] **Step 4: Run deleted-data planning tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::plan_changes_collects_overwrite_added_and_deleted_data_files connector::iceberg::changes::tests::plan_changes_collects_inserts_after_previous_snapshot
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "feat(iceberg): collect deleted data files from overwrite snapshots"
```

---

### Task 3: Materialize Deleted Data Files as Delete Rows

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

- [ ] **Step 1: Add a failing deleted-data scan test**

Add this test in `changes.rs` tests near the delete projection tests:

```rust
#[test]
fn scan_deleted_data_file_rows_reads_full_parquet_file() {
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let parquet_path = dir.path().join("deleted.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
    )
    .expect("batch");
    let file = std::fs::File::create(&parquet_path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write parquet");
    writer.close().expect("close parquet");

    let refs = vec![DeletedDataFileRef {
        path: format!("file://{}", parquet_path.display()),
        size: std::fs::metadata(&parquet_path).expect("stat").len() as i64,
        record_count: Some(2),
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
    }];
    let factory = crate::fs::opendal::OpendalRangeReaderFactory::from_operator(
        crate::fs::opendal::build_fs_operator("/").expect("local operator"),
    )
    .expect("factory");

    let rows = scan_deleted_data_file_rows(&refs, &factory, None).expect("scan");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].num_rows(), 2);
}
```

- [ ] **Step 2: Run the deleted-data scan test and confirm it fails**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::scan_deleted_data_file_rows_reads_full_parquet_file
```

Expected: FAIL because `scan_deleted_data_file_rows` is not defined.

- [ ] **Step 3: Implement deleted data-file scanning**

Rename `read_full_data_file` to keep its purpose generic:

```rust
fn read_full_data_file_for_delete_projection(
    path: &str,
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use crate::cache::CachedRangeReader;
    use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = factory
        .open_with_len(path, None)
        .map_err(|e| format!("open data file {path} for delete projection: {e}"))?;
    let reader = ParquetCachedReader::new(
        CachedRangeReader::new(reader, None),
        ParquetReadCachePolicy::with_flags(false, false, None),
    );
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|e| format!("read parquet metadata for {path}: {e}"))?;
    let reader = builder
        .build()
        .map_err(|e| format!("build parquet reader for {path}: {e}"))?;
    let mut out = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("read parquet batch for {path}: {e}"))?;
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}
```

Add the deleted-data scan helper:

```rust
fn scan_deleted_data_file_rows(
    deleted_data_files: &[DeletedDataFileRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let mut out = Vec::new();
    for file in deleted_data_files {
        let normalized = normalize_delete_projection_path(&file.path, object_store_config)
            .map_err(|e| format!("normalize deleted data file `{}`: {e}", file.path))?;
        let batches = read_full_data_file_for_delete_projection(&normalized, factory)
            .map_err(|e| format!("read deleted data file `{}`: {e}", file.path))?;
        out.extend(batches);
    }
    Ok(out)
}
```

- [ ] **Step 4: Wire deleted data files into `materialize_changes`**

Update delete-branch detection:

```rust
let needs_deletes_scan = !batch.deletes.is_empty()
    || !batch.equality_deletes.is_empty()
    || !batch.deleted_data_files.is_empty();
```

After equality-delete rows are appended, append deleted data-file rows:

```rust
if !batch.deleted_data_files.is_empty() {
    deleted_rows.extend(scan_deleted_data_file_rows(
        &batch.deleted_data_files,
        &factory,
        object_store_config,
    )?);
}
```

Remove `read_mutation_sidecar` and `scan_cow_update_old_rows`.

- [ ] **Step 5: Run materialization tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes::tests::scan_deleted_data_file_rows_reads_full_parquet_file connector::iceberg::changes::tests::normalize_delete_projection_path_uses_object_store_config_for_s3_uri
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "feat(iceberg): materialize overwrite deleted data files"
```

---

### Task 4: Update MV Refresh Policy for Deleted Data Files

**Files:**
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`

- [ ] **Step 1: Update `ivm_change_stream.rs` test literals**

In `ivm_change_stream.rs`, change every `IcebergChangeBatch` literal to include:

```rust
deleted_data_files: Vec::new(),
```

Remove any stale `cow_updates` and `mor_updates` fields from those literals.

- [ ] **Step 2: Add failing MV policy regression in `mv_refresh.rs`**

Add this unit test near `overwrite_change_error_routes_projection_mv_to_full_refresh_policy`:

```rust
#[test]
fn deleted_data_files_make_change_batch_delete_bearing() {
    let batch = crate::connector::iceberg::changes::IcebergChangeBatch {
        previous_snapshot_id: 10,
        current_snapshot_id: 11,
        inserts: Vec::new(),
        deletes: Vec::new(),
        equality_deletes: Vec::new(),
        deleted_data_files: vec![crate::connector::iceberg::changes::DeletedDataFileRef {
            path: "file:///tmp/old.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
        }],
    };

    assert!(change_batch_has_deletes(&batch));
}
```

- [ ] **Step 3: Run the new MV policy test and confirm it fails**

Run:

```bash
cargo test -p novarocks connector::starrocks::managed::mv_refresh::tests::deleted_data_files_make_change_batch_delete_bearing
```

Expected: FAIL because `change_batch_has_deletes` is not defined.

- [ ] **Step 4: Add a small helper and use it in both projection and aggregate paths**

In `mv_refresh.rs`, add this helper near `chunks_row_count`:

```rust
fn change_batch_has_deletes(batch: &crate::connector::iceberg::changes::IcebergChangeBatch) -> bool {
    !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
}
```

Replace the projection incremental local:

```rust
let has_deletes = change_batch_has_deletes(&batch);
```

Before calling `refresh_aggregate_mv_incremental`, compute apply policy from the same helper only when the code needs an early policy decision. Keep the existing in-function policy in `refresh_aggregate_mv_incremental`, because it checks materialized row counts after SQL projection.

- [ ] **Step 5: Make Iceberg-backed MV fallback on delete-bearing changes**

In `mv_refresh_iceberg.rs`, replace the current delete rejection block in `incremental_refresh_iceberg_mv` with:

```rust
if !batch.deletes.is_empty()
    || !batch.equality_deletes.is_empty()
    || !batch.deleted_data_files.is_empty()
{
    tracing::info!(
        "iceberg mv {db_name}.{mv_name}: change batch contains delete-bearing deltas; rebuilding with overwrite"
    );
    return rebuild_iceberg_mv(
        state,
        cfg,
        metadata_store,
        db_name,
        mv_name,
        mv_row,
        base_ref,
        Some(current_snapshot_id),
        current_table_uuid,
    );
}
```

- [ ] **Step 6: Run MV policy tests**

Run:

```bash
cargo test -p novarocks connector::starrocks::managed::ivm_change_stream connector::starrocks::managed::mv_refresh::tests::deleted_data_files_make_change_batch_delete_bearing
```

Expected: PASS.

- [ ] **Step 7: Commit Task 4**

```bash
git add src/connector/starrocks/managed/ivm_change_stream.rs src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat(mv): treat overwrite deleted data files as delete deltas"
```

---

### Task 5: Remove Persistent COW UPDATE Sidecar Writes

**Files:**
- Modify: `src/connector/iceberg/commit/types.rs`
- Modify: `src/connector/iceberg/commit/run.rs`
- Modify: `src/connector/iceberg/commit/update_cow.rs`
- Modify: `src/engine/mutation_flow.rs`
- Modify callers that construct `RunInput`

- [ ] **Step 1: Add failing COW summary test**

In `update_cow.rs` tests, add:

```rust
#[test]
fn cow_update_summary_uses_standard_overwrite_counters_only() {
    let added = vec![written_file("new.parquet")];
    let deleted = vec![(
        iceberg::spec::DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1024)
            .build()
            .expect("data file"),
        1,
        Some(1),
    )];

    let summary = cow_update_summary(&added, &deleted);

    assert_eq!(summary.get("added-data-files").map(String::as_str), Some("1"));
    assert_eq!(summary.get("deleted-data-files").map(String::as_str), Some("1"));
    assert!(!summary.contains_key("novarocks.row-level-op"));
    assert!(!summary.contains_key("novarocks.update.mode"));
    assert!(!summary.contains_key("novarocks.update.sidecar"));
}
```

- [ ] **Step 2: Run the COW summary test and confirm it fails**

Run:

```bash
cargo test -p novarocks connector::iceberg::commit::update_cow::tests::cow_update_summary_uses_standard_overwrite_counters_only
```

Expected: FAIL because `cow_update_summary` currently takes a sidecar path and emits NovaRocks marker keys.

- [ ] **Step 3: Replace persisted sidecar types with in-process rewrite types**

In `update_cow.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CowUpdateRewriteSet {
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<CowUpdateTouchedFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CowUpdateTouchedFile {
    pub old_file: String,
    pub new_files: Vec<String>,
    pub row_ids: Vec<i64>,
}

impl CowUpdateRewriteSet {
    pub(crate) fn new(
        base_snapshot_id: i64,
        target_table_uuid: String,
        updated_row_ids: Vec<i64>,
        touched_data_files: Vec<CowUpdateTouchedFile>,
    ) -> Self {
        Self {
            base_snapshot_id,
            target_table_uuid,
            updated_row_ids,
            touched_data_files,
        }
    }
}
```

Change `CowUpdateCommit`:

```rust
pub struct CowUpdateCommit {
    pub rewrite: CowUpdateRewriteSet,
}
```

Rename validation helpers:

```rust
fn validate_cow_update_inputs(
    rewrite: &CowUpdateRewriteSet,
    written: &[WrittenFile],
    parent_snapshot_id: Option<i64>,
    table_uuid: &str,
) -> Result<(), String> {
    let parent_snapshot_id = parent_snapshot_id
        .ok_or_else(|| "CowUpdateCommit requires a current snapshot".to_string())?;
    if rewrite.base_snapshot_id != parent_snapshot_id {
        return Err(format!(
            "CowUpdateCommit rewrite base snapshot {} does not match current snapshot {}",
            rewrite.base_snapshot_id, parent_snapshot_id
        ));
    }
    if rewrite.target_table_uuid != table_uuid {
        return Err(format!(
            "CowUpdateCommit rewrite target table UUID {} does not match current table UUID {}",
            rewrite.target_table_uuid, table_uuid
        ));
    }
    if rewrite.touched_data_files.is_empty() || written.is_empty() {
        return Err(
            "CowUpdateCommit requires touched data files and replacement data files".to_string(),
        );
    }
    if rewrite.updated_row_ids.is_empty() {
        return Err("CowUpdateCommit updated_row_ids must not be empty".to_string());
    }

    let mut updated_row_ids = std::collections::HashSet::new();
    for row_id in &rewrite.updated_row_ids {
        if !updated_row_ids.insert(*row_id) {
            return Err(format!("CowUpdateCommit contains duplicate updated row id {row_id}"));
        }
    }

    let mut old_files = std::collections::HashSet::new();
    let mut rewrite_row_ids = std::collections::HashSet::new();
    let mut rewrite_new_files = std::collections::HashSet::new();
    for file in &rewrite.touched_data_files {
        if !old_files.insert(file.old_file.clone()) {
            return Err(format!(
                "CowUpdateCommit contains duplicate touched data file {}",
                file.old_file
            ));
        }
        if file.row_ids.is_empty() {
            return Err(format!(
                "CowUpdateCommit touched data file {} has no row ids",
                file.old_file
            ));
        }
        if file.new_files.is_empty() {
            return Err(format!(
                "CowUpdateCommit touched data file {} has no replacement data files",
                file.old_file
            ));
        }
        for row_id in &file.row_ids {
            if !rewrite_row_ids.insert(*row_id) {
                return Err(format!("CowUpdateCommit contains duplicate touched row id {row_id}"));
            }
        }
        for new_file in &file.new_files {
            if !rewrite_new_files.insert(new_file.clone()) {
                return Err(format!(
                    "CowUpdateCommit contains duplicate replacement data file {new_file}"
                ));
            }
        }
    }

    for row_id in updated_row_ids.difference(&rewrite_row_ids) {
        return Err(format!(
            "CowUpdateCommit updated_row_ids contains row id {row_id}, but touched files are missing touched row id {row_id}"
        ));
    }
    let written_files: std::collections::HashSet<String> =
        written.iter().map(|f| f.path.clone()).collect();
    if written_files.len() != written.len() {
        return Err("CowUpdateCommit received duplicate replacement data file paths".to_string());
    }
    for new_file in &rewrite_new_files {
        if !written_files.contains(new_file) {
            return Err(format!(
                "CowUpdateCommit replacement data file {new_file} was not written"
            ));
        }
    }
    for written_file in &written_files {
        if !rewrite_new_files.contains(written_file) {
            return Err(format!(
                "CowUpdateCommit written data file {written_file} is missing from rewrite set"
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Stop writing sidecar files and marker summary**

In `CowUpdateTxnAction::commit`, remove:

```rust
let sidecar_path = format!("{metadata_dir}/{}-update-sidecar.json", self.commit_uuid);
self.abort_handle.record_manifest(sidecar_path.clone());
write_mutation_sidecar(&self.file_io, &sidecar_path, &self.sidecar)
    .await
    .map_err(to_iceberg_unexpected)?;
```

Replace `cow_update_summary` with:

```rust
fn cow_update_summary(
    added: &[WrittenFile],
    deleted: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("added-data-files".to_string(), added.len().to_string());
    p.insert(
        "added-records".to_string(),
        added.iter().map(|f| f.record_count).sum::<u64>().to_string(),
    );
    p.insert(
        "added-files-size".to_string(),
        added
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>()
            .to_string(),
    );
    p.insert("deleted-data-files".to_string(), deleted.len().to_string());
    p.insert(
        "deleted-records".to_string(),
        deleted
            .iter()
            .map(|(df, _, _)| df.record_count())
            .sum::<u64>()
            .to_string(),
    );
    p
}
```

Before building `Summary`, collect the deleted entries once:

```rust
let deleted_entries = live_files_as_delete_entries(&index.touched_live);
```

Use it for delete manifests and summary:

```rust
let summary = Summary {
    operation: Operation::Overwrite,
    additional_properties: cow_update_summary(&self.written, &deleted_entries),
};
```

- [ ] **Step 5: Update `RunInput` and COW dispatch**

In `run.rs`, change imports:

```rust
use super::types::{CommitOpKind, CommitOutcome};
use super::update_cow::{CowUpdateCommit, CowUpdateRewriteSet};
```

Change `RunInput`:

```rust
pub cow_update_rewrite: Option<CowUpdateRewriteSet>,
```

Change destructuring and dispatch:

```rust
cow_update_rewrite,
```

```rust
CommitOpKind::CowUpdate => Box::new(CowUpdateCommit {
    rewrite: cow_update_rewrite
        .ok_or_else(|| "CowUpdate commit requires a rewrite set".to_string())?,
}),
```

Update every literal that starts with `RunInput {`:

```rust
cow_update_rewrite: None,
```

The COW UPDATE path uses:

```rust
cow_update_rewrite: Some(rewrite_set),
```

- [ ] **Step 6: Update `mutation_flow.rs` to build rewrite sets**

Change imports:

```rust
use crate::connector::iceberg::commit::update_cow::{
    CowUpdateRewriteSet, CowUpdateTouchedFile,
};
```

Change `write_cow_update_files` signature:

```rust
async fn write_cow_update_files(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    target_ref: &str,
) -> Result<(Vec<iceberg::spec::DataFile>, CowUpdateRewriteSet), String>
```

Rename `build_cow_sidecar` to `build_cow_rewrite_set` and return:

```rust
Ok(CowUpdateRewriteSet::new(
    base_snapshot_id,
    table.metadata().uuid().to_string(),
    matched.row_ids.clone(),
    touched_data_files,
))
```

Change pushed touched file construction:

```rust
touched_data_files.push(CowUpdateTouchedFile {
    old_file,
    new_files: new_files.clone(),
    row_ids: row_ids.clone(),
});
```

Delete `empty_sidecar`. The empty update path already returns early before commit:

```rust
if matched.row_ids.is_empty() {
    let base_snapshot_id = if target_ref == "main" {
        table.metadata().current_snapshot().map(|s| s.snapshot_id()).unwrap_or(0)
    } else {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
            .unwrap_or(0)
    };
    return Ok((
        Vec::new(),
        CowUpdateRewriteSet::new(
            base_snapshot_id,
            table.metadata().uuid().to_string(),
            Vec::new(),
            Vec::new(),
        ),
    ));
}
```

- [ ] **Step 7: Remove sidecar public API**

In `types.rs`, remove:

```rust
pub const NOVAROCKS_ROW_LEVEL_OP: &str = "novarocks.row-level-op";
pub const NOVAROCKS_ROW_LEVEL_OP_UPDATE: &str = "update";
pub const NOVAROCKS_UPDATE_MODE: &str = "novarocks.update.mode";
pub const NOVAROCKS_UPDATE_MODE_COW: &str = "copy-on-write";
pub const NOVAROCKS_UPDATE_MODE_MOR: &str = "merge-on-read";
pub const NOVAROCKS_UPDATE_SIDECAR: &str = "novarocks.update.sidecar";
```

Remove the full `MutationSidecar` and `MutationSidecarFile` struct definitions, including their derives, fields, and constructors. Remove tests that only validate sidecar JSON round-trips. Keep `IcebergUpdateMode` if SQL UPDATE routing still uses it.

In `commit/mod.rs`, remove the `write_mutation_sidecar` export.

- [ ] **Step 8: Run COW compile tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::commit::update_cow
```

Expected: PASS.

- [ ] **Step 9: Commit Task 5**

```bash
git add src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/update_cow.rs src/connector/iceberg/commit/mod.rs src/engine/mutation_flow.rs
git commit -m "refactor(iceberg): remove persistent COW update sidecar"
```

---

### Task 6: End-to-End MV Regression and Final Verification

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify tests touched by earlier compile errors

- [ ] **Step 1: Add managed-lake aggregate MV COW UPDATE regression**

Add this test in `mv_refresh.rs` tests near the existing aggregate MV delete tests:

```rust
#[test]
fn aggregate_mv_incremental_refresh_handles_cow_update_without_sidecar() {
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
                    "skipping COW UPDATE MV refresh test: object store unavailable: {err}"
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
    session
        .execute_in_database(&create_catalog_sql, "default")
        .expect("create iceberg catalog");
    session
        .execute_in_database("create database ice.ns", "default")
        .expect("create iceberg namespace");
    session
        .execute_in_database(
            r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="3","write.row-lineage"="true","write.update.mode"="copy-on-write")"#,
            "default",
        )
        .expect("create row-lineage COW iceberg table");
    session
        .execute_in_database(
            "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
            "default",
        )
        .expect("seed iceberg base rows");

    session
        .execute_in_database("create database analytics", "default")
        .expect("create analytics database");
    if let Err(err) = session.execute_in_database(
        "create materialized view agg_mv \
         distributed by hash(customer) buckets 2 \
         as select customer, count(*) as c, sum(amount) as s \
         from ice.ns.orders group by customer",
        "analytics",
    ) {
        if is_unavailable_object_store_error(&err) {
            eprintln!(
                "skipping COW UPDATE MV refresh test: object store unavailable on create: {err}"
            );
            return;
        }
        panic!("create materialized view: {err}");
    }

    session
        .execute_in_database("refresh materialized view agg_mv", "analytics")
        .expect("first full refresh");

    let pre_state = collect_agg_mv_state(&session).expect("pre-update state");
    assert_eq!(
        pre_state,
        vec![
            ("A".to_string(), 2_i64, 30_i64),
            ("B".to_string(), 1_i64, 30_i64),
        ]
    );

    session
        .execute_in_database("update ice.ns.orders set amount = 100 where id = 1", "default")
        .expect("COW update base row id=1");

    let loaded = {
        let state = engine.state();
        let base_ref = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        };
        load_current_iceberg_base_table(&state, &base_ref).expect("load base table")
    };
    let summary = &loaded
        .table
        .metadata()
        .current_snapshot()
        .expect("current snapshot")
        .summary()
        .additional_properties;
    assert!(!summary.contains_key("novarocks.row-level-op"));
    assert!(!summary.contains_key("novarocks.update.mode"));
    assert!(!summary.contains_key("novarocks.update.sidecar"));

    session
        .execute_in_database("refresh materialized view agg_mv", "analytics")
        .expect("incremental refresh after COW update");

    let post_state = collect_agg_mv_state(&session).expect("post-update state");
    assert_eq!(
        post_state,
        vec![
            ("A".to_string(), 2_i64, 120_i64),
            ("B".to_string(), 1_i64, 30_i64),
        ],
        "COW update must retract old amount=10 and apply new amount=100"
    );

    drop(engine);
}
```

- [ ] **Step 2: Run the new MV regression and confirm it fails before all wiring is complete**

Run:

```bash
cargo test -p novarocks connector::starrocks::managed::mv_refresh::tests::aggregate_mv_incremental_refresh_handles_cow_update_without_sidecar -- --nocapture
```

Expected before Task 1-5 are complete: FAIL from sidecar marker assertion, overwrite planning, or delete materialization. Expected after Task 1-5 are complete: PASS or skip with an object-store unavailable message.

- [ ] **Step 3: Run targeted Iceberg change tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::changes
```

Expected: PASS.

- [ ] **Step 4: Run targeted COW commit tests**

Run:

```bash
cargo test -p novarocks connector::iceberg::commit::update_cow
```

Expected: PASS.

- [ ] **Step 5: Run targeted managed MV tests**

Run:

```bash
cargo test -p novarocks connector::starrocks::managed::mv_refresh::tests::aggregate_mv_incremental_refresh_handles_cow_update_without_sidecar -- --nocapture
```

Expected: PASS, or skip only when MinIO/object-store config is unavailable. A logic failure is not an acceptable skip.

- [ ] **Step 6: Run formatting**

Run:

```bash
cargo fmt
```

Expected: no output on success.

- [ ] **Step 7: Run compile sanity for tests**

Run:

```bash
cargo check -p novarocks --tests
```

Expected: PASS.

- [ ] **Step 8: Inspect diff for private sidecar leftovers**

Run:

```bash
rg -n "MutationSidecar|MutationSidecarFile|novarocks\\.update\\.sidecar|write_mutation_sidecar|CollectCowUpdate|CowUpdateRef|CollectMorUpdate|MorUpdateRef|update_marker_mode" src
```

Expected: no matches. If `IcebergUpdateMode` still appears, it must be SQL write-mode routing only and not snapshot summary/change-planning correctness.

- [ ] **Step 9: Commit Task 6**

```bash
git add src/connector/iceberg/changes.rs src/connector/iceberg/commit/types.rs src/connector/iceberg/commit/run.rs src/connector/iceberg/commit/update_cow.rs src/connector/iceberg/commit/mod.rs src/engine/mutation_flow.rs src/connector/starrocks/managed/ivm_change_stream.rs src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "test(mv): verify COW update refresh without sidecar"
```

---

## Final Verification

- [ ] Run full focused validation:

```bash
cargo fmt
cargo test -p novarocks connector::iceberg::changes
cargo test -p novarocks connector::iceberg::commit::update_cow
cargo test -p novarocks connector::starrocks::managed::ivm_change_stream
cargo test -p novarocks connector::starrocks::managed::mv_refresh::tests::aggregate_mv_incremental_refresh_handles_cow_update_without_sidecar -- --nocapture
cargo check -p novarocks --tests
```

Expected: all commands pass, except object-store-backed MV tests may print a skip message only when the local object-store endpoint is unavailable.

- [ ] If SQL/MV environment commands are needed, discover the active worktree environment first:

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

Use `$NOVAROCKS_STANDALONE_CONFIG`, `$NOVAROCKS_SQL_TEST_CONFIG`, and `$NOVA_ENV_MYSQL_PORT`; do not hard-code port `9030`.

- [ ] Confirm sidecar removal:

```bash
rg -n "MutationSidecar|MutationSidecarFile|novarocks\\.update\\.sidecar|write_mutation_sidecar|CollectCowUpdate|CowUpdateRef|CollectMorUpdate|MorUpdateRef|update_marker_mode" src
```

Expected: no matches.

- [ ] Confirm COW UPDATE summary is standard-only through tests:

```bash
cargo test -p novarocks connector::iceberg::commit::update_cow::tests::cow_update_summary_uses_standard_overwrite_counters_only
```

Expected: PASS.
