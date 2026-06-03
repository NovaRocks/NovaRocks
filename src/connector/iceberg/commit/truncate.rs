// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `TruncateCommit` — write a single `operation=delete` snapshot that marks
//! every live data + delete file as DELETED while preserving schema, partition
//! spec, properties, and other refs.
//!
//! Differences from `OverwriteCommit`:
//! * No `written` files: TRUNCATE never adds rows.
//! * No row-lineage advance: spec says `last-row-id` is NOT advanced, so the
//!   manifest list is written with `first_row_id: None` and the V3 snapshot
//!   row range carries `(next_row_id, 0)` — the validator at
//!   `iceberg-0.9.0/src/spec/table_metadata_builder.rs:419` rejects a V3
//!   snapshot with a null first-row-id, but `added_rows_count = 0` means
//!   `next_row_id` is preserved across the snapshot.
//! * Splits enumerated entries by `DataContentType` so position-delete /
//!   equality-delete / Iceberg v3 deletion-vector entries land in a separate
//!   `Deletes`-typed manifest (the existing `write_overwrite_deletes_manifest`
//!   helper is hard-wired to `build_v*_data()` and would reject delete-content
//!   entries via `ManifestWriter::check_data_file`).
//! * Summary `operation = "delete"` plus the proper `deleted-data-files` /
//!   `removed-position-delete-files` / `removed-equality-delete-files` counts.
//!
//! Even when the base table is empty we still write a `delete` snapshot with
//! `deleted-data-files = 0` so TRUNCATE leaves an audit trail entry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, Operation, PartitionSpecRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{
    finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::overwrite::{
    enumerate_live_all_files, write_overwrite_deletes_manifest, write_truncate_deletes_manifest,
};
use super::types::CommitOutcome;

pub struct TruncateCommit;

#[async_trait]
impl IcebergCommitAction for TruncateCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = TruncateTxnAction {
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("Truncate apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("Truncate commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        Ok(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        })
    }
}

struct TruncateTxnAction {
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
}

impl TruncateTxnAction {
    fn record_manifest_path(&self, path: String) {
        self.abort_handle.record_manifest(path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(path);
    }
}

#[async_trait]
impl TransactionAction for TruncateTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version == FormatVersion::V1 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "TruncateCommit does not support V1 tables",
            ));
        }

        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = m
            .refs()
            .get(target_ref.as_str())
            .map(|r| r.snapshot_id)
            .or_else(|| {
                if target_ref == "main" {
                    m.current_snapshot().map(|s| s.snapshot_id())
                } else {
                    None
                }
            });
        let metadata_dir = metadata_dir(table);

        // 1. Enumerate every live entry — Data AND Deletes manifests alike.
        //    TRUNCATE must mark every live data / position-delete /
        //    equality-delete / DV entry as DELETED.
        let existing = enumerate_live_all_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        // 2. Split by content type so we can route entries into the correct
        //    manifest kind. `add_delete_file` insists that every entry in a
        //    Data manifest has DataContentType::Data; mixing types triggers a
        //    DataInvalid error from `check_data_file`.
        let (data_entries, delete_entries): (Vec<_>, Vec<_>) = existing
            .iter()
            .cloned()
            .partition(|(df, _, _)| df.content_type() == DataContentType::Data);

        let mut new_manifests: Vec<iceberg::spec::ManifestFile> = Vec::new();

        // 3a. Write the data-content deletes manifest if any data files are live.
        if !data_entries.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-truncate-deleted-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &data_entries,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3b. Write the delete-content deletes manifest if any delete files
        //     (position-deletes, equality-deletes, or v3 deletion vectors —
        //     spec says DVs are encoded with content_type = PositionDeletes)
        //     are live.
        if !delete_entries.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-truncate-deleted-deletes-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_truncate_deletes_manifest(
                &self.file_io,
                &path,
                &delete_entries,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 4. Write the manifest list (may be empty for the empty-table case).
        //    `first_row_id = None` because TRUNCATE never advances row
        //    lineage — spec: `last-row-id is NOT advanced`.
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.record_manifest_path(manifest_list_path.clone());
        write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            None,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        // 5. Build the snapshot with operation = delete + classification
        //    counts. TRUNCATE adds zero rows, so `added_rows_count = 0` —
        //    `next_row_id` is therefore NOT advanced (per spec: `last-row-id
        //    is NOT advanced`). We still must set `first_row_id` for V3
        //    because `add_snapshot` rejects a V3 snapshot with no row range
        //    (see iceberg-0.9.0/src/spec/table_metadata_builder.rs:419).
        //    Setting `(next_row_id, 0)` is the spec-faithful way to record
        //    "no new row ids consumed" while still satisfying the validator.
        let summary = Summary {
            operation: Operation::Delete,
            additional_properties: finalize_snapshot_summary(
                truncate_summary(&data_entries, &delete_entries),
                m.current_snapshot().map(|s| s.summary()),
                true,
            ),
        };
        let snapshot_builder = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(self.schema_id);
        let snapshot = match format_version {
            FormatVersion::V3 => snapshot_builder.with_row_range(m.next_row_id(), 0).build(),
            _ => snapshot_builder.build(),
        };

        // 6. Build the TableUpdate / TableRequirement set.
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: target_ref.clone(),
                reference: SnapshotReference {
                    snapshot_id: new_snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ];
        let requirements = vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: m.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: m.default_partition_spec_id(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: target_ref.clone(),
                snapshot_id: parent_snapshot_id,
            },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}

fn truncate_summary(
    data_entries: &[(DataFile, i64, Option<i64>)],
    delete_entries: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let removed_position_delete_files = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .count();
    let removed_equality_delete_files = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .count();
    let removed_position_deletes = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let removed_equality_deletes = delete_entries
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let deleted_records = data_entries
        .iter()
        .map(|(df, _, _)| df.record_count())
        .sum::<u64>();
    let removed_files_size: u64 = data_entries
        .iter()
        .chain(delete_entries.iter())
        .map(|(df, _, _)| df.file_size_in_bytes())
        .sum();

    let mut p = HashMap::new();
    // TRUNCATE never adds anything; the added-* counters are pinned to 0 so
    // downstream tooling that diffs snapshot summaries doesn't see stale
    // values from a prior snapshot.
    p.insert("added-data-files".to_string(), "0".to_string());
    p.insert("added-records".to_string(), "0".to_string());
    p.insert("added-files-size".to_string(), "0".to_string());
    p.insert("added-delete-files".to_string(), "0".to_string());

    p.insert(
        "deleted-data-files".to_string(),
        data_entries.len().to_string(),
    );
    p.insert("deleted-records".to_string(), deleted_records.to_string());
    p.insert(
        "removed-delete-files".to_string(),
        delete_entries.len().to_string(),
    );
    p.insert(
        "removed-position-delete-files".to_string(),
        removed_position_delete_files.to_string(),
    );
    p.insert(
        "removed-equality-delete-files".to_string(),
        removed_equality_delete_files.to_string(),
    );
    p.insert(
        "removed-position-deletes".to_string(),
        removed_position_deletes.to_string(),
    );
    p.insert(
        "removed-equality-deletes".to_string(),
        removed_equality_deletes.to_string(),
    );
    p.insert(
        "removed-files-size".to_string(),
        removed_files_size.to_string(),
    );
    p
}

// Local copy of the helper at `overwrite.rs::to_iceberg_unexpected` —
// the original is `pub(super)`-scoped to `commit::overwrite` and is not
// visible from sibling submodules. Keep in sync with that definition;
// promote to `commit::helpers` if a third call site shows up.
fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    //! Unit tests for `TruncateCommit`.
    //!
    //! Coverage:
    //! * `truncate_empty_table_writes_noop_delete_snapshot` (Task 6a) —
    //!   non-negotiable: empty table TRUNCATE writes a `delete` snapshot with
    //!   `deleted-data-files = 0`.
    //! * `truncate_table_with_data_files_marks_all_deleted` (Task 6b) —
    //!   end-to-end via `FastAppendCommit` fixture: 3 data files become
    //!   DELETED, summary records `deleted-data-files = 3`, and a
    //!   re-enumeration of the post-truncate snapshot returns no live entries.
    //! * Two summary classifier unit tests pin the data / position-delete /
    //!   equality-delete classification.
    //!
    //! Task 6c (mixed data + DV) coverage is deferred to the SQL regression
    //! suite (Task 8 of the TRUNCATE plan): a real DV blob requires a Puffin
    //! writer driven against a real (DataFile, _row_id) input, which in turn
    //! needs a real Parquet file produced by the standalone engine. The SQL
    //! regression already covers `TRUNCATE` on tables with DV / position-delete
    //! / equality-delete files end-to-end with realistic inputs, which is
    //! stronger coverage than what an inline mock fixture would provide.
    use super::*;
    use crate::connector::iceberg::commit::test_helpers::{
        empty_v3_iceberg_table, run_commit_with, v3_table_with_n_data_files,
    };
    use crate::connector::iceberg::commit::types::CommitOpKind;
    use iceberg::spec::{DataFileBuilder, DataFileFormat, Operation, Struct};

    #[test]
    fn truncate_commit_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&TruncateCommit);
    }

    #[test]
    fn truncate_summary_pins_added_to_zero_for_empty_inputs() {
        let s = truncate_summary(&[], &[]);
        assert_eq!(s.get("operation").map(String::as_str), None);
        assert_eq!(s["added-data-files"], "0");
        assert_eq!(s["added-records"], "0");
        assert_eq!(s["added-files-size"], "0");
        assert_eq!(s["added-delete-files"], "0");
        assert_eq!(s["deleted-data-files"], "0");
        assert_eq!(s["deleted-records"], "0");
        assert_eq!(s["removed-delete-files"], "0");
        assert_eq!(s["removed-position-delete-files"], "0");
        assert_eq!(s["removed-equality-delete-files"], "0");
        // total-records is now set by finalize_snapshot_summary (truncate_full_table=true), not here.
        assert!(
            !s.contains_key("total-records"),
            "truncate_summary must not emit total-records; finalize_snapshot_summary owns it"
        );
    }

    #[test]
    fn truncate_summary_classifies_position_and_equality_deletes() {
        let data = vec![
            test_entry(DataContentType::Data, 7, 100),
            test_entry(DataContentType::Data, 11, 200),
        ];
        let deletes = vec![
            test_entry(DataContentType::PositionDeletes, 3, 50),
            test_entry(DataContentType::EqualityDeletes, 5, 75),
            test_entry(DataContentType::PositionDeletes, 2, 25),
        ];

        let s = truncate_summary(&data, &deletes);

        assert_eq!(s["deleted-data-files"], "2");
        assert_eq!(s["deleted-records"], "18");
        assert_eq!(s["removed-delete-files"], "3");
        assert_eq!(s["removed-position-delete-files"], "2");
        assert_eq!(s["removed-equality-delete-files"], "1");
        assert_eq!(s["removed-position-deletes"], "5");
        assert_eq!(s["removed-equality-deletes"], "5");
        // 100 + 200 + 50 + 75 + 25
        assert_eq!(s["removed-files-size"], "450");
        // Pinned even on non-empty input — TRUNCATE never adds anything.
        assert_eq!(s["added-data-files"], "0");
        // total-records is now set by finalize_snapshot_summary (truncate_full_table=true), not here.
        assert!(!s.contains_key("total-records"));
    }

    fn test_entry(
        content: DataContentType,
        record_count: u64,
        file_size: u64,
    ) -> (DataFile, i64, Option<i64>) {
        let mut b = DataFileBuilder::default();
        b.content(content)
            .file_path(format!(
                "memory://test/data/{:?}-{record_count}.parquet",
                content
            ))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(record_count)
            .file_size_in_bytes(file_size);
        if matches!(content, DataContentType::EqualityDeletes) {
            b.equality_ids(Some(vec![1]));
        }
        let df = b.build().expect("build DataFile");
        (df, 1, Some(1))
    }

    /// Task 6a: empty table TRUNCATE must produce a real `operation=delete`
    /// snapshot whose summary records `deleted-data-files = 0`.
    #[tokio::test]
    async fn truncate_empty_table_writes_noop_delete_snapshot() {
        let fixture = empty_v3_iceberg_table().await;
        let outcome = run_commit_with(
            TruncateCommit,
            CommitOpKind::Truncate,
            fixture.clone(),
            "main",
        )
        .await
        .expect("TruncateCommit succeeds on empty table");

        assert_ne!(outcome.new_snapshot_id, 0, "expected a fresh snapshot id");

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after truncate");
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("table should have a current snapshot after truncate");
        assert_eq!(snap.summary().operation, Operation::Delete);
        let p = &snap.summary().additional_properties;
        assert_eq!(p.get("deleted-data-files").map(String::as_str), Some("0"));
        assert_eq!(p.get("added-data-files").map(String::as_str), Some("0"));
    }

    /// `write_truncate_deletes_manifest` should produce a `Deletes`-typed
    /// manifest when handed delete-content `DataFile`s. This is the smoke
    /// test that covers Task 6c's writer path in isolation; the full
    /// end-to-end coverage of TRUNCATE against a table that already has
    /// position-delete / DV files comes from the SQL regression suite (see
    /// the module-level deferral note).
    #[tokio::test]
    async fn write_truncate_deletes_manifest_produces_deletes_typed_manifest() {
        use iceberg::io::FileIO;
        use iceberg::spec::{
            ManifestContentType, NestedField, PartitionSpec, PrimitiveType, Schema, SchemaRef, Type,
        };
        use std::sync::Arc;
        let file_io = FileIO::new_with_memory();
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let partition_spec: PartitionSpecRef = Arc::new(PartitionSpec::unpartition_spec());

        let entries = vec![
            test_entry(DataContentType::PositionDeletes, 4, 60),
            test_entry(DataContentType::EqualityDeletes, 2, 30),
        ];
        let path = "memory:///metadata/test-truncate-deletes.avro";
        let mf = write_truncate_deletes_manifest(
            &file_io,
            path,
            &entries,
            partition_spec,
            schema,
            42,
            FormatVersion::V3,
        )
        .await
        .expect("write_truncate_deletes_manifest succeeds");
        assert_eq!(mf.content, ManifestContentType::Deletes);
        assert_eq!(mf.deleted_files_count, Some(2));
        assert_eq!(mf.added_files_count, Some(0));
    }

    /// Task 6b: TRUNCATE against a table with N live data files writes a
    /// `delete` snapshot whose summary records `deleted-data-files = N` and
    /// whose post-commit live-file enumeration is empty.
    #[tokio::test]
    async fn truncate_table_with_data_files_marks_all_deleted() {
        let n = 3;
        let fixture = v3_table_with_n_data_files(n).await;

        // Sanity: the fixture put N data files into the live set.
        let pre = enumerate_live_all_files(&fixture.table, &fixture.table.file_io().clone())
            .await
            .expect("enumerate pre-truncate");
        assert_eq!(pre.len(), n, "fixture should expose {n} live files");

        // Pin the row-lineage invariant: TRUNCATE must NOT advance
        // `last-row-id`. iceberg-rust 0.9 V3 validation requires a non-null
        // first-row-id even on zero-row snapshots, so the implementation
        // records `(next_row_id, 0)`; `added_rows = 0` keeps `next_row_id`
        // stable across the commit. Snapshot the pre-truncate value here so
        // the post-truncate assertion below catches any future regression
        // in the validator that would silently advance row lineage.
        let pre_next_row_id = fixture.table.metadata().next_row_id();

        let outcome = run_commit_with(
            TruncateCommit,
            CommitOpKind::Truncate,
            fixture.clone(),
            "main",
        )
        .await
        .expect("TruncateCommit succeeds on data-only table");
        assert_ne!(outcome.new_snapshot_id, 0);

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after truncate");
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("post-truncate snapshot");
        assert_eq!(snap.summary().operation, Operation::Delete);
        let p = &snap.summary().additional_properties;
        assert_eq!(
            p.get("deleted-data-files").map(String::as_str),
            Some("3"),
            "summary should record 3 deleted data files"
        );
        assert_eq!(
            p.get("added-data-files").map(String::as_str),
            Some("0"),
            "TRUNCATE never adds anything"
        );
        // 3 files * 10 records each = 30 deleted records, all from the
        // synthetic FastAppend fixture.
        assert_eq!(
            p.get("deleted-records").map(String::as_str),
            Some("30"),
            "deleted-records should equal sum of record_count across data entries"
        );
        // finalize_snapshot_summary with truncate_full_table=true must reset all total-* to 0.
        assert_eq!(
            p.get("total-records").map(String::as_str),
            Some("0"),
            "total-records must be reset to 0 by finalize_snapshot_summary on truncate"
        );
        assert_eq!(
            p.get("total-data-files").map(String::as_str),
            Some("0"),
            "total-data-files must be reset to 0 on truncate"
        );
        assert_eq!(
            p.get("engine-name").map(String::as_str),
            Some("novarocks"),
            "engine-name must be stamped by finalize_snapshot_summary"
        );

        // Re-enumerate the post-truncate snapshot and confirm zero live files.
        let post = enumerate_live_all_files(&reloaded, &reloaded.file_io().clone())
            .await
            .expect("enumerate post-truncate");
        assert!(
            post.is_empty(),
            "expected no live entries after truncate, got {} ({:?})",
            post.len(),
            post.iter()
                .map(|(df, _, _)| df.file_path())
                .collect::<Vec<_>>(),
        );

        // Row-lineage invariance: TRUNCATE must NOT advance `last-row-id`.
        // See the pre_next_row_id capture above for context.
        assert_eq!(
            reloaded.metadata().next_row_id(),
            pre_next_row_id,
            "TRUNCATE must NOT advance last-row-id (Iceberg v3 spec); \
             a regression in iceberg-rust's V3 row-range validator could \
             silently advance it via the (next_row_id, 0) workaround"
        );
    }
}
