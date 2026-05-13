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
// KIND, either except as required by applicable law or agreed
// to in writing, either express or implied.  See the License
// for the specific language governing permissions and
// limitations under the License.

//! `RowDeltaCommit` — the DELETE-FROM commit-action.
//!
//! Iceberg-rust 0.9 does not ship a public `Transaction::row_delta()` action,
//! so this is implemented as a custom `TransactionAction` (the
//! `vendor/iceberg-0.9.0` patch raises the trait visibility — see
//! `vendor/iceberg-0.9.0/PATCH.md`). The action:
//!
//! 1. Writes one v2/v3 deletes manifest containing the freshly-written
//!    position-delete files via `ManifestWriter::add_delete_file`.
//! 2. Inherits every entry from the base snapshot's manifest list.
//! 3. Writes a new manifest list combining the inherited entries with the
//!    new delete manifest entry.
//! 4. Constructs a `Snapshot` whose `summary.operation = "delete"`.
//! 5. Returns an `ActionCommit` containing `AddSnapshot + SetSnapshotRef` and
//!    `AssertRefSnapshotId / AssertCurrentSchemaId / AssertDefaultSpecId`
//!    requirements for OCC. iceberg-rust's `Transaction::do_commit` packages
//!    this into a `TableCommit` and calls `Catalog::update_table`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestFile, ManifestWriterBuilder,
    Operation, PartitionSpecRef, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{
    effective_next_row_id, generate_snapshot_id, metadata_dir, now_ms, read_base_manifest_list,
    write_manifest_list,
};
use super::types::{CommitOutcome, WrittenFile};

pub struct RowDeltaCommit;

#[async_trait]
impl IcebergCommitAction for RowDeltaCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Spec §4.1: empty input → no-op.
        if written.is_empty() {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }
        for f in &written {
            if f.content != DataContentType::PositionDeletes
                && f.content != DataContentType::EqualityDeletes
            {
                return Err(format!(
                    "RowDeltaCommit received {:?} content; expected delete-file content",
                    f.content
                ));
            }
            if f.content == DataContentType::EqualityDeletes
                && f.equality_ids.as_ref().is_none_or(Vec::is_empty)
            {
                return Err(format!(
                    "RowDeltaCommit received equality-delete file {} without equality_ids",
                    f.path
                ));
            }
        }

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = RowDeltaTxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("RowDelta apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("RowDelta commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "RowDelta committed but new snapshot is not visible".to_string())?;
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

struct RowDeltaTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    /// Mutex<Vec<String>> shared with the outer RowDeltaCommit so the wrapper
    /// can return the written manifest paths in `CommitOutcome` after the
    /// transaction completes.
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
}

#[async_trait]
impl TransactionAction for RowDeltaTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
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

        // 1. Write one new delete manifest per referenced partition spec.
        let mut new_delete_manifests = Vec::new();
        for (idx, (spec_id, files)) in group_written_by_spec(self.written.clone())
            .into_iter()
            .enumerate()
        {
            let spec = m.partition_spec_by_id(spec_id).cloned().ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "RowDelta delete file references unknown partition spec id {spec_id}"
                ))
            })?;
            let delete_manifest_path = format!(
                "{metadata_dir}/{}-row-delta-deletes-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle
                .record_manifest(delete_manifest_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(delete_manifest_path.clone());
            let manifest = write_delete_manifest(
                &self.file_io,
                &delete_manifest_path,
                &files,
                spec,
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_delete_manifests.push(manifest);
        }

        // 2. Inherit every entry from the base manifest list.
        let mut entries = read_base_manifest_list(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;
        entries.extend(new_delete_manifests);

        // 3. Write the new manifest list.
        let row_lineage_first_row_id = if format_version == FormatVersion::V3 {
            Some(effective_next_row_id(m).map_err(to_iceberg_unexpected)?)
        } else {
            None
        };
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.abort_handle
            .record_manifest(manifest_list_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(manifest_list_path.clone());
        write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            entries,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            row_lineage_first_row_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        // 4. Construct the new Snapshot.
        let snapshot_summary = Summary {
            operation: Operation::Delete,
            additional_properties: row_delta_summary(&self.written),
        };
        let snapshot = if let Some(first_row_id) = row_lineage_first_row_id {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(snapshot_summary)
                .with_schema_id(self.schema_id)
                .with_row_range(first_row_id, 0)
                .build()
        } else {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(snapshot_summary)
                .with_schema_id(self.schema_id)
                .build()
        };

        // 5. Build TableUpdate / TableRequirement set. iceberg-rust's
        //    Transaction::do_commit packages this into a TableCommit and
        //    submits via Catalog::update_table.
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
        let mut requirements = vec![
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
        // De-dup is not required by iceberg-rust; placed deterministically.
        requirements.sort_by_key(|r| match r {
            TableRequirement::CurrentSchemaIdMatch { .. } => 0,
            TableRequirement::DefaultSpecIdMatch { .. } => 1,
            TableRequirement::RefSnapshotIdMatch { .. } => 2,
            _ => 99,
        });

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    written: &[WrittenFile],
    partition_spec: PartitionSpecRef,
    schema: iceberg::spec::SchemaRef,
    new_seq: i64,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = match format_version {
        FormatVersion::V2 => builder.build_v2_deletes(),
        FormatVersion::V3 => builder.build_v3_deletes(),
        FormatVersion::V1 => {
            return Err("v1 tables do not support delete files; phase 1 rejects v1".to_string());
        }
    };
    for f in written {
        // We don't have a per-file IcebergCommitCollector handy in this scope;
        // the conversion only needs schema-derived hints which are not used
        // (DataFileBuilder ignores the collector parameter for now).
        let df = written_file_to_iceberg_data_file_minimal(f)?;
        // Newly-introduced position-delete files must be recorded with
        // `ManifestStatus::Added` so downstream readers (e.g., the
        // `plan_changes` / `collect_files` lineage walk) include them in
        // the delete-bearing change set.  iceberg-rust's `add_delete_file`
        // helper sets `status=Deleted` — that variant is reserved for
        // marking files REMOVED in compaction-style snapshots, not for
        // adding new delete files — its name is misleading.  Use
        // `add_file`, which builds a status=Added entry that the writer
        // accepts for `Deletes`-content manifests too (`check_data_file`
        // verifies the file's `DataContentType` is `PositionDeletes` or
        // `EqualityDeletes`).
        writer
            .add_file(df, new_seq)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;

    // Sanity-check the content stamped by the writer matches what we asked for.
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

/// Construct a DataFile from a WrittenFile without needing the full collector.
/// Mirrors `written_file_to_iceberg_data_file` but avoids the unused
/// `_collector` argument.
fn written_file_to_iceberg_data_file_minimal(
    f: &WrittenFile,
) -> Result<iceberg::spec::DataFile, String> {
    use iceberg::spec::DataFileBuilder;
    let mut builder = DataFileBuilder::default();
    builder
        .content(f.content)
        .file_path(f.path.clone())
        .file_format(f.format)
        .partition(f.partition_values.clone())
        .partition_spec_id(f.partition_spec_id)
        .record_count(f.record_count)
        .file_size_in_bytes(f.file_size_in_bytes);
    if !f.split_offsets.is_empty() {
        builder.split_offsets(Some(f.split_offsets.clone()));
    }
    if let Some(km) = &f.key_metadata {
        builder.key_metadata(Some(km.clone()));
    }
    if let Some(ref_path) = &f.referenced_data_file {
        builder.referenced_data_file(Some(ref_path.clone()));
    }
    if let Some(equality_ids) = &f.equality_ids {
        builder.equality_ids(Some(equality_ids.clone()));
    }
    if !f.column_sizes.is_empty() {
        builder.column_sizes(f.column_sizes.clone());
    }
    if !f.value_counts.is_empty() {
        builder.value_counts(f.value_counts.clone());
    }
    if !f.null_value_counts.is_empty() {
        builder.null_value_counts(f.null_value_counts.clone());
    }
    builder
        .build()
        .map_err(|e| format!("DataFileBuilder::build failed: {e}"))
}

fn row_delta_summary(written: &[WrittenFile]) -> HashMap<String, String> {
    let mut p = HashMap::new();
    let position_files = written
        .iter()
        .filter(|f| f.content == DataContentType::PositionDeletes)
        .count();
    let equality_files = written
        .iter()
        .filter(|f| f.content == DataContentType::EqualityDeletes)
        .count();
    let position_records: u64 = written
        .iter()
        .filter(|f| f.content == DataContentType::PositionDeletes)
        .map(|f| f.record_count)
        .sum();
    let equality_records: u64 = written
        .iter()
        .filter(|f| f.content == DataContentType::EqualityDeletes)
        .map(|f| f.record_count)
        .sum();
    let total_size: u64 = written.iter().map(|f| f.file_size_in_bytes).sum();
    if position_files > 0 {
        p.insert(
            "added-position-delete-files".to_string(),
            position_files.to_string(),
        );
        p.insert(
            "added-position-deletes".to_string(),
            position_records.to_string(),
        );
    }
    if equality_files > 0 {
        p.insert(
            "added-equality-delete-files".to_string(),
            equality_files.to_string(),
        );
        p.insert(
            "added-equality-deletes".to_string(),
            equality_records.to_string(),
        );
        p.insert(
            "total-equality-deletes".to_string(),
            equality_records.to_string(),
        );
    }
    p.insert("added-delete-files".to_string(), written.len().to_string());
    p.insert("added-files-size".to_string(), total_size.to_string());
    p
}

fn group_written_by_spec(written: Vec<WrittenFile>) -> BTreeMap<i32, Vec<WrittenFile>> {
    let mut grouped = BTreeMap::new();
    for file in written {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file);
    }
    grouped
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{DataContentType, DataFileFormat, Struct};

    use super::*;

    fn test_written_position_delete(path: &str) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::PositionDeletes,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 1,
            file_size_in_bytes: 256,
            split_offsets: vec![],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            key_metadata: None,
            referenced_data_file: Some("s3://bucket/data.parquet".to_string()),
            equality_ids: None,
            first_row_id: None,
        }
    }

    #[test]
    fn equality_delete_written_file_preserves_equality_ids() {
        let file = WrittenFile {
            path: "s3://bucket/table/data/eq-delete.parquet".to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::EqualityDeletes,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 2,
            file_size_in_bytes: 128,
            split_offsets: Vec::new(),
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: Some(vec![1, 2]),
            first_row_id: None,
        };

        let df = written_file_to_iceberg_data_file_minimal(&file).expect("data file");

        assert_eq!(df.content_type(), DataContentType::EqualityDeletes);
        assert_eq!(df.equality_ids(), Some(vec![1, 2]));
    }

    #[test]
    fn row_delta_groups_written_delete_files_by_partition_spec() {
        let mut a = test_written_position_delete("s3://bucket/delete-a.parquet");
        a.partition_spec_id = 0;
        let mut b = test_written_position_delete("s3://bucket/delete-b.parquet");
        b.partition_spec_id = 7;

        let grouped = group_written_by_spec(vec![b.clone(), a.clone()]);

        assert_eq!(grouped.keys().copied().collect::<Vec<_>>(), vec![0, 7]);
        assert_eq!(grouped.get(&0).unwrap()[0].path, a.path);
        assert_eq!(grouped.get(&7).unwrap()[0].path, b.path);
    }
}
