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

//! `OverwriteCommit` — the INSERT-OVERWRITE commit-action.
//!
//! Iceberg-rust 0.9 does not ship a public `Transaction::overwrite_files()`
//! action, so this is a custom `TransactionAction` (depends on the
//! `vendor/iceberg-0.9.0` patch). The action:
//!
//! 1. Walks the base snapshot's manifest list and collects every live data
//!    file (status ∈ {Added, Existing}) along with its original sequence
//!    numbers — required to mark each as DELETED faithfully.
//! 2. Writes a v2/v3 data manifest containing one DELETED entry per base data
//!    file via `ManifestWriter::add_delete_file` (which the Task 1 spike
//!    confirmed is the only public path to status=Deleted entries).
//! 3. Writes a v2/v3 data manifest containing the freshly-written data files
//!    as ADDED via `ManifestWriter::add_file`.
//! 4. Writes a new manifest list. **Does not inherit base manifest list
//!    entries** (per spec §4.3 step 4): the `overwrite-deletes` manifest
//!    above already records the deletions; inheritance would be redundant.
//! 5. Builds a `Snapshot` whose `summary.operation = "overwrite"`.
//! 6. Returns an `ActionCommit` with `AddSnapshot + SetSnapshotRef` updates
//!    and `AssertRefSnapshotId / SchemaId / SpecId` requirements.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestStatus,
    ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference,
    SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    effective_next_row_id, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct OverwriteCommit;

#[async_trait]
impl IcebergCommitAction for OverwriteCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "OverwriteCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }
        let row_lineage_first_row_id =
            match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
                IcebergWriteMode::RowLineageV3 => {
                    Some(effective_next_row_id(ctx.table.metadata())?)
                }
                IcebergWriteMode::LegacyPositionDeletes => None,
            };
        let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
            sum.checked_add(f.record_count)
                .ok_or_else(|| "row-lineage added row count overflow".to_string())
        })?;

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = OverwriteTxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            row_lineage_first_row_id,
            row_lineage_added_rows,
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("Overwrite apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("Overwrite commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            // No-op overwrite (empty written + empty base) returns the
            // pre-existing snapshot id, which may also be None if the table
            // was empty to begin with. Treat that as snapshot_id = 0.
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

struct OverwriteTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    row_lineage_first_row_id: Option<u64>,
    row_lineage_added_rows: u64,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
}

#[async_trait]
impl TransactionAction for OverwriteTxnAction {
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

        // 1. Enumerate live data files in the base snapshot.
        let existing = enumerate_live_data_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        // No-op short circuit: empty input + empty base table → return existing
        // updates/requirements set as a degenerate ActionCommit. iceberg-rust's
        // Transaction::do_commit treats an empty action set as a noop, but
        // returning empty ActionCommit here keeps us consistent with the spec.
        if self.written.is_empty() && existing.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let additional_properties = merge_snapshot_summary_properties(
            overwrite_summary(&self.written, &existing),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties,
        };

        let mut new_manifests: Vec<ManifestFile> = Vec::with_capacity(2);

        // 2. Write the deleted-data manifest, if base had any data.
        if !existing.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-deletes-0.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &existing,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3. Write the added-data manifest, if any rows were written.
        if !self.written.is_empty() {
            let path = format!("{metadata_dir}/{}-overwrite-data-0.avro", self.commit_uuid);
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_added_data_manifest(
                &self.file_io,
                &path,
                &self.written,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 4. Write the manifest list (does NOT inherit base manifest entries
        //    per spec §4.3 step 4).
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
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            self.row_lineage_first_row_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if let Some(first_row_id) = self.row_lineage_first_row_id {
            let expected_next_row_id = first_row_id
                .checked_add(self.row_lineage_added_rows)
                .ok_or_else(|| {
                    to_iceberg_unexpected(format!(
                        "Row ID overflow when computing overwrite row lineage range: first_row_id={first_row_id}, added_rows={}",
                        self.row_lineage_added_rows
                    ))
                })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        // 5. Construct the Snapshot.
        let snapshot = if let Some(first_row_id) = self.row_lineage_first_row_id {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(summary)
                .with_schema_id(self.schema_id)
                .with_row_range(first_row_id, self.row_lineage_added_rows)
                .build()
        } else {
            Snapshot::builder()
                .with_snapshot_id(new_snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(new_seq)
                .with_timestamp_ms(now_ms())
                .with_manifest_list(manifest_list_path)
                .with_summary(summary)
                .with_schema_id(self.schema_id)
                .build()
        };

        // 6. Build TableUpdate / TableRequirement set.
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

/// Walk every data manifest in the base snapshot's manifest list and collect
/// each live entry's `(DataFile, sequence_number, file_sequence_number)`. The
/// sequence numbers are needed verbatim by `add_delete_file` to faithfully
/// preserve the original commit identity.
///
/// INSERT OVERWRITE intentionally preserves delete manifests (they keep
/// applying against any rows preserved from the base table), so this walker
/// skips manifests with content type `Deletes`.
pub(super) async fn enumerate_live_data_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    enumerate_live_files_filtered(table, file_io, |entry| {
        entry.content == ManifestContentType::Data
    })
    .await
}

/// Walk every manifest in the base snapshot's manifest list (Data and
/// Deletes alike) and collect every live entry's
/// `(DataFile, sequence_number, file_sequence_number)`.
///
/// Distinct from `enumerate_live_data_files` which skips delete manifests:
/// `TRUNCATE TABLE` must mark every live entry — data files,
/// position-delete files, equality-delete files, and Iceberg v3 deletion
/// vectors — as DELETED in the new snapshot, so this walker accepts both
/// `ManifestContentType::Data` and `ManifestContentType::Deletes`.
pub(super) async fn enumerate_live_all_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    enumerate_live_files_filtered(table, file_io, |_entry| true).await
}

/// Shared body for `enumerate_live_data_files` and `enumerate_live_all_files`.
/// Walks the base snapshot's manifest list, applying `manifest_filter` to
/// each manifest entry — only manifests for which the filter returns `true`
/// are loaded and inspected.
async fn enumerate_live_files_filtered<F>(
    table: &Table,
    file_io: &FileIO,
    manifest_filter: F,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String>
where
    F: Fn(&ManifestFile) -> bool,
{
    let m = table.metadata();
    let snap = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = iceberg::spec::ManifestList::parse_with_version(&bytes, m.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;

    let mut out = Vec::new();
    for entry in list.entries() {
        if !manifest_filter(entry) {
            continue;
        }
        let manifest = entry
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load_manifest({}) failed: {e}", entry.manifest_path))?;
        for me in manifest.entries() {
            if me.is_alive() {
                let data_file = me.data_file().clone();
                // For inherited entries, sequence_number / file_sequence_number
                // may be None — fall back to the manifest's sequence.
                let seq = me.sequence_number().unwrap_or(entry.sequence_number);
                let file_seq = me.file_sequence_number;
                out.push((data_file, seq, file_seq));
            }
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_overwrite_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    existing: &[(DataFile, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
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
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => {
            return Err("phase 1 does not support V1 tables".to_string());
        }
    };
    for (df, seq, file_seq) in existing {
        writer
            .add_delete_file(df.clone(), *seq, *file_seq)
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

/// Sibling of `write_overwrite_deletes_manifest` used by `TruncateCommit` for
/// the delete-content (position-delete / equality-delete / Iceberg v3 deletion
/// vector) entries. The existing helper above is hard-wired to
/// `build_v*_data()` so adding a `DataFile` whose `content_type()` is
/// `PositionDeletes` or `EqualityDeletes` would be rejected by
/// `ManifestWriter::check_data_file` (which insists every entry in a Data
/// manifest has `DataContentType::Data`). A separate helper that picks
/// `build_v*_deletes()` is the cleanest fix; mirroring the existing function
/// otherwise keeps the diff minimal and the behaviour parallel.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_truncate_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    existing: &[(DataFile, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
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
            return Err("phase 1 does not support V1 tables".to_string());
        }
    };
    for (df, seq, file_seq) in existing {
        writer
            .add_delete_file(df.clone(), *seq, *file_seq)
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_added_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    written: &[WrittenFile],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
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
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 => return Err("phase 1 does not support V1 tables".to_string()),
    };
    for f in written {
        let df = build_minimal_data_file(f)?;
        writer
            .add_file(df, new_seq)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

pub(super) fn build_minimal_data_file(f: &WrittenFile) -> Result<DataFile, String> {
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
    if !f.column_sizes.is_empty() {
        builder.column_sizes(f.column_sizes.clone());
    }
    if !f.value_counts.is_empty() {
        builder.value_counts(f.value_counts.clone());
    }
    if !f.null_value_counts.is_empty() {
        builder.null_value_counts(f.null_value_counts.clone());
    }
    if let Some(first_row_id) = f.first_row_id {
        builder.first_row_id(Some(first_row_id));
    }
    builder
        .build()
        .map_err(|e| format!("DataFileBuilder::build failed: {e}"))
}

fn overwrite_summary(
    added: &[WrittenFile],
    deleted: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("added-data-files".to_string(), added.len().to_string());
    p.insert(
        "added-records".to_string(),
        added
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
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

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[allow(dead_code)]
fn _check_status_variant_referenced() {
    let _ = ManifestStatus::Deleted;
}

#[cfg(test)]
mod enumerate_tests {
    //! Smoke tests for the manifest-list enumeration helpers. Only the
    //! empty-snapshot path is exercised here; the mixed-content-type
    //! assertion (Data + position-delete + equality-delete + DV) is
    //! covered end-to-end by the SQL regression suite (Task 8 of the
    //! TRUNCATE plan), where a real iceberg backend produces all four
    //! content types.
    use super::*;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, TableIdent};
    use std::collections::HashMap;

    fn build_empty_table() -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let file_io = FileIO::new_with_memory();
        let ident = TableIdent::new(NamespaceIdent::new("db".to_string()), "table".to_string());
        Table::builder()
            .file_io(file_io)
            .metadata(metadata)
            .identifier(ident)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn enumerate_live_all_files_returns_empty_when_no_snapshot() {
        let table = build_empty_table();
        let file_io = table.file_io().clone();
        let out = enumerate_live_all_files(&table, &file_io)
            .await
            .expect("enumerate_live_all_files succeeds on empty table");
        assert!(out.is_empty(), "expected no entries, got {}", out.len());
    }

    #[tokio::test]
    async fn enumerate_live_data_files_returns_empty_when_no_snapshot() {
        let table = build_empty_table();
        let file_io = table.file_io().clone();
        let out = enumerate_live_data_files(&table, &file_io)
            .await
            .expect("enumerate_live_data_files succeeds on empty table");
        assert!(out.is_empty(), "expected no entries, got {}", out.len());
    }
}
