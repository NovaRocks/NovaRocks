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

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile, ManifestList,
    ManifestStatus, ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::Transaction;
use crate::iceberg::transaction::{ActionCommit, TransactionAction};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    OccSubmit, effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id,
    metadata_dir, now_ms, required_target_ref_snapshot_id, snapshot_summary, submit_occ_action,
    target_ref_snapshot_id, write_manifest_list,
};
use crate::commit::abort::AbortLog;
use crate::commit::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct OverwriteCommit;

#[async_trait]
impl IcebergCommitAction for OverwriteCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let staged = prepare_overwrite_action(&ctx)?;
        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);
        match submit_occ_action(
            ctx.catalog,
            ctx.table,
            Arc::clone(&staged.action),
            "Overwrite",
            None,
        )
        .await
        {
            Ok(OccSubmit::Committed(table_after)) => {
                let new_snapshot_id = match required_target_ref_snapshot_id(
                    table_after.metadata(),
                    ctx.target_ref,
                    "Overwrite",
                ) {
                    Ok(snapshot_id) => snapshot_id,
                    Err(_) if prev_snapshot_id.is_none() => 0,
                    Err(err) => return Err(err),
                };
                Ok(CommitOutcome {
                    new_snapshot_id,
                    written_manifest_paths: staged.written_manifest_paths(),
                })
            }
            // Empty input over an empty base with no provider properties: the
            // action proved there is nothing to publish, so the target ref
            // stays where it was (0 when it does not exist yet).
            Ok(OccSubmit::NoOp) => Ok(CommitOutcome {
                new_snapshot_id: prev_snapshot_id.unwrap_or(0),
                written_manifest_paths: staged.written_manifest_paths(),
            }),
            Err(error) => Err(error.into_detail()),
        }
    }
}

/// One overwrite snapshot staged against `ctx`, not yet submitted.
struct PreparedOverwriteAction {
    action: Arc<OverwriteTxnAction>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
}

impl PreparedOverwriteAction {
    fn written_manifest_paths(&self) -> Vec<String> {
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone()
    }
}

/// Build the overwrite action for `ctx`. Shared by the ordinary overwrite
/// commit and by atomic managed repartition, which submits the same action
/// inside its own `TableCommit`.
fn prepare_overwrite_action(ctx: &CommitCtx<'_>) -> Result<PreparedOverwriteAction, String> {
    let written = ctx.collector.take_written_files()?;
    for f in &written {
        if f.content != DataContentType::Data {
            return Err(format!(
                "OverwriteCommit received {:?} content; expected Data only",
                f.content
            ));
        }
    }
    let row_lineage_first_row_id = match crate::commit::classify_iceberg_write_mode(ctx.table) {
        IcebergWriteMode::RowLineageV3 => Some(effective_next_row_id(ctx.table.metadata())?),
        IcebergWriteMode::LegacyPositionDeletes => None,
    };
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let action = Arc::new(OverwriteTxnAction {
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
    });
    Ok(PreparedOverwriteAction {
        action,
        manifest_paths_out,
    })
}

/// Provider-owned overwrite changes prepared without submitting a catalog
/// update. Atomic managed repartition prepends its partition-spec updates and
/// submits this snapshot action in the same `TableCommit`.
pub(crate) struct StagedOverwriteAction<'a> {
    pub action: ActionCommit,
    pub outcome: CommitOutcome,
    pub table_ident: crate::iceberg::TableIdent,
    pub catalog: &'a dyn crate::iceberg::Catalog,
}

pub(crate) async fn build_staged_overwrite_action(
    ctx: CommitCtx<'_>,
) -> Result<StagedOverwriteAction<'_>, String> {
    let prepared = prepare_overwrite_action(&ctx)?;
    let mut staged = Arc::clone(&prepared.action)
        .commit(ctx.table)
        .await
        .map_err(|e| format!("Overwrite apply failed: {e}"))?;
    let updates = staged.take_updates();
    let requirements = staged.take_requirements();
    let new_snapshot_id = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
            _ => None,
        })
        .ok_or_else(|| "staged overwrite did not build an add-snapshot update".to_string())?;
    let written_manifest_paths = prepared.written_manifest_paths();
    Ok(StagedOverwriteAction {
        action: ActionCommit::new(updates, requirements),
        outcome: CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        },
        table_ident: ctx.collector.table_ident.clone(),
        catalog: ctx.catalog,
    })
}

/// Eagerly stage a full-table overwrite without catalog I/O so statistics can
/// be bound to the transaction-local snapshot before one combined dispatch.
pub(crate) async fn stage_eager_overwrite(
    ctx: CommitCtx<'_>,
    initial_updates: Vec<TableUpdate>,
) -> Result<(Transaction, CommitOutcome), String> {
    let prepared = prepare_overwrite_action(&ctx)?;
    let action: Arc<dyn TransactionAction> = prepared.action.clone();
    let tx = Transaction::new(ctx.table)
        .stage_action_commit(ActionCommit::new(initial_updates, Vec::new()))
        .map_err(|error| format!("Overwrite initial eager stage failed: {error}"))?
        .stage_action(action)
        .await
        .map_err(|error| format!("Overwrite eager stage failed: {error}"))?;
    let snapshot_id = target_ref_snapshot_id(tx.staged_table().metadata(), ctx.target_ref)
        .ok_or_else(|| "staged overwrite did not produce a target snapshot".to_string())?;
    Ok((
        tx,
        CommitOutcome {
            new_snapshot_id: snapshot_id,
            written_manifest_paths: prepared.written_manifest_paths(),
        },
    ))
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
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);

        // 1. Enumerate live data files in the base snapshot.
        let existing_entries =
            enumerate_live_data_file_entries_at_snapshot(table, &self.file_io, parent_snapshot_id)
                .await
                .map_err(to_iceberg_unexpected)?;
        let existing = live_data_entries_as_delete_entries(&existing_entries);

        // Ordinary empty input over an empty base is a no-op. Managed
        // publication and operation-recovery properties require a real empty
        // overwrite snapshot, so a non-empty provider property set must flow
        // through the normal snapshot construction below.
        if self.written.is_empty() && existing.is_empty() && self.snapshot_properties.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let additional_properties = merge_snapshot_summary_properties(
            finalize_snapshot_summary(
                overwrite_summary(&self.written, &existing),
                parent_summary,
                false,
            ),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties,
        };

        let delete_groups = group_live_data_entries_by_partition_spec(&existing_entries);
        let mut new_manifests: Vec<ManifestFile> =
            Vec::with_capacity(delete_groups.len() + usize::from(!self.written.is_empty()));

        // 2. Write deleted-data manifests grouped by their original partition
        // spec. Manifest entries carry partition tuples encoded against the
        // manifest-level spec, which can differ from the current default spec
        // after ALTER MATERIALIZED VIEW ... REPARTITION.
        for (idx, (spec_id, entries)) in delete_groups.into_iter().enumerate() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-deletes-{idx}-spec-{spec_id}.avro",
                self.commit_uuid,
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let partition_spec = m.partition_spec_by_id(spec_id).cloned().ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "Overwrite delete file references unknown partition spec id {spec_id}"
                ))
            })?;
            let existing = live_data_entries_as_delete_entries(&entries);
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &existing,
                partition_spec,
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
            TableRequirement::UuidMatch { uuid: m.uuid() },
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

#[derive(Clone)]
struct LiveDataFileEntry {
    data_file: DataFile,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    partition_spec_id: i32,
}

/// Walk every data manifest in the base snapshot's manifest list and collect
/// each live entry's `(DataFile, sequence_number, file_sequence_number)`. The
/// sequence numbers are needed verbatim by `add_delete_file` to faithfully
/// preserve the original commit identity.
///
/// INSERT OVERWRITE intentionally preserves delete manifests (they keep
/// applying against any rows preserved from the base table), so this walker
/// skips manifests with content type `Deletes`.
#[allow(dead_code)]
pub(super) async fn enumerate_live_data_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, Option<i64>)>, String> {
    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let entries = enumerate_live_data_file_entries_at_snapshot(table, file_io, snapshot_id).await?;
    Ok(live_data_entries_as_delete_entries(&entries))
}

async fn enumerate_live_data_file_entries_at_snapshot(
    table: &Table,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
) -> Result<Vec<LiveDataFileEntry>, String> {
    enumerate_live_files_filtered_at_snapshot(table, file_io, snapshot_id, |entry| {
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
    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let entries =
        enumerate_live_files_filtered_at_snapshot(table, file_io, snapshot_id, |_entry| true)
            .await?;
    Ok(live_data_entries_as_delete_entries(&entries))
}

/// Shared body for `enumerate_live_data_files` and `enumerate_live_all_files`.
/// Walks the base snapshot's manifest list, applying `manifest_filter` to
/// each manifest entry — only manifests for which the filter returns `true`
/// are loaded and inspected.
async fn enumerate_live_files_filtered_at_snapshot<F>(
    table: &Table,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
    manifest_filter: F,
) -> Result<Vec<LiveDataFileEntry>, String>
where
    F: Fn(&ManifestFile) -> bool,
{
    let m = table.metadata();
    let Some(snapshot_id) = snapshot_id else {
        return Ok(Vec::new());
    };
    let snap = m
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found in table metadata"))?;
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = ManifestList::parse_with_version(&bytes, m.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;

    let mut out = Vec::new();
    for entry in list.entries() {
        if !manifest_filter(entry) {
            continue;
        }
        let partition_spec_id = entry.partition_spec_id;
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
                out.push(LiveDataFileEntry {
                    data_file,
                    sequence_number: seq,
                    file_sequence_number: file_seq,
                    partition_spec_id,
                });
            }
        }
    }
    Ok(out)
}

fn group_live_data_entries_by_partition_spec(
    entries: &[LiveDataFileEntry],
) -> BTreeMap<i32, Vec<LiveDataFileEntry>> {
    let mut grouped = BTreeMap::new();
    for entry in entries {
        grouped
            .entry(entry.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(entry.clone());
    }
    grouped
}

fn live_data_entries_as_delete_entries(
    entries: &[LiveDataFileEntry],
) -> Vec<(DataFile, i64, Option<i64>)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )
        })
        .collect()
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
    use crate::iceberg::spec::DataFileBuilder;
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
    if !f.lower_bounds.is_empty() {
        builder.lower_bounds(f.lower_bounds.clone());
    }
    if !f.upper_bounds.is_empty() {
        builder.upper_bounds(f.upper_bounds.clone());
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
    p.insert(
        "removed-files-size".to_string(),
        deleted
            .iter()
            .map(|(df, _, _)| df.file_size_in_bytes())
            .sum::<u64>()
            .to_string(),
    );
    p
}

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}

#[allow(dead_code)]
fn _check_status_variant_referenced() {
    let _ = ManifestStatus::Deleted;
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use super::*;
    use crate::commit::CommitOpKind;
    use crate::commit::collector::IcebergCommitCollector;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, Type as IcebergType,
    };
    use crate::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    struct LocalTableFixture {
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        _warehouse: tempfile::TempDir,
    }

    async fn empty_local_table(format_version: FormatVersion) -> LocalTableFixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = local_test_binding();
        let catalog: Arc<dyn Catalog> = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri,
                binding,
            ),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                IcebergType::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(format_version)
                    .build(),
            )
            .await
            .expect("create table");
        LocalTableFixture {
            catalog,
            table_ident: TableIdent::new(namespace, "t".to_string()),
            _warehouse: warehouse,
        }
    }

    /// An overwrite with no written files over an empty base stages no updates
    /// at all. `submit_action_commit` recognises that as a proven no-op and
    /// never reaches the catalog, so the reported snapshot id must stay the
    /// unchanged target ref — 0 while `main` does not exist yet — exactly as it
    /// was before the fenced-submit cut-over.
    #[tokio::test]
    async fn empty_overwrite_over_an_empty_base_reports_snapshot_zero_without_committing() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table");
        let metadata = table.metadata().clone();
        assert!(
            metadata.current_snapshot().is_none(),
            "fixture must start with no snapshot"
        );
        let collector = IcebergCommitCollector::new(
            CommitOpKind::Overwrite,
            fixture.table_ident.clone(),
            None,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/data/_staging/test", metadata.location()),
        );
        let snapshot_properties = BTreeMap::new();
        let abort_handle = collector.abort_log.clone();
        let outcome = OverwriteCommit
            .commit(CommitCtx {
                collector: &collector,
                table: &table,
                catalog: fixture.catalog.as_ref(),
                file_io: table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle,
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            })
            .await
            .expect("empty overwrite must succeed as a no-op");

        assert_eq!(
            outcome.new_snapshot_id, 0,
            "a no-op overwrite over a ref that does not exist reports snapshot id 0"
        );
        assert!(
            outcome.written_manifest_paths.is_empty(),
            "a no-op overwrite writes no manifests, got {:?}",
            outcome.written_manifest_paths
        );
        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table");
        assert!(
            reloaded.metadata().current_snapshot().is_none(),
            "a no-op overwrite must not publish a snapshot"
        );
    }

    /// The same no-op over a base that already has a snapshot reports that
    /// snapshot id back, unchanged.
    #[tokio::test]
    async fn empty_overwrite_over_a_populated_base_reports_the_previous_snapshot_id() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table");
        let metadata = table.metadata().clone();
        // Publish one data-free snapshot so `main` exists and the overwrite has
        // a previous snapshot id to report back.
        let marker_collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            fixture.table_ident.clone(),
            None,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/data/_staging/test", metadata.location()),
        );
        let marker_properties =
            BTreeMap::from([("novarocks.test.marker".to_string(), "true".to_string())]);
        let marker_abort = marker_collector.abort_log.clone();
        let marker = super::super::fast_append::commit_empty_iceberg_mv_snapshot(CommitCtx {
            collector: &marker_collector,
            table: &table,
            catalog: fixture.catalog.as_ref(),
            file_io: table.file_io(),
            commit_uuid: Uuid::now_v7(),
            abort_handle: marker_abort,
            target_ref: "main",
            snapshot_properties: &marker_properties,
        })
        .await
        .expect("publish the marker snapshot");
        assert_ne!(marker.new_snapshot_id, 0);

        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after marker");
        let metadata = table.metadata().clone();
        let collector = IcebergCommitCollector::new(
            CommitOpKind::Overwrite,
            fixture.table_ident.clone(),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{}/data/_staging/test", metadata.location()),
        );
        let snapshot_properties = BTreeMap::new();
        let abort_handle = collector.abort_log.clone();
        let outcome = OverwriteCommit
            .commit(CommitCtx {
                collector: &collector,
                table: &table,
                catalog: fixture.catalog.as_ref(),
                file_io: table.file_io(),
                commit_uuid: Uuid::now_v7(),
                abort_handle,
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            })
            .await
            .expect("empty overwrite over a populated base must succeed");

        // The base is not empty here, so the action stages a real overwrite
        // snapshot rather than short-circuiting; either way the commit must
        // report a visible target-ref snapshot, never 0.
        assert_ne!(
            outcome.new_snapshot_id, 0,
            "an overwrite over an existing ref never reports snapshot id 0"
        );
    }
}
