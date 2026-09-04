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

//! `OverwritePartitionsCommit` — `INSERT OVERWRITE PARTITIONS` semantics.
//!
//! Replace only the partitions touched by the new data; preserve all other
//! partitions. v3 row-lineage tables only.
//!
//! Differences from `OverwriteCommit`:
//! * The base files marked DELETED are restricted to those whose partition
//!   tuple appears in the set of new files' partition tuples (under the
//!   current partition spec).
//! * Cross-historical-spec base files are rejected with a hint to run
//!   `OPTIMIZE TABLE` first; see spec §10.1 R2 and
//!   `partition_match_in_touched` (`partition_spec.rs`).
//! * Empty SELECT result is a noop overwrite snapshot — same audit-trail
//!   behavior as `TruncateCommit` empty-table.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestList,
    ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference,
    SnapshotRetention, Struct, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{ActionCommit, TransactionAction};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::data_file::clone_data_file_with_first_row_id;
use super::helpers::{
    OccSubmit, effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id,
    metadata_dir, now_ms, required_target_ref_snapshot_id, snapshot_summary, submit_occ_action,
    target_ref_snapshot_id, write_manifest_list,
};
use super::overwrite::{
    write_added_data_manifest, write_overwrite_deletes_manifest, write_truncate_deletes_manifest,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionMatch {
    InSet,
    NotInSet,
    DifferentSpec,
}

fn partition_match_in_touched(
    base: &crate::iceberg::spec::Struct,
    base_spec_id: i32,
    current_spec_id: i32,
    touched: &[crate::iceberg::spec::Struct],
) -> PartitionMatch {
    if base_spec_id != current_spec_id {
        PartitionMatch::DifferentSpec
    } else if touched.iter().any(|candidate| candidate == base) {
        PartitionMatch::InSet
    } else {
        PartitionMatch::NotInSet
    }
}
use crate::commit::abort::AbortLog;
use crate::commit::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct OverwritePartitionsCommit;

#[async_trait]
impl IcebergCommitAction for OverwritePartitionsCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Reject non-Data content; the engine should never produce these here.
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "OverwritePartitionsCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }

        // Require v3 row-lineage table.
        match crate::commit::classify_iceberg_write_mode(ctx.table) {
            IcebergWriteMode::RowLineageV3 => {}
            IcebergWriteMode::LegacyPositionDeletes => {
                // A local read of the table's own metadata, before anything is
                // staged or sent. Without the marker this fell through the
                // substring classifier, matched none of its signals, and was
                // reported as an unknown publication -- so a statement that
                // provably did nothing told the caller it could not tell, and
                // left its staged files behind for review.
                return Err(format!(
                    "{} OverwritePartitionsCommit requires v3 row-lineage table",
                    crate::commit::service::PROVEN_UNCOMMITTED_MARKER
                ));
            }
        }

        let row_lineage_first_row_id = Some(effective_next_row_id(ctx.table.metadata())?);
        let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
            sum.checked_add(f.record_count)
                .ok_or_else(|| "row-lineage added row count overflow".to_string())
        })?;

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = Arc::new(OverwritePartitionsTxnAction {
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

        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);
        let written_manifest_paths = || {
            manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .clone()
        };

        match submit_occ_action(ctx.catalog, ctx.table, action, "OverwritePartitions", None).await {
            Ok(OccSubmit::Committed(table_after)) => {
                let new_snapshot_id = match required_target_ref_snapshot_id(
                    table_after.metadata(),
                    ctx.target_ref,
                    "OverwritePartitions",
                ) {
                    Ok(snapshot_id) => snapshot_id,
                    Err(_) if prev_snapshot_id.is_none() => 0,
                    Err(err) => return Err(err),
                };
                Ok(CommitOutcome {
                    new_snapshot_id,
                    written_manifest_paths: written_manifest_paths(),
                })
            }
            // The action always stages a snapshot, so this arm reports the same
            // outcome an unchanged target ref has: the previous snapshot id, or
            // 0 when the ref does not exist yet.
            Ok(OccSubmit::NoOp) => Ok(CommitOutcome {
                new_snapshot_id: prev_snapshot_id.unwrap_or(0),
                written_manifest_paths: written_manifest_paths(),
            }),
            Err(error) => Err(error.into_detail()),
        }
    }
}

struct OverwritePartitionsTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    /// Row-lineage first_row_id for the new files (always Some for v3 tables).
    row_lineage_first_row_id: Option<u64>,
    /// Sum of record_count across all written files.
    row_lineage_added_rows: u64,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
}

#[derive(Clone)]
pub(super) struct LiveFileWithSpec {
    pub(super) data_file: DataFile,
    pub(super) snapshot_id: i64,
    pub(super) sequence_number: i64,
    pub(super) file_sequence_number: Option<i64>,
    pub(super) manifest_spec_id: i32,
    pub(super) effective_first_row_id: Option<i64>,
}

impl OverwritePartitionsTxnAction {
    fn record_manifest_path(&self, path: String) {
        self.abort_handle.record_manifest(path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(path);
    }
}

#[async_trait]
impl TransactionAction for OverwritePartitionsTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version == FormatVersion::V1 {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "OverwritePartitionsCommit does not support V1 tables",
            ));
        }

        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);

        // 1. Compute the set of touched partitions from written files.
        //    Use HashSet deduplication (Struct: Hash + Eq confirmed in Task 5).
        let current_spec_id = self.partition_spec.spec_id();
        let touched: Vec<Struct> = {
            let mut seen: HashSet<Struct> = HashSet::new();
            for f in &self.written {
                seen.insert(f.partition_values.clone());
            }
            seen.into_iter().collect()
        };

        // 2. Walk all live base entries (Data + Deletes) and classify:
        //    - InSet  → touched, will be marked DELETED in the new snapshot
        //    - NotInSet → surviving, must be re-written as EXISTING in the new snapshot
        //    - DifferentSpec → reject the whole commit
        //
        //    We do NOT inherit base manifests because `enumerate_live_all_files`
        //    (and the test's post-commit re-enumeration) does a simple "is_alive"
        //    scan — it does not de-duplicate EXISTING vs DELETED entries across
        //    manifests. The only safe approach is to emit each file exactly once
        //    in the new manifest list:
        //    • surviving files → new EXISTING manifest
        //    • touched files   → new DELETED manifest
        //    • new files       → new ADDED manifest
        let existing = enumerate_live_all_files_with_spec_at_snapshot(
            table,
            &self.file_io,
            parent_snapshot_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        // `(DataFile, seq, file_seq)` tuples split by fate.
        let mut deleted_data: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
        let mut deleted_deletes: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
        // `(DataFile, snap_id, seq, file_seq, effective_first_row_id, spec_id)`.
        let mut surviving_data: Vec<(DataFile, i64, i64, Option<i64>, Option<i64>, i32)> =
            Vec::new();
        let mut surviving_deletes: Vec<(DataFile, i64, i64, Option<i64>)> = Vec::new();
        for live in &existing {
            let df = &live.data_file;
            match partition_match_in_touched(
                df.partition(),
                live.manifest_spec_id,
                current_spec_id,
                &touched,
            ) {
                PartitionMatch::InSet => {
                    if df.content_type() == DataContentType::Data {
                        deleted_data.push((
                            df.clone(),
                            live.sequence_number,
                            live.file_sequence_number,
                        ));
                    } else {
                        deleted_deletes.push((
                            df.clone(),
                            live.sequence_number,
                            live.file_sequence_number,
                        ));
                    }
                }
                PartitionMatch::NotInSet => {
                    if df.content_type() == DataContentType::Data {
                        surviving_data.push((
                            df.clone(),
                            live.snapshot_id,
                            live.sequence_number,
                            live.file_sequence_number,
                            live.effective_first_row_id,
                            live.manifest_spec_id,
                        ));
                    } else {
                        surviving_deletes.push((
                            df.clone(),
                            live.snapshot_id,
                            live.sequence_number,
                            live.file_sequence_number,
                        ));
                    }
                }
                PartitionMatch::DifferentSpec => {
                    return Err(to_iceberg_unexpected(format!(
                        "OVERWRITE PARTITIONS: base file under historical partition spec \
                         {base_spec_id} cannot be matched against current spec \
                         {current_spec_id}; run OPTIMIZE TABLE to consolidate first",
                        base_spec_id = live.manifest_spec_id,
                    )));
                }
            }
        }

        // 3. Write manifests. Order: surviving → deleted → added.
        let mut new_manifests: Vec<crate::iceberg::spec::ManifestFile> = Vec::new();

        // 3a. EXISTING-Data manifest for surviving data files.
        if !surviving_data.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-partitions-surviving-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_existing_data_manifest(
                &self.file_io,
                &path,
                &surviving_data,
                self.row_lineage_first_row_id,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3b. EXISTING-Deletes manifest for surviving delete files.
        if !surviving_deletes.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-partitions-surviving-deletes-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_existing_deletes_manifest(
                &self.file_io,
                &path,
                &surviving_deletes,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3c. DELETED-Data manifest for touched data files.
        if !deleted_data.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-partitions-deleted-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_overwrite_deletes_manifest(
                &self.file_io,
                &path,
                &deleted_data,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3d. DELETED-Deletes manifest for touched delete files.
        if !deleted_deletes.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-partitions-deleted-deletes-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_truncate_deletes_manifest(
                &self.file_io,
                &path,
                &deleted_deletes,
                self.partition_spec.clone(),
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        // 3e. ADDED-Data manifest for the new files.
        if !self.written.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-overwrite-partitions-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
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

        // 5. Write the manifest list.
        //    Surviving data manifests are pre-marked as assigned so they do
        //    not consume fresh row IDs. Only newly added files advance the
        //    writer's next_row_id, matching the snapshot row range below.
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.record_manifest_path(manifest_list_path.clone());
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
                        "Row ID overflow when computing overwrite partitions row lineage range: first_row_id={first_row_id}, added_rows={}",
                        self.row_lineage_added_rows
                    ))
                })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch for overwrite partitions: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        // 6. Build the snapshot.
        //    operation = Overwrite, summary includes `replace-partitions=true`.
        //    Row-range advances next_row_id by added_rows_count (even if zero
        //    when written is empty — the validator still requires a non-null
        //    first-row-id for V3).
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: merge_snapshot_summary_properties(
                finalize_snapshot_summary(
                    overwrite_partitions_summary(&self.written, &deleted_data, &deleted_deletes),
                    parent_summary,
                    false,
                ),
                &self.snapshot_properties,
            )
            .map_err(to_iceberg_unexpected)?,
        };
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
            // Non-V3 path: format_version was validated != V1 above; this
            // branch handles V2 tables that somehow reach here (they would
            // have been rejected by the v3 check in `commit()`, but the
            // TxnAction must remain safe regardless).
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

        // 7. Build TableUpdate / TableRequirement set.
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

/// Write a Data manifest in which every entry is EXISTING (status=Existing).
/// Used by `OverwritePartitionsCommit` to emit surviving data files (those in
/// non-touched partitions) into the new snapshot's manifest list.
type SurvivingDataFile = (DataFile, i64, i64, Option<i64>, Option<i64>, i32);

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_existing_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    surviving: &[SurvivingDataFile],
    assigned_manifest_first_row_id: Option<u64>,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<crate::iceberg::spec::ManifestFile, String> {
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
    for (df, snap_id, seq, file_seq, effective_first_row_id, spec_id) in surviving {
        let fseq = file_seq.unwrap_or(*seq);
        if format_version == FormatVersion::V3 && effective_first_row_id.is_none() {
            return Err(format!(
                "missing effective first_row_id for surviving data file {}",
                df.file_path()
            ));
        }
        let data_file = clone_data_file_with_first_row_id(df, *spec_id, *effective_first_row_id)?;
        writer
            .add_existing_file(data_file, *snap_id, *seq, Some(fseq))
            .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    }
    let mut manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    if format_version == FormatVersion::V3 {
        manifest_file.first_row_id = assigned_manifest_first_row_id;
    }
    Ok(manifest_file)
}

/// Write a Deletes manifest in which every entry is EXISTING (status=Existing).
/// Used by `OverwritePartitionsCommit` to preserve surviving delete files
/// (position-delete / equality-delete / DV) in non-touched partitions.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_existing_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    surviving: &[(DataFile, i64, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<crate::iceberg::spec::ManifestFile, String> {
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
        FormatVersion::V1 => return Err("phase 1 does not support V1 tables".to_string()),
    };
    for (df, snap_id, seq, file_seq) in surviving {
        let fseq = file_seq.unwrap_or(*seq);
        writer
            .add_existing_file(df.clone(), *snap_id, *seq, Some(fseq))
            .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    }
    writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))
}

/// Walk every manifest in the base snapshot's manifest list (Data and
/// Deletes alike) and collect every live entry's
/// data file, inherited sequence fields, source spec id, and effective row id.
///
/// - `snapshot_id`: the snapshot that originally wrote this entry (needed by
///   `add_existing_file` to preserve lineage faithfully).
/// - `manifest_spec_id`: the manifest-level `partition_spec_id` so that
///   OVERWRITE PARTITIONS can detect cross-spec base files.
#[allow(dead_code)]
async fn enumerate_live_all_files_with_spec(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<LiveFileWithSpec>, String> {
    let snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    enumerate_live_all_files_with_spec_at_snapshot(table, file_io, snapshot_id).await
}

pub(super) async fn enumerate_live_all_files_with_spec_at_snapshot(
    table: &Table,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
) -> Result<Vec<LiveFileWithSpec>, String> {
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
        let spec_id = entry.partition_spec_id;
        let manifest_snap_id = entry.added_snapshot_id;
        let is_data_manifest = entry.content == ManifestContentType::Data;
        let mut next_manifest_first_row_id = if is_data_manifest {
            entry
                .first_row_id
                .map(|v| {
                    i64::try_from(v).map_err(|_| format!("manifest first_row_id too large: {v}"))
                })
                .transpose()?
        } else {
            None
        };
        let manifest = entry
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load_manifest({}) failed: {e}", entry.manifest_path))?;
        for me in manifest.entries() {
            if me.is_alive() {
                let data_file = me.data_file().clone();
                let seq = me.sequence_number().unwrap_or(entry.sequence_number);
                let file_seq = me.file_sequence_number;
                let snap_id = me.snapshot_id().unwrap_or(manifest_snap_id);
                let effective_first_row_id = if is_data_manifest
                    && data_file.content_type() == DataContentType::Data
                {
                    let record_count = i64::try_from(data_file.record_count()).map_err(|_| {
                        format!("record_count too large for {}", data_file.file_path())
                    })?;
                    let first_row_id = data_file.first_row_id().or(next_manifest_first_row_id);
                    if let Some(next) = next_manifest_first_row_id.as_mut() {
                        *next = next.checked_add(record_count).ok_or_else(|| {
                            format!("first_row_id overflow for manifest {}", entry.manifest_path)
                        })?;
                    }
                    first_row_id
                } else {
                    None
                };
                out.push(LiveFileWithSpec {
                    data_file,
                    snapshot_id: snap_id,
                    sequence_number: seq,
                    file_sequence_number: file_seq,
                    manifest_spec_id: spec_id,
                    effective_first_row_id,
                });
            }
        }
    }
    Ok(out)
}

fn overwrite_partitions_summary(
    written: &[WrittenFile],
    deleted_data: &[(DataFile, i64, Option<i64>)],
    deleted_deletes: &[(DataFile, i64, Option<i64>)],
) -> HashMap<String, String> {
    let added_records: u64 = written.iter().map(|f| f.record_count).sum();
    let added_files_size: u64 = written.iter().map(|f| f.file_size_in_bytes).sum();
    let deleted_records: u64 = deleted_data
        .iter()
        .map(|(df, _, _)| df.record_count())
        .sum();
    let removed_files_size: u64 = deleted_data
        .iter()
        .chain(deleted_deletes.iter())
        .map(|(df, _, _)| df.file_size_in_bytes())
        .sum();
    let removed_position_delete_files = deleted_deletes
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .count();
    let removed_equality_delete_files = deleted_deletes
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .count();

    let removed_position_deletes: u64 = deleted_deletes
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum();
    let removed_equality_deletes: u64 = deleted_deletes
        .iter()
        .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
        .map(|(df, _, _)| df.record_count())
        .sum();

    let mut p = HashMap::new();
    p.insert("replace-partitions".to_string(), "true".to_string());
    p.insert("added-data-files".to_string(), written.len().to_string());
    p.insert("added-records".to_string(), added_records.to_string());
    p.insert("added-files-size".to_string(), added_files_size.to_string());
    p.insert(
        "deleted-data-files".to_string(),
        deleted_data.len().to_string(),
    );
    p.insert("deleted-records".to_string(), deleted_records.to_string());
    p.insert(
        "removed-files-size".to_string(),
        removed_files_size.to_string(),
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
        "removed-delete-files".to_string(),
        deleted_deletes.len().to_string(),
    );
    p.insert(
        "removed-position-deletes".to_string(),
        removed_position_deletes.to_string(),
    );
    p.insert(
        "removed-equality-deletes".to_string(),
        removed_equality_deletes.to_string(),
    );
    p
}

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}
