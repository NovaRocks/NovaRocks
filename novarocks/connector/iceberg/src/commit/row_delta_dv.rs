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

//! `RowDeltaDvCommit` — the v3 row-lineage DELETE commit-action.
//!
//! Where Phase-1 `RowDeltaCommit` produces v2 Parquet position-delete files,
//! this action consumes the engine-produced `(referenced_data_file,
//! positions)` groups directly and:
//!
//! 1. Builds one [`DeletionVector`] per touched data file.
//! 2. Walks the base manifest list. For every delete manifest that contains
//!    a Puffin DV blob whose `referenced_data_file` is in the touched set,
//!    the existing DV is read and merged into the in-memory bitmap; the
//!    enclosing manifest is rewritten so live entries that were NOT touched
//!    are carried forward as `Existing`. Untouched delete manifests and all
//!    data manifests are kept verbatim.
//! 3. Writes one single-blob Puffin DV per touched data file via
//!    [`write_single_deletion_vector_puffin`].
//! 4. Writes new delete manifests (`*-row-delta-dv-added-*.avro`), grouped by
//!    the referenced data file's partition spec, that record the new Puffin DV files with
//!    `content=PositionDeletes`, `file_format=Puffin`, and the
//!    `referenced-data-file` / `content_offset` / `content_size_in_bytes`
//!    fields required by the Iceberg v3 spec.
//! 5. Writes a v3 manifest list whose `first_row_id` carries through the
//!    table's current `next-row-id` (DELETE adds no new rows, so no
//!    advance) and a `Snapshot` whose `summary.operation = "delete"` and
//!    `row_range = (next_row_id, 0)`.
//! 6. Returns an `ActionCommit` with the standard
//!    `AddSnapshot + SetSnapshotRef` updates and OCC requirements.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, FormatVersion, ManifestFile, Operation, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{ActionCommit, TransactionAction};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::fast_append::commit_empty_iceberg_mv_snapshot;
use super::helpers::{
    OccSubmit, effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id,
    metadata_dir, now_ms, required_target_ref_snapshot_id, snapshot_summary,
    snapshot_total_records, submit_occ_action, target_ref_snapshot_id, write_manifest_list,
};
use super::row_delta_dv_metadata::{
    WrittenDvFile, build_snapshot_index_with_dv_merge, dv_summary, dv_total_records,
    group_live_files_by_partition_spec, group_written_dvs_by_partition_spec, partition_spec_by_id,
    to_iceberg_unexpected, write_added_dv_manifest, write_existing_delete_manifest,
};
use crate::commit::abort::AbortLog;
use crate::commit::{CommitOutcome, WrittenFile};
use crate::commit::{DeletionVector, PositionDeleteGroup, write_single_deletion_vector_puffin};

pub struct RowDeltaDvCommit;

#[async_trait]
impl IcebergCommitAction for RowDeltaDvCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let groups = ctx.collector.take_delete_groups();
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "RowDeltaDvCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }
        if groups.iter().all(|g| g.positions.is_empty()) && written.is_empty() {
            return commit_empty_iceberg_mv_snapshot(ctx).await;
        }

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = Arc::new(RowDeltaDvTxnAction {
            groups,
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            row_lineage_first_row_id: effective_next_row_id(ctx.table.metadata())?,
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
        });

        let written_manifest_paths = || {
            manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .clone()
        };
        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);

        match submit_occ_action(ctx.catalog, ctx.table, action, "RowDeltaDv", None).await {
            Ok(OccSubmit::Committed(table_after)) => {
                let new_snapshot_id = required_target_ref_snapshot_id(
                    table_after.metadata(),
                    ctx.target_ref,
                    "RowDeltaDv",
                )?;
                Ok(CommitOutcome {
                    new_snapshot_id,
                    written_manifest_paths: written_manifest_paths(),
                })
            }
            // Fully empty input already returned through
            // `commit_empty_iceberg_mv_snapshot` above, and the action always
            // stages a snapshot, so this arm reports the same value that path
            // reports for an empty change set.
            Ok(OccSubmit::NoOp) => Ok(CommitOutcome {
                new_snapshot_id: prev_snapshot_id.unwrap_or(0),
                written_manifest_paths: written_manifest_paths(),
            }),
            Err(error) => Err(error.into_detail()),
        }
    }
}

struct RowDeltaDvTxnAction {
    groups: Vec<PositionDeleteGroup>,
    /// Replacement data files produced by an MOR UPDATE. Empty for plain
    /// DELETE. Each file already carries stored row-lineage columns so the
    /// snapshot must NOT allocate fresh row IDs for them — the added-data
    /// manifest is marked with `first_row_id` to suppress allocation.
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    schema: SchemaRef,
    schema_id: i32,
    /// `next_row_id` of the base table — neither DELETE nor MOR UPDATE
    /// allocate fresh row IDs (UPDATE rows reuse the matched `_row_id`s),
    /// so the snapshot's `row_range = (first_row_id, 0)`.
    row_lineage_first_row_id: u64,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
}

#[async_trait]
impl TransactionAction for RowDeltaDvTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version != FormatVersion::V3 {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "RowDeltaDvCommit requires an Iceberg v3 table",
            ));
        }
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);

        let mut vectors = groups_to_vectors(&self.groups).map_err(to_iceberg_unexpected)?;
        let touched_files: HashSet<String> = vectors.keys().cloned().collect();
        let index = build_snapshot_index_with_dv_merge(
            table,
            &self.file_io,
            &touched_files,
            &mut vectors,
            target_ref,
        )
        .await
        .map_err(to_iceberg_unexpected)?;

        for referenced in vectors.keys() {
            if !index.data_files.contains_key(referenced) {
                return Err(to_iceberg_unexpected(format!(
                    "row-lineage DELETE referenced data file `{referenced}` is not present in the current snapshot"
                )));
            }
        }

        let mut written_dvs = Vec::with_capacity(vectors.len());
        for (idx, (referenced, dv)) in vectors.iter().enumerate() {
            let path = format!(
                "{}/data/_staging/{}/dv-{:08x}.puffin",
                m.location(),
                self.commit_uuid,
                idx
            );
            self.abort_handle.record_data_file(path.clone());
            let written = write_single_deletion_vector_puffin(&self.file_io, &path, referenced, dv)
                .await
                .map_err(|e| to_iceberg_unexpected(e.to_string()))?;
            written_dvs.push(WrittenDvFile::from(written));
        }

        let mut new_manifests = index.untouched_manifests;
        for (idx, (spec_id, files)) in
            group_live_files_by_partition_spec(index.touched_delete_existing)
                .into_iter()
                .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-row-delta-dv-existing-{idx}.avro",
                self.commit_uuid,
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_existing_delete_manifest(
                &self.file_io,
                &path,
                &files,
                partition_spec_by_id(m, spec_id)?,
                self.schema.clone(),
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        for (idx, (spec_id, dvs)) in
            group_written_dvs_by_partition_spec(&written_dvs, &index.data_files)
                .map_err(to_iceberg_unexpected)?
                .into_iter()
                .enumerate()
        {
            let added_path = format!(
                "{metadata_dir}/{}-row-delta-dv-added-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(added_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(added_path.clone());
            let added = write_added_dv_manifest(
                &self.file_io,
                &added_path,
                &dvs,
                &index.data_files,
                partition_spec_by_id(m, spec_id)?,
                self.schema.clone(),
                new_seq,
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(added);
        }

        if !self.written.is_empty() {
            let data_path = format!(
                "{metadata_dir}/{}-row-delta-update-data-0.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(data_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(data_path.clone());
            let data_manifest = super::overwrite::write_added_data_manifest(
                &self.file_io,
                &data_path,
                &self.written,
                m.default_partition_spec().clone(),
                self.schema.clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            // The replacement data files reuse the matched rows' `_row_id`s
            // (stored in the row-lineage columns). Mark the manifest as
            // already-assigned so the v3 manifest-list writer does NOT
            // allocate fresh row IDs for them.
            new_manifests.push(mark_replacement_manifest_row_id_assigned(
                data_manifest,
                self.row_lineage_first_row_id,
            ));
        }

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
            Some(self.row_lineage_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if manifest_list_next_row_id != Some(self.row_lineage_first_row_id) {
            return Err(to_iceberg_unexpected(format!(
                "row-lineage DELETE/MOR-UPDATE must not allocate row IDs: \
                 expected next-row-id {}, got {manifest_list_next_row_id:?}",
                self.row_lineage_first_row_id
            )));
        }

        let added_position_deletes = written_dvs.iter().try_fold(0u64, |sum, dv| {
            sum.checked_add(dv.cardinality)
                .ok_or_else(|| to_iceberg_unexpected("DV cardinality overflow".to_string()))
        })?;
        let newly_deleted_records = added_position_deletes
            .checked_sub(index.replaced_delete_records)
            .ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "DV delete summary underflow: added_position_deletes={added_position_deletes}, replaced_position_deletes={}",
                    index.replaced_delete_records
                ))
            })?;
        let added_data_records = self.written.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.record_count).ok_or_else(|| {
                to_iceberg_unexpected("DV added data record count overflow".to_string())
            })
        })?;
        let total_records = dv_total_records(
            snapshot_total_records(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?,
            newly_deleted_records,
            added_data_records,
        )
        .map_err(to_iceberg_unexpected)?;

        let mut dv_props = dv_summary(
            &written_dvs,
            &self.written,
            total_records,
            newly_deleted_records,
            index.replaced_delete_files,
            index.replaced_delete_records,
        )
        .map_err(to_iceberg_unexpected)?;
        if index.replaced_delete_files_size > 0 {
            dv_props.insert(
                "removed-files-size".to_string(),
                index.replaced_delete_files_size.to_string(),
            );
        }
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let summary_props = merge_snapshot_summary_properties(
            finalize_snapshot_summary(dv_props, parent_summary, false),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: summary_props,
            })
            .with_schema_id(self.schema_id)
            .with_row_range(self.row_lineage_first_row_id, 0)
            .build();

        Ok(ActionCommit::new(
            vec![
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
            ],
            vec![
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
            ],
        ))
    }
}

fn groups_to_vectors(
    groups: &[PositionDeleteGroup],
) -> Result<HashMap<String, DeletionVector>, String> {
    let mut out: HashMap<String, DeletionVector> = HashMap::new();
    for g in groups {
        if g.positions.is_empty() {
            continue;
        }
        let dv = out.entry(g.referenced_data_file.clone()).or_default();
        for pos in &g.positions {
            if *pos < 0 {
                return Err(format!(
                    "row-lineage DELETE position must be non-negative; got {pos} for `{}`",
                    g.referenced_data_file
                ));
            }
            dv.insert(*pos as u64).map_err(|e| e.to_string())?;
        }
    }
    Ok(out)
}

fn mark_replacement_manifest_row_id_assigned(
    mut manifest: ManifestFile,
    row_lineage_first_row_id: u64,
) -> ManifestFile {
    // MOR UPDATE replacement files carry stored row-lineage columns. The
    // manifest first-row-id is assigned only to prevent the v3 manifest-list
    // writer from allocating new row IDs for those replacement rows.
    manifest.first_row_id = Some(row_lineage_first_row_id);
    manifest
}
