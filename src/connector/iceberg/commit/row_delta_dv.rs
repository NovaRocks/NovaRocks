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

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    ManifestFile, ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    current_snapshot_total_records, effective_next_row_id, generate_snapshot_id, metadata_dir,
    now_ms, write_manifest_list,
};
use super::position_delete_writer::PositionDeleteGroup;
use super::puffin_dv::{
    DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
use super::types::{CommitOutcome, WrittenFile};

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

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = RowDeltaDvTxnAction {
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
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("RowDeltaDv apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("RowDeltaDv commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "RowDeltaDv committed but new snapshot is not visible".to_string())?;
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
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version != FormatVersion::V3 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "RowDeltaDvCommit requires an Iceberg v3 table",
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

        let mut vectors = groups_to_vectors(&self.groups).map_err(to_iceberg_unexpected)?;
        let touched_files: HashSet<String> = vectors.keys().cloned().collect();
        let index = build_snapshot_index(
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
            written_dvs.push(written);
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
            current_snapshot_total_records(m).map_err(to_iceberg_unexpected)?,
            newly_deleted_records,
            added_data_records,
        )
        .map_err(to_iceberg_unexpected)?;

        let summary_props = merge_snapshot_summary_properties(
            dv_summary(
                &written_dvs,
                &self.written,
                total_records,
                newly_deleted_records,
                index.replaced_delete_files,
                index.replaced_delete_records,
            ),
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

struct LiveFile {
    data_file: DataFile,
    partition_spec_id: i32,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

struct SnapshotIndex {
    /// Live data files keyed by `file_path()`.
    data_files: HashMap<String, LiveFile>,
    /// Manifests we did NOT touch; preserved verbatim in the new manifest list.
    untouched_manifests: Vec<ManifestFile>,
    /// Live delete entries from touched delete manifests that the current
    /// DELETE did not affect (i.e., reference some other data file). They are
    /// rewritten into a new `*-row-delta-dv-existing-*.avro` so the DV
    /// lineage is preserved for unrelated data files.
    touched_delete_existing: Vec<LiveFile>,
    /// Live DV files removed because a replacement DV was written.
    replaced_delete_files: usize,
    /// Position deletes already represented by removed DV files.
    replaced_delete_records: u64,
}

async fn build_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    vectors: &mut HashMap<String, DeletionVector>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    let mut data_files = HashMap::new();
    let mut untouched_manifests = Vec::new();
    let mut touched_delete_existing = Vec::new();
    let mut replaced_delete_files = 0usize;
    let mut replaced_delete_vectors: HashMap<String, DeletionVector> = HashMap::new();
    let m = table.metadata();
    // For branch-targeted deletes, read the manifest list from the branch head
    // snapshot (not from main's current snapshot). This ensures that files added
    // to the branch by prior branch DML are visible and carry forward correctly.
    let snapshot = if target_ref == "main" {
        m.current_snapshot()
            .ok_or_else(|| "row-lineage DELETE requires a current snapshot".to_string())?
    } else {
        let branch_snapshot_id = m.refs().get(target_ref).map(|r| r.snapshot_id).ok_or_else(
            || {
                format!(
                    "row-lineage DELETE target branch '{target_ref}' not found in table metadata"
                )
            },
        )?;
        m.snapshot_by_id(branch_snapshot_id)
            .ok_or_else(|| format!("row-lineage DELETE branch '{target_ref}' snapshot {branch_snapshot_id} not found in metadata"))?
    };
    let list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    for mf in list.entries() {
        match mf.content {
            ManifestContentType::Data => {
                let manifest = mf
                    .load_manifest(file_io)
                    .await
                    .map_err(|e| format!("load data manifest {} failed: {e}", mf.manifest_path))?;
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let seq = entry.sequence_number().unwrap_or(mf.sequence_number);
                    let file_seq = entry.file_sequence_number;
                    let snapshot_id = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
                    let file = entry.data_file().clone();
                    data_files.insert(
                        file.file_path().to_string(),
                        LiveFile {
                            data_file: file,
                            partition_spec_id: mf.partition_spec_id,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                        },
                    );
                }
                untouched_manifests.push(mf.clone());
            }
            ManifestContentType::Deletes => {
                let manifest = mf.load_manifest(file_io).await.map_err(|e| {
                    format!("load delete manifest {} failed: {e}", mf.manifest_path)
                })?;
                let mut manifest_touched = false;
                let mut keep: Vec<LiveFile> = Vec::new();
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let seq = entry.sequence_number().unwrap_or(mf.sequence_number);
                    let file_seq = entry.file_sequence_number;
                    let snapshot_id = entry.snapshot_id().unwrap_or(mf.added_snapshot_id);
                    let file = entry.data_file().clone();
                    validate_delete_file_for_row_lineage(&file)?;
                    let referenced = file.referenced_data_file().ok_or_else(|| {
                        format!(
                            "Puffin DV {} missing referenced_data_file",
                            file.file_path()
                        )
                    })?;
                    if touched_files.contains(&referenced) {
                        let offset = file.content_offset().ok_or_else(|| {
                            format!("Puffin DV {} missing content_offset", file.file_path())
                        })?;
                        let len = file.content_size_in_bytes().ok_or_else(|| {
                            format!(
                                "Puffin DV {} missing content_size_in_bytes",
                                file.file_path()
                            )
                        })?;
                        let old =
                            read_deletion_vector_puffin(file_io, file.file_path(), offset, len)
                                .await
                                .map_err(|e| {
                                    format!(
                                        "read existing Puffin DV {} failed: {e}",
                                        file.file_path()
                                    )
                                })?;
                        replaced_delete_files += 1;
                        replaced_delete_vectors
                            .entry(referenced.clone())
                            .or_default()
                            .merge(&old);
                        vectors.entry(referenced).or_default().merge(&old);
                        manifest_touched = true;
                    } else {
                        keep.push(LiveFile {
                            data_file: file,
                            partition_spec_id: mf.partition_spec_id,
                            snapshot_id,
                            sequence_number: seq,
                            file_sequence_number: file_seq,
                        });
                    }
                }
                if manifest_touched {
                    touched_delete_existing.extend(keep);
                } else {
                    untouched_manifests.push(mf.clone());
                }
            }
        }
    }

    let replaced_delete_records =
        replaced_delete_vectors
            .values()
            .try_fold(0u64, |sum, vector| {
                sum.checked_add(vector.cardinality())
                    .ok_or_else(|| "replaced DV cardinality overflow".to_string())
            })?;

    Ok(SnapshotIndex {
        data_files,
        untouched_manifests,
        touched_delete_existing,
        replaced_delete_files,
        replaced_delete_records,
    })
}

fn validate_delete_file_for_row_lineage(file: &DataFile) -> Result<(), String> {
    if file.content_type() == DataContentType::EqualityDeletes {
        return Err(
            "row-lineage DELETE does not support equality-delete files; compact them away first"
                .to_string(),
        );
    }
    if file.file_format() != DataFileFormat::Puffin {
        return Err(
            "row-lineage DELETE found v2 position-delete files; compact them away before writing Puffin deletion vectors"
                .to_string(),
        );
    }
    Ok(())
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

fn partition_spec_by_id(
    metadata: &iceberg::spec::TableMetadata,
    spec_id: i32,
) -> iceberg::Result<PartitionSpecRef> {
    metadata
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| {
            to_iceberg_unexpected(format!(
                "row-lineage DELETE references unknown partition spec id {spec_id}"
            ))
        })
}

fn group_live_files_by_partition_spec(files: Vec<LiveFile>) -> BTreeMap<i32, Vec<LiveFile>> {
    let mut grouped = BTreeMap::new();
    for file in files {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file);
    }
    grouped
}

fn group_written_dvs_by_partition_spec(
    dvs: &[WrittenPuffinDv],
    data_files: &HashMap<String, LiveFile>,
) -> Result<BTreeMap<i32, Vec<WrittenPuffinDv>>, String> {
    let mut grouped = BTreeMap::new();
    for dv in dvs {
        let referenced = data_files.get(&dv.referenced_data_file).ok_or_else(|| {
            format!(
                "row-lineage DELETE references data file `{}` which is not in the current snapshot",
                dv.referenced_data_file
            )
        })?;
        grouped
            .entry(referenced.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(dv.clone());
    }
    Ok(grouped)
}

async fn write_existing_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    files: &[LiveFile],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
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
    let mut writer = builder.build_v3_deletes();
    for f in files {
        writer
            .add_existing_file(
                f.data_file.clone(),
                f.snapshot_id,
                f.sequence_number,
                f.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

#[allow(clippy::too_many_arguments)]
async fn write_added_dv_manifest(
    file_io: &FileIO,
    out_path: &str,
    dvs: &[WrittenPuffinDv],
    data_files: &HashMap<String, LiveFile>,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_seq: i64,
    new_snapshot_id: i64,
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
    let mut writer = builder.build_v3_deletes();
    for written in dvs {
        let referenced = data_files.get(&written.referenced_data_file).ok_or_else(|| {
            format!(
                "row-lineage DELETE references data file `{}` which is not in the current snapshot",
                written.referenced_data_file
            )
        })?;
        let df = dv_data_file(written, referenced)?;
        writer
            .add_file(df, new_seq)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Deletes);
    Ok(manifest_file)
}

fn dv_data_file(written: &WrittenPuffinDv, referenced: &LiveFile) -> Result<DataFile, String> {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(written.path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(referenced.data_file.partition().clone())
        .partition_spec_id(referenced.partition_spec_id)
        .record_count(written.cardinality)
        .file_size_in_bytes(written.file_size_in_bytes)
        .referenced_data_file(Some(written.referenced_data_file.clone()))
        .content_offset(Some(written.content_offset))
        .content_size_in_bytes(Some(written.content_size_in_bytes))
        .build()
        .map_err(|e| format!("build DV DataFile failed: {e}"))
}

fn dv_total_records(
    parent_total_records: Option<u64>,
    newly_deleted_records: u64,
    added_data_records: u64,
) -> Result<Option<u64>, String> {
    parent_total_records
        .map(|parent| {
            parent
                .checked_sub(newly_deleted_records)
                .ok_or_else(|| {
                    format!(
                        "DV delete total-records underflow: parent={parent}, deleted={newly_deleted_records}"
                    )
                })?
                .checked_add(added_data_records)
                .ok_or_else(|| {
                    format!(
                        "DV delete total-records overflow: parent={parent}, deleted={newly_deleted_records}, added={added_data_records}"
                    )
                })
        })
        .transpose()
}

fn dv_summary(
    dvs: &[WrittenPuffinDv],
    written_data_files: &[WrittenFile],
    total_records: Option<u64>,
    newly_deleted_records: u64,
    removed_delete_files: usize,
    removed_position_deletes: u64,
) -> HashMap<String, String> {
    let mut p = HashMap::new();
    let added_position_deletes: u64 = dvs.iter().map(|d| d.cardinality).sum();
    let added_data_records: u64 = written_data_files.iter().map(|f| f.record_count).sum();
    let total_size: u64 = dvs
        .iter()
        .map(|d| d.file_size_in_bytes)
        .chain(written_data_files.iter().map(|f| f.file_size_in_bytes))
        .sum();
    p.insert("added-delete-files".to_string(), dvs.len().to_string());
    p.insert(
        "added-position-deletes".to_string(),
        added_position_deletes.to_string(),
    );
    if !written_data_files.is_empty() {
        p.insert(
            "added-data-files".to_string(),
            written_data_files.len().to_string(),
        );
        p.insert("added-records".to_string(), added_data_records.to_string());
    }
    if newly_deleted_records > 0 {
        p.insert(
            "deleted-records".to_string(),
            newly_deleted_records.to_string(),
        );
    }
    if removed_delete_files > 0 {
        p.insert(
            "removed-delete-files".to_string(),
            removed_delete_files.to_string(),
        );
        p.insert(
            "removed-position-delete-files".to_string(),
            removed_delete_files.to_string(),
        );
    }
    if removed_position_deletes > 0 {
        p.insert(
            "removed-position-deletes".to_string(),
            removed_position_deletes.to_string(),
        );
    }
    if let Some(total_records) = total_records {
        p.insert("total-records".to_string(), total_records.to_string());
    }
    p.insert("added-files-size".to_string(), total_size.to_string());
    p
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_delete_file_for_row_lineage_rejects_position_delete_parquet() {
        let file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("file:///tmp/delete.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(iceberg::spec::Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(10)
            .referenced_data_file(Some("file:///tmp/data.parquet".to_string()))
            .build()
            .unwrap();
        let err = validate_delete_file_for_row_lineage(&file).unwrap_err();
        assert!(err.contains("position-delete"));
        assert!(err.contains("compact"));
    }

    #[test]
    fn validate_delete_file_rejects_equality_deletes() {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::EqualityDeletes)
            .file_path("file:///tmp/eq.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(iceberg::spec::Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(10);
        builder.equality_ids(Some(vec![1]));
        let file = builder.build().unwrap();
        let err = validate_delete_file_for_row_lineage(&file).unwrap_err();
        assert!(err.contains("equality-delete"));
    }

    #[test]
    fn groups_to_vectors_merges_duplicate_files() {
        let groups = vec![
            PositionDeleteGroup {
                referenced_data_file: "file:///x/data.parquet".to_string(),
                partition_spec_id: 0,
                partition_values: iceberg::spec::Struct::empty(),
                positions: vec![1, 2],
            },
            PositionDeleteGroup {
                referenced_data_file: "file:///x/data.parquet".to_string(),
                partition_spec_id: 0,
                partition_values: iceberg::spec::Struct::empty(),
                positions: vec![3],
            },
            PositionDeleteGroup {
                referenced_data_file: "file:///x/empty.parquet".to_string(),
                partition_spec_id: 0,
                partition_values: iceberg::spec::Struct::empty(),
                positions: vec![],
            },
        ];
        let vectors = groups_to_vectors(&groups).unwrap();
        assert_eq!(vectors.len(), 1);
        let dv = vectors.get("file:///x/data.parquet").unwrap();
        assert_eq!(dv.cardinality(), 3);
        assert!(dv.contains(1));
        assert!(dv.contains(2));
        assert!(dv.contains(3));
    }

    #[test]
    fn groups_to_vectors_rejects_negative_positions() {
        let groups = vec![PositionDeleteGroup {
            referenced_data_file: "file:///x/data.parquet".to_string(),
            partition_spec_id: 0,
            partition_values: iceberg::spec::Struct::empty(),
            positions: vec![1, -1],
        }];
        let err = groups_to_vectors(&groups).unwrap_err();
        assert!(err.contains("non-negative"));
    }

    #[test]
    fn group_written_dvs_uses_referenced_data_file_partition_spec() {
        let data_files = HashMap::from([
            ("file:///x/old.parquet".to_string(), test_live_file(1)),
            ("file:///x/new.parquet".to_string(), test_live_file(2)),
        ]);
        let dvs = vec![
            test_written_dv("file:///x/dv-old.puffin", "file:///x/old.parquet"),
            test_written_dv("file:///x/dv-new.puffin", "file:///x/new.parquet"),
        ];

        let grouped = group_written_dvs_by_partition_spec(&dvs, &data_files).unwrap();

        assert_eq!(grouped.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(grouped[&1][0].referenced_data_file, "file:///x/old.parquet");
        assert_eq!(grouped[&2][0].referenced_data_file, "file:///x/new.parquet");
    }

    #[test]
    fn dv_summary_updates_total_records_for_new_deletes() {
        let dvs = vec![test_written_dv_with_cardinality(
            "file:///x/dv-new.puffin",
            "file:///x/data.parquet",
            4,
        )];
        let total_records = dv_total_records(Some(10), 4, 0).unwrap();
        let summary = dv_summary(&dvs, &[], total_records, 4, 0, 0);

        assert_eq!(summary["added-position-deletes"], "4");
        assert_eq!(summary["deleted-records"], "4");
        assert_eq!(summary["total-records"], "6");
    }

    #[test]
    fn dv_summary_counts_only_new_deletes_when_replacing_existing_dv() {
        let dvs = vec![test_written_dv_with_cardinality(
            "file:///x/dv-new.puffin",
            "file:///x/data.parquet",
            5,
        )];
        let total_records = dv_total_records(Some(10), 2, 0).unwrap();
        let summary = dv_summary(&dvs, &[], total_records, 2, 1, 3);

        assert_eq!(summary["added-position-deletes"], "5");
        assert_eq!(summary["deleted-records"], "2");
        assert_eq!(summary["removed-delete-files"], "1");
        assert_eq!(summary["removed-position-deletes"], "3");
        assert_eq!(summary["total-records"], "8");
    }

    #[test]
    fn dv_summary_adds_written_data_records_to_total_records() {
        let dvs = vec![test_written_dv_with_cardinality(
            "file:///x/dv-new.puffin",
            "file:///x/data.parquet",
            1,
        )];
        let written = vec![test_written_data_file("file:///x/new.parquet", 2)];
        let total_records = dv_total_records(Some(10), 1, 2).unwrap();
        let summary = dv_summary(&dvs, &written, total_records, 1, 0, 0);

        assert_eq!(summary["deleted-records"], "1");
        assert_eq!(summary["added-data-files"], "1");
        assert_eq!(summary["added-records"], "2");
        assert_eq!(summary["total-records"], "11");
    }

    fn test_live_file(partition_spec_id: i32) -> LiveFile {
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("file:///x/data-{partition_spec_id}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .partition(iceberg::spec::Struct::empty())
            .partition_spec_id(partition_spec_id)
            .record_count(1)
            .file_size_in_bytes(10)
            .build()
            .unwrap();
        LiveFile {
            data_file,
            partition_spec_id,
            snapshot_id: 11,
            sequence_number: 12,
            file_sequence_number: Some(13),
        }
    }

    fn test_written_dv(path: &str, referenced_data_file: &str) -> WrittenPuffinDv {
        test_written_dv_with_cardinality(path, referenced_data_file, 1)
    }

    fn test_written_dv_with_cardinality(
        path: &str,
        referenced_data_file: &str,
        cardinality: u64,
    ) -> WrittenPuffinDv {
        WrittenPuffinDv {
            path: path.to_string(),
            referenced_data_file: referenced_data_file.to_string(),
            cardinality,
            content_offset: 4,
            content_size_in_bytes: 8,
            file_size_in_bytes: 12,
        }
    }

    fn test_written_data_file(path: &str, record_count: u64) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: iceberg::spec::Struct::empty(),
            partition_spec_id: 0,
            record_count,
            file_size_in_bytes: 20,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: Some(0),
        }
    }
}
