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
//! 4. Writes a single new delete manifest (`*-row-delta-dv-added-*.avro`)
//!    that records the new Puffin DV files with
//!    `content=PositionDeletes`, `file_format=Puffin`, and the
//!    `referenced-data-file` / `content_offset` / `content_size_in_bytes`
//!    fields required by the Iceberg v3 spec.
//! 5. Writes a v3 manifest list whose `first_row_id` carries through the
//!    table's current `next-row-id` (DELETE adds no new rows, so no
//!    advance) and a `Snapshot` whose `summary.operation = "delete"` and
//!    `row_range = (next_row_id, 0)`.
//! 6. Returns an `ActionCommit` with the standard
//!    `AddSnapshot + SetSnapshotRef` updates and OCC requirements.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, MAIN_BRANCH,
    ManifestContentType, ManifestFile, ManifestWriterBuilder, Operation, PartitionSpecRef,
    SchemaRef, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::position_delete_writer::PositionDeleteGroup;
use super::puffin_dv::{
    DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
use super::types::CommitOutcome;

pub struct RowDeltaDvCommit;

#[async_trait]
impl IcebergCommitAction for RowDeltaDvCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let groups = ctx.collector.take_delete_groups();
        if groups.iter().all(|g| g.positions.is_empty()) {
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
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            row_lineage_first_row_id: ctx.table.metadata().next_row_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
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
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    schema_id: i32,
    /// `next_row_id` of the base table — DELETE does not advance this, so the
    /// snapshot's `row_range = (first_row_id, 0)`.
    row_lineage_first_row_id: u64,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
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
        let parent_snapshot_id = m.current_snapshot().map(|s| s.snapshot_id());
        let metadata_dir = metadata_dir(table);

        let mut vectors = groups_to_vectors(&self.groups).map_err(to_iceberg_unexpected)?;
        let touched_files: HashSet<String> = vectors.keys().cloned().collect();
        let index = build_snapshot_index(table, &self.file_io, &touched_files, &mut vectors)
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
        if !index.touched_delete_existing.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-row-delta-dv-existing-0.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_existing_delete_manifest(
                &self.file_io,
                &path,
                &index.touched_delete_existing,
                self.partition_spec.clone(),
                self.schema.clone(),
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        let added_path = format!(
            "{metadata_dir}/{}-row-delta-dv-added-0.avro",
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
            &written_dvs,
            &index.data_files,
            self.partition_spec.clone(),
            self.schema.clone(),
            new_seq,
            new_snapshot_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        new_manifests.push(added);

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
        let _ = write_manifest_list(
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

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Delete,
                additional_properties: dv_summary(&written_dvs),
            })
            .with_schema_id(self.schema_id)
            .with_row_range(self.row_lineage_first_row_id, 0)
            .build();

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_string(),
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
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: parent_snapshot_id,
                },
            ],
        ))
    }
}

struct LiveFile {
    data_file: DataFile,
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
}

async fn build_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    vectors: &mut HashMap<String, DeletionVector>,
) -> Result<SnapshotIndex, String> {
    let mut data_files = HashMap::new();
    let mut untouched_manifests = Vec::new();
    let mut touched_delete_existing = Vec::new();
    let snapshot = table
        .metadata()
        .current_snapshot()
        .ok_or_else(|| "row-lineage DELETE requires a current snapshot".to_string())?;
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
                        vectors.entry(referenced).or_default().merge(&old);
                        manifest_touched = true;
                    } else {
                        keep.push(LiveFile {
                            data_file: file,
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

    Ok(SnapshotIndex {
        data_files,
        untouched_manifests,
        touched_delete_existing,
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
    let partition_spec_id = partition_spec.spec_id();
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
        let df = dv_data_file(written, referenced, partition_spec_id)?;
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

fn dv_data_file(
    written: &WrittenPuffinDv,
    referenced: &LiveFile,
    partition_spec_id: i32,
) -> Result<DataFile, String> {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(written.path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(referenced.data_file.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(written.cardinality)
        .file_size_in_bytes(written.file_size_in_bytes)
        .referenced_data_file(Some(written.referenced_data_file.clone()))
        .content_offset(Some(written.content_offset))
        .content_size_in_bytes(Some(written.content_size_in_bytes))
        .build()
        .map_err(|e| format!("build DV DataFile failed: {e}"))
}

fn dv_summary(dvs: &[WrittenPuffinDv]) -> HashMap<String, String> {
    let mut p = HashMap::new();
    let total_records: u64 = dvs.iter().map(|d| d.cardinality).sum();
    let total_size: u64 = dvs.iter().map(|d| d.file_size_in_bytes).sum();
    p.insert("added-delete-files".to_string(), dvs.len().to_string());
    p.insert(
        "added-position-deletes".to_string(),
        total_records.to_string(),
    );
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
                positions: vec![1, 2],
            },
            PositionDeleteGroup {
                referenced_data_file: "file:///x/data.parquet".to_string(),
                positions: vec![3],
            },
            PositionDeleteGroup {
                referenced_data_file: "file:///x/empty.parquet".to_string(),
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
            positions: vec![1, -1],
        }];
        let err = groups_to_vectors(&groups).unwrap_err();
        assert!(err.contains("non-negative"));
    }
}
