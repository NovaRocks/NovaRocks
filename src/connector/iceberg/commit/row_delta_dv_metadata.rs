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

//! Shared metadata helpers for v3 row-lineage deletion-vector commits.

use std::collections::{BTreeMap, HashMap, HashSet};

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, ManifestFile,
    ManifestWriterBuilder, PartitionSpecRef, SchemaRef, TableMetadata,
};
use iceberg::table::Table;

use super::puffin_dv::{DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin};
use super::types::WrittenFile;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WrittenDvFile {
    pub path: String,
    pub referenced_data_file: String,
    pub cardinality: u64,
    pub content_offset: i64,
    pub content_size_in_bytes: i64,
    pub file_size_in_bytes: u64,
}

impl From<WrittenPuffinDv> for WrittenDvFile {
    fn from(written: WrittenPuffinDv) -> Self {
        Self {
            path: written.path,
            referenced_data_file: written.referenced_data_file,
            cardinality: written.cardinality,
            content_offset: written.content_offset,
            content_size_in_bytes: written.content_size_in_bytes,
            file_size_in_bytes: written.file_size_in_bytes,
        }
    }
}

#[derive(Clone)]
pub(super) struct LiveFile {
    pub data_file: DataFile,
    pub partition_spec_id: i32,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub file_sequence_number: Option<i64>,
}

pub(super) struct SnapshotIndex {
    /// Live data files keyed by `file_path()`.
    pub data_files: HashMap<String, LiveFile>,
    /// Manifests we did NOT touch; preserved verbatim in the new manifest list.
    pub untouched_manifests: Vec<ManifestFile>,
    /// Live delete entries from touched delete manifests that the current
    /// DELETE did not affect (i.e., reference some other data file). They are
    /// rewritten into a new `*-row-delta-dv-existing-*.avro` so the DV lineage
    /// is preserved for unrelated data files.
    pub touched_delete_existing: Vec<LiveFile>,
    /// Live DV files removed because a replacement DV was written.
    pub replaced_delete_files: usize,
    /// Position deletes already represented by removed DV files.
    pub replaced_delete_records: u64,
    /// Total file_size_in_bytes of replaced DV files.
    pub replaced_delete_files_size: u64,
}

pub(super) async fn build_snapshot_index_with_dv_merge(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    vectors: &mut HashMap<String, DeletionVector>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    build_snapshot_index(table, file_io, touched_files, Some(vectors), target_ref).await
}

pub(super) async fn build_snapshot_index_metadata_only(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    build_snapshot_index(table, file_io, touched_files, None, target_ref).await
}

async fn build_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_files: &HashSet<String>,
    mut vectors_to_merge: Option<&mut HashMap<String, DeletionVector>>,
    target_ref: &str,
) -> Result<SnapshotIndex, String> {
    let mut data_files = HashMap::new();
    let mut untouched_manifests = Vec::new();
    let mut touched_delete_existing = Vec::new();
    let mut replaced_delete_files = 0usize;
    let mut replaced_delete_files_size = 0u64;
    let mut replaced_delete_records = 0u64;
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
                        if vectors_to_merge.is_some() {
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
                            replaced_delete_vectors
                                .entry(referenced.clone())
                                .or_default()
                                .merge(&old);
                            if let Some(vectors) = vectors_to_merge.as_mut() {
                                vectors.entry(referenced).or_default().merge(&old);
                            }
                        } else {
                            replaced_delete_records = replaced_delete_records
                                .checked_add(file.record_count())
                                .ok_or_else(|| "replaced DV record_count overflow".to_string())?;
                        }
                        replaced_delete_files += 1;
                        replaced_delete_files_size = replaced_delete_files_size
                            .checked_add(file.file_size_in_bytes())
                            .ok_or_else(|| "replaced DV file_size_in_bytes overflow".to_string())?;
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

    if vectors_to_merge.is_some() {
        replaced_delete_records =
            replaced_delete_vectors
                .values()
                .try_fold(0u64, |sum, vector| {
                    sum.checked_add(vector.cardinality())
                        .ok_or_else(|| "replaced DV cardinality overflow".to_string())
                })?;
    }

    Ok(SnapshotIndex {
        data_files,
        untouched_manifests,
        touched_delete_existing,
        replaced_delete_files,
        replaced_delete_records,
        replaced_delete_files_size,
    })
}

pub(super) fn validate_delete_file_for_row_lineage(file: &DataFile) -> Result<(), String> {
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

pub(super) fn partition_spec_by_id(
    metadata: &TableMetadata,
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

pub(super) fn group_live_files_by_partition_spec(
    files: Vec<LiveFile>,
) -> BTreeMap<i32, Vec<LiveFile>> {
    let mut grouped = BTreeMap::new();
    for file in files {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file);
    }
    grouped
}

pub(super) fn group_written_dvs_by_partition_spec(
    dvs: &[WrittenDvFile],
    data_files: &HashMap<String, LiveFile>,
) -> Result<BTreeMap<i32, Vec<WrittenDvFile>>, String> {
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

pub(super) async fn write_existing_delete_manifest(
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
pub(super) async fn write_added_dv_manifest(
    file_io: &FileIO,
    out_path: &str,
    dvs: &[WrittenDvFile],
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

pub(super) fn dv_data_file(
    written: &WrittenDvFile,
    referenced: &LiveFile,
) -> Result<DataFile, String> {
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

pub(super) fn dv_total_records(
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

pub(super) fn dv_summary(
    dvs: &[WrittenDvFile],
    written_data_files: &[WrittenFile],
    total_records: Option<u64>,
    newly_deleted_records: u64,
    removed_delete_files: usize,
    removed_position_deletes: u64,
) -> Result<HashMap<String, String>, String> {
    let mut p = HashMap::new();
    let added_position_deletes = dvs.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.cardinality)
            .ok_or_else(|| "DV added position delete count overflow".to_string())
    })?;
    let added_data_records = written_data_files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.record_count)
            .ok_or_else(|| "DV added data record count overflow".to_string())
    })?;
    let total_size = dvs
        .iter()
        .map(|d| d.file_size_in_bytes)
        .chain(written_data_files.iter().map(|f| f.file_size_in_bytes))
        .try_fold(0u64, |sum, size| {
            sum.checked_add(size)
                .ok_or_else(|| "DV added file size overflow".to_string())
        })?;
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
    Ok(p)
}

pub(super) fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}
