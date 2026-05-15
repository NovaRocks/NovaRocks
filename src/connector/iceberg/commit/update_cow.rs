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

//! `CowUpdateCommit` — the Iceberg v3 copy-on-write UPDATE commit action.
//!
//! This module stages the metadata-only transaction action for COW UPDATE:
//! delete touched live data files and add rewritten data files while preserving
//! row-lineage metadata.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile,
    ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference,
    SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::overwrite::{write_added_data_manifest, write_overwrite_deletes_manifest};
use super::types::{CommitOutcome, WrittenFile};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CowUpdateRewriteSet {
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<CowUpdateTouchedFile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CowUpdateTouchedFile {
    pub old_file: String,
    pub new_files: Vec<String>,
    pub row_ids: Vec<i64>,
}

pub struct CowUpdateCommit {
    pub rewrite: CowUpdateRewriteSet,
}

#[async_trait]
impl IcebergCommitAction for CowUpdateCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "CowUpdateCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }
        if written.is_empty()
            && self.rewrite.touched_data_files.is_empty()
            && self.rewrite.updated_row_ids.is_empty()
        {
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
        let action = CowUpdateTxnAction {
            written,
            rewrite: self.rewrite.clone(),
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            target_ref: ctx.target_ref.to_string(),
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("CowUpdate apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("CowUpdate commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "CowUpdate committed but new snapshot is not visible".to_string())?;
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

struct CowUpdateTxnAction {
    written: Vec<WrittenFile>,
    rewrite: CowUpdateRewriteSet,
    commit_uuid: Uuid,
    file_io: FileIO,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    target_ref: String,
}

#[async_trait]
impl TransactionAction for CowUpdateTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version != FormatVersion::V3 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "CowUpdateCommit requires an Iceberg v3 table",
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
        let row_lineage_first_row_id = m.next_row_id();

        validate_cow_update_inputs(
            &self.rewrite,
            &self.written,
            parent_snapshot_id,
            &m.uuid().to_string(),
        )
        .map_err(to_iceberg_data_invalid)?;
        let touched_paths = touched_old_file_paths(&self.rewrite);
        let index = build_cow_snapshot_index(table, &self.file_io, &touched_paths, target_ref)
            .await
            .map_err(to_iceberg_unexpected)?;
        if index.touched_live.len() != touched_paths.len() {
            return Err(to_iceberg_unexpected(format!(
                "COW UPDATE touched {} data file(s), but only {} are live in the {} snapshot",
                touched_paths.len(),
                index.touched_live.len(),
                target_ref,
            )));
        }
        let touched_delete_groups = group_live_files_by_partition_spec(&index.touched_live);

        let mut new_manifests: Vec<ManifestFile> = index.untouched_manifests;
        for (idx, carried) in index.carried_live.iter().enumerate() {
            let path = format!(
                "{metadata_dir}/{}-cow-update-existing-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle.record_manifest(path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(path.clone());
            let mf = write_existing_data_manifest(
                &self.file_io,
                &path,
                carried,
                partition_spec_by_id(m, carried.partition_spec_id)?,
                m.current_schema().clone(),
                new_snapshot_id,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        for (idx, (spec_id, touched)) in touched_delete_groups.into_iter().enumerate() {
            let delete_manifest_path = format!(
                "{metadata_dir}/{}-cow-update-deletes-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle
                .record_manifest(delete_manifest_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(delete_manifest_path.clone());
            let delete_manifest = write_overwrite_deletes_manifest(
                &self.file_io,
                &delete_manifest_path,
                &live_files_as_delete_entries(&touched),
                partition_spec_by_id(m, spec_id)?,
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(delete_manifest);
        }

        let written_by_path = self
            .written
            .iter()
            .map(|file| (file.path.clone(), file.clone()))
            .collect::<HashMap<_, _>>();
        for (idx, rewrite_file) in self.rewrite.touched_data_files.iter().enumerate() {
            let data_manifest_path = format!(
                "{metadata_dir}/{}-cow-update-data-{idx}.avro",
                self.commit_uuid
            );
            self.abort_handle
                .record_manifest(data_manifest_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(data_manifest_path.clone());
            let replacement_files = rewrite_file
                .new_files
                .iter()
                .map(|path| {
                    written_by_path.get(path).cloned().ok_or_else(|| {
                        to_iceberg_data_invalid(format!(
                            "CowUpdateCommit rewrite replacement data file {path} was not written"
                        ))
                    })
                })
                .collect::<iceberg::Result<Vec<_>>>()?;
            let data_manifest = write_added_data_manifest(
                &self.file_io,
                &data_manifest_path,
                &replacement_files,
                m.default_partition_spec().clone(),
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mark_replacement_manifest_row_id_assigned(
                data_manifest,
                replacement_manifest_first_row_id(rewrite_file).map_err(to_iceberg_data_invalid)?,
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
            Some(row_lineage_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if manifest_list_next_row_id != Some(row_lineage_first_row_id) {
            return Err(to_iceberg_unexpected(format!(
                "COW UPDATE must not allocate row IDs: expected next-row-id {row_lineage_first_row_id}, got {manifest_list_next_row_id:?}"
            )));
        }

        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: HashMap::new(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(m.current_schema_id())
            .with_row_range(row_lineage_first_row_id, 0)
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

#[derive(Clone)]
struct LiveDataFile {
    data_file: DataFile,
    partition_spec_id: i32,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    first_row_id: u64,
}

struct CowSnapshotIndex {
    untouched_manifests: Vec<ManifestFile>,
    touched_live: Vec<LiveDataFile>,
    carried_live: Vec<LiveDataFile>,
}

fn group_live_files_by_partition_spec(files: &[LiveDataFile]) -> BTreeMap<i32, Vec<LiveDataFile>> {
    let mut grouped = BTreeMap::new();
    for file in files {
        grouped
            .entry(file.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(file.clone());
    }
    grouped
}

fn live_files_as_delete_entries(files: &[LiveDataFile]) -> Vec<(DataFile, i64, Option<i64>)> {
    files
        .iter()
        .map(|f| {
            (
                f.data_file.clone(),
                f.sequence_number,
                f.file_sequence_number,
            )
        })
        .collect()
}

async fn build_cow_snapshot_index(
    table: &Table,
    file_io: &FileIO,
    touched_paths: &HashSet<String>,
    target_ref: &str,
) -> Result<CowSnapshotIndex, String> {
    let m = table.metadata();
    // For branch-targeted updates, read the manifest list from the branch head
    // snapshot (not from main's current snapshot). This ensures that files added
    // to the branch by prior branch DML (e.g. a branch INSERT) are carried
    // forward correctly by the COW rewrite.
    let snapshot = if target_ref == "main" {
        m.current_snapshot()
            .ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?
    } else {
        let branch_snapshot_id =
            m.refs()
                .get(target_ref)
                .map(|r| r.snapshot_id)
                .ok_or_else(|| {
                    format!("COW UPDATE target branch '{target_ref}' not found in table metadata")
                })?;
        m.snapshot_by_id(branch_snapshot_id)
            .ok_or_else(|| format!("COW UPDATE branch '{target_ref}' snapshot {branch_snapshot_id} not found in metadata"))?
    };
    let manifest_list = snapshot
        .load_manifest_list(file_io, table.metadata())
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    let mut untouched_manifests = Vec::new();
    let mut touched_live = Vec::new();
    let mut carried_live = Vec::new();

    for mf in manifest_list.entries() {
        match mf.content {
            ManifestContentType::Deletes => {
                untouched_manifests.push(mf.clone());
            }
            ManifestContentType::Data => {
                let manifest = mf
                    .load_manifest(file_io)
                    .await
                    .map_err(|e| format!("load data manifest {} failed: {e}", mf.manifest_path))?;
                let mut next_manifest_first_row_id = mf
                    .first_row_id
                    .map(|v| {
                        i64::try_from(v)
                            .map_err(|_| format!("manifest first_row_id too large: {v}"))
                    })
                    .transpose()?;
                let mut manifest_touched = false;
                let mut manifest_carried = Vec::new();

                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let data_file = entry.data_file();
                    if data_file.content_type() != DataContentType::Data {
                        continue;
                    }
                    let first_row_id = data_file
                        .first_row_id()
                        .or(next_manifest_first_row_id)
                        .ok_or_else(|| {
                            format!(
                                "COW UPDATE requires first_row_id for live data file {}",
                                data_file.file_path()
                            )
                        })?;
                    if first_row_id < 0 {
                        return Err(format!(
                            "COW UPDATE found negative first_row_id {first_row_id} for live data file {}",
                            data_file.file_path()
                        ));
                    }
                    let record_count = i64::try_from(data_file.record_count()).map_err(|_| {
                        format!("record_count too large for {}", data_file.file_path())
                    })?;
                    if let Some(next) = next_manifest_first_row_id.as_mut() {
                        *next = next.checked_add(record_count).ok_or_else(|| {
                            format!("first_row_id overflow in manifest {}", mf.manifest_path)
                        })?;
                    }

                    let live = LiveDataFile {
                        data_file: data_file.clone(),
                        partition_spec_id: mf.partition_spec_id,
                        snapshot_id: entry.snapshot_id().unwrap_or(mf.added_snapshot_id),
                        sequence_number: entry.sequence_number().unwrap_or(mf.sequence_number),
                        file_sequence_number: entry.file_sequence_number,
                        first_row_id: first_row_id as u64,
                    };
                    if touched_paths.contains(data_file.file_path()) {
                        manifest_touched = true;
                        touched_live.push(live);
                    } else {
                        manifest_carried.push(live);
                    }
                }

                if manifest_touched {
                    carried_live.extend(manifest_carried);
                } else {
                    untouched_manifests.push(mf.clone());
                }
            }
        }
    }

    Ok(CowSnapshotIndex {
        untouched_manifests,
        touched_live,
        carried_live,
    })
}

async fn write_existing_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    file: &LiveDataFile,
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
    let mut writer = builder.build_v3_data();
    writer
        .add_existing_file(
            file.data_file.clone(),
            file.snapshot_id,
            file.sequence_number,
            file.file_sequence_number,
        )
        .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    let mut manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    manifest_file.first_row_id = Some(file.first_row_id);
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

fn mark_replacement_manifest_row_id_assigned(
    mut manifest: ManifestFile,
    row_lineage_first_row_id: u64,
) -> ManifestFile {
    // COW replacement files carry stored row-lineage columns. The manifest
    // first-row-id is assigned only to prevent the v3 manifest-list writer
    // from allocating new row IDs for those replacement rows.
    manifest.first_row_id = Some(row_lineage_first_row_id);
    manifest
}

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
        return Err("CowUpdateCommit rewrite updated_row_ids must not be empty".to_string());
    }

    let mut updated_row_ids = HashSet::new();
    for row_id in &rewrite.updated_row_ids {
        if !updated_row_ids.insert(*row_id) {
            return Err(format!(
                "CowUpdateCommit rewrite contains duplicate updated row id {row_id}"
            ));
        }
    }

    let mut old_files = HashSet::new();
    let mut rewrite_row_ids = HashSet::new();
    let mut rewrite_new_files = HashSet::new();
    for file in &rewrite.touched_data_files {
        if !old_files.insert(file.old_file.clone()) {
            return Err(format!(
                "CowUpdateCommit rewrite contains duplicate touched data file {}",
                file.old_file
            ));
        }
        if file.row_ids.is_empty() {
            return Err(format!(
                "CowUpdateCommit rewrite touched data file {} has no row ids",
                file.old_file
            ));
        }
        if file.new_files.is_empty() {
            return Err(format!(
                "CowUpdateCommit rewrite touched data file {} has no replacement data files",
                file.old_file
            ));
        }
        for row_id in &file.row_ids {
            if !rewrite_row_ids.insert(*row_id) {
                return Err(format!(
                    "CowUpdateCommit rewrite contains duplicate touched row id {row_id}"
                ));
            }
        }
        for new_file in &file.new_files {
            if !rewrite_new_files.insert(new_file.clone()) {
                return Err(format!(
                    "CowUpdateCommit rewrite contains duplicate replacement data file {new_file}"
                ));
            }
        }
    }
    if let Some(row_id) = updated_row_ids.difference(&rewrite_row_ids).next() {
        return Err(format!(
            "CowUpdateCommit rewrite updated_row_ids contains row id {row_id}, but touched files are missing touched row id {row_id}"
        ));
    }
    let written_files: HashSet<String> = written.iter().map(|f| f.path.clone()).collect();
    if written_files.len() != written.len() {
        return Err("CowUpdateCommit received duplicate replacement data file paths".to_string());
    }
    for new_file in &rewrite_new_files {
        if !written_files.contains(new_file) {
            return Err(format!(
                "CowUpdateCommit rewrite replacement data file {new_file} was not written"
            ));
        }
    }
    for written_file in &written_files {
        if !rewrite_new_files.contains(written_file) {
            return Err(format!(
                "CowUpdateCommit written data file {written_file} is missing from rewrite"
            ));
        }
    }

    Ok(())
}

fn replacement_manifest_first_row_id(rewrite_file: &CowUpdateTouchedFile) -> Result<u64, String> {
    let first = rewrite_file
        .row_ids
        .iter()
        .copied()
        .min()
        .ok_or_else(|| "CowUpdateCommit rewrite has no replacement row ids".to_string())?;
    u64::try_from(first)
        .map_err(|_| format!("CowUpdateCommit rewrite contains negative row id {first}"))
}

fn touched_old_file_paths(rewrite: &CowUpdateRewriteSet) -> HashSet<String> {
    rewrite
        .touched_data_files
        .iter()
        .map(|f| f.old_file.clone())
        .collect()
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
                "COW UPDATE references unknown partition spec id {spec_id}"
            ))
        })
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

fn to_iceberg_data_invalid(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{DataFileFormat, Struct};

    #[test]
    fn type_compiles() {
        let rewrite = cow_rewrite();

        let commit = CowUpdateCommit { rewrite };
        assert_eq!(commit.rewrite.base_snapshot_id, 7);
    }

    #[test]
    fn validate_cow_update_inputs_accepts_consistent_rewrite() {
        let rewrite = cow_rewrite();
        let written = vec![written_file("new.parquet")];

        validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect("valid rewrite");
    }

    #[test]
    fn validate_cow_update_inputs_rejects_duplicate_row_ids() {
        let mut rewrite = cow_rewrite();
        rewrite.updated_row_ids = vec![1, 1];
        let written = vec![written_file("new.parquet")];

        let err = validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect_err("duplicate row ids must fail");

        assert!(err.contains("duplicate updated row id 1"));
    }

    #[test]
    fn validate_cow_update_inputs_rejects_written_rewrite_mismatch() {
        let rewrite = cow_rewrite();
        let written = vec![written_file("other.parquet")];

        let err = validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect_err("replacement file mismatch must fail");

        assert!(err.contains("new.parquet was not written"));
    }

    #[test]
    fn validate_cow_update_inputs_rejects_updated_row_id_missing_from_touched_files() {
        let mut rewrite = cow_rewrite();
        rewrite.updated_row_ids = vec![1, 2];
        rewrite.touched_data_files[0].row_ids = vec![1];
        let written = vec![written_file("new.parquet")];

        let err = validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect_err("updated row id mismatch must fail");

        assert!(err.contains("missing touched row id 2"));
    }

    #[test]
    fn validate_cow_update_inputs_allows_rewritten_row_ids_in_touched_files() {
        let mut rewrite = cow_rewrite();
        rewrite.updated_row_ids = vec![1];
        rewrite.touched_data_files[0].row_ids = vec![1, 2];
        let written = vec![written_file("new.parquet")];

        validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect("rewritten row ids may include unchanged rows");
    }

    fn cow_rewrite() -> CowUpdateRewriteSet {
        CowUpdateRewriteSet {
            base_snapshot_id: 7,
            target_table_uuid: "table-uuid".to_string(),
            updated_row_ids: vec![1],
            touched_data_files: vec![CowUpdateTouchedFile {
                old_file: "old.parquet".to_string(),
                new_files: vec!["new.parquet".to_string()],
                row_ids: vec![1],
            }],
        }
    }

    fn written_file(path: &str) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 1,
            file_size_in_bytes: 128,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        }
    }
}
