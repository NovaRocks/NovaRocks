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

//! `rewrite_position_delete_files` — Spark-compatible V3 Puffin DV repack.
//!
//! This action rewrites groups of live Puffin deletion-vector delete files
//! that reference the same data file into a single replacement deletion
//! vector. It intentionally does not implement V2 Parquet position-delete
//! rewrite.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    ManifestFile, ManifestWriterBuilder, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Struct, Summary,
};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use uuid::Uuid;

use super::helpers::{
    current_snapshot_total_records, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::puffin_dv::{
    DeletionVector, DeletionVectorBlobInput, WrittenPuffinDv, read_deletion_vector_puffin,
    write_multi_deletion_vector_puffin,
};
use super::retry::{commit_with_retry, is_retryable_commit_conflict};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewritePositionDeleteOptions {
    pub rewrite_all: bool,
    pub min_input_files: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewritePositionDeleteOutcome {
    pub rewritten_delete_files_count: i32,
    pub added_delete_files_count: i32,
    pub rewritten_bytes_count: i64,
    pub added_bytes_count: i64,
}

impl RewritePositionDeleteOptions {
    pub fn from_map(values: &BTreeMap<String, String>) -> Result<Self, String> {
        let mut out = Self {
            rewrite_all: false,
            min_input_files: 2,
        };
        for (key, value) in values {
            match key.as_str() {
                "rewrite-all" => out.rewrite_all = parse_bool_option(key, value)?,
                "min-input-files" => out.min_input_files = parse_usize_option(key, value)?,
                "target-file-size-bytes" => return Err(
                    "rewrite_position_delete_files option `target-file-size-bytes` is not implemented in NovaRocks yet"
                        .to_string(),
                ),
                other => {
                    return Err(format!(
                        "unsupported rewrite_position_delete_files option `{other}`"
                    ));
                }
            }
        }
        Ok(out)
    }
}

pub async fn run_rewrite_position_delete_files(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    options: RewritePositionDeleteOptions,
) -> Result<RewritePositionDeleteOutcome, String> {
    let outcome: Arc<Mutex<Option<RewritePositionDeleteOutcome>>> = Arc::new(Mutex::new(None));
    let outcome_out = outcome.clone();
    commit_with_retry(|_attempt| {
        let catalog = catalog.clone();
        let table_ident = table_ident.clone();
        let options = options.clone();
        let outcome_out = outcome_out.clone();
        async move {
            let next = run_one_attempt(catalog, table_ident, options).await?;
            *outcome_out
                .lock()
                .expect("rewrite position delete outcome mutex poisoned") = Some(next);
            Ok(())
        }
    })
    .await?;
    outcome
        .lock()
        .expect("rewrite position delete outcome mutex poisoned")
        .clone()
        .ok_or_else(|| "rewrite_position_delete_files finished without an outcome".to_string())
}

async fn run_one_attempt(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    options: RewritePositionDeleteOptions,
) -> Result<RewritePositionDeleteOutcome, iceberg::Error> {
    let table = catalog.load_table(&table_ident).await?;
    let metadata = table.metadata();
    let file_io = table.file_io();
    let Some(current) = metadata.current_snapshot() else {
        return Ok(RewritePositionDeleteOutcome::default());
    };

    let manifest_list = current.load_manifest_list(file_io, metadata).await?;
    let plan = plan_rewrite(file_io, manifest_list.entries(), &options)
        .await
        .map_err(to_iceberg_unexpected)?;
    if plan.candidate_groups.is_empty() {
        return Ok(RewritePositionDeleteOutcome::default());
    }
    if metadata.format_version() != FormatVersion::V3 {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "rewrite_position_delete_files requires an Iceberg v3 table for Puffin deletion vector rewrite",
        ));
    }
    let total_records = rewrite_total_records(
        current_snapshot_total_records(metadata)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?,
    )
    .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;

    let new_seq = metadata.last_sequence_number() + 1;
    let new_snapshot_id = generate_snapshot_id();
    let parent_snapshot_id = Some(current.snapshot_id());
    let commit_uuid = Uuid::new_v4();
    let meta_dir = metadata_dir(&table);
    let created = RewriteArtifacts::default();

    let attempt_result: Result<RewritePositionDeleteOutcome, AttemptFailure> = async {
        let mut new_manifests = plan.untouched_manifests.clone();
        for (idx, (spec_id, entries)) in group_entries_by_partition_spec(&plan.existing_entries)
            .into_iter()
            .enumerate()
        {
            let path = format!("{meta_dir}/{commit_uuid}-rewrite-pos-delete-existing-{idx}.avro");
            created.record_manifest(path.clone());
            let manifest = write_existing_delete_manifest(
                file_io,
                &path,
                entries,
                partition_spec_by_id(metadata, spec_id).map_err(AttemptFailure::cleanup)?,
                metadata.current_schema().clone(),
                new_snapshot_id,
            )
            .await
            .map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;
            new_manifests.push(manifest);
        }

        for (idx, (spec_id, entries)) in group_entries_by_partition_spec(&plan.rewritten_entries)
            .into_iter()
            .enumerate()
        {
            let path = format!("{meta_dir}/{commit_uuid}-rewrite-pos-delete-deleted-{idx}.avro");
            created.record_manifest(path.clone());
            let manifest = write_deleted_delete_manifest(
                file_io,
                &path,
                entries,
                partition_spec_by_id(metadata, spec_id).map_err(AttemptFailure::cleanup)?,
                metadata.current_schema().clone(),
                new_snapshot_id,
            )
            .await
            .map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;
            new_manifests.push(manifest);
        }

        let written_dvs = write_repacked_dvs(
            file_io,
            metadata.location(),
            &commit_uuid,
            &plan.candidate_groups,
            &created,
        )
        .await
        .map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;

        for (idx, (spec_id, dvs)) in group_written_dvs_by_partition_spec(&written_dvs)
            .into_iter()
            .enumerate()
        {
            let path = format!("{meta_dir}/{commit_uuid}-rewrite-pos-delete-added-{idx}.avro");
            created.record_manifest(path.clone());
            let manifest = write_added_dv_manifest(
                file_io,
                &path,
                &dvs,
                partition_spec_by_id(metadata, spec_id).map_err(AttemptFailure::cleanup)?,
                metadata.current_schema().clone(),
                new_snapshot_id,
            )
            .await
            .map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;
            new_manifests.push(manifest);
        }

        let manifest_list_path = format!("{meta_dir}/snap-{new_snapshot_id}-{commit_uuid}.avro");
        created.record_manifest(manifest_list_path.clone());
        let manifest_list_next_row_id = write_manifest_list(
            file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            metadata.format_version(),
            Some(metadata.next_row_id()),
        )
        .await
        .map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;
        if manifest_list_next_row_id != Some(metadata.next_row_id()) {
            return Err(AttemptFailure::cleanup(to_iceberg_unexpected(format!(
                "rewrite_position_delete_files must not allocate row IDs: expected next-row-id {}, got {manifest_list_next_row_id:?}",
                metadata.next_row_id()
            ))));
        }

        let outcome =
            rewrite_outcome(&plan, &written_dvs).map_err(|e| AttemptFailure::cleanup(to_iceberg_unexpected(e)))?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Replace,
                additional_properties: rewrite_summary(&outcome, total_records),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(metadata.next_row_id(), 0)
            .build();
        let commit = TableCommit::builder()
            .ident(table_ident.clone())
            .updates(vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    reference: SnapshotReference {
                        snapshot_id: new_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
                },
            ])
            .requirements(vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: metadata.default_partition_spec_id(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".to_string(),
                    snapshot_id: parent_snapshot_id,
                },
            ])
            .build();
        catalog
            .update_table(commit)
            .await
            .map_err(AttemptFailure::from_catalog_commit_error)?;
        Ok(outcome)
    }
    .await;

    match attempt_result {
        Ok(outcome) => Ok(outcome),
        Err(failure) => {
            if failure.cleanup_artifacts {
                let cleanup_errors = created.cleanup(file_io).await;
                for err in cleanup_errors {
                    tracing::warn!(
                        path = %err.path,
                        source = ?err.source,
                        "rewrite_position_delete_files artifact cleanup error"
                    );
                }
            } else {
                tracing::warn!(
                    table = %table_ident,
                    "rewrite_position_delete_files catalog commit result is unknown; preserving staged artifacts"
                );
            }
            Err(failure.error)
        }
    }
}

async fn plan_rewrite(
    file_io: &FileIO,
    manifests: &[ManifestFile],
    options: &RewritePositionDeleteOptions,
) -> Result<RewritePlan, String> {
    let mut data_manifests = Vec::new();
    let mut delete_manifests = Vec::new();
    let mut live_data_files = HashMap::new();

    for mf in manifests {
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
                    let data_file = entry.data_file().clone();
                    if data_file.content_type() != DataContentType::Data {
                        return Err(format!(
                            "rewrite_position_delete_files found {:?} file {} inside a data manifest",
                            data_file.content_type(),
                            data_file.file_path()
                        ));
                    }
                    record_live_data_file(&mut live_data_files, data_file, mf.partition_spec_id)?;
                }
                data_manifests.push(mf.clone());
            }
            ManifestContentType::Deletes => {
                let manifest = mf.load_manifest(file_io).await.map_err(|e| {
                    format!("load delete manifest {} failed: {e}", mf.manifest_path)
                })?;
                let mut entries = Vec::new();
                for entry in manifest.entries() {
                    if !entry.is_alive() {
                        continue;
                    }
                    let data_file = entry.data_file().clone();
                    let rewrite_ref = classify_delete_file_for_rewrite(&data_file)?;
                    let live = LiveDeleteEntry {
                        data_file,
                        partition_spec_id: mf.partition_spec_id,
                        snapshot_id: entry.snapshot_id().unwrap_or(mf.added_snapshot_id),
                        sequence_number: entry.sequence_number().unwrap_or(mf.sequence_number),
                        file_sequence_number: entry.file_sequence_number,
                        rewrite_ref,
                    };
                    entries.push(live);
                }
                delete_manifests.push(ScannedDeleteManifest {
                    manifest: mf.clone(),
                    entries,
                });
            }
        }
    }

    build_rewrite_plan_from_scanned(data_manifests, delete_manifests, live_data_files, options)
}

fn build_rewrite_plan_from_scanned(
    data_manifests: Vec<ManifestFile>,
    delete_manifests: Vec<ScannedDeleteManifest>,
    live_data_files: HashMap<String, LiveDataEntry>,
    options: &RewritePositionDeleteOptions,
) -> Result<RewritePlan, String> {
    let mut groups: BTreeMap<String, Vec<LiveDeleteEntry>> = BTreeMap::new();
    for scanned in &delete_manifests {
        for entry in &scanned.entries {
            if let Some(puffin_ref) = &entry.rewrite_ref {
                groups
                    .entry(puffin_ref.referenced_data_file.clone())
                    .or_default()
                    .push(entry.clone());
            }
        }
    }

    let mut candidate_refs = HashSet::new();
    let mut candidate_groups = BTreeMap::new();
    for (referenced_data_file, entries) in groups {
        if options.rewrite_all || entries.len() >= options.min_input_files {
            let group = CandidateGroup::new(referenced_data_file.clone(), entries)?;
            candidate_refs.insert(referenced_data_file);
            candidate_groups.insert(group.referenced_data_file.clone(), group);
        }
    }
    validate_candidate_groups_against_live_data(&candidate_groups, &live_data_files)?;

    let mut untouched_manifests = data_manifests;
    let mut existing_entries = Vec::new();
    let mut rewritten_entries = Vec::new();
    for scanned in delete_manifests {
        let manifest_touched = scanned.entries.iter().any(|entry| {
            entry
                .rewrite_ref
                .as_ref()
                .is_some_and(|r| candidate_refs.contains(&r.referenced_data_file))
        });
        if !manifest_touched {
            untouched_manifests.push(scanned.manifest);
            continue;
        }
        for entry in scanned.entries {
            if entry
                .rewrite_ref
                .as_ref()
                .is_some_and(|r| candidate_refs.contains(&r.referenced_data_file))
            {
                rewritten_entries.push(entry);
            } else {
                existing_entries.push(entry);
            }
        }
    }

    Ok(RewritePlan {
        untouched_manifests,
        existing_entries,
        rewritten_entries,
        candidate_groups,
    })
}

fn record_live_data_file(
    live_data_files: &mut HashMap<String, LiveDataEntry>,
    data_file: DataFile,
    partition_spec_id: i32,
) -> Result<(), String> {
    let path = data_file.file_path().to_string();
    if live_data_files
        .insert(
            path.clone(),
            LiveDataEntry {
                data_file,
                partition_spec_id,
            },
        )
        .is_some()
    {
        return Err(format!(
            "rewrite_position_delete_files found duplicate live data file `{path}` in the current snapshot"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct PuffinDeleteRef {
    referenced_data_file: String,
    content_offset: i64,
    content_size_in_bytes: i64,
}

#[derive(Clone, Debug)]
struct LiveDeleteEntry {
    data_file: DataFile,
    partition_spec_id: i32,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
    rewrite_ref: Option<PuffinDeleteRef>,
}

#[derive(Clone, Debug)]
struct LiveDataEntry {
    data_file: DataFile,
    partition_spec_id: i32,
}

#[derive(Debug)]
struct ScannedDeleteManifest {
    manifest: ManifestFile,
    entries: Vec<LiveDeleteEntry>,
}

#[derive(Debug)]
struct RewritePlan {
    untouched_manifests: Vec<ManifestFile>,
    existing_entries: Vec<LiveDeleteEntry>,
    rewritten_entries: Vec<LiveDeleteEntry>,
    candidate_groups: BTreeMap<String, CandidateGroup>,
}

#[derive(Clone, Debug)]
struct CandidateGroup {
    referenced_data_file: String,
    entries: Vec<LiveDeleteEntry>,
    partition_spec_id: i32,
    partition: Struct,
    sequence_number: i64,
}

impl CandidateGroup {
    fn new(referenced_data_file: String, entries: Vec<LiveDeleteEntry>) -> Result<Self, String> {
        let Some(first) = entries.first() else {
            return Err(format!(
                "rewrite_position_delete_files candidate group `{referenced_data_file}` is empty"
            ));
        };
        let partition_spec_id = first.partition_spec_id;
        let partition = first.data_file.partition().clone();
        let sequence_number = first.sequence_number;
        for entry in &entries {
            if entry.partition_spec_id != partition_spec_id {
                return Err(format!(
                    "rewrite_position_delete_files cannot merge Puffin DVs for `{referenced_data_file}` across partition spec ids: {} and {}",
                    partition_spec_id, entry.partition_spec_id
                ));
            }
            if entry.data_file.partition() != &partition {
                return Err(format!(
                    "rewrite_position_delete_files cannot merge Puffin DVs for `{referenced_data_file}` across different partition tuples"
                ));
            }
            if entry.sequence_number != sequence_number {
                return Err(format!(
                    "rewrite_position_delete_files cannot merge Puffin DVs for `{referenced_data_file}` with different data sequence numbers: {} and {}",
                    sequence_number, entry.sequence_number
                ));
            }
        }
        Ok(Self {
            referenced_data_file,
            entries,
            partition_spec_id,
            partition,
            sequence_number,
        })
    }
}

fn validate_candidate_groups_against_live_data(
    groups: &BTreeMap<String, CandidateGroup>,
    live_data_files: &HashMap<String, LiveDataEntry>,
) -> Result<(), String> {
    for group in groups.values() {
        let live = live_data_files
            .get(&group.referenced_data_file)
            .ok_or_else(|| {
                format!(
                    "rewrite_position_delete_files candidate Puffin DV group references non-live data file `{}`",
                    group.referenced_data_file
                )
            })?;
        if group.partition_spec_id != live.partition_spec_id {
            return Err(format!(
                "rewrite_position_delete_files candidate Puffin DV group for `{}` has partition spec id {}, but live data file uses partition spec id {}",
                group.referenced_data_file, group.partition_spec_id, live.partition_spec_id
            ));
        }
        if group.partition != *live.data_file.partition() {
            return Err(format!(
                "rewrite_position_delete_files candidate Puffin DV group for `{}` has partition tuple {:?}, but live data file uses partition tuple {:?}",
                group.referenced_data_file,
                group.partition,
                live.data_file.partition()
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct WrittenCandidateDv {
    written: WrittenPuffinDv,
    partition_spec_id: i32,
    partition: Struct,
    sequence_number: i64,
}

#[derive(Default)]
struct RewriteArtifacts {
    puffin_paths: Mutex<Vec<String>>,
    manifest_paths: Mutex<Vec<String>>,
    cleared: AtomicBool,
}

struct AttemptFailure {
    error: iceberg::Error,
    cleanup_artifacts: bool,
}

impl AttemptFailure {
    fn cleanup(error: iceberg::Error) -> Self {
        Self {
            error,
            cleanup_artifacts: true,
        }
    }

    fn from_catalog_commit_error(error: iceberg::Error) -> Self {
        Self {
            cleanup_artifacts: should_cleanup_catalog_commit_error(&error),
            error,
        }
    }
}

#[derive(Debug)]
struct RewriteArtifactCleanupError {
    path: String,
    source: iceberg::Error,
}

impl RewriteArtifacts {
    fn record_puffin(&self, path: String) {
        self.puffin_paths
            .lock()
            .expect("rewrite artifact puffin_paths mutex poisoned")
            .push(path);
    }

    fn record_manifest(&self, path: String) {
        self.manifest_paths
            .lock()
            .expect("rewrite artifact manifest_paths mutex poisoned")
            .push(path);
    }

    fn drain_puffin_paths(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .puffin_paths
                .lock()
                .expect("rewrite artifact puffin_paths mutex poisoned"),
        )
    }

    fn drain_manifest_paths(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .manifest_paths
                .lock()
                .expect("rewrite artifact manifest_paths mutex poisoned"),
        )
    }

    async fn cleanup(&self, file_io: &FileIO) -> Vec<RewriteArtifactCleanupError> {
        if self.cleared.swap(true, Ordering::SeqCst) {
            return Vec::new();
        }

        let mut errors = Vec::new();
        for path in self
            .drain_puffin_paths()
            .into_iter()
            .chain(self.drain_manifest_paths())
        {
            if let Err(source) = file_io.delete(&path).await {
                errors.push(RewriteArtifactCleanupError { path, source });
            }
        }
        errors
    }

    #[cfg(test)]
    fn puffin_paths(&self) -> Vec<String> {
        self.puffin_paths
            .lock()
            .expect("rewrite artifact puffin_paths mutex poisoned")
            .clone()
    }

    #[cfg(test)]
    fn manifest_paths(&self) -> Vec<String> {
        self.manifest_paths
            .lock()
            .expect("rewrite artifact manifest_paths mutex poisoned")
            .clone()
    }
}

fn should_cleanup_catalog_commit_error(err: &iceberg::Error) -> bool {
    if is_retryable_commit_conflict(err) || err.kind() != iceberg::ErrorKind::Unexpected {
        return true;
    }

    let lower = format!("{err}").to_ascii_lowercase();
    let definite_signals = [
        "conflict",
        "assertrefsnapshotid",
        "ref_snapshot_id_match",
        "assertcurrentschemaidmatch",
        "schema id mismatch",
        "schemaidmatch",
        "spec id mismatch",
        "specidmatch",
        "data invalid",
        "datainvalid",
        "precondition failed",
        "preconditionfailed",
        "catalog commit conflict",
        "catalogcommitconflict",
    ];
    definite_signals.iter().any(|signal| lower.contains(signal))
}

fn classify_delete_file_for_rewrite(file: &DataFile) -> Result<Option<PuffinDeleteRef>, String> {
    match file.content_type() {
        DataContentType::PositionDeletes => match file.file_format() {
            DataFileFormat::Parquet => {
                Err("V2 Parquet position delete rewrite is not supported".to_string())
            }
            DataFileFormat::Puffin => {
                let referenced_data_file = file.referenced_data_file().ok_or_else(|| {
                    format!(
                        "Puffin DV {} missing referenced_data_file",
                        file.file_path()
                    )
                })?;
                let content_offset = file.content_offset().ok_or_else(|| {
                    format!("Puffin DV {} missing content_offset", file.file_path())
                })?;
                let content_size_in_bytes = file.content_size_in_bytes().ok_or_else(|| {
                    format!(
                        "Puffin DV {} missing content_size_in_bytes",
                        file.file_path()
                    )
                })?;
                Ok(Some(PuffinDeleteRef {
                    referenced_data_file,
                    content_offset,
                    content_size_in_bytes,
                }))
            }
            other => Err(format!(
                "rewrite_position_delete_files does not support position delete file format {other:?}"
            )),
        },
        DataContentType::EqualityDeletes => Ok(None),
        DataContentType::Data => Err(format!(
            "rewrite_position_delete_files found data file {} inside a delete manifest",
            file.file_path()
        )),
    }
}

async fn write_repacked_dvs(
    file_io: &FileIO,
    table_location: &str,
    commit_uuid: &Uuid,
    groups: &BTreeMap<String, CandidateGroup>,
    created: &RewriteArtifacts,
) -> Result<Vec<WrittenCandidateDv>, String> {
    let mut out = Vec::with_capacity(groups.len());
    for (idx, group) in groups.values().enumerate() {
        let mut merged = DeletionVector::new();
        for entry in &group.entries {
            let Some(puffin_ref) = &entry.rewrite_ref else {
                return Err(format!(
                    "rewrite_position_delete_files internal error: candidate `{}` contains non-Puffin delete file {}",
                    group.referenced_data_file,
                    entry.data_file.file_path()
                ));
            };
            let old = read_deletion_vector_puffin(
                file_io,
                entry.data_file.file_path(),
                puffin_ref.content_offset,
                puffin_ref.content_size_in_bytes,
            )
            .await
            .map_err(|e| {
                format!(
                    "read existing Puffin DV {} failed: {e}",
                    entry.data_file.file_path()
                )
            })?;
            merged.merge(&old);
        }
        let path = format!(
            "{table_location}/data/_staging/{commit_uuid}/rewrite-position-delete-dv-{idx}.puffin"
        );
        created.record_puffin(path.clone());
        let input = DeletionVectorBlobInput {
            referenced_data_file: group.referenced_data_file.clone(),
            deletion_vector: merged,
        };
        let mut written = write_multi_deletion_vector_puffin(file_io, &path, &[input])
            .await
            .map_err(|e| format!("write repacked Puffin DV {path} failed: {e}"))?;
        let written = written.pop().ok_or_else(|| {
            format!("write repacked Puffin DV {path} returned no deletion vector")
        })?;
        out.push(WrittenCandidateDv {
            written,
            partition_spec_id: group.partition_spec_id,
            partition: group.partition.clone(),
            sequence_number: group.sequence_number,
        });
    }
    Ok(out)
}

async fn write_existing_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<&LiveDeleteEntry>,
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
    for entry in entries {
        writer
            .add_existing_file(
                entry.data_file.clone(),
                entry.snapshot_id,
                entry.sequence_number,
                entry.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_existing_file failed: {e}"))?;
    }
    writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))
}

async fn write_deleted_delete_manifest(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<&LiveDeleteEntry>,
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
    for entry in entries {
        writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))
}

async fn write_added_dv_manifest(
    file_io: &FileIO,
    out_path: &str,
    dvs: &[&WrittenCandidateDv],
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
    for dv in dvs {
        writer
            .add_file(dv_data_file(dv)?, dv.sequence_number)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))
}

fn dv_data_file(dv: &WrittenCandidateDv) -> Result<DataFile, String> {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(dv.written.path.clone())
        .file_format(DataFileFormat::Puffin)
        .partition(dv.partition.clone())
        .partition_spec_id(dv.partition_spec_id)
        .record_count(dv.written.cardinality)
        .file_size_in_bytes(dv.written.file_size_in_bytes)
        .referenced_data_file(Some(dv.written.referenced_data_file.clone()))
        .content_offset(Some(dv.written.content_offset))
        .content_size_in_bytes(Some(dv.written.content_size_in_bytes))
        .build()
        .map_err(|e| format!("build rewritten Puffin DV DataFile failed: {e}"))
}

fn group_entries_by_partition_spec(
    entries: &[LiveDeleteEntry],
) -> BTreeMap<i32, Vec<&LiveDeleteEntry>> {
    let mut grouped = BTreeMap::new();
    for entry in entries {
        grouped
            .entry(entry.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(entry);
    }
    grouped
}

fn group_written_dvs_by_partition_spec(
    dvs: &[WrittenCandidateDv],
) -> BTreeMap<i32, Vec<&WrittenCandidateDv>> {
    let mut grouped = BTreeMap::new();
    for dv in dvs {
        grouped
            .entry(dv.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(dv);
    }
    grouped
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
                "rewrite_position_delete_files references unknown partition spec id {spec_id}"
            ))
        })
}

fn rewrite_outcome(
    plan: &RewritePlan,
    written_dvs: &[WrittenCandidateDv],
) -> Result<RewritePositionDeleteOutcome, String> {
    Ok(RewritePositionDeleteOutcome {
        rewritten_delete_files_count: checked_i32(
            plan.rewritten_entries.len(),
            "rewritten_delete_files_count",
        )?,
        added_delete_files_count: checked_i32(written_dvs.len(), "added_delete_files_count")?,
        rewritten_bytes_count: sum_rewritten_bytes(&plan.rewritten_entries)?,
        added_bytes_count: sum_added_bytes(written_dvs)?,
    })
}

fn rewrite_total_records(parent_total_records: Option<u64>) -> Result<u64, String> {
    parent_total_records.ok_or_else(|| {
        "rewrite_position_delete_files requires current snapshot summary `total-records`; cannot prove the delete-file-only Replace has no data-file effect"
            .to_string()
    })
}

fn rewrite_summary(
    outcome: &RewritePositionDeleteOutcome,
    total_records: u64,
) -> HashMap<String, String> {
    HashMap::from([
        (
            "rewritten-delete-files".to_string(),
            outcome.rewritten_delete_files_count.to_string(),
        ),
        (
            "added-delete-files".to_string(),
            outcome.added_delete_files_count.to_string(),
        ),
        (
            "rewritten-bytes".to_string(),
            outcome.rewritten_bytes_count.to_string(),
        ),
        (
            "added-bytes".to_string(),
            outcome.added_bytes_count.to_string(),
        ),
        ("added-data-files".to_string(), "0".to_string()),
        ("deleted-data-files".to_string(), "0".to_string()),
        ("total-records".to_string(), total_records.to_string()),
    ])
}

fn checked_i32(value: usize, name: &str) -> Result<i32, String> {
    i32::try_from(value)
        .map_err(|_| format!("rewrite_position_delete_files metric `{name}` overflow"))
}

fn sum_rewritten_bytes(entries: &[LiveDeleteEntry]) -> Result<i64, String> {
    entries.iter().try_fold(0_i64, |sum, entry| {
        let bytes = i64::try_from(entry.data_file.file_size_in_bytes())
            .map_err(|_| "rewritten delete file size overflow".to_string())?;
        sum.checked_add(bytes)
            .ok_or_else(|| "rewritten delete file bytes overflow".to_string())
    })
}

fn sum_added_bytes(dvs: &[WrittenCandidateDv]) -> Result<i64, String> {
    dvs.iter().try_fold(0_i64, |sum, dv| {
        let bytes = i64::try_from(dv.written.file_size_in_bytes)
            .map_err(|_| "added delete file size overflow".to_string())?;
        sum.checked_add(bytes)
            .ok_or_else(|| "added delete file bytes overflow".to_string())
    })
}

fn parse_bool_option(key: &str, value: &str) -> Result<bool, String> {
    if value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(format!(
            "rewrite_position_delete_files option `{key}` must be `true` or `false`"
        ))
    }
}

fn parse_usize_option(key: &str, value: &str) -> Result<usize, String> {
    let parsed = value.parse::<usize>().map_err(|_| {
        format!("rewrite_position_delete_files option `{key}` must be a positive integer")
    })?;
    if parsed == 0 {
        return Err(format!(
            "rewrite_position_delete_files option `{key}` must be >= 1"
        ));
    }
    Ok(parsed)
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::Path;

    #[test]
    fn options_default_min_input_files_is_two() {
        let options =
            RewritePositionDeleteOptions::from_map(&std::collections::BTreeMap::new()).unwrap();
        assert!(!options.rewrite_all);
        assert_eq!(options.min_input_files, 2);
    }

    #[test]
    fn options_reject_target_file_size_bytes_until_splitting_is_implemented() {
        let options = BTreeMap::from([(
            "target-file-size-bytes".to_string(),
            "134217728".to_string(),
        )]);
        let err = RewritePositionDeleteOptions::from_map(&options).unwrap_err();
        assert!(err.contains("target-file-size-bytes"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn options_reject_unknown_key() {
        let options = std::collections::BTreeMap::from([(
            "partial-progress.enabled".to_string(),
            "true".to_string(),
        )]);
        let err = RewritePositionDeleteOptions::from_map(&options).unwrap_err();
        assert!(err.contains("unsupported rewrite_position_delete_files option"));
    }

    #[test]
    fn v2_position_delete_detection_rejects_parquet_delete_file() {
        let file = iceberg::spec::DataFileBuilder::default()
            .content(iceberg::spec::DataContentType::PositionDeletes)
            .file_path("file:///tmp/delete.parquet".to_string())
            .file_format(iceberg::spec::DataFileFormat::Parquet)
            .partition(iceberg::spec::Struct::empty())
            .record_count(1)
            .file_size_in_bytes(64)
            .build()
            .unwrap();
        let err = classify_delete_file_for_rewrite(&file).unwrap_err();
        assert!(err.contains("V2 Parquet position delete rewrite is not supported"));
    }

    #[test]
    fn candidate_metadata_validation_rejects_dangling_referenced_data_file() {
        let referenced = "file:///tmp/data.parquet";
        let groups = BTreeMap::from([(
            referenced.to_string(),
            test_candidate_group(referenced, 0, iceberg::spec::Struct::empty()),
        )]);

        let err = validate_candidate_groups_against_live_data(&groups, &HashMap::new())
            .expect_err("dangling referenced data file must be rejected");

        assert!(err.contains("references non-live data file"));
        assert!(err.contains(referenced));
    }

    #[test]
    fn candidate_metadata_validation_rejects_partition_spec_mismatch() {
        let referenced = "file:///tmp/data.parquet";
        let groups = BTreeMap::from([(
            referenced.to_string(),
            test_candidate_group(referenced, 0, iceberg::spec::Struct::empty()),
        )]);
        let live = HashMap::from([(
            referenced.to_string(),
            test_live_data_entry(referenced, 7, iceberg::spec::Struct::empty()),
        )]);

        let err = validate_candidate_groups_against_live_data(&groups, &live)
            .expect_err("partition spec mismatch must be rejected");

        assert!(err.contains("partition spec"));
        assert!(err.contains(referenced));
    }

    #[test]
    fn candidate_metadata_validation_rejects_partition_tuple_mismatch() {
        let referenced = "file:///tmp/data.parquet";
        let groups = BTreeMap::from([(
            referenced.to_string(),
            test_candidate_group(referenced, 0, test_partition(1)),
        )]);
        let live = HashMap::from([(
            referenced.to_string(),
            test_live_data_entry(referenced, 0, test_partition(2)),
        )]);

        let err = validate_candidate_groups_against_live_data(&groups, &live)
            .expect_err("partition tuple mismatch must be rejected");

        assert!(err.contains("partition tuple"));
        assert!(err.contains(referenced));
    }

    #[test]
    fn candidate_group_rejects_incompatible_sequence_spec_and_partition() {
        let referenced = "file:///tmp/data.parquet";
        let base = test_live_delete_entry_with(
            "file:///tmp/delete-a.puffin",
            referenced,
            DataContentType::PositionDeletes,
            DataFileFormat::Puffin,
            0,
            iceberg::spec::Struct::empty(),
            10,
            Some(20),
        );

        let sequence_err = CandidateGroup::new(
            referenced.to_string(),
            vec![
                base.clone(),
                test_live_delete_entry_with(
                    "file:///tmp/delete-b.puffin",
                    referenced,
                    DataContentType::PositionDeletes,
                    DataFileFormat::Puffin,
                    0,
                    iceberg::spec::Struct::empty(),
                    11,
                    Some(21),
                ),
            ],
        )
        .unwrap_err();
        assert!(sequence_err.contains("different data sequence numbers"));

        let spec_err = CandidateGroup::new(
            referenced.to_string(),
            vec![
                base.clone(),
                test_live_delete_entry_with(
                    "file:///tmp/delete-c.puffin",
                    referenced,
                    DataContentType::PositionDeletes,
                    DataFileFormat::Puffin,
                    1,
                    iceberg::spec::Struct::empty(),
                    10,
                    Some(22),
                ),
            ],
        )
        .unwrap_err();
        assert!(spec_err.contains("partition spec ids"));

        let partition_err = CandidateGroup::new(
            referenced.to_string(),
            vec![
                base,
                test_live_delete_entry_with(
                    "file:///tmp/delete-d.puffin",
                    referenced,
                    DataContentType::PositionDeletes,
                    DataFileFormat::Puffin,
                    0,
                    test_partition(7),
                    10,
                    Some(23),
                ),
            ],
        )
        .unwrap_err();
        assert!(partition_err.contains("different partition tuples"));
    }

    #[test]
    fn build_rewrite_plan_splits_mixed_touched_manifest_and_preserves_sequences() {
        let data_a = "file:///tmp/data-a.parquet";
        let data_b = "file:///tmp/data-b.parquet";
        let data_c = "file:///tmp/data-c.parquet";
        let touched_manifest = fake_manifest(
            "file:///tmp/delete-touched.avro",
            0,
            ManifestContentType::Deletes,
        );
        let untouched_manifest = fake_manifest(
            "file:///tmp/delete-untouched.avro",
            0,
            ManifestContentType::Deletes,
        );
        let data_manifest = fake_manifest("file:///tmp/data.avro", 0, ManifestContentType::Data);
        let touched_entries = vec![
            test_live_delete_entry_with(
                "file:///tmp/delete-a-1.puffin",
                data_a,
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
                0,
                iceberg::spec::Struct::empty(),
                31,
                Some(41),
            ),
            test_live_delete_entry_with(
                "file:///tmp/delete-a-2.puffin",
                data_a,
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
                0,
                iceberg::spec::Struct::empty(),
                31,
                Some(42),
            ),
            test_live_delete_entry_with(
                "file:///tmp/delete-b-1.puffin",
                data_b,
                DataContentType::PositionDeletes,
                DataFileFormat::Puffin,
                0,
                iceberg::spec::Struct::empty(),
                51,
                Some(61),
            ),
            test_live_delete_entry_with(
                "file:///tmp/delete-eq.parquet",
                data_a,
                DataContentType::EqualityDeletes,
                DataFileFormat::Parquet,
                0,
                iceberg::spec::Struct::empty(),
                71,
                Some(81),
            ),
        ];
        let untouched_entries = vec![test_live_delete_entry_with(
            "file:///tmp/delete-c-1.puffin",
            data_c,
            DataContentType::PositionDeletes,
            DataFileFormat::Puffin,
            0,
            iceberg::spec::Struct::empty(),
            91,
            Some(101),
        )];
        let live_data = HashMap::from([
            (
                data_a.to_string(),
                test_live_data_entry(data_a, 0, iceberg::spec::Struct::empty()),
            ),
            (
                data_b.to_string(),
                test_live_data_entry(data_b, 0, iceberg::spec::Struct::empty()),
            ),
        ]);

        let plan = build_rewrite_plan_from_scanned(
            vec![data_manifest.clone()],
            vec![
                ScannedDeleteManifest {
                    manifest: touched_manifest,
                    entries: touched_entries,
                },
                ScannedDeleteManifest {
                    manifest: untouched_manifest.clone(),
                    entries: untouched_entries,
                },
            ],
            live_data,
            &RewritePositionDeleteOptions {
                rewrite_all: false,
                min_input_files: 2,
            },
        )
        .unwrap();

        assert_eq!(plan.candidate_groups.len(), 1);
        assert!(plan.candidate_groups.contains_key(data_a));
        assert_eq!(plan.rewritten_entries.len(), 2);
        assert_eq!(
            plan.rewritten_entries
                .iter()
                .map(|entry| (entry.sequence_number, entry.file_sequence_number))
                .collect::<Vec<_>>(),
            vec![(31, Some(41)), (31, Some(42))]
        );
        assert_eq!(plan.existing_entries.len(), 2);
        assert!(plan.existing_entries.iter().any(|entry| {
            entry.data_file.file_path() == "file:///tmp/delete-b-1.puffin"
                && entry.sequence_number == 51
                && entry.file_sequence_number == Some(61)
        }));
        assert!(plan.existing_entries.iter().any(|entry| {
            entry.data_file.content_type() == DataContentType::EqualityDeletes
                && entry.sequence_number == 71
                && entry.file_sequence_number == Some(81)
        }));
        assert_eq!(plan.untouched_manifests.len(), 2);
        assert!(
            plan.untouched_manifests
                .iter()
                .any(|mf| mf.manifest_path == data_manifest.manifest_path)
        );
        assert!(
            plan.untouched_manifests
                .iter()
                .any(|mf| mf.manifest_path == untouched_manifest.manifest_path)
        );
    }

    #[test]
    fn build_rewrite_plan_rejects_duplicate_live_data_files() {
        let data = "file:///tmp/data.parquet";
        let mut live_data = HashMap::new();
        record_live_data_file(
            &mut live_data,
            test_live_data_entry(data, 0, iceberg::spec::Struct::empty()).data_file,
            0,
        )
        .unwrap();
        let err = record_live_data_file(
            &mut live_data,
            test_live_data_entry(data, 0, iceberg::spec::Struct::empty()).data_file,
            0,
        )
        .unwrap_err();

        assert!(err.contains("duplicate live data file"));
        assert!(err.contains(data));
    }

    #[tokio::test]
    async fn rewrite_artifacts_cleanup_deletes_recorded_paths() {
        let dir = tempfile::tempdir().unwrap();
        let puffin = dir.path().join("staged.puffin");
        let manifest = dir.path().join("manifest.avro");
        fs::write(&puffin, b"dv").unwrap();
        fs::write(&manifest, b"manifest").unwrap();
        let file_io = FileIO::new_with_fs();

        let artifacts = RewriteArtifacts::default();
        artifacts.record_puffin(path_string(&puffin));
        artifacts.record_manifest(path_string(&manifest));

        let cleanup_errors = artifacts.cleanup(&file_io).await;

        assert!(
            cleanup_errors.is_empty(),
            "cleanup errors: {cleanup_errors:?}"
        );
        assert!(!puffin.exists());
        assert!(!manifest.exists());
        assert!(artifacts.puffin_paths().is_empty());
        assert!(artifacts.manifest_paths().is_empty());
    }

    #[test]
    fn catalog_commit_error_cleanup_policy_preserves_only_unknown_results() {
        let conflict = iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "Requirement failed: AssertRefSnapshotIdMatch",
        );
        assert!(should_cleanup_catalog_commit_error(&conflict));

        let invalid = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "bad metadata");
        assert!(should_cleanup_catalog_commit_error(&invalid));

        let wrapped_conflict = iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "catalog commit conflict on AssertRefSnapshotIdMatch",
        );
        assert!(should_cleanup_catalog_commit_error(&wrapped_conflict));

        let unknown = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "connection reset");
        assert!(!should_cleanup_catalog_commit_error(&unknown));
    }

    #[test]
    fn rewrite_summary_marks_replace_as_data_file_noop() {
        let summary = rewrite_summary(
            &RewritePositionDeleteOutcome {
                rewritten_delete_files_count: 2,
                added_delete_files_count: 1,
                rewritten_bytes_count: 128,
                added_bytes_count: 64,
            },
            42,
        );

        assert_eq!(summary.get("rewritten-delete-files").unwrap(), "2");
        assert_eq!(summary.get("added-delete-files").unwrap(), "1");
        assert_eq!(summary.get("rewritten-bytes").unwrap(), "128");
        assert_eq!(summary.get("added-bytes").unwrap(), "64");
        assert_eq!(summary.get("added-data-files").unwrap(), "0");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "0");
        assert_eq!(summary.get("total-records").unwrap(), "42");
    }

    #[test]
    fn rewrite_total_records_rejects_unknown_parent_total_records() {
        let err = rewrite_total_records(None).unwrap_err();
        assert!(err.contains("total-records"));
        assert!(err.contains("cannot prove"));
    }

    fn test_candidate_group(
        referenced_data_file: &str,
        partition_spec_id: i32,
        partition: iceberg::spec::Struct,
    ) -> CandidateGroup {
        CandidateGroup::new(
            referenced_data_file.to_string(),
            vec![test_live_delete_entry(
                referenced_data_file,
                partition_spec_id,
                partition,
            )],
        )
        .unwrap()
    }

    fn test_live_delete_entry(
        referenced_data_file: &str,
        partition_spec_id: i32,
        partition: iceberg::spec::Struct,
    ) -> LiveDeleteEntry {
        test_live_delete_entry_with(
            &format!("file:///tmp/delete-{partition_spec_id}.puffin"),
            referenced_data_file,
            DataContentType::PositionDeletes,
            DataFileFormat::Puffin,
            partition_spec_id,
            partition,
            11,
            Some(12),
        )
    }

    fn test_live_delete_entry_with(
        delete_file_path: &str,
        referenced_data_file: &str,
        content: DataContentType,
        format: DataFileFormat,
        partition_spec_id: i32,
        partition: iceberg::spec::Struct,
        sequence_number: i64,
        file_sequence_number: Option<i64>,
    ) -> LiveDeleteEntry {
        let data_file = iceberg::spec::DataFileBuilder::default()
            .content(content)
            .file_path(delete_file_path.to_string())
            .file_format(format)
            .partition(partition)
            .partition_spec_id(partition_spec_id)
            .record_count(1)
            .file_size_in_bytes(64)
            .pipe(|builder| {
                if content == DataContentType::PositionDeletes && format == DataFileFormat::Puffin {
                    builder
                        .referenced_data_file(Some(referenced_data_file.to_string()))
                        .content_offset(Some(4))
                        .content_size_in_bytes(Some(32))
                } else if content == DataContentType::EqualityDeletes {
                    builder.equality_ids(Some(vec![1]));
                    builder
                } else {
                    builder
                }
            })
            .build()
            .unwrap();
        let rewrite_ref = classify_delete_file_for_rewrite(&data_file).unwrap();
        LiveDeleteEntry {
            data_file,
            partition_spec_id,
            snapshot_id: 10,
            sequence_number,
            file_sequence_number,
            rewrite_ref,
        }
    }

    fn test_live_data_entry(
        path: &str,
        partition_spec_id: i32,
        partition: iceberg::spec::Struct,
    ) -> LiveDataEntry {
        let data_file = iceberg::spec::DataFileBuilder::default()
            .content(iceberg::spec::DataContentType::Data)
            .file_path(path.to_string())
            .file_format(iceberg::spec::DataFileFormat::Parquet)
            .partition(partition)
            .partition_spec_id(partition_spec_id)
            .record_count(1)
            .file_size_in_bytes(128)
            .build()
            .unwrap();
        LiveDataEntry {
            data_file,
            partition_spec_id,
        }
    }

    fn test_partition(value: i32) -> iceberg::spec::Struct {
        iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(value))])
    }

    fn fake_manifest(path: &str, spec_id: i32, content: ManifestContentType) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: 100,
            partition_spec_id: spec_id,
            content,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    fn path_string(path: &Path) -> String {
        path.to_str().unwrap().to_string()
    }

    trait Pipe: Sized {
        fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
            f(self)
        }
    }

    impl<T> Pipe for T {}
}
