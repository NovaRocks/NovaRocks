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
use super::fast_append::register_puffin_stats;
use super::helpers::{
    debug_assert_single_unmarked_row_bearing_data_manifest, effective_next_row_id,
    finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms,
    required_target_ref_snapshot_id, snapshot_summary, target_ref_snapshot_id, write_manifest_list,
};
use super::overwrite::{write_added_data_manifest, write_overwrite_deletes_manifest};
use super::types::{CommitOutcome, WrittenFile};
use crate::connector::iceberg::stats_assembler::CommitType;

// `Eq` is intentionally omitted: `appended_files: Vec<WrittenFile>` and
// `WrittenFile` is `PartialEq`-only (it carries stats fields not suited to `Eq`).
#[derive(Clone, Debug, PartialEq)]
pub struct CowUpdateRewriteSet {
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<CowUpdateTouchedFile>,
    /// BE-written data files that are NET-NEW to this commit (e.g. a folded MERGE
    /// not-matched INSERT), not tied to any rewritten `old_file`. Added to the same
    /// Overwrite snapshot alongside the rewrite outputs. Empty for a pure UPDATE.
    pub appended_files: Vec<WrittenFile>,
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
            let id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref).unwrap_or(0);
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

        let sketch_sets = ctx.collector.take_sketch_sets();
        let prev_snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref);

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("CowUpdate apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("CowUpdate commit failed: {e}"))?;
        let new_snapshot_id =
            required_target_ref_snapshot_id(table_after.metadata(), ctx.target_ref, "CowUpdate")?;
        let new_sequence_number = table_after.metadata().last_sequence_number();
        // CowUpdate replaces touched data files with rewritten ones; the
        // un-touched files remain live. Treat as Append so the new NDV is
        // an upper bound combining previous aggregate + new file sketches.
        register_puffin_stats(
            &table_after,
            ctx.catalog,
            ctx.file_io,
            CommitType::Append,
            sketch_sets,
            new_snapshot_id,
            new_sequence_number,
            prev_snapshot_id,
        )
        .await;
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
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let metadata_dir = metadata_dir(table);
        // REUSE base: the writer base / row-range floor used when there are no
        // appended (fresh) rows. Reuse manifests (rewrite outputs and carried
        // files) carry their own per-file `first_row_id` and do not draw from it;
        // they are marked already-assigned so the writer allocates nothing for them.
        let reuse_first_row_id = m.next_row_id();

        // FRESH base: net-new appended INSERT rows (a folded MERGE not-matched
        // INSERT) draw brand-new `_row_id`s, exactly like a standalone INSERT.
        // They must start at the table's true next-row-id — derived from the max
        // snapshot row-range end so we never collide with ids a non-echoing
        // catalog already handed out.
        let appended_rows = self
            .rewrite
            .appended_files
            .iter()
            .try_fold(0u64, |sum, f| {
                sum.checked_add(f.record_count)
                    .ok_or_else(|| to_iceberg_unexpected("appended row count overflow".to_string()))
            })?;
        let has_appended = !self.rewrite.appended_files.is_empty();
        let appended_first_row_id = if has_appended {
            effective_next_row_id(m).map_err(to_iceberg_unexpected)?
        } else {
            reuse_first_row_id
        };

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
        // Capture deleted-file metrics before index.touched_live is consumed by
        // the partition-spec grouping below. These feed the snapshot summary.
        let deleted_file_count = index.touched_live.len();
        let deleted_record_count: u64 = index
            .touched_live
            .iter()
            .map(|f| f.data_file.record_count())
            .sum();
        let removed_files_size: u64 = index
            .touched_live
            .iter()
            .map(|f| f.data_file.file_size_in_bytes())
            .sum();

        let touched_delete_groups = group_live_files_by_partition_spec(&index.touched_live);

        // Carried verbatim. Base data manifests written by this engine carry a `first_row_id`;
        // a foreign/pre-v3 manifest with `first_row_id == None` AND rows > 0 would be treated as
        // an unmarked advancer and trip the post-write next_row_id assertion below — that
        // fail-fast is intentional (no silent row-lineage corruption), not a bug.
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

        // Net-new appended data files (e.g. a folded MERGE not-matched INSERT)
        // are added to the same Overwrite snapshot. They are tied to no touched
        // `old_file`, so they remove nothing — only an added-data manifest is
        // written. Unlike the rewrite outputs (which preserve their scanned
        // `_row_id`s and are marked already-assigned), these rows are genuinely
        // new and MUST draw FRESH `_row_id`s. The manifest is left UNMARKED so
        // the v3 manifest-list writer allocates ids starting at
        // `appended_first_row_id` (= the table's effective next-row-id) and
        // advances `next_row_id` by exactly `Σ appended record_count`, mirroring
        // the added-data manifest in `overwrite.rs` / `fast_append.rs`. This
        // manifest is pushed LAST so every preceding (marked) manifest has
        // already been processed without moving the writer's counter, leaving it
        // at `appended_first_row_id` when this manifest is assigned.
        if has_appended {
            let appended_manifest_path = format!(
                "{metadata_dir}/{}-cow-update-appended-0.avro",
                self.commit_uuid
            );
            self.abort_handle
                .record_manifest(appended_manifest_path.clone());
            self.manifest_paths_out
                .lock()
                .expect("manifest_paths_out poisoned")
                .push(appended_manifest_path.clone());
            let appended_manifest = write_added_data_manifest(
                &self.file_io,
                &appended_manifest_path,
                &self.rewrite.appended_files,
                m.default_partition_spec().clone(),
                m.current_schema().clone(),
                new_seq,
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(appended_manifest);
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
        // The writer starts its counter at `appended_first_row_id`. The marked
        // rewrite/carried manifests are `(Some, Some)` and never move it; only
        // the unmarked appended manifest (if any) draws fresh ids and advances
        // the counter. When there are no appended files this equals
        // `m.next_row_id()`, so the pure-UPDATE / MOR-style reuse path is
        // byte-identical to before (row-range `(reuse_first_row_id, 0)`).
        debug_assert_single_unmarked_row_bearing_data_manifest(&new_manifests, has_appended);
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            Some(appended_first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        let expected_next_row_id = appended_first_row_id.checked_add(appended_rows).ok_or_else(|| {
            to_iceberg_unexpected(format!(
                "Row ID overflow computing COW UPDATE row lineage range: first_row_id={appended_first_row_id}, appended_rows={appended_rows}"
            ))
        })?;
        if manifest_list_next_row_id != Some(expected_next_row_id) {
            return Err(to_iceberg_unexpected(format!(
                "COW UPDATE row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
            )));
        }

        // Build canonical COW UPDATE summary: added keys from all written data
        // files (rewrite outputs + appended INSERT files); deleted keys from the
        // touched old data files.
        let mut summary_props = HashMap::new();
        summary_props.insert(
            "added-data-files".to_string(),
            self.written.len().to_string(),
        );
        summary_props.insert(
            "added-records".to_string(),
            self.written
                .iter()
                .map(|f| f.record_count)
                .sum::<u64>()
                .to_string(),
        );
        summary_props.insert(
            "added-files-size".to_string(),
            self.written
                .iter()
                .map(|f| f.file_size_in_bytes)
                .sum::<u64>()
                .to_string(),
        );
        summary_props.insert(
            "deleted-data-files".to_string(),
            deleted_file_count.to_string(),
        );
        summary_props.insert(
            "deleted-records".to_string(),
            deleted_record_count.to_string(),
        );
        summary_props.insert(
            "removed-files-size".to_string(),
            removed_files_size.to_string(),
        );
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: finalize_snapshot_summary(summary_props, parent_summary, false),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(m.current_schema_id())
            // Reuse rows (rewrite outputs) contribute 0; only the fresh appended
            // INSERT rows extend the snapshot's row-range. `appended_rows == 0`
            // for a pure UPDATE, preserving the prior `(first_row_id, 0)` shape.
            .with_row_range(appended_first_row_id, appended_rows)
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
    // Appended files are net-new data files (e.g. a folded MERGE not-matched
    // INSERT) that map to no `old_file`. They must be content==Data and must not
    // collide with a rewrite replacement path; a file is either a rewrite output
    // or net-new, never both.
    let mut appended_paths = HashSet::new();
    for appended in &rewrite.appended_files {
        if appended.content != DataContentType::Data {
            return Err(format!(
                "CowUpdateCommit appended file {} has {:?} content; expected Data only",
                appended.path, appended.content
            ));
        }
        if !appended_paths.insert(appended.path.clone()) {
            return Err(format!(
                "CowUpdateCommit received duplicate appended data file {}",
                appended.path
            ));
        }
        if rewrite_new_files.contains(&appended.path) {
            return Err(format!(
                "CowUpdateCommit appended data file {} also appears as a rewrite replacement file",
                appended.path
            ));
        }
    }

    let written_files: HashSet<String> = written.iter().map(|f| f.path.clone()).collect();
    if written_files.len() != written.len() {
        return Err("CowUpdateCommit received duplicate written data file paths".to_string());
    }
    for new_file in &rewrite_new_files {
        if !written_files.contains(new_file) {
            return Err(format!(
                "CowUpdateCommit rewrite replacement data file {new_file} was not written"
            ));
        }
    }
    for appended in &appended_paths {
        if !written_files.contains(appended) {
            return Err(format!(
                "CowUpdateCommit appended data file {appended} was not written"
            ));
        }
    }
    // Every collected written file must be either a rewrite replacement output
    // or a declared appended file; reject anything in neither set.
    for written_file in &written_files {
        if !rewrite_new_files.contains(written_file) && !appended_paths.contains(written_file) {
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
    use iceberg::spec::{DataFileFormat, Operation, Struct};

    #[test]
    fn type_compiles() {
        let rewrite = cow_rewrite();

        let commit = CowUpdateCommit { rewrite };
        assert_eq!(commit.rewrite.base_snapshot_id, 7);
    }

    /// M3a Part A: a COW UPDATE that folds a not-matched INSERT carries net-new
    /// `appended_files`. Those rows are genuinely new and MUST draw fresh
    /// `_row_id`s: the snapshot's row-range advances by exactly the appended
    /// files' `Σ record_count`, while the rewrite outputs reuse their preserved
    /// lineage and advance nothing. This pins the advance at the snapshot level
    /// (`row_range`) and the table level (`next_row_id`), and checks the
    /// per-manifest `first_row_id` split (rewrite manifest reuses the base id;
    /// the appended manifest starts at `effective_next_row_id`).
    #[tokio::test]
    async fn cow_update_appended_insert_files_allocate_fresh_row_ids() {
        use super::super::test_helpers::v3_table_with_n_data_files;

        // Seed a single data file with record_count=10 → fresh row-ids 0..10,
        // table next_row_id == 10.
        let fixture = v3_table_with_n_data_files(1).await;
        let file_io = fixture.table.file_io().clone();
        let base_next_row_id = effective_next_row_id(fixture.table.metadata()).unwrap();
        assert_eq!(base_next_row_id, 10, "fixture seeds row-ids 0..10");

        // Discover the seeded live data file's path and its assigned first_row_id.
        let live = single_live_data_file(&fixture.table, &file_io).await;
        let touched_first_row_id = live.first_row_id as i64;
        let touched_path = live.path();
        // Rewrite output reuses the matched rows' preserved lineage.
        let rewrite_path = format!("{touched_path}.rewrite.parquet");
        // Appended INSERT outputs are net-new (no _row_id).
        let appended_record_count: u64 = 7;
        let appended_path = format!("{touched_path}.insert.parquet");

        let rewrite = CowUpdateRewriteSet {
            base_snapshot_id: fixture
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id(),
            target_table_uuid: fixture.table.metadata().uuid().to_string(),
            // Only row 0 is updated; the rewrite file replays the whole touched file.
            updated_row_ids: vec![touched_first_row_id],
            touched_data_files: vec![CowUpdateTouchedFile {
                old_file: touched_path.clone(),
                new_files: vec![rewrite_path.clone()],
                // The touched file covers row-ids touched_first_row_id..+10.
                row_ids: (touched_first_row_id..touched_first_row_id + 10).collect(),
            }],
            appended_files: vec![written_data_file_with(
                &appended_path,
                appended_record_count,
                // FRESH: not-matched INSERT carries NO _row_id.
                None,
            )],
        };

        // BE writes both the rewrite replacement output (carrying preserved
        // _row_id) and the appended INSERT file (no _row_id).
        let rewrite_out = written_data_file_with(
            &rewrite_path,
            10,
            // REUSE: rewrite output preserves the touched rows' first_row_id.
            Some(touched_first_row_id),
        );
        let appended_out = written_data_file_with(&appended_path, appended_record_count, None);

        let outcome = run_cow_update_commit(&fixture, rewrite, vec![rewrite_out, appended_out])
            .await
            .expect("CowUpdateCommit with appended INSERT files succeeds");
        assert_ne!(outcome.new_snapshot_id, 0);

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after CowUpdate");
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("snapshot after CowUpdate");
        let (snapshot_first_row_id, snapshot_added_rows) = snap
            .row_range()
            .expect("v3 CowUpdate snapshot must carry a row range");

        // The fresh appended rows advance the range by exactly their record_count;
        // the rewrite outputs reuse their lineage and advance nothing.
        assert_eq!(
            snapshot_first_row_id, base_next_row_id,
            "fresh appended rows must start at the table's pre-commit next_row_id",
        );
        assert_eq!(
            snapshot_added_rows, appended_record_count,
            "snapshot row-range must advance by exactly the appended Σ record_count",
        );
        assert_eq!(
            reloaded.metadata().next_row_id(),
            base_next_row_id + appended_record_count,
            "table next_row_id must advance by exactly the appended fresh rows",
        );

        // Per-manifest split: the appended manifest starts fresh at base_next_row_id;
        // the rewrite manifest reuses the touched file's base id and is not fresh.
        let manifests = read_manifest_list(&reloaded).await;
        let appended_manifest = manifests
            .iter()
            .find(|mf| {
                mf.content == ManifestContentType::Data
                    && mf.added_files_count.unwrap_or(0) > 0
                    && mf.added_rows_count == Some(appended_record_count)
            })
            .expect("appended INSERT data manifest must be present");
        assert_eq!(
            appended_manifest.first_row_id,
            Some(base_next_row_id),
            "appended INSERT manifest must draw fresh ids starting at base_next_row_id",
        );
    }

    /// M3a Part A invariant: a pure UPDATE (empty `appended_files`) must remain
    /// byte-identical to the prior reuse path — the snapshot row-range stays
    /// `(reuse_first_row_id, 0)` and the table's `next_row_id` does not advance.
    #[tokio::test]
    async fn cow_update_pure_update_does_not_advance_row_ids() {
        use super::super::test_helpers::v3_table_with_n_data_files;

        let fixture = v3_table_with_n_data_files(1).await;
        let file_io = fixture.table.file_io().clone();
        let base_next_row_id = effective_next_row_id(fixture.table.metadata()).unwrap();
        let raw_next_row_id = fixture.table.metadata().next_row_id();

        let live = single_live_data_file(&fixture.table, &file_io).await;
        let touched_first_row_id = live.first_row_id as i64;
        let touched_path = live.path();
        let rewrite_path = format!("{touched_path}.rewrite.parquet");

        let rewrite = CowUpdateRewriteSet {
            base_snapshot_id: fixture
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id(),
            target_table_uuid: fixture.table.metadata().uuid().to_string(),
            updated_row_ids: vec![touched_first_row_id],
            touched_data_files: vec![CowUpdateTouchedFile {
                old_file: touched_path.clone(),
                new_files: vec![rewrite_path.clone()],
                row_ids: (touched_first_row_id..touched_first_row_id + 10).collect(),
            }],
            // Pure UPDATE: no net-new INSERT rows.
            appended_files: vec![],
        };
        let rewrite_out = written_data_file_with(&rewrite_path, 10, Some(touched_first_row_id));

        run_cow_update_commit(&fixture, rewrite, vec![rewrite_out])
            .await
            .expect("pure CowUpdate succeeds");

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after pure CowUpdate");
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("snapshot after pure CowUpdate");
        let (first_row_id, added_rows) = snap
            .row_range()
            .expect("v3 CowUpdate snapshot must carry a row range");
        assert_eq!(
            (first_row_id, added_rows),
            (raw_next_row_id, 0),
            "pure UPDATE must keep the reuse row-range shape (m.next_row_id(), 0)",
        );
        assert_eq!(
            reloaded.metadata().next_row_id(),
            base_next_row_id,
            "pure UPDATE must not advance the table's next_row_id",
        );
    }

    /// Read the single live data file from a table's current snapshot. Panics
    /// unless exactly one live data file is present.
    async fn single_live_data_file(
        table: &iceberg::table::Table,
        file_io: &iceberg::io::FileIO,
    ) -> LiveDataFile {
        let index = build_cow_snapshot_index(table, file_io, &HashSet::new(), "main")
            .await
            .expect("build_cow_snapshot_index");
        // With no touched paths, every live data file lands in untouched_manifests;
        // re-read them to expose the LiveDataFile records.
        let snapshot = table.metadata().current_snapshot().expect("snapshot");
        let manifest_list = snapshot
            .load_manifest_list(file_io, table.metadata())
            .await
            .expect("load manifest list");
        let mut found: Vec<LiveDataFile> = Vec::new();
        let mut next_first_row_id_base = 0u64;
        for mf in manifest_list.entries() {
            if mf.content != ManifestContentType::Data {
                continue;
            }
            let mut next_manifest_first_row_id = mf.first_row_id;
            let manifest = mf.load_manifest(file_io).await.expect("load data manifest");
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let df = entry.data_file();
                if df.content_type() != DataContentType::Data {
                    continue;
                }
                let first_row_id = df
                    .first_row_id()
                    .or(next_manifest_first_row_id.map(|v| v as i64))
                    .unwrap_or(next_first_row_id_base as i64);
                if let Some(next) = next_manifest_first_row_id.as_mut() {
                    *next += df.record_count();
                }
                next_first_row_id_base += df.record_count();
                found.push(LiveDataFile {
                    data_file: df.clone(),
                    partition_spec_id: mf.partition_spec_id,
                    snapshot_id: entry.snapshot_id().unwrap_or(mf.added_snapshot_id),
                    sequence_number: entry.sequence_number().unwrap_or(mf.sequence_number),
                    file_sequence_number: entry.file_sequence_number,
                    first_row_id: first_row_id as u64,
                });
            }
        }
        let _ = index;
        assert_eq!(
            found.len(),
            1,
            "fixture must expose exactly one live data file"
        );
        found.pop().unwrap()
    }

    impl LiveDataFile {
        fn path(&self) -> String {
            self.data_file.file_path().to_string()
        }
    }

    async fn read_manifest_list(table: &iceberg::table::Table) -> Vec<iceberg::spec::ManifestFile> {
        let snap = table.metadata().current_snapshot().expect("snapshot");
        let bytes = table
            .file_io()
            .new_input(snap.manifest_list())
            .expect("open manifest list")
            .read()
            .await
            .expect("read manifest list");
        iceberg::spec::ManifestList::parse_with_version(&bytes, table.metadata().format_version())
            .expect("parse manifest list")
            .entries()
            .to_vec()
    }

    /// Drive a `CowUpdateCommit` through a minimal collector with the given
    /// written files injected, mirroring `run_commit_with` but allowing
    /// pre-seeded `written` files (which the empty-collector helper does not).
    async fn run_cow_update_commit(
        fixture: &super::super::test_helpers::IcebergTestFixture,
        rewrite: CowUpdateRewriteSet,
        written: Vec<WrittenFile>,
    ) -> Result<CommitOutcome, String> {
        use super::super::collector::IcebergCommitCollector;
        use super::super::types::CommitOpKind;

        let metadata = fixture.table.metadata();
        let staging_dir = format!("{}/staging", metadata.location());
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::CowUpdate,
                fixture.table_ident.clone(),
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir,
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            )
            .with_table_metadata(metadata.clone()),
        );
        for wf in written {
            collector.inject_written_file(wf);
        }
        let file_io = fixture.table.file_io().clone();
        let abort_handle = collector.abort_log.clone();
        let snapshot_properties = BTreeMap::new();
        let ctx = CommitCtx {
            collector: &collector,
            table: &fixture.table,
            catalog: fixture.catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle,
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        };
        CowUpdateCommit { rewrite }.commit(ctx).await
    }

    fn written_data_file_with(
        path: &str,
        record_count: u64,
        first_row_id: Option<i64>,
    ) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count,
            file_size_in_bytes: 128,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
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

    /// M1: a COW MERGE folds a not-matched INSERT into the same Overwrite
    /// snapshot. The rewrite carries one touched file (old → [new_rewrite])
    /// PLUS one net-new appended INSERT data file that maps to no `old_file`.
    /// The commit must remove only the touched old file and add BOTH the
    /// rewritten file and the appended INSERT file. Validation must accept the
    /// appended file even though it is not the replacement output of any
    /// touched file.
    #[test]
    fn validate_cow_update_inputs_accepts_appended_insert_files() {
        let mut rewrite = cow_rewrite();
        rewrite.appended_files = vec![written_file("insert.parquet")];
        // BE writes both the rewrite replacement output and the appended
        // INSERT file; both arrive in the collected `written` set.
        let written = vec![written_file("new.parquet"), written_file("insert.parquet")];

        // Validation tolerates the appended file (written-but-not-a-rewrite-output).
        validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect("appended INSERT files must be accepted by validation");

        // Removed set = exactly the touched old file (appended files remove nothing).
        let removed = touched_old_file_paths(&rewrite);
        assert_eq!(
            removed,
            HashSet::from(["old.parquet".to_string()]),
            "removed set must be exactly the touched old file"
        );

        // Added set = union(new_files) ∪ appended_files — contains BOTH the
        // rewritten replacement file and the appended INSERT file.
        let added: HashSet<String> = rewrite
            .touched_data_files
            .iter()
            .flat_map(|f| f.new_files.iter().cloned())
            .chain(rewrite.appended_files.iter().map(|f| f.path.clone()))
            .collect();
        assert!(
            added.contains("new.parquet"),
            "added set must contain the rewritten replacement file"
        );
        assert!(
            added.contains("insert.parquet"),
            "added set must contain the appended INSERT file"
        );
        assert!(
            !added.contains("old.parquet"),
            "the touched old file is removed, not added"
        );
    }

    /// An appended file declared in `appended_files` but never actually written
    /// by the BE is a contract violation — validation must reject it.
    #[test]
    fn validate_cow_update_inputs_rejects_undeclared_appended_file() {
        let mut rewrite = cow_rewrite();
        rewrite.appended_files = vec![written_file("insert.parquet")];
        // Missing the appended file from the written set.
        let written = vec![written_file("new.parquet")];

        let err = validate_cow_update_inputs(&rewrite, &written, Some(7), "table-uuid")
            .expect_err("appended file missing from written set must fail");

        assert!(
            err.contains("insert.parquet"),
            "error must name the file: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Summary-building tests (IV3-2 fix: COW UPDATE must not leave an empty
    // summary that poisons the carry-chain for subsequent commits).
    // -----------------------------------------------------------------------

    /// Build a `Summary` representing a prior append snapshot with known
    /// `total-*` values to use as the `previous` argument.
    fn prior_summary(total_data_files: u64, total_records: u64, total_files_size: u64) -> Summary {
        let mut props = HashMap::new();
        props.insert("total-data-files".to_string(), total_data_files.to_string());
        props.insert("total-records".to_string(), total_records.to_string());
        props.insert("total-files-size".to_string(), total_files_size.to_string());
        props.insert("total-delete-files".to_string(), "0".to_string());
        props.insert("total-position-deletes".to_string(), "0".to_string());
        props.insert("total-equality-deletes".to_string(), "0".to_string());
        Summary {
            operation: Operation::Append,
            additional_properties: props,
        }
    }

    /// Verifies that building a COW UPDATE summary from a set of new and old
    /// files produces `engine-name = "novarocks"` and both `total-records` and
    /// `total-data-files` in the result.
    #[test]
    fn cow_update_summary_has_engine_name_and_totals() {
        // Simulate: 1 old file with 5 rows, replaced by 1 new file with 4 rows.
        let new_files = vec![written_file("new.parquet")]; // record_count=1, file_size=128
        let old_record_count: u64 = 5;
        let old_file_size: u64 = 256;

        let mut props = HashMap::new();
        props.insert("added-data-files".to_string(), new_files.len().to_string());
        props.insert(
            "added-records".to_string(),
            new_files
                .iter()
                .map(|f| f.record_count)
                .sum::<u64>()
                .to_string(),
        );
        props.insert(
            "added-files-size".to_string(),
            new_files
                .iter()
                .map(|f| f.file_size_in_bytes)
                .sum::<u64>()
                .to_string(),
        );
        props.insert("deleted-data-files".to_string(), "1".to_string());
        props.insert("deleted-records".to_string(), old_record_count.to_string());
        props.insert("removed-files-size".to_string(), old_file_size.to_string());

        // No prior snapshot → first snapshot baseline.
        let result = finalize_snapshot_summary(props.clone(), None, false);

        assert_eq!(
            result.get("engine-name").map(String::as_str),
            Some("novarocks"),
            "engine-name must be stamped"
        );
        assert!(
            result.contains_key("total-records"),
            "total-records must be present"
        );
        assert!(
            result.contains_key("total-data-files"),
            "total-data-files must be present"
        );
    }

    /// Regression: a COW UPDATE after a prior append must NOT omit `total-*`
    /// fields — an empty summary poisons the carry-chain so that every
    /// subsequent commit also loses its totals.
    ///
    /// Before the fix, `CowUpdateCommit` produced
    /// `Summary { additional_properties: {} }`, which caused
    /// `finalize_snapshot_summary` on the NEXT snapshot to return early
    /// (missing previous total), omitting all `total-*` fields forever.
    ///
    /// This test simulates the carry-chain:
    ///   snapshot-1 (append)  → prior with known totals
    ///   snapshot-2 (COW upd) → summary built via finalize_snapshot_summary
    ///   snapshot-3 (append)  → must still carry forward totals from snapshot-2
    #[test]
    fn cow_update_summary_carry_chain_not_poisoned() {
        // Step 1: prior snapshot after an append with known totals.
        let prior = prior_summary(
            /*total_data_files=*/ 2, /*total_records=*/ 20,
            /*total_files_size=*/ 2048,
        );

        // Step 2: build the COW UPDATE summary — deletes 1 old file (10 rows)
        // and adds 1 new file (8 rows).
        let mut cow_props = HashMap::new();
        cow_props.insert("added-data-files".to_string(), "1".to_string());
        cow_props.insert("added-records".to_string(), "8".to_string());
        cow_props.insert("added-files-size".to_string(), "512".to_string());
        cow_props.insert("deleted-data-files".to_string(), "1".to_string());
        cow_props.insert("deleted-records".to_string(), "10".to_string());
        cow_props.insert("removed-files-size".to_string(), "1024".to_string());

        let cow_summary_props = finalize_snapshot_summary(cow_props, Some(&prior), false);

        // Verify the COW UPDATE snapshot itself carries totals.
        assert_eq!(
            cow_summary_props
                .get("total-data-files")
                .map(String::as_str),
            Some("2"), // 2 - 1 + 1 = 2
            "COW UPDATE total-data-files must be carried forward"
        );
        assert_eq!(
            cow_summary_props.get("total-records").map(String::as_str),
            Some("18"), // 20 - 10 + 8 = 18
            "COW UPDATE total-records must be carried forward"
        );
        assert_eq!(
            cow_summary_props.get("engine-name").map(String::as_str),
            Some("novarocks"),
            "engine-name must be stamped on COW UPDATE snapshot"
        );

        // Step 3: simulate a next append (adding 5 new rows / 1 file).
        // This is the chain-poison regression: if cow_summary_props were empty,
        // finalize_snapshot_summary here would return early and omit totals.
        let cow_summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: cow_summary_props,
        };
        let mut next_props = HashMap::new();
        next_props.insert("added-data-files".to_string(), "1".to_string());
        next_props.insert("added-records".to_string(), "5".to_string());
        next_props.insert("added-files-size".to_string(), "256".to_string());

        let next_summary_props = finalize_snapshot_summary(next_props, Some(&cow_summary), false);

        assert_eq!(
            next_summary_props
                .get("total-data-files")
                .map(String::as_str),
            Some("3"), // 2 + 1 = 3
            "post-COW-update append total-data-files must be present (chain NOT poisoned)"
        );
        assert_eq!(
            next_summary_props.get("total-records").map(String::as_str),
            Some("23"), // 18 + 5 = 23
            "post-COW-update append total-records must be present (chain NOT poisoned)"
        );
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
            appended_files: vec![],
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
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }
}
