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

//! `RewriteDataFilesCommit` — the OPTIMIZE whole-table rewrite commit-action.
//!
//! The action replaces every current live data file with the compacted data
//! files produced by the pipeline and deletes every current live delete file,
//! so the resulting snapshot has `summary.operation = "replace"` and a new
//! manifest list containing only the rewrite's deleted/added manifests.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestFile,
    ManifestWriterBuilder, Operation, PartitionSpecRef, PrimitiveLiteral, PrimitiveType, SchemaRef,
    Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::fast_append::carry_forward_puffin_stats;
use super::helpers::{
    finalize_snapshot_summary, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::overwrite::{build_minimal_data_file, write_added_data_manifest};
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct RewriteDataFilesCommit;

#[async_trait]
impl IcebergCommitAction for RewriteDataFilesCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let mut written = ctx.collector.take_written_files()?;
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "RewriteDataFilesCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }

        // Row-lineage `next_row_id` accounting splits three ways:
        //
        // 1. `preserve` — the upstream engine flow wrote data files whose
        //    `_row_id` values are already stamped at the reserved field IDs
        //    (e.g. OPTIMIZE row-lineage preserve). The Replace snapshot must
        //    still set `row_range` because iceberg-rs vendor 0.9 enforces a
        //    non-null `first-row-id` on V3 snapshots. Replacement data files
        //    get explicit `first_row_id`s from their stored `_row_id` bounds so
        //    readers do not inherit a fresh manifest-level range. The snapshot
        //    uses `added_rows = 0` so `next_row_id` does not advance.
        // 2. `RowLineageV3` non-preserve — historical behaviour: allocate a
        //    contiguous range of size `record_count` starting at the
        //    table's current `next_row_id`.
        // 3. `LegacyPositionDeletes` (V2) — no row-lineage at all; omit
        //    `row_range` entirely.
        let preserve_row_lineage = ctx.collector.preserve_row_lineage();
        if preserve_row_lineage {
            stamp_preserve_row_lineage_first_row_ids(&mut written)?;
        }
        let written_record_count = written.iter().try_fold(0u64, |sum, f| {
            sum.checked_add(f.record_count)
                .ok_or_else(|| "row-lineage rewrite added row count overflow".to_string())
        })?;
        let (row_lineage_first_row_id, row_lineage_added_rows) = if preserve_row_lineage {
            // Stamp first-row-id at the current next_row_id with 0 added rows
            // so the V3 snapshot is well-formed but next_row_id is unchanged.
            (Some(ctx.table.metadata().next_row_id()), 0u64)
        } else {
            match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
                IcebergWriteMode::RowLineageV3 => (
                    Some(ctx.table.metadata().next_row_id()),
                    written_record_count,
                ),
                IcebergWriteMode::LegacyPositionDeletes => (None, 0),
            }
        };

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = RewriteDataFilesTxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: ctx.abort_handle.clone(),
            manifest_paths_out: manifest_paths_out.clone(),
            row_lineage_first_row_id,
            row_lineage_added_rows,
            preserve_row_lineage,
            target_ref: ctx.target_ref.to_string(),
        };

        let prev_snapshot_id = ctx
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("RewriteDataFiles apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("RewriteDataFiles commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
        // Rewrite/compaction preserves logical row contents, so the previous
        // snapshot's Puffin NDV stays valid. Carry it forward unchanged.
        if let Some(prev) = prev_snapshot_id {
            carry_forward_puffin_stats(&table_after, ctx.catalog, new_snapshot_id, prev).await;
        }
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

struct RewriteDataFilesTxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema_id: i32,
    abort_handle: Arc<AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    /// First row id to record on the V3 snapshot's `row_range` field. Set
    /// when the table is V3 row-lineage and either (a) we are allocating a
    /// fresh range (`row_lineage_added_rows > 0`) or (b) we are preserving
    /// per-row identity from the data files (`row_lineage_added_rows = 0`,
    /// `preserve_row_lineage = true`). `None` for legacy V2 tables.
    row_lineage_first_row_id: Option<u64>,
    row_lineage_added_rows: u64,
    /// True when the data files supplied to this rewrite already carry
    /// `_row_id` at the reserved field IDs. The manifest list writer must
    /// then NOT assign new first-row-id ranges to manifest entries (would
    /// over-advance `next_row_id`), and the validation check between the
    /// expected next-row-id and the manifest writer's reported next-row-id
    /// is skipped because the two no longer reflect the same accounting.
    preserve_row_lineage: bool,
    target_ref: String,
}

#[async_trait]
impl TransactionAction for RewriteDataFilesTxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version == FormatVersion::V1 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "RewriteDataFilesCommit does not support V1 tables",
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
        let live = enumerate_live_files(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        if self.written.is_empty() && live.data_files.is_empty() && live.delete_files.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let mut new_manifests = Vec::new();
        for (idx, (spec_id, entries)) in group_by_partition_spec(&live.data_files)
            .into_iter()
            .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-rewrite-deleted-data-{idx}.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_deleted_manifest(
                &self.file_io,
                &path,
                entries,
                ManifestContentType::Data,
                partition_spec_by_id(m, spec_id)?,
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        if !self.written.is_empty() {
            let path = format!(
                "{metadata_dir}/{}-rewrite-added-data-0.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mut mf = if self.preserve_row_lineage {
                write_preserve_row_lineage_data_manifest(
                    &self.file_io,
                    &path,
                    &self.written,
                    self.partition_spec.clone(),
                    m.current_schema().clone(),
                    new_snapshot_id,
                    format_version,
                )
                .await
            } else {
                write_added_data_manifest(
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
            }
            .map_err(to_iceberg_unexpected)?;
            if self.preserve_row_lineage {
                let first_row_id =
                    preserve_replacement_manifest_first_row_id(&self.written)
                        .map_err(to_iceberg_unexpected)?
                        .or(self.row_lineage_first_row_id)
                        .ok_or_else(|| {
                            to_iceberg_unexpected(
                                "preserve-mode RewriteDataFilesCommit requires row lineage first_row_id"
                                    .to_string(),
                            )
                        })?;
                mf.first_row_id = Some(first_row_id);
            }
            new_manifests.push(mf);
        }

        for (idx, (spec_id, entries)) in group_by_partition_spec(&live.delete_files)
            .into_iter()
            .enumerate()
        {
            let path = format!(
                "{metadata_dir}/{}-rewrite-deleted-delete-{idx}.avro",
                self.commit_uuid
            );
            self.record_manifest_path(path.clone());
            let mf = write_deleted_manifest(
                &self.file_io,
                &path,
                entries,
                ManifestContentType::Deletes,
                partition_spec_by_id(m, spec_id)?,
                m.current_schema().clone(),
                new_snapshot_id,
                format_version,
            )
            .await
            .map_err(to_iceberg_unexpected)?;
            new_manifests.push(mf);
        }

        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.record_manifest_path(manifest_list_path.clone());
        // Preserve-mode replacement files store `_row_id` at the reserved
        // field IDs, but the v3 manifest-list writer still needs a starting
        // point so unassigned deleted-data manifests are accepted. The added
        // data manifest is marked above as already assigned, so initializing
        // the writer does not allocate fresh row ids for rewritten rows.
        let manifest_list_first_row_id = self.row_lineage_first_row_id;
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            new_manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            format_version,
            manifest_list_first_row_id,
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if !self.preserve_row_lineage
            && let Some(first_row_id) = self.row_lineage_first_row_id
        {
            let expected_next_row_id = first_row_id
                .checked_add(self.row_lineage_added_rows)
                .ok_or_else(|| {
                    to_iceberg_unexpected(format!(
                        "Row ID overflow when computing rewrite row lineage range: first_row_id={first_row_id}, added_rows={}",
                        self.row_lineage_added_rows
                    ))
                })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        let summary = Summary {
            operation: Operation::Replace,
            additional_properties: finalize_snapshot_summary(
                rewrite_summary(&self.written, &live),
                m.current_snapshot().map(|s| s.summary()),
                false,
            ),
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

impl RewriteDataFilesTxnAction {
    fn record_manifest_path(&self, path: String) {
        self.abort_handle.record_manifest(path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(path);
    }
}

struct LiveManifestEntry {
    data_file: DataFile,
    partition_spec_id: i32,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

#[derive(Default)]
struct LiveFiles {
    data_files: Vec<LiveManifestEntry>,
    delete_files: Vec<LiveManifestEntry>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiveFileMetrics {
    pub(crate) data_files: i64,
    pub(crate) delete_files: i64,
    pub(crate) data_bytes: i64,
    pub(crate) delete_bytes: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiveDataFileCompactionStats {
    pub(crate) max_compactable_data_files: i64,
}

#[cfg_attr(test, allow(dead_code))]
pub(crate) async fn count_current_live_files(
    table: &Table,
    file_io: &FileIO,
) -> Result<(i64, i64), String> {
    let metrics = current_live_file_metrics(table, file_io).await?;
    Ok((metrics.data_files, metrics.delete_files))
}

pub(crate) async fn current_live_file_metrics(
    table: &Table,
    file_io: &FileIO,
) -> Result<LiveFileMetrics, String> {
    let live = enumerate_live_files(table, file_io).await?;
    Ok(LiveFileMetrics {
        data_files: i64::try_from(live.data_files.len())
            .map_err(|_| "live data file count overflow".to_string())?,
        delete_files: i64::try_from(live.delete_files.len())
            .map_err(|_| "live delete file count overflow".to_string())?,
        data_bytes: live_file_bytes(&live.data_files, "data")?,
        delete_bytes: live_file_bytes(&live.delete_files, "delete")?,
    })
}

pub(crate) async fn current_live_data_file_compaction_stats(
    table: &Table,
    file_io: &FileIO,
    preserve_row_lineage: bool,
) -> Result<LiveDataFileCompactionStats, String> {
    let live = enumerate_live_files(table, file_io).await?;
    let mut groups: HashMap<String, i64> = HashMap::new();
    for entry in &live.data_files {
        let sequence = if preserve_row_lineage {
            Some(entry.sequence_number)
        } else {
            None
        };
        let key = format!(
            "spec={};partition={:?};sequence={:?}",
            entry.partition_spec_id,
            entry.data_file.partition(),
            sequence
        );
        let count = groups.entry(key).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or_else(|| "live data file compaction group count overflow".to_string())?;
    }
    Ok(LiveDataFileCompactionStats {
        max_compactable_data_files: groups.into_values().max().unwrap_or(0),
    })
}

fn live_file_bytes(files: &[LiveManifestEntry], label: &str) -> Result<i64, String> {
    files.iter().try_fold(0_i64, |sum, entry| {
        let bytes = i64::try_from(entry.data_file.file_size_in_bytes())
            .map_err(|_| format!("live {label} file size overflow"))?;
        sum.checked_add(bytes)
            .ok_or_else(|| format!("live {label} file bytes overflow"))
    })
}

async fn enumerate_live_files(table: &Table, file_io: &FileIO) -> Result<LiveFiles, String> {
    let mut out = LiveFiles::default();
    let m = table.metadata();
    let snapshot = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(out),
    };
    let list = snapshot
        .load_manifest_list(file_io, m)
        .await
        .map_err(|e| format!("load manifest list failed: {e}"))?;

    for mf in list.entries() {
        let manifest = mf
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {} failed: {e}", mf.manifest_path))?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let live = LiveManifestEntry {
                data_file: entry.data_file().clone(),
                partition_spec_id: mf.partition_spec_id,
                sequence_number: entry.sequence_number().unwrap_or(mf.sequence_number),
                file_sequence_number: entry.file_sequence_number,
            };
            match mf.content {
                ManifestContentType::Data => out.data_files.push(live),
                ManifestContentType::Deletes => out.delete_files.push(live),
            }
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn write_preserve_row_lineage_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    written: &[WrittenFile],
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
        FormatVersion::V3 => builder.build_v3_data(),
        FormatVersion::V1 | FormatVersion::V2 => {
            return Err("preserve row-lineage rewrite requires V3 data manifests".to_string());
        }
    };
    for f in written {
        let df = build_minimal_data_file(f)?;
        let sequence_number = preserve_row_lineage_sequence_number(f)?;
        writer
            .add_file(df, sequence_number)
            .map_err(|e| format!("ManifestWriter::add_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, ManifestContentType::Data);
    Ok(manifest_file)
}

#[allow(clippy::too_many_arguments)]
async fn write_deleted_manifest(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<&LiveManifestEntry>,
    content: ManifestContentType,
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
    let mut writer = match (format_version, content) {
        (FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (FormatVersion::V2, ManifestContentType::Deletes) => builder.build_v2_deletes(),
        (FormatVersion::V3, ManifestContentType::Deletes) => builder.build_v3_deletes(),
        (FormatVersion::V1, _) => return Err("phase 1 does not support V1 tables".to_string()),
    };
    for entry in entries {
        writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    let manifest_file = writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))?;
    debug_assert_eq!(manifest_file.content, content);
    Ok(manifest_file)
}

fn group_by_partition_spec(
    entries: &[LiveManifestEntry],
) -> BTreeMap<i32, Vec<&LiveManifestEntry>> {
    let mut grouped = BTreeMap::new();
    for entry in entries {
        grouped
            .entry(entry.partition_spec_id)
            .or_insert_with(Vec::new)
            .push(entry);
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
                "RewriteDataFilesCommit references unknown partition spec id {spec_id}"
            ))
        })
}

fn rewrite_summary(added: &[WrittenFile], live: &LiveFiles) -> HashMap<String, String> {
    let added_records = added.iter().map(|f| f.record_count).sum::<u64>();
    let deleted_records = live
        .data_files
        .iter()
        .map(|f| f.data_file.record_count())
        .sum::<u64>();
    let removed_position_delete_files = live
        .delete_files
        .iter()
        .filter(|f| f.data_file.content_type() == DataContentType::PositionDeletes)
        .count();
    let removed_equality_delete_files = live
        .delete_files
        .iter()
        .filter(|f| f.data_file.content_type() == DataContentType::EqualityDeletes)
        .count();
    let removed_position_deletes = live
        .delete_files
        .iter()
        .filter(|f| f.data_file.content_type() == DataContentType::PositionDeletes)
        .map(|f| f.data_file.record_count())
        .sum::<u64>();
    let removed_equality_deletes = live
        .delete_files
        .iter()
        .filter(|f| f.data_file.content_type() == DataContentType::EqualityDeletes)
        .map(|f| f.data_file.record_count())
        .sum::<u64>();
    let mut p = HashMap::new();
    p.insert("added-data-files".to_string(), added.len().to_string());
    p.insert(
        "deleted-data-files".to_string(),
        live.data_files.len().to_string(),
    );
    p.insert("added-records".to_string(), added_records.to_string());
    p.insert("deleted-records".to_string(), deleted_records.to_string());
    p.insert(
        "added-files-size".to_string(),
        added
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "removed-files-size".to_string(),
        live.data_files
            .iter()
            .chain(live.delete_files.iter())
            .map(|e| e.data_file.file_size_in_bytes())
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "removed-delete-files".to_string(),
        live.delete_files.len().to_string(),
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
        "removed-position-deletes".to_string(),
        removed_position_deletes.to_string(),
    );
    p.insert(
        "removed-equality-deletes".to_string(),
        removed_equality_deletes.to_string(),
    );
    p.insert("added-delete-files".to_string(), "0".to_string());
    p
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

fn stamp_preserve_row_lineage_first_row_ids(written: &mut [WrittenFile]) -> Result<(), String> {
    for file in written.iter_mut() {
        if file.record_count == 0 || file.first_row_id.is_some() {
            continue;
        }
        file.first_row_id = Some(row_id_lower_bound_as_first_row_id(file)?);
    }
    Ok(())
}

fn preserve_replacement_manifest_first_row_id(
    written: &[WrittenFile],
) -> Result<Option<u64>, String> {
    let mut first_row_id = None;
    for file in written.iter().filter(|file| file.record_count > 0) {
        let file_first_row_id = file.first_row_id.ok_or_else(|| {
            format!(
                "preserve-mode RewriteDataFilesCommit replacement data file {} is missing first_row_id",
                file.path
            )
        })?;
        let file_first_row_id = u64::try_from(file_first_row_id).map_err(|_| {
            format!(
                "preserve-mode RewriteDataFilesCommit replacement data file {} has negative first_row_id {}",
                file.path, file_first_row_id
            )
        })?;
        first_row_id = Some(first_row_id.map_or(file_first_row_id, |current: u64| {
            current.min(file_first_row_id)
        }));
    }
    Ok(first_row_id)
}

fn row_id_lower_bound_as_first_row_id(file: &WrittenFile) -> Result<i64, String> {
    let datum = file
        .lower_bounds
        .get(&ICEBERG_RESERVED_FIELD_ID_ROW_ID)
        .ok_or_else(|| {
            format!(
                "preserve-mode RewriteDataFilesCommit replacement data file {} is missing `{ICEBERG_ROW_ID_COL}` lower bound",
                file.path
            )
        })?;
    let (PrimitiveType::Long, PrimitiveLiteral::Long(first_row_id)) =
        (datum.data_type(), datum.literal())
    else {
        return Err(format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} has non-Long `{ICEBERG_ROW_ID_COL}` lower bound: type={}, value={}",
            file.path,
            datum.data_type(),
            datum
        ));
    };
    if *first_row_id < 0 {
        return Err(format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} has negative `{ICEBERG_ROW_ID_COL}` lower bound {}",
            file.path, first_row_id
        ));
    }
    Ok(*first_row_id)
}

fn preserve_row_lineage_sequence_number(file: &WrittenFile) -> Result<i64, String> {
    let lower = lineage_long_bound(
        file,
        &file.lower_bounds,
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ICEBERG_LAST_UPDATED_SEQ_COL,
        "lower",
    )?;
    let upper = lineage_long_bound(
        file,
        &file.upper_bounds,
        ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ICEBERG_LAST_UPDATED_SEQ_COL,
        "upper",
    )?;
    if lower != upper {
        return Err(format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} spans multiple `{ICEBERG_LAST_UPDATED_SEQ_COL}` values: lower={lower}, upper={upper}",
            file.path
        ));
    }
    if lower < 0 {
        return Err(format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} has negative `{ICEBERG_LAST_UPDATED_SEQ_COL}` {lower}",
            file.path
        ));
    }
    Ok(lower)
}

fn lineage_long_bound(
    file: &WrittenFile,
    bounds: &HashMap<i32, iceberg::spec::Datum>,
    field_id: i32,
    field_name: &str,
    label: &str,
) -> Result<i64, String> {
    let datum = bounds.get(&field_id).ok_or_else(|| {
        format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} is missing `{field_name}` {label} bound",
            file.path
        )
    })?;
    let (PrimitiveType::Long, PrimitiveLiteral::Long(value)) = (datum.data_type(), datum.literal())
    else {
        return Err(format!(
            "preserve-mode RewriteDataFilesCommit replacement data file {} has non-Long `{field_name}` {label} bound: type={}, value={}",
            file.path,
            datum.data_type(),
            datum
        ));
    };
    Ok(*value)
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataFileBuilder, DataFileFormat, Datum, Struct};

    use super::*;

    #[test]
    fn rewrite_summary_reports_replace_counts() {
        let added = vec![
            test_written_data_file("file:///x/new-1.parquet", 7),
            test_written_data_file("file:///x/new-2.parquet", 11),
        ];
        let live = LiveFiles {
            data_files: vec![test_live_entry(
                DataContentType::Data,
                "file:///x/old.parquet",
                23,
            )],
            delete_files: vec![
                test_live_entry(
                    DataContentType::PositionDeletes,
                    "file:///x/delete-1.parquet",
                    3,
                ),
                test_live_entry(
                    DataContentType::EqualityDeletes,
                    "file:///x/delete-2.parquet",
                    5,
                ),
            ],
        };

        let summary = rewrite_summary(&added, &live);

        assert_eq!(summary["added-data-files"], "2");
        assert_eq!(summary["deleted-data-files"], "1");
        assert_eq!(summary["added-records"], "18");
        assert_eq!(summary["deleted-records"], "23");
        // total-records is now computed by finalize_snapshot_summary, not rewrite_summary.
        assert!(
            !summary.contains_key("total-records"),
            "rewrite_summary must not emit total-records; finalize_snapshot_summary owns it"
        );
        // 2 files * 1024 each
        assert_eq!(summary["added-files-size"], "2048");
        // old data (1024) + position-delete (1024) + equality-delete (1024)
        assert_eq!(summary["removed-files-size"], "3072");
        assert_eq!(summary["removed-delete-files"], "2");
        assert_eq!(summary["removed-position-delete-files"], "1");
        assert_eq!(summary["removed-equality-delete-files"], "1");
        assert_eq!(summary["removed-position-deletes"], "3");
        assert_eq!(summary["removed-equality-deletes"], "5");
        assert_eq!(summary["added-delete-files"], "0");
    }

    #[test]
    fn preserve_row_lineage_stamps_replacement_first_row_id_from_bounds() {
        let mut written = vec![
            test_written_data_file_with_lineage_bounds("file:///x/rewrite-1.parquet", 3, 20, 4),
            test_written_data_file_with_lineage_bounds("file:///x/rewrite-2.parquet", 2, 7, 2),
        ];

        stamp_preserve_row_lineage_first_row_ids(&mut written).expect("stamp first_row_id");

        assert_eq!(written[0].first_row_id, Some(20));
        assert_eq!(written[1].first_row_id, Some(7));
        assert_eq!(
            preserve_replacement_manifest_first_row_id(&written).expect("manifest first row id"),
            Some(7),
            "preserve-mode manifest marker must come from replacement row ids, not table next_row_id"
        );
        assert_eq!(
            preserve_row_lineage_sequence_number(&written[0])
                .expect("last updated sequence number"),
            4
        );
    }

    #[test]
    fn preserve_row_lineage_rejects_missing_row_id_bound() {
        let mut written = vec![test_written_data_file("file:///x/rewrite.parquet", 1)];

        let err = stamp_preserve_row_lineage_first_row_ids(&mut written)
            .expect_err("missing row-id lower bound must fail");

        assert!(
            err.contains("missing `_row_id` lower bound"),
            "unexpected error: {err}"
        );
    }

    fn test_written_data_file(path: &str, record_count: u64) -> WrittenFile {
        WrittenFile {
            path: path.to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count,
            file_size_in_bytes: 1024,
            split_offsets: vec![4],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    fn test_written_data_file_with_lineage_bounds(
        path: &str,
        record_count: u64,
        row_id_lower_bound: i64,
        last_updated_lower_bound: i64,
    ) -> WrittenFile {
        let mut file = test_written_data_file(path, record_count);
        file.lower_bounds.insert(
            ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            Datum::long(row_id_lower_bound),
        );
        file.lower_bounds.insert(
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            Datum::long(last_updated_lower_bound),
        );
        file.upper_bounds.insert(
            ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            Datum::long(last_updated_lower_bound),
        );
        file
    }

    fn test_live_entry(
        content: DataContentType,
        path: &str,
        record_count: u64,
    ) -> LiveManifestEntry {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(record_count)
            .file_size_in_bytes(1024);
        if content == DataContentType::EqualityDeletes {
            builder.equality_ids(Some(vec![1]));
        }
        if content == DataContentType::PositionDeletes {
            builder.referenced_data_file(Some("file:///x/old.parquet".to_string()));
        }
        LiveManifestEntry {
            data_file: builder.build().unwrap(),
            partition_spec_id: 0,
            sequence_number: 1,
            file_sequence_number: Some(1),
        }
    }
}
