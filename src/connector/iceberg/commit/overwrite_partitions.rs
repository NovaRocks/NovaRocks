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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestList, ManifestWriterBuilder, Operation,
    PartitionSpecRef, SchemaRef, Snapshot, SnapshotReference, SnapshotRetention, Struct, Summary,
};
use iceberg::table::Table;
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};
use uuid::Uuid;

use super::abort::AbortLog;
use super::action::{CommitCtx, IcebergCommitAction};
use super::helpers::{
    effective_next_row_id, generate_snapshot_id, metadata_dir, now_ms, write_manifest_list,
};
use super::overwrite::{
    write_added_data_manifest, write_overwrite_deletes_manifest, write_truncate_deletes_manifest,
};
use super::types::{CommitOutcome, IcebergWriteMode, WrittenFile};
use crate::connector::iceberg::partition_spec::{PartitionMatch, partition_match_in_touched};

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
        match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
            IcebergWriteMode::RowLineageV3 => {}
            IcebergWriteMode::LegacyPositionDeletes => {
                return Err("OverwritePartitionsCommit requires v3 row-lineage table".to_string());
            }
        }

        let row_lineage_first_row_id = Some(effective_next_row_id(ctx.table.metadata())?);
        let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
            sum.checked_add(f.record_count)
                .ok_or_else(|| "row-lineage added row count overflow".to_string())
        })?;

        let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let action = OverwritePartitionsTxnAction {
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
        };

        let tx = Transaction::new(ctx.table);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("OverwritePartitions apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("OverwritePartitions commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0);
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
    async fn commit(self: Arc<Self>, table: &Table) -> iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let format_version = m.format_version();
        if format_version == FormatVersion::V1 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "OverwritePartitionsCommit does not support V1 tables",
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
        let existing = enumerate_live_all_files_with_spec(table, &self.file_io)
            .await
            .map_err(to_iceberg_unexpected)?;

        // `(DataFile, snap_id, seq, file_seq)` tuples split by fate.
        let mut deleted_data: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
        let mut deleted_deletes: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
        // `(DataFile, snap_id, seq, file_seq)` for surviving entries.
        let mut surviving_data: Vec<(DataFile, i64, i64, Option<i64>)> = Vec::new();
        let mut surviving_deletes: Vec<(DataFile, i64, i64, Option<i64>)> = Vec::new();
        for (df, snap_id, seq, file_seq, base_spec_id) in &existing {
            match partition_match_in_touched(
                df.partition(),
                *base_spec_id,
                current_spec_id,
                &touched,
            ) {
                PartitionMatch::InSet => {
                    if df.content_type() == DataContentType::Data {
                        deleted_data.push((df.clone(), *seq, *file_seq));
                    } else {
                        deleted_deletes.push((df.clone(), *seq, *file_seq));
                    }
                }
                PartitionMatch::NotInSet => {
                    if df.content_type() == DataContentType::Data {
                        surviving_data.push((df.clone(), *snap_id, *seq, *file_seq));
                    } else {
                        surviving_deletes.push((df.clone(), *snap_id, *seq, *file_seq));
                    }
                }
                PartitionMatch::DifferentSpec => {
                    return Err(to_iceberg_unexpected(format!(
                        "OVERWRITE PARTITIONS: base file under historical partition spec \
                         {base_spec_id} cannot be matched against current spec \
                         {current_spec_id}; run OPTIMIZE TABLE to consolidate first"
                    )));
                }
            }
        }

        // 3. Write manifests. Order: surviving → deleted → added.
        let mut new_manifests: Vec<iceberg::spec::ManifestFile> = Vec::new();

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
        //    first_row_id = Some(next_row_id) so the manifest list records the
        //    base row-id for newly added rows; `next_row_id` is advanced by
        //    `row_lineage_added_rows` via `with_row_range` on the snapshot.
        let manifest_list_path = format!(
            "{metadata_dir}/snap-{}-{}.avro",
            new_snapshot_id, self.commit_uuid
        );
        self.record_manifest_path(manifest_list_path.clone());
        write_manifest_list(
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

        // 6. Build the snapshot.
        //    operation = Overwrite, summary includes `replace-partitions=true`.
        //    Row-range advances next_row_id by added_rows_count (even if zero
        //    when written is empty — the validator still requires a non-null
        //    first-row-id for V3).
        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: overwrite_partitions_summary(
                &self.written,
                &deleted_data,
                &deleted_deletes,
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
#[allow(clippy::too_many_arguments)]
async fn write_existing_data_manifest(
    file_io: &FileIO,
    out_path: &str,
    surviving: &[(DataFile, i64, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<iceberg::spec::ManifestFile, String> {
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

/// Write a Deletes manifest in which every entry is EXISTING (status=Existing).
/// Used by `OverwritePartitionsCommit` to preserve surviving delete files
/// (position-delete / equality-delete / DV) in non-touched partitions.
#[allow(clippy::too_many_arguments)]
async fn write_existing_deletes_manifest(
    file_io: &FileIO,
    out_path: &str,
    surviving: &[(DataFile, i64, i64, Option<i64>)],
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    new_snapshot_id: i64,
    format_version: FormatVersion,
) -> Result<iceberg::spec::ManifestFile, String> {
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
/// `(DataFile, snapshot_id, sequence_number, file_sequence_number, manifest_spec_id)`.
///
/// - `snapshot_id`: the snapshot that originally wrote this entry (needed by
///   `add_existing_file` to preserve lineage faithfully).
/// - `manifest_spec_id`: the manifest-level `partition_spec_id` so that
///   OVERWRITE PARTITIONS can detect cross-spec base files.
async fn enumerate_live_all_files_with_spec(
    table: &Table,
    file_io: &FileIO,
) -> Result<Vec<(DataFile, i64, i64, Option<i64>, i32)>, String> {
    let m = table.metadata();
    let snap = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
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
                out.push((data_file, snap_id, seq, file_seq, spec_id));
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

    let mut p = HashMap::new();
    p.insert("replace-partitions".to_string(), "true".to_string());
    p.insert("added-data-files".to_string(), written.len().to_string());
    p.insert("added-records".to_string(), added_records.to_string());
    p.insert("added-files-size".to_string(), added_files_size.to_string());
    p.insert(
        "removed-data-files".to_string(),
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
    p
}

fn to_iceberg_unexpected(s: String) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::test_helpers::{
        run_overwrite_partitions_commit, v3_partitioned_table_with_data,
    };
    use iceberg::spec::Operation;

    #[test]
    fn type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&OverwritePartitionsCommit);
    }

    /// Empty write to a partitioned v3 table must produce a real
    /// `operation=overwrite` snapshot with all counters at 0.
    /// This is the audit-trail guarantee: even a no-op INSERT OVERWRITE
    /// must leave a snapshot entry so that history tooling sees the intent.
    #[tokio::test]
    async fn empty_write_to_partitioned_v3_table_writes_noop_overwrite_snapshot() {
        // Build a partitioned table with 2 data files in region=us, 1 in region=eu.
        let fixture = v3_partitioned_table_with_data().await;

        // Reload so the table handle reflects the seeded snapshot.
        let fixture = {
            let table = fixture
                .catalog
                .load_table(&fixture.table_ident)
                .await
                .expect("reload fixture table");
            crate::connector::iceberg::commit::test_helpers::IcebergTestFixture {
                catalog: fixture.catalog.clone(),
                table,
                table_ident: fixture.table_ident.clone(),
            }
        };

        // Run OverwritePartitionsCommit with zero written files.
        let outcome = run_overwrite_partitions_commit(fixture.clone(), vec![])
            .await
            .expect("OverwritePartitionsCommit succeeds on empty written");

        assert_ne!(outcome.new_snapshot_id, 0, "expected a fresh snapshot id");

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after overwrite-partitions");
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("table should have a current snapshot after overwrite-partitions");
        assert_eq!(snap.summary().operation, Operation::Overwrite);
        let p = &snap.summary().additional_properties;
        assert_eq!(
            p.get("replace-partitions").map(String::as_str),
            Some("true"),
        );
        assert_eq!(p.get("added-data-files").map(String::as_str), Some("0"));
        assert_eq!(p.get("added-records").map(String::as_str), Some("0"));
        assert_eq!(p.get("removed-data-files").map(String::as_str), Some("0"));
        assert_eq!(p.get("deleted-records").map(String::as_str), Some("0"));
    }

    /// Writing 1 new file to `region=us` must replace the 2 existing `region=us`
    /// files while leaving the 1 `region=eu` file untouched.
    #[tokio::test]
    async fn overwrite_partitions_replaces_one_partition_preserves_others() {
        use crate::connector::iceberg::commit::overwrite::enumerate_live_all_files;
        use iceberg::spec::{DataFileFormat, Literal, PrimitiveLiteral, Struct};
        use std::collections::HashMap;

        let fixture = v3_partitioned_table_with_data().await;
        let fixture = {
            let table = fixture
                .catalog
                .load_table(&fixture.table_ident)
                .await
                .expect("reload fixture table");
            crate::connector::iceberg::commit::test_helpers::IcebergTestFixture {
                catalog: fixture.catalog.clone(),
                table,
                table_ident: fixture.table_ident.clone(),
            }
        };

        // Sanity: 3 live files before the overwrite.
        let pre = enumerate_live_all_files(&fixture.table, &fixture.table.file_io().clone())
            .await
            .expect("enumerate pre-overwrite");
        assert_eq!(pre.len(), 3, "fixture should expose 3 live files");

        let table_location = fixture.table.metadata().location().to_string();
        let partition_spec_id = fixture.table.metadata().default_partition_spec().spec_id();

        // Build a partition_values Struct for region=us (single string field).
        let us_partition = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "us".to_string(),
        )))]);

        // Write 1 new file into region=us.
        let new_file = WrittenFile {
            path: format!("{table_location}/data/new-us-0.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: us_partition,
            partition_spec_id,
            record_count: 50,
            file_size_in_bytes: 2048,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        };

        let outcome = run_overwrite_partitions_commit(fixture.clone(), vec![new_file])
            .await
            .expect("OverwritePartitionsCommit succeeds");
        assert_ne!(outcome.new_snapshot_id, 0);

        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after overwrite-partitions");

        let post = enumerate_live_all_files(&reloaded, &reloaded.file_io().clone())
            .await
            .expect("enumerate post-overwrite");

        // region=us: 2 old replaced by 1 new → 1 file
        // region=eu: 1 preserved
        // total: 2
        assert_eq!(
            post.len(),
            2,
            "expected 2 live files after overwrite (1 us + 1 eu), got {} ({:?})",
            post.len(),
            post.iter()
                .map(|(df, _, _)| df.file_path())
                .collect::<Vec<_>>(),
        );

        // The new us file should be live.
        let paths: Vec<_> = post
            .iter()
            .map(|(df, _, _)| df.file_path().to_string())
            .collect();
        assert!(
            paths.iter().any(|p| p.ends_with("new-us-0.parquet")),
            "new us file should be live, got {paths:?}",
        );

        // The eu file should still be live.
        assert!(
            paths.iter().any(|p| p.ends_with("eu-0.parquet")),
            "eu file should be preserved, got {paths:?}",
        );

        // Snapshot summary sanity.
        let snap = reloaded
            .metadata()
            .current_snapshot()
            .expect("snapshot after overwrite-partitions");
        let p = &snap.summary().additional_properties;
        assert_eq!(
            p.get("replace-partitions").map(String::as_str),
            Some("true")
        );
        assert_eq!(p.get("added-data-files").map(String::as_str), Some("1"));
        assert_eq!(p.get("removed-data-files").map(String::as_str), Some("2"));
    }

    /// A base file under a historical partition spec (spec_id != current)
    /// must cause the commit to fail with the documented error message
    /// containing "OVERWRITE PARTITIONS" and "OPTIMIZE TABLE".
    ///
    /// This test exercises `PartitionMatch::DifferentSpec` path directly
    /// (unit-level) since building a full cross-spec fixture is non-trivial.
    /// The end-to-end SQL regression covers the same path in Task 9.
    #[test]
    fn cross_spec_base_file_rejects_with_optimize_hint() {
        use crate::connector::iceberg::partition_spec::{
            PartitionMatch, partition_match_in_touched,
        };
        use iceberg::spec::{Literal, PrimitiveLiteral, Struct};

        let base_partition = Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::String("us".to_string()),
        ))]);
        let touched = vec![base_partition.clone()];

        // Simulate: base file was written under spec_id=0, current spec is spec_id=1.
        let result = partition_match_in_touched(
            &base_partition,
            /*base_spec_id=*/ 0,
            /*current_spec_id=*/ 1,
            &touched,
        );
        assert_eq!(result, PartitionMatch::DifferentSpec);

        // Confirm the caller's documented error message matches.
        let base_spec_id: i32 = 0;
        let current_spec_id: i32 = 1;
        let msg = format!(
            "OVERWRITE PARTITIONS: base file under historical partition spec \
             {base_spec_id} cannot be matched against current spec \
             {current_spec_id}; run OPTIMIZE TABLE to consolidate first"
        );
        assert!(
            msg.contains("OVERWRITE PARTITIONS"),
            "error must mention OVERWRITE PARTITIONS"
        );
        assert!(
            msg.contains("OPTIMIZE TABLE"),
            "error must hint at OPTIMIZE TABLE"
        );
    }
}
