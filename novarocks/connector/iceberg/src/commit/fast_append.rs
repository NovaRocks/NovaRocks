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

//! Self-assembled fast-append action for INSERT INTO.
//!
//! Every append — v2 and v3 — is staged by `FastAppendV3TxnAction` and
//! submitted through `helpers::submit_occ_action`. iceberg-rust's built-in
//! `Transaction::fast_append` is deliberately unused: `Transaction::commit`
//! re-runs every action against the base it just reloaded and therefore
//! recomputes each requirement from the value it is about to assert, which can
//! never reject a stale writer carrying an external write fence. V3
//! row-lineage tables additionally need the custom action so manifest-list
//! `first_row_id` and snapshot row ranges are populated for subsequent
//! `_row_id` scans and deletion-vector commits; v2 tables pass
//! `row_lineage: None` and stay free of every row-lineage field.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, ManifestFile, Operation, PartitionSpecRef, SchemaRef, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::table::Table;
use crate::iceberg::transaction::Transaction;
use crate::iceberg::transaction::{ActionCommit, TransactionAction};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction, merge_snapshot_summary_properties};
use super::helpers::{
    OccSubmit, effective_next_row_id, finalize_snapshot_summary, generate_snapshot_id,
    metadata_dir, now_ms, read_snapshot_manifest_list, required_target_ref_snapshot_id,
    snapshot_summary, snapshot_total_records, submit_occ_action, target_ref_snapshot_id,
    write_manifest_list,
};
use super::overwrite::write_added_data_manifest;
use crate::commit::{CommitOutcome, IcebergWriteMode, WrittenFile};

pub struct FastAppendCommit;

/// Commit an MV staging marker even when the change-stream produced no data
/// files. A frontend-owned MV refresh still publishes the staged snapshot, so
/// returning the parent snapshot would make the publication guard reject the
/// refresh marker.
pub(crate) async fn commit_empty_iceberg_mv_snapshot(
    ctx: CommitCtx<'_>,
) -> Result<CommitOutcome, String> {
    if ctx.snapshot_properties.is_empty() {
        let id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref).unwrap_or(0);
        return Ok(CommitOutcome {
            new_snapshot_id: id,
            written_manifest_paths: vec![],
        });
    }

    if matches!(
        crate::commit::classify_iceberg_write_mode(ctx.table),
        IcebergWriteMode::RowLineageV3
    ) {
        return commit_v3_row_lineage_append(ctx, Vec::new()).await;
    }
    if ctx.target_ref != "main" {
        return Err(format!(
            "MV empty snapshot branch target_ref={} requires the v3 row-lineage path",
            ctx.target_ref
        ));
    }

    commit_self_assembled_append(ctx, Vec::new(), None, "empty MV fast_append").await
}

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Ordinary empty input is a no-op. MV staging carries publication
        // marker properties and therefore needs a data-free snapshot instead.
        if written.is_empty() {
            return commit_empty_iceberg_mv_snapshot(ctx).await;
        }

        // FastAppendAction::validate_added_data_files rejects any non-Data
        // content — catch the misuse here with a clearer error.
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "FastAppendCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }

        if matches!(
            crate::commit::classify_iceberg_write_mode(ctx.table),
            IcebergWriteMode::RowLineageV3
        ) {
            return commit_v3_row_lineage_append(ctx, written).await;
        }

        if ctx.target_ref != "main" {
            return Err(format!(
                "FastAppendCommit branch target_ref={} requires the custom v3 row-lineage append path",
                ctx.target_ref
            ));
        }

        // V2 tables carry no row lineage at all: `row_lineage: None` keeps the
        // manifest list and snapshot free of `first_row_id` / row-range fields.
        commit_self_assembled_append(ctx, written, None, "fast_append").await
    }
}

async fn commit_v3_row_lineage_append(
    ctx: CommitCtx<'_>,
    written: Vec<WrittenFile>,
) -> Result<CommitOutcome, String> {
    let row_lineage_first_row_id = effective_next_row_id(ctx.table.metadata())?;
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    commit_self_assembled_append(
        ctx,
        written,
        Some((row_lineage_first_row_id, row_lineage_added_rows)),
        "fast_append v3",
    )
    .await
}

/// Stage one self-assembled append against the live base and submit it under
/// this attempt's external write fence.
///
/// `row_lineage` is `Some` only for v3 row-lineage tables; v2 passes `None` and
/// therefore produces no row-lineage fields at all.
async fn commit_self_assembled_append(
    ctx: CommitCtx<'_>,
    written: Vec<WrittenFile>,
    row_lineage: Option<(u64, u64)>,
    label: &str,
) -> Result<CommitOutcome, String> {
    let manifest_paths_out: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let action = Arc::new(FastAppendV3TxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema: ctx.table.metadata().current_schema().clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: ctx.abort_handle.clone(),
        manifest_paths_out: manifest_paths_out.clone(),
        row_lineage,
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
        #[cfg(test)]
        fail_before_manifest_list_write: false,
    });

    let guard = ctx.collector.fast_append_attempt_guard();
    match submit_occ_action(ctx.catalog, ctx.table, action, label, guard.as_deref()).await {
        Ok(OccSubmit::Committed(table_after)) => {
            let new_snapshot_id =
                required_target_ref_snapshot_id(table_after.metadata(), ctx.target_ref, label)?;
            Ok(CommitOutcome {
                new_snapshot_id,
                written_manifest_paths: collected_manifest_paths(&manifest_paths_out),
            })
        }
        // An append always stages `AddSnapshot` + `SetSnapshotRef`, so it never
        // proves itself a no-op. Report the same outcome an empty append input
        // reports at the entry points above rather than inventing a new one.
        Ok(OccSubmit::NoOp) => Ok(CommitOutcome {
            new_snapshot_id: target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref)
                .unwrap_or(0),
            written_manifest_paths: collected_manifest_paths(&manifest_paths_out),
        }),
        Err(error) => Err(error.into_detail()),
    }
}

/// Eagerly stage one append against this attempt's exact base without
/// dispatching it. The returned transaction-local table is the authority for
/// the snapshot id/sequence used by a following `SetStatistics` action.
pub(crate) async fn stage_eager_fast_append(
    ctx: CommitCtx<'_>,
) -> Result<(Transaction, CommitOutcome), String> {
    let written = ctx.collector.take_written_files()?;
    for file in &written {
        if file.content != DataContentType::Data {
            return Err(format!(
                "FastAppendCommit received {:?} content; expected Data only",
                file.content
            ));
        }
    }
    let row_lineage = match crate::commit::classify_iceberg_write_mode(ctx.table) {
        IcebergWriteMode::RowLineageV3 => {
            let first = effective_next_row_id(ctx.table.metadata())?;
            let rows = written.iter().try_fold(0u64, |sum, file| {
                sum.checked_add(file.record_count)
                    .ok_or_else(|| "row-lineage added row count overflow".to_string())
            })?;
            Some((first, rows))
        }
        IcebergWriteMode::LegacyPositionDeletes => {
            if ctx.target_ref != "main" {
                return Err(format!(
                    "FastAppendCommit branch target_ref={} requires the v3 row-lineage path",
                    ctx.target_ref
                ));
            }
            None
        }
    };

    // An ordinary empty append has no data-plane effect. A managed
    // publication carries provider properties and therefore still needs the
    // empty snapshot the custom action builds.
    if written.is_empty() && ctx.snapshot_properties.is_empty() {
        let snapshot_id = target_ref_snapshot_id(ctx.table.metadata(), ctx.target_ref).unwrap_or(0);
        return Ok((
            Transaction::new(ctx.table),
            CommitOutcome {
                new_snapshot_id: snapshot_id,
                written_manifest_paths: Vec::new(),
            },
        ));
    }

    let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
    let action: Arc<dyn TransactionAction> = Arc::new(FastAppendV3TxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema: ctx.table.metadata().current_schema().clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: ctx.abort_handle,
        manifest_paths_out: Arc::clone(&manifest_paths_out),
        row_lineage,
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
        #[cfg(test)]
        fail_before_manifest_list_write: false,
    });
    let tx = Transaction::new(ctx.table)
        .stage_action(action)
        .await
        .map_err(|error| format!("FastAppend eager stage failed: {error}"))?;
    let snapshot_id = required_target_ref_snapshot_id(
        tx.staged_table().metadata(),
        ctx.target_ref,
        "FastAppend eager stage",
    )?;
    Ok((
        tx,
        CommitOutcome {
            new_snapshot_id: snapshot_id,
            written_manifest_paths: collected_manifest_paths(&manifest_paths_out),
        },
    ))
}

fn collected_manifest_paths(manifest_paths_out: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .clone()
}

/// Snapshot changes built against an invisible staged table. The caller owns
/// catalog publication and must prepend the staged table's initialization
/// updates and submit exactly one assert-create commit.
#[allow(dead_code)]
pub(crate) struct StagedFastAppendAction {
    pub action: ActionCommit,
    pub outcome: Option<CommitOutcome>,
    pub abort_handle: Arc<crate::commit::abort::AbortLog>,
}

/// Build, but do not submit, v2 or v3 fast-append changes for atomic CTAS
/// publication. This deliberately bypasses `Transaction::commit`, whose first
/// step reloads a visible table and therefore cannot operate on a staged
/// create response.
#[allow(dead_code)]
pub(crate) async fn build_staged_fast_append_action(
    ctx: CommitCtx<'_>,
) -> Result<StagedFastAppendAction, String> {
    let written = ctx.collector.take_written_files()?;
    if written.is_empty() {
        return Ok(StagedFastAppendAction {
            action: ActionCommit::new(Vec::new(), Vec::new()),
            outcome: None,
            abort_handle: ctx.abort_handle,
        });
    }
    for file in &written {
        if file.content != DataContentType::Data {
            return Err(format!(
                "staged fast append received {:?} content; expected Data only",
                file.content
            ));
        }
    }
    if ctx.target_ref != "main" {
        return Err("atomic staged-table publication only supports the main ref".to_string());
    }

    if !matches!(
        crate::commit::classify_iceberg_write_mode(ctx.table),
        IcebergWriteMode::RowLineageV3
    ) {
        let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
        let action = FastAppendV3TxnAction {
            written,
            commit_uuid: ctx.commit_uuid,
            file_io: ctx.file_io.clone(),
            partition_spec: ctx.collector.partition_spec.clone(),
            schema: ctx.table.metadata().current_schema().clone(),
            schema_id: ctx.table.metadata().current_schema_id(),
            abort_handle: Arc::clone(&ctx.abort_handle),
            manifest_paths_out: Arc::clone(&manifest_paths_out),
            row_lineage: None,
            target_ref: ctx.target_ref.to_string(),
            snapshot_properties: ctx.snapshot_properties.clone(),
            #[cfg(test)]
            fail_before_manifest_list_write: false,
        };
        let mut action = Arc::new(action)
            .commit(ctx.table)
            .await
            .map_err(|error| format!("build staged v2 fast-append changes: {error}"))?;
        let updates = action.take_updates();
        let requirements = action.take_requirements();
        let new_snapshot_id = updates
            .iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
                _ => None,
            })
            .ok_or_else(|| {
                "staged v2 fast append did not build an add-snapshot update".to_string()
            })?;
        let written_manifest_paths = manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .clone();
        return Ok(StagedFastAppendAction {
            action: ActionCommit::new(updates, requirements),
            outcome: Some(CommitOutcome {
                new_snapshot_id,
                written_manifest_paths,
            }),
            abort_handle: ctx.abort_handle,
        });
    }

    let row_lineage_first_row_id = effective_next_row_id(ctx.table.metadata())?;
    let row_lineage_added_rows = written.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.record_count)
            .ok_or_else(|| "row-lineage added row count overflow".to_string())
    })?;
    let manifest_paths_out = Arc::new(Mutex::new(Vec::new()));
    let action = FastAppendV3TxnAction {
        written,
        commit_uuid: ctx.commit_uuid,
        file_io: ctx.file_io.clone(),
        partition_spec: ctx.collector.partition_spec.clone(),
        schema: ctx.table.metadata().current_schema().clone(),
        schema_id: ctx.table.metadata().current_schema_id(),
        abort_handle: Arc::clone(&ctx.abort_handle),
        manifest_paths_out: Arc::clone(&manifest_paths_out),
        row_lineage: Some((row_lineage_first_row_id, row_lineage_added_rows)),
        target_ref: ctx.target_ref.to_string(),
        snapshot_properties: ctx.snapshot_properties.clone(),
        #[cfg(test)]
        fail_before_manifest_list_write: false,
    };
    let mut action = Arc::new(action)
        .commit(ctx.table)
        .await
        .map_err(|error| format!("build staged fast-append changes: {error}"))?;
    let updates = action.take_updates();
    let requirements = action.take_requirements();
    let new_snapshot_id = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
            _ => None,
        })
        .ok_or_else(|| "staged fast append did not build an add-snapshot update".to_string())?;
    let written_manifest_paths = manifest_paths_out
        .lock()
        .expect("manifest_paths_out poisoned")
        .clone();
    Ok(StagedFastAppendAction {
        action: ActionCommit::new(updates, requirements),
        outcome: Some(CommitOutcome {
            new_snapshot_id,
            written_manifest_paths,
        }),
        abort_handle: ctx.abort_handle,
    })
}

struct FastAppendV3TxnAction {
    written: Vec<WrittenFile>,
    commit_uuid: Uuid,
    file_io: FileIO,
    partition_spec: PartitionSpecRef,
    schema: SchemaRef,
    schema_id: i32,
    abort_handle: Arc<crate::commit::abort::AbortLog>,
    manifest_paths_out: Arc<Mutex<Vec<String>>>,
    row_lineage: Option<(u64, u64)>,
    target_ref: String,
    snapshot_properties: BTreeMap<String, String>,
    #[cfg(test)]
    fail_before_manifest_list_write: bool,
}

#[async_trait]
impl TransactionAction for FastAppendV3TxnAction {
    async fn commit(self: Arc<Self>, table: &Table) -> crate::iceberg::Result<ActionCommit> {
        let m = table.metadata();
        let new_seq = m.last_sequence_number() + 1;
        let new_snapshot_id = generate_snapshot_id();
        let target_ref = &self.target_ref;
        let parent_snapshot_id = target_ref_snapshot_id(m, target_ref);
        let total_records = append_total_records(
            &self.written,
            snapshot_total_records(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?,
            parent_snapshot_id.is_some(),
        )
        .map_err(to_iceberg_unexpected)?;
        let parent_summary =
            snapshot_summary(m, parent_snapshot_id).map_err(to_iceberg_unexpected)?;
        let additional_properties = merge_snapshot_summary_properties(
            finalize_snapshot_summary(
                append_summary(&self.written, total_records),
                parent_summary,
                false,
            ),
            &self.snapshot_properties,
        )
        .map_err(to_iceberg_unexpected)?;
        let summary = Summary {
            operation: Operation::Append,
            additional_properties,
        };
        let metadata_dir = metadata_dir(table);

        let mut manifests: Vec<ManifestFile> =
            read_snapshot_manifest_list(m, &self.file_io, parent_snapshot_id)
                .await
                .map_err(to_iceberg_unexpected)?;

        let data_manifest_path = format!("{metadata_dir}/{}-append-data-0.avro", self.commit_uuid);
        self.abort_handle
            .record_manifest(data_manifest_path.clone());
        self.manifest_paths_out
            .lock()
            .expect("manifest_paths_out poisoned")
            .push(data_manifest_path.clone());
        let data_manifest = write_added_data_manifest(
            &self.file_io,
            &data_manifest_path,
            &self.written,
            self.partition_spec.clone(),
            self.schema.clone(),
            new_seq,
            new_snapshot_id,
            m.format_version(),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        manifests.push(data_manifest);

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
        #[cfg(test)]
        if self.fail_before_manifest_list_write {
            return Err(to_iceberg_unexpected(
                "injected staged append manifest-list write failure".to_string(),
            ));
        }
        let manifest_list_next_row_id = write_manifest_list(
            &self.file_io,
            &manifest_list_path,
            manifests,
            new_snapshot_id,
            parent_snapshot_id,
            new_seq,
            m.format_version(),
            self.row_lineage.map(|(first_row_id, _)| first_row_id),
        )
        .await
        .map_err(to_iceberg_unexpected)?;
        if let Some((first_row_id, added_rows)) = self.row_lineage {
            let expected_next_row_id = first_row_id.checked_add(added_rows).ok_or_else(|| {
                to_iceberg_unexpected(format!(
                    "Row ID overflow when computing append row lineage range: first_row_id={first_row_id}, added_rows={added_rows}"
                ))
            })?;
            if manifest_list_next_row_id != Some(expected_next_row_id) {
                return Err(to_iceberg_unexpected(format!(
                    "Manifest list row lineage mismatch: expected next-row-id {expected_next_row_id}, got {manifest_list_next_row_id:?}"
                )));
            }
        }

        let snapshot = Snapshot::builder()
            .with_snapshot_id(new_snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(new_seq)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(self.schema_id);
        let snapshot = match self.row_lineage {
            Some((first_row_id, added_rows)) => {
                snapshot.with_row_range(first_row_id, added_rows).build()
            }
            None => snapshot.build(),
        };
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
            TableRequirement::UuidMatch { uuid: m.uuid() },
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

fn append_total_records(
    written: &[WrittenFile],
    parent_total_records: Option<u64>,
    has_parent_snapshot: bool,
) -> Result<Option<u64>, String> {
    let added_records = written.iter().try_fold(0u64, |sum, f| {
        sum.checked_add(f.record_count)
            .ok_or_else(|| "append added row count overflow".to_string())
    })?;
    match (parent_total_records, has_parent_snapshot) {
        (Some(parent), _) => parent
            .checked_add(added_records)
            .map(Some)
            .ok_or_else(|| "append total-records overflow".to_string()),
        (None, false) => Ok(Some(added_records)),
        (None, true) => Ok(None),
    }
}

fn append_summary(
    written: &[WrittenFile],
    total_records: Option<u64>,
) -> std::collections::HashMap<String, String> {
    let mut p = std::collections::HashMap::new();
    let added_records = written.iter().map(|f| f.record_count).sum::<u64>();
    p.insert("added-data-files".to_string(), written.len().to_string());
    p.insert("added-records".to_string(), added_records.to_string());
    if let Some(total_records) = total_records {
        p.insert("total-records".to_string(), total_records.to_string());
    }
    p.insert(
        "added-files-size".to_string(),
        written
            .iter()
            .map(|f| f.file_size_in_bytes)
            .sum::<u64>()
            .to_string(),
    );
    p
}

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}
