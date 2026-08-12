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

//! Engine-layer orchestrator that owns the IcebergCommitCollector lifecycle:
//! pick the right commit-action based on `CommitOpKind`, dispatch it, and on
//! failure decide whether to clean staged files or leave them for human
//! review (spec §5.4 — "commit unknown").
//!
//! Commit failure classification is delegated to `service.rs`, which exposes
//! typed lifecycle errors while preserving cleanup behavior for known
//! uncommitted failures.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::iceberg::Catalog;
use crate::iceberg::io::FileIO;
use crate::iceberg::table::Table;
use crate::iceberg::{TableCommit, TableUpdate};
use crate::opendal::Operator;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::collector::IcebergCommitCollector;
use super::fast_append::FastAppendCommit;
use super::overwrite::OverwriteCommit;
use super::rewrite_data_files::RewriteDataFilesCommit;
use super::row_delta::RowDeltaCommit;
use super::row_delta_dv::RowDeltaDvCommit;
use super::row_delta_dv_from_files::RowDeltaDvFromFilesCommit;
use super::selected_rewrite::SelectedRewriteCommit;
use super::service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, RecoveryEvidence, classify_commit_error,
};
use super::truncate::TruncateCommit;
use super::update_cow::CowUpdateCommit;
use super::update_cow::CowUpdateRewriteSet;
use crate::commit::{CommitOpKind, CommitOutcome};

pub type CleanupPathMapper = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub struct RunInput {
    pub collector: Arc<IcebergCommitCollector>,
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub fs: Operator,
    pub file_io: FileIO,
    pub cleanup_path_mapper: Option<CleanupPathMapper>,
    pub cow_update_rewrite: Option<CowUpdateRewriteSet>,
    pub selected_rewrite: Option<super::selected_rewrite::SelectedRewriteFiles>,
    /// Iceberg ref to commit to. `"main"` is the default; branch-qualified
    /// DML (`INSERT INTO t.branch_dev`) supplies the branch name here.
    pub target_ref: String,
    pub snapshot_properties: BTreeMap<String, String>,
    /// Provider-assigned partition-spec updates that must share the exact
    /// external commit with one managed overwrite snapshot on `main`.
    pub atomic_partition_replacement: Option<AtomicPartitionReplacement>,
}

pub(crate) struct AtomicPartitionReplacement {
    updates: Vec<TableUpdate>,
}

impl AtomicPartitionReplacement {
    pub(super) fn try_new(updates: Vec<TableUpdate>) -> Result<Self, String> {
        if updates.len() != 2
            || !matches!(updates[0], TableUpdate::AddSpec { .. })
            || !matches!(updates[1], TableUpdate::SetDefaultSpec { .. })
        {
            return Err(
                "atomic Iceberg partition replacement requires AddSpec then SetDefaultSpec"
                    .to_string(),
            );
        }
        Ok(Self { updates })
    }
}

/// Dispatch a commit-action and return typed commit outcome/error.
///
/// On definite commit failure this function runs best-effort abort cleanup and
/// returns `KnownUncommitted`. On commit-unknown failure it leaves staged files
/// untouched and returns `Unknown` with recovery evidence.
pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, CommitServiceError> {
    let RunInput {
        collector,
        catalog,
        table,
        fs,
        file_io,
        cleanup_path_mapper,
        cow_update_rewrite,
        selected_rewrite,
        target_ref,
        snapshot_properties,
        atomic_partition_replacement,
    } = input;

    if let Some(replacement) = atomic_partition_replacement {
        if collector.op_kind != CommitOpKind::Overwrite || target_ref != "main" {
            return Err(CommitServiceError::invalid_input(
                "atomic Iceberg partition replacement requires one managed overwrite on main"
                    .to_string(),
            ));
        }
        let commit_uuid = Uuid::new_v4();
        collector.set_manifest_cleanup_token(commit_uuid.to_string());
        let ctx = CommitCtx {
            collector: &collector,
            table: &table,
            catalog: catalog.as_ref(),
            file_io: &file_io,
            commit_uuid,
            abort_handle: collector.abort_log.clone(),
            target_ref: &target_ref,
            snapshot_properties: &snapshot_properties,
        };
        let result = run_atomic_partition_replacement(ctx, replacement).await;
        return match result {
            Ok(outcome) => {
                collector.mark_committed();
                Ok(outcome)
            }
            Err(error) => {
                Err(handle_commit_error(error, &collector, &fs, cleanup_path_mapper.as_ref()).await)
            }
        };
    }

    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
        CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
        CommitOpKind::RowDeltaDvFromFiles => Box::new(RowDeltaDvFromFilesCommit),
        CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
        CommitOpKind::SelectedRewrite => Box::new(SelectedRewriteCommit {
            files: selected_rewrite.ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "selected rewrite commit requires its frozen file set".to_string(),
                )
            })?,
        }),
        CommitOpKind::CowUpdate => Box::new(CowUpdateCommit {
            rewrite: cow_update_rewrite.ok_or_else(|| {
                CommitServiceError::invalid_input(
                    "CowUpdate commit requires a rewrite set".to_string(),
                )
            })?,
        }),
        CommitOpKind::Truncate => Box::new(TruncateCommit),
        CommitOpKind::OverwritePartitions => {
            Box::new(super::overwrite_partitions::OverwritePartitionsCommit)
        }
        CommitOpKind::RewriteManifests => {
            return Err(CommitServiceError::invalid_input(
                "CommitOpKind::RewriteManifests must be invoked via run_rewrite_manifests directly, not the collector dispatcher".to_string(),
            ));
        }
    };

    let commit_uuid = Uuid::new_v4();
    collector.set_manifest_cleanup_token(commit_uuid.to_string());
    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid,
        abort_handle: collector.abort_log.clone(),
        target_ref: &target_ref,
        snapshot_properties: &snapshot_properties,
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => {
            Err(
                handle_commit_error(commit_err, &collector, &fs, cleanup_path_mapper.as_ref())
                    .await,
            )
        }
    }
}

async fn run_atomic_partition_replacement(
    ctx: CommitCtx<'_>,
    replacement: AtomicPartitionReplacement,
) -> Result<CommitOutcome, String> {
    let mut staged = super::overwrite::build_staged_overwrite_action(ctx).await?;
    let snapshot_updates = staged.action.take_updates();
    if snapshot_updates.len() != 2
        || !matches!(snapshot_updates[0], TableUpdate::AddSnapshot { .. })
        || !matches!(snapshot_updates[1], TableUpdate::SetSnapshotRef { .. })
    {
        return Err(
            "atomic Iceberg repartition overwrite did not produce AddSnapshot then SetSnapshotRef"
                .to_string(),
        );
    }
    let mut updates = replacement.updates;
    updates.extend(snapshot_updates);
    let commit = TableCommit::builder()
        .ident(staged.table_ident.clone())
        .requirements(staged.action.take_requirements())
        .updates(updates)
        .build();
    staged
        .catalog
        .update_table(commit)
        .await
        .map_err(|error| format!("atomic Iceberg repartition commit failed: {error}"))?;
    Ok(staged.outcome)
}

async fn handle_commit_error(
    commit_err: String,
    collector: &Arc<IcebergCommitCollector>,
    fs: &Operator,
    cleanup_path_mapper: Option<&CleanupPathMapper>,
) -> CommitServiceError {
    match classify_commit_error(&commit_err) {
        CommitFailureKind::Unknown => {
            let evidence = RecoveryEvidence::from_collector(collector);
            tracing::warn!(
                op_kind = ?collector.op_kind,
                table = %collector.table_ident,
                base_snapshot_id = ?collector.base_snapshot_id,
                staging_dir = collector.staging_dir,
                "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
            );
            CommitServiceError::unknown(commit_err, evidence)
        }
        CommitFailureKind::FinalizeFailedKnownCommitted => {
            collector.mark_committed();
            CommitServiceError::finalize_failed_known_committed(
                None,
                commit_err,
                RecoveryEvidence::from_collector(collector),
            )
        }
        CommitFailureKind::KnownUncommitted => {
            let cleanup_errors = if let Some(mapper) = cleanup_path_mapper {
                collector
                    .abort_log
                    .cleanup_with_path_mapper(fs, |path| mapper(path))
                    .await
            } else {
                collector.abort_log.cleanup(fs).await
            };
            for e in &cleanup_errors {
                tracing::warn!(path = %e.path, source = ?e.source, "abort cleanup error");
            }
            CommitServiceError::known_uncommitted(
                commit_err,
                CleanupAttempt::from_cleanup_errors(&cleanup_errors),
            )
        }
    }
}
