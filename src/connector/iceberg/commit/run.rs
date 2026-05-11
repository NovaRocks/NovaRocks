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
//! The classification of commit failures is informed by spike
//! `docs/superpowers/spikes/2026-04-28-commit-unknown-classification.md`:
//! only `iceberg::ErrorKind::Unexpected` is treated as commit-unknown, every
//! other variant means we know the commit definitively failed and may safely
//! clean up.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::table::Table;
use opendal::Operator;
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::collector::IcebergCommitCollector;
use super::fast_append::FastAppendCommit;
use super::overwrite::OverwriteCommit;
use super::rewrite_data_files::RewriteDataFilesCommit;
use super::row_delta::RowDeltaCommit;
use super::row_delta_dv::RowDeltaDvCommit;
use super::truncate::TruncateCommit;
use super::types::{CommitOpKind, CommitOutcome};
use super::update_cow::CowUpdateCommit;
use super::update_cow::CowUpdateRewriteSet;

pub type CleanupPathMapper = Arc<dyn Fn(&str) -> String + Send + Sync>;

pub struct RunInput {
    pub collector: Arc<IcebergCommitCollector>,
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub fs: Operator,
    pub file_io: FileIO,
    pub cleanup_path_mapper: Option<CleanupPathMapper>,
    pub cow_update_rewrite: Option<CowUpdateRewriteSet>,
    /// Iceberg ref to commit to. `"main"` is the default; branch-qualified
    /// DML (`INSERT INTO t.branch_dev`) supplies the branch name here.
    pub target_ref: String,
}

/// Dispatch a commit-action and handle abort/cleanup.
///
/// On error this function calls `AbortLog::cleanup` only when the underlying
/// failure is "definite" — see [`is_commit_unknown_message`] for the
/// classifier. For commit-unknown failures (network transport errors that
/// could leave the catalog in either state) the staged files are left on disk
/// and surfaced in the error message for manual reconciliation.
pub async fn run_iceberg_commit(input: RunInput) -> Result<CommitOutcome, String> {
    let RunInput {
        collector,
        catalog,
        table,
        fs,
        file_io,
        cleanup_path_mapper,
        cow_update_rewrite,
        target_ref,
    } = input;

    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
        CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
        CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
        CommitOpKind::CowUpdate => Box::new(CowUpdateCommit {
            rewrite: cow_update_rewrite
                .ok_or_else(|| "CowUpdate commit requires a rewrite set".to_string())?,
        }),
        CommitOpKind::Truncate => Box::new(TruncateCommit),
        CommitOpKind::OverwritePartitions => {
            Box::new(super::overwrite_partitions::OverwritePartitionsCommit)
        }
        CommitOpKind::RewriteManifests => {
            return Err(
                "CommitOpKind::RewriteManifests must be invoked via run_rewrite_manifests \
                directly, not the collector dispatcher"
                    .to_string(),
            );
        }
    };

    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle: collector.abort_log.clone(),
        target_ref: &target_ref,
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => {
            if is_commit_unknown_message(&commit_err) {
                tracing::warn!(
                    op_kind = ?collector.op_kind,
                    table = %collector.table_ident,
                    base_snapshot_id = ?collector.base_snapshot_id,
                    staging_dir = collector.staging_dir,
                    "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
                );
                Err(format!(
                    "iceberg commit unknown ({commit_err}); staged files left at {} for manual review",
                    collector.staging_dir
                ))
            } else {
                let cleanup_errors = if let Some(mapper) = cleanup_path_mapper {
                    collector
                        .abort_log
                        .cleanup_with_path_mapper(&fs, |path| mapper(path))
                        .await
                } else {
                    collector.abort_log.cleanup(&fs).await
                };
                for e in &cleanup_errors {
                    tracing::warn!(path = %e.path, source = ?e.source, "abort cleanup error");
                }
                Err(format!(
                    "iceberg commit failed: {commit_err}; abort cleanup ran ({} error(s))",
                    cleanup_errors.len()
                ))
            }
        }
    }
}

/// Classify a commit-error string into "definite fail" (false) vs
/// "commit unknown" (true).
///
/// Errors flow up from each commit-action as `String`, so this matches on
/// substrings derived from `iceberg::ErrorKind` `Display`. Per spike
/// (`docs/superpowers/spikes/2026-04-28-commit-unknown-classification.md`):
///
/// * `Unexpected` → commit unknown → leave files alone
/// * Any other ErrorKind → definite fail → clean up
///
/// Pipeline-level cancellation/cleanup signals are also treated as definite
/// failures because by the time we see them the pipeline never reached the
/// commit-action's `Catalog::update_table` call.
fn is_commit_unknown_message(err: &str) -> bool {
    let lower = err.to_lowercase();

    // Definite-fail signals override any "unexpected" mention they may
    // contain (e.g. an Unexpected wrapping a known sub-error).
    let definite_signals = [
        "conflict",
        "assertrefsnapshotid",
        "ref_snapshot_id_match",
        "schema id mismatch",
        "schemaidmatch",
        "spec id mismatch",
        "specidmatch",
        "data invalid",
        "datainvalid",
        "feature unsupported",
        "featureunsupported",
        "table not found",
        "tablenotfound",
        "table already exists",
        "tablealreadyexists",
        "namespace not found",
        "namespacenotfound",
        "namespace already exists",
        "namespacealreadyexists",
        "precondition failed",
        "preconditionfailed",
        "catalog commit conflict",
        "catalogcommitconflict",
        "expected data only",
        // pipeline-side errors are always definite
        "pipeline cancelled",
        "pipeline failed",
    ];
    !definite_signals.iter().any(|s| lower.contains(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_commit_unknown_classifies_definite_failures() {
        assert!(!is_commit_unknown_message(
            "RowDelta commit failed: catalog commit conflict on assert-ref-snapshot-id"
        ));
        assert!(!is_commit_unknown_message(
            "FastAppend commit failed: data invalid"
        ));
        assert!(!is_commit_unknown_message(
            "FastAppendCommit received PositionDeletes content; expected Data only"
        ));
        assert!(!is_commit_unknown_message("pipeline cancelled mid-write"));
        assert!(!is_commit_unknown_message(
            "Overwrite commit failed: TableAlreadyExists"
        ));
    }

    #[test]
    fn is_commit_unknown_classifies_unknown_failures() {
        assert!(is_commit_unknown_message(
            "RowDelta commit failed: io error reading from socket"
        ));
        assert!(is_commit_unknown_message(
            "FastAppend commit failed: connection reset by peer"
        ));
        assert!(is_commit_unknown_message("unexpected error"));
    }

    #[test]
    fn run_dispatch_accepts_rewrite_data_files_variant() {
        let _ = CommitOpKind::RewriteDataFiles;
        let _ = CommitOpKind::RewriteManifests;
        let _ = std::any::type_name::<crate::connector::iceberg::commit::RewriteDataFilesCommit>();
    }
}
