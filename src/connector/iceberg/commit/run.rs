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
use super::row_delta_dv_from_files::RowDeltaDvFromFilesCommit;
use super::service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, RecoveryEvidence, classify_commit_error,
};
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
    pub snapshot_properties: BTreeMap<String, String>,
}

/// Dispatch a commit-action and return typed commit outcome/error.
///
/// On definite commit failure this function runs best-effort abort cleanup and
/// returns `KnownUncommitted`. On commit-unknown failure it leaves staged files
/// untouched and returns `Unknown` with recovery evidence.
pub async fn run_iceberg_commit_typed(
    input: RunInput,
) -> Result<CommitOutcome, CommitServiceError> {
    let RunInput {
        collector,
        catalog,
        table,
        fs,
        file_io,
        cleanup_path_mapper,
        cow_update_rewrite,
        target_ref,
        snapshot_properties,
    } = input;

    let action: Box<dyn IcebergCommitAction> = match collector.op_kind {
        CommitOpKind::FastAppend => Box::new(FastAppendCommit),
        CommitOpKind::Overwrite => Box::new(OverwriteCommit),
        CommitOpKind::RowDelta => Box::new(RowDeltaCommit),
        CommitOpKind::RowDeltaDv => Box::new(RowDeltaDvCommit),
        CommitOpKind::RowDeltaDvFromFiles => Box::new(RowDeltaDvFromFilesCommit),
        CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
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

    let ctx = CommitCtx {
        collector: &collector,
        table: &table,
        catalog: catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle: collector.abort_log.clone(),
        target_ref: &target_ref,
        snapshot_properties: &snapshot_properties,
    };

    match action.commit(ctx).await {
        Ok(outcome) => {
            collector.mark_committed();
            Ok(outcome)
        }
        Err(commit_err) => match classify_commit_error(&commit_err) {
            CommitFailureKind::Unknown => {
                let evidence = RecoveryEvidence::from_collector(&collector);
                tracing::warn!(
                    op_kind = ?collector.op_kind,
                    table = %collector.table_ident,
                    base_snapshot_id = ?collector.base_snapshot_id,
                    staging_dir = collector.staging_dir,
                    "iceberg commit unknown — leaving all staged files for manual review: {commit_err}"
                );
                Err(CommitServiceError::unknown(commit_err, evidence))
            }
            CommitFailureKind::FinalizeFailedKnownCommitted => {
                Err(CommitServiceError::finalize_failed_known_committed(
                    None,
                    commit_err,
                    RecoveryEvidence::from_collector(&collector),
                ))
            }
            CommitFailureKind::KnownUncommitted => {
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
                Err(CommitServiceError::known_uncommitted(
                    commit_err,
                    CleanupAttempt::from_cleanup_errors(&cleanup_errors),
                ))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use opendal::Operator;
    use opendal::services::Memory;

    use super::super::service::{CleanupAttempt, CommitServiceError, RecoveryEvidence};
    use super::super::test_helpers::IcebergTestFixture;
    use super::super::test_helpers::empty_v3_iceberg_table;
    use super::*;

    fn input_for_op(fixture: IcebergTestFixture, op_kind: CommitOpKind) -> RunInput {
        let metadata = fixture.table.metadata();
        let collector = Arc::new(
            IcebergCommitCollector::new(
                op_kind,
                fixture.table_ident.clone(),
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/staging", metadata.location()),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            )
            .with_table_metadata(metadata.clone()),
        );
        RunInput {
            collector,
            catalog: fixture.catalog,
            table: fixture.table,
            fs: Operator::new(Memory::default())
                .expect("memory operator")
                .finish(),
            file_io: iceberg::io::FileIO::new_with_memory(),
            cleanup_path_mapper: None,
            cow_update_rewrite: None,
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        }
    }

    #[test]
    fn commit_service_error_legacy_string_preserves_known_failure_format() {
        let err = CommitServiceError::known_uncommitted(
            "FastAppend commit failed: data invalid".to_string(),
            CleanupAttempt::completed(vec!["staged.parquet".to_string()]),
        );
        assert_eq!(
            err.into_legacy_string(),
            "iceberg commit failed: FastAppend commit failed: data invalid; abort cleanup ran (1 error(s))"
        );
    }

    #[test]
    fn commit_service_error_legacy_string_preserves_unknown_format() {
        let evidence = RecoveryEvidence {
            table_ident: "db.tbl".to_string(),
            op_kind: CommitOpKind::FastAppend,
            base_snapshot_id: Some(10),
            base_sequence_number: 3,
            staging_dir: "s3://bucket/db/tbl/data/_staging/abc".to_string(),
        };
        let err = CommitServiceError::unknown("connection reset by peer".to_string(), evidence);
        assert_eq!(
            err.into_legacy_string(),
            "iceberg commit unknown (connection reset by peer); staged files left at s3://bucket/db/tbl/data/_staging/abc for manual review"
        );
    }

    #[test]
    fn run_dispatch_accepts_rewrite_data_files_variant() {
        let _ = CommitOpKind::RewriteDataFiles;
        let _ = CommitOpKind::RewriteManifests;
        let _ = std::any::type_name::<crate::connector::iceberg::commit::RewriteDataFilesCommit>();
    }

    #[test]
    fn run_dispatch_accepts_row_delta_dv_from_files_variant() {
        let _ = CommitOpKind::RowDeltaDvFromFiles;
        let _ =
            std::any::type_name::<crate::connector::iceberg::commit::RowDeltaDvFromFilesCommit>();
    }

    #[tokio::test]
    async fn invalid_dispatch_errors_are_invalid_input_not_known_uncommitted() {
        let fixture = empty_v3_iceberg_table().await;

        let cow_err =
            run_iceberg_commit_typed(input_for_op(fixture.clone(), CommitOpKind::CowUpdate))
                .await
                .expect_err("missing CowUpdate rewrite set should fail before dispatch");
        assert!(matches!(cow_err, CommitServiceError::InvalidInput { .. }));

        let rewrite_manifests_err =
            run_iceberg_commit_typed(input_for_op(fixture, CommitOpKind::RewriteManifests))
                .await
                .expect_err("RewriteManifests should not use the collector dispatcher");
        assert!(matches!(
            rewrite_manifests_err,
            CommitServiceError::InvalidInput { .. }
        ));
    }
}
