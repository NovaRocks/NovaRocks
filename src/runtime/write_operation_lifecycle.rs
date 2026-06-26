//! Adapts distributed writer coordinator outputs to Iceberg operation lifecycle
//! records.

use std::collections::{BTreeMap, BTreeSet};

use crate::connector::iceberg::commit::CommitOpKind;
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergCleanupOutcomeRecord, IcebergOperationFactUpdate,
    IcebergOperationFailureKind, IcebergOperationFailureRecord, IcebergOperationKind,
    IcebergOperationNextAction, IcebergOperationState, IcebergOperationTarget,
};
use crate::runtime::write_coordinator::{WriteAbortInput, WriteCommitInput, WriterCommitInput};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WriteOperationContext {
    pub(crate) operation_kind: IcebergOperationKind,
    pub(crate) target: IcebergOperationTarget,
    pub(crate) attempt_id: String,
    pub(crate) commit_op_kind: CommitOpKind,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) created_at_ms: i64,
}

pub(crate) fn operation_request_from_write_commit(
    ctx: WriteOperationContext,
    commit: &WriteCommitInput,
) -> Result<CreateIcebergOperationRequest, String> {
    if ctx.attempt_id.trim().is_empty() {
        return Err("write operation attempt_id is empty".to_string());
    }
    let expected_operation_kind = operation_kind_for_commit_op_kind(ctx.commit_op_kind);
    if ctx.operation_kind != expected_operation_kind {
        return Err(format!(
            "write operation kind {:?} does not match commit op {:?}; expected {:?}",
            ctx.operation_kind, ctx.commit_op_kind, expected_operation_kind
        ));
    }
    let staged_artifacts = staged_artifacts_from_writer_outputs(&commit.writers)?;
    Ok(CreateIcebergOperationRequest {
        operation_kind: ctx.operation_kind,
        operation_subkind: None,
        target: ctx.target,
        attempt_id: ctx.attempt_id,
        base_snapshot_id: ctx.base_snapshot_id,
        base_snapshot_map: ctx.base_snapshot_map,
        staged_artifacts,
        created_at_ms: ctx.created_at_ms,
    })
}

pub(crate) fn operation_fact_update_from_write_abort(
    operation_id: i64,
    abort: &WriteAbortInput,
    now_ms: i64,
) -> Result<IcebergOperationFactUpdate, String> {
    let staged_artifacts = staged_artifacts_from_writer_outputs(&abort.completed_writer_outputs)?;
    let needs_cleanup = !staged_artifacts.is_empty();
    let cleanup_outcome = needs_cleanup.then_some(IcebergCleanupOutcomeRecord {
        attempted: false,
        error_count: 0,
        error_paths: Vec::new(),
    });
    let next_action = if needs_cleanup {
        IcebergOperationNextAction::RetryAbort
    } else {
        IcebergOperationNextAction::None
    };
    Ok(IcebergOperationFactUpdate {
        operation_id,
        state: IcebergOperationState::FailedKnownUncommitted,
        commit_outcome: None,
        cleanup_outcome,
        recovery_evidence: None,
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::KnownUncommitted,
            message: format!(
                "write {} aborted before Iceberg commit: {}; completed_writers={} incomplete_writers={} staged_artifacts={}",
                format_unique_id(&abort.write_id),
                abort.reason,
                abort.completed_writer_outputs.len(),
                abort.incomplete_writers.len(),
                staged_artifacts.len()
            ),
            next_action,
        }),
        now_ms,
    })
}

fn staged_artifacts_from_writer_outputs(
    writers: &[WriterCommitInput],
) -> Result<Vec<String>, String> {
    let mut paths = BTreeSet::new();
    for writer in writers {
        for commit_info in &writer.sink_commit_infos {
            let Some(data_file) = commit_info.iceberg_data_file.as_ref() else {
                continue;
            };
            let path = data_file.path.as_ref().ok_or_else(|| {
                format!("writer {} Iceberg data file missing path", writer.writer_id)
            })?;
            paths.insert(path.clone());
        }
    }
    Ok(paths.into_iter().collect())
}

fn format_unique_id(id: &crate::thrift::types::TUniqueId) -> String {
    format!("{}/{}", id.hi, id.lo)
}

fn operation_kind_for_commit_op_kind(kind: CommitOpKind) -> IcebergOperationKind {
    match kind {
        CommitOpKind::FastAppend => IcebergOperationKind::InsertAppend,
        CommitOpKind::Overwrite | CommitOpKind::OverwritePartitions | CommitOpKind::Truncate => {
            IcebergOperationKind::InsertOverwrite
        }
        CommitOpKind::RowDelta
        | CommitOpKind::RowDeltaDv
        | CommitOpKind::RowDeltaDvFromFiles
        | CommitOpKind::CowUpdate => IcebergOperationKind::RowDelta,
        CommitOpKind::RewriteDataFiles | CommitOpKind::RewriteManifests => {
            IcebergOperationKind::Maintenance
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::meta::repository::iceberg_operation::{
        IcebergOperationFailureKind, IcebergOperationNextAction, IcebergOperationState,
    };
    use crate::runtime::write_coordinator::{WriteAbortInput, WriterKey};
    use crate::thrift::types;

    fn id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn key(
        query_hi: i64,
        query_lo: i64,
        finst_hi: i64,
        finst_lo: i64,
        backend_num: i32,
    ) -> WriterKey {
        WriterKey {
            query_id: id(query_hi, query_lo),
            fragment_instance_id: id(finst_hi, finst_lo),
            backend_num,
        }
    }

    fn writer_output(writer_id: usize, writer_key: WriterKey, path: &str) -> WriterCommitInput {
        WriterCommitInput {
            writer_id,
            writer_key,
            sink_commit_infos: vec![types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some(path.to_string()),
                    record_count: Some(7),
                    file_size_in_bytes: Some(70),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            tablet_commit_infos: Vec::new(),
            tablet_fail_infos: Vec::new(),
            load_counters: BTreeMap::from([("loaded.rows".to_string(), "7".to_string())]),
            loaded_rows: 7,
            loaded_bytes: 70,
            filtered_rows: 0,
        }
    }

    fn context() -> WriteOperationContext {
        WriteOperationContext {
            operation_kind: IcebergOperationKind::InsertAppend,
            target: IcebergOperationTarget {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                ref_name: None,
            },
            attempt_id: "insert-10-20".to_string(),
            commit_op_kind: CommitOpKind::FastAppend,
            base_snapshot_id: Some(42),
            base_snapshot_map: BTreeMap::new(),
            created_at_ms: 1000,
        }
    }

    #[test]
    fn write_commit_input_builds_operation_request_with_writer_artifacts() {
        let write_id = id(10, 20);
        let writer_a = writer_output(0, key(10, 20, 101, 201, 0), "s3://w/a.parquet");
        let writer_b = writer_output(1, key(10, 20, 102, 202, 1), "s3://w/b.parquet");
        let commit = WriteCommitInput {
            write_id,
            writers: vec![writer_a, writer_b],
        };

        let request = operation_request_from_write_commit(context(), &commit)
            .expect("writer commit operation request");

        assert_eq!(request.operation_kind, IcebergOperationKind::InsertAppend);
        assert_eq!(request.target.table, "orders");
        assert_eq!(request.attempt_id, "insert-10-20");
        assert_eq!(request.base_snapshot_id, Some(42));
        assert_eq!(request.created_at_ms, 1000);
        assert_eq!(
            request.staged_artifacts,
            vec![
                "s3://w/a.parquet".to_string(),
                "s3://w/b.parquet".to_string()
            ]
        );
    }

    #[test]
    fn write_commit_rejects_operation_kind_commit_op_mismatch() {
        let mut ctx = context();
        ctx.operation_kind = IcebergOperationKind::RowDelta;
        let commit = WriteCommitInput {
            write_id: id(11, 21),
            writers: vec![writer_output(
                0,
                key(11, 21, 111, 211, 0),
                "s3://w/a.parquet",
            )],
        };

        let err = operation_request_from_write_commit(ctx, &commit)
            .expect_err("mismatched operation kind must fail");

        assert!(err.contains("does not match commit op"), "{err}");
        assert!(err.contains("FastAppend"), "{err}");
    }

    #[test]
    fn writer_abort_with_artifacts_maps_to_known_uncommitted_retry_abort() {
        let completed = writer_output(0, key(30, 40, 301, 401, 0), "s3://w/done.parquet");
        let abort = WriteAbortInput {
            write_id: id(30, 40),
            reason: "query timed out after 1000 ms waiting for write final reports".to_string(),
            completed_writer_outputs: vec![completed],
            incomplete_writers: vec![key(30, 40, 302, 402, 1)],
        };

        let fact =
            operation_fact_update_from_write_abort(7, &abort, 2000).expect("abort fact update");

        assert_eq!(fact.operation_id, 7);
        assert_eq!(fact.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(fact.now_ms, 2000);
        assert_eq!(fact.commit_outcome, None);
        assert_eq!(fact.recovery_evidence, None);
        let cleanup = fact.cleanup_outcome.expect("cleanup fact");
        assert!(!cleanup.attempted);
        assert_eq!(cleanup.error_count, 0);
        let failure = fact.failure.expect("failure fact");
        assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
        assert_eq!(failure.next_action, IcebergOperationNextAction::RetryAbort);
        assert!(
            failure.message.contains("query timed out"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("completed_writers=1"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("incomplete_writers=1"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("staged_artifacts=1"),
            "{}",
            failure.message
        );
    }

    #[test]
    fn writer_abort_without_artifacts_needs_no_cleanup_action() {
        let abort = WriteAbortInput {
            write_id: id(50, 60),
            reason: "client disconnected".to_string(),
            completed_writer_outputs: Vec::new(),
            incomplete_writers: vec![key(50, 60, 501, 601, 0)],
        };

        let fact =
            operation_fact_update_from_write_abort(8, &abort, 3000).expect("abort fact update");

        assert_eq!(fact.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(fact.cleanup_outcome, None);
        let failure = fact.failure.expect("failure fact");
        assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
        assert_eq!(failure.next_action, IcebergOperationNextAction::None);
        assert!(
            failure.message.contains("client disconnected"),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("staged_artifacts=0"),
            "{}",
            failure.message
        );
    }

    #[test]
    fn staged_artifacts_ignore_non_iceberg_commit_infos_and_deduplicate_paths() {
        let mut writer = writer_output(0, key(70, 80, 701, 801, 0), "s3://w/a.parquet");
        writer.sink_commit_infos.push(types::TSinkCommitInfo {
            iceberg_data_file: Some(types::TIcebergDataFile {
                path: Some("s3://w/a.parquet".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });
        writer
            .sink_commit_infos
            .push(types::TSinkCommitInfo::default());

        let artifacts = staged_artifacts_from_writer_outputs(&[writer]).expect("artifacts");

        assert_eq!(artifacts, vec!["s3://w/a.parquet".to_string()]);
    }

    #[test]
    fn staged_artifacts_reject_missing_iceberg_path() {
        let mut writer = writer_output(0, key(90, 100, 901, 1001, 0), "s3://w/a.parquet");
        writer.sink_commit_infos.push(types::TSinkCommitInfo {
            iceberg_data_file: Some(types::TIcebergDataFile {
                path: None,
                ..Default::default()
            }),
            ..Default::default()
        });

        let err = staged_artifacts_from_writer_outputs(&[writer])
            .expect_err("missing Iceberg file path must fail");

        assert!(err.contains("missing path"), "{err}");
    }
}
