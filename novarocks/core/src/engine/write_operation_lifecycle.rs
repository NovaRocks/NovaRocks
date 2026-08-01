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

//! Engine-owned mapping and persistence boundary for writer operation lifecycle records.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::connector::iceberg::commit::CommitOpKind;
use crate::engine::StandaloneState;
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergCleanupOutcomeRecord, IcebergOperationFactUpdate,
    IcebergOperationFailureKind, IcebergOperationFailureRecord, IcebergOperationKind,
    IcebergOperationNextAction, IcebergOperationState, IcebergOperationTarget,
};
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput, WriterCommitInput};

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
    // Connector reports are intentionally opaque outside their provider
    // control binding.  Operation persistence therefore records lifecycle
    // facts, not provider file paths; abort/cleanup is delegated to the same
    // exact-generation control lease that staged the reports.
    let _ = writers;
    Ok(Vec::new())
}

fn format_unique_id(id: &crate::common::types::UniqueId) -> String {
    format!("{}/{}", id.high(), id.low())
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

pub(crate) fn create_writer_operation_from_commit(
    state: &Arc<StandaloneState>,
    ctx: WriteOperationContext,
    commit: &WriteCommitInput,
) -> Result<i64, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider is required for Iceberg writer operations".to_string())?;
    let request = operation_request_from_write_commit(ctx, commit)?;
    let mut txn = provider
        .begin_write("create iceberg writer operation")
        .map_err(|e| format!("begin writer operation create transaction failed: {e}"))?;
    let operation = state
        .iceberg_operation_repo
        .create_operation(txn.as_mut(), request)
        .map_err(|e| format!("create writer operation failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit writer operation create transaction failed: {e}"))?;
    Ok(operation.operation_id)
}

pub(crate) fn record_writer_abort_fact(
    state: &Arc<StandaloneState>,
    operation_id: i64,
    abort: &WriteAbortInput,
    now_ms: i64,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider is required for Iceberg writer operations".to_string())?;
    let update = operation_fact_update_from_write_abort(operation_id, abort, now_ms)?;
    let mut txn = provider
        .begin_write("record iceberg writer abort fact")
        .map_err(|e| format!("begin writer abort fact transaction failed: {e}"))?;
    state
        .iceberg_operation_repo
        .record_operation_fact(txn.as_mut(), update)
        .map_err(|e| format!("record writer abort fact failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit writer abort fact transaction failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use crate::common::types::UniqueId;
    use crate::query_execution::write::{
        WriteAbortInput, WriteCommitInput, WriterCommitInput, WriterKey,
    };

    fn staging_writer_key() -> WriterKey {
        let query_id = UniqueId::new(10, 20);
        WriterKey {
            query_id,
            fragment_instance_id: UniqueId::new(101, 201),
            backend_num: 0,
        }
    }

    fn staging_writer_commit_input(writer_key: WriterKey) -> WriterCommitInput {
        WriterCommitInput {
            writer_id: 0,
            fragment_id: 0,
            writer_key,
            connector_staged_report_frames: vec![
                crate::proto::novarocks::ConnectorStagedReportFrame::default(),
            ],
            load_counters: BTreeMap::from([("loaded.rows".to_string(), "11".to_string())]),
            loaded_rows: 11,
            loaded_bytes: 110,
            filtered_rows: 0,
        }
    }

    pub(crate) fn write_commit_with_data_file() -> WriteCommitInput {
        let query_id = UniqueId::new(10, 20);
        let writer_key = staging_writer_key();
        WriteCommitInput {
            write_id: query_id,
            writers: vec![staging_writer_commit_input(writer_key)],
        }
    }

    pub(crate) fn write_abort_with_data_file() -> WriteAbortInput {
        let query_id = UniqueId::new(10, 20);
        let writer_key = staging_writer_key();
        WriteAbortInput {
            write_id: query_id,
            reason: "query timed out waiting for write final reports".to_string(),
            completed_writer_outputs: vec![staging_writer_commit_input(writer_key)],
            incomplete_writers: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::CommitOpKind;
    use crate::meta::repository::iceberg_operation::{
        IcebergOperationFailureKind, IcebergOperationKind, IcebergOperationNextAction,
        IcebergOperationState, IcebergOperationTarget,
    };
    use crate::meta::{MetaStoreProvider, SqliteMetaStoreProvider};
    use crate::query_execution::write::{WriteAbortInput, WriterCommitInput, WriterKey};

    struct WriterOperationTestState {
        state: Arc<StandaloneState>,
        provider: Arc<dyn MetaStoreProvider>,
        _dir: tempfile::TempDir,
    }

    fn id(hi: i64, lo: i64) -> UniqueId {
        UniqueId::new(hi, lo)
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

    fn writer_output(writer_id: usize, writer_key: WriterKey, _path: &str) -> WriterCommitInput {
        WriterCommitInput {
            writer_id,
            fragment_id: 0,
            writer_key,
            connector_staged_report_frames: Vec::new(),
            load_counters: BTreeMap::from([("loaded.rows".to_string(), "11".to_string())]),
            loaded_rows: 11,
            loaded_bytes: 110,
            filtered_rows: 0,
        }
    }

    fn write_commit_input_with_data_file() -> WriteCommitInput {
        WriteCommitInput {
            write_id: id(10, 20),
            writers: vec![writer_output(
                0,
                key(10, 20, 101, 201, 0),
                "s3://warehouse/orders/_staging/a.parquet",
            )],
        }
    }

    fn write_abort_input_with_data_file() -> WriteAbortInput {
        WriteAbortInput {
            write_id: id(10, 20),
            reason: "query timed out after 1000 ms waiting for write final reports".to_string(),
            completed_writer_outputs: vec![writer_output(
                0,
                key(10, 20, 101, 201, 0),
                "s3://warehouse/orders/_staging/a.parquet",
            )],
            incomplete_writers: vec![key(10, 20, 102, 202, 1)],
        }
    }

    fn append_operation_context() -> WriteOperationContext {
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
            base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 42)]),
            created_at_ms: 1234,
        }
    }

    #[test]
    fn write_commit_input_does_not_expose_provider_artifacts() {
        let writer_a = writer_output(0, key(10, 20, 101, 201, 0), "s3://w/a.parquet");
        let writer_b = writer_output(1, key(10, 20, 102, 202, 1), "s3://w/b.parquet");
        let commit = WriteCommitInput {
            write_id: id(10, 20),
            writers: vec![writer_a, writer_b],
        };

        let request = operation_request_from_write_commit(append_operation_context(), &commit)
            .expect("writer commit operation request");

        assert_eq!(request.operation_kind, IcebergOperationKind::InsertAppend);
        assert_eq!(request.target.table, "orders");
        assert_eq!(request.attempt_id, "insert-10-20");
        assert_eq!(request.base_snapshot_id, Some(42));
        assert_eq!(request.created_at_ms, 1234);
        assert!(request.staged_artifacts.is_empty());
    }

    #[test]
    fn write_commit_rejects_operation_kind_commit_op_mismatch() {
        let mut ctx = append_operation_context();
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
    fn writer_abort_delegates_cleanup_to_connector_control() {
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
        assert_eq!(fact.cleanup_outcome, None);
        let failure = fact.failure.expect("failure fact");
        assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
        assert_eq!(failure.next_action, IcebergOperationNextAction::None);
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
            failure.message.contains("staged_artifacts=0"),
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
    fn staged_artifacts_are_opaque_outside_connector_control() {
        let writer = writer_output(0, key(70, 80, 701, 801, 0), "s3://w/a.parquet");
        let artifacts = staged_artifacts_from_writer_outputs(&[writer]).expect("artifacts");
        assert!(artifacts.is_empty());
    }

    fn test_state_with_metadata() -> WriterOperationTestState {
        let dir = tempfile::tempdir().expect("metadata tempdir");
        let provider: Arc<dyn MetaStoreProvider> = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))
                .expect("open metadata provider"),
        );
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::clone(&provider)),
            ..StandaloneState::default()
        });
        WriterOperationTestState {
            state,
            provider,
            _dir: dir,
        }
    }

    #[test]
    fn writer_operation_commit_persists_operation_record() {
        let env = test_state_with_metadata();

        let operation_id = create_writer_operation_from_commit(
            &env.state,
            append_operation_context(),
            &write_commit_input_with_data_file(),
        )
        .expect("create writer operation");

        let read = env.provider.begin_read().expect("open read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load operation")
            .expect("operation exists");

        assert_eq!(stored.operation_kind, IcebergOperationKind::InsertAppend);
        assert_eq!(stored.target.catalog, "ice");
        assert_eq!(stored.target.namespace, "sales");
        assert_eq!(stored.target.table, "orders");
        assert_eq!(stored.state, IcebergOperationState::Preparing);
        assert_eq!(stored.attempt_id, "insert-10-20");
        assert_eq!(stored.base_snapshot_id, Some(42));
        assert_eq!(stored.base_snapshot_map["ice.sales.orders"], 42);
        assert_eq!(
            stored.staged_artifacts,
            vec!["s3://warehouse/orders/_staging/a.parquet".to_string()]
        );
        assert_eq!(stored.created_at_ms, 1234);
        assert_eq!(stored.updated_at_ms, 1234);
    }

    #[test]
    fn writer_operation_abort_records_known_uncommitted_fact() {
        let env = test_state_with_metadata();
        let operation_id = create_writer_operation_from_commit(
            &env.state,
            append_operation_context(),
            &write_commit_input_with_data_file(),
        )
        .expect("create writer operation");

        record_writer_abort_fact(
            &env.state,
            operation_id,
            &write_abort_input_with_data_file(),
            2345,
        )
        .expect("record writer abort fact");

        let read = env.provider.begin_read().expect("open read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load operation")
            .expect("operation exists");

        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
        let failure = stored.failure.expect("failure record");
        assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
        assert_eq!(failure.next_action, IcebergOperationNextAction::RetryAbort);
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
        let cleanup = stored.cleanup_outcome.expect("cleanup outcome");
        assert!(!cleanup.attempted);
        assert_eq!(cleanup.error_count, 0);
        assert_eq!(stored.updated_at_ms, 2345);
        assert_eq!(stored.finished_at_ms, Some(2345));
    }
}
