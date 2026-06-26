//! Engine-owned persistence boundary for writer operation lifecycle records.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::runtime::write_coordinator::{WriteAbortInput, WriteCommitInput};
use crate::runtime::write_operation_lifecycle::{
    WriteOperationContext, operation_fact_update_from_write_abort,
    operation_request_from_write_commit,
};

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

    use crate::runtime::write_coordinator::{
        WriteAbortInput, WriteCommitInput, WriterCommitInput, WriterKey,
    };
    use crate::thrift::types;

    fn staging_writer_key() -> WriterKey {
        let query_id = types::TUniqueId::new(10, 20);
        WriterKey {
            query_id: query_id.clone(),
            fragment_instance_id: types::TUniqueId::new(101, 201),
            backend_num: 0,
        }
    }

    fn staging_writer_commit_input(writer_key: WriterKey) -> WriterCommitInput {
        WriterCommitInput {
            writer_id: 0,
            writer_key,
            sink_commit_infos: vec![types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some("s3://warehouse/orders/_staging/a.parquet".to_string()),
                    record_count: Some(11),
                    file_size_in_bytes: Some(110),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            tablet_commit_infos: Vec::new(),
            tablet_fail_infos: Vec::new(),
            load_counters: BTreeMap::from([("loaded.rows".to_string(), "11".to_string())]),
            loaded_rows: 11,
            loaded_bytes: 110,
            filtered_rows: 0,
        }
    }

    pub(crate) fn write_commit_with_data_file() -> WriteCommitInput {
        let query_id = types::TUniqueId::new(10, 20);
        let writer_key = staging_writer_key();
        WriteCommitInput {
            write_id: query_id,
            writers: vec![staging_writer_commit_input(writer_key)],
        }
    }

    pub(crate) fn write_abort_with_data_file() -> WriteAbortInput {
        let query_id = types::TUniqueId::new(10, 20);
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

    use crate::connector::iceberg::commit::CommitOpKind;
    use crate::meta::repository::iceberg_operation::{
        IcebergOperationFailureKind, IcebergOperationKind, IcebergOperationNextAction,
        IcebergOperationState, IcebergOperationTarget,
    };
    use crate::meta::{MetaStoreProvider, SqliteMetaStoreProvider};
    use crate::runtime::write_coordinator::{WriteAbortInput, WriterCommitInput, WriterKey};
    use crate::thrift::types;

    struct WriterOperationTestState {
        state: Arc<StandaloneState>,
        provider: Arc<dyn MetaStoreProvider>,
        _dir: tempfile::TempDir,
    }

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
                    record_count: Some(11),
                    file_size_in_bytes: Some(110),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            tablet_commit_infos: Vec::new(),
            tablet_fail_infos: Vec::new(),
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
