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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::dml::{
    CommitOpKind, CommitOutcome, CommitServiceError, CoordinatedWriteReport, DmlErrorKind,
    DmlService, IcebergOperationFailureKind, IcebergOperationNextAction, OperationKind,
    OperationState, OperationTarget, RecoveryEvidence, StateStoreOperationJournal, WriteExecutor,
    WriteTransactionSpec,
};
use novarocks_spi::state_store::{FeDeploymentView, StateStore};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

struct FakeExecutor;

impl WriteExecutor for FakeExecutor {
    type CommitHandle = ();

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<()>, String> {
        Ok(CoordinatedWriteReport::Committable(()))
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &(),
    ) -> Result<CommitOutcome, CommitServiceError> {
        Ok(CommitOutcome {
            new_snapshot_id: 555,
            written_manifest_paths: vec![],
        })
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        Ok(())
    }
}

struct KnownCommittedCommitErrorExecutor;

impl WriteExecutor for KnownCommittedCommitErrorExecutor {
    type CommitHandle = ();

    fn run_coordinated_write(
        &self,
        _spec: &WriteTransactionSpec,
    ) -> Result<CoordinatedWriteReport<()>, String> {
        Ok(CoordinatedWriteReport::Committable(()))
    }

    fn commit(
        &self,
        _spec: &WriteTransactionSpec,
        _handle: &(),
    ) -> Result<CommitOutcome, CommitServiceError> {
        Err(CommitServiceError::finalize_failed_known_committed(
            Some(CommitOutcome {
                new_snapshot_id: 777,
                written_manifest_paths: vec!["manifest.avro".to_string()],
            }),
            "finalize failed inside commit service".to_string(),
            RecoveryEvidence {
                table_ident: "cat.ns.tbl".to_string(),
                op_kind: CommitOpKind::FastAppend,
                base_snapshot_id: Some(10),
                base_sequence_number: 11,
                staging_dir: "/warehouse/staging/attempt-1".to_string(),
            },
        ))
    }

    fn finalize(&self, _spec: &WriteTransactionSpec) -> Result<(), String> {
        panic!("runner must not finalize after commit reports a finalize failure")
    }
}

async fn open_journal(
    path: &std::path::Path,
) -> (
    StateStoreHost,
    Arc<dyn StateStore>,
    StateStoreOperationJournal,
) {
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id: "dml-service-test".to_string(),
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: path.to_path_buf(),
                        deployment_owner: "dml-service-fe".to_string(),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"dml-service-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite StateStore");
    let store = host.state_store().expect("StateStore exposure");
    let journal =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("open DML journal");
    (host, store, journal)
}

#[tokio::test(flavor = "multi_thread")]
async fn dml_service_commits_over_real_state_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_host, _store, journal) = open_journal(&dir.path().join("state.sqlite")).await;
    let service = DmlService::new(Arc::new(journal));

    let spec = WriteTransactionSpec {
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        operation_kind: OperationKind::InsertAppend,
        commit_op_kind: CommitOpKind::FastAppend,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
    };

    let outcome = service
        .run_write(spec, &FakeExecutor)
        .expect("write succeeds");
    let id = outcome.operation_id.expect("committed operation id");
    assert_eq!(outcome.committed_snapshot_id, Some(555));

    let stored = service
        .load_operation(id)
        .unwrap()
        .expect("operation persisted");
    assert_eq!(stored.state, OperationState::Finalized);
    assert_eq!(stored.commit_outcome.unwrap().snapshot_id, 555);
    assert!(service.list_unfinished_operations().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn known_committed_commit_error_persists_retry_finalize_fact_over_real_state_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_host, _store, journal) = open_journal(&dir.path().join("state.sqlite")).await;
    let service = DmlService::new(Arc::new(journal));

    let spec = WriteTransactionSpec {
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        operation_kind: OperationKind::InsertAppend,
        commit_op_kind: CommitOpKind::FastAppend,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: Some(10),
        base_snapshot_map: BTreeMap::new(),
    };

    let error = service
        .run_write(spec, &KnownCommittedCommitErrorExecutor)
        .expect_err("known-committed finalize failure must remain an error");
    assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
    assert!(
        error
            .to_string()
            .contains("finalize failed inside commit service")
    );

    let stored = service
        .list_unfinished_operations()
        .unwrap()
        .into_iter()
        .next()
        .expect("operation persisted");
    assert_eq!(stored.state, OperationState::FinalizeFailedKnownCommitted);
    let outcome = stored.commit_outcome.expect("commit outcome persisted");
    assert_eq!(outcome.snapshot_id, 777);
    assert_eq!(outcome.written_manifest_paths, vec!["manifest.avro"]);
    let evidence = stored
        .recovery_evidence
        .expect("recovery evidence persisted");
    assert_eq!(evidence.table_ident, "cat.ns.tbl");
    assert_eq!(evidence.commit_op_kind, "fast_append");
    assert_eq!(evidence.base_snapshot_id, Some(10));
    assert_eq!(evidence.base_sequence_number, Some(11));
    assert_eq!(evidence.staging_dir, "/warehouse/staging/attempt-1");
    let failure = stored.failure.expect("failure classification persisted");
    assert_eq!(
        failure.kind,
        IcebergOperationFailureKind::FinalizeKnownCommitted
    );
    assert_eq!(failure.message, "finalize failed inside commit service");
    assert_eq!(
        failure.next_action,
        IcebergOperationNextAction::RetryFinalize
    );
}
