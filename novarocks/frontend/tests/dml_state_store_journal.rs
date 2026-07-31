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

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_frontend::dml::{
    CreatePreparingRequest, DmlErrorKind, IcebergCommitOutcomeRecord, OperationFact,
    OperationJournal, OperationKind, OperationState, OperationTarget, StateStoreOperationJournal,
};
use novarocks_spi::state_store::{
    ChangePage, ChangePollRequest, CommitOutcome as StateStoreCommitOutcome, CommitResolution,
    FeDeploymentView, Key, Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord,
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, Value, WriteTransaction,
};
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use serde_json::json;
use tempfile::TempDir;
use uuid::{Uuid, Version};

const OPERATION_PREFIX: &str = "novarocks/frontend/dml/v1/operations/";
const UNFINISHED_PREFIX: &str = "novarocks/frontend/dml/v1/unfinished/";

fn config(path: &std::path::Path) -> StateStoreHostConfig {
    StateStoreHostConfig {
        state_store: StateStoreAppConfig {
            store: StateStoreConfig {
                cluster_id: "dml-journal-test".to_string(),
                limits: StateStoreLimitOverrides::default(),
                provider: StateStoreProviderConfig::Sqlite {
                    path: path.to_path_buf(),
                    deployment_owner: "dml-journal-fe".to_string(),
                },
            },
            mysql_client: None,
        },
        foundationdb_client: None,
    }
}

async fn open_store(
    path: &std::path::Path,
) -> (
    StateStoreHost,
    Arc<dyn StateStore>,
    StateStoreOperationJournal,
) {
    let registry = builtin_state_store_provider_registry().expect("provider registry");
    let host = StateStoreHost::open(
        &registry,
        config(path),
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"dml-journal-topology"),
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

fn request() -> CreatePreparingRequest {
    CreatePreparingRequest {
        operation_kind: OperationKind::InsertAppend,
        operation_subkind: None,
        target: OperationTarget {
            catalog: "cat".to_string(),
            namespace: "ns".to_string(),
            table: "tbl".to_string(),
            ref_name: None,
        },
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: None,
        base_snapshot_map: BTreeMap::new(),
        staged_artifacts: Vec::new(),
        created_at_ms: 100,
    }
}

fn key(prefix: &str, operation_id: Uuid) -> Key {
    Key::try_from(Bytes::from(format!("{prefix}{}", operation_id.simple()))).unwrap()
}

async fn raw_put(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "inject DML journal test record",
        )
        .await
        .unwrap();
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        StateStoreCommitOutcome::Committed(_)
    ));
}

fn raw_operation(operation_id: Uuid, schema_version: u8) -> Value {
    let value = json!({
        "schema_version": schema_version,
        "operation_id": operation_id,
        "revision": 1,
        "last_mutation_id": Uuid::now_v7(),
        "operation_kind": "INSERT_APPEND",
        "operation_subkind": null,
        "target": {
            "catalog": "cat",
            "namespace": "ns",
            "table": "tbl",
            "ref_name": null
        },
        "state": "PREPARING",
        "attempt_id": "attempt-1",
        "base_snapshot_id": null,
        "base_snapshot_map": {},
        "staged_artifacts": [],
        "commit_outcome": null,
        "cleanup_outcome": null,
        "recovery_evidence": null,
        "failure": null,
        "created_at_ms": 1,
        "updated_at_ms": 1,
        "finished_at_ms": null
    });
    Value::try_from(Bytes::from(serde_json::to_vec(&value).unwrap())).unwrap()
}

#[derive(Clone, Copy)]
enum CommitUnknownMode {
    AfterCommit,
    BeforeCommit,
}

struct CommitUnknownStore {
    inner: Arc<dyn StateStore>,
    mode: CommitUnknownMode,
}

struct CommitUnknownTransaction {
    inner: Option<Box<dyn WriteTransaction>>,
    mode: CommitUnknownMode,
}

impl CommitUnknownTransaction {
    fn inner(&mut self) -> &mut dyn WriteTransaction {
        self.inner.as_deref_mut().expect("transaction is active")
    }
}

#[async_trait]
impl ReadTransaction for CommitUnknownTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner().get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.inner().range(request).await
    }

    async fn abort(mut self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner
            .take()
            .expect("transaction is active")
            .abort()
            .await
    }
}

#[async_trait]
impl WriteTransaction for CommitUnknownTransaction {
    fn transaction_id(&self) -> &TransactionId {
        self.inner
            .as_deref()
            .expect("transaction is active")
            .transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner().put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner().delete(key, precondition).await
    }

    async fn commit(mut self: Box<Self>) -> StateStoreCommitOutcome {
        if matches!(self.mode, CommitUnknownMode::AfterCommit) {
            let outcome = self
                .inner
                .take()
                .expect("transaction is active")
                .commit()
                .await;
            if !matches!(outcome, StateStoreCommitOutcome::Committed(_)) {
                return outcome;
            }
        }
        StateStoreCommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "injected commit unknown",
        ))
    }
}

#[async_trait]
impl StateStore for CommitUnknownStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Ok(Box::new(CommitUnknownTransaction {
            inner: Some(self.inner.begin_write(transaction_id, purpose).await?),
            mode: self.mode,
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        self.inner.poll_changes(request).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        self.inner.resolve_commit(transaction_id).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn creates_uuid_v7_operation_and_unfinished_index_atomically() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    assert_eq!(
        operation_id.as_uuid().get_version(),
        Some(Version::SortRand)
    );

    let operation_key = key(OPERATION_PREFIX, *operation_id.as_uuid());
    let unfinished_key = key(UNFINISHED_PREFIX, *operation_id.as_uuid());
    let mut read = store.begin_read().await.unwrap();
    assert!(read.get(&operation_key).await.unwrap().is_some());
    assert!(read.get(&unfinished_key).await.unwrap().is_some());
    read.abort().await.unwrap();

    let stored = journal.load(operation_id).unwrap().unwrap();
    assert_ne!(*stored.operation_id.as_uuid(), stored.last_mutation_id);
    assert_eq!(stored.revision, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_transition_removes_unfinished_index() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Aborting)
        .unwrap();
    journal
        .transition(operation_id, OperationState::Aborted)
        .unwrap();

    assert!(journal.list_unfinished().unwrap().is_empty());
    let mut read = store.begin_read().await.unwrap();
    assert!(
        read.get(&key(UNFINISHED_PREFIX, *operation_id.as_uuid()))
            .await
            .unwrap()
            .is_none()
    );
    read.abort().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_loads_unfinished_operations() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (mut host, store, journal) = open_store(&path).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    drop(journal);
    drop(store);
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();

    let (_host, _store, reopened) = open_store(&path).await;
    let unfinished = reopened.list_unfinished().unwrap();
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_fact_replay_is_idempotent() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Committing)
        .unwrap();
    let fact = OperationFact {
        state: OperationState::Committed,
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id: 7,
            written_manifest_paths: vec!["m.avro".to_string()],
        }),
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: None,
    };
    journal.record_fact(operation_id, fact.clone()).unwrap();
    journal.record_fact(operation_id, fact).unwrap();
    assert_eq!(
        journal
            .load(operation_id)
            .unwrap()
            .unwrap()
            .commit_outcome
            .unwrap()
            .snapshot_id,
        7
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicting_fact_replay_is_rejected() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    journal
        .transition(operation_id, OperationState::Committing)
        .unwrap();
    let fact = |snapshot_id| OperationFact {
        state: OperationState::Committed,
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id,
            written_manifest_paths: Vec::new(),
        }),
        cleanup_outcome: None,
        recovery_evidence: None,
        failure: None,
    };
    journal.record_fact(operation_id, fact(7)).unwrap();
    let error = journal.record_fact(operation_id, fact(8)).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.to_string().contains("conflicting"));
}

#[tokio::test(flavor = "multi_thread")]
async fn illegal_transition_is_rejected() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let operation_id = journal.create_preparing(request()).unwrap();
    let error = journal
        .transition(operation_id, OperationState::Finalized)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_schema_version_fails_open() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (host, store, journal) = open_store(&path).await;
    drop(journal);
    let operation_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, operation_id),
        raw_operation(operation_id, 99),
    )
    .await;
    let error =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .err()
            .expect("unknown schema must fail open");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    drop(host);
}

#[tokio::test(flavor = "multi_thread")]
async fn key_value_operation_identity_mismatch_is_corruption() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("state.sqlite");
    let (_host, store, journal) = open_store(&path).await;
    drop(journal);
    let key_id = Uuid::now_v7();
    let value_id = Uuid::now_v7();
    raw_put(
        store.as_ref(),
        key(OPERATION_PREFIX, key_id),
        raw_operation(value_id, 1),
    )
    .await;
    let error =
        StateStoreOperationJournal::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .err()
            .expect("identity mismatch must fail open");
    assert_eq!(error.kind(), DmlErrorKind::JournalCorruption);
    assert!(error.to_string().contains("identity mismatch"));
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_unknown_matching_last_mutation_is_success() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    drop(journal);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: store,
        mode: CommitUnknownMode::AfterCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let operation_id = journal.create_preparing(request()).unwrap();
    assert!(journal.load(operation_id).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_unknown_without_matching_record_is_unresolved() {
    let temp = TempDir::new().unwrap();
    let (_host, store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    drop(journal);
    let wrapped: Arc<dyn StateStore> = Arc::new(CommitUnknownStore {
        inner: Arc::clone(&store),
        mode: CommitUnknownMode::BeforeCommit,
    });
    let journal = StateStoreOperationJournal::open(wrapped, tokio::runtime::Handle::current())
        .await
        .unwrap();
    let error = journal.create_preparing(request()).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnresolved);
    let clean = StateStoreOperationJournal::open(store, tokio::runtime::Handle::current())
        .await
        .unwrap();
    assert!(clean.list_unfinished().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_record_is_rejected_without_truncation() {
    let temp = TempDir::new().unwrap();
    let (_host, _store, journal) = open_store(&temp.path().join("state.sqlite")).await;
    let mut oversized = request();
    oversized.attempt_id = "x".repeat(70 * 1024);
    let error = journal.create_preparing(oversized).unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(journal.list_unfinished().unwrap().is_empty());
}
