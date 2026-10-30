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

use super::*;
use futures::future::BoxFuture;
use novarocks_state_store_api::{
    AttemptSupervisor, CommitOutcome, StateStoreBlockingOriginal, StateStoreBlockingSlot,
    StateStoreExecutionFailure, StateStoreExecutionPort, StateStoreLimits, StoreIdentity, Value,
    WriteAttempt,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

// This fixture selects the original API over the real pure in-memory CAS and
// pagination implementation. It is not a physical executor or SQLite test.
// Every repository-facing legacy entry panics; only the test provider itself
// adapts the pure in-memory transaction into the original transaction contract.
struct NoPhysicalPort;
impl StateStoreExecutionPort for NoPhysicalPort {
    fn bind_original_in(
        &self,
        _: &Arc<dyn StateStoreBlockingOriginal>,
        _: Instant,
        _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        panic!("the pure fixture must not dispatch physical work")
    }
    fn request_progress(&self) {}
}
fn context() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
    let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
    let context = StateStoreExecutionContext::new(&port, Instant::now() + Duration::from_secs(30));
    (port, context)
}
struct OriginalMemory {
    inner: InMemoryStateStore,
    context: StateStoreExecutionContext,
    reads: AtomicUsize,
    read_deadlines: Arc<Mutex<Vec<Instant>>>,
    range_deadlines: Arc<Mutex<Vec<Instant>>>,
    fail_after_commit: Arc<AtomicBool>,
    last_receipt: Arc<Mutex<Option<CommitReceipt>>>,
    read_failure: SharedFailureCause,
    matching_gate: Mutex<Option<Arc<MatchingGate>>>,
    begin_gate: Mutex<Option<Arc<MatchingGate>>>,
    commit_return_gate: Arc<Mutex<Option<Arc<MatchingGate>>>>,
    unknown_on_commit: Arc<AtomicBool>,
    pending_commit: Arc<Mutex<Option<Box<dyn WriteTransaction>>>>,
    commit_failure: SharedFailureCause,
}
impl OriginalMemory {
    fn new(context: StateStoreExecutionContext) -> Self {
        Self {
            inner: InMemoryStateStore::new("catalog-original-api"),
            context,
            reads: AtomicUsize::new(0),
            read_deadlines: Arc::new(Mutex::new(Vec::new())),
            range_deadlines: Arc::new(Mutex::new(Vec::new())),
            matching_gate: Mutex::new(None),
            begin_gate: Mutex::new(None),
            commit_return_gate: Arc::new(Mutex::new(None)),
            unknown_on_commit: Arc::new(AtomicBool::new(false)),
            pending_commit: Arc::new(Mutex::new(None)),
            commit_failure: StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "original commit response is unavailable while its transaction remains owned",
            )
            .into_original_failure(),
            fail_after_commit: Arc::new(AtomicBool::new(false)),
            last_receipt: Arc::new(Mutex::new(None)),
            read_failure: StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "original matching read refused",
            )
            .into_original_failure(),
        }
    }
    fn check(&self, execution: &StateStoreExecutionContext) {
        assert!(execution.shares_owner(&self.context));
        assert!(execution.deadline() <= self.context.deadline());
    }
}
impl StateStore for OriginalMemory {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }
    fn begin_read<'life0, 'async_trait>(
        &'life0 self,
    ) -> BoxFuture<'async_trait, Result<Box<dyn ReadTransaction>, StateStoreError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        panic!("repository must not select legacy read")
    }
    fn begin_write<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: WriteAttempt,
        _: &'life1 str,
    ) -> BoxFuture<'async_trait, Result<Box<dyn WriteTransaction>, StateStoreError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        panic!("repository must not select legacy write")
    }
    fn identity<'life0, 'async_trait>(
        &'life0 self,
    ) -> BoxFuture<'async_trait, Result<StoreIdentity, StateStoreError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.identity()
    }
    fn begin_original_read_in<'life0, 'life1, 'async_trait>(
        &'life0 self,
        execution: &'life1 StateStoreExecutionContext,
    ) -> BoxFuture<'async_trait, Result<Box<dyn OriginalReadTransaction>, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.check(execution);
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.read_deadlines
            .lock()
            .unwrap()
            .push(execution.deadline());
        Box::pin(async move {
            let gate = if self.last_receipt.lock().unwrap().is_some() {
                self.matching_gate.lock().unwrap().take()
            } else {
                None
            };
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if self.fail_after_commit.load(Ordering::SeqCst)
                && self.last_receipt.lock().unwrap().is_some()
            {
                return Err(self.read_failure.clone());
            }
            let tx = self
                .inner
                .begin_read()
                .await
                .map_err(StateStoreError::into_original_failure)?;
            Ok(Box::new(OriginalRead(
                Some(tx),
                execution.deadline(),
                self.range_deadlines.clone(),
            )) as Box<dyn OriginalReadTransaction>)
        })
    }
    fn begin_original_write_in<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        attempt: WriteAttempt,
        purpose: &'life1 str,
        execution: &'life2 StateStoreExecutionContext,
    ) -> BoxFuture<
        'async_trait,
        Result<Box<dyn OriginalWriteTransaction>, StateStoreExecutionFailure>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.check(execution);
        Box::pin(async move {
            let gate = self.begin_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            if self.unknown_on_commit.load(Ordering::SeqCst) {
                // Conservative dispatch precedes the provider's owned work;
                // the same attempt stays live in pending_commit until resumed.
                attempt.mark_dispatched().unwrap();
            }
            let tx = self
                .inner
                .begin_write(attempt, purpose)
                .await
                .map_err(StateStoreError::into_original_failure)?;
            Ok(Box::new(OriginalWrite {
                tx: Some(tx),
                last_receipt: self.last_receipt.clone(),
                unknown_on_commit: self.unknown_on_commit.clone(),
                pending_commit: self.pending_commit.clone(),
                commit_failure: self.commit_failure.clone(),
                commit_return_gate: self.commit_return_gate.clone(),
            }) as Box<dyn OriginalWriteTransaction>)
        })
    }
}
struct OriginalRead(
    Option<Box<dyn ReadTransaction>>,
    Instant,
    Arc<Mutex<Vec<Instant>>>,
);
impl OriginalReadTransaction for OriginalRead {
    fn get_in<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        key: &'life1 Key,
    ) -> BoxFuture<'async_trait, Result<Option<StateRecord>, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.0
                .as_mut()
                .unwrap()
                .get(key)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn range_in<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        request: &'life1 RangeRequest,
    ) -> BoxFuture<'async_trait, Result<RangePage, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.2.lock().unwrap().push(self.1);
        Box::pin(async move {
            self.0
                .as_mut()
                .unwrap()
                .range(request)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn close_in<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.0
                .take()
                .unwrap()
                .abort()
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
}
struct OriginalWrite {
    tx: Option<Box<dyn WriteTransaction>>,
    last_receipt: Arc<Mutex<Option<CommitReceipt>>>,
    unknown_on_commit: Arc<AtomicBool>,
    pending_commit: Arc<Mutex<Option<Box<dyn WriteTransaction>>>>,
    commit_failure: SharedFailureCause,
    commit_return_gate: Arc<Mutex<Option<Arc<MatchingGate>>>>,
}
#[derive(Default)]
struct MatchingGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl OriginalReadTransaction for OriginalWrite {
    fn get_in<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        key: &'life1 Key,
    ) -> BoxFuture<'async_trait, Result<Option<StateRecord>, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.tx
                .as_mut()
                .unwrap()
                .get(key)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn range_in<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        request: &'life1 RangeRequest,
    ) -> BoxFuture<'async_trait, Result<RangePage, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.tx
                .as_mut()
                .unwrap()
                .range(request)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn close_in<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(tx) = self.tx.take() {
                tx.abort()
                    .await
                    .map_err(StateStoreError::into_original_failure)?;
            }
            Ok(())
        })
    }
}
impl OriginalWriteTransaction for OriginalWrite {
    fn attempt(&self) -> novarocks_state_store_api::AttemptId {
        self.tx.as_ref().unwrap().attempt()
    }
    fn original_failure(&self) -> Option<StateStoreExecutionFailure> {
        None
    }
    fn put_in<'life0, 'async_trait>(
        &'life0 mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.tx
                .as_mut()
                .unwrap()
                .put(key, value, precondition)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn delete_in<'life0, 'async_trait>(
        &'life0 mut self,
        key: Key,
        precondition: Precondition,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.tx
                .as_mut()
                .unwrap()
                .delete(key, precondition)
                .await
                .map_err(StateStoreError::into_original_failure)
        })
    }
    fn commit_in<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> BoxFuture<'async_trait, Result<CommitOutcome, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if self.unknown_on_commit.swap(false, Ordering::SeqCst) {
                let tx = self.tx.take().unwrap();
                let mut pending = self.pending_commit.lock().unwrap();
                assert!(pending.is_none());
                *pending = Some(tx);
                return Err(self.commit_failure.clone());
            }
            let outcome = self.tx.take().unwrap().commit().await;
            if let CommitOutcome::Committed(receipt) = &outcome {
                *self.last_receipt.lock().unwrap() = Some(receipt.clone());
            }
            let gate = self.commit_return_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            Ok(outcome)
        })
    }
}

fn requested(name: &str) -> CatalogAttachment {
    CatalogAttachment {
        attachment_id: Uuid::now_v7(),
        instance_id: ConnectorInstanceId::parse(name).unwrap(),
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        display_name: name.to_owned(),
        durable_properties: vec![("type".into(), "iceberg".into())],
        credential_bindings: Vec::new(),
        created_at_ms: 1,
    }
}

#[tokio::test]
async fn original_api_keeps_cas_pagination_and_same_context() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let first = repository
        .create_in(requested("b"), &execution)
        .await
        .unwrap();
    repository
        .create_in(requested("a"), &execution)
        .await
        .unwrap();
    repository
        .create_in(requested("c"), &execution)
        .await
        .unwrap();
    let page = repository
        .list_with_page_size_in(1, &execution)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|v| v.attachment.instance_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    assert_eq!(repository.list_in(&execution).await.unwrap(), page);
    assert_eq!(
        repository
            .get_in(&first.attachment.instance_id, &execution)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let duplicate = repository
        .create_in(requested("b"), &execution)
        .await
        .unwrap_err();
    assert_eq!(duplicate.kind(), CatalogAttachmentErrorKind::AlreadyExists);
    assert_eq!(
        duplicate
            .source_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .unwrap()
            .kind(),
        StateStoreErrorKind::PreconditionFailed
    );
    repository
        .drop_exact_in(first.clone(), &execution)
        .await
        .unwrap();
    let replacement = repository
        .create_in(requested("b"), &execution)
        .await
        .unwrap();
    assert_ne!(
        first.attachment.attachment_id,
        replacement.attachment.attachment_id
    );
    assert_eq!(
        repository
            .drop_exact_in(first, &execution)
            .await
            .unwrap_err()
            .kind(),
        CatalogAttachmentErrorKind::Conflict
    );
    assert_eq!(
        repository
            .get_in(&replacement.attachment.instance_id, &execution)
            .await
            .unwrap(),
        Some(replacement)
    );
    assert!(store.reads.load(Ordering::SeqCst) >= 8);
}

#[tokio::test]
async fn original_committed_create_keeps_receipt_when_matching_read_fails() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    store.fail_after_commit.store(true, Ordering::SeqCst);
    let error = repository
        .create_in(requested("a"), &execution)
        .await
        .unwrap_err();
    assert_eq!(error.source_failure(), Some(&store.read_failure));
    assert_eq!(error.failure_fact(), store.read_failure.failure_fact());
    let receipt = store.last_receipt.lock().unwrap().clone().unwrap();
    assert_eq!(error.committed_receipt(), Some(&receipt));
    assert!(error.commit_observation().is_none());
    assert_eq!(repository.published_wakeups(), 0);
    store.fail_after_commit.store(false, Ordering::SeqCst);
    assert!(
        repository
            .get_in(&ConnectorInstanceId::parse("a").unwrap(), &execution)
            .await
            .unwrap()
            .is_some()
    );
    let parts = error.into_parts();
    assert_eq!(parts.source, Some(store.read_failure.clone()));
    assert_eq!(parts.committed_receipt, Some(receipt));
}

struct LegacyReader;
impl CatalogReferenceReader for LegacyReader {
    fn observe_references<'a>(
        &'a self,
        _: &'a dyn StateStore,
        _: &'a ConnectorInstanceId,
        _: usize,
    ) -> BoxFuture<'a, Result<Option<&'static str>, String>> {
        panic!("missing original capability must not fall back to legacy reader")
    }
}
#[tokio::test]
async fn original_capability_refusal_never_calls_legacy_reader_or_store() {
    let (_port, execution) = context();
    let pure: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new("no-original-provider"));
    let error = match CatalogAttachmentRepository::open_in(
        pure.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("provider without original capability must refuse"),
    };
    assert!(
        error
            .source_failure()
            .unwrap()
            .downcast_ref::<StateStoreOriginalUnsupported>()
            .is_some()
    );
    let repository = CatalogAttachmentRepository::open(pure, StateStoreRunPolicy::default())
        .await
        .unwrap();
    let error = repository
        .observe_references_before_drop_in(
            &LegacyReader,
            &ConnectorInstanceId::parse("a").unwrap(),
            1,
            &execution,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .source_failure()
            .unwrap()
            .downcast_ref::<StateStoreOriginalUnsupported>()
            .is_some()
    );
}

#[test]
fn attachment_error_keeps_same_unresolved_observation_and_both_causes() {
    // The original supervisor issues both capabilities. No test reconstructs an
    // AttemptId or fabricates a receipt/physical-exit proof.
    let store = InMemoryStateStore::new("unknown-observation-owner");
    let (attempt, observation) = store.attempts().reserve().unwrap();
    attempt.mark_dispatched().unwrap();
    let commit = StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "original commit response lost",
    )
    .into_original_failure();
    let read = StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "original adjudication unavailable",
    )
    .into_original_failure();
    let error = store_source(read.clone()).with_unknown(observation, commit.clone());
    let wrapped = crate::CatalogApplicationError::from_domain(
        crate::CatalogApplicationErrorKind::Unavailable,
        error,
    );
    let inner = wrapped
        .source_failure()
        .unwrap()
        .downcast_ref::<CatalogAttachmentError>()
        .unwrap();
    assert_eq!(inner.source_failure(), Some(&read));
    assert_eq!(inner.commit_failure(), Some(&commit));
    assert_eq!(wrapped.failure_fact(), read.failure_fact());
    let alias = inner.clone();
    assert_eq!(alias.commit_observation().unwrap().id(), attempt.id());
    assert_eq!(alias.commit_observation().unwrap().peek().unwrap(), None);
    attempt.settle(AttemptOutcome::NotCommitted).unwrap();
    assert_eq!(
        inner.commit_observation().unwrap().peek().unwrap(),
        Some(AttemptOutcome::NotCommitted)
    );
    assert_eq!(
        alias.commit_observation().unwrap().peek().unwrap(),
        Some(AttemptOutcome::NotCommitted)
    );
    drop(wrapped);
    drop(attempt);
    assert_eq!(store.attempts().outstanding(), 1);
    drop(alias);
    assert_eq!(store.attempts().outstanding(), 0);
}

struct PanicCatalogWake;
impl std::task::Wake for PanicCatalogWake {
    fn wake(self: Arc<Self>) {
        panic!("foreign catalog subscriber wake panicked");
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("foreign catalog subscriber wake panicked");
    }
}

#[tokio::test]
async fn original_create_receives_durable_and_confirmed_facts_before_foreign_wake() {
    use futures::FutureExt;
    use std::future::Future;
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let mut signal = repository.wakeup_signal();
    let mut changed = Box::pin(signal.changed());
    let waker = std::task::Waker::from(Arc::new(PanicCatalogWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    let receiving = crate::CatalogMutationReceipt::default();
    let input = requested("stable-create");
    let unwind = std::panic::AssertUnwindSafe(repository.create_in_receiving(
        input.clone(),
        &execution,
        &receiving,
    ))
    .catch_unwind()
    .await;
    assert!(
        unwind.is_err(),
        "the actual public watch subscriber must unwind"
    );
    drop(changed);
    let actual = store.last_receipt.lock().unwrap().clone().unwrap();
    assert_eq!(receiving.durable_commit_receipt(), Some(actual));
    assert_eq!(receiving.requested_create(), Some(input.clone()));
    assert!(receiving.has_committed_effect());
    let found = repository
        .get_in(&input.instance_id, &execution)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receiving.committed(),
        Some(crate::CatalogMutationCommitted::Created(found))
    );
    assert!(receiving.commit_observation().is_none());
}

#[tokio::test]
async fn original_drop_receives_exact_input_and_receipt_before_foreign_wake() {
    use futures::FutureExt;
    use std::future::Future;
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let expected = repository
        .create_in(requested("stable-drop"), &execution)
        .await
        .unwrap();
    let mut signal = repository.wakeup_signal();
    let mut changed = Box::pin(signal.changed());
    let waker = std::task::Waker::from(Arc::new(PanicCatalogWake));
    assert!(
        changed
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    let receiving = crate::CatalogMutationReceipt::default();
    let unwind = std::panic::AssertUnwindSafe(repository.drop_exact_in_receiving(
        expected.clone(),
        &execution,
        &receiving,
    ))
    .catch_unwind()
    .await;
    assert!(unwind.is_err());
    drop(changed);
    assert_eq!(
        receiving.durable_commit_receipt(),
        store.last_receipt.lock().unwrap().clone()
    );
    assert_eq!(receiving.requested_drop(), Some(expected.clone()));
    assert_eq!(
        receiving.committed(),
        Some(crate::CatalogMutationCommitted::Deleted(expected.clone()))
    );
    assert!(
        repository
            .get_in(&expected.attachment.instance_id, &execution)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn original_create_receives_durable_fact_before_matching_read_and_borrower_drop() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let gate = Arc::new(MatchingGate::default());
    *store.matching_gate.lock().unwrap() = Some(gate.clone());
    let receiving = crate::CatalogMutationReceipt::default();
    let input = requested("matching-held");
    let mut operation =
        Box::pin(repository.create_in_receiving(input.clone(), &execution, &receiving));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut operation => panic!("matching must be held: {result:?}"),
            () = gate.entered.notified() => {}
        }
    })
    .await
    .expect("matching read did not enter");
    assert_eq!(
        receiving.durable_commit_receipt(),
        store.last_receipt.lock().unwrap().clone()
    );
    assert!(receiving.has_committed_effect());
    assert!(
        receiving.committed().is_none(),
        "versioned matching has not completed"
    );
    assert_eq!(repository.published_wakeups(), 0);
    drop(operation);
    assert_eq!(receiving.requested_create(), Some(input.clone()));
    assert!(receiving.durable_commit_receipt().is_some());
    gate.release.notify_one();
    assert!(
        repository
            .get_in(&input.instance_id, &execution)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn original_unknown_receives_same_observation_and_cause_without_replay() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    store.unknown_on_commit.store(true, Ordering::SeqCst);
    let receiving = crate::CatalogMutationReceipt::default();
    let input = requested("unknown-owned");
    let error = repository
        .create_in_receiving(input.clone(), &execution, &receiving)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), CatalogAttachmentErrorKind::CommitUnknown);
    let observed = error.commit_observation().unwrap().clone();
    assert_eq!(receiving.commit_observation().unwrap().id(), observed.id());
    assert_eq!(
        receiving.commit_failure(),
        Some(store.commit_failure.clone())
    );
    assert_eq!(receiving.requested_create(), Some(input.clone()));
    assert!(!receiving.has_committed_effect());
    assert_eq!(
        observed.outcome_in(&execution).await.unwrap(),
        AttemptOutcome::Unresolved
    );
    assert_eq!(repository.published_wakeups(), 0);
    assert_eq!(store.attempts().outstanding(), 1);
    drop(error);
    // Resume the SAME owned transaction, not a replay or reconstructed id.
    let original = store.pending_commit.lock().unwrap().take().unwrap();
    assert_eq!(original.attempt(), observed.id());
    let actual = match original.commit().await {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("original commit failed: {other:?}"),
    };
    assert_eq!(
        observed.outcome_in(&execution).await.unwrap(),
        AttemptOutcome::Committed(actual.clone())
    );
    receiving.record_committed(&actual).unwrap();
    assert!(
        receiving.commit_observation().is_none(),
        "real terminal receipt releases this observer alias"
    );
    assert_eq!(
        receiving.commit_failure(),
        Some(store.commit_failure.clone())
    );
    assert_eq!(receiving.durable_commit_receipt(), Some(actual));
    drop(observed);
    assert_eq!(store.attempts().outstanding(), 0);
}

#[tokio::test]
async fn original_receiving_rejects_reuse_without_replacing_input_or_fact() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let receiving = crate::CatalogMutationReceipt::default();
    let input = requested("single-receiver");
    let created = repository
        .create_in_receiving(input.clone(), &execution, &receiving)
        .await
        .unwrap();
    let durable = receiving.durable_commit_receipt();
    let other = requested("foreign-input");
    assert_eq!(
        repository
            .create_in_receiving(other.clone(), &execution, &receiving)
            .await
            .unwrap_err()
            .kind(),
        CatalogAttachmentErrorKind::Conflict
    );
    assert_eq!(receiving.requested_create(), Some(input));
    assert_eq!(
        receiving.committed(),
        Some(crate::CatalogMutationCommitted::Created(created))
    );
    assert_eq!(receiving.durable_commit_receipt(), durable);
    assert!(
        repository
            .get_in(&other.instance_id, &execution)
            .await
            .unwrap()
            .is_none()
    );
}

// A pure evidence owner with no physical work. The real issued observation is
// borrowed through outcome_in; neither the receiver nor cancellation mints proof.
struct GatedOriginalEvidence {
    execution: StateStoreExecutionContext,
    gate: Arc<MatchingGate>,
}
impl novarocks_state_store_api::InDoubtAdjudicator for GatedOriginalEvidence {
    fn adjudicate<'life0, 'async_trait>(
        &'life0 self,
        _: novarocks_state_store_api::AttemptId,
    ) -> BoxFuture<'async_trait, Result<AttemptOutcome, StateStoreError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        panic!("original observation must never select legacy evidence")
    }
    fn release_evidence<'life0, 'async_trait>(
        &'life0 self,
        _: novarocks_state_store_api::AttemptId,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        panic!("original observation must never select legacy release")
    }
    fn adjudicate_in<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: novarocks_state_store_api::AttemptId,
        execution: &'life1 StateStoreExecutionContext,
    ) -> BoxFuture<'async_trait, Result<AttemptOutcome, StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        assert!(execution.shares_owner(&self.execution));
        Box::pin(async move {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
            Ok(AttemptOutcome::Unresolved)
        })
    }
    fn release_evidence_in<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: novarocks_state_store_api::AttemptId,
        execution: &'life1 StateStoreExecutionContext,
    ) -> BoxFuture<'async_trait, Result<(), StateStoreExecutionFailure>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        assert!(execution.shares_owner(&self.execution));
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn original_receiving_keeps_issued_unknown_during_cancelled_outcome_borrow() {
    let (_port, execution) = context();
    let gate = Arc::new(MatchingGate::default());
    let supervisor = AttemptSupervisor::new(
        std::num::NonZeroUsize::new(1).unwrap(),
        Arc::new(GatedOriginalEvidence {
            execution: execution.clone(),
            gate: gate.clone(),
        }),
    );
    let (attempt, observation) = supervisor.reserve().unwrap();
    attempt.mark_dispatched().unwrap();
    let failure = StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "same original response loss",
    )
    .into_original_failure();
    let receiving = crate::CatalogMutationReceipt::default();
    receiving
        .begin_create(&requested("observing-original"))
        .unwrap();
    receiving.record_unknown(&observation, &failure).unwrap();
    let mut borrower = Box::pin(observation.outcome_in(&execution));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            answer = &mut borrower => panic!("evidence must be held: {answer:?}"),
            () = gate.entered.notified() => {}
        }
    })
    .await
    .expect("original evidence did not enter");
    assert_eq!(
        receiving.commit_observation().unwrap().id(),
        observation.id()
    );
    assert_eq!(receiving.commit_failure(), Some(failure.clone()));
    drop(borrower);
    assert_eq!(supervisor.outstanding(), 1);
    assert!(!receiving.has_committed_effect());
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), observation.outcome_in(&execution))
            .await
            .unwrap()
            .unwrap(),
        AttemptOutcome::Unresolved
    );
    assert!(receiving.commit_observation().is_some());
    // Only the actual capability owner can declare this no-I/O fixture done.
    attempt.settle(AttemptOutcome::NotCommitted).unwrap();
    assert_eq!(
        observation.outcome_in(&execution).await.unwrap(),
        AttemptOutcome::NotCommitted
    );
    receiving.record_not_committed(&observation).unwrap();
    assert!(receiving.commit_observation().is_none());
    assert!(!receiving.has_committed_effect());
    drop((observation, attempt));
    assert_eq!(supervisor.outstanding(), 0);
}

#[tokio::test]
async fn original_receiving_preserves_first_cause_across_two_real_attempts() {
    let (_port, execution) = context();
    let gate = Arc::new(MatchingGate::default());
    let supervisor = AttemptSupervisor::new(
        std::num::NonZeroUsize::new(1).unwrap(),
        Arc::new(GatedOriginalEvidence {
            execution: execution.clone(),
            gate: gate.clone(),
        }),
    );
    let receiving = crate::CatalogMutationReceipt::default();
    receiving
        .begin_create(&requested("same-logical-input"))
        .unwrap();
    let first_cause = StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "first actual attempt response loss",
    )
    .into_original_failure();
    let second_cause = StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "second actual attempt response loss",
    )
    .into_original_failure();
    let (first, first_observation) = supervisor.reserve().unwrap();
    let first_id = first.id();
    first.mark_dispatched().unwrap();
    receiving
        .record_unknown(&first_observation, &first_cause)
        .unwrap();
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            first_observation.outcome_in(&execution)
        )
        .await
        .unwrap()
        .unwrap(),
        AttemptOutcome::Unresolved
    );
    // This no-I/O provider's original capability owner has actually finished;
    // neither the receiving slot nor its dropped borrower supplies that proof.
    first.settle(AttemptOutcome::NotCommitted).unwrap();
    assert_eq!(
        first_observation.outcome_in(&execution).await.unwrap(),
        AttemptOutcome::NotCommitted
    );
    receiving.record_not_committed(&first_observation).unwrap();
    assert!(receiving.commit_observation().is_none());
    assert_eq!(receiving.commit_failure(), Some(first_cause.clone()));
    assert_eq!(
        supervisor.outstanding(),
        1,
        "real external observation still owns the first slot"
    );
    drop(first);
    assert_eq!(supervisor.outstanding(), 1);
    assert_eq!(
        supervisor.reserve().err().unwrap().kind(),
        StateStoreErrorKind::Saturated
    );
    drop(first_observation);
    assert_eq!(supervisor.outstanding(), 0);

    let (second, second_observation) = supervisor.reserve().unwrap();
    assert_ne!(second.id(), first_id);
    second.mark_dispatched().unwrap();
    receiving
        .record_unknown(&second_observation, &second_cause)
        .unwrap();
    assert_eq!(receiving.commit_observation().unwrap().id(), second.id());
    assert_eq!(receiving.commit_failure(), Some(first_cause.clone()));
    assert_ne!(receiving.commit_failure(), Some(second_cause));
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            second_observation.outcome_in(&execution)
        )
        .await
        .unwrap()
        .unwrap(),
        AttemptOutcome::Unresolved
    );
    assert!(!receiving.has_committed_effect());
    assert!(receiving.durable_commit_receipt().is_none());
    assert!(receiving.committed().is_none());
    assert_eq!(supervisor.outstanding(), 1);
    second.settle(AttemptOutcome::NotCommitted).unwrap();
    assert_eq!(
        second_observation.outcome_in(&execution).await.unwrap(),
        AttemptOutcome::NotCommitted
    );
    receiving.record_not_committed(&second_observation).unwrap();
    assert_eq!(receiving.commit_failure(), Some(first_cause));
    assert!(receiving.commit_observation().is_none());
    drop((second_observation, second));
    assert_eq!(supervisor.outstanding(), 0);
}

// Reuse this module's real pure original read implementation for the service
// deadline test; it panics if a caller chooses a legacy provider API.
pub(crate) type OriginalReadFixture = (
    Arc<dyn StateStore>,
    Arc<dyn StateStoreExecutionPort>,
    StateStoreExecutionContext,
    Arc<Mutex<Vec<Instant>>>,
    Arc<Mutex<Vec<Instant>>>,
);

pub(super) fn read_fixture() -> OriginalReadFixture {
    let (port, context) = context();
    let store = Arc::new(OriginalMemory::new(context.clone()));
    let read_deadlines = store.read_deadlines.clone();
    let range_deadlines = store.range_deadlines.clone();
    (store, port, context, read_deadlines, range_deadlines)
}

#[tokio::test]
async fn original_paginated_list_keeps_one_fixed_transaction_context() {
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    repository
        .create_in(requested("page-a"), &execution)
        .await
        .unwrap();
    repository
        .create_in(requested("page-b"), &execution)
        .await
        .unwrap();
    store.read_deadlines.lock().unwrap().clear();
    store.range_deadlines.lock().unwrap().clear();
    let fixed = execution.with_deadline(Instant::now() + Duration::from_secs(2));
    assert_eq!(
        repository
            .list_with_page_size_in(1, &fixed)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store.read_deadlines.lock().unwrap().as_slice(),
        &[fixed.deadline()]
    );
    let pages = store.range_deadlines.lock().unwrap();
    assert!(pages.len() >= 2);
    assert!(pages.iter().all(|deadline| *deadline == fixed.deadline()));
}

#[tokio::test]
async fn original_receives_attempt_before_foreign_begin_pending_without_claiming_dispatch() {
    use novarocks_state_store_api::CommitObservationState;
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let gate = Arc::new(MatchingGate::default());
    *store.begin_gate.lock().unwrap() = Some(gate.clone());
    let receiving = crate::CatalogMutationReceipt::default();
    let mut borrower =
        Box::pin(repository.create_in_receiving(requested("begin-held"), &execution, &receiving));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut borrower => panic!("begin must be held: {result:?}"),
            () = gate.entered.notified() => {}
        }
    })
    .await
    .expect("original begin did not enter");
    let observed = receiving.commit_observation().unwrap();
    assert_eq!(
        observed.publication_state().unwrap(),
        CommitObservationState::NotDispatched
    );
    assert!(observed.peek().unwrap().is_none());
    assert!(!receiving.has_committed_effect());
    assert!(receiving.commit_failure().is_none());
    drop(borrower);
    assert_eq!(
        observed.publication_state().unwrap(),
        CommitObservationState::NotDispatched
    );
    assert_eq!(store.attempts().outstanding(), 1);
    drop((receiving, observed));
    assert_eq!(store.attempts().outstanding(), 0);
}

#[tokio::test]
async fn original_receives_actual_committed_observation_while_runner_is_still_pending() {
    use novarocks_state_store_api::CommitObservationState;
    let (_port, execution) = context();
    let store = Arc::new(OriginalMemory::new(execution.clone()));
    let repository = CatalogAttachmentRepository::open_in(
        store.clone(),
        StateStoreRunPolicy::default(),
        &execution,
    )
    .await
    .unwrap();
    let gate = Arc::new(MatchingGate::default());
    *store.commit_return_gate.lock().unwrap() = Some(gate.clone());
    let receiving = crate::CatalogMutationReceipt::default();
    let input = requested("commit-return-held");
    let mut borrower =
        Box::pin(repository.create_in_receiving(input.clone(), &execution, &receiving));
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = &mut borrower => panic!("commit result must be held: {result:?}"),
            () = gate.entered.notified() => {}
        }
    })
    .await
    .expect("actual commit did not reach its result gate");
    let actual = store.last_receipt.lock().unwrap().clone().unwrap();
    let observed = receiving.commit_observation().unwrap();
    assert_eq!(
        observed.publication_state().unwrap(),
        CommitObservationState::Terminal(AttemptOutcome::Committed(actual.clone()))
    );
    assert!(
        receiving.durable_commit_receipt().is_none(),
        "repository has not received RunSuccess yet"
    );
    assert!(receiving.committed().is_none());
    assert_eq!(repository.published_wakeups(), 0);
    drop(borrower);
    assert_eq!(receiving.commit_observation().unwrap().id(), actual.attempt);
    assert_eq!(
        observed.peek().unwrap(),
        Some(AttemptOutcome::Committed(actual))
    );
    assert!(
        repository
            .get_in(&input.instance_id, &execution)
            .await
            .unwrap()
            .is_some()
    );
    drop((receiving, observed));
    assert_eq!(store.attempts().outstanding(), 0);
}

#[tokio::test]
async fn original_cap_one_runner_retires_real_conflict_before_reserving_its_retry() {
    use novarocks_state_store_api::CommitObservationState;
    let (_port, execution) = context();
    let mut store = OriginalMemory::new(execution.clone());
    store.inner = InMemoryStateStore::with_limits_and_capacity(
        "actual-conflict-cap-one",
        StateStoreLimits::default(),
        std::num::NonZeroUsize::new(1).unwrap(),
    );
    let receiving = crate::CatalogMutationReceipt::default();
    receiving
        .begin_create(&requested("runner-receiving-only"))
        .unwrap();
    let mut ids = Vec::new();
    let success = tokio::time::timeout(
        Duration::from_secs(5),
        novarocks_state_store_runtime::run_side_effect_free_in_receiving(
            &store,
            &CatalogAttachmentMetrics::default(),
            StateStoreRunPolicy::default(),
            "actual original retry",
            &execution,
            &receiving,
            |tx| {
                let observed = receiving.commit_observation().unwrap();
                assert_eq!(observed.id(), tx.attempt());
                assert_eq!(
                    observed.publication_state().unwrap(),
                    CommitObservationState::NotDispatched
                );
                ids.push(tx.attempt());
                // The first precondition is actually impossible on the absent key;
                // Memory's real commit algorithm publishes Conflict/NotCommitted.
                // The second attempt runs a valid absent CAS through that algorithm.
                let precondition = if ids.len() == 1 {
                    Precondition::Version(
                        VersionToken::try_from(bytes::Bytes::from_static(b"missing-version"))
                            .unwrap(),
                    )
                } else {
                    Precondition::Absent
                };
                Box::pin(async move {
                    tx.put_in(
                        Key::try_from(bytes::Bytes::from_static(b"runner-cas-key")).unwrap(),
                        Value::try_from(bytes::Bytes::from_static(b"value")).unwrap(),
                        precondition,
                    )
                    .await
                })
            },
        ),
    )
    .await
    .expect("receiver alias caused retry self-saturation")
    .unwrap();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(success.receipt.attempt, ids[1]);
    assert_eq!(
        receiving
            .commit_observation()
            .unwrap()
            .publication_state()
            .unwrap(),
        CommitObservationState::Terminal(AttemptOutcome::Committed(success.receipt))
    );
    assert_eq!(store.attempts().outstanding(), 1);
    drop(receiving);
    assert_eq!(store.attempts().outstanding(), 0);
}

#[test]
fn original_receiving_cannot_retire_reserved_or_dispatched_slots() {
    use novarocks_state_store_runtime::StateStoreRunReceiving;
    let (_port, execution) = context();
    let gate = Arc::new(MatchingGate::default());
    let supervisor = AttemptSupervisor::new(
        std::num::NonZeroUsize::new(2).unwrap(),
        Arc::new(GatedOriginalEvidence { execution, gate }),
    );
    let receiving = crate::CatalogMutationReceipt::default();
    receiving
        .begin_create(&requested("actual-proof-only"))
        .unwrap();
    let (first, first_observed) = supervisor.reserve().unwrap();
    let (second, second_observed) = supervisor.reserve().unwrap();
    receiving.receive_attempt(&first_observed).unwrap();
    assert!(receiving.retire_not_committed(&first_observed).is_err());
    assert!(receiving.receive_attempt(&second_observed).is_err());
    first.mark_dispatched().unwrap();
    assert!(receiving.retire_not_committed(&first_observed).is_err());
    assert!(receiving.receive_attempt(&second_observed).is_err());
    first.settle(AttemptOutcome::NotCommitted).unwrap();
    receiving.retire_not_committed(&first_observed).unwrap();
    receiving.receive_attempt(&second_observed).unwrap();
    assert_eq!(receiving.commit_observation().unwrap().id(), second.id());
    second.cancel_before_dispatch().unwrap();
    receiving.retire_not_committed(&second_observed).unwrap();
    drop((first, first_observed, second, second_observed));
    assert_eq!(supervisor.outstanding(), 0);
}
