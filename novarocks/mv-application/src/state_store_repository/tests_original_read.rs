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

// These consumer tests reuse real MV codecs/indexes and memory snapshots.
// Their explicit original adapter is pure; it is not a SQLite/physical-exit test.
use super::*;
use novarocks_failure_contract::FailureProjection;
use novarocks_state_store_api::{
    AttemptOutcome, AttemptSupervisor, OriginalReadTransaction, RangePage,
    StateStoreBlockingOriginal, StateStoreBlockingSlot, StateStoreExecutionContext,
    StateStoreExecutionFailure, StateStoreExecutionPort, StateStoreLimits,
    StateStoreOriginalUnsupported, StoreIdentity, WriteAttempt,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct NoPhysicalPort;
impl StateStoreExecutionPort for NoPhysicalPort {
    fn bind_original_in(
        &self,
        _original: &Arc<dyn StateStoreBlockingOriginal>,
        _deadline: Instant,
        _target: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        Err(StateStoreOriginalUnsupported.into_original_failure())
    }
    fn request_progress(&self) {}
}
fn context() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
    let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
    let context = StateStoreExecutionContext::new(&port, Instant::now() + Duration::from_secs(5));
    (port, context)
}
#[derive(Default)]
struct Trace {
    original_begins: AtomicUsize,
    original_writes: AtomicUsize,
    legacy_writes: AtomicUsize,
    conflict_once: AtomicBool,
    commit_failure: Mutex<Option<StateStoreExecutionFailure>>,
    legacy_begins: AtomicUsize,
    closes: AtomicUsize,
    ranges: Mutex<Vec<RangeRequest>>,
    range_failure: Mutex<Option<StateStoreExecutionFailure>>,
}
struct OriginalReadStore {
    inner: Arc<InMemoryStateStore>,
    expected: StateStoreExecutionContext,
    trace: Arc<Trace>,
}
#[async_trait::async_trait]
impl StateStore for OriginalReadStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.trace.legacy_begins.fetch_add(1, Ordering::SeqCst);
        Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "original read fell back to legacy",
        ))
    }
    async fn begin_original_read_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalReadTransaction>, StateStoreExecutionFailure> {
        assert!(execution.shares_owner(&self.expected));
        assert_eq!(execution.deadline(), self.expected.deadline());
        self.trace.original_begins.fetch_add(1, Ordering::SeqCst);
        // This inner store only supplies deterministic pure snapshots. The
        // selected outer provider entry above is the interface under test.
        let inner = self
            .inner
            .begin_read()
            .await
            .map_err(StateStoreError::into_original_failure)?;
        Ok(Box::new(OriginalRead {
            inner: Some(inner),
            trace: self.trace.clone(),
        }))
    }
    async fn begin_write(
        &self,
        _attempt: WriteAttempt,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.trace.legacy_writes.fetch_add(1, Ordering::SeqCst);
        Err(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "original write fell back to legacy",
        ))
    }
    async fn begin_original_write_in(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
        execution: &StateStoreExecutionContext,
    ) -> Result<
        Box<dyn novarocks_state_store_api::OriginalWriteTransaction>,
        StateStoreExecutionFailure,
    > {
        assert!(execution.shares_owner(&self.expected));
        assert!(execution.deadline() <= self.expected.deadline());
        self.trace.original_writes.fetch_add(1, Ordering::SeqCst);
        let inner = self
            .inner
            .begin_write(attempt, purpose)
            .await
            .map_err(StateStoreError::into_original_failure)?;
        let attempt = inner.attempt();
        Ok(Box::new(OriginalWrite {
            inner: Some(inner),
            trace: self.trace.clone(),
            attempt,
            cached: None,
            failure: None,
        }))
    }
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
}
struct OriginalRead {
    inner: Option<Box<dyn ReadTransaction>>,
    trace: Arc<Trace>,
}
#[async_trait::async_trait]
impl OriginalReadTransaction for OriginalRead {
    async fn get_in(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .get(key)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn range_in(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreExecutionFailure> {
        self.trace.ranges.lock().unwrap().push(request.clone());
        let failure = self.trace.range_failure.lock().unwrap().take();
        if let Some(failure) = failure {
            return Err(failure);
        }
        self.inner
            .as_mut()
            .unwrap()
            .range(request)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure> {
        self.trace.closes.fetch_add(1, Ordering::SeqCst);
        self.inner
            .take()
            .unwrap()
            .abort()
            .await
            .map_err(StateStoreError::into_original_failure)
    }
}
async fn original_repository(
    inner: Arc<InMemoryStateStore>,
    execution: &StateStoreExecutionContext,
) -> (Arc<StateStoreMvRepository>, Arc<Trace>) {
    let trace = Arc::new(Trace::default());
    let store: Arc<dyn StateStore> = Arc::new(OriginalReadStore {
        inner,
        expected: execution.clone(),
        trace: trace.clone(),
    });
    let repository = StateStoreMvRepository::open(store, StateStoreRunPolicy::default())
        .await
        .unwrap();
    (repository, trace)
}
fn bound() -> MvProjectionInventoryBound {
    MvProjectionInventoryBound {
        entries: 4,
        snapshot_bytes: 1024 * 1024,
        raw_page_bytes: 1024 * 1024,
        single_name_bytes: 65_536,
        continuation_token_bytes: 4096,
        decode: crate::persistence::validation::PersistenceDecodeBudget::default(),
    }
}

#[tokio::test]
async fn explicit_view_uses_original_snapshot_for_inventory_lookup_and_dependencies() {
    let (store, legacy) = tests_definition::repository().await;
    let first = legacy
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("first", b"first-object", 1, "orders"),
            None,
        )
        .await
        .unwrap();
    legacy
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("second", b"second-object", 2, "items"),
            None,
        )
        .await
        .unwrap();
    let (_port, execution) = context();
    let (original, trace) = original_repository(store, &execution).await;
    let view = original.in_execution(&execution);
    let inventory = view.list_keyed_projection_inventory(bound()).await.unwrap();
    assert_eq!(inventory.len(), 2);
    assert!(
        inventory
            .iter()
            .any(|entry| entry.mv_id == first.projection.mv_id)
    );
    assert_eq!(
        view.find_keyed_by_target_bounded(&first.projection.facts.target(), bound())
            .await
            .unwrap(),
        Some(first.clone())
    );
    let dependencies = view
        .list_keyed_dependencies_by_downstream_bounded(
            &first,
            MvDependencyReadBound {
                inventory: bound(),
                ..dependency_bound()
            },
        )
        .await
        .unwrap();
    assert!(!dependencies.is_empty());
    assert_eq!(dependencies[0].downstream_mv_id, first.projection.mv_id);
    assert_eq!(trace.legacy_begins.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace.original_begins.load(Ordering::SeqCst),
        trace.closes.load(Ordering::SeqCst)
    );
    let ranges = trace.ranges.lock().unwrap();
    assert!(ranges.iter().all(|request| request.page_size == 1));
    assert!(ranges.iter().any(|request| request.continuation.is_some()));
}
fn dependency_bound() -> MvDependencyReadBound {
    MvDependencyReadBound {
        inventory: bound(),
        entries: 32,
        collection_bytes: 1024 * 1024,
    }
}

#[tokio::test]
async fn catalog_original_scan_keeps_target_dependency_and_absent_selection() {
    let (store, legacy) = tests_definition::repository().await;
    let mut fixture = crate::persistence::test_support::KeyedProjectionFixture::new(
        tests_definition::target("target"),
        Some(1),
    );
    for relation in &mut fixture.definition.relation_occurrences {
        relation.catalog_at_binding = "upstream".into();
    }
    legacy
        .install_keyed_projection(Uuid::now_v7(), fixture.build().unwrap(), None)
        .await
        .unwrap();
    let (_port, execution) = context();
    let trace = Arc::new(Trace::default());
    let original = OriginalReadStore {
        inner: store,
        expected: execution.clone(),
        trace: trace.clone(),
    };
    assert_eq!(
        observe_catalog_references_in(&original, "ice", 1, &execution)
            .await
            .unwrap(),
        Some(MvCatalogReference::Target)
    );
    assert_eq!(
        observe_catalog_references_in(&original, "upstream", 1, &execution)
            .await
            .unwrap(),
        Some(MvCatalogReference::UpstreamDependency)
    );
    assert_eq!(
        observe_catalog_references_in(&original, "absent", 1, &execution)
            .await
            .unwrap(),
        None
    );
    assert_eq!(trace.legacy_begins.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace.original_begins.load(Ordering::SeqCst),
        trace.closes.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn original_range_failure_closes_snapshot_and_retains_the_same_cause() {
    let (store, _) = tests_definition::repository().await;
    let (_port, execution) = context();
    let (repository, trace) = original_repository(store, &execution).await;
    let cause = StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "injected original page refusal",
    )
    .into_original_failure();
    *trace.range_failure.lock().unwrap() = Some(cause.clone());
    let error = repository
        .in_execution(&execution)
        .list_keyed_projection_inventory(bound())
        .await
        .unwrap_err();
    assert_eq!(error.original_failure(), Some(&cause));
    assert_eq!(
        error.original_failure().unwrap().failure_fact(),
        cause.failure_fact()
    );
    assert!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .is_some()
    );
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(trace.closes.load(Ordering::SeqCst), 1);
    assert_eq!(trace.legacy_begins.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unsupported_original_provider_refuses_without_legacy_read_fallback() {
    let (_, repository) = tests_definition::repository().await;
    let (_port, execution) = context();
    let error = repository
        .in_execution(&execution)
        .list_keyed_projections()
        .await
        .unwrap_err();
    assert!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreOriginalUnsupported>()
            .is_some()
    );
}

#[test]
fn unknown_error_retains_the_issued_observation_and_original_state_store_failure() {
    let store = InMemoryStateStore::new("mv-unknown-observation");
    let (attempt, observation) = store.attempts().reserve().unwrap();
    attempt.mark_dispatched().unwrap();
    let id = observation.id();
    let mut error =
        operation::run_failure(novarocks_state_store_runtime::RunFailure::CommitUnknown {
            observation,
            error: StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected uncertain commit",
            ),
        });
    assert_eq!(error.kind(), MvRepositoryErrorKind::CommitUnknown);
    assert_eq!(error.commit_observation().unwrap().id(), id);
    assert_eq!(error.commit_observation().unwrap().peek().unwrap(), None);
    // The actual issued author changes the same original slot after the error
    // was formed. A reconstructed display id could not observe this transition.
    attempt.settle(AttemptOutcome::NotCommitted).unwrap();
    let retained = error.take_commit_observation().unwrap();
    assert_eq!(retained.peek().unwrap(), Some(AttemptOutcome::NotCommitted));
    assert!(error.commit_observation().is_none());
    assert_eq!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .unwrap()
            .kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
}

// This pure adapter executes the real memory-provider transaction/CAS/commit.
// It intentionally makes no physical Original/Root/actual-exit assertion.
struct OriginalWrite {
    inner: Option<Box<dyn WriteTransaction>>,
    trace: Arc<Trace>,
    attempt: novarocks_state_store_api::AttemptId,
    cached: Option<novarocks_state_store_api::CommitOutcome>,
    failure: Option<StateStoreExecutionFailure>,
}
#[async_trait::async_trait]
impl OriginalReadTransaction for OriginalWrite {
    async fn get_in(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .get(key)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn range_in(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .range(request)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure> {
        if let Some(inner) = self.inner.take() {
            inner
                .abort()
                .await
                .map_err(StateStoreError::into_original_failure)?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl novarocks_state_store_api::OriginalWriteTransaction for OriginalWrite {
    fn attempt(&self) -> novarocks_state_store_api::AttemptId {
        self.attempt
    }
    fn original_failure(&self) -> Option<StateStoreExecutionFailure> {
        self.failure.clone()
    }
    async fn put_in(
        &mut self,
        key: Key,
        value: novarocks_state_store_api::Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .put(key, value, precondition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn delete_in(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .delete(key, precondition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn commit_in(
        &mut self,
    ) -> Result<novarocks_state_store_api::CommitOutcome, StateStoreExecutionFailure> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if let Some(outcome) = &self.cached {
            return Ok(outcome.clone());
        }
        if self.trace.conflict_once.swap(false, Ordering::SeqCst) {
            self.inner
                .take()
                .unwrap()
                .abort()
                .await
                .map_err(StateStoreError::into_original_failure)?;
            let outcome = novarocks_state_store_api::CommitOutcome::Conflict(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "injected precommit conflict",
            ));
            self.cached = Some(outcome.clone());
            return Ok(outcome);
        }
        let outcome = self.inner.take().unwrap().commit().await;
        self.cached = Some(outcome.clone());
        let failure = self.trace.commit_failure.lock().unwrap().take();
        if let Some(failure) = failure {
            assert!(matches!(
                outcome,
                novarocks_state_store_api::CommitOutcome::Committed(_)
            ));
            self.failure = Some(failure.clone());
            return Err(failure);
        }
        Ok(outcome)
    }
}

#[tokio::test]
async fn original_write_view_preserves_guarded_cas_dependency_indexes_and_delete() {
    let (store, _) = tests_definition::repository().await;
    let (_port, execution) = context();
    let (repository, trace) = original_repository(store, &execution).await;
    let view = repository.in_execution(&execution);
    let first = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("written", b"write-target", 1, "orders"),
            None,
        )
        .await
        .unwrap();
    let replaced = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("written", b"write-target", 2, "items"),
            Some(first.clone()),
        )
        .await
        .unwrap();
    assert_eq!(first.projection.mv_id, replaced.projection.mv_id);
    assert_ne!(first.version, replaced.version);
    let error = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("written", b"write-target", 3, "stale"),
            Some(first),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert_eq!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .unwrap()
            .kind(),
        StateStoreErrorKind::PreconditionFailed
    );
    assert_eq!(
        view.find_keyed_by_target(&replaced.projection.facts.target(), Default::default())
            .await
            .unwrap(),
        Some(replaced.clone())
    );
    let dependencies = view
        .list_keyed_dependencies_by_downstream_bounded(&replaced, dependency_bound())
        .await
        .unwrap();
    assert!(!dependencies.is_empty());
    assert!(
        dependencies
            .iter()
            .all(|dependency| dependency.upstream.name == "items")
    );
    assert!(
        view.delete_keyed_projection(Uuid::now_v7(), replaced.clone())
            .await
            .unwrap()
    );
    assert!(
        !view
            .delete_keyed_projection(Uuid::now_v7(), replaced)
            .await
            .unwrap()
    );
    assert!(view.list_keyed_projections().await.unwrap().is_empty());
    assert_eq!(
        observe_catalog_references_in(repository.store.as_ref(), "ice", 1, &execution)
            .await
            .unwrap(),
        None
    );
    assert_eq!(trace.legacy_writes.load(Ordering::SeqCst), 0);
    assert_eq!(trace.legacy_begins.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn original_write_retries_only_a_proven_precommit_conflict_without_extra_projection() {
    let (store, _) = tests_definition::repository().await;
    let (_port, execution) = context();
    let (repository, trace) = original_repository(store, &execution).await;
    trace.conflict_once.store(true, Ordering::SeqCst);
    let view = repository.in_execution(&execution);
    let created = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("retry", b"retry-target", 1, "orders"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(trace.original_writes.load(Ordering::SeqCst), 2);
    assert_eq!(trace.legacy_writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        created.projection.mv_id, 1,
        "the rolled-back sequence was never published"
    );
    assert_eq!(view.list_keyed_projections().await.unwrap(), vec![created]);
}

#[tokio::test]
async fn original_write_unknown_retains_same_raw_cause_and_issued_committed_observation() {
    let (store, _) = tests_definition::repository().await;
    let (_port, execution) = context();
    let (repository, trace) = original_repository(store, &execution).await;
    let cause = StateStoreError::new(
        StateStoreErrorKind::Internal,
        "injected lost original commit response",
    )
    .into_original_failure();
    *trace.commit_failure.lock().unwrap() = Some(cause.clone());
    let view = repository.in_execution(&execution);
    let error = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts(
                "unknown_original",
                b"unknown-original-target",
                1,
                "orders",
            ),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::CommitUnknown);
    assert_eq!(error.original_failure(), Some(&cause));
    assert_eq!(error.failure_fact(), cause.failure_fact());
    let carrier = novarocks_failure_contract::SharedFailureCause::new(error);
    let retained = carrier.downcast_ref::<MvRepositoryError>().unwrap();
    assert!(matches!(
        retained.commit_observation().unwrap().peek().unwrap(),
        Some(AttemptOutcome::Committed(_))
    ));
    assert_eq!(
        trace.original_writes.load(Ordering::SeqCst),
        1,
        "an uncertain physical return never replays the write"
    );
    assert_eq!(view.list_keyed_projections().await.unwrap().len(), 1);
    let next = view
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts(
                "after_original",
                b"after-original-target",
                1,
                "items",
            ),
            None,
        )
        .await
        .unwrap();
    assert_eq!(next.projection.mv_id, 2);
}

#[tokio::test]
async fn original_write_view_wipe_reads_and_deletes_on_the_same_explicit_context() {
    let (store, _) = tests_definition::repository().await;
    let (_port, execution) = context();
    let (repository, trace) = original_repository(store, &execution).await;
    let view = repository.in_execution(&execution);
    view.install_keyed_projection(
        Uuid::now_v7(),
        tests_definition::projection_facts("wipe", b"wipe-target", 1, "orders"),
        None,
    )
    .await
    .unwrap();
    view.wipe_accelerator(Uuid::now_v7()).await.unwrap();
    assert!(view.list_keyed_projections().await.unwrap().is_empty());
    assert_eq!(trace.legacy_writes.load(Ordering::SeqCst), 0);
    assert_eq!(trace.legacy_begins.load(Ordering::SeqCst), 0);
    assert_eq!(trace.original_writes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn unsupported_original_write_refuses_without_legacy_write_or_projection() {
    let (_, repository) = tests_definition::repository().await;
    let (_port, execution) = context();
    let error = repository
        .in_execution(&execution)
        .install_keyed_projection(
            Uuid::now_v7(),
            tests_definition::projection_facts("unsupported", b"unsupported-target", 1, "orders"),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreOriginalUnsupported>()
            .is_some()
    );
    assert!(
        repository
            .list_keyed_projections()
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn repository_authors_local_facts_but_preserves_the_original_fact_and_whole_carrier() {
    use crate::repository::failure_reason as R;
    use novarocks_failure_contract::{FailureClass as C, domain};
    for (kind, class, reason) in [
        (
            MvRepositoryErrorKind::InvalidRequest,
            C::Data,
            R::INVALID_REQUEST,
        ),
        (MvRepositoryErrorKind::NotFound, C::Dependency, R::NOT_FOUND),
        (MvRepositoryErrorKind::Conflict, C::Dependency, R::CONFLICT),
        (MvRepositoryErrorKind::Corruption, C::Data, R::CORRUPTION),
        (
            MvRepositoryErrorKind::Unavailable,
            C::Dependency,
            R::UNAVAILABLE,
        ),
        (
            MvRepositoryErrorKind::CommitUnknown,
            C::Dependency,
            R::COMMIT_UNKNOWN,
        ),
    ] {
        let fact = MvRepositoryError::new(kind, "local MV repository refusal").failure_fact();
        assert_eq!(fact.class(), class);
        assert_eq!(fact.reason().domain(), domain::METADATA.get());
        assert_eq!(fact.reason().code(), reason.get());
    }
    let cause = StateStoreError::new(StateStoreErrorKind::Cancelled, "original stop observed")
        .into_original_failure();
    let error = MvRepositoryError::new(MvRepositoryErrorKind::CommitUnknown, "kept original error")
        .with_original_failure(cause.clone());
    let carrier = novarocks_failure_contract::SharedFailureCause::new(error);
    assert_eq!(
        carrier.failure_fact(),
        cause.failure_fact(),
        "no local kind overwrites originating/control/publication facts"
    );
    assert_eq!(
        carrier
            .downcast_ref::<MvRepositoryError>()
            .unwrap()
            .original_failure(),
        Some(&cause)
    );
}
