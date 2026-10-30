// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Pure original API adapter around the existing memory CAS/snapshot author.
// These tests cover readiness routing/custody, not physical SQLite execution.
use super::*;
use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
use novarocks_state_store_api::*;
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Weak};

struct NoPhysicalPort;
impl StateStoreExecutionPort for NoPhysicalPort {
    fn bind_original_in(
        &self,
        _: &Arc<dyn StateStoreBlockingOriginal>,
        _: Instant,
        _: &mut Option<Box<dyn StateStoreBlockingSlot>>,
    ) -> Result<(), StateStoreExecutionFailure> {
        Err(StateStoreOriginalUnsupported.into_original_failure())
    }
    fn request_progress(&self) {}
}
fn execution() -> (Arc<dyn StateStoreExecutionPort>, StateStoreExecutionContext) {
    let port: Arc<dyn StateStoreExecutionPort> = Arc::new(NoPhysicalPort);
    let context = StateStoreExecutionContext::new(&port, Instant::now() + Duration::from_secs(30));
    (port, context)
}
#[derive(Default)]
struct Trace {
    legacy: AtomicUsize,
    reads: AtomicUsize,
    closes: AtomicUsize,
    writes: AtomicUsize,
    write_ranges: AtomicUsize,
    last_attempt: Mutex<Option<AttemptId>>,
    commit_failure: Mutex<Option<SharedFailureCause>>,
    ranges: Mutex<Vec<RangeRequest>>,
}
struct OriginalStore {
    inner: Arc<InMemoryStateStore>,
    context: StateStoreExecutionContext,
    owner: Weak<dyn StateStoreExecutionPort>,
    trace: Arc<Trace>,
}
impl OriginalStore {
    fn check(&self, execution: &StateStoreExecutionContext) -> Result<(), SharedFailureCause> {
        assert!(execution.shares_owner(&self.context));
        assert!(execution.deadline() <= self.context.deadline());
        if self.owner.upgrade().is_none() {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "original test owner is gone",
            )
            .into_original_failure());
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl StateStore for OriginalStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.trace.legacy.fetch_add(1, Ordering::SeqCst);
        panic!("original readiness fell back to legacy read")
    }
    async fn begin_write(
        &self,
        _: WriteAttempt,
        _: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.trace.legacy.fetch_add(1, Ordering::SeqCst);
        panic!("original readiness fell back to legacy write")
    }
    async fn begin_original_read_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalReadTransaction>, StateStoreExecutionFailure> {
        self.check(execution)?;
        assert_eq!(execution.deadline(), self.context.deadline());
        self.trace.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(OriginalRead {
            inner: Some(
                self.inner
                    .begin_read()
                    .await
                    .map_err(StateStoreError::into_original_failure)?,
            ),
            trace: self.trace.clone(),
        }))
    }
    async fn begin_original_write_in(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalWriteTransaction>, StateStoreExecutionFailure> {
        self.check(execution)?;
        self.trace.writes.fetch_add(1, Ordering::SeqCst);
        let inner = self
            .inner
            .begin_write(attempt, purpose)
            .await
            .map_err(StateStoreError::into_original_failure)?;
        let attempt = inner.attempt();
        *self.trace.last_attempt.lock().unwrap() = Some(attempt);
        Ok(Box::new(OriginalWrite {
            inner: Some(inner),
            attempt,
            trace: self.trace.clone(),
            outcome: None,
            failure: None,
        }))
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
struct OriginalWrite {
    inner: Option<Box<dyn WriteTransaction>>,
    attempt: AttemptId,
    trace: Arc<Trace>,
    outcome: Option<CommitOutcome>,
    failure: Option<SharedFailureCause>,
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
        self.trace.write_ranges.fetch_add(1, Ordering::SeqCst);
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
impl OriginalWriteTransaction for OriginalWrite {
    fn attempt(&self) -> AttemptId {
        self.attempt
    }
    fn original_failure(&self) -> Option<StateStoreExecutionFailure> {
        self.failure.clone()
    }
    async fn put_in(
        &mut self,
        key: Key,
        value: Value,
        condition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .put(key, value, condition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn delete_in(
        &mut self,
        key: Key,
        condition: Precondition,
    ) -> Result<(), StateStoreExecutionFailure> {
        self.inner
            .as_mut()
            .unwrap()
            .delete(key, condition)
            .await
            .map_err(StateStoreError::into_original_failure)
    }
    async fn commit_in(&mut self) -> Result<CommitOutcome, StateStoreExecutionFailure> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if let Some(outcome) = &self.outcome {
            return Ok(outcome.clone());
        }
        let outcome = self.inner.take().unwrap().commit().await;
        self.outcome = Some(outcome.clone());
        let failure = self.trace.commit_failure.lock().unwrap().take();
        if let Some(failure) = failure {
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
            self.failure = Some(failure.clone());
            return Err(failure);
        }
        Ok(outcome)
    }
}
async fn original_service(
    owner: &Arc<dyn StateStoreExecutionPort>,
    context: &StateStoreExecutionContext,
) -> (MvKeyedReadinessService, Arc<Trace>) {
    let inner = Arc::new(InMemoryStateStore::with_limits(
        format!("readiness-original-{}", Uuid::now_v7()),
        StateStoreLimits {
            max_page_size: 1,
            ..StateStoreLimits::default()
        },
    ));
    let trace = Arc::new(Trace::default());
    let repository = StateStoreMvRepository::open(
        Arc::new(OriginalStore {
            inner,
            context: context.clone(),
            owner: Arc::downgrade(owner),
            trace: trace.clone(),
        }),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .await
    .unwrap();
    (
        MvKeyedReadinessService::new(repository, Arc::default()),
        trace,
    )
}
fn bound() -> crate::repository::MvProjectionInventoryBound {
    crate::repository::MvProjectionInventoryBound {
        entries: 4,
        snapshot_bytes: 1024 * 1024,
        raw_page_bytes: 1024 * 1024,
        single_name_bytes: 65_536,
        continuation_token_bytes: 4096,
        decode: PersistenceDecodeBudget::default(),
    }
}

#[tokio::test]
async fn readiness_original_view_keeps_same_context_through_install_candidates_inventory_and_delete()
 {
    let (owner, context) = execution();
    let (service, trace) = original_service(&owner, &context).await;
    let view = service.in_execution(&context);
    assert_eq!(Arc::strong_count(&owner), 1);
    assert!(matches!(
        view.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
            .await
            .unwrap(),
        MvKeyedProjectionInstallOutcome::Installed(_)
    ));
    let loaded = view.load_ready_settled(&target()).await.unwrap().unwrap();
    let candidates = view.candidate_reader();
    let mut inventory = view.bounded_projection_inventory(bound()).await.unwrap();
    let mut listable = view.bounded_projection_inventory(bound()).await.unwrap();
    let dependencies = view
        .list_ready_dependencies_by_downstream_bounded(
            &loaded,
            crate::repository::MvDependencyReadBound {
                inventory: bound(),
                entries: 32,
                collection_bytes: 1024 * 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        dependencies,
        view.list_ready_dependencies_by_downstream(&loaded)
            .await
            .unwrap()
    );
    assert!(!dependencies.is_empty());
    assert!(matches!(
        view.query_admission(&target()).await.unwrap(),
        MvQueryAdmission::Admitted
    ));
    assert_eq!(
        view.list_ready_projections().await.unwrap(),
        vec![loaded.clone()]
    );
    assert!(matches!(
        view.observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "observed read only".into()
        )
        .await
        .unwrap(),
        MvKeyedProjectionInstallOutcome::Unchanged(_)
    ));
    let deletion_view = view.clone();
    drop(view);
    drop(context);
    // Both returned objects preserve the original weak request selection.
    assert_eq!(
        candidates.list_candidate_definitions().await.unwrap(),
        vec![loaded.projection.clone()]
    );
    inventory.order_by_namespace_and_name();
    assert_eq!(inventory.next_ready().await.unwrap(), Some(loaded.clone()));
    assert!(inventory.next_ready().await.unwrap().is_none());
    let listed = listable.next_listable().await.unwrap().unwrap();
    assert_eq!(listed.loaded, loaded);
    assert_eq!(listed.manageability, MvListedManageability::Manageable);
    let guard = deletion_view
        .reserve_projection_delete(target())
        .await
        .unwrap();
    drop(deletion_view);
    // Legacy completion entry must consume the guard's original request, not
    // switch a previously reserved deletion to another repository path.
    assert!(matches!(
        service
            .delete_after_provider_drop(Uuid::now_v7(), guard)
            .await
            .unwrap(),
        MvKeyedProjectionInstallOutcome::Removed
    ));
    assert!(
        candidates
            .list_candidate_definitions()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace.reads.load(Ordering::SeqCst),
        trace.closes.load(Ordering::SeqCst)
    );
    assert_eq!(trace.writes.load(Ordering::SeqCst), 2);
    let ranges = trace.ranges.lock().unwrap();
    assert!(ranges.iter().all(|r| r.page_size == 1));
    assert!(ranges.iter().any(|r| r.continuation.is_some()));
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[tokio::test]
async fn readiness_original_slow_current_is_superseded_without_changing_request_mode() {
    let (owner, context) = execution();
    let (service, trace) = original_service(&owner, &context).await;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut slow = source(1);
    slow.entered = Some(entered.clone());
    slow.release = Some(release.clone());
    let first = service.in_execution(&context);
    let task = tokio::spawn(async move {
        first
            .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &slow)
            .await
    });
    entered.acquire().await.unwrap().forget();
    let next = service.in_execution(&context);
    next.observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &source(2))
        .await
        .unwrap();
    release.add_permits(1);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        MvKeyedProjectionInstallOutcome::Superseded
    ));
    let loaded = next.load_ready(&target()).await.unwrap().unwrap();
    assert_eq!(loaded.projection.facts.publication_snapshot(), Some(2));
    assert_eq!(trace.writes.load(Ordering::SeqCst), 1);
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn readiness_original_error_keeps_whole_repository_observation_and_raw_cause() {
    let (owner, context) = execution();
    let (service, trace) = original_service(&owner, &context).await;
    let cause = StateStoreError::new(
        StateStoreErrorKind::Internal,
        "injected lost original commit response",
    )
    .into_original_failure();
    *trace.commit_failure.lock().unwrap() = Some(cause.clone());
    let error = service
        .in_execution(&context)
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap_err();
    let carrier = error.original_failure().unwrap();
    assert_eq!(carrier.failure_fact(), cause.failure_fact());
    let repository = carrier.downcast_ref::<MvRepositoryError>().unwrap();
    assert_eq!(repository.kind(), MvRepositoryErrorKind::CommitUnknown);
    assert_eq!(repository.original_failure(), Some(&cause));
    let observation = repository.commit_observation().unwrap();
    assert_eq!(Some(observation.id()), *trace.last_attempt.lock().unwrap());
    assert!(matches!(
        observation.peek().unwrap(),
        Some(AttemptOutcome::Committed(_))
    ));
    assert_eq!(trace.writes.load(Ordering::SeqCst), 1);
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
    assert_eq!(
        service
            .in_execution(&context)
            .candidate_reader()
            .list_candidate_definitions()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn readiness_original_capability_missing_and_expired_guard_never_fall_back() {
    let (owner, context) = execution();
    let (_, legacy) = service().await;
    let refusal = legacy
        .in_execution(&context)
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap_err();
    let retained = refusal
        .original_failure()
        .unwrap()
        .downcast_ref::<MvRepositoryError>()
        .unwrap();
    assert_eq!(
        retained.original_failure().unwrap().failure_fact(),
        StateStoreOriginalUnsupported.failure_fact()
    );
    assert!(
        legacy
            .candidate_reader()
            .list_candidate_definitions()
            .await
            .unwrap()
            .is_empty()
    );

    let (service, trace) = original_service(&owner, &context).await;
    let view = service.in_execution(&context);
    view.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let candidates = view.candidate_reader();
    let mut inventory = view.bounded_projection_inventory(bound()).await.unwrap();
    let guard = view.reserve_projection_delete(target()).await.unwrap();
    drop(view);
    drop(context);
    assert_eq!(Arc::strong_count(&owner), 1);
    drop(owner);
    let (_other_owner, other_context) = execution();
    let error = service
        .in_execution(&other_context)
        .delete_after_provider_drop(Uuid::now_v7(), guard)
        .await
        .unwrap_err();
    let retained = error
        .original_failure()
        .unwrap()
        .downcast_ref::<MvRepositoryError>()
        .unwrap();
    assert_eq!(
        retained
            .original_failure()
            .unwrap()
            .downcast_ref::<StateStoreError>()
            .unwrap()
            .kind(),
        StateStoreErrorKind::ProviderUnavailable
    );
    assert!(candidates.list_candidate_definitions().await.is_err());
    assert!(inventory.next_listable().await.is_err());
    assert_eq!(trace.writes.load(Ordering::SeqCst), 1);
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn keyed_replace_and_delete_use_root_owned_point_reads_when_ranges_require_continuation() {
    let (owner, context) = execution();
    let (service, trace) = original_service(&owner, &context).await;
    let view = service.in_execution(&context);
    view.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let first = view.load_ready(&target()).await.unwrap().unwrap();
    assert!(
        first
            .projection
            .facts
            .definition()
            .relation_occurrences
            .len()
            >= 2,
        "the real fixture must require continuation with its original page size one"
    );
    view.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(2))
        .await
        .unwrap();
    let second = view.load_ready(&target()).await.unwrap().unwrap();
    assert_eq!(second.projection.mv_id, first.projection.mv_id);
    assert_ne!(
        second.version, first.version,
        "replacement must perform the original guarded CAS"
    );
    let dependencies = view
        .list_ready_dependencies_by_downstream(&second)
        .await
        .unwrap();
    assert_eq!(
        dependencies.len(),
        second
            .projection
            .facts
            .definition()
            .relation_occurrences
            .len()
    );
    let guard = view.reserve_projection_delete(target()).await.unwrap();
    assert!(matches!(
        service
            .delete_after_provider_drop(Uuid::now_v7(), guard)
            .await
            .unwrap(),
        MvKeyedProjectionInstallOutcome::Removed
    ));
    assert!(
        view.candidate_reader()
            .list_candidate_definitions()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(trace.writes.load(Ordering::SeqCst), 3);
    assert_eq!(
        trace.write_ranges.load(Ordering::SeqCst),
        0,
        "mutation transactions must not request a paginated range before writes"
    );
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
    assert_eq!(
        trace.reads.load(Ordering::SeqCst),
        trace.closes.load(Ordering::SeqCst)
    );
    let ranges = trace.ranges.lock().unwrap();
    assert!(ranges.iter().all(|range| range.page_size == 1));
    assert!(ranges.iter().any(|range| range.continuation.is_some()));
}

struct RequestBoundaries {
    context: StateStoreExecutionContext,
    reads: AtomicUsize,
    writes: AtomicUsize,
}
impl StateStoreReadExecution for RequestBoundaries {
    fn read_transaction_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.context.clone())
    }
}
impl StateStoreExecutionRequests for RequestBoundaries {
    fn write_operation_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(self.context.clone())
    }
}
#[tokio::test]
async fn readiness_request_factory_is_sampled_per_transaction_not_per_inventory_or_page() {
    let (owner, context) = execution();
    let (service, trace) = original_service(&owner, &context).await;
    let requests = Arc::new(RequestBoundaries {
        context,
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
    });
    let view = service.in_requests(requests.clone());
    assert!(matches!(
        view.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
            .await
            .unwrap(),
        MvKeyedProjectionInstallOutcome::Installed(_)
    ));
    let mut inventory = view.bounded_projection_inventory(bound()).await.unwrap();
    let candidates = view.candidate_reader();
    let before_next = requests.reads.load(Ordering::SeqCst);
    assert!(inventory.next_ready().await.unwrap().is_some());
    assert!(
        requests.reads.load(Ordering::SeqCst) > before_next,
        "a later independent inventory read gets its own transaction context"
    );
    let before_candidates = requests.reads.load(Ordering::SeqCst);
    assert_eq!(
        candidates.list_candidate_definitions().await.unwrap().len(),
        1
    );
    assert!(requests.reads.load(Ordering::SeqCst) > before_candidates);
    assert_eq!(trace.legacy.load(Ordering::SeqCst), 0);
    assert_eq!(
        requests.reads.load(Ordering::SeqCst),
        trace.reads.load(Ordering::SeqCst),
        "one factory call per actual read begin; pages must not renew it"
    );
    assert_eq!(
        requests.writes.load(Ordering::SeqCst),
        trace.writes.load(Ordering::SeqCst)
    );
    assert_eq!(trace.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&owner),
        1,
        "request views hold no physical owner"
    );
}
