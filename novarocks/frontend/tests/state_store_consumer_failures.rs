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

//! What the three StateStore consumers do when the store fails them.
//!
//! Catalog attachment, the MV Accelerator and the GC observation accelerator
//! each translate one shared runner outcome into their own vocabulary. The
//! translation is where the dangerous mistakes live: calling an unknown commit
//! an absent one, retrying a write the store proved dead, or treating a
//! transient admission ceiling as a permanent refusal. Every case below fixes
//! one of those, and says in a comment what breaks if the assertion stops
//! holding.
//!
//! # How the faults are made
//!
//! [`ScriptedStateStore`] decorates the shared in-memory reference store. It
//! never invents an [`AttemptSupervisor`]: attempts are issued by the store
//! that will run them, so saturation here is the real capacity path rather
//! than a fabricated error. Only the commit verdict, an optional stall before
//! `begin_write`, and an optional operation failure are scripted.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_frontend::StateStoreRunPolicy;
use novarocks_frontend::catalog_attachment::{
    CatalogAttachment, CatalogAttachmentErrorKind, CatalogAttachmentRepository,
};
use novarocks_frontend::common::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_frontend::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_frontend::mv::domain::persistence::definition::{
    CreateMvDefinitionRequest, MvAcceleratorSourceRevision, MvDesiredRefreshPolicy,
};
use novarocks_frontend::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use novarocks_frontend::mv::domain::repository::{
    InitialMvRefreshConfiguration, MvProjectionRequest, MvPublishedProjection,
    MvPublishedWaterline, MvRepository, MvRepositoryErrorKind, MvTarget,
};
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_frontend::table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
    GcOwnedRefObservationErrorKind,
};
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, ConnectorInstanceId,
    ConnectorProviderId, ConnectorTableObjectId, CredentialConsumerRole, StaticCredentialReference,
};
use novarocks_state_store_api::{
    AttemptSupervisor, CommitOutcome, Direction, Key, KeyRange, Precondition, RangePage,
    RangeRequest, ReadTransaction, StateRecord, StateStore, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StoreIdentity, Value, WriteAttempt, WriteTransaction,
};
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Scripted store
// ---------------------------------------------------------------------------

/// What the next commit answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitScript {
    /// The real store decides.
    Passthrough,
    /// The commit lost a race. Retryable, and proven not to have landed.
    Conflict,
    /// The store proves this write can never land.
    DefiniteFailure,
    /// The commit was dispatched and the response was lost, and the store can
    /// prove nothing afterwards. This is the genuinely ambiguous case: the
    /// attempt is left dispatched-but-unsettled with no evidence, so
    /// adjudication answers `Unresolved`.
    UnknownAndUnresolvable,
    /// The commit response was lost, but the attempt provably never reached
    /// storage, so adjudication answers `NotCommitted`.
    UnknownButProvenUncommitted,
}

/// Everything the store and its transactions share.
struct ScriptedState {
    commit: Mutex<CommitScript>,
    stall_before_begin_write: Mutex<Option<Duration>>,
    operation_error: Mutex<Option<StateStoreError>>,
    begin_write_calls: AtomicUsize,
    commit_calls: AtomicUsize,
}

/// Decorates the reference store, scripting only what a provider cannot be
/// asked to produce on demand.
struct ScriptedStateStore {
    inner: Arc<InMemoryStateStore>,
    state: Arc<ScriptedState>,
}

impl ScriptedStateStore {
    fn with_attempt_capacity(cluster_id: &str, outstanding_attempts: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemoryStateStore::with_limits_and_capacity(
                cluster_id,
                StateStoreLimits::default(),
                NonZeroUsize::new(outstanding_attempts).expect("positive attempt capacity"),
            )),
            state: Arc::new(ScriptedState {
                commit: Mutex::new(CommitScript::Passthrough),
                stall_before_begin_write: Mutex::new(None),
                operation_error: Mutex::new(None),
                begin_write_calls: AtomicUsize::new(0),
                commit_calls: AtomicUsize::new(0),
            }),
        })
    }

    fn new(cluster_id: &str) -> Arc<Self> {
        Self::with_attempt_capacity(cluster_id, 16)
    }

    /// Arms a sticky commit verdict: every commit from now on answers it.
    fn script_commits(&self, script: CommitScript) {
        *self.state.commit.lock().expect("commit script") = script;
    }

    /// Makes `begin_write` hang, so the operation budget expires before
    /// anything is dispatched.
    fn stall_begin_write(&self, stall: Duration) {
        *self
            .state
            .stall_before_begin_write
            .lock()
            .expect("begin-write stall") = Some(stall);
    }

    /// Fails every staged mutation with one error.
    fn fail_operations(&self, error: StateStoreError) {
        *self.state.operation_error.lock().expect("operation error") = Some(error);
    }

    fn begin_write_calls(&self) -> usize {
        self.state.begin_write_calls.load(Ordering::Relaxed)
    }

    fn commit_calls(&self) -> usize {
        self.state.commit_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl StateStore for ScriptedStateStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn attempts(&self) -> &AttemptSupervisor {
        // Deliberately the wrapped instance's own supervisor. A second
        // supervisor would mint identities this store never issued, and the
        // saturation case below would stop being the real capacity path.
        self.inner.attempts()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.state.begin_write_calls.fetch_add(1, Ordering::Relaxed);
        let stall = *self
            .state
            .stall_before_begin_write
            .lock()
            .expect("begin-write stall");
        if let Some(stall) = stall {
            // The caller's `timeout_at` fires here and drops this future, so
            // the wrapped store is never asked to begin anything.
            tokio::time::sleep(stall).await;
        }
        let script = *self.state.commit.lock().expect("commit script");
        if script == CommitScript::UnknownAndUnresolvable {
            // The wrapped store is never told about this attempt, so it holds
            // no evidence for it, which is exactly what "the store cannot
            // prove anything" means. Reads still come from a real snapshot.
            return Ok(Box::new(ScriptedWriteTransaction {
                state: Arc::clone(&self.state),
                body: Body::Detached {
                    attempt,
                    reads: self.inner.begin_read().await?,
                },
            }));
        }
        Ok(Box::new(ScriptedWriteTransaction {
            state: Arc::clone(&self.state),
            body: Body::Forwarded(self.inner.begin_write(attempt, purpose).await?),
        }))
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
}

enum Body {
    /// Everything runs against the wrapped store; only the verdict is scripted.
    Forwarded(Box<dyn WriteTransaction>),
    /// Nothing is applied and the wrapped store never learns the attempt
    /// exists. Reads are served from a real committed snapshot, which is
    /// enough for every consumer body here: all three read before they write
    /// and none reads back its own staged mutations.
    Detached {
        attempt: WriteAttempt,
        reads: Box<dyn ReadTransaction>,
    },
}

struct ScriptedWriteTransaction {
    state: Arc<ScriptedState>,
    body: Body,
}

#[async_trait]
impl ReadTransaction for ScriptedWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        match &mut self.body {
            Body::Forwarded(inner) => inner.get(key).await,
            Body::Detached { reads, .. } => reads.get(key).await,
        }
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        match &mut self.body {
            Body::Forwarded(inner) => inner.range(request).await,
            Body::Detached { reads, .. } => reads.range(request).await,
        }
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        match (*self).body {
            Body::Forwarded(inner) => inner.abort().await,
            Body::Detached { attempt, reads } => {
                attempt.cancel_before_dispatch()?;
                reads.abort().await
            }
        }
    }
}

#[async_trait]
impl WriteTransaction for ScriptedWriteTransaction {
    fn attempt(&self) -> novarocks_state_store_api::AttemptId {
        match &self.body {
            Body::Forwarded(inner) => inner.attempt(),
            Body::Detached { attempt, .. } => attempt.id(),
        }
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = self.state.operation_error.lock().expect("op error").clone() {
            return Err(error);
        }
        match &mut self.body {
            Body::Forwarded(inner) => inner.put(key, value, precondition).await,
            Body::Detached { .. } => Ok(()),
        }
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = self.state.operation_error.lock().expect("op error").clone() {
            return Err(error);
        }
        match &mut self.body {
            Body::Forwarded(inner) => inner.delete(key, precondition).await,
            Body::Detached { .. } => Ok(()),
        }
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let this = *self;
        this.state.commit_calls.fetch_add(1, Ordering::Relaxed);
        let script = *this.state.commit.lock().expect("commit script");
        match this.body {
            Body::Detached { attempt, reads } => {
                drop(reads);
                // A provider marks an attempt dispatched before anything could
                // leave a trace a later reader might see. Then the response is
                // lost: the attempt stays dispatched and unsettled, so the
                // supervisor has to adjudicate, and the store holds no
                // evidence to adjudicate from.
                if let Err(error) = attempt.mark_dispatched() {
                    return CommitOutcome::DefiniteFailure(error);
                }
                CommitOutcome::CommitUnknown(StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "scripted state store lost the commit response after dispatch",
                ))
            }
            Body::Forwarded(inner) => match script {
                CommitScript::Passthrough => inner.commit().await,
                CommitScript::Conflict => abort_then(inner, CommitOutcome::Conflict).await,
                CommitScript::DefiniteFailure => {
                    abort_then(inner, CommitOutcome::DefiniteFailure).await
                }
                CommitScript::UnknownButProvenUncommitted => {
                    abort_then(inner, CommitOutcome::CommitUnknown).await
                }
                CommitScript::UnknownAndUnresolvable => {
                    unreachable!("an unresolvable commit never forwards a transaction")
                }
            },
        }
    }
}

/// Rolls the wrapped transaction back, then answers the scripted verdict.
///
/// Aborting first is what makes `Conflict` and `UnknownButProvenUncommitted`
/// honest: the attempt is recorded as having no write effect, so the outcome a
/// consumer later observes is a real proof rather than an assumption.
async fn abort_then(
    inner: Box<dyn WriteTransaction>,
    outcome: fn(StateStoreError) -> CommitOutcome,
) -> CommitOutcome {
    match inner.abort().await {
        Ok(()) => outcome(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "scripted state store commit fault",
        )),
        Err(error) => CommitOutcome::DefiniteFailure(error),
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Tight enough that a budget case finishes fast, loose enough that a retry
/// case still gets its second attempt.
fn quick_policy(operation_timeout: Duration) -> StateStoreRunPolicy {
    StateStoreRunPolicy::new(2, operation_timeout).expect("policy inside the built-in ceilings")
}

fn attachment() -> CatalogAttachment {
    CatalogAttachment {
        attachment_id: Uuid::now_v7(),
        instance_id: ConnectorInstanceId::parse("Warehouse.Main").expect("instance"),
        provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
        display_name: "Warehouse.Main".to_string(),
        durable_properties: vec![("type".to_string(), "iceberg".to_string())],
        credential_bindings: vec![
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Static(
                    StaticCredentialReference::try_new("warehouse-data", "blue")
                        .expect("credential reference"),
                ),
            )
            .expect("credential binding"),
        ],
        created_at_ms: 1,
    }
}

fn projection_request() -> MvProjectionRequest {
    MvProjectionRequest {
        definition: CreateMvDefinitionRequest {
            query_definition: PersistedQueryDefinition::new(
                "SELECT * FROM ice.sales.orders",
                PersistedQueryDialect::StarRocks,
                "ice",
                "sales",
            )
            .expect("query definition"),
            base_table_refs: vec!["ice.sales.orders".to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("sales".to_string()),
            target_table: Some("orders_mv".to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration {
            policy: MvDesiredRefreshPolicy::Manual,
            ..Default::default()
        },
        publication: MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 10,
            last_refresh_rows: 20,
            last_refreshed_iceberg_snapshot_id: 9,
            base_snapshots: [("ice.sales.orders".to_string(), 7)].into_iter().collect(),
            base_table_object_ids: [("ice.sales.orders".to_string(), object_id(b"base-orders"))]
                .into_iter()
                .collect(),
        }),
        source_revision: MvAcceleratorSourceRevision {
            target_object_id: object_id(b"orders-mv-object"),
            descriptor_content_hash: "descriptor-orders-mv".to_string(),
            current_target_snapshot_id: Some(9),
        },
        dependencies: vec![CreateMvDependencyRequest {
            upstream: MvDependencyObjectRef {
                catalog: Some("ice".to_string()),
                database_or_namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: MvDependencyObjectType::Table,
                storage_engine: MvDependencyStorageEngine::Iceberg,
            },
            created_at_ms: 1,
        }],
    }
}

fn object_id(bytes: &[u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::copy_from_slice(bytes)).expect("bounded object ID")
}

fn observation() -> GcOwnedRefObservation {
    GcOwnedRefObservation::try_new(
        Uuid::from_u128(0x3c1),
        "__novarocks_gc_candidate".to_string(),
        101,
        1,
        [9; 32],
        1,
    )
    .expect("valid owned-ref observation")
}

fn mv_target() -> MvTarget {
    MvTarget {
        catalog: Some("ice".to_string()),
        database: "sales".to_string(),
        name: "orders_mv".to_string(),
    }
}

/// Every durable record any of the three families could have written.
///
/// A failing write that still leaves a record behind is the worst outcome
/// available, so the budget and definite-failure cases assert on this rather
/// than only on the reported error.
async fn frontend_record_count(store: &dyn StateStore) -> usize {
    let prefix = Key::try_from(Bytes::from_static(b"novarocks/frontend/")).expect("prefix key");
    let range = KeyRange::for_prefix(prefix).expect("prefix range");
    let mut transaction = store.begin_read().await.expect("begin read");
    let mut request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: store.limits().max_page_size,
        continuation: None,
    };
    let mut total = 0;
    loop {
        let page = transaction.range(&request).await.expect("range page");
        total += page.records.len();
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    transaction.abort().await.expect("abort read");
    total
}

// ---------------------------------------------------------------------------
// 1. Admission saturation is transient: it costs no attempt and is waited out
// ---------------------------------------------------------------------------

/// If saturation were reported as a failure, a `CREATE EXTERNAL CATALOG` would
/// fail whenever another frontend operation happened to hold the instance's
/// last attempt slot -- a resource bound turned into a user-visible error.
#[tokio::test]
async fn catalog_attachment_waits_out_admission_saturation_and_creates_exactly_once() {
    let store = ScriptedStateStore::with_attempt_capacity("catalog-saturation", 1);
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");

    // Occupy the instance's only attempt. Reserving touches no storage, which
    // is exactly why the runner is allowed to keep trying. Both handles have to
    // be held: the slot is charged until the attempt *and* every observation of
    // it are gone.
    let held = store.attempts().reserve().expect("hold the only attempt");
    assert_eq!(store.attempts().outstanding(), store.attempts().capacity());

    let requested = attachment();
    let mut pending = Box::pin(repository.create(requested.clone()));
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut pending)
            .await
            .is_err(),
        "a saturated instance must leave the create waiting, not answer it"
    );
    assert_eq!(
        store.begin_write_calls(),
        0,
        "no transaction may begin while the attempt ceiling is held"
    );

    drop(held);
    let created = pending
        .await
        .expect("saturation is transient, not a failure");

    assert_eq!(created.attachment.attachment_id, requested.attachment_id);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "waiting out saturation must not spend an attempt: exactly one write body ran"
    );
}

/// Same property for the MV Accelerator: a busy instance must not turn
/// `CREATE MATERIALIZED VIEW` into a failure the user has to re-issue.
#[tokio::test]
async fn mv_accelerator_waits_out_admission_saturation_and_creates_exactly_once() {
    let store = ScriptedStateStore::with_attempt_capacity("mv-saturation", 1);
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository =
        StateStoreMvRepository::open(Arc::clone(&handle), quick_policy(Duration::from_secs(3)))
            .await
            .expect("open MV Accelerator repository");

    let held = store.attempts().reserve().expect("hold the only attempt");

    let mut pending = Box::pin(repository.create_projection(Uuid::now_v7(), projection_request()));
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut pending)
            .await
            .is_err(),
        "a saturated instance must leave the projection create waiting"
    );
    assert_eq!(store.begin_write_calls(), 0);

    drop(held);
    let created = pending
        .await
        .expect("saturation is transient, not a failure");

    assert_eq!(created.definition.mv_id, 1);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "waiting out saturation must not spend an attempt"
    );
}

/// Same property for GC. Here a spurious failure is worse than an error
/// message: GC is fail-closed, so a refused observation stalls reclamation for
/// as long as the instance stays busy.
#[tokio::test]
async fn gc_observation_waits_out_admission_saturation_and_records_exactly_once() {
    let store = ScriptedStateStore::with_attempt_capacity("gc-saturation", 1);
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open GC observation accelerator");

    let held = store.attempts().reserve().expect("hold the only attempt");

    let mut pending = Box::pin(accelerator.observe(observation(), 1_000, 500));
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut pending)
            .await
            .is_err(),
        "a saturated instance must leave the observation waiting"
    );
    assert_eq!(store.begin_write_calls(), 0);

    drop(held);
    let decision = pending
        .await
        .expect("saturation is transient, not a failure");

    assert_eq!(
        decision,
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 1_000
        }
    );
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "waiting out saturation must not spend an attempt"
    );
}

/// The other half of the distinction: `LimitExceeded` is a permanent property
/// of the request. Waiting it out the way saturation is waited out would burn
/// the whole operation budget on a request that can never succeed.
#[tokio::test]
async fn a_permanent_limit_breach_is_answered_at_once_rather_than_waited_out() {
    let store = ScriptedStateStore::new("gc-limit-exceeded");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open GC observation accelerator");
    store.fail_operations(StateStoreError::new(
        StateStoreErrorKind::LimitExceeded,
        "record is outside a fixed storage limit",
    ));

    let error = accelerator
        .observe(observation(), 1_000, 500)
        .await
        .expect_err("a permanent limit breach must fail the operation");

    assert_eq!(error.kind(), GcOwnedRefObservationErrorKind::Store);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 0),
        "a permanent breach is not retried and never reaches commit"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);
}

// ---------------------------------------------------------------------------
// 2. An unknown commit is never downgraded to "absent"
// ---------------------------------------------------------------------------

/// Reporting an unknown commit as absent would let the caller create the same
/// attachment a second time, so one catalog instance id could end up with two
/// attachment identities racing over the same key.
#[tokio::test]
async fn catalog_attachment_reports_an_unknown_commit_instead_of_calling_it_absent() {
    let store = ScriptedStateStore::new("catalog-commit-unknown");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");
    store.script_commits(CommitScript::UnknownAndUnresolvable);

    let error = repository
        .create(attachment())
        .await
        .expect_err("an unresolvable commit is not a success");

    assert_eq!(
        error.kind(),
        CatalogAttachmentErrorKind::CommitUnknown,
        "the record being absent is not proof that nothing committed"
    );
    // Nothing about the attempt is provable, so it is not replayed. Only a
    // proven `NotCommitted` licenses a replay; replaying an unresolved attempt
    // is how one create becomes two durable attachments.
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "an unresolvable attempt must not be replayed"
    );
    // The wakeup means "a write of ours landed". Publishing one here would
    // train the reconciler to act on a non-event.
    assert_eq!(repository.published_wakeups(), 0);
}

/// The MV Accelerator carries the in-doubt attempt in its own `CommitUnknown`
/// so an operator can resolve it. Calling it `NotFound` instead would invite
/// the caller to recreate an MV whose projection may already be durable.
#[tokio::test]
async fn mv_accelerator_reports_an_unknown_commit_instead_of_calling_it_absent() {
    let store = ScriptedStateStore::new("mv-commit-unknown");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository =
        StateStoreMvRepository::open(Arc::clone(&handle), quick_policy(Duration::from_secs(3)))
            .await
            .expect("open MV Accelerator repository");
    store.script_commits(CommitScript::UnknownAndUnresolvable);

    let error = repository
        .create_projection(Uuid::now_v7(), projection_request())
        .await
        .expect_err("an unresolvable commit is not a success");

    assert_eq!(error.kind(), MvRepositoryErrorKind::CommitUnknown);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "an unresolvable attempt must not be replayed"
    );
    // Absence of the projection is exactly what makes the honest answer hard:
    // the record is not there, and the repository still refuses to call the
    // write "not done".
    assert!(
        repository
            .find_by_target(&mv_target())
            .await
            .expect("target lookup")
            .is_none()
    );
}

/// GC is fail-closed: an unknown observation write must not be reported as a
/// plain store failure, because the caller's recovery for the two differs --
/// an unknown write may already have started somebody's safety clock.
#[tokio::test]
async fn gc_observation_reports_an_unknown_commit_instead_of_calling_it_absent() {
    let store = ScriptedStateStore::new("gc-commit-unknown");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open GC observation accelerator");
    store.script_commits(CommitScript::UnknownAndUnresolvable);

    let error = accelerator
        .observe(observation(), 1_000, 500)
        .await
        .expect_err("an unresolvable commit is not a decision");

    assert_eq!(error.kind(), GcOwnedRefObservationErrorKind::CommitUnknown);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "an unresolvable attempt must not be replayed"
    );
}

// ---------------------------------------------------------------------------
// 3. A commit the store proved dead is never tried again
// ---------------------------------------------------------------------------

/// `DefiniteFailure` is a proof, not a hint. Retrying it spends the operation
/// budget on work that provably cannot succeed and delays the honest error the
/// caller needs.
#[tokio::test]
async fn catalog_attachment_never_retries_a_commit_the_store_proved_dead() {
    let store = ScriptedStateStore::new("catalog-definite-failure");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");
    store.script_commits(CommitScript::DefiniteFailure);

    let error = repository
        .create(attachment())
        .await
        .expect_err("a proven-dead commit is not a success");

    assert_eq!(error.kind(), CatalogAttachmentErrorKind::Unavailable);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "a proven-dead commit is asked exactly once"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);
}

#[tokio::test]
async fn mv_accelerator_never_retries_a_commit_the_store_proved_dead() {
    let store = ScriptedStateStore::new("mv-definite-failure");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository =
        StateStoreMvRepository::open(Arc::clone(&handle), quick_policy(Duration::from_secs(3)))
            .await
            .expect("open MV Accelerator repository");
    store.script_commits(CommitScript::DefiniteFailure);

    let error = repository
        .create_projection(Uuid::now_v7(), projection_request())
        .await
        .expect_err("a proven-dead commit is not a success");

    assert_eq!(error.kind(), MvRepositoryErrorKind::Unavailable);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "a proven-dead commit is asked exactly once"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);
}

#[tokio::test]
async fn gc_observation_never_retries_a_commit_the_store_proved_dead() {
    let store = ScriptedStateStore::new("gc-definite-failure");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open GC observation accelerator");
    store.script_commits(CommitScript::DefiniteFailure);

    let error = accelerator
        .observe(observation(), 1_000, 500)
        .await
        .expect_err("a proven-dead commit is not a decision");

    assert_eq!(error.kind(), GcOwnedRefObservationErrorKind::Store);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (1, 1),
        "a proven-dead commit is asked exactly once"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);
}

// ---------------------------------------------------------------------------
// 4. A spent budget is the consumer's own failure, with no second write effect
// ---------------------------------------------------------------------------

/// Two ways to run out: every permitted attempt spent on conflicts, and the
/// wall-clock budget expiring before anything is dispatched. Both must end in
/// the consumer's own vocabulary, and neither may leave a record behind -- a
/// half-applied attachment is a catalog that exists for the reconciler but not
/// for the caller who was told the operation failed.
#[tokio::test]
async fn catalog_attachment_reports_a_spent_budget_without_writing_twice() {
    let store = ScriptedStateStore::new("catalog-retry-exhausted");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");
    store.script_commits(CommitScript::Conflict);

    let error = repository
        .create(attachment())
        .await
        .expect_err("an exhausted retry budget is not a success");
    assert_eq!(error.kind(), CatalogAttachmentErrorKind::Unavailable);
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (2, 2),
        "the policy permits two attempts and the runner takes exactly two"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);

    let stalled = ScriptedStateStore::new("catalog-deadline");
    let stalled_handle: Arc<dyn StateStore> = Arc::clone(&stalled) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&stalled_handle),
        quick_policy(Duration::from_millis(200)),
    )
    .await
    .expect("open catalog attachment repository");
    stalled.stall_begin_write(Duration::from_secs(30));

    let error = repository
        .create(attachment())
        .await
        .expect_err("an expired budget is not a success");
    assert_eq!(error.kind(), CatalogAttachmentErrorKind::Unavailable);
    assert_eq!(
        (stalled.begin_write_calls(), stalled.commit_calls()),
        (1, 0),
        "the budget expired before anything was dispatched"
    );
    assert_eq!(frontend_record_count(stalled_handle.as_ref()).await, 0);
}

#[tokio::test]
async fn mv_accelerator_reports_a_spent_budget_without_writing_twice() {
    let store = ScriptedStateStore::new("mv-retry-exhausted");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository =
        StateStoreMvRepository::open(Arc::clone(&handle), quick_policy(Duration::from_secs(3)))
            .await
            .expect("open MV Accelerator repository");
    store.script_commits(CommitScript::Conflict);

    let error = repository
        .create_projection(Uuid::now_v7(), projection_request())
        .await
        .expect_err("an exhausted retry budget is not a success");
    // Every attempt lost a race, so the MV Accelerator reports contention.
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert_eq!((store.begin_write_calls(), store.commit_calls()), (2, 2));
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);

    let stalled = ScriptedStateStore::new("mv-deadline");
    let stalled_handle: Arc<dyn StateStore> = Arc::clone(&stalled) as Arc<dyn StateStore>;
    let repository = StateStoreMvRepository::open(
        Arc::clone(&stalled_handle),
        quick_policy(Duration::from_millis(200)),
    )
    .await
    .expect("open MV Accelerator repository");
    stalled.stall_begin_write(Duration::from_secs(30));

    let error = repository
        .create_projection(Uuid::now_v7(), projection_request())
        .await
        .expect_err("an expired budget is not a success");
    // Nothing was dispatched, so this is provably clean and must not be
    // reported as unknown: unknown is the one answer that forbids the caller
    // from simply trying again, and it sends an operator looking for an
    // in-doubt write that cannot exist.
    assert_eq!(error.kind(), MvRepositoryErrorKind::Unavailable);
    assert_eq!(
        (stalled.begin_write_calls(), stalled.commit_calls()),
        (1, 0)
    );
    assert_eq!(frontend_record_count(stalled_handle.as_ref()).await, 0);
}

#[tokio::test]
async fn gc_observation_reports_a_spent_budget_without_writing_twice() {
    let store = ScriptedStateStore::new("gc-retry-exhausted");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open GC observation accelerator");
    store.script_commits(CommitScript::Conflict);

    let error = accelerator
        .observe(observation(), 1_000, 500)
        .await
        .expect_err("an exhausted retry budget is not a decision");
    assert_eq!(error.kind(), GcOwnedRefObservationErrorKind::Store);
    assert_eq!((store.begin_write_calls(), store.commit_calls()), (2, 2));
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);

    let stalled = ScriptedStateStore::new("gc-deadline");
    let stalled_handle: Arc<dyn StateStore> = Arc::clone(&stalled) as Arc<dyn StateStore>;
    let accelerator = GcOwnedRefObservationAccelerator::open(
        Arc::clone(&stalled_handle),
        quick_policy(Duration::from_millis(200)),
    )
    .await
    .expect("open GC observation accelerator");
    stalled.stall_begin_write(Duration::from_secs(30));

    let error = accelerator
        .observe(observation(), 1_000, 500)
        .await
        .expect_err("an expired budget is not a decision");
    // A runner deadline is only produced before dispatch, so a plain store
    // failure is the precise answer here.
    assert_eq!(error.kind(), GcOwnedRefObservationErrorKind::Store);
    assert_eq!(
        (stalled.begin_write_calls(), stalled.commit_calls()),
        (1, 0)
    );
    assert_eq!(frontend_record_count(stalled_handle.as_ref()).await, 0);
}

// ---------------------------------------------------------------------------
// 5. Proven-uncommitted replay is bounded, and reported as what it is
// ---------------------------------------------------------------------------

/// A store that keeps losing commit responses while proving each attempt never
/// landed must not spin forever, and must not be described as "unknown": the
/// attempts are proven uncommitted, so `CommitUnknown` would send an operator
/// hunting for an in-doubt write that does not exist.
#[tokio::test]
async fn catalog_attachment_stops_replaying_a_proven_uncommitted_create() {
    let store = ScriptedStateStore::new("catalog-proven-uncommitted-create");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");
    store.script_commits(CommitScript::UnknownButProvenUncommitted);

    let error = repository
        .create(attachment())
        .await
        .expect_err("an endlessly replayed create must end in a failure");

    assert_eq!(
        error.kind(),
        CatalogAttachmentErrorKind::Unavailable,
        "a proven-uncommitted replay is an availability answer, not an unknown one"
    );
    // MAX_PROVEN_UNCOMMITTED_REPLAYS is 2: the first run plus two replays.
    assert_eq!(
        (store.begin_write_calls(), store.commit_calls()),
        (3, 3),
        "the replay loop is bounded by the repository, not by the runner policy"
    );
    assert_eq!(frontend_record_count(handle.as_ref()).await, 0);
}

/// The drop path owns the same bound. Losing it there is worse than on create:
/// an unbounded replay holds the caller's connection while the store is
/// already known to be refusing every commit.
#[tokio::test]
async fn catalog_attachment_stops_replaying_a_proven_uncommitted_drop() {
    let store = ScriptedStateStore::new("catalog-proven-uncommitted-drop");
    let handle: Arc<dyn StateStore> = Arc::clone(&store) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(
        Arc::clone(&handle),
        quick_policy(Duration::from_secs(3)),
    )
    .await
    .expect("open catalog attachment repository");
    let created = repository.create(attachment()).await.expect("create");
    let baseline = store.commit_calls();
    store.script_commits(CommitScript::UnknownButProvenUncommitted);

    let error = repository
        .drop_exact(created.clone())
        .await
        .expect_err("an endlessly replayed drop must end in a failure");

    assert_eq!(error.kind(), CatalogAttachmentErrorKind::Unavailable);
    assert_eq!(
        store.commit_calls() - baseline,
        3,
        "the drop replay is bounded by the same constant as the create replay"
    );
    // The attachment is still exactly the one that was there before, which is
    // what "proven not committed" has to mean for the caller.
    assert_eq!(
        repository
            .get(&created.attachment.instance_id)
            .await
            .expect("read attachment")
            .expect("attachment remains")
            .attachment,
        created.attachment
    );
}
