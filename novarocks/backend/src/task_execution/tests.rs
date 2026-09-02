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

//! The whole owner driven by an in-process caller: no RPC, no transport, and
//! no clock but the one the test moves.
//!
//! The fake host is a ledger. Every install, remove, materialize, release,
//! and submit is counted, which is what lets a rollback be asserted as a
//! balance rather than as an absence of a symptom.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use novarocks_execution::exec::fragment::program::{FragmentContractVersion, FragmentSinkKind};
use novarocks_execution::task_execution::descriptor::{
    ExchangeTopology, PhysicalFragmentPlan, TaskDescriptor,
};
use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    DomainVersion, PlanNodeId, SplitSequence,
};
use novarocks_execution::task_execution::identity::{
    QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution::task_execution::lease::{LeaseBounds, LeaseSequence, LeaseValidFor};
use novarocks_execution::task_execution::operation::{
    AbortQueryContext, AdvanceQueryContextDomain, CancelTask, CreateTask, CredentialUpdate,
    EstablishQueryContext, FetchTaskDynamicFilters, GetFinalTaskInfo, OperationOutcome,
    QueryContextDomainUpdate, ReleaseOutcome, ReleaseQueryContext, RenewQueryExecutionLease,
    SplitAssignmentIntent, TaskDomainUpdate, UpdateQueryContext, UpdateTask,
};
use novarocks_execution::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts,
    TaskResourceFacts, TaskState, TaskStatus, TaskStatusCursor, TaskStatusVersion,
};
use novarocks_execution::task_execution::transition::QueryContextState;
use novarocks_types::UniqueId;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};

use super::clock::ManualClock;
use super::host::{
    HostRejection, QueryContextHost, RunnableTask, SharedFactsRequest, TaskExecutionHost,
};
use super::observation::{CursorObservation, TaskStatusEvent, TaskStatusSource};
use super::receipt::OperationReceipt;
use super::registry::{TaskExecutionRegistry, TaskExecutionRegistryConfig};
use super::status::{METRIC_PUBLISH_MIN_INTERVAL, StatusAdvance, TaskStatusReporter};

// ------------------------------------------------------------------- fixtures

/// A stand-in for the codec-owned plan.
#[derive(Debug)]
struct FakePlan {
    fingerprint: u8,
}

impl FakePlan {
    fn arc(fingerprint: u8) -> Arc<dyn PhysicalFragmentPlan> {
        Arc::new(Self { fingerprint })
    }
}

impl CodecOwnedContent for FakePlan {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([self.fingerprint; 16])
    }

    fn encoded_len(&self) -> usize {
        1024
    }
}

impl PhysicalFragmentPlan for FakePlan {
    fn contract_version(&self) -> FragmentContractVersion {
        FragmentContractVersion::CURRENT
    }

    fn sink_kind(&self) -> FragmentSinkKind {
        FragmentSinkKind::Result
    }
}

#[derive(Debug)]
struct FakeContent {
    fingerprint: u8,
}

impl FakeContent {
    fn arc(fingerprint: u8) -> Arc<dyn CodecOwnedContent> {
        Arc::new(Self { fingerprint })
    }
}

impl CodecOwnedContent for FakeContent {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([self.fingerprint; 16])
    }

    fn encoded_len(&self) -> usize {
        64
    }
}

/// A stand-in for credential material. It has no `Debug` and no fingerprint,
/// exactly like the real thing.
struct FakeSecret {
    bytes: Vec<u8>,
}

impl FakeSecret {
    fn arc(seed: u8) -> Arc<dyn ConfidentialContent> {
        Arc::new(Self {
            bytes: vec![seed; 32],
        })
    }
}

impl ConfidentialContent for FakeSecret {
    fn encoded_len(&self) -> usize {
        self.bytes.len()
    }

    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        // The trait exposes no way to read another implementor's bytes, so
        // size is the whole comparison — exactly what the real codec's own
        // `ConfidentialContent` does.
        other.encoded_len() == self.bytes.len()
    }
}

#[derive(Debug, Default)]
struct HostLedger {
    receivers_installed: AtomicUsize,
    receivers_removed: AtomicUsize,
    capabilities_installed: AtomicUsize,
    capabilities_removed: AtomicUsize,
    submit_attempts: AtomicUsize,
    runnables_submitted: AtomicUsize,
    facts_materialized: AtomicUsize,
    facts_released: AtomicUsize,
    task_domains_applied: AtomicUsize,
    shared_domains_applied: AtomicUsize,
    aborts: AtomicUsize,
    cancels: AtomicUsize,
}

impl HostLedger {
    fn get(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

/// A gate the test opens, so a host call can be held inside the creation
/// transaction while the test observes the owner.
#[derive(Debug, Default)]
struct HostGate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl HostGate {
    fn opened() -> Self {
        Self {
            open: Mutex::new(true),
            changed: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut open = self.open.lock().expect("host gate");
        while !*open {
            open = self.changed.wait(open).expect("host gate");
        }
    }

    fn open(&self) {
        *self.open.lock().expect("host gate") = true;
        self.changed.notify_all();
    }

    fn close(&self) {
        *self.open.lock().expect("host gate") = false;
    }
}

struct FakeContextHost {
    ledger: Arc<HostLedger>,
    clock: Arc<ManualClock>,
    /// How far `materialize` moves the clock. This is how a slow catalog or
    /// credential load is expressed without a sleep.
    materialize_advance: Mutex<Duration>,
    fail_materialize: AtomicBool,
}

impl FakeContextHost {
    fn new(ledger: Arc<HostLedger>, clock: Arc<ManualClock>) -> Self {
        Self {
            ledger,
            clock,
            materialize_advance: Mutex::new(Duration::ZERO),
            fail_materialize: AtomicBool::new(false),
        }
    }
}

impl QueryContextHost for FakeContextHost {
    fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        let advance = *self
            .materialize_advance
            .lock()
            .expect("materialize advance");
        if !advance.is_zero() {
            self.clock.advance(advance);
        }
        if self.fail_materialize.load(Ordering::SeqCst) {
            return Err(HostRejection::new(
                TaskFailureCategory::Execution,
                "catalog binding could not be materialized",
            ));
        }
        self.ledger
            .facts_materialized
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, _context: QueryContextRef) {
        self.ledger.facts_released.fetch_add(1, Ordering::SeqCst);
    }

    fn advance_shared_domain(
        &self,
        _context: QueryContextRef,
        _domain: &QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        self.ledger
            .shared_domains_applied
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct FakeRunnable {
    reporter: TaskStatusReporter,
    ledger: Arc<HostLedger>,
    ignore_stand_down: Arc<AtomicBool>,
}

impl RunnableTask for FakeRunnable {
    fn cancel(&self, reason: CancelReason) {
        self.ledger.cancels.fetch_add(1, Ordering::SeqCst);
        if self.ignore_stand_down.load(Ordering::SeqCst) {
            return;
        }
        self.reporter.canceled(reason);
        self.reporter.release_output();
    }

    fn abort(&self, cause: AbortCause) {
        self.ledger.aborts.fetch_add(1, Ordering::SeqCst);
        if self.ignore_stand_down.load(Ordering::SeqCst) {
            return;
        }
        // A real host drives its own terminal here, including finishing a
        // task that was already converging on its own failure.
        if self.reporter.current().state().is_failure() {
            return;
        }
        self.reporter.aborted(cause);
        self.reporter.release_output();
    }
}

struct FakeTaskHost {
    ledger: Arc<HostLedger>,
    install_gate: Arc<HostGate>,
    fail_receiver: AtomicBool,
    fail_capability: AtomicBool,
    fail_submit: AtomicBool,
    fail_task_domain: AtomicBool,
    /// Held inside `submit_runnable`, after every install has succeeded.
    submit_gate: Arc<HostGate>,
    ignore_stand_down: Arc<AtomicBool>,
    arrivals: AtomicUsize,
    /// `install_receiver` waits until this many creates have arrived, which is
    /// how a concurrency test forces a real overlap.
    require_arrivals: AtomicUsize,
    reporters: Mutex<Vec<TaskStatusReporter>>,
}

impl FakeTaskHost {
    fn new(ledger: Arc<HostLedger>) -> Self {
        Self {
            ledger,
            install_gate: Arc::new(HostGate::opened()),
            fail_receiver: AtomicBool::new(false),
            fail_capability: AtomicBool::new(false),
            fail_submit: AtomicBool::new(false),
            fail_task_domain: AtomicBool::new(false),
            submit_gate: Arc::new(HostGate::opened()),
            ignore_stand_down: Arc::new(AtomicBool::new(false)),
            arrivals: AtomicUsize::new(0),
            require_arrivals: AtomicUsize::new(0),
            reporters: Mutex::new(Vec::new()),
        }
    }

    fn reporter(&self, identity: TaskIdentity) -> TaskStatusReporter {
        self.reporters
            .lock()
            .expect("reporters")
            .iter()
            .find(|reporter| reporter.identity() == identity)
            .expect("a submitted task has a reporter")
            .clone()
    }
}

impl TaskExecutionHost for FakeTaskHost {
    fn install_receiver(&self, _descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        let required = self.require_arrivals.load(Ordering::SeqCst);
        if required > 0 {
            while self.arrivals.load(Ordering::SeqCst) < required {
                std::thread::yield_now();
            }
        }
        self.install_gate.wait();
        if self.fail_receiver.load(Ordering::SeqCst) {
            return Err(HostRejection::new(
                TaskFailureCategory::Exchange,
                "receiver could not be installed",
            ));
        }
        self.ledger
            .receivers_installed
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {
        self.ledger.receivers_removed.fetch_add(1, Ordering::SeqCst);
    }

    fn install_inbound_capability(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        if self.fail_capability.load(Ordering::SeqCst) {
            return Err(HostRejection::new(
                TaskFailureCategory::Protocol,
                "inbound capability could not be installed",
            ));
        }
        self.ledger
            .capabilities_installed
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_inbound_capability(&self, _descriptor: &TaskDescriptor) {
        self.ledger
            .capabilities_removed
            .fetch_add(1, Ordering::SeqCst);
    }

    fn submit_runnable(
        &self,
        _descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        self.ledger.submit_attempts.fetch_add(1, Ordering::SeqCst);
        self.submit_gate.wait();
        if self.fail_submit.load(Ordering::SeqCst) {
            // Nothing is allocated and no worker exists: this is the only
            // step that could have started one.
            return Err(HostRejection::new(
                TaskFailureCategory::ResourceExhausted,
                "no driver capacity for this task",
            ));
        }
        self.reporters
            .lock()
            .expect("reporters")
            .push(reporter.clone());
        self.ledger
            .runnables_submitted
            .fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FakeRunnable {
            reporter,
            ledger: Arc::clone(&self.ledger),
            ignore_stand_down: Arc::clone(&self.ignore_stand_down),
        }))
    }

    fn apply_task_domain(
        &self,
        _descriptor: &TaskDescriptor,
        _domain: &TaskDomainUpdate,
    ) -> Result<(), HostRejection> {
        if self.fail_task_domain.load(Ordering::SeqCst) {
            return Err(HostRejection::new(
                TaskFailureCategory::ResourceExhausted,
                "split queue is full",
            ));
        }
        self.ledger
            .task_domains_applied
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// One assembled owner plus everything a test drives it with.
struct Fixture {
    registry: Arc<TaskExecutionRegistry>,
    clock: Arc<ManualClock>,
    ledger: Arc<HostLedger>,
    context_host: Arc<FakeContextHost>,
    task_host: Arc<FakeTaskHost>,
    backend: BackendProcessId,
    frontend: FrontendProcessId,
}

impl Fixture {
    fn new() -> Self {
        Self::with_config(|_| {})
    }

    fn with_config(adjust: impl FnOnce(&mut TaskExecutionRegistryConfig)) -> Self {
        let backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let mut config = TaskExecutionRegistryConfig::for_process(backend);
        // Only an explicit `advance_deadlines` may wake a waiter, so nothing
        // in these tests depends on elapsed wall time.
        config.gate_poll_interval = Duration::from_secs(3600);
        adjust(&mut config);
        let clock = Arc::new(ManualClock::new());
        let ledger = Arc::new(HostLedger::default());
        let context_host = Arc::new(FakeContextHost::new(
            Arc::clone(&ledger),
            Arc::clone(&clock),
        ));
        let task_host = Arc::new(FakeTaskHost::new(Arc::clone(&ledger)));
        let registry = TaskExecutionRegistry::new(
            config,
            Arc::clone(&clock) as Arc<dyn super::clock::BackendMonotonicClock>,
            Arc::clone(&context_host) as Arc<dyn QueryContextHost>,
            Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        );
        Self {
            registry,
            clock,
            ledger,
            context_host,
            task_host,
            backend,
            frontend,
        }
    }

    fn execution(&self, query: i64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(query, query + 1),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn context(&self, query: i64) -> QueryContextRef {
        QueryContextRef::new(self.execution(query), self.frontend, self.backend)
    }

    fn identity(&self, query: i64, stage: u32, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            self.execution(query),
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            self.backend,
        )
    }

    fn descriptor(&self, identity: TaskIdentity, plan: u8) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            UniqueId::new(
                i64::from(identity.stage_id().get()),
                i64::from(identity.task_id().get()),
            ),
            std::num::NonZeroUsize::new(2).expect("nonzero dop"),
            vec![PlanNodeId::new(3).expect("nonnegative node")],
            ExchangeTopology::default(),
            FakePlan::arc(plan),
        )
        .expect("a legal descriptor")
    }

    fn establish_request(&self, query: i64, valid_for: Duration) -> EstablishQueryContext {
        EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            self.context(query),
            FakeContent::arc(1),
            FakeContent::arc(2),
            CredentialUpdate::new(
                CredentialLeaseId::new(9),
                CredentialEpoch::FIRST,
                FakeSecret::arc(7),
            ),
            LeaseValidFor::new(valid_for).expect("a legal lease duration"),
        )
    }

    /// Establishes one context and asserts it reached `ACTIVE`.
    fn establish(&self, query: i64) -> QueryContextRef {
        let request = self.establish_request(query, LeaseBounds::INITIAL_REQUEST);
        let receipt = self
            .registry
            .update_query_context(&UpdateQueryContext::Establish(request));
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        let context = self.context(query);
        assert_eq!(
            self.registry.context_state(context),
            QueryContextState::Active
        );
        context
    }

    fn create_request(&self, identity: TaskIdentity, plan: u8) -> CreateTask {
        CreateTask::try_new(
            TaskOperationId::new_v7(),
            QueryContextRef::new(identity.query_execution_id(), self.frontend, self.backend),
            self.descriptor(identity, plan),
            Vec::new(),
        )
        .expect("a legal create")
    }

    /// Creates one task and asserts it was accepted.
    fn create(&self, identity: TaskIdentity, plan: u8) -> TaskStatusReporter {
        let receipt = self
            .registry
            .create_task(&self.create_request(identity, plan));
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        self.task_host.reporter(identity)
    }

    /// Finishes one task the way a healthy read task does.
    fn finish(&self, reporter: &TaskStatusReporter) {
        assert!(matches!(reporter.running(), StatusAdvance::Published(_)));
        assert!(matches!(
            reporter.finished(TaskOutputFacts::new(true)),
            StatusAdvance::Published(_)
        ));
    }
}

fn filter_update(version: u64, payload: u8) -> TaskDomainUpdate {
    TaskDomainUpdate::TaskDynamicFilter {
        version: DomainVersion::new(version).expect("nonzero version"),
        payload: FakeContent::arc(payload),
    }
}

fn split_update(node: i32, first: u64, last: u64, no_more: bool, payload: u8) -> TaskDomainUpdate {
    TaskDomainUpdate::SplitAssignment(
        SplitAssignmentIntent::new(
            PlanNodeId::new(node).expect("nonnegative node"),
            SplitSequence::new(first).expect("nonzero sequence"),
            SplitSequence::new(last).expect("nonzero sequence"),
            no_more,
            FakeContent::arc(payload),
        )
        .expect("a legal split batch"),
    )
}

// --------------------------------------------------------------------- create

#[test]
fn concurrent_exact_creates_produce_one_acknowledgement() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let descriptor = fixture.descriptor(identity, 5);

    const CREATES: usize = 4;
    fixture
        .task_host
        .require_arrivals
        .store(CREATES, Ordering::SeqCst);

    let mut handles = Vec::new();
    for _ in 0..CREATES {
        let registry = Arc::clone(&fixture.registry);
        let task_host = Arc::clone(&fixture.task_host);
        let descriptor = descriptor.clone();
        handles.push(std::thread::spawn(move || {
            let request =
                CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
                    .expect("a legal create");
            task_host.arrivals.fetch_add(1, Ordering::SeqCst);
            registry.create_task(&request)
        }));
    }
    let receipts: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("create thread"))
        .collect();

    let accepted: Vec<_> = receipts
        .iter()
        .filter(|receipt| receipt.outcome() == OperationOutcome::Accepted)
        .collect();
    assert_eq!(accepted.len(), 1, "{receipts:?}");
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt.outcome() == OperationOutcome::Idempotent)
            .count(),
        CREATES - 1
    );
    let expected = accepted[0]
        .acknowledgement()
        .expect("the acknowledged create carries a receipt");
    for receipt in &receipts {
        assert_eq!(
            receipt.acknowledgement(),
            Some(expected),
            "every converging create sees the same receipt"
        );
    }
    // One pipeline, not four.
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.capabilities_installed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
    assert_eq!(fixture.registry.counters().tasks_created, 1);
}

#[test]
fn a_lost_acknowledgement_replay_confirms_without_restarting() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let request = fixture.create_request(identity, 5);

    let first = fixture.registry.create_task(&request);
    assert_eq!(first.outcome(), OperationOutcome::Accepted);
    let replay = fixture.registry.create_task(&request);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(replay.acknowledgement(), first.acknowledgement());
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 1);
}

#[test]
fn a_conflicting_descriptor_fails_closed() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    fixture.create(identity, 5);

    let conflicting = fixture
        .registry
        .create_task(&fixture.create_request(identity, 6));
    assert_eq!(conflicting.outcome(), OperationOutcome::CreateConflict);
    assert!(conflicting.acknowledgement().is_none());
    // The installed task is untouched.
    assert!(fixture.registry.has_live_task(identity));
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_removed), 0);
}

#[test]
fn a_conflicting_descriptor_does_not_preempt_a_creation_in_progress() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);

    // Hold the creation owner inside its transaction, before its receiver is
    // installed.
    fixture.task_host.install_gate.close();
    let registry = Arc::clone(&fixture.registry);
    let owner_request = fixture.create_request(identity, 5);
    let owner = std::thread::spawn(move || registry.create_task(&owner_request));
    while fixture.registry.in_flight_operations(context) == 0 {
        std::thread::yield_now();
    }

    // A conflicting create reaches the reserved identity and is refused
    // without touching the creation in progress.
    let conflicting = fixture
        .registry
        .create_task(&fixture.create_request(identity, 9));
    assert_eq!(conflicting.outcome(), OperationOutcome::CreateConflict);

    fixture.task_host.install_gate.open();
    let accepted = owner.join().expect("owner thread");
    assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
    assert!(fixture.registry.has_live_task(identity));
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
}

#[test]
fn a_create_for_a_terminal_or_reaped_identity_fails_closed() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();

    // A different descriptor for a terminal identity is a conflict.
    let conflicting = fixture
        .registry
        .create_task(&fixture.create_request(identity, 6));
    assert_eq!(conflicting.outcome(), OperationOutcome::CreateConflict);

    // Past the request horizon the record is reclaimed and a create can no
    // longer prove anything about it.
    fixture.clock.advance(
        TaskExecutionRegistryConfig::for_process(fixture.backend)
            .request_horizon
            .total()
            + Duration::from_secs(1),
    );
    fixture.registry.advance_deadlines();
    let reaped = fixture
        .registry
        .create_task(&fixture.create_request(identity, 5));
    assert_eq!(reaped.outcome(), OperationOutcome::Gone, "{reaped:?}");
}

#[test]
fn a_create_waiting_on_the_gate_is_bounded_by_its_own_deadline() {
    let fixture = Fixture::new();
    let identity = fixture.identity(1, 1, 1);
    let request = fixture.create_request(identity, 5);
    let registry = Arc::clone(&fixture.registry);
    let waiter = std::thread::spawn(move || registry.create_task(&request));

    // The context is never established, so only the create's own deadline can
    // end the wait.
    while !waiter.is_finished() {
        fixture.clock.advance(Duration::from_secs(60));
        fixture.registry.advance_deadlines();
        std::thread::yield_now();
    }
    let outcome = waiter.join().expect("waiter thread");
    assert_eq!(outcome.outcome(), OperationOutcome::OperationTimedOut);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 0);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 0);
}

#[test]
fn a_failed_creation_leaves_nothing_findable() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    fixture.task_host.fail_submit.store(true, Ordering::SeqCst);

    let receipt = fixture
        .registry
        .create_task(&fixture.create_request(identity, 5));
    assert_eq!(receipt.outcome(), OperationOutcome::ResourceExhausted);
    assert!(receipt.acknowledgement().is_none());

    // Both installs were undone, and the only step that could have started a
    // worker never did.
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_removed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.capabilities_installed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.capabilities_removed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 0);
    assert_eq!(fixture.registry.counters().creations_rolled_back, 1);
    assert_eq!(fixture.registry.counters().tasks_created, 0);

    // Nothing is findable by a later status or update.
    assert!(!fixture.registry.has_live_task(identity));
    let update = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![filter_update(1, 3)],
        )
        .expect("a legal update"),
    );
    assert_eq!(update.outcome(), OperationOutcome::InvalidStateOrRequest);
    let final_info = fixture
        .registry
        .get_final_task_info(&GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity));
    assert_eq!(
        final_info.outcome(),
        OperationOutcome::InvalidStateOrRequest
    );
    let source = fixture
        .registry
        .status_source(fixture.context(1))
        .expect("an active context has a source");
    assert!(source.latest(identity).is_none());
    assert_eq!(
        source.observe(TaskStatusCursor::unobserved(identity)),
        CursorObservation::Unknown
    );
}

#[test]
fn concurrent_creates_observe_one_shared_failure() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let descriptor = fixture.descriptor(identity, 5);
    fixture.task_host.fail_submit.store(true, Ordering::SeqCst);

    const CREATES: usize = 3;
    fixture
        .task_host
        .require_arrivals
        .store(CREATES, Ordering::SeqCst);
    let mut handles = Vec::new();
    for _ in 0..CREATES {
        let registry = Arc::clone(&fixture.registry);
        let task_host = Arc::clone(&fixture.task_host);
        let descriptor = descriptor.clone();
        handles.push(std::thread::spawn(move || {
            let request =
                CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
                    .expect("a legal create");
            task_host.arrivals.fetch_add(1, Ordering::SeqCst);
            registry.create_task(&request)
        }));
    }
    let receipts: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("create thread"))
        .collect();
    // Every create either converged on the owner's failure or, arriving after
    // the reservation was already gone, ran and failed the same way. Either
    // path is the same category and never a partial success.
    for receipt in &receipts {
        assert_eq!(
            receipt.outcome(),
            OperationOutcome::ResourceExhausted,
            "{receipt:?}"
        );
    }
    assert!(!fixture.registry.has_live_task(identity));
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 0);
    assert_eq!(
        HostLedger::get(&fixture.ledger.receivers_installed),
        HostLedger::get(&fixture.ledger.receivers_removed)
    );
    assert_eq!(
        HostLedger::get(&fixture.ledger.capabilities_installed),
        HostLedger::get(&fixture.ledger.capabilities_removed)
    );
}

// --------------------------------------------------------- context lifecycle

#[test]
fn establishing_that_expires_before_active_rolls_back_and_wakes_waiters() {
    let fixture = Fixture::new();
    let context = fixture.context(1);
    let identity = fixture.identity(1, 1, 1);

    // Materialization outlives the whole initial lease.
    *fixture
        .context_host
        .materialize_advance
        .lock()
        .expect("materialize advance") = LeaseBounds::DEFAULT.max() + Duration::from_secs(5);

    let create_request = fixture.create_request(identity, 5);
    let registry = Arc::clone(&fixture.registry);
    let waiter = std::thread::spawn(move || registry.create_task(&create_request));
    // The create has entered the owner and is heading for the creation gate.
    while fixture.registry.in_flight_operations(context) == 0 {
        std::thread::yield_now();
    }

    let establish = fixture
        .registry
        .update_query_context(&UpdateQueryContext::Establish(
            fixture.establish_request(1, LeaseBounds::INITIAL_REQUEST),
        ));
    assert_eq!(establish.outcome(), OperationOutcome::LeaseExpired);

    let create = waiter.join().expect("waiter thread");
    assert_eq!(
        create.outcome(),
        OperationOutcome::LeaseExpired,
        "the waiter fails with the establish, not on its own deadline"
    );

    // Everything the establish installed was rolled back, and no task or
    // worker was ever allocated.
    let counters = fixture.registry.counters();
    assert_eq!(counters.contexts_established, 1);
    assert_eq!(counters.contexts_rolled_back, 1);
    assert_eq!(counters.tasks_created, 0);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_materialized), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 0);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 0);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
    assert_eq!(fixture.registry.installed_lease(context), None);
    assert!(!fixture.registry.has_live_task(identity));
}

#[test]
fn reaching_active_does_not_reset_the_initial_expiry() {
    let fixture = Fixture::new();
    // Materialization burns part of the initial lease but not all of it.
    *fixture
        .context_host
        .materialize_advance
        .lock()
        .expect("materialize advance") = Duration::from_secs(5);
    let context = fixture.establish(1);

    let lease = fixture
        .registry
        .installed_lease(context)
        .expect("an active context holds a lease");
    assert!(lease.sequence().is_initial());
    // The timer started when the gate was won, at origin, and the clamped
    // effective duration is the backend's maximum.
    assert_eq!(
        lease.expires_at().since_origin(),
        LeaseBounds::DEFAULT.max(),
        "reaching ACTIVE five seconds later must not extend the sequence-zero expiry"
    );
}

#[test]
fn a_renewal_extends_the_lease_and_a_replay_does_not() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    fixture.clock.advance(Duration::from_secs(10));

    let request = RenewQueryExecutionLease::new(
        TaskOperationId::new_v7(),
        context,
        LeaseSequence::new(1),
        LeaseValidFor::new(LeaseBounds::STEADY_REQUEST).expect("a legal duration"),
    );
    let renewed = fixture
        .registry
        .update_query_context(&UpdateQueryContext::RenewLease(request.clone()));
    assert_eq!(renewed.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        fixture
            .registry
            .installed_lease(context)
            .expect("lease")
            .expires_at()
            .since_origin(),
        Duration::from_secs(10) + LeaseBounds::STEADY_REQUEST
    );

    let replay = fixture
        .registry
        .update_query_context(&UpdateQueryContext::RenewLease(request));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        fixture
            .registry
            .installed_lease(context)
            .expect("lease")
            .expires_at()
            .since_origin(),
        Duration::from_secs(10) + LeaseBounds::STEADY_REQUEST,
        "an exact replay must not extend anything"
    );
}

#[test]
fn termination_is_first_wins_across_abort_expiry_and_task_failure() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        fixture.registry.termination_cause(context),
        Some(AbortCause::QueryFailed)
    );
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );

    // A later lease expiry loses.
    fixture.clock.advance(Duration::from_secs(60));
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.termination_cause(context),
        Some(AbortCause::QueryFailed),
        "a later cause must never rewrite the first"
    );

    // One fan-out: one capability revocation, one shared-fact release, one
    // abort of the runnable.
    assert_eq!(HostLedger::get(&fixture.ledger.capabilities_removed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.aborts), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_removed), 1);
}

#[test]
fn a_task_failure_can_win_the_context_latch() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let reporter = fixture.create(fixture.identity(1, 1, 1), 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));
    let failure = TaskFailure::new(
        TaskFailureCategory::Execution,
        SafeDetail::new("operator error").expect("fits"),
    );
    assert!(matches!(
        reporter.failing(failure.clone()),
        StatusAdvance::Published(_)
    ));
    assert!(matches!(
        reporter.failed(failure),
        StatusAdvance::Published(_)
    ));

    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.termination_cause(context),
        Some(AbortCause::PeerTaskFailed)
    );
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
    assert_eq!(fixture.registry.counters().task_failure_escalations, 1);

    // A late explicit abort loses.
    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::ContextTerminalReceipt);
    assert_eq!(
        fixture.registry.termination_cause(context),
        Some(AbortCause::PeerTaskFailed)
    );
}

#[test]
fn a_lease_expiry_aborts_and_is_reported_as_such() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    fixture.create(identity, 5);

    fixture
        .clock
        .advance(LeaseBounds::DEFAULT.max() + Duration::from_secs(1));
    let sweep = fixture.registry.advance_deadlines();
    assert_eq!(sweep.leases_expired, 1);
    assert_eq!(
        fixture.registry.termination_cause(context),
        Some(AbortCause::LeaseExpired)
    );
    assert_eq!(fixture.registry.counters().lease_expiries, 1);

    // A renewal that arrives after the local expiry never extends anything.
    let renew = fixture
        .registry
        .update_query_context(&UpdateQueryContext::RenewLease(
            RenewQueryExecutionLease::new(
                TaskOperationId::new_v7(),
                context,
                LeaseSequence::new(1),
                LeaseValidFor::new(LeaseBounds::STEADY_REQUEST).expect("a legal duration"),
            ),
        ));
    assert_eq!(renew.outcome(), OperationOutcome::LeaseExpired);
    assert_eq!(fixture.registry.installed_lease(context), None);
}

#[test]
fn abort_before_establish_fences_a_later_establish_and_create() {
    let fixture = Fixture::new();
    let context = fixture.context(1);
    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );

    let establish = fixture
        .registry
        .update_query_context(&UpdateQueryContext::Establish(
            fixture.establish_request(1, LeaseBounds::INITIAL_REQUEST),
        ));
    assert_eq!(
        establish.outcome(),
        OperationOutcome::ContextTerminalReceipt,
        "a late establish must not revive a fenced execution"
    );
    assert_eq!(HostLedger::get(&fixture.ledger.facts_materialized), 0);

    let create = fixture
        .registry
        .create_task(&fixture.create_request(fixture.identity(1, 1, 1), 5));
    assert_eq!(create.outcome(), OperationOutcome::ContextTerminalReceipt);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 0);
}

#[test]
fn an_establish_conflict_is_reported_rather_than_applied() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    // A different initial lease duration is a different immutable request.
    let conflicting = EstablishQueryContext::new(
        TaskOperationId::new_v7(),
        context,
        FakeContent::arc(1),
        FakeContent::arc(2),
        CredentialUpdate::new(
            CredentialLeaseId::new(9),
            CredentialEpoch::FIRST,
            FakeSecret::arc(7),
        ),
        LeaseValidFor::new(Duration::from_secs(7)).expect("a legal duration"),
    );
    let receipt = fixture
        .registry
        .update_query_context(&UpdateQueryContext::Establish(conflicting));
    assert_eq!(receipt.outcome(), OperationOutcome::ContextConflict);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_materialized), 1);
}

#[test]
fn an_exact_establish_replay_is_idempotent() {
    let fixture = Fixture::new();
    let request = fixture.establish_request(1, LeaseBounds::INITIAL_REQUEST);
    let first = fixture
        .registry
        .update_query_context(&UpdateQueryContext::Establish(request.clone()));
    assert_eq!(first.outcome(), OperationOutcome::Accepted);
    let replay = fixture
        .registry
        .update_query_context(&UpdateQueryContext::Establish(request));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_materialized), 1);
}

// -------------------------------------------------------------------- release

#[test]
fn release_reports_not_ready_before_it_succeeds() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    let request = ReleaseQueryContext::new(TaskOperationId::new_v7(), context);

    // A running task is not drained.
    let first = fixture.registry.release_query_context(&request);
    assert_eq!(first.outcome(), OperationOutcome::ReleaseNotReady);
    assert_eq!(
        first
            .acknowledgement()
            .expect("release carries an acknowledgement")
            .release(),
        ReleaseOutcome::NotReady
    );
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Active
    );

    // Terminal but its output has not drained: still not ready.
    fixture.finish(&reporter);
    fixture.registry.advance_deadlines();
    let second = fixture.registry.release_query_context(&request);
    assert_eq!(second.outcome(), OperationOutcome::ReleaseNotReady);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Active
    );
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 0);

    reporter.release_output();
    fixture.registry.advance_deadlines();
    let third = fixture.registry.release_query_context(&request);
    assert_eq!(third.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        third
            .acknowledgement()
            .expect("release carries an acknowledgement")
            .release(),
        ReleaseOutcome::Released
    );
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
}

#[test]
fn release_then_abort_reports_the_normal_terminal() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let reporter = fixture.create(fixture.identity(1, 1, 1), 5);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();

    let release = fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(release.outcome(), OperationOutcome::Accepted);

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::ContextTerminalReceipt);
    assert_eq!(
        fixture.registry.termination_cause(context),
        None,
        "a normal closure carries no abort cause"
    );
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
}

#[test]
fn abort_then_release_reports_the_abort() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    fixture.create(fixture.identity(1, 1, 1), 5);

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);

    let release = fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(release.outcome(), OperationOutcome::ContextTerminalReceipt);
    let ack = release.acknowledgement().expect("release acknowledgement");
    assert_eq!(ack.release(), ReleaseOutcome::AlreadyTerminal);
    assert_eq!(ack.termination_cause(), Some(AbortCause::QueryFailed));
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
}

#[test]
fn a_terminal_task_never_auto_releases_its_context() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let first = fixture.identity(1, 1, 1);
    let sibling = fixture.identity(1, 1, 2);
    let reporter = fixture.create(first, 5);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();

    // Every task the backend currently knows is terminal and drained. A
    // legal create for a sibling is still in flight.
    fixture.task_host.install_gate.close();
    let registry = Arc::clone(&fixture.registry);
    let request = fixture.create_request(sibling, 6);
    let creating = std::thread::spawn(move || registry.create_task(&request));
    while fixture.registry.in_flight_operations(context) == 0 {
        std::thread::yield_now();
    }

    for _ in 0..3 {
        fixture.registry.advance_deadlines();
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Active,
            "no sweep may release a context on its own"
        );
    }
    // An explicit release is not ready either, because the create is in
    // flight — the backend never guesses that its task set is closed.
    let release = fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(release.outcome(), OperationOutcome::ReleaseNotReady);

    fixture.task_host.install_gate.open();
    let created = creating.join().expect("creating thread");
    assert_eq!(created.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Active
    );
    assert!(fixture.registry.has_live_task(sibling));
}

// ---------------------------------------------------------------- retirement

#[test]
fn a_terminal_replay_returns_the_original_receipt_and_an_advance_does_not() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let request = fixture.create_request(identity, 5);
    let created = fixture.registry.create_task(&request);
    assert_eq!(created.outcome(), OperationOutcome::Accepted);
    let reporter = fixture.task_host.reporter(identity);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();

    let replay = fixture.registry.create_task(&request);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(replay.acknowledgement(), created.acknowledgement());

    let advance = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![filter_update(1, 3)],
        )
        .expect("a legal update"),
    );
    assert_eq!(advance.outcome(), OperationOutcome::TerminalRejected);
    assert!(advance.acknowledgement().is_none());

    // The retained final info still answers a read.
    let info = fixture
        .registry
        .get_final_task_info(&GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity));
    assert_eq!(info.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        info.acknowledgement()
            .expect("final info")
            .final_status()
            .state(),
        TaskState::Finished
    );
}

#[test]
fn retention_expiry_yields_gone() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();
    let source = fixture
        .registry
        .status_source(context)
        .expect("an active context has a source");
    while source.next_event().is_some() {}

    let horizon = TaskExecutionRegistryConfig::for_process(fixture.backend)
        .request_horizon
        .total();
    // Just inside the horizon the record is still retained.
    fixture.clock.advance(horizon - Duration::from_secs(1));
    let sweep = fixture.registry.advance_deadlines();
    assert_eq!(sweep.tasks_reaped, 0);
    assert_eq!(
        fixture
            .registry
            .get_final_task_info(&GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity))
            .outcome(),
        OperationOutcome::Accepted
    );

    fixture.clock.advance(Duration::from_secs(2));
    let sweep = fixture.registry.advance_deadlines();
    assert_eq!(sweep.tasks_reaped, 1);
    assert_eq!(
        fixture
            .registry
            .get_final_task_info(&GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity))
            .outcome(),
        OperationOutcome::Gone
    );
    assert_eq!(
        source.observe(TaskStatusCursor::unobserved(identity)),
        CursorObservation::Gone
    );
    assert_eq!(source.next_event(), Some(TaskStatusEvent::Gone(identity)));
}

#[test]
fn retention_is_swept_by_capacity_as_well_as_by_horizon() {
    let fixture = Fixture::with_config(|config| {
        config.retained_task_capacity = 1;
    });
    fixture.establish(1);
    for task in 1..=3u32 {
        let identity = fixture.identity(1, 1, task);
        let reporter = fixture.create(identity, 5);
        fixture.finish(&reporter);
        reporter.release_output();
        fixture.registry.advance_deadlines();
    }
    // Well inside the horizon, yet only one record is still retained.
    let retained = (1..=3u32)
        .filter(|task| {
            fixture
                .registry
                .get_final_task_info(&GetFinalTaskInfo::new(
                    TaskOperationId::new_v7(),
                    fixture.identity(1, 1, *task),
                ))
                .outcome()
                == OperationOutcome::Accepted
        })
        .count();
    assert_eq!(retained, 1);
    // The oldest were reclaimed, not silently forgotten.
    assert_eq!(
        fixture
            .registry
            .get_final_task_info(&GetFinalTaskInfo::new(
                TaskOperationId::new_v7(),
                fixture.identity(1, 1, 1),
            ))
            .outcome(),
        OperationOutcome::Gone
    );
}

// ------------------------------------------------------------------- status

#[test]
fn a_same_version_reread_is_identical_while_metrics_keep_changing() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));
    let source = fixture
        .registry
        .status_source(context)
        .expect("an active context has a source");

    let CursorObservation::Current(first) = source.observe(TaskStatusCursor::unobserved(identity))
    else {
        panic!("a behind cursor is answered with the current snapshot");
    };
    assert_eq!(first.state(), TaskState::Running);

    let metrics = reporter.metrics();
    for queued in 0..100u64 {
        assert_eq!(
            metrics.report(TaskResourceFacts::empty().with_queued_splits(queued), None),
            StatusAdvance::Throttled
        );
    }
    let CursorObservation::Current(reread) = source.observe(TaskStatusCursor::unobserved(identity))
    else {
        panic!("a behind cursor is answered with the current snapshot");
    };
    assert_eq!(reread, first, "a published version is bound to one value");

    // Past the throttle the retained metrics publish once, at a new version.
    fixture
        .clock
        .advance(METRIC_PUBLISH_MIN_INTERVAL + Duration::from_millis(1));
    assert_eq!(fixture.registry.advance_deadlines().metrics_flushed, 1);
    let CursorObservation::Current(after) =
        source.observe(TaskStatusCursor::at(identity, first.version()))
    else {
        panic!("a new version is observable");
    };
    assert_eq!(after.version().get(), first.version().get() + 1);
    assert_eq!(after.resources().queued_splits(), Some(99));
    assert_eq!(after.state(), TaskState::Running);
}

#[test]
fn a_metrics_producer_cannot_move_a_terminal_task() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let reporter = fixture.create(fixture.identity(1, 1, 1), 5);
    fixture.finish(&reporter);
    let terminal = reporter.current();

    assert_eq!(
        reporter
            .metrics()
            .report(TaskResourceFacts::empty().with_running_drivers(4), None),
        StatusAdvance::AlreadyTerminal(TaskState::Finished)
    );
    assert_eq!(reporter.current(), terminal);
    assert!(matches!(
        reporter.running(),
        StatusAdvance::AlreadyTerminal(TaskState::Finished)
    ));
    assert_eq!(reporter.current(), terminal);
}

#[test]
fn a_noisy_task_cannot_starve_another_tasks_snapshot() {
    let fixture = Fixture::new();
    let source = TaskStatusSource::new();
    let noisy = fixture.identity(1, 1, 1);
    let quiet = fixture.identity(1, 1, 2);

    for version in 1..=50u64 {
        source.publish(running_status(noisy, version));
    }
    source.publish(running_status(quiet, 1));

    let mut seen = BTreeSet::new();
    for _ in 0..2 {
        match source.next_event().expect("a queued frame") {
            TaskStatusEvent::Status(status) => {
                seen.insert(status.identity());
            }
            TaskStatusEvent::Gone(_) => panic!("no task was reclaimed"),
        }
    }
    assert!(
        seen.contains(&quiet),
        "the quiet task's snapshot is delivered without waiting out the noisy one"
    );
    assert!(seen.contains(&noisy));
    assert_eq!(source.next_event(), None);
    let stats = source.stats();
    assert_eq!(stats.published, 51);
    assert_eq!(stats.delivered, 2);
    assert_eq!(
        stats.coalesced, 49,
        "only non-current snapshots no observer had taken were dropped"
    );
}

#[test]
fn a_behind_cursor_gets_the_latest_rather_than_a_replay() {
    let fixture = Fixture::new();
    let source = TaskStatusSource::new();
    let identity = fixture.identity(1, 1, 1);
    for version in 1..=5u64 {
        source.publish(running_status(identity, version));
    }
    let CursorObservation::Current(status) =
        source.observe(TaskStatusCursor::at(identity, TaskStatusVersion::FIRST))
    else {
        panic!("a behind cursor is answered");
    };
    assert_eq!(status.version().get(), 5);
    assert_eq!(
        source.observe(TaskStatusCursor::at(identity, status.version())),
        CursorObservation::UpToDate
    );
}

fn running_status(identity: TaskIdentity, version: u64) -> TaskStatus {
    TaskStatus::try_new(
        identity,
        TaskStatusVersion::new(version).expect("nonzero version"),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .expect("a legal snapshot")
}

// --------------------------------------------------------------- operations

#[test]
fn a_task_update_advances_its_own_domains_atomically() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    fixture.create(identity, 5);

    let accepted = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_update(3, 1, 4, false, 11)],
        )
        .expect("a legal update"),
    );
    assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
    assert_eq!(HostLedger::get(&fixture.ledger.task_domains_applied), 1);

    // A gap fails closed and applies nothing.
    let gap = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_update(3, 9, 9, false, 12)],
        )
        .expect("a legal update"),
    );
    assert_eq!(gap.outcome(), OperationOutcome::DomainConflict);
    assert_eq!(HostLedger::get(&fixture.ledger.task_domains_applied), 1);

    // An unknown plan node is not a member of the frozen descriptor.
    let unknown = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_update(7, 1, 1, false, 13)],
        )
        .expect("a legal update"),
    );
    assert_eq!(unknown.outcome(), OperationOutcome::DomainConflict);

    // A host rejection leaves every token where it was, so the identical
    // request stays a clean replay.
    fixture
        .task_host
        .fail_task_domain
        .store(true, Ordering::SeqCst);
    let rejected = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_update(3, 5, 5, true, 14)],
        )
        .expect("a legal update"),
    );
    assert_eq!(rejected.outcome(), OperationOutcome::ResourceExhausted);
    fixture
        .task_host
        .fail_task_domain
        .store(false, Ordering::SeqCst);
    let retried = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_update(3, 5, 5, true, 14)],
        )
        .expect("a legal update"),
    );
    assert_eq!(retried.outcome(), OperationOutcome::Accepted);
}

#[test]
fn a_shared_domain_advance_cannot_create_a_context() {
    let fixture = Fixture::new();
    let context = fixture.context(1);
    let advance = fixture
        .registry
        .update_query_context(&UpdateQueryContext::AdvanceDomain(
            AdvanceQueryContextDomain::new(
                TaskOperationId::new_v7(),
                context,
                QueryContextDomainUpdate::CatalogBinding {
                    version: DomainVersion::new(2).expect("nonzero"),
                    payload: FakeContent::arc(4),
                },
            ),
        ));
    assert_eq!(advance.outcome(), OperationOutcome::ContextNotEstablished);
    assert_eq!(HostLedger::get(&fixture.ledger.shared_domains_applied), 0);
}

#[test]
fn a_shared_domain_advance_applies_once_and_replays_idempotently() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let request = AdvanceQueryContextDomain::new(
        TaskOperationId::new_v7(),
        context,
        QueryContextDomainUpdate::CatalogBinding {
            version: DomainVersion::new(2).expect("nonzero"),
            payload: FakeContent::arc(4),
        },
    );
    let applied = fixture
        .registry
        .update_query_context(&UpdateQueryContext::AdvanceDomain(request.clone()));
    assert_eq!(applied.outcome(), OperationOutcome::Accepted);
    let replay = fixture
        .registry
        .update_query_context(&UpdateQueryContext::AdvanceDomain(request));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(HostLedger::get(&fixture.ledger.shared_domains_applied), 1);
}

#[test]
fn a_cancel_stands_a_task_down_and_a_late_cancel_is_settled() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));

    let request = CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    );
    let cancel = fixture.registry.cancel_task(&request);
    assert_eq!(cancel.outcome(), OperationOutcome::Accepted);
    assert_eq!(HostLedger::get(&fixture.ledger.cancels), 1);
    assert_eq!(reporter.current().state(), TaskState::Canceled);

    let late = fixture.registry.cancel_task(&request);
    assert_eq!(
        late.outcome(),
        OperationOutcome::Idempotent,
        "a cancel racing a terminal is a normal race, not a failed attempt"
    );
    assert_eq!(HostLedger::get(&fixture.ledger.cancels), 1);
}

#[test]
fn a_dynamic_filter_read_returns_what_the_task_advertised() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));

    let empty = fixture
        .registry
        .fetch_task_dynamic_filters(&FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            None,
        ));
    assert_eq!(empty.outcome(), OperationOutcome::Idempotent);
    assert!(empty.acknowledgement().is_none());

    let version = DomainVersion::new(4).expect("nonzero");
    assert!(matches!(
        reporter.advertise_dynamic_filters(version, 2, FakeContent::arc(21)),
        StatusAdvance::Republished(_)
    ));
    let read = fixture
        .registry
        .fetch_task_dynamic_filters(&FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            None,
        ));
    assert_eq!(read.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        read.acknowledgement().expect("a filter read").version(),
        version
    );

    let acknowledged = fixture
        .registry
        .fetch_task_dynamic_filters(&FetchTaskDynamicFilters::new(
            TaskOperationId::new_v7(),
            identity,
            Some(version),
        ));
    assert_eq!(acknowledged.outcome(), OperationOutcome::Idempotent);
}

// ----------------------------------------------------------------- fencing

#[test]
fn a_request_for_another_backend_process_is_an_identity_mismatch() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let foreign = TaskIdentity::new(
        fixture.execution(1),
        StageId::new(1).expect("nonzero"),
        TaskId::new(1).expect("nonzero"),
        BackendProcessId::new_v7(),
    );
    let create = CreateTask::try_new(
        TaskOperationId::new_v7(),
        QueryContextRef::new(
            fixture.execution(1),
            fixture.frontend,
            foreign.backend_process_id(),
        ),
        fixture.descriptor(foreign, 5),
        Vec::new(),
    )
    .expect("a legal create for another process");
    let receipt = fixture.registry.create_task(&create);
    assert_eq!(receipt.outcome(), OperationOutcome::IdentityMismatch);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 0);
}

#[test]
fn a_replaced_frontend_incarnation_is_fenced() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let replaced = QueryContextRef::new(
        fixture.execution(1),
        FrontendProcessId::new_v7(),
        fixture.backend,
    );
    let receipt = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            replaced,
            AbortCause::QueryFailed,
        ));
    assert_eq!(receipt.outcome(), OperationOutcome::IdentityMismatch);
    assert_eq!(
        fixture.registry.context_state(fixture.context(1)),
        QueryContextState::Active
    );
}

// ------------------------------------------------------------------- bounds

#[test]
fn the_per_context_task_bound_is_a_typed_resource_exhaustion() {
    let fixture = Fixture::with_config(|config| {
        config.max_tasks_per_context = 1;
    });
    fixture.establish(1);
    fixture.create(fixture.identity(1, 1, 1), 5);

    let exhausted = fixture
        .registry
        .create_task(&fixture.create_request(fixture.identity(1, 1, 2), 6));
    assert_eq!(exhausted.outcome(), OperationOutcome::ResourceExhausted);
    assert!(exhausted.acknowledgement().is_none());
    // It failed closed rather than degrading: no install was even attempted.
    assert_eq!(HostLedger::get(&fixture.ledger.receivers_installed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.submit_attempts), 1);
}

#[test]
fn the_per_backend_task_bound_is_a_typed_resource_exhaustion() {
    let fixture = Fixture::with_config(|config| {
        config.max_active_tasks_per_backend = 1;
    });
    fixture.establish(1);
    fixture.establish(2);
    fixture.create(fixture.identity(1, 1, 1), 5);

    let exhausted = fixture
        .registry
        .create_task(&fixture.create_request(fixture.identity(2, 1, 1), 6));
    assert_eq!(exhausted.outcome(), OperationOutcome::ResourceExhausted);
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
}

#[test]
fn a_reclaimed_context_answers_gone_rather_than_absent() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let reporter = fixture.create(fixture.identity(1, 1, 1), 5);
    fixture.finish(&reporter);
    reporter.release_output();
    fixture.registry.advance_deadlines();
    fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));

    let horizon = TaskExecutionRegistryConfig::for_process(fixture.backend)
        .request_horizon
        .total();
    fixture.clock.advance(horizon + Duration::from_secs(1));
    let sweep = fixture.registry.advance_deadlines();
    assert!(sweep.contexts_reaped >= 1);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Gone
    );

    let create = fixture
        .registry
        .create_task(&fixture.create_request(fixture.identity(1, 1, 2), 7));
    assert_eq!(create.outcome(), OperationOutcome::Gone);
    let release = fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(release.outcome(), OperationOutcome::Gone);
}

#[test]
fn every_receipt_carries_its_own_operation_id() {
    let fixture = Fixture::new();
    fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let request = fixture.create_request(identity, 5);
    let operation = request.envelope().operation_id();
    let receipt: OperationReceipt<_> = fixture.registry.create_task(&request);
    assert_eq!(receipt.operation_id(), operation);

    let cancel = CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    );
    assert_eq!(
        fixture.registry.cancel_task(&cancel).operation_id(),
        cancel.envelope().operation_id()
    );
}

#[test]
fn an_uncooperative_task_is_terminated_after_the_termination_grace() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);
    let reporter = fixture.create(identity, 5);
    assert!(matches!(reporter.running(), StatusAdvance::Published(_)));
    fixture
        .task_host
        .ignore_stand_down
        .store(true, Ordering::SeqCst);

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);
    assert_eq!(HostLedger::get(&fixture.ledger.aborts), 1);
    // The task was asked and did not converge, so cleanup is not finished and
    // the capability is already revoked.
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Aborting
    );
    assert_eq!(HostLedger::get(&fixture.ledger.capabilities_removed), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 0);

    // Inside the grace the owner keeps waiting rather than lying about the
    // task's outcome.
    fixture
        .clock
        .advance(TaskExecutionRegistryConfig::for_process(fixture.backend).termination_grace / 2);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Aborting
    );

    fixture
        .clock
        .advance(TaskExecutionRegistryConfig::for_process(fixture.backend).termination_grace);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
    assert_eq!(HostLedger::get(&fixture.ledger.facts_released), 1);
    let info = fixture
        .registry
        .get_final_task_info(&GetFinalTaskInfo::new(TaskOperationId::new_v7(), identity));
    assert_eq!(info.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        info.acknowledgement()
            .expect("final info")
            .final_status()
            .state(),
        TaskState::Aborted
    );
}

#[test]
fn a_create_that_loses_to_an_abort_rolls_back_its_submitted_worker() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);
    let identity = fixture.identity(1, 1, 1);

    // Hold the creation owner inside `submit_runnable`, after both installs
    // have already succeeded.
    fixture.task_host.submit_gate.close();
    let registry = Arc::clone(&fixture.registry);
    let request = fixture.create_request(identity, 5);
    let creating = std::thread::spawn(move || registry.create_task(&request));
    while HostLedger::get(&fixture.ledger.submit_attempts) == 0 {
        std::thread::yield_now();
    }

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);

    fixture.task_host.submit_gate.open();
    let created = creating.join().expect("creating thread");
    assert_eq!(
        created.outcome(),
        OperationOutcome::ContextTerminalReceipt,
        "the abort linearized first, so the create must not install a task"
    );
    assert!(created.acknowledgement().is_none());

    // Nothing was left behind: no live task, both installs undone, and the
    // submitted worker was stood down rather than orphaned.
    assert!(!fixture.registry.has_live_task(identity));
    assert_eq!(
        HostLedger::get(&fixture.ledger.receivers_installed),
        HostLedger::get(&fixture.ledger.receivers_removed)
    );
    assert_eq!(
        HostLedger::get(&fixture.ledger.capabilities_installed),
        HostLedger::get(&fixture.ledger.capabilities_removed)
    );
    assert_eq!(HostLedger::get(&fixture.ledger.runnables_submitted), 1);
    assert_eq!(HostLedger::get(&fixture.ledger.aborts), 1);
    assert_eq!(fixture.registry.counters().creations_rolled_back, 1);
    assert_eq!(fixture.registry.counters().tasks_created, 0);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[test]
fn two_concurrent_advances_never_roll_a_shared_token_backwards() {
    let fixture = Fixture::new();
    let context = fixture.establish(1);

    let mut handles = Vec::new();
    for version in [2u64, 3, 4, 5] {
        let registry = Arc::clone(&fixture.registry);
        handles.push(std::thread::spawn(move || {
            registry.update_query_context(&UpdateQueryContext::AdvanceDomain(
                AdvanceQueryContextDomain::new(
                    TaskOperationId::new_v7(),
                    context,
                    QueryContextDomainUpdate::CatalogBinding {
                        version: DomainVersion::new(version).expect("nonzero"),
                        payload: FakeContent::arc(version as u8),
                    },
                ),
            ))
        }));
    }
    for handle in handles {
        let receipt = handle.join().expect("advance thread");
        assert!(
            matches!(
                receipt.outcome(),
                OperationOutcome::Accepted | OperationOutcome::Idempotent
            ),
            "{receipt:?}"
        );
    }

    // Whatever the interleaving, the accepted version is the highest one and
    // never an earlier writer's.
    let observed = fixture
        .registry
        .update_query_context(&UpdateQueryContext::AdvanceDomain(
            AdvanceQueryContextDomain::new(
                TaskOperationId::new_v7(),
                context,
                QueryContextDomainUpdate::CatalogBinding {
                    version: DomainVersion::new(5).expect("nonzero"),
                    payload: FakeContent::arc(5),
                },
            ),
        ));
    assert_eq!(
        observed.outcome(),
        OperationOutcome::Idempotent,
        "version five must already be the accepted token"
    );
}
