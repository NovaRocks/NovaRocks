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

//! Neutral owner tests for the Worker task registry.

use std::any::Any;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution_contract::task_execution::creation::{
    CreationContent, FrozenBytes, PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::{
    DataStreamPartitionType, ExchangeDestination, ExchangeEdge, ExchangeTopology, FragmentNodeId,
    FragmentSinkKind, RuntimeEndpoint, TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitOffer, SplitSequence,
};
use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::lease::{LeaseSequence, LeaseValidFor};
use novarocks_execution_contract::task_execution::operation::{
    AcquireQueryContextAdmissionTicket, CancelTask, CreateTask, CredentialUpdate,
    EstablishQueryContext, OperationOutcome, RenewQueryExecutionLease, SplitAssignmentIntent,
    TaskDomainUpdate, UpdateQueryContext, UpdateTask,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, TaskFailureCategory,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_types::{NativeCompatibilityId, UniqueId};

use crate::{
    HostRejection, ManualClock, QueryContextHost, ReleasedContextEvidence, RootResultRoute,
    RunnableTask, SharedFactsRequest, TaskCreationGate, TaskExecutionHost, TaskExecutionMetrics,
    TaskExecutionPorts, TaskExecutionRegistry, TaskExecutionRegistryConfig, TaskProtocolEvent,
    TaskProtocolObserver, TaskResultLifecycle, TaskStatusEvent, TaskStatusReporter,
    WorkerMonotonicClock,
};

/// The static half of a creation this test host prepares a result sink from.
const RESULT_PLAN: &[u8] = b"result-plan";
/// A second legal static half, which the host prepares as an exchange
/// producer. A replay carrying it must not turn a result owner into one.
const STREAM_PLAN: &[u8] = b"stream-plan";
/// A static half the host refuses, the way a real host refuses a plan it
/// cannot decode or that disagrees with its assignment.
const REFUSED_PLAN: &[u8] = b"refused-plan";

/// The codec-owned half of a test creation. The registry never looks inside
/// it; only the host that wins the creation receives it.
#[derive(Debug)]
struct TestAssignment;

impl CreationContent for TestAssignment {
    fn encoded_len(&self) -> usize {
        8
    }

    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

/// One create request's body: its static plan bytes and its assignment.
fn body(plan: &'static [u8]) -> TaskCreationInput {
    TaskCreationInput::new(
        FrozenBytes::freeze(Bytes::from_static(plan)),
        Box::new(TestAssignment),
    )
}

#[derive(Debug)]
struct TestContent(u8);

impl CodecOwnedContent for TestContent {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([self.0; 16])
    }

    fn encoded_len(&self) -> usize {
        16
    }
}

struct TestSecret;

impl ConfidentialContent for TestSecret {
    fn encoded_len(&self) -> usize {
        16
    }

    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == self.encoded_len()
    }
}

struct TestContextHost;

impl QueryContextHost for TestContextHost {
    fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        Ok(())
    }

    fn release(&self, _context: QueryContextRef) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }

    fn advance_shared_domain(
        &self,
        _context: QueryContextRef,
        _domain: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        Ok(())
    }
}

#[derive(Debug)]
struct TestRunnable {
    cancel_calls: Arc<AtomicUsize>,
}

impl RunnableTask for TestRunnable {
    fn commit_creation(&self) {}

    fn cancel(&self, _reason: CancelReason) {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn abort(&self, _cause: AbortCause) {}
}

/// A task host that is a ledger of what the owner asked it to do.
///
/// `prepared_bodies` records the static half of every creation the owner
/// handed to `install_receiver`, in call order. It is how these cases prove
/// that the host was reached exactly once per winning round, never for a
/// replay, and always with the winner's own body.
#[derive(Default)]
struct TestTaskHost {
    install_gate: Option<Arc<InstallGate>>,
    prepared_bodies: Mutex<Vec<Bytes>>,
    receivers_installed: AtomicUsize,
    receivers_removed: AtomicUsize,
    capabilities_installed: AtomicUsize,
    submitted: AtomicUsize,
    domains_applied: AtomicUsize,
    reporters: Mutex<Vec<TaskStatusReporter>>,
    cancel_calls: Arc<AtomicUsize>,
}

impl TestTaskHost {
    fn prepared_bodies(&self) -> Vec<Bytes> {
        self.prepared_bodies
            .lock()
            .expect("test prepared bodies")
            .clone()
    }
}

impl TaskExecutionHost for TestTaskHost {
    fn close_context_admission(&self, _context: QueryContextRef) {}

    fn retire_context_execution(&self, _context: QueryContextRef) {}

    fn forget_context_admission(&self, _context: QueryContextRef) {}

    fn install_receiver(
        &self,
        _descriptor: &TaskDescriptor,
        input: TaskCreationInput,
    ) -> Result<PreparedTaskFacts, HostRejection> {
        if let Some(gate) = &self.install_gate {
            gate.wait_for_release();
        }
        let (plan, _assignment) = input.into_parts();
        self.prepared_bodies
            .lock()
            .expect("test prepared bodies")
            .push(plan.to_bytes());
        if plan.bytes().as_ref() == REFUSED_PLAN {
            // A real host undoes its own local preparation before it refuses;
            // this one prepared nothing, so it has nothing to undo.
            return Err(HostRejection::new(
                TaskFailureCategory::Protocol,
                "test static plan is not decodable",
            ));
        }
        self.receivers_installed.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedTaskFacts::new(
            if plan.bytes().as_ref() == STREAM_PLAN {
                FragmentSinkKind::DataStream
            } else {
                FragmentSinkKind::Result
            },
        ))
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {
        self.receivers_removed.fetch_add(1, Ordering::SeqCst);
    }

    fn install_inbound_capability(
        &self,
        _descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        self.capabilities_installed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn remove_inbound_capability(&self, _descriptor: &TaskDescriptor) {}

    fn submit_runnable(
        &self,
        _descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        self.reporters
            .lock()
            .expect("test reporters")
            .push(reporter);
        Ok(Arc::new(TestRunnable {
            cancel_calls: Arc::clone(&self.cancel_calls),
        }))
    }

    fn apply_task_domain(
        &self,
        _descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection> {
        self.domains_applied.fetch_add(1, Ordering::SeqCst);
        Ok(match domain {
            TaskDomainUpdate::SplitAssignment(_) => Some(0),
            _ => None,
        })
    }
}

#[derive(Default)]
struct InstallGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl InstallGate {
    fn held() -> Self {
        Self {
            state: Mutex::new(GateState {
                held: true,
                waiter_entered: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().expect("test install gate");
        while !state.waiter_entered {
            state = self.changed.wait(state).expect("test install gate");
        }
    }

    fn wait_for_release(&self) {
        let mut state = self.state.lock().expect("test install gate");
        state.waiter_entered = true;
        self.changed.notify_all();
        while state.held {
            state = self.changed.wait(state).expect("test install gate");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("test install gate");
        state.held = false;
        self.changed.notify_all();
    }
}

struct NoopObserver;

impl TaskProtocolObserver for NoopObserver {
    fn observe(&self, _event: TaskProtocolEvent) {}
}

struct NoopResultLifecycle;

impl TaskResultLifecycle for NoopResultLifecycle {
    fn discard_task(&self, _identity: TaskIdentity) {}

    fn retire_task_result(&self, _identity: TaskIdentity) {}
}

struct NoopMetrics;

impl TaskExecutionMetrics for NoopMetrics {
    fn record_task_created(&self) {}
}

#[derive(Default)]
struct GateState {
    held: bool,
    waiter_entered: bool,
}

#[derive(Default)]
struct BlockingCreationGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl BlockingCreationGate {
    fn held() -> Self {
        Self {
            state: Mutex::new(GateState {
                held: true,
                waiter_entered: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_until_waiter_entered(&self) {
        let mut state = self.state.lock().expect("test creation gate");
        while !state.waiter_entered {
            state = self.changed.wait(state).expect("test creation gate");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("test creation gate");
        state.held = false;
        self.changed.notify_all();
    }
}

impl TaskCreationGate for BlockingCreationGate {
    fn holds_task_creation(&self, _context: QueryContextRef) -> bool {
        self.state.lock().expect("test creation gate").held
    }

    fn wait_for_task_creation_release(&self, _context: QueryContextRef) {
        let mut state = self.state.lock().expect("test creation gate");
        state.waiter_entered = true;
        self.changed.notify_all();
        while state.held {
            state = self.changed.wait(state).expect("test creation gate");
        }
    }
}

fn test_ports() -> TaskExecutionPorts {
    TaskExecutionPorts::new(
        Arc::new(NoopObserver),
        Arc::new(NoopResultLifecycle),
        Arc::new(NoopMetrics),
    )
}

fn establish(registry: &TaskExecutionRegistry, context: QueryContextRef) -> AdmissionTicketId {
    let ticket = registry
        .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid ticket lease"),
            NativeCompatibilityId::new([0x71; 32]),
            registry.admission_epoch_capability(),
        ))
        .acknowledgement()
        .expect("admission ticket")
        .ticket_id();
    let receipt =
        registry.update_query_context(&UpdateQueryContext::Establish(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            ticket,
            Arc::new(TestContent(1)),
            Arc::new(TestContent(2)),
            Arc::new(TestContent(3)),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(TestSecret),
            ),
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid context lease"),
        )));
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
    ticket
}

fn execution(query: i64) -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(query, query + 1),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id")
}

fn task(execution: QueryExecutionId, backend: BackendProcessId) -> TaskIdentity {
    TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    )
}

/// A descriptor whose every fact is parameterized, so a case can build a
/// second, different but legal body for the same identity.
fn descriptor_with(
    identity: TaskIdentity,
    kernel_key: UniqueId,
    pipeline_dop: usize,
    split_plan_node: i32,
) -> TaskDescriptor {
    TaskDescriptor::try_new(
        identity,
        kernel_key,
        std::num::NonZeroUsize::new(pipeline_dop).expect("nonzero dop"),
        vec![PlanNodeId::new(split_plan_node).expect("nonnegative node")],
        ExchangeTopology::default(),
    )
    .expect("legal descriptor")
}

fn descriptor(identity: TaskIdentity) -> TaskDescriptor {
    descriptor_with(identity, UniqueId::new(1, 1), 1, 1)
}

fn split_batch(node: i32, first: u64, last: u64) -> TaskDomainUpdate {
    TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
        PlanNodeId::new(node).expect("nonnegative node"),
        SplitOffer::batch(
            SplitSequence::new(first).expect("nonzero sequence"),
            SplitSequence::new(last).expect("nonzero sequence"),
            false,
        )
        .expect("a legal split batch"),
        Arc::new(TestContent(u8::try_from(first).expect("small sequence"))),
    ))
}

/// One established context over a fresh registry, with its ledger host.
struct Fixture {
    registry: Arc<TaskExecutionRegistry>,
    task_host: Arc<TestTaskHost>,
    backend: BackendProcessId,
    frontend: FrontendProcessId,
}

impl Fixture {
    fn new(task_host: TestTaskHost) -> Self {
        Self::with_config(task_host, |_| {})
    }

    fn with_config(
        task_host: TestTaskHost,
        adjust: impl FnOnce(&mut TaskExecutionRegistryConfig),
    ) -> Self {
        let backend = BackendProcessId::new_v7();
        let mut config = TaskExecutionRegistryConfig::for_process(backend, 17, 9);
        adjust(&mut config);
        let task_host = Arc::new(task_host);
        let registry = TaskExecutionRegistry::new(
            config,
            Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(TestContextHost),
            Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
            test_ports(),
        );
        Self {
            registry,
            task_host,
            backend,
            frontend: FrontendProcessId::new_v7(),
        }
    }

    fn context(&self, execution: QueryExecutionId) -> QueryContextRef {
        QueryContextRef::new(execution, self.frontend, self.backend)
    }

    fn create(
        &self,
        descriptor: TaskDescriptor,
        initial_domains: Vec<TaskDomainUpdate>,
    ) -> CreateTask {
        CreateTask::try_new(
            TaskOperationId::new_v7(),
            self.context(descriptor.identity().query_execution_id()),
            descriptor,
            initial_domains,
        )
        .expect("legal create")
    }
}

#[test]
fn lease_index_tracks_only_live_contexts_across_bulk_expiry() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let clock = Arc::new(ManualClock::new());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::new(TestTaskHost::default()),
        test_ports(),
    );

    let contexts: Vec<_> = (1..=32)
        .map(|number| {
            let execution = QueryExecutionId::new(
                QueryId::new(number, 1),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query id");
            QueryContextRef::new(execution, frontend, backend)
        })
        .collect();
    for (index, context) in contexts.iter().copied().enumerate() {
        establish(&registry, context);
        assert_eq!(registry.indexed_lease_count(), index + 1);
        assert_eq!(registry.context_state(context), QueryContextState::Active);
    }

    clock.advance(Duration::from_secs(9));
    assert_eq!(registry.advance_deadlines().leases_expired, 0);
    assert_eq!(registry.indexed_lease_count(), contexts.len());

    clock.advance(Duration::from_secs(1));
    assert_eq!(registry.advance_deadlines().leases_expired, contexts.len());
    assert_eq!(registry.indexed_lease_count(), 0);
    for context in contexts {
        assert_eq!(
            registry.context_state(context),
            QueryContextState::TerminalRetained
        );
        assert!(registry.status_source(context).is_some());
    }
}

#[test]
fn lease_renewal_and_replay_never_accumulate_expired_index_entries() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(91, 92),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let clock = Arc::new(ManualClock::new());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::new(TestTaskHost::default()),
        test_ports(),
    );
    establish(&registry, context);

    for sequence in 1..=64 {
        clock.advance(Duration::from_secs(1));
        let request = RenewQueryExecutionLease::new(
            TaskOperationId::new_v7(),
            context,
            LeaseSequence::new(sequence),
            LeaseValidFor::new(Duration::from_secs(10)).expect("valid context lease"),
        );
        let renewed =
            registry.update_query_context(&UpdateQueryContext::RenewLease(request.clone()));
        assert_eq!(renewed.outcome(), OperationOutcome::Accepted, "{renewed:?}");
        let replay = registry.update_query_context(&UpdateQueryContext::RenewLease(request));
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
        assert_eq!(registry.indexed_lease_count(), 1);
        assert_eq!(registry.context_state(context), QueryContextState::Active);
    }

    clock.advance(Duration::from_secs(9));
    assert_eq!(registry.advance_deadlines().leases_expired, 0);
    assert_eq!(registry.indexed_lease_count(), 1);
    clock.advance(Duration::from_secs(1));
    assert_eq!(registry.advance_deadlines().leases_expired, 1);
    assert_eq!(registry.indexed_lease_count(), 0);
    assert_eq!(
        registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[test]
fn adapter_gate_blocks_runnable_submission_until_it_releases() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(41, 42),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let task_host = Arc::new(TestTaskHost::default());
    let gate = Arc::new(BlockingCreationGate::held());
    let registry = TaskExecutionRegistry::new_with_task_creation_gate(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
        Arc::clone(&gate) as Arc<dyn TaskCreationGate>,
    );

    establish(&registry, context);
    let request = CreateTask::try_new(
        TaskOperationId::new_v7(),
        context,
        descriptor(task(execution, backend)),
        Vec::new(),
    )
    .expect("legal create");

    let create_registry = Arc::clone(&registry);
    let create =
        std::thread::spawn(move || create_registry.create_task(&request, body(RESULT_PLAN)));
    gate.wait_until_waiter_entered();
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 0);
    assert!(
        task_host.prepared_bodies().is_empty(),
        "a create held at the adapter gate has not won anything yet"
    );

    gate.release();
    let receipt = create.join().expect("create thread");
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
    assert_eq!(task_host.submitted.load(Ordering::SeqCst), 1);
}

#[test]
fn cancel_rejected_during_creation_does_not_stop_the_later_runnable() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let execution = QueryExecutionId::new(
        QueryId::new(51, 52),
        AttemptId::new(1).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let context = QueryContextRef::new(execution, frontend, backend);
    let gate = Arc::new(InstallGate::held());
    let host = Arc::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let registry = Arc::new(TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    ));
    establish(&registry, context);
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        backend,
    );
    let descriptor = TaskDescriptor::try_new(
        identity,
        UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).expect("nonzero dop"),
        vec![PlanNodeId::new(1).expect("nonnegative node")],
        ExchangeTopology::default(),
    )
    .expect("legal descriptor");
    let create_request =
        CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
            .expect("legal create");
    let creator = Arc::clone(&registry);
    let create =
        std::thread::spawn(move || creator.create_task(&create_request, body(RESULT_PLAN)));
    gate.wait_until_entered();

    let premature = registry.cancel_task(&CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    ));
    assert_eq!(premature.outcome(), OperationOutcome::InvalidStateOrRequest);
    assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);
    gate.release();
    assert_eq!(
        create.join().expect("create thread").outcome(),
        OperationOutcome::Accepted
    );
    assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 0);

    let accepted = registry.cancel_task(&CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    ));
    assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
    assert_eq!(host.cancel_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn lost_create_acknowledgement_replays_without_resubmitting_the_runnable() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(51);
    establish(&fixture.registry, fixture.context(execution));
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());

    let first = fixture.registry.create_task(&request, body(RESULT_PLAN));
    let replay = fixture.registry.create_task(&request, body(RESULT_PLAN));
    assert_eq!(first.outcome(), OperationOutcome::Accepted, "{first:?}");
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(replay.acknowledgement(), first.acknowledgement());
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)],
        "the replay's body never reached the execution host"
    );
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_exact_creates_share_one_runnable_and_receipt() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(61);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let descriptor = descriptor(task(execution, fixture.backend));

    let mut creates = Vec::new();
    for _ in 0..4 {
        let registry = Arc::clone(&fixture.registry);
        let descriptor = descriptor.clone();
        creates.push(std::thread::spawn(move || {
            registry.create_task(
                &CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())
                    .expect("legal create"),
                body(RESULT_PLAN),
            )
        }));
    }
    let receipts: Vec<_> = creates
        .into_iter()
        .map(|create| create.join().expect("create thread"))
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
        3,
        "{receipts:?}"
    );
    for receipt in &receipts {
        assert_eq!(receipt.acknowledgement(), accepted[0].acknowledgement());
    }
    assert_eq!(fixture.task_host.prepared_bodies().len(), 1);
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture
            .task_host
            .capabilities_installed
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.registry.counters().tasks_created, 1);
}

/// A request that names a live identity is answered by that task, whatever
/// body it carries.
///
/// The replay here changes every fact a body can carry -- the kernel key, the
/// parallelism, the split plan node, the initial domains, and a static plan
/// the host would prepare as a different sink -- and it is still answered
/// with the original receipt. None of it reaches the host, and none of it is
/// adopted: the original kernel key, the original result ownership, and the
/// original split plan node all still govern the task.
#[test]
fn a_live_identity_replays_its_original_task_whatever_body_a_request_carries() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(71);
    establish(&fixture.registry, fixture.context(execution));
    let identity = task(execution, fixture.backend);

    let accepted = fixture.registry.create_task(
        &fixture.create(
            descriptor_with(identity, UniqueId::new(1, 1), 1, 1),
            Vec::new(),
        ),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );

    let replay = fixture.registry.create_task(
        &fixture.create(
            descriptor_with(identity, UniqueId::new(9, 9), 4, 2),
            vec![split_batch(2, 1, 1)],
        ),
        body(STREAM_PLAN),
    );
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(
        replay.acknowledgement(),
        accepted.acknowledgement(),
        "the replay is answered with the original entity receipt"
    );
    assert_ne!(
        replay.operation_id(),
        accepted.operation_id(),
        "the answer correlates the replay's own operation"
    );

    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)],
        "only the winner's body was ever interpreted"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture.task_host.domains_applied.load(Ordering::SeqCst),
        0,
        "the replay's initial domains were never applied"
    );

    // The original prepared facts still govern the task: it is the result
    // owner under its original kernel key, not an exchange producer.
    let RootResultRoute::Serve(binding) = fixture.registry.root_result_route(identity) else {
        panic!("the original result owner still serves its result");
    };
    assert_eq!(binding.kernel_key(), UniqueId::new(1, 1));

    // And the original descriptor still governs its domains: the split node
    // it froze is accepted, and the one only the replay named is refused.
    let original_node = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_batch(1, 1, 1)],
        )
        .expect("legal update"),
    );
    assert_eq!(
        original_node.outcome(),
        OperationOutcome::Accepted,
        "{original_node:?}"
    );
    let replayed_node = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_batch(2, 1, 1)],
        )
        .expect("legal update"),
    );
    assert_eq!(
        replayed_node.outcome(),
        OperationOutcome::DomainConflict,
        "{replayed_node:?}"
    );
    assert!(fixture.registry.has_live_task(identity));
}

/// A request that names an identity whose creation is in progress waits for
/// that round, whatever body it carries.
///
/// It never answers before the round settles, never preempts the owner, and
/// never has its own body interpreted. When the round succeeds it receives
/// the owner's receipt.
#[test]
fn a_changed_body_converges_on_the_creation_in_progress_without_preempting_it() {
    let install_gate = Arc::new(InstallGate::held());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&install_gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(81);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);

    let owner_request = fixture.create(
        descriptor_with(identity, UniqueId::new(1, 1), 1, 1),
        Vec::new(),
    );
    let owner_registry = Arc::clone(&fixture.registry);
    let owner =
        std::thread::spawn(move || owner_registry.create_task(&owner_request, body(RESULT_PLAN)));
    install_gate.wait_until_entered();

    let changed = || {
        fixture.create(
            descriptor_with(identity, UniqueId::new(9, 9), 4, 2),
            vec![split_batch(2, 1, 1)],
        )
    };
    let waiting_replay = fixture.registry.create_task_with_local_wait_cap(
        &changed(),
        body(STREAM_PLAN),
        Duration::ZERO,
    );
    assert_eq!(
        waiting_replay.outcome(),
        OperationOutcome::OperationTimedOut,
        "a converging create is bounded by its own deadline and never answers early"
    );
    assert!(waiting_replay.acknowledgement().is_none());

    let follower_registry = Arc::clone(&fixture.registry);
    let follower_request = changed();
    let follower = std::thread::spawn(move || {
        follower_registry.create_task(&follower_request, body(STREAM_PLAN))
    });
    while fixture.registry.in_flight_operations(context) < 2 {
        std::thread::yield_now();
    }

    install_gate.release();
    let accepted = owner.join().expect("owner create thread");
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    let converged = follower.join().expect("follower create thread");
    assert_eq!(
        converged.outcome(),
        OperationOutcome::Idempotent,
        "{converged:?}"
    );
    assert_eq!(converged.acknowledgement(), accepted.acknowledgement());

    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)],
        "neither converging body was interpreted"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 0);
    assert!(matches!(
        fixture.registry.root_result_route(identity),
        RootResultRoute::Serve(_)
    ));
}

#[test]
fn a_create_rejected_after_context_closure_reaps_without_a_published_status() {
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let clock = Arc::new(ManualClock::new());
    let gate = Arc::new(InstallGate::held());
    let host = Arc::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let registry = Arc::new(TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(TestContextHost),
        Arc::clone(&host) as Arc<dyn TaskExecutionHost>,
        test_ports(),
    ));
    let execution = execution(82);
    let context = QueryContextRef::new(execution, frontend, backend);
    establish(&registry, context);
    let identity = task(execution, backend);
    let source = registry
        .status_source(context)
        .expect("established status source");
    let request = CreateTask::try_new(
        TaskOperationId::new_v7(),
        context,
        descriptor(identity),
        Vec::new(),
    )
    .expect("legal create");
    let creating = Arc::clone(&registry);
    let create = std::thread::spawn(move || creating.create_task(&request, body(RESULT_PLAN)));
    gate.wait_until_entered();

    clock.advance(Duration::from_secs(10));
    assert_eq!(registry.advance_deadlines().leases_expired, 1);
    gate.release();
    let rejected = create.join().expect("create thread");
    assert_eq!(rejected.outcome(), OperationOutcome::ContextTerminalReceipt);
    assert!(rejected.acknowledgement().is_none());
    assert!(source.latest(identity).is_none());

    let reporter = host.reporters.lock().expect("test reporters")[0].clone();
    reporter.release_output();
    reporter.note_actual_stopped();
    reporter.note_resources_converged();
    assert_eq!(registry.advance_deadlines().tasks_retired, 1);
    assert!(source.latest(identity).is_none());

    clock.advance(Duration::from_secs(121));
    assert_eq!(registry.advance_deadlines().tasks_reaped, 1);
    assert!(source.latest(identity).is_none());
    assert!(matches!(
        registry.root_result_route(identity),
        RootResultRoute::Gone
    ));
    assert_eq!(
        source.observe(
            novarocks_execution_contract::task_execution::status::TaskStatusCursor::unobserved(
                identity,
            ),
        ),
        crate::observation::CursorObservation::Unknown,
    );
    assert!(source.next_task_event().is_none());
}

/// Initial-domain membership is the winner's own check, run under its
/// reservation and before any install.
///
/// A refusal rolls the reservation back completely, so the identity is not
/// spent and a later legal create of it wins a new round.
#[test]
fn an_initial_domain_the_descriptor_never_froze_is_refused_after_reservation_and_rolled_back() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(21);
    establish(&fixture.registry, fixture.context(execution));
    let identity = task(execution, fixture.backend);

    let refused = fixture.registry.create_task(
        &fixture.create(descriptor(identity), vec![split_batch(9, 1, 1)]),
        body(RESULT_PLAN),
    );
    assert_eq!(
        refused.outcome(),
        OperationOutcome::InvalidStateOrRequest,
        "{refused:?}"
    );
    assert!(refused.acknowledgement().is_none());
    assert!(
        refused
            .detail()
            .is_some_and(|detail| detail.as_str().contains("did not freeze")),
        "{refused:?}"
    );
    assert!(
        fixture.task_host.prepared_bodies().is_empty(),
        "membership is refused before the host interprets anything"
    );
    assert_eq!(fixture.registry.counters().creations_rolled_back, 1);
    assert!(!fixture.registry.has_live_task(identity));

    let accepted = fixture.registry.create_task(
        &fixture.create(descriptor(identity), vec![split_batch(1, 1, 1)]),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 1);
    assert!(fixture.registry.has_live_task(identity));
}

/// A first winner whose body the host refuses is rolled back completely.
///
/// The host undid its own preparation before refusing, so the owner removes
/// nothing on its behalf; the identity stays unspent, and a later request with
/// a legal body wins the next round.
#[test]
fn a_refused_first_preparation_rolls_back_so_a_later_legal_create_wins() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31);
    establish(&fixture.registry, fixture.context(execution));
    let identity = task(execution, fixture.backend);

    let refused = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(REFUSED_PLAN),
    );
    assert_eq!(
        refused.outcome(),
        OperationOutcome::InvalidStateOrRequest,
        "{refused:?}"
    );
    assert!(refused.acknowledgement().is_none());
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0,
        "a refused install recorded nothing for the owner to undo"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.registry.counters().creations_rolled_back, 1);
    assert!(!fixture.registry.has_live_task(identity));

    let accepted = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![
            Bytes::from_static(REFUSED_PLAN),
            Bytes::from_static(RESULT_PLAN)
        ],
        "each winning round interprets exactly its own body"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);

    // The failed round fixed nothing: the task that exists now answers.
    let replay = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(REFUSED_PLAN),
    );
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(replay.acknowledgement(), accepted.acknowledgement());
    assert_eq!(fixture.task_host.prepared_bodies().len(), 2);
}

/// A follower waiting on a round that fails either observes that round's
/// failure or, once the reservation is gone, wins the next round itself.
///
/// Both are legal: the protocol does not promise every follower the previous
/// round's failure. What it does promise is that no answer is a partial
/// success and that at most one runnable exists afterwards.
#[test]
fn a_follower_of_a_failed_round_observes_its_failure_or_wins_the_next_round() {
    let install_gate = Arc::new(InstallGate::held());
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&install_gate)),
            ..TestTaskHost::default()
        },
        |config| {
            // Only a settled round wakes the follower.
            config.gate_poll_interval = Duration::from_secs(3600);
        },
    );
    let execution = execution(33);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);

    let owner_request = fixture.create(descriptor(identity), Vec::new());
    let owner_registry = Arc::clone(&fixture.registry);
    let owner =
        std::thread::spawn(move || owner_registry.create_task(&owner_request, body(REFUSED_PLAN)));
    install_gate.wait_until_entered();

    let follower_request = fixture.create(descriptor(identity), Vec::new());
    let follower_registry = Arc::clone(&fixture.registry);
    let follower = std::thread::spawn(move || {
        follower_registry.create_task(&follower_request, body(RESULT_PLAN))
    });
    while fixture.registry.in_flight_operations(context) < 2 {
        std::thread::yield_now();
    }

    install_gate.release();
    let failed = owner.join().expect("owner create thread");
    assert_eq!(
        failed.outcome(),
        OperationOutcome::InvalidStateOrRequest,
        "{failed:?}"
    );
    let followed = follower.join().expect("follower create thread");
    match followed.outcome() {
        OperationOutcome::Accepted => {
            assert_eq!(
                fixture.task_host.prepared_bodies(),
                vec![
                    Bytes::from_static(REFUSED_PLAN),
                    Bytes::from_static(RESULT_PLAN)
                ],
                "the follower won the next round with its own body"
            );
        }
        OperationOutcome::InvalidStateOrRequest => {
            assert_eq!(followed.detail(), failed.detail());
            assert_eq!(fixture.task_host.prepared_bodies().len(), 1);
            let retried = fixture.registry.create_task(
                &fixture.create(descriptor(identity), Vec::new()),
                body(RESULT_PLAN),
            );
            assert_eq!(retried.outcome(), OperationOutcome::Accepted, "{retried:?}");
        }
        other => panic!("a follower of a failed round answered {other:?}: {followed:?}"),
    }
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert!(fixture.registry.has_live_task(identity));
}

/// An identity is only ever matched as a whole, under its exact context.
///
/// Each of these requests names the created task's stage and task ids, and
/// none of them may read its receipt: another frontend incarnation is fenced,
/// another backend process is not this owner's, and another attempt is a
/// different query execution whose context was never established here.
#[test]
fn a_create_for_another_scope_never_reads_this_identitys_receipt() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(11);
    establish(&fixture.registry, fixture.context(execution));
    let identity = task(execution, fixture.backend);
    let accepted = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(accepted.outcome(), OperationOutcome::Accepted);

    let replaced_frontend =
        QueryContextRef::new(execution, FrontendProcessId::new_v7(), fixture.backend);
    let foreign_frontend = fixture.registry.create_task(
        &CreateTask::try_new(
            TaskOperationId::new_v7(),
            replaced_frontend,
            descriptor(identity),
            Vec::new(),
        )
        .expect("the identity agrees with the replaced frontend's context"),
        body(RESULT_PLAN),
    );
    assert_eq!(
        foreign_frontend.outcome(),
        OperationOutcome::IdentityMismatch,
        "{foreign_frontend:?}"
    );
    assert!(foreign_frontend.acknowledgement().is_none());

    let other_backend = BackendProcessId::new_v7();
    let foreign_backend = fixture.registry.create_task(
        &CreateTask::try_new(
            TaskOperationId::new_v7(),
            QueryContextRef::new(execution, fixture.frontend, other_backend),
            descriptor(task(execution, other_backend)),
            Vec::new(),
        )
        .expect("a legal create for another process"),
        body(RESULT_PLAN),
    );
    assert_eq!(
        foreign_backend.outcome(),
        OperationOutcome::IdentityMismatch,
        "{foreign_backend:?}"
    );
    assert!(foreign_backend.acknowledgement().is_none());

    let next_attempt = QueryExecutionId::new(
        execution.query_id(),
        AttemptId::new(2).expect("nonzero attempt"),
    )
    .expect("nonzero query id");
    let other_attempt = fixture.registry.create_task_with_local_wait_cap(
        &fixture.create(descriptor(task(next_attempt, fixture.backend)), Vec::new()),
        body(RESULT_PLAN),
        Duration::ZERO,
    );
    assert_eq!(
        other_attempt.outcome(),
        OperationOutcome::OperationTimedOut,
        "another attempt waits for its own context rather than reusing this one: {other_attempt:?}"
    );
    assert!(other_attempt.acknowledgement().is_none());

    assert_eq!(fixture.task_host.prepared_bodies().len(), 1);
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert!(fixture.registry.has_live_task(identity));
}

/// A replay is not a domain update and not a renewal.
///
/// After the task's split domain advanced past its initial batch, replaying
/// the original create re-delivers nothing -- neither the initial split batch
/// nor the initial edge open -- rolls no watermark back, and does not touch
/// the context lease; it is answered with the receipt the creation first
/// produced.
#[test]
fn a_replay_after_domains_advanced_redelivers_nothing_and_renews_nothing() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(13);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
    let producer = TaskDescriptor::try_new(
        identity,
        UniqueId::new(1, 1),
        std::num::NonZeroUsize::new(1).expect("nonzero dop"),
        vec![PlanNodeId::new(1).expect("nonnegative node")],
        ExchangeTopology::try_new(
            vec![
                ExchangeEdge::try_new(
                    edge,
                    FragmentNodeId::new(7),
                    DataStreamPartitionType::Unpartitioned,
                    vec![ExchangeDestination::new(
                        TaskIdentity::new(
                            execution,
                            StageId::new(2).expect("nonzero stage"),
                            TaskId::new(1).expect("nonzero task"),
                            fixture.backend,
                        ),
                        UniqueId::new(2, 1),
                        RuntimeEndpoint::new("127.0.0.1", 9060).expect("endpoint"),
                        FragmentNodeId::new(7),
                    )],
                    0,
                    NonZeroU32::new(1).expect("nonzero senders"),
                )
                .expect("legal edge"),
            ],
            Vec::new(),
        )
        .expect("legal topology"),
    )
    .expect("legal descriptor");
    let request = fixture.create(
        producer,
        vec![
            split_batch(1, 1, 1),
            TaskDomainUpdate::OpenExchangeEdges {
                version: EdgeOpenVersion::FIRST,
                edges: vec![edge],
            },
        ],
    );

    let accepted = fixture.registry.create_task(&request, body(STREAM_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 2);
    let advanced = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_batch(1, 2, 2)],
        )
        .expect("legal update"),
    );
    assert_eq!(
        advanced.outcome(),
        OperationOutcome::Accepted,
        "{advanced:?}"
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 3);
    let lease = fixture
        .registry
        .installed_lease(context)
        .expect("an active context holds a lease");

    let replay = fixture.registry.create_task(&request, body(STREAM_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(
        replay.acknowledgement(),
        accepted.acknowledgement(),
        "the replay reports the creation's own receipt, not a re-application"
    );
    assert_eq!(
        fixture.task_host.domains_applied.load(Ordering::SeqCst),
        3,
        "neither the initial split batch nor the initial edge open was delivered again"
    );
    assert_eq!(
        fixture.registry.installed_lease(context),
        Some(lease),
        "a replay never renews the context lease"
    );

    // The advanced watermark still stands: the next batch after it applies.
    let next = fixture.registry.update_task(
        &UpdateTask::try_new(
            TaskOperationId::new_v7(),
            identity,
            vec![split_batch(1, 3, 3)],
        )
        .expect("legal update"),
    );
    assert_eq!(next.outcome(), OperationOutcome::Accepted, "{next:?}");
    assert_eq!(fixture.task_host.prepared_bodies().len(), 1);
}
