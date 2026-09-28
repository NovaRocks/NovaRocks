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
    DataStreamPartitionType, ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource,
    ExchangeTopology, FragmentNodeId, FragmentSinkKind, RuntimeEndpoint, TaskDescriptor,
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
    AbortQueryContext, AcquireQueryContextAdmissionTicket, CancelTask, CreateTask,
    CredentialUpdate, EstablishQueryContext, OperationOutcome, QuiesceQueryContext,
    ReleaseQueryContext, RenewQueryExecutionLease, SplitAssignmentIntent, TaskDomainUpdate,
    UpdateQueryContext, UpdateTask,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, TaskFailureCategory, TaskFailurePhase, TaskState, TerminationDetail,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_types::identity::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use novarocks_types::{NativeCompatibilityId, UniqueId};

use crate::{
    HostRejection, ManualClock, QueryContextHost, ReleasedContextEvidence, RootResultRoute,
    RunnableTask, SharedFactsRequest, TaskCreationGate, TaskExecutionHost, TaskExecutionMetrics,
    TaskExecutionPorts, TaskExecutionRegistry, TaskExecutionRegistryConfig,
    TaskInboundCapabilities, TaskProtocolEvent, TaskProtocolObserver, TaskResultLifecycle,
    TaskStatusReporter, WorkerMonotonicClock,
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
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
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

struct BlockingContextHost {
    gate: Arc<InstallGate>,
}

impl QueryContextHost for BlockingContextHost {
    fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        self.gate.wait_for_release();
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
    retain_normal_close: bool,
    inbound_capabilities: Option<Arc<TaskInboundCapabilities>>,
    install_gate: Option<Arc<InstallGate>>,
    prepared_bodies: Mutex<Vec<Bytes>>,
    receivers_installed: AtomicUsize,
    receivers_removed: AtomicUsize,
    capabilities_installed: AtomicUsize,
    submitted: AtomicUsize,
    domains_applied: AtomicUsize,
    reporters: Mutex<Vec<TaskStatusReporter>>,
    retired_executions: Mutex<Vec<QueryContextRef>>,
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

    fn retire_context_execution(&self, context: QueryContextRef) {
        self.retired_executions
            .lock()
            .expect("retired executions")
            .push(context);
    }

    fn forget_context_admission(&self, context: QueryContextRef) {
        if let Some(capabilities) = &self.inbound_capabilities {
            capabilities.forget_context(context);
        }
    }

    fn retains_normal_close(&self, _context: QueryContextRef) -> bool {
        self.retain_normal_close
    }

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

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        if let Some(capabilities) = &self.inbound_capabilities {
            capabilities.install(Arc::new(descriptor.clone()))?;
        }
        self.capabilities_installed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn reserve_inbound_close_capacity(
        &self,
        descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        if let Some(capabilities) = &self.inbound_capabilities {
            capabilities.reserve_normal_close(descriptor)?;
        }
        Ok(())
    }

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        if let Some(capabilities) = &self.inbound_capabilities {
            capabilities.remove(descriptor);
        }
    }

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
    clock: Arc<ManualClock>,
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
        let clock = Arc::new(ManualClock::new());
        let registry = TaskExecutionRegistry::new(
            config,
            Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(TestContextHost),
            Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
            test_ports(),
        );
        Self {
            registry,
            task_host,
            backend,
            frontend: FrontendProcessId::new_v7(),
            clock,
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
fn cumulative_context_capacity_rejects_a_new_identity_without_reinterpreting_a_replay() {
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.max_tasks_per_context = 1;
    });
    let execution = execution(52);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let first = task(execution, fixture.backend);
    let first_request = fixture.create(descriptor(first), Vec::new());
    let accepted = fixture
        .registry
        .create_task(&first_request, body(RESULT_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );

    let second = TaskIdentity::new(
        execution,
        StageId::new(1).expect("nonzero stage"),
        TaskId::new(2).expect("nonzero task"),
        fixture.backend,
    );
    let rejected = fixture.registry.create_task(
        &fixture.create(descriptor(second), Vec::new()),
        body(STREAM_PLAN),
    );
    assert_eq!(
        rejected.outcome(),
        OperationOutcome::ResourceExhausted,
        "{rejected:?}"
    );
    assert!(rejected.acknowledgement().is_none());
    assert_eq!(
        fixture
            .registry
            .create_task(&first_request, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Idempotent,
        "the occupied capacity must not change an existing identity's answer"
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)],
        "the rejected body and replay body must not reach the host"
    );
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Active
    );
}

#[test]
fn backend_active_capacity_rejects_another_context_before_host_preparation() {
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.max_active_tasks_per_backend = 1;
    });
    let first_execution = execution(53);
    let second_execution = execution(54);
    let first_context = fixture.context(first_execution);
    let second_context = fixture.context(second_execution);
    establish(&fixture.registry, first_context);
    establish(&fixture.registry, second_context);

    let first = task(first_execution, fixture.backend);
    let accepted = fixture.registry.create_task(
        &fixture.create(descriptor(first), Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );

    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            first_context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted, "{abort:?}");
    assert!(
        fixture.registry.has_live_task(first),
        "aborting an old attempt cannot release its task slot before physical convergence"
    );

    let second = task(second_execution, fixture.backend);
    let rejected = fixture.registry.create_task(
        &fixture.create(descriptor(second), Vec::new()),
        body(STREAM_PLAN),
    );
    assert_eq!(
        rejected.outcome(),
        OperationOutcome::ResourceExhausted,
        "{rejected:?}"
    );
    assert!(rejected.acknowledgement().is_none());
    assert_eq!(
        fixture.registry.context_state(second_context),
        QueryContextState::Active
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)],
        "a hard capacity rejection cannot own a runnable or interpret its body"
    );
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

#[test]
fn accepted_create_replays_before_preparation_and_cancel_stops_before_submit() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(991);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());

    let accepted = fixture
        .registry
        .accept_create_task(&request, body(RESULT_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert_eq!(
        accepted.acknowledgement().expect("accepted status").state(),
        TaskState::Planned
    );
    gate.wait_until_entered();
    let replay = fixture
        .registry
        .accept_create_task(&request, body(STREAM_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);

    let stopped = fixture.registry.cancel_task(&CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    ));
    assert_eq!(stopped.outcome(), OperationOutcome::Accepted, "{stopped:?}");
    gate.release();
    wait_for_accepted_state(&fixture.registry, &request, TaskState::Canceled);
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)]
    );
}

#[test]
fn quiesce_fences_absent_context_before_establish() {
    let fixture = Fixture::new(TestTaskHost::default());
    let context = fixture.context(execution(1101));
    let source = fixture.registry.status_source(context);
    let quiesce = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
    let first = fixture.registry.quiesce_query_context(&quiesce);
    assert_eq!(first.outcome(), OperationOutcome::Accepted);
    let cut = first.acknowledgement().expect("quiesce cut");
    assert_eq!(cut.state(), QueryContextState::Quiescing);
    assert!(cut.accepted_tasks().is_empty());
    assert_eq!(cut.fence_version(), 1);
    let source = fixture
        .registry
        .status_source(context)
        .or(source)
        .expect("quiesce source");
    let subscription = source
        .begin_covered_subscription(context, 1, &[], &[], None, &[], 1)
        .unwrap();
    let selected = subscription.select_next(4096, |_| 1).unwrap().unwrap();
    assert!(
        matches!(&selected.frame, crate::observation::CoveredObservationFrame::CatchUp(crate::observation::CoveredObservationFact::Quiesce(receipt)) if receipt == cut)
    );
    subscription.note_delivered(selected.delivery_id).unwrap();
    let replay = fixture.registry.quiesce_query_context(&quiesce);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(replay.acknowledgement(), Some(cut));
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Quiescing
    );
}

#[test]
fn first_quiesce_is_published_to_an_existing_covered_subscription() {
    let fixture = Fixture::new(TestTaskHost::default());
    let context = fixture.context(execution(1110));
    establish(&fixture.registry, context);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established source");
    let subscription = source
        .begin_covered_subscription(context, 1, &[], &[], None, &[], 1)
        .unwrap();
    let initial = subscription.select_next(4096, |_| 1).unwrap().unwrap();
    assert!(matches!(
        initial.frame,
        crate::observation::CoveredObservationFrame::CatchUpComplete { .. }
    ));
    subscription.note_delivered(initial.delivery_id).unwrap();
    let request = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
    let receipt = fixture.registry.quiesce_query_context(&request);
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    let selected = subscription.select_next(4096, |_| 1).unwrap().unwrap();
    assert!(
        matches!(&selected.frame, crate::observation::CoveredObservationFrame::Live { fact: crate::observation::CoveredObservationFact::Quiesce(cut), .. } if Some(cut) == receipt.acknowledgement())
    );
    subscription.note_delivered(selected.delivery_id).unwrap();
    assert_eq!(
        fixture.registry.quiesce_query_context(&request).outcome(),
        OperationOutcome::Idempotent
    );
    assert!(
        subscription.select_next(4096, |_| 1).unwrap().is_none(),
        "replay must not publish another control fact"
    );
}

#[test]
fn normal_close_record_survives_retained_context_capacity_pressure_until_horizon() {
    let fixture = Fixture::with_config(
        TestTaskHost {
            retain_normal_close: true,
            ..TestTaskHost::default()
        },
        |config| config.retained_context_capacity = 1,
    );
    let first = fixture.context(execution(1120));
    let second = fixture.context(execution(1121));
    for context in [first, second] {
        establish(&fixture.registry, context);
        let quiesce = fixture
            .registry
            .quiesce_query_context(&QuiesceQueryContext::new(
                TaskOperationId::new_v7(),
                context,
            ));
        assert_eq!(quiesce.outcome(), OperationOutcome::Accepted);
        let release = fixture
            .registry
            .release_query_context(&ReleaseQueryContext::new(
                TaskOperationId::new_v7(),
                context,
            ));
        assert_eq!(release.outcome(), OperationOutcome::Accepted);
    }
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(first),
        QueryContextState::TerminalRetained
    );
    assert_eq!(
        fixture.registry.context_state(second),
        QueryContextState::TerminalRetained
    );
    fixture
        .clock
        .advance(crate::RequestHorizon::DEFAULT.total());
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(first),
        QueryContextState::Gone
    );
    assert_eq!(
        fixture.registry.context_state(second),
        QueryContextState::Gone
    );
}

#[test]
fn quiesce_wins_over_inflight_establish_without_failure_latch() {
    let backend = BackendProcessId::new_v7();
    let context = QueryContextRef::new(execution(1104), FrontendProcessId::new_v7(), backend);
    let gate = Arc::new(InstallGate::held());
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
        Arc::new(BlockingContextHost {
            gate: Arc::clone(&gate),
        }),
        Arc::new(TestTaskHost::default()),
        test_ports(),
    );
    let ticket = registry
        .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).expect("ticket lease"),
            NativeCompatibilityId::new([0x71; 32]),
            registry.admission_epoch_capability(),
        ))
        .acknowledgement()
        .expect("ticket")
        .ticket_id();
    let request = UpdateQueryContext::Establish(EstablishQueryContext::new(
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
        LeaseValidFor::new(Duration::from_secs(10)).expect("context lease"),
    ));
    let running = Arc::clone(&registry);
    let handle = std::thread::spawn(move || running.update_query_context(&request));
    gate.wait_until_entered();
    let cut = registry.quiesce_query_context(&QuiesceQueryContext::new(
        TaskOperationId::new_v7(),
        context,
    ));
    assert_eq!(cut.outcome(), OperationOutcome::Accepted);
    assert!(
        cut.acknowledgement()
            .expect("cut")
            .accepted_tasks()
            .is_empty()
    );
    gate.release();
    let establish = handle.join().expect("establish thread");
    assert_eq!(
        establish.outcome(),
        OperationOutcome::ContextTerminalReceipt
    );
    assert_eq!(
        registry.context_state(context),
        QueryContextState::Quiescing
    );
    assert_eq!(registry.termination_cause(context), None);
}

#[test]
fn quiesce_membership_includes_accepted_preparation_and_stops_it() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(1102);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let create = fixture.create(descriptor(identity), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&create, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let request = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
    let first = fixture.registry.quiesce_query_context(&request);
    assert_eq!(first.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        first.acknowledgement().expect("cut").accepted_tasks(),
        &[identity]
    );
    assert_eq!(fixture.registry.termination_cause(context), None);
    gate.release();
    wait_for_accepted_state(&fixture.registry, &create, TaskState::Canceled);
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    let replay = fixture.registry.quiesce_query_context(&request);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        replay.acknowledgement().expect("replay").accepted_tasks(),
        &[identity]
    );
    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted);
    let after_abort = fixture.registry.quiesce_query_context(&request);
    assert_eq!(
        after_abort
            .acknowledgement()
            .expect("retained cut")
            .accepted_tasks(),
        &[identity]
    );
}

#[test]
fn normal_release_requires_quiesce_and_keeps_lease_until_release() {
    let fixture = Fixture::new(TestTaskHost::default());
    let context = fixture.context(execution(1103));
    establish(&fixture.registry, context);
    let release = ReleaseQueryContext::new(TaskOperationId::new_v7(), context);
    assert_eq!(
        fixture.registry.release_query_context(&release).outcome(),
        OperationOutcome::InvalidStateOrRequest
    );
    let lease = fixture
        .registry
        .installed_lease(context)
        .expect("active lease");
    let quiesce = fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(quiesce.outcome(), OperationOutcome::Accepted);
    assert_eq!(fixture.registry.installed_lease(context), Some(lease));
    let released = fixture.registry.release_query_context(&release);
    assert_eq!(
        released.outcome(),
        OperationOutcome::Accepted,
        "{released:?}"
    );
    assert_eq!(fixture.registry.termination_cause(context), None);
}

#[test]
fn quiesce_ack_precedes_live_stop_fanout() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(1105);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let create = fixture.create(descriptor(identity), Vec::new());
    assert_eq!(
        fixture
            .registry
            .create_task(&create, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    let cut = fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(cut.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        cut.acknowledgement().expect("cut").accepted_tasks(),
        &[identity]
    );
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 0);
    fixture.registry.advance_deadlines();
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 1);
    fixture.registry.advance_deadlines();
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn abort_of_accepted_preparation_stops_before_submit_and_keeps_identity() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(997);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&request, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let source = fixture
        .registry
        .status_source(context)
        .expect("accepted source");
    assert_eq!(
        source
            .latest(identity)
            .expect("published acceptance")
            .state(),
        TaskState::Planned
    );
    let abort = fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(abort.outcome(), OperationOutcome::Accepted, "{abort:?}");
    gate.release();
    wait_for_accepted_state(&fixture.registry, &request, TaskState::Aborted);
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(
        source
            .latest(identity)
            .expect("accepted task remains observable after closure")
            .state(),
        TaskState::Aborted,
    );
}

#[test]
fn accepted_preparation_capacity_rejects_before_a_second_task_is_owned() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        },
        |config| {
            config.max_preparing_tasks = 1;
            config.max_preparing_bytes = 1024;
        },
    );
    let execution = execution(992);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let first = fixture.create(descriptor(task(execution, fixture.backend)), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&first, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let second_identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("stage"),
        TaskId::new(2).expect("task"),
        fixture.backend,
    );
    let second = fixture.create(descriptor(second_identity), Vec::new());
    let rejected = fixture
        .registry
        .accept_create_task(&second, body(STREAM_PLAN));
    assert_eq!(
        rejected.outcome(),
        OperationOutcome::PreparationBusy,
        "{rejected:?}"
    );
    gate.release();
    wait_for_accepted_installed(&fixture.registry, &first);
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)]
    );
}

#[test]
fn accepted_preparation_preserves_context_fifo() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        },
        |config| config.max_prepare_workers = 2,
    );
    let execution = execution(994);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let first = fixture.create(descriptor(task(execution, fixture.backend)), Vec::new());
    let second_identity = TaskIdentity::new(
        execution,
        StageId::new(1).expect("stage"),
        TaskId::new(2).expect("task"),
        fixture.backend,
    );
    let second = fixture.create(descriptor(second_identity), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&first, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&second, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    gate.release();
    for _ in 0..1000 {
        if fixture.task_host.prepared_bodies().len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![
            Bytes::from_static(RESULT_PLAN),
            Bytes::from_static(STREAM_PLAN)
        ]
    );
}

#[test]
fn accepted_preparation_failure_remains_the_same_terminal_identity() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(993);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    let accepted = fixture
        .registry
        .accept_create_task(&request, body(REFUSED_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    wait_for_accepted_state(&fixture.registry, &request, TaskState::Failed);
    let replay = fixture
        .registry
        .accept_create_task(&request, body(RESULT_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert!(matches!(
        replay.acknowledgement().expect("terminal replay").termination(),
        Some(TerminationDetail::Failed(failure)) if failure.phase() == TaskFailurePhase::Preparation
    ));
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(REFUSED_PLAN)]
    );
}

#[test]
fn failed_accepted_preparation_returns_uninstalled_normal_close_reservation() {
    let capabilities = TaskInboundCapabilities::with_limits(1, 4096);
    let fixture = Fixture::new(TestTaskHost {
        inbound_capabilities: Some(Arc::clone(&capabilities)),
        ..TestTaskHost::default()
    });
    let execution = execution(994);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let source = TaskIdentity::new(
        execution,
        StageId::new(2).expect("nonzero stage"),
        TaskId::new(1).expect("nonzero task"),
        fixture.backend,
    );
    let inbound = ExchangeTopology::try_new(
        Vec::new(),
        vec![
            ExchangeInbound::try_new(
                FragmentNodeId::new(7),
                vec![ExchangeSource::new(source, UniqueId::new(30, 31), 0)],
            )
            .expect("frozen inbound"),
        ],
    )
    .expect("frozen topology");
    let make_descriptor = |task_number, kernel| {
        TaskDescriptor::try_new(
            TaskIdentity::new(
                execution,
                StageId::new(1).expect("nonzero stage"),
                TaskId::new(task_number).expect("nonzero task"),
                fixture.backend,
            ),
            kernel,
            std::num::NonZeroUsize::new(1).expect("nonzero dop"),
            Vec::new(),
            inbound.clone(),
        )
        .expect("legal descriptor")
    };
    let failed = fixture.create(make_descriptor(1, UniqueId::new(10, 11)), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&failed, body(REFUSED_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    wait_for_accepted_state(&fixture.registry, &failed, TaskState::Failed);
    assert_eq!(capabilities.len(), 0);

    let next = fixture.create(make_descriptor(2, UniqueId::new(20, 21)), Vec::new());
    let accepted = fixture
        .registry
        .accept_create_task(&next, body(RESULT_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    wait_for_accepted_installed(&fixture.registry, &next);
}

#[test]
fn accepted_create_requires_active_context_without_waiting_or_owning_identity() {
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(995);
    let context = fixture.context(execution);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    let not_ready = fixture
        .registry
        .accept_create_task(&request, body(RESULT_PLAN));
    assert_eq!(
        not_ready.outcome(),
        OperationOutcome::NotReady,
        "{not_ready:?}"
    );
    assert!(not_ready.acknowledgement().is_none());
    establish(&fixture.registry, context);
    let accepted = fixture
        .registry
        .accept_create_task(&request, body(RESULT_PLAN));
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
}

#[test]
fn one_oversized_preparation_is_long_lived_rejection() {
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.max_preparing_bytes = 1;
    });
    let execution = execution(996);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    let rejected = fixture
        .registry
        .accept_create_task(&request, body(RESULT_PLAN));
    assert_eq!(
        rejected.outcome(),
        OperationOutcome::ResourceExhausted,
        "{rejected:?}"
    );
    assert!(fixture.task_host.prepared_bodies().is_empty());
}

fn wait_for_accepted_state(
    registry: &Arc<TaskExecutionRegistry>,
    request: &CreateTask,
    expected: TaskState,
) {
    for _ in 0..1000 {
        let current = registry.accept_create_task(request, body(RESULT_PLAN));
        if current
            .acknowledgement()
            .is_some_and(|status| status.state() == expected)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("accepted task did not reach {expected:?}");
}

fn wait_for_accepted_installed(registry: &Arc<TaskExecutionRegistry>, request: &CreateTask) {
    for _ in 0..1000 {
        let current = registry.accept_create_task(request, body(RESULT_PLAN));
        if current
            .acknowledgement()
            .is_some_and(|status| status.installed())
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("accepted task did not publish Installed");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptedFenceEvent {
    AdmitSecond,
    Quiesce,
    CancelFirst,
    ReplayFirst,
    LateEstablish,
    ExpireLease,
}

fn accepted_fence_permutations(events: &[AcceptedFenceEvent]) -> Vec<Vec<AcceptedFenceEvent>> {
    fn visit(
        events: &mut [AcceptedFenceEvent],
        next: usize,
        traces: &mut Vec<Vec<AcceptedFenceEvent>>,
    ) {
        if next == events.len() {
            traces.push(events.to_vec());
            return;
        }
        for selected in next..events.len() {
            events.swap(next, selected);
            visit(events, next + 1, traces);
            events.swap(next, selected);
        }
    }
    let mut traces = Vec::new();
    visit(&mut events.to_vec(), 0, &mut traces);
    traces
}

// A failed assertion still releases the real preparation worker's rendezvous.
struct PreparationGateRelease(Arc<InstallGate>);

impl Drop for PreparationGateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn establish_for_replay(
    registry: &TaskExecutionRegistry,
    context: QueryContextRef,
) -> UpdateQueryContext {
    let ticket = registry
        .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).expect("ticket lease"),
            NativeCompatibilityId::new([0x71; 32]),
            registry.admission_epoch_capability(),
        ))
        .acknowledgement()
        .expect("admission ticket")
        .ticket_id();
    let request = UpdateQueryContext::Establish(EstablishQueryContext::new(
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
        LeaseValidFor::new(Duration::from_secs(10)).expect("context lease"),
    ));
    assert_eq!(
        registry.update_query_context(&request).outcome(),
        OperationOutcome::Accepted
    );
    request
}

fn wait_for_terminal_record(registry: &TaskExecutionRegistry, identity: TaskIdentity) {
    for _ in 0..1000 {
        if matches!(
            registry.root_result_route(identity),
            RootResultRoute::Terminal(_) | RootResultRoute::TerminalResultOwner(_)
        ) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("accepted task did not retire its actual preparation worker");
}

#[test]
fn bounded_accepted_quiesce_interleavings_freeze_membership_without_repreparing() {
    use AcceptedFenceEvent::*;
    // All 5! serial orders after the first real worker entered preparation.
    // Worker completion is held until the end: this explores control ordering,
    // not arbitrary OS scheduling or more than two accepted identities.
    let traces = accepted_fence_permutations(&[
        AdmitSecond,
        Quiesce,
        CancelFirst,
        ReplayFirst,
        LateEstablish,
    ]);
    let mut second_accepted = 0;
    let mut second_fenced = 0;
    let mut observed_cuts = 0;
    for (case, trace) in traces.iter().enumerate() {
        let gate = Arc::new(InstallGate::held());
        let _release = PreparationGateRelease(Arc::clone(&gate));
        let fixture = Fixture::new(TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        });
        let execution = execution(20_000 + case as i64);
        let context = fixture.context(execution);
        let late_establish = establish_for_replay(&fixture.registry, context);
        let first_identity = task(execution, fixture.backend);
        let first = fixture.create(descriptor(first_identity), Vec::new());
        let second_identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(2).unwrap(),
            fixture.backend,
        );
        let second = fixture.create(
            descriptor_with(second_identity, UniqueId::new(2, 2), 1, 1),
            Vec::new(),
        );
        assert_eq!(
            fixture
                .registry
                .accept_create_task(&first, body(RESULT_PLAN))
                .outcome(),
            OperationOutcome::Accepted,
            "{trace:?} initial acceptance"
        );
        gate.wait_until_entered();
        let quiesce = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
        let mut accepted = vec![first_identity];
        let mut fenced = false;
        for event in trace {
            match event {
                AdmitSecond => {
                    let receipt = fixture
                        .registry
                        .accept_create_task(&second, body(STREAM_PLAN));
                    if fenced {
                        assert_eq!(
                            receipt.outcome(),
                            OperationOutcome::ContextTerminalReceipt,
                            "{trace:?} after {event:?}"
                        );
                        assert!(receipt.acknowledgement().is_none(), "{trace:?}");
                        second_fenced += 1;
                    } else {
                        assert_eq!(
                            receipt.outcome(),
                            OperationOutcome::Accepted,
                            "{trace:?} after {event:?}"
                        );
                        accepted.push(second_identity);
                        second_accepted += 1;
                    }
                }
                Quiesce => {
                    let receipt = fixture.registry.quiesce_query_context(&quiesce);
                    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{trace:?}");
                    assert_eq!(
                        receipt.acknowledgement().unwrap().accepted_tasks(),
                        accepted,
                        "{trace:?}"
                    );
                    fenced = true;
                    observed_cuts += 1;
                }
                CancelFirst => {
                    let receipt = fixture.registry.cancel_task(&CancelTask::new(
                        TaskOperationId::new_v7(),
                        first_identity,
                        CancelReason::UpstreamNoLongerNeeded,
                    ));
                    assert!(
                        matches!(
                            receipt.outcome(),
                            OperationOutcome::Accepted | OperationOutcome::Idempotent
                        ),
                        "{trace:?}: {receipt:?}"
                    );
                }
                ReplayFirst => {
                    let receipt = fixture
                        .registry
                        .accept_create_task(&first, body(STREAM_PLAN));
                    assert_eq!(receipt.outcome(), OperationOutcome::Idempotent, "{trace:?}");
                    assert_eq!(
                        receipt.acknowledgement().unwrap().identity(),
                        first_identity,
                        "{trace:?}"
                    );
                }
                LateEstablish => {
                    let receipt = fixture.registry.update_query_context(&late_establish);
                    assert_eq!(
                        receipt.outcome(),
                        if fenced {
                            OperationOutcome::ContextTerminalReceipt
                        } else {
                            OperationOutcome::Idempotent
                        },
                        "{trace:?}"
                    );
                }
                ExpireLease => unreachable!("normal traces do not expire the lease"),
            }
            assert_eq!(
                fixture.registry.termination_cause(context),
                None,
                "{trace:?} after {event:?}"
            );
            assert_eq!(
                fixture.task_host.submitted.load(Ordering::SeqCst),
                0,
                "{trace:?}"
            );
            assert_eq!(
                fixture.registry.counters().contexts_established,
                1,
                "{trace:?}"
            );
        }
        let replay = fixture.registry.quiesce_query_context(&quiesce);
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{trace:?}");
        assert_eq!(
            replay.acknowledgement().unwrap().accepted_tasks(),
            accepted,
            "{trace:?}"
        );
        gate.release();
        for identity in &accepted {
            wait_for_terminal_record(&fixture.registry, *identity);
            assert!(
                matches!(
                    fixture.registry.root_result_route(*identity),
                    RootResultRoute::Terminal(TaskState::Canceled)
                ),
                "{trace:?}: normal preparation stop must retain Canceled"
            );
        }
        assert_eq!(
            fixture.task_host.submitted.load(Ordering::SeqCst),
            0,
            "{trace:?}"
        );
        assert_eq!(
            fixture.task_host.prepared_bodies(),
            vec![Bytes::from_static(RESULT_PLAN)],
            "{trace:?}: replay or queued canceled body was prepared"
        );
        assert_eq!(
            fixture.registry.termination_cause(context),
            None,
            "{trace:?}"
        );
    }
    assert_eq!(
        (traces.len(), observed_cuts, second_accepted, second_fenced),
        (120, 120, 60, 60)
    );
    eprintln!(
        "accepted/quiesce bounded exploration: traces={} events={} cuts={observed_cuts} second_accepted={second_accepted} second_fenced={second_fenced}",
        traces.len(),
        traces.len() * 5
    );
}

#[test]
fn bounded_preparing_stop_interleavings_keep_capacity_until_actual_exit() {
    use AcceptedFenceEvent::*;
    let traces = accepted_fence_permutations(&[
        Quiesce,
        CancelFirst,
        ReplayFirst,
        LateEstablish,
        ExpireLease,
    ]);
    let mut normal_cuts = 0;
    let mut late_cuts_rejected = 0;
    let mut capacity_rejections = 0;
    for (case, trace) in traces.iter().enumerate() {
        let gate = Arc::new(InstallGate::held());
        let _release = PreparationGateRelease(Arc::clone(&gate));
        let fixture = Fixture::with_config(
            TestTaskHost {
                install_gate: Some(Arc::clone(&gate)),
                ..TestTaskHost::default()
            },
            |config| {
                config.max_preparing_tasks = 1;
                config.max_prepare_workers = 1;
            },
        );
        let query_execution = execution(21_000 + case as i64);
        let context = fixture.context(query_execution);
        let late_establish = establish_for_replay(&fixture.registry, context);
        let identity = task(query_execution, fixture.backend);
        let first = fixture.create(descriptor(identity), Vec::new());
        assert_eq!(
            fixture
                .registry
                .accept_create_task(&first, body(RESULT_PLAN))
                .outcome(),
            OperationOutcome::Accepted,
            "{trace:?}"
        );
        gate.wait_until_entered();
        let quiesce = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
        let mut expired = false;
        let mut fenced = false;
        for event in trace {
            match event {
                Quiesce => {
                    let receipt = fixture.registry.quiesce_query_context(&quiesce);
                    if expired {
                        assert_eq!(
                            receipt.outcome(),
                            OperationOutcome::ContextTerminalReceipt,
                            "{trace:?}"
                        );
                        late_cuts_rejected += 1;
                    } else {
                        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{trace:?}");
                        assert_eq!(
                            receipt.acknowledgement().unwrap().accepted_tasks(),
                            &[identity],
                            "{trace:?}"
                        );
                        fenced = true;
                        normal_cuts += 1;
                    }
                }
                CancelFirst => {
                    let receipt = fixture.registry.cancel_task(&CancelTask::new(
                        TaskOperationId::new_v7(),
                        identity,
                        CancelReason::UpstreamNoLongerNeeded,
                    ));
                    assert!(
                        matches!(
                            receipt.outcome(),
                            OperationOutcome::Accepted | OperationOutcome::Idempotent
                        ),
                        "{trace:?}: {receipt:?}"
                    );
                }
                ReplayFirst => {
                    let receipt = fixture
                        .registry
                        .accept_create_task(&first, body(STREAM_PLAN));
                    assert_eq!(receipt.outcome(), OperationOutcome::Idempotent, "{trace:?}");
                    assert_eq!(
                        receipt.acknowledgement().unwrap().identity(),
                        identity,
                        "{trace:?}"
                    );
                }
                LateEstablish => {
                    let receipt = fixture.registry.update_query_context(&late_establish);
                    assert_eq!(
                        receipt.outcome(),
                        if expired {
                            OperationOutcome::InvalidStateOrRequest
                        } else if fenced {
                            OperationOutcome::ContextTerminalReceipt
                        } else {
                            OperationOutcome::Idempotent
                        },
                        "{trace:?}"
                    );
                }
                ExpireLease => {
                    fixture.clock.advance(Duration::from_secs(10));
                    assert_eq!(
                        fixture.registry.advance_deadlines().leases_expired,
                        1,
                        "{trace:?}"
                    );
                    expired = true;
                }
                AdmitSecond => {
                    unreachable!("capacity traces admit a fresh probe after all controls")
                }
            }
            assert_eq!(
                fixture.registry.termination_cause(context),
                expired.then_some(AbortCause::LeaseExpired),
                "{trace:?} after {event:?}"
            );
            assert_eq!(
                fixture.task_host.submitted.load(Ordering::SeqCst),
                0,
                "{trace:?}"
            );
            assert!(
                matches!(
                    fixture.registry.root_result_route(identity),
                    RootResultRoute::Creating
                ),
                "{trace:?}: control verdict must not manufacture physical exit"
            );
        }
        assert_eq!(fixture.registry.counters().lease_expiries, 1, "{trace:?}");
        assert_eq!(
            fixture.registry.counters().contexts_established,
            1,
            "{trace:?}"
        );
        let probe_execution = execution(22_000 + case as i64);
        let probe_context = fixture.context(probe_execution);
        establish(&fixture.registry, probe_context);
        let probe = fixture.create(
            descriptor_with(
                task(probe_execution, fixture.backend),
                UniqueId::new(3, 3),
                1,
                1,
            ),
            Vec::new(),
        );
        let busy = fixture
            .registry
            .accept_create_task(&probe, body(STREAM_PLAN));
        assert_eq!(
            busy.outcome(),
            OperationOutcome::PreparationBusy,
            "{trace:?}: held preparation must stay charged"
        );
        capacity_rejections += 1;
        gate.release();
        wait_for_terminal_record(&fixture.registry, identity);
        wait_for_preparation_exit(&fixture.registry, identity);
        assert_eq!(
            fixture.task_host.submitted.load(Ordering::SeqCst),
            0,
            "{trace:?}: stopped preparation must not submit"
        );
        let admitted = fixture
            .registry
            .accept_create_task(&probe, body(STREAM_PLAN));
        assert_eq!(
            admitted.outcome(),
            OperationOutcome::Accepted,
            "{trace:?}: actual exit must return capacity"
        );
        wait_for_accepted_installed(&fixture.registry, &probe);
        assert_eq!(
            fixture.task_host.prepared_bodies(),
            vec![
                Bytes::from_static(RESULT_PLAN),
                Bytes::from_static(STREAM_PLAN)
            ],
            "{trace:?}"
        );
        if fenced {
            let cut = fixture.registry.quiesce_query_context(&quiesce);
            assert_eq!(cut.outcome(), OperationOutcome::Idempotent, "{trace:?}");
            assert_eq!(
                cut.acknowledgement().unwrap().accepted_tasks(),
                &[identity],
                "{trace:?}: lease expiry must not rewrite an accepted fence"
            );
        }
    }
    assert_eq!(
        (
            traces.len(),
            normal_cuts,
            late_cuts_rejected,
            capacity_rejections
        ),
        (120, 60, 60, 120)
    );
    eprintln!(
        "preparing-stop bounded exploration: traces={} events={} normal_cuts={normal_cuts} expired_before_cut={late_cuts_rejected} held_capacity_rejections={capacity_rejections}",
        traces.len(),
        traces.len() * 5
    );
}

fn wait_for_preparation_exit(registry: &Arc<TaskExecutionRegistry>, identity: TaskIdentity) {
    for _ in 0..1000 {
        if !registry.has_preparation_charge(identity) {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("preparation retained its charge after the job should have exited");
}

#[test]
fn preparation_exit_returns_temporary_capacity_while_installed_tasks_stay_active() {
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.max_preparing_tasks = 1;
        config.max_preparing_tasks_per_context = 1;
        config.max_preparing_bytes = 1024;
        config.max_prepare_workers = 1;
    });
    let query_execution = execution(30_001);
    let context = fixture.context(query_execution);
    establish(&fixture.registry, context);
    let first_identity = task(query_execution, fixture.backend);
    let first = fixture.create(descriptor(first_identity), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&first, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    wait_for_accepted_installed(&fixture.registry, &first);
    wait_for_preparation_exit(&fixture.registry, first_identity);
    let second_identity = TaskIdentity::new(
        query_execution,
        StageId::new(1).unwrap(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let second = fixture.create(
        descriptor_with(second_identity, UniqueId::new(30, 2), 1, 1),
        Vec::new(),
    );
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&second, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted,
        "a live installed predecessor must not monopolize temporary preparation capacity"
    );
    wait_for_accepted_installed(&fixture.registry, &second);
    wait_for_preparation_exit(&fixture.registry, second_identity);
    let state = fixture.registry.preparation_snapshot();
    assert_eq!(state.bytes, 0);
    assert_eq!(state.positions, 0);
    assert_eq!(state.context_positions, 0);
    assert_eq!(state.workers, 0);
    wait_for_accepted_installed(&fixture.registry, &first);
    wait_for_accepted_installed(&fixture.registry, &second);
}

#[test]
fn preparation_positions_preserve_per_context_fifo_and_global_conservation_until_exit() {
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        },
        |config| {
            config.max_preparing_tasks = 2;
            config.max_preparing_tasks_per_context = 1;
            config.max_preparing_bytes = 2048;
            config.max_prepare_workers = 1;
        },
    );
    let a = execution(30_002);
    let b = execution(30_003);
    let ca = fixture.context(a);
    let cb = fixture.context(b);
    establish(&fixture.registry, ca);
    establish(&fixture.registry, cb);
    let ai = task(a, fixture.backend);
    let first = fixture.create(descriptor(ai), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&first, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let second_identity = TaskIdentity::new(
        a,
        StageId::new(1).unwrap(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let same_context = fixture.create(
        descriptor_with(second_identity, UniqueId::new(30, 4), 1, 1),
        Vec::new(),
    );
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&same_context, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::PreparationBusy,
        "the exact Context preparation window is full"
    );
    let bi = task(b, fixture.backend);
    let other_context = fixture.create(descriptor_with(bi, UniqueId::new(30, 3), 1, 1), Vec::new());
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&other_context, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted,
        "another Context may occupy the independent global position"
    );
    let charged = fixture.registry.preparation_snapshot().bytes;
    assert!(charged > 0 && charged <= 2048);
    assert_eq!(
        fixture
            .registry
            .cancel_task(&CancelTask::new(
                TaskOperationId::new_v7(),
                ai,
                CancelReason::UpstreamNoLongerNeeded
            ))
            .outcome(),
        OperationOutcome::Accepted
    );
    {
        let state = fixture.registry.preparation_snapshot();
        assert_eq!(
            state.bytes, charged,
            "normal cancellation does not return a still-running preparation's backing"
        );
        assert_eq!(state.positions, 2);
        assert_eq!(state.workers, 1);
        assert_eq!(state.queued_positions, 1);
    }
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&same_context, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::PreparationBusy
    );
    gate.release();
    wait_for_terminal_record(&fixture.registry, ai);
    wait_for_preparation_exit(&fixture.registry, ai);
    wait_for_accepted_installed(&fixture.registry, &other_context);
    wait_for_preparation_exit(&fixture.registry, bi);
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&same_context, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted,
        "the actual exited job returns its exact Context preparation position"
    );
    wait_for_accepted_installed(&fixture.registry, &same_context);
    wait_for_preparation_exit(&fixture.registry, second_identity);
    let state = fixture.registry.preparation_snapshot();
    assert_eq!(state.bytes, 0);
    assert_eq!(state.positions, 0);
    assert_eq!(state.context_positions, 0);
    wait_for_accepted_installed(&fixture.registry, &other_context);
    wait_for_accepted_installed(&fixture.registry, &same_context);
}

/// Holds the real preparation worker after it installed a runnable or retained
/// a failed task, before its exit guard can return the physical P/byte charge.
struct PreparationTailGate {
    gate: Arc<InstallGate>,
    installed: bool,
}

impl TaskProtocolObserver for PreparationTailGate {
    fn observe(&self, event: TaskProtocolEvent) {
        if !self.installed && matches!(event, TaskProtocolEvent::TaskTerminalRetained { .. }) {
            self.gate.wait_for_release();
        }
    }
}

impl TaskExecutionMetrics for PreparationTailGate {
    fn record_task_created(&self) {
        if self.installed {
            self.gate.wait_for_release();
        }
    }
}

#[test]
fn context_resource_retirement_waits_for_installed_or_terminal_preparation_to_exit() {
    for installed in [false, true] {
        for abort in [false, true] {
            let backend = BackendProcessId::new_v7();
            let frontend = FrontendProcessId::new_v7();
            let clock = Arc::new(ManualClock::new());
            let gate = Arc::new(InstallGate::held());
            let _release = PreparationGateRelease(Arc::clone(&gate));
            let tail = Arc::new(PreparationTailGate {
                gate: Arc::clone(&gate),
                installed,
            });
            let host = Arc::new(TestTaskHost::default());
            let registry = Arc::new(TaskExecutionRegistry::new(
                TaskExecutionRegistryConfig::for_process(backend, 17, 9),
                clock as Arc<dyn WorkerMonotonicClock>,
                Arc::new(TestContextHost),
                Arc::clone(&host) as Arc<dyn TaskExecutionHost>,
                TaskExecutionPorts::new(
                    Arc::clone(&tail) as Arc<dyn TaskProtocolObserver>,
                    Arc::new(NoopResultLifecycle),
                    tail as Arc<dyn TaskExecutionMetrics>,
                ),
            ));
            let execution = execution(30_090 + i64::from(installed) * 2 + i64::from(abort));
            let context = QueryContextRef::new(execution, frontend, backend);
            establish(&registry, context);
            let identity = task(execution, backend);
            let source = registry.status_source(context).expect("context source");
            let request = CreateTask::try_new(
                TaskOperationId::new_v7(),
                context,
                descriptor(identity),
                Vec::new(),
            )
            .expect("create");
            assert_eq!(
                registry
                    .accept_create_task(
                        &request,
                        body(if installed { RESULT_PLAN } else { REFUSED_PLAN })
                    )
                    .outcome(),
                OperationOutcome::Accepted,
            );
            gate.wait_until_entered();
            if installed {
                assert!(source.latest(identity).expect("installed task").installed());
                let reporter = host.reporters.lock().expect("test reporters")[0].clone();
                assert!(matches!(
                    reporter.running(),
                    crate::StatusAdvance::Published(_)
                ));
                assert!(matches!(
                    reporter.finished(novarocks_execution_contract::TaskOutputFacts::new(true)),
                    crate::StatusAdvance::Published(_)
                ));
                assert_eq!(reporter.current().state(), TaskState::Finished);
                reporter.release_output();
                reporter.note_actual_stopped();
                reporter.note_resources_converged();
                registry.advance_deadlines();
                assert!(matches!(
                    registry.root_result_route(identity),
                    RootResultRoute::TerminalResultOwner(TaskState::Finished)
                ));
            }
            assert_eq!(registry.preparation_snapshot().context_positions, 1);
            assert!(registry.preparation_snapshot().bytes > 0);
            if abort {
                registry.abort_query_context(&AbortQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    AbortCause::QueryFailed,
                ));
            } else {
                registry.quiesce_query_context(&QuiesceQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                ));
                assert_eq!(
                    registry
                        .release_query_context(&ReleaseQueryContext::new(
                            TaskOperationId::new_v7(),
                            context
                        ))
                        .outcome(),
                    OperationOutcome::ReleaseNotReady,
                    "a terminal record does not settle its still-running preparation",
                );
            }
            registry.advance_deadlines();
            assert!(source.latest_context_convergence().is_none());
            assert!(
                host.retired_executions
                    .lock()
                    .expect("retired executions")
                    .is_empty()
            );
            gate.release();
            wait_for_preparation_exit(&registry, identity);
            if !abort {
                assert_eq!(
                    registry
                        .release_query_context(&ReleaseQueryContext::new(
                            TaskOperationId::new_v7(),
                            context
                        ))
                        .outcome(),
                    OperationOutcome::Accepted
                );
            }
            for _ in 0..1000 {
                if source.latest_context_convergence().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(
                source.latest_context_convergence().is_some(),
                "job exit must drive context convergence without waiting for its own exit"
            );
            assert_eq!(
                *host.retired_executions.lock().expect("retired executions"),
                vec![context]
            );
            assert_eq!(registry.preparation_snapshot().bytes, 0);
        }
    }
}

#[test]
fn preparation_charge_adds_private_input_to_incoming_domain_backing_bound() {
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        },
        |config| {
            config.max_preparing_bytes = 4096;
        },
    );
    let execution = execution(30_004);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    let input = body(STREAM_PLAN);
    let input_owned = input.retained_bytes();
    let incoming_domain_backing = 400;
    assert_eq!(
        fixture
            .registry
            .accept_create_task_with_retained_bytes(&request, input, incoming_domain_backing)
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let charged = fixture.registry.preparation_snapshot().bytes;
    assert!(
        charged >= incoming_domain_backing + input_owned,
        "concurrent private input and incoming domain backing must both be charged"
    );
    gate.release();
    wait_for_accepted_installed(&fixture.registry, &request);
    wait_for_preparation_exit(&fixture.registry, identity);
    assert_eq!(fixture.registry.preparation_snapshot().bytes, 0);
}
