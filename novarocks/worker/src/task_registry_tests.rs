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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution_contract::failure::FailureProjection;
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
    creation_commits: Arc<AtomicUsize>,
    preparation_order: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl RunnableTask for TestRunnable {
    fn commit_creation(&self) {
        if let Some(order) = &self.preparation_order {
            order.lock().unwrap().push("commit");
        }
        self.creation_commits.fetch_add(1, Ordering::SeqCst);
    }

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparationFailureStage {
    Receiver,
    Capability,
    Domain,
    Submit,
}

#[derive(Default)]
struct TestTaskHost {
    preparation_rejection: Option<(PreparationFailureStage, HostRejection)>,
    preparation_child: Mutex<Option<(TaskId, Arc<dyn crate::TaskPreparationChild>)>>,
    preparation_order: Option<Arc<Mutex<Vec<&'static str>>>>,
    preparation_receiver_cleanup_panic: bool,
    preparation_cleanup_payload_drops: Option<Arc<AtomicUsize>>,
    preparation_panic: Option<(PreparationFailureStage, Arc<AtomicUsize>)>,
    owned_preparation_panic: Option<(TaskId, Arc<OwnedPreparationPanic>)>,
    owned_preparation_payload: Mutex<Option<BodyUnwindPayload>>,
    creation_commits: Arc<AtomicUsize>,
    actual_dormant_cleanup: Option<(TaskId, Arc<ActualDormantCleanup>)>,
    preparation_stages: Mutex<Vec<PreparationFailureStage>>,
    capabilities_removed: AtomicUsize,
    retain_normal_close: bool,
    normal_retirement_rejection: bool,
    normal_retirement_calls: AtomicUsize,
    inbound_capabilities: Option<Arc<TaskInboundCapabilities>>,
    install_gate: Option<Arc<InstallGate>>,
    /// Mark entry into the original loan wait, then hold cleanup after stop.
    pending_stop_gates: Option<(Arc<InstallGate>, Arc<InstallGate>)>,
    /// Restricts `install_gate` to creations of this static plan, so a peer
    /// task of the same context can install while another one is held.
    install_gate_plan: Option<&'static [u8]>,
    /// Parks the named task's creation rollback after it decided how to stop
    /// and before it publishes that task's terminal.
    rollback_gate: Option<(TaskId, Arc<InstallGate>)>,
    prepared_bodies: Mutex<Vec<Bytes>>,
    preparation_stops: Mutex<Vec<(TaskIdentity, crate::TaskPreparationView)>>,
    prepared_root: Mutex<Option<Arc<crate::root_result_channel::RootResultChannel>>>,
    receivers_installed: AtomicUsize,
    receivers_removed: AtomicUsize,
    capabilities_installed: AtomicUsize,
    submitted: AtomicUsize,
    domains_applied: AtomicUsize,
    applied_domain_operations: Mutex<Vec<TaskOperationId>>,
    reporters: Mutex<Vec<TaskStatusReporter>>,
    retired_executions: Mutex<Vec<QueryContextRef>>,
    cancel_calls: Arc<AtomicUsize>,
}

/// The fixture authors its domain meaning explicitly. Production has no
/// category/text-to-source constructor and cannot use this local test type.
#[derive(Debug)]
struct RegistryFixtureFailure {
    category: TaskFailureCategory,
    detail: String,
}
impl std::fmt::Display for RegistryFixtureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}
impl std::error::Error for RegistryFixtureFailure {}
impl novarocks_execution_contract::failure::FailureProjection for RegistryFixtureFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self.category {
            TaskFailureCategory::Execution => (FailureClass::Data, 65001),
            TaskFailureCategory::ResourceExhausted => (FailureClass::Resource, 65002),
            TaskFailureCategory::Exchange => (FailureClass::Dependency, 65003),
            TaskFailureCategory::Protocol => (FailureClass::Invariant, 65004),
            TaskFailureCategory::Internal => (FailureClass::Unexpected, 65005),
            TaskFailureCategory::Unsupported => (FailureClass::Unsupported, 65006),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn fixture_host_rejection(
    category: TaskFailureCategory,
    detail: impl Into<String>,
) -> HostRejection {
    let detail = detail.into();
    let safe = novarocks_execution_contract::SafeDetail::truncating(&detail);
    HostRejection::from_domain_with_detail(
        category,
        RegistryFixtureFailure { category, detail },
        safe,
    )
}
#[derive(Debug)]
struct PreparationStoppedFixture;
impl std::fmt::Display for PreparationStoppedFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("preparation stopped before body")
    }
}
impl std::error::Error for PreparationStoppedFixture {}
impl novarocks_execution_contract::failure::FailureProjection for PreparationStoppedFixture {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::stop_observation(FailureReason::new(
            domain::WORKER,
            std::num::NonZeroU16::new(65006).unwrap(),
        ))
    }
}

struct PreparationPanicPayload {
    dropped: Arc<AtomicUsize>,
    _detail: &'static str,
}
impl Drop for PreparationPanicPayload {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl TestTaskHost {
    fn enter_preparation_stage(&self, stage: PreparationFailureStage) -> Result<(), HostRejection> {
        self.preparation_stages
            .lock()
            .expect("preparation stage log")
            .push(stage);
        if stage == PreparationFailureStage::Capability {
            let payload = self.owned_preparation_payload.lock().unwrap().take();
            if let Some(payload) = payload {
                std::panic::panic_any(payload);
            }
        }
        if let Some((at, dropped)) = &self.preparation_panic
            && *at == stage
        {
            std::panic::panic_any(PreparationPanicPayload {
                dropped: dropped.clone(),
                _detail: "ResourceExhausted: arbitrary panic diagnostic",
            });
        }
        if let Some((refused, rejection)) = &self.preparation_rejection
            && stage == *refused
        {
            return Err(rejection.clone());
        }
        Ok(())
    }

    fn prepared_bodies(&self) -> Vec<Bytes> {
        self.prepared_bodies
            .lock()
            .expect("test prepared bodies")
            .clone()
    }

    fn preparation_stop(&self, identity: TaskIdentity) -> crate::TaskPreparationView {
        self.preparation_stops
            .lock()
            .expect("test preparation stops")
            .iter()
            .find(|(task, _)| *task == identity)
            .expect("host received the preparation's actual stop view")
            .1
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
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        preparation_scope: crate::TaskPreparationContext<'_>,
    ) -> Result<crate::PreparedTaskInstallation, HostRejection> {
        let child = {
            let mut child = self.preparation_child.lock().unwrap();
            if child
                .as_ref()
                .is_some_and(|(task, _)| *task == descriptor.identity().task_id())
            {
                child.take().map(|(_, child)| child)
            } else {
                None
            }
        };
        if let Some(child) = child {
            preparation_scope.register_child(child).map_err(|cause| {
                HostRejection::from_domain(TaskFailureCategory::Internal, cause)
            })?;
        }
        if let Some((task, owned)) = &self.owned_preparation_panic
            && descriptor.identity().task_id() == *task
        {
            let authority = preparation_scope.runtime_control();
            *owned.authority.lock().unwrap() = Some(Arc::downgrade(&authority));
            let data = owned
                .data
                .lock()
                .unwrap()
                .take()
                .expect("one original data capture");
            *self.owned_preparation_payload.lock().unwrap() = Some(BodyUnwindPayload {
                authority: Some(authority),
                data,
                gate: owned.gate.clone(),
                dropped: owned.dropped.clone(),
                secondary: owned.secondary,
                secondary_drops: owned.secondary_drops.clone(),
            });
        }
        let preparation_stop = preparation_scope.view();
        self.preparation_stops
            .lock()
            .expect("test preparation stops")
            .push((descriptor.identity(), preparation_stop.clone()));
        if let Some((entered, exiting)) = &self.pending_stop_gates {
            {
                let mut state = entered.state.lock().unwrap();
                state.waiter_entered = true;
                entered.changed.notify_all();
            }
            // The original winner lends the bounded wait; an actual stop
            // interrupts it without another preparation worker.
            while preparation_stop.borrow_control().wait().is_ok() {}
            exiting.wait_for_release();
            return Err(HostRejection::from_domain(
                TaskFailureCategory::Execution,
                PreparationStoppedFixture,
            ));
        }
        let (plan, _assignment) = input.into_parts();
        if let Some(gate) = &self.install_gate
            && self
                .install_gate_plan
                .is_none_or(|gated| plan.bytes().as_ref() == gated)
        {
            gate.wait_for_release();
        }
        self.prepared_bodies
            .lock()
            .expect("test prepared bodies")
            .push(plan.to_bytes());
        self.enter_preparation_stage(PreparationFailureStage::Receiver)?;
        if plan.bytes().as_ref() == REFUSED_PLAN {
            // A real host undoes its own local preparation before it refuses;
            // this one prepared nothing, so it has nothing to undo.
            return Err(fixture_host_rejection(
                TaskFailureCategory::Protocol,
                "test static plan is not decodable",
            ));
        }
        self.receivers_installed.fetch_add(1, Ordering::SeqCst);
        crate::PreparedTaskInstallation::new(
            PreparedTaskFacts::new(if plan.bytes().as_ref() == STREAM_PLAN {
                FragmentSinkKind::DataStream
            } else {
                FragmentSinkKind::Result
            }),
            self.prepared_root
                .lock()
                .expect("test root installation")
                .take(),
        )
    }

    fn remove_receiver(&self, _descriptor: &TaskDescriptor) {
        self.receivers_removed.fetch_add(1, Ordering::SeqCst);
        if self.preparation_receiver_cleanup_panic {
            if let Some(dropped) = &self.preparation_cleanup_payload_drops {
                std::panic::panic_any(PreparationPanicPayload {
                    dropped: dropped.clone(),
                    _detail: "opaque receiver cleanup failure",
                });
            }
            panic!("injected receiver/source destructor panic");
        }
    }

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.enter_preparation_stage(PreparationFailureStage::Capability)?;
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
        self.capabilities_removed.fetch_add(1, Ordering::SeqCst);
        if let Some((task, gate)) = &self.rollback_gate
            && descriptor.identity().task_id() == *task
        {
            gate.wait_for_release();
        }
        if let Some(capabilities) = &self.inbound_capabilities {
            capabilities.remove(descriptor);
        }
    }

    fn retire_receiver_normally(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.normal_retirement_calls.fetch_add(1, Ordering::SeqCst);
        if self.normal_retirement_rejection {
            return Err(fixture_host_rejection(
                TaskFailureCategory::Protocol,
                "checked inbound close contradiction",
            ));
        }
        self.remove_inbound_capability(descriptor);
        self.remove_receiver(descriptor);
        Ok(())
    }

    fn submit_runnable(
        &self,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        self.enter_preparation_stage(PreparationFailureStage::Submit)?;
        self.submitted.fetch_add(1, Ordering::SeqCst);
        self.reporters
            .lock()
            .expect("test reporters")
            .push(reporter);
        if let Some((task, cleanup)) = &self.actual_dormant_cleanup
            && descriptor.identity().task_id() == *task
        {
            *cleanup.reporter.lock().unwrap() = self.reporters.lock().unwrap().last().cloned();
            return Ok(cleanup.clone());
        }
        Ok(Arc::new(TestRunnable {
            cancel_calls: Arc::clone(&self.cancel_calls),
            creation_commits: Arc::clone(&self.creation_commits),
            preparation_order: self.preparation_order.clone(),
        }))
    }

    fn apply_task_domain(
        &self,
        _descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
        operation: novarocks_execution_contract::TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        self.enter_preparation_stage(PreparationFailureStage::Domain)?;
        self.domains_applied.fetch_add(1, Ordering::SeqCst);
        self.applied_domain_operations
            .lock()
            .expect("applied domain operations")
            .push(operation);
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
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
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

fn bounded_root(
    identity: TaskIdentity,
) -> (
    Arc<crate::root_result_channel::RootResultChannel>,
    Arc<crate::result_buffer::ResultRetainedBudget>,
) {
    use novarocks_execution::runtime::fragment::io::RootResultWriteSpec;
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    let limits =
        crate::WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, 4 * 1024 * 1024 * 1024)
            .expect("frozen result limits");
    let budget = crate::result_buffer::ResultRetainedBudget::new(limits.per_process());
    let root = crate::root_result_channel::RootResultChannel::try_open(
        RootResultWriteSpec {
            task: identity,
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            )),
        },
        Arc::clone(&budget),
        limits,
        // This standalone channel fixture owns an explicit test health latch;
        // it does not claim identity with an unrelated Registry's test ports.
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
    )
    .expect("bounded root");
    (root, budget)
}

fn bounded_read(
    root: &crate::root_result_channel::RootResultChannel,
    wanted: u64,
) -> novarocks_execution_contract::root_result::RootResultRead {
    novarocks_execution_contract::root_result::RootResultRead::try_new(
        root.spec().task,
        root.spec().contract.profile(),
        root.spec().contract.kind(),
        std::num::NonZeroU64::new(wanted),
        0,
        Duration::from_millis(1),
    )
    .expect("bounded read without final ACK")
}

fn install_bounded_root(
    fixture: &Fixture,
    identity: TaskIdentity,
    root: &Arc<crate::root_result_channel::RootResultChannel>,
) {
    *fixture
        .task_host
        .prepared_root
        .lock()
        .expect("test root installation") = Some(Arc::clone(root));
    let created = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(created.outcome(), OperationOutcome::Accepted, "{created:?}");
}

fn finish_bounded_root(
    fixture: &Fixture,
    root: &Arc<crate::root_result_channel::RootResultChannel>,
) {
    let producer = root.start_producer().expect("one root producer");
    root.note_rows(3).expect("checked rows");
    root.request_finish().expect("finish request");
    let mut builder = root
        .try_segment()
        .expect("segment admission")
        .expect("segment");
    builder.output()[..3].copy_from_slice(b"abc");
    root.publish_segment(builder, 3, true)
        .expect("atomic Data and End");
    producer.finish_encoder();
    assert_eq!(
        root.producer_state(),
        novarocks_execution::runtime::fragment::io::RootProducerState::ContextHeld
    );
    let reporter = fixture.task_host.reporters.lock().expect("test reporters")[0].clone();
    reporter.running();
    reporter.finished(novarocks_execution_contract::TaskOutputFacts::new(true));
    reporter.release_output();
    reporter.note_actual_stopped();
    reporter.note_resources_converged();
    assert_eq!(fixture.registry.advance_deadlines().tasks_retired, 1);
}

#[test]
fn root_ownership_snapshot_refuses_incomplete_scan_coverage() {
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.retained_context_capacity = 2048;
    });
    // Vacant quiesce fences are genuine existing context entries; they must
    // count toward bounded work even though they contain no root channel.
    for index in 0..1024 {
        let context = fixture.context(execution(32_000 + index));
        let receipt = fixture
            .registry
            .quiesce_query_context(&QuiesceQueryContext::new(
                TaskOperationId::new_v7(),
                context,
            ));
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    }
    assert!(
        fixture
            .registry
            .try_root_ownership_snapshot()
            .unwrap()
            .is_some()
    );
    let extra = fixture.context(execution(33_024));
    fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(TaskOperationId::new_v7(), extra));
    assert_eq!(
        fixture.registry.try_root_ownership_snapshot().unwrap(),
        None
    );
}

#[cfg(novarocks_dev_controls)]
#[test]
fn root_ownership_snapshot_observes_retired_task_context_and_busy_registry() {
    let fixture = Fixture::new(TestTaskHost::default());
    assert_eq!(
        fixture
            .registry
            .try_root_ownership_snapshot()
            .unwrap()
            .unwrap()
            .channels,
        0
    );
    let execution = execution(31_010);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, _) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    let snapshot = fixture
        .registry
        .try_root_ownership_snapshot()
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            snapshot.channels,
            snapshot.terminal_task_records,
            snapshot.producers_exited
        ),
        (1, 1, 1)
    );
    assert_eq!(
        (
            snapshot.ends_published,
            snapshot.ends_acknowledged,
            snapshot.segments
        ),
        (1, 0, 1)
    );
    #[cfg(novarocks_dev_controls)]
    fixture.registry.with_registry_lock_for_test(|| {
        assert_eq!(
            fixture.registry.try_root_ownership_snapshot().unwrap(),
            None
        );
    });
}

#[tokio::test]
async fn context_owned_root_survives_task_retirement_and_request_horizon() {
    use crate::root_result_channel::ContextRootRoute;
    use novarocks_execution_contract::root_result::RootReadOutcome;
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_001);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, _budget) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    // Keep the original context live while its task's separate receipt
    // horizon expires. Each renewal is an exact monotonic domain update.
    for sequence in 1..=25 {
        fixture.clock.advance(Duration::from_secs(5));
        let receipt = fixture
            .registry
            .update_query_context(&UpdateQueryContext::RenewLease(
                RenewQueryExecutionLease::new(
                    TaskOperationId::new_v7(),
                    context,
                    LeaseSequence::new(sequence),
                    LeaseValidFor::new(Duration::from_secs(10)).expect("lease"),
                ),
            ));
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        fixture.registry.advance_deadlines();
    }
    assert!(matches!(
        fixture.registry.root_result_route(identity),
        RootResultRoute::Gone
    ));
    let request = bounded_read(&root, 1);
    let ContextRootRoute::Read(read) = fixture.registry.context_root_result_route(&request) else {
        panic!("original context serves after the task horizon");
    };
    let delivery = read.read().await.expect("context read");
    let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
        panic!("Data");
    };
    assert_eq!(data.body().as_ref(), b"abc");
    assert_eq!(data.end_after_data().expect("End").output_rows, 3);
    drop(delivery);
    let ContextRootRoute::Read(read) = fixture.registry.context_root_result_route(&request) else {
        panic!("replay");
    };
    let replay = read.read().await.expect("same sequence");
    assert_eq!(replay.reply().accepted_consumed, 0);
    assert!(matches!(replay.reply().outcome, RootReadOutcome::Data(_)));
}

#[tokio::test]
async fn root_release_seals_before_late_reads_and_waits_for_body_only_alias() {
    use crate::root_result_channel::ContextRootRoute;
    use novarocks_execution_contract::root_result::RootReadOutcome;
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_002);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, budget) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    let request = bounded_read(&root, 1);
    let ContextRootRoute::Read(read) = fixture.registry.context_root_result_route(&request) else {
        panic!("read");
    };
    let delivery = read.read().await.expect("read");
    let alias = match &delivery.reply().outcome {
        RootReadOutcome::Data(data) => data.body().clone(),
        _ => panic!("Data"),
    };
    drop(delivery);
    fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    let release = ReleaseQueryContext::new(TaskOperationId::new_v7(), context);
    let receipt = fixture.registry.release_query_context(&release);
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Releasing
    );
    assert!(matches!(
        fixture.registry.context_root_result_route(&request),
        ContextRootRoute::AwaitTerminalControl {
            accepted_consumed: 0
        }
    ));
    assert!(
        budget.retained_bytes_for_test() > 1024 * 1024,
        "real backing remains charged after queue clear"
    );
    let replay = fixture.registry.release_query_context(&release);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        fixture.task_host.retired_executions.lock().unwrap().len(),
        0
    );
    drop(alias);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
    assert_eq!(
        fixture.task_host.retired_executions.lock().unwrap().len(),
        1
    );
    drop(root);
    assert_eq!(budget.retained_bytes_for_test(), 0);
    let replay = fixture.registry.release_query_context(&release);
    assert_eq!(
        replay.outcome(),
        OperationOutcome::ContextTerminalReceipt,
        "lost ACK still resolves to original context"
    );
}

#[tokio::test]
async fn closed_context_root_route_returns_frozen_ack_watermark_before_late_ack() {
    use crate::root_result_channel::ContextRootRoute;
    use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_007);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, _budget) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    let request = bounded_read(&root, 1);
    let ContextRootRoute::Read(read) = fixture.registry.context_root_result_route(&request) else {
        panic!("read original data");
    };
    let delivery = read.read().await.unwrap();
    let alias = match &delivery.reply().outcome {
        RootReadOutcome::Data(data) => data.body().clone(),
        _ => panic!("original Data"),
    };
    drop(delivery);
    let ack = RootResultRead::try_new(
        identity,
        root.spec().contract.profile(),
        root.spec().contract.kind(),
        None,
        1,
        Duration::from_millis(1),
    )
    .unwrap();
    let ContextRootRoute::Read(admitted) = fixture.registry.context_root_result_route(&ack) else {
        panic!("apply ACK before seal");
    };
    let acknowledged = admitted.read().await.unwrap();
    assert_eq!(acknowledged.reply().accepted_consumed, 1);
    assert_eq!(acknowledged.reply().outcome, RootReadOutcome::AckOnly);
    drop(acknowledged);
    fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    let late_ack = RootResultRead::try_new(
        identity,
        root.spec().contract.profile(),
        root.spec().contract.kind(),
        None,
        2,
        Duration::from_millis(1),
    )
    .unwrap();
    assert!(matches!(
        fixture.registry.context_root_result_route(&late_ack),
        ContextRootRoute::AwaitTerminalControl {
            accepted_consumed: 1
        },
    ));
    assert_eq!(root.snapshot().consumed_through, 1);
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Releasing
    );
    drop(alias);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained,
    );
}

#[test]
fn root_read_admission_under_context_fence_survives_until_actual_handler_exit() {
    use crate::root_result_channel::ContextRootRoute;
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_003);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, _budget) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    let request = bounded_read(&root, 1);
    let ContextRootRoute::Read(admitted) = fixture.registry.context_root_result_route(&request)
    else {
        panic!("admitted read");
    };
    // The handler has not polled yet. Its admission already counts; lookup
    // cannot be detached from the context's closing fence.
    fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Releasing
    );
    assert!(matches!(
        fixture.registry.context_root_result_route(&request),
        ContextRootRoute::AwaitTerminalControl {
            accepted_consumed: 0
        }
    ));
    drop(admitted);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[test]
fn finished_root_abort_and_lease_expiry_wait_for_real_reservation_exit() {
    use crate::root_result_channel::ContextRootRoute;
    use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
    for expire in [false, true] {
        let fixture = Fixture::new(TestTaskHost::default());
        let execution = execution(31_004 + i64::from(expire));
        let context = fixture.context(execution);
        establish(&fixture.registry, context);
        let identity = task(execution, fixture.backend);
        let (root, _budget) = bounded_root(identity);
        install_bounded_root(&fixture, identity, &root);
        finish_bounded_root(&fixture, &root);
        let ResultWriteAdmission::Granted(mut reservation) =
            root.try_reserve(1).expect("one holder")
        else {
            panic!("credit");
        };
        reservation
            .shrink_to(0)
            .expect("shrink does not mean physical exit");
        if expire {
            fixture.clock.advance(Duration::from_secs(10));
            assert_eq!(fixture.registry.advance_deadlines().leases_expired, 1);
        } else {
            fixture
                .registry
                .abort_query_context(&AbortQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    AbortCause::QueryFailed,
                ));
        }
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Aborting
        );
        assert!(matches!(
            fixture
                .registry
                .context_root_result_route(&bounded_read(&root, 1)),
            ContextRootRoute::AwaitTerminalControl {
                accepted_consumed: 0
            }
        ));
        assert_eq!(
            fixture.task_host.retired_executions.lock().unwrap().len(),
            0
        );
        drop(reservation);
        fixture.registry.advance_deadlines();
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::TerminalRetained
        );
        assert_eq!(
            fixture.task_host.retired_executions.lock().unwrap().len(),
            1
        );
    }
}

#[test]
fn foreign_prepared_root_is_refused_before_installation_and_remains_unreadable() {
    use crate::root_result_channel::ContextRootRoute;
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_006);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let foreign_identity = task(self::execution(31_007), fixture.backend);
    let (foreign, _budget) = bounded_root(foreign_identity);
    *fixture.task_host.prepared_root.lock().unwrap() = Some(Arc::clone(&foreign));
    let receipt = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(RESULT_PLAN),
    );
    assert!(receipt.acknowledgement().is_none(), "{receipt:?}");
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        1
    );
    assert!(matches!(
        fixture
            .registry
            .context_root_result_route(&bounded_read(&foreign, 1)),
        ContextRootRoute::UnknownRoot
    ));
    assert!(
        foreign.begin_read(&bounded_read(&foreign, 1)).is_err(),
        "no provisional channel becomes a route"
    );
}

#[tokio::test]
async fn release_wakes_an_already_parked_root_read_without_waiting_for_its_deadline() {
    use crate::root_result_channel::ContextRootRoute;
    use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(31_008);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let (root, _budget) = bounded_root(identity);
    install_bounded_root(&fixture, identity, &root);
    finish_bounded_root(&fixture, &root);
    let ContextRootRoute::Read(first) = fixture
        .registry
        .context_root_result_route(&bounded_read(&root, 1))
    else {
        panic!("initial read");
    };
    drop(first.read().await.expect("offer immutable Data and End"));
    let request = RootResultRead::try_new(
        identity,
        root.spec().contract.profile(),
        root.spec().contract.kind(),
        std::num::NonZeroU64::new(3),
        0,
        Duration::from_secs(1),
    )
    .expect("one-second read wait");
    let ContextRootRoute::Read(admitted) = fixture.registry.context_root_result_route(&request)
    else {
        panic!("read");
    };
    let mut waiting = Box::pin(admitted.read());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending()))
            .await,
        "handler is actually parked before release"
    );
    fixture
        .registry
        .quiesce_query_context(&QuiesceQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    fixture
        .registry
        .release_query_context(&ReleaseQueryContext::new(
            TaskOperationId::new_v7(),
            context,
        ));
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Releasing
    );
    let reply = tokio::time::timeout(Duration::from_millis(250), waiting)
        .await
        .expect("seal wakes the parked read")
        .expect("closing reply");
    assert!(matches!(
        reply.reply().outcome,
        RootReadOutcome::AwaitTerminalControl
    ));
    assert_eq!(
        reply.reply().accepted_consumed,
        0,
        "release does not forge consumption"
    );
    drop(reply);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
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
    health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
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
        let ports = test_ports();
        let health = ports.execution_health();
        let registry = TaskExecutionRegistry::new(
            config,
            Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(TestContextHost),
            Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
            ports,
        );
        Self {
            health,
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
fn accepted_preparation_cancel_prevents_runnable_activation() {
    use novarocks_execution_contract::TaskConclusionPort;

    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(51);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established status source");
    let request = fixture.create(descriptor(identity), Vec::new());
    let creator = Arc::clone(&fixture.registry);
    let create = std::thread::spawn(move || creator.create_task(&request, body(RESULT_PLAN)));
    gate.wait_until_entered();
    let status = fixture
        .task_host
        .preparation_stop(identity)
        .control()
        .0
        .expect("the real accepted Task owns the in-flight preparation");
    assert_eq!(status.current().state(), TaskState::Planned);
    assert!(!status.current().installed());
    assert_eq!(source.latest(identity), Some(status.current()));

    let cancel_request = CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    );
    let canceled = fixture.registry.cancel_task(&cancel_request);
    assert_eq!(
        canceled.outcome(),
        OperationOutcome::Accepted,
        "{canceled:?}"
    );
    let canceling = canceled.acknowledgement().expect("accepted cancellation");
    assert_eq!(canceling.state(), TaskState::Canceling);
    assert!(!canceling.installed());
    assert_eq!(
        canceling.termination(),
        Some(&TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        ))
    );
    let conclusion = status
        .accepted()
        .expect("accepted cancellation owns the Task");
    assert!(conclusion.originating_failure().is_none());
    assert!(
        !status.convergence().actual_stopped(),
        "a control decision is not actual exit"
    );
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 0);

    gate.release();
    let interrupted = create.join().expect("create thread");
    assert_eq!(
        interrupted.outcome(),
        OperationOutcome::ContextTerminalReceipt,
        "{interrupted:?}"
    );
    assert!(interrupted.acknowledgement().is_none());
    assert_eq!(
        interrupted.detail().unwrap().as_str(),
        "task stopped during preparation"
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)]
    );
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture
            .task_host
            .capabilities_installed
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .task_host
            .reporters
            .lock()
            .expect("test reporters")
            .is_empty()
    );
    assert_eq!(fixture.task_host.creation_commits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 0);
    assert_preparation_actual_exit(&status);
    assert!(Arc::ptr_eq(&status.accepted().unwrap(), &conclusion));
    let terminal = source
        .latest(identity)
        .expect("accepted cancellation remains observable");
    assert_eq!(terminal.state(), TaskState::Canceled);
    assert!(!terminal.installed());
    assert_eq!(terminal.termination(), canceling.termination());
    let replay = fixture.registry.cancel_task(&cancel_request);
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(replay.acknowledgement(), Some(&terminal));
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
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
fn accepted_preparation_closed_by_lease_expiry_publishes_and_reaps_its_terminal() {
    use crate::observation::{CursorObservation, TaskStatusEvent};
    use novarocks_execution_contract::{TaskConclusionPort, TaskStatusCursor};

    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(82);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established status source");
    let request = fixture.create(descriptor(identity), Vec::new());
    let creating = Arc::clone(&fixture.registry);
    let create = std::thread::spawn(move || creating.create_task(&request, body(RESULT_PLAN)));
    gate.wait_until_entered();
    let status = fixture
        .task_host
        .preparation_stop(identity)
        .control()
        .0
        .expect("hold the real accepted owner while preparation is in flight");
    assert_eq!(status.current().state(), TaskState::Planned);
    assert!(!status.current().installed());
    assert_eq!(source.latest(identity), Some(status.current()));

    fixture.clock.advance(Duration::from_secs(10));
    assert_eq!(fixture.registry.advance_deadlines().leases_expired, 1);
    let conclusion = status
        .accepted()
        .expect("lease expiry concludes the accepted Task");
    assert_eq!(
        conclusion.neutral().conclusion(),
        &novarocks_execution_contract::TaskConclusion::Terminated(TerminationDetail::Aborted(
            AbortCause::LeaseExpired
        ))
    );
    assert!(conclusion.originating_failure().is_none());
    assert!(
        !status.convergence().actual_stopped(),
        "lease expiry does not prove preparation exit"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    gate.release();
    let rejected = create.join().expect("create thread");
    assert_eq!(
        rejected.outcome(),
        OperationOutcome::ContextTerminalReceipt,
        "{rejected:?}"
    );
    assert!(rejected.acknowledgement().is_none());
    assert_eq!(
        rejected.detail().unwrap().as_str(),
        "task stopped during preparation"
    );
    assert!(
        fixture
            .task_host
            .reporters
            .lock()
            .expect("test reporters")
            .is_empty()
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.task_host.creation_commits.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.task_host.cancel_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture
            .task_host
            .capabilities_installed
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)]
    );
    assert_preparation_actual_exit(&status);
    assert!(Arc::ptr_eq(&status.accepted().unwrap(), &conclusion));
    let terminal = source
        .latest(identity)
        .expect("accepted Task retains its terminal");
    assert_eq!(terminal.state(), TaskState::Aborted);
    assert!(!terminal.installed());
    assert_eq!(
        terminal.termination(),
        Some(&TerminationDetail::Aborted(AbortCause::LeaseExpired))
    );
    assert_eq!(
        source.next_task_event(),
        Some(TaskStatusEvent::Status(terminal.clone()))
    );
    assert!(source.next_task_event().is_none());

    fixture.clock.advance(Duration::from_secs(121));
    assert_eq!(fixture.registry.advance_deadlines().tasks_reaped, 1);
    assert!(source.latest(identity).is_none());
    assert!(matches!(
        fixture.registry.root_result_route(identity),
        RootResultRoute::Gone
    ));
    assert_eq!(
        source.observe(TaskStatusCursor::unobserved(identity)),
        CursorObservation::Current(Box::new(terminal.clone())),
        "an observer behind the terminal must catch up before Gone"
    );
    assert_eq!(
        source.observe(TaskStatusCursor::at(identity, terminal.version())),
        CursorObservation::Gone
    );
    assert_eq!(
        source.next_task_event(),
        Some(TaskStatusEvent::Gone(identity))
    );
    assert!(source.next_task_event().is_none());
}

// These helpers inspect the real accepted owner retained by a host loan.
// They never manufacture a Task failure from the operation's diagnostic text.
fn assert_failed_preparation_status(
    status: &novarocks_execution_contract::TaskStatus,
    identity: TaskIdentity,
    neutral_detail: &str,
) {
    assert_eq!(status.identity(), identity);
    assert_eq!(status.state(), TaskState::Failed);
    assert!(!status.installed());
    let Some(TerminationDetail::Failed(failure)) = status.termination() else {
        panic!("expected the retained preparation failure: {status:?}");
    };
    assert_eq!(failure.category(), TaskFailureCategory::Internal);
    assert_eq!(failure.phase(), TaskFailurePhase::Preparation);
    assert_eq!(failure.detail().as_str(), neutral_detail);
}

fn assert_preparation_actual_exit(status: &crate::TaskStatusOwner) {
    let convergence = status.convergence();
    assert!(convergence.conclusion_stable());
    assert!(convergence.actual_stopped());
    assert!(convergence.output_released());
    assert!(convergence.resources_converged());
    assert!(convergence.retired());
}

fn assert_refused_plan_origin(
    status: &crate::TaskStatusOwner,
) -> Arc<
    novarocks_execution_contract::AcceptedTaskConclusion<
        novarocks_execution::runtime::fragment::ExecutionFailure,
    >,
> {
    use novarocks_execution::runtime::fragment::ExecutionFailureCause;
    use novarocks_execution_contract::TaskConclusionPort;
    use novarocks_execution_contract::failure::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };

    let accepted = status
        .accepted()
        .expect("the host's original refusal was accepted");
    let original = accepted
        .originating_failure()
        .expect("retain the actual host failure");
    assert_eq!(
        original.failure_fact(),
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(65004).unwrap())
        )
    );
    let ExecutionFailureCause::Domain(source) = original.cause() else {
        panic!("the host domain source must not be reconstructed: {original:?}");
    };
    let authored = source
        .downcast_ref::<RegistryFixtureFailure>()
        .expect("fixture host owns the source");
    assert_eq!(authored.category, TaskFailureCategory::Protocol);
    assert_eq!(authored.detail, "test static plan is not decodable");
    assert_eq!(original.detail(), "test static plan is not decodable");
    assert_eq!(
        original.safe_detail().as_str(),
        "Invariant failure (reason 10:65004)"
    );
    assert_failed_preparation_status(
        &status.current(),
        status.identity(),
        "Invariant failure (reason 10:65004)",
    );
    assert_eq!(
        accepted.neutral().conclusion(),
        &novarocks_execution_contract::TaskConclusion::Terminated(
            status.current().termination().unwrap().clone()
        )
    );
    assert_preparation_actual_exit(status);
    accepted
}

/// Membership is validated before host effects, but after Task acceptance.
/// A refusal spends this identity and leaves its neutral terminal observable.
#[test]
fn an_unfrozen_initial_domain_fails_the_accepted_identity_without_host_effects() {
    use novarocks_execution_contract::DomainConflict;
    use novarocks_execution_contract::failure::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };

    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(21);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established status source");
    let private_detail = "split assignment names plan node 9 which the descriptor did not freeze";
    // Check the real policy author independently. The retired public seam
    // deliberately exports only neutral status, not the rich local source.
    let authored = crate::task_domain_execution::validate_task_domain_execution_membership(
        &descriptor(identity),
        &[split_batch(9, 1, 1)],
    )
    .expect_err("the descriptor never froze node 9");
    assert_eq!(authored.outcome(), OperationOutcome::DomainConflict);
    assert_eq!(authored.category(), TaskFailureCategory::Protocol);
    assert_eq!(authored.detail(), private_detail);
    let policy = std::error::Error::source(&authored)
        .unwrap()
        .downcast_ref::<crate::DomainPolicyRejection>()
        .expect("original Worker policy source");
    assert_eq!(policy.conflict(), DomainConflict::UnknownMember);
    assert_eq!(policy.detail(), private_detail);
    let expected_fact = FailureFact::new(
        FailureClass::Invariant,
        FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(63).unwrap()),
    );
    assert_eq!(policy.failure_fact(), expected_fact);
    let projected = authored.into_host();
    assert_eq!(projected.category(), TaskFailureCategory::Protocol);
    assert_eq!(projected.detail().as_str(), private_detail);
    let original = projected.into_failure();
    assert_eq!(original.failure_fact(), expected_fact);
    let novarocks_execution::runtime::fragment::ExecutionFailureCause::Domain(policy_source) =
        original.cause()
    else {
        panic!("membership retains its original policy source");
    };
    let original_policy = policy_source
        .downcast_ref::<crate::DomainPolicyRejection>()
        .unwrap();
    assert_eq!(original_policy.conflict(), DomainConflict::UnknownMember);
    assert_eq!(original_policy.detail(), private_detail);

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
    assert_eq!(refused.detail().unwrap().as_str(), private_detail);
    assert!(
        fixture.task_host.prepared_bodies().is_empty(),
        "membership is refused before host interpretation"
    );
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture
            .task_host
            .capabilities_installed
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.registry.counters().creations_rolled_back,
        0,
        "accepted failure retains a terminal identity"
    );
    assert!(!fixture.registry.has_live_task(identity));
    let terminal = source
        .latest(identity)
        .expect("accepted failed Task is observable");
    assert_failed_preparation_status(&terminal, identity, "Invariant failure (reason 10:63)");
    assert_ne!(
        refused.detail().unwrap().as_str(),
        match terminal.termination().unwrap() {
            TerminationDetail::Failed(failure) => failure.detail().as_str(),
            other => panic!("expected failure, got {other:?}"),
        }
    );
    let replay_request = fixture.create(descriptor(identity), vec![split_batch(1, 1, 1)]);
    let replay = fixture
        .registry
        .create_task(&replay_request, body(RESULT_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(
        replay.acknowledgement().unwrap().current_status(),
        &terminal
    );
    assert!(fixture.task_host.prepared_bodies().is_empty());
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 0);

    // A different identity can install normally; the failed one stays spent.
    let legal_identity = TaskIdentity::new(
        execution,
        identity.stage_id(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let legal_descriptor = descriptor_with(legal_identity, UniqueId::new(1, 2), 1, 1);
    let accepted = fixture.registry.create_task(
        &fixture.create(legal_descriptor, vec![split_batch(1, 1, 1)]),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert!(fixture.registry.has_live_task(legal_identity));
    let replay_after_install = fixture
        .registry
        .create_task(&replay_request, body(REFUSED_PLAN));
    assert_eq!(replay_after_install.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        replay_after_install.acknowledgement(),
        replay.acknowledgement()
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(RESULT_PLAN)]
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 1);
}

/// Host refusal preserves its typed author and spends the accepted identity.
/// Only a different identity may interpret another preparation body.
#[test]
fn a_refused_accepted_preparation_retains_its_failure_and_only_a_new_identity_installs() {
    use novarocks_execution_contract::TaskConclusionPort;

    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        ..TestTaskHost::default()
    });
    let execution = execution(31);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established status source");
    let request = fixture.create(descriptor(identity), Vec::new());
    let creator = Arc::clone(&fixture.registry);
    let create = std::thread::spawn(move || creator.create_task(&request, body(REFUSED_PLAN)));
    gate.wait_until_entered();
    let status = fixture
        .task_host
        .preparation_stop(identity)
        .control()
        .0
        .expect("hold the actual accepted owner");
    assert_eq!(status.current().state(), TaskState::Planned);
    assert_eq!(source.latest(identity), Some(status.current()));
    assert!(!status.convergence().actual_stopped());
    gate.release();
    let refused = create.join().expect("refused preparation thread");
    assert_eq!(
        refused.outcome(),
        OperationOutcome::InvalidStateOrRequest,
        "{refused:?}"
    );
    assert!(refused.acknowledgement().is_none());
    assert_eq!(
        refused.detail().unwrap().as_str(),
        "test static plan is not decodable"
    );
    let original = assert_refused_plan_origin(&status);
    let terminal = source.latest(identity).expect("retained failure");
    assert_eq!(terminal, status.current());
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0,
        "the host recorded no receiver for Worker to undo"
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.registry.counters().creations_rolled_back, 0);
    assert!(!fixture.registry.has_live_task(identity));
    let replay_request = fixture.create(descriptor(identity), Vec::new());
    let replay = fixture
        .registry
        .create_task(&replay_request, body(RESULT_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(
        replay.acknowledgement().unwrap().current_status(),
        &terminal
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(REFUSED_PLAN)]
    );
    let replayed_source = status.accepted().unwrap();
    assert!(Arc::ptr_eq(&original, &replayed_source));
    assert!(std::ptr::eq(
        original.originating_failure().unwrap().cause(),
        replayed_source.originating_failure().unwrap().cause()
    ));

    let legal_identity = TaskIdentity::new(
        execution,
        identity.stage_id(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let legal_descriptor = descriptor_with(legal_identity, UniqueId::new(1, 2), 1, 1);
    let accepted = fixture.registry.create_task(
        &fixture.create(legal_descriptor, Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(
        accepted.outcome(),
        OperationOutcome::Accepted,
        "{accepted:?}"
    );
    assert!(fixture.registry.has_live_task(legal_identity));
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    let replay_after_install = fixture
        .registry
        .create_task(&replay_request, body(REFUSED_PLAN));
    assert_eq!(replay_after_install.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        replay_after_install.acknowledgement(),
        replay.acknowledgement()
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![
            Bytes::from_static(REFUSED_PLAN),
            Bytes::from_static(RESULT_PLAN)
        ],
        "the failed identity never reinterprets any replay body"
    );
    assert!(Arc::ptr_eq(&original, &assert_refused_plan_origin(&status)));
}

/// A follower can observe the original operation refusal or its retained Task
/// terminal, but can never win a second creation for the spent identity.
#[test]
fn a_follower_of_failed_accepted_preparation_never_reinterprets_its_body() {
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(Arc::clone(&gate));
    let fixture = Fixture::with_config(
        TestTaskHost {
            install_gate: Some(Arc::clone(&gate)),
            ..TestTaskHost::default()
        },
        |config| {
            config.gate_poll_interval = Duration::from_secs(3600);
        },
    );
    let execution = execution(33);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let source = fixture
        .registry
        .status_source(context)
        .expect("established status source");
    let owner_request = fixture.create(descriptor(identity), Vec::new());
    let owner_registry = Arc::clone(&fixture.registry);
    let owner =
        std::thread::spawn(move || owner_registry.create_task(&owner_request, body(REFUSED_PLAN)));
    gate.wait_until_entered();
    let status = fixture
        .task_host
        .preparation_stop(identity)
        .control()
        .0
        .expect("hold the actual accepted owner");
    assert_eq!(source.latest(identity), Some(status.current()));
    assert!(!status.convergence().actual_stopped());
    let follower_request = fixture.create(descriptor(identity), Vec::new());
    let follower_registry = Arc::clone(&fixture.registry);
    let follower = std::thread::spawn(move || {
        follower_registry.create_task(&follower_request, body(RESULT_PLAN))
    });
    while fixture.registry.in_flight_operations(context) < 2 {
        std::thread::yield_now();
    }
    gate.release();
    let failed = owner.join().expect("owner create thread");
    let followed = follower.join().expect("follower create thread");
    assert_eq!(
        failed.outcome(),
        OperationOutcome::InvalidStateOrRequest,
        "{failed:?}"
    );
    assert!(failed.acknowledgement().is_none());
    assert_eq!(
        failed.detail().unwrap().as_str(),
        "test static plan is not decodable"
    );
    let original = assert_refused_plan_origin(&status);
    let terminal = source.latest(identity).expect("retained Task failure");
    assert_eq!(terminal, status.current());
    match followed.outcome() {
        OperationOutcome::InvalidStateOrRequest => {
            assert!(followed.acknowledgement().is_none());
            assert_eq!(followed.detail(), failed.detail());
        }
        OperationOutcome::Idempotent => {
            assert!(followed.detail().is_none());
            assert_eq!(followed.acknowledgement().unwrap().identity(), identity);
            assert_eq!(
                followed.acknowledgement().unwrap().current_status(),
                &terminal
            );
            assert!(followed.acknowledgement().unwrap().domains().is_empty());
        }
        other => panic!(
            "a follower cannot create a new round for a spent identity: {other:?}: {followed:?}"
        ),
    }
    let replay = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(RESULT_PLAN),
    );
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
    assert_eq!(
        replay.acknowledgement().unwrap().current_status(),
        &terminal
    );
    assert_eq!(
        fixture.task_host.prepared_bodies(),
        vec![Bytes::from_static(REFUSED_PLAN)]
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.task_host.receivers_installed.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0
    );
    assert!(!fixture.registry.has_live_task(identity));
    assert!(Arc::ptr_eq(&original, &assert_refused_plan_origin(&status)));
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
    let update_operation = TaskOperationId::new_v7();
    let advanced = fixture.registry.update_task(
        &UpdateTask::try_new(update_operation, identity, vec![split_batch(1, 2, 2)])
            .expect("legal update"),
    );
    assert_eq!(
        advanced.outcome(),
        OperationOutcome::Accepted,
        "{advanced:?}"
    );
    assert_eq!(fixture.task_host.domains_applied.load(Ordering::SeqCst), 3);
    // The real host receives the owning request's identity for both initial
    // domains and later updates, independent of development controls.
    assert_eq!(
        *fixture
            .task_host
            .applied_domain_operations
            .lock()
            .expect("applied operations"),
        vec![
            request.envelope().operation_id(),
            request.envelope().operation_id(),
            update_operation
        ]
    );
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
    assert_eq!(
        *fixture
            .task_host
            .applied_domain_operations
            .lock()
            .expect("applied operations"),
        vec![
            request.envelope().operation_id(),
            request.envelope().operation_id(),
            update_operation
        ],
        "a creation replay must not synthesize or apply a second operation"
    );

    // The advanced watermark still stands: the next batch after it applies.
    let next_operation = TaskOperationId::new_v7();
    let next = fixture.registry.update_task(
        &UpdateTask::try_new(next_operation, identity, vec![split_batch(1, 3, 3)])
            .expect("legal update"),
    );
    assert_eq!(next.outcome(), OperationOutcome::Accepted, "{next:?}");
    assert_eq!(
        *fixture
            .task_host
            .applied_domain_operations
            .lock()
            .expect("applied operations"),
        vec![
            request.envelope().operation_id(),
            request.envelope().operation_id(),
            update_operation,
            next_operation
        ]
    );
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
    let host_stop = fixture.task_host.preparation_stop(identity);
    assert!(host_stop.borrow_control().checkpoint().is_ok());
    assert_eq!(
        fixture
            .registry
            .status_source(context)
            .unwrap()
            .latest(identity)
            .unwrap()
            .termination()
            .cloned(),
        None
    );
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
    assert_eq!(
        fixture
            .registry
            .status_source(context)
            .unwrap()
            .latest(identity)
            .unwrap()
            .termination()
            .cloned(),
        Some(TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        ))
    );
    assert!(host_stop.borrow_control().checkpoint().is_err());
    assert!(fixture.registry.has_preparation_charge(identity));
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
fn actual_cancel_with_strict_input_uses_abort_cleanup_and_read_quiesce_keeps_normal_close() {
    use novarocks_execution_contract::ExchangeDeliveryRequirement;
    for delivery in [
        ExchangeDeliveryRequirement::DemandDriven,
        ExchangeDeliveryRequirement::MustDrain,
    ] {
        let fixture = Fixture::new(TestTaskHost::default());
        let execution = execution(1189);
        let context = fixture.context(execution);
        establish(&fixture.registry, context);
        let identity = task(execution, fixture.backend);
        let source = TaskIdentity::new(
            execution,
            StageId::new(2).unwrap(),
            TaskId::new(1).unwrap(),
            fixture.backend,
        );
        let input = ExchangeInbound::try_new(
            FragmentNodeId::new(7),
            vec![ExchangeSource::new(source, UniqueId::new(11, 12), 0)],
            delivery,
        )
        .unwrap();
        let descriptor = TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 1),
            std::num::NonZeroUsize::MIN,
            vec![PlanNodeId::new(1).unwrap()],
            ExchangeTopology::try_new(Vec::new(), vec![input]).unwrap(),
        )
        .unwrap();
        let created = fixture
            .registry
            .create_task(&fixture.create(descriptor, Vec::new()), body(STREAM_PLAN));
        assert_eq!(created.outcome(), OperationOutcome::Accepted);
        let reporter = fixture.task_host.reporters.lock().unwrap()[0].clone();
        reporter.running();
        reporter.conclude_termination(
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            novarocks_execution_contract::TaskOutputFacts::default(),
        );
        reporter.release_output();
        reporter.note_actual_stopped();
        reporter.note_resources_converged();
        assert_eq!(fixture.registry.advance_deadlines().tasks_retired, 1);
        assert_eq!(
            fixture
                .task_host
                .normal_retirement_calls
                .load(Ordering::SeqCst),
            usize::from(delivery == ExchangeDeliveryRequirement::DemandDriven)
        );
        assert_eq!(
            fixture.task_host.receivers_removed.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            reporter.current().termination(),
            Some(&TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        );
        assert!(fixture.health.is_healthy());
    }
}

#[test]
fn checked_close_failure_keeps_ingress_until_original_context_abort() {
    let fixture = Fixture::new(TestTaskHost {
        normal_retirement_rejection: true,
        ..Default::default()
    });
    let execution = execution(1190);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let created = fixture.registry.create_task(
        &fixture.create(descriptor(identity), Vec::new()),
        body(STREAM_PLAN),
    );
    assert_eq!(created.outcome(), OperationOutcome::Accepted);
    let reporter = fixture.task_host.reporters.lock().unwrap()[0].clone();
    reporter.running();
    reporter.conclude_success(novarocks_execution_contract::TaskOutputFacts::new(true));
    reporter.release_output();
    reporter.note_actual_stopped();
    reporter.note_resources_converged();
    assert_eq!(fixture.registry.advance_deadlines().tasks_retired, 0);
    assert!(!fixture.health.is_healthy());
    assert_eq!(
        fixture
            .task_host
            .normal_retirement_calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        0
    );
    // An unchanged revision does not spin a failed retirement retry.
    assert_eq!(fixture.registry.advance_deadlines().tasks_retired, 0);
    assert_eq!(
        fixture
            .task_host
            .normal_retirement_calls
            .load(Ordering::SeqCst),
        1
    );
    fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        reporter.current().state(),
        TaskState::Finished,
        "cleanup cannot rewrite a terminal conclusion"
    );
    assert_eq!(
        fixture
            .task_host
            .normal_retirement_calls
            .load(Ordering::SeqCst),
        1
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
    let accepted_status = fixture
        .registry
        .status_source(context)
        .unwrap()
        .latest(identity)
        .unwrap();
    assert_eq!(accepted_status.state(), TaskState::Canceling);
    assert!(accepted_status.installed());
    assert_eq!(
        accepted_status.termination(),
        Some(&TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        ))
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
fn accepted_preparation_projects_author_facts_and_keeps_phase_and_identity_across_all_stages() {
    use PreparationFailureStage::{Capability, Domain, Receiver, Submit};
    for (category_index, category) in [
        TaskFailureCategory::Protocol,
        TaskFailureCategory::Exchange,
        TaskFailureCategory::Execution,
        TaskFailureCategory::Internal,
        TaskFailureCategory::Unsupported,
        TaskFailureCategory::ResourceExhausted,
    ]
    .into_iter()
    .enumerate()
    {
        for (stage_index, stage) in [Receiver, Capability, Domain, Submit]
            .into_iter()
            .enumerate()
        {
            let fixture = Fixture::new(TestTaskHost {
                preparation_rejection: Some((
                    stage,
                    fixture_host_rejection(
                        category,
                        "ResourceExhausted: identical typed host diagnostic",
                    ),
                )),
                ..TestTaskHost::default()
            });
            let execution = execution(90_000 + category_index as i64 * 4 + stage_index as i64);
            establish(&fixture.registry, fixture.context(execution));
            let identity = task(execution, fixture.backend);
            let request = fixture.create(descriptor(identity), vec![split_batch(1, 1, 1)]);
            assert_eq!(
                fixture
                    .registry
                    .accept_create_task(&request, body(RESULT_PLAN))
                    .outcome(),
                OperationOutcome::Accepted
            );
            wait_for_accepted_state(&fixture.registry, &request, TaskState::Failed);
            let replay = fixture
                .registry
                .accept_create_task(&request, body(STREAM_PLAN));
            assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
            let status = replay
                .acknowledgement()
                .expect("same accepted terminal identity");
            assert!(!status.installed());
            let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                panic!("host refusal remains a failure");
            };
            let expected_task_category = match category {
                TaskFailureCategory::Protocol | TaskFailureCategory::Internal => {
                    TaskFailureCategory::Internal
                }
                TaskFailureCategory::Exchange | TaskFailureCategory::Execution => {
                    TaskFailureCategory::Execution
                }
                TaskFailureCategory::ResourceExhausted => TaskFailureCategory::ResourceExhausted,
                TaskFailureCategory::Unsupported => TaskFailureCategory::Unsupported,
            };
            assert_eq!(failure.category(), expected_task_category);
            assert_eq!(failure.phase(), TaskFailurePhase::Preparation);
            let expected_summary = match category {
                TaskFailureCategory::Protocol => "Invariant failure (reason 10:65004)",
                TaskFailureCategory::Exchange => "Dependency failure (reason 10:65003)",
                TaskFailureCategory::Execution => "Data failure (reason 10:65001)",
                TaskFailureCategory::Internal => "Unexpected failure (reason 10:65005)",
                TaskFailureCategory::ResourceExhausted => "Resource failure (reason 10:65002)",
                TaskFailureCategory::Unsupported => "Unsupported failure (reason 10:65006)",
            };
            assert_eq!(failure.detail().as_str(), expected_summary);
            assert!(
                !failure
                    .detail()
                    .as_str()
                    .contains("identical typed host diagnostic")
            );
            assert_eq!(
                fixture.task_host.prepared_bodies(),
                vec![Bytes::from_static(RESULT_PLAN)]
            );
            assert_eq!(
                *fixture.task_host.preparation_stages.lock().unwrap(),
                [Receiver, Capability, Domain, Submit][..=stage_index]
            );
            assert_eq!(
                fixture.task_host.receivers_removed.load(Ordering::SeqCst),
                usize::from(stage != Receiver)
            );
            assert_eq!(
                fixture
                    .task_host
                    .capabilities_removed
                    .load(Ordering::SeqCst),
                1
            );
            assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
            let cancel = CancelTask::new(
                TaskOperationId::new_v7(),
                identity,
                CancelReason::UpstreamNoLongerNeeded,
            );
            fixture.registry.cancel_task(&cancel);
            let after = fixture
                .registry
                .accept_create_task(&request, body(REFUSED_PLAN));
            assert_eq!(after.acknowledgement(), replay.acknowledgement());
            for _ in 0..1000 {
                if fixture.registry.preparation_snapshot().workers == 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let charges = fixture.registry.preparation_snapshot();
            assert_eq!(charges.workers, 0);
            assert_eq!(charges.positions, 0);
            assert_eq!(charges.context_positions, 0);
            assert_eq!(charges.bytes, 0);
        }
    }
}

#[test]
fn accepted_preparation_category_retention_keeps_original_synchronous_operation_verdicts() {
    use PreparationFailureStage::{Capability, Domain, Receiver, Submit};
    for category in [
        TaskFailureCategory::Protocol,
        TaskFailureCategory::Exchange,
        TaskFailureCategory::Execution,
        TaskFailureCategory::Internal,
        TaskFailureCategory::Unsupported,
        TaskFailureCategory::ResourceExhausted,
    ] {
        for stage in [Receiver, Capability, Domain, Submit] {
            let fixture = Fixture::new(TestTaskHost {
                preparation_rejection: Some((
                    stage,
                    fixture_host_rejection(
                        category,
                        "ResourceExhausted: identical typed host diagnostic",
                    ),
                )),
                ..TestTaskHost::default()
            });
            let execution = execution(90_100);
            establish(&fixture.registry, fixture.context(execution));
            let request = fixture.create(
                descriptor(task(execution, fixture.backend)),
                vec![split_batch(1, 1, 1)],
            );
            let refused = fixture.registry.create_task(&request, body(RESULT_PLAN));
            let expected = if matches!(stage, Domain | Submit)
                && category == TaskFailureCategory::ResourceExhausted
            {
                OperationOutcome::ResourceExhausted
            } else {
                OperationOutcome::InvalidStateOrRequest
            };
            assert_eq!(refused.outcome(), expected, "{stage:?} {category:?}");
            assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
            // The synchronous operation verdict is separate from the actual
            // Task acceptance domain; this API owns no queued-worker position.
            assert_eq!(fixture.registry.preparation_snapshot().positions, 0);
        }
    }
}

#[test]
fn accepted_preparation_stop_before_host_refusal_keeps_original_cancel_winner() {
    let gate = Arc::new(InstallGate::held());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(Arc::clone(&gate)),
        preparation_rejection: Some((
            PreparationFailureStage::Receiver,
            fixture_host_rejection(
                TaskFailureCategory::Internal,
                "ResourceExhausted: identical typed host diagnostic",
            ),
        )),
        ..TestTaskHost::default()
    });
    let execution = execution(90_101);
    establish(&fixture.registry, fixture.context(execution));
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
    assert_eq!(
        fixture
            .registry
            .cancel_task(&CancelTask::new(
                TaskOperationId::new_v7(),
                identity,
                CancelReason::UpstreamNoLongerNeeded
            ))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.release();
    wait_for_accepted_state(&fixture.registry, &request, TaskState::Canceled);
    let replay = fixture
        .registry
        .accept_create_task(&request, body(STREAM_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(
        replay.acknowledgement().unwrap().termination(),
        Some(&TerminationDetail::Canceled(
            CancelReason::UpstreamNoLongerNeeded
        ))
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
}

/// The real Worker rollback gate observes the original author before cleanup,
/// rather than letting a flattened create receipt decide the Task afterward.
#[test]
fn originating_host_failure_is_accepted_before_worker_rollback_begins() {
    use PreparationFailureStage::{Capability, Domain, Receiver, Submit};
    use novarocks_execution::runtime::fragment::ExecutionFailure;
    use novarocks_execution_contract::failure::{FailureClass, FailureEvidence, FailureProjection};
    for (case, (original, category, expected_class)) in [
        (
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted),
            TaskFailureCategory::ResourceExhausted,
            FailureClass::Resource,
        ),
        (
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            ),
            TaskFailureCategory::Internal,
            FailureClass::Invariant,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let expected_fact = original.failure_fact();
        assert_eq!(expected_fact.class(), expected_class);
        assert_eq!(expected_fact.evidence(), FailureEvidence::Originating);
        for (stage_index, stage) in [Receiver, Capability, Domain, Submit]
            .into_iter()
            .enumerate()
        {
            let rollback = Arc::new(InstallGate::held());
            let _release = PreparationGateRelease(rollback.clone());
            let fixture = Fixture::new(TestTaskHost {
                preparation_rejection: Some((
                    stage,
                    HostRejection::from_failure(category, original.clone()),
                )),
                rollback_gate: Some((TaskId::new(1).unwrap(), rollback.clone())),
                ..TestTaskHost::default()
            });
            let execution = execution(90_200 + case as i64 * 4 + stage_index as i64);
            let context = fixture.context(execution);
            establish(&fixture.registry, context);
            let identity = task(execution, fixture.backend);
            let request = fixture.create(descriptor(identity), vec![split_batch(1, 1, 1)]);
            let accepted = fixture
                .registry
                .accept_create_task(&request, body(RESULT_PLAN));
            assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
            rollback.wait_until_entered();
            let view = fixture.task_host.preparation_stop(identity);
            let observed = view
                .borrow_control()
                .checkpoint()
                .expect_err("the actual source must be accepted before any Worker cleanup");
            assert!(
                std::ptr::eq(observed.cause(), original.cause()),
                "{stage:?}: retain the original local occurrence"
            );
            assert_eq!(observed.failure_fact(), expected_fact);
            let before = fixture
                .registry
                .status_source(context)
                .unwrap()
                .latest(identity)
                .unwrap();
            assert_eq!(before.state(), TaskState::Failing);
            let primary = before.termination().cloned().unwrap();
            assert!(
                matches!(&primary, TerminationDetail::Failed(failure) if failure.phase() == TaskFailurePhase::Preparation)
            );
            assert!(fixture.registry.has_preparation_charge(identity));
            assert_eq!(
                fixture.task_host.receivers_removed.load(Ordering::SeqCst),
                0
            );
            let cancelled = fixture.registry.cancel_task(&CancelTask::new(
                TaskOperationId::new_v7(),
                identity,
                CancelReason::UpstreamNoLongerNeeded,
            ));
            assert_eq!(
                cancelled.outcome(),
                OperationOutcome::Idempotent,
                "the actual Task receipt, rather than a Cell flag, says the originating failure already won"
            );
            assert!(
                std::ptr::eq(
                    view.borrow_control().checkpoint().unwrap_err().cause(),
                    original.cause()
                ),
                "late cancellation cannot replace an already accepted originating source"
            );
            rollback.release();
            wait_for_terminal_record(&fixture.registry, identity);
            wait_for_preparation_exit(&fixture.registry, identity);
            let after = fixture
                .registry
                .status_source(context)
                .unwrap()
                .latest(identity)
                .unwrap();
            assert_eq!(after.state(), TaskState::Failed);
            assert_eq!(after.termination(), Some(&primary));
        }
    }
}

struct DropHeldContent(Arc<InstallGate>);
impl std::fmt::Debug for DropHeldContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DropHeldContent")
    }
}
impl CodecOwnedContent for DropHeldContent {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([211; 16])
    }
    fn encoded_len(&self) -> usize {
        16
    }
}
impl Drop for DropHeldContent {
    fn drop(&mut self) {
        self.0.wait_for_release();
    }
}

/// A terminal cause is not evidence that the accepted preparation job exited.
/// The codec payload's destructor runs on that actual job after rollback;
/// parking it keeps the job and its bytes alive without a clock assumption.
#[test]
fn rollback_does_not_retire_task_while_the_preparation_request_is_still_dropping() {
    let install = Arc::new(InstallGate::held());
    let exiting = Arc::new(InstallGate::held());
    let _release_install = PreparationGateRelease(install.clone());
    let _release_exit = PreparationGateRelease(exiting.clone());
    let original: novarocks_execution::runtime::fragment::ExecutionFailure =
        novarocks_functions::KernelFailure::ResourceExhausted.into();
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(install.clone()),
        preparation_rejection: Some((
            PreparationFailureStage::Receiver,
            HostRejection::from_failure(TaskFailureCategory::ResourceExhausted, original.clone()),
        )),
        ..TestTaskHost::default()
    });
    let execution = execution(90_220);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let domain = TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
        PlanNodeId::new(1).unwrap(),
        SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, false).unwrap(),
        Arc::new(DropHeldContent(exiting.clone())),
    ));
    let request = fixture.create(descriptor(identity), vec![domain]);
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&request, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    install.wait_until_entered();
    let view = fixture.task_host.preparation_stop(identity);
    // The accepted job is now the last holder of its frozen domain payload.
    drop(request);
    install.release();
    exiting.wait_until_entered();
    assert!(
        fixture.registry.has_preparation_charge(identity),
        "the real preparation job has not exited"
    );
    assert!(
        matches!(
            fixture.registry.root_result_route(identity),
            RootResultRoute::Creating
        ),
        "rollback cannot retire a Task while its real preparation input is still dropping"
    );
    assert!(
        std::ptr::eq(
            view.borrow_control().checkpoint().unwrap_err().cause(),
            original.cause()
        ),
        "actual in-flight work retains its one Task owner and original source"
    );
    exiting.release();
    wait_for_preparation_exit(&fixture.registry, identity);
    wait_for_terminal_record(&fixture.registry, identity);
}

#[test]
fn installed_runtime_stays_dormant_until_its_preparation_request_actually_exits() {
    let install = Arc::new(InstallGate::held());
    let exiting = Arc::new(InstallGate::held());
    let _release_install = PreparationGateRelease(install.clone());
    let _release_exit = PreparationGateRelease(exiting.clone());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(install.clone()),
        ..TestTaskHost::default()
    });
    let execution = execution(90_221);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let domain = TaskDomainUpdate::SplitAssignment(SplitAssignmentIntent::new(
        PlanNodeId::new(1).unwrap(),
        SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, false).unwrap(),
        Arc::new(DropHeldContent(exiting.clone())),
    ));
    let request = fixture.create(descriptor(identity), vec![domain]);
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&request, body(RESULT_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    install.wait_until_entered();
    drop(request);
    install.release();
    exiting.wait_until_entered();
    assert!(
        fixture.registry.has_live_task(identity),
        "the runtime is installed while its owned preparation is still exiting"
    );
    assert!(
        fixture
            .registry
            .status_source(context)
            .unwrap()
            .latest(identity)
            .unwrap()
            .installed()
    );
    assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.task_host.creation_commits.load(Ordering::SeqCst),
        0,
        "Installed cannot authorize execution before the preparation input exits"
    );
    assert!(fixture.registry.has_preparation_charge(identity));
    exiting.release();
    wait_for_preparation_exit(&fixture.registry, identity);
    for _ in 0..1000 {
        if fixture.task_host.creation_commits.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fixture.task_host.creation_commits.load(Ordering::SeqCst), 1);
}

#[test]
fn preparation_panic_disposes_its_actual_payload_after_acceptance_before_rollback() {
    use novarocks_execution_contract::failure::{
        FailureClass, FailureProjection, FailureReason, domain,
    };
    let rollback = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(rollback.clone());
    let dropped = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::new(TestTaskHost {
        preparation_panic: Some((PreparationFailureStage::Receiver, dropped.clone())),
        rollback_gate: Some((TaskId::new(1).unwrap(), rollback.clone())),
        ..TestTaskHost::default()
    });
    let execution = execution(90_222);
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
    rollback.wait_until_entered();
    let view = fixture.task_host.preparation_stop(identity);
    let observed = view
        .borrow_control()
        .checkpoint()
        .expect_err("the panic source must reach the Task before Worker cleanup");
    let fact = observed.failure_fact();
    assert_eq!(
        fact.class(),
        FailureClass::Unexpected,
        "arbitrary panic text is not a resource classifier"
    );
    assert_eq!(
        fact.reason(),
        FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(147).unwrap())
    );
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "the original opaque payload has actually exited before rollback; the Task source is typed facts only"
    );
    assert!(fixture.registry.has_preparation_charge(identity));
    rollback.release();
    wait_for_preparation_exit(&fixture.registry, identity);
    wait_for_terminal_record(&fixture.registry, identity);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "a diagnostic clone must not prolong execution captures"
    );
    drop(observed);
    for _ in 0..1000 {
        if dropped.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "one original payload exits, with no reconstructed replacement"
    );
}

/// Reuse the real owner's nonblocking management snapshot: callback execution
/// can acquire the exact Registry fence and reenter its public reads. The
/// installed and preparing cases share the same acceptance/notification rule.
#[test]
fn task_control_callbacks_reenter_registry_after_each_context_or_cancel_fence() {
    use novarocks_execution_contract::{TaskConclusion, TaskConclusionPort, TaskFailurePhase};
    for preparing in [true, false] {
        for event in 0..3 {
            let gate = Arc::new(InstallGate::held());
            let _release = PreparationGateRelease(gate.clone());
            let fixture = Fixture::new(TestTaskHost {
                install_gate: preparing.then_some(gate.clone()),
                ..TestTaskHost::default()
            });
            let execution = execution(90_230 + event + if preparing { 0 } else { 3 });
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
            if preparing {
                gate.wait_until_entered();
            } else {
                wait_for_preparation_exit(&fixture.registry, identity);
            }
            let view = fixture.task_host.preparation_stop(identity);
            let owner = view.control().0.unwrap();
            let expected = if event == 2 {
                TerminationDetail::Aborted(AbortCause::QueryFailed)
            } else {
                TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded)
            };
            let expected_context = match event {
                0 => QueryContextState::Active,
                1 => QueryContextState::Quiescing,
                _ => QueryContextState::Aborting,
            };
            let callbacks = Arc::new(AtomicUsize::new(0));
            let observed = callbacks.clone();
            let registry = Arc::downgrade(&fixture.registry);
            let task = Arc::downgrade(&owner);
            owner
                .execution_control(TaskFailurePhase::Preparation)
                .stop_observable()
                .add_observer(Arc::new(move || {
                    let registry = registry.upgrade().unwrap();
                    let snapshot = registry
                        .try_preparation_snapshot()
                        .expect("registry is not poisoned")
                        .expect("Task callback runs after Registry unlock");
                    assert_eq!(snapshot.positions, usize::from(preparing));
                    assert_eq!(registry.context_state(context), expected_context);
                    let route = registry.root_result_route(identity);
                    if preparing {
                        assert!(matches!(route, RootResultRoute::Creating));
                    } else if event == 2 {
                        assert!(matches!(route, RootResultRoute::AwaitTerminalControl));
                    } else {
                        assert!(matches!(route, RootResultRoute::Serve(_)));
                    }
                    let accepted = task.upgrade().unwrap().accepted().unwrap();
                    assert_eq!(
                        accepted.neutral().conclusion(),
                        &TaskConclusion::Terminated(expected.clone())
                    );
                    observed.fetch_add(1, Ordering::SeqCst);
                }));
            let outcome = match event {
                0 => fixture
                    .registry
                    .cancel_task(&CancelTask::new(
                        TaskOperationId::new_v7(),
                        identity,
                        CancelReason::UpstreamNoLongerNeeded,
                    ))
                    .outcome(),
                1 => fixture
                    .registry
                    .quiesce_query_context(&QuiesceQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                    ))
                    .outcome(),
                _ => fixture
                    .registry
                    .abort_query_context(&AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        AbortCause::QueryFailed,
                    ))
                    .outcome(),
            };
            assert_eq!(outcome, OperationOutcome::Accepted);
            assert_eq!(
                callbacks.load(Ordering::SeqCst),
                1,
                "one accepted cause notifies after the original fence: preparing={preparing} event={event}"
            );
            if preparing {
                gate.release();
                wait_for_preparation_exit(&fixture.registry, identity);
                wait_for_terminal_record(&fixture.registry, identity);
            }
        }
    }
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
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
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
        let host_stop = fixture.task_host.preparation_stop(identity);
        assert_eq!(
            fixture
                .registry
                .status_source(context)
                .unwrap()
                .latest(identity)
                .unwrap()
                .termination()
                .cloned(),
            None,
            "{trace:?}"
        );
        let quiesce = QuiesceQueryContext::new(TaskOperationId::new_v7(), context);
        let mut expired = false;
        // Independent event-order oracle. Context expiry still happens,
        // but cannot replace a normal Task cancellation accepted earlier.
        let mut expected_stop = None;
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
                        expected_stop.get_or_insert(TerminationDetail::Canceled(
                            CancelReason::UpstreamNoLongerNeeded,
                        ));
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
                    expected_stop.get_or_insert(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    ));
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
                    expected_stop
                        .get_or_insert(TerminationDetail::Aborted(AbortCause::LeaseExpired));
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
                host_stop.borrow_control().checkpoint().is_err(),
                expected_stop.is_some(),
                "{trace:?}: preparation reads the accepted Task decision"
            );
            assert_eq!(
                fixture
                    .registry
                    .status_source(context)
                    .unwrap()
                    .latest(identity)
                    .unwrap()
                    .termination()
                    .cloned(),
                expected_stop,
                "{trace:?} after {event:?}: decoding must observe the original stop"
            );
            assert!(
                fixture.registry.has_preparation_charge(identity),
                "{trace:?}"
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
        let terminal = fixture
            .registry
            .status_source(context)
            .unwrap()
            .latest(identity)
            .expect("retained terminal");
        let expected_detail = expected_stop.unwrap();
        let expected_state = match &expected_detail {
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded) => {
                TaskState::Canceled
            }
            TerminationDetail::Aborted(AbortCause::LeaseExpired) => TaskState::Aborted,
            other => panic!("the declared trace has no other intent: {other:?}"),
        };
        assert_eq!(
            (terminal.state(), terminal.termination()),
            (expected_state, Some(&expected_detail)),
            "{trace:?}: physical exit keeps the first accepted Task cause"
        );
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
                    Arc::new(
                        novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                    ),
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

/// Accepted cancellation stays the Task conclusion while a later context peer
/// failure strengthens physical stand-down. Actual preparation rollback and
/// exit remain independently observed, including a preparation refusal.
#[test]
fn preparation_rollback_concludes_on_the_stand_down_that_won() {
    for (case, (plan, escalate)) in [
        (STREAM_PLAN, true),
        (REFUSED_PLAN, true),
        (STREAM_PLAN, false),
        (REFUSED_PLAN, false),
    ]
    .into_iter()
    .enumerate()
    {
        let install_gate = Arc::new(InstallGate::held());
        let rollback_gate = Arc::new(InstallGate::held());
        let _install_release = PreparationGateRelease(Arc::clone(&install_gate));
        let _rollback_release = PreparationGateRelease(Arc::clone(&rollback_gate));
        let preparing_task = TaskId::new(4).expect("nonzero task");
        let fixture = Fixture::new(TestTaskHost {
            install_gate: Some(Arc::clone(&install_gate)),
            install_gate_plan: Some(plan),
            rollback_gate: Some((preparing_task, Arc::clone(&rollback_gate))),
            ..TestTaskHost::default()
        });
        let query_execution = execution(31_000 + case as i64);
        let context = fixture.context(query_execution);
        establish(&fixture.registry, context);
        let source = fixture
            .registry
            .status_source(context)
            .expect("context source");

        let peer_identity = task(query_execution, fixture.backend);
        let peer = fixture.create(descriptor(peer_identity), Vec::new());
        assert_eq!(
            fixture
                .registry
                .accept_create_task(&peer, body(RESULT_PLAN))
                .outcome(),
            OperationOutcome::Accepted,
            "case {case}"
        );
        wait_for_accepted_installed(&fixture.registry, &peer);
        let peer_reporter = fixture.task_host.reporters.lock().expect("test reporters")[0].clone();
        assert!(matches!(
            peer_reporter.running(),
            crate::StatusAdvance::Published(_)
        ));

        let preparing_identity = TaskIdentity::new(
            query_execution,
            StageId::new(2).expect("nonzero stage"),
            preparing_task,
            fixture.backend,
        );
        let preparing = fixture.create(
            descriptor_with(preparing_identity, UniqueId::new(4, 4), 1, 1),
            Vec::new(),
        );
        assert_eq!(
            fixture
                .registry
                .accept_create_task(&preparing, body(plan))
                .outcome(),
            OperationOutcome::Accepted,
            "case {case}"
        );
        install_gate.wait_until_entered();

        let cancel = fixture.registry.cancel_task(&CancelTask::new(
            TaskOperationId::new_v7(),
            preparing_identity,
            CancelReason::UpstreamNoLongerNeeded,
        ));
        assert_eq!(cancel.outcome(), OperationOutcome::Accepted, "case {case}");
        assert_eq!(
            source.latest(preparing_identity).expect("accepted").state(),
            TaskState::Canceling,
            "case {case}"
        );

        // Preparation returns, observes the normal stand-down, and starts
        // rolling back; it is parked before it publishes the terminal.
        install_gate.release();
        rollback_gate.wait_until_entered();

        let peer_cause: novarocks_execution::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let peer_failure = novarocks_execution_contract::TaskFailure::new_in_phase(
            peer_cause.failure_fact(),
            peer_cause.safe_detail(),
            TaskFailurePhase::Execution,
        );
        if escalate {
            let peer_owner = fixture
                .task_host
                .preparation_stop(peer_identity)
                .control()
                .0
                .unwrap();
            let control = peer_owner.execution_control(TaskFailurePhase::Execution);
            let before_accepted = control.accepted();
            control.report_failure(peer_cause.clone());
            let accepted = control.accepted().unwrap();
            assert!(before_accepted.is_none());
            assert_eq!(accepted.neutral().generation().get(), 1);
            assert!(std::ptr::eq(
                accepted.originating_failure().unwrap().cause(),
                peer_cause.cause()
            ));
            assert_eq!(peer_reporter.current().state(), TaskState::Failing);
            assert_eq!(
                peer_reporter.current().termination(),
                Some(&TerminationDetail::Failed(peer_failure.clone()))
            );
            fixture.registry.advance_deadlines();
            assert_eq!(
                fixture.registry.termination_cause(context),
                Some(AbortCause::PeerTaskFailed),
                "case {case}"
            );
            assert_eq!(
                source.latest(preparing_identity).expect("accepted").state(),
                TaskState::Canceling,
                "case {case}: context fan-out cannot rewrite accepted cancellation"
            );
        }

        rollback_gate.release();
        wait_for_terminal_record(&fixture.registry, preparing_identity);
        wait_for_preparation_exit(&fixture.registry, preparing_identity);
        let terminal = source
            .latest(preparing_identity)
            .expect("retained terminal");
        let expected = (
            TaskState::Canceled,
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
        );
        assert_eq!(
            (terminal.state(), terminal.termination().cloned()),
            (expected.0, Some(expected.1)),
            "case {case}"
        );
        assert!(!terminal.installed(), "case {case}");
        assert_eq!(
            fixture.task_host.submitted.load(Ordering::SeqCst),
            1,
            "case {case}: only the peer was submitted"
        );

        if escalate {
            assert!(matches!(
                peer_reporter.failed(peer_failure),
                crate::StatusAdvance::Published(_)
            ));
            peer_reporter.release_output();
            peer_reporter.note_actual_stopped();
            peer_reporter.note_resources_converged();
            fixture.registry.advance_deadlines();
            assert_eq!(
                fixture.registry.context_state(context),
                QueryContextState::TerminalRetained,
                "case {case}: every task is a terminal record"
            );
        }
    }
}

#[test]
fn accepted_preparation_control_pending_stop_wakes_same_worker_and_keeps_charges_until_exit() {
    for kind in 0..3 {
        let entered = Arc::new(InstallGate::held());
        let exiting = Arc::new(InstallGate::held());
        let _release = PreparationGateRelease(Arc::clone(&exiting));
        let fixture = Fixture::with_config(
            TestTaskHost {
                pending_stop_gates: Some((Arc::clone(&entered), Arc::clone(&exiting))),
                ..TestTaskHost::default()
            },
            |config| {
                config.max_prepare_workers = 1;
                config.max_preparing_tasks_per_context = 1;
                config.gate_poll_interval = Duration::from_secs(30);
            },
        );
        let execution = execution(37_001 + kind);
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
        entered.wait_until_entered();
        let pending = fixture.registry.preparation_snapshot();
        assert_eq!(
            (
                pending.positions,
                pending.workers,
                pending.context_positions
            ),
            (1, 1, 1)
        );
        assert!(pending.bytes > 0);
        let stop = if kind == 1 {
            fixture
                .registry
                .abort_query_context(&AbortQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    AbortCause::QueryFailed,
                ))
                .outcome()
        } else if kind == 0 {
            fixture
                .registry
                .cancel_task(&CancelTask::new(
                    TaskOperationId::new_v7(),
                    identity,
                    CancelReason::UpstreamNoLongerNeeded,
                ))
                .outcome()
        } else {
            fixture.clock.advance(Duration::from_secs(10));
            assert_eq!(fixture.registry.advance_deadlines().leases_expired, 1);
            OperationOutcome::Accepted
        };
        assert_eq!(stop, OperationOutcome::Accepted);
        // Stop must notify the existing waiter rather than require the next
        // 30-second admission poll or another queued preparation worker.
        {
            let state = exiting.state.lock().unwrap();
            let (state, _) = exiting
                .changed
                .wait_timeout_while(state, Duration::from_secs(1), |state| !state.waiter_entered)
                .unwrap();
            assert!(
                state.waiter_entered,
                "original stop did not wake preparation"
            );
        }
        let stopped = fixture.registry.preparation_snapshot();
        assert_eq!(
            (stopped.positions, stopped.workers, stopped.bytes),
            (pending.positions, pending.workers, pending.bytes)
        );
        assert!(fixture.task_host.prepared_bodies().is_empty());
        assert_eq!(
            fixture.task_host.receivers_installed.load(Ordering::SeqCst),
            0
        );
        exiting.release();
        wait_for_terminal_record(&fixture.registry, identity);
        wait_for_preparation_exit(&fixture.registry, identity);
        wait_for_accepted_state(
            &fixture.registry,
            &request,
            if kind == 0 {
                TaskState::Canceled
            } else {
                TaskState::Aborted
            },
        );
        let exited = fixture.registry.preparation_snapshot();
        assert_eq!((exited.positions, exited.workers, exited.bytes), (0, 0, 0));
        assert_eq!(fixture.task_host.submitted.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn unresolved_exit_closes_new_admission_without_changing_owned_task_or_replay() {
    use novarocks_execution::runtime::{
        execution_health::ExecutionExitOrigin, fragment::ExecutionFailure,
    };
    use novarocks_execution_contract::failure::FailureProjection;

    let fixture = Fixture::new(TestTaskHost::default());
    let execution = execution(5081);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), Vec::new());
    let created = fixture.registry.create_task(&request, body(STREAM_PLAN));
    assert_eq!(created.outcome(), OperationOutcome::Accepted);
    let status = fixture.task_host.reporters.lock().unwrap()[0].clone();
    let before = status.current();
    let convergence = status.convergence();
    assert!(status.current().termination().is_none());
    let unknown = ExecutionFailure::from_domain(RegistryFixtureFailure {
        category: TaskFailureCategory::Internal,
        detail: "actual fixture exit observation failed".to_owned(),
    });
    fixture
        .health
        .report_unresolved(ExecutionExitOrigin::Process, unknown.failure_fact());
    let health = &fixture.health;
    let fault = health
        .failure()
        .expect("same runtime health received the Task-scoped fact");
    assert_eq!(fault.origin(), ExecutionExitOrigin::Process);
    assert_eq!(fault.fact(), unknown.failure_fact());
    assert_eq!(status.current(), before);
    assert_eq!(status.convergence(), convergence);
    assert!(status.current().termination().is_none());

    let replay = fixture.registry.create_task(&request, body(REFUSED_PLAN));
    assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
    assert_eq!(replay.acknowledgement(), created.acknowledgement());
    let next = TaskIdentity::new(
        execution,
        identity.stage_id(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let refused = fixture.registry.create_task(
        &fixture.create(descriptor(next), Vec::new()),
        body(STREAM_PLAN),
    );
    assert_eq!(refused.outcome(), OperationOutcome::NotReady);
    assert!(!fixture.registry.has_live_task(next));
    assert_eq!(fixture.task_host.prepared_bodies().len(), 1);
    assert_eq!(status.current(), before);
    assert_eq!(status.convergence(), convergence);
}

// A real child job owns data until its rendezvous is released. The observer
// itself holds only this original job's exit/cancel metadata, never a Task.
#[derive(Default)]
struct PreparationChildMetadata {
    exited: AtomicBool,
    rollback_requests: AtomicUsize,
    polls: AtomicUsize,
    adoptions: AtomicUsize,
    observer_drops: AtomicUsize,
    panic_on_observer_drop: AtomicBool,
    payload_drops: Arc<AtomicUsize>,
    order: Arc<Mutex<Vec<&'static str>>>,
}
struct PreparationChildObserver(Arc<PreparationChildMetadata>);
impl crate::TaskPreparationChild for PreparationChildObserver {
    fn request_rollback(&self) {
        self.0.rollback_requests.fetch_add(1, Ordering::AcqRel);
    }
    fn poll_exit(&self) -> crate::TaskPreparationChildExit {
        self.0.polls.fetch_add(1, Ordering::AcqRel);
        if self.0.exited.load(Ordering::Acquire) {
            crate::TaskPreparationChildExit::Exited
        } else {
            crate::TaskPreparationChildExit::Pending
        }
    }
    fn adopt_runtime(&self) {
        self.0.adoptions.fetch_add(1, Ordering::AcqRel);
        self.0.order.lock().unwrap().push("adopt");
    }
}
impl Drop for PreparationChildObserver {
    fn drop(&mut self) {
        self.0.observer_drops.fetch_add(1, Ordering::AcqRel);
        if self.0.panic_on_observer_drop.swap(false, Ordering::AcqRel) {
            std::panic::panic_any(PreparationPanicPayload {
                dropped: self.0.payload_drops.clone(),
                _detail: "opaque child observation destructor failure",
            });
        }
    }
}
struct PreparationChildJob {
    release: Option<std::sync::mpsc::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}
impl PreparationChildJob {
    fn start(metadata: Arc<PreparationChildMetadata>) -> Self {
        let (release, released) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            let actual_data = vec![7_u8; 1024];
            let _ = released.recv();
            // Whole-work receipt follows destruction of the actual data.
            drop(actual_data);
            metadata.exited.store(true, Ordering::Release);
        });
        Self {
            release: Some(release),
            join: Some(join),
        }
    }
    fn finish(mut self) -> std::thread::Result<()> {
        drop(self.release.take());
        self.join.take().unwrap().join()
    }
}
impl Drop for PreparationChildJob {
    fn drop(&mut self) {
        drop(self.release.take());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
fn wait_for_physical_preparation_body_exit(registry: &TaskExecutionRegistry) {
    for _ in 0..1000 {
        if registry.preparation_snapshot().workers == 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("the real preparation body did not leave its original worker");
}

#[test]
fn an_exited_preparation_child_cannot_outrun_the_real_parent_body() {
    // This is the child-only NoJobInstalled shape: no async child exists,
    // while the accepted parent's actual install body is still blocked.
    let metadata = Arc::new(PreparationChildMetadata::default());
    metadata.exited.store(true, Ordering::Release);
    let body_gate = Arc::new(InstallGate::held());
    let _release_body = PreparationGateRelease(body_gate.clone());
    let child: Arc<dyn crate::TaskPreparationChild> =
        Arc::new(PreparationChildObserver(metadata.clone()));
    let fixture = Fixture::new(TestTaskHost {
        preparation_child: Mutex::new(Some((TaskId::new(1).unwrap(), child))),
        install_gate: Some(body_gate.clone()),
        ..TestTaskHost::default()
    });
    let context = fixture.context(execution(59_101));
    establish(&fixture.registry, context);
    let identity = task(context.query_execution_id(), fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    let receipt = fixture
        .registry
        .accept_create_task(&request, body(REFUSED_PLAN));
    body_gate.wait_until_entered();
    fixture.registry.advance_deadlines();
    let held = fixture.registry.preparation_snapshot();
    let polls_while_parent_live = metadata.polls.load(Ordering::Acquire);
    let source = fixture.registry.status_source(context).unwrap();
    let stopped_while_parent_live = source.latest_task_convergence(identity);
    body_gate.release();
    wait_for_preparation_exit(&fixture.registry, identity);
    wait_for_physical_preparation_body_exit(&fixture.registry);
    let stopped_after_body = source.latest_task_convergence(identity);
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        (held.positions, held.workers, held.context_positions),
        (1, 1, 1)
    );
    assert!(held.bytes > 0);
    assert_eq!(polls_while_parent_live, 0);
    assert!(stopped_while_parent_live.is_none());
    assert!(stopped_after_body.is_some());
    assert_eq!(metadata.rollback_requests.load(Ordering::Acquire), 1);
    assert_eq!(metadata.observer_drops.load(Ordering::Acquire), 1);
    assert_eq!(metadata.adoptions.load(Ordering::Acquire), 0);
    assert!(fixture.health.is_healthy());
}

#[test]
fn a_pending_preparation_child_keeps_its_original_p_while_the_next_body_runs() {
    let metadata = Arc::new(PreparationChildMetadata::default());
    let child_job = PreparationChildJob::start(metadata.clone());
    let child: Arc<dyn crate::TaskPreparationChild> =
        Arc::new(PreparationChildObserver(metadata.clone()));
    let fixture = Fixture::with_config(
        TestTaskHost {
            preparation_child: Mutex::new(Some((TaskId::new(1).unwrap(), child))),
            ..TestTaskHost::default()
        },
        |config| {
            config.max_prepare_workers = 1;
            config.max_preparing_tasks_per_context = 2;
        },
    );
    let context = fixture.context(execution(59_102));
    establish(&fixture.registry, context);
    let identity = task(context.query_execution_id(), fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    let receipt = fixture
        .registry
        .accept_create_task(&request, body(REFUSED_PLAN));
    wait_for_physical_preparation_body_exit(&fixture.registry);
    fixture.registry.advance_deadlines();
    let pending = fixture.registry.preparation_snapshot();
    let source = fixture.registry.status_source(context).unwrap();
    let stopped_before_child = source.latest_task_convergence(identity);
    let next_identity = TaskIdentity::new(
        context.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(4).unwrap(),
        fixture.backend,
    );
    let next = fixture.create(
        descriptor_with(next_identity, UniqueId::new(59_102, 4), 1, 1),
        vec![],
    );
    let next_receipt = fixture
        .registry
        .accept_create_task(&next, body(STREAM_PLAN));
    wait_for_accepted_installed(&fixture.registry, &next);
    wait_for_preparation_exit(&fixture.registry, next_identity);
    let old_p_still_held = fixture.registry.has_preparation_charge(identity);
    let polls_before_release = metadata.polls.load(Ordering::Acquire);
    let joined = child_job.finish();
    fixture.registry.advance_deadlines();
    wait_for_preparation_exit(&fixture.registry, identity);
    let stopped_after_child = source.latest_task_convergence(identity);
    assert!(joined.is_ok());
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    assert_eq!(next_receipt.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        (
            pending.positions,
            pending.workers,
            pending.context_positions
        ),
        (1, 0, 1)
    );
    assert!(pending.bytes > 0);
    assert!(old_p_still_held);
    assert!(polls_before_release > 0);
    assert!(stopped_before_child.is_none());
    assert!(stopped_after_child.is_some());
    assert_eq!(metadata.rollback_requests.load(Ordering::Acquire), 1);
    assert_eq!(metadata.observer_drops.load(Ordering::Acquire), 1);
    assert_eq!(fixture.registry.preparation_snapshot().bytes, 0);
    assert!(fixture.health.is_healthy());
}

#[test]
fn installed_preparation_explicitly_adopts_before_committing_the_real_runnable() {
    let metadata = Arc::new(PreparationChildMetadata::default());
    let child: Arc<dyn crate::TaskPreparationChild> =
        Arc::new(PreparationChildObserver(metadata.clone()));
    let fixture = Fixture::new(TestTaskHost {
        preparation_child: Mutex::new(Some((TaskId::new(1).unwrap(), child))),
        preparation_order: Some(metadata.order.clone()),
        ..TestTaskHost::default()
    });
    let context = fixture.context(execution(59_103));
    establish(&fixture.registry, context);
    let identity = task(context.query_execution_id(), fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    let receipt = fixture
        .registry
        .accept_create_task(&request, body(STREAM_PLAN));
    wait_for_accepted_installed(&fixture.registry, &request);
    wait_for_preparation_exit(&fixture.registry, identity);
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    assert_eq!(*metadata.order.lock().unwrap(), vec!["adopt", "commit"]);
    assert_eq!(metadata.adoptions.load(Ordering::Acquire), 1);
    assert_eq!(metadata.rollback_requests.load(Ordering::Acquire), 0);
    assert_eq!(metadata.observer_drops.load(Ordering::Acquire), 1);
    assert_eq!(
        metadata.polls.load(Ordering::Acquire),
        0,
        "runtime owns exit observation after explicit adoption"
    );
    assert!(fixture.health.is_healthy());
}

#[test]
fn a_receiver_cleanup_panic_cannot_skip_original_child_cancel_or_refund_its_p() {
    use novarocks_execution_contract::failure::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    for (drop_observer, expected_reason) in [(false, 154), (true, 157)] {
        let metadata = Arc::new(PreparationChildMetadata::default());
        metadata
            .panic_on_observer_drop
            .store(drop_observer, Ordering::Release);
        let cleanup_payload_drops = Arc::new(AtomicUsize::new(0));
        let child_job = PreparationChildJob::start(metadata.clone());
        let child: Arc<dyn crate::TaskPreparationChild> =
            Arc::new(PreparationChildObserver(metadata.clone()));
        let original = fixture_host_rejection(
            TaskFailureCategory::ResourceExhausted,
            "original preparation capacity refusal",
        );
        let expected = original.clone().into_failure();
        let fixture = Fixture::new(TestTaskHost {
            preparation_child: Mutex::new(Some((TaskId::new(1).unwrap(), child))),
            preparation_rejection: Some((PreparationFailureStage::Capability, original)),
            preparation_receiver_cleanup_panic: !drop_observer,
            preparation_cleanup_payload_drops: Some(cleanup_payload_drops.clone()),
            ..TestTaskHost::default()
        });
        let context = fixture.context(execution(59_104));
        establish(&fixture.registry, context);
        let identity = task(context.query_execution_id(), fixture.backend);
        let request = fixture.create(descriptor(identity), vec![]);
        let receipt = fixture
            .registry
            .accept_create_task(&request, body(STREAM_PLAN));
        wait_for_physical_preparation_body_exit(&fixture.registry);
        let view = fixture.task_host.preparation_stop(identity);
        let loan = view.borrow_control();
        let observed = loan.checkpoint();
        let joined = child_job.finish();
        for _ in 0..3 {
            fixture.registry.advance_deadlines();
        }
        let remaining = fixture.registry.preparation_snapshot();
        let source = fixture.registry.status_source(context).unwrap();
        assert!(joined.is_ok());
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        let observed = observed.unwrap_err();
        assert!(std::ptr::eq(observed.cause(), expected.cause()));
        assert_eq!(
            observed.failure_fact(),
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(65002).unwrap()),
            )
        );
        assert_eq!(observed.failure_fact(), expected.failure_fact());
        assert_eq!(observed.detail(), "original preparation capacity refusal");
        assert_eq!(cleanup_payload_drops.load(Ordering::Acquire), 0);
        assert_eq!(metadata.payload_drops.load(Ordering::Acquire), 0);
        assert_eq!(
            metadata.observer_drops.load(Ordering::Acquire),
            usize::from(drop_observer)
        );
        assert_eq!(remaining.positions, 1);
        assert_eq!(remaining.workers, 0);
        assert!(remaining.bytes > 0);
        assert!(source.latest_task_convergence(identity).is_none());
        assert_eq!(metadata.rollback_requests.load(Ordering::Acquire), 1);
        assert_eq!(
            fixture.task_host.receivers_removed.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            fixture
                .task_host
                .capabilities_removed
                .load(Ordering::Acquire),
            1
        );
        let fault = fixture.health.failure().unwrap();
        assert_eq!(fault.fact().reason().code(), expected_reason);
        assert_eq!(
            fault.fact().class(),
            novarocks_execution_contract::failure::FailureClass::Unexpected
        );
        assert_eq!(
            fault.origin(),
            novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                identity,
                phase: TaskFailurePhase::Preparation
            }
        );
    }
}

// Real task authority and data are captured by the original panic payload,
// never by the resulting Task failure source.
struct OwnedPreparationPanic {
    data: Mutex<Option<Arc<Vec<u8>>>>,
    authority: Mutex<
        Option<std::sync::Weak<novarocks_execution::runtime::task_control::TaskExecutionControl>>,
    >,
    gate: Arc<InstallGate>,
    dropped: Arc<AtomicUsize>,
    secondary: bool,
    secondary_drops: Arc<AtomicUsize>,
}
struct BodyUnwindPayload {
    authority: Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>>,
    data: Arc<Vec<u8>>,
    gate: Arc<InstallGate>,
    dropped: Arc<AtomicUsize>,
    secondary: bool,
    secondary_drops: Arc<AtomicUsize>,
}
impl Drop for BodyUnwindPayload {
    fn drop(&mut self) {
        self.gate.wait_for_release();
        self.dropped.fetch_add(1, Ordering::AcqRel);
        if self.secondary {
            std::panic::panic_any(SecondaryBodyUnwind {
                _authority: self.authority.clone(),
                _data: self.data.clone(),
                dropped: self.secondary_drops.clone(),
            });
        }
    }
}
struct SecondaryBodyUnwind {
    _authority: Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>>,
    _data: Arc<Vec<u8>>,
    dropped: Arc<AtomicUsize>,
}
impl Drop for SecondaryBodyUnwind {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

fn run_task_authority_body_unwind(secondary: bool, number: i64) {
    use novarocks_execution_contract::failure::FailureProjection;
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(gate.clone());
    let data = Arc::new(vec![71u8; 32 * 1024]);
    let weak_data = Arc::downgrade(&data);
    let owned = Arc::new(OwnedPreparationPanic {
        data: Mutex::new(Some(data)),
        authority: Mutex::new(None),
        gate: gate.clone(),
        dropped: Arc::new(AtomicUsize::new(0)),
        secondary,
        secondary_drops: Arc::new(AtomicUsize::new(0)),
    });
    let metadata = Arc::new(PreparationChildMetadata::default());
    let child_job = PreparationChildJob::start(metadata.clone());
    let fixture = Fixture::with_config(
        TestTaskHost {
            owned_preparation_panic: Some((TaskId::new(1).unwrap(), owned.clone())),
            preparation_child: Mutex::new(Some((
                TaskId::new(1).unwrap(),
                Arc::new(PreparationChildObserver(metadata.clone())),
            ))),
            ..TestTaskHost::default()
        },
        |config| {
            config.max_prepare_workers = 1;
            config.max_preparing_tasks_per_context = 2;
        },
    );
    let execution = execution(number);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&request, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    gate.wait_until_entered();
    let view = fixture.task_host.preparation_stop(identity);
    let observed = view.borrow_control().checkpoint().unwrap_err();
    assert_eq!(
        observed.failure_fact().reason().code(),
        147,
        "the original author accepts before the actual payload Drop gate"
    );
    assert_eq!(owned.dropped.load(Ordering::Acquire), 0);
    assert!(weak_data.upgrade().is_some());
    let authority = owned.authority.lock().unwrap().as_ref().unwrap().clone();
    assert!(authority.upgrade().is_some());
    let before = fixture.registry.preparation_snapshot();
    assert_eq!((before.positions, before.workers), (1, 1));
    let source = fixture.registry.status_source(context).unwrap();
    assert!(source.latest_task_convergence(identity).is_none());
    // This sibling is accepted before health changes; it must still execute.
    let next = TaskIdentity::new(
        execution,
        identity.stage_id(),
        TaskId::new(2).unwrap(),
        fixture.backend,
    );
    let next_request = fixture.create(
        descriptor_with(next, UniqueId::new(number, 2), 1, 1),
        vec![],
    );
    assert_eq!(
        fixture
            .registry
            .accept_create_task(&next_request, body(STREAM_PLAN))
            .outcome(),
        OperationOutcome::Accepted
    );
    fixture.registry.cancel_task(&CancelTask::new(
        TaskOperationId::new_v7(),
        identity,
        CancelReason::UpstreamNoLongerNeeded,
    ));
    assert_eq!(
        view.borrow_control()
            .checkpoint()
            .unwrap_err()
            .failure_fact()
            .reason()
            .code(),
        147,
        "late cancel cannot replace the accepted originating cause"
    );
    gate.release();
    wait_for_accepted_installed(&fixture.registry, &next_request);
    wait_for_preparation_exit(&fixture.registry, next);
    wait_for_physical_preparation_body_exit(&fixture.registry);
    child_job.finish().unwrap();
    for _ in 0..4 {
        fixture.registry.advance_deadlines();
    }
    assert_eq!(owned.dropped.load(Ordering::Acquire), 1);
    assert_eq!(owned.secondary_drops.load(Ordering::Acquire), 0);
    assert_eq!(metadata.rollback_requests.load(Ordering::Acquire), 1);
    assert_eq!(
        metadata.observer_drops.load(Ordering::Acquire),
        1,
        "independent child retirement proceeds even when body payload disposal is unknown"
    );
    assert_eq!(
        fixture.task_host.receivers_removed.load(Ordering::Acquire),
        1
    );
    assert!(
        fixture.task_host.creation_commits.load(Ordering::Acquire) >= 1,
        "the previously accepted sibling really commits"
    );
    if secondary {
        assert_eq!(
            fixture.health.failure().unwrap().fact().reason().code(),
            161
        );
        assert!(fixture.registry.has_preparation_charge(identity));
        assert!(source.latest_task_convergence(identity).is_none());
        assert!(weak_data.upgrade().is_some());
        assert!(authority.upgrade().is_some());
    } else {
        wait_for_preparation_exit(&fixture.registry, identity);
        wait_for_terminal_record(&fixture.registry, identity);
        assert!(fixture.health.is_healthy());
        assert!(
            weak_data.upgrade().is_none(),
            "safe diagnostic clones cannot retain the original data"
        );
        assert!(
            authority.upgrade().is_none(),
            "the original Task authority capture actually exited"
        );
    }
    assert_eq!(observed.failure_fact().reason().code(), 147);
    drop(observed);
    assert_eq!(owned.dropped.load(Ordering::Acquire), 1);
}
#[test]
fn preparation_panic_task_authority_and_data_exit_independently_of_diagnostic_clones() {
    run_task_authority_body_unwind(false, 59_201);
}
#[test]
fn preparation_panic_secondary_drop_keeps_original_p_and_runs_child_and_sibling_cleanup() {
    run_task_authority_body_unwind(true, 59_202);
}

struct PreacceptDropContent {
    data: Arc<Vec<u8>>,
    gate: Arc<InstallGate>,
    measure_panics: bool,
    drop_panics: bool,
    secondary: bool,
    payload_drops: Arc<AtomicUsize>,
    input_drops: Arc<AtomicUsize>,
    secondary_drops: Arc<AtomicUsize>,
}
impl std::fmt::Debug for PreacceptDropContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreacceptDropContent")
    }
}
impl PreacceptDropContent {
    fn payload(&self) -> BodyUnwindPayload {
        BodyUnwindPayload {
            authority: None,
            data: self.data.clone(),
            gate: self.gate.clone(),
            dropped: self.payload_drops.clone(),
            secondary: self.secondary,
            secondary_drops: self.secondary_drops.clone(),
        }
    }
}
impl CreationContent for PreacceptDropContent {
    fn encoded_len(&self) -> usize {
        1
    }
    fn retained_bytes(&self) -> usize {
        if self.measure_panics {
            std::panic::panic_any(self.payload());
        }
        usize::MAX
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
impl Drop for PreacceptDropContent {
    fn drop(&mut self) {
        self.input_drops.fetch_add(1, Ordering::AcqRel);
        if self.drop_panics {
            std::panic::panic_any(self.payload());
        }
        self.gate.wait_for_release();
    }
}
fn run_unaccepted_reserved_input_tail(measure_panics: bool, secondary: bool, number: i64) {
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(gate.clone());
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let input_drops = Arc::new(AtomicUsize::new(0));
    let secondary_drops = Arc::new(AtomicUsize::new(0));
    let data = Arc::new(vec![37; 16 * 1024]);
    let weak_data = Arc::downgrade(&data);
    let input = TaskCreationInput::new(
        FrozenBytes::freeze(Bytes::from_static(STREAM_PLAN)),
        Box::new(PreacceptDropContent {
            data,
            gate: gate.clone(),
            measure_panics,
            drop_panics: secondary,
            secondary,
            payload_drops: payload_drops.clone(),
            input_drops: input_drops.clone(),
            secondary_drops: secondary_drops.clone(),
        }),
    );
    let fixture = Fixture::with_config(TestTaskHost::default(), |config| {
        config.max_active_tasks_per_backend = 1
    });
    let execution = execution(number);
    let context = fixture.context(execution);
    let other_execution = self::execution(number + 1);
    let other_context = fixture.context(other_execution);
    establish(&fixture.registry, context);
    establish(&fixture.registry, other_context);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    let registry = fixture.registry.clone();
    let joined = std::thread::spawn(move || registry.accept_create_task(&request, input));
    gate.wait_until_entered();
    let source = fixture.registry.status_source(context).unwrap();
    assert!(
        source.latest(identity).is_none(),
        "no Accepted Task was published"
    );
    assert!(source.latest_task_convergence(identity).is_none());
    assert!(matches!(
        fixture.registry.root_result_route(identity),
        RootResultRoute::Creating
    ));
    assert!(
        fixture.health.is_healthy(),
        "an ordinary pending Drop is not unknown exit"
    );
    let next = task(other_execution, fixture.backend);
    let refused = fixture
        .registry
        .create_task(&fixture.create(descriptor(next), vec![]), body(STREAM_PLAN));
    assert_eq!(
        refused.outcome(),
        OperationOutcome::ResourceExhausted,
        "the original unaccepted Creating still consumes its actual active slot"
    );
    fixture
        .registry
        .abort_query_context(&AbortQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            AbortCause::QueryFailed,
        ));
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::Aborting
    );
    assert_eq!(
        fixture.registry.preparation_snapshot().positions,
        0,
        "no synthetic Accepted P is invented"
    );
    gate.release();
    let receipt = joined
        .join()
        .expect("all catchable input/payload unwinds stay with their original reservation");
    assert!(receipt.acknowledgement().is_none());
    assert_eq!(
        input_drops.load(Ordering::Acquire),
        1,
        "measurement unwind must not skip the independent input destructor"
    );
    for _ in 0..4 {
        fixture.registry.advance_deadlines();
    }
    assert!(fixture.task_host.prepared_bodies().is_empty());
    if secondary {
        assert_eq!(
            payload_drops.load(Ordering::Acquire),
            if measure_panics { 2 } else { 1 }
        );
        assert_eq!(secondary_drops.load(Ordering::Acquire), 0);
        assert_eq!(
            fixture.health.failure().unwrap().fact().reason().code(),
            161
        );
        assert!(matches!(
            fixture.registry.root_result_route(identity),
            RootResultRoute::Creating
        ));
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Aborting
        );
        assert!(weak_data.upgrade().is_some());
    } else {
        assert!(fixture.health.is_healthy());
        assert!(matches!(
            fixture.registry.root_result_route(identity),
            RootResultRoute::UnknownTask
        ));
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::TerminalRetained
        );
        assert!(weak_data.upgrade().is_none());
    }
    assert!(
        source.latest_task_convergence(identity).is_none(),
        "unaccepted work never fabricates TaskStopped"
    );
}
#[test]
fn preaccept_retained_bytes_unwind_keeps_original_creating_until_input_and_body_really_exit() {
    run_unaccepted_reserved_input_tail(true, false, 59_210);
}
#[test]
fn preaccept_refused_input_drop_keeps_original_active_capacity_until_actual_exit() {
    run_unaccepted_reserved_input_tail(false, false, 59_212);
}
#[test]
fn preaccept_measurement_and_input_secondary_panics_have_separate_bounded_original_custody() {
    run_unaccepted_reserved_input_tail(true, true, 59_214);
}
#[test]
fn preaccept_refused_input_secondary_drop_never_mints_task_or_returns_creating_capacity() {
    run_unaccepted_reserved_input_tail(false, true, 59_216);
}

struct AcceptedPublicationGate(Arc<InstallGate>);
impl TaskProtocolObserver for AcceptedPublicationGate {
    fn observe(&self, event: TaskProtocolEvent) {
        if matches!(event, TaskProtocolEvent::TaskCreateApplied { .. }) {
            self.0.wait_for_release();
        }
    }
}
#[test]
fn original_admission_publication_tail_cannot_be_outrun_by_the_preparation_worker() {
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(gate.clone());
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let clock = Arc::new(ManualClock::new());
    let host = Arc::new(TestTaskHost::default());
    let ports = TaskExecutionPorts::new(
        Arc::new(AcceptedPublicationGate(gate.clone())),
        Arc::new(NoopResultLifecycle),
        Arc::new(NoopMetrics),
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
    );
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        clock,
        Arc::new(TestContextHost),
        host.clone(),
        ports,
    );
    let execution = execution(59_220);
    let context = QueryContextRef::new(execution, frontend, backend);
    establish(&registry, context);
    let identity = task(execution, backend);
    let request = CreateTask::try_new(
        TaskOperationId::new_v7(),
        context,
        descriptor(identity),
        vec![],
    )
    .unwrap();
    let create_registry = registry.clone();
    let joined =
        std::thread::spawn(move || create_registry.accept_create_task(&request, body(STREAM_PLAN)));
    gate.wait_until_entered();
    wait_for_physical_preparation_body_exit(&registry);
    let while_admission_live = registry.preparation_snapshot();
    let committed_early = host.creation_commits.load(Ordering::Acquire);
    gate.release();
    let receipt = joined.join().unwrap();
    wait_for_preparation_exit(&registry, identity);
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    assert_eq!(
        (while_admission_live.positions, while_admission_live.workers),
        (1, 0),
        "the worker is physically free, but the original Accepted call's tail still owns preparation responsibility"
    );
    assert_eq!(
        committed_early, 0,
        "ordinary runtime activation follows whole preparation handoff"
    );
    assert_eq!(host.creation_commits.load(Ordering::Acquire), 1);
}

// This fixture drives the actual Worker RunnableTask handoff and owns real
// data in an actual cleanup job. It is not a counter-only completion mock.
struct DormantCleanupData {
    _data: Arc<Vec<u8>>,
    gate: Arc<InstallGate>,
}
impl Drop for DormantCleanupData {
    fn drop(&mut self) {
        self.gate.wait_for_release();
    }
}
struct ActualDormantCleanup {
    reporter: Mutex<Option<TaskStatusReporter>>,
    data: Mutex<Option<DormantCleanupData>>,
    job: Mutex<Option<std::thread::JoinHandle<()>>>,
    stopped: AtomicBool,
    commits: AtomicUsize,
    ordinary_runs: AtomicUsize,
    actual_exit: Arc<AtomicBool>,
}
impl std::fmt::Debug for ActualDormantCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActualDormantCleanup")
    }
}
impl RunnableTask for ActualDormantCleanup {
    fn cancel(&self, _: CancelReason) {
        self.stopped.store(true, Ordering::Release);
    }
    fn abort(&self, _: AbortCause) {
        self.stopped.store(true, Ordering::Release);
    }
    fn commit_creation(&self) {
        self.commits.fetch_add(1, Ordering::AcqRel);
        if !self.stopped.load(Ordering::Acquire) {
            self.ordinary_runs.fetch_add(1, Ordering::AcqRel);
        }
        let data = self
            .data
            .lock()
            .unwrap()
            .take()
            .expect("one actual dormant work handoff");
        let exit = self.actual_exit.clone();
        let reporter = self
            .reporter
            .lock()
            .unwrap()
            .take()
            .expect("original runtime reporter");
        *self.job.lock().unwrap() = Some(std::thread::spawn(move || {
            drop(data);
            let original = reporter
                .current()
                .termination()
                .cloned()
                .expect("original Task failure precedes cleanup start");
            reporter.conclude_termination(
                original,
                novarocks_execution_contract::TaskOutputFacts::default(),
            );
            reporter.release_output();
            reporter.note_actual_stopped();
            reporter.note_resources_converged();
            drop(reporter);
            exit.store(true, Ordering::Release);
        }));
    }
}
struct LateInstalledBodyPanicObserver {
    host: std::sync::Weak<TestTaskHost>,
    published: Arc<InstallGate>,
    owned: Arc<OwnedPreparationPanic>,
}
impl TaskProtocolObserver for LateInstalledBodyPanicObserver {
    fn observe(&self, event: TaskProtocolEvent) {
        let TaskProtocolEvent::TaskCreateApplied { identity } = event else {
            return;
        };
        self.published.wait_for_release();
        let host = self.host.upgrade().unwrap();
        let owner = host.preparation_stop(identity).control().0.unwrap();
        let authority = owner.execution_control(TaskFailurePhase::Preparation);
        *self.owned.authority.lock().unwrap() = Some(Arc::downgrade(&authority));
        let data = self.owned.data.lock().unwrap().take().unwrap();
        std::panic::panic_any(BodyUnwindPayload {
            authority: Some(authority),
            data,
            gate: self.owned.gate.clone(),
            dropped: self.owned.dropped.clone(),
            secondary: true,
            secondary_drops: self.owned.secondary_drops.clone(),
        });
    }
}
#[test]
fn late_installed_body_panic_drains_actual_dormant_runtime_while_unknown_payload_keeps_p() {
    use novarocks_execution_contract::failure::FailureProjection;
    let published = Arc::new(InstallGate::held());
    let payload_drop = Arc::new(InstallGate::held());
    let cleanup_drop = Arc::new(InstallGate::held());
    let _release_publication = PreparationGateRelease(published.clone());
    let _release_payload = PreparationGateRelease(payload_drop.clone());
    let _release_cleanup = PreparationGateRelease(cleanup_drop.clone());
    let runtime_data = Arc::new(vec![81u8; 32 * 1024]);
    let weak_runtime_data = Arc::downgrade(&runtime_data);
    let cleanup = Arc::new(ActualDormantCleanup {
        reporter: Mutex::new(None),
        data: Mutex::new(Some(DormantCleanupData {
            _data: runtime_data,
            gate: cleanup_drop.clone(),
        })),
        job: Mutex::new(None),
        stopped: AtomicBool::new(false),
        commits: AtomicUsize::new(0),
        ordinary_runs: AtomicUsize::new(0),
        actual_exit: Arc::new(AtomicBool::new(false)),
    });
    let owned = Arc::new(OwnedPreparationPanic {
        data: Mutex::new(Some(Arc::new(vec![82u8; 16 * 1024]))),
        authority: Mutex::new(None),
        gate: payload_drop.clone(),
        dropped: Arc::new(AtomicUsize::new(0)),
        secondary: true,
        secondary_drops: Arc::new(AtomicUsize::new(0)),
    });
    let host = Arc::new(TestTaskHost {
        actual_dormant_cleanup: Some((TaskId::new(1).unwrap(), cleanup.clone())),
        ..TestTaskHost::default()
    });
    let backend = BackendProcessId::new_v7();
    let frontend = FrontendProcessId::new_v7();
    let health =
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default());
    let ports = TaskExecutionPorts::new(
        Arc::new(LateInstalledBodyPanicObserver {
            host: Arc::downgrade(&host),
            published: published.clone(),
            owned: owned.clone(),
        }),
        Arc::new(NoopResultLifecycle),
        Arc::new(NoopMetrics),
        health.clone(),
    );
    let registry = TaskExecutionRegistry::new(
        TaskExecutionRegistryConfig::for_process(backend, 17, 9),
        Arc::new(ManualClock::new()),
        Arc::new(TestContextHost),
        host.clone(),
        ports,
    );
    let execution = execution(59_221);
    let context = QueryContextRef::new(execution, frontend, backend);
    establish(&registry, context);
    let identity = task(execution, backend);
    let request = CreateTask::try_new(
        TaskOperationId::new_v7(),
        context,
        descriptor(identity),
        vec![],
    )
    .unwrap();
    let caller = registry.clone();
    let joined = std::thread::spawn(move || caller.accept_create_task(&request, body(STREAM_PLAN)));
    published.wait_until_entered();
    wait_for_physical_preparation_body_exit(&registry);
    let installed_before_fault = registry.has_live_task(identity);
    let commits_before_fault = cleanup.commits.load(Ordering::Acquire);
    published.release();
    payload_drop.wait_until_entered();
    let view = host.preparation_stop(identity);
    let original = view.borrow_control().checkpoint().unwrap_err();
    let first_reason = original.failure_fact().reason().code();
    let while_payload_live = view.control().0.unwrap().current();
    assert!(while_payload_live.installed());
    assert_eq!(while_payload_live.state(), TaskState::Failing);
    let Some(TerminationDetail::Failed(failure)) = while_payload_live.termination() else {
        panic!("the original installed body failure is already accepted")
    };
    assert_eq!(failure.phase(), TaskFailurePhase::Execution);
    payload_drop.release();
    let receipt = joined.join().expect("original body unwind stays owned");
    cleanup_drop.wait_until_entered();
    let p_while_cleanup_live = registry.has_preparation_charge(identity);
    let real_data_still_live = weak_runtime_data.upgrade().is_some();
    let actual_exit_early = cleanup.actual_exit.load(Ordering::Acquire);
    cleanup_drop.release();
    let actual_job = cleanup.job.lock().unwrap().take().unwrap();
    actual_job
        .join()
        .expect("actual independent cleanup job exits");
    for _ in 0..4 {
        registry.advance_deadlines();
    }
    assert!(
        installed_before_fault,
        "only the original already-Live installation can be handed off"
    );
    assert_eq!(
        commits_before_fault, 0,
        "the original admission body still owned its real tail"
    );
    assert_eq!(
        receipt.outcome(),
        OperationOutcome::Accepted,
        "an existing Accepted Task cannot be changed into an unaccepted receipt"
    );
    assert_eq!(first_reason, 147);
    assert_eq!(health.failure().unwrap().fact().reason().code(), 161);
    assert!(cleanup.stopped.load(Ordering::Acquire));
    assert_eq!(cleanup.commits.load(Ordering::Acquire), 1);
    assert_eq!(
        cleanup.ordinary_runs.load(Ordering::Acquire),
        0,
        "only stop-before-start cleanup runs"
    );
    assert!(p_while_cleanup_live && real_data_still_live && !actual_exit_early);
    assert!(cleanup.actual_exit.load(Ordering::Acquire));
    let owner = view.control().0.unwrap();
    assert!(
        owner.runtime_actual_stopped_received(),
        "the real runtime receipt was retained rather than discarded"
    );
    assert!(!owner.convergence().actual_stopped() && !owner.convergence().resources_converged());
    assert!(
        weak_runtime_data.upgrade().is_none(),
        "the independent runtime data really left"
    );
    assert!(
        registry.has_preparation_charge(identity),
        "runtime exit does not prove opaque body exit"
    );
    assert!(
        registry
            .status_source(context)
            .unwrap()
            .latest_task_convergence(identity)
            .is_none()
    );
    assert_eq!(
        view.borrow_control()
            .checkpoint()
            .unwrap_err()
            .failure_fact()
            .reason()
            .code(),
        147
    );
    assert_eq!(owned.dropped.load(Ordering::Acquire), 1);
    assert_eq!(owned.secondary_drops.load(Ordering::Acquire), 0);
}

struct ReentrantPreparationPublicationWake {
    registry: std::sync::Weak<TaskExecutionRegistry>,
    samples: Mutex<Vec<(bool, usize)>>,
}
impl std::task::Wake for ReentrantPreparationPublicationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // Record only. The oracle runs outside all callback/actor locks.
        let sample = self
            .registry
            .upgrade()
            .and_then(|registry| registry.try_preparation_snapshot().ok().flatten());
        self.samples
            .lock()
            .unwrap()
            .push((sample.is_some(), sample.map_or(0, |s| s.positions)));
    }
}
#[test]
fn accepted_and_installed_source_notify_wakes_reenter_the_original_registry_after_unlock() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let gate = Arc::new(InstallGate::held());
    let _release = PreparationGateRelease(gate.clone());
    let fixture = Fixture::new(TestTaskHost {
        install_gate: Some(gate.clone()),
        ..TestTaskHost::default()
    });
    let execution = execution(59_222);
    let context = fixture.context(execution);
    establish(&fixture.registry, context);
    let source = fixture.registry.status_source(context).unwrap();
    let wake = Arc::new(ReentrantPreparationPublicationWake {
        registry: Arc::downgrade(&fixture.registry),
        samples: Mutex::new(vec![]),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut accepted = Box::pin(source.next_task_event_owned());
    let before_accept = accepted.as_mut().poll(&mut cx);
    let identity = task(execution, fixture.backend);
    let request = fixture.create(descriptor(identity), vec![]);
    let receipt = fixture
        .registry
        .accept_create_task(&request, body(STREAM_PLAN));
    gate.wait_until_entered();
    let accepted_event = accepted.as_mut().poll(&mut cx);
    let at_accepted = source.latest(identity).unwrap();
    let after_accepted = wake.samples.lock().unwrap().clone();
    let mut installed = Box::pin(source.next_task_event_owned());
    let before_install = installed.as_mut().poll(&mut cx);
    let commits_before_install = fixture.task_host.creation_commits.load(Ordering::Acquire);
    gate.release();
    wait_for_preparation_exit(&fixture.registry, identity);
    wait_for_physical_preparation_body_exit(&fixture.registry);
    let installed_event = installed.as_mut().poll(&mut cx);
    let at_installed = source.latest(identity).unwrap();
    let all_samples = wake.samples.lock().unwrap().clone();
    assert!(matches!(before_accept, Poll::Pending));
    assert!(matches!(accepted_event, Poll::Ready(Some(_))));
    assert!(matches!(before_install, Poll::Pending));
    assert!(matches!(installed_event, Poll::Ready(Some(_))));
    assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
    assert!(!at_accepted.installed() && at_installed.installed());
    assert!(
        at_installed.version() > at_accepted.version(),
        "Installed cannot precede or roll back Accepted"
    );
    assert_eq!(
        commits_before_install, 0,
        "no actor starts before original installation/body handoff"
    );
    assert_eq!(
        fixture.task_host.creation_commits.load(Ordering::Acquire),
        1
    );
    assert_eq!(
        after_accepted,
        vec![(true, 1)],
        "the actual Accepted Notify wake reentered the same original Registry and saw its one real charge"
    );
    assert_eq!(
        all_samples,
        vec![(true, 1), (true, 1)],
        "the separate actual Installed Notify wake also reentered after unlock, before original P returned"
    );
}
