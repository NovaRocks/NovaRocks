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

//! The backend-local owner of the task protocol.
//!
//! Everything the protocol calls a decision happens here, under one lock, at
//! one linearization point per operation: the establish creation gate, the
//! creation owner election, every domain token, the first-wins termination
//! latch, release readiness, and retirement. The execution side is reached
//! only through the ports in [`super::host`].
//!
//! # Lock discipline
//!
//! One mutex guards all owner state and one condition variable wakes every
//! waiter. Two kinds of call are allowed while that mutex is held: reading or
//! writing a task's own status, and the teardown ports
//! (`remove_receiver`, `remove_inbound_capability`, context-admission fence
//! changes, and `release`), which are local map operations. Everything that can block — `materialize`, the two
//! installs, `submit_runnable`, `apply_task_domain`,
//! `advance_shared_domain`, and `cancel` / `abort` on a runnable — is called
//! with the mutex released. The lock order is owner, then status, then
//! observation source; nothing ever runs in the other direction, because the
//! observation source never calls back.
//!
//! # Deadlines
//!
//! No thread here sleeps against a wall clock. Every deadline is decided from
//! the injected clock, and [`TaskExecutionRegistry::advance_deadlines`] is
//! what re-evaluates them and wakes the waiters. A deployment runs it from a
//! maintenance tick; a test calls it directly after moving the clock, which is
//! why the whole lifecycle is deterministic. The gate's own wait timeout is a
//! liveness backstop for a deployment that misses a tick and never a source of
//! truth.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use novarocks_execution::exec::fragment::program::FragmentSinkKind;
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::{ContentFingerprint, DomainProgression};
use novarocks_execution_contract::task_execution::identity::{
    IdentityField, IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::{
    AbortQueryContext, AcquireQueryContextAdmissionTicket, AdvanceQueryContextDomain, CancelTask,
    CreateTask, CreateTaskReceipt, EstablishQueryContext, FetchTaskDynamicFilters,
    GetFinalTaskInfo, OperationEnvelope, OperationOutcome, QueryContextDomainReceipt,
    QueryContextDomainUpdate, QueryContextReceipt, ReleaseOutcome, ReleaseQueryContext,
    RenewQueryExecutionLease, UpdateQueryContext, UpdateTask, UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, TaskFailureCategory, TaskOutputFacts, TaskState, TerminationDetail,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_task_codec::TransportBudget;
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};
use novarocks_worker::{
    AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority, AdmissionTicketConfig,
    AdmissionTicketProgression, AdmissionTicketRedemptionRejection, ContextOperationKind,
    ContextTransition, InstalledLease, LatchOutcome, LeaseBounds, LeaseProgression,
    MonotonicInstant, OperationAdmission, OperationWaitCaps, QueryContextDomains,
    QueryContextEvent, RequestHorizon, classify_context_transition, classify_operation_admission,
};

use super::clock::{BackendMonotonicClock, ProcessMonotonicClock};
use super::domains::{self, InitialDomainKey, TaskDomains};
use super::entry::{
    ContextEntry, CreationCell, CreationFailure, EstablishRecord, LiveTask, RetiredTask, TaskEntry,
    estimate_retained_bytes,
};
use super::host::{
    QueryContextHost, ReleasedContextEvidence, RunnableTask, SharedFactsRequest, TaskExecutionHost,
};
use super::marker;
use super::observation::TaskStatusSource;
use super::receipt::{
    AdmissionTicketOutcome, CancelTaskOutcome, CreateTaskOutcome, DynamicFilterReadOutcome,
    FinalTaskInfoOutcome, OperationReceipt, QueryContextOutcome, ReleaseAcknowledgement,
    ReleaseQueryContextOutcome, UpdateTaskOutcome,
};
use super::status::{
    METRIC_PUBLISH_MIN_INTERVAL, RootResultBinding, RootResultRoute, StatusAdvance,
    TaskStatusOwner, TaskStatusReporter,
};

const REGISTRY_LOCK: &str = "task execution registry lock";

/// How many times one settle drives termination before yielding.
///
/// A latch, its fan-out, retirement, and completion take one pass each; the
/// extra passes exist only for a fan-out that latches something new.
const MAX_SETTLE_PASSES: usize = 4;

/// The bounds and budgets one owner runs with.
///
/// The two task-count bounds are the frozen protocol budgets from
/// [`TransportBudget`]; they live here as fields so that a test can shrink
/// them without redefining the neutral type or reaching into it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskExecutionRegistryConfig {
    pub backend_process_id: BackendProcessId,
    pub lease_bounds: LeaseBounds,
    pub wait_caps: OperationWaitCaps,
    pub admission_tickets: AdmissionTicketConfig,
    pub request_horizon: RequestHorizon,
    pub metric_publish_min_interval: Duration,
    /// Cumulative task identities one context may consume. Retiring a task
    /// does not replenish this bound.
    pub max_tasks_per_context: usize,
    pub max_active_tasks_per_backend: usize,
    /// Terminal task records retained past retirement.
    pub retained_task_capacity: usize,
    /// Estimated bytes of retained terminal task records.
    pub retained_task_max_bytes: usize,
    /// Query contexts retained in `TERMINAL_RETAINED`.
    pub retained_context_capacity: usize,
    /// Reclaimed-record fences kept after retention ends. Past this, a late
    /// request no longer proves anything and the owner answers as if the
    /// identity were unknown.
    pub gone_fence_capacity: usize,
    /// How long a terminating context waits for a task to converge on its
    /// own terminal before the owner forces one.
    pub termination_grace: Duration,
    /// Liveness backstop for a creation-gate wait. It never decides an
    /// outcome; the injected clock does.
    pub gate_poll_interval: Duration,
}

impl TaskExecutionRegistryConfig {
    pub fn for_process(backend_process_id: BackendProcessId) -> Self {
        let budget = TransportBudget::DEFAULT;
        Self {
            backend_process_id,
            lease_bounds: LeaseBounds::DEFAULT,
            wait_caps: OperationWaitCaps::DEFAULT,
            admission_tickets: AdmissionTicketConfig::DEFAULT,
            request_horizon: RequestHorizon::DEFAULT,
            metric_publish_min_interval: METRIC_PUBLISH_MIN_INTERVAL,
            max_tasks_per_context: budget.max_tasks_per_context(),
            max_active_tasks_per_backend: budget.max_active_tasks_per_backend(),
            retained_task_capacity: budget.max_tasks_per_context(),
            retained_task_max_bytes: 16 * 1024 * 1024,
            retained_context_capacity: 1024,
            gone_fence_capacity: budget.max_tasks_per_context(),
            termination_grace: RequestHorizon::DEFAULT.server_wait(),
            gate_poll_interval: Duration::from_millis(50),
        }
    }
}

/// What one deadline sweep did.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct DeadlineSweep {
    pub admission_tickets_expired: usize,
    pub leases_expired: usize,
    pub tasks_retired: usize,
    pub tasks_reaped: usize,
    pub contexts_reaped: usize,
    pub metrics_flushed: usize,
}

/// Cumulative owner counters.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct RegistryCounters {
    pub contexts_established: u64,
    pub contexts_rolled_back: u64,
    pub tasks_created: u64,
    pub creations_rolled_back: u64,
    pub creations_converged: u64,
    pub lease_expiries: u64,
    pub task_failure_escalations: u64,
}

#[derive(Debug, Default)]
struct AtomicCounters {
    contexts_established: AtomicU64,
    contexts_rolled_back: AtomicU64,
    tasks_created: AtomicU64,
    creations_rolled_back: AtomicU64,
    creations_converged: AtomicU64,
    lease_expiries: AtomicU64,
    task_failure_escalations: AtomicU64,
}

impl AtomicCounters {
    fn snapshot(&self) -> RegistryCounters {
        RegistryCounters {
            contexts_established: self.contexts_established.load(Ordering::Relaxed),
            contexts_rolled_back: self.contexts_rolled_back.load(Ordering::Relaxed),
            tasks_created: self.tasks_created.load(Ordering::Relaxed),
            creations_rolled_back: self.creations_rolled_back.load(Ordering::Relaxed),
            creations_converged: self.creations_converged.load(Ordering::Relaxed),
            lease_expiries: self.lease_expiries.load(Ordering::Relaxed),
            task_failure_escalations: self.task_failure_escalations.load(Ordering::Relaxed),
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
struct InFlight {
    mutations: usize,
    reads: usize,
}

impl InFlight {
    const fn is_idle(self) -> bool {
        self.mutations == 0 && self.reads == 0
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Lane {
    Mutation,
    Read,
}

#[derive(Default)]
struct RegistryState {
    contexts: BTreeMap<QueryContextRef, ContextEntry>,
    context_by_execution: BTreeMap<QueryExecutionId, QueryContextRef>,
    task_index: BTreeMap<TaskIdentity, QueryContextRef>,
    in_flight: BTreeMap<QueryContextRef, InFlight>,
    pending_termination: BTreeSet<QueryContextRef>,
    retired_task_order: VecDeque<(QueryContextRef, TaskIdentity)>,
    gone_task_order: VecDeque<(QueryContextRef, TaskIdentity)>,
    retired_context_order: VecDeque<QueryContextRef>,
    gone_context_order: VecDeque<QueryContextRef>,
    active_tasks: usize,
    retained_tasks: usize,
    retained_bytes: usize,
}

impl RegistryState {
    fn context_state(&self, context: QueryContextRef) -> QueryContextState {
        self.contexts
            .get(&context)
            .map_or(QueryContextState::Absent, |entry| entry.state)
    }

    fn in_flight_of(&self, context: QueryContextRef) -> InFlight {
        self.in_flight.get(&context).copied().unwrap_or_default()
    }
}

/// The backend-local owner of query contexts, tasks, status, and retention.
pub struct TaskExecutionRegistry {
    config: TaskExecutionRegistryConfig,
    clock: Arc<dyn BackendMonotonicClock>,
    context_host: Arc<dyn QueryContextHost>,
    task_host: Arc<dyn TaskExecutionHost>,
    admission_tickets: AdmissionTicketAuthority,
    state: Mutex<RegistryState>,
    gate: Condvar,
    counters: AtomicCounters,
}

impl TaskExecutionRegistry {
    pub fn new(
        config: TaskExecutionRegistryConfig,
        clock: Arc<dyn BackendMonotonicClock>,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
    ) -> Arc<Self> {
        assert!(
            config.max_tasks_per_context <= TransportBudget::DEFAULT.max_tasks_per_context(),
            "task registry context bound exceeds the transport contract"
        );
        Arc::new(Self {
            config,
            clock,
            context_host,
            task_host,
            admission_tickets: AdmissionTicketAuthority::new(config.admission_tickets),
            state: Mutex::new(RegistryState::default()),
            gate: Condvar::new(),
            counters: AtomicCounters::default(),
        })
    }

    /// The owner a deployment builds: the process monotonic clock.
    pub fn with_process_clock(
        config: TaskExecutionRegistryConfig,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
    ) -> Arc<Self> {
        Self::new(
            config,
            Arc::new(ProcessMonotonicClock::new()),
            context_host,
            task_host,
        )
    }

    pub const fn config(&self) -> &TaskExecutionRegistryConfig {
        &self.config
    }

    /// The worker-local epoch capability a new acquisition must freeze.
    pub fn admission_epoch_capability(
        &self,
    ) -> novarocks_execution_contract::AdmissionEpochCapability {
        self.admission_tickets.current_epoch(self.clock.now())
    }

    pub fn counters(&self) -> RegistryCounters {
        self.counters.snapshot()
    }

    pub fn context_state(&self, context: QueryContextRef) -> QueryContextState {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .context_state(context)
    }

    /// The observation channel of one query context.
    pub fn status_source(&self, context: QueryContextRef) -> Option<Arc<TaskStatusSource>> {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .contexts
            .get(&context)
            .map(|entry| Arc::clone(&entry.source))
    }

    /// The shared-resource abort cause one terminal context reports.
    ///
    /// The neutral [`QueryContextReceipt`] has no field for it, so a transport
    /// adapter reads it here to fill the wire acknowledgement. It is never a
    /// second authority over a task's own terminal outcome.
    pub fn termination_cause(&self, context: QueryContextRef) -> Option<AbortCause> {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .contexts
            .get(&context)
            .and_then(ContextEntry::termination_cause)
    }

    /// The terminal evidence this backend sealed when it released one query
    /// context's shared facts.
    ///
    /// Empty until the completion pass has actually handed the facts back:
    /// a release that answered `NOT_READY` has sealed nothing, and reporting
    /// a contribution then would state a terminal fact about a context that is
    /// still draining.
    pub fn released_context_evidence(&self, context: QueryContextRef) -> ReleasedContextEvidence {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .contexts
            .get(&context)
            .map(|entry| entry.released_evidence.clone())
            .unwrap_or_else(ReleasedContextEvidence::none)
    }

    /// How many operations and reads this owner is currently running for one
    /// query context. A release is ready only when both are zero.
    pub fn in_flight_operations(&self, context: QueryContextRef) -> usize {
        let counters = self
            .state
            .lock()
            .expect(REGISTRY_LOCK)
            .in_flight_of(context);
        counters.mutations + counters.reads
    }

    /// The lease currently installed on one query context.
    pub fn installed_lease(&self, context: QueryContextRef) -> Option<InstalledLease> {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .contexts
            .get(&context)
            .and_then(|entry| entry.lease)
    }

    /// Whether one task identity is currently findable as a live task.
    /// The execution kernel's key for one live task on this process.
    /// Resolves one root result poll against this process's task set.
    ///
    /// Three facts are checked here and nowhere else: the identity addresses a
    /// task of this exact process, that task is live, and its descriptor's
    /// sink is the query's result sink. The last one is why routing by buffer
    /// key alone is not enough: every task has a buffer key, but only the
    /// result owner owes the coordinator a result, and answering a poll out of
    /// any other task's buffer would hand back an exchange producer's output
    /// as if it were the query's answer.
    pub fn root_result_route(&self, identity: TaskIdentity) -> RootResultRoute {
        if identity.backend_process_id() != self.config.backend_process_id {
            return RootResultRoute::UnknownTask;
        }
        let state = self.state.lock().expect(REGISTRY_LOCK);
        let Some(context) = state.task_index.get(&identity).copied() else {
            return RootResultRoute::UnknownTask;
        };
        match self.locate_task_locked(&state, context, identity) {
            TaskLocation::Live => {
                let live = live_task(&state, context, identity).expect("located live task");
                if live.descriptor.sink_kind() != FragmentSinkKind::Result {
                    return RootResultRoute::NotResultOwner;
                }
                RootResultRoute::Serve(RootResultBinding::new(
                    live.descriptor.fragment_instance_id(),
                    Arc::clone(&live.status),
                ))
            }
            TaskLocation::Creating => RootResultRoute::Creating,
            TaskLocation::Retired => {
                let retired = retired_task(&state, context, identity).expect("retired task");
                if retired.result_owner {
                    RootResultRoute::TerminalResultOwner(retired.status.state())
                } else {
                    RootResultRoute::Terminal(retired.status.state())
                }
            }
            TaskLocation::Gone => RootResultRoute::Gone,
            TaskLocation::Unknown => RootResultRoute::UnknownTask,
        }
    }

    pub fn has_live_task(&self, identity: TaskIdentity) -> bool {
        let state = self.state.lock().expect(REGISTRY_LOCK);
        state
            .task_index
            .get(&identity)
            .and_then(|context| state.contexts.get(context))
            .and_then(|entry| entry.tasks.get(&identity))
            .is_some_and(|entry| matches!(entry, TaskEntry::Live(_)))
    }

    // ---------------------------------------------------- admission tickets

    /// Reserves worker-local query-context capacity before establish.
    pub fn acquire_query_context_admission_ticket(
        &self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> AdmissionTicketOutcome {
        let operation = request.envelope().operation_id();
        let context = request.context();
        if context.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }

        let now = self.clock.now();
        let acquisition = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return identity_mismatch(operation, mismatch);
            }
            if matches!(
                state.context_state(context),
                QueryContextState::Releasing
                    | QueryContextState::Aborting
                    | QueryContextState::TerminalRetained
                    | QueryContextState::Gone
            ) {
                self.admission_tickets.revoke_unredeemed(context, now);
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::ContextTerminalReceipt,
                    "admission ticket acquisition reached a closed query context",
                );
            }
            // The registry lock remains held across issuance so an abort cannot
            // close admission between the context-state check and the grant.
            self.admission_tickets.acquire(request, now)
        };

        match acquisition {
            Ok(grant) => {
                let outcome = match grant.progression() {
                    AdmissionTicketProgression::Issued => OperationOutcome::Accepted,
                    AdmissionTicketProgression::Replayed => OperationOutcome::Idempotent,
                };
                OperationReceipt::acknowledged(operation, outcome, grant.receipt())
            }
            Err(rejection) => {
                let outcome = match rejection {
                    AdmissionTicketAcquisitionRejection::ReservationCapacityExhausted
                    | AdmissionTicketAcquisitionRejection::ReplayCapacityExhausted => {
                        OperationOutcome::ResourceExhausted
                    }
                    AdmissionTicketAcquisitionRejection::Inactive(_) => {
                        OperationOutcome::ContextTerminalReceipt
                    }
                    AdmissionTicketAcquisitionRejection::ValidityExceedsWorkerLimit
                    | AdmissionTicketAcquisitionRejection::OperationReplayConflict
                    | AdmissionTicketAcquisitionRejection::SealedEpoch => {
                        OperationOutcome::InvalidStateOrRequest
                    }
                    AdmissionTicketAcquisitionRejection::ContextAlreadyGranted => {
                        OperationOutcome::AdmissionTicketStillActive
                    }
                };
                OperationReceipt::rejected(operation, outcome, rejection.to_string())
            }
        }
    }

    /// Returns query-context capacity currently reserved by admission grants.
    pub fn admission_reservation_count(&self) -> usize {
        self.admission_tickets.reserved_count(self.clock.now())
    }

    // ---------------------------------------------------------------- create

    /// Creates one task, atomically or not at all.
    pub fn create_task(&self, request: &CreateTask) -> CreateTaskOutcome {
        let receipt = self.admit_create_task(request);
        // Read off the receipt this call is about to return, so the evidence
        // names the same verdict the frontend is given. Re-deriving it from
        // registry state here could disagree with that answer, because the
        // lock is released by now.
        marker::create_task(request.identity(), &receipt);
        receipt
    }

    fn admit_create_task(&self, request: &CreateTask) -> CreateTaskOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let identity = request.identity();
        let context = request.context();
        let descriptor = request.descriptor();

        // Structural validation and process fencing come first, so a request
        // aimed at another process or naming an unfrozen member never
        // reserves an identity and never waits on a gate.
        if let Err(mismatch) = identity.verify_query_context(context) {
            return identity_mismatch(operation, mismatch);
        }
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        if let Err(rejection) = domains::validate_membership(descriptor, request.initial_domains())
        {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                rejection.detail(),
            );
        }

        let _scope = OperationScope::enter(self, context, Lane::Mutation);
        let deadline = self.deadline_of(envelope);
        let fingerprint = descriptor.fingerprint();
        let initial_keys = domains::initial_domain_keys(request.initial_domains());

        let (cell, source) = match self.elect_creation_owner(
            context,
            identity,
            operation,
            fingerprint,
            &initial_keys,
            deadline,
        ) {
            Ok(reservation) => reservation,
            Err(outcome) => return *outcome,
        };

        // The transaction owns the identity from here. Its Drop rolls back
        // every install, removes the reservation, and publishes the shared
        // failure to whoever converged on it.
        let mut transaction = CreationTransaction {
            registry: self,
            context,
            identity,
            descriptor: Arc::new(descriptor.clone()),
            cell,
            receiver_installed: false,
            capability_installed: false,
            failure: None,
            committed: false,
        };

        if let Err(rejection) = self.task_host.install_receiver(&transaction.descriptor) {
            return transaction.abandon(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                rejection.detail().as_str(),
            );
        }
        transaction.receiver_installed = true;
        if let Err(rejection) = self
            .task_host
            .install_inbound_capability(&transaction.descriptor)
        {
            return transaction.abandon(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                rejection.detail().as_str(),
            );
        }
        transaction.capability_installed = true;

        let mut task_domains = TaskDomains::for_descriptor(&transaction.descriptor);
        let receipts = match domains::apply_updates(
            &*self.task_host,
            &transaction.descriptor,
            &mut task_domains,
            request.initial_domains(),
        ) {
            Ok((receipts, _)) => receipts,
            Err(rejection) => {
                let outcome = rejection.outcome();
                let detail = rejection.detail().to_owned();
                return transaction.abandon(operation, outcome, detail);
            }
        };

        let status = Arc::new(TaskStatusOwner::new(
            identity,
            source,
            Arc::clone(&self.clock),
            self.config.metric_publish_min_interval,
        ));
        let runnable = match self.task_host.submit_runnable(
            &transaction.descriptor,
            TaskStatusReporter::new(Arc::clone(&status)),
        ) {
            Ok(runnable) => runnable,
            Err(rejection) => {
                let outcome = match rejection.category() {
                    TaskFailureCategory::ResourceExhausted => OperationOutcome::ResourceExhausted,
                    _ => OperationOutcome::InvalidStateOrRequest,
                };
                let detail = rejection.detail().as_str().to_owned();
                return transaction.abandon(operation, outcome, detail);
            }
        };

        let receipt = CreateTaskReceipt::new(identity, receipts, status.current());
        if let Some((runnable, status)) = transaction.commit(LiveTask {
            descriptor: Arc::clone(&transaction.descriptor),
            fingerprint,
            initial_domains: initial_keys,
            receipt: receipt.clone(),
            creation_failure: None,
            status,
            runnable,
            domains: task_domains,
            receiver_installed: true,
            capability_installed: true,
        }) {
            // The context closed while this task was being built. Stand the
            // submitted worker down. The closing context retained the live
            // entry, so its eventual stop and resource convergence remain
            // owned and charged rather than becoming an untracked orphan.
            let cause = self
                .termination_cause(context)
                .unwrap_or(AbortCause::QueryFailed);
            runnable.abort(cause);
            status.force_conclusion(cause);
            self.counters
                .creations_rolled_back
                .fetch_add(1, Ordering::Relaxed);
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                "the query context closed while this task was being created",
            );
        }
        self.counters.tasks_created.fetch_add(1, Ordering::Relaxed);
        crate::metrics::record_task_execution_task_created();
        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, receipt)
    }

    /// Elects a creation owner, waiting on the exact creation gate when the
    /// context does not exist yet.
    ///
    /// Every concurrent create of the identical descriptor converges on one
    /// owner: the winner reserves the identity, the others wait on the gate
    /// and then read the owner's result, so they all observe the same
    /// acknowledgement or the same failure.
    fn elect_creation_owner(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
        fingerprint: ContentFingerprint,
        initial_keys: &[InitialDomainKey],
        deadline: MonotonicInstant,
    ) -> Result<(Arc<CreationCell>, Arc<TaskStatusSource>), Box<CreateTaskOutcome>> {
        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        loop {
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return Err(Box::new(identity_mismatch(operation, mismatch)));
            }
            match classify_operation_admission(
                state.context_state(context),
                ContextOperationKind::CreateTask,
            ) {
                OperationAdmission::Admit => {}
                OperationAdmission::WaitForCreationGate => {
                    if now.has_reached(deadline) {
                        return Err(Box::new(OperationReceipt::rejected(
                            operation,
                            OperationOutcome::OperationTimedOut,
                            "create exceeded its effective wait on the creation gate",
                        )));
                    }
                    state = self.wait_gate(state);
                    continue;
                }
                OperationAdmission::NotEstablished => {
                    return Err(Box::new(OperationReceipt::rejected(
                        operation,
                        OperationOutcome::ContextNotEstablished,
                        "create reached a context that was never established",
                    )));
                }
                OperationAdmission::TerminalReceipt => {
                    // A create already holding this exact identity must finish
                    // publishing its immutable verdict even if context
                    // termination won in the meantime. Returning the generic
                    // context terminal here would make the converging caller
                    // disagree with the creation owner.
                    if let Some(TaskEntry::Creating(cell)) = state
                        .contexts
                        .get(&context)
                        .and_then(|entry| entry.tasks.get(&identity))
                    {
                        if !cell.same_creation(fingerprint, initial_keys) {
                            return Err(Box::new(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::CreateConflict,
                                "a different descriptor is already being created for this identity",
                            )));
                        }
                        if let Some(failure) = cell.failure() {
                            self.counters
                                .creations_converged
                                .fetch_add(1, Ordering::Relaxed);
                            return Err(Box::new(OperationReceipt::rejected(
                                operation,
                                failure.outcome,
                                failure.detail,
                            )));
                        }
                        if now.has_reached(deadline) {
                            return Err(Box::new(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::OperationTimedOut,
                                "create exceeded its effective wait behind a converging creation",
                            )));
                        }
                        state = self.wait_gate(state);
                        continue;
                    }
                    // The context is closed, but a create that already
                    // succeeded is still answerable from its retained record.
                    if let Some(outcome) = retained_create_reply(
                        &state,
                        context,
                        identity,
                        operation,
                        fingerprint,
                        initial_keys,
                    ) {
                        return Err(Box::new(outcome));
                    }
                    let outcome = terminal_outcome(&state, context);
                    return Err(Box::new(OperationReceipt::rejected(
                        operation,
                        outcome,
                        "create reached a retained terminal query context",
                    )));
                }
                OperationAdmission::Gone => {
                    return Err(Box::new(OperationReceipt::rejected(
                        operation,
                        OperationOutcome::Gone,
                        "create reached a reclaimed query context",
                    )));
                }
            }

            let decision = {
                let entry = state
                    .contexts
                    .get(&context)
                    .expect("an admitted context exists");
                match entry.tasks.get(&identity) {
                    Some(TaskEntry::Creating(cell)) => {
                        if cell.same_creation(fingerprint, initial_keys) {
                            Decision::Converge(Arc::clone(cell))
                        } else {
                            // A conflicting descriptor never preempts a
                            // creation already in progress.
                            Decision::done(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::CreateConflict,
                                "a different descriptor is already being created for this identity",
                            ))
                        }
                    }
                    Some(TaskEntry::Live(live)) => Decision::done(
                        if live.fingerprint != fingerprint || live.initial_domains != initial_keys {
                            OperationReceipt::rejected(
                                operation,
                                OperationOutcome::CreateConflict,
                                "this identity already carries a different descriptor",
                            )
                        } else if let Some(failure) = &live.creation_failure {
                            OperationReceipt::rejected(
                                operation,
                                failure.outcome,
                                failure.detail.clone(),
                            )
                        } else {
                            OperationReceipt::acknowledged(
                                operation,
                                OperationOutcome::Idempotent,
                                live.receipt.clone(),
                            )
                        },
                    ),
                    Some(TaskEntry::Retired(retired)) => Decision::done(
                        if retired.fingerprint != fingerprint
                            || retired.initial_domains != initial_keys
                        {
                            OperationReceipt::rejected(
                                operation,
                                OperationOutcome::CreateConflict,
                                "this identity already carries a different descriptor",
                            )
                        } else if let Some(failure) = &retired.creation_failure {
                            OperationReceipt::rejected(
                                operation,
                                failure.outcome,
                                failure.detail.clone(),
                            )
                        } else {
                            OperationReceipt::acknowledged(
                                operation,
                                OperationOutcome::Idempotent,
                                retired.receipt.clone(),
                            )
                        },
                    ),
                    Some(TaskEntry::Gone) => Decision::done(OperationReceipt::rejected(
                        operation,
                        OperationOutcome::Gone,
                        "this identity's retained record was reclaimed",
                    )),
                    None if entry.has_spent(identity) => {
                        Decision::done(OperationReceipt::rejected(
                            operation,
                            OperationOutcome::Gone,
                            "this identity's detailed record was reclaimed",
                        ))
                    }
                    None => {
                        if entry.occupied_slots() >= self.config.max_tasks_per_context {
                            Decision::done(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::ResourceExhausted,
                                "query context reached its cumulative task bound",
                            ))
                        } else if state.active_tasks >= self.config.max_active_tasks_per_backend {
                            Decision::done(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::ResourceExhausted,
                                "backend reached its active task bound",
                            ))
                        } else {
                            Decision::Reserve(Arc::clone(&entry.source))
                        }
                    }
                }
            };

            match decision {
                Decision::Done(outcome) => return Err(outcome),
                Decision::Reserve(source) => {
                    let cell = Arc::new(CreationCell::new(fingerprint, initial_keys.to_vec()));
                    state
                        .contexts
                        .get_mut(&context)
                        .expect("an admitted context exists")
                        .tasks
                        .insert(identity, TaskEntry::Creating(Arc::clone(&cell)));
                    state.active_tasks = state.active_tasks.saturating_add(1);
                    state.task_index.insert(identity, context);
                    return Ok((cell, source));
                }
                Decision::Converge(cell) => {
                    if let Some(failure) = cell.failure() {
                        self.counters
                            .creations_converged
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(Box::new(OperationReceipt::rejected(
                            operation,
                            failure.outcome,
                            failure.detail,
                        )));
                    }
                    if self.clock.now().has_reached(deadline) {
                        return Err(Box::new(OperationReceipt::rejected(
                            operation,
                            OperationOutcome::OperationTimedOut,
                            "create exceeded its effective wait behind a converging creation",
                        )));
                    }
                    state = self.wait_gate(state);
                }
            }
        }
    }

    // ----------------------------------------------------------- update task

    pub fn update_task(&self, request: &UpdateTask) -> UpdateTaskOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let identity = request.identity();
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let Some(context) = self.context_of(identity) else {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "update names a task this backend does not own",
            );
        };
        let _scope = OperationScope::enter(self, context, Lane::Mutation);

        // The descriptor and a snapshot of the current tokens are taken under
        // the lock. The snapshot only decides what the execution side is
        // asked to do; the authoritative tokens are re-classified on commit.
        let (descriptor, task_domains) = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            match self.locate_task_locked(&state, context, identity) {
                TaskLocation::Live => {
                    let live = live_task(&state, context, identity).expect("located live task");
                    (Arc::clone(&live.descriptor), live.domains.clone())
                }
                TaskLocation::Creating => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "update reached a task whose creation has not been acknowledged",
                    );
                }
                TaskLocation::Retired => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::TerminalRejected,
                        "update reached a terminal task and was never applied",
                    );
                }
                TaskLocation::Gone => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::Gone,
                        "update reached a reclaimed task record",
                    );
                }
                TaskLocation::Unknown => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "update names an unknown task",
                    );
                }
            }
        };

        let plan = match domains::plan_updates(&descriptor, &task_domains, request.domains()) {
            Ok(plan) => plan,
            Err(rejection) => {
                return OperationReceipt::rejected(
                    operation,
                    rejection.outcome(),
                    rejection.detail(),
                );
            }
        };
        let queued =
            match domains::apply_planned(&*self.task_host, &descriptor, request.domains(), &plan) {
                Ok(queued) => queued,
                Err(rejection) => {
                    return OperationReceipt::rejected(
                        operation,
                        rejection.outcome(),
                        rejection.detail(),
                    );
                }
            };

        let (receipts, applied) = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let Some(live) = live_task_mut(&mut state, context, identity) else {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::TerminalRejected,
                    "the task terminated while its update was being applied",
                );
            };
            // Re-classified against the tokens as they are now, so a
            // concurrent update cannot be rolled back by this one.
            match domains::commit_updates(&mut live.domains, request.domains(), &queued) {
                Ok(result) => result,
                Err(rejection) => {
                    return OperationReceipt::rejected(
                        operation,
                        rejection.outcome(),
                        rejection.detail(),
                    );
                }
            }
        };
        let outcome = if applied {
            OperationOutcome::Accepted
        } else {
            OperationOutcome::Idempotent
        };
        OperationReceipt::acknowledged(
            operation,
            outcome,
            UpdateTaskReceipt::new(identity, receipts),
        )
    }

    // -------------------------------------------------- update query context

    pub fn update_query_context(&self, request: &UpdateQueryContext) -> QueryContextOutcome {
        match request {
            UpdateQueryContext::Establish(establish) => {
                let receipt = self.establish_query_context(establish);
                // Emitted on the receipt, not inside the handler: the
                // establish decision has several exit paths and the receipt is
                // the one place all of them agree on.
                marker::establish_query_context(establish.context(), &receipt);
                receipt
            }
            UpdateQueryContext::AdvanceDomain(advance) => {
                self.advance_query_context_domain(advance)
            }
            UpdateQueryContext::RenewLease(renew) => {
                let receipt = self.renew_query_execution_lease(renew);
                marker::renew_query_execution_lease(renew.context(), renew.sequence(), &receipt);
                receipt
            }
        }
    }

    fn establish_query_context(&self, request: &EstablishQueryContext) -> QueryContextOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let context = request.context();
        if context.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let _scope = OperationScope::enter(self, context, Lane::Mutation);
        let record = EstablishRecord::of(request);
        let deadline = self.deadline_of(envelope);

        // The creation gate. Winning it installs the sequence-zero lease and
        // starts the backend-local timer at this exact linearization point,
        // so a slow materialization is already racing its own deadline.
        let mut transaction = loop {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return identity_mismatch(operation, mismatch);
            }
            let current = state.context_state(context);
            match classify_context_transition(current, QueryContextEvent::Establish) {
                ContextTransition::Apply(QueryContextState::Establishing) => {
                    if let Some(receipt) = self.redeem_admission_ticket(request, now) {
                        return receipt;
                    }
                    let lease = InstalledLease::install_initial(
                        request.initial_lease_valid_for(),
                        self.config.lease_bounds,
                        now,
                    );
                    let mut entry = ContextEntry::absent(Arc::new(TaskStatusSource::new()));
                    entry.state = QueryContextState::Establishing;
                    entry.lease = Some(lease);
                    entry.establish = Some(record.clone());
                    state.contexts.insert(context, entry);
                    state
                        .context_by_execution
                        .insert(context.query_execution_id(), context);
                    self.counters
                        .contexts_established
                        .fetch_add(1, Ordering::Relaxed);
                    break EstablishTransaction {
                        registry: self,
                        context,
                        committed: false,
                    };
                }
                ContextTransition::Idempotent => {
                    let entry = state.contexts.get(&context).expect("existing context");
                    let same = entry
                        .establish
                        .as_ref()
                        .is_some_and(|installed| installed.same_request(&record))
                        && entry
                            .domains
                            .initial_credential_matches(request.initial_credential());
                    let original_receipt = entry
                        .establish
                        .as_ref()
                        .and_then(|installed| installed.original_receipt.clone());
                    if !same {
                        return OperationReceipt::rejected(
                            operation,
                            OperationOutcome::ContextConflict,
                            "establish conflicts with the query context that already exists",
                        );
                    }
                    if let Some(receipt) = self.redeem_admission_ticket(request, now) {
                        return receipt;
                    }
                    if let Some(receipt) = original_receipt {
                        return OperationReceipt::acknowledged(
                            operation,
                            OperationOutcome::Idempotent,
                            receipt,
                        );
                    }
                    if now.has_reached(deadline) {
                        return OperationReceipt::rejected(
                            operation,
                            OperationOutcome::OperationTimedOut,
                            "exact establish replay timed out behind the creation gate",
                        );
                    }
                    drop(self.wait_gate(state));
                    continue;
                }
                ContextTransition::AlreadyTerminal => {
                    self.admission_tickets.revoke_unredeemed(context, now);
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::ContextTerminalReceipt,
                        "establish names a ticket for a closed query context",
                    );
                }
                ContextTransition::LostToRelease
                | ContextTransition::Illegal
                | ContextTransition::Apply(_) => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "establish is not legal in the current query context state",
                    );
                }
            }
        };

        // Materialization runs with the lock released. The shared facts stay
        // invisible until the context reaches `Active`.
        let materialized = self.context_host.materialize(SharedFactsRequest::new(
            context,
            request.catalog_binding(),
            request.initial_runtime_filter(),
            request.query_options(),
            request.initial_credential(),
        ));

        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        let now = self.clock.now();
        if let Err(rejection) = materialized {
            transaction.abandon(&mut state, AbortCause::QueryFailed, now);
            drop(state);
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                rejection.detail().as_str(),
            );
        }
        let expired = state
            .contexts
            .get(&context)
            .and_then(|entry| entry.lease)
            .is_some_and(|lease| lease.is_expired_at(now));
        if expired {
            // The establish lost the race against its own initial lease.
            transaction.abandon(&mut state, AbortCause::LeaseExpired, now);
            drop(state);
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::LeaseExpired,
                "the initial lease expired before the shared facts were materialized",
            );
        }
        let current = state.context_state(context);
        match classify_context_transition(current, QueryContextEvent::EstablishCompleted) {
            ContextTransition::Apply(QueryContextState::Active) => {
                let entry = state.contexts.get_mut(&context).expect("establishing");
                entry.state = QueryContextState::Active;
                entry.facts_visible = true;
                entry.domains = QueryContextDomains::install_initial(
                    record.semantic_identity.catalog_binding(),
                    record.semantic_identity.initial_runtime_filter(),
                    record.semantic_identity.credential_lease_id(),
                    record.semantic_identity.credential_epoch(),
                    Arc::clone(request.initial_credential().material()),
                );
                // Deliberately untouched: reaching `Active` must not reset or
                // extend the sequence-zero expiry.
                debug_assert!(
                    entry
                        .lease
                        .is_some_and(|lease| lease.sequence().is_initial())
                );
                let receipt = context_receipt(entry, context);
                entry
                    .establish
                    .as_mut()
                    .expect("establishing context retains its request")
                    .original_receipt = Some(receipt.clone());
                transaction.commit();
                self.gate.notify_all();
                drop(state);
                OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, receipt)
            }
            _ => {
                // An abort won the latch while the facts were loading.
                let outcome = terminal_outcome(&state, context);
                transaction.abandon(&mut state, AbortCause::QueryFailed, now);
                let receipt = state
                    .contexts
                    .get(&context)
                    .map(|entry| context_receipt(entry, context));
                drop(state);
                match receipt {
                    Some(receipt) => OperationReceipt::acknowledged(operation, outcome, receipt),
                    None => OperationReceipt::rejected(
                        operation,
                        outcome,
                        "the query context terminated while it was establishing",
                    ),
                }
            }
        }
    }

    fn redeem_admission_ticket(
        &self,
        request: &EstablishQueryContext,
        now: MonotonicInstant,
    ) -> Option<QueryContextOutcome> {
        let operation = request.envelope().operation_id();
        match self
            .admission_tickets
            .redeem(request.admission_ticket_id(), request.context(), now)
        {
            Ok(_) => None,
            Err(rejection) => {
                let outcome = match rejection {
                    AdmissionTicketRedemptionRejection::ForeignContext => {
                        OperationOutcome::ContextConflict
                    }
                    AdmissionTicketRedemptionRejection::Unknown
                    | AdmissionTicketRedemptionRejection::Expired
                    | AdmissionTicketRedemptionRejection::Closed => {
                        OperationOutcome::InvalidStateOrRequest
                    }
                };
                Some(OperationReceipt::rejected(
                    operation,
                    outcome,
                    rejection.to_string(),
                ))
            }
        }
    }

    fn advance_query_context_domain(
        &self,
        request: &AdvanceQueryContextDomain,
    ) -> QueryContextOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let context = request.context();
        let _scope = OperationScope::enter(self, context, Lane::Mutation);

        // Classification happens under the lock; the execution-side apply
        // then runs without it, and the token is committed afterwards.
        let progression = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return identity_mismatch(operation, mismatch);
            }
            match classify_operation_admission(
                state.context_state(context),
                ContextOperationKind::AdvanceDomain,
            ) {
                OperationAdmission::Admit => {}
                OperationAdmission::WaitForCreationGate | OperationAdmission::NotEstablished => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::ContextNotEstablished,
                        "a domain advance cannot create a query context",
                    );
                }
                OperationAdmission::TerminalReceipt => {
                    let outcome = terminal_outcome(&state, context);
                    let entry = state.contexts.get(&context).expect("closed context");
                    return OperationReceipt::acknowledged(
                        operation,
                        outcome,
                        context_receipt(entry, context),
                    );
                }
                OperationAdmission::Gone => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::Gone,
                        "a domain advance reached a reclaimed query context",
                    );
                }
            }
            let entry = state.contexts.get(&context).expect("active context");
            classify_shared_domain(entry, request.domain())
        };

        // A conflict is decided before the execution side is touched at all.
        if let DomainProgression::Conflict(conflict) = progression {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::DomainConflict,
                conflict.to_string(),
            );
        }
        if matches!(progression, DomainProgression::Apply)
            && let Err(rejection) = self
                .context_host
                .advance_shared_domain(context, request.domain())
        {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                rejection.detail().as_str(),
            );
        }

        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        let Some(entry) = state.contexts.get_mut(&context) else {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::Gone,
                "the query context was reclaimed while its domain was advancing",
            );
        };
        // Re-classified against the accepted token as it is now: an advance
        // whose version was overtaken while the execution side was applying
        // finds itself `Older` and commits nothing.
        let progression = classify_shared_domain(entry, request.domain());
        if let DomainProgression::Conflict(conflict) = progression {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::DomainConflict,
                conflict.to_string(),
            );
        }
        if matches!(progression, DomainProgression::Apply) {
            apply_shared_domain(entry, request.domain());
        }
        let domain_receipt = shared_domain_receipt(entry, request.domain(), progression);
        let receipt = context_receipt(entry, context).with_domains(vec![domain_receipt]);
        let outcome = match progression {
            DomainProgression::Apply => OperationOutcome::Accepted,
            // `Older` is settled, not applied: the retained receipt is
            // returned and no token ever rolls back.
            DomainProgression::Idempotent | DomainProgression::Older => {
                OperationOutcome::Idempotent
            }
            DomainProgression::Conflict(_) => unreachable!("conflicts return above"),
        };
        drop(state);
        OperationReceipt::acknowledged(operation, outcome, receipt)
    }

    fn renew_query_execution_lease(
        &self,
        request: &RenewQueryExecutionLease,
    ) -> QueryContextOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let context = request.context();
        let _scope = OperationScope::enter(self, context, Lane::Mutation);

        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        let now = self.clock.now();
        self.expire_leases_locked(&mut state, now);
        if let Err(mismatch) = fence_frontend(&state, context) {
            return identity_mismatch(operation, mismatch);
        }
        match classify_operation_admission(
            state.context_state(context),
            ContextOperationKind::RenewLease,
        ) {
            OperationAdmission::Admit => {}
            OperationAdmission::WaitForCreationGate | OperationAdmission::NotEstablished => {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::ContextNotEstablished,
                    "a renewal cannot create a query context",
                );
            }
            OperationAdmission::TerminalReceipt => {
                let outcome = terminal_outcome(&state, context);
                let entry = state.contexts.get(&context).expect("closed context");
                return OperationReceipt::acknowledged(
                    operation,
                    outcome,
                    context_receipt(entry, context),
                );
            }
            OperationAdmission::Gone => {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::Gone,
                    "a renewal reached a reclaimed query context",
                );
            }
        }
        let entry = state.contexts.get_mut(&context).expect("active context");
        let installed = entry.lease.expect("an active context holds a lease");
        match installed.classify_renewal(request.sequence(), request.valid_for()) {
            LeaseProgression::Apply { .. } => {
                let renewed = installed.renew(
                    request.sequence(),
                    request.valid_for(),
                    self.config.lease_bounds,
                    now,
                );
                entry.lease = Some(renewed);
                let receipt = context_receipt(entry, context);
                OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, receipt)
            }
            LeaseProgression::Idempotent { .. } | LeaseProgression::Stale { .. } => {
                // Neither extends anything: the accepted receipt is returned
                // exactly as it was first issued.
                let receipt = context_receipt(entry, context);
                OperationReceipt::acknowledged(operation, OperationOutcome::Idempotent, receipt)
            }
            LeaseProgression::Conflict { .. } => OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "the accepted lease sequence was replayed with a different duration",
            ),
            LeaseProgression::Gap {
                accepted, received, ..
            } => OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                format!(
                    "lease sequence {received} skipped past {}",
                    accepted.get() + 1
                ),
            ),
        }
    }

    // ----------------------------------------------------------- cancel task

    pub fn cancel_task(&self, request: &CancelTask) -> CancelTaskOutcome {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let Some(context) = self.context_of(identity) else {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "cancel names a task this backend does not own",
            );
        };
        let _scope = OperationScope::enter(self, context, Lane::Mutation);

        let runnable = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            match self.locate_task_locked(&state, context, identity) {
                TaskLocation::Live => {
                    let live = live_task(&state, context, identity).expect("located live task");
                    let status = Arc::clone(&live.status);
                    let runnable = Arc::clone(&live.runnable);
                    if status.is_terminal() {
                        // A cancel racing a normal finish is expected — a
                        // `LIMIT` query does exactly this — so the terminal
                        // status settles it instead of failing the attempt.
                        return OperationReceipt::acknowledged(
                            operation,
                            OperationOutcome::Idempotent,
                            status.current(),
                        );
                    }
                    match status.advance(
                        TaskState::Canceling,
                        Some(TerminationDetail::Canceled(request.reason())),
                        TaskOutputFacts::default(),
                    ) {
                        StatusAdvance::AlreadyTerminal(_) => {
                            return OperationReceipt::acknowledged(
                                operation,
                                OperationOutcome::Idempotent,
                                status.current(),
                            );
                        }
                        StatusAdvance::Illegal { from, to } => {
                            return OperationReceipt::rejected(
                                operation,
                                OperationOutcome::InvalidStateOrRequest,
                                format!("cancel cannot move a task from {from} to {to}"),
                            );
                        }
                        _ => {}
                    }
                    Some((status, runnable))
                }
                TaskLocation::Creating => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "cancel reached a task whose creation has not been acknowledged",
                    );
                }
                TaskLocation::Retired => {
                    let retired = retired_task(&state, context, identity).expect("retired task");
                    return OperationReceipt::acknowledged(
                        operation,
                        OperationOutcome::Idempotent,
                        retired.status.clone(),
                    );
                }
                TaskLocation::Gone => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::Gone,
                        "cancel reached a reclaimed task record",
                    );
                }
                TaskLocation::Unknown => {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "cancel names an unknown task",
                    );
                }
            }
        };

        let (status, runnable) = runnable.expect("a live task was located");
        // Cancellation revokes client-visible output at its own linearization
        // point. The fragment may already have finished producing and dropped
        // its runnable handle, so task retirement cannot be the only cleanup.
        crate::runtime::result_buffer::discard_task(identity);
        runnable.cancel(request.reason());
        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, status.current())
    }

    // --------------------------------------------------- abort query context

    pub fn abort_query_context(&self, request: &AbortQueryContext) -> QueryContextOutcome {
        let receipt = self.apply_abort_query_context(request);
        marker::abort_query_context(request.context(), &receipt);
        receipt
    }

    fn apply_abort_query_context(&self, request: &AbortQueryContext) -> QueryContextOutcome {
        let operation = request.envelope().operation_id();
        let context = request.context();
        if context.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let _scope = OperationScope::enter(self, context, Lane::Mutation);

        let classification = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return identity_mismatch(operation, mismatch);
            }
            let current = state.context_state(context);
            let classification = classify_context_transition(current, QueryContextEvent::Abort);
            if matches!(classification, ContextTransition::Apply(_)) {
                self.begin_termination_locked(
                    &mut state,
                    context,
                    TerminationDetail::Aborted(request.cause()),
                    now,
                );
            }
            classification
        };

        // The winner drives the fan-out and the completion once.
        self.settle();

        let state = self.state.lock().expect(REGISTRY_LOCK);
        let receipt = state
            .contexts
            .get(&context)
            .map(|entry| context_receipt(entry, context));
        let outcome = match classification {
            ContextTransition::Apply(_) => OperationOutcome::Accepted,
            ContextTransition::Idempotent => OperationOutcome::Idempotent,
            // Normal closure already won, so this abort only receives the
            // normal terminal receipt and changes nothing.
            ContextTransition::LostToRelease | ContextTransition::AlreadyTerminal => {
                terminal_outcome(&state, context)
            }
            ContextTransition::Illegal => OperationOutcome::InvalidStateOrRequest,
        };
        drop(state);
        match receipt {
            Some(receipt) => OperationReceipt::acknowledged(operation, outcome, receipt),
            None => OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextNotEstablished,
                "abort reached a query context this backend does not hold",
            ),
        }
    }

    // ------------------------------------------------- release query context

    pub fn release_query_context(
        &self,
        request: &ReleaseQueryContext,
    ) -> ReleaseQueryContextOutcome {
        let receipt = self.apply_release_query_context(request);
        marker::release_query_context(
            request.context(),
            &receipt,
            &self.released_context_evidence(request.context()),
        );
        receipt
    }

    fn apply_release_query_context(
        &self,
        request: &ReleaseQueryContext,
    ) -> ReleaseQueryContextOutcome {
        let operation = request.envelope().operation_id();
        let context = request.context();
        if context.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        // A release deliberately does not register itself as an in-flight
        // operation: readiness asks whether anything *else* is still
        // draining.
        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        let now = self.clock.now();
        self.expire_leases_locked(&mut state, now);
        if let Err(mismatch) = fence_frontend(&state, context) {
            return identity_mismatch(operation, mismatch);
        }
        let current = state.context_state(context);
        let (release, outcome, applied) = match current {
            QueryContextState::Absent => {
                drop(state);
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::ContextNotEstablished,
                    "release reached a context that was never established",
                );
            }
            QueryContextState::Gone => {
                drop(state);
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::Gone,
                    "release reached a reclaimed query context",
                );
            }
            QueryContextState::Establishing => {
                drop(state);
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::InvalidStateOrRequest,
                    "release is not legal while a query context is establishing",
                );
            }
            // An abort linearized first, so release finds a terminating
            // context and reports the terminal rather than an illegal move.
            QueryContextState::Aborting | QueryContextState::TerminalRetained => (
                ReleaseOutcome::AlreadyTerminal,
                OperationOutcome::ContextTerminalReceipt,
                false,
            ),
            QueryContextState::Releasing => (
                ReleaseOutcome::Released,
                OperationOutcome::Idempotent,
                false,
            ),
            QueryContextState::Active => {
                self.retire_locked(&mut state, now);
                if self.release_ready_locked(&state, context) {
                    let entry = state.contexts.get_mut(&context).expect("active context");
                    entry.state = QueryContextState::Releasing;
                    self.task_host.close_context_admission(context);
                    self.admission_tickets.revoke_unredeemed(context, now);
                    (ReleaseOutcome::Released, OperationOutcome::Accepted, true)
                } else {
                    // Nothing was applied and the first-wins position is
                    // untouched, so the identical request may be retried once
                    // local state advances.
                    (
                        ReleaseOutcome::NotReady,
                        OperationOutcome::ReleaseNotReady,
                        false,
                    )
                }
            }
        };
        drop(state);
        // Settling always runs: a latch this call observed still owes its
        // fan-out even when the release itself changed nothing.
        let _ = applied;
        self.settle();

        let state = self.state.lock().expect(REGISTRY_LOCK);
        let entry = state.contexts.get(&context);
        let observed = entry.map_or(QueryContextState::Gone, |entry| entry.state);
        let cause = entry.and_then(ContextEntry::termination_cause);
        drop(state);
        OperationReceipt::acknowledged(
            operation,
            outcome,
            ReleaseAcknowledgement::new(context, release, observed, cause),
        )
    }

    /// Whether a release may linearize.
    ///
    /// It looks only at what this backend can observe locally: every task it
    /// knows is a terminal record, every output has drained, and no other
    /// operation or read is in flight. It never reconstructs an expected task
    /// set, because a legal create may still be arriving.
    fn release_ready_locked(&self, state: &RegistryState, context: QueryContextRef) -> bool {
        let Some(entry) = state.contexts.get(&context) else {
            return false;
        };
        let tasks_drained = entry.tasks.values().all(TaskEntry::is_terminal_record);
        tasks_drained && state.in_flight_of(context).is_idle()
    }

    // ----------------------------------------------------------------- reads

    pub fn fetch_task_dynamic_filters(
        &self,
        request: &FetchTaskDynamicFilters,
    ) -> DynamicFilterReadOutcome {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let Some(context) = self.context_of(identity) else {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "dynamic filter read names a task this backend does not own",
            );
        };
        let _scope = OperationScope::enter(self, context, Lane::Read);
        let state = self.state.lock().expect(REGISTRY_LOCK);
        match self.locate_task_locked(&state, context, identity) {
            TaskLocation::Live => {
                let live = live_task(&state, context, identity).expect("located live task");
                let read = live.status.dynamic_filters();
                drop(state);
                match read {
                    Some(read)
                        if request
                            .acknowledged_version()
                            .is_none_or(|acknowledged| read.version() > acknowledged) =>
                    {
                        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, read)
                    }
                    Some(read) => OperationReceipt::acknowledged(
                        operation,
                        OperationOutcome::Idempotent,
                        read,
                    ),
                    None => OperationReceipt::settled(
                        operation,
                        OperationOutcome::Idempotent,
                        "this task has not advertised a dynamic filter domain",
                    ),
                }
            }
            TaskLocation::Creating | TaskLocation::Unknown => OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "dynamic filter read names a task that is not observable",
            ),
            // A terminal task's filter payload is not part of its
            // secret-free retained record, and nothing further will be
            // advertised, so the read is settled with nothing to return.
            TaskLocation::Retired => OperationReceipt::settled(
                operation,
                OperationOutcome::Idempotent,
                "this task is terminal and retains no dynamic filter payload",
            ),
            TaskLocation::Gone => OperationReceipt::rejected(
                operation,
                OperationOutcome::Gone,
                "dynamic filter read reached a reclaimed task record",
            ),
        }
    }

    pub fn get_final_task_info(&self, request: &GetFinalTaskInfo) -> FinalTaskInfoOutcome {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let Some(context) = self.context_of(identity) else {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "final info read names a task this backend does not own",
            );
        };
        let _scope = OperationScope::enter(self, context, Lane::Read);
        let state = self.state.lock().expect(REGISTRY_LOCK);
        match self.locate_task_locked(&state, context, identity) {
            TaskLocation::Live => {
                let live = live_task(&state, context, identity).expect("located live task");
                let info = live.status.final_info();
                drop(state);
                match info {
                    Some(info) => {
                        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, info)
                    }
                    None => OperationReceipt::rejected(
                        operation,
                        OperationOutcome::InvalidStateOrRequest,
                        "final info requires a terminal task",
                    ),
                }
            }
            TaskLocation::Retired => {
                let retired = retired_task(&state, context, identity).expect("retired task");
                let info = retired.final_info.clone();
                drop(state);
                match info {
                    Some(info) => {
                        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, info)
                    }
                    None => OperationReceipt::settled(
                        operation,
                        OperationOutcome::Idempotent,
                        "this task retained no final info",
                    ),
                }
            }
            TaskLocation::Creating | TaskLocation::Unknown => OperationReceipt::rejected(
                operation,
                OperationOutcome::InvalidStateOrRequest,
                "final info read names a task that is not observable",
            ),
            TaskLocation::Gone => OperationReceipt::rejected(
                operation,
                OperationOutcome::Gone,
                "final info read reached a reclaimed task record",
            ),
        }
    }

    // ------------------------------------------------------------- deadlines

    /// Re-evaluates every backend-local deadline and wakes the waiters.
    ///
    /// A deployment drives this from a maintenance tick. It is the only thing
    /// that turns the passage of time into a decision, which is what makes
    /// the whole lifecycle testable by moving an injected clock.
    pub fn advance_deadlines(&self) -> DeadlineSweep {
        let mut sweep = self.settle();
        sweep.admission_tickets_expired =
            self.admission_tickets.advance_deadlines(self.clock.now());
        sweep.metrics_flushed = self.flush_throttled_metrics();
        sweep
    }

    fn settle(&self) -> DeadlineSweep {
        let mut sweep = DeadlineSweep::default();
        for _ in 0..MAX_SETTLE_PASSES {
            let fanouts = {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                let now = self.clock.now();
                sweep.leases_expired += self.expire_leases_locked(&mut state, now);
                self.escalate_task_failures_locked(&mut state, now);
                std::mem::take(&mut state.pending_termination)
            };
            for context in &fanouts {
                self.stand_down_tasks(*context);
            }
            {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                let now = self.clock.now();
                sweep.tasks_retired += self.retire_locked(&mut state, now);
                self.complete_terminations_locked(&mut state, now);
                let (tasks, contexts) = self.reap_locked(&mut state, now);
                sweep.tasks_reaped += tasks;
                sweep.contexts_reaped += contexts;
                self.enforce_capacity_locked(&mut state);
            }
            self.gate.notify_all();
            if fanouts.is_empty() {
                break;
            }
        }
        sweep
    }

    fn flush_throttled_metrics(&self) -> usize {
        let owners: Vec<Arc<TaskStatusOwner>> = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            state
                .contexts
                .values()
                .flat_map(|entry| entry.tasks.values())
                .filter_map(|task| match task {
                    TaskEntry::Live(live) => Some(Arc::clone(&live.status)),
                    _ => None,
                })
                .collect()
        };
        owners
            .into_iter()
            .filter(|owner| {
                owner
                    .flush_throttled_metrics()
                    .published_version()
                    .is_some()
            })
            .count()
    }

    /// Fails an expired lease closed. It never extends anything and never
    /// waits for the fan-out, which happens with the lock released.
    fn expire_leases_locked(&self, state: &mut RegistryState, now: MonotonicInstant) -> usize {
        let expired: Vec<QueryContextRef> = state
            .contexts
            .iter()
            .filter(|(_, entry)| {
                matches!(
                    entry.state,
                    QueryContextState::Establishing | QueryContextState::Active
                ) && entry.lease.is_some_and(|lease| lease.is_expired_at(now))
            })
            .map(|(context, _)| *context)
            .collect();
        let mut count = 0;
        for context in expired {
            if self.begin_termination_locked(
                state,
                context,
                TerminationDetail::Aborted(AbortCause::LeaseExpired),
                now,
            ) {
                self.counters.lease_expiries.fetch_add(1, Ordering::Relaxed);
                marker::query_execution_lease_expired(context);
                count += 1;
            }
        }
        count
    }

    /// Lets a task's own failure race the same first-wins latch as an
    /// explicit abort and a lease expiry.
    fn escalate_task_failures_locked(&self, state: &mut RegistryState, now: MonotonicInstant) {
        let mut escalations: Vec<(QueryContextRef, TerminationDetail)> = Vec::new();
        for (context, entry) in &state.contexts {
            if !matches!(
                entry.state,
                QueryContextState::Establishing | QueryContextState::Active
            ) {
                continue;
            }
            if !entry.source.failure_seen() {
                continue;
            }
            for task in entry.tasks.values() {
                if let TaskEntry::Live(live) = task {
                    let status = live.status.current();
                    if status.state().is_failure()
                        && let Some(TerminationDetail::Failed(failure)) = status.termination()
                    {
                        escalations.push((*context, TerminationDetail::Failed(failure.clone())));
                        break;
                    }
                }
            }
        }
        for (context, detail) in escalations {
            if self.begin_termination_locked(state, context, detail, now) {
                self.counters
                    .task_failure_escalations
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Races one cause for a context's termination latch.
    ///
    /// Returns whether this cause won. Only the winner revokes capabilities
    /// and schedules the single fan-out.
    fn begin_termination_locked(
        &self,
        state: &mut RegistryState,
        context: QueryContextRef,
        detail: TerminationDetail,
        now: MonotonicInstant,
    ) -> bool {
        let current = state.context_state(context);
        if current == QueryContextState::Absent {
            // The pre-establish fence: aborting an absent context stops a
            // late but still legal establish or create from reviving it.
            let mut entry = ContextEntry::absent(Arc::new(TaskStatusSource::new()));
            entry.latch.latch(detail);
            entry.state = QueryContextState::TerminalRetained;
            entry.retired_at = Some(now);
            state.contexts.insert(context, entry);
            state
                .context_by_execution
                .insert(context.query_execution_id(), context);
            state.retired_context_order.push_back(context);
            self.task_host.close_context_admission(context);
            self.admission_tickets.release_context(context, now);
            return true;
        }
        if !matches!(
            classify_context_transition(current, QueryContextEvent::Abort),
            ContextTransition::Apply(QueryContextState::Aborting)
        ) {
            return false;
        }
        let (won, task_identities) = {
            let entry = state
                .contexts
                .get_mut(&context)
                .expect("a non-absent context exists");
            if !matches!(entry.latch.latch(detail), LatchOutcome::Won) {
                return false;
            }
            self.task_host.close_context_admission(context);
            entry.state = QueryContextState::Aborting;
            entry.terminating_since = Some(now);
            entry.lease = None;
            // Capability revocation belongs to the winner and happens once,
            // at the linearization point, not once per observer.
            for task in entry.tasks.values_mut() {
                if let TaskEntry::Live(live) = task
                    && live.capability_installed
                {
                    self.task_host.remove_inbound_capability(&live.descriptor);
                    live.capability_installed = false;
                }
            }
            (true, entry.tasks.keys().copied().collect::<Vec<_>>())
        };
        if won {
            // The context termination invalidates every unconsumed root result
            // immediately, including output whose producer already finished
            // and no longer holds a fragment cancellation handle.
            for identity in task_identities {
                crate::runtime::result_buffer::discard_task(identity);
            }
            self.admission_tickets.revoke_unredeemed(context, now);
            state.pending_termination.insert(context);
        }
        won
    }

    /// Asks every non-terminal task of a terminating context to stand down.
    fn stand_down_tasks(&self, context: QueryContextRef) {
        let (targets, cause) = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            let Some(entry) = state.contexts.get(&context) else {
                return;
            };
            if entry.state != QueryContextState::Aborting {
                return;
            }
            let cause = entry.termination_cause().unwrap_or(AbortCause::QueryFailed);
            let targets: Vec<(Arc<TaskStatusOwner>, Arc<dyn RunnableTask>)> = entry
                .tasks
                .values()
                .filter_map(|task| match task {
                    TaskEntry::Live(live) if !live.status.is_terminal() => {
                        Some((Arc::clone(&live.status), Arc::clone(&live.runnable)))
                    }
                    _ => None,
                })
                .collect();
            (targets, cause)
        };
        for (status, runnable) in targets {
            status.advance(
                TaskState::Aborting,
                Some(TerminationDetail::Aborted(cause)),
                TaskOutputFacts::default(),
            );
            runnable.abort(cause);
        }
    }

    /// Moves every terminal task into its secret-free retained record.
    ///
    /// A task only moves after its stable conclusion, actual stop, output
    /// release, and runtime-resource convergence have each been observed.
    /// Termination grace may fix the conclusion, but cannot manufacture any
    /// of the physical convergence facts.
    fn retire_locked(&self, state: &mut RegistryState, now: MonotonicInstant) -> usize {
        let contexts: Vec<QueryContextRef> = state.contexts.keys().copied().collect();
        let mut retired = 0;
        for context in contexts {
            let force = matches!(
                state.context_state(context),
                QueryContextState::Aborting | QueryContextState::TerminalRetained
            );
            // A task that ignored stand-down gets a bounded window before its
            // conclusion is fixed. It remains live and charged until its
            // execution owner supplies the remaining convergence facts.
            if force {
                self.force_stalled_tasks_locked(state, context, now);
            } else {
                let unchanged = state.contexts.get(&context).is_some_and(|entry| {
                    entry.last_retire_revision == Some(entry.source.revision())
                });
                if unchanged {
                    continue;
                }
            }
            if let Some(entry) = state.contexts.get_mut(&context) {
                entry.last_retire_revision = Some(entry.source.revision());
            }
            let retirements: Vec<(TaskIdentity, usize, TaskState)> = {
                let Some(entry) = state.contexts.get_mut(&context) else {
                    continue;
                };
                let ready: Vec<TaskIdentity> = entry
                    .tasks
                    .iter()
                    .filter_map(|(identity, task)| match task {
                        TaskEntry::Live(live) if live.status.retirement_ready() => Some(*identity),
                        _ => None,
                    })
                    .collect();
                let mut retirements = Vec::with_capacity(ready.len());
                for identity in ready {
                    let Some(TaskEntry::Live(live)) = entry.tasks.remove(&identity) else {
                        continue;
                    };
                    let mut live = *live;
                    live.status
                        .retire()
                        .expect("a retirement-ready live task can retire exactly once");
                    if live.capability_installed {
                        self.task_host.remove_inbound_capability(&live.descriptor);
                        live.capability_installed = false;
                    }
                    if live.receiver_installed {
                        self.task_host.remove_receiver(&live.descriptor);
                        live.receiver_installed = false;
                    }
                    let status = live.status.current();
                    let final_info = live.status.final_info();
                    let result_owner = live.descriptor.sink_kind() == FragmentSinkKind::Result;
                    let bytes =
                        estimate_retained_bytes(&live.receipt, &status, final_info.as_ref());
                    let terminal_state = status.state();
                    entry.tasks.insert(
                        identity,
                        TaskEntry::Retired(Box::new(RetiredTask {
                            fingerprint: live.fingerprint,
                            initial_domains: live.initial_domains,
                            receipt: live.receipt,
                            creation_failure: live.creation_failure,
                            status,
                            final_info,
                            result_owner,
                            retired_at: now,
                            bytes,
                        })),
                    );
                    retirements.push((identity, bytes, terminal_state));
                }
                retirements
            };
            for (identity, bytes, terminal_state) in retirements {
                crate::runtime::result_buffer::retire_task_result(identity);
                state.active_tasks = state.active_tasks.saturating_sub(1);
                state.retained_tasks = state.retained_tasks.saturating_add(1);
                state.retained_bytes = state.retained_bytes.saturating_add(bytes);
                state.retired_task_order.push_back((context, identity));
                marker::task_terminal_retained(identity, terminal_state, bytes);
                retired += 1;
            }
        }
        retired
    }

    /// Fixes the conclusion of every task that outlived termination grace.
    fn force_stalled_tasks_locked(
        &self,
        state: &mut RegistryState,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) {
        let Some(entry) = state.contexts.get(&context) else {
            return;
        };
        let Some(since) = entry.terminating_since else {
            return;
        };
        if now.saturating_duration_since(since) < self.config.termination_grace {
            return;
        }
        let cause = entry.termination_cause().unwrap_or(AbortCause::QueryFailed);
        let stalled: Vec<Arc<TaskStatusOwner>> = entry
            .tasks
            .values()
            .filter_map(|task| match task {
                TaskEntry::Live(live) if !live.status.is_terminal() => {
                    Some(Arc::clone(&live.status))
                }
                _ => None,
            })
            .collect();
        for status in stalled {
            status.force_conclusion(cause);
        }
    }

    /// Completes a terminating or releasing context once every task it knows
    /// is a terminal record.
    fn complete_terminations_locked(&self, state: &mut RegistryState, now: MonotonicInstant) {
        let contexts: Vec<QueryContextRef> = state.contexts.keys().copied().collect();
        for context in contexts {
            let event = {
                let Some(entry) = state.contexts.get(&context) else {
                    continue;
                };
                let ready = entry.tasks.values().all(TaskEntry::is_terminal_record);
                match entry.state {
                    QueryContextState::Aborting if ready => QueryContextEvent::AbortCompleted,
                    QueryContextState::Releasing if ready => QueryContextEvent::ReleaseCompleted,
                    _ => continue,
                }
            };
            let current = state.context_state(context);
            let ContextTransition::Apply(next) = classify_context_transition(current, event) else {
                continue;
            };
            let task_identities = {
                let entry = state
                    .contexts
                    .get_mut(&context)
                    .expect("a completing context exists");
                entry.state = next;
                entry.retired_at = Some(now);
                entry.lease = None;
                entry.facts_visible = false;
                if !entry.facts_released {
                    // Retained on the entry: this is the only point at which
                    // the host seals it, and the release acknowledgement that
                    // reports it is encoded from the retired entry afterwards.
                    entry.released_evidence = self.context_host.release(context);
                    entry.facts_released = true;
                }
                entry.domains = QueryContextDomains::empty();
                if event == QueryContextEvent::AbortCompleted {
                    marker::context_termination_completed(
                        context,
                        entry.latch.cause(),
                        entry.tasks.len(),
                    );
                }
                entry.tasks.keys().copied().collect::<Vec<_>>()
            };
            for identity in task_identities {
                crate::runtime::result_buffer::discard_task(identity);
            }
            self.admission_tickets.release_context(context, now);
            state.retired_context_order.push_back(context);
        }
    }

    /// Releases retained records whose request horizon has elapsed.
    fn reap_locked(&self, state: &mut RegistryState, now: MonotonicInstant) -> (usize, usize) {
        let horizon = self.config.request_horizon;
        let mut tasks = 0;
        while let Some((context, identity)) = state.retired_task_order.front().copied() {
            let retired_at = match state
                .contexts
                .get(&context)
                .and_then(|entry| entry.tasks.get(&identity))
            {
                Some(TaskEntry::Retired(retired)) => retired.retired_at,
                _ => {
                    state.retired_task_order.pop_front();
                    continue;
                }
            };
            if horizon.must_retain_at(retired_at, now) {
                break;
            }
            state.retired_task_order.pop_front();
            self.reap_task_locked(state, context, identity);
            tasks += 1;
        }

        let mut contexts = 0;
        while let Some(context) = state.retired_context_order.front().copied() {
            let retired_at = match state.contexts.get(&context) {
                Some(entry) if entry.state == QueryContextState::TerminalRetained => {
                    entry.retired_at
                }
                _ => {
                    state.retired_context_order.pop_front();
                    continue;
                }
            };
            let Some(retired_at) = retired_at else {
                state.retired_context_order.pop_front();
                continue;
            };
            if horizon.must_retain_at(retired_at, now) {
                break;
            }
            state.retired_context_order.pop_front();
            self.reap_context_locked(state, context);
            contexts += 1;
        }
        (tasks, contexts)
    }

    fn reap_task_locked(
        &self,
        state: &mut RegistryState,
        context: QueryContextRef,
        identity: TaskIdentity,
    ) {
        crate::runtime::result_buffer::discard_task(identity);
        let bytes = {
            let Some(entry) = state.contexts.get_mut(&context) else {
                return;
            };
            let bytes = entry
                .tasks
                .get(&identity)
                .map_or(0, TaskEntry::retained_bytes);
            entry.tasks.insert(identity, TaskEntry::Gone);
            entry.source.mark_gone(identity);
            bytes
        };
        state.retained_tasks = state.retained_tasks.saturating_sub(1);
        state.retained_bytes = state.retained_bytes.saturating_sub(bytes);
        state.gone_task_order.push_back((context, identity));
    }

    fn reap_context_locked(&self, state: &mut RegistryState, context: QueryContextRef) {
        let identities: Vec<TaskIdentity> = {
            let Some(entry) = state.contexts.get_mut(&context) else {
                return;
            };
            let ContextTransition::Apply(QueryContextState::Gone) =
                classify_context_transition(entry.state, QueryContextEvent::Reap)
            else {
                return;
            };
            entry.state = QueryContextState::Gone;
            entry.domains = QueryContextDomains::empty();
            entry.establish = None;
            entry.tasks.keys().copied().collect()
        };
        for identity in &identities {
            crate::runtime::result_buffer::discard_task(*identity);
            let bytes = state
                .contexts
                .get(&context)
                .and_then(|entry| entry.tasks.get(identity))
                .map_or(0, TaskEntry::retained_bytes);
            if bytes > 0 {
                state.retained_tasks = state.retained_tasks.saturating_sub(1);
                state.retained_bytes = state.retained_bytes.saturating_sub(bytes);
            }
        }
        if let Some(entry) = state.contexts.get_mut(&context) {
            entry.tasks.clear();
            entry.clear_spent();
        }
        state.gone_context_order.push_back(context);
    }

    /// Sweeps by capacity as well as by horizon, so retention is bounded in
    /// records and in bytes and never becomes an unbounded tombstone.
    fn enforce_capacity_locked(&self, state: &mut RegistryState) {
        while state.retained_tasks > self.config.retained_task_capacity
            || state.retained_bytes > self.config.retained_task_max_bytes
        {
            let Some((context, identity)) = state.retired_task_order.pop_front() else {
                break;
            };
            if !matches!(
                state
                    .contexts
                    .get(&context)
                    .and_then(|entry| entry.tasks.get(&identity)),
                Some(TaskEntry::Retired(_))
            ) {
                continue;
            }
            self.reap_task_locked(state, context, identity);
        }
        while state.gone_task_order.len() > self.config.gone_fence_capacity {
            let Some((context, identity)) = state.gone_task_order.pop_front() else {
                break;
            };
            if let Some(entry) = state.contexts.get_mut(&context) {
                entry.tasks.remove(&identity);
            }
            let spent = state
                .contexts
                .get(&context)
                .is_some_and(|entry| entry.has_spent(identity));
            if !spent {
                state.task_index.remove(&identity);
            }
        }
        while state.retired_context_order.len() > self.config.retained_context_capacity {
            let Some(context) = state.retired_context_order.pop_front() else {
                break;
            };
            self.reap_context_locked(state, context);
        }
        while state.gone_context_order.len() > self.config.gone_fence_capacity {
            let Some(context) = state.gone_context_order.pop_front() else {
                break;
            };
            if let Some(entry) = state.contexts.remove(&context) {
                for identity in entry.tasks.keys() {
                    state.task_index.remove(identity);
                }
            }
            self.task_host.forget_context_admission(context);
            state.task_index.retain(|_, owner| *owner != context);
            if state
                .context_by_execution
                .get(&context.query_execution_id())
                == Some(&context)
            {
                state
                    .context_by_execution
                    .remove(&context.query_execution_id());
            }
        }
    }

    // ------------------------------------------------------------- internals

    fn deadline_of(&self, envelope: OperationEnvelope) -> MonotonicInstant {
        let effective = self
            .config
            .wait_caps
            .clamp(envelope.kind(), envelope.max_wait());
        self.clock.now().saturating_add(effective)
    }

    fn wait_gate<'a>(
        &'a self,
        guard: MutexGuard<'a, RegistryState>,
    ) -> MutexGuard<'a, RegistryState> {
        let (guard, _) = self
            .gate
            .wait_timeout(guard, self.config.gate_poll_interval)
            .expect(REGISTRY_LOCK);
        guard
    }

    fn context_of(&self, identity: TaskIdentity) -> Option<QueryContextRef> {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .task_index
            .get(&identity)
            .copied()
    }

    fn locate_task_locked(
        &self,
        state: &RegistryState,
        context: QueryContextRef,
        identity: TaskIdentity,
    ) -> TaskLocation {
        if state.context_state(context) == QueryContextState::Gone {
            return TaskLocation::Gone;
        }
        match state
            .contexts
            .get(&context)
            .and_then(|entry| entry.tasks.get(&identity))
        {
            Some(TaskEntry::Creating(_)) => TaskLocation::Creating,
            Some(TaskEntry::Live(_)) => TaskLocation::Live,
            Some(TaskEntry::Retired(_)) => TaskLocation::Retired,
            Some(TaskEntry::Gone) => TaskLocation::Gone,
            None if state
                .contexts
                .get(&context)
                .is_some_and(|entry| entry.has_spent(identity)) =>
            {
                TaskLocation::Gone
            }
            None => TaskLocation::Unknown,
        }
    }

    fn enter(&self, context: QueryContextRef, lane: Lane) {
        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        let counters = state.in_flight.entry(context).or_default();
        match lane {
            Lane::Mutation => counters.mutations = counters.mutations.saturating_add(1),
            Lane::Read => counters.reads = counters.reads.saturating_add(1),
        }
    }

    fn leave(&self, context: QueryContextRef, lane: Lane) {
        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        if let Some(counters) = state.in_flight.get_mut(&context) {
            match lane {
                Lane::Mutation => counters.mutations = counters.mutations.saturating_sub(1),
                Lane::Read => counters.reads = counters.reads.saturating_sub(1),
            }
            if counters.is_idle() {
                state.in_flight.remove(&context);
            }
        }
    }
}

/// What the creation-owner election decided for one create.
enum Decision {
    /// This create reserves the identity and runs the transaction.
    Reserve(Arc<TaskStatusSource>),
    /// An identical creation is already in progress; wait for its result.
    Converge(Arc<CreationCell>),
    /// The create is already answerable without a transaction.
    Done(Box<CreateTaskOutcome>),
}

impl Decision {
    fn done(outcome: CreateTaskOutcome) -> Self {
        Self::Done(Box::new(outcome))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum TaskLocation {
    Creating,
    Live,
    Retired,
    Gone,
    Unknown,
}

/// Runs one operation's in-flight accounting and settles the owner afterwards.
struct OperationScope<'a> {
    registry: &'a TaskExecutionRegistry,
    context: QueryContextRef,
    lane: Lane,
}

impl<'a> OperationScope<'a> {
    fn enter(registry: &'a TaskExecutionRegistry, context: QueryContextRef, lane: Lane) -> Self {
        registry.enter(context, lane);
        Self {
            registry,
            context,
            lane,
        }
    }
}

impl Drop for OperationScope<'_> {
    fn drop(&mut self) {
        self.registry.leave(self.context, self.lane);
        self.registry.settle();
    }
}

/// The establish creation gate's rollback guard.
///
/// Everything the winning establish installed — the sequence-zero lease, the
/// staged shared facts, and any task it admitted — is undone here unless the
/// context actually reached `Active`, and every create waiting on the gate is
/// woken so it fails with the establish rather than waiting out its deadline.
struct EstablishTransaction<'a> {
    registry: &'a TaskExecutionRegistry,
    context: QueryContextRef,
    committed: bool,
}

impl EstablishTransaction<'_> {
    fn commit(&mut self) {
        self.committed = true;
    }

    fn abandon(&mut self, state: &mut RegistryState, cause: AbortCause, now: MonotonicInstant) {
        self.committed = true;
        self.registry.begin_termination_locked(
            state,
            self.context,
            TerminationDetail::Aborted(cause),
            now,
        );
        self.registry
            .rollback_context_locked(state, self.context, now);
        self.registry
            .admission_tickets
            .release_context(self.context, now);
        self.registry
            .counters
            .contexts_rolled_back
            .fetch_add(1, Ordering::Relaxed);
        self.registry.gate.notify_all();
    }
}

impl Drop for EstablishTransaction<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.registry.state.lock().expect(REGISTRY_LOCK);
        let now = self.registry.clock.now();
        self.abandon(&mut state, AbortCause::QueryFailed, now);
    }
}

/// The establish rollback, kept in its own block beside the transaction that
/// is its only caller.
impl TaskExecutionRegistry {
    /// Undoes everything an establish installed and retires the context.
    fn rollback_context_locked(
        &self,
        state: &mut RegistryState,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) {
        let Some(entry) = state.contexts.get_mut(&context) else {
            return;
        };
        entry.lease = None;
        entry.facts_visible = false;
        entry.domains = QueryContextDomains::empty();
        if !entry.facts_released {
            self.context_host.release(context);
            entry.facts_released = true;
        }
        let live: Vec<TaskIdentity> = entry
            .tasks
            .iter()
            .filter_map(|(identity, task)| match task {
                TaskEntry::Live(_) => Some(*identity),
                _ => None,
            })
            .collect();
        for identity in live {
            if let Some(TaskEntry::Live(task)) = entry.tasks.remove(&identity) {
                let mut task = *task;
                if task.capability_installed {
                    self.task_host.remove_inbound_capability(&task.descriptor);
                    task.capability_installed = false;
                }
                if task.receiver_installed {
                    self.task_host.remove_receiver(&task.descriptor);
                    task.receiver_installed = false;
                }
                state.active_tasks = state.active_tasks.saturating_sub(1);
                state.task_index.remove(&identity);
            }
        }
        entry.tasks.retain(|_, task| task.is_terminal_record());
        if entry.state != QueryContextState::TerminalRetained
            && entry.state != QueryContextState::Gone
        {
            entry.state = QueryContextState::TerminalRetained;
            entry.retired_at = Some(now);
            state.retired_context_order.push_back(context);
        }
        state.pending_termination.remove(&context);
    }
}

/// The creation transaction's rollback guard.
///
/// A creation is atomic because every step before the last one is undoable and
/// the last one is the only step that can start execution: the receiver, the
/// inbound capability, and the reservation are removed in reverse on any
/// failure, and `submit_runnable` either returns a handle — after which
/// nothing can fail — or returns an error before execution starts.
struct CreationTransaction<'a> {
    registry: &'a TaskExecutionRegistry,
    context: QueryContextRef,
    identity: TaskIdentity,
    descriptor: Arc<TaskDescriptor>,
    cell: Arc<CreationCell>,
    receiver_installed: bool,
    capability_installed: bool,
    failure: Option<CreationFailure>,
    committed: bool,
}

impl CreationTransaction<'_> {
    /// Records the failure this creation will report and lets Drop undo it.
    fn abandon(
        &mut self,
        operation: TaskOperationId,
        outcome: OperationOutcome,
        detail: impl Into<String>,
    ) -> CreateTaskOutcome {
        let detail = detail.into();
        self.failure = Some(CreationFailure {
            outcome,
            detail: detail.clone(),
        });
        OperationReceipt::rejected(operation, outcome, detail)
    }

    /// Installs the task, retaining convergence handles when the context
    /// closed underneath the transaction.
    ///
    /// The installs and the submission ran with the lock released, so an abort
    /// may have linearized in the meantime. Committing into a closed context
    /// would leave a live task nothing has agreed to supervise. The create
    /// still loses the race, but the closing context owns the submitted task
    /// until its physical responsibilities converge.
    fn commit(
        &mut self,
        mut live: LiveTask,
    ) -> Option<(Arc<dyn RunnableTask>, Arc<TaskStatusOwner>)> {
        let status = Arc::clone(&live.status);
        let runnable = Arc::clone(&live.runnable);
        let closed;
        {
            let mut state = self.registry.state.lock().expect(REGISTRY_LOCK);
            closed = state.context_state(self.context) != QueryContextState::Active;
            if closed {
                let failure = CreationFailure {
                    outcome: OperationOutcome::ContextTerminalReceipt,
                    detail: "the query context closed while this task was being created".to_owned(),
                };
                self.cell.fail(failure.clone());
                live.creation_failure = Some(failure);
            }
            let entry = state
                .contexts
                .get_mut(&self.context)
                .expect("a reserved creation retains its query context");
            entry.mark_spent(self.identity);
            entry
                .tasks
                .insert(self.identity, TaskEntry::Live(Box::new(live)));
            if !closed {
                // The acknowledgement is the linearization point, so the
                // first snapshot becomes observable exactly here.
                status.release_to_observers();
            }
        }
        // Completion can race ahead of the creation transaction. It may run
        // only after the task is findable as Live, so an immediate terminal
        // fact cannot make a creating task disappear behind the transaction.
        runnable.commit_creation();
        if closed {
            // The context already revoked new work. Close the submitted
            // task's independently installed data-plane capability as part of
            // the same losing create, while retaining its receiver until the
            // running task supplies actual-stop and resource evidence.
            self.registry
                .task_host
                .remove_inbound_capability(&self.descriptor);
            let mut state = self.registry.state.lock().expect(REGISTRY_LOCK);
            if let Some(TaskEntry::Live(live)) = state
                .contexts
                .get_mut(&self.context)
                .and_then(|entry| entry.tasks.get_mut(&self.identity))
            {
                live.capability_installed = false;
            }
        }
        self.committed = true;
        self.registry.gate.notify_all();
        closed.then_some((runnable, status))
    }
}

impl Drop for CreationTransaction<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if self.capability_installed {
            self.registry
                .task_host
                .remove_inbound_capability(&self.descriptor);
        }
        if self.receiver_installed {
            self.registry.task_host.remove_receiver(&self.descriptor);
        }
        let failure = self.failure.clone().unwrap_or(CreationFailure {
            outcome: OperationOutcome::InvalidStateOrRequest,
            detail: "the creation transaction did not complete".to_owned(),
        });
        self.cell.fail(failure);
        {
            let mut state = self.registry.state.lock().expect(REGISTRY_LOCK);
            let removed = matches!(
                state
                    .contexts
                    .get(&self.context)
                    .and_then(|entry| entry.tasks.get(&self.identity)),
                Some(TaskEntry::Creating(cell)) if Arc::ptr_eq(cell, &self.cell)
            );
            if removed {
                if let Some(entry) = state.contexts.get_mut(&self.context) {
                    entry.tasks.remove(&self.identity);
                }
                state.task_index.remove(&self.identity);
                state.active_tasks = state.active_tasks.saturating_sub(1);
            }
        }
        self.registry
            .counters
            .creations_rolled_back
            .fetch_add(1, Ordering::Relaxed);
        self.registry.gate.notify_all();
    }
}

// ------------------------------------------------------------ free functions

fn identity_mismatch<T>(
    operation: TaskOperationId,
    mismatch: IdentityMismatch,
) -> OperationReceipt<T> {
    OperationReceipt::rejected(
        operation,
        OperationOutcome::IdentityMismatch,
        mismatch.to_string(),
    )
}

/// Fences a request against the frontend incarnation this backend already
/// serves for the same query execution.
///
/// A task identity deliberately does not carry the frontend process id, so
/// this is the only place that can catch a request minted by a replaced
/// frontend.
fn fence_frontend(state: &RegistryState, context: QueryContextRef) -> Result<(), IdentityMismatch> {
    match state
        .context_by_execution
        .get(&context.query_execution_id())
    {
        Some(installed) if *installed != context => installed.verify_matches(context),
        _ => Ok(()),
    }
}

/// The outcome a closed or reclaimed context reports.
fn terminal_outcome(state: &RegistryState, context: QueryContextRef) -> OperationOutcome {
    match state.contexts.get(&context) {
        Some(entry) if entry.state == QueryContextState::Gone => OperationOutcome::Gone,
        Some(entry)
            if matches!(
                entry.latch.cause(),
                Some(TerminationDetail::Aborted(AbortCause::LeaseExpired))
            ) =>
        {
            OperationOutcome::LeaseExpired
        }
        Some(_) => OperationOutcome::ContextTerminalReceipt,
        None => OperationOutcome::Gone,
    }
}

fn context_receipt(entry: &ContextEntry, context: QueryContextRef) -> QueryContextReceipt {
    let receipt = QueryContextReceipt::new(context, entry.state);
    match entry.lease {
        Some(lease) => receipt.with_lease(lease.receipt()),
        None => receipt,
    }
}

/// Answers a create that already succeeded from its retained record.
fn retained_create_reply(
    state: &RegistryState,
    context: QueryContextRef,
    identity: TaskIdentity,
    operation: TaskOperationId,
    fingerprint: ContentFingerprint,
    initial_keys: &[InitialDomainKey],
) -> Option<CreateTaskOutcome> {
    match state
        .contexts
        .get(&context)
        .and_then(|entry| entry.tasks.get(&identity))?
    {
        TaskEntry::Retired(retired) => {
            if retired.fingerprint != fingerprint || retired.initial_domains != initial_keys {
                return Some(OperationReceipt::rejected(
                    operation,
                    OperationOutcome::CreateConflict,
                    "this identity already carries a different descriptor",
                ));
            }
            if let Some(failure) = &retired.creation_failure {
                return Some(OperationReceipt::rejected(
                    operation,
                    failure.outcome,
                    failure.detail.clone(),
                ));
            }
            Some(OperationReceipt::acknowledged(
                operation,
                OperationOutcome::Idempotent,
                retired.receipt.clone(),
            ))
        }
        TaskEntry::Live(live) => {
            if live.fingerprint != fingerprint || live.initial_domains != initial_keys {
                return Some(OperationReceipt::rejected(
                    operation,
                    OperationOutcome::CreateConflict,
                    "this identity already carries a different descriptor",
                ));
            }
            if let Some(failure) = &live.creation_failure {
                return Some(OperationReceipt::rejected(
                    operation,
                    failure.outcome,
                    failure.detail.clone(),
                ));
            }
            Some(OperationReceipt::acknowledged(
                operation,
                OperationOutcome::Idempotent,
                live.receipt.clone(),
            ))
        }
        TaskEntry::Gone => Some(OperationReceipt::rejected(
            operation,
            OperationOutcome::Gone,
            "this identity's retained record was reclaimed",
        )),
        TaskEntry::Creating(_) => None,
    }
}

fn live_task(
    state: &RegistryState,
    context: QueryContextRef,
    identity: TaskIdentity,
) -> Option<&LiveTask> {
    match state
        .contexts
        .get(&context)
        .and_then(|entry| entry.tasks.get(&identity))?
    {
        TaskEntry::Live(live) => Some(live),
        _ => None,
    }
}

fn live_task_mut(
    state: &mut RegistryState,
    context: QueryContextRef,
    identity: TaskIdentity,
) -> Option<&mut LiveTask> {
    match state
        .contexts
        .get_mut(&context)
        .and_then(|entry| entry.tasks.get_mut(&identity))?
    {
        TaskEntry::Live(live) => Some(live),
        _ => None,
    }
}

fn retired_task(
    state: &RegistryState,
    context: QueryContextRef,
    identity: TaskIdentity,
) -> Option<&RetiredTask> {
    match state
        .contexts
        .get(&context)
        .and_then(|entry| entry.tasks.get(&identity))?
    {
        TaskEntry::Retired(retired) => Some(retired),
        _ => None,
    }
}

fn classify_shared_domain(
    entry: &ContextEntry,
    domain: &QueryContextDomainUpdate,
) -> DomainProgression {
    entry.domains.classify(domain)
}

fn apply_shared_domain(entry: &mut ContextEntry, domain: &QueryContextDomainUpdate) {
    entry.domains.apply(domain);
}

fn shared_domain_receipt(
    entry: &ContextEntry,
    domain: &QueryContextDomainUpdate,
    progression: DomainProgression,
) -> QueryContextDomainReceipt {
    entry.domains.receipt(domain, progression)
}
