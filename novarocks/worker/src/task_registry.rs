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

//! The Worker-local owner of the task protocol.
//!
//! Everything the protocol calls a decision happens here, under one lock, at
//! one linearization point per operation: the establish creation gate, the
//! creation owner election, every domain token, the first-wins termination
//! latch, release readiness, and retirement. The execution side is reached
//! only through the context and execution ports it receives.
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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

use crate::{
    AdmissionTicketAcquisitionRejection, AdmissionTicketAuthority, AdmissionTicketObservation,
    AdmissionTicketProgression, AdmissionTicketRedemptionRejection, ContextOperationKind,
    ContextTransition, InstalledLease, LatchOutcome, LeaseProgression, MonotonicInstant,
    OperationAdmission, ProcessMonotonicClock, QueryContextDomains, QueryContextEvent,
    TaskCreationGate, TaskProtocolEvent, WorkerMonotonicClock, classify_context_transition,
    classify_operation_admission,
};
use novarocks_execution_contract::FragmentSinkKind;
use novarocks_execution_contract::operation::DomainFailureOutcome;
use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceReceipt, QueryContextConvergenceState, QueryContextConvergenceVersion,
};
use novarocks_execution_contract::task_execution::creation::TaskCreationInput;
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::DomainProgression;
use novarocks_execution_contract::task_execution::identity::{
    IdentityField, IdentityMismatch, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::{
    AbortQueryContext, AcquireQueryContextAdmissionTicket, AdvanceQueryContextDomain, CancelTask,
    CreateTask, CreateTaskReceipt, EstablishQueryContext, FetchTaskDynamicFilters,
    GetFinalTaskInfo, OperationEnvelope, OperationOutcome, QueryContextDomainReceipt,
    QueryContextDomainUpdate, QueryContextReceipt, QuiesceQueryContext, QuiesceQueryContextReceipt,
    ReleaseOutcome, ReleaseQueryContext, RenewQueryExecutionLease, UpdateQueryContext, UpdateTask,
    UpdateTaskReceipt,
};
use novarocks_execution_contract::task_execution::status::{
    AbortCause, TaskFailureCategory, TaskOutputFacts, TaskState, TerminationDetail,
};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_types::identity::QueryExecutionId;

use crate::lease_expiry_index::LeaseExpiryIndex;
use crate::task_registry_entry::{
    ContextEntry, CreationCell, CreationFailure, EstablishRecord, LiveTask, RetiredTask, TaskEntry,
    estimate_retained_bytes,
};
use crate::{
    AdmissionTicketOutcome, CancelTaskOutcome, CreateTaskOutcome, DynamicFilterReadOutcome,
    FinalTaskInfoOutcome, OperationReceipt, QueryContextHost, QueryContextOutcome,
    QuiesceQueryContextOutcome, ReleaseAcknowledgement, ReleaseQueryContextOutcome,
    ReleasedContextEvidence, RootResultBinding, RootResultRoute, RunnableTask, SharedFactsRequest,
    TaskDomains, TaskExecutionHost, TaskExecutionRegistryConfig, TaskStatusOwner,
    TaskStatusReporter, TaskStatusSource, UpdateTaskOutcome, WorkerAdmissionEpochAuthority,
    apply_planned_task_domain_updates, apply_task_domain_updates,
    commit_task_domain_execution_updates, plan_task_domain_execution_updates,
    validate_task_domain_execution_membership,
};

impl WorkerAdmissionEpochAuthority for TaskExecutionRegistry {
    fn admission_epoch_capability(&self) -> novarocks_execution_contract::AdmissionEpochCapability {
        self.admission_epoch_capability()
    }
}

impl crate::WorkerDeadlineAuthority for TaskExecutionRegistry {
    fn advance_deadlines(&self) {
        let _ = TaskExecutionRegistry::advance_deadlines(self);
    }
}

const REGISTRY_LOCK: &str = "task execution registry lock";

/// Lock-free measurements of the actual Worker registry mutex. A condition
/// wait is not counted as lock contention or lock hold time.
pub struct RegistryLockObservation {
    created_at: Instant,
    wait_samples: AtomicU64,
    wait_nanoseconds: AtomicU64,
    wait_max_nanoseconds: AtomicU64,
    hold_samples: AtomicU64,
    hold_nanoseconds: AtomicU64,
    hold_max_nanoseconds: AtomicU64,
    last_sample_nanoseconds: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegistryLockSnapshot {
    pub wait_samples: u64,
    pub wait_nanoseconds: u64,
    pub wait_max_nanoseconds: u64,
    pub hold_samples: u64,
    pub hold_nanoseconds: u64,
    pub hold_max_nanoseconds: u64,
    pub sample_age_nanoseconds: Option<u64>,
}

impl Default for RegistryLockObservation {
    fn default() -> Self {
        Self {
            created_at: Instant::now(),
            wait_samples: AtomicU64::new(0),
            wait_nanoseconds: AtomicU64::new(0),
            wait_max_nanoseconds: AtomicU64::new(0),
            hold_samples: AtomicU64::new(0),
            hold_nanoseconds: AtomicU64::new(0),
            hold_max_nanoseconds: AtomicU64::new(0),
            last_sample_nanoseconds: AtomicU64::new(0),
        }
    }
}

impl RegistryLockObservation {
    fn record_wait(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.wait_samples.fetch_add(1, Ordering::Relaxed);
        self.wait_nanoseconds.fetch_add(nanos, Ordering::Relaxed);
        self.wait_max_nanoseconds
            .fetch_max(nanos, Ordering::Relaxed);
        self.last_sample_nanoseconds.store(
            (self.created_at.elapsed().as_nanos().min(u64::MAX as u128) as u64).max(1),
            Ordering::Release,
        );
    }

    fn record_hold(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.hold_samples.fetch_add(1, Ordering::Relaxed);
        self.hold_nanoseconds.fetch_add(nanos, Ordering::Relaxed);
        self.hold_max_nanoseconds
            .fetch_max(nanos, Ordering::Relaxed);
        self.last_sample_nanoseconds.store(
            (self.created_at.elapsed().as_nanos().min(u64::MAX as u128) as u64).max(1),
            Ordering::Release,
        );
    }

    pub fn snapshot(&self) -> RegistryLockSnapshot {
        let last = self.last_sample_nanoseconds.load(Ordering::Acquire);
        RegistryLockSnapshot {
            wait_samples: self.wait_samples.load(Ordering::Relaxed),
            wait_nanoseconds: self.wait_nanoseconds.load(Ordering::Relaxed),
            wait_max_nanoseconds: self.wait_max_nanoseconds.load(Ordering::Relaxed),
            hold_samples: self.hold_samples.load(Ordering::Relaxed),
            hold_nanoseconds: self.hold_nanoseconds.load(Ordering::Relaxed),
            hold_max_nanoseconds: self.hold_max_nanoseconds.load(Ordering::Relaxed),
            sample_age_nanoseconds: (last != 0).then(|| {
                (self.created_at.elapsed().as_nanos().min(u64::MAX as u128) as u64)
                    .saturating_sub(last)
            }),
        }
    }
}

struct ObservedRegistryMutex<T> {
    inner: Mutex<T>,
    observation: Arc<RegistryLockObservation>,
}

impl<T> ObservedRegistryMutex<T> {
    fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(value),
            observation: Arc::new(RegistryLockObservation::default()),
        }
    }

    fn try_lock(
        &self,
    ) -> Result<Option<ObservedRegistryGuard<'_, T>>, TaskPreparationSnapshotError> {
        let started = Instant::now();
        let inner = match self.inner.try_lock() {
            Ok(inner) => inner,
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                return Err(TaskPreparationSnapshotError::Poisoned);
            }
        };
        // Only an actual acquisition contributes a wait/hold observation.
        self.observation.record_wait(started.elapsed());
        Ok(Some(ObservedRegistryGuard {
            inner: Some(inner),
            observation: &self.observation,
            acquired: Instant::now(),
        }))
    }

    fn lock(&self) -> Result<ObservedRegistryGuard<'_, T>, ()> {
        let started = Instant::now();
        let inner = self.inner.lock().map_err(|_| ())?;
        self.observation.record_wait(started.elapsed());
        Ok(ObservedRegistryGuard {
            inner: Some(inner),
            observation: &self.observation,
            acquired: Instant::now(),
        })
    }
}

struct ObservedRegistryGuard<'a, T> {
    inner: Option<MutexGuard<'a, T>>,
    observation: &'a RegistryLockObservation,
    acquired: Instant,
}

impl<'a, T> ObservedRegistryGuard<'a, T> {
    fn into_inner(mut self) -> MutexGuard<'a, T> {
        self.observation.record_hold(self.acquired.elapsed());
        self.inner.take().expect("observed registry guard is held")
    }

    fn from_condvar(inner: MutexGuard<'a, T>, observation: &'a RegistryLockObservation) -> Self {
        Self {
            inner: Some(inner),
            observation,
            acquired: Instant::now(),
        }
    }
}

impl<T> Deref for ObservedRegistryGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner
            .as_deref()
            .expect("observed registry guard is held")
    }
}

impl<T> DerefMut for ObservedRegistryGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner
            .as_deref_mut()
            .expect("observed registry guard is held")
    }
}

impl<T> Drop for ObservedRegistryGuard<'_, T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let held = self.acquired.elapsed();
            drop(inner);
            self.observation.record_hold(held);
        }
    }
}

/// How many times one settle drives termination before yielding.
///
/// A latch, its fan-out, retirement, and completion take one pass each; the
/// extra passes exist only for a fan-out that latches something new.
const MAX_SETTLE_PASSES: usize = 4;

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

/// One lock-consistent view of temporary preparation ownership.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskPreparationSnapshot {
    pub positions: usize,
    pub position_limit: usize,
    pub context_positions: usize,
    pub context_position_limit: usize,
    pub queued_positions: usize,
    pub workers: usize,
    pub worker_limit: usize,
    pub bytes: usize,
    pub byte_limit: usize,
}

/// Failure to observe preparation ownership without waiting for its mutex.
/// A busy mutex is reported as an unavailable snapshot, never as this error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPreparationSnapshotError {
    /// The actual registry mutex was poisoned; no trustworthy snapshot exists.
    Poisoned,
}

impl std::fmt::Display for TaskPreparationSnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Poisoned => {
                formatter.write_str("task preparation snapshot registry mutex poisoned")
            }
        }
    }
}

impl std::error::Error for TaskPreparationSnapshotError {}

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
    lease_expiry: LeaseExpiryIndex,
    context_by_execution: BTreeMap<QueryExecutionId, QueryContextRef>,
    task_index: BTreeMap<TaskIdentity, QueryContextRef>,
    in_flight: BTreeMap<QueryContextRef, InFlight>,
    pending_termination: BTreeSet<QueryContextRef>,
    pending_quiesce: BTreeSet<QueryContextRef>,
    released_root_owners: Vec<ContextRootOwners>,
    // Effects only: conclusions are already accepted in their Task owner.
    pending_task_publications: Vec<crate::status::OwnedTaskConclusion>,
    retired_task_order: VecDeque<(QueryContextRef, TaskIdentity)>,
    gone_task_order: VecDeque<(QueryContextRef, TaskIdentity)>,
    retired_context_order: VecDeque<QueryContextRef>,
    gone_context_order: VecDeque<QueryContextRef>,
    active_tasks: usize,
    prepare_queue: VecDeque<QueuedPreparation>,
    preparing_contexts: BTreeSet<QueryContextRef>,
    prepare_workers: usize,
    prepare_charges: BTreeMap<TaskIdentity, usize>,
    pending_preparation_exits: BTreeMap<TaskIdentity, PendingPreparationExit>,
    prepare_context_counts: BTreeMap<QueryContextRef, usize>,
    prepare_bytes: usize,
    retained_tasks: usize,
    retained_bytes: usize,
}
type ContextRootOwners = BTreeMap<TaskIdentity, Arc<crate::root_result_channel::RootResultChannel>>;

#[derive(Clone)]
struct PendingPreparationExit {
    context: QueryContextRef,
    cell: Arc<CreationCell>,
    // Async records retain an existing P charge. Sync records remain bounded
    // by their original active Task entry and never invent a synthetic P.
    charged: bool,
}

struct QueuedPreparation {
    request: CreateTask,
    input: TaskCreationInput,
    cell: Arc<CreationCell>,
    source: Arc<TaskStatusSource>,
}

struct PreparationPanicked;
impl std::fmt::Debug for PreparationPanicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparationPanicked")
    }
}
impl std::fmt::Display for PreparationPanicked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Task preparation panicked")
    }
}
impl std::error::Error for PreparationPanicked {}
impl novarocks_execution_contract::failure::FailureProjection for PreparationPanicked {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Unexpected,
            crate::failure_reason::WorkerFailureReason::PreparationPanicked.reason(),
        )
    }
}

#[derive(Debug)]
struct PreparationWorkerStartFailure(std::io::Error);
impl std::fmt::Display for PreparationWorkerStartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot start Task preparation worker: {}", self.0)
    }
}
impl std::error::Error for PreparationWorkerStartFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for PreparationWorkerStartFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let class = match self.0.kind() {
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::OutOfMemory => {
                FailureClass::Resource
            }
            _ => FailureClass::Unexpected,
        };
        FailureFact::new(
            class,
            crate::failure_reason::WorkerFailureReason::PreparationWorkerStart.reason(),
        )
    }
}

struct PreparationWorkerCompletion {
    registry: Arc<TaskExecutionRegistry>,
    context: QueryContextRef,
    identity: TaskIdentity,
    cell: Arc<CreationCell>,
}

impl Drop for PreparationWorkerCompletion {
    fn drop(&mut self) {
        self.registry
            .complete_preparation_worker(self.context, self.identity, &self.cell);
    }
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

/// The Worker-local owner of query contexts, tasks, status, and retention.
pub struct TaskExecutionRegistry {
    config: TaskExecutionRegistryConfig,
    clock: Arc<dyn WorkerMonotonicClock>,
    context_host: Arc<dyn QueryContextHost>,
    task_host: Arc<dyn TaskExecutionHost>,
    ports: crate::TaskExecutionPorts,
    task_creation_gate: Arc<dyn TaskCreationGate>,
    admission_tickets: AdmissionTicketAuthority,
    state: ObservedRegistryMutex<RegistryState>,
    gate: Condvar,
    counters: AtomicCounters,
    self_weak: std::sync::Weak<Self>,
}

impl TaskExecutionRegistry {
    /// Bounded, nonblocking census of existing context-owned roots. Each
    /// context and channel costs one scan position, including empty retained
    /// contexts. Exceeding the observation bound invalidates the whole scrape;
    /// it changes no admission, lifecycle or configured ownership limit.
    /// Physical counters are independent atomic samples within each channel.
    pub fn try_root_ownership_snapshot(
        &self,
    ) -> Result<
        Option<crate::root_result_channel::RootOwnershipSnapshot>,
        crate::root_result_channel::RootOwnershipSnapshotError,
    > {
        use crate::root_result_channel::{RootOwnershipSnapshot, RootOwnershipSnapshotError};
        const SCAN_POSITIONS: usize = 1024;
        let Some(state) = self
            .state
            .try_lock()
            .map_err(|_| RootOwnershipSnapshotError::RegistryPoisoned)?
        else {
            return Ok(None);
        };
        let mut remaining = SCAN_POSITIONS;
        let mut snapshot = RootOwnershipSnapshot::default();
        for entry in state.contexts.values() {
            let Some(next) = remaining.checked_sub(1) else {
                return Ok(None);
            };
            remaining = next;
            for (identity, root) in &entry.roots {
                let Some(next) = remaining.checked_sub(1) else {
                    return Ok(None);
                };
                remaining = next;
                let Some(mut channel) = root.try_ownership_snapshot()? else {
                    return Ok(None);
                };
                channel.terminal_task_records = usize::from(
                    entry
                        .tasks
                        .get(identity)
                        .is_some_and(TaskEntry::is_terminal_record),
                );
                if snapshot.try_add(channel).is_none() {
                    return Ok(None);
                }
            }
        }
        // Released roots have already passed physical_idle under this fence;
        // they contain no producer, segment, read/send or metadata subreservation.
        // Their fixed core backing and any unrelated Arc tail can still live
        // after removal. A zero census is not their last-owner exit receipt.
        Ok(Some(snapshot))
    }

    /// Reports the existing charge ledger, including canceled jobs until their exit.
    pub fn preparation_snapshot(&self) -> TaskPreparationSnapshot {
        let state = self.state.lock().expect(REGISTRY_LOCK);
        self.project_preparation_snapshot(&state)
    }

    /// Observe the same lock-consistent preparation ledger without waiting.
    /// `Ok(None)` means the actual mutex is busy: callers must not substitute
    /// zero ownership or republish a stale snapshot as a fresh measurement.
    /// Poisoning remains a distinct error. Failed attempts do not contribute
    /// wait/hold samples to the actual registry lock observation.
    pub fn try_preparation_snapshot(
        &self,
    ) -> Result<Option<TaskPreparationSnapshot>, TaskPreparationSnapshotError> {
        let Some(state) = self.state.try_lock()? else {
            return Ok(None);
        };
        Ok(Some(self.project_preparation_snapshot(&state)))
    }

    fn project_preparation_snapshot(&self, state: &RegistryState) -> TaskPreparationSnapshot {
        TaskPreparationSnapshot {
            positions: state.prepare_charges.len(),
            position_limit: self.config.max_preparing_tasks,
            context_positions: state
                .prepare_context_counts
                .values()
                .copied()
                .max()
                .unwrap_or(0),
            context_position_limit: self.config.max_preparing_tasks_per_context,
            queued_positions: state.prepare_queue.len(),
            workers: state.prepare_workers,
            worker_limit: self.config.max_prepare_workers,
            bytes: state.prepare_bytes,
            byte_limit: self.config.max_preparing_bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_preparation_charge(&self, identity: TaskIdentity) -> bool {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .prepare_charges
            .contains_key(&identity)
    }

    pub fn new(
        config: TaskExecutionRegistryConfig,
        clock: Arc<dyn WorkerMonotonicClock>,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
        ports: crate::TaskExecutionPorts,
    ) -> Arc<Self> {
        Self::new_with_task_creation_gate(
            config,
            clock,
            context_host,
            task_host,
            ports,
            Arc::new(crate::NoopTaskCreationGate),
        )
    }

    /// Builds a Worker registry with one outer adapter gate for create admission.
    pub fn new_with_task_creation_gate(
        config: TaskExecutionRegistryConfig,
        clock: Arc<dyn WorkerMonotonicClock>,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
        ports: crate::TaskExecutionPorts,
        task_creation_gate: Arc<dyn TaskCreationGate>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            self_weak: self_weak.clone(),
            config,
            clock,
            context_host,
            task_host,
            ports,
            task_creation_gate,
            admission_tickets: AdmissionTicketAuthority::new(config.admission_tickets),
            state: ObservedRegistryMutex::new(RegistryState::default()),
            gate: Condvar::new(),
            counters: AtomicCounters::default(),
        })
    }

    /// The owner a deployment builds: the process monotonic clock.
    pub fn with_process_clock(
        config: TaskExecutionRegistryConfig,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
        ports: crate::TaskExecutionPorts,
    ) -> Arc<Self> {
        Self::new(
            config,
            Arc::new(ProcessMonotonicClock::new()),
            context_host,
            task_host,
            ports,
        )
    }

    /// Builds the production Worker owner with its process clock and adapter gate.
    pub fn with_process_clock_and_task_creation_gate(
        config: TaskExecutionRegistryConfig,
        context_host: Arc<dyn QueryContextHost>,
        task_host: Arc<dyn TaskExecutionHost>,
        ports: crate::TaskExecutionPorts,
        task_creation_gate: Arc<dyn TaskCreationGate>,
    ) -> Arc<Self> {
        Self::new_with_task_creation_gate(
            config,
            Arc::new(ProcessMonotonicClock::new()),
            context_host,
            task_host,
            ports,
            task_creation_gate,
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

    pub fn registry_lock_observation(&self) -> Arc<RegistryLockObservation> {
        Arc::clone(&self.state.observation)
    }

    /// Test-only rendezvous on the actual registry mutex. The callback owns
    /// no Worker state and cannot alter a task or context verdict.
    #[cfg(novarocks_dev_controls)]
    pub fn with_registry_lock_for_test<T>(&self, callback: impl FnOnce() -> T) -> T {
        let _state = self.state.lock().expect(REGISTRY_LOCK);
        callback()
    }

    pub fn context_state(&self, context: QueryContextRef) -> QueryContextState {
        self.state
            .lock()
            .expect(REGISTRY_LOCK)
            .context_state(context)
    }

    /// Loan the original live Task's acceptance capability for a finite Native
    /// test action. Retired Context result channels never mint new Task rights.
    #[cfg(novarocks_dev_controls)]
    pub fn live_task_execution_control_for_test(
        &self,
        identity: TaskIdentity,
    ) -> Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>> {
        let owner = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            state
                .contexts
                .values()
                .find_map(|context| match context.tasks.get(&identity) {
                    Some(TaskEntry::Live(task)) => Some(Arc::clone(&task.status)),
                    _ => None,
                })
        }?;
        Some(owner.execution_control(novarocks_execution_contract::TaskFailurePhase::Execution))
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

    #[cfg(test)]
    pub(crate) fn indexed_lease_count(&self) -> usize {
        self.state.lock().expect(REGISTRY_LOCK).lease_expiry.len()
    }

    /// Whether one task identity is currently findable as a live task.
    /// The execution kernel's key for one live task on this process.
    /// Resolves one root result poll against this process's task set.
    ///
    /// Three facts are checked here and nowhere else: the identity addresses a
    /// task of this exact process, that task is live, and the sink its
    /// creation winner validated is the query's result sink. The last one is
    /// why routing by buffer key alone is not enough: every task has a buffer
    /// key, but only the result owner owes the coordinator a result, and
    /// answering a poll out of any other task's buffer would hand back an
    /// exchange producer's output as if it were the query's answer.
    pub fn root_result_route(&self, identity: TaskIdentity) -> RootResultRoute {
        if identity.backend_process_id() != self.config.backend_process_id {
            return RootResultRoute::UnknownTask;
        }
        let state = self.state.lock().expect(REGISTRY_LOCK);
        let Some(context) = state.task_index.get(&identity).copied() else {
            return RootResultRoute::UnknownTask;
        };
        let awaiting_terminal_control = state.contexts.get(&context).is_some_and(|entry| {
            matches!(
                entry.state,
                QueryContextState::Aborting | QueryContextState::TerminalRetained
            ) && entry.latch.cause().is_some()
        });
        match self.locate_task_locked(&state, context, identity) {
            TaskLocation::Live => {
                let live = live_task(&state, context, identity).expect("located live task");
                if live.prepared.sink_kind() != FragmentSinkKind::Result {
                    return RootResultRoute::NotResultOwner;
                }
                if awaiting_terminal_control {
                    return RootResultRoute::AwaitTerminalControl;
                }
                RootResultRoute::Serve(RootResultBinding::new(
                    live.descriptor.fragment_instance_id(),
                    Arc::clone(&live.status),
                ))
            }
            TaskLocation::Creating => RootResultRoute::Creating,
            TaskLocation::Retired => {
                let retired = retired_task(&state, context, identity).expect("retired task");
                if retired.result_owner && awaiting_terminal_control {
                    RootResultRoute::AwaitTerminalControl
                } else if retired.result_owner {
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

    /// Resolve an exact root from its original context, independent of the
    /// task record/tombstone horizon. Read admission is minted under this
    /// fence, so release cannot race between lookup and handler entry.
    pub fn context_root_result_route(
        &self,
        read: &novarocks_execution_contract::root_result::RootResultRead,
    ) -> crate::root_result_channel::ContextRootRoute {
        use crate::root_result_channel::{ContextRootRoute as Route, RootChannelError};
        let identity = read.root_task();
        if identity.backend_process_id() != self.config.backend_process_id {
            return Route::Mismatch;
        }
        let state = self.state.lock().expect(REGISTRY_LOCK);
        let Some(context) = state
            .context_by_execution
            .get(&identity.query_execution_id())
        else {
            return Route::UnknownRoot;
        };
        let Some(entry) = state.contexts.get(context) else {
            return Route::UnknownRoot;
        };
        let Some(root) = entry.roots.get(&identity) else {
            return if matches!(entry.tasks.get(&identity), Some(TaskEntry::Creating(_))) {
                Route::Preparing
            } else {
                Route::UnknownRoot
            };
        };
        if root.spec().task != identity
            || root.spec().contract.profile() != read.profile()
            || root.spec().contract.kind() != read.kind()
        {
            return Route::Mismatch;
        }
        if !matches!(
            entry.state,
            QueryContextState::Active | QueryContextState::Quiescing
        ) {
            return Route::AwaitTerminalControl {
                accepted_consumed: root.snapshot().consumed_through,
            };
        }
        match root.begin_read(read) {
            Ok(admitted) => Route::Read(admitted),
            Err(RootChannelError::Capacity(cause)) => Route::Busy(cause.resource_facts()),
            Err(RootChannelError::Closed) => Route::AwaitTerminalControl {
                accepted_consumed: root.snapshot().consumed_through,
            },
            Err(cause) => Route::Refused(cause),
        }
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

    /// A lock-free projection for management readout. The ticket authority's
    /// mutex remains the admission decision source.
    pub fn admission_reservation_observation(&self) -> Arc<crate::AdmissionReservationObservation> {
        self.admission_tickets.reservation_observation()
    }

    /// Reads a ticket's remaining lifetime for ingress wait budgeting.
    /// Final establish admission still redeems through the Worker owner.
    pub fn observe_admission_ticket(
        &self,
        ticket_id: novarocks_execution_contract::AdmissionTicketId,
        context: QueryContextRef,
    ) -> AdmissionTicketObservation {
        self.admission_tickets
            .observe(ticket_id, context, self.clock.now())
    }

    /// Returns an early expired-ticket refusal only when the exact Establish
    /// would reach ticket redemption as its next Worker decision. Every other
    /// state falls through to the normal fully decoded request path.
    pub fn preflight_expired_establish_ticket(
        &self,
        operation: TaskOperationId,
        context: QueryContextRef,
        ticket_id: novarocks_execution_contract::AdmissionTicketId,
    ) -> Option<QueryContextOutcome> {
        if context.backend_process_id() != self.config.backend_process_id {
            return None;
        }
        let now = self.clock.now();
        let mut state = self.state.lock().expect(REGISTRY_LOCK);
        self.expire_leases_locked(&mut state, now);
        if fence_frontend(&state, context).is_err()
            || state.context_state(context) != QueryContextState::Absent
        {
            return None;
        }
        self.admission_tickets
            .confirm_expired_for_context(ticket_id, context, now)
            .then(|| {
                OperationReceipt::rejected(
                    operation,
                    OperationOutcome::InvalidStateOrRequest,
                    "establish names an expired admission ticket",
                )
            })
    }

    // ---------------------------------------------------------------- create

    /// Creates one task, atomically or not at all.
    ///
    /// The create is keyed by its exact task identity under its exact query
    /// context, and nothing else. When that identity is new in an admissible
    /// context this request wins the creation and `input` is the body the
    /// task is prepared from. When the identity already exists the request is
    /// answered from that task's lifecycle -- the original receipt, the fixed
    /// creation failure, or the terminal outcome -- and `input` is dropped
    /// unread: a repeated body is never compared, never interpreted, and never
    /// applied.
    pub fn create_task(&self, request: &CreateTask, input: TaskCreationInput) -> CreateTaskOutcome {
        self.create_task_inner(request, input, None)
    }

    /// Takes ownership of one task before its expensive preparation begins.
    /// The returned status describes admission only; installation is observed
    /// through this task's status stream.
    pub fn accept_create_task(
        self: &Arc<Self>,
        request: &CreateTask,
        input: TaskCreationInput,
    ) -> OperationReceipt<novarocks_execution_contract::TaskStatus> {
        self.accept_create_task_with_retained_bytes(request, input, 0)
    }

    /// `retained_bytes` is the adapter's exact bounded incoming Create item
    /// size. The Worker also charges its neutral in-memory structural floor.
    pub fn accept_create_task_with_retained_bytes(
        self: &Arc<Self>,
        request: &CreateTask,
        input: TaskCreationInput,
        retained_bytes: usize,
    ) -> OperationReceipt<novarocks_execution_contract::TaskStatus> {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        let context = request.context();
        if let Err(mismatch) = identity.verify_query_context(context) {
            return map_create_status(identity_mismatch(operation, mismatch));
        }
        if identity.backend_process_id() != self.config.backend_process_id {
            return map_create_status(identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            ));
        }
        let _scope = OperationScope::enter(self, context, Lane::Mutation);
        let deadline = self.deadline_of(request.envelope(), None);
        let (cell, source) = match self
            .elect_creation_owner(context, identity, operation, deadline, true)
        {
            Ok(reservation) => reservation,
            Err(outcome) => {
                let receipt = self.map_create_status_current(identity, *outcome);
                if let Some(status) = receipt.acknowledgement() {
                    let event_receipt = OperationReceipt::acknowledged(
                        operation,
                        receipt.outcome(),
                        CreateTaskReceipt::new(identity, Vec::new(), status.clone()),
                    );
                    if let Some(event) = TaskProtocolEvent::create_task(identity, &event_receipt) {
                        self.ports.observe(event);
                    }
                }
                return receipt;
            }
        };
        cell.preparation_scope().begin_admission();
        cell.preparation_scope().install_input(input);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.accept_reserved_create_task(request, Arc::clone(&cell), source, retained_bytes)
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                let failure = self.preparation_panicked(
                    &cell,
                    crate::task_preparation::PreparationUnwindRole::Admission,
                    payload,
                );
                if let Some(status) = cell.accepted_status() {
                    OperationReceipt::acknowledged(
                        operation,
                        OperationOutcome::Accepted,
                        status.current(),
                    )
                } else {
                    failure.receipt(operation)
                }
            }
        };
        cell.preparation_scope().admission_exited();
        if cell.preparation_scope().body_handed_off() {
            // Even a post-enqueue publication unwind cannot strand accepted work.
            self.launch_preparations();
            self.recheck_preparation_exits();
        } else {
            if let Err(payload) = cell.preparation_scope().dispose_input() {
                self.preparation_panicked(
                    &cell,
                    crate::task_preparation::PreparationUnwindRole::Input,
                    payload,
                );
            }
            self.finish_unaccepted_reservation(context, identity, &cell);
        }
        result
    }

    fn accept_reserved_create_task(
        self: &Arc<Self>,
        request: &CreateTask,
        cell: Arc<CreationCell>,
        source: Arc<TaskStatusSource>,
        retained_bytes: usize,
    ) -> OperationReceipt<novarocks_execution_contract::TaskStatus> {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        let context = request.context();
        // The incoming item bound covers codec-owned domain slices that can
        // still retain its independently isolated metadata backing. Assignment
        // storage is separate, so it must be added rather than hidden by max.
        let bytes = cell
            .preparation_scope()
            .input_retained_bytes()
            .saturating_add(retained_bytes)
            .saturating_add(create_request_structural_bytes(request));
        let status = Arc::new(TaskStatusOwner::from_preparation_scope(
            identity,
            Arc::clone(&source),
            Arc::clone(&self.clock),
            self.config.metric_publish_min_interval,
            self.ports.execution_health(),
            cell.preparation_scope_arc(),
        ));
        {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let rejection = if state.context_state(context) != QueryContextState::Active {
                Some((
                    OperationOutcome::ContextTerminalReceipt,
                    TaskFailureCategory::Protocol,
                    "the query context closed before task admission",
                ))
            } else if bytes > self.config.max_preparing_bytes {
                Some((
                    OperationOutcome::ResourceExhausted,
                    TaskFailureCategory::ResourceExhausted,
                    "one task exceeds the accepted preparation byte bound",
                ))
            } else if state
                .prepare_context_counts
                .get(&context)
                .copied()
                .unwrap_or(0)
                >= self.config.max_preparing_tasks_per_context
            {
                Some((
                    OperationOutcome::PreparationBusy,
                    TaskFailureCategory::ResourceExhausted,
                    "query context reached its accepted preparation task bound",
                ))
            } else if state.prepare_charges.len() >= self.config.max_preparing_tasks {
                Some((
                    OperationOutcome::PreparationBusy,
                    TaskFailureCategory::ResourceExhausted,
                    "backend reached its accepted preparation task bound",
                ))
            } else if bytes
                > self
                    .config
                    .max_preparing_bytes
                    .saturating_sub(state.prepare_bytes)
            {
                Some((
                    OperationOutcome::PreparationBusy,
                    TaskFailureCategory::ResourceExhausted,
                    "backend reached its accepted preparation byte bound",
                ))
            } else {
                None
            };
            if let Some((outcome, _category, detail)) = rejection {
                cell.fail(CreationFailure::verdict(outcome, detail));
                self.gate.notify_all();
                return OperationReceipt::rejected(operation, outcome, detail);
            }
            if let Err(rejection) = self
                .task_host
                .reserve_inbound_close_capacity(request.descriptor())
            {
                let domain_outcome = match rejection.category() {
                    TaskFailureCategory::ResourceExhausted => {
                        DomainFailureOutcome::ResourceExhausted
                    }
                    _ => DomainFailureOutcome::InvalidStateOrRequest,
                };
                let failure =
                    CreationFailure::domain(domain_outcome, rejection.operation_failure());
                cell.fail(failure.clone());
                self.gate.notify_all();
                return failure.receipt(operation);
            }
            // This lock is the Accepted/Context-fence linearization point.
            // The queue and both hard charges become owned by the Task before
            // the first acknowledgement can leave the Worker.
            cell.accept(Arc::clone(&status));
            state
                .contexts
                .get_mut(&context)
                .expect("reserved context")
                .mark_spent(identity);
            state.prepare_bytes += bytes;
            state.prepare_charges.insert(identity, bytes);
            *state.prepare_context_counts.entry(context).or_default() += 1;
            state.prepare_queue.push_back(QueuedPreparation {
                request: request.clone(),
                input: cell.preparation_scope().take_input(),
                cell: Arc::clone(&cell),
                source,
            });
            cell.preparation_scope().hand_off_body();
            self.gate.notify_all();
        }
        // The original queue/charges are already installed atomically. Only
        // notifications execute here, outside the Registry acceptance fence.
        status.release_to_observers();
        self.launch_preparations();
        let result =
            OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, status.current());
        let event_receipt = OperationReceipt::acknowledged(
            operation,
            OperationOutcome::Accepted,
            CreateTaskReceipt::new(identity, Vec::new(), status.current()),
        );
        if let Some(event) = TaskProtocolEvent::create_task(identity, &event_receipt) {
            self.ports.observe(event);
        }
        result
    }

    fn map_create_status_current(
        &self,
        identity: TaskIdentity,
        receipt: CreateTaskOutcome,
    ) -> OperationReceipt<novarocks_execution_contract::TaskStatus> {
        if receipt.acknowledgement().is_none() {
            return map_create_status(receipt);
        }
        let status = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            state
                .task_index
                .get(&identity)
                .and_then(|context| state.contexts.get(context))
                .and_then(|entry| entry.tasks.get(&identity))
                .and_then(|task| match task {
                    TaskEntry::Creating(cell) => {
                        cell.accepted_status().map(|status| status.current())
                    }
                    TaskEntry::Live(live) => Some(live.status.current()),
                    TaskEntry::Retired(retired) => Some(retired.status.clone()),
                    TaskEntry::Gone => None,
                })
        };
        match status {
            Some(status) => {
                OperationReceipt::acknowledged(receipt.operation_id(), receipt.outcome(), status)
            }
            None => map_create_status(receipt),
        }
    }

    fn launch_preparations(self: &Arc<Self>) {
        loop {
            let job = {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                if state.prepare_workers >= self.config.max_prepare_workers {
                    return;
                }
                let Some(index) = state
                    .prepare_queue
                    .iter()
                    .position(|job| !state.preparing_contexts.contains(&job.request.context()))
                else {
                    return;
                };
                let job = state
                    .prepare_queue
                    .remove(index)
                    .expect("selected queue job");
                state.preparing_contexts.insert(job.request.context());
                state.prepare_workers += 1;
                job
            };
            let context = job.request.context();
            let identity = job.request.identity();
            let cell = Arc::clone(&job.cell);
            let runner_cell = Arc::clone(&cell);
            let holder = Arc::new(Mutex::new(Some(job)));
            let runner_holder = Arc::clone(&holder);
            let registry = Arc::clone(self);
            let started = std::thread::Builder::new()
                .name("novarocks-task-prepare".to_owned())
                .spawn(move || {
                    let _completion = PreparationWorkerCompletion {
                        registry: Arc::clone(&registry),
                        context,
                        identity,
                        cell: runner_cell,
                    };
                    let job = runner_holder
                        .lock()
                        .expect("preparation job lock")
                        .take()
                        .expect("a launched worker owns one preparation job");
                    registry.run_accepted_preparation(job);
                });
            if let Err(error) = started {
                let job = holder
                    .lock()
                    .expect("preparation job lock")
                    .take()
                    .expect("a rejected worker retains its preparation job");
                self.fail_accepted_preparation(job, error);
                self.settle();
                self.complete_preparation_worker(context, identity, &cell);
            }
        }
    }

    fn run_accepted_preparation(&self, job: QueuedPreparation) {
        let cell = Arc::clone(&job.cell);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let QueuedPreparation {
                request,
                input,
                cell,
                source,
            } = job;
            let outcome = self.prepare_reserved_create_task(&request, input, cell, source);
            drop(outcome);
            drop(request);
            self.settle();
        }));
        if let Err(payload) = result {
            self.preparation_panicked(
                &cell,
                crate::task_preparation::PreparationUnwindRole::Tail,
                payload,
            );
        }
    }

    fn fail_accepted_preparation(&self, job: QueuedPreparation, error: std::io::Error) {
        let QueuedPreparation {
            request,
            input,
            cell,
            source,
        } = job;
        cell.preparation_scope().install_input(input);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut transaction = CreationTransaction {
                registry: self,
                context: request.context(),
                identity: request.identity(),
                descriptor: Arc::new(request.descriptor().clone()),
                cell: Arc::clone(&cell),
                receiver_installed: false,
                capability_installed: false,
                failure: None,
                committed: false,
            };
            use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
            let cause = PreparationWorkerStartFailure(error);
            let (outcome, category) = if cause.failure_fact().class() == FailureClass::Resource {
                (
                    DomainFailureOutcome::ResourceExhausted,
                    TaskFailureCategory::ResourceExhausted,
                )
            } else {
                (
                    DomainFailureOutcome::InvalidStateOrRequest,
                    TaskFailureCategory::Internal,
                )
            };
            let _ = transaction.abandon_host(
                request.envelope().operation_id(),
                outcome,
                crate::HostRejection::from_domain(category, cause),
            );
            drop(transaction);
            drop(request);
            drop(source);
        }));
        if let Err(payload) = outcome {
            self.preparation_panicked(
                &cell,
                crate::task_preparation::PreparationUnwindRole::Tail,
                payload,
            );
        }
        if let Err(payload) = cell.preparation_scope().dispose_input() {
            self.preparation_panicked(
                &cell,
                crate::task_preparation::PreparationUnwindRole::Input,
                payload,
            );
        }
    }

    fn complete_preparation_worker(
        self: &Arc<Self>,
        context: QueryContextRef,
        identity: TaskIdentity,
        cell: &Arc<CreationCell>,
    ) {
        cell.preparation_scope().body_exited();
        // The normal child-ready path finishes ordered callbacks before
        // releasing the physical worker, preserving its real tail gate.
        let ready = self.finish_preparation_exit(context, identity, cell);
        let claimed = ready && cell.preparation_scope().claim_settlement();
        {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            state.preparing_contexts.remove(&context);
            state.prepare_workers -= 1;
            if claimed {
                Self::return_preparation_charge(&mut state, context, identity);
            } else if !ready {
                assert!(state.prepare_charges.contains_key(&identity));
                state.pending_preparation_exits.insert(
                    identity,
                    PendingPreparationExit {
                        context,
                        cell: Arc::clone(cell),
                        charged: true,
                    },
                );
            }
        }
        self.settle();
        self.launch_preparations();
    }

    fn return_preparation_charge(
        state: &mut RegistryState,
        context: QueryContextRef,
        identity: TaskIdentity,
    ) {
        let bytes = state
            .prepare_charges
            .remove(&identity)
            .expect("exiting preparation owns its charge");
        state.prepare_bytes -= bytes;
        let count = state
            .prepare_context_counts
            .get_mut(&context)
            .expect("exiting preparation owns its context position");
        *count -= 1;
        if *count == 0 {
            state.prepare_context_counts.remove(&context);
        }
    }

    fn recheck_preparation_exits(&self) {
        let pending: Vec<_> = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            state
                .pending_preparation_exits
                .iter()
                .map(|(identity, entry)| (*identity, entry.clone()))
                .collect()
        };
        let mut progressed = false;
        for (identity, entry) in pending {
            if !self.finish_preparation_exit(entry.context, identity, &entry.cell)
                || !entry.cell.preparation_scope().claim_settlement()
            {
                continue;
            }
            let removed = {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                let removed = state.pending_preparation_exits.remove(&identity);
                if entry.charged {
                    Self::return_preparation_charge(&mut state, entry.context, identity);
                }
                removed
            };
            // CreationCell/Task/source destruction is never under Registry lock.
            drop(removed);
            progressed = true;
        }
        if progressed && let Some(registry) = self.self_weak.upgrade() {
            registry.launch_preparations();
        }
    }

    /// This is called only after the owned preparation body, request and
    /// temporaries exited. Installed runtime work starts after that handover.
    fn finish_preparation_exit(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
        cell: &Arc<CreationCell>,
    ) -> bool {
        use novarocks_execution_contract::{TaskConclusion, TaskConclusionPort};
        let scope = cell.preparation_scope();
        // Do not claim even a no-op stand-down phase while an original body
        // can still author a failure before its own real tail exit.
        if !scope.physical_bodies_exited() {
            return false;
        }
        let Some(status) = cell.accepted_status() else {
            // No Accepted Task exists. Keep the original Creating position
            // until every body/input/opaque/child obligation actually exited.
            if !cell.rollback_complete() || !scope.rollback_exited() {
                return false;
            }
            return scope.finish(|| self.finish_preparation_record(context, identity, cell, None));
        };
        if cell.rollback_complete() {
            if !scope.rollback_exited() {
                return false;
            }
            return scope.finish(|| {
                let accepted = status
                    .accepted()
                    .expect("preparation source/control was accepted before rollback");
                let TaskConclusion::Terminated(detail) = accepted.neutral().conclusion() else {
                    panic!("an unfinished preparation cannot already have succeeded");
                };
                status.conclude_termination(detail.clone(), TaskOutputFacts::default());
                let reporter = TaskStatusReporter::new(status.clone());
                reporter.release_output();
                reporter.note_actual_stopped();
                reporter.note_resources_converged();
                status
                    .retire()
                    .expect("actual failed preparation exit satisfies retirement");
                self.finish_preparation_record(context, identity, cell, Some(status.clone()));
            });
        }
        let installed = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            state
                .contexts
                .get(&context)
                .and_then(|entry| entry.tasks.get(&identity))
                .and_then(|task| match task {
                    TaskEntry::Live(live) => Some((live.status.clone(), live.runnable.clone())),
                    _ => None,
                })
        };
        let Some((status, runnable)) = installed else {
            return scope.complete_empty_after_actual_runtime_exit(
                status.runtime_actual_stopped_received(),
                || status.refresh_preparation_exit_receipts(),
            );
        };
        if !scope.stand_down(|| {
            if let Some(accepted) = status.accepted() {
                match accepted.neutral().conclusion() {
                    TaskConclusion::Terminated(TerminationDetail::Canceled(reason)) => {
                        runnable.cancel(*reason)
                    }
                    TaskConclusion::Terminated(TerminationDetail::Aborted(cause)) => {
                        runnable.abort(*cause)
                    }
                    TaskConclusion::Terminated(TerminationDetail::Failed(_)) => {
                        runnable.abort(AbortCause::QueryFailed)
                    }
                    TaskConclusion::Succeeded => {
                        panic!("a dormant prepared runtime cannot already have succeeded")
                    }
                }
            }
        }) {
            return false;
        }
        if !scope.adopt_runtime() {
            return false;
        }
        scope.start_runtime(|| {
            runnable.commit_creation();
            status.refresh_preparation_exit_receipts();
        })
    }

    fn finish_preparation_record(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
        cell: &Arc<CreationCell>,
        accepted: Option<Arc<TaskStatusOwner>>,
    ) {
        let mut terminal_retained = None;
        {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let removed = matches!(
                state
                    .contexts
                    .get(&context)
                    .and_then(|entry| entry.tasks.get(&identity)),
                Some(TaskEntry::Creating(current_cell)) if Arc::ptr_eq(current_cell, cell)
            );
            if removed {
                if let Some(entry) = state.contexts.get_mut(&context) {
                    if let Some(status) = &accepted {
                        let current = status.current();
                        let final_info = status.final_info();
                        let receipt = CreateTaskReceipt::new(identity, Vec::new(), current.clone());
                        let bytes =
                            estimate_retained_bytes(&receipt, &current, final_info.as_ref());
                        entry.tasks.insert(
                            identity,
                            TaskEntry::Retired(Box::new(RetiredTask {
                                receipt,
                                creation_failure: None,
                                status: current,
                                final_info,
                                result_owner: false,
                                retired_at: self.clock.now(),
                                bytes,
                            })),
                        );
                        state.retained_tasks += 1;
                        state.retained_bytes += bytes;
                        state.retired_task_order.push_back((context, identity));
                        terminal_retained = Some((bytes, status.current().state()));
                    } else {
                        entry.tasks.remove(&identity);
                    }
                }
                if accepted.is_none() {
                    state.task_index.remove(&identity);
                }
                state.active_tasks = state.active_tasks.saturating_sub(1);
            }
        }
        if let Some((bytes, state)) = terminal_retained {
            self.ports.retire_task_result(identity);
            self.ports
                .observe(TaskProtocolEvent::task_terminal_retained(
                    identity, state, bytes,
                ));
        } else {
            self.counters
                .creations_rolled_back
                .fetch_add(1, Ordering::Relaxed);
        }
        self.gate.notify_all();
    }

    /// Applies the same exact create with a shorter local ingress wait budget.
    /// This cap is transport-local and never enters create identity or replay.
    pub fn create_task_with_local_wait_cap(
        &self,
        request: &CreateTask,
        input: TaskCreationInput,
        local_wait_cap: Duration,
    ) -> CreateTaskOutcome {
        self.create_task_inner(request, input, Some(local_wait_cap))
    }

    fn create_task_inner(
        &self,
        request: &CreateTask,
        input: TaskCreationInput,
        local_wait_cap: Option<Duration>,
    ) -> CreateTaskOutcome {
        let receipt = self.admit_create_task(request, input, local_wait_cap);
        // Read off the receipt this call is about to return, so the evidence
        // names the same verdict the frontend is given. Re-deriving it from
        // registry state here could disagree with that answer, because the
        // lock is released by now.
        if let Some(event) = TaskProtocolEvent::create_task(request.identity(), &receipt) {
            self.ports.observe(event);
        }
        receipt
    }

    fn admit_create_task(
        &self,
        request: &CreateTask,
        input: TaskCreationInput,
        local_wait_cap: Option<Duration>,
    ) -> CreateTaskOutcome {
        let envelope = request.envelope();
        let operation = envelope.operation_id();
        let identity = request.identity();
        let context = request.context();

        // Identity validation and process fencing come first, so a request
        // aimed at another process never reserves an identity and never waits
        // on a gate. Nothing here reads what the request would create: that
        // belongs to the winner, below.
        if let Err(mismatch) = identity.verify_query_context(context) {
            return identity_mismatch(operation, mismatch);
        }
        if identity.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }

        let _scope = OperationScope::enter(self, context, Lane::Mutation);
        let deadline = self.deadline_of(envelope, local_wait_cap);

        // Every return before the reservation drops `input` unread. A request
        // that converges on, or replays, an existing identity never reaches
        // the execution host, so its body is neither interpreted nor applied.
        let (cell, source) =
            match self.elect_creation_owner(context, identity, operation, deadline, false) {
                Ok(reservation) => reservation,
                Err(outcome) => return *outcome,
            };

        let outcome = self.prepare_reserved_create_task(request, input, Arc::clone(&cell), source);
        cell.preparation_scope().body_exited();
        if self.finish_preparation_exit(context, identity, &cell) {
            cell.preparation_scope().claim_settlement();
        } else {
            // This legacy synchronous caller has no async P charge. Its
            // original active Task entry remains the explicit capacity bound.
            self.state
                .lock()
                .expect(REGISTRY_LOCK)
                .pending_preparation_exits
                .insert(
                    identity,
                    PendingPreparationExit {
                        context,
                        cell: Arc::clone(&cell),
                        charged: false,
                    },
                );
        }
        outcome
    }

    fn preparation_panicked(
        &self,
        cell: &Arc<CreationCell>,
        role: crate::task_preparation::PreparationUnwindRole,
        payload: Box<dyn std::any::Any + Send>,
    ) -> CreationFailure {
        let scope = cell.preparation_scope();
        // Store first. Acceptance may notify observers, which is independent
        // foreign work; it cannot implicitly dispose the original Any.
        scope.retain_body_panic(role, payload);
        // One real typed panic occurrence supplies both the operation snapshot
        // and, independently, the original Task acceptance candidate.
        let rejection = crate::HostRejection::from_domain_with_detail(
            TaskFailureCategory::Internal,
            PreparationPanicked,
            novarocks_execution_contract::SafeDetail::truncating("Task preparation panicked"),
        );
        let failure = cell.failure().unwrap_or_else(|| {
            CreationFailure::domain(
            novarocks_execution_contract::operation::DomainFailureOutcome::InvalidStateOrRequest,
            rejection.operation_failure(),
        )
        });
        cell.fail(failure.clone());
        if let Some(status) = cell.accepted_status() {
            let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                status.report_preparation_body_failure(rejection.into_failure());
            }));
            if let Err(payload) = accepted {
                scope.body_failure_publication_panicked(role, payload);
            }
        }
        // The original Task first cause, if any, is accepted before this Drop.
        scope.dispose_body_panic(role);
        failure
    }

    fn finish_unaccepted_reservation(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
        cell: &Arc<CreationCell>,
    ) {
        assert!(
            cell.accepted_status().is_none(),
            "unaccepted reservation has no Task authority"
        );
        let scope = cell.preparation_scope();
        scope.request_rollback();
        scope.cleanup(crate::task_preparation::PreparationCleanup::Inbound, || {});
        scope.cleanup(crate::task_preparation::PreparationCleanup::Receiver, || {});
        if !cell.rollback_complete() {
            cell.finish_rollback(
                cell.failure()
                    .expect("unaccepted reserved refusal has its original operation result"),
            );
        }
        scope.body_exited();
        if self.finish_preparation_exit(context, identity, cell) {
            scope.claim_settlement();
        } else {
            self.state
                .lock()
                .expect(REGISTRY_LOCK)
                .pending_preparation_exits
                .insert(
                    identity,
                    PendingPreparationExit {
                        context,
                        cell: Arc::clone(cell),
                        charged: false,
                    },
                );
        }
    }

    fn prepare_reserved_create_task(
        &self,
        request: &CreateTask,
        input: TaskCreationInput,
        cell: Arc<CreationCell>,
        source: Arc<TaskStatusSource>,
    ) -> CreateTaskOutcome {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        let context = request.context();
        let descriptor = request.descriptor();
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

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.prepare_creation_body(&mut transaction, request, input, source)
        }));
        match outcome {
            Ok(outcome) => outcome,
            Err(payload) => {
                let failure = self.preparation_panicked(
                    &transaction.cell,
                    crate::task_preparation::PreparationUnwindRole::Preparation,
                    payload,
                );
                transaction.failure = Some(failure.clone());
                failure.receipt(operation)
            }
        }
    }

    fn prepare_creation_body(
        &self,
        transaction: &mut CreationTransaction<'_>,
        request: &CreateTask,
        input: TaskCreationInput,
        source: Arc<TaskStatusSource>,
    ) -> CreateTaskOutcome {
        let operation = request.envelope().operation_id();
        let identity = request.identity();
        let context = request.context();
        if transaction.cell.accepted_status().is_none() {
            // The synchronous caller shares the real Context acceptance
            // fence; it cannot bind new heavy work after that fence closes.
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            if state.context_state(context) != QueryContextState::Active {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::ContextTerminalReceipt,
                    "the query context closed before Task preparation acceptance",
                );
            }
            let status = Arc::new(TaskStatusOwner::from_preparation_scope(
                identity,
                Arc::clone(&source),
                Arc::clone(&self.clock),
                self.config.metric_publish_min_interval,
                self.ports.execution_health(),
                transaction.cell.preparation_scope_arc(),
            ));
            transaction.cell.accept(status.clone());
            state
                .contexts
                .get_mut(&context)
                .expect("reserved context")
                .mark_spent(identity);
            drop(state);
            status.release_to_observers();
        }
        if transaction.interrupted() {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                "task stopped before preparation",
            );
        }

        // Checked by the winner only, under the reservation it now owns and
        // before any install: an initial domain naming a member the descriptor
        // never froze is this round's own refusal. Abandoning rolls the
        // reservation back and wakes every create that converged on it, so a
        // later legal request for the same identity can win a new round.
        if let Err(rejection) = validate_task_domain_execution_membership(
            &transaction.descriptor,
            request.initial_domains(),
        ) {
            return transaction.abandon_host(
                operation,
                DomainFailureOutcome::InvalidStateOrRequest,
                rejection.into_host(),
            );
        }

        // The one call that interprets this task's static plan. The host
        // proves the plan and the assignment against the descriptor before it
        // publishes anything, and a refusal returns only after the host undid
        // its own local preparation: nothing is recorded as installed until it
        // succeeds, so this owner never removes a receiver it does not hold.
        let preparation_owner = transaction
            .cell
            .accepted_status()
            .expect("accepted Task owns preparation work");
        let prepared = match self.task_host.install_receiver(
            &transaction.descriptor,
            input,
            crate::TaskPreparationContext::new(&preparation_owner, self.config.gate_poll_interval),
        ) {
            Ok(prepared) => prepared,
            Err(rejection) => {
                return transaction.abandon_host(
                    operation,
                    DomainFailureOutcome::InvalidStateOrRequest,
                    rejection,
                );
            }
        };
        transaction.receiver_installed = true;
        if let Err(rejection) = prepared.validate_task(&transaction.descriptor) {
            return transaction.abandon_host(
                operation,
                DomainFailureOutcome::InvalidStateOrRequest,
                rejection,
            );
        }
        if transaction.interrupted() {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                "task stopped during preparation",
            );
        }
        if let Err(rejection) = self
            .task_host
            .install_inbound_capability(&transaction.descriptor)
        {
            return transaction.abandon_host(
                operation,
                DomainFailureOutcome::InvalidStateOrRequest,
                rejection,
            );
        }
        transaction.capability_installed = true;

        let mut task_domains = TaskDomains::for_descriptor(&transaction.descriptor);
        let receipts = match apply_task_domain_updates(
            &*self.task_host,
            &transaction.descriptor,
            &mut task_domains,
            request.initial_domains(),
            operation,
        ) {
            Ok((receipts, _)) => receipts,
            Err(rejection) => {
                let outcome = rejection.domain_failure_outcome();
                return transaction.abandon_host(operation, outcome, rejection.into_host());
            }
        };

        if transaction.interrupted() {
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                "task stopped before activation",
            );
        }

        let status = transaction
            .cell
            .accepted_status()
            .expect("heavy preparation has the original Accepted owner and scope");
        let runnable = match self.task_host.submit_runnable(
            &transaction.descriptor,
            TaskStatusReporter::new(Arc::clone(&status)),
        ) {
            Ok(runnable) => runnable,
            Err(rejection) => {
                let outcome = match rejection.category() {
                    TaskFailureCategory::ResourceExhausted => {
                        DomainFailureOutcome::ResourceExhausted
                    }
                    _ => DomainFailureOutcome::InvalidStateOrRequest,
                };
                return transaction.abandon_host(operation, outcome, rejection);
            }
        };

        let receipt = CreateTaskReceipt::new(identity, receipts, status.current());
        let (prepared, root) = prepared.into_parts();
        if let Some((runnable, status)) = transaction.commit(
            LiveTask {
                descriptor: Arc::clone(&transaction.descriptor),
                prepared,
                receipt: receipt.clone(),
                creation_failure: None,
                status,
                runnable,
                domains: task_domains,
                receiver_installed: true,
                capability_installed: true,
            },
            root,
        ) {
            // The context closed while this task was being built. Stand the
            // committed worker down. The closing context retained the Live
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
            if transaction.cell.accepted_status().is_some() {
                return OperationReceipt::acknowledged(
                    operation,
                    OperationOutcome::Accepted,
                    receipt,
                );
            }
            return OperationReceipt::rejected(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                "the query context closed while this task was being created",
            );
        }
        self.counters.tasks_created.fetch_add(1, Ordering::Relaxed);
        self.ports.record_task_created();
        OperationReceipt::acknowledged(operation, OperationOutcome::Accepted, receipt)
    }

    /// Elects a creation owner, waiting on the exact creation gate when the
    /// context does not exist yet.
    ///
    /// The election key is the exact task identity under the exact query
    /// context; no part of either is ever matched on its own, and no body is
    /// compared. Every concurrent create of one identity converges on one
    /// owner: the winner reserves the identity, the others wait on the gate
    /// within their own deadline and then read the owner's result, so they
    /// observe the same acknowledgement or the same failure. A follower never
    /// answers before that round settles and never preempts it. Once a failed
    /// round has fully rolled back and its reservation is gone, a follower
    /// that is still waiting, or a later request, may win the next round.
    ///
    /// | Identity state | Answer |
    /// |---|---|
    /// | absent, admissible context | reserve `Creating`; this request wins |
    /// | `Creating` | wait for that round; its failure, or a timeout |
    /// | `Live` / `Retired`, created | `Idempotent`, the original receipt |
    /// | `Live` / `Retired`, failed | the fixed creation failure |
    /// | `Gone`, spent, closed or reclaimed context | the terminal outcome |
    // Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)
    fn elect_creation_owner(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
        accepted_replay: bool,
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
                OperationAdmission::Admit => {
                    // A create can enter this method before an establish
                    // reaches its runner-owned restart rendezvous, then wake
                    // from the creation gate after that marker is published.
                    // Check at the owner election linearization point, rather
                    // than at RPC ingress, so no task becomes admitted in the
                    // marker-to-process-loss interval.
                    if self.task_creation_gate.holds_task_creation(context) {
                        if accepted_replay {
                            return Err(Box::new(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::NotReady,
                                "create admission is waiting for context activation",
                            )));
                        }
                        drop(state);
                        self.task_creation_gate
                            .wait_for_task_creation_release(context);
                        state = self.state.lock().expect(REGISTRY_LOCK);
                        continue;
                    }
                }
                OperationAdmission::WaitForCreationGate => {
                    if accepted_replay {
                        return Err(Box::new(OperationReceipt::rejected(
                            operation,
                            OperationOutcome::NotReady,
                            "create admission requires an active context",
                        )));
                    }
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
                        if accepted_replay {
                            OperationOutcome::NotReady
                        } else {
                            OperationOutcome::ContextNotEstablished
                        },
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
                        if accepted_replay && let Some(status) = cell.accepted_status() {
                            return Err(Box::new(OperationReceipt::acknowledged(
                                operation,
                                OperationOutcome::Idempotent,
                                CreateTaskReceipt::new(identity, Vec::new(), status.current()),
                            )));
                        }
                        if let Some(failure) = cell.failure() {
                            self.counters
                                .creations_converged
                                .fetch_add(1, Ordering::Relaxed);
                            return Err(Box::new(failure.receipt(operation)));
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
                    // The context is closed, but a create whose identity
                    // already names a task is still answered from that task's
                    // retained record, exactly as it would be while open.
                    if let Some(outcome) = state
                        .contexts
                        .get(&context)
                        .and_then(|entry| entry.tasks.get(&identity))
                        .and_then(|task| existing_create_reply(task, operation))
                    {
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
                    // A creation of this identity is in progress. Whatever
                    // this request carries, it waits for that round rather
                    // than preempting it or answering early.
                    Some(TaskEntry::Creating(cell)) => Decision::Converge(Arc::clone(cell)),
                    Some(task) => Decision::done(
                        existing_create_reply(task, operation)
                            .expect("only a creating identity has no settled answer"),
                    ),
                    None if entry.has_spent(identity) => {
                        Decision::done(OperationReceipt::rejected(
                            operation,
                            OperationOutcome::Gone,
                            "this identity's detailed record was reclaimed",
                        ))
                    }
                    None => {
                        // This admission observation precedes the new identity
                        // reservation. Already owned identities still replay,
                        // and existing cleanup remains serviceable after a
                        // process exit fault; no Task reason is rewritten.
                        if !self.ports.execution_health().is_healthy() {
                            Decision::done(OperationReceipt::rejected(
                                operation,
                                OperationOutcome::NotReady,
                                "backend execution health closed new task admission",
                            ))
                        } else if entry.occupied_slots() >= self.config.max_tasks_per_context {
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
                    let cell = Arc::new(CreationCell::new(identity, self.ports.execution_health()));
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
                    if accepted_replay && let Some(status) = cell.accepted_status() {
                        return Err(Box::new(OperationReceipt::acknowledged(
                            operation,
                            OperationOutcome::Idempotent,
                            CreateTaskReceipt::new(identity, Vec::new(), status.current()),
                        )));
                    }
                    if let Some(failure) = cell.failure() {
                        self.counters
                            .creations_converged
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(Box::new(failure.receipt(operation)));
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

    #[cfg(novarocks_dev_controls)]
    pub fn reserve_test_update_consumption(
        &self,
        request: &UpdateTask,
        control: novarocks_failpoint::scope::ControlCancellation,
    ) -> Result<Box<dyn crate::TestTaskUpdateConsumptionReservation>, crate::HostRejection> {
        // Validate actual membership before a test reservation can create a
        // queue. The ordinary update remains the sole application authority.
        let descriptor = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            let live = state
                .task_index
                .get(&request.identity())
                .and_then(|context| live_task(&state, *context, request.identity()))
                .ok_or_else(|| {
                    crate::HostRejection::from_domain(
                        TaskFailureCategory::Protocol,
                        crate::host::TestUpdateConsumptionFailure::LiveTaskRequired(
                            request.identity(),
                        ),
                    )
                })?;
            Arc::clone(&live.descriptor)
        };
        validate_task_domain_execution_membership(&descriptor, request.domains())
            .map_err(|error| error.into_host())?;
        self.task_host
            .reserve_test_update_consumption(request, control)
    }

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

        let plan =
            match plan_task_domain_execution_updates(&descriptor, &task_domains, request.domains())
            {
                Ok(plan) => plan,
                Err(rejection) => {
                    return OperationReceipt::domain_failure(
                        operation,
                        rejection.domain_failure_outcome(),
                        rejection.operation_failure(),
                    );
                }
            };
        let queued = match apply_planned_task_domain_updates(
            &*self.task_host,
            &descriptor,
            request.domains(),
            &plan,
            operation,
        ) {
            Ok(queued) => queued,
            Err(rejection) => {
                return OperationReceipt::domain_failure(
                    operation,
                    rejection.domain_failure_outcome(),
                    rejection.operation_failure(),
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
            match commit_task_domain_execution_updates(
                &mut live.domains,
                request.domains(),
                &queued,
            ) {
                Ok(result) => result,
                Err(rejection) => {
                    return OperationReceipt::domain_failure(
                        operation,
                        rejection.domain_failure_outcome(),
                        rejection.operation_failure(),
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
        self.update_query_context_inner(request, None)
    }

    /// Applies the same exact context command with a shorter local ingress
    /// wait budget for Establish. Other commands do not wait on a gate.
    pub fn update_query_context_with_local_wait_cap(
        &self,
        request: &UpdateQueryContext,
        local_wait_cap: Duration,
    ) -> QueryContextOutcome {
        self.update_query_context_inner(request, Some(local_wait_cap))
    }

    fn update_query_context_inner(
        &self,
        request: &UpdateQueryContext,
        local_wait_cap: Option<Duration>,
    ) -> QueryContextOutcome {
        match request {
            UpdateQueryContext::Establish(establish) => {
                let receipt = self.establish_query_context(establish, local_wait_cap);
                // Emitted on the receipt, not inside the handler: the
                // establish decision has several exit paths and the receipt is
                // the one place all of them agree on.
                if let Some(event) =
                    TaskProtocolEvent::establish_query_context(establish.context(), &receipt)
                {
                    self.ports.observe(event);
                }
                receipt
            }
            UpdateQueryContext::AdvanceDomain(advance) => {
                self.advance_query_context_domain(advance)
            }
            UpdateQueryContext::RenewLease(renew) => {
                let receipt = self.renew_query_execution_lease(renew);
                if let Some(event) = TaskProtocolEvent::renew_query_execution_lease(
                    renew.context(),
                    renew.sequence(),
                    &receipt,
                ) {
                    self.ports.observe(event);
                }
                receipt
            }
        }
    }

    fn establish_query_context(
        &self,
        request: &EstablishQueryContext,
        local_wait_cap: Option<Duration>,
    ) -> QueryContextOutcome {
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
        let deadline = self.deadline_of(envelope, local_wait_cap);

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
                    state.lease_expiry.insert(context, lease);
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
        if state.context_state(context) == QueryContextState::Quiescing {
            // Materialization lost to the normal admission fence. Release only
            // staged shared facts; the fence and its membership remain intact.
            let entry = state.contexts.get_mut(&context).expect("quiescing context");
            entry.released_evidence = self.context_host.release(context);
            entry.facts_released = true;
            transaction.commit();
            self.gate.notify_all();
            let receipt = context_receipt(entry, context);
            drop(state);
            return OperationReceipt::acknowledged(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                receipt,
            );
        }
        if let Err(rejection) = materialized {
            let failure = rejection.operation_failure();
            transaction.abandon(&mut state, AbortCause::QueryFailed, now);
            drop(state);
            return OperationReceipt::domain_failure(operation,novarocks_execution_contract::operation::DomainFailureOutcome::InvalidStateOrRequest,failure);
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
            return OperationReceipt::domain_failure(operation,novarocks_execution_contract::operation::DomainFailureOutcome::InvalidStateOrRequest,rejection.operation_failure());
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
        let entry = state.contexts.get_mut(&context).expect("admitted context");
        let Some(installed) = entry.lease else {
            return OperationReceipt::acknowledged(
                operation,
                OperationOutcome::ContextTerminalReceipt,
                context_receipt(entry, context),
            );
        };
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
                state.lease_expiry.replace(context, installed, renewed);
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

        let (snapshot, owner, runnable, outcome) = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            match self.locate_task_locked(&state, context, identity) {
                TaskLocation::Live | TaskLocation::Creating => {
                    let (status, runnable) = match state
                        .contexts
                        .get(&context)
                        .and_then(|entry| entry.tasks.get(&identity))
                    {
                        Some(TaskEntry::Live(live)) => {
                            (Some(live.status.clone()), Some(live.runnable.clone()))
                        }
                        Some(TaskEntry::Creating(cell)) => (cell.accepted_status(), None),
                        _ => unreachable!("located exact Task"),
                    };
                    let Some(status) = status else {
                        return OperationReceipt::rejected(
                            operation,
                            OperationOutcome::InvalidStateOrRequest,
                            "cancel reached a task whose creation has not been acknowledged",
                        );
                    };
                    let candidate = crate::task_conclusion::PreparedTaskConclusion::new(
                        novarocks_execution_contract::TaskConclusionProposal::Canceled(
                            request.reason(),
                        ),
                    );
                    let publication = status.submit_prepared_owned(candidate);
                    let outcome = if publication.acceptance().decision
                        == novarocks_execution_contract::ConclusionDecision::Keep
                    {
                        OperationOutcome::Idempotent
                    } else {
                        OperationOutcome::Accepted
                    };
                    state.pending_task_publications.push(publication);
                    (status.current(), Some(status), runnable, outcome)
                }
                TaskLocation::Retired => {
                    let retired = retired_task(&state, context, identity).expect("retired Task");
                    (
                        retired.status.clone(),
                        None,
                        None,
                        OperationOutcome::Idempotent,
                    )
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
        // The sole Task acceptance is already visible; its notifications stay
        // fenced until exact output revocation and physical stand-down finish.
        // Context-owned output can still exist after the Task retired.
        self.ports.discard_task(identity);
        if let Some(runnable) = runnable {
            runnable.cancel(request.reason());
        }
        let snapshot = owner.map_or(snapshot, |status| status.current());
        OperationReceipt::acknowledged(operation, outcome, snapshot)
    }

    // --------------------------------------------------- abort query context

    pub fn abort_query_context(&self, request: &AbortQueryContext) -> QueryContextOutcome {
        let receipt = self.apply_abort_query_context(request);
        if let Some(event) = TaskProtocolEvent::abort_query_context(request.context(), &receipt) {
            self.ports.observe(event);
        }
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

    // ------------------------------------------------ quiesce query context

    pub fn quiesce_query_context(
        &self,
        request: &QuiesceQueryContext,
    ) -> QuiesceQueryContextOutcome {
        let operation = request.envelope().operation_id();
        let context = request.context();
        if context.backend_process_id() != self.config.backend_process_id {
            return identity_mismatch(
                operation,
                IdentityMismatch::new(IdentityField::BackendProcess),
            );
        }
        let (receipt, outcome, publications) = {
            let mut state = self.state.lock().expect(REGISTRY_LOCK);
            let now = self.clock.now();
            self.expire_leases_locked(&mut state, now);
            if let Err(mismatch) = fence_frontend(&state, context) {
                return identity_mismatch(operation, mismatch);
            }
            let current = state.context_state(context);
            if current == QueryContextState::Gone {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::Gone,
                    "quiesce reached a reclaimed query context",
                );
            }
            if current == QueryContextState::Absent {
                let vacant_fences = state
                    .contexts
                    .values()
                    .filter(|entry| {
                        entry.state == QueryContextState::Quiescing && entry.lease.is_none()
                    })
                    .count();
                if vacant_fences >= self.config.retained_context_capacity {
                    return OperationReceipt::rejected(
                        operation,
                        OperationOutcome::ResourceExhausted,
                        "backend reached its vacant context fence bound",
                    );
                }
                state.contexts.insert(
                    context,
                    ContextEntry::absent(Arc::new(TaskStatusSource::new())),
                );
                state
                    .contexts
                    .get_mut(&context)
                    .expect("new context")
                    .retired_at = Some(now);
                state
                    .context_by_execution
                    .insert(context.query_execution_id(), context);
            }
            let entry = state
                .contexts
                .get_mut(&context)
                .expect("quiesce fence retains context");
            if entry.quiesce.is_some() {
                let receipt = entry
                    .quiesce
                    .clone()
                    .expect("checked")
                    .with_state(entry.state);
                (receipt, OperationOutcome::Idempotent, Vec::new())
            } else if matches!(
                entry.state,
                QueryContextState::Aborting
                    | QueryContextState::Releasing
                    | QueryContextState::TerminalRetained
            ) {
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::ContextTerminalReceipt,
                    "quiesce reached a context already closing without a normal fence",
                );
            } else {
                entry.state = QueryContextState::Quiescing;
                let receipt = QuiesceQueryContextReceipt::new(
                    context,
                    1,
                    entry.accepted_identities(context),
                    entry.state,
                );
                entry.quiesce = Some(receipt.clone());
                entry.source.publish_quiesce(receipt.clone());
                let publications =
                    entry
                        .tasks
                        .values()
                        .filter_map(|task| match task {
                            TaskEntry::Creating(cell) => cell.accepted_status(),
                            TaskEntry::Live(live) => Some(live.status.clone()),
                            _ => None,
                        })
                        .map(|status| {
                            let candidate = crate::task_conclusion::PreparedTaskConclusion::new(
                        novarocks_execution_contract::TaskConclusionProposal::Canceled(
                            novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded));
                            status.submit_prepared_owned(candidate)
                        })
                        .collect::<Vec<_>>();
                state.pending_quiesce.insert(context);
                self.task_host.close_context_admission(context);
                self.admission_tickets.revoke_unredeemed(context, now);
                self.gate.notify_all();
                (receipt, OperationOutcome::Accepted, publications)
            }
        };
        // Acceptance is synchronous. Publish its already installed receipts
        // outside the registry fence; the maintenance owner performs physical
        // stand-down without delaying this control acknowledgement.
        for publication in publications {
            publication.publish();
        }
        OperationReceipt::acknowledged(operation, outcome, receipt)
    }

    // ------------------------------------------------- release query context

    pub fn release_query_context(
        &self,
        request: &ReleaseQueryContext,
    ) -> ReleaseQueryContextOutcome {
        let receipt = self.apply_release_query_context(request);
        let runtime_filter = self
            .released_context_evidence(request.context())
            .runtime_filter_observation();
        if let Some(event) =
            TaskProtocolEvent::release_query_context(request.context(), &receipt, runtime_filter)
        {
            self.ports.observe(event);
        }
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
                drop(state);
                return OperationReceipt::rejected(
                    operation,
                    OperationOutcome::InvalidStateOrRequest,
                    "normal release requires a quiesce fence",
                );
            }
            QueryContextState::Quiescing => {
                self.retire_locked(&mut state, now);
                if self.release_ready_locked(&state, context) {
                    let entry = state.contexts.get_mut(&context).expect("quiescing context");
                    let lease = entry.lease;
                    for root in entry.roots.values() {
                        root.seal_reads(novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextReleased);
                    }
                    entry.state = QueryContextState::Releasing;
                    if let Some(lease) = lease {
                        let removed = state.lease_expiry.remove(context, lease);
                        assert!(removed, "releasing context must have an indexed lease");
                    }
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
        tasks_drained
            && state
                .prepare_context_counts
                .get(&context)
                .copied()
                .unwrap_or(0)
                == 0
            && state.in_flight_of(context).is_idle()
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
        self.recheck_preparation_exits();
        let mut sweep = self.settle();
        sweep.admission_tickets_expired =
            self.admission_tickets.advance_deadlines(self.clock.now());
        sweep.metrics_flushed = self.flush_throttled_metrics();
        sweep
    }

    fn settle(&self) -> DeadlineSweep {
        let mut sweep = DeadlineSweep::default();
        for _ in 0..MAX_SETTLE_PASSES {
            let (normal_fanouts, fanouts, sealed_roots, publications) = {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                let now = self.clock.now();
                sweep.leases_expired += self.expire_leases_locked(&mut state, now);
                self.escalate_task_failures_locked(&mut state, now);
                (
                    std::mem::take(&mut state.pending_quiesce),
                    std::mem::take(&mut state.pending_termination),
                    state
                        .contexts
                        .values()
                        .filter(|entry| {
                            matches!(
                                entry.state,
                                QueryContextState::Aborting | QueryContextState::Releasing
                            )
                        })
                        .flat_map(|entry| entry.roots.values().cloned())
                        .collect::<Vec<_>>(),
                    std::mem::take(&mut state.pending_task_publications),
                )
            };
            for root in sealed_roots {
                root.finish_seal();
            }
            let published = !publications.is_empty();
            for publication in publications {
                publication.publish();
            }
            for context in &normal_fanouts {
                self.stand_down_quiescing_tasks(*context);
            }
            for context in &fanouts {
                self.stand_down_tasks(*context);
            }
            let released_root_owners = {
                let mut state = self.state.lock().expect(REGISTRY_LOCK);
                let now = self.clock.now();
                sweep.tasks_retired += self.retire_locked(&mut state, now);
                self.complete_terminations_locked(&mut state, now);
                self.expire_vacant_quiesce_locked(&mut state, now);
                let (tasks, contexts) = self.reap_locked(&mut state, now);
                sweep.tasks_reaped += tasks;
                sweep.contexts_reaped += contexts;
                self.enforce_capacity_locked(&mut state);
                std::mem::take(&mut state.released_root_owners)
            };
            drop(released_root_owners);
            self.gate.notify_all();
            if fanouts.is_empty() && normal_fanouts.is_empty() && !published {
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
        let mut count = 0;
        while let Some((deadline, context)) = state.lease_expiry.take_due(now) {
            let current = state.contexts.get(&context);
            assert!(
                current.is_some_and(|entry| {
                    matches!(
                        entry.state,
                        QueryContextState::Establishing
                            | QueryContextState::Active
                            | QueryContextState::Quiescing
                    ) && entry
                        .lease
                        .is_some_and(|lease| lease.expires_at() == deadline)
                }),
                "indexed lease must match an expirable context"
            );
            if self.begin_termination_locked(
                state,
                context,
                TerminationDetail::Aborted(AbortCause::LeaseExpired),
                now,
            ) {
                self.counters.lease_expiries.fetch_add(1, Ordering::Relaxed);
                self.ports
                    .observe(TaskProtocolEvent::query_execution_lease_expired(context));
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
                QueryContextState::Establishing
                    | QueryContextState::Active
                    | QueryContextState::Quiescing
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
            state.contexts.insert(context, entry);
            state
                .context_by_execution
                .insert(context.query_execution_id(), context);
            self.retain_context_terminal_locked(state, context, now);
            return true;
        }
        if !matches!(
            classify_context_transition(current, QueryContextEvent::Abort),
            ContextTransition::Apply(QueryContextState::Aborting)
        ) {
            return false;
        }
        let (won, task_identities, installed_lease, publications) = {
            let mut publications = Vec::new();
            let entry = state
                .contexts
                .get_mut(&context)
                .expect("a non-absent context exists");
            if !matches!(entry.latch.latch(detail), LatchOutcome::Won) {
                return false;
            }
            self.task_host.abort_context_admission(context);
            entry.state = QueryContextState::Aborting;
            let root_close = if matches!(
                entry.latch.cause(),
                Some(TerminationDetail::Aborted(AbortCause::LeaseExpired))
            ) {
                novarocks_execution_contract::root_lifetime::RootRetentionClose::LeaseExpired
            } else {
                novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted
            };
            // Fix every accepted Task's exact lifecycle cause before root
            // sealing becomes visible to producer/driver threads. Runnable abort
            // remains outside the registry fence; until that fan-out arrives,
            // completion reads this Worker-owned cause rather than treating
            // the closed channel as an originating execution failure.
            let cause = entry
                .termination_cause()
                .expect("a latched context has an abort cause");
            for task in entry.tasks.values() {
                let status = match task {
                    TaskEntry::Creating(cell) => cell.accepted_status(),
                    TaskEntry::Live(live) => Some(live.status.clone()),
                    _ => None,
                };
                if let Some(status) = status {
                    let candidate = crate::task_conclusion::PreparedTaskConclusion::new(
                        novarocks_execution_contract::TaskConclusionProposal::Aborted(cause),
                    );
                    publications.push(status.submit_prepared_owned(candidate));
                }
            }
            for root in entry.roots.values() {
                root.seal_reads(root_close);
            }
            entry.terminating_since = Some(now);
            let installed_lease = entry.lease.take();
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
            (
                true,
                entry.tasks.keys().copied().collect::<Vec<_>>(),
                installed_lease,
                publications,
            )
        };
        state.pending_task_publications.extend(publications);
        if let Some(lease) = installed_lease {
            state.lease_expiry.remove(context, lease);
        }
        if won {
            // The context termination invalidates every unconsumed root result
            // immediately, including output whose producer already finished
            // and no longer holds a fragment cancellation handle.
            for identity in task_identities {
                self.ports.discard_task(identity);
            }
            self.admission_tickets.revoke_unredeemed(context, now);
            state.pending_termination.insert(context);
        }
        won
    }

    /// Asks every non-terminal task of a terminating context to stand down.
    fn stand_down_quiescing_tasks(&self, context: QueryContextRef) {
        let targets = {
            let state = self.state.lock().expect(REGISTRY_LOCK);
            let Some(entry) = state.contexts.get(&context) else {
                return;
            };
            if entry.state != QueryContextState::Quiescing {
                return;
            }
            entry
                .tasks
                .values()
                .filter_map(|task| match task {
                    TaskEntry::Live(live) => {
                        Some((Arc::clone(&live.status), Arc::clone(&live.runnable)))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for (status, runnable) in targets {
            status.advance(
                TaskState::Canceling,
                Some(TerminationDetail::Canceled(
                    novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
                )),
                TaskOutputFacts::default(),
            );
            runnable.quiesce();
        }
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
                    TaskEntry::Live(live) => {
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
                let normal_context = matches!(
                    entry.state,
                    QueryContextState::Active
                        | QueryContextState::Quiescing
                        | QueryContextState::Releasing
                );
                for identity in ready {
                    let Some(TaskEntry::Live(live)) = entry.tasks.remove(&identity) else {
                        continue;
                    };
                    let mut live = *live;
                    let status = live.status.current();
                    let normally_stopped = normal_context
                        && matches!(status.state(), TaskState::Finished | TaskState::Canceled)
                        && (status.state() == TaskState::Finished
                            || matches!(
                                status.termination(),
                                Some(TerminationDetail::Canceled(_))
                            ));
                    // Real cancellation may withdraw strict input without
                    // pretending consumption. It keeps its original cause and
                    // uses cancellation cleanup; reads retain normal withdrawal.
                    let strict_canceled = status.state() == TaskState::Canceled
                        && live.descriptor.topology().inbound().iter().any(|input|
                            input.delivery() == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain);
                    if normally_stopped
                        && !strict_canceled
                        && live.capability_installed
                        && live.receiver_installed
                    {
                        if let Err(rejection) =
                            self.task_host.retire_receiver_normally(&live.descriptor)
                        {
                            // A contradiction after terminal publication cannot
                            // rewrite that Task. The existing physical-exit
                            // health owner keeps the evidence; Live retains the
                            // original ingress/reservation until abort cleanup.
                            use novarocks_execution_contract::failure::FailureProjection;
                            self.ports.execution_health().report_unresolved(
                                novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                                    identity,
                                    phase: novarocks_execution_contract::TaskFailurePhase::Execution,
                                }, rejection.failure_fact());
                            entry
                                .tasks
                                .insert(identity, TaskEntry::Live(Box::new(live)));
                            continue;
                        }
                        live.capability_installed = false;
                        live.receiver_installed = false;
                    } else {
                        if live.capability_installed {
                            self.task_host.remove_inbound_capability(&live.descriptor);
                            live.capability_installed = false;
                        }
                        if live.receiver_installed {
                            self.task_host.remove_receiver(&live.descriptor);
                            live.receiver_installed = false;
                        }
                    }
                    live.status
                        .retire()
                        .expect("a retirement-ready live task can retire exactly once");
                    let final_info = live.status.final_info();
                    let result_owner = live.prepared.sink_kind() == FragmentSinkKind::Result;
                    let bytes =
                        estimate_retained_bytes(&live.receipt, &status, final_info.as_ref());
                    let terminal_state = status.state();
                    entry.tasks.insert(
                        identity,
                        TaskEntry::Retired(Box::new(RetiredTask {
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
                self.ports.retire_task_result(identity);
                state.active_tasks = state.active_tasks.saturating_sub(1);
                state.retained_tasks = state.retained_tasks.saturating_add(1);
                state.retained_bytes = state.retained_bytes.saturating_add(bytes);
                state.retired_task_order.push_back((context, identity));
                self.ports
                    .observe(TaskProtocolEvent::task_terminal_retained(
                        identity,
                        terminal_state,
                        bytes,
                    ));
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
                let ready = entry.tasks.values().all(TaskEntry::is_terminal_record)
                    && entry.roots.values().all(|root| root.try_join_physical())
                    && state
                        .prepare_context_counts
                        .get(&context)
                        .copied()
                        .unwrap_or(0)
                        == 0;
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
            debug_assert_eq!(next, QueryContextState::TerminalRetained);
            if event == QueryContextEvent::AbortCompleted {
                let entry = state
                    .contexts
                    .get(&context)
                    .expect("a completing context exists");
                self.ports
                    .observe(TaskProtocolEvent::context_termination_completed(
                        context,
                        entry.latch.cause(),
                        entry.tasks.len(),
                    ));
            }
            self.retain_context_terminal_locked(state, context, now);
        }
    }

    /// A fence installed before Establish has no lease to drive cleanup. It
    /// retains its original cut for the legal request horizon, then retires
    /// without claiming any Task ever existed.
    fn expire_vacant_quiesce_locked(&self, state: &mut RegistryState, now: MonotonicInstant) {
        let expired: Vec<QueryContextRef> = state
            .contexts
            .iter()
            .filter_map(|(context, entry)| {
                (entry.state == QueryContextState::Quiescing
                    && entry.lease.is_none()
                    && entry.tasks.is_empty()
                    && entry.retired_at.is_some_and(|since| {
                        !self.config.request_horizon.must_retain_at(since, now)
                    }))
                .then_some(*context)
            })
            .collect();
        for context in expired {
            self.retain_context_terminal_locked(state, context, now);
        }
    }

    /// Closes every Worker responsibility before publishing context convergence.
    ///
    /// Every path into `TerminalRetained` comes through this helper. The
    /// receipt is published last, after task convergence, data-plane fencing,
    /// host release, result discard, and admission-ticket release are all
    /// observable facts. Re-entering it is idempotent and cannot mint another
    /// convergence version.
    fn retain_context_terminal_locked(
        &self,
        state: &mut RegistryState,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> bool {
        let Some(entry) = state.contexts.get(&context) else {
            return false;
        };
        if entry.state == QueryContextState::TerminalRetained {
            return false;
        }
        assert_ne!(
            entry.state,
            QueryContextState::Gone,
            "a reclaimed query context cannot become retained again"
        );
        assert!(
            entry.tasks.values().all(TaskEntry::is_terminal_record),
            "query context convergence requires every task to have physically converged"
        );
        assert!(
            entry.roots.values().all(|root| root.physical_idle()),
            "context convergence requires root read/send/producer owners to exit"
        );

        assert_eq!(
            state
                .prepare_context_counts
                .get(&context)
                .copied()
                .unwrap_or(0),
            0,
            "query context convergence requires every preparation job to have exited"
        );
        self.task_host.close_context_admission(context);
        if let Some(lease) = entry.lease {
            state.lease_expiry.remove(context, lease);
        }
        let (task_identities, source, roots) = {
            let entry = state
                .contexts
                .get_mut(&context)
                .expect("a retained context exists");
            entry.lease = None;
            entry.facts_visible = false;
            if !entry.facts_released {
                entry.released_evidence = self.context_host.release(context);
                entry.facts_released = true;
            }
            entry.domains = QueryContextDomains::empty();
            (
                entry.tasks.keys().copied().collect::<Vec<_>>(),
                Arc::clone(&entry.source),
                std::mem::take(&mut entry.roots),
            )
        };
        if !roots.is_empty() {
            state.released_root_owners.push(roots);
        }
        for identity in task_identities {
            self.ports.discard_task(identity);
        }
        self.admission_tickets.release_context(context, now);
        self.task_host.retire_context_execution(context);

        let entry = state
            .contexts
            .get_mut(&context)
            .expect("a retained context exists");
        entry.state = QueryContextState::TerminalRetained;
        entry.retired_at = Some(now);
        entry.terminating_since = None;
        state.pending_termination.remove(&context);
        state.retired_context_order.push_back(context);

        source.publish_context_convergence(QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        ));
        true
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
        self.ports.discard_task(identity);
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
            self.ports.discard_task(*identity);
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
        // Gone is beyond the legal request horizon for protected normal-close
        // records. Return their Accepted-time reservation at that transition.
        self.task_host.forget_context_admission(context);
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
                entry.source.forget_gone(identity);
            }
            let spent = state
                .contexts
                .get(&context)
                .is_some_and(|entry| entry.has_spent(identity));
            if !spent {
                state.task_index.remove(&identity);
            }
        }
        let now = self.clock.now();
        while state.retired_context_order.len() > self.config.retained_context_capacity {
            let Some(position) = state.retired_context_order.iter().position(|context| {
                let Some(entry) = state.contexts.get(context) else {
                    return true;
                };
                let Some(retired_at) = entry.retired_at else {
                    return true;
                };
                !self.task_host.retains_normal_close(*context)
                    || !self.config.request_horizon.must_retain_at(retired_at, now)
            }) else {
                break;
            };
            let Some(context) = state.retired_context_order.remove(position) else {
                break;
            };
            self.reap_context_locked(state, context);
        }
        while state.gone_context_order.len() > self.config.gone_fence_capacity {
            let Some(context) = state.gone_context_order.pop_front() else {
                break;
            };
            if let Some(entry) = state.contexts.remove(&context) {
                if let Some(lease) = entry.lease {
                    state.lease_expiry.remove(context, lease);
                }
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

    fn deadline_of(
        &self,
        envelope: OperationEnvelope,
        local_wait_cap: Option<Duration>,
    ) -> MonotonicInstant {
        self.clock.now().saturating_add(effective_wait(
            self.config.wait_caps,
            envelope,
            local_wait_cap,
        ))
    }

    fn wait_gate<'a>(
        &'a self,
        guard: ObservedRegistryGuard<'a, RegistryState>,
    ) -> ObservedRegistryGuard<'a, RegistryState> {
        let (guard, _) = self
            .gate
            .wait_timeout(guard.into_inner(), self.config.gate_poll_interval)
            .expect(REGISTRY_LOCK);
        ObservedRegistryGuard::from_condvar(guard, &self.state.observation)
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
    /// A creation of this identity is already in progress; wait for its
    /// result.
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
        if let Some(lease) = state.contexts.get(&context).and_then(|entry| entry.lease) {
            state.lease_expiry.remove(context, lease);
        }
        {
            let Some(entry) = state.contexts.get_mut(&context) else {
                return;
            };
            entry.lease = None;
            entry.facts_visible = false;
            entry.domains = QueryContextDomains::empty();
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
        }
        self.retain_context_terminal_locked(state, context, now);
    }
}

/// The creation transaction's rollback guard.
///
/// A creation is atomic because every step before the Live commit is undoable:
/// the receiver, inbound capability, and reservation are removed in reverse
/// on failure. `submit_runnable` returns a dormant handle or fails before
/// execution starts; only `commit_creation` starts it after Live is installed.
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
    /// Operation summaries describe the create response only. The original
    /// host/domain cause reaches the Task's sole acceptance owner before any
    /// Worker rollback, callbacks, receiver removal, or source destruction.
    fn abandon_host(
        &mut self,
        operation: TaskOperationId,
        domain_outcome: DomainFailureOutcome,
        rejection: crate::HostRejection,
    ) -> CreateTaskOutcome {
        let failure = CreationFailure::domain(domain_outcome, rejection.operation_failure());
        self.failure = Some(failure.clone());
        if let Some(status) = self.cell.accepted_status() {
            status
                .execution_control(novarocks_execution_contract::TaskFailurePhase::Preparation)
                .report_failure(rejection.into_failure());
        }
        failure.receipt(operation)
    }

    fn interrupted(&self) -> bool {
        let status = self
            .cell
            .accepted_status()
            .expect("heavy preparation has an accepted Task owner");
        novarocks_execution::runtime::task_control::TaskExecutionPort::is_stopped(status.as_ref())
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
        root: Option<Arc<crate::root_result_channel::RootResultChannel>>,
    ) -> Option<(Arc<dyn RunnableTask>, Arc<TaskStatusOwner>)> {
        let status = Arc::clone(&live.status);
        let runnable = Arc::clone(&live.runnable);
        let closed;
        let mut sealed_root = None;
        let mut installed_publication = None;
        {
            let mut state = self.registry.state.lock().expect(REGISTRY_LOCK);
            closed = state.context_state(self.context) != QueryContextState::Active;
            if closed && self.cell.accepted_status().is_none() {
                let failure = CreationFailure::verdict(
                    OperationOutcome::ContextTerminalReceipt,
                    "the query context closed while this task was being created",
                );
                self.cell.fail(failure.clone());
                live.creation_failure = Some(failure);
            }
            let entry = state
                .contexts
                .get_mut(&self.context)
                .expect("a reserved creation retains its query context");
            if let Some(root) = root {
                root.bind_context_progress(&entry.source)
                    .expect("exact root preparation binds its one context source");
                if closed {
                    let reason = if matches!(
                        entry.latch.cause(),
                        Some(TerminationDetail::Aborted(AbortCause::LeaseExpired))
                    ) {
                        novarocks_execution_contract::root_lifetime::RootRetentionClose::LeaseExpired
                    } else {
                        novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted
                    };
                    root.seal_reads(reason);
                    sealed_root = Some(Arc::clone(&root));
                } else {
                    root.mark_context_owned()
                        .expect("active context takes its provisional root");
                }
                assert!(
                    entry.roots.insert(self.identity, root).is_none(),
                    "one root channel per exact creation"
                );
            }
            entry.mark_spent(self.identity);
            entry
                .tasks
                .insert(self.identity, TaskEntry::Live(Box::new(live)));
            // Live now owns every installation. Later callbacks or unwind
            // cannot make this transaction roll them back a second time.
            self.committed = true;
            if closed && self.cell.accepted_status().is_none() {
                // A creation that never became Accepted is invisible to
                // observers. Accepted tasks already have a source fact and
                // retain it until their physical convergence is observed.
                entry.source.withhold(self.identity);
            } else if !closed
                && !novarocks_execution::runtime::task_control::TaskExecutionPort::is_stopped(
                    status.as_ref(),
                )
            {
                let (_, publication) = status.prepare_installed();
                installed_publication = Some(publication);
            }
            // The exact Installed snapshot and Live record are installed
            // under this fence. Its detached notification is foreign work.
        }
        if let Some(publication) = installed_publication {
            publication.publish();
        }
        if let Some(root) = sealed_root {
            root.finish_seal();
        }
        // Actual preparation exit hands this installed runtime over and
        // applies its one accepted Task cause before activation.
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
        self.registry.gate.notify_all();
        closed.then_some((runnable, status))
    }
}

impl Drop for CreationTransaction<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let accepted = self.cell.accepted_status();
        {
            let scope = self.cell.preparation_scope();
            scope.request_rollback();
            scope.cleanup(crate::task_preparation::PreparationCleanup::Inbound, || {
                if self.capability_installed || accepted.is_some() {
                    self.registry
                        .task_host
                        .remove_inbound_capability(&self.descriptor);
                }
            });
            scope.cleanup(
                crate::task_preparation::PreparationCleanup::Receiver,
                || {
                    if self.receiver_installed {
                        self.registry.task_host.remove_receiver(&self.descriptor);
                    }
                },
            );
        }
        let failure = self
            .failure
            .clone()
            .or_else(|| self.cell.failure())
            .unwrap_or_else(|| {
                // No producer failure was observed. This is only the unfinished
                // operation's protocol verdict, never a synthetic Task cause.
                CreationFailure::verdict(
                    OperationOutcome::InvalidStateOrRequest,
                    "the creation transaction did not complete",
                )
            });
        // The original reservation survives transaction rollback. Only the
        // physical body/input/opaque exit owner may settle Creating or P.
        self.cell.finish_rollback(failure);
        self.registry.gate.notify_all();
    }
}

// ------------------------------------------------------------ free functions

fn map_create_status(
    receipt: CreateTaskOutcome,
) -> OperationReceipt<novarocks_execution_contract::TaskStatus> {
    receipt.map_ack(|acknowledgement| acknowledgement.current_status().clone())
}

fn create_request_structural_bytes(request: &CreateTask) -> usize {
    let mut bytes = std::mem::size_of::<CreateTask>()
        .saturating_add(std::mem::size_of_val(
            request.descriptor().split_plan_nodes(),
        ))
        .saturating_add(std::mem::size_of_val(request.initial_domains()));
    let topology = request.descriptor().topology();
    bytes = bytes
        .saturating_add(std::mem::size_of_val(topology.outbound()))
        .saturating_add(std::mem::size_of_val(topology.inbound()));
    for edge in topology.outbound() {
        bytes = bytes.saturating_add(std::mem::size_of_val(edge.destinations()));
    }
    for inbound in topology.inbound() {
        bytes = bytes.saturating_add(std::mem::size_of_val(inbound.sources()));
    }
    for domain in request.initial_domains() {
        let extra = match domain {
            novarocks_execution_contract::TaskDomainUpdate::SplitAssignment(intent) => {
                intent.payload().encoded_len()
            }
            novarocks_execution_contract::TaskDomainUpdate::TaskDynamicFilter {
                payload, ..
            } => payload.encoded_len(),
            novarocks_execution_contract::TaskDomainUpdate::OpenExchangeEdges { edges, .. } => {
                std::mem::size_of_val(edges.as_slice())
            }
            novarocks_execution_contract::TaskDomainUpdate::CloseExchangeDestination { .. } => {
                std::mem::size_of::<novarocks_execution_contract::TaskIdentity>()
                    + std::mem::size_of::<novarocks_execution_contract::ExchangeEdgeId>()
            }
        };
        bytes = bytes.saturating_add(extra);
    }
    bytes
}

fn effective_wait(
    caps: crate::OperationWaitCaps,
    envelope: OperationEnvelope,
    local_wait_cap: Option<Duration>,
) -> Duration {
    match local_wait_cap {
        Some(local_cap) => {
            // Native already validated the original wire wait and passed its
            // remaining time. Neutral constructors currently retain a default
            // wait; it must not narrow a longer legal request.
            let maximum = novarocks_execution_contract::MaxWait::new(
                novarocks_execution_contract::MaxWait::MAX_REPRESENTABLE,
            )
            .expect("the representable maximum is a valid wait");
            local_cap.min(caps.clamp(envelope.kind(), maximum))
        }
        None => caps.clamp(envelope.kind(), envelope.max_wait()),
    }
}

#[cfg(test)]
mod registry_lock_observation_tests {
    use super::{ObservedRegistryGuard, ObservedRegistryMutex};
    use std::sync::Condvar;
    use std::time::Duration;

    #[test]
    fn actual_mutex_acquisition_and_guard_lifetime_are_observed() {
        let mutex = ObservedRegistryMutex::new(());
        assert_eq!(mutex.observation.snapshot().sample_age_nanoseconds, None);
        let guard = mutex.lock().expect("lock observed mutex");
        let snapshot = mutex.observation.snapshot();
        assert_eq!(snapshot.wait_samples, 1);
        assert_eq!(snapshot.hold_samples, 0);
        drop(guard);
        let snapshot = mutex.observation.snapshot();
        assert_eq!(snapshot.hold_samples, 1);
        assert!(snapshot.sample_age_nanoseconds.is_some());
    }

    #[test]
    fn intentional_condition_wait_is_not_reported_as_mutex_contention() {
        let mutex = ObservedRegistryMutex::new(());
        let guard = mutex.lock().expect("lock observed mutex");
        let (raw, _) = Condvar::new()
            .wait_timeout(guard.into_inner(), Duration::from_millis(1))
            .expect("condition wait");
        drop(ObservedRegistryGuard::from_condvar(raw, &mutex.observation));
        let snapshot = mutex.observation.snapshot();
        assert_eq!(snapshot.wait_samples, 1);
        assert_eq!(snapshot.hold_samples, 2);
    }
}

#[cfg(test)]
mod preparation_snapshot_tests {
    use super::*;
    use novarocks_execution_contract::task_execution::operation::TaskDomainUpdate;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryId, StageId, TaskId,
    };
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::mpsc;

    // Sampling must never invoke an execution port. These hosts deliberately
    // panic instead of supplying lifecycle behavior to the observation tests.
    struct UncalledPorts;

    impl QueryContextHost for UncalledPorts {
        fn materialize(&self, _: SharedFactsRequest<'_>) -> Result<(), crate::HostRejection> {
            panic!("preparation observation called materialize")
        }
        fn release(&self, _: QueryContextRef) -> ReleasedContextEvidence {
            panic!("preparation observation called release")
        }
        fn advance_shared_domain(
            &self,
            _: QueryContextRef,
            _: &QueryContextDomainUpdate,
        ) -> Result<(), crate::HostRejection> {
            panic!("preparation observation advanced shared domain")
        }
    }

    impl TaskExecutionHost for UncalledPorts {
        fn close_context_admission(&self, _: QueryContextRef) {
            panic!("preparation observation closed admission")
        }
        fn retire_context_execution(&self, _: QueryContextRef) {
            panic!("preparation observation retired context")
        }
        fn forget_context_admission(&self, _: QueryContextRef) {
            panic!("preparation observation forgot context")
        }
        fn install_receiver(
            &self,
            _: &TaskDescriptor,
            _: TaskCreationInput,
            _preparation: crate::TaskPreparationContext<'_>,
        ) -> Result<crate::PreparedTaskInstallation, crate::HostRejection> {
            panic!("preparation observation installed receiver")
        }
        fn remove_receiver(&self, _: &TaskDescriptor) {
            panic!("preparation observation removed receiver")
        }
        fn install_inbound_capability(
            &self,
            _: &TaskDescriptor,
        ) -> Result<(), crate::HostRejection> {
            panic!("preparation observation installed inbound capability")
        }
        fn remove_inbound_capability(&self, _: &TaskDescriptor) {
            panic!("preparation observation removed inbound capability")
        }
        fn submit_runnable(
            &self,
            _: &TaskDescriptor,
            _: TaskStatusReporter,
        ) -> Result<Arc<dyn RunnableTask>, crate::HostRejection> {
            panic!("preparation observation submitted runnable")
        }
        fn apply_task_domain(
            &self,
            _: &TaskDescriptor,
            _: &TaskDomainUpdate,
            _operation: novarocks_execution_contract::TaskOperationId,
        ) -> Result<Option<u64>, crate::HostRejection> {
            panic!("preparation observation applied task domain")
        }
    }

    impl crate::TaskProtocolObserver for UncalledPorts {
        fn observe(&self, _: TaskProtocolEvent) {
            panic!("preparation observation emitted protocol event")
        }
    }
    impl crate::TaskResultLifecycle for UncalledPorts {
        fn discard_task(&self, _: TaskIdentity) {
            panic!("preparation observation discarded result")
        }
        fn retire_task_result(&self, _: TaskIdentity) {
            panic!("preparation observation retired result")
        }
    }
    impl crate::TaskExecutionMetrics for UncalledPorts {
        fn record_task_created(&self) {
            panic!("preparation observation created task")
        }
    }

    fn registry() -> Arc<TaskExecutionRegistry> {
        let config = TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 17, 9);
        TaskExecutionRegistry::new(
            config,
            Arc::new(crate::ManualClock::new()),
            Arc::new(UncalledPorts),
            Arc::new(UncalledPorts),
            crate::TaskExecutionPorts::new(
                Arc::new(UncalledPorts),
                Arc::new(UncalledPorts),
                Arc::new(UncalledPorts),
                Arc::new(
                    novarocks_execution::runtime::execution_health::ExecutionHealth::default(),
                ),
            ),
        )
    }

    #[test]
    fn preparation_panic_freezes_one_nominal_cause_and_keeps_an_existing_original_cache() {
        use novarocks_execution::runtime::fragment::{ExecutionFailure, ExecutionFailureCause};
        use novarocks_execution_contract::TaskConclusionPort;
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_execution_contract::operation::{DomainFailureOutcome, OperationResult};
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct ActualPayload(Arc<AtomicUsize>);
        impl Drop for ActualPayload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }
        for with_original in [false, true] {
            let registry = registry();
            let execution =
                QueryExecutionId::new(QueryId::new(31, 41), AttemptId::new(1).unwrap()).unwrap();
            let identity = TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                registry.config.backend_process_id,
            );
            let owner = Arc::new(TaskStatusOwner::new(
                identity,
                Arc::new(TaskStatusSource::new()),
                Arc::clone(&registry.clock),
                Duration::ZERO,
                registry.ports.execution_health(),
            ));
            let cell = Arc::new(CreationCell::for_accepted_fixture(&owner));
            cell.accept(Arc::clone(&owner));
            let original = with_original.then(|| {
                let source: ExecutionFailure =
                    novarocks_functions::KernelFailure::ResourceExhausted.into();
                let rejection = crate::HostRejection::from_failure(
                    TaskFailureCategory::ResourceExhausted,
                    source.clone(),
                );
                cell.fail(CreationFailure::domain(
                    DomainFailureOutcome::ResourceExhausted,
                    rejection.operation_failure(),
                ));
                owner.report_preparation_body_failure(rejection.into_failure());
                source
            });
            let drops = Arc::new(AtomicUsize::new(0));
            let payload = catch_unwind(AssertUnwindSafe(|| {
                std::panic::panic_any(ActualPayload(Arc::clone(&drops)));
            }))
            .unwrap_err();
            let failure = registry.preparation_panicked(
                &cell,
                crate::task_preparation::PreparationUnwindRole::Preparation,
                payload,
            );
            assert_eq!(
                drops.load(Ordering::Acquire),
                1,
                "actual original Any was disposed once"
            );
            let receipt: OperationReceipt<()> = failure.receipt(TaskOperationId::new_v7());
            let OperationResult::DomainFailure { outcome, failure } = receipt.result() else {
                panic!("a real preparation author has mandatory failure facts");
            };
            let accepted = owner
                .accepted()
                .expect("the original Task owner accepted a cause");
            let actual = accepted.originating_failure().unwrap();
            if let Some(original) = original {
                assert_eq!(*outcome, DomainFailureOutcome::ResourceExhausted);
                assert_eq!(failure.fact(), original.failure_fact());
                assert!(
                    std::ptr::eq(actual.cause(), original.cause()),
                    "late panic cannot replace the first cause"
                );
            } else {
                assert_eq!(*outcome, DomainFailureOutcome::InvalidStateOrRequest);
                assert_eq!(failure.fact(), PreparationPanicked.failure_fact());
                assert_eq!(failure.fact(), actual.failure_fact());
                assert_eq!(failure.detail().as_str(), "Task preparation panicked");
                let ExecutionFailureCause::Domain(source) = actual.cause() else {
                    panic!("retain the actual nominal preparation source");
                };
                assert!(source.downcast_ref::<PreparationPanicked>().is_some());
            }
            assert_eq!(
                cell.failure()
                    .unwrap()
                    .receipt::<()>(receipt.operation_id()),
                receipt
            );
            assert!(
                !owner.runtime_actual_stopped_received(),
                "logical failure is not runtime exit"
            );
        }
    }

    fn assert_same_samples(before: RegistryLockSnapshot, after: RegistryLockSnapshot) {
        assert_eq!(before.wait_samples, after.wait_samples);
        assert_eq!(before.wait_nanoseconds, after.wait_nanoseconds);
        assert_eq!(before.wait_max_nanoseconds, after.wait_max_nanoseconds);
        assert_eq!(before.hold_samples, after.hold_samples);
        assert_eq!(before.hold_nanoseconds, after.hold_nanoseconds);
        assert_eq!(before.hold_max_nanoseconds, after.hold_max_nanoseconds);
    }

    #[test]
    fn busy_actual_registry_returns_without_waiting_or_fabricating_samples() {
        let registry = registry();
        let guard = registry.state.lock().expect("hold actual registry");
        let before = registry.registry_lock_observation().snapshot();
        let sampling = Arc::clone(&registry);
        let (sent, received) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let attempts = (0..16)
                .map(|_| sampling.try_preparation_snapshot())
                .collect::<Vec<_>>();
            sent.send((attempts, sampling.registry_lock_observation().snapshot()))
                .expect("send observation results");
        });
        let result = received.recv_timeout(Duration::from_secs(2));
        // Always release and join before reporting a regression. A blocking
        // implementation must fail the oracle without hanging the test suite.
        drop(guard);
        worker.join().expect("join observation worker");
        let (attempts, during) = result.expect("sampling must finish while registry is held");
        assert!(attempts.iter().all(|result| *result == Ok(None)));
        assert_same_samples(before, during);
        assert!(registry.try_preparation_snapshot().unwrap().is_some());
    }

    #[test]
    fn successful_try_snapshot_matches_the_same_nonempty_owned_ledger_and_observation() {
        let registry = registry();
        let execution =
            QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap();
        let backend = registry.config.backend_process_id;
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        {
            // Seed the real owner's ledger under its mutex. This verifies
            // measurement, not task-admission or preparation-job lifecycle.
            let mut state = registry.state.lock().unwrap();
            for (id, bytes) in [(1, 17), (2, 36)] {
                let task = TaskIdentity::new(
                    execution,
                    StageId::new(1).unwrap(),
                    TaskId::new(id).unwrap(),
                    backend,
                );
                state.prepare_charges.insert(task, bytes);
            }
            state.prepare_context_counts.insert(context, 2);
            state.prepare_workers = 1;
            state.prepare_bytes = 53;
        }
        let expected = TaskPreparationSnapshot {
            positions: 2,
            position_limit: registry.config.max_preparing_tasks,
            context_positions: 2,
            context_position_limit: registry.config.max_preparing_tasks_per_context,
            queued_positions: 0,
            workers: 1,
            worker_limit: registry.config.max_prepare_workers,
            bytes: 53,
            byte_limit: registry.config.max_preparing_bytes,
        };
        let before = registry.registry_lock_observation().snapshot();
        assert_eq!(registry.try_preparation_snapshot(), Ok(Some(expected)));
        let after = registry.registry_lock_observation().snapshot();
        assert_eq!(after.wait_samples, before.wait_samples + 1);
        assert_eq!(after.hold_samples, before.hold_samples + 1);
        assert!(after.sample_age_nanoseconds.is_some());
        assert_eq!(registry.preparation_snapshot(), expected);
        {
            let mut state = registry.state.lock().unwrap();
            state.prepare_charges.clear();
            state.prepare_context_counts.clear();
            state.prepare_workers = 0;
            state.prepare_bytes = 0;
        }
        let released = registry.try_preparation_snapshot().unwrap().unwrap();
        assert_eq!(
            (
                released.positions,
                released.context_positions,
                released.workers,
                released.bytes
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(released, registry.preparation_snapshot());
    }

    #[test]
    fn poisoned_actual_registry_is_an_explicit_error_not_an_unavailable_snapshot() {
        let registry = registry();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _guard = registry.state.lock().unwrap();
                panic!("poison actual registry for observation test");
            }))
            .is_err()
        );
        let before = registry.registry_lock_observation().snapshot();
        assert_eq!(
            registry.try_preparation_snapshot(),
            Err(TaskPreparationSnapshotError::Poisoned)
        );
        assert_same_samples(before, registry.registry_lock_observation().snapshot());
        assert_eq!(
            TaskPreparationSnapshotError::Poisoned.to_string(),
            "task preparation snapshot registry mutex poisoned"
        );
        // The pre-existing blocking API keeps its original poison failure.
        assert!(catch_unwind(AssertUnwindSafe(|| registry.preparation_snapshot())).is_err());
    }
}

#[cfg(test)]
mod local_wait_tests {
    use super::effective_wait;
    use crate::OperationWaitCaps;
    use novarocks_execution_contract::{OperationEnvelope, OperationKind, TaskOperationId};
    use std::time::Duration;

    #[test]
    fn ingress_wait_uses_raw_remaining_time_above_neutral_default() {
        let caps = OperationWaitCaps::new(Duration::from_secs(30), Duration::from_secs(5))
            .expect("valid caps");
        let neutral = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        );
        assert_eq!(effective_wait(caps, neutral, None), Duration::from_secs(15));
        assert_eq!(
            effective_wait(caps, neutral, Some(Duration::from_secs(25))),
            Duration::from_secs(25),
            "the neutral default must not cut a longer legal wire wait"
        );
        assert_eq!(
            effective_wait(caps, neutral, Some(Duration::from_secs(40))),
            Duration::from_secs(30),
            "the Worker process cap still applies"
        );
    }
}

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

/// Answers a create from the task its identity already names.
///
/// The answer is the original entity's: its retained receipt, correlated to
/// this request's operation id, or the creation failure it fixed. Nothing the
/// request carries is read, because a repeated body is never a second
/// authority over a task that already exists. `None` means only that the
/// identity's creation round has not settled yet.
fn existing_create_reply(
    task: &TaskEntry,
    operation: TaskOperationId,
) -> Option<CreateTaskOutcome> {
    let (receipt, creation_failure) = match task {
        TaskEntry::Live(live) => (&live.receipt, live.creation_failure.as_ref()),
        TaskEntry::Retired(retired) => (&retired.receipt, retired.creation_failure.as_ref()),
        TaskEntry::Gone => {
            return Some(OperationReceipt::rejected(
                operation,
                OperationOutcome::Gone,
                "this identity's retained record was reclaimed",
            ));
        }
        TaskEntry::Creating(_) => return None,
    };
    Some(match creation_failure {
        Some(failure) => failure.receipt(operation),
        None => {
            OperationReceipt::acknowledged(operation, OperationOutcome::Idempotent, receipt.clone())
        }
    })
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
