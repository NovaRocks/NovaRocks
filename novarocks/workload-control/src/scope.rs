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

use crate::{
    CancellationReason, CancellationView, LocalResourceAuthority, ResourceClass, ResourceConfig,
    WorkError, WorkloadObservationHandle,
    admission::{PendingAdmission, Stage},
    cancellation::{Cancellation, CancellationRequestOutcome, CancellationSuccessSealOutcome},
    observation::{ControlIntents, ObligationKey, ObligationRecord, OwnerState},
    queue::FairQueue,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkId(pub(crate) u64);

impl WorkId {
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkClass {
    Query,
    MaterializedView,
    Statistics,
    TableMaintenance,
    Management,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingState {
    Initializing,
    Ready,
    Closed,
}

#[derive(Clone, Debug)]
pub struct WorkRequest {
    pub class: WorkClass,
    pub deadline: Option<Instant>,
}

impl WorkRequest {
    pub fn new(class: WorkClass) -> Self {
        Self {
            class,
            deadline: None,
        }
    }
}

/// Bounds on one process's governance metadata and policy admission.
/// Byte limits here bound retained queue declarations, not physical memory.
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub root_limit: usize,
    pub business_limit: usize,
    pub preparation_limit: usize,
    pub execution_limit: usize,
    pub executions_per_root: usize,
    pub waiting_limit: usize,
    pub waiting_bytes: u64,
    /// Bound one capacity wait without changing the logical work's deadline.
    pub capacity_wait_timeout: Duration,
    pub restarts_per_work: usize,
    pub old_attempts_per_work: usize,
    pub old_attempts_limit: usize,
    pub unknown_creates_limit: usize,
    /// Includes parent, child and unresolved orphan records.
    pub scope_records_limit: usize,
    /// Includes known work and external completion obligations.
    pub obligation_records_limit: usize,
    pub control_inflight_limit: usize,
    pub control_ready_limit: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            root_limit: 256,
            business_limit: 256,
            preparation_limit: 16,
            execution_limit: 64,
            executions_per_root: 4,
            waiting_limit: 1024,
            waiting_bytes: 64 * 1024 * 1024,
            capacity_wait_timeout: Duration::from_secs(30),
            restarts_per_work: 2,
            old_attempts_per_work: 2,
            old_attempts_limit: 128,
            unknown_creates_limit: 4096,
            scope_records_limit: 8192,
            obligation_records_limit: 8192,
            control_inflight_limit: 16,
            control_ready_limit: 256,
        }
    }
}

impl WorkloadConfig {
    pub fn validate(&self) -> Result<(), WorkError> {
        for (name, value) in [
            ("root_limit", self.root_limit),
            ("business_limit", self.business_limit),
            ("preparation_limit", self.preparation_limit),
            ("execution_limit", self.execution_limit),
            ("executions_per_root", self.executions_per_root),
            ("waiting_limit", self.waiting_limit),
            ("restarts_per_work", self.restarts_per_work),
            ("old_attempts_per_work", self.old_attempts_per_work),
            ("old_attempts_limit", self.old_attempts_limit),
            ("unknown_creates_limit", self.unknown_creates_limit),
            ("scope_records_limit", self.scope_records_limit),
            ("obligation_records_limit", self.obligation_records_limit),
            ("control_inflight_limit", self.control_inflight_limit),
            ("control_ready_limit", self.control_ready_limit),
        ] {
            if value == 0 || value > 1_000_000 {
                return Err(WorkError::InvalidConfig(name));
            }
        }
        if self.waiting_bytes == 0 || self.waiting_bytes > isize::MAX as u64 {
            return Err(WorkError::InvalidConfig("waiting_bytes"));
        }
        if self.capacity_wait_timeout.is_zero()
            || self.capacity_wait_timeout > Duration::from_secs(30)
        {
            return Err(WorkError::InvalidConfig("capacity_wait_timeout"));
        }
        if self.business_limit > self.root_limit || self.root_limit > self.scope_records_limit {
            return Err(WorkError::InvalidConfig("business/root/scope limits"));
        }
        if self.old_attempts_per_work > self.old_attempts_limit
            || self.old_attempts_limit > self.obligation_records_limit
            || self.unknown_creates_limit > self.obligation_records_limit
        {
            return Err(WorkError::InvalidConfig("residual/obligation limits"));
        }
        Ok(())
    }
}

pub(crate) struct Node {
    pub parent: Option<WorkId>,
    pub root: WorkId,
    pub class: WorkClass,
    pub cancellation: Arc<Cancellation>,
    pub children: usize,
    pub completed: bool,
    pub owner: OwnerState,
    pub handoffs: u64,
    pub business: bool,
    pub stages: BTreeSet<Stage>,
    pub pending_admissions: usize,
    pub root_executions: usize,
    pub obligations: BTreeMap<ObligationKey, ObligationRecord>,
    pub restarts: usize,
    pub resource_holders: usize,
    pub resource_waiters: usize,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
    pub result_credit: crate::ResultCreditSnapshot,
    pub control_pending: ControlIntents,
    pub cancellation_signalled: bool,
    pub control_queued: bool,
    pub control_inflight: bool,
}

impl Node {
    fn new(
        parent: Option<WorkId>,
        root: WorkId,
        class: WorkClass,
        cancellation: Arc<Cancellation>,
    ) -> Self {
        Self {
            parent,
            root,
            class,
            cancellation,
            children: 0,
            completed: false,
            owner: OwnerState::Active,
            handoffs: 0,
            business: false,
            stages: BTreeSet::new(),
            pending_admissions: 0,
            root_executions: 0,
            obligations: BTreeMap::new(),
            restarts: 0,
            resource_holders: 0,
            resource_waiters: 0,
            reserved_bytes: 0,
            used_bytes: 0,
            result_credit: crate::ResultCreditSnapshot::default(),
            control_pending: ControlIntents::empty(),
            cancellation_signalled: false,
            control_queued: false,
            control_inflight: false,
        }
    }

    pub(crate) fn check(&self) -> Result<(), WorkError> {
        if self.completed {
            return Err(WorkError::Released);
        }
        if let Some(reason) = self.cancellation.check_reason() {
            return Err(WorkError::Cancelled(reason));
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct State {
    pub closed: bool,
    pub ready: bool,
    pub next_id: u64,
    pub nodes: BTreeMap<WorkId, Node>,
    pub roots: usize,
    pub businesses: usize,
    pub preparation: usize,
    pub execution: usize,
    pub requests: BTreeMap<u64, PendingAdmission>,
    pub resource_waiters: ResourceWaiters,
    pub next_result_fetch_waiter_id: u64,
    pub next_decode_waiter_id: u64,
    pub next_protocol_waiter_id: u64,
    pub preparation_queue: FairQueue,
    pub execution_queue: FairQueue,
    pub waiting_bytes: u64,
    pub old_attempts: usize,
    pub unknown_creates: usize,
    pub obligations: usize,
    pub control_ready: VecDeque<WorkId>,
    pub control_waiting: BTreeSet<WorkId>,
    pub control_inflight: usize,
    pub control_cursor: Option<WorkId>,
    pub data_reserved: u64,
    pub data_used: u64,
    pub control_reserved: u64,
    pub control_used: u64,
    pub peak_held_bytes: u64,
    pub result_credit: crate::ResultCreditSnapshot,
    pub peak_waiting: usize,
    pub peak_waiting_records: usize,
    pub peak_waiting_bytes: u64,
    pub progress_revision: u64,
}

impl State {
    pub(crate) fn waiting_records(&self) -> usize {
        self.requests.len() + self.resource_waiters.len()
    }

    pub(crate) fn record_waiting_peak(&mut self) {
        self.peak_waiting_records = self.peak_waiting_records.max(self.waiting_records());
    }

    pub(crate) fn advance_progress_revision(&mut self) {
        self.progress_revision = self.progress_revision.wrapping_add(1);
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.closed
            && self.nodes.is_empty()
            && self.roots == 0
            && self.businesses == 0
            && self.preparation == 0
            && self.execution == 0
            && self.requests.is_empty()
            && self.resource_waiters.generic.is_empty()
            && self.resource_waiters.result_fetch.is_empty()
            && self.resource_waiters.decode.is_empty()
            && self.resource_waiters.protocol.is_empty()
            && self.waiting_bytes == 0
            && self.old_attempts == 0
            && self.unknown_creates == 0
            && self.obligations == 0
            && self.control_ready.is_empty()
            && self.control_waiting.is_empty()
            && self.control_inflight == 0
            && self.data_reserved == 0
            && self.data_used == 0
            && self.control_reserved == 0
            && self.control_used == 0
            && self.result_credit.held_bytes() == 0
    }

    pub(crate) fn next_id(&mut self) -> Result<u64, WorkError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_id)
    }

    pub(crate) fn next_protocol_waiter_id(&mut self) -> Result<u64, WorkError> {
        self.next_protocol_waiter_id = self
            .next_protocol_waiter_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_protocol_waiter_id)
    }

    pub(crate) fn next_decode_waiter_id(&mut self) -> Result<u64, WorkError> {
        self.next_decode_waiter_id = self
            .next_decode_waiter_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_decode_waiter_id)
    }

    pub(crate) fn next_result_fetch_waiter_id(&mut self) -> Result<u64, WorkError> {
        self.next_result_fetch_waiter_id = self
            .next_result_fetch_waiter_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_result_fetch_waiter_id)
    }

    pub(crate) fn collect(&mut self, mut id: WorkId) {
        loop {
            let Some(node) = self.nodes.get(&id) else {
                return;
            };
            if !node.completed
                || node.children != 0
                || node.business
                || !node.stages.is_empty()
                || node.pending_admissions != 0
                || !node.obligations.is_empty()
                || node.resource_holders != 0
                || node.resource_waiters != 0
                || !node.control_pending.is_empty()
                || node.control_queued
                || node.control_inflight
            {
                return;
            }
            let node = self.nodes.remove(&id).unwrap();
            node.cancellation.detach();
            if let Some(parent) = node.parent {
                self.nodes.get_mut(&parent).unwrap().children -= 1;
                id = parent;
            } else {
                self.roots -= 1;
                return;
            }
        }
    }
}

pub(crate) struct ResultWaiter {
    pub scope: WorkId,
    pub bytes: u64,
}

#[derive(Default)]
pub(crate) struct ResourceWaiters {
    pub generic: BTreeSet<(WorkId, ResourceClass)>,
    pub result_fetch: BTreeMap<u64, ResultWaiter>,
    pub result_fetch_by_scope: BTreeMap<WorkId, u64>,
    pub decode: BTreeMap<u64, ResultWaiter>,
    pub decode_by_scope: BTreeMap<WorkId, u64>,
    pub protocol: BTreeMap<u64, ResultWaiter>,
}

impl ResourceWaiters {
    pub(crate) fn len(&self) -> usize {
        self.generic.len() + self.result_fetch.len() + self.decode.len() + self.protocol.len()
    }
}

pub(crate) struct Inner {
    pub config: WorkloadConfig,
    pub resource_config: ResourceConfig,
    pub state: Mutex<State>,
    pub changed: Notify,
}

impl Inner {
    /// Allocation and observation facts do not drive policy queues. Keep their
    /// hot path independent of the number of scopes and admission requests.
    pub(crate) fn update_facts<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let result = {
            let mut state = self.state.lock().unwrap();
            let result = f(&mut state);
            state.advance_progress_revision();
            result
        };
        self.changed.notify_waiters();
        result
    }

    /// Mutate accounting facts that cannot make a capacity waiter runnable.
    /// Result packet state transitions use this path while capacity is held or
    /// reduced, avoiding a process-wide waiter wakeup for every packet step.
    pub(crate) fn update_facts_silent<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().unwrap();
        let result = f(&mut state);
        state.advance_progress_revision();
        result
    }

    pub(crate) fn notify_capacity_available(&self) {
        self.changed.notify_waiters();
    }

    pub(crate) fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let (result, wakers) = {
            let mut state = self.state.lock().unwrap();
            let result = f(&mut state);
            let wakers = crate::admission::dispatch(&mut state, &self.config);
            crate::observation::fill_control(&mut state, &self.config);
            state.advance_progress_revision();
            (result, wakers)
        };
        // Never invoke arbitrary wake implementations while holding the owner lock.
        for waker in wakers {
            waker.wake();
        }
        self.changed.notify_waiters();
        result
    }
}

/// Role-owned policy and control-progression authority.
///
/// This type is deliberately not cloneable. Role composition keeps it in the
/// process owner and injects only the narrower handles returned by
/// [`Self::try_new_split`] into long-lived services.
// Design: ADR-0147 (docs/adr/ADR-0147-process-local-work-governance-separates-responsibility-and-resources.md)
pub struct WorkloadControl {
    pub(crate) inner: Arc<Inner>,
}

/// Cloneable capability that can only begin a root business responsibility.
///
/// Holding this handle grants no readiness, drain, control-progression,
/// observation, recovery, or local-resource authority.
#[derive(Clone)]
pub struct RootAdmissionHandle {
    inner: Arc<Inner>,
}

/// Complete process-composition result for one workload authority.
///
/// The owner is unique. Each handle is intentionally a separate capability so
/// a business service cannot obtain process lifecycle authority by cloning the
/// object it was injected.
pub struct WorkloadControlParts {
    pub owner: WorkloadControl,
    pub root_admission: RootAdmissionHandle,
    pub observation: WorkloadObservationHandle,
    pub resources: LocalResourceAuthority,
}

/// Proof that the unique workload owner closed admission and observed a fully
/// drained local authority before relinquishing process ownership.
#[derive(Debug)]
pub struct WorkloadShutdown {
    _private: (),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadShutdownError {
    AdmissionOpen,
    NotDrained,
}

impl std::fmt::Display for WorkloadShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionOpen => f.write_str("New work admission is still open"),
            Self::NotDrained => f.write_str("Workload authority has not fully drained"),
        }
    }
}

impl std::error::Error for WorkloadShutdownError {}

/// A failed shutdown returns the unique owner so role composition can continue
/// driving control and convergence.
pub struct WorkloadShutdownFailure {
    error: WorkloadShutdownError,
    owner: WorkloadControl,
}

/// Monotonic process-local token used to await a later governance event.
///
/// Equality is the only supported interpretation. The counter may wrap after
/// `u64::MAX` authority transactions; callers must never derive elapsed work
/// or ordering distances from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkloadProgressRevision(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadProgress {
    ControlReady(WorkloadProgressRevision),
    StateChanged(WorkloadProgressRevision),
    Drained(WorkloadProgressRevision),
}

impl WorkloadProgress {
    pub fn revision(self) -> WorkloadProgressRevision {
        match self {
            Self::ControlReady(revision)
            | Self::StateChanged(revision)
            | Self::Drained(revision) => revision,
        }
    }
}

impl WorkloadShutdownFailure {
    pub fn error(&self) -> WorkloadShutdownError {
        self.error
    }

    pub fn into_parts(self) -> (WorkloadShutdownError, WorkloadControl) {
        (self.error, self.owner)
    }
}

fn try_begin_root(inner: &Arc<Inner>, request: WorkRequest) -> Result<RootWork, WorkError> {
    inner.update(|state| {
        if state.closed {
            return Err(WorkError::Closed);
        }
        if !state.ready {
            return Err(WorkError::NotReady);
        }
        if state.roots >= inner.config.root_limit {
            return Err(WorkError::Capacity("root responsibilities"));
        }
        if state.businesses >= inner.config.business_limit {
            return Err(WorkError::Capacity("business admission"));
        }
        if state.nodes.len() >= inner.config.scope_records_limit {
            return Err(WorkError::Capacity("scope records"));
        }
        let cancellation = Cancellation::root(request.deadline);
        if let Some(reason) = cancellation.reason() {
            return Err(WorkError::Cancelled(reason));
        }
        let id = WorkId(state.next_id()?);
        let mut node = Node::new(None, id, request.class, cancellation);
        node.business = true;
        state.nodes.insert(id, node);
        state.roots += 1;
        state.businesses += 1;
        let scope = WorkScope {
            inner: Arc::clone(inner),
            id,
        };
        Ok(RootWork {
            owner: WorkOwner {
                scope: Some(scope.clone()),
            },
            business: BusinessPermit { scope: Some(scope) },
        })
    })
}

impl RootAdmissionHandle {
    pub fn try_begin_root(&self, request: WorkRequest) -> Result<RootWork, WorkError> {
        try_begin_root(&self.inner, request)
    }
}

impl WorkloadControl {
    /// Transitional owner-only constructor retained until the Frontend host
    /// atomically switches to [`Self::try_new_split`]. Do not inject this owner
    /// into product-lived services.
    pub fn try_new(config: WorkloadConfig, resources: ResourceConfig) -> Result<Self, WorkError> {
        config.validate()?;
        resources.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                resource_config: resources,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
            }),
        })
    }

    /// Construct one unique process owner and the complete set of narrow,
    /// cloneable service capabilities backed by that same local authority.
    pub fn try_new_split(
        config: WorkloadConfig,
        resources: ResourceConfig,
    ) -> Result<WorkloadControlParts, WorkError> {
        let owner = Self::try_new(config, resources)?;
        Ok(WorkloadControlParts {
            root_admission: owner.root_admission(),
            observation: owner.observation(),
            resources: owner.resources(),
            owner,
        })
    }

    pub fn root_admission(&self) -> RootAdmissionHandle {
        RootAdmissionHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn observation(&self) -> WorkloadObservationHandle {
        WorkloadObservationHandle::new(Arc::clone(&self.inner))
    }

    pub fn try_begin_root(&self, request: WorkRequest) -> Result<RootWork, WorkError> {
        try_begin_root(&self.inner, request)
    }

    /// Close new roots; existing work and control retain their authority.
    pub fn close_admission(&self) {
        self.inner.update(|state| state.closed = true);
    }

    /// Role composition opens data admission only after its required services
    /// are ready. Repeating readiness is harmless; closed admission never reopens.
    pub fn mark_ready(&self) -> Result<(), WorkError> {
        self.inner.update(|state| {
            if state.closed {
                return Err(WorkError::Closed);
            }
            state.ready = true;
            Ok(())
        })
    }

    pub fn resources(&self) -> LocalResourceAuthority {
        LocalResourceAuthority {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Adopt an orphan without manufacturing a stop or release fact.
    pub fn adopt(&self, id: WorkId) -> Result<WorkOwner, WorkError> {
        self.inner.update(|state| {
            let node = state.nodes.get_mut(&id).ok_or(WorkError::Released)?;
            if node.owner != OwnerState::Orphaned {
                return Err(WorkError::OwnerStillPresent);
            }
            node.owner = OwnerState::Active;
            Ok(WorkOwner {
                scope: Some(WorkScope {
                    inner: Arc::clone(&self.inner),
                    id,
                }),
            })
        })
    }

    /// Role supervision drives this together with `next_deadline`; no work owns
    /// an OS waiting thread, and timeout only creates cancellation intent.
    pub fn expire_deadlines(&self) {
        let notifications = self.inner.update(|state| {
            let cancelled = state
                .nodes
                .iter()
                .filter_map(|(&id, node)| {
                    (!node.cancellation_signalled && node.cancellation.check_reason().is_some())
                        .then_some(id)
                })
                .collect::<Vec<_>>();
            let mut notifications = Vec::new();
            for id in cancelled {
                let cancellation = Arc::clone(&state.nodes[&id].cancellation);
                let reason = cancellation.check_reason().unwrap();
                notifications.push((cancellation, reason));
                state.nodes.get_mut(&id).unwrap().cancellation_signalled = true;
                crate::observation::queue_control(state, id, crate::ControlIntent::Cancel).unwrap();
            }
            notifications
        });
        for (cancellation, reason) in notifications {
            cancellation.request(reason);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        let state = self.inner.state.lock().unwrap();
        state
            .nodes
            .values()
            .filter(|node| !node.cancellation_signalled)
            .filter_map(|node| node.cancellation.view().deadline())
            .chain(
                state
                    .requests
                    .values()
                    .filter(|request| {
                        !matches!(request.state, crate::admission::AdmissionState::Rejected(_))
                    })
                    .map(|request| request.wait_deadline),
            )
            .min()
    }

    /// Capture the current event revision before attempting a state-dependent
    /// owner operation such as shutdown.
    pub fn progress_revision(&self) -> WorkloadProgressRevision {
        WorkloadProgressRevision(self.inner.state.lock().unwrap().progress_revision)
    }

    /// Await a later authority event without dedicating a thread or polling.
    ///
    /// The subscription is armed before state inspection, so convergence
    /// between the caller's failed shutdown attempt and this future cannot be
    /// lost. A drained authority is returned immediately even when its revision
    /// equals `after`.
    pub async fn wait_progress(&self, after: WorkloadProgressRevision) -> WorkloadProgress {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = {
                let state = self.inner.state.lock().unwrap();
                let revision = WorkloadProgressRevision(state.progress_revision);
                if state.is_drained() {
                    Some(WorkloadProgress::Drained(revision))
                } else if !state.control_ready.is_empty()
                    && state.control_inflight < self.inner.config.control_inflight_limit
                {
                    Some(WorkloadProgress::ControlReady(revision))
                } else if revision != after {
                    Some(WorkloadProgress::StateChanged(revision))
                } else {
                    None
                }
            };
            if let Some(progress) = progress {
                return progress;
            }
            changed.await;
        }
    }

    /// Consume the unique owner after admission has closed and every local
    /// responsibility, waiter, control intent, and allocation has converged.
    ///
    /// On failure the owner is returned so the caller can continue driving
    /// convergence. This method never fabricates cancellation, stop, or
    /// release facts.
    pub fn shutdown(self) -> Result<WorkloadShutdown, WorkloadShutdownFailure> {
        let result = {
            let state = self.inner.state.lock().unwrap();
            if !state.closed {
                Err(WorkloadShutdownError::AdmissionOpen)
            } else if !state.is_drained() {
                Err(WorkloadShutdownError::NotDrained)
            } else {
                Ok(WorkloadShutdown { _private: () })
            }
        };
        result.map_err(|error| WorkloadShutdownFailure { error, owner: self })
    }
}

impl Drop for WorkloadControl {
    fn drop(&mut self) {
        // Losing the unique process owner must fail closed. Existing work and
        // cleanup capabilities remain valid and retain their real accounting.
        self.close_admission();
    }
}

pub struct RootWork {
    pub owner: WorkOwner,
    pub business: BusinessPermit,
}

/// Cloneable, non-forgeable attribution. Clones do not create work or permits.
#[derive(Clone)]
pub struct WorkScope {
    pub(crate) inner: Arc<Inner>,
    pub(crate) id: WorkId,
}

impl WorkScope {
    pub fn id(&self) -> WorkId {
        self.id
    }

    pub fn check(&self) -> Result<(), WorkError> {
        self.inner
            .state
            .lock()
            .unwrap()
            .nodes
            .get(&self.id)
            .ok_or(WorkError::Released)?
            .check()
    }

    pub fn cancellation(&self) -> Result<CancellationView, WorkError> {
        Ok(self
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .get(&self.id)
            .ok_or(WorkError::Released)?
            .cancellation
            .view())
    }

    /// Derivation preserves responsibility and the earliest parent deadline.
    /// It does not acquire another business or execution permit.
    pub fn child(&self, request: WorkRequest) -> Result<WorkOwner, WorkError> {
        self.inner.update(|state| {
            let parent = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            parent.check()?;
            if state.nodes.len() >= self.inner.config.scope_records_limit {
                return Err(WorkError::Capacity("scope records"));
            }
            let root = parent.root;
            let cancellation = parent.cancellation.child(request.deadline);
            if let Some(reason) = cancellation.reason() {
                return Err(WorkError::Cancelled(reason));
            }
            let id = WorkId(state.next_id()?);
            state.nodes.insert(
                id,
                Node::new(Some(self.id), root, request.class, cancellation),
            );
            state.nodes.get_mut(&self.id).unwrap().children += 1;
            Ok(WorkOwner {
                scope: Some(Self {
                    inner: Arc::clone(&self.inner),
                    id,
                }),
            })
        })
    }

    pub async fn wait_released(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&self.id)
            {
                return;
            }
            changed.await;
        }
    }
}

/// Exactly one product owner, or an explicitly recorded orphan. Dropping an
/// owner requests termination; only `complete` supplies its completion fact.
pub struct WorkOwner {
    scope: Option<WorkScope>,
}

impl WorkOwner {
    pub fn scope(&self) -> WorkScope {
        self.scope.as_ref().unwrap().clone()
    }

    /// Derive a cloneable capability that may only request cancellation for
    /// this responsibility and its descendants. The requester does not retain
    /// ownership, keep the scope alive, or acquire completion/release rights.
    pub fn cancellation_requester(&self) -> WorkCancellationRequester {
        let scope = self.scope.as_ref().unwrap();
        WorkCancellationRequester {
            inner: Arc::clone(&scope.inner),
            id: scope.id,
        }
    }

    pub fn success_sealer(&self) -> WorkSuccessSealer {
        let scope = self.scope.as_ref().unwrap();
        WorkSuccessSealer {
            inner: Arc::clone(&scope.inner),
            id: scope.id,
        }
    }

    pub fn cancel(&self, reason: CancellationReason) {
        self.cancellation_requester()
            .request(reason)
            .expect("an active work owner must retain its responsibility");
    }

    /// Record a responsibility transfer before moving this owner to the
    /// receiving product/supervisor. No second work item is registered.
    pub fn handoff(self) -> Result<Self, WorkError> {
        let scope = self.scope.as_ref().unwrap();
        scope.inner.update(|state| {
            let node = state.nodes.get_mut(&scope.id).ok_or(WorkError::Released)?;
            node.handoffs = node
                .handoffs
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            Ok(())
        })?;
        Ok(self)
    }

    pub fn complete(mut self) {
        let scope = self.scope.take().unwrap();
        scope.inner.update(|state| {
            let node = state.nodes.get_mut(&scope.id).unwrap();
            node.completed = true;
            node.owner = OwnerState::Completed;
            state.collect(scope.id);
        });
    }
}

/// Cloneable authority to request first-wins downward cancellation.
///
/// This capability cannot complete or hand off its owner responsibility and
/// cannot release business, stage, allocation, or obligation resources.
#[derive(Clone)]
pub struct WorkCancellationRequester {
    inner: Arc<Inner>,
    id: WorkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkCancellationRequestOutcome {
    Requested,
    AlreadyRequested(CancellationReason),
    SuccessSealed,
}

#[derive(Clone)]
pub struct WorkSuccessSealer {
    inner: Arc<Inner>,
    id: WorkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkSuccessSealOutcome {
    Sealed,
    AlreadySealed,
    Cancelled(CancellationReason),
}

impl WorkSuccessSealer {
    pub fn seal(&self) -> Result<WorkSuccessSealOutcome, WorkError> {
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            if node.parent.is_some() {
                return Err(WorkError::Conflict);
            }
            Arc::clone(&node.cancellation)
        };
        Ok(match cancellation.seal_success() {
            CancellationSuccessSealOutcome::Sealed => WorkSuccessSealOutcome::Sealed,
            CancellationSuccessSealOutcome::AlreadySealed => WorkSuccessSealOutcome::AlreadySealed,
            CancellationSuccessSealOutcome::Cancelled(reason) => {
                WorkSuccessSealOutcome::Cancelled(reason)
            }
        })
    }
}

impl WorkCancellationRequester {
    /// Request cancellation for this work and its descendants. Repeated or
    /// competing requests preserve the cancellation tree's first reason.
    /// Cancellation is intent only and never releases governed resources.
    pub fn request(&self, reason: CancellationReason) -> Result<(), WorkError> {
        self.request_with_outcome(reason).map(|_| ())
    }

    /// Request cancellation and report the exact first-wins decision made by
    /// this workload authority.
    pub fn request_with_outcome(
        &self,
        reason: CancellationReason,
    ) -> Result<WorkCancellationRequestOutcome, WorkError> {
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            Arc::clone(
                &state
                    .nodes
                    .get(&self.id)
                    .ok_or(WorkError::Released)?
                    .cancellation,
            )
        };
        let outcome = cancellation.request(reason);
        if outcome != CancellationRequestOutcome::SuccessSealed {
            self.inner.update(|state| {
                let node = state.nodes.get_mut(&self.id).ok_or(WorkError::Released)?;
                node.cancellation_signalled = true;
                crate::observation::queue_control(state, self.id, crate::ControlIntent::Cancel)?;
                Ok(())
            })?;
        }
        Ok(match outcome {
            CancellationRequestOutcome::Requested => WorkCancellationRequestOutcome::Requested,
            CancellationRequestOutcome::AlreadyRequested(reason) => {
                WorkCancellationRequestOutcome::AlreadyRequested(reason)
            }
            CancellationRequestOutcome::SuccessSealed => {
                WorkCancellationRequestOutcome::SuccessSealed
            }
        })
    }
}

impl Drop for WorkOwner {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            let cancellation =
                Arc::clone(&scope.inner.state.lock().unwrap().nodes[&scope.id].cancellation);
            let cancellation_outcome = cancellation.request(CancellationReason::OwnerDropped);
            scope.inner.update(|state| {
                if let Some(node) = state.nodes.get_mut(&scope.id) {
                    node.owner = OwnerState::Orphaned;
                    if cancellation_outcome != CancellationRequestOutcome::SuccessSealed {
                        node.cancellation_signalled = true;
                        crate::observation::queue_control(
                            state,
                            scope.id,
                            crate::ControlIntent::Cancel,
                        )
                        .unwrap();
                    }
                }
            });
        }
    }
}

/// The product-defined business lifecycle is distinct from the responsibility
/// tree. Release this at that boundary, including during a longer cleanup tail.
pub struct BusinessPermit {
    scope: Option<WorkScope>,
}

impl BusinessPermit {
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for BusinessPermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.inner.update(|state| {
                state.nodes.get_mut(&scope.id).unwrap().business = false;
                state.businesses -= 1;
                state.collect(scope.id);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };

    struct InspectWake(Arc<Inner>);

    impl Wake for InspectWake {
        fn wake(self: Arc<Self>) {
            assert!(
                self.0.state.try_lock().is_ok(),
                "Wake callback ran under the work owner lock"
            );
        }
    }

    fn controller() -> WorkloadControl {
        let control = WorkloadControl::try_new_split(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 128,
                control_bytes: 16,
                per_scope_bytes: 112,
            },
        )
        .unwrap()
        .owner;
        control.mark_ready().unwrap();
        control
    }

    #[tokio::test]
    async fn cancellation_wake_may_reenter_the_work_owner() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = work.owner.scope().cancellation().unwrap();
        let mut cancelled = Box::pin(cancellation.cancelled());
        let waker = Waker::from(Arc::new(InspectWake(Arc::clone(&control.inner))));
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        work.owner.cancel(CancellationReason::Requested);
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn identity_exhaustion_is_rejected_without_reusing_an_id_or_charging_work() {
        let control = controller();
        control.inner.state.lock().unwrap().next_id = u64::MAX;
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::ArithmeticOverflow)
        ));
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(control.snapshot().businesses, 0);
    }
}
