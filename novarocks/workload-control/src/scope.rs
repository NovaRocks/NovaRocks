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

mod cancellation_delivery;
mod retirement;
pub use retirement::WorkRetirementObserver;

#[cfg(test)]
use crate::cancellation::Cancellation;
use crate::control_storage::{CancellationStorage, SlotId};
use crate::{
    CancellationReason, CancellationView, LocalResourceAuthority, ResourceClass, ResourceConfig,
    WorkError, WorkloadObservationHandle,
    admission::{PendingAdmission, Stage},
    cancellation::{CancellationRequestOutcome, CancellationSuccessSealOutcome},
    observation::{
        ControlIntents, ObligationKey, ObligationRecord, OwnerState, RootLifecycleSnapshot,
    },
    queue::FairQueue,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
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

impl WorkClass {
    /// Work that can initiate warehouse computation shares the one query
    /// concurrency queue. Management requests retain their own protocol
    /// semantics and never consume this slot.
    pub const fn uses_warehouse_concurrency(self) -> bool {
        matches!(
            self,
            Self::Query | Self::MaterializedView | Self::Statistics | Self::TableMaintenance
        )
    }
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
    /// Concurrent logical queries admitted by the warehouse. This is separate
    /// from the legacy protocol-business limit while production callers move
    /// to `begin_query_root`.
    pub query_concurrency_limit: usize,
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
            query_concurrency_limit: 256,
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
            ("query_concurrency_limit", self.query_concurrency_limit),
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
            if value == 0 {
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
    pub(crate) internal_result_bytes:
        std::sync::Weak<crate::result_window::retained_bytes::RetainedBytes>,
    pub(crate) root_domains: Option<crate::joint_admission::ledger::root_domains::RootDomains>,
    pub parent: Option<WorkId>,
    pub root: WorkId,
    pub class: WorkClass,
    pub cancellation: SlotId,
    pub children: usize,
    pub completed: bool,
    pub owner: OwnerState,
    pub handoffs: u64,
    pub business: bool,
    pub query_admitted: bool,
    pub stages: BTreeSet<Stage>,
    pub pending_admissions: usize,
    pub root_executions: usize,
    pub obligations: BTreeMap<ObligationKey, ObligationRecord>,
    pub restarts: usize,
    pub resource_holders: usize,
    pub resource_waiters: usize,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
    pub result_windows: crate::ResultCapacitySnapshot,
    /// The original root has one queued or not-yet-received query admission,
    /// including admissions that request no result window.
    pub pending_query: bool,
    /// The result window class a queued query admission must take together
    /// with its computation permit; `None` when it is not queued or takes none.
    pub pending_query_window: Option<crate::ResultWindowClass>,
    pub control_pending: ControlIntents,
    pub cancellation_signalled: bool,
    cancellation_notification: cancellation_delivery::Notification,
    pub terminal_cancel_settled: bool,
    pub control_queued: bool,
    pub control_inflight: bool,
}

impl Node {
    fn new(parent: Option<WorkId>, root: WorkId, class: WorkClass, cancellation: SlotId) -> Self {
        Self {
            internal_result_bytes: std::sync::Weak::new(),
            root_domains: None,
            parent,
            root,
            class,
            cancellation,
            children: 0,
            completed: false,
            owner: OwnerState::Active,
            handoffs: 0,
            business: false,
            query_admitted: false,
            stages: BTreeSet::new(),
            pending_admissions: 0,
            root_executions: 0,
            obligations: BTreeMap::new(),
            restarts: 0,
            resource_holders: 0,
            resource_waiters: 0,
            reserved_bytes: 0,
            used_bytes: 0,
            result_windows: crate::ResultCapacitySnapshot::default(),
            pending_query: false,
            pending_query_window: None,
            control_pending: ControlIntents::empty(),
            cancellation_signalled: false,
            cancellation_notification: cancellation_delivery::Notification::Idle,
            terminal_cancel_settled: false,
            control_queued: false,
            control_inflight: false,
        }
    }

    pub(crate) fn check(&self, storage: &CancellationStorage) -> Result<(), WorkError> {
        if self.completed {
            return Err(WorkError::Released);
        }
        if let Some(reason) = storage.cell(self.cancellation).check_reason() {
            return Err(WorkError::Cancelled(reason));
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) internal_cpu_factory:
        Option<crate::joint_admission::InternalResultClosedFactoryDescriptor>,
    pub(crate) joint: Option<crate::joint_admission::ledger::JointLedger>,
    pub(crate) cancellations: CancellationStorage,
    pub root_admission_closed: bool,
    pub closed: bool,
    pub ready: bool,
    pub next_id: u64,
    pub nodes: BTreeMap<WorkId, Node>,
    pub roots: usize,
    pub businesses: usize,
    pub admitted_queries: usize,
    pub preparation: usize,
    pub execution: usize,
    pub requests: BTreeMap<u64, PendingAdmission>,
    pub resource_waiters: BTreeSet<(WorkId, ResourceClass)>,
    pub preparation_queue: FairQueue,
    pub execution_queue: FairQueue,
    pub query_queue: FairQueue,
    pub waiting_bytes: u64,
    pub old_attempts: usize,
    pub unknown_creates: usize,
    pub obligations: usize,
    pub obligation_endings: crate::ObligationEndSnapshot,
    pub control_ready: VecDeque<WorkId>,
    pub control_waiting: BTreeSet<WorkId>,
    pub control_inflight: usize,
    pub root_lifecycle: RootLifecycleSnapshot,
    pub control_cursor: Option<WorkId>,
    pub data_reserved: u64,
    pub data_used: u64,
    pub control_reserved: u64,
    pub control_used: u64,
    pub peak_held_bytes: u64,
    pub result_capacity: Option<crate::ResultCapacityConfig>,
    pub result_windows: crate::ResultCapacitySnapshot,
    pub peak_waiting: usize,
    pub peak_waiting_records: usize,
    pub peak_waiting_bytes: u64,
    pub progress_revision: u64,
}

impl State {
    pub(crate) fn root_admission_is_closed(&self) -> bool {
        self.root_admission_closed || self.closed
    }

    fn take_root_cancellation(
        &mut self,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        let slot = self.cancellations.take_root(class, id, deadline)?;
        if let Some(claim) = self.cancellations.slots[slot.0].claim
            && let Err(error) = self
                .joint
                .as_mut()
                .unwrap()
                .activate_control_slot(slot, claim, class)
        {
            self.cancellations.retire(slot);
            return Err(error);
        }
        Ok(slot)
    }

    fn take_child_cancellation(
        &mut self,
        parent: SlotId,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        if self.cancellations.fixed {
            let original = &self.cancellations.slots[parent.0];
            if original.class != class {
                return Err(WorkError::Conflict);
            }
            if !self
                .joint
                .as_ref()
                .unwrap()
                .control_is_empty(original.claim.unwrap())
            {
                // Bound parents use request_child and its original installed receipt.
                return Err(WorkError::NotReady);
            }
        }
        let slot = self.cancellations.take_child(parent, class, id, deadline)?;
        if let Some(claim) = self.cancellations.slots[slot.0].claim
            && let Err(error) = self
                .joint
                .as_mut()
                .unwrap()
                .activate_control_slot(slot, claim, class)
        {
            self.cancellations.retire(slot);
            return Err(error);
        }
        Ok(slot)
    }

    /// Publish only the exact slot already committed by the original proof.
    /// Every fallible preflight precedes registration of the cancellation edge.
    pub(crate) fn install_bound_child(
        &mut self,
        inner: &Arc<Inner>,
        parent: WorkId,
        slot: SlotId,
        deadline: Option<Instant>,
    ) -> Result<WorkOwner, WorkError> {
        let node = self.nodes.get(&parent).ok_or(WorkError::Released)?;
        node.check(&self.cancellations)?;
        let root = node.root;
        let class = node.class;
        let parent_slot = node.cancellation;
        let children = node
            .children
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        if self.nodes.len() >= inner.config.scope_records_limit {
            return Err(WorkError::Capacity("scope records"));
        }
        let id = WorkId(self.next_id()?);
        self.cancellations
            .install_granted_child(slot, parent_slot, id, deadline)?;
        self.nodes
            .insert(id, Node::new(Some(parent), root, class, slot));
        self.nodes.get_mut(&parent).unwrap().children = children;
        Ok(WorkOwner {
            scope: Some(WorkScope {
                inner: Arc::clone(inner),
                id,
            }),
        })
    }

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
            && self
                .joint
                .as_ref()
                .is_none_or(|ledger| ledger.is_drained(&self.cancellations))
            && self.cancellations.drained()
            && self.nodes.is_empty()
            && self.roots == 0
            && self.businesses == 0
            && self.admitted_queries == 0
            && self.preparation == 0
            && self.execution == 0
            && self.requests.is_empty()
            && self.resource_waiters.is_empty()
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
    }

    pub(crate) fn next_id(&mut self) -> Result<u64, WorkError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        Ok(self.next_id)
    }

    pub(crate) fn collect(&mut self, mut id: WorkId) {
        loop {
            let Some(node) = self.nodes.get(&id) else {
                return;
            };
            if !node.completed
                || node.children != 0
                || node.business
                || node.query_admitted
                || !node.stages.is_empty()
                || node.pending_admissions != 0
                || !node.obligations.is_empty()
                || node.resource_holders != 0
                || node.resource_waiters != 0
                || !node.control_pending.is_empty()
                || node.control_queued
                || node.control_inflight
                || !node.cancellation_notification.is_idle()
                || self.cancellations.slots[node.cancellation.0]
                    .polls
                    .iter()
                    .any(|role| role.phase == crate::role_poll::PollPhase::Mounted)
            {
                return;
            }
            let mut node = self.nodes.remove(&id).unwrap();
            if let Some(domains) = node.root_domains.take() {
                let slot = &mut self.cancellations.slots[node.cancellation.0];
                assert!(slot.retired_root_domains.is_none());
                slot.retired_root_domains = Some(domains);
            }
            self.cancellations.retire(node.cancellation);
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

pub(crate) struct Inner {
    pub config: WorkloadConfig,
    pub resource_config: Option<Arc<ResourceConfig>>,
    pub state: Mutex<State>,
    pub changed: Notify,
    reclaim_generation: AtomicU64,
}

/// A raw cancellation alias may outlive the State transaction that cloned it.
/// Declare this before every escaping alias so unwind drops those aliases first.
/// This weak observer cannot keep a Work node or the process authority alive.
pub(crate) struct ReclaimWakeOnExit {
    inner: Weak<Inner>,
}

impl ReclaimWakeOnExit {
    pub(crate) fn observe(inner: &Weak<Inner>) -> Self {
        Self {
            inner: inner.clone(),
        }
    }

    fn new(inner: &Arc<Inner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }
}

impl Drop for ReclaimWakeOnExit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.notify_reclaim_progress();
        }
    }
}

impl Inner {
    /// Capture both event sources while the caller holds the original State lock.
    /// Equality only: neither wrapping counter expresses elapsed work or order.
    pub(crate) fn capture_progress(&self, state: &State) -> WorkloadProgressRevision {
        WorkloadProgressRevision {
            state_revision: state.progress_revision,
            reclaim_generation: self.reclaim_generation.load(Ordering::Acquire),
        }
    }

    /// Called only after the notifying cancellation Arc has actually exited.
    /// Notify only outside State; this does not release resources or author terminal facts.
    pub(crate) fn notify_reclaim_progress(&self) {
        self.reclaim_generation.fetch_add(1, Ordering::Release);
        self.changed.notify_waiters();
    }

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

    /// Mutate reservation and allocation facts without waking capacity waiters.
    /// Capacity release separately notifies waiters after the actual holder exits.
    pub(crate) fn update_facts_silent<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().unwrap();
        let result = f(&mut state);
        state.advance_progress_revision();
        result
    }

    pub(crate) fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let result = {
            let mut state = self.state.lock().unwrap();
            let result = f(&mut state);
            crate::admission::dispatch(&mut state, &self.config);
            crate::observation::fill_control(&mut state, &self.config);
            state.advance_progress_revision();
            result
        };
        // Original rows retain each callback until its actual lock-free exit.
        crate::admission::drain_wakes(self);
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

/// Cloneable capability used only by role supervision to deliver elapsed
/// deadline cancellation. It cannot admit, release, or otherwise mutate
/// business responsibility.
#[derive(Clone)]
pub struct DeadlineExpiryHandle {
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

/// Process-composition capabilities for a role with no allocation budget.
pub struct CountedWorkloadControlParts {
    pub owner: WorkloadControl,
    pub root_admission: RootAdmissionHandle,
    pub observation: WorkloadObservationHandle,
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

/// Opaque process-local token used to await a later governance event.
///
/// Equality is the only supported interpretation. Both counters may wrap;
/// callers must never derive elapsed work or ordering distances from them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkloadProgressRevision {
    state_revision: u64,
    reclaim_generation: u64,
}

/// Notification for facts already committed in the original authority.
/// Carry it out of every receiving lock before calling notify. It owns no
/// capacity or completion right; Drop does not roll back or release anything.
#[must_use = "Notify committed progress after releasing the receiving owner's locks"]
pub struct WorkloadProgressNotification {
    inner: Arc<Inner>,
}

impl WorkloadProgressNotification {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    pub fn notify(self) {
        self.inner.notify_reclaim_progress();
    }
}

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
    try_begin_root_window(inner, request, None).map(|(root, _)| root)
}

fn try_begin_root_window(
    inner: &Arc<Inner>,
    request: WorkRequest,
    window: Option<crate::ResultWindowClass>,
) -> Result<(RootWork, Option<crate::ResultWindowGrant>), WorkError> {
    let mut destination = None;
    install_root_window(
        inner,
        request,
        window,
        RootDestination::Optional(&mut destination),
    )?;
    Ok(destination.expect("successful root publication installs its original owner"))
}

// Concrete receiving locations only: no caller callback runs under State.
// The receiver must live outside any destructible publication/receipt future.
enum RootDestination<'a> {
    Root(&'a mut Option<RootWork>),
    Window(&'a mut Option<(RootWork, crate::ResultWindowGrant)>),
    Optional(&'a mut Option<(RootWork, Option<crate::ResultWindowGrant>)>),
}

impl RootDestination<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Root(slot) => slot.is_none(),
            Self::Window(slot) => slot.is_none(),
            Self::Optional(slot) => slot.is_none(),
        }
    }

    fn install(self, root: RootWork, window: Option<crate::ResultWindowGrant>) {
        match self {
            Self::Root(slot) => {
                debug_assert!(window.is_none());
                *slot = Some(root);
            }
            Self::Window(slot) => {
                *slot = Some((root, window.expect("requested complete result window")));
            }
            Self::Optional(slot) => *slot = Some((root, window)),
        }
    }
}

fn install_root_window(
    inner: &Arc<Inner>,
    request: WorkRequest,
    window: Option<crate::ResultWindowClass>,
    destination: RootDestination<'_>,
) -> Result<(), WorkError> {
    if !destination.is_empty() {
        return Err(WorkError::Conflict);
    }
    inner.update(|state| {
        let class = request.class;
        let result = (|| {
            if window == Some(crate::ResultWindowClass::Closing)
                || (window.is_some() && class.uses_warehouse_concurrency())
            {
                return Err(WorkError::Conflict);
            }
            if state.root_admission_is_closed() {
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
            let id = WorkId(state.next_id()?);
            let cancellation = state.take_root_cancellation(class, id, request.deadline)?;
            let mut node = Node::new(None, id, class, cancellation);
            node.business = true;
            state.nodes.insert(id, node);
            state.roots += 1;
            state.businesses += 1;
            let scope = WorkScope {
                inner: Arc::clone(inner),
                id,
            };
            let grant = match window {
                None => None,
                Some(class) => match crate::result_window::reserve_window(state, &scope, class) {
                    Ok(grant) => Some(grant),
                    Err(error) => {
                        // No owner or permit has escaped this transaction.
                        let node = state.nodes.remove(&id).unwrap();
                        state.cancellations.retire(node.cancellation);
                        state.roots -= 1;
                        state.businesses -= 1;
                        return Err(error);
                    }
                },
            };
            destination.install(
                RootWork {
                    owner: WorkOwner {
                        scope: Some(scope.clone()),
                    },
                    business: BusinessPermit { scope: Some(scope) },
                },
                grant,
            );
            Ok(())
        })();
        if result.is_err() {
            state.root_lifecycle.rejected_admissions.increment(class);
        }
        result
    })
}

fn begin_warehouse_root(
    inner: &Arc<Inner>,
    request: WorkRequest,
) -> Result<PendingQueryRoot, WorkError> {
    let mut destination = None;
    install_warehouse_root(inner, request, &mut destination)?;
    Ok(destination.expect("successful warehouse publication installs its original owner"))
}

fn install_warehouse_root(
    inner: &Arc<Inner>,
    request: WorkRequest,
    destination: &mut Option<PendingQueryRoot>,
) -> Result<(), WorkError> {
    if destination.is_some() {
        return Err(WorkError::Conflict);
    }
    inner.update(|state| {
        let class = request.class;
        let result = (|| {
            if !class.uses_warehouse_concurrency() {
                return Err(WorkError::Conflict);
            }
            if state.root_admission_is_closed() {
                return Err(WorkError::Closed);
            }
            if !state.ready {
                return Err(WorkError::NotReady);
            }
            // Registering an unpolled request already retains a real scope.
            // Bound that responsibility before minting its identity, while
            // leaving computation concurrency to the original warehouse queue.
            if state.nodes.len() >= inner.config.scope_records_limit {
                return Err(WorkError::Capacity("scope records"));
            }
            let id = WorkId(state.next_id()?);
            let cancellation = state.take_root_cancellation(class, id, request.deadline)?;
            state
                .nodes
                .insert(id, Node::new(None, id, class, cancellation));
            state.roots += 1;
            *destination = Some(PendingQueryRoot {
                owner: WorkOwner {
                    scope: Some(WorkScope {
                        inner: Arc::clone(inner),
                        id,
                    }),
                },
            });
            Ok(())
        })();
        if result.is_err() {
            state.root_lifecycle.rejected_admissions.increment(class);
        }
        result
    })
}

impl RootAdmissionHandle {
    /// Publish the original root into an existing stable receiving location
    /// before post-transaction wakes can unwind. On an ordinary refusal the
    /// location stays empty; a nonempty location is never replaced.
    ///
    /// The receiving supervisor must retain this location outside its waiter
    /// future and still drive first binding, cancellation, and actual exit.
    /// This publication alone grants no external-I/O or execution capacity.
    pub fn try_begin_root_in(
        &self,
        request: WorkRequest,
        destination: &mut Option<RootWork>,
    ) -> Result<(), WorkError> {
        install_root_window(
            &self.inner,
            request,
            None,
            RootDestination::Root(destination),
        )
    }

    /// Publish both the original root and its complete result window into one
    /// stable location in the same authority transaction.
    pub fn try_begin_root_with_result_in(
        &self,
        request: WorkRequest,
        class: crate::ResultWindowClass,
        destination: &mut Option<(RootWork, crate::ResultWindowGrant)>,
    ) -> Result<(), WorkError> {
        install_root_window(
            &self.inner,
            request,
            Some(class),
            RootDestination::Window(destination),
        )
    }

    /// Register an original warehouse root directly in supervisor custody,
    /// before any arbitrary post-State wake. Warehouse concurrency is still
    /// acquired later through this same root's ordinary fair queue.
    pub fn begin_warehouse_root_in(
        &self,
        request: WorkRequest,
        destination: &mut Option<PendingQueryRoot>,
    ) -> Result<(), WorkError> {
        install_warehouse_root(&self.inner, request, destination)
    }

    pub fn try_begin_root(&self, request: WorkRequest) -> Result<RootWork, WorkError> {
        try_begin_root(&self.inner, request)
    }

    /// Admit nonqueued work and its complete result position in one authority
    /// transaction. Refusal publishes neither a root nor a business permit.
    pub fn try_begin_root_with_result(
        &self,
        request: WorkRequest,
        class: crate::ResultWindowClass,
    ) -> Result<(RootWork, crate::ResultWindowGrant), WorkError> {
        try_begin_root_window(&self.inner, request, Some(class)).map(|(root, window)| {
            (
                root,
                window.expect("requested result position is returned atomically"),
            )
        })
    }

    /// Register a logical query before it receives warehouse concurrency.
    /// The caller installs protocol cancellation from the returned owner, then
    /// awaits `WorkScope::admit_query` without creating a second root.
    pub fn begin_query_root(&self, request: WorkRequest) -> Result<PendingQueryRoot, WorkError> {
        if request.class != WorkClass::Query {
            return Err(WorkError::Conflict);
        }
        begin_warehouse_root(&self.inner, request)
    }

    /// Register one compute-producing warehouse root before it receives the
    /// shared query concurrency permit. Statistics, MV refresh and table
    /// maintenance use the same queue as foreground queries; management work
    /// remains outside this admission class.
    pub fn begin_warehouse_root(
        &self,
        request: WorkRequest,
    ) -> Result<PendingQueryRoot, WorkError> {
        begin_warehouse_root(&self.inner, request)
    }
}

impl WorkloadControl {
    /// Construct a workload owner with an explicit allocation budget.
    /// Inject narrow capabilities into product-lived services.
    pub fn try_new(config: WorkloadConfig, resources: ResourceConfig) -> Result<Self, WorkError> {
        config.validate()?;
        resources.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                resource_config: Some(Arc::new(resources)),
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                reclaim_generation: AtomicU64::new(0),
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
            resources: owner.resources()?,
            owner,
        })
    }

    /// Construct responsibility, admission and lifecycle control without a
    /// generic allocation authority. Result windows are installed separately.
    pub fn try_new_counted(
        config: WorkloadConfig,
    ) -> Result<CountedWorkloadControlParts, WorkError> {
        config.validate()?;
        let owner = Self {
            inner: Arc::new(Inner {
                config,
                resource_config: None,
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                reclaim_generation: AtomicU64::new(0),
            }),
        };
        Ok(CountedWorkloadControlParts {
            root_admission: owner.root_admission(),
            observation: owner.observation(),
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

    pub fn begin_query_root(&self, request: WorkRequest) -> Result<PendingQueryRoot, WorkError> {
        if request.class != WorkClass::Query {
            return Err(WorkError::Conflict);
        }
        begin_warehouse_root(&self.inner, request)
    }

    pub fn begin_warehouse_root(
        &self,
        request: WorkRequest,
    ) -> Result<PendingQueryRoot, WorkError> {
        begin_warehouse_root(&self.inner, request)
    }

    /// Returns the narrow process-supervision capability for observing new
    /// deadlines and delivering their cancellation intent without handing a
    /// long-lived task the unique workload owner.
    pub fn deadline_expiry_handle(&self) -> DeadlineExpiryHandle {
        DeadlineExpiryHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Close new roots while admitted work retains its ordinary authority.
    /// This one-way serving drain neither cancels work nor grants resources.
    pub fn close_root_admission(&self) {
        self.inner
            .update(|state| state.root_admission_closed = true);
    }

    /// Close new roots and ordinary work admission during execution shutdown.
    /// Existing settlement and cleanup retain their separately admitted authority.
    pub fn close_admission(&self) {
        self.inner.update(|state| {
            state.root_admission_closed = true;
            state.closed = true;
        });
    }

    /// Commit first-wins cancellation for the active roots in one State
    /// transaction, then propagate outside the lock. Callback reentrancy cannot
    /// change membership or the already-recorded decisions of this batch.
    /// Cancellation intent never proves actual exit or releases resources.
    pub fn cancel_active_roots(&self, reason: CancellationReason) -> usize {
        cancellation_delivery::cancel_active_roots(&self.inner, reason)
    }

    /// Role composition opens data admission only after its required services
    /// are ready. Repeating readiness is harmless; closed admission never reopens.
    pub fn mark_ready(&self) -> Result<(), WorkError> {
        self.inner.update(|state| {
            if state.root_admission_is_closed() {
                return Err(WorkError::Closed);
            }
            if state
                .cancellations
                .slots
                .iter()
                .any(|slot| slot.polls.iter().any(|role| !role.is_installed()))
            {
                return Err(WorkError::NotReady);
            }
            state.ready = true;
            Ok(())
        })
    }

    /// Obtain the explicitly installed allocation authority. Count-only role
    /// composition has no allocation budget and cannot mint this capability.
    pub fn resources(&self) -> Result<LocalResourceAuthority, WorkError> {
        let config = self
            .inner
            .resource_config
            .as_ref()
            .ok_or(WorkError::NotReady)?;
        Ok(LocalResourceAuthority {
            inner: Arc::clone(&self.inner),
            config: Arc::clone(config),
        })
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
        self.deadline_expiry_handle().expire_deadlines();
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadline_expiry_handle().next_deadline()
    }

    /// Capture the current event revision before attempting a state-dependent
    /// owner operation such as shutdown.
    pub fn progress_revision(&self) -> WorkloadProgressRevision {
        self.inner
            .capture_progress(&self.inner.state.lock().unwrap())
    }

    /// Await a later authority event without dedicating a thread or polling.
    ///
    /// The subscription is armed before state inspection, so convergence
    /// between the caller's failed shutdown attempt and this future cannot be
    /// lost. A drained authority is returned immediately even when its revision
    /// equals `after`.
    pub async fn wait_progress(&self, after: WorkloadProgressRevision) -> WorkloadProgress {
        loop {
            crate::admission::drain_wakes(&self.inner);
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = {
                let state = self.inner.state.lock().unwrap();
                let revision = self.inner.capture_progress(&state);
                if state.is_drained() {
                    Some(WorkloadProgress::Drained(revision))
                } else if !state.control_ready.is_empty()
                    && state.control_inflight < self.inner.config.control_inflight_limit
                {
                    Some(WorkloadProgress::ControlReady(revision))
                } else if revision != after {
                    Some(WorkloadProgress::StateChanged(revision))
                } else if crate::admission::has_pending_wakes(&state) {
                    // Only a real wait needs another finite frontier. Already
                    // observed progress never waits for global notification drain.
                    continue;
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

/// A registered logical query that has a cancellation authority but has not
/// yet received warehouse concurrency. Dropping its owner follows the normal
/// orphan/cancellation path; it never consumes a query permit by itself.
pub struct PendingQueryRoot {
    pub owner: WorkOwner,
}

/// Cloneable, non-forgeable attribution. Clones do not create work or permits.
#[derive(Clone)]
pub struct WorkScope {
    pub(crate) inner: Arc<Inner>,
    pub(crate) id: WorkId,
}

impl WorkScope {
    /// Observe only whether this original owner installed its responsibility
    /// service. This grants no source, resource or permission to start work.
    pub fn has_responsibility_service(&self) -> bool {
        self.inner.state.lock().unwrap().joint.is_some()
    }

    pub fn id(&self) -> WorkId {
        self.id
    }

    pub fn check(&self) -> Result<(), WorkError> {
        let state = self.inner.state.lock().unwrap();
        state
            .nodes
            .get(&self.id)
            .ok_or(WorkError::Released)?
            .check(&state.cancellations)
    }

    pub fn cancellation(&self) -> Result<CancellationView, WorkError> {
        let state = self.inner.state.lock().unwrap();
        let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
        Ok(state
            .cancellations
            .cell(node.cancellation)
            .view(&self.inner))
    }

    /// Derivation preserves responsibility and the earliest parent deadline.
    /// It does not acquire another business or execution permit.
    pub fn child(&self, request: WorkRequest) -> Result<WorkOwner, WorkError> {
        self.derive_child(Some(request))
    }

    /// Derive responsibility for an action of this same workload. Its class
    /// and earliest deadline remain those of the original parent; no caller
    /// projection can charge MV, statistics or maintenance work as Query.
    pub fn child_inherited(&self) -> Result<WorkOwner, WorkError> {
        self.derive_child(None)
    }

    fn derive_child(&self, request: Option<WorkRequest>) -> Result<WorkOwner, WorkError> {
        self.inner.update(|state| {
            let (root, request) = {
                let parent = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
                parent.check(&state.cancellations)?;
                (
                    parent.root,
                    request.unwrap_or_else(|| WorkRequest::new(parent.class)),
                )
            };
            if state.nodes.len() >= self.inner.config.scope_records_limit {
                return Err(WorkError::Capacity("scope records"));
            }
            // Mint the original identity before registering any cancellation
            // edge. State serializes this sequence with child publication.
            let id = WorkId(state.next_id()?);
            let parent_slot = state.nodes[&self.id].cancellation;
            let cancellation =
                state.take_child_cancellation(parent_slot, request.class, id, request.deadline)?;
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
    /// Verify the exact host and work before transferring its unique owner.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.scope
            .as_ref()
            .is_some_and(|held| held.id == scope.id && Arc::ptr_eq(&held.inner, &scope.inner))
    }

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

    pub fn complete(self) {
        self.complete_with_terminal_cancel_settled(false);
    }

    /// Records responsibility completion after the matching cancellation
    /// delivery has reached its terminal boundary.
    ///
    /// This is narrower than [`Self::complete`]: ordinary completion retains
    /// a pending cancellation notification until its dispatcher settles it.
    /// A logical-execution owner may use this only after it has observed the
    /// real attempt/output/resource terminal condition that makes a pending
    /// Cancel notification obsolete.
    pub fn complete_after_terminal_cancel_settled(self) {
        self.complete_with_terminal_cancel_settled(true);
    }

    fn complete_with_terminal_cancel_settled(mut self, terminal_cancel_settled: bool) {
        let scope = self.scope.take().unwrap();
        scope.inner.update(|state| {
            let (is_root, class, remove_ready_control, remove_waiting_control) = {
                let node = state.nodes.get_mut(&scope.id).unwrap();
                node.completed = true;
                node.owner = OwnerState::Completed;
                if terminal_cancel_settled {
                    node.terminal_cancel_settled = true;
                    node.control_pending.remove(crate::ControlIntent::Cancel);
                }
                let remove_waiting_control =
                    terminal_cancel_settled && node.control_pending.is_empty();
                let remove_ready_control = remove_waiting_control && node.control_queued;
                if remove_ready_control {
                    node.control_queued = false;
                }
                (
                    node.parent.is_none(),
                    node.class,
                    remove_ready_control,
                    remove_waiting_control,
                )
            };
            if remove_waiting_control {
                state.control_waiting.remove(&scope.id);
            }
            if remove_ready_control {
                state.control_ready.retain(|id| *id != scope.id);
            }
            if is_root && state.root_admission_is_closed() {
                state
                    .root_lifecycle
                    .completed_after_admission_closed
                    .increment(class);
            }
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
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            if node.parent.is_some() {
                return Err(WorkError::Conflict);
            }
            Arc::clone(state.cancellations.cell(node.cancellation))
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
        let _reclaim = ReclaimWakeOnExit::new(&self.inner);
        let cancellation = {
            let state = self.inner.state.lock().unwrap();
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            Arc::clone(state.cancellations.cell(node.cancellation))
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
            let _reclaim = ReclaimWakeOnExit::new(&scope.inner);
            let cancellation = {
                let state = scope.inner.state.lock().unwrap();
                Arc::clone(
                    state
                        .cancellations
                        .cell(state.nodes[&scope.id].cancellation),
                )
            };
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

/// One non-copyable warehouse query-concurrency grant. It is intentionally
/// independent of thread execution and remote cleanup; its owner releases it
/// when the logical query reaches a Frontend terminal outcome.
pub struct QueryConcurrencyPermit {
    pub(crate) scope: Option<WorkScope>,
}

impl QueryConcurrencyPermit {
    /// Verify both the host and exact admitted root before a runtime handoff.
    pub fn is_for_scope(&self, scope: &WorkScope) -> bool {
        self.scope
            .as_ref()
            .is_some_and(|held| held.id == scope.id && Arc::ptr_eq(&held.inner, &scope.inner))
    }

    /// Move one existing warehouse grant between two original root owners.
    /// Both source and receiver remain unchanged on refusal. No queue entry,
    /// grant or concurrency gap is created; callbacks belong to the caller
    /// after every receiving slot and lock has been settled.
    pub fn handoff_in(
        source: &mut Option<Self>,
        expected_source: &WorkScope,
        receiver: &WorkOwner,
        destination: &mut Option<Self>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        if destination.is_some() {
            return Err(WorkError::Conflict);
        }
        let original = source.as_ref().ok_or(WorkError::Released)?;
        if !original.is_for_scope(expected_source) {
            return Err(WorkError::Conflict);
        }
        let receiving_scope = receiver.scope();
        if expected_source.id == receiving_scope.id
            || !Arc::ptr_eq(&expected_source.inner, &receiving_scope.inner)
        {
            return Err(WorkError::Conflict);
        }
        let inner = Arc::clone(&expected_source.inner);
        let mut retired_view = None;
        inner.update_facts_silent(|state| {
            if state.closed {
                return Err(WorkError::Closed);
            }
            let sending = state
                .nodes
                .get(&expected_source.id)
                .ok_or(WorkError::Released)?;
            sending.check(&state.cancellations)?;
            let receiving = state
                .nodes
                .get(&receiving_scope.id)
                .ok_or(WorkError::Released)?;
            receiving.check(&state.cancellations)?;
            if sending.parent.is_some()
                || sending.root != expected_source.id
                || !sending.class.uses_warehouse_concurrency()
                || !sending.query_admitted
                || sending.pending_query
                || receiving.parent.is_some()
                || receiving.root != receiving_scope.id
                || !receiving.class.uses_warehouse_concurrency()
                || receiving.query_admitted
                || receiving.pending_query
                || state.admitted_queries == 0
            {
                return Err(WorkError::Conflict);
            }
            state
                .nodes
                .get_mut(&expected_source.id)
                .unwrap()
                .query_admitted = false;
            state
                .nodes
                .get_mut(&receiving_scope.id)
                .unwrap()
                .query_admitted = true;
            // The global count is intentionally untouched.
            let mut original = source.take().expect("validated original query grant");
            retired_view = original.scope.replace(receiving_scope);
            *destination = Some(original);
            Ok(())
        })?;
        drop(retired_view);
        Ok(WorkloadProgressNotification::new(inner))
    }

    pub fn release(self) {
        drop(self);
    }
}

impl Drop for QueryConcurrencyPermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.inner.update(|state| {
                state.nodes.get_mut(&scope.id).unwrap().query_admitted = false;
                state.admitted_queries -= 1;
                state.collect(scope.id);
            });
        }
    }
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

    #[tokio::test]
    async fn configured_query_count_above_million_uses_one_original_permit() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            query_concurrency_limit: 1_000_001,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let permit = root.owner.scope().admit_query().unwrap().await.unwrap();
        let snapshot = control.snapshot();
        assert_eq!(snapshot.admitted_queries, 1);
        assert_eq!(snapshot.root_responsibilities, 1);
        assert_eq!(snapshot.scopes.len(), 1);
        drop(permit);
        assert_eq!(control.snapshot().admitted_queries, 0);
        root.owner.complete();
        root.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
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

    fn single_query_controller() -> WorkloadControl {
        let control = WorkloadControl::try_new_split(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
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
    async fn query_handoff_has_no_gap_and_only_the_receiving_root_releases_the_grant() {
        let control = single_query_controller();
        let sql = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let job = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let next = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source_scope = sql.owner.scope();
        let mut source = Some(source_scope.admit_query().unwrap().await.unwrap());
        let mut target = None;
        let mut pending = Box::pin(next.owner.scope().admit_query().unwrap());
        let wake = Waker::from(Arc::new(InspectWake(Arc::clone(&source_scope.inner))));
        let mut cx = Context::from_waker(&wake);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        let notice =
            QueryConcurrencyPermit::handoff_in(&mut source, &source_scope, &job.owner, &mut target)
                .unwrap();
        assert!(source.is_none());
        assert!(target.as_ref().unwrap().is_for_scope(&job.owner.scope()));
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        notice.notify();
        sql.owner.complete_after_terminal_cancel_settled();
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(target.take());
        let next_permit = pending.await.unwrap();
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        drop(next_permit);
        job.owner.complete_after_terminal_cancel_settled();
        next.owner.complete_after_terminal_cancel_settled();
        assert_eq!(control.observation().snapshot().admitted_queries, 0);
    }

    #[tokio::test]
    async fn query_handoff_foreign_or_cancelled_receiver_preserves_both_slots() {
        let control = single_query_controller();
        let other = single_query_controller();
        let sql = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let other_root = other
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let foreign = other
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let job = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        assert_eq!(foreign.owner.scope().id(), job.owner.scope().id());
        let scope = sql.owner.scope();
        let mut source = Some(scope.admit_query().unwrap().await.unwrap());
        let mut destination = Some(foreign.owner.scope().admit_query().unwrap().await.unwrap());
        assert!(matches!(
            QueryConcurrencyPermit::handoff_in(&mut source, &scope, &job.owner, &mut destination),
            Err(WorkError::Conflict)
        ));
        assert!(source.as_ref().unwrap().is_for_scope(&scope));
        assert!(
            destination
                .as_ref()
                .unwrap()
                .is_for_scope(&foreign.owner.scope())
        );
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        assert_eq!(other.observation().snapshot().admitted_queries, 1);
        drop(destination.take());
        assert!(matches!(
            QueryConcurrencyPermit::handoff_in(
                &mut source,
                &scope,
                &foreign.owner,
                &mut destination
            ),
            Err(WorkError::Conflict)
        ));
        assert!(source.as_ref().unwrap().is_for_scope(&scope));
        assert!(destination.is_none());
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        assert_eq!(other.observation().snapshot().admitted_queries, 0);
        job.owner.cancel(CancellationReason::Requested);
        assert!(matches!(
            QueryConcurrencyPermit::handoff_in(&mut source, &scope, &job.owner, &mut destination),
            Err(WorkError::Cancelled(CancellationReason::Requested))
        ));
        assert!(source.as_ref().unwrap().is_for_scope(&scope));
        assert!(destination.is_none());
        drop(source);
        sql.owner.complete_after_terminal_cancel_settled();
        job.owner.complete_after_terminal_cancel_settled();
        foreign.owner.complete_after_terminal_cancel_settled();
        other_root.owner.complete_after_terminal_cancel_settled();
    }

    #[test]
    fn owner_scope_identity_rejects_the_same_numeric_id_from_another_host() {
        let first = controller();
        let second = controller();
        let a = first
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let b = second
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let a_scope = a.owner.scope();
        let b_scope = b.owner.scope();
        assert_eq!(a_scope.id(), b_scope.id());
        assert!(a.owner.is_for_scope(&a_scope));
        assert!(!a.owner.is_for_scope(&b_scope));
        assert!(!b.owner.is_for_scope(&a_scope));
        a.owner.complete_after_terminal_cancel_settled();
        b.owner.complete_after_terminal_cancel_settled();
        drop((a.business, b.business, a_scope, b_scope));
        first.close_admission();
        second.close_admission();
        assert!(first.shutdown().is_ok());
        assert!(second.shutdown().is_ok());
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

    #[tokio::test]
    async fn observation_waits_for_the_last_root_responsibility() {
        let control = controller();
        let observation = control.observation();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        let mut waiter = Box::pin(tokio::spawn(async move {
            observation.wait_until_no_root_responsibilities().await;
        }));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiter)
                .await
                .is_err(),
            "observation must not report drained while a root remains"
        );

        drop(work.business);
        work.owner.complete();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("observation wakes when the last root is released")
            .expect("wait task joins");
    }

    #[tokio::test]
    async fn completed_work_is_not_reenqueued_when_its_deadline_elapses() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(tokio::time::Instant::now() + Duration::from_millis(10)),
            })
            .expect("admit deadline-bound root");

        work.owner.complete();
        tokio::time::sleep(Duration::from_millis(20)).await;
        control.expire_deadlines();

        assert!(control.next_deadline().is_none());
        assert!(
            control.next_control().is_none(),
            "completed work has no owner left to deliver a later deadline control"
        );
        work.business.release();
    }

    #[test]
    fn late_request_cannot_requeue_terminally_settled_cancel() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        let requester = work.owner.cancellation_requester();
        let scope = work.owner.scope();
        requester
            .request(CancellationReason::DeadlineExceeded)
            .expect("request cancellation");
        work.owner.complete_after_terminal_cancel_settled();
        assert_eq!(control.snapshot().root_responsibilities, 1);
        assert!(control.next_control().is_none());

        requester
            .request(CancellationReason::Requested)
            .expect("late request retains the first cancellation reason");
        scope
            .request_control(crate::ControlIntent::Cancel)
            .expect("late direct control request is harmless");
        assert!(control.next_control().is_none());
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn dropped_inflight_cancel_cannot_requeue_after_terminal_settlement() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit root work");
        work.owner.cancel(CancellationReason::DeadlineExceeded);
        let permit = control.next_control().expect("dispatch cancel control");
        assert!(permit.intents().contains(crate::ControlIntent::Cancel));

        work.owner.complete_after_terminal_cancel_settled();
        drop(permit);
        assert!(control.next_control().is_none());
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn process_owner_cancels_each_active_root_once() {
        let control = controller();
        let first = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit first root");
        let second = control
            .try_begin_root(WorkRequest::new(WorkClass::TableMaintenance))
            .expect("admit second root");
        let first_cancellation = first
            .owner
            .scope()
            .cancellation()
            .expect("first cancellation");
        let second_cancellation = second
            .owner
            .scope()
            .cancellation()
            .expect("second cancellation");

        assert_eq!(
            control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
            2
        );
        assert_eq!(
            first_cancellation.reason(),
            Some(CancellationReason::FrontendDrainDeadlineExceeded)
        );
        assert_eq!(
            second_cancellation.reason(),
            Some(CancellationReason::FrontendDrainDeadlineExceeded)
        );
        assert_eq!(
            control.cancel_active_roots(CancellationReason::ServerShutdown),
            0,
            "a drain deadline must preserve the first cancellation reason"
        );

        drop(first.business);
        first.owner.complete();
        drop(second.business);
        second.owner.complete();
    }

    #[test]
    fn root_admission_drain_before_ready_cannot_be_reopened_or_reconfigured() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control.close_root_admission();
        assert_eq!(control.mark_ready(), Err(WorkError::Closed));
        assert!(matches!(
            control.configure_responsibility(crate::joint_admission::ResponsibilityConfig {
                work_per_turn: std::num::NonZeroUsize::new(1).unwrap(),
            }),
            Err(WorkError::Conflict)
        ));
        assert!(control.snapshot().admission_closed);
        assert!(!control.inner.state.lock().unwrap().closed);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn root_lifecycle_observation_records_rejection_drain_completion_and_deadline_cancel() {
        let control = controller();
        control.close_admission();
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Management)),
            Err(WorkError::Closed)
        ));
        assert_eq!(
            control
                .snapshot()
                .root_lifecycle
                .rejected_admissions
                .management,
            1
        );

        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("admit query root");
        control.close_admission();
        assert_eq!(
            control.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded),
            1
        );
        drop(work.business);
        work.owner.complete();

        let snapshot = control.snapshot().root_lifecycle;
        assert_eq!(snapshot.frontend_drain_deadline_cancelled.query, 1);
        assert_eq!(snapshot.completed_after_admission_closed.query, 1);
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

    #[test]
    fn child_identity_exhaustion_registers_no_cancellation_edge() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let cancellation = {
            let mut state = control.inner.state.lock().unwrap();
            state.next_id = u64::MAX;
            Arc::clone(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert!(matches!(
            scope.child(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::ArithmeticOverflow)
        ));
        assert_eq!(cancellation.linked_children_for_test(), 0);
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&scope.id].children,
            0
        );
        work.business.release();
        work.owner.complete();
    }

    #[test]
    fn unpublished_cancelled_child_detaches_its_registered_edge() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let cancellation = {
            let state = control.inner.state.lock().unwrap();
            Arc::clone(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert!(matches!(
            scope.child(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now()),
            }),
            Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
        ));
        assert_eq!(cancellation.linked_children_for_test(), 0);
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&scope.id].children,
            0
        );
        assert_eq!(control.snapshot().root_responsibilities, 1);
        work.business.release();
        work.owner.complete();
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    struct InspectReclaimWake {
        authority: Weak<Inner>,
        cancellation: Weak<Cancellation>,
        wakes: std::sync::atomic::AtomicUsize,
    }

    impl Wake for InspectReclaimWake {
        fn wake(self: Arc<Self>) {
            let inner = self.authority.upgrade().unwrap();
            assert!(inner.state.try_lock().is_ok(), "reclaim wake held State");
            assert_eq!(
                self.cancellation.strong_count(),
                0,
                "the final view must drop its actual Arc before notifying"
            );
            self.wakes.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn retained_view_exit_wakes_both_waiters_after_actual_arc_exit() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let first = scope.cancellation().unwrap();
        let before_clone_drop = control.progress_revision();
        let view = first.clone();
        let cell = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        assert_eq!(
            control.progress_revision(),
            before_clone_drop,
            "clones allocate no event"
        );
        drop(first);
        work.business.release();
        work.owner.complete();
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        assert!(
            cell.upgrade().is_some(),
            "only the retained view keeps cancellation facts"
        );

        let after = control.progress_revision();
        let deadline_owner = control.deadline_expiry_handle();
        let wakes = Arc::new(InspectReclaimWake {
            authority: Arc::downgrade(&control.inner),
            cancellation: cell.clone(),
            wakes: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&wakes));
        let mut progress = Box::pin(control.wait_progress(after));
        let mut deadlines = Box::pin(deadline_owner.wait_state_change(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            deadlines
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(view);
        assert!(cell.upgrade().is_none());
        let now = control.progress_revision();
        assert_eq!(now.state_revision, after.state_revision);
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        assert!(wakes.wakes.load(Ordering::Relaxed) > 0);
        assert!(
            matches!(progress.as_mut().poll(&mut Context::from_waker(&waker)),
            std::task::Poll::Ready(WorkloadProgress::StateChanged(revision)) if revision == now)
        );
        assert!(
            deadlines
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[tokio::test]
    async fn view_exit_before_wait_registration_is_not_lost_and_does_not_pin_authority() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let view = work.owner.scope().cancellation().unwrap();
        let survivor = view.clone();
        work.business.release();
        work.owner.complete();
        let after = control.progress_revision();
        drop(view);
        let deadlines = control.deadline_expiry_handle();
        let mut changed = Box::pin(deadlines.wait_state_change(after));
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        let mut progress = Box::pin(control.wait_progress(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(progress);
        drop(changed);
        drop(deadlines);
        let original_authority = Arc::downgrade(&control.inner);
        drop(control);
        assert!(
            original_authority.upgrade().is_none(),
            "a retained view is non-owning"
        );
        drop(survivor);
    }

    #[tokio::test(start_paused = true)]
    async fn each_original_raw_author_reports_exit_and_repeated_deadline_reads_are_silent() {
        for action in 0..5 {
            let control = controller();
            let deadline = Instant::now() + Duration::from_secs(1);
            let work = control
                .try_begin_root(WorkRequest {
                    class: WorkClass::Query,
                    deadline: Some(deadline),
                })
                .unwrap();
            let scope = work.owner.scope();
            let view = scope.cancellation().unwrap();
            let cell = {
                let state = control.inner.state.lock().unwrap();
                Arc::downgrade(
                    state
                        .cancellations
                        .cell(state.nodes[&scope.id].cancellation),
                )
            };
            let mut owner = Some(work.owner);
            let before = control.progress_revision();
            assert_eq!(control.next_deadline(), Some(deadline));
            assert_eq!(control.next_deadline(), Some(deadline));
            assert_eq!(
                control.progress_revision(),
                before,
                "deadline getters cannot create dropping views"
            );
            if action == 1 {
                tokio::time::advance(Duration::from_secs(2)).await;
            }
            match action {
                0 => {
                    control.cancel_active_roots(CancellationReason::Requested);
                }
                1 => control.expire_deadlines(),
                2 => {
                    assert_eq!(
                        owner.as_ref().unwrap().success_sealer().seal().unwrap(),
                        WorkSuccessSealOutcome::Sealed
                    );
                }
                3 => {
                    owner
                        .as_ref()
                        .unwrap()
                        .cancellation_requester()
                        .request(CancellationReason::Requested)
                        .unwrap();
                }
                4 => drop(owner.take()),
                _ => unreachable!(),
            }
            let after = control.progress_revision();
            assert_eq!(
                after.reclaim_generation,
                before.reclaim_generation.wrapping_add(1),
                "raw author {action}"
            );
            assert_eq!(
                cell.strong_count(),
                2,
                "only original node and view survive raw author {action}"
            );
            if action == 2 {
                assert_eq!(
                    after.state_revision, before.state_revision,
                    "seal's raw exit must be observable without a State mutation"
                );
            }
            let owner = owner.unwrap_or_else(|| control.adopt(scope.id).unwrap());
            work.business.release();
            owner.complete_after_terminal_cancel_settled();
            drop(view);
        }
    }

    struct PanicOnceCancellationWake(std::sync::atomic::AtomicBool);
    impl Wake for PanicOnceCancellationWake {
        fn wake(self: Arc<Self>) {
            if !self.0.swap(true, Ordering::Relaxed) {
                panic!("injected cancellation wake unwind");
            }
        }
    }

    #[tokio::test]
    async fn original_request_unwind_drops_raw_arc_before_publishing_reclaim_revision() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let view = scope.cancellation().unwrap();
        let cell = {
            let state = control.inner.state.lock().unwrap();
            Arc::downgrade(
                state
                    .cancellations
                    .cell(state.nodes[&scope.id].cancellation),
            )
        };
        let requester = work.owner.cancellation_requester();
        let mut observer = Box::pin(view.cancelled());
        let waker = Waker::from(Arc::new(PanicOnceCancellationWake(
            std::sync::atomic::AtomicBool::new(false),
        )));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let after = control.progress_revision();
        let mut progress = Box::pin(control.wait_progress(after));
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            requester.request(CancellationReason::Requested)
        }));
        assert!(
            result.is_err(),
            "actual cancellation notification unwinds the original request"
        );
        assert_eq!(
            cell.strong_count(),
            2,
            "the escaping raw request Arc was actually dropped"
        );
        let now = control.progress_revision();
        assert_eq!(
            now.state_revision, after.state_revision,
            "unwind preceded the request's State update"
        );
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        assert!(
            progress
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(progress);
        drop(observer);
        work.business.release();
        work.owner.complete_after_terminal_cancel_settled();
    }

    #[test]
    fn raw_author_error_return_also_advances_the_observable_pair() {
        let control = controller();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let child = work
            .owner
            .scope()
            .child(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let after = control.progress_revision();
        assert_eq!(child.success_sealer().seal(), Err(WorkError::Conflict));
        let now = control.progress_revision();
        assert_eq!(now.state_revision, after.state_revision);
        assert_ne!(now.reclaim_generation, after.reclaim_generation);
        child.complete();
        work.business.release();
        work.owner.complete();
    }
}

#[cfg(test)]
pub(crate) mod fixed_control_storage_tests;

#[cfg(test)]
mod root_publication_tests;
