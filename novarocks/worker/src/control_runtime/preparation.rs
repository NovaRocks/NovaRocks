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

//! Actual Accepted input and preparation custody. The fair queue owns the
//! original jobs; body slots and whole preparation have separate release
//! conditions. No host call or opaque Drop runs locked.

use super::actors::{WorkerAction, WorkerActor};
use crate::{
    control_admission::Dimension,
    task_port::{TaskPendingNotice, TaskStopSignal},
};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, ActorTurnError, EffectTicket},
    completion::ReportSlot,
    obligation::{ReservationObligation, StepRegistration, WorkObligation},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    fair_queue::{FairQueueError, FairWorkQueue},
    identity::{ConsumerId, ScopeId, WorkId},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    ParticipationRef, TaskCreationInput, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::{
    any::Any,
    collections::BTreeMap,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

type Key = (ScopeId, WorkId);
type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

struct State {
    work: Option<WorkObligation>,
    capacity: Option<ReservationObligation>,
    body_exited: bool,
    captures_exited: bool,
    exit_published: bool,
    body_report_retired: bool,
    exit_report_retired: bool,
    transferred: bool,
    task_source_storage_transferred: bool,
    unresolved: bool,
    // One original body and one independent input-disposal boundary. Panic
    // graphs are retained without invoking their destructor a second time.
    panics: [Option<PanicPayload>; 2],
}
struct Custody {
    task: TaskIdentity,
    stop: TaskStopSignal,
    parent: PreparationParent,
    input: Mutex<Option<ManuallyDrop<TaskCreationInput>>>,
    state: Mutex<State>,
    body_report: ReportSlot,
    exit_report: ReportSlot,
    preparing: ResourceVector,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
    // The original host child registration stays in this preparation owner
    // until actual rollback/adoption callback and metadata capture exit.
    host_children: Mutex<Option<Arc<crate::task_preparation::TaskPreparationScope>>>,
}

/// This loan can derive only from the actual still-counted original work.
/// Closing external admission does not revoke existing drain authority;
/// stop/failure still rejects business and permits necessary cleanup.
#[derive(Clone)]
pub(crate) struct PreparationParent {
    scope: ScopeHandle,
    work: WorkId,
    authority: Result<novarocks_control_runtime::obligation::WorkChildAuthority, RuntimeError>,
}
impl PreparationParent {
    pub(crate) fn from_admitted(work: &WorkObligation) -> Self {
        Self {
            scope: work.scope().clone(),
            work: work.id(),
            authority: work.child_authority(),
        }
    }
    pub(crate) fn admission_proof(
        &self,
    ) -> Result<novarocks_control_runtime::obligation::AdmissionProof, RuntimeError> {
        self.authority
            .as_ref()
            .map_err(|error| *error)?
            .verify_live()
    }
    #[cfg(test)]
    pub(crate) fn derive(&self, kind: WorkKind) -> Result<StepRegistration, RuntimeError> {
        self.derive_with_storage(kind, ReservationResources::work_only())
    }
    /// This actual parent is already counted. Allocate only the new child's
    /// own positions before any of that child's foreign captures can escape.
    pub(crate) fn derive_with_storage(
        &self,
        kind: WorkKind,
        resources: ReservationResources,
    ) -> Result<StepRegistration, RuntimeError> {
        let generation = self.scope.gates()?.generation;
        self.scope.reserve_next(
            generation,
            resources,
            kind,
            WorkAdmission::Derived(self.work),
        )
    }
}

/// Only references to the original work/reports live here. No copied active,
/// preparing, byte or child counters constitute another admission authority.
#[derive(Clone)]
pub(crate) struct PreparationCustody(Arc<Custody>);

#[must_use = "The actual preparation body or unused input must exit explicitly"]
pub(crate) struct PreparationJob {
    custody: PreparationCustody,
    active: bool,
}
pub(crate) enum PreparationOutcome<T> {
    Returned(T),
    TaskStopped,
    StartRefused(RuntimeError),
    Unresolved,
}
impl<T> std::fmt::Display for PreparationOutcome<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Returned(_) => f.write_str("Returned"),
            Self::TaskStopped => f.write_str("Stopped"),
            Self::StartRefused(error) => write!(f, "StartRefused: {error}"),
            Self::Unresolved => f.write_str("Unresolved"),
        }
    }
}
#[derive(Debug)]
pub(crate) enum PreparationAdoptionError {
    WrongEffect,
    WrongTask,
    ActorTurn(ActorTurnError),
    Runtime(RuntimeError),
}
impl std::fmt::Display for PreparationAdoptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongEffect => {
                f.write_str("preparation adoption requires the exact EnqueuePreparation effect")
            }
            Self::WrongTask => {
                f.write_str("preparation stop signal differs from the admitted Task")
            }
            Self::ActorTurn(error) => {
                write!(f, "preparation responsibility transfer failed: {error}")
            }
            Self::Runtime(error) => write!(f, "preparation report admission failed: {error}"),
        }
    }
}
impl std::error::Error for PreparationAdoptionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ActorTurn(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::WrongEffect | Self::WrongTask => None,
        }
    }
}

/// Refusal leaves the original preparation inventory and split opportunity intact.
#[derive(Debug)]
pub(crate) enum PreparationStorageSplitError {
    InvalidResources,
    AlreadyTransferred,
    Unavailable,
    Runtime(RuntimeError),
}

impl std::fmt::Display for PreparationStorageSplitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidResources => {
                f.write_str("preparation storage split requires valid owned resources")
            }
            Self::AlreadyTransferred => f.write_str("preparation storage was already transferred"),
            Self::Unavailable => f.write_str("preparation storage is unavailable"),
            Self::Runtime(error) => write!(f, "preparation storage split failed: {error}"),
        }
    }
}
impl std::error::Error for PreparationStorageSplitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::InvalidResources | Self::AlreadyTransferred | Self::Unavailable => None,
        }
    }
}
/// The original Accepted preparation body is independent of its future Task
/// lifecycle authors. Its same guard owns one Work, two exit reports, and the
/// one actual input capture; it has no causal input, cut, reply, or RPC role.
pub(crate) const PREPARATION_STORAGE: crate::control_runtime_policy::StorageNeed =
    crate::control_runtime_policy::StorageNeed {
        requests: 0,
        inputs: 0,
        batches: 0,
        effects: 0,
        cuts: 0,
        replies: 0,
        completions: 2,
        captures: 1,
        work: 1,
    };

pub(crate) const PREPARATION_CORE_CONSUMER: ConsumerId = ConsumerId::new(1).unwrap();
pub(crate) const PREPARATION_OBSERVER_CONSUMER: ConsumerId = ConsumerId::new(2).unwrap();
pub(crate) struct PreparationBinding {
    pub stop: TaskStopSignal,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}
fn revoke_unbound_report(report: &ReportSlot) {
    report.revoke_unused().expect("unused preparation report");
    report
        .body_exited()
        .expect("unused report has no producer body");
    report
        .captures_exited()
        .expect("unused report has no captures");
    assert!(report.try_retire().expect("unused report retirement"));
}
impl PreparationCustody {
    /// Adopt the original, still-unbound Accepted guard directly from its
    /// committed batch. Report headers are installed before binding a payload;
    /// an admission/failure race rolls back only unused headers and that guard.
    /// It never allocates a second quota bundle or invents an exit observation.
    pub(crate) fn adopt(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        input: TaskCreationInput,
        binding: PreparationBinding,
    ) -> Result<(Self, PreparationJob), (PreparationAdoptionError, TaskCreationInput)> {
        let (task, reservation) = match &ticket.effect {
            WorkerAction::Participation(
                novarocks_worker_protocol::participation::ParticipationEffect::EnqueuePreparation {
                    task,
                    reservation,
                    ..
                },
            ) => (*task, *reservation),
            _ => return Err((PreparationAdoptionError::WrongEffect, input)),
        };
        if binding.stop.identity() != task {
            return Err((PreparationAdoptionError::WrongTask, input));
        }
        let mut guard = match runtime.take_step_reservation(ticket, reservation) {
            Ok(guard) => guard,
            Err(error) => return Err((PreparationAdoptionError::ActorTurn(error), input)),
        };
        let scope = guard.scope().clone();
        let generation = ticket.generation;
        let mut report = || {
            ReportSlot::reserve_next(
                &mut guard,
                generation,
                2,
                [
                    (
                        PREPARATION_CORE_CONSUMER,
                        ConsumerKind::Owner {
                            scope: scope.id(),
                            generation,
                        },
                    ),
                    (PREPARATION_OBSERVER_CONSUMER, ConsumerKind::Observer),
                ],
            )
        };
        let body = match report() {
            Ok(report) => report,
            Err(error) => return Err((PreparationAdoptionError::Runtime(error), input)),
        };
        let exit = match report() {
            Ok(report) => report,
            Err(error) => {
                revoke_unbound_report(&body);
                return Err((PreparationAdoptionError::Runtime(error), input));
            }
        };
        let (work, capacity) = match guard.bind(WorkKind::Business) {
            Ok(bound) => bound,
            Err(error) => {
                revoke_unbound_report(&body);
                revoke_unbound_report(&exit);
                return Err((PreparationAdoptionError::Runtime(error), input));
            }
        };
        Ok(Self::from_admitted(
            binding,
            work,
            capacity,
            [body, exit],
            input,
        ))
    }
    /// All work, storage and both reports were admitted before attaching this
    /// input. The capacity is the same Accepted reservation from the Core step.
    /// The Task owner receives it back, with preparation charges removed, only
    /// after real whole exit and report retirement.
    pub(crate) fn from_admitted(
        binding: PreparationBinding,
        mut work: WorkObligation,
        mut capacity: ReservationObligation,
        reports: [ReportSlot; 2],
        input: TaskCreationInput,
    ) -> (Self, PreparationJob) {
        // The original input holder is one real preparation capture. Future
        // Task role storage remains unused until its unique Task transfer.
        work.bind_owned_captures(&mut capacity, 1)
            .expect("the Accepted role declares its original input capture");
        let resources = capacity
            .resources()
            .expect("live original Accepted reservation");
        assert_eq!(resources.quota.get(Dimension::PreparingTasks.id()), 1);
        let preparing = ResourceVector::new([
            (Dimension::PreparingTasks.id(), 1),
            (
                Dimension::PreparingBytes.id(),
                resources.quota.get(Dimension::PreparingBytes.id()),
            ),
        ])
        .expect("fixed preparation release dimensions");
        let parent = PreparationParent::from_admitted(&work);
        let [body_report, exit_report] = reports;
        let custody = Arc::new(Custody {
            task: binding.stop.identity(),
            stop: binding.stop,
            parent,
            input: Mutex::new(Some(ManuallyDrop::new(input))),
            state: Mutex::new(State {
                work: Some(work),
                capacity: Some(capacity),
                body_exited: false,
                captures_exited: false,
                exit_published: false,
                body_report_retired: false,
                exit_report_retired: false,
                transferred: false,
                task_source_storage_transferred: false,
                unresolved: false,
                panics: [None, None],
            }),
            body_report,
            exit_report,
            preparing,
            pending: binding.pending,
            health: binding.health,
            retained: Mutex::new(None),
            host_children: Mutex::new(None),
        });
        *custody.retained.lock().expect("preparation retention lock") = Some(custody.clone());
        let owner = Self(custody);
        let job = PreparationJob {
            custody: owner.clone(),
            active: true,
        };
        (owner, job)
    }
    /// Transfer the original unused Task roles once. Preparation P/bytes,
    /// its actual job/input and independent reports stay with this owner.
    pub(crate) fn take_task_source_storage(
        &self,
        resources: &ReservationResources,
    ) -> Result<ReservationObligation, PreparationStorageSplitError> {
        let per_cut = crate::control_runtime_policy::StorageNeed::causal_cut_storage(false);
        let headers = resources.owned_slots.work_headers;
        let cut_count = headers / per_cut.work;
        let expected = per_cut
            .checked_mul(cut_count)
            .and_then(crate::control_runtime_policy::StorageNeed::resources)
            .map_err(|_| PreparationStorageSplitError::InvalidResources)?;
        if cut_count == 0 || !resources.quota.is_empty() || *resources != expected {
            return Err(PreparationStorageSplitError::InvalidResources);
        }
        let mut state = self.0.state.lock().expect("preparation custody lock");
        if state.task_source_storage_transferred {
            return Err(PreparationStorageSplitError::AlreadyTransferred);
        }
        if state.transferred || state.unresolved || state.exit_published {
            return Err(PreparationStorageSplitError::Unavailable);
        }
        let capacity = state
            .capacity
            .as_mut()
            .ok_or(PreparationStorageSplitError::Unavailable)?;
        let storage = capacity
            .split_next(resources)
            .map_err(PreparationStorageSplitError::Runtime)?;
        state.task_source_storage_transferred = true;
        Ok(storage)
    }
    pub(crate) fn key(&self) -> Key {
        (self.0.parent.scope.id(), self.0.parent.work)
    }
    /// These flags are set only by this original custodian after a
    /// successful retirement of the corresponding actual report slot.
    pub(crate) fn body_report_retired(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original body report retirement")
            .body_report_retired
    }
    pub(crate) fn exit_report_retired(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original whole report retirement")
            .exit_report_retired
    }
    pub(crate) fn body_report(&self) -> &ReportSlot {
        &self.0.body_report
    }
    pub(crate) fn exit_report(&self) -> &ReportSlot {
        &self.0.exit_report
    }
    pub(crate) fn bind_host_children(
        &self,
        children: Arc<crate::task_preparation::TaskPreparationScope>,
    ) {
        assert_eq!(
            children.identity(),
            self.0.task,
            "exact host preparation identity"
        );
        let mut holder = self
            .0
            .host_children
            .lock()
            .expect("preparation host child holder");
        assert!(holder.is_none(), "one original host child registration");
        *holder = Some(children);
    }
    pub(crate) fn host_children(
        &self,
    ) -> Option<Arc<crate::task_preparation::TaskPreparationScope>> {
        self.0
            .host_children
            .lock()
            .expect("preparation host child holder")
            .clone()
    }
    /// The live host owns this transition after actual installation and its
    /// ordered Installed publication. Provider adoption transfers runtime
    /// custody; only the original preparation metadata holder exits here.
    /// The caller's commit action runs outside every runtime/custody lock.
    pub(crate) fn adopt_host_runtime<F: FnOnce()>(&self, commit: F) -> Result<bool, F> {
        let Some(children) = self.host_children() else {
            return Err(commit);
        };
        if !children.adopt_runtime() {
            return Err(commit);
        }
        children.start_runtime(commit)
    }
    /// Real rejection/stand-down cleanup, including an original child that
    /// never became runnable. Pending or unknown child exit retains P/bytes;
    /// a canceled Task or the return from install_receiver cannot replace it.
    pub(crate) fn rollback_host_runtime(
        &self,
        remove_inbound: impl FnOnce(),
        remove_receiver: impl FnOnce(),
    ) -> bool {
        let Some(children) = self.host_children() else {
            return false;
        };
        children.request_rollback();
        children.cleanup(
            crate::task_preparation::PreparationCleanup::Inbound,
            remove_inbound,
        );
        children.cleanup(
            crate::task_preparation::PreparationCleanup::Receiver,
            remove_receiver,
        );
        children.rollback_exited()
    }
    fn take_input(&self) -> Option<TaskCreationInput> {
        self.0
            .input
            .lock()
            .expect("preparation input transfer lock")
            .take()
            .map(ManuallyDrop::into_inner)
    }
    fn unresolved(&self, panic: Option<(usize, Box<dyn Any + Send>)>) {
        {
            let mut state = self.0.state.lock().expect("preparation custody lock");
            state.unresolved = true;
            state
                .work
                .as_ref()
                .expect("preparation work")
                .mark_unresolved();
            if let Some((slot, payload)) = panic {
                assert!(
                    state.panics[slot].is_none(),
                    "one original panic per boundary"
                );
                state.panics[slot] = Some(ManuallyDrop::new(payload));
            }
        }
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::PreparationChildUnresolved.reason(),
            ),
        );
        self.0.pending.pending();
    }
    fn observe_body(&self, captures_exited: bool) {
        let mut state = self.0.state.lock().expect("preparation custody lock");
        assert!(!state.body_exited, "one original preparation body exit");
        state
            .work
            .as_mut()
            .expect("preparation work")
            .body_exited_with_report(&self.0.body_report)
            .expect("prepaid preparation body report");
        state.body_exited = true;
        self.0
            .body_report
            .body_exited()
            .expect("body report producer exit");
        if captures_exited {
            state
                .work
                .as_mut()
                .expect("preparation work")
                .captures_exited()
                .expect("actual preparation capture exit");
            state.captures_exited = true;
            self.0
                .body_report
                .captures_exited()
                .expect("body report capture exit");
        }
        drop(state);
        self.0.pending.pending();
    }
    pub(crate) fn body_and_captures_exited(&self) -> bool {
        let state = self.0.state.lock().expect("original preparation body exit");
        !state.unresolved && state.body_exited && state.captures_exited
    }
    /// Only the actual preparation call, its captures and its registered
    /// provider-host holders. This read cannot invoke a provider or publish an
    /// exit report. Source aliases and report consumption retain their own
    /// independent whole-preparation/capacity retirement obligations.
    pub(crate) fn host_borrowers_exited(&self) -> bool {
        let exited = {
            let state = self.0.state.lock().expect("preparation borrower exit");
            !state.unresolved && state.body_exited && state.captures_exited
        };
        exited
            && self
                .host_children()
                .is_none_or(|child| child.host_captures_exited())
    }
    /// The physical worker can already be reused. Children, input unwind and
    /// independent report consumers can still prevent whole exit/charge reuse.
    pub(crate) fn publish_whole_exit(&self) -> Result<bool, RuntimeError> {
        // The foreign provider was already driven by the host, outside these
        // locks. Body return alone cannot retire a registered dormant kernel.
        if self
            .host_children()
            .is_some_and(|child| !child.host_captures_exited())
        {
            return Ok(false);
        }
        let mut state = self.0.state.lock().expect("preparation custody lock");
        if state.exit_published {
            return Ok(true);
        }
        if state.unresolved || !state.body_exited || !state.captures_exited {
            return Ok(false);
        }
        if !state
            .work
            .as_ref()
            .expect("preparation work")
            .actual_exit_ready()?
        {
            return Ok(false);
        }
        self.0.exit_report.fill()?;
        self.0.exit_report.body_exited()?;
        self.0.exit_report.captures_exited()?;
        state.exit_published = true;
        drop(state);
        self.0.pending.pending();
        Ok(true)
    }
    /// Transfer the original bundle once. Only P/count/bytes leave the ledger;
    /// Remaining Task role storage stays in the same original bundle.
    pub(crate) fn take_after_exit(&self) -> Result<Option<ReservationObligation>, RuntimeError> {
        let mut state = self.0.state.lock().expect("preparation custody lock");
        if state.transferred || !state.exit_published || state.unresolved {
            return Ok(None);
        }
        if !state.body_report_retired {
            state.body_report_retired = self.0.body_report.try_retire()?;
        }
        if !state.exit_report_retired {
            state.exit_report_retired = self.0.exit_report.try_retire()?;
        }
        if !state.body_report_retired || !state.exit_report_retired {
            return Ok(None);
        }
        let capacity = state.capacity.as_mut().expect("original Accepted bundle");
        capacity.release_quota(&self.0.preparing)?;
        let capacity = state.capacity.take();
        let work = state.work.take();
        state.transferred = true;
        drop(state);
        drop(work);
        let retained = self
            .0
            .retained
            .lock()
            .expect("preparation retention lock")
            .take();
        drop(retained);
        Ok(capacity)
    }
}
impl PreparationJob {
    pub(super) fn key(&self) -> Key {
        self.custody.key()
    }
    /// Invoked by an actual host worker, never an actor poller. Start checks
    /// the exact Task stop intent and current scope stop/failure gate; an old Accepted fact cannot grant
    /// execution. A refused job still really disposes its original input.
    pub(crate) fn run<T>(
        mut self,
        host: impl FnOnce(TaskCreationInput, PreparationParent) -> T,
    ) -> PreparationOutcome<T> {
        let result = catch_unwind(AssertUnwindSafe(|| {
            if self.custody.0.stop.is_set() {
                drop(host);
                drop(self.custody.take_input());
                return PreparationOutcome::TaskStopped;
            }
            let start = self
                .custody
                .0
                .state
                .lock()
                .expect("preparation custody lock")
                .work
                .as_mut()
                .expect("preparation work")
                .start();
            match start {
                Ok(()) if !self.custody.0.stop.is_set() => PreparationOutcome::Returned(host(
                    self.custody
                        .take_input()
                        .expect("one actual preparation input"),
                    self.custody.0.parent.clone(),
                )),
                Ok(()) => {
                    drop(host);
                    drop(self.custody.take_input());
                    PreparationOutcome::TaskStopped
                }
                Err(error) => {
                    drop(host);
                    drop(self.custody.take_input());
                    PreparationOutcome::StartRefused(error)
                }
            }
        }));
        let outcome = match result {
            Ok(outcome) => {
                self.custody.observe_body(true);
                outcome
            }
            Err(payload) => {
                self.custody.unresolved(Some((0, payload)));
                // A rejected start can unwind while disposing the callback,
                // leaving its input in original custody. Dispose separately.
                let input = self.custody.take_input();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(input))) {
                    self.custody.unresolved(Some((1, payload)));
                }
                self.custody.observe_body(false);
                PreparationOutcome::Unresolved
            }
        };
        self.active = false;
        outcome
    }
    /// Cancellation removes a queued job; this host-side disposal is the
    /// positive exit observation. Queue removal/Drop alone proves no exit.
    #[cfg(test)]
    pub(crate) fn dispose(mut self) -> bool {
        let input = self.custody.take_input();
        let result = catch_unwind(AssertUnwindSafe(|| drop(input)));
        let exited = match result {
            Ok(()) => true,
            Err(payload) => {
                self.custody.unresolved(Some((1, payload)));
                false
            }
        };
        self.custody.observe_body(exited);
        self.active = false;
        exited
    }
}
impl Drop for PreparationJob {
    fn drop(&mut self) {
        if self.active {
            self.custody.unresolved(None);
        }
    }
}

/// Fairness is scheduling metadata, never a Scope capability or Work identity.
/// An ingress RPC has its own paid source scope but shares the Participation's
/// physical-worker limit with every other RPC for that exact Participation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum HostFairnessGroup {
    Scope(ScopeId),
    Participation(ParticipationRef),
}
pub(crate) trait QueuedPreparation: Send + 'static {
    /// Read-only metadata, called before taking the queue lock.
    fn queue_key(&self) -> (ScopeId, WorkId);
    fn fairness_group(&self, scope: ScopeId) -> HostFairnessGroup {
        HostFairnessGroup::Scope(scope)
    }
}
impl QueuedPreparation for PreparationJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.key()
    }
}

struct QueueState<J: QueuedPreparation> {
    fair: FairWorkQueue<HostFairnessGroup, Key>,
    jobs: BTreeMap<Key, QueuedJob<J>>,
    running: usize,
}
struct QueuedJob<J> {
    group: HostFairnessGroup,
    // The fixed membership header remains while a body runs. This prevents
    // resubmission of its real owner key under a different fairness group.
    original: Option<ManuallyDrop<J>>,
}
struct Queue<J: QueuedPreparation = PreparationJob> {
    state: Mutex<QueueState<J>>,
    workers: usize,
}
/// Actual physical queue membership sampled under its one original mutex.
/// A body may leave this queue before its independent preparation fee exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparationQueueSnapshot {
    pub queued: usize,
    pub running: usize,
    pub worker_limit: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparationQueueSnapshotError {
    Poisoned,
}
impl std::fmt::Display for PreparationQueueSnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Poisoned => f.write_str("preparation queue snapshot lock is poisoned"),
        }
    }
}
impl std::error::Error for PreparationQueueSnapshotError {}
pub(crate) struct PreparationQueue<J: QueuedPreparation = PreparationJob>(Arc<Queue<J>>);
impl<J: QueuedPreparation> Clone for PreparationQueue<J> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
pub(crate) struct RunningPreparation<J: QueuedPreparation = PreparationJob> {
    queue: PreparationQueue<J>,
    group: HostFairnessGroup,
    key: Key,
    job: Option<J>,
}
impl<J: QueuedPreparation> PreparationQueue<J> {
    pub(crate) fn new(jobs: usize, per_group: usize, workers: usize) -> Self {
        Self::with_queue(
            FairWorkQueue::new(jobs, per_group),
            jobs,
            per_group,
            workers,
        )
    }
    pub(crate) fn eligible_fifo(jobs: usize, per_group: usize, workers: usize) -> Self {
        Self::with_queue(
            FairWorkQueue::eligible_fifo(jobs, per_group),
            jobs,
            per_group,
            workers,
        )
    }
    fn with_queue(
        fair: FairWorkQueue<HostFairnessGroup, Key>,
        jobs: usize,
        per_group: usize,
        workers: usize,
    ) -> Self {
        assert!(jobs > 0 && per_group > 0 && workers > 0);
        Self(Arc::new(Queue {
            state: Mutex::new(QueueState {
                fair,
                jobs: BTreeMap::new(),
                running: 0,
            }),
            workers,
        }))
    }
    /// Observation grants no exit or capacity. Busy owners are unavailable;
    /// the caller must not replace this original sample with zero or a cache.
    pub(crate) fn try_snapshot(
        &self,
    ) -> Result<Option<PreparationQueueSnapshot>, PreparationQueueSnapshotError> {
        let state = match self.0.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(PreparationQueueSnapshotError::Poisoned);
            }
        };
        Ok(Some(PreparationQueueSnapshot {
            queued: state
                .fair
                .len()
                .checked_sub(state.running)
                .expect("running queue membership"),
            running: state.running,
            worker_limit: self.0.workers,
        }))
    }
    /// True only when neither queued jobs nor actual running bodies remain.
    pub(super) fn is_empty(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("preparation queue lock")
            .fair
            .is_empty()
    }
    pub(crate) fn enqueue(&self, job: J) -> Result<(), (FairQueueError, J)> {
        let key = job.queue_key();
        let group = job.fairness_group(key.0);
        let mut state = self.0.state.lock().expect("preparation queue lock");
        // A caller cannot enqueue the same actual owner under another group.
        // Membership remains unique across the entire physical queue.
        if state.jobs.contains_key(&key) {
            drop(state);
            return Err((FairQueueError::DuplicateWork, job));
        }
        if let Err(error) = state.fair.enqueue(group, key) {
            drop(state);
            return Err((error, job));
        }
        assert!(
            state
                .jobs
                .insert(
                    key,
                    QueuedJob {
                        group,
                        original: Some(ManuallyDrop::new(job)),
                    }
                )
                .is_none()
        );
        Ok(())
    }
    pub(crate) fn pop(&self) -> Option<RunningPreparation<J>> {
        let mut state = self.0.state.lock().expect("preparation queue lock");
        if state.running == self.0.workers {
            return None;
        }
        let (group, key) = state.fair.pop()?;
        let job = state
            .jobs
            .get_mut(&key)
            .expect("original queued preparation")
            .original
            .take()
            .expect("one original queued body");
        state.running += 1;
        drop(state);
        Some(RunningPreparation {
            queue: self.clone(),
            group,
            key,
            job: Some(ManuallyDrop::into_inner(job)),
        })
    }
    /// The caller disposes the returned original on a host cleanup executor.
    pub(crate) fn cancel(&self, scope: ScopeId, work: WorkId) -> Result<J, FairQueueError> {
        let mut state = self.0.state.lock().expect("preparation queue lock");
        let key = (scope, work);
        let group = state
            .jobs
            .get(&key)
            .ok_or(FairQueueError::UnknownWork)?
            .group;
        state.fair.remove_queued(group, key)?;
        let job = state
            .jobs
            .remove(&(scope, work))
            .expect("original canceled job");
        drop(state);
        Ok(ManuallyDrop::into_inner(
            job.original.expect("original canceled body"),
        ))
    }
}
impl<J: QueuedPreparation> RunningPreparation<J> {
    pub(super) fn run_body<T>(mut self, body: impl FnOnce(J) -> T) -> T {
        let result = body(self.job.take().expect("one actual running body"));
        // This returns only the physical worker slot, including on unwind.
        // All original work/capture/report custody stays with its real job.
        drop(self);
        result
    }
}
#[cfg(test)]
impl RunningPreparation<PreparationJob> {
    #[cfg(test)]
    pub(crate) fn run<T>(
        self,
        host: impl FnOnce(TaskCreationInput, PreparationParent) -> T,
    ) -> PreparationOutcome<T> {
        self.run_body(|job| job.run(host))
    }
}
impl<J: QueuedPreparation> Drop for RunningPreparation<J> {
    fn drop(&mut self) {
        // Dropping an uninvoked body leaves its original work unresolved.
        let job = self.job.take();
        {
            let mut state = self.queue.0.state.lock().expect("preparation queue lock");
            state
                .fair
                .finish(self.group, self.key)
                .expect("actual running preparation body slot");
            let member = state
                .jobs
                .remove(&self.key)
                .expect("original running membership");
            assert!(member.original.is_none());
            state.running -= 1;
        }
        drop(job);
    }
}

impl<J: QueuedPreparation> Drop for Queue<J> {
    fn drop(&mut self) {
        let jobs =
            std::mem::take(&mut self.state.get_mut().expect("exclusive queue teardown").jobs);
        for job in jobs.into_values() {
            if let Some(original) = job.original {
                drop(ManuallyDrop::into_inner(original));
            }
        }
    }
}

#[cfg(test)]
mod tests;
