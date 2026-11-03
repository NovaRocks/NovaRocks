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

//! The Participation's actual Task I/O handoff. Unconfigured I/O never acknowledges
//! an effect; its exact ticket remains in the caller's existing paid slot.

#[path = "participation_admission.rs"]
pub(super) mod participation_admission;

use super::{ParticipationHostError, ParticipationHostRecord};
use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    effect_dispatch::DispatchError,
    host_executor::HostExecutorHandle,
    loading_host_job::LoadingHostJob,
    task::TaskHostError,
};
use novarocks_control_runtime::actor_turn::{ActorTurnError, EffectTicket};
use novarocks_control_runtime_core::{
    fair_queue::{FairQueueError, FairWorkQueue},
    identity::{ActorId, Generation, InputId},
};
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use novarocks_worker_protocol::participation::ParticipationEffect;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Concrete original I/O ports. A cold Participation can exist before Task intake is
/// opened; an absent port rejects capture rather than fabricating a host call.
#[derive(Clone, Default)]
pub(crate) struct TaskIoPorts {
    pub protocol_events: Option<crate::TaskExecutionPorts>,
    pub observation: Option<super::observation::ObservationProjection>,
    pub execution: Option<Arc<dyn crate::TaskExecutionHost>>,
    pub loading: Option<HostExecutorHandle<LoadingHostJob>>,
    pub preparation: Option<HostExecutorHandle<TaskIoHostJob>>,
    pub lifecycle: Option<HostExecutorHandle<TaskIoHostJob>>,
    pub cadence: Duration,
    pub capacity: Option<crate::control_runtime::task_capacity::TaskCapacitySignal>,
}
impl TaskIoPorts {
    pub(crate) fn intake_open(&self) -> bool {
        self.execution.is_some()
            && self.loading.is_some()
            && self.preparation.is_some()
            && self.lifecycle.is_some()
            && self.capacity.is_some()
            && !self.cadence.is_zero()
    }
}
#[derive(Debug)]
pub(crate) enum TaskIoError {
    WrongOwner,
    MissingExecutionPort,
    MissingExecutionHealth,
    MissingLoadingPort,
    MissingPreparationPort,
    MissingLifecyclePort,
    Capacity(crate::control_runtime::task_capacity::TaskCapacityError),
    HostCall(crate::control_runtime::task_host_call::TaskHostCallError),
    Settlement(crate::settlement::SettlementDispatchError),
    Publication,
    PublicationRefused(super::super::task_publication::TaskPublicationError),
    Projection(crate::task_port::ProjectionError),
    Task(ParticipationHostError),
    Host(TaskHostError),
    ActorTurn(ActorTurnError),
    Dispatch(DispatchError),
    Request(super::requests::ParticipationRequestError),
    Loading(super::super::loading_events::LoadingEventError),
    EffectRequiresOriginalOwner,
    UnsupportedEffect(&'static str),
    RetainedRootSeal(crate::control_runtime::task::RetainedRootSealError),
    DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError),
    Source(crate::control_runtime::source_positions::SourcePositionError),
}
impl std::fmt::Display for TaskIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Task I/O does not belong to the original Participation owner")
            }
            Self::MissingExecutionPort => f.write_str("Task driver execution port is missing"),
            Self::MissingExecutionHealth => {
                f.write_str("Task driver execution health source is missing")
            }
            Self::MissingLoadingPort => f.write_str("Task driver loading port is missing"),
            Self::MissingPreparationPort => f.write_str("Task driver preparation port is missing"),
            Self::MissingLifecyclePort => f.write_str("Task driver lifecycle port is missing"),
            Self::Capacity(error) => {
                write!(f, "Task driver capacity notification failed: {error:?}")
            }
            Self::HostCall(error) => write!(f, "Task driver host call failed: {error}"),
            Self::Settlement(error) => write!(f, "Task driver settlement dispatch failed: {error}"),
            Self::Publication => f.write_str("Task driver publication failed"),
            Self::PublicationRefused(error) => {
                write!(f, "Task driver publication was refused: {error:?}")
            }
            Self::Projection(error) => write!(f, "Task driver projection was refused: {error:?}"),
            Self::Task(error) => {
                write!(
                    f,
                    "Task I/O Participation ownership operation failed: {error}"
                )
            }
            Self::Host(error) => write!(
                f,
                "Task driver original Task host operation failed: {error}"
            ),
            Self::ActorTurn(error) => {
                write!(f, "Task driver input or effect operation failed: {error}")
            }
            Self::Dispatch(error) => write!(f, "Task driver effect dispatch failed: {error}"),
            Self::Request(error) => write!(f, "Task driver original request failed: {error}"),
            Self::Loading(error) => write!(
                f,
                "Task driver original loading operation failed: {error:?}"
            ),
            Self::EffectRequiresOriginalOwner => {
                f.write_str("Task driver effect requires its original action owner")
            }
            Self::UnsupportedEffect(kind) => {
                write!(f, "Task driver does not support effect: {kind}")
            }
            Self::RetainedRootSeal(error) => {
                write!(f, "Task driver retained Root seal failed: {error:?}")
            }
            Self::DynamicFilter(error) => {
                write!(f, "Task driver runtime-filter operation failed: {error:?}")
            }
            Self::Source(error) => write!(
                f,
                "Task driver original Source position operation failed: {error:?}"
            ),
        }
    }
}
impl std::error::Error for TaskIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::HostCall(error) => Some(error),
            Self::Settlement(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Host(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::Dispatch(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::WrongOwner
            | Self::MissingExecutionPort
            | Self::MissingExecutionHealth
            | Self::MissingLoadingPort
            | Self::MissingPreparationPort
            | Self::MissingLifecyclePort
            | Self::Capacity(_)
            | Self::Publication
            | Self::PublicationRefused(_)
            | Self::Projection(_)
            | Self::Loading(_)
            | Self::EffectRequiresOriginalOwner
            | Self::UnsupportedEffect(_)
            | Self::RetainedRootSeal(_)
            | Self::DynamicFilter(_)
            | Self::Source(_) => None,
        }
    }
}

pub(crate) struct TaskIoRefusal {
    pub error: TaskIoError,
    pub ticket: Option<EffectTicket<WorkerAction>>,
}
impl std::fmt::Debug for TaskIoRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskIoRefusal")
            .field("error", &self.error)
            .field("retains_original_ticket", &self.ticket.is_some())
            .finish()
    }
}
#[derive(Debug, Default)]
pub(crate) struct TaskIoProgress {
    pub visited: usize,
    pub delivered: usize,
    pub runnable: bool,
}
/// The Participation has one original ready address per live Task, bounded by T.
/// TaskReady replaces the old identity-ordered set: its group/work/ready
/// indexes each contain at most T positions. This is a position bound, not a
/// claim that its physical byte layout equals a single BTreeSet.
struct TaskReady {
    queue: FairWorkQueue<TaskIdentity, ()>,
}
impl TaskReady {
    fn new(max_live_tasks: usize) -> Self {
        Self {
            queue: FairWorkQueue::new(max_live_tasks, 1),
        }
    }
    fn insert(&mut self, task: TaskIdentity) {
        match self.queue.enqueue(task, ()) {
            Ok(()) | Err(FairQueueError::DuplicateWork) => {}
            Err(error) => panic!("original bounded Task readiness refused: {error:?}"),
        }
    }
    fn pop(&mut self) -> Option<TaskIdentity> {
        let (task, ()) = self.queue.pop()?;
        // The caller retains the same ready mutex across both operations.
        // No running metadata marker escapes into Task or foreign work.
        self.queue
            .finish(task, ())
            .expect("same original Task readiness marker");
        Some(task)
    }
    fn remove(&mut self, task: &TaskIdentity) {
        if self.contains(task) {
            self.queue
                .remove_queued(*task, ())
                .expect("same queued Task readiness marker");
        }
    }
    fn contains(&self, task: &TaskIdentity) -> bool {
        self.queue.contains(*task, ())
    }
    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

pub(super) struct TaskIoState {
    pub(super) admission_partial_sweep: super::requests::rejected_capture::AdmissionPartialSweep,
    pub(super) sources: BTreeMap<ActorId, TaskIdentity>,
    pub(super) domain_feedback_cuts: BTreeMap<
        (
            TaskIdentity,
            novarocks_worker_protocol::proposal_summary::CutId,
        ),
        novarocks_control_runtime_core::identity::ScopedWorkRef,
    >,
    pub(super) domain_requests:
        BTreeMap<TaskIdentity, BTreeSet<novarocks_control_runtime_core::identity::ScopedWorkRef>>,
    pub(super) domain_wakes: BTreeMap<
        TaskIdentity,
        (
            Option<novarocks_control_runtime_core::identity::ScopedWorkRef>,
            novarocks_control_runtime_core::identity::ScopedWorkRef,
            bool,
        ),
    >,
    pub(super) domain_wake_order: VecDeque<TaskIdentity>,
    notices: BTreeMap<TaskIdentity, Arc<TaskIoPending>>,
    admission_closed: bool,
    roots_sealed: bool,
    root_close_reason: Option<novarocks_execution_contract::root_lifetime::RootRetentionClose>,
    root_seal_cursor: Option<TaskIdentity>,
    root_seal_finished: bool,
    retained_seal_cursor: Option<TaskIdentity>,
    ready: Arc<Mutex<TaskReady>>,
    jobs: BTreeMap<TaskIdentity, TaskIoHostJob>,
    jobs_retry_after: Option<TaskIdentity>,
    physical_pending: VecDeque<TaskIoHostJob>,
    retirements: BTreeMap<TaskIdentity, TaskRetirement>,
    // Original payload disambiguates a physically retired rejected creation
    // from a later same-identity attempt admitted after its Core release fact.
    retiring_tasks: BTreeMap<(TaskIdentity, u64), Arc<crate::control_runtime::task::TaskDisposal>>,
    disposal_cursor: Option<(TaskIdentity, u64)>,
    failed_retirement_cursor: Option<TaskIdentity>,
    publication_refusal:
        Option<std::mem::ManuallyDrop<super::super::task_publication::TaskPublicationRefusal>>,
}
struct TaskRetirement {
    ticket: Option<EffectTicket<WorkerAction>>,
    execution_job_issued: bool,
    projection_committed: bool,
    finalize_execution_only: bool,
    disposal_authorization: Option<super::super::task_disposal::TaskDisposalAuthorization>,
    retained_custody: Option<(novarocks_worker_protocol::creation::PayloadId, Generation)>,
}
impl TaskIoState {
    pub(super) fn new(max_live_tasks: usize) -> Self {
        Self {
            admission_partial_sweep: Default::default(),
            sources: BTreeMap::new(),
            domain_feedback_cuts: BTreeMap::new(),
            domain_requests: BTreeMap::new(),
            domain_wakes: BTreeMap::new(),
            domain_wake_order: VecDeque::new(),
            notices: BTreeMap::new(),
            admission_closed: false,
            roots_sealed: false,
            root_close_reason: None,
            root_seal_cursor: None,
            root_seal_finished: false,
            retained_seal_cursor: None,
            ready: Arc::new(Mutex::new(TaskReady::new(max_live_tasks))),
            jobs: BTreeMap::new(),
            jobs_retry_after: None,
            physical_pending: VecDeque::new(),
            retirements: BTreeMap::new(),
            retiring_tasks: BTreeMap::new(),
            disposal_cursor: None,
            failed_retirement_cursor: None,
            publication_refusal: None,
        }
    }
    /// Continue an already owned Task's finite metadata work. Callers retain
    /// the original owner; this queue grants no Task or retirement authority.
    pub(super) fn queue_owned_task(&self, task: TaskIdentity) {
        self.ready
            .lock()
            .expect("original owned Task readiness")
            .insert(task);
    }
    pub(super) fn is_idle(&self) -> bool {
        self.sources.is_empty()
            && self.domain_feedback_cuts.is_empty()
            && self.domain_requests.is_empty()
            && self.domain_wakes.is_empty()
            && self.domain_wake_order.is_empty()
            && self
                .ready
                .lock()
                .expect("original Task ready inventory")
                .is_empty()
            && self.jobs.is_empty()
            && self.physical_pending.is_empty()
            && self.retirements.is_empty()
            && self.retiring_tasks.is_empty()
            && self.publication_refusal.is_none()
            && self.notices.is_empty()
    }
    pub(super) fn try_is_idle(&self) -> Option<bool> {
        let ready = self.ready.try_lock().ok()?;
        Some(
            self.sources.is_empty()
                && self.domain_feedback_cuts.is_empty()
                && self.domain_requests.is_empty()
                && self.domain_wakes.is_empty()
                && self.domain_wake_order.is_empty()
                && ready.is_empty()
                && self.jobs.is_empty()
                && self.physical_pending.is_empty()
                && self.retirements.is_empty()
                && self.retiring_tasks.is_empty()
                && self.publication_refusal.is_none()
                && self.notices.is_empty(),
        )
    }
}
impl ParticipationHostRecord {
    pub(crate) fn trace_drain_inventory(&self, budget: usize) {
        // Bound diagnostics by the same caller budget; counts describe only
        // this prefix of the original inventory, never all queued captures.
        let mut pending_kinds = [0usize; 6];
        for job in self.task_io.physical_pending.iter().take(budget) {
            let index = match job {
                TaskIoHostJob::Activate { .. } => 0,
                TaskIoHostJob::DisposeProjection { .. } => 1,
                TaskIoHostJob::RetireExecution { .. } => 2,
                TaskIoHostJob::DisposeTask { .. } => 3,
                TaskIoHostJob::RollbackPreparation { .. } => 4,
                _ => 5,
            };
            pending_kinds[index] += 1;
        }
        tracing::debug!(pid = std::process::id(), participation = ?self.identity,
            actor = ?self.control_runtime.snapshot(self.actor),
            scope = ?self.resources.scope.as_ref().map(|scope| scope.snapshot()),
            release_pending = self.resources.host_jobs.as_ref().map(|jobs| jobs.release_pending()),
            release_evidence_ready = self.resources.host_jobs.as_ref().map(|jobs| jobs.released_observation().is_some()),
            roots_sealed = self.task_io_roots_sealed(),
            release_root_cursor = ?self.resources.pending_host_jobs.root_release_after,
            release_roots_finished = self.resources.pending_host_jobs.root_release_finished,
            tasks = self.tasks.len(), jobs = self.task_io.jobs.len(),
            cleanup = self.task_io.physical_pending.len(), retirements = self.task_io.retirements.len(),
            pending_kinds_activate_projection_retire_dispose_rollback_other = ?pending_kinds,
            pending_kinds_complete = self.task_io.physical_pending.len() <= budget,
            close_fanouts = ?self.resources.pending_host_jobs.close_fanouts.iter()
                .filter_map(|f| f.ticket.as_ref().map(|t| (t.input, t.descriptor.id, t.descriptor.work)))
                .collect::<Vec<_>>(),
            actor_seal = ?self.resources.pending_host_jobs.actor_seal.as_ref()
                .map(|t| (t.input, t.descriptor.id, t.descriptor.work)),
            "Worker original Participation inventory at process drain");
        for request in self.resources.requests.values().take(budget) {
            request.trace_drain_inventory();
        }
        for (task, owner) in self.tasks.iter().take(budget) {
            match owner {
                super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                    let task = *task;
                    let retirement_flags = self.task_io.retirements.get(&task).map(|original| {
                        (
                            original.ticket.is_some(),
                            original.execution_job_issued,
                            original.projection_committed,
                            original.finalize_execution_only,
                        )
                    });
                    // Observe each original lock independently. These samples
                    // are diagnostics, not a coherent transition or exit proof.
                    let ready = self
                        .task_io
                        .ready
                        .lock()
                        .expect("original Task drain readiness")
                        .contains(&task);
                    let notice_active =
                        self.task_io.notices.get(&task).map(|notice| {
                            *notice.active.lock().expect("original Task drain notice")
                        });
                    tracing::debug!(
                        pid = std::process::id(),
                        ?task,
                        ?retirement_flags,
                        ready,
                        ?notice_active,
                        "Worker original Task retirement route at process drain"
                    );
                    owner.trace_draining_originals();
                }
                super::TaskHostOwner::Unpublished(_) => {}
            }
        }
    }
    /// Core commitment wakes the same original Task even when that step
    /// produced no effect. A producer's earlier send notice may already have
    /// been consumed before the pure actor committed its input.
    pub(crate) fn task_io_inputs_committed(
        &mut self,
        inputs: &[InputId],
    ) -> Result<(), TaskIoError> {
        for &input in inputs {
            let custody = self
                .control_runtime
                .input_custody(self.actor, input)
                .map_err(TaskIoError::ActorTurn)?
                .ok_or(TaskIoError::ActorTurn(ActorTurnError::UnknownInput))?;
            if let Some(&task) = self.task_io.sources.get(&custody.source) {
                self.notify_task_io_progress(task);
            }
        }
        Ok(())
    }
    pub(crate) fn handoff_task_io_effect(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        ports: &TaskIoPorts,
    ) -> Result<(), TaskIoRefusal> {
        let reject = |error, ticket| TaskIoRefusal {
            error,
            ticket: Some(ticket),
        };
        if matches!(
            &ticket.effect,
            WorkerAction::Participation(
                ParticipationEffect::ApplyTaskDomains { .. }
                    | ParticipationEffect::ApplyParticipationDomain { .. }
                    | ParticipationEffect::WaitTaskDomain { .. }
                    | ParticipationEffect::WaitParticipationDomain { .. }
                    | ParticipationEffect::WakeTaskDomains { .. }
                    | ParticipationEffect::WakeParticipationDomains
                    | ParticipationEffect::RecompareParticipationCredential { .. }
                    | ParticipationEffect::DomainRequestCompleted { .. }
            )
        ) {
            return self.handoff_domain_effect(ticket, ports);
        }

        if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
            return Err(reject(TaskIoError::ActorTurn(error), ticket));
        }
        if ticket.actor != self.actor {
            return Err(reject(TaskIoError::WrongOwner, ticket));
        }
        if let WorkerAction::Participation(ParticipationEffect::PrepareReadPayload {
            task, ..
        }) = &ticket.effect
        {
            let task = *task;
            let effect = crate::control_runtime::effect_dispatch::EffectRef {
                input: ticket.input,
                action: ticket.descriptor.id,
            };
            if let Err(error) = self.prepare_dynamic_filter_read(&ticket) {
                return Err(reject(TaskIoError::Request(error), ticket));
            }
            self.complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket))?;
            // Actual completion releases this original Task position AND
            // acknowledges the original reply's declared predecessor.
            return self
                .dispatch
                .task_preparation_completed(task, effect)
                .map_err(|error| TaskIoRefusal {
                    error: TaskIoError::Dispatch(error),
                    ticket: None,
                });
        }
        if let WorkerAction::Participation(ParticipationEffect::LoadInput {
            task, payload, ..
        }) = &ticket.effect
        {
            let (task, payload) = (*task, *payload);
            let Some(executor) = ports.loading.as_ref() else {
                return Err(reject(TaskIoError::MissingLoadingPort, ticket));
            };
            let (work, payload, mut binding) =
                match self.take_create_for_load(ticket.input, task, payload) {
                    Ok(originals) => originals,
                    Err(error) => return Err(reject(TaskIoError::Request(error), ticket)),
                };
            let task_notice = Arc::new(TaskIoPending {
                task,
                ready: self.task_io.ready.clone(),
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Participation ready notice")
                    .clone(),
                active: Mutex::new(true),
            });
            binding.pending = task_notice.clone();
            let runtime = self.control_runtime.clone();
            if let Err(error) = self.adopt_task(&runtime, &ticket, &binding) {
                self.restore_create_after_load_refusal(work, payload, binding);
                // The same Task notice already backs every admitted partial
                // child. Preserve it even when no port could be constructed.
                let retained = self.tasks.get(&task).is_some_and(|owner| match owner {
                    super::TaskHostOwner::Unpublished(owner) => owner.matches_loading(&ticket),
                    super::TaskHostOwner::Initializing(owner) => {
                        owner.resources_for_retirement().lifetime.work().reference()
                            == ticket.descriptor.work
                    }
                    super::TaskHostOwner::Ready(_) => false,
                });
                if retained {
                    assert!(
                        self.task_io
                            .notices
                            .insert(task, task_notice.clone())
                            .is_none()
                    );
                    crate::task_port::TaskPendingNotice::pending(task_notice.as_ref());
                }
                return Err(reject(TaskIoError::Task(error), ticket));
            }
            assert!(self.task_io.notices.insert(task, task_notice).is_none());
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            let result = self
                .task(task)
                .expect("same adopted Task")
                .submit_loading(payload, executor);
            if matches!(
                &result,
                Ok(crate::control_runtime::task::LoadingSubmission::Pending(_))
            ) && let (Some(capacity), Some(before)) = (&ports.capacity, before)
                && let Err(error) = capacity.register_waiter(self.registration(), before)
            {
                // Original job already moved into the actual Task owner.
                return Err(reject(TaskIoError::Capacity(error), ticket));
            }
            if let Err((error, payload)) = result {
                self.restore_create_after_load_refusal(work, payload, binding);
                return Err(reject(TaskIoError::Host(error), ticket));
            }
            if let Err(error) = self.create_transferred_to_loader(work) {
                return Err(reject(TaskIoError::Request(error), ticket));
            }
            self.notify_task_io_progress(task);
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Participation(ParticipationEffect::DisposeLoadedInput {
            task, ..
        }) = &ticket.effect
        {
            let task = *task;
            if ports.lifecycle.is_none() {
                return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
            }
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let job = match owner.take_rejected_loader_input(&ticket) {
                Ok(job) => job,
                Err(error) => return Err(reject(TaskIoError::Host(error), ticket)),
            };
            let pending = owner.pending_notice();
            let key = (ticket.descriptor.work.scope, ticket.descriptor.work.work);
            if let Err(error) = self.queue_task_cleanup(
                TaskIoHostJob::DisposeLoadedInput {
                    fairness: self.identity,
                    key,
                    runtime: self.control_runtime.clone(),
                    ticket: std::mem::ManuallyDrop::new(ticket),
                    job,
                    pending,
                },
                ports,
            ) {
                return Err(TaskIoRefusal {
                    error,
                    ticket: None,
                });
            }
            return Ok(());
        }
        if let WorkerAction::Participation(ParticipationEffect::EnqueuePreparation {
            task,
            initial_progressions,
            initial_pending,
            ..
        }) = &ticket.effect
        {
            let task = *task;
            let plan = initial_progressions.clone();
            let initial_pending = *initial_pending;
            let (Some(host), Some(executor)) =
                (ports.execution.as_ref(), ports.preparation.as_ref())
            else {
                return Err(reject(TaskIoError::MissingPreparationPort, ticket));
            };
            let runtime = self.control_runtime.clone();
            let root_close_reason = self.task_io.root_close_reason;
            let pending = match self.tasks.get(&task) {
                Some(super::TaskHostOwner::Ready(owner)) => owner.pending_notice(),
                _ => {
                    return Err(reject(
                        TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                        ticket,
                    ));
                }
            };
            let root_wake = Arc::new(TaskRootWake {
                task,
                pending,
                ready: self.task_io.ready.clone(),
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Participation Root notice")
                    .clone(),
                route: Mutex::new(RootWakeRoute::Task),
            });
            let owner = match self.tasks.get_mut(&task) {
                Some(super::TaskHostOwner::Ready(owner)) => owner.as_mut(),
                _ => {
                    return Err(reject(
                        TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                        ticket,
                    ));
                }
            };
            let mut job = match owner.adopt_preparation(&runtime, &ticket) {
                Ok(job) => job,
                Err(error) => return Err(reject(TaskIoError::Host(error), ticket)),
            };
            let capture = match owner.issue_preparation_capture() {
                Ok(capture) => capture,
                Err(error) => {
                    // This is still the original admitted job, retained for failed-owner cleanup.
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskIoError::Host(error), ticket));
                }
            };
            if let Err(capture) = owner.bind_host_preparation(&mut job, capture, ports.cadence) {
                std::mem::forget((job, capture));
                std::process::abort();
            }
            let installed = match owner.issue_installed_author() {
                Ok(author) => author,
                Err(error) => {
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskIoError::Host(error), ticket));
                }
            };
            let preparation = match owner.bind_production_preparation(root_wake) {
                Ok(custody) => custody,
                Err(error) => {
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskIoError::Host(error), ticket));
                }
            };
            if let Some(reason) = root_close_reason {
                preparation.close_root_issuance(reason);
            }
            let key = crate::control_runtime::preparation::QueuedPreparation::queue_key(&job);
            let job = TaskIoHostJob::Prepare {
                key,
                job,
                host: host.clone(),
                reporter: owner.reporter(),
                plan,
                initial_refusal: None,
                initial_pending,
                installed,
                owner: preparation,
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if initial_pending {
                assert!(self.task_io.jobs.insert(task, job).is_none());
            } else if let Err((_, job)) = executor.try_submit(job) {
                assert!(self.task_io.jobs.insert(task, job).is_none());
                if let (Some(capacity), Some(before)) = (&ports.capacity, before)
                    && let Err(error) = capacity.register_waiter(self.registration(), before)
                {
                    return Err(reject(TaskIoError::Capacity(error), ticket));
                }
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Participation(ParticipationEffect::InitialPreparationReady {
            task,
            progressions,
            refusal,
        }) = &ticket.effect
        {
            let task = *task;
            let Some(executor) = ports.preparation.as_ref() else {
                return Err(reject(TaskIoError::MissingPreparationPort, ticket));
            };
            let Some(mut job) = self.task_io.jobs.remove(&task) else {
                return Err(reject(TaskIoError::EffectRequiresOriginalOwner, ticket));
            };
            match &mut job {
                TaskIoHostJob::Prepare {
                    plan,
                    initial_refusal,
                    initial_pending,
                    ..
                } if *initial_pending => {
                    *plan = progressions.clone();
                    *initial_refusal = *refusal;
                    *initial_pending = false;
                }
                _ => {
                    assert!(self.task_io.jobs.insert(task, job).is_none());
                    return Err(reject(TaskIoError::EffectRequiresOriginalOwner, ticket));
                }
            }
            let diagnostic_key =
                crate::control_runtime::preparation::QueuedPreparation::queue_key(&job);
            if refusal.is_some() {
                tracing::debug!(pid = std::process::id(), ?task, work = ?diagnostic_key,
                    input = ?ticket.input, action = ?ticket.descriptor.id,
                    "Original rejected initial plan reached its paid preparation job");
            }
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit(job) {
                if refusal.is_some() {
                    tracing::debug!(pid = std::process::id(), ?task, work = ?diagnostic_key,
                        "Original rejected preparation remains queued after physical pool refusal");
                }
                assert!(self.task_io.jobs.insert(task, job).is_none());
                if let (Some(capacity), Some(before)) = (&ports.capacity, before)
                    && let Err(error) = capacity.register_waiter(self.registration(), before)
                {
                    return Err(reject(TaskIoError::Capacity(error), ticket));
                }
            }
            if refusal.is_some() {
                tracing::debug!(pid = std::process::id(), ?task, work = ?diagnostic_key,
                    still_queued = self.task_io.jobs.contains_key(&task),
                    "Original rejected initial plan handoff finished");
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Participation(ParticipationEffect::ApplyTaskClose {
            task,
            generation,
            intent,
        }) = &ticket.effect
        {
            let (task, generation, intent) = (*task, *generation, *intent);
            let reason = match intent {
                novarocks_worker_protocol::participation::TaskCloseIntent::Quiesce => None,
                novarocks_worker_protocol::participation::TaskCloseIntent::Abort(cause) => {
                    Some(cause)
                }
            };
            let Some(fanout) = self
                .resources
                .pending_host_jobs
                .close_fanouts
                .iter()
                .find(|fanout| fanout.generation == generation && fanout.reason == reason)
            else {
                return Err(reject(TaskIoError::WrongOwner, ticket));
            };
            let gate = fanout.gate.clone();
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if let Err(error) = owner.publish_participation_close(generation, intent, gate) {
                return Err(reject(TaskIoError::Host(error), ticket));
            }
            let fanout = self
                .resources
                .pending_host_jobs
                .close_fanouts
                .iter_mut()
                .find(|fanout| fanout.generation == generation && fanout.reason == reason)
                .expect("same original Participation close action");
            fanout.staged += 1;
            assert!(
                fanout
                    .targets
                    .is_none_or(|targets| fanout.staged <= targets),
                "the original Core scan bounds its actual fanout"
            );
            self.notify_task_io_progress(task);
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Participation(
            ParticipationEffect::RetireTask { task, .. }
            | ParticipationEffect::FinalizeTaskExecution { task, .. },
        ) = &ticket.effect
        {
            let task = *task;
            let finalize_execution_only = matches!(
                &ticket.effect,
                WorkerAction::Participation(ParticipationEffect::FinalizeTaskExecution { .. })
            );
            let retained_custody = match &ticket.effect {
                WorkerAction::Participation(ParticipationEffect::RetireTask {
                    payload,
                    close_epoch,
                    ..
                }) => Some((*payload, *close_epoch)),
                _ => None,
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if !finalize_execution_only
                && let Some(filters) = owner.dynamic_filters()
                && let Err(error) = filters.close_read_admission(&ticket)
            {
                return Err(reject(TaskIoError::DynamicFilter(error), ticket));
            }
            let disposal_authorization = if !finalize_execution_only {
                match owner.authorize_normal_disposal(&ticket) {
                    Ok(original) => Some(original),
                    Err(error) => return Err(reject(TaskIoError::Host(error), ticket)),
                }
            } else {
                None
            };
            if !finalize_execution_only {
                owner.close_request_admission();
            }
            let execution_job_issued = owner.production_retired();
            assert!(
                self.task_io
                    .retirements
                    .insert(
                        task,
                        TaskRetirement {
                            ticket: Some(ticket),
                            execution_job_issued,
                            projection_committed: false,
                            finalize_execution_only,
                            disposal_authorization,
                            retained_custody,
                        }
                    )
                    .is_none(),
                "one original Task retirement"
            );
            self.notify_task_io_progress(task);
            return Ok(());
        }
        if let WorkerAction::Participation(
            ParticipationEffect::PublishTask { task, .. }
            | ParticipationEffect::PublishMetrics { task, .. }
            | ParticipationEffect::PublishTaskConvergence { task, .. }
            | ParticipationEffect::SummaryConsumed { task, .. }
            | ParticipationEffect::PublishConclusion { task, .. },
        ) = &ticket.effect
        {
            let task = *task;
            if let Err(error) = self.adopt_installed_task_root(task) {
                return Err(reject(error, ticket));
            }
            self.notify_task_io_progress(task);
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let port = owner.port();
            let task_pending = owner.pending_notice();
            let old = match crate::control_runtime::task_publication::commit_task_projection(
                &self.control_runtime,
                &ticket,
                &port,
            ) {
                Ok(old) => old,
                Err(refusal) => {
                    let error = TaskIoError::PublicationRefused(refusal.error);
                    assert!(self.task_io.publication_refusal.is_none());
                    self.task_io.publication_refusal = Some(std::mem::ManuallyDrop::new(refusal));
                    return Err(reject(error, ticket));
                }
            };
            if let WorkerAction::Participation(ParticipationEffect::PublishTask {
                snapshot, ..
            }) = &ticket.effect
                && let Some(advertisement) = snapshot.dynamic_filters()
                && let Some(source) = self.task(task).and_then(|owner| owner.dynamic_filters())
                && let Err(error) = source.publication_committed(advertisement.version())
            {
                std::mem::forget((old, ticket));
                return Err(TaskIoRefusal {
                    error: TaskIoError::DynamicFilter(error),
                    ticket: None,
                });
            }
            if let Err(error) = self
                .dispatch
                .publication_committed(&self.control_runtime, &ticket)
            {
                // The original committed replacement remains supervised; an
                // ordering invariant failure cannot unwind its opaque source.
                std::mem::forget((old, ticket));
                return Err(TaskIoRefusal {
                    error: TaskIoError::Dispatch(error),
                    ticket: None,
                });
            }
            let job = TaskIoHostJob::DisposeProjection {
                fairness: self.identity,
                key: (ticket.descriptor.work.scope, ticket.descriptor.work.work),
                runtime: self.control_runtime.clone(),
                ticket,
                old: std::mem::ManuallyDrop::new(old),
                pending: task_pending,
            };
            // One Task lane's publication is synchronous. Its slow destructor
            // keeps the original ticket and source, while the lane is released.
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Participation(
                    self.identity,
                ),
            ) {
                self.task_io.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskIoRefusal {
                            error: TaskIoError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            if let Some(job) = self
                .task(task)
                .and_then(|owner| owner.take_production_activation())
            {
                // The prepared runnable owns the continuation. Activation
                // must not queue behind another preparation body waiting for
                // this Task's completion; it shares the paid rollback slot.
                self.queue_task_cleanup(job, ports)
                    .map_err(|error| TaskIoRefusal {
                        error,
                        ticket: None,
                    })?;
            }
            return Ok(());
        }
        if let WorkerAction::Participation(ParticipationEffect::DisposeSource { task, source }) =
            &ticket.effect
        {
            let task = *task;
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let source = match owner.port().ingress().take_source(*source) {
                Ok(source) => source,
                Err(error) => return Err(reject(TaskIoError::Source(error), ticket)),
            };
            let pending = owner.pending_notice();
            // Only the Core's committed loser selector authorizes taking this
            // original source. Its foreign destructor runs on the lifecycle
            // worker while the same effect ticket still owes actual exit.
            let job = TaskIoHostJob::DisposeSource {
                fairness: self.identity,
                key: (ticket.descriptor.work.scope, ticket.descriptor.work.work),
                runtime: self.control_runtime.clone(),
                ticket: std::mem::ManuallyDrop::new(ticket),
                source: Box::new(std::mem::ManuallyDrop::new(source)),
                pending,
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Participation(
                    self.identity,
                ),
            ) {
                self.task_io.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskIoRefusal {
                            error: TaskIoError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Participation(ParticipationEffect::StopTask { task, .. }) =
            &ticket.effect
        {
            let task = *task;
            let notice = self.request_notice_for_input(ticket.input);
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let job = match owner
                .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                .handoff_stop_effect(ticket, notice)
            {
                Ok(job) => TaskIoHostJob::Stop { job },
                Err((error, ticket)) => {
                    return Err(reject(TaskIoError::HostCall(error), ticket));
                }
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Participation(
                    self.identity,
                ),
            ) {
                self.task_io.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskIoRefusal {
                            error: TaskIoError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Participation(ParticipationEffect::SettlementBarrier { task, work }) =
            &ticket.effect
        {
            let (task, work) = (*task, *work);
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskIoError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let Some(completion_set) = owner.resources_for_retirement().completion.clone() else {
                return Err(reject(TaskIoError::EffectRequiresOriginalOwner, ticket));
            };
            let port = owner.port();
            if let Err((error, ticket)) = completion_set.handoff_settlement_effect(
                &self.control_runtime,
                &mut self.dispatch,
                ticket,
                &port,
            ) {
                return Err(reject(TaskIoError::Settlement(error), ticket));
            }
            let job = TaskIoHostJob::Settlement {
                key: (work.scope, work.work),
                completion_set,
                port: std::mem::ManuallyDrop::new(port),
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Participation(
                    self.identity,
                ),
            ) {
                self.task_io.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskIoRefusal {
                            error: TaskIoError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Participation(ParticipationEffect::WakeTaskDomains { task }) =
            &ticket.effect
        {
            let task = *task;
            if !matches!(self.tasks.get(&task), Some(super::TaskHostOwner::Ready(_))) {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            }
            // Wait effects remain with their original caller until a real
            // waiter owner accepts them; this adapter has accepted none.
            // This notification reaches the original Task metadata owner.
            self.notify_task_io_progress(task);
            self.resources
                .notice
                .as_ref()
                .expect("original Participation notification")
                .notify();
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if matches!(
            &ticket.effect,
            WorkerAction::Participation(ParticipationEffect::WakeParticipationDomains)
        ) {
            self.resources
                .notice
                .as_ref()
                .expect("original Participation notification")
                .notify();
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Participation(ParticipationEffect::CancelLoader { task, .. }) =
            &ticket.effect
        {
            let Some(owner) = self.task(*task) else {
                return Err(reject(
                    TaskIoError::Task(ParticipationHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if let Some(loader) = owner.loader() {
                loader.cancel();
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskIoError::ActorTurn(refusal.error), refusal.ticket));
        }
        let kind = match &ticket.effect {
            WorkerAction::Participation(effect) => match effect {
                ParticipationEffect::ApplyTaskClose { .. } => "ApplyTaskClose",
                ParticipationEffect::PrepareReadPayload { .. } => "PrepareReadPayload",
                ParticipationEffect::ApplyTaskDomains { .. } => "ApplyTaskDomains",
                ParticipationEffect::WaitTaskDomain { .. } => "WaitTaskDomain",
                ParticipationEffect::WakeTaskDomains { .. } => "WakeTaskDomains",
                ParticipationEffect::ApplyParticipationDomain { .. } => "ApplyParticipationDomain",
                ParticipationEffect::WaitParticipationDomain { .. } => "WaitParticipationDomain",
                ParticipationEffect::DisposeSource { .. } => "DisposeSource",
                _ => "ParticipationEffect",
            },
            _ => "WorkerAction",
        };
        Err(reject(TaskIoError::UnsupportedEffect(kind), ticket))
    }
    pub(crate) fn advance_task_io(
        &mut self,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<TaskIoProgress, TaskIoError> {
        let mut progress = TaskIoProgress::default();
        // Accepted jobs are cumulative-bounded by the Participation's original T.
        // Advance after every inspection, including an outstanding initial
        // plan, so a low address cannot monopolize capacity retry hints.
        let retry_budget = budget.min(self.task_io.jobs.len());
        for _ in 0..retry_budget {
            use std::ops::Bound::{Excluded, Unbounded};
            let next = self
                .task_io
                .jobs
                .range((
                    self.task_io.jobs_retry_after.map_or(Unbounded, Excluded),
                    Unbounded,
                ))
                .next()
                .or_else(|| self.task_io.jobs.first_key_value())
                .map(|(&task, job)| (task, !job.awaiting_initial_plan()));
            let Some((task, runnable)) = next else {
                break;
            };
            self.task_io.jobs_retry_after = Some(task);
            if runnable {
                self.task_io
                    .ready
                    .lock()
                    .expect("original pending Task address")
                    .insert(task);
            }
        }

        for _ in 0..budget {
            let Some(job) = self.task_io.physical_pending.pop_front() else {
                break;
            };
            if let Some(executor) = &ports.lifecycle {
                let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                if let Err((_, original)) = executor.try_submit_for_group(
                    job,
                    crate::control_runtime::preparation::HostFairnessGroup::Participation(
                        self.identity,
                    ),
                ) {
                    self.task_io.physical_pending.push_front(original);
                    if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                        capacity
                            .register_waiter(self.registration(), before)
                            .map_err(TaskIoError::Capacity)?;
                    }
                    break;
                }
            } else {
                self.task_io.physical_pending.push_front(job);
                break;
            }
        }

        for _ in 0..budget {
            let Some(task) = self
                .task_io
                .ready
                .lock()
                .expect("Task ready metadata")
                .pop()
            else {
                break;
            };
            progress.visited += 1;
            if let Some(job) = self.task_io.jobs.remove(&task) {
                if job.awaiting_initial_plan() {
                    self.task_io.jobs.insert(task, job);
                } else if let Some(executor) = &ports.preparation {
                    let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                    if let Err((_, original)) = executor.try_submit(job) {
                        self.task_io.jobs.insert(task, original);
                        if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                            capacity
                                .register_waiter(self.registration(), before)
                                .map_err(TaskIoError::Capacity)?;
                        }
                    }
                } else {
                    self.task_io.jobs.insert(task, job);
                }
            }
            self.adopt_installed_task_root(task)?;
            let mut loading_wait = None;
            let mut source_progress = false;
            let mut retirement_runnable = false;
            let mut retired_authors = Vec::new();
            let mut request_cut = None;
            let ledger = self.ledger.clone();
            let reaped = ledger
                .reap_scope_ready(self.scope().id(), 1)
                .map_err(|error| TaskIoError::Host(TaskHostError::Runtime(error)))?;
            if let Some(host) = self.task(task) {
                if reaped > 0 {
                    host.pending_notice().pending();
                }
                if let Some(executor) = &ports.loading {
                    let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                    if matches!(
                        host.retry_loading_submission(executor),
                        crate::control_runtime::task::LoadingSubmission::Pending(_)
                    ) {
                        loading_wait = before;
                    }
                }
                if let Some(returned) = host.take_loading_return().map_err(TaskIoError::Host)?
                    && let Err((error, original)) = host.observe_loading_job_return(returned)
                {
                    // A broken original return binding cannot be unwound through opaque custody.
                    std::mem::forget(original);
                    return Err(TaskIoError::Host(error));
                }
                if let Some(receipt) = host.publish_loading_exit().map_err(TaskIoError::Loading)?
                    && let Err((error, original)) = host.observe_loading_author_return(receipt)
                {
                    std::mem::forget(original);
                    return Err(TaskIoError::Host(error));
                }
                host.observe_loading_exit_report()
                    .map_err(TaskIoError::Loading)?;
                let open = host.advance_open_summary().map_err(|error| {
                    tracing::error!(?task, operation = "advance_open_summary", %error,
                            "Original Task host transition refused");
                    TaskIoError::Host(error)
                })?;
                source_progress |= open.progressed;
                retired_authors.extend(open.retired_authors);
                if open.progressed {
                    host.pending_notice().pending();
                }
                let retirement = host.advance_production_cut().map_err(|error| {
                    tracing::error!(?task, operation = "advance_production_cut", %error,
                            "Original Task host transition refused");
                    TaskIoError::Host(error)
                })?;
                retirement_runnable = retirement.runnable;
                retired_authors.extend(retirement.retired_authors);
                request_cut = retirement.request_cut;
                if retirement.progressed {
                    source_progress = true;
                    host.pending_notice().pending();
                }

                if host.advance_production_facts().map_err(|error| {
                    tracing::error!(?task, operation = "advance_production_facts", %error,
                            "Original Task host transition refused");
                    TaskIoError::Host(error)
                })? {
                    source_progress = true;
                    host.pending_notice().pending();
                }
                if host.advance_normal_retirement(&ledger).map_err(|error| {
                    tracing::error!(?task, operation = "advance_normal_retirement", %error,
                            "Original Task host transition refused");
                    TaskIoError::Host(error)
                })? {
                    host.pending_notice().pending();
                }
            }
            self.notify_retired_input_authors(&retired_authors);
            if let Some(cut) = request_cut
                && let Some(original) = self
                    .task_request_cuts
                    .get(&(task, cut))
                    .and_then(|work| self.resources.requests.get(work))
            {
                // Physical effect jobs wake their original Task. Its finite
                // cut visit forwards that event to the same admitted R; it
                // neither scans requests nor claims Core/physical exit.
                original.note_input_progress();
            }
            if retirement_runnable {
                // Continue the frozen finite round without manufacturing a
                // new producer/IO epoch. Coalesced external notices retain
                // their independently recorded epoch for the next round.
                self.task_io
                    .ready
                    .lock()
                    .expect("finite Task retirement continuation")
                    .insert(task);
            }
            if let Some(execution) = &ports.execution
                && let Some(job) = self
                    .task(task)
                    .and_then(|owner| owner.take_preparation_rollback_job(execution.clone()))
            {
                self.queue_task_cleanup(job, ports)?;
            }
            if source_progress || reaped > 0 {
                self.notify_domain_source_progress(task);
            }
            let rf_progress = match self.task(task).and_then(|host| host.dynamic_filters()) {
                Some(source) => Some(source.advance().map_err(TaskIoError::DynamicFilter)?),
                None => None,
            };
            if let Some(rf) = rf_progress {
                if rf.progressed
                    && let Some(host) = self.task(task)
                {
                    host.pending_notice().pending();
                }
                if let Some(job) = rf.job {
                    self.task_io
                        .physical_pending
                        .push_back(TaskIoHostJob::DynamicFilter(job));
                    self.resources
                        .notice
                        .as_ref()
                        .expect("original RF physical dispatch")
                        .notify();
                }
            }
            if source_progress {
                self.notify_deferred_root_source_progress(task);
            }
            if let (Some(capacity), Some(before)) = (&ports.capacity, loading_wait) {
                capacity
                    .register_waiter(self.registration(), before)
                    .map_err(TaskIoError::Capacity)?;
            }
            if self
                .deliver_task_event(task)
                .map_err(TaskIoError::Request)?
                .is_some()
            {
                progress.delivered += 1;
                self.task_io
                    .ready
                    .lock()
                    .expect("Task ready metadata")
                    .insert(task);
                self.notify_deferred_root_source_progress(task);
            }
            self.advance_task_retirement(task, ports)?;
        }
        let disposal_progress = self.advance_retired_task_disposals(budget)?;
        progress.runnable = disposal_progress
            || !self
                .task_io
                .ready
                .lock()
                .expect("Task ready metadata")
                .is_empty();
        Ok(progress)
    }
    pub(crate) fn close_task_io_admission(
        &mut self,
        participation: ParticipationRef,
        _generation: Generation,
        _ports: &TaskIoPorts,
    ) -> Result<bool, TaskIoError> {
        if participation != self.identity {
            return Err(TaskIoError::WrongOwner);
        }
        self.task_io.admission_closed = true;
        self.control_runtime
            .close_actor_admission(self.actor)
            .map_err(TaskIoError::ActorTurn)?;
        // Existing admitted return/cleanup roles remain usable. New Task/R
        // issuers must inspect this same original gate before any capture.
        Ok(true)
    }
    pub(super) fn task_io_roots_sealed(&self) -> bool {
        self.task_io.root_seal_finished
    }
    pub(crate) fn seal_task_roots(
        &mut self,
        participation: ParticipationRef,
        _generation: Generation,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
        seal_work: novarocks_control_runtime_core::identity::ScopedWorkRef,
        ports: &TaskIoPorts,
    ) -> Result<bool, TaskIoError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if participation != self.identity {
            return Err(TaskIoError::WrongOwner);
        }
        self.task_io.roots_sealed = true;
        let reason = *self.task_io.root_close_reason.get_or_insert(reason);
        if self.task_io.root_seal_finished {
            return self.seal_retained_roots(seal_work, reason, ports);
        }
        let next = self
            .tasks
            .range((
                self.task_io.root_seal_cursor.map_or(Unbounded, Excluded),
                Unbounded,
            ))
            .next()
            .map(|(task, _)| *task);
        let Some(task) = next else {
            self.task_io.root_seal_finished = true;
            return self.seal_retained_roots(seal_work, reason, ports);
        };
        let known = match self.tasks.get_mut(&task).expect("same Task inventory") {
            super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                owner.close_root_issuance(reason)
            }
            super::TaskHostOwner::Unpublished(_) => true,
        };
        self.adopt_installed_task_root(task)?;
        // A running original installation may still return its Root. Keep
        // this same Seal effect until that outcome is actually registered.
        if known {
            self.task_io.root_seal_cursor = Some(task);
            self.ready_notice().notify();
        }
        Ok(false)
    }
}

impl ParticipationHostRecord {
    fn adopt_installed_task_root(&mut self, task: TaskIdentity) -> Result<(), TaskIoError> {
        let root = match self.task(task) {
            Some(owner) => owner
                .take_root_participation_retention()
                .map_err(TaskIoError::Host)?,
            None => None,
        };
        if let Some(root) = root {
            let scope = self.scope().clone();
            if let Err((error, root)) =
                self.resources
                    .root_reads
                    .adopt_installed_root(self.identity, &scope, root)
            {
                if let Err(original) = self
                    .task(task)
                    .expect("same original Root issuer")
                    .restore_root_participation_retention(root)
                {
                    std::mem::forget(original);
                    std::process::abort();
                }
                return Err(TaskIoError::Task(ParticipationHostError::RootRead(error)));
            }
        }
        Ok(())
    }
    fn seal_retained_roots(
        &mut self,
        work: novarocks_control_runtime_core::identity::ScopedWorkRef,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
        ports: &TaskIoPorts,
    ) -> Result<bool, TaskIoError> {
        use crate::control_runtime::task::RetainedRootSealPreparation;
        let Some((task, state)) = self
            .resources
            .root_reads
            .prepare_retained_seal(self.task_io.retained_seal_cursor, reason)
            .map_err(TaskIoError::RetainedRootSeal)?
        else {
            return Ok(true);
        };
        match state {
            RetainedRootSealPreparation::Ready => {
                self.task_io.retained_seal_cursor = Some(task);
                self.ready_notice().notify();
            }
            RetainedRootSealPreparation::Pending => {}
            RetainedRootSealPreparation::Job(job) => {
                self.queue_task_cleanup(
                    TaskIoHostJob::SealRetainedRoot {
                        fairness: self.identity,
                        key: (work.scope, work.work),
                        job,
                    },
                    ports,
                )?;
            }
        }
        Ok(false)
    }
}
struct TaskIoPending {
    active: Mutex<bool>,
    task: TaskIdentity,
    ready: Arc<Mutex<TaskReady>>,
    notice: crate::control_runtime::participation_ready::ParticipationReadyNotice,
}
impl TaskIoPending {
    fn retire_to_participation(&self) {
        let mut active = self.active.lock().expect("original Task pending route");
        *active = false;
        self.ready
            .lock()
            .expect("same original Task pending route")
            .remove(&self.task);
    }
}
impl crate::task_port::TaskPendingNotice for TaskIoPending {
    fn pending(&self) {
        let active = self.active.lock().expect("original Task pending route");
        if *active {
            self.ready
                .lock()
                .expect("Task ready metadata")
                .insert(self.task);
        }
        self.notice.notify();
    }
}

/// One original queued preparation or its ordered activation capture. The
/// physical executor owns this value until its complete body/tail returns.
pub(crate) enum TaskIoHostJob {
    FailedTask(crate::control_runtime::task::FailedTaskAction),
    FailureTail(super::failure_tail::FailureTailJob),
    CloseParticipationAdmission(participation_admission::ParticipationAdmissionCloseJob),
    ParticipationForget(crate::control_runtime::participation_forget::ParticipationForgetJob),
    DisposeLoadedInput {
        fairness: ParticipationRef,
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        runtime: novarocks_control_runtime::actor_turn::ActorRuntime<WorkerActor>,
        ticket: std::mem::ManuallyDrop<EffectTicket<WorkerAction>>,
        job: crate::control_runtime::loading::InputDisposalJob,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    DisposeCreate(super::requests::create_disposal::TaskCreateDisposalJob),
    DisposeRejectedPartial(super::requests::rejected_capture::RejectedCaptureJob),
    DisposeRejectedDomain(super::requests::rejected_domain_capture::RejectedDomainJob),
    RequestObservation(crate::control_runtime::request_observation::RequestObservationJob),
    DynamicFilter(crate::control_runtime::dynamic_filters::TaskDynamicFilterHostJob),
    RollbackPreparation {
        preparation: crate::control_runtime::preparation::PreparationCustody,
        owner: Arc<crate::control_runtime::task::ProductionPreparation>,
        host: Arc<dyn crate::TaskExecutionHost>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    DisposeSource {
        fairness: ParticipationRef,
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        runtime: novarocks_control_runtime::actor_turn::ActorRuntime<WorkerActor>,
        ticket: std::mem::ManuallyDrop<EffectTicket<WorkerAction>>,
        source: Box<
            std::mem::ManuallyDrop<
                novarocks_execution_contract::RetainedTaskSource<
                    novarocks_execution::runtime::task::ExecutionFailure,
                >,
            >,
        >,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    Domain(super::requests::domain::DomainHostJob),
    DomainProjection(super::requests::domain_progress::DomainProjectionJob),
    SealRetainedRoot {
        fairness: ParticipationRef,
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        job: crate::control_runtime::task::RetainedRootSeal,
    },
    RetireExecution {
        job: crate::control_runtime::task_host_call::TaskHostCallJob,
        call: crate::control_runtime::task_host_call::TaskHostCall,
        owner: Option<Arc<crate::control_runtime::task::ProductionPreparation>>,
        host: Arc<dyn crate::TaskExecutionHost>,
        ingress: crate::TaskIngressRetirement,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    DisposeTask {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        owner: Arc<crate::control_runtime::task::TaskDisposal>,
        job: super::super::task_disposal::TaskDisposalJob,
        notice: crate::control_runtime::participation_ready::ParticipationReadyNotice,
    },

    ReplyNotification(crate::control_runtime::request_endpoint::RequestNotificationJob),
    Stop {
        job: crate::control_runtime::task_host_call::TaskHostCallJob,
    },
    Settlement {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        completion_set: Arc<crate::task_completion_set::TaskCompletionSet>,
        port: std::mem::ManuallyDrop<Arc<crate::task_port::TaskRuntimePort>>,
    },
    DisposeProjection {
        fairness: ParticipationRef,
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        runtime: novarocks_control_runtime::actor_turn::ActorRuntime<WorkerActor>,
        ticket: EffectTicket<WorkerAction>,
        old: std::mem::ManuallyDrop<Arc<crate::task_port::TaskProjection>>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    Prepare {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        job: crate::control_runtime::task::TaskPreparationJob,
        host: Arc<dyn crate::TaskExecutionHost>,
        reporter: crate::TaskStatusReporter,
        plan: Arc<Vec<novarocks_execution_contract::DomainProgression>>,
        initial_refusal: Option<novarocks_execution_contract::DomainConflict>,
        initial_pending: bool,
        installed: crate::control_runtime::causal_input::InputAuthor,
        owner: Arc<crate::control_runtime::task::ProductionPreparation>,
    },
    Activate {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        preparation: crate::control_runtime::preparation::PreparationCustody,
        owner: Arc<crate::control_runtime::task::ProductionPreparation>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
}
impl TaskIoHostJob {
    fn awaiting_initial_plan(&self) -> bool {
        matches!(
            self,
            Self::Prepare {
                initial_pending: true,
                ..
            }
        )
    }
}
impl crate::control_runtime::preparation::QueuedPreparation for TaskIoHostJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        match self {
            Self::FailedTask(job) => job.queue_key(),
            Self::FailureTail(job) => job.queue_key(),
            Self::CloseParticipationAdmission(job) => job.queue_key(),
            Self::ParticipationForget(job) => job.queue_key(),
            Self::RollbackPreparation { preparation, .. } => preparation.key(),
            Self::DisposeCreate(job) => job.queue_key(),
            Self::DisposeRejectedPartial(job) => job.queue_key(),
            Self::DisposeRejectedDomain(job) => job.queue_key(),
            Self::RequestObservation(job) => job.queue_key(),
            Self::DynamicFilter(job) => job.queue_key(),
            Self::RetireExecution { job, .. } => {
                crate::control_runtime::preparation::QueuedPreparation::queue_key(job)
            }
            Self::DisposeLoadedInput { key, .. }
            | Self::DisposeTask { key, .. }
            | Self::DisposeSource { key, .. }
            | Self::SealRetainedRoot { key, .. } => *key,
            Self::ReplyNotification(job) => job.queue_key(),
            Self::Domain(job) => {
                crate::control_runtime::preparation::QueuedPreparation::queue_key(job)
            }
            Self::DomainProjection(job) => {
                crate::control_runtime::preparation::QueuedPreparation::queue_key(job)
            }
            Self::Stop { job } => {
                crate::control_runtime::preparation::QueuedPreparation::queue_key(job)
            }
            Self::Settlement { key, .. } => *key,
            Self::Prepare { key, .. }
            | Self::Activate { key, .. }
            | Self::DisposeProjection { key, .. } => *key,
        }
    }
    fn fairness_group(
        &self,
        scope: novarocks_control_runtime_core::identity::ScopeId,
    ) -> crate::control_runtime::preparation::HostFairnessGroup {
        use crate::control_runtime::preparation::HostFairnessGroup;
        match self {
            Self::DisposeLoadedInput { fairness, .. }
            | Self::DisposeSource { fairness, .. }
            | Self::DisposeProjection { fairness, .. }
            | Self::SealRetainedRoot { fairness, .. } => {
                HostFairnessGroup::Participation(*fairness)
            }
            Self::CloseParticipationAdmission(job) => {
                HostFairnessGroup::Participation(job.participation())
            }
            Self::ParticipationForget(job) => HostFairnessGroup::Participation(job.participation()),
            _ => HostFairnessGroup::Scope(scope),
        }
    }
}
impl TaskIoHostJob {
    pub(crate) fn run_on_host(self) {
        match self {
            Self::FailedTask(job) => job.run(),
            Self::FailureTail(job) => job.run(),
            Self::CloseParticipationAdmission(job) => job.run(),
            Self::ParticipationForget(job) => job.run(),
            Self::RollbackPreparation {
                preparation,
                owner,
                host,
                pending,
            } => owner.rollback_on_host(&preparation, host.as_ref(), pending.as_ref()),
            Self::DisposeLoadedInput {
                runtime,
                ticket,
                job,
                pending,
                ..
            } => {
                if !job.run() {
                    // The original child retains its panic/Unknown exit. Do
                    // not complete this effect or retry a foreign destructor.
                    std::mem::forget((runtime, ticket, pending));
                    return;
                }
                let ticket = std::mem::ManuallyDrop::into_inner(ticket);
                if let Err(refusal) = runtime.effect_completed(ticket) {
                    std::mem::forget((refusal, pending));
                    std::process::abort();
                }
                pending.pending();
            }
            Self::DisposeCreate(job) => job.run(),
            Self::DisposeRejectedPartial(job) => job.run(),
            Self::DisposeRejectedDomain(job) => job.run(),
            Self::RequestObservation(job) => job.run(),
            Self::DynamicFilter(job) => job.run(),
            Self::DisposeSource {
                runtime,
                ticket,
                mut source,
                pending,
                key,
                ..
            } => {
                let input = ticket.input;
                let action = ticket.descriptor.id;
                tracing::debug!(
                    pid = std::process::id(),
                    ?key,
                    ?input,
                    ?action,
                    "Original source disposal body started"
                );
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // SAFETY: destruction is attempted once at this stable
                    // Box address. A partial unwind never moves or retries the
                    // source and never completes the original effect.
                    unsafe { std::mem::ManuallyDrop::drop(source.as_mut()) };
                }));
                if let Err(panic) = result {
                    std::mem::forget((runtime, ticket, source, pending));
                    std::panic::resume_unwind(panic);
                }
                drop(source);
                tracing::debug!(
                    pid = std::process::id(),
                    ?key,
                    ?input,
                    ?action,
                    "Original source disposal captures exited"
                );
                if let Err(refusal) =
                    runtime.effect_completed(std::mem::ManuallyDrop::into_inner(ticket))
                {
                    tracing::error!(pid = std::process::id(), error = ?refusal.error,
                        "Original source disposal effect completion failed");
                    std::mem::forget(refusal);
                    std::process::abort();
                }
                tracing::debug!(
                    pid = std::process::id(),
                    ?key,
                    ?input,
                    ?action,
                    "Original source disposal effect completed"
                );
                pending.pending();
            }
            Self::SealRetainedRoot { job, .. } => {
                if let Err(error) = job.finish_on_host() {
                    tracing::error!(?error, "retained Root sealing failed");
                }
            }
            Self::RetireExecution {
                job,
                call,
                owner,
                host,
                ingress,
                pending,
            } => {
                job.run_retirement(owner, host, ingress);
                drop(call);
                pending.pending();
            }
            Self::DisposeTask {
                job, owner, notice, ..
            } => {
                job.run(&owner);
                drop(owner);
                notice.notify();
            }
            Self::ReplyNotification(job) => job.run(),
            Self::Domain(job) => job.run_on_host(),
            Self::DomainProjection(job) => job.run_on_host(),
            Self::Stop { job } => job.run(),
            Self::Settlement {
                completion_set,
                port,
                ..
            } => {
                let port = std::mem::ManuallyDrop::into_inner(port);
                let _ = completion_set.dispatch_settlement(&port);
                drop(port);
                let _ = completion_set.finish_settlement_effect();
            }
            Self::DisposeProjection {
                runtime,
                ticket,
                old,
                pending,
                ..
            } => {
                drop(std::mem::ManuallyDrop::into_inner(old));
                if let Err(refusal) = runtime.effect_completed(ticket) {
                    tracing::error!(pid = std::process::id(), error = ?refusal.error,
                        "Original projection disposal effect completion failed");
                    std::mem::forget(refusal);
                    std::process::abort();
                }
                pending.pending();
            }
            Self::Prepare {
                job,
                host,
                reporter,
                plan,
                initial_refusal,
                installed,
                owner,
                ..
            } => {
                let _ =
                    job.install_complete(host, reporter, plan, initial_refusal, installed, owner);
            }
            Self::Activate {
                preparation,
                owner,
                pending,
                ..
            } => {
                owner.activate(&preparation);
                pending.pending();
            }
        }
    }
}

#[derive(Clone, Copy)]
enum RootWakeRoute {
    Task,
    Participation,
}
/// This fixed I/O wake follows the actual Root owner. Transition and pending
/// Task-address removal share one lock with delivery of an already-started
/// wake, so transfer cannot leave a stale Task address after retirement.
pub(crate) struct TaskRootWake {
    task: TaskIdentity,
    pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    ready: Arc<Mutex<TaskReady>>,
    notice: crate::control_runtime::participation_ready::ParticipationReadyNotice,
    route: Mutex<RootWakeRoute>,
}
impl TaskRootWake {
    pub(crate) fn retain_by_participation(&self) {
        let mut route = self.route.lock().expect("original Root wake routing");
        *route = RootWakeRoute::Participation;
        self.ready
            .lock()
            .expect("same original Task ready address")
            .remove(&self.task);
    }
}
impl std::task::Wake for TaskRootWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let route = self.route.lock().expect("original Root wake routing");
        if matches!(*route, RootWakeRoute::Task) {
            self.pending.pending();
        } else {
            self.notice.notify();
        }
    }
}

impl ParticipationHostRecord {
    pub(super) fn queue_task_cleanup(
        &mut self,
        job: TaskIoHostJob,
        ports: &TaskIoPorts,
    ) -> Result<(), TaskIoError> {
        let Some(executor) = ports.lifecycle.as_ref() else {
            // The same original physical job remains owned even when startup
            // supplied no lifecycle executor; refusal cannot drop its captures.
            self.task_io.physical_pending.push_back(job);
            return Err(TaskIoError::MissingLifecyclePort);
        };
        let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
        if let Err((_, job)) = executor.try_submit_for_group(
            job,
            crate::control_runtime::preparation::HostFairnessGroup::Participation(self.identity),
        ) {
            self.task_io.physical_pending.push_back(job);
            if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                capacity
                    .register_waiter(self.registration(), before)
                    .map_err(TaskIoError::Capacity)?;
            }
        }
        Ok(())
    }
    pub(super) fn failed_task_ready(&self) -> bool {
        !self
            .task_io
            .ready
            .lock()
            .expect("original failed Task readiness")
            .is_empty()
    }

    pub(super) fn flush_task_cleanup_pending(
        &mut self,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<(), TaskIoError> {
        for _ in 0..budget {
            let Some(job) = self.task_io.physical_pending.pop_front() else {
                break;
            };
            let retained = self.task_io.physical_pending.len();
            self.queue_task_cleanup(job, ports)?;
            if self.task_io.physical_pending.len() > retained {
                break;
            }
        }
        Ok(())
    }

    /// Failure reuses the same ready addresses and original physical jobs.
    /// A stopped-scope job does not need a lost Core InitialReady instruction.
    pub(super) fn next_failed_task_step(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<Option<(TaskIdentity, crate::control_runtime::task::FailedTaskStep)>, TaskIoError>
    {
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(TaskIoError::ActorTurn)?;
        if budget == 0 {
            return Ok(None);
        }
        self.flush_task_cleanup_pending(ports, budget)?;
        let Some(task) = self
            .task_io
            .ready
            .lock()
            .expect("original failed Task readiness")
            .pop()
        else {
            return Ok(None);
        };
        if let Some(job) = self.task_io.jobs.remove(&task) {
            self.queue_task_cleanup(job, ports)?;
        }
        if matches!(
            self.tasks.get(&task),
            Some(super::TaskHostOwner::Unpublished(_))
        ) {
            let step = self.advance_unpublished_task(stage, task, budget)?;
            if step.runnable {
                self.task_io
                    .ready
                    .lock()
                    .expect("original unpublished ready address")
                    .insert(task);
            }
            return Ok(Some((task, step)));
        }
        let ledger = self.ledger.clone();
        let execution = ports
            .execution
            .as_ref()
            .ok_or(TaskIoError::MissingExecutionPort)?;
        let step = match self.tasks.get_mut(&task) {
            Some(
                super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
            ) => owner
                .advance_failed_retirement(stage, &ledger, execution, budget)
                .map_err(TaskIoError::Host)?,
            Some(super::TaskHostOwner::Unpublished(_)) => {
                unreachable!("handled original unpublished Task")
            }
            None => return Ok(None),
        };
        if step.runnable {
            self.task_io
                .ready
                .lock()
                .expect("original failed Task continuation")
                .insert(task);
        }
        Ok(Some((task, step)))
    }

    fn advance_unpublished_task(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        task: TaskIdentity,
        budget: usize,
    ) -> Result<crate::control_runtime::task::FailedTaskStep, TaskIoError> {
        let Some(super::TaskHostOwner::Unpublished(owner)) = self.tasks.get_mut(&task) else {
            return Err(TaskIoError::WrongOwner);
        };
        if !owner
            .advance_failed_unused(
                stage,
                &self.control_runtime,
                self.actor,
                &self.ledger,
                budget,
            )
            .map_err(TaskIoError::Host)?
        {
            return Ok(crate::control_runtime::task::FailedTaskStep {
                runnable: true,
                ..Default::default()
            });
        }
        let (input, payload) = (owner.loading_input, owner.payload);
        // The failed custodian must first settle the matching Started effect.
        // Its original pure input can then leave without waiting on Root R.
        if self
            .control_runtime
            .input_custody(self.actor, input)
            .map_err(TaskIoError::ActorTurn)?
            .is_some()
            && !self
                .retire_failed_unpublished_create_input(stage, input, task, payload)
                .map_err(TaskIoError::Request)?
        {
            return Ok(crate::control_runtime::task::FailedTaskStep::default());
        }
        let Some(super::TaskHostOwner::Unpublished(owner)) = self.tasks.get_mut(&task) else {
            unreachable!("same original unpublished retirement");
        };
        let (retired, runnable) = owner
            .retire_after_input(stage, &self.control_runtime, self.actor, budget)
            .map_err(TaskIoError::Host)?;
        if retired {
            // All fields are the same empty, truly exited bootstrap holders.
            // There was no port, execution host or opaque task carrier here.
            if let Some(notice) = self.task_io.notices.remove(&task) {
                notice.retire_to_participation();
            }
            self.task_io
                .ready
                .lock()
                .expect("retired unpublished address")
                .remove(&task);
            let original = self
                .tasks
                .remove(&task)
                .expect("same retired unpublished Task");
            drop(original);
            self.notify_host_progress();
        }
        Ok(crate::control_runtime::task::FailedTaskStep {
            runnable,
            ..Default::default()
        })
    }

    pub(super) fn retain_failed_retirement_ticket(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (TaskIoError, EffectTicket<WorkerAction>)> {
        let (task, finalize_execution_only, retained_custody) = match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::FinalizeTaskExecution {
                task,
                ..
            }) => (*task, true, None),
            WorkerAction::Participation(ParticipationEffect::RetireTask {
                task,
                payload,
                close_epoch,
                ..
            }) => (*task, false, Some((*payload, *close_epoch))),
            _ => return Err((TaskIoError::WrongOwner, ticket)),
        };
        if self.task_io.retirements.contains_key(&task) {
            return Err((TaskIoError::EffectRequiresOriginalOwner, ticket));
        }
        let Some(owner) = self.task(task) else {
            return Err((TaskIoError::WrongOwner, ticket));
        };
        if retained_custody.is_some_and(|(payload, _)| payload != owner.original_payload_id()) {
            return Err((TaskIoError::WrongOwner, ticket));
        }
        self.task_io.retirements.insert(
            task,
            TaskRetirement {
                ticket: Some(ticket),
                execution_job_issued: false,
                projection_committed: false,
                finalize_execution_only,
                disposal_authorization: None,
                retained_custody,
            },
        );
        Ok(())
    }

    /// Inspect one original retirement row. Normal jobs that already own the
    /// ticket stay on that job; this path never duplicates their old Arc.
    pub(super) fn failed_retirement_candidate(
        &mut self,
    ) -> Result<Option<(TaskIdentity, Arc<crate::task_port::TaskProjection>)>, TaskIoError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let next = self
            .task_io
            .retirements
            .range((
                self.task_io
                    .failed_retirement_cursor
                    .map_or(Unbounded, Excluded),
                Unbounded,
            ))
            .next()
            .or_else(|| self.task_io.retirements.first_key_value())
            .map(|(&task, row)| {
                (
                    task,
                    row.ticket.is_some(),
                    row.projection_committed,
                    row.finalize_execution_only,
                )
            });
        let Some((task, owns_ticket, committed, finalize_only)) = next else {
            return Ok(None);
        };
        self.task_io.failed_retirement_cursor = Some(task);
        let Some(owner) = self.task(task) else {
            return Err(TaskIoError::WrongOwner);
        };
        if !owns_ticket {
            if owner
                .retirement_projection_exited(finalize_only)
                .map_err(TaskIoError::Host)?
            {
                self.task_io.retirements.remove(&task);
                if let Some(owner) = self.task(task) {
                    owner.pending_notice().pending();
                }
            }
            return Ok(None);
        }
        if committed || !owner.production_retired() {
            return Ok(None);
        }
        let candidate = owner
            .port()
            .prepare_source_retirement()
            .map_err(TaskIoError::Projection)?;
        Ok(Some((task, Arc::new(candidate))))
    }

    pub(super) fn failed_retirement_projection_committed(&mut self, task: TaskIdentity) {
        self.task_io
            .retirements
            .get_mut(&task)
            .expect("original failed retirement")
            .projection_committed = true;
    }

    pub(super) fn complete_failed_retirement_projection(
        &mut self,
        task: TaskIdentity,
    ) -> Result<(), TaskIoError> {
        let row = self
            .task_io
            .retirements
            .get_mut(&task)
            .ok_or(TaskIoError::WrongOwner)?;
        let ticket = row.ticket.take().ok_or(TaskIoError::WrongOwner)?;
        if let Err(refusal) = self.complete_host_effect(ticket) {
            self.task_io.retirements.get_mut(&task).unwrap().ticket = Some(refusal.ticket);
            return Err(TaskIoError::ActorTurn(refusal.error));
        }
        self.task_io.retirements.remove(&task);
        if let Some(owner) = self.task(task) {
            owner.pending_notice().pending();
        }
        Ok(())
    }

    pub(super) fn retire_failed_task_owner(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        task: TaskIdentity,
        ports: &TaskIoPorts,
    ) -> Result<bool, TaskIoError> {
        // A completed physical callback and deletion of its original
        // retirement row are separate transitions. Preserve the Task until
        // this bounded owner visit has positively settled that same row.
        if self.task_io.retirements.contains_key(&task) {
            return Ok(false);
        }
        let (authorization, payload) = match self.tasks.get(&task) {
            Some(
                super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
            ) => (
                owner
                    .authorize_failed_disposal(stage)
                    .map_err(TaskIoError::Host)?,
                owner.original_payload_id(),
            ),
            _ => return Err(TaskIoError::Task(ParticipationHostError::TaskNotOwned)),
        };
        let owner = match self.take_failed_task(task, stage) {
            Ok(owner) => owner,
            Err(ParticipationHostError::TaskStillOwned) => return Ok(false),
            Err(error) => return Err(TaskIoError::Task(error)),
        };
        if let Some(notice) = self.task_io.notices.get(&task) {
            notice.retire_to_participation();
        }
        self.task_io.notices.remove(&task);
        let resources = match owner {
            super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                owner.into_resources()
            }
            super::TaskHostOwner::Unpublished(_) => unreachable!("sealed original failed Task"),
        };
        let (disposal, job) = crate::control_runtime::task::TaskDisposal::from_original(
            resources,
            ports.protocol_events.clone(),
            authorization,
        );
        let disposal = Arc::new(disposal);
        let key = disposal.key();
        self.task_io
            .ready
            .lock()
            .expect("removed failed Task address")
            .remove(&task);
        assert!(
            self.task_io
                .retiring_tasks
                .insert((task, payload.0), disposal.clone())
                .is_none()
        );
        self.queue_task_cleanup(
            TaskIoHostJob::DisposeTask {
                key,
                owner: disposal,
                job,
                notice: self.ready_notice().clone(),
            },
            ports,
        )?;
        Ok(true)
    }

    pub(super) fn advance_failed_task_disposals(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        ports: &TaskIoPorts,
        budget: usize,
    ) -> Result<bool, TaskIoError> {
        use crate::control_runtime::task_disposal::TaskDisposalError;
        use std::ops::Bound::{Excluded, Unbounded};
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(TaskIoError::ActorTurn)?;
        let mut progressed = false;
        for _ in 0..budget {
            let next = self
                .task_io
                .retiring_tasks
                .range((
                    self.task_io.disposal_cursor.map_or(Unbounded, Excluded),
                    Unbounded,
                ))
                .next()
                .or_else(|| self.task_io.retiring_tasks.first_key_value())
                .map(|(&key, owner)| (key, owner.clone()));
            let Some((key, owner)) = next else {
                break;
            };
            self.task_io.disposal_cursor = Some(key);
            match owner.take_failed_job(stage) {
                Ok(job) => {
                    self.queue_task_cleanup(
                        TaskIoHostJob::DisposeTask {
                            key: owner.key(),
                            owner,
                            job,
                            notice: self.ready_notice().clone(),
                        },
                        ports,
                    )?;
                    progressed = true;
                }
                Err(TaskDisposalError::AlreadyIssued) => {}
                Err(error) => return Err(TaskIoError::Host(TaskHostError::Disposal(error))),
            }
        }
        progressed |= self.advance_retired_task_disposals(budget)?;
        Ok(progressed)
    }

    fn advance_task_retirement(
        &mut self,
        task: TaskIdentity,
        ports: &TaskIoPorts,
    ) -> Result<(), TaskIoError> {
        let Some(retirement) = self.task_io.retirements.get(&task) else {
            return Ok(());
        };
        if !retirement.execution_job_issued {
            let host = ports
                .execution
                .as_ref()
                .ok_or(TaskIoError::MissingExecutionPort)?
                .clone();
            let ticket = retirement
                .ticket
                .as_ref()
                .expect("original uncommitted Task retirement");
            let owner = match self.tasks.get(&task) {
                Some(
                    super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
                ) => owner,
                _ => return Err(TaskIoError::Task(ParticipationHostError::TaskNotOwned)),
            };
            let job = owner
                .take_production_retire_job(ticket, host, &self.ledger)
                .map_err(TaskIoError::Host)?;
            if let Some(job) = job {
                self.task_io
                    .retirements
                    .get_mut(&task)
                    .expect("same retirement")
                    .execution_job_issued = true;
                self.queue_task_cleanup(job, ports)?;
            } else if owner.production_retired() {
                // An actually rejected pre-preparation Task has no foreign
                // execution owner. Both original unused host calls have now
                // really retired; continue its normal publication/Source tail.
                self.task_io
                    .retirements
                    .get_mut(&task)
                    .expect("same original rejection retirement")
                    .execution_job_issued = true;
                self.notify_task_io_progress(task);
            }
            return Ok(());
        }
        let retired = self
            .task(task)
            .is_some_and(|owner| owner.production_retired());
        if !retired {
            return Ok(());
        }
        if !self.task_io.retirements[&task].projection_committed {
            let retirement = self
                .task_io
                .retirements
                .get_mut(&task)
                .expect("original Task retirement");
            let ticket = retirement
                .ticket
                .take()
                .expect("original Task retirement effect");
            let port = self.task(task).expect("same original Task").port();
            let candidate =
                port.prepare_source_retirement()
                    .and_then(|candidate| match &ticket.effect {
                        WorkerAction::Participation(ParticipationEffect::RetireTask {
                            convergence,
                            ..
                        }) => candidate.with_convergence(*convergence),
                        WorkerAction::Participation(
                            ParticipationEffect::FinalizeTaskExecution { .. },
                        ) => Ok(candidate),
                        _ => unreachable!("original execution finalization or Task retirement"),
                    });
            let candidate = match candidate {
                Ok(candidate) => candidate,
                Err(error) => {
                    self.task_io.retirements.get_mut(&task).unwrap().ticket = Some(ticket);
                    return Err(TaskIoError::Projection(error));
                }
            };
            let observer = match self.control_runtime.effect_completion_observer(&ticket) {
                Ok(observer) => observer,
                Err(error) => {
                    self.task_io.retirements.get_mut(&task).unwrap().ticket = Some(ticket);
                    return Err(TaskIoError::ActorTurn(error));
                }
            };
            let old = match self.control_runtime.commit_snapshot(
                &ticket,
                port.publication(),
                Arc::new(candidate),
            ) {
                Ok(old) => old,
                Err((error, original)) => {
                    // This neutral detached candidate has no foreign source.
                    drop(original);
                    self.task_io.retirements.get_mut(&task).unwrap().ticket = Some(ticket);
                    return Err(TaskIoError::ActorTurn(error));
                }
            };
            let finalize_only = self.task_io.retirements[&task].finalize_execution_only;
            self.task(task)
                .expect("same original Task")
                .retain_retirement_projection(finalize_only, observer);
            self.dispatch
                .publication_committed(&self.control_runtime, &ticket)
                .map_err(TaskIoError::Dispatch)?;
            let pending = self
                .task(task)
                .expect("same original Task")
                .pending_notice();
            self.task_io
                .retirements
                .get_mut(&task)
                .unwrap()
                .projection_committed = true;
            self.queue_task_cleanup(
                TaskIoHostJob::DisposeProjection {
                    fairness: self.identity,
                    key: (ticket.descriptor.work.scope, ticket.descriptor.work.work),
                    runtime: self.control_runtime.clone(),
                    ticket,
                    old: std::mem::ManuallyDrop::new(old),
                    pending,
                },
                ports,
            )?;
            if self.task_io.retirements[&task].finalize_execution_only {
                self.task_io.retirements.remove(&task);
            }
            return Ok(());
        }
        if !self
            .task(task)
            .is_some_and(|owner| owner.normal_retirement_ready())
        {
            return Ok(());
        }
        let owner = match self.take_task(task) {
            Ok(owner) => owner,
            // A completed Task read seal does not remove the original R's
            // final routing address. Its actual deletion wakes this Task.
            Err(ParticipationHostError::TaskStillOwned) => return Ok(()),
            Err(error) => return Err(TaskIoError::Task(error)),
        };
        // The same original pending object routes late physical tails only
        // to the Participation after its real Task inventory closes. This is a
        // positive ownership transition, never a missing-Task fallback.
        if let Some(notice) = self.task_io.notices.get(&task) {
            notice.retire_to_participation();
        }
        self.task_io.notices.remove(&task);
        let resources = match owner {
            super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                owner.into_resources()
            }
            super::TaskHostOwner::Unpublished(_) => unreachable!("original sealed Task owner"),
        };
        let authorization = self
            .task_io
            .retirements
            .get_mut(&task)
            .expect("original Task retirement")
            .disposal_authorization
            .take()
            .expect("original validated disposal authorization");
        let (original_payload, original_epoch) = self.task_io.retirements[&task]
            .retained_custody
            .expect("original Core Task retirement payload and close epoch");
        let (disposal, job) = crate::control_runtime::task::TaskDisposal::from_original(
            resources,
            ports.protocol_events.clone(),
            authorization,
        );
        disposal.bind_custody_retirement(original_payload, original_epoch);
        let disposal = Arc::new(disposal);
        let key = job.key();
        self.task_io
            .ready
            .lock()
            .expect("removed original Task ready address")
            .remove(&task);
        self.task_io.retirements.remove(&task);
        assert!(
            self.task_io
                .retiring_tasks
                .insert((task, original_payload.0), disposal.clone())
                .is_none()
        );
        self.notify_deferred_root_source_progress(task);
        self.queue_task_cleanup(
            TaskIoHostJob::DisposeTask {
                key,
                owner: disposal,
                job,
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Participation notice")
                    .clone(),
            },
            ports,
        )
    }
    fn advance_retired_task_disposals(&mut self, budget: usize) -> Result<bool, TaskIoError> {
        use novarocks_control_runtime_core::completion::Consumption;
        use std::ops::Bound::{Excluded, Unbounded};
        let mut progress = false;
        for _ in 0..budget {
            let next = self
                .task_io
                .retiring_tasks
                .range((
                    self.task_io.disposal_cursor.map_or(Unbounded, Excluded),
                    Unbounded,
                ))
                .next()
                .or_else(|| self.task_io.retiring_tasks.first_key_value())
                .map(|(task, _)| *task);
            let Some(key) = next else {
                break;
            };
            self.task_io.disposal_cursor = Some(key);
            let task = key.0;
            let owner = self.task_io.retiring_tasks[&key].clone();
            progress |= owner
                .advance_retirement(&self.ledger)
                .map_err(TaskIoError::Host)?;
            if !owner.complete() {
                // One actual ready header is one bounded progress unit. Do not
                // equate payload Drop or ledger absence with its original exit.
                progress |= self
                    .ledger
                    .reap_scope_ready(self.scope().id(), 1)
                    .map_err(|error| TaskIoError::Host(TaskHostError::Runtime(error)))?
                    != 0;
                if !owner.complete() {
                    continue;
                }
            }
            let rejected_payload = owner.rejected_payload();
            let accepted_custody = owner.accepted_custody();
            if (rejected_payload.is_some() || accepted_custody.is_some())
                && !(owner.retirement_input().is_none()
                    && self.resources.internal.failed_publications_drained())
            {
                if let Some(input) = owner.retirement_input() {
                    match self
                        .resources
                        .internal
                        .finish_continuation(input)
                        .map_err(TaskIoError::ActorTurn)?
                    {
                        Consumption::Pending => continue,
                        Consumption::Consumed => {}
                        // This exact input's failed-owner receipt is not a
                        // normal Core decision. All physical exit gates above
                        // remain required before either branch removes custody.
                        Consumption::AbandonedByOwnerFailure => {}
                    }
                } else {
                    let input = if let Some(payload) = rejected_payload {
                        self.resources
                            .internal
                            .try_submit_rejected_creation(task, payload)
                    } else {
                        let (payload, close_epoch) =
                            accepted_custody.expect("same completed accepted Task custody");
                        self.resources.internal.try_submit_task_custody_retired(
                            task,
                            payload,
                            close_epoch,
                        )
                    }
                    .map_err(TaskIoError::ActorTurn)?;
                    if let Some(input) = input {
                        owner.bind_retirement_input(input);
                        if rejected_payload.is_some() {
                            // Only this rejected identity may be created again
                            // after its actual self-source fact is consumed.
                            self.retired_task_reads
                                .remove(&task)
                                .ok_or(TaskIoError::Publication)?;
                        }
                        progress = true;
                    }
                    continue;
                }
            }
            self.task_io.retiring_tasks.remove(&key);
            progress = true;
            // ReleaseHost may have parked earlier in this same turn. Notify
            // only this positive inventory transition, never an I/O wait.
            self.resources
                .notice
                .as_ref()
                .expect("original Participation disposal notice")
                .notify();
        }
        Ok(progress)
    }
}

#[cfg(test)]
impl ParticipationHostRecord {
    pub(crate) fn print_neutral_task_io_inventory(&self, task: TaskIdentity) {
        let flags = self.task_io.retirements.get(&task).map(|retirement| {
            (
                retirement.ticket.is_some(),
                retirement.execution_job_issued,
                retirement.projection_committed,
            )
        });
        // Snapshot each original metadata lock separately. The actual pending
        // route locks active before ready; diagnostic output must not retain
        // either guard while acquiring the other in a formatting expression.
        let ready = self
            .task_io
            .ready
            .lock()
            .expect("neutral Task readiness diagnostic")
            .contains(&task);
        let notice_active = self.task_io.notices.get(&task).map(|notice| {
            *notice
                .active
                .lock()
                .expect("original pending route diagnostic")
        });
        eprintln!(
            "Participation Task retirement task={task:?} retirement_flags={flags:?} ready={} pending_business={} pending_lifecycle={} disposal={:?} source_index={} notice_active={:?}",
            ready,
            self.task_io.jobs.contains_key(&task),
            self.task_io.physical_pending.len(),
            self.task_io
                .retiring_tasks
                .iter()
                .filter(|((identity, _), _)| *identity == task)
                .map(|(_, owner)| owner.complete())
                .collect::<Vec<_>>(),
            self.task_io
                .sources
                .values()
                .any(|candidate| *candidate == task),
            notice_active
        );
    }
}
