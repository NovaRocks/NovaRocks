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

//! The Context's actual Task I/O handoff. Unconfigured I/O never acknowledges
//! an effect; its exact ticket remains in the caller's existing paid slot.

use super::{ContextHostError, ContextHostRecord};
use crate::control_runtime::{
    actors::{WorkerAction, WorkerActor},
    effect_dispatch::DispatchError,
    host_executor::HostExecutorHandle,
    loading_host_job::LoadingHostJob,
    task::TaskHostError,
};
use novarocks_control_runtime::driver::{DriverError, EffectTicket};
use novarocks_control_runtime_core::identity::{ActorId, Generation, InputId};
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::context::ContextEffect;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Concrete original I/O ports. A cold Context can exist before Task intake is
/// opened; an absent port rejects capture rather than fabricating a host call.
#[derive(Clone, Default)]
pub(crate) struct TaskDriverPorts {
    pub protocol_events: Option<crate::TaskExecutionPorts>,
    pub observation: Option<super::observation::ObservationProjection>,
    pub execution: Option<Arc<dyn crate::TaskExecutionHost>>,
    pub loading: Option<HostExecutorHandle<LoadingHostJob>>,
    pub preparation: Option<HostExecutorHandle<TaskDriverHostJob>>,
    pub lifecycle: Option<HostExecutorHandle<TaskDriverHostJob>>,
    pub cadence: Duration,
    pub capacity: Option<crate::control_runtime::task_capacity::TaskCapacitySignal>,
}
impl TaskDriverPorts {
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
pub(crate) enum TaskDriverError {
    WrongOwner,
    MissingExecutionPort,
    MissingLoadingPort,
    MissingPreparationPort,
    MissingLifecyclePort,
    Capacity(crate::control_runtime::task_capacity::TaskCapacityError),
    HostCall(crate::control_runtime::task_host_call::TaskHostCallError),
    Settlement(crate::settlement::SettlementDispatchError),
    Publication,
    Task(ContextHostError),
    Host(TaskHostError),
    Driver(DriverError),
    Dispatch(DispatchError),
    Request(super::requests::ContextRequestError),
    Loading(super::super::loading_events::LoadingEventError),
    EffectRequiresOriginalOwner,
    UnsupportedEffect(&'static str),
    RetainedRootSeal(crate::control_runtime::task::RetainedRootSealError),
    DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError),
    Source(crate::control_runtime::source_positions::SourcePositionError),
}
pub(crate) struct TaskDriverRefusal {
    pub error: TaskDriverError,
    pub ticket: Option<EffectTicket<WorkerAction>>,
}
impl std::fmt::Debug for TaskDriverRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskDriverRefusal")
            .field("error", &self.error)
            .field("retains_original_ticket", &self.ticket.is_some())
            .finish()
    }
}
#[derive(Debug, Default)]
pub(crate) struct TaskDriverProgress {
    pub visited: usize,
    pub delivered: usize,
    pub runnable: bool,
}
#[derive(Default)]
pub(super) struct TaskDriverState {
    pub(super) sources: BTreeMap<ActorId, TaskIdentity>,
    pub(super) domain_feedback_cuts: BTreeMap<
        (
            TaskIdentity,
            novarocks_worker_protocol::proposal_summary::CutId,
        ),
        novarocks_control_runtime_core::identity::WorkId,
    >,
    pub(super) domain_requests:
        BTreeMap<TaskIdentity, BTreeSet<novarocks_control_runtime_core::identity::WorkId>>,
    pub(super) domain_wakes: BTreeMap<
        TaskIdentity,
        (
            Option<novarocks_control_runtime_core::identity::WorkId>,
            novarocks_control_runtime_core::identity::WorkId,
            bool,
        ),
    >,
    pub(super) domain_wake_order: VecDeque<TaskIdentity>,
    notices: BTreeMap<TaskIdentity, Arc<TaskDriverPending>>,
    admission_closed: bool,
    roots_sealed: bool,
    root_close_reason: Option<novarocks_execution_contract::root_lifetime::RootRetentionClose>,
    root_seal_cursor: Option<TaskIdentity>,
    root_seal_finished: bool,
    retained_seal_cursor: Option<TaskIdentity>,
    ready: Arc<Mutex<BTreeSet<TaskIdentity>>>,
    jobs: BTreeMap<TaskIdentity, TaskDriverHostJob>,
    physical_pending: VecDeque<TaskDriverHostJob>,
    retirements: BTreeMap<TaskIdentity, TaskRetirement>,
    retiring_tasks: BTreeMap<TaskIdentity, Arc<crate::control_runtime::task::RetiredTaskDisposal>>,
    disposal_cursor: Option<TaskIdentity>,
    publication_refusal:
        Option<std::mem::ManuallyDrop<super::super::task_publication::TaskPublicationRefusal>>,
}
struct TaskRetirement {
    ticket: Option<EffectTicket<WorkerAction>>,
    execution_job_issued: bool,
    projection_committed: bool,
    finalize_execution_only: bool,
    retained_state: Option<novarocks_execution_contract::TaskState>,
}
impl TaskDriverState {
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
}
impl ContextHostRecord {
    pub(crate) fn trace_drain_inventory(&self, budget: usize) {
        tracing::debug!(pid = std::process::id(), context = ?self.identity,
            actor = ?self.control_runtime.snapshot(self.actor), scope = ?self.scope().snapshot(),
            tasks = self.tasks.len(), jobs = self.task_driver.jobs.len(),
            cleanup = self.task_driver.physical_pending.len(), retirements = self.task_driver.retirements.len(),
            "Worker original Context inventory at process drain");
        for request in self.resources.requests.values().take(budget) {
            request.trace_drain_inventory();
        }
        for (_, owner) in self.tasks.iter().take(budget) {
            match owner {
                super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                    owner.trace_draining_originals();
                }
                super::TaskHostOwner::Unpublished(_) => {}
            }
        }
    }
    /// Core commitment wakes the same original Task even when that step
    /// produced no effect. A producer's earlier send notice may already have
    /// been consumed before the pure actor committed its input.
    pub(crate) fn task_driver_inputs_committed(
        &mut self,
        inputs: &[InputId],
    ) -> Result<(), TaskDriverError> {
        for &input in inputs {
            let custody = self
                .control_runtime
                .input_custody(self.actor, input)
                .map_err(TaskDriverError::Driver)?
                .ok_or(TaskDriverError::Driver(DriverError::UnknownInput))?;
            if let Some(&task) = self.task_driver.sources.get(&custody.source) {
                self.task_driver
                    .ready
                    .lock()
                    .expect("original Task commitment notice")
                    .insert(task);
            }
        }
        Ok(())
    }
    pub(crate) fn handoff_task_driver_effect(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        ports: &TaskDriverPorts,
    ) -> Result<(), TaskDriverRefusal> {
        let reject = |error, ticket| TaskDriverRefusal {
            error,
            ticket: Some(ticket),
        };
        if matches!(
            &ticket.effect,
            WorkerAction::Context(
                ContextEffect::ApplyTaskDomains { .. }
                    | ContextEffect::ApplyContextDomain { .. }
                    | ContextEffect::WaitTaskDomain { .. }
                    | ContextEffect::WaitContextDomain { .. }
                    | ContextEffect::WakeTaskDomains { .. }
                    | ContextEffect::WakeContextDomains
                    | ContextEffect::RecompareContextCredential { .. }
                    | ContextEffect::DomainRequestCompleted { .. }
            )
        ) {
            return self.handoff_domain_effect(ticket, ports);
        }

        if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
            return Err(reject(TaskDriverError::Driver(error), ticket));
        }
        if ticket.actor != self.actor {
            return Err(reject(TaskDriverError::WrongOwner, ticket));
        }
        if let WorkerAction::Context(ContextEffect::LoadInput { task, payload, .. }) =
            &ticket.effect
        {
            let (task, payload) = (*task, *payload);
            let Some(executor) = ports.loading.as_ref() else {
                return Err(reject(TaskDriverError::MissingLoadingPort, ticket));
            };
            let (work, payload, mut binding) =
                match self.take_create_for_load(ticket.input, task, payload) {
                    Ok(originals) => originals,
                    Err(error) => return Err(reject(TaskDriverError::Request(error), ticket)),
                };
            let task_notice = Arc::new(TaskDriverPending {
                task,
                ready: self.task_driver.ready.clone(),
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Context ready notice")
                    .clone(),
                active: Mutex::new(true),
            });
            binding.pending = task_notice.clone();
            let runtime = self.control_runtime.clone();
            if let Err(error) = self.adopt_task(&runtime, &ticket, binding) {
                self.restore_create_after_load_refusal(work, payload);
                return Err(reject(TaskDriverError::Task(error), ticket));
            }
            assert!(self.task_driver.notices.insert(task, task_notice).is_none());
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            let result = self
                .task(task)
                .expect("same adopted Task")
                .submit_loading(payload, executor);
            if matches!(
                &result,
                Ok(crate::control_runtime::task::LoadingSubmission::Pending(_))
            ) {
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    if let Err(error) = capacity.register_waiter(self.registration(), before) {
                        // Original job already moved into the actual Task owner.
                        return Err(reject(TaskDriverError::Capacity(error), ticket));
                    }
                }
            }
            if let Err((error, payload)) = result {
                self.restore_create_after_load_refusal(work, payload);
                return Err(reject(TaskDriverError::Host(error), ticket));
            }
            if let Err(error) = self.create_transferred_to_loader(work) {
                return Err(reject(TaskDriverError::Request(error), ticket));
            }
            self.task_driver
                .ready
                .lock()
                .expect("Task ready metadata")
                .insert(task);
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Context(ContextEffect::EnqueuePreparation {
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
                return Err(reject(TaskDriverError::MissingPreparationPort, ticket));
            };
            let runtime = self.control_runtime.clone();
            let root_close_reason = self.task_driver.root_close_reason;
            let root_wake = Arc::new(TaskRootWake {
                task,
                ready: self.task_driver.ready.clone(),
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Context Root notice")
                    .clone(),
                route: Mutex::new(RootWakeRoute::Task),
            });
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let mut job = match owner.adopt_preparation(&runtime, &ticket) {
                Ok(job) => job,
                Err(error) => return Err(reject(TaskDriverError::Host(error), ticket)),
            };
            let roles = match owner.preparation_roles() {
                Ok(roles) => roles,
                Err(error) => {
                    // This is still the original admitted job, retained for failed-owner cleanup.
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskDriverError::Host(error), ticket));
                }
            };
            if let Err(roles) = owner.bind_host_preparation(&mut job, roles, ports.cadence) {
                std::mem::forget((job, roles));
                std::process::abort();
            }
            let installed = match owner.issue_installed_author() {
                Ok(author) => author,
                Err(error) => {
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskDriverError::Host(error), ticket));
                }
            };
            let preparation = match owner.bind_production_preparation(root_wake) {
                Ok(custody) => custody,
                Err(error) => {
                    owner.retain_preparation_job(job);
                    return Err(reject(TaskDriverError::Host(error), ticket));
                }
            };
            if let Some(reason) = root_close_reason {
                preparation.close_root_issuance(reason);
            }
            let key = crate::control_runtime::preparation::QueuedPreparation::queue_key(&job);
            let job = TaskDriverHostJob::Prepare {
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
                assert!(self.task_driver.jobs.insert(task, job).is_none());
            } else if let Err((_, job)) = executor.try_submit(job) {
                assert!(self.task_driver.jobs.insert(task, job).is_none());
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    if let Err(error) = capacity.register_waiter(self.registration(), before) {
                        return Err(reject(TaskDriverError::Capacity(error), ticket));
                    }
                }
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Context(ContextEffect::InitialPreparationReady {
            task,
            progressions,
            refusal,
        }) = &ticket.effect
        {
            let task = *task;
            let Some(executor) = ports.preparation.as_ref() else {
                return Err(reject(TaskDriverError::MissingPreparationPort, ticket));
            };
            let Some(mut job) = self.task_driver.jobs.remove(&task) else {
                return Err(reject(TaskDriverError::EffectRequiresOriginalOwner, ticket));
            };
            match &mut job {
                TaskDriverHostJob::Prepare {
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
                    assert!(self.task_driver.jobs.insert(task, job).is_none());
                    return Err(reject(TaskDriverError::EffectRequiresOriginalOwner, ticket));
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
                assert!(self.task_driver.jobs.insert(task, job).is_none());
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    if let Err(error) = capacity.register_waiter(self.registration(), before) {
                        return Err(reject(TaskDriverError::Capacity(error), ticket));
                    }
                }
            }
            if refusal.is_some() {
                tracing::debug!(pid = std::process::id(), ?task, work = ?diagnostic_key,
                    still_queued = self.task_driver.jobs.contains_key(&task),
                    "Original rejected initial plan handoff finished");
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Context(ContextEffect::ApplyTaskClose {
            task,
            generation,
            intent,
        }) = &ticket.effect
        {
            let (task, generation, intent) = (*task, *generation, *intent);
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if let Err(error) = owner.publish_context_close(generation, intent) {
                return Err(reject(TaskDriverError::Host(error), ticket));
            }
            self.task_driver
                .ready
                .lock()
                .expect("original Context close delivery")
                .insert(task);
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Context(
            ContextEffect::RetireTask { task, .. } | ContextEffect::FinalizeTaskExecution { task },
        ) = &ticket.effect
        {
            let task = *task;
            let finalize_execution_only = matches!(
                &ticket.effect,
                WorkerAction::Context(ContextEffect::FinalizeTaskExecution { .. })
            );
            let retained_state = match &ticket.effect {
                WorkerAction::Context(ContextEffect::RetireTask { state, .. }) => Some(*state),
                _ => None,
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if !finalize_execution_only {
                owner.close_request_admission();
            }
            let execution_job_issued = owner.production_retired();
            assert!(
                self.task_driver
                    .retirements
                    .insert(
                        task,
                        TaskRetirement {
                            ticket: Some(ticket),
                            execution_job_issued,
                            projection_committed: false,
                            finalize_execution_only,
                            retained_state,
                        }
                    )
                    .is_none(),
                "one original Task retirement"
            );
            self.task_driver
                .ready
                .lock()
                .expect("original Task retirement notice")
                .insert(task);
            return Ok(());
        }
        if let WorkerAction::Context(
            ContextEffect::PublishTask { task, .. }
            | ContextEffect::PublishMetrics { task, .. }
            | ContextEffect::PublishTaskConvergence { task, .. }
            | ContextEffect::SummaryConsumed { task, .. }
            | ContextEffect::PublishConclusion { task, .. },
        ) = &ticket.effect
        {
            let task = *task;
            if let Err(error) = self.adopt_installed_task_root(task) {
                return Err(reject(error, ticket));
            }
            self.task_driver
                .ready
                .lock()
                .expect("Task ready metadata")
                .insert(task);
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskDriverError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
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
                    let error = TaskDriverError::Publication;
                    assert!(self.task_driver.publication_refusal.is_none());
                    self.task_driver.publication_refusal =
                        Some(std::mem::ManuallyDrop::new(refusal));
                    return Err(reject(error, ticket));
                }
            };
            if let WorkerAction::Context(ContextEffect::PublishTask { snapshot, .. }) =
                &ticket.effect
            {
                if let Some(advertisement) = snapshot.dynamic_filters() {
                    if let Some(source) = self.task(task).and_then(|owner| owner.dynamic_filters())
                    {
                        if let Err(error) = source.publication_committed(advertisement.version()) {
                            std::mem::forget((old, ticket));
                            return Err(TaskDriverRefusal {
                                error: TaskDriverError::DynamicFilter(error),
                                ticket: None,
                            });
                        }
                    }
                }
            }
            if let Err(error) = self
                .dispatch
                .publication_committed(&self.control_runtime, &ticket)
            {
                // The original committed replacement remains supervised; an
                // ordering invariant failure cannot unwind its opaque source.
                std::mem::forget((old, ticket));
                return Err(TaskDriverRefusal {
                    error: TaskDriverError::Dispatch(error),
                    ticket: None,
                });
            }
            let job = TaskDriverHostJob::DisposeProjection {
                key: (self.scope().id(), ticket.descriptor.work),
                runtime: self.control_runtime.clone(),
                ticket,
                old: std::mem::ManuallyDrop::new(old),
                pending: task_pending,
            };
            // One Task lane's publication is synchronous. Its slow destructor
            // keeps the original ticket and source, while the lane is released.
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit(job) {
                self.task_driver.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskDriverRefusal {
                            error: TaskDriverError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            if let Some(job) = self
                .task(task)
                .and_then(|owner| owner.take_production_activation())
            {
                let Some(executor) = ports.preparation.as_ref() else {
                    self.task_driver.jobs.insert(task, job);
                    return Err(TaskDriverRefusal {
                        error: TaskDriverError::MissingPreparationPort,
                        ticket: None,
                    });
                };
                let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                if let Err((_, job)) = executor.try_submit(job) {
                    assert!(self.task_driver.jobs.insert(task, job).is_none());
                    if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                        capacity
                            .register_waiter(self.registration(), before)
                            .map_err(|error| TaskDriverRefusal {
                                error: TaskDriverError::Capacity(error),
                                ticket: None,
                            })?;
                    }
                }
            }
            return Ok(());
        }
        if let WorkerAction::Context(ContextEffect::DisposeSource { task, source }) = &ticket.effect
        {
            let task = *task;
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskDriverError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let source = match owner.port().ingress().take_source(*source) {
                Ok(source) => source,
                Err(error) => return Err(reject(TaskDriverError::Source(error), ticket)),
            };
            let pending = owner.pending_notice();
            // Only the Core's committed loser selector authorizes taking this
            // original source. Its foreign destructor runs on the lifecycle
            // worker while the same effect ticket still owes actual exit.
            let job = TaskDriverHostJob::DisposeSource {
                key: (self.scope().id(), ticket.descriptor.work),
                runtime: self.control_runtime.clone(),
                ticket: std::mem::ManuallyDrop::new(ticket),
                source: Box::new(std::mem::ManuallyDrop::new(source)),
                pending,
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit(job) {
                self.task_driver.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskDriverRefusal {
                            error: TaskDriverError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Context(ContextEffect::StopTask { task, .. }) = &ticket.effect {
            let task = *task;
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskDriverError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let job = match owner
                .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                .handoff_stop_effect(ticket)
            {
                Ok(job) => TaskDriverHostJob::Stop { job },
                Err((error, ticket)) => {
                    return Err(reject(TaskDriverError::HostCall(error), ticket));
                }
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit(job) {
                self.task_driver.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskDriverRefusal {
                            error: TaskDriverError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Context(ContextEffect::SettlementBarrier { task, work }) =
            &ticket.effect
        {
            let (task, work) = (*task, *work);
            let Some(executor) = ports.lifecycle.as_ref() else {
                return Err(reject(TaskDriverError::MissingLifecyclePort, ticket));
            };
            let Some(owner) = self.task(task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            let Some(roles) = owner.resources_for_retirement().completion.clone() else {
                return Err(reject(TaskDriverError::EffectRequiresOriginalOwner, ticket));
            };
            let port = owner.port();
            if let Err((error, ticket)) = roles.handoff_settlement_effect(
                &self.control_runtime,
                &mut self.dispatch,
                ticket,
                &port,
            ) {
                return Err(reject(TaskDriverError::Settlement(error), ticket));
            }
            let job = TaskDriverHostJob::Settlement {
                key: (self.scope().id(), work),
                roles,
                port: std::mem::ManuallyDrop::new(port),
            };
            let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
            if let Err((_, job)) = executor.try_submit(job) {
                self.task_driver.physical_pending.push_back(job);
                if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                    capacity
                        .register_waiter(self.registration(), before)
                        .map_err(|error| TaskDriverRefusal {
                            error: TaskDriverError::Capacity(error),
                            ticket: None,
                        })?;
                }
            }
            return Ok(());
        }
        if let WorkerAction::Context(ContextEffect::WakeTaskDomains { task }) = &ticket.effect {
            let task = *task;
            if !matches!(self.tasks.get(&task), Some(super::TaskHostOwner::Ready(_))) {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            }
            // Wait effects remain with their original caller until a real
            // waiter owner accepts them; this adapter has accepted none.
            // This notification reaches the original Task metadata owner.
            self.task_driver
                .ready
                .lock()
                .expect("original Task notification")
                .insert(task);
            self.resources
                .notice
                .as_ref()
                .expect("original Context notification")
                .notify();
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if matches!(
            &ticket.effect,
            WorkerAction::Context(ContextEffect::WakeContextDomains)
        ) {
            self.resources
                .notice
                .as_ref()
                .expect("original Context notification")
                .notify();
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        if let WorkerAction::Context(ContextEffect::CancelLoader { task, .. }) = &ticket.effect {
            let Some(owner) = self.task(*task) else {
                return Err(reject(
                    TaskDriverError::Task(ContextHostError::TaskNotOwned),
                    ticket,
                ));
            };
            if let Some(loader) = owner.loader() {
                loader.cancel();
            }
            return self
                .complete_host_effect(ticket)
                .map_err(|refusal| reject(TaskDriverError::Driver(refusal.error), refusal.ticket));
        }
        let kind = match &ticket.effect {
            WorkerAction::Context(effect) => match effect {
                ContextEffect::ApplyTaskClose { .. } => "ApplyTaskClose",
                ContextEffect::PrepareReadPayload { .. } => "PrepareReadPayload",
                ContextEffect::ApplyTaskDomains { .. } => "ApplyTaskDomains",
                ContextEffect::WaitTaskDomain { .. } => "WaitTaskDomain",
                ContextEffect::WakeTaskDomains { .. } => "WakeTaskDomains",
                ContextEffect::ApplyContextDomain { .. } => "ApplyContextDomain",
                ContextEffect::WaitContextDomain { .. } => "WaitContextDomain",
                ContextEffect::DisposeSource { .. } => "DisposeSource",
                _ => "ContextEffect",
            },
            _ => "WorkerAction",
        };
        Err(reject(TaskDriverError::UnsupportedEffect(kind), ticket))
    }
    pub(crate) fn advance_task_driver(
        &mut self,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<TaskDriverProgress, TaskDriverError> {
        let mut progress = TaskDriverProgress::default();
        for (task, job) in self.task_driver.jobs.iter().take(budget) {
            if !job.awaiting_initial_plan() {
                self.task_driver
                    .ready
                    .lock()
                    .expect("original pending Task address")
                    .insert(*task);
            }
        }

        for _ in 0..budget {
            let Some(job) = self.task_driver.physical_pending.pop_front() else {
                break;
            };
            if let Some(executor) = &ports.lifecycle {
                let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                if let Err((_, original)) = executor.try_submit(job) {
                    self.task_driver.physical_pending.push_front(original);
                    if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                        capacity
                            .register_waiter(self.registration(), before)
                            .map_err(TaskDriverError::Capacity)?;
                    }
                    break;
                }
            } else {
                self.task_driver.physical_pending.push_front(job);
                break;
            }
        }

        for _ in 0..budget {
            let Some(task) = self
                .task_driver
                .ready
                .lock()
                .expect("Task ready metadata")
                .pop_first()
            else {
                break;
            };
            progress.visited += 1;
            if let Some(job) = self.task_driver.jobs.remove(&task) {
                if job.awaiting_initial_plan() {
                    self.task_driver.jobs.insert(task, job);
                } else if let Some(executor) = &ports.preparation {
                    let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
                    if let Err((_, original)) = executor.try_submit(job) {
                        self.task_driver.jobs.insert(task, original);
                        if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                            capacity
                                .register_waiter(self.registration(), before)
                                .map_err(TaskDriverError::Capacity)?;
                        }
                    }
                } else {
                    self.task_driver.jobs.insert(task, job);
                }
            }
            self.adopt_installed_task_root(task)?;
            let mut loading_wait = None;
            let mut source_progress = false;
            let ledger = self.ledger.clone();
            let reaped = ledger
                .reap_scope_ready(self.scope().id(), 1)
                .map_err(|error| TaskDriverError::Host(TaskHostError::Runtime(error)))?;
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
                if let Some(returned) = host.take_loading_return().map_err(TaskDriverError::Host)? {
                    if let Err((error, original)) = host.observe_loading_job_return(returned) {
                        // A broken original return binding cannot be unwound through opaque custody.
                        std::mem::forget(original);
                        return Err(TaskDriverError::Host(error));
                    }
                }
                if let Some(receipt) = host
                    .publish_loading_exit()
                    .map_err(TaskDriverError::Loading)?
                {
                    if let Err((error, original)) = host.observe_loading_author_return(receipt) {
                        std::mem::forget(original);
                        return Err(TaskDriverError::Host(error));
                    }
                }
                host.observe_loading_exit_report()
                    .map_err(TaskDriverError::Loading)?;
                if host
                    .advance_production_cut(&ledger)
                    .map_err(TaskDriverError::Host)?
                {
                    source_progress = true;
                    host.pending_notice().pending();
                }

                if host
                    .advance_production_facts()
                    .map_err(TaskDriverError::Host)?
                {
                    source_progress = true;
                    host.pending_notice().pending();
                }
                if host
                    .advance_normal_retirement(&ledger)
                    .map_err(TaskDriverError::Host)?
                {
                    host.pending_notice().pending();
                }
            }
            if let Some(execution) = &ports.execution {
                if let Some(job) = self
                    .task(task)
                    .and_then(|owner| owner.take_preparation_rollback_job(execution.clone()))
                {
                    self.queue_task_cleanup(job, ports)?;
                }
            }
            if source_progress || reaped > 0 {
                self.notify_domain_source_progress(task);
            }
            let rf_progress = match self.task(task).and_then(|host| host.dynamic_filters()) {
                Some(source) => Some(source.advance().map_err(TaskDriverError::DynamicFilter)?),
                None => None,
            };
            if let Some(rf) = rf_progress {
                if rf.progressed {
                    if let Some(host) = self.task(task) {
                        host.pending_notice().pending();
                    }
                }
                if let Some(job) = rf.job {
                    self.task_driver
                        .physical_pending
                        .push_back(TaskDriverHostJob::DynamicFilter(job));
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
                    .map_err(TaskDriverError::Capacity)?;
            }
            if self
                .deliver_task_event(task)
                .map_err(TaskDriverError::Request)?
                .is_some()
            {
                progress.delivered += 1;
                self.task_driver
                    .ready
                    .lock()
                    .expect("Task ready metadata")
                    .insert(task);
                self.notify_deferred_root_source_progress(task);
            }
            self.advance_task_retirement(task, ports)?;
        }
        self.advance_retired_task_disposals(budget);
        progress.runnable = !self
            .task_driver
            .ready
            .lock()
            .expect("Task ready metadata")
            .is_empty();
        Ok(progress)
    }
    pub(crate) fn close_task_driver_admission(
        &mut self,
        context: QueryContextRef,
        _generation: Generation,
        _ports: &TaskDriverPorts,
    ) -> Result<bool, TaskDriverError> {
        if context != self.identity {
            return Err(TaskDriverError::WrongOwner);
        }
        self.task_driver.admission_closed = true;
        self.control_runtime
            .close_actor_admission(self.actor)
            .map_err(TaskDriverError::Driver)?;
        // Existing admitted return/cleanup roles remain usable. New Task/R
        // issuers must inspect this same original gate before any capture.
        Ok(true)
    }
    pub(super) fn task_driver_roots_sealed(&self) -> bool {
        self.task_driver.root_seal_finished
    }
    pub(crate) fn seal_task_roots(
        &mut self,
        context: QueryContextRef,
        _generation: Generation,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
        seal_work: novarocks_control_runtime_core::identity::WorkId,
        ports: &TaskDriverPorts,
    ) -> Result<bool, TaskDriverError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if context != self.identity {
            return Err(TaskDriverError::WrongOwner);
        }
        self.task_driver.roots_sealed = true;
        let reason = *self.task_driver.root_close_reason.get_or_insert(reason);
        if self.task_driver.root_seal_finished {
            return self.seal_retained_roots(seal_work, reason, ports);
        }
        let next = self
            .tasks
            .range((
                self.task_driver
                    .root_seal_cursor
                    .map_or(Unbounded, Excluded),
                Unbounded,
            ))
            .next()
            .map(|(task, _)| *task);
        let Some(task) = next else {
            self.task_driver.root_seal_finished = true;
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
            self.task_driver.root_seal_cursor = Some(task);
            self.ready_notice().notify();
        }
        Ok(false)
    }
}

impl ContextHostRecord {
    fn adopt_installed_task_root(&mut self, task: TaskIdentity) -> Result<(), TaskDriverError> {
        let root = match self.task(task) {
            Some(owner) => owner
                .take_root_context_retention()
                .map_err(TaskDriverError::Host)?,
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
                    .restore_root_context_retention(root)
                {
                    std::mem::forget(original);
                    std::process::abort();
                }
                return Err(TaskDriverError::Task(ContextHostError::RootRead(error)));
            }
        }
        Ok(())
    }
    fn seal_retained_roots(
        &mut self,
        work: novarocks_control_runtime_core::identity::WorkId,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
        ports: &TaskDriverPorts,
    ) -> Result<bool, TaskDriverError> {
        use crate::control_runtime::task::RetainedRootSealPreparation;
        let Some((task, state)) = self
            .resources
            .root_reads
            .prepare_retained_seal(self.task_driver.retained_seal_cursor, reason)
            .map_err(TaskDriverError::RetainedRootSeal)?
        else {
            return Ok(true);
        };
        match state {
            RetainedRootSealPreparation::Ready => {
                self.task_driver.retained_seal_cursor = Some(task);
                self.ready_notice().notify();
            }
            RetainedRootSealPreparation::Pending => {}
            RetainedRootSealPreparation::Job(job) => {
                self.queue_task_cleanup(
                    TaskDriverHostJob::SealRetainedRoot {
                        key: (self.scope().id(), work),
                        job,
                    },
                    ports,
                )?;
            }
        }
        Ok(false)
    }
}
struct TaskDriverPending {
    active: Mutex<bool>,
    task: TaskIdentity,
    ready: Arc<Mutex<BTreeSet<TaskIdentity>>>,
    notice: crate::control_runtime::context_ready::ContextReadyNotice,
}
impl TaskDriverPending {
    fn retire_to_context(&self) {
        let mut active = self.active.lock().expect("original Task pending route");
        *active = false;
        self.ready
            .lock()
            .expect("same original Task pending route")
            .remove(&self.task);
    }
}
impl crate::task_port::TaskPendingNotice for TaskDriverPending {
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
pub(crate) enum TaskDriverHostJob {
    DisposeCreate(super::requests::create_disposal::TaskCreateDisposalJob),
    RequestObservation(crate::control_runtime::request_observation::RequestObservationJob),
    DynamicFilter(crate::control_runtime::dynamic_filters::TaskDynamicFilterHostJob),
    RollbackPreparation {
        preparation: crate::control_runtime::preparation::PreparationCustody,
        owner: Arc<crate::control_runtime::task::ProductionPreparation>,
        host: Arc<dyn crate::TaskExecutionHost>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    DisposeSource {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        runtime: novarocks_control_runtime::driver::ActorRuntime<WorkerActor>,
        ticket: std::mem::ManuallyDrop<EffectTicket<WorkerAction>>,
        source: Box<
            std::mem::ManuallyDrop<
                novarocks_execution_contract::RetainedTaskSource<
                    novarocks_execution::runtime::fragment::ExecutionFailure,
                >,
            >,
        >,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    Domain(super::requests::domain::DomainHostJob),
    DomainProjection(super::requests::domain_driver::DomainProjectionJob),
    SealRetainedRoot {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        job: crate::control_runtime::task::RetainedRootSeal,
    },
    RetireExecution {
        job: crate::control_runtime::task_host_call::TaskHostCallJob,
        call: crate::control_runtime::task_host_call::TaskHostCall,
        owner: Arc<crate::control_runtime::task::ProductionPreparation>,
        host: Arc<dyn crate::TaskExecutionHost>,
        pending: Arc<dyn crate::task_port::TaskPendingNotice>,
    },
    DisposeTask {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        owner: Arc<crate::control_runtime::task::RetiredTaskDisposal>,
        notice: crate::control_runtime::context_ready::ContextReadyNotice,
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
        roles: Arc<crate::task_completion_roles::TaskCompletionRoles>,
        port: std::mem::ManuallyDrop<Arc<crate::task_port::TaskRuntimePort>>,
    },
    DisposeProjection {
        key: (
            novarocks_control_runtime_core::identity::ScopeId,
            novarocks_control_runtime_core::identity::WorkId,
        ),
        runtime: novarocks_control_runtime::driver::ActorRuntime<WorkerActor>,
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
impl TaskDriverHostJob {
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
impl crate::control_runtime::preparation::QueuedPreparation for TaskDriverHostJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        match self {
            Self::RollbackPreparation { preparation, .. } => preparation.key(),
            Self::DisposeCreate(job) => job.queue_key(),
            Self::RequestObservation(job) => job.queue_key(),
            Self::DynamicFilter(job) => job.queue_key(),
            Self::RetireExecution { job, .. } => {
                crate::control_runtime::preparation::QueuedPreparation::queue_key(job)
            }
            Self::DisposeTask { key, .. }
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
}
impl TaskDriverHostJob {
    pub(crate) fn run_on_host(self) {
        match self {
            Self::RollbackPreparation {
                preparation,
                owner,
                host,
                pending,
            } => owner.rollback_on_host(&preparation, host.as_ref(), pending.as_ref()),
            Self::DisposeCreate(job) => job.run(),
            Self::RequestObservation(job) => job.run(),
            Self::DynamicFilter(job) => job.run(),
            Self::DisposeSource {
                runtime,
                ticket,
                mut source,
                pending,
                ..
            } => {
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
                if let Err(refusal) =
                    runtime.effect_completed(std::mem::ManuallyDrop::into_inner(ticket))
                {
                    tracing::error!(pid = std::process::id(), error = ?refusal.error,
                        "Original source disposal effect completion failed");
                    std::mem::forget(refusal);
                    std::process::abort();
                }
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
                pending,
            } => {
                job.run();
                if call.actual_call_exited() {
                    owner.retire_execution_on_host(host.as_ref());
                }
                drop((host, call, owner));
                pending.pending();
            }
            Self::DisposeTask { owner, notice, .. } => {
                if let Err(error) = owner.run_on_host() {
                    eprintln!("Original Task payment retirement failed: {error:?}");
                }
                drop(owner);
                notice.notify();
            }
            Self::ReplyNotification(job) => job.run(),
            Self::Domain(job) => job.run_on_host(),
            Self::DomainProjection(job) => job.run_on_host(),
            Self::Stop { job } => job.run(),
            Self::Settlement { roles, port, .. } => {
                let port = std::mem::ManuallyDrop::into_inner(port);
                let _ = roles.dispatch_settlement(&port);
                drop(port);
                let _ = roles.finish_settlement_effect();
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
    Context,
}
/// This fixed I/O wake follows the actual Root owner. Transition and pending
/// Task-address removal share one lock with delivery of an already-started
/// wake, so transfer cannot leave a stale Task address after retirement.
pub(crate) struct TaskRootWake {
    task: TaskIdentity,
    ready: Arc<Mutex<BTreeSet<TaskIdentity>>>,
    notice: crate::control_runtime::context_ready::ContextReadyNotice,
    route: Mutex<RootWakeRoute>,
}
impl TaskRootWake {
    pub(crate) fn retain_by_context(&self) {
        let mut route = self.route.lock().expect("original Root wake routing");
        *route = RootWakeRoute::Context;
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
            self.ready
                .lock()
                .expect("original Root Task readiness")
                .insert(self.task);
        }
        self.notice.notify();
    }
}

impl ContextHostRecord {
    fn queue_task_cleanup(
        &mut self,
        job: TaskDriverHostJob,
        ports: &TaskDriverPorts,
    ) -> Result<(), TaskDriverError> {
        let executor = ports
            .lifecycle
            .as_ref()
            .ok_or(TaskDriverError::MissingLifecyclePort)?;
        let before = ports.capacity.as_ref().map(|capacity| capacity.revision());
        if let Err((_, job)) = executor.try_submit(job) {
            self.task_driver.physical_pending.push_back(job);
            if let (Some(capacity), Some(before)) = (&ports.capacity, before) {
                capacity
                    .register_waiter(self.registration(), before)
                    .map_err(TaskDriverError::Capacity)?;
            }
        }
        Ok(())
    }
    fn advance_task_retirement(
        &mut self,
        task: TaskIdentity,
        ports: &TaskDriverPorts,
    ) -> Result<(), TaskDriverError> {
        let Some(retirement) = self.task_driver.retirements.get(&task) else {
            return Ok(());
        };
        if !retirement.execution_job_issued {
            let host = ports
                .execution
                .as_ref()
                .ok_or(TaskDriverError::MissingExecutionPort)?
                .clone();
            let ticket = retirement
                .ticket
                .as_ref()
                .expect("original uncommitted Task retirement");
            let owner = match self.tasks.get(&task) {
                Some(
                    super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
                ) => owner,
                _ => return Err(TaskDriverError::Task(ContextHostError::TaskNotOwned)),
            };
            let job = owner
                .take_production_retire_job(ticket, host, &self.ledger)
                .map_err(TaskDriverError::Host)?;
            if let Some(job) = job {
                self.task_driver
                    .retirements
                    .get_mut(&task)
                    .expect("same retirement")
                    .execution_job_issued = true;
                self.queue_task_cleanup(job, ports)?;
            }
            return Ok(());
        }
        let retired = self
            .task(task)
            .is_some_and(|owner| owner.production_retired());
        if !retired {
            return Ok(());
        }
        if !self.task_driver.retirements[&task].projection_committed {
            let retirement = self
                .task_driver
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
                        WorkerAction::Context(ContextEffect::RetireTask {
                            convergence, ..
                        }) => candidate.with_convergence(*convergence),
                        WorkerAction::Context(ContextEffect::FinalizeTaskExecution { .. }) => {
                            Ok(candidate)
                        }
                        _ => unreachable!("original execution finalization or Task retirement"),
                    });
            let candidate = match candidate {
                Ok(candidate) => candidate,
                Err(_) => {
                    self.task_driver.retirements.get_mut(&task).unwrap().ticket = Some(ticket);
                    return Err(TaskDriverError::Publication);
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
                    self.task_driver.retirements.get_mut(&task).unwrap().ticket = Some(ticket);
                    return Err(TaskDriverError::Driver(error));
                }
            };
            self.dispatch
                .publication_committed(&self.control_runtime, &ticket)
                .map_err(TaskDriverError::Dispatch)?;
            let pending = self
                .task(task)
                .expect("same original Task")
                .pending_notice();
            self.task_driver
                .retirements
                .get_mut(&task)
                .unwrap()
                .projection_committed = true;
            self.queue_task_cleanup(
                TaskDriverHostJob::DisposeProjection {
                    key: (self.scope().id(), ticket.descriptor.work),
                    runtime: self.control_runtime.clone(),
                    ticket,
                    old: std::mem::ManuallyDrop::new(old),
                    pending,
                },
                ports,
            )?;
            if self.task_driver.retirements[&task].finalize_execution_only {
                self.task_driver.retirements.remove(&task);
            }
            return Ok(());
        }
        if !self
            .task(task)
            .is_some_and(|owner| owner.normal_retirement_ready())
        {
            return Ok(());
        }
        // The same original pending object routes late physical tails only
        // to the Context after its real Task inventory closes. This is a
        // positive ownership transition, never a missing-Task fallback.
        if let Some(notice) = self.task_driver.notices.get(&task) {
            notice.retire_to_context();
        }
        let owner = self.take_task(task).map_err(TaskDriverError::Task)?;
        self.task_driver.notices.remove(&task);
        let resources = match owner {
            super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner) => {
                owner.into_resources()
            }
            super::TaskHostOwner::Unpublished(_) => unreachable!("original sealed Task owner"),
        };
        let state = self.task_driver.retirements[&task]
            .retained_state
            .expect("original Core Task retention state");
        let disposal = Arc::new(
            crate::control_runtime::task::RetiredTaskDisposal::from_original(
                resources,
                ports.protocol_events.clone(),
                task,
                state,
            ),
        );
        let key = disposal.key();
        self.task_driver
            .ready
            .lock()
            .expect("removed original Task ready address")
            .remove(&task);
        self.task_driver.retirements.remove(&task);
        assert!(
            self.task_driver
                .retiring_tasks
                .insert(task, disposal.clone())
                .is_none()
        );
        self.notify_deferred_root_source_progress(task);
        self.queue_task_cleanup(
            TaskDriverHostJob::DisposeTask {
                key,
                owner: disposal,
                notice: self
                    .resources
                    .notice
                    .as_ref()
                    .expect("original Context notice")
                    .clone(),
            },
            ports,
        )
    }
    fn advance_retired_task_disposals(&mut self, budget: usize) {
        use std::ops::Bound::{Excluded, Unbounded};
        for _ in 0..budget {
            let next = self
                .task_driver
                .retiring_tasks
                .range((
                    self.task_driver.disposal_cursor.map_or(Unbounded, Excluded),
                    Unbounded,
                ))
                .next()
                .or_else(|| self.task_driver.retiring_tasks.first_key_value())
                .map(|(task, _)| *task);
            let Some(task) = next else {
                break;
            };
            self.task_driver.disposal_cursor = Some(task);
            if self.task_driver.retiring_tasks[&task].complete() {
                self.task_driver.retiring_tasks.remove(&task);
            }
        }
    }
}

#[cfg(test)]
impl ContextHostRecord {
    pub(crate) fn print_neutral_task_driver_inventory(&self, task: TaskIdentity) {
        let flags = self.task_driver.retirements.get(&task).map(|retirement| {
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
            .task_driver
            .ready
            .lock()
            .expect("neutral Task readiness diagnostic")
            .contains(&task);
        let notice_active = self.task_driver.notices.get(&task).map(|notice| {
            *notice
                .active
                .lock()
                .expect("original pending route diagnostic")
        });
        eprintln!(
            "Context Task retirement task={task:?} retirement_flags={flags:?} ready={} pending_business={} pending_lifecycle={} disposal={:?} source_index={} notice_active={:?}",
            ready,
            self.task_driver.jobs.contains_key(&task),
            self.task_driver.physical_pending.len(),
            self.task_driver
                .retiring_tasks
                .get(&task)
                .map(|owner| owner.complete()),
            self.task_driver
                .sources
                .values()
                .any(|candidate| *candidate == task),
            notice_active
        );
    }
}
