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

//! The actual Context metadata turn. Effects move only into their original
//! owners; physical getters, callbacks and destructors run in the host pools.

use super::task_driver::{TaskDriverError, TaskDriverPorts, TaskDriverProgress};
use super::{ContextHostProgress, ContextHostProgressError, ContextHostRecord};
use crate::control_runtime::{
    actors::{ContextActor, WorkerAction},
    context_executors::ContextExecutorHandles,
    context_host_jobs::ContextHostJobError,
    effect_dispatch::DispatchError,
    establish_route::EstablishRouteError,
    request_ready::ControlRequestReady,
};
use novarocks_control_runtime::driver::{DriverError, EffectTicket};
use novarocks_control_runtime_core::effect::EffectState;
use novarocks_worker_protocol::context::ContextEffect;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Debug)]
pub(crate) enum ContextDriverError {
    WrongOwner,
    Observation(super::observation::ContextObservationError),
    Driver(DriverError),
    Dispatch(DispatchError),
    Host(ContextHostJobError),
    HostProgress(ContextHostProgressError),
    Route(EstablishRouteError),
    Request(super::requests::ContextRequestError),
    Failure(super::failure::FailurePublicationError),
    Metadata(super::ContextHostError),
    Task(TaskDriverError),
    Capacity(crate::control_runtime::task_capacity::TaskCapacityError),
}
#[derive(Debug, Default)]
pub(crate) struct ContextDriverProgress {
    pub indexed: usize,
    pub effects: usize,
    pub requests: usize,
    pub routes: usize,
    pub timer_inputs_retired: usize,
    pub host: Option<ContextHostProgress>,
    pub tasks: Option<TaskDriverProgress>,
    pub failure_publications_drained: bool,
    pub metadata_teardown_ready: bool,
    pub runnable: bool,
}

impl ContextHostRecord {
    fn is_owner_failure_fence(error: &ContextDriverError) -> bool {
        let fence = |error: &DriverError| {
            matches!(
                error,
                DriverError::ActorFailed
                    | DriverError::FailurePublicationRequired
                    | DriverError::Effect(
                        novarocks_control_runtime_core::effect::EffectError::StaleGeneration
                    )
            )
        };
        match error {
            ContextDriverError::Dispatch(DispatchError::OwnerFailed) => true,
            ContextDriverError::Driver(error)
            | ContextDriverError::Task(TaskDriverError::Driver(error))
            | ContextDriverError::Task(TaskDriverError::Request(super::requests::ContextRequestError::Driver(error)))
            | ContextDriverError::Task(TaskDriverError::DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError::Driver(error)))
            | ContextDriverError::Task(TaskDriverError::Request(super::requests::ContextRequestError::DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError::Driver(error))))
            | ContextDriverError::Task(TaskDriverError::Host(crate::control_runtime::task::TaskHostError::Driver(error)))
            | ContextDriverError::Host(ContextHostJobError::Driver(error))
            | ContextDriverError::HostProgress(ContextHostProgressError::Host(ContextHostJobError::Driver(error)))
            | ContextDriverError::Request(super::requests::ContextRequestError::Driver(error))
            | ContextDriverError::Request(super::requests::ContextRequestError::Causal(crate::control_runtime::causal::CausalError::Driver(error)))
            | ContextDriverError::Task(TaskDriverError::Request(super::requests::ContextRequestError::Causal(crate::control_runtime::causal::CausalError::Driver(error))))
            | ContextDriverError::Task(TaskDriverError::Host(crate::control_runtime::task::TaskHostError::Causal(crate::control_runtime::causal::CausalError::Driver(error)))) => fence(error),
            ContextDriverError::Task(TaskDriverError::Host(crate::control_runtime::task::TaskHostError::OwnerFailed)) => true,
            ContextDriverError::Task(TaskDriverError::Host(crate::control_runtime::task::TaskHostError::Statistics(
                crate::control_runtime::operator_statistics::StatisticsError::FailurePublicationRequired,
            ))) => true,
            ContextDriverError::Task(TaskDriverError::DynamicFilter(
                crate::control_runtime::dynamic_filters::DynamicFilterError::FailurePublicationRequired,
            ))
            | ContextDriverError::Task(TaskDriverError::Request(super::requests::ContextRequestError::DynamicFilter(
                crate::control_runtime::dynamic_filters::DynamicFilterError::FailurePublicationRequired,
            ))) => true,
            _ => false,
        }
    }

    fn retain_driver_refusal(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        error: ContextDriverError,
    ) -> ContextDriverError {
        assert!(self.resources.pending_host_jobs.driver_effect.is_none());
        self.resources.pending_host_jobs.driver_effect = Some(ticket);
        error
    }

    fn handoff_driver_effect(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        tasks: &TaskDriverPorts,
    ) -> Result<(), ContextDriverError> {
        let fact = match self.observation_effect_fact(&ticket.effect) {
            Ok(fact) => fact,
            Err(error) => {
                return Err(
                    self.retain_driver_refusal(ticket, ContextDriverError::Observation(error))
                );
            }
        };
        if fact.is_some() && self.observation_publication_pending() {
            assert!(self.resources.pending_host_jobs.driver_effect.is_none());
            self.resources.pending_host_jobs.driver_effect = Some(ticket);
            return Ok(());
        }
        let result = match &ticket.effect {
            WorkerAction::Context(ContextEffect::Subscribe { .. }) => self
                .handoff_observation_subscribe(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_driver_refusal(ticket, ContextDriverError::Request(error))
                }),
            WorkerAction::Context(ContextEffect::PublishContextConvergence { .. }) => {
                self.complete_host_effect(ticket).map_err(|refusal| {
                    self.retain_driver_refusal(
                        refusal.ticket,
                        ContextDriverError::Driver(refusal.error),
                    )
                })
            }
            WorkerAction::Context(ContextEffect::WakeInternal) => {
                self.handoff_internal_wake(ticket).map_err(|refusal| {
                    self.retain_driver_refusal(
                        refusal.ticket,
                        ContextDriverError::Driver(refusal.error),
                    )
                })
            }
            WorkerAction::Context(ContextEffect::EstablishProbed { .. }) => self
                .handoff_establish_probe(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_driver_refusal(ticket, ContextDriverError::Route(error))
                }),
            WorkerAction::Context(ContextEffect::EstablishmentDecided { .. }) => self
                .handoff_establishment_decision(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_driver_refusal(ticket, ContextDriverError::Host(error))
                }),
            WorkerAction::Context(ContextEffect::ReleaseHost { .. }) => {
                let sealed = self.task_driver_roots_sealed()
                    && self.resources.pending_host_jobs.driver_seal.is_none();
                if !sealed {
                    assert!(
                        self.resources
                            .pending_host_jobs
                            .driver_release
                            .replace(ticket)
                            .is_none()
                    );
                    return Ok(());
                }
                if !self.resources.pending_host_jobs.root_release_finished {
                    let next = self
                        .resources
                        .root_reads
                        .next_release_root(self.resources.pending_host_jobs.root_release_after);
                    if let Some((task, idle)) = next {
                        if idle {
                            self.resources.pending_host_jobs.root_release_after = Some(task);
                            self.ready_notice().notify();
                        }
                        assert!(
                            self.resources
                                .pending_host_jobs
                                .driver_release
                                .replace(ticket)
                                .is_none()
                        );
                        return Ok(());
                    }
                    self.resources.pending_host_jobs.root_release_finished = true;
                }
                // R samples the original normal-close retention bank. Core's
                // terminal/RetireTask intent does not prove that the physical
                // Finalize/Retire call has installed its normal-close record.
                // Roots are already sealed and fully idle above; now wait for
                // the same Task cleanup and its real Context notices, not R.
                if !self.tasks.is_empty() || !self.task_driver.is_idle() {
                    assert!(
                        self.resources
                            .pending_host_jobs
                            .driver_release
                            .replace(ticket)
                            .is_none()
                    );
                    return Ok(());
                }
                self.handoff_host_release(ticket)
                    .map_err(|(error, ticket)| {
                        self.retain_driver_refusal(ticket, ContextDriverError::Host(error))
                    })
            }
            WorkerAction::Context(ContextEffect::RetainContext { .. }) => {
                self.handoff_retention(ticket).map_err(|(error, ticket)| {
                    self.retain_driver_refusal(ticket, ContextDriverError::Host(error))
                })
            }
            WorkerAction::Context(ContextEffect::CloseExternalAdmission {
                context,
                generation,
                reason,
            }) if *context == self.identity => {
                if let Err(error) = self
                    .control_runtime
                    .validate_effect_ticket(&ticket)
                    .and_then(|()| self.control_runtime.close_actor_admission(self.actor))
                {
                    return Err(
                        self.retain_driver_refusal(ticket, ContextDriverError::Driver(error))
                    );
                }
                match self.close_task_driver_admission(*context, *generation, tasks) {
                    Ok(true) => {
                        let reason = *reason;
                        self.handoff_context_admission_close(ticket, reason, tasks)
                            .map_err(|refusal| {
                                let error = ContextDriverError::Task(refusal.error);
                                match refusal.ticket {
                                    Some(ticket) => self.retain_driver_refusal(ticket, error),
                                    None => error,
                                }
                            })
                    }
                    result => {
                        assert!(self.resources.pending_host_jobs.driver_close.is_none());
                        self.resources.pending_host_jobs.driver_close = Some(ticket);
                        result.map(|_| ()).map_err(ContextDriverError::Task)
                    }
                }
            }
            WorkerAction::Context(ContextEffect::SealRoot {
                context,
                generation,
                reason,
            }) if *context == self.identity => {
                if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
                    return Err(
                        self.retain_driver_refusal(ticket, ContextDriverError::Driver(error))
                    );
                }
                // Every new root installer crosses this same owner gate. A
                // late M return cannot reopen it after the Core close winner.
                self.root_issuance_sealed = true;
                match self.seal_task_roots(
                    *context,
                    *generation,
                    *reason,
                    ticket.descriptor.work,
                    tasks,
                ) {
                    Ok(true) => self.complete_host_effect(ticket).map_err(|refusal| {
                        self.retain_driver_refusal(
                            refusal.ticket,
                            ContextDriverError::Driver(refusal.error),
                        )
                    }),
                    result => {
                        assert!(self.resources.pending_host_jobs.driver_seal.is_none());
                        self.resources.pending_host_jobs.driver_seal = Some(ticket);
                        result.map(|_| ()).map_err(ContextDriverError::Task)
                    }
                }
            }
            WorkerAction::Reply(_) => self.publish_request_reply(ticket).map_err(|refusal| {
                use super::requests::ContextRequestPublishRefusal;
                match refusal {
                    ContextRequestPublishRefusal::Rejected { error, ticket } => {
                        self.retain_driver_refusal(ticket, ContextDriverError::Request(error))
                    }
                    ContextRequestPublishRefusal::Publication {
                        error,
                        ticket,
                        body,
                    } => {
                        // Only a neutral Core body alias is released here.
                        drop(body);
                        self.retain_driver_refusal(ticket, ContextDriverError::Driver(error))
                    }
                    ContextRequestPublishRefusal::Completion(refusal) => self
                        .retain_driver_refusal(
                            refusal.ticket,
                            ContextDriverError::Driver(refusal.error),
                        ),
                }
            }),
            WorkerAction::OwnerFailed { .. } => {
                self.begin_failure_publications(ticket).map_err(|refusal| {
                    self.retain_driver_refusal(
                        refusal.ticket,
                        ContextDriverError::Failure(refusal.error),
                    )
                })
            }
            // The actual Task owner accepts the same original ticket. Its
            // refusal either returns that original or states that the real
            // admitted job has already taken custody; neither is an ACK.
            _ => self
                .handoff_task_driver_effect(ticket, tasks)
                .map_err(|refusal| {
                    let error = ContextDriverError::Task(refusal.error);
                    match refusal.ticket {
                        Some(original) => self.retain_driver_refusal(original, error),
                        None => error,
                    }
                }),
        };
        result?;
        if let Some((fact, bytes)) = fact {
            self.commit_observation_fact(fact, bytes)
                .map_err(ContextDriverError::Observation)?;
        }
        Ok(())
    }

    /// Failed batches bypass normal publication lanes. Discover only the
    /// runtime's actual original supervisor action, with a bounded cursor.
    fn find_driver_failure(
        &mut self,
        tasks: &TaskDriverPorts,
        budget: usize,
    ) -> Result<bool, ContextDriverError> {
        for _ in 0..budget {
            let after = self.resources.pending_host_jobs.failure_after;
            let next = self
                .control_runtime
                .committed_batches(self.actor, after, 1)
                .map_err(ContextDriverError::Driver)?
                .first()
                .copied();
            let Some((ordinal, input)) = next else {
                return Ok(false);
            };
            self.resources.pending_host_jobs.failure_after = Some(ordinal);
            let inventory = self
                .control_runtime
                .effect_inventory(self.actor, input, None, ContextActor::MAX_STEP_ACTIONS)
                .map_err(ContextDriverError::Driver)?;
            for (descriptor, state) in inventory {
                if state != EffectState::Pending {
                    continue;
                }
                let metadata = self
                    .control_runtime
                    .pending_effect_metadata(self.actor, input, descriptor.id)
                    .map_err(ContextDriverError::Driver)?;
                if matches!(metadata, Some(WorkerAction::OwnerFailed { .. })) {
                    let original = self
                        .control_runtime
                        .take_effect(self.actor, input, descriptor.id)
                        .map_err(ContextDriverError::Driver)?;
                    self.handoff_driver_effect(original, tasks)?;
                    return Ok(true);
                }
            }
        }
        // More committed metadata may precede the supervisor batch. This is a
        // finite scan, not a request to poll an I/O producer.
        Ok(true)
    }

    /// `budget` bounds each independent class. A complete committed batch costs
    /// one index record plus up to sixteen actions; budget one must still be
    /// able to index that batch. Zero budget never claims or changes an owner.
    pub(crate) fn advance_driver(
        &mut self,
        handles: &ContextExecutorHandles,
        requests: &ControlRequestReady,
        tasks: &TaskDriverPorts,
        budget: usize,
    ) -> Result<ContextDriverProgress, ContextDriverError> {
        match self.advance_driver_inner(handles, requests, tasks, budget) {
            Err(error)
                if Self::is_owner_failure_fence(&error)
                    && self
                        .control_runtime
                        .snapshot(self.actor)
                        .is_ok_and(|snapshot| snapshot.failed) =>
            {
                // Core and host turns have independent poller claims. A real
                // failure after the initial snapshot withdraws only normal
                // publication authority; all original pending jobs stay here.
                self.notify_host_progress();
                Ok(ContextDriverProgress {
                    runnable: true,
                    ..Default::default()
                })
            }
            result => result,
        }
    }

    fn advance_driver_inner(
        &mut self,
        handles: &ContextExecutorHandles,
        requests: &ControlRequestReady,
        tasks: &TaskDriverPorts,
        budget: usize,
    ) -> Result<ContextDriverProgress, ContextDriverError> {
        let mut progress = ContextDriverProgress::default();
        if budget == 0 {
            return Ok(progress);
        }
        if !requests.same_runtime(&self.control_runtime) {
            return Err(ContextDriverError::WrongOwner);
        }
        self.configure_observation(tasks.observation)
            .map_err(ContextDriverError::Observation)?;
        // Registration precedes the original bootstrap's M/R/H binding. A
        // different poller can claim this address in that interval; it must
        // not take an effect whose actual receiver is still being installed.
        // Staging the original M supplies the subsequent real owner notice.
        let Some(jobs) = self.resources.host_jobs.as_ref() else {
            return Ok(progress);
        };
        if !handles.accepts_owner(jobs) {
            return Err(ContextDriverError::WrongOwner);
        }
        jobs.bind_execution_host(tasks.execution.as_ref())
            .map_err(ContextDriverError::Host)?;
        let failed = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextDriverError::Driver)?
            .failed;
        if failed {
            if self.failure_tail_started() {
                progress.runnable |= self.advance_failed_cleanup_tail(tasks, budget)?;
            }
            if self.resources.failure.is_none() && !self.failed_owner_completed() {
                progress.runnable = self.find_driver_failure(tasks, budget)?;
            }
            if self.resources.failure.is_some() && !self.failure_tail_started() {
                progress.failure_publications_drained = self
                    .advance_failure_publications(budget)
                    .map_err(ContextDriverError::Failure)?;
                if progress.failure_publications_drained {
                    self.discard_failed_internal_hint()
                        .map_err(ContextDriverError::Failure)?;
                    let inputs = self
                        .advance_failed_runtime_inputs(budget)
                        .map_err(ContextDriverError::Failure)?;
                    progress.runnable |= !inputs.empty;
                    self.advance_failed_root_release(tasks, budget)?;
                    progress.runnable |= self.advance_failed_task_cleanup(tasks, budget)?;
                } else {
                    progress.runnable = true;
                }
            }
        } else {
            if let Some(original) = self.resources.pending_host_jobs.driver_close.take() {
                self.handoff_driver_effect(original, tasks)?;
            }
            if let Some(original) = self.resources.pending_host_jobs.driver_seal.take() {
                self.handoff_driver_effect(original, tasks)?;
            }
            if let Some(original) = self.resources.pending_host_jobs.driver_release.take() {
                self.handoff_driver_effect(original, tasks)?;
            }
            let allowance = budget.saturating_mul(ContextActor::MAX_STEP_ACTIONS + 1);
            let indexed = self
                .dispatch
                .index_committed(&self.control_runtime, allowance)
                .map_err(ContextDriverError::Dispatch)?;
            self.domain_inputs_committed(&indexed.inputs);
            self.task_driver_inputs_committed(&indexed.inputs)
                .map_err(ContextDriverError::Task)?;
            progress.indexed = indexed.units;
            let revision = self.ready_notice().version();
            if indexed.units > 0
                || self.resources.pending_host_jobs.effect_scan_version != Some(revision)
            {
                self.resources.pending_host_jobs.effect_scan_remaining =
                    self.dispatch.ready_positions();
                self.resources.pending_host_jobs.effect_scan_version = Some(revision);
            }
            for _ in 0..budget {
                let original = match self.resources.pending_host_jobs.driver_effect.take() {
                    Some(original) => Some(original),
                    None => {
                        let allowance = self
                            .resources
                            .pending_host_jobs
                            .effect_scan_remaining
                            .min(self.dispatch.ready_positions())
                            .min(ContextActor::MAX_STEP_ACTIONS);
                        if allowance == 0 {
                            self.resources.pending_host_jobs.effect_scan_remaining = 0;
                            break;
                        }
                        let original = self
                            .dispatch
                            .take_ready(&self.control_runtime, allowance)
                            .map_err(ContextDriverError::Dispatch)?;
                        if original.is_none() {
                            self.resources.pending_host_jobs.effect_scan_remaining -= allowance;
                        }
                        original
                    }
                };
                let Some(original) = original else {
                    break;
                };
                self.handoff_driver_effect(original, tasks)?;
                self.resources.pending_host_jobs.effect_scan_remaining =
                    self.dispatch.ready_positions();
                progress.effects += 1;
            }
            progress.runnable |= self.resources.pending_host_jobs.effect_scan_remaining > 0;
            // A full class allowance merits one more bounded turn; absence of
            // a ready effect parks on its original publication/return notice.
            progress.runnable |= progress.effects == budget;
            progress.runnable |= self
                .dispatch
                .has_unindexed(&self.control_runtime)
                .map_err(ContextDriverError::Dispatch)?;
        }
        progress.runnable |= self
            .advance_observation_publication(budget)
            .map_err(ContextDriverError::Observation)?;
        if let Some(capacity) = &tasks.capacity {
            progress.runnable |= requests
                .retry_capacity(self.registration(), capacity.revision(), budget)
                .map_err(|error| {
                    ContextDriverError::Request(super::requests::ContextRequestError::Ready(error))
                })?;
        }
        progress.runnable |= self.advance_domain_wakes(budget);
        for _ in 0..budget {
            let Some(work) = requests.next_context_ready(self.registration()) else {
                break;
            };
            self.advance_domain_request(work, tasks)
                .map_err(ContextDriverError::Request)?;
            self.advance_request_notification(work, tasks)
                .map_err(ContextDriverError::Request)?;
            self.advance_request(work)
                .map_err(ContextDriverError::Request)?;
            progress.requests += 1;
        }
        progress.runnable |= progress.requests == budget;
        // A real notice starts a bounded sweep through the actual rows that
        // existed at its start. New notices coalesce the next sweep instead of
        // resetting this cursor; fresh lower keys cannot starve an older tail.
        // The frozen upper key also prevents fresh higher keys extending it.
        let revision = self.ready_notice().version();
        let pending = &mut self.resources.pending_host_jobs;
        if pending.route_sweep_version.is_none()
            || (pending.route_sweep_complete && pending.route_sweep_version != Some(revision))
        {
            pending.route_sweep_version = Some(revision);
            pending.route_after = None;
            pending.route_until = self
                .resources
                .routes
                .last_key_value()
                .map(|(&input, _)| input);
            pending.route_sweep_complete = pending.route_until.is_none();
        }
        if !self.resources.pending_host_jobs.route_sweep_complete {
            for _ in 0..budget {
                let after = self.resources.pending_host_jobs.route_after;
                let until = self
                    .resources
                    .pending_host_jobs
                    .route_until
                    .expect("actual sweep upper key");
                let next = self
                    .resources
                    .routes
                    .range((after.map_or(Unbounded, Excluded), Included(until)))
                    .next()
                    .map(|(&input, _)| input);
                let Some(input) = next else {
                    break;
                };
                self.resources.pending_host_jobs.route_after = Some(input);
                if self.resources.routes[&input].context_cleanup_ready()
                    && self
                        .advance_establish_route(input)
                        .map_err(ContextDriverError::Route)?
                {
                    progress.routes += 1;
                }
            }
            let pending = &mut self.resources.pending_host_jobs;
            let until = pending.route_until.expect("actual sweep upper key");
            pending.route_sweep_complete = self
                .resources
                .routes
                .range((
                    pending.route_after.map_or(Unbounded, Excluded),
                    Included(until),
                ))
                .next()
                .is_none();
            progress.runnable |= !pending.route_sweep_complete;
        }
        // Common already owns exactly one builtin Tick position. Read that
        // original address directly; never scan or retire another R's receipt.
        if let Some(input) = self
            .control_runtime
            .pending_timer_input(self.actor)
            .map_err(ContextDriverError::Driver)?
        {
            match self.control_runtime.retire_input(self.actor, input) {
                Ok(()) => progress.timer_inputs_retired += 1,
                Err(
                    DriverError::EffectsRemain
                    | DriverError::ReservationsRemain
                    | DriverError::InputStillQueued,
                ) => {}
                Err(error) => return Err(ContextDriverError::Driver(error)),
            }
        }
        if !failed {
            let task_progress = self
                .advance_task_driver(tasks, budget)
                .map_err(ContextDriverError::Task)?;
            progress.runnable |= task_progress.runnable;
            progress.tasks = Some(task_progress);
        }
        if failed {
            if progress.failure_publications_drained {
                progress.host = Some(self.advance_failed_host_io(handles, tasks, budget)?);
            }
        } else {
            progress.host = Some(
                self.advance_host_io(handles)
                    .map_err(ContextDriverError::HostProgress)?,
            );
        }
        progress.runnable |= self
            .advance_root_reads(tasks, budget)
            .map_err(ContextDriverError::Request)?;
        if self.advance_observation_teardown(budget) {
            if failed && !self.failed_owner_completed() && !self.failure_tail_started() {
                progress.runnable |= self.advance_failed_cleanup_tail(tasks, budget)?;
            }
            progress.metadata_teardown_ready = self
                .advance_metadata_teardown()
                .map_err(ContextDriverError::Metadata)?;
        } else if self.resources.control_admission_closed && self.resources.requests.is_empty() {
            progress.runnable = true;
        }
        if progress.metadata_teardown_ready
            && let Some(capacity) = &tasks.capacity
        {
            capacity
                .unregister_waiter(self.registration())
                .map_err(ContextDriverError::Capacity)?;
        }
        // A real event during this sweep is retained even if its old cursor
        // had already passed the affected row. No waiting state creates it.
        progress.runnable |= self.resources.pending_host_jobs.route_sweep_version
            != Some(self.ready_notice().version());
        Ok(progress)
    }
}
