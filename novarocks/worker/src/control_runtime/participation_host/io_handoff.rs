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

//! The actual Participation metadata turn. Effects move only into their original
//! owners; physical getters, callbacks and destructors run in the host pools.

use super::observation::ObservationTeardownProgress;
use super::task_io::{TaskIoError, TaskIoPorts, TaskIoProgress};
use super::{ParticipationHostProgress, ParticipationHostProgressError, ParticipationHostRecord};
use crate::control_runtime::{
    actors::{ParticipationActor, WorkerAction},
    effect_dispatch::DispatchError,
    establish_route::EstablishRouteError,
    participation_executors::ParticipationExecutorHandles,
    participation_host_jobs::ParticipationHostJobError,
    request_ready::ControlRequestReady,
};
use novarocks_control_runtime::actor_turn::{ActorTurnError, EffectTicket};
use novarocks_control_runtime_core::effect::EffectState;
use novarocks_worker_protocol::participation::ParticipationEffect;
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Debug)]
pub(crate) enum ParticipationIoHandoffError {
    WrongOwner,
    Observation(super::observation::ParticipationObservationError),
    ActorTurn(ActorTurnError),
    Dispatch(DispatchError),
    Host(ParticipationHostJobError),
    HostProgress(ParticipationHostProgressError),
    Route(EstablishRouteError),
    Request(super::requests::ParticipationRequestError),
    Failure(super::failure::FailurePublicationError),
    Metadata(super::ParticipationHostError),
    Task(TaskIoError),
    Capacity(crate::control_runtime::task_capacity::TaskCapacityError),
}
impl std::fmt::Display for ParticipationIoHandoffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => {
                f.write_str("Participation I/O handoff does not belong to the original owner")
            }
            Self::Observation(error) => {
                write!(
                    f,
                    "Participation I/O handoff observation operation failed: {error:?}"
                )
            }
            Self::ActorTurn(error) => write!(
                f,
                "Participation I/O handoff runtime operation failed: {error}"
            ),
            Self::Dispatch(error) => write!(
                f,
                "Participation I/O handoff effect dispatch failed: {error}"
            ),
            Self::Host(error) => write!(f, "Participation I/O handoff host job failed: {error}"),
            Self::HostProgress(error) => {
                write!(
                    f,
                    "Participation I/O handoff host progress failed: {error:?}"
                )
            }
            Self::Route(error) => write!(
                f,
                "Participation I/O handoff establishment route failed: {error:?}"
            ),
            Self::Request(error) => write!(
                f,
                "Participation I/O handoff original request failed: {error}"
            ),
            Self::Failure(error) => {
                write!(
                    f,
                    "Participation I/O handoff failure publication failed: {error:?}"
                )
            }
            Self::Metadata(error) => write!(
                f,
                "Participation I/O handoff original metadata operation failed: {error}"
            ),
            Self::Task(error) => {
                write!(
                    f,
                    "Participation I/O handoff original Task operation failed: {error}"
                )
            }
            Self::Capacity(error) => {
                write!(
                    f,
                    "Participation I/O handoff capacity notification failed: {error:?}"
                )
            }
        }
    }
}
impl std::error::Error for ParticipationIoHandoffError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ActorTurn(error) => Some(error),
            Self::Dispatch(error) => Some(error),
            Self::Host(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::WrongOwner
            | Self::Observation(_)
            | Self::HostProgress(_)
            | Self::Route(_)
            | Self::Failure(_)
            | Self::Capacity(_) => None,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ParticipationIoHandoffProgress {
    pub indexed: usize,
    pub effects: usize,
    pub requests: usize,
    pub routes: usize,
    pub timer_inputs_retired: usize,
    pub host: Option<ParticipationHostProgress>,
    pub tasks: Option<TaskIoProgress>,
    pub failure_publications_drained: bool,
    pub metadata_teardown_ready: bool,
    pub runnable: bool,
}

impl ParticipationHostRecord {
    fn is_owner_failure_fence(error: &ParticipationIoHandoffError) -> bool {
        let fence = |error: &ActorTurnError| {
            matches!(
                error,
                ActorTurnError::ActorFailed
                    | ActorTurnError::FailurePublicationRequired
                    | ActorTurnError::Effect(
                        novarocks_control_runtime_core::effect::EffectError::StaleGeneration
                    )
            )
        };
        match error {
            ParticipationIoHandoffError::Dispatch(DispatchError::OwnerFailed) => true,
            ParticipationIoHandoffError::ActorTurn(error)
            | ParticipationIoHandoffError::Task(TaskIoError::ActorTurn(error))
            | ParticipationIoHandoffError::Task(TaskIoError::Request(super::requests::ParticipationRequestError::ActorTurn(error)))
            | ParticipationIoHandoffError::Task(TaskIoError::DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError::ActorTurn(error)))
            | ParticipationIoHandoffError::Task(TaskIoError::Request(super::requests::ParticipationRequestError::DynamicFilter(crate::control_runtime::dynamic_filters::DynamicFilterError::ActorTurn(error))))
            | ParticipationIoHandoffError::Task(TaskIoError::Host(crate::control_runtime::task::TaskHostError::ActorTurn(error)))
            | ParticipationIoHandoffError::Task(TaskIoError::Task(super::ParticipationHostError::Task(crate::control_runtime::task::TaskHostError::ActorTurn(error))))
            | ParticipationIoHandoffError::Host(ParticipationHostJobError::ActorTurn(error))
            | ParticipationIoHandoffError::HostProgress(ParticipationHostProgressError::Host(ParticipationHostJobError::ActorTurn(error)))
            | ParticipationIoHandoffError::Request(super::requests::ParticipationRequestError::ActorTurn(error))
            | ParticipationIoHandoffError::Request(super::requests::ParticipationRequestError::Causal(crate::control_runtime::causal::CausalError::ActorTurn(error)))
            | ParticipationIoHandoffError::Task(TaskIoError::Request(super::requests::ParticipationRequestError::Causal(crate::control_runtime::causal::CausalError::ActorTurn(error))))
            | ParticipationIoHandoffError::Task(TaskIoError::Host(crate::control_runtime::task::TaskHostError::Causal(crate::control_runtime::causal::CausalError::ActorTurn(error)))) => fence(error),
            ParticipationIoHandoffError::Task(TaskIoError::Task(super::ParticipationHostError::Task(
                crate::control_runtime::task::TaskHostError::Runtime(
                    novarocks_control_runtime::transaction::RuntimeError::Scope(
                        novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch,
                    ),
                )
                | crate::control_runtime::task::TaskHostError::SourceSlots(
                    crate::control_runtime::source_slot_issuer::SourceSlotIssueError::Runtime(
                        novarocks_control_runtime::transaction::RuntimeError::Scope(
                            novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch,
                        ),
                    ),
                ),
            ))) => true,
            ParticipationIoHandoffError::Task(TaskIoError::Host(crate::control_runtime::task::TaskHostError::OwnerFailed)) => true,
            ParticipationIoHandoffError::Task(TaskIoError::Host(crate::control_runtime::task::TaskHostError::Statistics(
                crate::control_runtime::operator_statistics::StatisticsError::FailurePublicationRequired,
            ))) => true,
            ParticipationIoHandoffError::Task(TaskIoError::DynamicFilter(
                crate::control_runtime::dynamic_filters::DynamicFilterError::FailurePublicationRequired,
            ))
            | ParticipationIoHandoffError::Task(TaskIoError::Request(super::requests::ParticipationRequestError::DynamicFilter(
                crate::control_runtime::dynamic_filters::DynamicFilterError::FailurePublicationRequired,
            ))) => true,
            _ => false,
        }
    }

    fn retain_io_refusal(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        error: ParticipationIoHandoffError,
    ) -> ParticipationIoHandoffError {
        assert!(self.resources.pending_host_jobs.actor_effect.is_none());
        self.resources.pending_host_jobs.actor_effect = Some(ticket);
        error
    }

    fn handoff_actor_effect(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        tasks: &TaskIoPorts,
    ) -> Result<(), ParticipationIoHandoffError> {
        let fact = match self.observation_effect_fact(&ticket.effect) {
            Ok(fact) => fact,
            Err(error) => {
                return Err(
                    self.retain_io_refusal(ticket, ParticipationIoHandoffError::Observation(error))
                );
            }
        };
        if fact.is_some() && self.observation_publication_pending() {
            assert!(self.resources.pending_host_jobs.actor_effect.is_none());
            self.resources.pending_host_jobs.actor_effect = Some(ticket);
            return Ok(());
        }
        let result = match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::Subscribe { .. }) => self
                .handoff_observation_subscribe(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_io_refusal(ticket, ParticipationIoHandoffError::Request(error))
                }),
            WorkerAction::Participation(ParticipationEffect::PublishParticipationConvergence {
                close_epoch,
                ..
            }) => {
                if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
                    return Err(self
                        .retain_io_refusal(ticket, ParticipationIoHandoffError::ActorTurn(error)));
                }
                if self
                    .resources
                    .execution_close_epoch
                    .is_some_and(|original| original != *close_epoch)
                {
                    return Err(
                        self.retain_io_refusal(ticket, ParticipationIoHandoffError::WrongOwner)
                    );
                }
                self.resources.execution_close_epoch = Some(*close_epoch);
                self.complete_host_effect(ticket).map_err(|refusal| {
                    self.retain_io_refusal(
                        refusal.ticket,
                        ParticipationIoHandoffError::ActorTurn(refusal.error),
                    )
                })
            }
            WorkerAction::Participation(ParticipationEffect::WakeInternal) => {
                self.handoff_internal_wake(ticket).map_err(|refusal| {
                    self.retain_io_refusal(
                        refusal.ticket,
                        ParticipationIoHandoffError::ActorTurn(refusal.error),
                    )
                })
            }
            WorkerAction::Participation(ParticipationEffect::EstablishProbed { .. }) => self
                .handoff_establish_probe(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_io_refusal(ticket, ParticipationIoHandoffError::Route(error))
                }),
            WorkerAction::Participation(ParticipationEffect::EstablishmentDecided { .. }) => self
                .handoff_establishment_decision(ticket)
                .map_err(|(error, ticket)| {
                    self.retain_io_refusal(ticket, ParticipationIoHandoffError::Host(error))
                }),
            WorkerAction::Participation(ParticipationEffect::ReleaseHost { .. }) => {
                // R accepts this original instruction independently of the
                // Task cut that carried it. Its physical job still waits for
                // the original Root/Task/M/loan gates in host progress.
                self.handoff_host_release(ticket)
                    .map_err(|(error, ticket)| {
                        self.retain_io_refusal(ticket, ParticipationIoHandoffError::Host(error))
                    })
            }
            WorkerAction::Participation(ParticipationEffect::CloseExternalAdmission {
                participation,
                generation,
                reason,
            }) if *participation == self.identity => {
                if let Err(error) = self
                    .control_runtime
                    .validate_effect_ticket(&ticket)
                    .and_then(|()| self.control_runtime.close_actor_admission(self.actor))
                {
                    return Err(self
                        .retain_io_refusal(ticket, ParticipationIoHandoffError::ActorTurn(error)));
                }
                match self.close_task_io_admission(*participation, *generation, tasks) {
                    Ok(true) => {
                        let (generation, reason) = (*generation, *reason);
                        self.stage_participation_admission_close(ticket, generation, reason);
                        Ok(())
                    }
                    result => {
                        let error = result
                            .err()
                            .map(ParticipationIoHandoffError::Task)
                            .unwrap_or(ParticipationIoHandoffError::WrongOwner);
                        Err(self.retain_io_refusal(ticket, error))
                    }
                }
            }
            WorkerAction::Participation(ParticipationEffect::CloseFanoutPrepared {
                participation,
                generation,
                reason,
                targets,
            }) if *participation == self.identity => {
                if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
                    return Err(self
                        .retain_io_refusal(ticket, ParticipationIoHandoffError::ActorTurn(error)));
                }
                let Some(fanout) = self
                    .resources
                    .pending_host_jobs
                    .close_fanouts
                    .iter_mut()
                    .find(|fanout| fanout.generation == *generation && fanout.reason == *reason)
                else {
                    return Err(
                        self.retain_io_refusal(ticket, ParticipationIoHandoffError::WrongOwner)
                    );
                };
                if fanout.targets.is_some() || fanout.staged > *targets {
                    return Err(
                        self.retain_io_refusal(ticket, ParticipationIoHandoffError::WrongOwner)
                    );
                }
                fanout.targets = Some(*targets);
                self.complete_host_effect(ticket).map_err(|refusal| {
                    self.retain_io_refusal(
                        refusal.ticket,
                        ParticipationIoHandoffError::ActorTurn(refusal.error),
                    )
                })
            }
            WorkerAction::Participation(ParticipationEffect::SealRoot {
                participation,
                generation,
                reason,
            }) if *participation == self.identity => {
                if let Err(error) = self.control_runtime.validate_effect_ticket(&ticket) {
                    return Err(self
                        .retain_io_refusal(ticket, ParticipationIoHandoffError::ActorTurn(error)));
                }
                // Every new root installer crosses this same owner gate. A
                // late M return cannot reopen it after the Core close winner.
                self.root_issuance_sealed = true;
                match self.seal_task_roots(
                    *participation,
                    *generation,
                    *reason,
                    ticket.descriptor.work,
                    tasks,
                ) {
                    Ok(true) => self.complete_host_effect(ticket).map_err(|refusal| {
                        self.retain_io_refusal(
                            refusal.ticket,
                            ParticipationIoHandoffError::ActorTurn(refusal.error),
                        )
                    }),
                    result => {
                        assert!(self.resources.pending_host_jobs.actor_seal.is_none());
                        self.resources.pending_host_jobs.actor_seal = Some(ticket);
                        result
                            .map(|_| ())
                            .map_err(ParticipationIoHandoffError::Task)
                    }
                }
            }
            WorkerAction::Reply(_) => self.publish_request_reply(ticket).map_err(|refusal| {
                use super::requests::ParticipationRequestPublishRefusal;
                match refusal {
                    ParticipationRequestPublishRefusal::Rejected { error, ticket } => {
                        self.retain_io_refusal(ticket, ParticipationIoHandoffError::Request(error))
                    }
                    ParticipationRequestPublishRefusal::Publication {
                        error,
                        ticket,
                        body,
                    } => {
                        // Only a neutral Core body alias is released here.
                        drop(body);
                        self.retain_io_refusal(
                            ticket,
                            ParticipationIoHandoffError::ActorTurn(error),
                        )
                    }
                    ParticipationRequestPublishRefusal::Completion(refusal) => self
                        .retain_io_refusal(
                            refusal.ticket,
                            ParticipationIoHandoffError::ActorTurn(refusal.error),
                        ),
                }
            }),
            WorkerAction::OwnerFailed { .. } => {
                self.begin_failure_publications(ticket).map_err(|refusal| {
                    self.retain_io_refusal(
                        refusal.ticket,
                        ParticipationIoHandoffError::Failure(refusal.error),
                    )
                })
            }
            // The actual Task owner accepts the same original ticket. Its
            // refusal either returns that original or states that the real
            // admitted job has already taken custody; neither is an ACK.
            _ => self
                .handoff_task_io_effect(ticket, tasks)
                .map_err(|refusal| {
                    let error = ParticipationIoHandoffError::Task(refusal.error);
                    match refusal.ticket {
                        Some(original) => self.retain_io_refusal(original, error),
                        None => error,
                    }
                }),
        };
        result?;
        if let Some((fact, bytes)) = fact {
            self.commit_observation_fact(fact, bytes)
                .map_err(ParticipationIoHandoffError::Observation)?;
        }
        Ok(())
    }

    /// Failed batches bypass normal publication lanes. Discover only the
    /// runtime's actual original supervisor action, with a bounded cursor.
    fn find_actor_failure(
        &mut self,
        tasks: &TaskIoPorts,
        budget: usize,
    ) -> Result<bool, ParticipationIoHandoffError> {
        for _ in 0..budget {
            let after = self.resources.pending_host_jobs.failure_after;
            let next = self
                .control_runtime
                .committed_batches(self.actor, after, 1)
                .map_err(ParticipationIoHandoffError::ActorTurn)?
                .first()
                .copied();
            let Some((ordinal, input)) = next else {
                return Ok(false);
            };
            self.resources.pending_host_jobs.failure_after = Some(ordinal);
            let inventory = self
                .control_runtime
                .effect_inventory(
                    self.actor,
                    input,
                    None,
                    ParticipationActor::MAX_STEP_ACTIONS,
                )
                .map_err(ParticipationIoHandoffError::ActorTurn)?;
            for (descriptor, state) in inventory {
                if state != EffectState::Pending {
                    continue;
                }
                let metadata = self
                    .control_runtime
                    .pending_effect_metadata(self.actor, input, descriptor.id)
                    .map_err(ParticipationIoHandoffError::ActorTurn)?;
                if matches!(metadata, Some(WorkerAction::OwnerFailed { .. })) {
                    let original = self
                        .control_runtime
                        .take_effect(self.actor, input, descriptor.id)
                        .map_err(ParticipationIoHandoffError::ActorTurn)?;
                    self.handoff_actor_effect(original, tasks)?;
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
    pub(crate) fn advance_io_handoff(
        &mut self,
        handles: &ParticipationExecutorHandles,
        requests: &ControlRequestReady,
        tasks: &TaskIoPorts,
        budget: usize,
    ) -> Result<ParticipationIoHandoffProgress, ParticipationIoHandoffError> {
        match self.advance_io_handoff_inner(handles, requests, tasks, budget) {
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
                Ok(ParticipationIoHandoffProgress {
                    runnable: true,
                    ..Default::default()
                })
            }
            result => result,
        }
    }

    fn advance_io_handoff_inner(
        &mut self,
        handles: &ParticipationExecutorHandles,
        requests: &ControlRequestReady,
        tasks: &TaskIoPorts,
        budget: usize,
    ) -> Result<ParticipationIoHandoffProgress, ParticipationIoHandoffError> {
        let mut progress = ParticipationIoHandoffProgress::default();
        if budget == 0 {
            return Ok(progress);
        }
        if !requests.same_runtime(&self.control_runtime) {
            return Err(ParticipationIoHandoffError::WrongOwner);
        }
        self.configure_observation(tasks.observation)
            .map_err(ParticipationIoHandoffError::Observation)?;
        // Registration precedes the original bootstrap's M/R/H binding. A
        // different poller can claim this address in that interval; it must
        // not take an effect whose actual receiver is still being installed.
        // Staging the original M supplies the subsequent real owner notice.
        let Some(jobs) = self.resources.host_jobs.as_ref() else {
            return Ok(progress);
        };
        if !handles.accepts_owner(jobs) {
            return Err(ParticipationIoHandoffError::WrongOwner);
        }
        let detached = self.resources.scope.is_none();
        if !detached {
            jobs.bind_execution_host(tasks.execution.as_ref())
                .map_err(ParticipationIoHandoffError::Host)?;
        }
        // Admission partials belong to the Participation's request custody, not
        // its execution Scope. Every host turn advances this bounded class
        // once, including healthy retained actors and failed cleanup actors.
        self.advance_admission_partials(tasks, budget)
            .map_err(ParticipationIoHandoffError::Request)?;
        let failed = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ParticipationIoHandoffError::ActorTurn)?
            .failed;
        if failed && detached {
            let publication_active = self
                .resources
                .failure
                .as_ref()
                .is_some_and(|owner| owner.publication_active())
                && !self.failure_tail_started();
            if publication_active {
                progress.failure_publications_drained =
                    self.advance_failure_publications(budget)
                        .map_err(ParticipationIoHandoffError::Failure)?;
                if progress.failure_publications_drained {
                    let inputs = self
                        .advance_failed_runtime_inputs(budget)
                        .map_err(ParticipationIoHandoffError::Failure)?;
                    progress.runnable |= !inputs.empty;
                } else {
                    progress.runnable = true;
                }
            }
            if self
                .resources
                .failure
                .as_ref()
                .is_some_and(|owner| owner.is_drained())
            {
                progress.runnable |= self.advance_cold_failure_tail(tasks, budget)?;
            }
        } else if failed {
            if self.failure_tail_started() {
                progress.runnable |= self.advance_failed_cleanup_tail(tasks, budget)?;
            }
            if self.resources.failure.is_none() && !self.failed_owner_completed() {
                progress.runnable = self.find_actor_failure(tasks, budget)?;
            }
            if self.resources.failure.is_some() && !self.failure_tail_started() {
                progress.failure_publications_drained =
                    self.advance_failure_publications(budget)
                        .map_err(ParticipationIoHandoffError::Failure)?;
                if progress.failure_publications_drained {
                    self.discard_failed_internal_hint()
                        .map_err(ParticipationIoHandoffError::Failure)?;
                    let inputs = self
                        .advance_failed_runtime_inputs(budget)
                        .map_err(ParticipationIoHandoffError::Failure)?;
                    progress.runnable |= !inputs.empty;
                    let proof = self
                        .control_runtime
                        .failure_proof_for(self.registration())
                        .map_err(ParticipationIoHandoffError::ActorTurn)?;
                    if let Some(lifecycle) = self.resources.lifecycle.as_mut() {
                        lifecycle
                            .withdraw_failed_callback(&proof)
                            .map_err(ParticipationIoHandoffError::Host)?;
                    }
                    self.advance_failed_root_release(tasks, budget)?;
                    progress.runnable |= self.advance_failed_task_cleanup(tasks, budget)?;
                } else {
                    progress.runnable = true;
                }
            }
        } else {
            if let Some(original) = self.resources.pending_host_jobs.actor_seal.take() {
                self.handoff_actor_effect(original, tasks)?;
            }
            let allowance = budget.saturating_mul(ParticipationActor::MAX_STEP_ACTIONS + 1);
            let indexed = self
                .dispatch
                .index_committed(&self.control_runtime, allowance)
                .map_err(ParticipationIoHandoffError::Dispatch)?;
            self.domain_inputs_committed(&indexed.inputs);
            self.task_io_inputs_committed(&indexed.inputs)
                .map_err(ParticipationIoHandoffError::Task)?;
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
                let original = match self.resources.pending_host_jobs.actor_effect.take() {
                    Some(original) => Some(original),
                    None => {
                        let allowance = self
                            .resources
                            .pending_host_jobs
                            .effect_scan_remaining
                            .min(self.dispatch.ready_positions())
                            .min(ParticipationActor::MAX_STEP_ACTIONS);
                        if allowance == 0 {
                            self.resources.pending_host_jobs.effect_scan_remaining = 0;
                            break;
                        }
                        let original = self
                            .dispatch
                            .take_ready(&self.control_runtime, allowance)
                            .map_err(ParticipationIoHandoffError::Dispatch)?;
                        if original.is_none() {
                            self.resources.pending_host_jobs.effect_scan_remaining -= allowance;
                        }
                        original
                    }
                };
                let Some(original) = original else {
                    break;
                };
                self.handoff_actor_effect(original, tasks)?;
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
                .map_err(ParticipationIoHandoffError::Dispatch)?;
            self.advance_participation_admission_closes(tasks)
                .map_err(ParticipationIoHandoffError::Task)?;
        }
        // Failure takeover preserves the same physical close and gate. Its
        // real return must also drain the original neutral ready addresses;
        // a failed Core never waits for the normal scan-completion marker.
        progress.runnable |= self.advance_participation_close_notifications(budget);
        progress.runnable |= self
            .advance_observation_publication(budget)
            .map_err(ParticipationIoHandoffError::Observation)?;
        if let Some(capacity) = &tasks.capacity {
            progress.runnable |= requests
                .retry_capacity(self.registration(), capacity.revision(), budget)
                .map_err(|error| {
                    ParticipationIoHandoffError::Request(
                        super::requests::ParticipationRequestError::Ready(error),
                    )
                })?;
        }
        progress.runnable |= self
            .advance_sealed_task_reads(budget)
            .map_err(ParticipationIoHandoffError::Request)?;
        progress.runnable |= self.advance_domain_wakes(budget);
        for _ in 0..budget {
            let Some(work) = requests.next_participation_ready(self.registration()) else {
                break;
            };
            self.advance_domain_request(work, tasks)
                .map_err(ParticipationIoHandoffError::Request)?;
            self.advance_request_notification(work, tasks)
                .map_err(ParticipationIoHandoffError::Request)?;
            self.advance_request(work)
                .map_err(ParticipationIoHandoffError::Request)?;
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
                if failed
                    && let Some(failure) = &self.resources.failure
                    && let Ok(stage) = failure.drained()
                {
                    self.resources.routes[&input]
                        .fail_after_publications_drained(&stage)
                        .map_err(ParticipationIoHandoffError::Route)?;
                }
                if self.resources.routes[&input].participation_cleanup_ready()
                    && self
                        .advance_establish_route(input)
                        .map_err(ParticipationIoHandoffError::Route)?
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
            .map_err(ParticipationIoHandoffError::ActorTurn)?
        {
            // Metadata completion and cursor release are distinct facts. The
            // next original bounded scan must finish borrowing this batch
            // before its Timer input is removed from the runtime.
            let scanning = self
                .resources
                .lifecycle
                .as_ref()
                .is_some_and(|original| original.scans_input(input));
            if !scanning {
                match self.control_runtime.retire_input(self.actor, input) {
                    Ok(()) => progress.timer_inputs_retired += 1,
                    Err(
                        ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain
                        | ActorTurnError::InputStillQueued
                        | ActorTurnError::MetadataStillPending,
                    ) => {}
                    Err(error) => return Err(ParticipationIoHandoffError::ActorTurn(error)),
                }
            }
        }
        if !failed && !detached {
            let task_progress = self
                .advance_task_io(tasks, budget)
                .map_err(ParticipationIoHandoffError::Task)?;
            progress.runnable |= task_progress.runnable;
            progress.tasks = Some(task_progress);
        }
        if failed && !detached {
            if progress.failure_publications_drained {
                progress.host = Some(self.advance_failed_host_io(handles, tasks, budget)?);
            }
        } else if !detached {
            progress.host = Some(
                self.advance_host_io(handles)
                    .map_err(ParticipationIoHandoffError::HostProgress)?,
            );
        }
        progress.runnable |= self
            .advance_root_reads(tasks, budget)
            .map_err(ParticipationIoHandoffError::Request)?;
        match self.advance_observation_teardown(budget) {
            ObservationTeardownProgress::Complete => {
                if failed
                    && !detached
                    && !self.failed_owner_completed()
                    && !self.failure_tail_started()
                {
                    progress.runnable |= self.advance_failed_cleanup_tail(tasks, budget)?;
                }
                progress.metadata_teardown_ready = self
                    .advance_metadata_teardown()
                    .map_err(ParticipationIoHandoffError::Metadata)?;
            }
            // Cold metadata still owns its bounded cursor after the execution
            // Scope has exited. No physical producer remains to wake this tail.
            ObservationTeardownProgress::MoreReady => progress.runnable = true,
            ObservationTeardownProgress::Blocked => {}
        }
        if progress.metadata_teardown_ready
            && let Some(capacity) = &tasks.capacity
        {
            capacity
                .unregister_waiter(self.registration())
                .map_err(ParticipationIoHandoffError::Capacity)?;
        }
        // A real event during this sweep is retained even if its old cursor
        // had already passed the affected row. No waiting state creates it.
        progress.runnable |= self.resources.pending_host_jobs.route_sweep_version
            != Some(self.ready_notice().version());
        Ok(progress)
    }
}
