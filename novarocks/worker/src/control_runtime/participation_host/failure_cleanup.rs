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

//! Transfer the original failed batch cleanup into its actual resource owner.

use super::*;
use crate::control_runtime::adopted_cleanup::AdoptedCleanupEffect;
use crate::control_runtime::participation_host::ParticipationIoHandoffError;
use novarocks_worker_protocol::participation::ParticipationEffect;

impl ParticipationHostRecord {
    /// Finish this finite binding pass before Task source/header retirement.
    /// No new inputs can be committed by this already-failed Core.
    pub(super) fn bind_failed_cleanup(
        &mut self,
        original: &mut ScopeFailureOwner,
        budget: usize,
    ) -> Result<bool, ParticipationIoHandoffError> {
        if let Some(ticket) = self.resources.pending_host_jobs.actor_effect.take() {
            if ticket.descriptor.kind.allowed_after_failure() {
                if let Err((error, ticket)) = self.adopt_failed_cleanup(original, ticket) {
                    self.resources.pending_host_jobs.actor_effect = Some(ticket);
                    return Err(error);
                }
            } else {
                self.resources.pending_host_jobs.actor_effect = Some(ticket);
            }
        }
        for _ in 0..budget {
            match original.cleanup_bindings {
                CleanupBindingCursor::Bound => return Ok(true),
                CleanupBindingCursor::Batches(after) => {
                    let inputs = self
                        .control_runtime
                        .pending_batches(self.actor, after, 1)
                        .map_err(ParticipationIoHandoffError::ActorTurn)?;
                    match inputs.first().copied() {
                        Some(input)
                            if original
                                .ticket
                                .as_ref()
                                .is_some_and(|ticket| ticket.input == input) =>
                        {
                            original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                        }
                        Some(input) => {
                            original.cleanup_bindings = CleanupBindingCursor::Effects(input, None)
                        }
                        None => original.cleanup_bindings = CleanupBindingCursor::Bound,
                    }
                }
                CleanupBindingCursor::Effects(input, after) => {
                    let effects = match self
                        .control_runtime
                        .effect_inventory(self.actor, input, after, 1)
                    {
                        Ok(effects) => effects,
                        Err(ActorTurnError::UnknownInput) => {
                            original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                            continue;
                        }
                        Err(error) => return Err(ParticipationIoHandoffError::ActorTurn(error)),
                    };
                    let Some((descriptor, state)) = effects.first() else {
                        original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                        continue;
                    };
                    if *state == EffectState::Pending && descriptor.kind.allowed_after_failure() {
                        let ticket = original
                            .session_mut()
                            .map_err(ParticipationIoHandoffError::Failure)?
                            .take_cleanup(input, descriptor.id)
                            .map_err(ParticipationIoHandoffError::ActorTurn)?;
                        if let Err((error, ticket)) = self.adopt_failed_cleanup(original, ticket) {
                            // This is the same exclusive ticket, retained at
                            // the existing dispatch-refusal position.
                            assert!(self.resources.pending_host_jobs.actor_effect.is_none());
                            self.resources.pending_host_jobs.actor_effect = Some(ticket);
                            return Err(error);
                        }
                    }
                    original.cleanup_bindings =
                        CleanupBindingCursor::Effects(input, Some(descriptor.id));
                }
            }
        }
        Ok(matches!(
            original.cleanup_bindings,
            CleanupBindingCursor::Bound
        ))
    }

    fn adopt_failed_cleanup(
        &mut self,
        original: &ScopeFailureOwner,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ParticipationIoHandoffError, EffectTicket<WorkerAction>)> {
        let stage = match original.drained() {
            Ok(stage) => stage,
            Err(error) => return Err((ParticipationIoHandoffError::Failure(error), ticket)),
        };
        if let Err(error) = stage.validate(&self.control_runtime, self.actor) {
            return Err((ParticipationIoHandoffError::ActorTurn(error), ticket));
        }
        match &ticket.effect {
            WorkerAction::Participation(ParticipationEffect::SealRoot {
                participation, ..
            }) if *participation == self.identity => {
                if self.resources.pending_host_jobs.actor_seal.is_some() {
                    return Err((
                        ParticipationIoHandoffError::ActorTurn(ActorTurnError::StandingInputBusy),
                        ticket,
                    ));
                }
                self.resources.pending_host_jobs.actor_seal = Some(ticket);
                Ok(())
            }
            WorkerAction::Participation(ParticipationEffect::ReleaseHost {
                participation, ..
            }) if *participation == self.identity => {
                let Some(jobs) = self.resources.host_jobs.as_ref() else {
                    return Err((ParticipationIoHandoffError::WrongOwner, ticket));
                };
                if let Err(error) = jobs.request_failed_release(&stage) {
                    return Err((ParticipationIoHandoffError::Host(error), ticket));
                }
                // This exact drained-stage handoff ends only the source
                // effect. The original R body/return still waits for every
                // Root/Task/M/loan gate in the shared physical path.
                self.complete_host_effect(ticket).map_err(|refusal| {
                    (
                        ParticipationIoHandoffError::ActorTurn(refusal.error),
                        refusal.ticket,
                    )
                })
            }
            WorkerAction::Participation(ParticipationEffect::CancelLoader { task, .. }) => {
                if let Some(TaskHostOwner::Unpublished(owner)) = self.tasks.get(task) {
                    if owner.task != *task {
                        return Err((ParticipationIoHandoffError::WrongOwner, ticket));
                    }
                    if let Err(error) =
                        owner.validate_never_built_loader(&stage, &self.control_runtime, self.actor)
                    {
                        return Err((
                            ParticipationIoHandoffError::Task(task_io::TaskIoError::Host(error)),
                            ticket,
                        ));
                    }
                    // Authenticate this same original payload incarnation;
                    // CancelLoader itself contains only the neutral Task ID.
                    if let Err(error) = self.validate_failed_unpublished_create(
                        &stage,
                        owner.loading_input,
                        *task,
                        owner.payload,
                    ) {
                        return Err((ParticipationIoHandoffError::Request(error), ticket));
                    }
                    // Only the cancellation instruction is now discharged.
                    // Original guard/roles still retire through their owners.
                    return self.complete_host_effect(ticket).map_err(|refusal| {
                        (
                            ParticipationIoHandoffError::ActorTurn(refusal.error),
                            refusal.ticket,
                        )
                    });
                }
                let owner = match self.failed_task_for_cleanup(&stage, *task) {
                    Ok(owner) => owner,
                    Err(error) => {
                        return Err((ParticipationIoHandoffError::Metadata(error), ticket));
                    }
                };
                // This field is the original loader custodian; it is never
                // cleared while the Task remains owned. None means LoadInput
                // was never handed to a loader, rather than inferred exit.
                if let Some(loader) = owner.loader() {
                    loader.cancel();
                }
                self.complete_host_effect(ticket).map_err(|refusal| {
                    (
                        ParticipationIoHandoffError::ActorTurn(refusal.error),
                        refusal.ticket,
                    )
                })
            }
            WorkerAction::Participation(
                ParticipationEffect::StopTask { task, .. }
                | ParticipationEffect::DisposeSource { task, .. }
                | ParticipationEffect::DisposeLoadedInput { task, .. },
            ) => {
                let notice = self.request_notice_for_input(ticket.input);
                let runtime = self.control_runtime.clone();
                let owner = match self.failed_task_for_cleanup(&stage, *task) {
                    Ok(owner) => owner,
                    Err(error) => {
                        return Err((ParticipationIoHandoffError::Metadata(error), ticket));
                    }
                };
                if matches!(&ticket.effect, WorkerAction::Participation(ParticipationEffect::DisposeLoadedInput { payload, .. }) if *payload != owner.original_payload_id())
                {
                    return Err((ParticipationIoHandoffError::WrongOwner, ticket));
                }
                let cleanup = AdoptedCleanupEffect::from_original(&runtime, &stage, ticket, notice)
                    .map_err(|(error, ticket)| {
                        (ParticipationIoHandoffError::ActorTurn(error), ticket)
                    })?;
                match cleanup.effect() {
                    WorkerAction::Participation(ParticipationEffect::StopTask { .. }) => owner
                        .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                        .adopt_failed_stop_effect(&stage, cleanup)
                        .map_err(|(error, cleanup)| {
                            (
                                ParticipationIoHandoffError::Task(task_io::TaskIoError::HostCall(
                                    error,
                                )),
                                cleanup.into_ticket(),
                            )
                        }),
                    WorkerAction::Participation(ParticipationEffect::DisposeSource { .. }) => owner
                        .port()
                        .ingress()
                        .adopt_source_cleanup(&stage, cleanup)
                        .map_err(|(error, cleanup)| {
                            (
                                ParticipationIoHandoffError::ActorTurn(error),
                                cleanup.into_ticket(),
                            )
                        }),
                    WorkerAction::Participation(ParticipationEffect::DisposeLoadedInput {
                        ..
                    }) => {
                        let Some(loader) = owner.loader() else {
                            return Err((
                                ParticipationIoHandoffError::WrongOwner,
                                cleanup.into_ticket(),
                            ));
                        };
                        loader
                            .adopt_input_cleanup(&stage, cleanup)
                            .map_err(|(error, cleanup)| {
                                (
                                    ParticipationIoHandoffError::ActorTurn(error),
                                    cleanup.into_ticket(),
                                )
                            })
                    }
                    _ => unreachable!("selected original resource cleanup"),
                }
            }
            _ => Err((ParticipationIoHandoffError::WrongOwner, ticket)),
        }
    }
}
