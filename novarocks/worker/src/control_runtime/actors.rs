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

//! A participation step has one protocol decision and one atomic responsibility
//! handoff. Host jobs, rich sources and reply channels stay outside this actor.

use crate::control_admission::{AdmissionStorage, ScopeAdmissionPort};
use novarocks_control_runtime::{
    actor::{Actor, ActorStep},
    actor_turn::InputAuthor,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, Generation, InputId, ScopeId, ScopedWorkRef, Tick},
    mailbox::InputEnvelope,
};
use novarocks_execution_contract::{ParticipationState, TaskIdentity};
use novarocks_worker_protocol::{
    creation::RequestReplyTag,
    lease::MonotonicInstant,
    participation::{
        ParticipationCore, ParticipationEffect, ParticipationInput, ParticipationRequestSettlement,
    },
};
use std::{collections::BTreeMap, time::Duration};

pub(crate) use crate::task_proposal_ingress::RequestMetadata;

#[derive(Debug)]
pub(crate) enum ParticipationMessage {
    Command {
        work: ScopedWorkRef,
        request: Option<RequestMetadata>,
        command: ParticipationInput,
    },
    /// A declared experimental action selects this same admitted R occurrence.
    /// It changes no pure protocol input and allocates no additional position.
    #[cfg(novarocks_dev_controls)]
    TestOwnerPanicOnRenew {
        work: ScopedWorkRef,
        request: RequestMetadata,
        command: ParticipationInput,
    },
    Timer,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Prepaid effect positions store bounded neutral protocol values inline; no host payload or foreign destructor is carried."
)]
#[derive(Clone, Debug)]
pub(crate) enum WorkerAction {
    AdmissionEpoch(super::node_metrics::ControlAdmissionProjection),
    Participation(ParticipationEffect),
    Control {
        effect: novarocks_worker_protocol::control::ControlEffect,
        /// Only actual Participation materialization has an original guard in this
        /// batch. Ticket grants carry protocol metadata without a reservation.
        admitted: Option<novarocks_control_runtime_core::identity::ReservationId>,
    },
    Reply(ParticipationRequestSettlement),
    ControlReply(novarocks_worker_protocol::control::ControlRequestSettlement),
    OwnerFailed {
        scope: ScopeId,
        generation: Generation,
    },
}

pub(crate) struct ParticipationActor {
    core: ParticipationCore,
    // The only live port is the explicitly allowed synchronous ledger port.
    // Its generation is the runtime failure fence, not the protocol close epoch.
    scope: ScopeHandle,
    runtime_generation: Generation,
    storage: AdmissionStorage,
    timer_work: ScopedWorkRef,
    lifetime_work: ScopedWorkRef,
    // Read-only origins only. Reply decisions/once cells remain in their
    // original pure Core/host owners; no credit is cloned by these proofs.
    request_authors: BTreeMap<InputId, InputAuthor>,
    // The builtin timer is the retained actor's one coalesced continuation.
    // It allocates no Work or second self-mailbox position.
    retained_continuation: Option<Tick>,
}
impl ParticipationActor {
    // The pure bound includes its selector probes and representation swap.
    // The adapter adds at most one coalesced scheduling effect.
    pub(crate) const MAX_STEP_WORK: usize = ParticipationCore::MAX_STEP_WORK + 1;
    pub(crate) const MAX_STEP_ACTIONS: usize = ParticipationCore::MAX_STEP_ACTIONS + 1;
    pub(crate) const TURN_BUDGET: usize = 4 * (Self::MAX_STEP_WORK + 1);
    pub(crate) fn new(
        core: ParticipationCore,
        scope: ScopeHandle,
        storage: AdmissionStorage,
        timer_work: ScopedWorkRef,
        lifetime_work: ScopedWorkRef,
    ) -> Self {
        assert_eq!(
            timer_work.scope,
            scope.id(),
            "original Participation timer scope"
        );
        assert_eq!(
            lifetime_work.scope,
            scope.id(),
            "original Participation lifetime scope"
        );
        let runtime_generation = scope.gates().expect("live participation scope").generation;
        Self {
            core,
            scope,
            runtime_generation,
            storage,
            timer_work,
            lifetime_work,
            request_authors: BTreeMap::new(),
            retained_continuation: None,
        }
    }
}
impl Actor for ParticipationActor {
    type Input = ParticipationMessage;
    type Effect = WorkerAction;
    fn timer_input(_: Tick) -> Self::Input {
        ParticipationMessage::Timer
    }
    fn handle(
        &mut self,
        now: Tick,
        envelope: InputEnvelope,
        message: ParticipationMessage,
        step: &mut ActorStep<'_, WorkerAction>,
    ) {
        let now = MonotonicInstant::from_origin(Duration::from_nanos(now.0));
        let metadata_timer = step.is_retained() && matches!(&message, ParticipationMessage::Timer);
        if metadata_timer {
            assert!(
                matches!(
                    self.core.state(),
                    ParticipationState::TerminalRetained | ParticipationState::Gone
                ),
                "an original retained actor keeps only terminal protocol state"
            );
        }
        #[cfg(novarocks_dev_controls)]
        let panic_on_renew = matches!(&message, ParticipationMessage::TestOwnerPanicOnRenew { .. });
        let (work, request, command) = match message {
            ParticipationMessage::Command {
                work,
                request,
                command,
            } => (work, request, command),
            #[cfg(novarocks_dev_controls)]
            ParticipationMessage::TestOwnerPanicOnRenew {
                work,
                request,
                command,
            } => {
                assert!(
                    matches!(
                        &command,
                        ParticipationInput::Request(
                            novarocks_worker_protocol::participation::ParticipationCommand::Renew { .. }
                        )
                    ),
                    "the selected experimental turn retains its original Renew"
                );
                (work, Some(request), command)
            }
            ParticipationMessage::Timer => {
                let command = if metadata_timer
                    && self.retained_continuation.take().is_some()
                    && self.core.has_internal_work()
                {
                    ParticipationInput::NextInternal
                } else {
                    ParticipationInput::Tick
                };
                (self.timer_work, None, command)
            }
        };
        let mut admission = ScopeAdmissionPort::new(
            self.scope.clone(),
            self.runtime_generation,
            Some(self.core.identity()),
            self.storage.clone(),
        )
        .with_parent(self.lifetime_work.work);
        // Keep failure diagnostics neutral and finite. The command may own
        // large domain inputs; never format its payload during unwinding.
        let (input_kind, input_task) = match &command {
            ParticipationInput::RejectedCreationRetired { task, .. } => {
                ("RejectedCreationRetired", Some(*task))
            }
            ParticipationInput::LoadedTask { task, .. } => ("LoadedTask", Some(*task)),
            ParticipationInput::Loaded { task, .. } => ("Loaded", Some(*task)),
            ParticipationInput::Request(_) => ("Request", None),
            _ => ("OtherParticipationInput", None),
        };
        let current_author = step.input_author().cloned();
        if let Some(proof) = &current_author {
            assert!(
                proof.work() == work
                    || (request.is_none() && !step.is_retained() && work.scope == self.scope.id()),
                "a request keeps its exact author; internal duty stays in the live execution scope"
            );
        }
        if request.is_some() {
            let proof = current_author.expect("an admitted R has its original runtime author");
            assert!(
                self.request_authors.insert(envelope.input, proof).is_none(),
                "one original proof per R token"
            );
        }
        let result = match request {
            Some(request) => {
                let original = RequestReplyTag {
                    author: work,
                    token: envelope.input,
                    operation: request.operation,
                    deadline: request.deadline,
                };
                match command {
                    ParticipationInput::Request(command) => self.core.handle_request(now, original, command, &mut admission),
                    ParticipationInput::RootEndAcknowledged { task } => self.core.handle_root_end_acknowledged(now, original, task, &mut admission),
                    _ => panic!("an admitted Participation R carries its original command or End ACK"),
                }
            }
            None => self.core.handle(now, command, &mut admission),
        }
        .unwrap_or_else(|error| {
            self.request_authors.clear();
            panic!(
                "participation protocol input preserves its frozen invariants: input={input_kind}, task={input_task:?}, error={error:?}"
            )
        });
        #[cfg(novarocks_dev_controls)]
        if panic_on_renew && request.is_some_and(|request| !now.has_reached(request.deadline)) {
            // The pure decision exists but no effect/reply has left this turn.
            // Common catches this panic and owns the actual scope-failure path.
            self.request_authors.clear();
            panic!(
                "declared Participation owner panic participation={:?} operation={}",
                self.core.identity(),
                request.expect("original Renew metadata").operation
            );
        }
        let internal_ready = self.core.has_internal_work();
        if !step.is_retained()
            && matches!(
                self.core.state(),
                ParticipationState::Aborting
                    | ParticipationState::Releasing
                    | ParticipationState::TerminalRetained
            )
        {
            // Commit the start fence with this same pure decision, before a
            // host consumes any stop/release effect. Already started work and
            // conclusion-before-stop publication keep their separate duties.
            step.stop_producing();
        }
        assert!(
            step.charge(result.records_visited),
            "bounded participation traversal"
        );
        let guards = admission.take_guards();
        let mut reservations = BTreeMap::new();
        for guard in guards {
            reservations.insert(
                guard.reservation_id(),
                ScopedWorkRef {
                    scope: guard.scope().id(),
                    work: guard.work_id(),
                },
            );
            if let Err((error, guard)) = step.reservation(guard) {
                // Still pure and unbound. Roll back outside every ledger lock.
                drop(guard);
                panic!("participation reservation handoff invariant: {error:?}");
            }
        }
        let mut next = 0u64;
        let mut publications = BTreeMap::<TaskIdentity, ActionId>::new();
        let mut reply_preparations = BTreeMap::<InputId, Vec<ActionId>>::new();
        for effect in result.effects {
            // These hints carry no host facts or payload. The Core retains
            // its cursors; one prepaid self-source selects the next slice.
            if matches!(
                &effect,
                ParticipationEffect::ContinueTaskDomain { .. }
                    | ParticipationEffect::ContinueRequestReplies { .. }
                    | ParticipationEffect::ContinueQuiesce
                    | ParticipationEffect::ContinueClose { .. }
                    | ParticipationEffect::ContinueReplies { .. }
            ) {
                continue;
            }
            if step.is_retained()
                && matches!(
                    &effect,
                    ParticipationEffect::RetainParticipation { .. }
                        | ParticipationEffect::Expired { .. }
                        | ParticipationEffect::WakeInternal
                )
            {
                step.metadata(WorkerAction::Participation(effect))
                    .unwrap_or_else(|(error, _)| {
                        panic!("bounded Participation metadata handoff: {error:?}")
                    });
                continue;
            }
            assert!(
                !metadata_timer,
                "a retained timer emits only original neutral metadata"
            );
            let mut dependencies = Vec::new();
            let mut effect_work = work;
            let kind = match &effect {
                ParticipationEffect::LoadInput { reservation, .. } => {
                    effect_work = *reservations
                        .get(reservation)
                        .expect("original step reservation accompanies host admission");
                    EffectKind::Business
                }
                ParticipationEffect::EnqueuePreparation {
                    task, reservation, ..
                } => {
                    effect_work = *reservations
                        .get(reservation)
                        .expect("original Accepted reservation accompanies preparation");
                    dependencies.push(
                        *publications
                            .get(task)
                            .expect("Accepted publication precedes preparation dispatch"),
                    );
                    EffectKind::Business
                }
                ParticipationEffect::PublishTask { task, .. }
                | ParticipationEffect::PublishMetrics { task, .. }
                | ParticipationEffect::PublishTaskConvergence { task, .. }
                | ParticipationEffect::SummaryConsumed { task, .. }
                | ParticipationEffect::RetireTask { task, .. }
                | ParticipationEffect::FinalizeTaskExecution { task, .. }
                | ParticipationEffect::PublishConclusion { task, .. } => {
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Publication
                }
                ParticipationEffect::StopTask { task, .. } => {
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Cleanup
                }
                ParticipationEffect::SettlementBarrier { task, work: role } => {
                    assert_eq!(*role, work, "settlement uses its original admitted Work");
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Publication
                }
                ParticipationEffect::DisposeSource { .. }
                | ParticipationEffect::DisposeLoadedInput { .. }
                | ParticipationEffect::CancelLoader { .. }
                | ParticipationEffect::ReleaseHost { .. }
                | ParticipationEffect::SealRoot { .. } => EffectKind::Cleanup,
                // These are neutral control/publication handoffs. Their actual
                // subsystem jobs require their own originally admitted roles.
                _ => EffectKind::Publication,
            };
            next += 1;
            let id = ActionId::new(next).unwrap();
            match &effect {
                ParticipationEffect::PublishTask { task, .. }
                | ParticipationEffect::PublishMetrics { task, .. }
                | ParticipationEffect::PublishTaskConvergence { task, .. }
                | ParticipationEffect::SummaryConsumed { task, .. }
                | ParticipationEffect::RetireTask { task, .. }
                | ParticipationEffect::FinalizeTaskExecution { task, .. }
                | ParticipationEffect::PublishConclusion { task, .. } => {
                    publications.insert(*task, id);
                }
                _ => {}
            }
            if let ParticipationEffect::PrepareReadPayload { request, task, .. } = &effect {
                // This is an actual mechanical read preparation, not a second
                // copy of the Core-authored reply body. A real loan pin must be
                // established before the corresponding reply can start.
                if let Some(publication) = publications.get(task) {
                    dependencies.push(*publication);
                }
                reply_preparations.entry(*request).or_default().push(id);
            }
            step.effect(
                EffectDescriptor {
                    id,
                    work: effect_work,
                    kind,
                    dependencies,
                },
                WorkerAction::Participation(effect),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded participation effect handoff: {error:?}"));
        }
        for reply in result.requests {
            next += 1;
            let mut dependencies = reply_preparations
                .remove(&reply.request.token)
                .unwrap_or_default();
            // The immutable complete reply is already authored by this Core
            // step. Only actual publication and read preparation gate delivery.
            if let Some(task) = reply.task {
                dependencies.extend(publications.get(&task).copied());
            } else {
                dependencies.extend(publications.values().copied());
            }
            let proof = self
                .request_authors
                .remove(&reply.request.token)
                .expect("the pure reply returns its original R origin");
            assert_eq!(
                proof.work(),
                reply.request.author,
                "pure request metadata preserves the original author"
            );
            step.effect_from_author(
                &proof,
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work: reply.request.author,
                    kind: EffectKind::Reply,
                    dependencies,
                },
                WorkerAction::Reply(reply),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded participation reply handoff: {error:?}"));
        }
        self.retained_continuation = (internal_ready && step.is_retained()).then_some(Tick(
            u64::try_from(now.since_origin().as_nanos()).expect("runtime Tick fits its origin"),
        ));
        if internal_ready && !step.is_retained() {
            next += 1;
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work: self.timer_work,
                    kind: EffectKind::Publication,
                    dependencies: Vec::new(),
                },
                WorkerAction::Participation(ParticipationEffect::WakeInternal),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded Participation internal wake: {error:?}"));
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.core
            .next_deadline()
            .map(|at| Tick(u64::try_from(at.since_origin().as_nanos()).unwrap_or(u64::MAX)))
            .into_iter()
            .chain(self.retained_continuation)
            .min()
    }
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        _: InputEnvelope,
        step: &mut ActorStep<'_, WorkerAction>,
    ) {
        let work = step.failure_work().expect(
            "participation registration retains its original cleanup role outside the core",
        );
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work,
                kind: EffectKind::Cleanup,
                dependencies: Vec::new(),
            },
            WorkerAction::OwnerFailed { scope, generation },
        )
        .unwrap_or_else(|(error, _)| panic!("fixed participation failure handoff: {error:?}"));
    }
}

#[cfg(test)]
pub(super) mod tests;

mod control;
mod worker;
pub(crate) use control::{ControlActor, ControlMessage};
pub(crate) use worker::{WorkerActor, WorkerMessage};
