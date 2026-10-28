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

//! A context step has one protocol decision and one atomic responsibility
//! handoff. Host jobs, rich sources and reply channels stay outside this actor.

use crate::control_admission::{AdmissionStorage, ScopeAdmissionPort};
use novarocks_control_runtime::{
    actor::{Actor, StepContext},
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, Generation, InputId, ScopeId, Tick, WorkId},
    mailbox::InputEnvelope,
};
use novarocks_execution_contract::{QueryContextState, TaskIdentity};
use novarocks_worker_protocol::{
    context::{ContextCore, ContextEffect, ContextInput, ContextRequestSettlement},
    creation::RequestContext,
    lease::MonotonicInstant,
};
use std::{collections::BTreeMap, time::Duration};

pub(crate) use crate::task_proposal_ingress::RequestMetadata;

#[expect(
    clippy::large_enum_variant,
    reason = "The admitted input already pays for the neutral protocol command; no opaque payload is stored here."
)]
#[derive(Debug)]
pub(crate) enum ContextMessage {
    Command {
        work: WorkId,
        request: Option<RequestMetadata>,
        command: ContextInput,
    },
    Timer,
}

#[expect(
    clippy::large_enum_variant,
    reason = "Prepaid effect positions store bounded neutral protocol values inline; no host payload or foreign destructor is carried."
)]
#[derive(Clone, Debug)]
pub(crate) enum WorkerAction {
    Context(ContextEffect),
    Control {
        effect: novarocks_worker_protocol::control::ControlEffect,
        /// The original guard is in the same batch. Ticket grants have no
        /// host I/O effect of their own, so their body adopts it here.
        admitted: Option<novarocks_control_runtime_core::identity::ReservationId>,
    },
    Reply(ContextRequestSettlement),
    ControlReply(novarocks_worker_protocol::control::ControlRequestSettlement),
    OwnerFailed {
        scope: ScopeId,
        generation: Generation,
    },
}

pub(crate) struct ContextActor {
    core: ContextCore,
    // The only live port is the explicitly allowed synchronous ledger port.
    // Its generation is the runtime failure fence, not the protocol close epoch.
    scope: ScopeHandle,
    runtime_generation: Generation,
    storage: AdmissionStorage,
    timer_work: WorkId,
    lifetime_work: WorkId,
}
impl ContextActor {
    pub(crate) fn new(
        core: ContextCore,
        scope: ScopeHandle,
        storage: AdmissionStorage,
        timer_work: WorkId,
        lifetime_work: WorkId,
    ) -> Self {
        let runtime_generation = scope.gates().expect("live context scope").generation;
        Self {
            core,
            scope,
            runtime_generation,
            storage,
            timer_work,
            lifetime_work,
        }
    }
}
impl Actor for ContextActor {
    type Input = ContextMessage;
    type Effect = WorkerAction;
    fn timer_input(_: Tick) -> Self::Input {
        ContextMessage::Timer
    }
    fn handle(
        &mut self,
        now: Tick,
        envelope: InputEnvelope,
        message: ContextMessage,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        let now = MonotonicInstant::from_origin(Duration::from_nanos(now.0));
        let (work, request, command) = match message {
            ContextMessage::Command {
                work,
                request,
                command,
            } => (work, request, command),
            ContextMessage::Timer => (self.timer_work, None, ContextInput::Tick),
        };
        let mut admission = ScopeAdmissionPort::new(
            self.scope.clone(),
            self.runtime_generation,
            Some(self.core.identity()),
            self.storage.clone(),
        )
        .with_parent(self.lifetime_work);
        let result = match request {
            Some(request) => {
                let ContextInput::Request(command) = command else {
                    panic!("an admitted Context request carries its original command");
                };
                self.core.handle_request(
                    now,
                    RequestContext {
                        token: envelope.input,
                        operation: request.operation,
                        deadline: request.deadline,
                    },
                    command,
                    &mut admission,
                )
            }
            None => self.core.handle(now, command, &mut admission),
        }
        .expect("context protocol input preserves its frozen invariants");
        if matches!(
            self.core.state(),
            QueryContextState::Aborting
                | QueryContextState::Releasing
                | QueryContextState::TerminalRetained
        ) {
            // Commit the start fence with this same pure decision, before a
            // host consumes any stop/release effect. Already started work and
            // conclusion-before-stop publication keep their separate duties.
            step.stop_producing();
        }
        assert!(
            step.charge(result.records_visited),
            "bounded context traversal"
        );
        let guards = admission.take_guards();
        let mut reservations = BTreeMap::new();
        for guard in guards {
            reservations.insert(guard.reservation_id(), guard.work_id());
            if let Err((error, guard)) = step.reservation(guard) {
                // Still pure and unbound. Roll back outside every ledger lock.
                drop(guard);
                panic!("context reservation handoff invariant: {error:?}");
            }
        }
        let mut next = 0u64;
        let mut publications = BTreeMap::<TaskIdentity, ActionId>::new();
        let mut reply_bodies = BTreeMap::<InputId, Vec<ActionId>>::new();
        for effect in result.effects {
            let mut dependencies = Vec::new();
            let mut effect_work = work;
            let kind = match &effect {
                ContextEffect::LoadInput { reservation, .. } => {
                    effect_work = *reservations
                        .get(reservation)
                        .expect("original step reservation accompanies host admission");
                    EffectKind::Business
                }
                ContextEffect::EnqueuePreparation {
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
                ContextEffect::PublishTask { task, .. }
                | ContextEffect::PublishMetrics { task, .. }
                | ContextEffect::PublishTaskConvergence { task, .. }
                | ContextEffect::SummaryConsumed { task, .. }
                | ContextEffect::RetireTask { task, .. }
                | ContextEffect::PublishConclusion { task, .. } => {
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Publication
                }
                ContextEffect::StopTask { task, .. } => {
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Cleanup
                }
                ContextEffect::SettlementBarrier { task, work: role } => {
                    assert_eq!(*role, work, "settlement uses its original admitted Work");
                    if let Some(previous) = publications.get(task) {
                        dependencies.push(*previous);
                    }
                    EffectKind::Publication
                }
                ContextEffect::DisposeSource { .. }
                | ContextEffect::CancelLoader { .. }
                | ContextEffect::ReleaseHost { .. }
                | ContextEffect::SealRoot { .. } => EffectKind::Cleanup,
                // These are neutral control/publication handoffs. Their actual
                // subsystem jobs require their own originally admitted roles.
                _ => EffectKind::Publication,
            };
            next += 1;
            let id = ActionId::new(next).unwrap();
            match &effect {
                ContextEffect::PublishTask { task, .. }
                | ContextEffect::PublishMetrics { task, .. }
                | ContextEffect::PublishTaskConvergence { task, .. }
                | ContextEffect::SummaryConsumed { task, .. }
                | ContextEffect::RetireTask { task, .. }
                | ContextEffect::PublishConclusion { task, .. } => {
                    publications.insert(*task, id);
                }
                _ => {}
            }
            let reply = match &effect {
                ContextEffect::Reply { request, .. }
                | ContextEffect::TaskDomainReply { request, .. }
                | ContextEffect::DomainFailureReply { request, .. }
                | ContextEffect::ContextDomainReply { request, .. }
                | ContextEffect::QuiesceReply { request, .. }
                | ContextEffect::RootDrainReply { request, .. }
                | ContextEffect::ReadPayload { request, .. }
                | ContextEffect::FinalInfoReply { request, .. }
                | ContextEffect::Subscribe { request, .. } => Some(*request),
                _ => None,
            };
            if let Some(token) = reply {
                // A reply body or covered read cannot run ahead of the
                // snapshots this same Core step promised to make visible.
                // Opaque source disposal is deliberately not a predecessor.
                dependencies.extend(publications.values().copied());
                reply_bodies.entry(token).or_default().push(id);
            }
            step.effect(
                EffectDescriptor {
                    id,
                    work: effect_work,
                    kind,
                    dependencies,
                },
                WorkerAction::Context(effect),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded context effect handoff: {error:?}"));
        }
        for reply in result.requests {
            next += 1;
            let dependencies = reply_bodies
                .get(&reply.request.token)
                .cloned()
                .unwrap_or_default();
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work,
                    kind: EffectKind::Reply,
                    dependencies,
                },
                WorkerAction::Reply(reply),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded context reply handoff: {error:?}"));
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.core
            .next_deadline()
            .map(|at| Tick(u64::try_from(at.since_origin().as_nanos()).unwrap_or(u64::MAX)))
    }
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        _: InputEnvelope,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        let work = step
            .failure_work()
            .expect("context registration retains its original cleanup role outside the core");
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work,
                kind: EffectKind::Cleanup,
                dependencies: Vec::new(),
            },
            WorkerAction::OwnerFailed { scope, generation },
        )
        .unwrap_or_else(|(error, _)| panic!("fixed context failure handoff: {error:?}"));
    }
}

#[cfg(test)]
pub(super) mod tests;

mod control;
mod worker;
pub(crate) use control::{ControlActor, ControlMessage};
pub(crate) use worker::{WorkerActor, WorkerMessage};
