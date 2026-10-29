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

//! Low-frequency directory and ticket decisions share the same step/guard
//! handoff and scheduler as contexts. There is no task-capacity mailbox hop.
use super::{RequestMetadata, WorkerAction};
use crate::control_admission::{AdmissionStorage, ScopeAdmissionPort};
use novarocks_control_runtime::{
    actor::{Actor, StepContext},
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, Generation, ScopeId, Tick, WorkId},
    mailbox::InputEnvelope,
};
use novarocks_execution_contract::TaskOperationId;
use novarocks_worker_protocol::{
    control::{ControlCore, ControlEffect, ControlInput},
    creation::RequestContext,
    lease::MonotonicInstant,
};
use std::{collections::BTreeMap, time::Duration};

#[expect(
    clippy::large_enum_variant,
    reason = "Each paid input position stores one neutral command inline, including the payload-free timer alternative."
)]
#[derive(Debug)]
pub(crate) enum ControlMessage {
    ObserveTicket {
        work: WorkId,
        request: RequestMetadata,
        ticket: novarocks_execution_contract::AdmissionTicketId,
        context: novarocks_execution_contract::QueryContextRef,
        preflight: bool,
    },
    Command {
        work: WorkId,
        request: Option<RequestMetadata>,
        command: ControlInput,
    },
    Timer,
}

pub(crate) struct ControlActor {
    core: ControlCore,
    scope: ScopeHandle,
    runtime_generation: Generation,
    storage: AdmissionStorage,
    timer_work: WorkId,
    lifetime_work: WorkId,
}
impl ControlActor {
    pub(crate) const MAX_STEP_ACTIONS: usize = ControlCore::MAX_STEP_WORK + 2;
    pub(crate) const MAX_STEP_WORK: usize = ControlCore::MAX_STEP_WORK + 2;
    pub(crate) const TURN_BUDGET: usize = 4 * (Self::MAX_STEP_WORK + 1);
    pub(crate) fn new(
        core: ControlCore,
        scope: ScopeHandle,
        storage: AdmissionStorage,
        timer_work: WorkId,
        lifetime_work: WorkId,
    ) -> Self {
        let runtime_generation = scope
            .gates()
            .expect("live process control scope")
            .generation;
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
impl Actor for ControlActor {
    type Input = ControlMessage;
    type Effect = WorkerAction;
    fn timer_input(_: Tick) -> ControlMessage {
        ControlMessage::Timer
    }
    fn handle(
        &mut self,
        now: Tick,
        envelope: InputEnvelope,
        message: ControlMessage,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        let now = MonotonicInstant::from_origin(Duration::from_nanos(now.0));
        let (work, request, command) = match message {
            ControlMessage::Command {
                work,
                request,
                command,
            } => (work, request, command),
            ControlMessage::Timer => (self.timer_work, None, ControlInput::Tick),
            ControlMessage::ObserveTicket {
                work,
                request,
                ticket,
                context,
                preflight,
            } => {
                use novarocks_worker_protocol::{
                    context::{RequestResult, RequestSettlement},
                    control::{ControlReplyBody, ControlRequestSettlement},
                };
                let (result, body) = if now.has_reached(request.deadline) {
                    let result = RequestResult::Outcome(
                        novarocks_execution_contract::OperationOutcome::OperationTimedOut,
                    );
                    (result, ControlReplyBody::Verdict(result))
                } else {
                    (
                        RequestResult::Outcome(
                            novarocks_execution_contract::OperationOutcome::Accepted,
                        ),
                        if preflight {
                            ControlReplyBody::PreflightExpired(
                                self.core.preflight_expired_establish(context, ticket, now),
                            )
                        } else {
                            ControlReplyBody::TicketObservation(
                                self.core.admission().observe(ticket, context, now),
                            )
                        },
                    )
                };
                assert!(step.charge(1), "one original ticket observation");
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).expect("original read reply action"),
                        work,
                        kind: EffectKind::Reply,
                        dependencies: Vec::new(),
                    },
                    WorkerAction::ControlReply(ControlRequestSettlement {
                        settlement: RequestSettlement {
                            request: RequestContext {
                                token: envelope.input,
                                operation: request.operation,
                                deadline: request.deadline,
                            },
                            result,
                        },
                        body,
                    }),
                )
                .unwrap_or_else(|(error, _)| {
                    panic!("bounded original ticket observation reply: {error:?}")
                });
                return;
            }
        };
        let acquiring = matches!(&command, ControlInput::Acquire { .. });
        let mut admission = ScopeAdmissionPort::new(
            self.scope.clone(),
            self.runtime_generation,
            None,
            self.storage.clone(),
        )
        .with_parent(self.lifetime_work);
        let previous_epoch = self.core.admission().capability();
        let result = match request {
            Some(request) => self.core.handle_request(
                now,
                RequestContext {
                    token: envelope.input,
                    operation: request.operation,
                    deadline: request.deadline,
                },
                command,
                &mut admission,
            ),
            None => self.core.handle(now, command, &mut admission),
        }
        .expect("process control input preserves its frozen invariants");
        let internal_ready = self.core.has_internal_work();
        assert!(
            step.charge(result.records_visited),
            "bounded process control traversal"
        );
        let mut reservations = BTreeMap::new();
        for guard in admission.take_guards() {
            reservations.insert(guard.reservation_id(), guard.work_id());
            if let Err((error, guard)) = step.reservation(guard) {
                drop(guard);
                panic!("process reservation handoff invariant: {error:?}");
            }
        }
        assert!(
            reservations.len() <= 1,
            "one control command can admit only its original ticket or context"
        );
        let ticket_admission = acquiring
            .then(|| reservations.keys().next().copied())
            .flatten();
        let mut adopted_ticket = false;
        let mut next = 0u64;
        let epoch = self.core.admission().capability();
        let epoch_publication = if epoch != previous_epoch {
            next += 1;
            let id = ActionId::new(next).expect("first epoch projection action");
            step.effect(
                EffectDescriptor {
                    id,
                    work,
                    kind: EffectKind::Publication,
                    dependencies: Vec::new(),
                },
                WorkerAction::AdmissionEpoch(epoch),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded admission epoch projection: {error:?}"));
            Some(id)
        } else {
            None
        };
        let mut reply_bodies = BTreeMap::<TaskOperationId, ActionId>::new();
        let mut released_admission = Vec::new();
        let mut route_publications = Vec::new();
        for effect in result.effects {
            // These are pure continuation hints, not lifecycle facts. The
            // same Core retains their cursors and chooses NextInternal. Host
            // custody is one separately prepaid coalesced wake cell.
            if matches!(
                &effect,
                ControlEffect::ContinueReplies { .. }
                    | ControlEffect::ContinueRequestReplies { .. }
                    | ControlEffect::ContinueMaintenance
                    | ControlEffect::ContinueFence
            ) {
                continue;
            }
            let mut dependencies: Vec<_> = epoch_publication.into_iter().collect();
            let admitted = match &effect {
                ControlEffect::Materialize { reservation, .. } => Some(*reservation),
                ControlEffect::AdmissionReply { result: Ok(_), .. } => {
                    adopted_ticket = ticket_admission.is_some();
                    ticket_admission
                }
                _ => None,
            };
            let kind = if admitted.is_some() {
                EffectKind::Business
            } else {
                match &effect {
                    ControlEffect::RetireReservation(_)
                    | ControlEffect::RemoveHost { .. }
                    | ControlEffect::AbortContext { .. } => EffectKind::Cleanup,
                    _ => EffectKind::Publication,
                }
            };
            let effect_work = admitted.map_or(work, |id| {
                *reservations
                    .get(&id)
                    .expect("original control guard accompanies its host effect")
            });
            // A TerminalRetained route follows actual active-ticket release.
            // Its separately funded metadata survives in the directory.
            if matches!(
                &effect,
                ControlEffect::Route {
                    state: novarocks_execution_contract::QueryContextState::TerminalRetained,
                    ..
                }
            ) {
                dependencies.clone_from(&released_admission);
            }
            next += 1;
            let id = ActionId::new(next).unwrap();
            match &effect {
                ControlEffect::AdmissionReply { request, .. }
                | ControlEffect::Reply { request, .. }
                | ControlEffect::DomainFailureReply { request, .. } => {
                    // Establish must not acknowledge Active before the
                    // corresponding actual route snapshot becomes visible.
                    dependencies.extend(route_publications.iter().copied());
                    reply_bodies.insert(*request, id);
                }
                ControlEffect::Route { .. } => route_publications.push(id),
                ControlEffect::RetireReservation(_) => released_admission.push(id),
                _ => {}
            }
            step.effect(
                EffectDescriptor {
                    id,
                    work: effect_work,
                    kind,
                    dependencies,
                },
                WorkerAction::Control { effect, admitted },
            )
            .unwrap_or_else(|(error, _)| {
                panic!("bounded process control effect handoff: {error:?}")
            });
        }
        if internal_ready {
            next += 1;
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work,
                    kind: EffectKind::Publication,
                    dependencies: Vec::new(),
                },
                WorkerAction::Control {
                    effect: ControlEffect::WakeInternal,
                    admitted: None,
                },
            )
            .unwrap_or_else(|(error, _)| panic!("bounded internal wake handoff: {error:?}"));
        }
        assert!(
            ticket_admission.is_none() || adopted_ticket,
            "every newly issued ticket transfers its actual guard in the grant body"
        );
        for reply in result.requests {
            next += 1;
            let dependencies = reply_bodies
                .get(&reply.request.operation)
                .copied()
                .into_iter()
                .collect();
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work,
                    kind: EffectKind::Reply,
                    dependencies,
                },
                WorkerAction::ControlReply(reply),
            )
            .unwrap_or_else(|(error, _)| {
                panic!("bounded process control reply handoff: {error:?}")
            });
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
        input: InputEnvelope,
        step: &mut StepContext<'_, WorkerAction>,
    ) {
        <super::ContextActor as Actor>::owner_failed(scope, generation, input, step);
    }
}
