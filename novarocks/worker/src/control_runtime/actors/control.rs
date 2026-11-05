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
//! handoff and scheduler as participations. There is no task-capacity mailbox hop.
use super::{RequestMetadata, WorkerAction};
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
use novarocks_execution_contract::TaskOperationId;
use novarocks_worker_protocol::{
    control::{ControlCore, ControlEffect, ControlInput},
    creation::RequestReplyTag,
    lease::MonotonicInstant,
};
use std::{collections::BTreeMap, time::Duration};

#[derive(Debug)]
pub(crate) enum ControlMessage {
    ObserveTicket {
        work: ScopedWorkRef,
        request: RequestMetadata,
        ticket: novarocks_execution_contract::AdmissionTicketId,
        participation: novarocks_execution_contract::ParticipationRef,
        preflight: bool,
    },
    Command {
        work: ScopedWorkRef,
        request: Option<RequestMetadata>,
        command: ControlInput,
    },
    /// A payload forwarded through the runtime's original metadata ticket.
    Metadata(ControlInput),
    Timer,
}

pub(crate) struct ControlActor {
    core: ControlCore,
    scope: ScopeHandle,
    runtime_generation: Generation,
    storage: AdmissionStorage,
    timer_work: ScopedWorkRef,
    lifetime_work: ScopedWorkRef,
    request_authors: BTreeMap<InputId, InputAuthor>,
}
impl ControlActor {
    pub(crate) const MAX_STEP_ACTIONS: usize = ControlCore::MAX_STEP_WORK + 2;
    pub(crate) const MAX_STEP_WORK: usize = ControlCore::MAX_STEP_WORK + 2;
    #[cfg(test)]
    pub(crate) const TURN_BUDGET: usize = 4 * (Self::MAX_STEP_WORK + 1);
    pub(crate) fn new(
        core: ControlCore,
        scope: ScopeHandle,
        storage: AdmissionStorage,
        timer_work: ScopedWorkRef,
        lifetime_work: ScopedWorkRef,
    ) -> Self {
        assert_eq!(timer_work.scope, scope.id(), "original Root timer scope");
        assert_eq!(
            lifetime_work.scope,
            scope.id(),
            "original Root lifetime scope"
        );
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
            request_authors: BTreeMap::new(),
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
        step: &mut ActorStep<'_, WorkerAction>,
    ) {
        let now = MonotonicInstant::from_origin(Duration::from_nanos(now.0));
        assert_eq!(
            matches!(&message, ControlMessage::Metadata(_)),
            step.is_runtime_metadata(),
            "only an original runtime metadata delivery carries a metadata command"
        );
        let (work, request, command) = match message {
            ControlMessage::Command {
                work,
                request,
                command,
            } => (work, request, command),
            ControlMessage::Timer => (self.timer_work, None, ControlInput::Tick),
            ControlMessage::Metadata(command) => (self.timer_work, None, command),
            ControlMessage::ObserveTicket {
                work,
                request,
                ticket,
                participation,
                preflight,
            } => {
                use novarocks_worker_protocol::{
                    control::{ControlReplyBody, ControlRequestSettlement},
                    participation::{RequestResult, RequestSettlement},
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
                                self.core
                                    .preflight_expired_establish(participation, ticket, now),
                            )
                        } else {
                            ControlReplyBody::TicketObservation(self.core.admission().observe(
                                ticket,
                                participation,
                                now,
                            ))
                        },
                    )
                };
                assert!(step.charge(1), "one original ticket observation");
                let proof = step
                    .input_author()
                    .cloned()
                    .expect("the original ticket read R author");
                assert_eq!(proof.work(), work);
                step.effect_from_author(
                    &proof,
                    EffectDescriptor {
                        id: ActionId::new(1).expect("original read reply action"),
                        work,
                        kind: EffectKind::Reply,
                        dependencies: Vec::new(),
                    },
                    WorkerAction::ControlReply(ControlRequestSettlement {
                        settlement: RequestSettlement {
                            request: RequestReplyTag {
                                author: work,
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
        let mut admission = ScopeAdmissionPort::new(
            self.scope.clone(),
            self.runtime_generation,
            None,
            self.storage.clone(),
        )
        .with_parent(self.lifetime_work.work);
        if let Some(proof) = step.input_author() {
            assert!(
                proof.work() == work
                    || (request.is_none() && !step.is_retained() && work.scope == self.scope.id()),
                "a request keeps its exact author; internal duty stays in the live execution scope"
            );
        }
        if request.is_some() {
            let proof = step
                .input_author()
                .cloned()
                .expect("the original Root R author");
            assert!(
                self.request_authors.insert(envelope.input, proof).is_none(),
                "one original Root R proof"
            );
        }
        let previous_epoch = self.core.admission().capability();
        let previous_live_grants = self.core.admission().logical_live_grants();
        let previous_admission_retention = self.core.admission().retention_inventory();
        let previous_control_retention = self.core.retention_inventory();
        let result = match request {
            Some(request) => self.core.handle_request(
                now,
                RequestReplyTag {
                    author: work,
                    token: envelope.input,
                    operation: request.operation,
                    deadline: request.deadline,
                },
                command,
                &mut admission,
            ),
            None => self.core.handle(now, command, &mut admission),
        }
        .unwrap_or_else(|error| {
            self.request_authors.clear();
            panic!("process control input preserves its frozen invariants: {error:?}")
        });
        let internal_ready = self.core.has_internal_work();
        assert!(
            step.charge(result.records_visited),
            "bounded process control traversal"
        );
        let mut reservations = BTreeMap::new();
        for guard in admission.take_guards() {
            reservations.insert(
                guard.reservation_id(),
                ScopedWorkRef {
                    scope: guard.scope().id(),
                    work: guard.work_id(),
                },
            );
            if let Err((error, guard)) = step.reservation(guard) {
                drop(guard);
                panic!("process reservation handoff invariant: {error:?}");
            }
        }
        assert!(
            reservations.len() <= 1,
            "one control command can admit only its original participation materialization"
        );
        let mut next = 0u64;
        let epoch = self.core.admission().capability();
        let logical_live_grants = self.core.admission().logical_live_grants();
        let admission_retention = self.core.admission().retention_inventory();
        let control_retention = self.core.retention_inventory();
        let epoch_publication = if epoch != previous_epoch
            || logical_live_grants != previous_live_grants
            || admission_retention != previous_admission_retention
            || control_retention != previous_control_retention
        {
            next += 1;
            let id = ActionId::new(next).expect("first epoch projection action");
            step.effect(
                EffectDescriptor {
                    id,
                    work,
                    kind: EffectKind::Publication,
                    dependencies: Vec::new(),
                },
                WorkerAction::AdmissionEpoch(
                    super::super::node_metrics::ControlAdmissionProjection {
                        epoch,
                        logical_live_grants,
                        admission_retention,
                        control_retention,
                    },
                ),
            )
            .unwrap_or_else(|(error, _)| panic!("bounded admission epoch projection: {error:?}"));
            Some(id)
        } else {
            None
        };
        let mut reply_bodies = BTreeMap::<TaskOperationId, ActionId>::new();
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
                _ => None,
            };
            let kind = if admitted.is_some() {
                EffectKind::Business
            } else {
                match &effect {
                    ControlEffect::RetireReservation(_)
                    | ControlEffect::RemoveHost { .. }
                    | ControlEffect::AbortParticipation { .. } => EffectKind::Cleanup,
                    _ => EffectKind::Publication,
                }
            };
            let effect_work = admitted.map_or(work, |id| {
                *reservations
                    .get(&id)
                    .expect("original control guard accompanies its host effect")
            });
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
        for reply in result.requests {
            next += 1;
            let dependencies = reply_bodies
                .get(&reply.request.operation)
                .copied()
                .into_iter()
                .collect();
            let proof = self
                .request_authors
                .remove(&reply.request.token)
                .expect("the pure Root reply returns its original author");
            assert_eq!(
                proof.work(),
                reply.request.author,
                "same pure Root request author"
            );
            step.effect_from_author(
                &proof,
                EffectDescriptor {
                    id: ActionId::new(next).unwrap(),
                    work: reply.request.author,
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
        step: &mut ActorStep<'_, WorkerAction>,
    ) {
        <super::ParticipationActor as Actor>::owner_failed(scope, generation, input, step);
    }
}
