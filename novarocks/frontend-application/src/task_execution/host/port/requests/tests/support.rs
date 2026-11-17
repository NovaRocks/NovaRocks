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

//! Two actual actors/scopes on one original ledger. No synthetic effect ticket.
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect, FrontendInput, TestActor},
    host::PreparedEffectJob,
    logical::adapter::{LogicalEffectEnvelope, LogicalHostEffect},
    transport_withdrawal::AttemptEffectEnvelope,
};
use novarocks_attempt_protocol::port_output::{
    AttemptToLogical as ApMessage, AttemptToLogicalBody as ApBody,
};
use novarocks_attempt_protocol::{
    effect::{AttemptEffect, EffectBody, EffectClass},
    failure::AttemptFailure,
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{ActorRegistration, ActorRuntime, EffectTicket, StandingInputObligation},
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, ConsumerId, Generation, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptRequestToken, ExecutionFailure,
    logical_port::{
        AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
    },
};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::{
    num::NonZeroU64,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Wake,
};

pub(crate) const EPOCH: Generation = Generation::new(1).unwrap();
pub(crate) const OBSERVER: ConsumerId = ConsumerId::new(1).unwrap();
pub(crate) fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        71,
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap(),
        12,
    )
}
pub(crate) fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
pub(crate) fn action(n: u64) -> AttemptActionId {
    AttemptActionId::new(NonZeroU64::new(n).unwrap())
}
pub(crate) fn request(n: u64) -> AttemptToLogical {
    AttemptToLogical {
        activation: activation(),
        action: action(n),
        body: AttemptToLogicalBody::PreReadyReplacementQuery {
            token: token(n),
            refusal: ExecutionFailure::from_input(
                novarocks_attempt_protocol::port_projection::owner_failure(),
            ),
        },
    }
}
pub(crate) fn reply(n: u64, may_observe: bool) -> LogicalToAttempt {
    LogicalToAttempt {
        activation: activation(),
        body: LogicalToAttemptBody::PreReadyReplacementReply {
            token: token(n),
            may_observe,
        },
    }
}
pub(crate) fn ap_effect(n: u64) -> FrontendEffect {
    FrontendEffect::Attempt(AttemptEffectEnvelope::new(AttemptEffect {
        id: action(n),
        class: EffectClass::Settlement,
        body: EffectBody::Port(ApMessage {
            activation: activation(),
            action: action(n),
            body: ApBody::PreReadyReplacementQuery {
                token: token(n),
                refusal: AttemptFailure::OwnerFailed,
            },
        }),
    }))
}
pub(crate) fn logical_effect(
    n: u64,
    reply: LogicalToAttempt,
    payer: &ReservationObligation,
) -> FrontendEffect {
    FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
        body: LogicalHostEffect::Protocol(Box::new(
            novarocks_logical_execution_protocol::effect::LogicalEffect {
                action: novarocks_logical_execution_protocol::LogicalActionId::new(
                    NonZeroU64::new(n).unwrap(),
                ),
                body: novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(
                    Box::new(reply),
                ),
            },
        )),
        reservation: payer.id(),
    }))
}
#[derive(Default)]
pub(crate) struct Probe(pub(crate) AtomicUsize);
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Actor {
    next: Arc<Mutex<Option<FrontendEffect>>>,
    seen: Arc<Mutex<Vec<FrontendInput>>>,
}
impl TestActor for Actor {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        assert!(
            !matches!(input, FrontendInput::LogicalInfrastructureFailed),
            "controlled original logical owner failure"
        );
        if matches!(input, FrontendInput::Test(_)) {
            let effect = self
                .next
                .lock()
                .unwrap()
                .take()
                .expect("configured original effect");
            let kind = match &effect {
                FrontendEffect::Attempt(effect) => match effect.protocol.class {
                    EffectClass::Business => EffectKind::Business,
                    EffectClass::Publication => EffectKind::Publication,
                    EffectClass::Reply => EffectKind::Reply,
                    EffectClass::Cleanup => EffectKind::Cleanup,
                    EffectClass::Settlement => EffectKind::Settlement,
                },
                FrontendEffect::Logical(effect) => match effect.body.work_kind() {
                    novarocks_logical_execution_protocol::effect::LogicalWorkKind::Business => {
                        EffectKind::Business
                    }
                    novarocks_logical_execution_protocol::effect::LogicalWorkKind::Settlement => {
                        EffectKind::Settlement
                    }
                    novarocks_logical_execution_protocol::effect::LogicalWorkKind::Cleanup => {
                        EffectKind::Cleanup
                    }
                },
                _ => EffectKind::Business,
            };
            let action_id = match &effect {
                FrontendEffect::Attempt(effect) => effect.protocol.id.get(),
                FrontendEffect::Logical(effect) => match &effect.body {
                    LogicalHostEffect::Protocol(effect) => effect.action.get(),
                    _ => 1,
                },
                _ => 1,
            };
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(action_id).unwrap(),
                    work: step.input_work().expect("same original paid source"),
                    kind,
                    dependencies: Vec::new(),
                },
                effect,
            )
            .unwrap();
        } else {
            self.seen.lock().unwrap().push(input);
        }
    }
}
pub(crate) struct Side {
    pub(crate) scope: ScopeHandle,
    pub(crate) work: WorkObligation,
    pub(crate) payer: ReservationObligation,
    pub(crate) registration: ActorRegistration,
    pub(crate) seen: Arc<Mutex<Vec<FrontendInput>>>,
    next: Arc<Mutex<Option<FrontendEffect>>>,
    triggers: Vec<StandingInputObligation<FrontendActor>>,
}
pub(crate) struct Fixture {
    pub(crate) ledger: RuntimeLedger,
    pub(crate) runtime: ActorRuntime<FrontendActor>,
    pub(crate) logical: Side,
    pub(crate) attempt: Side,
}
impl Fixture {
    pub(crate) fn new() -> Self {
        Self::build(false).0
    }
    pub(crate) fn with_attempt_failure_work() -> (Self, ReportedWork) {
        let (fixture, work) = Self::build(true);
        (
            fixture,
            work.expect("same original supervised attempt Work"),
        )
    }
    fn build(supervised: bool) -> (Self, Option<ReportedWork>) {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let runtime = ActorRuntime::new();
        let mut failure = None;
        let mut make = |id| {
            let scope = ledger.open_scope_next(None, EPOCH).unwrap();
            let (mut work, payer) = scope
                .reserve_next(
                    EPOCH,
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            inputs: 32,
                            batches: 32,
                            effects: 32,
                            ..Default::default()
                        },
                    ),
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Business)
                .unwrap();
            work.start().unwrap();
            let next = Arc::new(Mutex::new(None));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let actor = FrontendActor::Test(Box::new(Actor {
                next: Arc::clone(&next),
                seen: Arc::clone(&seen),
            }));
            let config = ActorConfig {
                mailbox: [32; 4],
                max_batches: 32,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 16,
                max_step_work: 2,
            };
            if supervised && id == 11 {
                let (cleanup, mut positions) = scope
                    .reserve_child_next(
                        EPOCH,
                        payer.id(),
                        ReservationResources::new(
                            ResourceVector::default(),
                            OwnedSlotNeed {
                                work_headers: 1,
                                completions: 1,
                                captures: 1,
                                ..Default::default()
                            },
                        ),
                        WorkKind::Cleanup,
                    )
                    .unwrap()
                    .bind(WorkKind::Cleanup)
                    .unwrap();
                let report = ReportSlot::reserve_next(
                    &mut positions,
                    EPOCH,
                    1,
                    [(OBSERVER, ConsumerKind::Observer)],
                )
                .unwrap();
                failure = Some(ReportedWork::try_new(cleanup, positions, report).unwrap());
            }
            let registration = if supervised && id == 11 {
                runtime.register_supervised_bound_with_source_cut(
                    ActorId::new(id).unwrap(),
                    scope.clone(),
                    actor,
                    config,
                    failure.as_ref().unwrap().work(),
                    ActorId::new(1).unwrap(),
                )
            } else {
                runtime.register_bound(ActorId::new(id).unwrap(), scope.clone(), actor, config)
            }
            .unwrap_or_else(|(error, _)| panic!("original paid test actor: {error:?}"));
            Side {
                scope,
                work,
                payer,
                registration,
                seen,
                next,
                triggers: Vec::new(),
            }
        };
        let logical = make(1);
        let attempt = make(11);
        (
            Self {
                ledger,
                runtime,
                logical,
                attempt,
            },
            failure,
        )
    }
    pub(crate) fn emit(
        &mut self,
        from_logical: bool,
        effect: FrontendEffect,
    ) -> EffectTicket<FrontendEffect> {
        let side = if from_logical {
            &mut self.logical
        } else {
            &mut self.attempt
        };
        assert!(side.next.lock().unwrap().replace(effect).is_none());
        let position = self
            .runtime
            .reserve_admitted_input_for(
                &side.registration,
                side.registration.actor(),
                InputClass::Lifecycle,
                &side.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original paid trigger"));
        let input = position
            .fill(FrontendInput::Test(1))
            .unwrap_or_else(|(error, _)| panic!("original trigger fill: {error:?}"));
        side.triggers.push(position);
        for _ in 0..16 {
            self.runtime.poll_one(Tick(1)).unwrap();
            if self
                .runtime
                .effect_inventory(side.registration.actor(), input, None, 1)
                .is_ok()
            {
                break;
            }
        }
        let action = self
            .runtime
            .effect_inventory(side.registration.actor(), input, None, 1)
            .unwrap()[0]
            .0
            .id;
        self.runtime
            .take_effect(side.registration.actor(), input, action)
            .unwrap()
    }
    pub(crate) fn consume(&self) {
        for _ in 0..16 {
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                break;
            }
        }
    }
    pub(crate) fn finish(mut self) {
        self.consume();
        for side in [&mut self.logical, &mut self.attempt] {
            for position in &mut side.triggers {
                assert!(position.retire_ready_after_exit().unwrap());
                position.close_after_exit().unwrap();
            }
            side.work.body_exited().unwrap();
            side.work.captures_exited().unwrap();
            side.payer.finish().unwrap();
            assert!(
                side.scope
                    .reap_observed_header(&side.work.retirement_observer())
                    .unwrap()
            );
            side.scope.close_external().unwrap();
            while let Some(turn) = self.runtime.claim_host_actor() {
                turn.release(false).unwrap();
            }
            self.runtime
                .remove_actor(side.registration.actor())
                .unwrap();
            self.ledger.remove_scope(side.scope.id()).unwrap();
        }
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
    pub(crate) fn job(
        &self,
        n: u64,
        captures: Box<dyn std::any::Any + Send>,
    ) -> (
        PreparedEffectJob,
        ExitCustody,
        ReportSlot,
        WorkRetirementObserver,
    ) {
        let guard = self
            .attempt
            .scope
            .reserve_child_next(
                EPOCH,
                self.attempt.payer.id(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
            )
            .unwrap();
        let (work, mut payer) = guard.bind(WorkKind::Settlement).unwrap();
        let observer = work.retirement_observer();
        let report =
            ReportSlot::reserve_next(&mut payer, EPOCH, 1, [(OBSERVER, ConsumerKind::Observer)])
                .unwrap();
        let reported = ReportedWork::try_new(work, payer, report.clone()).unwrap();
        let (custody, exit) = ExitCustody::from_admitted(reported);
        let result =
            novarocks_control_runtime::causal_input::CausalInputObligation::reserve_admitted(
                &self.runtime,
                &self.attempt.registration,
                self.logical.registration.actor(),
                InputClass::Lifecycle,
                &self.attempt.work,
            )
            .unwrap();
        (
            PreparedEffectJob {
                effect: ap_effect(n),
                exit,
                result,
                captures,
                transport: None,
            },
            custody,
            report,
            observer,
        )
    }
}
