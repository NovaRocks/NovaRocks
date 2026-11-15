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

//! Real LP/AP registrations, original published cell, paid roles and full exit.
use super::*;
use crate::control_runtime::{actor::TestActor, logical::adapter::LogicalEffectEnvelope};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    obligation::{ReservationObligation, WorkObligation},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    effect::{EffectDescriptor, EffectKind, EffectState},
    identity::{ActionId, ActorId, Generation, InputId, ReservationId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::ExecutionFailureSite;
use novarocks_coordination_contract::{
    AttemptActionId, IdSeed, OperationIds,
    logical_port::{AttemptToLogical, AttemptToLogicalBody},
};
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_execution_contract::{SafeDetail, TaskIdentity};
use novarocks_logical_execution_protocol::{
    LogicalActionId,
    effect::{LogicalEffect, LogicalEffectBody},
};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::{num::NonZeroU64, time::Duration};

struct Emitter {
    work: ScopedWorkRef,
    next: Arc<Mutex<Option<FrontendEffect>>>,
}
impl TestActor for Emitter {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        _: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work: self.work,
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            self.next
                .lock()
                .unwrap()
                .take()
                .expect("original LP command"),
        )
        .unwrap();
    }
}
type Recorded = Arc<
    Mutex<
        Vec<(
            InputId,
            novarocks_coordination_contract::LogicalToAttempt<
                novarocks_attempt_protocol::failure::AttemptFailure,
            >,
        )>,
    >,
>;
struct Receiver(Recorded);
impl TestActor for Receiver {
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        value: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        let FrontendInput::Attempt(value) = value else {
            panic!("only recipient control commands");
        };
        let AttemptInput::Port(value) = *value else {
            panic!("original port command");
        };
        self.0.lock().unwrap().push((envelope.input, value));
    }
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [8; 4],
        max_batches: 32,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 16,
        max_step_work: 2,
    }
}
fn root_work(scope: &ScopeHandle) -> (WorkObligation, ReservationObligation) {
    let (mut work, payer) = scope
        .reserve_next(
            Generation::new(1).unwrap(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    inputs: 8,
                    work_headers: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    work.start().unwrap();
    (work, payer)
}
pub(crate) fn token(n: u64) -> AttemptRequestToken {
    AttemptRequestToken::new(NonZeroU64::new(n).unwrap())
}
pub(crate) fn at(n: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(n))
}
pub(crate) struct Fixture {
    pub(crate) runtime: ActorRuntime<FrontendActor>,
    pub(crate) lp: ActorRegistration,
    pub(crate) ap: ActorRegistration,
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) root: TaskIdentity,
    pub(crate) roots: Arc<RootControlRequests>,
    pub(crate) slot: SourcePublishedSlot,
    pub(crate) reader: ControlReaderObligation,
    ledger: RuntimeLedger,
    pub(crate) lp_scope: ScopeHandle,
    ap_scope: ScopeHandle,
    lp_work: WorkObligation,
    lp_payer: ReservationObligation,
    ap_work: WorkObligation,
    ap_payer: ReservationObligation,
    trigger: StandingInputObligation<FrontendActor>,
    next: Arc<Mutex<Option<FrontendEffect>>>,
    seen: Recorded,
}
impl Fixture {
    pub(crate) fn new() -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let epoch = Generation::new(1).unwrap();
        let lp_scope = ledger.open_scope_next(None, epoch).unwrap();
        let ap_scope = ledger.open_scope_next(None, epoch).unwrap();
        let (lp_work, lp_payer) = root_work(&lp_scope);
        let (ap_work, ap_payer) = root_work(&ap_scope);
        let runtime = ActorRuntime::new();
        let next = Arc::new(Mutex::new(None));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let lp = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                lp_scope.clone(),
                FrontendActor::Test(Box::new(Emitter {
                    work: lp_work.reference(),
                    next: next.clone(),
                })),
                config(),
            )
            .unwrap_or_else(|_| panic!("actual LP"));
        let ap = runtime
            .register_bound(
                ActorId::new(11).unwrap(),
                ap_scope.clone(),
                FrontendActor::Test(Box::new(Receiver(seen.clone()))),
                config(),
            )
            .unwrap_or_else(|_| panic!("actual AP"));
        let reserve = |target: &ActorRegistration, source, work: &WorkObligation, class| {
            runtime
                .reserve_admitted_input_for(target, source, class, work)
                .unwrap()
                .into_standing_input()
                .unwrap_or_else(|_| panic!("actual paid standing role"))
        };
        let trigger = reserve(
            &lp,
            ActorId::new(901).unwrap(),
            &lp_work,
            InputClass::Control,
        );
        let positions = ControlPositions {
            stop: reserve(&ap, lp.actor(), &ap_work, InputClass::Control),
            convergence: reserve(&ap, lp.actor(), &ap_work, InputClass::Control),
            changed: reserve(&ap, lp.actor(), &ap_work, InputClass::Ordinary),
        };
        let guard = lp_scope
            .reserve_child_next(
                epoch,
                lp_payer.id(),
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
        let observer = ConsumerId::new(1).unwrap();
        let report =
            ReportSlot::reserve_next(&mut payer, epoch, 1, [(observer, ConsumerKind::Observer)])
                .unwrap();
        let storage = ReportedWork::try_new(work, payer, report).unwrap();
        let activation = AttemptActivationIdentity::from_parts(
            71,
            QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(1).unwrap()).unwrap(),
            12,
        );
        let mut ids = OperationIds::new(IdSeed::new(7, 1));
        let root = TaskIdentity::new(
            activation.execution(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::try_from_bytes(ids.issue().to_bytes()).unwrap(),
        );
        let roots = RootControlRequests::new(
            super::super::root_control::RootControlBinding::for_registered_source(
                ap.clone(),
                activation,
                root,
            )
            .unwrap(),
        );
        let (slot, reader) = SourcePublishedSlot::bind(
            &runtime,
            ControlBinding {
                lp: lp.clone(),
                ap: ap.clone(),
                activation,
                positions,
                storage,
                observer,
                roots: roots.clone(),
            },
        )
        .unwrap_or_else(|e| panic!("paid original binding: {e:?}"));
        Self {
            runtime,
            lp,
            ap,
            activation,
            root,
            roots,
            slot,
            reader,
            ledger,
            lp_scope,
            ap_scope,
            lp_work,
            lp_payer,
            ap_work,
            ap_payer,
            trigger,
            next,
            seen,
        }
    }
    pub(crate) fn message(&self, body: LogicalToAttemptBody) -> LogicalToAttempt {
        LogicalToAttempt {
            activation: self.activation,
            body,
        }
    }
    pub(crate) fn ticket(&mut self, message: &LogicalToAttempt) -> EffectTicket<FrontendEffect> {
        assert!(self.trigger.retire_ready_after_exit().unwrap());
        *self.next.lock().unwrap() =
            Some(FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
                body: LogicalHostEffect::Protocol(Box::new(LogicalEffect {
                    action: LogicalActionId::new(NonZeroU64::new(1).unwrap()),
                    body: LogicalEffectBody::AttemptPort(Box::new(message.clone())),
                })),
                reservation: ReservationId::new(1).unwrap(),
            })));
        let input = self
            .trigger
            .fill(FrontendInput::LogicalHostSettled)
            .unwrap_or_else(|(e, _)| panic!("original LP input: {e:?}"));
        for _ in 0..16 {
            if self
                .runtime
                .effect_inventory(self.lp.actor(), input, None, 1)
                .is_ok_and(|rows| !rows.is_empty())
            {
                break;
            }
            assert!(self.runtime.poll_one(Tick(1)).unwrap());
        }
        self.runtime
            .take_effect(self.lp.actor(), input, ActionId::new(1).unwrap())
            .unwrap()
    }
    pub(crate) fn complete(&self, ticket: EffectTicket<FrontendEffect>) {
        self.runtime.effect_completed(ticket).unwrap();
        assert!(self.trigger.retire_ready_after_exit().unwrap());
    }
    pub(crate) fn publish(&mut self, body: LogicalToAttemptBody) -> CommittedControlReceipt {
        let message = self.message(body);
        let ticket = self.ticket(&message);
        let receipt = self.slot.commit(&self.runtime, &ticket, &message).unwrap();
        assert_eq!(
            self.runtime
                .effect_inventory(self.lp.actor(), ticket.input, None, 1)
                .unwrap()[0]
                .1,
            EffectState::Published
        );
        self.complete(ticket);
        receipt
    }
    pub(crate) fn pass(&mut self, budget: usize) -> Vec<RootControlRef> {
        self.reader.begin_pass();
        let mut queued = Vec::new();
        for _ in 0..16 {
            let progress = self
                .reader
                .progress(budget, |r| {
                    queued.push(r);
                    Ok(())
                })
                .unwrap();
            assert!(progress.units <= budget);
            if !progress.more {
                return queued;
            }
        }
        panic!("finite control pass must not restart itself");
    }
    pub(crate) fn consume(
        &mut self,
    ) -> Vec<
        novarocks_coordination_contract::LogicalToAttempt<
            novarocks_attempt_protocol::failure::AttemptFailure,
        >,
    > {
        for _ in 0..16 {
            if !self.runtime.poll_one(Tick(2)).unwrap() {
                break;
            }
        }
        let seen = std::mem::take(&mut *self.seen.lock().unwrap());
        let inputs: Vec<_> = seen.iter().map(|(id, _)| *id).collect();
        self.reader.index_committed(&inputs).unwrap();
        seen.into_iter().map(|(_, value)| value).collect()
    }
    pub(crate) fn reply(
        &self,
        token: AttemptRequestToken,
        kind: RootControlKind,
    ) -> AttemptToLogical {
        AttemptToLogical {
            activation: self.activation,
            action: AttemptActionId::new(NonZeroU64::new(1).unwrap()),
            body: match kind {
                RootControlKind::Seal => AttemptToLogicalBody::SealResult {
                    token,
                    result: Ok(Arc::new([])),
                },
                RootControlKind::Terminal => AttemptToLogicalBody::TerminalControlResult {
                    token,
                    result: Ok(()),
                },
            },
        }
    }
    pub(crate) fn finish(mut self) {
        self.slot.close_publication();
        for _ in 0..4 {
            self.consume();
            self.pass(5);
        }
        assert!(
            self.reader.close_after_exit(&self.runtime).unwrap(),
            "only actual input/reader exit retires the original storage Work"
        );
        // Close the original producer positions before the root Works and Scope.
        self.trigger.close_after_exit().unwrap();
        for (work, payer, scope) in [
            (&mut self.lp_work, &mut self.lp_payer, &self.lp_scope),
            (&mut self.ap_work, &mut self.ap_payer, &self.ap_scope),
        ] {
            scope.close_external().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            payer.finish().unwrap();
            assert!(
                scope
                    .reap_observed_header(&work.retirement_observer())
                    .unwrap()
            );
        }
        while let Some(host) = self.runtime.claim_host_actor() {
            host.release(false).unwrap();
        }
        self.runtime.remove_actor(self.lp.actor()).unwrap();
        self.runtime.remove_actor(self.ap.actor()).unwrap();
        self.ledger.remove_scope(self.lp_scope.id()).unwrap();
        self.ledger.remove_scope(self.ap_scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn committed_controls_survive_source_failure_and_tighten_without_waiting_for_ap() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::BeginConvergence {
        mode: AttemptConvergenceMode::Normal,
        remote_deadline: at(100),
        computation_ready: None,
        retain_diagnostics: false,
    });
    f.publish(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(7) });
    f.publish(LogicalToAttemptBody::BeginConvergence {
        mode: AttemptConvergenceMode::Cleanup,
        remote_deadline: at(60),
        computation_ready: None,
        retain_diagnostics: true,
    });
    f.publish(LogicalToAttemptBody::BeginConvergence {
        mode: AttemptConvergenceMode::Normal,
        remote_deadline: at(200),
        computation_ready: None,
        retain_diagnostics: false,
    });
    assert!(
        f.seen.lock().unwrap().is_empty(),
        "LP completion does not wait for AP consumption"
    );
    assert!(!f.reader.custody.body_exited());
    f.lp_scope.fail().unwrap();
    f.pass(1);
    let inputs = f.consume();
    assert_eq!(inputs.len(), 2);
    assert!(inputs.iter().any(|v|matches!(v.body,novarocks_coordination_contract::LogicalToAttemptBody::BeginConvergence {mode:AttemptConvergenceMode::Cleanup,remote_deadline,retain_diagnostics:true,..} if remote_deadline==at(60))));
    assert!(inputs.iter().any(|v|matches!(v.body,novarocks_coordination_contract::LogicalToAttemptBody::PreReadyReplacementChanged {token:t} if t==token(7))));
    f.pass(1);
    assert!(
        f.consume().is_empty(),
        "unchanged committed packets never redeliver"
    );
    f.finish();
}
#[test]
fn source_failure_before_commit_preserves_the_original_empty_mailbox() {
    let mut f = Fixture::new();
    let message = f.message(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(3) });
    let ticket = f.ticket(&message);
    f.lp_scope.fail().unwrap();
    assert!(matches!(
        f.slot.commit(&f.runtime, &ticket, &message),
        Err(PortControlError::Runtime(
            ActorTurnError::FailurePublicationRequired
        ))
    ));
    f.complete(ticket);
    f.pass(1);
    assert!(f.consume().is_empty());
    assert!(!f.reader.custody.body_exited());
    f.finish();
}
#[test]
fn direct_stop_uses_the_original_lp_ticket_and_cannot_fake_reader_exit() {
    let mut f = Fixture::new();
    let failure = novarocks_coordination_contract::ExecutionFailure::from_input(
        novarocks_coordination_contract::ExecutionFailureInput::new(
            novarocks_coordination_contract::QueryFailureReason::DeadlineExceeded.failure_fact(),
            SafeDetail::truncating("original stop"),
            ExecutionFailureSite::Coordinator,
        ),
    );
    let message = f.message(LogicalToAttemptBody::Stop {
        reason: novarocks_coordination_contract::AttemptStopReason::Deadline,
        failure: failure.clone(),
    });
    let ticket = f.ticket(&message);
    f.slot.deliver_stop(&f.runtime, &ticket, message).unwrap();
    f.complete(ticket);
    assert!(!f.reader.custody.body_exited());
    let inputs = f.consume();
    assert_eq!(inputs.len(), 1);
    let novarocks_coordination_contract::LogicalToAttemptBody::Stop {
        reason,
        failure: projected,
    } = &inputs[0].body
    else {
        panic!("original Stop");
    };
    assert_eq!(
        *reason,
        novarocks_coordination_contract::AttemptStopReason::Deadline
    );
    assert_eq!(
        novarocks_attempt_protocol::port_projection::project_failure(projected, &mut |_| None)
            .unwrap(),
        failure
    );
    f.pass(1);
    f.finish();
}
#[test]
fn an_unread_root_control_cannot_be_discarded_as_reader_exit() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::SealRequest { token: token(9) });
    assert!(!f.reader.close_after_exit(&f.runtime).unwrap());
    assert!(!f.reader.custody.body_exited());
    let refs = f.pass(1);
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].token(), token(9));
    let reply = f.reply(token(9), RootControlKind::Seal);
    assert!(f.roots.reply_delivered(&reply).unwrap());
    f.finish();
}

#[test]
fn identical_public_actor_numbers_do_not_bind_a_foreign_original_lp_ticket() {
    let mut first = Fixture::new();
    let mut second = Fixture::new();
    assert!(
        !second
            .slot
            .matches_pair(&first.lp, &first.ap, first.activation)
    );
    assert!(
        first
            .slot
            .matches_pair(&first.lp, &first.ap, first.activation)
    );
    let message =
        first.message(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(6) });
    let ticket = first.ticket(&message);
    assert!(matches!(
        second.slot.commit(&first.runtime, &ticket, &message),
        Err(PortControlError::ForeignPair)
    ));
    first.complete(ticket);
    second.pass(1);
    assert!(second.consume().is_empty());
    first.finish();
    second.finish();
}
#[test]
fn root_token_collision_is_refused_before_the_actual_packet_is_replaced() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::SealRequest { token: token(15) });
    let terminal = f.message(LogicalToAttemptBody::TerminalControlDemand {
        token: token(15),
        root: f.root,
    });
    let ticket = f.ticket(&terminal);
    assert!(matches!(
        f.slot.commit(&f.runtime, &ticket, &terminal),
        Err(PortControlError::ConflictingRootToken)
    ));
    f.complete(ticket);
    let queued = f.pass(1);
    assert_eq!(queued.len(), 1);
    assert!(
        matches!(f.roots.project(&queued[0]).unwrap().body,LogicalToAttemptBody::SealRequest {token:t} if t==token(15))
    );
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(15), RootControlKind::Seal))
            .unwrap()
    );
    f.finish();
}

#[test]
fn changed_token_waits_for_its_original_occurrence_retirement_before_reuse() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(21) });
    f.pass(1);
    let premature =
        f.message(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(22) });
    let ticket = f.ticket(&premature);
    assert!(matches!(
        f.slot.commit(&f.runtime, &ticket, &premature),
        Err(PortControlError::ConflictingChangedToken)
    ));
    f.complete(ticket);
    let first = f.consume();
    assert_eq!(first.len(), 1);
    f.pass(1);
    f.publish(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(22) });
    f.pass(1);
    let second = f.consume();
    assert_eq!(second.len(), 1);
    assert!(
        matches!(first[0].body,novarocks_coordination_contract::LogicalToAttemptBody::PreReadyReplacementChanged {token:t} if t==token(21))
    );
    assert!(
        matches!(second[0].body,novarocks_coordination_contract::LogicalToAttemptBody::PreReadyReplacementChanged {token:t} if t==token(22))
    );
    f.finish();
}
#[test]
fn closed_publisher_is_not_a_positive_reader_or_storage_exit() {
    let mut f = Fixture::new();
    f.slot.close_publication();
    assert!(!f.reader.custody.body_exited());
    assert!(!f.reader.custody.try_retire().unwrap());
    let value = f.message(LogicalToAttemptBody::PreReadyReplacementChanged { token: token(31) });
    let ticket = f.ticket(&value);
    assert!(matches!(
        f.slot.commit(&f.runtime, &ticket, &value),
        Err(PortControlError::Closed)
    ));
    assert_eq!(
        f.runtime
            .effect_inventory(f.lp.actor(), ticket.input, None, 1)
            .unwrap()[0]
            .1,
        EffectState::Started
    );
    f.complete(ticket);
    f.pass(1);
    assert!(f.consume().is_empty());
    f.finish();
}

#[test]
fn unbound_original_writer_cannot_fake_root_delivery_or_reader_exit() {
    let mut f = Fixture::new();
    f.publish(LogicalToAttemptBody::SealRequest { token: token(41) });
    let mut refused = false;
    for _ in 0..5 {
        match f.reader.progress_ordered(1) {
            Err(PortControlError::Observation(ObservationError::Closed)) => {
                refused = true;
                break;
            }
            Ok(_) => {}
            Err(original) => panic!("unexpected original writer refusal: {original:?}"),
        }
    }
    assert!(refused);
    assert!(!f.reader.custody.body_exited());
    assert!(matches!(
        f.roots.close_after_exit(),
        Err(PortControlError::UnsettledRoot)
    ));
    let original = f.pass(1);
    assert_eq!(
        original.len(),
        1,
        "the same held command remains available for its writer"
    );
    assert_eq!(original[0].token(), token(41));
    assert!(
        f.roots
            .reply_delivered(&f.reply(token(41), RootControlKind::Seal))
            .unwrap()
    );
    f.finish();
}

#[test]
fn publication_during_a_budget_one_partial_pass_owes_one_finite_rescan() {
    let mut f = Fixture::new();
    let initial = f.reader.progress(1, |_| panic!("no root command")).unwrap();
    assert_eq!(initial.units, 1);
    assert!(initial.more);
    f.publish(LogicalToAttemptBody::BeginConvergence {
        mode: AttemptConvergenceMode::Normal,
        remote_deadline: at(90),
        computation_ready: None,
        retain_diagnostics: false,
    });
    let mut visits = 0;
    for _ in 0..15 {
        let progress = f.reader.progress(1, |_| panic!("no root command")).unwrap();
        visits += progress.units;
        if !progress.more {
            break;
        }
    }
    assert_eq!(
        visits, 9,
        "four unfinished rows and exactly one owed five-row pass"
    );
    let input = f.consume();
    assert_eq!(
        input.len(),
        1,
        "the real commit cannot be lost behind the already visited row"
    );
    assert!(
        matches!(input[0].body, novarocks_coordination_contract::LogicalToAttemptBody::BeginConvergence { remote_deadline, .. } if remote_deadline == at(90))
    );
    f.finish();
}

#[test]
fn original_failed_owner_session_stops_once_and_preserves_an_existing_stop() {
    use novarocks_control_runtime::actor_turn::FailureSourceState;
    for existing in [false, true] {
        let mut f = std::mem::ManuallyDrop::new(Fixture::new());
        let failure = novarocks_coordination_contract::ExecutionFailure::from_input(
            novarocks_coordination_contract::ExecutionFailureInput::new(
                novarocks_coordination_contract::QueryFailureReason::ActorProtocol.failure_fact(),
                SafeDetail::truncating("original logical owner failure"),
                ExecutionFailureSite::Coordinator,
            ),
        );
        if existing {
            let message = f.message(LogicalToAttemptBody::Stop {
                reason: novarocks_coordination_contract::AttemptStopReason::Deadline,
                failure: failure.clone(),
            });
            let ticket = f.ticket(&message);
            f.slot.deliver_stop(&f.runtime, &ticket, message).unwrap();
            f.complete(ticket);
        }
        let (mut cleanup, mut capacity) = f
            .ap_scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        cleanup.start().unwrap();
        f.trigger.fill(FrontendInput::LogicalHostSettled).unwrap();
        for _ in 0..4 {
            f.runtime.poll_one(Tick(2)).unwrap();
            if f.runtime.failure_proof_for(&f.lp).is_ok() {
                break;
            }
        }
        let proof = f.runtime.failure_proof_for(&f.lp).unwrap();
        let mut session = f
            .runtime
            .claim_actor_failure_publication(&proof, &cleanup)
            .unwrap();
        let receipt = f
            .slot
            .publish_owner_failed_stop(&mut session, &failure)
            .unwrap();
        assert_eq!(
            matches!(receipt, OwnerFailedStopReceipt::AlreadyInFlight(_)),
            existing
        );
        assert_eq!(
            f.slot
                .publish_owner_failed_stop(&mut session, &failure)
                .unwrap(),
            receipt
        );
        assert!(!f.runtime.snapshot(f.ap.actor()).unwrap().failed);
        let seen = f.consume();
        assert_eq!(seen.len(), 1);
        let novarocks_coordination_contract::LogicalToAttemptBody::Stop { reason, .. } =
            &seen[0].body
        else {
            panic!("same original Stop command");
        };
        assert_eq!(
            *reason,
            if existing {
                novarocks_coordination_contract::AttemptStopReason::Deadline
            } else {
                novarocks_coordination_contract::AttemptStopReason::LogicalExecutionFailed
            }
        );
        f.pass(3);
        assert_eq!(
            f.slot
                .publish_owner_failed_stop(&mut session, &failure)
                .unwrap(),
            receipt,
            "actual input retirement does not authorize a second Stop"
        );
        session.finish().unwrap();
        cleanup.body_exited().unwrap();
        cleanup.captures_exited().unwrap();
        capacity.finish().unwrap();
        assert!(
            f.ap_scope
                .reap_observed_header(&cleanup.retirement_observer())
                .unwrap()
        );
        let FailureSourceState::ClosedPending(supervisor) =
            f.runtime.failure_source_state(f.lp.actor()).unwrap()
        else {
            panic!("same original failed owner supervisor");
        };
        f.runtime.retire_input(f.lp.actor(), supervisor).unwrap();
        assert!(f.trigger.retire_ready_after_exit().unwrap());
        drop(f.runtime.take_failure_payload(f.lp.actor()).unwrap());
        std::mem::ManuallyDrop::into_inner(f).finish();
    }
}
