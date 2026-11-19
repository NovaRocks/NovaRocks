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

//! Original prepaid handoffs, using production inbox/ledger/snapshot gates.
//! An owned session or Causal obligation is never concurrently mutably aliased.
use loom::{
    sync::{Arc, Mutex},
    thread,
};
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::{
        ActorFailureProof, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        FailurePublicationSession, FailureSourceState, StandingInputObligation,
    },
    causal_input::CausalInputObligation,
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, Generation, InputId, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};

type Paid = (WorkObligation, ReservationObligation);

fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(run);
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn action(value: u64) -> ActionId {
    ActionId::new(value).unwrap()
}

#[derive(Debug, Default)]
struct OriginalActor {
    seen: Vec<Arc<u64>>,
}
impl Actor for OriginalActor {
    type Input = Arc<u64>;
    type Effect = ();
    fn timer_input(_: Tick) -> Self::Input {
        panic!("no model timer")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: Arc<u64>, step: &mut ActorStep<'_, ()>) {
        assert!(step.charge(1));
        assert_ne!(*value, 99, "original controlled actor failure");
        let emit = *value == 7;
        self.seen.push(value);
        if emit {
            step.effect(
                EffectDescriptor {
                    id: action(1),
                    work: step.input_work().unwrap(),
                    kind: EffectKind::Business,
                    dependencies: vec![],
                },
                (),
            )
            .unwrap();
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, step: &mut ActorStep<'_, ()>) {
        step.effect(
            EffectDescriptor {
                id: action(17),
                work: step.failure_work().unwrap(),
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            (),
        )
        .unwrap();
    }
}

struct OriginalNode {
    registration: ActorRegistration,
    scope: ScopeHandle,
    body: Option<Paid>,
    cleanup: Option<Paid>,
    inputs: Vec<InputId>,
    tickets: Vec<EffectTicket<()>>,
    failure_ticket: Option<EffectTicket<()>>,
    failure_input: Option<InputId>,
}
fn end_paid((mut work, mut capacity): Paid) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    capacity.finish().unwrap();
}
impl OriginalNode {
    fn new(runtime: &ActorRuntime<OriginalActor>, ledger: &RuntimeLedger, id: u64) -> Self {
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let admit = |kind| {
            let (mut work, capacity) = scope
                .reserve_next(
                    generation(),
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            inputs: 8,
                            batches: 8,
                            effects: 8,
                            ..Default::default()
                        },
                    ),
                    kind,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(kind)
                .unwrap();
            work.start().unwrap();
            (work, capacity)
        };
        let body = admit(WorkKind::Settlement);
        let cleanup = admit(WorkKind::Cleanup);
        let registration = runtime
            .register_supervised_bound(
                ActorId::new(id).unwrap(),
                scope.clone(),
                OriginalActor::default(),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    // One explicit handle unit plus one effect, and one dispatch
                    // unit for each of the four original input-class guarantees.
                    turn_budget: 12,
                    max_step_work: 2,
                },
                &cleanup.0,
            )
            .unwrap();
        Self {
            registration,
            scope,
            body: Some(body),
            cleanup: Some(cleanup),
            inputs: Vec::new(),
            tickets: Vec::new(),
            failure_ticket: None,
            failure_input: None,
        }
    }
    fn id(&self) -> ActorId {
        self.registration.actor()
    }
    fn input(&mut self, runtime: &ActorRuntime<OriginalActor>, value: u64) -> InputId {
        let input = runtime
            .reserve_admitted_input_for(
                &self.registration,
                self.id(),
                InputClass::Ordinary,
                &self.body.as_ref().unwrap().0,
            )
            .unwrap()
            .fill(Arc::new(value))
            .unwrap();
        self.inputs.push(input);
        input
    }
    fn queue_failure(&mut self, runtime: &ActorRuntime<OriginalActor>) {
        assert!(!runtime.snapshot(self.id()).unwrap().failed);
        self.input(runtime, 99);
    }
    fn failed(&mut self, runtime: &ActorRuntime<OriginalActor>) {
        self.queue_failure(runtime);
        poll_until_failed(runtime, self.id());
    }
    fn session(
        &mut self,
        runtime: &ActorRuntime<OriginalActor>,
    ) -> FailurePublicationSession<OriginalActor> {
        assert!(self.failure_ticket.is_none());
        let FailureSourceState::ClosedPending(input) =
            runtime.failure_source_state(self.id()).unwrap()
        else {
            panic!("same original failure projection")
        };
        let ticket = runtime.take_effect(self.id(), input, action(17)).unwrap();
        let session = runtime.claim_failure_publication(&ticket).unwrap();
        self.failure_input = Some(input);
        self.failure_ticket = Some(ticket);
        session
    }
    fn finish_failure(&mut self, runtime: &ActorRuntime<OriginalActor>) {
        if !runtime.snapshot(self.id()).unwrap().failed {
            return;
        }
        if self.failure_ticket.is_none()
            && let FailureSourceState::ClosedPending(_) =
                runtime.failure_source_state(self.id()).unwrap()
        {
            drop(self.session(runtime));
        }
        if let Some(paid) = self.cleanup.take() {
            end_paid(paid);
        }
        if let Some(ticket) = self.failure_ticket.take() {
            runtime.effect_completed(ticket).unwrap();
            runtime
                .retire_input(self.id(), self.failure_input.take().unwrap())
                .unwrap();
        }
    }
    fn finish(
        &mut self,
        runtime: &ActorRuntime<OriginalActor>,
        ledger: &RuntimeLedger,
    ) -> Vec<Arc<u64>> {
        self.finish_failure(runtime);
        for ticket in self.tickets.drain(..) {
            runtime.effect_completed(ticket).unwrap();
        }
        for input in self.inputs.drain(..) {
            runtime
                .retire_input(self.registration.actor(), input)
                .unwrap();
        }
        if let Some(paid) = self.body.take() {
            end_paid(paid);
        }
        if let Some(paid) = self.cleanup.take() {
            end_paid(paid);
        }
        runtime.close_actor_admission(self.id()).unwrap();
        self.scope.close_external().unwrap();
        ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        drop(runtime.take_failure_payload(self.id()).unwrap());
        while let Some(turn) = runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        if !runtime.snapshot(self.id()).unwrap().failed {
            runtime.seal_actor(self.id()).unwrap();
        }
        let seen = runtime.remove_actor(self.id()).unwrap().seen;
        ledger.remove_scope(self.scope.id()).unwrap();
        seen
    }
}
fn poll_until_failed(runtime: &ActorRuntime<OriginalActor>, actor: ActorId) {
    for _ in 0..4 {
        if runtime.snapshot(actor).unwrap().failed {
            return;
        }
        assert!(runtime.poll_one(Tick(1)).unwrap());
    }
    assert!(runtime.snapshot(actor).unwrap().failed);
}
fn close_role(
    runtime: &ActorRuntime<OriginalActor>,
    actor: ActorId,
    mut role: StandingInputObligation<OriginalActor>,
) {
    if runtime.snapshot(actor).unwrap().failed
        && let Some(payload) = role.take_failed_role_payload().unwrap()
    {
        payload.dispose().unwrap();
    }
    if role.in_flight().unwrap().is_some() {
        runtime.poll_one(Tick(2)).unwrap();
        assert!(role.retire_ready_after_exit().unwrap());
    }
    role.close_after_exit().unwrap();
}
struct Pair {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<OriginalActor>,
    sender: OriginalNode,
    receiver: OriginalNode,
}
impl Pair {
    fn new(reverse: bool) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let runtime = ActorRuntime::new();
        let sender = OriginalNode::new(&runtime, &ledger, if reverse { 2 } else { 1 });
        let receiver = OriginalNode::new(&runtime, &ledger, if reverse { 1 } else { 2 });
        Self {
            ledger,
            runtime,
            sender,
            receiver,
        }
    }
    fn role(&self, class: InputClass) -> StandingInputObligation<OriginalActor> {
        self.runtime
            .reserve_admitted_input_for(
                &self.receiver.registration,
                self.sender.id(),
                class,
                &self.receiver.body.as_ref().unwrap().0,
            )
            .unwrap()
            .into_standing_input()
            .unwrap()
    }
    fn receiver_failure(&mut self) -> ActorFailureProof {
        self.receiver.failed(&self.runtime);
        let proof = self
            .runtime
            .failure_proof_for(&self.receiver.registration)
            .unwrap();
        // Dispose the exact original failure projection/input before testing
        // that a separate admitted receiver Work is pinned only by the cell.
        self.receiver.finish_failure(&self.runtime);
        for input in self.receiver.inputs.drain(..) {
            self.runtime
                .retire_input(self.receiver.registration.actor(), input)
                .unwrap();
        }
        drop(
            self.runtime
                .take_failure_payload(self.receiver.id())
                .unwrap(),
        );
        proof
    }
    fn ticket(&mut self) -> EffectTicket<()> {
        let input = self.sender.input(&self.runtime, 7);
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        self.runtime
            .take_effect(self.sender.id(), input, action(1))
            .unwrap()
    }
    fn finish(mut self) -> (Vec<Arc<u64>>, Vec<Arc<u64>>) {
        let sender = self.sender.finish(&self.runtime, &self.ledger);
        let receiver = self.receiver.finish(&self.runtime, &self.ledger);
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
        (sender, receiver)
    }
}

#[test]
fn same_session_negative_fill_and_original_cleanup_exit_use_both_actor_lock_orders() {
    for reverse in [false, true] {
        for class in [InputClass::Control, InputClass::Lifecycle] {
            model(move || {
                let mut f = Pair::new(reverse);
                let role = Arc::new(f.role(class));
                let author = role.author().unwrap();
                f.sender.failed(&f.runtime);
                let mut session = f.sender.session(&f.runtime);
                let original = Arc::new(14);
                let value = original.clone();
                let position = role.clone();
                let publish =
                    thread::spawn(move || session.fill_standing(&position, Some(&author), value));
                let paid = f.sender.cleanup.take().unwrap();
                let exit = thread::spawn(move || end_paid(paid));
                let result = publish.join().unwrap();
                exit.join().unwrap();
                let accepted = match result {
                    Ok(input) => {
                        assert_eq!(
                            role.input_author(input).unwrap().work(),
                            role.author().unwrap().work()
                        );
                        true
                    }
                    Err((ActorTurnError::FailureWorkInactive, rejected)) => {
                        assert!(Arc::ptr_eq(&rejected, &original));
                        assert_eq!(role.in_flight().unwrap(), None);
                        false
                    }
                    other => panic!("unexpected original session gate: {other:?}"),
                };
                let role =
                    Arc::try_unwrap(role).unwrap_or_else(|_| panic!("same original role owner"));
                close_role(&f.runtime, f.receiver.id(), role);
                let (_, seen) = f.finish();
                assert_eq!(seen.len(), usize::from(accepted));
                if accepted {
                    assert!(Arc::ptr_eq(&seen[0], &original));
                }
            });
        }
    }
}

#[test]
fn same_session_fill_and_target_failure_keep_original_payload_custody() {
    for reverse in [false, true] {
        model(move || {
            let mut f = Pair::new(reverse);
            let role = Arc::new(f.role(InputClass::Control));
            f.sender.failed(&f.runtime);
            let mut session = f.sender.session(&f.runtime);
            f.receiver.queue_failure(&f.runtime);
            let runtime = f.runtime.clone();
            let receiver = f.receiver.id();
            let failure = thread::spawn(move || poll_until_failed(&runtime, receiver));
            let original = Arc::new(14);
            let value = original.clone();
            let position = role.clone();
            let publish = thread::spawn(move || session.fill_standing(&position, None, value));
            let result = publish.join().unwrap();
            failure.join().unwrap();
            match result {
                Ok(input) => assert_eq!(role.in_flight().unwrap(), Some(input)),
                Err((ActorTurnError::ActorFailed, rejected)) => {
                    assert!(Arc::ptr_eq(&rejected, &original));
                    assert_eq!(role.in_flight().unwrap(), None);
                }
                other => panic!("unexpected target failure gate: {other:?}"),
            }
            let role = Arc::try_unwrap(role).unwrap_or_else(|_| panic!("original role returned"));
            close_role(&f.runtime, f.receiver.id(), role);
            f.finish();
        });
    }
}

#[test]
fn busy_negative_role_never_overwrites_and_only_actual_retirement_allows_reuse() {
    model(|| {
        let mut f = Pair::new(false);
        let role = Arc::new(f.role(InputClass::Lifecycle));
        f.sender.failed(&f.runtime);
        let mut session = f.sender.session(&f.runtime);
        let first = Arc::new(14);
        let input = session.fill_standing(&role, None, first.clone()).unwrap();
        let busy = Arc::new(15);
        let Err((ActorTurnError::StandingInputBusy, returned)) =
            session.fill_standing(&role, None, busy.clone())
        else {
            panic!("an unconsumed original occurrence is Full")
        };
        assert!(Arc::ptr_eq(&returned, &busy));
        assert_eq!(role.in_flight().unwrap(), Some(input));
        let runtime = f.runtime.clone();
        let consumed = role.clone();
        let retire = thread::spawn(move || {
            assert!(runtime.poll_one(Tick(2)).unwrap());
            assert!(consumed.retire_ready_after_exit().unwrap());
        });
        let position = role.clone();
        let second = busy.clone();
        let publish = thread::spawn(move || session.fill_standing(&position, None, second));
        let result = publish.join().unwrap();
        retire.join().unwrap();
        let accepted = match result {
            Ok(_) => true,
            Err((ActorTurnError::StandingInputBusy, returned)) => {
                assert!(Arc::ptr_eq(&returned, &busy));
                false
            }
            other => panic!("unexpected reuse result: {other:?}"),
        };
        let role = Arc::try_unwrap(role).unwrap_or_else(|_| panic!("same original role"));
        close_role(&f.runtime, f.receiver.id(), role);
        let (_, seen) = f.finish();
        assert_eq!(seen.len(), 1 + usize::from(accepted));
        assert!(Arc::ptr_eq(&seen[0], &first));
        if accepted {
            assert!(Arc::ptr_eq(&seen[1], &busy));
        }
    });
}

#[test]
fn causal_failure_bridge_preserves_empty_prefix_author_and_session_exit_fence() {
    for reverse in [false, true] {
        model(move || {
            let mut f = Pair::new(reverse);
            let causal = CausalInputObligation::reserve_admitted(
                &f.runtime,
                &f.receiver.registration,
                f.sender.id(),
                InputClass::Control,
                &f.receiver.body.as_ref().unwrap().0,
            )
            .unwrap();
            let author = causal.event_author().unwrap();
            let original_work = author.work();
            f.sender.failed(&f.runtime);
            let mut session = f.sender.session(&f.runtime);
            let original = Arc::new(14);
            let next = original.clone();
            let publish = thread::spawn(move || {
                let mut causal = causal;
                let result = causal.fill_event_from_failure(&mut session, Some(&author), next);
                (causal, result)
            });
            let paid = f.sender.cleanup.take().unwrap();
            let exit = thread::spawn(move || end_paid(paid));
            let (mut causal, result) = publish.join().unwrap();
            exit.join().unwrap();
            let accepted = match result {
                Ok(input) => {
                    assert_eq!(causal.in_flight().unwrap(), [None, Some(input)]);
                    assert_eq!(causal.event_author().unwrap().work(), original_work);
                    true
                }
                Err((ActorTurnError::FailureWorkInactive, returned)) => {
                    assert!(Arc::ptr_eq(&returned, &original));
                    assert_eq!(causal.in_flight().unwrap(), [None, None]);
                    false
                }
                other => panic!("unexpected original Causal gate: {other:?}"),
            };
            if accepted {
                assert!(f.runtime.poll_one(Tick(2)).unwrap());
                assert!(causal.retire_ready_after_exit().unwrap());
            }
            causal.close_after_exit().unwrap();
            let (_, seen) = f.finish();
            assert_eq!(seen.len(), usize::from(accepted));
            if accepted {
                assert!(Arc::ptr_eq(&seen[0], &original));
            }
        });
    }
}

#[test]
fn causal_failure_bridge_cannot_skip_a_live_prefix_even_when_event_credit_returned() {
    model(|| {
        let mut f = Pair::new(false);
        let mut causal = CausalInputObligation::reserve_admitted(
            &f.runtime,
            &f.receiver.registration,
            f.sender.id(),
            InputClass::Control,
            &f.receiver.body.as_ref().unwrap().0,
        )
        .unwrap();
        let prefix = Arc::new(11);
        let event = Arc::new(12);
        let (prefix_input, event_input) = causal
            .fill_with_receipt(Some(prefix.clone()), event.clone())
            .unwrap_or_else(|_| panic!("same paid Causal prefix and event"));
        let prefix_input = prefix_input.unwrap();
        f.sender.failed(&f.runtime);
        let mut session = f.sender.session(&f.runtime);
        // Consume the actual FIFO prefix, but initially return ONLY event credit.
        f.runtime.poll_one(Tick(2)).unwrap();
        f.runtime
            .retire_input(f.receiver.id(), event_input)
            .unwrap();
        assert_eq!(causal.in_flight().unwrap(), [Some(prefix_input), None]);
        let next = Arc::new(14);
        let Err((ActorTurnError::StandingInputBusy, returned)) =
            causal.fill_event_from_failure(&mut session, None, next.clone())
        else {
            panic!("a live original prefix blocks the event bridge")
        };
        assert!(Arc::ptr_eq(&returned, &next));
        let runtime = f.runtime.clone();
        let receiver = f.receiver.id();
        let retire = thread::spawn(move || runtime.retire_input(receiver, prefix_input).unwrap());
        let original = next.clone();
        let publish = thread::spawn(move || {
            let result = causal.fill_event_from_failure(&mut session, None, original);
            (causal, session, result)
        });
        let (mut causal, mut session, result) = publish.join().unwrap();
        retire.join().unwrap();
        let accepted = match result {
            Ok(input) => {
                assert_eq!(causal.in_flight().unwrap(), [None, Some(input)]);
                // A genuinely outstanding event has its own independent Busy gate.
                let Err((ActorTurnError::StandingInputBusy, returned)) =
                    causal.fill_event_from_failure(&mut session, None, next.clone())
                else {
                    panic!("an original event cannot be overwritten")
                };
                assert!(Arc::ptr_eq(&returned, &next));
                f.runtime.poll_one(Tick(3)).unwrap();
                assert!(causal.retire_ready_after_exit().unwrap());
                true
            }
            Err((ActorTurnError::StandingInputBusy, returned)) => {
                assert!(Arc::ptr_eq(&returned, &next));
                assert_eq!(causal.in_flight().unwrap(), [None, None]);
                false
            }
            other => panic!("unexpected original prefix retirement: {other:?}"),
        };
        drop(session);
        causal.close_after_exit().unwrap();
        let (_, seen) = f.finish();
        assert_eq!(seen.len(), 2 + usize::from(accepted));
        assert!(Arc::ptr_eq(&seen[0], &prefix));
        assert!(Arc::ptr_eq(&seen[1], &event));
        if accepted {
            assert!(Arc::ptr_eq(&seen[2], &next));
        }
    });
}

#[derive(Clone, Copy)]
enum PublicationRace {
    SenderFails,
    OriginalRoleCloses,
}

#[test]
fn current_ticket_failed_receiver_swap_and_source_failure_or_role_close_have_one_order() {
    for reverse in [false, true] {
        for race in [
            PublicationRace::SenderFails,
            PublicationRace::OriginalRoleCloses,
        ] {
            model(move || {
                let mut f = Pair::new(reverse);
                let role = f.role(InputClass::Lifecycle);
                let initial = Arc::new(0);
                let cell = f
                    .runtime
                    .receiver_failed_snapshot_for(&role, &f.sender.registration, initial.clone())
                    .unwrap();
                let reader = cell.reader();
                let failure = f.receiver_failure();
                let ticket = f.ticket();
                let original = Arc::new(42);
                let next = original.clone();
                let runtime = f.runtime.clone();
                let publish = thread::spawn(move || {
                    let result =
                        runtime.commit_receiver_failed_snapshot(&ticket, &failure, &cell, next);
                    (ticket, cell, result)
                });
                let competitor = match race {
                    PublicationRace::SenderFails => {
                        f.sender.queue_failure(&f.runtime);
                        let runtime = f.runtime.clone();
                        let sender = f.sender.id();
                        thread::spawn(move || {
                            poll_until_failed(&runtime, sender);
                            role
                        })
                    }
                    PublicationRace::OriginalRoleCloses => thread::spawn(move || {
                        let mut role = role;
                        role.close_after_exit().unwrap();
                        role
                    }),
                };
                let (ticket, cell, result) = publish.join().unwrap();
                let role = competitor.join().unwrap();
                match result {
                    Ok(old) => {
                        assert!(Arc::ptr_eq(&old, &initial));
                        assert!(Arc::ptr_eq(&reader.read(), &original));
                        assert!(
                            f.runtime
                                .commit_receiver_failed_snapshot(
                                    &ticket,
                                    &f.runtime
                                        .failure_proof_for(&f.receiver.registration)
                                        .unwrap(),
                                    &cell,
                                    Arc::new(43)
                                )
                                .is_err(),
                            "the same current ticket cannot swap twice"
                        );
                    }
                    Err((error, returned)) => {
                        assert_eq!(
                            error,
                            match race {
                                PublicationRace::SenderFails =>
                                    ActorTurnError::FailurePublicationRequired,
                                PublicationRace::OriginalRoleCloses =>
                                    ActorTurnError::UnknownStandingInput,
                            }
                        );
                        assert!(Arc::ptr_eq(&returned, &original));
                        assert!(Arc::ptr_eq(&reader.read(), &initial));
                    }
                }
                f.sender.tickets.push(ticket);
                if matches!(race, PublicationRace::SenderFails) {
                    close_role(&f.runtime, f.receiver.id(), role);
                }
                // Both publication bodies have actually returned before retiring
                // this original cell; a passive reader outlives it without a pin.
                let committed = cell.retire();
                assert!(Arc::ptr_eq(&reader.read(), &committed));
                f.finish();
                assert!(Arc::ptr_eq(&reader.read(), &committed));
            });
        }
    }
}

#[test]
fn same_number_foreign_failure_proof_and_busy_original_role_cannot_publish_custody() {
    model(|| {
        let mut f = Pair::new(false);
        let role = f.role(InputClass::Lifecycle);
        let initial = Arc::new(0);
        let cell = f
            .runtime
            .receiver_failed_snapshot_for(&role, &f.sender.registration, initial.clone())
            .unwrap();
        let occurrence = role.fill(Arc::new(14)).unwrap();
        let proof = f.receiver_failure();
        assert_eq!(role.in_flight().unwrap(), Some(occurrence));
        let ticket = f.ticket();
        let foreign_ledger = RuntimeLedger::new(ResourceVector::default());
        let empty = foreign_ledger.open_scope_next(None, generation()).unwrap();
        let foreign_runtime = ActorRuntime::new();
        let mut foreign =
            OriginalNode::new(&foreign_runtime, &foreign_ledger, f.receiver.id().get());
        foreign.failed(&foreign_runtime);
        let wrong = foreign_runtime
            .failure_proof_for(&foreign.registration)
            .unwrap();
        assert_eq!(wrong.registration().actor(), proof.registration().actor());
        assert_eq!(foreign.scope.id(), f.receiver.scope.id());
        let original = Arc::new(42);
        let (error, returned) = f
            .runtime
            .commit_receiver_failed_snapshot(&ticket, &wrong, &cell, original.clone())
            .unwrap_err();
        assert_eq!(error, ActorTurnError::PublicationTargetMismatch);
        assert!(Arc::ptr_eq(&returned, &original));
        let (error, returned) = f
            .runtime
            .commit_receiver_failed_snapshot(&ticket, &proof, &cell, original.clone())
            .unwrap_err();
        assert_eq!(error, ActorTurnError::InputAuthorMismatch);
        assert!(Arc::ptr_eq(&returned, &original));
        assert!(Arc::ptr_eq(&cell.read(), &initial));
        if let Some(payload) = role.take_failed_role_payload().unwrap() {
            payload.dispose().unwrap();
        }
        assert!(role.retire_ready_after_exit().unwrap());
        let old = f
            .runtime
            .commit_receiver_failed_snapshot(&ticket, &proof, &cell, original.clone())
            .unwrap();
        assert!(Arc::ptr_eq(&old, &initial));
        assert_eq!(
            role.in_flight().unwrap(),
            None,
            "passive custody never fills the failed Core"
        );
        assert!(Arc::ptr_eq(&cell.read(), &original));
        f.sender.tickets.push(ticket);
        cell.retire();
        close_role(&f.runtime, f.receiver.id(), role);
        foreign.finish(&foreign_runtime, &foreign_ledger);
        empty.close_external().unwrap();
        foreign_ledger.remove_scope(empty.id()).unwrap();
        assert!(foreign_runtime.is_empty());
        assert!(foreign_ledger.is_empty());
        f.finish();
    });
}

#[test]
fn admitted_receiver_pin_binding_and_real_work_exit_preserve_header_and_reader_boundaries() {
    model(|| {
        let mut f = Pair::new(false);
        let proof = f.receiver_failure();
        let observer = f.receiver.body.as_ref().unwrap().0.retirement_observer();
        let paid = Arc::new(Mutex::new(f.receiver.body.take().unwrap()));
        let closing = paid.clone();
        let exit = thread::spawn(move || {
            let mut paid = closing.lock().unwrap();
            paid.0.body_exited().unwrap();
            paid.0.captures_exited().unwrap();
            paid.1.finish().unwrap();
        });
        let initial = Arc::new(0);
        // This is the sole original Work owner loan. Its host gate permits
        // binding before or after actual exit, without an illegal &mut alias.
        let bound = {
            let original = paid.lock().unwrap();
            f.runtime.receiver_failed_snapshot_for_admitted_work(
                &proof,
                &f.sender.registration,
                &original.0,
                initial.clone(),
            )
        };
        exit.join().unwrap();
        f.ledger.reap_scope_ready(f.receiver.scope.id(), 8).unwrap();
        match bound {
            Ok(cell) => {
                assert!(
                    !observer.is_retired(),
                    "same original pin prevents premature Work reap"
                );
                assert!(observer.header_holders().unwrap().0 >= 1);
                let reader = cell.reader();
                let observe = thread::spawn(move || {
                    let value = reader.read();
                    (reader, value)
                });
                assert!(Arc::ptr_eq(&cell.retire(), &initial));
                let (reader, value) = observe.join().unwrap();
                assert!(Arc::ptr_eq(&value, &initial));
                f.ledger.reap_scope_ready(f.receiver.scope.id(), 8).unwrap();
                assert!(observer.is_retired());
                f.finish();
                assert!(
                    Arc::ptr_eq(&reader.read(), &initial),
                    "reader owns a value, never Work retirement authority"
                );
            }
            Err((ActorTurnError::InputAuthorInactive, returned)) => {
                assert!(Arc::ptr_eq(&returned, &initial));
                assert!(observer.is_retired());
                f.finish();
            }
            _ => panic!("unexpected admitted receiver binding gate"),
        }
    });
}

#[test]
fn failed_sender_session_custody_swap_obeys_original_cleanup_exit_and_explicit_pin_retirement() {
    for reverse in [false, true] {
        model(move || {
            let mut f = Pair::new(reverse);
            let proof = f.receiver_failure();
            let observer = f.receiver.body.as_ref().unwrap().0.retirement_observer();
            let initial = Arc::new(0);
            let cell = f
                .runtime
                .receiver_failed_snapshot_for_admitted_work(
                    &proof,
                    &f.sender.registration,
                    &f.receiver.body.as_ref().unwrap().0,
                    initial.clone(),
                )
                .unwrap();
            let reader = cell.reader();
            f.sender.failed(&f.runtime);
            let mut session = f.sender.session(&f.runtime);
            let original = Arc::new(42);
            let value = original.clone();
            let publish = thread::spawn(move || {
                let result = session.commit_receiver_failed_snapshot(&proof, &cell, value);
                (cell, result)
            });
            let paid = f.sender.cleanup.take().unwrap();
            let exit = thread::spawn(move || end_paid(paid));
            let (cell, result) = publish.join().unwrap();
            exit.join().unwrap();
            let expected = match result {
                Ok(old) => {
                    assert!(Arc::ptr_eq(&old, &initial));
                    assert!(Arc::ptr_eq(&reader.read(), &original));
                    original
                }
                Err((ActorTurnError::FailureWorkInactive, returned)) => {
                    assert!(Arc::ptr_eq(&returned, &original));
                    assert!(Arc::ptr_eq(&reader.read(), &initial));
                    initial
                }
                other => panic!("unexpected failed sender snapshot gate: {other:?}"),
            };
            end_paid(f.receiver.body.take().unwrap());
            f.ledger.reap_scope_ready(f.receiver.scope.id(), 8).unwrap();
            assert!(!observer.is_retired());
            assert!(Arc::ptr_eq(&cell.retire(), &expected));
            f.ledger.reap_scope_ready(f.receiver.scope.id(), 8).unwrap();
            assert!(observer.is_retired());
            f.finish();
            assert!(Arc::ptr_eq(&reader.read(), &expected));
        });
    }
}

#[test]
fn failure_session_cannot_borrow_a_foreign_source_class_or_second_session() {
    model(|| {
        let mut f = Pair::new(false);
        let ordinary = f.role(InputClass::Ordinary);
        let mut wrong_source = f
            .runtime
            .reserve_admitted_input_for(
                &f.receiver.registration,
                f.receiver.id(),
                InputClass::Control,
                &f.receiver.body.as_ref().unwrap().0,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        let mut own = f
            .runtime
            .reserve_admitted_input_for(
                &f.sender.registration,
                f.sender.id(),
                InputClass::Control,
                &f.sender.body.as_ref().unwrap().0,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        f.sender.failed(&f.runtime);
        let mut session = f.sender.session(&f.runtime);
        assert_eq!(
            f.runtime
                .claim_failure_publication(f.sender.failure_ticket.as_ref().unwrap())
                .unwrap_err(),
            ActorTurnError::FailurePublicationAlreadyClaimed
        );
        let original = Arc::new(14);
        for (role, expected) in [
            (&ordinary, ActorTurnError::PublicationTargetMismatch),
            (&wrong_source, ActorTurnError::InputAuthorMismatch),
            (&own, ActorTurnError::PublicationTargetMismatch),
        ] {
            let (error, returned) = session
                .fill_standing(role, None, original.clone())
                .unwrap_err();
            assert_eq!(error, expected);
            assert!(Arc::ptr_eq(&returned, &original));
            assert_eq!(role.in_flight().unwrap(), None);
        }
        session.finish().unwrap();
        assert_eq!(
            f.runtime
                .claim_failure_publication(f.sender.failure_ticket.as_ref().unwrap())
                .unwrap_err(),
            ActorTurnError::FailurePublicationAlreadyClaimed
        );
        close_role(&f.runtime, f.receiver.id(), ordinary);
        wrong_source.close_after_exit().unwrap();
        own.close_after_exit().unwrap();
        let (_, seen) = f.finish();
        assert!(seen.is_empty());
    });
}

#[test]
fn same_actor_runtime_foreign_ledger_binding_returns_original_arc_without_a_pin() {
    model(|| {
        let mut f = Pair::new(false);
        let foreign_ledger = RuntimeLedger::new(ResourceVector::default());
        let mut foreign = OriginalNode::new(&f.runtime, &foreign_ledger, 3);
        let role = f
            .runtime
            .reserve_admitted_input_for(
                &f.receiver.registration,
                foreign.id(),
                InputClass::Lifecycle,
                &f.receiver.body.as_ref().unwrap().0,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        let proof = f.receiver_failure();
        let original = Arc::new(41);
        let observer = f.receiver.body.as_ref().unwrap().0.retirement_observer();
        let pins = observer.header_holders();
        let Err((error, returned)) = f.runtime.receiver_failed_snapshot_for_admitted_work(
            &proof,
            &foreign.registration,
            &f.receiver.body.as_ref().unwrap().0,
            original.clone(),
        ) else {
            panic!("one ActorRuntime does not unify independent original Work ledgers")
        };
        assert_eq!(error, ActorTurnError::PublicationTargetMismatch);
        assert!(Arc::ptr_eq(&returned, &original));
        assert_eq!(
            observer.header_holders(),
            pins,
            "rejection never obtains a receiver pin"
        );
        let Err((error, returned)) =
            f.runtime
                .receiver_failed_snapshot_for(&role, &foreign.registration, original.clone())
        else {
            panic!("a role-backed cell must use the same original ledger as its sender")
        };
        assert_eq!(error, ActorTurnError::PublicationTargetMismatch);
        assert!(Arc::ptr_eq(&returned, &original));
        assert_eq!(observer.header_holders(), pins);
        close_role(&f.runtime, f.receiver.id(), role);
        foreign.finish(&f.runtime, &foreign_ledger);
        assert!(foreign_ledger.is_empty());
        f.finish();
    });
}

#[test]
fn opposite_failed_publishers_share_one_two_inbox_order_without_aliasing_sessions() {
    model(|| {
        // Locked Loom 0.7.2 forwards this argument unchanged to generator
        // 0.8.10, which sizes its stack in machine words (not byte units).
        // The two-panic setup needs a larger controller stack; the two actual
        // publication threads and every exploration bound stay unchanged.
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(opposite_failed_publishers_body)
            .unwrap()
            .join()
            .unwrap();
    });
}

fn opposite_failed_publishers_body() {
    let mut f = Pair::new(false);
    let forward = f.role(InputClass::Lifecycle);
    let backward = f
        .runtime
        .reserve_admitted_input_for(
            &f.sender.registration,
            f.receiver.id(),
            InputClass::Lifecycle,
            &f.sender.body.as_ref().unwrap().0,
        )
        .unwrap()
        .into_standing_input()
        .unwrap();
    let forward_cell = f
        .runtime
        .receiver_failed_snapshot_for(&forward, &f.sender.registration, Arc::new(0))
        .unwrap();
    let backward_cell = f
        .runtime
        .receiver_failed_snapshot_for(&backward, &f.receiver.registration, Arc::new(0))
        .unwrap();
    f.sender.failed(&f.runtime);
    f.receiver.failed(&f.runtime);
    let sender_proof = f.runtime.failure_proof_for(&f.sender.registration).unwrap();
    let receiver_proof = f
        .runtime
        .failure_proof_for(&f.receiver.registration)
        .unwrap();
    let mut sender_session = f.sender.session(&f.runtime);
    let mut receiver_session = f.receiver.session(&f.runtime);
    // Each thread owns a DISTINCT original failure session and cell.
    // The calls take opposite source/destination ActorIds concurrently.
    let a = thread::spawn(move || {
        assert_eq!(
            *sender_session
                .commit_receiver_failed_snapshot(&receiver_proof, &forward_cell, Arc::new(41))
                .unwrap(),
            0
        );
        (forward_cell, sender_session)
    });
    let b = thread::spawn(move || {
        assert_eq!(
            *receiver_session
                .commit_receiver_failed_snapshot(&sender_proof, &backward_cell, Arc::new(42))
                .unwrap(),
            0
        );
        (backward_cell, receiver_session)
    });
    let (forward_cell, mut sender_session) = a.join().unwrap();
    let (backward_cell, mut receiver_session) = b.join().unwrap();
    assert_eq!(*forward_cell.read(), 41);
    assert_eq!(*backward_cell.read(), 42);
    let rejected_a = Arc::new(14);
    let (error, returned) = sender_session
        .fill_standing(&forward, None, rejected_a.clone())
        .unwrap_err();
    assert_eq!(error, ActorTurnError::ActorFailed);
    assert!(Arc::ptr_eq(&returned, &rejected_a));
    let rejected_b = Arc::new(15);
    let (error, returned) = receiver_session
        .fill_standing(&backward, None, rejected_b.clone())
        .unwrap_err();
    assert_eq!(error, ActorTurnError::ActorFailed);
    assert!(Arc::ptr_eq(&returned, &rejected_b));
    assert_eq!(forward.in_flight().unwrap(), None);
    assert_eq!(backward.in_flight().unwrap(), None);
    drop(sender_session);
    drop(receiver_session);
    forward_cell.retire();
    backward_cell.retire();
    close_role(&f.runtime, f.receiver.id(), forward);
    close_role(&f.runtime, f.sender.id(), backward);
    f.finish();
}
