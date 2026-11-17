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

//! Actual AP Bootstrap, source selection, owner publication tickets and FIFO.
//! Parked external effects stay unstarted; these tests claim no remote closure.
use super::*;
use crate::control_runtime::{
    actor::TestActor,
    logical::adapter::{LogicalEffectEnvelope, LogicalHostEffect},
};
use novarocks_attempt_protocol::effect::EffectBody;
use novarocks_control_runtime::actor_turn::{EffectTicket, InputPhase};
use novarocks_control_runtime_core::effect::EffectState;
use novarocks_coordination_contract::{
    AttemptInheritedAdmission, AttemptRequestToken, AttemptStopReason, ExecutionFailure,
    logical_port::{LogicalToAttempt as OwnerMessage, LogicalToAttemptBody as OwnerBody},
};
use novarocks_execution_contract::{
    AdmissionTicketId, LeaseValidFor, ParticipationAdmissionTicketReceipt,
};
use std::sync::Mutex;

struct OwnerActor {
    next: Arc<Mutex<Vec<OwnerMessage>>>,
    reservation: ReservationId,
    received: Arc<Mutex<Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>>>,
}
impl TestActor for OwnerActor {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        if let FrontendInput::Logical(input) = input {
            let novarocks_logical_execution_protocol::input::LogicalInput::AttemptPort(message) =
                *input
            else {
                panic!("original AP fact expected")
            };
            self.received.lock().unwrap().push(*message);
            return;
        }
        assert!(matches!(input, FrontendInput::Test(_)));
        let messages = std::mem::take(&mut *self.next.lock().unwrap());
        for (i, message) in messages.into_iter().enumerate() {
            let n = (i + 1) as u64;
            step.effect(descriptor(n,step.input_work().unwrap(),EffectKind::Settlement),FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope{body:LogicalHostEffect::Protocol(Box::new(novarocks_logical_execution_protocol::effect::LogicalEffect{action:novarocks_logical_execution_protocol::LogicalActionId::new(positive(n)),body:novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(Box::new(message))})),reservation:self.reservation}))).unwrap();
        }
    }
}
struct Owner {
    scope: ScopeHandle,
    work: WorkObligation,
    payer: ReservationObligation,
    registration: ActorRegistration,
    next: Arc<Mutex<Vec<OwnerMessage>>>,
    triggers: Vec<StandingInputObligation<FrontendActor>>,
    received: Arc<Mutex<Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>>>,
}
struct Fixture {
    runtime: ActorRuntime<FrontendActor>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    registration: ActorRegistration,
    work: WorkObligation,
    payer: ReservationObligation,
    failure: WorkObligation,
    failure_payer: ReservationObligation,
    capacity: AttemptCapacitySource,
    sources: AttemptSources,
    activation: AttemptActivationIdentity,
    root: novarocks_execution_contract::TaskIdentity,
    roles: Vec<StandingInputObligation<FrontendActor>>,
    owners: [Owner; 2],
    native_inputs: Vec<InputId>,
    native_count: usize,
    parked: Vec<EffectTicket<FrontendEffect>>,
}
fn failure() -> ExecutionFailure {
    ExecutionFailure::from_input(novarocks_attempt_protocol::port_projection::owner_failure())
}
impl Fixture {
    fn new(inherited: bool) -> Self {
        let layout =
            NativeTransportCapacity::from_transport(novarocks_task_codec::TransportBudget::DEFAULT)
                .unwrap();
        let ledger =
            RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
                .unwrap();
        let runtime = ActorRuntime::new();
        let epoch = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, epoch).unwrap();
        let reserve = |scope: &ScopeHandle, kind, inputs| {
            scope
                .reserve_next(
                    epoch,
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            inputs,
                            ..Default::default()
                        },
                    ),
                    kind,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(kind)
                .unwrap()
        };
        let (mut failure, failure_payer) = reserve(&scope, WorkKind::Cleanup, 0);
        failure.start().unwrap();
        let mut start = super::start();
        let max_step_work = start.minimum_core_step_units().unwrap() + OUTPUT_UNITS;
        let turn_budget = 4 * (max_step_work + 1);
        // ActorStep refunds its unused per-step allowance. Size the finite
        // prefix against actual turn units, including owner-ticket creation,
        // rather than assuming every cheap stale ACK spends max_step_work.
        let native_count = 4 * turn_budget;
        start.sources.mailbox = [native_count + 16; 4];
        let (mut work, payer) = reserve(&scope, WorkKind::Settlement, native_count + 64);
        work.start().unwrap();
        start.sources.owner = AttemptSourceId(positive(11));
        start.sources.qa = start.sources.owner;
        if inherited {
            start.activation.inherited_admissions = start
                .plan
                .establish_material
                .keys()
                .map(|&participation| AttemptInheritedAdmission {
                    participation,
                    receipt: ParticipationAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
                        participation,
                        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
                    ),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
        }
        let sources = start.sources;
        let activation = start.activation.identity;
        let root = start.plan.graph.root;
        let members: Arc<[FrozenMember]> = start
            .plan
            .establish_material
            .keys()
            .map(|&participation| FrozenMember {
                participation,
                endpoint: novarocks_execution_contract::RuntimeEndpoint::new("127.0.0.1", 19091)
                    .unwrap(),
            })
            .collect();
        let mut registered = AttemptActor::register(
            &runtime,
            start,
            &members,
            scope.clone(),
            layout,
            ActorConfig {
                mailbox: sources.mailbox,
                max_batches: native_count + 64,
                max_effects_per_step: OUTPUT_UNITS,
                max_dependencies_per_effect: 0,
                turn_budget,
                max_step_work,
            },
            &failure,
        )
        .unwrap();
        let registration = registered.original.clone();
        let author = runtime
            .reserve_admitted_input_for(
                &registration,
                registration.actor(),
                InputClass::Lifecycle,
                &work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        let proof = author.author().unwrap();
        let capacity = registered
            .bind(
                &runtime,
                AttemptEffectAuthors {
                    business: proof.clone(),
                    publication: proof.clone(),
                    reply: proof.clone(),
                    cleanup: proof.clone(),
                    settlement: proof,
                },
                &payer,
                &work,
            )
            .unwrap();
        let make_owner = |id| {
            let scope = ledger.open_scope_next(None, epoch).unwrap();
            let (mut work, payer) = reserve(&scope, WorkKind::Settlement, 8);
            work.start().unwrap();
            let next = Arc::new(Mutex::new(Vec::new()));
            let received = Arc::new(Mutex::new(Vec::new()));
            let registration = runtime
                .register_bound(
                    ActorId::new(id).unwrap(),
                    scope.clone(),
                    FrontendActor::Test(Box::new(OwnerActor {
                        next: Arc::clone(&next),
                        reservation: payer.id(),
                        received: Arc::clone(&received),
                    })),
                    ActorConfig {
                        mailbox: [8; 4],
                        max_batches: 8,
                        max_effects_per_step: 2,
                        max_dependencies_per_effect: 0,
                        max_step_work: 3,
                        turn_budget: 16,
                    },
                )
                .unwrap_or_else(|(error, _)| panic!("original owner registration: {error:?}"));
            Owner {
                scope,
                work,
                payer,
                registration,
                next,
                triggers: Vec::new(),
                received,
            }
        };
        let owners = [make_owner(11), make_owner(21)];
        // Root replies retain the original caller's financial author, whose
        // scope is distinct from the attempt. This is an actual paid AP role,
        // not a copied work ID or a fabricated host failure reference.
        let root_reply = runtime
            .reserve_admitted_input_for(
                &registration,
                runtime_source(sources.owner),
                InputClass::Lifecycle,
                &owners[0].work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        registered
            .root_control
            .bind_reply_author(root_reply.author().unwrap());
        let mut f = Self {
            runtime,
            ledger,
            scope,
            registration,
            work,
            payer,
            failure,
            failure_payer,
            capacity,
            sources,
            activation,
            root,
            roles: vec![author, root_reply],
            owners,
            native_inputs: Vec::new(),
            native_count,
            parked: Vec::new(),
        };
        let bootstrap = f.role(f.registration.actor(), InputClass::Lifecycle);
        let input = f.roles[bootstrap]
            .fill(FrontendInput::AttemptStartup(StartupInput::Bootstrap))
            .unwrap();
        f.poll();
        assert_eq!(
            f.runtime
                .input_custody(f.registration.actor(), input)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed,
            "real Bootstrap commits before any owner message"
        );
        f
    }
    fn role(&mut self, source: ActorId, class: InputClass) -> usize {
        let role = self
            .runtime
            .reserve_admitted_input_for(&self.registration, source, class, &self.work)
            .unwrap()
            .into_standing_input()
            .unwrap();
        self.roles.push(role);
        self.roles.len() - 1
    }
    fn poll(&self) {
        assert!(self.runtime.poll_one(Tick(1)).unwrap());
        assert!(
            !self
                .runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .failed,
            "original AP Core must remain healthy"
        );
    }
    fn fill(&mut self, source: AttemptSourceId, input: AttemptInput) -> InputId {
        let class = input_class(&input);
        let role = self.role(runtime_source(source), class);
        self.roles[role]
            .fill(FrontendInput::Attempt(Box::new(input)))
            .unwrap()
    }
    fn send_batch(&mut self, owner: usize, bodies: Vec<OwnerBody>) -> Vec<InputId> {
        let side = &mut self.owners[owner];
        let messages = bodies
            .into_iter()
            .map(|body| OwnerMessage {
                activation: self.activation,
                body,
            })
            .collect::<Vec<_>>();
        let copies = messages.clone();
        assert!(side.next.lock().unwrap().is_empty());
        *side.next.lock().unwrap() = messages;
        let trigger = self
            .runtime
            .reserve_admitted_input_for(
                &side.registration,
                side.registration.actor(),
                InputClass::Lifecycle,
                &side.work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap();
        let input = trigger.fill(FrontendInput::Test(1)).unwrap();
        side.triggers.push(trigger);
        for _ in 0..16 {
            self.runtime.poll_one(Tick(1)).unwrap();
            if self
                .runtime
                .effect_inventory(side.registration.actor(), input, None, 2)
                .is_ok()
            {
                break;
            }
        }
        let actor = side.registration.actor();
        let mut tickets = Vec::new();
        for n in 1..=copies.len() {
            tickets.push(
                self.runtime
                    .take_effect(actor, input, ActionId::new(n as u64).unwrap())
                    .unwrap(),
            );
        }
        let mut inputs = Vec::new();
        for (ticket, message) in tickets.into_iter().zip(copies) {
            let projected = message.clone();
            let class = input_class(&AttemptInput::Port(projected.clone()));
            let role = self.role(runtime_source(self.sources.owner), class);
            let filled = self.runtime.fill_from_effect(
                &ticket,
                &self.roles[role],
                FrontendInput::Attempt(Box::new(AttemptInput::Port(projected))),
            );
            if owner == 0 {
                inputs.push(filled.unwrap());
            } else {
                assert!(matches!(
                    filled,
                    Err((ActorTurnError::InputAuthorMismatch, _))
                ));
            }
            self.runtime.effect_completed(ticket).unwrap();
        }
        inputs
    }
    fn stop(&self) -> OwnerBody {
        OwnerBody::Stop {
            reason: AttemptStopReason::LogicalExecutionFailed,
            failure: failure(),
        }
    }
    fn park_native(&mut self) {
        for n in 0..self.native_count {
            let id = self.fill(
                self.sources.native,
                AttemptInput::Executor(ExecutorFact::SendStarted {
                    effect: AttemptActionId::new(positive(900 + n as u64)),
                    started_at: MonotonicInstant::ORIGIN,
                }),
            );
            self.native_inputs.push(id);
        }
        self.poll();
        assert!(self.native_pending());
        assert_eq!(self.runtime.next_deadline(), None);
    }
    fn native_pending(&self) -> bool {
        self.native_inputs.iter().any(|&id| {
            self.runtime
                .input_custody(self.registration.actor(), id)
                .unwrap()
                .is_some_and(|input| input.phase == InputPhase::Filled)
        })
    }
    fn parked_effect(&self, process: bool) -> bool {
        let matches = |effect: &FrontendEffect| {
            let FrontendEffect::Attempt(effect) = effect else {
                return false;
            };
            if process {
                matches!(effect.protocol.body, EffectBody::ObserveProcess { .. })
            } else {
                matches!(
                    &effect.protocol.body,
                    EffectBody::Publish(
                        novarocks_attempt_protocol::effect::Publication::ProtocolBatch(batch)
                    ) if batch.initial_membership.as_deref().is_some_and(|membership|
                        membership == &novarocks_attempt_protocol::publication::ProtocolMembership::default()
                    ) && batch.changes.is_empty()
                        && batch.selected_failure.is_none()
                        && !batch.root_success_sealed
                )
            }
        };
        // take_effect moves the original payload into its exclusive ticket;
        // pending_effect_metadata then correctly returns None. Observe that
        // SAME ticket and its still-Started original record after claiming.
        let held = self.parked.iter().any(|ticket| {
            matches(&ticket.effect)
                && self
                    .runtime
                    .effect_inventory(ticket.actor, ticket.input, None, OUTPUT_UNITS)
                    .unwrap()
                    .iter()
                    .any(|(descriptor, state)| {
                        descriptor.id == ticket.descriptor.id && *state == EffectState::Started
                    })
        });
        held || self
            .runtime
            .pending_batches(self.registration.actor(), None, 64)
            .unwrap()
            .into_iter()
            .any(|input| {
                self.runtime
                    .effect_inventory(self.registration.actor(), input, None, OUTPUT_UNITS)
                    .unwrap()
                    .into_iter()
                    .any(|(descriptor, _)| {
                        self.runtime
                            .pending_effect_metadata(
                                self.registration.actor(),
                                input,
                                descriptor.id,
                            )
                            .unwrap()
                            .as_ref()
                            .is_some_and(&matches)
                    })
            })
    }
    fn diagnostic(&self) -> String {
        let actor = self.registration.actor();
        let inputs = self
            .runtime
            .input_inventory(actor, None, self.native_count + 64);
        let mut batches = Vec::new();
        for input in self.runtime.pending_batches(actor, None, 64).unwrap() {
            let effects = self
                .runtime
                .effect_inventory(actor, input, None, OUTPUT_UNITS)
                .unwrap()
                .into_iter()
                .map(|(descriptor, state)| {
                    let payload = self
                        .runtime
                        .pending_effect_metadata(actor, input, descriptor.id);
                    (descriptor, state, payload)
                })
                .collect::<Vec<_>>();
            batches.push((input, effects));
        }
        let held = self
            .parked
            .iter()
            .map(|ticket| {
                (
                    &ticket.effect,
                    ticket.input,
                    ticket.generation,
                    &ticket.descriptor,
                )
            })
            .collect::<Vec<_>>();
        format!(
            "snapshot={:?}, next_deadline={:?}, pending_timer={:?}, timer_state={:?}, inputs={inputs:?}, batches={batches:?}, held={held:?}",
            self.runtime.snapshot(actor),
            self.runtime.next_deadline(),
            self.runtime.pending_timer_input(actor),
            self.runtime.timer_source_state(actor)
        )
    }
    fn claim_parked_metadata(&mut self) {
        for input in self
            .runtime
            .pending_batches(self.registration.actor(), None, self.native_count + 64)
            .unwrap()
        {
            for (descriptor, state) in self
                .runtime
                .effect_inventory(self.registration.actor(), input, None, OUTPUT_UNITS)
                .unwrap()
            {
                if state == EffectState::Pending {
                    self.parked.push(
                        self.runtime
                            .take_effect(self.registration.actor(), input, descriptor.id)
                            .unwrap(),
                    );
                }
            }
        }
    }
    fn park_publication(&mut self) {
        let demand = self.fill(
            self.sources.observation,
            AttemptInput::Port(novarocks_coordination_contract::LogicalToAttempt {
                activation: self.activation,
                body: LogicalToAttemptBody::TerminalControlDemand {
                    token: AttemptRequestToken::new(positive(40)),
                    root: self.root,
                },
            }),
        );
        self.poll();
        // Demand is an immediate request/reply, not a publication wait. First
        // hand its exact reply to the actual LP actor and acknowledge the
        // original processed-input metadata before reusing this root source.
        self.claim_parked_metadata();
        for ticket in std::mem::take(&mut self.parked) {
            if let FrontendEffect::Attempt(effect) = &ticket.effect {
                let EffectBody::Port(original) = &effect.protocol.body else {
                    panic!("only the original terminal-demand reply precedes the seal")
                };
                assert!(
                    matches!(original.body,novarocks_attempt_protocol::port_output::AttemptToLogicalBody::TerminalControlResult{token,result:Ok(())} if token==AttemptRequestToken::new(positive(40)))
                );
                let projected =
                    novarocks_attempt_protocol::port_projection::project_attempt_message(
                        original,
                        &mut |_| None,
                        &mut |_, _| None,
                        &mut |_, _, _| None,
                    )
                    .unwrap();
                let mut position = self
                    .runtime
                    .reserve_admitted_input_for(
                        &self.owners[0].registration,
                        self.registration.actor(),
                        InputClass::Lifecycle,
                        &self.owners[0].work,
                    )
                    .unwrap()
                    .into_standing_input()
                    .unwrap();
                self.runtime
                    .fill_from_effect(
                        &ticket,
                        &position,
                        FrontendInput::Logical(Box::new(
                            novarocks_logical_execution_protocol::input::LogicalInput::AttemptPort(
                                Box::new(projected),
                            ),
                        )),
                    )
                    .unwrap();
                self.runtime.effect_completed(ticket).unwrap();
                self.poll();
                assert!(
                    matches!(self.owners[0].received.lock().unwrap().as_slice(),[message] if matches!(message.body,novarocks_coordination_contract::logical_port::AttemptToLogicalBody::TerminalControlResult{token,result:Ok(())} if token==AttemptRequestToken::new(positive(40))))
                );
                assert!(position.retire_ready_after_exit().unwrap());
                position.close_after_exit().unwrap();
            } else {
                assert!(
                    matches!(ticket.effect,FrontendEffect::AttemptRootControlProcessed(input) if input==demand)
                );
                self.runtime.effect_completed(ticket).unwrap();
            }
        }
        self.runtime
            .retire_input(self.registration.actor(), demand)
            .unwrap();
        // Finish the demand's one real immediate continuation. No horizon is
        // advanced, and the builtin occurrence must actually retire first.
        if self.runtime.next_deadline() == Some(Tick(1)) {
            assert_eq!(self.runtime.advance_time(Tick(1), 64).unwrap(), 1);
            self.poll();
            let timer = self
                .runtime
                .pending_timer_input(self.registration.actor())
                .unwrap()
                .expect("original demand continuation Tick");
            assert!(
                self.runtime
                    .effect_inventory(self.registration.actor(), timer, None, OUTPUT_UNITS)
                    .unwrap()
                    .is_empty()
            );
            self.runtime
                .retire_input(self.registration.actor(), timer)
                .unwrap();
        }
        // Continue the original Tick path to its empty initial ProtocolBatch.
        // Hold that exact publication without returning PublicationApplied or
        // adding an unrelated observation input while publication is parked.
        for _ in 0..32 {
            if self.parked_effect(false) {
                assert_eq!(self.runtime.next_deadline(), None);
                self.claim_parked_metadata();
                return;
            }
            self.runtime.advance_time(Tick(1), 64).unwrap();
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                break;
            }
        }
        panic!(
            "original publication did not reach pending result boundary: {}",
            self.diagnostic()
        );
    }
    fn park_process(&mut self) {
        self.send_batch(
            0,
            vec![OwnerBody::BeginConvergence {
                mode: novarocks_coordination_contract::AttemptConvergenceMode::Normal,
                remote_deadline: MonotonicInstant::from_origin(Duration::from_secs(3)),
                computation_ready: None,
                retain_diagnostics: false,
            }],
        );
        for _ in 0..32 {
            if self.parked_effect(true) {
                assert_eq!(self.runtime.next_deadline(), None);
                self.claim_parked_metadata();
                return;
            }
            self.runtime.advance_time(Tick(1), 64).unwrap();
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                break;
            }
        }
        panic!(
            "original process observation did not reach pending reply boundary: {}",
            self.diagnostic()
        );
    }
    fn assert_stop(&self, input: InputId) {
        assert_eq!(
            self.runtime
                .input_custody(self.registration.actor(), input)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed
        );
        assert!(
            !self
                .runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .failed
        );
        assert!(
            self.scope
                .reserve_next(
                    Generation::new(1).unwrap(),
                    ReservationResources::work_only(),
                    WorkKind::Business,
                    WorkAdmission::External
                )
                .is_err(),
            "actual Core Stop closes business admission"
        );
    }
    fn finish(mut self) {
        // Parked metadata was accepted by this fixture under its ORIGINAL
        // tickets, but no external Work/future was ever started or reported a
        // result. Stop advances the actor epoch. A late metadata completion
        // must therefore consume those same origin-only tickets, never try to
        // abandon an old batch under the new current generation.
        for ticket in self.parked.drain(..) {
            self.runtime.effect_completed(ticket).unwrap();
        }
        for _ in 0..16 {
            if !self.runtime.poll_one(Tick(1)).unwrap() {
                break;
            }
        }
        for input in self
            .runtime
            .pending_batches(self.registration.actor(), None, self.native_count + 64)
            .unwrap()
        {
            for (descriptor, state) in self
                .runtime
                .effect_inventory(self.registration.actor(), input, None, OUTPUT_UNITS)
                .unwrap()
            {
                assert_ne!(state, EffectState::Started);
                if state != EffectState::Completed {
                    self.runtime
                        .abandon_unstarted_effect(self.registration.actor(), input, descriptor.id)
                        .unwrap();
                }
            }
        }
        if let Some(timer) = self
            .runtime
            .pending_timer_input(self.registration.actor())
            .unwrap()
        {
            assert_eq!(
                self.runtime
                    .input_custody(self.registration.actor(), timer)
                    .unwrap()
                    .unwrap()
                    .phase,
                InputPhase::Committed
            );
            assert!(
                self.runtime
                    .effect_inventory(self.registration.actor(), timer, None, OUTPUT_UNITS)
                    .unwrap()
                    .iter()
                    .all(|(_, state)| matches!(
                        state,
                        EffectState::Completed | EffectState::AbandonedByOwnerFailure
                    ))
            );
            self.runtime
                .retire_input(self.registration.actor(), timer)
                .unwrap();
        }
        assert_eq!(
            self.runtime
                .close_timer_source(self.registration.actor())
                .unwrap(),
            novarocks_control_runtime::actor_turn::TimerSourceState::ClosedDrained
        );
        for role in &mut self.roles {
            assert!(role.retire_ready_after_exit().unwrap());
            role.close_after_exit().unwrap();
        }
        self.capacity.close_after_exit().unwrap();
        for side in &mut self.owners {
            for role in &mut side.triggers {
                assert!(role.retire_ready_after_exit().unwrap());
                role.close_after_exit().unwrap();
            }
            side.work.body_exited().unwrap();
            side.work.captures_exited().unwrap();
            side.payer.finish().unwrap();
            side.scope.close_external().unwrap();
            side.scope.reap_ready(128).unwrap();
        }
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.failure.body_exited().unwrap();
        self.failure.captures_exited().unwrap();
        self.payer.finish().unwrap();
        self.failure_payer.finish().unwrap();
        self.scope.close_external().unwrap();
        self.scope.reap_ready(128).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        for actor in [
            self.registration.actor(),
            self.owners[0].registration.actor(),
            self.owners[1].registration.actor(),
        ] {
            self.runtime.remove_actor(actor).unwrap_or_else(|error| {
                panic!(
                    "original fixture actor {actor:?} failed physical retirement: {error:?}; {}",
                    self.diagnostic()
                )
            });
        }
        for scope in [
            self.scope.id(),
            self.owners[0].scope.id(),
            self.owners[1].scope.id(),
        ] {
            self.ledger.remove_scope(scope).unwrap();
        }
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
}
#[test]
fn owner_stop_reaches_actual_adapter_during_frozen_native_prefix() {
    let mut f = Fixture::new(false);
    f.park_native();
    let stop = f.stop();
    let inputs = f.send_batch(0, vec![stop]);
    f.poll();
    f.assert_stop(inputs[0]);
    assert!(
        f.native_pending(),
        "owner Stop does not await Native prefix retirement"
    );
    f.finish();
}
#[test]
fn owner_stop_reaches_actual_adapter_while_publication_has_no_result() {
    let mut f = Fixture::new(false);
    f.park_publication();
    let stop = f.stop();
    let inputs = f.send_batch(0, vec![stop]);
    f.poll();
    f.assert_stop(inputs[0]);
    assert!(f.parked_effect(false));
    f.finish();
}
#[test]
fn owner_stop_reaches_actual_adapter_while_process_observation_has_no_reply() {
    let mut f = Fixture::new(true);
    f.park_process();
    let stop = f.stop();
    let inputs = f.send_batch(0, vec![stop]);
    f.poll();
    f.assert_stop(inputs[0]);
    assert!(f.parked_effect(true));
    f.finish();
}
#[test]
fn owner_lifecycle_head_precedes_stop_across_classes_without_starving_control() {
    for first in [
        OwnerBody::PreReadyReplacementChanged {
            token: AttemptRequestToken::new(positive(909)),
        },
        OwnerBody::AuthorizationReply {
            token: AttemptRequestToken::new(positive(910)),
            decision: Ok(
                novarocks_coordination_contract::AttemptAuthorizationDecision::AdmissionRetryable,
            ),
        },
        OwnerBody::PreReadyReplacementReply {
            token: AttemptRequestToken::new(positive(911)),
            may_observe: false,
        },
    ] {
        let mut f = Fixture::new(false);
        f.park_publication();
        let stop = f.stop();
        let inputs = f.send_batch(0, vec![first, stop]);
        f.poll();
        f.assert_stop(inputs[1]);
        let committed = f
            .runtime
            .committed_batches(f.registration.actor(), None, f.native_count + 64)
            .unwrap();
        let ordinal = |input| committed.iter().find(|(_, id)| *id == input).unwrap().0;
        assert!(
            ordinal(inputs[0]) < ordinal(inputs[1]),
            "actual owner FIFO commits Lifecycle before Control"
        );
        f.finish();
    }
}
#[test]
fn foreign_current_ticket_cannot_publish_original_owner_control() {
    let mut f = Fixture::new(false);
    f.park_publication();
    let stop = f.stop();
    assert!(f.send_batch(1, vec![stop]).is_empty());
    assert!(!f.runtime.snapshot(f.registration.actor()).unwrap().failed);
    let stop = f.stop();
    let inputs = f.send_batch(0, vec![stop]);
    f.poll();
    f.assert_stop(inputs[0]);
    f.finish();
}
