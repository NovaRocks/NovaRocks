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

//! A paid causal role can hand off a predecessor summary and its event even
//! while the predecessor's host effects still retain their original batch.

#[cfg(test)]
use crate::actor_turn::StandingInputId;
use crate::{
    actor::Actor,
    actor_turn::{
        ActorRegistration, ActorRuntime, ActorTurnError, InputAuthor, StandingInputObligation,
    },
    obligation::WorkObligation,
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId},
    mailbox::InputClass,
};

/// The original role's worst case is two distinct bounded Core steps. Combining
/// them would exceed the fixed step budget, while reusing one position would
/// make event progress wait for the predecessor's actual capture/source exit.
pub const CAUSAL_INPUT_POSITIONS: usize = 2;
#[cfg(test)]
type FillResult<T> = Result<(Option<InputId>, InputId), (ActorTurnError, Option<T>, T)>;
pub struct CausalFillFailure<T> {
    pub error: ActorTurnError,
    pub predecessor: Option<T>,
    pub event: T,
    /// The prefix may have transferred even when the event fill was refused.
    /// Keep its exact original identity; queue/role disappearance is not proof
    /// that a concurrent failure owner never received that prefix.
    pub predecessor_input: Option<InputId>,
}

pub struct CausalInputObligation<A: Actor> {
    predecessor: StandingInputObligation<A>,
    event: StandingInputObligation<A>,
}
impl<A: Actor> CausalInputObligation<A> {
    /// Both positions are issued for the original target incarnation and the
    /// actual already admitted producer Work, including a process-scope R.
    pub fn reserve_admitted(
        runtime: &ActorRuntime<A>,
        target: &ActorRegistration,
        source: ActorId,
        class: InputClass,
        author: &WorkObligation,
    ) -> Result<Self, ActorTurnError> {
        assert!(
            class != InputClass::Timer,
            "causal roles cannot consume the actor timer"
        );
        let reserve = |class| runtime.reserve_admitted_input_for(target, source, class, author);
        let predecessor = reserve(InputClass::Lifecycle)?;
        let event = match reserve(class) {
            Ok(event) => event,
            Err(error) => {
                predecessor
                    .revoke_unused()
                    .expect("unbound causal predecessor reservation");
                return Err(error);
            }
        };
        let mut predecessor = match predecessor.into_standing_input() {
            Ok(standing_input) => standing_input,
            Err((error, unused)) => {
                unused
                    .revoke_unused()
                    .expect("no predecessor captures bound");
                event.revoke_unused().expect("no event captures bound");
                return Err(error);
            }
        };
        let event = match event.into_standing_input() {
            Ok(standing_input) => standing_input,
            Err((error, unused)) => {
                unused.revoke_unused().expect("no event captures bound");
                predecessor
                    .close_after_exit()
                    .expect("original unused predecessor holder exits");
                return Err(error);
            }
        };
        Ok(Self { predecessor, event })
    }
    /// Read only the original event position's actual producer/financial
    /// binding. It grants no additional Work, reply cell or consumed fact.
    pub fn event_author(&self) -> Result<InputAuthor, ActorTurnError> {
        self.event.author()
    }
    #[cfg(test)]
    fn positions(&self) -> [StandingInputId; CAUSAL_INPUT_POSITIONS] {
        [self.predecessor.id(), self.event.id()]
    }
    /// The per-Task handoff owner calls this in sealed-cut order, before filling
    /// a later cut. The two fills share one source/receiver FIFO. The poller may
    /// consume the prefix between them without granting another handoff owner.
    ///
    /// `None` is only for a proven-empty prefix; its coverage comes from actual
    /// event consumption, never a fabricated consumed predecessor input. This
    /// avoids an extra Core dispatch on an uncontended ordinary operation.
    ///
    /// Errors return all values not handed off. `None` in the error's prefix
    /// means it was absent or is already real runtime custody; failure teardown
    /// must use that actual inventory instead of rolling back published input.
    #[cfg(test)]
    fn fill(&mut self, predecessor: Option<A::Input>, event: A::Input) -> FillResult<A::Input> {
        self.fill_with_receipt(predecessor, event)
            .map_err(|failure| (failure.error, failure.predecessor, failure.event))
    }
    pub fn in_flight(&self) -> Result<[Option<InputId>; 2], ActorTurnError> {
        Ok([self.predecessor.in_flight()?, self.event.in_flight()?])
    }
    /// Called by the original result owner after actual producer exit.
    /// A busy predecessor does not authorize closing either paid position.
    pub fn retire_ready_after_exit(&self) -> Result<bool, ActorTurnError> {
        let predecessor = self.predecessor.retire_ready_after_exit()?;
        let event = self.event.retire_ready_after_exit()?;
        Ok(predecessor && event)
    }
    /// After a failure fence, the original owner may take only this causal
    /// role's actual Source head. The returned payload must actually exit.
    pub fn take_failed_payload(
        &self,
    ) -> Result<Option<crate::actor_turn::FailedSourcePayload<A>>, ActorTurnError> {
        self.predecessor
            .take_failed_source_payload_matching(|role| {
                role == self.predecessor.id() || role == self.event.id()
            })
    }
    pub fn fill_with_receipt(
        &mut self,
        predecessor: Option<A::Input>,
        event: A::Input,
    ) -> Result<(Option<InputId>, InputId), CausalFillFailure<A::Input>> {
        let idle = self.predecessor.in_flight().and_then(|prefix| {
            self.event
                .in_flight()
                .map(|event| prefix.is_none() && event.is_none())
        });
        match idle {
            Ok(true) => {}
            Ok(false) => {
                return Err(CausalFillFailure {
                    error: ActorTurnError::StandingInputBusy,
                    predecessor,
                    event,
                    predecessor_input: None,
                });
            }
            Err(error) => {
                return Err(CausalFillFailure {
                    error,
                    predecessor,
                    event,
                    predecessor_input: None,
                });
            }
        }
        // These positions already own their original producer proofs. A
        // physical callback may have exited after queuing its completion fact;
        // delivery does not borrow new active-work authority from that body.
        // Each prefix keeps its own original position identity. The event's
        // proof remains exactly the one used by the RPC once-only reply cell.
        let predecessor = if let Some(value) = predecessor {
            match self.predecessor.fill(value) {
                Ok(input) => Some(input),
                Err((error, value)) => {
                    return Err(CausalFillFailure {
                        error,
                        predecessor: Some(value),
                        event,
                        predecessor_input: None,
                    });
                }
            }
        } else {
            None
        };
        match self.event.fill(event) {
            Ok(input) => Ok((predecessor, input)),
            Err((error, value)) => Err(CausalFillFailure {
                error,
                predecessor: None,
                event: value,
                predecessor_input: predecessor,
            }),
        }
    }
    /// The higher-level owner has proved actual holder/capture/report exit.
    /// Neither an expired RPC nor an unprocessed event supplies that proof.
    pub fn close_after_exit(&mut self) -> Result<(), ActorTurnError> {
        if self.predecessor.in_flight()?.is_some() || self.event.in_flight()?.is_some() {
            return Err(ActorTurnError::StandingInputBusy);
        }
        self.predecessor.close_after_exit()?;
        self.event.close_after_exit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actor::{ActorConfig, ActorStep},
        obligation::{ReservationObligation, WorkObligation},
        transaction::{RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        effect::{EffectDescriptor, EffectKind},
        identity::{ActionId, DimensionId, Generation, ScopeId, Tick},
        mailbox::InputEnvelope,
        scope::{WorkAdmission, WorkKind},
    };

    #[derive(Debug, Default)]
    struct Receiver(Vec<u8>);
    impl Actor for Receiver {
        type Input = u8;
        type Effect = ();
        fn timer_input(_: Tick) -> u8 {
            unreachable!("no timer")
        }
        fn handle(&mut self, _: Tick, _: InputEnvelope, value: u8, step: &mut ActorStep<'_, ()>) {
            assert!(step.charge(1));
            self.0.push(value);
            if value == 1 {
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: step
                            .input_work()
                            .expect("the original admitted role authors this effect"),
                        kind: EffectKind::Cleanup,
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
        fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {}
    }
    fn actor() -> ActorId {
        ActorId::new(1).unwrap()
    }
    fn source() -> ActorId {
        ActorId::new(2).unwrap()
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        work: WorkObligation,
        capacity: ReservationObligation,
        runtime: ActorRuntime<Receiver>,
        registration: crate::actor_turn::ActorRegistration,
    }
    fn fixture(control: usize) -> Fixture {
        let resources = ResourceVector::new([(DimensionId::new(1).unwrap(), 3)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                novarocks_control_runtime_core::capacity::ReservationResources::new(
                    resources,
                    novarocks_control_runtime_core::owned_storage::OwnedSlotNeed {
                        work_headers: 1,
                        inputs: 2,
                        batches: 2,
                        effects: 2,
                        ..Default::default()
                    },
                ),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_supervised_bound(
                actor(),
                scope.clone(),
                Receiver::default(),
                ActorConfig {
                    mailbox: [3, control, 2, 1],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 12,
                    max_step_work: 2,
                },
                &work,
            )
            .unwrap();
        Fixture {
            ledger,
            scope,
            work,
            capacity,
            runtime,
            registration,
        }
    }
    impl Fixture {
        fn finish(mut self) -> Vec<u8> {
            self.work.body_exited().unwrap();
            self.work.captures_exited().unwrap();
            self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
            self.capacity
                .release_quota(&self.capacity.resources().unwrap().quota)
                .unwrap();
            self.capacity.finish().unwrap();
            self.scope.close_external().unwrap();
            while let Some(original_host_turn) = self.runtime.claim_host_actor() {
                original_host_turn.release(false).unwrap();
            }
            let receiver = self.runtime.remove_actor(actor()).unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
            receiver.0
        }
    }
    #[test]
    fn event_core_consumption_does_not_wait_for_the_predecessors_actual_host_cleanup() {
        let f = fixture(2);
        let mut causal_input = CausalInputObligation::reserve_admitted(
            &f.runtime,
            &f.registration,
            source(),
            InputClass::Control,
            &f.work,
        )
        .unwrap();
        assert_ne!(causal_input.positions()[0], causal_input.positions()[1]);
        let (prefix, event) = causal_input.fill(Some(1), 2).unwrap();
        let other = f
            .runtime
            .reserve_input(actor(), ActorId::new(3).unwrap(), InputClass::Control)
            .unwrap()
            .fill(3)
            .unwrap();
        f.runtime.poll_one(Tick(1)).unwrap();
        let prefix = prefix.unwrap();
        assert_eq!(
            f.runtime.retire_input(actor(), prefix),
            Err(ActorTurnError::EffectsRemain)
        );
        // Both actual command inputs have reached Core despite the held host
        // effect. This is not inferred from notification or queue emptiness.
        f.runtime.retire_input(actor(), event).unwrap();
        f.runtime.retire_input(actor(), other).unwrap();
        assert_eq!(
            causal_input.fill(Some(1), 4).unwrap_err(),
            (ActorTurnError::StandingInputBusy, Some(1), 4)
        );
        assert_eq!(
            causal_input.close_after_exit(),
            Err(ActorTurnError::StandingInputBusy)
        );
        let cleanup = f
            .runtime
            .take_effect(actor(), prefix, ActionId::new(1).unwrap())
            .unwrap();
        f.runtime.effect_completed(cleanup).unwrap();
        f.runtime.retire_input(actor(), prefix).unwrap();
        f.runtime.close_admission();
        let (prefix, event) = causal_input.fill(None, 4).unwrap();
        assert!(prefix.is_none(), "an empty prefix needs no extra dispatch");
        f.runtime.poll_one(Tick(2)).unwrap();
        f.runtime.retire_input(actor(), event).unwrap();
        causal_input.close_after_exit().unwrap();
        let seen = f.finish();
        assert_eq!(seen[0], 1);
        assert!(
            seen.iter().position(|v| *v == 1).unwrap() < seen.iter().position(|v| *v == 2).unwrap()
        );
        assert!(seen.contains(&3));
        assert_eq!(seen.last(), Some(&4));
    }
    #[test]
    fn a_second_position_refusal_rolls_back_only_the_still_unused_first_position() {
        let f = fixture(1);
        let held = f
            .runtime
            .reserve_input(actor(), source(), InputClass::Control)
            .unwrap();
        assert!(matches!(
            CausalInputObligation::reserve_admitted(
                &f.runtime,
                &f.registration,
                source(),
                InputClass::Control,
                &f.work
            ),
            Err(ActorTurnError::InputCapacity)
        ));
        assert_eq!(f.runtime.snapshot(actor()).unwrap().inputs, 1);
        assert_eq!(f.runtime.snapshot(actor()).unwrap().standing_inputs, 0);
        held.revoke_unused().unwrap();
        let mut causal_input = CausalInputObligation::reserve_admitted(
            &f.runtime,
            &f.registration,
            source(),
            InputClass::Control,
            &f.work,
        )
        .unwrap();
        causal_input.close_after_exit().unwrap();
        assert!(f.finish().is_empty());
    }
}
