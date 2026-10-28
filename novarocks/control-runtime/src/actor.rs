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

//! Actor state is plain data; resource execution is performed by host owners.

use novarocks_control_runtime_core::{
    effect::{EffectBatchState, EffectDescriptor, EffectError},
    identity::{BatchId, Generation, InputId, ReservationId, ScopeId, Tick, WorkId},
    mailbox::InputEnvelope,
    scheduler::TurnBudget,
};

/// Immutable registration policy, outside the potentially damaged actor state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ActorFailurePolicy {
    #[default]
    IsolateScope,
    /// A process control actor cannot project its own failure as a query error.
    ProcessFailStop,
}

/// Implementations contain only pure protocol state, never runnable or rich-source custody.
pub trait Actor: Send + 'static {
    /// Neutral command values, containing no runnable, rich source or foreign destructor.
    type Input: Send + 'static;
    /// Neutral effect values. Rich objects are represented only by resource identities.
    type Effect: Send + 'static;
    fn timer_input(now: Tick) -> Self::Input;
    fn handle(
        &mut self,
        now: Tick,
        input: InputEnvelope,
        value: Self::Input,
        step: &mut StepContext<'_, Self::Effect>,
    );
    fn next_deadline(&self) -> Option<Tick>;
    /// A pure failure hook cannot inspect the partially mutated protocol state.
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        input: InputEnvelope,
        step: &mut StepContext<'_, Self::Effect>,
    );
}

pub struct StepContext<'a, E> {
    scope: &'a crate::transaction::ScopeHandle,
    turn: &'a mut TurnBudget,
    work_remaining: usize,
    batch: EffectBatchState,
    payloads: std::collections::BTreeMap<novarocks_control_runtime_core::identity::ActionId, E>,
    reservations: std::collections::BTreeMap<ReservationId, crate::lease::StepGuard>,
    max_reservations: usize,
    failure_work: Option<WorkId>,
    stop_producing: bool,
}
impl<'a, E> StepContext<'a, E> {
    pub(crate) fn new(
        budget: &'a mut TurnBudget,
        scope: &'a crate::transaction::ScopeHandle,
        generation: Generation,
        input: InputId,
        max_effects: usize,
        max_dependencies: usize,
        work_budget: usize,
    ) -> Self {
        Self {
            scope,
            turn: budget,
            work_remaining: work_budget,
            payloads: std::collections::BTreeMap::new(),
            reservations: std::collections::BTreeMap::new(),
            max_reservations: max_effects,
            failure_work: None,
            stop_producing: false,
            batch: EffectBatchState::new(
                scope.id(),
                generation,
                input,
                max_effects,
                max_dependencies,
            ),
        }
    }
    pub(crate) fn set_failure_work(&mut self, work: Option<WorkId>) {
        self.failure_work = work;
    }
    /// Immutable registration metadata, independent of the suspect core. The
    /// registration owner retains this admitted cleanup lease until removal.
    pub fn failure_work(&self) -> Option<WorkId> {
        self.failure_work
    }
    /// Stage a stop of new business starts in this step's original scope.
    /// The gate changes only when the driver commits the whole normal step.
    /// This neither signals running tasks nor observes any work or capture exit.
    pub fn stop_producing(&mut self) {
        self.stop_producing = true;
    }
    /// Transfer a still-unbound synchronous reservation into the same atomic
    /// handoff as its neutral effect. No queue/capture may bind it beforehand.
    /// An unwinding step rolls these pure guards back; a committed batch keeps
    /// the originals until its actual host or failure custodian takes them.
    pub fn reservation(
        &mut self,
        guard: crate::lease::StepGuard,
    ) -> Result<(), (ReservationHandoffError, crate::lease::StepGuard)> {
        let error = if !guard.belongs_to(self.scope) {
            Some(ReservationHandoffError::WrongScope)
        } else if self.reservations.contains_key(&guard.reservation_id()) {
            Some(ReservationHandoffError::Duplicate)
        } else if self.reservations.len() >= self.max_reservations {
            Some(ReservationHandoffError::Capacity)
        } else {
            None
        };
        if let Some(error) = error {
            return Err((error, guard));
        }
        self.reservations.insert(guard.reservation_id(), guard);
        Ok(())
    }
    /// Traversal and effect production consume the same finite turn budget.
    pub fn charge(&mut self, units: usize) -> bool {
        if units > self.work_remaining {
            return false;
        }
        self.work_remaining -= units;
        true
    }
    /// The work identity references a pre-admitted host role. Emitting a neutral
    /// effect does not create a WorkLease, payload, execution credit or exit fact.
    pub fn effect(
        &mut self,
        descriptor: EffectDescriptor,
        effect: E,
    ) -> Result<(), (EffectEmissionError, E)> {
        if !self.charge(1) {
            return Err((EffectEmissionError::Yield, effect));
        }
        let id = descriptor.id;
        if let Err(error) = self.batch.push(descriptor) {
            return Err((EffectEmissionError::Invalid(error), effect));
        }
        let previous = self.payloads.insert(id, effect);
        assert!(
            previous.is_none(),
            "one payload accompanies one batch action"
        );
        Ok(())
    }
    pub fn remaining(&self) -> usize {
        self.work_remaining
    }
    pub(crate) fn finish(mut self) -> StepEnvelope<E> {
        self.batch
            .commit()
            .expect("one atomic step envelope handoff");
        self.turn.return_unused(self.work_remaining);
        StepEnvelope {
            ordinal: None,
            state: self.batch,
            payloads: self.payloads,
            reservations: self.reservations,
            stop_producing: self.stop_producing,
        }
    }
}
pub(crate) struct StepEnvelope<E> {
    pub(crate) ordinal: Option<BatchId>,
    pub(crate) stop_producing: bool,
    pub(crate) state: EffectBatchState,
    pub(crate) payloads:
        std::collections::BTreeMap<novarocks_control_runtime_core::identity::ActionId, E>,
    pub(crate) reservations: std::collections::BTreeMap<ReservationId, crate::lease::StepGuard>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationHandoffError {
    WrongScope,
    Duplicate,
    Capacity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectEmissionError {
    Yield,
    Invalid(EffectError),
}

#[derive(Clone, Copy, Debug)]
pub struct ActorConfig {
    pub mailbox: [usize; 4],
    pub max_batches: usize,
    pub max_effects_per_step: usize,
    pub max_dependencies_per_effect: usize,
    pub turn_budget: usize,
    /// Upper bound of one complete pure handle step, including effects and traversals.
    /// Input dispatch costs one additional unit. All four class guarantees must fit.
    pub max_step_work: usize,
}
impl ActorConfig {
    pub fn validate(self) -> Result<Self, &'static str> {
        if self.mailbox.contains(&0) || self.max_batches == 0 || self.max_effects_per_step == 0 {
            return Err("actor capacities must be positive");
        }
        let Some(guaranteed_turn) = self
            .max_step_work
            .checked_add(1)
            .and_then(|units| units.checked_mul(4))
        else {
            return Err("actor step budget overflows");
        };
        if self.max_effects_per_step > self.max_step_work || self.turn_budget < guaranteed_turn {
            return Err("actor turn must fit one complete bounded step from every input class");
        }
        Ok(self)
    }
}
