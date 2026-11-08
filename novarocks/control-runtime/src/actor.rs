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
    effect::{EffectBatchState, EffectDescriptor, EffectError, EffectKind},
    identity::{
        ActorId, BatchId, Generation, InputId, ReservationId, ScopeId, ScopedWorkRef, Tick,
    },
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
        step: &mut ActorStep<'_, Self::Effect>,
    );
    fn next_deadline(&self) -> Option<Tick>;
    /// A pure failure hook cannot inspect the partially mutated protocol state.
    fn owner_failed(
        scope: ScopeId,
        generation: Generation,
        input: InputEnvelope,
        step: &mut ActorStep<'_, Self::Effect>,
    );
}

/// Passive tail of one source sampled under the original inbox lock before
/// this input was popped. Later fills do not change this cut. An empty tail
/// proves neither producer closure nor actual exit; InputId is not a sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputSourceCut {
    pub source: ActorId,
    pub tail: Option<InputId>,
}

pub struct ActorStep<'a, E> {
    scope: &'a crate::transaction::ScopeHandle,
    turn: &'a mut TurnBudget,
    work_remaining: usize,
    batch: EffectBatchState,
    payloads: std::collections::BTreeMap<novarocks_control_runtime_core::identity::ActionId, E>,
    reservations: std::collections::BTreeMap<ReservationId, crate::obligation::StepRegistration>,
    max_reservations: usize,
    failure_work: Option<ScopedWorkRef>,
    retained: bool,
    runtime_metadata: bool,
    input_author: Option<crate::actor_turn::InputAuthor>,
    authors: std::collections::BTreeMap<
        novarocks_control_runtime_core::identity::ActionId,
        crate::actor_turn::InputAuthor,
    >,
    metadata: Vec<MetadataEffect<E>>,
    stop_producing: bool,
    input_source: Option<Option<ActorId>>,
    input_source_cut: Option<InputSourceCut>,
}
impl<'a, E> ActorStep<'a, E> {
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
            retained: false,
            runtime_metadata: false,
            input_author: None,
            authors: std::collections::BTreeMap::new(),
            metadata: Vec::new(),
            stop_producing: false,
            input_source: None,
            input_source_cut: None,
            batch: EffectBatchState::new(
                scope.id(),
                generation,
                input,
                max_effects,
                max_dependencies,
            ),
        }
    }
    /// None means this registration did not request a source observation.
    /// Some with tail=None is this claim's empty-source sample only. It grants
    /// no input, scheduling, resource, producer-liveness or exit authority.
    pub fn input_source_cut(&self) -> Option<InputSourceCut> {
        self.input_source_cut
    }
    pub(crate) fn set_input_source_cut(&mut self, cut: Option<InputSourceCut>) {
        self.input_source_cut = cut;
    }
    pub(crate) fn set_input_author(&mut self, author: Option<crate::actor_turn::InputAuthor>) {
        self.input_author = author;
    }
    pub(crate) fn retained(&mut self) {
        self.retained = true;
    }
    /// The current retained request's already admitted original work. A cold
    /// timer has no work; this observation never admits or derives any work.
    pub fn input_work(&self) -> Option<ScopedWorkRef> {
        self.input_author
            .as_ref()
            .map(crate::actor_turn::InputAuthor::work)
    }
    pub fn input_author(&self) -> Option<&crate::actor_turn::InputAuthor> {
        self.input_author.as_ref()
    }
    pub(crate) fn set_runtime_metadata(&mut self, value: bool) {
        self.runtime_metadata = value;
    }
    /// True only for an input issued by the actual Workless forwarding port.
    pub const fn is_runtime_metadata(&self) -> bool {
        self.runtime_metadata
    }
    /// The driver's actual execution-detachment mode, never a protocol phase.
    pub const fn is_retained(&self) -> bool {
        self.retained
    }
    /// Stage a neutral retained-state notice without execution or publication
    /// authority. Custody remains until its actual destination consumes it.
    pub fn metadata(&mut self, effect: E) -> Result<(), (EffectEmissionError, E)> {
        if !self.retained {
            return Err((EffectEmissionError::Unfunded, effect));
        }
        if !self.charge(1) {
            return Err((EffectEmissionError::Yield, effect));
        }
        if self.metadata.len() + self.payloads.len() >= self.max_reservations {
            return Err((EffectEmissionError::Invalid(EffectError::BatchFull), effect));
        }
        self.metadata.push(MetadataEffect {
            value: Some(effect),
            completed: false,
            forwarded: None,
            credit_waiter: None,
        });
        Ok(())
    }
    pub(crate) fn set_failure_work(&mut self, work: Option<ScopedWorkRef>) {
        self.failure_work = work;
    }
    /// Immutable registration metadata, independent of the suspect core. The
    /// registration owner retains this admitted cleanup lease until removal.
    pub fn failure_work(&self) -> Option<ScopedWorkRef> {
        self.failure_work
    }
    /// Stage a stop of new business starts in this step's original scope.
    /// The gate changes only when the driver commits the whole normal step.
    /// This neither signals running tasks nor observes any work or capture exit.
    pub fn stop_producing(&mut self) {
        assert!(
            !self.retained,
            "retained state cannot stop its process payer"
        );
        self.stop_producing = true;
    }
    /// Select only the original continuation Source and this actor's own
    /// source (including its timer). Other sources retain their paid queued
    /// heads and within-source FIFO. None reopens all sources. This changes
    /// selection at atomic step commitment, never admission or resource rights.
    /// Callers keep protocol ingress distinct from these continuation sources;
    /// changing a Source identity to bypass its earlier input is invalid.
    pub fn select_input_source(&mut self, source: Option<ActorId>) {
        self.input_source = Some(source);
    }
    /// Transfer a still-unbound synchronous reservation into the same atomic
    /// handoff as its neutral effect. No queue/capture may bind it beforehand.
    /// An unwinding step rolls these pure guards back; a committed batch keeps
    /// the originals until its actual host or failure custodian takes them.
    pub fn reservation(
        &mut self,
        guard: crate::obligation::StepRegistration,
    ) -> Result<(), (ReservationHandoffError, crate::obligation::StepRegistration)> {
        let error = if self.retained || !guard.belongs_to(self.scope) {
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
    /// effect does not create a WorkObligation, payload, execution credit or exit fact.
    pub fn effect(
        &mut self,
        descriptor: EffectDescriptor,
        effect: E,
    ) -> Result<(), (EffectEmissionError, E)> {
        let author = self
            .input_author
            .as_ref()
            .filter(|author| author.work() == descriptor.work)
            .cloned();
        self.effect_inner(author, descriptor, effect)
    }
    /// A delayed reply must borrow its original admitted author binding.
    pub fn effect_from_author(
        &mut self,
        author: &crate::actor_turn::InputAuthor,
        descriptor: EffectDescriptor,
        effect: E,
    ) -> Result<(), (EffectEmissionError, E)> {
        if descriptor.work != author.work() {
            return Err((EffectEmissionError::Unfunded, effect));
        }
        self.effect_inner(Some(author.clone()), descriptor, effect)
    }
    fn effect_inner(
        &mut self,
        author: Option<crate::actor_turn::InputAuthor>,
        descriptor: EffectDescriptor,
        effect: E,
    ) -> Result<(), (EffectEmissionError, E)> {
        if (descriptor.work.scope != self.scope.id() && author.is_none())
            || (self.retained && (author.is_none() || descriptor.kind == EffectKind::Business))
        {
            return Err((EffectEmissionError::Unfunded, effect));
        }
        if !self.charge(1) {
            return Err((EffectEmissionError::Yield, effect));
        }
        if !self.metadata.is_empty()
            && self.metadata.len() + self.payloads.len() >= self.max_reservations
        {
            return Err((EffectEmissionError::Invalid(EffectError::BatchFull), effect));
        }
        let id = descriptor.id;
        if let Err(error) = self.batch.push(descriptor) {
            return Err((EffectEmissionError::Invalid(error), effect));
        }
        if let Some(author) = author {
            self.authors.insert(id, author);
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
            metadata: self.metadata,
            authors: self.authors,
            stop_producing: self.stop_producing,
            input_source: self.input_source,
        }
    }
}
pub(crate) struct MetadataEffect<E> {
    pub(crate) value: Option<E>,
    pub(crate) completed: bool,
    pub(crate) forwarded: Option<crate::actor_turn::MetadataDelivery>,
    pub(crate) credit_waiter: Option<crate::actor_turn::MetadataCreditWait>,
}
pub(crate) struct StepEnvelope<E> {
    pub(crate) ordinal: Option<BatchId>,
    pub(crate) stop_producing: bool,
    pub(crate) input_source: Option<Option<ActorId>>,
    pub(crate) state: EffectBatchState,
    pub(crate) payloads:
        std::collections::BTreeMap<novarocks_control_runtime_core::identity::ActionId, E>,
    pub(crate) reservations:
        std::collections::BTreeMap<ReservationId, crate::obligation::StepRegistration>,
    pub(crate) metadata: Vec<MetadataEffect<E>>,
    pub(crate) authors: std::collections::BTreeMap<
        novarocks_control_runtime_core::identity::ActionId,
        crate::actor_turn::InputAuthor,
    >,
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
    Unfunded,
    Invalid(EffectError),
}

#[derive(Clone, Copy, Debug)]
pub struct ActorConfig {
    pub mailbox: [usize; 4],
    /// Normal resident batches. IsolateScope also owns one separately funded
    /// builtin supervisor batch, unavailable to ordinary mailbox dispatch.
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
