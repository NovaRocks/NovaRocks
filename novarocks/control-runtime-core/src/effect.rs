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

//! Atomic batch custody does not make external effects atomic.

use crate::identity::{ActionId, Generation, InputId, ScopeId, WorkId};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectKind {
    Business,
    Publication,
    Reply,
    Cleanup,
    Settlement,
}
impl EffectKind {
    pub const fn allowed_after_failure(self) -> bool {
        matches!(self, Self::Cleanup | Self::Settlement)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectState {
    Pending,
    Started,
    Published,
    Completed,
    AbandonedByOwnerFailure,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchPhase {
    Building,
    Committed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectError {
    BatchFull,
    DuplicateAction,
    IdentityOrder,
    TooManyDependencies,
    DuplicateDependency,
    SelfDependency,
    UnknownAction,
    UnknownDependency,
    NotCommitted,
    AlreadyCommitted,
    NotReady,
    AlreadyStarted,
    InvalidTransition,
    StaleGeneration,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectDescriptor {
    pub id: ActionId,
    pub work: WorkId,
    pub kind: EffectKind,
    pub dependencies: Vec<ActionId>,
}
#[derive(Debug)]
pub struct EffectRecord {
    descriptor: EffectDescriptor,
    state: EffectState,
    dependencies: BTreeMap<ActionId, bool>,
}
impl EffectRecord {
    pub fn descriptor(&self) -> &EffectDescriptor {
        &self.descriptor
    }
    pub const fn state(&self) -> EffectState {
        self.state
    }
}
#[derive(Debug)]
pub struct EffectBatchState {
    scope: ScopeId,
    generation: Generation,
    input: InputId,
    phase: BatchPhase,
    max_effects: usize,
    max_dependencies: usize,
    last_action: u64,
    effects: BTreeMap<ActionId, EffectRecord>,
    pending: VecDeque<ActionId>,
}
impl EffectBatchState {
    pub fn new(
        scope: ScopeId,
        generation: Generation,
        input: InputId,
        max_effects: usize,
        max_dependencies: usize,
    ) -> Self {
        Self {
            scope,
            generation,
            input,
            phase: BatchPhase::Building,
            max_effects,
            max_dependencies,
            last_action: 0,
            effects: BTreeMap::new(),
            pending: VecDeque::new(),
        }
    }
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    pub const fn input(&self) -> InputId {
        self.input
    }
    pub const fn phase(&self) -> BatchPhase {
        self.phase
    }
    pub fn len(&self) -> usize {
        self.effects.len()
    }
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }
    pub fn push(&mut self, descriptor: EffectDescriptor) -> Result<(), EffectError> {
        if self.phase != BatchPhase::Building {
            return Err(EffectError::AlreadyCommitted);
        }
        if self.effects.contains_key(&descriptor.id) {
            return Err(EffectError::DuplicateAction);
        }
        if descriptor.id.get() <= self.last_action {
            return Err(EffectError::IdentityOrder);
        }
        if self.effects.len() >= self.max_effects {
            return Err(EffectError::BatchFull);
        }
        if descriptor.dependencies.len() > self.max_dependencies {
            return Err(EffectError::TooManyDependencies);
        }
        let mut dependencies = BTreeMap::new();
        for id in &descriptor.dependencies {
            if *id == descriptor.id {
                return Err(EffectError::SelfDependency);
            }
            if dependencies.insert(*id, false).is_some() {
                return Err(EffectError::DuplicateDependency);
            }
        }
        self.last_action = descriptor.id.get();
        self.pending.push_back(descriptor.id);
        self.effects.insert(
            descriptor.id,
            EffectRecord {
                descriptor,
                state: EffectState::Pending,
                dependencies,
            },
        );
        Ok(())
    }
    /// The synchronized driver also transfers resources and consumes the input here.
    pub fn commit(&mut self) -> Result<(), EffectError> {
        if self.phase != BatchPhase::Building {
            return Err(EffectError::AlreadyCommitted);
        }
        self.phase = BatchPhase::Committed;
        Ok(())
    }
    pub fn record(&self, id: ActionId) -> Option<&EffectRecord> {
        self.effects.get(&id)
    }
    pub fn dependency_published(
        &mut self,
        action: ActionId,
        predecessor: ActionId,
    ) -> Result<(), EffectError> {
        let record = self
            .effects
            .get_mut(&action)
            .ok_or(EffectError::UnknownAction)?;
        let dependency = record
            .dependencies
            .get_mut(&predecessor)
            .ok_or(EffectError::UnknownDependency)?;
        *dependency = true;
        Ok(())
    }
    pub fn ready(&self, id: ActionId, generation: Generation) -> Result<bool, EffectError> {
        if self.phase != BatchPhase::Committed {
            return Err(EffectError::NotCommitted);
        }
        let record = self.effects.get(&id).ok_or(EffectError::UnknownAction)?;
        Ok(record.state == EffectState::Pending
            && record.dependencies.values().all(|done| *done)
            && (generation == self.generation || record.descriptor.kind.allowed_after_failure()))
    }
    /// Each inspected record costs one unit; ineligible items are rotated fairly.
    pub fn poll_ready(
        &mut self,
        budget: usize,
        generation: Generation,
    ) -> Result<Vec<ActionId>, EffectError> {
        if self.phase != BatchPhase::Committed {
            return Err(EffectError::NotCommitted);
        }
        let mut ready = Vec::new();
        let count = budget.min(self.pending.len());
        for _ in 0..count {
            let id = self.pending.pop_front().expect("bounded pending count");
            if self.ready(id, generation)? {
                ready.push(id);
            }
            self.pending.push_back(id);
        }
        Ok(ready)
    }
    /// This gate is synchronized with owner failure. Starting never follows a stale business generation.
    pub fn start(&mut self, id: ActionId, generation: Generation) -> Result<(), EffectError> {
        if !self.ready(id, generation)? {
            return Err(EffectError::NotReady);
        }
        self.effects.get_mut(&id).expect("validated action").state = EffectState::Started;
        Ok(())
    }
    pub fn published(&mut self, id: ActionId) -> Result<(), EffectError> {
        let record = self
            .effects
            .get_mut(&id)
            .ok_or(EffectError::UnknownAction)?;
        if record.state != EffectState::Started {
            return Err(EffectError::InvalidTransition);
        }
        record.state = EffectState::Published;
        Ok(())
    }
    pub fn complete(&mut self, id: ActionId) -> Result<(), EffectError> {
        let record = self
            .effects
            .get_mut(&id)
            .ok_or(EffectError::UnknownAction)?;
        if !matches!(record.state, EffectState::Started | EffectState::Published) {
            return Err(EffectError::InvalidTransition);
        }
        record.state = EffectState::Completed;
        Ok(())
    }
    /// Bounded failure fanout: never rolls back a started or published effect.
    pub fn abandon_unstarted(
        &mut self,
        id: ActionId,
        current_generation: Generation,
    ) -> Result<(), EffectError> {
        if current_generation == self.generation {
            return Err(EffectError::StaleGeneration);
        }
        let record = self
            .effects
            .get_mut(&id)
            .ok_or(EffectError::UnknownAction)?;
        if record.state != EffectState::Pending || record.descriptor.kind.allowed_after_failure() {
            return Err(EffectError::InvalidTransition);
        }
        record.state = EffectState::AbandonedByOwnerFailure;
        Ok(())
    }
    pub fn all_terminal(&self) -> bool {
        self.effects.values().all(|r| {
            matches!(
                r.state,
                EffectState::Completed | EffectState::AbandonedByOwnerFailure
            )
        })
    }
}
