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

//! Reserved reports survive work exit until every declared consumer retires.

use crate::identity::{ConsumerId, Generation, ScopeId, SlotId, WorkId};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerKind {
    Owner {
        scope: ScopeId,
        generation: Generation,
    },
    Observer,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Consumption {
    Pending,
    Consumed,
    AbandonedByOwnerFailure,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    Reserved,
    Filled,
    RevokedUnused,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionError {
    TooManyConsumers,
    DuplicateConsumer,
    UnknownConsumer,
    AlreadyFilled,
    NotFilled,
    AlreadyRetired,
    InvalidRevocation,
}
#[derive(Debug)]
pub struct CompletionSlot {
    id: SlotId,
    scope: ScopeId,
    generation: Generation,
    producer: WorkId,
    state: SlotState,
    body_exited: bool,
    captures_exited: bool,
    consumers: BTreeMap<ConsumerId, (ConsumerKind, Consumption)>,
}
impl CompletionSlot {
    pub fn new(
        id: SlotId,
        scope: ScopeId,
        generation: Generation,
        producer: WorkId,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, CompletionError> {
        let mut entries = BTreeMap::new();
        for (id, kind) in consumers {
            if entries.contains_key(&id) {
                return Err(CompletionError::DuplicateConsumer);
            }
            if entries.len() >= max_consumers {
                return Err(CompletionError::TooManyConsumers);
            }
            entries.insert(id, (kind, Consumption::Pending));
        }
        Ok(Self {
            id,
            scope,
            generation,
            producer,
            state: SlotState::Reserved,
            body_exited: false,
            captures_exited: false,
            consumers: entries,
        })
    }
    pub const fn id(&self) -> SlotId {
        self.id
    }
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    pub const fn producer(&self) -> WorkId {
        self.producer
    }
    pub const fn state(&self) -> SlotState {
        self.state
    }
    pub fn fill(&mut self) -> Result<(), CompletionError> {
        if self.state != SlotState::Reserved {
            return Err(CompletionError::AlreadyFilled);
        }
        self.state = SlotState::Filled;
        Ok(())
    }
    pub fn consume(&mut self, consumer: ConsumerId) -> Result<(), CompletionError> {
        if self.state != SlotState::Filled {
            return Err(CompletionError::NotFilled);
        }
        let (_, state) = self
            .consumers
            .get_mut(&consumer)
            .ok_or(CompletionError::UnknownConsumer)?;
        if *state != Consumption::Pending {
            return Err(CompletionError::AlreadyRetired);
        }
        *state = Consumption::Consumed;
        Ok(())
    }
    /// Failure authorizes only the failed owner's consumption leg to retire.
    pub fn abandon_owner(
        &mut self,
        consumer: ConsumerId,
        scope: ScopeId,
        generation: Generation,
    ) -> Result<(), CompletionError> {
        let (kind, state) = self
            .consumers
            .get_mut(&consumer)
            .ok_or(CompletionError::UnknownConsumer)?;
        if *kind != (ConsumerKind::Owner { scope, generation }) {
            return Err(CompletionError::UnknownConsumer);
        }
        if *state != Consumption::Pending {
            return Err(CompletionError::AlreadyRetired);
        }
        *state = Consumption::AbandonedByOwnerFailure;
        Ok(())
    }
    pub fn consumption(&self, consumer: ConsumerId) -> Option<Consumption> {
        self.consumers.get(&consumer).map(|(_, s)| *s)
    }
    pub fn observe_body_exit(&mut self) {
        self.body_exited = true;
    }
    pub fn observe_captures_exit(&mut self) {
        self.captures_exited = true;
    }
    /// This is legal only before registration/publication. Exit still must be observed.
    pub fn revoke_unused(&mut self) -> Result<(), CompletionError> {
        if self.state != SlotState::Reserved {
            return Err(CompletionError::InvalidRevocation);
        }
        self.state = SlotState::RevokedUnused;
        Ok(())
    }
    pub fn retirable(&self) -> bool {
        self.body_exited
            && self.captures_exited
            && match self.state {
                SlotState::Reserved => false,
                SlotState::RevokedUnused => true,
                SlotState::Filled => self
                    .consumers
                    .values()
                    .all(|(_, s)| *s != Consumption::Pending),
            }
    }
}
