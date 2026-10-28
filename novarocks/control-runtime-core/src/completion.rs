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
/// One payload in this original report position, independent of its consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReturnState {
    Unclaimed,
    Reserved,
    Filled,
    Taken,
    RevokedUnused,
    Unknown,
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
    ReturnAlreadyClaimed,
    InvalidReturnClaim,
    InvalidReturnState(ReturnState),
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
    returned: ReturnState,
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
            returned: ReturnState::Unclaimed,
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
    pub const fn return_state(&self) -> ReturnState {
        self.returned
    }
    /// The wrapper also verifies the original producer is bound, not started.
    pub fn claim_return(&mut self) -> Result<(), CompletionError> {
        if self.returned != ReturnState::Unclaimed {
            return Err(CompletionError::ReturnAlreadyClaimed);
        }
        if self.state != SlotState::Reserved || self.body_exited || self.captures_exited {
            return Err(CompletionError::InvalidReturnClaim);
        }
        self.returned = ReturnState::Reserved;
        Ok(())
    }
    pub fn fill_return(&mut self) -> Result<(), CompletionError> {
        if self.returned != ReturnState::Reserved {
            return Err(CompletionError::InvalidReturnState(self.returned));
        }
        self.returned = ReturnState::Filled;
        Ok(())
    }
    pub fn take_return(&mut self) -> Result<(), CompletionError> {
        if self.returned != ReturnState::Filled {
            return Err(CompletionError::InvalidReturnState(self.returned));
        }
        self.returned = ReturnState::Taken;
        Ok(())
    }
    /// This revokes only an unfilled payload, not the role or any consumer.
    pub fn revoke_return_unused(&mut self) -> Result<(), CompletionError> {
        if self.returned != ReturnState::Reserved {
            return Err(CompletionError::InvalidReturnState(self.returned));
        }
        self.returned = ReturnState::RevokedUnused;
        Ok(())
    }
    /// Lost custody is sticky. A later producer must return its own value.
    pub fn lose_return(&mut self) -> bool {
        if matches!(self.returned, ReturnState::Reserved | ReturnState::Filled) {
            self.returned = ReturnState::Unknown;
            true
        } else {
            false
        }
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
            && matches!(
                self.returned,
                ReturnState::Unclaimed | ReturnState::Taken | ReturnState::RevokedUnused
            )
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
