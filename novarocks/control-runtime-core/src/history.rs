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

//! History records are observations, never inputs to scheduling or policy.

use crate::identity::{
    ActionId, ActorId, Generation, InputId, ReservationId, ScopeId, SlotId, Tick, WorkId,
};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryFact {
    CapacitySplit {
        from: ReservationId,
        to: ReservationId,
    },
    UnboundReservationRolledBack {
        scope: ScopeId,
        work: WorkId,
        reservation: ReservationId,
    },
    ScopeOpened {
        scope: ScopeId,
        generation: Generation,
    },
    ScopeClosed {
        scope: ScopeId,
    },
    ScopeStopped {
        scope: ScopeId,
    },
    WorkAdmitted {
        scope: ScopeId,
        work: WorkId,
    },
    WorkRetired {
        scope: ScopeId,
        work: WorkId,
    },
    SlotReserved {
        scope: ScopeId,
        slot: SlotId,
    },
    SlotFilled {
        slot: SlotId,
    },
    SlotAbandoned {
        slot: SlotId,
    },
    SlotRetired {
        slot: SlotId,
    },
    Admitted {
        scope: ScopeId,
        work: WorkId,
        reservation: ReservationId,
    },
    Transferred {
        work: WorkId,
        from: ScopeId,
        to: ScopeId,
    },
    Claimed {
        actor: ActorId,
    },
    InputConsumed {
        actor: ActorId,
        input: InputId,
    },
    BatchCommitted {
        scope: ScopeId,
        input: InputId,
    },
    Published {
        scope: ScopeId,
        generation: Generation,
        action: ActionId,
    },
    Started {
        scope: ScopeId,
        work: WorkId,
    },
    BodyExited {
        scope: ScopeId,
        work: WorkId,
    },
    CapturesExited {
        scope: ScopeId,
        work: WorkId,
    },
    CompletionConsumed {
        slot: SlotId,
    },
    OwnerFailed {
        scope: ScopeId,
        generation: Generation,
    },
    CapacityReleased {
        reservation: ReservationId,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryEntry {
    pub ordinal: u64,
    pub now: Tick,
    pub fact: HistoryFact,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryError {
    Full,
    OrdinalExhausted,
    TimeWentBackwards,
}
#[derive(Debug)]
pub struct HistoryBuffer {
    limit: usize,
    next_ordinal: u64,
    now: Tick,
    entries: VecDeque<HistoryEntry>,
}
impl HistoryBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            next_ordinal: 1,
            now: Tick(0),
            entries: VecDeque::new(),
        }
    }
    /// Full buffers are drained by the observer; history pressure never changes policy.
    /// Drivers may disable collection instead, but cannot silently claim a complete history.
    pub fn record(&mut self, now: Tick, fact: HistoryFact) -> Result<(), HistoryError> {
        if now < self.now {
            return Err(HistoryError::TimeWentBackwards);
        }
        if self.entries.len() >= self.limit {
            return Err(HistoryError::Full);
        }
        let next = self
            .next_ordinal
            .checked_add(1)
            .ok_or(HistoryError::OrdinalExhausted)?;
        self.entries.push_back(HistoryEntry {
            ordinal: self.next_ordinal,
            now,
            fact,
        });
        self.now = now;
        self.next_ordinal = next;
        Ok(())
    }
    pub fn pop(&mut self) -> Option<HistoryEntry> {
        self.entries.pop_front()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
