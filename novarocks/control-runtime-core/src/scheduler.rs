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

//! Coalesced readiness and per-class turn budgets are shared by every driver.

use crate::identity::ActorId;
use crate::mailbox::InputClass;
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerError {
    Full,
    DuplicateActor,
    UnknownActor,
    AlreadyClaimed,
    NotClaimed,
    Busy,
    BudgetTooSmall,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ActorReadiness {
    claimed: bool,
    queued: bool,
    pending: bool,
}
#[derive(Debug)]
pub struct ReadyQueue {
    limit: usize,
    actors: BTreeMap<ActorId, ActorReadiness>,
    ready: VecDeque<ActorId>,
}
impl ReadyQueue {
    pub fn new(max_actors: usize) -> Self {
        Self {
            limit: max_actors,
            actors: BTreeMap::new(),
            ready: VecDeque::new(),
        }
    }
    pub fn register(&mut self, actor: ActorId) -> Result<(), SchedulerError> {
        if self.actors.contains_key(&actor) {
            return Err(SchedulerError::DuplicateActor);
        }
        if self.actors.len() >= self.limit {
            return Err(SchedulerError::Full);
        }
        self.actors.insert(actor, ActorReadiness::default());
        Ok(())
    }
    pub fn unregister(&mut self, actor: ActorId) -> Result<(), SchedulerError> {
        let state = self
            .actors
            .get(&actor)
            .ok_or(SchedulerError::UnknownActor)?;
        if state.claimed || state.queued || state.pending {
            return Err(SchedulerError::Busy);
        }
        self.actors.remove(&actor);
        Ok(())
    }
    pub fn notify(&mut self, actor: ActorId) -> Result<(), SchedulerError> {
        let state = self
            .actors
            .get_mut(&actor)
            .ok_or(SchedulerError::UnknownActor)?;
        state.pending = true;
        if !state.claimed && !state.queued {
            state.queued = true;
            self.ready.push_back(actor);
        }
        Ok(())
    }
    pub fn claim(&mut self) -> Option<ActorId> {
        let actor = self.ready.pop_front()?;
        let state = self.actors.get_mut(&actor).expect("queued actor exists");
        assert!(!state.claimed, "one poller owns an actor");
        state.queued = false;
        state.claimed = true;
        state.pending = false;
        Some(actor)
    }
    /// Notification racing the end of a turn remains pending under the driver's lock.
    pub fn release(&mut self, actor: ActorId, has_pending: bool) -> Result<(), SchedulerError> {
        let state = self
            .actors
            .get_mut(&actor)
            .ok_or(SchedulerError::UnknownActor)?;
        if !state.claimed {
            return Err(SchedulerError::NotClaimed);
        }
        state.claimed = false;
        state.pending |= has_pending;
        if state.pending && !state.queued {
            state.queued = true;
            self.ready.push_back(actor);
        }
        Ok(())
    }
    /// Remove the caller's claimed scheduling position after its real actor
    /// has independently satisfied retirement. Pending marks carry no input,
    /// Work or exit fact, so they do not keep a retired actor address alive.
    /// The claim already removed its sole FIFO entry.
    pub fn unregister_claimed(&mut self, actor: ActorId) -> Result<(), SchedulerError> {
        let state = self
            .actors
            .get(&actor)
            .ok_or(SchedulerError::UnknownActor)?;
        if !state.claimed {
            return Err(SchedulerError::NotClaimed);
        }
        if state.queued {
            return Err(SchedulerError::Busy);
        }
        self.actors.remove(&actor);
        Ok(())
    }
    /// Used to validate multi-queue retirement before mutating either index.
    pub fn is_idle(&self, actor: ActorId) -> Result<bool, SchedulerError> {
        self.actors
            .get(&actor)
            .map(|state| !state.claimed && !state.queued && !state.pending)
            .ok_or(SchedulerError::UnknownActor)
    }
    pub fn is_claimed(&self, actor: ActorId) -> Result<bool, SchedulerError> {
        self.actors
            .get(&actor)
            .map(|s| s.claimed)
            .ok_or(SchedulerError::UnknownActor)
    }
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }
    pub fn actor_count(&self) -> usize {
        self.actors.len()
    }
}

/// One positive opportunity per class followed by round-robin excess work.
/// Call `charge` for each traversal/effect as well as each consumed input.
#[derive(Clone, Debug)]
pub struct TurnBudget {
    total: usize,
    remaining: usize,
    guarantees: [bool; 4],
    cursor: usize,
}
impl TurnBudget {
    pub fn new(total: usize) -> Result<Self, SchedulerError> {
        if total < InputClass::ALL.len() {
            return Err(SchedulerError::BudgetTooSmall);
        }
        Ok(Self {
            total,
            remaining: total,
            guarantees: [true; 4],
            cursor: 0,
        })
    }
    pub const fn remaining(&self) -> usize {
        self.remaining
    }
    /// Returns only the unused part of a mechanically prepaid step. Class
    /// guarantees stay consumed, so excess work cannot replace their order.
    pub fn return_unused(&mut self, units: usize) {
        self.remaining = self
            .remaining
            .checked_add(units)
            .filter(|remaining| *remaining <= self.total)
            .expect("step budget refund cannot exceed its original turn");
    }
    pub fn charge(&mut self, units: usize) -> bool {
        let reserved = self.guarantees.iter().filter(|pending| **pending).count();
        if units > self.remaining.saturating_sub(reserved) {
            return false;
        }
        self.remaining -= units;
        true
    }
    /// Selecting consumes one unit. Availability must describe causal-ready heads.
    pub fn next(&mut self, available: [bool; 4]) -> Option<InputClass> {
        if self.remaining == 0 {
            return None;
        }
        for (index, is_available) in available.iter().enumerate() {
            if self.guarantees[index] && *is_available {
                self.guarantees[index] = false;
                self.remaining -= 1;
                return Some(InputClass::ALL[index]);
            }
        }
        for offset in 0..4 {
            let index = (self.cursor + offset) % 4;
            if available[index] {
                self.cursor = (index + 1) % 4;
                self.remaining -= 1;
                return Some(InputClass::ALL[index]);
            }
        }
        None
    }
}
