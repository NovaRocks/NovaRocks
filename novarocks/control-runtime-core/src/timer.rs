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

//! One mutable deadline node and one coalesced tick per actor.

use crate::identity::{ActorId, Tick};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerError {
    TimeWentBackwards,
}
#[derive(Debug)]
pub struct TimerIndex {
    now: Tick,
    by_actor: BTreeMap<ActorId, Tick>,
    by_deadline: BTreeSet<(Tick, ActorId)>,
    pending: BTreeSet<ActorId>,
    deferred_due: BTreeSet<ActorId>,
}
impl Default for TimerIndex {
    fn default() -> Self {
        Self::new()
    }
}
impl TimerIndex {
    pub fn new() -> Self {
        Self {
            now: Tick(0),
            by_actor: BTreeMap::new(),
            by_deadline: BTreeSet::new(),
            pending: BTreeSet::new(),
            deferred_due: BTreeSet::new(),
        }
    }
    pub fn set(&mut self, actor: ActorId, deadline: Tick) -> Result<(), TimerError> {
        self.deferred_due.remove(&actor);
        if let Some(old) = self.by_actor.insert(actor, deadline) {
            self.by_deadline.remove(&(old, actor));
        }
        self.by_deadline.insert((deadline, actor));
        Ok(())
    }
    pub fn cancel(&mut self, actor: ActorId) {
        self.deferred_due.remove(&actor);
        if let Some(old) = self.by_actor.remove(&actor) {
            self.by_deadline.remove(&(old, actor));
        }
        // A tick already handed off remains pending until acknowledged.
    }
    pub fn next_deadline(&self) -> Option<Tick> {
        self.by_deadline.first().map(|(deadline, _)| *deadline)
    }
    pub fn advance(&mut self, now: Tick, budget: usize) -> Result<Vec<ActorId>, TimerError> {
        if now < self.now {
            return Err(TimerError::TimeWentBackwards);
        }
        self.now = now;
        let mut ready = Vec::new();
        for _ in 0..budget {
            let Some((deadline, actor)) = self.by_deadline.first().copied() else {
                break;
            };
            if deadline > now {
                break;
            }
            self.by_deadline.remove(&(deadline, actor));
            if self.pending.contains(&actor) {
                // A newer deadline can expire while an older tick is still
                // in flight. Retain the newer node until that tick retires.
                // Removing it from the due index lets other actors advance.
                self.deferred_due.insert(actor);
            } else {
                self.by_actor.remove(&actor);
                self.pending.insert(actor);
                ready.push(actor);
            }
        }
        Ok(ready)
    }
    pub fn acknowledge_tick(&mut self, actor: ActorId) -> bool {
        let removed = self.pending.remove(&actor);
        if self.deferred_due.remove(&actor) {
            assert!(removed, "a deferred deadline retains its pending tick");
            let deadline = *self.by_actor.get(&actor).expect("deferred deadline exists");
            self.by_deadline.insert((deadline, actor));
        }
        removed
    }
    pub fn tick_pending(&self, actor: ActorId) -> bool {
        self.pending.contains(&actor)
    }
    pub fn deadline_count(&self) -> usize {
        self.by_actor.len()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}
