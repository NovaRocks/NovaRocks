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

//! Bounded work scheduling, with at most one running item per group.
//! Fairness keys are independent of the original owner-qualified job keys.

use crate::identity::{ScopeId, WorkId};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FairQueueError {
    Full,
    ContextFull,
    DuplicateWork,
    UnknownWork,
    WrongRunningWork,
    ContextCapacity,
    OrderExhausted,
}
#[derive(Debug)]
struct GroupQueue<W> {
    head: Option<W>,
    tail: Option<W>,
    queued: usize,
    running: Option<W>,
    ready_order: Option<u64>,
}
impl<W> Default for GroupQueue<W> {
    fn default() -> Self {
        Self {
            head: None,
            tail: None,
            queued: 0,
            running: None,
            ready_order: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct WorkNode<W> {
    previous: Option<W>,
    next: Option<W>,
    running: bool,
    enqueue_order: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Discipline {
    RoundRobin,
    EligibleFifo,
}
/// A finite FIFO per complete fairness group, independent of job ownership.
/// Membership is unique by `(G, W)`, preserving scope-local WorkIds in the
/// default API. When a group covers multiple scopes, W retains the original
/// owner-qualified job identity. Keys are neutral deterministic values.
/// The default discipline rotates eligible groups after each body. The
/// `eligible_fifo` constructor instead preserves each head's enqueue priority.
#[derive(Debug)]
pub struct FairWorkQueue<G = ScopeId, W = WorkId> {
    max_groups: usize,
    max_work: usize,
    per_group: usize,
    groups: BTreeMap<G, GroupQueue<W>>,
    ready: BTreeSet<(u64, G)>,
    // Default WorkIds are local to their ScopeId queue. Callers with another
    // fairness key retain the complete original owner identity in W instead.
    work: BTreeMap<(G, W), WorkNode<W>>,
    next_order: u64,
    discipline: Discipline,
}
impl<G: Copy + Ord + Debug, W: Copy + Ord + Debug> FairWorkQueue<G, W> {
    pub fn new(max_groups: usize, max_work: usize, per_group: usize) -> Self {
        Self::with_discipline(max_groups, max_work, per_group, Discipline::RoundRobin)
    }
    /// Select the oldest enqueued head among groups without a running item.
    /// Finishing a body restores its queued head's original priority.
    pub fn eligible_fifo(max_groups: usize, max_work: usize, per_group: usize) -> Self {
        Self::with_discipline(max_groups, max_work, per_group, Discipline::EligibleFifo)
    }
    fn with_discipline(
        max_groups: usize,
        max_work: usize,
        per_group: usize,
        discipline: Discipline,
    ) -> Self {
        Self {
            max_groups,
            max_work,
            per_group,
            groups: BTreeMap::new(),
            ready: BTreeSet::new(),
            work: BTreeMap::new(),
            next_order: 1,
            discipline,
        }
    }
    pub fn len(&self) -> usize {
        self.work.len()
    }
    pub fn is_empty(&self) -> bool {
        self.work.is_empty()
    }
    /// Read existing queue membership without creating a scheduling position.
    pub fn contains(&self, group: G, work: W) -> bool {
        self.work.contains_key(&(group, work))
    }
    pub fn enqueue(&mut self, group: G, work: W) -> Result<(), FairQueueError> {
        if self.work.contains_key(&(group, work)) {
            return Err(FairQueueError::DuplicateWork);
        }
        if self.work.len() >= self.max_work {
            return Err(FairQueueError::Full);
        }
        if !self.groups.contains_key(&group) && self.groups.len() >= self.max_groups {
            return Err(FairQueueError::ContextCapacity);
        }
        if let Some(queue) = self.groups.get(&group) {
            if queue.queued + usize::from(queue.running.is_some()) >= self.per_group {
                return Err(FairQueueError::ContextFull);
            }
        } else if self.per_group == 0 {
            return Err(FairQueueError::ContextFull);
        }
        let ready = self
            .groups
            .get(&group)
            .is_none_or(|c| c.running.is_none() && c.ready_order.is_none());
        let order = self.next_order;
        let next_order = if ready || self.discipline == Discipline::EligibleFifo {
            order.checked_add(1).ok_or(FairQueueError::OrderExhausted)?
        } else {
            order
        };
        let queue = self.groups.entry(group).or_default();
        let previous = queue.tail;
        self.work.insert(
            (group, work),
            WorkNode {
                previous,
                next: None,
                running: false,
                enqueue_order: order,
            },
        );
        if let Some(tail) = previous {
            self.work
                .get_mut(&(group, tail))
                .expect("queued tail exists")
                .next = Some(work);
        } else {
            queue.head = Some(work);
        }
        queue.tail = Some(work);
        queue.queued += 1;
        if ready {
            queue.ready_order = Some(order);
            self.ready.insert((order, group));
        }
        self.next_order = next_order;
        Ok(())
    }
    pub fn pop(&mut self) -> Option<(G, W)> {
        let (order, group) = self.ready.pop_first()?;
        let queue = self.groups.get_mut(&group).expect("eligible group exists");
        assert_eq!(queue.ready_order, Some(order));
        assert!(queue.running.is_none(), "one running item per group");
        queue.ready_order = None;
        let work = queue.head.expect("eligible group has queued work");
        let node = self.work.get_mut(&(group, work)).expect("head work exists");
        node.running = true;
        let next = node.next;
        node.next = None;
        queue.head = next;
        queue.queued -= 1;
        if let Some(next) = next {
            self.work
                .get_mut(&(group, next))
                .expect("next work exists")
                .previous = None;
        } else {
            queue.tail = None;
        }
        queue.running = Some(work);
        Some((group, work))
    }
    /// Body exit returns the worker slot; the resource lease may remain live.
    pub fn finish(&mut self, group: G, work: W) -> Result<(), FairQueueError> {
        let queue = self.groups.get(&group).ok_or(FairQueueError::UnknownWork)?;
        if queue.running != Some(work) {
            return Err(FairQueueError::WrongRunningWork);
        }
        let next_order = if queue.queued > 0 && self.discipline == Discipline::RoundRobin {
            self.next_order
                .checked_add(1)
                .ok_or(FairQueueError::OrderExhausted)?
        } else {
            self.next_order
        };
        let queue = self.groups.get_mut(&group).expect("validated group");
        queue.running = None;
        self.work.remove(&(group, work));
        if queue.queued > 0 {
            let head = queue.head.expect("queued group has a head");
            let order = match self.discipline {
                Discipline::EligibleFifo => {
                    self.work
                        .get(&(group, head))
                        .expect("queued head exists")
                        .enqueue_order
                }
                Discipline::RoundRobin => self.next_order,
            };
            queue.ready_order = Some(order);
            self.ready.insert((order, group));
            self.next_order = next_order;
        } else {
            self.groups.remove(&group);
        }
        Ok(())
    }
    /// Indexed cancellation avoids scanning sibling groups or queued work.
    pub fn remove_queued(&mut self, group: G, work: W) -> Result<(), FairQueueError> {
        let node = self
            .work
            .get(&(group, work))
            .copied()
            .ok_or(FairQueueError::UnknownWork)?;
        if node.running {
            return Err(FairQueueError::UnknownWork);
        }
        let queue = self.groups.get_mut(&group).expect("work retains group");
        // Removing an eligible head must not lend its old priority to the
        // next item. A running group has no ready entry to replace.
        let replace_ready = self.discipline == Discipline::EligibleFifo
            && node.previous.is_none()
            && queue.running.is_none();
        if replace_ready {
            let order = queue.ready_order.take().expect("queued group is eligible");
            assert!(self.ready.remove(&(order, group)));
        }
        match node.previous {
            Some(previous) => {
                self.work
                    .get_mut(&(group, previous))
                    .expect("previous exists")
                    .next = node.next
            }
            None => queue.head = node.next,
        }
        match node.next {
            Some(next) => {
                self.work
                    .get_mut(&(group, next))
                    .expect("next exists")
                    .previous = node.previous
            }
            None => queue.tail = node.previous,
        }
        queue.queued -= 1;
        self.work.remove(&(group, work));
        if queue.queued == 0 {
            if let Some(order) = queue.ready_order.take() {
                self.ready.remove(&(order, group));
            }
            if queue.running.is_none() {
                self.groups.remove(&group);
            }
        } else if replace_ready {
            let head = queue.head.expect("remaining queued head exists");
            let order = self
                .work
                .get(&(group, head))
                .expect("remaining head work exists")
                .enqueue_order;
            queue.ready_order = Some(order);
            self.ready.insert((order, group));
        }
        Ok(())
    }
    pub fn running(&self, group: G) -> Option<W> {
        self.groups.get(&group).and_then(|c| c.running)
    }
}
