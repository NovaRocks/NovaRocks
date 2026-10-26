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

//! Bounded context-fair work, with at most one running item per context.

use crate::identity::{ScopeId, WorkId};
use std::collections::{BTreeMap, BTreeSet};

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
#[derive(Debug, Default)]
struct ContextQueue {
    head: Option<WorkId>,
    tail: Option<WorkId>,
    queued: usize,
    running: Option<WorkId>,
    ready_order: Option<u64>,
}
#[derive(Clone, Copy, Debug)]
struct WorkNode {
    previous: Option<WorkId>,
    next: Option<WorkId>,
    running: bool,
}
#[derive(Debug)]
pub struct FairWorkQueue {
    max_contexts: usize,
    max_work: usize,
    per_context: usize,
    contexts: BTreeMap<ScopeId, ContextQueue>,
    ready: BTreeSet<(u64, ScopeId)>,
    // Work identities are minted independently by each scope. Global queue
    // membership must retain the complete owner identity, also for removal.
    work: BTreeMap<(ScopeId, WorkId), WorkNode>,
    next_order: u64,
}
impl FairWorkQueue {
    pub fn new(max_contexts: usize, max_work: usize, per_context: usize) -> Self {
        Self {
            max_contexts,
            max_work,
            per_context,
            contexts: BTreeMap::new(),
            ready: BTreeSet::new(),
            work: BTreeMap::new(),
            next_order: 1,
        }
    }
    pub fn len(&self) -> usize {
        self.work.len()
    }
    pub fn is_empty(&self) -> bool {
        self.work.is_empty()
    }
    pub fn enqueue(&mut self, scope: ScopeId, work: WorkId) -> Result<(), FairQueueError> {
        if self.work.contains_key(&(scope, work)) {
            return Err(FairQueueError::DuplicateWork);
        }
        if self.work.len() >= self.max_work {
            return Err(FairQueueError::Full);
        }
        if !self.contexts.contains_key(&scope) && self.contexts.len() >= self.max_contexts {
            return Err(FairQueueError::ContextCapacity);
        }
        if let Some(context) = self.contexts.get(&scope) {
            if context.queued + usize::from(context.running.is_some()) >= self.per_context {
                return Err(FairQueueError::ContextFull);
            }
        } else if self.per_context == 0 {
            return Err(FairQueueError::ContextFull);
        }
        let ready = self
            .contexts
            .get(&scope)
            .is_none_or(|c| c.running.is_none() && c.ready_order.is_none());
        let next_order = if ready {
            self.next_order
                .checked_add(1)
                .ok_or(FairQueueError::OrderExhausted)?
        } else {
            self.next_order
        };
        let context = self.contexts.entry(scope).or_default();
        let previous = context.tail;
        self.work.insert(
            (scope, work),
            WorkNode {
                previous,
                next: None,
                running: false,
            },
        );
        if let Some(tail) = previous {
            self.work
                .get_mut(&(scope, tail))
                .expect("queued tail exists")
                .next = Some(work);
        } else {
            context.head = Some(work);
        }
        context.tail = Some(work);
        context.queued += 1;
        if ready {
            context.ready_order = Some(self.next_order);
            self.ready.insert((self.next_order, scope));
            self.next_order = next_order;
        }
        Ok(())
    }
    pub fn pop(&mut self) -> Option<(ScopeId, WorkId)> {
        let (order, scope) = self.ready.pop_first()?;
        let context = self
            .contexts
            .get_mut(&scope)
            .expect("eligible context exists");
        assert_eq!(context.ready_order, Some(order));
        assert!(context.running.is_none(), "one running item per context");
        context.ready_order = None;
        let work = context.head.expect("eligible context has queued work");
        let node = self.work.get_mut(&(scope, work)).expect("head work exists");
        node.running = true;
        let next = node.next;
        node.next = None;
        context.head = next;
        context.queued -= 1;
        if let Some(next) = next {
            self.work
                .get_mut(&(scope, next))
                .expect("next work exists")
                .previous = None;
        } else {
            context.tail = None;
        }
        context.running = Some(work);
        Some((scope, work))
    }
    /// Body exit returns the worker slot; the resource lease may remain live.
    pub fn finish(&mut self, scope: ScopeId, work: WorkId) -> Result<(), FairQueueError> {
        let context = self
            .contexts
            .get(&scope)
            .ok_or(FairQueueError::UnknownWork)?;
        if context.running != Some(work) {
            return Err(FairQueueError::WrongRunningWork);
        }
        let next_order = if context.queued > 0 {
            self.next_order
                .checked_add(1)
                .ok_or(FairQueueError::OrderExhausted)?
        } else {
            self.next_order
        };
        let context = self.contexts.get_mut(&scope).expect("validated context");
        context.running = None;
        self.work.remove(&(scope, work));
        if context.queued > 0 {
            context.ready_order = Some(self.next_order);
            self.ready.insert((self.next_order, scope));
            self.next_order = next_order;
        } else {
            self.contexts.remove(&scope);
        }
        Ok(())
    }
    /// Indexed cancellation avoids scanning sibling contexts or queued work.
    pub fn remove_queued(&mut self, scope: ScopeId, work: WorkId) -> Result<(), FairQueueError> {
        let node = self
            .work
            .get(&(scope, work))
            .copied()
            .ok_or(FairQueueError::UnknownWork)?;
        if node.running {
            return Err(FairQueueError::UnknownWork);
        }
        let context = self.contexts.get_mut(&scope).expect("work retains context");
        match node.previous {
            Some(previous) => {
                self.work
                    .get_mut(&(scope, previous))
                    .expect("previous exists")
                    .next = node.next
            }
            None => context.head = node.next,
        }
        match node.next {
            Some(next) => {
                self.work
                    .get_mut(&(scope, next))
                    .expect("next exists")
                    .previous = node.previous
            }
            None => context.tail = node.previous,
        }
        context.queued -= 1;
        self.work.remove(&(scope, work));
        if context.queued == 0 {
            if let Some(order) = context.ready_order.take() {
                self.ready.remove(&(order, scope));
            }
            if context.running.is_none() {
                self.contexts.remove(&scope);
            }
        }
        Ok(())
    }
    pub fn running(&self, scope: ScopeId) -> Option<WorkId> {
        self.contexts.get(&scope).and_then(|c| c.running)
    }
}
