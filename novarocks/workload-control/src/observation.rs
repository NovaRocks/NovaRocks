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

use crate::{
    Stage, WorkClass, WorkError, WorkId, WorkScope, WorkloadControl,
    scope::{State, WorkloadConfig},
};
use std::{
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerState {
    Active,
    Orphaned,
    Completed,
}

/// Domain owners derive this stable key from the exact Task/attempt/operation
/// identity. Governance never merges identities by endpoint or query name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObligationKey(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObligationKind {
    RunningWork,
    RetiredAttempt,
    UnknownCreate,
    ExternalCompletion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageObservation {
    pub last_known_bytes: u64,
    pub observed_at: Instant,
    pub current_unknown: bool,
}

#[derive(Clone)]
pub(crate) struct ObligationRecord {
    pub generation: u64,
    pub kind: ObligationKind,
    pub usage: Option<UsageObservation>,
}

#[derive(Clone, Debug)]
pub struct ObligationSnapshot {
    pub key: ObligationKey,
    pub kind: ObligationKind,
    pub usage: Option<UsageObservation>,
}

#[derive(Clone, Debug)]
pub struct ScopeSnapshot {
    pub id: WorkId,
    pub parent: Option<WorkId>,
    pub root: WorkId,
    pub class: WorkClass,
    pub owner: OwnerState,
    pub handoffs: u64,
    pub own_completed: bool,
    pub children: usize,
    pub business_admitted: bool,
    pub occupied_stages: Vec<Stage>,
    pub restarts: usize,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
    pub resource_holders: usize,
    pub resource_waiters: usize,
    pub obligations: Vec<ObligationSnapshot>,
    pub control_pending: ControlIntents,
    pub control_inflight: bool,
}

#[derive(Clone, Debug)]
pub struct WorkloadSnapshot {
    pub serving: crate::ServingState,
    pub admission_closed: bool,
    pub root_responsibilities: usize,
    pub businesses: usize,
    pub preparation: usize,
    pub execution: usize,
    /// Includes grants not yet received; their future still owns queue memory.
    pub admission_records: usize,
    pub resource_waiters: usize,
    /// Combined stage and resource waits, governed by waiting_limit.
    pub waiting_records: usize,
    pub peak_waiting_records: usize,
    pub waiting_bytes: u64,
    pub peak_admission_records: usize,
    pub peak_waiting_bytes: u64,
    pub old_attempts: usize,
    pub unknown_creates: usize,
    pub control_ready: usize,
    pub control_inflight: usize,
    pub scopes: Vec<ScopeSnapshot>,
}

impl WorkloadControl {
    /// Recover a fact reporter for an existing obligation after a product
    /// owner/dispatcher was lost. This grants no new scheduling authority.
    pub fn recover_obligation(
        &self,
        work: WorkId,
        key: ObligationKey,
    ) -> Result<Obligation, WorkError> {
        let state = self.inner.state.lock().unwrap();
        let record = state
            .nodes
            .get(&work)
            .and_then(|node| node.obligations.get(&key))
            .ok_or(WorkError::Released)?;
        Ok(Obligation {
            scope: WorkScope {
                inner: Arc::clone(&self.inner),
                id: work,
            },
            key,
            generation: record.generation,
        })
    }

    pub fn snapshot(&self) -> WorkloadSnapshot {
        let state = self.inner.state.lock().unwrap();
        WorkloadSnapshot {
            serving: if state.closed {
                crate::ServingState::Closed
            } else if state.ready {
                crate::ServingState::Ready
            } else {
                crate::ServingState::Initializing
            },
            admission_closed: state.closed,
            root_responsibilities: state.roots,
            businesses: state.businesses,
            preparation: state.preparation,
            execution: state.execution,
            admission_records: state.requests.len(),
            resource_waiters: state.resource_waiters.len(),
            waiting_records: state.waiting_records(),
            peak_waiting_records: state.peak_waiting_records,
            waiting_bytes: state.waiting_bytes,
            peak_admission_records: state.peak_waiting,
            peak_waiting_bytes: state.peak_waiting_bytes,
            old_attempts: state.old_attempts,
            unknown_creates: state.unknown_creates,
            control_ready: state.control_ready.len(),
            control_inflight: state.control_inflight,
            scopes: state
                .nodes
                .iter()
                .map(|(&id, node)| ScopeSnapshot {
                    id,
                    parent: node.parent,
                    root: node.root,
                    class: node.class,
                    owner: node.owner,
                    handoffs: node.handoffs,
                    own_completed: node.completed,
                    children: node.children,
                    business_admitted: node.business,
                    occupied_stages: node.stages.iter().copied().collect(),
                    restarts: node.restarts,
                    reserved_bytes: node.reserved_bytes,
                    used_bytes: node.used_bytes,
                    resource_holders: node.resource_holders,
                    resource_waiters: node.resource_waiters,
                    obligations: node
                        .obligations
                        .iter()
                        .map(|(&key, record)| ObligationSnapshot {
                            key,
                            kind: record.kind,
                            usage: record.usage.clone(),
                        })
                        .collect(),
                    control_pending: node.control_pending,
                    control_inflight: node.control_inflight,
                })
                .collect(),
        }
    }
}

impl WorkScope {
    /// Register responsibility before sending/starting work. Duplicate exact
    /// keys share one record. Drop does not settle an unknown outcome.
    /// A retired attempt also consumes this logical work's restart allowance.
    pub fn register_obligation(
        &self,
        key: ObligationKey,
        kind: ObligationKind,
    ) -> Result<Obligation, WorkError> {
        self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            if let Some(record) = node.obligations.get(&key) {
                if record.kind != kind {
                    return Err(WorkError::Conflict);
                }
                return Ok(Obligation {
                    scope: self.clone(),
                    key,
                    generation: record.generation,
                });
            }
            node.check()?;
            let config = &self.inner.config;
            if state.obligations >= config.obligation_records_limit {
                return Err(WorkError::Capacity("obligation records"));
            }
            match kind {
                ObligationKind::RetiredAttempt => {
                    let old = node
                        .obligations
                        .values()
                        .filter(|record| record.kind == ObligationKind::RetiredAttempt)
                        .count();
                    if node.restarts >= config.restarts_per_work {
                        return Err(WorkError::Capacity("restart allowance"));
                    }
                    if old >= config.old_attempts_per_work
                        || state.old_attempts >= config.old_attempts_limit
                    {
                        return Err(WorkError::Capacity("retired attempts"));
                    }
                    if state.unknown_creates >= config.unknown_creates_limit {
                        return Err(WorkError::Capacity("unknown creates"));
                    }
                }
                ObligationKind::UnknownCreate
                    if state.unknown_creates >= config.unknown_creates_limit =>
                {
                    return Err(WorkError::Capacity("unknown creates"));
                }
                _ => {}
            }
            let generation = state.next_id()?;
            let node = state.nodes.get_mut(&self.id).unwrap();
            node.obligations.insert(
                key,
                ObligationRecord {
                    generation,
                    kind,
                    usage: None,
                },
            );
            state.obligations += 1;
            match kind {
                ObligationKind::RetiredAttempt => {
                    state.old_attempts += 1;
                    node.restarts += 1;
                }
                ObligationKind::UnknownCreate => state.unknown_creates += 1,
                _ => {}
            }
            Ok(Obligation {
                scope: self.clone(),
                key,
                generation,
            })
        })
    }
}

/// A fact-reporting capability, separate from scheduling and business permits.
/// Only a trusted local/domain owner should receive it. All clones address the
/// same generation; an old handle cannot settle a reused key's newer record.
#[derive(Clone)]
pub struct Obligation {
    scope: WorkScope,
    key: ObligationKey,
    generation: u64,
}

impl Obligation {
    pub fn key(&self) -> ObligationKey {
        self.key
    }

    pub fn observe_usage(&self, bytes: u64) -> Result<(), WorkError> {
        self.scope.inner.update_facts(|state| {
            let record = state
                .nodes
                .get_mut(&self.scope.id)
                .and_then(|node| node.obligations.get_mut(&self.key))
                .ok_or(WorkError::Released)?;
            if record.generation != self.generation {
                return Err(WorkError::Released);
            }
            record.usage = Some(UsageObservation {
                last_known_bytes: bytes,
                observed_at: Instant::now(),
                current_unknown: false,
            });
            Ok(())
        })
    }

    /// Unreachable or retired identity is not zero usage or a physical stop.
    /// It does not consume capacity belonging to a healthy local authority.
    pub fn mark_current_unknown(&self) -> Result<(), WorkError> {
        self.scope.inner.update_facts(|state| {
            let record = state
                .nodes
                .get_mut(&self.scope.id)
                .and_then(|node| node.obligations.get_mut(&self.key))
                .ok_or(WorkError::Released)?;
            if record.generation != self.generation {
                return Err(WorkError::Released);
            }
            if let Some(usage) = record.usage.as_mut() {
                usage.current_unknown = true;
            }
            Ok(())
        })
    }

    /// Supply actual completion, release, or an explicit responsibility takeover
    /// fact. Timeout, failure conclusion and identity replacement are not facts
    /// that authorize this method. Returns false for an already settled handle.
    pub fn resolve(&self) -> bool {
        self.scope.inner.update_facts(|state| {
            let Some(node) = state.nodes.get_mut(&self.scope.id) else {
                return false;
            };
            if node
                .obligations
                .get(&self.key)
                .is_none_or(|record| record.generation != self.generation)
            {
                return false;
            }
            let record = node.obligations.remove(&self.key).unwrap();
            state.obligations -= 1;
            match record.kind {
                ObligationKind::RetiredAttempt => state.old_attempts -= 1,
                ObligationKind::UnknownCreate => state.unknown_creates -= 1,
                _ => {}
            }
            state.collect(self.scope.id);
            true
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlIntent {
    Cancel,
    Release,
    Observe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlIntents(u8);

impl ControlIntents {
    pub fn empty() -> Self {
        Self(0)
    }
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
    pub fn contains(self, intent: ControlIntent) -> bool {
        self.0 & Self::bit(intent) != 0
    }
    fn bit(intent: ControlIntent) -> u8 {
        match intent {
            ControlIntent::Cancel => 1,
            ControlIntent::Release => 2,
            ControlIntent::Observe => 4,
        }
    }
    pub(crate) fn insert(&mut self, intent: ControlIntent) {
        self.0 |= Self::bit(intent);
    }
    fn merge(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

pub(crate) fn queue_control(
    state: &mut State,
    id: WorkId,
    intent: ControlIntent,
) -> Result<(), WorkError> {
    let node = state.nodes.get_mut(&id).ok_or(WorkError::Released)?;
    node.control_pending.insert(intent);
    if !node.control_queued && !node.control_inflight {
        state.control_waiting.insert(id);
    }
    Ok(())
}

pub(crate) fn fill_control(state: &mut State, config: &WorkloadConfig) {
    while state.control_ready.len() < config.control_ready_limit {
        let next = state
            .control_cursor
            .and_then(|cursor| {
                state
                    .control_waiting
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| state.control_waiting.first().copied());
        let Some(id) = next else {
            break;
        };
        state.control_waiting.remove(&id);
        let node = state.nodes.get_mut(&id).unwrap();
        node.control_queued = true;
        state.control_ready.push_back(id);
        state.control_cursor = Some(id);
    }
}

impl WorkScope {
    /// This always retains the intent in its bounded owner record even when
    /// the ready queue is full. It is valid after cancellation/own completion.
    pub fn request_control(&self, intent: ControlIntent) -> Result<(), WorkError> {
        self.inner
            .update(|state| queue_control(state, self.id, intent))
    }
}

impl WorkloadControl {
    pub fn next_control(&self) -> Option<ControlPermit> {
        self.inner.update(|state| {
            if state.control_inflight >= self.inner.config.control_inflight_limit {
                return None;
            }
            let id = state.control_ready.pop_front()?;
            let node = state.nodes.get_mut(&id).unwrap();
            node.control_queued = false;
            node.control_inflight = true;
            let intents = std::mem::replace(&mut node.control_pending, ControlIntents::empty());
            state.control_inflight += 1;
            Some(ControlPermit {
                scope: Some(WorkScope {
                    inner: Arc::clone(&self.inner),
                    id,
                }),
                intents,
                acknowledged: false,
            })
        })
    }

    pub async fn wait_control_ready(&self) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.inner.state.lock().unwrap();
                if !state.control_ready.is_empty()
                    && state.control_inflight < self.inner.config.control_inflight_limit
                {
                    return;
                }
            }
            changed.await;
        }
    }
}

/// Independent control I/O admission. Losing a dispatcher future requeues its
/// intent; acknowledging transport progress does not resolve work obligations.
pub struct ControlPermit {
    scope: Option<WorkScope>,
    intents: ControlIntents,
    acknowledged: bool,
}

impl ControlPermit {
    pub fn scope(&self) -> WorkScope {
        self.scope.as_ref().unwrap().clone()
    }
    pub fn intents(&self) -> ControlIntents {
        self.intents
    }
    pub fn acknowledge(mut self) {
        self.acknowledged = true;
    }
}

impl Drop for ControlPermit {
    fn drop(&mut self) {
        let scope = self.scope.take().unwrap();
        scope.inner.update(|state| {
            state.control_inflight -= 1;
            let node = state.nodes.get_mut(&scope.id).unwrap();
            node.control_inflight = false;
            if !self.acknowledged {
                node.control_pending.merge(self.intents);
            }
            if !node.control_pending.is_empty() {
                state.control_waiting.insert(scope.id);
            }
            state.collect(scope.id);
        });
    }
}
