// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend-owned serialization for activity against one materialized view.
//!
//! The gate deliberately does not model scheduler or maintenance capacity.
//! Callers first enqueue a ticket, then acquire their own capacity only after
//! [`MvActivityTicket::try_acquire`] returns a lease.  This keeps waiters from
//! consuming either worker's independent concurrency budget.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};

use novarocks::mv::repository::MvTarget;
use novarocks::query_execution::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};

/// A stable, provider-neutral identity for the MV whose activity is being
/// serialized.  Inputs must already have passed SQL/catalog canonicalization;
/// this type intentionally does not lowercase quoted identifiers.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CanonicalMvTarget {
    catalog: Option<String>,
    database: String,
    name: String,
}

impl CanonicalMvTarget {
    pub(crate) fn from_mv_target(target: &MvTarget) -> Self {
        Self::from_parts(target.catalog.as_deref(), &target.database, &target.name)
    }

    /// Build a key from a target whose identifiers have already been resolved
    /// by the frontend. Table maintenance uses this for the MV storage table.
    pub(crate) fn from_parts(catalog: Option<&str>, database: &str, name: &str) -> Self {
        Self {
            catalog: catalog.map(str::to_owned),
            database: database.to_owned(),
            name: name.to_owned(),
        }
    }
}

/// The application path currently holding, or waiting to hold, an MV gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvActivityOwner {
    ManualRefresh,
    ScheduledRefresh,
    AutomaticMaintenance,
}

impl MvActivityOwner {
    fn is_worker_owned(self) -> bool {
        matches!(self, Self::ScheduledRefresh | Self::AutomaticMaintenance)
    }
}

/// Admission can no longer be granted because frontend shutdown has started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvActivityGateError {
    Stopping,
}

/// A process-local, FIFO gate.  One gate is shared by manual refresh, the MV
/// scheduler, and automatic maintenance; it must be constructed once by the
/// frontend application host and injected into each owner.
#[derive(Clone, Default)]
pub(crate) struct MvActivityGate {
    inner: Arc<Mutex<GateState>>,
}

impl MvActivityGate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register one attempt without taking an execution permit.  A ticket is
    /// FIFO only relative to the same canonical target; different MVs remain
    /// eligible to overlap subject to their worker-specific limits.
    pub(crate) fn request(
        &self,
        target: CanonicalMvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, MvActivityGateError> {
        let mut state = lock(&self.inner);
        if state.stopping {
            return Err(MvActivityGateError::Stopping);
        }

        let ticket_id = state.next_ticket_id;
        state.next_ticket_id = state.next_ticket_id.checked_add(1).unwrap_or_else(|| {
            // Reusing a ticket ID could release another attempt.  A process
            // restart is the only safe recovery from this theoretical limit.
            panic!("MV activity ticket ID overflow")
        });
        state
            .entries
            .entry(target.clone())
            .or_default()
            .waiters
            .push_back(Waiter { ticket_id, owner });
        Ok(MvActivityTicket {
            inner: Arc::downgrade(&self.inner),
            target,
            ticket_id,
            claimed: false,
        })
    }

    /// Stop admitting new work and cancel only frontend-worker owned attempts.
    /// Manual work retains its session-owned cancellation lifecycle, but still
    /// releases the gate normally when its statement terminates.
    pub(crate) fn begin_stopping(&self) {
        let mut state = lock(&self.inner);
        state.stopping = true;
        for entry in state.entries.values_mut() {
            if let Some(active) = &entry.active {
                if let Some(source) = &active.cancellation {
                    let _ = source.request(QueryCancellationReason::ServerShutdown);
                }
            }
        }
    }

    #[cfg(test)]
    fn tracked_target_count(&self) -> usize {
        lock(&self.inner).entries.len()
    }
}

/// A queued request for one gate.  Dropping an unclaimed ticket removes it
/// from the FIFO queue, so cancelled/pre-dispatch work cannot strand later
/// attempts or retain an empty target entry.
pub(crate) struct MvActivityTicket {
    inner: Weak<Mutex<GateState>>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    claimed: bool,
}

impl MvActivityTicket {
    /// Acquire only when this ticket is at the head of its target's FIFO queue.
    /// `Ok(None)` means an earlier attempt is active or waiting; callers must
    /// not acquire a refresh/maintenance permit before retrying this method.
    pub(crate) fn try_acquire(&mut self) -> Result<Option<MvActivityLease>, MvActivityGateError> {
        if self.claimed {
            return Ok(None);
        }
        let Some(inner) = self.inner.upgrade() else {
            return Err(MvActivityGateError::Stopping);
        };
        let mut state = lock(&inner);
        if state.stopping {
            remove_waiter(&mut state, &self.target, self.ticket_id);
            return Err(MvActivityGateError::Stopping);
        }
        let Some(entry) = state.entries.get_mut(&self.target) else {
            return Ok(None);
        };
        if entry.active.is_some()
            || entry
                .waiters
                .front()
                .is_none_or(|waiter| waiter.ticket_id != self.ticket_id)
        {
            return Ok(None);
        }

        let waiter = entry
            .waiters
            .pop_front()
            .expect("front waiter exists after FIFO check");
        debug_assert_eq!(waiter.ticket_id, self.ticket_id);
        let cancellation = waiter
            .owner
            .is_worker_owned()
            .then(QueryCancellationSource::new);
        entry.active = Some(ActiveAttempt {
            ticket_id: self.ticket_id,
            owner: waiter.owner,
            cancellation: cancellation.clone(),
        });
        self.claimed = true;
        Ok(Some(MvActivityLease {
            inner: Arc::downgrade(&inner),
            target: self.target.clone(),
            ticket_id: self.ticket_id,
            cancellation: cancellation.map(|source| source.view()),
        }))
    }
}

impl Drop for MvActivityTicket {
    fn drop(&mut self) {
        if self.claimed {
            return;
        }
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = lock(&inner);
        remove_waiter(&mut state, &self.target, self.ticket_id);
    }
}

/// Exclusive ownership of an MV activity slot.  Dropping it is the terminal
/// transition for the attempt and wakes the next FIFO ticket on its next poll.
pub(crate) struct MvActivityLease {
    inner: Weak<Mutex<GateState>>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    cancellation: Option<QueryCancellationView>,
}

impl MvActivityLease {
    /// Worker-owned attempts receive a source that `begin_stopping` cancels
    /// with `ServerShutdown`. Manual refresh returns `None` because its
    /// statement already owns the cancellation source.
    pub(crate) fn cancellation(&self) -> Option<QueryCancellationView> {
        self.cancellation.clone()
    }
}

impl Drop for MvActivityLease {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = lock(&inner);
        let mut remove_entry = false;
        if let Some(entry) = state.entries.get_mut(&self.target) {
            if entry
                .active
                .as_ref()
                .is_some_and(|active| active.ticket_id == self.ticket_id)
            {
                entry.active = None;
            }
            remove_entry = entry.active.is_none() && entry.waiters.is_empty();
        }
        if remove_entry {
            state.entries.remove(&self.target);
        }
    }
}

#[derive(Default)]
struct GateState {
    stopping: bool,
    next_ticket_id: u64,
    entries: BTreeMap<CanonicalMvTarget, TargetEntry>,
}

#[derive(Default)]
struct TargetEntry {
    waiters: VecDeque<Waiter>,
    active: Option<ActiveAttempt>,
}

struct Waiter {
    ticket_id: u64,
    owner: MvActivityOwner,
}

struct ActiveAttempt {
    ticket_id: u64,
    #[allow(dead_code)]
    owner: MvActivityOwner,
    cancellation: Option<QueryCancellationSource>,
}

fn remove_waiter(state: &mut GateState, target: &CanonicalMvTarget, ticket_id: u64) {
    let mut remove_entry = false;
    if let Some(entry) = state.entries.get_mut(target) {
        if let Some(position) = entry
            .waiters
            .iter()
            .position(|waiter| waiter.ticket_id == ticket_id)
        {
            entry.waiters.remove(position);
        }
        remove_entry = entry.active.is_none() && entry.waiters.is_empty();
    }
    if remove_entry {
        state.entries.remove(target);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str) -> CanonicalMvTarget {
        CanonicalMvTarget::from_parts(Some("iceberg"), "db", name)
    }

    #[test]
    fn tickets_acquire_in_fifo_order_for_one_target() {
        let gate = MvActivityGate::new();
        let mut first = gate
            .request(target("mv"), MvActivityOwner::ScheduledRefresh)
            .expect("first ticket");
        let mut second = gate
            .request(target("mv"), MvActivityOwner::AutomaticMaintenance)
            .expect("second ticket");

        assert!(second.try_acquire().expect("gate is running").is_none());
        let first_lease = first
            .try_acquire()
            .expect("gate is running")
            .expect("first ticket acquires");
        assert!(second.try_acquire().expect("active first ticket").is_none());
        drop(first_lease);
        assert!(second.try_acquire().expect("gate is running").is_some());
    }

    #[test]
    fn same_target_manual_refresh_and_maintenance_cannot_overlap() {
        let gate = MvActivityGate::new();
        let mut manual = gate
            .request(target("mv"), MvActivityOwner::ManualRefresh)
            .expect("manual ticket");
        let mut maintenance = gate
            .request(target("mv"), MvActivityOwner::AutomaticMaintenance)
            .expect("maintenance ticket");

        let manual_lease = manual
            .try_acquire()
            .expect("gate is running")
            .expect("manual acquires");
        assert!(manual_lease.cancellation().is_none());
        assert!(
            maintenance
                .try_acquire()
                .expect("gate is running")
                .is_none()
        );
        drop(manual_lease);
        assert!(
            maintenance
                .try_acquire()
                .expect("gate is running")
                .is_some()
        );
    }

    #[test]
    fn cancelled_waiters_and_terminal_leases_reap_empty_targets() {
        let gate = MvActivityGate::new();
        let cancelled = gate
            .request(target("cancelled"), MvActivityOwner::ScheduledRefresh)
            .expect("ticket");
        assert_eq!(gate.tracked_target_count(), 1);
        drop(cancelled);
        assert_eq!(gate.tracked_target_count(), 0);

        let mut ticket = gate
            .request(target("terminal"), MvActivityOwner::ScheduledRefresh)
            .expect("ticket");
        let lease = ticket
            .try_acquire()
            .expect("gate is running")
            .expect("ticket acquires");
        assert_eq!(gate.tracked_target_count(), 1);
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[test]
    fn stopping_rejects_tickets_and_cancels_worker_owned_attempts() {
        let gate = MvActivityGate::new();
        let mut ticket = gate
            .request(target("mv"), MvActivityOwner::ScheduledRefresh)
            .expect("ticket");
        let lease = ticket
            .try_acquire()
            .expect("gate is running")
            .expect("ticket acquires");
        let cancellation = lease.cancellation().expect("worker cancellation view");

        gate.begin_stopping();
        assert_eq!(
            cancellation.reason(),
            Some(QueryCancellationReason::ServerShutdown)
        );
        assert!(matches!(
            gate.request(target("other"), MvActivityOwner::AutomaticMaintenance),
            Err(MvActivityGateError::Stopping)
        ));
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }
}
