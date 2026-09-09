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

//! Observation values and the bounded event ring.
//!
//! A snapshot separates the facts MEM-1 requires to stay separate: known live
//! allocation `L`, unfulfilled grants `F`, the bounded third-party upper bound
//! `O`, their sum `C`, the floor, the installed policy, the excess, and the
//! peaks. It carries the versions it was read under so a consumer can tell a
//! stale reading from a policy change.
//!
//! Two things a snapshot deliberately does not offer: a claim that several
//! accounts were read at the same instant, and any way to add a child's peak
//! into a parent's. Both are called out per field.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{ConstraintKind, MetadataRegistryLabel};
use crate::ids::{AccountId, AccountKind, ConfigVersion, PolicyVersion};

/// One account's separately expressed facts.
///
/// Every byte count is in the managed byte unit the adapters declare. Peaks
/// belong to this account alone: `peak_committed_bytes` of a parent is not the
/// sum of its children's, and summing children's peaks is never valid because
/// they may have occurred at different times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountSnapshot {
    /// The account this snapshot describes.
    pub account: AccountId,
    /// What the account stands for.
    pub kind: AccountKind,
    /// Known live allocation `L`: backing proven by an owner and still alive.
    pub live_bytes: u64,
    /// Unfulfilled grants `F`: issued rights that may still be fulfilled,
    /// including sub-grants handed down and not yet returned.
    pub granted_bytes: u64,
    /// Third-party remaining upper bound `O`: authorised coverage that cannot
    /// yet be expressed as known `L`. This is not measured usage.
    pub bounded_bytes: u64,
    /// Total commitment `C`, as the account itself maintains it.
    ///
    /// This is the authoritative number, and the one every bound is enforced
    /// on: it changes only when capacity crosses the account's own boundary.
    /// `L + F + O` above is a walk of the subtree taken without a global
    /// lock, so the two agree at quiescent points and the decomposition can
    /// transiently read higher while a top-up's optimistic claim is in
    /// flight. A consumer judging a limit uses this field; a consumer
    /// explaining where the bytes went uses the decomposition.
    pub committed_bytes: u64,
    /// Capacity this account keeps for a progressable unit, which ordinary
    /// competition may not revoke. The floor is a lower bound on retention,
    /// not a fourth quantity: the bytes it protects appear in `L`, `F` or `O`.
    pub floor_bytes: u64,
    /// Installed policy limit, when one is installed.
    pub policy_limit_bytes: Option<u64>,
    /// Bytes held beyond the installed policy. A lowered policy does not erase
    /// commitments, so this is reported honestly rather than clamped.
    pub excess_bytes: u64,
    /// Whether the account is closed to further growth.
    pub growth_frozen: bool,
    /// This account's own peak `C`.
    pub peak_committed_bytes: u64,
    /// This account's own peak `L`.
    pub peak_live_bytes: u64,
    /// Policy version the numbers were read under.
    pub policy_version: PolicyVersion,
}

impl AccountSnapshot {
    /// Reports whether the subtree decomposition `L + F + O` matches the
    /// maintained `C`.
    ///
    /// Counters are read without a global lock, so this is transiently false
    /// while another thread is mid-transition: an in-flight top-up has
    /// already raised a child's commitment before the parent has handed the
    /// capacity over, and a sponsor transfer deliberately leaves the common
    /// ancestor inconsistent for its duration. Tests assert this at quiescent
    /// points; production consumers treat a false result as "re-read", not as
    /// a bug.
    pub const fn is_internally_consistent(&self) -> bool {
        match self.live_bytes.checked_add(self.granted_bytes) {
            Some(partial) => match partial.checked_add(self.bounded_bytes) {
                Some(total) => total == self.committed_bytes,
                None => false,
            },
            None => false,
        }
    }

    /// Returns the bytes still available under the installed policy, or `None`
    /// when no policy is installed.
    pub const fn policy_remaining_bytes(&self) -> Option<u64> {
        match self.policy_limit_bytes {
            Some(limit) => Some(limit.saturating_sub(self.committed_bytes)),
            None => None,
        }
    }
}

/// The process authority's own facts.
///
/// `capacity_bytes` and `headroom_budget_bytes` partition
/// `process_bound_bytes`. The hard guarantee `C <= B` covers only the declared
/// hard-governed set; the headroom budget covers what the observation tier
/// measures instead. The two are overlapping views of one process and must
/// never be added into a single total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoritySnapshot {
    /// The root account of this process.
    pub root: AccountSnapshot,
    /// Managed capacity `B`, the bound `C` is kept under.
    pub capacity_bytes: u64,
    /// Headroom budget `H` for allocations outside hard governance.
    pub headroom_budget_bytes: u64,
    /// Process bound `P` this authority was configured against.
    pub process_bound_bytes: u64,
    /// Configuration version the numbers were read under.
    pub config_version: ConfigVersion,
    /// Live accounts in the tree at the moment of the read.
    pub live_accounts: u32,
}

impl AuthoritySnapshot {
    /// Returns the capacity still grantable under `B`.
    pub const fn capacity_remaining_bytes(&self) -> u64 {
        self.capacity_bytes
            .saturating_sub(self.root.committed_bytes)
    }

    /// Reports whether the hard guarantee `C <= B` holds in this reading.
    ///
    /// This is judged on the root's maintained commitment, which is where the
    /// bound is actually enforced, and not on the subtree decomposition. The
    /// decomposition is a lock-free walk and can read higher than `B` while a
    /// top-up's optimistic claim is in flight; treating that as a broken
    /// bound would report a violation that never existed.
    pub const fn honours_capacity_bound(&self) -> bool {
        self.root.committed_bytes <= self.capacity_bytes
    }

    /// Returns the subtree decomposition's own total, for a consumer that
    /// wants to compare it against the maintained commitment.
    pub const fn decomposed_committed_bytes(&self) -> u64 {
        self.root
            .live_bytes
            .saturating_add(self.root.granted_bytes)
            .saturating_add(self.root.bounded_bytes)
    }
}

/// What happened, for a bounded observer.
///
/// Events exist so an arbitrator or an observability consumer can react
/// without polling every account. They are droppable: losing an event never
/// loses an authorisation result, and a consumer that sees a gap re-reads the
/// snapshot instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryEventKind {
    /// A request was refused by a constraint.
    GrantDenied {
        /// The account the request was made against.
        scope: AccountId,
        /// The constraint that refused.
        constraint: ConstraintKind,
        /// Bytes requested.
        requested: u64,
    },
    /// A fulfilment established live allocation beyond its grant's remainder.
    /// The memory exists, so it is charged; growth is frozen instead.
    ExcessRecorded {
        /// The account that recorded the excess.
        scope: AccountId,
        /// Bytes recorded beyond the grant remainder.
        excess_bytes: u64,
    },
    /// An account was closed to further growth.
    GrowthFrozen {
        /// The affected account.
        scope: AccountId,
    },
    /// An account was reopened to growth after its excess was resolved.
    GrowthResumed {
        /// The affected account.
        scope: AccountId,
    },
    /// A policy limit was installed or changed.
    PolicyInstalled {
        /// The affected account.
        scope: AccountId,
        /// The new limit.
        limit_bytes: u64,
        /// The version the new limit carries.
        version: PolicyVersion,
    },
    /// Idle account capacity was reclaimed by an arbitrator.
    IdleCapacityReclaimed {
        /// The affected account.
        scope: AccountId,
        /// Bytes actually reclaimed.
        reclaimed_bytes: u64,
    },
    /// A bounded registry refused a new entry.
    MetadataExhausted {
        /// Which registry refused.
        registry: MetadataRegistryLabel,
    },
}

/// One buffered event with its sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryEvent {
    /// Monotonic sequence number within this ring.
    pub sequence: u64,
    /// What happened.
    pub kind: MemoryEventKind,
}

/// Events read from the ring, plus whether anything was lost before them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    /// The events, in sequence order.
    pub events: Vec<MemoryEvent>,
    /// Events dropped before the first returned event because the reader fell
    /// behind. A non-zero value means the reader must re-read the snapshot.
    pub dropped_before: u64,
    /// The sequence to request next.
    pub next_sequence: u64,
}

impl EventBatch {
    /// Reports whether the reader lost events and must re-read a snapshot.
    pub const fn has_gap(&self) -> bool {
        self.dropped_before > 0
    }
}

/// A bounded ring of observation events.
///
/// The ring never blocks a producer and never grows: when it is full the
/// oldest event is dropped and a counter records the loss, so a slow observer
/// costs memory nothing and still cannot silently miss a change.
#[derive(Debug)]
pub struct EventRing {
    capacity: usize,
    next_sequence: AtomicU64,
    dropped: AtomicU64,
    buffered: Mutex<VecDeque<MemoryEvent>>,
}

impl EventRing {
    /// Creates a ring holding at most `capacity` events. A zero capacity is
    /// raised to one so a ring always reports the most recent change.
    pub fn new(capacity: u32) -> Self {
        let capacity = (capacity as usize).max(1);
        Self {
            capacity,
            next_sequence: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            buffered: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Returns the configured capacity.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Records an event, dropping the oldest if the ring is full.
    ///
    /// This is deliberately infallible: an event is a notification, and losing
    /// one must never fail the capacity operation that produced it.
    pub fn record(&self, kind: MemoryEventKind) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let mut buffered = self
            .buffered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if buffered.len() == self.capacity {
            buffered.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        buffered.push_back(MemoryEvent { sequence, kind });
    }

    /// Returns buffered events with a sequence at or after `from_sequence`.
    ///
    /// The batch reports how many events were dropped before the first
    /// returned one, which is how a reader detects that it fell behind.
    pub fn read_from(&self, from_sequence: u64) -> EventBatch {
        let buffered = self
            .buffered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let events: Vec<MemoryEvent> = buffered
            .iter()
            .copied()
            .filter(|event| event.sequence >= from_sequence)
            .collect();
        let oldest_present = buffered.front().map(|event| event.sequence);
        let dropped_before = match oldest_present {
            Some(oldest) => oldest.saturating_sub(from_sequence),
            None => self
                .next_sequence
                .load(Ordering::Relaxed)
                .saturating_sub(from_sequence),
        };
        let next_sequence = events
            .last()
            .map(|event| event.sequence + 1)
            .unwrap_or_else(|| self.next_sequence.load(Ordering::Relaxed));
        EventBatch {
            events,
            dropped_before,
            next_sequence,
        }
    }

    /// Returns the total number of events dropped over the ring's lifetime.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(live: u64, granted: u64, bounded: u64, committed: u64) -> AccountSnapshot {
        AccountSnapshot {
            account: AccountId::new(1),
            kind: AccountKind::Work,
            live_bytes: live,
            granted_bytes: granted,
            bounded_bytes: bounded,
            committed_bytes: committed,
            floor_bytes: 0,
            policy_limit_bytes: None,
            excess_bytes: 0,
            growth_frozen: false,
            peak_committed_bytes: committed,
            peak_live_bytes: live,
            policy_version: PolicyVersion::INITIAL,
        }
    }

    #[test]
    fn consistency_check_matches_the_l_f_o_decomposition() {
        assert!(snapshot(60, 40, 0, 100).is_internally_consistent());
        assert!(snapshot(0, 100, 0, 100).is_internally_consistent());
        assert!(snapshot(10, 20, 30, 60).is_internally_consistent());
        assert!(!snapshot(60, 40, 0, 90).is_internally_consistent());
    }

    #[test]
    fn policy_remaining_is_absent_without_a_policy_and_saturates_over_it() {
        assert_eq!(snapshot(0, 0, 0, 0).policy_remaining_bytes(), None);
        let mut over = snapshot(120, 0, 0, 120);
        over.policy_limit_bytes = Some(100);
        assert_eq!(over.policy_remaining_bytes(), Some(0));
    }

    #[test]
    fn authority_snapshot_reports_the_capacity_bound_and_remainder() {
        let authority = AuthoritySnapshot {
            root: snapshot(30, 20, 0, 50),
            capacity_bytes: 100,
            headroom_budget_bytes: 20,
            process_bound_bytes: 128,
            config_version: ConfigVersion::new(1),
            live_accounts: 3,
        };
        assert!(authority.honours_capacity_bound());
        assert_eq!(authority.capacity_remaining_bytes(), 50);

        let over = AuthoritySnapshot {
            root: snapshot(150, 0, 0, 150),
            ..authority
        };
        assert!(!over.honours_capacity_bound());
        assert_eq!(over.capacity_remaining_bytes(), 0);
    }

    #[test]
    fn ring_returns_events_in_order_from_a_requested_sequence() {
        let ring = EventRing::new(8);
        for index in 0..4u64 {
            ring.record(MemoryEventKind::GrowthFrozen {
                scope: AccountId::new(index + 1),
            });
        }
        let batch = ring.read_from(0);
        assert_eq!(batch.events.len(), 4);
        assert_eq!(batch.events[0].sequence, 0);
        assert_eq!(batch.next_sequence, 4);
        assert!(!batch.has_gap());

        let tail = ring.read_from(2);
        assert_eq!(tail.events.len(), 2);
        assert_eq!(tail.events[0].sequence, 2);
        assert!(!tail.has_gap());
    }

    #[test]
    fn slow_reader_sees_a_detectable_gap_instead_of_silent_loss() {
        let ring = EventRing::new(2);
        for index in 0..5u64 {
            ring.record(MemoryEventKind::GrowthFrozen {
                scope: AccountId::new(index + 1),
            });
        }
        assert_eq!(ring.dropped_total(), 3);
        let batch = ring.read_from(0);
        assert!(batch.has_gap(), "{batch:?}");
        assert_eq!(batch.dropped_before, 3);
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].sequence, 3);
    }

    #[test]
    fn reading_past_the_end_reports_no_events_and_the_next_sequence() {
        let ring = EventRing::new(4);
        ring.record(MemoryEventKind::MetadataExhausted {
            registry: MetadataRegistryLabel::Accounts,
        });
        let batch = ring.read_from(9);
        assert!(batch.events.is_empty());
        assert_eq!(batch.next_sequence, 1);
        assert!(!batch.has_gap());
    }

    #[test]
    fn zero_capacity_still_keeps_the_latest_event() {
        let ring = EventRing::new(0);
        assert_eq!(ring.capacity(), 1);
        ring.record(MemoryEventKind::GrowthResumed {
            scope: AccountId::new(1),
        });
        ring.record(MemoryEventKind::GrowthResumed {
            scope: AccountId::new(2),
        });
        let batch = ring.read_from(0);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].sequence, 1);
        assert!(batch.has_gap());
    }
}
