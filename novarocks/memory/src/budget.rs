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

//! Bounded metadata budgets (MEM-1 wave-1 T03).
//!
//! The core's own growing state is governed like any other: accounts, leases,
//! reclaimers and buffered events all have limits. Exhaustion is a typed
//! refusal of new operations — never a reason to discard a live charge in
//! order to keep succeeding.
//!
//! # Why the core budgets itself
//!
//! A capacity authority that grows without a bound is a capacity leak wearing
//! a different name: a workload that opens a million scopes would push the
//! process over its bound through the authority's own bookkeeping, and the
//! authority would report `C <= B` the whole way down. Bounding each registry
//! turns that failure into an early, attributable refusal.
//!
//! # Why exhaustion never frees itself
//!
//! The tempting shortcut when a registry is full is to evict something to make
//! room. The core refuses to, because every entry in these registries stands
//! for memory that is still alive somewhere: dropping a lease record does not
//! drop the backing, it only stops the authority from knowing about it. So an
//! exhausted registry produces [`CapacityError::MetadataExhausted`], the new
//! operation fails, and every existing entry keeps its meaning.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{CapacityError, ConfigError, MetadataRegistryLabel};

/// Per-registry limits for the core's own growing state.
///
/// The defaults are sized so that a healthy process never notices them and a
/// runaway one is stopped well before the bookkeeping itself becomes a memory
/// problem. They are deliberately generous rather than tight: a limit that
/// bites during normal work would turn a bookkeeping bound into a workload
/// limit, which is the arbitrator's job and not this crate's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataBudget {
    accounts: u32,
    holders: u32,
    reclaimers: u32,
    events: u32,
}

impl MetadataBudget {
    /// Default limit on live accounts in the strict tree.
    ///
    /// One account per owner inside every concurrent task is already far below
    /// this; reaching it means scopes are being created and never closed.
    pub const DEFAULT_ACCOUNTS: u32 = 65_536;

    /// Default limit on live retention leases and pins together.
    ///
    /// Leases and pins share one budget because they share one failure mode: a
    /// consumer that forgets to drop them. The limit is the largest of the
    /// four because one resident cache can legitimately hold a very large
    /// number of small frames.
    pub const DEFAULT_HOLDERS: u32 = 1_048_576;

    /// Default limit on registered reclaimers.
    ///
    /// Reclaimers are registered by long-lived subsystems, not per query, so a
    /// small limit is enough and a breach almost certainly means registration
    /// without deregistration.
    pub const DEFAULT_RECLAIMERS: u32 = 4_096;

    /// Default capacity of the bounded observation event ring.
    ///
    /// Events are droppable by construction, so this limit trades a slow
    /// observer's completeness against a fixed memory cost rather than
    /// refusing anything.
    pub const DEFAULT_EVENTS: u32 = 65_536;

    /// Returns the default budget.
    ///
    /// This is a `const fn` so an authority can be configured in a constant
    /// without running code, which keeps the defaults visible at their use
    /// site instead of hidden behind a builder.
    pub const fn with_defaults() -> Self {
        Self {
            accounts: Self::DEFAULT_ACCOUNTS,
            holders: Self::DEFAULT_HOLDERS,
            reclaimers: Self::DEFAULT_RECLAIMERS,
            events: Self::DEFAULT_EVENTS,
        }
    }

    /// Builds a budget from explicit limits.
    ///
    /// A zero limit is accepted here and rejected by [`Self::validate`], so
    /// that a bad configuration is reported once, as a
    /// [`ConfigError`], rather than panicking wherever the value is read.
    pub const fn new(accounts: u32, holders: u32, reclaimers: u32, events: u32) -> Self {
        Self {
            accounts,
            holders,
            reclaimers,
            events,
        }
    }

    /// Returns a copy with a different account limit.
    pub const fn with_accounts(mut self, limit: u32) -> Self {
        self.accounts = limit;
        self
    }

    /// Returns a copy with a different holder limit.
    pub const fn with_holders(mut self, limit: u32) -> Self {
        self.holders = limit;
        self
    }

    /// Returns a copy with a different reclaimer limit.
    pub const fn with_reclaimers(mut self, limit: u32) -> Self {
        self.reclaimers = limit;
        self
    }

    /// Returns a copy with a different event-ring capacity.
    pub const fn with_events(mut self, limit: u32) -> Self {
        self.events = limit;
        self
    }

    /// Returns the account limit.
    pub const fn accounts(&self) -> u32 {
        self.accounts
    }

    /// Returns the shared retention-lease and pin limit.
    pub const fn holders(&self) -> u32 {
        self.holders
    }

    /// Returns the reclaimer limit.
    pub const fn reclaimers(&self) -> u32 {
        self.reclaimers
    }

    /// Returns the event-ring capacity.
    pub const fn events(&self) -> u32 {
        self.events
    }

    /// Returns the limit for one registry.
    ///
    /// Registries are addressed by their label so a caller — a diagnostic, a
    /// test, or a generic registry constructor — can handle all four without
    /// repeating the mapping and without being able to forget one.
    pub const fn limit_for(&self, registry: MetadataRegistryLabel) -> u32 {
        match registry {
            MetadataRegistryLabel::Accounts => self.accounts,
            MetadataRegistryLabel::Holders => self.holders,
            MetadataRegistryLabel::Reclaimers => self.reclaimers,
            MetadataRegistryLabel::Events => self.events,
        }
    }

    /// Rejects a budget that could never admit even a first entry.
    ///
    /// A zero limit is not a strict configuration, it is an unusable one: a
    /// zero account limit refuses the process root, and a zero holder limit
    /// makes every shared resource unreportable. Catching it at configuration
    /// time keeps the refusal away from the allocation path, where the only
    /// honest answer would be a permanent denial.
    pub const fn validate(&self) -> Result<(), ConfigError> {
        if self.accounts == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Accounts,
            });
        }
        if self.holders == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Holders,
            });
        }
        if self.reclaimers == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Reclaimers,
            });
        }
        if self.events == 0 {
            return Err(ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Events,
            });
        }
        Ok(())
    }
}

impl Default for MetadataBudget {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// A counted, bounded set of registry slots.
///
/// Every bounded registry in the core holds one of these instead of checking a
/// length against a limit itself. That gives the whole crate a single
/// exhaustion story: one refusal type, one label, one place where the
/// check-then-insert race is closed.
///
/// The check and the reservation are a single atomic step. A registry that
/// compared its length and then inserted would admit more entries than its
/// limit whenever two threads raced, and the limit exists precisely to hold
/// under load.
#[derive(Debug)]
pub struct BoundedSlots {
    registry: MetadataRegistryLabel,
    limit: u32,
    in_use: AtomicU32,
}

impl BoundedSlots {
    /// Creates an empty slot set for one registry.
    pub const fn new(registry: MetadataRegistryLabel, limit: u32) -> Self {
        Self {
            registry,
            limit,
            in_use: AtomicU32::new(0),
        }
    }

    /// Creates an empty slot set from a budget's limit for that registry.
    pub const fn from_budget(budget: &MetadataBudget, registry: MetadataRegistryLabel) -> Self {
        Self::new(registry, budget.limit_for(registry))
    }

    /// Returns which registry these slots belong to.
    pub const fn registry(&self) -> MetadataRegistryLabel {
        self.registry
    }

    /// Returns the configured limit.
    pub const fn limit(&self) -> u32 {
        self.limit
    }

    /// Returns the number of slots currently held.
    ///
    /// This is a diagnostic reading, not a reservation: by the time a caller
    /// acts on it another thread may have taken the remaining slot. Only
    /// [`Self::try_acquire`] reserves.
    pub fn in_use(&self) -> u32 {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Returns how many slots were free at the moment of the read.
    ///
    /// Advisory for the same reason as [`Self::in_use`].
    pub fn remaining(&self) -> u32 {
        self.limit.saturating_sub(self.in_use())
    }

    /// Reserves one slot, or refuses because the registry is full.
    ///
    /// A refusal is final for this attempt: the caller fails its operation and
    /// leaves every existing entry alone. The core never makes room by
    /// dropping a record that stands for live memory.
    pub fn try_acquire(&self) -> Result<(), CapacityError> {
        let mut observed = self.in_use.load(Ordering::Relaxed);
        loop {
            if observed >= self.limit {
                return Err(CapacityError::MetadataExhausted {
                    registry: self.registry,
                    limit: self.limit,
                });
            }
            match self.in_use.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => observed = current,
            }
        }
    }

    /// Returns one reserved slot.
    ///
    /// Releasing more often than acquiring cannot drive the count below zero.
    /// That is a deliberate choice rather than an assertion: these counters
    /// are driven by `Drop`, and a double release must degrade into a
    /// no-op instead of panicking while another value is unwinding.
    pub fn release(&self) {
        let mut observed = self.in_use.load(Ordering::Relaxed);
        loop {
            if observed == 0 {
                return;
            }
            match self.in_use.compare_exchange_weak(
                observed,
                observed - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_matches_the_documented_limits() {
        let budget = MetadataBudget::default();
        assert_eq!(budget.accounts(), 65_536);
        assert_eq!(budget.holders(), 1_048_576);
        assert_eq!(budget.reclaimers(), 4_096);
        assert_eq!(budget.events(), 65_536);
        assert_eq!(budget, MetadataBudget::with_defaults());
    }

    #[test]
    fn limit_for_covers_every_registry_label() {
        let budget = MetadataBudget::new(1, 2, 3, 4);
        assert_eq!(budget.limit_for(MetadataRegistryLabel::Accounts), 1);
        assert_eq!(budget.limit_for(MetadataRegistryLabel::Holders), 2);
        assert_eq!(budget.limit_for(MetadataRegistryLabel::Reclaimers), 3);
        assert_eq!(budget.limit_for(MetadataRegistryLabel::Events), 4);
    }

    #[test]
    fn builder_methods_change_one_limit_at_a_time() {
        let budget = MetadataBudget::with_defaults()
            .with_accounts(8)
            .with_holders(9)
            .with_reclaimers(10)
            .with_events(11);
        assert_eq!(budget, MetadataBudget::new(8, 9, 10, 11));
    }

    #[test]
    fn validate_accepts_the_defaults_and_names_a_zero_registry() {
        assert_eq!(MetadataBudget::with_defaults().validate(), Ok(()));
        assert_eq!(
            MetadataBudget::with_defaults()
                .with_holders(0)
                .validate()
                .unwrap_err(),
            ConfigError::MetadataLimitIsZero {
                registry: MetadataRegistryLabel::Holders,
            }
        );
    }

    #[test]
    fn slots_refuse_at_the_limit_with_the_registry_and_limit_named() {
        let slots = BoundedSlots::new(MetadataRegistryLabel::Reclaimers, 2);
        assert_eq!(slots.try_acquire(), Ok(()));
        assert_eq!(slots.try_acquire(), Ok(()));
        assert_eq!(slots.in_use(), 2);
        assert_eq!(slots.remaining(), 0);
        assert_eq!(
            slots.try_acquire().unwrap_err(),
            CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Reclaimers,
                limit: 2,
            }
        );
        assert_eq!(slots.in_use(), 2, "a refusal must not reserve a slot");
    }

    #[test]
    fn release_frees_exactly_one_slot_and_never_underflows() {
        let slots = BoundedSlots::from_budget(
            &MetadataBudget::with_defaults().with_holders(1),
            MetadataRegistryLabel::Holders,
        );
        assert_eq!(slots.limit(), 1);
        assert_eq!(slots.registry(), MetadataRegistryLabel::Holders);
        assert!(slots.try_acquire().is_ok());
        assert!(slots.try_acquire().is_err());
        slots.release();
        assert_eq!(slots.in_use(), 0);
        slots.release();
        slots.release();
        assert_eq!(slots.in_use(), 0, "release must not wrap below zero");
        assert!(slots.try_acquire().is_ok(), "the slot is reusable");
    }

    #[test]
    fn slots_hold_their_limit_under_concurrent_acquisition() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let slots = Arc::new(BoundedSlots::new(MetadataRegistryLabel::Holders, 64));
        let granted = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let slots = Arc::clone(&slots);
                let granted = Arc::clone(&granted);
                thread::spawn(move || {
                    for _ in 0..32 {
                        if slots.try_acquire().is_ok() {
                            granted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("worker thread must not panic");
        }
        assert_eq!(granted.load(Ordering::Relaxed), 64);
        assert_eq!(slots.in_use(), 64);
    }
}
