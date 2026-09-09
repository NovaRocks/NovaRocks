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

//! Reclaim registration (MEM-1 wave-1 T03).
//!
//! A reclaimer offers an estimate and performs an action; the two are separate
//! facts. A request in flight, a confirmed release and the amount still
//! retained are reported separately, because a failed reclaim must never
//! increase grantable capacity. The core registers reclaimers and reports
//! outcomes; it never calls an operator from inside an allocation path.
//!
//! # Why the core never reclaims inside an allocation
//!
//! The obvious design is to have a denied allocation ask a cache to drop
//! something and then retry. The core refuses to, for three reasons that all
//! bite in production:
//!
//! - **Re-entrancy.** A reclaimer frees memory, which releases charges, which
//!   re-enters the authority while the allocation path holds its own
//!   invariants half-applied.
//! - **Latency ownership.** Reclaiming is slow and may block on I/O. An
//!   allocation call that silently waits for a spill turns every operator into
//!   a place where a query can stall, with no timeout anyone chose.
//! - **Priority.** Deciding *whose* memory to take back is a policy question
//!   about the whole workload. An allocation knows only about itself, so it is
//!   the worst possible place to decide.
//!
//! So the authority answers immediately with a typed refusal, and an
//! arbitrator — outside the allocation path — reads [`ReclaimRegistry`],
//! chooses, requests, and reports the outcome back.
//!
//! # Why an estimate is not capacity
//!
//! [`ReclaimEstimate`] says how much a subsystem *believes* it could give
//! back. Believing is not releasing: a cache may find its candidate frames
//! pinned, a spiller may fail on a full disk. Nothing in this module lets an
//! estimate raise grantable capacity. Only a `Completed` [`ReclaimOutcome`]
//! contributes, and only its confirmed bytes — see
//! [`ReclaimOutcome::grantable_contribution_bytes`].

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::budget::{BoundedSlots, MetadataBudget};
use crate::error::{CapacityError, MetadataRegistryLabel};
use crate::ids::{IdSource, ReclaimTicketId, ReclaimerId};

/// How much trust an estimate carries.
///
/// The arbitrator needs this to choose between reclaimers: a measured 100 MiB
/// is worth more than a speculative 1 GiB, and without the distinction the
/// biggest liar always wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReclaimConfidence {
    /// The subsystem has counted the candidate bytes and they are droppable
    /// now. Still not a promise: a candidate can be claimed before the
    /// request arrives.
    Measured,
    /// Derived from the subsystem's own bookkeeping, with known slack.
    Estimated,
    /// A guess from indirect signals, offered so the arbitrator knows the
    /// subsystem exists at all.
    Speculative,
}

impl ReclaimConfidence {
    /// Returns the label used in diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Measured => "measured",
            Self::Estimated => "estimated",
            Self::Speculative => "speculative",
        }
    }
}

impl fmt::Display for ReclaimConfidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What a reclaimer believes it could release.
///
/// This is an offer, not an arrival. It is safe to display, safe to rank
/// reclaimers by, and never safe to add to available capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimEstimate {
    /// Bytes the reclaimer believes it could give back.
    pub candidate_bytes: u64,
    /// How much that belief is worth.
    pub confidence: ReclaimConfidence,
}

impl ReclaimEstimate {
    /// An estimate offering nothing.
    ///
    /// The honest answer from a reclaimer with no candidates, and better than
    /// a speculative number, which would keep the arbitrator asking.
    pub const NONE: Self = Self {
        candidate_bytes: 0,
        confidence: ReclaimConfidence::Measured,
    };

    /// A counted, droppable-now estimate.
    pub const fn measured(candidate_bytes: u64) -> Self {
        Self {
            candidate_bytes,
            confidence: ReclaimConfidence::Measured,
        }
    }

    /// An estimate derived from the subsystem's own bookkeeping.
    pub const fn estimated(candidate_bytes: u64) -> Self {
        Self {
            candidate_bytes,
            confidence: ReclaimConfidence::Estimated,
        }
    }

    /// An estimate from indirect signals.
    pub const fn speculative(candidate_bytes: u64) -> Self {
        Self {
            candidate_bytes,
            confidence: ReclaimConfidence::Speculative,
        }
    }

    /// Reports whether the reclaimer is worth asking at all.
    pub const fn offers_anything(&self) -> bool {
        self.candidate_bytes > 0
    }
}

/// A subsystem that can give memory back when asked.
///
/// Implemented by caches, spillers and buffer pools; called only by an
/// arbitrator, and never from inside an allocation path (see the module
/// documentation for why).
///
/// Both methods must return promptly. [`Self::estimate`] is polled while the
/// arbitrator surveys its options, so it reads bookkeeping rather than walking
/// structures. [`Self::request`] *starts* the work and hands back a ticket; it
/// does not perform the reclaim inline, which is what keeps the arbitrator's
/// survey loop from stalling on the slowest subsystem.
pub trait Reclaimer: Send + Sync {
    /// Returns what this subsystem believes it could release right now.
    fn estimate(&self) -> ReclaimEstimate;

    /// Starts reclaiming toward `target_bytes` and returns the ticket the
    /// outcome will be reported under.
    ///
    /// `target_bytes` is a target, not a quota: releasing less is normal and
    /// is reported honestly as `still_retained_bytes`. The implementation must
    /// not block on the reclaim itself.
    fn request(&self, target_bytes: u64) -> ReclaimTicketId;
}

/// How a reclaim request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReclaimStatus {
    /// The reclaimer finished and its numbers are trustworthy.
    Completed,
    /// The reclaimer failed. It may have released bytes on the way, but it
    /// does not vouch for its own end state.
    Failed,
    /// The request was abandoned before the reclaimer answered. Work may still
    /// be running inside the subsystem, which is precisely why nothing here
    /// can be trusted as free capacity.
    TimedOut,
}

impl ReclaimStatus {
    /// Returns the label used in diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed-out",
        }
    }

    /// Reports whether the reclaimer vouched for its end state.
    pub const fn is_completed(self) -> bool {
        matches!(self, Self::Completed)
    }
}

impl fmt::Display for ReclaimStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What one reclaim request actually achieved.
///
/// The three byte counts are separate facts and stay separate. `requested` is
/// what was asked for, `released_confirmed_bytes` is what the reclaimer says
/// it freed, and `still_retained_bytes` is what it is still holding. A
/// consumer reading only the first two would conclude a reclaim succeeded
/// whenever it merely started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimOutcome {
    /// The request this outcome answers.
    pub ticket: ReclaimTicketId,
    /// The target the request carried.
    pub requested_bytes: u64,
    /// Bytes the reclaimer confirms it freed.
    ///
    /// Recorded for every status, because a partial physical release is a real
    /// event worth observing. It becomes *grantable* only when the status is
    /// [`ReclaimStatus::Completed`].
    pub released_confirmed_bytes: u64,
    /// Bytes the reclaimer is still holding after the request.
    pub still_retained_bytes: u64,
    /// How the request ended.
    pub status: ReclaimStatus,
}

impl ReclaimOutcome {
    /// Builds an outcome from its four facts.
    pub const fn new(
        ticket: ReclaimTicketId,
        requested_bytes: u64,
        released_confirmed_bytes: u64,
        still_retained_bytes: u64,
        status: ReclaimStatus,
    ) -> Self {
        Self {
            ticket,
            requested_bytes,
            released_confirmed_bytes,
            still_retained_bytes,
            status,
        }
    }

    /// A reclaim that finished and vouches for its numbers.
    pub const fn completed(
        ticket: ReclaimTicketId,
        requested_bytes: u64,
        released_confirmed_bytes: u64,
        still_retained_bytes: u64,
    ) -> Self {
        Self::new(
            ticket,
            requested_bytes,
            released_confirmed_bytes,
            still_retained_bytes,
            ReclaimStatus::Completed,
        )
    }

    /// A reclaim that failed part-way.
    pub const fn failed(
        ticket: ReclaimTicketId,
        requested_bytes: u64,
        released_confirmed_bytes: u64,
        still_retained_bytes: u64,
    ) -> Self {
        Self::new(
            ticket,
            requested_bytes,
            released_confirmed_bytes,
            still_retained_bytes,
            ReclaimStatus::Failed,
        )
    }

    /// A reclaim abandoned before the reclaimer answered.
    pub const fn timed_out(
        ticket: ReclaimTicketId,
        requested_bytes: u64,
        released_confirmed_bytes: u64,
        still_retained_bytes: u64,
    ) -> Self {
        Self::new(
            ticket,
            requested_bytes,
            released_confirmed_bytes,
            still_retained_bytes,
            ReclaimStatus::TimedOut,
        )
    }

    /// Bytes this outcome may add to grantable capacity.
    ///
    /// Returns `released_confirmed_bytes` only for
    /// [`ReclaimStatus::Completed`], and zero for `Failed` and `TimedOut`.
    ///
    /// This is the single most important method in the module. A failed or
    /// abandoned reclaim has an unknown end state: the subsystem may still be
    /// writing, its frames may be re-pinned, its own accounting may be
    /// mid-update. Treating its partial release as free capacity is how a
    /// process grants memory it does not have and then dies of an
    /// out-of-memory kill that no counter predicted. So the refusal to count
    /// it is expressed here, once, where every consumer must go through it —
    /// rather than as a rule in a document that each call site remembers
    /// differently.
    pub const fn grantable_contribution_bytes(&self) -> u64 {
        match self.status {
            ReclaimStatus::Completed => self.released_confirmed_bytes,
            ReclaimStatus::Failed | ReclaimStatus::TimedOut => 0,
        }
    }

    /// Reports whether the reclaimer reached its target.
    pub const fn met_target(&self) -> bool {
        self.status.is_completed() && self.released_confirmed_bytes >= self.requested_bytes
    }
}

/// One registered reclaimer's current estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimerEstimate {
    /// The reclaimer that produced it.
    pub reclaimer: ReclaimerId,
    /// What it believes it could release.
    pub estimate: ReclaimEstimate,
}

/// The registry's separately kept running totals.
///
/// Nothing here is added into anything else. In particular
/// `confirmed_released_bytes` counts every status while
/// `grantable_released_bytes` counts only completed ones, and the gap between
/// them is exactly the reclaim work that cannot be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimTotals {
    /// Requests started and not yet reported.
    pub requests_in_flight: u32,
    /// Sum of the targets of those in-flight requests. A target, not memory.
    pub requested_bytes_in_flight: u64,
    /// Bytes reported as confirmed released, across every status.
    pub confirmed_released_bytes: u64,
    /// Bytes reported as confirmed released by completed requests only. The
    /// only total that may inform grantable capacity.
    pub grantable_released_bytes: u64,
    /// Bytes reported as still retained by the most recent outcomes.
    pub still_retained_bytes: u64,
    /// Requests reported as completed.
    pub completed_requests: u64,
    /// Requests reported as failed.
    pub failed_requests: u64,
    /// Requests reported as timed out.
    pub timed_out_requests: u64,
}

impl ReclaimTotals {
    /// Bytes that reclaim work has actually made grantable.
    ///
    /// Identical to `grantable_released_bytes`, exposed as a method so a
    /// consumer looking for "how much did reclaim give us" cannot reach for
    /// `confirmed_released_bytes` by mistake.
    pub const fn grantable_contribution_bytes(&self) -> u64 {
        self.grantable_released_bytes
    }

    /// Bytes reported released by requests that did not vouch for their end
    /// state. Diagnostic only; never grantable.
    pub const fn untrusted_released_bytes(&self) -> u64 {
        self.confirmed_released_bytes
            .saturating_sub(self.grantable_released_bytes)
    }
}

/// Why a reclaim request could not be started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclaimRequestError {
    /// No reclaimer is registered under that identity. Normal in a race with
    /// deregistration, and not an error worth escalating.
    NotRegistered {
        /// The identity that was asked for.
        reclaimer: ReclaimerId,
    },
    /// Too many requests are already in flight.
    ///
    /// The in-flight table is bounded like every other registry, so an
    /// arbitrator that starts requests faster than it reports outcomes is
    /// refused instead of growing the table without limit.
    Exhausted {
        /// The typed metadata refusal.
        cause: CapacityError,
    },
}

impl fmt::Display for ReclaimRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRegistered { reclaimer } => {
                write!(f, "reclaim request rejected: {reclaimer} is not registered")
            }
            Self::Exhausted { cause } => write!(f, "reclaim request rejected: {cause}"),
        }
    }
}

impl Error for ReclaimRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Exhausted { cause } => Some(cause),
            Self::NotRegistered { .. } => None,
        }
    }
}

/// The bounded registry of reclaimers and their in-flight requests.
///
/// The registry is a directory and a ledger, not a scheduler: it knows who can
/// reclaim, what they believe they could give back, and what came of each
/// request. Choosing whom to ask and when is the arbitrator's decision.
///
/// Registrations and in-flight requests are separately bounded by
/// [`MetadataRegistryLabel::Reclaimers`], so neither a subsystem that
/// registers in a loop nor an arbitrator that never reports an outcome can
/// grow the core's state without limit.
pub struct ReclaimRegistry {
    registration_slots: BoundedSlots,
    request_slots: BoundedSlots,
    ids: IdSource,
    registered: Mutex<BTreeMap<ReclaimerId, Arc<dyn Reclaimer>>>,
    in_flight: Mutex<BTreeMap<ReclaimTicketId, u64>>,
    confirmed_released: AtomicU64,
    grantable_released: AtomicU64,
    still_retained: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
}

impl fmt::Debug for ReclaimRegistry {
    /// Reports the registry's shape without naming a reclaimer.
    ///
    /// A registered subsystem is not required to be `Debug` — it is arbitrary
    /// application code behind a trait object — so the registry prints its own
    /// counters instead of trying to print its members.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let totals = self.totals();
        f.debug_struct("ReclaimRegistry")
            .field("registered", &self.registered_count())
            .field("registration_limit", &self.registration_limit())
            .field("requests_in_flight", &totals.requests_in_flight)
            .field("grantable_released_bytes", &totals.grantable_released_bytes)
            .field(
                "untrusted_released_bytes",
                &totals.untrusted_released_bytes(),
            )
            .finish()
    }
}

impl ReclaimRegistry {
    /// Creates an empty registry bounded by `budget`.
    pub fn new(budget: &MetadataBudget) -> Self {
        Self {
            registration_slots: BoundedSlots::from_budget(
                budget,
                MetadataRegistryLabel::Reclaimers,
            ),
            request_slots: BoundedSlots::from_budget(budget, MetadataRegistryLabel::Reclaimers),
            ids: IdSource::new(),
            registered: Mutex::new(BTreeMap::new()),
            in_flight: Mutex::new(BTreeMap::new()),
            confirmed_released: AtomicU64::new(0),
            grantable_released: AtomicU64::new(0),
            still_retained: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
        }
    }

    /// Registers a reclaimer and returns the identity it is addressed by.
    ///
    /// Refused with [`CapacityError::MetadataExhausted`] when the registry is
    /// full. An existing registration is never displaced to make room: the
    /// subsystem behind it is still holding memory, and forgetting it would
    /// remove the only way to ask for that memory back.
    pub fn register(&self, reclaimer: Arc<dyn Reclaimer>) -> Result<ReclaimerId, CapacityError> {
        self.registration_slots.try_acquire()?;
        let id = ReclaimerId::new(self.ids.next_raw());
        Self::guard(&self.registered).insert(id, reclaimer);
        Ok(id)
    }

    /// Removes a registration and reports whether one was there.
    ///
    /// In-flight requests are deliberately left alone: their outcomes are
    /// still worth recording, and dropping them would strand
    /// `requests_in_flight` above zero forever.
    pub fn deregister(&self, reclaimer: ReclaimerId) -> bool {
        let removed = Self::guard(&self.registered).remove(&reclaimer).is_some();
        if removed {
            self.registration_slots.release();
        }
        removed
    }

    /// Returns the number of registered reclaimers.
    pub fn registered_count(&self) -> u32 {
        self.registration_slots.in_use()
    }

    /// Returns the configured registration limit.
    pub fn registration_limit(&self) -> u32 {
        self.registration_slots.limit()
    }

    /// Polls every registered reclaimer for its current estimate.
    ///
    /// The registry lock is released before any reclaimer is called. A
    /// subsystem is free to touch its own state, or even to deregister itself,
    /// while answering — holding the lock across the callback would turn that
    /// into a deadlock.
    pub fn estimates(&self) -> Vec<ReclaimerEstimate> {
        let snapshot: Vec<(ReclaimerId, Arc<dyn Reclaimer>)> = Self::guard(&self.registered)
            .iter()
            .map(|(id, reclaimer)| (*id, Arc::clone(reclaimer)))
            .collect();
        snapshot
            .into_iter()
            .map(|(reclaimer, handle)| ReclaimerEstimate {
                reclaimer,
                estimate: handle.estimate(),
            })
            .collect()
    }

    /// Asks one reclaimer to release toward `target_bytes` and records the
    /// request as in flight.
    ///
    /// Call this from an arbitrator, never from an allocation path. Like
    /// [`Self::estimates`], the lock is dropped before the reclaimer is
    /// called.
    pub fn request(
        &self,
        reclaimer: ReclaimerId,
        target_bytes: u64,
    ) -> Result<ReclaimTicketId, ReclaimRequestError> {
        let handle = Self::guard(&self.registered)
            .get(&reclaimer)
            .map(Arc::clone);
        let Some(handle) = handle else {
            return Err(ReclaimRequestError::NotRegistered { reclaimer });
        };
        self.request_slots
            .try_acquire()
            .map_err(|cause| ReclaimRequestError::Exhausted { cause })?;
        let ticket = handle.request(target_bytes);
        if Self::guard(&self.in_flight)
            .insert(ticket, target_bytes)
            .is_some()
        {
            // The reclaimer reused a ticket that is still in flight. The
            // table now holds one entry for two requests, so the slot just
            // reserved goes back rather than being stranded.
            self.request_slots.release();
        }
        Ok(ticket)
    }

    /// Records what a request achieved and reports whether it matched an
    /// in-flight ticket.
    ///
    /// An unmatched outcome is still recorded, because losing a confirmed
    /// release would leave the totals understating what came back. A `false`
    /// return means the ticket was already reported or never started — worth a
    /// diagnostic, not worth discarding the facts.
    pub fn record_outcome(&self, outcome: &ReclaimOutcome) -> bool {
        let matched = Self::guard(&self.in_flight)
            .remove(&outcome.ticket)
            .is_some();
        if matched {
            self.request_slots.release();
        }
        self.confirmed_released
            .fetch_add(outcome.released_confirmed_bytes, Ordering::Relaxed);
        // Only a completed request contributes; see
        // `ReclaimOutcome::grantable_contribution_bytes`.
        self.grantable_released
            .fetch_add(outcome.grantable_contribution_bytes(), Ordering::Relaxed);
        self.still_retained
            .store(outcome.still_retained_bytes, Ordering::Relaxed);
        let counter = match outcome.status {
            ReclaimStatus::Completed => &self.completed,
            ReclaimStatus::Failed => &self.failed,
            ReclaimStatus::TimedOut => &self.timed_out,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        matched
    }

    /// Returns the registry's running totals.
    pub fn totals(&self) -> ReclaimTotals {
        let in_flight = Self::guard(&self.in_flight);
        let requests_in_flight = in_flight.len() as u32;
        let requested_bytes_in_flight = in_flight
            .values()
            .fold(0u64, |total, target| total.saturating_add(*target));
        drop(in_flight);
        ReclaimTotals {
            requests_in_flight,
            requested_bytes_in_flight,
            confirmed_released_bytes: self.confirmed_released.load(Ordering::Relaxed),
            grantable_released_bytes: self.grantable_released.load(Ordering::Relaxed),
            still_retained_bytes: self.still_retained.load(Ordering::Relaxed),
            completed_requests: self.completed.load(Ordering::Relaxed),
            failed_requests: self.failed.load(Ordering::Relaxed),
            timed_out_requests: self.timed_out.load(Ordering::Relaxed),
        }
    }

    /// Locks one of the registry's tables, recovering from poisoning.
    ///
    /// A panic in a reclaimer callback must not make the registry permanently
    /// unusable: the subsystems it names are still holding memory, and the
    /// arbitrator still needs to be able to ask for it.
    fn guard<T>(table: &Mutex<T>) -> MutexGuard<'_, T> {
        table
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reclaimer that answers from fixed numbers, so a test asserts on the
    /// registry rather than on a subsystem's behaviour.
    #[derive(Debug)]
    struct StubReclaimer {
        estimate: ReclaimEstimate,
        ticket: ReclaimTicketId,
        requested: AtomicU64,
    }

    impl StubReclaimer {
        fn new(candidate_bytes: u64, ticket: u64) -> Arc<Self> {
            Arc::new(Self {
                estimate: ReclaimEstimate::measured(candidate_bytes),
                ticket: ReclaimTicketId::new(ticket),
                requested: AtomicU64::new(0),
            })
        }
    }

    impl Reclaimer for StubReclaimer {
        fn estimate(&self) -> ReclaimEstimate {
            self.estimate
        }

        fn request(&self, target_bytes: u64) -> ReclaimTicketId {
            self.requested.store(target_bytes, Ordering::Relaxed);
            self.ticket
        }
    }

    fn registry() -> ReclaimRegistry {
        ReclaimRegistry::new(&MetadataBudget::with_defaults())
    }

    #[test]
    fn only_a_completed_outcome_contributes_grantable_bytes() {
        let ticket = ReclaimTicketId::new(1);
        assert_eq!(
            ReclaimOutcome::completed(ticket, 1_000, 800, 200).grantable_contribution_bytes(),
            800
        );
        assert_eq!(
            ReclaimOutcome::failed(ticket, 1_000, 800, 200).grantable_contribution_bytes(),
            0,
            "a failed reclaim must never increase grantable capacity"
        );
        assert_eq!(
            ReclaimOutcome::timed_out(ticket, 1_000, 800, 200).grantable_contribution_bytes(),
            0,
            "an abandoned reclaim must never increase grantable capacity"
        );
    }

    #[test]
    fn met_target_requires_completion_as_well_as_bytes() {
        let ticket = ReclaimTicketId::new(2);
        assert!(ReclaimOutcome::completed(ticket, 100, 100, 0).met_target());
        assert!(!ReclaimOutcome::completed(ticket, 100, 99, 1).met_target());
        assert!(!ReclaimOutcome::failed(ticket, 100, 100, 0).met_target());
    }

    #[test]
    fn estimates_are_polled_without_holding_the_registry_lock() {
        let registry = registry();
        let first = registry
            .register(StubReclaimer::new(4_096, 10))
            .expect("registered");
        let second = registry
            .register(StubReclaimer::new(0, 11))
            .expect("registered");
        let estimates = registry.estimates();
        assert_eq!(estimates.len(), 2);
        assert_eq!(estimates[0].reclaimer, first);
        assert_eq!(estimates[0].estimate, ReclaimEstimate::measured(4_096));
        assert!(estimates[0].estimate.offers_anything());
        assert_eq!(estimates[1].reclaimer, second);
        assert!(!estimates[1].estimate.offers_anything());
    }

    #[test]
    fn deregistration_frees_a_slot_and_is_reported_once() {
        let registry = registry();
        let id = registry
            .register(StubReclaimer::new(1, 12))
            .expect("registered");
        assert_eq!(registry.registered_count(), 1);
        assert!(registry.deregister(id));
        assert_eq!(registry.registered_count(), 0);
        assert!(!registry.deregister(id), "a second removal finds nothing");
        assert_eq!(registry.registered_count(), 0);
    }

    #[test]
    fn requesting_an_unregistered_reclaimer_is_typed_and_costs_no_slot() {
        let registry = registry();
        let missing = ReclaimerId::new(99);
        assert_eq!(
            registry.request(missing, 4_096).unwrap_err(),
            ReclaimRequestError::NotRegistered { reclaimer: missing }
        );
        assert_eq!(registry.totals().requests_in_flight, 0);
    }

    #[test]
    fn in_flight_requests_and_confirmed_releases_stay_separate_facts() {
        let registry = registry();
        let reclaimer = registry
            .register(StubReclaimer::new(8_192, 20))
            .expect("registered");
        let ticket = registry.request(reclaimer, 4_096).expect("requested");

        let pending = registry.totals();
        assert_eq!(pending.requests_in_flight, 1);
        assert_eq!(pending.requested_bytes_in_flight, 4_096);
        assert_eq!(
            pending.grantable_contribution_bytes(),
            0,
            "a request in flight has released nothing"
        );

        assert!(registry.record_outcome(&ReclaimOutcome::completed(ticket, 4_096, 3_000, 5_192)));
        let settled = registry.totals();
        assert_eq!(settled.requests_in_flight, 0);
        assert_eq!(settled.requested_bytes_in_flight, 0);
        assert_eq!(settled.confirmed_released_bytes, 3_000);
        assert_eq!(settled.grantable_contribution_bytes(), 3_000);
        assert_eq!(settled.still_retained_bytes, 5_192);
        assert_eq!(settled.completed_requests, 1);
        assert_eq!(settled.untrusted_released_bytes(), 0);
    }

    #[test]
    fn a_failed_outcome_is_recorded_but_adds_nothing_grantable() {
        let registry = registry();
        let reclaimer = registry
            .register(StubReclaimer::new(8_192, 21))
            .expect("registered");
        let ticket = registry.request(reclaimer, 8_192).expect("requested");
        assert!(registry.record_outcome(&ReclaimOutcome::failed(ticket, 8_192, 1_024, 7_168)));

        let totals = registry.totals();
        assert_eq!(totals.failed_requests, 1);
        assert_eq!(totals.confirmed_released_bytes, 1_024);
        assert_eq!(totals.grantable_contribution_bytes(), 0);
        assert_eq!(
            totals.untrusted_released_bytes(),
            1_024,
            "the gap is exactly the reclaim work that cannot be trusted"
        );
    }

    #[test]
    fn an_unmatched_outcome_is_still_recorded_and_reported_as_unmatched() {
        let registry = registry();
        let outcome = ReclaimOutcome::completed(ReclaimTicketId::new(77), 512, 512, 0);
        assert!(!registry.record_outcome(&outcome));
        assert_eq!(registry.totals().grantable_contribution_bytes(), 512);
    }

    #[test]
    fn in_flight_requests_are_bounded_like_every_other_registry() {
        let budget = MetadataBudget::with_defaults().with_reclaimers(1);
        let registry = ReclaimRegistry::new(&budget);
        assert_eq!(registry.registration_limit(), 1);
        let reclaimer = registry
            .register(StubReclaimer::new(1, 30))
            .expect("registered");
        let refused = registry
            .register(StubReclaimer::new(1, 31))
            .expect_err("the registration limit is one");
        assert_eq!(
            refused,
            CapacityError::MetadataExhausted {
                registry: MetadataRegistryLabel::Reclaimers,
                limit: 1,
            }
        );

        let ticket = registry.request(reclaimer, 64).expect("first request");
        let second = registry
            .request(reclaimer, 64)
            .expect_err("the in-flight limit is one");
        assert!(matches!(second, ReclaimRequestError::Exhausted { .. }));
        assert!(registry.record_outcome(&ReclaimOutcome::completed(ticket, 64, 64, 0)));
        assert!(
            registry.request(reclaimer, 64).is_ok(),
            "reporting an outcome frees the in-flight slot"
        );
    }
}
