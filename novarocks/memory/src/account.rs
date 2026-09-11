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

//! The strict account tree (MEM-1 wave-1 T02).
//!
//! Hard limits and capacity propagate along one tree: a process root with
//! resource-group, work, attempt, task and owner branches, plus service and
//! preparation branches for state that has no attempt. A work belongs to
//! exactly one hard-limit group at a time. Components, resource classes and
//! holder exposure are observation labels, not extra tree edges, so no
//! allocation ever evaluates a general graph.
//!
//! # The accounting model
//!
//! Every account holds capacity it obtained from its parent. That amount,
//! `reserved`, *is* the account's total commitment `C`: the parent cannot hand
//! the same bytes to anyone else, whether or not the child is currently using
//! them. What the child does inside its own reservation is a local matter and
//! is visible in the decomposition:
//!
//! ```text
//! reserved = local_free + granted + live + bounded + handed_down
//! ```
//!
//! - `local_free` is slack the account holds but has not issued.
//! - `granted` is `F` issued from this account and not yet fulfilled.
//! - `live` is `L` fulfilled here and still alive.
//! - `bounded` is `O`, the third-party upper bound authorised here.
//! - `handed_down` is what children hold, which is their own `reserved`.
//!
//! The root is the one exception: capacity it has not handed out is not a
//! commitment, so `C(root) = reserved - local_free` while `C(child) =
//! reserved`. The root's `reserved` starts at the managed capacity `B` and
//! only ever rises above it when the process is honestly over-committed.
//!
//! # Why the root stays off the hot path
//!
//! An ordinary request is served from `local_free` with one compare-and-swap
//! on the account's own counter. Only topping that slack up, or returning it,
//! walks the parent chain, and top-ups are quantised so that walk is rare.
//! The hard bound `C <= B` is therefore enforced once per top-up at the root,
//! not once per allocation.
//!
//! # Aggregation for snapshots
//!
//! `L`, `F` and `O` in a snapshot are subtree sums, which is the view MEM-1
//! describes: a process that handed 100 MiB down and whose child allocated
//! 60 MiB reports `L = 60`, `F = 40`, `C = 100`. An account's own `local_free`
//! counts towards `F` from its parent's point of view, because the parent has
//! already committed it. Walking the subtree happens only when a snapshot is
//! taken; it is never on an allocation path.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::error::{CapacityError, ConstraintKind, MetadataRegistryLabel};
use crate::ids::{AccountId, AccountKind, ExternalRef, PolicyVersion};
use crate::policy::{LimitDimension, PolicyInstallOutcome, PolicyLimit};
use crate::snapshot::{AccountSnapshot, EventRing, MemoryEventKind};

/// How much capacity a top-up moves at once.
///
/// Quantising the walk up the tree is what keeps the root off the hot path: a
/// small account tops up in small steps so it does not strand capacity, and a
/// large one takes bigger steps so a long-running operator is not repeating
/// the same walk thousands of times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopUpPolicy {
    small_threshold_bytes: u64,
    small_step_bytes: u64,
    medium_threshold_bytes: u64,
    medium_step_bytes: u64,
    large_step_bytes: u64,
}

impl TopUpPolicy {
    /// The default step table: 1 MiB below 16 MiB of commitment, 4 MiB below
    /// 64 MiB, 8 MiB above that.
    pub const DEFAULT: Self = Self {
        small_threshold_bytes: 16 * 1024 * 1024,
        small_step_bytes: 1024 * 1024,
        medium_threshold_bytes: 64 * 1024 * 1024,
        medium_step_bytes: 4 * 1024 * 1024,
        large_step_bytes: 8 * 1024 * 1024,
    };

    /// A table with one uniform step, used by tests that want predictable
    /// top-up arithmetic.
    pub const fn uniform(step_bytes: u64) -> Self {
        let step = if step_bytes == 0 { 1 } else { step_bytes };
        Self {
            small_threshold_bytes: 0,
            small_step_bytes: step,
            medium_threshold_bytes: 0,
            medium_step_bytes: step,
            large_step_bytes: step,
        }
    }

    /// Builds a step table. Zero steps are raised to one byte so a top-up
    /// always makes progress.
    pub const fn new(
        small_threshold_bytes: u64,
        small_step_bytes: u64,
        medium_threshold_bytes: u64,
        medium_step_bytes: u64,
        large_step_bytes: u64,
    ) -> Self {
        Self {
            small_threshold_bytes,
            small_step_bytes: if small_step_bytes == 0 {
                1
            } else {
                small_step_bytes
            },
            medium_threshold_bytes,
            medium_step_bytes: if medium_step_bytes == 0 {
                1
            } else {
                medium_step_bytes
            },
            large_step_bytes: if large_step_bytes == 0 {
                1
            } else {
                large_step_bytes
            },
        }
    }

    /// Returns the step size for an account at the given commitment.
    pub const fn step_for(&self, committed_bytes: u64) -> u64 {
        if committed_bytes < self.small_threshold_bytes {
            self.small_step_bytes
        } else if committed_bytes < self.medium_threshold_bytes {
            self.medium_step_bytes
        } else {
            self.large_step_bytes
        }
    }

    /// Returns how much to ask the parent for, given the shortfall and the
    /// account's current commitment.
    ///
    /// The result is at least the shortfall: quantisation may take more than
    /// needed, never less.
    pub const fn amount_for(&self, shortfall_bytes: u64, committed_bytes: u64) -> u64 {
        let step = self.step_for(committed_bytes);
        match shortfall_bytes.div_ceil(step).checked_mul(step) {
            Some(rounded) => rounded,
            // A shortfall this close to the address space cannot be satisfied
            // anyway; asking for the exact amount lets the refusal come from
            // the capacity check rather than from an overflow.
            None => shortfall_bytes,
        }
    }
}

impl Default for TopUpPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What an idle-capacity reclamation actually achieved.
///
/// An arbitrator may take back capacity an account holds but has not issued.
/// It may not take capacity a grant already reserved, capacity already
/// fulfilled, or capacity the floor protects, and this outcome says which is
/// which rather than reporting one number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShrinkOutcome {
    /// Bytes actually returned to the parent.
    pub reclaimed_bytes: u64,
    /// Idle bytes left in place because the floor protects them.
    pub kept_for_floor_bytes: u64,
    /// Bytes left in place because grants, live charges, bounds or children
    /// hold them.
    pub kept_for_grants_bytes: u64,
}

/// State shared by every account of one authority.
#[derive(Debug)]
pub struct AccountTreeShared {
    next_account_id: AtomicU64,
    live_accounts: AtomicU32,
    max_accounts: u32,
    capacity_bytes: u64,
    top_up: TopUpPolicy,
    events: EventRing,
}

impl AccountTreeShared {
    /// Creates the shared state for one authority.
    pub fn new(
        capacity_bytes: u64,
        max_accounts: u32,
        top_up: TopUpPolicy,
        event_capacity: u32,
    ) -> Self {
        Self {
            next_account_id: AtomicU64::new(1),
            live_accounts: AtomicU32::new(0),
            max_accounts: max_accounts.max(1),
            capacity_bytes,
            top_up,
            events: EventRing::new(event_capacity),
        }
    }

    /// Returns the event ring, so observers can read change notifications.
    pub const fn events(&self) -> &EventRing {
        &self.events
    }

    /// Returns the number of live accounts.
    pub fn live_accounts(&self) -> u32 {
        self.live_accounts.load(Ordering::Relaxed)
    }

    /// Returns the configured account limit.
    pub const fn max_accounts(&self) -> u32 {
        self.max_accounts
    }

    /// Returns the managed capacity `B`.
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the top-up step table.
    pub const fn top_up_policy(&self) -> TopUpPolicy {
        self.top_up
    }

    fn claim_account_slot(&self) -> Result<(), CapacityError> {
        let mut current = self.live_accounts.load(Ordering::Acquire);
        loop {
            if current >= self.max_accounts {
                self.events.record(MemoryEventKind::MetadataExhausted {
                    registry: MetadataRegistryLabel::Accounts,
                });
                return Err(CapacityError::MetadataExhausted {
                    registry: MetadataRegistryLabel::Accounts,
                    limit: self.max_accounts,
                });
            }
            match self.live_accounts.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn release_account_slot(&self) {
        let _ = self
            .live_accounts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            });
    }
}

/// One node of the strict account tree.
///
/// Accounts are reached through [`AccountHandle`]; this type is the shared
/// state behind that handle.
#[derive(Debug)]
pub struct Account {
    id: AccountId,
    kind: AccountKind,
    external: ExternalRef,
    /// The parent link. `None` marks the single process root.
    parent: Option<Arc<Account>>,
    /// Weak child links, upgraded only when a snapshot walks the subtree.
    children: Mutex<Vec<Weak<Account>>>,
    /// Capacity obtained from the parent. At the root this starts at `B`.
    reserved: AtomicU64,
    /// Slack held but not issued.
    local_free: AtomicU64,
    /// `F` issued here and not yet fulfilled.
    granted: AtomicU64,
    /// `L` fulfilled here and still alive.
    live: AtomicU64,
    /// `O` authorised here for bounded third-party steps.
    bounded: AtomicU64,
    /// Capacity children hold.
    handed_down: AtomicU64,
    /// Capacity ordinary competition may not revoke.
    floor: AtomicU64,
    /// Bytes held beyond the applicable bound.
    excess: AtomicU64,
    /// Bytes ever charged here without a grant.
    unbudgeted: AtomicU64,
    /// Closed to growth because the account is over its bound. This lifts on
    /// its own once the commitment is back inside the bound.
    growth_frozen: AtomicBool,
    /// Closed to growth because unbudgeted allocation was absorbed. This does
    /// not lift on its own: releasing the bytes does not make the sizing that
    /// produced them correct, so an arbitrator has to clear it.
    unbudgeted_frozen: AtomicBool,
    /// Closed to growth because the work was cancelled.
    closed: AtomicBool,
    /// This account's own peak commitment.
    peak_committed: AtomicU64,
    /// This account's own peak live allocation.
    peak_live: AtomicU64,
    /// The installed policy limit, if any.
    policy: Mutex<Option<PolicyLimit>>,
    /// The version the account currently carries.
    policy_version: AtomicU64,
    /// Shared authority state.
    shared: Arc<AccountTreeShared>,
}

impl Account {
    /// Returns this account's identity.
    pub const fn id(&self) -> AccountId {
        self.id
    }

    /// Returns what the account stands for.
    pub const fn kind(&self) -> AccountKind {
        self.kind
    }

    /// Returns the neutral external reference the caller attached.
    pub const fn external_ref(&self) -> ExternalRef {
        self.external
    }

    /// Reports whether this is the process root.
    pub const fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    /// Returns the shared authority state.
    pub fn shared(&self) -> &Arc<AccountTreeShared> {
        &self.shared
    }

    /// Returns this account's total commitment `C`.
    ///
    /// For a child this is everything it holds from its parent. For the root
    /// it is the managed capacity minus the slack never handed out.
    pub fn committed_bytes(&self) -> u64 {
        let reserved = self.reserved.load(Ordering::Acquire);
        if self.is_root() {
            reserved.saturating_sub(self.local_free.load(Ordering::Acquire))
        } else {
            reserved
        }
    }

    /// Returns slack this account holds but has not issued.
    pub fn local_free_bytes(&self) -> u64 {
        self.local_free.load(Ordering::Acquire)
    }

    /// Returns `F` issued from this account and not yet fulfilled.
    pub fn own_granted_bytes(&self) -> u64 {
        self.granted.load(Ordering::Acquire)
    }

    /// Returns `L` fulfilled at this account and still alive.
    pub fn own_live_bytes(&self) -> u64 {
        self.live.load(Ordering::Acquire)
    }

    /// Returns `O` authorised at this account.
    pub fn own_bounded_bytes(&self) -> u64 {
        self.bounded.load(Ordering::Acquire)
    }

    /// Returns capacity this account's children hold.
    pub fn handed_down_bytes(&self) -> u64 {
        self.handed_down.load(Ordering::Acquire)
    }

    /// Returns the floor this account keeps against ordinary competition.
    pub fn floor_bytes(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// Returns bytes held beyond the applicable bound.
    pub fn excess_bytes(&self) -> u64 {
        self.excess.load(Ordering::Acquire)
    }

    /// Returns bytes ever charged here without a grant.
    pub fn unbudgeted_bytes(&self) -> u64 {
        self.unbudgeted.load(Ordering::Acquire)
    }

    /// Reports whether growth is closed because unbudgeted allocation is
    /// unresolved.
    pub fn is_frozen_by_unbudgeted(&self) -> bool {
        self.unbudgeted_frozen.load(Ordering::Acquire)
    }

    /// Reports whether the account is closed to growth for any reason.
    pub fn is_closed_to_growth(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            || self.growth_frozen.load(Ordering::Acquire)
            || self.unbudgeted_frozen.load(Ordering::Acquire)
    }

    /// Returns the version the account currently carries.
    pub fn policy_version(&self) -> PolicyVersion {
        PolicyVersion::new(self.policy_version.load(Ordering::Acquire))
    }

    /// Returns this account's own peak commitment.
    pub fn peak_committed_bytes(&self) -> u64 {
        self.peak_committed.load(Ordering::Relaxed)
    }

    /// Returns this account's own peak live allocation.
    pub fn peak_live_bytes(&self) -> u64 {
        self.peak_live.load(Ordering::Relaxed)
    }

    /// Reports whether the local decomposition of `reserved` adds up.
    ///
    /// Counters are read without a global lock, so this is transiently false
    /// while another thread is mid-transition, and it is deliberately false at
    /// the common ancestor for the duration of a sponsor transfer. Tests
    /// assert it at quiescent points.
    pub fn is_locally_consistent(&self) -> bool {
        let reserved = self.reserved.load(Ordering::Acquire);
        let parts = self
            .local_free
            .load(Ordering::Acquire)
            .saturating_add(self.granted.load(Ordering::Acquire))
            .saturating_add(self.live.load(Ordering::Acquire))
            .saturating_add(self.bounded.load(Ordering::Acquire))
            .saturating_add(self.handed_down.load(Ordering::Acquire));
        reserved == parts
    }

    /// Returns the bound this account's commitment is judged against: its own
    /// policy limit, or the managed capacity `B` at the root.
    fn effective_bound_bytes(&self) -> Option<u64> {
        match self.policy_limit() {
            Some(limit) => Some(limit.limit_bytes()),
            None if self.is_root() => Some(self.shared.capacity_bytes),
            None => None,
        }
    }

    fn policy_limit(&self) -> Option<PolicyLimit> {
        *self
            .policy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record_peak_committed(&self) {
        let committed = self.committed_bytes();
        let mut observed = self.peak_committed.load(Ordering::Relaxed);
        while committed > observed {
            match self.peak_committed.compare_exchange_weak(
                observed,
                committed,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    fn record_peak_live(&self, live: u64) {
        let mut observed = self.peak_live.load(Ordering::Relaxed);
        while live > observed {
            match self.peak_live.compare_exchange_weak(
                observed,
                live,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    /// Takes `amount` out of this account's slack, refusing rather than
    /// borrowing when the slack is short.
    ///
    /// This is one compare-and-swap on the account's own counter, and it is
    /// the single point where two competitors for the same idle bytes are
    /// resolved: exactly one of them succeeds.
    fn take_local_free(&self, amount: u64) -> bool {
        let mut current = self.local_free.load(Ordering::Acquire);
        loop {
            if current < amount {
                return false;
            }
            match self.local_free.compare_exchange_weak(
                current,
                current - amount,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self.is_root() {
                        self.record_peak_committed();
                    }
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Takes up to `amount` out of slack, returning what it actually got.
    fn take_local_free_up_to(&self, amount: u64) -> u64 {
        let mut current = self.local_free.load(Ordering::Acquire);
        loop {
            let taken = current.min(amount);
            if taken == 0 {
                return 0;
            }
            match self.local_free.compare_exchange_weak(
                current,
                current - taken,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self.is_root() {
                        self.record_peak_committed();
                    }
                    return taken;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn give_local_free(&self, amount: u64) {
        if amount > 0 {
            self.local_free.fetch_add(amount, Ordering::AcqRel);
        }
    }

    fn denied(&self, constraint: ConstraintKind, requested: u64, available: u64) -> CapacityError {
        self.shared.events.record(MemoryEventKind::GrantDenied {
            scope: self.id,
            constraint,
            requested,
        });
        CapacityError::Denied {
            scope: self.id,
            constraint,
            requested,
            available,
            version: self.policy_version(),
        }
    }

    fn refuse_if_closed(&self) -> Result<(), CapacityError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(CapacityError::Cancelled { scope: self.id });
        }
        if self.growth_frozen.load(Ordering::Acquire) {
            return Err(CapacityError::FrozenByExcess {
                scope: self.id,
                excess_bytes: self.excess_bytes(),
            });
        }
        if self.unbudgeted_frozen.load(Ordering::Acquire) {
            return Err(CapacityError::FrozenByExcess {
                scope: self.id,
                excess_bytes: self.unbudgeted_bytes(),
            });
        }
        Ok(())
    }

    /// Hands `amount` down to a child, topping this account up first when its
    /// own slack is short.
    fn hand_down(&self, amount: u64) -> Result<(), CapacityError> {
        loop {
            if self.take_local_free(amount) {
                self.handed_down.fetch_add(amount, Ordering::AcqRel);
                return Ok(());
            }
            let held = self.local_free.load(Ordering::Acquire);
            if self.parent.is_none() {
                return Err(self.denied(ConstraintKind::ProcessCapacity, amount, held));
            }
            self.reserve_more(amount.saturating_sub(held))?;
        }
    }

    /// Obtains at least `shortfall` more capacity from the parent.
    ///
    /// The account's own policy is claimed first, by optimistically raising
    /// `reserved`, and that claim is rolled back if the parent refuses. The
    /// intermediate state over-reports the commitment rather than
    /// under-reporting it, and the claimed bytes are never usable until the
    /// parent has actually handed them over.
    fn reserve_more(&self, shortfall: u64) -> Result<(), CapacityError> {
        let parent = match &self.parent {
            Some(parent) => parent,
            None => {
                return Err(CapacityError::Unsupported {
                    detail: "the process root cannot reserve from a parent",
                });
            }
        };
        self.refuse_if_closed()?;

        let step_basis = self.reserved.load(Ordering::Acquire);
        let quantised = self.shared.top_up.amount_for(shortfall, step_basis);
        let claimed = self.claim_own_bound(quantised, shortfall)?;
        match parent.hand_down(claimed) {
            Ok(()) => {
                self.give_local_free(claimed);
                self.record_peak_committed();
                Ok(())
            }
            Err(error) => {
                self.reserved.fetch_sub(claimed, Ordering::AcqRel);
                Err(error)
            }
        }
    }

    /// Raises `reserved` under this account's own bound.
    ///
    /// Returns the amount actually claimed: the quantised amount when the
    /// bound allows it, and the bare shortfall when quantisation would
    /// overshoot a bound that still admits the request.
    fn claim_own_bound(&self, quantised: u64, shortfall: u64) -> Result<u64, CapacityError> {
        let bound = self.effective_bound_bytes();
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let amount = match bound {
                None => quantised,
                Some(bound) => {
                    let remaining = bound.saturating_sub(current);
                    if remaining < shortfall {
                        return Err(self.denied(
                            ConstraintKind::AccountPolicy,
                            shortfall,
                            remaining,
                        ));
                    }
                    quantised.min(remaining).max(shortfall)
                }
            };
            let next = match current.checked_add(amount) {
                Some(next) => next,
                None => {
                    return Err(self.denied(ConstraintKind::AccountPolicy, amount, 0));
                }
            };
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(amount),
                Err(observed) => current = observed,
            }
        }
    }

    /// Serves `amount` from local slack, topping up from the parent when the
    /// slack is short.
    fn acquire_for_grant(&self, amount: u64) -> Result<(), CapacityError> {
        if amount == 0 {
            return Ok(());
        }
        self.refuse_if_closed()?;
        loop {
            if self.take_local_free(amount) {
                self.granted.fetch_add(amount, Ordering::AcqRel);
                return Ok(());
            }
            let held = self.local_free.load(Ordering::Acquire);
            if self.parent.is_none() {
                return Err(self.denied(ConstraintKind::ProcessCapacity, amount, held));
            }
            self.reserve_more(amount.saturating_sub(held))
                .map_err(|error| error.for_original_request(amount))?;
        }
    }

    /// Returns an unfulfilled grant remainder to local slack.
    fn return_granted(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        let _ = self
            .granted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
        self.give_local_free(amount);
    }

    /// Turns granted capacity into live allocation.
    fn commit_live(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        // `L` is raised before `F` is lowered so a concurrent reader never
        // sees the commitment dip below its true value.
        let live = self.live.fetch_add(amount, Ordering::AcqRel) + amount;
        let _ = self
            .granted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
        self.record_peak_live(live);
    }

    /// Records live allocation the account had no granted capacity for.
    ///
    /// The memory exists, so refusing to account for it would hide it. The
    /// account absorbs it, its commitment rises, growth freezes until an
    /// arbitrator resolves the excess, and the parent chain sees the same
    /// bytes as a commitment it did not authorise. This is the honest,
    /// deliberately visible path — not a silent overdraft.
    fn absorb_excess_live(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.reserved.fetch_add(amount, Ordering::AcqRel);
        let live = self.live.fetch_add(amount, Ordering::AcqRel) + amount;
        self.unbudgeted.fetch_add(amount, Ordering::AcqRel);
        self.record_peak_live(live);
        self.record_peak_committed();
        self.recompute_excess();
        // Unbudgeted absorption freezes growth on its own account, whether or
        // not any bound was exceeded. The process may well have had the
        // capacity; what is broken is the caller's sizing, and letting it
        // carry on allocating unbudgeted would turn one wrong estimate into an
        // unbounded one.
        if !self.unbudgeted_frozen.swap(true, Ordering::AcqRel) {
            self.shared
                .events
                .record(MemoryEventKind::GrowthFrozen { scope: self.id });
        }
        self.shared.events.record(MemoryEventKind::ExcessRecorded {
            scope: self.id,
            excess_bytes: amount,
        });
        if let Some(parent) = &self.parent {
            parent.absorb_from_child(amount);
        }
    }

    /// Accepts `amount` a child took without asking, taking it out of slack
    /// where there is slack and pushing the shortfall further up where there
    /// is not.
    fn absorb_from_child(&self, amount: u64) {
        self.handed_down.fetch_add(amount, Ordering::AcqRel);
        let taken = self.take_local_free_up_to(amount);
        let shortfall = amount - taken;
        if shortfall > 0 {
            self.reserved.fetch_add(shortfall, Ordering::AcqRel);
            match &self.parent {
                Some(parent) => parent.absorb_from_child(shortfall),
                None => {
                    // The root has nowhere to push: the process now holds more
                    // than its managed capacity, and says so.
                    self.recompute_excess();
                }
            }
        }
        self.record_peak_committed();
        self.recompute_excess();
    }

    /// Releases live allocation back into local slack.
    fn release_live(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        let _ = self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
        self.give_local_free(amount);
        self.recompute_excess();
    }

    /// Turns granted capacity into a bounded third-party upper bound.
    fn commit_bounded(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.bounded.fetch_add(amount, Ordering::AcqRel);
        let _ = self
            .granted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
    }

    /// Converts part of a bound into live allocation, leaving the total
    /// commitment unchanged.
    fn convert_bounded_to_live(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        let live = self.live.fetch_add(amount, Ordering::AcqRel) + amount;
        let _ = self
            .bounded
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
        self.record_peak_live(live);
    }

    /// Releases an unconverted bound remainder back into local slack.
    fn release_bounded(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        let _ = self
            .bounded
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
        self.give_local_free(amount);
        self.recompute_excess();
    }

    /// Recomputes the excess against the applicable bound and reopens growth
    /// once the account is back inside it.
    fn recompute_excess(&self) {
        let committed = self.committed_bytes();
        let over = match self.effective_bound_bytes() {
            Some(bound) => committed.saturating_sub(bound),
            None => 0,
        };
        self.excess.store(over, Ordering::Release);
        if over > 0 {
            if !self.growth_frozen.swap(true, Ordering::AcqRel) {
                self.shared
                    .events
                    .record(MemoryEventKind::GrowthFrozen { scope: self.id });
            }
        } else if self.growth_frozen.swap(false, Ordering::AcqRel) {
            self.shared
                .events
                .record(MemoryEventKind::GrowthResumed { scope: self.id });
        }
    }

    /// Visits this account's live children.
    ///
    /// The strong references are collected under the lock and the lock is
    /// released before any of them is used or dropped. Holding it across the
    /// visit would be a deadlock: the last reference to a child can fall here,
    /// and an account's own `Drop` reaches back into its parent, which would
    /// try to take the very lock this walk is holding.
    fn walk_children<F: FnMut(&Arc<Account>)>(&self, mut visit: F) {
        let live: Vec<Arc<Account>> = {
            let children = self
                .children
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            children.iter().filter_map(Weak::upgrade).collect()
        };
        for child in &live {
            visit(child);
        }
    }

    /// Registers a child link, dropping any link whose account is already
    /// gone.
    ///
    /// Pruning happens here rather than in `Drop` for the same reason the walk
    /// releases its lock first: a dropping account must never reach for its
    /// parent's child lock, because the drop can be triggered from inside a
    /// walk that already holds it.
    fn register_child(&self, child: &Arc<Account>) {
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        children.retain(|weak| weak.strong_count() > 0);
        children.push(Arc::downgrade(child));
    }

    /// Sums `L`, `F` and `O` over this account's subtree.
    ///
    /// The sums are taken with plain atomic loads and no global lock, so they
    /// describe a short period rather than one instant. That is deliberate: a
    /// consumer needing a coherent number for one account reads that account,
    /// and a consumer needing a subtree total treats this as a versioned
    /// reading rather than an atomic one.
    fn aggregate(&self) -> (u64, u64, u64) {
        let mut live = self.live.load(Ordering::Acquire);
        let mut granted = self.granted.load(Ordering::Acquire);
        let mut bounded = self.bounded.load(Ordering::Acquire);
        if !self.is_root() {
            // Slack a child holds is capacity its parent already committed,
            // so in any view at or above that child it belongs to `F`.
            granted = granted.saturating_add(self.local_free.load(Ordering::Acquire));
        }
        self.walk_children(|child| {
            let (child_live, child_granted, child_bounded) = child.aggregate();
            live = live.saturating_add(child_live);
            granted = granted.saturating_add(child_granted);
            bounded = bounded.saturating_add(child_bounded);
        });
        (live, granted, bounded)
    }

    /// Produces this account's snapshot, with `L`, `F` and `O` summed over its
    /// subtree and the peaks belonging to this account alone.
    pub fn snapshot(&self) -> AccountSnapshot {
        let (live, granted, bounded) = self.aggregate();
        AccountSnapshot {
            account: self.id,
            kind: self.kind,
            live_bytes: live,
            granted_bytes: granted,
            bounded_bytes: bounded,
            committed_bytes: self.committed_bytes(),
            floor_bytes: self.floor_bytes(),
            policy_limit_bytes: self.policy_limit().map(|limit| limit.limit_bytes()),
            excess_bytes: self.excess_bytes(),
            unbudgeted_bytes: self.unbudgeted_bytes(),
            growth_frozen: self.is_closed_to_growth(),
            frozen_by_unbudgeted: self.is_frozen_by_unbudgeted(),
            peak_committed_bytes: self.peak_committed_bytes(),
            peak_live_bytes: self.peak_live_bytes(),
            policy_version: self.policy_version(),
        }
    }
}

impl Drop for Account {
    fn drop(&mut self) {
        // An account that goes away returns everything it still holds, so a
        // dropped handle cannot strand its parent's capacity. Live charges
        // keep their sponsor alive, so reaching here means nothing is charged
        // against this account any more.
        let reserved = *self.reserved.get_mut();
        if let Some(parent) = &self.parent
            && reserved > 0
        {
            let _ =
                parent
                    .handed_down
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(reserved))
                    });
            parent.give_local_free(reserved);
            parent.recompute_excess();
        }
        self.shared.release_account_slot();
    }
}

/// A handle to one account.
///
/// The handle is the capability: holding one lets a caller derive children and
/// request capacity against that account, and nothing else. It cannot set
/// global byte counts, and it cannot reach another authority.
#[derive(Debug, Clone)]
pub struct AccountHandle {
    account: Arc<Account>,
}

impl AccountHandle {
    /// Builds the process root of one authority.
    pub(crate) fn new_root(
        external: ExternalRef,
        shared: Arc<AccountTreeShared>,
    ) -> Result<Self, CapacityError> {
        shared.claim_account_slot()?;
        let id = AccountId::new(shared.next_account_id.fetch_add(1, Ordering::Relaxed));
        let capacity = shared.capacity_bytes();
        let account = Arc::new(Account {
            id,
            kind: AccountKind::Process,
            external,
            parent: None,
            children: Mutex::new(Vec::new()),
            reserved: AtomicU64::new(capacity),
            local_free: AtomicU64::new(capacity),
            granted: AtomicU64::new(0),
            live: AtomicU64::new(0),
            bounded: AtomicU64::new(0),
            handed_down: AtomicU64::new(0),
            floor: AtomicU64::new(0),
            excess: AtomicU64::new(0),
            unbudgeted: AtomicU64::new(0),
            growth_frozen: AtomicBool::new(false),
            unbudgeted_frozen: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            peak_committed: AtomicU64::new(0),
            peak_live: AtomicU64::new(0),
            policy: Mutex::new(None),
            policy_version: AtomicU64::new(PolicyVersion::INITIAL.get()),
            shared,
        });
        Ok(Self { account })
    }

    /// Returns the shared account behind this handle.
    pub fn account(&self) -> &Arc<Account> {
        &self.account
    }

    /// Returns this account's identity.
    pub fn id(&self) -> AccountId {
        self.account.id()
    }

    /// Returns what the account stands for.
    pub fn kind(&self) -> AccountKind {
        self.account.kind()
    }

    /// Returns the external reference attached to this account.
    pub fn external_ref(&self) -> ExternalRef {
        self.account.external_ref()
    }

    /// Derives a child account.
    ///
    /// A child starts with no capacity of its own and tops up from this
    /// account when it first needs some. Creating one costs a bounded metadata
    /// slot, which is why it can be refused.
    pub fn create_child(
        &self,
        kind: AccountKind,
        external: ExternalRef,
    ) -> Result<AccountHandle, CapacityError> {
        if kind == AccountKind::Process {
            return Err(CapacityError::Unsupported {
                detail: "a process root cannot be created as a child account",
            });
        }
        if self.account.closed.load(Ordering::Acquire) {
            return Err(CapacityError::Cancelled {
                scope: self.account.id,
            });
        }
        let shared = Arc::clone(&self.account.shared);
        shared.claim_account_slot()?;
        let id = AccountId::new(shared.next_account_id.fetch_add(1, Ordering::Relaxed));
        let child = Arc::new(Account {
            id,
            kind,
            external,
            parent: Some(Arc::clone(&self.account)),
            children: Mutex::new(Vec::new()),
            reserved: AtomicU64::new(0),
            local_free: AtomicU64::new(0),
            granted: AtomicU64::new(0),
            live: AtomicU64::new(0),
            bounded: AtomicU64::new(0),
            handed_down: AtomicU64::new(0),
            floor: AtomicU64::new(0),
            excess: AtomicU64::new(0),
            unbudgeted: AtomicU64::new(0),
            growth_frozen: AtomicBool::new(false),
            unbudgeted_frozen: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            peak_committed: AtomicU64::new(0),
            peak_live: AtomicU64::new(0),
            policy: Mutex::new(None),
            policy_version: AtomicU64::new(PolicyVersion::INITIAL.get()),
            shared,
        });
        self.account.register_child(&child);
        Ok(AccountHandle { account: child })
    }

    /// Installs or replaces this account's policy limit.
    ///
    /// Lowering a limit below the current commitment neither fails nor erases
    /// anything: the excess is reported, growth stops, and the commitments
    /// already issued under the previous version keep their settlement
    /// guarantee.
    pub fn install_policy(
        &self,
        limit_bytes: u64,
        dimension: LimitDimension,
    ) -> PolicyInstallOutcome {
        let version = PolicyVersion::new(
            self.account
                .policy_version
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1),
        );
        let limit = PolicyLimit::bytes(limit_bytes, dimension, version);
        {
            let mut installed = self
                .account
                .policy
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *installed = Some(limit);
        }
        self.account
            .shared
            .events
            .record(MemoryEventKind::PolicyInstalled {
                scope: self.account.id,
                limit_bytes,
                version,
            });
        self.account.recompute_excess();
        let committed = self.account.committed_bytes();
        PolicyInstallOutcome {
            version,
            committed_bytes: committed,
            excess_bytes: self.account.excess_bytes(),
            growth_frozen: self.account.is_closed_to_growth(),
        }
    }

    /// Sets the floor this account keeps against ordinary competition.
    ///
    /// The floor is a retention bound, not a fourth quantity: the bytes it
    /// protects are the same bytes counted in `L`, `F` and `O`. It lasts for
    /// the lifetime of the progressable unit rather than expiring on a clock,
    /// so a bounded step cannot have its working set taken away mid-flight.
    pub fn set_floor(&self, floor_bytes: u64) {
        self.account.floor.store(floor_bytes, Ordering::Release);
    }

    /// Lets the account grow again after unbudgeted allocation was resolved.
    ///
    /// This is the arbitrator's acknowledgement, and it is deliberately
    /// explicit: an over-bound freeze lifts by itself when the commitment
    /// comes back inside the bound, but unbudgeted allocation means a caller
    /// sized something wrongly, and releasing the bytes does not fix that.
    /// The cumulative record of what was absorbed is kept either way.
    pub fn resume_growth_after_arbitration(&self) -> u64 {
        let absorbed = self.account.unbudgeted_bytes();
        if self.account.unbudgeted_frozen.swap(false, Ordering::AcqRel)
            && !self.account.growth_frozen.load(Ordering::Acquire)
        {
            self.account
                .shared
                .events
                .record(MemoryEventKind::GrowthResumed {
                    scope: self.account.id,
                });
        }
        absorbed
    }

    /// Closes the account to further growth while keeping every existing
    /// obligation.
    ///
    /// Cancellation revokes only what is still revocable. Fulfilled charges,
    /// live allocations, bounded third-party steps and background work keep
    /// their accounting until their real owners release them.
    pub fn close_to_growth(&self) {
        self.account.closed.store(true, Ordering::Release);
    }

    /// Reports whether the account refuses further growth.
    pub fn is_closed_to_growth(&self) -> bool {
        self.account.is_closed_to_growth()
    }

    /// Returns this account's total commitment `C`.
    pub fn committed_bytes(&self) -> u64 {
        self.account.committed_bytes()
    }

    /// Returns slack held but not issued.
    pub fn local_free_bytes(&self) -> u64 {
        self.account.local_free_bytes()
    }

    /// Returns this account's snapshot.
    pub fn snapshot(&self) -> AccountSnapshot {
        self.account.snapshot()
    }

    /// Takes back capacity the account holds but has not issued.
    ///
    /// This is the arbitrator's free path: idle slack can be reclaimed without
    /// the account's cooperation. Capacity a grant already reserved, capacity
    /// already fulfilled, and capacity the floor protects are all left alone,
    /// and the outcome says which is which. A concurrent request competing for
    /// the same idle bytes has exactly one winner, because both go through the
    /// same compare-and-swap on `local_free`.
    pub fn shrink_idle(&self, target_bytes: u64) -> ShrinkOutcome {
        let account = &self.account;
        if account.is_root() {
            // Reclaiming the root's slack would move capacity out of the
            // authority, which is a configuration change rather than
            // arbitration.
            return ShrinkOutcome {
                reclaimed_bytes: 0,
                kept_for_floor_bytes: account.floor_bytes(),
                kept_for_grants_bytes: account.committed_bytes(),
            };
        }
        let floor = account.floor_bytes();
        let mut reclaimed = 0u64;
        while reclaimed < target_bytes {
            let idle = account.local_free.load(Ordering::Acquire);
            let reserved = account.reserved.load(Ordering::Acquire);
            let above_floor = reserved.saturating_sub(floor);
            let eligible = idle.min(above_floor).min(target_bytes - reclaimed);
            if eligible == 0 {
                break;
            }
            // A lost race just re-reads: `take_local_free` only reports false
            // when the slack is genuinely short at that moment.
            if account.take_local_free(eligible) {
                account.reserved.fetch_sub(eligible, Ordering::AcqRel);
                if let Some(parent) = &account.parent {
                    let _ = parent.handed_down.fetch_update(
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        |current| Some(current.saturating_sub(eligible)),
                    );
                    parent.give_local_free(eligible);
                }
                reclaimed += eligible;
            }
        }
        if reclaimed > 0 {
            account
                .shared
                .events
                .record(MemoryEventKind::IdleCapacityReclaimed {
                    scope: account.id,
                    reclaimed_bytes: reclaimed,
                });
            account.recompute_excess();
        }
        let idle_left = account.local_free.load(Ordering::Acquire);
        let reserved_left = account.reserved.load(Ordering::Acquire);
        let above_floor_left = reserved_left.saturating_sub(floor);
        ShrinkOutcome {
            reclaimed_bytes: reclaimed,
            kept_for_floor_bytes: idle_left.saturating_sub(above_floor_left.min(idle_left)),
            kept_for_grants_bytes: account
                .granted
                .load(Ordering::Acquire)
                .saturating_add(account.live.load(Ordering::Acquire))
                .saturating_add(account.bounded.load(Ordering::Acquire))
                .saturating_add(account.handed_down.load(Ordering::Acquire)),
        }
    }

    // -- internal capacity plumbing used by grant, charge and bound ---------

    pub(crate) fn acquire_for_grant(&self, amount: u64) -> Result<(), CapacityError> {
        self.account.acquire_for_grant(amount)
    }

    pub(crate) fn return_granted(&self, amount: u64) {
        self.account.return_granted(amount);
    }

    pub(crate) fn commit_live(&self, amount: u64) {
        self.account.commit_live(amount);
    }

    pub(crate) fn absorb_excess_live(&self, amount: u64) {
        self.account.absorb_excess_live(amount);
    }

    pub(crate) fn release_live(&self, amount: u64) {
        self.account.release_live(amount);
    }

    pub(crate) fn commit_bounded(&self, amount: u64) {
        self.account.commit_bounded(amount);
    }

    pub(crate) fn convert_bounded_to_live(&self, amount: u64) {
        self.account.convert_bounded_to_live(amount);
    }

    pub(crate) fn release_bounded(&self, amount: u64) {
        self.account.release_bounded(amount);
    }
}

/// Returns an account's ancestry, self first and root last.
pub fn ancestry(account: &Arc<Account>) -> Vec<Arc<Account>> {
    let mut path = Vec::new();
    let mut current = Some(Arc::clone(account));
    while let Some(node) = current {
        current = node.parent.as_ref().map(Arc::clone);
        path.push(node);
    }
    path
}

/// Returns the lowest common ancestor of two accounts, if they share one.
///
/// A sponsor transfer moves the same debt under one authority. Finding the
/// common ancestor first is what makes the move a move: only the branches
/// below it change, so no ancestor observes the debt leaving one side before
/// it arrives on the other.
pub fn lowest_common_ancestor(left: &Arc<Account>, right: &Arc<Account>) -> Option<Arc<Account>> {
    let left_path = ancestry(left);
    let right_path = ancestry(right);
    let mut common = None;
    let mut depth = 0;
    while depth < left_path.len() && depth < right_path.len() {
        let left_node = &left_path[left_path.len() - 1 - depth];
        let right_node = &right_path[right_path.len() - 1 - depth];
        if Arc::ptr_eq(left_node, right_node) {
            common = Some(Arc::clone(left_node));
            depth += 1;
        } else {
            break;
        }
    }
    common
}

/// Moves `amount` of already-live allocation from one account to another.
///
/// This is a move of one debt, not a release and a re-acquire. Only the
/// branches strictly below the common ancestor change: the destination branch
/// is validated and charged first, and the source branch is released only
/// after that succeeds. The common ancestor's own commitment never moves at
/// all, because the capacity never leaves it — which is exactly why a sampler
/// watching the ancestor sees no jitter, and why a failure leaves the source
/// exactly as it was.
pub(crate) fn transfer_live(
    source: &Arc<Account>,
    destination: &Arc<Account>,
    amount: u64,
) -> Result<(), CapacityError> {
    if amount == 0 || Arc::ptr_eq(source, destination) {
        return Ok(());
    }
    let source_path = ancestry(source);
    let destination_path = ancestry(destination);
    let mut shared_depth = 0;
    while shared_depth < source_path.len()
        && shared_depth < destination_path.len()
        && Arc::ptr_eq(
            &source_path[source_path.len() - 1 - shared_depth],
            &destination_path[destination_path.len() - 1 - shared_depth],
        )
    {
        shared_depth += 1;
    }
    if shared_depth == 0 {
        return Err(CapacityError::Unsupported {
            detail: "the accounts belong to different authorities",
        });
    }

    let destination_only = destination_path.len() - shared_depth;
    let source_only = source_path.len() - shared_depth;

    // Charge the destination-only branch from the common ancestor downwards.
    // Each node raises its own commitment under its own bound; the bytes come
    // from the source branch, which still holds them, so no ancestor is asked
    // for anything.
    let mut charged: Vec<&Arc<Account>> = Vec::with_capacity(destination_only);
    for index in (0..destination_only).rev() {
        let node = &destination_path[index];
        if let Err(error) = node
            .refuse_if_closed()
            .and_then(|()| node.claim_own_bound(amount, amount).map(|_| ()))
        {
            for reverted in charged.iter().rev() {
                reverted.reserved.fetch_sub(amount, Ordering::AcqRel);
                if let Some(parent) = &reverted.parent {
                    let _ = parent.handed_down.fetch_update(
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        |current| Some(current.saturating_sub(amount)),
                    );
                }
            }
            return Err(error);
        }
        if let Some(parent) = &node.parent {
            parent.handed_down.fetch_add(amount, Ordering::AcqRel);
        }
        charged.push(node);
    }
    // The destination leaf now owns the live bytes.
    destination.live.fetch_add(amount, Ordering::AcqRel);
    destination.record_peak_live(destination.own_live_bytes());
    for node in &charged {
        node.record_peak_committed();
    }

    // Release the source-only branch. The leaf gives up the live bytes and
    // every node on that branch gives up the commitment.
    let _ = source
        .live
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(amount))
        });
    for node in source_path.iter().take(source_only) {
        node.reserved.fetch_sub(amount, Ordering::AcqRel);
        if let Some(parent) = &node.parent {
            let _ =
                parent
                    .handed_down
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(amount))
                    });
        }
        node.recompute_excess();
    }
    Ok(())
}
