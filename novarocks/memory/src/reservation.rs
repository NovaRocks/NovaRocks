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

//! A dedicated leaf for already allocated, governed retention.
//!
//! The leaf's counters are the account tree's own counters. F is the only
//! arbiter for fast growth and idle return. C changes under `slow`; the
//! version brackets those changes for leaf snapshots. A successful slow
//! growth commits its own delta directly to L before surplus F is published.
// Design: ADR-0160 (docs/adr/ADR-0160-governed-retention-accounting.md)

#[cfg(all(test, loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(all(test, loom))]
use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(all(test, loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(all(test, loom)))]
use std::sync::{Arc, Mutex, MutexGuard};
// Diagnostic counters do not participate in the leaf protocol or loom model.
use std::sync::atomic::{AtomicU64 as MetricAtomicU64, Ordering as MetricOrdering};

use crate::account::{AccountHandle, DeferredEvents, ShrinkOutcome};
use crate::error::CapacityError;
use crate::ids::{AccountId, AccountKind, ExternalRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReservationSnapshot {
    pub account: AccountId,
    pub live_bytes: u64,
    pub free_bytes: u64,
    pub committed_bytes: u64,
    pub closed: bool,
}

/// Cumulative leaf protocol observations. Parent calls count the immediate
/// leaf-to-sponsor edge, including refused top-up attempts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReservationMetrics {
    pub free_cas_retries: u64,
    pub parent_top_up_calls: u64,
    pub parent_return_calls: u64,
}

#[derive(Debug)]
struct ReservationState {
    account: AccountHandle,
    slow: Mutex<()>,
    version: AtomicU64,
    closed: AtomicBool,
    target: AtomicU64,
    free_cas_retries: MetricAtomicU64,
    parent_top_up_calls: MetricAtomicU64,
    parent_return_calls: MetricAtomicU64,
}

/// One private accounting leaf under a stable query or service sponsor.
/// Clones share the exact same leaf and can outlive the domain that created it.
#[derive(Debug, Clone)]
pub struct Reservation {
    state: Arc<ReservationState>,
}

/// Owns one live debit against a reservation until released or dropped.
/// Splitting changes ownership of the debit without touching the account tree.
#[derive(Debug)]
#[must_use = "dropping the lease releases its retained bytes"]
pub struct ReservationLease {
    reservation: Reservation,
    bytes: u64,
}

impl ReservationLease {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn split_off(&mut self, bytes: u64) -> Self {
        assert!(
            bytes <= self.bytes,
            "reservation lease split exceeds live bytes"
        );
        self.bytes -= bytes;
        Self {
            reservation: self.reservation.clone(),
            bytes,
        }
    }

    /// Identifies this exact in-process leaf for grouping its leases. The key
    /// is valid only while a lease keeps the leaf alive.
    pub fn leaf_key(&self) -> usize {
        Arc::as_ptr(&self.reservation.state) as usize
    }

    /// Combines two debits against the same leaf without moving capacity.
    /// On a mismatch or overflow, `other` remains live and is returned.
    pub fn merge(&mut self, mut other: Self) -> Result<(), Self> {
        if !Arc::ptr_eq(&self.reservation.state, &other.reservation.state) {
            return Err(other);
        }
        let Some(combined) = self.bytes.checked_add(other.bytes) else {
            return Err(other);
        };
        self.bytes = combined;
        other.bytes = 0;
        Ok(())
    }

    pub fn release(mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        self.reservation.release_live(bytes);
    }
}

impl Drop for ReservationLease {
    fn drop(&mut self) {
        self.reservation.release_live(self.bytes);
    }
}

struct VersionWrite<'a>(&'a AtomicU64);

impl VersionWrite<'_> {
    fn begin(version: &AtomicU64) -> VersionWrite<'_> {
        let previous = version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(previous & 1, 0, "reservation writer must hold slow lock");
        VersionWrite(version)
    }
}

impl Drop for VersionWrite<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

impl Reservation {
    pub fn new(sponsor: &AccountHandle, external: ExternalRef) -> Result<Self, CapacityError> {
        let account = sponsor.create_child(AccountKind::Owner, external)?;
        let target = account.account().reservation_target();
        Ok(Self {
            state: Arc::new(ReservationState {
                account,
                slow: Mutex::new(()),
                version: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                target: AtomicU64::new(target),
                free_cas_retries: MetricAtomicU64::new(0),
                parent_top_up_calls: MetricAtomicU64::new(0),
                parent_return_calls: MetricAtomicU64::new(0),
            }),
        })
    }

    pub fn account_id(&self) -> AccountId {
        self.state.account.id()
    }

    pub fn metrics(&self) -> ReservationMetrics {
        ReservationMetrics {
            free_cas_retries: self.state.free_cas_retries.load(MetricOrdering::Relaxed),
            parent_top_up_calls: self.state.parent_top_up_calls.load(MetricOrdering::Relaxed),
            parent_return_calls: self.state.parent_return_calls.load(MetricOrdering::Relaxed),
        }
    }

    pub fn try_grow(&self, bytes: u64) -> Result<ReservationLease, CapacityError> {
        if bytes == 0 {
            return Ok(ReservationLease {
                reservation: self.clone(),
                bytes: 0,
            });
        }
        if self.state.closed.load(Ordering::Acquire) {
            return Err(self.cancelled());
        }
        let account = self.state.account.account();
        if account.reservation_take_free(bytes, &self.state.free_cas_retries) {
            if self.state.closed.load(Ordering::Acquire) {
                account.reservation_restore_free(bytes);
                self.trim();
                return Err(self.cancelled());
            }
            account.reservation_commit_live(bytes);
            return Ok(ReservationLease {
                reservation: self.clone(),
                bytes,
            });
        }
        let mut deferred = DeferredEvents::new();
        let result = {
            let _slow = self.lock_slow();
            if self.state.closed.load(Ordering::Acquire) {
                Err(self.cancelled())
            } else if account.reservation_take_free(bytes, &self.state.free_cas_retries) {
                account.reservation_commit_live(bytes);
                Ok(())
            } else {
                let result = {
                    let _version = VersionWrite::begin(&self.state.version);
                    account.reservation_grow_slow(
                        bytes,
                        &self.state.free_cas_retries,
                        &self.state.parent_top_up_calls,
                        &mut deferred,
                    )
                };
                self.state
                    .target
                    .store(account.reservation_target(), Ordering::Release);
                result
            }
        };
        account.reservation_flush_events(deferred);
        result.map(|()| ReservationLease {
            reservation: self.clone(),
            bytes,
        })
    }

    /// Retires live retention without waiting for capacity or fast-path users.
    /// An excess return joins the serialized slow path in this call.
    fn release_live(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let account = self.state.account.account();
        account.reservation_shrink_live(bytes);
        let target = self.state.target.load(Ordering::Acquire);
        let idle = account.local_free_bytes();
        // Close's first F sweep is an RMW. If this release publishes F first,
        // close includes it; otherwise this Acquire read observes CLOSED and
        // returns it after joining the slow path.
        let closed = self.state.closed.load(Ordering::Acquire);
        if closed || idle > target.saturating_mul(2) {
            let mut deferred = DeferredEvents::new();
            {
                let _slow = self.lock_slow();
                self.return_idle_locked(&mut deferred);
            }
            account.reservation_flush_events(deferred);
        }
    }

    /// Returns all non-floor idle commitment, regardless of the adaptive
    /// target. This does not retire a live holder or revoke issued capacity.
    pub fn trim(&self) -> ShrinkOutcome {
        let account = self.state.account.account();
        let mut deferred = DeferredEvents::new();
        let outcome = {
            let _slow = self.lock_slow();
            let _version = VersionWrite::begin(&self.state.version);
            account.reservation_trim_deferred(
                u64::MAX,
                &self.state.free_cas_retries,
                &self.state.parent_return_calls,
                &mut deferred,
            )
        };
        account.reservation_flush_events(deferred);
        outcome
    }

    /// Closes new retention while existing holders remain billable.
    pub fn close(&self) -> ShrinkOutcome {
        self.state.closed.swap(true, Ordering::AcqRel);
        let account = self.state.account.account();
        let mut deferred = DeferredEvents::new();
        let outcome = {
            let _slow = self.lock_slow();
            account.reservation_close();
            let _version = VersionWrite::begin(&self.state.version);
            account.reservation_close_trim(
                &self.state.free_cas_retries,
                &self.state.parent_return_calls,
                &mut deferred,
            )
        };
        account.reservation_flush_events(deferred);
        outcome
    }

    pub fn revoke(&self) -> ShrinkOutcome {
        self.close()
    }

    pub fn snapshot(&self) -> ReservationSnapshot {
        let account = self.state.account.account();
        loop {
            let before = self.state.version.load(Ordering::Acquire);
            if before & 1 != 0 {
                #[cfg(all(test, loom))]
                loom::thread::yield_now();
                #[cfg(not(all(test, loom)))]
                std::hint::spin_loop();
                continue;
            }
            let committed = account.committed_bytes();
            let live = account.own_live_bytes();
            let after = self.state.version.load(Ordering::Acquire);
            if before == after && committed >= live {
                return ReservationSnapshot {
                    account: account.id(),
                    live_bytes: live,
                    free_bytes: committed - live,
                    committed_bytes: committed,
                    closed: self.state.closed.load(Ordering::Acquire),
                };
            }
        }
    }

    fn return_idle_locked(&self, deferred: &mut DeferredEvents) {
        let account = self.state.account.account();
        let closed = self.state.closed.load(Ordering::Acquire);
        let idle = account.local_free_bytes();
        let target = if closed {
            0
        } else {
            account.reservation_target()
        };
        if !closed && idle <= target.saturating_mul(2) {
            return;
        }
        let _version = VersionWrite::begin(&self.state.version);
        let outcome = account.reservation_trim_deferred(
            idle.saturating_sub(target),
            &self.state.free_cas_retries,
            &self.state.parent_return_calls,
            deferred,
        );
        if outcome.reclaimed_bytes > 0 {
            account.reservation_reset_demand();
            self.state
                .target
                .store(account.reservation_target(), Ordering::Release);
        }
    }

    fn lock_slow(&self) -> MutexGuard<'_, ()> {
        self.state
            .slow
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(all(test, loom))]
    pub(crate) fn lock_slow_for_test(&self) -> MutexGuard<'_, ()> {
        self.lock_slow()
    }

    fn cancelled(&self) -> CapacityError {
        CapacityError::Cancelled {
            scope: self.state.account.id(),
        }
    }
}

impl Drop for ReservationState {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.account.account().reservation_close();
        let mut deferred = DeferredEvents::new();
        self.account.account().reservation_trim_deferred(
            u64::MAX,
            &self.free_cas_retries,
            &self.parent_return_calls,
            &mut deferred,
        );
        self.account.account().reservation_flush_events(deferred);
    }
}
