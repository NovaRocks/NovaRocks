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

//! Live allocation (MEM-1 wave-1 T02).
//!
//! A charge is trusted backing: an identity, a capacity, exactly one sponsor
//! and a real obligation to release. It ends when the last necessary resource
//! owner goes away, never when a short-lived wrapper does. Transfer moves the
//! same debt under a common authority: the destination branch is validated
//! first, and a failure leaves the source untouched, so no common ancestor
//! ever sees a gap or a double count.
//!
//! # Release happens once, through the owner
//!
//! There is deliberately no public "subtract these bytes" call. A charge is
//! released by dropping it, by shrinking it to a smaller proven capacity, or
//! by moving it to another sponsor. Every one of those paths funnels through
//! the same guard, so a repeated request, a `Drop` after an explicit release,
//! and two threads racing on the same charge all settle exactly once.
//!
//! # Why the state is shared
//!
//! An Arrow buffer's accounting has to outlive the wrapper that first
//! measured it: slices, clones and exported arrays keep the same backing
//! alive. [`ChargeState`] is therefore reference-counted and is what the Arrow
//! adapter hands to a buffer's reservation, so the charge ends with the last
//! alias rather than with the first wrapper to be dropped.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::account::{Account, AccountHandle, transfer_live};
use crate::error::{FulfilError, TransferError};
use crate::grant::CapacityGrant;
use crate::ids::AccountId;

/// The shared, reference-counted state behind one live charge.
///
/// The Arrow adapter holds this directly: a buffer's reservation is one of
/// these, so the accounting follows the backing rather than the wrapper.
#[derive(Debug)]
pub struct ChargeState {
    /// The single account currently sponsoring these bytes. The mutex also
    /// serialises transfer against release, so a charge cannot be moved and
    /// settled at the same time.
    sponsor: Mutex<AccountHandle>,
    bytes: AtomicU64,
    released: AtomicBool,
}

impl ChargeState {
    fn new(sponsor: AccountHandle, bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            sponsor: Mutex::new(sponsor),
            bytes: AtomicU64::new(bytes),
            released: AtomicBool::new(false),
        })
    }

    /// Returns the account currently sponsoring these bytes.
    ///
    /// An adapter that moves several charges together needs the origin handle
    /// so a refusal partway through can put the moved ones back.
    pub fn sponsor(&self) -> AccountHandle {
        self.sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Returns the capacity currently charged.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }

    /// Returns the sponsoring account's identity.
    pub fn sponsor_id(&self) -> AccountId {
        self.sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .id()
    }

    /// Reports whether the charge has already settled.
    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    /// Grows the charge inside a grant's remainder.
    ///
    /// A grow that may relocate must cover the full replacement first: the
    /// caller obtains capacity for the new block while the old one is still
    /// charged, and only shrinks after the old block is actually gone. That
    /// is the caller's sequencing; this call is the accounting half of it.
    pub fn grow_within(&self, grant: &CapacityGrant, additional: u64) -> Result<(), FulfilError> {
        if additional == 0 {
            return Ok(());
        }
        let guard = self
            .sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.released.load(Ordering::Acquire) {
            return Err(FulfilError::Cancelled { grant: grant.id() });
        }
        if grant.account_id() != guard.id() {
            // Growing a charge with another account's capacity would move the
            // debt without moving the sponsor, so the two would disagree
            // about who owes the bytes.
            return Err(FulfilError::ExceedsRemainder {
                grant: grant.id(),
                requested: additional,
                remainder: 0,
            });
        }
        let charge = grant.fulfil(additional)?;
        // The grant produced its own charge object; fold it into this one
        // instead of tracking two charges for one backing.
        let folded = charge.into_bytes_without_release();
        self.bytes.fetch_add(folded, Ordering::AcqRel);
        drop(guard);
        Ok(())
    }

    /// Grows the charge with capacity nobody granted.
    ///
    /// Used where the underlying library has already allocated and cannot be
    /// asked to stop. The bytes exist, so they are charged and the account's
    /// growth freezes; hiding them would be worse.
    pub fn absorb_growth(&self, additional: u64) {
        if additional == 0 {
            return;
        }
        let guard = self
            .sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.released.load(Ordering::Acquire) {
            return;
        }
        guard.absorb_excess_live(additional);
        self.bytes.fetch_add(additional, Ordering::AcqRel);
        drop(guard);
    }

    /// Shrinks the charge to a smaller proven capacity, releasing the
    /// difference.
    ///
    /// Only a real reduction in backing capacity belongs here. A shorter
    /// logical length, a truncated container that kept its allocation, or a
    /// dropped view are not releases and must not reach this call.
    pub fn shrink_to(&self, proven_bytes: u64) {
        let guard = self
            .sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let mut current = self.bytes.load(Ordering::Acquire);
        loop {
            if proven_bytes >= current {
                return;
            }
            match self.bytes.compare_exchange_weak(
                current,
                proven_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    guard.release_live_bytes(current - proven_bytes);
                    drop(guard);
                    return;
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Settles the charge, exactly once.
    ///
    /// Returns the bytes released, or zero if some other path already settled
    /// it. `Drop`, an explicit release and a concurrent release all end up
    /// here, which is why the "release exactly once" rule holds without the
    /// caller having to pair anything by hand.
    pub fn release(&self) -> u64 {
        let guard = self
            .sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.released.swap(true, Ordering::AcqRel) {
            return 0;
        }
        let bytes = self.bytes.swap(0, Ordering::AcqRel);
        guard.release_live_bytes(bytes);
        drop(guard);
        bytes
    }

    /// Moves the debt to another account under the same authority.
    ///
    /// The destination branch is charged before the source branch is
    /// released, and only the branches below the common ancestor move. A
    /// refusal leaves the source exactly as it was.
    pub fn transfer_to(&self, destination: &AccountHandle) -> Result<(), TransferError> {
        let mut guard = self
            .sponsor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.released.load(Ordering::Acquire) {
            return Err(TransferError::AlreadyReleased);
        }
        if guard.id() == destination.id() {
            return Err(TransferError::SameAccount { scope: guard.id() });
        }
        let bytes = self.bytes.load(Ordering::Acquire);
        let source: Arc<Account> = Arc::clone(guard.account());
        match transfer_live(&source, destination.account(), bytes) {
            Ok(()) => {
                *guard = destination.clone();
                drop(guard);
                Ok(())
            }
            Err(cause) => Err(match cause {
                crate::error::CapacityError::Unsupported { .. } => TransferError::ForeignAuthority,
                other => TransferError::Denied { cause: other },
            }),
        }
    }
}

/// One live charge, owned by whoever is responsible for releasing it.
///
/// Dropping the charge settles it. Cloning is deliberately not available:
/// several aliases of the same backing share a [`ChargeState`], and the
/// charge object itself is the single release obligation.
#[derive(Debug)]
pub struct Charge {
    state: Arc<ChargeState>,
}

impl Charge {
    pub(crate) fn new(sponsor: AccountHandle, bytes: u64) -> Self {
        Self {
            state: ChargeState::new(sponsor, bytes),
        }
    }

    /// Returns the shared state, for an adapter that must bind this charge to
    /// a buffer's own lifetime.
    pub fn state(&self) -> &Arc<ChargeState> {
        &self.state
    }

    /// Returns the shared state and gives up the release obligation with it.
    ///
    /// The state keeps the charge alive; the caller becomes responsible for
    /// releasing it through that state. This is how an Arrow buffer takes
    /// over: the reservation it carries becomes the owner, and the charge
    /// object stops being one.
    pub fn into_state(self) -> Arc<ChargeState> {
        let state = Arc::clone(&self.state);
        std::mem::forget(self);
        state
    }

    /// Returns the capacity charged.
    pub fn bytes(&self) -> u64 {
        self.state.bytes()
    }

    /// Returns the sponsoring account's identity.
    pub fn sponsor_id(&self) -> AccountId {
        self.state.sponsor_id()
    }

    /// Returns the sponsoring account.
    pub fn sponsor(&self) -> AccountHandle {
        self.state.sponsor()
    }

    /// Grows this charge inside a grant's remainder.
    pub fn grow_within(&self, grant: &CapacityGrant, additional: u64) -> Result<(), FulfilError> {
        self.state.grow_within(grant, additional)
    }

    /// Shrinks this charge to a smaller proven capacity.
    pub fn shrink_to(&self, proven_bytes: u64) {
        self.state.shrink_to(proven_bytes);
    }

    /// Moves this charge to another sponsor.
    pub fn transfer_to(&self, destination: &AccountHandle) -> Result<(), TransferError> {
        self.state.transfer_to(destination)
    }

    /// Settles this charge now, returning the bytes released.
    pub fn release(self) -> u64 {
        self.state.release()
    }

    /// Consumes the charge, keeping its bytes charged.
    ///
    /// Used when one charge is folded into another for the same backing, so
    /// the accounting stays a single obligation rather than two.
    fn into_bytes_without_release(self) -> u64 {
        let bytes = self.state.bytes();
        // The bytes stay charged to the account; the enclosing state now owns
        // the obligation, so this object must not settle on drop.
        self.state.bytes.store(0, Ordering::Release);
        self.state.released.store(true, Ordering::Release);
        std::mem::forget(self);
        bytes
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.state.release();
    }
}

impl AccountHandle {
    /// Releases live bytes back into this account's slack.
    ///
    /// This is crate-internal on purpose: consumers release through a charge,
    /// never by adjusting an account's counters directly.
    pub(crate) fn release_live_bytes(&self, bytes: u64) {
        self.release_live(bytes);
    }
}
