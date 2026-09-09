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

//! Issued capacity (MEM-1 wave-1 T02).
//!
//! A grant is one already-issued, non-reassignable amount plus the right to
//! fulfil or return it. It is an RAII value: dropping it returns the
//! unfulfilled remainder to its account. Requests that are not yet granted,
//! and the tickets that wait for them, belong to the arbitrator, not here — a
//! waiting request holds no capacity at all.
//!
//! # The settlement guarantee
//!
//! Fulfilling within a grant's remainder cannot fail on capacity. That is the
//! whole point of holding a grant: a caller reserves first, performs the
//! physical allocation, and then settles. If settlement could refuse after a
//! successful allocation, the caller would be left holding memory with no
//! accounting, which is precisely the state MEM-1 forbids.
//!
//! Cancelling a scope therefore does not break settlement. Cancellation stops
//! *new* capacity being issued; capacity already granted stays settleable
//! until its holder returns it. The one way a holder loses the right is an
//! explicit revocation, which takes only the remainder that no fulfilment has
//! claimed, and which the arbitrator may treat as confirmed the moment it
//! sees how much it actually took.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::account::AccountHandle;
use crate::bound::ExternalBound;
use crate::charge::Charge;
use crate::error::{CapacityError, FulfilError};
use crate::ids::{AccountId, GrantId};

/// One issued, non-reassignable amount of capacity.
///
/// The grant is the capability: holding it is what makes a later fulfilment
/// infallible. It is not transferable to another account, and it cannot be
/// used to change any global byte count.
#[derive(Debug)]
pub struct CapacityGrant {
    id: GrantId,
    account: AccountHandle,
    remainder: AtomicU64,
    revoked: AtomicBool,
}

impl CapacityGrant {
    fn new(id: GrantId, account: AccountHandle, bytes: u64) -> Self {
        Self {
            id,
            account,
            remainder: AtomicU64::new(bytes),
            revoked: AtomicBool::new(false),
        }
    }

    /// Returns this grant's identity.
    pub const fn id(&self) -> GrantId {
        self.id
    }

    /// Returns the account this capacity was issued from.
    pub fn account(&self) -> &AccountHandle {
        &self.account
    }

    /// Returns the account's identity.
    pub fn account_id(&self) -> AccountId {
        self.account.id()
    }

    /// Returns the capacity still unfulfilled on this grant.
    pub fn remaining_bytes(&self) -> u64 {
        self.remainder.load(Ordering::Acquire)
    }

    /// Reports whether the remainder was revoked.
    pub fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }

    /// Claims `bytes` out of the remainder, or reports why it cannot.
    ///
    /// This single compare-and-swap is where a fulfilment and a concurrent
    /// revocation are resolved: exactly one of them takes the bytes.
    fn claim(&self, bytes: u64) -> Result<(), FulfilError> {
        if bytes == 0 {
            return Ok(());
        }
        let mut current = self.remainder.load(Ordering::Acquire);
        loop {
            if current < bytes {
                if self.is_revoked() {
                    return Err(FulfilError::Cancelled { grant: self.id });
                }
                return Err(FulfilError::ExceedsRemainder {
                    grant: self.id,
                    requested: bytes,
                    remainder: current,
                });
            }
            match self.remainder.compare_exchange_weak(
                current,
                current - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Turns part of this grant into known live allocation.
    ///
    /// Call this after the physical allocation has succeeded. Within the
    /// remainder it cannot fail on capacity, so it never leaves a caller
    /// holding memory it cannot account for.
    pub fn fulfil(&self, bytes: u64) -> Result<Charge, FulfilError> {
        self.claim(bytes)?;
        self.account.commit_live(bytes);
        Ok(Charge::new(self.account.clone(), bytes))
    }

    /// Splits capacity out of this grant into a separate grant.
    ///
    /// Sub-grants are how one owner hands a bounded share to a helper without
    /// widening anyone's rights: the total issued capacity does not change,
    /// and the account still sees exactly one commitment.
    pub fn split(&self, bytes: u64) -> Result<CapacityGrant, FulfilError> {
        self.claim(bytes)?;
        let id = GrantId::new(next_grant_id());
        Ok(CapacityGrant::new(id, self.account.clone(), bytes))
    }

    /// Turns part of this grant into an upper bound covering one short
    /// third-party step.
    pub fn begin_external(&self, bytes: u64) -> Result<ExternalBound, FulfilError> {
        self.claim(bytes)?;
        self.account.commit_bounded(bytes);
        Ok(ExternalBound::new(self.account.clone(), bytes))
    }

    /// Takes back the capacity no fulfilment has claimed.
    ///
    /// Returns how much was actually taken. That number is the confirmation an
    /// arbitrator needs: only capacity it can see it took may be re-issued to
    /// someone else. A notification, a timeout, or the holder finishing its
    /// own logic are all insufficient.
    pub fn revoke_remainder(&self) -> u64 {
        self.revoked.store(true, Ordering::Release);
        let taken = self.remainder.swap(0, Ordering::AcqRel);
        if taken > 0 {
            self.account.return_granted(taken);
        }
        taken
    }
}

impl Drop for CapacityGrant {
    fn drop(&mut self) {
        let remainder = *self.remainder.get_mut();
        if remainder > 0 {
            self.account.return_granted(remainder);
        }
    }
}

/// Issues grant identities for this process.
///
/// Grant identities are diagnostics, not capabilities, so one process-wide
/// counter is enough and it does not have to be per authority.
fn next_grant_id() -> u64 {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl AccountHandle {
    /// Requests capacity from this account.
    ///
    /// The request is served from the account's own slack, and only walks up
    /// the tree when that slack is short. It never waits: either the capacity
    /// is issued now, or the refusal says which constraint refused and what it
    /// could still offer. Waiting, queueing and priority belong to the
    /// arbitrator.
    pub fn request_grant(&self, bytes: u64) -> Result<CapacityGrant, CapacityError> {
        self.acquire_for_grant(bytes)?;
        Ok(CapacityGrant::new(
            GrantId::new(next_grant_id()),
            self.clone(),
            bytes,
        ))
    }

    /// Records live allocation this account has no granted capacity for.
    ///
    /// This exists for adapters whose underlying library cannot be asked to
    /// stop: the memory is already there, so the account absorbs it, reports
    /// the excess, and freezes growth until an arbitrator resolves it.
    /// Refusing would only hide the bytes.
    pub fn absorb_unbudgeted_live(&self, bytes: u64) -> Charge {
        self.absorb_excess_live(bytes);
        Charge::new(self.clone(), bytes)
    }
}

/// Convenience for the common "grant exactly this much, then settle it all"
/// shape, used by adapters that allocate and immediately own the result.
pub fn grant_and_fulfil(
    account: &AccountHandle,
    bytes: u64,
) -> Result<(CapacityGrant, Charge), CapacityError> {
    let grant = account.request_grant(bytes)?;
    match grant.fulfil(bytes) {
        Ok(charge) => Ok((grant, charge)),
        // Fulfilling the exact amount just granted cannot exceed the
        // remainder, and nothing has had the chance to revoke it yet.
        Err(error) => Err(CapacityError::Unsupported {
            detail: match error {
                FulfilError::Cancelled { .. } => "grant revoked before its first fulfilment",
                FulfilError::ExceedsRemainder { .. } => "grant remainder shrank unexpectedly",
            },
        }),
    }
}

/// Shared handle to a grant, for the adapters that must hand the same issued
/// capacity to several collaborating objects.
pub type SharedGrant = Arc<CapacityGrant>;
