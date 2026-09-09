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

//! Turning issued capacity into Arrow reservations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_buffer::{MemoryPool, MemoryReservation};
use novarocks_memory::account::AccountHandle;
use novarocks_memory::charge::ChargeState;
use novarocks_memory::grant::{CapacityGrant, SharedGrant};

use crate::receipt::ClaimReceipt;
use crate::reservation::ChargeReservation;

/// An Arrow memory pool backed by capacity the core already issued.
///
/// This is the link the plan's design turns on: `MemoryPool::reserve` cannot
/// fail, so it must not be where capacity is decided. The pool is built from a
/// grant, and reserving converts that grant's unfulfilled `F` into known live
/// `L`. A bare account is deliberately not accepted as a pool: without a grant
/// in hand every claim would be an unbudgeted allocation.
#[derive(Debug)]
pub struct FulfilmentPool {
    grant: SharedGrant,
    issued_bytes: u64,
    excess_bytes: AtomicU64,
    excess_events: AtomicU64,
}

impl FulfilmentPool {
    /// Builds a pool from issued capacity.
    pub fn new(grant: CapacityGrant) -> Self {
        Self::from_shared(Arc::new(grant))
    }

    /// Builds a pool from capacity already shared with other collaborators.
    pub fn from_shared(grant: SharedGrant) -> Self {
        let issued_bytes = grant.remaining_bytes();
        Self {
            grant,
            issued_bytes,
            excess_bytes: AtomicU64::new(0),
            excess_events: AtomicU64::new(0),
        }
    }

    /// Returns the account this pool charges.
    pub fn account(&self) -> &AccountHandle {
        self.grant.account()
    }

    /// Returns the grant this pool fulfils.
    pub fn grant(&self) -> &SharedGrant {
        &self.grant
    }

    /// Returns the capacity the grant held when the pool was built.
    pub const fn issued_bytes(&self) -> u64 {
        self.issued_bytes
    }

    /// Returns bytes this pool charged beyond its grant's remainder.
    ///
    /// A non-zero value means an allocation happened that nobody had
    /// authorised. The bytes are charged and the account's growth is frozen;
    /// this counter is how a caller sees that its own sizing was wrong.
    pub fn excess_bytes(&self) -> u64 {
        self.excess_bytes.load(Ordering::Acquire)
    }

    /// Returns how many separate reservations had to absorb excess.
    pub fn excess_events(&self) -> u64 {
        self.excess_events.load(Ordering::Acquire)
    }

    /// Creates one reservation, charging it to this pool's account.
    fn make_reservation(&self, size: u64) -> ChargeReservation {
        match self.grant.fulfil(size) {
            Ok(charge) => ChargeReservation::new(charge.into_state(), Arc::clone(&self.grant)),
            Err(_) => {
                // The buffer exists. Charging it as excess keeps the account
                // honest and stops its growth; declining to account for it
                // would make the same bytes invisible.
                self.excess_bytes.fetch_add(size, Ordering::AcqRel);
                self.excess_events.fetch_add(1, Ordering::AcqRel);
                let charge = self.account().absorb_unbudgeted_live(size);
                ChargeReservation::new(charge.into_state(), Arc::clone(&self.grant))
            }
        }
    }

    /// Opens a claim session that records the charges it creates.
    ///
    /// A session exists so a caller can get a [`ClaimReceipt`] back. Without
    /// one, `reserve` would have nowhere to report the charge it made, and the
    /// only way to move a charge later would be to claim the backing again —
    /// which double counts for an instant.
    pub fn begin_session(&self) -> ClaimSession<'_> {
        ClaimSession {
            pool: self,
            collected: Mutex::new(Vec::new()),
        }
    }
}

impl MemoryPool for FulfilmentPool {
    fn reserve(&self, size: usize) -> Box<dyn MemoryReservation> {
        Box::new(self.make_reservation(size as u64))
    }

    fn available(&self) -> isize {
        let remaining = self.grant.remaining_bytes();
        let excess = self.excess_bytes();
        // Reported as a signed value because an overfilled pool is a real
        // state: excess beyond the grant makes the pool owe capacity.
        isize::try_from(remaining).unwrap_or(isize::MAX)
            - isize::try_from(excess).unwrap_or(isize::MAX)
    }

    fn used(&self) -> usize {
        let fulfilled = self
            .issued_bytes
            .saturating_sub(self.grant.remaining_bytes());
        usize::try_from(fulfilled.saturating_add(self.excess_bytes())).unwrap_or(usize::MAX)
    }

    fn capacity(&self) -> usize {
        usize::try_from(self.issued_bytes).unwrap_or(usize::MAX)
    }
}

/// One claim, recording the charges it created.
///
/// The session is itself the pool Arrow is handed, so every reservation made
/// during the claim is collected and comes back in the receipt. It borrows the
/// underlying pool, so a session cannot outlive the capacity it draws on, and
/// concurrent claims each get their own collector rather than sharing one
/// unbounded list.
#[derive(Debug)]
pub struct ClaimSession<'a> {
    pool: &'a FulfilmentPool,
    collected: Mutex<Vec<Arc<ChargeState>>>,
}

impl ClaimSession<'_> {
    /// Returns the pool this session draws capacity from.
    pub const fn pool(&self) -> &FulfilmentPool {
        self.pool
    }

    /// Closes the session and returns what it charged.
    pub fn into_receipt(self) -> ClaimReceipt {
        let collected = self
            .collected
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ClaimReceipt::new(collected)
    }
}

impl MemoryPool for ClaimSession<'_> {
    fn reserve(&self, size: usize) -> Box<dyn MemoryReservation> {
        let reservation = self.pool.make_reservation(size as u64);
        self.collected
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(Arc::clone(reservation.state()));
        Box::new(reservation)
    }

    fn available(&self) -> isize {
        self.pool.available()
    }

    fn used(&self) -> usize {
        self.pool.used()
    }

    fn capacity(&self) -> usize {
        self.pool.capacity()
    }
}
