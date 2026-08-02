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

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub trait RuntimeFilterClock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryAccountError {
    CapacityExceeded,
}

pub trait RuntimeFilterMemoryAccount: Send + Sync {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError>;
    fn release(&self, bytes: usize);
}

#[derive(Debug)]
pub struct ArtifactRetainedBudget {
    max_bytes: usize,
    retained_bytes: AtomicUsize,
}

impl ArtifactRetainedBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            retained_bytes: AtomicUsize::new(0),
        }
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<ArtifactRetainedLease, RetainedReservationError> {
        let mut retained = self.retained_bytes.load(Ordering::Acquire);
        loop {
            let next = retained
                .checked_add(bytes)
                .ok_or(RetainedReservationError::SizeOverflow)?;
            if next > self.max_bytes {
                return Err(RetainedReservationError::CapacityExceeded);
            }
            match self.retained_bytes.compare_exchange_weak(
                retained,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ArtifactRetainedLease {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => retained = actual,
            }
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }
}

pub struct ArtifactRetainedLease {
    budget: Arc<ArtifactRetainedBudget>,
    bytes: usize,
}

#[derive(Debug)]
pub struct ArtifactScratchBudget {
    max_bytes_per_job: usize,
    max_total_bytes: usize,
    retained_bytes: AtomicUsize,
}

impl ArtifactScratchBudget {
    pub fn new(
        max_bytes_per_job: usize,
        max_total_bytes: usize,
    ) -> Result<Self, RetainedReservationError> {
        if max_bytes_per_job == 0 || max_total_bytes == 0 || max_bytes_per_job > max_total_bytes {
            return Err(RetainedReservationError::CapacityExceeded);
        }
        Ok(Self {
            max_bytes_per_job,
            max_total_bytes,
            retained_bytes: AtomicUsize::new(0),
        })
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<ArtifactScratchLease, RetainedReservationError> {
        if bytes > self.max_bytes_per_job {
            return Err(RetainedReservationError::CapacityExceeded);
        }
        let mut retained = self.retained_bytes.load(Ordering::Acquire);
        loop {
            let next = retained
                .checked_add(bytes)
                .ok_or(RetainedReservationError::SizeOverflow)?;
            if next > self.max_total_bytes {
                return Err(RetainedReservationError::CapacityExceeded);
            }
            match self.retained_bytes.compare_exchange_weak(
                retained,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ArtifactScratchLease {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => retained = actual,
            }
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Acquire)
    }
}

pub struct ArtifactScratchLease {
    budget: Arc<ArtifactScratchBudget>,
    bytes: usize,
}

impl ArtifactScratchLease {
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for ArtifactScratchLease {
    fn drop(&mut self) {
        if self.bytes != 0 {
            let previous = self
                .budget
                .retained_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes);
        }
    }
}

impl fmt::Debug for ArtifactScratchLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactScratchLease")
            .field("bytes", &self.bytes)
            .finish()
    }
}

pub struct ArtifactScratchReservation {
    budget: ArtifactScratchLease,
    memory: MemoryLease,
}

impl fmt::Debug for ArtifactScratchReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactScratchReservation")
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl ArtifactScratchReservation {
    pub fn try_new(
        bytes: usize,
        budget: Arc<ArtifactScratchBudget>,
        account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, RetainedReservationError> {
        let budget = budget.try_acquire(bytes)?;
        let memory = MemoryLease::try_new(account, bytes)
            .map_err(|_| RetainedReservationError::CapacityExceeded)?;
        Ok(Self { budget, memory })
    }

    pub const fn bytes(&self) -> usize {
        self.memory.bytes
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget.bytes()
    }
}

impl ArtifactRetainedLease {
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for ArtifactRetainedLease {
    fn drop(&mut self) {
        if self.bytes != 0 {
            let previous = self
                .budget
                .retained_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes);
        }
    }
}

impl fmt::Debug for ArtifactRetainedLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactRetainedLease")
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[derive(Debug)]
pub struct ArtifactRetention {
    budget: ArtifactRetainedLease,
    memory: RetainedMemoryReservation,
}

impl ArtifactRetention {
    pub fn try_new(
        bytes: usize,
        budget: Arc<ArtifactRetainedBudget>,
        account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<Self, RetainedReservationError> {
        let budget = budget.try_acquire(bytes)?;
        let memory = RetainedMemoryReservation::try_new(account, bytes)?;
        Ok(Self { budget, memory })
    }

    pub const fn bytes(&self) -> usize {
        self.memory.bytes()
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget.bytes()
    }
}

struct MemoryLease {
    account: Arc<dyn RuntimeFilterMemoryAccount>,
    bytes: usize,
}

impl MemoryLease {
    fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, MemoryAccountError> {
        if bytes != 0 {
            account.try_consume(bytes)?;
        }
        Ok(Self { account, bytes })
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.account.release(self.bytes);
        }
    }
}

pub struct TemporaryContributionLease(MemoryLease);

impl TemporaryContributionLease {
    pub fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, MemoryAccountError> {
        MemoryLease::try_new(account, bytes).map(Self)
    }

    #[cfg(any(test, feature = "runtime-filter-test-support"))]
    pub fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        Self::try_new(account, bytes).expect("test memory account accepts reservation")
    }

    pub const fn bytes(&self) -> usize {
        self.0.bytes
    }
}

impl fmt::Debug for TemporaryContributionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TemporaryContributionLease")
            .field("bytes", &self.bytes())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainedReservationError {
    SizeOverflow,
    AccountMismatch,
    CapacityExceeded,
}

impl fmt::Display for RetainedReservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeOverflow => write!(formatter, "retained reservation size overflow"),
            Self::AccountMismatch => write!(formatter, "retained reservation account mismatch"),
            Self::CapacityExceeded => {
                write!(formatter, "retained reservation account rejected bytes")
            }
        }
    }
}

impl std::error::Error for RetainedReservationError {}

#[derive(Default)]
pub struct RetainedMemoryReservation {
    account: Option<Arc<dyn RuntimeFilterMemoryAccount>>,
    bytes: usize,
}

pub struct RetainedReservationAbsorbFailure {
    error: RetainedReservationError,
    incoming: RetainedMemoryReservation,
}

impl RetainedReservationAbsorbFailure {
    #[cfg(test)]
    pub const fn error(&self) -> RetainedReservationError {
        self.error
    }

    pub fn into_parts(self) -> (RetainedReservationError, RetainedMemoryReservation) {
        (self.error, self.incoming)
    }
}

impl fmt::Debug for RetainedReservationAbsorbFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedReservationAbsorbFailure")
            .field("error", &self.error)
            .field("incoming_bytes", &self.incoming.bytes())
            .finish()
    }
}

impl RetainedMemoryReservation {
    pub const fn empty() -> Self {
        Self {
            account: None,
            bytes: 0,
        }
    }

    pub fn try_new(
        account: Arc<dyn RuntimeFilterMemoryAccount>,
        bytes: usize,
    ) -> Result<Self, RetainedReservationError> {
        if bytes == 0 {
            return Ok(Self::empty());
        }
        account
            .try_consume(bytes)
            .map_err(|_| RetainedReservationError::CapacityExceeded)?;
        Ok(Self {
            account: Some(account),
            bytes,
        })
    }

    #[cfg(test)]
    pub fn new(account: Arc<dyn RuntimeFilterMemoryAccount>, bytes: usize) -> Self {
        Self::try_new(account, bytes).expect("test memory account accepts reservation")
    }

    pub fn absorb(&mut self, mut incoming: Self) -> Result<(), RetainedReservationAbsorbFailure> {
        if incoming.bytes == 0 {
            return Ok(());
        }
        if self.bytes == 0 {
            self.account = incoming.account.take();
            self.bytes = incoming.bytes;
            incoming.bytes = 0;
            return Ok(());
        }

        let account = self
            .account
            .as_ref()
            .expect("non-empty reservation account");
        let incoming_account = incoming
            .account
            .as_ref()
            .expect("non-empty incoming reservation account");
        if !Arc::ptr_eq(account, incoming_account) {
            return Err(RetainedReservationAbsorbFailure {
                error: RetainedReservationError::AccountMismatch,
                incoming,
            });
        }
        let Some(bytes) = self.bytes.checked_add(incoming.bytes) else {
            return Err(RetainedReservationAbsorbFailure {
                error: RetainedReservationError::SizeOverflow,
                incoming,
            });
        };

        self.bytes = bytes;
        incoming.bytes = 0;
        incoming.account = None;
        Ok(())
    }

    pub fn split_off_excess(&mut self, retained_bytes: usize) -> Self {
        assert!(
            retained_bytes <= self.bytes,
            "retained reservation cannot grow while splitting excess"
        );
        let released_bytes = self.bytes - retained_bytes;
        if released_bytes == 0 {
            return Self::empty();
        }
        let account = self
            .account
            .as_ref()
            .expect("non-empty reservation account")
            .clone();
        self.bytes = retained_bytes;
        if retained_bytes == 0 {
            self.account = None;
        }
        Self {
            account: Some(account),
            bytes: released_bytes,
        }
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for RetainedMemoryReservation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.account
                .as_ref()
                .expect("non-empty reservation account")
                .release(self.bytes);
        }
    }
}

impl fmt::Debug for RetainedMemoryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedMemoryReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime_filter::model::contract::ChannelId;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    #[derive(Default)]
    struct CountingMemoryAccount(AtomicUsize);

    impl RuntimeFilterMemoryAccount for CountingMemoryAccount {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    struct RejectingMemoryAccount;

    impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Err(MemoryAccountError::CapacityExceeded)
        }

        fn release(&self, _bytes: usize) {}
    }

    #[test]
    fn temporary_and_retained_reservations_propagate_account_rejection() {
        let account = Arc::new(RejectingMemoryAccount);
        assert!(TemporaryContributionLease::try_new(account.clone(), 1).is_err());
        assert!(RetainedMemoryReservation::try_new(account, 1).is_err());
    }

    #[test]
    fn artifact_budget_is_atomic_and_combined_reservation_rolls_back() {
        let budget = Arc::new(ArtifactRetainedBudget::new(64));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let leases = (0..2)
            .map(|_| {
                let budget = budget.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    budget.try_acquire(40)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let mut accepted = Vec::new();
        let mut rejected = 0;
        for lease in leases {
            match lease.join().unwrap() {
                Ok(lease) => accepted.push(lease),
                Err(RetainedReservationError::CapacityExceeded) => rejected += 1,
                Err(error) => panic!("unexpected artifact budget error: {error:?}"),
            }
        }
        assert_eq!(accepted.len(), 1);
        assert_eq!(rejected, 1);
        assert_eq!(budget.retained_bytes(), 40);
        drop(accepted);
        assert_eq!(budget.retained_bytes(), 0);

        assert!(
            ArtifactRetention::try_new(32, budget.clone(), Arc::new(RejectingMemoryAccount))
                .is_err()
        );
        assert_eq!(budget.retained_bytes(), 0);
    }

    #[test]
    fn scratch_budget_enforces_per_job_aggregate_and_memory_account_raii() {
        assert!(ArtifactScratchBudget::new(0, 64).is_err());
        assert!(ArtifactScratchBudget::new(65, 64).is_err());
        let budget = Arc::new(ArtifactScratchBudget::new(40, 64).unwrap());
        assert!(budget.try_acquire(41).is_err());

        let first = budget.try_acquire(40).unwrap();
        assert_eq!(budget.retained_bytes(), 40);
        assert!(budget.try_acquire(25).is_err());
        drop(first);
        assert_eq!(budget.retained_bytes(), 0);

        assert!(
            ArtifactScratchReservation::try_new(
                32,
                budget.clone(),
                Arc::new(RejectingMemoryAccount),
            )
            .is_err()
        );
        assert_eq!(budget.retained_bytes(), 0);

        let account = Arc::new(CountingMemoryAccount::default());
        {
            let reservation =
                ArtifactScratchReservation::try_new(32, budget.clone(), account.clone()).unwrap();
            assert_eq!(reservation.bytes(), 32);
            assert_eq!(reservation.budget_bytes(), 32);
            assert_eq!(budget.retained_bytes(), 32);
            assert_eq!(account.0.load(Ordering::SeqCst), 32);
        }
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn temporary_and_retained_reservations_have_distinct_raii_ownership() {
        let account = Arc::new(CountingMemoryAccount::default());

        {
            let _temporary = TemporaryContributionLease::new(account.clone(), 11);
            assert_eq!(account.0.load(Ordering::SeqCst), 11);
        }
        assert_eq!(account.0.load(Ordering::SeqCst), 0);

        let mut retained = RetainedMemoryReservation::empty();
        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 13))
            .unwrap();
        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 17))
            .unwrap();
        assert_eq!(retained.bytes(), 30);
        assert_eq!(account.0.load(Ordering::SeqCst), 30);
        drop(retained);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn logical_snapshot_arc_keeps_retained_bytes_until_the_last_owner_drops() {
        let account = Arc::new(CountingMemoryAccount::default());
        let snapshot = Arc::new(LogicalSnapshot::first(
            ChannelId::new(1),
            ReducedMembershipDomain::new(MembershipValues::int64([7]), false),
            RetainedMemoryReservation::new(account.clone(), 23),
        ));
        let last_owner = snapshot.clone();

        assert_eq!(account.0.load(Ordering::SeqCst), 23);
        drop(snapshot);
        assert_eq!(account.0.load(Ordering::SeqCst), 23);
        drop(last_owner);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_zero_reservations_are_empty_and_do_not_grow_an_account() {
        let account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(account.clone(), 0);

        retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 0))
            .unwrap();

        assert_eq!(retained.bytes(), 0);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_absorb_overflow_keeps_self_and_releases_incoming() {
        let account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(account.clone(), usize::MAX);

        let failure = retained
            .absorb(RetainedMemoryReservation::new(account.clone(), 1))
            .unwrap_err();
        assert_eq!(failure.error(), RetainedReservationError::SizeOverflow);
        drop(failure);
        assert_eq!(retained.bytes(), usize::MAX);
        assert_eq!(account.0.load(Ordering::SeqCst), usize::MAX);
        drop(retained);
        assert_eq!(account.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_absorb_rejects_account_mismatch_without_leaking_incoming() {
        let left_account = Arc::new(CountingMemoryAccount::default());
        let right_account = Arc::new(CountingMemoryAccount::default());
        let mut retained = RetainedMemoryReservation::new(left_account.clone(), 11);

        let failure = retained
            .absorb(RetainedMemoryReservation::new(right_account.clone(), 13))
            .unwrap_err();
        assert_eq!(failure.error(), RetainedReservationError::AccountMismatch);
        drop(failure);
        assert_eq!(retained.bytes(), 11);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 11);
        assert_eq!(right_account.0.load(Ordering::SeqCst), 0);
        drop(retained);
        assert_eq!(left_account.0.load(Ordering::SeqCst), 0);
    }
}
