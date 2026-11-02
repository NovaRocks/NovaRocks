// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One atomic, multidimensional ledger. Callers synchronize this with its scope.

use crate::identity::{DimensionId, Generation, ReservationId, ScopeId, WorkId};
use crate::owned_storage::{
    OwnedPosition, OwnedPositionSpan, OwnedSlotKind, OwnedSlotNeed, OwnedStorageError,
    OwnedStorageInventory,
};
use crate::scope::{ScopeError, ScopeState, WorkAdmission, WorkKind};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceVector(BTreeMap<DimensionId, u64>);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityError {
    DuplicateDimension,
    UnknownDimension,
    Overflow,
    Exhausted(DimensionId),
    DuplicateReservation,
    UnknownReservation,
    EmptyReservation,
    WrongOwner,
    BoundReservation,
    UnboundReservation,
    InvalidSplit,
    Scope(ScopeError),
    OwnedStorage(OwnedStorageError),
    QuotaRemains,
}
impl From<OwnedStorageError> for CapacityError {
    fn from(error: OwnedStorageError) -> Self {
        Self::OwnedStorage(error)
    }
}
impl From<ScopeError> for CapacityError {
    fn from(e: ScopeError) -> Self {
        Self::Scope(e)
    }
}
impl ResourceVector {
    pub fn new(items: impl IntoIterator<Item = (DimensionId, u64)>) -> Result<Self, CapacityError> {
        let mut entries = BTreeMap::new();
        for (id, value) in items {
            if entries.insert(id, value).is_some() {
                return Err(CapacityError::DuplicateDimension);
            }
        }
        entries.retain(|_, value| *value != 0);
        Ok(Self(entries))
    }
    pub fn get(&self, dimension: DimensionId) -> u64 {
        self.0.get(&dimension).copied().unwrap_or(0)
    }
    pub fn iter(&self) -> impl Iterator<Item = (DimensionId, u64)> + '_ {
        self.0.iter().map(|(id, value)| (*id, *value))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn checked_add(&self, other: &Self) -> Result<Self, CapacityError> {
        let mut result = self.clone();
        for (dimension, amount) in other.iter() {
            let next = result
                .get(dimension)
                .checked_add(amount)
                .ok_or(CapacityError::Overflow)?;
            result.0.insert(dimension, next);
        }
        Ok(result)
    }
    fn checked_sub(&self, other: &Self) -> Result<Self, CapacityError> {
        let mut result = self.clone();
        for (dimension, amount) in other.iter() {
            let next = result
                .get(dimension)
                .checked_sub(amount)
                .ok_or(CapacityError::InvalidSplit)?;
            if next == 0 {
                result.0.remove(&dimension);
            } else {
                result.0.insert(dimension, next);
            }
        }
        Ok(result)
    }
}
/// Requested or observed values only. Cloning this description never clones
/// quota, a position, or Work. Storage is not a capacity dimension.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReservationResources {
    pub quota: ResourceVector,
    pub owned_slots: OwnedSlotNeed,
}
impl ReservationResources {
    pub fn new(quota: ResourceVector, owned_slots: OwnedSlotNeed) -> Self {
        Self { quota, owned_slots }
    }
    pub fn work_only() -> Self {
        Self::new(ResourceVector::default(), OwnedSlotNeed::work_only())
    }
    pub fn for_work(quota: ResourceVector) -> Self {
        Self::new(quota, OwnedSlotNeed::work_only())
    }
    pub fn is_empty(&self) -> bool {
        self.quota.is_empty() && self.owned_slots.is_empty()
    }
    pub fn checked_add(&self, other: &Self) -> Result<Self, CapacityError> {
        Ok(Self::new(
            self.quota.checked_add(&other.quota)?,
            self.owned_slots.checked_add(other.owned_slots)?,
        ))
    }
}
#[derive(Debug)]
pub struct ReservationRecord {
    pub scope: ScopeId,
    pub generation: Generation,
    pub work: WorkId,
    pub quota: ResourceVector,
    owned_slots: OwnedStorageInventory,
    pub bound: bool,
    owns_work: bool,
}
impl ReservationRecord {
    pub fn owned_slots(&self) -> OwnedSlotNeed {
        self.owned_slots.remaining()
    }
    pub fn resources(&self) -> ReservationResources {
        ReservationResources::new(self.quota.clone(), self.owned_slots.remaining())
    }
}
#[derive(Clone, Debug)]
pub struct ReservationRequest {
    pub id: ReservationId,
    pub generation: Generation,
    pub guard: WorkId,
    pub resources: ReservationResources,
    pub admission: WorkAdmission,
    pub kind: WorkKind,
}
/// Same-transaction projection of the original reservations for one dimension.
/// Only nonzero owners occupy rows; the histogram is an index, not a payer or
/// a second resource ledger. Its maximum is observable without an owner scan.
#[derive(Debug)]
struct OwnerUsageIndex {
    by_scope: BTreeMap<ScopeId, u64>,
    histogram: BTreeMap<u64, usize>,
}
impl OwnerUsageIndex {
    fn check_add(&self, scope: ScopeId, amount: u64) -> Result<(), CapacityError> {
        if amount == 0 {
            return Ok(());
        }
        self.by_scope
            .get(&scope)
            .copied()
            .unwrap_or(0)
            .checked_add(amount)
            .ok_or(CapacityError::Overflow)?;
        Ok(())
    }
    fn replace(&mut self, scope: ScopeId, next: u64) {
        if let Some(old) = self.by_scope.remove(&scope) {
            let count = self
                .histogram
                .get_mut(&old)
                .expect("indexed original owner usage");
            *count = count.checked_sub(1).expect("owner usage histogram count");
            if *count == 0 {
                self.histogram.remove(&old);
            }
        }
        if next != 0 {
            self.by_scope.insert(scope, next);
            let count = self.histogram.entry(next).or_default();
            *count = count
                .checked_add(1)
                .expect("owner usage histogram count exhausted");
        }
    }
    fn add(&mut self, scope: ScopeId, amount: u64) {
        if amount != 0 {
            let next = self
                .by_scope
                .get(&scope)
                .copied()
                .unwrap_or(0)
                .checked_add(amount)
                .expect("validated aggregate owner addition");
            self.replace(scope, next);
        }
    }
    fn subtract(&mut self, scope: ScopeId, amount: u64) {
        if amount != 0 {
            let next = self
                .by_scope
                .get(&scope)
                .copied()
                .expect("original indexed resource owner")
                .checked_sub(amount)
                .expect("original owner resources cover release");
            self.replace(scope, next);
        }
    }
}
#[derive(Debug)]
pub struct CapacityLedger {
    limits: ResourceVector,
    used: ResourceVector,
    reservations: BTreeMap<ReservationId, ReservationRecord>,
    owner_counts: BTreeMap<ScopeId, usize>,
    owner_usage: BTreeMap<DimensionId, OwnerUsageIndex>,
    last_reservation: u64,
    healthy: bool,
}
impl CapacityLedger {
    pub fn new(limits: ResourceVector) -> Self {
        Self {
            limits,
            used: ResourceVector::default(),
            reservations: BTreeMap::new(),
            owner_counts: BTreeMap::new(),
            owner_usage: BTreeMap::new(),
            last_reservation: 0,
            healthy: true,
        }
    }
    /// Configure only dimensions needed by an actual owner read port.
    /// Metadata follows the original nonzero owners without a separate quota.
    pub fn with_owner_usage_dimensions(
        limits: ResourceVector,
        dimensions: impl IntoIterator<Item = DimensionId>,
    ) -> Result<Self, CapacityError> {
        let mut ledger = Self::new(limits);
        for dimension in dimensions {
            if !ledger.limits.0.contains_key(&dimension) {
                return Err(CapacityError::UnknownDimension);
            }
            if ledger
                .owner_usage
                .insert(
                    dimension,
                    OwnerUsageIndex {
                        by_scope: BTreeMap::new(),
                        histogram: BTreeMap::new(),
                    },
                )
                .is_some()
            {
                return Err(CapacityError::DuplicateDimension);
            }
        }
        Ok(ledger)
    }
    /// Maximum actual charged units for a configured dimension. None means
    /// no index was configured, never an observed zero-resource owner.
    pub fn max_owner_usage(&self, dimension: DimensionId) -> Option<u64> {
        self.owner_usage.get(&dimension).map(|index| {
            index
                .histogram
                .last_key_value()
                .map_or(0, |(&usage, _)| usage)
        })
    }
    /// Actual metadata entries for the original dimension, nonzero owners and
    /// usage histogram. This observation grants no storage or resource permit.
    pub fn owner_usage_metadata_entries(&self, dimension: DimensionId) -> Option<usize> {
        self.owner_usage.get(&dimension).map(|index| {
            index
                .by_scope
                .len()
                .checked_add(index.histogram.len())
                .and_then(|n| n.checked_add(1))
                .expect("owner index entry count exhausted")
        })
    }
    fn check_owner_add(
        &self,
        scope: ScopeId,
        resources: &ResourceVector,
    ) -> Result<(), CapacityError> {
        for (&dimension, index) in &self.owner_usage {
            index.check_add(scope, resources.get(dimension))?;
        }
        Ok(())
    }
    fn add_owner_usage(&mut self, scope: ScopeId, resources: &ResourceVector) {
        for (&dimension, index) in &mut self.owner_usage {
            index.add(scope, resources.get(dimension));
        }
    }
    fn subtract_owner_usage(&mut self, scope: ScopeId, resources: &ResourceVector) {
        for (&dimension, index) in &mut self.owner_usage {
            index.subtract(scope, resources.get(dimension));
        }
    }
    pub fn used(&self, dimension: DimensionId) -> u64 {
        self.used.get(dimension)
    }
    pub fn limit(&self, dimension: DimensionId) -> u64 {
        self.limits.get(dimension)
    }
    pub fn dimension_usage(&self, dimension: DimensionId) -> Option<(u64, u64)> {
        self.limits
            .0
            .get(&dimension)
            .map(|&limit| (self.used(dimension), limit))
    }
    pub fn reservation(&self, id: ReservationId) -> Option<&ReservationRecord> {
        self.reservations.get(&id)
    }
    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }
    /// Read only the next original reservation in a bounded observation range.
    pub fn observed_reservation_after(
        &self,
        after: Option<ReservationId>,
        until: ReservationId,
    ) -> Option<(ReservationId, &ReservationRecord)> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        self.reservations
            .range((after.map_or(Unbounded, Excluded), Included(until)))
            .next()
            .map(|(id, record)| (*id, record))
    }
    pub fn last_observed_reservation(&self) -> Option<ReservationId> {
        self.reservations.last_key_value().map(|(id, _)| *id)
    }
    /// Use under the same transaction as reserve, rather than assigning IDs
    /// on another thread before the ledger linearization point.
    pub fn next_reservation_id(&self) -> Result<ReservationId, CapacityError> {
        self.last_reservation
            .checked_add(1)
            .and_then(ReservationId::new)
            .ok_or(CapacityError::Overflow)
    }
    /// An index of the authoritative reservation records, updated in the same
    /// transaction. Scope gates never scan other owners' reservations.
    pub fn reservation_count_for(&self, scope: ScopeId) -> usize {
        self.owner_counts.get(&scope).copied().unwrap_or(0)
    }
    fn add_owner_record(&mut self, scope: ScopeId) {
        let count = self.owner_counts.entry(scope).or_default();
        *count = count
            .checked_add(1)
            .expect("reservation owner count exhausted");
    }
    fn remove_owner_record(&mut self, scope: ScopeId) {
        let count = self
            .owner_counts
            .get_mut(&scope)
            .expect("reservation has its owner index");
        *count = count
            .checked_sub(1)
            .expect("reservation owner count underflow");
        if *count == 0 {
            self.owner_counts.remove(&scope);
        }
    }
    pub fn reservations_for(
        &self,
        scope: ScopeId,
    ) -> impl Iterator<Item = (ReservationId, &ReservationRecord)> + '_ {
        self.reservations
            .iter()
            .filter(move |(_, r)| r.scope == scope)
            .map(|(id, r)| (*id, r))
    }
    pub fn close_admission(&mut self) {
        self.healthy = false;
    }
    /// Scope, quota and owned storage all preflight before one Work is admitted.
    pub fn try_reserve_bundle(
        &mut self,
        scope: &mut ScopeState,
        request: ReservationRequest,
    ) -> Result<ReservationId, CapacityError> {
        if !self.healthy {
            return Err(ScopeError::Failed.into());
        }
        if request.id.get() <= self.last_reservation {
            return Err(CapacityError::DuplicateReservation);
        }
        scope.check_admission(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
        )?;
        for (dimension, amount) in request.resources.quota.iter() {
            let limit = self
                .limits
                .0
                .get(&dimension)
                .ok_or(CapacityError::UnknownDimension)?;
            let next = self
                .used(dimension)
                .checked_add(amount)
                .ok_or(CapacityError::Overflow)?;
            if next > *limit {
                return Err(CapacityError::Exhausted(dimension));
            }
        }
        let next = self.used.checked_add(&request.resources.quota)?;
        self.check_owner_add(scope.id(), &request.resources.quota)?;
        let mut owned = OwnedStorageInventory::allocate(request.id, request.resources.owned_slots)?;
        let header = owned.take(OwnedSlotKind::WorkHeader)?;
        scope.admit_guard(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
            header,
        )?;
        self.used = next;
        self.add_owner_usage(scope.id(), &request.resources.quota);
        self.last_reservation = request.id.get();
        self.reservations.insert(
            request.id,
            ReservationRecord {
                scope: scope.id(),
                generation: request.generation,
                work: request.guard,
                quota: request.resources.quota,
                owned_slots: owned,
                bound: false,
                owns_work: true,
            },
        );
        self.add_owner_record(scope.id());
        Ok(request.id)
    }
    /// Moves original unused storage and already-held quota into precisely one
    /// real child. No quota is acquired and no intermediate role is exposed.
    pub fn reserve_from(
        &mut self,
        scope: &mut ScopeState,
        payer: ReservationId,
        request: ReservationRequest,
    ) -> Result<ReservationId, CapacityError> {
        if !self.healthy {
            return Err(ScopeError::Failed.into());
        }
        if request.id.get() <= self.last_reservation {
            return Err(CapacityError::DuplicateReservation);
        }
        scope.check_admission(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
        )?;
        let original = self
            .reservations
            .get(&payer)
            .ok_or(CapacityError::UnknownReservation)?;
        if original.scope != scope.id() {
            return Err(CapacityError::WrongOwner);
        }
        if !original.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let remaining_quota = original.quota.checked_sub(&request.resources.quota)?;
        original
            .owned_slots
            .check_split(request.resources.owned_slots)?;
        if request.resources.owned_slots.work_headers == 0 {
            return Err(OwnedStorageError::MissingPosition(OwnedSlotKind::WorkHeader).into());
        }
        let original = self
            .reservations
            .get_mut(&payer)
            .expect("validated original payer");
        let mut owned = original
            .owned_slots
            .split_unused(request.id, request.resources.owned_slots)?;
        let header = owned
            .take(OwnedSlotKind::WorkHeader)
            .expect("preflighted child header");
        scope
            .admit_guard(
                request.generation,
                request.guard,
                request.kind,
                request.admission,
                header,
            )
            .expect("same transaction retains the admitted parent");
        original.quota = remaining_quota;
        self.last_reservation = request.id.get();
        self.reservations.insert(
            request.id,
            ReservationRecord {
                scope: scope.id(),
                generation: request.generation,
                work: request.guard,
                quota: request.resources.quota,
                owned_slots: owned,
                bound: false,
                owns_work: true,
            },
        );
        self.add_owner_record(scope.id());
        self.remove_empty_partition(payer);
        Ok(request.id)
    }
    pub fn bind(
        &mut self,
        scope: &mut ScopeState,
        id: ReservationId,
        kind: WorkKind,
    ) -> Result<(), CapacityError> {
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if record.scope != scope.id() {
            return Err(CapacityError::WrongOwner);
        }
        if record.bound {
            return Err(CapacityError::BoundReservation);
        }
        scope.bind_guard(record.work, kind)?;
        self.reservations
            .get_mut(&id)
            .expect("validated reservation")
            .bound = true;
        Ok(())
    }
    /// Only never-bound guards can refund and withdraw their original Work.
    pub fn rollback_unbound(
        &mut self,
        scope: &mut ScopeState,
        id: ReservationId,
    ) -> Result<(), CapacityError> {
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if record.scope != scope.id() {
            return Err(CapacityError::WrongOwner);
        }
        if record.bound {
            return Err(CapacityError::BoundReservation);
        }
        let next = self.used.checked_sub(&record.quota)?;
        let quota = record.quota.clone();
        scope.rollback_guard(record.work)?;
        self.used = next;
        self.subtract_owner_usage(scope.id(), &quota);
        self.reservations.remove(&id);
        self.remove_owner_record(scope.id());
        Ok(())
    }
    /// This releases quota only. Moved/bound positions are never refunded here.
    pub fn release_quota(
        &mut self,
        id: ReservationId,
        quota: &ResourceVector,
    ) -> Result<(), CapacityError> {
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let owner = record.scope;
        let remaining = record.quota.checked_sub(quota)?;
        let next = self.used.checked_sub(quota)?;
        self.used = next;
        self.subtract_owner_usage(owner, quota);
        self.reservations
            .get_mut(&id)
            .expect("validated original quota")
            .quota = remaining;
        Ok(())
    }
    /// Work already owns its header. Actual reports and other bound objects
    /// own their moved positions and independently pin their real scope slots.
    pub fn finish(&mut self, scope: &ScopeState, id: ReservationId) -> Result<(), CapacityError> {
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if record.scope != scope.id() {
            return Err(CapacityError::WrongOwner);
        }
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        if !record.quota.is_empty() {
            return Err(CapacityError::QuotaRemains);
        }
        if record.owns_work
            && let Some(work) = scope.work(record.work)
        {
            if work.children != 0 {
                return Err(ScopeError::ChildrenRemain.into());
            }
            if !work.body_exited || !work.captures_exited {
                return Err(ScopeError::ExitUnobserved.into());
            }
        }
        let owner = record.scope;
        self.reservations.remove(&id);
        self.remove_owner_record(owner);
        Ok(())
    }
    fn remove_empty_partition(&mut self, id: ReservationId) {
        if self.reservations.get(&id).is_some_and(|record| {
            !record.owns_work
                && record.quota.is_empty()
                && record.owned_slots.remaining().is_empty()
        }) {
            let owner = self
                .reservations
                .remove(&id)
                .expect("empty original partition")
                .scope;
            self.remove_owner_record(owner);
        }
    }
    /// Only unused positions remain in a bundle. Taken positions physically
    /// moved into their original Work, report or explicit object holder.
    pub fn take_owned(
        &mut self,
        id: ReservationId,
        kind: OwnedSlotKind,
    ) -> Result<OwnedPosition, CapacityError> {
        self.reservations
            .get_mut(&id)
            .ok_or(CapacityError::UnknownReservation)?
            .owned_slots
            .take(kind)
            .map_err(Into::into)
    }
    pub fn take_owned_span(
        &mut self,
        id: ReservationId,
        kind: OwnedSlotKind,
        count: usize,
    ) -> Result<Option<OwnedPositionSpan>, CapacityError> {
        self.reservations
            .get_mut(&id)
            .ok_or(CapacityError::UnknownReservation)?
            .owned_slots
            .take_span(kind, count)
            .map_err(Into::into)
    }
    pub fn take_owned_kind(
        &mut self,
        id: ReservationId,
        kind: OwnedSlotKind,
    ) -> Result<Option<OwnedPositionSpan>, CapacityError> {
        Ok(self
            .reservations
            .get_mut(&id)
            .ok_or(CapacityError::UnknownReservation)?
            .owned_slots
            .take_kind(kind))
    }
    /// Quota-only, storage-only and whole partitions are valid. The original
    /// public IDs and actual cells are moved, never cloned.
    pub fn split(
        &mut self,
        id: ReservationId,
        new_id: ReservationId,
        resources: &ReservationResources,
    ) -> Result<(), CapacityError> {
        if id == new_id || new_id.get() <= self.last_reservation {
            return Err(CapacityError::DuplicateReservation);
        }
        if resources.is_empty() {
            return Err(CapacityError::EmptyReservation);
        }
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let remaining = record.quota.checked_sub(&resources.quota)?;
        record.owned_slots.check_split(resources.owned_slots)?;
        let scope = record.scope;
        let generation = record.generation;
        let work = record.work;
        let record = self.reservations.get_mut(&id).expect("validated split");
        let owned = record
            .owned_slots
            .split_unused(new_id, resources.owned_slots)?;
        record.quota = remaining;
        self.last_reservation = new_id.get();
        self.reservations.insert(
            new_id,
            ReservationRecord {
                scope,
                generation,
                work,
                quota: resources.quota.clone(),
                owned_slots: owned,
                bound: true,
                owns_work: false,
            },
        );
        self.add_owner_record(scope);
        self.remove_empty_partition(id);
        Ok(())
    }
    /// Moves unused positions and quota only; a bound position stays in its
    /// actual Work/report/object. Transfer creates no Work or exit fact.
    pub fn transfer(
        &mut self,
        id: ReservationId,
        target: &ScopeState,
        work: WorkId,
    ) -> Result<(), CapacityError> {
        let holder = target.work(work).ok_or(ScopeError::UnknownWork)?;
        if holder.unbound_guard || holder.body_exited || holder.captures_exited {
            return Err(ScopeError::InvalidTransition.into());
        }
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        if record.owns_work && (record.scope != target.id() || record.work != work) {
            return Err(OwnedStorageError::BoundPosition.into());
        }
        let old_owner = record.scope;
        let quota = record.quota.clone();
        if old_owner != target.id() {
            self.check_owner_add(target.id(), &quota)?;
        }
        let record = self
            .reservations
            .get_mut(&id)
            .expect("validated original transfer");
        record.scope = target.id();
        record.generation = target.generation();
        record.work = work;
        if old_owner != target.id() {
            self.subtract_owner_usage(old_owner, &quota);
            self.add_owner_usage(target.id(), &quota);
            self.remove_owner_record(old_owner);
            self.add_owner_record(target.id());
        }
        Ok(())
    }
}
