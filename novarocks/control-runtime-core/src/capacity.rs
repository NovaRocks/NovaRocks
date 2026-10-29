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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReservationRecord {
    pub scope: ScopeId,
    pub generation: Generation,
    pub work: WorkId,
    pub resources: ResourceVector,
    pub bound: bool,
}
#[derive(Clone, Debug)]
pub struct ReservationRequest {
    pub id: ReservationId,
    pub generation: Generation,
    pub guard: WorkId,
    pub resources: ResourceVector,
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
    max_owners: usize,
}
impl OwnerUsageIndex {
    fn check_add(
        &self,
        scope: ScopeId,
        amount: u64,
        replacing: Option<ScopeId>,
    ) -> Result<(), CapacityError> {
        if amount == 0 {
            return Ok(());
        }
        self.by_scope
            .get(&scope)
            .copied()
            .unwrap_or(0)
            .checked_add(amount)
            .ok_or(CapacityError::Overflow)?;
        if !self.by_scope.contains_key(&scope)
            && self.by_scope.len() == self.max_owners
            && replacing.is_none_or(|old| self.by_scope.get(&old).copied() != Some(amount))
        {
            return Err(CapacityError::Scope(ScopeError::SlotCapacity));
        }
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
            *count = count.checked_add(1).expect("bounded owner usage histogram");
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
    max_reservations: usize,
    last_reservation: u64,
    healthy: bool,
}
impl CapacityLedger {
    pub fn new(limits: ResourceVector, max_reservations: usize) -> Self {
        Self {
            limits,
            used: ResourceVector::default(),
            reservations: BTreeMap::new(),
            owner_counts: BTreeMap::new(),
            owner_usage: BTreeMap::new(),
            max_reservations,
            last_reservation: 0,
            healthy: true,
        }
    }
    /// Configure only dimensions needed by an actual owner read port. With
    /// integer permits, each nonzero scope owns at least one global unit.
    /// The explicit scope bound covers all runtime scopes, not just Contexts.
    pub fn with_owner_usage_dimensions(
        limits: ResourceVector,
        max_reservations: usize,
        max_scopes: usize,
        dimensions: impl IntoIterator<Item = DimensionId>,
    ) -> Result<Self, CapacityError> {
        let mut ledger = Self::new(limits, max_reservations);
        for dimension in dimensions {
            let limit = *ledger
                .limits
                .0
                .get(&dimension)
                .ok_or(CapacityError::UnknownDimension)?;
            let max_owners =
                usize::try_from(limit).map_or(max_scopes, |limit| max_scopes.min(limit));
            max_owners
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(CapacityError::Overflow)?;
            if ledger
                .owner_usage
                .insert(
                    dimension,
                    OwnerUsageIndex {
                        by_scope: BTreeMap::new(),
                        histogram: BTreeMap::new(),
                        max_owners,
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
    /// Bounded metadata entries: one dimension row plus at most one owner and
    /// one histogram row per nonzero scope. These are not Work/reservation IDs.
    pub fn owner_usage_metadata_entries(&self, dimension: DimensionId) -> Option<usize> {
        self.owner_usage.get(&dimension).map(|index| {
            index
                .max_owners
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .expect("validated owner index geometry")
        })
    }
    fn check_owner_add(
        &self,
        scope: ScopeId,
        resources: &ResourceVector,
        replacing: Option<ScopeId>,
    ) -> Result<(), CapacityError> {
        for (&dimension, index) in &self.owner_usage {
            index.check_add(scope, resources.get(dimension), replacing)?;
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
    /// No mutation occurs until every scope gate, dimension and storage check succeeds.
    pub fn try_reserve_bundle(
        &mut self,
        scope: &mut ScopeState,
        request: ReservationRequest,
    ) -> Result<ReservationId, CapacityError> {
        if !self.healthy {
            return Err(CapacityError::Scope(ScopeError::Failed));
        }
        if request.id.get() <= self.last_reservation {
            return Err(CapacityError::DuplicateReservation);
        }
        if self.reservations.len() >= self.max_reservations {
            return Err(CapacityError::Scope(ScopeError::SlotCapacity));
        }
        scope.check_admission(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
        )?;
        for (dimension, amount) in request.resources.iter() {
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
        let next = self.used.checked_add(&request.resources)?;
        self.check_owner_add(scope.id(), &request.resources, None)?;
        scope.admit_guard(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
        )?;
        self.used = next;
        self.add_owner_usage(scope.id(), &request.resources);
        self.last_reservation = request.id.get();
        self.reservations.insert(
            request.id,
            ReservationRecord {
                scope: scope.id(),
                generation: request.generation,
                work: request.guard,
                resources: request.resources,
                bound: false,
            },
        );
        self.add_owner_record(scope.id());
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
    /// A queued payload has a real holder and cannot take this rollback path.
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
        let next = self.used.checked_sub(&record.resources)?;
        let resources = record.resources.clone();
        scope.rollback_guard(record.work)?;
        self.used = next;
        self.subtract_owner_usage(scope.id(), &resources);
        self.reservations.remove(&id);
        self.remove_owner_record(scope.id());
        Ok(())
    }
    /// Release is an observed holder fact. Scope work retirement remains separate.
    pub fn release(
        &mut self,
        id: ReservationId,
        resources: &ResourceVector,
    ) -> Result<(), CapacityError> {
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let owner = record.scope;
        let remaining = record.resources.checked_sub(resources)?;
        let next = self.used.checked_sub(resources)?;
        self.used = next;
        self.subtract_owner_usage(owner, resources);
        if remaining.is_empty() {
            self.reservations.remove(&id);
            self.remove_owner_record(owner);
        } else {
            self.reservations
                .get_mut(&id)
                .expect("validated reservation")
                .resources = remaining;
        }
        Ok(())
    }
    /// Splits already held units, without acquiring capacity or duplicating work.
    pub fn split(
        &mut self,
        id: ReservationId,
        new_id: ReservationId,
        resources: &ResourceVector,
    ) -> Result<(), CapacityError> {
        if id == new_id || new_id.get() <= self.last_reservation {
            return Err(CapacityError::DuplicateReservation);
        }
        if self.reservations.len() >= self.max_reservations {
            return Err(CapacityError::Scope(ScopeError::SlotCapacity));
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
        let remaining = record.resources.checked_sub(resources)?;
        if remaining.is_empty() {
            return Err(CapacityError::InvalidSplit);
        }
        let mut split = record.clone();
        split.resources = resources.clone();
        self.reservations
            .get_mut(&id)
            .expect("validated reservation")
            .resources = remaining;
        self.last_reservation = new_id.get();
        let owner = split.scope;
        self.reservations.insert(new_id, split);
        self.add_owner_record(owner);
        Ok(())
    }
    /// The receiving work must already be admitted; transfer never issues a new lease.
    pub fn transfer(
        &mut self,
        id: ReservationId,
        target: &ScopeState,
        work: WorkId,
    ) -> Result<(), CapacityError> {
        if target.work(work).is_none() {
            return Err(CapacityError::Scope(ScopeError::UnknownWork));
        }
        let record = self
            .reservations
            .get(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let old_owner = record.scope;
        let resources = record.resources.clone();
        if old_owner != target.id() {
            self.check_owner_add(target.id(), &resources, Some(old_owner))?;
        }
        let record = self
            .reservations
            .get_mut(&id)
            .expect("validated original transfer");
        record.scope = target.id();
        record.generation = target.generation();
        record.work = work;
        if old_owner != target.id() {
            self.subtract_owner_usage(old_owner, &resources);
            self.add_owner_usage(target.id(), &resources);
            self.remove_owner_record(old_owner);
            self.add_owner_record(target.id());
        }
        Ok(())
    }
}
