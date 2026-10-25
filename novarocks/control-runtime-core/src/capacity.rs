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
#[derive(Debug)]
pub struct CapacityLedger {
    limits: ResourceVector,
    used: ResourceVector,
    reservations: BTreeMap<ReservationId, ReservationRecord>,
    owner_counts: BTreeMap<ScopeId, usize>,
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
            max_reservations,
            last_reservation: 0,
            healthy: true,
        }
    }
    pub fn used(&self, dimension: DimensionId) -> u64 {
        self.used.get(dimension)
    }
    pub fn limit(&self, dimension: DimensionId) -> u64 {
        self.limits.get(dimension)
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
        scope.admit_guard(
            request.generation,
            request.guard,
            request.kind,
            request.admission,
        )?;
        self.used = next;
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
        scope.rollback_guard(record.work)?;
        self.used = next;
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
            .get_mut(&id)
            .ok_or(CapacityError::UnknownReservation)?;
        if !record.bound {
            return Err(CapacityError::UnboundReservation);
        }
        let old_owner = record.scope;
        record.scope = target.id();
        record.generation = target.generation();
        record.work = work;
        if old_owner != target.id() {
            self.remove_owner_record(old_owner);
            self.add_owner_record(target.id());
        }
        Ok(())
    }
}
