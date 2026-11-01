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

//! Linear object-local storage. Descriptions grant no positions or Work.

use crate::identity::{ReservationId, ScopeId, WorkId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum OwnedSlotKind {
    Input,
    Batch,
    Effect,
    Cut,
    Reply,
    Completion,
    Capture,
    WorkHeader,
}
impl OwnedSlotKind {
    pub const ALL: [Self; 8] = [
        Self::Input,
        Self::Batch,
        Self::Effect,
        Self::Cut,
        Self::Reply,
        Self::Completion,
        Self::Capture,
        Self::WorkHeader,
    ];
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OwnedSlotNeed {
    pub inputs: usize,
    pub batches: usize,
    pub effects: usize,
    pub cuts: usize,
    pub replies: usize,
    pub completions: usize,
    pub captures: usize,
    pub work_headers: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnedStorageError {
    Overflow,
    MissingPosition(OwnedSlotKind),
    WrongKind,
    WrongOwner,
    BoundPosition,
}
impl OwnedSlotNeed {
    pub const fn work_only() -> Self {
        Self {
            work_headers: 1,
            inputs: 0,
            batches: 0,
            effects: 0,
            cuts: 0,
            replies: 0,
            completions: 0,
            captures: 0,
        }
    }
    pub const fn get(self, kind: OwnedSlotKind) -> usize {
        match kind {
            OwnedSlotKind::Input => self.inputs,
            OwnedSlotKind::Batch => self.batches,
            OwnedSlotKind::Effect => self.effects,
            OwnedSlotKind::Cut => self.cuts,
            OwnedSlotKind::Reply => self.replies,
            OwnedSlotKind::Completion => self.completions,
            OwnedSlotKind::Capture => self.captures,
            OwnedSlotKind::WorkHeader => self.work_headers,
        }
    }
    const fn counts(self) -> [usize; 8] {
        [
            self.inputs,
            self.batches,
            self.effects,
            self.cuts,
            self.replies,
            self.completions,
            self.captures,
            self.work_headers,
        ]
    }
    fn from_counts(n: [usize; 8]) -> Self {
        Self {
            inputs: n[0],
            batches: n[1],
            effects: n[2],
            cuts: n[3],
            replies: n[4],
            completions: n[5],
            captures: n[6],
            work_headers: n[7],
        }
    }
    pub fn is_empty(self) -> bool {
        self.counts().into_iter().all(|n| n == 0)
    }
    pub fn checked_add(self, other: Self) -> Result<Self, OwnedStorageError> {
        let mut n = [0; 8];
        for kind in OwnedSlotKind::ALL {
            n[kind as usize] = self
                .get(kind)
                .checked_add(other.get(kind))
                .ok_or(OwnedStorageError::Overflow)?;
        }
        Ok(Self::from_counts(n))
    }
    pub fn checked_sub(self, other: Self) -> Result<Self, OwnedStorageError> {
        let mut n = [0; 8];
        for kind in OwnedSlotKind::ALL {
            n[kind as usize] = self
                .get(kind)
                .checked_sub(other.get(kind))
                .ok_or(OwnedStorageError::MissingPosition(kind))?;
        }
        Ok(Self::from_counts(n))
    }
}
/// A read-only origin. Its public numbers never authorize a runtime operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedPositionId {
    allocation: Option<ReservationId>,
    scope: Option<ScopeId>,
    ordinal_or_work: u64,
    kind: OwnedSlotKind,
}
impl OwnedPositionId {
    pub fn allocation(self) -> Option<ReservationId> {
        self.allocation
    }
    pub fn scope(self) -> Option<ScopeId> {
        self.scope
    }
    pub fn ordinal_or_work(self) -> u64 {
        self.ordinal_or_work
    }
    pub fn kind(self) -> OwnedSlotKind {
        self.kind
    }
}
/// One actual position. Binding moves this value into its original object.
/// There is deliberately no Clone, Copy, or public identity constructor.
#[derive(Debug, Eq, PartialEq)]
pub struct OwnedPosition {
    id: OwnedPositionId,
    owner: Option<ReservationId>,
}
impl OwnedPosition {
    pub fn id(&self) -> OwnedPositionId {
        self.id
    }
    pub fn kind(&self) -> OwnedSlotKind {
        self.id.kind
    }
    pub fn reservation(&self) -> Option<ReservationId> {
        self.owner
    }
    pub(crate) fn intrinsic_work(scope: ScopeId, work: WorkId) -> Self {
        Self {
            id: OwnedPositionId {
                allocation: None,
                scope: Some(scope),
                ordinal_or_work: work.get(),
                kind: OwnedSlotKind::WorkHeader,
            },
            owner: None,
        }
    }
}
/// A moved contiguous group owned by one actual object, without allocating
/// one abstract cell for every possible effect or capture.
#[derive(Debug, Eq, PartialEq)]
pub struct OwnedPositionSpan {
    allocation: ReservationId,
    owner: ReservationId,
    kind: OwnedSlotKind,
    start: usize,
    len: usize,
}
impl OwnedPositionSpan {
    pub fn allocation(&self) -> ReservationId {
        self.allocation
    }
    pub fn reservation(&self) -> ReservationId {
        self.owner
    }
    pub fn start(&self) -> usize {
        self.start
    }
    pub fn kind(&self) -> OwnedSlotKind {
        self.kind
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
#[derive(Debug, Default, Eq, PartialEq)]
struct Range {
    start: usize,
    len: usize,
}
/// Eight disjoint linear ranges, independent of their declared slot counts.
/// Splits move subranges; only actual bound objects materialize a position.
/// No arena or shared allocation keeps the parent object's storage alive.
#[derive(Debug)]
pub struct OwnedStorageInventory {
    allocation: ReservationId,
    owner: ReservationId,
    free: [Range; 8],
}
impl OwnedStorageInventory {
    pub fn allocate(
        allocation: ReservationId,
        need: OwnedSlotNeed,
    ) -> Result<Self, OwnedStorageError> {
        for kind in OwnedSlotKind::ALL {
            u64::try_from(need.get(kind)).map_err(|_| OwnedStorageError::Overflow)?;
        }
        Ok(Self {
            allocation,
            owner: allocation,
            free: std::array::from_fn(|i| Range {
                start: 0,
                len: need.get(OwnedSlotKind::ALL[i]),
            }),
        })
    }
    pub fn remaining(&self) -> OwnedSlotNeed {
        OwnedSlotNeed::from_counts(std::array::from_fn(|i| self.free[i].len))
    }
    pub fn check_split(&self, need: OwnedSlotNeed) -> Result<(), OwnedStorageError> {
        self.remaining().checked_sub(need).map(|_| ())
    }
    pub fn split_unused(
        &mut self,
        new_owner: ReservationId,
        need: OwnedSlotNeed,
    ) -> Result<Self, OwnedStorageError> {
        self.check_split(need)?;
        let free = std::array::from_fn(|i| {
            let n = need.get(OwnedSlotKind::ALL[i]);
            let remaining = self.free[i].len - n;
            let start = self.free[i]
                .start
                .checked_add(remaining)
                .expect("subrange remains inside original allocation");
            self.free[i].len = remaining;
            Range { start, len: n }
        });
        Ok(Self {
            allocation: self.allocation,
            owner: new_owner,
            free,
        })
    }
    /// Move exactly this original contiguous span. Every ordinary refusal
    /// precedes mutation; zero creates no position or binding fact.
    pub fn take_span(
        &mut self,
        kind: OwnedSlotKind,
        count: usize,
    ) -> Result<Option<OwnedPositionSpan>, OwnedStorageError> {
        let range = &mut self.free[kind as usize];
        if count > range.len {
            return Err(OwnedStorageError::MissingPosition(kind));
        }
        if count == 0 {
            return Ok(None);
        }
        let remaining = range.len - count;
        let start = range
            .start
            .checked_add(remaining)
            .ok_or(OwnedStorageError::Overflow)?;
        range.len = remaining;
        Ok(Some(OwnedPositionSpan {
            allocation: self.allocation,
            owner: self.owner,
            kind,
            start,
            len: count,
        }))
    }
    pub fn take_kind(&mut self, kind: OwnedSlotKind) -> Option<OwnedPositionSpan> {
        let range = &mut self.free[kind as usize];
        if range.len == 0 {
            return None;
        }
        let len = std::mem::take(&mut range.len);
        Some(OwnedPositionSpan {
            allocation: self.allocation,
            owner: self.owner,
            kind,
            start: range.start,
            len,
        })
    }
    pub fn take(&mut self, kind: OwnedSlotKind) -> Result<OwnedPosition, OwnedStorageError> {
        let range = &mut self.free[kind as usize];
        if range.len == 0 {
            return Err(OwnedStorageError::MissingPosition(kind));
        }
        let ordinal = range
            .start
            .checked_add(range.len - 1)
            .ok_or(OwnedStorageError::Overflow)?;
        let ordinal = u64::try_from(ordinal).map_err(|_| OwnedStorageError::Overflow)?;
        range.len -= 1;
        Ok(OwnedPosition {
            id: OwnedPositionId {
                allocation: Some(self.allocation),
                scope: None,
                ordinal_or_work: ordinal,
                kind,
            },
            owner: Some(self.owner),
        })
    }
}
