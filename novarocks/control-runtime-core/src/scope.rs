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

//! Work lifetime is independent from protocol termination and report retirement.

use crate::{
    identity::{Generation, ScopeId, SlotId, WorkId},
    owned_storage::{OwnedPosition, OwnedPositionId, OwnedPositionSpan, OwnedSlotKind},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkKind {
    Business,
    Cleanup,
    Settlement,
    Step,
}
impl WorkKind {
    pub const fn allowed_after_stop(self) -> bool {
        matches!(self, Self::Cleanup | Self::Settlement)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkPhase {
    Admitted,
    Queued,
    Started,
    BodyExited,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkAdmission {
    External,
    Derived(WorkId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeError {
    Closed,
    Stopped,
    Failed,
    GenerationMismatch,
    DuplicateWork,
    UnknownWork,
    WorkCapacity,
    DuplicateSlot,
    UnknownSlot,
    InvalidTransition,
    ChildrenRemain,
    HeaderStillHeld,
    ExitUnobserved,
    IdentityExhausted,
}
#[derive(Debug, Eq, PartialEq)]
pub struct WorkRecord {
    header: OwnedPosition,
    owned_captures: Option<OwnedPositionSpan>,
    header_pins: usize,
    pub kind: WorkKind,
    pub phase: WorkPhase,
    pub parent: Option<WorkId>,
    pub children: usize,
    pub body_exited: bool,
    pub captures_exited: bool,
    pub unbound_guard: bool,
}

/// One actual holder of this original header. The pure state consumes the
/// token explicitly; dropping it cannot manufacture a completed input or exit.
#[derive(Debug)]
#[must_use = "Return the original header pin when its actual holder retires"]
pub struct WorkHeaderObligation {
    scope: ScopeId,
    work: WorkId,
    header: OwnedPositionId,
}
impl WorkHeaderObligation {
    pub const fn scope(&self) -> ScopeId {
        self.scope
    }
    pub const fn work(&self) -> WorkId {
        self.work
    }
    pub const fn header(&self) -> OwnedPositionId {
        self.header
    }
}
impl WorkRecord {
    pub const fn header_pin_count(&self) -> usize {
        self.header_pins
    }
    /// Read-only origin; it grants no new Work or publication permission.
    pub fn header_identity(&self) -> OwnedPositionId {
        self.header.id()
    }
}
#[derive(Debug)]
pub struct ScopeState {
    id: ScopeId,
    parent: Option<ScopeId>,
    generation: Generation,
    external_open: bool,
    stopped: bool,
    failed: bool,
    last_work: u64,
    last_slot: u64,
    work: BTreeMap<WorkId, WorkRecord>,
    slots: BTreeSet<SlotId>,
}
impl ScopeState {
    pub fn new(id: ScopeId, parent: Option<ScopeId>, generation: Generation) -> Self {
        Self {
            id,
            parent,
            generation,
            external_open: true,
            stopped: false,
            failed: false,
            last_work: 0,
            last_slot: 0,
            work: BTreeMap::new(),
            slots: BTreeSet::new(),
        }
    }
    pub const fn id(&self) -> ScopeId {
        self.id
    }
    pub const fn parent(&self) -> Option<ScopeId> {
        self.parent
    }
    pub const fn generation(&self) -> Generation {
        self.generation
    }
    pub const fn external_open(&self) -> bool {
        self.external_open
    }
    pub const fn stopped(&self) -> bool {
        self.stopped
    }
    pub const fn failed(&self) -> bool {
        self.failed
    }
    pub fn work_count(&self) -> usize {
        self.work.len()
    }
    pub fn storage_count(&self) -> usize {
        self.slots.len()
    }
    pub fn work(&self, id: WorkId) -> Option<&WorkRecord> {
        self.work.get(&id)
    }
    pub fn work_ids(&self) -> impl Iterator<Item = WorkId> + '_ {
        self.work.keys().copied()
    }
    /// A bounded observer follows only original records in a frozen ID range.
    /// This exposes no mutable Work or admission/retirement authority.
    pub fn observed_work_after(
        &self,
        after: Option<WorkId>,
        until: WorkId,
    ) -> Option<(WorkId, &WorkRecord)> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        self.work
            .range((after.map_or(Unbounded, Excluded), Included(until)))
            .next()
            .map(|(id, record)| (*id, record))
    }
    pub fn last_observed_work(&self) -> Option<WorkId> {
        self.work.last_key_value().map(|(id, _)| *id)
    }
    /// Allocated and admitted within the caller's same scope transaction.
    /// Generating IDs before taking that transaction can reorder concurrent
    /// admissions past the monotonic spent-identity watermark.
    pub fn next_work_id(&self) -> Result<WorkId, ScopeError> {
        self.last_work
            .checked_add(1)
            .and_then(WorkId::new)
            .ok_or(ScopeError::IdentityExhausted)
    }
    pub fn next_slot_id(&self) -> Result<SlotId, ScopeError> {
        self.last_slot
            .checked_add(1)
            .and_then(SlotId::new)
            .ok_or(ScopeError::IdentityExhausted)
    }
    pub fn check_admission(
        &self,
        generation: Generation,
        id: WorkId,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> Result<(), ScopeError> {
        if generation != self.generation {
            return Err(ScopeError::GenerationMismatch);
        }
        if id.get() <= self.last_work {
            return Err(ScopeError::DuplicateWork);
        }
        match admission {
            WorkAdmission::External => {
                if self.failed {
                    return Err(ScopeError::Failed);
                }
                if !self.external_open {
                    return Err(ScopeError::Closed);
                }
            }
            WorkAdmission::Derived(parent) => {
                let parent = self.work.get(&parent).ok_or(ScopeError::UnknownWork)?;
                if parent.body_exited || parent.captures_exited || parent.unbound_guard {
                    return Err(ScopeError::InvalidTransition);
                }
                if parent.children == usize::MAX {
                    return Err(ScopeError::WorkCapacity);
                }
            }
        }
        if self.stopped && !kind.allowed_after_stop() {
            return Err(ScopeError::Stopped);
        }
        Ok(())
    }
    pub fn admit(
        &mut self,
        generation: Generation,
        id: WorkId,
        kind: WorkKind,
        admission: WorkAdmission,
    ) -> Result<(), ScopeError> {
        self.admit_owned(
            generation,
            id,
            kind,
            admission,
            OwnedPosition::intrinsic_work(self.id, id),
        )
    }
    pub(crate) fn admit_owned(
        &mut self,
        generation: Generation,
        id: WorkId,
        kind: WorkKind,
        admission: WorkAdmission,
        header: OwnedPosition,
    ) -> Result<(), ScopeError> {
        self.check_admission(generation, id, kind, admission)?;
        if header.kind() != OwnedSlotKind::WorkHeader {
            return Err(ScopeError::InvalidTransition);
        }
        let parent = match admission {
            WorkAdmission::External => None,
            WorkAdmission::Derived(parent) => {
                self.work
                    .get_mut(&parent)
                    .expect("validated parent")
                    .children += 1;
                Some(parent)
            }
        };
        self.work.insert(
            id,
            WorkRecord {
                header,
                owned_captures: None,
                header_pins: 0,
                kind,
                parent,
                phase: WorkPhase::Admitted,
                children: 0,
                body_exited: false,
                captures_exited: false,
                unbound_guard: false,
            },
        );
        self.last_work = id.get();
        Ok(())
    }
    /// The guard is admitted with the final role policy before custody moves.
    pub fn admit_guard(
        &mut self,
        generation: Generation,
        id: WorkId,
        kind: WorkKind,
        admission: WorkAdmission,
        header: OwnedPosition,
    ) -> Result<(), ScopeError> {
        self.admit_owned(generation, id, kind, admission, header)?;
        self.work
            .get_mut(&id)
            .expect("newly admitted guard")
            .unbound_guard = true;
        Ok(())
    }
    /// A step guard is already counted. Binding transfers that same responsibility.
    pub fn bind_guard(&mut self, id: WorkId, kind: WorkKind) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if !work.unbound_guard || work.kind != kind || work.phase != WorkPhase::Admitted {
            return Err(ScopeError::InvalidTransition);
        }
        work.unbound_guard = false;
        work.phase = WorkPhase::Queued;
        Ok(())
    }
    pub fn queue(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if work.unbound_guard || work.phase != WorkPhase::Admitted {
            return Err(ScopeError::InvalidTransition);
        }
        work.phase = WorkPhase::Queued;
        Ok(())
    }
    /// Driver calls this under the same synchronization as stop/failure fencing.
    pub fn start(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if self.stopped && !work.kind.allowed_after_stop() {
            return Err(ScopeError::Stopped);
        }
        if work.unbound_guard
            || !matches!(work.phase, WorkPhase::Admitted | WorkPhase::Queued)
            || work.body_exited
        {
            return Err(ScopeError::InvalidTransition);
        }
        work.phase = WorkPhase::Started;
        Ok(())
    }
    /// Also observes a canceled queue item that never invoked its body.
    pub fn body_exited(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if work.body_exited {
            return Err(ScopeError::InvalidTransition);
        }
        work.body_exited = true;
        work.phase = WorkPhase::BodyExited;
        Ok(())
    }
    /// Only call after actual capture destruction, never merely after cancellation.
    pub fn bind_captures(
        &mut self,
        id: WorkId,
        captures: OwnedPositionSpan,
    ) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if captures.kind() != OwnedSlotKind::Capture
            || work.unbound_guard
            || work.body_exited
            || work.captures_exited
            || work.owned_captures.is_some()
        {
            return Err(ScopeError::InvalidTransition);
        }
        work.owned_captures = Some(captures);
        Ok(())
    }
    pub fn capture_positions(&self, id: WorkId) -> Result<usize, ScopeError> {
        Ok(self
            .work
            .get(&id)
            .ok_or(ScopeError::UnknownWork)?
            .owned_captures
            .as_ref()
            .map_or(0, OwnedPositionSpan::len))
    }
    pub fn captures_bound(&self, id: WorkId) -> Result<bool, ScopeError> {
        Ok(self
            .work
            .get(&id)
            .ok_or(ScopeError::UnknownWork)?
            .owned_captures
            .is_some())
    }
    pub fn captures_exited(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if work.captures_exited {
            return Err(ScopeError::InvalidTransition);
        }
        work.captures_exited = true;
        work.owned_captures.take();
        Ok(())
    }
    pub fn work_exited(&self, id: WorkId) -> Result<bool, ScopeError> {
        let work = self.work.get(&id).ok_or(ScopeError::UnknownWork)?;
        Ok(work.body_exited && work.captures_exited && work.children == 0)
    }
    /// Header custody is independent of the physical body/capture observation.
    pub fn work_header_retirement_ready(&self, id: WorkId) -> Result<bool, ScopeError> {
        Ok(self.work_exited(id)? && self.work[&id].header_pins == 0)
    }
    /// Called by the actual Work or still-owned guard holder, never an identity alone.
    pub fn pin_work_header(&mut self, id: WorkId) -> Result<WorkHeaderObligation, ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        work.header_pins = work
            .header_pins
            .checked_add(1)
            .ok_or(ScopeError::IdentityExhausted)?;
        Ok(WorkHeaderObligation {
            scope: self.id,
            work: id,
            header: work.header_identity(),
        })
    }
    pub fn retain_work_header(
        &mut self,
        pin: &WorkHeaderObligation,
    ) -> Result<WorkHeaderObligation, ScopeError> {
        if pin.scope != self.id
            || self
                .work
                .get(&pin.work)
                .is_none_or(|work| work.header_identity() != pin.header || work.header_pins == 0)
        {
            return Err(ScopeError::InvalidTransition);
        }
        self.pin_work_header(pin.work)
    }
    pub fn release_work_header(
        &mut self,
        pin: WorkHeaderObligation,
    ) -> Result<bool, (ScopeError, WorkHeaderObligation)> {
        if pin.scope != self.id
            || self
                .work
                .get(&pin.work)
                .is_none_or(|work| work.header_identity() != pin.header || work.header_pins == 0)
        {
            return Err((ScopeError::InvalidTransition, pin));
        }
        self.work
            .get_mut(&pin.work)
            .expect("original pinned header")
            .header_pins -= 1;
        Ok(self
            .work_header_retirement_ready(pin.work)
            .expect("pin retains its original Work"))
    }
    /// Retires one record. A ready parent is returned for a later bounded turn.
    pub fn retire_work(&mut self, id: WorkId) -> Result<Option<WorkId>, ScopeError> {
        let work = self.work.get(&id).ok_or(ScopeError::UnknownWork)?;
        if work.children != 0 {
            return Err(ScopeError::ChildrenRemain);
        }
        if !work.body_exited || !work.captures_exited {
            return Err(ScopeError::ExitUnobserved);
        }
        if work.header_pins != 0 {
            return Err(ScopeError::HeaderStillHeld);
        }
        let parent = work.parent;
        self.work.remove(&id);
        if let Some(parent) = parent {
            let record = self
                .work
                .get_mut(&parent)
                .expect("live child retains its parent");
            record.children = record
                .children
                .checked_sub(1)
                .expect("child ownership underflow");
            if record.body_exited
                && record.captures_exited
                && record.children == 0
                && record.header_pins == 0
            {
                return Ok(Some(parent));
            }
        }
        Ok(None)
    }
    /// Only pure, never-bound step reservations have no body or capture to await.
    pub fn rollback_guard(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let record = self.work.get(&id).ok_or(ScopeError::UnknownWork)?;
        if !record.unbound_guard || record.phase != WorkPhase::Admitted || record.children != 0 {
            return Err(ScopeError::InvalidTransition);
        }
        if record.header_pins != 0 {
            // The original never-bound holder really withdrew. No async body
            // or capture existed; only a resident batch still owns the header.
            // Its last pin, rather than a fabricated active Work, retires it.
            let record = self.work.get_mut(&id).expect("same unused original guard");
            record.body_exited = true;
            record.captures_exited = true;
            record.phase = WorkPhase::BodyExited;
            return Ok(());
        }
        let parent = record.parent;
        self.work.remove(&id);
        if let Some(parent) = parent {
            let p = self
                .work
                .get_mut(&parent)
                .expect("live child retains its parent");
            p.children = p
                .children
                .checked_sub(1)
                .expect("child ownership underflow");
        }
        Ok(())
    }
    pub fn reserve_slot(&mut self, id: SlotId) -> Result<(), ScopeError> {
        if id.get() <= self.last_slot {
            return Err(ScopeError::DuplicateSlot);
        }
        self.slots.insert(id);
        self.last_slot = id.get();
        Ok(())
    }
    pub fn retire_slot(&mut self, id: SlotId) -> Result<(), ScopeError> {
        if !self.slots.remove(&id) {
            return Err(ScopeError::UnknownSlot);
        }
        Ok(())
    }
    pub fn close_external(&mut self) {
        self.external_open = false;
    }
    pub fn request_stop(&mut self) {
        self.external_open = false;
        self.stopped = true;
    }
    pub fn fail(&mut self) -> Result<Generation, ScopeError> {
        if self.failed {
            return Ok(self.generation);
        }
        let generation = self
            .generation
            .next()
            .ok_or(ScopeError::IdentityExhausted)?;
        self.external_open = false;
        self.stopped = true;
        self.failed = true;
        self.generation = generation;
        Ok(generation)
    }
    pub fn join_ready(&self) -> bool {
        !self.external_open && self.work.is_empty()
    }
    pub fn retirement_ready(&self) -> bool {
        self.join_ready() && self.slots.is_empty()
    }
}
