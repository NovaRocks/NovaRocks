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

use crate::identity::{Generation, ScopeId, SlotId, WorkId};
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
    SlotCapacity,
    DuplicateSlot,
    UnknownSlot,
    InvalidTransition,
    ChildrenRemain,
    ExitUnobserved,
    IdentityExhausted,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkRecord {
    pub kind: WorkKind,
    pub phase: WorkPhase,
    pub parent: Option<WorkId>,
    pub children: usize,
    pub body_exited: bool,
    pub captures_exited: bool,
    pub unbound_guard: bool,
}

#[derive(Debug)]
pub struct ScopeState {
    id: ScopeId,
    parent: Option<ScopeId>,
    generation: Generation,
    external_open: bool,
    stopped: bool,
    failed: bool,
    max_work: usize,
    max_slots: usize,
    last_work: u64,
    last_slot: u64,
    work: BTreeMap<WorkId, WorkRecord>,
    slots: BTreeSet<SlotId>,
}
impl ScopeState {
    pub fn new(
        id: ScopeId,
        parent: Option<ScopeId>,
        generation: Generation,
        max_work: usize,
        max_slots: usize,
    ) -> Self {
        Self {
            id,
            parent,
            generation,
            external_open: true,
            stopped: false,
            failed: false,
            max_work,
            max_slots,
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
        if self.work.len() >= self.max_work {
            return Err(ScopeError::WorkCapacity);
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
        self.check_admission(generation, id, kind, admission)?;
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
    ) -> Result<(), ScopeError> {
        self.admit(generation, id, kind, admission)?;
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
    pub fn captures_exited(&mut self, id: WorkId) -> Result<(), ScopeError> {
        let work = self.work.get_mut(&id).ok_or(ScopeError::UnknownWork)?;
        if work.captures_exited {
            return Err(ScopeError::InvalidTransition);
        }
        work.captures_exited = true;
        Ok(())
    }
    pub fn work_exited(&self, id: WorkId) -> Result<bool, ScopeError> {
        let work = self.work.get(&id).ok_or(ScopeError::UnknownWork)?;
        Ok(work.body_exited && work.captures_exited && work.children == 0)
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
            if record.body_exited && record.captures_exited && record.children == 0 {
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
        if self.slots.len() >= self.max_slots {
            return Err(ScopeError::SlotCapacity);
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
