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

//! The original State's backing inventory, not another Work registry.
//! This storage envelope does not certify an application's complete role or
//! I/O profile. Application composition owns those additional contracts.

use crate::{
    WorkClass, WorkError, WorkId,
    cancellation::{Cancellation, CancellationReset},
    joint_admission::{JointResource, JointResources},
};
use std::{
    alloc::Layout,
    mem::size_of,
    sync::{Arc, atomic::AtomicUsize},
};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlotId(pub(crate) usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlotPhase {
    /// Only the migration's nonprepaid, on-demand mode has vacant entries.
    Vacant,
    Free,
    Node(WorkId),
    /// Exact resident backing selected by one paid constructor. This grants
    /// neither a Work nor the parent's domain-bound control capacity.
    ChildReserved {
        parent: WorkId,
    },
    /// The original queued proof owns its reserved resident backing.
    ChildQueued {
        parent: WorkId,
        ticket: u64,
    },
    /// A real committed reply, not a published Work or a free slot.
    ChildGranted {
        parent: WorkId,
        ticket: u64,
    },
    Retired,
}

pub(crate) struct ControlSlot {
    pub(crate) retired_root_domains:
        Option<crate::joint_admission::ledger::root_domains::RootDomains>,
    pub(crate) cell: Option<Arc<Cancellation>>,
    pub(crate) polls: Vec<crate::role_poll::PollBacking>,
    pub(crate) class: WorkClass,
    pub(crate) phase: SlotPhase,
    pub(crate) generation: u64,
    pub(crate) claim: Option<u64>,
    pub(crate) base: JointResources,
    pub(crate) parent: Option<SlotId>,
    pub(crate) next_free: Option<SlotId>,
}

#[derive(Default)]
pub(crate) struct CancellationStorage {
    pub(crate) slots: Vec<ControlSlot>,
    pub(crate) fixed: bool,
    pub(crate) retired: usize,
    pub(crate) retired_polls: usize,
    pub(crate) free_head: Option<SlotId>,
    pub(crate) legacy_vacant: Option<SlotId>,
}

/// Fixed local containers in the original workload authority.
///
/// This quotes storage only. Application composition must separately validate
/// its required roles, payloads, dependencies, physical adapters and services.
/// Installing this storage does not establish application readiness.
#[derive(Clone, Copy)]
pub struct ControlStorageConfig<'a> {
    pub(crate) scope_records: usize,
    pub(crate) poll_roles: &'a [crate::role_poll::PollRoleGeometry],
    pub(crate) process_sources: crate::PollRoleSourceBacking,
}

impl<'a> ControlStorageConfig<'a> {
    /// One original global scope inventory. WorkClass is assigned on actual
    /// installation and does not partition or reserve this inventory.
    pub fn new(scope_records: usize, poll_roles: &'a [crate::PollRoleGeometry]) -> Self {
        Self {
            scope_records,
            poll_roles,
            process_sources: crate::PollRoleSourceBacking::default(),
        }
    }

    /// Independent process-owned source headers sharing this SAME prepaid
    /// envelope. This is a storage fact, not a new role or admission authority.
    pub fn with_process_source_backing(mut self, source: crate::PollRoleSourceBacking) -> Self {
        self.process_sources = source;
        self
    }

    /// Original fixed container storage only. No request inventory is reserved.
    pub fn base_charge(self) -> Result<JointResources, WorkError> {
        crate::joint_admission::ledger::control_storage_base(self)
    }

    pub(crate) fn count(self) -> Result<usize, WorkError> {
        Ok(self.scope_records)
    }
}

impl CancellationStorage {
    pub(crate) fn cell(&self, id: SlotId) -> &Arc<Cancellation> {
        self.slots[id.0]
            .cell
            .as_ref()
            .expect("live cancellation backing")
    }

    pub(crate) fn drained(&self) -> bool {
        self.free_inventory_valid()
            && self.retired_polls == 0
            && self.slots.iter().all(|slot| {
                slot.polls
                    .iter()
                    .all(|role| role.phase == crate::role_poll::PollPhase::Free)
                    && slot.phase
                        == if self.fixed {
                            SlotPhase::Free
                        } else {
                            SlotPhase::Vacant
                        }
            })
    }

    /// Validate every reachable free entry with a total visit budget. One
    /// same-class head plus unique next links makes a repeated entry a cycle;
    /// distinct class chains cannot share a correctly classified entry. Thus
    /// count equality proves full coverage without a bitmap or allocation.
    pub(crate) fn free_inventory_valid(&self) -> bool {
        let mut expected = 0usize;
        let mut retired = 0usize;
        let mut retired_polls = 0usize;
        for slot in &self.slots {
            if self.fixed {
                retired_polls += slot
                    .polls
                    .iter()
                    .filter(|role| role.phase == crate::role_poll::PollPhase::Retired)
                    .count();
                if slot.phase == SlotPhase::Vacant || slot.cell.is_none() || slot.claim.is_none() {
                    return false;
                }
                if slot.phase == SlotPhase::Free {
                    if slot
                        .polls
                        .iter()
                        .any(|role| role.phase != crate::role_poll::PollPhase::Free)
                    {
                        return false;
                    }
                    if slot.parent.is_some() {
                        return false;
                    }
                    expected += 1;
                }
                if slot.phase == SlotPhase::Retired {
                    retired += 1;
                }
                if slot.phase != SlotPhase::Free && slot.next_free.is_some() {
                    return false;
                }
            } else {
                match slot.phase {
                    SlotPhase::Vacant => {
                        if slot.cell.is_some() || slot.claim.is_some() || slot.parent.is_some() {
                            return false;
                        }
                        expected += 1;
                    }
                    SlotPhase::Node(_) => {
                        if slot.cell.is_none() || slot.claim.is_some() || slot.next_free.is_some() {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
        }
        if retired != self.retired || retired_polls != self.retired_polls {
            return false;
        }
        if self.fixed {
            if self.legacy_vacant.is_some() {
                return false;
            }
        } else if self.free_head.is_some() {
            return false;
        }
        let mut visited = 0usize;
        if self.fixed {
            let mut next = self.free_head;
            while let Some(id) = next {
                if visited >= self.slots.len() {
                    return false;
                }
                let Some(slot) = self.slots.get(id.0) else {
                    return false;
                };
                if slot.phase != SlotPhase::Free {
                    return false;
                }
                visited += 1;
                next = slot.next_free;
            }
        } else {
            let mut next = self.legacy_vacant;
            while let Some(id) = next {
                if visited >= self.slots.len() {
                    return false;
                }
                let Some(slot) = self.slots.get(id.0) else {
                    return false;
                };
                if slot.phase != SlotPhase::Vacant {
                    return false;
                }
                visited += 1;
                next = slot.next_free;
            }
        }
        visited == expected
    }

    fn insert_legacy(
        &mut self,
        cell: Arc<Cancellation>,
        class: WorkClass,
        id: WorkId,
        parent: Option<SlotId>,
    ) -> Result<SlotId, WorkError> {
        if let Some(free) = self.legacy_vacant {
            let slot = &mut self.slots[free.0];
            assert_eq!(slot.phase, SlotPhase::Vacant);
            let generation = slot
                .generation
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            // Every fallible preflight above leaves the free chain untouched.
            self.legacy_vacant = slot.next_free.take();
            slot.cell = Some(cell);
            slot.class = class;
            slot.phase = SlotPhase::Node(id);
            slot.generation = generation;
            slot.parent = parent;
            return Ok(free);
        }
        self.slots.push(ControlSlot {
            retired_root_domains: None,
            cell: Some(cell),
            polls: Vec::new(),
            class,
            phase: SlotPhase::Node(id),
            generation: 1,
            claim: None,
            base: JointResources::ZERO,
            parent,
            next_free: None,
        });
        Ok(SlotId(self.slots.len() - 1))
    }

    pub(crate) fn take_root(
        &mut self,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        if !self.fixed {
            let cell = Cancellation::root(deadline);
            if let Some(reason) = cell.check_reason() {
                return Err(WorkError::Cancelled(reason));
            }
            return self.insert_legacy(cell, class, id, None);
        }
        let free = self
            .free_head
            .ok_or(WorkError::Capacity("fixed control slots"))?;
        let slot = &mut self.slots[free.0];
        assert_eq!(slot.phase, SlotPhase::Free);
        let generation = slot
            .generation
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let cell = Arc::get_mut(slot.cell.as_mut().unwrap()).ok_or(WorkError::Conflict)?;
        cell.reuse_root(deadline);
        if let Some(reason) = cell.check_reason() {
            cell.reuse_root(None);
            return Err(WorkError::Cancelled(reason));
        }
        self.free_head = slot.next_free.take();
        slot.generation = generation;
        slot.class = class;
        slot.parent = None;
        slot.phase = SlotPhase::Node(id);
        Ok(free)
    }

    /// Migration convenience for same-key empty-D children only. Bound parents
    /// enter through the original proof and install_granted_child author.
    pub(crate) fn take_child(
        &mut self,
        parent: SlotId,
        class: WorkClass,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<SlotId, WorkError> {
        if !self.fixed {
            let cell = self.cell(parent).child_with_sequence(deadline, id);
            if let Some(reason) = cell.check_reason() {
                cell.detach();
                return Err(WorkError::Cancelled(reason));
            }
            let result = self.insert_legacy(Arc::clone(&cell), class, id, Some(parent));
            if result.is_err() {
                cell.detach();
            }
            return result;
        }
        let free = self
            .free_head
            .ok_or(WorkError::Capacity("fixed control slots"))?;
        let index = free.0;
        assert_eq!(self.slots[index].phase, SlotPhase::Free);
        let generation = self.slots[index]
            .generation
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let parent_cell = Arc::clone(self.cell(parent));
        Cancellation::reuse_child(
            self.slots[index].cell.as_mut().unwrap(),
            &parent_cell,
            deadline,
            id,
        )?;
        // A failed capacity/uniqueness preflight must not consume the head.
        self.free_head = self.slots[index].next_free.take();
        self.slots[index].generation = generation;
        self.slots[index].class = class;
        self.slots[index].parent = Some(parent);
        self.slots[index].phase = SlotPhase::Node(id);
        let slot = SlotId(index);
        if let Some(reason) = self.cell(slot).check_reason() {
            self.retire(slot);
            return Err(WorkError::Cancelled(reason));
        }
        Ok(slot)
    }

    /// The original committed reply owns this exact position. Registration
    /// failure keeps that reply charged; only its actual exit may retire it.
    pub(crate) fn install_granted_child(
        &mut self,
        slot: SlotId,
        parent: SlotId,
        id: WorkId,
        deadline: Option<Instant>,
    ) -> Result<(), WorkError> {
        assert!(matches!(
            self.slots[slot.0].phase,
            SlotPhase::ChildGranted { .. }
        ));
        let generation = self.slots[slot.0]
            .generation
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let parent_cell = Arc::clone(self.cell(parent));
        Cancellation::reuse_child(
            self.slots[slot.0].cell.as_mut().unwrap(),
            &parent_cell,
            deadline,
            id,
        )?;
        self.slots[slot.0].parent = Some(parent);
        if let Some(reason) = self.cell(slot).check_reason() {
            // The reply remains ChildGranted until its original holder exits.
            // A reentrant driver must not reset a still-pending claim.
            self.cell(slot).detach();
            return Err(WorkError::Cancelled(reason));
        }
        self.slots[slot.0].generation = generation;
        self.slots[slot.0].phase = SlotPhase::Node(id);
        Ok(())
    }

    /// Return an uncommitted reservation. No cancellation cell was attached,
    /// no domain fee was installed, and no observer alias escaped this phase.
    pub(crate) fn withdraw_child_reservation(&mut self, id: SlotId) {
        let slot = &mut self.slots[id.0];
        assert!(matches!(
            slot.phase,
            SlotPhase::ChildReserved { .. } | SlotPhase::ChildQueued { .. }
        ));
        assert!(slot.parent.is_none());
        assert!(slot.next_free.is_none());
        slot.phase = SlotPhase::Free;
        slot.next_free = self.free_head;
        self.free_head = Some(id);
    }

    pub(crate) fn retire(&mut self, id: SlotId) {
        self.cell(id).detach();
        let slot = &mut self.slots[id.0];
        assert!(
            matches!(
                slot.phase,
                SlotPhase::Node(_) | SlotPhase::ChildGranted { .. }
            ),
            "retire exactly one assigned slot"
        );
        assert!(
            slot.next_free.is_none(),
            "assigned backing is absent from all free chains"
        );
        if self.fixed {
            slot.phase = SlotPhase::Retired;
            self.retired += 1;
        } else {
            // Preserve the original migration behavior: retained observers own
            // their old Arc, while the State drops its nonprepaid anchor now.
            slot.phase = SlotPhase::Vacant;
            slot.parent = None;
            drop(slot.cell.take());
            slot.next_free = self.legacy_vacant;
            self.legacy_vacant = Some(id);
        }
    }

    /// Called only by the original reaper after exclusive reset, actual
    /// deferred-value exit and original claim rekey. Release cannot fail even
    /// when generation exhaustion closes all later positive operations.
    pub(crate) fn finish_reaped(&mut self, id: SlotId) -> bool {
        assert!(self.fixed);
        let slot = &mut self.slots[id.0];
        assert_eq!(slot.phase, SlotPhase::Retired);
        assert!(slot.next_free.is_none());
        let advanced = if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            true
        } else {
            false
        };
        slot.parent = None;
        slot.phase = SlotPhase::Free;
        slot.next_free = self.free_head;
        self.free_head = Some(id);
        self.retired -= 1;
        advanced
    }

    pub(crate) fn prepare_reap(&mut self, id: SlotId) -> Option<ReapPreparation> {
        let slot = self.slots.get_mut(id.0)?;
        // This original author remains available after the bank exits. At most
        // one actual role anchor is reset per turn, including on a live Node.
        for role in &mut slot.polls {
            if role.phase == crate::role_poll::PollPhase::Retired && role.try_reap() {
                self.retired_polls -= 1;
                return Some(ReapPreparation::PollRole);
            }
        }
        if slot.phase != SlotPhase::Retired
            || slot
                .polls
                .iter()
                .any(|role| role.phase != crate::role_poll::PollPhase::Free)
        {
            return None;
        }
        let cell = Arc::get_mut(slot.cell.as_mut()?)?;
        Some(ReapPreparation::Cancellation {
            reset: cell.reset_for_reuse(),
            parent: slot.parent,
            generation: slot.generation,
        })
    }
}

// A role reset alone never resets the original slot claim or its generation.
pub(crate) enum ReapPreparation {
    PollRole,
    Cancellation {
        reset: CancellationReset,
        parent: Option<SlotId>,
        generation: u64,
    },
}

pub(crate) fn arc_bytes<T>() -> Result<usize, WorkError> {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| WorkError::ArithmeticOverflow)
}

pub(crate) fn array_bytes<T>(count: usize) -> Result<usize, WorkError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| WorkError::ArithmeticOverflow)
}

/// Source-pinned Rust 1.98.1 BTree upper layout: eleven key/value slots,
/// parent pointer/index/length, and twelve internal child pointers. Every
/// retained node is conservatively charged as an internal allocation.
pub(crate) fn btree_peak_bytes<K, V>(entries: usize) -> Result<usize, WorkError> {
    let fields = [
        Layout::new::<Option<std::ptr::NonNull<()>>>(),
        Layout::new::<u16>(),
        Layout::new::<u16>(),
        Layout::array::<K>(11).map_err(|_| WorkError::ArithmeticOverflow)?,
        Layout::array::<V>(11).map_err(|_| WorkError::ArithmeticOverflow)?,
    ];
    let alignment = fields.iter().map(Layout::align).max().unwrap();
    let mut leaf = 0usize;
    for field in fields {
        let rounded = field
            .size()
            .checked_add(alignment - 1)
            .ok_or(WorkError::ArithmeticOverflow)?
            & !(alignment - 1);
        leaf = leaf
            .checked_add(rounded)
            .ok_or(WorkError::ArithmeticOverflow)?;
    }
    let leaf =
        Layout::from_size_align(leaf, alignment).map_err(|_| WorkError::ArithmeticOverflow)?;
    let node = leaf
        .extend(
            Layout::array::<std::ptr::NonNull<()>>(12)
                .map_err(|_| WorkError::ArithmeticOverflow)?,
        )
        .map_err(|_| WorkError::ArithmeticOverflow)?
        .0
        .pad_to_align()
        .size();
    let retained = entries.max(1);
    let levels = (usize::BITS - retained.leading_zeros()) as usize;
    retained
        .checked_add(levels)
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_mul(node))
        .ok_or(WorkError::ArithmeticOverflow)
}

pub(crate) fn storage_cell_bytes() -> Result<usize, WorkError> {
    arc_bytes::<Cancellation>()?
        .checked_add(size_of::<ControlSlot>())
        .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 128 } else { 0 }))
        .ok_or(WorkError::ArithmeticOverflow)
}

pub(crate) fn resources_for_bytes(bytes: usize) -> Result<JointResources, WorkError> {
    Ok(JointResources::ZERO
        .with(JointResource::ScopeRecords, 1)
        .with(JointResource::ClaimRecords, 1)
        .with(
            JointResource::MetadataBytes,
            u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?,
        ))
}
