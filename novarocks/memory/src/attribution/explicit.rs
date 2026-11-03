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

//! R1 attribution helpers retain access responsibility without funding authority.
// Design: ADR-0167 (docs/adr/ADR-0167-size-banded-allocation-attribution.md)
use super::{band::is_tagged, binding};
use crate::lane::{Lane, RecordRef};
use std::{alloc::Layout, marker::PhantomData, rc::Rc};

#[derive(Clone, Debug)]
pub struct ExplicitOwner {
    lane: Option<Lane>,
}
struct ExplicitStep {
    previous: Option<RecordRef>,
    _thread: PhantomData<Rc<()>>,
}
impl Drop for ExplicitStep {
    fn drop(&mut self) {
        // SAFETY: exact saved binding on the same synchronous stack. The helper
        // retains its owner's handle until this guard has restored/flushed.
        if let Some(previous) = self.previous {
            unsafe { binding::restore_explicit(previous) };
        }
    }
}
impl ExplicitOwner {
    pub(crate) fn new(lane: Lane) -> Self {
        Self { lane: Some(lane) }
    }
    pub(crate) fn uncovered() -> Self {
        Self { lane: None }
    }
    /// This diagnostic identity grants no access after the owner is dropped.
    pub fn reference(&self) -> Option<RecordRef> {
        self.lane.as_ref().map(Lane::reference)
    }
    fn binding(&self) -> RecordRef {
        self.reference().unwrap_or(RecordRef::SHADOW)
    }
    unsafe fn publish_small(&self, bytes: i64, count: i64, transfer: bool) {
        if let Some(reference) = self.reference() {
            // SAFETY: caller supplies one real allocation transition and this
            // owner retains the record for the complete publication.
            unsafe {
                if transfer {
                    binding::publish_small_transfer(reference, bytes, count);
                } else {
                    binding::publish_small(reference, bytes, count);
                }
            }
        }
    }
    fn bind(&self) -> ExplicitStep {
        // Explicit ownership remains effective after seal/teardown: a surviving
        // container can still resize; diagnostics report growth without refusal.
        // SAFETY: self holds the owner throughout the helper and guard lifetime.
        let previous = unsafe { binding::try_install_explicit(self.binding()) };
        if previous.is_none() {
            crate::lane::global_store().faults.binding_failed();
        }
        ExplicitStep {
            previous,
            _thread: PhantomData,
        }
    }
    /// Calls an allocation operation through the wrapped global allocator using
    /// this exact requested layout. Ok must mean a real successful allocation;
    /// it must publish neither additional small facts nor a duplicate token.
    /// Zero-sized allocator-api allocations have no underlying block or fact.
    pub fn allocate_with<T, E>(
        &self,
        layout: Layout,
        underlying: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let _guard = is_tagged(layout.size()).then(|| self.bind());
        let result = underlying();
        if result.is_ok() && layout.size() != 0 && !is_tagged(layout.size()) {
            // SAFETY: successful distinct allocation, owner held to publication.
            unsafe { self.publish_small(layout.size() as i64, 1, false) };
        }
        result
    }
    /// Releases storage before publishing its final small-allocation decrement.
    /// # Safety
    /// The block was allocated by this owner, is genuinely outstanding and is
    /// released exactly once through the wrapped allocator with its original
    /// requested layout. underlying must complete that actual release, and must
    /// not duplicate this helper's small facts. Do not use the block afterward.
    pub unsafe fn deallocate_with(&self, layout: Layout, underlying: impl FnOnce()) {
        underlying();
        if layout.size() != 0 && !is_tagged(layout.size()) {
            // SAFETY: caller guarantees the one outstanding allocation's free.
            unsafe { self.publish_small(-(layout.size() as i64), -1, false) };
        }
    }
    /// Wraps grow, grow_zeroed or shrink through the wrapped allocator.
    /// # Safety
    /// The old live block belongs to this owner; old/new are the exact requested
    /// layouts. Ok means underlying successfully replaces it with new, Err
    /// preserves the old block and facts. The allocator must preserve contents
    /// as its resize contract requires and must not publish duplicate R1 facts.
    /// The strong owner protects cross-band publication, even at count zero.
    pub unsafe fn resize_with<T, E>(
        &self,
        old: Layout,
        new: Layout,
        underlying: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let _guard = self.bind();
        let result = underlying();
        if result.is_ok() {
            let old_small = !is_tagged(old.size());
            let new_small = !is_tagged(new.size());
            let bytes = (if new_small { new.size() as i64 } else { 0 })
                - (if old_small { old.size() as i64 } else { 0 });
            let count =
                i64::from(new_small && new.size() != 0) - i64::from(old_small && old.size() != 0);
            if bytes != 0 || count != 0 {
                // A tagged→small shrink changes representation, not physical
                // growth. Suppress only that positive transfer's growth fault.
                // SAFETY: successful transition and held owner protect both sides.
                unsafe {
                    if !old_small && new_small {
                        self.publish_small(bytes, count, true);
                    } else {
                        self.publish_small(bytes, count, false);
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::AttributingAllocator;
    use crate::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence};
    use std::alloc::{GlobalAlloc, System};
    #[test]
    fn funded_small_facts_are_counted_once_and_survive_teardown_then_growth() {
        let mut config = AuthorityConfig::new(1 << 20, 1 << 20, 0);
        config.max_accounts = 4;
        config.max_active_owners = 4;
        config.metadata_budget_bytes = 16 * 1024;
        let authority = MemoryAuthority::new(config).unwrap();
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let domain = account.create_domain(128).unwrap();
        let owner = ExplicitOwner::new(domain.lane().clone());
        let allocator = AttributingAllocator::new(System);
        let small = Layout::from_size_align(64, 8).unwrap();
        let grown = Layout::from_size_align(96, 8).unwrap();
        let pointer = owner
            .allocate_with(small, || {
                // SAFETY: nonzero valid layout, checked successful allocation.
                std::ptr::NonNull::new(unsafe { allocator.alloc(small) }).ok_or(())
            })
            .unwrap();
        binding::flush_current();
        let sample = domain.lane().record().snapshot();
        assert_eq!(
            (
                sample.tagged_bytes,
                sample.r1_small_bytes,
                sample.outstanding
            ),
            (0, 64, 1)
        );
        assert_eq!(domain.settle().accepted_live, 64);
        let before = authority.pressure_projection();
        assert_eq!(domain.snapshot().committed, 128);
        account
            .retire(&TeardownEvidence {
                tasks_exited: true,
                operators_destroyed: true,
                io: &[],
                now_ns: 1,
            })
            .unwrap();
        assert_eq!(
            domain.lane().responsibility_class(),
            crate::lane::ResponsibilityClass::Residual
        );
        assert_eq!(domain.snapshot().committed, 64);
        assert_eq!(authority.pressure_projection().query_pressure(), 0);
        assert_eq!(
            before.root_committed - authority.pressure_projection().root_committed,
            64
        );
        let before_fault = crate::lane::global_store()
            .faults
            .snapshot()
            .residual_growth_events;
        // SAFETY: one live block, exact old/new layouts, no duplicate facts.
        let pointer = unsafe {
            owner.resize_with(small, grown, || {
                std::ptr::NonNull::new(allocator.realloc(pointer.as_ptr(), small, grown.size()))
                    .ok_or(())
            })
        }
        .unwrap();
        binding::flush_current();
        assert_eq!(domain.lane().record().snapshot().r1_small_bytes, 96);
        assert_eq!(
            crate::lane::global_store()
                .faults
                .snapshot()
                .residual_growth_events,
            before_fault + 1
        );
        assert_eq!(domain.settle().accepted_live, 96);
        // SAFETY: the sole successful block is released through the same owner.
        unsafe {
            owner.deallocate_with(grown, || allocator.dealloc(pointer.as_ptr(), grown));
        }
        binding::flush_current();
        assert_eq!(domain.lane().record().snapshot().outstanding, 0);
        authority.maintain(32);
    }
}
