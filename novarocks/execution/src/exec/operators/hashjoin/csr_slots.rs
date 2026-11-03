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

//! Parallel CSR numeric storage, initialized in disjoint worker-owned segments.
//! Each segment is safely initialized under its private Arc and installed once.
//! Readiness is the original phase barrier, never an Observable notification.

use super::{
    build_state::build_contract,
    join_completion::{arithmetic, funded_shared, funded_vec},
};
use crate::{
    exec::{
        phased_work::{PhasedRangeWork, RangeObligation},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::{
        observable::Observable,
        task::{ExecutionFailure, ExecutionResult},
    },
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard, OnceLock, atomic::AtomicUsize},
};

// The same balanced q/r geometry used by the original Direct shard directory,
// without depending on that KeyIndex-specific storage or its lifecycle.
fn segment_cells(cells: usize, segments: usize, segment: usize) -> ExecutionResult<Range<usize>> {
    if segments == 0 || segments > cells || segment >= segments {
        return Err(build_contract(
            "join CSR segment is outside its original directory",
        ));
    }
    let q = cells / segments;
    let r = cells % segments;
    let start = segment
        .checked_mul(q)
        .and_then(|v| v.checked_add(segment.min(r)))
        .ok_or_else(arithmetic)?;
    let count = q
        .checked_add(usize::from(segment < r))
        .ok_or_else(arithmetic)?;
    Ok(start..start.checked_add(count).ok_or_else(arithmetic)?)
}
fn segment_for_cell(cells: usize, segments: usize, cell: usize) -> ExecutionResult<usize> {
    if segments == 0 || segments > cells || cell >= cells {
        return Err(build_contract(
            "join CSR cell is outside its original directory",
        ));
    }
    let q = cells / segments;
    let r = cells % segments;
    let large = if r == 0 {
        q
    } else {
        q.checked_add(1).ok_or_else(arithmetic)?
    };
    let large_end = large.checked_mul(r).ok_or_else(arithmetic)?;
    if cell < large_end {
        Ok(cell / large)
    } else {
        r.checked_add((cell - large_end) / q).ok_or_else(arithmetic)
    }
}
struct Segment {
    cells: Vec<AtomicUsize>,
    range: Range<usize>,
    // This is the actual claim worker's finite creator, not initializer F0.
    _storage: ValueTypeStorageLifetime,
}
/// One numeric cell per requested CSR slot. Ready retains only its actual
/// directory/data and their finite creators.
pub(crate) struct CsrSlots {
    segments: Vec<OnceLock<Arc<Segment>>>,
    rows: usize,
    _storage: ValueTypeStorageLifetime,
}
impl CsrSlots {
    pub(crate) fn len(&self) -> usize {
        self.rows
    }
    pub(crate) fn at(&self, index: usize) -> ExecutionResult<&AtomicUsize> {
        if index >= self.rows {
            return Err(build_contract("CSR slot is outside its initialized domain"));
        }
        let id = segment_for_cell(self.rows, self.segments.len(), index)?;
        let segment = self
            .segments
            .get(id)
            .and_then(OnceLock::get)
            .ok_or_else(|| build_contract("CSR segment was not initialized"))?;
        segment
            .cells
            .get(index - segment.range.start)
            .ok_or_else(|| build_contract("CSR segment does not cover its exact slot"))
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Initializing,
    Retired,
    Frozen,
}
struct State {
    phase: Phase,
    // Fixed O(F) directory. Only a successful original barrier may move it.
    segments: Option<Vec<OnceLock<Arc<Segment>>>>,
}
pub(crate) struct CsrSlotsInit {
    state: Mutex<State>,
    ranges: Arc<PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>>,
    changed: Arc<Observable>,
    rows: usize,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) enum SlotsClaim {
    Pending,
    Finished,
    Retired,
    Range(SlotsObligation),
}
pub(crate) enum SlotsFreeze {
    Pending,
    Retired,
    Frozen,
    Ready(CsrSlots),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlotsProgress {
    Initialized,
    Progressed,
    Yielded,
    Retired,
}
/// Only the original phase can issue this opaque lease. Its pending Arc never
/// escapes before installation; Arc::get_mut is the sole mutable storage port.
pub(crate) struct SlotsObligation {
    pending: Option<Arc<Segment>>,
    owner: Arc<CsrSlotsInit>,
    obligation: Option<RangeObligation<ExecutionFailure, ValueTypeStorageLifetime>>,
    cells: Range<usize>,
}
impl CsrSlotsInit {
    pub(crate) fn new(
        rows: usize,
        workers: NonZeroUsize,
        changed: Arc<Observable>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        let count = rows.min(workers.get());
        let mut segments = funded_vec(owner, count)?;
        for _ in 0..count {
            control.checkpoint(1)?;
            segments.push(OnceLock::new());
        }
        let ranges = funded_shared(
            owner,
            PhasedRangeWork::with_retention(
                build_contract("join CSR initialization lease was abandoned"),
                arithmetic(),
                owner.storage_lifetime(),
            ),
        )?;
        // One original segment id per lease; this width grants no CPU budget.
        ranges.begin(count, NonZeroUsize::MIN)?;
        funded_shared(
            owner,
            Self {
                state: Mutex::new(State {
                    phase: Phase::Initializing,
                    segments: Some(segments),
                }),
                ranges,
                changed,
                rows,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    fn state(&self) -> ExecutionResult<MutexGuard<'_, State>> {
        self.state.lock().map_err(|poisoned| {
            drop(poisoned.into_inner());
            self.ranges
                .latch(build_contract("join CSR initialization state was poisoned"))
        })
    }
    /// Allocation is charged to this actual claim worker, not initializer F0.
    /// It reserves the segment's backing without an O(cells) initialization.
    pub(crate) fn claim(
        self: &Arc<Self>,
        creator: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<SlotsClaim> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            control
                .checkpoint(0)
                .map_err(|cause| self.ranges.latch(cause.into()))?;
            let mut owned = {
                let state = self.state()?;
                self.ranges.check()?;
                match state.phase {
                    Phase::Retired => return Ok(SlotsClaim::Retired),
                    Phase::Frozen => return Ok(SlotsClaim::Finished),
                    Phase::Initializing => {}
                }
                let Some(lease) = self.ranges.claim()? else {
                    return Ok(if self.ranges.barrier()? {
                        SlotsClaim::Finished
                    } else {
                        SlotsClaim::Pending
                    });
                };
                // Keep the raw original lease local until pure geometry is
                // checked. On failure it drops before state is unlocked;
                // the outer deferred notification then runs after both exit.
                let directory = state.segments.as_ref().ok_or_else(|| {
                    self.ranges
                        .latch(build_contract("join CSR directory is absent"))
                })?;
                let id = lease.range().start;
                let count = self.rows;
                let cells = segment_cells(count, directory.len(), id)
                    .map_err(|cause| self.ranges.latch(cause))?;
                SlotsObligation {
                    pending: None,
                    owner: Arc::clone(self),
                    obligation: Some(lease),
                    cells,
                }
            };
            // Neither allocator callbacks nor cell initialization hold state.
            let cells =
                funded_vec(creator, owned.cells.len()).map_err(|cause| self.ranges.latch(cause))?;
            owned.pending = Some(
                funded_shared(
                    creator,
                    Segment {
                        cells,
                        range: owned.cells.clone(),
                        _storage: creator.storage_lifetime(),
                    },
                )
                .map_err(|cause| self.ranges.latch(cause))?,
            );
            Ok(SlotsClaim::Range(owned))
        })();
        if result.is_err() {
            notify.arm();
        }
        result
    }
    /// Caller must be the original global normal-demand withdrawal author.
    pub(crate) fn retire(&self) -> ExecutionResult<()> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            let mut state = self.state()?;
            self.ranges.check()?;
            state.phase = Phase::Retired;
            Ok(())
        })();
        notify.arm();
        result
    }
    pub(crate) fn try_freeze(&self) -> ExecutionResult<SlotsFreeze> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            let mut state = self.state()?;
            self.ranges.check()?;
            match state.phase {
                Phase::Retired => return Ok(SlotsFreeze::Retired),
                Phase::Frozen => return Ok(SlotsFreeze::Frozen),
                Phase::Initializing => {}
            }
            if !self.ranges.barrier()? {
                return Ok(SlotsFreeze::Pending);
            }
            // complete installs its exact, fully initialized segment before
            // settling its original range. Barrier proves all directory slots
            // are installed; no O(F) re-scan or second directory is required.
            let segments = state.segments.take().ok_or_else(|| {
                self.ranges
                    .latch(build_contract("join CSR directory was already moved"))
            })?;
            state.phase = Phase::Frozen;
            Ok(SlotsFreeze::Ready(CsrSlots {
                segments,
                rows: self.rows,
                _storage: self._storage.clone(),
            }))
        })();
        if matches!(&result, Ok(SlotsFreeze::Ready(_)) | Err(_)) {
            notify.arm();
        }
        result
    }
}
impl SlotsObligation {
    pub(crate) fn initialize(
        &mut self,
        quota: usize,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<SlotsProgress> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            {
                let state = self.owner.state()?;
                let lease = self.obligation.as_ref().ok_or_else(|| {
                    self.owner
                        .ranges
                        .latch(build_contract("join CSR lease was already dropped"))
                })?;
                self.owner.ranges.validate(lease)?;
                if state.phase == Phase::Retired {
                    return Ok(SlotsProgress::Retired);
                }
                if state.phase != Phase::Initializing {
                    return Err(self
                        .owner
                        .ranges
                        .latch(build_contract("join CSR lease used after freeze")));
                }
            }
            let pending = self.pending.as_mut().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join CSR segment is absent"))
            })?;
            let segment = Arc::get_mut(pending).ok_or_else(|| {
                self.owner.ranges.latch(build_contract(
                    "join CSR pending segment is not uniquely owned",
                ))
            })?;
            let target = self.cells.len();
            if segment.range != self.cells
                || segment.cells.len() > target
                || segment.cells.capacity() < target
            {
                return Err(self.owner.ranges.latch(build_contract(
                    "join CSR segment differs from its original cell reservation",
                )));
            }
            if segment.cells.len() == target {
                return Ok(SlotsProgress::Initialized);
            }
            if quota == 0 {
                return Ok(SlotsProgress::Yielded);
            }
            let count = quota.min(target - segment.cells.len());
            let end = segment
                .cells
                .len()
                .checked_add(count)
                .ok_or_else(|| self.owner.ranges.latch(arithmetic()))?;
            // This uniquely owned Vec is already reserved for its entire
            // physical segment. Push cannot grow allocation; each turn adds
            // only its original quota. No global write lock or raw address.
            while segment.cells.len() < end {
                control
                    .checkpoint(1)
                    .map_err(|cause| self.owner.ranges.latch(cause.into()))?;
                segment.cells.push(AtomicUsize::new(0));
            }
            Ok(if segment.cells.len() == target {
                SlotsProgress::Initialized
            } else {
                SlotsProgress::Progressed
            })
        })();
        if result.is_err() {
            notify.arm();
        }
        result
    }
    #[cfg(test)]
    pub(crate) fn complete(&mut self) -> ExecutionResult<()> {
        self.settle_inner(false).map(|_| ())
    }
    /// Resolve completion against the same monotonic retirement lock. A
    /// withdrawal racing the last initialization step is not an invariant error.
    pub(crate) fn settle(&mut self) -> ExecutionResult<SlotsProgress> {
        self.settle_inner(true)
    }
    fn settle_inner(&mut self, allow_retired: bool) -> ExecutionResult<SlotsProgress> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            let state = self.owner.state()?;
            let lease = self.obligation.as_mut().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join CSR lease was already dropped"))
            })?;
            self.owner.ranges.validate(lease)?;
            if state.phase == Phase::Retired && allow_retired {
                drop(state);
                drop(self.pending.take());
                self.owner.ranges.complete(lease)?;
                return Ok(SlotsProgress::Retired);
            }
            if state.phase != Phase::Initializing {
                return Err(self
                    .owner
                    .ranges
                    .latch(build_contract("join CSR completed after normal retirement")));
            }
            let directory = state.segments.as_ref().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join CSR directory is absent"))
            })?;
            let id = lease.range().start;
            let count = self.owner.rows;
            let expected = segment_cells(count, directory.len(), id)
                .map_err(|cause| self.owner.ranges.latch(cause))?;
            let segment = self.pending.as_ref().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join CSR segment was already installed"))
            })?;
            if segment.range != expected
                || self.cells != expected
                || segment.cells.len() != expected.len()
            {
                return Err(self.owner.ranges.latch(build_contract(
                    "join CSR range completed before exact initialization",
                )));
            }
            let slot = directory.get(id).ok_or_else(|| {
                self.owner.ranges.latch(build_contract(
                    "join CSR segment id is outside the directory",
                ))
            })?;
            let segment = self
                .pending
                .take()
                .expect("private checked pending segment");
            if let Err(segment) = slot.set(segment) {
                self.pending = Some(segment);
                return Err(self.owner.ranges.latch(build_contract(
                    "join CSR segment was installed more than once",
                )));
            }
            self.owner.ranges.complete(lease)?;
            Ok(SlotsProgress::Initialized)
        })();
        notify.arm();
        result
    }
    pub(crate) fn retire(&mut self) -> ExecutionResult<()> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            {
                let state = self.owner.state()?;
                if state.phase != Phase::Retired {
                    return Err(self.owner.ranges.latch(build_contract(
                        "join CSR lease retired while demand remains",
                    )));
                }
                let lease = self.obligation.as_ref().ok_or_else(|| {
                    self.owner
                        .ranges
                        .latch(build_contract("join CSR lease was already dropped"))
                })?;
                self.owner.ranges.validate(lease)?;
            }
            // Retired is monotonic. Drop partial actual worker backing outside
            // state, before returning its true phase position. It is not Ready.
            drop(self.pending.take());
            self.owner
                .ranges
                .complete(self.obligation.as_mut().expect("private validated lease"))
        })();
        notify.arm();
        result
    }
}
impl Drop for SlotsObligation {
    fn drop(&mut self) {
        // All safe Vec elements and their actual segment creator exit before
        // the original lease returns its inflight position and notifies.
        drop(self.pending.take());
        if let Some(lease) = self.obligation.take() {
            let unfinished = !lease.is_settled();
            drop(lease);
            if unfinished {
                self.owner.changed.notify_observers();
            }
        }
    }
}
