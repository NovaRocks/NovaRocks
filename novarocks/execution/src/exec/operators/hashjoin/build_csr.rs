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

//! CSR from original K assignments: parallel count, layered prefix, scatter.
//! No per-input-row group array or coordinator traversal of the row domain.

use super::{
    build_pages::{BuildRows, ProjectionKind},
    build_state::build_contract,
    csr_slots::{CsrSlots, CsrSlotsInit, SlotsClaim, SlotsFreeze, SlotsLease},
    join_completion::{arithmetic, funded_shared, funded_vec},
};
use crate::{
    exec::{
        hash_table::key_index::{BulkAssignments, BulkKey, BulkMatch, BulkWork, IndexView},
        phased_work::{PhasedRangeWork, RangeLease},
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        observable::Observable,
    },
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

type Ranges = PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>;
type Lease = RangeLease<ExecutionFailure, ValueTypeStorageLifetime>;

struct Partition {
    start: usize,
    keys: usize,
}
struct Layout {
    partitions: Vec<Partition>,
    keys: usize,
    width: NonZeroUsize,
    _storage: ValueTypeStorageLifetime,
}
impl Layout {
    fn cell(&self, key: BulkKey) -> ExecutionResult<usize> {
        let partition = self
            .partitions
            .get(key.partition())
            .ok_or_else(|| build_contract("CSR key has a foreign K partition"))?;
        if key.id().index() >= partition.keys {
            return Err(build_contract("CSR key has a foreign local K ID"));
        }
        Ok(partition.start + key.id().index())
    }
    fn count<'a>(&self, cells: &'a CsrSlots, key: usize) -> ExecutionResult<&'a AtomicUsize> {
        cells.at(key)
    }
    fn offset<'a>(&self, cells: &'a CsrSlots, key: usize) -> ExecutionResult<&'a AtomicUsize> {
        cells.at(self.keys + key)
    }
    fn cursor<'a>(&self, cells: &'a CsrSlots, key: usize) -> ExecutionResult<&'a AtomicUsize> {
        cells.at(self.keys * 2 + key)
    }
}

/// Publication keeps only numeric storage and its actual creators. It does not
/// retain a phase owner, initializer, assignment directory or completion object.
pub(crate) struct BuildCsr {
    layout: Arc<Layout>,
    cells: Arc<CsrSlots>,
    bases: Arc<Bases>,
    rows: Arc<CsrSlots>,
}
struct Bases {
    values: Vec<usize>,
    _storage: ValueTypeStorageLifetime,
}
impl BuildCsr {
    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn group(&self, key: BulkKey) -> ExecutionResult<Range<usize>> {
        let cell = self.layout.cell(key)?;
        let base = self.bases.values[cell / self.layout.width.get()];
        let start = base
            .checked_add(
                self.layout
                    .offset(&self.cells, cell)?
                    .load(Ordering::Relaxed),
            )
            .ok_or_else(arithmetic)?;
        let end = start
            .checked_add(
                self.layout
                    .count(&self.cells, cell)?
                    .load(Ordering::Relaxed),
            )
            .ok_or_else(arithmetic)?;
        if end > self.rows.len() {
            return Err(build_contract("CSR group exceeds its original row domain"));
        }
        Ok(start..end)
    }
    pub(crate) fn row(&self, position: usize) -> ExecutionResult<usize> {
        Ok(self.rows.at(position)?.load(Ordering::Relaxed))
    }
}

struct Source {
    assignments: BulkAssignments,
    rows: Arc<BuildRows>,
    projection: ProjectionKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CsrStage {
    Count,
    Prefix,
    Scatter,
}
enum State {
    Initializing(Arc<CsrSlotsInit>),
    Count(Arc<CsrSlots>),
    Prefix(Arc<CsrSlots>),
    InitializingRows {
        cells: Arc<CsrSlots>,
        bases: Arc<Bases>,
        init: Arc<CsrSlotsInit>,
    },
    Scatter {
        cells: Arc<CsrSlots>,
        bases: Arc<Bases>,
        rows: Arc<CsrSlots>,
    },
    Ready(Option<BuildCsr>),
    Transition,
    Retired,
}
pub(crate) struct CsrBuilder {
    state: Mutex<State>,
    layout: Arc<Layout>,
    source: Source,
    sums: Vec<AtomicUsize>,
    workers: NonZeroUsize,
    ranges: Arc<Ranges>,
    changed: Arc<Observable>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) enum CsrClaim {
    Initialize(SlotsLease),
    Work(CsrLease),
    Pending,
    Progressed,
    Ready,
    Retired,
}
pub(crate) struct CsrLease {
    owner: Arc<CsrBuilder>,
    lease: Lease,
    stage: CsrStage,
    cells: Arc<CsrSlots>,
    scatter: Option<(Arc<Bases>, Arc<CsrSlots>)>,
    cursor: usize,
    sum: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CsrProgress {
    Done,
    Progressed,
    Yielded,
    Retired,
}

impl CsrBuilder {
    /// This consumes only immutable metadata from the actual K Ready. Its
    /// partition coordinates are preserved even when K selected hash storage.
    pub(crate) fn new(
        view: &IndexView,
        assignments: BulkAssignments,
        rows: Arc<BuildRows>,
        projection: ProjectionKind,
        workers: NonZeroUsize,
        changed: Arc<Observable>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        work.control.checkpoint(0)?;
        let mut partitions = funded_vec(work.owner, view.partition_count())?;
        let mut keys = 0usize;
        for p in 0..view.partition_count() {
            work.control.checkpoint(1)?;
            let count = view.key_count(p)?;
            partitions.push(Partition {
                start: keys,
                keys: count,
            });
            keys = keys.checked_add(count).ok_or_else(arithmetic)?;
        }
        let width = NonZeroUsize::new(keys.div_ceil(workers.get()).max(1)).expect("positive width");
        let segments = keys.div_ceil(width.get());
        let mut sums = funded_vec(work.owner, segments)?;
        for _ in 0..segments {
            sums.push(AtomicUsize::new(0));
        }
        let layout = funded_shared(
            work.owner,
            Layout {
                partitions,
                keys,
                width,
                _storage: work.owner.storage_lifetime(),
            },
        )?;
        let init = CsrSlotsInit::new(
            keys.checked_mul(3).ok_or_else(arithmetic)?,
            workers,
            Arc::clone(&changed),
            work.owner,
            work.control,
        )?;
        let ranges = funded_shared(
            work.owner,
            PhasedRangeWork::with_retention(
                build_contract("CSR worker abandoned its original range"),
                arithmetic(),
                work.owner.storage_lifetime(),
            ),
        )?;
        funded_shared(
            work.owner,
            Self {
                state: Mutex::new(State::Initializing(init)),
                layout,
                source: Source {
                    assignments,
                    rows,
                    projection,
                },
                sums,
                workers,
                ranges,
                changed,
                _storage: work.owner.storage_lifetime(),
            },
        )
    }
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.ranges.latch(cause);
        self.changed.notify_observers();
        cause
    }
    fn state(&self) -> ExecutionResult<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|p| {
            drop(p.into_inner());
            self.ranges
                .latch(build_contract("CSR phase state was poisoned"))
        })
    }
    /// Normal global demand withdrawal has no Ready result. Unfinished ranges
    /// are never reissued. Existing leases still retain their actual backing.
    pub(crate) fn retire(&self) -> ExecutionResult<()> {
        let old = {
            let mut state = self.state()?;
            std::mem::replace(&mut *state, State::Retired)
        };
        if let State::Initializing(init) | State::InitializingRows { init, .. } = &old {
            init.retire()?;
        }
        drop(old);
        self.changed.notify_observers();
        Ok(())
    }
    pub(crate) fn take_ready(&self) -> ExecutionResult<Option<BuildCsr>> {
        self.ranges.check()?;
        let mut state = self.state()?;
        Ok(match &mut *state {
            State::Ready(value) => value.take(),
            _ => None,
        })
    }
    pub(crate) fn claim(self: &Arc<Self>, work: &BulkWork<'_>) -> ExecutionResult<CsrClaim> {
        let result = self.claim_inner(work);
        result.map_err(|cause| self.fail(cause))
    }
    fn claim_inner(self: &Arc<Self>, work: &BulkWork<'_>) -> ExecutionResult<CsrClaim> {
        work.control.checkpoint(0)?;
        self.ranges.check()?;
        // A transition does O(F + P) metadata work and a single backing
        // reservation. Per-cell initialization remains separately claimable.
        let old = {
            let mut state = self.state()?;
            match &*state {
                State::Retired => return Ok(CsrClaim::Retired),
                State::Ready(_) => return Ok(CsrClaim::Ready),
                State::Transition => return Ok(CsrClaim::Pending),
                State::Initializing(init) | State::InitializingRows { init, .. } => {
                    let init = Arc::clone(init);
                    drop(state);
                    match init.claim(work.owner, work.control)? {
                        SlotsClaim::Range(lease) => return Ok(CsrClaim::Initialize(lease)),
                        SlotsClaim::Pending => return Ok(CsrClaim::Pending),
                        SlotsClaim::Retired => return Ok(CsrClaim::Retired),
                        SlotsClaim::Finished => {}
                    }
                    state = self.state()?;
                    if matches!(&*state, State::Retired) {
                        return Ok(CsrClaim::Retired);
                    }
                    let same = match &*state {
                        State::Initializing(current)
                        | State::InitializingRows { init: current, .. } => {
                            Arc::ptr_eq(current, &init)
                        }
                        _ => false,
                    };
                    if !same {
                        return Ok(CsrClaim::Pending);
                    }
                }
                State::Count(_) | State::Prefix(_) | State::Scatter { .. } => {
                    if let Some(lease) = self.ranges.claim()? {
                        let (stage, cells, scatter) = match &*state {
                            State::Count(cells) => (CsrStage::Count, Arc::clone(cells), None),
                            State::Prefix(cells) => (CsrStage::Prefix, Arc::clone(cells), None),
                            State::Scatter { cells, bases, rows } => (
                                CsrStage::Scatter,
                                Arc::clone(cells),
                                Some((Arc::clone(bases), Arc::clone(rows))),
                            ),
                            _ => unreachable!("phase locked"),
                        };
                        let cursor = if stage == CsrStage::Prefix {
                            lease.range().start
                        } else {
                            0
                        };
                        return Ok(CsrClaim::Work(CsrLease {
                            owner: Arc::clone(self),
                            lease,
                            stage,
                            cells,
                            scatter,
                            cursor,
                            sum: 0,
                        }));
                    }
                    if !self.ranges.barrier()? {
                        return Ok(CsrClaim::Pending);
                    }
                }
            }
            std::mem::replace(&mut *state, State::Transition)
        };
        let next = self.transition(old, work)?;
        {
            let mut state = self.state()?;
            if matches!(&*state, State::Retired) {
                drop(state);
                drop(next);
                return Ok(CsrClaim::Retired);
            }
            if !matches!(&*state, State::Transition) {
                return Err(build_contract("CSR transition lost its original owner"));
            }
            *state = next;
        }
        self.changed.notify_observers();
        // A completed phase is real progress. Yield control instead of
        // recursively starting the next phase within this claim.
        Ok(CsrClaim::Progressed)
    }
    fn transition(&self, old: State, work: &BulkWork<'_>) -> ExecutionResult<State> {
        match old {
            State::Initializing(init) => {
                let SlotsFreeze::Ready(cells) = init.try_freeze()? else {
                    return Err(build_contract("CSR cell barrier did not freeze"));
                };
                let cells = funded_shared(work.owner, cells)?;
                self.ranges
                    .begin(self.source.assignments.range_count(), NonZeroUsize::MIN)?;
                Ok(State::Count(cells))
            }
            State::Count(cells) => {
                self.ranges.begin(self.layout.keys, self.layout.width)?;
                Ok(State::Prefix(cells))
            }
            State::Prefix(cells) => {
                let mut values = funded_vec(work.owner, self.sums.len())?;
                let mut total = 0usize;
                for sum in &self.sums {
                    work.control.checkpoint(1)?;
                    values.push(total);
                    total = total
                        .checked_add(sum.load(Ordering::Relaxed))
                        .ok_or_else(arithmetic)?;
                }
                if total > self.source.rows.len() {
                    return Err(build_contract(
                        "CSR assignments exceed the original physical row domain",
                    ));
                }
                let bases = funded_shared(
                    work.owner,
                    Bases {
                        values,
                        _storage: work.owner.storage_lifetime(),
                    },
                )?;
                let init = CsrSlotsInit::new(
                    total,
                    self.workers,
                    Arc::clone(&self.changed),
                    work.owner,
                    work.control,
                )?;
                Ok(State::InitializingRows { cells, bases, init })
            }
            State::InitializingRows { cells, bases, init } => {
                let SlotsFreeze::Ready(rows) = init.try_freeze()? else {
                    return Err(build_contract("CSR row barrier did not freeze"));
                };
                let rows = funded_shared(work.owner, rows)?;
                self.ranges
                    .begin(self.source.assignments.range_count(), NonZeroUsize::MIN)?;
                Ok(State::Scatter { cells, bases, rows })
            }
            State::Scatter { cells, bases, rows } => Ok(State::Ready(Some(BuildCsr {
                layout: Arc::clone(&self.layout),
                cells,
                bases,
                rows,
            }))),
            _ => Err(build_contract(
                "CSR transition has no completed original phase",
            )),
        }
    }
}
impl CsrLease {
    pub(crate) fn stage(&self) -> CsrStage {
        self.stage
    }
    pub(crate) fn range(&self) -> Range<usize> {
        self.lease.range()
    }
    pub(crate) fn advance(
        &mut self,
        quota: usize,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<CsrProgress> {
        let result = self.advance_inner(quota, work);
        result.map_err(|cause| self.owner.fail(cause))
    }
    fn advance_inner(&mut self, quota: usize, work: &BulkWork<'_>) -> ExecutionResult<CsrProgress> {
        self.owner.ranges.validate(&self.lease)?;
        work.control.checkpoint(0)?;
        if self.lease.is_settled() {
            return Err(build_contract("CSR worker advanced a completed range"));
        }
        if matches!(&*self.owner.state()?, State::Retired) {
            return Ok(CsrProgress::Retired);
        }
        if quota == 0 {
            return Ok(CsrProgress::Yielded);
        }
        let layout = &self.owner.layout;
        if self.stage == CsrStage::Prefix {
            let end = self
                .cursor
                .saturating_add(quota)
                .min(self.lease.range().end);
            while self.cursor < end {
                work.control.checkpoint(1)?;
                let count = layout
                    .count(&self.cells, self.cursor)?
                    .load(Ordering::Relaxed);
                if count == 0 {
                    return Err(build_contract("K emitted an unassigned dense class ID"));
                }
                layout
                    .offset(&self.cells, self.cursor)?
                    .store(self.sum, Ordering::Relaxed);
                self.sum = self.sum.checked_add(count).ok_or_else(arithmetic)?;
                self.cursor += 1;
            }
            return Ok(if self.cursor == self.lease.range().end {
                CsrProgress::Done
            } else {
                CsrProgress::Progressed
            });
        }
        let chunk = self
            .owner
            .source
            .assignments
            .range(self.lease.range().start)?;
        let end = self.cursor.saturating_add(quota).min(chunk.matches().len());
        while self.cursor < end {
            work.control.checkpoint(1)?;
            match chunk.matches()[self.cursor] {
                BulkMatch::Miss => {
                    return Err(build_contract(
                        "K build assignment missed its own original index",
                    ));
                }
                BulkMatch::Unmatchable => {}
                BulkMatch::Found(key) => {
                    let cell = layout.cell(key)?;
                    if self.stage == CsrStage::Count {
                        layout
                            .count(&self.cells, cell)?
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                                n.checked_add(1)
                            })
                            .map_err(|_| arithmetic())?;
                    } else {
                        let (bases, rows) = self.scatter.as_ref().expect("original scatter lease");
                        let cursor = layout.cursor(&self.cells, cell)?;
                        let count = layout.count(&self.cells, cell)?.load(Ordering::Relaxed);
                        let ordinal = cursor
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                                (n < count).then(|| n + 1)
                            })
                            .map_err(|_| {
                                build_contract("CSR scatter exceeded its counted class")
                            })?;
                        let start = bases.values[cell / layout.width.get()]
                            .checked_add(layout.offset(&self.cells, cell)?.load(Ordering::Relaxed))
                            .ok_or_else(arithmetic)?;
                        let address = self.owner.source.rows.address_for_assignment(
                            self.owner.source.projection,
                            chunk.page(),
                            chunk.start() + self.cursor,
                        )?;
                        let row = self.owner.source.rows.row_id(address)?;
                        rows.at(start.checked_add(ordinal).ok_or_else(arithmetic)?)?
                            .store(row.index(), Ordering::Relaxed);
                    }
                }
            }
            self.cursor += 1;
        }
        Ok(if self.cursor == chunk.matches().len() {
            CsrProgress::Done
        } else {
            CsrProgress::Progressed
        })
    }
    pub(crate) fn complete(&mut self) -> ExecutionResult<()> {
        let result = (|| {
            self.owner.ranges.validate(&self.lease)?;
            if matches!(&*self.owner.state()?, State::Retired) {
                return Err(build_contract(
                    "CSR worker completed after demand withdrawal",
                ));
            }
            let expected = if self.stage == CsrStage::Prefix {
                self.lease.range().end
            } else {
                self.owner
                    .source
                    .assignments
                    .range(self.lease.range().start)?
                    .matches()
                    .len()
            };
            if self.cursor != expected {
                return Err(build_contract(
                    "CSR worker completed before consuming its original range",
                ));
            }
            if self.stage == CsrStage::Prefix {
                self.owner.sums[self.lease.range().start / self.owner.layout.width.get()]
                    .store(self.sum, Ordering::Relaxed);
            }
            self.owner.ranges.complete(&mut self.lease)
        })();
        self.owner.changed.notify_observers();
        result.map_err(|cause| self.owner.fail(cause))
    }
    pub(crate) fn retire(&mut self) -> ExecutionResult<()> {
        if !matches!(&*self.owner.state()?, State::Retired) {
            return Err(self
                .owner
                .fail(build_contract("CSR worker retired while demand remains")));
        }
        let result = self.owner.ranges.complete(&mut self.lease);
        self.owner.changed.notify_observers();
        result
    }
}
impl Drop for CsrLease {
    fn drop(&mut self) {
        if !self.lease.is_settled() {
            self.owner
                .fail(build_contract("CSR worker abandoned its original range"));
        }
    }
}

#[cfg(test)]
#[path = "build_csr_tests.rs"]
pub(super) mod tests;
