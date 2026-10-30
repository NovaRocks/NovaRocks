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

//! Hash data state for the common BulkTaskGraph. This module creates no
//! completion token or task identity. Only the parent lease author may settle
//! these private data outputs or cross a phase barrier.

use super::super::{
    KeyIndex,
    hash_build::{BuildWork, PartitionBuildCursor},
    hash_layout::HashLayout,
    route::{
        MergeAccumulator, RouteAdvance, RouteCursor, RoutePartial, RoutePlan, RouteWork,
        RouteWorkspace, RoutedNdv, RoutedRange,
    },
    sizing::{SizingDomain, SizingDomainKind},
    table::{ClassSourcePlan, MaterializedClassChunk, RetiredClassBatches},
};
use super::*;

// Work segmentation preferences. Neither bounds accepted keys nor grants CPU.
const CLASS_CHUNK_ROWS: usize = 4096;

struct Partition {
    accumulator: Option<MergeAccumulator>,
    busy: bool,
    build: Option<PartitionBuildCursor>,
    source: Option<Arc<ClassSourcePlan>>,
    chunks: Vec<MaterializedClassChunk>,
    index: Option<KeyIndex>,
    retired: Option<RetiredClassBatches>,
}
enum Frontier {
    Empty,
    Reserved,
    Queued(RoutePartial),
    Merging,
}
pub(super) struct HashState {
    plan: Arc<RoutePlan>,
    layout: HashLayout,
    purpose: IndexPurpose,
    expected: usize,
    workspaces: Vec<Option<RouteWorkspace>>,
    partitions: Vec<Partition>,
    frontier: Vec<Frontier>,
    routed: usize,
    merges: usize,
    next_partition: usize,
    next_chunk: usize,
    _storage: ValueTypeStorageLifetime,
}
fn route_work<'a>(work: &'a DirectWork<'_>) -> RouteWork<'a> {
    RouteWork {
        owner: work.owner,
        host: Arc::clone(&work.host),
        control: work.control,
    }
}
fn build_work<'a>(work: &'a DirectWork<'_>) -> BuildWork<'a> {
    BuildWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        control: work.control,
    }
}
impl HashState {
    pub(super) fn new(
        input: &CompleteInput,
        layout: HashLayout,
        workers: NonZeroUsize,
        live_slots: NonZeroUsize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let domain = input.stats().domain();
        let mut domains = vector(work.owner, layout.partitions())?;
        let mut partitions = vector(work.owner, layout.partitions())?;
        for p in 0..layout.partitions() {
            work.control.checkpoint(0)?;
            domains.push(SizingDomain::new(
                domain.spec(),
                domain.policy(),
                SizingDomainKind::Partition(p),
                work.owner,
            )?);
            partitions.push(Partition {
                accumulator: None,
                busy: false,
                build: None,
                source: None,
                chunks: Vec::new(),
                index: None,
                retired: None,
            });
        }
        let plan = RoutePlan::new(input, layout, domains, &route_work(work))?;
        let mut workspaces = vector(work.owner, workers.get())?;
        for _ in 0..workers.get() {
            work.control.checkpoint(0)?;
            workspaces.push(Some(RouteWorkspace::new(
                Arc::clone(&plan),
                live_slots,
                &route_work(work),
            )?));
        }
        let frontier_len = workers
            .get()
            .checked_mul(live_slots.get().min(layout.partitions()))
            .ok_or_else(arithmetic)?;
        let mut frontier = vector(work.owner, frontier_len)?;
        for i in 0..frontier_len {
            super::super::checkpoint_at(i, &|| {
                work.control.checkpoint(0).map_err(ExecutionFailure::from)
            })?;
            frontier.push(Frontier::Empty);
        }
        Ok(Self {
            plan,
            layout,
            purpose: input.stats().purpose(),
            expected: input.stats().exact().indexable_rows(),
            workspaces,
            partitions,
            frontier,
            routed: 0,
            merges: 0,
            next_partition: 0,
            next_chunk: 0,
            _storage: work.owner.storage_lifetime(),
        })
    }
    /// Eager merges remain claimable while W route cursors retain workspaces.
    /// Both use the same parent's leases and caller's existing CPU allowance.
    pub(super) fn claim_route(
        &mut self,
        ranges: &mut InputRanges,
        exhausted: &mut bool,
        workers: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Option<HashSeed>> {
        if self.merges < workers {
            for slot in 0..self.frontier.len() {
                super::super::checkpoint_at(slot, &|| {
                    work.control.checkpoint(0).map_err(ExecutionFailure::from)
                })?;
                let Frontier::Queued(partial) = &self.frontier[slot] else {
                    continue;
                };
                let p = partial.partition();
                if self.partitions[p].busy {
                    continue;
                }
                let Frontier::Queued(partial) =
                    std::mem::replace(&mut self.frontier[slot], Frontier::Merging)
                else {
                    unreachable!()
                };
                self.partitions[p].busy = true;
                self.merges += 1;
                return Ok(Some(HashSeed::Merge {
                    slot,
                    p,
                    partial,
                    accumulator: self.partitions[p].accumulator.take(),
                }));
            }
        }
        if *exhausted || !self.frontier.iter().any(|s| matches!(s, Frontier::Empty)) {
            return Ok(None);
        }
        let Some(worker) = self.workspaces.iter().position(Option::is_some) else {
            return Ok(None);
        };
        let Some(range) = ranges.next(self.plan.sources(), &|| {
            work.control.checkpoint(0).map_err(ExecutionFailure::from)
        })?
        else {
            *exhausted = true;
            return Ok(None);
        };
        let workspace = self.workspaces[worker].take().expect("selected workspace");
        Ok(Some(HashSeed::Route {
            worker,
            workspace,
            range,
        }))
    }
    /// Reserve before advancing a route cursor: no yielded partial exists
    /// outside this W*L frontier, including queued and currently merging data.
    pub(super) fn reserve_frontier(&mut self) -> Option<usize> {
        let slot = self
            .frontier
            .iter()
            .position(|s| matches!(s, Frontier::Empty))?;
        self.frontier[slot] = Frontier::Reserved;
        Some(slot)
    }
    pub(super) fn route_advanced(
        &mut self,
        slot: usize,
        partial: Option<RoutePartial>,
    ) -> ExecutionResult<()> {
        let target = self
            .frontier
            .get_mut(slot)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if !matches!(target, Frontier::Reserved) {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        *target = match partial {
            Some(partial) => Frontier::Queued(partial),
            None => Frontier::Empty,
        };
        Ok(())
    }
    pub(super) fn claim_partition(
        &mut self,
        phase: Phase,
        exhausted: &mut bool,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Option<HashSeed>> {
        loop {
            work.control.checkpoint(0)?;
            let p = self.next_partition;
            let Some(part) = self.partitions.get_mut(p) else {
                *exhausted = true;
                return Ok(None);
            };
            if phase == Phase::HashMaterialize {
                let source = part
                    .source
                    .as_ref()
                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
                if self.next_chunk == source.chunk_count() {
                    self.next_partition += 1;
                    self.next_chunk = 0;
                    continue;
                }
                let ordinal = self.next_chunk;
                self.next_chunk += 1;
                return Ok(Some(HashSeed::Materialize {
                    p,
                    ordinal,
                    source: Arc::clone(source),
                }));
            }
            self.next_partition += 1;
            let missing = || invariant(ValueViolation::RowTransaction);
            return Ok(Some(match phase {
                Phase::HashInit => HashSeed::Init {
                    p,
                    accumulator: part.accumulator.take(),
                },
                Phase::HashBuild => HashSeed::Build {
                    p,
                    cursor: part.build.take().ok_or_else(missing)?,
                },
                Phase::HashPublish => HashSeed::Publish {
                    p,
                    source: part.source.take().ok_or_else(missing)?,
                    chunks: std::mem::take(&mut part.chunks),
                },
                Phase::HashRetire => HashSeed::Retire {
                    p,
                    index: part.index.take().ok_or_else(missing)?,
                    retired: part.retired.take().ok_or_else(missing)?,
                },
                _ => return Err(missing()),
            }));
        }
    }
    pub(super) fn complete(&mut self, phase: Phase, output: HashOutput) -> ExecutionResult<()> {
        let missing = || invariant(ValueViolation::RowTransaction);
        match (phase, output) {
            (
                Phase::HashRoute,
                HashOutput::Routed {
                    worker,
                    workspace,
                    range,
                },
            ) => {
                let target = self.workspaces.get_mut(worker).ok_or_else(missing)?;
                if target.is_some() {
                    return Err(missing());
                }
                self.routed = self
                    .routed
                    .checked_add(range.eligible())
                    .filter(|n| *n <= self.expected)
                    .ok_or_else(missing)?;
                *target = Some(workspace);
            }
            (
                Phase::HashRoute,
                HashOutput::Merged {
                    slot,
                    p,
                    accumulator,
                },
            ) => {
                if !matches!(self.frontier.get(slot), Some(Frontier::Merging)) || self.merges == 0 {
                    return Err(missing());
                }
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if !part.busy || part.accumulator.is_some() {
                    return Err(missing());
                }
                part.accumulator = Some(accumulator);
                part.busy = false;
                self.frontier[slot] = Frontier::Empty;
                self.merges -= 1;
            }
            (Phase::HashInit, HashOutput::Initialized { p, cursor }) => {
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if part.build.is_some() {
                    return Err(missing());
                }
                part.build = Some(cursor);
            }
            (Phase::HashBuild, HashOutput::Built { p, source, chunks }) => {
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if part.source.is_some() {
                    return Err(missing());
                }
                part.source = Some(source);
                part.chunks = chunks;
            }
            (Phase::HashMaterialize, HashOutput::Materialized { p, chunk }) => {
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if part.chunks.len() == part.chunks.capacity() {
                    return Err(missing());
                }
                part.chunks.push(chunk);
            }
            (Phase::HashPublish, HashOutput::Published { p, index, retired }) => {
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if part.index.is_some() || part.retired.is_some() {
                    return Err(missing());
                }
                part.index = Some(index);
                part.retired = Some(retired);
            }
            (Phase::HashRetire, HashOutput::Retired { p, index }) => {
                let part = self.partitions.get_mut(p).ok_or_else(missing)?;
                if part.index.is_some() {
                    return Err(missing());
                }
                part.index = Some(index);
            }
            _ => return Err(missing()),
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn frontier_counts(&self) -> (usize, usize, usize) {
        (
            self.frontier
                .iter()
                .filter(|s| matches!(s, Frontier::Empty))
                .count(),
            self.frontier
                .iter()
                .filter(|s| matches!(s, Frontier::Queued(_)))
                .count(),
            self.frontier
                .iter()
                .filter(|s| matches!(s, Frontier::Merging))
                .count(),
        )
    }
    pub(super) fn route_drained(&self) -> bool {
        self.merges == 0
            && self.workspaces.iter().all(Option::is_some)
            && self.frontier.iter().all(|s| matches!(s, Frontier::Empty))
    }
    /// Called only after the common exact completion/actual Body-exit barrier.
    pub(super) fn cross_barrier(
        &mut self,
        phase: Phase,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Phase> {
        let next = match phase {
            Phase::HashRoute => {
                if self.merges != 0
                    || self.workspaces.iter().any(Option::is_none)
                    || self.frontier.iter().any(|s| !matches!(s, Frontier::Empty))
                {
                    return Err(invariant(ValueViolation::RowTransaction));
                }
                let mut merged = 0usize;
                for part in &self.partitions {
                    work.control.checkpoint(0)?;
                    if part.busy {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    if let Some(acc) = &part.accumulator {
                        merged = merged.checked_add(acc.eligible()?).ok_or_else(arithmetic)?;
                    }
                }
                if merged != self.routed || merged != self.expected {
                    return Err(invariant(ValueViolation::Bounds));
                }
                Phase::HashInit
            }
            Phase::HashInit => Phase::HashBuild,
            Phase::HashBuild => Phase::HashMaterialize,
            Phase::HashMaterialize => Phase::HashPublish,
            Phase::HashPublish => Phase::HashRetire,
            Phase::HashRetire => Phase::Assign,
            _ => return Err(invariant(ValueViolation::RowTransaction)),
        };
        self.next_partition = 0;
        self.next_chunk = 0;
        Ok(next)
    }
    pub(super) fn take_view(
        &mut self,
        domain: &BulkDomain,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<IndexView> {
        let mut indexes = vector(work.owner, self.partitions.len())?;
        for part in &mut self.partitions {
            work.control.checkpoint(0)?;
            indexes.push(
                part.index
                    .take()
                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
            );
        }
        IndexView::hash(
            indexes,
            domain.spec().clone(),
            domain.policy().clone(),
            self.layout,
            self.purpose,
            work.owner,
            work.control,
        )
    }
}

pub(super) enum HashSeed {
    Route {
        worker: usize,
        workspace: RouteWorkspace,
        range: InputRange,
    },
    Merge {
        slot: usize,
        p: usize,
        partial: RoutePartial,
        accumulator: Option<MergeAccumulator>,
    },
    Init {
        p: usize,
        accumulator: Option<MergeAccumulator>,
    },
    Build {
        p: usize,
        cursor: PartitionBuildCursor,
    },
    Materialize {
        p: usize,
        ordinal: usize,
        source: Arc<ClassSourcePlan>,
    },
    Publish {
        p: usize,
        source: Arc<ClassSourcePlan>,
        chunks: Vec<MaterializedClassChunk>,
    },
    Retire {
        p: usize,
        index: KeyIndex,
        retired: RetiredClassBatches,
    },
}
pub(super) enum HashOutput {
    Routed {
        worker: usize,
        workspace: RouteWorkspace,
        range: RoutedRange,
    },
    Merged {
        slot: usize,
        p: usize,
        accumulator: MergeAccumulator,
    },
    Initialized {
        p: usize,
        cursor: PartitionBuildCursor,
    },
    Built {
        p: usize,
        source: Arc<ClassSourcePlan>,
        chunks: Vec<MaterializedClassChunk>,
    },
    Materialized {
        p: usize,
        chunk: MaterializedClassChunk,
    },
    Published {
        p: usize,
        index: KeyIndex,
        retired: RetiredClassBatches,
    },
    Retired {
        p: usize,
        index: KeyIndex,
    },
}
pub(super) enum HashBody {
    Route {
        worker: usize,
        cursor: RouteCursor,
    },
    Merge {
        slot: usize,
        p: usize,
        partial: RoutePartial,
        accumulator: Option<MergeAccumulator>,
    },
    Init {
        p: usize,
        accumulator: Option<MergeAccumulator>,
    },
    Build {
        p: usize,
        cursor: PartitionBuildCursor,
    },
    Materialize {
        p: usize,
        ordinal: usize,
        source: Arc<ClassSourcePlan>,
    },
    Publish {
        p: usize,
        source: Arc<ClassSourcePlan>,
        chunks: Vec<MaterializedClassChunk>,
    },
    Retire {
        p: usize,
        index: KeyIndex,
        retired: RetiredClassBatches,
    },
}
impl HashSeed {
    pub(super) fn start(self, work: &DirectWork<'_>) -> ExecutionResult<HashBody> {
        Ok(match self {
            Self::Route {
                worker,
                workspace,
                range,
            } => HashBody::Route {
                worker,
                cursor: workspace.start(range.page, range.occurrences, &route_work(work))?,
            },
            Self::Merge {
                slot,
                p,
                partial,
                accumulator,
            } => HashBody::Merge {
                slot,
                p,
                partial,
                accumulator,
            },
            Self::Init { p, accumulator } => HashBody::Init { p, accumulator },
            Self::Build { p, cursor } => HashBody::Build { p, cursor },
            Self::Materialize { p, ordinal, source } => {
                HashBody::Materialize { p, ordinal, source }
            }
            Self::Publish { p, source, chunks } => HashBody::Publish { p, source, chunks },
            Self::Retire { p, index, retired } => HashBody::Retire { p, index, retired },
        })
    }
}
impl HashBody {
    /// `None` is dependency pending or a finite unfinished data turn. The
    /// caller must keep this same task and spend no extra CPU position.
    pub(super) fn advance(
        body: &mut Option<Self>,
        shared: &Shared,
        quota: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Option<HashOutput>> {
        let active = body
            .as_mut()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        let done = match active {
            Self::Route { cursor, .. } => {
                let slot = {
                    let mut state = shared.state()?;
                    state
                        .hash
                        .as_mut()
                        .ok_or_else(|| invariant(ValueViolation::Layout))?
                        .reserve_frontier()
                };
                let Some(slot) = slot else { return Ok(None) };
                let result = cursor.advance(quota, &route_work(work))?;
                let (partial, done) = match result {
                    RouteAdvance::Progress => (None, false),
                    RouteAdvance::Yielded(partial) => (Some(partial), false),
                    RouteAdvance::Completed => (None, true),
                };
                shared
                    .state()?
                    .hash
                    .as_mut()
                    .ok_or_else(|| invariant(ValueViolation::Layout))?
                    .route_advanced(slot, partial)?;
                done
            }
            Self::Build { cursor, .. } => cursor.advance(quota, &build_work(work))?,
            Self::Retire { retired, .. } => {
                for _ in 0..quota {
                    work.control.checkpoint(0)?;
                    if !retired.retire_one() {
                        break;
                    }
                }
                retired.remaining_batches() == 0
            }
            _ => true,
        };
        if !done {
            return Ok(None);
        }
        let active = body.take().expect("advanced original hash body");
        let output = match active {
            Self::Route { worker, cursor } => {
                let (workspace, range) = cursor.finish(&route_work(work))?;
                HashOutput::Routed {
                    worker,
                    workspace,
                    range,
                }
            }
            Self::Merge {
                slot,
                p,
                partial,
                accumulator,
            } => {
                let plan = {
                    let state = shared.state()?;
                    Arc::clone(
                        &state
                            .hash
                            .as_ref()
                            .ok_or_else(|| invariant(ValueViolation::Layout))?
                            .plan,
                    )
                };
                let mut accumulator = match accumulator {
                    Some(acc) => acc,
                    None => MergeAccumulator::new(plan, p, &route_work(work))?,
                };
                accumulator.merge(partial, &route_work(work))?;
                HashOutput::Merged {
                    slot,
                    p,
                    accumulator,
                }
            }
            Self::Init { p, accumulator } => {
                let (plan, layout, purpose) = {
                    let state = shared.state()?;
                    let h = state
                        .hash
                        .as_ref()
                        .ok_or_else(|| invariant(ValueViolation::Layout))?;
                    (Arc::clone(&h.plan), h.layout, h.purpose)
                };
                let accumulator = match accumulator {
                    Some(acc) => acc,
                    None => MergeAccumulator::new(plan, p, &route_work(work))?,
                };
                let merged = accumulator.finish(&route_work(work))?;
                let initial = match merged.ndv() {
                    RoutedNdv::ExactEmptyMergedSet if merged.eligible() == 0 => 0,
                    RoutedNdv::ObservedHll(estimate) if merged.eligible() != 0 => {
                        (u128::from(estimate).saturating_mul(5).div_ceil(4))
                            .min(merged.eligible() as u128) as usize
                    }
                    _ => return Err(invariant(ValueViolation::Layout)),
                };
                let mut index = KeyIndex::new_for_bulk_hash_with_resources(
                    shared.domain.spec().clone(),
                    purpose,
                    shared.domain.policy().clone(),
                    layout,
                    work.allocator.clone(),
                    Arc::clone(&work.host),
                    work.owner,
                )?;
                index.reserve_bulk_distinct(initial, work.control)?;
                let cursor = PartitionBuildCursor::new(index, merged, purpose, &build_work(work))?;
                HashOutput::Initialized { p, cursor }
            }
            Self::Build { p, cursor } => {
                let index = cursor.finish(&build_work(work))?;
                let source = ClassSourcePlan::new(
                    index,
                    NonZeroUsize::new(CLASS_CHUNK_ROWS).expect("nonzero work preference"),
                    work.owner,
                    work.control,
                )?;
                let chunks = vector(work.owner, source.chunk_count())?;
                HashOutput::Built { p, source, chunks }
            }
            Self::Materialize { p, ordinal, source } => {
                let chunk = source.materialize_chunk(ordinal, work.owner, work.control)?;
                drop(source);
                HashOutput::Materialized { p, chunk }
            }
            Self::Publish { p, source, chunks } => {
                let (index, retired) =
                    ClassSourcePlan::publish(source, chunks, work.owner, work.control)?;
                HashOutput::Published { p, index, retired }
            }
            Self::Retire { p, index, retired } => {
                drop(retired);
                HashOutput::Retired { p, index }
            }
        };
        work.control.checkpoint(0)?;
        Ok(Some(output))
    }
}
