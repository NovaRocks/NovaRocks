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

//! Local data dependencies and exact task custody. There is no executor here:
//! a caller spends its original turn allowance on `advance`, and owns wakeups
//! and actual driver exit. A completed data cursor is not a phase barrier.

#[cfg(test)]
use super::KeyInput;
use super::{
    IndexPurpose, PreparedKeyBatch,
    bulk::CapturedRuns,
    bulk::CompleteInput,
    bulk_read::{BulkKey, BulkMatch, IndexView},
    capture::{BulkDomain, CapturedPageId},
    direct_int::{self, DirectWork},
    invariant, shared,
    strategy::{StrategyKind, choose_layout},
    vector,
};
use crate::{
    exec::{
        phased_work::{PhasedRangeWork, RangeObligation},
        pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    },
    runtime::task::{ExecutionFailure, ExecutionResult},
};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::ValueViolation;
use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard},
};

// A claim/work segmentation, never an input or distinct-count limit.
const INPUT_RANGE_ROWS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    HashRoute,
    HashInit,
    HashBuild,
    HashMaterialize,
    HashPublish,
    HashRetire,
    Init,
    Mark,
    Count,
    Prefix,
    Fill,
    Null,
    Assign,
    Ready,
    Published,
}

#[derive(Clone)]
struct InputRanges {
    run: usize,
    page: usize,
    occurrence: usize,
}
struct InputRange {
    page: CapturedPageId,
    occurrences: Range<usize>,
}
impl InputRanges {
    fn new() -> Self {
        Self {
            run: 0,
            page: 0,
            occurrence: 0,
        }
    }
    /// Skip only E empty run headers. Even an empty captured page is a task;
    /// one claim never walks all B page descriptors before doing useful work.
    fn next(
        &mut self,
        runs: &CapturedRuns,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Option<InputRange>> {
        while self.run < runs.len() {
            super::checkpoint_at(self.run, checkpoint)?;
            let run = runs.run(self.run)?;
            if self.page == run.len() {
                self.run += 1;
                self.page = 0;
                self.occurrence = 0;
                continue;
            }
            let page = run.page(self.page)?;
            let start = self.occurrence;
            let end = start.saturating_add(INPUT_RANGE_ROWS).min(page.len());
            let result = InputRange {
                page: page.id(),
                occurrences: start..end,
            };
            if end == page.len() {
                self.page += 1;
                self.occurrence = 0;
            } else {
                self.occurrence = end;
            }
            return Ok(Some(result));
        }
        Ok(None)
    }
}

struct ShardFacts {
    counted: Option<direct_int::CountedShard>,
    prefixed: Option<direct_int::PrefixedShard>,
    base: usize,
}
struct State {
    phase: Phase,
    ranges: InputRanges,
    exhausted: bool,
    shards: Vec<ShardFacts>,
    nonnull_classes: usize,
    assignments: Option<AssignmentDirectory>,
    hash: Option<hash::HashState>,
    view: Option<Arc<IndexView>>,
}
struct Shared {
    domain: BulkDomain,
    runs: CapturedRuns,
    store: Option<Arc<direct_int::Storage>>,
    purpose: IndexPurpose,
    workers: usize,
    phases: Arc<PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>>,
    state: Mutex<State>,
    _storage: ValueTypeStorageLifetime,
}
impl Shared {
    fn direct(&self) -> ExecutionResult<&Arc<direct_int::Storage>> {
        self.store
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))
    }
    fn state(&self) -> ExecutionResult<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| invariant(ValueViolation::RowTransaction))
    }
    fn check(&self, work: &DirectWork<'_>) -> ExecutionResult<()> {
        self.domain.check_failure()?;
        self.phases.check()?;
        if let Some(store) = &self.store {
            store.check_failure()?;
        }
        work.control.checkpoint(0).map_err(ExecutionFailure::from)
    }
    fn latch<T>(&self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Err(cause) = &result {
            let cause = self.phases.latch(cause.clone());
            self.domain.latch(cause);
        }
        result
    }
}

/// One graph owns the identities and barriers for the selected local layout.
/// Direct, Hash and Empty share this author and exact private leases. Caller
/// scheduling and wakeups remain outside this local data-dependency graph.
pub(crate) struct BulkBuildGraph(Arc<Shared>);
#[expect(
    clippy::large_enum_variant,
    reason = "One linear task moves with its claim; boxing would add a separately funded allocation per claim."
)]
pub(crate) enum Claim {
    Step(BulkBuildStep),
    Pending,
    Finished,
}

impl BulkBuildGraph {
    /// The scheduling owner preserves its original control/allocation cause
    /// before dropping retained cursors. No abandoned lease may replace it.
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let cause = self.0.phases.latch(cause);
        self.0.domain.latch(cause.clone());
        self.0.domain.check_failure().err().unwrap_or(cause)
    }

    /// Strategy selection precedes any routing or layout allocation. The
    /// caller supplies its actual worker positions and live-sketch preference.
    pub(crate) fn begin(
        input: CompleteInput,
        workers: NonZeroUsize,
        live_slots: NonZeroUsize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<Self> {
        let domain = input.stats().domain().clone();
        let result: ExecutionResult<_> = (|| {
            let decision =
                choose_layout(input.stats(), Some(input.stats().hint()), workers, &|| {
                    work.control.checkpoint(0).map_err(ExecutionFailure::from)
                })?;
            let purpose = input.stats().purpose();
            let mut shards = Vec::new();
            let (store, hash, phase, view) = match decision.kind {
                StrategyKind::Direct(geometry) => {
                    let store = direct_int::Storage::new(
                        geometry,
                        domain.spec(),
                        input.stats().exact().integer_root_nullable_union(),
                        work,
                    )?;
                    shards = vector(work.owner, geometry.shard_count())?;
                    for _ in 0..geometry.shard_count() {
                        work.control.checkpoint(0)?;
                        shards.push(ShardFacts {
                            counted: None,
                            prefixed: None,
                            base: 0,
                        });
                    }
                    (Some(store), None, Phase::Init, None)
                }
                StrategyKind::Hash(layout) => (
                    None,
                    Some(hash::HashState::new(
                        &input, layout, workers, live_slots, work,
                    )?),
                    Phase::HashRoute,
                    None,
                ),
                StrategyKind::Empty => {
                    let view = shared(
                        work.owner,
                        IndexView::empty(
                            domain.spec().clone(),
                            domain.policy().clone(),
                            purpose,
                            work.owner,
                            work.control,
                        )?,
                    )?;
                    (
                        None,
                        None,
                        if purpose == IndexPurpose::PresenceOnly {
                            Phase::Ready
                        } else {
                            Phase::Assign
                        },
                        Some(view),
                    )
                }
            };
            let (runs, _) = input.into_parts();
            let assignments = AssignmentDirectory::new(runs.len(), work)?;
            let phases = shared(
                work.owner,
                PhasedRangeWork::with_retention(
                    invariant(ValueViolation::RowTransaction),
                    arithmetic(),
                    work.owner.storage_lifetime(),
                ),
            )?;
            begin_phase(&phases, phase, shards.len())?;
            let shared = shared(
                work.owner,
                Shared {
                    domain: domain.clone(),
                    runs,
                    store,
                    purpose,
                    workers: workers.get(),
                    phases,
                    state: Mutex::new(State {
                        phase,
                        ranges: InputRanges::new(),
                        exhausted: false,
                        shards,
                        nonnull_classes: 0,
                        assignments: Some(assignments),
                        hash,
                        view,
                    }),
                    _storage: work.owner.storage_lifetime(),
                },
            )?;
            shared.check(work)?;
            Ok(Self(shared))
        })();
        if let Err(cause) = &result {
            domain.latch(cause.clone());
        }
        result
    }

    #[cfg(test)]
    pub(super) fn phase(&self) -> ExecutionResult<Phase> {
        self.0.domain.check_failure()?;
        Ok(self.0.state()?.phase)
    }

    pub(crate) fn claim(&self, work: &DirectWork<'_>) -> ExecutionResult<Claim> {
        self.0.latch(self.claim_inner(work))
    }
    fn claim_inner(&self, work: &DirectWork<'_>) -> ExecutionResult<Claim> {
        self.0.check(work)?;
        let (lease, seed) = {
            let mut state = self.0.state()?;
            loop {
                self.cross_barrier(&mut state, work)?;
                if matches!(state.phase, Phase::Ready | Phase::Published) {
                    return Ok(Claim::Finished);
                }
                // A retained route cursor is data custody, not another CPU
                // position. Let eager merges use the same caller allowance.
                if state.phase != Phase::HashRoute
                    && (self.0.phases.inflight()? == self.0.workers || state.exhausted)
                {
                    return Ok(Claim::Pending);
                }
                let mut claimed = None;
                let seed = match state.phase {
                    Phase::HashRoute => {
                        let State {
                            hash,
                            ranges,
                            exhausted,
                            ..
                        } = &mut *state;
                        let Some(seed) = hash
                            .as_mut()
                            .ok_or_else(|| invariant(ValueViolation::Layout))?
                            .claim_route(ranges, exhausted, self.0.workers, work)?
                        else {
                            if state.exhausted && self.0.phases.inflight()? == 0 {
                                continue;
                            }
                            return Ok(Claim::Pending);
                        };
                        Seed::Hash(seed)
                    }
                    Phase::HashInit
                    | Phase::HashBuild
                    | Phase::HashMaterialize
                    | Phase::HashPublish
                    | Phase::HashRetire => {
                        let phase = state.phase;
                        let State {
                            hash, exhausted, ..
                        } = &mut *state;
                        let Some(seed) = hash
                            .as_mut()
                            .ok_or_else(|| invariant(ValueViolation::Layout))?
                            .claim_partition(phase, exhausted, work)?
                        else {
                            continue;
                        };
                        Seed::Hash(seed)
                    }
                    Phase::Init | Phase::Count | Phase::Prefix | Phase::Fill => {
                        let Some(token) = self.0.phases.claim()? else {
                            state.exhausted = true;
                            continue;
                        };
                        let shard = token.range().start;
                        claimed = Some(token);
                        match state.phase {
                            Phase::Init => Seed::Init(shard),
                            Phase::Count => Seed::Count(shard),
                            Phase::Prefix => {
                                let facts = &mut state.shards[shard];
                                Seed::Prefix(
                                    facts
                                        .counted
                                        .take()
                                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                                    facts.base,
                                )
                            }
                            Phase::Fill => Seed::Fill(
                                state.shards[shard]
                                    .prefixed
                                    .take()
                                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                            ),
                            _ => unreachable!(),
                        }
                    }
                    Phase::Mark | Phase::Assign => {
                        let Some(range) =
                            state.ranges.next(&self.0.runs, &|| self.0.check(work))?
                        else {
                            state.exhausted = true;
                            continue;
                        };
                        if state.phase == Phase::Mark {
                            Seed::Mark(range)
                        } else {
                            Seed::Assign(range)
                        }
                    }
                    Phase::Null => {
                        let Some(token) = self.0.phases.claim()? else {
                            state.exhausted = true;
                            continue;
                        };
                        claimed = Some(token);
                        state.exhausted = true;
                        Seed::Null(state.nonnull_classes)
                    }
                    Phase::Ready | Phase::Published => unreachable!(),
                };
                let token = match claimed {
                    Some(token) => token,
                    None => self.0.phases.claim_one()?,
                };
                let ordinal = token.ordinal();
                if let Seed::Assign(range) = &seed {
                    let result = state
                        .assignments
                        .as_mut()
                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))
                        .and_then(|directory| directory.claim(ordinal, range, work));
                    if let Err(cause) = result {
                        self.0.phases.latch(cause.clone());
                        self.0.domain.latch(cause.clone());
                        return Err(cause);
                    }
                }
                break (
                    BulkStepObligation {
                        shared: Arc::clone(&self.0),
                        phase: state.phase,
                        ordinal,
                        token,
                    },
                    seed,
                );
            }
        };
        // Allocation, source access and actual cursor work run outside the
        // metadata mutex. A failed constructor drops its still-open lease.
        let body: ExecutionResult<_> = (|| {
            Ok(match seed {
                Seed::Hash(seed) => Body::Hash(Some(seed.start(work)?)),
                Seed::Init(shard) => Body::Init(direct_int::InitCursor::new(
                    Arc::clone(self.0.direct()?),
                    shard,
                    work,
                )?),
                Seed::Mark(range) => Body::Mark(direct_int::MarkCursor::new(
                    Arc::clone(self.0.direct()?),
                    self.0.runs.page(range.page)?.input(),
                    range.occurrences,
                )?),
                Seed::Count(shard) => Body::Count(direct_int::CountCursor::new(
                    Arc::clone(self.0.direct()?),
                    shard,
                    work,
                )?),
                Seed::Prefix(counted, base) => {
                    Body::Prefix(direct_int::PrefixCursor::new(counted, base)?)
                }
                Seed::Fill(prefixed) => Body::Fill(direct_int::FillCursor::new(prefixed, work)?),
                Seed::Null(count) => Body::Null(count),
                Seed::Assign(range) => Body::Assign(AssignCursor::new(&self.0, range, work)?),
            })
        })();
        let body = match body {
            Ok(body) => body,
            Err(cause) => {
                // Preserve the actual refusal before the unfinished lease's
                // Drop records abandonment of this exact task.
                self.0.phases.latch(cause.clone());
                self.0.domain.latch(cause.clone());
                return Err(cause);
            }
        };
        Ok(Claim::Step(BulkBuildStep {
            body: Some(body),
            output: None,
            obligation: Some(lease),
        }))
    }

    /// Taking a completion is exact once even when a caller repeats its
    /// `complete` invocation. Output and lease are not separately constructible.
    pub(crate) fn complete(
        &self,
        completion: &mut Completion,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        self.0.latch(self.complete_inner(completion, work))
    }
    fn complete_inner(
        &self,
        completion: &mut Completion,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        self.0.check(work)?;
        let (output, mut lease) = completion
            .value
            .take()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        let result = (|| {
            if !Arc::ptr_eq(&self.0, &lease.shared) {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            let mut state = self.0.state()?;
            if lease.phase != state.phase || lease.token.is_settled() {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            self.0.phases.validate(&lease.token)?;
            match (state.phase, output) {
                (
                    Phase::HashRoute
                    | Phase::HashInit
                    | Phase::HashBuild
                    | Phase::HashMaterialize
                    | Phase::HashPublish
                    | Phase::HashRetire,
                    Output::Hash(output),
                ) => {
                    let phase = state.phase;
                    state
                        .hash
                        .as_mut()
                        .ok_or_else(|| invariant(ValueViolation::Layout))?
                        .complete(phase, output)?;
                }
                (Phase::Init | Phase::Mark | Phase::Fill | Phase::Null, Output::Unit) => {}
                (Phase::Count, Output::Counted(counted)) => {
                    let shard = counted.shard();
                    let facts = state
                        .shards
                        .get_mut(shard)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    if facts.counted.is_some() {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.counted = Some(counted);
                }
                (Phase::Prefix, Output::Prefixed { shard, prefix }) => {
                    let facts = state
                        .shards
                        .get_mut(shard)
                        .ok_or_else(|| invariant(ValueViolation::Bounds))?;
                    if facts.prefixed.is_some() {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.prefixed = Some(prefix);
                }
                (Phase::Assign, Output::Assigned(node)) => {
                    state
                        .assignments
                        .as_mut()
                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?
                        .install(lease.ordinal, node)?;
                }
                _ => return Err(invariant(ValueViolation::RowTransaction)),
            }
            self.0.phases.complete(&mut lease.token)?;
            self.cross_barrier(&mut state, work)
        })();
        // In particular, drop any unconsumed cursor/output before the failed
        // token publishes its abandonment fact; never refund an active worker.
        if let Err(cause) = &result {
            lease.shared.phases.latch(cause.clone());
            lease.shared.domain.latch(cause.clone());
        }
        result
    }

    fn cross_barrier(&self, state: &mut State, work: &DirectWork<'_>) -> ExecutionResult<()> {
        if !state.exhausted || self.0.phases.inflight()? != 0 {
            return Ok(());
        }
        // Enumeration EOF does not consume queued, as-yet unclaimed merges.
        // Leave Route open so the same claim author can drain that dependency.
        if state.phase == Phase::HashRoute
            && !state
                .hash
                .as_ref()
                .ok_or_else(|| invariant(ValueViolation::Layout))?
                .route_drained()
        {
            return Ok(());
        }
        if matches!(
            state.phase,
            Phase::HashRoute
                | Phase::HashInit
                | Phase::HashBuild
                | Phase::HashMaterialize
                | Phase::HashPublish
                | Phase::HashRetire
                | Phase::Mark
                | Phase::Assign
        ) {
            self.0.phases.seal()?;
        }
        if !self.0.phases.barrier()? {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        self.0.check(work)?;
        let next = match state.phase {
            Phase::HashRoute
            | Phase::HashInit
            | Phase::HashBuild
            | Phase::HashMaterialize
            | Phase::HashPublish
            | Phase::HashRetire => {
                let phase = state.phase;
                let hash = state
                    .hash
                    .as_mut()
                    .ok_or_else(|| invariant(ValueViolation::Layout))?;
                let next = hash.cross_barrier(phase, work)?;
                if phase == Phase::HashRetire {
                    state.view = Some(shared(work.owner, hash.take_view(&self.0.domain, work)?)?);
                    if self.0.purpose == IndexPurpose::PresenceOnly {
                        Phase::Ready
                    } else {
                        next
                    }
                } else {
                    next
                }
            }
            Phase::Init => Phase::Mark,
            Phase::Mark if self.0.purpose == IndexPurpose::PresenceOnly => Phase::Ready,
            Phase::Mark => Phase::Count,
            Phase::Count => {
                // Only W small totals here; each worker prefixes its own word
                // counts later. There is no coordinator scan of bitmap/slots.
                let mut base = 0usize;
                for (index, facts) in state.shards.iter_mut().enumerate() {
                    work.control.checkpoint(0)?;
                    let counted = facts
                        .counted
                        .as_ref()
                        .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
                    if counted.shard() != index {
                        return Err(invariant(ValueViolation::RowTransaction));
                    }
                    facts.base = base;
                    base = base.checked_add(counted.total()).ok_or_else(arithmetic)?;
                }
                state.nonnull_classes = base;
                Phase::Prefix
            }
            Phase::Prefix => Phase::Fill,
            Phase::Fill => Phase::Null,
            Phase::Null => Phase::Assign,
            Phase::Assign => Phase::Ready,
            Phase::Ready | Phase::Published => return Ok(()),
        };
        if (next == Phase::Assign || next == Phase::Ready) && state.view.is_none() {
            state.view = Some(shared(
                work.owner,
                IndexView::direct(Arc::clone(self.0.direct()?), work.owner, work.control)?,
            )?);
        }
        state.phase = next;
        state.ranges = InputRanges::new();
        state.exhausted = false;
        begin_phase(&self.0.phases, next, state.shards.len())?;
        Ok(())
    }

    pub(crate) fn take_ready(&self, work: &DirectWork<'_>) -> ExecutionResult<Option<BulkReady>> {
        self.0.latch((|| {
            self.0.check(work)?;
            let mut state = self.0.state()?;
            self.cross_barrier(&mut state, work)?;
            if state.phase != Phase::Ready {
                return Ok(None);
            }
            self.0.check(work)?;
            state.phase = Phase::Published;
            Ok(Some(BulkReady {
                assignments: state
                    .assignments
                    .take()
                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                view: state
                    .view
                    .take()
                    .ok_or_else(|| invariant(ValueViolation::RowTransaction))?,
                #[cfg(test)]
                direct: self.0.store.clone(),
            }))
        })())
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "The claimed seed transfers existing cursor ownership inline without a new allocation."
)]
enum Seed {
    Hash(hash::HashSeed),
    Init(usize),
    Mark(InputRange),
    Count(usize),
    Prefix(direct_int::CountedShard, usize),
    Fill(direct_int::PrefixedShard),
    Null(usize),
    Assign(InputRange),
}
struct BulkStepObligation {
    shared: Arc<Shared>,
    phase: Phase,
    ordinal: usize,
    token: RangeObligation<ExecutionFailure, ValueTypeStorageLifetime>,
}
impl Drop for BulkStepObligation {
    fn drop(&mut self) {
        if !self.token.is_settled() {
            self.shared
                .domain
                .latch(invariant(ValueViolation::RowTransaction));
        }
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "The task keeps its original cursor and resource holders inline across bounded advances."
)]
enum Body {
    Hash(Option<hash::HashBody>),
    Init(direct_int::InitCursor),
    Mark(direct_int::MarkCursor),
    Count(direct_int::CountCursor),
    Prefix(direct_int::PrefixCursor),
    Fill(direct_int::FillCursor),
    Null(usize),
    Assign(AssignCursor),
}
#[expect(
    clippy::large_enum_variant,
    reason = "Completion moves its existing owned payload once without allocating another carrier."
)]
enum Output {
    Hash(hash::HashOutput),
    Unit,
    Counted(direct_int::CountedShard),
    Prefixed {
        shard: usize,
        prefix: direct_int::PrefixedShard,
    },
    Assigned(Arc<AssignmentNode>),
}
/// A finite turn either completes, makes actual data progress, or spends no
/// quota because its dependency is unavailable. Scheduling belongs to caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepAdvance {
    Done,
    Progressed,
    Blocked,
}
impl StepAdvance {
    #[cfg(test)]
    pub(crate) fn is_done(self) -> bool {
        self == Self::Done
    }
}

pub(crate) struct BulkBuildStep {
    body: Option<Body>,
    output: Option<Output>,
    // Actual cursor/output fields drop before an abandoned lease.
    obligation: Option<BulkStepObligation>,
}
pub(crate) struct Completion {
    value: Option<(Output, BulkStepObligation)>,
}
impl BulkBuildStep {
    #[cfg(test)]
    pub(crate) fn phase_name_for_test(&self) -> String {
        format!("{:?}", self.phase())
    }
    #[cfg(test)]
    pub(super) fn phase(&self) -> Phase {
        self.obligation.as_ref().expect("unconsumed task").phase
    }
    /// Quota is the caller's existing synchronous turn allowance, not a new
    /// resource authorization. Zero cannot run even the small NULL task.
    pub(crate) fn advance(
        &mut self,
        quota: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<StepAdvance> {
        let origin = Arc::clone(&self.obligation.as_ref().expect("unconsumed task").shared);
        let result = self.advance_inner(quota, work);
        if let Err(cause) = &result {
            self.body.take();
            self.output.take();
            origin.phases.latch(cause.clone());
            origin.domain.latch(cause.clone());
        }
        result
    }
    fn advance_inner(
        &mut self,
        quota: usize,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<StepAdvance> {
        let lease = self.obligation.as_ref().expect("unconsumed task");
        lease.shared.check(work)?;
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        if quota == 0 {
            return Ok(StepAdvance::Blocked);
        }
        if let Body::Hash(body) = body {
            let output = match hash::HashBody::advance(body, &lease.shared, quota, work)? {
                hash::HashAdvance::Done(output) => output,
                hash::HashAdvance::Progressed => return Ok(StepAdvance::Progressed),
                hash::HashAdvance::Blocked => return Ok(StepAdvance::Blocked),
            };
            self.body.take();
            lease.shared.check(work)?;
            self.output = Some(Output::Hash(output));
            return Ok(StepAdvance::Done);
        }
        let done = match body {
            Body::Hash(_) => unreachable!(),
            Body::Init(c) => c.advance(quota, work)?,
            Body::Mark(c) => c.advance(quota, work)?,
            Body::Count(c) => c.advance(quota, work)?,
            Body::Prefix(c) => c.advance(quota, work)?,
            Body::Fill(c) => c.advance(quota, work)?,
            Body::Null(classes) => {
                lease.shared.direct()?.finish_null(*classes, work)?;
                true
            }
            Body::Assign(cursor) => cursor.advance(quota, work)?,
        };
        if done {
            let output = match self.body.take().expect("advanced body") {
                Body::Count(c) => Output::Counted(c.finish()?),
                Body::Prefix(c) => {
                    // Claim order is the exact shard ordinal in these phases.
                    Output::Prefixed {
                        shard: lease.ordinal,
                        prefix: c.finish()?,
                    }
                }
                Body::Assign(cursor) => {
                    Output::Assigned(shared(work.owner, cursor.finish(work.owner)?)?)
                }
                Body::Hash(_) => unreachable!(),
                Body::Init(_) | Body::Mark(_) | Body::Fill(_) | Body::Null(_) => Output::Unit,
            };
            lease.shared.check(work)?;
            self.output = Some(output);
        }
        Ok(if done {
            StepAdvance::Done
        } else {
            StepAdvance::Progressed
        })
    }
    pub(crate) fn finish(mut self) -> ExecutionResult<Completion> {
        let lease = self.obligation.as_ref().expect("unconsumed task");
        lease.shared.domain.check_failure()?;
        let Some(output) = self.output.take() else {
            let cause = invariant(ValueViolation::RowTransaction);
            lease.shared.phases.latch(cause.clone());
            lease.shared.domain.latch(cause.clone());
            return Err(cause);
        };
        Ok(Completion {
            value: Some((output, self.obligation.take().expect("unconsumed task"))),
        })
    }
}

struct AssignmentPage {
    start: usize,
    matches: Vec<BulkMatch>,
    _storage: ValueTypeStorageLifetime,
}
struct AssignCursor {
    view: Arc<IndexView>,
    runs: CapturedRuns,
    #[cfg(test)]
    input: KeyInput,
    page: CapturedPageId,
    position: usize,
    end: usize,
    output: AssignmentPage,
}
impl AssignCursor {
    fn new(shared: &Shared, range: InputRange, work: &DirectWork<'_>) -> ExecutionResult<Self> {
        let view = shared
            .state()?
            .view
            .as_ref()
            .cloned()
            .ok_or_else(|| invariant(ValueViolation::RowTransaction))?;
        #[cfg(test)]
        let input = shared.runs.page(range.page)?.input().clone();
        let output = AssignmentPage {
            start: range.occurrences.start,
            matches: vector(work.owner, range.occurrences.len())?,
            _storage: work.owner.storage_lifetime(),
        };
        Ok(Self {
            view,
            runs: shared.runs.clone(),
            #[cfg(test)]
            input,
            page: range.page,
            position: range.occurrences.start,
            end: range.occurrences.end,
            output,
        })
    }
    fn advance(&mut self, quota: usize, work: &DirectWork<'_>) -> ExecutionResult<bool> {
        work.control.checkpoint(0)?;
        if self.position == self.end {
            return Ok(true);
        }
        let end = self.position.saturating_add(quota).min(self.end);
        let page = self.runs.page(self.page)?;
        let prepared = PreparedKeyBatch::from_captured(
            self.view.spec(),
            page.input(),
            work.allocator.clone(),
            &|| work.control.checkpoint(0).map_err(ExecutionFailure::from),
        )?;
        let found = self.view.probe_range(
            &prepared,
            Some(page.hashes()),
            self.position..end,
            work.owner,
            work.control,
        )?;
        if found.matches().len() != end - self.position {
            return Err(invariant(ValueViolation::Bounds));
        }
        for &found in found.matches() {
            work.control.checkpoint(0)?;
            // All participating captured rows were routed and built. Empty
            // input also reaches here: its every original row is Unmatchable.
            if matches!(found, BulkMatch::Miss) {
                return Err(invariant(ValueViolation::RowTransaction));
            }
            self.output.matches.push(found);
        }
        self.position = end;
        Ok(self.position == self.end)
    }
    fn finish(self, owner: &dyn InputConstructionOwner) -> ExecutionResult<AssignmentNode> {
        if self.position != self.end || self.output.matches.len() != self.end - self.output.start {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        Ok(AssignmentNode {
            page: self.page,
            #[cfg(test)]
            input: self.input,
            assignment: self.output,
            _storage: owner.storage_lifetime(),
        })
    }
}

struct AssignmentNode {
    page: CapturedPageId,
    #[cfg(test)]
    input: KeyInput,
    assignment: AssignmentPage,
    _storage: ValueTypeStorageLifetime,
}
struct AssignmentPageIndex {
    id: CapturedPageId,
    first: usize,
    ranges: usize,
}
struct AssignmentRun {
    pages: Vec<AssignmentPageIndex>,
    _storage: ValueTypeStorageLifetime,
}
struct AssignmentDirectory {
    slots: Vec<Option<Arc<AssignmentNode>>>,
    runs: Vec<AssignmentRun>,
    _slots_storage: ValueTypeStorageLifetime,
    _runs_storage: ValueTypeStorageLifetime,
}
// A reallocation is paired to its real new creator. Old backing is released
// inside reserve before its original hold is replaced, including post-control
// failure. An unchanged allocation retains its original creator.
fn grow_directory<T>(
    values: &mut Vec<T>,
    storage: &mut ValueTypeStorageLifetime,
    work: &DirectWork<'_>,
) -> ExecutionResult<()> {
    let before = values.capacity();
    let result = super::reserve_push(values, work.owner, &|| {
        work.control.checkpoint(0).map_err(ExecutionFailure::from)
    });
    if values.capacity() != before {
        *storage = work.owner.storage_lifetime();
    }
    result
}
impl AssignmentDirectory {
    fn new(runs: usize, work: &DirectWork<'_>) -> ExecutionResult<Self> {
        let mut directory = vector(work.owner, runs)?;
        for run in 0..runs {
            super::checkpoint_at(run, &|| {
                work.control.checkpoint(0).map_err(ExecutionFailure::from)
            })?;
            directory.push(AssignmentRun {
                pages: Vec::new(),
                _storage: work.owner.storage_lifetime(),
            });
        }
        Ok(Self {
            slots: Vec::new(),
            runs: directory,
            _slots_storage: work.owner.storage_lifetime(),
            _runs_storage: work.owner.storage_lifetime(),
        })
    }
    /// Enumeration, not completion order, fixes every public range coordinate.
    /// One claim inspects one page header and admits one output slot. It never
    /// traverses the input rows or creates a coordinator-wide B-page scan.
    fn claim(
        &mut self,
        ordinal: usize,
        range: &InputRange,
        work: &DirectWork<'_>,
    ) -> ExecutionResult<()> {
        if ordinal != self.slots.len() || !range.occurrences.start.is_multiple_of(INPUT_RANGE_ROWS)
        {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let run = self
            .runs
            .get_mut(range.page.run.index())
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if range.page.page == run.pages.len() {
            if range.occurrences.start != 0 {
                return Err(invariant(ValueViolation::Bounds));
            }
            grow_directory(&mut run.pages, &mut run._storage, work)?;
            run.pages.push(AssignmentPageIndex {
                id: range.page,
                first: ordinal,
                ranges: 0,
            });
        }
        let page = run
            .pages
            .get_mut(range.page.page)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if page.id != range.page || range.occurrences.start / INPUT_RANGE_ROWS != page.ranges {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        grow_directory(&mut self.slots, &mut self._slots_storage, work)?;
        self.slots.push(None);
        page.ranges = page.ranges.checked_add(1).ok_or_else(arithmetic)?;
        Ok(())
    }
    fn install(&mut self, ordinal: usize, node: Arc<AssignmentNode>) -> ExecutionResult<()> {
        let page = self
            .runs
            .get(node.page.run.index())
            .and_then(|r| r.pages.get(node.page.page))
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if page.id != node.page || page.first + node.assignment.start / INPUT_RANGE_ROWS != ordinal
        {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        let slot = self
            .slots
            .get_mut(ordinal)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if slot.is_some() {
            return Err(invariant(ValueViolation::RowTransaction));
        }
        *slot = Some(node);
        Ok(())
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Checked input-coordinate assignment lookup is the B3 finalize handoff."
        )
    )]
    fn get(&self, page: CapturedPageId, start: usize) -> ExecutionResult<AssignmentChunk<'_>> {
        if !start.is_multiple_of(INPUT_RANGE_ROWS) {
            return Err(invariant(ValueViolation::Bounds));
        }
        let index = self
            .runs
            .get(page.run.index())
            .and_then(|r| r.pages.get(page.page))
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        let range = start / INPUT_RANGE_ROWS;
        if index.id != page || range >= index.ranges {
            return Err(invariant(ValueViolation::Bounds));
        }
        self.range(index.first + range)
    }
    fn range(&self, ordinal: usize) -> ExecutionResult<AssignmentChunk<'_>> {
        self.slots
            .get(ordinal)
            .and_then(Option::as_deref)
            .map(AssignmentChunk)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    fn iter(&self) -> Assignments<'_> {
        Assignments(self.slots.iter())
    }
}

/// Only the all-task barrier constructs this readonly holder. It cannot expose
/// mark/intern or be manufactured from one successful low-level cursor.
pub(crate) struct BulkReady {
    assignments: AssignmentDirectory,
    view: Arc<IndexView>,
    #[cfg(test)]
    direct: Option<Arc<direct_int::Storage>>,
}
/// The caller may finish its row-chain/CSR work and drop this original source
/// custody independently of the immutable index. No new task or proof is made.
pub(crate) struct BulkAssignments(AssignmentDirectory);
impl BulkAssignments {
    /// Stable input-range ordinal, independent of worker completion order.
    pub(crate) fn range_count(&self) -> usize {
        self.0.slots.len()
    }
    pub(crate) fn range(&self, ordinal: usize) -> ExecutionResult<AssignmentChunk<'_>> {
        self.0.range(ordinal)
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Checked input-coordinate assignment lookup is the B3 finalize handoff."
        )
    )]
    pub(crate) fn get(
        &self,
        page: CapturedPageId,
        start: usize,
    ) -> ExecutionResult<AssignmentChunk<'_>> {
        self.0.get(page, start)
    }
    #[cfg(test)]
    pub(crate) fn iter(&self) -> Assignments<'_> {
        self.0.iter()
    }
}
impl BulkReady {
    pub(crate) fn into_parts(self) -> (Arc<IndexView>, BulkAssignments) {
        (self.view, BulkAssignments(self.assignments))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready metadata accessor; the current join consumes into_parts."
        )
    )]
    pub(crate) fn spec(&self) -> &super::KeySpec {
        self.view.spec()
    }
    #[expect(
        dead_code,
        reason = "Frozen Ready purpose/presence query API; current join queries the retained IndexView."
    )]
    pub(crate) fn purpose(&self) -> IndexPurpose {
        self.view.purpose()
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready query/typed-output/assignment handoff; current join consumes into_parts."
        )
    )]
    pub(crate) fn probe(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&super::LocalHashBatch>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::bulk_read::BulkProbeBatch> {
        self.view.probe(input, saved, owner, control)
    }
    #[expect(
        dead_code,
        reason = "Frozen Ready purpose/presence query API; current join queries the retained IndexView."
    )]
    pub(crate) fn contains(
        &self,
        input: &PreparedKeyBatch<'_>,
        saved: Option<&super::LocalHashBatch>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::bulk_read::BulkPresenceBatch> {
        self.view.contains(input, saved, owner, control)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready query/typed-output/assignment handoff; current join consumes into_parts."
        )
    )]
    pub(super) fn view(&self) -> &IndexView {
        &self.view
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready query/typed-output/assignment handoff; current join consumes into_parts."
        )
    )]
    pub(crate) fn read_keys(
        &self,
        keys: &[BulkKey],
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::OwnedKeyReadBatch<'_>> {
        self.view.read_keys(keys, owner, control)
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready query/typed-output/assignment handoff; current join consumes into_parts."
        )
    )]
    pub(crate) fn export_keys(
        &self,
        keys: &[BulkKey],
        targets: &[FunctionValueType],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::ExportedKeys> {
        self.view.export_keys(keys, targets, owner, host, control)
    }
    #[cfg(test)]
    fn contains_integer(&self, value: Option<i64>) -> ExecutionResult<bool> {
        self.direct
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .contains_value_after_mark(value)
    }
    #[cfg(test)]
    fn lookup_integer(&self, value: Option<i64>) -> ExecutionResult<super::KeyMatch> {
        self.direct
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .lookup_value_after_fill(value)
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Frozen Ready query/typed-output/assignment handoff; current join consumes into_parts."
        )
    )]
    pub(crate) fn assignments(&self) -> Assignments<'_> {
        self.assignments.iter()
    }
}
pub(crate) struct Assignments<'a>(std::slice::Iter<'a, Option<Arc<AssignmentNode>>>);
pub(crate) struct AssignmentChunk<'a>(&'a AssignmentNode);
impl<'a> Iterator for Assignments<'a> {
    type Item = AssignmentChunk<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|node| AssignmentChunk(node.as_deref().expect("complete assignment barrier")))
    }
}
impl AssignmentChunk<'_> {
    pub(crate) fn page(&self) -> CapturedPageId {
        self.0.page
    }
    pub(crate) fn start(&self) -> usize {
        self.0.assignment.start
    }
    pub(crate) fn matches(&self) -> &[BulkMatch] {
        &self.0.assignment.matches
    }
    #[cfg(test)]
    pub(crate) fn row(&self, ordinal: usize) -> ExecutionResult<usize> {
        if ordinal >= self.matches().len() {
            return Err(invariant(ValueViolation::Bounds));
        }
        self.0.input.row(self.start() + ordinal)
    }
}
fn arithmetic() -> ExecutionFailure {
    ConstructionFailure::from(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
    .into()
}

#[path = "bulk_build_hash.rs"]
mod hash;

#[cfg(test)]
#[path = "bulk_build_tests.rs"]
mod tests;

// The neutral author owns every linear barrier and all specialized leases.
// Only K enumerates source pages and hash dependency seeds.
fn begin_phase(
    phases: &PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>,
    phase: Phase,
    shards: usize,
) -> ExecutionResult<()> {
    match phase {
        Phase::Init | Phase::Count | Phase::Prefix | Phase::Fill => {
            phases.begin(shards, NonZeroUsize::new(1).unwrap())
        }
        Phase::Null => phases.begin(1, NonZeroUsize::new(1).unwrap()),
        Phase::Ready | Phase::Published => Ok(()),
        _ => phases.begin_open(),
    }
}
