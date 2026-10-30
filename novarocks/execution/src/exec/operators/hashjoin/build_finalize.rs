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

//! Concrete E/F join build ownership. E hands sealed receipts to the original
//! collection; F advances the real K/CSR/bitmap phases before data publication.

use super::{
    build_collection::{BeginClaim, BuildCollection},
    build_match_bitmap::{
        BitmapFreeze, BitmapInitClaim, BuildMatchBitmap, BuildMatchBitmapInit, InitLease,
        InitProgress,
    },
    build_pages::{BuildInputReceipt, BuildRows, CapturesForGraphs, ProjectionKind},
    build_projection::{
        IndexedBuild, ProjectionBuild, ProjectionCapture, ProjectionStep, ProjectionWorker,
        ProjectionWorkers,
    },
    build_state::{ProjectionDomains, build_contract},
    join_completion::{
        JoinCompletion, JoinParticipants, PublishResult, RemainingDemand, funded_shared,
    },
};
use crate::{
    exec::{
        hash_table::key_index::{BulkWork, IndexPurpose},
        operators::local_exchanger::LocalExchanger,
        pipeline::step_context::TurnWork,
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        observable::{Observable, ObserverSubscription},
    },
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};

pub(crate) enum BuildLookup {
    Ordinary(Arc<IndexedBuild>),
    Naa {
        group: Option<Arc<IndexedBuild>>,
        full: Arc<IndexedBuild>,
    },
}
/// Data-only artifact: no plan, factory, driver, finalizer or completion owner.
pub(crate) struct FinalizedBuild {
    pub(crate) rows: Arc<BuildRows>,
    pub(crate) lookup: BuildLookup,
    pub(crate) matches: Option<Arc<BuildMatchBitmap>>,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) struct FinalizeOptions {
    pub(crate) participants: JoinParticipants,
    pub(crate) handoff: Option<Arc<LocalExchanger>>,
    /// Existing K live-sketch preference. It changes partial aggregation,
    /// never input admissibility or the original driver's CPU allowance.
    pub(crate) live_slots: NonZeroUsize,
    pub(crate) match_bits: bool,
}
struct ProjectionSlot {
    build: Arc<ProjectionBuild>,
    ready: OnceLock<Arc<IndexedBuild>>,
}
impl ProjectionSlot {
    fn new(build: Arc<ProjectionBuild>) -> Self {
        Self {
            build,
            ready: OnceLock::new(),
        }
    }
    fn seal(&self, work: &BulkWork<'_>) -> ExecutionResult<bool> {
        if let Some(data) = self.build.take_ready()? {
            let data = funded_shared(work.owner, data)?;
            self.ready
                .set(data)
                .map_err(|_| build_contract("join projection data was installed twice"))?;
            return Ok(true);
        }
        Ok(false)
    }
}
enum Projections {
    Ordinary(ProjectionSlot),
    Naa {
        group: Option<ProjectionSlot>,
        full: ProjectionSlot,
    },
}
impl Projections {
    fn slot(&self, lane: usize) -> Option<&ProjectionSlot> {
        match (self, lane) {
            (Self::Ordinary(slot), 0) | (Self::Naa { full: slot, .. }, 0) => Some(slot),
            (Self::Naa { group, .. }, 1) => group.as_ref(),
            _ => None,
        }
    }
    fn ready(&self) -> Option<BuildLookup> {
        match self {
            Self::Ordinary(slot) => Some(BuildLookup::Ordinary(Arc::clone(slot.ready.get()?))),
            Self::Naa { group, full } => {
                let group = match group {
                    Some(slot) => Some(Arc::clone(slot.ready.get()?)),
                    None => None,
                };
                Some(BuildLookup::Naa {
                    group,
                    full: Arc::clone(full.ready.get()?),
                })
            }
        }
    }
}
struct ActiveBuild {
    rows: Arc<BuildRows>,
    projections: Projections,
    bitmap: Option<Arc<BuildMatchBitmapInit>>,
    ready_bitmap: OnceLock<Arc<BuildMatchBitmap>>,
    _storage: ValueTypeStorageLifetime,
}
impl ActiveBuild {
    fn retire(&self) -> ExecutionResult<()> {
        for lane in 0..2 {
            if let Some(slot) = self.projections.slot(lane) {
                slot.build.retire()?;
            }
        }
        if let Some(bitmap) = &self.bitmap {
            bitmap.retire()?;
        }
        Ok(())
    }
    fn fail(&self, cause: &ExecutionFailure) {
        for lane in 0..2 {
            if let Some(slot) = self.projections.slot(lane) {
                slot.build.fail(cause.clone());
            }
        }
        if let Some(bitmap) = &self.bitmap {
            bitmap.fail(cause.clone());
        }
    }
}
enum State {
    Collecting,
    Installing(Arc<ActiveBuild>),
    Active(Arc<ActiveBuild>),
    Publishing,
    Published,
    Retiring,
    Retired,
    Failed(ExecutionFailure),
}
pub(crate) struct JoinBuildFrame {
    pub(crate) domains: ProjectionDomains,
    pub(crate) completion: Arc<JoinCompletion<FinalizedBuild>>,
    pub(crate) inputs: Arc<BuildCollection<BuildInputReceipt, FinalizedBuild>>,
    state: Mutex<State>,
    live_slots: NonZeroUsize,
    match_bits: bool,
    activation: OnceLock<ExecutionResult<Option<ObserverSubscription>>>,
    _storage: ValueTypeStorageLifetime,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FinalizeStep {
    Finished,
    Progressed,
    Blocked,
    Yielded,
    Retired,
}
pub(crate) struct FinalizeWorker {
    projections: [Option<ProjectionWorker>; 2],
    bitmap: Option<InitLease>,
    next: usize,
    active: Option<Arc<ActiveBuild>>,
}
impl JoinBuildFrame {
    pub(crate) fn new(
        domains: ProjectionDomains,
        options: FinalizeOptions,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        let changed = funded_shared(work.owner, Observable::new())?;
        let completion = JoinCompletion::new(
            options.participants,
            options.handoff,
            Arc::clone(&changed),
            work.owner,
            work.control,
        )?;
        let inputs = BuildCollection::new(Arc::clone(&completion), work.owner, work.control)?;
        funded_shared(
            work.owner,
            Self {
                domains,
                completion,
                inputs,
                state: Mutex::new(State::Collecting),
                live_slots: options.live_slots,
                match_bits: options.match_bits,
                activation: OnceLock::new(),
                _storage: work.owner.storage_lifetime(),
            },
        )
    }
    /// Registration belongs to admitted operator activation. Preparation
    /// creates only dormant metadata, never a live subscription.
    pub(crate) fn activate(&self, work: &BulkWork<'_>) -> ExecutionResult<()> {
        self.activation
            .get_or_init(|| {
                let Some(closed) = self.completion.handoff_closed_observable() else {
                    return Ok(None);
                };
                let changed = self.completion.observable();
                let weak = Arc::downgrade(&changed);
                let retention = work.owner.storage_lifetime();
                let callback = funded_shared(work.owner, move || {
                    let _retention = &retention;
                    if let Some(changed) = weak.upgrade() {
                        changed.notify_observers();
                    }
                })?;
                Ok(Some(closed.subscribe(callback)))
            })
            .as_ref()
            .map(|_| ())
            .map_err(Clone::clone)
    }
    fn state(&self) -> ExecutionResult<std::sync::MutexGuard<'_, State>> {
        self.state.lock().map_err(|p| {
            drop(p.into_inner());
            self.completion
                .fail(build_contract("join finalization state was poisoned"))
        })
    }
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let first = self.completion.fail(cause);
        let old = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *state, State::Failed(first.clone()))
        };
        if let State::Active(active) | State::Installing(active) = &old {
            active.fail(&first);
        }
        drop(old);
        first
    }
    /// Fully retire nested owners before exposing Retired to source cleanup.
    pub(crate) fn retire(&self) -> ExecutionResult<FinalizeStep> {
        self.completion.check()?;
        if self.completion.remaining_demand()? != RemainingDemand::Released {
            return Err(self.fail(build_contract(
                "join finalization retired while output demand remains",
            )));
        }
        let old = {
            let mut state = self.state()?;
            match &*state {
                State::Failed(cause) => return Err(cause.clone()),
                State::Retired => return Ok(FinalizeStep::Retired),
                State::Retiring => return Ok(FinalizeStep::Blocked),
                _ => {}
            }
            std::mem::replace(&mut *state, State::Retiring)
        };
        let result = (|| {
            if let State::Active(active) | State::Installing(active) = &old {
                active.retire()?;
            }
            drop(old);
            let mut state = self.state()?;
            if let State::Failed(cause) = &*state {
                return Err(cause.clone());
            }
            if !matches!(&*state, State::Retiring) {
                return Err(build_contract(
                    "join finalization retirement lost its original owner",
                ));
            }
            *state = State::Retired;
            drop(state);
            self.completion.finalized_step();
            Ok(FinalizeStep::Retired)
        })();
        result.map_err(|cause| self.fail(cause))
    }
    fn begin(&self, work: &BulkWork<'_>) -> ExecutionResult<FinalizeStep> {
        let BeginClaim::Claimed { inputs, mut lease } =
            self.inputs.claim_begin(work.owner, work.control)?
        else {
            return Ok(FinalizeStep::Blocked);
        };
        let result = (|| {
            let (rows, captures) = BuildRows::from_inputs(inputs, &self.domains, work)?;
            let workers = self.completion.participants().finalizers;
            let projection_workers = || ProjectionWorkers {
                positions: workers,
                live_slots: self.live_slots,
                changed: self.completion.observable(),
            };
            let make = |capture, domain, purpose, projection| {
                ProjectionBuild::begin(
                    ProjectionCapture {
                        capture,
                        domain,
                        purpose,
                        rows: Arc::clone(&rows),
                        projection,
                    },
                    projection_workers(),
                    work,
                )
                .map(ProjectionSlot::new)
            };
            let projections = match (&self.domains, captures) {
                (
                    ProjectionDomains::Ordinary {
                        keys: domain,
                        purpose,
                    },
                    CapturesForGraphs::Ordinary { keys },
                ) => Projections::Ordinary(make(keys, domain, *purpose, ProjectionKind::Ordinary)?),
                (
                    ProjectionDomains::Naa {
                        group: group_domain,
                        full: full_domain,
                        full_purpose,
                        ..
                    },
                    CapturesForGraphs::Naa { group, full },
                ) => {
                    let group = match (group, group_domain) {
                        (Some(capture), Some(domain)) => Some(make(
                            capture,
                            domain,
                            IndexPurpose::KeyIds,
                            ProjectionKind::Group,
                        )?),
                        (None, None) => None,
                        _ => {
                            return Err(build_contract(
                                "join scope capture differs from its frozen domain",
                            ));
                        }
                    };
                    Projections::Naa {
                        group,
                        full: make(full, full_domain, *full_purpose, ProjectionKind::Full)?,
                    }
                }
                _ => {
                    return Err(build_contract(
                        "join capture kind differs from its frozen domains",
                    ));
                }
            };
            let bitmap = self
                .match_bits
                .then(|| {
                    BuildMatchBitmapInit::new(
                        rows.len(),
                        workers,
                        self.completion.observable(),
                        work.owner,
                        work.control,
                    )
                })
                .transpose()?;
            let active = funded_shared(
                work.owner,
                ActiveBuild {
                    rows,
                    projections,
                    bitmap,
                    ready_bitmap: OnceLock::new(),
                    _storage: work.owner.storage_lifetime(),
                },
            )?;
            {
                let mut state = self.state()?;
                match &*state {
                    State::Collecting => {
                        *state = State::Installing(Arc::clone(&active));
                    }
                    State::Retiring | State::Retired => {
                        drop(state);
                        active.retire()?;
                        lease.withdraw_after_demand_released()?;
                        return Ok(FinalizeStep::Retired);
                    }
                    State::Failed(cause) => return Err(cause.clone()),
                    _ => {
                        return Err(build_contract(
                            "join begin has no original installation position",
                        ));
                    }
                }
            }
            self.inputs.complete_begin(&mut lease)?;
            let mut state = self.state()?;
            match &*state {
                State::Installing(current) if Arc::ptr_eq(current, &active) => {
                    *state = State::Active(active);
                }
                State::Retiring | State::Retired => return Ok(FinalizeStep::Retired),
                State::Failed(cause) => return Err(cause.clone()),
                _ => {
                    return Err(build_contract(
                        "join installed graph lost its begin position",
                    ));
                }
            }
            drop(state);
            self.completion.finalized_step();
            Ok(FinalizeStep::Progressed)
        })();
        result.map_err(|cause| lease.fail(cause))
    }
    fn publish(
        &self,
        active: &Arc<ActiveBuild>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<FinalizeStep> {
        let Some(lookup) = active.projections.ready() else {
            return Ok(FinalizeStep::Blocked);
        };
        let matches = if active.bitmap.is_some() {
            let Some(bitmap) = active.ready_bitmap.get() else {
                return Ok(FinalizeStep::Blocked);
            };
            Some(Arc::clone(bitmap))
        } else {
            None
        };
        {
            let mut state = self.state()?;
            match &*state {
                State::Active(current) if Arc::ptr_eq(current, active) => {
                    *state = State::Publishing;
                }
                State::Published => return Ok(FinalizeStep::Finished),
                State::Retiring | State::Retired => return Ok(FinalizeStep::Retired),
                State::Failed(cause) => return Err(cause.clone()),
                _ => return Ok(FinalizeStep::Blocked),
            }
        }
        let data = funded_shared(
            work.owner,
            FinalizedBuild {
                rows: Arc::clone(&active.rows),
                lookup,
                matches,
                _storage: work.owner.storage_lifetime(),
            },
        )?;
        let publication = self.inputs.publish(data, active.rows.len())?;
        let next = match publication {
            PublishResult::Installed => State::Published,
            PublishResult::NotNeeded(data) => {
                drop(data);
                State::Retired
            }
        };
        let mut state = self.state()?;
        match &*state {
            State::Publishing => *state = next,
            State::Failed(cause) => return Err(cause.clone()),
            State::Retiring | State::Retired => return Ok(FinalizeStep::Retired),
            _ => {
                return Err(build_contract(
                    "join publication lost its original position",
                ));
            }
        }
        let result = if matches!(&*state, State::Published) {
            FinalizeStep::Finished
        } else {
            FinalizeStep::Retired
        };
        drop(state);
        self.completion.finalized_step();
        Ok(result)
    }
}
impl FinalizeWorker {
    pub(crate) fn new() -> Self {
        Self {
            projections: [None, None],
            bitmap: None,
            next: 0,
            active: None,
        }
    }
    /// Retain exact nested cursor owners through synchronous exit. Once output
    /// is withdrawn this uses the existing normal retirement facts, no CPU grant.
    fn retire_owned(&mut self, active: &ActiveBuild) -> ExecutionResult<()> {
        for lane in 0..2 {
            if let (Some(worker), Some(slot)) =
                (&mut self.projections[lane], active.projections.slot(lane))
            {
                worker.retire_owned(&slot.build)?;
            }
        }
        if let Some(mut lease) = self.bitmap.take() {
            lease.retire()?;
        }
        Ok(())
    }
    fn retired(&mut self, frame: &JoinBuildFrame) -> ExecutionResult<FinalizeStep> {
        let next = frame.retire()?;
        if next == FinalizeStep::Retired {
            if let Some(active) = self.active.as_ref().cloned() {
                self.retire_owned(&active)?;
            }
            self.active = None;
        }
        Ok(next)
    }
    /// Real synchronous exit, independent of output EOS. A caller with an
    /// already recorded failure drops the worker only after fail() latched it.
    pub(crate) fn finish_exit(&mut self, frame: &JoinBuildFrame) -> ExecutionResult<bool> {
        frame.completion.check()?;
        if frame.completion.remaining_demand()? == RemainingDemand::Released {
            return self
                .retired(frame)
                .map(|next| next == FinalizeStep::Retired);
        }
        if frame.completion.published()?.is_some() {
            self.active = None;
            self.projections = [None, None];
            self.bitmap = None;
            return Ok(true);
        }
        Err(build_contract(
            "join finalizer exited before its required build publication",
        ))
    }
    pub(crate) fn step(
        &mut self,
        frame: &JoinBuildFrame,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<FinalizeStep> {
        self.step_inner(frame, turn, work)
            .map_err(|cause| frame.fail(cause))
    }
    fn step_inner(
        &mut self,
        frame: &JoinBuildFrame,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<FinalizeStep> {
        frame.completion.check()?;
        work.control.checkpoint(0)?;
        let active = {
            let state = frame.state()?;
            match &*state {
                State::Active(active) => {
                    self.active = Some(Arc::clone(active));
                    Some(Arc::clone(active))
                }
                State::Published => {
                    drop(state);
                    self.active = None;
                    return Ok(FinalizeStep::Finished);
                }
                State::Retired => {
                    drop(state);
                    return self.retired(frame);
                }
                State::Failed(cause) => return Err(cause.clone()),
                _ => None,
            }
        };
        if frame.completion.remaining_demand()? == RemainingDemand::Released {
            return self.retired(frame);
        }
        if turn.remaining() == 0 {
            return Ok(FinalizeStep::Yielded);
        }
        let Some(active) = active else {
            let collecting = matches!(&*frame.state()?, State::Collecting);
            if collecting {
                if !turn.try_consume(1) {
                    return Ok(FinalizeStep::Yielded);
                }
                return frame.begin(work);
            }
            return Ok(FinalizeStep::Blocked);
        };
        let first = self.next;
        for offset in 0..3 {
            let lane = (first + offset) % 3;
            if lane < 2 {
                let Some(slot) = active.projections.slot(lane) else {
                    continue;
                };
                if slot.ready.get().is_some() {
                    continue;
                }
                let worker =
                    self.projections[lane].get_or_insert_with(|| ProjectionWorker::new(work));
                let step = worker.step(&slot.build, turn, work)?;
                self.next = (lane + 1) % 3;
                match step {
                    ProjectionStep::Finished => {
                        if slot.seal(work)? {
                            frame.completion.finalized_step();
                            return Ok(FinalizeStep::Progressed);
                        }
                        continue;
                    }
                    ProjectionStep::Progressed => return Ok(FinalizeStep::Progressed),
                    ProjectionStep::Blocked => continue,
                    ProjectionStep::Yielded => return Ok(FinalizeStep::Yielded),
                    ProjectionStep::Retired => return self.retired(frame),
                }
            }
            if let Some(init) = &active.bitmap {
                if active.ready_bitmap.get().is_some() {
                    continue;
                }
                if self.bitmap.is_none() {
                    if !turn.try_consume(1) {
                        return Ok(FinalizeStep::Yielded);
                    }
                    match init.claim(work.owner, work.control)? {
                        BitmapInitClaim::Range(lease) => self.bitmap = Some(lease),
                        BitmapInitClaim::Pending => continue,
                        BitmapInitClaim::Retired => return self.retired(frame),
                        BitmapInitClaim::Finished => match init.try_freeze()? {
                            BitmapFreeze::Ready(bitmap) => {
                                let bitmap = funded_shared(work.owner, bitmap)?;
                                active.ready_bitmap.set(bitmap).map_err(|_| {
                                    build_contract("join bitmap data was installed twice")
                                })?;
                                frame.completion.finalized_step();
                                return Ok(FinalizeStep::Progressed);
                            }
                            BitmapFreeze::Retired => return self.retired(frame),
                            BitmapFreeze::Pending | BitmapFreeze::Frozen => continue,
                        },
                    }
                }
                let quota = turn.remaining();
                if quota == 0 {
                    return Ok(FinalizeStep::Yielded);
                }
                if !turn.try_consume(quota) {
                    return Err(build_contract(
                        "join bitmap lost its original turn allowance",
                    ));
                }
                let lease = self.bitmap.as_mut().expect("original bitmap lease");
                let progress = lease.initialize(quota as usize, work.control)?;
                self.next = 0;
                return match progress {
                    InitProgress::Initialized => {
                        let next = lease.settle()?;
                        self.bitmap = None;
                        if next == InitProgress::Retired {
                            self.retired(frame)
                        } else {
                            Ok(FinalizeStep::Progressed)
                        }
                    }
                    InitProgress::Progressed => Ok(FinalizeStep::Progressed),
                    InitProgress::Yielded => Ok(FinalizeStep::Yielded),
                    InitProgress::Retired => {
                        lease.retire()?;
                        self.bitmap = None;
                        self.retired(frame)
                    }
                };
            }
        }
        if !turn.try_consume(1) {
            return Ok(FinalizeStep::Yielded);
        }
        frame.publish(&active, work)
    }
}

#[cfg(test)]
#[path = "build_finalize_tests.rs"]
mod tests;
