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

//! One captured K projection and its join-owned CSR phases. F workers retain
//! their original cursors; publication consumes actual K Ready and CSR barriers.

use super::{
    build_csr::{BuildCsr, CsrBuilder, CsrClaim, CsrLease, CsrProgress},
    build_pages::{BuildRows, CapturedInputRun, ProjectionKind},
    build_state::build_contract,
    csr_slots::{SlotsLease, SlotsProgress},
    join_completion::funded_shared,
    key_build_worker::{KeyBuildWorker, KeyWorkerStep},
};
use crate::{
    exec::{
        hash_table::key_index::{BulkDomain, BulkTaskGraph, BulkWork, IndexPurpose, IndexView},
        pipeline::step_context::TurnWork,
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionFailureCause, ExecutionResult},
        observable::Observable,
    },
};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// Data only. It owns neither a finalizer nor its input captures/assignments.
pub(crate) struct IndexedBuild {
    pub(crate) view: Arc<IndexView>,
    pub(crate) groups: Option<BuildCsr>,
}
enum Phase {
    Keys(Arc<BulkTaskGraph>),
    Csr {
        view: Arc<IndexView>,
        build: Arc<CsrBuilder>,
    },
    Ready(Option<IndexedBuild>),
    Transition,
    Retiring,
    Retired,
    Failed(ExecutionFailure),
}
pub(crate) struct ProjectionCapture<'a> {
    pub(crate) capture: CapturedInputRun,
    pub(crate) domain: &'a BulkDomain,
    pub(crate) purpose: IndexPurpose,
    pub(crate) rows: Arc<BuildRows>,
    pub(crate) projection: ProjectionKind,
}
pub(crate) struct ProjectionWorkers {
    pub(crate) positions: NonZeroUsize,
    pub(crate) live_slots: NonZeroUsize,
    pub(crate) changed: Arc<Observable>,
}
pub(crate) struct ProjectionBuild {
    phase: Mutex<Phase>,
    rows: Arc<BuildRows>,
    projection: ProjectionKind,
    workers: NonZeroUsize,
    changed: Arc<Observable>,
    _storage: ValueTypeStorageLifetime,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionStep {
    Finished,
    Progressed,
    Blocked,
    Yielded,
    Retired,
}
pub(crate) struct ProjectionWorker {
    keys: KeyBuildWorker,
    csr: Option<CsrWork>,
}
enum CsrWork {
    Initialize(SlotsLease),
    Range(CsrLease),
}
impl ProjectionBuild {
    /// Caller is the original sole begin-lease author. All input runs must
    /// already be sealed; construction below does not declare E completion.
    pub(crate) fn begin(
        input: ProjectionCapture<'_>,
        workers: ProjectionWorkers,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        let ProjectionCapture {
            capture,
            domain,
            purpose,
            rows,
            projection,
        } = input;
        let ProjectionWorkers {
            positions,
            live_slots,
            changed,
        } = workers;
        let graph = BulkTaskGraph::begin(
            capture.into_complete(domain, purpose, work)?,
            positions,
            live_slots,
            work,
        )?;
        Self::from_graph(graph, rows, projection, positions, changed, work)
    }
    fn from_graph(
        graph: BulkTaskGraph,
        rows: Arc<BuildRows>,
        projection: ProjectionKind,
        workers: NonZeroUsize,
        changed: Arc<Observable>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Arc<Self>> {
        let graph = funded_shared(work.owner, graph)?;
        funded_shared(
            work.owner,
            Self {
                phase: Mutex::new(Phase::Keys(graph)),
                rows,
                projection,
                workers,
                changed,
                _storage: work.owner.storage_lifetime(),
            },
        )
    }
    fn phase(&self) -> ExecutionResult<std::sync::MutexGuard<'_, Phase>> {
        self.phase.lock().map_err(|p| {
            drop(p.into_inner());
            build_contract("join projection phase was poisoned")
        })
    }
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let (old, cause) = {
            let mut phase = self.phase.lock().unwrap_or_else(|p| p.into_inner());
            if let Phase::Failed(first) = &*phase {
                return first.clone();
            }
            (
                std::mem::replace(&mut *phase, Phase::Failed(cause.clone())),
                cause,
            )
        };
        match &old {
            Phase::Keys(graph) => {
                graph.fail(cause.clone());
            }
            Phase::Csr { build, .. } => {
                build.fail(cause.clone());
            }
            _ => {}
        }
        drop(old);
        self.changed.notify_observers();
        cause
    }
    /// Only the caller's original global-demand proof authorizes withdrawal.
    pub(crate) fn retire(&self) -> ExecutionResult<()> {
        let old = {
            let mut phase = self.phase()?;
            if let Phase::Failed(cause) = &*phase {
                return Err(cause.clone());
            }
            if matches!(&*phase, Phase::Retiring | Phase::Retired) {
                return Ok(());
            }
            std::mem::replace(&mut *phase, Phase::Retiring)
        };
        match &old {
            Phase::Keys(graph) => {
                // Global demand withdrawal cancels these original K cursors.
                // Latch that actual control cause before their lease drops;
                // an intentional withdrawal is not an abandoned-work defect.
                let first = graph.fail(KernelFailure::Cancelled.into());
                if !matches!(
                    first.cause(),
                    ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
                ) {
                    return Err(self.fail(first));
                }
            }
            Phase::Csr { build, .. } => build.retire()?,
            _ => {}
        }
        drop(old);
        {
            let mut phase = self.phase()?;
            if let Phase::Failed(cause) = &*phase {
                return Err(cause.clone());
            }
            if !matches!(&*phase, Phase::Retiring) {
                return Err(build_contract(
                    "join projection retirement lost its original owner",
                ));
            }
            *phase = Phase::Retired;
        }
        self.changed.notify_observers();
        Ok(())
    }
    pub(crate) fn take_ready(&self) -> ExecutionResult<Option<IndexedBuild>> {
        let mut phase = self.phase()?;
        match &mut *phase {
            Phase::Ready(value) => Ok(value.take()),
            Phase::Failed(cause) => Err(cause.clone()),
            _ => Ok(None),
        }
    }
    fn install(&self, next: Phase) -> ExecutionResult<ProjectionStep> {
        let mut phase = self.phase()?;
        match &*phase {
            Phase::Failed(cause) => Err(cause.clone()),
            Phase::Retiring | Phase::Retired => {
                drop(phase);
                if let Phase::Csr { build, .. } = &next {
                    build.retire()?;
                }
                drop(next);
                Ok(ProjectionStep::Retired)
            }
            Phase::Transition => {
                *phase = next;
                drop(phase);
                self.changed.notify_observers();
                Ok(ProjectionStep::Progressed)
            }
            _ => Err(build_contract(
                "join projection transition lost its original owner",
            )),
        }
    }
    fn keys_ready(
        &self,
        graph: &Arc<BulkTaskGraph>,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<ProjectionStep> {
        {
            let mut phase = self.phase()?;
            match &*phase {
                Phase::Keys(current) if Arc::ptr_eq(current, graph) => {
                    *phase = Phase::Transition;
                }
                Phase::Failed(cause) => return Err(cause.clone()),
                Phase::Retiring | Phase::Retired => return Ok(ProjectionStep::Retired),
                _ => return Ok(ProjectionStep::Progressed),
            }
        }
        let ready = graph
            .take_ready(work)?
            .ok_or_else(|| build_contract("original K graph finished without its unique Ready"))?;
        let (view, assignments) = ready.into_parts();
        let next = if view.purpose() == IndexPurpose::PresenceOnly {
            drop(assignments);
            Phase::Ready(Some(IndexedBuild { view, groups: None }))
        } else {
            let build = CsrBuilder::new(
                &view,
                assignments,
                Arc::clone(&self.rows),
                self.projection,
                self.workers,
                Arc::clone(&self.changed),
                work,
            )?;
            Phase::Csr { view, build }
        };
        self.install(next)
    }
    fn csr_ready(&self, build: &Arc<CsrBuilder>) -> ExecutionResult<ProjectionStep> {
        let view = {
            let mut phase = self.phase()?;
            match &*phase {
                Phase::Csr {
                    view,
                    build: current,
                } if Arc::ptr_eq(current, build) => {
                    let view = Arc::clone(view);
                    *phase = Phase::Transition;
                    view
                }
                Phase::Failed(cause) => return Err(cause.clone()),
                Phase::Retiring | Phase::Retired => return Ok(ProjectionStep::Retired),
                _ => return Ok(ProjectionStep::Progressed),
            }
        };
        let groups = build
            .take_ready()?
            .ok_or_else(|| build_contract("CSR barrier finished without its unique data"))?;
        self.install(Phase::Ready(Some(IndexedBuild {
            view,
            groups: Some(groups),
        })))
    }
}
impl ProjectionWorker {
    pub(crate) fn new(work: &BulkWork<'_>) -> Self {
        Self {
            keys: KeyBuildWorker::new(work),
            csr: None,
        }
    }
    /// Synchronous lifecycle retirement needs no new CPU/control capability.
    /// The original global demand author must have fully retired this build.
    pub(crate) fn retire_owned(&mut self, build: &ProjectionBuild) -> ExecutionResult<()> {
        let phase = build.phase()?;
        if let Phase::Failed(cause) = &*phase {
            return Err(cause.clone());
        }
        if !matches!(&*phase, Phase::Retired) {
            return Err(build_contract(
                "join worker retired before the original projection",
            ));
        }
        drop(phase);
        if let Some(mut pending) = self.csr.take() {
            match &mut pending {
                CsrWork::Initialize(lease) => lease.retire()?,
                CsrWork::Range(lease) => lease.retire()?,
            }
        }
        self.keys.release_retired();
        Ok(())
    }
    pub(crate) fn step(
        &mut self,
        build: &ProjectionBuild,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<ProjectionStep> {
        self.step_inner(build, turn, work)
            .map_err(|cause| build.fail(cause))
    }
    fn step_inner(
        &mut self,
        build: &ProjectionBuild,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<ProjectionStep> {
        work.control.checkpoint(0)?;
        let (graph, csr) = {
            let phase = build.phase()?;
            match &*phase {
                Phase::Keys(graph) => (Some(Arc::clone(graph)), None),
                Phase::Csr { build, .. } => (None, Some(Arc::clone(build))),
                Phase::Ready(_) => return Ok(ProjectionStep::Finished),
                Phase::Transition | Phase::Retiring => {
                    return Ok(if turn.remaining() == 0 {
                        ProjectionStep::Yielded
                    } else {
                        ProjectionStep::Blocked
                    });
                }
                Phase::Failed(cause) => return Err(cause.clone()),
                Phase::Retired => {
                    drop(phase);
                    if let Some(mut pending) = self.csr.take() {
                        match &mut pending {
                            CsrWork::Initialize(lease) => lease.retire()?,
                            CsrWork::Range(lease) => lease.retire()?,
                        }
                    }
                    return Ok(ProjectionStep::Retired);
                }
            }
        };
        if let Some(graph) = graph {
            return match self.keys.step(&graph, &build.changed, turn, work)? {
                KeyWorkerStep::Finished => build.keys_ready(&graph, work),
                KeyWorkerStep::Progressed => Ok(ProjectionStep::Progressed),
                KeyWorkerStep::Blocked => Ok(ProjectionStep::Blocked),
                KeyWorkerStep::Yielded => Ok(ProjectionStep::Yielded),
            };
        }
        let csr = csr.expect("original phase has K or CSR work");
        if self.csr.is_none() {
            if !turn.try_consume(1) {
                return Ok(ProjectionStep::Yielded);
            }
            match csr.claim(work)? {
                CsrClaim::Initialize(lease) => self.csr = Some(CsrWork::Initialize(lease)),
                CsrClaim::Work(lease) => self.csr = Some(CsrWork::Range(lease)),
                CsrClaim::Ready => return build.csr_ready(&csr),
                CsrClaim::Progressed => return Ok(ProjectionStep::Progressed),
                CsrClaim::Pending => return Ok(ProjectionStep::Blocked),
                CsrClaim::Retired => return Ok(ProjectionStep::Retired),
            }
        }
        let quota = turn.remaining();
        if quota == 0 {
            return Ok(ProjectionStep::Yielded);
        }
        if !turn.try_consume(quota) {
            return Err(build_contract(
                "join projection lost its original turn allowance",
            ));
        }
        match self.csr.as_mut().expect("retained original CSR lease") {
            CsrWork::Initialize(lease) => match lease.initialize(quota as usize, work.control)? {
                SlotsProgress::Initialized => {
                    let settled = lease.settle()?;
                    self.csr = None;
                    Ok(if settled == SlotsProgress::Retired {
                        ProjectionStep::Retired
                    } else {
                        ProjectionStep::Progressed
                    })
                }
                SlotsProgress::Progressed => Ok(ProjectionStep::Progressed),
                SlotsProgress::Yielded => Ok(ProjectionStep::Yielded),
                SlotsProgress::Retired => {
                    lease.retire()?;
                    self.csr = None;
                    Ok(ProjectionStep::Retired)
                }
            },
            CsrWork::Range(lease) => match lease.advance(quota as usize, work)? {
                CsrProgress::Done => {
                    let settled = lease.settle()?;
                    self.csr = None;
                    Ok(if settled == CsrProgress::Retired {
                        ProjectionStep::Retired
                    } else {
                        ProjectionStep::Progressed
                    })
                }
                CsrProgress::Progressed => Ok(ProjectionStep::Progressed),
                CsrProgress::Yielded => Ok(ProjectionStep::Yielded),
                CsrProgress::Retired => {
                    lease.retire()?;
                    self.csr = None;
                    Ok(ProjectionStep::Retired)
                }
            },
        }
    }
}

#[cfg(test)]
#[path = "build_projection_tests.rs"]
mod tests;
