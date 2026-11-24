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

use super::{
    artifact::{Artifact, KeySpace, Membership},
    contract,
};
use crate::{
    exec::{
        chunk::{ChunkConstructionFailure, ChunkSchema},
        hash_table::key_index::{
            BulkBuildGraph, BulkDomain, BulkInput, BulkWork, CaptureWork, CompleteInput,
            IndexPurpose, KeySpec, SealedCapture,
        },
        operators::local_exchanger::LocalExchanger,
        shared_stage::{
            build_collection::{BeginClaim, BuildCollection},
            funded_shared, funded_vec,
            key_bitmap::{KeyBitmap, KeyBitmapInit},
            stage_completion::{
                PublishResult, RemainingDemand, StageCompletion, StageParticipants, TailStatus,
            },
            tail_ranges::{TailClaim, TailRanges},
        },
    },
    runtime::{
        observable::Observable,
        task::{ExecutionFailure, ExecutionFailureCause, ExecutionResult},
    },
};
use novarocks_functions::KernelFailure;
use novarocks_local_program::{ProgramNodeId, SetOpKind, StaticLayout};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

/// A dormant declaration shared by all actual E/F/Q/T factories.
pub(crate) struct SetOpDeclaration {
    pub(super) node: ProgramNodeId,
    pub(super) layout: StaticLayout,
    pub(super) participants: StageParticipants,
    membership: Membership,
    handoff: Arc<LocalExchanger>,
    prepared: OnceLock<ExecutionResult<Arc<SetOpFrame>>>,
}

impl SetOpDeclaration {
    pub(crate) fn new(
        node: ProgramNodeId,
        layout: StaticLayout,
        kind: SetOpKind,
        probe_inputs: usize,
        participants: StageParticipants,
        handoff: Arc<LocalExchanger>,
    ) -> Self {
        Self {
            node,
            layout,
            participants,
            membership: Membership::new(kind, probe_inputs),
            handoff,
            prepared: OnceLock::new(),
        }
    }

    pub(super) fn prepare(
        &self,
        schema: &ChunkSchema,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Arc<SetOpFrame>> {
        self.prepared
            .get_or_init(|| {
                let checkpoint = || work.control.checkpoint(0).map_err(ExecutionFailure::from);
                let mut types = funded_vec(work.owner, schema.slots().len())?;
                for slot in schema.slots() {
                    let binding = slot.typed_binding().clone_observed(checkpoint)?;
                    types.push(
                        FunctionValueType::try_from_typed_binding(binding)
                            .map_err(ChunkConstructionFailure::from)?,
                    );
                }
                let spec = KeySpec::for_grouping(&types, work.owner, &checkpoint)?;
                let domain = BulkDomain::new(&spec, work.owner, work.control)?;
                let changed = funded_shared(work.owner, Observable::new())?;
                let completion = StageCompletion::new(
                    self.participants,
                    Some(self.participants.tails),
                    Some(self.handoff.clone()),
                    changed,
                    work.owner,
                    work.control,
                    contract,
                )?;
                let inputs = BuildCollection::new(completion.clone(), work.owner, work.control)?;
                funded_shared(
                    work.owner,
                    SetOpFrame {
                        completion,
                        domain,
                        inputs,
                        tails: OnceLock::new(),
                        phase: Mutex::new(Phase::Collecting),
                        membership: self.membership,
                        _storage: work.owner.storage_lifetime(),
                    },
                )
            })
            .clone()
    }
}

pub(super) struct BitmapPhase {
    keys: Arc<KeySpace>,
    pub(super) init: Arc<KeyBitmapInit>,
}

#[derive(Clone)]
pub(super) enum Phase {
    Collecting,
    Keys {
        graph: Arc<BulkBuildGraph>,
        workers_remaining: usize,
    },
    WaitingForKeyWorkers,
    Bitmap(Arc<BitmapPhase>),
    Published,
    Retiring,
    Retired,
    Failed(ExecutionFailure),
}

pub(super) struct SetOpFrame {
    pub(super) completion: Arc<StageCompletion<Artifact>>,
    pub(super) domain: BulkDomain,
    pub(super) inputs: Arc<BuildCollection<SealedCapture, Artifact>>,
    tails: OnceLock<ExecutionResult<Arc<TailRanges<Artifact>>>>,
    phase: Mutex<Phase>,
    membership: Membership,
    _storage: ValueTypeStorageLifetime,
}

impl SetOpFrame {
    pub(super) fn claim_tail(&self, work: &BulkWork<'_>) -> ExecutionResult<TailClaim<Artifact>> {
        match self.completion.tail_status()? {
            TailStatus::Pending => return Ok(TailClaim::Pending),
            TailStatus::Unavailable => return Ok(TailClaim::Unavailable),
            TailStatus::Withdrawn => return Ok(TailClaim::Withdrawn),
            TailStatus::Ready(_) => {}
        }
        let ranges = self
            .tails
            .get_or_init(|| {
                TailRanges::new(
                    self.completion.clone(),
                    super::tail::TAIL_WORK_ROWS,
                    work.owner,
                    work.control,
                )
            })
            .clone()?;
        ranges.claim()
    }

    fn phase(&self) -> ExecutionResult<MutexGuard<'_, Phase>> {
        self.phase.lock().map_err(|poisoned| {
            drop(poisoned.into_inner());
            self.completion
                .fail(contract("set operation finalization state was poisoned"))
        })
    }

    pub(super) fn observe(&self, keys_finished: bool) -> ExecutionResult<Phase> {
        self.completion.check()?;
        let phase = self.phase()?;
        if keys_finished && matches!(*phase, Phase::Keys { .. }) {
            // Workers that settled K must not reacquire its captured inputs.
            return Ok(Phase::WaitingForKeyWorkers);
        }
        Ok(phase.clone())
    }

    pub(super) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let first = self.completion.fail(cause);
        let old = {
            let mut phase = self.phase.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *phase, Phase::Failed(first.clone()))
        };
        match old {
            Phase::Keys { graph, .. } => {
                graph.fail(first.clone());
            }
            Phase::Bitmap(bitmap) => {
                bitmap.init.fail(first.clone());
            }
            _ => {}
        }
        first
    }

    pub(super) fn begin_keys(&self, work: &BulkWork<'_>) -> ExecutionResult<bool> {
        let (mut inputs, mut obligation) =
            match self.inputs.claim_begin(work.owner, work.control)? {
                BeginClaim::Claimed {
                    inputs,
                    begin_obligation,
                } => (inputs, begin_obligation),
                BeginClaim::Pending | BeginClaim::InProgress => return Ok(false),
                BeginClaim::Unavailable => {
                    if self.completion.remaining_demand()? == RemainingDemand::Released {
                        return Ok(false);
                    }
                    return Err(contract(
                        "set operation build withdrew while demand remains",
                    ));
                }
            };
        let result: ExecutionResult<_> = (|| {
            let mut captures = funded_vec(work.owner, inputs.as_slice().len())?;
            while let Some(capture) = inputs.pop() {
                work.control.checkpoint(0)?;
                captures.push(capture);
            }
            let capture_ports = capture_work(work);
            let input = BulkInput::new(&self.domain, captures, &capture_ports)?;
            let input = CompleteInput::from_captures(input, IndexPurpose::KeyIds, &capture_ports)?;
            let graph = BulkBuildGraph::begin(
                input,
                self.completion.participants().finalizers,
                NonZeroUsize::MIN,
                work,
            )?;
            let graph = funded_shared(work.owner, graph)?;
            let mut phase = self.phase()?;
            match &*phase {
                Phase::Collecting => {
                    *phase = Phase::Keys {
                        graph,
                        workers_remaining: self.completion.participants().finalizers.get(),
                    };
                    self.inputs.complete_begin(&mut obligation)?;
                    Ok(true)
                }
                Phase::Retired | Phase::Retiring => {
                    drop(phase);
                    cancel_keys(&graph)?;
                    obligation.withdraw_after_demand_released()?;
                    Ok(false)
                }
                Phase::Failed(cause) => Err(graph.fail(cause.clone())),
                _ => Err(contract(
                    "set operation build lost its installation position",
                )),
            }
        })();
        result.map_err(|cause| obligation.fail(cause))
    }

    /// Settle one installed F after it returned all leases and released its
    /// graph handle. The final worker can then release captured inputs before
    /// constructing the bitmap domain.
    pub(super) fn finish_keys(&self, work: &BulkWork<'_>) -> ExecutionResult<bool> {
        let mut phase = self.phase()?;
        let Phase::Keys {
            workers_remaining, ..
        } = &mut *phase
        else {
            return Ok(false);
        };
        *workers_remaining = workers_remaining
            .checked_sub(1)
            .ok_or_else(|| contract("set operation K worker settled twice"))?;
        if *workers_remaining != 0 {
            return Ok(false);
        }
        let Phase::Keys { graph, .. } = std::mem::replace(&mut *phase, Phase::WaitingForKeyWorkers)
        else {
            unreachable!("owned K phase");
        };
        let graph = Arc::try_unwrap(graph)
            .map_err(|_| contract("set operation still holds a settled K worker handle"))?;
        let ready = graph
            .take_ready(work)?
            .ok_or_else(|| contract("set operation finished K without its original Ready"))?;
        let (view, assignments) = ready.into_parts();
        drop(assignments);
        drop(graph);
        let keys = funded_shared(work.owner, KeySpace::new(view, work)?)?;
        let init = KeyBitmapInit::new(
            self.membership.len(keys.len())?,
            self.completion.participants().finalizers,
            self.completion.observable(),
            work.owner,
            work.control,
            contract,
        )?;
        *phase = Phase::Bitmap(funded_shared(work.owner, BitmapPhase { keys, init })?);
        drop(phase);
        self.completion.finalized_step();
        Ok(true)
    }

    pub(super) fn publish(
        &self,
        bits: KeyBitmap,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<Option<usize>> {
        let mut phase = self.phase()?;
        let published = match &*phase {
            Phase::Bitmap(bitmap) => {
                let artifact = funded_shared(
                    work.owner,
                    Artifact {
                        keys: bitmap.keys.clone(),
                        bits,
                        membership: self.membership,
                    },
                )?;
                let len = artifact.keys.len();
                *phase = match self.inputs.publish(artifact, len)? {
                    PublishResult::Installed => Phase::Published,
                    PublishResult::NotNeeded(_) => Phase::Retired,
                };
                matches!(&*phase, Phase::Published).then_some(len)
            }
            Phase::Retiring | Phase::Retired => None,
            Phase::Failed(cause) => return Err(cause.clone()),
            _ => {
                return Err(contract(
                    "set operation bitmap lost its publication position",
                ));
            }
        };
        drop(phase);
        self.completion.finalized_step();
        Ok(published)
    }

    /// The original Handoff owns the proof that output demand was released.
    /// Seal K with the normal cancellation cause before F drops its cursors.
    pub(super) fn retire(&self) -> ExecutionResult<bool> {
        if self.completion.remaining_demand()? != RemainingDemand::Released {
            return Err(contract(
                "set operation retirement requires released demand",
            ));
        }
        let old = {
            let mut phase = self.phase()?;
            match &*phase {
                Phase::Published | Phase::Retired => return Ok(true),
                Phase::Retiring => return Ok(false),
                Phase::Failed(cause) => return Err(cause.clone()),
                _ => std::mem::replace(&mut *phase, Phase::Retiring),
            }
        };
        match &old {
            Phase::Keys { graph, .. } => cancel_keys(graph)?,
            Phase::Bitmap(bitmap) => bitmap.init.retire()?,
            _ => {}
        }
        drop(old);
        let mut phase = self.phase()?;
        self.completion.check()?;
        *phase = Phase::Retired;
        drop(phase);
        self.completion.finalized_step();
        Ok(true)
    }
}

pub(super) fn capture_work<'a>(work: &'a BulkWork<'_>) -> CaptureWork<'a> {
    CaptureWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: work.host.clone(),
        control: work.control,
    }
}

pub(super) fn is_cancellation(cause: &ExecutionFailure) -> bool {
    matches!(
        cause.cause(),
        ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    )
}

fn cancel_keys(graph: &BulkBuildGraph) -> ExecutionResult<()> {
    let first = graph.fail(KernelFailure::Cancelled.into());
    if is_cancellation(&first) {
        Ok(())
    } else {
        Err(first)
    }
}
