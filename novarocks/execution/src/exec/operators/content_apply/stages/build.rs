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

//! E captures an exact run. F moves sealed runs through the original K graph
//! and publishes net weights only after assignments are complete.

use super::super::*;
use super::{Frame, Host, SealedRun, columns, readable, wait};
use crate::exec::{
    chunk::Chunk,
    hash_table::key_index::{
        BulkBuildGraph, BulkInput, CaptureBuilder, CaptureWork, CompleteInput, EvaluatedPage,
        IndexPurpose,
    },
    pipeline::{
        driver_step::DriverStep,
        step::{NextStep, StepResult},
    },
    shared_stage::{
        build_collection::{BeginClaim, CollectedInputs},
        funded_shared, funded_vec,
        key_build_worker::{KeyBuildWorker, KeyWorkerStep},
        stage_completion::RemainingDemand,
    },
};

pub(super) enum Finalize {
    Collect,
    Build {
        graph: BulkBuildGraph,
        worker: KeyBuildWorker,
        weights: Vec<CapturedWeights>,
    },
    Weights(WeightFinalizer),
    Done,
}

pub(super) fn capture_work<'a>(work: &BulkWork<'a>) -> CaptureWork<'a> {
    CaptureWork {
        owner: work.owner,
        allocator: work.allocator.clone(),
        host: work.host.clone(),
        control: work.control,
    }
}

pub(super) fn capture_input(
    host: &Host,
    writer: &mut CaptureBuilder,
    weights: &mut CapturedWeights,
    chunk: &Chunk,
    conversions: &mut ValueConversionContext,
) -> ExecutionResult<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    let owner = host.owner()?;
    let work = host.work(owner.as_ref())?;
    let spec = host.stages.spec()?;
    let arrays = columns(chunk, &spec.weighted_content, &work)?;
    let weight = chunk
        .column_by_slot_id(spec.weight)
        .map_err(ExecutionFailure::from_domain)?;
    let weight = weight
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| contract("content weights have an invalid carrier"))?;
    let readable = readable(ReachableRows::all(chunk.len()), &work)?;
    let mut occurrences = funded_vec(work.owner, chunk.len())?;
    for row in 0..chunk.len() {
        work.control.checkpoint(0)?;
        occurrences.push(row);
    }
    let page = writer.push_evaluated(
        EvaluatedPage {
            arrays: &arrays,
            bindings: host.prepared()?.frame.domain.spec().types(),
            readable: &readable,
            occurrences: &occurrences,
        },
        conversions,
        &capture_work(&work),
    )?;
    weights.record(page, weight, &work)
}

fn begin_build(
    mut inputs: CollectedInputs<SealedRun>,
    frame: &Frame,
    work: &BulkWork<'_>,
) -> ExecutionResult<Finalize> {
    let mut runs = funded_vec(work.owner, inputs.as_slice().len())?;
    let mut weights = funded_vec(work.owner, inputs.as_slice().len())?;
    while let Some((run, run_weights)) = inputs.pop() {
        work.control.checkpoint(0)?;
        runs.push(run);
        weights.push(run_weights);
    }
    let capture = capture_work(work);
    let complete = CompleteInput::from_captures(
        BulkInput::new(&frame.domain, runs, &capture)?,
        IndexPurpose::KeyIds,
        &capture,
    )?;
    let graph = BulkBuildGraph::begin(complete, NonZeroUsize::MIN, NonZeroUsize::MIN, work)?;
    Ok(Finalize::Build {
        graph,
        worker: KeyBuildWorker::new(work, contract),
        weights,
    })
}

pub(super) fn drive_finalize(
    host: &Host,
    state: &mut Finalize,
    ctx: &mut DriverStep<'_>,
) -> ExecutionResult<StepResult> {
    let frame = &host.prepared()?.frame;
    let observable = frame.completion.observable();
    let generation = observable.generation();
    let owner = host.owner()?;
    let work = host.work(owner.as_ref())?;
    match state {
        Finalize::Collect => {
            let (inputs, mut begin_obligation) =
                match frame.inputs.claim_begin(work.owner, work.control)? {
                    BeginClaim::Claimed {
                        inputs,
                        begin_obligation,
                    } => (inputs, begin_obligation),
                    BeginClaim::Pending => return Ok(wait(observable, generation)),
                    BeginClaim::Unavailable
                        if host.remaining_demand()? == RemainingDemand::Released =>
                    {
                        return Ok(StepResult::new(None, NextStep::Finished, 0));
                    }
                    BeginClaim::Unavailable => {
                        return Err(contract("content build lacks normal capture EOS"));
                    }
                    BeginClaim::InProgress => {
                        return Err(contract("content build already has a finalizer"));
                    }
                };
            *state =
                begin_build(inputs, frame, &work).map_err(|cause| begin_obligation.fail(cause))?;
            frame.inputs.complete_begin(&mut begin_obligation)?;
        }
        Finalize::Build {
            graph,
            worker,
            weights,
        } => match worker.step(graph, &observable, ctx.work(), &work)? {
            KeyWorkerStep::Finished => {
                let ready = graph
                    .take_ready(&work)?
                    .ok_or_else(|| contract("content build finished without its index"))?;
                *state =
                    Finalize::Weights(WeightFinalizer::new(ready, std::mem::take(weights), &work)?);
            }
            KeyWorkerStep::Blocked => return Ok(wait(observable, generation)),
            KeyWorkerStep::Yielded => return Ok(StepResult::new(None, NextStep::Yield, 0)),
            KeyWorkerStep::Progressed => {}
        },
        Finalize::Weights(finalizer) => {
            if finalizer.advance(ctx.work(), &work)? {
                let Finalize::Weights(finalizer) = std::mem::replace(state, Finalize::Done) else {
                    unreachable!()
                };
                let index = finalizer.finish(&work)?;
                let keys = index.len();
                // Demand interests use this one finalized index publication.
                frame
                    .inputs
                    .publish(funded_shared(work.owner, index)?, keys)?;
                return Ok(StepResult::new(None, NextStep::Finished, 0));
            }
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        Finalize::Done => return Ok(StepResult::new(None, NextStep::Finished, 0)),
    }
    Ok(StepResult::new(None, NextStep::Runnable, 0))
}
