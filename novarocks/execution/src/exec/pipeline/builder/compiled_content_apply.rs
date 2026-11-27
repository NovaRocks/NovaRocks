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

use super::*;
use crate::exec::operators::NoopSinkFactory;
use crate::exec::operators::content_apply::{
    ContentApplyFactory, ContentApplyPhase, ContentApplyStages,
};
use crate::exec::pipeline::operator::ConstructionFailure;
use crate::exec::shared_stage::{arithmetic, stage_completion::StageParticipants};
use novarocks_local_program::StaticSummaryAuthor;
use novarocks_result_contract::SummaryKind;
use std::num::NonZeroUsize;

pub(super) fn assemble(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    mut weighted: PipelineBuildResult,
    mut target: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
) -> ExecutionResult<PipelineBuildResult> {
    let positions = |dop| {
        usize::try_from(dop)
            .ok()
            .and_then(NonZeroUsize::new)
            .map(NonZeroUsize::get)
            .ok_or_else(|| {
                program_failure(
                    FragmentBindingErrorKind::InvalidAssignment,
                    "content apply stage has no installed drivers",
                )
            })
    };
    let participants = StageParticipants {
        inputs: positions(weighted.pipeline.dop)?,
        // One cooperative finalizer owns the original index cursor and net reduction.
        finalizers: NonZeroUsize::MIN,
        probes: positions(target.pipeline.dop)?,
        tails: positions(ctx.pipeline_dop)?,
    };
    let tracker = ctx
        .task_tracker
        .clone()
        .ok_or(ConstructionFailure::MissingOwner)?;
    let summary = ctx
        .summary_authors
        .claim(
            SummaryKind::DeleteVerification,
            StaticSummaryAuthor::Node(id),
        )?
        .declare_units(participants.tails, tracker.clone())?;
    let producers = participants
        .probes
        .checked_add(participants.tails)
        .ok_or_else(arithmetic)?;
    let handoff =
        LocalExchanger::new_handoff(producers, participants.probes, ctx.operator_buffer_chunks);
    let stages = Arc::new(ContentApplyStages::new(
        program.clone(),
        id,
        participants,
        handoff.clone(),
        summary,
        tracker,
        ctx.output_batch_rows,
        error.clone(),
    ));
    let factory = |phase| Box::new(ContentApplyFactory::new(stages.clone(), phase));
    weighted
        .pipeline
        .factories
        .push(factory(ContentApplyPhase::Capture));
    weighted.pipeline.needs_sink = false;
    target
        .pipeline
        .factories
        .push(factory(ContentApplyPhase::Probe));
    target
        .pipeline
        .factories
        .push(Box::new(LocalExchangeSinkFactory::new(
            node_id,
            handoff.clone(),
        )));
    target.pipeline.needs_sink = false;
    let mut finalize = new_source_pipeline_with_dop(ctx, factory(ContentApplyPhase::Finalize), 1);
    finalize.factories.push(Box::new(NoopSinkFactory::new()));
    finalize.needs_sink = false;
    let tail_dop = ctx.pipeline_dop;
    let mut tail = new_source_pipeline_with_dop(ctx, factory(ContentApplyPhase::Tail), tail_dop);
    tail.factories.push(Box::new(LocalExchangeSinkFactory::new(
        node_id,
        handoff.clone(),
    )));
    tail.needs_sink = false;

    let output_dop = target.pipeline.dop;
    let mut extra_pipelines = weighted.extra_pipelines;
    extra_pipelines.append(&mut target.extra_pipelines);
    extra_pipelines.extend([weighted.pipeline, finalize, target.pipeline, tail]);
    let pipeline = new_source_pipeline_with_dop(
        ctx,
        Box::new(LocalExchangeSourceFactory::new(node_id, 1, handoff)),
        output_dop,
    );
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::any(output_dop),
    })
}
