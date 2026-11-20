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
use crate::exec::{
    operators::{
        NoopSinkFactory,
        setop::staged::{
            SetOpDeclaration, SetOpFinalizeFactory, SetOpInputFactory, SetOpTailFactory,
        },
    },
    shared_stage::stage_completion::StageParticipants,
};
use novarocks_local_program::SetOpKind;
use std::num::NonZeroUsize;

// Design: ADR-0243 (docs/adr/ADR-0243-set-operation-shared-stages-and-exactly-once-placement.md)
pub(super) fn build_set_op(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    kind: SetOpKind,
    inputs: &[ProgramNodeId],
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    let family = match kind {
        SetOpKind::Intersect => "Intersect",
        SetOpKind::Except => "Except",
    };
    validate_set_branches(program, id, inputs, family)?;
    let output = program.graph().nodes()[id.index()].output_layout();
    if output.slots().is_empty() {
        return Err(program_failure(
            FragmentBindingErrorKind::InvalidAssignment,
            "compiled set operation has no comparison columns",
        ));
    }

    let mut branches = Vec::with_capacity(inputs.len());
    for input in inputs {
        let child = build_node(program, *input, ctx, error, ordered)?;
        let positions = installed_positions(child.pipeline.dop)?;
        branches.push((child, positions.get()));
    }
    let (mut build, build_positions) = branches.remove(0);
    let probe_positions = branches.iter().try_fold(0usize, |total, (_, positions)| {
        total.checked_add(*positions).ok_or_else(|| {
            program_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                "compiled set operation probe position count overflows",
            )
        })
    })?;
    let dop = ctx.pipeline_dop;
    let workers = installed_positions(dop)?;
    let handoff =
        LocalExchanger::new_handoff(workers.get(), workers.get(), ctx.operator_buffer_chunks);
    let declaration = Arc::new(SetOpDeclaration::new(
        id,
        output.clone(),
        kind,
        branches.len(),
        StageParticipants {
            inputs: build_positions,
            finalizers: workers,
            probes: probe_positions,
            tails: workers.get(),
        },
        handoff.clone(),
    ));

    let mut extra_pipelines = Vec::new();
    build
        .pipeline
        .factories
        .push(Box::new(SetOpInputFactory::build(
            declaration.clone(),
            error.clone(),
        )));
    build.pipeline.needs_sink = false;
    extra_pipelines.append(&mut build.extra_pipelines);
    extra_pipelines.push(build.pipeline);

    let mut offset = 0;
    for (lane, (mut probe, positions)) in branches.into_iter().enumerate() {
        probe
            .pipeline
            .factories
            .push(Box::new(SetOpInputFactory::probe(
                declaration.clone(),
                error.clone(),
                lane,
                offset,
                positions,
            )));
        offset += positions;
        probe.pipeline.needs_sink = false;
        extra_pipelines.append(&mut probe.extra_pipelines);
        extra_pipelines.push(probe.pipeline);
    }

    let mut finalize = new_source_pipeline_with_dop(
        ctx,
        Box::new(SetOpFinalizeFactory::new(
            declaration.clone(),
            error.clone(),
        )),
        dop,
    );
    finalize.factories.push(Box::new(NoopSinkFactory::new()));
    finalize.needs_sink = false;
    extra_pipelines.push(finalize);

    let mut tail = new_source_pipeline_with_dop(
        ctx,
        Box::new(SetOpTailFactory::new(declaration, error.clone())),
        dop,
    );
    tail.factories.push(Box::new(LocalExchangeSinkFactory::new(
        node_id,
        handoff.clone(),
    )));
    tail.needs_sink = false;
    extra_pipelines.push(tail);

    let pipeline = new_source_pipeline_with_dop(
        ctx,
        Box::new(LocalExchangeSourceFactory::new(node_id, 1, handoff)),
        dop,
    );
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::any(dop),
    })
}

fn installed_positions(dop: i32) -> ExecutionResult<NonZeroUsize> {
    usize::try_from(dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            program_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                "compiled set operation requires a positive installed DOP",
            )
        })
}

#[cfg(test)]
#[path = "set_op/tests.rs"]
mod tests;
