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

//! Compiled join pipelines.
//!
//! Both join families share one build per instance: the build input is
//! captured by its installed E drivers, finalized by independent F workers, and the probe runs at
//! its own pipeline width against it. Every physical distribution has already
//! delivered this instance its whole share of both sides, so this local
//! strategy is correct for each of them. The probe is the program's left input
//! and the build its right input; the compiler normalized that orientation.
//! The build pipeline is an extra pipeline that ends in its build sink.
//!
//! A hash join's membership runtime-filter producers observe its build keys
//! in the build sink. The one build driver is their one local partition.

use super::super::program_failure;
use super::*;
use crate::exec::fragment::error::FragmentBindingErrorKind;
use crate::exec::operators::compiled_expression::root_value_type;
use crate::exec::operators::hashjoin::compiled_hash_join::{
    CompiledHashJoinPlan, CompiledJoinFinalizeSourceFactory, CompiledJoinFrame,
    CompiledJoinInputSinkFactory, CompiledJoinProbeProcessorFactory, CompiledJoinTailSourceFactory,
};
use crate::exec::operators::hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory;
use crate::exec::operators::{NoopSinkFactory, hashjoin::JoinParticipants};
use novarocks_local_program::FilterProducerKind;
use std::num::NonZeroUsize;

/// The membership producers of one compiled hash join, one stream per
/// binding over the build key it names. This milestone executes Membership
/// (SetUnion) producers only; every other producer kind is refused by name.
fn compiled_join_producers(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    plan: &CompiledHashJoinPlan,
    ctx: &PipelineBuildContext,
    input_positions: i32,
) -> ExecutionResult<Option<Arc<NativeRuntimeFilterProducerFactory>>> {
    let ProgramNodeKind::Join {
        runtime_filters, ..
    } = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| {
            program_failure(
                FragmentBindingErrorKind::MissingAssignment,
                format!("compiled join node {} is absent", id.index()),
            )
        })?
        .kind()
    else {
        return Err(program_failure(
            FragmentBindingErrorKind::InvalidAssignment,
            format!("compiled node {} is not a hash join", id.index()),
        ));
    };
    let Some(first) = runtime_filters.first() else {
        return Ok(None);
    };
    let session =
        runtime_filter_session(&ctx.runtime_filter_execution, first.producer.binding_id())?.clone();
    let mut producers = Vec::with_capacity(runtime_filters.len());
    for filter in runtime_filters {
        let producer = &filter.producer;
        if producer.kind() != FilterProducerKind::Membership {
            return Err(program_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled hash join at local node {} runtime-filter binding_id={} with a {:?} producer is not executable yet",
                    id.index(),
                    producer.binding_id(),
                    producer.kind()
                ),
            ));
        }
        producers.push((filter.key_ordinal, runtime_filter_producer(producer)?));
    }
    NativeRuntimeFilterProducerFactory::from_compiled(
        &producers,
        plan.eq_null_safe(),
        |ordinal| {
            let site = plan.build_site(ordinal).ok_or_else(|| {
                program_failure(
                    FragmentBindingErrorKind::MissingAssignment,
                    format!(
                        "compiled hash join at local node {} has no build key {ordinal}",
                        id.index()
                    ),
                )
            })?;
            Ok(root_value_type(program, site)?.data_type().clone())
        },
        session,
        input_positions,
    )
    .map(|factory| Some(Arc::new(factory)))
}

pub(super) fn build_hash_join(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    // A refused node, or a refused runtime-filter producer, builds no driver.
    let plan = Arc::new(CompiledHashJoinPlan::try_new(program, id)?);
    let ProgramNodeKind::Join {
        runtime_filter_consumers,
        ..
    } = program.graph().nodes()[id.index()].kind()
    else {
        unreachable!()
    };
    let bindings = runtime_filter_consumers
        .iter()
        .map(|filter| novarocks_local_program::FilterConsumerAtExpr {
            expr_id: filter.expr_id,
            consumer: filter.consumer.clone(),
        })
        .collect::<Vec<_>>();
    let consumers = super::compiled_runtime_filter_consumers(
        "Join", "join", program, id, &bindings, ctx, error,
    )?;
    let probe_build = build_node(program, probe, ctx, error, ordered)?;
    let build_build = build_node(program, build, ctx, error, ordered)?;
    let producers = compiled_join_producers(program, id, &plan, ctx, build_build.pipeline.dop)?;
    let mut probe_build = probe_build;
    if let Some(consumers) = consumers {
        probe_build.pipeline.factories.push(Box::new(
            crate::exec::operators::runtime_filter::NativeRuntimeFilterProcessorFactory::new_compiled(node_id, consumers)
        ));
    }
    assemble(
        program,
        id,
        node_id,
        probe_build,
        build_build,
        ctx,
        error,
        Some(plan),
        producers,
    )
}

pub(super) fn build_nested_loop_join(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    let probe_build = build_node(program, probe, ctx, error, ordered)?;
    let build_build = build_node(program, build, ctx, error, ordered)?;
    assemble(
        program,
        id,
        node_id,
        probe_build,
        build_build,
        ctx,
        error,
        None,
        None,
    )
}

fn installed_positions(dop: i32, role: &str) -> ExecutionResult<NonZeroUsize> {
    usize::try_from(dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            program_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!("compiled join {role} has no positive installed pipeline DOP"),
            )
        })
}

/// E and Q belong to the actual child pipelines. F owns independently installed
/// cooperative work positions; K chooses P later. Only build-output joins add
/// the original shared Q+T handoff boundary.
#[allow(clippy::too_many_arguments)]
fn assemble(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    mut probe: PipelineBuildResult,
    mut input: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    hash: Option<Arc<CompiledHashJoinPlan>>,
    producers: Option<Arc<NativeRuntimeFilterProducerFactory>>,
) -> ExecutionResult<PipelineBuildResult> {
    let e = installed_positions(input.pipeline.dop, "E")?;
    let q = installed_positions(probe.pipeline.dop, "Q")?;
    let f = installed_positions(ctx.pipeline_dop, "F")?;
    let build_output = matches!(
        program.graph().nodes()[id.index()].kind(),
        ProgramNodeKind::Join {
            join_type: novarocks_local_program::JoinType::RightOuter
                | novarocks_local_program::JoinType::FullOuter
                | novarocks_local_program::JoinType::RightSemi
                | novarocks_local_program::JoinType::RightAnti,
            ..
        } | ProgramNodeKind::NestedLoopJoin {
            join_type: novarocks_local_program::NestedLoopJoinType::RightOuter
                | novarocks_local_program::NestedLoopJoinType::FullOuter,
            ..
        }
    );
    let t = if build_output { f.get() } else { 0 };
    let participants = JoinParticipants {
        inputs: e.get(),
        finalizers: f,
        probes: q.get(),
        tails: t,
    };
    let handoff = participants.handoff_producers()?.map(|positions| {
        LocalExchanger::new_handoff(positions, q.get(), ctx.operator_buffer_chunks)
    });
    let declaration = Arc::new(match hash {
        Some(plan) => CompiledJoinFrame::new(program.clone(), plan, participants, handoff.clone()),
        None => {
            CompiledJoinFrame::new_nested_loop(program.clone(), id, participants, handoff.clone())?
        }
    });
    input
        .pipeline
        .factories
        .push(Box::new(CompiledJoinInputSinkFactory::new(
            declaration.clone(),
            error.clone(),
            producers,
        )));
    input.pipeline.needs_sink = false;
    probe
        .pipeline
        .factories
        .push(Box::new(CompiledJoinProbeProcessorFactory::new(
            declaration.clone(),
            error.clone(),
        )));
    let mut finalize = new_source_pipeline_with_dop(
        ctx,
        Box::new(CompiledJoinFinalizeSourceFactory::new(
            declaration.clone(),
            error.clone(),
        )),
        i32::try_from(f.get()).expect("F came from installed i32 DOP"),
    );
    finalize.factories.push(Box::new(NoopSinkFactory::new()));
    finalize.needs_sink = false;
    let mut extra = Vec::new();
    extra.append(&mut probe.extra_pipelines);
    extra.append(&mut input.extra_pipelines);
    extra.push(input.pipeline);
    extra.push(finalize);
    let output_dop = probe.pipeline.dop;
    if let Some(handoff) = handoff {
        probe
            .pipeline
            .factories
            .push(Box::new(LocalExchangeSinkFactory::new(
                node_id,
                handoff.clone(),
            )));
        probe.pipeline.needs_sink = false;
        extra.push(probe.pipeline);
        let mut tail = new_source_pipeline_with_dop(
            ctx,
            Box::new(CompiledJoinTailSourceFactory::new(
                declaration,
                error.clone(),
            )),
            i32::try_from(t).expect("T came from installed i32 DOP"),
        );
        tail.factories.push(Box::new(LocalExchangeSinkFactory::new(
            node_id,
            handoff.clone(),
        )));
        tail.needs_sink = false;
        extra.push(tail);
        let output = new_source_pipeline_with_dop(
            ctx,
            Box::new(LocalExchangeSourceFactory::new(node_id, 1, handoff)),
            output_dop,
        );
        Ok(PipelineBuildResult {
            pipeline: output,
            extra_pipelines: extra,
            stream: StreamDesc::any(output_dop),
        })
    } else {
        Ok(PipelineBuildResult {
            pipeline: probe.pipeline,
            extra_pipelines: extra,
            stream: StreamDesc::any(output_dop),
        })
    }
}
