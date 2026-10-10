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
//! gathered to one driver, which publishes one artifact, and the probe runs at
//! its own pipeline width against it. Every physical distribution has already
//! delivered this instance its whole share of both sides, so this local
//! strategy is correct for each of them. The probe is the program's left input
//! and the build its right input; the compiler normalized that orientation.
//! The build pipeline is an extra pipeline that ends in its build sink.
//!
//! A hash join's membership runtime-filter producers observe its build keys
//! in the build sink. The one build driver is their one local partition.

use super::*;
use crate::exec::operators::compiled_expression::root_value_type;
use crate::exec::operators::compiled_nljoin::{
    CompiledNlJoinPlan, CompiledNlJoinProbeProcessorFactory,
};
use crate::exec::operators::hashjoin::compiled_hash_join::{
    CompiledHashJoinBuildSinkFactory, CompiledHashJoinPlan, CompiledHashJoinProbeProcessorFactory,
};
use crate::exec::operators::hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory;
use novarocks_local_program::FilterProducerKind;

/// Local partitions of a compiled hash join's runtime-filter producers: the
/// build input is gathered to one driver, which publishes the instance's one
/// build and so is the producers' only partition.
const COMPILED_JOIN_BUILD_PARTITIONS: i32 = 1;

/// The membership producers of one compiled hash join, one stream per
/// binding over the build key it names. This milestone executes Membership
/// (SetUnion) producers only; every other producer kind is refused by name.
fn compiled_join_producers(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    plan: &CompiledHashJoinPlan,
    ctx: &PipelineBuildContext,
) -> Result<Option<Arc<NativeRuntimeFilterProducerFactory>>, String> {
    let ProgramNodeKind::Join {
        runtime_filters, ..
    } = program
        .graph()
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("compiled join node {} is absent", id.index()))?
        .kind()
    else {
        return Err(format!("compiled node {} is not a hash join", id.index()));
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
            return Err(format!(
                "compiled hash join at local node {} runtime-filter binding_id={} with a {:?} producer is not executable yet",
                id.index(),
                producer.binding_id(),
                producer.kind()
            ));
        }
        producers.push((filter.key_ordinal, runtime_filter_producer(producer)?));
    }
    NativeRuntimeFilterProducerFactory::from_compiled(
        &producers,
        plan.eq_null_safe(),
        |ordinal| {
            let site = plan.build_site(ordinal).ok_or_else(|| {
                format!(
                    "compiled hash join at local node {} has no build key {ordinal}",
                    id.index()
                )
            })?;
            Ok(root_value_type(program, site)?.data_type)
        },
        session,
        COMPILED_JOIN_BUILD_PARTITIONS,
    )
    .map(|factory| Some(Arc::new(factory)))
}

pub(super) fn build_hash_join<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    // A refused node, or a refused runtime-filter producer, builds no driver.
    let plan = Arc::new(CompiledHashJoinPlan::try_new(program, id)?);
    let producers = compiled_join_producers(program, id, &plan, ctx)?;
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
    let probe_build = build_node(program, probe, ctx, error, metadata)?;
    let build_build = build_node(program, build, ctx, error, metadata)?;
    let mut build_build = gather_to_one(build_build, ctx, node_id);
    if producers.is_some() && build_build.pipeline.dop != COMPILED_JOIN_BUILD_PARTITIONS {
        return Err(format!(
            "compiled hash join at local node {} builds on {} drivers, not on the one partition its runtime-filter producers declare",
            id.index(),
            build_build.pipeline.dop
        ).into());
    }
    let probe_dop = probe_build.pipeline.dop.max(1) as usize;
    let state = Arc::new(BroadcastJoinSharedState::new(
        node_id,
        ctx.dep_manager.clone(),
        probe_dop,
    ));
    let mut probe_build = probe_build;
    if let Some(consumers) = consumers {
        probe_build.pipeline.factories.push(Box::new(
            crate::exec::operators::runtime_filter::NativeRuntimeFilterProcessorFactory::new_compiled(node_id, consumers)
        ));
    }
    probe_build
        .pipeline
        .factories
        .push(Box::new(CompiledHashJoinProbeProcessorFactory::new(
            Arc::clone(program),
            Arc::clone(&plan),
            Arc::clone(&state),
            Arc::clone(error),
        )));
    build_build
        .pipeline
        .factories
        .push(Box::new(CompiledHashJoinBuildSinkFactory::new(
            Arc::clone(program),
            plan,
            state,
            Arc::clone(error),
            producers,
        )));
    Ok(finish(probe_build, build_build))
}

pub(super) fn build_nested_loop_join<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    probe: ProgramNodeId,
    build: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    let plan = Arc::new(CompiledNlJoinPlan::try_new(program, id)?);
    let mut probe_build = build_node(program, probe, ctx, error, metadata)?;
    let build_build = build_node(program, build, ctx, error, metadata)?;
    let mut build_build = gather_to_one(build_build, ctx, node_id);
    let state = Arc::new(NlJoinSharedState::new(
        node_id,
        probe_build.pipeline.dop.max(1) as usize,
        ctx.dep_manager.clone(),
    ));
    probe_build
        .pipeline
        .factories
        .push(Box::new(CompiledNlJoinProbeProcessorFactory::new(
            Arc::clone(program),
            plan,
            Arc::clone(&state),
            Arc::clone(error),
        )));
    // The nested-loop build sink evaluates nothing; it only retains rows.
    build_build
        .pipeline
        .factories
        .push(Box::new(NlJoinBuildSinkFactory::new(state)));
    Ok(finish(probe_build, build_build))
}

/// The probe pipeline continues; the build pipeline ends in its sink.
fn finish(
    mut probe_build: PipelineBuildResult,
    mut build_build: PipelineBuildResult,
) -> PipelineBuildResult {
    build_build.pipeline.needs_sink = false;
    let mut extra_pipelines = Vec::new();
    extra_pipelines.append(&mut probe_build.extra_pipelines);
    extra_pipelines.append(&mut build_build.extra_pipelines);
    extra_pipelines.push(build_build.pipeline);
    let dop = probe_build.pipeline.dop;
    PipelineBuildResult {
        pipeline: probe_build.pipeline,
        extra_pipelines,
        stream: StreamDesc::any(dop),
    }
}
