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

//! Compiled writer-family pipelines.
//!
//! A TableWriter runs on its input's drivers, one independent writer per
//! driver, exactly as the plan-tree writer does. An immediate partitioned
//! receiver is re-shuffled on its exact edge keys at driver granularity.
//! A TableFinish owns the complete prepared write set, so its one
//! writer-result input is gathered to one driver first: DOP 1 is the finish
//! operator's own law.

use super::super::program_failure;
use super::*;
use crate::exec::fragment::error::FragmentBindingErrorKind;
use crate::exec::operators::compiled_writer::{
    compiled_table_finish_factory, compiled_table_writer_factory,
};

pub(super) fn build_table_writer(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    input: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    // The factory copies the Task capability it needs, so a refused binding
    // builds no driver.
    let binding = ctx.compiled_writers.writer(id).ok_or_else(|| {
        program_failure(
            FragmentBindingErrorKind::MissingAssignment,
            format!(
                "missing Task write binding for compiled table writer at local node {}",
                id.index()
            ),
        )
    })?;
    let factory = compiled_table_writer_factory(program, id, node_id, binding, error)?;
    let mut build = build_node(program, input, ctx, error, ordered)?;
    if matches!(
        program.graph().nodes()[input.index()].kind(),
        ProgramNodeKind::ExchangeSource { .. }
    ) {
        let receiver = program.exchange_inputs().get(&input).ok_or_else(|| {
            program_failure(
                FragmentBindingErrorKind::MissingAssignment,
                format!(
                    "missing compiled exchange input for table writer at local node {}",
                    id.index()
                ),
            )
        })?;
        use novarocks_local_program::CompiledExchangeRouting;
        use novarocks_type_contract::PartitionHashAlgorithm;
        let partitions = build.pipeline.dop as usize;
        match receiver.routing {
            CompiledExchangeRouting::ConditionalKey { applies, .. } => {
                let keys = frozen_routing_keys(
                    program.graph().nodes()[input.index()].output_layout(),
                    &receiver.hash_partition_slots,
                )?;
                build = shuffle_compiled_typed_keys(
                    build,
                    ctx,
                    node_id,
                    keys,
                    Some(applies),
                    partitions,
                );
            }
            CompiledExchangeRouting::Hash {
                algorithm: PartitionHashAlgorithm::NativeHierarchicalV1,
            } => {
                let keys = frozen_routing_keys(
                    program.graph().nodes()[input.index()].output_layout(),
                    &receiver.hash_partition_slots,
                )?;
                build = shuffle_compiled_typed_keys(build, ctx, node_id, keys, None, partitions);
            }
            CompiledExchangeRouting::Hash { .. }
            | CompiledExchangeRouting::BucketShuffle { .. } => {
                build = shuffle_compiled_group_input_slots(
                    build,
                    ctx,
                    node_id,
                    receiver.hash_partition_slots.to_vec(),
                    partitions,
                );
            }
            CompiledExchangeRouting::Unpartitioned | CompiledExchangeRouting::RoundRobin => {}
        }
    }
    build.pipeline.factories.push(Box::new(factory));
    build.stream = StreamDesc::any(build.pipeline.dop);
    Ok(build)
}

pub(super) fn build_table_finish(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    inputs: &[ProgramNodeId],
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    if let ProgramNodeKind::TableFinish {
        count_mode: novarocks_local_program::FinishCountMode::LogicalChanges,
        input_roles,
        logical_summary_layout: Some(layout),
        ..
    } = program.graph().nodes()[id.index()].kind()
    {
        return build_logical_finish(
            program,
            id,
            node_id,
            inputs,
            *input_roles,
            layout,
            ctx,
            error,
            ordered,
        );
    }
    let binding = ctx.compiled_writers.finisher(id).ok_or_else(|| {
        program_failure(
            FragmentBindingErrorKind::MissingAssignment,
            format!(
                "missing Task finish binding for compiled table finish at local node {}",
                id.index()
            ),
        )
    })?;
    let factory = compiled_table_finish_factory(program, id, node_id, binding, error)?;
    let [input] = inputs else {
        return Err(program_failure(
            FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "compiled table finish at local node {} reads more than one writer input",
                id.index()
            ),
        ));
    };
    let build = build_node(program, *input, ctx, error, ordered)?;
    let mut build = gather_to_one(build, ctx, node_id);
    build.pipeline.factories.push(Box::new(factory));
    build.stream = StreamDesc::single();
    Ok(build)
}

fn build_logical_finish(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    inputs: &[ProgramNodeId],
    roles: novarocks_result_contract::FinishInputs,
    layout: &novarocks_local_program::StaticLayout,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    ordered: Option<&CompiledOrderedFilterContext>,
) -> ExecutionResult<PipelineBuildResult> {
    let invalid = || {
        program_failure(
            FragmentBindingErrorKind::InvalidAssignment,
            "logical finish has no exact Task summary input",
        )
    };
    let writer = *inputs
        .get(roles.writer_results as usize)
        .ok_or_else(invalid)?;
    let summary = *inputs
        .get(roles.logical_summary.ok_or_else(invalid)? as usize)
        .ok_or_else(invalid)?;
    let binding = ctx
        .compiled_writers
        .finisher(id)
        .and_then(|finish| finish.logical_summary.clone())
        .ok_or_else(invalid)?;
    let receiver = ctx
        .exchange_bindings
        .get(binding.receiver_node)
        .ok_or_else(invalid)?;
    if receiver.expected_senders != binding.sources.len() {
        return Err(invalid());
    }
    let ProgramNodeKind::ExchangeSource { timeout, .. } =
        program.graph().nodes()[summary.index()].kind()
    else {
        return Err(invalid());
    };
    let handle = receiver
        .receiver_port
        .receiver_handle(
            crate::runtime::fragment::io::ExchangeReceiverKey {
                fragment_instance_id: novarocks_types::identity::UniqueId::new(
                    receiver.key.finst_id_hi,
                    receiver.key.finst_id_lo,
                ),
                node_id: receiver.key.node_id,
            },
            receiver.expected_senders,
        )
        .map_err(|cause| cause.into_execution())?;
    let summary = crate::exec::operators::logical_change_summary::SourceBoundSummaryInput::new(
        handle,
        binding.sources,
        layout.clone(),
        *timeout,
    )?;
    let mut child = build_node(program, writer, ctx, error, ordered)?;
    let producers = usize::try_from(child.pipeline.dop)
        .ok()
        .filter(|dop| *dop > 0)
        .ok_or_else(invalid)?;
    let state = UnionAllSharedState::new(producers, node_id);
    child
        .pipeline
        .factories
        .push(Box::new(UnionAllSinkFactory::new(state.clone(), node_id)));
    child.pipeline.needs_sink = false;
    let binding = ctx.compiled_writers.finisher(id).ok_or_else(invalid)?;
    let factory = crate::exec::operators::compiled_writer::compiled_logical_table_finish_factory(
        program, id, node_id, binding, error, state, summary,
    )?;
    let pipeline = new_source_pipeline_with_dop(ctx, Box::new(factory), 1);
    let mut extra_pipelines = child.extra_pipelines;
    extra_pipelines.push(child.pipeline);
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::single(),
    })
}
