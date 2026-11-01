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
//! Pipeline graph builder from lowered execution plans.
//!
//! Responsibilities:
//! - Transforms exec-node trees into source/processor/sink pipelines with dependencies.
//! - Allocates shared operator state and wiring metadata required for runtime scheduling.
//!
//! Key exported interfaces:
//! - Types: `PipelinePlan`, `PipelineGraph`.
//! - Functions: fixed native and compat pipeline graph builders.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

fn program_failure(
    kind: crate::exec::fragment::error::FragmentBindingErrorKind,
    detail: impl Into<String>,
) -> ExecutionFailure {
    crate::exec::fragment::error::FragmentBindingError::new(
        crate::exec::fragment::error::FragmentBindingTarget::Program,
        kind,
        detail,
    )
    .into()
}

use std::sync::Arc;

mod compiled;
#[cfg(test)]
pub(crate) use compiled::expand_tests::{unpivot_program, unpivot_rows};
#[cfg(test)]
pub(crate) use compiled::family_fixture::{
    compile as compile_family_fixture, package_with_sink as family_package_with_sink,
    package_with_sink_and_names as family_package_with_sink_and_names, values as family_values,
    values_chunk_program,
};
#[cfg(test)]
pub(crate) use compiled::ordered_scan_with_numeric_membership_fixture;
#[cfg(test)]
pub(crate) use compiled::{compiled_statistics_finish_fixture, compiled_statistics_writer_fixture};
mod runtime_filter;
#[cfg(test)]
pub(crate) use compiled::build_compiled_pipeline_graph;
#[cfg(any(test, novarocks_dev_controls))]
pub use compiled::build_compiled_pipeline_graph_for_test;
pub(crate) use compiled::{CompiledOrderedFilterContext, build_compiled_pipeline_graph_typed};

use crate::runtime_filter as execution;

use super::operator_factory::OperatorFactory;
use crate::exec::node::assert::AssertNumRowsMode;
use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
use crate::exec::operators::local_exchanger::{LocalExchangePartitionSpec, LocalExchanger};
use crate::exec::operators::scan::StreamScanSourceFactory;
use crate::exec::operators::{
    AssertNumRowsProcessorFactory, ExchangeSourceFactory, LimitProcessorFactory,
    LocalExchangeSinkFactory, LocalExchangeSourceFactory, NlJoinBuildSinkFactory,
    NlJoinSharedState, UnionAllSharedState, UnionAllSinkFactory, UnionAllSourceFactory,
};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::distribution::{Distribution, StreamDesc};
use crate::runtime::function_set::SealedExecutionFunctionSet;

/// Pipeline-level plan metadata produced by pipeline graph construction.
pub struct PipelinePlan {
    pub id: i32,
    pub factories: Vec<Box<dyn OperatorFactory>>,
    pub dop: i32,
    pub needs_sink: bool,
}

/// Pipeline graph with factories, dependencies, and stream edges for one fragment.
pub struct PipelineGraph {
    pub pipelines: Vec<PipelinePlan>,
    pub root_id: i32,
}

impl PipelineGraph {
    /// Add a complete independent summary pipeline to this Task's graph.
    /// Its single driver joins the same batch and completion owner.
    pub(crate) fn append_summary_pipeline(
        &mut self,
        source: Box<dyn OperatorFactory>,
        sink: Box<dyn OperatorFactory>,
    ) -> ExecutionResult<()> {
        let mut ids = std::collections::BTreeSet::new();
        if self
            .pipelines
            .iter()
            .any(|pipeline| !ids.insert(pipeline.id))
            || !ids.contains(&self.root_id)
        {
            return Err(program_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled pipeline identities do not define one original root",
            ));
        }
        let id = ids.last().and_then(|id| id.checked_add(1)).ok_or_else(|| {
            program_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled summary pipeline identity is exhausted",
            )
        })?;
        self.pipelines.push(PipelinePlan {
            id,
            factories: vec![source, sink],
            dop: 1,
            needs_sink: false,
        });
        Ok(())
    }
}

struct PipelineBuildResult {
    pipeline: PipelinePlan,
    extra_pipelines: Vec<PipelinePlan>,
    stream: StreamDesc,
}

struct PipelineBuildContext {
    dep_manager: DependencyManager,
    runtime_filter_execution: PipelineRuntimeFilterExecution,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    /// Exact task-owned write capabilities for the compiled program.
    compiled_writers: crate::runtime::fragment::CompiledWriterBindings,
    next_pipeline_id: i32,
    pipeline_dop: i32,
    operator_buffer_chunks: usize,
    local_exchange_buffer_mem_limit_per_driver: usize,
    local_exchange_max_buffered_rows: i64,
}

struct PipelineRuntimeFilterExecution {
    session: Option<execution::RuntimeFilterSessionRef>,
}

impl PipelineBuildContext {
    fn next_pipeline_id(&mut self) -> i32 {
        let id = self.next_pipeline_id;
        self.next_pipeline_id += 1;
        id
    }
}

const ROOT_SINK_LOCAL_EXCHANGE_NODE_ID: i32 = -1;

fn gather_to_one(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
) -> PipelineBuildResult {
    let dop = build.pipeline.dop.max(1);
    if dop <= 1 {
        build.stream = StreamDesc::single();
        return build;
    }

    let partition_count = 1usize;
    let exchanger = LocalExchanger::new_with_limits(
        partition_count,
        dop as usize,
        LocalExchangePartitionSpec::Single,
        ctx.local_exchange_buffer_mem_limit_per_driver,
        ctx.local_exchange_max_buffered_rows,
    );
    build
        .pipeline
        .factories
        .push(Box::new(LocalExchangeSinkFactory::new(
            owner_node_id,
            Arc::clone(&exchanger),
        )));
    build.pipeline.needs_sink = false;

    let source_factory = Box::new(LocalExchangeSourceFactory::new(
        owner_node_id,
        partition_count,
        exchanger,
    ));
    let downstream = new_source_pipeline_with_dop(ctx, source_factory, 1);

    let mut extra_pipelines = build.extra_pipelines;
    extra_pipelines.push(build.pipeline);

    PipelineBuildResult {
        pipeline: downstream,
        extra_pipelines,
        stream: StreamDesc::single(),
    }
}

// Design: ADR-0159 (docs/adr/ADR-0159-driver-polled-connector-scan-streams.md)
/// Spreads a pipeline's chunks over `target_dop` consumer drivers.
///
/// The consumers take whole chunks from one shared queue, so an idle consumer
/// takes the next chunk instead of it waiting behind a busy one. The queue
/// holds at most `operator_buffer_chunks` chunks for the whole exchange and
/// only applies backpressure when full; it never spills. The output carries
/// no key ownership or order.
fn hand_off_to_dop(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    target_dop: i32,
) -> PipelineBuildResult {
    let consumer_count = target_dop.max(1) as usize;
    let producer_count = build.pipeline.dop.max(1) as usize;
    let exchanger =
        LocalExchanger::new_handoff(producer_count, consumer_count, ctx.operator_buffer_chunks);
    build
        .pipeline
        .factories
        .push(Box::new(LocalExchangeSinkFactory::new(
            owner_node_id,
            Arc::clone(&exchanger),
        )));
    build.pipeline.needs_sink = false;

    let source_factory = Box::new(LocalExchangeSourceFactory::new(owner_node_id, 1, exchanger));
    let downstream = new_source_pipeline_with_dop(ctx, source_factory, consumer_count as i32);

    let mut extra_pipelines = build.extra_pipelines;
    extra_pipelines.push(build.pipeline);

    PipelineBuildResult {
        pipeline: downstream,
        extra_pipelines,
        stream: StreamDesc::any(consumer_count as i32),
    }
}

fn shuffle_compiled_group_input_slots(
    build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    slots: Vec<novarocks_types::SlotId>,
    partition_count: usize,
) -> PipelineBuildResult {
    let distribution = Distribution::HashInputSlots {
        slots: slots.clone(),
        partitions: partition_count.max(1),
        hash_version: 0,
    };
    shuffle_input_slots_with_distribution(
        build,
        ctx,
        owner_node_id,
        slots,
        distribution,
        partition_count,
    )
}

/// The receiver/assertion has already frozen its typed key and applicability.
/// DriverV1 routing is independent of the network's InstanceV1 selection.
fn shuffle_compiled_typed_keys(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    keys: Vec<(
        novarocks_types::SlotId,
        novarocks_type_contract::FunctionValueType,
    )>,
    applies: Option<novarocks_types::SlotId>,
    partition_count: usize,
) -> PipelineBuildResult {
    if partition_count <= 1 {
        build.stream = StreamDesc::single();
        return build;
    }
    let slots = keys.iter().map(|(slot, _)| *slot).collect();
    let producer_count = build.pipeline.dop.max(1) as usize;
    let spec = match applies {
        Some(applies) => LocalExchangePartitionSpec::ConditionalKey { applies, keys },
        None => LocalExchangePartitionSpec::HierarchicalInputSlotIds(keys),
    };
    let exchanger = LocalExchanger::new_with_limits(
        partition_count,
        producer_count,
        spec,
        ctx.local_exchange_buffer_mem_limit_per_driver,
        ctx.local_exchange_max_buffered_rows,
    );
    build
        .pipeline
        .factories
        .push(Box::new(LocalExchangeSinkFactory::new(
            owner_node_id,
            Arc::clone(&exchanger),
        )));
    build.pipeline.needs_sink = false;
    let source = Box::new(LocalExchangeSourceFactory::new(
        owner_node_id,
        partition_count,
        exchanger,
    ));
    let pipeline = new_source_pipeline_with_dop(ctx, source, partition_count as i32);
    let mut extra_pipelines = build.extra_pipelines;
    extra_pipelines.push(build.pipeline);
    PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc {
            dop: partition_count as i32,
            distribution: Distribution::HashInputSlots {
                slots,
                partitions: partition_count,
                hash_version: 1,
            },
        },
    }
}

fn shuffle_input_slots_with_distribution(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    partition_slot_ids: Vec<novarocks_types::SlotId>,
    distribution: Distribution,
    partition_count: usize,
) -> PipelineBuildResult {
    let partition_count = partition_count.max(1);
    if partition_count <= 1 {
        build.stream = StreamDesc::single();
        return build;
    }

    let producer_count = build.pipeline.dop.max(1) as usize;
    let exchanger = LocalExchanger::new_with_limits(
        partition_count,
        producer_count,
        LocalExchangePartitionSpec::InputSlotIds(partition_slot_ids),
        ctx.local_exchange_buffer_mem_limit_per_driver,
        ctx.local_exchange_max_buffered_rows,
    );
    build
        .pipeline
        .factories
        .push(Box::new(LocalExchangeSinkFactory::new(
            owner_node_id,
            Arc::clone(&exchanger),
        )));
    build.pipeline.needs_sink = false;

    let source_factory = Box::new(LocalExchangeSourceFactory::new(
        owner_node_id,
        partition_count,
        exchanger,
    ));
    let downstream = new_source_pipeline_with_dop(ctx, source_factory, partition_count as i32);

    let mut extra_pipelines = build.extra_pipelines;
    extra_pipelines.push(build.pipeline);

    PipelineBuildResult {
        pipeline: downstream,
        extra_pipelines,
        stream: StreamDesc {
            dop: partition_count as i32,
            distribution,
        },
    }
}

fn runtime_filter_session(
    execution: &PipelineRuntimeFilterExecution,
    binding_id: u32,
) -> ExecutionResult<&execution::RuntimeFilterSessionRef> {
    match execution {
        PipelineRuntimeFilterExecution {
            session: Some(session),
        } => Ok(session),
        PipelineRuntimeFilterExecution { session: None } => Err(crate::runtime_filter::RuntimeFilterContractViolation::new(crate::runtime_filter::RuntimeFilterContractViolationKind::MissingSession, format!(
            "native runtime-filter binding_id={binding_id} requires an execution runtime-filter session"
        )).into()),
    }
}

fn new_source_pipeline(
    ctx: &mut PipelineBuildContext,
    source: Box<dyn OperatorFactory>,
) -> PipelinePlan {
    new_source_pipeline_with_dop(ctx, source, ctx.pipeline_dop)
}

fn new_source_pipeline_with_dop(
    ctx: &mut PipelineBuildContext,
    source: Box<dyn OperatorFactory>,
    dop: i32,
) -> PipelinePlan {
    PipelinePlan {
        id: ctx.next_pipeline_id(),
        factories: vec![source],
        dop: dop.max(1),
        needs_sink: true,
    }
}

#[cfg(test)]
pub(crate) fn compiled_root_result_fixture(
    project: Option<bool>,
) -> Arc<novarocks_local_program::LocalProgram> {
    compiled::values_tests::root_result_boundary_tests::boundary_program(project)
}
