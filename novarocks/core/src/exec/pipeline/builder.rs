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

use std::sync::Arc;

use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::aggregate::{
    AggregateNode, AggregateRuntimeFilterSpec, AggregateTopNRuntimeFilterProducerBinding,
    StreamingPreaggregationMode,
};
use crate::exec::node::analytic::AnalyticNode;
use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode};
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinType};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::runtime_filter::{
    RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
};
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::sort::SortNode;
use crate::exec::node::table_function::TableFunctionNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
use crate::exec::operators::hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory;
use crate::exec::operators::hashjoin::partitioned_join_shared::PartitionedJoinSharedState;
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::distribution::{Distribution, StreamDesc};
use crate::runtime::fragment::io::{FragmentLookupClient, UnavailableFragmentLookupClient};
use crate::runtime_filter::model::contract::ReductionRequirement;
use crate::runtime_filter::port::producer::ProducerPortKind;
use crate::runtime_filter::port::subscription::SubscriptionKind;
use crate::runtime_filter::service::{
    InstalledRuntimeFilterExecutionContract, NativeRuntimeFilterExecutionContext,
};

use super::operator_factory::OperatorFactory;
use crate::exec::operators::AssertNumRowsProcessorFactory;
use crate::exec::operators::FetchProcessorFactory;
use crate::exec::operators::analytic_shared::AnalyticSharedState;
use crate::exec::operators::local_exchanger::{LocalExchangePartitionSpec, LocalExchanger};
use crate::exec::operators::runtime_filter::NativeRuntimeFilterProcessorFactory;
use crate::exec::operators::{
    AggregateProcessorFactory, AggregateStreamingSinkFactory, AggregateStreamingSourceFactory,
    AggregateStreamingState, AnalyticSinkFactory, AnalyticSourceFactory,
    BroadcastJoinProbeProcessorFactory, ChangeEventExpandProcessorFactory, ExceptSinkFactory,
    ExceptSourceFactory, ExchangeSourceFactory, FilterProcessorFactory, HashJoinBuildSinkFactory,
    IntersectSinkFactory, IntersectSourceFactory, LimitProcessorFactory, LocalExchangeSinkFactory,
    LocalExchangeSourceFactory, LookUpSourceFactory, PartitionedJoinProbeProcessorFactory,
    ProjectProcessorFactory, RepeatProcessorFactory, ScanSourceFactory, SortProcessorFactory,
    TableFunctionProcessorFactory, UnionAllSharedState, UnionAllSinkFactory, UnionAllSourceFactory,
    ValuesSourceFactory,
};
use crate::exec::operators::{ExceptSharedState, IntersectSharedState, SetOpStageController};
use crate::exec::operators::{
    NlJoinBuildSinkFactory, NlJoinProbeProcessorFactory, NlJoinSharedState,
};

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

struct PipelineBuildResult {
    pipeline: PipelinePlan,
    extra_pipelines: Vec<PipelinePlan>,
    stream: StreamDesc,
}

struct PipelineBuildContext {
    arena: Arc<ExprArena>,
    dep_manager: DependencyManager,
    runtime_filter_execution: PipelineRuntimeFilterExecution,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    lookup_client: Arc<dyn FragmentLookupClient>,
    next_pipeline_id: i32,
    pipeline_dop: i32,
}

struct PipelineRuntimeFilterExecution {
    context: Option<NativeRuntimeFilterExecutionContext>,
}

impl PipelineBuildContext {
    fn next_pipeline_id(&mut self) -> i32 {
        let id = self.next_pipeline_id;
        self.next_pipeline_id += 1;
        id
    }
}

#[allow(dead_code)]
/// Build a pipeline graph from an execution plan using the default degree of parallelism.
pub(crate) fn build_native_pipeline_graph_for_exec_plan(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
) -> Result<PipelineGraph, String> {
    let default_dop = crate::runtime::exec_env::calc_pipeline_dop(0);
    build_native_pipeline_graph_for_exec_plan_with_dop(
        plan,
        _debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        default_dop,
    )
}

/// Build a pipeline graph from an execution plan with an explicit degree of parallelism.
pub(crate) fn build_native_pipeline_graph_for_exec_plan_with_dop(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    _exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
) -> Result<PipelineGraph, String> {
    build_pipeline_graph_in_mode(
        plan,
        _debug,
        dep_manager,
        _exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        None,
        PipelineRuntimeFilterExecution { context: None },
        Arc::new(UnavailableFragmentLookupClient),
    )
}

pub(crate) fn build_native_pipeline_graph_for_exec_plan_with_root_sink_dop(
    plan: &ExecPlan,
    debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
) -> Result<PipelineGraph, String> {
    build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context(
        plan,
        debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        root_sink_dop,
        None,
    )
}

pub(crate) fn build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context(
    plan: &ExecPlan,
    debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    context: Option<NativeRuntimeFilterExecutionContext>,
) -> Result<PipelineGraph, String> {
    build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context_and_lookup_client(
        plan,
        debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        root_sink_dop,
        context,
        Arc::new(UnavailableFragmentLookupClient),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context_and_lookup_client(
    plan: &ExecPlan,
    debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    context: Option<NativeRuntimeFilterExecutionContext>,
    lookup_client: Arc<dyn FragmentLookupClient>,
) -> Result<PipelineGraph, String> {
    build_pipeline_graph_in_mode(
        plan,
        debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        root_sink_dop,
        PipelineRuntimeFilterExecution { context },
        lookup_client,
    )
}

pub(crate) fn build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
    plan: &ExecPlan,
    debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    context: Option<NativeRuntimeFilterExecutionContext>,
) -> Result<PipelineGraph, String> {
    build_pipeline_graph_in_mode(
        plan,
        debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        None,
        PipelineRuntimeFilterExecution { context },
        Arc::new(UnavailableFragmentLookupClient),
    )
}

fn build_pipeline_graph_in_mode(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    _exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    runtime_filter_execution: PipelineRuntimeFilterExecution,
    lookup_client: Arc<dyn FragmentLookupClient>,
) -> Result<PipelineGraph, String> {
    let arena = Arc::new(plan.arena.clone());
    let mut ctx = PipelineBuildContext {
        arena,
        dep_manager,
        runtime_filter_execution,
        exchange_bindings,
        scan_bindings,
        lookup_client,
        next_pipeline_id: 0,
        pipeline_dop: pipeline_dop.max(1),
    };
    let mut build = build_pipeline_for_node(&plan.root, &mut ctx)?;
    if let Some(root_sink_dop) = root_sink_dop {
        let root_sink_dop = root_sink_dop.max(1);
        if root_sink_dop == 1 {
            build = gather_to_one(build, &mut ctx, ROOT_SINK_LOCAL_EXCHANGE_NODE_ID);
        } else if root_sink_dop != build.pipeline.dop.max(1) {
            return Err(format!(
                "root sink dop override {root_sink_dop} is unsupported for upstream dop {}",
                build.pipeline.dop.max(1)
            ));
        }
    }
    build.pipeline.needs_sink = true;

    let root_id = build.pipeline.id;
    let mut pipelines = Vec::new();
    pipelines.push(build.pipeline);
    pipelines.append(&mut build.extra_pipelines);
    Ok(PipelineGraph { pipelines, root_id })
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
    let exchanger = LocalExchanger::new(
        partition_count,
        dop as usize,
        LocalExchangePartitionSpec::Single,
        Arc::clone(&ctx.arena),
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

fn shuffle_by_hash(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    partition_exprs: Vec<crate::exec::expr::ExprId>,
    partition_count: usize,
) -> PipelineBuildResult {
    let partition_count = partition_count.max(1);
    if partition_count <= 1 {
        build.stream = StreamDesc::single();
        return build;
    }

    let producer_count = build.pipeline.dop.max(1) as usize;
    let exchanger = LocalExchanger::new(
        partition_count,
        producer_count,
        LocalExchangePartitionSpec::Exprs(partition_exprs.clone()),
        Arc::clone(&ctx.arena),
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
            distribution: Distribution::Hash {
                keys: partition_exprs,
                partitions: partition_count,
                hash_version: 0,
            },
        },
    }
}

fn shuffle_by_hash_on_input_slots(
    mut build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    partition_slot_ids: Vec<crate::common::ids::SlotId>,
    distribution_keys: Vec<crate::exec::expr::ExprId>,
    partition_count: usize,
) -> PipelineBuildResult {
    let partition_count = partition_count.max(1);
    if partition_count <= 1 {
        build.stream = StreamDesc::single();
        return build;
    }

    let producer_count = build.pipeline.dop.max(1) as usize;
    let exchanger = LocalExchanger::new(
        partition_count,
        producer_count,
        LocalExchangePartitionSpec::InputSlotIds(partition_slot_ids),
        Arc::clone(&ctx.arena),
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
            distribution: Distribution::Hash {
                keys: distribution_keys,
                partitions: partition_count,
                hash_version: 0,
            },
        },
    }
}

fn ensure_hash(
    build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    keys: Vec<crate::exec::expr::ExprId>,
    partitions: usize,
) -> PipelineBuildResult {
    let partitions = partitions.max(1);
    if partitions <= 1 {
        let mut build = gather_to_one(build, ctx, owner_node_id);
        build.stream = StreamDesc::single();
        return build;
    }

    let desired = Distribution::Hash {
        keys: keys.clone(),
        partitions,
        hash_version: 0,
    };
    if build.stream.distribution == desired && build.stream.dop == partitions as i32 {
        return build;
    }
    shuffle_by_hash(build, ctx, owner_node_id, keys, partitions)
}

fn ensure_hash_on_input_slots(
    build: PipelineBuildResult,
    ctx: &mut PipelineBuildContext,
    owner_node_id: i32,
    partition_slot_ids: Vec<crate::common::ids::SlotId>,
    distribution_keys: Vec<ExprId>,
    partitions: usize,
) -> PipelineBuildResult {
    let partitions = partitions.max(1);
    if partitions <= 1 {
        let mut build = gather_to_one(build, ctx, owner_node_id);
        build.stream = StreamDesc::single();
        return build;
    }

    let desired = Distribution::Hash {
        keys: distribution_keys.clone(),
        partitions,
        hash_version: 0,
    };
    if build.stream.distribution == desired && build.stream.dop == partitions as i32 {
        return build;
    }
    shuffle_by_hash_on_input_slots(
        build,
        ctx,
        owner_node_id,
        partition_slot_ids,
        distribution_keys,
        partitions,
    )
}

fn output_chunk_schema_for_node(node: &ExecNode) -> Option<crate::exec::chunk::ChunkSchemaRef> {
    match &node.kind {
        ExecNodeKind::AssertNumRows(AssertNumRowsNode { input, .. }) => {
            output_chunk_schema_for_node(input)
        }
        ExecNodeKind::Values(values) => Some(values.chunk.chunk_schema_ref()),
        ExecNodeKind::Project(project) => Some(Arc::clone(&project.output_chunk_schema)),
        ExecNodeKind::Filter(FilterNode { input, .. })
        | ExecNodeKind::Repeat(RepeatNode { input, .. })
        | ExecNodeKind::Limit(LimitNode { input, .. })
        | ExecNodeKind::Sort(SortNode { input, .. }) => output_chunk_schema_for_node(input),
        ExecNodeKind::ChangeEventExpand(node) => Some(Arc::clone(&node.output_chunk_schema)),
        ExecNodeKind::UnionAll(UnionAllNode { inputs, .. }) => {
            inputs.first().and_then(output_chunk_schema_for_node)
        }
        ExecNodeKind::ExchangeSource(exchange) => Some(Arc::clone(&exchange.expected_chunk_schema)),
        ExecNodeKind::Scan(scan) => Some(scan.output_chunk_schema()),
        ExecNodeKind::RuntimeFilterConsumer(consumer) => {
            output_chunk_schema_for_node(&consumer.input)
        }
        ExecNodeKind::Fetch(fetch) => Some(Arc::clone(&fetch.output_chunk_schema)),
        ExecNodeKind::LookUp(lookup) => Some(Arc::clone(&lookup.output_chunk_schema)),
        ExecNodeKind::Aggregate(aggregate) => Some(Arc::clone(&aggregate.output_chunk_schema)),
        ExecNodeKind::Join(join) => Some(Arc::clone(&join.join_scope_chunk_schema)),
        ExecNodeKind::NestedLoopJoin(join) => Some(Arc::clone(&join.join_scope_chunk_schema)),
        ExecNodeKind::TableFunction(table_function) => {
            Some(Arc::clone(&table_function.output_chunk_schema))
        }
        ExecNodeKind::Analytic(analytic) => Some(Arc::clone(&analytic.output_chunk_schema)),
        ExecNodeKind::SetOp(set_op) => Some(Arc::clone(&set_op.output_chunk_schema)),
    }
}

fn keyed_assert_distribution_keys(
    ctx: &mut PipelineBuildContext,
    input: &ExecNode,
    key_slots: &[crate::common::ids::SlotId],
) -> Result<Vec<ExprId>, String> {
    let output_schema = output_chunk_schema_for_node(input).ok_or_else(|| {
        "keyed assert_num_rows local hash requires child output schema".to_string()
    })?;
    let mut distribution_keys = Vec::with_capacity(key_slots.len());
    for slot in key_slots {
        let slot_schema = output_schema.slot(*slot).cloned().ok_or_else(|| {
            format!(
                "keyed assert_num_rows key slot {} is not present in child output schema",
                slot
            )
        })?;
        let arena = Arc::make_mut(&mut ctx.arena);
        let expr_id = arena.push_typed(
            ExprNode::SlotId(*slot),
            slot_schema.field().data_type().clone(),
        );
        arena.set_field_schema(expr_id, slot_schema.field_schema().clone());
        distribution_keys.push(expr_id);
    }
    Ok(distribution_keys)
}

fn existing_hash_distribution_keys_for_slots(
    ctx: &PipelineBuildContext,
    stream: &StreamDesc,
    key_slots: &[crate::common::ids::SlotId],
    partitions: usize,
) -> Option<Vec<ExprId>> {
    let Distribution::Hash {
        keys,
        partitions: existing_partitions,
        hash_version,
    } = &stream.distribution
    else {
        return None;
    };
    if *existing_partitions != partitions || *hash_version != 0 || keys.len() != key_slots.len() {
        return None;
    }
    for (key, slot) in keys.iter().zip(key_slots) {
        let Some(ExprNode::SlotId(existing_slot)) = ctx.arena.node(*key) else {
            return None;
        };
        if existing_slot != slot {
            return None;
        }
    }
    Some(keys.clone())
}

fn build_distinct_set_op_pipeline<S, MakeShared, MakeSink, MakeSource>(
    inputs: &[ExecNode],
    node_id: i32,
    output_chunk_schema: &crate::exec::chunk::ChunkSchemaRef,
    node_name: &'static str,
    controller_name: &'static str,
    ctx: &mut PipelineBuildContext,
    make_shared: MakeShared,
    make_sink: MakeSink,
    make_source: MakeSource,
) -> Result<PipelineBuildResult, String>
where
    S: Clone + 'static,
    MakeShared: FnOnce(SetOpStageController, crate::exec::chunk::ChunkSchemaRef) -> S,
    MakeSink: Fn(usize, S, i32) -> Box<dyn OperatorFactory>,
    MakeSource: Fn(S, i32) -> Box<dyn OperatorFactory>,
{
    if inputs.len() < 2 {
        return Err(format!("{node_name} expects at least 2 inputs"));
    }

    let mut input_builds = Vec::with_capacity(inputs.len());
    for input in inputs {
        input_builds.push(build_pipeline_for_node(input, ctx)?);
    }

    let stage_producers = input_builds
        .iter()
        .map(|b| b.pipeline.dop as usize)
        .collect::<Vec<_>>();
    let controller = SetOpStageController::new(controller_name, stage_producers)?;
    let shared = make_shared(controller, Arc::clone(output_chunk_schema));

    let mut extra_pipelines = Vec::new();
    for (stage, mut child_build) in input_builds.into_iter().enumerate() {
        child_build
            .pipeline
            .factories
            .push(make_sink(stage, shared.clone(), node_id));
        child_build.pipeline.needs_sink = false;
        extra_pipelines.push(child_build.pipeline);
        extra_pipelines.append(&mut child_build.extra_pipelines);
    }

    let source = make_source(shared, node_id);
    let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::any(1),
    })
}

fn native_runtime_filter_context(
    execution: &PipelineRuntimeFilterExecution,
    binding_id: u32,
) -> Result<&NativeRuntimeFilterExecutionContext, String> {
    match execution {
        PipelineRuntimeFilterExecution {
            context: Some(context),
        } => Ok(context),
        PipelineRuntimeFilterExecution { context: None } => Err(format!(
            "native runtime-filter binding_id={binding_id} requires an installed runtime-filter context"
        )),
    }
}

fn native_aggregate_topn_context(
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
    execution: &PipelineRuntimeFilterExecution,
) -> Result<Option<NativeRuntimeFilterExecutionContext>, String> {
    let Some(spec) = specs.first() else {
        return Ok(None);
    };
    native_runtime_filter_context(execution, spec.binding_id)
        .cloned()
        .map(Some)
}

fn validate_native_producer_specs(
    specs: &[crate::exec::node::join::JoinRuntimeFilterProducerBinding],
    ctx: &PipelineBuildContext,
) -> Result<(), String> {
    for spec in specs {
        let context =
            native_runtime_filter_context(&ctx.runtime_filter_execution, spec.binding_id)?;
        let requested_kind = match (&spec.contract, &spec.reduction) {
            (_, RuntimeFilterExecutionReduction::MergeTopKSummary { .. }) => {
                ProducerPortKind::TopKSummary
            }
            (_, _)
                if spec.contribution_kinds.contains(
                    &crate::runtime_filter::model::contract::ContributionKind::FinalDomainShard,
                ) =>
            {
                ProducerPortKind::FinalDomain
            }
            (RuntimeFilterExecutionContract::Membership { .. }, _) => ProducerPortKind::Membership,
            (RuntimeFilterExecutionContract::Ordered { .. }, _) => ProducerPortKind::OrderedBound,
        };
        let resolved = context
            .resolve_producer(
                crate::runtime_filter::model::contract::BindingId::new(spec.binding_id),
                crate::runtime_filter::model::contract::ChannelId::new(spec.channel_id),
                requested_kind,
            )
            .map_err(|error| {
                format!(
                    "native runtime-filter producer binding_id={} resolution failed: {error}",
                    spec.binding_id
                )
            })?;
        validate_native_contract(spec.binding_id, &spec.contract, resolved.contract())?;
        validate_native_reduction(
            spec.binding_id,
            &spec.reduction,
            resolved.reduction_requirement(),
            resolved.topk_contract_digest(),
        )?;
        if &spec.contribution_kinds != resolved.allowed_contribution_kinds() {
            return Err(format!(
                "native runtime-filter producer binding_id={} contribution kinds do not match the installed descriptor",
                spec.binding_id
            ));
        }
        if spec.completion_requirement != resolved.completion_requirement() {
            return Err(format!(
                "native runtime-filter producer binding_id={} completion requirement does not match the installed descriptor",
                spec.binding_id
            ));
        }
    }
    Ok(())
}

fn validate_native_aggregate_topn_specs(
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
    group_by: &[ExprId],
    ctx: &PipelineBuildContext,
) -> Result<(), String> {
    let expected_contributions = std::collections::BTreeSet::from([
        crate::runtime_filter::model::contract::ContributionKind::OrderedBoundUpdate,
        crate::runtime_filter::model::contract::ContributionKind::ProducerClosed,
    ]);
    let mut seen = std::collections::BTreeSet::new();
    for spec in specs {
        if !seen.insert((spec.binding_id, spec.group_key_ordinal)) {
            return Err(format!(
                "native aggregate TopN producer binding_id={} duplicates group key ordinal={}",
                spec.binding_id, spec.group_key_ordinal
            ));
        }
    }
    for spec in specs {
        let expected_expr = group_by.get(spec.group_key_ordinal).ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} targets missing group key ordinal={}, key_count={}",
                spec.binding_id,
                spec.group_key_ordinal,
                group_by.len()
            )
        })?;
        if *expected_expr != spec.group_key_expr_id {
            return Err(format!(
                "native aggregate TopN producer binding_id={} expression does not match group key ordinal={}",
                spec.binding_id, spec.group_key_ordinal
            ));
        }
        let group_key_type = ctx.arena.data_type(spec.group_key_expr_id).ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} group key expression has no type",
                spec.binding_id
            )
        })?;
        let RuntimeFilterExecutionContract::Ordered { keys, .. } = &spec.contract else {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires an ordered contract",
                spec.binding_id
            ));
        };
        if keys.len() != 1 || keys[0].data_type() != group_key_type {
            return Err(format!(
                "native aggregate TopN producer binding_id={} ordered contract must contain exactly one key matching group key type={group_key_type:?}",
                spec.binding_id
            ));
        }
        if spec.reduction != RuntimeFilterExecutionReduction::TightenOrderedBound {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires TightenOrderedBound reduction",
                spec.binding_id
            ));
        }
        if spec.contribution_kinds != expected_contributions {
            return Err(format!(
                "native aggregate TopN producer binding_id={} contribution kinds must be exactly OrderedBoundUpdate and ProducerClosed",
                spec.binding_id
            ));
        }
        if spec.completion_requirement
            != crate::runtime_filter::model::contract::CompletionRequirement::ProducerClosed
        {
            return Err(format!(
                "native aggregate TopN producer binding_id={} completion must be ProducerClosed",
                spec.binding_id
            ));
        }
        let context =
            native_runtime_filter_context(&ctx.runtime_filter_execution, spec.binding_id)?;
        let resolved = context
            .resolve_producer(
                crate::runtime_filter::model::contract::BindingId::new(spec.binding_id),
                crate::runtime_filter::model::contract::ChannelId::new(spec.channel_id),
                ProducerPortKind::OrderedBound,
            )
            .map_err(|error| {
                format!(
                    "native aggregate TopN producer binding_id={} resolution failed: {error}",
                    spec.binding_id
                )
            })?;
        validate_native_contract(spec.binding_id, &spec.contract, resolved.contract())?;
        validate_native_reduction(
            spec.binding_id,
            &spec.reduction,
            resolved.reduction_requirement(),
            resolved.topk_contract_digest(),
        )?;
        if &spec.contribution_kinds != resolved.allowed_contribution_kinds() {
            return Err(format!(
                "native aggregate TopN producer binding_id={} contribution kinds do not match the installed descriptor",
                spec.binding_id
            ));
        }
        if spec.completion_requirement != resolved.completion_requirement() {
            return Err(format!(
                "native aggregate TopN producer binding_id={} completion requirement does not match the installed descriptor",
                spec.binding_id
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateTopNProducerSite {
    AggregateProcessor,
    PartialAggregateProcessor,
    FinalAggregateProcessor,
    StreamingAggregateSink,
    StreamingAggregateSource,
}

#[derive(Clone, Copy, Debug)]
struct AggregateTopNProducerSiteCandidate {
    site: AggregateTopNProducerSite,
    owns_complete_group_identity: bool,
}

fn resolve_aggregate_topn_producer_site(
    binding_id: u32,
    candidates: &[AggregateTopNProducerSiteCandidate],
) -> Result<AggregateTopNProducerSite, String> {
    let mut owners = candidates
        .iter()
        .filter(|candidate| candidate.owns_complete_group_identity)
        .map(|candidate| candidate.site);
    let Some(owner) = owners.next() else {
        return Err(format!(
            "native aggregate TopN producer binding_id={binding_id} has no ownership-safe physical producer site after pipeline expansion"
        ));
    };
    if owners.next().is_some() {
        return Err(format!(
            "native aggregate TopN producer binding_id={binding_id} has multiple ownership-safe physical producer sites after pipeline expansion"
        ));
    }
    Ok(owner)
}

fn resolve_aggregate_topn_producer_site_if_present(
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
    candidates: &[AggregateTopNProducerSiteCandidate],
) -> Result<Option<AggregateTopNProducerSite>, String> {
    specs
        .first()
        .map(|spec| resolve_aggregate_topn_producer_site(spec.binding_id, candidates))
        .transpose()
}

fn aggregate_topn_producers_for_site(
    resolved_site: Option<AggregateTopNProducerSite>,
    physical_site: AggregateTopNProducerSite,
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
) -> Vec<AggregateTopNRuntimeFilterProducerBinding> {
    if resolved_site == Some(physical_site) {
        specs.to_vec()
    } else {
        Vec::new()
    }
}

fn native_join_producer_factory(
    specs: &[crate::exec::node::join::JoinRuntimeFilterProducerBinding],
    build_keys: &[crate::exec::expr::ExprId],
    eq_null_safe: &[bool],
    build_dop: i32,
    ctx: &PipelineBuildContext,
) -> Result<Option<Arc<NativeRuntimeFilterProducerFactory>>, String> {
    if specs.is_empty() {
        return Ok(None);
    }
    let context =
        native_runtime_filter_context(&ctx.runtime_filter_execution, specs[0].binding_id)?;
    Ok(Some(Arc::new(
        NativeRuntimeFilterProducerFactory::from_plan(
            specs,
            build_keys,
            eq_null_safe,
            ctx.arena.as_ref(),
            context.clone(),
            build_dop,
        )?,
    )))
}

fn validate_native_consumer_specs(
    specs: &[RuntimeFilterConsumerBinding],
    ctx: &PipelineBuildContext,
) -> Result<(), String> {
    for spec in specs {
        let context =
            native_runtime_filter_context(&ctx.runtime_filter_execution, spec.binding_id)?;
        let requested_kind = match spec.activation {
            crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot => {
                SubscriptionKind::BlockingSnapshot
            }
            crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive {
                ..
            } => SubscriptionKind::NonBlockingLive,
        };
        let resolved = context
            .resolve_consumer(
                crate::runtime_filter::model::contract::BindingId::new(spec.binding_id),
                crate::runtime_filter::model::contract::ChannelId::new(spec.channel_id),
                requested_kind,
            )
            .map_err(|error| {
                format!(
                    "native runtime-filter consumer binding_id={} resolution failed: {error}",
                    spec.binding_id
                )
            })?;
        if spec.activation != resolved.activation() {
            return Err(format!(
                "native runtime-filter consumer binding_id={} activation does not match the installed descriptor",
                spec.binding_id
            ));
        }
        if &spec.capabilities != resolved.capabilities() {
            return Err(format!(
                "native runtime-filter consumer binding_id={} capabilities do not match the installed descriptor",
                spec.binding_id
            ));
        }
        validate_native_contract(spec.binding_id, &spec.contract, resolved.contract())?;
        validate_native_reduction(
            spec.binding_id,
            &spec.reduction,
            resolved.reduction_requirement(),
            resolved.topk_contract_digest(),
        )?;
    }
    Ok(())
}

fn validate_native_contract(
    binding_id: u32,
    expected: &RuntimeFilterExecutionContract,
    installed: &InstalledRuntimeFilterExecutionContract,
) -> Result<(), String> {
    let matches = match (expected, installed) {
        (
            RuntimeFilterExecutionContract::Membership {
                canonical_schema,
                schema_digest,
            },
            InstalledRuntimeFilterExecutionContract::Membership {
                canonical_schema: installed_schema,
                schema_digest: installed_digest,
            },
        ) => {
            canonical_schema.as_ref() == installed_schema.as_ref()
                && schema_digest == installed_digest
        }
        (
            RuntimeFilterExecutionContract::Ordered {
                keys,
                comparator_digest,
                order_contract_digest,
            },
            InstalledRuntimeFilterExecutionContract::Ordered {
                keys: installed_keys,
                comparator_digest: installed_comparator,
                order_contract_digest: installed_order,
            },
        ) => {
            keys.as_ref() == installed_keys.as_ref()
                && comparator_digest == installed_comparator
                && order_contract_digest == installed_order
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "native runtime-filter binding_id={binding_id} schema/contract does not match the installed descriptor"
        ))
    }
}

fn validate_native_reduction(
    binding_id: u32,
    expected: &RuntimeFilterExecutionReduction,
    installed: ReductionRequirement,
    installed_topk_contract_digest: Option<[u8; 32]>,
) -> Result<(), String> {
    let matches = match (expected, installed) {
        (RuntimeFilterExecutionReduction::SetUnion, ReductionRequirement::SetUnion)
        | (
            RuntimeFilterExecutionReduction::TightenOrderedBound,
            ReductionRequirement::TightenOrderedBound,
        ) => true,
        (
            RuntimeFilterExecutionReduction::MergeTopKSummary { k, contract_digest },
            ReductionRequirement::MergeTopKSummary(installed),
        ) => *k == installed.k() && installed_topk_contract_digest == Some(*contract_digest),
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(format!(
            "native runtime-filter binding_id={binding_id} reduction does not match the installed descriptor"
        ))
    }
}

fn build_pipeline_for_node(
    node: &ExecNode,
    ctx: &mut PipelineBuildContext,
) -> Result<PipelineBuildResult, String> {
    match &node.kind {
        ExecNodeKind::RuntimeFilterConsumer(consumer) => {
            validate_native_consumer_specs(&consumer.bindings, ctx)?;
            let mut build = build_pipeline_for_node(&consumer.input, ctx)?;
            if !consumer.bindings.is_empty() {
                build
                    .pipeline
                    .factories
                    .push(Box::new(NativeRuntimeFilterProcessorFactory::new(
                        consumer.owner_node_id,
                        &consumer.bindings,
                        Arc::clone(&ctx.arena),
                    )?));
            }
            Ok(build)
        }
        ExecNodeKind::AssertNumRows(AssertNumRowsNode {
            input,
            node_id,
            mode,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            if let AssertNumRowsMode::PerKeyAtMostOne { key_slots, .. } = mode {
                let partition_count = ctx.pipeline_dop.max(1) as usize;
                let distribution_keys = existing_hash_distribution_keys_for_slots(
                    ctx,
                    &build.stream,
                    key_slots,
                    partition_count,
                )
                .map(Ok)
                .unwrap_or_else(|| keyed_assert_distribution_keys(ctx, input, key_slots))?;
                build = ensure_hash_on_input_slots(
                    build,
                    ctx,
                    *node_id,
                    key_slots.clone(),
                    distribution_keys,
                    partition_count,
                );
            }
            build
                .pipeline
                .factories
                .push(Box::new(AssertNumRowsProcessorFactory::new(
                    *node_id,
                    mode.clone(),
                )?));
            Ok(build)
        }
        ExecNodeKind::Project(ProjectNode {
            input,
            node_id,
            is_subordinate,
            exprs,
            expr_slot_ids,
            expr_slot_schemas,
            output_indices,
            output_chunk_schema,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(ProjectProcessorFactory::new(
                    *node_id,
                    *is_subordinate,
                    Arc::clone(&ctx.arena),
                    exprs.clone(),
                    expr_slot_ids.clone(),
                    expr_slot_schemas.clone(),
                    output_indices.clone(),
                    output_chunk_schema.clone(),
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ExecNodeKind::Filter(FilterNode {
            input,
            node_id,
            predicate,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(FilterProcessorFactory::new(
                    *node_id,
                    Arc::clone(&ctx.arena),
                    *predicate,
                )));
            Ok(build)
        }
        ExecNodeKind::Repeat(RepeatNode {
            input,
            node_id,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(RepeatProcessorFactory::new(
                    *node_id,
                    null_slot_ids.clone(),
                    grouping_slot_ids.clone(),
                    grouping_list.clone(),
                    *repeat_times,
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ExecNodeKind::ChangeEventExpand(node) => {
            let mut build = build_pipeline_for_node(&node.input, ctx)?;
            let factory = ChangeEventExpandProcessorFactory::new(
                node.node_id,
                Arc::clone(&ctx.arena),
                node.events.clone(),
                node.output_chunk_schema.clone(),
                node.output_slot_ids.clone(),
                node.change_op_slot_id,
                node.data_route_slot_id,
            )?;
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ExecNodeKind::Limit(LimitNode {
            input,
            node_id,
            limit,
            offset,
        }) => {
            let build = build_pipeline_for_node(input, ctx)?;
            let mut build = gather_to_one(build, ctx, *node_id);
            build
                .pipeline
                .factories
                .push(Box::new(LimitProcessorFactory::new(
                    *node_id, *limit, *offset,
                )));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ExecNodeKind::Sort(SortNode {
            input,
            node_id,
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
            partition_exprs,
            partition_limit,
        }) => {
            let build = build_pipeline_for_node(input, ctx)?;
            let mut build = gather_to_one(build, ctx, *node_id);
            if *use_top_n {
                build
                    .pipeline
                    .factories
                    .push(Box::new(SortProcessorFactory::new_topn(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        order_by.clone(),
                        *limit,
                        *offset,
                        *topn_type,
                        *max_buffered_rows,
                        *max_buffered_bytes,
                        partition_exprs.clone(),
                        *partition_limit,
                    )));
            } else {
                build
                    .pipeline
                    .factories
                    .push(Box::new(SortProcessorFactory::new(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        order_by.clone(),
                        *limit,
                        *offset,
                        *topn_type,
                        *max_buffered_rows,
                        *max_buffered_bytes,
                        partition_exprs.clone(),
                        *partition_limit,
                    )));
            }
            build.stream = StreamDesc::single();
            Ok(build)
        }
        ExecNodeKind::TableFunction(TableFunctionNode {
            input,
            node_id,
            function_name,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            param_types,
            ret_types,
            output_chunk_schema,
            output_slot_sources,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(TableFunctionProcessorFactory::new(
                    *node_id,
                    function_name.clone(),
                    param_slots.clone(),
                    outer_slots.clone(),
                    fn_result_slots.clone(),
                    *fn_result_required,
                    *is_left_join,
                    param_types.clone(),
                    ret_types.clone(),
                    Arc::clone(output_chunk_schema),
                    output_slot_sources.clone(),
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        ExecNodeKind::Analytic(AnalyticNode {
            input,
            node_id,
            partition_exprs,
            order_by_exprs,
            functions,
            window,
            output_columns,
            output_chunk_schema,
        }) => {
            let build = build_pipeline_for_node(input, ctx)?;
            let mut build = gather_to_one(build, ctx, *node_id);

            let state = AnalyticSharedState::new(
                Arc::clone(&ctx.arena),
                partition_exprs.clone(),
                order_by_exprs.clone(),
                functions.clone(),
                window.clone(),
                output_columns.clone(),
                Arc::clone(output_chunk_schema),
                *node_id,
            );

            build
                .pipeline
                .factories
                .push(Box::new(AnalyticSinkFactory::new(state.clone())));
            build.pipeline.needs_sink = false;

            let source_factory = Box::new(AnalyticSourceFactory::new(state));
            let downstream = new_source_pipeline_with_dop(ctx, source_factory, 1);

            let mut extra_pipelines = build.extra_pipelines;
            extra_pipelines.push(build.pipeline);

            Ok(PipelineBuildResult {
                pipeline: downstream,
                extra_pipelines,
                stream: StreamDesc::single(),
            })
        }
        ExecNodeKind::Aggregate(AggregateNode {
            input,
            node_id,
            group_by,
            functions,
            need_finalize,
            input_is_intermediate: _input_is_intermediate,
            output_chunk_schema,
            runtime_filter_spec,
            streaming_preaggregation_mode,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            let output_slots = output_chunk_schema.slot_ids();
            let AggregateRuntimeFilterSpec { topn_producers } = runtime_filter_spec;
            validate_native_aggregate_topn_specs(topn_producers, group_by, ctx)?;
            let native_topn_producers = topn_producers.as_slice();

            let dop = build.pipeline.dop.max(1);
            let all_update = functions.iter().all(|f| !f.input_is_intermediate);

            if !*need_finalize && !group_by.is_empty() && dop > 1 {
                // StarRocks pipeline semantics: when an aggregate runs with pipeline DOP > 1, all
                // rows for a given group key must be processed by the same driver within the
                // fragment instance. Otherwise, per-driver aggregation can emit duplicate groups,
                // which breaks correctness when a downstream operator assumes "one row per group",
                // e.g.:
                // - merge-stage group-by aggregates (DISTINCT rewrites)
                // - intermediate-output group-by aggregates (need_finalize=false) with an upstream
                //   Sort+LIMIT top-N over group keys (TPC-DS Q7/Q26)
                //
                // StarRocks' exchange receiver channels provide this guarantee; we emulate it by
                // inserting a local hash shuffle on the group keys before running the aggregate.
                //
                // NOTE: We intentionally do this regardless of function phase (update vs merge),
                // because the requirement is about group-key ownership under parallelism.
                build = ensure_hash(build, ctx, *node_id, group_by.clone(), dop as usize);
            }
            if *need_finalize && !group_by.is_empty() && dop > 1 && all_update {
                let producer_site = resolve_aggregate_topn_producer_site_if_present(
                    native_topn_producers,
                    &[
                        AggregateTopNProducerSiteCandidate {
                            site: AggregateTopNProducerSite::PartialAggregateProcessor,
                            owns_complete_group_identity: false,
                        },
                        AggregateTopNProducerSiteCandidate {
                            site: AggregateTopNProducerSite::FinalAggregateProcessor,
                            owns_complete_group_identity: true,
                        },
                    ],
                )?;
                // StarRocks-aligned two-phase hash aggregation:
                // - Partial aggregation per upstream driver
                // - Hash shuffle by group keys
                // - Final aggregation merges intermediate states
                let mut partial_functions = functions.clone();
                for func in &mut partial_functions {
                    func.input_is_intermediate = false;
                }

                let partial_agg_factory: Box<dyn OperatorFactory> = {
                    let topn_producers = aggregate_topn_producers_for_site(
                        producer_site,
                        AggregateTopNProducerSite::PartialAggregateProcessor,
                        native_topn_producers,
                    );
                    let runtime_filter_context = native_aggregate_topn_context(
                        &topn_producers,
                        &ctx.runtime_filter_execution,
                    )?;
                    Box::new(AggregateProcessorFactory::new_native(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        partial_functions,
                        true,
                        false,
                        output_chunk_schema.clone(),
                        topn_producers,
                        runtime_filter_context,
                        dop,
                        None,
                    )?)
                };

                build.pipeline.factories.push(partial_agg_factory);

                if output_slots.len() < group_by.len() {
                    return Err(format!(
                        "aggregate output slots missing group keys: group_by={} output_slots={}",
                        group_by.len(),
                        output_slots.len()
                    ));
                }
                let partition_slot_ids = output_slots[..group_by.len()].to_vec();
                let partition_count = dop as usize;
                let mut build = ensure_hash_on_input_slots(
                    build,
                    ctx,
                    *node_id,
                    partition_slot_ids,
                    group_by.clone(),
                    partition_count,
                );
                let mut merge_functions = functions.clone();
                for func in &mut merge_functions {
                    func.input_is_intermediate = true;
                }
                let final_topn_producers = aggregate_topn_producers_for_site(
                    producer_site,
                    AggregateTopNProducerSite::FinalAggregateProcessor,
                    native_topn_producers,
                );
                let final_runtime_filter_context = native_aggregate_topn_context(
                    &final_topn_producers,
                    &ctx.runtime_filter_execution,
                )?;
                build
                    .pipeline
                    .factories
                    .push(Box::new(AggregateProcessorFactory::new_native(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        merge_functions,
                        false,
                        true,
                        output_chunk_schema.clone(),
                        final_topn_producers,
                        final_runtime_filter_context,
                        build.pipeline.dop,
                        None,
                    )?));
                return Ok(build);
            }

            if *need_finalize && group_by.is_empty() && dop > 1 && all_update {
                let mut partial_functions = functions.clone();
                for func in &mut partial_functions {
                    func.input_is_intermediate = false;
                }
                let local_factory: Box<dyn OperatorFactory> = {
                    Box::new(AggregateProcessorFactory::new_native(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        partial_functions,
                        true,
                        false,
                        output_chunk_schema.clone(),
                        Vec::new(),
                        None,
                        dop,
                        None,
                    )?)
                };
                build.pipeline.factories.push(local_factory);

                let partition_count = 1usize;
                let exchanger = LocalExchanger::new(
                    partition_count,
                    dop as usize,
                    LocalExchangePartitionSpec::Single,
                    Arc::clone(&ctx.arena),
                );
                build
                    .pipeline
                    .factories
                    .push(Box::new(LocalExchangeSinkFactory::new(
                        *node_id,
                        Arc::clone(&exchanger),
                    )));
                build.pipeline.needs_sink = false;

                let source_factory = Box::new(LocalExchangeSourceFactory::new(
                    *node_id,
                    partition_count,
                    exchanger,
                ));
                let mut downstream =
                    new_source_pipeline_with_dop(ctx, source_factory, partition_count as i32);
                let downstream_dop = downstream.dop;
                let mut merge_functions = functions.clone();
                for func in &mut merge_functions {
                    func.input_is_intermediate = true;
                }
                downstream
                    .factories
                    .push(Box::new(AggregateProcessorFactory::new_native(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        merge_functions,
                        false,
                        true,
                        output_chunk_schema.clone(),
                        Vec::new(),
                        None,
                        downstream_dop,
                        None,
                    )?));

                let mut extra_pipelines = build.extra_pipelines;
                extra_pipelines.push(build.pipeline);

                return Ok(PipelineBuildResult {
                    pipeline: downstream,
                    extra_pipelines,
                    stream: StreamDesc::any(downstream_dop),
                });
            }

            // Streaming pre-aggregation: split into Sink (Pipeline 1) and Source (Pipeline 2).
            // This creates a pipeline boundary that enables TopN runtime filter yield points.
            // The ensure_hash above (for !need_finalize && group_by && dop > 1) already
            // guarantees group-key ownership per driver, so the per-driver streaming aggregate
            // won't produce duplicate groups.
            if matches!(
                streaming_preaggregation_mode,
                Some(StreamingPreaggregationMode::ForcePreaggregation)
            ) {
                let producer_site = resolve_aggregate_topn_producer_site_if_present(
                    native_topn_producers,
                    &[
                        AggregateTopNProducerSiteCandidate {
                            site: AggregateTopNProducerSite::StreamingAggregateSink,
                            owns_complete_group_identity: true,
                        },
                        AggregateTopNProducerSiteCandidate {
                            site: AggregateTopNProducerSite::StreamingAggregateSource,
                            owns_complete_group_identity: false,
                        },
                    ],
                )?;
                let streaming_state = AggregateStreamingState::new(dop.max(1) as usize);
                let sink_factory: Box<dyn OperatorFactory> = {
                    let topn_producers = aggregate_topn_producers_for_site(
                        producer_site,
                        AggregateTopNProducerSite::StreamingAggregateSink,
                        native_topn_producers,
                    );
                    let runtime_filter_context = native_aggregate_topn_context(
                        &topn_producers,
                        &ctx.runtime_filter_execution,
                    )?;
                    Box::new(AggregateStreamingSinkFactory::new_native(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        functions.clone(),
                        !*need_finalize,
                        output_chunk_schema.clone(),
                        streaming_state.clone(),
                        topn_producers,
                        runtime_filter_context,
                        dop,
                    )?)
                };
                build.pipeline.factories.push(sink_factory);
                build.pipeline.needs_sink = false;

                let source_factory = Box::new(AggregateStreamingSourceFactory::new(
                    *node_id,
                    streaming_state,
                ));
                let source_dop = build.pipeline.dop;
                let downstream = new_source_pipeline_with_dop(ctx, source_factory, source_dop);

                let mut extra_pipelines = build.extra_pipelines;
                extra_pipelines.push(build.pipeline);

                return Ok(PipelineBuildResult {
                    pipeline: downstream,
                    extra_pipelines,
                    stream: StreamDesc::any(source_dop),
                });
            }

            let producer_site = resolve_aggregate_topn_producer_site_if_present(
                native_topn_producers,
                &[AggregateTopNProducerSiteCandidate {
                    site: AggregateTopNProducerSite::AggregateProcessor,
                    owns_complete_group_identity: true,
                }],
            )?;
            let agg_factory: Box<dyn OperatorFactory> = {
                let topn_producers = aggregate_topn_producers_for_site(
                    producer_site,
                    AggregateTopNProducerSite::AggregateProcessor,
                    native_topn_producers,
                );
                let runtime_filter_context =
                    native_aggregate_topn_context(&topn_producers, &ctx.runtime_filter_execution)?;
                Box::new(AggregateProcessorFactory::new_native(
                    *node_id,
                    Arc::clone(&ctx.arena),
                    group_by.clone(),
                    functions.clone(),
                    !*need_finalize,
                    false,
                    output_chunk_schema.clone(),
                    topn_producers,
                    runtime_filter_context,
                    dop,
                    None,
                )?)
            };

            if *need_finalize && dop > 1 {
                let partition_count = if group_by.is_empty() { 1 } else { dop as usize };
                let partition_spec = if partition_count <= 1 {
                    LocalExchangePartitionSpec::Single
                } else {
                    LocalExchangePartitionSpec::Exprs(group_by.clone())
                };
                let exchanger = LocalExchanger::new(
                    partition_count,
                    dop as usize,
                    partition_spec,
                    Arc::clone(&ctx.arena),
                );
                build
                    .pipeline
                    .factories
                    .push(Box::new(LocalExchangeSinkFactory::new(
                        *node_id,
                        Arc::clone(&exchanger),
                    )));
                build.pipeline.needs_sink = false;

                let source_factory = Box::new(LocalExchangeSourceFactory::new(
                    *node_id,
                    partition_count,
                    exchanger,
                ));
                let mut downstream =
                    new_source_pipeline_with_dop(ctx, source_factory, partition_count as i32);
                let downstream_dop = downstream.dop;
                downstream.factories.push(agg_factory);

                let mut extra_pipelines = build.extra_pipelines;
                extra_pipelines.push(build.pipeline);

                Ok(PipelineBuildResult {
                    pipeline: downstream,
                    extra_pipelines,
                    stream: StreamDesc::any(downstream_dop),
                })
            } else {
                build.pipeline.factories.push(agg_factory);
                build.stream = StreamDesc::any(build.pipeline.dop);
                Ok(build)
            }
        }
        ExecNodeKind::Join(JoinNode {
            left,
            right,
            node_id,
            join_type,
            distribution_mode,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_chunk_schema,
            probe_keys,
            build_keys,
            eq_null_safe,
            residual_predicate,
            runtime_filter_execution,
        }) => {
            validate_native_producer_specs(&runtime_filter_execution.producers, ctx)?;
            let left_build = build_pipeline_for_node(left, ctx)?;
            let right_build = build_pipeline_for_node(right, ctx)?;

            let probe_is_left = *join_type != JoinType::RightSemi;
            let (mut probe_build, mut build_build) = if probe_is_left {
                (left_build, right_build)
            } else {
                (right_build, left_build)
            };
            let probe_keys = probe_keys.clone();
            let build_keys = build_keys.clone();
            let eq_null_safe = eq_null_safe.clone();
            let has_equi_keys = !probe_keys.is_empty() && !build_keys.is_empty();

            if *distribution_mode == JoinDistributionMode::Broadcast {
                if *join_type == JoinType::FullOuter {
                    probe_build = gather_to_one(probe_build, ctx, *node_id);
                }
                build_build = gather_to_one(build_build, ctx, *node_id);
                let probe_dop = probe_build.pipeline.dop.max(1) as usize;
                let join_state = Arc::new(BroadcastJoinSharedState::new(
                    *node_id,
                    ctx.dep_manager.clone(),
                    probe_dop,
                ));

                let probe_factory = {
                    BroadcastJoinProbeProcessorFactory::new_native(
                        Arc::clone(&ctx.arena),
                        *join_type,
                        probe_keys.clone(),
                        *residual_predicate,
                        probe_is_left,
                        has_equi_keys,
                        Arc::clone(left_chunk_schema),
                        Arc::clone(right_chunk_schema),
                        Arc::clone(join_scope_chunk_schema),
                        Arc::clone(&join_state),
                    )
                };
                probe_build.pipeline.factories.push(Box::new(probe_factory));

                let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
                let native_producers = native_join_producer_factory(
                    &runtime_filter_execution.producers,
                    &build_keys,
                    &eq_null_safe,
                    build_build.pipeline.dop,
                    ctx,
                )?;
                let build_factory = {
                    HashJoinBuildSinkFactory::new_native_with_runtime_filters(
                        Arc::clone(&ctx.arena),
                        *join_type,
                        residual_predicate.is_some(),
                        probe_is_left,
                        has_equi_keys,
                        build_keys.clone(),
                        eq_null_safe.clone(),
                        *distribution_mode,
                        build_state,
                        native_producers,
                    )
                };
                build_build.pipeline.factories.push(Box::new(build_factory));
                build_build.pipeline.needs_sink = false;

                let mut extra_pipelines = Vec::new();
                extra_pipelines.append(&mut probe_build.extra_pipelines);
                extra_pipelines.append(&mut build_build.extra_pipelines);
                extra_pipelines.push(build_build.pipeline);

                let dop = probe_build.pipeline.dop;
                return Ok(PipelineBuildResult {
                    pipeline: probe_build.pipeline,
                    extra_pipelines,
                    stream: StreamDesc::any(dop),
                });
            }

            // Partitioned INNER hash join (StarRocks-aligned):
            // - Hash shuffle both sides by join keys into the same partition count.
            // - Each probe partition waits for its corresponding build partition to be ready.
            if probe_keys.is_empty() || build_keys.is_empty() {
                // Cross join is not partitionable in current implementation.
                probe_build = gather_to_one(probe_build, ctx, *node_id);
                build_build = gather_to_one(build_build, ctx, *node_id);
            }

            let join_partitions = probe_build
                .pipeline
                .dop
                .max(1)
                .max(build_build.pipeline.dop.max(1)) as usize;
            if !probe_keys.is_empty() {
                probe_build = ensure_hash(
                    probe_build,
                    ctx,
                    *node_id,
                    probe_keys.clone(),
                    join_partitions,
                );
            }
            if !build_keys.is_empty() {
                build_build = ensure_hash(
                    build_build,
                    ctx,
                    *node_id,
                    build_keys.clone(),
                    join_partitions,
                );
            }

            let join_state = Arc::new(PartitionedJoinSharedState::new(
                *node_id,
                join_partitions,
                ctx.dep_manager.clone(),
                *join_type == JoinType::NullAwareLeftAnti,
            ));

            let probe_factory = {
                PartitionedJoinProbeProcessorFactory::new_native(
                    Arc::clone(&ctx.arena),
                    *join_type,
                    probe_keys.clone(),
                    *residual_predicate,
                    probe_is_left,
                    has_equi_keys,
                    Arc::clone(left_chunk_schema),
                    Arc::clone(right_chunk_schema),
                    Arc::clone(join_scope_chunk_schema),
                    Arc::clone(&join_state),
                )
            };
            probe_build.pipeline.factories.push(Box::new(probe_factory));

            let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
            let native_producers = native_join_producer_factory(
                &runtime_filter_execution.producers,
                &build_keys,
                &eq_null_safe,
                build_build.pipeline.dop,
                ctx,
            )?;
            let build_factory = {
                HashJoinBuildSinkFactory::new_native_with_runtime_filters(
                    Arc::clone(&ctx.arena),
                    *join_type,
                    residual_predicate.is_some(),
                    probe_is_left,
                    has_equi_keys,
                    build_keys.clone(),
                    eq_null_safe.clone(),
                    *distribution_mode,
                    build_state,
                    native_producers,
                )
            };
            build_build.pipeline.factories.push(Box::new(build_factory));
            build_build.pipeline.needs_sink = false;

            let mut extra_pipelines = Vec::new();
            extra_pipelines.append(&mut probe_build.extra_pipelines);
            extra_pipelines.append(&mut build_build.extra_pipelines);
            extra_pipelines.push(build_build.pipeline);

            let dop = probe_build.pipeline.dop;
            Ok(PipelineBuildResult {
                pipeline: probe_build.pipeline,
                extra_pipelines,
                stream: StreamDesc::any(dop),
            })
        }
        ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
            left,
            right,
            node_id,
            join_type,
            join_conjunct,
            left_chunk_schema,
            right_chunk_schema,
            join_scope_chunk_schema,
        }) => {
            let probe_is_left = *join_type != NestedLoopJoinType::RightOuter;
            let (probe_child, build_child) = if probe_is_left {
                (left, right)
            } else {
                (right, left)
            };

            let mut probe_build = build_pipeline_for_node(probe_child, ctx)?;
            let mut build_build = build_pipeline_for_node(build_child, ctx)?;

            build_build = gather_to_one(build_build, ctx, *node_id);

            let probe_producers = probe_build.pipeline.dop.max(1) as usize;
            let state = Arc::new(NlJoinSharedState::new(
                *node_id,
                probe_producers,
                ctx.dep_manager.clone(),
            ));

            probe_build
                .pipeline
                .factories
                .push(Box::new(NlJoinProbeProcessorFactory::new(
                    Arc::clone(&ctx.arena),
                    *join_type,
                    *join_conjunct,
                    probe_is_left,
                    Arc::clone(left_chunk_schema),
                    Arc::clone(right_chunk_schema),
                    Arc::clone(join_scope_chunk_schema),
                    Arc::clone(&state),
                )));

            build_build
                .pipeline
                .factories
                .push(Box::new(NlJoinBuildSinkFactory::new(Arc::clone(&state))));
            build_build.pipeline.needs_sink = false;

            let mut extra_pipelines = Vec::new();
            extra_pipelines.append(&mut probe_build.extra_pipelines);
            extra_pipelines.append(&mut build_build.extra_pipelines);
            extra_pipelines.push(build_build.pipeline);

            let dop = probe_build.pipeline.dop;
            Ok(PipelineBuildResult {
                pipeline: probe_build.pipeline,
                extra_pipelines,
                stream: StreamDesc::any(dop),
            })
        }
        ExecNodeKind::UnionAll(UnionAllNode { inputs, node_id }) => {
            let mut input_builds = Vec::with_capacity(inputs.len());
            let mut producer_count = 0usize;
            for input in inputs {
                let child_build = build_pipeline_for_node(input, ctx)?;
                producer_count = producer_count.saturating_add(child_build.pipeline.dop as usize);
                input_builds.push(child_build);
            }
            let state = UnionAllSharedState::new(producer_count.max(1), *node_id);

            let mut extra_pipelines = Vec::new();
            for mut child_build in input_builds {
                child_build
                    .pipeline
                    .factories
                    .push(Box::new(UnionAllSinkFactory::new(state.clone(), *node_id)));
                child_build.pipeline.needs_sink = false;
                extra_pipelines.push(child_build.pipeline);
                extra_pipelines.append(&mut child_build.extra_pipelines);
            }

            let source = Box::new(UnionAllSourceFactory::new(state, *node_id));
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines,
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ExecNodeKind::SetOp(SetOpNode {
            kind,
            inputs,
            node_id,
            output_chunk_schema,
        }) => match kind {
            SetOpKind::Intersect => build_distinct_set_op_pipeline(
                inputs,
                *node_id,
                output_chunk_schema,
                "INTERSECT_NODE",
                "intersect",
                ctx,
                |controller, output_chunk_schema| {
                    IntersectSharedState::new(controller, output_chunk_schema)
                },
                |stage, shared, id| Box::new(IntersectSinkFactory::new(stage, shared, id)),
                |shared, id| Box::new(IntersectSourceFactory::new(shared, id)),
            ),
            SetOpKind::Except => build_distinct_set_op_pipeline(
                inputs,
                *node_id,
                output_chunk_schema,
                "EXCEPT_NODE",
                "except",
                ctx,
                |controller, output_chunk_schema| {
                    ExceptSharedState::new(controller, output_chunk_schema)
                },
                |stage, shared, id| Box::new(ExceptSinkFactory::new(stage, shared, id)),
                |shared, id| Box::new(ExceptSourceFactory::new(shared, id)),
            ),
        },
        ExecNodeKind::Values(ValuesNode { chunk, node_id }) => {
            let source: Box<dyn OperatorFactory> =
                Box::new(ValuesSourceFactory::new(chunk.clone(), *node_id));
            let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(1),
            })
        }
        ExecNodeKind::ExchangeSource(node) => {
            validate_native_consumer_specs(node.native_runtime_filter_specs(), ctx)?;
            let binding = ctx
                .exchange_bindings
                .get(node.node_id)
                .ok_or_else(|| format!("missing exchange binding for node {}", node.node_id))?;
            let factory =
                ExchangeSourceFactory::new_native(node.clone(), binding, Arc::clone(&ctx.arena))?;
            let source: Box<dyn OperatorFactory> = Box::new(factory);
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ExecNodeKind::LookUp(lookup) => {
            let source: Box<dyn OperatorFactory> =
                Box::new(LookUpSourceFactory::new(lookup.node_id));
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ExecNodeKind::Scan(scan) => {
            validate_native_consumer_specs(scan.native_runtime_filter_specs(), ctx)?;
            // The bound op is materialized per-instance in `ScanBindings`
            // (see `runtime::fragment::scan::materialize_scan_bindings`); the
            // node itself is static and only supplies its source + config.
            let node_id = scan.node_id().ok_or_else(|| {
                "scan node missing node_id; cannot resolve scan binding".to_string()
            })?;
            let op = ctx
                .scan_bindings
                .get(node_id)
                .ok_or_else(|| format!("missing scan binding for node {node_id}"))?;
            let factory = ScanSourceFactory::new_native(scan.clone(), op, Arc::clone(&ctx.arena))?;
            let source: Box<dyn OperatorFactory> = Box::new(factory);
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ExecNodeKind::Fetch(fetch) => {
            let mut child_build = build_pipeline_for_node(&fetch.input, ctx)?;
            child_build
                .pipeline
                .factories
                .push(Box::new(FetchProcessorFactory::new(
                    fetch.node_id,
                    fetch.target_node_id,
                    fetch.row_pos_descs.clone(),
                    fetch.output_slots_by_tuple.clone(),
                    fetch.nodes_info.clone(),
                    fetch.output_chunk_schema.clone(),
                    Arc::clone(&ctx.lookup_client),
                )));
            Ok(PipelineBuildResult {
                pipeline: child_build.pipeline,
                extra_pipelines: child_build.extra_pipelines,
                stream: child_build.stream,
            })
        }
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
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        AggregateTopNProducerSite, AggregateTopNProducerSiteCandidate,
        build_native_pipeline_graph_for_exec_plan_with_dop,
        build_native_pipeline_graph_for_exec_plan_with_root_sink_dop,
        build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context,
        resolve_aggregate_topn_producer_site,
    };
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{
        AggFunction, AggTypeSignature, AggregateNode, AggregateRuntimeFilterSpec,
    };
    use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
    use crate::exec::node::join::{
        JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
    };
    use crate::exec::node::lookup::LookUpNode;
    use crate::exec::node::runtime_filter::{
        RuntimeFilterConsumerBinding, RuntimeFilterConsumerNode, RuntimeFilterExecutionContract,
        RuntimeFilterExecutionReduction,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    use crate::exec::pipeline::dependency::DependencyManager;

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    fn lookup_node(node_id: i32, output_chunk_schema: ChunkSchemaRef) -> ExecNode {
        ExecNode {
            kind: ExecNodeKind::LookUp(LookUpNode {
                node_id,
                row_pos_descs: HashMap::new(),
                output_chunk_schema,
            }),
        }
    }

    fn assert_dormant_consumer_installs_fail_open_processor_without_dependency_or_wait(
        activation: crate::runtime_filter::model::contract::ConsumerActivation,
    ) {
        let (_manager, runtime_filter_context) =
            installed_native_consumer_context_for_activation(activation);
        let schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            &[SlotId::new(1)],
        );
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let baseline = ExecPlan {
            arena: arena.clone(),
            root: lookup_node(1, Arc::clone(&schema)),
        };
        let build = |plan: &ExecPlan| {
            build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
                plan,
                false,
                DependencyManager::new(),
                None,
                ExchangeBindings::default(),
                ScanBindings::default(),
                2,
                Some(runtime_filter_context.clone()),
            )
            .expect("graph")
        };
        let baseline = build(&baseline);
        let wrapped = ExecPlan {
            arena: arena.clone(),
            root: ExecNode {
                kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode {
                    input: Box::new(lookup_node(1, Arc::clone(&schema))),
                    owner_node_id: 2,
                    bindings: vec![RuntimeFilterConsumerBinding {
                        binding_id: 4,
                        channel_id: 1,
                        expr_id,
                        activation,
                        capabilities: BTreeSet::from([
                            crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                            crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                        ]),
                        contract: installed_native_membership_contract_for_activation(activation),
                        reduction: RuntimeFilterExecutionReduction::SetUnion,
                    }],
                }),
            },
        };
        let wrapped = build(&wrapped);
        assert_eq!(wrapped.pipelines.len(), baseline.pipelines.len());
        assert_eq!(
            wrapped.pipelines[0].factories.len(),
            baseline.pipelines[0].factories.len() + 1
        );
        assert_eq!(
            wrapped.pipelines[0]
                .factories
                .last()
                .expect("fail-open factory")
                .name(),
            "NativeRuntimeFilter (id=2)"
        );
    }

    fn installed_native_consumer_context() -> (
        Arc<crate::runtime::query_context::QueryContextManager>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        installed_native_consumer_context_for_activation(
            crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
        )
    }

    fn installed_native_consumer_context_for_activation(
        activation: crate::runtime_filter::model::contract::ConsumerActivation,
    ) -> (
        Arc<crate::runtime::query_context::QueryContextManager>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        use std::time::Duration;

        use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
        use crate::runtime::query_context::QueryContextManager;

        let manager = QueryContextManager::new_for_test();
        let query_id = crate::runtime::query_context::QueryId { hi: 70, lo: 9_201 };
        let lifecycle = RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(11),
            query_expire: Duration::from_secs(29),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(5),
            transport_max_pending_entries: 128,
            transport_max_pending_bytes: 1024 * 1024,
        };
        manager
            .ensure_native_context(
                query_id,
                false,
                lifecycle.delivery_expire,
                lifecycle.query_expire,
            )
            .expect("native query context");
        manager
            .install_runtime_filter_deployment(
                query_id,
                lifecycle,
                crate::runtime::query_context::runtime_filter_service_lifecycle_tests::participant_install_with_consumer(
                    activation,
                    BTreeSet::from([
                        crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                        crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                    ]),
                ),
            )
            .expect("runtime-filter deployment");
        let context = manager
            .runtime_filter_context_for_native_execution(
                query_id,
                crate::common::types::UniqueId { hi: 70, lo: 40 },
            )
            .expect("strict native runtime-filter context");
        (manager, context)
    }

    fn installed_native_membership_contract_for_activation(
        activation: crate::runtime_filter::model::contract::ConsumerActivation,
    ) -> RuntimeFilterExecutionContract {
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

        let installed_schema = ArtifactMembershipSchema::new(
            &DataType::Int64,
            if matches!(
                activation,
                crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive { .. }
            ) {
                crate::runtime_filter::model::contract::NullSemantics::NullSafeEqual
            } else {
                crate::runtime_filter::model::contract::NullSemantics::NeverMatches
            },
        )
        .expect("membership schema");
        RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::from(installed_schema.canonical_bytes()),
            schema_digest: installed_schema.digest().bytes(),
        }
    }

    fn native_consumer_plan(schema_digest: [u8; 32]) -> ExecPlan {
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk_schema = chunk_schema_of(&arrow_schema, &[SlotId::new(1)]);
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let installed_schema = ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new_with_chunk_schema(
                                RecordBatch::new_empty(arrow_schema),
                                chunk_schema,
                            ),
                            node_id: 1,
                        }),
                    }),
                    owner_node_id: 2,
                    bindings: vec![RuntimeFilterConsumerBinding {
                        binding_id: 4,
                        channel_id: 1,
                        expr_id,
                        activation: crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
                        capabilities: BTreeSet::from([
                            crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                            crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                        ]),
                        contract: RuntimeFilterExecutionContract::Membership {
                            canonical_schema: Arc::from(installed_schema.canonical_bytes()),
                            schema_digest,
                        },
                        reduction: RuntimeFilterExecutionReduction::SetUnion,
                    }],
                }),
            },
        }
    }

    #[test]
    fn native_direct_consumer_builder_appends_runtime_filter_factory() {
        let (_manager, runtime_filter_context) = installed_native_consumer_context();
        let RuntimeFilterExecutionContract::Membership { schema_digest, .. } =
            installed_native_membership_contract_for_activation(
                crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
            )
        else {
            panic!("installed Join contract must be Membership")
        };
        let graph = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_consumer_plan(schema_digest),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(runtime_filter_context),
        )
        .expect("native direct consumer pipeline");
        let names = graph.pipelines[0]
            .factories
            .iter()
            .map(|factory| factory.name())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["ValuesSource (id=1)", "NativeRuntimeFilter (id=2)"]
        );
    }

    fn installed_native_producer_context() -> (
        Arc<crate::runtime::query_context::QueryContextManager>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        let (manager, _) = installed_native_consumer_context();
        let context = manager
            .runtime_filter_context_for_native_execution(
                crate::runtime::query_context::QueryId { hi: 70, lo: 9_201 },
                crate::common::types::UniqueId { hi: 70, lo: 30 },
            )
            .expect("producer runtime-filter context");
        (manager, context)
    }

    fn native_join_producer_plan(distribution_mode: JoinDistributionMode) -> ExecPlan {
        use crate::exec::node::join::JoinRuntimeFilterProducerBinding;
        use crate::runtime_filter::model::contract::{CompletionRequirement, ContributionKind};
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "probe",
            DataType::Int64,
            false,
        )]));
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "build",
            DataType::Int64,
            false,
        )]));
        let join_schema = Arc::new(Schema::new(vec![
            Field::new("probe", DataType::Int64, false),
            Field::new("build", DataType::Int64, false),
        ]));
        let left_chunk_schema = chunk_schema_of(&left_schema, &[SlotId::new(1)]);
        let right_chunk_schema = chunk_schema_of(&right_schema, &[SlotId::new(2)]);
        let join_chunk_schema = chunk_schema_of(&join_schema, &[SlotId::new(1), SlotId::new(2)]);
        let mut arena = ExprArena::default();
        let probe_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let build_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let membership_schema = ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let contract = RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::from(membership_schema.canonical_bytes()),
            schema_digest: membership_schema.digest().bytes(),
        };

        ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(lookup_node(1, Arc::clone(&left_chunk_schema))),
                    right: Box::new(lookup_node(2, Arc::clone(&right_chunk_schema))),
                    node_id: 3,
                    join_type: JoinType::Inner,
                    distribution_mode,
                    left_chunk_schema,
                    right_chunk_schema,
                    join_scope_chunk_schema: join_chunk_schema,
                    probe_keys: vec![probe_expr],
                    build_keys: vec![build_expr],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution: JoinRuntimeFilterExecution {
                        producers: vec![JoinRuntimeFilterProducerBinding {
                            binding_id: 3,
                            channel_id: 1,
                            build_expr_id: build_expr,
                            build_key_index: 0,
                            contribution_kinds: BTreeSet::from([
                                ContributionKind::ValueDomainDelta,
                                ContributionKind::ProducerClosed,
                            ]),
                            completion_requirement: CompletionRequirement::ProducerClosed,
                            contract,
                            reduction: RuntimeFilterExecutionReduction::SetUnion,
                        }],
                    },
                }),
            },
        }
    }

    fn native_aggregate_topn_spec(
        binding_id: u32,
        group_key_expr_id: crate::exec::expr::ExprId,
    ) -> crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding {
        native_aggregate_topn_spec_for_type(binding_id, group_key_expr_id, DataType::Int64)
    }

    fn native_aggregate_topn_spec_for_type(
        binding_id: u32,
        group_key_expr_id: crate::exec::expr::ExprId,
        data_type: DataType,
    ) -> crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding {
        use crate::runtime_filter::model::contract::{
            CompletionRequirement, ContributionKind, NullOrder, OrderContract, OrderKeyContract,
            SortDirection,
        };
        use crate::runtime_filter::port::ordered_bound::{
            COMPARATOR_ALGORITHM_VERSION, RuntimeOrderContract, comparator_digest_for_test,
        };

        let keys = vec![OrderKeyContract {
            data_type,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let runtime = RuntimeOrderContract::try_from_plan(&OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        })
        .expect("canonical aggregate TopN order");
        crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding {
            binding_id,
            channel_id: 1,
            group_key_expr_id,
            group_key_ordinal: 0,
            limit: std::num::NonZeroU32::new(5).expect("nonzero limit"),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: Arc::from(runtime.keys()),
                comparator_digest: runtime.plan_comparator_digest().get(),
                order_contract_digest: runtime.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
            contribution_kinds: BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
        }
    }

    fn native_aggregate_topn_plan(
        duplicate_spec: bool,
        need_finalize: bool,
        streaming_preaggregation_mode: Option<
            crate::exec::node::aggregate::StreamingPreaggregationMode,
        >,
    ) -> ExecPlan {
        native_aggregate_topn_plan_for_type(
            DataType::Int64,
            duplicate_spec,
            need_finalize,
            streaming_preaggregation_mode,
        )
    }

    fn native_aggregate_topn_plan_for_type(
        data_type: DataType,
        duplicate_spec: bool,
        need_finalize: bool,
        streaming_preaggregation_mode: Option<
            crate::exec::node::aggregate::StreamingPreaggregationMode,
        >,
    ) -> ExecPlan {
        let chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new(
                "group_key",
                data_type.clone(),
                false,
            )])),
            &[SlotId::new(1)],
        );
        let mut arena = ExprArena::default();
        let group_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), data_type.clone());
        let spec = native_aggregate_topn_spec_for_type(3, group_key, data_type);
        let mut topn_producers = vec![spec.clone()];
        if duplicate_spec {
            topn_producers.push(spec);
        }
        ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(lookup_node(1, Arc::clone(&chunk_schema))),
                    node_id: 2,
                    group_by: vec![group_key],
                    functions: Vec::new(),
                    need_finalize,
                    input_is_intermediate: false,
                    output_chunk_schema: chunk_schema,
                    runtime_filter_spec: AggregateRuntimeFilterSpec { topn_producers },
                    streaming_preaggregation_mode,
                }),
            },
        }
    }

    #[test]
    fn native_aggregate_topn_binding_rejects_duplicate_owner_spec_before_service_resolution() {
        let error = match build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(true, false, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            None,
        ) {
            Err(error) => error,
            Ok(_) => panic!("duplicate aggregate TopN target must fail before Service resolution"),
        };
        assert!(error.contains("duplicates group key ordinal=0"), "{error}");
    }

    #[test]
    fn native_aggregate_topn_binding_requires_one_ownership_safe_physical_site() {
        let partial = AggregateTopNProducerSiteCandidate {
            site: AggregateTopNProducerSite::PartialAggregateProcessor,
            owns_complete_group_identity: false,
        };
        let final_owner = AggregateTopNProducerSiteCandidate {
            site: AggregateTopNProducerSite::FinalAggregateProcessor,
            owns_complete_group_identity: true,
        };
        assert_eq!(
            resolve_aggregate_topn_producer_site(3, &[partial, final_owner])
                .expect("final aggregate is the unique group owner"),
            AggregateTopNProducerSite::FinalAggregateProcessor
        );

        let error = resolve_aggregate_topn_producer_site(3, &[partial])
            .expect_err("partial aggregate cannot prove complete group ownership");
        assert!(error.contains("no ownership-safe"), "{error}");

        let error = resolve_aggregate_topn_producer_site(
            3,
            &[
                final_owner,
                AggregateTopNProducerSiteCandidate {
                    site: AggregateTopNProducerSite::AggregateProcessor,
                    owns_complete_group_identity: true,
                },
            ],
        )
        .expect_err("two complete group owners are ambiguous");
        assert!(error.contains("multiple ownership-safe"), "{error}");
    }

    fn installed_native_aggregate_topn_context() -> (
        Arc<crate::runtime::query_context::QueryContextManager>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        installed_native_aggregate_topn_context_for_type(DataType::Int64)
    }

    fn installed_native_aggregate_topn_context_for_type(
        data_type: DataType,
    ) -> (
        Arc<crate::runtime::query_context::QueryContextManager>,
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    ) {
        use std::collections::BTreeMap;
        use std::time::Duration;

        use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
        use crate::runtime::query_context::{QueryContextManager, QueryId};
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
            ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, OrderContract,
            OrderKeyContract, ReductionRequirement, RuntimeFilterLifecycle,
            RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement, SortDirection,
        };
        use crate::runtime_filter::model::coverage::Coverage;
        use crate::runtime_filter::port::artifact::ConsumerArtifactProfile;
        use crate::runtime_filter::port::identity::{
            DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
        };
        use crate::runtime_filter::port::install::{
            ConsumerDeployment, MaterializationPolicy, ProducerDeployment,
            RuntimeFilterChannelDeployment, RuntimeFilterCoreBudget, RuntimeFilterInstallView,
            local_participant_install_for_test,
        };
        use crate::runtime_filter::port::ordered_bound::{
            COMPARATOR_ALGORITHM_VERSION, RuntimeOrderContract, comparator_digest_for_test,
        };

        let manager = QueryContextManager::new_for_test();
        let query_id = QueryId { hi: 70, lo: 9_301 };
        let producer_instance = crate::common::types::UniqueId { hi: 70, lo: 30 };
        let consumer_instance = crate::common::types::UniqueId { hi: 70, lo: 40 };
        let lifecycle = RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(11),
            query_expire: Duration::from_secs(29),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(5),
            transport_max_pending_entries: 128,
            transport_max_pending_bytes: 1024 * 1024,
        };
        manager
            .ensure_native_context(
                query_id,
                false,
                lifecycle.delivery_expire,
                lifecycle.query_expire,
            )
            .expect("native query context");

        let keys = vec![OrderKeyContract {
            data_type,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let order = OrderContract {
            comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
            keys,
            inclusive: true,
        };
        let runtime_order =
            RuntimeOrderContract::try_from_plan(&order).expect("canonical installed order");
        let witness = CoverageWitnessId::new(2);
        let channel = RuntimeFilterChannelDeployment::new(
            ChannelId::new(1),
            RuntimeFilterLogicalDomain::OrderedBound(order),
            RuntimeFilterLifecycle::MonotonicUpdates,
            Coverage::Leaf(witness),
            Coverage::Leaf(witness),
            ReductionRequirement::TightenOrderedBound,
            BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 1,
            },
            RuntimeFilterCoreBudget::new(8192),
            MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(3),
                ProducerDeployment::new(witness, BTreeSet::from([producer_instance])),
            )]),
            BTreeMap::from([(
                BindingId::new(4),
                ConsumerDeployment::with_profile(
                    ConsumerActivation::NonBlockingLive {
                        late_apply: LateApplyGranularity::Batch,
                    },
                    BTreeSet::from([ArtifactCapability::OrderedRange]),
                    ConsumerArtifactProfile::new_ordered_range(runtime_order.digest())
                        .expect("ordered range profile"),
                    BTreeSet::from([RouteEdgeId::new(5)]),
                    BTreeSet::from([consumer_instance]),
                ),
            )]),
        );
        manager
            .install_runtime_filter_deployment(
                query_id,
                lifecycle,
                local_participant_install_for_test(RuntimeFilterInstallView::new(
                    DeploymentEpoch::new(6),
                    RuntimeFilterParticipantId::new(7),
                    BTreeMap::from([(ChannelId::new(1), channel)]),
                )),
            )
            .expect("ordered runtime-filter deployment");
        let context = manager
            .runtime_filter_context_for_native_execution(query_id, producer_instance)
            .expect("aggregate producer runtime-filter context");
        (manager, context)
    }

    #[test]
    fn native_aggregate_topn_boolean_target_fails_at_factory_build_before_first_input() {
        let (_manager, context) =
            installed_native_aggregate_topn_context_for_type(DataType::Boolean);
        let error = match build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan_for_type(DataType::Boolean, false, false, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(context),
        ) {
            Err(error) => error,
            Ok(_) => panic!(
                "typed Boolean aggregate TopN target must fail at factory build before operator input"
            ),
        };
        assert!(error.contains("Boolean"), "{error}");
    }

    #[test]
    fn native_aggregate_topn_binding_resolves_ordered_bound_on_unique_final_owner() {
        let (_manager, context) = installed_native_aggregate_topn_context();
        let graph = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(false, true, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(context),
        )
        .expect("installed ordered binding and unique final owner must build");
        let aggregate_factories = graph
            .pipelines
            .iter()
            .flat_map(|pipeline| pipeline.factories.iter())
            .filter(|factory| factory.name().starts_with("AGGREGATE"))
            .count();
        assert_eq!(
            aggregate_factories, 2,
            "two-phase expansion must keep one partial helper and one final aggregate"
        );
    }

    #[test]
    fn native_aggregate_topn_binding_factory_ownership_is_ordinary_final_or_streaming_sink_only() {
        fn attachments(graph: &super::PipelineGraph) -> Vec<(i32, String, Vec<u32>)> {
            graph
                .pipelines
                .iter()
                .flat_map(|pipeline| {
                    pipeline.factories.iter().map(move |factory| {
                        (
                            pipeline.id,
                            factory.name().to_string(),
                            factory
                                .native_aggregate_topn_producers()
                                .iter()
                                .map(|spec| spec.binding_id)
                                .collect(),
                        )
                    })
                })
                .filter(|(_, name, _)| name.contains("AGG"))
                .collect()
        }

        let (_ordinary_manager, ordinary_context) = installed_native_aggregate_topn_context();
        let ordinary = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(false, false, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(ordinary_context),
        )
        .expect("ordinary aggregate owner");
        assert_eq!(
            attachments(&ordinary),
            vec![(ordinary.root_id, "AGGREGATE (id=2)".to_string(), vec![3])]
        );

        let (_final_manager, final_context) = installed_native_aggregate_topn_context();
        let two_phase = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(false, true, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(final_context),
        )
        .expect("two-phase final aggregate owner");
        let two_phase_attachments = attachments(&two_phase);
        assert_eq!(
            two_phase_attachments
                .iter()
                .filter(|(_, _, bindings)| !bindings.is_empty())
                .cloned()
                .collect::<Vec<_>>(),
            vec![(two_phase.root_id, "AGGREGATE (id=2)".to_string(), vec![3])],
            "only the final aggregate in the root pipeline may own the binding"
        );
        assert_eq!(
            two_phase_attachments
                .iter()
                .filter(|(pipeline_id, _, bindings)| {
                    *pipeline_id != two_phase.root_id && bindings.is_empty()
                })
                .count(),
            1,
            "the partial helper must carry no binding"
        );

        let (_streaming_manager, streaming_context) = installed_native_aggregate_topn_context();
        let streaming = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(
                false,
                false,
                Some(
                    crate::exec::node::aggregate::StreamingPreaggregationMode::ForcePreaggregation,
                ),
            ),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(streaming_context),
        )
        .expect("streaming sink aggregate owner");
        let streaming_attachments = attachments(&streaming);
        assert_eq!(
            streaming_attachments
                .iter()
                .filter(|(_, _, bindings)| !bindings.is_empty())
                .map(|(_, name, bindings)| (name.as_str(), bindings.as_slice()))
                .collect::<Vec<_>>(),
            vec![("AGGREGATE_STREAMING_SINK (id=2)", &[3][..])],
            "only the streaming sink may own the binding"
        );
        assert!(
            streaming_attachments
                .iter()
                .any(|(pipeline_id, name, bindings)| {
                    *pipeline_id == streaming.root_id
                        && name == "AGG_STREAMING_SOURCE (id=2)"
                        && bindings.is_empty()
                }),
            "the streaming source must carry no binding"
        );
    }

    #[test]
    fn native_aggregate_topn_binding_never_falls_back_to_membership_producer() {
        let (_manager, membership_context) = installed_native_producer_context();
        let error = match build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_aggregate_topn_plan(false, false, None),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
            Some(membership_context),
        ) {
            Err(error) => error,
            Ok(_) => panic!("aggregate TopN must resolve only an OrderedBound producer"),
        };
        assert!(
            error.contains("resolution failed") || error.contains("contract"),
            "{error}"
        );
    }

    fn assert_native_join_build_sinks_bind_exact_producer(
        distribution_mode: JoinDistributionMode,
        expected_build_dop: i32,
    ) {
        use std::time::Duration;

        use crate::runtime_filter::model::contract::BindingId;
        use crate::runtime_filter::port::subscription::{ArtifactAcquireOutcome, SubscriptionKind};

        let (_manager, context) = installed_native_producer_context();
        let service = Arc::clone(context.service());
        let graph = build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_join_producer_plan(distribution_mode),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            3,
            Some(context),
        )
        .expect("native join pipeline");
        let build_pipeline = graph
            .pipelines
            .iter()
            .find(|pipeline| {
                pipeline.factories.last().is_some_and(|factory| {
                    factory.is_sink() && factory.name().starts_with("HASH_JOIN")
                })
            })
            .expect("real hash-join build pipeline");
        assert_eq!(build_pipeline.dop, expected_build_dop);
        let factory = build_pipeline.factories.last().expect("build sink factory");
        let state = crate::runtime::runtime_state::RuntimeState::default();
        let subscription = service
            .subscribe(
                BindingId::new(4),
                crate::common::types::UniqueId { hi: 70, lo: 40 },
                SubscriptionKind::BlockingSnapshot,
            )
            .expect("installed local consumer subscription")
            .into_blocking()
            .expect("blocking subscription");
        let mut operators = Vec::new();
        for local_index in 0..build_pipeline.dop {
            let mut operator = factory.create(build_pipeline.dop, local_index);
            operator
                .bind_runtime_state(&state)
                .expect("bind exact native producer");
            operators.push(operator);
        }
        assert!(
            service.core_producer_handle_exists_for_test(
                BindingId::new(3),
                crate::common::types::UniqueId { hi: 70, lo: 30 },
            ),
            "every real native build sink must bind the installed producer"
        );
        let operator_count = operators.len();
        for (index, operator) in operators.iter_mut().enumerate() {
            operator
                .as_processor_mut()
                .expect("hash-join build processor")
                .set_finishing(&state)
                .expect("finish real native build sink");
            if index + 1 < operator_count {
                assert!(
                    subscription.snapshot().is_none(),
                    "native RF must not publish before every real build sink closes"
                );
            }
        }
        assert!(
            matches!(
                subscription.acquire(Duration::from_secs(1)),
                ArtifactAcquireOutcome::Published(_)
            ),
            "every real build sink must close its RF partition and publish the local artifact"
        );
        assert!(service.admitted_transport_envelopes_for_test().is_empty());
        drop(operators);
    }

    #[test]
    fn native_broadcast_pipeline_supplies_producer_specs_to_every_real_build_sink() {
        assert_native_join_build_sinks_bind_exact_producer(JoinDistributionMode::Broadcast, 1);
    }

    #[test]
    fn native_partitioned_pipeline_supplies_producer_specs_to_every_real_build_sink() {
        assert_native_join_build_sinks_bind_exact_producer(JoinDistributionMode::Partitioned, 3);
    }

    #[test]
    fn native_runtime_filter_pipeline_with_bindings_requires_installed_service() {
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let error = match build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_consumer_plan(schema.digest().bytes()),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            1,
            None,
        ) {
            Err(error) => error,
            Ok(_) => panic!("native binding requires installed Service context"),
        };
        assert!(error.contains("runtime-filter context"), "{error}");
    }

    #[test]
    fn native_runtime_filter_binding_contract_mismatch_fails_before_subscribe() {
        let (_manager, context) = installed_native_consumer_context();
        let error = match build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &native_consumer_plan([9; 32]),
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            1,
            Some(context),
        ) {
            Err(error) => error,
            Ok(_) => panic!("schema drift must fail while building the pipeline"),
        };
        assert!(error.contains("schema"), "{error}");
    }

    #[test]
    fn native_runtime_filter_pipeline_without_bindings_needs_no_runtime_filter_context() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::new_with_chunk_schema(
                        RecordBatch::new_empty(Arc::clone(&schema)),
                        chunk_schema_of(&schema, &[SlotId::new(1)]),
                    ),
                    node_id: 1,
                }),
            },
        };
        build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            1,
            None,
        )
        .expect("binding-free native pipeline");
    }

    #[test]
    fn native_runtime_filter_topk_contract_digest_mismatch_fails_before_open() {
        let error = super::validate_native_reduction(
            7,
            &RuntimeFilterExecutionReduction::MergeTopKSummary {
                k: std::num::NonZeroU32::new(3).expect("non-zero k"),
                contract_digest: [1; 32],
            },
            crate::runtime_filter::model::contract::ReductionRequirement::MergeTopKSummary(
                crate::runtime_filter::model::contract::TopKSummaryRequirement::try_new(3)
                    .expect("non-zero k"),
            ),
            Some([2; 32]),
        )
        .expect_err("TopK contract digest drift must fail");
        assert!(error.contains("reduction"), "{error}");
    }

    #[test]
    fn dormant_blocking_consumer_installs_fail_open_processor_without_dependency_or_wait() {
        assert_dormant_consumer_installs_fail_open_processor_without_dependency_or_wait(
            crate::runtime_filter::model::contract::ConsumerActivation::BlockingSnapshot,
        );
    }

    #[test]
    fn dormant_live_consumer_installs_fail_open_processor_without_snapshot_poll() {
        assert_dormant_consumer_installs_fail_open_processor_without_dependency_or_wait(
            crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive {
                late_apply: crate::runtime_filter::model::contract::LateApplyGranularity::Batch,
            },
        );
    }

    #[test]
    fn dormant_consumer_pipeline_is_chunk_exact_passthrough() {
        let activation =
            crate::runtime_filter::model::contract::ConsumerActivation::NonBlockingLive {
                late_apply: crate::runtime_filter::model::contract::LateApplyGranularity::Batch,
            };
        let (_manager, runtime_filter_context) =
            installed_native_consumer_context_for_activation(activation);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("batch");
        let chunk_schema = chunk_schema_of(&schema, &[SlotId::new(1)]);
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let baseline = ExecPlan {
            arena: arena.clone(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: chunk.clone(),
                    node_id: 1,
                }),
            },
        };
        let wrapped = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: chunk.clone(),
                            node_id: 1,
                        }),
                    }),
                    owner_node_id: 2,
                    bindings: vec![RuntimeFilterConsumerBinding {
                        binding_id: 4,
                        channel_id: 1,
                        expr_id,
                        activation,
                        capabilities: BTreeSet::from([
                            crate::runtime_filter::model::contract::ArtifactCapability::Membership,
                            crate::runtime_filter::model::contract::ArtifactCapability::EmptyDomain,
                        ]),
                        contract: installed_native_membership_contract_for_activation(activation),
                        reduction: RuntimeFilterExecutionReduction::SetUnion,
                    }],
                }),
            },
        };
        let ExecNodeKind::RuntimeFilterConsumer(consumer) = &wrapped.root.kind else {
            panic!("consumer")
        };
        let ExecNodeKind::Values(values) = &consumer.input.kind else {
            panic!("values")
        };
        assert!(Arc::ptr_eq(
            values.chunk.batch.column(0),
            chunk.batch.column(0)
        ));

        let build = |plan: &ExecPlan| {
            build_native_pipeline_graph_for_exec_plan_with_runtime_filter_context(
                plan,
                false,
                DependencyManager::new(),
                None,
                ExchangeBindings::default(),
                ScanBindings::default(),
                2,
                Some(runtime_filter_context.clone()),
            )
            .expect("graph")
        };
        let baseline = build(&baseline);
        let wrapped = build(&wrapped);
        assert_eq!(wrapped.pipelines.len(), baseline.pipelines.len());
        assert_eq!(
            wrapped.pipelines[0].factories.len(),
            baseline.pipelines[0].factories.len() + 1
        );
        assert_eq!(
            wrapped.pipelines[0]
                .factories
                .last()
                .expect("fail-open factory")
                .name(),
            "NativeRuntimeFilter (id=2)"
        );
    }

    #[test]
    fn ensure_hash_dedups_redundant_shuffle_for_nested_group_by() {
        // The input must be a dop>1 source so the inner aggregate inserts a local hash shuffle;
        // the outer aggregate must then recognize the input is already partitioned by the same
        // group keys and skip its own shuffle (dedup). A LookUp source maps to a dop>1 pipeline
        // (StreamDesc::any(ctx.pipeline_dop)); a Values source is dop=1 and would suppress both
        // shuffles, making this assertion vacuous.
        let lookup_input_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int32, false),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );
        let agg_output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("sum", DataType::Int64, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let inner = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::LookUp(LookUpNode {
                        node_id: 0,
                        row_pos_descs: HashMap::new(),
                        output_chunk_schema: Arc::clone(&lookup_input_chunk_schema),
                    }),
                }),
                node_id: 0,
                group_by: vec![k],
                functions: vec![AggFunction {
                    name: "sum".to_string(),
                    inputs: vec![v],
                    input_is_intermediate: false,
                    types: Some(AggTypeSignature {
                        intermediate_type: None,
                        output_type: Some(DataType::Int64),
                        input_arg_type: None,
                    }),
                    ..Default::default()
                }],
                need_finalize: true,
                input_is_intermediate: false,
                output_chunk_schema: Arc::clone(&agg_output_chunk_schema),
                runtime_filter_spec: AggregateRuntimeFilterSpec {
                    topn_producers: Vec::new(),
                },
                streaming_preaggregation_mode: None,
            }),
        };

        let root = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(inner),
                node_id: 1,
                group_by: vec![k],
                functions: vec![AggFunction {
                    name: "sum".to_string(),
                    inputs: vec![v],
                    input_is_intermediate: false,
                    types: Some(AggTypeSignature {
                        intermediate_type: None,
                        output_type: Some(DataType::Int64),
                        input_arg_type: None,
                    }),
                    ..Default::default()
                }],
                need_finalize: true,
                input_is_intermediate: false,
                output_chunk_schema: Arc::clone(&agg_output_chunk_schema),
                runtime_filter_spec: AggregateRuntimeFilterSpec {
                    topn_producers: Vec::new(),
                },
                streaming_preaggregation_mode: None,
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_native_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
        )
        .expect("build pipeline graph");

        assert_eq!(graph.pipelines.len(), 2);
        let local_exchange_sources = graph
            .pipelines
            .iter()
            .flat_map(|p| p.factories.iter())
            .filter(|f| f.name().starts_with("LOCAL_EXCHANGE_SOURCE"))
            .count();
        assert_eq!(local_exchange_sources, 1);
    }

    #[test]
    fn assert_num_rows_requires_local_hash_shuffle_when_dop_gt_one() {
        let row_id_slot = SlotId::new(7);
        let lookup_output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new(
                "_row_id",
                DataType::Int32,
                false,
            )])),
            &[row_id_slot],
        );

        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::LookUp(LookUpNode {
                            node_id: 0,
                            row_pos_descs: HashMap::new(),
                            output_chunk_schema: Arc::clone(&lookup_output_chunk_schema),
                        }),
                    }),
                    node_id: 11,
                    mode: AssertNumRowsMode::PerKeyAtMostOne {
                        key_slots: vec![row_id_slot],
                        key_labels: vec!["_row_id".to_string()],
                        message_prefix: "assert_num_rows failed".to_string(),
                    },
                }),
            },
        };

        let graph = build_native_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
        )
        .expect("build pipeline graph");

        let root = graph
            .pipelines
            .iter()
            .find(|pipeline| pipeline.id == graph.root_id)
            .expect("root pipeline");
        assert_eq!(root.dop, 2);
        assert!(
            root.factories
                .iter()
                .any(|factory| factory.name().starts_with("LOCAL_EXCHANGE_SOURCE")),
            "keyed assert root pipeline should read from local hash exchange"
        );

        let local_exchange_sources = graph
            .pipelines
            .iter()
            .flat_map(|p| p.factories.iter())
            .filter(|f| f.name().starts_with("LOCAL_EXCHANGE_SOURCE"))
            .count();
        assert_eq!(local_exchange_sources, 1);
    }

    #[test]
    fn global_assert_num_rows_does_not_require_local_hash_shuffle_when_dop_gt_one() {
        let lookup_output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, false)])),
            &[SlotId::new(1)],
        );

        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::LookUp(LookUpNode {
                            node_id: 0,
                            row_pos_descs: HashMap::new(),
                            output_chunk_schema: Arc::clone(&lookup_output_chunk_schema),
                        }),
                    }),
                    node_id: 11,
                    mode: AssertNumRowsMode::Global {
                        desired_num_rows: Some(1),
                        assertion: Assertion::Eq,
                        subquery_string: None,
                    },
                }),
            },
        };

        let graph = build_native_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
        )
        .expect("build pipeline graph");

        let local_exchange_sources = graph
            .pipelines
            .iter()
            .flat_map(|p| p.factories.iter())
            .filter(|f| f.name().starts_with("LOCAL_EXCHANGE_SOURCE"))
            .count();
        assert_eq!(local_exchange_sources, 0);
    }

    #[test]
    fn root_sink_dop_override_gathers_multi_driver_root_to_one() {
        let lookup_input_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)])),
            &[SlotId::new(1)],
        );
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::LookUp(LookUpNode {
                    node_id: 17,
                    row_pos_descs: HashMap::new(),
                    output_chunk_schema: Arc::clone(&lookup_input_chunk_schema),
                }),
            },
        };

        let graph = build_native_pipeline_graph_for_exec_plan_with_root_sink_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            4,
            Some(1),
        )
        .expect("build pipeline graph");

        let root = graph
            .pipelines
            .iter()
            .find(|pipeline| pipeline.id == graph.root_id)
            .expect("root pipeline");
        assert_eq!(
            root.dop, 1,
            "root sink DOP override should attach the terminal sink to one local driver"
        );
        assert!(root.needs_sink, "new root still needs the terminal sink");
        let mut has_local_exchange_sink = false;
        for pipeline in &graph.pipelines {
            for factory in &pipeline.factories {
                if factory.name().starts_with("LOCAL_EXCHANGE_SINK") {
                    has_local_exchange_sink = true;
                }
            }
        }
        assert!(
            has_local_exchange_sink,
            "multi-driver input should be gathered through a local exchange before the sink"
        );
    }

    #[test]
    fn merge_group_by_requires_local_shuffle_when_dop_gt_one() {
        // Regression for TPC-DS Q28: merge-serialize group-by aggregates must not emit duplicate
        // groups across drivers, otherwise downstream `count(key)` will over-count DISTINCT keys.
        //
        // The input must be a dop>1 source for the aggregate's local-shuffle insertion to fire
        // (it is gated on `build.pipeline.dop > 1`). A LookUp source maps to a dop>1 pipeline
        // (StreamDesc::any(ctx.pipeline_dop)); a Values source is dop=1 and would suppress the
        // shuffle, making this assertion vacuous.
        let lookup_input_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Int32, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );
        let agg_output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("sum", DataType::Int64, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let root = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::LookUp(LookUpNode {
                        node_id: 0,
                        row_pos_descs: HashMap::new(),
                        output_chunk_schema: Arc::clone(&lookup_input_chunk_schema),
                    }),
                }),
                node_id: 0,
                group_by: vec![k],
                functions: vec![AggFunction {
                    name: "sum".to_string(),
                    inputs: vec![v],
                    input_is_intermediate: true,
                    types: Some(AggTypeSignature {
                        intermediate_type: Some(DataType::Int64),
                        output_type: Some(DataType::Int64),
                        input_arg_type: Some(DataType::Int32),
                    }),
                    ..Default::default()
                }],
                // "merge serialize" style node: outputs intermediate, but still groups by keys.
                need_finalize: false,
                input_is_intermediate: true,
                output_chunk_schema: agg_output_chunk_schema,
                runtime_filter_spec: AggregateRuntimeFilterSpec {
                    topn_producers: Vec::new(),
                },
                streaming_preaggregation_mode: None,
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_native_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
        )
        .expect("build pipeline graph");

        let local_exchange_sources = graph
            .pipelines
            .iter()
            .flat_map(|p| p.factories.iter())
            .filter(|f| f.name().starts_with("LOCAL_EXCHANGE_SOURCE"))
            .count();
        assert_eq!(local_exchange_sources, 1);
    }

    #[test]
    fn update_group_by_intermediate_requires_local_shuffle_when_dop_gt_one() {
        // Regression for TPC-DS Q7/Q26:
        // update-style group-by aggregates that output intermediate states (need_finalize=false)
        // must not emit duplicate group keys across drivers, otherwise upstream Sort+LIMIT can
        // return fewer than N distinct groups after downstream merge/finalize aggregation.
        //
        // The input must be a dop>1 source for the aggregate's local-shuffle insertion to fire
        // (it is gated on `build.pipeline.dop > 1`). A LookUp source maps to a dop>1 pipeline
        // (StreamDesc::any(ctx.pipeline_dop)); a Values source is dop=1 and would suppress the
        // shuffle, making this assertion vacuous.
        let lookup_input_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Int32, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );
        let agg_output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("sum", DataType::Int64, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let root = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::LookUp(LookUpNode {
                        node_id: 0,
                        row_pos_descs: HashMap::new(),
                        output_chunk_schema: Arc::clone(&lookup_input_chunk_schema),
                    }),
                }),
                node_id: 0,
                group_by: vec![k],
                functions: vec![AggFunction {
                    name: "sum".to_string(),
                    inputs: vec![v],
                    input_is_intermediate: false,
                    types: Some(AggTypeSignature {
                        intermediate_type: Some(DataType::Int64),
                        output_type: Some(DataType::Int64),
                        input_arg_type: Some(DataType::Int32),
                    }),
                    ..Default::default()
                }],
                // Outputs intermediate states to be merged/finalized downstream.
                need_finalize: false,
                input_is_intermediate: false,
                output_chunk_schema: agg_output_chunk_schema,
                runtime_filter_spec: AggregateRuntimeFilterSpec {
                    topn_producers: Vec::new(),
                },
                streaming_preaggregation_mode: None,
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_native_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            2,
        )
        .expect("build pipeline graph");

        let local_exchange_sources = graph
            .pipelines
            .iter()
            .flat_map(|p| p.factories.iter())
            .filter(|f| f.name().starts_with("LOCAL_EXCHANGE_SOURCE"))
            .count();
        assert_eq!(local_exchange_sources, 1);
    }
}
