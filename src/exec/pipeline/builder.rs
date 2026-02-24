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
//! - Functions: `build_pipeline_graph_for_exec_plan`, `build_pipeline_graph_for_exec_plan_with_dop`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use crate::exec::expr::ExprArena;
use crate::exec::node::aggregate::AggregateNode;
use crate::exec::node::analytic::AnalyticNode;
use crate::exec::node::assert::AssertNumRowsNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinType};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::sort::SortNode;
use crate::exec::node::table_function::TableFunctionNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
use crate::exec::operators::hashjoin::partitioned_join_shared::PartitionedJoinSharedState;
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::distribution::{Distribution, StreamDesc};
use crate::exec::runtime_filter::{MAX_RUNTIME_IN_FILTER_CONDITIONS, PartialRuntimeInFilterMerger};
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;

use super::operator_factory::OperatorFactory;
use crate::exec::operators::AssertNumRowsProcessorFactory;
use crate::exec::operators::analytic_shared::AnalyticSharedState;
use crate::exec::operators::local_exchanger::{LocalExchangePartitionSpec, LocalExchanger};
use crate::exec::operators::{
    AggregateProcessorFactory, AnalyticSinkFactory, AnalyticSourceFactory,
    BroadcastJoinProbeProcessorFactory, ExceptSinkFactory, ExceptSourceFactory,
    ExchangeSourceFactory, FetchProcessorFactory, FilterProcessorFactory, HashJoinBuildSinkFactory,
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
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    next_pipeline_id: i32,
    pipeline_dop: i32,
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
pub(crate) fn build_pipeline_graph_for_exec_plan(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    exchange_finst_id: Option<(i64, i64)>,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
) -> Result<PipelineGraph, String> {
    let default_dop = crate::runtime::exec_env::calc_pipeline_dop(0);
    build_pipeline_graph_for_exec_plan_with_dop(
        plan,
        _debug,
        dep_manager,
        exchange_finst_id,
        default_dop,
        runtime_filter_hub,
    )
}

/// Build a pipeline graph from an execution plan with an explicit degree of parallelism.
pub(crate) fn build_pipeline_graph_for_exec_plan_with_dop(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    _exchange_finst_id: Option<(i64, i64)>,
    pipeline_dop: i32,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
) -> Result<PipelineGraph, String> {
    let arena = Arc::new(plan.arena.clone());
    let mut ctx = PipelineBuildContext {
        arena,
        dep_manager,
        runtime_filter_hub,
        next_pipeline_id: 0,
        pipeline_dop: pipeline_dop.max(1),
    };
    let mut build = build_pipeline_for_node(&plan.root, &mut ctx)?;
    build.pipeline.needs_sink = true;

    let root_id = build.pipeline.id;
    let mut pipelines = Vec::new();
    pipelines.push(build.pipeline);
    pipelines.append(&mut build.extra_pipelines);
    Ok(PipelineGraph { pipelines, root_id })
}

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
    distribution_keys: Vec<crate::exec::expr::ExprId>,
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

fn build_distinct_set_op_pipeline<S, MakeShared, MakeSink, MakeSource>(
    inputs: &[ExecNode],
    node_id: i32,
    output_slots: &[crate::common::ids::SlotId],
    node_name: &'static str,
    controller_name: &'static str,
    ctx: &mut PipelineBuildContext,
    make_shared: MakeShared,
    make_sink: MakeSink,
    make_source: MakeSource,
) -> Result<PipelineBuildResult, String>
where
    S: Clone + 'static,
    MakeShared: FnOnce(SetOpStageController, Vec<crate::common::ids::SlotId>) -> S,
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
    let shared = make_shared(controller, output_slots.to_vec());

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

fn build_pipeline_for_node(
    node: &ExecNode,
    ctx: &mut PipelineBuildContext,
) -> Result<PipelineBuildResult, String> {
    match &node.kind {
        ExecNodeKind::AssertNumRows(AssertNumRowsNode {
            input,
            node_id,
            desired_num_rows,
            assertion,
            subquery_string,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(AssertNumRowsProcessorFactory::new(
                    *node_id,
                    *desired_num_rows,
                    assertion.clone(),
                    subquery_string.clone(),
                )));
            Ok(build)
        }
        ExecNodeKind::Project(ProjectNode {
            input,
            node_id,
            is_subordinate,
            exprs,
            expr_slot_ids,
            output_indices,
            output_slots,
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
                    output_indices.clone(),
                    output_slots.clone(),
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
            output_schema,
            output_slots,
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
                    Arc::clone(output_schema),
                    output_slots.clone(),
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
            output_slots,
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
                output_slots.clone(),
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
            output_slots,
        }) => {
            let mut build = build_pipeline_for_node(input, ctx)?;

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
                // StarRocks-aligned two-phase hash aggregation:
                // - Partial aggregation per upstream driver
                // - Hash shuffle by group keys
                // - Final aggregation merges intermediate states
                let mut partial_functions = functions.clone();
                for func in &mut partial_functions {
                    func.input_is_intermediate = false;
                }
                build
                    .pipeline
                    .factories
                    .push(Box::new(AggregateProcessorFactory::new(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        partial_functions,
                        true,
                        false,
                        output_slots.clone(),
                    )));

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
                build
                    .pipeline
                    .factories
                    .push(Box::new(AggregateProcessorFactory::new(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        merge_functions,
                        false,
                        true,
                        output_slots.clone(),
                    )));
                return Ok(build);
            }

            if *need_finalize && group_by.is_empty() && dop > 1 && all_update {
                let mut partial_functions = functions.clone();
                for func in &mut partial_functions {
                    func.input_is_intermediate = false;
                }
                let local_factory = Box::new(AggregateProcessorFactory::new(
                    *node_id,
                    Arc::clone(&ctx.arena),
                    group_by.clone(),
                    partial_functions,
                    true,
                    false,
                    output_slots.clone(),
                ));
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
                    .push(Box::new(AggregateProcessorFactory::new(
                        *node_id,
                        Arc::clone(&ctx.arena),
                        group_by.clone(),
                        merge_functions,
                        false,
                        true,
                        output_slots.clone(),
                    )));

                let mut extra_pipelines = build.extra_pipelines;
                extra_pipelines.push(build.pipeline);

                return Ok(PipelineBuildResult {
                    pipeline: downstream,
                    extra_pipelines,
                    stream: StreamDesc::any(downstream_dop),
                });
            }

            let agg_factory = Box::new(AggregateProcessorFactory::new(
                *node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                functions.clone(),
                !*need_finalize,
                false,
                output_slots.clone(),
            ));

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
            left_schema,
            right_schema,
            join_scope_schema,
            probe_keys,
            build_keys,
            eq_null_safe,
            residual_predicate,
            runtime_filters,
        }) => {
            ctx.runtime_filter_hub
                .register_filter_specs(*node_id, runtime_filters);
            let left_build = build_pipeline_for_node(left, ctx)?;
            let right_build = build_pipeline_for_node(right, ctx)?;

            // StarRocks uses left child as probe and right child as build across join types.
            // Keep the execution order aligned with the FE plan to avoid swapping runtime filters.
            let probe_is_left = true;
            let mut probe_build = left_build;
            let mut build_build = right_build;
            let probe_keys = probe_keys.clone();
            let build_keys = build_keys.clone();
            let eq_null_safe = eq_null_safe.clone();

            if *distribution_mode == JoinDistributionMode::Broadcast {
                if *join_type == JoinType::FullOuter {
                    probe_build = gather_to_one(probe_build, ctx, *node_id);
                }
                build_build = gather_to_one(build_build, ctx, *node_id);
                let runtime_in_filter_merger = if runtime_filters.is_empty() {
                    None
                } else {
                    Some(Arc::new(PartialRuntimeInFilterMerger::new(
                        1,
                        MAX_RUNTIME_IN_FILTER_CONDITIONS,
                    )))
                };

                let join_state = Arc::new(BroadcastJoinSharedState::new(
                    *node_id,
                    ctx.dep_manager.clone(),
                ));

                probe_build.pipeline.factories.push(Box::new(
                    BroadcastJoinProbeProcessorFactory::new(
                        Arc::clone(&ctx.arena),
                        *join_type,
                        probe_keys.clone(),
                        *residual_predicate,
                        probe_is_left,
                        Arc::clone(left_schema),
                        Arc::clone(right_schema),
                        Arc::clone(join_scope_schema),
                        Arc::clone(&join_state),
                    ),
                ));

                let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
                build_build
                    .pipeline
                    .factories
                    .push(Box::new(HashJoinBuildSinkFactory::new(
                        Arc::clone(&ctx.arena),
                        *join_type,
                        build_keys.clone(),
                        eq_null_safe.clone(),
                        runtime_filters.clone(),
                        *distribution_mode,
                        build_state,
                        Arc::clone(&ctx.runtime_filter_hub),
                        runtime_in_filter_merger,
                    )));
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
            let runtime_in_filter_merger = if runtime_filters.is_empty() {
                None
            } else {
                Some(Arc::new(PartialRuntimeInFilterMerger::new(
                    join_partitions,
                    MAX_RUNTIME_IN_FILTER_CONDITIONS,
                )))
            };

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

            probe_build.pipeline.factories.push(Box::new(
                PartitionedJoinProbeProcessorFactory::new(
                    Arc::clone(&ctx.arena),
                    *join_type,
                    probe_keys.clone(),
                    *residual_predicate,
                    probe_is_left,
                    Arc::clone(left_schema),
                    Arc::clone(right_schema),
                    Arc::clone(join_scope_schema),
                    Arc::clone(&join_state),
                ),
            ));

            let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
            build_build
                .pipeline
                .factories
                .push(Box::new(HashJoinBuildSinkFactory::new(
                    Arc::clone(&ctx.arena),
                    *join_type,
                    build_keys.clone(),
                    eq_null_safe.clone(),
                    runtime_filters.clone(),
                    *distribution_mode,
                    build_state,
                    Arc::clone(&ctx.runtime_filter_hub),
                    runtime_in_filter_merger,
                )));
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
            left_schema,
            right_schema,
            join_scope_schema,
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
                    Arc::clone(left_schema),
                    Arc::clone(right_schema),
                    Arc::clone(join_scope_schema),
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
            output_slots,
        }) => match kind {
            SetOpKind::Intersect => build_distinct_set_op_pipeline(
                inputs,
                *node_id,
                output_slots,
                "INTERSECT_NODE",
                "intersect",
                ctx,
                |controller, slots| IntersectSharedState::new(controller, slots),
                |stage, shared, id| Box::new(IntersectSinkFactory::new(stage, shared, id)),
                |shared, id| Box::new(IntersectSourceFactory::new(shared, id)),
            ),
            SetOpKind::Except => build_distinct_set_op_pipeline(
                inputs,
                *node_id,
                output_slots,
                "EXCEPT_NODE",
                "except",
                ctx,
                |controller, slots| ExceptSharedState::new(controller, slots),
                |stage, shared, id| Box::new(ExceptSinkFactory::new(stage, shared, id)),
                |shared, id| Box::new(ExceptSourceFactory::new(shared, id)),
            ),
        },
        ExecNodeKind::Values(ValuesNode { chunk, node_id }) => {
            let source: Box<dyn OperatorFactory> =
                Box::new(ValuesSourceFactory::new(chunk.clone(), *node_id));
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        ExecNodeKind::ExchangeSource(node) => {
            let source: Box<dyn OperatorFactory> = Box::new(ExchangeSourceFactory::new(
                node.clone(),
                Arc::clone(&ctx.runtime_filter_hub),
                Arc::clone(&ctx.arena),
            ));
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
            let source: Box<dyn OperatorFactory> = Box::new(ScanSourceFactory::new(
                scan.clone(),
                Arc::clone(&ctx.runtime_filter_hub),
                Arc::clone(&ctx.arena),
            ));
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
                    fetch.nodes_info.clone(),
                    fetch.output_slots.clone(),
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
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;

    use super::build_pipeline_graph_for_exec_plan_with_dop;

    #[test]
    fn ensure_hash_dedups_redundant_shuffle_for_nested_group_by() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let inner = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
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
                }],
                need_finalize: true,
                input_is_intermediate: false,
                output_slots: vec![SlotId::new(1), SlotId::new(2)],
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
                }],
                need_finalize: true,
                input_is_intermediate: false,
                output_slots: vec![SlotId::new(1), SlotId::new(2)],
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            2,
            Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
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
    fn merge_group_by_requires_local_shuffle_when_dop_gt_one() {
        // Regression for TPC-DS Q28: merge-serialize group-by aggregates must not emit duplicate
        // groups across drivers, otherwise downstream `count(key)` will over-count DISTINCT keys.
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, true), SlotId::new(2)),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let root = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
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
                }],
                // "merge serialize" style node: outputs intermediate, but still groups by keys.
                need_finalize: false,
                input_is_intermediate: true,
                output_slots: vec![SlotId::new(1), SlotId::new(2)],
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            2,
            Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
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
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, true), SlotId::new(2)),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let root = ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
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
                }],
                // Outputs intermediate states to be merged/finalized downstream.
                need_finalize: false,
                input_is_intermediate: false,
                output_slots: vec![SlotId::new(1), SlotId::new(2)],
            }),
        };

        let plan = ExecPlan { arena, root };
        let graph = build_pipeline_graph_for_exec_plan_with_dop(
            &plan,
            false,
            DependencyManager::new(),
            None,
            2,
            Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
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
