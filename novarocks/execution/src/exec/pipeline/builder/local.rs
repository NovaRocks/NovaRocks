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

//! Direct pipeline construction from the single frozen local program.

use std::collections::BTreeSet;
use std::sync::Arc;

use novarocks_local_program as lp;

use super::*;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::expr::static_program::thaw_field_schema;
use crate::exec::node::LocalRuntimeBindings;
use crate::exec::node::aggregate::{
    AggFunction, AggOrderSpec, AggTypeSignature, AggregateTopNRuntimeFilterProducerBinding,
};
use crate::exec::node::analytic::{
    AnalyticOutputColumn as RuntimeAnalyticOutputColumn, WindowAggregateBinding,
    WindowBoundary as RuntimeWindowBoundary, WindowFrame as RuntimeWindowFrame,
    WindowFunctionKind as RuntimeWindowFunctionKind, WindowFunctionSpec,
    WindowType as RuntimeWindowType,
};
use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
use crate::exec::node::change_event_expand::{
    ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::join::{
    JoinDistributionMode as RuntimeJoinDistributionMode, JoinRuntimeFilterProducerBinding,
    JoinType as RuntimeJoinType,
};
use crate::exec::node::nljoin::NestedLoopJoinType as RuntimeNestedLoopJoinType;
use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use crate::exec::node::scan::ScanNode;
use crate::exec::node::sort::{
    SortExpression as RuntimeSortExpression, SortTopNType as RuntimeSortTopNType,
};
use crate::exec::node::table_function::TableFunctionOutputSlot as RuntimeTableFunctionOutputSlot;
use crate::exec::node::table_write_aggregate::{
    WriterFinalAggregateCall as RuntimeWriterFinalAggregateCall,
    WriterFinalAggregatePlan as RuntimeWriterFinalAggregatePlan,
    WriterGroupedUnpivotMapping as RuntimeWriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan as RuntimeWriterGroupedUnpivotPlan,
    WriterPartialAggregateCall as RuntimeWriterPartialAggregateCall,
};
use crate::exec::node::table_write_relation::{
    RootWriteResultRelationSchema, WriterMultiplexRelationSchema,
};
use crate::exec::node::table_writer::TableWriterInputProjection;
use crate::exec::node::unpivot::{
    UnpivotConstant as RuntimeUnpivotConstant, UnpivotPassthroughColumn, UnpivotValueMapping,
};

#[expect(
    clippy::too_many_arguments,
    reason = "Native runtime dependencies are explicit"
)]
pub(crate) fn build_native_pipeline_graph_for_local_program_with_runtime_settings(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
    _debug: bool,
    dep_manager: DependencyManager,
    _exchange_finst_id: Option<(i64, i64)>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    pipeline_dop: i32,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<execution::RuntimeFilterSessionRef>,
    function_set: Arc<SealedExecutionFunctionSet>,
    operator_buffer_chunks: usize,
    local_exchange_buffer_mem_limit_per_driver: usize,
    local_exchange_max_buffered_rows: i64,
) -> Result<PipelineGraph, String> {
    validate_runtime_binding_shape(program, bindings)?;
    let mut ctx = PipelineBuildContext {
        arena: Arc::new(ExprArena::from_immutable(program.expressions())),
        function_set,
        dep_manager,
        runtime_filter_execution: PipelineRuntimeFilterExecution {
            session: runtime_filter_session,
        },
        exchange_bindings,
        scan_bindings,
        next_pipeline_id: 0,
        pipeline_dop: pipeline_dop.max(1),
        operator_buffer_chunks: operator_buffer_chunks.max(1),
        local_exchange_buffer_mem_limit_per_driver: local_exchange_buffer_mem_limit_per_driver
            .max(1),
        local_exchange_max_buffered_rows,
        precomputed_keyed_assert_keys: std::collections::HashMap::new(),
    };
    // Synthetic slot expressions are installed before any child factory takes
    // an arena Arc. This avoids cloning a full expression graph at each keyed
    // assertion encountered during recursive pipeline construction.
    for node in program.nodes() {
        if let lp::ProgramNodeKind::AssertNumRows {
            input,
            mode: lp::AssertRowsMode::PerKeyAtMostOne { key_slots, .. },
        } = node.kind()
        {
            let layout = program.nodes()[input.index()].output_layout();
            let keys = keyed_assert_distribution_keys_from_layout(&mut ctx, layout, key_slots)?;
            ctx.precomputed_keyed_assert_keys
                .insert(node.native_node_id(), keys);
        }
    }
    let mut build = build_pipeline_for_program_node(program, bindings, program.root(), &mut ctx)?;
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
    let mut pipelines = vec![build.pipeline];
    pipelines.append(&mut build.extra_pipelines);
    Ok(PipelineGraph { pipelines, root_id })
}

fn validate_runtime_binding_shape(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
) -> Result<(), String> {
    let mut expected_scans = BTreeSet::new();
    let mut expected_writers = BTreeSet::new();
    let mut expected_finishers = BTreeSet::new();
    for (index, node) in program.nodes().iter().enumerate() {
        let id = lp::ProgramNodeId::new(index);
        match node.kind() {
            lp::ProgramNodeKind::Scan { .. } => {
                expected_scans.insert(id);
            }
            lp::ProgramNodeKind::TableWriter { .. } => {
                expected_writers.insert(id);
            }
            lp::ProgramNodeKind::TableFinish { .. } => {
                expected_finishers.insert(id);
            }
            _ => {}
        }
    }
    if expected_scans != bindings.scans.keys().copied().collect()
        || expected_writers != bindings.writers.keys().copied().collect()
        || expected_finishers != bindings.finishers.keys().copied().collect()
    {
        return Err("local program runtime capability set does not match frozen nodes".to_string());
    }

    let mut stack = vec![program.root()];
    let mut seen_effectful = BTreeSet::new();
    while let Some(id) = stack.pop() {
        let node = &program.nodes()[id.index()];
        let effectful = matches!(
            node.kind(),
            lp::ProgramNodeKind::Scan { .. }
                | lp::ProgramNodeKind::ExchangeSource { .. }
                | lp::ProgramNodeKind::TableWriter { .. }
                | lp::ProgramNodeKind::TableFinish { .. }
        );
        if effectful && !seen_effectful.insert(id) {
            return Err(format!(
                "local program reuses runtime-capability node {} in its expanded execution graph",
                node.native_node_id()
            ));
        }
        match node.kind() {
            lp::ProgramNodeKind::Values { .. }
            | lp::ProgramNodeKind::Scan { .. }
            | lp::ProgramNodeKind::ExchangeSource { .. } => {}
            lp::ProgramNodeKind::AssertNumRows { input, .. }
            | lp::ProgramNodeKind::Project { input, .. }
            | lp::ProgramNodeKind::Unpivot { input, .. }
            | lp::ProgramNodeKind::Filter { input, .. }
            | lp::ProgramNodeKind::Repeat { input, .. }
            | lp::ProgramNodeKind::ChangeEventExpand { input, .. }
            | lp::ProgramNodeKind::Limit { input, .. }
            | lp::ProgramNodeKind::Aggregate { input, .. }
            | lp::ProgramNodeKind::Analytic { input, .. }
            | lp::ProgramNodeKind::RuntimeFilterConsumer { input, .. }
            | lp::ProgramNodeKind::TableWriter { input, .. }
            | lp::ProgramNodeKind::Sort { input, .. }
            | lp::ProgramNodeKind::TableFunction { input, .. } => stack.push(*input),
            lp::ProgramNodeKind::Join { left, right, .. }
            | lp::ProgramNodeKind::NestedLoopJoin { left, right, .. } => {
                stack.push(*left);
                stack.push(*right);
            }
            lp::ProgramNodeKind::UnionAll { inputs }
            | lp::ProgramNodeKind::TableFinish { inputs, .. }
            | lp::ProgramNodeKind::SetOp { inputs, .. } => stack.extend(inputs.iter().copied()),
        }
    }
    Ok(())
}

fn expr(id: lp::ProgramExprId) -> ExprId {
    ExprId(id.index())
}

fn sort_expressions(expressions: &[lp::SortExpression]) -> Vec<RuntimeSortExpression> {
    expressions
        .iter()
        .map(|sort| RuntimeSortExpression {
            expr: expr(sort.expr),
            asc: sort.asc,
            nulls_first: sort.nulls_first,
        })
        .collect()
}

fn thaw_window_boundary(boundary: lp::WindowBoundary) -> RuntimeWindowBoundary {
    match boundary {
        lp::WindowBoundary::CurrentRow => RuntimeWindowBoundary::CurrentRow,
        lp::WindowBoundary::Preceding(value) => RuntimeWindowBoundary::Preceding(value),
        lp::WindowBoundary::Following(value) => RuntimeWindowBoundary::Following(value),
    }
}

fn thaw_window_function_kind(kind: &lp::WindowFunctionKind) -> RuntimeWindowFunctionKind {
    use RuntimeWindowFunctionKind as R;
    use lp::WindowFunctionKind as S;
    match kind {
        S::RowNumber => R::RowNumber,
        S::Rank => R::Rank,
        S::DenseRank => R::DenseRank,
        S::CumeDist => R::CumeDist,
        S::PercentRank => R::PercentRank,
        S::Ntile => R::Ntile,
        S::FirstValue { ignore_nulls } => R::FirstValue {
            ignore_nulls: *ignore_nulls,
        },
        S::FirstValueRewrite { ignore_nulls } => R::FirstValueRewrite {
            ignore_nulls: *ignore_nulls,
        },
        S::LastValue { ignore_nulls } => R::LastValue {
            ignore_nulls: *ignore_nulls,
        },
        S::Lead { ignore_nulls } => R::Lead {
            ignore_nulls: *ignore_nulls,
        },
        S::Lag { ignore_nulls } => R::Lag {
            ignore_nulls: *ignore_nulls,
        },
        S::SessionNumber => R::SessionNumber,
        S::Count => R::Count,
        S::Sum => R::Sum,
        S::Avg => R::Avg,
        S::Min => R::Min,
        S::Max => R::Max,
        S::BitmapUnion => R::BitmapUnion,
        S::BitmapUnionCount => R::BitmapUnionCount,
        S::MaxBy => R::MaxBy,
        S::MinBy => R::MinBy,
        S::VarianceSamp => R::VarianceSamp,
        S::StddevSamp => R::StddevSamp,
        S::BoolOr => R::BoolOr,
        S::CovarPop => R::CovarPop,
        S::CovarSamp => R::CovarSamp,
        S::Corr => R::Corr,
        S::ArrayAgg {
            is_distinct,
            is_asc_order,
            nulls_first,
        } => R::ArrayAgg {
            is_distinct: *is_distinct,
            is_asc_order: is_asc_order.clone(),
            nulls_first: nulls_first.clone(),
        },
        S::ApproxTopK => R::ApproxTopK,
    }
}

fn runtime_filter_contract(
    contract: &lp::StaticFilterContract,
) -> Result<execution::RuntimeFilterExecutionContract, String> {
    match contract {
        lp::StaticFilterContract::Membership {
            data_type,
            null_semantics,
            digest,
        } => {
            let null_semantics = match null_semantics {
                lp::FilterNullSemantics::NeverMatches => {
                    execution::RuntimeFilterNullSemantics::NeverMatches
                }
                lp::FilterNullSemantics::NullSafeEqual => {
                    execution::RuntimeFilterNullSemantics::NullSafeEqual
                }
            };
            let schema = execution::RuntimeFilterMembershipSchema::new(data_type, null_semantics)
                .map_err(|error| error.to_string())?;
            if schema.digest() != *digest {
                return Err("local program membership filter digest mismatch".to_string());
            }
            Ok(execution::RuntimeFilterExecutionContract::Membership(
                schema,
            ))
        }
        lp::StaticFilterContract::Ordered {
            keys,
            comparator_digest,
            contract_digest,
        } => {
            let keys = keys
                .iter()
                .map(|key| {
                    execution::contribution::RuntimeOrderKey::with_order(
                        key.data_type.clone(),
                        match key.direction {
                            lp::FilterSortDirection::Ascending => {
                                execution::contribution::RuntimeOrderSortDirection::Ascending
                            }
                            lp::FilterSortDirection::Descending => {
                                execution::contribution::RuntimeOrderSortDirection::Descending
                            }
                        },
                        match key.null_order {
                            lp::FilterNullOrder::First => {
                                execution::contribution::RuntimeOrderNullOrder::First
                            }
                            lp::FilterNullOrder::Last => {
                                execution::contribution::RuntimeOrderNullOrder::Last
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            let order = execution::contribution::RuntimeOrderContract::from_fragment_contract(
                keys,
                *comparator_digest,
                *contract_digest,
            )
            .map_err(|error| error.to_string())?;
            Ok(execution::RuntimeFilterExecutionContract::Ordered(
                Arc::new(order),
            ))
        }
    }
}

fn runtime_filter_consumer(
    binding: &lp::FilterConsumerAtExpr,
) -> Result<RuntimeFilterConsumerBinding, String> {
    let static_consumer = &binding.consumer;
    let id = execution::RuntimeFilterBindingId::new(static_consumer.binding_id());
    let channel = execution::RuntimeFilterChannelId::new(static_consumer.channel_id());
    let contract = runtime_filter_contract(static_consumer.contract())?;
    let runtime_contract = match (static_consumer.activation(), static_consumer.reduction()) {
        (lp::FilterConsumerActivation::BlockingSnapshot, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterConsumerContract::membership_blocking(id, channel, contract)
        }
        (lp::FilterConsumerActivation::NonBlockingLive { late_apply }, reduction) => {
            let late_apply = match late_apply {
                lp::FilterLateApplyGranularity::Row => {
                    execution::RuntimeFilterLateApplyGranularity::Row
                }
                lp::FilterLateApplyGranularity::Batch => {
                    execution::RuntimeFilterLateApplyGranularity::Batch
                }
                lp::FilterLateApplyGranularity::RowGroup => {
                    execution::RuntimeFilterLateApplyGranularity::RowGroup
                }
                lp::FilterLateApplyGranularity::Split => {
                    execution::RuntimeFilterLateApplyGranularity::Split
                }
                lp::FilterLateApplyGranularity::File => {
                    execution::RuntimeFilterLateApplyGranularity::File
                }
            };
            match reduction {
                lp::FilterReduction::SetUnion => {
                    execution::RuntimeFilterConsumerContract::membership_live(
                        id, channel, late_apply, contract,
                    )
                }
                lp::FilterReduction::TightenOrderedBound => {
                    execution::RuntimeFilterConsumerContract::ordered_live(
                        id, channel, late_apply, contract,
                    )
                }
                lp::FilterReduction::MergeTopKSummary { k } => {
                    execution::RuntimeFilterConsumerContract::top_k_live(
                        id,
                        channel,
                        late_apply,
                        k.get(),
                        contract,
                    )
                }
            }
        }
        _ => {
            return Err(
                "local program runtime filter consumer activation/reduction mismatch".to_string(),
            );
        }
    }
    .map_err(|error| error.to_string())?;
    let scan_domain = static_consumer.scan_domain().map(|target| {
        execution::scan_domain::RuntimeFilterScanDomainBinding::new(
            id,
            execution::scan_domain::RuntimeFilterScanDomainTarget::new(
                target.field_ordinal,
                target.data_type.clone(),
                target.nullable,
            ),
        )
    });
    Ok(RuntimeFilterConsumerBinding::new(
        expr(binding.expr_id),
        runtime_contract,
        scan_domain,
    ))
}

fn runtime_filter_consumers(
    bindings: &[lp::FilterConsumerAtExpr],
) -> Result<Vec<RuntimeFilterConsumerBinding>, String> {
    bindings.iter().map(runtime_filter_consumer).collect()
}

fn runtime_filter_producer(
    producer: &lp::StaticFilterProducer,
) -> Result<execution::RuntimeFilterProducerContract, String> {
    let id = execution::RuntimeFilterBindingId::new(producer.binding_id());
    let channel = execution::RuntimeFilterChannelId::new(producer.channel_id());
    let contract = runtime_filter_contract(producer.contract())?;
    let result = match (producer.kind(), producer.reduction()) {
        (lp::FilterProducerKind::Membership, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterProducerContract::membership(id, channel, contract)
        }
        (lp::FilterProducerKind::FinalDomain, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterProducerContract::final_domain(id, channel, contract)
        }
        (lp::FilterProducerKind::OrderedBound, lp::FilterReduction::TightenOrderedBound) => {
            execution::RuntimeFilterProducerContract::ordered_bound(id, channel, contract)
        }
        (lp::FilterProducerKind::TopKSummary, lp::FilterReduction::MergeTopKSummary { k }) => {
            execution::RuntimeFilterProducerContract::top_k_summary(id, channel, k.get(), contract)
        }
        _ => {
            return Err(
                "local program runtime filter producer kind/reduction mismatch".to_string(),
            );
        }
    };
    result.map_err(|error| error.to_string())
}

fn runtime_unpivot_constant(value: &lp::UnpivotConstant) -> RuntimeUnpivotConstant {
    match value {
        lp::UnpivotConstant::Scalar { expr_id, nullable } => RuntimeUnpivotConstant::Scalar {
            expr_id: expr(*expr_id),
            nullable: *nullable,
        },
        lp::UnpivotConstant::Int32List(values) => RuntimeUnpivotConstant::Int32List(values.clone()),
        lp::UnpivotConstant::Utf8Map(values) => RuntimeUnpivotConstant::Utf8Map(
            values
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        ),
    }
}

fn runtime_writer_final_plan(
    plan: &lp::WriterFinalAggregatePlan,
) -> RuntimeWriterFinalAggregatePlan {
    RuntimeWriterFinalAggregatePlan {
        calls: plan
            .calls
            .iter()
            .map(|call| RuntimeWriterFinalAggregateCall {
                function_name: Arc::clone(&call.function_name),
                resolved: call.resolved.clone(),
                intermediate_input_slot_id: call.intermediate_input_slot_id,
                final_output_slot_id: call.final_output_slot_id,
            })
            .collect(),
        unpivot: plan
            .unpivot
            .as_ref()
            .map(|unpivot| RuntimeWriterGroupedUnpivotPlan {
                grouping_input_slot_id: unpivot.grouping_input_slot_id,
                grouping_output_slot_id: unpivot.grouping_output_slot_id,
                passthrough_output_slot_id: unpivot.passthrough_output_slot_id,
                value_output_slot_id: unpivot.value_output_slot_id,
                literal_output_slot_ids: unpivot.literal_output_slot_ids.clone(),
                mappings: unpivot
                    .mappings
                    .iter()
                    .map(|mapping| RuntimeWriterGroupedUnpivotMapping {
                        grouping_key: mapping.grouping_key,
                        input_value_slot_id: mapping.input_value_slot_id,
                        constants: mapping
                            .constants
                            .iter()
                            .map(runtime_unpivot_constant)
                            .collect(),
                    })
                    .collect(),
                max_output_rows: unpivot.max_output_rows,
                max_output_bytes: unpivot.max_output_bytes,
            }),
    }
}

fn runtime_aggregate_calls(
    functions: &[lp::StaticAggregateCall],
) -> (
    Vec<AggFunction>,
    Vec<novarocks_functions::ResolvedAggregateSignature>,
) {
    let calls = functions
        .iter()
        .map(|function| AggFunction {
            name: function.name.to_string(),
            inputs: function.inputs.iter().copied().map(expr).collect(),
            input_is_intermediate: function.input_is_intermediate,
            types: function.types.as_ref().map(|types| AggTypeSignature {
                intermediate_type: types.intermediate_type.clone(),
                output_type: types.output_type.clone(),
                input_arg_type: types.input_arg_type.clone(),
            }),
            order: AggOrderSpec {
                is_asc_order: function.order.is_asc_order.clone(),
                nulls_first: function.order.nulls_first.clone(),
                is_distinct: function.order.is_distinct,
                group_concat_max_len: function.order.group_concat_max_len,
            },
        })
        .collect();
    let resolved = functions
        .iter()
        .map(|function| function.resolved.clone())
        .collect();
    (calls, resolved)
}

fn runtime_aggregate_topn_filters(
    filters: &[lp::AggregateTopNFilter],
) -> Result<Vec<AggregateTopNRuntimeFilterProducerBinding>, String> {
    filters
        .iter()
        .map(|filter| {
            Ok(AggregateTopNRuntimeFilterProducerBinding::new(
                expr(filter.group_key_expr),
                filter.group_key_ordinal,
                filter.limit,
                runtime_filter_producer(&filter.producer)?,
            ))
        })
        .collect()
}

fn runtime_join_type(join_type: lp::JoinType) -> RuntimeJoinType {
    match join_type {
        lp::JoinType::Inner => RuntimeJoinType::Inner,
        lp::JoinType::LeftOuter => RuntimeJoinType::LeftOuter,
        lp::JoinType::RightOuter => RuntimeJoinType::RightOuter,
        lp::JoinType::FullOuter => RuntimeJoinType::FullOuter,
        lp::JoinType::LeftSemi => RuntimeJoinType::LeftSemi,
        lp::JoinType::RightSemi => RuntimeJoinType::RightSemi,
        lp::JoinType::LeftAnti => RuntimeJoinType::LeftAnti,
        lp::JoinType::RightAnti => RuntimeJoinType::RightAnti,
        lp::JoinType::NullAwareLeftAnti => RuntimeJoinType::NullAwareLeftAnti,
    }
}

fn runtime_join_producers(
    filters: &[lp::FilterProducerAtExpr],
) -> Result<Vec<JoinRuntimeFilterProducerBinding>, String> {
    filters
        .iter()
        .map(|filter| {
            Ok(JoinRuntimeFilterProducerBinding::new(
                expr(filter.expr_id),
                filter.key_ordinal,
                runtime_filter_producer(&filter.producer)?,
            ))
        })
        .collect()
}

fn keyed_assert_distribution_keys_from_layout(
    ctx: &mut PipelineBuildContext,
    layout: &lp::StaticLayout,
    key_slots: &[novarocks_types::SlotId],
) -> Result<Vec<ExprId>, String> {
    let schema = ChunkSchema::from_static_layout(layout)?;
    let mut keys = Vec::with_capacity(key_slots.len());
    for slot in key_slots {
        let slot_schema = schema.slot(*slot).ok_or_else(|| {
            format!("keyed assert_num_rows key slot {slot} is not present in child output schema")
        })?;
        let arena = Arc::make_mut(&mut ctx.arena);
        let id = arena.push_typed(ExprNode::SlotId(*slot), slot_schema.data_type().clone());
        arena.set_field_schema(id, slot_schema.field_schema().clone());
        keys.push(id);
    }
    Ok(keys)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Typed set operators share physical stage construction"
)]
fn build_distinct_set_op_pipeline_for_program<S, MakeShared, MakeSink, MakeSource>(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
    inputs: &[lp::ProgramNodeId],
    node_id: i32,
    output_schema: &crate::exec::chunk::ChunkSchemaRef,
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
        input_builds.push(build_pipeline_for_program_node(
            program, bindings, *input, ctx,
        )?);
    }
    let stage_producers = input_builds
        .iter()
        .map(|build| build.pipeline.dop as usize)
        .collect();
    let controller = SetOpStageController::new(controller_name, stage_producers)?;
    let shared = make_shared(controller, Arc::clone(output_schema));
    let mut extra_pipelines = Vec::new();
    for (stage, mut child) in input_builds.into_iter().enumerate() {
        child
            .pipeline
            .factories
            .push(make_sink(stage, shared.clone(), node_id));
        child.pipeline.needs_sink = false;
        extra_pipelines.push(child.pipeline);
        extra_pipelines.append(&mut child.extra_pipelines);
    }
    let source = make_source(shared, node_id);
    let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
    Ok(PipelineBuildResult {
        pipeline,
        extra_pipelines,
        stream: StreamDesc::any(1),
    })
}

fn build_pipeline_for_program_node(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
    id: lp::ProgramNodeId,
    ctx: &mut PipelineBuildContext,
) -> Result<PipelineBuildResult, String> {
    let node = program
        .nodes()
        .get(id.index())
        .ok_or_else(|| format!("missing local program node {}", id.index()))?;
    let node_id = node.native_node_id();
    match node.kind() {
        lp::ProgramNodeKind::RuntimeFilterConsumer {
            input,
            bindings: filter_bindings,
        } => {
            let consumers = runtime_filter_consumers(filter_bindings)?;
            validate_native_consumer_specs(&consumers, ctx)?;
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            if !consumers.is_empty() {
                build
                    .pipeline
                    .factories
                    .push(Box::new(NativeRuntimeFilterProcessorFactory::new(
                        node_id,
                        &consumers,
                        Arc::clone(&ctx.arena),
                    )?));
            }
            Ok(build)
        }
        lp::ProgramNodeKind::AssertNumRows { input, mode } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let mode = match mode {
                lp::AssertRowsMode::Global {
                    desired_num_rows,
                    assertion,
                    subquery_string,
                } => AssertNumRowsMode::Global {
                    desired_num_rows: *desired_num_rows,
                    assertion: match assertion {
                        lp::RowAssertion::Eq => Assertion::Eq,
                        lp::RowAssertion::Ne => Assertion::Ne,
                        lp::RowAssertion::Lt => Assertion::Lt,
                        lp::RowAssertion::Le => Assertion::Le,
                        lp::RowAssertion::Gt => Assertion::Gt,
                        lp::RowAssertion::Ge => Assertion::Ge,
                    },
                    subquery_string: subquery_string.as_ref().map(ToString::to_string),
                },
                lp::AssertRowsMode::PerKeyAtMostOne {
                    key_slots,
                    key_labels,
                    message_prefix,
                } => AssertNumRowsMode::PerKeyAtMostOne {
                    key_slots: key_slots.clone(),
                    key_labels: key_labels.iter().map(ToString::to_string).collect(),
                    message_prefix: message_prefix.to_string(),
                },
            };
            if let AssertNumRowsMode::PerKeyAtMostOne { key_slots, .. } = &mode {
                let partition_count = ctx.pipeline_dop.max(1) as usize;
                let distribution_keys = existing_hash_distribution_keys_for_slots(
                    ctx,
                    &build.stream,
                    key_slots,
                    partition_count,
                )
                .or_else(|| ctx.precomputed_keyed_assert_keys.get(&node_id).cloned())
                .ok_or_else(|| {
                    format!(
                        "keyed assert_num_rows node {node_id} has no precomputed distribution keys"
                    )
                })?;
                build = ensure_hash_on_input_slots(
                    build,
                    ctx,
                    node_id,
                    key_slots.clone(),
                    distribution_keys,
                    partition_count,
                );
            }
            build
                .pipeline
                .factories
                .push(Box::new(AssertNumRowsProcessorFactory::new(node_id, mode)?));
            Ok(build)
        }
        lp::ProgramNodeKind::Values { values } => {
            let chunk_schema = ChunkSchema::from_static_layout(values.layout())?;
            let chunk = Chunk::new_with_chunk_schema(values.batch().clone(), chunk_schema);
            let source: Box<dyn OperatorFactory> =
                Box::new(ValuesSourceFactory::new(chunk, node_id));
            let pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(1),
            })
        }
        lp::ProgramNodeKind::ExchangeSource {
            timeout,
            runtime_filters,
            hash_partition_exprs,
        } => {
            let consumers = runtime_filter_consumers(runtime_filters)?;
            validate_native_consumer_specs(&consumers, ctx)?;
            let binding = ctx
                .exchange_bindings
                .get(node_id)
                .ok_or_else(|| format!("missing exchange binding for node {node_id}"))?;
            let exchange = ExchangeSourceNode::new(
                node_id,
                *timeout,
                ChunkSchema::from_static_layout(node.output_layout())?,
            )
            .with_hash_partition_exprs(hash_partition_exprs.iter().copied().map(expr).collect())
            .with_runtime_filter_consumers(consumers);
            let factory =
                ExchangeSourceFactory::new_native(exchange, binding, Arc::clone(&ctx.arena))?;
            let source: Box<dyn OperatorFactory> = Box::new(factory);
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        lp::ProgramNodeKind::Scan {
            runtime_filters,
            conjunct_predicate,
            connector_io_tasks_per_scan_operator,
            limit,
            accept_empty_scan_ranges,
            ..
        } => {
            let consumers = runtime_filter_consumers(runtime_filters)?;
            validate_native_consumer_specs(&consumers, ctx)?;
            let source = bindings
                .scans
                .get(&id)
                .ok_or_else(|| format!("missing runtime scan source for node {node_id}"))?;
            let op = ctx
                .scan_bindings
                .get(node_id)
                .ok_or_else(|| format!("missing scan binding for node {node_id}"))?;
            let scan = ScanNode::new(Arc::clone(source))
                .with_node_id(node_id)
                .with_runtime_filter_consumers(consumers)
                .with_output_chunk_schema(ChunkSchema::from_static_layout(node.output_layout())?)
                .with_conjunct_predicate(conjunct_predicate.map(expr))
                .with_connector_io_tasks_per_scan_operator(*connector_io_tasks_per_scan_operator)
                .with_limit(*limit)
                .with_accept_empty_scan_ranges(*accept_empty_scan_ranges);
            let factory = ScanSourceFactory::new_native(scan, op, Arc::clone(&ctx.arena))?
                .with_operator_buffer_chunks(ctx.operator_buffer_chunks);
            let source: Box<dyn OperatorFactory> = Box::new(factory);
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines: Vec::new(),
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        lp::ProgramNodeKind::TableWriter {
            input,
            target,
            expected_layout,
            projection,
            writer_multiplex_layout,
            partial_aggregates,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let hash_key = match program.nodes()[input.index()].kind() {
                lp::ProgramNodeKind::ExchangeSource {
                    hash_partition_exprs,
                    ..
                } => hash_partition_exprs.iter().copied().map(expr).collect(),
                _ => Vec::new(),
            };
            if !hash_key.is_empty() {
                let partitions = build.pipeline.dop.max(1) as usize;
                build = shuffle_by_hash(build, ctx, node_id, hash_key, partitions);
            }
            let binding = bindings
                .writers
                .get(&id)
                .ok_or_else(|| format!("missing runtime writer binding for node {node_id}"))?;
            let relation =
                WriterMultiplexRelationSchema::try_from_static_layout(writer_multiplex_layout)?;
            let projection = TableWriterInputProjection::from_static(projection)?;
            let partial_calls = partial_aggregates
                .iter()
                .map(|call| RuntimeWriterPartialAggregateCall {
                    input_slot_id: call.input_slot_id,
                    function_name: Arc::clone(&call.function_name),
                    resolved: call.resolved.clone(),
                    intermediate_slot_id: call.intermediate_slot_id,
                })
                .collect::<Vec<_>>();
            build
                .pipeline
                .factories
                .push(Box::new(TableWriterOperatorFactory::try_new_local(
                    node_id,
                    *target,
                    Arc::clone(expected_layout.schema()),
                    projection,
                    relation,
                    &partial_calls,
                    binding,
                    Arc::clone(&ctx.function_set),
                )?));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        lp::ProgramNodeKind::TableFinish {
            inputs,
            expected_targets,
            writer_multiplex_layout,
            root_result_layout,
            final_aggregates,
        } => {
            let mut input_builds = Vec::with_capacity(inputs.len());
            let mut producer_count = 0usize;
            for input in inputs {
                let child = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
                producer_count = producer_count.saturating_add(child.pipeline.dop as usize);
                input_builds.push(child);
            }
            let state = UnionAllSharedState::new(producer_count.max(1), node_id);
            let mut extra_pipelines = Vec::new();
            for mut child in input_builds {
                child
                    .pipeline
                    .factories
                    .push(Box::new(UnionAllSinkFactory::new(state.clone(), node_id)));
                child.pipeline.needs_sink = false;
                extra_pipelines.push(child.pipeline);
                extra_pipelines.append(&mut child.extra_pipelines);
            }
            let source = Box::new(UnionAllSourceFactory::new(state, node_id));
            let mut pipeline = new_source_pipeline_with_dop(ctx, source, 1);
            let binding = bindings
                .finishers
                .get(&id)
                .ok_or_else(|| format!("missing runtime finish binding for node {node_id}"))?;
            pipeline
                .factories
                .push(Box::new(TableFinishOperatorFactory::new_local(
                    node_id,
                    expected_targets.clone(),
                    WriterMultiplexRelationSchema::try_from_static_layout(writer_multiplex_layout)?,
                    RootWriteResultRelationSchema::try_from_static_layout(root_result_layout)?,
                    runtime_writer_final_plan(final_aggregates),
                    binding,
                    Arc::clone(&ctx.arena),
                )?));
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines,
                stream: StreamDesc::single(),
            })
        }
        lp::ProgramNodeKind::NestedLoopJoin {
            left,
            right,
            join_type,
            join_conjunct,
            left_layout,
            right_layout,
            join_scope_layout,
        } => {
            let join_type = match join_type {
                lp::NestedLoopJoinType::Inner => RuntimeNestedLoopJoinType::Inner,
                lp::NestedLoopJoinType::Cross => RuntimeNestedLoopJoinType::Cross,
                lp::NestedLoopJoinType::LeftOuter => RuntimeNestedLoopJoinType::LeftOuter,
                lp::NestedLoopJoinType::RightOuter => RuntimeNestedLoopJoinType::RightOuter,
                lp::NestedLoopJoinType::FullOuter => RuntimeNestedLoopJoinType::FullOuter,
                lp::NestedLoopJoinType::LeftSemi => RuntimeNestedLoopJoinType::LeftSemi,
                lp::NestedLoopJoinType::LeftAnti => RuntimeNestedLoopJoinType::LeftAnti,
                lp::NestedLoopJoinType::NullAwareLeftAnti => {
                    RuntimeNestedLoopJoinType::NullAwareLeftAnti
                }
            };
            let probe_is_left = join_type != RuntimeNestedLoopJoinType::RightOuter;
            let (probe_child, build_child) = if probe_is_left {
                (left, right)
            } else {
                (right, left)
            };
            let mut probe_build =
                build_pipeline_for_program_node(program, bindings, *probe_child, ctx)?;
            let build_build =
                build_pipeline_for_program_node(program, bindings, *build_child, ctx)?;
            let mut build_build = gather_to_one(build_build, ctx, node_id);
            let probe_producers = probe_build.pipeline.dop.max(1) as usize;
            let state = Arc::new(NlJoinSharedState::new(
                node_id,
                probe_producers,
                ctx.dep_manager.clone(),
            ));
            probe_build
                .pipeline
                .factories
                .push(Box::new(NlJoinProbeProcessorFactory::new(
                    Arc::clone(&ctx.arena),
                    join_type,
                    join_conjunct.map(expr),
                    probe_is_left,
                    ChunkSchema::from_static_layout(left_layout)?,
                    ChunkSchema::from_static_layout(right_layout)?,
                    ChunkSchema::from_static_layout(join_scope_layout)?,
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
        lp::ProgramNodeKind::Aggregate {
            input,
            group_by,
            functions,
            need_finalize,
            topn_filters,
            streaming_preaggregation_mode,
            ..
        } => build_aggregate_pipeline(
            program,
            bindings,
            *input,
            node_id,
            node.output_layout(),
            group_by,
            functions,
            *need_finalize,
            topn_filters,
            *streaming_preaggregation_mode,
            ctx,
        ),
        lp::ProgramNodeKind::Join {
            left,
            right,
            join_type,
            distribution_mode,
            left_layout,
            right_layout,
            join_scope_layout,
            probe_keys,
            build_keys,
            eq_null_safe,
            residual_predicate,
            runtime_filters,
        } => build_join_pipeline(
            program,
            bindings,
            *left,
            *right,
            node_id,
            runtime_join_type(*join_type),
            match distribution_mode {
                lp::JoinDistributionMode::Broadcast => RuntimeJoinDistributionMode::Broadcast,
                lp::JoinDistributionMode::Partitioned => RuntimeJoinDistributionMode::Partitioned,
            },
            left_layout,
            right_layout,
            join_scope_layout,
            probe_keys,
            build_keys,
            eq_null_safe,
            *residual_predicate,
            runtime_filters,
            ctx,
        ),
        lp::ProgramNodeKind::Filter { input, predicate } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(FilterProcessorFactory::new(
                    node_id,
                    Arc::clone(&ctx.arena),
                    expr(*predicate),
                )));
            Ok(build)
        }
        lp::ProgramNodeKind::Unpivot {
            input,
            passthrough_columns,
            value_output_slot_id,
            literal_output_slot_ids,
            value_mappings,
            max_output_rows,
            max_output_bytes,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let passthrough_columns = passthrough_columns
                .iter()
                .map(|column| UnpivotPassthroughColumn {
                    input_slot_id: column.input_slot_id,
                    output_slot_id: column.output_slot_id,
                })
                .collect();
            let value_mappings = value_mappings
                .iter()
                .map(|mapping| UnpivotValueMapping {
                    input_value_slot_id: mapping.input_value_slot_id,
                    constants: mapping
                        .constants
                        .iter()
                        .map(|value| match value {
                            lp::UnpivotConstant::Scalar { expr_id, nullable } => {
                                RuntimeUnpivotConstant::Scalar {
                                    expr_id: expr(*expr_id),
                                    nullable: *nullable,
                                }
                            }
                            lp::UnpivotConstant::Int32List(values) => {
                                RuntimeUnpivotConstant::Int32List(values.clone())
                            }
                            lp::UnpivotConstant::Utf8Map(values) => {
                                RuntimeUnpivotConstant::Utf8Map(
                                    values
                                        .iter()
                                        .map(|(k, v)| (k.to_string(), v.to_string()))
                                        .collect(),
                                )
                            }
                        })
                        .collect(),
                })
                .collect();
            build
                .pipeline
                .factories
                .push(Box::new(UnpivotProcessorFactory::new(
                    node_id,
                    Arc::clone(&ctx.arena),
                    passthrough_columns,
                    *value_output_slot_id,
                    literal_output_slot_ids.clone(),
                    value_mappings,
                    ChunkSchema::from_static_layout(node.output_layout())?,
                    *max_output_rows,
                    *max_output_bytes,
                )?));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        lp::ProgramNodeKind::Repeat {
            input,
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            build
                .pipeline
                .factories
                .push(Box::new(RepeatProcessorFactory::new(
                    node_id,
                    null_slot_ids.clone(),
                    grouping_slot_ids.clone(),
                    grouping_list.clone(),
                    *repeat_times,
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        lp::ProgramNodeKind::ChangeEventExpand {
            input,
            events,
            output_slot_ids,
            effect_slot_id,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let events = events
                .iter()
                .map(|event| ChangeEventRuntimeSpec {
                    predicate: event.predicate.map(expr),
                    effect: event.effect,
                    assignments: event
                        .assignments
                        .iter()
                        .map(|assignment| ChangeEventRuntimeOutputExpr {
                            output_slot_id: assignment.output_slot_id,
                            expr: assignment.expr.map(expr),
                        })
                        .collect(),
                })
                .collect();
            build
                .pipeline
                .factories
                .push(Box::new(ChangeEventExpandProcessorFactory::new(
                    node_id,
                    Arc::clone(&ctx.arena),
                    events,
                    ChunkSchema::from_static_layout(node.output_layout())?,
                    output_slot_ids.clone(),
                    *effect_slot_id,
                )?));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        lp::ProgramNodeKind::Limit {
            input,
            limit,
            offset,
        } => {
            let build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let mut build = gather_to_one(build, ctx, node_id);
            build
                .pipeline
                .factories
                .push(Box::new(LimitProcessorFactory::new(
                    node_id, *limit, *offset,
                )));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        lp::ProgramNodeKind::Sort {
            input,
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows,
            max_buffered_bytes,
            partition_exprs,
            partition_limit,
        } => {
            let build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let mut build = gather_to_one(build, ctx, node_id);
            let order_by = sort_expressions(order_by);
            let partition_exprs = sort_expressions(partition_exprs);
            let topn_type = match topn_type {
                lp::SortTopNType::RowNumber => RuntimeSortTopNType::RowNumber,
                lp::SortTopNType::Rank => RuntimeSortTopNType::Rank,
                lp::SortTopNType::DenseRank => RuntimeSortTopNType::DenseRank,
            };
            let factory = if *use_top_n {
                SortProcessorFactory::new_topn(
                    node_id,
                    Arc::clone(&ctx.arena),
                    order_by,
                    *limit,
                    *offset,
                    topn_type,
                    *max_buffered_rows,
                    *max_buffered_bytes,
                    partition_exprs,
                    *partition_limit,
                )
            } else {
                SortProcessorFactory::new(
                    node_id,
                    Arc::clone(&ctx.arena),
                    order_by,
                    *limit,
                    *offset,
                    topn_type,
                    *max_buffered_rows,
                    *max_buffered_bytes,
                    partition_exprs,
                    *partition_limit,
                )
            };
            build.pipeline.factories.push(Box::new(factory));
            build.stream = StreamDesc::single();
            Ok(build)
        }
        lp::ProgramNodeKind::TableFunction {
            input,
            function_name,
            param_slots,
            outer_slots,
            fn_result_slots,
            fn_result_required,
            is_left_join,
            param_types,
            ret_types,
            output_slot_sources,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let output_slot_sources = output_slot_sources
                .iter()
                .map(|source| match source {
                    lp::TableFunctionOutputSlot::Outer { slot } => {
                        RuntimeTableFunctionOutputSlot::Outer { slot: *slot }
                    }
                    lp::TableFunctionOutputSlot::Result { index } => {
                        RuntimeTableFunctionOutputSlot::Result { index: *index }
                    }
                })
                .collect();
            build
                .pipeline
                .factories
                .push(Box::new(TableFunctionProcessorFactory::new(
                    node_id,
                    function_name.to_string(),
                    param_slots.clone(),
                    outer_slots.clone(),
                    fn_result_slots.clone(),
                    *fn_result_required,
                    *is_left_join,
                    param_types.clone(),
                    ret_types.clone(),
                    ChunkSchema::from_static_layout(node.output_layout())?,
                    output_slot_sources,
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
        lp::ProgramNodeKind::Analytic {
            input,
            partition_exprs,
            order_by_exprs,
            functions,
            window,
            output_columns,
        } => {
            let build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let mut build = gather_to_one(build, ctx, node_id);
            let window = window.as_ref().map(|frame| RuntimeWindowFrame {
                start: frame.start.map(thaw_window_boundary),
                end: frame.end.map(thaw_window_boundary),
                window_type: match frame.window_type {
                    lp::WindowType::Rows => RuntimeWindowType::Rows,
                    lp::WindowType::Range => RuntimeWindowType::Range,
                },
            });
            let functions = functions
                .iter()
                .map(|function| WindowFunctionSpec {
                    kind: thaw_window_function_kind(&function.kind),
                    args: function.args.iter().copied().map(expr).collect(),
                    return_type: function.return_type.clone(),
                    aggregate_binding: function.aggregate_binding.as_ref().map(
                        |(name, resolved)| WindowAggregateBinding {
                            function_name: name.to_string(),
                            resolved: resolved.clone(),
                        },
                    ),
                })
                .collect();
            let output_columns = output_columns
                .iter()
                .map(|column| match column {
                    lp::AnalyticOutputColumn::InputSlotId(slot) => {
                        RuntimeAnalyticOutputColumn::InputSlotId(*slot)
                    }
                    lp::AnalyticOutputColumn::Window(index) => {
                        RuntimeAnalyticOutputColumn::Window(*index)
                    }
                })
                .collect();
            let state = AnalyticSharedState::new_with_buffer_limit(
                Arc::clone(&ctx.arena),
                partition_exprs.iter().copied().map(expr).collect(),
                order_by_exprs.iter().copied().map(expr).collect(),
                functions,
                window,
                output_columns,
                ChunkSchema::from_static_layout(node.output_layout())?,
                Arc::clone(&ctx.function_set),
                node_id,
                ctx.operator_buffer_chunks,
            )?;
            build
                .pipeline
                .factories
                .push(Box::new(AnalyticSinkFactory::new(state.clone())));
            build.pipeline.needs_sink = false;
            let source = Box::new(AnalyticSourceFactory::new(state));
            let downstream = new_source_pipeline_with_dop(ctx, source, 1);
            let mut extra_pipelines = build.extra_pipelines;
            extra_pipelines.push(build.pipeline);
            Ok(PipelineBuildResult {
                pipeline: downstream,
                extra_pipelines,
                stream: StreamDesc::single(),
            })
        }
        lp::ProgramNodeKind::UnionAll { inputs } => {
            let mut input_builds = Vec::with_capacity(inputs.len());
            let mut producer_count = 0usize;
            for input in inputs {
                let child = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
                producer_count = producer_count.saturating_add(child.pipeline.dop as usize);
                input_builds.push(child);
            }
            let state = UnionAllSharedState::new(producer_count.max(1), node_id);
            let mut extra_pipelines = Vec::new();
            for mut child in input_builds {
                child
                    .pipeline
                    .factories
                    .push(Box::new(UnionAllSinkFactory::new(state.clone(), node_id)));
                child.pipeline.needs_sink = false;
                extra_pipelines.push(child.pipeline);
                extra_pipelines.append(&mut child.extra_pipelines);
            }
            let source = Box::new(UnionAllSourceFactory::new(state, node_id));
            let pipeline = new_source_pipeline(ctx, source);
            Ok(PipelineBuildResult {
                pipeline,
                extra_pipelines,
                stream: StreamDesc::any(ctx.pipeline_dop),
            })
        }
        lp::ProgramNodeKind::SetOp { kind, inputs } => {
            let output_schema = ChunkSchema::from_static_layout(node.output_layout())?;
            match kind {
                lp::SetOpKind::Intersect => build_distinct_set_op_pipeline_for_program(
                    program,
                    bindings,
                    inputs,
                    node_id,
                    &output_schema,
                    "INTERSECT_NODE",
                    "intersect",
                    ctx,
                    IntersectSharedState::new,
                    |stage, shared, id| Box::new(IntersectSinkFactory::new(stage, shared, id)),
                    |shared, id| Box::new(IntersectSourceFactory::new(shared, id)),
                ),
                lp::SetOpKind::Except => build_distinct_set_op_pipeline_for_program(
                    program,
                    bindings,
                    inputs,
                    node_id,
                    &output_schema,
                    "EXCEPT_NODE",
                    "except",
                    ctx,
                    ExceptSharedState::new,
                    |stage, shared, id| Box::new(ExceptSinkFactory::new(stage, shared, id)),
                    |shared, id| Box::new(ExceptSourceFactory::new(shared, id)),
                ),
            }
        }
        lp::ProgramNodeKind::Project {
            input,
            is_subordinate,
            exprs,
            expr_slot_ids,
            expr_slot_schemas,
            output_indices,
        } => {
            let mut build = build_pipeline_for_program_node(program, bindings, *input, ctx)?;
            let schemas = expr_slot_schemas
                .as_ref()
                .map(|schemas| {
                    schemas
                        .iter()
                        .map(|slot| {
                            ChunkSlotSchema::try_new_with_field(
                                slot.slot_id,
                                slot.field.clone(),
                                Some(thaw_field_schema(&slot.field_schema)),
                                slot.unique_id,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
            build
                .pipeline
                .factories
                .push(Box::new(ProjectProcessorFactory::new(
                    node_id,
                    *is_subordinate,
                    Arc::clone(&ctx.arena),
                    exprs.iter().copied().map(expr).collect(),
                    expr_slot_ids.clone(),
                    schemas,
                    output_indices.clone(),
                    ChunkSchema::from_static_layout(node.output_layout())?,
                )));
            build.stream = StreamDesc::any(build.pipeline.dop);
            Ok(build)
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Aggregate kernel receives its complete frozen static and runtime contract"
)]
fn build_aggregate_pipeline(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
    input: lp::ProgramNodeId,
    node_id: i32,
    output_layout: &lp::StaticLayout,
    static_group_by: &[lp::ProgramExprId],
    static_functions: &[lp::StaticAggregateCall],
    need_finalize: bool,
    topn_filters: &[lp::AggregateTopNFilter],
    streaming_preaggregation_mode: Option<lp::StreamingPreaggregationMode>,
    ctx: &mut PipelineBuildContext,
) -> Result<PipelineBuildResult, String> {
    let group_by = static_group_by
        .iter()
        .copied()
        .map(expr)
        .collect::<Vec<_>>();
    let (functions, resolved_aggregates) = runtime_aggregate_calls(static_functions);
    let output_chunk_schema = ChunkSchema::from_static_layout(output_layout)?;
    let streaming_preaggregation_mode = streaming_preaggregation_mode.map(|mode| match mode {
        lp::StreamingPreaggregationMode::Auto => StreamingPreaggregationMode::Auto,
        lp::StreamingPreaggregationMode::ForceStreaming => {
            StreamingPreaggregationMode::ForceStreaming
        }
        lp::StreamingPreaggregationMode::ForcePreaggregation => {
            StreamingPreaggregationMode::ForcePreaggregation
        }
        lp::StreamingPreaggregationMode::LimitedMem => StreamingPreaggregationMode::LimitedMem,
    });
    let mut build = build_pipeline_for_program_node(program, bindings, input, ctx)?;
    let output_slots = output_chunk_schema.slot_ids();
    let topn_producers = runtime_aggregate_topn_filters(topn_filters)?;
    validate_native_aggregate_topn_specs(&topn_producers, &group_by, ctx)?;
    let native_topn_producers = topn_producers.as_slice();

    let dop = build.pipeline.dop.max(1);
    let all_update = functions.iter().all(|f| !f.input_is_intermediate);

    if !need_finalize && !group_by.is_empty() && dop > 1 {
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
        build = ensure_hash(build, ctx, node_id, group_by.clone(), dop as usize);
    }
    if need_finalize && !group_by.is_empty() && dop > 1 && all_update {
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
            let runtime_filter_session =
                native_aggregate_topn_session(&topn_producers, &ctx.runtime_filter_execution)?;
            Box::new(AggregateProcessorFactory::new_native(
                node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                partial_functions,
                Arc::clone(&ctx.function_set),
                resolved_aggregates.clone(),
                true,
                false,
                Arc::clone(&output_chunk_schema),
                topn_producers,
                runtime_filter_session,
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
            node_id,
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
        let final_runtime_filter_session =
            native_aggregate_topn_session(&final_topn_producers, &ctx.runtime_filter_execution)?;
        build
            .pipeline
            .factories
            .push(Box::new(AggregateProcessorFactory::new_native(
                node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                merge_functions,
                Arc::clone(&ctx.function_set),
                resolved_aggregates.clone(),
                false,
                true,
                Arc::clone(&output_chunk_schema),
                final_topn_producers,
                final_runtime_filter_session,
                build.pipeline.dop,
                None,
            )?));
        return Ok(build);
    }

    if need_finalize && group_by.is_empty() && dop > 1 && all_update {
        let mut partial_functions = functions.clone();
        for func in &mut partial_functions {
            func.input_is_intermediate = false;
        }
        let local_factory: Box<dyn OperatorFactory> = {
            Box::new(AggregateProcessorFactory::new_native(
                node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                partial_functions,
                Arc::clone(&ctx.function_set),
                resolved_aggregates.clone(),
                true,
                false,
                Arc::clone(&output_chunk_schema),
                Vec::new(),
                None,
                dop,
                None,
            )?)
        };
        build.pipeline.factories.push(local_factory);

        let partition_count = 1usize;
        let exchanger = LocalExchanger::new_with_limits(
            partition_count,
            dop as usize,
            LocalExchangePartitionSpec::Single,
            Arc::clone(&ctx.arena),
            ctx.local_exchange_buffer_mem_limit_per_driver,
            ctx.local_exchange_max_buffered_rows,
        );
        build
            .pipeline
            .factories
            .push(Box::new(LocalExchangeSinkFactory::new(
                node_id,
                Arc::clone(&exchanger),
            )));
        build.pipeline.needs_sink = false;

        let source_factory = Box::new(LocalExchangeSourceFactory::new(
            node_id,
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
                node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                merge_functions,
                Arc::clone(&ctx.function_set),
                resolved_aggregates.clone(),
                false,
                true,
                Arc::clone(&output_chunk_schema),
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
        let streaming_state = AggregateStreamingState::new_with_buffer_limit(
            dop.max(1) as usize,
            ctx.operator_buffer_chunks,
        );
        let sink_factory: Box<dyn OperatorFactory> = {
            let topn_producers = aggregate_topn_producers_for_site(
                producer_site,
                AggregateTopNProducerSite::StreamingAggregateSink,
                native_topn_producers,
            );
            let runtime_filter_session =
                native_aggregate_topn_session(&topn_producers, &ctx.runtime_filter_execution)?;
            Box::new(AggregateStreamingSinkFactory::new_native(
                node_id,
                Arc::clone(&ctx.arena),
                group_by.clone(),
                functions.clone(),
                Arc::clone(&ctx.function_set),
                resolved_aggregates.clone(),
                !need_finalize,
                Arc::clone(&output_chunk_schema),
                streaming_state.clone(),
                topn_producers,
                runtime_filter_session,
                dop,
            )?)
        };
        build.pipeline.factories.push(sink_factory);
        build.pipeline.needs_sink = false;

        let source_factory = Box::new(AggregateStreamingSourceFactory::new(
            node_id,
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
        let runtime_filter_session =
            native_aggregate_topn_session(&topn_producers, &ctx.runtime_filter_execution)?;
        Box::new(AggregateProcessorFactory::new_native(
            node_id,
            Arc::clone(&ctx.arena),
            group_by.clone(),
            functions.clone(),
            Arc::clone(&ctx.function_set),
            resolved_aggregates.clone(),
            !need_finalize,
            false,
            Arc::clone(&output_chunk_schema),
            topn_producers,
            runtime_filter_session,
            dop,
            None,
        )?)
    };

    if need_finalize && dop > 1 {
        let partition_count = if group_by.is_empty() { 1 } else { dop as usize };
        let partition_spec = if partition_count <= 1 {
            LocalExchangePartitionSpec::Single
        } else {
            LocalExchangePartitionSpec::Exprs(group_by.clone())
        };
        let exchanger = LocalExchanger::new_with_limits(
            partition_count,
            dop as usize,
            partition_spec,
            Arc::clone(&ctx.arena),
            ctx.local_exchange_buffer_mem_limit_per_driver,
            ctx.local_exchange_max_buffered_rows,
        );
        build
            .pipeline
            .factories
            .push(Box::new(LocalExchangeSinkFactory::new(
                node_id,
                Arc::clone(&exchanger),
            )));
        build.pipeline.needs_sink = false;

        let source_factory = Box::new(LocalExchangeSourceFactory::new(
            node_id,
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

#[expect(
    clippy::too_many_arguments,
    reason = "Join kernel receives the complete frozen node contract"
)]
fn build_join_pipeline(
    program: &lp::LocalProgram,
    bindings: &LocalRuntimeBindings,
    left: lp::ProgramNodeId,
    right: lp::ProgramNodeId,
    node_id: i32,
    join_type: RuntimeJoinType,
    distribution_mode: RuntimeJoinDistributionMode,
    left_layout: &lp::StaticLayout,
    right_layout: &lp::StaticLayout,
    join_scope_layout: &lp::StaticLayout,
    static_probe_keys: &[lp::ProgramExprId],
    static_build_keys: &[lp::ProgramExprId],
    eq_null_safe: &[bool],
    residual_predicate: Option<lp::ProgramExprId>,
    runtime_filters: &[lp::FilterProducerAtExpr],
    ctx: &mut PipelineBuildContext,
) -> Result<PipelineBuildResult, String> {
    let left_chunk_schema = ChunkSchema::from_static_layout(left_layout)?;
    let right_chunk_schema = ChunkSchema::from_static_layout(right_layout)?;
    let join_scope_chunk_schema = ChunkSchema::from_static_layout(join_scope_layout)?;
    let probe_keys = static_probe_keys
        .iter()
        .copied()
        .map(expr)
        .collect::<Vec<_>>();
    let build_keys = static_build_keys
        .iter()
        .copied()
        .map(expr)
        .collect::<Vec<_>>();
    let eq_null_safe = eq_null_safe.to_vec();
    let residual_predicate = residual_predicate.map(expr);
    let runtime_filter_execution = crate::exec::node::join::JoinRuntimeFilterExecution::new(
        runtime_join_producers(runtime_filters)?,
    );
    validate_native_producer_specs(&runtime_filter_execution.producers, ctx)?;
    let left_build = build_pipeline_for_program_node(program, bindings, left, ctx)?;
    let right_build = build_pipeline_for_program_node(program, bindings, right, ctx)?;

    let probe_is_left = hash_join_probe_is_left(join_type);
    let (mut probe_build, mut build_build) = if probe_is_left {
        (left_build, right_build)
    } else {
        (right_build, left_build)
    };
    let probe_keys = probe_keys.clone();
    let build_keys = build_keys.clone();
    let eq_null_safe = eq_null_safe.clone();
    let has_equi_keys = !probe_keys.is_empty() && !build_keys.is_empty();

    if distribution_mode == JoinDistributionMode::Broadcast {
        if join_type == JoinType::FullOuter {
            probe_build = gather_to_one(probe_build, ctx, node_id);
        }
        build_build = gather_to_one(build_build, ctx, node_id);
        let probe_dop = probe_build.pipeline.dop.max(1) as usize;
        let join_state = Arc::new(BroadcastJoinSharedState::new(
            node_id,
            ctx.dep_manager.clone(),
            probe_dop,
        ));

        let probe_factory = {
            BroadcastJoinProbeProcessorFactory::new_native(
                Arc::clone(&ctx.arena),
                join_type,
                probe_keys.clone(),
                residual_predicate,
                probe_is_left,
                has_equi_keys,
                Arc::clone(&left_chunk_schema),
                Arc::clone(&right_chunk_schema),
                Arc::clone(&join_scope_chunk_schema),
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
                join_type,
                residual_predicate.is_some(),
                probe_is_left,
                has_equi_keys,
                build_keys.clone(),
                eq_null_safe.clone(),
                distribution_mode,
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
        probe_build = gather_to_one(probe_build, ctx, node_id);
        build_build = gather_to_one(build_build, ctx, node_id);
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
            node_id,
            probe_keys.clone(),
            join_partitions,
        );
    }
    if !build_keys.is_empty() {
        build_build = ensure_hash(
            build_build,
            ctx,
            node_id,
            build_keys.clone(),
            join_partitions,
        );
    }

    let join_state = Arc::new(PartitionedJoinSharedState::new(
        node_id,
        join_partitions,
        ctx.dep_manager.clone(),
        join_type == JoinType::NullAwareLeftAnti,
    ));

    let probe_factory = {
        PartitionedJoinProbeProcessorFactory::new_native(
            Arc::clone(&ctx.arena),
            join_type,
            probe_keys.clone(),
            residual_predicate,
            probe_is_left,
            has_equi_keys,
            Arc::clone(&left_chunk_schema),
            Arc::clone(&right_chunk_schema),
            Arc::clone(&join_scope_chunk_schema),
            Arc::clone(&join_state),
        )
        .with_max_buffered_probe_chunks(ctx.operator_buffer_chunks)
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
            join_type,
            residual_predicate.is_some(),
            probe_is_left,
            has_equi_keys,
            build_keys.clone(),
            eq_null_safe.clone(),
            distribution_mode,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_types::SlotId;

    use super::*;
    use crate::exec::expr::ExprNode;
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::project::ProjectNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan, ExternalSinkRequirement};

    #[test]
    fn direct_local_program_builds_values_filter_project_without_exec_plan() {
        let slot = SlotId::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[slot]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, Arc::clone(&chunk_schema)).unwrap();
        let mut arena = ExprArena::default();
        let value_expr = arena.push_typed(ExprNode::SlotId(slot), DataType::Int64);
        let predicate = arena.push_typed(
            ExprNode::Literal(crate::exec::expr::LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let values = ExecNode {
            kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
        };
        let filter = ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(values),
                node_id: 2,
                predicate,
            }),
        };
        let root = ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(filter),
                node_id: 3,
                is_subordinate: false,
                exprs: vec![value_expr],
                expr_slot_ids: vec![slot],
                expr_slot_schemas: None,
                output_indices: None,
                output_chunk_schema: Arc::clone(&chunk_schema),
            }),
        };
        let layout = lp::StaticLayout::try_new_exact(
            Arc::clone(&schema),
            Arc::from([slot]),
            vec![(lp::StaticFieldSchema::new(None, vec![]), None)],
        )
        .unwrap();
        let profile = lp::CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            lp::KernelAbiVersion::CURRENT,
        );
        let (program, bindings) = ExecPlan { arena, root }
            .into_local_program_and_bindings(
                profile,
                BTreeMap::new(),
                vec![ExternalSinkRequirement::Result],
                FragmentSinkProgram::Result.into_static().unwrap(),
            )
            .unwrap();
        let graph = build_native_pipeline_graph_for_local_program_with_runtime_settings(
            &program,
            &bindings,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            1,
            None,
            None,
            crate::exec::expr::agg::test_builtin_execution_function_set(),
            1,
            1,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(graph.pipelines.len(), 1);
        let names = graph.pipelines[0]
            .factories
            .iter()
            .map(|factory| factory.name())
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 3);
        assert!(names[0].contains("ValuesSource"));
        assert!(names[1].contains("FILTER"));
        assert!(names[2].contains("PROJECT"));
    }

    #[test]
    fn shared_exchange_source_is_rejected_before_binding_a_receiver() {
        let slot = SlotId::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let layout = lp::StaticLayout::try_new_exact(
            schema,
            Arc::from([slot]),
            vec![(lp::StaticFieldSchema::new(None, vec![]), None)],
        )
        .unwrap();
        let source_id = lp::ProgramNodeId::new(0);
        let root_id = lp::ProgramNodeId::new(1);
        let program = lp::LocalProgram::try_new(
            vec![
                lp::ProgramNode::new(
                    10,
                    lp::ProgramNodeKind::ExchangeSource {
                        timeout: Duration::from_secs(1),
                        runtime_filters: Vec::new(),
                        hash_partition_exprs: Vec::new(),
                    },
                    layout.clone(),
                ),
                lp::ProgramNode::new(
                    20,
                    lp::ProgramNodeKind::UnionAll {
                        inputs: vec![source_id, source_id],
                    },
                    layout.clone(),
                ),
            ],
            root_id,
            Arc::new(
                lp::ImmutableExpressions::try_new(Vec::new(), false, HashMap::new(), None).unwrap(),
            ),
            lp::CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                layout.identity().unwrap(),
                lp::KernelAbiVersion::CURRENT,
            ),
            lp::BindingRequirements::try_new(vec![lp::BindingRequirement::ExchangeInput {
                node: source_id,
                layout,
            }])
            .unwrap(),
        )
        .unwrap();
        let bindings = LocalRuntimeBindings {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        };
        assert!(
            validate_runtime_binding_shape(&program, &bindings)
                .unwrap_err()
                .contains("reuses runtime-capability node")
        );
    }

    #[test]
    fn shared_values_backing_expands_into_two_independent_sources() {
        let slot = SlotId::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let layout = lp::StaticLayout::try_new_exact(
            schema,
            Arc::from([slot]),
            vec![(lp::StaticFieldSchema::new(None, vec![]), None)],
        )
        .unwrap();
        let values = lp::StaticValues::try_new(batch, layout.clone()).unwrap();
        let values_id = lp::ProgramNodeId::new(0);
        let program = lp::LocalProgram::try_new(
            vec![
                lp::ProgramNode::new(10, lp::ProgramNodeKind::Values { values }, layout.clone()),
                lp::ProgramNode::new(
                    20,
                    lp::ProgramNodeKind::UnionAll {
                        inputs: vec![values_id, values_id],
                    },
                    layout.clone(),
                ),
            ],
            lp::ProgramNodeId::new(1),
            Arc::new(
                lp::ImmutableExpressions::try_new(Vec::new(), false, HashMap::new(), None).unwrap(),
            ),
            lp::CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                layout.identity().unwrap(),
                lp::KernelAbiVersion::CURRENT,
            ),
            lp::BindingRequirements::try_new(Vec::new()).unwrap(),
        )
        .unwrap();
        let bindings = LocalRuntimeBindings {
            scans: BTreeMap::new(),
            writers: BTreeMap::new(),
            finishers: BTreeMap::new(),
        };
        let graph = build_native_pipeline_graph_for_local_program_with_runtime_settings(
            &program,
            &bindings,
            false,
            DependencyManager::new(),
            None,
            ExchangeBindings::default(),
            ScanBindings::default(),
            1,
            None,
            None,
            crate::exec::expr::agg::test_builtin_execution_function_set(),
            1,
            1,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(graph.pipelines.len(), 3);
        assert_eq!(
            graph
                .pipelines
                .iter()
                .flat_map(|pipeline| &pipeline.factories)
                .filter(|factory| factory.name().contains("ValuesSource"))
                .count(),
            2
        );
    }
}
