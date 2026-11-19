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

//! Original typed SQL construction fixture, shared by Execution-only tests.
//! One authored graph; no completed-plan clone, second lowering, or state walker.
use super::contract_lowering::{ContractLoweringError, lower_final_physical_plan};
use crate::analysis::{ExprKind, LiteralValue, OutputColumn, SortItem, TypedExpr};
use crate::binding::{AggregateArgumentSource, SqlFunctionBinding};
use crate::column_id::ColumnId;
use crate::compiler::{SqlAuthoredPhysicalPlan, SqlCompileError, SqlFunctionCatalog};
use crate::planner::payload::{AggregateCall, PlanValuesNode};
use crate::planner::physical::{
    AggMode, AggregateOutputLayout, PhysicalHashAggregateNode, PhysicalPlanKind, PhysicalPlanNode,
    PhysicalPlanStats, PlanSetOpKind, PlannerConfidence, RedistributeMode,
};
use arrow::datatypes::DataType;
use novarocks_functions::FunctionResultType;
use novarocks_type_contract::{
    AggregateStateInterpretation, AggregateStateOrderKey, AggregateStateOrderKeys,
    DecimalOverflowPolicy, PureCompileControl,
};
use std::collections::HashMap;
fn integer(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    }
}
fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

        is_internal: false,
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 1.0,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn values(columns: Vec<OutputColumn>, rows: Vec<Vec<TypedExpr>>) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows,
            columns: columns.clone(),
        }),
        children: Vec::new(),
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn aggregate(
    name: &str,
    arguments: Vec<TypedExpr>,
    order: Vec<SortItem>,
    state_interpretation: Option<AggregateStateInterpretation>,
    child: PhysicalPlanNode,
) -> PhysicalPlanNode {
    let control = crate::compiler::SqlCompileControl::unbounded();
    let request = arguments
        .iter()
        .chain(order.iter().map(|item| &item.expr))
        .map(|arg| {
            crate::analysis::function_argument(
                arg,
                crate::constant::test_constant_policy(),
                &control,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let resolved = crate::functions::builtin_engine_function_catalog()
        .resolve_aggregate_binding(name, arguments.len(), &request, &control)
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("actual Aggregate scalar result");
    };
    let mut output = column(
        91,
        "aggregate_result",
        result.data_type().clone(),
        result.nullable(),
    );
    output.value_type = result.clone();
    let mut binding = SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError);
    if let Some(source) = state_interpretation {
        binding = binding.with_aggregate_state_source(source);
    }
    PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: name.into(),
                source: AggregateArgumentSource::logical_update(arguments, order, binding),
                distinct: false,
                result_type: output.value_type.clone(),
                output_column_id: output.column_id,
            }],
            is_merge: vec![false],
            output_layout: AggregateOutputLayout::new(vec![], vec![output.clone()]),
            output_columns: vec![output.clone()],
            topn_runtime_filter_builds: vec![],
        })),
        children: vec![child],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn local(name: &str, args: Vec<TypedExpr>) -> PhysicalPlanNode {
    let mut result = aggregate(name, args, vec![], None, values(vec![], vec![vec![]]));
    let PhysicalPlanKind::HashAggregate(spec) = &mut result.kind else {
        unreachable!()
    };
    let intermediate = spec.aggregates[0]
        .source
        .binding()
        .selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let mut state = column(
        91,
        "state",
        intermediate.data_type().clone(),
        intermediate.nullable(),
    );
    state.value_type = intermediate;
    spec.mode = AggMode::Local;
    spec.aggregates[0].result_type = state.value_type.clone();
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
    spec.output_columns = vec![state.clone()];
    result.output_columns = vec![state];
    result
}
fn merge(
    name: &str,
    args: Vec<TypedExpr>,
    child: PhysicalPlanNode,
    intermediate: bool,
) -> PhysicalPlanNode {
    let mut result = aggregate(name, args, vec![], None, child);
    let PhysicalPlanKind::HashAggregate(spec) = &mut result.kind else {
        unreachable!()
    };
    spec.mode = if intermediate {
        AggMode::DistinctLocal
    } else {
        AggMode::Global
    };
    spec.is_merge = vec![true];
    if intermediate {
        let ty = spec.aggregates[0]
            .source
            .binding()
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
            .clone();
        let mut state = column(
            91,
            "intermediate_state",
            ty.data_type().clone(),
            ty.nullable(),
        );
        state.value_type = ty;
        spec.aggregates[0].result_type = state.value_type.clone();
        spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
        spec.output_columns = vec![state.clone()];
        result.output_columns = vec![state];
    }
    result
}
fn gather(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let columns = child.output_columns.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: columns.clone(),
        }),
        children: vec![child],
        output_columns: columns,
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn union(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
    let mut result = left.output_columns[0].clone();
    result.column_id = ColumnId(190);
    result.name = "union_state".into();
    let mappings = vec![left.output_columns.clone(), right.output_columns.clone()];
    PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![result.clone()],
            child_output_columns: mappings,
        }),
        children: vec![left, right],
        output_columns: vec![result],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

/// Exact source kinds are explicit test materials. Names are resolved only in
/// this SQL fixture producer, never inside pure CPU or runtime dispatch.
pub fn ordinary_union_source(
    sum_constants: Option<(i64, i64, i64)>,
    intermediate: bool,
    control: &dyn PureCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, SqlCompileError> {
    let (name, left, right, final_args) = match sum_constants {
        None => ("count", vec![], vec![], vec![]),
        Some((a, b, c)) => ("sum", vec![integer(a)], vec![integer(b)], vec![integer(c)]),
    };
    let contributions = union(local(name, left), local(name, right));
    let source = if intermediate {
        merge(
            name,
            final_args.clone(),
            gather(merge(name, final_args, contributions, true)),
            false,
        )
    } else {
        merge(name, final_args, gather(contributions), false)
    };
    let draft = lower_final_physical_plan(
        &source,
        novarocks_physical_plan::PlanVersionId::try_new([41; 16]).unwrap(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        crate::constant::test_constant_policy(),
        control,
    )
    .map_err(|e| match e {
        ContractLoweringError::Control(c) => c.into(),
        e => SqlCompileError::Compilation(e.to_string()),
    })?;
    draft.finish_observed(control).map_err(|e| match e {
        novarocks_physical_plan::PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::Control(cause),
        ) => cause.into(),
        e => SqlCompileError::Compilation(e.to_string()),
    })
}
/// Original independent MIN state channel: producer UnaryMinus can gain NULL,
/// while merge's OWN literal source remains a complete non-null constant.
pub fn ordinary_extrema_constant_state_source_for_test(
    maximum: bool,
    control: &dyn PureCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, SqlCompileError> {
    let name = if maximum { "max" } else { "min" };
    // The actual Int64 syntax value is cast to Int8 before native NEGATE.
    // Its successful NULL then passes through the original widening cast,
    // retaining the complete Int64 domain of the independent CV2/CV3 sources.
    let narrow_input = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(integer(i64::from(i8::MIN))),
            target: DataType::Int8,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
    };
    let narrow_minus = TypedExpr {
        kind: ExprKind::UnaryOp {
            op: crate::analysis::UnOp::Negate,
            expr: Box::new(narrow_input),
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
    };
    let minus = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(narrow_minus),
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    };
    let contributions = union(local(name, vec![minus]), local(name, vec![integer(2)]));
    let source = merge(
        name,
        vec![integer(3)],
        gather(merge(name, vec![integer(3)], contributions, true)),
        false,
    );
    let draft = lower_final_physical_plan(
        &source,
        novarocks_physical_plan::PlanVersionId::try_new([41; 16]).unwrap(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        crate::constant::test_constant_policy(),
        control,
    )
    .map_err(|e| match e {
        ContractLoweringError::Control(c) => c.into(),
        e => SqlCompileError::Compilation(e.to_string()),
    })?;
    draft
        .finish_observed(control)
        .map_err(|e| SqlCompileError::Compilation(e.to_string()))
}
/// A checked typed physical construction fixture, NOT optimizer-produced SQL.
/// One original aggregate update source is moved/borrowed across its phases.
pub fn ordered_array_state_source_for_test(
    intermediate: bool,
    control: &dyn PureCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, SqlCompileError> {
    let narrow_input = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(integer(i64::from(i8::MIN))),
            target: DataType::Int8,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
    };
    let order = SortItem {
        expr: TypedExpr {
            kind: ExprKind::UnaryOp {
                op: crate::analysis::UnOp::Negate,
                expr: Box::new(narrow_input),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        },
        asc: true,
        nulls_first: true,
    };
    // The fixture directly authors a typed call, so it retains the same
    // original ORDER interpretation that normal analysis records.
    let state_interpretation = AggregateStateInterpretation {
        distinct: false,
        order_keys: AggregateStateOrderKeys::from_pure_boxed(Box::new([AggregateStateOrderKey {
            ascending: order.asc,
            nulls_first: order.nulls_first,
        }])),
    };
    let mut partial = aggregate(
        "array_agg",
        vec![integer(7)],
        vec![order],
        Some(state_interpretation),
        values(vec![], vec![vec![]]),
    );
    let result_columns = partial.output_columns.clone();
    let PhysicalPlanKind::HashAggregate(spec) = &mut partial.kind else {
        unreachable!()
    };
    let original = spec.aggregates[0].source.clone();
    let state_type = original
        .binding()
        .selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let mut state = column(
        91,
        "ordered_state",
        state_type.data_type().clone(),
        state_type.nullable(),
    );
    state.value_type = state_type;
    spec.mode = AggMode::Local;
    spec.aggregates[0].result_type = state.value_type.clone();
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
    spec.output_columns = vec![state.clone()];
    partial.output_columns = vec![state.clone()];
    let stage =
        |child: PhysicalPlanNode, output: OutputColumn, stage_mode: AggMode| PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: stage_mode,
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "array_agg".into(),
                    source: original.clone(),
                    distinct: false,
                    result_type: output.value_type.clone(),
                    output_column_id: output.column_id,
                }],
                is_merge: vec![true],
                output_layout: AggregateOutputLayout::new(vec![], vec![output.clone()]),
                output_columns: vec![output.clone()],
                topn_runtime_filter_builds: vec![],
            })),
            children: vec![child],
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
    let child = if intermediate {
        stage(partial, state, AggMode::DistinctLocal)
    } else {
        partial
    };
    let source = stage(gather(child), result_columns[0].clone(), AggMode::Global);
    let draft = lower_final_physical_plan(
        &source,
        novarocks_physical_plan::PlanVersionId::try_new([41; 16]).unwrap(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        crate::constant::test_constant_policy(),
        control,
    )
    .map_err(|e| match e {
        ContractLoweringError::Control(c) => c.into(),
        e => SqlCompileError::Compilation(e.to_string()),
    })?;
    draft.finish_observed(control).map_err(|e| match e {
        novarocks_physical_plan::PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::Control(cause),
        ) => cause.into(),
        e => SqlCompileError::Compilation(e.to_string()),
    })
}

#[cfg(test)]
mod call_dependency_tests;
