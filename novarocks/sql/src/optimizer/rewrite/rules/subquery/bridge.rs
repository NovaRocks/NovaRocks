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

//! Local bridges between `OptExpr` and `LogicalPlanNode` for subquery rules.
//!
//! The subquery rewrite rules were originally written against `LogicalPlanNode`.
//! This module keeps the test-only reverse bridge used by legacy subquery
//! assertions to materialise `ScalarId` handles back to `TypedExpr`.

use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::scalar::ScalarArena;
use crate::planner::logical::{
    LogicalAggregateNode, LogicalApplyNode, LogicalExceptNode, LogicalImvDeltaNode,
    LogicalImvVersionNode, LogicalIntersectNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
    LogicalUnionNode,
};
use crate::planner::optimizer_bridge::scalar::materialize;
use crate::planner::optimizer_bridge::scalar::{
    materialize_aggregate_call, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};
use crate::planner::payload::{
    PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};

/// Materialise an `OptExpr` subtree into a `LogicalPlanNode`, converting all
/// `ScalarId` handles back to `TypedExpr` using the arena.
///
/// This covers the operator variants that can appear inside Apply subtrees
/// during the SubqueryRewrite stage.
pub(super) fn opt_expr_to_plan(expr: &OptExpr, arena: &ScalarArena) -> LogicalPlanNode {
    let children: Vec<LogicalPlanNode> = expr
        .children
        .iter()
        .map(|c| opt_expr_to_plan(c, arena))
        .collect();

    let kind = match &expr.op {
        Operator::LogicalScan(op) => LogicalPlanKind::Scan(PlanScanNode {
            database: op.database.clone(),
            table: op.table.clone(),
            alias: op.alias.clone(),
            columns: op.columns.clone(),
            predicates: materialize_exprs(arena, &op.predicates),
            required_columns: op.required_columns.clone(),
            variant_columns: op.variant_columns.clone(),
            mv_rewritten_from: None,
        }),

        Operator::LogicalFilter(op) => LogicalPlanKind::Filter(PlanFilterNode {
            predicate: materialize(arena, op.predicate),
        }),

        Operator::LogicalProject(op) => LogicalPlanKind::Project(PlanProjectNode {
            items: materialize_project_items(arena, &op.items),
            output_qualifier: op.output_qualifier.clone(),
        }),

        Operator::LogicalAggregate(op) => {
            let group_by = materialize_exprs(arena, &op.group_by);
            let aggregates = op
                .aggregates
                .iter()
                .map(|a| materialize_aggregate_call(arena, a, &op.output_layout))
                .collect();
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by,
                aggregates,
                output_columns: op.output_layout.full_output_columns(),
                already_pushed: false,
            })
        }

        Operator::LogicalJoin(op) => LogicalPlanKind::Join(LogicalJoinNode {
            join_type: op.join_type,
            condition: op.condition.map(|id| materialize(arena, id)),
        }),

        Operator::LogicalSort(op) => {
            let items = materialize_sort_keys(arena, &op.items);
            let analytic_partition_by = materialize_exprs(arena, &op.analytic_partition_exprs);
            LogicalPlanKind::Sort(PlanSortNode {
                items,
                analytic_partition_by,
                output_columns: vec![],
                offset: None,
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            })
        }

        Operator::LogicalLimit(op) => LogicalPlanKind::Limit(PlanLimitNode {
            limit: op.limit,
            offset: op.offset,
        }),

        Operator::LogicalValues(op) => {
            let rows = op
                .rows
                .iter()
                .map(|row| materialize_exprs(arena, row))
                .collect();
            LogicalPlanKind::Values(PlanValuesNode {
                rows,
                columns: op.columns.clone(),
            })
        }

        Operator::LogicalWindow(op) => {
            let window_exprs =
                materialize_window_exprs(arena, &op.window_exprs, &op.output_columns);
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs,
                output_columns: op.output_columns.clone(),
            })
        }

        Operator::LogicalAssertOneRow(op) => LogicalPlanKind::AssertOneRow(
            PlanAssertOneRowNode::global_at_most_one(op.subquery_text.clone()),
        ),

        Operator::LogicalApply(op) => LogicalPlanKind::Apply(LogicalApplyNode {
            kind: op.kind,
            subquery_expr: materialize(arena, op.subquery_expr),
            output_column: op.output_column.clone(),
            inner_output_column_id: op.inner_output_column_id,
            correlation_column_ids: op.correlation_column_ids.clone(),
            correlation_conjuncts: materialize_exprs(arena, &op.correlation_conjuncts),
            residual_predicate: op.residual_predicate.map(|id| materialize(arena, id)),
            need_check_max_rows: op.need_check_max_rows,
            use_semi_anti: op.use_semi_anti,
            uncorrelated_outer_predicate_columns: op.uncorrelated_outer_predicate_columns.clone(),
        }),

        Operator::LogicalTableFunction(op) => {
            let args = materialize_exprs(arena, &op.args);
            LogicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: op.function_name.clone(),
                args,
                output_columns: op.output_columns.clone(),
                alias: op.alias.clone(),
                is_left_join: op.is_left_join,
            })
        }

        Operator::LogicalCTEConsume(op) => LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id: op.cte_id,
            alias: op.alias.clone(),
            output_columns: op.output_columns.clone(),
            producer_column_ids: op.producer_column_ids.clone(),
        }),

        Operator::LogicalCTEAnchor(op) => {
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: op.cte_id })
        }

        Operator::LogicalCTEProduce(op) => LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id: op.cte_id,
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalUnion(op) => LogicalPlanKind::Union(LogicalUnionNode {
            all: op.all,
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalIntersect(op) => LogicalPlanKind::Intersect(LogicalIntersectNode {
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalExcept(op) => LogicalPlanKind::Except(LogicalExceptNode {
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalGenerateSeries(op) => {
            LogicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
                output_column_id: op.output_column_id,
            })
        }

        Operator::LogicalRepeat(op) => LogicalPlanKind::Repeat(PlanRepeatNode {
            repeat_column_ref_list: op.repeat_column_ref_list.clone(),
            repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
            grouping_ids: op.grouping_ids.clone(),
            all_rollup_columns: op.all_rollup_columns.clone(),
            all_rollup_column_ids: op.all_rollup_column_ids.clone(),
            grouping_key_aliases: op.grouping_key_aliases.clone(),
            grouping_fn_args: op.grouping_fn_args.clone(),
            grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
            grouping_fn_ids: op.grouping_fn_ids.clone(),
            virtual_tuple_id: None,
        }),

        Operator::LogicalImvDelta(op) => LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
            is_root: op.is_root,
            action_column: op.action_column,
            branch_scope: op.branch_scope.clone(),
        }),

        Operator::LogicalImvVersion(op) => LogicalPlanKind::ImvVersion(LogicalImvVersionNode {
            version_ref: op.version_ref.clone(),
        }),

        op => panic!(
            "opt_expr_to_plan: unexpected operator variant in Apply subtree: {:?}",
            std::mem::discriminant(op)
        ),
    };

    let mut plan = LogicalPlanNode::new(kind, children, None);
    plan.required_output_columns = expr.required_output_columns.clone();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{
        AggregateOutputLayout, LogicalAggregateOp, ScalarAggregateSpec, ValuesOp,
    };
    use crate::optimizer::scalar::ScalarNode;
    use crate::planner::optimizer_bridge::logical::to_optimizer_expr;
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    #[test]
    fn aggregate_hidden_group_layout_round_trips_through_subquery_bridge() {
        let mut arena = ScalarArena::new();
        let group_output = output_column(1, "k");
        let value_output = output_column(2, "v");
        let sum_output = output_column(3, "sum_v");
        let group = arena.intern(
            ScalarNode::ColumnRef(group_output.column_id),
            DataType::Int64,
            false,
        );
        let value = arena.intern(
            ScalarNode::ColumnRef(value_output.column_id),
            DataType::Int64,
            false,
        );
        let aggregate = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![group],
                vec![ScalarAggregateSpec {
                    output_column_id: sum_output.column_id,
                    name: "sum".to_string(),
                    args: vec![value],
                    distinct: false,
                    order_by: vec![],
                    resolved: crate::functions::test_resolved_aggregate(
                        "sum",
                        &[DataType::Int64],
                        false,
                    ),
                }],
                AggregateOutputLayout::new(vec![group_output.clone()], vec![sum_output.clone()]),
                vec![sum_output.clone()],
            )),
            vec![OptExpr::leaf(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![group_output.clone(), value_output],
            }))],
        );

        let plan = opt_expr_to_plan(&aggregate, &arena);
        let mut round_trip_arena = ScalarArena::new();
        let round_tripped = to_optimizer_expr(&plan, &mut round_trip_arena);
        let Operator::LogicalAggregate(round_tripped_agg) = round_tripped.op else {
            panic!("expected LogicalAggregate after round trip");
        };
        assert_eq!(
            round_tripped_agg
                .output_layout
                .group_key_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![group_output.column_id]
        );
        assert_eq!(
            round_tripped_agg
                .output_layout
                .aggregate_columns
                .iter()
                .map(|output| output.column_id)
                .collect::<Vec<_>>(),
            vec![sum_output.column_id]
        );
    }
}
