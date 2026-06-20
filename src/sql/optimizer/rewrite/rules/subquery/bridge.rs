//! Local bridges between `OptExpr` and `LogicalPlanNode` for subquery rules.
//!
//! The subquery rewrite rules were originally written against `LogicalPlanNode`.
//! This module keeps the test-only reverse bridge used by legacy subquery
//! assertions to materialise `ScalarId` handles back to `TypedExpr`.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, materialize};
use crate::sql::optimizer::scalar_bridge::{
    materialize_aggregate_call, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalApplyNode,
    LogicalAssertOneRowNode, LogicalCTEAnchorNode, LogicalCTEConsumeNode, LogicalCTEProduceNode,
    LogicalDecodeNode, LogicalExceptNode, LogicalFilterNode, LogicalGenerateSeriesNode,
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalIntersectNode, LogicalJoinNode,
    LogicalLimitNode, LogicalPlanNode, LogicalProjectNode, LogicalRepeatNode, LogicalScanNode,
    LogicalSortNode, LogicalTableFunctionNode, LogicalUnionNode, LogicalValuesNode,
    LogicalWindowNode, PlanNodeKind,
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
        Operator::LogicalScan(op) => PlanNodeKind::Scan(LogicalScanNode {
            database: op.database.clone(),
            table: op.table.clone(),
            alias: op.alias.clone(),
            columns: op.columns.clone(),
            predicates: materialize_exprs(arena, &op.predicates),
            required_columns: op.required_columns.clone(),
            dict_columns: op.dict_columns.clone(),
            variant_columns: op.variant_columns.clone(),
            mv_rewritten_from: None,
        }),

        Operator::LogicalFilter(op) => PlanNodeKind::Filter(LogicalFilterNode {
            predicate: materialize(arena, op.predicate),
        }),

        Operator::LogicalProject(op) => PlanNodeKind::Project(LogicalProjectNode {
            items: materialize_project_items(arena, &op.items),
            output_qualifier: op.output_qualifier.clone(),
        }),

        Operator::LogicalAggregate(op) => {
            let group_by = materialize_exprs(arena, &op.group_by);
            let group_by_len = op.group_by.len();
            let aggregates = op
                .aggregates
                .iter()
                .enumerate()
                .map(|(i, a)| {
                    materialize_aggregate_call(arena, a, op.output_columns.get(group_by_len + i))
                })
                .collect();
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by,
                aggregates,
                output_columns: op.output_columns.clone(),
                already_pushed: false,
            })
        }

        Operator::LogicalJoin(op) => PlanNodeKind::Join(LogicalJoinNode {
            join_type: op.join_type,
            condition: op.condition.map(|id| materialize(arena, id)),
        }),

        Operator::LogicalSort(op) => {
            let items = materialize_sort_keys(arena, &op.items);
            let analytic_partition_by = materialize_exprs(arena, &op.analytic_partition_exprs);
            PlanNodeKind::Sort(LogicalSortNode {
                items,
                analytic_partition_by,
                output_columns: vec![],
                offset: None,
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            })
        }

        Operator::LogicalLimit(op) => PlanNodeKind::Limit(LogicalLimitNode {
            limit: op.limit,
            offset: op.offset,
        }),

        Operator::LogicalValues(op) => {
            let rows = op
                .rows
                .iter()
                .map(|row| materialize_exprs(arena, row))
                .collect();
            PlanNodeKind::Values(LogicalValuesNode {
                rows,
                columns: op.columns.clone(),
            })
        }

        Operator::LogicalWindow(op) => {
            let window_exprs =
                materialize_window_exprs(arena, &op.window_exprs, &op.output_columns);
            PlanNodeKind::Window(LogicalWindowNode {
                window_exprs,
                output_columns: op.output_columns.clone(),
            })
        }

        Operator::LogicalAssertOneRow(op) => PlanNodeKind::AssertOneRow(LogicalAssertOneRowNode {
            subquery_text: op.subquery_text.clone(),
        }),

        Operator::LogicalApply(op) => PlanNodeKind::Apply(LogicalApplyNode {
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
            PlanNodeKind::TableFunction(LogicalTableFunctionNode {
                function_name: op.function_name.clone(),
                args,
                output_columns: op.output_columns.clone(),
                alias: op.alias.clone(),
                is_left_join: op.is_left_join,
            })
        }

        Operator::LogicalCTEConsume(op) => PlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
            cte_id: op.cte_id,
            alias: op.alias.clone(),
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalCTEAnchor(op) => {
            PlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: op.cte_id })
        }

        Operator::LogicalCTEProduce(op) => PlanNodeKind::CTEProduce(LogicalCTEProduceNode {
            cte_id: op.cte_id,
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalUnion(op) => PlanNodeKind::Union(LogicalUnionNode {
            all: op.all,
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalIntersect(op) => PlanNodeKind::Intersect(LogicalIntersectNode {
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalExcept(op) => PlanNodeKind::Except(LogicalExceptNode {
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalGenerateSeries(op) => {
            PlanNodeKind::GenerateSeries(LogicalGenerateSeriesNode {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
                output_column_id: op.output_column_id,
            })
        }

        Operator::LogicalRepeat(op) => PlanNodeKind::Repeat(LogicalRepeatNode {
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

        Operator::LogicalDecode(op) => PlanNodeKind::Decode(LogicalDecodeNode {
            mappings: op.mappings.clone(),
            output_columns: op.output_columns.clone(),
        }),

        Operator::LogicalAggregateStateMerge(op) => {
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: op.group_key_names.clone(),
                aggregate_state_names: op.aggregate_state_names.clone(),
                change_op_column: op.change_op_column.clone(),
                output_columns: op.output_columns.clone(),
            })
        }

        Operator::LogicalImvDelta(op) => PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
            is_root: op.is_root,
            action_column: op.action_column,
            branch_scope: op.branch_scope.clone(),
        }),

        Operator::LogicalImvVersion(op) => PlanNodeKind::ImvVersion(LogicalImvVersionNode {
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
