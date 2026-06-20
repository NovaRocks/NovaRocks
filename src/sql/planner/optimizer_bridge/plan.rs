//! Conversion between `LogicalPlanNode` and `OptExpr`.

use crate::sql::analysis::SortItem;
use crate::sql::optimizer::operator::{
    AggregateStateMergeOp, ApplyOp, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
    DecodeOp, ExceptOp, FilterOp, GenerateSeriesOp, ImvDeltaOp, ImvVersionOp, IntersectOp, LimitOp,
    LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, RepeatOp, ScanOp, SortOp,
    TableFunctionOp, UnionOp, ValuesOp, WindowOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::scalar::{
    intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items, intern_typed,
    intern_window_exprs, materialize, materialize_aggregate_calls, materialize_exprs,
    materialize_project_items, materialize_sort_keys, materialize_window_exprs,
};
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalApplyNode,
    LogicalAssertOneRowNode, LogicalCTEAnchorNode, LogicalCTEConsumeNode, LogicalCTEProduceNode,
    LogicalDecodeNode, LogicalExceptNode, LogicalFilterNode, LogicalGenerateSeriesNode,
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalIntersectNode, LogicalJoinNode,
    LogicalLimitNode, LogicalPlanNode, LogicalProjectNode, LogicalRepeatNode, LogicalScanNode,
    LogicalSortNode, LogicalTableFunctionNode, LogicalUnionNode, LogicalValuesNode,
    LogicalWindowNode, PlanNodeKind, validate_logical_plan_stage,
};

/// Bridge 1: convert a `LogicalPlanNode` tree into an `OptExpr` tree, interning
/// all scalars into the provided `ScalarArena`. No Memo groups are minted here.
pub(crate) fn try_logical_plan_to_opt_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> Result<OptExpr, String> {
    validate_logical_plan_stage(plan)?;
    Ok(logical_plan_to_opt_expr_unchecked(plan, scalars))
}

pub(crate) fn logical_plan_to_opt_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> OptExpr {
    try_logical_plan_to_opt_expr(plan, scalars).expect("invalid logical plan stage")
}

fn logical_plan_to_opt_expr_unchecked(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> OptExpr {
    let mut expr = match &plan.kind {
        PlanNodeKind::Scan(node) => {
            for column in &node.columns {
                scalars.remember_source_column_display(
                    column.column_id,
                    node.alias.clone(),
                    column.name.clone(),
                );
            }
            let op = Operator::LogicalScan(ScanOp {
                database: node.database.clone(),
                table: node.table.clone(),
                alias: node.alias.clone(),
                columns: node.columns.clone(),
                predicates: intern_exprs(scalars, &node.predicates),
                required_columns: node.required_columns.clone(),
                dict_columns: node.dict_columns.clone(),
                variant_columns: node.variant_columns.clone(),
                mv_rewritten_from: None,
            });
            OptExpr::leaf(op)
        }

        PlanNodeKind::Filter(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalFilter(FilterOp {
                predicate: intern_typed(scalars, &node.predicate),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Project(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalProject(ProjectOp {
                items: intern_project_items(scalars, &node.items),
                output_qualifier: node.output_qualifier.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Aggregate(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let group_by = intern_exprs(scalars, &node.group_by);
            for (scalar_id, output) in group_by.iter().zip(node.output_columns.iter()) {
                scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
            }
            let aggregates = intern_aggregate_calls(scalars, &node.aggregates);
            let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                group_by,
                aggregates,
                node.output_columns.clone(),
            ));
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Join(node) => {
            let left = logical_plan_to_opt_expr_unchecked(plan.left(), scalars);
            let right = logical_plan_to_opt_expr_unchecked(plan.right(), scalars);
            let op = Operator::LogicalJoin(LogicalJoinOp {
                join_type: node.join_type,
                condition: node
                    .condition
                    .as_ref()
                    .map(|condition| intern_typed(scalars, condition)),
            });
            OptExpr::new(op, vec![left, right])
        }

        PlanNodeKind::Sort(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalSort(SortOp {
                items: intern_sort_items(scalars, &node.items),
                analytic_partition_exprs: intern_exprs(scalars, &node.analytic_partition_by),
                partition_limit: node.partition_limit,
                topn_type: node.topn_type,
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Limit(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalLimit(LimitOp {
                limit: node.limit,
                offset: node.offset,
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Union(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr_unchecked(input, scalars))
                .collect();
            let op = Operator::LogicalUnion(UnionOp {
                all: node.all,
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        PlanNodeKind::Intersect(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr_unchecked(input, scalars))
                .collect();
            let op = Operator::LogicalIntersect(IntersectOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        PlanNodeKind::Except(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| logical_plan_to_opt_expr_unchecked(input, scalars))
                .collect();
            let op = Operator::LogicalExcept(ExceptOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        PlanNodeKind::Values(node) => {
            let op = Operator::LogicalValues(ValuesOp {
                rows: node
                    .rows
                    .iter()
                    .map(|row| intern_exprs(scalars, row))
                    .collect(),
                columns: node.columns.clone(),
            });
            OptExpr::leaf(op)
        }

        PlanNodeKind::GenerateSeries(node) => {
            let op = Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id,
            });
            OptExpr::leaf(op)
        }

        PlanNodeKind::TableFunction(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalTableFunction(TableFunctionOp {
                function_name: node.function_name.clone(),
                args: intern_exprs(scalars, &node.args),
                output_columns: node.output_columns.clone(),
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Window(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalWindow(WindowOp {
                window_exprs: intern_window_exprs(scalars, &node.window_exprs),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Repeat(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: node.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: node.repeat_column_ref_ids.clone(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.clone(),
                grouping_key_aliases: node.grouping_key_aliases.clone(),
                grouping_fn_args: node.grouping_fn_args.clone(),
                grouping_fn_arg_ids: node.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: node.grouping_fn_ids.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::CTEConsume(node) => {
            let op = Operator::LogicalCTEConsume(CTEConsumeOp {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::leaf(op)
        }

        PlanNodeKind::CTEAnchor(node) => {
            let produce = logical_plan_to_opt_expr_unchecked(plan.child(0), scalars);
            let consumer = logical_plan_to_opt_expr_unchecked(plan.child(1), scalars);
            let op = Operator::LogicalCTEAnchor(CTEAnchorOp {
                cte_id: node.cte_id,
            });
            OptExpr::new(op, vec![produce, consumer])
        }

        PlanNodeKind::CTEProduce(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: node.cte_id,
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Decode(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalDecode(DecodeOp {
                mappings: node.mappings.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::AggregateStateMerge(node) => {
            let old_input = logical_plan_to_opt_expr_unchecked(plan.left(), scalars);
            let delta_input = logical_plan_to_opt_expr_unchecked(plan.right(), scalars);
            let op = Operator::LogicalAggregateStateMerge(AggregateStateMergeOp {
                group_key_names: node.group_key_names.clone(),
                aggregate_state_names: node.aggregate_state_names.clone(),
                change_op_column: node.change_op_column.clone(),
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![old_input, delta_input])
        }

        PlanNodeKind::AssertOneRow(node) => {
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalAssertOneRow(AssertOneRowOp {
                subquery_text: node.subquery_text.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::Apply(node) => {
            // Apply is consumed by the subquery/imv rewrite rules BEFORE memo
            // conversion. Building an OptExpr here allows the rewrite rules
            // (subquery/ and imv/ dirs) to operate on OptExpr trees. After
            // rewrite the SubqueryRewrite backstop asserts no Apply remains.
            let outer = logical_plan_to_opt_expr_unchecked(plan.left(), scalars);
            let inner = logical_plan_to_opt_expr_unchecked(plan.right(), scalars);
            let op = Operator::LogicalApply(ApplyOp {
                kind: node.kind,
                subquery_expr: intern_typed(scalars, &node.subquery_expr),
                output_column: node.output_column.clone(),
                inner_output_column_id: node.inner_output_column_id,
                correlation_column_ids: node.correlation_column_ids.clone(),
                correlation_conjuncts: intern_exprs(scalars, &node.correlation_conjuncts),
                residual_predicate: node
                    .residual_predicate
                    .as_ref()
                    .map(|e| intern_typed(scalars, e)),
                need_check_max_rows: node.need_check_max_rows,
                use_semi_anti: node.use_semi_anti,
                uncorrelated_outer_predicate_columns: node
                    .uncorrelated_outer_predicate_columns
                    .clone(),
            });
            OptExpr::new(op, vec![outer, inner])
        }

        PlanNodeKind::ImvDelta(node) => {
            // ImvDelta wraps a child subtree (the base plan being rewritten).
            let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
            let op = Operator::LogicalImvDelta(ImvDeltaOp {
                is_root: node.is_root,
                action_column: node.action_column,
                branch_scope: node.branch_scope.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        PlanNodeKind::ImvVersion(node) => {
            // ImvVersion wraps a child plan (the snapshot scan subtree).
            let op = Operator::LogicalImvVersion(ImvVersionOp {
                version_ref: node.version_ref.clone(),
            });
            if plan.children.is_empty() {
                OptExpr::leaf(op)
            } else {
                let child = logical_plan_to_opt_expr_unchecked(plan.unary_input(), scalars);
                OptExpr::new(op, vec![child])
            }
        }
        PlanNodeKind::TopN(_)
        | PlanNodeKind::Exchange(_)
        | PlanNodeKind::HashAggregate(_)
        | PlanNodeKind::HashJoin(_)
        | PlanNodeKind::NestLoopJoin(_)
        | PlanNodeKind::SetOp(_) => {
            panic!(
                "distributed plan node {} leaked into logical optimizer bridge",
                plan.kind.variant_name()
            );
        }
    };
    expr.required_output_columns = plan.required_output_columns.clone();
    expr
}

/// Bridge 2 (reverse): convert an `OptExpr` tree back into a `LogicalPlanNode`
/// tree, materializing all `ScalarId` values from the provided arena.
///
/// This is used exclusively by the IMV rewrite pipeline, which operates on
/// `OptExpr` internally but produces output that callers (e.g. `engine/mod.rs`)
/// still consume as `LogicalPlanNode`. Only the operator variants that can
/// appear in the IMV rewrite path are handled; the remainder panic because
/// they cannot arise from a well-formed IMV rewrite output.
pub(crate) fn opt_expr_to_logical_plan(expr: OptExpr, arena: &ScalarArena) -> LogicalPlanNode {
    let children: Vec<LogicalPlanNode> = expr
        .children
        .into_iter()
        .map(|c| opt_expr_to_logical_plan(c, arena))
        .collect();
    let kind = match expr.op {
        Operator::LogicalScan(op) => PlanNodeKind::Scan(LogicalScanNode {
            database: op.database,
            table: op.table,
            alias: op.alias,
            columns: op.columns,
            predicates: materialize_exprs(arena, &op.predicates),
            required_columns: op.required_columns,
            dict_columns: op.dict_columns,
            variant_columns: op.variant_columns,
            mv_rewritten_from: None,
        }),
        Operator::LogicalFilter(op) => PlanNodeKind::Filter(LogicalFilterNode {
            predicate: materialize(arena, op.predicate),
        }),
        Operator::LogicalProject(op) => PlanNodeKind::Project(LogicalProjectNode {
            items: materialize_project_items(arena, &op.items),
            output_qualifier: op.output_qualifier,
        }),
        Operator::LogicalAggregate(op) => {
            let group_by = materialize_exprs(arena, &op.group_by);
            let aggregates = materialize_aggregate_calls(
                arena,
                &op.aggregates,
                op.group_by.len(),
                &op.output_columns,
            );
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by,
                aggregates,
                output_columns: op.output_columns,
                already_pushed: false,
            })
        }
        Operator::LogicalJoin(op) => PlanNodeKind::Join(LogicalJoinNode {
            join_type: op.join_type,
            condition: op.condition.map(|id| materialize(arena, id)),
        }),
        Operator::LogicalSort(op) => {
            let items: Vec<SortItem> = materialize_sort_keys(arena, &op.items);
            PlanNodeKind::Sort(LogicalSortNode {
                items,
                analytic_partition_by: materialize_exprs(arena, &op.analytic_partition_exprs),
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
        Operator::LogicalUnion(op) => PlanNodeKind::Union(LogicalUnionNode {
            all: op.all,
            output_columns: op.output_columns,
        }),
        Operator::LogicalValues(op) => PlanNodeKind::Values(LogicalValuesNode {
            rows: op
                .rows
                .iter()
                .map(|row| materialize_exprs(arena, row))
                .collect(),
            columns: op.columns,
        }),
        Operator::LogicalAggregateStateMerge(op) => {
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: op.group_key_names,
                aggregate_state_names: op.aggregate_state_names,
                change_op_column: op.change_op_column,
                output_columns: op.output_columns,
            })
        }
        Operator::LogicalImvDelta(op) => PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
            is_root: op.is_root,
            action_column: op.action_column,
            branch_scope: op.branch_scope,
        }),
        Operator::LogicalImvVersion(op) => PlanNodeKind::ImvVersion(LogicalImvVersionNode {
            version_ref: op.version_ref,
        }),
        Operator::LogicalAssertOneRow(op) => PlanNodeKind::AssertOneRow(LogicalAssertOneRowNode {
            subquery_text: op.subquery_text,
        }),
        Operator::LogicalIntersect(op) => PlanNodeKind::Intersect(LogicalIntersectNode {
            output_columns: op.output_columns,
        }),
        Operator::LogicalExcept(op) => PlanNodeKind::Except(LogicalExceptNode {
            output_columns: op.output_columns,
        }),
        Operator::LogicalGenerateSeries(op) => {
            PlanNodeKind::GenerateSeries(LogicalGenerateSeriesNode {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name,
                alias: op.alias,
                output_column_id: op.output_column_id,
            })
        }
        Operator::LogicalTableFunction(op) => {
            PlanNodeKind::TableFunction(LogicalTableFunctionNode {
                function_name: op.function_name,
                args: materialize_exprs(arena, &op.args),
                output_columns: op.output_columns,
                alias: op.alias,
                is_left_join: op.is_left_join,
            })
        }
        Operator::LogicalWindow(op) => {
            let window_exprs =
                materialize_window_exprs(arena, &op.window_exprs, &op.output_columns);
            PlanNodeKind::Window(LogicalWindowNode {
                window_exprs,
                output_columns: op.output_columns,
            })
        }
        Operator::LogicalRepeat(op) => PlanNodeKind::Repeat(LogicalRepeatNode {
            repeat_column_ref_list: op.repeat_column_ref_list,
            repeat_column_ref_ids: op.repeat_column_ref_ids,
            grouping_ids: op.grouping_ids,
            all_rollup_columns: op.all_rollup_columns,
            all_rollup_column_ids: op.all_rollup_column_ids,
            grouping_key_aliases: op.grouping_key_aliases,
            grouping_fn_args: op.grouping_fn_args,
            grouping_fn_arg_ids: op.grouping_fn_arg_ids,
            grouping_fn_ids: op.grouping_fn_ids,
            virtual_tuple_id: None,
        }),
        Operator::LogicalCTEAnchor(op) => {
            PlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: op.cte_id })
        }
        Operator::LogicalCTEProduce(op) => PlanNodeKind::CTEProduce(LogicalCTEProduceNode {
            cte_id: op.cte_id,
            output_columns: op.output_columns,
        }),
        Operator::LogicalCTEConsume(op) => PlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
            cte_id: op.cte_id,
            alias: op.alias,
            output_columns: op.output_columns,
        }),
        Operator::LogicalDecode(op) => PlanNodeKind::Decode(LogicalDecodeNode {
            mappings: op.mappings,
            output_columns: op.output_columns,
        }),
        Operator::LogicalApply(op) => {
            // Apply is expected to be eliminated by the SubqueryRewrite stage
            // before opt_expr_to_logical_plan is called. If it survives, we
            // still need to materialize it correctly for callers that inspect
            // the plan before memo conversion.
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: op.kind,
                subquery_expr: materialize(arena, op.subquery_expr),
                output_column: op.output_column,
                inner_output_column_id: op.inner_output_column_id,
                correlation_column_ids: op.correlation_column_ids,
                correlation_conjuncts: materialize_exprs(arena, &op.correlation_conjuncts),
                residual_predicate: op.residual_predicate.map(|id| materialize(arena, id)),
                need_check_max_rows: op.need_check_max_rows,
                use_semi_anti: op.use_semi_anti,
                uncorrelated_outer_predicate_columns: op.uncorrelated_outer_predicate_columns,
            })
        }
        // Physical operators should never reach opt_expr_to_logical_plan.
        other => panic!(
            "opt_expr_to_logical_plan: unexpected operator kind {:?} — \
             physical/unknown operators cannot be materialized to LogicalPlanNode",
            other
        ),
    };
    LogicalPlanNode::new(kind, children, expr.required_output_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::cascades_rules::implement::ScanToPhysical;
    use crate::sql::optimizer::memo::{GroupId, Memo};
    use crate::sql::optimizer::memo_copy::opt_expr_to_memo;
    use crate::sql::optimizer::rule::Rule;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        LogicalFilterNode, LogicalScanNode, LogicalUnionNode, LogicalValuesNode, PlanNodeKind,
        ScanVariantColumn,
    };
    use arrow::datatypes::DataType;

    fn logical_plan_to_memo_for_test(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
        let opt_expr = try_logical_plan_to_opt_expr(plan, &mut memo.scalars)
            .expect("logical plan to opt expr");
        opt_expr_to_memo(&opt_expr, memo)
    }

    fn dummy_table_def() -> TableDef {
        TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn dummy_output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn test_output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn values_with_columns(columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: columns,
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn logical_plan_to_opt_expr_rejects_logical_scan_with_mv_rewrite_source() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: Some("mv_t1".to_string()),
            }),
            vec![],
            None,
        );

        let mut scalars = ScalarArena::new();
        let err = try_logical_plan_to_opt_expr(&scan, &mut scalars).unwrap_err();

        assert!(err.contains("Scan.mv_rewritten_from"), "{err}");
    }

    #[test]
    fn logical_plan_to_opt_expr_rejects_logical_sort_with_distributed_fields() {
        let sort = LogicalPlanNode::new(
            PlanNodeKind::Sort(LogicalSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: dummy_output_columns(),
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![values_with_columns(dummy_output_columns())],
            None,
        );

        let mut scalars = ScalarArena::new();
        let err = try_logical_plan_to_opt_expr(&sort, &mut scalars).unwrap_err();

        assert!(err.contains("Sort.output_columns"), "{err}");
    }

    #[test]
    fn logical_plan_to_opt_expr_rejects_logical_repeat_with_virtual_tuple_id() {
        let repeat = LogicalPlanNode::new(
            PlanNodeKind::Repeat(LogicalRepeatNode {
                repeat_column_ref_list: vec![],
                repeat_column_ref_ids: vec![],
                grouping_ids: vec![],
                all_rollup_columns: vec![],
                all_rollup_column_ids: vec![],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
                virtual_tuple_id: Some(7),
            }),
            vec![values_with_columns(dummy_output_columns())],
            None,
        );

        let mut scalars = ScalarArena::new();
        let err = try_logical_plan_to_opt_expr(&repeat, &mut scalars).unwrap_err();

        assert!(err.contains("Repeat.virtual_tuple_id"), "{err}");
    }

    #[test]
    fn set_op_output_columns_survive_memo_stats_with_duplicate_names() {
        let target = vec![test_output_column(20, "dup"), test_output_column(21, "dup")];
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: target.clone(),
            }),
            vec![
                values_with_columns(vec![
                    test_output_column(10, "dup"),
                    test_output_column(11, "dup"),
                ]),
                values_with_columns(vec![
                    test_output_column(12, "dup"),
                    test_output_column(13, "dup"),
                ]),
            ],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&plan, &mut memo);
        crate::sql::optimizer::stats::derive_group_statistics(
            &mut memo,
            &std::collections::HashMap::new(),
        );

        let output_columns = &memo.groups[root]
            .logical_props
            .as_ref()
            .expect("set-op root should have logical properties")
            .output_columns;
        assert_eq!(output_columns.len(), target.len());
        assert_eq!(output_columns[0].name, "dup");
        assert_eq!(output_columns[1].name, "dup");
        assert_eq!(output_columns[0].column_id, target[0].column_id);
        assert_eq!(output_columns[1].column_id, target[1].column_id);

        let root_expr = memo.groups[root]
            .logical_exprs
            .first()
            .expect("root logical expression");
        let Operator::LogicalUnion(op) = &root_expr.op else {
            panic!("expected logical union");
        };
        assert_eq!(op.child_output_columns.len(), 2);
        assert_eq!(op.child_output_columns[0][0].column_id, ColumnId(10));
        assert_eq!(op.child_output_columns[0][1].column_id, ColumnId(11));
        assert_eq!(op.child_output_columns[1][0].column_id, ColumnId(12));
        assert_eq!(op.child_output_columns[1][1].column_id, ColumnId(13));
    }

    #[test]
    fn test_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&scan, &mut memo);

        assert_eq!(gid, 0);
        assert_eq!(memo.groups.len(), 1);
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(memo.groups[0].physical_exprs.is_empty());
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());
    }

    #[test]
    fn variant_path_scan_descriptor_survives_physical_conversion() {
        let source_column_id = ColumnId::new_for_test(100);
        let synthetic_column_id = ColumnId::new_for_test(101);
        let variant_descriptor = ScanVariantColumn {
            source_column_id,
            source_column: "payload".to_string(),
            synthetic_column_id,
            synthetic_column: "__nr_var_payload_0".to_string(),
            canonical_path: "$.user.id".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        };

        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: source_column_id,
                        name: "payload".to_string(),
                        data_type: DataType::LargeBinary,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: synthetic_column_id,
                        name: "__nr_var_payload_0".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: true,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![variant_descriptor.clone()],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&scan, &mut memo);
        let logical_expr = memo.groups[gid].logical_exprs[0].clone();

        let physical = ScanToPhysical.apply(&logical_expr, &mut memo);

        assert_eq!(physical.len(), 1);
        let Operator::PhysicalScan(scan) = &physical[0].op else {
            panic!("expected PhysicalScan");
        };
        assert_eq!(scan.variant_columns.len(), 1);
        let actual = &scan.variant_columns[0];
        assert_eq!(actual.source_column_id, variant_descriptor.source_column_id);
        assert_eq!(actual.source_column, variant_descriptor.source_column);
        assert_eq!(
            actual.synthetic_column_id,
            variant_descriptor.synthetic_column_id
        );
        assert_eq!(actual.synthetic_column, variant_descriptor.synthetic_column);
        assert_eq!(actual.canonical_path, variant_descriptor.canonical_path);
        assert_eq!(actual.requested_type, variant_descriptor.requested_type);
        assert_eq!(actual.strict, variant_descriptor.strict);
    }

    #[test]
    fn test_filter_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };

        let filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: predicate,
            }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&filter, &mut memo);

        // Should produce 2 groups: Scan (group 0) and Filter (group 1).
        assert_eq!(memo.groups.len(), 2);
        assert_eq!(gid, 1);

        // Group 0: Scan, no children.
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());

        // Group 1: Filter, child = group 0.
        assert_eq!(memo.groups[1].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[1].logical_exprs[0].op,
            Operator::LogicalFilter(_)
        ));
        assert_eq!(memo.groups[1].logical_exprs[0].children, vec![0]);
    }

    #[test]
    fn test_cte_anchor_to_memo() {
        let scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let produce = LogicalPlanNode::new(
            PlanNodeKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: 7,
                output_columns: dummy_output_columns(),
            }),
            vec![scan.clone()],
            None,
        );

        let consume = LogicalPlanNode::new(
            PlanNodeKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: 7,
                alias: "t".to_string(),
                output_columns: dummy_output_columns(),
            }),
            vec![],
            None,
        );

        let anchor = LogicalPlanNode::new(
            PlanNodeKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: 7 }),
            vec![produce, consume],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&anchor, &mut memo);

        assert_eq!(gid, 3);
        assert!(matches!(
            memo.groups[3].logical_exprs[0].op,
            Operator::LogicalCTEAnchor(_)
        ));
        assert_eq!(memo.groups[3].logical_exprs[0].children, vec![1, 2]);
    }
}
