//! Converts a `LogicalPlan` tree into Memo groups.

use super::memo::{GroupId, MExpr, Memo};
use super::operator::{
    AggregateStateMergeOp, LogicalAggregateOp, LogicalAssertOneRowOp, LogicalCTEAnchorOp,
    LogicalCTEConsumeOp, LogicalCTEProduceOp, LogicalDecodeOp, LogicalExceptOp, LogicalFilterOp,
    LogicalGenerateSeriesOp, LogicalIntersectOp, LogicalJoinOp, LogicalLimitOp, LogicalProjectOp,
    LogicalRepeatOp, LogicalScanOp, LogicalSortOp, LogicalTableFunctionOp, LogicalUnionOp,
    LogicalValuesOp, LogicalWindowOp, Operator,
};
use crate::sql::planner::plan::LogicalPlan;

/// Recursively convert a `LogicalPlan` tree into Memo groups.
///
/// Each plan node becomes a new Group containing a single logical MExpr.
/// Child plan references become child `GroupId`s.
pub(crate) fn logical_plan_to_memo(plan: &LogicalPlan, memo: &mut Memo) -> GroupId {
    match plan {
        LogicalPlan::Scan(node) => {
            let op = Operator::LogicalScan(LogicalScanOp {
                database: node.database.clone(),
                table: node.table.clone(),
                alias: node.alias.clone(),
                columns: node.columns.clone(),
                predicates: node.predicates.clone(),
                required_columns: node.required_columns.clone(),
                dict_columns: node.dict_columns.clone(),
                mv_rewritten_from: None,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Filter(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalFilter(LogicalFilterOp {
                predicate: node.predicate.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Project(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalProject(LogicalProjectOp {
                items: node.items.clone(),
                output_qualifier: node.output_qualifier.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Aggregate(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                node.group_by.clone(),
                node.aggregates.clone(),
                node.output_columns.clone(),
            ));
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Join(node) => {
            let left = logical_plan_to_memo(&node.left, memo);
            let right = logical_plan_to_memo(&node.right, memo);
            let op = Operator::LogicalJoin(LogicalJoinOp {
                join_type: node.join_type,
                condition: node.condition.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![left, right],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Sort(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalSort(LogicalSortOp {
                items: node.items.clone(),
                analytic_partition_exprs: node.analytic_partition_by.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Limit(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalLimit(LogicalLimitOp {
                limit: node.limit,
                offset: node.offset,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Union(node) => {
            let child_output_columns = node
                .inputs
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalUnion(LogicalUnionOp {
                all: node.all,
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children,
            };
            memo.new_group(expr)
        }

        LogicalPlan::Intersect(node) => {
            let child_output_columns = node
                .inputs
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalIntersect(LogicalIntersectOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children,
            };
            memo.new_group(expr)
        }

        LogicalPlan::Except(node) => {
            let child_output_columns = node
                .inputs
                .iter()
                .map(|input| crate::sql::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalExcept(LogicalExceptOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children,
            };
            memo.new_group(expr)
        }

        LogicalPlan::Values(node) => {
            let op = Operator::LogicalValues(LogicalValuesOp {
                rows: node.rows.clone(),
                columns: node.columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![],
            };
            memo.new_group(expr)
        }

        LogicalPlan::GenerateSeries(node) => {
            let op = Operator::LogicalGenerateSeries(LogicalGenerateSeriesOp {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![],
            };
            memo.new_group(expr)
        }

        LogicalPlan::TableFunction(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalTableFunction(LogicalTableFunctionOp {
                function_name: node.function_name.clone(),
                args: node.args.clone(),
                output_columns: node.output_columns.clone(),
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Window(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalWindow(LogicalWindowOp {
                window_exprs: node.window_exprs.clone(),
                output_columns: node.output_columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::Repeat(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalRepeat(LogicalRepeatOp {
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
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::CTEConsume(node) => {
            let op = Operator::LogicalCTEConsume(LogicalCTEConsumeOp {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: node.output_columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![],
            };
            memo.new_group(expr)
        }

        LogicalPlan::CTEAnchor(node) => {
            let produce = logical_plan_to_memo(&node.produce, memo);
            let consumer = logical_plan_to_memo(&node.consumer, memo);
            let expr = MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalCTEAnchor(LogicalCTEAnchorOp {
                    cte_id: node.cte_id,
                }),
                children: vec![produce, consumer],
            };
            memo.new_group(expr)
        }

        LogicalPlan::CTEProduce(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalCTEProduce(LogicalCTEProduceOp {
                cte_id: node.cte_id,
                output_columns: node.output_columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            let group_id = memo.new_group(expr);
            // Register the CTEProduce group so CTEConsume can look up its stats.
            memo.cte_produce_groups.insert(node.cte_id, group_id);
            group_id
        }

        LogicalPlan::Decode(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalDecode(LogicalDecodeOp {
                mappings: node.mappings.clone(),
                output_columns: node.output_columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }

        LogicalPlan::AggregateStateMerge(node) => {
            let old_input = logical_plan_to_memo(&node.old_input, memo);
            let delta_input = logical_plan_to_memo(&node.delta_input, memo);
            let op = Operator::LogicalAggregateStateMerge(AggregateStateMergeOp {
                group_key_names: node.group_key_names.clone(),
                aggregate_state_names: node.aggregate_state_names.clone(),
                change_op_column: node.change_op_column.clone(),
                output_columns: node.output_columns.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![old_input, delta_input],
            };
            memo.new_group(expr)
        }

        LogicalPlan::AssertOneRow(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalAssertOneRow(LogicalAssertOneRowOp {
                subquery_text: node.subquery_text.clone(),
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![child],
            };
            memo.new_group(expr)
        }
        LogicalPlan::Apply(_) => {
            // Defence in depth: the SubqueryRewrite stage's ApplyException
            // rule and the optimize() residual-Apply backstop eliminate every
            // Apply before this point. Reaching here means a planner bug, so
            // fail loudly rather than mis-optimize.
            panic!(
                "apply operator must be eliminated by the SubqueryRewrite stage before memo conversion"
            );
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{FilterNode, ScanNode, UnionNode, ValuesNode};
    use arrow::datatypes::DataType;
    use std::path::PathBuf;

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

    fn values_with_columns(columns: Vec<OutputColumn>) -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns,
            required_output_columns: None,
        })
    }

    #[test]
    fn set_op_output_columns_survive_memo_stats_with_duplicate_names() {
        let target = vec![test_output_column(20, "dup"), test_output_column(21, "dup")];
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![
                values_with_columns(vec![
                    test_output_column(10, "dup"),
                    test_output_column(11, "dup"),
                ]),
                values_with_columns(vec![
                    test_output_column(12, "dup"),
                    test_output_column(13, "dup"),
                ]),
            ],
            all: true,
            output_columns: target.clone(),
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let root = logical_plan_to_memo(&plan, &mut memo);
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
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: dummy_table_def(),
            alias: None,
            columns: dummy_output_columns(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&scan, &mut memo);

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
    fn test_filter_scan_to_memo() {
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: dummy_table_def(),
            alias: None,
            columns: dummy_output_columns(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });

        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&filter, &mut memo);

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
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: dummy_table_def(),
            alias: None,
            columns: dummy_output_columns(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        });

        let produce = LogicalPlan::CTEProduce(crate::sql::planner::plan::CTEProduceNode {
            cte_id: 7,
            input: Box::new(scan.clone()),
            output_columns: dummy_output_columns(),
            required_output_columns: None,
        });

        let consume = LogicalPlan::CTEConsume(crate::sql::planner::plan::CTEConsumeNode {
            cte_id: 7,
            alias: "t".to_string(),
            output_columns: dummy_output_columns(),
            required_output_columns: None,
        });

        let anchor = LogicalPlan::CTEAnchor(crate::sql::planner::plan::CTEAnchorNode {
            cte_id: 7,
            produce: Box::new(produce),
            consumer: Box::new(consume),
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo(&anchor, &mut memo);

        assert_eq!(gid, 3);
        assert!(matches!(
            memo.groups[3].logical_exprs[0].op,
            Operator::LogicalCTEAnchor(_)
        ));
        assert_eq!(memo.groups[3].logical_exprs[0].children, vec![1, 2]);
    }
}
