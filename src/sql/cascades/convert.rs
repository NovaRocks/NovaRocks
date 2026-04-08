//! Converts a `LogicalPlan` tree into Memo groups.

use super::memo::{GroupId, MExpr, Memo};
use super::operator::{
    LogicalAggregateOp, LogicalCTEAnchorOp, LogicalCTEConsumeOp, LogicalCTEProduceOp,
    LogicalExceptOp, LogicalFilterOp, LogicalGenerateSeriesOp, LogicalIntersectOp, LogicalJoinOp,
    LogicalLimitOp, LogicalProjectOp, LogicalRepeatOp, LogicalScanOp, LogicalSortOp,
    LogicalSubqueryAliasOp, LogicalUnionOp, LogicalValuesOp, LogicalWindowOp, Operator,
};
use crate::sql::plan::LogicalPlan;

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
            let op = Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: node.group_by.clone(),
                aggregates: node.aggregates.clone(),
                output_columns: node.output_columns.clone(),
            });
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
                join_type: node.join_type.clone(),
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
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalUnion(LogicalUnionOp { all: node.all });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children,
            };
            memo.new_group(expr)
        }

        LogicalPlan::Intersect(node) => {
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalIntersect(LogicalIntersectOp);
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children,
            };
            memo.new_group(expr)
        }

        LogicalPlan::Except(node) => {
            let children: Vec<GroupId> = node
                .inputs
                .iter()
                .map(|input| logical_plan_to_memo(input, memo))
                .collect();
            let op = Operator::LogicalExcept(LogicalExceptOp);
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
            });
            let expr = MExpr {
                id: memo.next_expr_id(),
                op,
                children: vec![],
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

        LogicalPlan::SubqueryAlias(node) => {
            let child = logical_plan_to_memo(&node.input, memo);
            let op = Operator::LogicalSubqueryAlias(LogicalSubqueryAliasOp {
                alias: node.alias.clone(),
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
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                grouping_fn_args: node.grouping_fn_args.clone(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::plan::{FilterNode, ScanNode};
    use arrow::datatypes::DataType;
    use std::path::PathBuf;

    fn dummy_table_def() -> TableDef {
        TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            storage: TableStorage::LocalParquetFile {
                path: PathBuf::from("/tmp/t1.parquet"),
            },
        }
    }

    fn dummy_output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]
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
        });

        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate,
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
        });

        let produce = LogicalPlan::CTEProduce(crate::sql::plan::CTEProduceNode {
            cte_id: 7,
            input: Box::new(scan.clone()),
            output_columns: dummy_output_columns(),
        });

        let consume = LogicalPlan::CTEConsume(crate::sql::plan::CTEConsumeNode {
            cte_id: 7,
            alias: "t".to_string(),
            output_columns: dummy_output_columns(),
        });

        let anchor = LogicalPlan::CTEAnchor(crate::sql::plan::CTEAnchorNode {
            cte_id: 7,
            produce: Box::new(produce),
            consumer: Box::new(consume),
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
