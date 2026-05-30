//! PruneAggregateColumns — Phase 2 rule for Aggregate nodes.
//!
//! **Currently a no-op.**
//!
//! `AggregateNode.output_columns` is built by `split_projection_for_aggregate`
//! in SELECT order (1:1 with the SELECT list), NOT in `[group_by ++ aggregates]`
//! order.  The `aggregates` list is extracted separately from the projection
//! expressions and has NO positional correspondence to `output_columns`.
//! Indexing `output_columns[group_by.len() + i]` to find the output id of
//! `aggregates[i]` is therefore incorrect and can panic or silently drop the
//! wrong aggregate.
//!
//! Per-aggregate output pruning (Gap 5) requires an explicit `output_column_id`
//! field on `AggregateCall` so that each aggregate result can be addressed by
//! id independently of `output_columns` position.  Until that is implemented,
//! this rule returns `Unchanged` unconditionally.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneAggregateColumns;

impl LogicalRewriteRule for PruneAggregateColumns {
    fn name(&self) -> &'static str {
        "PruneAggregateColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Aggregate(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // No-op: see module-level doc comment.
        //
        // Per-aggregate output pruning (Gap 5) requires an explicit
        // output_column_id on AggregateCall.  Until that is added, this rule
        // always returns Unchanged to avoid incorrect positional indexing into
        // output_columns.
        let _ = plan; // suppress unused-variable warning
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref_expr(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn dummy_input() -> LogicalPlan {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "x".to_string(),
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
        };
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(99),
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    // -----------------------------------------------------------------------
    // Bug A regression: PruneAggregateColumns must be a no-op.
    //
    // AggregateNode.output_columns is SELECT-ordered (built by
    // split_projection_for_aggregate), NOT [group_by ++ aggregates].
    // Indexing output_columns[group_by.len() + i] to find aggregates[i]'s
    // output id is incorrect and can panic or drop the wrong aggregate.
    // Until Gap 5 (per-aggregate output_column_id) is implemented, the rule
    // must always return Unchanged regardless of what required_output_columns
    // contains.
    // -----------------------------------------------------------------------

    /// Rule is a no-op even when needed contains only some aggregate output ids.
    /// Previously this would have returned Changed and incorrectly dropped avg.
    #[test]
    fn prune_aggregate_is_noop_regardless_of_needed_set() {
        // output_columns is SELECT-ordered: [count_oc@301, sum_oc@302]
        // group_by = [y@1],  aggregates = [count, sum(x)]
        // This layout does NOT match [group_by ++ aggregates].
        // The old code would have tried output_columns[1+0]=count_oc and
        // output_columns[1+1]=sum_oc but those are wrong positions given the
        // SELECT-ordered layout — here they happen to line up by accident, but
        // in a query like SELECT count(*), sum(x) GROUP BY y the positions
        // would be output_columns[0]=count, [1]=sum, group_by.len()=1, so
        // output_columns[1+0]=sum and output_columns[1+1] would panic.
        //
        // Regardless: the rule must be Unchanged.
        let id_y = ColumnId::new_for_test(1);
        let id_x = ColumnId::new_for_test(10);
        let id_count_oc = ColumnId::new_for_test(301);
        let id_sum_oc = ColumnId::new_for_test(302);

        let mut needed = HashSet::new();
        needed.insert(id_count_oc); // only count needed, not sum

        let node = AggregateNode {
            input: Box::new(dummy_input()),
            group_by: vec![col_ref_expr(id_y, "y")],
            aggregates: vec![
                AggregateCall {
                    name: "count".to_string(),
                    args: vec![],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                },
                AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref_expr(id_x, "x")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                },
            ],
            // SELECT-ordered output_columns: [count_oc, sum_oc]
            // group_by key y is NOT in output_columns (it's in group_by only).
            output_columns: vec![
                make_output_column(id_count_oc, "count"),
                make_output_column(id_sum_oc, "sum_x"),
            ],
            already_pushed: false,
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Aggregate(node);
        let rule = PruneAggregateColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneAggregateColumns must be a no-op (Gap 5 not yet implemented); got {result:?}"
        );
    }

    /// Rule is also a no-op when required_output_columns is None (untagged).
    #[test]
    fn prune_aggregate_noop_when_required_output_columns_is_none() {
        let id_k = ColumnId::new_for_test(1);
        let id_sum = ColumnId::new_for_test(201);
        let id_a = ColumnId::new_for_test(10);

        let node = AggregateNode {
            input: Box::new(dummy_input()),
            group_by: vec![col_ref_expr(id_k, "k")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref_expr(id_a, "a")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![
                make_output_column(id_k, "k"),
                make_output_column(id_sum, "sum_a"),
            ],
            already_pushed: false,
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::Aggregate(node);
        let rule = PruneAggregateColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }
}
