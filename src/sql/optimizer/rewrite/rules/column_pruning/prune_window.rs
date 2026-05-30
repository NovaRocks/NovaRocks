//! PruneWindowColumns — Phase 2 rule for Window nodes.
//!
//! ## Gap-5: Window output pruning is intentionally deferred (NO-OP)
//!
//! `WindowNode.output_columns` is built by `build_window_and_project` in the
//! planner using **fresh factory-allocated ColumnIds** (`factory.create(...)`),
//! one per item in the original SELECT projection.  These fresh ids are
//! distinct from the ColumnIds carried by the child scan/project items, and
//! the name-based `output_name` ↔ `window_exprs[i].output_name` matching used
//! by the old pruning strategy is fragile: in some query shapes the name
//! match over-prunes and leaves `window_exprs` empty, which codegen rejects
//! with "empty window_exprs".
//!
//! Safe pruning of Window output_columns requires a correct ColumnId contract
//! between the Window node's `output_columns` entries and the window function
//! output slots — equivalent to the `output_column_id` fix applied to
//! `ProjectItem` (Gap 2).  Until that contract is established, this rule is a
//! documented NO-OP.
//!
//! `tag_window` already passes `None` (keep-all) to the child, so the child
//! keeps all its input columns as well.  This is consistent and safe.
//!
//! Follow-up: re-enable pruning here once `WindowExpr` carries a stable
//! `output_column_id` that the parent can address without name matching.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneWindowColumns;

impl LogicalRewriteRule for PruneWindowColumns {
    fn name(&self) -> &'static str {
        "PruneWindowColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Window(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // NO-OP: Window output pruning is deferred (Gap-5).
        // Window.output_columns use fresh planner-allocated ColumnIds that
        // don't correlate cleanly with the parent's required_output_columns set.
        // Name-based pruning (matching output_column.name to window_expr.output_name)
        // over-prunes in some query shapes, leaving window_exprs empty and
        // causing a codegen error. Return Unchanged always until a proper
        // ColumnId contract is established for WindowExpr outputs.
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

    fn make_window_expr(output_name: &str) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: output_name.to_string(),
            ignore_nulls: false,
        }
    }

    fn dummy_input() -> Box<LogicalPlan> {
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
        Box::new(LogicalPlan::Scan(ScanNode {
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
        }))
    }

    /// PruneWindowColumns is always a NO-OP regardless of required_output_columns.
    /// Window output_columns use fresh planner-allocated ColumnIds (Gap-5 deferred).
    #[test]
    fn prune_window_is_always_noop() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn1 = ColumnId::new_for_test(101);
        let id_rn2 = ColumnId::new_for_test(102);

        // Only id_a and id_rn1 are needed — but the rule must still be a NO-OP.
        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_rn1);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn1"), make_window_expr("rn2")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_rn1, "rn1"),
                make_output_column(id_rn2, "rn2"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneWindowColumns must always return Unchanged (Gap-5 no-op)"
        );
    }

    #[test]
    fn prune_window_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_rn, "rn"),
            ],
            required_output_columns: None,
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    /// Even with an empty needed set, PruneWindowColumns must be a NO-OP.
    #[test]
    fn prune_window_noop_even_with_empty_needed() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let node = WindowNode {
            input: dummy_input(),
            window_exprs: vec![make_window_expr("rn")],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_rn, "rn"),
            ],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::Window(node);
        let rule = PruneWindowColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneWindowColumns must always be a NO-OP (Gap-5 deferred)"
        );
    }
}
