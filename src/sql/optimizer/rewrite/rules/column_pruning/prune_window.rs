//! PruneWindowColumns — Phase 2 rule for Window nodes.
//!
//! ## Gap-5: Window output pruning is intentionally deferred (NO-OP)
//!
//! `WindowOp.output_columns` is built by `build_window_and_project` in the
//! planner. Window function outputs now carry stable
//! `WindowExpr.output_column_id` values that correspond to entries in
//! `WindowOp.output_columns`, including internal synthetic slots for
//! compound SELECT expressions.
//!
//! Safe pruning is still deferred because the parent Project currently reads
//! rewritten window outputs through `ColumnRef { column_id: UNSET, column:
//! <display-name> }`. The parent request is ColumnId-based, but the reference
//! from Project back to Window output slots has not yet been rebound by id.
//! Falling back to name-based `output_name` matching remains fragile and can
//! over-prune `window_exprs`.
//!
//! `tag_window` already passes `None` (keep-all) to the child, so the child
//! keeps all its input columns as well.  This is consistent and safe.
//!
//! Follow-up: re-enable pruning once the parent Project/window references are
//! rewritten to address `WindowExpr.output_column_id` directly.

use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneWindowColumns;

impl LogicalRewriteRule for PruneWindowColumns {
    fn name(&self) -> &'static str {
        "PruneWindowColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Window,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, _expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // NO-OP: Window output pruning is deferred (Gap-5).
        // WindowExpr outputs have stable ids, but the parent Project still
        // references them through UNSET/name ColumnRefs. Name-based pruning
        // remains unsafe until those references are rebound by id.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{Operator, ScalarWindowSpec, ScanOp, WindowOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
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

    fn make_window_spec(output_column_id: ColumnId) -> ScalarWindowSpec {
        ScalarWindowSpec {
            output_column_id,
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
        }
    }

    fn dummy_input() -> OptExpr {
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
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table,
            alias: None,
            stats_ref: None,
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
            variant_columns: vec![],
            mv_rewritten_from: None,
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

        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn1), make_window_spec(id_rn2)],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_rn1, "rn1"),
                    make_output_column(id_rn2, "rn2"),
                ],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneWindowColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneWindowColumns must always return Unchanged (Gap-5 no-op)"
        );
    }

    #[test]
    fn prune_window_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn)],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_rn, "rn"),
                ],
            }),
            vec![dummy_input()],
        );
        // required_output_columns = None (default)

        let rule = PruneWindowColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

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

        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn)],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_rn, "rn"),
                ],
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(HashSet::new());

        let rule = PruneWindowColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneWindowColumns must always be a NO-OP (Gap-5 deferred)"
        );
    }
}
