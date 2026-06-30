//! PruneWindowColumns — Phase 2 rule for Window nodes.
//!
//! Window expressions are identified by `ScalarWindowSpec.output_column_id`.
//! Parent requirements are ColumnId-based, so the rule can prune unused window
//! expressions without falling back to output names or output layout positions.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{Operator, WindowOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_opt;

pub(crate) struct PruneWindowColumns;

fn validate_window_output_contract(window: &WindowOp) -> Result<HashSet<ColumnId>, String> {
    let mut window_ids = HashSet::new();
    for expr in &window.window_exprs {
        if expr.output_column_id == ColumnId::UNSET {
            return Err("PruneWindowColumns found UNSET window output_column_id".to_string());
        }
        if !window_ids.insert(expr.output_column_id) {
            return Err(format!(
                "duplicate window output column id {}",
                expr.output_column_id.0
            ));
        }
        let count = window
            .output_columns
            .iter()
            .filter(|column| column.column_id == expr.output_column_id)
            .count();
        if count == 0 {
            return Err(format!(
                "window output column id {} missing from WindowOp.output_columns",
                expr.output_column_id.0
            ));
        }
        if count > 1 {
            return Err(format!(
                "duplicate window output column id {}",
                expr.output_column_id.0
            ));
        }
    }
    Ok(window_ids)
}

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

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            mut children,
            required_output_columns,
        } = expr;
        let Operator::LogicalWindow(mut node) = op else {
            unreachable!()
        };

        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let input = children.remove(0);
        let original_window_expr_len = node.window_exprs.len();
        let original_output_len = node.output_columns.len();
        validate_window_output_contract(&node)?;

        let child_output_ids = collect_output_ids_opt(&input);
        let retained_window_exprs: Vec<_> = node
            .window_exprs
            .into_iter()
            .filter(|expr| needed.contains(&expr.output_column_id))
            .collect();

        if retained_window_exprs.is_empty() {
            return Ok(RewriteResult::Changed(input));
        }

        let retained_window_ids: HashSet<_> = retained_window_exprs
            .iter()
            .map(|expr| expr.output_column_id)
            .collect();
        let retained_output_columns: Vec<_> = node
            .output_columns
            .into_iter()
            .filter(|column| {
                retained_window_ids.contains(&column.column_id)
                    || (child_output_ids.contains(&column.column_id)
                        && needed.contains(&column.column_id))
            })
            .collect();

        if retained_window_exprs.len() == original_window_expr_len
            && retained_output_columns.len() == original_output_len
        {
            return Ok(RewriteResult::Unchanged);
        }

        node.window_exprs = retained_window_exprs;
        node.output_columns = retained_output_columns;
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalWindow(node),
            children: vec![input],
            required_output_columns,
        }))
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

    fn make_window_spec(output_column_id: ColumnId, name: &str) -> ScalarWindowSpec {
        ScalarWindowSpec {
            output_column_id,
            name: name.to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
        }
    }

    fn dummy_input_with_columns(columns: Vec<OutputColumn>) -> OptExpr {
        let table = TableDef {
            name: "t".to_string(),
            columns: columns
                .iter()
                .map(|column| ColumnDef {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
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
            columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    #[test]
    fn prune_window_keeps_required_window_expr_and_drops_unrequired_expr() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_rn1 = ColumnId::new_for_test(101);
        let id_rn2 = ColumnId::new_for_test(102);
        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_rn1);

        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![
                    make_window_spec(id_rn1, "row_number"),
                    make_window_spec(id_rn2, "rank"),
                ],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_rn1, "rn1"),
                    make_output_column(id_rn2, "rn2"),
                ],
            }),
            vec![dummy_input_with_columns(vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
            ])],
        );
        expr.required_output_columns = Some(needed);

        let rule = PruneWindowColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(changed) = result else {
            panic!("expected PruneWindowColumns to change the Window node");
        };
        let Operator::LogicalWindow(window) = changed.op else {
            panic!("expected the changed node to remain LogicalWindow");
        };

        assert_eq!(window.window_exprs.len(), 1);
        assert_eq!(window.window_exprs[0].output_column_id, id_rn1);
        let output_ids: Vec<_> = window.output_columns.iter().map(|c| c.column_id).collect();
        assert_eq!(output_ids, vec![id_a, id_rn1]);
    }

    #[test]
    fn prune_window_eliminates_node_when_no_window_expr_is_required() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);
        let mut needed = HashSet::new();
        needed.insert(id_a);

        let input = dummy_input_with_columns(vec![make_output_column(id_a, "a")]);
        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn, "row_number")],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_rn, "rn"),
                ],
            }),
            vec![input],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneWindowColumns.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(changed) = result else {
            panic!("expected the Window node to be eliminated");
        };
        assert!(
            !matches!(changed.op, Operator::LogicalWindow(_)),
            "Window must be removed when no window output is required"
        );
    }

    #[test]
    fn prune_window_errors_when_expr_id_is_unset() {
        let id_a = ColumnId::new_for_test(1);
        let mut needed = HashSet::new();
        needed.insert(id_a);
        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(ColumnId::UNSET, "row_number")],
                output_columns: vec![make_output_column(id_a, "a")],
            }),
            vec![dummy_input_with_columns(vec![make_output_column(
                id_a, "a",
            )])],
        );
        expr.required_output_columns = Some(needed);

        let err = PruneWindowColumns.apply(expr, &mut ctx()).unwrap_err();

        assert!(err.contains("PruneWindowColumns found UNSET window output_column_id"));
    }

    #[test]
    fn prune_window_errors_when_expr_id_is_missing_from_output_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);
        let mut needed = HashSet::new();
        needed.insert(id_rn);
        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn, "row_number")],
                output_columns: vec![make_output_column(id_a, "a")],
            }),
            vec![dummy_input_with_columns(vec![make_output_column(
                id_a, "a",
            )])],
        );
        expr.required_output_columns = Some(needed);

        let err = PruneWindowColumns.apply(expr, &mut ctx()).unwrap_err();

        assert!(err.contains("window output column id 101 missing from WindowOp.output_columns"));
    }

    #[test]
    fn prune_window_errors_when_window_expr_ids_are_duplicate() {
        let id_rn = ColumnId::new_for_test(101);
        let mut needed = HashSet::new();
        needed.insert(id_rn);
        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![
                    make_window_spec(id_rn, "row_number"),
                    make_window_spec(id_rn, "rank"),
                ],
                output_columns: vec![make_output_column(id_rn, "rn")],
            }),
            vec![dummy_input_with_columns(vec![])],
        );
        expr.required_output_columns = Some(needed);

        let err = PruneWindowColumns.apply(expr, &mut ctx()).unwrap_err();

        assert!(err.contains("duplicate window output column id 101"));
    }

    #[test]
    fn prune_window_errors_when_output_columns_have_duplicate_window_id() {
        let id_rn = ColumnId::new_for_test(101);
        let mut needed = HashSet::new();
        needed.insert(id_rn);
        let mut expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn, "row_number")],
                output_columns: vec![
                    make_output_column(id_rn, "rn_a"),
                    make_output_column(id_rn, "rn_b"),
                ],
            }),
            vec![dummy_input_with_columns(vec![])],
        );
        expr.required_output_columns = Some(needed);

        let err = PruneWindowColumns.apply(expr, &mut ctx()).unwrap_err();

        assert!(err.contains("duplicate window output column id 101"));
    }

    #[test]
    fn prune_window_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let id_rn = ColumnId::new_for_test(101);

        let expr = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![make_window_spec(id_rn, "row_number")],
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_rn, "rn"),
                ],
            }),
            vec![dummy_input_with_columns(vec![make_output_column(
                id_a, "a",
            )])],
        );
        // required_output_columns = None (default)

        let rule = PruneWindowColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }
}
