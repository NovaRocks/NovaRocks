//! PruneCTEConsumeColumns trims logical CTE consume outputs while preserving
//! each consumer output column's mapped producer column id.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneCTEConsumeColumns;

impl LogicalRewriteRule for PruneCTEConsumeColumns {
    fn name(&self) -> &'static str {
        "PruneCTEConsumeColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::CTEConsume,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalCTEConsume(mut node) = op else {
            unreachable!();
        };
        node.validate_mapping()?;
        let Some(needed) = required_output_columns.as_ref() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();
        let original_pairs = node
            .output_columns
            .into_iter()
            .zip(node.producer_column_ids)
            .collect::<Vec<_>>();
        let mut kept = original_pairs
            .iter()
            .filter(|(output, _)| needed.contains(&output.column_id))
            .cloned()
            .collect::<Vec<_>>();
        if kept.is_empty() {
            let Some(first) = original_pairs.first().cloned() else {
                return Err(format!(
                    "CTEConsume has no output columns for cte_id={}",
                    node.cte_id
                ));
            };
            kept.push(first);
        }
        if kept.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        let (output_columns, producer_column_ids): (Vec<_>, Vec<_>) = kept.into_iter().unzip();
        node.output_columns = output_columns;
        node.producer_column_ids = producer_column_ids;

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalCTEConsume(node),
            children,
            required_output_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{CTEConsumeOp, Operator};
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
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    #[test]
    fn prune_cte_consume_keeps_required_output_and_parallel_producer_mapping() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let p_a = ColumnId::new_for_test(11);
        let p_b = ColumnId::new_for_test(12);
        let p_c = ColumnId::new_for_test(13);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let mut expr = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: 1,
            alias: "cte1".to_string(),
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_c, "c"),
            ],
            producer_column_ids: vec![p_a, p_b, p_c],
        }));
        expr.required_output_columns = Some(needed);

        let result = PruneCTEConsumeColumns.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(new_expr) = result else {
            panic!("expected changed CTE consume");
        };
        let Operator::LogicalCTEConsume(op) = new_expr.op else {
            panic!("expected CTEConsume");
        };
        assert_eq!(
            op.output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            vec![id_b]
        );
        assert_eq!(op.producer_column_ids, vec![p_b]);
    }

    #[test]
    fn prune_cte_consume_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let expr = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: 2u32,
            alias: "cte2".to_string(),
            output_columns: vec![make_output_column(id_a, "a")],
            producer_column_ids: vec![id_a],
        }));
        // required_output_columns = None (default)

        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_cte_consume_empty_needed_keeps_first_mapping_pair() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let p_a = ColumnId::new_for_test(11);
        let p_b = ColumnId::new_for_test(12);

        let mut expr = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: 3,
            alias: "cte3".to_string(),
            output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            producer_column_ids: vec![p_a, p_b],
        }));
        expr.required_output_columns = Some(HashSet::new());

        let result = PruneCTEConsumeColumns.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(new_expr) = result else {
            panic!("expected fallback changed CTE consume");
        };
        let Operator::LogicalCTEConsume(op) = new_expr.op else {
            panic!("expected CTEConsume");
        };
        assert_eq!(op.output_columns[0].column_id, id_a);
        assert_eq!(op.producer_column_ids, vec![p_a]);
    }
}
