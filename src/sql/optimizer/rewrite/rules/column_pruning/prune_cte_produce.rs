//! PruneCTEProduceColumns trims logical CTE produce outputs in producer
//! ColumnId space.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::collect_output_ids_ordered_opt;

pub(crate) struct PruneCTEProduceColumns;

impl LogicalRewriteRule for PruneCTEProduceColumns {
    fn name(&self) -> &'static str {
        "PruneCTEProduceColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::CTEProduce,
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
        let Operator::LogicalCTEProduce(mut node) = op else {
            unreachable!();
        };
        let Some(needed) = required_output_columns.as_ref() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_output_columns = node.output_columns;
        let original_len = original_output_columns.len();
        let mut output_columns = original_output_columns
            .iter()
            .filter(|column| needed.contains(&column.column_id))
            .cloned()
            .collect::<Vec<_>>();
        if output_columns.is_empty() {
            if let Some(first) = original_output_columns.first().cloned() {
                output_columns.push(first);
            }
        }
        if output_columns.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }
        let desired_output_ids = output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        let child_output_ids = children
            .first()
            .map(collect_output_ids_ordered_opt)
            .unwrap_or_default();
        if child_output_ids != desired_output_ids {
            return Ok(RewriteResult::Unchanged);
        }

        node.output_columns = output_columns;

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalCTEProduce(node),
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
    use crate::sql::optimizer::operator::{CTEProduceOp, LogicalAggregateOp, Operator, ValuesOp};
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

    fn values_input(columns: Vec<OutputColumn>) -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns,
        }))
    }

    fn aggregate_input(output_columns: Vec<OutputColumn>) -> OptExpr {
        OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(vec![], vec![], output_columns)),
            vec![values_input(vec![])],
        )
    }

    #[test]
    fn prune_cte_produce_keeps_required_producer_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_c);

        let mut expr = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 42u32,
                output_columns: vec![
                    make_output_column(id_a, "a"),
                    make_output_column(id_b, "b"),
                    make_output_column(id_c, "c"),
                ],
            }),
            vec![values_input(vec![
                make_output_column(id_a, "a"),
                make_output_column(id_c, "c"),
            ])],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneCTEProduceColumns.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(new_expr) = result else {
            panic!("expected changed CTE produce");
        };
        let Operator::LogicalCTEProduce(op) = new_expr.op else {
            panic!("expected CTEProduce");
        };
        assert_eq!(
            op.output_columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>(),
            vec![id_a, id_c]
        );
    }

    #[test]
    fn prune_cte_produce_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let expr = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 5u32,
                output_columns: vec![make_output_column(id_a, "a")],
            }),
            vec![values_input(vec![make_output_column(id_a, "a")])],
        );
        // required_output_columns = None (default)

        let rule = PruneCTEProduceColumns;
        let result = rule.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_cte_produce_empty_needed_keeps_first_column() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let mut expr = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 7u32,
                output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            }),
            vec![values_input(vec![make_output_column(id_a, "a")])],
        );
        expr.required_output_columns = Some(HashSet::new());

        let result = PruneCTEProduceColumns.apply(expr, &mut ctx()).unwrap();
        let RewriteResult::Changed(new_expr) = result else {
            panic!("expected fallback changed CTE produce");
        };
        let Operator::LogicalCTEProduce(op) = new_expr.op else {
            panic!("expected CTEProduce");
        };
        assert_eq!(op.output_columns[0].column_id, id_a);
    }

    #[test]
    fn prune_cte_produce_noop_when_child_keeps_unpruned_outputs() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let output_columns = vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")];

        let mut needed = HashSet::new();
        needed.insert(id_a);

        let mut expr = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: 9u32,
                output_columns: output_columns.clone(),
            }),
            vec![aggregate_input(output_columns)],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneCTEProduceColumns.apply(expr, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "CTE produce must not trim declared outputs when its child still emits the full schema"
        );
    }
}
