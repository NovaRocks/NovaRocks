//! AggregatePushdownRule entry point.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule as RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;

#[allow(dead_code)]
pub(crate) struct AggregatePushdownRule {
    table_stats: Arc<HashMap<String, TableStatistics>>,
}

impl AggregatePushdownRule {
    #[allow(dead_code)]
    pub(crate) fn new(table_stats: Arc<HashMap<String, TableStatistics>>) -> Self {
        Self { table_stats }
    }
}

impl RewriteRule for AggregatePushdownRule {
    fn name(&self) -> &'static str {
        "AggregatePushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalAggregate(_))
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // Extract the aggregate op; return Unchanged if shape doesn't match.
        let agg = match expr.op {
            Operator::LogicalAggregate(ref a) => a.clone(),
            _ => return Ok(RewriteResult::Unchanged),
        };

        let arena_rc = ctx.scalar_arena();

        // Phase 1: read-only borrow for collection and cost gating.
        let push = {
            let arena = arena_rc.borrow();
            let push = super::collector::collect_push_plan(
                &agg,
                expr.unary_input(),
                &self.table_stats,
                &arena,
            );
            if let Some(ref p) = push {
                if !super::cost::should_push(p, &arena, &self.table_stats) {
                    return Ok(RewriteResult::Unchanged);
                }
            }
            push
        };
        let Some(push) = push else {
            return Ok(RewriteResult::Unchanged);
        };

        // Phase 2: mutable borrow for rewriting.
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "AggregatePushdown requires ColumnRefFactory".to_string())?;
        let mut factory = factory.borrow_mut();
        let mut arena = arena_rc.borrow_mut();

        Ok(RewriteResult::Changed(super::rewriter::rewrite(
            &agg,
            expr.unary_input(),
            push,
            &mut factory,
            &mut arena,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use arrow::datatypes::DataType;

    fn dummy_scan(name: &str, cols: &[&str]) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: (*n).into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn dummy_aggregate() -> OptExpr {
        let scan = dummy_scan("t", &["id"]);
        OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                vec![],
                vec![],
                vec![],
                vec![],
                false,
            )),
            vec![scan],
        )
    }

    #[test]
    fn stub_returns_none() {
        use crate::sql::optimizer::scalar::ScalarArena;
        use std::cell::RefCell;
        use std::rc::Rc;
        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        let plan = dummy_aggregate();
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        assert!(rule.matches(&plan, &ctx));
        assert!(matches!(
            rule.apply(plan, &mut ctx).unwrap(),
            RewriteResult::Unchanged
        ));
    }

    #[test]
    fn idempotent_does_not_repush_already_pushed_plan() {
        use crate::sql::analysis::{BinOp, ExprKind, JoinKind};
        use crate::sql::optimizer::operator::{LogicalJoinOp, ScalarAggregateSpec};
        use crate::sql::optimizer::scalar::ScalarArena;

        use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
        use std::cell::RefCell;
        use std::rc::Rc;

        fn col_typed(name: &str) -> crate::sql::analysis::TypedExpr {
            // Use a stable non-UNSET ColumnId derived from the name bytes (FNV-1a).
            let mut h: u32 = 2166136261;
            for b in name.bytes() {
                h ^= b as u32;
                h = h.wrapping_mul(16777619);
            }
            let col_id = ColumnId::new_for_test((h % 10000) + 1);
            crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: col_id,
                    qualifier: None,
                    column: name.into(),
                },
                data_type: DataType::Int64,
                nullable: true,
            }
        }

        let mut arena = ScalarArena::new();

        let gb_id = intern_typed(&mut arena, &col_typed("k"));
        let sum_arg = intern_typed(&mut arena, &col_typed("v"));
        let sum_spec = ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![sum_arg],
            distinct: false,
            order_by: vec![],
        };

        let cond_typed = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_typed("k")),
                op: BinOp::Eq,
                right: Box::new(col_typed("k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let cond_id = intern_typed(&mut arena, &cond_typed);

        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond_id),
            }),
            vec![dummy_scan("a", &["k", "v"]), dummy_scan("b", &["k"])],
        );

        // Build a plan with is_split = true. The rule must reject.
        let plan = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                vec![gb_id],
                vec![sum_spec],
                vec![],
                vec![false],
                true, // is_split = true: already pushed, must not repush
            )),
            vec![join],
        );

        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));

        assert!(
            matches!(
                rule.apply(plan, &mut ctx).unwrap(),
                RewriteResult::Unchanged
            ),
            "must not re-fire on is_split=true"
        );
    }
}
