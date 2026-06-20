//! `ExistentialApplyToJoin` - EXISTS / NOT EXISTS -> LeftSemi / LeftAnti join.
//!
//! Self-contained: reads the inner subquery's WHERE directly (no dependency on
//! PushDownApplyFilter). Correlated EXISTS becomes
//! `outer LEFT SEMI JOIN inner ON <normalized correlation predicate>`;
//! NOT EXISTS -> LEFT ANTI; uncorrelated -> semi/anti ON true.

use super::predicate_apply_util::lift_correlated_inner_opt;
use super::scalar_utils;
use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::plan::ApplyKind;

#[allow(dead_code)] // Registered by Task 6.
pub(crate) struct ExistentialApplyToJoin;

impl LogicalRewriteRule for ExistentialApplyToJoin {
    fn name(&self) -> &'static str {
        "ExistentialApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let _ = ctx;
        matches_expr(expr)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena = ctx.scalar_arena();
        let mut arena = arena.borrow_mut();
        match apply_expr(expr, &mut arena)? {
            Some(new_expr) => Ok(RewriteResult::Changed(new_expr)),
            None => Ok(RewriteResult::Unchanged),
        }
    }
}

fn matches_expr(expr: &OptExpr) -> bool {
    matches!(&expr.op, Operator::LogicalApply(a) if matches!(a.kind, ApplyKind::Exists { .. }))
}

fn apply_expr(expr: OptExpr, arena: &mut ScalarArena) -> Result<Option<OptExpr>, String> {
    let OptExpr {
        op,
        mut children,
        required_output_columns: _,
    } = expr;
    let Operator::LogicalApply(a) = op else {
        return Ok(None);
    };
    if children.len() != 2 {
        return Ok(None);
    }
    let apply_right = children.remove(1);
    let apply_left = children.remove(0);
    let negated = match a.kind {
        ApplyKind::Exists { negated } => negated,
        _ => return Ok(None),
    };
    let join_type = if negated {
        JoinKind::LeftAnti
    } else {
        JoinKind::LeftSemi
    };

    let (right, condition) = if a.correlation_column_ids.is_empty() {
        (apply_right, scalar_utils::bool_literal(arena, true))
    } else {
        let Some(lifted) = lift_correlated_inner_opt(apply_right, &a.correlation_column_ids, arena)
        else {
            return Ok(None);
        };
        let Some(pred) = lifted.on_predicate else {
            return Ok(None);
        };
        (lifted.right, pred)
    };

    Ok(Some(scalar_utils::join(
        apply_left,
        right,
        join_type,
        Some(condition),
    )))
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rules::subquery::bridge::opt_expr_to_plan;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        ApplyKind, LogicalApplyNode, LogicalFilterNode, LogicalJoinNode, LogicalProjectNode,
        LogicalScanNode, PlanNodeKind,
    };

    const OUTER_K: ColumnId = ColumnId(1);
    const INNER_K: ColumnId = ColumnId(2);
    const EXISTS_OUT: ColumnId = ColumnId(3);
    const CONST_ONE: ColumnId = ColumnId(4);

    fn ctx_with_arena() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn to_opt_expr(plan: &LogicalPlanNode, ctx: &mut RewriteContext) -> OptExpr {
        logical_plan_to_opt_expr(plan, &mut ctx.scalar_arena().borrow_mut())
    }

    fn output_column(id: ColumnId, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn table_def(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn scan(table: &str, id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: table_def(table),
                alias: None,
                columns: vec![output_column(id, "k", DataType::Int64)],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn col_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn exists_output() -> OutputColumn {
        OutputColumn {
            column_id: EXISTS_OUT,
            name: "exists".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: true,
        }
    }

    fn correlation_predicate() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_K, "k")),
                op: BinOp::Eq,
                right: Box::new(col_ref(OUTER_K, "k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn correlated_inner() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(INNER_K, "k"),
                    output_name: "k".to_string(),
                    output_column_id: INNER_K,
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::Filter(LogicalFilterNode {
                    predicate: correlation_predicate(),
                }),
                vec![scan("inner", INNER_K)],
                None,
            )],
            None,
        )
    }

    fn correlated_select_one_inner() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "1".to_string(),
                    output_column_id: CONST_ONE,
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::Filter(LogicalFilterNode {
                    predicate: correlation_predicate(),
                }),
                vec![scan("inner", INNER_K)],
                None,
            )],
            None,
        )
    }

    fn correlated_project_scan_inner() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(INNER_K, "k"),
                    output_name: "k".to_string(),
                    output_column_id: INNER_K,
                }],
                output_qualifier: None,
            }),
            vec![scan("inner", INNER_K)],
            None,
        )
    }

    fn exists_apply(negated: bool, right: LogicalPlanNode, correlated: bool) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Exists { negated },
                subquery_expr: bool_expr(),
                output_column: exists_output(),
                inner_output_column_id: INNER_K,
                correlation_column_ids: if correlated { vec![OUTER_K] } else { vec![] },
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: true,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![scan("outer", OUTER_K), right],
            None,
        )
    }

    fn rewrite(plan: LogicalPlanNode) -> LogicalPlanNode {
        let rule = ExistentialApplyToJoin;
        let mut ctx = ctx_with_arena();
        let expr = to_opt_expr(&plan, &mut ctx);
        assert!(rule.matches(&expr, &ctx));
        match rule.apply(expr, &mut ctx).expect("rewrite must not error") {
            RewriteResult::Changed(new_expr) => {
                opt_expr_to_plan(&new_expr, &ctx.scalar_arena().borrow())
            }
            other => panic!("expected Changed, got: {other:?}"),
        }
    }

    fn assert_join(plan: &LogicalPlanNode, expected_kind: JoinKind) -> &LogicalJoinNode {
        assert!(
            !contains_apply(plan),
            "result must not contain Apply: {plan:?}"
        );
        let PlanNodeKind::Join(join) = &plan.kind else {
            panic!("expected Join, got: {plan:?}");
        };
        assert_eq!(join.join_type, expected_kind);
        assert!(join.condition.is_some(), "join condition must exist");
        join
    }

    fn assert_correlation_condition(condition: &TypedExpr) {
        let ExprKind::BinaryOp { left, op, right } = &condition.kind else {
            panic!("expected binary condition, got: {condition:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        // The arena normalizes commutative Eq by ScalarId order, so the
        // left/right assignment is an implementation detail rather than a
        // semantic guarantee. Assert that the two expected column ids appear
        // somewhere in the condition, regardless of which side is "left".
        let left_id = match &left.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("expected column ref on left, got: {other:?}"),
        };
        let right_id = match &right.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("expected column ref on right, got: {other:?}"),
        };
        let pair = (left_id, right_id);
        assert!(
            pair == (OUTER_K, INNER_K) || pair == (INNER_K, OUTER_K),
            "expected correlation condition to reference OUTER_K and INNER_K; got {pair:?}"
        );
    }

    fn assert_true_condition(condition: &TypedExpr) {
        assert!(matches!(
            condition.kind,
            ExprKind::Literal(LiteralValue::Bool(true))
        ));
        assert_eq!(condition.data_type, DataType::Boolean);
        assert!(!condition.nullable);
    }

    fn contains_apply(plan: &LogicalPlanNode) -> bool {
        match &plan.kind {
            PlanNodeKind::Apply(_) => true,
            PlanNodeKind::Filter(_) | PlanNodeKind::Project(_) => {
                contains_apply(plan.unary_input())
            }
            PlanNodeKind::Join(_) => contains_apply(plan.left()) || contains_apply(plan.right()),
            _ => false,
        }
    }

    fn project_outputs_column(plan: &LogicalPlanNode, expected: ColumnId) -> bool {
        let PlanNodeKind::Project(project) = &plan.kind else {
            return false;
        };
        project
            .items
            .iter()
            .any(|item| item.output_column_id == expected)
    }

    #[test]
    fn exists_correlated_emits_left_semi() {
        let plan = rewrite(exists_apply(false, correlated_inner(), true));
        let join = assert_join(&plan, JoinKind::LeftSemi);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        let right = plan.right();
        let PlanNodeKind::Project(_project) = &right.kind else {
            panic!("expected Project right, got: {:?}", right);
        };
        assert!(matches!(&right.unary_input().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn exists_correlated_select_one_exposes_inner_key_for_join_on() {
        let plan = rewrite(exists_apply(false, correlated_select_one_inner(), true));
        let join = assert_join(&plan, JoinKind::LeftSemi);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        assert!(
            project_outputs_column(plan.right(), INNER_K),
            "right child must expose INNER_K referenced by the join ON"
        );
    }

    #[test]
    fn exists_correlated_project_scan_returns_unchanged() {
        let rule = ExistentialApplyToJoin;
        let mut ctx = ctx_with_arena();
        let plan = exists_apply(false, correlated_project_scan_inner(), true);
        let expr = to_opt_expr(&plan, &mut ctx);

        let result = rule.apply(expr, &mut ctx).expect("rewrite must not error");

        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn not_exists_correlated_emits_left_anti() {
        let plan = rewrite(exists_apply(true, correlated_inner(), true));
        let join = assert_join(&plan, JoinKind::LeftAnti);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        let right = plan.right();
        let PlanNodeKind::Project(_project) = &right.kind else {
            panic!("expected Project right, got: {:?}", right);
        };
        assert!(matches!(&right.unary_input().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn exists_uncorrelated_emits_left_semi_on_true() {
        let plan = rewrite(exists_apply(false, scan("inner", INNER_K), false));
        let join = assert_join(&plan, JoinKind::LeftSemi);

        assert_true_condition(join.condition.as_ref().unwrap());
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn not_exists_uncorrelated_emits_left_anti_on_true() {
        let plan = rewrite(exists_apply(true, scan("inner", INNER_K), false));
        let join = assert_join(&plan, JoinKind::LeftAnti);

        assert_true_condition(join.condition.as_ref().unwrap());
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }
}
