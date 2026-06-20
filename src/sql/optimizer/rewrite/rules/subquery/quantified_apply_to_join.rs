//! `QuantifiedApplyToJoin` — IN / NOT IN → LeftSemi / NullAwareLeftAnti | LeftAnti.
//!
//! Self-contained. The IN key (`lhs = inner_col`) is ALWAYS a bare `Eq` so the
//! Cascades implement phase can extract a hash key; NULL-aware NOT IN semantics
//! live entirely in the JoinKind (NullAwareLeftAnti), never in an IS-NULL-OR
//! wrapper (existing lesson: IS-NULL-OR wrapping degraded NOT IN to a NestLoop
//! join that timed out). For correlated NOT IN with a nullable lifted inner
//! WHERE, that lifted predicate is wrapped coalesce(pred, false).

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
pub(crate) struct QuantifiedApplyToJoin;

impl LogicalRewriteRule for QuantifiedApplyToJoin {
    fn name(&self) -> &'static str {
        "QuantifiedApplyToJoin"
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
    matches!(
        &expr.op,
        Operator::LogicalApply(a) if a.use_semi_anti && matches!(a.kind, ApplyKind::In { .. })
    )
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
        ApplyKind::In { negated } => negated,
        _ => return Ok(None),
    };
    if !a.use_semi_anti {
        return Ok(None);
    }

    let lhs = a.subquery_expr;
    let inner_cols = scalar_utils::opt_output_columns(&apply_right, arena)?;
    let available_output_ids = inner_cols
        .iter()
        .map(|c| c.column_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let inner_col_oc = inner_cols
        .iter()
        .find(|c| c.column_id == a.inner_output_column_id)
        .ok_or_else(|| {
            format!(
                "IN subquery inner output column {} not found in right output columns [{}]",
                a.inner_output_column_id, available_output_ids
            )
        })?;
    let inner_col_ref = scalar_utils::column_ref(arena, inner_col_oc);

    let either_nullable = arena.nullable(lhs) || inner_col_oc.nullable;
    let join_type = if negated {
        if either_nullable {
            JoinKind::NullAwareLeftAnti
        } else {
            JoinKind::LeftAnti
        }
    } else {
        JoinKind::LeftSemi
    };

    let in_key = scalar_utils::eq(arena, lhs, inner_col_ref);

    let (right, condition) = if a.correlation_column_ids.is_empty() {
        (apply_right, in_key)
    } else {
        let Some(lifted) = lift_correlated_inner_opt(apply_right, &a.correlation_column_ids, arena)
        else {
            return Ok(None);
        };
        let Some(lifted_pred) = lifted.on_predicate else {
            return Ok(None);
        };
        let extra = if negated && arena.nullable(lifted_pred) {
            scalar_utils::coalesce_false(arena, lifted_pred)
        } else {
            lifted_pred
        };
        let Some(condition) = scalar_utils::combine_and(arena, vec![in_key, extra]) else {
            return Ok(None);
        };
        (lifted.right, condition)
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
    use crate::sql::optimizer::rewrite::rules::utils::split_and;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        ApplyKind, LogicalApplyNode, LogicalFilterNode, LogicalJoinNode, LogicalProjectNode,
        LogicalScanNode, PlanNodeKind,
    };

    const OUTER_A: ColumnId = ColumnId(1);
    const OUTER_K: ColumnId = ColumnId(2);
    const INNER_B: ColumnId = ColumnId(3);
    const INNER_K: ColumnId = ColumnId(4);
    const IN_OUT: ColumnId = ColumnId(5);

    fn ctx_with_arena() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn to_opt_expr(plan: &LogicalPlanNode, ctx: &mut RewriteContext) -> OptExpr {
        logical_plan_to_opt_expr(plan, &mut ctx.scalar_arena().borrow_mut())
    }

    fn output_column(id: ColumnId, name: &str, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable,
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

    fn scan(table: &str, columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: table_def(table),
                alias: None,
                columns: columns,
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

    fn outer_scan(outer_a_nullable: bool) -> LogicalPlanNode {
        scan(
            "outer",
            vec![
                output_column(OUTER_A, "a", outer_a_nullable),
                output_column(OUTER_K, "k", false),
            ],
        )
    }

    fn inner_scan(inner_b_nullable: bool, inner_k_nullable: bool) -> LogicalPlanNode {
        scan(
            "inner",
            vec![
                output_column(INNER_B, "b", inner_b_nullable),
                output_column(INNER_K, "k", inner_k_nullable),
            ],
        )
    }

    fn col_ref(id: ColumnId, name: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn bool_output() -> OutputColumn {
        OutputColumn {
            column_id: IN_OUT,
            name: "in_result".to_string(),
            data_type: DataType::Boolean,
            nullable: true,
            is_internal: true,
        }
    }

    fn correlation_predicate(nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_K, "k", nullable)),
                op: BinOp::Eq,
                right: Box::new(col_ref(OUTER_K, "k", false)),
            },
            data_type: DataType::Boolean,
            nullable,
        }
    }

    fn projected_correlated_inner(
        inner_b_nullable: bool,
        inner_k_nullable: bool,
        predicate_nullable: bool,
        include_inner_k_projection: bool,
    ) -> LogicalPlanNode {
        let mut items = vec![ProjectItem {
            expr: col_ref(INNER_B, "b", inner_b_nullable),
            output_name: "b".to_string(),
            output_column_id: INNER_B,
        }];
        if include_inner_k_projection {
            items.push(ProjectItem {
                expr: col_ref(INNER_K, "k", inner_k_nullable),
                output_name: "k".to_string(),
                output_column_id: INNER_K,
            });
        }

        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: items,
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::Filter(LogicalFilterNode {
                    predicate: correlation_predicate(predicate_nullable),
                }),
                vec![inner_scan(inner_b_nullable, inner_k_nullable)],
                None,
            )],
            None,
        )
    }

    fn in_apply(
        negated: bool,
        outer_a_nullable: bool,
        right: LogicalPlanNode,
        correlated: bool,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::In { negated },
                subquery_expr: col_ref(OUTER_A, "a", outer_a_nullable),
                output_column: bool_output(),
                inner_output_column_id: INNER_B,
                correlation_column_ids: if correlated { vec![OUTER_K] } else { vec![] },
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: true,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![outer_scan(outer_a_nullable), right],
            None,
        )
    }

    fn set_inner_output_column_id(plan: &mut LogicalPlanNode, inner_output_column_id: ColumnId) {
        let PlanNodeKind::Apply(apply) = &mut plan.kind else {
            panic!("expected Apply plan, got: {plan:?}");
        };
        apply.inner_output_column_id = inner_output_column_id;
    }

    fn set_use_semi_anti(plan: &mut LogicalPlanNode, use_semi_anti: bool) {
        let PlanNodeKind::Apply(apply) = &mut plan.kind else {
            panic!("expected Apply plan, got: {plan:?}");
        };
        apply.use_semi_anti = use_semi_anti;
    }

    fn rewrite(plan: LogicalPlanNode) -> LogicalPlanNode {
        let rule = QuantifiedApplyToJoin;
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

    fn assert_eq_condition(condition: &TypedExpr, expected_a: ColumnId, expected_b: ColumnId) {
        let ExprKind::BinaryOp { left, op, right } = &condition.kind else {
            panic!("expected bare Eq condition, got: {condition:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        // The arena normalizes commutative Eq by ScalarId order, so left/right
        // assignment is an implementation detail. Check that the expected pair
        // of column ids appears in either order.
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
            pair == (expected_a, expected_b) || pair == (expected_b, expected_a),
            "expected Eq condition to reference {expected_a:?} and {expected_b:?}; got {pair:?}"
        );
    }

    /// Returns true if `condition` is `a = b` (Eq BinaryOp) with the two
    /// expected column ids in either order.
    fn eq_condition_has_pair(condition: &TypedExpr, a: ColumnId, b: ColumnId) -> bool {
        let ExprKind::BinaryOp { left, op, right } = &condition.kind else {
            return false;
        };
        if *op != BinOp::Eq {
            return false;
        }
        let left_id = match &left.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            _ => return false,
        };
        let right_id = match &right.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            _ => return false,
        };
        let pair = (left_id, right_id);
        pair == (a, b) || pair == (b, a)
    }

    fn assert_coalesce_false(
        condition: &TypedExpr,
        expected_left: ColumnId,
        expected_right: ColumnId,
    ) {
        let ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } = &condition.kind
        else {
            panic!("expected coalesce predicate, got: {condition:?}");
        };
        assert_eq!(name, "coalesce");
        assert!(!distinct);
        assert_eq!(args.len(), 2);
        assert_eq_condition(&args[0], expected_left, expected_right);
        assert!(matches!(
            args[1].kind,
            ExprKind::Literal(LiteralValue::Bool(false))
        ));
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
    fn in_uncorrelated_emits_left_semi() {
        let plan = rewrite(in_apply(false, false, inner_scan(false, false), false));
        let join = assert_join(&plan, JoinKind::LeftSemi);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn not_in_nullable_emits_null_aware_left_anti() {
        let plan = rewrite(in_apply(true, false, inner_scan(true, false), false));
        let join = assert_join(&plan, JoinKind::NullAwareLeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn not_in_lhs_nullable_emits_null_aware_left_anti() {
        let plan = rewrite(in_apply(true, true, inner_scan(false, false), false));
        let join = assert_join(&plan, JoinKind::NullAwareLeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn not_in_non_nullable_downgrades_to_left_anti() {
        let plan = rewrite(in_apply(true, false, inner_scan(false, false), false));
        let join = assert_join(&plan, JoinKind::LeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(&plan.right().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn in_correlated_emits_semi_with_lifted_on() {
        let plan = rewrite(in_apply(
            false,
            false,
            projected_correlated_inner(false, false, false, true),
            true,
        ));
        let join = assert_join(&plan, JoinKind::LeftSemi);
        let conjuncts = split_and(join.condition.as_ref().unwrap().clone());

        assert_eq!(conjuncts.len(), 2);
        // The arena normalizes AND by ScalarId order, so conjunct order may
        // differ from the original. Assert that each expected pair appears
        // somewhere in the conjunct list.
        let has_outer_a_inner_b = conjuncts
            .iter()
            .any(|c| eq_condition_has_pair(c, OUTER_A, INNER_B));
        let has_outer_k_inner_k = conjuncts
            .iter()
            .any(|c| eq_condition_has_pair(c, OUTER_K, INNER_K));
        assert!(has_outer_a_inner_b, "expected OUTER_A=INNER_B conjunct");
        assert!(has_outer_k_inner_k, "expected OUTER_K=INNER_K conjunct");
        let right = plan.right();
        let PlanNodeKind::Project(_project) = &right.kind else {
            panic!("expected Project right, got: {:?}", right);
        };
        assert!(matches!(&right.unary_input().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn not_in_correlated_nullable_coalesces_lifted_pred() {
        let plan = rewrite(in_apply(
            true,
            false,
            projected_correlated_inner(true, true, true, true),
            true,
        ));
        let join = assert_join(&plan, JoinKind::NullAwareLeftAnti);
        let conjuncts = split_and(join.condition.as_ref().unwrap().clone());

        assert_eq!(conjuncts.len(), 2);
        // The arena normalizes AND by ScalarId order; find each expected conjunct
        // without relying on its position in the list.
        let outer_a_inner_b_idx = conjuncts
            .iter()
            .position(|c| eq_condition_has_pair(c, OUTER_A, INNER_B))
            .expect("expected OUTER_A=INNER_B conjunct");
        let coalesce_idx = if outer_a_inner_b_idx == 0 { 1 } else { 0 };
        assert_coalesce_false(&conjuncts[coalesce_idx], OUTER_K, INNER_K);
        let right = plan.right();
        let PlanNodeKind::Project(_project) = &right.kind else {
            panic!("expected Project right, got: {:?}", right);
        };
        assert!(matches!(&right.unary_input().kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn in_correlated_project_exposes_lifted_predicate_inner_column() {
        let plan = rewrite(in_apply(
            false,
            false,
            projected_correlated_inner(false, false, false, false),
            true,
        ));
        let _join = assert_join(&plan, JoinKind::LeftSemi);

        assert!(
            project_outputs_column(plan.right(), INNER_K),
            "right child must expose INNER_K referenced by the join ON"
        );
    }

    #[test]
    fn missing_inner_output_column_id_errors() {
        let rule = QuantifiedApplyToJoin;
        let mut ctx = ctx_with_arena();
        let mut plan = in_apply(false, false, inner_scan(false, false), false);
        let missing = ColumnId(999);
        set_inner_output_column_id(&mut plan, missing);
        let expr = to_opt_expr(&plan, &mut ctx);

        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("missing inner output column id must error");

        assert!(err.contains("c999"), "unexpected error: {err}");
        assert!(err.contains("c3"), "unexpected error: {err}");
        assert!(err.contains("c4"), "unexpected error: {err}");
    }

    #[test]
    fn use_semi_anti_false_is_not_rewritten() {
        let rule = QuantifiedApplyToJoin;
        let mut ctx = ctx_with_arena();
        let mut plan = in_apply(false, false, inner_scan(false, false), false);
        set_use_semi_anti(&mut plan, false);
        let expr = to_opt_expr(&plan, &mut ctx);

        assert!(
            !rule.matches(&expr, &ctx),
            "use_semi_anti=false must not match"
        );
        let result = rule.apply(expr, &mut ctx).expect("rewrite must not error");
        assert!(matches!(result, RewriteResult::Unchanged));
    }
}
