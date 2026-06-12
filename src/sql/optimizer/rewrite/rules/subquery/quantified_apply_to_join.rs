//! `QuantifiedApplyToJoin` — IN / NOT IN → LeftSemi / NullAwareLeftAnti | LeftAnti.
//!
//! Self-contained. The IN key (`lhs = inner_col`) is ALWAYS a bare `Eq` so the
//! Cascades implement phase can extract a hash key; NULL-aware NOT IN semantics
//! live entirely in the JoinKind (NullAwareLeftAnti), never in an IS-NULL-OR
//! wrapper (existing lesson: IS-NULL-OR wrapping degraded NOT IN to a NestLoop
//! join that timed out). For correlated NOT IN with a nullable lifted inner
//! WHERE, that lifted predicate is wrapped coalesce(pred, false).

use super::predicate_apply_util::{coalesce_false, eq, lift_correlated_inner};
use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::combine_and;
use crate::sql::planner::plan::{ApplyKind, JoinNode, LogicalPlan};
use crate::sql::planner::plan_output_columns;

#[allow(dead_code)] // Registered by Task 6.
pub(crate) struct QuantifiedApplyToJoin;

impl LogicalRewriteRule for QuantifiedApplyToJoin {
    fn name(&self) -> &'static str {
        "QuantifiedApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::Apply(a) if a.use_semi_anti && matches!(a.kind, ApplyKind::In { .. })
        )
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let negated = match a.kind {
            ApplyKind::In { negated } => negated,
            _ => return Ok(RewriteResult::Unchanged),
        };
        if !a.use_semi_anti {
            return Ok(RewriteResult::Unchanged);
        }

        let lhs = a.subquery_expr.clone();
        let inner_cols = plan_output_columns(&a.right)?;
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
        let inner_col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: inner_col_oc.column_id,
                qualifier: None,
                column: inner_col_oc.name.clone(),
            },
            data_type: inner_col_oc.data_type.clone(),
            nullable: inner_col_oc.nullable,
        };

        let either_nullable = lhs.nullable || inner_col_ref.nullable;
        let join_type = if negated {
            if either_nullable {
                JoinKind::NullAwareLeftAnti
            } else {
                JoinKind::LeftAnti
            }
        } else {
            JoinKind::LeftSemi
        };

        let in_key = eq(lhs, inner_col_ref);

        let (right, condition) = if a.correlation_column_ids.is_empty() {
            (*a.right, in_key)
        } else {
            let Some(lifted) = lift_correlated_inner(*a.right, &a.correlation_column_ids) else {
                return Ok(RewriteResult::Unchanged);
            };
            let Some(lifted_pred) = lifted.on_predicate else {
                return Ok(RewriteResult::Unchanged);
            };
            let extra = if negated && lifted_pred.nullable {
                coalesce_false(lifted_pred)
            } else {
                lifted_pred
            };
            (lifted.right, combine_and(vec![in_key, extra]))
        };

        Ok(RewriteResult::Changed(LogicalPlan::Join(JoinNode {
            left: a.left,
            right: Box::new(right),
            join_type,
            condition: Some(condition),
            required_output_columns: None,
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rules::utils::split_and;
    use crate::sql::planner::plan::{
        ApplyKind, ApplyNode, FilterNode, JoinNode, ProjectNode, ScanNode,
    };

    const OUTER_A: ColumnId = ColumnId(1);
    const OUTER_K: ColumnId = ColumnId(2);
    const INNER_B: ColumnId = ColumnId(3);
    const INNER_K: ColumnId = ColumnId(4);
    const IN_OUT: ColumnId = ColumnId(5);

    fn ctx() -> RewriteContext {
        RewriteContext::for_query(Vec::<String>::new())
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

    fn scan(table: &str, columns: Vec<OutputColumn>) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: table_def(table),
            alias: None,
            columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        })
    }

    fn outer_scan(outer_a_nullable: bool) -> LogicalPlan {
        scan(
            "outer",
            vec![
                output_column(OUTER_A, "a", outer_a_nullable),
                output_column(OUTER_K, "k", false),
            ],
        )
    }

    fn inner_scan(inner_b_nullable: bool, inner_k_nullable: bool) -> LogicalPlan {
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
    ) -> LogicalPlan {
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

        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(inner_scan(inner_b_nullable, inner_k_nullable)),
                predicate: correlation_predicate(predicate_nullable),
                required_output_columns: None,
            })),
            items,
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn in_apply(
        negated: bool,
        outer_a_nullable: bool,
        right: LogicalPlan,
        correlated: bool,
    ) -> LogicalPlan {
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(outer_scan(outer_a_nullable)),
            right: Box::new(right),
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
            required_output_columns: None,
        })
    }

    fn set_inner_output_column_id(plan: &mut LogicalPlan, inner_output_column_id: ColumnId) {
        let LogicalPlan::Apply(apply) = plan else {
            panic!("expected Apply plan, got: {plan:?}");
        };
        apply.inner_output_column_id = inner_output_column_id;
    }

    fn set_use_semi_anti(plan: &mut LogicalPlan, use_semi_anti: bool) {
        let LogicalPlan::Apply(apply) = plan else {
            panic!("expected Apply plan, got: {plan:?}");
        };
        apply.use_semi_anti = use_semi_anti;
    }

    fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        let rule = QuantifiedApplyToJoin;
        let mut ctx = ctx();
        assert!(rule.matches(&plan, &ctx));
        match rule.apply(plan, &mut ctx).expect("rewrite must not error") {
            RewriteResult::Changed(plan) => plan,
            other => panic!("expected Changed, got: {other:?}"),
        }
    }

    fn assert_join(plan: LogicalPlan, expected_kind: JoinKind) -> JoinNode {
        assert!(
            !contains_apply(&plan),
            "result must not contain Apply: {plan:?}"
        );
        let LogicalPlan::Join(join) = plan else {
            panic!("expected Join, got: {plan:?}");
        };
        assert_eq!(join.join_type, expected_kind);
        assert!(join.condition.is_some(), "join condition must exist");
        join
    }

    fn assert_eq_condition(
        condition: &TypedExpr,
        expected_left: ColumnId,
        expected_right: ColumnId,
    ) {
        let ExprKind::BinaryOp { left, op, right } = &condition.kind else {
            panic!("expected bare Eq condition, got: {condition:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert_column_id(left, expected_left);
        assert_column_id(right, expected_right);
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

    fn assert_column_id(expr: &TypedExpr, expected: ColumnId) {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected column ref, got: {expr:?}");
        };
        assert_eq!(*column_id, expected);
    }

    fn contains_apply(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Apply(_) => true,
            LogicalPlan::Filter(n) => contains_apply(&n.input),
            LogicalPlan::Project(n) => contains_apply(&n.input),
            LogicalPlan::Join(n) => contains_apply(&n.left) || contains_apply(&n.right),
            _ => false,
        }
    }

    fn project_outputs_column(plan: &LogicalPlan, expected: ColumnId) -> bool {
        let LogicalPlan::Project(project) = plan else {
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
        let join = assert_join(plan, JoinKind::LeftSemi);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn not_in_nullable_emits_null_aware_left_anti() {
        let plan = rewrite(in_apply(true, false, inner_scan(true, false), false));
        let join = assert_join(plan, JoinKind::NullAwareLeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn not_in_lhs_nullable_emits_null_aware_left_anti() {
        let plan = rewrite(in_apply(true, true, inner_scan(false, false), false));
        let join = assert_join(plan, JoinKind::NullAwareLeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn not_in_non_nullable_downgrades_to_left_anti() {
        let plan = rewrite(in_apply(true, false, inner_scan(false, false), false));
        let join = assert_join(plan, JoinKind::LeftAnti);

        assert_eq_condition(join.condition.as_ref().unwrap(), OUTER_A, INNER_B);
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn in_correlated_emits_semi_with_lifted_on() {
        let plan = rewrite(in_apply(
            false,
            false,
            projected_correlated_inner(false, false, false, true),
            true,
        ));
        let join = assert_join(plan, JoinKind::LeftSemi);
        let conjuncts = split_and(join.condition.unwrap());

        assert_eq!(conjuncts.len(), 2);
        assert_eq_condition(&conjuncts[0], OUTER_A, INNER_B);
        assert_eq_condition(&conjuncts[1], OUTER_K, INNER_K);
        let LogicalPlan::Project(project) = join.right.as_ref() else {
            panic!("expected Project right, got: {:?}", join.right);
        };
        assert!(matches!(project.input.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn not_in_correlated_nullable_coalesces_lifted_pred() {
        let plan = rewrite(in_apply(
            true,
            false,
            projected_correlated_inner(true, true, true, true),
            true,
        ));
        let join = assert_join(plan, JoinKind::NullAwareLeftAnti);
        let conjuncts = split_and(join.condition.unwrap());

        assert_eq!(conjuncts.len(), 2);
        assert_eq_condition(&conjuncts[0], OUTER_A, INNER_B);
        assert_coalesce_false(&conjuncts[1], OUTER_K, INNER_K);
        let LogicalPlan::Project(project) = join.right.as_ref() else {
            panic!("expected Project right, got: {:?}", join.right);
        };
        assert!(matches!(project.input.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn in_correlated_project_exposes_lifted_predicate_inner_column() {
        let plan = rewrite(in_apply(
            false,
            false,
            projected_correlated_inner(false, false, false, false),
            true,
        ));
        let join = assert_join(plan, JoinKind::LeftSemi);

        assert!(
            project_outputs_column(join.right.as_ref(), INNER_K),
            "right child must expose INNER_K referenced by the join ON"
        );
    }

    #[test]
    fn missing_inner_output_column_id_errors() {
        let rule = QuantifiedApplyToJoin;
        let mut ctx = ctx();
        let mut plan = in_apply(false, false, inner_scan(false, false), false);
        let missing = ColumnId(999);
        set_inner_output_column_id(&mut plan, missing);

        let err = rule
            .apply(plan, &mut ctx)
            .expect_err("missing inner output column id must error");

        assert!(err.contains("c999"), "unexpected error: {err}");
        assert!(err.contains("c3"), "unexpected error: {err}");
        assert!(err.contains("c4"), "unexpected error: {err}");
    }

    #[test]
    fn use_semi_anti_false_is_not_rewritten() {
        let rule = QuantifiedApplyToJoin;
        let mut ctx = ctx();
        let mut plan = in_apply(false, false, inner_scan(false, false), false);
        set_use_semi_anti(&mut plan, false);

        assert!(
            !rule.matches(&plan, &ctx),
            "use_semi_anti=false must not match"
        );
        let result = rule.apply(plan, &mut ctx).expect("rewrite must not error");
        assert!(matches!(result, RewriteResult::Unchanged));
    }
}
