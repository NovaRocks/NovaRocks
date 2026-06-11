//! SubqueryRewrite stage rules.
//!
//! M0 ships only the ApplyException terminal guard. The decorrelation rules
//! (push-down normalization, ApplyToWindow, *ApplyToJoin) land with M1+; see
//! docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §6.

mod apply_exception;

pub(crate) use apply_exception::ApplyException;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(ApplyException)]
}

/// Non-disableable backstop used by `optimize()`: returns the ApplyException
/// error if any Apply survived the rewrite pipeline (possible when the user
/// disabled the ApplyException rule via `disable_optimizer_rules`); a leaked
/// Apply must surface as a user-readable error, never as the memo-conversion
/// panic.
pub(crate) fn find_residual_apply(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Apply(node) => Some(apply_exception::apply_exception_message(node)),
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => None,
        LogicalPlan::Filter(n) => find_residual_apply(&n.input),
        LogicalPlan::Project(n) => find_residual_apply(&n.input),
        LogicalPlan::Aggregate(n) => find_residual_apply(&n.input),
        LogicalPlan::Join(n) => {
            find_residual_apply(&n.left).or_else(|| find_residual_apply(&n.right))
        }
        LogicalPlan::Sort(n) => find_residual_apply(&n.input),
        LogicalPlan::Limit(n) => find_residual_apply(&n.input),
        LogicalPlan::Union(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Intersect(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::Except(n) => n.inputs.iter().find_map(find_residual_apply),
        LogicalPlan::TableFunction(n) => find_residual_apply(&n.input),
        LogicalPlan::Window(n) => find_residual_apply(&n.input),
        LogicalPlan::Repeat(n) => find_residual_apply(&n.input),
        LogicalPlan::CTEAnchor(n) => {
            find_residual_apply(&n.produce).or_else(|| find_residual_apply(&n.consumer))
        }
        LogicalPlan::CTEProduce(n) => find_residual_apply(&n.input),
        LogicalPlan::Decode(n) => find_residual_apply(&n.input),
        LogicalPlan::AggregateStateMerge(n) => {
            find_residual_apply(&n.old_input).or_else(|| find_residual_apply(&n.delta_input))
        }
        LogicalPlan::AssertOneRow(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvDelta(n) => find_residual_apply(&n.input),
        LogicalPlan::ImvVersion(n) => find_residual_apply(&n.input),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::planner::plan::{ApplyKind, ApplyNode, LimitNode, ValuesNode};

    fn apply_over_values() -> LogicalPlan {
        let values = || {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        };
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(values()),
            right: Box::new(values()),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    #[test]
    fn apply_exception_fails_residual_apply() {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let err = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect_err("residual apply must fail the pipeline");
        assert!(
            err.contains("subquery decorrelation failed"),
            "unexpected error: {err}"
        );
        assert!(err.contains("kind=Scalar"), "unexpected error: {err}");
    }

    #[test]
    fn disabled_apply_exception_is_caught_by_backstop() {
        let mut ctx = RewriteContext::for_query(vec!["ApplyException".to_string()]);
        let rewritten = query_rewrite_pipeline(&HashMap::new())
            .rewrite(apply_over_values(), &mut ctx)
            .expect("pipeline passes with the rule disabled");
        let message = find_residual_apply(&rewritten).expect("backstop must detect the apply");
        assert!(message.contains("subquery decorrelation failed"));
    }

    #[test]
    fn find_residual_apply_ignores_plain_plans() {
        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        });
        assert!(find_residual_apply(&plan).is_none());
    }

    #[test]
    fn find_residual_apply_finds_apply_nested_under_unary() {
        // Apply one level below a unary container: exercises the walker's
        // recursive descent, not just the root case.
        let plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(apply_over_values()),
            limit: Some(1),
            offset: None,
            required_output_columns: None,
        });
        let message = find_residual_apply(&plan).expect("walker must find the nested apply");
        assert!(
            message.contains("subquery decorrelation failed"),
            "unexpected message: {message}"
        );
    }
}
