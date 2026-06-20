//! JoinAssociativity transformation rule.
//!
//! Re-associates inner joins: `(A JOIN B) JOIN C` -> `A JOIN (B JOIN C)`.
//!
//! Only applies when both the outer and inner joins are INNER joins, AND
//! when reusing the original conditions in their new positions is sound:
//! the inner_op.condition (originally over A∪B) must reference only
//! columns from B in its new position over (B JOIN C). If it references
//! any column from A, we cannot reuse it without redistribution and the
//! rewrite is skipped. Full predicate re-association across the rewrite
//! is a future improvement.

use crate::sql::common::JoinKind;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalJoinOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

use crate::sql::optimizer::rewrite::rules::utils::collect_scalar_column_id_refs_strict;

use super::implement::get_group_column_ids;

pub(crate) struct JoinAssociativity;

impl Rule for JoinAssociativity {
    fn name(&self) -> &str {
        "JoinAssociativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                ..
            })
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(outer_op) = &expr.op else {
            return vec![];
        };

        // Outer join must be INNER.
        if outer_op.join_type != JoinKind::Inner {
            return vec![];
        }

        // Must have two children: child[0] = inner join group, child[1] = C.
        if expr.children.len() != 2 {
            return vec![];
        }

        let inner_group_id = expr.children[0];
        let c_group = expr.children[1];

        // Check if the inner group contains a LogicalJoin(Inner) expression.
        let inner_group = &memo.groups[inner_group_id];
        let inner_join = inner_group.logical_exprs.iter().find(|e| {
            matches!(
                &e.op,
                Operator::LogicalJoin(LogicalJoinOp {
                    join_type: JoinKind::Inner,
                    ..
                })
            )
        });

        let Some(inner_expr) = inner_join else {
            return vec![];
        };

        // inner_expr represents LogicalJoin(A, B) with INNER join.
        if inner_expr.children.len() != 2 {
            return vec![];
        }

        let a_group = inner_expr.children[0];
        let b_group = inner_expr.children[1];

        let inner_op = match &inner_expr.op {
            Operator::LogicalJoin(op) => op,
            _ => return vec![],
        };

        // Soundness gate: the new inner join (B JOIN C) reuses inner_op.condition
        // verbatim, so that condition must reference only columns available in
        // (B ∪ C). Originally inner_op.condition was over (A ∪ B); if it
        // references any column from A, A is no longer in the inner join's
        // scope after re-association, and reusing the condition would either
        // panic the fragment builder (column-not-resolvable) or silently
        // produce wrong rows. Skip the rewrite in that case rather than emit
        // an unsound plan. A future improvement would split the condition by
        // conjunct and re-distribute across the new structure.
        if let Some(ref cond) = inner_op.condition {
            let Some(cond_ids) = collect_scalar_column_id_refs_strict(&memo.scalars, *cond) else {
                return vec![];
            };
            let a_ids = get_group_column_ids(memo, a_group);
            let b_ids = get_group_column_ids(memo, b_group);
            let c_ids = get_group_column_ids(memo, c_group);
            let bc_ids: std::collections::HashSet<_> = b_ids.union(&c_ids).copied().collect();
            // Only fire if every column the condition references is available
            // in B ∪ C, and at least one column also lies in B (otherwise the
            // condition is purely over A, which is even more clearly wrong).
            let refs_only_bc = cond_ids.iter().all(|id| bc_ids.contains(id));
            let refs_any_a = cond_ids
                .iter()
                .any(|id| a_ids.contains(id) && !bc_ids.contains(id));
            if !refs_only_bc || refs_any_a {
                return vec![];
            }
        }

        // Produce: A JOIN_outer (B JOIN_inner C)
        //
        // The outer join's condition (originally over (A∪B)∪C) is reused on
        // the new outer (A JOIN BC), which has the same combined scope; that
        // is always safe.
        // The inner join's condition is reused on the new inner (B JOIN C);
        // soundness was checked above.

        // Create the new inner join group: B JOIN C
        let new_inner_join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: inner_op.condition.clone(),
            }),
            children: vec![b_group, c_group],
        };
        let new_inner_group = memo.new_group(new_inner_join);

        // New outer join: A JOIN (B JOIN C)
        vec![NewExpr {
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: outer_op.condition.clone(),
            }),
            children: vec![a_group, new_inner_group],
        }]
    }
}
