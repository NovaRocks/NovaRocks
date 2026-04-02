//! JoinAssociativity transformation rule.
//!
//! Re-associates inner joins: `(A JOIN B) JOIN C` -> `A JOIN (B JOIN C)`.
//!
//! Only applies when both the outer and inner joins are INNER joins.
//! In Phase 2+3 the conditions are kept on the outer join without
//! redistribution; full predicate re-association is a future optimization.

use crate::sql::cascades::memo::{MExpr, Memo};
use crate::sql::cascades::operator::{LogicalJoinOp, Operator};
use crate::sql::cascades::rule::{NewExpr, Rule, RuleType};
use crate::sql::ir::JoinKind;

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

        // Produce: A JOIN_outer (B JOIN_inner C)
        //
        // Phase 2+3 simplification: the inner join's condition moves to the new
        // inner join (B JOIN C), and the outer join's condition stays on the
        // outer join (A JOIN ...).  Full condition re-distribution across the
        // new structure is a future improvement.

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
