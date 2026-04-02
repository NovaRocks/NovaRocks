//! JoinCommutativity transformation rule.
//!
//! Swaps children of a LogicalJoin: `A JOIN B` -> `B JOIN A`, adjusting the
//! join type to preserve semantics (LeftOuter <-> RightOuter, etc.).

use crate::sql::cascades::memo::{MExpr, Memo};
use crate::sql::cascades::operator::{LogicalJoinOp, Operator};
use crate::sql::cascades::rule::{NewExpr, Rule, RuleType};
use crate::sql::ir::JoinKind;

pub(crate) struct JoinCommutativity;

/// Map a join type to its commuted (left/right swapped) equivalent.
fn commute_join_kind(kind: JoinKind) -> JoinKind {
    match kind {
        JoinKind::Inner => JoinKind::Inner,
        JoinKind::Cross => JoinKind::Cross,
        JoinKind::FullOuter => JoinKind::FullOuter,
        JoinKind::LeftOuter => JoinKind::RightOuter,
        JoinKind::RightOuter => JoinKind::LeftOuter,
        JoinKind::LeftSemi => JoinKind::RightSemi,
        JoinKind::RightSemi => JoinKind::LeftSemi,
        JoinKind::LeftAnti => JoinKind::RightAnti,
        JoinKind::RightAnti => JoinKind::LeftAnti,
    }
}

impl Rule for JoinCommutativity {
    fn name(&self) -> &str {
        "JoinCommutativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalJoin(_))
    }

    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };

        // A join must have exactly two children: [left, right].
        if expr.children.len() != 2 {
            return vec![];
        }

        let left = expr.children[0];
        let right = expr.children[1];

        // Swap children and adjust the join type.
        vec![NewExpr {
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: commute_join_kind(op.join_type),
                condition: op.condition.clone(),
            }),
            children: vec![right, left],
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commute_symmetric_kinds() {
        assert_eq!(commute_join_kind(JoinKind::Inner), JoinKind::Inner);
        assert_eq!(commute_join_kind(JoinKind::Cross), JoinKind::Cross);
        assert_eq!(commute_join_kind(JoinKind::FullOuter), JoinKind::FullOuter);
    }

    #[test]
    fn commute_asymmetric_kinds() {
        assert_eq!(commute_join_kind(JoinKind::LeftOuter), JoinKind::RightOuter);
        assert_eq!(commute_join_kind(JoinKind::RightOuter), JoinKind::LeftOuter);
        assert_eq!(commute_join_kind(JoinKind::LeftSemi), JoinKind::RightSemi);
        assert_eq!(commute_join_kind(JoinKind::RightSemi), JoinKind::LeftSemi);
        assert_eq!(commute_join_kind(JoinKind::LeftAnti), JoinKind::RightAnti);
        assert_eq!(commute_join_kind(JoinKind::RightAnti), JoinKind::LeftAnti);
    }
}
