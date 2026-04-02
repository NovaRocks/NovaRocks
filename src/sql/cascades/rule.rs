//! Rule trait for the Cascades optimizer.
//!
//! Rules transform or implement expressions in the Memo. Transformation rules
//! produce logically equivalent alternatives; implementation rules map logical
//! operators to their physical counterparts.

use super::memo::{GroupId, MExpr, Memo};
use super::operator::Operator;

// ---------------------------------------------------------------------------
// Rule types
// ---------------------------------------------------------------------------

pub(crate) enum RuleType {
    Transformation,
    Implementation,
}

/// A new expression to add to a group.
pub(crate) struct NewExpr {
    pub op: Operator,
    pub children: Vec<GroupId>,
}

// ---------------------------------------------------------------------------
// Rule trait
// ---------------------------------------------------------------------------

pub(crate) trait Rule: Send + Sync {
    fn name(&self) -> &str;
    fn rule_type(&self) -> RuleType;
    /// Returns true if this rule can apply to the given operator.
    fn matches(&self, op: &Operator) -> bool;
    /// Produce alternative expressions for the given MExpr.
    ///
    /// Takes `&mut Memo` so that rules creating intermediate groups (e.g. two-phase
    /// aggregation) can allocate new groups for their internal structure.
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr>;
}
