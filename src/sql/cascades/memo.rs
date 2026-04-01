//! Memo table for the Cascades optimizer.
//!
//! The Memo stores equivalence classes (Groups) of expressions (MExprs).
//! Each MExpr holds an Operator and references its children as GroupIds.

use super::operator::Operator;
use crate::sql::ir::OutputColumn;

// ---------------------------------------------------------------------------
// Core type aliases
// ---------------------------------------------------------------------------

pub(crate) type GroupId = usize;
pub(crate) type MExprId = usize;
pub(crate) type Cost = f64;

// ---------------------------------------------------------------------------
// Memo
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Memo {
    pub(crate) groups: Vec<Group>,
}

impl Memo {
    pub(crate) fn new() -> Self {
        Self {
            groups: Vec::new(),
        }
    }

    /// Create a new group containing a single expression. Returns the new GroupId.
    pub(crate) fn new_group(&mut self, expr: MExpr) -> GroupId {
        let id = self.groups.len();
        let is_physical = expr.op.is_physical();
        let mut group = Group {
            id,
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_props: None,
        };
        if is_physical {
            group.physical_exprs.push(expr);
        } else {
            group.logical_exprs.push(expr);
        }
        self.groups.push(group);
        id
    }

    /// Add an alternative expression to an existing group.
    pub(crate) fn add_expr_to_group(&mut self, group_id: GroupId, expr: MExpr) {
        let group = &mut self.groups[group_id];
        if expr.op.is_physical() {
            group.physical_exprs.push(expr);
        } else {
            group.logical_exprs.push(expr);
        }
    }

    /// Return the next globally unique MExprId (total count of all exprs).
    pub(crate) fn next_expr_id(&self) -> MExprId {
        self.groups
            .iter()
            .map(|g| g.logical_exprs.len() + g.physical_exprs.len())
            .sum()
    }
}

// ---------------------------------------------------------------------------
// Group
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) logical_exprs: Vec<MExpr>,
    pub(crate) physical_exprs: Vec<MExpr>,
    /// Logical properties derived from the first logical expression.
    pub(crate) logical_props: Option<LogicalProperties>,
}

// ---------------------------------------------------------------------------
// Logical properties
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct LogicalProperties {
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) row_count: f64,
}

// ---------------------------------------------------------------------------
// MExpr (memo expression)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct MExpr {
    pub(crate) id: MExprId,
    pub(crate) op: Operator,
    pub(crate) children: Vec<GroupId>,
}
