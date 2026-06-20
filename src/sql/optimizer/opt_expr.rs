//! `OptExpr` — the optimizer's concrete logical operator tree.
//!
//! Mirrors StarRocks `OptExpression`: an `Operator` payload plus child
//! `OptExpr`s. Scalars inside the operator are already interned `ScalarId`
//! handles into the owning `ScalarArena`. This is the tree the RBO rewrite
//! phase operates on; `memo_copy::opt_expr_to_memo` copies it into the Memo for
//! CBO.

use std::collections::HashSet;

use super::operator::Operator;
use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug)]
pub(crate) struct OptExpr {
    pub op: Operator,
    pub children: Vec<OptExpr>,
    /// Mirrors `LogicalPlanNode.required_output_columns` — the column-pruning
    /// annotation that rewrite rules read and propagate. `None` means all
    /// columns are required. Carried through Bridge 1; ignored by copy-in
    /// (the Memo does not use it).
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

impl OptExpr {
    pub(crate) fn new(op: Operator, children: Vec<OptExpr>) -> Self {
        Self {
            op,
            children,
            required_output_columns: None,
        }
    }

    pub(crate) fn leaf(op: Operator) -> Self {
        Self {
            op,
            children: Vec::new(),
            required_output_columns: None,
        }
    }

    pub(crate) fn child(&self, index: usize) -> &OptExpr {
        &self.children[index]
    }

    pub(crate) fn unary_input(&self) -> &OptExpr {
        &self.children[0]
    }

    pub(crate) fn left(&self) -> &OptExpr {
        &self.children[0]
    }

    pub(crate) fn right(&self) -> &OptExpr {
        &self.children[1]
    }
}
