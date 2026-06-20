//! Memo table for the Cascades optimizer.
//!
//! The Memo stores equivalence classes (Groups) of expressions (MExprs).
//! Each MExpr holds an Operator and references its children as GroupIds.

use std::collections::{HashMap, HashSet};

use super::operator::{LogicalJoinOp, Operator};
use super::property::{ColumnIdSet, EquivalenceClasses};
use super::statistics::{ColumnStatistic, Confidence};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::common::CteId;
use crate::sql::common::OutputColumn;
use crate::sql::optimizer::scalar::ScalarArena;

// ---------------------------------------------------------------------------
// Core type aliases
// ---------------------------------------------------------------------------

pub(crate) type GroupId = usize;
pub(crate) type MExprId = usize;
pub(crate) type Cost = f64;

/// A candidate join order produced by the reorder enumerator, expressed over
/// existing memo groups (leaves) and new join operators (internal nodes).
/// [`crate::sql::optimizer::stats::copy_in_join_tree`] materializes it
/// bottom-up into the memo.
#[derive(Clone, Debug)]
pub(crate) enum JoinTree {
    /// An existing memo group — an "atom" of the flattened join chain.
    Leaf(GroupId),
    /// A join of two subtrees under the given logical join operator.
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
        op: LogicalJoinOp,
    },
}

// ---------------------------------------------------------------------------
// Memo
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Memo {
    pub(crate) groups: Vec<Group>,
    /// Mapping from CTE id to the GroupId of the CTEProduce node.
    /// Populated during plan-to-memo conversion so that CTEConsume
    /// nodes can look up the produce group's row count.
    pub(crate) cte_produce_groups: HashMap<CteId, GroupId>,
    /// Column metadata factory threaded from the analyzer. Used by
    /// optimizer rules and codegen to look up column names, types,
    /// and qualifiers from [`ColumnId`]s in distribution specs,
    /// sort keys, and equivalence classes.
    pub(crate) factory: ColumnRefFactory,
    /// Deduplication index for join groups materialized by
    /// [`crate::sql::optimizer::stats::copy_in_join_tree`]. Maps a structural
    /// key `(op debug string, child group ids)` to the existing group, so that
    /// multiple reorder candidates sharing intermediate sub-joins reuse one
    /// group instead of minting duplicates (StarRocks `Memo.groupExpressions`).
    pub(crate) join_group_index: HashMap<(String, Vec<GroupId>), GroupId>,
    /// Join groups that the in-memo reorder pass took ownership of (chains
    /// larger than the exhaustive threshold, for which it injected
    /// multi-candidate orders). `explore` skips `JoinAssociativity` on these so
    /// it does not redundantly re-enumerate orders the reorder pass already
    /// produced (D2: reorder/associativity mutual exclusion).
    pub(crate) reorder_owned_groups: HashSet<GroupId>,
    /// Owns interned scalar expressions for this optimize() call. After M1,
    /// memo operators store only `ScalarId` handles into this arena instead of
    /// owning deep `TypedExpr` trees.
    pub(crate) scalars: ScalarArena,
}

impl Memo {
    pub(crate) fn new() -> Self {
        Self {
            groups: Vec::new(),
            cte_produce_groups: HashMap::new(),
            factory: ColumnRefFactory::new(),
            join_group_index: HashMap::new(),
            reorder_owned_groups: HashSet::new(),
            scalars: ScalarArena::new(),
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
    #[allow(dead_code)]
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
    pub(crate) row_count_confidence: Confidence,
    pub(crate) column_statistics: HashMap<ColumnId, ColumnStatistic>,
    pub(crate) equivalence_classes: EquivalenceClasses,
    pub(crate) unique_columns: Vec<ColumnIdSet>,
}

impl LogicalProperties {
    pub(crate) fn new(output_columns: Vec<OutputColumn>, row_count: f64) -> Self {
        Self {
            output_columns,
            row_count,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
            equivalence_classes: EquivalenceClasses::default(),
            unique_columns: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// MExpr (memo expression)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct MExpr {
    #[allow(dead_code)]
    pub(crate) id: MExprId,
    pub(crate) op: Operator,
    pub(crate) children: Vec<GroupId>,
}
