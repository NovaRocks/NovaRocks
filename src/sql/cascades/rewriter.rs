//! RBO rewrite pass applied before Memo-based CBO search.
//!
//! Calls existing rule-based optimizer passes (predicate pushdown,
//! column pruning) on the LogicalPlan before it enters the Memo.

use crate::sql::plan::LogicalPlan;

/// Apply RBO rewrites to the logical plan before Memo insertion.
pub(crate) fn rewrite(plan: LogicalPlan) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::column_pruning::prune_columns(plan);
    plan
}
