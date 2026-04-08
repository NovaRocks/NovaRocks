//! RBO rewrite pass applied before Memo-based CBO search.
//!
//! Calls existing rule-based optimizer passes (predicate pushdown,
//! join reorder, column pruning) on the LogicalPlan before it enters
//! the Memo.

use std::collections::HashMap;

use crate::sql::plan::LogicalPlan;
use crate::sql::statistics::TableStatistics;

/// Apply RBO rewrites to the logical plan before Memo insertion.
pub(crate) fn rewrite(
    plan: LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> LogicalPlan {
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::join_reorder::reorder_joins_cbo(plan, table_stats);
    // Second pushdown pass: after join reorder, newly formed joins may have
    // cross-side predicates that can now be pushed into join conditions
    // (e.g., OR factoring extracting common equi-joins at a lower level).
    let plan = crate::sql::optimizer::predicate_pushdown::push_down_predicates(plan);
    let plan = crate::sql::optimizer::column_pruning::prune_columns(plan);
    plan
}
