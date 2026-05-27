//! JoinReorderRule — query rewrite rule wrapping the DP/Greedy/LeftDeep/Heuristic
//! join reorder algorithms.
//!
//! **Convention exception.** Like PruneColumns, this rule recurses
//! internally: it takes the full plan tree, finds inner-join chains,
//! flattens them, runs cost-based reorder, and rebuilds. A generic
//! bottom-up local traversal can't express global join-graph optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

/// Wraps `reorder_joins_cbo` as a RewriteRule.
///
/// Stores `table_stats` internally, set at construction time by the rewrite
/// pipeline's `JoinReorder` stage.
#[allow(dead_code)]
pub(crate) struct JoinReorderRule {
    table_stats: Arc<HashMap<String, TableStatistics>>,
}

impl JoinReorderRule {
    #[allow(dead_code)]
    pub(crate) fn new(table_stats: Arc<HashMap<String, TableStatistics>>) -> Self {
        Self { table_stats }
    }
}

impl RewriteRule for JoinReorderRule {
    fn name(&self) -> &'static str {
        "JoinReorder"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        // The rule does its own internal traversal (`reorder_joins_cbo`
        // recursively walks the subtree to find inner-join chains, build
        // a JoinGraph, and rewrite the chain). The pipeline driver,
        // independently, visits *every* node bottom-up.
        //
        // If we returned `true` everywhere the way we used to, every
        // Project / Filter / SubqueryAlias / Scan / CTEAnchor / ... node
        // re-ran the full subtree walk + Debug-format no-op check, even
        // though the framework's recursion has already processed those
        // children. Profiling TPC-DS q14 step 2 showed ~1200 invocations
        // adding up to ~7.8 s of redundant walks (75 % of the 10 s
        // optimizer budget) before the actual join chains could finish
        // reordering — surfacing as `optimizer timeout during JoinReorder`.
        //
        // Restrict matching to the nodes where `apply` can actually do
        // useful work:
        //
        //   * Inner/Cross JOIN — the chain root the rule reorders.
        //   * Filter sitting *directly* on top of an inner/cross JOIN —
        //     `flatten_inner_joins` absorbs the Filter's predicate into
        //     the chain (so a Filter that's been hoisted above a join
        //     chain still gets its predicate folded into the join graph).
        //
        // Children of any other node are still visited by the framework's
        // bottom-up traversal, so the chains they contain do get
        // reordered through their own Inner/Cross JOIN matches.
        match plan {
            LogicalPlan::Join(j) if matches!(j.join_type, JoinKind::Inner | JoinKind::Cross) => {
                true
            }
            LogicalPlan::Filter(f) => matches!(
                f.input.as_ref(),
                LogicalPlan::Join(j) if matches!(j.join_type, JoinKind::Inner | JoinKind::Cross)
            ),
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let before = plan.clone();
        let after = super::reorder::reorder_joins_cbo(plan, &self.table_stats);
        // Structural comparison to detect no-op. `matches` has already
        // narrowed us to nodes where reorder *could* fire; this check
        // catches the (common) case where the chain is already in
        // cheapest order, so the rewrite pipeline can converge instead
        // of spinning until `rewrite_max_iterations`.
        if format!("{:?}", before) == format!("{:?}", after) {
            None
        } else {
            Some(after)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::ScanNode;
    use arrow::datatypes::DataType;

    fn dummy_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks,
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
        })
    }

    #[test]
    fn matches_rejects_non_join_nodes() {
        // `matches` must skip non-Join nodes so the pipeline's bottom-up
        // traversal doesn't trigger a full `reorder_joins_cbo` walk at
        // every Project / Filter / Scan / SubqueryAlias / … in the plan.
        // The previous "always match + idempotent no-op" design was
        // O(N × subtree) per node visit and burned the entire optimizer
        // budget on plans with deeply-nested CTEs (see TPC-DS q14 step 2).
        let rule = JoinReorderRule::new(Arc::new(HashMap::new()));
        let scan = dummy_scan("t1");
        assert!(
            !rule.matches(&scan),
            "single scan should not match the rule"
        );
    }
}
