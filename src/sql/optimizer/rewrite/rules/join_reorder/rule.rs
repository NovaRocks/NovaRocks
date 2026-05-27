//! JoinReorderRule — query rewrite rule wrapping the DP/Greedy/LeftDeep/Heuristic
//! join reorder algorithms.
//!
//! **Convention exception.** Like PruneColumns, this rule recurses
//! internally: it takes the full plan tree, finds inner-join chains,
//! flattens them, runs cost-based reorder, and rebuilds. A generic
//! bottom-up local traversal can't express global join-graph optimization.

use std::collections::HashMap;
use std::sync::Arc;

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

    fn matches(&self, _plan: &LogicalPlan) -> bool {
        // Like PruneColumns, this rule takes the full tree and recurses
        // internally to find join chains. The driver invokes it at every
        // node bottom-up; the first invocation at the tree root does the
        // work; subsequent invocations at interior nodes are no-ops
        // (reorder_joins_cbo is idempotent on an already-reordered tree).
        true
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let before = plan.clone();
        let after = super::reorder::reorder_joins_cbo(plan, &self.table_stats);
        // Structural comparison to detect no-op.
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
                source: ScanSource::StarRocks { db_id: 0, table_id: 0 },
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
    fn single_scan_is_no_op() {
        let rule = JoinReorderRule::new(Arc::new(HashMap::new()));
        let plan = dummy_scan("t1");
        assert!(rule.matches(&plan));
        assert!(rule.apply(plan).is_none(), "single scan should be no-op");
    }
}
