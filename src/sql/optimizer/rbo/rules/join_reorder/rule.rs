//! JoinReorderRule — RBO rule wrapping the DP/Greedy/LeftDeep/Heuristic
//! join reorder algorithms.
//!
//! **Convention exception.** Like PruneColumns, this rule recurses
//! internally: it takes the full plan tree, finds inner-join chains,
//! flattens them, runs cost-based reorder, and rebuilds. The RBO driver's
//! bottom-up traversal can't express global join-graph optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rbo::rule::RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

/// Wraps `reorder_joins_cbo` as a RewriteRule.
///
/// Stores `table_stats` internally (set at construction time by
/// `all_rbo_rules(table_stats)`).
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
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::planner::plan::ScanNode;
    use arrow::datatypes::DataType;

    fn dummy_scan(name: &str) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
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
