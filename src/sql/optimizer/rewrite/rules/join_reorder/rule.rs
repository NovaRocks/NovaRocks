//! JoinReorderRule — query rewrite rule wrapping the DP/Greedy/LeftDeep/Heuristic
//! join reorder algorithms.
//!
//! **Convention exception.** Like PruneColumns, this rule recurses
//! internally: it takes the full plan tree, finds inner-join chains,
//! flattens them, runs cost-based reorder, and rebuilds. A generic
//! bottom-up local traversal can't express global join-graph optimization.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
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

impl LogicalRewriteRule for JoinReorderRule {
    fn name(&self) -> &'static str {
        "JoinReorder"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
        !ctx.join_reorder_global_applied()
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        ctx.mark_join_reorder_global_applied();
        let after = super::reorder::reorder_joins_cbo(plan, &self.table_stats);
        Ok(RewriteResult::Changed(after))
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
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        })
    }

    #[test]
    fn matches_allows_initial_global_pass() {
        // JoinReorder is an internally-recursive global pass. It must match
        // the first stage-root visit, even when that root is not itself a Join.
        let rule = JoinReorderRule::new(Arc::new(HashMap::new()));
        let ctx = RewriteContext::for_query(Vec::<String>::new());
        let scan = dummy_scan("t1");
        assert!(
            rule.matches(&scan, &ctx),
            "the global rule should match once at the stage root"
        );
    }

    #[test]
    fn matches_rejects_after_global_pass_applied() {
        let rule = JoinReorderRule::new(Arc::new(HashMap::new()));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.mark_join_reorder_global_applied();
        let scan = dummy_scan("t1");
        assert!(
            !rule.matches(&scan, &ctx),
            "descendant visits should not re-run global join reorder"
        );
    }
}
