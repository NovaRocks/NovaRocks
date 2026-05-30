//! PruneCTEConsumeColumns — Phase 2 rule for CTEConsume nodes.
//!
//! Intentionally a no-op: CTE consume output columns are NOT pruned here.
//! See the `apply` comment for the rationale (Gap-3 deferred).

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneCTEConsumeColumns;

impl LogicalRewriteRule for PruneCTEConsumeColumns {
    fn name(&self) -> &'static str {
        "PruneCTEConsumeColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::CTEConsume(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // Conservative no-op: do not prune CTE consume output columns.
        //
        // CTE column pruning (Gap-3) is deferred because the consume↔produce
        // positional mapping is fragile with multiple consumers: each consumer
        // has distinct ColumnIds but the same positional schema as the produce.
        // The coordinator sends ALL produce columns; each consumer's exchange
        // node reads them by position (idx 0, 1, ...).  If we trim a consumer
        // to fewer columns, its idx-based slot assignments no longer align with
        // the produce's column order, and the wrong data ends up in each slot
        // — causing incorrect filter evaluation or silent wrong results.
        //
        // The Phase-1 tag still helps the body below CTEProduce (via the
        // keep-all pass in tag_cte_anchor → tag_cte_produce → body), so leaf
        // scans still benefit from column pruning.  CTE node pruning is a
        // follow-up optimization (Gap-3).
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::CTEConsumeNode;
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    /// PruneCTEConsumeColumns is a conservative no-op (Gap-3 deferred).
    /// Even when required_output_columns is set to a strict subset, the rule
    /// must return Unchanged so the produce/consume positional alignment is
    /// preserved.
    #[test]
    fn prune_cte_consume_is_noop_even_when_subset_tagged() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_b);

        let node = CTEConsumeNode {
            cte_id: 1,
            alias: "cte1".to_string(),
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_c, "c"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        // Must be Unchanged — CTE consume pruning is a no-op (Gap-3).
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "CTE consume must not be pruned (positional alignment with produce must be preserved)"
        );
    }

    #[test]
    fn prune_cte_consume_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let node = CTEConsumeNode {
            cte_id: 2,
            alias: "cte2".to_string(),
            output_columns: vec![make_output_column(id_a, "a")],
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_cte_consume_noop_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let node = CTEConsumeNode {
            cte_id: 3,
            alias: "cte3".to_string(),
            output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::CTEConsume(node);
        let rule = PruneCTEConsumeColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        // Even with an empty needed set, the no-op rule returns Unchanged.
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "CTE consume must not be pruned even with empty needed set (Gap-3 no-op)"
        );
    }
}
