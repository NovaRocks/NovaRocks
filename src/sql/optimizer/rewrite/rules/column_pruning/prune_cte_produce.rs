//! PruneCTEProduceColumns — Phase 2 rule for CTEProduce nodes.
//!
//! Intentionally a no-op: CTE produce output columns are NOT pruned here.
//! See the `apply` comment for the rationale (Gap-3 deferred).

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::*;

pub(crate) struct PruneCTEProduceColumns;

impl LogicalRewriteRule for PruneCTEProduceColumns {
    fn name(&self) -> &'static str {
        "PruneCTEProduceColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::CTEProduce(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // Conservative no-op: do not prune CTE produce output columns.
        //
        // The CTE produce fragment multicasts ALL its output columns to every
        // consumer exchange node.  Each consumer reads columns by position
        // (idx 0, 1, …).  Trimming the produce's output_columns would shift
        // positions and mis-align consumer slots, producing incorrect results.
        //
        // The body BELOW the CTEProduce node (e.g. a Scan) is still tagged by
        // tag_cte_anchor (which passes None / keep-all to the produce body via
        // tag_cte_produce → tag_required_columns), so leaf-level column
        // pruning still fires correctly.  Only the produce's own output_columns
        // list is left intact here.  CTE produce pruning (Gap-3) is deferred
        // to a follow-up task that will also align consumer slot assignments.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{CTEProduceNode, ValuesNode};
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

    fn dummy_input() -> Box<LogicalPlan> {
        Box::new(LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        }))
    }

    /// PruneCTEProduceColumns is a conservative no-op (Gap-3 deferred).
    /// Even when required_output_columns is set to a strict subset, the rule
    /// must return Unchanged so the produce fragment continues to send all
    /// columns and consumer positional alignment is preserved.
    #[test]
    fn prune_cte_produce_is_noop_even_when_subset_tagged() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_c);

        let node = CTEProduceNode {
            cte_id: 42,
            input: dummy_input(),
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_c, "c"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::CTEProduce(node);
        let rule = PruneCTEProduceColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        // Must be Unchanged — CTE produce pruning is a no-op (Gap-3).
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "CTE produce must not be pruned (consumer positional alignment must be preserved)"
        );
    }

    #[test]
    fn prune_cte_produce_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let node = CTEProduceNode {
            cte_id: 5,
            input: dummy_input(),
            output_columns: vec![make_output_column(id_a, "a")],
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::CTEProduce(node);
        let rule = PruneCTEProduceColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_cte_produce_noop_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let node = CTEProduceNode {
            cte_id: 7,
            input: dummy_input(),
            output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::CTEProduce(node);
        let rule = PruneCTEProduceColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        // Even with an empty needed set, the no-op rule returns Unchanged.
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "CTE produce must not be pruned even with empty needed set (Gap-3 no-op)"
        );
    }
}
