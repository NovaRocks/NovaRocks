//! PruneIntersectColumns — Phase 2 rule for Intersect nodes.
//!
//! Filters `IntersectNode.output_columns` to only those whose `column_id`
//! is in `required_output_columns`. Keeps at least one column to preserve
//! a valid output schema (Gap 4).
//!
//! Only the set-op node's own `output_columns` list is touched. Branch inputs
//! are NOT modified here — the Phase-1 tagging pass has already tagged each
//! branch with the position-restricted required set, and the branches' own
//! prune rules handle their pruning independently. Because output and branch
//! schemas align by position, dropping a position here stays consistent with
//! the branches after the full rule set runs.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning::keep_at_least_one;
use crate::sql::planner::plan::*;

pub(crate) struct PruneIntersectColumns;

impl LogicalRewriteRule for PruneIntersectColumns {
    fn name(&self) -> &'static str {
        "PruneIntersectColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Intersect(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Intersect(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.output_columns.len();

        // Determine which ids to keep.
        let filtered: HashSet<ColumnId> = node
            .output_columns
            .iter()
            .map(|c| c.column_id)
            .filter(|id| needed.contains(id))
            .collect();

        // Ensure at least one column survives.
        let fallback = node
            .output_columns
            .first()
            .map(|c| c.column_id)
            .unwrap_or(ColumnId::UNSET);
        let keep_ids = keep_at_least_one(filtered, fallback);

        let new_output_columns: Vec<_> = node
            .output_columns
            .into_iter()
            .filter(|c| keep_ids.contains(&c.column_id))
            .collect();

        if new_output_columns.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        node.output_columns = new_output_columns;
        Ok(RewriteResult::Changed(LogicalPlan::Intersect(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{IntersectNode, ValuesNode};
    use arrow::datatypes::DataType;

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

    fn dummy_input() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })
    }

    #[test]
    fn prune_intersect_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        needed.insert(id_c);

        let node = IntersectNode {
            inputs: vec![dummy_input(), dummy_input()],
            output_columns: vec![
                make_output_column(id_a, "a"),
                make_output_column(id_b, "b"),
                make_output_column(id_c, "c"),
            ],
            required_output_columns: Some(needed),
        };

        let plan = LogicalPlan::Intersect(node);
        let rule = PruneIntersectColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Intersect(pruned) = changed else {
            panic!("expected Intersect");
        };

        assert_eq!(pruned.output_columns.len(), 2);
        assert_eq!(pruned.output_columns[0].column_id, id_a);
        assert_eq!(pruned.output_columns[1].column_id, id_c);
        // inputs are untouched
        assert_eq!(pruned.inputs.len(), 2);
    }

    #[test]
    fn prune_intersect_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);

        let node = IntersectNode {
            inputs: vec![dummy_input()],
            output_columns: vec![make_output_column(id_a, "a")],
            required_output_columns: None, // not tagged
        };

        let plan = LogicalPlan::Intersect(node);
        let rule = PruneIntersectColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_intersect_keeps_at_least_one_when_needed_empty() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        // needed is empty — must keep first column.
        let node = IntersectNode {
            inputs: vec![dummy_input()],
            output_columns: vec![make_output_column(id_a, "a"), make_output_column(id_b, "b")],
            required_output_columns: Some(HashSet::new()),
        };

        let plan = LogicalPlan::Intersect(node);
        let rule = PruneIntersectColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Intersect(pruned) = changed else {
            panic!("expected Intersect");
        };

        assert_eq!(pruned.output_columns.len(), 1);
        assert_eq!(pruned.output_columns[0].column_id, id_a, "first col kept");
    }
}
