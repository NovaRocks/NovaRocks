//! PruneTableFunctionColumns — Phase 2 rule for TableFunction nodes.
//!
//! This is a documented NO-OP. The TableFunction node was assigned keep-all-child
//! semantics by the Phase-1 tagging pass: all output columns (both input pass-through
//! columns and the lateral table function result columns) are treated as required.
//! Pruning lateral table function outputs would require re-evaluating which
//! function arguments and output slots are actually used, which is deferred.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct PruneTableFunctionColumns;

impl LogicalRewriteRule for PruneTableFunctionColumns {
    fn name(&self) -> &'static str {
        "PruneTableFunctionColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::TableFunction(_))
    }

    fn apply(
        &self,
        _plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        // No-op: TableFunction was assigned keep-all-child semantics by the
        // Phase-1 tagging pass. Kept for architectural symmetry + per-operator
        // disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::plan::{TableFunctionNode, ValuesNode};
    use arrow::datatypes::DataType;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_table_function_is_always_unchanged() {
        let node = TableFunctionNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            function_name: "generate_series".to_string(),
            args: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            alias: None,
            is_left_join: false,
            required_output_columns: None,
        };

        let plan = LogicalPlan::TableFunction(node);
        let rule = PruneTableFunctionColumns;

        // matches the right variant
        assert!(rule.matches(&plan, &ctx()));

        // apply always returns Unchanged
        let result = rule.apply(plan, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneTableFunctionColumns must always return Unchanged"
        );
    }
}
