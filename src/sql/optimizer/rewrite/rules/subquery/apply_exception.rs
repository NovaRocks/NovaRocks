//! Terminal guard of the SubqueryRewrite stage: any Apply node still present
//! after the decorrelation rules means the subquery shape is unsupported.

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{ApplyNode, LogicalPlan};

pub(crate) struct ApplyException;

impl LogicalRewriteRule for ApplyException {
    fn name(&self) -> &'static str {
        "ApplyException"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match &plan {
            LogicalPlan::Apply(node) => Err(apply_exception_message(node)),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

pub(super) fn apply_exception_message(node: &ApplyNode) -> String {
    format!(
        "subquery decorrelation failed: a residual Apply node (kind={:?}, correlated={}) \
         survived the SubqueryRewrite stage; this subquery shape is not yet supported",
        node.kind,
        !node.correlation_column_ids.is_empty()
    )
}
