use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

#[derive(Default)]
pub(crate) struct VariantPathPushdownRule;

impl PlanRewriteRule for VariantPathPushdownRule {
    fn name(&self) -> &'static str {
        "VariantPathPushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, _plan: &LogicalPlan) -> bool {
        false
    }

    fn apply(&self, _plan: LogicalPlan) -> Option<LogicalPlan> {
        None
    }
}
