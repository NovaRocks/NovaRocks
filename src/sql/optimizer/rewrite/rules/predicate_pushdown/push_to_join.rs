//! PushDownPredicateJoin rule wrapper.

use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::join_pushdown::{
    push_filter_predicates_through_join, push_join_condition_predicates,
};
use crate::sql::planner::plan::*;

pub(crate) struct PushDownPredicateJoin;

impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Join(_))
        ) || matches!(plan, LogicalPlan::Join(join) if join.condition.is_some())
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let LogicalPlan::Join(join) = *filter.input else {
                    return None;
                };
                let (rewritten, changed) =
                    push_filter_predicates_through_join(filter.predicate, join);
                changed.then_some(rewritten)
            }
            LogicalPlan::Join(join) => push_join_condition_predicates(join),
            _ => None,
        }
    }
}
