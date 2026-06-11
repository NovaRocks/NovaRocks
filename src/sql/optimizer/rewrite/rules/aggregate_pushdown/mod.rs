//! Aggregate pushdown rule (OPT-1).
//!
//! Pushes `LogicalAggregate` past `LogicalJoin` toward leaves when cost-justified.
//! See docs/design/specs/2026-05-20-opt-1-aggregate-pushdown-design.md.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;

pub(crate) mod collector;
pub(crate) mod context;
pub(crate) mod cost;
pub(crate) mod rewriter;
pub(crate) mod rule;

pub(crate) use rule::AggregatePushdownRule;

#[allow(dead_code)]
pub(crate) fn aggregate_pushdown_rules(
    table_stats: &HashMap<String, TableStatistics>,
) -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(AggregatePushdownRule::new(Arc::new(
        table_stats.clone(),
    )))]
}
