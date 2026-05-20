//! Aggregate pushdown collector/rewriter shared state.

use crate::sql::analysis::TypedExpr;
use crate::sql::planner::plan::{AggregateCall, LogicalPlan};

/// Which side of the original join receives the partial aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

/// State accumulated by the collector before producing a PushPlan.
#[derive(Clone, Debug)]
pub(crate) struct AggregatePushDownContext {
    /// Original group-by expressions from the LogicalAggregate at the
    /// top of the descent. Unchanged across the walk.
    pub original_groupby: Vec<TypedExpr>,
    /// Original aggregate calls from the top LogicalAggregate.
    pub original_aggregates: Vec<AggregateCall>,
    /// Columns required by aggregate args + group-by.
    pub required_columns: Vec<String>,
}

/// Result of a successful collector descent.
#[derive(Clone, Debug)]
pub(crate) struct PushPlan {
    /// Which side of the original join the partial aggregate wraps.
    pub side: Side,
    /// The chosen side's subtree (a `LogicalPlan::Scan` in v1).
    pub target_subtree: LogicalPlan,
    /// Group-by columns for the partial aggregate.
    pub partial_groupby: Vec<TypedExpr>,
    /// Aggregate calls to use at the partial stage. For v1 these are
    /// the same shape as the original calls (function name unchanged
    /// for SUM/MIN/MAX/COUNT — see rewriter for the final-stage table).
    pub partial_aggregates: Vec<AggregateCall>,
}
