//! Aggregate pushdown collector/rewriter shared state.

use crate::sql::optimizer::operator::ScalarAggregateSpec;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::ScalarId;

pub(crate) type ColumnRefIdentity = (Option<String>, String);

/// Which side of the original join receives the partial aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

/// State accumulated by the collector before producing a PushPlan.
#[derive(Clone, Debug)]
pub(crate) struct AggregatePushDownContext {
    /// Original group-by ScalarIds from the LogicalAggregateOp at the
    /// top of the descent. Unchanged across the walk.
    pub original_groupby: Vec<ScalarId>,
    /// Original aggregate specs from the top LogicalAggregateOp.
    pub original_aggregates: Vec<ScalarAggregateSpec>,
    /// Qualified columns required by aggregate args + group-by.
    pub required_column_refs: Vec<ColumnRefIdentity>,
}

/// Result of a successful collector descent.
#[derive(Clone, Debug)]
pub(crate) struct PushPlan {
    /// Which side of the original join the partial aggregate wraps.
    pub side: Side,
    /// The chosen side's subtree (a `Operator::LogicalScan` in v1).
    pub target_subtree: OptExpr,
    /// Group-by ScalarIds for the partial aggregate.
    pub partial_groupby: Vec<ScalarId>,
    /// Side-bound join keys that must become partial group-by expressions but
    /// were discovered from join predicates rather than existing aggregate
    /// group-by ScalarIds.
    pub partial_extra_groupby: Vec<ScalarId>,
    /// Aggregate specs to use at the partial stage. For v1 these are
    /// the same shape as the original specs (function name unchanged
    /// for SUM/MIN/MAX/COUNT — see rewriter for the final-stage table).
    pub partial_aggregates: Vec<ScalarAggregateSpec>,
}
