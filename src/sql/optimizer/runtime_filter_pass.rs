//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `PhysicalPlanNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::operator::JoinDistribution;

/// The optimizer-layer name used by `SET disable_optimizer_rules`.
pub(crate) const RUNTIME_FILTER_RULE: &str = "RuntimeFilterPushDown";

/// Build-side runtime filter produced by a hash join (one per equi-conjunct
/// that survives gating + push-down).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterDesc {
    pub filter_id: i32,
    /// Build-side key expression (eq.right, in build-child column space).
    pub build_expr: TypedExpr,
    /// Probe-side key expression (eq.left), in the target node's column space.
    pub probe_expr: TypedExpr,
    /// Index into the join's `eq_conditions`.
    pub expr_order: usize,
    /// Join distribution; drives thrift build_join_mode + layout.
    pub distribution: JoinDistribution,
    /// Estimated build-side row count (for thrift build_cardinality / debug).
    pub build_cardinality: f64,
}

/// Probe-side runtime filter consumed by a node (scan or intermediate).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterProbe {
    pub filter_id: i32,
    /// Probe key expression in this node's column space.
    pub probe_expr: TypedExpr,
}

#[cfg(test)]
impl RuntimeFilterDesc {
    pub(crate) fn placeholder(filter_id: i32) -> Self {
        Self {
            filter_id,
            build_expr: test_null_expr(),
            probe_expr: test_null_expr(),
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
            build_cardinality: 0.0,
        }
    }
}

#[cfg(test)]
impl RuntimeFilterProbe {
    pub(crate) fn placeholder(filter_id: i32) -> Self {
        Self { filter_id, probe_expr: test_null_expr() }
    }
}

#[cfg(test)]
fn test_null_expr() -> TypedExpr {
    use crate::sql::analysis::{ExprKind, LiteralValue};
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        data_type: arrow::datatypes::DataType::Null,
        nullable: true,
    }
}
