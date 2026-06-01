//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `PhysicalPlanNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::analysis::{JoinKind, TypedExpr};
use crate::sql::optimizer::operator::{JoinDistribution, Operator};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

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

/// Entry point: walk the physical plan tree and annotate eligible hash joins
/// with build-side [`RuntimeFilterDesc`]s plus placeholder probe descriptors
/// on the immediate probe child.
///
/// Returns immediately if the rule is disabled via
/// `SET disable_optimizer_rules = 'RuntimeFilterPushDown'`.
pub(crate) fn annotate(root: &mut PhysicalPlanNode, options: &OptimizerOptions) {
    if !options.is_enabled(RUNTIME_FILTER_RULE) {
        return;
    }
    let mut next_filter_id: i32 = 0;
    annotate_node(root, &mut next_filter_id);
}

/// True if a hash join of this kind should produce runtime filters on its
/// build side.  Anti-joins and full-outer joins are excluded because they
/// cannot safely early-filter the probe side.
fn join_builds_rf(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Inner
            | JoinKind::LeftSemi
            | JoinKind::RightOuter
            | JoinKind::RightSemi
            | JoinKind::RightAnti
            | JoinKind::Cross
    )
}

/// Recursive tree walk: post-order so that nested joins get distinct filter ids.
fn annotate_node(node: &mut PhysicalPlanNode, next_filter_id: &mut i32) {
    // Recurse into children first (post-order).
    for child in &mut node.children {
        annotate_node(child, next_filter_id);
    }

    // Clone the data we need from the join before borrowing children mutably.
    let Operator::PhysicalHashJoin(join) = &node.op else {
        return;
    };
    if !join_builds_rf(join.join_type) {
        return;
    }
    let eq_conditions = join.eq_conditions.clone();
    let distribution = join.distribution.clone();
    // Right child is build side (confirmed via pipeline builder + lowering).
    let build_card = node.children[1].stats.output_row_count;

    // Build descriptors for each non-null-safe equi-conjunct.
    let mut descs: Vec<RuntimeFilterDesc> = Vec::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if eq.null_safe {
            continue;
        }
        let filter_id = *next_filter_id;
        *next_filter_id += 1;
        descs.push(RuntimeFilterDesc {
            filter_id,
            build_expr: eq.right.clone(),
            probe_expr: eq.left.clone(),
            expr_order,
            distribution: distribution.clone(),
            build_cardinality: build_card,
        });
    }

    // PLACEHOLDER targeting (Task 3 replaces with real column-lineage push-down):
    // attach probe descriptors to the immediate probe child (children[0]).
    for d in &descs {
        node.children[0].probe_runtime_filters.push(RuntimeFilterProbe {
            filter_id: d.filter_id,
            probe_expr: d.probe_expr.clone(),
        });
    }

    node.build_runtime_filters = descs;
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

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        PhysicalValuesOp,
    };
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::statistics::Statistics;

    /// Helper: an Int32 column + a matching ColumnRef expr + OutputColumn.
    fn col(id: u32, name: &str) -> (OutputColumn, TypedExpr) {
        let cid = ColumnId::new_for_test(id);
        let oc = OutputColumn {
            column_id: cid,
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: true,
            is_internal: false,
        };
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: true,
        };
        (oc, expr)
    }

    fn leaf(rows: f64, oc: OutputColumn) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalValues(PhysicalValuesOp { rows: vec![], columns: vec![] }),
            children: vec![],
            stats: Statistics { output_row_count: rows, column_statistics: Default::default() },
            output_columns: vec![oc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_two_scans() -> PhysicalPlanNode {
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: lexpr,
                    right: rexpr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics { output_row_count: 10.0, column_statistics: Default::default() },
            output_columns: vec![loc, roc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::options::OptimizerOptions;

    #[test]
    fn inner_join_gets_one_build_rf() {
        let mut join = super::test_support::inner_join_two_scans();
        annotate(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.build_runtime_filters[0].filter_id, 0);
    }

    #[test]
    fn disabled_rule_emits_nothing() {
        let mut join = super::test_support::inner_join_two_scans();
        let mut opts = OptimizerOptions::default_settings();
        opts.disable(RUNTIME_FILTER_RULE);
        annotate(&mut join, &opts);
        assert!(join.build_runtime_filters.is_empty());
    }
}
