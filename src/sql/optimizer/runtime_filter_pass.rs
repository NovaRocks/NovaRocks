//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `PhysicalPlanNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{JoinDistribution, Operator};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use std::collections::HashSet;

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
        }
    }
}

#[cfg(test)]
impl RuntimeFilterProbe {
    pub(crate) fn placeholder(filter_id: i32) -> Self {
        Self {
            filter_id,
            probe_expr: test_null_expr(),
        }
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

/// Collect ALL column ids referenced by `expr` into `out`.
///
/// This function is exhaustive over every `ExprKind` variant that holds
/// sub-expressions or column refs. Under-collection would cause `could_bound`
/// to return `true` when a node does NOT expose a column, placing a probe
/// incorrectly. Each variant is handled explicitly; only verified leaf variants
/// (no sub-expressions) use a wildcard arm.
fn column_ids(expr: &TypedExpr, out: &mut HashSet<ColumnId>) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            out.insert(*column_id);
        }
        ExprKind::BinaryOp { left, right, .. } => {
            column_ids(left, out);
            column_ids(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => {
            column_ids(expr, out);
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                column_ids(arg, out);
            }
        }
        ExprKind::LambdaFunction { body, .. } => {
            // params are declaration-only; column refs can appear in the body.
            column_ids(body, out);
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                column_ids(arg, out);
            }
            for item in order_by {
                column_ids(&item.expr, out);
            }
        }
        ExprKind::Cast { expr, .. } => {
            column_ids(expr, out);
        }
        ExprKind::IsNull { expr, .. } => {
            column_ids(expr, out);
        }
        ExprKind::InList { expr, list, .. } => {
            column_ids(expr, out);
            for item in list {
                column_ids(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            column_ids(expr, out);
            column_ids(low, out);
            column_ids(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            column_ids(expr, out);
            column_ids(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(op) = operand {
                column_ids(op, out);
            }
            for (when, then) in when_then {
                column_ids(when, out);
                column_ids(then, out);
            }
            if let Some(el) = else_expr {
                column_ids(el, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => {
            column_ids(expr, out);
        }
        ExprKind::Nested(inner) => {
            column_ids(inner, out);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                column_ids(arg, out);
            }
            for pb in partition_by {
                column_ids(pb, out);
            }
            for item in order_by {
                column_ids(&item.expr, out);
            }
        }
        ExprKind::Lambda { body, .. } => {
            // params are parameter names only; the body may reference outer columns.
            column_ids(body, out);
        }
        // Verified leaves — no sub-expressions, no column refs:
        // - Literal: holds only a LiteralValue (no TypedExpr).
        // - LambdaParamRef: references a lambda slot by name/id, not a ColumnId.
        // - SubqueryPlaceholder: consumed before planning; has no TypedExpr children.
        ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Returns true if `node` outputs every column id referenced by `probe_expr`.
///
/// An empty needed set (e.g. a literal probe expression) cannot be bound — the
/// probe is always non-trivial for real join keys, but we guard for correctness.
fn could_bound(node: &PhysicalPlanNode, probe_expr: &TypedExpr) -> bool {
    let mut needed = HashSet::new();
    column_ids(probe_expr, &mut needed);
    if needed.is_empty() {
        return false;
    }
    let have: HashSet<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    needed.iter().all(|id| have.contains(id))
}

/// Stage 1: stop at fragment boundaries (exchange / distribution nodes).
/// Cross-exchange push-down is a later stage.
fn is_exchange(node: &PhysicalPlanNode) -> bool {
    matches!(node.op, Operator::PhysicalDistribution(_))
}

/// Descend into `node` and attach `probe` at the DEEPEST descendant that can
/// bind the probe expression. Returns `true` when the probe has been placed.
///
/// Rules:
/// 1. Never cross an exchange (fragment boundary).
/// 2. If `node` cannot bind the probe, stop (return false) — do not descend.
/// 3. Try each child recursively; if a child accepts the probe, we are done.
/// 4. If no child accepted it, place the probe on `node` itself (the deepest
///    reachable binder).
fn push_probe_down(node: &mut PhysicalPlanNode, probe: &RuntimeFilterProbe) -> bool {
    if is_exchange(node) {
        return false;
    }
    if !could_bound(node, &probe.probe_expr) {
        return false;
    }
    for child in &mut node.children {
        if push_probe_down(child, probe) {
            return true;
        }
    }
    node.probe_runtime_filters.push(probe.clone());
    true
}

// StarRocks defaults (SessionVariable.java). Bytes; size = rows * avg_row_size.
const BUILD_MAX_SIZE: f64 = 64.0 * 1024.0 * 1024.0;
const BUILD_MIN_SIZE: f64 = 128.0 * 1024.0;
const PROBE_MIN_SIZE: f64 = 100.0 * 1024.0;
const PROBE_MIN_SELECTIVITY: f64 = 0.5;

/// StarRocks JoinNode.java: only Shuffle/Partitioned gate on build size.
fn build_gate_passes(distribution: &JoinDistribution, build_size: f64) -> bool {
    match distribution {
        JoinDistribution::Shuffle => !(build_size <= 0.0 || build_size > BUILD_MAX_SIZE),
        _ => true, // Broadcast / Colocate: no build-size gate
    }
}

/// StarRocks RuntimeFilterDescription.canProbeUse selectivity gate.
fn probe_gate_passes(local: bool, build_size: f64, probe_size: f64) -> bool {
    if local {
        return true;
    }
    if build_size <= BUILD_MIN_SIZE {
        return true;
    }
    if probe_size < PROBE_MIN_SIZE {
        return false;
    }
    (build_size / probe_size.max(1.0)) <= 1.0 - PROBE_MIN_SELECTIVITY
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
    let build_size = node.children[1].stats.compute_size();
    let probe_size = node.children[0].stats.compute_size();

    // Build gate: Shuffle joins are rejected if build side is too large or empty.
    if !build_gate_passes(&distribution, build_size) {
        return;
    }

    // Stage 1: Broadcast/Colocate are local (same fragment); Shuffle is non-local.
    let local = !matches!(distribution, JoinDistribution::Shuffle);

    // Build descriptors for each non-null-safe equi-conjunct.
    let mut descs: Vec<RuntimeFilterDesc> = Vec::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if eq.null_safe {
            continue;
        }
        // Probe gate: skip this equi-conjunct if it would not reduce probe rows enough.
        if !probe_gate_passes(local, build_size, probe_size) {
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
        });
    }

    // Push each probe descriptor down to the deepest binding descendant within
    // the probe child (children[0]). Stops at exchange (fragment) boundaries.
    // If no binding node is found the RF is build-only (probe remains unplaced).
    for d in &descs {
        let probe = RuntimeFilterProbe {
            filter_id: d.filter_id,
            probe_expr: d.probe_expr.clone(),
        };
        // children[0] = probe side; descend to the deepest binding node.
        let _ = push_probe_down(&mut node.children[0], &probe);
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
            op: Operator::PhysicalValues(PhysicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: rows,
                column_statistics: Default::default(),
            },
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
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
            },
            output_columns: vec![loc, roc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn shuffle_join(build_rows: f64, probe_rows: f64) -> PhysicalPlanNode {
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let probe = leaf(probe_rows, loc.clone());
        let build = leaf(build_rows, roc.clone());
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: lexpr,
                    right: rexpr,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            children: vec![probe, build], // children[0]=probe, children[1]=build
            stats: Statistics {
                output_row_count: build_rows.min(probe_rows),
                column_statistics: Default::default(),
            },
            output_columns: vec![loc, roc],
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn join_with_project_over_probe_scan() -> PhysicalPlanNode {
        use crate::sql::optimizer::operator::PhysicalProjectOp;
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        // probe side: PhysicalProject(node) over a leaf scan; both expose column 1.
        let scan = leaf(1_000_000.0, loc.clone());
        let project = PhysicalPlanNode {
            op: Operator::PhysicalProject(PhysicalProjectOp { items: vec![] }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
            },
            output_columns: vec![loc.clone()], // project passes column 1 through
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(10.0, roc.clone());
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
            children: vec![project, build],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
            },
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

    #[test]
    fn skips_rf_when_build_side_too_large_for_shuffle() {
        // build 50M rows * 8 = 400MB > 64MB build_max -> skip.
        let mut j = super::test_support::shuffle_join(50_000_000.0, 50_000_000.0);
        annotate(&mut j, &OptimizerOptions::default_settings());
        assert!(j.build_runtime_filters.is_empty());
    }

    #[test]
    fn keeps_rf_when_build_small_relative_to_probe() {
        // Broadcast (local), tiny build -> kept.
        let mut j = super::test_support::inner_join_two_scans();
        annotate(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(j.build_runtime_filters.len(), 1);
    }

    #[test]
    fn skips_rf_low_selectivity_across_exchange() {
        // shuffle build 900k*8=7.2MB, probe 1M*8=8MB, ratio 0.9 > 0.5 -> reject.
        let mut j = super::test_support::shuffle_join(900_000.0, 1_000_000.0);
        annotate(&mut j, &OptimizerOptions::default_settings());
        assert!(j.build_runtime_filters.is_empty());
    }

    #[test]
    fn probe_pushes_through_project_to_scan() {
        let mut join = super::test_support::join_with_project_over_probe_scan();
        annotate(&mut join, &OptimizerOptions::default_settings());
        // Probe RF must NOT stop at the project (children[0])...
        assert!(
            join.children[0].probe_runtime_filters.is_empty(),
            "probe should not stop at the project node"
        );
        // ...it reached the scan beneath the project (children[0].children[0]).
        assert_eq!(join.children[0].children[0].probe_runtime_filters.len(), 1);
    }
}
