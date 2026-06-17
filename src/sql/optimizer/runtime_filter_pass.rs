//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `PhysicalPlanNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{JoinDistribution, Operator, PhysicalHashJoinEqCondition};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::{JoinExecutionDistribution, PhysicalPlanNode};
use crate::sql::optimizer::scalar::{ScalarArena, materialize};
use std::collections::HashSet;

/// The optimizer-layer name used by `SET disable_optimizer_rules`.
pub(crate) const RUNTIME_FILTER_RULE: &str = "RuntimeFilterPushDown";

/// Build-side runtime filter produced by a hash join (one per equi-conjunct
/// that survives gating + push-down).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterDesc {
    pub filter_id: i32,
    /// Oriented build-side key expression in build-child column space.
    pub build_expr: TypedExpr,
    /// Oriented probe-side key expression in the target node's column space.
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
pub(crate) fn annotate(
    root: &mut PhysicalPlanNode,
    scalars: &ScalarArena,
    options: &OptimizerOptions,
) {
    if !options.is_enabled(RUNTIME_FILTER_RULE) {
        return;
    }
    let mut next_filter_id: i32 = 0;
    annotate_node(root, scalars, &mut next_filter_id, options);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JoinRfSides {
    probe_child: usize,
    build_child: usize,
}

/// Runtime filters are only produced when the join semantics allow the true
/// probe child to be early-filtered by keys from the true build child.
fn rf_sides_for_join(kind: JoinKind) -> Option<JoinRfSides> {
    match kind {
        JoinKind::Inner
        | JoinKind::LeftSemi
        | JoinKind::RightOuter
        | JoinKind::RightSemi
        | JoinKind::RightAnti => Some(JoinRfSides {
            probe_child: 0,
            build_child: 1,
        }),
        JoinKind::LeftOuter
        | JoinKind::FullOuter
        | JoinKind::LeftAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::Cross => None,
    }
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

fn column_id_vec(expr: &TypedExpr) -> Vec<ColumnId> {
    let mut ids = HashSet::new();
    column_ids(expr, &mut ids);
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort();
    ids
}

fn child_column_set(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    node.output_columns.iter().map(|c| c.column_id).collect()
}

fn expr_bound_child(node: &PhysicalPlanNode, expr: &TypedExpr) -> Option<usize> {
    let ids = column_id_vec(expr);
    if ids.is_empty() {
        return None;
    }

    let mut bound_child = None;
    for (idx, child) in node.children.iter().enumerate() {
        let child_cols = child_column_set(child);
        if ids.iter().all(|id| child_cols.contains(id)) {
            if bound_child.is_some() {
                return None;
            }
            bound_child = Some(idx);
        }
    }
    bound_child
}

#[derive(Clone, Debug)]
struct OrientedRfKey {
    build_expr: TypedExpr,
    probe_expr: TypedExpr,
    expr_order: usize,
}

fn orient_rf_key(
    node: &PhysicalPlanNode,
    scalars: &ScalarArena,
    sides: JoinRfSides,
    expr_order: usize,
    eq: &PhysicalHashJoinEqCondition,
) -> Option<OrientedRfKey> {
    let left = materialize(scalars, eq.left);
    let right = materialize(scalars, eq.right);
    let left_child = expr_bound_child(node, &left)?;
    let right_child = expr_bound_child(node, &right)?;

    if left_child == sides.probe_child && right_child == sides.build_child {
        Some(OrientedRfKey {
            build_expr: right,
            probe_expr: left,
            expr_order,
        })
    } else if left_child == sides.build_child && right_child == sides.probe_child {
        Some(OrientedRfKey {
            build_expr: left,
            probe_expr: right,
            expr_order,
        })
    } else {
        None
    }
}

fn rf_key_types_match(scalars: &ScalarArena, eq: &PhysicalHashJoinEqCondition) -> bool {
    scalars.data_type(eq.left) == scalars.data_type(eq.right)
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

/// Returns true for non-crossable exchange boundaries (Gather / Any distribution).
/// These remain hard fragment boundaries that probe runtime filters cannot cross.
fn is_exchange(node: &PhysicalPlanNode) -> bool {
    matches!(node.op, Operator::PhysicalDistribution(_))
}

/// A `PhysicalDistribution` whose spec is a shuffle/hash partition is the kind
/// of boundary a probe runtime filter may cross when the build RF is complete.
/// Gather / Any are never crossable.
fn distribution_is_crossable(node: &PhysicalPlanNode) -> bool {
    use crate::sql::optimizer::property::DistributionSpec;
    matches!(
        &node.op,
        Operator::PhysicalDistribution(op)
            if matches!(op.spec, DistributionSpec::HashPartitioned { .. })
    )
}

#[derive(Clone, Copy, Debug)]
struct ProbePushPolicy {
    allow_cross_exchange: bool,
    cross_exchange_build_complete: bool,
}

fn join_is_outer_or_anti_boundary(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::LeftOuter
            | JoinKind::RightOuter
            | JoinKind::FullOuter
            | JoinKind::LeftAnti
            | JoinKind::RightAnti
            | JoinKind::NullAwareLeftAnti
    )
}

fn is_probe_semantic_boundary(node: &PhysicalPlanNode) -> bool {
    matches!(
        &node.op,
        Operator::PhysicalHashJoin(join) if join_is_outer_or_anti_boundary(join.join_type)
    )
}

/// Descend into `node` and attach `probe` at the DEEPEST descendant that can
/// bind the probe expression. Returns `true` when the probe has been placed.
///
/// Rules:
/// 1. Crossable exchange (HashPartitioned/shuffle): descend transparently only
///    when the policy allows it and the build RF is complete. Shuffle exchanges
///    preserve column ids and carry no projection, so no exchange bind check is
///    needed in that case.
/// 2. Non-crossable exchange (Gather/Any): hard fragment boundary — stop.
/// 3. If `node` cannot bind the probe, stop (return false) — do not descend.
/// 4. Outer/anti/null-preserving joins are semantic boundaries: place there.
/// 5. Try each child recursively; if a child accepts the probe, we are done.
/// 6. If no child accepted it, place the probe on `node` itself (the deepest
///    reachable binder).
fn push_probe_down(
    node: &mut PhysicalPlanNode,
    probe: &RuntimeFilterProbe,
    policy: ProbePushPolicy,
) -> bool {
    if policy.allow_cross_exchange
        && policy.cross_exchange_build_complete
        && distribution_is_crossable(node)
    {
        if let Some(child) = node.children.first_mut() {
            return push_probe_down(child, probe, policy);
        }
        return false;
    }
    if is_exchange(node) {
        return false;
    }
    if !could_bound(node, &probe.probe_expr) {
        return false;
    }
    if is_probe_semantic_boundary(node) {
        node.probe_runtime_filters.push(probe.clone());
        return true;
    }
    for child in &mut node.children {
        if push_probe_down(child, probe, policy) {
            return true;
        }
    }
    node.probe_runtime_filters.push(probe.clone());
    true
}

/// StarRocks LogicalJoinNode.java: only Shuffle/Partitioned gate on build size.
fn build_gate_passes(distribution: &JoinDistribution, build_size: f64, build_max: f64) -> bool {
    match distribution {
        JoinDistribution::Shuffle => !(build_size <= 0.0 || build_size > build_max),
        _ => true, // Broadcast / Colocate: no build-size gate
    }
}

/// StarRocks RuntimeFilterDescription.canProbeUse selectivity gate.
fn probe_gate_passes(
    local: bool,
    build_size: f64,
    probe_size: f64,
    build_min: f64,
    probe_min: f64,
    min_sel: f64,
) -> bool {
    if local {
        return true;
    }
    if build_size <= build_min {
        return true;
    }
    if probe_size < probe_min {
        return false;
    }
    (build_size / probe_size.max(1.0)) <= 1.0 - min_sel
}

fn join_distribution_for_runtime_filter(
    node: &PhysicalPlanNode,
    fallback: &JoinDistribution,
) -> JoinDistribution {
    match node.execution_props.join_distribution {
        Some(JoinExecutionDistribution::Broadcast) => JoinDistribution::Broadcast,
        Some(JoinExecutionDistribution::Partitioned) => JoinDistribution::Shuffle,
        Some(JoinExecutionDistribution::Colocate) => JoinDistribution::Colocate,
        None => fallback.clone(),
    }
}

/// Recursive tree walk: post-order so that nested joins get distinct filter ids.
fn annotate_node(
    node: &mut PhysicalPlanNode,
    scalars: &ScalarArena,
    next_filter_id: &mut i32,
    options: &OptimizerOptions,
) {
    // Recurse into children first (post-order).
    for child in &mut node.children {
        annotate_node(child, scalars, next_filter_id, options);
    }

    // Clone the data we need from the join before borrowing children mutably.
    let Operator::PhysicalHashJoin(join) = &node.op else {
        return;
    };
    let Some(sides) = rf_sides_for_join(join.join_type) else {
        return;
    };
    let max_child = sides.probe_child.max(sides.build_child);
    if node.children.len() <= max_child {
        return;
    }
    let eq_conditions = join.eq_conditions.clone();
    let distribution = join_distribution_for_runtime_filter(node, &join.distribution);
    if matches!(distribution, JoinDistribution::Unknown) {
        return;
    }
    let build_size = node.children[sides.build_child].stats.compute_size();
    let probe_size = node.children[sides.probe_child].stats.compute_size();

    // Cast session thresholds (u64 bytes) to f64 for size comparisons.
    let build_max = options.rf_build_max_bytes as f64;
    let build_min = options.rf_build_min_bytes as f64;
    let probe_min = options.rf_probe_min_bytes as f64;
    let min_sel = options.rf_probe_min_selectivity;

    // Build gate: Shuffle joins are rejected if build side is too large or empty.
    if !build_gate_passes(&distribution, build_size, build_max) {
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
        if !rf_key_types_match(scalars, eq) {
            continue;
        }
        // Probe gate: skip this equi-conjunct if it would not reduce probe rows enough.
        if !probe_gate_passes(local, build_size, probe_size, build_min, probe_min, min_sel) {
            continue;
        }
        let Some(oriented) = orient_rf_key(node, scalars, sides, expr_order, eq) else {
            continue;
        };
        if (*next_filter_id as usize) >= options.rf_max_count {
            continue;
        }
        let filter_id = *next_filter_id;
        *next_filter_id += 1;
        descs.push(RuntimeFilterDesc {
            filter_id,
            build_expr: oriented.build_expr,
            probe_expr: oriented.probe_expr,
            expr_order: oriented.expr_order,
            distribution: distribution.clone(),
        });
    }

    // Push each probe descriptor down to the deepest binding descendant within
    // the true probe child. Stops at exchange (fragment) boundaries.
    // If no binding node is found the RF is build-only (probe remains unplaced).
    let policy = ProbePushPolicy {
        allow_cross_exchange: options.allow_cross_exchange_rf,
        cross_exchange_build_complete: matches!(distribution, JoinDistribution::Broadcast),
    };
    for d in &descs {
        let probe = RuntimeFilterProbe {
            filter_id: d.filter_id,
            probe_expr: d.probe_expr.clone(),
        };
        // Descend into the true probe side to the deepest binding node.
        let _ = push_probe_down(&mut node.children[sides.probe_child], &probe, policy);
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
    use std::sync::Arc;

    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
    };
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, attach_scalar_arena};
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use crate::sql::optimizer::statistics::Statistics;

    /// Helper: an Int32 column + a matching ColumnRef expr + OutputColumn.
    fn col(id: u32, name: &str) -> (OutputColumn, TypedExpr) {
        col_with_type(id, name, arrow::datatypes::DataType::Int32)
    }

    fn col_with_type(
        id: u32,
        name: &str,
        data_type: arrow::datatypes::DataType,
    ) -> (OutputColumn, TypedExpr) {
        let cid = ColumnId::new_for_test(id);
        let oc = OutputColumn {
            column_id: cid,
            name: name.to_string(),
            data_type: data_type.clone(),
            nullable: true,
            is_internal: false,
        };
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable: true,
        };
        (oc, expr)
    }

    fn eq(
        scalars: &mut ScalarArena,
        left: &TypedExpr,
        right: &TypedExpr,
    ) -> PhysicalHashJoinEqCondition {
        PhysicalHashJoinEqCondition {
            left: intern_typed(scalars, left),
            right: intern_typed(scalars, right),
            null_safe: false,
        }
    }

    fn with_scalars(mut plan: PhysicalPlanNode, scalars: ScalarArena) -> PhysicalPlanNode {
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn leaf(rows: f64, oc: OutputColumn) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: rows,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![oc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_two_scans() -> PhysicalPlanNode {
        inner_join_two_scans_with_key_types(
            arrow::datatypes::DataType::Int32,
            arrow::datatypes::DataType::Int32,
        )
    }

    pub(crate) fn inner_join_two_scans_with_key_types(
        left_type: arrow::datatypes::DataType,
        right_type: arrow::datatypes::DataType,
    ) -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col_with_type(1, "lc", left_type);
        let (roc, rexpr) = col_with_type(2, "rc", right_type);
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn hash_join_two_scans(join_type: JoinKind) -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn cross_hash_join_without_eq_conditions() -> PhysicalPlanNode {
        let scalars = ScalarArena::new();
        let (loc, _) = col(1, "lc");
        let (roc, _) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Cross,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn inner_join_with_swapped_eq_labels() -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &rexpr, &lexpr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn shuffle_join(build_rows: f64, probe_rows: f64) -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let probe = leaf(probe_rows, loc.clone());
        let build = leaf(build_rows, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            children: vec![probe, build], // children[0]=probe, children[1]=build
            stats: Statistics {
                output_row_count: build_rows.min(probe_rows),
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Shuffle join where the probe child is a `PhysicalDistribution(HashPartitioned)`
    /// over a leaf scan. Tests that probe RFs cross the shuffle exchange to reach the
    /// underlying scan rather than stopping at the exchange boundary.
    pub(crate) fn shuffle_join_with_probe_exchange() -> PhysicalPlanNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        // probe side: PhysicalDistribution(HashPartitioned on col 1) over a leaf scan.
        let scan = leaf(1_000_000.0, loc.clone());
        let exch = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols: vec![loc.column_id],
                    source: HashSource::ShuffleJoin,
                },
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc.clone()], // exchange preserves column 1
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        // build side SMALL so the build gate and probe gate both pass
        // (build_size = 100 * 8 = 800 bytes, well below BUILD_MIN 128KB).
        let build = leaf(100.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            children: vec![exch, build], // children[0]=probe-exchange, children[1]=build
            stats: Statistics {
                output_row_count: 100.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn broadcast_join_with_probe_exchange() -> PhysicalPlanNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        let scan = leaf(1_000_000.0, loc.clone());
        let exch = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols: vec![loc.column_id],
                    source: HashSource::ShuffleJoin,
                },
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc.clone()],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(100.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![exch, build],
            stats: Statistics {
                output_row_count: 100.0,
                row_count_confidence: crate::sql::optimizer::statistics::Confidence::Estimated,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn inner_join_over_left_outer_probe_child() -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let (preserved_oc, preserved_expr) = col(1, "preserved");
        let (outer_build_oc, outer_build_expr) = col(2, "outer_build");
        let (top_build_oc, top_build_expr) = col(3, "top_build");
        let preserved = leaf(1_000_000.0, preserved_oc.clone());
        let outer_build = leaf(10.0, outer_build_oc.clone());
        let left_outer = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::LeftOuter,
                eq_conditions: vec![eq(&mut scalars, &preserved_expr, &outer_build_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![preserved, outer_build],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![preserved_oc.clone(), outer_build_oc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let top_build = leaf(10.0, top_build_oc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &preserved_expr, &top_build_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left_outer, top_build],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![preserved_oc, top_build_oc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn join_with_project_over_probe_scan() -> PhysicalPlanNode {
        use crate::sql::optimizer::operator::ProjectOp;
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        // probe side: PhysicalProject(node) over a leaf scan; both expose column 1.
        let scan = leaf(1_000_000.0, loc.clone());
        let project = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![],
                output_qualifier: None,
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc.clone()], // project passes column 1 through
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(10.0, roc.clone());
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &lexpr, &rexpr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![project, build],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::options::OptimizerOptions;

    fn annotate_test(plan: &mut PhysicalPlanNode, options: &OptimizerOptions) {
        let scalars = plan
            .execution_props
            .scalar_arena
            .as_ref()
            .expect("runtime-filter test plan must carry scalar arena")
            .clone();
        annotate(plan, scalars.as_ref(), options);
    }

    #[test]
    fn inner_join_gets_one_build_rf() {
        let mut join = super::test_support::inner_join_two_scans();
        annotate_test(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.build_runtime_filters[0].filter_id, 0);
    }

    #[test]
    fn disabled_rule_emits_nothing() {
        let mut join = super::test_support::inner_join_two_scans();
        let mut opts = OptimizerOptions::default_settings();
        opts.disable(RUNTIME_FILTER_RULE);
        annotate_test(&mut join, &opts);
        assert!(join.build_runtime_filters.is_empty());
    }

    #[test]
    fn skips_rf_when_build_side_too_large_for_shuffle() {
        // build 50M rows * 8 = 400MB > 64MB build_max -> skip.
        let mut j = super::test_support::shuffle_join(50_000_000.0, 50_000_000.0);
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert!(j.build_runtime_filters.is_empty());
    }

    #[test]
    fn keeps_rf_when_build_small_relative_to_probe() {
        // Broadcast (local), tiny build -> kept.
        let mut j = super::test_support::inner_join_two_scans();
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(j.build_runtime_filters.len(), 1);
    }

    #[test]
    fn skips_rf_for_mixed_type_join_keys() {
        let mut join = super::test_support::inner_join_two_scans_with_key_types(
            arrow::datatypes::DataType::Utf8,
            arrow::datatypes::DataType::Int32,
        );

        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert!(
            join.build_runtime_filters.is_empty(),
            "mixed-type RF descriptors would not carry the join-key cast semantics"
        );
        assert_eq!(probe_runtime_filter_count(&join), 0);
    }

    #[test]
    fn skips_rf_low_selectivity_across_exchange() {
        // shuffle build 900k*8=7.2MB, probe 1M*8=8MB, ratio 0.9 > 0.5 -> reject.
        let mut j = super::test_support::shuffle_join(900_000.0, 1_000_000.0);
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert!(j.build_runtime_filters.is_empty());
    }

    #[test]
    fn probe_pushes_through_project_to_scan() {
        let mut join = super::test_support::join_with_project_over_probe_scan();
        annotate_test(&mut join, &OptimizerOptions::default_settings());
        // Probe RF must NOT stop at the project (children[0])...
        assert!(
            join.children[0].probe_runtime_filters.is_empty(),
            "probe should not stop at the project node"
        );
        // ...it reached the scan beneath the project (children[0].children[0]).
        assert_eq!(join.children[0].children[0].probe_runtime_filters.len(), 1);
    }

    #[test]
    fn partitioned_rf_does_not_cross_exchange_even_when_flag_enabled() {
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = true;

        annotate_test(&mut j, &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "partitioned probe RF must not be placed on the exchange"
        );
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "partitioned probe RF must not cross the exchange"
        );
    }

    #[test]
    fn broadcast_rf_crosses_exchange_when_flag_enabled() {
        let mut j = super::test_support::broadcast_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = true;

        annotate_test(&mut j, &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "broadcast probe RF must not stop at the exchange"
        );
        assert_eq!(
            exch.children[0].probe_runtime_filters.len(),
            1,
            "broadcast probe RF should reach the scan below the exchange"
        );
    }

    #[test]
    fn probe_does_not_descend_through_outer_join_boundary() {
        let mut join = super::test_support::inner_join_over_left_outer_probe_child();

        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1, "build RF expected");
        let left_outer = &join.children[0];
        assert_eq!(
            left_outer.probe_runtime_filters.len(),
            1,
            "probe RF should stop on the outer join boundary"
        );
        assert!(
            left_outer.children[0].probe_runtime_filters.is_empty(),
            "probe RF must not descend into the preserved child"
        );
    }

    #[test]
    fn probe_stays_within_fragment_by_default() {
        // Default: partial partitioned RF must not cross the shuffle exchange.
        // It falls back to build-only — the probe stays unplaced above the
        // exchange.
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(j.build_runtime_filters.len(), 1, "build RF still expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "probe must not be placed on the exchange"
        );
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "probe must NOT cross the exchange by default (within-fragment fallback)"
        );
    }

    #[test]
    fn unknown_join_distribution_does_not_build_runtime_filters() {
        let mut join = super::test_support::inner_join_two_scans();
        let Operator::PhysicalHashJoin(op) = &mut join.op else {
            panic!("expected hash join");
        };
        op.distribution = JoinDistribution::Unknown;

        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert!(join.build_runtime_filters.is_empty());
        assert_eq!(probe_runtime_filter_count(&join), 0);
    }

    #[test]
    fn runtime_filter_uses_execution_distribution_metadata() {
        let mut join = super::test_support::inner_join_two_scans();
        join.execution_props.join_distribution =
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned);
        let Operator::PhysicalHashJoin(op) = &mut join.op else {
            panic!("expected hash join");
        };
        op.distribution = JoinDistribution::Unknown;

        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1);
        assert!(
            join.build_runtime_filters
                .iter()
                .all(|rf| matches!(rf.distribution, JoinDistribution::Shuffle))
        );
    }

    #[test]
    fn runtime_filter_uses_local_execution_distribution_metadata() {
        for (metadata, expected) in [
            (
                crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast,
                JoinDistribution::Broadcast,
            ),
            (
                crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Colocate,
                JoinDistribution::Colocate,
            ),
        ] {
            let mut join = super::test_support::inner_join_two_scans();
            join.execution_props.join_distribution = Some(metadata);
            let Operator::PhysicalHashJoin(op) = &mut join.op else {
                panic!("expected hash join");
            };
            op.distribution = JoinDistribution::Unknown;

            annotate_test(&mut join, &OptimizerOptions::default_settings());

            assert_eq!(join.build_runtime_filters.len(), 1);
            assert_eq!(join.build_runtime_filters[0].distribution, expected);
        }
    }

    #[test]
    fn rf_join_eligibility_matches_probe_output_semantics() {
        use crate::sql::analysis::JoinKind;

        for kind in [
            JoinKind::Inner,
            JoinKind::LeftSemi,
            JoinKind::RightOuter,
            JoinKind::RightSemi,
            JoinKind::RightAnti,
        ] {
            let mut join = super::test_support::hash_join_two_scans(kind);
            annotate_test(&mut join, &OptimizerOptions::default_settings());
            assert_eq!(
                join.build_runtime_filters.len(),
                1,
                "{kind:?} should build an RF"
            );
        }

        for kind in [
            JoinKind::LeftOuter,
            JoinKind::FullOuter,
            JoinKind::LeftAnti,
            JoinKind::NullAwareLeftAnti,
        ] {
            let mut join = super::test_support::hash_join_two_scans(kind);
            annotate_test(&mut join, &OptimizerOptions::default_settings());
            assert!(
                join.build_runtime_filters.is_empty(),
                "{kind:?} should not build an RF"
            );
        }

        let mut cross = super::test_support::cross_hash_join_without_eq_conditions();
        annotate_test(&mut cross, &OptimizerOptions::default_settings());
        assert!(
            cross.build_runtime_filters.is_empty(),
            "Cross without equality keys should not build an RF"
        );
        assert!(
            rf_sides_for_join(JoinKind::Cross).is_none(),
            "Cross should not be marked RF-producing"
        );
    }

    #[test]
    fn rf_orients_swapped_eq_labels_by_child_column_ids() {
        let mut join = super::test_support::inner_join_with_swapped_eq_labels();
        annotate_test(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.children[0].probe_runtime_filters.len(), 1);
        assert!(join.children[1].probe_runtime_filters.is_empty());
        assert_eq!(
            column_id_vec(&join.build_runtime_filters[0].build_expr),
            vec![crate::sql::column_id::ColumnId::new_for_test(2)]
        );
        assert_eq!(
            column_id_vec(&join.build_runtime_filters[0].probe_expr),
            vec![crate::sql::column_id::ColumnId::new_for_test(1)]
        );
    }

    #[test]
    fn session_build_max_can_skip_rf() {
        // build_rows=1000, avg_row_size=8 bytes -> build_size=8KB.
        // With rf_build_max_bytes=1, build gate rejects even a tiny build.
        let mut j = super::test_support::shuffle_join(1000.0, 1_000_000.0);
        let mut opts = OptimizerOptions::default_settings();
        opts.rf_build_max_bytes = 1; // 1 byte -> build gate rejects
        annotate_test(&mut j, &opts);
        assert!(j.build_runtime_filters.is_empty());
    }

    #[test]
    fn rf_count_cap_limits_new_descriptors() {
        let mut join = super::test_support::inner_join_two_scans();
        let mut opts = OptimizerOptions::default_settings();
        opts.rf_max_count = 0;

        annotate_test(&mut join, &opts);

        assert!(join.build_runtime_filters.is_empty());
        assert_eq!(probe_runtime_filter_count(&join), 0);
    }

    fn probe_runtime_filter_count(node: &PhysicalPlanNode) -> usize {
        node.probe_runtime_filters.len()
            + node
                .children
                .iter()
                .map(probe_runtime_filter_count)
                .sum::<usize>()
    }
}
