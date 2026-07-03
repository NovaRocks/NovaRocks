//! OQ-5 Stage 1: physical-tree runtime-filter planning pass.
//!
//! Annotates eligible hash-join `OptimizerPhysicalNode`s with build-side filter
//! descriptors and pushes a matching probe descriptor down to the deepest
//! descendant that can bind the probe column. EXPLAIN renders the annotations;
//! codegen lowers them to thrift `TRuntimeFilterDescription`.

use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::operator::{JoinDistribution, Operator, PhysicalHashJoinEqCondition};
use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_tree::{JoinExecutionDistribution, OptimizerPhysicalNode};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use std::collections::HashSet;

/// The optimizer-layer name used by `SET disable_optimizer_rules`.
pub(crate) const RUNTIME_FILTER_RULE: &str = "RuntimeFilterPushDown";

/// Build-side runtime filter produced by a hash join (one per equi-conjunct
/// that survives gating + push-down).
#[derive(Clone, Debug)]
pub(crate) struct RuntimeFilterDesc {
    pub filter_id: i32,
    /// Oriented build-side key expression in build-child column space.
    pub build_expr: ScalarId,
    /// Oriented probe-side key expression in the target node's column space.
    pub probe_expr: ScalarId,
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
    pub probe_expr: ScalarId,
}

#[cfg(test)]
impl RuntimeFilterDesc {
    pub(crate) fn placeholder(arena: &mut ScalarArena, filter_id: i32) -> Self {
        let expr = test_null_expr(arena);
        Self {
            filter_id,
            build_expr: expr,
            probe_expr: expr,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }
    }
}

#[cfg(test)]
impl RuntimeFilterProbe {
    pub(crate) fn placeholder(arena: &mut ScalarArena, filter_id: i32) -> Self {
        Self {
            filter_id,
            probe_expr: test_null_expr(arena),
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
    root: &mut OptimizerPhysicalNode,
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
        JoinKind::Inner | JoinKind::RightOuter | JoinKind::LeftSemi | JoinKind::RightSemi => {
            Some(JoinRfSides {
                probe_child: 0,
                build_child: 1,
            })
        }
        JoinKind::LeftOuter
        | JoinKind::FullOuter
        | JoinKind::LeftAnti
        | JoinKind::RightAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::Cross => None,
    }
}

fn column_ids(scalars: &ScalarArena, expr: ScalarId, out: &mut HashSet<ColumnId>) {
    match scalars.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            out.insert(*column_id);
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            column_ids(scalars, *left, out);
            column_ids(scalars, *right, out);
        }
        ScalarNode::UnaryOp { child, .. } => {
            column_ids(scalars, *child, out);
        }
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                column_ids(scalars, *arg, out);
            }
        }
        ScalarNode::LambdaFunction { body, .. } => {
            column_ids(scalars, *body, out);
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args {
                column_ids(scalars, *arg, out);
            }
            for item in order_by {
                column_ids(scalars, item.expr, out);
            }
        }
        ScalarNode::Cast { child, .. } => {
            column_ids(scalars, *child, out);
        }
        ScalarNode::IsNull { child, .. } => {
            column_ids(scalars, *child, out);
        }
        ScalarNode::InList { child, list, .. } => {
            column_ids(scalars, *child, out);
            for item in list {
                column_ids(scalars, *item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            column_ids(scalars, *child, out);
            column_ids(scalars, *low, out);
            column_ids(scalars, *high, out);
        }
        ScalarNode::Like { child, pattern, .. } => {
            column_ids(scalars, *child, out);
            column_ids(scalars, *pattern, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                column_ids(scalars, *op, out);
            }
            for (when, then) in when_then {
                column_ids(scalars, *when, out);
                column_ids(scalars, *then, out);
            }
            if let Some(el) = else_expr {
                column_ids(scalars, *el, out);
            }
        }
        ScalarNode::IsTruthValue { child, .. } => {
            column_ids(scalars, *child, out);
        }
        ScalarNode::Nested(inner) => {
            column_ids(scalars, *inner, out);
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                column_ids(scalars, *arg, out);
            }
            for pb in partition_by {
                column_ids(scalars, *pb, out);
            }
            for item in order_by {
                column_ids(scalars, item.expr, out);
            }
        }
        ScalarNode::Lambda { body, .. } => {
            column_ids(scalars, *body, out);
        }
        ScalarNode::Literal(_) | ScalarNode::LambdaParamRef { .. } => {}
    }
}

fn column_id_vec(scalars: &ScalarArena, expr: ScalarId) -> Vec<ColumnId> {
    let mut ids = HashSet::new();
    column_ids(scalars, expr, &mut ids);
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort();
    ids
}

fn child_column_set(node: &OptimizerPhysicalNode) -> HashSet<ColumnId> {
    node.output_columns.iter().map(|c| c.column_id).collect()
}

fn expr_bound_child(
    node: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Option<usize> {
    let ids = column_id_vec(scalars, expr);
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
    build_expr: ScalarId,
    probe_expr: ScalarId,
    expr_order: usize,
}

fn orient_rf_key(
    node: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
    sides: JoinRfSides,
    expr_order: usize,
    eq: &PhysicalHashJoinEqCondition,
) -> Option<OrientedRfKey> {
    let left_child = expr_bound_child(node, scalars, eq.left)?;
    let right_child = expr_bound_child(node, scalars, eq.right)?;

    if left_child == sides.probe_child && right_child == sides.build_child {
        Some(OrientedRfKey {
            build_expr: eq.right,
            probe_expr: eq.left,
            expr_order,
        })
    } else if left_child == sides.build_child && right_child == sides.probe_child {
        Some(OrientedRfKey {
            build_expr: eq.left,
            probe_expr: eq.right,
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
fn could_bound(node: &OptimizerPhysicalNode, scalars: &ScalarArena, probe_expr: ScalarId) -> bool {
    let mut needed = HashSet::new();
    column_ids(scalars, probe_expr, &mut needed);
    if needed.is_empty() {
        return false;
    }
    let have: HashSet<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    needed.iter().all(|id| have.contains(id))
}

/// Returns true for non-crossable exchange boundaries (Gather / Any distribution).
/// These remain hard fragment boundaries that probe runtime filters cannot cross.
fn is_exchange(node: &OptimizerPhysicalNode) -> bool {
    matches!(node.op, Operator::PhysicalDistribution(_))
}

/// A `PhysicalDistribution` whose spec is a shuffle/hash partition is the kind
/// of boundary a probe runtime filter may cross when the build RF is complete.
/// Gather / Any are never crossable.
fn distribution_is_crossable(node: &OptimizerPhysicalNode) -> bool {
    use crate::sql::optimizer::property::DistributionSpec;
    matches!(
        &node.op,
        Operator::PhysicalDistribution(op)
            if matches!(op.spec, DistributionSpec::HashPartitioned { .. })
    )
}

/// True when `node` is a `HashPartitioned` exchange whose partition columns
/// carry every column of `probe_expr`. A Shuffle/Colocate probe RF may cross
/// such an exchange because matching rows remain co-located by the shared
/// partitioning; crossing an exchange partitioned on an unrelated key would
/// place a probe that drops rows which legitimately join (a correctness gate).
fn hash_partition_carries_probe_key(
    node: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
    probe_expr: ScalarId,
) -> bool {
    use crate::sql::optimizer::property::DistributionSpec;
    let Operator::PhysicalDistribution(op) = &node.op else {
        return false;
    };
    let DistributionSpec::HashPartitioned { cols, .. } = &op.spec else {
        return false;
    };
    let mut needed = HashSet::new();
    column_ids(scalars, probe_expr, &mut needed);
    if needed.is_empty() {
        return false;
    }
    needed.iter().all(|id| cols.contains(id))
}

/// How a probe runtime filter may cross a shuffle (`HashPartitioned`) exchange
/// on its way down to the producing fragment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrossExchangeMode {
    /// Distribution unknown — never cross.
    Disabled,
    /// Broadcast: the build RF is complete at every instance
    /// (StarRocks `directly_send_broadcast_grf`), so it may cross any shuffle
    /// exchange unconditionally.
    Unconditional,
    /// Shuffle/Colocate: the merged (total) RF is only valid within the same
    /// partitioning, so it may cross an exchange ONLY when that exchange
    /// re-partitions on the probe key (StarRocks `canCrossExchangeNode`).
    KeyAligned,
}

impl From<&JoinDistribution> for CrossExchangeMode {
    fn from(d: &JoinDistribution) -> Self {
        match d {
            JoinDistribution::Broadcast => CrossExchangeMode::Unconditional,
            JoinDistribution::Shuffle | JoinDistribution::Colocate => CrossExchangeMode::KeyAligned,
            JoinDistribution::Unknown => CrossExchangeMode::Disabled,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ProbePushPolicy {
    allow_cross_exchange: bool,
    cross_exchange: CrossExchangeMode,
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

fn is_probe_semantic_boundary(node: &OptimizerPhysicalNode) -> bool {
    matches!(
        &node.op,
        Operator::PhysicalHashJoin(join) if join_is_outer_or_anti_boundary(join.join_type)
    )
}

/// Choose the deterministic representative of a bindable equivalence-set member
/// to place on a node: the member with the lexicographically smallest column-id
/// vector. All members are join-key-equal, so any is correct; a stable choice
/// keeps EXPLAIN goldens deterministic. When the set is a single expression
/// (the common M2 case) this returns that expression unchanged.
fn best_member(scalars: &ScalarArena, members: &[ScalarId]) -> Option<ScalarId> {
    members
        .iter()
        .copied()
        .min_by(|a, b| column_id_vec(scalars, *a).cmp(&column_id_vec(scalars, *b)))
}

/// Members of `members` bindable at `node` (a superset survives interior nodes;
/// each child re-filters). Preserves input order.
fn bindable_members(
    node: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
    members: &[ScalarId],
) -> Vec<ScalarId> {
    members
        .iter()
        .copied()
        .filter(|m| could_bound(node, scalars, *m))
        .collect()
}

/// Push a probe onto `node`, keyed on the best bindable member of `members`.
fn place_probe(
    node: &mut OptimizerPhysicalNode,
    scalars: &ScalarArena,
    filter_id: i32,
    members: &[ScalarId],
) -> bool {
    let Some(expr) = best_member(scalars, members) else {
        return false;
    };
    node.probe_runtime_filters.push(RuntimeFilterProbe {
        filter_id,
        probe_expr: expr,
    });
    true
}

/// Expand `members` by ONE equivalence hop across an INNER/SEMI `join` node,
/// using that join's own equi-conditions (their `ScalarId`s already exist, so no
/// arena interning is needed). For each eq-condition `l = r`, if a member shares
/// a column with one side, the OTHER side is added — but ONLY when that other
/// side survives in `join.output_columns`.
///
/// The output-survival gate is the correctness pivot for semi/anti joins: a
/// LEFT/RIGHT semi join drops its build side's columns from its output, so the
/// build-side key never enters the set and no probe can be chased into the
/// existence-only child (which would drop rows that legitimately pass the
/// upstream join). For inner joins both sides survive, so both partners expand.
/// Outer/anti joins never reach here — they are stopped earlier as semantic
/// boundaries. Results are deduped by column id via the incoming set.
fn expand_probe_set_across_join(
    join: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
    eq_conditions: &[PhysicalHashJoinEqCondition],
    members: &[ScalarId],
) -> Vec<ScalarId> {
    let mut expanded: Vec<ScalarId> = members.to_vec();
    let mut seen: HashSet<Vec<ColumnId>> =
        members.iter().map(|m| column_id_vec(scalars, *m)).collect();
    let output_cols = child_column_set(join);

    // A partner is only admissible if all of its columns survive in the join's
    // output (excludes the dropped side of a semi join).
    let survives = |expr: ScalarId| -> bool {
        let cols = column_id_vec(scalars, expr);
        !cols.is_empty() && cols.iter().all(|c| output_cols.contains(c))
    };

    for eq in eq_conditions {
        if eq.null_safe {
            // Null-safe keys carry different match semantics; do not treat them
            // as plain equivalences for RF expansion.
            continue;
        }
        let left_cols = column_id_vec(scalars, eq.left);
        let right_cols = column_id_vec(scalars, eq.right);
        if left_cols.is_empty() || right_cols.is_empty() {
            continue;
        }
        for member in members {
            let member_cols = column_id_vec(scalars, *member);
            // Add the partner side when the member matches the opposite side.
            if member_cols == left_cols && survives(eq.right) && seen.insert(right_cols.clone()) {
                expanded.push(eq.right);
            }
            if member_cols == right_cols && survives(eq.left) && seen.insert(left_cols.clone()) {
                expanded.push(eq.left);
            }
        }
    }
    expanded
}

/// Descend into `node` carrying a SET of equivalent probe expressions (all
/// sharing `filter_id`) and place a rewritten probe at the deepest binder in
/// EACH child subtree that binds a set member. Returns `true` when at least one
/// probe was placed in this subtree.
///
/// Rules (M3 — set-carrying, multi-target; supersedes the M2 single-probe form):
/// 1. Crossable exchange (HashPartitioned/shuffle): descend transparently with
///    the subset of members permitted to cross THIS node — `Unconditional`
///    (complete build RF, e.g. Broadcast) lets every member cross; `KeyAligned`
///    (Shuffle/Colocate) lets a member cross only when the exchange re-partitions
///    on that member (`hash_partition_carries_probe_key`); `Disabled` never
///    crosses. When no member may cross, the exchange falls through to rule 2.
/// 2. Non-crossable exchange (Gather/Any): hard fragment boundary — stop.
/// 3. Filter the set to members bindable at `node`; if none bind, stop.
/// 4. Outer/anti/null-preserving joins are semantic boundaries: place ONE best
///    bindable member here and stop — never expand across, never descend past.
/// 5. If `node` is an INNER/SEMI join, expand the set by one hop using its
///    equi-conditions (partner columns admitted only if they survive the join's
///    output — see `expand_probe_set_across_join`).
/// 6. Descend into EACH child with the (expanded) set; a child selects its own
///    bindable member. At most one probe per child subtree emerges.
/// 7. If NO child accepted a probe, place ONE best bindable member on `node`
///    itself (the deepest reachable binder in this subtree).
fn push_probe_down(
    node: &mut OptimizerPhysicalNode,
    scalars: &ScalarArena,
    filter_id: i32,
    members: &[ScalarId],
    policy: ProbePushPolicy,
) -> bool {
    if policy.allow_cross_exchange && distribution_is_crossable(node) {
        let crossable: Vec<ScalarId> = match policy.cross_exchange {
            CrossExchangeMode::Unconditional => members.to_vec(),
            CrossExchangeMode::KeyAligned => members
                .iter()
                .copied()
                .filter(|m| hash_partition_carries_probe_key(node, scalars, *m))
                .collect(),
            CrossExchangeMode::Disabled => Vec::new(),
        };
        if !crossable.is_empty() {
            if let Some(child) = node.children.first_mut() {
                return push_probe_down(child, scalars, filter_id, &crossable, policy);
            }
            return false;
        }
        // No member may cross this exchange: fall through so it is treated as a
        // hard boundary (RF stays build-only for this branch).
    }
    if is_exchange(node) {
        return false;
    }

    let bindable = bindable_members(node, scalars, members);
    if bindable.is_empty() {
        return false;
    }

    // Outer/anti/null-preserving boundary: place the best member here and stop.
    // Never expand the equivalence set across it, never descend past it.
    if is_probe_semantic_boundary(node) {
        return place_probe(node, scalars, filter_id, &bindable);
    }

    // Expand the equivalence set by one hop across an inner/semi join.
    let descend_set = match &node.op {
        Operator::PhysicalHashJoin(join) => {
            let eq_conditions = join.eq_conditions.clone();
            expand_probe_set_across_join(node, scalars, &eq_conditions, &bindable)
        }
        _ => bindable.clone(),
    };

    // Descend into EACH child; each subtree contributes at most one probe.
    // Unlike the M2 single-probe form, we visit ALL children (no first-match
    // short-circuit) so both sides of an inner join can each receive a probe
    // for this filter_id.
    let mut placed_in_child = false;
    for child in &mut node.children {
        if push_probe_down(child, scalars, filter_id, &descend_set, policy) {
            placed_in_child = true;
        }
    }
    if placed_in_child {
        return true;
    }

    // Deepest reachable binder: no child accepted, so place here.
    place_probe(node, scalars, filter_id, &bindable)
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
    node: &OptimizerPhysicalNode,
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
    node: &mut OptimizerPhysicalNode,
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
    // `build_size`/`probe_size` stay full-row: they feed the selectivity gate
    // below, which estimates scanned rows, not the RF footprint.
    let build_size = node.children[sides.build_child].stats.compute_size();
    let probe_size = node.children[sides.probe_child].stats.compute_size();

    // The build *gate* must measure only the RF key columns — a bloom/min-max RF
    // holds just the join keys, not the whole build row. Full-row width wrongly
    // inflated the gate and dropped RFs on wide build sides (e.g. q18 node9's
    // o_orderkey RF, killed by a c_name VARCHAR payload). Empty key set (no eq
    // orients) falls back to full-column width, preserving prior behavior.
    let mut build_key_columns: HashSet<ColumnId> = HashSet::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if let Some(oriented) = orient_rf_key(node, scalars, sides, expr_order, eq) {
            column_ids(scalars, oriented.build_expr, &mut build_key_columns);
        }
    }
    let build_key_column_ids: Vec<ColumnId> = build_key_columns.into_iter().collect();
    let build_key_size = node.children[sides.build_child]
        .stats
        .compute_size_for_columns(&build_key_column_ids);

    // Cast session thresholds (u64 bytes) to f64 for size comparisons.
    let build_max = options.rf_build_max_bytes as f64;
    let build_min = options.rf_build_min_bytes as f64;
    let probe_min = options.rf_probe_min_bytes as f64;
    let min_sel = options.rf_probe_min_selectivity;

    // Build gate: Shuffle joins are rejected if build side is too large or empty.
    if !build_gate_passes(&distribution, build_key_size, build_max) {
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

    // Push each probe descriptor down through the true probe child, carrying an
    // equivalence SET seeded with the oriented probe key. The set is expanded by
    // one hop across each inner/semi join descended through, and a rewritten
    // probe (sharing the one filter id) is placed at the deepest binder in each
    // child subtree that binds a set member. Stops at exchange (fragment)
    // boundaries unless `CrossExchangeMode` permits crossing, and never expands
    // across outer/anti boundaries (see `push_probe_down`). If no member binds
    // anywhere the RF is build-only (probe remains unplaced).
    let policy = ProbePushPolicy {
        allow_cross_exchange: options.allow_cross_exchange_rf,
        cross_exchange: CrossExchangeMode::from(&distribution),
    };
    for d in &descs {
        // Descend into the true probe side, seeded with the single probe key.
        let _ = push_probe_down(
            &mut node.children[sides.probe_child],
            scalars,
            d.filter_id,
            &[d.probe_expr],
            policy,
        );
    }

    node.build_runtime_filters = descs;
}

#[cfg(test)]
fn test_null_expr(arena: &mut ScalarArena) -> ScalarId {
    use crate::sql::analysis::LiteralValue;
    use crate::sql::optimizer::scalar::HashableLiteral;
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
        arrow::datatypes::DataType::Null,
        true,
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
    };
    use crate::sql::optimizer::physical_tree::{OptimizerPhysicalNode, attach_scalar_arena};
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;

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

    fn with_scalars(
        mut plan: OptimizerPhysicalNode,
        scalars: ScalarArena,
    ) -> OptimizerPhysicalNode {
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn leaf(rows: f64, oc: OutputColumn) -> OptimizerPhysicalNode {
        OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    pub(crate) fn inner_join_two_scans() -> OptimizerPhysicalNode {
        inner_join_two_scans_with_key_types(
            arrow::datatypes::DataType::Int32,
            arrow::datatypes::DataType::Int32,
        )
    }

    pub(crate) fn inner_join_two_scans_with_key_types(
        left_type: arrow::datatypes::DataType,
        right_type: arrow::datatypes::DataType,
    ) -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col_with_type(1, "lc", left_type);
        let (roc, rexpr) = col_with_type(2, "rc", right_type);
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn hash_join_two_scans(join_type: JoinKind) -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn cross_hash_join_without_eq_conditions() -> OptimizerPhysicalNode {
        let scalars = ScalarArena::new();
        let (loc, _) = col(1, "lc");
        let (roc, _) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn inner_join_with_swapped_eq_labels() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let left = leaf(1_000_000.0, loc.clone());
        let right = leaf(10.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn shuffle_join(build_rows: f64, probe_rows: f64) -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc");
        let (roc, rexpr) = col(2, "rc");
        let probe = leaf(probe_rows, loc.clone());
        let build = leaf(build_rows, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Shuffle inner join whose build side (children[1]) carries a narrow key
    /// column plus a WIDE payload column. Full-row build size blows past
    /// rf_build_max_bytes (64MB) while the key-only size stays well under it —
    /// exercises the build-key-width gate (M0).
    pub(crate) fn wide_build_shuffle_join(
        build_rows: f64,
        probe_rows: f64,
    ) -> OptimizerPhysicalNode {
        use crate::sql::optimizer::statistics::ColumnStatistic;

        let mut scalars = ScalarArena::new();
        let (probe_oc, probe_expr) = col(1, "probe_key"); // Int32 probe key
        let (build_key_oc, build_key_expr) = col(2, "build_key"); // Int32 build key
        let (payload_oc, _payload_expr) =
            col_with_type(3, "payload", arrow::datatypes::DataType::Utf8);

        let width = |w: f64| ColumnStatistic {
            average_row_size: w,
            ..ColumnStatistic::unknown()
        };

        // probe leaf: just the key column.
        let mut probe = leaf(probe_rows, probe_oc.clone());
        probe.stats.column_statistics = [(probe_oc.column_id, width(8.0))].into_iter().collect();

        // build leaf: narrow key (8 bytes) + wide payload (256 bytes).
        let mut build = leaf(build_rows, build_key_oc.clone());
        build.output_columns = vec![build_key_oc.clone(), payload_oc.clone()];
        build.stats.column_statistics = [
            (build_key_oc.column_id, width(8.0)),
            (payload_oc.column_id, width(256.0)),
        ]
        .into_iter()
        .collect();

        let plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &probe_expr, &build_key_expr)],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
            }),
            children: vec![probe, build], // [0]=probe, [1]=build (rf_sides_for_join Inner)
            stats: Statistics {
                output_row_count: build_rows.min(probe_rows),
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![probe_oc, build_key_oc, payload_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Shuffle join where the probe child is a `PhysicalDistribution(HashPartitioned)`
    /// over a leaf scan. Tests that probe RFs cross the shuffle exchange to reach the
    /// underlying scan rather than stopping at the exchange boundary.
    pub(crate) fn shuffle_join_with_probe_exchange() -> OptimizerPhysicalNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        // probe side: PhysicalDistribution(HashPartitioned on col 1) over a leaf scan.
        let scan = leaf(1_000_000.0, loc.clone());
        let exch = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc.clone()], // exchange preserves column 1
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        // build side SMALL so the build gate and probe gate both pass
        // (build_size = 100 * 8 = 800 bytes, well below BUILD_MIN 128KB).
        let build = leaf(100.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Shuffle join where the probe child is a `PhysicalDistribution(HashPartitioned)`
    /// over a leaf scan, but the exchange re-partitions on a column UNRELATED to the
    /// probe key. Tests that a Shuffle probe RF stops at (does not cross) an exchange
    /// whose partitioning does not carry the probe key — crossing it would place a
    /// probe that drops rows which legitimately join.
    pub(crate) fn shuffle_join_with_misaligned_probe_exchange() -> OptimizerPhysicalNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        let (unrelated_oc, _unrelated_expr) = col(99, "unrelated_key"); // exchange partition key
        // probe side: PhysicalDistribution(HashPartitioned on col 99) over a leaf scan
        // that outputs both the probe column (1) and the unrelated key (99).
        let mut scan = leaf(1_000_000.0, loc.clone());
        scan.output_columns = vec![loc.clone(), unrelated_oc.clone()];
        let exch = OptimizerPhysicalNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols: vec![unrelated_oc.column_id],
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            // exchange preserves both columns; probe key (1) is still bindable here,
            // but the partition key (99) is unrelated to it.
            output_columns: vec![loc.clone(), unrelated_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        // build side SMALL so the build gate and probe gate both pass
        // (build_size = 100 * 8 = 800 bytes, well below BUILD_MIN 128KB).
        let build = leaf(100.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn broadcast_join_with_probe_exchange() -> OptimizerPhysicalNode {
        use crate::sql::optimizer::operator::PhysicalDistributionOp;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        let scan = leaf(1_000_000.0, loc.clone());
        let exch = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc.clone()],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(100.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn inner_join_over_left_outer_probe_child() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (preserved_oc, preserved_expr) = col(1, "preserved");
        let (outer_build_oc, outer_build_expr) = col(2, "outer_build");
        let (top_build_oc, top_build_expr) = col(3, "top_build");
        let preserved = leaf(1_000_000.0, preserved_oc.clone());
        let outer_build = leaf(10.0, outer_build_oc.clone());
        let left_outer = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![preserved_oc.clone(), outer_build_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let top_build = leaf(10.0, top_build_oc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![preserved_oc, top_build_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    pub(crate) fn join_with_project_over_probe_scan() -> OptimizerPhysicalNode {
        use crate::sql::optimizer::operator::ProjectOp;
        let mut scalars = ScalarArena::new();
        let (loc, lexpr) = col(1, "lc"); // probe column
        let (roc, rexpr) = col(2, "rc"); // build column
        // probe side: PhysicalProject(node) over a leaf scan; both expose column 1.
        let scan = leaf(1_000_000.0, loc.clone());
        let project = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc.clone()], // project passes column 1 through
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let build = leaf(10.0, roc.clone());
        let plan = OptimizerPhysicalNode {
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
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![loc, roc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Transitive-equivalence probe subtree (M3). Shape:
    ///
    /// ```text
    ///        TopJoin (Inner): eq (b = c)        <- build RF on c
    ///        /              \
    ///     J_ab (Inner)       scan_C(col 3)      <- BUILD side of TopJoin
    ///     eq (a = b)
    ///     /       \
    ///  scan_A(1)   scan_B(2)
    /// ```
    ///
    /// The build key `c` (col 3) is transitively equal to both `b` (col 2, the
    /// direct probe key) and `a` (col 1) via `J_ab`'s `a = b`. An M3 pushdown
    /// must place one probe (sharing the single filter id) on BOTH `scan_A` and
    /// `scan_B`, and NONE on the build-side `scan_C`.
    pub(crate) fn transitive_inner_join_probe_subtree() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (a_oc, a_expr) = col(1, "a");
        let (b_oc, b_expr) = col(2, "b");
        let (c_oc, c_expr) = col(3, "c");

        let scan_a = leaf(1_000_000.0, a_oc.clone());
        let scan_b = leaf(1_000_000.0, b_oc.clone());
        // J_ab: inner join on a = b, outputs both a and b.
        let j_ab = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &a_expr, &b_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![scan_a, scan_b],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![a_oc.clone(), b_oc.clone()],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        // Build side of the top join: small so gates pass.
        let scan_c = leaf(10.0, c_oc.clone());
        let plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &b_expr, &c_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![j_ab, scan_c], // children[0]=probe subtree, children[1]=build
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![a_oc, b_oc, c_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Probe subtree with an equivalent column that lives BEYOND a left-outer
    /// boundary (M3 outer/anti stop). Shape:
    ///
    /// ```text
    ///        TopJoin (Inner): eq (b = c)      <- build RF on c
    ///        /              \
    ///     LeftOuter          scan_C(col 3)    <- BUILD side of TopJoin
    ///     eq (b = d)
    ///     /       \
    ///  scan_B(2)   scan_D(4)  <- null-supplying side
    /// ```
    ///
    /// The direct probe key `b` (col 2) is equal to `d` (col 4) ONLY through the
    /// null-preserving left-outer join. An M3 pushdown must NOT expand across the
    /// outer boundary: it places the probe on the LeftOuter node itself and
    /// leaves `scan_D` (and `scan_B`) unfiltered.
    pub(crate) fn transitive_probe_subtree_over_outer_boundary() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (b_oc, b_expr) = col(2, "b");
        let (d_oc, d_expr) = col(4, "d");
        let (c_oc, c_expr) = col(3, "c");

        let scan_b = leaf(1_000_000.0, b_oc.clone());
        let scan_d = leaf(1_000_000.0, d_oc.clone());
        let left_outer = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::LeftOuter,
                eq_conditions: vec![eq(&mut scalars, &b_expr, &d_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![scan_b, scan_d],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![b_oc.clone(), d_oc.clone()],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let scan_c = leaf(10.0, c_oc.clone());
        let plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &b_expr, &c_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left_outer, scan_c], // children[0]=probe subtree, children[1]=build
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![b_oc, d_oc, c_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        with_scalars(plan, scalars)
    }

    /// Semi-join-interior probe subtree (M3 output-survival gate). Shape:
    ///
    /// ```text
    ///        TopJoin (Inner): eq (a = c)        <- build RF on c
    ///        /              \
    ///     J_semi (LeftSemi)   scan_C(col 3)     <- BUILD side of TopJoin
    ///     eq (a = b)
    ///     /       \
    ///  scan_A(1)   scan_B(2)
    /// ```
    ///
    /// `J_semi` is a `LeftSemi` join, so per `join_output_columns`
    /// (`extract.rs`) its own `output_columns` is `children[0]`'s columns only
    /// — `a` (col 1) — never `b` (col 2). `scan_B` is the existence-only side:
    /// it decides whether an `A` row has a match but never contributes columns
    /// upward. Because `b` never survives into `J_semi`'s output, the TopJoin's
    /// probe key must itself be `a` (the only column visible above `J_semi`);
    /// `b` only re-enters consideration if `expand_probe_set_across_join`'s
    /// output-survival gate is bypassed.
    ///
    /// The direct probe member `a` is one side of `J_semi`'s own `a = b`
    /// equi-condition, so a pushdown that ignores output-survival would expand
    /// the set to include `b` and then place a second probe on `scan_B`. That
    /// would be wrong: `scan_B` only participates in existence testing for the
    /// semi join, and a probe runtime filter derived from `c` (unrelated to the
    /// semi join's own semantics) could drop `B` rows that a matching `A` row
    /// depends on, silently turning matching `A` rows into non-matches.
    ///
    /// An M3 pushdown must place the probe ONLY on `scan_A` (the preserved
    /// side) and NEVER on `scan_B` (the dropped, existence-only side) — this is
    /// exactly the invariant `expand_probe_set_across_join`'s survival gate
    /// exists to protect.
    pub(crate) fn semi_join_interior_probe_subtree() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let (a_oc, a_expr) = col(1, "a");
        let (b_oc, b_expr) = col(2, "b");
        let (c_oc, c_expr) = col(3, "c");

        let scan_a = leaf(1_000_000.0, a_oc.clone());
        let scan_b = leaf(1_000_000.0, b_oc.clone());
        // J_semi: LEFT SEMI join on a = b. Only scan_A's columns (children[0])
        // survive into output_columns; scan_B (children[1]) is existence-only
        // and is dropped, mirroring `join_output_columns`'s LeftSemi arm.
        let j_semi = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::LeftSemi,
                eq_conditions: vec![eq(&mut scalars, &a_expr, &b_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![scan_a, scan_b],
            stats: Statistics {
                output_row_count: 1_000_000.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![a_oc.clone()], // LeftSemi drops scan_B's columns.
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        // Build side of the top join: small so gates pass.
        let scan_c = leaf(10.0, c_oc.clone());
        let plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq(&mut scalars, &a_expr, &c_expr)],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![j_semi, scan_c], // children[0]=probe subtree, children[1]=build
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![a_oc, c_oc],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
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

    fn annotate_test(plan: &mut OptimizerPhysicalNode, options: &OptimizerOptions) {
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

    /// Returns the single column id targeted by the probe for `filter_id` on
    /// `node`, asserting there is at most ONE probe for that filter id here
    /// (the "one probe per equivalence class per subtree" invariant) and that
    /// its key is a lone ColumnRef (all RF keys in these fixtures are). A node
    /// may still carry probes for OTHER filter ids (e.g. a nested join's own RF).
    fn probe_column_for(
        node: &OptimizerPhysicalNode,
        scalars: &ScalarArena,
        filter_id: i32,
    ) -> Option<ColumnId> {
        let matching: Vec<_> = node
            .probe_runtime_filters
            .iter()
            .filter(|p| p.filter_id == filter_id)
            .collect();
        assert!(
            matching.len() <= 1,
            "at most one probe per equivalence class (filter id) per subtree"
        );
        matching.first().map(|p| {
            let cols = column_id_vec(scalars, p.probe_expr);
            assert_eq!(cols.len(), 1, "fixture probe keys are single columns");
            cols[0]
        })
    }

    #[test]
    fn transitive_bilateral_places_probe_on_both_equivalent_scans() {
        // build on c (col 3); probe subtree joins a=b (J_ab) then b=c (top).
        // The single RF must reach BOTH transitively-equal scans: scan_A(col 1)
        // and scan_B(col 2), sharing one filter id.
        let mut join = super::test_support::transitive_inner_join_probe_subtree();
        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1, "one build RF expected");
        let filter_id = join.build_runtime_filters[0].filter_id;

        let scalars = join.execution_props.scalar_arena.as_deref().unwrap();
        let j_ab = &join.children[0];
        let scan_a = &j_ab.children[0];
        let scan_b = &j_ab.children[1];

        // Neither the top probe child (J_ab) nor an interior node keeps the probe:
        // it is pushed all the way to the two leaf scans.
        assert!(
            !j_ab
                .probe_runtime_filters
                .iter()
                .any(|p| p.filter_id == filter_id),
            "the top join's probe should not stop at the intermediate inner join"
        );
        assert_eq!(
            probe_column_for(scan_a, scalars, filter_id),
            Some(ColumnId::new_for_test(1)),
            "equivalent column a (col 1) must receive the top join's probe"
        );
        assert_eq!(
            probe_column_for(scan_b, scalars, filter_id),
            Some(ColumnId::new_for_test(2)),
            "direct probe column b (col 2) must receive the top join's probe"
        );
    }

    #[test]
    fn transitive_pushdown_does_not_place_probe_on_build_scan() {
        // Correctness invariant 2: a probe must NEVER land on the building join's
        // own build-side scan (that scan would wait on an RF built from itself).
        let mut join = super::test_support::transitive_inner_join_probe_subtree();
        annotate_test(&mut join, &OptimizerOptions::default_settings());

        let scan_c = &join.children[1]; // build side of the top join
        assert!(
            scan_c.probe_runtime_filters.is_empty(),
            "the build-side scan must never receive a probe runtime filter"
        );
    }

    #[test]
    fn equivalent_column_beyond_outer_boundary_gets_no_probe() {
        // Correctness invariant 1: the equivalence set must NOT expand across a
        // null-preserving (left-outer) boundary. `d` (col 4) is equal to the
        // probe key `b` only through the outer join, so it must stay unfiltered;
        // the probe stops on the outer-join node itself.
        let mut join = super::test_support::transitive_probe_subtree_over_outer_boundary();
        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1, "one build RF expected");
        let filter_id = join.build_runtime_filters[0].filter_id;
        let scalars = join.execution_props.scalar_arena.as_deref().unwrap();

        let left_outer = &join.children[0];
        let scan_b = &left_outer.children[0];
        let scan_d = &left_outer.children[1];

        // Probe stops ON the outer-join boundary, bound to the surviving probe
        // key `b` (col 2) — never expanded to the far side.
        assert_eq!(
            probe_column_for(left_outer, scalars, filter_id),
            Some(ColumnId::new_for_test(2)),
            "probe should rest on the outer-join boundary, keyed on b"
        );
        assert!(
            scan_b.probe_runtime_filters.is_empty(),
            "probe must not descend past the outer boundary into the preserved side"
        );
        assert!(
            scan_d.probe_runtime_filters.is_empty(),
            "the equivalent column beyond the outer boundary must get NO probe"
        );
    }

    #[test]
    fn semi_join_interior_survival_gate_blocks_existence_only_side() {
        // Output-survival gate (M3 correctness pivot, expand_probe_set_across_join):
        // J_semi (LeftSemi, a = b) sits interior to the top join's probe subtree.
        // The direct probe member `a` is preserved-side (survives J_semi's
        // output); its eq-partner `b` lives on the existence-only side, which
        // LeftSemi drops from its output. The gate must admit `a` alone and
        // refuse to expand the equivalence set to `b`, so the probe reaches
        // ONLY scan_A and NEVER scan_B.
        let mut join = super::test_support::semi_join_interior_probe_subtree();
        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1, "one build RF expected");
        let filter_id = join.build_runtime_filters[0].filter_id;
        let scalars = join.execution_props.scalar_arena.as_deref().unwrap();

        let j_semi = &join.children[0];
        let scan_a = &j_semi.children[0];
        let scan_b = &j_semi.children[1];

        // The probe descends past J_semi itself (it is not a semantic boundary
        // for RF purposes — only outer/anti joins stop the pushdown).
        assert!(
            !j_semi
                .probe_runtime_filters
                .iter()
                .any(|p| p.filter_id == filter_id),
            "the probe should not rest on the semi join node itself"
        );
        assert_eq!(
            probe_column_for(scan_a, scalars, filter_id),
            Some(ColumnId::new_for_test(1)),
            "the preserved-side scan (a, col 1) must receive the probe"
        );
        assert!(
            scan_b.probe_runtime_filters.is_empty(),
            "the existence-only side (scan_B) must NEVER receive a probe: it \
             would drop join-preserving rows for the outer semi join"
        );
    }

    #[test]
    fn keeps_rf_when_wide_build_fits_on_key_columns() {
        // build 1M rows: full-row 1M*(8+256)=264MB > 64MB (old gate drops it),
        // key-only 1M*8=8MB < 64MB (fixed gate keeps it). probe 100M*8=800MB so
        // selectivity passes (full 264MB / 800MB = 0.33 < 0.5).
        let mut j = super::test_support::wide_build_shuffle_join(1_000_000.0, 100_000_000.0);
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(
            j.build_runtime_filters.len(),
            1,
            "wide build side must not drop the RF: the build gate should measure \
             key-column width, not full-row width"
        );
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
    fn key_aligned_partitioned_rf_crosses_exchange_when_flag_enabled() {
        // M2: a Shuffle probe RF now crosses a HashPartitioned exchange when that
        // exchange re-partitions on the probe key (`shuffle_join_with_probe_exchange`
        // partitions on the same column the join probes). See
        // `cross_exchange_tests::shuffle_rf_stops_at_misaligned_exchange` for the
        // complementary case where the exchange key does NOT carry the probe key.
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = true;

        annotate_test(&mut j, &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "key-aligned probe RF must not stop at the exchange"
        );
        assert_eq!(
            exch.children[0].probe_runtime_filters.len(),
            1,
            "key-aligned partitioned probe RF should cross the exchange"
        );
    }

    #[test]
    fn key_aligned_partitioned_rf_does_not_cross_exchange_when_flag_disabled() {
        // The session override from Task 1 (`allow_cross_exchange_rf = false`)
        // must still block crossing even when the exchange is key-aligned.
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        let mut opts = OptimizerOptions::default_settings();
        opts.allow_cross_exchange_rf = false;

        annotate_test(&mut j, &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "probe must not be placed on the exchange"
        );
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "crossing must stay disabled when allow_cross_exchange_rf is false, \
             even for a key-aligned exchange"
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
    fn key_aligned_probe_crosses_fragment_by_default() {
        // M2: with default options (`allow_cross_exchange_rf` defaults to true),
        // a Shuffle probe RF crosses a shuffle exchange re-partitioned on the
        // probe key without needing any explicit override.
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(j.build_runtime_filters.len(), 1, "build RF still expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "probe must not be placed on the exchange"
        );
        assert_eq!(
            exch.children[0].probe_runtime_filters.len(),
            1,
            "key-aligned probe RF should cross the exchange by default"
        );
    }

    #[test]
    fn misaligned_probe_stays_within_fragment_by_default() {
        // Default: a Shuffle probe RF must not cross an exchange partitioned on
        // an unrelated key. It falls back to build-only — the probe stays
        // unplaced above the exchange.
        let mut j = super::test_support::shuffle_join_with_misaligned_probe_exchange();
        annotate_test(&mut j, &OptimizerOptions::default_settings());
        assert_eq!(j.build_runtime_filters.len(), 1, "build RF still expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "probe must not be placed on the exchange"
        );
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "misaligned probe must NOT cross the exchange (within-fragment fallback)"
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
            Some(crate::sql::optimizer::physical_tree::JoinExecutionDistribution::Partitioned);
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
                crate::sql::optimizer::physical_tree::JoinExecutionDistribution::Broadcast,
                JoinDistribution::Broadcast,
            ),
            (
                crate::sql::optimizer::physical_tree::JoinExecutionDistribution::Colocate,
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

        for kind in [JoinKind::Inner, JoinKind::RightOuter, JoinKind::LeftSemi] {
            let mut join = super::test_support::hash_join_two_scans(kind);
            annotate_test(&mut join, &OptimizerOptions::default_settings());
            assert_eq!(
                join.build_runtime_filters.len(),
                1,
                "{kind:?} should build an RF"
            );
        }

        for kind in [JoinKind::LeftSemi, JoinKind::RightSemi] {
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
            JoinKind::RightAnti,
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
        assert!(
            rf_sides_for_join(JoinKind::LeftSemi).is_some(),
            "Semi joins can early-filter their probe side after completion-safe RF lifecycle"
        );
        assert!(
            rf_sides_for_join(JoinKind::LeftAnti).is_none(),
            "Anti joins remain disabled until null and exclusion semantics are reviewed"
        );
    }

    #[test]
    fn right_semi_join_builds_probe_side_runtime_filter() {
        let mut join = super::test_support::hash_join_two_scans(JoinKind::RightSemi);
        annotate_test(&mut join, &OptimizerOptions::default_settings());

        assert_eq!(join.build_runtime_filters.len(), 1);
        assert!(
            !join.children[0].probe_runtime_filters.is_empty(),
            "right semi joins should place probe RFs on the left/probe child"
        );
        assert!(
            join.children[1].probe_runtime_filters.is_empty(),
            "right semi joins must not place build-side probe RFs"
        );
    }

    #[test]
    fn rf_orients_swapped_eq_labels_by_child_column_ids() {
        let mut join = super::test_support::inner_join_with_swapped_eq_labels();
        annotate_test(&mut join, &OptimizerOptions::default_settings());
        assert_eq!(join.build_runtime_filters.len(), 1);
        assert_eq!(join.children[0].probe_runtime_filters.len(), 1);
        assert!(join.children[1].probe_runtime_filters.is_empty());
        let scalars = join.execution_props.scalar_arena.as_deref().unwrap();
        assert_eq!(
            column_id_vec(scalars, join.build_runtime_filters[0].build_expr),
            vec![crate::sql::column_id::ColumnId::new_for_test(2)]
        );
        assert_eq!(
            column_id_vec(scalars, join.build_runtime_filters[0].probe_expr),
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

    fn probe_runtime_filter_count(node: &OptimizerPhysicalNode) -> usize {
        node.probe_runtime_filters.len()
            + node
                .children
                .iter()
                .map(probe_runtime_filter_count)
                .sum::<usize>()
    }
}

#[cfg(test)]
mod cross_exchange_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::optimizer::operator::PhysicalDistributionOp;
    use crate::sql::optimizer::physical_tree::{OptimizerExplainStats, PlanExecutionProps};
    use crate::sql::optimizer::property::{DistributionSpec, HashSource};
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;

    #[test]
    fn mode_from_distribution_maps_correctly() {
        assert_eq!(
            CrossExchangeMode::from(&JoinDistribution::Broadcast),
            CrossExchangeMode::Unconditional
        );
        assert_eq!(
            CrossExchangeMode::from(&JoinDistribution::Shuffle),
            CrossExchangeMode::KeyAligned
        );
        assert_eq!(
            CrossExchangeMode::from(&JoinDistribution::Colocate),
            CrossExchangeMode::KeyAligned
        );
        assert_eq!(
            CrossExchangeMode::from(&JoinDistribution::Unknown),
            CrossExchangeMode::Disabled
        );
    }

    /// Minimal `PhysicalDistribution(HashPartitioned)` node over no children —
    /// `hash_partition_carries_probe_key` only inspects `node.op`, so a childless
    /// stub is sufficient and avoids coupling this isolated helper test to the
    /// heavier join-tree fixtures in `test_support`.
    fn distribution_node(cols: Vec<ColumnId>) -> OptimizerPhysicalNode {
        OptimizerPhysicalNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned {
                    cols,
                    source: HashSource::ShuffleJoin,
                },
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: OptimizerExplainStats::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    /// Builds a `ScalarArena` with a single `ColumnRef(probe_col)` scalar, plus
    /// a `HashPartitioned` node on `probe_col` (aligned) and one on a distinct,
    /// unrelated column (misaligned).
    fn build_alignment_fixture() -> (
        ScalarArena,
        ScalarId,
        OptimizerPhysicalNode,
        OptimizerPhysicalNode,
    ) {
        let probe_col = ColumnId::new_for_test(1);
        let unrelated_col = ColumnId::new_for_test(2);
        let mut scalars = ScalarArena::new();
        let probe_expr = intern_typed(
            &mut scalars,
            &TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: probe_col,
                    qualifier: None,
                    column: "probe_col".to_string(),
                },
                data_type: arrow::datatypes::DataType::Int32,
                nullable: true,
            },
        );
        let aligned_node = distribution_node(vec![probe_col]);
        let misaligned_node = distribution_node(vec![unrelated_col]);
        (scalars, probe_expr, aligned_node, misaligned_node)
    }

    #[test]
    fn key_alignment_requires_probe_col_in_shuffle_cols() {
        let (scalars, probe_expr, aligned_node, misaligned_node) = build_alignment_fixture();
        assert!(hash_partition_carries_probe_key(
            &aligned_node,
            &scalars,
            probe_expr
        ));
        assert!(!hash_partition_carries_probe_key(
            &misaligned_node,
            &scalars,
            probe_expr
        ));
    }

    #[test]
    fn shuffle_rf_crosses_key_aligned_exchange() {
        let mut j = super::test_support::shuffle_join_with_probe_exchange();
        let opts = OptimizerOptions::default_settings();

        let scalars = j
            .execution_props
            .scalar_arena
            .as_ref()
            .expect("plan must carry scalar arena")
            .clone();
        annotate(&mut j, scalars.as_ref(), &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "key-aligned probe RF must not stop at the exchange"
        );
        assert_eq!(
            exch.children[0].probe_runtime_filters.len(),
            1,
            "key-aligned Shuffle probe RF should cross into the scan below the exchange"
        );
    }

    #[test]
    fn shuffle_rf_stops_at_misaligned_exchange() {
        let mut j = super::test_support::shuffle_join_with_misaligned_probe_exchange();
        let opts = OptimizerOptions::default_settings();

        let scalars = j
            .execution_props
            .scalar_arena
            .as_ref()
            .expect("plan must carry scalar arena")
            .clone();
        annotate(&mut j, scalars.as_ref(), &opts);

        assert_eq!(j.build_runtime_filters.len(), 1, "build RF expected");
        let exch = &j.children[0];
        assert!(
            exch.probe_runtime_filters.is_empty(),
            "probe must not be placed on the exchange"
        );
        assert!(
            exch.children[0].probe_runtime_filters.is_empty(),
            "misaligned Shuffle probe RF must NOT cross the exchange"
        );
    }
}
