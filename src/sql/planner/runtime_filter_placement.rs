#![allow(dead_code)] // removed in Task 7 once wired into the bridge.

//! Planner-side runtime-filter placement pass (RFP-1).
//!
//! Runs on the single `PhysicalPlanNode` tree produced by the optimizer->planner
//! bridge, BEFORE `build_distributed_plan` fragments it. Annotates hash joins
//! with build-side `RuntimeFilterBuildIntent`s. Probe-side pushdown lands in
//! Task 6. Behavior is a byte-for-byte port of the retired
//! `optimizer::runtime_filter_pass` -- do not "improve" placement here; changes
//! belong in the RF baseline / producer arcs.

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::options::current_session_optimizer_settings;
use crate::sql::planner::physical_vocab::JoinDistribution;
use crate::sql::planner::plan::{
    PhysicalHashJoinEqCondition, PhysicalHashJoinNode, PhysicalPlanKind, PhysicalPlanNode,
};
use crate::sql::planner::{JoinExecutionMode, PhysicalPlanStats, RuntimeFilterBuildIntent};
use std::collections::HashSet;

/// Rule name recognized by `SET disable_optimizer_rules='RuntimeFilterPushDown'`.
/// Kept identical to the retired optimizer constant so the session knob is
/// unchanged. `optimizer::is_known_rule_name` references this (Task 7).
pub(crate) const RUNTIME_FILTER_RULE: &str = "RuntimeFilterPushDown";

/// RF placement config, derived from the session optimizer settings that the
/// engine installs before `optimize()` and that remain live on the same thread
/// through `optimizer_physical_to_distributed_plan`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimeFilterPlacementConfig {
    pub enabled: bool,
    pub build_max_bytes: u64,
    pub build_min_bytes: u64,
    pub probe_min_bytes: u64,
    pub probe_min_selectivity: f64,
    pub max_count: usize,
    pub allow_cross_exchange: bool,
}

impl RuntimeFilterPlacementConfig {
    pub(crate) fn from_current_session() -> Self {
        let s = current_session_optimizer_settings();
        Self {
            enabled: !s.disabled_rules.iter().any(|r| r == RUNTIME_FILTER_RULE),
            build_max_bytes: s.rf_build_max_bytes.unwrap_or(64 * 1024 * 1024),
            build_min_bytes: s.rf_build_min_bytes.unwrap_or(128 * 1024),
            probe_min_bytes: s.rf_probe_min_bytes.unwrap_or(100 * 1024),
            probe_min_selectivity: s.rf_probe_min_selectivity.unwrap_or(0.5),
            max_count: 1024,
            allow_cross_exchange: s.allow_cross_exchange_rf.unwrap_or(true),
        }
    }
}

const MAX_FINITE_SIZE: f64 = 1.0e300;

fn avg_row_size(stats: &PhysicalPlanStats) -> f64 {
    if stats.column_statistics.is_empty() {
        8.0
    } else {
        stats
            .column_statistics
            .values()
            .map(|c| c.average_row_size)
            .sum()
    }
}

fn stats_compute_size(stats: &PhysicalPlanStats) -> f64 {
    stats.output_row_count * avg_row_size(stats)
}

fn safe_output_row_count(stats: &PhysicalPlanStats) -> f64 {
    if stats.output_row_count.is_finite() && stats.output_row_count > 0.0 {
        stats.output_row_count
    } else if stats.output_row_count.is_infinite() && stats.output_row_count.is_sign_positive() {
        MAX_FINITE_SIZE
    } else {
        1.0
    }
}

fn add_safe_width(total: f64, width: Option<f64>) -> f64 {
    let contribution = match width {
        Some(width) if width.is_finite() && width > 0.0 => width,
        Some(width) if width.is_infinite() && width.is_sign_positive() => return MAX_FINITE_SIZE,
        _ => 8.0,
    };
    let total = total + contribution;
    if total.is_finite() && total >= 0.0 {
        total.min(MAX_FINITE_SIZE)
    } else {
        MAX_FINITE_SIZE
    }
}

fn safe_width_for_all_columns(stats: &PhysicalPlanStats) -> f64 {
    if stats.column_statistics.is_empty() {
        return 8.0;
    }
    let mut row_width = 0.0;
    for column in stats.column_statistics.values() {
        row_width = add_safe_width(row_width, Some(column.average_row_size));
        if row_width >= MAX_FINITE_SIZE {
            return MAX_FINITE_SIZE;
        }
    }
    row_width
}

fn safe_size(stats: &PhysicalPlanStats, row_width: f64) -> f64 {
    if row_width >= MAX_FINITE_SIZE {
        return MAX_FINITE_SIZE;
    }
    let row_count = safe_output_row_count(stats);
    if row_count >= MAX_FINITE_SIZE {
        return MAX_FINITE_SIZE;
    }
    let size = row_count * row_width;
    if size.is_finite() && size >= 0.0 {
        size.min(MAX_FINITE_SIZE)
    } else {
        MAX_FINITE_SIZE
    }
}

fn stats_compute_size_for_columns(stats: &PhysicalPlanStats, columns: &[ColumnId]) -> f64 {
    let row_width = if columns.is_empty() {
        safe_width_for_all_columns(stats)
    } else {
        let mut row_width = 0.0;
        for id in columns {
            row_width = add_safe_width(
                row_width,
                stats.column_statistics.get(id).map(|c| c.average_row_size),
            );
            if row_width >= MAX_FINITE_SIZE {
                return MAX_FINITE_SIZE;
            }
        }
        row_width
    };
    safe_size(stats, row_width)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JoinRfSides {
    probe_child: usize,
    build_child: usize,
}

fn rf_sides_for_join(kind: JoinKind) -> Option<JoinRfSides> {
    match kind {
        JoinKind::Inner | JoinKind::RightOuter | JoinKind::LeftSemi => Some(JoinRfSides {
            probe_child: 0,
            build_child: 1,
        }),
        JoinKind::LeftOuter
        | JoinKind::FullOuter
        | JoinKind::RightSemi
        | JoinKind::LeftAnti
        | JoinKind::RightAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::Cross => None,
    }
}

fn collect_typed_expr_column_ids(expr: &TypedExpr, out: &mut Vec<ColumnId>) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            if *column_id != ColumnId::UNSET {
                out.push(*column_id);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_typed_expr_column_ids(left, out);
            collect_typed_expr_column_ids(right, out);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => collect_typed_expr_column_ids(expr, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_typed_expr_column_ids(arg, out);
            }
        }
        ExprKind::LambdaFunction { body, .. } => collect_typed_expr_column_ids(body, out),
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_typed_expr_column_ids(arg, out);
            }
            for item in order_by {
                collect_typed_expr_column_ids(&item.expr, out);
            }
        }
        ExprKind::InList { expr, list, .. } => {
            collect_typed_expr_column_ids(expr, out);
            for item in list {
                collect_typed_expr_column_ids(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_typed_expr_column_ids(expr, out);
            collect_typed_expr_column_ids(low, out);
            collect_typed_expr_column_ids(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_typed_expr_column_ids(expr, out);
            collect_typed_expr_column_ids(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_typed_expr_column_ids(op, out);
            }
            for (when, then) in when_then {
                collect_typed_expr_column_ids(when, out);
                collect_typed_expr_column_ids(then, out);
            }
            if let Some(el) = else_expr {
                collect_typed_expr_column_ids(el, out);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_typed_expr_column_ids(arg, out);
            }
            for item in partition_by {
                collect_typed_expr_column_ids(item, out);
            }
            for item in order_by {
                collect_typed_expr_column_ids(&item.expr, out);
            }
        }
        ExprKind::Lambda { body, .. } => collect_typed_expr_column_ids(body, out),
        ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

fn column_id_vec(expr: &TypedExpr) -> Vec<ColumnId> {
    let mut ids = Vec::new();
    collect_typed_expr_column_ids(expr, &mut ids);
    ids.sort();
    ids.dedup();
    ids
}

fn child_column_set(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    node.output_columns.iter().map(|c| c.column_id).collect()
}

fn could_bound(node: &PhysicalPlanNode, probe_expr: &TypedExpr) -> bool {
    let needed = column_id_vec(probe_expr);
    if needed.is_empty() {
        return false;
    }
    let have = child_column_set(node);
    needed.iter().all(|id| have.contains(id))
}

fn expr_bound_child(node: &PhysicalPlanNode, expr: &TypedExpr) -> Option<usize> {
    let ids = column_id_vec(expr);
    if ids.is_empty() {
        return None;
    }

    let mut bound = None;
    for (idx, child) in node.children.iter().enumerate() {
        let cols = child_column_set(child);
        if ids.iter().all(|id| cols.contains(id)) {
            if bound.is_some() {
                return None;
            }
            bound = Some(idx);
        }
    }
    bound
}

fn rf_key_types_match(eq: &PhysicalHashJoinEqCondition) -> bool {
    eq.left.data_type == eq.right.data_type
}

#[derive(Clone, Debug)]
struct OrientedRfKey {
    build_expr: TypedExpr,
    probe_expr: TypedExpr,
    expr_order: usize,
}

fn resolve_join_distribution(join: &PhysicalHashJoinNode) -> JoinDistribution {
    match join.execution_mode {
        Some(JoinExecutionMode::Broadcast) => JoinDistribution::Broadcast,
        Some(JoinExecutionMode::Partitioned) => JoinDistribution::Shuffle,
        Some(JoinExecutionMode::Colocate) => JoinDistribution::Colocate,
        None => join.distribution.clone(),
    }
}

fn execution_mode_for(distribution: &JoinDistribution) -> JoinExecutionMode {
    match distribution {
        JoinDistribution::Broadcast => JoinExecutionMode::Broadcast,
        JoinDistribution::Shuffle => JoinExecutionMode::Partitioned,
        JoinDistribution::Colocate => JoinExecutionMode::Colocate,
        JoinDistribution::Unknown => unreachable!("Unknown distribution returns early"),
    }
}

fn orient_rf_key(
    node: &PhysicalPlanNode,
    sides: JoinRfSides,
    expr_order: usize,
    eq: &PhysicalHashJoinEqCondition,
) -> Option<OrientedRfKey> {
    let left_child = expr_bound_child(node, &eq.left)?;
    let right_child = expr_bound_child(node, &eq.right)?;

    if left_child == sides.probe_child && right_child == sides.build_child {
        Some(OrientedRfKey {
            build_expr: eq.right.clone(),
            probe_expr: eq.left.clone(),
            expr_order,
        })
    } else if left_child == sides.build_child && right_child == sides.probe_child {
        Some(OrientedRfKey {
            build_expr: eq.left.clone(),
            probe_expr: eq.right.clone(),
            expr_order,
        })
    } else {
        None
    }
}

/// Callers pass bindable members with non-empty column ids; choose a stable
/// representative for deterministic placement and EXPLAIN output.
fn best_member(members: &[TypedExpr]) -> Option<TypedExpr> {
    members
        .iter()
        .cloned()
        .min_by(|a, b| column_id_vec(a).cmp(&column_id_vec(b)))
}

fn bindable_members(node: &PhysicalPlanNode, members: &[TypedExpr]) -> Vec<TypedExpr> {
    members
        .iter()
        .filter(|m| could_bound(node, m))
        .cloned()
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrossExchangeMode {
    Disabled,
    Unconditional,
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

fn build_gate_passes(distribution: &JoinDistribution, build_size: f64, build_max: f64) -> bool {
    match distribution {
        JoinDistribution::Shuffle => !(build_size <= 0.0 || build_size > build_max),
        _ => true,
    }
}

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

#[derive(Clone, Copy, Debug)]
struct ProbePushPolicy {
    allow_cross_exchange: bool,
    cross_exchange: CrossExchangeMode,
}

fn push_probe_down(
    _node: &mut PhysicalPlanNode,
    _filter_id: i32,
    _members: &[TypedExpr],
    _policy: ProbePushPolicy,
) -> bool {
    false
}

/// Entry point for the planner-side RF placement pass once the bridge wires it.
pub(crate) fn place_runtime_filters(root: &mut PhysicalPlanNode) {
    let config = RuntimeFilterPlacementConfig::from_current_session();
    if !config.enabled {
        return;
    }
    let mut next_filter_id: i32 = 0;
    place_node(root, &config, &mut next_filter_id);
}

fn place_node(
    node: &mut PhysicalPlanNode,
    config: &RuntimeFilterPlacementConfig,
    next_filter_id: &mut i32,
) {
    walk_plan_mut(node, &mut |node| {
        place_current_node(node, config, next_filter_id);
    });
}

fn place_current_node(
    node: &mut PhysicalPlanNode,
    config: &RuntimeFilterPlacementConfig,
    next_filter_id: &mut i32,
) {
    if !config.enabled {
        return;
    }

    let (sides, eq_conditions, distribution) = {
        let PhysicalPlanKind::HashJoin(join) = &node.kind else {
            return;
        };
        let Some(sides) = rf_sides_for_join(join.join_type) else {
            return;
        };
        let max_child = sides.probe_child.max(sides.build_child);
        if node.children.len() <= max_child {
            return;
        }
        let distribution = resolve_join_distribution(join);
        if matches!(distribution, JoinDistribution::Unknown) {
            return;
        }
        (sides, join.eq_conditions.clone(), distribution)
    };

    let build_size = stats_compute_size(&node.children[sides.build_child].stats);
    let probe_size = stats_compute_size(&node.children[sides.probe_child].stats);

    let mut build_key_columns = HashSet::new();
    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if let Some(oriented) = orient_rf_key(node, sides, expr_order, eq) {
            build_key_columns.extend(column_id_vec(&oriented.build_expr));
        }
    }
    let mut build_key_column_ids: Vec<ColumnId> = build_key_columns.into_iter().collect();
    build_key_column_ids.sort();
    let build_key_size = stats_compute_size_for_columns(
        &node.children[sides.build_child].stats,
        &build_key_column_ids,
    );

    let build_max = config.build_max_bytes as f64;
    let build_min = config.build_min_bytes as f64;
    let probe_min = config.probe_min_bytes as f64;
    let min_sel = config.probe_min_selectivity;

    if !build_gate_passes(&distribution, build_key_size, build_max) {
        return;
    }

    let local = !matches!(distribution, JoinDistribution::Shuffle);
    let execution_mode = execution_mode_for(&distribution);
    let mut descs = Vec::new();

    for (expr_order, eq) in eq_conditions.iter().enumerate() {
        if eq.null_safe {
            continue;
        }
        if !rf_key_types_match(eq) {
            continue;
        }
        if !probe_gate_passes(local, build_size, probe_size, build_min, probe_min, min_sel) {
            continue;
        }
        let Some(oriented) = orient_rf_key(node, sides, expr_order, eq) else {
            continue;
        };
        if (*next_filter_id as usize) >= config.max_count {
            continue;
        }
        let filter_id = *next_filter_id;
        *next_filter_id += 1;
        descs.push(RuntimeFilterBuildIntent {
            filter_id,
            build_expr: oriented.build_expr,
            probe_expr: oriented.probe_expr,
            expr_order: oriented.expr_order,
            execution_mode,
        });
    }

    let policy = ProbePushPolicy {
        allow_cross_exchange: config.allow_cross_exchange,
        cross_exchange: CrossExchangeMode::from(&distribution),
    };
    for desc in &descs {
        let _ = push_probe_down(
            &mut node.children[sides.probe_child],
            desc.filter_id,
            std::slice::from_ref(&desc.probe_expr),
            policy,
        );
    }

    if let PhysicalPlanKind::HashJoin(join) = &mut node.kind {
        join.build_runtime_filters = descs;
    }
}

fn walk_plan_mut(node: &mut PhysicalPlanNode, f: &mut impl FnMut(&mut PhysicalPlanNode)) {
    for child in &mut node.children {
        walk_plan_mut(child, f);
    }
    f(node);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{BinOp, JoinKind, OutputColumn};
    use crate::sql::optimizer::options::{
        SessionOptimizerSettings, with_session_optimizer_settings,
    };
    use crate::sql::planner::JoinExecutionMode;
    use crate::sql::planner::physical_vocab::JoinDistribution;
    use crate::sql::planner::plan::{
        PhysicalHashJoinEqCondition, PhysicalHashJoinNode, PhysicalPlanKind, PlanValuesNode,
    };
    use crate::sql::planner::{PhysicalPlanStats, PlannerColumnStatistic, PlannerConfidence};
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    #[test]
    fn rf_sides_match_join_semantics() {
        assert_eq!(
            rf_sides_for_join(JoinKind::Inner),
            Some(JoinRfSides {
                probe_child: 0,
                build_child: 1
            })
        );
        assert_eq!(
            rf_sides_for_join(JoinKind::RightOuter),
            Some(JoinRfSides {
                probe_child: 0,
                build_child: 1
            })
        );
        assert_eq!(rf_sides_for_join(JoinKind::RightSemi), None);
        assert_eq!(rf_sides_for_join(JoinKind::LeftOuter), None);
        assert_eq!(rf_sides_for_join(JoinKind::FullOuter), None);
    }

    #[test]
    fn cross_exchange_mode_maps_distribution() {
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

    #[test]
    fn build_gate_rejects_oversized_shuffle_build() {
        assert!(!build_gate_passes(&JoinDistribution::Shuffle, 200.0, 100.0));
        assert!(build_gate_passes(&JoinDistribution::Shuffle, 50.0, 100.0));
        assert!(build_gate_passes(
            &JoinDistribution::Broadcast,
            200.0,
            100.0
        ));
    }

    #[test]
    fn probe_gate_matches_local_and_selectivity_thresholds() {
        assert!(probe_gate_passes(true, 1_000.0, 1.0, 10.0, 10.0, 0.5));
        assert!(probe_gate_passes(false, 5.0, 1.0, 10.0, 10.0, 0.5));
        assert!(!probe_gate_passes(false, 20.0, 5.0, 10.0, 10.0, 0.5));
        assert!(probe_gate_passes(false, 20.0, 100.0, 10.0, 10.0, 0.5));
    }

    #[test]
    fn walk_plan_mut_visits_descendants_before_parent() {
        let mut root = values_node(
            1.0,
            vec![
                values_node(2.0, vec![values_node(3.0, vec![])]),
                values_node(4.0, vec![]),
            ],
        );
        let mut visited = Vec::new();

        walk_plan_mut(&mut root, &mut |node| {
            visited.push(node.stats.output_row_count as i32);
        });

        assert_eq!(visited, vec![3, 2, 4, 1]);
    }

    #[test]
    fn config_reads_defaults_when_session_unset() {
        let cfg = with_session_optimizer_settings(SessionOptimizerSettings::default(), || {
            RuntimeFilterPlacementConfig::from_current_session()
        });
        assert!(cfg.enabled);
        assert_eq!(cfg.build_max_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.build_min_bytes, 128 * 1024);
        assert_eq!(cfg.probe_min_bytes, 100 * 1024);
        assert_eq!(cfg.probe_min_selectivity, 0.5);
        assert_eq!(cfg.max_count, 1024);
        assert!(cfg.allow_cross_exchange);
    }

    #[test]
    fn config_disabled_when_rule_in_disabled_set() {
        let settings = SessionOptimizerSettings {
            disabled_rules: vec![RUNTIME_FILTER_RULE.to_string()],
            ..SessionOptimizerSettings::default()
        };
        let cfg = with_session_optimizer_settings(settings, || {
            RuntimeFilterPlacementConfig::from_current_session()
        });
        assert!(!cfg.enabled);
    }

    #[test]
    fn config_reads_session_overrides() {
        let settings = SessionOptimizerSettings {
            rf_build_max_bytes: Some(11),
            rf_build_min_bytes: Some(22),
            rf_probe_min_bytes: Some(33),
            rf_probe_min_selectivity: Some(0.25),
            allow_cross_exchange_rf: Some(false),
            ..SessionOptimizerSettings::default()
        };
        let cfg = with_session_optimizer_settings(settings, || {
            RuntimeFilterPlacementConfig::from_current_session()
        });

        assert!(cfg.enabled);
        assert_eq!(cfg.build_max_bytes, 11);
        assert_eq!(cfg.build_min_bytes, 22);
        assert_eq!(cfg.probe_min_bytes, 33);
        assert_eq!(cfg.probe_min_selectivity, 0.25);
        assert_eq!(cfg.max_count, 1024);
        assert!(!cfg.allow_cross_exchange);
    }

    #[test]
    fn stats_size_uses_row_count_times_avg_width() {
        let stats = stats_with_columns(10.0, &[(1, 4.0)]);

        assert_eq!(stats_compute_size(&stats), 40.0);
        assert_eq!(
            stats_compute_size_for_columns(&stats, &[ColumnId::new_for_test(1)]),
            40.0
        );

        let empty = stats_with_columns(2.0, &[]);

        assert_eq!(stats_compute_size(&empty), 16.0);
        assert_eq!(stats_compute_size_for_columns(&empty, &[]), 16.0);
    }

    #[test]
    fn stats_size_for_columns_matches_optimizer_safe_size_edges() {
        let invalid_stats = stats_with_columns(f64::NAN, &[(1, f64::NAN)]);

        assert_eq!(
            stats_compute_size_for_columns(&invalid_stats, &[ColumnId::new_for_test(1)]),
            8.0
        );
        assert_eq!(stats_compute_size_for_columns(&invalid_stats, &[]), 8.0);

        let infinite_row_stats = stats_with_columns(f64::INFINITY, &[(1, 4.0)]);

        assert_eq!(
            stats_compute_size_for_columns(&infinite_row_stats, &[ColumnId::new_for_test(1)]),
            MAX_FINITE_SIZE
        );
        assert_eq!(
            stats_compute_size_for_columns(&infinite_row_stats, &[]),
            MAX_FINITE_SIZE
        );

        let infinite_width_stats = stats_with_columns(10.0, &[(1, f64::INFINITY)]);

        assert_eq!(
            stats_compute_size_for_columns(&infinite_width_stats, &[ColumnId::new_for_test(1)]),
            MAX_FINITE_SIZE
        );
        assert_eq!(
            stats_compute_size_for_columns(&infinite_width_stats, &[]),
            MAX_FINITE_SIZE
        );

        let overflow_stats = stats_with_columns(1.0e299, &[(1, 1.0e10)]);

        assert_eq!(
            stats_compute_size_for_columns(&overflow_stats, &[ColumnId::new_for_test(1)]),
            MAX_FINITE_SIZE
        );
        assert_eq!(
            stats_compute_size_for_columns(&overflow_stats, &[]),
            MAX_FINITE_SIZE
        );

        let missing_column_stats = stats_with_columns(2.0, &[(1, 4.0)]);

        assert_eq!(
            stats_compute_size_for_columns(&missing_column_stats, &[ColumnId::new_for_test(99)]),
            16.0
        );
    }

    #[test]
    fn could_bound_requires_all_columns_present() {
        let node = leaf(vec![out_col(1, "a"), out_col(2, "b")]);

        assert!(could_bound(&node, &col_ref(1, "a")));
        assert!(!could_bound(&node, &col_ref(9, "z")));
        assert!(could_bound(
            &node,
            &binary_expr(col_ref(1, "a"), col_ref(2, "b"))
        ));
        assert!(!could_bound(
            &node,
            &binary_expr(col_ref(1, "a"), col_ref(9, "z"))
        ));
    }

    #[test]
    fn expr_bound_child_requires_unique_binding_child() {
        let mut parent = values_node(
            1.0,
            vec![leaf(vec![out_col(1, "a")]), leaf(vec![out_col(2, "b")])],
        );

        assert_eq!(expr_bound_child(&parent, &col_ref(1, "a")), Some(0));
        assert_eq!(expr_bound_child(&parent, &col_ref(2, "b")), Some(1));
        assert_eq!(
            expr_bound_child(&parent, &binary_expr(col_ref(1, "a"), col_ref(2, "b"))),
            None
        );

        parent.children = vec![leaf(vec![out_col(1, "a")]), leaf(vec![out_col(1, "a")])];
        assert_eq!(expr_bound_child(&parent, &col_ref(1, "a")), None);
    }

    #[test]
    fn best_member_picks_lexicographically_smallest() {
        let best =
            best_member(&[col_ref(5, "e"), col_ref(2, "b")]).expect("member should be selected");
        assert_eq!(column_id_vec(&best), vec![ColumnId::new_for_test(2)]);

        let best = best_member(&[
            binary_expr(col_ref(3, "c"), col_ref(4, "d")),
            binary_expr(col_ref(1, "a"), col_ref(9, "z")),
        ])
        .expect("member should be selected");
        assert_eq!(
            column_id_vec(&best),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(9)]
        );
    }

    #[test]
    fn rf_key_types_match_compares_data_types() {
        assert!(rf_key_types_match(&PhysicalHashJoinEqCondition {
            left: col_ref(1, "a"),
            right: col_ref(2, "b"),
            null_safe: false,
        }));
        assert!(!rf_key_types_match(&PhysicalHashJoinEqCondition {
            left: col_ref(1, "a"),
            right: col_ref_with_type(2, "b", DataType::Utf8),
            null_safe: false,
        }));
    }

    #[test]
    fn bindable_members_filters_to_node_output() {
        let node = leaf(vec![out_col(1, "a")]);
        let members = vec![col_ref(1, "a"), col_ref(2, "b")];
        let bindable = bindable_members(&node, &members);

        assert_eq!(bindable.len(), 1);
        assert_eq!(column_id_vec(&bindable[0]), vec![ColumnId::new_for_test(1)]);
    }

    #[test]
    fn inner_broadcast_join_emits_one_build_intent() {
        let probe = leaf(vec![out_col(1, "probe_key")]);
        let build = leaf(vec![out_col(2, "build_key")]);
        let mut join = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            vec![eq_cond(
                col_ref(1, "probe_key"),
                col_ref(2, "build_key"),
                false,
            )],
            vec![probe, build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0)]),
        );
        let cfg = permissive_config(1024);
        let mut nid = 0;

        place_node(&mut join, &cfg, &mut nid);

        let join_kind = expect_hash_join(&join);
        assert_eq!(join_kind.build_runtime_filters.len(), 1);
        let rf = &join_kind.build_runtime_filters[0];
        assert_eq!(rf.filter_id, 0);
        assert_eq!(rf.expr_order, 0);
        assert_eq!(rf.execution_mode, JoinExecutionMode::Broadcast);
        assert_column_ref(&rf.build_expr, 2);
        assert_column_ref(&rf.probe_expr, 1);
        assert_eq!(nid, 1);
        assert!(join.children[0].probe_runtime_filters.is_empty());
    }

    #[test]
    fn place_node_assigns_nested_filter_ids_post_order() {
        let nested_probe = leaf(vec![out_col(1, "nested_probe")]);
        let nested_build = leaf(vec![out_col(2, "nested_build")]);
        let nested = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            vec![eq_cond(
                col_ref(1, "nested_probe"),
                col_ref(2, "nested_build"),
                false,
            )],
            vec![nested_probe, nested_build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0)]),
        );
        let outer_build = leaf(vec![out_col(4, "outer_build")]);
        let mut outer = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            vec![eq_cond(
                col_ref(1, "nested_probe"),
                col_ref(4, "outer_build"),
                false,
            )],
            vec![nested, outer_build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0), (4, 8.0)]),
        );
        let cfg = permissive_config(1024);
        let mut nid = 0;

        place_node(&mut outer, &cfg, &mut nid);

        let nested_join = expect_hash_join(&outer.children[0]);
        let outer_join = expect_hash_join(&outer);
        assert_eq!(nested_join.build_runtime_filters[0].filter_id, 0);
        assert_eq!(outer_join.build_runtime_filters[0].filter_id, 1);
        assert_eq!(nid, 2);
    }

    #[test]
    fn build_intent_skips_null_safe_type_mismatch_and_max_count() {
        let probe = leaf(vec![out_col(1, "probe_key")]);
        let build = leaf(vec![
            out_col(2, "null_safe_build"),
            out_col(3, "mismatch_build"),
            out_col(4, "valid_build"),
            out_col(5, "second_valid_build"),
        ]);
        let mut join = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            vec![
                eq_cond(col_ref(1, "probe_key"), col_ref(2, "null_safe_build"), true),
                eq_cond(
                    col_ref(1, "probe_key"),
                    col_ref_with_type(3, "mismatch_build", DataType::Utf8),
                    false,
                ),
                eq_cond(col_ref(1, "probe_key"), col_ref(4, "valid_build"), false),
                eq_cond(
                    col_ref(1, "probe_key"),
                    col_ref(5, "second_valid_build"),
                    false,
                ),
            ],
            vec![probe, build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0), (3, 8.0), (4, 8.0), (5, 8.0)]),
        );
        let cfg = permissive_config(1);
        let mut nid = 0;

        place_node(&mut join, &cfg, &mut nid);

        let join_kind = expect_hash_join(&join);
        assert_eq!(join_kind.build_runtime_filters.len(), 1);
        let rf = &join_kind.build_runtime_filters[0];
        assert_eq!(rf.filter_id, 0);
        assert_eq!(rf.expr_order, 2);
        assert_column_ref(&rf.build_expr, 4);
        assert_column_ref(&rf.probe_expr, 1);
        assert_eq!(nid, 1);
    }

    #[test]
    fn shuffle_build_gate_uses_key_width_not_full_row_width() {
        let mut probe = leaf(vec![out_col(1, "probe_key")]);
        probe.stats = stats_with_columns(100.0, &[(1, 8.0)]);
        let mut build = leaf(vec![out_col(2, "build_key"), out_col(3, "payload")]);
        build.stats = stats_with_columns(100.0, &[(2, 1.0), (3, 10_000.0)]);
        let mut join = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Shuffle,
            Some(JoinExecutionMode::Partitioned),
            vec![eq_cond(
                col_ref(1, "probe_key"),
                col_ref(2, "build_key"),
                false,
            )],
            vec![probe, build],
            stats_with_columns(100.0, &[(1, 8.0), (2, 1.0), (3, 10_000.0)]),
        );
        let cfg = RuntimeFilterPlacementConfig {
            enabled: true,
            build_max_bytes: 200,
            build_min_bytes: 2_000_000,
            probe_min_bytes: 0,
            probe_min_selectivity: 0.5,
            max_count: 1024,
            allow_cross_exchange: true,
        };
        let mut nid = 0;

        place_node(&mut join, &cfg, &mut nid);

        let join_kind = expect_hash_join(&join);
        assert_eq!(join_kind.build_runtime_filters.len(), 1);
        let rf = &join_kind.build_runtime_filters[0];
        assert_eq!(rf.filter_id, 0);
        assert_eq!(rf.execution_mode, JoinExecutionMode::Partitioned);
        assert_column_ref(&rf.build_expr, 2);
        assert_column_ref(&rf.probe_expr, 1);
        assert_eq!(nid, 1);
    }

    #[test]
    fn unknown_distribution_emits_no_build_intents() {
        let probe = leaf(vec![out_col(1, "probe_key")]);
        let build = leaf(vec![out_col(2, "build_key")]);
        let mut join = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Unknown,
            None,
            vec![eq_cond(
                col_ref(1, "probe_key"),
                col_ref(2, "build_key"),
                false,
            )],
            vec![probe, build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0)]),
        );
        let cfg = permissive_config(1024);
        let mut nid = 0;

        place_node(&mut join, &cfg, &mut nid);

        assert!(expect_hash_join(&join).build_runtime_filters.is_empty());
        assert_eq!(nid, 0);
    }

    #[test]
    fn execution_mode_overrides_unknown_distribution_for_build_intents() {
        let probe = leaf(vec![out_col(1, "probe_key")]);
        let build = leaf(vec![out_col(2, "build_key")]);
        let mut join = hash_join_node(
            JoinKind::Inner,
            JoinDistribution::Unknown,
            Some(JoinExecutionMode::Broadcast),
            vec![eq_cond(
                col_ref(1, "probe_key"),
                col_ref(2, "build_key"),
                false,
            )],
            vec![probe, build],
            stats_with_columns(10.0, &[(1, 8.0), (2, 8.0)]),
        );
        let cfg = permissive_config(1024);
        let mut nid = 0;

        place_node(&mut join, &cfg, &mut nid);

        let join_kind = expect_hash_join(&join);
        assert_eq!(join_kind.build_runtime_filters.len(), 1);
        assert_eq!(
            join_kind.build_runtime_filters[0].execution_mode,
            JoinExecutionMode::Broadcast
        );
        assert_eq!(nid, 1);
    }

    #[test]
    fn column_id_vec_recurses_and_skips_unset() {
        let expr = typed_expr(ExprKind::FunctionCall {
            name: "outer_fn".to_string(),
            args: vec![
                unset_col_ref("unset"),
                typed_expr(ExprKind::AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(7, "agg_arg")],
                    distinct: false,
                    order_by: vec![sort_item(col_ref(3, "agg_order"))],
                }),
                typed_expr(ExprKind::Case {
                    operand: Some(Box::new(col_ref(6, "case_operand"))),
                    when_then: vec![(
                        col_ref(5, "case_when"),
                        typed_expr(ExprKind::FunctionCall {
                            name: "then_fn".to_string(),
                            args: vec![col_ref(4, "case_then")],
                            distinct: false,
                        }),
                    )],
                    else_expr: Some(Box::new(col_ref(2, "case_else"))),
                }),
                typed_expr(ExprKind::WindowCall {
                    name: "row_number".to_string(),
                    args: vec![col_ref(9, "window_arg")],
                    distinct: false,
                    partition_by: vec![col_ref(8, "window_partition")],
                    order_by: vec![sort_item(col_ref(1, "window_order"))],
                    window_frame: None,
                    ignore_nulls: false,
                }),
                typed_expr(ExprKind::Lambda {
                    params: vec!["x".to_string()],
                    body: Box::new(col_ref(10, "lambda_body")),
                }),
            ],
            distinct: false,
        });

        let mut raw = Vec::new();
        collect_typed_expr_column_ids(&expr, &mut raw);
        assert!(!raw.contains(&ColumnId::UNSET));
        for id in 1..=10 {
            assert!(raw.contains(&ColumnId::new_for_test(id)));
        }
        assert_eq!(
            column_id_vec(&expr),
            (1..=10).map(ColumnId::new_for_test).collect::<Vec<_>>()
        );
    }

    fn planner_column_statistic(average_row_size: f64) -> PlannerColumnStatistic {
        PlannerColumnStatistic {
            min_value: 0.0,
            max_value: 0.0,
            nulls_fraction: 0.0,
            average_row_size,
            ndv: None,
            confidence: PlannerConfidence::Estimated,
        }
    }

    fn stats_with_columns(output_row_count: f64, columns: &[(u32, f64)]) -> PhysicalPlanStats {
        let mut column_statistics = HashMap::new();
        for (id, average_row_size) in columns {
            column_statistics.insert(
                ColumnId::new_for_test(*id),
                planner_column_statistic(*average_row_size),
            );
        }
        PhysicalPlanStats {
            output_row_count,
            row_count_confidence: PlannerConfidence::Estimated,
            column_statistics,
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn out_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        col_ref_with_type(id, name, DataType::Int64)
    }

    fn col_ref_with_type(id: u32, name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable: false,
        }
    }

    fn unset_col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn typed_expr(kind: ExprKind) -> TypedExpr {
        TypedExpr {
            kind,
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn sort_item(expr: TypedExpr) -> SortItem {
        SortItem {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    fn binary_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Add,
                right: Box::new(right),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq_cond(left: TypedExpr, right: TypedExpr, null_safe: bool) -> PhysicalHashJoinEqCondition {
        PhysicalHashJoinEqCondition {
            left,
            right,
            null_safe,
        }
    }

    fn hash_join_node(
        join_type: JoinKind,
        distribution: JoinDistribution,
        execution_mode: Option<JoinExecutionMode>,
        eq_conditions: Vec<PhysicalHashJoinEqCondition>,
        children: Vec<PhysicalPlanNode>,
        stats: PhysicalPlanStats,
    ) -> PhysicalPlanNode {
        let output_columns = children
            .iter()
            .flat_map(|child| child.output_columns.iter().cloned())
            .collect::<Vec<_>>();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type,
                eq_conditions,
                other_condition: None,
                distribution,
                execution_mode,
                build_runtime_filters: vec![],
                output_columns: output_columns.clone(),
            })),
            children,
            output_columns,
            stats,
            probe_runtime_filters: vec![],
        }
    }

    fn expect_hash_join(node: &PhysicalPlanNode) -> &PhysicalHashJoinNode {
        let PhysicalPlanKind::HashJoin(join) = &node.kind else {
            panic!("expected hash join");
        };
        join
    }

    fn assert_column_ref(expr: &TypedExpr, id: u32) {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(id));
    }

    fn permissive_config(max_count: usize) -> RuntimeFilterPlacementConfig {
        RuntimeFilterPlacementConfig {
            enabled: true,
            build_max_bytes: u64::MAX,
            build_min_bytes: 0,
            probe_min_bytes: 0,
            probe_min_selectivity: 0.5,
            max_count,
            allow_cross_exchange: true,
        }
    }

    fn leaf(cols: Vec<OutputColumn>) -> PhysicalPlanNode {
        let mut node = values_node(1.0, vec![]);
        node.kind = PhysicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: cols.clone(),
        });
        node.output_columns = cols;
        node
    }

    fn values_node(marker: f64, children: Vec<PhysicalPlanNode>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            children,
            output_columns: vec![],
            stats: PhysicalPlanStats {
                output_row_count: marker,
                row_count_confidence: PlannerConfidence::Exact,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            probe_runtime_filters: vec![],
        }
    }
}
