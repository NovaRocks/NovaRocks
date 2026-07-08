#![allow(dead_code)] // removed in Task 7 once wired into the bridge.

//! Planner-side runtime-filter placement pass (RFP-1).
//!
//! Runs on the single `PhysicalPlanNode` tree produced by the optimizer->planner
//! bridge, BEFORE `build_distributed_plan` fragments it. Annotates hash joins
//! with build-side `RuntimeFilterBuildIntent`s and pushes matching
//! `RuntimeFilterProbeIntent`s down to the deepest bindable probe descendant.
//! Behavior is a byte-for-byte port of the retired
//! `optimizer::runtime_filter_pass` -- do not "improve" placement here; changes
//! belong in the RF baseline / producer arcs.

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::JoinKind;
use crate::sql::optimizer::options::current_session_optimizer_settings;
use crate::sql::planner::physical_vocab::JoinDistribution;
use crate::sql::planner::plan::{PhysicalHashJoinEqCondition, PhysicalPlanNode};
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

/// Entry point. No-op until Task 7 fills the traversal.
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
    _node: &mut PhysicalPlanNode,
    _config: &RuntimeFilterPlacementConfig,
    _next_filter_id: &mut i32,
) {
    // Filled incrementally in Tasks 5-6.
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
    use crate::sql::planner::physical_vocab::JoinDistribution;
    use crate::sql::planner::plan::{
        PhysicalHashJoinEqCondition, PhysicalPlanKind, PlanValuesNode,
    };
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};
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
