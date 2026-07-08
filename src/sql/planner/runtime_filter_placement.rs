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

use crate::sql::optimizer::options::current_session_optimizer_settings;
use crate::sql::planner::plan::PhysicalPlanNode;

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
    use crate::sql::optimizer::options::{
        SessionOptimizerSettings, with_session_optimizer_settings,
    };
    use crate::sql::planner::plan::{PhysicalPlanKind, PlanValuesNode};
    use crate::sql::planner::{PhysicalPlanStats, PlannerConfidence};
    use std::collections::HashMap;

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
