// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Per-optimize-call configuration shared by logical rewrite and CBO drivers.

use std::collections::HashSet;
use std::time::Duration;

use crate::optimizer::cascades_rules::multi_join_reorder::ReorderOptions;
use crate::optimizer::cost::CostOptions;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionOptimizerSettings {
    pub enable_ukfk_opt: bool,
    pub enable_query_rewrite_table_prune: bool,
    pub enable_cbo_table_prune: bool,
    pub enable_table_prune_on_update: bool,
    pub enable_eliminate_agg: bool,
    pub disabled_rules: Vec<String>,
    /// Session override for common-subexpression reuse (None = default true).
    pub enable_common_subexpr_reuse: Option<bool>,
    /// Session override for the RF build-side maximum size gate (bytes).
    /// `None` means use the StarRocks default (64 MiB).
    pub rf_build_max_bytes: Option<u64>,
    /// Session override for the RF build-side minimum size gate (bytes).
    /// `None` means use the StarRocks default (128 KiB).
    pub rf_build_min_bytes: Option<u64>,
    /// Session override for the RF probe-side minimum size gate (bytes).
    /// `None` means use the StarRocks default (100 KiB).
    pub rf_probe_min_bytes: Option<u64>,
    /// Session override for the RF probe-side minimum selectivity gate.
    /// `None` means use the StarRocks default (0.5).
    pub rf_probe_min_selectivity: Option<f64>,
    /// Session override for `enable_global_runtime_filter`.
    /// `None` means the default (enabled).
    pub enable_global_runtime_filter: Option<bool>,
    /// Session override for transparent MV query rewrite.
    /// `None` means the default (enabled).
    pub enable_materialized_view_rewrite: Option<bool>,
    /// Session override for connector static predicate pushdown.
    /// `None` means the default (enabled). This controls only whether Core
    /// offers static predicates to a connector; it never changes residual
    /// predicate evaluation.
    pub enable_connector_static_predicate_pushdown: Option<bool>,
    /// Session override for `cbo_enable_dp_join_reorder` (None = default true).
    pub enable_dp_join_reorder: Option<bool>,
    /// Session override for `cbo_enable_greedy_join_reorder` (None = default true).
    pub enable_greedy_join_reorder: Option<bool>,
    /// Session override for `cbo_max_reorder_node_use_exhaustive` (None = default 4).
    pub max_reorder_node_use_exhaustive: Option<usize>,
    /// Session override for `cbo_max_reorder_node_use_dp` (None = default 10).
    pub max_reorder_node_use_dp: Option<usize>,
    /// Session override for `cbo_max_reorder_node_use_greedy` (None = default 16).
    pub max_reorder_node_use_greedy: Option<usize>,
    /// Session override for `cbo_max_reorder_node` (None = default 50).
    pub max_reorder_node: Option<usize>,
    /// Session override for the broadcast fanout backend count (`cbo_broadcast_backend_count`).
    /// `None` means use the statement-admission estimate, then the profile default.
    pub cbo_broadcast_backend_count: Option<f64>,
    /// Session override for the per-node broadcast build memory budget in bytes
    /// (`cbo_broadcast_node_mem_budget_bytes`). `None` means the profile default (1 GiB).
    pub cbo_broadcast_node_mem_budget_bytes: Option<f64>,
    /// Statement-admission snapshot of the first non-empty BE topology count.
    /// It stays fixed across later execution attempts. `None` means admission
    /// had no topology estimate, so costing falls back to the profile default.
    pub effective_backend_count: Option<f64>,
    /// Query memory budget frozen at admission. SQL applies this only to
    /// costing; it never consults the process-global runtime configuration.
    pub optimizer_query_mem_limit_bytes: Option<f64>,
    /// Session override for `enable_global_runtime_filter_cross_exchange`
    /// (None = built-in default, which is true). Setting false disables
    /// placing probe runtime filters across shuffle exchanges, for bisecting
    /// cross-fragment RF behavior.
    pub allow_cross_exchange_rf: Option<bool>,
}

impl SessionOptimizerSettings {
    pub(crate) fn stable_digest_material(&self) -> Vec<u8> {
        let mut disabled_rules = self.disabled_rules.clone();
        disabled_rules.sort();
        disabled_rules.dedup();
        format!(
            "ukfk={};rewrite_prune={};cbo_prune={};update_prune={};elim_agg={};rules={:?};cse={:?};rf_max={:?};rf_min={:?};probe_min={:?};probe_sel={:?};global_rf={:?};mv={:?};static_pd={:?};dp={:?};greedy={:?};exhaustive={:?};dp_n={:?};greedy_n={:?};max_n={:?};broadcast_n={:?};broadcast_mem={:?};effective_n={:?};query_mem={:?};cross_rf={:?}",
            self.enable_ukfk_opt,
            self.enable_query_rewrite_table_prune,
            self.enable_cbo_table_prune,
            self.enable_table_prune_on_update,
            self.enable_eliminate_agg,
            disabled_rules,
            self.enable_common_subexpr_reuse,
            self.rf_build_max_bytes,
            self.rf_build_min_bytes,
            self.rf_probe_min_bytes,
            self.rf_probe_min_selectivity.map(f64::to_bits),
            self.enable_global_runtime_filter,
            self.enable_materialized_view_rewrite,
            self.enable_connector_static_predicate_pushdown,
            self.enable_dp_join_reorder,
            self.enable_greedy_join_reorder,
            self.max_reorder_node_use_exhaustive,
            self.max_reorder_node_use_dp,
            self.max_reorder_node_use_greedy,
            self.max_reorder_node,
            self.cbo_broadcast_backend_count.map(f64::to_bits),
            self.cbo_broadcast_node_mem_budget_bytes.map(f64::to_bits),
            self.effective_backend_count.map(f64::to_bits),
            self.optimizer_query_mem_limit_bytes.map(f64::to_bits),
            self.allow_cross_exchange_rf,
        )
        .into_bytes()
    }

    pub fn set_disabled_rules(&mut self, rules: Vec<String>) {
        self.disabled_rules = rules;
    }

    pub fn set_enable_eliminate_agg(&mut self, enabled: bool) {
        self.enable_eliminate_agg = enabled;
    }

    pub fn set_enable_ukfk_opt(&mut self, enabled: bool) {
        self.enable_ukfk_opt = enabled;
    }

    pub fn set_broadcast_backend_count(&mut self, count: f64) {
        self.cbo_broadcast_backend_count = Some(count);
    }

    pub(crate) fn global_runtime_filter_enabled(&self) -> bool {
        self.enable_global_runtime_filter.unwrap_or(true)
    }

    pub(crate) fn mv_rewrite_enabled(&self) -> bool {
        self.enable_materialized_view_rewrite.unwrap_or(true)
    }

    /// Return the frozen connector static-predicate admission policy.
    ///
    /// This controls only whether application scan preparation offers static
    /// predicates to a connector; SQL and Core always retain residual
    /// predicate evaluation.
    pub fn connector_static_predicate_pushdown_enabled(&self) -> bool {
        self.enable_connector_static_predicate_pushdown
            .unwrap_or(true)
    }
}

/// Controls which rules fire and bounds resource use.
///
/// Constructed once per `optimize()` call. Held by both the logical rewrite
/// pipeline and the CBO search loop. Rule names live in a single namespace
/// shared across logical rewrite and CBO rules; names must be unique across
/// both rule families.
pub(crate) struct OptimizerOptions {
    disabled_rules: HashSet<String>,
    /// Hard cap on each logical rewrite stage's tree-level fixed-point loop.
    pub rewrite_max_iterations: usize,
    /// Hard cap on the CBO Memo group count. Exploration stops early (and logs a
    /// truncation warning) once the memo exceeds this, bounding join-enumeration
    /// blowup. Defaults to 5000.
    pub cbo_max_groups: usize,
    /// Wall-clock budget for the entire `optimize()` call (existing constant; documented here).
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub optimize_timeout: Duration,
    /// Runtime-filter build-side maximum size gate (bytes).
    /// Shuffle joins with a build side larger than this are skipped.
    /// Default: 64 MiB (StarRocks SessionVariable.runtimeFilterMaxSize).
    pub rf_build_max_bytes: u64,
    /// Runtime-filter build-side minimum size gate (bytes).
    /// If the build side is at or below this threshold the selectivity check is
    /// skipped (always emit the RF). Default: 128 KiB.
    pub rf_build_min_bytes: u64,
    /// Runtime-filter probe-side minimum size gate (bytes).
    /// Non-local RFs whose probe side is below this threshold are rejected.
    /// Default: 100 KiB.
    pub rf_probe_min_bytes: u64,
    /// Runtime-filter minimum required selectivity for non-local RFs.
    /// The RF is emitted only when `build/probe <= 1 - min_selectivity`.
    /// Default: 0.5 (StarRocks RuntimeFilterDescription.MIN_RUNTIME_FILTER_SELECTIVITY).
    pub rf_probe_min_selectivity: f64,
    /// Hard cap on runtime-filter descriptors emitted by one optimize call.
    /// Prevents complex plans from producing unbounded RF lists.
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub rf_max_count: usize,
    /// Whether probe runtime filters may be placed across shuffle exchanges at
    /// all. When true, crossing is further gated per-join-distribution by
    /// `CrossExchangeMode` (see planner `runtime_filter_placement.rs`): Broadcast RFs cross
    /// unconditionally; Shuffle/Colocate RFs cross only exchanges that
    /// re-partition on the probe key (shuffle-key alignment). Probe pushdown
    /// always stops at outer/anti/null-preserving semantic boundaries
    /// regardless of this flag.
    pub allow_cross_exchange_rf: bool,
    /// In-memo join-reorder knobs (algorithm toggles + size cutoffs). Defaults
    /// match StarRocks; overridable via the `cbo_enable_dp/greedy_join_reorder`
    /// and `cbo_max_reorder_node*` session variables.
    pub reorder: ReorderOptions,
    /// Physical operator cost-model options, including session resource-profile overrides.
    pub cost_options: CostOptions,
}

impl OptimizerOptions {
    pub(crate) fn default_settings() -> Self {
        Self {
            disabled_rules: HashSet::new(),
            rewrite_max_iterations: 32,
            cbo_max_groups: 5000,
            optimize_timeout: Duration::from_secs(10),
            rf_build_max_bytes: 64 * 1024 * 1024,
            rf_build_min_bytes: 128 * 1024,
            rf_probe_min_bytes: 100 * 1024,
            rf_probe_min_selectivity: 0.5,
            rf_max_count: 1024,
            allow_cross_exchange_rf: true,
            reorder: ReorderOptions::default(),
            cost_options: CostOptions::default(),
        }
    }

    pub(crate) fn is_enabled(&self, rule_name: &str) -> bool {
        !self.disabled_rules.contains(rule_name)
    }

    pub(crate) fn disable(&mut self, rule_name: &str) {
        self.disabled_rules.insert(rule_name.to_string());
    }

    pub(crate) fn from_session(settings: &SessionOptimizerSettings) -> Self {
        let mut opts = Self::default_settings();
        for rule_name in &settings.disabled_rules {
            opts.disable(rule_name);
        }
        if settings.enable_common_subexpr_reuse == Some(false) {
            opts.disable(crate::optimizer::cse_pass::CSE_RULE);
        }
        if let Some(v) = settings.rf_build_max_bytes {
            opts.rf_build_max_bytes = v;
        }
        if let Some(v) = settings.rf_build_min_bytes {
            opts.rf_build_min_bytes = v;
        }
        if let Some(v) = settings.rf_probe_min_bytes {
            opts.rf_probe_min_bytes = v;
        }
        if let Some(v) = settings.rf_probe_min_selectivity {
            opts.rf_probe_min_selectivity = v;
        }
        if let Some(v) = settings.allow_cross_exchange_rf {
            opts.allow_cross_exchange_rf = v;
        }
        if let Some(v) = settings.enable_dp_join_reorder {
            opts.reorder.enable_dp = v;
        }
        if let Some(v) = settings.enable_greedy_join_reorder {
            opts.reorder.enable_greedy = v;
        }
        if let Some(v) = settings.max_reorder_node_use_exhaustive {
            opts.reorder.max_reorder_node_use_exhaustive = v;
        }
        if let Some(v) = settings.max_reorder_node_use_dp {
            opts.reorder.max_reorder_node_use_dp = v;
        }
        if let Some(v) = settings.max_reorder_node_use_greedy {
            opts.reorder.max_reorder_node_use_greedy = v;
        }
        if let Some(v) = settings.max_reorder_node {
            opts.reorder.max_reorder_node = v;
        }
        // BC-1: cluster/resource profile overrides. Precedence: explicit
        // session SET > admission-frozen environment > profile default.
        let mut profile = opts.cost_options.profile.clone();
        if let Some(query_mem_limit_bytes) = settings.optimizer_query_mem_limit_bytes {
            profile.apply_query_mem_limit_bytes(query_mem_limit_bytes);
        }
        if let Some(v) = settings
            .cbo_broadcast_backend_count
            .or(settings.effective_backend_count)
        {
            profile.effective_backend_count = v;
        }
        if let Some(v) = settings.cbo_broadcast_node_mem_budget_bytes {
            profile.per_node_build_memory_budget_bytes = v;
        }
        opts.cost_options.apply_profile(profile);
        opts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_enables_all_rules() {
        let opts = OptimizerOptions::default_settings();
        assert!(opts.is_enabled("AnyRuleName"));
        assert!(opts.is_enabled("PushDownPredicateScan"));
    }

    #[test]
    fn disable_blocks_named_rule_only() {
        let mut opts = OptimizerOptions::default_settings();
        opts.disable("PushDownPredicateScan");
        assert!(!opts.is_enabled("PushDownPredicateScan"));
        assert!(opts.is_enabled("PushDownPredicateProject"));
    }

    #[test]
    fn defaults_match_existing_optimizer_constants() {
        let opts = OptimizerOptions::default_settings();
        assert_eq!(opts.rewrite_max_iterations, 32);
        assert_eq!(opts.cbo_max_groups, 5000);
        assert_eq!(opts.optimize_timeout, Duration::from_secs(10));
    }

    #[test]
    fn from_session_copies_disabled_rules() {
        let settings = SessionOptimizerSettings {
            disabled_rules: vec!["JoinCommutativity".to_string(), "FooRule".to_string()],
            ..Default::default()
        };
        let opts = OptimizerOptions::from_session(&settings);
        assert!(!opts.is_enabled("JoinCommutativity"));
        assert!(!opts.is_enabled("FooRule"));
        assert!(opts.is_enabled("UnrelatedRule"));
    }

    #[test]
    fn from_session_empty_disabled_rules_enables_everything() {
        let settings = SessionOptimizerSettings::default();
        let opts = OptimizerOptions::from_session(&settings);
        assert!(opts.is_enabled("JoinCommutativity"));
        assert!(opts.is_enabled("AnyRuleAtAll"));
    }

    #[test]
    fn disabling_cse_via_session_disables_rule() {
        let settings = SessionOptimizerSettings {
            enable_common_subexpr_reuse: Some(false),
            ..Default::default()
        };
        let opts = OptimizerOptions::from_session(&settings);
        assert!(!opts.is_enabled(crate::optimizer::cse_pass::CSE_RULE));
    }

    #[test]
    fn cse_session_default_and_true_enable_rule() {
        let default_opts = OptimizerOptions::from_session(&SessionOptimizerSettings::default());
        assert!(default_opts.is_enabled(crate::optimizer::cse_pass::CSE_RULE));

        let settings = SessionOptimizerSettings {
            enable_common_subexpr_reuse: Some(true),
            ..Default::default()
        };
        let opts = OptimizerOptions::from_session(&settings);
        assert!(opts.is_enabled(crate::optimizer::cse_pass::CSE_RULE));
    }

    #[test]
    fn runtime_filter_thresholds_default_to_starrocks() {
        let o = OptimizerOptions::default_settings();
        assert_eq!(o.rf_build_max_bytes, 64 * 1024 * 1024);
        assert_eq!(o.rf_build_min_bytes, 128 * 1024);
        assert_eq!(o.rf_probe_min_bytes, 100 * 1024);
        assert!((o.rf_probe_min_selectivity - 0.5).abs() < 1e-9);
        assert_eq!(o.rf_max_count, 1024);
    }

    #[test]
    fn runtime_filter_max_count_default_is_stable() {
        assert_eq!(OptimizerOptions::default_settings().rf_max_count, 1024);
    }

    #[test]
    fn default_settings_carry_cost_options() {
        let opts = OptimizerOptions::default_settings();
        let default_cost_options = crate::optimizer::cost::CostOptions::default();

        assert_eq!(
            opts.cost_options.cpu_weight,
            default_cost_options.cpu_weight
        );
        assert_eq!(
            opts.cost_options.memory_weight,
            default_cost_options.memory_weight
        );
        assert_eq!(
            opts.cost_options.network_weight,
            default_cost_options.network_weight
        );
    }

    #[test]
    fn from_session_overrides_rf_thresholds() {
        let s = SessionOptimizerSettings {
            rf_build_max_bytes: Some(1),
            rf_probe_min_selectivity: Some(0.9),
            ..Default::default()
        };
        let o = OptimizerOptions::from_session(&s);
        assert_eq!(o.rf_build_max_bytes, 1);
        assert!((o.rf_probe_min_selectivity - 0.9).abs() < 1e-9);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn from_session_overrides_profile_and_syncs_backend_factor() {
        let mut settings = SessionOptimizerSettings::default();
        settings.cbo_broadcast_backend_count = Some(16.0);
        settings.cbo_broadcast_node_mem_budget_bytes = Some(256.0 * 1024.0 * 1024.0);
        let opts = OptimizerOptions::from_session(&settings);
        assert_eq!(opts.cost_options.profile.effective_backend_count, 16.0);
        assert_eq!(opts.cost_options.backend_factor, 16.0);
        assert_eq!(
            opts.cost_options.profile.per_node_build_memory_budget_bytes,
            256.0 * 1024.0 * 1024.0
        );
        // LAYER 2 network budget is cluster-wide, not per-backend.
        assert_eq!(
            opts.cost_options
                .profile
                .cluster_broadcast_network_budget_bytes,
            256.0 * 1024.0 * 1024.0
        );
    }

    #[test]
    fn from_session_default_keeps_ci_baseline_profile() {
        let opts = OptimizerOptions::from_session(&SessionOptimizerSettings::default());
        assert_eq!(opts.cost_options.profile.effective_backend_count, 3.0);
        assert_eq!(opts.cost_options.backend_factor, 3.0);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn from_session_uses_admission_snapshot_when_set_unset() {
        let mut settings = SessionOptimizerSettings::default();
        settings.cbo_broadcast_backend_count = None;
        settings.effective_backend_count = Some(11.0);
        let default_per_node = CostOptions::default()
            .profile
            .per_node_build_memory_budget_bytes;
        let opts = OptimizerOptions::from_session(&settings);
        assert_eq!(opts.cost_options.profile.effective_backend_count, 11.0);
        assert_eq!(opts.cost_options.backend_factor, 11.0);
        assert_eq!(
            opts.cost_options
                .profile
                .cluster_broadcast_network_budget_bytes,
            default_per_node
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn from_session_explicit_set_overrides_admission_snapshot() {
        // SET takes precedence over the admission-frozen estimate.
        let mut settings = SessionOptimizerSettings::default();
        settings.cbo_broadcast_backend_count = Some(7.0);
        settings.effective_backend_count = Some(3.0);
        let opts = OptimizerOptions::from_session(&settings);
        assert_eq!(opts.cost_options.profile.effective_backend_count, 7.0);
    }

    #[test]
    fn sqlx2_optimizer_uses_admission_frozen_query_mem_limit() {
        let opts = OptimizerOptions::from_session(&SessionOptimizerSettings {
            optimizer_query_mem_limit_bytes: Some(512.0 * 1024.0 * 1024.0),
            ..Default::default()
        });

        assert_eq!(
            opts.cost_options.profile.query_mem_limit_bytes,
            512.0 * 1024.0 * 1024.0
        );
        assert_eq!(
            opts.cost_options.profile.per_node_build_memory_budget_bytes,
            256.0 * 1024.0 * 1024.0
        );
        assert_eq!(
            opts.cost_options
                .profile
                .cluster_broadcast_network_budget_bytes,
            256.0 * 1024.0 * 1024.0
        );
    }

    #[test]
    fn reorder_knobs_default_to_starrocks() {
        let o = OptimizerOptions::default_settings();
        assert!(o.reorder.enable_dp);
        assert!(o.reorder.enable_greedy);
        assert_eq!(o.reorder.max_reorder_node_use_exhaustive, 4);
        assert_eq!(o.reorder.max_reorder_node_use_dp, 10);
        assert_eq!(o.reorder.max_reorder_node_use_greedy, 16);
        assert_eq!(o.reorder.max_reorder_node, 50);
    }

    #[test]
    fn from_session_overrides_reorder_knobs() {
        let s = SessionOptimizerSettings {
            enable_dp_join_reorder: Some(false),
            enable_greedy_join_reorder: Some(false),
            max_reorder_node_use_exhaustive: Some(2),
            max_reorder_node_use_dp: Some(7),
            max_reorder_node_use_greedy: Some(9),
            max_reorder_node: Some(40),
            ..Default::default()
        };
        let o = OptimizerOptions::from_session(&s);
        assert!(!o.reorder.enable_dp);
        assert!(!o.reorder.enable_greedy);
        assert_eq!(o.reorder.max_reorder_node_use_exhaustive, 2);
        assert_eq!(o.reorder.max_reorder_node_use_dp, 7);
        assert_eq!(o.reorder.max_reorder_node_use_greedy, 9);
        assert_eq!(o.reorder.max_reorder_node, 40);
    }

    #[test]
    fn from_session_reorder_knobs_default_when_unset() {
        // No session override → ReorderOptions stays at StarRocks defaults.
        let o = OptimizerOptions::from_session(&SessionOptimizerSettings::default());
        assert!(o.reorder.enable_dp);
        assert_eq!(o.reorder.max_reorder_node_use_dp, 10);
    }

    #[test]
    fn session_can_disable_cross_exchange_rf() {
        let mut settings = SessionOptimizerSettings::default();
        let opts = OptimizerOptions::from_session(&settings);
        assert!(opts.allow_cross_exchange_rf);

        settings.allow_cross_exchange_rf = Some(false);
        let opts = OptimizerOptions::from_session(&settings);
        assert!(!opts.allow_cross_exchange_rf);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn global_runtime_filter_defaults_enabled_and_honors_explicit_override() {
        let settings = SessionOptimizerSettings::default();
        assert!(settings.global_runtime_filter_enabled());

        let mut off = SessionOptimizerSettings::default();
        off.enable_global_runtime_filter = Some(false);
        assert!(!off.global_runtime_filter_enabled());

        let mut on = SessionOptimizerSettings::default();
        on.enable_global_runtime_filter = Some(true);
        assert!(on.global_runtime_filter_enabled());
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn mv_rewrite_enabled_defaults_to_true() {
        let settings = SessionOptimizerSettings::default();
        assert!(settings.mv_rewrite_enabled());
        let mut off = SessionOptimizerSettings::default();
        off.enable_materialized_view_rewrite = Some(false);
        assert!(!off.mv_rewrite_enabled());
        let mut on = SessionOptimizerSettings::default();
        on.enable_materialized_view_rewrite = Some(true);
        assert!(on.mv_rewrite_enabled());
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn connector_static_predicate_pushdown_defaults_enabled_and_honors_override() {
        let settings = SessionOptimizerSettings::default();
        assert!(settings.connector_static_predicate_pushdown_enabled());

        let mut off = SessionOptimizerSettings::default();
        off.enable_connector_static_predicate_pushdown = Some(false);
        assert!(!off.connector_static_predicate_pushdown_enabled());

        let mut on = SessionOptimizerSettings::default();
        on.enable_connector_static_predicate_pushdown = Some(true);
        assert!(on.connector_static_predicate_pushdown_enabled());
    }
}
