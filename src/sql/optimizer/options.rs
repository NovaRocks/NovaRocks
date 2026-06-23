//! Per-optimize-call configuration shared by logical rewrite and CBO drivers.

use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

use crate::sql::optimizer::cascades_rules::multi_join_reorder::ReorderOptions;
use crate::sql::optimizer::cost::CostOptions;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SessionOptimizerSettings {
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
    /// Session override for transparent MV query rewrite.
    /// `None` means the default (enabled).
    pub enable_materialized_view_rewrite: Option<bool>,
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
}

impl SessionOptimizerSettings {
    pub(crate) fn mv_rewrite_enabled(&self) -> bool {
        self.enable_materialized_view_rewrite.unwrap_or(true)
    }
}

thread_local! {
    static SESSION_OPTIMIZER_SETTINGS: RefCell<SessionOptimizerSettings> =
        RefCell::new(SessionOptimizerSettings::default());
}

pub(crate) fn with_session_optimizer_settings<T>(
    settings: SessionOptimizerSettings,
    f: impl FnOnce() -> T,
) -> T {
    SESSION_OPTIMIZER_SETTINGS.with(|cell| {
        let previous = cell.replace(settings);
        let result = f();
        cell.replace(previous);
        result
    })
}

pub(crate) fn current_session_optimizer_settings() -> SessionOptimizerSettings {
    SESSION_OPTIMIZER_SETTINGS.with(|cell| cell.borrow().clone())
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
    pub rf_max_count: usize,
    /// Whether probe runtime filters may be placed across shuffle exchanges
    /// when placement is conservative. Cross-exchange placement requires a
    /// complete build RF; currently that means broadcast joins only. Partial
    /// partitioned RFs still stop at exchange boundaries even when this flag is
    /// true, and probe pushdown stops at outer/anti/null-preserving semantic
    /// boundaries.
    pub allow_cross_exchange_rf: bool,
    /// In-memo join-reorder knobs (algorithm toggles + size cutoffs). Defaults
    /// match StarRocks; overridable via the `cbo_enable_dp/greedy_join_reorder`
    /// and `cbo_max_reorder_node*` session variables.
    pub reorder: ReorderOptions,
    /// Physical operator cost-model options. Currently not session-overridable.
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
            opts.disable(crate::sql::optimizer::cse_pass::CSE_RULE);
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
        // `allow_cross_exchange_rf` has no session override; the default is safe
        // because placement rejects partial partitioned RF.
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
        assert!(!opts.is_enabled(crate::sql::optimizer::cse_pass::CSE_RULE));
    }

    #[test]
    fn cse_session_default_and_true_enable_rule() {
        let default_opts = OptimizerOptions::from_session(&SessionOptimizerSettings::default());
        assert!(default_opts.is_enabled(crate::sql::optimizer::cse_pass::CSE_RULE));

        let settings = SessionOptimizerSettings {
            enable_common_subexpr_reuse: Some(true),
            ..Default::default()
        };
        let opts = OptimizerOptions::from_session(&settings);
        assert!(opts.is_enabled(crate::sql::optimizer::cse_pass::CSE_RULE));
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
        let default_cost_options = crate::sql::optimizer::cost::CostOptions::default();

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
}
