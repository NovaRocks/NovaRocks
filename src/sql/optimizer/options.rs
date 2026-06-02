//! Per-optimize-call configuration shared by logical rewrite and CBO drivers.

use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SessionOptimizerSettings {
    pub enable_ukfk_opt: bool,
    pub enable_query_rewrite_table_prune: bool,
    pub enable_cbo_table_prune: bool,
    pub enable_table_prune_on_update: bool,
    pub enable_eliminate_agg: bool,
    pub disabled_rules: Vec<String>,
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
    /// Hard cap on the CBO Memo group count (existing constant; documented here).
    #[allow(dead_code)]
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
    fn runtime_filter_thresholds_default_to_starrocks() {
        let o = OptimizerOptions::default_settings();
        assert_eq!(o.rf_build_max_bytes, 64 * 1024 * 1024);
        assert_eq!(o.rf_build_min_bytes, 128 * 1024);
        assert_eq!(o.rf_probe_min_bytes, 100 * 1024);
        assert!((o.rf_probe_min_selectivity - 0.5).abs() < 1e-9);
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
}
