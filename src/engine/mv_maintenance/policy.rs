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

//! Pure decision logic for automatic Iceberg MV maintenance. No IO: every
//! input is collected by stats.rs / the coordinator and passed in by value,
//! which keeps the whole policy table-test friendly.

use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(crate) const DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000; // 5 days (Iceberg default)
pub(crate) const DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
pub(crate) const DEFAULT_TARGET_FILE_SIZE_BYTES: u64 = 536_870_912; // 512 MiB (Iceberg default)
/// avg file size < 3/4 of target counts as "small files dominate".
const SMALL_FILE_RATIO_NUM: u64 = 3;
const SMALL_FILE_RATIO_DEN: u64 = 4;
/// DV compaction needs at least this many position-delete input files to be
/// worth a rewrite pass.
const MIN_DV_INPUT_FILES: usize = 2;
/// Failure backoff is fixed (not config-exposed in v1), matching the spec.
pub(crate) const FAILURE_BACKOFF_BASE_MS: i64 = 60_000;
pub(crate) const FAILURE_BACKOFF_MAX_MS: i64 = 1_800_000;

pub(crate) const MAINTENANCE_ENABLED_PROPERTY: &str = "novarocks.maintenance.enabled";
pub(crate) const EXPIRE_MAX_AGE_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
pub(crate) const EXPIRE_MIN_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
pub(crate) const TARGET_FILE_SIZE_PROPERTY: &str = "write.target-file-size-bytes";

/// Global thresholds resolved from `[standalone_server]` config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenancePolicyConfig {
    pub(crate) compaction_min_data_files: u64,
    pub(crate) dv_min_delete_files: u64,
    pub(crate) action_cooldown_ms: i64,
    /// Circuit-breaker threshold: the coordinator trips the breaker for an
    /// action after this many consecutive failures (enforced in the
    /// coordinator, not in this pure module).
    pub(crate) max_consecutive_failures: u32,
}

impl Default for MaintenancePolicyConfig {
    fn default() -> Self {
        Self {
            compaction_min_data_files: 100,
            dv_min_delete_files: 10,
            action_cooldown_ms: 3_600_000,
            max_consecutive_failures: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotInfo {
    pub(crate) snapshot_id: i64,
    pub(crate) timestamp_ms: i64,
}

/// Raw facts about one MV storage table, collected from a single metadata load.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableMaintenanceStats {
    pub(crate) current_snapshot_id: Option<i64>,
    pub(crate) snapshots: Vec<SnapshotInfo>,
    pub(crate) total_data_files: Option<u64>,
    pub(crate) max_compactable_data_files: Option<u64>,
    pub(crate) total_files_size_bytes: Option<u64>,
    pub(crate) total_delete_files: Option<u64>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) non_main_ref_count: usize,
    /// min over downstream incremental consumers of the timestamp of their
    /// last-consumed snapshot of this table. None = no downstream consumers.
    pub(crate) downstream_floor_ts_ms: Option<i64>,
    /// true when a downstream consumer references a snapshot we could not
    /// resolve in this table's metadata; expire must then be skipped.
    pub(crate) downstream_floor_unknown: bool,
}

/// Per-table policy: global defaults overridden by table properties.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TablePolicy {
    pub(crate) enabled: bool,
    pub(crate) expire_max_age_ms: i64,
    pub(crate) expire_min_keep: u32,
    pub(crate) target_file_size_bytes: u64,
    pub(crate) compaction_min_data_files: u64,
    pub(crate) dv_min_delete_files: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ActionKind {
    Expire,
    RewriteDv,
    Optimize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceAction {
    ExpireSnapshots {
        older_than_ms: i64,
        retain_last: u32,
    },
    RewritePositionDeletes {
        min_input_files: usize,
    },
    SubmitOptimize,
}

impl MaintenanceAction {
    pub(crate) fn kind(&self) -> ActionKind {
        match self {
            Self::ExpireSnapshots { .. } => ActionKind::Expire,
            Self::RewritePositionDeletes { .. } => ActionKind::RewriteDv,
            Self::SubmitOptimize => ActionKind::Optimize,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Disabled,
    NonMainRefs,
    DownstreamFloorUnknown,
    NothingToExpire,
    SnapshotUnchanged,
    MissingSummaryStats,
    BelowThreshold,
    SuppressedByOptimize,
    Cooldown,
    FailureBackoff,
    CircuitBroken,
}

/// Coordinator-memory state for one table; lost on restart by design (all
/// actions are idempotent re-evaluations of current metadata).
#[derive(Clone, Debug, Default)]
pub(crate) struct TableRuntimeState {
    pub(crate) last_seen_snapshot_id: Option<i64>,
    pub(crate) last_action_ms: BTreeMap<ActionKind, i64>,
    /// Per-action consecutive failure counts, maintained by the coordinator and
    /// checked against `MaintenancePolicyConfig::max_consecutive_failures`.
    pub(crate) consecutive_failures: BTreeMap<ActionKind, u32>,
    pub(crate) next_attempt_after_ms: BTreeMap<ActionKind, i64>,
    pub(crate) circuit_broken: BTreeSet<ActionKind>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EvaluationOutcome {
    pub(crate) actions: Vec<MaintenanceAction>,
    pub(crate) skips: Vec<(ActionKind, SkipReason)>,
}

impl TablePolicy {
    pub(crate) fn resolve(
        global: &MaintenancePolicyConfig,
        properties: &HashMap<String, String>,
    ) -> Self {
        fn parse_or<T: std::str::FromStr>(
            properties: &HashMap<String, String>,
            key: &str,
            default: T,
        ) -> T {
            properties
                .get(key)
                .and_then(|v| v.trim().parse::<T>().ok())
                .unwrap_or(default)
        }
        let enabled = properties
            .get(MAINTENANCE_ENABLED_PROPERTY)
            .map(|v| !v.trim().eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        Self {
            enabled,
            expire_max_age_ms: parse_or(
                properties,
                EXPIRE_MAX_AGE_PROPERTY,
                DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS,
            ),
            expire_min_keep: parse_or(
                properties,
                EXPIRE_MIN_KEEP_PROPERTY,
                DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP,
            )
            .max(1),
            target_file_size_bytes: parse_or(
                properties,
                TARGET_FILE_SIZE_PROPERTY,
                DEFAULT_TARGET_FILE_SIZE_BYTES,
            )
            .max(1),
            compaction_min_data_files: global.compaction_min_data_files.max(1),
            dv_min_delete_files: global.dv_min_delete_files.max(1),
        }
    }
}

pub(crate) fn failure_backoff_ms(attempt: u32) -> i64 {
    let shift = attempt.max(1).saturating_sub(1).min(62);
    let multiplier = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
    FAILURE_BACKOFF_BASE_MS
        .saturating_mul(multiplier)
        .min(FAILURE_BACKOFF_MAX_MS)
}

/// Per-action admission guards shared by all three actions.
fn admission(
    kind: ActionKind,
    runtime: &TableRuntimeState,
    global: &MaintenancePolicyConfig,
    now_ms: i64,
) -> Result<(), SkipReason> {
    if runtime.circuit_broken.contains(&kind) {
        return Err(SkipReason::CircuitBroken);
    }
    if runtime
        .next_attempt_after_ms
        .get(&kind)
        .map(|next| *next > now_ms)
        .unwrap_or(false)
    {
        return Err(SkipReason::FailureBackoff);
    }
    // Cooldown applies to write-amplifying actions only; expire is naturally
    // rate-limited by candidate availability.
    if matches!(kind, ActionKind::Optimize | ActionKind::RewriteDv)
        && runtime
            .last_action_ms
            .get(&kind)
            .map(|last| last.saturating_add(global.action_cooldown_ms) > now_ms)
            .unwrap_or(false)
    {
        return Err(SkipReason::Cooldown);
    }
    Ok(())
}

fn plan_expire(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
    now_ms: i64,
) -> Result<MaintenanceAction, SkipReason> {
    if stats.non_main_ref_count > 0 {
        return Err(SkipReason::NonMainRefs);
    }
    if stats.downstream_floor_unknown {
        return Err(SkipReason::DownstreamFloorUnknown);
    }
    let mut cutoff = now_ms.saturating_sub(policy.expire_max_age_ms);
    if let Some(floor) = stats.downstream_floor_ts_ms {
        cutoff = cutoff.min(floor);
    }
    if stats.snapshots.len() <= policy.expire_min_keep as usize {
        return Err(SkipReason::NothingToExpire);
    }
    let expirable = stats
        .snapshots
        .iter()
        .filter(|s| s.timestamp_ms < cutoff && Some(s.snapshot_id) != stats.current_snapshot_id)
        .count();
    if expirable == 0 {
        return Err(SkipReason::NothingToExpire);
    }
    Ok(MaintenanceAction::ExpireSnapshots {
        older_than_ms: cutoff,
        retain_last: policy.expire_min_keep,
    })
}

fn plan_optimize(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
) -> Result<MaintenanceAction, SkipReason> {
    let (Some(files), Some(size)) = (stats.total_data_files, stats.total_files_size_bytes) else {
        return Err(SkipReason::MissingSummaryStats);
    };
    let compactable_files = stats.max_compactable_data_files.unwrap_or(files).min(files);
    if files == 0 || compactable_files < policy.compaction_min_data_files {
        return Err(SkipReason::BelowThreshold);
    }
    let avg = size / files;
    // Trigger only when avg < (NUM/DEN) * target, i.e. small files dominate.
    if avg.saturating_mul(SMALL_FILE_RATIO_DEN)
        >= policy
            .target_file_size_bytes
            .saturating_mul(SMALL_FILE_RATIO_NUM)
    {
        return Err(SkipReason::BelowThreshold);
    }
    Ok(MaintenanceAction::SubmitOptimize)
}

fn plan_rewrite_dv(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
) -> Result<MaintenanceAction, SkipReason> {
    let Some(delete_files) = stats.total_delete_files else {
        return Err(SkipReason::MissingSummaryStats);
    };
    if delete_files < policy.dv_min_delete_files {
        return Err(SkipReason::BelowThreshold);
    }
    Ok(MaintenanceAction::RewritePositionDeletes {
        min_input_files: MIN_DV_INPUT_FILES,
    })
}

pub(crate) fn evaluate_table(
    stats: &TableMaintenanceStats,
    policy: &TablePolicy,
    runtime: &TableRuntimeState,
    global: &MaintenancePolicyConfig,
    now_ms: i64,
) -> EvaluationOutcome {
    let mut out = EvaluationOutcome::default();
    if !policy.enabled {
        for kind in [
            ActionKind::Expire,
            ActionKind::RewriteDv,
            ActionKind::Optimize,
        ] {
            out.skips.push((kind, SkipReason::Disabled));
        }
        return out;
    }

    // Expire: evaluated every pass (pure computation over loaded metadata).
    match admission(ActionKind::Expire, runtime, global, now_ms)
        .and_then(|()| plan_expire(stats, policy, now_ms))
    {
        Ok(action) => out.actions.push(action),
        Err(reason) => out.skips.push((ActionKind::Expire, reason)),
    }

    // Compaction signals only make sense when the table changed since the
    // last pass (Dremio-style short circuit).
    let snapshot_changed = stats.current_snapshot_id != runtime.last_seen_snapshot_id;
    if !snapshot_changed {
        out.skips
            .push((ActionKind::Optimize, SkipReason::SnapshotUnchanged));
        out.skips
            .push((ActionKind::RewriteDv, SkipReason::SnapshotUnchanged));
        return out;
    }

    let optimize = admission(ActionKind::Optimize, runtime, global, now_ms)
        .and_then(|()| plan_optimize(stats, policy));
    let optimize_planned = optimize.is_ok();
    match optimize {
        Ok(action) => out.actions.push(action),
        Err(reason) => out.skips.push((ActionKind::Optimize, reason)),
    }

    if optimize_planned {
        // Whole-table rewrite absorbs delete files; a DV pass would be wasted.
        out.skips
            .push((ActionKind::RewriteDv, SkipReason::SuppressedByOptimize));
    } else {
        match admission(ActionKind::RewriteDv, runtime, global, now_ms)
            .and_then(|()| plan_rewrite_dv(stats, policy))
        {
            Ok(action) => out.actions.push(action),
            Err(reason) => out.skips.push((ActionKind::RewriteDv, reason)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_stats() -> TableMaintenanceStats {
        TableMaintenanceStats {
            current_snapshot_id: Some(30),
            snapshots: vec![
                SnapshotInfo {
                    snapshot_id: 10,
                    timestamp_ms: 1_000,
                },
                SnapshotInfo {
                    snapshot_id: 20,
                    timestamp_ms: 2_000,
                },
                SnapshotInfo {
                    snapshot_id: 30,
                    timestamp_ms: 3_000,
                },
            ],
            total_data_files: Some(200),
            max_compactable_data_files: Some(200),
            total_files_size_bytes: Some(200 * 1024 * 1024), // avg 1 MiB << 384 MiB
            total_delete_files: Some(0),
            properties: HashMap::new(),
            non_main_ref_count: 0,
            downstream_floor_ts_ms: None,
            downstream_floor_unknown: false,
        }
    }

    fn enabled_policy() -> TablePolicy {
        TablePolicy::resolve(&MaintenancePolicyConfig::default(), &HashMap::new())
    }

    const NOW: i64 = 1_000_000_000;

    // --- TablePolicy::resolve ---
    #[test]
    fn resolve_uses_iceberg_defaults_without_properties() {
        let p = enabled_policy();
        assert!(p.enabled);
        assert_eq!(p.expire_max_age_ms, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
        assert_eq!(p.expire_min_keep, DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP);
        assert_eq!(p.target_file_size_bytes, DEFAULT_TARGET_FILE_SIZE_BYTES);
        assert_eq!(p.compaction_min_data_files, 100);
        assert_eq!(p.dv_min_delete_files, 10);
    }

    #[test]
    fn resolve_honors_table_properties() {
        let mut props = HashMap::new();
        props.insert(EXPIRE_MAX_AGE_PROPERTY.to_string(), "1000".to_string());
        props.insert(EXPIRE_MIN_KEEP_PROPERTY.to_string(), "3".to_string());
        props.insert(TARGET_FILE_SIZE_PROPERTY.to_string(), "1048576".to_string());
        props.insert(
            MAINTENANCE_ENABLED_PROPERTY.to_string(),
            "false".to_string(),
        );
        let p = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        assert!(!p.enabled);
        assert_eq!(p.expire_max_age_ms, 1000);
        assert_eq!(p.expire_min_keep, 3);
        assert_eq!(p.target_file_size_bytes, 1_048_576);
    }

    #[test]
    fn resolve_ignores_malformed_property_values() {
        let mut props = HashMap::new();
        props.insert(
            EXPIRE_MAX_AGE_PROPERTY.to_string(),
            "not-a-number".to_string(),
        );
        let p = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        assert_eq!(p.expire_max_age_ms, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
    }

    // --- disabled / guards ---
    #[test]
    fn disabled_table_skips_everything() {
        let mut props = HashMap::new();
        props.insert(
            MAINTENANCE_ENABLED_PROPERTY.to_string(),
            "false".to_string(),
        );
        let policy = TablePolicy::resolve(&MaintenancePolicyConfig::default(), &props);
        let out = evaluate_table(
            &base_stats(),
            &policy,
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.is_empty());
        assert!(out.skips.iter().all(|(_, r)| *r == SkipReason::Disabled));
    }

    // --- expire ---
    #[test]
    fn expire_triggers_when_old_snapshots_exist() {
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        let expire = out
            .actions
            .iter()
            .find(|a| a.kind() == ActionKind::Expire)
            .expect("expire planned");
        match expire {
            MaintenanceAction::ExpireSnapshots {
                older_than_ms,
                retain_last,
            } => {
                assert_eq!(*older_than_ms, NOW - DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
                assert_eq!(*retain_last, 1);
            }
            other => panic!("unexpected action {other:?}"),
        }
    }

    #[test]
    fn expire_skips_when_no_snapshot_is_old_enough() {
        let mut stats = base_stats();
        for (i, s) in stats.snapshots.iter_mut().enumerate() {
            s.timestamp_ms = NOW - i as i64;
        }
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::NothingToExpire))
        );
    }

    #[test]
    fn expire_cutoff_is_tightened_by_downstream_floor() {
        let mut stats = base_stats();
        stats.downstream_floor_ts_ms = Some(1_500); // protect snapshot 20 and newer
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        match out.actions.iter().find(|a| a.kind() == ActionKind::Expire) {
            Some(MaintenanceAction::ExpireSnapshots { older_than_ms, .. }) => {
                assert_eq!(*older_than_ms, 1_500);
            }
            other => panic!("expected tightened expire, got {other:?}"),
        }
    }

    #[test]
    fn expire_skips_when_downstream_floor_unknown() {
        let mut stats = base_stats();
        stats.downstream_floor_unknown = true;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::DownstreamFloorUnknown))
        );
    }

    #[test]
    fn expire_skips_when_non_main_refs_exist() {
        let mut stats = base_stats();
        stats.non_main_ref_count = 1;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::NonMainRefs))
        );
    }

    #[test]
    fn expire_respects_min_snapshots_to_keep() {
        let mut stats = base_stats();
        stats.snapshots.truncate(1); // single snapshot, min_keep = 1
        stats.current_snapshot_id = Some(10);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::NothingToExpire))
        );
    }

    // --- optimize / dv ---
    #[test]
    fn optimize_triggers_on_many_small_files_and_suppresses_dv() {
        let mut stats = base_stats();
        stats.total_delete_files = Some(50); // DV would trigger alone
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.contains(&MaintenanceAction::SubmitOptimize));
        assert!(
            out.skips
                .contains(&(ActionKind::RewriteDv, SkipReason::SuppressedByOptimize))
        );
    }

    #[test]
    fn optimize_skips_below_file_count_threshold() {
        let mut stats = base_stats();
        stats.total_data_files = Some(99);
        stats.max_compactable_data_files = Some(99);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::BelowThreshold))
        );
    }

    #[test]
    fn optimize_skips_when_no_compactable_group_reaches_threshold() {
        let mut stats = base_stats();
        stats.total_data_files = Some(200);
        stats.max_compactable_data_files = Some(1);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::BelowThreshold))
        );
    }

    #[test]
    fn optimize_skips_when_avg_file_size_is_large() {
        let mut stats = base_stats();
        stats.total_files_size_bytes =
            Some(stats.total_data_files.unwrap() * DEFAULT_TARGET_FILE_SIZE_BYTES);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::BelowThreshold))
        );
    }

    #[test]
    fn optimize_skips_when_summary_stats_missing() {
        let mut stats = base_stats();
        stats.total_files_size_bytes = None;
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::MissingSummaryStats))
        );
    }

    #[test]
    fn dv_triggers_on_delete_file_threshold_without_optimize() {
        let mut stats = base_stats();
        stats.total_data_files = Some(10); // below optimize threshold
        stats.total_delete_files = Some(10);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.actions
                .contains(&MaintenanceAction::RewritePositionDeletes { min_input_files: 2 })
        );
    }

    #[test]
    fn compaction_signals_skip_when_snapshot_unchanged() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_seen_snapshot_id = Some(30); // == current
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::SnapshotUnchanged))
        );
        assert!(out.actions.iter().any(|a| a.kind() == ActionKind::Expire));
    }

    // --- cooldown / backoff / circuit ---
    #[test]
    fn optimize_respects_cooldown() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_action_ms.insert(ActionKind::Optimize, NOW - 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::Cooldown))
        );
    }

    #[test]
    fn optimize_boundary_at_three_quarters_target() {
        // avg == 0.75 * target is NOT "small files" -> skip.
        let mut stats = base_stats();
        let files = stats.total_data_files.unwrap();
        stats.total_files_size_bytes = Some(files * DEFAULT_TARGET_FILE_SIZE_BYTES / 4 * 3);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Optimize, SkipReason::BelowThreshold)),
            "avg == 0.75*target must skip optimize"
        );
        // Drop avg clearly below the 0.75 boundary -> trigger.
        stats.total_files_size_bytes =
            Some(files * (DEFAULT_TARGET_FILE_SIZE_BYTES / 4 * 3 - files));
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &TableRuntimeState::default(),
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.actions.contains(&MaintenanceAction::SubmitOptimize),
            "avg below 0.75*target must trigger optimize"
        );
    }

    #[test]
    fn rewrite_dv_respects_cooldown() {
        let mut stats = base_stats();
        stats.total_data_files = Some(10); // below optimize threshold, so DV is the candidate
        stats.total_delete_files = Some(10);
        let mut runtime = TableRuntimeState::default();
        runtime
            .last_action_ms
            .insert(ActionKind::RewriteDv, NOW - 1);
        let out = evaluate_table(
            &stats,
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::RewriteDv, SkipReason::Cooldown))
        );
    }

    #[test]
    fn expire_has_no_cooldown() {
        let mut runtime = TableRuntimeState::default();
        runtime.last_action_ms.insert(ActionKind::Expire, NOW - 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(out.actions.iter().any(|a| a.kind() == ActionKind::Expire));
    }

    #[test]
    fn failure_backoff_defers_action() {
        let mut runtime = TableRuntimeState::default();
        runtime
            .next_attempt_after_ms
            .insert(ActionKind::Expire, NOW + 1);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::FailureBackoff))
        );
    }

    #[test]
    fn circuit_breaker_blocks_action() {
        let mut runtime = TableRuntimeState::default();
        runtime.circuit_broken.insert(ActionKind::Expire);
        let out = evaluate_table(
            &base_stats(),
            &enabled_policy(),
            &runtime,
            &MaintenancePolicyConfig::default(),
            NOW,
        );
        assert!(
            out.skips
                .contains(&(ActionKind::Expire, SkipReason::CircuitBroken))
        );
    }

    #[test]
    fn failure_backoff_is_bounded_exponential() {
        assert_eq!(failure_backoff_ms(1), 60_000);
        assert_eq!(failure_backoff_ms(2), 120_000);
        assert_eq!(failure_backoff_ms(10), FAILURE_BACKOFF_MAX_MS);
    }
}
