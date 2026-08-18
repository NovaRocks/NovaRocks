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

//! Frontend-owned policy and admission for automatic MV maintenance.
//!
//! This module intentionally consumes only [`MvMaintenanceFacts`].  Provider
//! metadata stays behind the Core background-engine port, while the frontend
//! decides retry, capacity, and the durable lifecycle route for every action.
//! A host must obtain the per-MV activity gate *before* calling
//! [`MaintenanceCoordinator::try_begin`]; a queued gate ticket therefore never
//! consumes the independent maintenance concurrency budget.

use std::collections::{BTreeMap, BTreeSet};

use super::background::{MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts};
use crate::query_execution::maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, OptimizeSubmission,
};
use novarocks::maintenance::MaintenanceTarget;

const DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000;
const DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
const DEFAULT_TARGET_FILE_SIZE_BYTES: i64 = 536_870_912;
const MIN_POSITION_DELETE_INPUT_FILES: usize = 2;
const SMALL_FILE_RATIO_NUMERATOR: i64 = 3;
const SMALL_FILE_RATIO_DENOMINATOR: i64 = 4;
const FAILURE_BACKOFF_BASE_MS: i64 = 60_000;
const FAILURE_BACKOFF_MAX_MS: i64 = 1_800_000;

/// Existing `[standalone_server]` values projected into the frontend owner.
/// `max_concurrent` is a real attempt limit: one admitted attempt includes a
/// complete policy evaluation and all of its actions for one MV.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceCoordinatorConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent: usize,
    pub(crate) compaction_min_data_files: i64,
    pub(crate) dv_min_delete_files: i64,
    pub(crate) action_cooldown_ms: i64,
    pub(crate) max_consecutive_failures: u32,
}

impl MaintenanceCoordinatorConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent: usize,
        compaction_min_data_files: i64,
        dv_min_delete_files: i64,
        action_cooldown_ms: i64,
        max_consecutive_failures: u32,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent,
            compaction_min_data_files,
            dv_min_delete_files,
            action_cooldown_ms,
            max_consecutive_failures,
        }
    }
}

impl Default for MaintenanceCoordinatorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick_interval_ms: 60_000,
            max_concurrent: 1,
            compaction_min_data_files: 100,
            dv_min_delete_files: 10,
            action_cooldown_ms: 3_600_000,
            max_consecutive_failures: 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MaintenanceActionKind {
    Expire,
    RewritePositionDeletes,
    Optimize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AutomaticMaintenanceAction {
    ExpireSnapshots {
        older_than_ms: i64,
        retain_last: u32,
    },
    RewritePositionDeletes {
        min_input_files: usize,
    },
    Optimize,
}

impl AutomaticMaintenanceAction {
    pub(crate) fn kind(&self) -> MaintenanceActionKind {
        match self {
            Self::ExpireSnapshots { .. } => MaintenanceActionKind::Expire,
            Self::RewritePositionDeletes { .. } => MaintenanceActionKind::RewritePositionDeletes,
            Self::Optimize => MaintenanceActionKind::Optimize,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceSkipReason {
    Disabled,
    NonDefaultRefs,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceEvaluation {
    pub(crate) actions: Vec<AutomaticMaintenanceAction>,
    pub(crate) skips: Vec<(MaintenanceActionKind, MaintenanceSkipReason)>,
}

#[derive(Clone, Debug, Default)]
struct TableRuntimeState {
    last_seen_snapshot_id: Option<i64>,
    last_action_ms: BTreeMap<MaintenanceActionKind, i64>,
    consecutive_failures: BTreeMap<MaintenanceActionKind, u32>,
    next_attempt_after_ms: BTreeMap<MaintenanceActionKind, i64>,
    circuit_broken: BTreeSet<MaintenanceActionKind>,
}

/// A policy pass admitted by the frontend maintenance worker.  The caller
/// owns the matching activity-gate lease for the whole lifetime of this value.
#[derive(Clone, Debug)]
pub(crate) struct MaintenanceAttempt {
    mv_id: i64,
    target: MaintenanceTarget,
    evaluation: MaintenanceEvaluation,
    observed_snapshot_id: Option<i64>,
}

impl MaintenanceAttempt {
    pub(crate) fn target(&self) -> &MaintenanceTarget {
        &self.target
    }

    pub(crate) fn evaluation(&self) -> &MaintenanceEvaluation {
        &self.evaluation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceAdmission {
    Disabled,
    AtCapacity,
    AlreadyActive,
    Admitted,
}

/// Result of a policy evaluation plus its durable actions.  `NoOp` is still a
/// complete attempt: it proves that the current provider facts were evaluated
/// while holding the MV gate, rather than treating absent actions as a retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceExecutionReport {
    pub(crate) evaluation: MaintenanceEvaluation,
    pub(crate) completed: Vec<MaintenanceActionKind>,
    pub(crate) already_active: Vec<MaintenanceActionKind>,
    pub(crate) failures: Vec<(MaintenanceActionKind, MvBackgroundEngineErrorKind)>,
}

impl MaintenanceExecutionReport {
    pub(crate) fn is_noop(&self) -> bool {
        self.evaluation.actions.is_empty()
    }
}

/// The only automatic-maintenance side-effect boundary.  Each method names a
/// durable lifecycle on purpose: implementations must not route any automatic
/// action through `TableMaintenanceEngine::execute_action`.
pub(crate) trait AutomaticMaintenanceRunner {
    /// Create, plan, execute and reconcile the existing durable metadata
    /// operation for `ExpireSnapshots`.
    fn expire_snapshots_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError>;

    /// Create, stage, commit/finalize or retain the existing durable
    /// distributed-rewrite operation for position deletes.
    fn rewrite_position_deletes_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError>;

    /// Create, claim, execute and terminally persist the existing durable
    /// optimize job before this method returns.  Submission alone is not a
    /// completed automatic optimize attempt.
    fn optimize_durably(
        &mut self,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, MvBackgroundEngineError>;
}

/// Process-local maintenance policy state.  It is intentionally non-durable:
/// recovery re-evaluates current provider facts and durable action state.
pub(crate) struct MaintenanceCoordinator {
    config: MaintenanceCoordinatorConfig,
    runtime: BTreeMap<i64, TableRuntimeState>,
    active: BTreeSet<i64>,
}

impl MaintenanceCoordinator {
    pub(crate) fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self {
            config: MaintenanceCoordinatorConfig {
                tick_interval_ms: config.tick_interval_ms.max(1),
                max_concurrent: config.max_concurrent.max(1),
                compaction_min_data_files: config.compaction_min_data_files.max(1),
                dv_min_delete_files: config.dv_min_delete_files.max(1),
                max_consecutive_failures: config.max_consecutive_failures.max(1),
                ..config
            },
            runtime: BTreeMap::new(),
            active: BTreeSet::new(),
        }
    }

    pub(crate) fn config(&self) -> &MaintenanceCoordinatorConfig {
        &self.config
    }

    /// Admit work only after the caller has acquired the MV activity gate.
    /// This is what keeps a FIFO gate waiter from consuming a maintenance
    /// permit.  The returned attempt holds one permit until `run_attempt` or
    /// `cancel_attempt` is called.
    pub(crate) fn try_begin(
        &mut self,
        mv_id: i64,
        target: MaintenanceTarget,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> Result<MaintenanceAttempt, MaintenanceAdmission> {
        if !self.config.enabled {
            return Err(MaintenanceAdmission::Disabled);
        }
        if self.active.contains(&mv_id) {
            return Err(MaintenanceAdmission::AlreadyActive);
        }
        if self.active.len() >= self.config.max_concurrent {
            return Err(MaintenanceAdmission::AtCapacity);
        }
        let evaluation = self.evaluate(mv_id, facts, now_ms);
        self.active.insert(mv_id);
        Ok(MaintenanceAttempt {
            mv_id,
            target,
            observed_snapshot_id: facts.current_snapshot_id,
            evaluation,
        })
    }

    /// Execute external durable actions without holding the coordinator lock.
    /// The caller must subsequently call [`Self::finish_attempt`] while
    /// retaining the attempt's activity lease.  Splitting execution from
    /// admission is what makes `max_concurrent` a real parallelism bound
    /// rather than a mutex-shaped serial queue.
    pub(crate) fn execute_attempt(
        attempt: &MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
    ) -> MaintenanceExecutionReport {
        let mut report = MaintenanceExecutionReport {
            evaluation: attempt.evaluation.clone(),
            completed: Vec::new(),
            already_active: Vec::new(),
            failures: Vec::new(),
        };
        for action in &attempt.evaluation.actions {
            let kind = action.kind();
            let result = match action {
                AutomaticMaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                } => runner.expire_snapshots_durably(MaintenanceActionRequest::ExpireSnapshots {
                    target: attempt.target.clone(),
                    older_than_ms: Some(*older_than_ms),
                    retain_last: Some(*retain_last),
                }),
                AutomaticMaintenanceAction::RewritePositionDeletes { min_input_files } => {
                    let mut options = BTreeMap::new();
                    options.insert("min-input-files".to_string(), min_input_files.to_string());
                    runner.rewrite_position_deletes_durably(
                        MaintenanceActionRequest::RewritePositionDeleteFiles {
                            target: attempt.target.clone(),
                            options,
                            where_clause: None,
                        },
                    )
                }
                AutomaticMaintenanceAction::Optimize => {
                    match runner.optimize_durably(attempt.target.clone()) {
                        Ok(OptimizeSubmission::Submitted { .. }) => {
                            report.completed.push(kind);
                            continue;
                        }
                        Ok(OptimizeSubmission::AlreadyActive) => {
                            report.already_active.push(kind);
                            continue;
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            match result {
                Ok(outcome) if expected_outcome(kind, &outcome) => {
                    report.completed.push(kind);
                }
                Ok(outcome) => {
                    tracing::error!(action = ?kind, ?outcome, "automatic maintenance returned an incompatible durable outcome");
                    report
                        .failures
                        .push((kind, MvBackgroundEngineErrorKind::InvariantViolation));
                }
                Err(error) => {
                    report.failures.push((kind, error.kind()));
                }
            }
        }
        report
    }

    /// Persist the local policy outcome and release the permit after external
    /// execution completes. This must be called exactly once for every
    /// admitted attempt, including a cancellation before dispatch.
    pub(crate) fn finish_attempt(
        &mut self,
        attempt: MaintenanceAttempt,
        report: &MaintenanceExecutionReport,
        now_ms: i64,
    ) {
        for kind in &report.completed {
            self.record_success(attempt.mv_id, *kind, now_ms);
        }
        for kind in &report.already_active {
            self.record_success(attempt.mv_id, *kind, now_ms);
        }
        for (kind, error) in &report.failures {
            self.record_failure(attempt.mv_id, *kind, *error, now_ms);
        }
        self.active.remove(&attempt.mv_id);
        self.runtime_entry(attempt.mv_id).last_seen_snapshot_id = attempt.observed_snapshot_id;
    }

    #[cfg(test)]
    fn run_attempt(
        &mut self,
        attempt: MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
        now_ms: i64,
    ) -> MaintenanceExecutionReport {
        let report = Self::execute_attempt(&attempt, runner);
        self.finish_attempt(attempt, &report, now_ms);
        report
    }

    /// End a pre-dispatch or shutdown-cancelled attempt without converting it
    /// into success, a metadata observation, or an ordinary retry.
    pub(crate) fn cancel_attempt(&mut self, attempt: MaintenanceAttempt) {
        self.active.remove(&attempt.mv_id);
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.active.len()
    }

    fn runtime_entry(&mut self, mv_id: i64) -> &mut TableRuntimeState {
        self.runtime.entry(mv_id).or_default()
    }

    fn record_success(&mut self, mv_id: i64, kind: MaintenanceActionKind, now_ms: i64) {
        let state = self.runtime_entry(mv_id);
        state.last_action_ms.insert(kind, now_ms);
        state.consecutive_failures.remove(&kind);
        state.next_attempt_after_ms.remove(&kind);
        state.circuit_broken.remove(&kind);
    }

    fn record_failure(
        &mut self,
        mv_id: i64,
        kind: MaintenanceActionKind,
        error: MvBackgroundEngineErrorKind,
        now_ms: i64,
    ) {
        let max_consecutive_failures = self.config.max_consecutive_failures;
        let state = self.runtime_entry(mv_id);
        match error {
            MvBackgroundEngineErrorKind::TransientUnavailable => {
                let attempts = state.consecutive_failures.entry(kind).or_insert(0);
                *attempts = attempts.saturating_add(1);
                if *attempts >= max_consecutive_failures {
                    state.circuit_broken.insert(kind);
                    state.next_attempt_after_ms.remove(&kind);
                } else {
                    state
                        .next_attempt_after_ms
                        .insert(kind, now_ms.saturating_add(failure_backoff_ms(*attempts)));
                }
            }
            // A stopped frontend must neither fabricate a retry nor mark a
            // possibly external-unknown attempt as successful.
            MvBackgroundEngineErrorKind::ShutdownCancelled => {}
            // Target loss/recovery is re-discovered by the next inventory
            // scan; definition/corruption/invariant errors must not spin.
            MvBackgroundEngineErrorKind::TargetGone
            | MvBackgroundEngineErrorKind::RecoveryRequired
            | MvBackgroundEngineErrorKind::InvalidDefinition
            | MvBackgroundEngineErrorKind::Corruption
            | MvBackgroundEngineErrorKind::InvariantViolation => {
                state.circuit_broken.insert(kind);
            }
        }
    }

    fn evaluate(
        &mut self,
        mv_id: i64,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> MaintenanceEvaluation {
        let policy = TablePolicy::resolve(&self.config, facts);
        let state = self.runtime_entry(mv_id).clone();
        evaluate_facts(facts, &policy, &state, &self.config, now_ms)
    }
}

#[derive(Clone, Debug)]
struct TablePolicy {
    enabled: bool,
    expire_max_age_ms: i64,
    expire_min_keep: u32,
    target_file_size_bytes: i64,
    compaction_min_data_files: i64,
    dv_min_delete_files: i64,
}

impl TablePolicy {
    /// Apply frontend-owned defaults and clamps on top of the typed policy
    /// facts. An absent fact means the table declared nothing usable, so the
    /// default applies; a declared fact is still clamped to a workable range.
    fn resolve(config: &MaintenanceCoordinatorConfig, facts: &MvMaintenanceFacts) -> Self {
        Self {
            enabled: facts.maintenance_enabled.unwrap_or(true),
            expire_max_age_ms: facts
                .expire_max_snapshot_age_ms
                .unwrap_or(DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS)
                .max(1),
            expire_min_keep: facts
                .expire_min_snapshots_to_keep
                .unwrap_or(DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP)
                .max(1),
            target_file_size_bytes: facts
                .target_file_size_bytes
                .unwrap_or(DEFAULT_TARGET_FILE_SIZE_BYTES)
                .max(1),
            compaction_min_data_files: config.compaction_min_data_files,
            dv_min_delete_files: config.dv_min_delete_files,
        }
    }
}

fn evaluate_facts(
    facts: &MvMaintenanceFacts,
    policy: &TablePolicy,
    state: &TableRuntimeState,
    config: &MaintenanceCoordinatorConfig,
    now_ms: i64,
) -> MaintenanceEvaluation {
    let mut evaluation = MaintenanceEvaluation::default();
    if !policy.enabled {
        for kind in [
            MaintenanceActionKind::Expire,
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceActionKind::Optimize,
        ] {
            evaluation
                .skips
                .push((kind, MaintenanceSkipReason::Disabled));
        }
        return evaluation;
    }

    match admit(MaintenanceActionKind::Expire, state, config, now_ms)
        .and_then(|()| plan_expire(facts, policy, now_ms))
    {
        Ok(action) => evaluation.actions.push(action),
        Err(skip) => evaluation.skips.push((MaintenanceActionKind::Expire, skip)),
    }

    if facts.current_snapshot_id == state.last_seen_snapshot_id {
        evaluation.skips.push((
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::SnapshotUnchanged,
        ));
        evaluation.skips.push((
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceSkipReason::SnapshotUnchanged,
        ));
        return evaluation;
    }

    let optimize = admit(MaintenanceActionKind::Optimize, state, config, now_ms)
        .and_then(|()| plan_optimize(facts, policy));
    let optimize_planned = optimize.is_ok();
    match optimize {
        Ok(action) => evaluation.actions.push(action),
        Err(skip) => evaluation
            .skips
            .push((MaintenanceActionKind::Optimize, skip)),
    }
    if optimize_planned {
        evaluation.skips.push((
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceSkipReason::SuppressedByOptimize,
        ));
    } else {
        match admit(
            MaintenanceActionKind::RewritePositionDeletes,
            state,
            config,
            now_ms,
        )
        .and_then(|()| plan_rewrite_position_deletes(facts, policy))
        {
            Ok(action) => evaluation.actions.push(action),
            Err(skip) => evaluation
                .skips
                .push((MaintenanceActionKind::RewritePositionDeletes, skip)),
        }
    }
    evaluation
}

fn admit(
    kind: MaintenanceActionKind,
    state: &TableRuntimeState,
    config: &MaintenanceCoordinatorConfig,
    now_ms: i64,
) -> Result<(), MaintenanceSkipReason> {
    if state.circuit_broken.contains(&kind) {
        return Err(MaintenanceSkipReason::CircuitBroken);
    }
    if state
        .next_attempt_after_ms
        .get(&kind)
        .is_some_and(|next| *next > now_ms)
    {
        return Err(MaintenanceSkipReason::FailureBackoff);
    }
    if matches!(
        kind,
        MaintenanceActionKind::Optimize | MaintenanceActionKind::RewritePositionDeletes
    ) && state
        .last_action_ms
        .get(&kind)
        .is_some_and(|last| last.saturating_add(config.action_cooldown_ms) > now_ms)
    {
        return Err(MaintenanceSkipReason::Cooldown);
    }
    Ok(())
}

fn plan_expire(
    facts: &MvMaintenanceFacts,
    policy: &TablePolicy,
    now_ms: i64,
) -> Result<AutomaticMaintenanceAction, MaintenanceSkipReason> {
    if facts.non_default_reference_count > 0 {
        return Err(MaintenanceSkipReason::NonDefaultRefs);
    }
    if facts.downstream_floor_unknown {
        return Err(MaintenanceSkipReason::DownstreamFloorUnknown);
    }
    if facts.snapshot_count <= policy.expire_min_keep as usize {
        return Err(MaintenanceSkipReason::NothingToExpire);
    }
    let Some(oldest) = facts.oldest_snapshot_timestamp_ms else {
        return Err(MaintenanceSkipReason::NothingToExpire);
    };
    let mut older_than_ms = now_ms.saturating_sub(policy.expire_max_age_ms);
    if let Some(floor) = facts.downstream_floor_ts_ms {
        older_than_ms = older_than_ms.min(floor);
    }
    if oldest >= older_than_ms {
        return Err(MaintenanceSkipReason::NothingToExpire);
    }
    Ok(AutomaticMaintenanceAction::ExpireSnapshots {
        older_than_ms,
        retain_last: policy.expire_min_keep,
    })
}

fn plan_optimize(
    facts: &MvMaintenanceFacts,
    policy: &TablePolicy,
) -> Result<AutomaticMaintenanceAction, MaintenanceSkipReason> {
    let (Some(files), Some(size)) = (facts.total_data_files, facts.total_files_size_bytes) else {
        return Err(MaintenanceSkipReason::MissingSummaryStats);
    };
    let compactable = facts.max_compactable_data_files.unwrap_or(files).min(files);
    if files <= 0 || compactable < policy.compaction_min_data_files {
        return Err(MaintenanceSkipReason::BelowThreshold);
    }
    if size / files * SMALL_FILE_RATIO_DENOMINATOR
        >= policy.target_file_size_bytes * SMALL_FILE_RATIO_NUMERATOR
    {
        return Err(MaintenanceSkipReason::BelowThreshold);
    }
    Ok(AutomaticMaintenanceAction::Optimize)
}

fn plan_rewrite_position_deletes(
    facts: &MvMaintenanceFacts,
    policy: &TablePolicy,
) -> Result<AutomaticMaintenanceAction, MaintenanceSkipReason> {
    let Some(delete_files) = facts.total_delete_files else {
        return Err(MaintenanceSkipReason::MissingSummaryStats);
    };
    if delete_files < policy.dv_min_delete_files {
        return Err(MaintenanceSkipReason::BelowThreshold);
    }
    Ok(AutomaticMaintenanceAction::RewritePositionDeletes {
        min_input_files: MIN_POSITION_DELETE_INPUT_FILES,
    })
}

fn expected_outcome(kind: MaintenanceActionKind, outcome: &MaintenanceActionOutcome) -> bool {
    matches!(
        (kind, outcome),
        (
            MaintenanceActionKind::Expire,
            MaintenanceActionOutcome::ExpireSnapshots { .. }
        ) | (
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceActionOutcome::RewritePositionDeleteFiles { .. }
        )
    )
}

fn failure_backoff_ms(attempt: u32) -> i64 {
    let shift = attempt.max(1).saturating_sub(1).min(62);
    FAILURE_BACKOFF_BASE_MS
        .saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(FAILURE_BACKOFF_MAX_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_000_000_000;

    fn target(name: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: name.to_string(),
        }
    }

    fn facts() -> MvMaintenanceFacts {
        MvMaintenanceFacts {
            current_snapshot_id: Some(3),
            total_data_files: Some(200),
            max_compactable_data_files: Some(200),
            total_delete_files: Some(0),
            total_files_size_bytes: Some(200 * 1024 * 1024),
            oldest_snapshot_timestamp_ms: Some(1_000),
            snapshot_count: 3,
            ..MvMaintenanceFacts::default()
        }
    }

    struct Runner {
        transient_expire: bool,
        calls: Vec<MaintenanceActionKind>,
    }

    impl AutomaticMaintenanceRunner for Runner {
        fn expire_snapshots_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Expire);
            if self.transient_expire {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    "temporary metadata lease failure",
                ));
            }
            Ok(MaintenanceActionOutcome::ExpireSnapshots {
                deleted_data_files_count: None,
                deleted_position_delete_files_count: None,
                deleted_equality_delete_files_count: None,
                deleted_manifest_files_count: None,
                deleted_manifest_lists_count: None,
                deleted_statistics_files_count: None,
            })
        }

        fn rewrite_position_deletes_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls
                .push(MaintenanceActionKind::RewritePositionDeletes);
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: 1,
                added_delete_files_count: 1,
                rewritten_bytes_count: 1,
                added_bytes_count: 1,
            })
        }

        fn optimize_durably(
            &mut self,
            _target: MaintenanceTarget,
        ) -> Result<OptimizeSubmission, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Optimize);
            Ok(OptimizeSubmission::Submitted { job_id: 7 })
        }
    }

    #[test]
    fn policy_prefers_durable_optimize_and_suppresses_delete_rewrite() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let attempt = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit maintenance");
        assert!(
            attempt
                .evaluation()
                .actions
                .contains(&AutomaticMaintenanceAction::Optimize)
        );
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceSkipReason::SuppressedByOptimize,
        )));
        let report = coordinator.run_attempt(
            attempt,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW,
        );
        assert!(report.completed.contains(&MaintenanceActionKind::Optimize));
    }

    #[test]
    fn transient_failure_sets_backoff_without_direct_retry() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit first pass");
        let report = coordinator.run_attempt(
            first,
            &mut Runner {
                transient_expire: true,
                calls: Vec::new(),
            },
            NOW,
        );
        assert_eq!(
            report.failures,
            vec![(
                MaintenanceActionKind::Expire,
                MvBackgroundEngineErrorKind::TransientUnavailable,
            )]
        );

        let second = coordinator
            .try_begin(1, target("mv"), &facts(), NOW + 1)
            .expect("admit policy reevaluation after gate");
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::FailureBackoff,
        )));
        coordinator.cancel_attempt(second);
    }

    #[test]
    fn unchanged_snapshot_is_a_noop_after_first_completed_pass() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit first pass");
        coordinator.run_attempt(
            first,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW,
        );

        let mut current = facts();
        current.oldest_snapshot_timestamp_ms = Some(NOW);
        let second = coordinator
            .try_begin(1, target("mv"), &current, NOW + 1)
            .expect("admit second pass");
        assert!(second.evaluation().actions.is_empty());
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::SnapshotUnchanged,
        )));
        let report = coordinator.run_attempt(
            second,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW + 1,
        );
        assert!(report.is_noop());
    }

    #[test]
    fn absent_typed_facts_fall_back_to_frontend_defaults() {
        let policy = TablePolicy::resolve(&MaintenanceCoordinatorConfig::default(), &facts());
        assert!(policy.enabled);
        assert_eq!(policy.expire_max_age_ms, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS);
        assert_eq!(policy.expire_min_keep, DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP);
        assert_eq!(
            policy.target_file_size_bytes,
            DEFAULT_TARGET_FILE_SIZE_BYTES
        );
    }

    #[test]
    fn declared_typed_facts_override_frontend_defaults() {
        let mut current = facts();
        current.maintenance_enabled = Some(true);
        current.expire_max_snapshot_age_ms = Some(7_200_000);
        current.expire_min_snapshots_to_keep = Some(5);
        current.target_file_size_bytes = Some(64 * 1024 * 1024);
        let policy = TablePolicy::resolve(&MaintenanceCoordinatorConfig::default(), &current);
        assert!(policy.enabled);
        assert_eq!(policy.expire_max_age_ms, 7_200_000);
        assert_eq!(policy.expire_min_keep, 5);
        assert_eq!(policy.target_file_size_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn non_positive_typed_facts_are_clamped_to_one() {
        let mut zero = facts();
        zero.expire_max_snapshot_age_ms = Some(0);
        zero.expire_min_snapshots_to_keep = Some(0);
        zero.target_file_size_bytes = Some(0);
        let policy = TablePolicy::resolve(&MaintenanceCoordinatorConfig::default(), &zero);
        assert_eq!(policy.expire_max_age_ms, 1);
        assert_eq!(policy.expire_min_keep, 1);
        assert_eq!(policy.target_file_size_bytes, 1);

        let mut negative = facts();
        negative.expire_max_snapshot_age_ms = Some(-1);
        negative.target_file_size_bytes = Some(-1);
        let policy = TablePolicy::resolve(&MaintenanceCoordinatorConfig::default(), &negative);
        assert_eq!(policy.expire_max_age_ms, 1);
        assert_eq!(policy.target_file_size_bytes, 1);
    }

    #[test]
    fn disabled_typed_fact_skips_every_action() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.maintenance_enabled = Some(false);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().actions.is_empty());
        for kind in [
            MaintenanceActionKind::Expire,
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceActionKind::Optimize,
        ] {
            assert!(
                attempt
                    .evaluation()
                    .skips
                    .contains(&(kind, MaintenanceSkipReason::Disabled)),
                "missing Disabled skip for {kind:?}"
            );
        }
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn explicitly_enabled_typed_fact_evaluates_normally() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.maintenance_enabled = Some(true);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(
            attempt
                .evaluation()
                .actions
                .contains(&AutomaticMaintenanceAction::Optimize)
        );
        assert!(
            !attempt
                .evaluation()
                .skips
                .iter()
                .any(|(_, reason)| *reason == MaintenanceSkipReason::Disabled)
        );
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_min_snapshots_to_keep_blocks_expire() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.expire_min_snapshots_to_keep = Some(5);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::NothingToExpire,
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_target_file_size_drives_the_small_file_ratio() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        // The fixture's 1 MiB average file is "small" against the 512 MiB
        // default, but not against a 1-byte declared target.
        let mut current = facts();
        current.target_file_size_bytes = Some(1);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::BelowThreshold,
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_expire_max_age_keeps_recent_snapshots() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.oldest_snapshot_timestamp_ms = Some(NOW - 1_000);
        current.expire_max_snapshot_age_ms = Some(10_000);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::NothingToExpire,
        )));
        coordinator.cancel_attempt(attempt);

        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        current.expire_max_snapshot_age_ms = Some(100);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().actions.iter().any(|action| matches!(
            action,
            AutomaticMaintenanceAction::ExpireSnapshots { retain_last, .. } if *retain_last == 1
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn admitted_attempts_enforce_real_per_mv_capacity() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            max_concurrent: 1,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv_one"), &facts(), NOW)
            .expect("admit first MV");
        assert_eq!(coordinator.active_count(), 1);
        assert_eq!(
            coordinator
                .try_begin(2, target("mv_two"), &facts(), NOW)
                .expect_err("second MV must wait for capacity"),
            MaintenanceAdmission::AtCapacity
        );
        coordinator.cancel_attempt(first);
        assert_eq!(coordinator.active_count(), 0);
    }
}
