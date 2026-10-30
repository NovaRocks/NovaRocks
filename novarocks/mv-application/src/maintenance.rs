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

//! Provider-neutral facts and terminal classifications for MV maintenance.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Mutex,
};

use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, runtime::JobHandle,
};

/// Product policy configuration for process-local automatic MV maintenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceCoordinatorConfig {
    pub enabled: bool,
    pub tick_interval_ms: u64,
    pub max_concurrent: usize,
    pub compaction_min_data_files: i64,
    pub dv_min_delete_files: i64,
    pub action_cooldown_ms: i64,
    pub max_consecutive_failures: u32,
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
pub enum MaintenanceActionKind {
    Expire,
    RewritePositionDeletes,
    Optimize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutomaticMaintenanceAction {
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
    pub const fn kind(&self) -> MaintenanceActionKind {
        match self {
            Self::ExpireSnapshots { .. } => MaintenanceActionKind::Expire,
            Self::RewritePositionDeletes { .. } => MaintenanceActionKind::RewritePositionDeletes,
            Self::Optimize => MaintenanceActionKind::Optimize,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceSkipReason {
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
pub struct MaintenanceEvaluation {
    pub actions: Vec<AutomaticMaintenanceAction>,
    pub skips: Vec<(MaintenanceActionKind, MaintenanceSkipReason)>,
}

/// A policy pass admitted while the caller holds the matching MV activity
/// lease. The product owns the policy result; the host owns the lease itself.
#[derive(Clone, Debug)]
pub struct MaintenanceAttempt {
    pub mv_id: i64,
    pub target: MaintenanceTarget,
    pub evaluation: MaintenanceEvaluation,
    pub observed_snapshot_id: Option<i64>,
}

impl MaintenanceAttempt {
    pub fn target(&self) -> &MaintenanceTarget {
        &self.target
    }

    pub fn evaluation(&self) -> &MaintenanceEvaluation {
        &self.evaluation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceAdmission {
    Disabled,
    AtCapacity,
    AlreadyActive,
    Admitted,
}

/// Actual action classification and optional compilation cause; policy uses `kind` only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceActionFailure {
    pub action: MaintenanceActionKind,
    pub kind: MvBackgroundEngineErrorKind,
    pub compile_control: Option<novarocks_type_contract::CompileControlError>,
}

/// Result of a policy evaluation plus action evidence. A no-op is a completed
/// policy pass, not a request to retry absent actions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceExecutionReport {
    pub evaluation: MaintenanceEvaluation,
    pub completed: Vec<MaintenanceActionKind>,
    /// The exact OPTIMIZE job whose terminal success was observed, if any.
    pub optimize_finished_handle: Option<JobHandle>,
    pub already_active: Vec<MaintenanceActionKind>,
    pub failures: Vec<MaintenanceActionFailure>,
}

impl MaintenanceExecutionReport {
    pub fn is_noop(&self) -> bool {
        self.evaluation.actions.is_empty()
    }
}

/// A durable OPTIMIZE result after the host has waited on the exact job.
/// Submission alone cannot establish this outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizeDurableOutcome {
    Finished { handle: JobHandle },
    AlreadyActive,
}

/// The only automatic-maintenance effect boundary. Host adapters must invoke
/// existing durable table-maintenance routes, never a provider shortcut.
#[async_trait::async_trait]
pub trait AutomaticMaintenanceRunner: Send {
    async fn expire_snapshots_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError>;

    async fn rewrite_position_deletes_durably(
        &mut self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError>;

    async fn optimize_durably(
        &mut self,
        target: MaintenanceTarget,
    ) -> Result<OptimizeDurableOutcome, MvBackgroundEngineError>;
}

/// Product classification of a failed background capability invocation.
///
/// The Frontend adapter supplies this classification; product policy consumes
/// it without observing a provider, query, or Native transport type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvBackgroundEngineErrorKind {
    TargetGone,
    TransientUnavailable,
    InvalidDefinition,
    TerminalFailure,
    Corruption,
    InvariantViolation,
    ShutdownCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvBackgroundEngineError {
    kind: MvBackgroundEngineErrorKind,
    message: String,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
}

impl MvBackgroundEngineError {
    pub fn new(kind: MvBackgroundEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            compile_control: None,
            original_failure: None,
        }
    }

    pub fn with_original_failure(
        mut self,
        cause: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        self.original_failure = Some(cause);
        self
    }
    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original_failure.as_ref()
    }
    pub fn with_compile_control(
        mut self,
        control: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = control;
        self
    }
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    pub const fn kind(&self) -> MvBackgroundEngineErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvBackgroundEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvBackgroundEngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure
            .as_ref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

/// Facts needed to apply automatic-maintenance policy without a provider
/// metadata object or property-map fallback. Absent facts remain unknown.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvMaintenanceFacts {
    pub current_snapshot_id: Option<i64>,
    pub total_data_files: Option<i64>,
    pub max_compactable_data_files: Option<i64>,
    pub total_delete_files: Option<i64>,
    pub total_files_size_bytes: Option<i64>,
    pub oldest_snapshot_timestamp_ms: Option<i64>,
    pub snapshot_count: usize,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
}

const DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS: i64 = 432_000_000;
const DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: u32 = 1;
const DEFAULT_TARGET_FILE_SIZE_BYTES: i64 = 536_870_912;
const MIN_POSITION_DELETE_INPUT_FILES: usize = 2;
const SMALL_FILE_RATIO_NUMERATOR: i64 = 3;
const SMALL_FILE_RATIO_DENOMINATOR: i64 = 4;
const FAILURE_BACKOFF_BASE_MS: i64 = 60_000;
const FAILURE_BACKOFF_MAX_MS: i64 = 1_800_000;

#[derive(Clone, Debug, Default)]
struct TableRuntimeState {
    last_seen_snapshot_id: Option<i64>,
    last_action_ms: BTreeMap<MaintenanceActionKind, i64>,
    consecutive_failures: BTreeMap<MaintenanceActionKind, u32>,
    next_attempt_after_ms: BTreeMap<MaintenanceActionKind, i64>,
    circuit_broken: BTreeSet<MaintenanceActionKind>,
}

/// Product-owned, process-local state for automatic MV policy. It contains no
/// provider client, Frontend worker, or Native transport state; a restart
/// intentionally re-discovers provider facts and durable action state.
pub struct MaintenancePolicyState {
    config: MaintenanceCoordinatorConfig,
    runtime: BTreeMap<i64, TableRuntimeState>,
}

impl MaintenancePolicyState {
    pub fn new(config: MaintenanceCoordinatorConfig) -> Self {
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
        }
    }

    pub fn config(&self) -> &MaintenanceCoordinatorConfig {
        &self.config
    }

    pub fn evaluate(
        &mut self,
        mv_id: i64,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> MaintenanceEvaluation {
        let policy = TablePolicy::resolve(&self.config, facts);
        let state = self.runtime_entry(mv_id).clone();
        evaluate_facts(facts, &policy, &state, &self.config, now_ms)
    }

    /// Apply the durable runner's action report. The host owns the matching
    /// activity lease and concurrency permit; this product state owns only
    /// policy observation, cooldown, backoff, and circuit-breaking facts.
    pub fn finish(
        &mut self,
        attempt: &MaintenanceAttempt,
        report: &MaintenanceExecutionReport,
        now_ms: i64,
    ) {
        for kind in &report.completed {
            self.record_success(attempt.mv_id, *kind, now_ms);
        }
        for failure in &report.failures {
            self.record_failure(attempt.mv_id, failure.action, failure.kind, now_ms);
        }
        // A failed, cancelled, or already-active pass has not consumed the
        // observed snapshot: later actions must remain eligible on reentry.
        if report.completed.len() == attempt.evaluation.actions.len()
            && report.already_active.is_empty()
            && report.failures.is_empty()
        {
            self.runtime_entry(attempt.mv_id).last_seen_snapshot_id = attempt.observed_snapshot_id;
        }
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
            MvBackgroundEngineErrorKind::ShutdownCancelled => {}
            MvBackgroundEngineErrorKind::TargetGone
            | MvBackgroundEngineErrorKind::TerminalFailure
            | MvBackgroundEngineErrorKind::InvalidDefinition
            | MvBackgroundEngineErrorKind::Corruption
            | MvBackgroundEngineErrorKind::InvariantViolation => {
                state.circuit_broken.insert(kind);
            }
        }
    }
}

/// Product-owned coordinator for process-local automatic MV maintenance.
/// Hosts acquire the per-target activity lease before admission and execute
/// the returned attempt through their own durable runner adapter.
pub struct MaintenanceCoordinator {
    policy: MaintenancePolicyState,
    active: BTreeSet<i64>,
}

/// The sole process-local owner of automatic-maintenance policy state.
///
/// Repository and provider adapters supply facts and execute the durable
/// action, but they do not own a second coordinator mutex, cooldown ledger or
/// active set.  The explicit begin/finish pair preserves the admitted attempt
/// across the provider call, including its exact terminal report.
pub struct MvMaintenanceRuntime {
    coordinator: Mutex<MaintenanceCoordinator>,
}

impl MvMaintenanceRuntime {
    pub fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self {
            coordinator: Mutex::new(MaintenanceCoordinator::new(config)),
        }
    }

    pub fn config(&self) -> MaintenanceCoordinatorConfig {
        self.lock().config().clone()
    }

    pub fn try_begin(
        &self,
        mv_id: i64,
        target: MaintenanceTarget,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> Result<MaintenanceAttempt, MaintenanceAdmission> {
        self.lock().try_begin(mv_id, target, facts, now_ms)
    }

    pub fn finish(
        &self,
        attempt: MaintenanceAttempt,
        report: &MaintenanceExecutionReport,
        now_ms: i64,
    ) {
        self.lock().finish_attempt(attempt, report, now_ms);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MaintenanceCoordinator> {
        self.coordinator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl MaintenanceCoordinator {
    pub fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self {
            policy: MaintenancePolicyState::new(config),
            active: BTreeSet::new(),
        }
    }

    pub fn config(&self) -> &MaintenanceCoordinatorConfig {
        self.policy.config()
    }

    /// Admit work only after the host has acquired the matching activity
    /// lease, so a FIFO gate waiter never consumes a maintenance permit.
    pub fn try_begin(
        &mut self,
        mv_id: i64,
        target: MaintenanceTarget,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> Result<MaintenanceAttempt, MaintenanceAdmission> {
        if !self.policy.config().enabled {
            return Err(MaintenanceAdmission::Disabled);
        }
        if self.active.contains(&mv_id) {
            return Err(MaintenanceAdmission::AlreadyActive);
        }
        if self.active.len() >= self.policy.config().max_concurrent {
            return Err(MaintenanceAdmission::AtCapacity);
        }
        let evaluation = self.policy.evaluate(mv_id, facts, now_ms);
        self.active.insert(mv_id);
        Ok(MaintenanceAttempt {
            mv_id,
            target,
            observed_snapshot_id: facts.current_snapshot_id,
            evaluation,
        })
    }

    /// Run product actions without holding a host coordinator lock. The host
    /// must settle the result with [`Self::finish_attempt`] exactly once.
    /// Later actions are never dispatched after a nonterminal or failed one.
    pub async fn execute_attempt(
        attempt: &MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
    ) -> MaintenanceExecutionReport {
        let mut report = MaintenanceExecutionReport {
            evaluation: attempt.evaluation.clone(),
            completed: Vec::new(),
            optimize_finished_handle: None,
            already_active: Vec::new(),
            failures: Vec::new(),
        };
        for action in &attempt.evaluation.actions {
            let kind = action.kind();
            let result = match action {
                AutomaticMaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                } => {
                    runner
                        .expire_snapshots_durably(MaintenanceActionRequest::ExpireSnapshots {
                            target: attempt.target.clone(),
                            older_than_ms: Some(*older_than_ms),
                            retain_last: Some(*retain_last),
                        })
                        .await
                }
                AutomaticMaintenanceAction::RewritePositionDeletes { min_input_files } => {
                    let mut options = BTreeMap::new();
                    options.insert("min-input-files".to_string(), min_input_files.to_string());
                    runner
                        .rewrite_position_deletes_durably(
                            MaintenanceActionRequest::RewritePositionDeleteFiles {
                                target: attempt.target.clone(),
                                options,
                                where_clause: None,
                            },
                        )
                        .await
                }
                AutomaticMaintenanceAction::Optimize => {
                    match runner.optimize_durably(attempt.target.clone()).await {
                        Ok(OptimizeDurableOutcome::Finished { handle }) => {
                            report.optimize_finished_handle = Some(handle);
                            report.completed.push(kind);
                            continue;
                        }
                        Ok(OptimizeDurableOutcome::AlreadyActive) => {
                            report.already_active.push(kind);
                            break;
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            match result {
                Ok(outcome) if expected_outcome(kind, &outcome) => report.completed.push(kind),
                Ok(outcome) => {
                    tracing::error!(action = ?kind, ?outcome, "automatic maintenance returned an incompatible durable outcome");
                    report.failures.push(MaintenanceActionFailure {
                        action: kind,
                        kind: MvBackgroundEngineErrorKind::InvariantViolation,
                        compile_control: None,
                    });
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        mv_id = attempt.mv_id,
                        action = ?kind,
                        error_kind = ?error.kind(),
                        error = %error,
                        "automatic MV maintenance action failed"
                    );
                    report.failures.push(MaintenanceActionFailure {
                        action: kind,
                        kind: error.kind(),
                        compile_control: error.compile_control_error(),
                    });
                    break;
                }
            }
        }
        report
    }

    pub fn finish_attempt(
        &mut self,
        attempt: MaintenanceAttempt,
        report: &MaintenanceExecutionReport,
        now_ms: i64,
    ) {
        self.policy.finish(&attempt, report, now_ms);
        self.active.remove(&attempt.mv_id);
    }

    pub fn cancel_attempt(&mut self, attempt: MaintenanceAttempt) {
        self.active.remove(&attempt.mv_id);
    }

    #[doc(hidden)]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
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

fn failure_backoff_ms(attempt: u32) -> i64 {
    let shift = attempt.max(1).saturating_sub(1).min(62);
    FAILURE_BACKOFF_BASE_MS
        .saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(FAILURE_BACKOFF_MAX_MS)
}

#[cfg(test)]
mod tests {
    use super::{
        AutomaticMaintenanceAction, AutomaticMaintenanceRunner, DEFAULT_EXPIRE_MAX_SNAPSHOT_AGE_MS,
        DEFAULT_EXPIRE_MIN_SNAPSHOTS_TO_KEEP, DEFAULT_TARGET_FILE_SIZE_BYTES,
        MaintenanceActionKind, MaintenanceAdmission, MaintenanceCoordinator,
        MaintenanceCoordinatorConfig, MaintenanceExecutionReport, MaintenanceSkipReason,
        MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
        OptimizeDurableOutcome, TablePolicy,
    };
    use novarocks_table_maintenance::{
        MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, runtime::JobHandle,
    };

    fn facts() -> MvMaintenanceFacts {
        MvMaintenanceFacts {
            current_snapshot_id: Some(3),
            total_data_files: Some(200),
            total_files_size_bytes: Some(200 * 1024 * 1024),
            oldest_snapshot_timestamp_ms: Some(1_000),
            snapshot_count: 3,
            ..MvMaintenanceFacts::default()
        }
    }

    #[test]
    fn typed_error_keeps_retry_policy_out_of_display_text() {
        let error = MvBackgroundEngineError::new(
            MvBackgroundEngineErrorKind::TransientUnavailable,
            "connector lease is temporarily unavailable",
        );
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
        assert_eq!(
            error.to_string(),
            "connector lease is temporarily unavailable"
        );
    }

    #[test]
    fn absent_typed_facts_use_product_defaults() {
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
    fn declared_typed_facts_override_product_defaults() {
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
    fn coordinator_keeps_admission_and_active_permits_in_product() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            max_concurrent: 1,
            ..MaintenanceCoordinatorConfig::default()
        });
        let target = |table: &str| MaintenanceTarget {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        };
        let first = coordinator
            .try_begin(1, target("first"), &facts(), 1_000_000_000)
            .expect("first target must be admitted");
        assert_eq!(coordinator.active_count(), 1);
        assert_eq!(
            coordinator
                .try_begin(2, target("second"), &facts(), 1_000_000_000)
                .expect_err("second target must wait for product capacity"),
            MaintenanceAdmission::AtCapacity
        );
        coordinator.cancel_attempt(first);
        assert_eq!(coordinator.active_count(), 0);
    }

    const NOW: i64 = 1_000_000_000;

    fn target(name: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: name.to_string(),
        }
    }

    fn policy_facts() -> MvMaintenanceFacts {
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
        expire_error: Option<MvBackgroundEngineErrorKind>,
        compile_control: Option<novarocks_type_contract::CompileControlError>,
        optimize_outcome: OptimizeDurableOutcome,
        calls: Vec<MaintenanceActionKind>,
    }

    impl Default for Runner {
        fn default() -> Self {
            Self {
                expire_error: None,
                compile_control: None,
                optimize_outcome: OptimizeDurableOutcome::Finished {
                    handle: JobHandle::new(7),
                },
                calls: Vec::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl AutomaticMaintenanceRunner for Runner {
        async fn expire_snapshots_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Expire);
            if let Some(kind) = self.expire_error {
                return Err(MvBackgroundEngineError::new(
                    kind,
                    "maintenance operation did not complete",
                )
                .with_compile_control(self.compile_control.clone()));
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

        async fn rewrite_position_deletes_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls
                .push(MaintenanceActionKind::RewritePositionDeletes);
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: 1,
                added_delete_files_count: Some(1),
                rewritten_bytes_count: 1,
                added_bytes_count: 1,
            })
        }

        async fn optimize_durably(
            &mut self,
            _target: MaintenanceTarget,
        ) -> Result<OptimizeDurableOutcome, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Optimize);
            Ok(self.optimize_outcome)
        }
    }

    async fn run_attempt(
        coordinator: &mut MaintenanceCoordinator,
        attempt: super::MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
        now_ms: i64,
    ) -> MaintenanceExecutionReport {
        let report = MaintenanceCoordinator::execute_attempt(&attempt, runner).await;
        coordinator.finish_attempt(attempt, &report, now_ms);
        report
    }

    #[tokio::test]
    async fn policy_prefers_durable_optimize_and_suppresses_delete_rewrite() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let attempt = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW)
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
        let report = run_attempt(&mut coordinator, attempt, &mut Runner::default(), NOW).await;
        assert_eq!(
            report.completed,
            vec![
                MaintenanceActionKind::Expire,
                MaintenanceActionKind::Optimize
            ]
        );
        assert_eq!(report.optimize_finished_handle, Some(JobHandle::new(7)));
        let next = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
            .expect("terminal success must release admission");
        assert!(next.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::SnapshotUnchanged,
        )));
        coordinator.cancel_attempt(next);

        let mut advanced = policy_facts();
        advanced.current_snapshot_id = Some(4);
        let next = coordinator
            .try_begin(1, target("mv"), &advanced, NOW + 1)
            .expect("new snapshot may be evaluated");
        assert!(next.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::Cooldown,
        )));
        coordinator.cancel_attempt(next);
    }

    #[tokio::test]
    async fn failed_or_cancelled_action_stops_the_pass_without_consuming_snapshot() {
        for error in [
            MvBackgroundEngineErrorKind::TransientUnavailable,
            MvBackgroundEngineErrorKind::ShutdownCancelled,
        ] {
            let mut coordinator =
                MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
            let attempt = coordinator
                .try_begin(1, target("mv"), &policy_facts(), NOW)
                .expect("admit maintenance");
            assert_eq!(attempt.evaluation().actions.len(), 2);
            let mut runner = Runner {
                expire_error: Some(error),
                ..Runner::default()
            };
            let report = run_attempt(&mut coordinator, attempt, &mut runner, NOW).await;
            assert_eq!(runner.calls, vec![MaintenanceActionKind::Expire]);
            assert_eq!(
                report.failures,
                vec![super::MaintenanceActionFailure {
                    action: MaintenanceActionKind::Expire,
                    kind: error,
                    compile_control: None
                }]
            );
            assert!(report.completed.is_empty());
            assert!(report.optimize_finished_handle.is_none());

            let next = coordinator
                .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
                .expect("failed pass must allow new admission");
            assert!(
                next.evaluation()
                    .actions
                    .contains(&AutomaticMaintenanceAction::Optimize)
            );
            coordinator.cancel_attempt(next);
        }
    }

    #[tokio::test]
    async fn already_active_optimize_does_not_start_cooldown_or_consume_snapshot() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let attempt = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW)
            .expect("admit maintenance");
        let mut runner = Runner {
            optimize_outcome: OptimizeDurableOutcome::AlreadyActive,
            ..Runner::default()
        };
        let report = run_attempt(&mut coordinator, attempt, &mut runner, NOW).await;
        assert_eq!(
            runner.calls,
            vec![
                MaintenanceActionKind::Expire,
                MaintenanceActionKind::Optimize
            ]
        );
        assert_eq!(report.completed, vec![MaintenanceActionKind::Expire]);
        assert_eq!(report.already_active, vec![MaintenanceActionKind::Optimize]);

        let next = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
            .expect("already-active pass must allow new admission");
        assert!(
            next.evaluation()
                .actions
                .contains(&AutomaticMaintenanceAction::Optimize)
        );
        coordinator.cancel_attempt(next);
    }

    #[tokio::test]
    async fn transient_failure_sets_backoff_without_direct_retry() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW)
            .expect("admit first pass");
        let report = run_attempt(
            &mut coordinator,
            first,
            &mut Runner {
                expire_error: Some(MvBackgroundEngineErrorKind::TransientUnavailable),
                ..Runner::default()
            },
            NOW,
        )
        .await;
        assert_eq!(
            report.failures,
            vec![super::MaintenanceActionFailure {
                action: MaintenanceActionKind::Expire,
                kind: MvBackgroundEngineErrorKind::TransientUnavailable,
                compile_control: None
            }]
        );

        let second = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
            .expect("admit policy reevaluation after gate");
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::FailureBackoff,
        )));
        coordinator.cancel_attempt(second);
    }

    #[tokio::test]
    async fn unchanged_snapshot_is_a_noop_after_first_completed_pass() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &policy_facts(), NOW)
            .expect("admit first pass");
        run_attempt(&mut coordinator, first, &mut Runner::default(), NOW).await;

        let mut current = policy_facts();
        current.oldest_snapshot_timestamp_ms = Some(NOW);
        let second = coordinator
            .try_begin(1, target("mv"), &current, NOW + 1)
            .expect("admit second pass");
        assert!(second.evaluation().actions.is_empty());
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::SnapshotUnchanged,
        )));
        let report = run_attempt(&mut coordinator, second, &mut Runner::default(), NOW + 1).await;
        assert!(report.is_noop());
    }

    #[test]
    fn disabled_typed_fact_skips_every_action() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = policy_facts();
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
        let mut current = policy_facts();
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
        let mut current = policy_facts();
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
        let mut current = policy_facts();
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
        let mut current = policy_facts();
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
            .try_begin(1, target("mv_one"), &policy_facts(), NOW)
            .expect("admit first MV");
        assert_eq!(coordinator.active_count(), 1);
        assert_eq!(
            coordinator
                .try_begin(2, target("mv_two"), &policy_facts(), NOW)
                .expect_err("second MV must wait for capacity"),
            MaintenanceAdmission::AtCapacity
        );
        coordinator.cancel_attempt(first);
        assert_eq!(coordinator.active_count(), 0);
    }
    #[tokio::test]
    async fn actual_maintenance_report_keeps_compile_cause_without_changing_policy() {
        use novarocks_type_contract::CompileControlError as C;
        for control in [C::Cancelled, C::DeadlineExceeded, C::ResourceExhausted] {
            for kind in [
                MvBackgroundEngineErrorKind::TransientUnavailable,
                MvBackgroundEngineErrorKind::ShutdownCancelled,
                MvBackgroundEngineErrorKind::TerminalFailure,
            ] {
                let mut baseline =
                    MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
                let mut typed =
                    MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
                let baseline_attempt = baseline
                    .try_begin(1, target("mv"), &policy_facts(), NOW)
                    .unwrap();
                let typed_attempt = typed
                    .try_begin(1, target("mv"), &policy_facts(), NOW)
                    .unwrap();
                let baseline_report = run_attempt(
                    &mut baseline,
                    baseline_attempt,
                    &mut Runner {
                        expire_error: Some(kind),
                        ..Runner::default()
                    },
                    NOW,
                )
                .await;
                let mut runner = Runner {
                    expire_error: Some(kind),
                    compile_control: Some(control.clone()),
                    ..Runner::default()
                };
                let report = run_attempt(&mut typed, typed_attempt, &mut runner, NOW).await;
                assert_eq!(runner.calls, [MaintenanceActionKind::Expire]);
                assert_eq!(report.failures.len(), 1);
                assert_eq!(report.failures[0].kind, baseline_report.failures[0].kind);
                assert_eq!(report.failures[0].compile_control, Some(control.clone()));
                assert_eq!(baseline_report.failures[0].compile_control, None);
                let baseline_next = baseline
                    .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
                    .unwrap();
                let typed_next = typed
                    .try_begin(1, target("mv"), &policy_facts(), NOW + 1)
                    .unwrap();
                assert_eq!(typed_next.evaluation(), baseline_next.evaluation());
            }
        }
    }
}
