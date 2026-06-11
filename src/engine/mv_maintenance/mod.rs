//! Automatic maintenance for NovaRocks-owned Iceberg MV storage tables
//! (IV3-11): EXPIRE SNAPSHOTS / OPTIMIZE / DV compaction, driven by a
//! background coordinator. See
//! docs/design/specs/2026-06-10-iceberg-mv-maintenance-scheduler-design.md.

pub(crate) mod policy;
pub(crate) mod stats;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use self::policy::{
    ActionKind, EvaluationOutcome, MaintenanceAction, MaintenancePolicyConfig,
    TableMaintenanceStats, TablePolicy, TableRuntimeState, evaluate_table, failure_backoff_ms,
};

use crate::engine::StandaloneState;

/// Signals consumed by the coordinator thread. `Wake` is sent after every
/// successful MV refresh; `Stop` is sent by the handle on drop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceSignal {
    Wake,
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceTarget {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

impl MaintenanceTarget {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OptimizeSubmission {
    Submitted { job_id: i64 },
    AlreadyActive,
}

/// Side-effect boundary: real impl talks to iceberg catalogs and the job
/// queue; tests inject a recording fake (same pattern as RefreshExecutor in
/// mv_scheduler.rs).
pub(crate) trait MaintenanceExecutor {
    fn expire_snapshots(
        &mut self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
        retain_last: u32,
    ) -> Result<crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome, String>;

    fn rewrite_position_deletes(
        &mut self,
        target: &MaintenanceTarget,
        min_input_files: usize,
    ) -> Result<
        crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome,
        String,
    >;

    fn submit_optimize(&mut self, target: &MaintenanceTarget)
    -> Result<OptimizeSubmission, String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaintenanceCoordinatorConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent: usize,
    pub(crate) policy: MaintenancePolicyConfig,
}

impl MaintenanceCoordinatorConfig {
    pub(crate) fn from_standalone_config(
        config: &crate::common::app_config::StandaloneServerConfig,
    ) -> Self {
        Self {
            enabled: config.iceberg_maintenance_enabled,
            tick_interval_ms: config.iceberg_maintenance_tick_interval_ms.max(1),
            max_concurrent: config.iceberg_maintenance_max_concurrent.max(1),
            policy: MaintenancePolicyConfig {
                compaction_min_data_files: config.iceberg_maintenance_compaction_min_data_files,
                dv_min_delete_files: config.iceberg_maintenance_dv_min_delete_files,
                action_cooldown_ms: config.iceberg_maintenance_action_cooldown_ms,
                max_consecutive_failures: config.iceberg_maintenance_max_consecutive_failures,
            },
        }
    }
}

pub(crate) struct MaintenanceCoordinator {
    config: MaintenanceCoordinatorConfig,
    runtime: BTreeMap<i64, TableRuntimeState>,
}

impl MaintenanceCoordinator {
    pub(crate) fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self {
            config,
            runtime: BTreeMap::new(),
        }
    }

    fn runtime_entry(&mut self, mv_id: i64) -> &mut TableRuntimeState {
        self.runtime.entry(mv_id).or_default()
    }

    fn record_success(&mut self, mv_id: i64, kind: ActionKind) {
        let entry = self.runtime_entry(mv_id);
        entry.consecutive_failures.remove(&kind);
        entry.next_attempt_after_ms.remove(&kind);
    }

    fn record_failure(&mut self, mv_id: i64, kind: ActionKind, now_ms: i64, max_failures: u32) {
        let entry = self.runtime_entry(mv_id);
        let attempts = entry.consecutive_failures.entry(kind).or_insert(0);
        *attempts = attempts.saturating_add(1);
        if *attempts >= max_failures {
            entry.circuit_broken.insert(kind);
        } else {
            entry
                .next_attempt_after_ms
                .insert(kind, now_ms.saturating_add(failure_backoff_ms(*attempts)));
        }
    }

    /// Evaluate one table and run the planned actions through `executor`.
    /// Returns the evaluation outcome for logging/testing.
    pub(crate) fn process_table(
        &mut self,
        mv_id: i64,
        target: &MaintenanceTarget,
        stats: &TableMaintenanceStats,
        executor: &mut dyn MaintenanceExecutor,
        now_ms: i64,
    ) -> EvaluationOutcome {
        // Clone the global policy config up front: `runtime_entry` borrows all
        // of `self` mutably, so we cannot also hold `&self.config.policy` across
        // the evaluate_table call.
        let global = self.config.policy.clone();
        let policy = TablePolicy::resolve(&global, &stats.properties);
        let outcome = {
            let runtime = self.runtime_entry(mv_id);
            evaluate_table(stats, &policy, runtime, &global, now_ms)
        };
        let max_failures = global.max_consecutive_failures;
        for action in &outcome.actions {
            let kind = action.kind();
            self.runtime_entry(mv_id)
                .last_action_ms
                .insert(kind, now_ms);
            let result: Result<String, String> = match action {
                MaintenanceAction::ExpireSnapshots {
                    older_than_ms,
                    retain_last,
                } => executor
                    .expire_snapshots(target, *older_than_ms, *retain_last)
                    .map(|o| {
                        format!(
                            "expired_snapshots={} deleted_files={}",
                            o.expired_snapshot_count, o.deleted_file_count
                        )
                    }),
                MaintenanceAction::RewritePositionDeletes { min_input_files } => executor
                    .rewrite_position_deletes(target, *min_input_files)
                    .map(|o| {
                        format!(
                            "rewritten_delete_files={} added_delete_files={}",
                            o.rewritten_delete_files_count, o.added_delete_files_count
                        )
                    }),
                MaintenanceAction::SubmitOptimize => {
                    executor.submit_optimize(target).map(|s| match s {
                        OptimizeSubmission::Submitted { job_id } => {
                            format!("optimize_job_id={job_id}")
                        }
                        OptimizeSubmission::AlreadyActive => {
                            "optimize_job=already-active".to_string()
                        }
                    })
                }
            };
            match result {
                Ok(detail) => {
                    self.record_success(mv_id, kind);
                    tracing::info!(
                        table = %target.fqn(),
                        action = ?kind,
                        data_files = ?stats.total_data_files,
                        files_size = ?stats.total_files_size_bytes,
                        delete_files = ?stats.total_delete_files,
                        %detail,
                        "auto maintenance action completed"
                    );
                }
                Err(err) => {
                    self.record_failure(mv_id, kind, now_ms, max_failures);
                    tracing::warn!(
                        table = %target.fqn(),
                        action = ?kind,
                        error = %err,
                        "auto maintenance action failed"
                    );
                }
            }
        }
        for (kind, reason) in &outcome.skips {
            tracing::debug!(
                table = %target.fqn(),
                action = ?kind,
                reason = ?reason,
                "auto maintenance action skipped"
            );
        }
        self.runtime_entry(mv_id).last_seen_snapshot_id = stats.current_snapshot_id;
        outcome
    }
}

/// Production executor: expire / DV-rewrite run inline via block_on_iceberg;
/// optimize is submitted to the existing SQLite job queue and executed by the
/// iceberg-optimize-worker.
pub(crate) struct StateMaintenanceExecutor {
    state: Arc<StandaloneState>,
}

impl StateMaintenanceExecutor {
    pub(crate) fn new(state: Arc<StandaloneState>) -> Self {
        Self { state }
    }
}

impl MaintenanceExecutor for StateMaintenanceExecutor {
    fn expire_snapshots(
        &mut self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
        retain_last: u32,
    ) -> Result<crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome, String> {
        let (catalog, table_ident, _) =
            crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
                &self.state,
                &target.catalog,
                &target.namespace,
                &target.table,
            )?;
        let params = crate::connector::iceberg::commit::expire_snapshots::ExpireParams {
            older_than_ms: Some(older_than_ms),
            retain_last: Some(retain_last),
        };
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            crate::connector::iceberg::commit::expire_snapshots::run_expire_snapshots(
                catalog,
                table_ident,
                params,
            )
            .await
        })?
    }

    fn rewrite_position_deletes(
        &mut self,
        target: &MaintenanceTarget,
        min_input_files: usize,
    ) -> Result<
        crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome,
        String,
    >{
        let (catalog, table_ident, _) =
            crate::engine::iceberg_maintenance::resolve_maintenance_catalog(
                &self.state,
                &target.catalog,
                &target.namespace,
                &target.table,
            )?;
        let options =
            crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOptions {
                rewrite_all: false,
                min_input_files,
            };
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            crate::connector::iceberg::commit::rewrite_position_delete_files::run_rewrite_position_delete_files(
                catalog,
                table_ident,
                options,
            )
            .await
        })?
    }

    fn submit_optimize(
        &mut self,
        target: &MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        match crate::engine::iceberg_maintenance::enqueue_optimize_job(
            &self.state,
            &target.catalog,
            &target.namespace,
            &target.table,
        ) {
            Ok(job_id) => Ok(OptimizeSubmission::Submitted { job_id }),
            // The job repo rejects duplicates with a conflict error; treat it
            // as "already active" instead of a failure.
            Err(err) if err.contains("already exists") => Ok(OptimizeSubmission::AlreadyActive),
            Err(err) => Err(err),
        }
    }
}

struct MaintenanceCandidate {
    mv_id: i64,
    target: MaintenanceTarget,
    refresh_in_flight: bool,
}

fn load_candidates(
    state: &Arc<StandaloneState>,
) -> Result<
    (
        Vec<crate::meta::repository::mv::StoredMvDefinition>,
        Vec<MaintenanceCandidate>,
    ),
    String,
> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open mv maintenance read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("list mv definitions for maintenance failed: {e}"))?;
    let candidates = definitions
        .iter()
        .filter(|d| d.storage_engine.eq_ignore_ascii_case("iceberg"))
        .filter_map(|d| {
            let (Some(catalog), Some(namespace), Some(table)) = (
                d.target_catalog.as_ref(),
                d.target_namespace.as_ref(),
                d.target_table.as_ref(),
            ) else {
                return None;
            };
            Some(MaintenanceCandidate {
                mv_id: d.mv_id,
                target: MaintenanceTarget {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                },
                refresh_in_flight: d.refresh_in_progress || d.active_refresh_id.is_some(),
            })
        })
        .collect();
    Ok((definitions, candidates))
}

impl MaintenanceCoordinator {
    /// One full evaluation pass over all Iceberg-backed MV storage tables.
    /// Deterministic given (state contents, now_ms); the integration tests
    /// call this directly instead of going through the thread.
    pub(crate) fn run_pass(
        &mut self,
        state: &Arc<StandaloneState>,
        executor: &mut dyn MaintenanceExecutor,
        now_ms: i64,
    ) -> Result<(), String> {
        let (definitions, candidates) = load_candidates(state)?;
        let mut executed_tables = 0usize;
        for candidate in &candidates {
            if candidate.refresh_in_flight {
                tracing::debug!(
                    table = %candidate.target.fqn(),
                    "auto maintenance skipped: refresh in flight"
                );
                continue;
            }
            // Stop once enough tables have acted this pass. Checked before
            // loading metadata so deferred tables cost no IO; the table is left
            // un-observed, so the next pass re-evaluates it from scratch.
            if executed_tables >= self.config.max_concurrent {
                continue;
            }
            let stats = match stats::collect_table_stats(
                state,
                &candidate.target.catalog,
                &candidate.target.namespace,
                &candidate.target.table,
                &definitions,
            ) {
                Ok(stats) => stats,
                Err(err) => {
                    tracing::warn!(
                        table = %candidate.target.fqn(),
                        error = %err,
                        "auto maintenance stats collection failed"
                    );
                    continue;
                }
            };
            let outcome =
                self.process_table(candidate.mv_id, &candidate.target, &stats, executor, now_ms);
            // `executed_tables` counts tables that actually performed an action;
            // no-op evaluations (cooldown, snapshot unchanged) do not consume
            // the concurrency budget.
            if !outcome.actions.is_empty() {
                executed_tables += 1;
            }
        }
        Ok(())
    }
}

pub(crate) struct MaintenanceCoordinatorHandle {
    enabled: bool,
    signal_tx: Option<Sender<MaintenanceSignal>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl MaintenanceCoordinatorHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            signal_tx: None,
            worker: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Drop for MaintenanceCoordinatorHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.signal_tx.take() {
            let _ = tx.send(MaintenanceSignal::Stop);
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

/// Notify the coordinator that an MV refresh committed. Cheap no-op when the
/// coordinator is not running (tests, disabled config).
pub(crate) fn notify_refresh_completed(state: &Arc<StandaloneState>) {
    if let Ok(guard) = state.maintenance_signal_tx.lock()
        && let Some(tx) = guard.as_ref()
    {
        let _ = tx.send(MaintenanceSignal::Wake);
    }
}

pub(crate) fn start_maintenance_coordinator_for_server(
    engine: &crate::engine::StandaloneNovaRocks,
    config: MaintenanceCoordinatorConfig,
) -> MaintenanceCoordinatorHandle {
    if !config.enabled {
        return MaintenanceCoordinatorHandle::disabled();
    }
    let state = Arc::clone(&engine.inner);
    let (signal_tx, signal_rx) = mpsc::channel();
    if let Ok(mut guard) = state.maintenance_signal_tx.lock() {
        *guard = Some(signal_tx.clone());
    }
    let worker_config = config.clone();
    let worker_state = Arc::clone(&state);
    let worker = thread::Builder::new()
        .name("novarocks-iceberg-maintenance".to_string())
        .spawn(move || {
            let mut coordinator = MaintenanceCoordinator::new(worker_config.clone());
            let mut executor = StateMaintenanceExecutor::new(Arc::clone(&worker_state));
            loop {
                if let Err(err) =
                    coordinator.run_pass(&worker_state, &mut executor, current_time_ms())
                {
                    tracing::warn!(error = %err, "iceberg maintenance pass failed");
                }
                match signal_rx.recv_timeout(Duration::from_millis(worker_config.tick_interval_ms))
                {
                    Ok(MaintenanceSignal::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                    Ok(MaintenanceSignal::Wake) => {
                        // Coalesce bursts of refresh completions into one pass.
                        let mut stop = false;
                        while let Ok(signal) = signal_rx.try_recv() {
                            if signal == MaintenanceSignal::Stop {
                                stop = true;
                                break;
                            }
                        }
                        if stop {
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        });
    match worker {
        Ok(worker) => MaintenanceCoordinatorHandle {
            enabled: true,
            signal_tx: Some(signal_tx),
            worker: Some(worker),
        },
        Err(err) => {
            tracing::warn!(error = %err, "failed to start iceberg maintenance worker");
            MaintenanceCoordinatorHandle::disabled()
        }
    }
}

#[cfg(test)]
mod integration_tests;

#[cfg(test)]
mod tests {
    use super::policy::*;
    use super::*;
    use crate::connector::iceberg::commit::expire_snapshots::ExpireOutcome;
    use crate::connector::iceberg::commit::rewrite_position_delete_files::RewritePositionDeleteOutcome;

    #[derive(Default)]
    struct RecordingExecutor {
        expires: Vec<(String, i64, u32)>,
        dv_rewrites: Vec<(String, usize)>,
        optimize_submissions: Vec<String>,
        fail_expire: Option<String>,
    }

    impl MaintenanceExecutor for RecordingExecutor {
        fn expire_snapshots(
            &mut self,
            target: &MaintenanceTarget,
            older_than_ms: i64,
            retain_last: u32,
        ) -> Result<ExpireOutcome, String> {
            self.expires
                .push((target.fqn(), older_than_ms, retain_last));
            match self.fail_expire.as_ref() {
                Some(message) => Err(message.clone()),
                None => Ok(ExpireOutcome {
                    expired_snapshot_count: 1,
                    deleted_file_count: 2,
                }),
            }
        }

        fn rewrite_position_deletes(
            &mut self,
            target: &MaintenanceTarget,
            min_input_files: usize,
        ) -> Result<RewritePositionDeleteOutcome, String> {
            self.dv_rewrites.push((target.fqn(), min_input_files));
            Ok(RewritePositionDeleteOutcome::default())
        }

        fn submit_optimize(
            &mut self,
            target: &MaintenanceTarget,
        ) -> Result<OptimizeSubmission, String> {
            self.optimize_submissions.push(target.fqn());
            Ok(OptimizeSubmission::Submitted { job_id: 7 })
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_x".to_string(),
        }
    }

    fn coordinator() -> MaintenanceCoordinator {
        MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            enabled: true,
            tick_interval_ms: 600_000,
            max_concurrent: 1,
            policy: MaintenancePolicyConfig::default(),
        })
    }

    fn old_small_file_stats() -> TableMaintenanceStats {
        TableMaintenanceStats {
            current_snapshot_id: Some(30),
            snapshots: vec![
                SnapshotInfo {
                    snapshot_id: 10,
                    timestamp_ms: 1_000,
                },
                SnapshotInfo {
                    snapshot_id: 30,
                    timestamp_ms: 3_000,
                },
            ],
            total_data_files: Some(200),
            total_files_size_bytes: Some(200 * 1024 * 1024),
            total_delete_files: Some(0),
            properties: std::collections::HashMap::new(),
            non_main_ref_count: 0,
            downstream_floor_ts_ms: None,
            downstream_floor_unknown: false,
        }
    }

    const NOW: i64 = 1_000_000_000;

    #[test]
    fn process_table_runs_planned_actions_and_records_snapshot() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor::default();
        let outcome =
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, NOW);
        assert_eq!(executor.expires.len(), 1);
        assert_eq!(executor.optimize_submissions.len(), 1);
        assert!(executor.dv_rewrites.is_empty()); // suppressed by optimize
        assert_eq!(outcome.actions.len(), 2);
        // Second pass with identical stats: snapshot unchanged -> no compaction,
        // expire still planned (old snapshot remains in fake-world stats).
        let outcome2 = coordinator.process_table(
            1,
            &target(),
            &old_small_file_stats(),
            &mut executor,
            NOW + 1,
        );
        assert!(
            outcome2
                .skips
                .contains(&(ActionKind::Optimize, SkipReason::SnapshotUnchanged))
        );
    }

    #[test]
    fn repeated_failures_trip_the_circuit_breaker() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor {
            fail_expire: Some("simulated failure".to_string()),
            ..RecordingExecutor::default()
        };
        let mut now = NOW;
        // 4 failures (default max) -> circuit broken; backoff between attempts.
        for _ in 0..4 {
            now += FAILURE_BACKOFF_MAX_MS + 1;
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, now);
        }
        assert_eq!(executor.expires.len(), 4);
        now += FAILURE_BACKOFF_MAX_MS + 1;
        let outcome =
            coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, now);
        assert!(
            outcome
                .skips
                .contains(&(ActionKind::Expire, SkipReason::CircuitBroken))
        );
        assert_eq!(
            executor.expires.len(),
            4,
            "no further attempts after circuit break"
        );
    }

    #[test]
    fn optimize_cooldown_prevents_immediate_retrigger() {
        let mut coordinator = coordinator();
        let mut executor = RecordingExecutor::default();
        coordinator.process_table(1, &target(), &old_small_file_stats(), &mut executor, NOW);
        // New snapshot but within cooldown.
        let mut stats = old_small_file_stats();
        stats.current_snapshot_id = Some(31);
        let outcome = coordinator.process_table(1, &target(), &stats, &mut executor, NOW + 1);
        assert!(
            outcome
                .skips
                .contains(&(ActionKind::Optimize, SkipReason::Cooldown))
        );
        assert_eq!(executor.optimize_submissions.len(), 1);
    }
}
