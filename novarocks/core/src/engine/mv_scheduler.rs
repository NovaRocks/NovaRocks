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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::mv::refresh_io::{load_current_iceberg_base_table, parse_iceberg_table_refs};
use crate::mv::persistence::definition::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use crate::mv::persistence::refresh::MvRefreshState;
use crate::mv::repository::MvRepository;
use crate::novarocks_config::StandaloneServerConfig;
use crate::sql::parser::ast::{ObjectName, RefreshMaterializedViewStmt};
use novarocks_catalog::identifier::TableIdentity;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCoordinatorConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent_refreshes: usize,
    pub(crate) failure_backoff_ms: i64,
    pub(crate) max_failure_backoff_ms: i64,
}

impl RefreshCoordinatorConfig {
    pub(crate) fn from_standalone_config(config: &StandaloneServerConfig) -> Self {
        let failure_backoff_ms = config.mv_refresh_scheduler_failure_backoff_ms.max(1);
        Self {
            enabled: config.mv_refresh_scheduler_enabled,
            tick_interval_ms: config.mv_refresh_scheduler_interval_ms.max(1),
            max_concurrent_refreshes: config.mv_refresh_scheduler_max_concurrent.max(1),
            failure_backoff_ms,
            max_failure_backoff_ms: config
                .mv_refresh_scheduler_max_failure_backoff_ms
                .max(failure_backoff_ms),
        }
    }
}

impl Default for RefreshCoordinatorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick_interval_ms: 30_000,
            max_concurrent_refreshes: 1,
            failure_backoff_ms: 60_000,
            max_failure_backoff_ms: 1_800_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshTaskState {
    Pending,
    Running,
    Succeeded,
    FailedBackoff,
    FailedUserError,
    BlockedRecovery,
    Paused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshTaskReason {
    Manual,
    Periodic,
    SnapshotChange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCandidate {
    pub(crate) mv_id: i64,
    pub(crate) policy: StoredMvRefreshPolicy,
    pub(crate) state: RefreshTaskState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PeriodicRefreshDecision {
    pub(crate) mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveRefreshState {
    pub(crate) refresh_id: i64,
    pub(crate) state: MvRefreshState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SchedulerGuardDecision {
    pub(crate) state: RefreshTaskState,
    pub(crate) can_enqueue: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RefreshQueueEntry {
    mv_id: i64,
    reason: RefreshTaskReason,
}

pub(crate) trait RefreshExecutor {
    fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String>;
}

#[derive(Debug)]
pub(crate) struct RefreshCoordinator {
    config: RefreshCoordinatorConfig,
    queue: VecDeque<RefreshQueueEntry>,
    queued_mv_ids: BTreeSet<i64>,
    running_mv_ids: BTreeSet<i64>,
    states: BTreeMap<i64, RefreshTaskState>,
    observed_snapshots: BTreeMap<(i64, String), i64>,
    failure_attempts: BTreeMap<i64, u32>,
}

impl RefreshCoordinator {
    fn new(config: RefreshCoordinatorConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued_mv_ids: BTreeSet::new(),
            running_mv_ids: BTreeSet::new(),
            states: BTreeMap::new(),
            observed_snapshots: BTreeMap::new(),
            failure_attempts: BTreeMap::new(),
        }
    }

    pub(crate) fn enqueue_refresh(&mut self, mv_id: i64, reason: RefreshTaskReason) -> bool {
        if self.queued_mv_ids.contains(&mv_id) || self.running_mv_ids.contains(&mv_id) {
            return false;
        }
        self.queue.push_back(RefreshQueueEntry { mv_id, reason });
        self.queued_mv_ids.insert(mv_id);
        self.states.insert(mv_id, RefreshTaskState::Pending);
        true
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn state_for_mv(&self, mv_id: i64) -> Option<RefreshTaskState> {
        self.states.get(&mv_id).copied()
    }

    pub(crate) fn drain_ready<E: RefreshExecutor>(
        &mut self,
        executor: &mut E,
        _now_ms: i64,
    ) -> Result<(), String> {
        let capacity = self
            .config
            .max_concurrent_refreshes
            .saturating_sub(self.running_mv_ids.len());
        for _ in 0..capacity {
            let Some(entry) = self.queue.pop_front() else {
                break;
            };
            self.queued_mv_ids.remove(&entry.mv_id);
            if self.running_mv_ids.contains(&entry.mv_id) {
                continue;
            }
            let _reason = entry.reason;
            self.running_mv_ids.insert(entry.mv_id);
            self.states.insert(entry.mv_id, RefreshTaskState::Running);
            let result = executor.execute_refresh(entry.mv_id);
            self.running_mv_ids.remove(&entry.mv_id);
            match result {
                Ok(()) => {
                    self.failure_attempts.remove(&entry.mv_id);
                    self.states.insert(entry.mv_id, RefreshTaskState::Succeeded);
                }
                Err(_) => {
                    self.states
                        .insert(entry.mv_id, RefreshTaskState::FailedBackoff);
                }
            }
        }
        Ok(())
    }

    fn tick_state(
        &mut self,
        state: &Arc<crate::engine::StandaloneState>,
        now_ms: i64,
    ) -> Result<(), String> {
        let metadata = load_scheduler_metadata(state)?;
        let mut snapshot_source = IcebergSnapshotSource::new(Arc::clone(state));
        for error in self.poll_snapshot_watch(
            &metadata.definitions,
            &metadata.active_refreshes,
            &mut snapshot_source,
            now_ms,
        )? {
            self.record_scheduler_runtime_failure(state, error.mv_id, &error.error, now_ms)?;
        }
        for definition in &metadata.definitions {
            let guard = scheduler_guard_for_definition(
                definition,
                metadata.active_refreshes.get(&definition.mv_id),
                now_ms,
            );
            if !guard.can_enqueue {
                self.states.insert(definition.mv_id, guard.state);
                continue;
            }
            if is_periodic_refresh_due(definition, now_ms) {
                self.enqueue_refresh(definition.mv_id, RefreshTaskReason::Periodic);
            }
        }
        let capacity = self
            .config
            .max_concurrent_refreshes
            .saturating_sub(self.running_mv_ids.len());
        let mut executor = MetadataRefreshExecutor::new(Arc::clone(state));
        for _ in 0..capacity {
            let Some(entry) = self.queue.pop_front() else {
                break;
            };
            self.queued_mv_ids.remove(&entry.mv_id);
            if self.running_mv_ids.contains(&entry.mv_id) {
                continue;
            }
            let _reason = entry.reason;
            self.running_mv_ids.insert(entry.mv_id);
            self.states.insert(entry.mv_id, RefreshTaskState::Running);
            let result = executor.execute_refresh(entry.mv_id);
            self.running_mv_ids.remove(&entry.mv_id);
            match result {
                Ok(()) => {
                    self.failure_attempts.remove(&entry.mv_id);
                    self.states.insert(entry.mv_id, RefreshTaskState::Succeeded);
                    record_scheduler_success_metadata(state, entry.mv_id, now_ms)?;
                }
                Err(err) => {
                    self.record_scheduler_runtime_failure(state, entry.mv_id, &err, now_ms)?;
                }
            }
        }
        Ok(())
    }

    fn record_scheduler_runtime_failure(
        &mut self,
        state: &Arc<crate::engine::StandaloneState>,
        mv_id: i64,
        err: &str,
        now_ms: i64,
    ) -> Result<(), String> {
        match classify_scheduler_failure(err) {
            SchedulerFailureClass::Retryable => {
                let attempt = self.next_failure_attempt(mv_id);
                let backoff_ms = scheduler_backoff_ms(&self.config, attempt);
                self.states.insert(mv_id, RefreshTaskState::FailedBackoff);
                record_scheduler_failure_metadata(state, mv_id, err, now_ms, backoff_ms)?;
            }
            SchedulerFailureClass::User => {
                self.failure_attempts.remove(&mv_id);
                self.states.insert(mv_id, RefreshTaskState::FailedUserError);
                record_scheduler_user_error_metadata(state, mv_id, err)?;
            }
        }
        Ok(())
    }

    fn next_failure_attempt(&mut self, mv_id: i64) -> u32 {
        let attempt = self.failure_attempts.entry(mv_id).or_insert(0);
        *attempt = attempt.saturating_add(1).max(1);
        *attempt
    }

    fn poll_snapshot_watch<S: SnapshotSource>(
        &mut self,
        definitions: &[StoredMvDefinition],
        active_refreshes: &BTreeMap<i64, ActiveRefreshState>,
        source: &mut S,
        now_ms: i64,
    ) -> Result<Vec<SnapshotWatchError>, String> {
        let mut errors = Vec::new();
        for definition in definitions {
            let guard = scheduler_guard_for_definition(
                definition,
                active_refreshes.get(&definition.mv_id),
                now_ms,
            );
            if !guard.can_enqueue {
                self.states.insert(definition.mv_id, guard.state);
                continue;
            }
            if !matches!(
                definition.refresh_policy,
                StoredMvRefreshPolicy::AsyncOnChange
            ) || definition.refresh_paused
            {
                continue;
            }
            if definition
                .next_refresh_after_ms
                .map(|next| next > now_ms)
                .unwrap_or(false)
            {
                continue;
            }
            let base_refs = match parse_iceberg_table_refs(&definition.base_table_refs) {
                Ok(base_refs) => base_refs,
                Err(err) => {
                    self.states
                        .insert(definition.mv_id, RefreshTaskState::FailedBackoff);
                    errors.push(SnapshotWatchError {
                        mv_id: definition.mv_id,
                        error: err,
                    });
                    continue;
                }
            };
            let mut should_enqueue = false;
            for table_ref in base_refs {
                let fqn = table_ref.fqn();
                let key = (definition.mv_id, fqn.clone());
                let snapshot_id = match source.current_snapshot(&table_ref) {
                    Ok(snapshot_id) => snapshot_id,
                    Err(err) => {
                        self.states
                            .insert(definition.mv_id, RefreshTaskState::FailedBackoff);
                        errors.push(SnapshotWatchError {
                            mv_id: definition.mv_id,
                            error: err,
                        });
                        should_enqueue = false;
                        break;
                    }
                };
                let Some(snapshot_id) = snapshot_id else {
                    continue;
                };
                match self.observed_snapshots.get(&key).copied() {
                    Some(previous) if snapshot_id != previous => {
                        self.observed_snapshots.insert(key, snapshot_id);
                        should_enqueue = true;
                    }
                    Some(_) => {}
                    None => {
                        self.observed_snapshots.insert(key, snapshot_id);
                    }
                }
            }
            if should_enqueue {
                self.enqueue_refresh(definition.mv_id, RefreshTaskReason::SnapshotChange);
            }
        }
        Ok(errors)
    }
}

pub(crate) fn scheduler_guard_for_definition(
    definition: &StoredMvDefinition,
    active_refresh: Option<&ActiveRefreshState>,
    _now_ms: i64,
) -> SchedulerGuardDecision {
    if definition.refresh_paused {
        return SchedulerGuardDecision {
            state: RefreshTaskState::Paused,
            can_enqueue: false,
        };
    }
    if active_refresh
        .map(|active| active.state == MvRefreshState::CommitUnknown)
        .unwrap_or(false)
    {
        return SchedulerGuardDecision {
            state: RefreshTaskState::BlockedRecovery,
            can_enqueue: false,
        };
    }
    if active_refresh.is_some()
        || definition.refresh_in_progress
        || definition.active_refresh_id.is_some()
    {
        return SchedulerGuardDecision {
            state: RefreshTaskState::Running,
            can_enqueue: false,
        };
    }
    if has_non_retryable_scheduler_error(definition) {
        return SchedulerGuardDecision {
            state: RefreshTaskState::FailedUserError,
            can_enqueue: false,
        };
    }
    SchedulerGuardDecision {
        state: RefreshTaskState::Pending,
        can_enqueue: true,
    }
}

pub(crate) trait SnapshotSource {
    fn current_snapshot(&mut self, table_ref: &TableIdentity) -> Result<Option<i64>, String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapshotWatchError {
    mv_id: i64,
    error: String,
}

pub(crate) struct IcebergSnapshotSource {
    state: Arc<crate::engine::StandaloneState>,
}

impl IcebergSnapshotSource {
    pub(crate) fn new(state: Arc<crate::engine::StandaloneState>) -> Self {
        Self { state }
    }
}

impl SnapshotSource for IcebergSnapshotSource {
    fn current_snapshot(&mut self, table_ref: &TableIdentity) -> Result<Option<i64>, String> {
        let loaded = load_current_iceberg_base_table(&self.state, table_ref)?;
        Ok(loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()))
    }
}

pub(crate) fn plan_periodic_refreshes(
    definitions: &[StoredMvDefinition],
    now_ms: i64,
) -> Vec<PeriodicRefreshDecision> {
    definitions
        .iter()
        .filter(|definition| scheduler_guard_for_definition(definition, None, now_ms).can_enqueue)
        .filter(|definition| is_periodic_refresh_due(definition, now_ms))
        .map(|definition| PeriodicRefreshDecision {
            mv_id: definition.mv_id,
        })
        .collect()
}

fn is_periodic_refresh_due(definition: &StoredMvDefinition, now_ms: i64) -> bool {
    if has_non_retryable_scheduler_error(definition) {
        return false;
    }
    matches!(
        definition.refresh_policy,
        StoredMvRefreshPolicy::AsyncInterval
    ) && definition.refresh_interval_ms.is_some()
        && definition
            .next_refresh_after_ms
            .map(|next| next <= now_ms)
            .unwrap_or(true)
}

pub(crate) fn metadata_update_after_success(
    definition: &StoredMvDefinition,
    now_ms: i64,
) -> Result<UpdateMvRefreshMetadataRequest, String> {
    let next_refresh_after_ms = match definition.refresh_policy {
        StoredMvRefreshPolicy::AsyncInterval => {
            let interval = definition.refresh_interval_ms.ok_or_else(|| {
                format!(
                    "MV definition {} has ASYNC_INTERVAL policy without interval",
                    definition.mv_id
                )
            })?;
            Some(now_ms.saturating_add(interval))
        }
        _ => definition.next_refresh_after_ms,
    };
    Ok(UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy: definition.refresh_policy.clone(),
        refresh_paused: definition.refresh_paused,
        refresh_interval_ms: definition.refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms,
    })
}

pub(crate) fn metadata_update_after_failure(
    definition: &StoredMvDefinition,
    err: &str,
    now_ms: i64,
    failure_backoff_ms: i64,
) -> UpdateMvRefreshMetadataRequest {
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy: definition.refresh_policy.clone(),
        refresh_paused: definition.refresh_paused,
        refresh_interval_ms: definition.refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: Some(err.to_string()),
        next_refresh_after_ms: Some(now_ms.saturating_add(failure_backoff_ms.max(1))),
    }
}

pub(crate) fn scheduler_backoff_ms(config: &RefreshCoordinatorConfig, attempt: u32) -> i64 {
    let base = config.failure_backoff_ms.max(1);
    let max_backoff = config.max_failure_backoff_ms.max(base);
    let shift = attempt.max(1).saturating_sub(1).min(62);
    let multiplier = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
    base.saturating_mul(multiplier).min(max_backoff)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SchedulerFailureClass {
    Retryable,
    User,
}

fn classify_scheduler_failure(err: &str) -> SchedulerFailureClass {
    if is_retryable_scheduler_error_text(err) {
        SchedulerFailureClass::Retryable
    } else {
        SchedulerFailureClass::User
    }
}

fn is_retryable_scheduler_error_text(err: &str) -> bool {
    let lower = err.trim().to_ascii_lowercase();
    if lower.starts_with(USER_ERROR_PREFIX.to_ascii_lowercase().as_str()) {
        return false;
    }
    let retryable_markers = [
        "timeout",
        "timed out",
        "temporar",
        "unavailable",
        "connection",
        "connect",
        "network",
        "transport",
        "deadline",
        "throttl",
        "too many requests",
        "429",
        "500",
        "502",
        "503",
        "504",
        "io error",
        "i/o error",
        "broken pipe",
        "reset by peer",
        "refused",
    ];
    if retryable_markers
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return true;
    }
    let user_markers = [
        "unsupported",
        "not supported",
        "invalid",
        "syntax",
        "parse",
        "analyze",
        "analysis",
        "unknown column",
        "unknown table",
        "not found",
        "does not exist",
        "ambiguous",
        "type mismatch",
        "permission denied",
    ];
    !user_markers.iter().any(|marker| lower.contains(marker))
}

const USER_ERROR_PREFIX: &str = "USER_ERROR: ";

fn has_non_retryable_scheduler_error(definition: &StoredMvDefinition) -> bool {
    definition
        .last_scheduler_error
        .as_ref()
        .map(|err| err.trim_start().starts_with(USER_ERROR_PREFIX))
        .unwrap_or(false)
}

fn scheduler_user_error_text(err: &str) -> String {
    let trimmed = err.trim();
    if trimmed.starts_with(USER_ERROR_PREFIX) {
        trimmed.to_string()
    } else {
        format!("{USER_ERROR_PREFIX}{trimmed}")
    }
}

#[derive(Clone, Debug, Default)]
struct SchedulerMetadata {
    definitions: Vec<StoredMvDefinition>,
    active_refreshes: BTreeMap<i64, ActiveRefreshState>,
}

fn load_scheduler_metadata(
    state: &Arc<crate::engine::StandaloneState>,
) -> Result<SchedulerMetadata, String> {
    if !state.mv_repository.availability().is_available() {
        return Ok(SchedulerMetadata::default());
    }
    let definitions = state
        .mv_repository
        .list_definitions()
        .map_err(|e| format!("list MV definitions failed: {e}"))?;
    let mut active_refreshes = BTreeMap::new();
    for definition in &definitions {
        let Some(refresh_id) = definition.active_refresh_id else {
            continue;
        };
        let Some(refresh) = state
            .mv_repository
            .load_refresh(refresh_id)
            .map_err(|e| format!("load active MV refresh failed: {e}"))?
        else {
            continue;
        };
        active_refreshes.insert(
            definition.mv_id,
            ActiveRefreshState {
                refresh_id,
                state: refresh.state,
            },
        );
    }
    Ok(SchedulerMetadata {
        definitions,
        active_refreshes,
    })
}

fn record_scheduler_success_metadata(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    now_ms: i64,
) -> Result<(), String> {
    update_scheduler_metadata(state, mv_id, |definition| {
        metadata_update_after_success(definition, now_ms)
    })
}

fn record_scheduler_failure_metadata(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    err: &str,
    now_ms: i64,
    failure_backoff_ms: i64,
) -> Result<(), String> {
    update_scheduler_metadata(state, mv_id, |definition| {
        Ok(metadata_update_after_failure(
            definition,
            err,
            now_ms,
            failure_backoff_ms,
        ))
    })
}

fn record_scheduler_user_error_metadata(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    err: &str,
) -> Result<(), String> {
    update_scheduler_metadata(state, mv_id, |definition| {
        Ok(UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy.clone(),
            refresh_paused: definition.refresh_paused,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: Some(scheduler_user_error_text(err)),
            next_refresh_after_ms: None,
        })
    })
}

fn update_scheduler_metadata<F>(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    build_request: F,
) -> Result<(), String>
where
    F: FnOnce(&StoredMvDefinition) -> Result<UpdateMvRefreshMetadataRequest, String>,
{
    let definition = state
        .mv_repository
        .load_by_id(mv_id)
        .map_err(|e| format!("load MV definition failed: {e}"))?
        .ok_or_else(|| format!("MV definition {mv_id} not found"))?;
    let req = build_request(&definition)?;
    state
        .mv_repository
        .update_refresh_metadata(req)
        .map_err(|e| format!("update MV scheduler metadata failed: {e}"))?;
    Ok(())
}

pub(crate) struct MetadataRefreshExecutor {
    state: Arc<crate::engine::StandaloneState>,
}

impl MetadataRefreshExecutor {
    pub(crate) fn new(state: Arc<crate::engine::StandaloneState>) -> Self {
        Self { state }
    }
}

impl RefreshExecutor for MetadataRefreshExecutor {
    fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String> {
        let target = load_refresh_execution_target(&self.state, mv_id)?;
        let connector_context = crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?;
        crate::engine::mv_flow::refresh_mv_with_connector_context(
            &self.state,
            target.current_catalog.as_deref(),
            &target.current_database,
            &RefreshMaterializedViewStmt {
                name: target.name,
                full: false,
            },
            &connector_context,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RefreshExecutionTarget {
    current_catalog: Option<String>,
    current_database: String,
    name: ObjectName,
}

fn load_refresh_execution_target(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
) -> Result<RefreshExecutionTarget, String> {
    let definition = state
        .mv_repository
        .load_by_id(mv_id)
        .map_err(|e| format!("load MV definition failed: {e}"))?
        .ok_or_else(|| format!("MV definition {mv_id} not found"))?;
    refresh_execution_target_for_definition(state, &definition)
}

fn refresh_execution_target_for_definition(
    state: &Arc<crate::engine::StandaloneState>,
    definition: &StoredMvDefinition,
) -> Result<RefreshExecutionTarget, String> {
    match (
        definition.target_catalog.as_ref(),
        definition.target_namespace.as_ref(),
        definition.target_table.as_ref(),
    ) {
        (Some(catalog), Some(namespace), Some(table)) => {
            return Ok(RefreshExecutionTarget {
                current_catalog: Some(catalog.clone()),
                current_database: namespace.clone(),
                name: ObjectName {
                    parts: vec![table.clone()],
                },
            });
        }
        (None, None, None) => {}
        _ => {
            return Err(format!(
                "MV definition {} has incomplete target metadata",
                definition.mv_id
            ));
        }
    }

    Err(format!(
        "legacy materialized view definition {} has no external target identity",
        definition.mv_id
    ))
}

pub(crate) struct RefreshCoordinatorHandle {
    enabled: bool,
    stop_tx: Option<Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl RefreshCoordinatorHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            stop_tx: None,
            worker: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Drop for RefreshCoordinatorHandle {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn start_refresh_coordinator_for_server(
    engine: &crate::engine::StandaloneNovaRocks,
    config: RefreshCoordinatorConfig,
) -> RefreshCoordinatorHandle {
    if !config.enabled || !engine.inner.mv_repository.availability().is_available() {
        return RefreshCoordinatorHandle::disabled();
    }
    let state = Arc::clone(&engine.inner);
    let worker_config = config.clone();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::Builder::new()
        .name("novarocks-mv-refresh-scheduler".to_string())
        .spawn(move || {
            let mut coordinator = RefreshCoordinator::new(worker_config.clone());
            loop {
                if let Err(err) = coordinator.tick_state(&state, current_time_ms()) {
                    tracing::warn!(error = %err, "MV refresh scheduler tick failed");
                }
                match stop_rx.recv_timeout(Duration::from_millis(worker_config.tick_interval_ms)) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        });
    match worker {
        Ok(worker) => RefreshCoordinatorHandle {
            enabled: true,
            stop_tx: Some(stop_tx),
            worker: Some(worker),
        },
        Err(err) => {
            tracing::warn!(error = %err, "failed to start MV refresh scheduler worker");
            RefreshCoordinatorHandle::disabled()
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

pub(crate) fn scan_refresh_candidates(
    definitions: &[StoredMvDefinition],
    _now_ms: i64,
) -> Vec<RefreshCandidate> {
    definitions
        .iter()
        .filter_map(|definition| {
            if definition.refresh_paused {
                return Some(RefreshCandidate {
                    mv_id: definition.mv_id,
                    policy: definition.refresh_policy.clone(),
                    state: RefreshTaskState::Paused,
                });
            }
            if matches!(definition.refresh_policy, StoredMvRefreshPolicy::Manual) {
                return None;
            }
            Some(RefreshCandidate {
                mv_id: definition.mv_id,
                policy: definition.refresh_policy.clone(),
                state: RefreshTaskState::Pending,
            })
        })
        .filter(|candidate| !matches!(candidate.state, RefreshTaskState::Paused))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
    use crate::mv::persistence::refresh::MvRefreshState;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct RecordingRefreshExecutor {
        executed_mv_ids: Vec<i64>,
        failure: Option<String>,
    }

    struct FakeSnapshotSource {
        snapshots: BTreeMap<String, Result<Option<i64>, String>>,
    }

    impl FakeSnapshotSource {
        fn new(
            entries: impl IntoIterator<Item = (&'static str, Result<Option<i64>, &'static str>)>,
        ) -> Self {
            Self {
                snapshots: entries
                    .into_iter()
                    .map(|(fqn, result)| {
                        (
                            fqn.to_string(),
                            result.map_err(|message| message.to_string()),
                        )
                    })
                    .collect(),
            }
        }
    }

    impl SnapshotSource for FakeSnapshotSource {
        fn current_snapshot(&mut self, table_ref: &TableIdentity) -> Result<Option<i64>, String> {
            self.snapshots
                .get(&table_ref.fqn())
                .cloned()
                .unwrap_or(Ok(None))
        }
    }

    impl RecordingRefreshExecutor {
        fn failing(message: &str) -> Self {
            Self {
                executed_mv_ids: Vec::new(),
                failure: Some(message.to_string()),
            }
        }

        fn executed_mv_ids(&self) -> Vec<i64> {
            self.executed_mv_ids.clone()
        }
    }

    impl RefreshExecutor for RecordingRefreshExecutor {
        fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String> {
            self.executed_mv_ids.push(mv_id);
            match self.failure.as_ref() {
                Some(message) => Err(message.clone()),
                None => Ok(()),
            }
        }
    }

    impl RefreshCoordinatorConfig {
        fn enabled_for_test() -> Self {
            Self {
                enabled: true,
                ..Self::default()
            }
        }
    }

    impl RefreshCoordinator {
        fn new_for_test(config: RefreshCoordinatorConfig) -> Self {
            Self::new(config)
        }

        fn drain_ready_for_test<E: RefreshExecutor>(
            &mut self,
            executor: &mut E,
            now_ms: i64,
        ) -> Result<(), String> {
            self.drain_ready(executor, now_ms)
        }

        fn observe_snapshot_for_test(&mut self, mv_id: i64, fqn: &str, snapshot_id: i64) {
            self.observed_snapshots
                .insert((mv_id, fqn.to_string()), snapshot_id);
        }

        fn observed_snapshot_for_test(&self, mv_id: i64, fqn: &str) -> Option<i64> {
            self.observed_snapshots
                .get(&(mv_id, fqn.to_string()))
                .copied()
        }

        fn pending_mv_ids_for_test(&self) -> Vec<i64> {
            self.queue.iter().map(|entry| entry.mv_id).collect()
        }

        fn poll_snapshot_watch_for_test<S: SnapshotSource>(
            &mut self,
            definitions: &[StoredMvDefinition],
            source: &mut S,
            now_ms: i64,
        ) -> Result<(), String> {
            let _errors =
                self.poll_snapshot_watch(definitions, &BTreeMap::new(), source, now_ms)?;
            Ok(())
        }
    }

    fn test_definition(mv_id: i64, refresh_policy: StoredMvRefreshPolicy) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: "starrocks".to_string(),
            target_catalog: None,
            target_namespace: None,
            target_table: None,
            schema_contract: None,
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    fn async_on_change_definition(mv_id: i64, base_table_refs: Vec<&str>) -> StoredMvDefinition {
        let mut definition = test_definition(mv_id, StoredMvRefreshPolicy::AsyncOnChange);
        definition.base_table_refs = base_table_refs
            .into_iter()
            .map(|value| value.to_string())
            .collect();
        definition
    }

    #[test]
    fn disabled_coordinator_handle_does_not_start_worker() {
        let handle = RefreshCoordinatorHandle::disabled();

        assert!(!handle.is_enabled());
    }

    #[test]
    fn scan_candidates_skips_manual_and_paused_mvs() {
        let now_ms = 1_000;
        let manual = test_definition(1, StoredMvRefreshPolicy::Manual);
        let mut paused = test_definition(2, StoredMvRefreshPolicy::AsyncOnChange);
        paused.refresh_paused = true;
        let async_mv = test_definition(3, StoredMvRefreshPolicy::AsyncOnChange);

        let candidates = scan_refresh_candidates(&[manual, paused, async_mv], now_ms);

        assert_eq!(
            candidates,
            vec![RefreshCandidate {
                mv_id: 3,
                policy: StoredMvRefreshPolicy::AsyncOnChange,
                state: RefreshTaskState::Pending,
            }]
        );
    }

    #[test]
    fn enqueue_refresh_deduplicates_same_mv_until_drained() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());

        assert!(coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
        assert!(!coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
        assert_eq!(coordinator.pending_len(), 1);
    }

    #[test]
    fn drain_once_executes_manual_refresh_and_records_success() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::default();

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(executor.executed_mv_ids(), vec![7]);
        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::Succeeded)
        );
    }

    #[test]
    fn drain_once_records_failure_backoff() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::failing("refresh failed");

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::FailedBackoff)
        );
    }

    #[test]
    fn periodic_policy_enqueues_only_when_due() {
        let mut due = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
        due.refresh_interval_ms = Some(10_000);
        due.next_refresh_after_ms = Some(1_000);
        let mut future = test_definition(2, StoredMvRefreshPolicy::AsyncInterval);
        future.refresh_interval_ms = Some(10_000);
        future.next_refresh_after_ms = Some(2_000);

        let decisions = plan_periodic_refreshes(&[due, future], 1_500);

        assert_eq!(
            decisions
                .into_iter()
                .map(|decision| decision.mv_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn periodic_success_sets_next_refresh_after() {
        let mut definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(10_000);

        let req = metadata_update_after_success(&definition, 1_500).expect("success metadata");

        assert_eq!(req.last_scheduler_error, None);
        assert_eq!(req.next_refresh_after_ms, Some(11_500));
    }

    #[test]
    fn periodic_failure_sets_backoff_and_preserves_policy() {
        let definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);

        let req = metadata_update_after_failure(&definition, "boom", 1_500, 30_000);

        assert_eq!(req.last_scheduler_error, Some("boom".to_string()));
        assert_eq!(req.next_refresh_after_ms, Some(31_500));
        assert_eq!(req.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
    }

    #[test]
    fn transient_failures_use_bounded_exponential_backoff() {
        let config = RefreshCoordinatorConfig {
            enabled: true,
            failure_backoff_ms: 1_000,
            max_failure_backoff_ms: 8_000,
            ..RefreshCoordinatorConfig::default()
        };

        assert_eq!(scheduler_backoff_ms(&config, 1), 1_000);
        assert_eq!(scheduler_backoff_ms(&config, 2), 2_000);
        assert_eq!(scheduler_backoff_ms(&config, 5), 8_000);
    }

    #[test]
    fn non_retryable_user_error_does_not_plan_periodic_retry() {
        let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(1_000);
        definition.last_scheduler_error = Some("USER_ERROR: unsupported MV shape".to_string());
        definition.next_refresh_after_ms = None;

        let decisions = plan_periodic_refreshes(&[definition], 10_000);

        assert!(decisions.is_empty());
    }

    #[test]
    fn periodic_policy_skips_paused_and_enqueues_after_resume() {
        let mut paused = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        paused.refresh_interval_ms = Some(1_000);
        paused.next_refresh_after_ms = Some(500);
        paused.refresh_paused = true;

        assert!(plan_periodic_refreshes(&[paused.clone()], 1_000).is_empty());

        let mut resumed = paused;
        resumed.refresh_paused = false;

        let decisions = plan_periodic_refreshes(&[resumed], 1_000);

        assert_eq!(
            decisions
                .into_iter()
                .map(|decision| decision.mv_id)
                .collect::<Vec<_>>(),
            vec![7]
        );
    }

    #[test]
    fn scheduler_guard_reports_non_retryable_user_error() {
        let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(1_000);
        definition.last_scheduler_error = Some("USER_ERROR: unsupported MV shape".to_string());

        let decision = scheduler_guard_for_definition(&definition, None, 1_000);

        assert_eq!(decision.state, RefreshTaskState::FailedUserError);
        assert!(!decision.can_enqueue);
    }

    #[test]
    fn successful_drain_resets_failure_attempts() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.failure_attempts.insert(7, 3);
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::default();

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(executor.executed_mv_ids(), vec![7]);
        assert!(!coordinator.failure_attempts.contains_key(&7));
        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::Succeeded)
        );
    }

    #[test]
    fn snapshot_watch_does_not_enqueue_when_snapshot_is_unchanged() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
        let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
        let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(Some(100)))]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch succeeds");

        assert_eq!(coordinator.pending_len(), 0);
    }

    #[test]
    fn snapshot_watch_enqueues_once_when_snapshot_advances() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
        let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
        let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(Some(101)))]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch succeeds");

        assert_eq!(coordinator.pending_mv_ids_for_test(), vec![7]);
        assert!(!coordinator.enqueue_refresh(7, RefreshTaskReason::SnapshotChange));
    }

    #[test]
    fn snapshot_watch_enqueues_when_snapshot_id_changes_without_monotonicity() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 200);
        let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
        let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(Some(100)))]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch succeeds");

        assert_eq!(coordinator.pending_mv_ids_for_test(), vec![7]);
    }

    #[test]
    fn snapshot_watch_records_error_without_overwriting_known_snapshot() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
        let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
        let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Err("catalog unavailable"))]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch records error");

        assert_eq!(
            coordinator.observed_snapshot_for_test(7, "ice.ns.tbl"),
            Some(100)
        );
        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::FailedBackoff)
        );
    }

    #[test]
    fn snapshot_watch_multi_base_enqueues_when_any_snapshot_advances() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.left", 100);
        coordinator.observe_snapshot_for_test(7, "ice.ns.right", 200);
        let definition = async_on_change_definition(7, vec!["ice.ns.left", "ice.ns.right"]);
        let mut source = FakeSnapshotSource::new([
            ("ice.ns.left", Ok(Some(100))),
            ("ice.ns.right", Ok(Some(201))),
        ]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch succeeds");

        assert_eq!(coordinator.pending_mv_ids_for_test(), vec![7]);
    }

    #[test]
    fn snapshot_watch_no_current_snapshot_is_noop() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.observe_snapshot_for_test(7, "ice.ns.tbl", 100);
        let definition = async_on_change_definition(7, vec!["ice.ns.tbl"]);
        let mut source = FakeSnapshotSource::new([("ice.ns.tbl", Ok(None))]);

        coordinator
            .poll_snapshot_watch_for_test(&[definition], &mut source, 1_000)
            .expect("snapshot watch succeeds");

        assert_eq!(coordinator.pending_len(), 0);
        assert_eq!(
            coordinator.observed_snapshot_for_test(7, "ice.ns.tbl"),
            Some(100)
        );
    }

    #[test]
    fn scheduler_blocks_commit_unknown_refresh() {
        let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(10_000);
        definition.active_refresh_id = Some(99);
        definition.refresh_in_progress = true;
        let active = ActiveRefreshState {
            refresh_id: 99,
            state: MvRefreshState::CommitUnknown,
        };

        let decision = scheduler_guard_for_definition(&definition, Some(&active), 1_000);

        assert_eq!(decision.state, RefreshTaskState::BlockedRecovery);
        assert!(!decision.can_enqueue);
    }

    #[test]
    fn scheduler_skips_running_refresh_without_reenqueue() {
        let mut definition = test_definition(7, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(10_000);
        definition.active_refresh_id = Some(99);
        definition.refresh_in_progress = true;
        let active = ActiveRefreshState {
            refresh_id: 99,
            state: MvRefreshState::IntentCreated,
        };

        let decision = scheduler_guard_for_definition(&definition, Some(&active), 1_000);

        assert_eq!(decision.state, RefreshTaskState::Running);
        assert!(!decision.can_enqueue);
    }
}
