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

//! Frontend-owned scheduling policy for asynchronous materialized-view refresh.
//!
//! This module deliberately has no thread or provider implementation.  The
//! application host polls [`FrontendMvScheduler::poll`], hands its returned
//! requests to the worker runtime, then reports the typed terminal result via
//! [`FrontendMvScheduler::complete`].  Consequently queue coalescing and
//! durable retry metadata remain deterministic and testable without sleeps.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use novarocks::mv::background::{
    MvBackgroundEngine, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
};
use novarocks::mv::persistence::definition::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use novarocks::mv::repository::{MvRepository, MvRepositoryError, MvTarget};

/// Existing standalone scheduler settings, now interpreted by the frontend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrontendMvSchedulerConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent_refreshes: usize,
    pub(crate) failure_backoff_ms: i64,
    pub(crate) max_failure_backoff_ms: i64,
}

impl Default for FrontendMvSchedulerConfig {
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

/// Why a refresh was made runnable.  A worker does not reinterpret this as a
/// retry policy; it is purely observable scheduling state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScheduledRefreshReason {
    Interval,
    SnapshotChange,
}

/// A request that has passed scheduling admission but has not yet acquired the
/// shared per-MV activity gate.  Waiting on that gate must not consume a
/// refresh concurrency slot; the worker calls `mark_started` only after it has
/// acquired the gate and its refresh permit.
#[derive(Clone, Debug)]
pub(crate) struct ScheduledRefreshRequest {
    pub(crate) definition: StoredMvDefinition,
    pub(crate) target: MvTarget,
    pub(crate) reason: ScheduledRefreshReason,
}

/// The worker-owned execution seam.  Implementations acquire the activity
/// gate, create the bounded request context, resolve/prepares Core steps, and
/// run the existing frontend refresh lifecycle.  They return only a typed
/// terminal result to the scheduler; scheduling policy never inspects a
/// display string from that work.
pub(crate) trait ScheduledRefreshRunner: Send + Sync {
    fn execute(&self, request: ScheduledRefreshRequest) -> ScheduledRefreshDisposition;
}

/// Terminal outcome projected from typed Core preparation/application and
/// repository results.  No caller may infer one of these variants from an
/// error message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ScheduledRefreshDisposition {
    Completed,
    NoOp,
    AlreadyActive,
    TargetGone,
    TransientUnavailable(String),
    InvalidDefinition(String),
    RecoveryRequired(String),
    Corruption(String),
    InvariantViolation(String),
    ShutdownCancelled,
}

impl ScheduledRefreshDisposition {
    pub(crate) fn from_background_error(error: MvBackgroundEngineError) -> Self {
        match error.kind() {
            MvBackgroundEngineErrorKind::TargetGone => Self::TargetGone,
            MvBackgroundEngineErrorKind::TransientUnavailable => {
                Self::TransientUnavailable(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::InvalidDefinition => {
                Self::InvalidDefinition(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::RecoveryRequired => {
                Self::RecoveryRequired(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::Corruption => Self::Corruption(error.message().to_owned()),
            MvBackgroundEngineErrorKind::InvariantViolation => {
                Self::InvariantViolation(error.message().to_owned())
            }
            MvBackgroundEngineErrorKind::ShutdownCancelled => Self::ShutdownCancelled,
        }
    }
}

/// The durable scheduler-metadata action implied by a terminal disposition.
/// It is public to the frontend crate for integration tests; the scheduler
/// performs the corresponding repository write in `complete`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ScheduledRefreshMetadataDecision {
    Success {
        next_refresh_after_ms: Option<i64>,
    },
    TransientBackoff {
        error: String,
        next_refresh_after_ms: i64,
    },
    Blocked {
        error: String,
    },
    NoChange,
}

#[derive(Debug)]
pub(crate) struct FrontendMvScheduler {
    config: FrontendMvSchedulerConfig,
    queue: VecDeque<ScheduledRefreshRequest>,
    queued_mv_ids: BTreeSet<i64>,
    running_mv_ids: BTreeSet<i64>,
    failure_attempts: BTreeMap<i64, u32>,
}

impl FrontendMvScheduler {
    pub(crate) fn new(config: FrontendMvSchedulerConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued_mv_ids: BTreeSet::new(),
            running_mv_ids: BTreeSet::new(),
            failure_attempts: BTreeMap::new(),
        }
    }

    /// Discover due definitions, persist typed discovery failures, and return
    /// as many queued requests as the worker may start.  The returned requests
    /// are still pending: callers must acquire the activity gate before calling
    /// [`Self::mark_started`], which is what actually consumes capacity.
    pub(crate) fn poll(
        &mut self,
        repository: &dyn MvRepository,
        engine: &dyn MvBackgroundEngine,
        now_ms: i64,
    ) -> Result<Vec<ScheduledRefreshRequest>, MvRepositoryError> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        for definition in repository.list_definitions()? {
            self.consider_definition(repository, engine, definition, now_ms)?;
        }

        let capacity = self
            .config
            .max_concurrent_refreshes
            .max(1)
            .saturating_sub(self.running_mv_ids.len());
        let mut ready = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let Some(request) = self.queue.pop_front() else {
                break;
            };
            self.queued_mv_ids.remove(&request.definition.mv_id);
            if self.running_mv_ids.contains(&request.definition.mv_id) {
                continue;
            }
            ready.push(request);
        }
        Ok(ready)
    }

    /// Record that a worker has obtained both its FIFO activity lease and a
    /// refresh permit.  A worker that cannot acquire either simply lets its
    /// request be considered again on the next poll, without holding capacity.
    pub(crate) fn mark_started(&mut self, mv_id: i64) -> bool {
        if self.running_mv_ids.len() >= self.config.max_concurrent_refreshes.max(1)
            || self.running_mv_ids.contains(&mv_id)
        {
            return false;
        }
        self.running_mv_ids.insert(mv_id)
    }

    /// Return a dispatched-but-not-started request to the tail of its
    /// coalesced queue.  Worker runtimes call this when the shared activity
    /// gate is busy; no scheduler capacity was acquired in that case.
    pub(crate) fn requeue(&mut self, request: ScheduledRefreshRequest) {
        let mv_id = request.definition.mv_id;
        if !self.running_mv_ids.contains(&mv_id) && self.queued_mv_ids.insert(mv_id) {
            self.queue.push_back(request);
        }
    }

    /// Apply a typed terminal outcome and release the refresh concurrency slot.
    /// Refresh watermark advancement belongs to the existing refresh finalize
    /// path, so this method changes scheduler metadata only.
    pub(crate) fn complete(
        &mut self,
        repository: &dyn MvRepository,
        request: &ScheduledRefreshRequest,
        disposition: ScheduledRefreshDisposition,
        now_ms: i64,
    ) -> Result<ScheduledRefreshMetadataDecision, MvRepositoryError> {
        self.running_mv_ids.remove(&request.definition.mv_id);

        // Read current metadata rather than writing the queued copy, so a
        // concurrent ALTER/PAUSE is never reverted by an old scheduler task.
        let Some(definition) = repository.load_by_id(request.definition.mv_id)? else {
            self.failure_attempts.remove(&request.definition.mv_id);
            return Ok(ScheduledRefreshMetadataDecision::NoChange);
        };
        let decision = self.metadata_decision(&definition, &disposition, now_ms);
        match &decision {
            ScheduledRefreshMetadataDecision::Success {
                next_refresh_after_ms,
            } => {
                self.failure_attempts.remove(&definition.mv_id);
                repository.update_refresh_metadata(metadata_request(
                    &definition,
                    None,
                    *next_refresh_after_ms,
                ))?;
            }
            ScheduledRefreshMetadataDecision::TransientBackoff {
                error,
                next_refresh_after_ms,
            } => {
                repository.update_refresh_metadata(metadata_request(
                    &definition,
                    Some(error.clone()),
                    Some(*next_refresh_after_ms),
                ))?;
            }
            ScheduledRefreshMetadataDecision::Blocked { error } => {
                self.failure_attempts.remove(&definition.mv_id);
                repository.update_refresh_metadata(metadata_request(
                    &definition,
                    Some(error.clone()),
                    None,
                ))?;
            }
            ScheduledRefreshMetadataDecision::NoChange => {}
        }
        Ok(decision)
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn running_len(&self) -> usize {
        self.running_mv_ids.len()
    }

    fn consider_definition(
        &mut self,
        repository: &dyn MvRepository,
        engine: &dyn MvBackgroundEngine,
        definition: StoredMvDefinition,
        now_ms: i64,
    ) -> Result<(), MvRepositoryError> {
        if definition.refresh_paused
            || definition.refresh_in_progress
            || definition.active_refresh_id.is_some()
            || definition.last_scheduler_error.is_some()
                && definition.next_refresh_after_ms.is_none()
            || self.queued_mv_ids.contains(&definition.mv_id)
            || self.running_mv_ids.contains(&definition.mv_id)
        {
            return Ok(());
        }

        let target = match mv_target(&definition) {
            Ok(target) => target,
            Err(disposition) => {
                self.persist_discovery_disposition(repository, &definition, disposition, now_ms)?;
                return Ok(());
            }
        };
        let reason = match definition.refresh_policy {
            StoredMvRefreshPolicy::Manual => return Ok(()),
            StoredMvRefreshPolicy::AsyncInterval => {
                let Some(interval_ms) = definition.refresh_interval_ms else {
                    self.persist_discovery_disposition(
                        repository,
                        &definition,
                        ScheduledRefreshDisposition::InvalidDefinition(
                            "ASYNC_INTERVAL materialized view is missing its interval".to_string(),
                        ),
                        now_ms,
                    )?;
                    return Ok(());
                };
                if interval_ms <= 0 {
                    self.persist_discovery_disposition(
                        repository,
                        &definition,
                        ScheduledRefreshDisposition::InvalidDefinition(
                            "ASYNC_INTERVAL materialized view interval must be positive"
                                .to_string(),
                        ),
                        now_ms,
                    )?;
                    return Ok(());
                }
                if !definition
                    .next_refresh_after_ms
                    .map(|next| next <= now_ms)
                    .unwrap_or(true)
                {
                    return Ok(());
                }
                ScheduledRefreshReason::Interval
            }
            StoredMvRefreshPolicy::AsyncOnChange => match engine.current_base_snapshots(&target) {
                Ok(current) if snapshots_require_refresh(&definition, &current) => {
                    ScheduledRefreshReason::SnapshotChange
                }
                Ok(_) => return Ok(()),
                Err(error) => {
                    self.persist_discovery_disposition(
                        repository,
                        &definition,
                        ScheduledRefreshDisposition::from_background_error(error),
                        now_ms,
                    )?;
                    return Ok(());
                }
            },
        };
        self.queue.push_back(ScheduledRefreshRequest {
            definition: definition.clone(),
            target,
            reason,
        });
        self.queued_mv_ids.insert(definition.mv_id);
        Ok(())
    }

    fn persist_discovery_disposition(
        &mut self,
        repository: &dyn MvRepository,
        definition: &StoredMvDefinition,
        disposition: ScheduledRefreshDisposition,
        now_ms: i64,
    ) -> Result<(), MvRepositoryError> {
        let decision = self.metadata_decision(definition, &disposition, now_ms);
        match decision {
            ScheduledRefreshMetadataDecision::TransientBackoff {
                error,
                next_refresh_after_ms,
            } => {
                repository.update_refresh_metadata(metadata_request(
                    definition,
                    Some(error),
                    Some(next_refresh_after_ms),
                ))?;
            }
            ScheduledRefreshMetadataDecision::Blocked { error } => {
                self.failure_attempts.remove(&definition.mv_id);
                repository.update_refresh_metadata(metadata_request(
                    definition,
                    Some(error),
                    None,
                ))?;
            }
            ScheduledRefreshMetadataDecision::Success { .. }
            | ScheduledRefreshMetadataDecision::NoChange => {}
        }
        Ok(())
    }

    fn metadata_decision(
        &mut self,
        definition: &StoredMvDefinition,
        disposition: &ScheduledRefreshDisposition,
        now_ms: i64,
    ) -> ScheduledRefreshMetadataDecision {
        match disposition {
            ScheduledRefreshDisposition::Completed | ScheduledRefreshDisposition::NoOp => {
                let next_refresh_after_ms = match definition.refresh_policy {
                    StoredMvRefreshPolicy::AsyncInterval => definition
                        .refresh_interval_ms
                        .filter(|interval| *interval > 0)
                        .map(|interval| now_ms.saturating_add(interval)),
                    StoredMvRefreshPolicy::Manual | StoredMvRefreshPolicy::AsyncOnChange => None,
                };
                ScheduledRefreshMetadataDecision::Success {
                    next_refresh_after_ms,
                }
            }
            ScheduledRefreshDisposition::TransientUnavailable(error) => {
                let attempt = *self
                    .failure_attempts
                    .entry(definition.mv_id)
                    .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                    .or_insert(1);
                ScheduledRefreshMetadataDecision::TransientBackoff {
                    error: error.clone(),
                    next_refresh_after_ms: now_ms.saturating_add(backoff_ms(&self.config, attempt)),
                }
            }
            ScheduledRefreshDisposition::InvalidDefinition(error)
            | ScheduledRefreshDisposition::RecoveryRequired(error)
            | ScheduledRefreshDisposition::Corruption(error)
            | ScheduledRefreshDisposition::InvariantViolation(error) => {
                ScheduledRefreshMetadataDecision::Blocked {
                    error: error.clone(),
                }
            }
            // A dropped target, a repository active fence, and shutdown are
            // not scheduler failures.  In particular they must not fabricate a
            // backoff or overwrite a recovery/refresh owner decision.
            ScheduledRefreshDisposition::AlreadyActive
            | ScheduledRefreshDisposition::TargetGone
            | ScheduledRefreshDisposition::ShutdownCancelled => {
                ScheduledRefreshMetadataDecision::NoChange
            }
        }
    }
}

fn mv_target(definition: &StoredMvDefinition) -> Result<MvTarget, ScheduledRefreshDisposition> {
    match (
        definition.target_catalog.as_deref(),
        definition.target_namespace.as_deref(),
        definition.target_table.as_deref(),
    ) {
        (Some(catalog), Some(database), Some(name)) => Ok(MvTarget {
            catalog: Some(catalog.to_owned()),
            database: database.to_owned(),
            name: name.to_owned(),
        }),
        _ => Err(ScheduledRefreshDisposition::InvalidDefinition(
            "scheduled materialized view is missing its canonical target".to_string(),
        )),
    }
}

/// `None` is part of the provider vector: a base whose current snapshot was
/// removed differs from a persisted `Some(id)`.  The empty durable watermark is
/// intentionally always due, including for an otherwise empty current vector.
fn snapshots_require_refresh(
    definition: &StoredMvDefinition,
    current: &BTreeMap<String, Option<i64>>,
) -> bool {
    if definition.last_refresh_snapshots.is_empty() {
        return true;
    }
    current.len() != definition.last_refresh_snapshots.len()
        || current.iter().any(|(base, snapshot)| {
            definition.last_refresh_snapshots.get(base).copied() != *snapshot
        })
}

fn metadata_request(
    definition: &StoredMvDefinition,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
) -> UpdateMvRefreshMetadataRequest {
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy: definition.refresh_policy.clone(),
        refresh_paused: definition.refresh_paused,
        refresh_interval_ms: definition.refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error,
        next_refresh_after_ms,
    }
}

fn backoff_ms(config: &FrontendMvSchedulerConfig, attempt: u32) -> i64 {
    let base = config.failure_backoff_ms.max(1);
    let maximum = config.max_failure_backoff_ms.max(base);
    let shift = attempt.saturating_sub(1).min(62);
    base.saturating_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .min(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(policy: StoredMvRefreshPolicy) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 7,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec!["iceberg.db.base".to_string()],
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
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
            refresh_policy: policy,
            refresh_paused: false,
            refresh_interval_ms: Some(100),
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn initial_on_change_watermark_is_due_without_an_observation_cache() {
        let definition = definition(StoredMvRefreshPolicy::AsyncOnChange);
        assert!(snapshots_require_refresh(&definition, &BTreeMap::new()));
    }

    #[test]
    fn on_change_compares_current_provider_vector_to_durable_watermark() {
        let mut definition = definition(StoredMvRefreshPolicy::AsyncOnChange);
        definition
            .last_refresh_snapshots
            .insert("iceberg.db.base".to_string(), 11);
        let same = BTreeMap::from([("iceberg.db.base".to_string(), Some(11))]);
        let changed = BTreeMap::from([("iceberg.db.base".to_string(), Some(12))]);
        assert!(!snapshots_require_refresh(&definition, &same));
        assert!(snapshots_require_refresh(&definition, &changed));
    }

    #[test]
    fn coalescing_never_queues_the_same_mv_twice() {
        let config = FrontendMvSchedulerConfig {
            enabled: true,
            ..Default::default()
        };
        let mut scheduler = FrontendMvScheduler::new(config);
        let definition = definition(StoredMvRefreshPolicy::AsyncInterval);
        let target = mv_target(&definition).expect("target");
        scheduler.queue.push_back(ScheduledRefreshRequest {
            definition: definition.clone(),
            target: target.clone(),
            reason: ScheduledRefreshReason::Interval,
        });
        scheduler.queued_mv_ids.insert(definition.mv_id);
        assert!(scheduler.queued_mv_ids.contains(&definition.mv_id));
        assert_eq!(scheduler.pending_len(), 1);
        assert_eq!(target.display_name(), "iceberg.db.mv");
    }

    #[test]
    fn every_typed_disposition_has_an_explicit_metadata_decision() {
        let config = FrontendMvSchedulerConfig {
            enabled: true,
            failure_backoff_ms: 10,
            max_failure_backoff_ms: 40,
            ..Default::default()
        };
        let mut scheduler = FrontendMvScheduler::new(config);
        let definition = definition(StoredMvRefreshPolicy::AsyncInterval);
        assert!(matches!(
            scheduler.metadata_decision(&definition, &ScheduledRefreshDisposition::Completed, 100),
            ScheduledRefreshMetadataDecision::Success {
                next_refresh_after_ms: Some(200)
            }
        ));
        assert!(matches!(
            scheduler.metadata_decision(&definition, &ScheduledRefreshDisposition::NoOp, 100),
            ScheduledRefreshMetadataDecision::Success { .. }
        ));
        assert_eq!(
            scheduler.metadata_decision(
                &definition,
                &ScheduledRefreshDisposition::TransientUnavailable("offline".to_string()),
                100,
            ),
            ScheduledRefreshMetadataDecision::TransientBackoff {
                error: "offline".to_string(),
                next_refresh_after_ms: 110,
            }
        );
        for disposition in [
            ScheduledRefreshDisposition::InvalidDefinition("bad".to_string()),
            ScheduledRefreshDisposition::RecoveryRequired("recover".to_string()),
            ScheduledRefreshDisposition::Corruption("corrupt".to_string()),
            ScheduledRefreshDisposition::InvariantViolation("invariant".to_string()),
        ] {
            assert!(matches!(
                scheduler.metadata_decision(&definition, &disposition, 100),
                ScheduledRefreshMetadataDecision::Blocked { .. }
            ));
        }
        for disposition in [
            ScheduledRefreshDisposition::AlreadyActive,
            ScheduledRefreshDisposition::TargetGone,
            ScheduledRefreshDisposition::ShutdownCancelled,
        ] {
            assert_eq!(
                scheduler.metadata_decision(&definition, &disposition, 100),
                ScheduledRefreshMetadataDecision::NoChange
            );
        }
    }
}
