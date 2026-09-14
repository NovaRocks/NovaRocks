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

//! Frontend repository/provider discovery adapter for asynchronous refresh.
//!
//! It interprets validated durable definitions and provider observations. The
//! MV application product owns the queue, source-revision, activity, retry and
//! terminal state; this adapter owns no process runtime ledger.

use std::collections::BTreeMap;

use super::background::MvBackgroundEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::persistence::definition::{
    MvDesiredRefreshPolicy, StoredMvDefinition,
};
use novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration;
use novarocks_mv_application::repository::{
    MvPublishedProjection, MvPublishedWaterline, MvRepositoryError,
};
use novarocks_mv_application::{
    scheduler::MvSchedulerConfig,
    scheduler_runtime::{MvRefreshDisposition, MvRefreshProductRuntime, MvRefreshRuntimeDecision},
};
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

pub(crate) type ScheduledRefreshDisposition = MvRefreshDisposition;
pub(crate) type ScheduledRefreshRuntimeDecision = MvRefreshRuntimeDecision;

/// Why a refresh was made runnable.  A worker does not reinterpret this as a
/// retry policy; it is purely observable scheduling state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScheduledRefreshReason {
    Interval,
    SnapshotChange,
}

/// The complete, reproducible scheduling interpretation of lake-authoritative
/// desired refresh semantics and its published projection. Runtime queue,
/// activity, and failure-backoff gates deliberately do not enter this value:
/// wipe-start equivalence compares this decision directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvSchedulerSemanticDecision {
    Paused,
    Manual,
    IntervalNotDue { eligible_at_ms: i64 },
    IntervalDue,
    OnChangeNotDue,
    OnChangeDue,
    Invalid { reason: String },
}

/// Derive scheduler eligibility from the complete durable semantics.
///
/// `current_base_snapshots` is required only for `ASYNC_ON_CHANGE`, where it
/// must be one exact provider observation captured for this decision. The
/// caller owns observation failures; this pure function only diagnoses absent
/// or malformed semantic inputs.
pub(crate) fn mv_scheduler_semantic_decision(
    refresh: &MvRefreshDesiredConfiguration,
    publication: &MvPublishedProjection,
    now_ms: i64,
    current_base_snapshots: Option<&BTreeMap<String, Option<i64>>>,
) -> MvSchedulerSemanticDecision {
    if refresh.paused {
        return MvSchedulerSemanticDecision::Paused;
    }
    if let Err(error) = refresh.validate() {
        return MvSchedulerSemanticDecision::Invalid { reason: error };
    }

    match &refresh.policy {
        MvDesiredRefreshPolicy::Manual => MvSchedulerSemanticDecision::Manual,
        MvDesiredRefreshPolicy::AsyncInterval => {
            let interval_ms = refresh.interval_ms.expect("validated above");
            match publication {
                MvPublishedProjection::NeverPublished => MvSchedulerSemanticDecision::IntervalDue,
                MvPublishedProjection::Published(MvPublishedWaterline {
                    last_refresh_ms, ..
                }) => {
                    let eligible_at_ms = last_refresh_ms.saturating_add(interval_ms);
                    if now_ms >= eligible_at_ms {
                        MvSchedulerSemanticDecision::IntervalDue
                    } else {
                        MvSchedulerSemanticDecision::IntervalNotDue { eligible_at_ms }
                    }
                }
            }
        }
        MvDesiredRefreshPolicy::AsyncOnChange => {
            let Some(current_base_snapshots) = current_base_snapshots else {
                return MvSchedulerSemanticDecision::Invalid {
                    reason:
                        "ASYNC_ON_CHANGE scheduler decision requires exact current base snapshots"
                            .to_string(),
                };
            };
            match publication {
                MvPublishedProjection::NeverPublished => MvSchedulerSemanticDecision::OnChangeDue,
                MvPublishedProjection::Published(MvPublishedWaterline {
                    base_snapshots, ..
                }) if current_base_snapshots_match(base_snapshots, current_base_snapshots) => {
                    MvSchedulerSemanticDecision::OnChangeNotDue
                }
                MvPublishedProjection::Published(_) => MvSchedulerSemanticDecision::OnChangeDue,
            }
        }
    }
}

/// A request that has passed scheduling admission but has not yet acquired the
/// shared per-MV activity gate.  Waiting on that gate must not consume a
/// refresh concurrency slot; the worker calls `mark_started` only after it has
/// acquired the gate and its refresh permit.
#[derive(Clone, Debug)]
pub(crate) struct ScheduledRefreshRequest {
    pub(crate) definition: StoredMvDefinition,
    pub(crate) target: MvTarget,
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) reason: ScheduledRefreshReason,
}

/// The worker-owned execution seam.  Implementations acquire the activity
/// gate, create the bounded request context, resolve/prepares Core steps, and
/// run the existing frontend refresh lifecycle.  They return only a typed
/// terminal result to the scheduler; scheduling policy never inspects a
/// display string from that work.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) trait ScheduledRefreshRunner: Send + Sync {
    fn execute(&self, request: ScheduledRefreshRequest) -> ScheduledRefreshDisposition;
}

#[derive(Debug)]
pub(crate) struct FrontendMvScheduler {
    runtime: MvRefreshProductRuntime<
        i64,
        novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision,
        ScheduledRefreshRequest,
    >,
}

impl FrontendMvScheduler {
    pub(crate) fn new(config: MvSchedulerConfig) -> Self {
        Self {
            runtime: MvRefreshProductRuntime::new(config),
        }
    }

    /// Discover due projections and return
    /// as many queued requests as the worker may start.  The returned requests
    /// are still pending: callers must acquire the activity gate before calling
    /// [`Self::mark_started`], which is what actually consumes capacity.
    pub(crate) fn poll(
        &mut self,
        readiness: &MvReadinessPort,
        engine: &dyn MvBackgroundEngine,
        now_ms: i64,
    ) -> Result<Vec<ScheduledRefreshRequest>, MvRepositoryError> {
        if !self.runtime.enabled() {
            return Ok(Vec::new());
        }

        for projection in readiness.list_ready_projections()? {
            self.consider_definition(engine, projection.definition, now_ms);
        }

        Ok(self.runtime.take_ready())
    }

    /// Record that a worker has obtained both its FIFO activity lease and a
    /// refresh permit.  A worker that cannot acquire either simply lets its
    /// request be considered again on the next poll, without holding capacity.
    pub(crate) fn mark_started(&mut self, mv_id: i64) -> bool {
        self.runtime.mark_started(&mv_id)
    }

    /// Return a dispatched-but-not-started request to the tail of its
    /// coalesced queue.  Worker runtimes call this when the shared activity
    /// gate is busy; no scheduler capacity was acquired in that case.
    pub(crate) fn requeue(&mut self, request: ScheduledRefreshRequest) {
        self.runtime.requeue(request.definition.mv_id, request);
    }

    /// Apply a typed terminal outcome and release the refresh concurrency slot.
    /// Refresh watermark advancement belongs to the existing refresh finalize
    /// path, so this method changes process-local scheduler runtime only.
    pub(crate) fn complete(
        &mut self,
        request: &ScheduledRefreshRequest,
        disposition: ScheduledRefreshDisposition,
        now_ms: i64,
    ) -> Result<ScheduledRefreshRuntimeDecision, MvRepositoryError> {
        Ok(self
            .runtime
            .complete(&request.definition.mv_id, disposition, now_ms))
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn pending_len(&self) -> usize {
        self.runtime.pending_len()
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn running_len(&self) -> usize {
        self.runtime.running_len()
    }

    fn consider_definition(
        &mut self,
        engine: &dyn MvBackgroundEngine,
        definition: StoredMvDefinition,
        now_ms: i64,
    ) {
        if !self.runtime.begin_observation(
            definition.mv_id,
            definition.source_revision.clone(),
            now_ms,
        ) || definition.refresh_paused
        {
            return;
        }

        let target = match mv_target(&definition) {
            Ok(target) => target,
            Err(disposition) => {
                self.record_runtime_disposition(&definition, disposition, now_ms);
                return;
            }
        };
        let refresh = match desired_refresh_configuration(&definition) {
            Ok(refresh) => refresh,
            Err(error) => {
                self.record_runtime_disposition(
                    &definition,
                    ScheduledRefreshDisposition::InvalidDefinition(error),
                    now_ms,
                );
                return;
            }
        };
        let publication = match published_projection(&definition) {
            Ok(publication) => publication,
            Err(error) => {
                self.record_runtime_disposition(
                    &definition,
                    ScheduledRefreshDisposition::InvalidDefinition(error),
                    now_ms,
                );
                return;
            }
        };
        let current_base_snapshots =
            if matches!(&refresh.policy, MvDesiredRefreshPolicy::AsyncOnChange) {
                match engine.current_base_snapshots(&target) {
                    Ok(current) => Some(current),
                    Err(error) => {
                        self.record_runtime_disposition(
                            &definition,
                            ScheduledRefreshDisposition::from_background_error(error),
                            now_ms,
                        );
                        return;
                    }
                }
            } else {
                None
            };
        let reason = match mv_scheduler_semantic_decision(
            &refresh,
            &publication,
            now_ms,
            current_base_snapshots.as_ref(),
        ) {
            MvSchedulerSemanticDecision::IntervalDue => ScheduledRefreshReason::Interval,
            MvSchedulerSemanticDecision::OnChangeDue => ScheduledRefreshReason::SnapshotChange,
            MvSchedulerSemanticDecision::Paused
            | MvSchedulerSemanticDecision::Manual
            | MvSchedulerSemanticDecision::IntervalNotDue { .. }
            | MvSchedulerSemanticDecision::OnChangeNotDue => return,
            MvSchedulerSemanticDecision::Invalid { reason } => {
                self.record_runtime_disposition(
                    &definition,
                    ScheduledRefreshDisposition::InvalidDefinition(reason),
                    now_ms,
                );
                return;
            }
        };
        self.runtime.enqueue(
            definition.mv_id,
            ScheduledRefreshRequest {
                definition: definition.clone(),
                target,
                reason,
            },
        );
    }

    fn record_runtime_disposition(
        &mut self,
        definition: &StoredMvDefinition,
        disposition: ScheduledRefreshDisposition,
        now_ms: i64,
    ) {
        let _ = self.runtime.record(&definition.mv_id, disposition, now_ms);
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

/// Translate the accelerator projection into the complete publication fact
/// consumed by the semantic decision. Partial fields are invalid: no scheduler
/// path may fabricate a published waterline.
fn desired_refresh_configuration(
    definition: &StoredMvDefinition,
) -> Result<MvRefreshDesiredConfiguration, String> {
    MvRefreshDesiredConfiguration::new(
        definition.refresh_policy.clone(),
        definition.refresh_paused,
        definition.refresh_interval_ms,
        definition.max_staleness_ms,
    )
}

fn published_projection(definition: &StoredMvDefinition) -> Result<MvPublishedProjection, String> {
    let values = (
        definition.last_refresh_ms,
        definition.last_refresh_rows,
        definition.last_refreshed_iceberg_snapshot_id,
    );
    match values {
        (None, None, None)
            if definition.last_refresh_snapshots.is_empty()
                && definition.last_refresh_table_object_ids.is_empty() =>
        {
            Ok(MvPublishedProjection::NeverPublished)
        }
        (Some(last_refresh_ms), Some(last_refresh_rows), Some(last_refreshed_iceberg_snapshot_id))
            if definition.last_refresh_snapshots.keys().eq(
                definition.last_refresh_table_object_ids.keys(),
            ) =>
        {
            if last_refresh_ms < 0
                || last_refresh_rows < 0
                || last_refreshed_iceberg_snapshot_id < 0
                || definition
                    .last_refresh_snapshots
                    .values()
                    .any(|snapshot| *snapshot < 0)
            {
                return Err("published MV scheduler projection contains a negative value".to_string());
            }
            Ok(MvPublishedProjection::Published(MvPublishedWaterline {
                last_refresh_ms,
                last_refresh_rows,
                last_refreshed_iceberg_snapshot_id,
                base_snapshots: definition.last_refresh_snapshots.clone(),
                base_table_object_ids: definition.last_refresh_table_object_ids.clone(),
            }))
        }
        _ => Err(
            "MV scheduler projection is neither complete published state nor complete never-published state"
                .to_string(),
        ),
    }
}

fn current_base_snapshots_match(
    published: &BTreeMap<String, i64>,
    current: &BTreeMap<String, Option<i64>>,
) -> bool {
    published.len() == current.len()
        && current
            .iter()
            .all(|(base, snapshot)| published.get(base).copied() == *snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };

    fn definition(policy: MvDesiredRefreshPolicy) -> StoredMvDefinition {
        let refresh_interval_ms =
            matches!(&policy, MvDesiredRefreshPolicy::AsyncInterval).then_some(100);
        StoredMvDefinition {
            mv_id: 7,
            query_definition: PersistedQueryDefinition::new(
                "SELECT 1",
                PersistedQueryDialect::StarRocks,
                "iceberg",
                "db",
            )
            .unwrap(),
            base_table_refs: vec!["iceberg.db.base".to_string()],
            primary_key_columns: Vec::new(),
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("iceberg".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_object_ids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_policy: policy,
            refresh_paused: false,
            refresh_interval_ms,
            max_staleness_ms: None,
            created_at_ms: 1,
            source_revision:
                novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision {
                    target_object_id: novarocks_spi::connector::ConnectorTableObjectId::try_new(
                        Bytes::from_static(b"scheduler-test-target"),
                    )
                    .expect("valid object ID"),
                    descriptor_content_hash: "test-descriptor".to_string(),
                    current_target_snapshot_id: None,
                },
        }
    }

    #[test]
    fn never_published_async_on_change_is_due_for_an_exact_empty_observation() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &refresh,
                &MvPublishedProjection::NeverPublished,
                100,
                Some(&BTreeMap::new()),
            ),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }

    #[test]
    fn on_change_compares_exact_current_vector_to_complete_published_projection() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        let published = MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 10,
            last_refresh_rows: 1,
            last_refreshed_iceberg_snapshot_id: 20,
            base_snapshots: BTreeMap::from([("iceberg.db.base".to_string(), 11)]),
            base_table_object_ids: BTreeMap::new(),
        });
        let same = BTreeMap::from([("iceberg.db.base".to_string(), Some(11))]);
        let changed = BTreeMap::from([("iceberg.db.base".to_string(), Some(12))]);
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 100, Some(&same)),
            MvSchedulerSemanticDecision::OnChangeNotDue
        );
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 100, Some(&changed)),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }

    #[test]
    fn interval_uses_published_refresh_timestamp_not_runtime_next_run_state() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            false,
            Some(100),
            None,
        )
        .expect("valid desired refresh");
        let published = MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 1_000,
            last_refresh_rows: 1,
            last_refreshed_iceberg_snapshot_id: 20,
            base_snapshots: BTreeMap::new(),
            base_table_object_ids: BTreeMap::new(),
        });
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 1_099, None),
            MvSchedulerSemanticDecision::IntervalNotDue {
                eligible_at_ms: 1_100,
            }
        );
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 1_100, None),
            MvSchedulerSemanticDecision::IntervalDue
        );
    }

    #[test]
    fn paused_manual_and_missing_on_change_observation_are_not_reinterpreted() {
        let paused = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            true,
            Some(100),
            None,
        )
        .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &paused,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Paused
        );

        let manual =
            MvRefreshDesiredConfiguration::new(MvDesiredRefreshPolicy::Manual, false, None, None)
                .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &manual,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Manual
        );

        let on_change = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        assert!(matches!(
            mv_scheduler_semantic_decision(
                &on_change,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Invalid { .. }
        ));
    }

    #[test]
    fn coalescing_never_queues_the_same_mv_twice() {
        let config = MvSchedulerConfig::new(true, 30_000, 1, 60_000, 1_800_000);
        let mut scheduler = FrontendMvScheduler::new(config);
        let definition = definition(MvDesiredRefreshPolicy::AsyncInterval);
        let target = mv_target(&definition).expect("target");
        scheduler.runtime.enqueue(
            definition.mv_id,
            ScheduledRefreshRequest {
                definition: definition.clone(),
                target: target.clone(),
                reason: ScheduledRefreshReason::Interval,
            },
        );
        assert_eq!(scheduler.pending_len(), 1);
        assert_eq!(target.display_name(), "iceberg.db.mv");
    }

    #[test]
    fn every_typed_disposition_has_an_explicit_runtime_decision() {
        let config = MvSchedulerConfig::new(true, 30_000, 1, 10, 40);
        let mut scheduler = FrontendMvScheduler::new(config);
        let definition = definition(MvDesiredRefreshPolicy::AsyncInterval);
        assert!(matches!(
            scheduler.runtime.record(
                &definition.mv_id,
                ScheduledRefreshDisposition::Completed,
                100
            ),
            ScheduledRefreshRuntimeDecision::Success
        ));
        assert!(matches!(
            scheduler
                .runtime
                .record(&definition.mv_id, ScheduledRefreshDisposition::NoOp, 100),
            ScheduledRefreshRuntimeDecision::Success
        ));
        assert_eq!(
            scheduler.runtime.record(
                &definition.mv_id,
                ScheduledRefreshDisposition::TransientUnavailable("offline".to_string()),
                100,
            ),
            ScheduledRefreshRuntimeDecision::TransientBackoff {
                error: "offline".to_string(),
                retry_at_ms: 110,
            }
        );
        for disposition in [
            ScheduledRefreshDisposition::InvalidDefinition("bad".to_string()),
            ScheduledRefreshDisposition::TerminalFailure("terminal".to_string()),
            ScheduledRefreshDisposition::Corruption("corrupt".to_string()),
            ScheduledRefreshDisposition::InvariantViolation("invariant".to_string()),
        ] {
            assert!(matches!(
                scheduler
                    .runtime
                    .record(&definition.mv_id, disposition, 100),
                ScheduledRefreshRuntimeDecision::Blocked { .. }
            ));
        }
        for disposition in [
            ScheduledRefreshDisposition::AlreadyActive,
            ScheduledRefreshDisposition::TargetGone,
            ScheduledRefreshDisposition::ShutdownCancelled,
        ] {
            assert_eq!(
                scheduler
                    .runtime
                    .record(&definition.mv_id, disposition, 100),
                ScheduledRefreshRuntimeDecision::NoChange
            );
        }
    }
}
