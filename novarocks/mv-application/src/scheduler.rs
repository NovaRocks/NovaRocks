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

//! Product policy configuration for asynchronous MV refresh scheduling.

use std::collections::BTreeMap;

use crate::persistence::definition::{
    MvAcceleratorSourceRevision, MvDesiredRefreshPolicy, StoredMvDefinition,
};
use crate::persistence::semantic::MvRefreshDesiredConfiguration;
use crate::product::MvTarget;
use crate::repository::{MvPublishedProjection, MvPublishedWaterline};
use crate::scheduler_runtime::{
    MvRefreshDisposition, MvRefreshProductRuntime, MvRefreshRuntimeDecision,
};

/// The semantic result of interpreting one durable refresh policy against its
/// exact published and current-base facts. Provider observation remains an
/// outer adapter responsibility; this decision performs no I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvSchedulerSemanticDecision {
    Paused,
    Manual,
    IntervalNotDue { eligible_at_ms: i64 },
    IntervalDue,
    OnChangeNotDue,
    OnChangeDue,
    Invalid { reason: String },
}

/// Derive scheduler eligibility from complete durable semantics. For
/// `ASYNC_ON_CHANGE`, the caller must pass one exact provider observation.
pub fn mv_scheduler_semantic_decision(
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

fn current_base_snapshots_match(
    published: &BTreeMap<String, i64>,
    current: &BTreeMap<String, Option<i64>>,
) -> bool {
    published.len() == current.len()
        && current
            .iter()
            .all(|(base, snapshot)| published.get(base).copied() == *snapshot)
}

/// Why a refresh is runnable. Effect adapters may observe this fact but must
/// not reinterpret it as a retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvScheduledRefreshReason {
    Interval,
    SnapshotChange,
}

/// A product-admitted refresh that has not yet acquired the shared activity
/// lease. Waiting for that lease must not consume scheduler capacity.
#[derive(Clone, Debug)]
pub struct MvScheduledRefreshRequest {
    definition: StoredMvDefinition,
    target: MvTarget,
    reason: MvScheduledRefreshReason,
}

impl MvScheduledRefreshRequest {
    pub fn definition(&self) -> &StoredMvDefinition {
        &self.definition
    }

    pub fn target(&self) -> &MvTarget {
        &self.target
    }

    pub const fn reason(&self) -> MvScheduledRefreshReason {
        self.reason
    }
}

/// A provider observation required only for an `ASYNC_ON_CHANGE` definition.
/// The product freezes its durable semantics before the outer adapter reads
/// current provider state.
pub struct MvCurrentBaseSnapshotObservation {
    definition: StoredMvDefinition,
    target: MvTarget,
    refresh: MvRefreshDesiredConfiguration,
    publication: MvPublishedProjection,
}

impl MvCurrentBaseSnapshotObservation {
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
}

/// The one process-local owner of refresh queueing, source-revision reset,
/// semantic interpretation, backoff and terminal transitions. Outer adapters
/// supply only exact provider observations and effect outcomes.
#[derive(Debug)]
pub struct MvRefreshScheduler {
    runtime: MvRefreshProductRuntime<i64, MvAcceleratorSourceRevision, MvScheduledRefreshRequest>,
}

impl MvRefreshScheduler {
    pub fn new(config: MvSchedulerConfig) -> Self {
        Self {
            runtime: MvRefreshProductRuntime::new(config),
        }
    }

    pub const fn enabled(&self) -> bool {
        self.runtime.enabled()
    }

    /// Consume one ready durable projection. A returned observation is the
    /// only case where the outer adapter may read provider snapshots.
    pub fn observe_definition(
        &mut self,
        definition: StoredMvDefinition,
        now_ms: i64,
    ) -> Option<MvCurrentBaseSnapshotObservation> {
        if !self.runtime.enabled()
            || !self.runtime.begin_observation(
                definition.mv_id,
                definition.source_revision.clone(),
                now_ms,
            )
            || definition.refresh_paused
        {
            return None;
        }

        let target = match scheduler_target(&definition) {
            Ok(target) => target,
            Err(disposition) => {
                self.record_definition(&definition, disposition, now_ms);
                return None;
            }
        };
        let refresh = match scheduler_refresh_configuration(&definition) {
            Ok(refresh) => refresh,
            Err(error) => {
                self.record_definition(
                    &definition,
                    MvRefreshDisposition::InvalidDefinition(error),
                    now_ms,
                );
                return None;
            }
        };
        let publication = match scheduler_published_projection(&definition) {
            Ok(publication) => publication,
            Err(error) => {
                self.record_definition(
                    &definition,
                    MvRefreshDisposition::InvalidDefinition(error),
                    now_ms,
                );
                return None;
            }
        };
        if matches!(&refresh.policy, MvDesiredRefreshPolicy::AsyncOnChange) {
            return Some(MvCurrentBaseSnapshotObservation {
                definition,
                target,
                refresh,
                publication,
            });
        }
        self.apply_semantic_decision(definition, target, refresh, publication, None, now_ms);
        None
    }

    pub fn resolve_current_base_snapshots(
        &mut self,
        observation: MvCurrentBaseSnapshotObservation,
        current_base_snapshots: BTreeMap<String, Option<i64>>,
        now_ms: i64,
    ) {
        self.apply_semantic_decision(
            observation.definition,
            observation.target,
            observation.refresh,
            observation.publication,
            Some(&current_base_snapshots),
            now_ms,
        );
    }

    pub fn record_observation_failure(
        &mut self,
        observation: &MvCurrentBaseSnapshotObservation,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) {
        self.record_definition(&observation.definition, disposition, now_ms);
    }

    pub fn take_ready(&mut self) -> Vec<MvScheduledRefreshRequest> {
        self.runtime.take_ready()
    }

    pub fn mark_started(&mut self, mv_id: i64) -> bool {
        self.runtime.mark_started(&mv_id)
    }

    pub fn requeue(&mut self, request: MvScheduledRefreshRequest) {
        self.runtime.requeue(request.definition.mv_id, request);
    }

    pub fn complete(
        &mut self,
        request: &MvScheduledRefreshRequest,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.runtime
            .complete(&request.definition.mv_id, disposition, now_ms)
    }

    pub fn record(
        &mut self,
        mv_id: i64,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.runtime.record(&mv_id, disposition, now_ms)
    }

    fn apply_semantic_decision(
        &mut self,
        definition: StoredMvDefinition,
        target: MvTarget,
        refresh: MvRefreshDesiredConfiguration,
        publication: MvPublishedProjection,
        current_base_snapshots: Option<&BTreeMap<String, Option<i64>>>,
        now_ms: i64,
    ) {
        let reason = match mv_scheduler_semantic_decision(
            &refresh,
            &publication,
            now_ms,
            current_base_snapshots,
        ) {
            MvSchedulerSemanticDecision::IntervalDue => MvScheduledRefreshReason::Interval,
            MvSchedulerSemanticDecision::OnChangeDue => MvScheduledRefreshReason::SnapshotChange,
            MvSchedulerSemanticDecision::Paused
            | MvSchedulerSemanticDecision::Manual
            | MvSchedulerSemanticDecision::IntervalNotDue { .. }
            | MvSchedulerSemanticDecision::OnChangeNotDue => return,
            MvSchedulerSemanticDecision::Invalid { reason } => {
                self.record_definition(
                    &definition,
                    MvRefreshDisposition::InvalidDefinition(reason),
                    now_ms,
                );
                return;
            }
        };
        self.runtime.enqueue(
            definition.mv_id,
            MvScheduledRefreshRequest {
                definition,
                target,
                reason,
            },
        );
    }

    fn record_definition(
        &mut self,
        definition: &StoredMvDefinition,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) {
        let _ = self.runtime.record(&definition.mv_id, disposition, now_ms);
    }
}

fn scheduler_target(definition: &StoredMvDefinition) -> Result<MvTarget, MvRefreshDisposition> {
    match (
        definition.target_catalog.as_deref(),
        definition.target_namespace.as_deref(),
        definition.target_table.as_deref(),
    ) {
        (Some(catalog), Some(namespace), Some(name)) => {
            Ok(MvTarget::from_parts(Some(catalog), namespace, name))
        }
        _ => Err(MvRefreshDisposition::InvalidDefinition(
            "scheduled materialized view is missing its canonical target".to_string(),
        )),
    }
}

fn scheduler_refresh_configuration(
    definition: &StoredMvDefinition,
) -> Result<MvRefreshDesiredConfiguration, String> {
    MvRefreshDesiredConfiguration::new(
        definition.refresh_policy.clone(),
        definition.refresh_paused,
        definition.refresh_interval_ms,
        definition.max_staleness_ms,
    )
}

fn scheduler_published_projection(
    definition: &StoredMvDefinition,
) -> Result<MvPublishedProjection, String> {
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
            if definition
                .last_refresh_snapshots
                .keys()
                .eq(definition.last_refresh_table_object_ids.keys()) =>
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

/// Frozen process-local policy for asynchronous materialized-view refresh.
///
/// It does not own a queue, thread, provider handle, or persisted record. The
/// role-local scheduler owns those resources and consumes these product bounds
/// after configuration has been resolved once at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvSchedulerConfig {
    enabled: bool,
    tick_interval_ms: u64,
    max_concurrent_refreshes: usize,
    failure_backoff_ms: i64,
    max_failure_backoff_ms: i64,
}

impl MvSchedulerConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent_refreshes: usize,
        failure_backoff_ms: i64,
        max_failure_backoff_ms: i64,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent_refreshes,
            failure_backoff_ms,
            max_failure_backoff_ms,
        }
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub const fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    pub const fn max_concurrent_refreshes(&self) -> usize {
        self.max_concurrent_refreshes
    }

    pub const fn failure_backoff_ms(&self) -> i64 {
        self.failure_backoff_ms
    }

    pub const fn max_failure_backoff_ms(&self) -> i64 {
        self.max_failure_backoff_ms
    }
}

impl Default for MvSchedulerConfig {
    fn default() -> Self {
        Self::new(false, 30_000, 1, 60_000, 1_800_000)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::MvRefreshScheduler;
    use super::{MvSchedulerConfig, MvSchedulerSemanticDecision, mv_scheduler_semantic_decision};
    use crate::persistence::definition::{
        MvAcceleratorSourceRevision, MvDesiredRefreshPolicy, StoredMvDefinition,
    };
    use crate::persistence::semantic::MvRefreshDesiredConfiguration;
    use crate::repository::{MvPublishedProjection, MvPublishedWaterline};
    use crate::scheduler_runtime::{MvRefreshDisposition, MvRefreshRuntimeDecision};
    use bytes::Bytes;
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };
    use novarocks_spi::connector::ConnectorTableObjectId;

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
            .expect("valid persisted query"),
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
            source_revision: MvAcceleratorSourceRevision {
                target_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(
                    b"scheduler-test-target",
                ))
                .expect("valid object ID"),
                descriptor_content_hash: "test-descriptor".to_string(),
                current_target_snapshot_id: None,
            },
        }
    }

    #[test]
    fn default_policy_preserves_the_deployed_scheduler_bounds() {
        assert_eq!(
            MvSchedulerConfig::default(),
            MvSchedulerConfig::new(false, 30_000, 1, 60_000, 1_800_000)
        );
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
    fn concrete_scheduler_coalesces_one_due_definition() {
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 60_000, 1_800_000));
        let definition = definition(MvDesiredRefreshPolicy::AsyncInterval);

        assert!(
            scheduler
                .observe_definition(definition.clone(), 100)
                .is_none()
        );
        assert!(scheduler.observe_definition(definition, 100).is_none());

        let ready = scheduler.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].target().catalog(), Some("iceberg"));
        assert_eq!(ready[0].target().namespace(), "db");
        assert_eq!(ready[0].target().name(), "mv");
    }

    #[test]
    fn concrete_scheduler_requires_exact_on_change_observation_before_queueing() {
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 60_000, 1_800_000));
        let observation = scheduler
            .observe_definition(definition(MvDesiredRefreshPolicy::AsyncOnChange), 100)
            .expect("on-change must request an exact provider observation");
        assert!(scheduler.take_ready().is_empty());

        scheduler.resolve_current_base_snapshots(observation, BTreeMap::new(), 100);

        assert_eq!(scheduler.take_ready().len(), 1);
    }

    #[test]
    fn concrete_scheduler_has_explicit_terminal_decisions() {
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));
        assert!(matches!(
            scheduler.record(7, MvRefreshDisposition::Completed, 100),
            MvRefreshRuntimeDecision::Success
        ));
        assert_eq!(
            scheduler.record(
                7,
                MvRefreshDisposition::TransientUnavailable("offline".to_string()),
                100,
            ),
            MvRefreshRuntimeDecision::TransientBackoff {
                error: "offline".to_string(),
                retry_at_ms: 110,
            }
        );
        for disposition in [
            MvRefreshDisposition::InvalidDefinition("bad".to_string()),
            MvRefreshDisposition::TerminalFailure("terminal".to_string()),
            MvRefreshDisposition::Corruption("corrupt".to_string()),
            MvRefreshDisposition::InvariantViolation("invariant".to_string()),
        ] {
            assert!(matches!(
                scheduler.record(7, disposition, 100),
                MvRefreshRuntimeDecision::Blocked { .. }
            ));
        }
    }
}
