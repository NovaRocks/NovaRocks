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

//! Product policy and scheduling over validated lake document projections.

use crate::freshness::exact_inputs_match;
use std::collections::BTreeMap;

use crate::persistence::codec::{
    ConfigurationDocument, PublicationInput, RefreshPolicy, RelationOccurrence,
};
use crate::persistence::definition::MvAcceleratorSourceRevision;
use crate::persistence::keyed_documents::StoredKeyedMvProjection;
use crate::persistence::validation::validate_configuration;
use crate::product::MvTarget;
use crate::scheduler_runtime::{
    MvRefreshDisposition, MvRefreshProductRuntime, MvRefreshRuntimeDecision,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvSchedulerSemanticDecision {
    Paused,
    Manual,
    IntervalNotDue { eligible_at_ms: u64 },
    IntervalDue,
    OnChangeNotDue,
    OnChangeDue,
    Invalid { reason: String },
}

fn semantic_decision(
    configuration: &ConfigurationDocument,
    publication: Option<(u64, &[PublicationInput])>,
    now_ms: i64,
    current_inputs: Option<&[PublicationInput]>,
) -> MvSchedulerSemanticDecision {
    if let Err(error) = validate_configuration(configuration) {
        return MvSchedulerSemanticDecision::Invalid {
            reason: error.to_string(),
        };
    }
    if configuration.paused {
        return MvSchedulerSemanticDecision::Paused;
    }
    let Ok(now_ms) = u64::try_from(now_ms) else {
        return MvSchedulerSemanticDecision::Invalid {
            reason: "MV scheduler time must not be negative".to_string(),
        };
    };
    match configuration.refresh_policy {
        RefreshPolicy::Manual => MvSchedulerSemanticDecision::Manual,
        RefreshPolicy::AsyncInterval => {
            let Some(publication) = publication else {
                return MvSchedulerSemanticDecision::IntervalDue;
            };
            // This is P's frozen publication-fact time, not a claimed exact
            // provider commit completion time or an Accelerator insertion time.
            let eligible_at_ms = publication.0.saturating_add(
                configuration
                    .refresh_interval_ms
                    .expect("validated interval"),
            );
            if now_ms >= eligible_at_ms {
                MvSchedulerSemanticDecision::IntervalDue
            } else {
                MvSchedulerSemanticDecision::IntervalNotDue { eligible_at_ms }
            }
        }
        RefreshPolicy::AsyncOnChange => {
            let Some(current_inputs) = current_inputs else {
                return MvSchedulerSemanticDecision::Invalid {
                    reason: "ASYNC_ON_CHANGE requires exact current source occurrences".to_string(),
                };
            };
            if publication.is_some_and(|published| exact_inputs_match(published.1, current_inputs))
            {
                MvSchedulerSemanticDecision::OnChangeNotDue
            } else {
                MvSchedulerSemanticDecision::OnChangeDue
            }
        }
    }
}

fn validate_current_inputs(
    occurrences: &[RelationOccurrence],
    inputs: &[PublicationInput],
) -> Result<(), String> {
    let expected = occurrences
        .iter()
        .map(|item| (item.occurrence_id, item))
        .collect::<BTreeMap<_, _>>();
    if expected.len() != occurrences.len() || inputs.len() != occurrences.len() {
        return Err("MV current inputs do not cover every definition occurrence".to_string());
    }
    let mut seen = std::collections::BTreeSet::new();
    for input in inputs {
        let occurrence = expected
            .get(&input.relation_occurrence_id)
            .ok_or_else(|| "MV current input names an unknown definition occurrence".to_string())?;
        if !seen.insert(input.relation_occurrence_id) {
            return Err("MV current inputs repeat a definition occurrence".to_string());
        }
        if input.object_id != occurrence.object_id {
            return Err("MV current input belongs to a replaced source object".to_string());
        }
    }
    Ok(())
}

pub fn mv_keyed_scheduler_semantic_decision(
    facts: &crate::persistence::keyed_documents::MvKeyedDocumentProjection,
    now: i64,
    inputs: Option<&[PublicationInput]>,
) -> MvSchedulerSemanticDecision {
    if let Some(inputs) = inputs
        && let Err(reason) =
            validate_current_inputs(&facts.definition().relation_occurrences, inputs)
    {
        return MvSchedulerSemanticDecision::Invalid { reason };
    }
    semantic_decision(
        facts.configuration(),
        facts
            .publication()
            .map(|p| (p.publication_prepared_at_ms, p.inputs.as_slice())),
        now,
        inputs,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvScheduledRefreshReason {
    Interval,
    SourceChange,
}

#[derive(Clone, Debug)]
pub struct MvScheduledRefreshRequest {
    projection: StoredKeyedMvProjection,
    target: MvTarget,
    reason: MvScheduledRefreshReason,
}

impl MvScheduledRefreshRequest {
    pub fn projection(&self) -> &StoredKeyedMvProjection {
        &self.projection
    }
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub const fn reason(&self) -> MvScheduledRefreshReason {
        self.reason
    }
}

/// Frozen D/C/P facts whose Current source observation is still outstanding.
pub struct MvCurrentInputObservation {
    projection: StoredKeyedMvProjection,
    target: MvTarget,
}

impl MvCurrentInputObservation {
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub fn projection(&self) -> &StoredKeyedMvProjection {
        &self.projection
    }
    pub fn occurrences(&self) -> &[RelationOccurrence] {
        &self.projection.facts.definition().relation_occurrences
    }
}

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

    pub fn observe_projection(
        &mut self,
        projection: StoredKeyedMvProjection,
        now_ms: i64,
    ) -> Option<MvCurrentInputObservation> {
        if !self.runtime.enabled()
            || !self.runtime.begin_observation(
                projection.mv_id,
                projection.facts.source_revision().clone(),
                now_ms,
            )
            || projection.facts.configuration().paused
        {
            return None;
        }
        let target = &projection.facts.source_revision().target;
        let target = MvTarget::from_parts(
            Some(target.instance_id.as_str()),
            &target.namespace,
            &target.table,
        );
        if matches!(
            projection.facts.configuration().refresh_policy,
            RefreshPolicy::AsyncOnChange
        ) {
            return Some(MvCurrentInputObservation { projection, target });
        }
        self.apply_semantic_decision(projection, target, None, now_ms);
        None
    }

    pub fn resolve_current_inputs(
        &mut self,
        observation: MvCurrentInputObservation,
        current_inputs: Vec<PublicationInput>,
        now_ms: i64,
    ) {
        self.apply_semantic_decision(
            observation.projection,
            observation.target,
            Some(&current_inputs),
            now_ms,
        );
    }

    pub fn record_observation_failure(
        &mut self,
        observation: &MvCurrentInputObservation,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) {
        if self.runtime.is_current_source(
            &observation.projection.mv_id,
            observation.projection.facts.source_revision(),
        ) {
            let _ = self
                .runtime
                .record(&observation.projection.mv_id, disposition, now_ms);
        }
    }

    pub fn take_ready(&mut self) -> Vec<MvScheduledRefreshRequest> {
        self.runtime.take_ready()
    }
    pub fn mark_started(&mut self, mv_id: i64) -> bool {
        self.runtime.mark_started(&mv_id)
    }
    pub fn requeue(&mut self, request: MvScheduledRefreshRequest) {
        self.runtime.requeue(request.projection.mv_id, request);
    }
    pub fn complete(
        &mut self,
        request: &MvScheduledRefreshRequest,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.runtime
            .complete(&request.projection.mv_id, disposition, now_ms)
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
        projection: StoredKeyedMvProjection,
        target: MvTarget,
        current_inputs: Option<&[PublicationInput]>,
        now_ms: i64,
    ) {
        if !self
            .runtime
            .is_current_source(&projection.mv_id, projection.facts.source_revision())
        {
            return;
        }
        let reason =
            match mv_keyed_scheduler_semantic_decision(&projection.facts, now_ms, current_inputs) {
                MvSchedulerSemanticDecision::IntervalDue => MvScheduledRefreshReason::Interval,
                MvSchedulerSemanticDecision::OnChangeDue => MvScheduledRefreshReason::SourceChange,
                MvSchedulerSemanticDecision::Paused
                | MvSchedulerSemanticDecision::Manual
                | MvSchedulerSemanticDecision::IntervalNotDue { .. }
                | MvSchedulerSemanticDecision::OnChangeNotDue => return,
                MvSchedulerSemanticDecision::Invalid { reason } => {
                    self.runtime.record(
                        &projection.mv_id,
                        MvRefreshDisposition::InvalidDefinition(reason.into()),
                        now_ms,
                    );
                    return;
                }
            };
        self.runtime.enqueue(
            projection.mv_id,
            MvScheduledRefreshRequest {
                projection,
                target,
                reason,
            },
        );
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
#[path = "scheduler_tests.rs"]
mod tests;
