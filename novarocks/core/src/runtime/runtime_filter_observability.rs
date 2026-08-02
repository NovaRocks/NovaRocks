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

//! Per-filter runtime-filter lifecycle observability.
//!
//! This module records query-scoped runtime-filter lifecycle events without
//! affecting execution semantics. Low-frequency call sites can use
//! `RfLifecycleRecorder` directly; apply hot paths can cache an
//! `RfLifecycleHandle` and update counters with relaxed atomics only.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::types::UniqueId;
use crate::runtime::profile::RuntimeProfile;
use crate::runtime_filter::port::events::{
    ArtifactMaterializationIdentity, ConsumerEventIdentity, ProducerEventIdentity,
    RouteEventIdentity, RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::identity::{
    ContributionIdentity, DeploymentEpoch, RuntimeFilterParticipantId,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryKey {
    pub hi: i64,
    pub lo: i64,
}

impl QueryKey {
    pub fn from_hi_lo(hi: i64, lo: i64) -> Self {
        Self { hi, lo }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RfDropReason {
    SizeExceeded,
    SendFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RfBuiltInfo {
    pub rows: i64,
    pub bytes: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RfAcquiredInfo {
    pub outcome: String,
    pub latency_ns: i64,
}

#[derive(Default)]
pub struct RuntimeFilterLifecycleRegistry {
    queries: RwLock<HashMap<QueryKey, Arc<QueryRfLifecycle>>>,
}

impl RuntimeFilterLifecycleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global() -> &'static Self {
        runtime_filter_lifecycle_registry()
    }

    pub fn recorder(&self, query: QueryKey) -> RfLifecycleRecorder {
        RfLifecycleRecorder {
            query,
            lifecycle: self.query_entry(query),
        }
    }

    fn event_sink(&self, query: QueryKey) -> RegistryRuntimeFilterEventSink {
        RegistryRuntimeFilterEventSink {
            lifecycle: self.query_entry(query),
        }
    }

    pub fn snapshot(&self, query: QueryKey) -> Option<QueryRfSnapshot> {
        let lifecycle = {
            let guard = rw_read(&self.queries);
            guard.get(&query).cloned()
        };
        lifecycle.map(|lifecycle| lifecycle.snapshot())
    }

    pub fn remove_query(&self, query: QueryKey) {
        rw_write(&self.queries).remove(&query);
    }

    pub fn export_to_profile(&self, query: QueryKey, profile: &RuntimeProfile) {
        let Some(snapshot) = self.snapshot(query) else {
            return;
        };
        if snapshot.filters.is_empty() {
            return;
        }

        let rf_profile = profile.child("RuntimeFilters");
        let mut filters = snapshot.filters.iter().collect::<Vec<_>>();
        filters.sort_by_key(|(filter_id, _)| **filter_id);
        for (filter_id, record) in filters {
            let filter_profile = rf_profile.child(format!("Filter{filter_id}"));
            filter_profile.counter_set_unit("Planned", i64::from(record.planned));
            if let Some(built) = record.built {
                filter_profile.counter_set_unit("BuiltRows", built.rows);
                filter_profile.counter_set_bytes("BuiltBytes", built.bytes);
            }
            filter_profile.counter_set_unit("SentPartials", record.sent_partials);
            filter_profile.counter_set_bytes("SentBytes", record.sent_bytes);
            filter_profile.counter_set_unit("MergedReceived", record.merged_received());
            filter_profile.counter_set_unit("MergedExpected", record.merged_expected());
            filter_profile.counter_set_unit("Delivered", i64::from(record.delivered));
            if let Some(acquired) = record.acquired.as_ref() {
                filter_profile.add_info_string("AcquireOutcome", acquired.outcome.clone());
                filter_profile
                    .add_timer("AcquireLatency")
                    .set(acquired.latency_ns);
            }
            filter_profile.counter_set_unit("AppliedInputRows", record.applied_input_rows());
            filter_profile.counter_set_unit("AppliedOutputRows", record.applied_output_rows());
            filter_profile.counter_set_unit("AppliedEvals", record.applied_evals());
            if !record.drop_reasons.is_empty() {
                filter_profile
                    .add_info_string("Dropped", format_drop_reasons(&record.drop_reasons));
            }
        }
    }

    fn query_entry(&self, query: QueryKey) -> Arc<QueryRfLifecycle> {
        let mut guard = rw_write(&self.queries);
        Arc::clone(
            guard
                .entry(query)
                .or_insert_with(|| Arc::new(QueryRfLifecycle::new())),
        )
    }
}

/// Construct a one-way diagnostic event sink for a Backend-owned participant.
///
/// This is intentionally an observer only: it exposes neither service lookup
/// nor lifecycle mutation, so execution ownership cannot flow back into Core.
pub fn backend_participant_event_sink(query: QueryKey) -> Arc<dyn RuntimeFilterEventSink> {
    Arc::new(RegistryRuntimeFilterEventSink::new(
        RuntimeFilterLifecycleRegistry::global(),
        query,
    ))
}

fn runtime_filter_lifecycle_registry() -> &'static RuntimeFilterLifecycleRegistry {
    static REGISTRY: OnceLock<RuntimeFilterLifecycleRegistry> = OnceLock::new();
    REGISTRY.get_or_init(RuntimeFilterLifecycleRegistry::new)
}

#[derive(Default)]
struct QueryRfLifecycle {
    filters: RwLock<HashMap<i32, Arc<RfLifecycleRecord>>>,
    channel_events: Mutex<BTreeMap<RuntimeFilterChannelEventCoordinate, Vec<RuntimeFilterEvent>>>,
}

impl QueryRfLifecycle {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> QueryRfSnapshot {
        let filters = rw_read(&self.filters)
            .iter()
            .map(|(filter_id, record)| (*filter_id, record.snapshot()))
            .collect();
        QueryRfSnapshot {
            filters,
            channel_events: mutex_lock(&self.channel_events).clone(),
        }
    }

    fn record(&self, filter_id: i32) -> Arc<RfLifecycleRecord> {
        if let Some(record) = {
            let guard = rw_read(&self.filters);
            guard.get(&filter_id).cloned()
        } {
            return record;
        }

        let mut guard = rw_write(&self.filters);
        Arc::clone(
            guard
                .entry(filter_id)
                .or_insert_with(|| Arc::new(RfLifecycleRecord::new())),
        )
    }

    fn record_channel_event(&self, event: RuntimeFilterEvent) {
        mutex_lock(&self.channel_events)
            .entry(RuntimeFilterChannelEventCoordinate::from(&event))
            .or_default()
            .push(event);
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RuntimeFilterChannelEventCoordinate {
    Deployment {
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
    },
    Channel(RuntimeFilterEventIdentity),
    Contribution(ContributionIdentity),
    Producer(ProducerEventIdentity),
    Materialization(ArtifactMaterializationIdentity),
    Route(RouteEventIdentity),
    Consumer(ConsumerEventIdentity),
}

impl From<&RuntimeFilterEvent> for RuntimeFilterChannelEventCoordinate {
    fn from(event: &RuntimeFilterEvent) -> Self {
        match event {
            RuntimeFilterEvent::DeploymentInstalled {
                query_id,
                participant_id,
                epoch,
            } => Self::Deployment {
                query_id: *query_id,
                participant_id: *participant_id,
                epoch: *epoch,
            },
            RuntimeFilterEvent::ChannelPlanned { identity }
            | RuntimeFilterEvent::ChannelCompleted { identity, .. }
            | RuntimeFilterEvent::OrderedAvailabilityReached { identity }
            | RuntimeFilterEvent::LogicalVersionPublished { identity, .. }
            | RuntimeFilterEvent::ChannelCompletedWithoutArtifact { identity }
            | RuntimeFilterEvent::ChannelLogicalDegraded { identity, .. }
            | RuntimeFilterEvent::ChannelUnavailable { identity, .. }
            | RuntimeFilterEvent::ChannelCancelled { identity } => Self::Channel(*identity),
            RuntimeFilterEvent::DeltaAccepted { identity }
            | RuntimeFilterEvent::DeltaDuplicateIgnored { identity }
            | RuntimeFilterEvent::FinalDomainShardAccepted { identity }
            | RuntimeFilterEvent::FinalDomainShardDuplicate { identity }
            | RuntimeFilterEvent::FinalDomainShardRejected { identity, .. }
            | RuntimeFilterEvent::OrderedUpdateStale { identity }
            | RuntimeFilterEvent::OrderedUpdateApplied { identity }
            | RuntimeFilterEvent::OrderedUpdateRejected { identity, .. }
            | RuntimeFilterEvent::OrderedUpdateEqual { identity }
            | RuntimeFilterEvent::OrderedStreamTightened { identity }
            | RuntimeFilterEvent::TopKSummaryStale { identity }
            | RuntimeFilterEvent::TopKSummaryApplied { identity }
            | RuntimeFilterEvent::TopKSummaryRejected { identity, .. }
            | RuntimeFilterEvent::TopKSummaryEqual { identity }
            | RuntimeFilterEvent::TopKStreamUpdated { identity }
            | RuntimeFilterEvent::OrderedGlobalTightened { identity, .. }
            | RuntimeFilterEvent::SequenceGapObserved { identity } => Self::Contribution(*identity),
            RuntimeFilterEvent::ProducerInstanceClosed { identity }
            | RuntimeFilterEvent::ProducerInstanceFailed { identity, .. } => {
                Self::Producer(*identity)
            }
            RuntimeFilterEvent::MaterializationStarted { identity }
            | RuntimeFilterEvent::ArtifactMaterialized { identity, .. }
            | RuntimeFilterEvent::ArtifactPublished { identity, .. }
            | RuntimeFilterEvent::ArtifactPublishStaleSkipped { identity }
            | RuntimeFilterEvent::ArtifactUnsupported { identity, .. }
            | RuntimeFilterEvent::ArtifactUnavailable { identity, .. } => {
                Self::Materialization(*identity)
            }
            RuntimeFilterEvent::LoopbackDelivered { identity, .. } => Self::Route(*identity),
            // A sender-side transport event names a delivery route, not a local consumer
            // instance, so it is grouped under its channel coordinate.
            RuntimeFilterEvent::TransportEnvelope { identity, .. } => {
                Self::Channel(identity.common())
            }
            RuntimeFilterEvent::SubscriptionAcquired { identity, .. }
            | RuntimeFilterEvent::SubscriptionTimedOut { identity }
            | RuntimeFilterEvent::SubscriptionUnavailable { identity, .. }
            | RuntimeFilterEvent::SubscriptionUnsupported { identity, .. }
            | RuntimeFilterEvent::SubscriptionCancelled { identity }
            | RuntimeFilterEvent::LiveSubscriptionUpdated { identity, .. }
            | RuntimeFilterEvent::LiveSubscriptionIdle { identity, .. }
            | RuntimeFilterEvent::LiveSubscriptionTerminal { identity, .. } => {
                Self::Consumer(*identity)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct RegistryRuntimeFilterEventSink {
    lifecycle: Arc<QueryRfLifecycle>,
}

impl RegistryRuntimeFilterEventSink {
    pub(crate) fn new(registry: &RuntimeFilterLifecycleRegistry, query: QueryKey) -> Self {
        registry.event_sink(query)
    }
}

impl RuntimeFilterEventSink for RegistryRuntimeFilterEventSink {
    fn record(&self, event: RuntimeFilterEvent) {
        self.lifecycle.record_channel_event(event);
    }
}

#[derive(Default)]
struct RfLifecycleRecord {
    planned: AtomicBool,
    built: Mutex<Option<RfBuiltInfo>>,
    sent_partials: AtomicI64,
    sent_bytes: AtomicI64,
    merged_received: AtomicI64,
    merged_expected: AtomicI64,
    delivered: AtomicBool,
    acquired: Mutex<Option<RfAcquiredInfo>>,
    applied_input_rows: AtomicI64,
    applied_output_rows: AtomicI64,
    applied_evals: AtomicI64,
    drop_reasons: Mutex<Vec<RfDropReason>>,
    disabled: Mutex<Option<String>>,
}

impl RfLifecycleRecord {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> RfRecordView {
        let drop_reasons = mutex_lock(&self.drop_reasons).clone();
        RfRecordView {
            planned: self.planned.load(Ordering::Relaxed),
            built: *mutex_lock(&self.built),
            sent_partials: self.sent_partials.load(Ordering::Relaxed),
            sent_bytes: self.sent_bytes.load(Ordering::Relaxed),
            merged_received: self.merged_received.load(Ordering::Relaxed),
            merged_expected: self.merged_expected.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            acquired: mutex_lock(&self.acquired).clone(),
            applied_input_rows: self.applied_input_rows.load(Ordering::Relaxed),
            applied_output_rows: self.applied_output_rows.load(Ordering::Relaxed),
            applied_evals: self.applied_evals.load(Ordering::Relaxed),
            dropped: drop_reasons.first().copied(),
            drop_reasons,
            disabled: mutex_lock(&self.disabled).clone(),
        }
    }
}

#[derive(Clone)]
pub struct RfLifecycleRecorder {
    query: QueryKey,
    lifecycle: Arc<QueryRfLifecycle>,
}

impl RfLifecycleRecorder {
    pub fn query(&self) -> QueryKey {
        self.query
    }

    pub fn filter(&self, filter_id: i32) -> RfLifecycleHandle {
        RfLifecycleHandle {
            filter_id,
            record: self.lifecycle.record(filter_id),
        }
    }

    pub fn planned(&self, filter_id: i32) {
        self.filter(filter_id).planned();
    }

    pub fn built(&self, filter_id: i32, rows: i64, bytes: i64) {
        self.filter(filter_id).built(rows, bytes);
    }

    pub fn sent_partial(&self, filter_id: i32, bytes: i64) {
        self.filter(filter_id).sent_partial(bytes);
    }

    pub fn merge_progress(&self, filter_id: i32, received: i64, expected: i64) {
        self.filter(filter_id).merge_progress(received, expected);
    }

    pub fn delivered(&self, filter_id: i32) {
        self.filter(filter_id).delivered();
    }

    pub fn acquired(&self, filter_id: i32, outcome: impl Into<String>, latency_ns: i64) {
        self.filter(filter_id).acquired(outcome, latency_ns);
    }

    pub fn applied(&self, filter_id: i32, input_rows: i64, output_rows: i64, evals: i64) {
        self.filter(filter_id)
            .applied(input_rows, output_rows, evals);
    }

    pub fn dropped(&self, filter_id: i32, reason: RfDropReason) {
        self.filter(filter_id).dropped(reason);
    }
}

#[derive(Clone)]
pub struct RfLifecycleHandle {
    filter_id: i32,
    record: Arc<RfLifecycleRecord>,
}

impl RfLifecycleHandle {
    pub fn filter_id(&self) -> i32 {
        self.filter_id
    }

    pub fn planned(&self) {
        self.record.planned.store(true, Ordering::Relaxed);
    }

    pub fn built(&self, rows: i64, bytes: i64) {
        *mutex_lock(&self.record.built) = Some(RfBuiltInfo { rows, bytes });
    }

    pub fn sent_partial(&self, bytes: i64) {
        self.record.sent_partials.fetch_add(1, Ordering::Relaxed);
        self.record.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn merge_progress(&self, received: i64, expected: i64) {
        self.record
            .merged_received
            .store(received, Ordering::Relaxed);
        self.record
            .merged_expected
            .store(expected, Ordering::Relaxed);
    }

    pub fn delivered(&self) {
        self.record.delivered.store(true, Ordering::Relaxed);
    }

    pub fn acquired(&self, outcome: impl Into<String>, latency_ns: i64) {
        let mut acquired = mutex_lock(&self.record.acquired);
        if acquired.is_none() {
            *acquired = Some(RfAcquiredInfo {
                outcome: outcome.into(),
                latency_ns,
            });
        }
    }

    pub fn applied(&self, input_rows: i64, output_rows: i64, evals: i64) {
        self.record
            .applied_input_rows
            .fetch_add(input_rows, Ordering::Relaxed);
        self.record
            .applied_output_rows
            .fetch_add(output_rows, Ordering::Relaxed);
        self.record
            .applied_evals
            .fetch_add(evals, Ordering::Relaxed);
    }

    pub fn dropped(&self, reason: RfDropReason) {
        let mut drop_reasons = mutex_lock(&self.record.drop_reasons);
        if !drop_reasons.contains(&reason) {
            drop_reasons.push(reason);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryRfSnapshot {
    pub filters: HashMap<i32, RfRecordView>,
    pub(crate) channel_events:
        BTreeMap<RuntimeFilterChannelEventCoordinate, Vec<RuntimeFilterEvent>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RfRecordView {
    pub planned: bool,
    pub built: Option<RfBuiltInfo>,
    pub sent_partials: i64,
    pub sent_bytes: i64,
    merged_received: i64,
    merged_expected: i64,
    pub delivered: bool,
    pub acquired: Option<RfAcquiredInfo>,
    applied_input_rows: i64,
    applied_output_rows: i64,
    applied_evals: i64,
    pub dropped: Option<RfDropReason>,
    pub drop_reasons: Vec<RfDropReason>,
    disabled: Option<String>,
}

impl RfRecordView {
    pub fn merged_received(&self) -> i64 {
        self.merged_received
    }

    pub fn merged_expected(&self) -> i64 {
        self.merged_expected
    }

    pub fn applied_input_rows(&self) -> i64 {
        self.applied_input_rows
    }

    pub fn applied_output_rows(&self) -> i64 {
        self.applied_output_rows
    }

    pub fn applied_evals(&self) -> i64 {
        self.applied_evals
    }

    pub fn drop_reasons(&self) -> &[RfDropReason] {
        &self.drop_reasons
    }

    pub fn has_drop_reason(&self, reason: RfDropReason) -> bool {
        self.drop_reasons.contains(&reason)
    }
}

fn mutex_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn rw_read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn rw_write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn format_drop_reasons(reasons: &[RfDropReason]) -> String {
    reasons
        .iter()
        .map(|reason| match reason {
            RfDropReason::SizeExceeded => "SizeExceeded",
            RfDropReason::SendFailed => "SendFailed",
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::UniqueId;
    use crate::runtime::profile::RuntimeProfile;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerProfileId};
    use crate::runtime_filter::port::events::{
        ArtifactMaterializationIdentity, ConsumerEventIdentity, RouteEventIdentity,
        RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink,
    };
    use crate::runtime_filter::port::identity::{
        ContributionIdentity, DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence,
        ProducerStreamId, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
    use crate::runtime_filter::port::subscription::{
        ArtifactUnsupportedReason, LiveTerminal, UnavailableReason,
    };
    use std::thread;

    fn channel_identity(query: QueryKey) -> RuntimeFilterEventIdentity {
        RuntimeFilterEventIdentity::new(
            UniqueId::new(query.hi, query.lo),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(4),
            DeploymentEpoch::new(5),
        )
    }

    fn channel_events(snapshot: &QueryRfSnapshot) -> Vec<RuntimeFilterEvent> {
        snapshot
            .channel_events
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    #[test]
    fn channel_events_share_existing_query_lifecycle_registry() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(11, 12);
        registry.recorder(query).planned(7);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        let event = RuntimeFilterEvent::ChannelPlanned {
            identity: channel_identity(query),
        };

        sink.record(event.clone());

        let snapshot = registry.snapshot(query).expect("query snapshot");
        assert!(snapshot.filters.get(&7).expect("legacy filter").planned);
        assert_eq!(channel_events(&snapshot), vec![event]);
    }

    #[test]
    fn channel_event_snapshot_preserves_full_stable_identity() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(21, 22);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        let identity = RouteEventIdentity::new(
            channel_identity(query),
            BindingId::new(6),
            UniqueId::new(7, 8),
            RouteEdgeId::new(9),
        );
        let event = RuntimeFilterEvent::LoopbackDelivered {
            identity,
            version: LogicalVersion::FIRST,
        };

        sink.record(event.clone());

        let snapshot = registry.snapshot(query).expect("query snapshot");
        assert_eq!(channel_events(&snapshot), vec![event]);
    }

    #[test]
    fn ordered_applied_and_rejected_events_keep_contribution_coordinates() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(31, 32);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        let common = channel_identity(query);
        let identity = ContributionIdentity::new(
            common.query_id(),
            common.participant_id(),
            common.channel_id(),
            common.epoch(),
            ProducerStreamId::new(BindingId::new(6), UniqueId::new(7, 8), PartitionId::new(9)),
            ProducerSequence::new(10),
        );
        let events = vec![
            RuntimeFilterEvent::OrderedUpdateApplied { identity },
            RuntimeFilterEvent::OrderedUpdateRejected {
                identity,
                violation: RuntimeContractViolationKind::OrderedBoundLoosened,
            },
        ];
        for event in &events {
            sink.record(event.clone());
        }

        let snapshot = registry.snapshot(query).expect("query snapshot");
        assert_eq!(
            snapshot
                .channel_events
                .get(&RuntimeFilterChannelEventCoordinate::Contribution(identity)),
            Some(&events)
        );
    }

    #[test]
    fn topk_summary_input_events_share_one_contribution_coordinate() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(33, 34);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        let common = channel_identity(query);
        let identity = ContributionIdentity::new(
            common.query_id(),
            common.participant_id(),
            common.channel_id(),
            common.epoch(),
            ProducerStreamId::new(BindingId::new(6), UniqueId::new(7, 8), PartitionId::new(9)),
            ProducerSequence::new(10),
        );
        let events = vec![
            RuntimeFilterEvent::TopKSummaryStale { identity },
            RuntimeFilterEvent::TopKSummaryApplied { identity },
            RuntimeFilterEvent::TopKSummaryRejected {
                identity,
                violation: RuntimeContractViolationKind::OrderedBoundLoosened,
            },
            RuntimeFilterEvent::TopKSummaryEqual { identity },
            RuntimeFilterEvent::TopKStreamUpdated { identity },
        ];
        for event in &events {
            sink.record(event.clone());
        }

        let snapshot = registry.snapshot(query).expect("query snapshot");
        assert_eq!(
            snapshot
                .channel_events
                .get(&RuntimeFilterChannelEventCoordinate::Contribution(identity)),
            Some(&events)
        );
    }

    #[test]
    fn materialization_adapter_preserves_terminal_reasons_and_cancel_coordinates() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(23, 24);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        let common = channel_identity(query);
        let materialization = ArtifactMaterializationIdentity::new(
            common,
            ConsumerProfileId::for_test([6; 32]),
            LogicalVersion::new(7),
        );
        let consumer = ConsumerEventIdentity::new(common, BindingId::new(8), UniqueId::new(9, 10));
        let materialization_events = vec![
            RuntimeFilterEvent::MaterializationStarted {
                identity: materialization,
            },
            RuntimeFilterEvent::ArtifactMaterialized {
                identity: materialization,
                kind: ArtifactKind::Bitset,
                bytes: 128,
                digest: [11; 32],
            },
            RuntimeFilterEvent::ArtifactPublished {
                identity: materialization,
                kind: ArtifactKind::Bitset,
                bytes: 128,
                digest: [11; 32],
            },
            RuntimeFilterEvent::ArtifactPublishStaleSkipped {
                identity: materialization,
            },
            RuntimeFilterEvent::ArtifactUnsupported {
                identity: materialization,
                reason: ArtifactUnsupportedReason::RangeDeferred,
            },
            RuntimeFilterEvent::ArtifactUnsupported {
                identity: materialization,
                reason: ArtifactUnsupportedReason::NoAcceptedRepresentation,
            },
            RuntimeFilterEvent::ArtifactUnavailable {
                identity: materialization,
                reason: UnavailableReason::ResourceLimit,
            },
            RuntimeFilterEvent::ArtifactUnavailable {
                identity: materialization,
                reason: UnavailableReason::MaterializationFailed,
            },
        ];
        for event in &materialization_events {
            sink.record(event.clone());
        }
        let channel_cancelled = RuntimeFilterEvent::ChannelCancelled { identity: common };
        sink.record(channel_cancelled.clone());
        let subscription_cancelled =
            RuntimeFilterEvent::SubscriptionCancelled { identity: consumer };
        sink.record(subscription_cancelled.clone());
        let live_events = vec![
            RuntimeFilterEvent::LiveSubscriptionUpdated {
                identity: consumer,
                version: LogicalVersion::new(7),
                terminal: None,
            },
            RuntimeFilterEvent::LiveSubscriptionIdle {
                identity: consumer,
                latest_version: Some(LogicalVersion::new(7)),
                terminal: None,
            },
            RuntimeFilterEvent::LiveSubscriptionTerminal {
                identity: consumer,
                terminal: LiveTerminal::DegradedDelivery(UnavailableReason::RouteUnavailable),
                retained_version: Some(LogicalVersion::new(7)),
            },
        ];
        for event in &live_events {
            sink.record(event.clone());
        }

        let snapshot = registry.snapshot(query).expect("query snapshot");
        assert_eq!(
            snapshot
                .channel_events
                .get(&RuntimeFilterChannelEventCoordinate::Materialization(
                    materialization
                )),
            Some(&materialization_events)
        );
        assert_eq!(
            snapshot
                .channel_events
                .get(&RuntimeFilterChannelEventCoordinate::Channel(common)),
            Some(&vec![channel_cancelled])
        );
        assert_eq!(
            snapshot
                .channel_events
                .get(&RuntimeFilterChannelEventCoordinate::Consumer(consumer)),
            Some(
                &std::iter::once(subscription_cancelled)
                    .chain(live_events)
                    .collect::<Vec<_>>()
            )
        );
    }

    #[test]
    fn legacy_filter_records_remain_unchanged_by_channel_events() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(31, 32);
        let recorder = registry.recorder(query);
        recorder.planned(13);
        recorder.built(13, 3, 128);
        let before = registry.snapshot(query).expect("legacy snapshot").filters;
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);

        sink.record(RuntimeFilterEvent::ChannelPlanned {
            identity: channel_identity(query),
        });

        assert_eq!(
            registry.snapshot(query).expect("combined snapshot").filters,
            before
        );
    }

    #[test]
    fn removing_query_removes_legacy_and_channel_records_together() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let query = QueryKey::from_hi_lo(41, 42);
        registry.recorder(query).planned(17);
        let sink = RegistryRuntimeFilterEventSink::new(&registry, query);
        sink.record(RuntimeFilterEvent::ChannelPlanned {
            identity: channel_identity(query),
        });

        registry.remove_query(query);
        sink.record(RuntimeFilterEvent::ChannelCancelled {
            identity: channel_identity(query),
        });

        assert!(registry.snapshot(query).is_none());
    }

    #[test]
    fn lifecycle_record_accumulates_and_exports() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(1, 2);
        let rec = registry.recorder(q);

        rec.planned(7);
        rec.built(7, 3, 128);
        rec.sent_partial(7, 128);
        rec.merge_progress(7, 1, 3);
        rec.merge_progress(7, 3, 3);
        rec.delivered(7);
        rec.acquired(7, "Complete", 4_000);
        rec.applied(7, 1024, 100, 1);
        rec.applied(7, 1024, 50, 1);
        rec.dropped(9, RfDropReason::SizeExceeded);

        let snap = registry.snapshot(q).expect("query snapshot");
        let f7 = snap.filters.get(&7).expect("filter 7");
        assert!(f7.planned);
        assert_eq!(f7.built.as_ref().map(|b| (b.rows, b.bytes)), Some((3, 128)));
        assert_eq!(f7.sent_partials, 1);
        assert_eq!(f7.sent_bytes, 128);
        assert_eq!(f7.merged_received(), 3);
        assert_eq!(f7.merged_expected(), 3);
        assert!(f7.delivered);
        assert_eq!(
            f7.acquired
                .as_ref()
                .map(|a| (a.outcome.as_str(), a.latency_ns)),
            Some(("Complete", 4_000))
        );
        assert_eq!(f7.applied_input_rows(), 2048);
        assert_eq!(f7.applied_output_rows(), 150);
        assert_eq!(f7.applied_evals(), 2);
        let f9 = snap.filters.get(&9).expect("filter 9");
        assert!(f9.has_drop_reason(RfDropReason::SizeExceeded));
        assert_eq!(f9.drop_reasons(), &[RfDropReason::SizeExceeded]);

        registry.remove_query(q);
        assert!(registry.snapshot(q).is_none());
    }

    #[test]
    fn acquired_preserves_first_record() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(3, 4);
        let rec = registry.recorder(q);

        rec.acquired(7, "complete", 4_000);
        rec.acquired(7, "complete", 0);

        let snap = registry.snapshot(q).expect("query snapshot");
        let filter = snap.filters.get(&7).expect("filter 7");
        assert_eq!(
            filter
                .acquired
                .as_ref()
                .map(|a| (a.outcome.as_str(), a.latency_ns)),
            Some(("complete", 4_000))
        );
    }

    #[test]
    fn dropped_reasons_preserve_order_and_dedupe() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(7, 8);
        let rec = registry.recorder(q);

        rec.dropped(13, RfDropReason::SizeExceeded);
        rec.dropped(13, RfDropReason::SendFailed);
        rec.dropped(13, RfDropReason::SizeExceeded);

        let snap = registry.snapshot(q).expect("query snapshot");
        let filter = snap.filters.get(&13).expect("filter 13");
        assert_eq!(
            filter.drop_reasons(),
            &[RfDropReason::SizeExceeded, RfDropReason::SendFailed]
        );
        assert!(filter.has_drop_reason(RfDropReason::SizeExceeded));
        assert!(filter.has_drop_reason(RfDropReason::SendFailed));
    }

    #[test]
    fn recorder_is_noop_safe_for_unknown_query() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(9, 9);
        registry.recorder(q).applied(1, 10, 10, 1);
        assert!(
            registry.snapshot(q).is_some(),
            "recorder auto-creates the query entry"
        );
    }

    #[test]
    fn cached_filter_handle_accumulates_applied_counters_concurrently() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(5, 6);
        let handle = registry.recorder(q).filter(11);
        assert_eq!(handle.filter_id(), 11);

        let mut workers = Vec::new();
        for _ in 0..8 {
            let handle = handle.clone();
            workers.push(thread::spawn(move || {
                for _ in 0..1000 {
                    handle.applied(2, 1, 1);
                }
            }));
        }
        for worker in workers {
            worker.join().expect("worker");
        }

        let snap = registry.snapshot(q).expect("query snapshot");
        let filter = snap.filters.get(&11).expect("filter 11");
        assert_eq!(filter.applied_input_rows(), 16_000);
        assert_eq!(filter.applied_output_rows(), 8_000);
        assert_eq!(filter.applied_evals(), 8_000);
    }

    #[test]
    fn export_builds_runtime_filters_subtree() {
        let registry = RuntimeFilterLifecycleRegistry::new();
        let q = QueryKey::from_hi_lo(1, 1);
        let rec = registry.recorder(q);
        rec.planned(7);
        rec.built(7, 3, 128);
        rec.sent_partial(7, 64);
        rec.sent_partial(7, 64);
        rec.merge_progress(7, 2, 2);
        rec.delivered(7);
        rec.acquired(7, "complete", 4_000);
        rec.applied(7, 100, 60, 1);
        rec.dropped(7, RfDropReason::SizeExceeded);

        let profile = RuntimeProfile::new("Query");
        registry.export_to_profile(q, &profile);

        let child = profile.get_child("RuntimeFilters").expect("subtree");
        let f = child.get_child("Filter7").expect("filter child");
        assert_eq!(f.counter_value("Planned"), Some(1));
        assert_eq!(f.counter_value("BuiltRows"), Some(3));
        assert_eq!(f.counter_value("BuiltBytes"), Some(128));
        assert_eq!(f.counter_value("SentPartials"), Some(2));
        assert_eq!(f.counter_value("SentBytes"), Some(128));
        assert_eq!(f.counter_value("MergedReceived"), Some(2));
        assert_eq!(f.counter_value("MergedExpected"), Some(2));
        assert_eq!(f.counter_value("Delivered"), Some(1));
        assert_eq!(
            f.get_info_string("AcquireOutcome").as_deref(),
            Some("complete")
        );
        assert_eq!(f.counter_value("AcquireLatency"), Some(4_000));
        assert_eq!(f.counter_value("AppliedInputRows"), Some(100));
        assert_eq!(f.counter_value("AppliedOutputRows"), Some(60));
        assert_eq!(f.counter_value("AppliedEvals"), Some(1));
        assert_eq!(
            f.get_info_string("Dropped").as_deref(),
            Some("SizeExceeded")
        );
    }
}
