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

//! Query-scoped, per-channel ingress dedupe + `(query, epoch)` tombstone.
//!
//! RFD-4/M3 "bounded at-least-once transport" retries wire envelopes, so both
//! ingress directions must idempotently absorb a duplicate/out-of-order envelope
//! instead of re-applying or re-delivering it. This component is the single home
//! for that state; the two ingress paths (`inbound.rs`, `consumer_ingress.rs`)
//! consult it, and the service teardown (`mod.rs`) populates the tombstone.
//!
//! It holds three logically-distinct structures, all keyed per channel so each set
//! is bounded naturally by a self-owned per-channel identity ceiling (M3 Task 4):
//!
//! 1. **Transport-identity dedupe** — one index per direction, keyed on the wire
//!    route identity *including the transport sequence*. It absorbs a re-arrival of
//!    the *same wire message* (an at-least-once retry).
//!    - Producer contributions carry a content guard: the Core downstream is
//!      content-aware (it distinguishes a byte-identical retry from a conflicting
//!      replay). A same-identity arrival whose content differs is therefore **not**
//!      a valid retry — it must flow to the Core so its `ConflictingReplay`
//!      detection still fires — so the producer gate short-circuits only when the
//!      recorded content digest matches.
//!    - Consumer deliveries have no downstream content-conflict check, so their
//!      gate is identity-only: any re-arrival of a delivery identity is absorbed.
//! 2. **Logical delivery idempotency** (consumer only) — the stable
//!    `(route_edge, version)` identity already delivered into a subscription,
//!    absorbed from the former `RuntimeFilterService::delivered_versions` (M2C
//!    spec §7.7). This catches "the same logical version re-delivered via a
//!    *distinct* transport sequence", which the transport-identity index cannot
//!    see. Both indices must hold on the consumer side.
//! 3. **`(query, epoch)` tombstone** — the set of `(query_id, deployment_epoch)`
//!    pairs retired by cancel/completion, plus a stale-epoch check. A late envelope
//!    for a retired epoch is rejected without rebuilding context (M2B3 lookup-only);
//!    an envelope older than a retired epoch is rejected as stale.
//!
//! Each per-channel identity set (contributions, deliveries, delivered_versions) is
//! self-bounded by a count ceiling: a genuinely-new identity beyond the ceiling is
//! refused with a `ResourceLimit` admission (the ingress path surfaces it as a
//! first-class rejection), while an already-recorded identity at the ceiling still
//! answers its normal Duplicate/DuplicateRetry/Conflict verdict so idempotency
//! survives at the cap. The bounding is purely these self-owned counters — the dedupe
//! sets are deliberately NOT wired into the global MemTracker this milestone. The
//! `(query, epoch)` tombstone is not ceilinged: it is keyed to this one query and
//! grows only with the query's own deployment epochs (realistically one).

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
};
use crate::runtime_filter::port::transport::ProducerInstanceRouteIdentity;
use crate::runtime_filter::port::transport::{ContributionRouteIdentity, DeliveryRouteIdentity};

/// Per-channel key for a producer contribution transport identity. It is exactly
/// the `ContributionRouteIdentity` (binding + finst + partition + sequence).
type ContributionKey = (BindingId, UniqueId, PartitionId, ProducerSequence);
type ProducerInstanceKey = (BindingId, UniqueId);

/// Per-channel key for a consumer delivery transport identity. It is exactly the
/// `DeliveryRouteIdentity` (route edge + sequence).
type DeliveryKey = (RouteEdgeId, ProducerSequence);

/// Result of admitting a producer contribution transport identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContributionAdmission {
    /// First arrival of this identity: proceed to the Core.
    Fresh,
    /// A byte-identical at-least-once retry: short-circuit to `Duplicate` without
    /// touching the Core.
    DuplicateRetry,
    /// The identity was seen before but with different content: this is not a valid
    /// retry. Proceed to the Core, which rejects it as a `ConflictingReplay`.
    Conflict,
    /// A genuinely-new identity that would push this channel's contribution set past
    /// its self-owned ceiling: refuse it as an explicit resource rejection instead of
    /// growing without bound. An already-recorded identity is never `ResourceLimit`.
    ResourceLimit,
}

pub(super) type ProducerInstanceAdmission = ContributionAdmission;

/// Result of admitting a consumer delivery transport / logical identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeliveryAdmission {
    /// First arrival of this identity: proceed to deliver.
    Fresh,
    /// Already recorded: answer `Duplicate` and do not re-deliver.
    Duplicate,
    /// A genuinely-new identity that would push this channel's delivery set past its
    /// self-owned ceiling: refuse it as an explicit resource rejection instead of
    /// growing without bound. An already-recorded identity is never `ResourceLimit`.
    ResourceLimit,
}

/// Logical delivery state retained for one stable `(route_edge, version)` key.
/// A final artifact is a strict upgrade over an earlier non-final delivery: it
/// carries the same artifact plus the `Completed` terminal atomically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeliveredVersionKind {
    NonFinal,
    FinalArtifact,
}

/// Admit `key` into a per-channel delivery-identity set under its self-owned ceiling.
/// An already-present key is a `Duplicate` (idempotency survives at the ceiling); a
/// genuinely-new key at or above the ceiling is refused as `ResourceLimit`; otherwise
/// it is inserted and answered `Fresh`.
fn admit_into_set<K: Ord>(set: &mut BTreeSet<K>, key: K, ceiling: usize) -> DeliveryAdmission {
    if set.contains(&key) {
        DeliveryAdmission::Duplicate
    } else if set.len() >= ceiling {
        DeliveryAdmission::ResourceLimit
    } else {
        set.insert(key);
        DeliveryAdmission::Fresh
    }
}

/// Verdict from consulting the `(query, epoch)` tombstone for an inbound envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TombstoneVerdict {
    /// Not retired and not older than a retired epoch: dispatch may proceed to the
    /// normal admission/authorization pipeline.
    Live,
    /// The envelope's `(query, epoch)` is retired (its deployment was cancelled or
    /// completed): reject without rebuilding context.
    Retired,
    /// The envelope's epoch is older than a retired epoch for this query: reject as
    /// a stale epoch.
    StaleEpoch,
}

// Generous per-channel identity ceiling (M3 Task 4). This is a query-scoped SOFTWARE
// SAFETY cap, NOT a cluster-topology quantity: it must NOT be sized to the live BE
// count (no single-BE assumption). A channel's real producer fan-in
// (instances x partitions x sequences) or consumer fan-out (route edges x versions)
// stays far below a million distinct identities even in a large 1FE+NBE cluster, so
// this only stops pathological unbounded growth from a retry storm. It is applied
// independently to each per-channel set. Bounding is self-owned; the dedupe sets are
// never wired into the global MemTracker this milestone.
const DEFAULT_MAX_IDENTITIES_PER_CHANNEL: usize = 1 << 20;

pub(super) struct IngressDedupe {
    /// The query this dedupe is scoped to; the tombstone is keyed
    /// `(query_id, deployment_epoch)`, so it is recorded and consulted against this
    /// identity.
    query_id: UniqueId,
    /// Self-owned ceiling on the size of each per-channel identity set. Exceeding it
    /// with a genuinely-new identity yields a `ResourceLimit` admission.
    max_identities_per_channel: usize,
    state: Mutex<DedupeState>,
}

#[derive(Default)]
struct DedupeState {
    /// (1) producer transport-identity index, per channel, each identity carrying
    /// the content digest of its first arrival for the retry-vs-conflict guard.
    contributions: BTreeMap<ChannelId, BTreeMap<ContributionKey, [u8; 32]>>,
    producer_instances: BTreeMap<ChannelId, BTreeMap<ProducerInstanceKey, [u8; 32]>>,
    /// (1) consumer transport-identity index, per channel.
    deliveries: BTreeMap<ChannelId, BTreeSet<DeliveryKey>>,
    /// (2) absorbed logical delivery idempotency, per channel.
    delivered_versions:
        BTreeMap<ChannelId, BTreeMap<(RouteEdgeId, LogicalVersion), DeliveredVersionKind>>,
    /// (3) `(query, epoch)` tombstone set.
    retired: BTreeSet<(UniqueId, DeploymentEpoch)>,
}

impl IngressDedupe {
    pub(super) fn new(query_id: UniqueId) -> Self {
        Self::with_max_identities_per_channel(query_id, DEFAULT_MAX_IDENTITIES_PER_CHANNEL)
    }

    pub(super) fn with_max_identities_per_channel(
        query_id: UniqueId,
        max_identities_per_channel: usize,
    ) -> Self {
        assert!(
            max_identities_per_channel >= 1,
            "each per-channel dedupe set must admit at least one identity"
        );
        Self {
            query_id,
            max_identities_per_channel,
            state: Mutex::new(DedupeState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DedupeState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Producer transport-identity gate. Records the identity on first arrival with
    /// its content digest; a repeat with the same digest is a genuine retry
    /// (`DuplicateRetry`), a repeat with a different digest is a `Conflict` that the
    /// caller must forward to the Core (never silently absorbed).
    pub(super) fn admit_contribution(
        &self,
        channel_id: ChannelId,
        route: &ContributionRouteIdentity,
        content_digest: [u8; 32],
    ) -> ContributionAdmission {
        let key = (
            route.producer_binding_id(),
            route.fragment_instance_id(),
            route.partition_id(),
            route.sequence(),
        );
        let ceiling = self.max_identities_per_channel;
        let mut state = self.lock();
        let channel = state.contributions.entry(channel_id).or_default();
        // Snapshot the size before taking the entry (which borrows the map): a Vacant
        // slot at the ceiling is a genuinely-new identity beyond the cap.
        let occupancy = channel.len();
        match channel.entry(key) {
            // An already-recorded identity keeps answering its normal retry/conflict
            // verdict, at the ceiling or not, so idempotency survives at the cap.
            Entry::Occupied(slot) => {
                if *slot.get() == content_digest {
                    ContributionAdmission::DuplicateRetry
                } else {
                    ContributionAdmission::Conflict
                }
            }
            Entry::Vacant(slot) => {
                if occupancy >= ceiling {
                    ContributionAdmission::ResourceLimit
                } else {
                    slot.insert(content_digest);
                    ContributionAdmission::Fresh
                }
            }
        }
    }

    pub(super) fn admit_producer_instance(
        &self,
        channel_id: ChannelId,
        route: &ProducerInstanceRouteIdentity,
        content_digest: [u8; 32],
    ) -> ProducerInstanceAdmission {
        let key = (route.producer_binding_id(), route.fragment_instance_id());
        let ceiling = self.max_identities_per_channel;
        let mut state = self.lock();
        let channel = state.producer_instances.entry(channel_id).or_default();
        let occupancy = channel.len();
        match channel.entry(key) {
            Entry::Occupied(slot) if *slot.get() == content_digest => {
                ProducerInstanceAdmission::DuplicateRetry
            }
            Entry::Occupied(_) => ProducerInstanceAdmission::Conflict,
            Entry::Vacant(_) if occupancy >= ceiling => ProducerInstanceAdmission::ResourceLimit,
            Entry::Vacant(slot) => {
                slot.insert(content_digest);
                ProducerInstanceAdmission::Fresh
            }
        }
    }

    /// Consumer transport-identity gate: absorbs a re-arrival of the same delivery
    /// identity (route edge + transport sequence) regardless of its content.
    pub(super) fn admit_delivery(
        &self,
        channel_id: ChannelId,
        route: &DeliveryRouteIdentity,
    ) -> DeliveryAdmission {
        let key = (route.route_edge_id(), route.sequence());
        let ceiling = self.max_identities_per_channel;
        let mut state = self.lock();
        let set = state.deliveries.entry(channel_id).or_default();
        admit_into_set(set, key, ceiling)
    }

    /// Consumer logical idempotency (absorbed from `delivered_versions`): the stable
    /// `(route_edge, version)` identity delivered into a subscription. Distinct from
    /// the transport gate — it catches the same logical version re-delivered via a
    /// distinct transport sequence.
    pub(super) fn admit_delivered_version(
        &self,
        channel_id: ChannelId,
        route_edge_id: RouteEdgeId,
        version: LogicalVersion,
        kind: DeliveredVersionKind,
    ) -> DeliveryAdmission {
        let ceiling = self.max_identities_per_channel;
        let mut state = self.lock();
        let delivered = state.delivered_versions.entry(channel_id).or_default();
        let occupancy = delivered.len();
        match delivered.entry((route_edge_id, version)) {
            Entry::Occupied(mut slot)
                if *slot.get() == DeliveredVersionKind::NonFinal
                    && kind == DeliveredVersionKind::FinalArtifact =>
            {
                // This is the one permitted same-version transition. It consumes no
                // additional identity capacity and must proceed to delivery so ingress
                // can merge `Completed` with the already-visible artifact.
                slot.insert(DeliveredVersionKind::FinalArtifact);
                DeliveryAdmission::Fresh
            }
            Entry::Occupied(_) => DeliveryAdmission::Duplicate,
            Entry::Vacant(slot) => {
                if occupancy >= ceiling {
                    DeliveryAdmission::ResourceLimit
                } else {
                    slot.insert(kind);
                    DeliveryAdmission::Fresh
                }
            }
        }
    }

    /// Tombstone this service's `(query, epoch)` at cancel/completion so a late or
    /// duplicate envelope arriving after teardown is rejected without rebuilding
    /// context.
    pub(super) fn retire_epoch(&self, epoch: DeploymentEpoch) {
        self.lock().retired.insert((self.query_id, epoch));
    }

    /// Consult the tombstone for an inbound envelope. Never rebuilds or revives any
    /// query state — a retired or stale envelope is rejected outright.
    ///
    /// The lookup is keyed by this component's own `query_id`: the service is looked
    /// up by the envelope's query id before dispatch, so an envelope reaching here
    /// always speaks this same query. The redundant caller-supplied query id has been
    /// dropped in favour of `self.query_id`.
    pub(super) fn tombstone_verdict(&self, epoch: DeploymentEpoch) -> TombstoneVerdict {
        let query_id = self.query_id;
        let state = self.lock();
        if state.retired.contains(&(query_id, epoch)) {
            return TombstoneVerdict::Retired;
        }
        // The highest epoch retired for this query bounds staleness: an envelope
        // older than a retired epoch belongs to a superseded deployment generation.
        if let Some((_, max_retired)) = state
            .retired
            .range(
                (query_id, DeploymentEpoch::new(u64::MIN))
                    ..=(query_id, DeploymentEpoch::new(u64::MAX)),
            )
            .next_back()
        {
            if epoch < *max_retired {
                return TombstoneVerdict::StaleEpoch;
            }
        }
        TombstoneVerdict::Live
    }

    /// Size of a channel's producer contribution identity set. Test seam for the
    /// self-owned ceiling and teardown assertions.
    #[cfg(test)]
    fn contribution_len(&self, channel_id: ChannelId) -> usize {
        self.lock()
            .contributions
            .get(&channel_id)
            .map_or(0, BTreeMap::len)
    }

    /// Size of a channel's consumer delivery transport-identity set.
    #[cfg(test)]
    fn delivery_len(&self, channel_id: ChannelId) -> usize {
        self.lock()
            .deliveries
            .get(&channel_id)
            .map_or(0, BTreeSet::len)
    }

    /// Size of a channel's consumer logical `(route_edge, version)` set.
    #[cfg(test)]
    fn delivered_version_len(&self, channel_id: ChannelId) -> usize {
        self.lock()
            .delivered_versions
            .get(&channel_id)
            .map_or(0, BTreeMap::len)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ContributionAdmission, DeliveredVersionKind, DeliveryAdmission, IngressDedupe,
        TombstoneVerdict,
    };
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
    };
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, DeliveryRouteIdentity,
    };

    const QID: UniqueId = UniqueId::new(5, 6);

    fn dedupe() -> IngressDedupe {
        IngressDedupe::new(QID)
    }

    fn contribution(sequence: u64) -> ContributionRouteIdentity {
        ContributionRouteIdentity::try_new(
            BindingId::new(1),
            UniqueId::new(1, 2),
            PartitionId::new(0),
            ProducerSequence::new(sequence),
        )
        .unwrap()
    }

    fn delivery(sequence: u64) -> DeliveryRouteIdentity {
        DeliveryRouteIdentity::try_new(RouteEdgeId::new(40), ProducerSequence::new(sequence))
            .unwrap()
    }

    #[test]
    fn ingress_dedupe_component_contribution_retry_vs_conflict() {
        let dedupe = dedupe();
        let channel = ChannelId::new(1);
        let route = contribution(0);
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
        // Same identity + same digest = a genuine retry.
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [1; 32]),
            ContributionAdmission::DuplicateRetry,
        );
        // Same identity + different digest = a conflict that must reach the Core.
        assert_eq!(
            dedupe.admit_contribution(channel, &route, [2; 32]),
            ContributionAdmission::Conflict,
        );
    }

    #[test]
    fn ingress_dedupe_component_contribution_is_scoped_per_channel() {
        let dedupe = dedupe();
        let route = contribution(0);
        assert_eq!(
            dedupe.admit_contribution(ChannelId::new(1), &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
        // The same identity on a different channel is independent.
        assert_eq!(
            dedupe.admit_contribution(ChannelId::new(2), &route, [1; 32]),
            ContributionAdmission::Fresh,
        );
    }

    #[test]
    fn ingress_dedupe_component_delivery_transport_and_logical_are_independent() {
        let dedupe = dedupe();
        let channel = ChannelId::new(1);
        let edge = RouteEdgeId::new(40);
        let version = LogicalVersion::new(5);

        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Fresh,
        );
        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version, DeliveredVersionKind::NonFinal,),
            DeliveryAdmission::Fresh,
        );

        // A distinct transport sequence is fresh at the transport gate ...
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(2)),
            DeliveryAdmission::Fresh,
        );
        // ... but the same logical version is a duplicate at the logical gate.
        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version, DeliveredVersionKind::NonFinal,),
            DeliveryAdmission::Duplicate,
        );
        // An exact transport retry is a duplicate at the transport gate.
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Duplicate,
        );
    }

    #[test]
    fn logical_artifact_can_upgrade_to_final_without_new_identity() {
        let dedupe = bounded_dedupe(1);
        let channel = ChannelId::new(1);
        let edge = RouteEdgeId::new(40);
        let version = LogicalVersion::new(5);

        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version, DeliveredVersionKind::NonFinal,),
            DeliveryAdmission::Fresh,
        );
        assert_eq!(
            dedupe.admit_delivered_version(
                channel,
                edge,
                version,
                DeliveredVersionKind::FinalArtifact,
            ),
            DeliveryAdmission::Fresh,
            "final delivery upgrades the existing key even at the ceiling",
        );
        assert_eq!(dedupe.delivered_version_len(channel), 1);
        assert_eq!(
            dedupe.admit_delivered_version(
                channel,
                edge,
                version,
                DeliveredVersionKind::FinalArtifact,
            ),
            DeliveryAdmission::Duplicate,
        );
        assert_eq!(
            dedupe.admit_delivered_version(channel, edge, version, DeliveredVersionKind::NonFinal,),
            DeliveryAdmission::Duplicate,
        );
    }

    #[test]
    fn ingress_dedupe_component_tombstone_retired_and_stale() {
        let dedupe = dedupe();
        assert_eq!(
            dedupe.tombstone_verdict(DeploymentEpoch::new(9)),
            TombstoneVerdict::Live,
        );
        dedupe.retire_epoch(DeploymentEpoch::new(9));
        assert_eq!(
            dedupe.tombstone_verdict(DeploymentEpoch::new(9)),
            TombstoneVerdict::Retired,
        );
        assert_eq!(
            dedupe.tombstone_verdict(DeploymentEpoch::new(8)),
            TombstoneVerdict::StaleEpoch,
        );
        // A newer epoch is neither retired nor stale (a still-live generation).
        assert_eq!(
            dedupe.tombstone_verdict(DeploymentEpoch::new(10)),
            TombstoneVerdict::Live,
        );
    }

    #[test]
    fn ingress_dedupe_component_tombstone_is_query_scoped() {
        // The tombstone is keyed to the dedupe's own query (the lookup no longer takes
        // a query id — the component is query-scoped by construction), so a distinct
        // query's dedupe has an entirely independent tombstone.
        let dedupe = dedupe();
        dedupe.retire_epoch(DeploymentEpoch::new(9));
        assert_eq!(
            dedupe.tombstone_verdict(DeploymentEpoch::new(9)),
            TombstoneVerdict::Retired,
        );

        let other = IngressDedupe::new(UniqueId::new(7, 8));
        // The other query's dedupe never saw the retire; its epochs stay Live.
        assert_eq!(
            other.tombstone_verdict(DeploymentEpoch::new(9)),
            TombstoneVerdict::Live,
        );
        assert_eq!(
            other.tombstone_verdict(DeploymentEpoch::new(1)),
            TombstoneVerdict::Live,
        );
    }

    // ==============================================================================
    // M3 Task 4: self-owned per-channel ceilings -> explicit ResourceLimit admission.
    // ==============================================================================

    // A dedupe whose per-channel identity ceiling is a tiny `max`, so the tests can
    // fill a set to the cap without materializing a million identities.
    fn bounded_dedupe(max: usize) -> IngressDedupe {
        IngressDedupe::with_max_identities_per_channel(QID, max)
    }

    #[test]
    fn transport_bounded_contribution_dedupe_rejects_new_identity_at_ceiling() {
        // Ceiling of 2 distinct contribution identities per channel.
        let dedupe = bounded_dedupe(2);
        let channel = ChannelId::new(1);
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(1), [1; 32]),
            ContributionAdmission::Fresh,
        );
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(2), [2; 32]),
            ContributionAdmission::Fresh,
        );
        assert_eq!(dedupe.contribution_len(channel), 2);

        // A genuinely-new identity beyond the ceiling is refused, not admitted.
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(3), [3; 32]),
            ContributionAdmission::ResourceLimit,
        );
        assert_eq!(
            dedupe.contribution_len(channel),
            2,
            "a rejected identity is not recorded"
        );
    }

    #[test]
    fn transport_bounded_contribution_idempotency_survives_at_ceiling() {
        // At the ceiling, an already-recorded identity keeps answering its normal
        // retry / conflict verdict — only genuinely-new identities are rejected.
        let dedupe = bounded_dedupe(1);
        let channel = ChannelId::new(1);
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(1), [1; 32]),
            ContributionAdmission::Fresh,
        );
        // Same identity + same digest at the cap: still a genuine retry.
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(1), [1; 32]),
            ContributionAdmission::DuplicateRetry,
        );
        // Same identity + different digest at the cap: still a conflict for the Core.
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(1), [9; 32]),
            ContributionAdmission::Conflict,
        );
        // Only a genuinely-new identity is refused.
        assert_eq!(
            dedupe.admit_contribution(channel, &contribution(2), [2; 32]),
            ContributionAdmission::ResourceLimit,
        );
    }

    #[test]
    fn transport_bounded_delivery_dedupe_rejects_new_identity_at_ceiling() {
        // Ceiling of 2 distinct delivery transport identities per channel.
        let dedupe = bounded_dedupe(2);
        let channel = ChannelId::new(1);
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Fresh,
        );
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(2)),
            DeliveryAdmission::Fresh,
        );
        // A genuinely-new transport identity beyond the ceiling is refused ...
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(3)),
            DeliveryAdmission::ResourceLimit,
        );
        // ... but an already-recorded identity at the ceiling is still a Duplicate.
        assert_eq!(
            dedupe.admit_delivery(channel, &delivery(1)),
            DeliveryAdmission::Duplicate,
        );
        assert_eq!(dedupe.delivery_len(channel), 2);
    }

    #[test]
    fn transport_bounded_delivered_version_dedupe_rejects_new_identity_at_ceiling() {
        // The logical `(route_edge, version)` set is bounded independently.
        let dedupe = bounded_dedupe(1);
        let channel = ChannelId::new(1);
        let edge = RouteEdgeId::new(40);
        assert_eq!(
            dedupe.admit_delivered_version(
                channel,
                edge,
                LogicalVersion::new(5),
                DeliveredVersionKind::NonFinal,
            ),
            DeliveryAdmission::Fresh,
        );
        // A genuinely-new version beyond the ceiling is refused ...
        assert_eq!(
            dedupe.admit_delivered_version(
                channel,
                edge,
                LogicalVersion::new(6),
                DeliveredVersionKind::NonFinal,
            ),
            DeliveryAdmission::ResourceLimit,
        );
        // ... but the recorded version at the ceiling is still a Duplicate.
        assert_eq!(
            dedupe.admit_delivered_version(
                channel,
                edge,
                LogicalVersion::new(5),
                DeliveredVersionKind::NonFinal,
            ),
            DeliveryAdmission::Duplicate,
        );
        assert_eq!(dedupe.delivered_version_len(channel), 1);
    }

    #[test]
    fn transport_bounded_ceiling_is_per_channel_and_released_on_teardown() {
        // The ceiling applies to each channel independently, and every count lives
        // inside this query-scoped component, so dropping it (as query cancel destroys
        // the service) frees everything — a freshly-built dedupe observes zero counts.
        let dedupe = bounded_dedupe(1);
        let channel_a = ChannelId::new(1);
        let channel_b = ChannelId::new(2);
        assert_eq!(
            dedupe.admit_delivery(channel_a, &delivery(1)),
            DeliveryAdmission::Fresh,
        );
        // Channel A is at its cap; channel B has its own independent budget.
        assert_eq!(
            dedupe.admit_delivery(channel_a, &delivery(2)),
            DeliveryAdmission::ResourceLimit,
        );
        assert_eq!(
            dedupe.admit_delivery(channel_b, &delivery(2)),
            DeliveryAdmission::Fresh,
        );
        assert_eq!(dedupe.delivery_len(channel_a), 1);
        assert_eq!(dedupe.delivery_len(channel_b), 1);

        drop(dedupe);
        // Teardown frees the self-owned counters: a new query's dedupe starts at zero.
        let fresh = bounded_dedupe(1);
        assert_eq!(fresh.delivery_len(channel_a), 0);
        assert_eq!(fresh.delivery_len(channel_b), 0);
    }
}
