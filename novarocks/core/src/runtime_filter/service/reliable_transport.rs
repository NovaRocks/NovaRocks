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

//! Sender-side reliable transport for the delivery Router's remote leg.
//!
//! Every remote envelope the Service emits (an artifact bundle or an `Unavailable`
//! sentinel today; `Contribution`/`ProducerClosed` once RFD-6 converges the
//! producer leg here) flows through this query-scoped transport instead of going
//! fire-and-forget. The transport:
//!
//! * buffers each already-serialized [`EncodedArtifactFrame`] keyed by its delivery
//!   route identity (`route_edge_id` + a transport-assigned monotonic `sequence`),
//!   holding the frame behind an [`Arc`] so one logical envelope that fans out to
//!   several routes is serialized once and shared, then acked per route;
//! * releases a buffered frame when its ack arrives ([`Self::on_ack`]) — `Accepted`
//!   and `Duplicate` both release and never re-transmit; `Rejected` releases but is
//!   surfaced as a running-contract corruption rather than silently swallowed;
//! * re-hands unacked frames to the underlying sink on an explicit tick
//!   ([`Self::drive_retries`]) under a bounded attempt count, and, once a frame
//!   outlives its deadline, drops it and reports the route as *failed open* — the
//!   route degrades but the query neither errors nor panics (runtime filters are an
//!   optimization, never a correctness dependency).
//!
//! Every buffered step emits a structured [`RuntimeFilterEvent::TransportEnvelope`]
//! through the injected [`RuntimeFilterEventSink`] — the SAME RFD-3 lifecycle sink the
//! Service already emits through, never a second registry — so `Sent` (with the metered
//! byte size), `Retried`, `Acked` (with the peer's accept status), and deadline
//! `FailedOpen` are observable. The resource-limit fail-open is emitted by the Service at
//! the `send` call site instead of here: a resource-refused frame never entered the
//! buffer, so this module only emits for frames it actually holds.
//!
//! Retry and deadline timing are driven by the injected [`RuntimeFilterClock`] and
//! the query manager's bounded production tick. There is no per-query background
//! thread or timer here.
//!
//! The transport is kind-agnostic: it keys, waits, and releases purely by delivery
//! route identity. It never inspects the producer's semantic kind (Join / TopN /
//! aggregate) to decide routing.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
use crate::runtime_filter::port::events::{
    RuntimeFilterEvent, RuntimeFilterEventSink, TransportEventKind, TransportFailOpenReason,
    TransportRouteEventIdentity,
};
use crate::runtime_filter::port::identity::{ProducerSequence, RouteEdgeId};
use crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute;
use crate::runtime_filter::port::support::RuntimeFilterClock;
use crate::runtime_filter::port::transport::{
    DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
    RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity, RuntimeFilterTransportEnvelope,
};
use crate::runtime_filter::router::remote::{
    RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome,
};
use crate::service::grpc_runtime_filter_sender::GrpcRuntimeFilterEnvelopeSink;

use super::{
    CloseRole, FinalizerCompletion, LifecycleBarrier, LifecyclePermit, finish_finalizer_panic,
    retain_first_finalizer_panic,
};

/// Bounded retry / deadline / buffer policy for the reliable transport.
///
/// `max_attempts` caps the total number of times a frame is handed to the sink
/// (the initial send plus retries), bounding network chatter. `deadline` caps how
/// long a frame may stay buffered before it is released and the route fails open,
/// bounding buffer lifetime. The two limits are independent: exhausting the attempt
/// count stops re-transmission but keeps the frame buffered until the deadline, so
/// an ack that finally arrives before the deadline still releases cleanly.
///
/// `max_pending_entries` and `max_pending_bytes` are the M3 Task 4 self-owned buffer
/// ceilings. Bytes charge each envelope's deterministic inline size plus retained
/// payload capacity, and each entry's inline route plus retained endpoint-host capacity;
/// allocator/control-block and pending-entry overhead is bounded by the entry ceiling. They
/// bound the sender-side buffer purely through the transport's OWN
/// counters — RF buffer memory is deliberately NOT wired into the global MemTracker
/// this milestone. Offering a frame that would exceed either returns
/// [`ReliableSendOutcome::ResourceLimit`] instead of buffering: an explicit resource
/// rejection, distinct from the deadline fail-open degradation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReliableTransportPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
    max_pending_entries: usize,
    max_pending_bytes: usize,
}

impl ReliableTransportPolicy {
    pub(crate) fn new(
        retry_interval: Duration,
        max_attempts: u32,
        deadline: Duration,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> Self {
        assert!(
            max_attempts >= 1,
            "reliable transport must allow at least the initial send"
        );
        assert!(
            max_pending_entries >= 1,
            "reliable transport must be able to buffer at least one frame"
        );
        Self {
            retry_interval,
            max_attempts,
            deadline,
            max_pending_entries,
            max_pending_bytes,
        }
    }
}

// Sane defaults for the not-yet-wired production driver. RFD-6 sources the real
// values from the query deadline and cluster RPC policy; until the live sender and
// tick loop exist these constants only shape test-free production construction.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

// Self-owned buffer ceilings (M3 Task 4). These are query-scoped SOFTWARE SAFETY
// caps, not cluster-topology quantities: they must NOT be sized to the live BE count
// (no single-BE assumption), so they are generous fixed constants that a healthy
// broadcast fan-out and its unacked backlog stay far below. They exist only to stop
// pathological unbounded growth (a retry storm, a peer that never acks). Bounding is
// self-owned via the transport's own counters; RF buffer memory stays out of the
// global MemTracker this milestone.
//
// `DEFAULT_MAX_PENDING_ENTRIES`: an in-flight backlog this deep (65536 unacked remote
// deliveries for one query, across all its channels) is already pathological; real
// ack cadence keeps the live buffer far smaller even when broadcasting to a large
// cluster.
const DEFAULT_MAX_PENDING_ENTRIES: usize = 1 << 16;
// `DEFAULT_MAX_PENDING_BYTES`: 256 MiB of DISTINCT buffered retained-envelope charge
// plus per-entry retained route charge per query. Each complete immutable envelope is
// itself bounded by its channel's wire ceiling and is metered for as long as that exact
// allocation remains pending.
const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

impl Default for ReliableTransportPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_RETRY_INTERVAL,
            DEFAULT_MAX_ATTEMPTS,
            DEFAULT_DEADLINE,
            DEFAULT_MAX_PENDING_ENTRIES,
            DEFAULT_MAX_PENDING_BYTES,
        )
    }
}

/// Buffer key: a delivery route identity reduced to its hashable coordinates. Both
/// components are non-zero (the route edge id is validated at route construction and
/// the transport assigns sequences starting at 1), so it round-trips losslessly to a
/// [`DeliveryRouteIdentity`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum PendingKey {
    Delivery {
        route_edge_id: RouteEdgeId,
        sequence: ProducerSequence,
    },
    Contribution {
        binding_id: crate::runtime_filter::model::contract::BindingId,
        fragment_instance_id: crate::common::types::UniqueId,
        partition_id: crate::runtime_filter::port::identity::PartitionId,
        sequence: ProducerSequence,
    },
    ProducerInstance {
        binding_id: crate::runtime_filter::model::contract::BindingId,
        fragment_instance_id: crate::common::types::UniqueId,
    },
}

impl PendingKey {
    fn from_identity(identity: &RuntimeFilterRouteIdentity) -> Self {
        if let Some(identity) = identity.as_delivery() {
            Self::Delivery {
                route_edge_id: identity.route_edge_id(),
                sequence: identity.sequence(),
            }
        } else if let Some(identity) = identity.as_contribution() {
            Self::Contribution {
                binding_id: identity.producer_binding_id(),
                fragment_instance_id: identity.fragment_instance_id(),
                partition_id: identity.partition_id(),
                sequence: identity.sequence(),
            }
        } else {
            let identity = identity
                .as_producer_instance()
                .expect("runtime filter route identity is typed");
            Self::ProducerInstance {
                binding_id: identity.producer_binding_id(),
                fragment_instance_id: identity.fragment_instance_id(),
            }
        }
    }

    fn into_route_identity(self) -> RuntimeFilterRouteIdentity {
        use crate::runtime_filter::port::transport::{
            ContributionRouteIdentity, ProducerInstanceRouteIdentity,
        };
        match self {
            Self::Delivery {
                route_edge_id,
                sequence,
            } => RuntimeFilterRouteIdentity::delivery(
                DeliveryRouteIdentity::try_new(route_edge_id, sequence)
                    .expect("pending keys carry validated delivery coordinates"),
            ),
            Self::Contribution {
                binding_id,
                fragment_instance_id,
                partition_id,
                sequence,
            } => RuntimeFilterRouteIdentity::contribution(
                ContributionRouteIdentity::try_new(
                    binding_id,
                    fragment_instance_id,
                    partition_id,
                    sequence,
                )
                .expect("pending keys carry validated contribution coordinates"),
            ),
            Self::ProducerInstance {
                binding_id,
                fragment_instance_id,
            } => RuntimeFilterRouteIdentity::producer_instance(
                ProducerInstanceRouteIdentity::try_new(binding_id, fragment_instance_id)
                    .expect("pending keys carry validated producer-instance coordinates"),
            ),
        }
    }
}

/// A buffered in-flight frame awaiting acknowledgement.
struct PendingEntry {
    envelope: Arc<RuntimeFilterEnvelope>,
    allocation: usize,
    retained_bytes: usize,
    route_retained_bytes: usize,
    route: RuntimeFilterRemoteRoute,
    attempts: u32,
    first_sent_at: Instant,
    last_sent_at: Instant,
    // The route-level identity the delivery bridge stamped at `send`, carried so retry,
    // ack, and deadline emissions all key their structured event off the same route.
    event_identity: TransportRouteEventIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompletedWitness {
    route_fingerprint: [u8; 32],
    fingerprint: [u8; 32],
}

#[derive(Default)]
struct RetiredIdentityFilter {
    bits: Vec<u64>,
}

// Query-scoped 8 KiB no-false-negative retirement filter. False positives are safe:
// producer adapters map `RetiredIdentity` to RF fail-open, never to a query error.
const RETIRED_IDENTITY_FILTER_BITS: usize = 1 << 16;

impl RetiredIdentityFilter {
    fn ensure_capacity(&mut self) {
        if self.bits.is_empty() {
            self.bits.resize(RETIRED_IDENTITY_FILTER_BITS / 64, 0);
        }
    }

    fn indices(&self, key: PendingKey) -> [usize; 3] {
        let digest = pending_key_fingerprint(key);
        let bit_count = self.bits.len() * 64;
        let index = |offset: usize| {
            usize::try_from(u64::from_le_bytes(
                digest[offset..offset + 8]
                    .try_into()
                    .expect("fingerprint chunk has fixed width"),
            ))
            .unwrap_or(0)
                % bit_count
        };
        [index(0), index(8), index(16)]
    }

    fn insert(&mut self, key: PendingKey) {
        if matches!(key, PendingKey::Delivery { .. }) {
            return;
        }
        self.ensure_capacity();
        for index in self.indices(key) {
            self.bits[index / 64] |= 1_u64 << (index % 64);
        }
    }

    fn might_contain(&self, key: PendingKey) -> bool {
        !matches!(key, PendingKey::Delivery { .. })
            && !self.bits.is_empty()
            && self
                .indices(key)
                .into_iter()
                .all(|index| self.bits[index / 64] & (1_u64 << (index % 64)) != 0)
    }
}

/// Which self-owned transport ceiling a `send` would exceed. Kept distinct so a
/// later task's structured degradation event can name the limit that tripped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportResourceLimit {
    /// Buffering another frame would exceed the pending-entry count ceiling.
    PendingEntries,
    /// Buffering another distinct frame would exceed the retained envelope-and-route
    /// byte ceiling.
    SerializedBytes,
}

/// Outcome of offering a frame to the reliable transport ([`ReliableEnvelopeTransport::send`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReliableSendOutcome {
    /// Buffered for ack-release + bounded retry and handed to the sink once; carries
    /// the delivery route identity the transport stamped so a later ack can address
    /// exactly this in-flight frame.
    Buffered(RuntimeFilterRouteIdentity),
    /// Refused: a self-owned ceiling (pending-entry count or buffered serialized
    /// bytes) would be exceeded. The frame was NOT buffered and NOT put on the wire.
    /// This is an EXPLICIT resource rejection — a first-class outcome, not a silent
    /// drop and not the deadline fail-open degradation. The caller degrades the route.
    ResourceLimit(TransportResourceLimit),
    /// The query transport is terminal. The frame was neither buffered nor sent.
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReliableSendError {
    IdentityConflict,
    RetiredIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReliableFailedOpenWork {
    route: RuntimeFilterRemoteRoute,
    envelope: Arc<RuntimeFilterEnvelope>,
    event_identity: TransportRouteEventIdentity,
    reason: TransportFailOpenReason,
}

impl ReliableFailedOpenWork {
    pub(crate) const fn route(&self) -> &RuntimeFilterRemoteRoute {
        &self.route
    }

    pub(crate) const fn envelope(&self) -> &Arc<RuntimeFilterEnvelope> {
        &self.envelope
    }

    pub(crate) const fn event_identity(&self) -> TransportRouteEventIdentity {
        self.event_identity
    }

    pub(crate) const fn reason(&self) -> TransportFailOpenReason {
        self.reason
    }
}

#[cfg(test)]
impl ReliableSendOutcome {
    /// The stamped delivery identity of a buffered send, or a panic if the send was
    /// refused. Test convenience for the common "expected to buffer" call site.
    fn expect_buffered(self) -> RuntimeFilterRouteIdentity {
        match self {
            ReliableSendOutcome::Buffered(identity) => identity,
            ReliableSendOutcome::ResourceLimit(limit) => {
                panic!("expected a buffered send, got ResourceLimit({limit:?})")
            }
            ReliableSendOutcome::Shutdown => panic!("expected a buffered send after shutdown"),
        }
    }
}

/// The query-scoped in-flight buffer plus its self-owned counters.
///
/// Byte metering charges the deterministic inline envelope plus its retained payload
/// capacity once per envelope allocation, and the inline route plus retained endpoint-host
/// capacity once per pending entry. Arc/control-block, allocator, and pending-entry fixed
/// overhead are bounded separately by the entry ceiling. Route-specific broadcast entries
/// are each charged; retries reuse the same allocation and add no charge.
/// `allocation_refs` keys on `Arc::as_ptr`; while an envelope remains pending its address
/// is stable, and `bytes` changes only on the allocation's 0<->1 reference transition.
#[derive(Default)]
struct PendingBuffer {
    entries: HashMap<PendingKey, PendingEntry>,
    bytes: usize,
    allocation_refs: HashMap<usize, usize>,
    completed: HashMap<PendingKey, CompletedWitness>,
    completed_order: VecDeque<PendingKey>,
    retired: RetiredIdentityFilter,
}

impl PendingBuffer {
    /// Admit a new entry under the ceilings. On success the entry is inserted and its
    /// envelope's bytes are metered (once per unique allocation); on a ceiling breach
    /// nothing is inserted and the tripped limit is returned. `send_prepared` resolves
    /// exact duplicates and conflicts before admission, so this insertion never overwrites.
    fn admit(
        &mut self,
        key: PendingKey,
        entry: PendingEntry,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(), TransportResourceLimit> {
        if self.entries.len() >= max_entries {
            return Err(TransportResourceLimit::PendingEntries);
        }
        let allocation = entry.allocation;
        let is_new_allocation = !self.allocation_refs.contains_key(&allocation);
        // A route-specific envelope allocation adds bytes once. Retrying the same Arc
        // never calls admission and therefore never charges the allocation again.
        let envelope_added = if is_new_allocation {
            entry.retained_bytes
        } else {
            0
        };
        let added = envelope_added.saturating_add(entry.route_retained_bytes);
        if self.bytes.saturating_add(added) > max_bytes {
            return Err(TransportResourceLimit::SerializedBytes);
        }
        *self.allocation_refs.entry(allocation).or_insert(0) += 1;
        self.bytes += added;
        // Underscore-bound so the assertion's variable is not flagged unused in release
        // builds, where `debug_assert!` compiles out.
        let _previous = self.entries.insert(key, entry);
        debug_assert!(
            _previous.is_none(),
            "reliable transport admission requires a unique pending identity"
        );
        Ok(())
    }

    /// Release the accounting for an entry's envelope: drop one reference, and when the
    /// last reference to an allocation goes away, reclaim its bytes.
    fn release(&mut self, entry: &PendingEntry) {
        self.bytes = self.bytes.saturating_sub(entry.route_retained_bytes);
        let allocation = entry.allocation;
        if let Some(refs) = self.allocation_refs.get_mut(&allocation) {
            *refs -= 1;
            if *refs == 0 {
                self.allocation_refs.remove(&allocation);
                self.bytes = self.bytes.saturating_sub(entry.retained_bytes);
            }
        }
    }

    fn record_completed(&mut self, key: PendingKey, entry: &PendingEntry, max_entries: usize) {
        if self.completed.contains_key(&key) {
            return;
        }
        while self.completed.len() >= max_entries {
            let Some(oldest) = self.completed_order.pop_front() else {
                break;
            };
            self.completed.remove(&oldest);
        }
        self.completed.insert(
            key,
            CompletedWitness {
                route_fingerprint: route_fingerprint(&entry.route),
                fingerprint: envelope_fingerprint(&entry.envelope),
            },
        );
        self.completed_order.push_back(key);
        self.retired.insert(key);
    }
}

fn pending_key_fingerprint(key: PendingKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    match key {
        PendingKey::Delivery {
            route_edge_id,
            sequence,
        } => {
            digest.update([1]);
            digest.update(route_edge_id.get().to_le_bytes());
            digest.update(sequence.get().to_le_bytes());
        }
        PendingKey::Contribution {
            binding_id,
            fragment_instance_id,
            partition_id,
            sequence,
        } => {
            digest.update([2]);
            digest.update(binding_id.get().to_le_bytes());
            digest.update(fragment_instance_id.high().to_le_bytes());
            digest.update(fragment_instance_id.low().to_le_bytes());
            digest.update(partition_id.get().to_le_bytes());
            digest.update(sequence.get().to_le_bytes());
        }
        PendingKey::ProducerInstance {
            binding_id,
            fragment_instance_id,
        } => {
            digest.update([3]);
            digest.update(binding_id.get().to_le_bytes());
            digest.update(fragment_instance_id.high().to_le_bytes());
            digest.update(fragment_instance_id.low().to_le_bytes());
        }
    }
    digest.finalize().into()
}

fn route_fingerprint(route: &RuntimeFilterRemoteRoute) -> [u8; 32] {
    use crate::runtime_filter::port::routing::RuntimeFilterRouteRole;

    let mut digest = Sha256::new();
    digest.update(route.route_edge_id().get().to_le_bytes());
    digest.update(route.peer_participant_id().get().to_le_bytes());
    digest.update(route.endpoint().host().as_bytes());
    digest.update(route.endpoint().port().to_le_bytes());
    match route.target_role() {
        RuntimeFilterRouteRole::Producer(binding) => {
            digest.update([1]);
            digest.update(binding.get().to_le_bytes());
        }
        RuntimeFilterRouteRole::Aggregator => digest.update([2]),
        RuntimeFilterRouteRole::Relay => digest.update([3]),
        RuntimeFilterRouteRole::Consumer(binding) => {
            digest.update([4]);
            digest.update(binding.get().to_le_bytes());
        }
    }
    digest.finalize().into()
}

fn envelope_fingerprint(envelope: &RuntimeFilterEnvelope) -> [u8; 32] {
    let kind_tag = match envelope.kind() {
        RuntimeFilterEnvelopeKind::Contribution => 1,
        RuntimeFilterEnvelopeKind::Artifact => 2,
        RuntimeFilterEnvelopeKind::ProducerClosed => 3,
        RuntimeFilterEnvelopeKind::ProducerUnavailable => 4,
        RuntimeFilterEnvelopeKind::Unavailable => 5,
        RuntimeFilterEnvelopeKind::Ack => 6,
        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => 7,
        RuntimeFilterEnvelopeKind::DegradedLogical => 8,
        RuntimeFilterEnvelopeKind::FinalArtifact => 9,
    };
    let mut digest = Sha256::new();
    digest.update([kind_tag]);
    digest.update(envelope.query_id().high().to_le_bytes());
    digest.update(envelope.query_id().low().to_le_bytes());
    digest.update(envelope.channel_id().get().to_le_bytes());
    digest.update(envelope.deployment_epoch().get().to_le_bytes());
    match envelope.producer_open() {
        Some(open) => {
            digest.update([1]);
            digest.update(open.local_partition_count().get().to_le_bytes());
        }
        None => digest.update([0]),
    }
    match envelope.accept_status() {
        Some(RuntimeFilterAcceptStatus::Accepted) => digest.update([1]),
        Some(RuntimeFilterAcceptStatus::Duplicate) => digest.update([2]),
        Some(RuntimeFilterAcceptStatus::Rejected) => digest.update([3]),
        None => digest.update([0]),
    }
    digest.update(envelope.schema_digest());
    digest.update(envelope.payload());
    digest.finalize().into()
}

/// The result of applying an ack to the buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvelopeAckOutcome {
    /// The buffered frame was released after an `Accepted` ack.
    Released,
    /// The peer reported it had already seen this delivery; the frame is released
    /// and never re-transmitted.
    ReleasedOnDuplicate,
    /// The peer rejected the delivery. The frame is released (retry stops), but this
    /// is a running-contract corruption for the route, surfaced rather than swallowed
    /// — a later task turns it into a structured event.
    Rejected,
    /// No buffered frame matched the acked identity: a duplicate or out-of-order ack
    /// for an entry already released. A no-op.
    Unknown,
}

/// The result of one [`ReliableEnvelopeTransport::drive_retries`] tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReliableTransportTick {
    retried: usize,
    failed_open: Vec<RuntimeFilterRouteIdentity>,
    failed_open_work: Vec<ReliableFailedOpenWork>,
}

impl ReliableTransportTick {
    /// How many buffered frames were re-handed to the sink on this tick.
    pub(crate) fn retried(&self) -> usize {
        self.retried
    }

    /// The delivery route identities that outlived their deadline on this tick and
    /// were released. Their routes are degraded (failed open); the query is
    /// unaffected. A later task emits a structured degradation event for each.
    pub(crate) fn failed_open(&self) -> &[RuntimeFilterRouteIdentity] {
        &self.failed_open
    }

    pub(crate) fn failed_open_work(&self) -> &[ReliableFailedOpenWork] {
        &self.failed_open_work
    }

    /// True when the tick neither retried nor failed anything open.
    pub(crate) fn is_quiescent(&self) -> bool {
        self.retried == 0 && self.failed_open.is_empty()
    }
}

/// Query-scoped sender-side reliable transport. See the module docs for the model.
pub(crate) struct ReliableEnvelopeTransport {
    sink: Arc<dyn RuntimeFilterEnvelopeSink>,
    // Test-only override so a service assembled with the live production sink can be
    // pointed at a recording / drivable fake without threading the sink through the
    // ~30 `new_with_dependencies` call sites. Mirrors the service's other
    // `Mutex<Option<..>>` test seams.
    #[cfg(test)]
    sink_override: Mutex<Option<Arc<dyn RuntimeFilterEnvelopeSink>>>,
    #[cfg(test)]
    before_event_emit: Mutex<Option<Arc<dyn Fn(TransportEventKind) + Send + Sync>>>,
    #[cfg(test)]
    admitted_envelopes: Mutex<Vec<(RuntimeFilterRemoteRoute, Arc<RuntimeFilterEnvelope>)>>,
    clock: Arc<dyn RuntimeFilterClock>,
    policy: Mutex<ReliableTransportPolicy>,
    pending: Mutex<PendingBuffer>,
    next_sequence: AtomicU64,
    lifecycle: LifecycleBarrier,
    // The RFD-3 lifecycle event sink the Service assembles from its own `EventEmitter`.
    // Structured `TransportEnvelope` events flow through this SAME sink — never a second
    // registry — so the sender-side transport lifecycle is observable end to end.
    event_sink: Arc<dyn RuntimeFilterEventSink>,
}

struct TransportCall<'a> {
    transport: &'a ReliableEnvelopeTransport,
    permit: Option<LifecyclePermit<'a>>,
}

impl<'a> TransportCall<'a> {
    fn admit(transport: &'a ReliableEnvelopeTransport) -> Option<Self> {
        Some(Self {
            transport,
            permit: Some(transport.lifecycle.try_admit()?),
        })
    }
}

impl Drop for TransportCall<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.transport.finish_close_if_requested();
    }
}

impl ReliableEnvelopeTransport {
    pub(crate) fn new(
        sink: Arc<dyn RuntimeFilterEnvelopeSink>,
        clock: Arc<dyn RuntimeFilterClock>,
        policy: ReliableTransportPolicy,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        Self {
            sink,
            #[cfg(test)]
            sink_override: Mutex::new(None),
            #[cfg(test)]
            before_event_emit: Mutex::new(None),
            #[cfg(test)]
            admitted_envelopes: Mutex::new(Vec::new()),
            clock,
            policy: Mutex::new(policy),
            pending: Mutex::new(PendingBuffer::default()),
            next_sequence: AtomicU64::new(1),
            lifecycle: LifecycleBarrier::new(),
            event_sink,
        }
    }

    /// Assemble the production transport with one query-scoped bounded live gRPC sink.
    pub(crate) fn for_query(
        clock: Arc<dyn RuntimeFilterClock>,
        event_sink: Arc<dyn RuntimeFilterEventSink>,
    ) -> Self {
        Self::new(
            GrpcRuntimeFilterEnvelopeSink::new(),
            clock,
            ReliableTransportPolicy::default(),
            event_sink,
        )
    }

    pub(crate) fn configure_policy(&self, policy: ReliableTransportPolicy) -> Result<(), String> {
        let mut current = self
            .policy
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *current == policy {
            return Ok(());
        }
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !pending.entries.is_empty() || !pending.completed.is_empty() {
            return Err(
                "runtime filter transport policy cannot change after delivery starts".into(),
            );
        }
        drop(pending);
        *current = policy;
        Ok(())
    }

    pub(crate) fn shutdown(&self) {
        self.close_from_request();
    }

    fn finish_close_if_requested(&self) {
        if self.lifecycle.is_closing() {
            self.close_from_request();
        }
    }

    fn close_from_request(&self) {
        match self.lifecycle.request_close() {
            CloseRole::Closed | CloseRole::Deferred => return,
            CloseRole::Follower => {
                self.lifecycle.wait_until_closed();
                return;
            }
            CloseRole::Leader => self.lifecycle.wait_for_quiescence(),
        }
        let entered_while_panicking = std::thread::panicking();
        let completion = FinalizerCompletion::new(&self.lifecycle);
        let mut first_panic = None;
        // The lifecycle mutex is not held across trait calls. A synchronous shutdown
        // reentry observes this thread as the finalizer and returns deferred; this
        // finalizer then completes the close and wakes every duplicate caller.
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.sink.shutdown()))
        {
            retain_first_finalizer_panic(&mut first_panic, payload);
        }
        #[cfg(test)]
        if let Some(sink) = self
            .sink_override
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .cloned()
        {
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.shutdown()))
            {
                retain_first_finalizer_panic(&mut first_panic, payload);
            }
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.entries.clear();
        pending.allocation_refs.clear();
        pending.completed.clear();
        pending.completed_order.clear();
        pending.retired.bits.clear();
        pending.bytes = 0;
        drop(pending);
        drop(completion);
        finish_finalizer_panic(first_panic, entered_while_panicking);
    }

    /// Emit a structured transport lifecycle event through the query's RFD-3 sink.
    fn emit(
        &self,
        identity: TransportRouteEventIdentity,
        kind: TransportEventKind,
        bytes: usize,
    ) -> bool {
        #[cfg(test)]
        if let Some(hook) = self
            .before_event_emit
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .cloned()
        {
            hook(kind);
        }
        let Some(_call) = TransportCall::admit(self) else {
            return false;
        };
        self.event_sink
            .record(RuntimeFilterEvent::TransportEnvelope {
                identity,
                kind,
                bytes,
            });
        true
    }

    /// Offer `frame` for reliable delivery to `route`: buffer it for ack-release and
    /// bounded retry and hand it to the underlying sink once, UNLESS a self-owned
    /// buffer ceiling would be exceeded.
    ///
    /// On success returns [`ReliableSendOutcome::Buffered`] with the delivery route
    /// identity the transport stamped, so an ack can later address exactly this
    /// in-flight frame. When the pending-entry count or retained envelope-and-route byte
    /// ceiling would be exceeded the frame is NOT buffered and NOT transmitted, and
    /// [`ReliableSendOutcome::ResourceLimit`] is returned — an explicit resource
    /// rejection the caller degrades the route on, distinct from the deadline fail-open.
    pub(crate) fn send(
        &self,
        route: &RuntimeFilterRemoteRoute,
        frame: Arc<EncodedArtifactFrame>,
        identity: TransportRouteEventIdentity,
    ) -> ReliableSendOutcome {
        self.send_kind(route, frame, identity, RuntimeFilterEnvelopeKind::Artifact)
    }

    pub(crate) fn send_kind(
        &self,
        route: &RuntimeFilterRemoteRoute,
        frame: Arc<EncodedArtifactFrame>,
        identity: TransportRouteEventIdentity,
        kind: RuntimeFilterEnvelopeKind,
    ) -> ReliableSendOutcome {
        let sequence = ProducerSequence::new(self.next_sequence.fetch_add(1, Ordering::Relaxed));
        let route_identity = RuntimeFilterRouteIdentity::delivery(
            DeliveryRouteIdentity::try_new(route.route_edge_id(), sequence)
                .expect("installed delivery route carries non-zero coordinates"),
        );
        let common = identity.common();
        let envelope = Arc::new(
            RuntimeFilterEnvelope::try_new(
                kind,
                common.query_id(),
                common.channel_id(),
                common.epoch(),
                route_identity,
                None,
                None,
                frame.profile_digest(),
                frame.payload().to_vec(),
            )
            .expect("installed delivery route and encoded frame form a valid envelope"),
        );
        self.send_prepared(route, envelope, identity)
            .expect("transport-assigned delivery identity cannot conflict")
    }

    pub(crate) fn send_envelope(
        &self,
        route: &RuntimeFilterRemoteRoute,
        envelope: Arc<RuntimeFilterEnvelope>,
        identity: TransportRouteEventIdentity,
    ) -> Result<ReliableSendOutcome, ReliableSendError> {
        self.send_prepared(route, envelope, identity)
    }

    fn send_prepared(
        &self,
        route: &RuntimeFilterRemoteRoute,
        envelope: Arc<RuntimeFilterEnvelope>,
        identity: TransportRouteEventIdentity,
    ) -> Result<ReliableSendOutcome, ReliableSendError> {
        let Some(call) = TransportCall::admit(self) else {
            return Ok(ReliableSendOutcome::Shutdown);
        };
        let key = PendingKey::from_identity(envelope.route_identity());
        let route_identity = envelope.route_identity().clone();
        let allocation = Arc::as_ptr(&envelope) as usize;
        let retained_bytes = envelope.retained_bytes();
        let route_retained_bytes = route.retained_bytes();
        let fingerprint = envelope_fingerprint(&envelope);
        let route_digest = route_fingerprint(route);
        let now = self.clock.now();
        let bytes = envelope.payload().len();
        let policy = *self
            .policy
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(completed) = pending.completed.get(&key) {
                if completed.route_fingerprint == route_digest
                    && completed.fingerprint == fingerprint
                {
                    return Ok(ReliableSendOutcome::Buffered(route_identity));
                }
                return Err(ReliableSendError::IdentityConflict);
            }
            if let Some(existing) = pending.entries.get(&key) {
                if existing.route == *route && existing.envelope.as_ref() == envelope.as_ref() {
                    return Ok(ReliableSendOutcome::Buffered(route_identity));
                }
                return Err(ReliableSendError::IdentityConflict);
            }
            if pending.retired.might_contain(key) {
                return Err(ReliableSendError::RetiredIdentity);
            }
            if let Err(limit) = pending.admit(
                key,
                PendingEntry {
                    envelope: Arc::clone(&envelope),
                    allocation,
                    retained_bytes,
                    route_retained_bytes,
                    route: route.clone(),
                    attempts: 1,
                    first_sent_at: now,
                    last_sent_at: now,
                    event_identity: identity,
                },
                policy.max_pending_entries,
                policy.max_pending_bytes,
            ) {
                return Ok(ReliableSendOutcome::ResourceLimit(limit));
            }
        }
        #[cfg(test)]
        self.admitted_envelopes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((route.clone(), Arc::clone(&envelope)));
        let transport_envelope =
            RuntimeFilterTransportEnvelope::new(Arc::clone(&envelope), policy.deadline);
        let submit = self
            .resolve_sink()
            .try_send(route.clone(), transport_envelope);
        if !self.lifecycle.is_running() {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(entry) = pending.entries.remove(&key) {
                pending.release(&entry);
            }
            return Ok(ReliableSendOutcome::Shutdown);
        }
        drop(call);
        let outcome = match submit {
            SinkSubmitOutcome::Submitted => {
                if self.emit(identity, TransportEventKind::Sent, bytes) {
                    ReliableSendOutcome::Buffered(route_identity)
                } else {
                    ReliableSendOutcome::Shutdown
                }
            }
            SinkSubmitOutcome::QueueFull => ReliableSendOutcome::Buffered(route_identity),
            SinkSubmitOutcome::Shutdown => {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if let Some(entry) = pending.entries.remove(&key) {
                    pending.release(&entry);
                }
                ReliableSendOutcome::Shutdown
            }
        };
        Ok(outcome)
    }

    /// Apply an ack for `identity` with `status`, releasing the matching buffered
    /// frame if present.
    pub(crate) fn on_ack(
        &self,
        identity: &RuntimeFilterRouteIdentity,
        status: RuntimeFilterAcceptStatus,
    ) -> EnvelopeAckOutcome {
        let Some(call) = TransportCall::admit(self) else {
            return EnvelopeAckOutcome::Unknown;
        };
        let (outcome, event, _failed_open_work) = self.on_ack_admitted(identity, status);
        drop(call);
        if let Some((event_identity, bytes)) = event {
            let _ = self.emit(event_identity, TransportEventKind::Acked(status), bytes);
        }
        outcome
    }

    fn on_ack_admitted(
        &self,
        identity: &RuntimeFilterRouteIdentity,
        status: RuntimeFilterAcceptStatus,
    ) -> (
        EnvelopeAckOutcome,
        Option<(TransportRouteEventIdentity, usize)>,
        Option<ReliableFailedOpenWork>,
    ) {
        let key = PendingKey::from_identity(identity);
        let max_completed = self
            .policy
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .max_pending_entries;
        let released = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            pending.entries.remove(&key).map(|entry| {
                pending.release(&entry);
                if matches!(
                    status,
                    RuntimeFilterAcceptStatus::Accepted | RuntimeFilterAcceptStatus::Duplicate
                ) {
                    pending.record_completed(key, &entry, max_completed);
                }
                entry
            })
        };
        match released {
            // Already released, or a duplicate / out-of-order ack for an identity that
            // is no longer in flight. A no-op — never a re-delivery, and no event.
            None => (EnvelopeAckOutcome::Unknown, None, None),
            Some(entry) => {
                let outcome = match status {
                    RuntimeFilterAcceptStatus::Accepted => EnvelopeAckOutcome::Released,
                    RuntimeFilterAcceptStatus::Duplicate => EnvelopeAckOutcome::ReleasedOnDuplicate,
                    RuntimeFilterAcceptStatus::Rejected => EnvelopeAckOutcome::Rejected,
                };
                let event = Some((entry.event_identity, entry.envelope.payload().len()));
                let failed_open_work = matches!(status, RuntimeFilterAcceptStatus::Rejected)
                    .then_some(ReliableFailedOpenWork {
                        route: entry.route,
                        envelope: entry.envelope,
                        event_identity: entry.event_identity,
                        reason: TransportFailOpenReason::ContractRejected,
                    });
                (outcome, event, failed_open_work)
            }
        }
    }

    /// Advance the transport to `now`: re-hand due unacked frames to the sink under
    /// the bounded attempt count, and release + fail open any frame past its
    /// deadline. Explicit and side-effect-scoped — no background thread.
    pub(crate) fn drive_retries(&self, now: Instant) -> ReliableTransportTick {
        let Some(call) = TransportCall::admit(self) else {
            return ReliableTransportTick::default();
        };
        let (tick, events) = self.drive_retries_admitted(now);
        drop(call);
        for (identity, kind, bytes) in events {
            let _ = self.emit(identity, kind, bytes);
        }
        tick
    }

    fn drive_retries_admitted(
        &self,
        now: Instant,
    ) -> (
        ReliableTransportTick,
        Vec<(TransportRouteEventIdentity, TransportEventKind, usize)>,
    ) {
        let policy = *self
            .policy
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut to_send: Vec<(
            RuntimeFilterRemoteRoute,
            Arc<RuntimeFilterEnvelope>,
            TransportRouteEventIdentity,
            usize,
        )> = Vec::new();
        let mut failed_open: Vec<(PendingKey, ReliableFailedOpenWork, usize)> = Vec::new();
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut expired: Vec<PendingKey> = Vec::new();
            for (key, entry) in pending.entries.iter_mut() {
                // Deadline wins over retry: past the deadline the frame is dropped and
                // the route fails open. No panic, no error surfaced to the query.
                if now.saturating_duration_since(entry.first_sent_at) >= policy.deadline {
                    expired.push(*key);
                    continue;
                }
                // Under the attempt bound and past the retry interval: re-hand it. Once
                // the count is exhausted the frame stays buffered until its deadline, so
                // a late ack can still release it cleanly.
                if entry.attempts < policy.max_attempts
                    && now.saturating_duration_since(entry.last_sent_at) >= policy.retry_interval
                {
                    entry.attempts += 1;
                    entry.last_sent_at = now;
                    to_send.push((
                        entry.route.clone(),
                        Arc::clone(&entry.envelope),
                        entry.event_identity,
                        entry.envelope.payload().len(),
                    ));
                }
            }
            // Remove the deadline-expired frames, reclaiming their byte accounting.
            for key in expired {
                if let Some(entry) = pending.entries.remove(&key) {
                    pending.release(&entry);
                    let bytes = entry.envelope.payload().len();
                    failed_open.push((
                        key,
                        ReliableFailedOpenWork {
                            route: entry.route,
                            envelope: entry.envelope,
                            event_identity: entry.event_identity,
                            reason: TransportFailOpenReason::Deadline,
                        },
                        bytes,
                    ));
                }
            }
        }
        // Re-hand retries outside the buffer lock (see `send`). The re-hand order
        // within a tick is unspecified (it follows HashMap iteration), so no caller
        // may depend on it; only `failed_open` is sorted below for determinism.
        let mut events = Vec::new();
        for (route, envelope, identity, bytes) in &to_send {
            if !self.lifecycle.is_running() {
                break;
            }
            if matches!(
                self.resolve_sink().try_send(
                    route.clone(),
                    RuntimeFilterTransportEnvelope::new(Arc::clone(envelope), policy.deadline),
                ),
                SinkSubmitOutcome::Submitted
            ) && self.lifecycle.is_running()
            {
                events.push((*identity, TransportEventKind::Retried, *bytes));
            }
        }
        // Sort by delivery key so the tick's `failed_open` order and the deadline events
        // are deterministic regardless of HashMap iteration order.
        failed_open.sort_unstable_by_key(|(key, _, _)| *key);
        let mut failed_identities = Vec::with_capacity(failed_open.len());
        let mut failed_open_work = Vec::with_capacity(failed_open.len());
        for (key, work, bytes) in failed_open {
            if self.lifecycle.is_running() {
                events.push((
                    work.event_identity,
                    TransportEventKind::FailedOpen(TransportFailOpenReason::Deadline),
                    bytes,
                ));
            }
            failed_identities.push(key.into_route_identity());
            failed_open_work.push(work);
        }
        (
            ReliableTransportTick {
                retried: to_send.len(),
                failed_open: failed_identities,
                failed_open_work,
            },
            events,
        )
    }

    /// Drain every currently available unary completion before advancing retry and
    /// deadline state. Network failures leave entries pending; ACK rejection and
    /// strict response-contract failures release and fail the route open.
    pub(crate) fn drain_completions_and_drive(&self, now: Instant) -> ReliableTransportTick {
        let Some(call) = TransportCall::admit(self) else {
            return ReliableTransportTick::default();
        };
        let mut contract_failed = Vec::new();
        let mut contract_failed_work = Vec::new();
        let mut events = Vec::new();
        while let Some(completion) = self.resolve_sink().try_recv_completion() {
            if !self.lifecycle.is_running() {
                return ReliableTransportTick::default();
            }
            match completion {
                SinkCompletion::Ack(identity, status) => {
                    let (outcome, event, work) = self.on_ack_admitted(&identity, status);
                    if matches!(outcome, EnvelopeAckOutcome::Rejected) {
                        contract_failed.push(identity);
                        if let Some(work) = work {
                            contract_failed_work.push(work);
                        }
                    }
                    if let Some((event_identity, bytes)) = event {
                        events.push((event_identity, TransportEventKind::Acked(status), bytes));
                    }
                }
                SinkCompletion::TransportFailure(identity, error) if error.is_contract() => {
                    let (outcome, event, work) =
                        self.on_ack_admitted(&identity, RuntimeFilterAcceptStatus::Rejected);
                    if matches!(outcome, EnvelopeAckOutcome::Rejected) {
                        contract_failed.push(identity);
                        if let Some(work) = work {
                            contract_failed_work.push(work);
                        }
                    }
                    if let Some((event_identity, bytes)) = event {
                        events.push((
                            event_identity,
                            TransportEventKind::Acked(RuntimeFilterAcceptStatus::Rejected),
                            bytes,
                        ));
                    }
                }
                SinkCompletion::TransportFailure(_identity, _error) => {
                    // A network failure is retryable. The pending entry stays owned by
                    // this transport until a later ACK, retry deadline, or shutdown.
                }
            }
        }
        if !self.lifecycle.is_running() {
            return ReliableTransportTick::default();
        }
        let (mut tick, mut drive_events) = self.drive_retries_admitted(now);
        events.append(&mut drive_events);
        contract_failed.append(&mut tick.failed_open);
        contract_failed.sort_by_key(PendingKey::from_identity);
        contract_failed_work
            .sort_by_key(|work| PendingKey::from_identity(work.envelope.route_identity()));
        tick.failed_open = contract_failed;
        tick.failed_open_work.extend(contract_failed_work);
        drop(call);
        for (identity, kind, bytes) in events {
            let _ = self.emit(identity, kind, bytes);
        }
        tick
    }

    /// Resolve the sink to transmit through: the test override when installed,
    /// otherwise the sink the transport was constructed with.
    fn resolve_sink(&self) -> Arc<dyn RuntimeFilterEnvelopeSink> {
        #[cfg(test)]
        {
            if let Some(sink) = self
                .sink_override
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
            {
                return Arc::clone(sink);
            }
        }
        Arc::clone(&self.sink)
    }

    #[cfg(test)]
    pub(super) fn pending_len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .len()
    }

    /// The deterministic inline-envelope plus retained-payload bytes (per unique allocation)
    /// and per-entry retained route bytes currently buffered. Retries do not re-meter either
    /// charge. Test seam for the self-owned byte ceiling and release-to-zero assertion.
    #[cfg(test)]
    pub(super) fn pending_bytes(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .bytes
    }

    #[cfg(test)]
    fn completed_witness_len(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .completed
            .len()
    }

    #[cfg(test)]
    pub(super) fn saturate_retired_filter_for_test(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        pending.retired.ensure_capacity();
        pending.retired.bits.fill(u64::MAX);
    }

    /// Override the live production sink with a fake. This test-only seam lets
    /// service-level delivery tests observe and drive outbound transport deterministically.
    #[cfg(test)]
    pub(crate) fn set_sink_for_test(&self, sink: Arc<dyn RuntimeFilterEnvelopeSink>) {
        *self
            .sink_override
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sink);
    }

    #[cfg(test)]
    fn set_before_event_emit_for_test(&self, hook: Arc<dyn Fn(TransportEventKind) + Send + Sync>) {
        *self
            .before_event_emit
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn admitted_envelopes_for_test(
        &self,
    ) -> Vec<(RuntimeFilterRemoteRoute, Arc<RuntimeFilterEnvelope>)> {
        self.admitted_envelopes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use super::{
        EnvelopeAckOutcome, ReliableEnvelopeTransport, ReliableSendOutcome,
        ReliableTransportPolicy, TransportResourceLimit,
    };
    use crate::common::types::UniqueId;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::events::{
        RuntimeFilterEvent, RuntimeFilterEventIdentity, RuntimeFilterEventSink, TransportEventKind,
        TransportFailOpenReason, TransportRouteEventIdentity,
    };
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::routing::{RuntimeFilterRemoteRoute, RuntimeFilterRouteRole};
    use crate::runtime_filter::port::support::RuntimeFilterClock;
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, ProducerInstanceRouteIdentity, ProducerOpenMetadata,
        RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind,
        RuntimeFilterRouteIdentity, RuntimeFilterTransportEnvelope,
    };
    use crate::runtime_filter::router::remote::{
        RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome, SinkTransportError,
    };

    /// A no-op lifecycle sink for the Task-2/Task-4 mechanics tests, which assert buffer /
    /// retry / ack / deadline behavior and do not observe events.
    struct NoopEvents;

    impl RuntimeFilterEventSink for NoopEvents {
        fn record(&self, _event: RuntimeFilterEvent) {}
    }

    /// Recording lifecycle sink used by the `transport_events_*` tests: captures every
    /// event so a test can assert the transport's structured `TransportEnvelope`
    /// emissions (kind + byte size + accept status) flow through the existing sink.
    #[derive(Default)]
    struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for RecordingEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(event);
        }
    }

    impl RecordingEvents {
        fn transport_events(
            &self,
        ) -> Vec<(TransportRouteEventIdentity, TransportEventKind, usize)> {
            self.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter_map(|event| match event {
                    RuntimeFilterEvent::TransportEnvelope {
                        identity,
                        kind,
                        bytes,
                    } => Some((*identity, *kind, *bytes)),
                    _ => None,
                })
                .collect()
        }
    }

    #[derive(Clone, Copy)]
    enum BlockedTransportEvent {
        Acked,
        FailedOpen,
    }

    struct BlockingTransportEvents {
        blocked: BlockedTransportEvent,
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        recorded: Mutex<Vec<RuntimeFilterEvent>>,
    }

    impl RuntimeFilterEventSink for BlockingTransportEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            let targeted = matches!(
                (&self.blocked, &event),
                (
                    BlockedTransportEvent::Acked,
                    RuntimeFilterEvent::TransportEnvelope {
                        kind: TransportEventKind::Acked(_),
                        ..
                    }
                ) | (
                    BlockedTransportEvent::FailedOpen,
                    RuntimeFilterEvent::TransportEnvelope {
                        kind: TransportEventKind::FailedOpen(_),
                        ..
                    }
                )
            );
            if targeted {
                self.entered.send(()).expect("event callback entered");
                self.release
                    .lock()
                    .expect("event release")
                    .recv_timeout(Duration::from_secs(1))
                    .expect("event callback released");
            }
            self.recorded.lock().expect("recorded events").push(event);
        }
    }

    impl BlockingTransportEvents {
        fn target_count(&self) -> usize {
            self.recorded
                .lock()
                .expect("recorded events")
                .iter()
                .filter(|event| {
                    matches!(
                        (&self.blocked, event),
                        (
                            BlockedTransportEvent::Acked,
                            RuntimeFilterEvent::TransportEnvelope {
                                kind: TransportEventKind::Acked(_),
                                ..
                            }
                        ) | (
                            BlockedTransportEvent::FailedOpen,
                            RuntimeFilterEvent::TransportEnvelope {
                                kind: TransportEventKind::FailedOpen(_),
                                ..
                            }
                        )
                    )
                })
                .count()
        }
    }

    /// A stable transport route identity for a given delivery edge. The query /
    /// participant / channel / epoch coordinates are fixed test constants; only the route
    /// edge varies, which is what `send`/`on_ack`/`drive_retries` key their events on.
    fn event_identity(edge: RouteEdgeId) -> TransportRouteEventIdentity {
        TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                UniqueId::new(1, 1),
                RuntimeFilterParticipantId::new(7),
                ChannelId::new(5),
                DeploymentEpoch::new(9),
            ),
            edge,
        )
    }

    impl ReliableEnvelopeTransport {
        /// Test convenience: send with a synthetic transport route identity derived from
        /// the route's own edge, so the Task-2/Task-4 mechanics tests need not thread an
        /// event identity through every call.
        fn send_test(
            &self,
            route: &RuntimeFilterRemoteRoute,
            frame: Arc<EncodedArtifactFrame>,
        ) -> ReliableSendOutcome {
            self.send(route, frame, event_identity(route.route_edge_id()))
        }

        fn lifecycle_closed_for_test(&self) -> bool {
            self.lifecycle
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .phase
                == super::super::LifecyclePhase::Closed
        }
    }

    /// Drivable fake sink: records every (route edge, frame) it is handed so a test
    /// can assert exact send / retry counts and compare the transmitted bytes.
    #[derive(Default)]
    struct RecordingSink {
        sends: Mutex<Vec<(RouteEdgeId, EncodedArtifactFrame)>>,
        envelopes: Mutex<Vec<Arc<RuntimeFilterEnvelope>>>,
        completions: Mutex<VecDeque<SinkCompletion>>,
        shutdown: AtomicBool,
    }

    impl RuntimeFilterEnvelopeSink for RecordingSink {
        fn try_send(
            &self,
            route: RuntimeFilterRemoteRoute,
            envelope: RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            if self.shutdown.load(Ordering::Acquire) {
                return SinkSubmitOutcome::Shutdown;
            }
            let domain_envelope = Arc::clone(envelope.envelope_arc());
            self.envelopes
                .lock()
                .unwrap()
                .push(Arc::clone(&domain_envelope));
            let envelope = domain_envelope.as_ref();
            self.sends.lock().unwrap().push((
                route.route_edge_id(),
                EncodedArtifactFrame::from_parts_for_test(
                    *envelope.schema_digest(),
                    envelope.payload().to_vec(),
                ),
            ));
            SinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            self.completions.lock().unwrap().pop_front()
        }

        fn shutdown(&self) {
            self.shutdown.store(true, Ordering::Release);
        }
    }

    impl RecordingSink {
        fn count(&self) -> usize {
            self.sends.lock().unwrap().len()
        }

        fn edges(&self) -> Vec<RouteEdgeId> {
            self.sends.lock().unwrap().iter().map(|(e, _)| *e).collect()
        }

        fn frames(&self) -> Vec<(RouteEdgeId, EncodedArtifactFrame)> {
            self.sends.lock().unwrap().clone()
        }

        fn envelopes(&self) -> Vec<Arc<RuntimeFilterEnvelope>> {
            self.envelopes.lock().unwrap().clone()
        }

        fn complete(&self, completion: SinkCompletion) {
            self.completions.lock().unwrap().push_back(completion);
        }
    }

    /// Manually advanced clock: the transport reads `now()`; the test moves time.
    struct ManualClock(Mutex<Instant>);

    impl RuntimeFilterClock for ManualClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    impl ManualClock {
        fn new(start: Instant) -> Self {
            Self(Mutex::new(start))
        }

        fn advance(&self, by: Duration) {
            let mut guard = self.0.lock().unwrap();
            *guard += by;
        }
    }

    struct ReentrantShutdownSink {
        transport: Mutex<Weak<ReliableEnvelopeTransport>>,
    }

    struct PanicOnceShutdownSink {
        panic_once: AtomicBool,
    }

    struct NestedUnwindShutdownSink {
        transport: Mutex<Weak<ReliableEnvelopeTransport>>,
    }

    impl RuntimeFilterEnvelopeSink for NestedUnwindShutdownSink {
        fn try_send(
            &self,
            _route: RuntimeFilterRemoteRoute,
            _envelope: RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            self.transport
                .lock()
                .expect("nested-unwind transport")
                .upgrade()
                .expect("transport installed")
                .shutdown();
            panic!("outer transport callback panic");
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            None
        }

        fn shutdown(&self) {
            panic!("secondary transport teardown panic");
        }
    }

    impl RuntimeFilterEnvelopeSink for PanicOnceShutdownSink {
        fn try_send(
            &self,
            _route: RuntimeFilterRemoteRoute,
            _envelope: RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            SinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            None
        }

        fn shutdown(&self) {
            if self.panic_once.swap(false, Ordering::AcqRel) {
                panic!("intentional sink shutdown panic");
            }
        }
    }

    impl RuntimeFilterEnvelopeSink for ReentrantShutdownSink {
        fn try_send(
            &self,
            _route: RuntimeFilterRemoteRoute,
            _envelope: RuntimeFilterTransportEnvelope,
        ) -> SinkSubmitOutcome {
            self.transport
                .lock()
                .expect("reentrant transport")
                .upgrade()
                .expect("transport installed")
                .shutdown();
            SinkSubmitOutcome::Submitted
        }

        fn try_recv_completion(&self) -> Option<SinkCompletion> {
            None
        }

        fn shutdown(&self) {}
    }

    struct Harness {
        transport: ReliableEnvelopeTransport,
        sink: Arc<RecordingSink>,
        clock: Arc<ManualClock>,
    }

    fn harness(policy: ReliableTransportPolicy) -> Harness {
        let sink = Arc::new(RecordingSink::default());
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let transport = ReliableEnvelopeTransport::new(
            sink.clone(),
            clock.clone(),
            policy,
            Arc::new(NoopEvents),
        );
        Harness {
            transport,
            sink,
            clock,
        }
    }

    // Roomy buffer ceilings so the pre-existing retry/ack/deadline tests never trip a
    // resource limit; the `transport_bounded_*` tests set tight ceilings explicitly.
    const ROOMY_MAX_ENTRIES: usize = 1024;
    const ROOMY_MAX_BYTES: usize = 1 << 30;

    fn policy(retry_ms: u64, max_attempts: u32, deadline_ms: u64) -> ReliableTransportPolicy {
        policy_bounded(
            retry_ms,
            max_attempts,
            deadline_ms,
            ROOMY_MAX_ENTRIES,
            ROOMY_MAX_BYTES,
        )
    }

    fn policy_bounded(
        retry_ms: u64,
        max_attempts: u32,
        deadline_ms: u64,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> ReliableTransportPolicy {
        ReliableTransportPolicy::new(
            Duration::from_millis(retry_ms),
            max_attempts,
            Duration::from_millis(deadline_ms),
            max_pending_entries,
            max_pending_bytes,
        )
    }

    // A frame whose serialized payload is exactly `bytes` long, tagged by `tag` so
    // distinct sends can be told apart. Byte-ceiling tests size payloads against a cap.
    fn frame_sized(tag: u8, bytes: usize) -> Arc<EncodedArtifactFrame> {
        Arc::new(EncodedArtifactFrame::from_parts_for_test(
            [tag; 32],
            vec![tag; bytes],
        ))
    }

    fn retained_envelope_bytes(payload_bytes: usize) -> usize {
        size_of::<RuntimeFilterEnvelope>() + payload_bytes + route(30).retained_bytes()
    }

    fn route(edge: u32) -> RuntimeFilterRemoteRoute {
        RuntimeFilterRemoteRoute::new(
            RouteEdgeId::new(edge),
            RuntimeFilterParticipantId::new(7),
            RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
            RuntimeFilterRouteRole::Consumer(BindingId::new(edge)),
        )
        .unwrap()
    }

    fn contribution_envelope(sequence: u64, tag: u8) -> Arc<RuntimeFilterEnvelope> {
        Arc::new(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Contribution,
                UniqueId::new(1, 1),
                ChannelId::new(5),
                DeploymentEpoch::new(9),
                RuntimeFilterRouteIdentity::contribution(
                    ContributionRouteIdentity::try_new(
                        BindingId::new(91),
                        UniqueId::new(92, 93),
                        PartitionId::new(0),
                        ProducerSequence::new(sequence),
                    )
                    .unwrap(),
                ),
                Some(ProducerOpenMetadata::try_new(1).unwrap()),
                None,
                &[tag; 32],
                vec![tag; 8],
            )
            .unwrap(),
        )
    }

    fn producer_unavailable_envelope(tag: u8) -> Arc<RuntimeFilterEnvelope> {
        Arc::new(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
                UniqueId::new(1, 1),
                ChannelId::new(5),
                DeploymentEpoch::new(9),
                RuntimeFilterRouteIdentity::producer_instance(
                    ProducerInstanceRouteIdentity::try_new(
                        BindingId::new(91),
                        UniqueId::new(92, 93),
                    )
                    .unwrap(),
                ),
                None,
                None,
                &[tag; 32],
                vec![tag],
            )
            .unwrap(),
        )
    }

    fn producer_closed_envelope(sequence: u64) -> Arc<RuntimeFilterEnvelope> {
        Arc::new(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::ProducerClosed,
                UniqueId::new(1, 1),
                ChannelId::new(5),
                DeploymentEpoch::new(9),
                RuntimeFilterRouteIdentity::contribution(
                    ContributionRouteIdentity::try_new(
                        BindingId::new(91),
                        UniqueId::new(92, 93),
                        PartitionId::new(0),
                        ProducerSequence::new(sequence),
                    )
                    .unwrap(),
                ),
                Some(ProducerOpenMetadata::try_new(1).unwrap()),
                None,
                &[7; 32],
                Vec::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn contribution_retry_preserves_canonical_identity_and_payload() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));
        let envelope = contribution_envelope(7, 44);
        let expected_identity = envelope.route_identity().clone();
        transport
            .send_envelope(
                &route(30),
                Arc::clone(&envelope),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);

        let seen = sink.envelopes();
        assert_eq!(seen.len(), 2);
        assert!(Arc::ptr_eq(&seen[0], &envelope));
        assert!(Arc::ptr_eq(&seen[0], &seen[1]));
        assert_eq!(seen[0].route_identity(), &expected_identity);
        assert_eq!(seen[0].payload().as_ptr(), seen[1].payload().as_ptr());
        assert_eq!(seen[0].payload(), &[44; 8]);
    }

    #[test]
    fn accepted_or_duplicate_ack_releases_contribution() {
        for status in [
            RuntimeFilterAcceptStatus::Accepted,
            RuntimeFilterAcceptStatus::Duplicate,
        ] {
            let Harness {
                transport,
                sink,
                clock,
            } = harness(policy(100, 3, 10_000));
            let envelope = contribution_envelope(
                if status == RuntimeFilterAcceptStatus::Accepted {
                    8
                } else {
                    9
                },
                45,
            );
            let identity = envelope.route_identity().clone();
            transport
                .send_envelope(
                    &route(30),
                    Arc::clone(&envelope),
                    event_identity(RouteEdgeId::new(30)),
                )
                .unwrap()
                .expect_buffered();
            assert_eq!(transport.pending_len(), 1);
            assert_eq!(transport.pending_bytes(), retained_envelope_bytes(8));
            let adjacent_identity = contribution_envelope(
                if status == RuntimeFilterAcceptStatus::Accepted {
                    108
                } else {
                    109
                },
                45,
            )
            .route_identity()
            .clone();
            assert_eq!(
                transport.on_ack(&adjacent_identity, status),
                EnvelopeAckOutcome::Unknown
            );
            assert_eq!(transport.pending_len(), 1);
            assert!(matches!(
                transport.on_ack(&identity, status),
                EnvelopeAckOutcome::Released | EnvelopeAckOutcome::ReleasedOnDuplicate
            ));
            assert_eq!(transport.pending_len(), 0);
            assert_eq!(transport.pending_bytes(), 0);
            clock.advance(Duration::from_millis(100));
            assert!(transport.drive_retries(clock.now()).is_quiescent());
            assert_eq!(sink.count(), 1);
        }
    }

    #[test]
    fn canonical_identity_conflict_survives_accepted_or_duplicate_ack() {
        for status in [
            RuntimeFilterAcceptStatus::Accepted,
            RuntimeFilterAcceptStatus::Duplicate,
        ] {
            let Harness {
                transport,
                sink,
                clock: _clock,
            } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));
            let original = contribution_envelope(70, 1);
            let identity = original.route_identity().clone();
            transport
                .send_envelope(
                    &route(30),
                    Arc::clone(&original),
                    event_identity(RouteEdgeId::new(30)),
                )
                .unwrap()
                .expect_buffered();
            assert!(matches!(
                transport.on_ack(&identity, status),
                EnvelopeAckOutcome::Released | EnvelopeAckOutcome::ReleasedOnDuplicate
            ));

            transport
                .send_envelope(
                    &route(30),
                    Arc::clone(&original),
                    event_identity(RouteEdgeId::new(30)),
                )
                .expect("an exact acknowledged replay stays deterministic")
                .expect_buffered();
            assert_eq!(transport.pending_len(), 0);
            assert_eq!(sink.count(), 1, "an exact replay is never re-enqueued");

            let conflict = contribution_envelope(70, 2);
            assert_eq!(
                transport
                    .send_envelope(&route(30), conflict, event_identity(RouteEdgeId::new(30)),),
                Err(super::ReliableSendError::IdentityConflict)
            );
            assert_eq!(transport.pending_len(), 0);
            assert_eq!(
                sink.count(),
                1,
                "a conflicting replay never reaches the wire"
            );
        }
    }

    #[test]
    fn completed_identity_witnesses_are_bounded_by_the_entry_ceiling() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));
        let mut latest = None;
        for sequence in 90..93 {
            let envelope = contribution_envelope(sequence, sequence as u8);
            let identity = envelope.route_identity().clone();
            transport
                .send_envelope(
                    &route(30),
                    Arc::clone(&envelope),
                    event_identity(RouteEdgeId::new(30)),
                )
                .unwrap()
                .expect_buffered();
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted);
            latest = Some(envelope);
        }
        assert_eq!(transport.completed_witness_len(), 2);
        let sends = sink.count();
        transport
            .send_envelope(
                &route(30),
                latest.unwrap(),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(sink.count(), sends);
    }

    #[test]
    fn evicted_retired_identity_cannot_reenter_while_a_gap_identity_can() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));
        for sequence in 1..=3 {
            let envelope = contribution_envelope(sequence, sequence as u8);
            let identity = envelope.route_identity().clone();
            transport
                .send_envelope(&route(30), envelope, event_identity(RouteEdgeId::new(30)))
                .unwrap()
                .expect_buffered();
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted);
        }
        assert_eq!(transport.completed_witness_len(), 2);
        assert_eq!(sink.count(), 3);

        assert_eq!(
            transport.send_envelope(
                &route(30),
                contribution_envelope(1, 99),
                event_identity(RouteEdgeId::new(30)),
            ),
            Err(super::ReliableSendError::RetiredIdentity)
        );
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(sink.count(), 3, "retired identity never re-enters the wire");

        transport
            .send_envelope(
                &route(30),
                contribution_envelope(5, 5),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        assert_eq!(transport.pending_len(), 1);
        assert_eq!(
            sink.count(),
            4,
            "a never-retired gap identity remains valid"
        );
    }

    #[test]
    fn saturated_retirement_filter_is_safe_for_pending_retry_and_fresh_delivery() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));
        let pending = contribution_envelope(40, 4);
        transport
            .send_envelope(
                &route(30),
                Arc::clone(&pending),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        transport.saturate_retired_filter_for_test();

        transport
            .send_envelope(&route(30), pending, event_identity(RouteEdgeId::new(30)))
            .expect("pending exact retry precedes probabilistic retirement")
            .expect_buffered();
        assert_eq!(sink.count(), 1, "pending exact retry is not re-enqueued");
        assert_eq!(
            transport.send_envelope(
                &route(30),
                contribution_envelope(41, 5),
                event_identity(RouteEdgeId::new(30)),
            ),
            Err(super::ReliableSendError::RetiredIdentity)
        );
        assert_eq!(sink.count(), 1, "Bloom-only hit never reaches the wire");

        let delivery = transport.send_test(&route(31), frame(9)).expect_buffered();
        assert_eq!(
            sink.count(),
            2,
            "fresh internal delivery bypasses producer retirement"
        );
        assert!(delivery.as_delivery().is_some());
    }

    #[test]
    fn pending_retained_charge_includes_route_and_endpoint_host() {
        let route = route(30);
        let envelope = contribution_envelope(120, 7);
        let expected = envelope.retained_bytes() + route.retained_bytes();
        let Harness {
            transport,
            sink: _sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, expected));
        transport
            .send_envelope(&route, envelope, event_identity(RouteEdgeId::new(30)))
            .unwrap()
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), expected);
    }

    #[test]
    fn retained_envelope_accounting_exact_fit_over_limit_and_retry_stable() {
        let contribution = contribution_envelope(80, 8);
        let contribution_bytes = contribution.retained_bytes() + route(30).retained_bytes();
        let exact = harness(policy_bounded(100, 3, 10_000, 4, contribution_bytes));
        let identity = exact
            .transport
            .send_envelope(
                &route(30),
                Arc::clone(&contribution),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        assert_eq!(exact.transport.pending_bytes(), contribution_bytes);
        exact.clock.advance(Duration::from_millis(100));
        assert_eq!(
            exact.transport.drive_retries(exact.clock.now()).retried(),
            1
        );
        assert_eq!(exact.transport.pending_bytes(), contribution_bytes);
        exact
            .transport
            .on_ack(&identity, RuntimeFilterAcceptStatus::Accepted);
        assert_eq!(exact.transport.pending_bytes(), 0);

        let over = harness(policy_bounded(100, 3, 10_000, 4, contribution_bytes - 1));
        assert_eq!(
            over.transport.send_envelope(
                &route(30),
                contribution,
                event_identity(RouteEdgeId::new(30)),
            ),
            Ok(ReliableSendOutcome::ResourceLimit(
                TransportResourceLimit::SerializedBytes
            ))
        );

        for (edge, envelope) in [
            (31, producer_closed_envelope(81)),
            (32, producer_unavailable_envelope(9)),
        ] {
            let retained = envelope.retained_bytes() + route(edge).retained_bytes();
            assert!(retained > 0);
            let terminal = harness(policy_bounded(100, 3, 10_000, 4, retained));
            terminal
                .transport
                .send_envelope(
                    &route(edge),
                    envelope,
                    event_identity(RouteEdgeId::new(edge)),
                )
                .unwrap()
                .expect_buffered();
            assert_eq!(terminal.transport.pending_bytes(), retained);
        }
    }

    #[test]
    fn async_rejected_or_mismatched_ack_fails_route_open() {
        for completion in [
            None,
            Some(SinkTransportError::contract(
                "runtime filter ACK identity mismatch",
            )),
        ] {
            let Harness {
                transport,
                sink,
                clock,
            } = harness(policy(100, 3, 10_000));
            let envelope = contribution_envelope(10, 46);
            let identity = envelope.route_identity().clone();
            transport
                .send_envelope(
                    &route(30),
                    Arc::clone(&envelope),
                    event_identity(RouteEdgeId::new(30)),
                )
                .unwrap()
                .expect_buffered();
            match completion {
                None => sink.complete(SinkCompletion::Ack(
                    identity.clone(),
                    RuntimeFilterAcceptStatus::Rejected,
                )),
                Some(error) => {
                    sink.complete(SinkCompletion::TransportFailure(identity.clone(), error))
                }
            }

            let tick = transport.drain_completions_and_drive(clock.now());
            assert_eq!(tick.failed_open(), &[identity]);
            assert_eq!(tick.failed_open_work().len(), 1);
            assert_eq!(
                tick.failed_open_work()[0].reason(),
                TransportFailOpenReason::ContractRejected
            );
            assert!(Arc::ptr_eq(
                tick.failed_open_work()[0].envelope(),
                &envelope
            ));
            assert_eq!(transport.pending_len(), 0);
        }
    }

    #[test]
    fn contribution_deadline_degrades_filter_without_query_failure() {
        let Harness {
            transport,
            sink: _,
            clock,
        } = harness(policy(50, 2, 150));
        let envelope = contribution_envelope(11, 47);
        let identity = envelope.route_identity().clone();
        transport
            .send_envelope(
                &route(30),
                Arc::clone(&envelope),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        clock.advance(Duration::from_millis(200));

        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.failed_open(), &[identity]);
        assert_eq!(tick.failed_open_work().len(), 1);
        assert_eq!(
            tick.failed_open_work()[0].reason(),
            TransportFailOpenReason::Deadline
        );
        assert!(Arc::ptr_eq(
            tick.failed_open_work()[0].envelope(),
            &envelope
        ));
        assert_eq!(transport.pending_len(), 0);
    }

    #[test]
    fn producer_unavailable_send_failure_does_not_recurse() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(50, 2, 150));
        let envelope = producer_unavailable_envelope(48);
        transport
            .send_envelope(
                &route(30),
                Arc::clone(&envelope),
                event_identity(RouteEdgeId::new(30)),
            )
            .unwrap()
            .expect_buffered();
        clock.advance(Duration::from_millis(200));

        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.failed_open_work().len(), 1);
        assert_eq!(
            tick.failed_open_work()[0].envelope().kind(),
            RuntimeFilterEnvelopeKind::ProducerUnavailable
        );
        assert_eq!(sink.count(), 1);
        assert_eq!(transport.pending_len(), 0);

        clock.advance(Duration::from_millis(1_000));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(
            sink.count(),
            1,
            "failure must not synthesize another unavailable"
        );
    }

    fn frame(tag: u8) -> Arc<EncodedArtifactFrame> {
        Arc::new(EncodedArtifactFrame::from_parts_for_test(
            [tag; 32],
            vec![tag, tag, tag],
        ))
    }

    #[test]
    fn reliable_transport_send_buffers_frame_and_hands_it_to_the_sink() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));
        let payload = frame(1);

        let identity = transport
            .send_test(&route(30), Arc::clone(&payload))
            .expect_buffered();

        // Buffered exactly once.
        assert_eq!(transport.pending_len(), 1);
        // Handed to the sink exactly once with the authorized edge and the bytes.
        let sends = sink.frames();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, RouteEdgeId::new(30));
        assert_eq!(&sends[0].1, payload.as_ref());
        // The stamped identity is a delivery identity on the same edge.
        assert_eq!(
            identity.as_delivery().unwrap().route_edge_id(),
            RouteEdgeId::new(30)
        );
    }

    #[test]
    fn sink_try_send_can_reenter_shutdown_without_deadlock() {
        let sink = Arc::new(ReentrantShutdownSink {
            transport: Mutex::new(Weak::new()),
        });
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            sink.clone(),
            Arc::new(ManualClock::new(Instant::now())),
            policy(100, 3, 10_000),
            Arc::new(NoopEvents),
        ));
        *sink.transport.lock().expect("reentrant transport") = Arc::downgrade(&transport);

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let sender = std::thread::spawn(move || {
            let outcome = transport.send_test(&route(130), frame(1));
            done_tx.send(outcome).expect("reentrant send complete");
        });
        let outcome = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("sink shutdown reentry deadlocked");
        sender.join().expect("reentrant sender thread");
        assert!(matches!(outcome, ReliableSendOutcome::Shutdown));
    }

    #[test]
    fn sink_shutdown_panic_closes_transport_and_wakes_duplicate_shutdown() {
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            Arc::new(PanicOnceShutdownSink {
                panic_once: AtomicBool::new(true),
            }),
            Arc::new(ManualClock::new(Instant::now())),
            policy(100, 3, 10_000),
            Arc::new(NoopEvents),
        ));
        transport.send_test(&route(144), frame(5)).expect_buffered();
        assert_eq!(transport.pending_len(), 1);

        let first = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let transport = transport.clone();
            move || transport.shutdown()
        }));
        assert!(first.is_err(), "the original sink panic must be resumed");

        let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
        let second_transport = transport.clone();
        let second = std::thread::spawn(move || {
            second_transport.shutdown();
            second_done_tx.send(()).expect("second shutdown complete");
        });
        let second_completed = second_done_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        if second_completed {
            second.join().expect("second shutdown thread");
        }

        assert!(
            second_completed,
            "duplicate shutdown remained blocked after sink panic"
        );
        assert!(transport.lifecycle_closed_for_test());
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn nested_transport_unwind_preserves_outer_panic_in_subprocess() {
        let output = std::process::Command::new(
            std::env::current_exe().expect("current lib-test executable"),
        )
        .arg("nested_transport_unwind_child")
        .arg("--ignored")
        .arg("--nocapture")
        .output()
        .expect("run isolated nested-unwind transport regression");
        assert!(
            output.status.success(),
            "nested transport unwind aborted the child process:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "isolated by nested_transport_unwind_preserves_outer_panic_in_subprocess"]
    fn nested_transport_unwind_child() {
        let sink = Arc::new(NestedUnwindShutdownSink {
            transport: Mutex::new(Weak::new()),
        });
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            sink.clone(),
            Arc::new(ManualClock::new(Instant::now())),
            policy(100, 3, 10_000),
            Arc::new(NoopEvents),
        ));
        *sink.transport.lock().expect("nested-unwind transport") = Arc::downgrade(&transport);

        let outer = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let transport = transport.clone();
            move || transport.send_test(&route(145), frame(6))
        }));
        let payload = match outer {
            Ok(_) => panic!("outer transport callback must panic"),
            Err(payload) => payload,
        };
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("outer transport callback panic"),
            "the original callback panic must survive nested finalization"
        );

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let duplicate_transport = transport.clone();
        let duplicate = std::thread::spawn(move || {
            duplicate_transport.shutdown();
            done_tx.send(()).expect("duplicate shutdown complete");
        });
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("duplicate shutdown must observe Closed");
        duplicate.join().expect("duplicate shutdown thread");
        assert!(transport.lifecycle_closed_for_test());
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn reliable_transport_accepted_ack_releases_the_buffered_envelope() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));

        let identity = transport.send_test(&route(30), frame(1)).expect_buffered();
        assert_eq!(transport.pending_len(), 1);

        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);

        // A second ack for the already-released identity is a no-op (duplicate arrival).
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Unknown
        );

        // A released frame is never retried, however far the clock advances.
        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn reliable_transport_duplicate_ack_releases_without_redelivery() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));

        let identity = transport.send_test(&route(30), frame(1)).expect_buffered();
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Duplicate),
            EnvelopeAckOutcome::ReleasedOnDuplicate
        );
        assert_eq!(transport.pending_len(), 0);

        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1, "a duplicate-acked frame is never re-sent");
    }

    #[test]
    fn reliable_transport_missing_ack_retries_up_to_the_bounded_count() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 100_000));

        transport.send_test(&route(30), frame(1));
        assert_eq!(sink.count(), 1);

        // A tick before the retry interval elapses re-sends nothing.
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);

        // First interval → retry #1 (2 sends total).
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);

        // Second interval → retry #2, reaching the bound of 3 total sends.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 3);

        // Third interval → attempt count exhausted; no more sends, still buffered
        // because the (much larger) deadline has not yet elapsed.
        clock.advance(Duration::from_millis(100));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.retried(), 0);
        assert!(tick.failed_open().is_empty());
        assert_eq!(sink.count(), 3, "retry count is strictly bounded");
        assert_eq!(transport.pending_len(), 1);
    }

    #[test]
    fn reliable_transport_exhausted_retries_still_release_on_a_late_ack() {
        // The composed M3 semantic in one sequence: exhausting the attempt count stops
        // retransmission but keeps the frame buffered until its (far-off) deadline, so
        // an ack arriving after exhaustion still releases cleanly.
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 2, 100_000));

        let identity = transport.send_test(&route(30), frame(1)).expect_buffered();
        assert_eq!(sink.count(), 1);

        // One interval → the single allowed retry, reaching the bound of 2 sends.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);

        // Attempt count now exhausted: further ticks are quiescent (no retransmit) yet
        // the entry stays buffered because the deadline is nowhere near.
        for _ in 0..3 {
            clock.advance(Duration::from_millis(100));
            let tick = transport.drive_retries(clock.now());
            assert_eq!(tick.retried(), 0);
            assert!(tick.failed_open().is_empty());
        }
        assert_eq!(sink.count(), 2, "no retransmit past the attempt bound");
        assert_eq!(transport.pending_len(), 1);

        // A late ack (well before the deadline) still releases the buffered frame.
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);

        // And nothing is re-sent afterwards.
        clock.advance(Duration::from_millis(100));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 2);
    }

    #[test]
    fn reliable_transport_out_of_order_and_duplicate_acks_are_handled() {
        let Harness {
            transport,
            sink: _sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));

        // Two envelopes to the SAME route get distinct sequences.
        let first = transport.send_test(&route(30), frame(1)).expect_buffered();
        let second = transport.send_test(&route(30), frame(2)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_ne!(
            first.as_delivery().unwrap().sequence(),
            second.as_delivery().unwrap().sequence()
        );

        // Ack the second delivery first (out of order): only its entry releases.
        assert_eq!(
            transport.on_ack(&second, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 1);

        // A duplicate ack for the already-released second delivery is a no-op.
        assert_eq!(
            transport.on_ack(&second, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Unknown
        );
        assert_eq!(transport.pending_len(), 1);

        // The still-pending first delivery releases independently.
        assert_eq!(
            transport.on_ack(&first, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);
    }

    #[test]
    fn reliable_transport_deadline_releases_and_fails_open_without_error() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 10, 250));

        let identity = transport.send_test(&route(30), frame(1)).expect_buffered();

        // A retry fires before the deadline.
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        assert_eq!(sink.count(), 2);
        assert_eq!(transport.pending_len(), 1);

        // Crossing the deadline releases the frame and reports it failed open — no
        // panic, no error surfaced to the query.
        clock.advance(Duration::from_millis(150));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(tick.retried(), 0);
        assert_eq!(tick.failed_open(), &[identity]);

        // Once failed open, further ticks neither re-send nor re-report.
        clock.advance(Duration::from_millis(1_000));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 2);
    }

    #[test]
    fn reliable_transport_rejected_ack_stops_retry_and_surfaces_the_rejection() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 5, 10_000));

        let identity = transport.send_test(&route(30), frame(1)).expect_buffered();
        assert_eq!(
            transport.on_ack(&identity, RuntimeFilterAcceptStatus::Rejected),
            EnvelopeAckOutcome::Rejected
        );
        assert_eq!(transport.pending_len(), 0);

        // A rejected frame is released, so retries stop for it.
        clock.advance(Duration::from_millis(500));
        assert!(transport.drive_retries(clock.now()).is_quiescent());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn reliable_transport_broadcast_fanout_retains_complete_envelopes_and_acks_independently() {
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy(100, 3, 10_000));

        let payload = frame(1);
        assert_eq!(Arc::strong_count(&payload), 1);

        // One serialized frame fans out to two routes.
        let route_a = transport
            .send_test(&route(30), Arc::clone(&payload))
            .expect_buffered();
        let route_b = transport
            .send_test(&route(31), Arc::clone(&payload))
            .expect_buffered();

        // Pending owns complete immutable envelopes, not a borrowed frame allocation.
        assert_eq!(Arc::strong_count(&payload), 1);
        assert_eq!(transport.pending_len(), 2);

        // Both routes received the identical bytes.
        let mut edges = sink.edges();
        edges.sort_unstable();
        assert_eq!(edges, vec![RouteEdgeId::new(30), RouteEdgeId::new(31)]);
        for (_, transmitted) in sink.frames() {
            assert_eq!(&transmitted, payload.as_ref());
        }

        // Acking one route releases only its complete envelope entry.
        assert_eq!(
            transport.on_ack(&route_a, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 1);
        assert_eq!(Arc::strong_count(&payload), 1);

        assert_eq!(
            transport.on_ack(&route_b, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released
        );
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(Arc::strong_count(&payload), 1);
    }

    // ==============================================================================
    // M3 Task 4: self-owned buffer ceilings -> explicit ResourceLimit rejection.
    // ==============================================================================

    #[test]
    fn transport_bounded_retry_queue_rejects_new_frame_at_entry_ceiling() {
        // Entry-count ceiling of 2: the buffer admits two in-flight frames, then a
        // third genuinely-new frame is refused with an explicit ResourceLimit — never
        // silently dropped, never buffered beyond the cap.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(100, 3, 10_000, 2, ROOMY_MAX_BYTES));

        let first = transport.send_test(&route(30), frame(1)).expect_buffered();
        let _second = transport.send_test(&route(31), frame(2)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(sink.count(), 2);

        // The third send is refused: not buffered, not put on the wire.
        assert_eq!(
            transport.send_test(&route(32), frame(3)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::PendingEntries),
        );
        assert_eq!(
            transport.pending_len(),
            2,
            "a refused frame is not buffered"
        );
        assert_eq!(sink.count(), 2, "a refused frame is not transmitted");

        // Releasing an in-flight frame frees a slot; the next send is admitted again.
        assert_eq!(
            transport.on_ack(&first, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_len(), 1);
        let _third = transport.send_test(&route(32), frame(3)).expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(sink.count(), 3);
    }

    #[test]
    fn transport_bounded_serialized_buffer_rejects_new_frame_at_byte_ceiling() {
        // Byte ceiling holds two inline envelopes plus ten retained payload bytes:
        // distinct frames are admitted until their retained charge
        // bytes would exceed the cap, then a new frame is refused with an explicit
        // ResourceLimit. The cap rejects only strictly-greater, so the exact-fit send
        // is still admitted.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(
            100,
            3,
            10_000,
            ROOMY_MAX_ENTRIES,
            retained_envelope_bytes(6) + retained_envelope_bytes(4),
        ));

        let _a = transport
            .send_test(&route(30), frame_sized(1, 6))
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), retained_envelope_bytes(6));

        // 6 + 6 = 12 > 10: refused on the byte ceiling.
        assert_eq!(
            transport.send_test(&route(31), frame_sized(2, 6)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::SerializedBytes),
        );
        assert_eq!(
            transport.pending_len(),
            1,
            "a byte-refused frame is not buffered"
        );
        assert_eq!(transport.pending_bytes(), retained_envelope_bytes(6));

        // 6 + 4 = 10 == cap: admitted.
        let _b = transport
            .send_test(&route(32), frame_sized(3, 4))
            .expect_buffered();
        assert_eq!(
            transport.pending_bytes(),
            retained_envelope_bytes(6) + retained_envelope_bytes(4)
        );
        assert_eq!(sink.count(), 2);

        // Any further distinct byte is refused.
        assert_eq!(
            transport.send_test(&route(33), frame_sized(4, 1)),
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::SerializedBytes),
        );
    }

    #[test]
    fn transport_bounded_complete_envelope_allocations_are_each_metered_once() {
        // Each route owns a distinct complete envelope allocation. Retry reuses those
        // same Arcs and must not increase accounting.
        let Harness {
            transport,
            sink,
            clock: _clock,
        } = harness(policy_bounded(
            100,
            3,
            10_000,
            ROOMY_MAX_ENTRIES,
            retained_envelope_bytes(6) * 2,
        ));
        let payload = frame_sized(9, 6);

        let route_a = transport
            .send_test(&route(30), Arc::clone(&payload))
            .expect_buffered();
        let route_b = transport
            .send_test(&route(31), Arc::clone(&payload))
            .expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(
            transport.pending_bytes(),
            retained_envelope_bytes(6) * 2,
            "each actually retained complete envelope is metered once"
        );
        assert_eq!(sink.count(), 2);

        transport.drive_retries(transport.clock.now() + Duration::from_millis(100));
        assert_eq!(
            transport.pending_bytes(),
            retained_envelope_bytes(6) * 2,
            "retry does not re-meter Arcs"
        );

        // Each ACK releases exactly the allocation addressed by its identity.
        assert_eq!(
            transport.on_ack(&route_a, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_bytes(), retained_envelope_bytes(6));
        assert_eq!(
            transport.on_ack(&route_b, RuntimeFilterAcceptStatus::Accepted),
            EnvelopeAckOutcome::Released,
        );
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn transport_bounded_release_returns_counts_to_zero() {
        // The self-owned counters live inside the query-scoped transport, so releasing
        // every in-flight frame (as query teardown does when the service is destroyed)
        // returns both the entry count and the buffered bytes to zero — the buffer
        // never grows without bound and never leaks.
        let Harness {
            transport,
            sink: _sink,
            clock,
        } = harness(policy_bounded(
            100,
            10,
            250,
            ROOMY_MAX_ENTRIES,
            ROOMY_MAX_BYTES,
        ));

        let a = transport
            .send_test(&route(30), frame_sized(1, 5))
            .expect_buffered();
        let b = transport
            .send_test(&route(31), frame_sized(2, 7))
            .expect_buffered();
        assert_eq!(transport.pending_len(), 2);
        assert_eq!(
            transport.pending_bytes(),
            retained_envelope_bytes(5) + retained_envelope_bytes(7)
        );

        // Ack-release frees both entries and reclaims their bytes.
        transport.on_ack(&a, RuntimeFilterAcceptStatus::Accepted);
        transport.on_ack(&b, RuntimeFilterAcceptStatus::Duplicate);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);

        // The deadline fail-open path also reclaims bytes, not just entries.
        let _c = transport
            .send_test(&route(32), frame_sized(3, 9))
            .expect_buffered();
        assert_eq!(transport.pending_bytes(), retained_envelope_bytes(9));
        clock.advance(Duration::from_millis(300));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.failed_open().len(), 1);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(
            transport.pending_bytes(),
            0,
            "deadline fail-open reclaims buffered bytes"
        );
    }

    // ==============================================================================
    // M3 Task 5: structured transport events flow through the RFD-3 lifecycle sink.
    // ==============================================================================

    /// A harness whose transport emits into a recording lifecycle sink, so a test can
    /// assert the structured `TransportEnvelope` stream (kind + byte size + accept status).
    struct EventsHarness {
        transport: ReliableEnvelopeTransport,
        events: Arc<RecordingEvents>,
        clock: Arc<ManualClock>,
    }

    fn events_harness(policy: ReliableTransportPolicy) -> EventsHarness {
        let sink = Arc::new(RecordingSink::default());
        let events = Arc::new(RecordingEvents::default());
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let transport =
            ReliableEnvelopeTransport::new(sink.clone(), clock.clone(), policy, events.clone());
        EventsHarness {
            transport,
            events,
            clock,
        }
    }

    fn blocking_events(
        blocked: BlockedTransportEvent,
    ) -> (
        Arc<BlockingTransportEvents>,
        mpsc::Receiver<()>,
        mpsc::SyncSender<()>,
    ) {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        (
            Arc::new(BlockingTransportEvents {
                blocked,
                entered: entered_tx,
                release: Mutex::new(release_rx),
                recorded: Mutex::new(Vec::new()),
            }),
            entered_rx,
            release_tx,
        )
    }

    fn install_before_emit_barrier(
        transport: &ReliableEnvelopeTransport,
        blocked: BlockedTransportEvent,
    ) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let armed = AtomicBool::new(true);
        let release_rx = Mutex::new(release_rx);
        transport.set_before_event_emit_for_test(Arc::new(move |kind| {
            let targeted = matches!(
                (blocked, kind),
                (BlockedTransportEvent::Acked, TransportEventKind::Acked(_))
                    | (
                        BlockedTransportEvent::FailedOpen,
                        TransportEventKind::FailedOpen(_)
                    )
            );
            if targeted && armed.swap(false, Ordering::AcqRel) {
                entered_tx.send(()).expect("before-emit barrier entered");
                release_rx
                    .lock()
                    .expect("before-emit release")
                    .recv_timeout(Duration::from_secs(1))
                    .expect("before-emit barrier released");
            }
        }));
        (entered_rx, release_tx)
    }

    #[test]
    fn transport_events_send_records_sent_through_the_lifecycle_sink_with_byte_size() {
        let EventsHarness {
            transport, events, ..
        } = events_harness(policy(100, 3, 10_000));
        let identity = event_identity(RouteEdgeId::new(30));

        transport
            .send(&route(30), frame_sized(1, 9), identity)
            .expect_buffered();

        // Exactly one Sent event, keyed by the route identity and carrying the serialized
        // frame byte size — flowing through the SAME lifecycle sink, not a second registry.
        assert_eq!(
            events.transport_events(),
            vec![(identity, TransportEventKind::Sent, 9)],
        );
    }

    #[test]
    fn transport_events_ack_records_acked_with_the_peer_accept_status() {
        let EventsHarness {
            transport, events, ..
        } = events_harness(policy(100, 3, 10_000));
        let accepted = event_identity(RouteEdgeId::new(30));
        let duplicate = event_identity(RouteEdgeId::new(31));
        let rejected = event_identity(RouteEdgeId::new(32));

        let a = transport
            .send(&route(30), frame_sized(1, 4), accepted)
            .expect_buffered();
        let b = transport
            .send(&route(31), frame_sized(2, 5), duplicate)
            .expect_buffered();
        let c = transport
            .send(&route(32), frame_sized(3, 6), rejected)
            .expect_buffered();

        transport.on_ack(&a, RuntimeFilterAcceptStatus::Accepted);
        transport.on_ack(&b, RuntimeFilterAcceptStatus::Duplicate);
        transport.on_ack(&c, RuntimeFilterAcceptStatus::Rejected);

        // Each ack emits an Acked event carrying the peer's accept status verbatim; every
        // one of Accepted / Duplicate / Rejected surfaces (Rejected is never swallowed).
        let acked: Vec<_> = events
            .transport_events()
            .into_iter()
            .filter(|(_, kind, _)| matches!(kind, TransportEventKind::Acked(_)))
            .collect();
        assert_eq!(
            acked,
            vec![
                (
                    accepted,
                    TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                    4,
                ),
                (
                    duplicate,
                    TransportEventKind::Acked(RuntimeFilterAcceptStatus::Duplicate),
                    5,
                ),
                (
                    rejected,
                    TransportEventKind::Acked(RuntimeFilterAcceptStatus::Rejected),
                    6,
                ),
            ],
        );

        // A duplicate / out-of-order ack for an already-released identity is a no-op and
        // emits nothing further.
        transport.on_ack(&a, RuntimeFilterAcceptStatus::Accepted);
        assert_eq!(
            events
                .transport_events()
                .into_iter()
                .filter(|(_, kind, _)| matches!(kind, TransportEventKind::Acked(_)))
                .count(),
            3,
        );
    }

    #[test]
    fn shutdown_linearizes_before_an_ack_callback_can_return_late() {
        let sink = Arc::new(RecordingSink::default());
        let (events, entered_rx, release_tx) = blocking_events(BlockedTransportEvent::Acked);
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            sink,
            Arc::new(ManualClock::new(Instant::now())),
            policy(100, 3, 10_000),
            events.clone(),
        ));
        let ack = transport
            .send(&route(140), frame(1), event_identity(RouteEdgeId::new(140)))
            .expect_buffered();

        let (ack_done_tx, ack_done_rx) = mpsc::sync_channel(1);
        let ack_transport = transport.clone();
        let ack_thread = std::thread::spawn(move || {
            let outcome = ack_transport.on_ack(&ack, RuntimeFilterAcceptStatus::Accepted);
            ack_done_tx.send(outcome).expect("ack complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ack callback entered");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_transport = transport.clone();
        let shutdown_events = events.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_transport.shutdown();
            shutdown_done_tx
                .send(shutdown_events.target_count())
                .expect("shutdown complete");
        });
        let early_count = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .ok();

        release_tx.send(()).expect("release ack callback");
        ack_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ack joined in time");
        let count_at_shutdown = early_count.unwrap_or_else(|| {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time")
        });
        ack_thread.join().expect("ack thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            early_count.is_none(),
            "shutdown returned while an admitted ACK callback was still running"
        );
        assert_eq!(events.target_count(), count_at_shutdown);
    }

    #[test]
    fn shutdown_linearizes_before_a_deadline_callback_can_return_late() {
        let sink = Arc::new(RecordingSink::default());
        let started = Instant::now();
        let (events, entered_rx, release_tx) = blocking_events(BlockedTransportEvent::FailedOpen);
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            sink,
            Arc::new(ManualClock::new(started)),
            policy(100, 3, 200),
            events.clone(),
        ));
        transport
            .send(&route(141), frame(2), event_identity(RouteEdgeId::new(141)))
            .expect_buffered();

        let (tick_done_tx, tick_done_rx) = mpsc::sync_channel(1);
        let tick_transport = transport.clone();
        let tick_thread = std::thread::spawn(move || {
            tick_transport.drive_retries(started + Duration::from_millis(200));
            tick_done_tx.send(()).expect("deadline tick complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline callback entered");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_transport = transport.clone();
        let shutdown_events = events.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_transport.shutdown();
            shutdown_done_tx
                .send(shutdown_events.target_count())
                .expect("shutdown complete");
        });
        let early_count = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .ok();

        release_tx.send(()).expect("release deadline callback");
        tick_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline tick joined in time");
        let count_at_shutdown = early_count.unwrap_or_else(|| {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time")
        });
        tick_thread.join().expect("deadline tick thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            early_count.is_none(),
            "shutdown returned while an admitted deadline callback was still running"
        );
        assert_eq!(events.target_count(), count_at_shutdown);
    }

    #[test]
    fn ack_removed_before_shutdown_does_not_emit_after_terminal() {
        let events = Arc::new(RecordingEvents::default());
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            Arc::new(RecordingSink::default()),
            Arc::new(ManualClock::new(Instant::now())),
            policy(100, 3, 10_000),
            events.clone(),
        ));
        let ack = transport
            .send(&route(142), frame(3), event_identity(RouteEdgeId::new(142)))
            .expect_buffered();
        let (entered_rx, release_tx) =
            install_before_emit_barrier(&transport, BlockedTransportEvent::Acked);

        let (ack_done_tx, ack_done_rx) = mpsc::sync_channel(1);
        let ack_transport = transport.clone();
        let ack_thread = std::thread::spawn(move || {
            ack_transport.on_ack(&ack, RuntimeFilterAcceptStatus::Accepted);
            ack_done_tx.send(()).expect("ack complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ACK removed before emit");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_transport = transport.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_transport.shutdown();
            shutdown_done_tx.send(()).expect("shutdown complete");
        });
        let shutdown_completed_before_release = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        release_tx.send(()).expect("release ACK emit gap");
        ack_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ack joined in time");
        if !shutdown_completed_before_release {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time");
        }
        ack_thread.join().expect("ack thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            shutdown_completed_before_release,
            "shutdown must close while an unadmitted ACK callback is paused"
        );
        assert!(
            events
                .transport_events()
                .iter()
                .all(|(_, kind, _)| !matches!(kind, TransportEventKind::Acked(_)))
        );
    }

    #[test]
    fn deadline_removed_before_shutdown_does_not_emit_after_terminal() {
        let events = Arc::new(RecordingEvents::default());
        let started = Instant::now();
        let transport = Arc::new(ReliableEnvelopeTransport::new(
            Arc::new(RecordingSink::default()),
            Arc::new(ManualClock::new(started)),
            policy(100, 3, 200),
            events.clone(),
        ));
        transport
            .send(&route(143), frame(4), event_identity(RouteEdgeId::new(143)))
            .expect_buffered();
        let (entered_rx, release_tx) =
            install_before_emit_barrier(&transport, BlockedTransportEvent::FailedOpen);

        let (tick_done_tx, tick_done_rx) = mpsc::sync_channel(1);
        let tick_transport = transport.clone();
        let tick_thread = std::thread::spawn(move || {
            tick_transport.drive_retries(started + Duration::from_millis(200));
            tick_done_tx.send(()).expect("deadline tick complete");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline removed before emit");

        let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
        let shutdown_transport = transport.clone();
        let shutdown_thread = std::thread::spawn(move || {
            shutdown_transport.shutdown();
            shutdown_done_tx.send(()).expect("shutdown complete");
        });
        let shutdown_completed_before_release = shutdown_done_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        release_tx.send(()).expect("release deadline emit gap");
        tick_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("deadline tick joined in time");
        if !shutdown_completed_before_release {
            shutdown_done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown joined in time");
        }
        tick_thread.join().expect("deadline tick thread");
        shutdown_thread.join().expect("shutdown thread");

        assert!(
            shutdown_completed_before_release,
            "shutdown must close while an unadmitted deadline callback is paused"
        );
        assert!(
            events
                .transport_events()
                .iter()
                .all(|(_, kind, _)| !matches!(kind, TransportEventKind::FailedOpen(_)))
        );
    }

    #[test]
    fn transport_events_retry_and_deadline_record_retried_then_failed_open() {
        // retry 100ms, attempt bound 3, deadline 250ms.
        let EventsHarness {
            transport,
            events,
            clock,
        } = events_harness(policy(100, 3, 250));
        let identity = event_identity(RouteEdgeId::new(30));

        transport
            .send(&route(30), frame_sized(1, 7), identity)
            .expect_buffered();

        // Two retry intervals → two Retried events (bounded by the attempt count of 3).
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);
        clock.advance(Duration::from_millis(100));
        assert_eq!(transport.drive_retries(clock.now()).retried(), 1);

        // Past the deadline → one FailedOpen(Deadline); the route degrades (no error).
        clock.advance(Duration::from_millis(100));
        let tick = transport.drive_retries(clock.now());
        assert_eq!(tick.failed_open().len(), 1);

        // The full ordered stream: Sent, Retried x2, FailedOpen(Deadline) — every event
        // keyed by the same route identity and carrying the frame byte size.
        let kinds: Vec<_> = events
            .transport_events()
            .into_iter()
            .map(|(recorded, kind, bytes)| {
                assert_eq!(recorded, identity);
                assert_eq!(bytes, 7);
                kind
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                TransportEventKind::Sent,
                TransportEventKind::Retried,
                TransportEventKind::Retried,
                TransportEventKind::FailedOpen(TransportFailOpenReason::Deadline),
            ],
        );
    }

    #[test]
    fn transport_events_resource_limit_send_emits_nothing_from_the_transport() {
        // A resource-refused send never entered the buffer, so the transport emits NO
        // event for it — the Service's delivery bridge owns the resource-limit fail-open
        // event at the `send` call site. Only the first (buffered) send is observed here.
        let EventsHarness {
            transport, events, ..
        } = events_harness(policy_bounded(100, 3, 10_000, 1, ROOMY_MAX_BYTES));

        transport
            .send(
                &route(30),
                frame_sized(1, 4),
                event_identity(RouteEdgeId::new(30)),
            )
            .expect_buffered();
        let refused = transport.send(
            &route(31),
            frame_sized(2, 5),
            event_identity(RouteEdgeId::new(31)),
        );
        assert_eq!(
            refused,
            ReliableSendOutcome::ResourceLimit(TransportResourceLimit::PendingEntries),
        );

        assert_eq!(
            events.transport_events(),
            vec![(
                event_identity(RouteEdgeId::new(30)),
                TransportEventKind::Sent,
                4,
            )],
        );
    }

    #[test]
    fn accepted_and_duplicate_ack_release_pending() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));
        let accepted = transport
            .send_test(&route(71), frame_sized(1, 5))
            .expect_buffered();
        let duplicate = transport
            .send_test(&route(72), frame_sized(2, 7))
            .expect_buffered();

        sink.complete(SinkCompletion::Ack(
            accepted,
            RuntimeFilterAcceptStatus::Accepted,
        ));
        sink.complete(SinkCompletion::Ack(
            duplicate,
            RuntimeFilterAcceptStatus::Duplicate,
        ));
        let tick = transport.drain_completions_and_drive(clock.now());

        assert!(tick.failed_open().is_empty());
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn ack_identity_mismatch_is_contract_rejection() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));
        let requested = transport
            .send_test(&route(73), frame_sized(3, 9))
            .expect_buffered();

        sink.complete(SinkCompletion::TransportFailure(
            requested.clone(),
            SinkTransportError::contract("runtime filter ACK identity mismatch"),
        ));
        let tick = transport.drain_completions_and_drive(clock.now());

        assert_eq!(tick.failed_open(), &[requested]);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn malformed_ack_contract_failure_releases_without_retry() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));
        let requested = transport
            .send_test(&route(78), frame_sized(8, 23))
            .expect_buffered();

        sink.complete(SinkCompletion::TransportFailure(
            requested.clone(),
            SinkTransportError::contract("runtime filter ACK accept status must be specified"),
        ));
        let tick = transport.drain_completions_and_drive(clock.now());

        assert_eq!(tick.failed_open(), &[requested]);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
        clock.advance(Duration::from_millis(100));
        assert!(
            transport
                .drain_completions_and_drive(clock.now())
                .is_quiescent()
        );
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn network_failure_remains_pending_until_retry() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(100, 3, 10_000));
        let requested = transport
            .send_test(&route(74), frame_sized(4, 11))
            .expect_buffered();
        sink.complete(SinkCompletion::TransportFailure(
            requested,
            SinkTransportError::network("temporary peer outage"),
        ));

        assert!(
            transport
                .drain_completions_and_drive(clock.now())
                .is_quiescent()
        );
        assert_eq!(transport.pending_len(), 1);
        clock.advance(Duration::from_millis(100));
        assert_eq!(
            transport.drain_completions_and_drive(clock.now()).retried(),
            1
        );
        assert_eq!(sink.count(), 2);
        assert_eq!(transport.pending_len(), 1);
    }

    #[test]
    fn deadline_failure_opens_route_without_failing_query() {
        let Harness {
            transport,
            sink,
            clock,
        } = harness(policy(50, 2, 150));
        let requested = transport
            .send_test(&route(75), frame_sized(5, 13))
            .expect_buffered();
        sink.complete(SinkCompletion::TransportFailure(
            requested.clone(),
            SinkTransportError::network("peer unavailable"),
        ));
        clock.advance(Duration::from_millis(200));

        let tick = transport.drain_completions_and_drive(clock.now());
        assert_eq!(tick.failed_open(), &[requested]);
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
    }

    #[test]
    fn shutdown_releases_pending_and_rejects_new_send() {
        let Harness {
            transport,
            sink: _,
            clock: _,
        } = harness(policy(100, 3, 10_000));
        transport
            .send_test(&route(76), frame_sized(6, 17))
            .expect_buffered();
        assert_eq!(transport.pending_len(), 1);

        transport.shutdown();
        assert_eq!(transport.pending_len(), 0);
        assert_eq!(transport.pending_bytes(), 0);
        assert_eq!(
            transport.send_test(&route(77), frame_sized(7, 19)),
            ReliableSendOutcome::Shutdown
        );
    }
}
