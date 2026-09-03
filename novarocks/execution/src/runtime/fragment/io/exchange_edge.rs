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

//! Send permission of a push exchange producer's outbound edges.
//!
//! Every edge frozen by a task descriptor starts closed, and the producer's
//! sink asks this module on every enqueue whether it may send. The opener runs
//! on another thread, so each edge's live answer is one atomic word rather
//! than a lock the send path has to take.
//!
//! Whether an open request is *legal* is not decided here:
//! [`ExchangeEdgeDomain`] already owns that classification, and this module
//! holds one instance of it and publishes what it accepted. Nothing here can
//! add a destination, reopen an edge, or reconfigure one.
//!
//! One runtime fact has no descriptor equivalent: a destination that withdrew
//! its ingress capability because it no longer needs this output. That is the
//! destination's normal departure, not this producer's failure, so it closes
//! exactly one edge and is recorded for a status producer to report.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::exec::pipeline::schedule::observer::Observable;
use novarocks_types::UniqueId;

use crate::task_execution::descriptor::ExchangeEdge;
use crate::task_execution::domain::{
    DomainConflict, DomainProgression, EdgeOpenVersion, EdgeSendPermission, ExchangeEdgeDomain,
    ExchangeEdgeId,
};

/// What a producer may do on one outbound edge right now.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EdgeSendState {
    /// Send permission has not been granted yet. The sink sends nothing and
    /// holds what it has under the send queue's existing bounded
    /// backpressure.
    AwaitingPermission,
    /// Every frozen destination of this edge acknowledged its creation, so
    /// the producer may send.
    Open,
    /// A destination withdrew its ingress capability because it no longer
    /// needs this output. Nothing further is sent on this edge and its
    /// pending frames are discarded.
    NormallyCanceled,
}

impl EdgeSendState {
    pub const fn may_send(self) -> bool {
        matches!(self, Self::Open)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingPermission => "AWAITING_PERMISSION",
            Self::Open => "OPEN",
            Self::NormallyCanceled => "NORMALLY_CANCELED",
        }
    }
}

impl fmt::Display for EdgeSendState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

const STATE_AWAITING_PERMISSION: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_NORMALLY_CANCELED: u8 = 2;

/// The live send permission of one outbound exchange edge.
///
/// The sink reads it on every enqueue while the opener and the send workers
/// write it from other threads, which is why the whole state is a single
/// atomic word shared by `Arc` rather than a copied snapshot.
#[derive(Debug)]
pub struct EdgeSendGate {
    edge_id: ExchangeEdgeId,
    state: AtomicU8,
}

impl EdgeSendGate {
    fn closed(edge_id: ExchangeEdgeId) -> Self {
        Self {
            edge_id,
            state: AtomicU8::new(STATE_AWAITING_PERMISSION),
        }
    }

    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub fn state(&self) -> EdgeSendState {
        match self.state.load(Ordering::Acquire) {
            STATE_OPEN => EdgeSendState::Open,
            STATE_NORMALLY_CANCELED => EdgeSendState::NormallyCanceled,
            _ => EdgeSendState::AwaitingPermission,
        }
    }

    pub fn may_send(&self) -> bool {
        self.state().may_send()
    }

    pub fn is_normally_canceled(&self) -> bool {
        self.state() == EdgeSendState::NormallyCanceled
    }

    /// Publishes an open that [`ExchangeEdgeGates::open`] already accepted.
    ///
    /// Only `AwaitingPermission -> Open` moves. A grant that was already in
    /// flight when the destination departed must not resurrect the edge, so a
    /// withdrawn edge stays withdrawn.
    fn grant_send_permission(&self) {
        let _ = self.state.compare_exchange(
            STATE_AWAITING_PERMISSION,
            STATE_OPEN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Records that a destination of this edge withdrew its ingress
    /// capability for the one normal reason
    /// (`CancelReason::UpstreamNoLongerNeeded`).
    ///
    /// Returns whether this call is the one that closed the edge, so its
    /// abandoned frames are discarded exactly once. It is a latch: a repeat,
    /// or a second destination of the same edge reporting the same thing,
    /// converges on the same state whatever the order.
    pub(crate) fn close_for_normal_cancellation(&self) -> bool {
        self.state.swap(STATE_NORMALLY_CANCELED, Ordering::AcqRel) != STATE_NORMALLY_CANCELED
    }
}

/// The address that attributes one frozen destination to exactly one edge.
///
/// It is the pair an outbound frame already carries: the destination's
/// execution kernel key and the inbound node it feeds. Resolving an edge
/// through it is what keeps a decision about one edge from reaching another
/// edge's destination.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ExchangeDestinationKey {
    fragment_instance_id: UniqueId,
    destination_node_id: i32,
}

impl ExchangeDestinationKey {
    pub const fn new(fragment_instance_id: UniqueId, destination_node_id: i32) -> Self {
        Self {
            fragment_instance_id,
            destination_node_id,
        }
    }

    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn destination_node_id(self) -> i32 {
        self.destination_node_id
    }
}

impl fmt::Display for ExchangeDestinationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} node {}",
            self.fragment_instance_id, self.destination_node_id
        )
    }
}

/// Why an edge gate set is not a legal value.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExchangeEdgeGateError {
    /// The frozen edge set repeats an edge id.
    DuplicateEdge(ExchangeEdgeId),
    /// An edge froze no destination, so nothing could ever be attributed to
    /// it.
    EdgeWithoutDestinations(ExchangeEdgeId),
    /// Two edges, or one edge twice, froze the same destination. One edge's
    /// decision would then silently reach another's destination.
    DestinationClaimedTwice(ExchangeDestinationKey),
}

impl fmt::Display for ExchangeEdgeGateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateEdge(edge) => {
                write!(formatter, "outbound edge {edge} is frozen twice")
            }
            Self::EdgeWithoutDestinations(edge) => {
                write!(formatter, "outbound edge {edge} has no destination")
            }
            Self::DestinationClaimedTwice(key) => write!(
                formatter,
                "exchange destination {key} is claimed by more than one outbound edge"
            ),
        }
    }
}

impl std::error::Error for ExchangeEdgeGateError {}

/// How an accepted open request was applied.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EdgeOpenAccepted {
    /// The named edges moved from closed to open.
    Applied,
    /// This exact version already opened this exact edge set.
    Idempotent,
}

/// Every outbound edge of one producer task, each with its own gate.
///
/// The set is frozen at construction from the descriptor's outbound edges and
/// never grows. Opening is the only mutation the frontend can drive, and it is
/// monotonic: this release has no close, reopen, or reconfigure operation.
#[derive(Debug)]
pub struct ExchangeEdgeGates {
    gates: BTreeMap<ExchangeEdgeId, Arc<EdgeSendGate>>,
    destinations: HashMap<ExchangeDestinationKey, Arc<EdgeSendGate>>,
    granted: Mutex<ExchangeEdgeDomain>,
    /// Every producer driver that must be woken when an edge opens.
    ///
    /// A driver that parked because its edge was closed is parked on a
    /// notification, not on a poll: the executor hands an output-blocked
    /// driver to its event scheduler, which re-checks readiness only when an
    /// observable that driver registered fires. So the open has to fire one,
    /// or the driver waits for a wake-up that never comes. Measured on a real
    /// 1FE+3BE cluster: the one producer of a distributed `SELECT` that had
    /// rows parked its payload behind the closed edge and was never
    /// rescheduled after the edge opened, so it never sent end of stream and
    /// the query hung until its statement deadline.
    ///
    /// Weak, exactly as the send queue's own observer list is: a driver that
    /// has gone away must not be kept alive by a gate set that outlives it.
    open_waiters: Mutex<Vec<Weak<Observable>>>,
}

impl ExchangeEdgeGates {
    /// Freezes one gate per edge and the destination-to-edge attribution.
    pub fn try_new(
        edges: impl IntoIterator<Item = (ExchangeEdgeId, Vec<ExchangeDestinationKey>)>,
    ) -> Result<Arc<Self>, ExchangeEdgeGateError> {
        let mut gates: BTreeMap<ExchangeEdgeId, Arc<EdgeSendGate>> = BTreeMap::new();
        let mut destinations: HashMap<ExchangeDestinationKey, Arc<EdgeSendGate>> = HashMap::new();
        for (edge_id, keys) in edges {
            if keys.is_empty() {
                return Err(ExchangeEdgeGateError::EdgeWithoutDestinations(edge_id));
            }
            let gate = Arc::new(EdgeSendGate::closed(edge_id));
            if gates.insert(edge_id, Arc::clone(&gate)).is_some() {
                return Err(ExchangeEdgeGateError::DuplicateEdge(edge_id));
            }
            for key in keys {
                if destinations.insert(key, Arc::clone(&gate)).is_some() {
                    return Err(ExchangeEdgeGateError::DestinationClaimedTwice(key));
                }
            }
        }
        let granted = ExchangeEdgeDomain::from_frozen_edges(gates.keys().copied());
        Ok(Arc::new(Self {
            gates,
            destinations,
            granted: Mutex::new(granted),
            open_waiters: Mutex::new(Vec::new()),
        }))
    }

    /// Freezes the gates from a descriptor's outbound edges, which are the
    /// only authority over edge and destination membership.
    pub fn from_frozen_edges(edges: &[ExchangeEdge]) -> Result<Arc<Self>, ExchangeEdgeGateError> {
        Self::try_new(edges.iter().map(|edge| {
            let keys = edge
                .destinations()
                .iter()
                .map(|destination| {
                    ExchangeDestinationKey::new(
                        destination.fragment_instance_id(),
                        destination.destination_node_id().get(),
                    )
                })
                .collect();
            (edge.edge_id(), keys)
        }))
    }

    /// Registers one producer driver's observable to be woken when an edge of
    /// this set opens.
    ///
    /// Every sink bound to this gate set registers, because the open is one
    /// decision for the whole set and any of them may be parked behind it.
    pub fn register_open_waiter(&self, waiter: &Arc<Observable>) {
        self.open_waiters
            .lock()
            .expect("exchange edge open waiter lock")
            .push(Arc::downgrade(waiter));
    }

    /// Wakes every registered producer driver, dropping the ones that are gone.
    fn notify_open_waiters(&self) {
        let waiters = {
            let mut guard = self
                .open_waiters
                .lock()
                .expect("exchange edge open waiter lock");
            let mut alive = Vec::new();
            guard.retain(|weak| match weak.upgrade() {
                Some(waiter) => {
                    alive.push(waiter);
                    true
                }
                None => false,
            });
            alive
        };
        for waiter in waiters {
            let notify = waiter.defer_notify();
            notify.arm();
        }
    }

    pub fn gate(&self, edge: ExchangeEdgeId) -> Option<&Arc<EdgeSendGate>> {
        self.gates.get(&edge)
    }

    /// The gate of the edge that froze `key`, or `None` for a destination
    /// this producer does not have. An absent attribution is never an
    /// implicit permission: the caller must fail closed.
    pub fn gate_for_destination(&self, key: ExchangeDestinationKey) -> Option<&Arc<EdgeSendGate>> {
        self.destinations.get(&key)
    }

    pub fn edges(&self) -> impl Iterator<Item = (ExchangeEdgeId, EdgeSendState)> + '_ {
        self.gates.iter().map(|(edge, gate)| (*edge, gate.state()))
    }

    /// The permission the frontend granted, which is monotonic and is never
    /// taken back. A destination's later departure changes what the producer
    /// does, not what the frontend decided, so the two answers are separate.
    pub fn granted_permission(&self, edge: ExchangeEdgeId) -> Option<EdgeSendPermission> {
        self.granted
            .lock()
            .expect("exchange edge grant lock")
            .permission(edge)
    }

    /// Grants send permission to a complete edge set.
    ///
    /// The classification belongs to [`ExchangeEdgeDomain`], not to this
    /// module: an unknown edge, a repeat naming a different set, and any
    /// version other than the one that opened an edge are conflicts. A
    /// conflict grants nothing, so the edges stay closed.
    pub fn open(
        &self,
        version: EdgeOpenVersion,
        edges: &[ExchangeEdgeId],
    ) -> Result<EdgeOpenAccepted, DomainConflict> {
        let mut granted = self.granted.lock().expect("exchange edge grant lock");
        match granted.classify_open(version, edges) {
            DomainProgression::Apply => {
                granted.apply_open(version, edges);
                for edge in edges {
                    if let Some(gate) = self.gates.get(edge) {
                        gate.grant_send_permission();
                    }
                }
                // Released before the wake, so a woken driver reads the open
                // it was woken for rather than the state that parked it.
                drop(granted);
                self.notify_open_waiters();
                Ok(EdgeOpenAccepted::Applied)
            }
            DomainProgression::Idempotent => Ok(EdgeOpenAccepted::Idempotent),
            DomainProgression::Conflict(conflict) => Err(conflict),
            // An edge-open request carries no ordering it could fall behind,
            // so this arm is unreachable; it is rejected rather than assumed
            // away.
            DomainProgression::Older => Err(DomainConflict::NotMonotonic),
        }
    }

    /// Every edge a destination's normal cancellation closed, lowest id
    /// first. This is the fact a status producer needs to report normal
    /// downstream cancellation instead of a failure.
    pub fn normally_canceled_edges(&self) -> Vec<ExchangeEdgeId> {
        self.gates
            .iter()
            .filter(|(_, gate)| gate.is_normally_canceled())
            .map(|(edge, _)| *edge)
            .collect()
    }

    /// How many outbound edges a normal downstream cancellation closed. The
    /// latch is set once per edge, so this counts edges and cancellation
    /// reports alike.
    pub fn normally_canceled_edge_count(&self) -> usize {
        self.gates
            .values()
            .filter(|gate| gate.is_normally_canceled())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EdgeOpenAccepted, EdgeSendState, ExchangeDestinationKey, ExchangeEdgeGateError,
        ExchangeEdgeGates,
    };
    use crate::exec::fragment::program::FragmentNodeId;
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::task_execution::descriptor::{ExchangeDestination, ExchangeEdge};
    use crate::task_execution::domain::{
        DomainConflict, EdgeOpenVersion, EdgeSendPermission, ExchangeEdgeId,
    };
    use crate::task_execution::identity::TaskIdentity;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const NODE: i32 = 10;

    fn edge(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("nonzero edge")
    }

    fn key(value: i64) -> ExchangeDestinationKey {
        ExchangeDestinationKey::new(UniqueId::new(1, value), NODE)
    }

    fn version(value: u32) -> EdgeOpenVersion {
        EdgeOpenVersion::new(value).expect("nonzero version")
    }

    fn two_edges() -> Arc<ExchangeEdgeGates> {
        ExchangeEdgeGates::try_new([(edge(1), vec![key(1), key(2)]), (edge(2), vec![key(3)])])
            .expect("legal gate set")
    }

    /// The defect this catches: opening an edge granted send permission and
    /// notified nobody. A producer driver that parked because its edge was
    /// closed is parked on a notification -- the executor hands an
    /// output-blocked driver to its event scheduler, which re-checks readiness
    /// only when an observable that driver registered fires -- so the open
    /// left it parked for the rest of the query.
    ///
    /// The consequence, measured on a real 1FE+3BE cluster: of three producers
    /// of one distributed `SELECT`, the two with no rows to send never parked
    /// and sealed their streams, while the one holding the two matching rows
    /// parked with the payload in hand, never sent end of stream, and the
    /// query timed out after 120 s with no failure reported anywhere.
    #[test]
    fn opening_an_edge_wakes_every_producer_parked_behind_it() {
        let gates = two_edges();
        let waiter = Arc::new(Observable::new());
        let woken = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&woken);
        waiter.add_observer(Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
        gates.register_open_waiter(&waiter);

        assert_eq!(woken.load(Ordering::SeqCst), 0);
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1)]),
            Ok(EdgeOpenAccepted::Applied)
        );
        assert_eq!(
            woken.load(Ordering::SeqCst),
            1,
            "a granted edge has to wake the producers parked behind it"
        );

        // A replay grants nothing, so it wakes nobody: a wake per replay would
        // spin every parked driver of the set.
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1)]),
            Ok(EdgeOpenAccepted::Idempotent)
        );
        assert_eq!(woken.load(Ordering::SeqCst), 1);

        // A waiter whose driver is gone is dropped rather than kept alive.
        drop(waiter);
        assert_eq!(
            gates.open(version(2), &[edge(2)]),
            Ok(EdgeOpenAccepted::Applied)
        );
        assert_eq!(woken.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn every_frozen_edge_starts_closed_and_opens_exactly_once() {
        let gates = two_edges();
        assert_eq!(
            gates.gate(edge(1)).expect("edge one").state(),
            EdgeSendState::AwaitingPermission
        );
        assert_eq!(
            gates.granted_permission(edge(1)),
            Some(EdgeSendPermission::Closed)
        );
        assert!(gates.gate(edge(3)).is_none());

        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1)]),
            Ok(EdgeOpenAccepted::Applied)
        );
        assert!(gates.gate(edge(1)).expect("edge one").may_send());
        assert_eq!(
            gates.granted_permission(edge(1)),
            Some(EdgeSendPermission::Open)
        );
        assert_eq!(
            gates.gate(edge(2)).expect("edge two").state(),
            EdgeSendState::AwaitingPermission,
            "opening one edge must not grant permission on another"
        );

        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1)]),
            Ok(EdgeOpenAccepted::Idempotent),
            "the same version and the same edge set is a replay"
        );
    }

    #[test]
    fn a_conflicting_open_grants_nothing_and_leaves_the_edge_closed() {
        let gates = two_edges();
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(3)]),
            Err(DomainConflict::UnknownMember),
            "an edge this producer does not have can never be opened"
        );
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[]),
            Err(DomainConflict::UnknownMember)
        );
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1), edge(1)]),
            Err(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(
            gates.gate(edge(1)).expect("edge one").state(),
            EdgeSendState::AwaitingPermission,
            "a rejected open must leave the edge closed"
        );

        gates
            .open(EdgeOpenVersion::FIRST, &[edge(1)])
            .expect("first open");
        assert_eq!(
            gates.open(version(2), &[edge(1)]),
            Err(DomainConflict::SameTokenDifferentContent),
            "this release has no reconfigure, so another version is a conflict"
        );
        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]),
            Err(DomainConflict::SameTokenDifferentContent),
            "one version may not mean two different edge sets"
        );
        assert_eq!(
            gates.gate(edge(2)).expect("edge two").state(),
            EdgeSendState::AwaitingPermission,
            "the rejected set's unopened edge stays closed"
        );
    }

    #[test]
    fn a_destination_is_attributed_to_exactly_one_edge() {
        let gates = two_edges();
        assert_eq!(
            gates
                .gate_for_destination(key(2))
                .expect("destination two")
                .edge_id(),
            edge(1)
        );
        assert_eq!(
            gates
                .gate_for_destination(key(3))
                .expect("destination three")
                .edge_id(),
            edge(2)
        );
        assert!(
            gates.gate_for_destination(key(9)).is_none(),
            "an unattributed destination must not resolve to some edge"
        );
        assert!(
            gates
                .gate_for_destination(ExchangeDestinationKey::new(UniqueId::new(1, 1), NODE + 1))
                .is_none(),
            "the inbound node is part of the address"
        );

        let rejected = |edges: Vec<(ExchangeEdgeId, Vec<ExchangeDestinationKey>)>| {
            ExchangeEdgeGates::try_new(edges).expect_err("rejected")
        };
        assert_eq!(
            rejected(vec![(edge(1), vec![key(1)]), (edge(2), vec![key(1)])]),
            ExchangeEdgeGateError::DestinationClaimedTwice(key(1))
        );
        assert_eq!(
            rejected(vec![(edge(1), vec![key(1), key(1)])]),
            ExchangeEdgeGateError::DestinationClaimedTwice(key(1))
        );
        assert_eq!(
            rejected(vec![(edge(1), vec![key(1)]), (edge(1), vec![key(2)])]),
            ExchangeEdgeGateError::DuplicateEdge(edge(1))
        );
        assert_eq!(
            rejected(vec![(edge(1), Vec::new())]),
            ExchangeEdgeGateError::EdgeWithoutDestinations(edge(1))
        );
    }

    #[test]
    fn a_normal_cancellation_closes_only_its_own_edge_and_never_reopens_it() {
        let gates = two_edges();
        gates
            .open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)])
            .expect("open both");

        let first = gates.gate(edge(1)).expect("edge one");
        assert!(first.close_for_normal_cancellation());
        assert_eq!(first.state(), EdgeSendState::NormallyCanceled);
        assert!(!first.may_send());
        assert!(
            !first.close_for_normal_cancellation(),
            "only the first report closes the edge, so frames are discarded once"
        );

        let second = gates.gate(edge(2)).expect("edge two");
        assert!(second.may_send(), "the other edge keeps sending");

        assert_eq!(gates.normally_canceled_edges(), vec![edge(1)]);
        assert_eq!(gates.normally_canceled_edge_count(), 1);
        assert_eq!(
            gates.granted_permission(edge(1)),
            Some(EdgeSendPermission::Open),
            "the frontend's grant is monotonic; the departure is a separate fact"
        );

        assert_eq!(
            gates.open(EdgeOpenVersion::FIRST, &[edge(1), edge(2)]),
            Ok(EdgeOpenAccepted::Idempotent),
            "a replay of the exact set this version opened is still idempotent"
        );
        assert_eq!(
            first.state(),
            EdgeSendState::NormallyCanceled,
            "a grant may never resurrect a destination that already left"
        );
    }

    #[test]
    fn out_of_order_cancellations_across_edges_converge() {
        let gates = ExchangeEdgeGates::try_new([
            (edge(1), vec![key(1), key(2)]),
            (edge(2), vec![key(3)]),
            (edge(3), vec![key(4)]),
        ])
        .expect("legal gate set");
        gates
            .open(EdgeOpenVersion::FIRST, &[edge(3), edge(1)])
            .expect("open two of three");

        // Two destinations of edge one, an unopened edge, and a repeat, all
        // reported in an order no producer controls.
        let third = gates.gate(edge(3)).expect("edge three");
        assert!(third.close_for_normal_cancellation());
        let first = gates.gate(edge(1)).expect("edge one");
        assert!(first.close_for_normal_cancellation());
        assert!(!first.close_for_normal_cancellation());
        assert!(!third.close_for_normal_cancellation());

        assert_eq!(gates.normally_canceled_edges(), vec![edge(1), edge(3)]);
        assert_eq!(gates.normally_canceled_edge_count(), 2);
        assert_eq!(
            gates.gate(edge(2)).expect("edge two").state(),
            EdgeSendState::AwaitingPermission,
            "an edge nobody cancelled keeps its own state"
        );
        assert_eq!(
            gates
                .edges()
                .filter(|(_, state)| *state == EdgeSendState::NormallyCanceled)
                .count(),
            2
        );
    }

    #[test]
    fn gates_are_frozen_from_the_descriptors_outbound_edges() {
        let backend = BackendProcessId::new_v7();
        let identity = |task: u32| {
            TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).expect("nonzero"))
                    .expect("nonzero query"),
                StageId::new(2).expect("nonzero stage"),
                TaskId::new(task).expect("nonzero task"),
                backend,
            )
        };
        let destination = |task: u32, ordinal: u32| {
            ExchangeDestination::try_new(
                identity(task),
                UniqueId::new(1, i64::from(task)),
                RuntimeEndpoint::new("127.0.0.1", 9060).expect("endpoint"),
                FragmentNodeId::new(NODE),
                ordinal,
                NonZeroU32::new(2).expect("nonzero"),
            )
            .expect("legal destination")
        };
        let outbound = vec![
            ExchangeEdge::try_new(
                edge(1),
                FragmentNodeId::new(NODE),
                DataStreamPartitionType::HashPartitioned,
                vec![destination(1, 0), destination(2, 1)],
            )
            .expect("legal edge"),
        ];

        let gates = ExchangeEdgeGates::from_frozen_edges(&outbound).expect("legal gate set");
        assert_eq!(gates.edges().count(), 1);
        assert_eq!(
            gates
                .gate_for_destination(key(2))
                .expect("second destination")
                .edge_id(),
            edge(1)
        );
        assert_eq!(
            gates.gate(edge(1)).expect("edge one").state(),
            EdgeSendState::AwaitingPermission,
            "a frozen descriptor grants no send permission by itself"
        );
    }
}
