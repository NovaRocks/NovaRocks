// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native connection admission held outside the HTTP/2 stack.
//!
//! Every Native connection, accepted or dialed, first takes one physical
//! position of its transport class and one handshake position. Both are
//! counts only: the bytes Tokio, Hyper, H2 and Tonic allocate for that
//! connection stay inside those libraries, bounded by their public
//! configuration and verified by the transport measurement gate (spec v6
//! §5.9). Positions are returned at public exit events: the handshake
//! position when bootstrap completes or the connection is dropped, and the
//! physical position when the connection's IO wrapper is dropped.
//!
//! Peer/lane quotas for dialed connections and the incoming key a connection
//! is sealed to are tracked here too, with the same exit points.
//!
//! Design: ADR-0170 (docs/adr/ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md)

use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_proto_codec::native_rpc::{FrontendNativeLane, NativeRpcMethod};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::native_channel_identity::InlineNativeChannelIdentity;
use crate::native_connection_key_capacity::{
    NativeConnectionKeyCapacity, NativeConnectionKeyToken,
};
use crate::native_incoming_key_capacity::{
    NativeIncomingKey, NativeIncomingKeyCapacity, NativeIncomingKeyToken,
};
use crate::native_lane::{
    NativeLane, NativeLaneStreamGate, NativeLaneStreamPermit, NativeTransportObserver,
    PositionKind, SharedObserver, StreamDirection,
};

/// Independent admission domains; incoming Membership never borrows outgoing positions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportClass {
    /// Incoming FE data and both directions of peer exchange/runtime filters.
    Data,
    /// Incoming FE lifecycle, conservative outgoing reports, and handshakes.
    Control,
    /// Frontend incoming Backend announcements, including bootstrap and closing tails.
    Membership,
}

impl TransportClass {
    const fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Control => 1,
            Self::Membership => 2,
        }
    }

    /// Stable metric label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Control => "control",
            Self::Membership => "membership",
        }
    }
}

/// Position counts derived from the frozen Native result support geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionDimensions {
    pub data_positions: usize,
    pub control_positions: usize,
    pub data_handshakes: usize,
    pub control_handshakes: usize,
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}

fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(invalid)
}

fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b).ok_or_else(invalid)
}

fn value(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid())
}

impl AdmissionDimensions {
    /// The Backend role's dimensions from the frozen geometry.
    pub fn frozen() -> io::Result<Self> {
        Self::backend(&NativeResultSupportGeometry::V1)
    }

    /// Checked arithmetic over a Backend's support geometry. The counts cover
    /// every legal FE lane and both peer directions with their connecting and
    /// closing headroom, plus the handshake positions of each class.
    pub fn backend(g: &NativeResultSupportGeometry) -> io::Result<Self> {
        let frontends = value(g.transport_authenticated_live_frontends_per_backend)?;
        let connecting = value(g.transport_connecting_positions_per_lane)?;
        let closing = value(g.transport_closing_positions_per_lane)?;
        let fe_live = add(
            add(
                value(g.transport_connections_per_frontend_backend_result)?,
                value(g.transport_connections_per_frontend_backend_observation)?,
            )?,
            value(g.transport_connections_per_frontend_backend_submission)?,
        )?;
        // FE data is incoming only: frontends * (live + 3 lanes * (connecting + closing)).
        let incoming_fe_data = mul(frontends, add(fe_live, mul(3, add(connecting, closing)?)?)?)?;
        let exchange = add(
            value(g.transport_exchange_connections_per_peer)?,
            add(
                value(g.transport_exchange_connecting_positions_per_peer)?,
                value(g.transport_exchange_closing_positions_per_peer)?,
            )?,
        )?;
        let filters = add(
            value(g.transport_runtime_filter_connections_per_peer)?,
            add(
                value(g.transport_runtime_filter_connecting_positions_per_peer)?,
                value(g.transport_runtime_filter_closing_positions_per_peer)?,
            )?,
        )?;
        // Both peer directions conservatively include every live peer.
        let peer_data = mul(
            mul(2, value(g.transport_maximum_live_backends)?)?,
            add(exchange, filters)?,
        )?;
        let data_handshakes = value(g.transport_data_handshake_positions)?;
        let control_handshakes = value(g.transport_control_handshake_positions)?;
        let data_floor = add(add(incoming_fe_data, peer_data)?, data_handshakes)?;
        // Incoming lifecycle lane per frontend, with connecting/closing headroom.
        let control_lane = mul(
            frontends,
            add(
                value(g.transport_connections_per_frontend_backend_lifecycle_control)?,
                add(connecting, closing)?,
            )?,
        )?;
        // Backend announce/report uses the data client runtime in the outgoing
        // FE direction; reserve another control-lane worth there rather than
        // borrowing incoming Control positions.
        let data_positions = add(data_floor, control_lane)?;
        let control_positions = add(mul(2, control_lane)?, control_handshakes)?;
        if data_handshakes == 0 || control_handshakes == 0 {
            return Err(invalid());
        }
        Ok(Self {
            data_positions,
            control_positions,
            data_handshakes,
            control_handshakes,
        })
    }

    /// Checked arithmetic over a Frontend's outgoing lanes: for every live
    /// Backend, each lane's connections with their connecting and closing
    /// headroom. A dial (including Tonic's internal reconnect) holds one
    /// handshake position until its IO is established; each lane of each
    /// Backend may have its connecting positions in flight at once.
    pub fn frontend(g: &NativeResultSupportGeometry) -> io::Result<Self> {
        let backends = value(g.transport_maximum_live_backends)?;
        let connecting = value(g.transport_connecting_positions_per_lane)?;
        let tails = add(connecting, value(g.transport_closing_positions_per_lane)?)?;
        let data_live = add(
            add(
                value(g.transport_connections_per_frontend_backend_result)?,
                value(g.transport_connections_per_frontend_backend_observation)?,
            )?,
            value(g.transport_connections_per_frontend_backend_submission)?,
        )?;
        let control_live = value(g.transport_connections_per_frontend_backend_lifecycle_control)?;
        let data_positions = mul(backends, add(data_live, mul(3, tails)?)?)?;
        let control_positions = mul(backends, add(control_live, tails)?)?;
        let data_handshakes = mul(backends, mul(3, connecting)?)?;
        let control_handshakes = mul(backends, connecting)?;
        if data_handshakes == 0 || control_handshakes == 0 {
            return Err(invalid());
        }
        Ok(Self {
            data_positions,
            control_positions,
            data_handshakes,
            control_handshakes,
        })
    }

    /// Every connection position of both classes; handshakes are a subset
    /// of these, because a bootstrapping connection is already a connection.
    pub fn connection_positions(&self) -> io::Result<usize> {
        add(self.data_positions, self.control_positions)
    }

    /// Frontend incoming membership (physical, handshake) counts, independent of outgoing lanes.
    pub fn frontend_membership(g: &NativeResultSupportGeometry) -> io::Result<(usize, usize)> {
        let backends = value(g.transport_maximum_live_backends)?;
        let connecting = value(g.transport_connecting_positions_per_lane)?;
        let positions = mul(
            backends,
            add(
                1,
                add(connecting, value(g.transport_closing_positions_per_lane)?)?,
            )?,
        )?;
        let handshakes = mul(backends, connecting)?;
        if positions == 0 || handshakes == 0 {
            return Err(invalid());
        }
        Ok((positions, handshakes))
    }
}

/// Which role's lanes an admission serves. A Backend serves Native lanes and
/// dials peers and its Frontend; a Frontend dials Backend lanes and serves membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportRole {
    Backend,
    Frontend,
}

struct ClassState {
    physical_limit: usize,
    handshake_limit: usize,
    physical: Arc<Semaphore>,
    handshake: Arc<Semaphore>,
    refused: AtomicU64,
}

impl ClassState {
    fn new(positions: usize, handshakes: usize) -> Self {
        Self {
            physical_limit: positions,
            handshake_limit: handshakes,
            physical: Arc::new(Semaphore::new(positions)),
            handshake: Arc::new(Semaphore::new(handshakes)),
            refused: AtomicU64::new(0),
        }
    }
}

// Fixed observations of the original Frontend call/body holders, not capacity.
#[derive(Default)]
struct FrontendOutgoingState {
    closed: bool,
    requests: [usize; 4],
}

/// Frontend outgoing public-exit observations, independent of incoming Membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendOutgoingSnapshot {
    pub closed: bool,
    pub data_connections: usize,
    pub control_connections: usize,
    pub data_handshakes: usize,
    pub control_handshakes: usize,
    /// ResultData, Submission, Observation, LifecycleControl, in that order.
    pub requests: [usize; 4],
}

impl FrontendOutgoingSnapshot {
    pub fn is_drained(&self) -> bool {
        self.closed
            && self.data_connections == 0
            && self.control_connections == 0
            && self.data_handshakes == 0
            && self.control_handshakes == 0
            && self.requests == [0; 4]
    }
}

struct Core {
    role: TransportRole,
    dimensions: AdmissionDimensions,
    classes: [ClassState; 3],
    connection_keys: NativeConnectionKeyCapacity,
    incoming_keys: NativeIncomingKeyCapacity,
    incoming_streams: [NativeLaneStreamGate; NativeLane::COUNT],
    root_result_ingress_owners: usize,
    observer: SharedObserver,
    frontend_outgoing: Mutex<FrontendOutgoingState>,
    frontend_outgoing_changed: Arc<Notify>,
}

/// Stream positions a Backend serves per lane: every legal connection of the
/// lane times the per-connection stream limit H2 advertises. A Frontend
/// serves only the Membership lane.
pub fn incoming_lane_stream_limit(
    role: TransportRole,
    lane: NativeLane,
    g: &NativeResultSupportGeometry,
) -> io::Result<usize> {
    if role == TransportRole::Frontend {
        return if lane == NativeLane::Membership {
            mul(
                AdmissionDimensions::frontend_membership(g)?.0,
                value(g.transport_streams_per_connection)?,
            )
        } else {
            Ok(0)
        };
    }
    let frontends = value(g.transport_authenticated_live_frontends_per_backend)?;
    let backends = value(g.transport_maximum_live_backends)?;
    let connections = match lane {
        NativeLane::ResultData => mul(
            frontends,
            value(g.transport_connections_per_frontend_backend_result)?,
        )?,
        NativeLane::Submission => mul(
            frontends,
            value(g.transport_connections_per_frontend_backend_submission)?,
        )?,
        NativeLane::Observation => mul(
            frontends,
            value(g.transport_connections_per_frontend_backend_observation)?,
        )?,
        NativeLane::LifecycleControl => mul(
            frontends,
            value(g.transport_connections_per_frontend_backend_lifecycle_control)?,
        )?,
        NativeLane::Exchange => mul(backends, value(g.transport_exchange_connections_per_peer)?)?,
        NativeLane::RuntimeFilter => mul(
            backends,
            value(g.transport_runtime_filter_connections_per_peer)?,
        )?,
        // Announce is served by the Frontend's membership listener.
        NativeLane::Membership => 0,
    };
    mul(connections, value(g.transport_streams_per_connection)?)
}

/// Root unary ownership outlives a served stream when h2 still owns DATA.
/// Cover every live result stream plus every legal root's fixed metadata and
/// payload send holder. These are metadata positions, never payload grants.
/// The root channel's joint process wallet remains the payload authority.
pub fn root_result_ingress_owner_limit(
    role: TransportRole,
    g: &NativeResultSupportGeometry,
) -> io::Result<usize> {
    if role == TransportRole::Frontend {
        return Ok(0);
    }
    let roots = mul(
        value(g.transport_authenticated_live_frontends_per_backend)?,
        value(g.transport_worst_case_roots_per_frontend_per_backend)?,
    )?;
    // Mirrors the channel's finite fixed_metadata_holders geometry: driver
    // owners, send owners, and one producer cursor. Include payload holders
    // separately because their backing can coexist with those metadata owners.
    let metadata = add(
        add(
            value(g.root_maximum_root_drivers)?,
            value(g.root_live_send_holders)?,
        )?,
        1,
    )?;
    let tails = mul(roots, add(metadata, value(g.root_live_send_holders)?)?)?;
    add(
        incoming_lane_stream_limit(role, NativeLane::ResultData, g)?,
        tails,
    )
}

/// Process-scoped Native connection admission. Clones share one set of
/// positions; there is exactly one per role process.
#[derive(Clone)]
pub struct NativeTransportAdmission {
    core: Arc<Core>,
}

impl fmt::Debug for NativeTransportAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTransportAdmission")
            .field("role", &self.core.role)
            .field("dimensions", &self.core.dimensions)
            .finish_non_exhaustive()
    }
}

impl NativeTransportAdmission {
    /// A Backend admission without a metrics observer.
    pub fn new() -> io::Result<Self> {
        Self::backend(None)
    }

    /// The Backend role's admission from the frozen geometry.
    pub fn backend(observer: Option<Arc<dyn NativeTransportObserver>>) -> io::Result<Self> {
        Self::with_parts(
            TransportRole::Backend,
            AdmissionDimensions::frozen()?,
            observer,
        )
    }

    /// The Frontend role's outgoing and incoming membership admission from the frozen geometry.
    pub fn frontend(observer: Option<Arc<dyn NativeTransportObserver>>) -> io::Result<Self> {
        Self::with_parts(
            TransportRole::Frontend,
            AdmissionDimensions::frontend(&NativeResultSupportGeometry::V1)?,
            observer,
        )
    }

    pub fn with_dimensions(dimensions: AdmissionDimensions) -> io::Result<Self> {
        Self::with_parts(TransportRole::Backend, dimensions, None)
    }

    pub(crate) fn with_parts(
        role: TransportRole,
        dimensions: AdmissionDimensions,
        observer: SharedObserver,
    ) -> io::Result<Self> {
        let g = NativeResultSupportGeometry::V1;
        let (membership_positions, membership_handshakes) = if role == TransportRole::Frontend {
            AdmissionDimensions::frontend_membership(&g)?
        } else {
            (0, 0)
        };
        let mut limits = [0; NativeLane::COUNT];
        for lane in NativeLane::ALL {
            limits[lane.index()] = incoming_lane_stream_limit(role, lane, &g)?;
        }
        let root_result_ingress_owners = root_result_ingress_owner_limit(role, &g)?;
        let incoming_streams = NativeLane::ALL.map(|lane| {
            NativeLaneStreamGate::new(
                lane,
                StreamDirection::Incoming,
                limits[lane.index()],
                observer.clone(),
            )
        });
        let admission = Self {
            core: Arc::new(Core {
                role,
                dimensions,
                classes: [
                    ClassState::new(dimensions.data_positions, dimensions.data_handshakes),
                    ClassState::new(dimensions.control_positions, dimensions.control_handshakes),
                    ClassState::new(membership_positions, membership_handshakes),
                ],
                connection_keys: NativeConnectionKeyCapacity::new()?,
                incoming_keys: NativeIncomingKeyCapacity::new()?,
                incoming_streams,
                root_result_ingress_owners,
                observer,
                frontend_outgoing: Mutex::new(FrontendOutgoingState::default()),
                frontend_outgoing_changed: Arc::new(Notify::new()),
            }),
        };
        for class in [
            TransportClass::Data,
            TransportClass::Control,
            TransportClass::Membership,
        ] {
            admission.publish(class, PositionKind::Connection);
            admission.publish(class, PositionKind::Handshake);
        }
        Ok(admission)
    }

    /// Close only this Frontend's outgoing Native capability. Existing holders
    /// retain their original positions until their public exits.
    pub fn close_frontend_outgoing(&self) -> io::Result<()> {
        if self.core.role != TransportRole::Frontend {
            return Err(invalid());
        }
        {
            let mut state = self
                .core
                .frontend_outgoing
                .lock()
                .expect("outgoing state lock");
            // The same lock linearizes call registration against close. Dial
            // acquisitions linearize at the original Semaphore::close calls.
            for class in [TransportClass::Data, TransportClass::Control] {
                self.core.classes[class.index()].physical.close();
                self.core.classes[class.index()].handshake.close();
            }
            state.closed = true;
        }
        self.core.frontend_outgoing_changed.notify_waiters();
        Ok(())
    }

    pub fn frontend_outgoing_snapshot(&self) -> io::Result<FrontendOutgoingSnapshot> {
        if self.core.role != TransportRole::Frontend {
            return Err(invalid());
        }
        let state = self
            .core
            .frontend_outgoing
            .lock()
            .expect("outgoing state lock");
        Ok(FrontendOutgoingSnapshot {
            closed: state.closed,
            data_connections: self.positions(TransportClass::Data)
                - self.available_positions(TransportClass::Data),
            control_connections: self.positions(TransportClass::Control)
                - self.available_positions(TransportClass::Control),
            data_handshakes: self.handshake_positions(TransportClass::Data)
                - self.available_handshakes(TransportClass::Data),
            control_handshakes: self.handshake_positions(TransportClass::Control)
                - self.available_handshakes(TransportClass::Control),
            requests: state.requests,
        })
    }

    /// Observe the closed role's original exits using the caller's original
    /// absolute deadline. A timeout consumes no holder and permits retry.
    pub async fn wait_frontend_outgoing_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        if self.core.role != TransportRole::Frontend {
            return Err(invalid());
        }
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let changed = self.core.frontend_outgoing_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.frontend_outgoing_snapshot()?.is_drained() {
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?;
        }
    }

    pub(crate) fn frontend_outgoing_is_closed(&self) -> bool {
        self.core
            .frontend_outgoing
            .lock()
            .expect("outgoing state lock")
            .closed
    }

    pub(crate) fn frontend_outgoing_notified(&self) -> tokio::sync::futures::OwnedNotified {
        Arc::clone(&self.core.frontend_outgoing_changed).notified_owned()
    }

    /// Register the original outgoing request at its actual dispatch boundary.
    /// This is an exit observation, not another admission position or wallet.
    pub(crate) fn register_frontend_outgoing_call(
        &self,
        lane: NativeLane,
    ) -> io::Result<FrontendOutgoingCallExit> {
        if self.core.role != TransportRole::Frontend || lane.index() >= 4 {
            return Err(invalid());
        }
        {
            let mut state = self
                .core
                .frontend_outgoing
                .lock()
                .expect("outgoing state lock");
            if state.closed {
                return Err(io::ErrorKind::ConnectionAborted.into());
            }
            state.requests[lane.index()] = state.requests[lane.index()]
                .checked_add(1)
                .ok_or_else(invalid)?;
        }
        Ok(FrontendOutgoingCallExit {
            admission: self.clone(),
            lane,
        })
    }

    pub fn role(&self) -> TransportRole {
        self.core.role
    }

    pub fn dimensions(&self) -> AdmissionDimensions {
        self.core.dimensions
    }

    pub fn positions(&self, class: TransportClass) -> usize {
        self.core.classes[class.index()].physical_limit
    }

    pub fn available_positions(&self, class: TransportClass) -> usize {
        self.core.classes[class.index()]
            .physical
            .available_permits()
    }

    pub fn handshake_positions(&self, class: TransportClass) -> usize {
        self.core.classes[class.index()].handshake_limit
    }

    pub fn available_handshakes(&self, class: TransportClass) -> usize {
        self.core.classes[class.index()]
            .handshake
            .available_permits()
    }

    /// Connections of `class` refused for lack of a position since start.
    pub fn refused_connections(&self, class: TransportClass) -> u64 {
        self.core.classes[class.index()]
            .refused
            .load(Ordering::Relaxed)
    }

    /// The positions of streams this process serves on `lane`.
    pub fn incoming_streams(&self, lane: NativeLane) -> &NativeLaneStreamGate {
        &self.core.incoming_streams[lane.index()]
    }

    pub fn root_result_ingress_owner_limit(&self) -> usize {
        self.core.root_result_ingress_owners
    }

    /// Take one served stream position for `method`'s lane, or refuse.
    pub(crate) fn try_incoming_stream(
        &self,
        method: NativeRpcMethod,
    ) -> Option<Option<NativeLaneStreamPermit>> {
        let Some(lane) = NativeLane::of(method.contract().traffic) else {
            return Some(None);
        };
        self.incoming_streams(lane).try_acquire().map(Some)
    }

    pub(crate) fn observer(&self) -> SharedObserver {
        self.core.observer.clone()
    }

    fn publish(&self, class: TransportClass, kind: PositionKind) {
        let Some(observer) = &self.core.observer else {
            return;
        };
        let state = &self.core.classes[class.index()];
        let (limit, available) = match kind {
            PositionKind::Connection => (state.physical_limit, state.physical.available_permits()),
            PositionKind::Handshake => (state.handshake_limit, state.handshake.available_permits()),
        };
        observer.positions(class, kind, limit.saturating_sub(available), limit);
    }

    fn refuse(&self, class: TransportClass) -> io::Error {
        self.core.classes[class.index()]
            .refused
            .fetch_add(1, Ordering::Relaxed);
        if let Some(observer) = &self.core.observer {
            observer.refused(class);
        }
        io::ErrorKind::WouldBlock.into()
    }

    fn class_permit(&self, class: TransportClass, kind: PositionKind) -> io::Result<ClassPermit> {
        let state = &self.core.classes[class.index()];
        let semaphore = match kind {
            PositionKind::Connection => &state.physical,
            PositionKind::Handshake => &state.handshake,
        };
        let permit = Arc::clone(semaphore)
            .try_acquire_owned()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        self.publish(class, kind);
        Ok(ClassPermit {
            permit: Some(permit),
            admission: self.clone(),
            class,
            kind,
        })
    }

    fn try_positions(&self, class: TransportClass) -> io::Result<(ClassPermit, ClassPermit)> {
        let positions = self
            .class_permit(class, PositionKind::Connection)
            .and_then(|physical| {
                self.class_permit(class, PositionKind::Handshake)
                    .map(|handshake| (physical, handshake))
            });
        positions.map_err(|_| self.refuse(class))
    }

    /// Admit one accepted socket. Refusal (`WouldBlock`) takes nothing; the
    /// caller closes the socket.
    pub(crate) fn try_accept(&self, class: TransportClass) -> io::Result<AcceptedConnection> {
        let (physical, handshake) = self.try_positions(class)?;
        let seal = Arc::new(IncomingSeal {
            admission: self.clone(),
            state: Mutex::new(SealState::Open),
        });
        Ok(AcceptedConnection {
            connection: NativeConnectionPermit {
                key: None,
                incoming: Some(Arc::clone(&seal)),
                lane: None,
                _physical: physical,
            },
            handshake: NativeHandshakePermit::new(handshake),
            binding: NativeIncomingConnectionBinding { seal },
        })
    }

    /// Admit one outgoing connection attempt before any IO. A peer/lane key
    /// claims its connecting position first; refusal rolls everything back.
    pub(crate) fn try_dial(
        &self,
        class: TransportClass,
        key: Option<InlineNativeChannelIdentity>,
    ) -> io::Result<DialAdmission> {
        let lane = key.and_then(|key| NativeLane::of(key.method().contract().traffic));
        let key = key
            .map(|key| {
                self.core
                    .connection_keys
                    .claim(key)
                    .map(|token| KeyClaim {
                        admission: self.clone(),
                        token,
                    })
                    .map_err(|error| match error.kind() {
                        io::ErrorKind::WouldBlock => self.refuse(class),
                        _ => error,
                    })
            })
            .transpose()?;
        let (physical, handshake) = self.try_positions(class)?;
        Ok(DialAdmission {
            connection: NativeConnectionPermit {
                key,
                incoming: None,
                lane: None,
                _physical: physical,
            },
            handshake: NativeHandshakePermit::new(handshake),
            lane,
        })
    }

    /// Admit one Frontend dial of `lane`, before any IO.
    pub(crate) fn try_dial_lane(&self, lane: FrontendNativeLane) -> io::Result<DialAdmission> {
        let lane = NativeLane::frontend(lane);
        let (physical, handshake) = self.try_positions(lane.class())?;
        Ok(DialAdmission {
            connection: NativeConnectionPermit {
                key: None,
                incoming: None,
                lane: None,
                _physical: physical,
            },
            handshake: NativeHandshakePermit::new(handshake),
            lane: Some(lane),
        })
    }
}

/// One held class position; dropping it publishes the new count.
struct ClassPermit {
    permit: Option<OwnedSemaphorePermit>,
    admission: NativeTransportAdmission,
    class: TransportClass,
    kind: PositionKind,
}

impl Drop for ClassPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        // Wake from the original permit return, before optional diagnostics.
        self.admission
            .core
            .frontend_outgoing_changed
            .notify_waiters();
        self.admission.publish(self.class, self.kind);
    }
}

/// An observation carried by the existing original stream holder. It grants
/// no position and drops only at that holder's public exit.
pub(crate) struct FrontendOutgoingCallExit {
    admission: NativeTransportAdmission,
    lane: NativeLane,
}

impl Drop for FrontendOutgoingCallExit {
    fn drop(&mut self) {
        {
            let mut state = self
                .admission
                .core
                .frontend_outgoing
                .lock()
                .expect("outgoing state lock");
            state.requests[self.lane.index()] = state.requests[self.lane.index()]
                .checked_sub(1)
                .expect("original outgoing request exits once");
        }
        self.admission
            .core
            .frontend_outgoing_changed
            .notify_waiters();
    }
}

/// Positions of one accepted connection.
pub(crate) struct AcceptedConnection {
    pub(crate) connection: NativeConnectionPermit,
    pub(crate) handshake: NativeHandshakePermit,
    pub(crate) binding: NativeIncomingConnectionBinding,
}

/// Positions of one dial attempt, before its IO is established.
pub(crate) struct DialAdmission {
    connection: NativeConnectionPermit,
    handshake: NativeHandshakePermit,
    /// Counted as a live lane connection only once the IO is established.
    lane: Option<NativeLane>,
}

/// Temporary custody during the fallible Connecting-to-Live transition. The
/// actual IO and original admission are installed before callbacks can unwind.
struct DialIoCustody {
    io: Option<novarocks_transport_io::BoxedIo>,
    admission: Option<DialAdmission>,
}

impl Drop for DialIoCustody {
    fn drop(&mut self) {
        let admission = self.admission.take();
        drop(self.io.take());
        drop(admission);
    }
}

impl DialAdmission {
    /// Publish a connected IO as Live without allowing a refusal or observer
    /// unwind to return its physical position before the IO is destroyed.
    pub(crate) fn established_io(
        self,
        io: novarocks_transport_io::BoxedIo,
    ) -> io::Result<novarocks_transport_io::OwnedIo> {
        let mut custody = DialIoCustody {
            io: Some(io),
            admission: Some(self),
        };
        let admission = custody.admission.as_mut().expect("original dial installed");
        if let Some(key) = &admission.connection.key {
            key.admission.core.connection_keys.install(key.token)?;
        }
        admission.handshake.release();
        if let (Some(lane), Some(observer)) = (
            admission.lane,
            &admission.connection._physical.admission.core.observer,
        ) {
            observer.lane_connections(lane, 1);
        }
        admission.connection.lane = admission.lane;
        // Every fallible or foreign step has completed. Transfer the same IO
        // and physical permit directly into the established transport holder.
        let io = custody.io.take().expect("original connected IO installed");
        let DialAdmission { connection, .. } =
            custody.admission.take().expect("original dial installed");
        Ok(novarocks_transport_io::OwnedIo::with_guard(io, connection))
    }
}

struct KeyClaim {
    admission: NativeTransportAdmission,
    token: NativeConnectionKeyToken,
}

impl Drop for KeyClaim {
    fn drop(&mut self) {
        let keys = &self.admission.core.connection_keys;
        let _ = keys.retire(self.token);
        keys.exit(self.token)
            .expect("exact physical key generation exits once");
    }
}

/// Held by the connection's IO wrapper. Dropping it (after the IO itself) is
/// the connection's public exit: the peer key and the incoming key are
/// cleared first, then the physical position is returned.
pub struct NativeConnectionPermit {
    key: Option<KeyClaim>,
    incoming: Option<Arc<IncomingSeal>>,
    /// The established outgoing lane, counted in the observer.
    lane: Option<NativeLane>,
    _physical: ClassPermit,
}

impl fmt::Debug for NativeConnectionPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeConnectionPermit")
            .field("keyed", &self.key.is_some())
            .field("incoming", &self.incoming.is_some())
            .field("lane", &self.lane)
            .finish_non_exhaustive()
    }
}

impl Drop for NativeConnectionPermit {
    fn drop(&mut self) {
        if let Some(seal) = self.incoming.take() {
            seal.close();
        }
        drop(self.key.take());
        if let (Some(lane), Some(observer)) = (self.lane, &self._physical.admission.core.observer) {
            observer.lane_connections(lane, -1);
        }
    }
}

/// The handshake position of one connection. Clones share it; the first
/// `release` (bootstrap complete) or the drop of every clone returns it.
#[derive(Clone)]
pub(crate) struct NativeHandshakePermit {
    permit: Arc<Mutex<Option<ClassPermit>>>,
}

impl NativeHandshakePermit {
    fn new(permit: ClassPermit) -> Self {
        Self {
            permit: Arc::new(Mutex::new(Some(permit))),
        }
    }

    pub(crate) fn release(&self) {
        let permit = self.permit.lock().expect("handshake permit lock").take();
        drop(permit);
    }

    pub(crate) fn is_held(&self) -> bool {
        self.permit.lock().expect("handshake permit lock").is_some()
    }
}

enum SealState {
    Open,
    Sealed(
        NativeIncomingKey,
        NativeIncomingKeyToken,
        Option<NativeLane>,
    ),
    Closed,
}

struct IncomingSeal {
    admission: NativeTransportAdmission,
    state: Mutex<SealState>,
}

impl IncomingSeal {
    fn close(&self) {
        let previous = std::mem::replace(
            &mut *self.state.lock().expect("incoming seal lock"),
            SealState::Closed,
        );
        if let SealState::Sealed(_, token, lane) = previous {
            let keys = &self.admission.core.incoming_keys;
            let _ = keys.retire(token);
            keys.exit(token)
                .expect("exact original incoming key exits once");
            if let (Some(lane), Some(observer)) = (lane, &self.admission.core.observer) {
                observer.lane_connections(lane, -1);
            }
        }
    }
}

/// One accepted connection's incoming identity. The first authenticated
/// request seals it to one caller process, endpoint and traffic class; any
/// later request with another key is refused and the connection is closed.
#[derive(Clone)]
pub(crate) struct NativeIncomingConnectionBinding {
    seal: Arc<IncomingSeal>,
}

impl NativeIncomingConnectionBinding {
    pub(crate) fn seal(&self, key: NativeIncomingKey) -> io::Result<()> {
        let mut state = self
            .seal
            .state
            .lock()
            .map_err(|_| io::ErrorKind::InvalidData)?;
        match &*state {
            SealState::Closed => Err(io::ErrorKind::ConnectionAborted.into()),
            SealState::Sealed(existing, ..) if *existing == key => Ok(()),
            SealState::Sealed(..) => Err(io::ErrorKind::ConnectionAborted.into()),
            SealState::Open => {
                let token = self.seal.admission.core.incoming_keys.claim(key)?;
                let lane = NativeLane::of(key.traffic());
                *state = SealState::Sealed(key, token, lane);
                if let (Some(lane), Some(observer)) = (lane, &self.seal.admission.core.observer) {
                    observer.lane_connections(lane, 1);
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "native_dial_io_exit_tests.rs"]
mod dial_io_exit_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_ingress_owner_geometry_covers_live_streams_and_both_root_tail_owners() {
        let g = NativeResultSupportGeometry::V1;
        assert_eq!(
            root_result_ingress_owner_limit(TransportRole::Backend, &g).unwrap(),
            45_184
        );
        assert_eq!(
            root_result_ingress_owner_limit(TransportRole::Frontend, &g).unwrap(),
            0
        );
        let mut invalid = g;
        invalid.transport_worst_case_roots_per_frontend_per_backend = u64::MAX;
        assert!(root_result_ingress_owner_limit(TransportRole::Backend, &invalid).is_err());
        invalid = g;
        invalid.root_maximum_root_drivers = u64::MAX;
        assert!(root_result_ingress_owner_limit(TransportRole::Backend, &invalid).is_err());
    }

    fn small() -> NativeTransportAdmission {
        NativeTransportAdmission::with_dimensions(AdmissionDimensions {
            data_positions: 2,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        })
        .unwrap()
    }

    #[test]
    fn frozen_dimensions_are_checked_and_nonzero() {
        let dimensions = AdmissionDimensions::frozen().unwrap();
        assert!(dimensions.data_positions > dimensions.data_handshakes);
        assert!(dimensions.control_positions > dimensions.control_handshakes);
        assert_eq!(dimensions.data_handshakes, 32);
        assert_eq!(dimensions.control_handshakes, 8);
    }

    #[test]
    fn membership_saturation_never_borrows_outgoing_positions_and_returns_on_exit() {
        let admission = NativeTransportAdmission::frontend(None).unwrap();
        let class = TransportClass::Membership;
        assert_eq!(admission.positions(class), 96);
        assert_eq!(admission.handshake_positions(class), 32);
        assert_eq!(
            incoming_lane_stream_limit(
                TransportRole::Frontend,
                NativeLane::Membership,
                &NativeResultSupportGeometry::V1
            )
            .unwrap(),
            12_288
        );
        let outgoing = (
            admission.available_positions(TransportClass::Data),
            admission.available_positions(TransportClass::Control),
        );
        let mut held = Vec::new();
        for _ in 0..32 {
            held.push(admission.try_accept(class).unwrap());
        }
        assert_eq!(
            admission.try_accept(class).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            admission.available_positions(class),
            64,
            "failed handshake rolls back its physical position"
        );
        for connection in &held {
            connection.handshake.release();
        }
        for _ in 32..96 {
            let connection = admission.try_accept(class).unwrap();
            connection.handshake.release();
            held.push(connection);
        }
        assert_eq!(
            admission.try_accept(class).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            (
                admission.available_positions(TransportClass::Data),
                admission.available_positions(TransportClass::Control)
            ),
            outgoing
        );
        assert_eq!(admission.refused_connections(class), 2);
        drop(held);
        assert_eq!(admission.available_positions(class), 96);
        assert_eq!(admission.available_handshakes(class), 32);
        assert_eq!(NativeTransportAdmission::new().unwrap().positions(class), 0);
    }

    #[test]
    fn membership_key_reuse_waits_for_the_original_connection_exit() {
        use novarocks_native_trust::NativeProcessIdentity;
        use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeTrafficClass};
        let admission = NativeTransportAdmission::frontend(None).unwrap();
        let key = NativeIncomingKey::new(
            NativeProcessIdentity::Backend(novarocks_types::BackendProcessId::new_v7()),
            NativeEndpointDomain::FrontendMembership,
            NativeTrafficClass::Membership,
        )
        .unwrap();
        let other = NativeIncomingKey::new(
            NativeProcessIdentity::Backend(novarocks_types::BackendProcessId::new_v7()),
            NativeEndpointDomain::FrontendMembership,
            NativeTrafficClass::Membership,
        )
        .unwrap();
        let first = admission.try_accept(TransportClass::Membership).unwrap();
        first.binding.seal(key).unwrap();
        first.handshake.release();
        first.binding.seal(key).unwrap();
        assert_eq!(
            first.binding.seal(other).err().unwrap().kind(),
            io::ErrorKind::ConnectionAborted
        );
        let replacement = admission.try_accept(TransportClass::Membership).unwrap();
        assert_eq!(
            replacement.binding.seal(key).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        // Dropping a handler alias cannot retire the original socket's identity.
        let AcceptedConnection {
            connection,
            handshake,
            binding,
        } = first;
        drop(binding);
        drop(handshake);
        assert!(replacement.binding.seal(key).is_err());
        drop(connection);
        replacement.binding.seal(key).unwrap();
        drop(replacement);
        assert_eq!(
            admission.available_positions(TransportClass::Membership),
            96
        );
    }

    #[test]
    fn control_and_data_positions_never_borrow_from_each_other() {
        let admission = small();
        let control = admission.try_accept(TransportClass::Control).unwrap();
        assert_eq!(
            admission
                .try_accept(TransportClass::Control)
                .err()
                .map(|error| error.kind()),
            Some(io::ErrorKind::WouldBlock)
        );
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
        let data = admission.try_accept(TransportClass::Data).unwrap();
        assert_eq!(admission.available_positions(TransportClass::Control), 0);
        drop(control);
        drop(data);
        assert_eq!(admission.available_positions(TransportClass::Control), 1);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn handshake_position_returns_at_bootstrap_while_physical_stays_until_drop() {
        let admission = small();
        let accepted = admission.try_accept(TransportClass::Data).unwrap();
        assert_eq!(admission.available_handshakes(TransportClass::Data), 0);
        assert!(
            admission.try_accept(TransportClass::Data).is_err(),
            "an incomplete bootstrap holds the only data handshake position"
        );
        accepted.handshake.release();
        assert!(!accepted.handshake.is_held());
        assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
        assert_eq!(admission.available_positions(TransportClass::Data), 1);
        drop(accepted.connection);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn dropping_an_unfinished_bootstrap_returns_its_handshake_position() {
        let admission = small();
        let accepted = admission.try_accept(TransportClass::Data).unwrap();
        let AcceptedConnection {
            connection,
            handshake,
            binding,
        } = accepted;
        drop(handshake);
        drop(binding);
        assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
        drop(connection);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn refused_handshake_rolls_back_the_physical_position() {
        let admission = small();
        let first = admission.try_accept(TransportClass::Data).unwrap();
        assert!(admission.try_accept(TransportClass::Data).is_err());
        assert_eq!(admission.available_positions(TransportClass::Data), 1);
        drop(first);
    }
}
