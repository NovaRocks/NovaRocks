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

//! Startup validation of the Native transport geometry, and the count part of
//! its structural memory envelope `E_native_transport`.
//!
//! Every transport count NovaRocks enforces (connections per lane, streams per
//! connection, Tonic's pending queue, handshakes) and every per-item size the
//! libraries accept through public configuration (receive windows, frames,
//! header lists, send buffers) must be self-consistent and multiply without
//! overflow. The product of counts and sizes is the structural part of the
//! envelope. The per-object fixed overheads `c_*` (connection, stream,
//! handshake and queued request) are measured and frozen by P00b; until then
//! the envelope reports only its structural part and claims no byte bound.
//!
//! Design: ADR-0170 (docs/adr/ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md)

use std::io;

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;

use crate::native_lane::{NativeLane, StreamDirection};
use crate::native_transport_admission::{AdmissionDimensions, TransportRole};
use crate::root_result_unary::{ROOT_CONTROL_FRAME_MAX_BYTES, ROOT_DATA_FRAME_MAX_BYTES};

/// Tonic 0.12.3's `Endpoint` does not expose these HTTP/2 client settings, so
/// the Hyper 1.8 / H2 0.4 client defaults apply. They are constants of the
/// structural bound and must be re-checked by the measurement gate whenever
/// those crates change.
pub mod client_defaults {
    /// Largest frame the client accepts (`SETTINGS_MAX_FRAME_SIZE`).
    pub const MAX_FRAME_BYTES: u64 = 16 * 1024;
    /// Hyper's per-stream send buffer limit.
    pub const SEND_BUFFER_BYTES_PER_STREAM: u64 = 1024 * 1024;
    /// HPACK dynamic table size (`SETTINGS_HEADER_TABLE_SIZE`).
    pub const HEADER_TABLE_BYTES: u64 = 4096;
    /// Locally reset streams H2 remembers per connection.
    pub const CONCURRENT_RESET_STREAMS: u64 = 50;
    /// Remotely reset streams H2 tolerates before closing the connection.
    pub const PENDING_ACCEPT_RESET_STREAMS: u64 = 20;
    /// Local error resets H2 tolerates before closing the connection.
    pub const LOCAL_ERROR_RESET_STREAMS: u64 = 1024;
}

const H2_MIN_FRAME_BYTES: u64 = 16_384;
const H2_MAX_FRAME_BYTES: u64 = (1 << 24) - 1;
const H2_MAX_WINDOW_BYTES: u64 = (1 << 31) - 1;

/// Measured fixed overhead per transport object (`c_conn`, `c_stream`,
/// `c_handshake`, `c_queue`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeTransportCoefficients {
    pub per_connection_bytes: u64,
    pub per_stream_bytes: u64,
    pub per_handshake_bytes: u64,
    pub per_queued_request_bytes: u64,
}

/// The frozen coefficients. `None` until P00b measures and freezes them on
/// real lanes; no value is assumed before that.
pub const FROZEN_TRANSPORT_COEFFICIENTS: Option<NativeTransportCoefficients> = None;

/// One lane and direction of a role's transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeLaneEnvelope {
    pub lane: NativeLane,
    pub direction: StreamDirection,
    /// Live connections with their connecting and closing headroom.
    pub connections: u64,
    pub streams_per_connection: u64,
    /// Receive window of one connection.
    pub connection_window_bytes: u64,
    /// Stream receive window, send buffer and header list of one stream.
    pub stream_bytes: u64,
    /// `connections * (connection_window + streams * stream_bytes)`.
    pub structural_bytes: u64,
}

/// A role's transport envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeTransportEnvelope {
    pub role: TransportRole,
    pub lanes: Vec<NativeLaneEnvelope>,
    pub connections: u64,
    pub streams: u64,
    pub handshake_positions: u64,
    /// Requests Tonic may queue in front of outgoing connections.
    pub queued_requests: u64,
    /// The count part: per-lane structural bytes summed.
    pub structural_bytes: u64,
    /// `None` until P00b; see [`FROZEN_TRANSPORT_COEFFICIENTS`].
    pub coefficients: Option<NativeTransportCoefficients>,
}

impl NativeTransportEnvelope {
    /// The complete envelope, available only once coefficients are frozen.
    pub fn total_bytes(&self) -> io::Result<Option<u64>> {
        let Some(c) = self.coefficients else {
            return Ok(None);
        };
        let total = add(
            add(
                self.structural_bytes,
                mul(self.connections, c.per_connection_bytes)?,
            )?,
            add(
                mul(self.streams, c.per_stream_bytes)?,
                add(
                    mul(self.handshake_positions, c.per_handshake_bytes)?,
                    mul(self.queued_requests, c.per_queued_request_bytes)?,
                )?,
            )?,
        )?;
        Ok(Some(total))
    }
}

/// The validated geometry of both roles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeTransportGeometryReport {
    pub backend_admission: AdmissionDimensions,
    pub frontend_admission: AdmissionDimensions,
    pub backend: NativeTransportEnvelope,
    pub frontend: NativeTransportEnvelope,
    pub backend_socket_positions: u64,
    pub frontend_socket_positions: u64,
}

/// Requested-byte bounds using the BE's effective Root wallet and ingress limit.
/// The lane subtotal stays independently comparable with the static report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackendRootEncodingReport {
    pub root_process_limit_bytes: u64,
    pub ingress_owner_limit: u64,
    pub data_owner_limit: u64,
    pub encoding_requested_bytes: u64,
    pub known_structural_bytes: u64,
}

impl NativeTransportGeometryReport {
    // Design: ADR-0277 (docs/adr/ADR-0277-third-party-encoding-buffers-use-transport-headroom.md)
    pub(crate) fn backend_root_encoding_report(
        &self,
        geometry: &NativeResultSupportGeometry,
        effective_process_limit: usize,
        actual_ingress_limit: usize,
    ) -> io::Result<BackendRootEncodingReport> {
        let process_limit = u64::try_from(effective_process_limit)
            .map_err(|_| refuse("effective Root process limit does not fit u64"))?;
        let ingress_limit = u64::try_from(actual_ingress_limit)
            .map_err(|_| refuse("Root ingress owner limit does not fit u64"))?;
        let fixed_charge = positive(
            "root_fixed_schema_cursor_driver_capacity_bytes",
            geometry.root_fixed_schema_cursor_driver_capacity_bytes,
        )?;
        let control_frame = ROOT_CONTROL_FRAME_MAX_BYTES as u64;
        let additional_data_bytes = (ROOT_DATA_FRAME_MAX_BYTES as u64)
            .checked_sub(control_frame)
            .ok_or_else(|| refuse("Root data frame limit is below the control frame limit"))?;

        // Data retains its original delivery position until the encoded backing
        // exits. Each Root represented here still holds its fixed wallet charge.
        // Control replies may lack a Root owner but all encoded replies hold
        // their original ingress permit, including DATA left in h2 after Body.
        let data_owner_limit = ingress_limit.min(mul(
            process_limit / fixed_charge,
            geometry.root_live_send_holders,
        )?);
        let encoding_requested_bytes = add(
            mul(ingress_limit, control_frame)?,
            mul(data_owner_limit, additional_data_bytes)?,
        )?;
        Ok(BackendRootEncodingReport {
            root_process_limit_bytes: process_limit,
            ingress_owner_limit: ingress_limit,
            data_owner_limit,
            encoding_requested_bytes,
            known_structural_bytes: add(self.backend.structural_bytes, encoding_requested_bytes)?,
        })
    }
}

fn refuse(detail: impl Into<String>) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("inconsistent Native transport geometry: {}", detail.into()),
    )
}

fn add(a: u64, b: u64) -> io::Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| refuse("transport counts overflow"))
}

fn mul(a: u64, b: u64) -> io::Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| refuse("transport counts overflow"))
}

fn positive(name: &str, value: u64) -> io::Result<u64> {
    if value == 0 {
        return Err(refuse(format!("{name} must be greater than 0")));
    }
    Ok(value)
}

fn at_most(name: &str, value: u64, limit: u64) -> io::Result<u64> {
    if value > limit {
        return Err(refuse(format!("{name}={value} exceeds {limit}")));
    }
    Ok(value)
}

/// Refuse a geometry whose counts or sizes the libraries cannot honor, that
/// overflows, or whose lanes cannot carry the supported root peak; then
/// compute both roles' admission and structural envelopes.
pub fn validate_native_transport_geometry(
    g: &NativeResultSupportGeometry,
) -> io::Result<NativeTransportGeometryReport> {
    let streams = positive(
        "transport_streams_per_connection",
        g.transport_streams_per_connection,
    )?;
    at_most(
        "transport_streams_per_connection",
        streams,
        u64::from(u32::MAX),
    )?;
    positive(
        "transport_tonic_pending_per_connection",
        g.transport_tonic_pending_per_connection,
    )?;
    for (name, value) in [
        (
            "transport_connections_per_frontend_backend_result",
            g.transport_connections_per_frontend_backend_result,
        ),
        (
            "transport_connections_per_frontend_backend_observation",
            g.transport_connections_per_frontend_backend_observation,
        ),
        (
            "transport_connections_per_frontend_backend_submission",
            g.transport_connections_per_frontend_backend_submission,
        ),
        (
            "transport_connections_per_frontend_backend_lifecycle_control",
            g.transport_connections_per_frontend_backend_lifecycle_control,
        ),
        (
            "transport_exchange_connections_per_peer",
            g.transport_exchange_connections_per_peer,
        ),
        (
            "transport_runtime_filter_connections_per_peer",
            g.transport_runtime_filter_connections_per_peer,
        ),
        (
            "transport_authenticated_live_frontends_per_backend",
            g.transport_authenticated_live_frontends_per_backend,
        ),
        (
            "transport_maximum_live_backends",
            g.transport_maximum_live_backends,
        ),
        (
            "transport_connecting_positions_per_lane",
            g.transport_connecting_positions_per_lane,
        ),
        (
            "transport_data_handshake_positions",
            g.transport_data_handshake_positions,
        ),
        (
            "transport_control_handshake_positions",
            g.transport_control_handshake_positions,
        ),
        (
            "transport_connect_deadline_ms",
            g.transport_connect_deadline_ms,
        ),
        (
            "transport_handshake_deadline_ms",
            g.transport_handshake_deadline_ms,
        ),
        ("transport_h2_pending_resets", g.transport_h2_pending_resets),
        (
            "transport_h2_send_buffer_bytes",
            g.transport_h2_send_buffer_bytes,
        ),
    ] {
        positive(name, value)?;
    }
    if !(H2_MIN_FRAME_BYTES..=H2_MAX_FRAME_BYTES).contains(&g.transport_h2_frame_bytes) {
        return Err(refuse(format!(
            "transport_h2_frame_bytes={} is outside the HTTP/2 range \
             {H2_MIN_FRAME_BYTES}..={H2_MAX_FRAME_BYTES}",
            g.transport_h2_frame_bytes
        )));
    }
    positive("transport_h2_header_bytes", g.transport_h2_header_bytes)?;
    at_most(
        "transport_h2_header_bytes",
        g.transport_h2_header_bytes,
        u64::from(u32::MAX),
    )?;
    let stream_window = positive(
        "transport_h2_stream_receive_window_bytes",
        g.transport_h2_stream_receive_window_bytes,
    )?;
    let connection_window = positive(
        "transport_h2_connection_receive_window_bytes",
        g.transport_h2_connection_receive_window_bytes,
    )?;
    at_most(
        "transport_h2_connection_receive_window_bytes",
        connection_window,
        H2_MAX_WINDOW_BYTES,
    )?;
    if stream_window > connection_window {
        return Err(refuse(format!(
            "transport_h2_stream_receive_window_bytes={stream_window} exceeds \
             transport_h2_connection_receive_window_bytes={connection_window}"
        )));
    }
    if g.transport_h2_adaptive_window {
        return Err(refuse(
            "transport_h2_adaptive_window must be off: an adaptive window has no configured bound",
        ));
    }
    // The supported root peak of one FE on one BE must fit the result lane's
    // actual stream positions; nothing waits in a hidden transport queue.
    let result_streams = mul(g.transport_connections_per_frontend_backend_result, streams)?;
    if result_streams < g.transport_worst_case_roots_per_frontend_per_backend {
        return Err(refuse(format!(
            "result lane carries {result_streams} streams per FE and BE, below the \
             supported {} roots",
            g.transport_worst_case_roots_per_frontend_per_backend
        )));
    }
    let backend_admission = AdmissionDimensions::backend(g)
        .map_err(|_| refuse("Backend connection admission overflows"))?;
    let frontend_admission = AdmissionDimensions::frontend(g)
        .map_err(|_| refuse("Frontend connection admission overflows"))?;
    let backend = envelope(TransportRole::Backend, g, &backend_admission)?;
    let frontend = envelope(TransportRole::Frontend, g, &frontend_admission)?;
    let backend_socket_positions = crate::native_fd_capacity::backend_socket_positions(g)?;
    let frontend_socket_positions = crate::native_fd_capacity::frontend_socket_positions(g)?;
    if backend_socket_positions > g.backend_minimum_open_file_limit {
        return Err(refuse(format!(
            "Backend Native sockets {backend_socket_positions} exceed the \
             {} descriptor baseline",
            g.backend_minimum_open_file_limit
        )));
    }
    if frontend_socket_positions > g.frontend_minimum_open_file_limit {
        return Err(refuse(format!(
            "Frontend Native sockets {frontend_socket_positions} exceed the \
             {} descriptor baseline",
            g.frontend_minimum_open_file_limit
        )));
    }
    Ok(NativeTransportGeometryReport {
        backend_admission,
        frontend_admission,
        backend,
        frontend,
        backend_socket_positions,
        frontend_socket_positions,
    })
}

/// Connections of one lane per peer, with the lane's connecting and closing
/// headroom.
fn per_peer_connections(g: &NativeResultSupportGeometry, lane: NativeLane) -> io::Result<u64> {
    let fe_tails = add(
        g.transport_connecting_positions_per_lane,
        g.transport_closing_positions_per_lane,
    )?;
    match lane {
        NativeLane::ResultData => add(
            g.transport_connections_per_frontend_backend_result,
            fe_tails,
        ),
        NativeLane::Submission => add(
            g.transport_connections_per_frontend_backend_submission,
            fe_tails,
        ),
        NativeLane::Observation => add(
            g.transport_connections_per_frontend_backend_observation,
            fe_tails,
        ),
        NativeLane::LifecycleControl => add(
            g.transport_connections_per_frontend_backend_lifecycle_control,
            fe_tails,
        ),
        NativeLane::Exchange => add(
            g.transport_exchange_connections_per_peer,
            add(
                g.transport_exchange_connecting_positions_per_peer,
                g.transport_exchange_closing_positions_per_peer,
            )?,
        ),
        NativeLane::RuntimeFilter => add(
            g.transport_runtime_filter_connections_per_peer,
            add(
                g.transport_runtime_filter_connecting_positions_per_peer,
                g.transport_runtime_filter_closing_positions_per_peer,
            )?,
        ),
        NativeLane::Membership => add(1, fe_tails),
    }
}

fn envelope(
    role: TransportRole,
    g: &NativeResultSupportGeometry,
    admission: &AdmissionDimensions,
) -> io::Result<NativeTransportEnvelope> {
    use NativeLane::*;
    use StreamDirection::*;
    let frontends = g.transport_authenticated_live_frontends_per_backend;
    let backends = g.transport_maximum_live_backends;
    // (lane, direction, peers): the peers a lane's connections can come from.
    let lanes: &[(NativeLane, StreamDirection, u64)] = match role {
        TransportRole::Backend => &[
            (ResultData, Incoming, frontends),
            (Submission, Incoming, frontends),
            (Observation, Incoming, frontends),
            (LifecycleControl, Incoming, frontends),
            (Exchange, Incoming, backends),
            (RuntimeFilter, Incoming, backends),
            (Exchange, Outgoing, backends),
            (RuntimeFilter, Outgoing, backends),
            (Membership, Outgoing, frontends),
        ],
        TransportRole::Frontend => &[
            (ResultData, Outgoing, backends),
            (Submission, Outgoing, backends),
            (Observation, Outgoing, backends),
            (LifecycleControl, Outgoing, backends),
            // The report listener serves every Backend's announcement.
            (Membership, Incoming, backends),
        ],
    };
    let streams = g.transport_streams_per_connection;
    let header = g.transport_h2_header_bytes;
    let mut out = Vec::with_capacity(lanes.len());
    let (mut connections, mut total_streams, mut structural, mut outgoing) = (0, 0, 0, 0);
    for &(lane, direction, peers) in lanes {
        let lane_connections = mul(peers, per_peer_connections(g, lane)?)?;
        let send_buffer = match direction {
            Incoming => g.transport_h2_send_buffer_bytes,
            Outgoing => client_defaults::SEND_BUFFER_BYTES_PER_STREAM,
        };
        let stream_bytes = add(
            add(g.transport_h2_stream_receive_window_bytes, send_buffer)?,
            header,
        )?;
        let per_connection = add(
            g.transport_h2_connection_receive_window_bytes,
            mul(streams, stream_bytes)?,
        )?;
        let structural_bytes = mul(lane_connections, per_connection)?;
        connections = add(connections, lane_connections)?;
        total_streams = add(total_streams, mul(lane_connections, streams)?)?;
        structural = add(structural, structural_bytes)?;
        if direction == Outgoing {
            outgoing = add(outgoing, lane_connections)?;
        }
        out.push(NativeLaneEnvelope {
            lane,
            direction,
            connections: lane_connections,
            streams_per_connection: streams,
            connection_window_bytes: g.transport_h2_connection_receive_window_bytes,
            stream_bytes,
            structural_bytes,
        });
    }
    let outgoing_handshake_positions = add(
        u64::try_from(admission.data_handshakes).map_err(|_| refuse("handshakes overflow"))?,
        u64::try_from(admission.control_handshakes).map_err(|_| refuse("handshakes overflow"))?,
    )?;
    let handshake_positions = if role == TransportRole::Frontend {
        add(
            outgoing_handshake_positions,
            u64::try_from(AdmissionDimensions::frontend_membership(g)?.1)
                .map_err(|_| refuse("membership handshakes overflow"))?,
        )?
    } else {
        outgoing_handshake_positions
    };
    Ok(NativeTransportEnvelope {
        role,
        lanes: out,
        connections,
        streams: total_streams,
        handshake_positions,
        queued_requests: mul(outgoing, g.transport_tonic_pending_per_connection)?,
        structural_bytes: structural,
        coefficients: FROZEN_TRANSPORT_COEFFICIENTS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(g: NativeResultSupportGeometry) -> String {
        let error = validate_native_transport_geometry(&g).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        error.to_string()
    }

    #[test]
    fn frozen_geometry_is_consistent_and_reports_its_count_envelope() {
        let report = validate_native_transport_geometry(&NativeResultSupportGeometry::V1).unwrap();
        assert_eq!(report.backend.coefficients, None);
        assert_eq!(report.backend.total_bytes().unwrap(), None);
        let result = report
            .frontend
            .lanes
            .iter()
            .find(|lane| lane.lane == NativeLane::ResultData)
            .unwrap();
        // 32 BEs * (4 live + 1 connecting + 1 closing).
        assert_eq!(result.connections, 192);
        assert_eq!(result.streams_per_connection, 128);
        // 256 KiB window + 1 MiB Hyper send buffer + 16 KiB header list.
        assert_eq!(result.stream_bytes, 262_144 + 1_048_576 + 16_384);
        assert_eq!(
            result.structural_bytes,
            192 * (1_048_576 + 128 * result.stream_bytes)
        );
        let incoming = report
            .backend
            .lanes
            .iter()
            .find(|lane| lane.lane == NativeLane::ResultData)
            .unwrap();
        assert_eq!(incoming.direction, StreamDirection::Incoming);
        assert_eq!(incoming.connections, 2 * 6);
        assert_eq!(incoming.stream_bytes, 262_144 + 65_536 + 16_384);
        assert_eq!(
            report.backend.structural_bytes,
            report
                .backend
                .lanes
                .iter()
                .map(|lane| lane.structural_bytes)
                .sum::<u64>()
        );
        assert_eq!(report.backend.handshake_positions, 40);
        assert_eq!(report.frontend.handshake_positions, 160);
        assert!(report.backend_socket_positions <= 1024);
        assert!(report.frontend_socket_positions <= 2048);
    }

    #[test]
    fn backend_root_encoding_uses_the_effective_wallet_and_original_ingress_limit() {
        use crate::native_transport_admission::root_result_ingress_owner_limit;

        let geometry = NativeResultSupportGeometry::V1;
        let report = validate_native_transport_geometry(&geometry).unwrap();
        let ingress = root_result_ingress_owner_limit(TransportRole::Backend, &geometry).unwrap();
        assert_eq!(
            ROOT_DATA_FRAME_MAX_BYTES as u64,
            geometry.root_segment_backing_capacity_bytes
        );
        let nominal = report
            .backend_root_encoding_report(&geometry, 4 << 30, ingress)
            .unwrap();
        let larger = report
            .backend_root_encoding_report(&geometry, 8 << 30, ingress)
            .unwrap();
        assert_eq!(nominal.root_process_limit_bytes, 4 << 30);
        assert_eq!(nominal.ingress_owner_limit, 45_184);
        assert_eq!(nominal.data_owner_limit, 8_192);
        assert_eq!(nominal.encoding_requested_bytes, 8_775_008_256);
        assert_eq!(larger.root_process_limit_bytes, 8 << 30);
        assert_eq!(larger.data_owner_limit, 16_384);
        assert_eq!(larger.encoding_requested_bytes, 17_364_942_848);
        assert_eq!(nominal.known_structural_bytes, 59_899_379_712);
        assert_eq!(
            larger.known_structural_bytes,
            report.backend.structural_bytes + larger.encoding_requested_bytes
        );

        let clipped = report
            .backend_root_encoding_report(&geometry, 4 << 30, 7)
            .unwrap();
        assert_eq!(clipped.data_owner_limit, 7);
        assert_eq!(
            clipped.encoding_requested_bytes,
            7 * ROOT_DATA_FRAME_MAX_BYTES as u64
        );
    }

    #[test]
    fn backend_root_encoding_diagnostic_rejects_overflow_or_zero_fixed_charge() {
        let mut geometry = NativeResultSupportGeometry::V1;
        let mut report = validate_native_transport_geometry(&geometry).unwrap();
        report.backend.structural_bytes = u64::MAX;
        assert!(
            report
                .backend_root_encoding_report(&geometry, 4 << 30, 1)
                .is_err()
        );
        geometry.root_fixed_schema_cursor_driver_capacity_bytes = 0;
        assert!(
            report
                .backend_root_encoding_report(&geometry, 4 << 30, 1)
                .is_err()
        );
    }

    #[test]
    fn coefficients_extend_the_structural_part_with_checked_arithmetic() {
        let mut envelope = validate_native_transport_geometry(&NativeResultSupportGeometry::V1)
            .unwrap()
            .frontend;
        envelope.coefficients = Some(NativeTransportCoefficients {
            per_connection_bytes: 1,
            per_stream_bytes: 1,
            per_handshake_bytes: 1,
            per_queued_request_bytes: 1,
        });
        assert_eq!(
            envelope.total_bytes().unwrap(),
            Some(
                envelope.structural_bytes
                    + envelope.connections
                    + envelope.streams
                    + envelope.handshake_positions
                    + envelope.queued_requests
            )
        );
        envelope.coefficients = Some(NativeTransportCoefficients {
            per_connection_bytes: u64::MAX,
            per_stream_bytes: 0,
            per_handshake_bytes: 0,
            per_queued_request_bytes: 0,
        });
        assert!(envelope.total_bytes().is_err());
    }

    #[test]
    fn geometry_overflow_is_refused_at_startup() {
        for mutate in [
            |g: &mut NativeResultSupportGeometry| g.transport_maximum_live_backends = u64::MAX,
            |g: &mut NativeResultSupportGeometry| {
                g.transport_connections_per_frontend_backend_result = u64::MAX
            },
            |g: &mut NativeResultSupportGeometry| g.transport_h2_send_buffer_bytes = u64::MAX,
            |g: &mut NativeResultSupportGeometry| {
                g.transport_exchange_closing_positions_per_peer = u64::MAX
            },
        ] {
            let mut g = NativeResultSupportGeometry::V1;
            mutate(&mut g);
            refused(g);
        }
    }

    type Mutation = fn(&mut NativeResultSupportGeometry);

    #[test]
    fn geometry_inconsistency_is_refused_at_startup() {
        let cases: [(Mutation, &str); 9] = [
            (|g| g.transport_streams_per_connection = 0, "streams"),
            (|g| g.transport_tonic_pending_per_connection = 0, "pending"),
            (
                |g| g.transport_connections_per_frontend_backend_observation = 0,
                "observation",
            ),
            (|g| g.transport_h2_frame_bytes = 1024, "frame"),
            (
                |g| g.transport_h2_stream_receive_window_bytes = 4 << 20,
                "stream_receive_window",
            ),
            (
                |g| g.transport_h2_connection_receive_window_bytes = 1 << 31,
                "connection_receive_window",
            ),
            (|g| g.transport_h2_adaptive_window = true, "adaptive"),
            (
                |g| g.transport_worst_case_roots_per_frontend_per_backend = 513,
                "roots",
            ),
            (|g| g.backend_minimum_open_file_limit = 256, "descriptor"),
        ];
        for (mutate, expected) in cases {
            let mut g = NativeResultSupportGeometry::V1;
            mutate(&mut g);
            let error = refused(g);
            assert!(error.contains(expected), "{expected}: {error}");
        }
    }
}
