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

//! Immutable advertised support geometry. These are declared capabilities,
//! not runtime grants. Hosts advertise V1 only after installing every matching
//! capacity/exit guard; an absent descriptor explicitly cannot serve V1 roots.

use crate::RuntimeEndpoint;
use novarocks_result_contract::RootProfileId;

/// Existing decoded Native message ceiling shared by host composition and
/// carrier-neutral execution. Full protobuf size is still checked at the wire
/// boundary; this is not an additional payload credit or memory budget.
pub const NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeResultSupportGeometry {
    pub frontend_client_compute_positions: u64,
    pub frontend_short_tail_positions: u64,
    pub frontend_client_window_positions: u64,
    pub frontend_closing_positions: u64,
    pub frontend_ordinary_connections: u64,
    pub frontend_control_connections: u64,
    pub frontend_local_positions: u64,
    pub frontend_internal_positions: u64,
    pub frontend_supported_cancel_burst: u64,
    pub frontend_sustained_cancels_per_second: u64,
    pub transport_authenticated_live_frontends_per_backend: u64,
    pub transport_worst_case_roots_per_frontend_per_backend: u64,
    pub transport_connections_per_frontend_backend_result: u64,
    pub transport_connections_per_frontend_backend_observation: u64,
    pub transport_connections_per_frontend_backend_submission: u64,
    pub transport_connections_per_frontend_backend_lifecycle_control: u64,
    pub transport_streams_per_connection: u64,
    pub transport_tonic_pending_per_connection: u64,
    pub transport_h2_frame_bytes: u64,
    pub transport_h2_header_bytes: u64,
    pub transport_h2_send_buffer_bytes: u64,
    pub transport_h2_adaptive_window: bool,
    pub transport_connect_deadline_ms: u64,
    pub transport_fetch_deadline_ms: u64,
    pub transport_short_tail_exit_deadline_ms: u64,
    pub transport_handshake_deadline_ms: u64,
    pub transport_maximum_live_backends: u64,
    pub transport_h2_connection_receive_window_bytes: u64,
    pub transport_h2_stream_receive_window_bytes: u64,
    pub transport_h2_pending_resets: u64,
    pub transport_connection_all_independent_backings_bytes: u64,
    pub transport_stream_bookkeeping_bytes: u64,
    pub transport_idle_decoder_bytes: u64,
    pub transport_header_raw_and_expanded_bytes: u64,
    pub transport_nonroot_producer_positions: u64,
    pub transport_nonroot_encode_positions: u64,
    pub transport_nonroot_decode_positions: u64,
    pub transport_nonroot_producer_expanded_bytes: u64,
    pub transport_nonroot_wire_actual_backing_bytes: u64,
    pub transport_nonroot_decoded_expanded_bytes: u64,
    pub transport_nonroot_wire_logical_bytes: u64,
    pub transport_nonroot_compression: bool,
    pub transport_frontend_native_additional_bytes: u64,
    pub transport_data_handshake_positions: u64,
    pub transport_control_handshake_positions: u64,
    pub transport_exchange_connections_per_peer: u64,
    pub transport_runtime_filter_connections_per_peer: u64,
    pub transport_exchange_inflight_per_peer: u64,
    pub transport_exchange_short_tails_per_peer: u64,
    pub transport_runtime_filter_inflight_per_peer: u64,
    pub transport_runtime_filter_short_tails_per_peer: u64,
    pub root_joint_retained_bytes_per_root: u64,
    pub root_joint_retained_bytes_per_process: u64,
    pub root_maximum_root_drivers: u64,
    pub root_shared_input_positions: u64,
    pub root_shared_encoding_cursors: u64,
    pub root_original_input_backing_capacity_bytes: u64,
    pub root_additional_hydrate_backing_capacity_bytes: u64,
    pub root_scratch_capacity_bytes: u64,
    pub root_small_row_staging_bytes: u64,
    pub root_active_segment_positions: u64,
    pub root_queued_segment_positions: u64,
    pub root_terminal_positions: u64,
    pub root_segment_backing_capacity_bytes: u64,
    pub root_live_send_holders: u64,
    pub root_independent_payload_copies_per_send: u64,
    pub root_fixed_schema_cursor_driver_capacity_bytes: u64,
    pub root_actual_exit_deadline_ms: u64,
    pub root_retired_segment_tail_positions: u64,
    pub frontend_window_all_objects_bytes: u64,
    pub frontend_closing_all_objects_bytes: u64,
    pub frontend_minimum_open_file_limit: u64,
    pub backend_minimum_open_file_limit: u64,
    pub transport_connecting_positions_per_lane: u64,
    pub transport_closing_positions_per_lane: u64,
    pub transport_exchange_connecting_positions_per_peer: u64,
    pub transport_exchange_closing_positions_per_peer: u64,
    pub transport_runtime_filter_connecting_positions_per_peer: u64,
    pub transport_runtime_filter_closing_positions_per_peer: u64,
}
impl NativeResultSupportGeometry {
    pub const V1: Self = Self {
        frontend_client_compute_positions: 256,
        frontend_short_tail_positions: 64,
        frontend_client_window_positions: 320,
        frontend_closing_positions: 64,
        frontend_ordinary_connections: 512,
        frontend_control_connections: 32,
        frontend_local_positions: 16,
        frontend_internal_positions: 4,
        frontend_supported_cancel_burst: 32,
        frontend_sustained_cancels_per_second: 16,
        transport_authenticated_live_frontends_per_backend: 2,
        transport_worst_case_roots_per_frontend_per_backend: 320,
        transport_connections_per_frontend_backend_result: 4,
        transport_connections_per_frontend_backend_observation: 4,
        transport_connections_per_frontend_backend_submission: 2,
        transport_connections_per_frontend_backend_lifecycle_control: 1,
        transport_streams_per_connection: 128,
        transport_tonic_pending_per_connection: 8,
        transport_h2_frame_bytes: 16384,
        transport_h2_header_bytes: 16384,
        transport_h2_send_buffer_bytes: 65536,
        transport_h2_adaptive_window: false,
        transport_connect_deadline_ms: 2000,
        transport_fetch_deadline_ms: 1000,
        transport_short_tail_exit_deadline_ms: 2000,
        transport_handshake_deadline_ms: 2000,
        transport_maximum_live_backends: 32,
        transport_h2_connection_receive_window_bytes: 1048576,
        transport_h2_stream_receive_window_bytes: 262144,
        transport_h2_pending_resets: 32,
        transport_connection_all_independent_backings_bytes: 2097152,
        transport_stream_bookkeeping_bytes: 4096,
        transport_idle_decoder_bytes: 8192,
        transport_header_raw_and_expanded_bytes: 32768,
        transport_nonroot_producer_positions: 64,
        transport_nonroot_encode_positions: 64,
        transport_nonroot_decode_positions: 64,
        transport_nonroot_producer_expanded_bytes: 4194304,
        transport_nonroot_wire_actual_backing_bytes: 2097152,
        transport_nonroot_decoded_expanded_bytes: 4194304,
        transport_nonroot_wire_logical_bytes: 1052672,
        transport_nonroot_compression: false,
        transport_frontend_native_additional_bytes: 3221225472,
        transport_data_handshake_positions: 32,
        transport_control_handshake_positions: 8,
        transport_exchange_connections_per_peer: 2,
        transport_runtime_filter_connections_per_peer: 1,
        transport_exchange_inflight_per_peer: 32,
        transport_exchange_short_tails_per_peer: 32,
        transport_runtime_filter_inflight_per_peer: 8,
        transport_runtime_filter_short_tails_per_peer: 8,
        root_joint_retained_bytes_per_root: 268435456,
        root_joint_retained_bytes_per_process: 4294967296,
        root_maximum_root_drivers: 64,
        root_shared_input_positions: 1,
        root_shared_encoding_cursors: 1,
        root_original_input_backing_capacity_bytes: 100663296,
        root_additional_hydrate_backing_capacity_bytes: 100663296,
        root_scratch_capacity_bytes: 2097152,
        root_small_row_staging_bytes: 65536,
        root_active_segment_positions: 1,
        root_queued_segment_positions: 2,
        root_terminal_positions: 1,
        root_segment_backing_capacity_bytes: 1052672,
        root_live_send_holders: 2,
        root_independent_payload_copies_per_send: 2,
        root_fixed_schema_cursor_driver_capacity_bytes: 1048576,
        root_actual_exit_deadline_ms: 2000,
        root_retired_segment_tail_positions: 2,
        frontend_window_all_objects_bytes: 8388608,
        frontend_closing_all_objects_bytes: 8388608,
        frontend_minimum_open_file_limit: 2048,
        backend_minimum_open_file_limit: 1024,
        transport_connecting_positions_per_lane: 1,
        transport_closing_positions_per_lane: 1,
        transport_exchange_connecting_positions_per_peer: 1,
        transport_exchange_closing_positions_per_peer: 1,
        transport_runtime_filter_connecting_positions_per_peer: 1,
        transport_runtime_filter_closing_positions_per_peer: 1,
    };
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedRootSupport {
    control_endpoint: RuntimeEndpoint,
    profile: RootProfileId,
}
impl BoundedRootSupport {
    pub const fn new(control_endpoint: RuntimeEndpoint, profile: RootProfileId) -> Self {
        Self {
            control_endpoint,
            profile,
        }
    }
    pub fn control_endpoint(&self) -> &RuntimeEndpoint {
        &self.control_endpoint
    }
    pub const fn profile(&self) -> RootProfileId {
        self.profile
    }
    pub const fn geometry(&self) -> NativeResultSupportGeometry {
        NativeResultSupportGeometry::V1
    }
}
