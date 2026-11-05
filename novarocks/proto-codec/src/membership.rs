//! Validated native backend membership wire values.
//!
//! The generated protobuf messages remain the stored representation. This
//! module is the sole structural validation boundary for process identity,
//! descriptor, announce, and heartbeat membership carriers.

use crate::lifecycle::QueryControlEndpoint;
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_execution_contract::{
    BackendProcessDescriptor as ContractBackendProcessDescriptor, RuntimeEndpoint,
};
use novarocks_proto_models::novarocks;
use novarocks_types::{
    BackendProcessId as DomainBackendProcessId, BackendProcessIdentityError, NativeCompatibilityId,
};

const MAX_SAFE_DETAIL_BYTES: usize = 512;

/// Validated generated backend process identity.
#[derive(Clone, Debug, PartialEq)]
pub struct BackendProcessId {
    raw: novarocks::BackendProcessId,
}

impl BackendProcessId {
    pub fn from_domain(value: DomainBackendProcessId) -> Self {
        Self {
            raw: novarocks::BackendProcessId {
                value: value.to_bytes().to_vec(),
            },
        }
    }

    pub fn parse(raw: novarocks::BackendProcessId) -> Result<Self, ProtocolError> {
        domain_process_id(&raw.value)?;
        Ok(Self { raw })
    }

    pub fn domain(&self) -> Result<DomainBackendProcessId, ProtocolError> {
        domain_process_id(&self.raw.value)
    }

    pub const fn as_proto(&self) -> &novarocks::BackendProcessId {
        &self.raw
    }
}

/// Transport-neutral backend state validated from a generated representation.
pub use novarocks_execution_contract::BackendReportedState;

/// Validated generated backend process descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct BackendProcessDescriptor {
    raw: novarocks::BackendProcessDescriptor,
}

impl BackendProcessDescriptor {
    pub fn new(
        process_id: DomainBackendProcessId,
        endpoint: QueryControlEndpoint,
        control_endpoint: QueryControlEndpoint,
        deployment_id: impl Into<String>,
        build_identity: impl Into<String>,
        native_compatibility_id: NativeCompatibilityId,
        preparing_positions: usize,
    ) -> Result<Self, ProtocolError> {
        let endpoint =
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port())).map_err(|error| {
                invalid(
                    FieldPath::root("backend_process_descriptor").field("endpoint"),
                    format!("backend process endpoint is invalid: {error}"),
                )
            })?;
        let control_endpoint =
            RuntimeEndpoint::new(control_endpoint.host(), i32::from(control_endpoint.port()))
                .map_err(|error| {
                    invalid(
                        FieldPath::root("backend_process_descriptor").field("control_endpoint"),
                        format!("backend control endpoint is invalid: {error}"),
                    )
                })?;
        ContractBackendProcessDescriptor::try_new(
            process_id,
            endpoint,
            control_endpoint,
            deployment_id,
            build_identity,
            native_compatibility_id,
            preparing_positions,
        )
        .map(Self::from_contract)
        .map_err(|error| {
            invalid(
                FieldPath::root("backend_process_descriptor"),
                error.to_string(),
            )
        })
    }

    pub fn parse(raw: novarocks::BackendProcessDescriptor) -> Result<Self, ProtocolError> {
        let process_id = required_process_id(
            &raw.process_id,
            FieldPath::root("backend_process_descriptor").field("process_id"),
        )?;
        let endpoint = raw.endpoint.clone().ok_or_else(|| {
            missing(
                FieldPath::root("backend_process_descriptor").field("endpoint"),
                "backend process endpoint is required",
            )
        })?;
        let endpoint = QueryControlEndpoint::parse(endpoint).map_err(|error| {
            prefix_path(
                FieldPath::root("backend_process_descriptor").field("endpoint"),
                error,
            )
        })?;
        let native_compatibility_id = required_native_compatibility_id(
            &raw.native_compatibility_id,
            FieldPath::root("backend_process_descriptor").field("native_compatibility_id"),
        )?;
        let control_endpoint = raw.control_endpoint.clone().ok_or_else(|| {
            missing(
                FieldPath::root("backend_process_descriptor").field("control_endpoint"),
                "backend control endpoint is required",
            )
        })?;
        let control_endpoint = QueryControlEndpoint::parse(control_endpoint).map_err(|error| {
            prefix_path(
                FieldPath::root("backend_process_descriptor").field("control_endpoint"),
                error,
            )
        })?;
        let control_endpoint =
            RuntimeEndpoint::new(control_endpoint.host(), i32::from(control_endpoint.port()))
                .map_err(|error| {
                    invalid(
                        FieldPath::root("backend_process_descriptor").field("control_endpoint"),
                        format!("backend control endpoint is invalid: {error}"),
                    )
                })?;
        let endpoint =
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port())).map_err(|error| {
                invalid(
                    FieldPath::root("backend_process_descriptor").field("endpoint"),
                    format!("backend process endpoint is invalid: {error}"),
                )
            })?;
        let preparing_positions = usize::try_from(raw.preparing_positions).map_err(|_| {
            invalid(
                FieldPath::root("backend_process_descriptor").field("preparing_positions"),
                "backend preparation capacity cannot be represented on this host",
            )
        })?;
        let support = raw
            .bounded_root_support
            .as_ref()
            .map(|support| {
                decode_bounded_root_support(
                    support,
                    FieldPath::root("backend_process_descriptor").field("bounded_root_support"),
                )
            })
            .transpose()?;
        let descriptor = ContractBackendProcessDescriptor::try_new(
            process_id,
            endpoint,
            control_endpoint,
            raw.deployment_id,
            raw.build_identity,
            native_compatibility_id,
            preparing_positions,
        )
        .map_err(|error| {
            invalid(
                FieldPath::root("backend_process_descriptor"),
                error.to_string(),
            )
        })?;
        let descriptor = match support {
            Some(support) => descriptor
                .with_bounded_root_support(support)
                .map_err(|error| {
                    invalid(
                        FieldPath::root("backend_process_descriptor").field("bounded_root_support"),
                        error.to_string(),
                    )
                })?,
            None => descriptor,
        };
        Ok(Self::from_contract(descriptor))
    }

    pub fn from_contract(descriptor: ContractBackendProcessDescriptor) -> Self {
        Self {
            raw: novarocks::BackendProcessDescriptor {
                process_id: Some(BackendProcessId::from_domain(descriptor.process_id()).raw),
                endpoint: Some(
                    QueryControlEndpoint::new(
                        descriptor.endpoint().host(),
                        descriptor.endpoint().port() as u16,
                    )
                    .expect("contract endpoint retains a valid u16 port")
                    .as_proto()
                    .clone(),
                ),
                preparing_positions: descriptor.preparing_positions() as u64,
                control_endpoint: Some(
                    QueryControlEndpoint::new(
                        descriptor.control_endpoint().host(),
                        descriptor.control_endpoint().port() as u16,
                    )
                    .expect("contract control endpoint retains a valid u16 port")
                    .as_proto()
                    .clone(),
                ),
                bounded_root_support: descriptor
                    .bounded_root_support()
                    .map(encode_bounded_root_support),
                deployment_id: descriptor.deployment_id().to_string(),
                build_identity: descriptor.build_identity().to_string(),
                native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                    value: descriptor.native_compatibility_id().as_bytes().to_vec(),
                }),
            },
        }
    }

    pub fn to_contract(&self) -> Result<ContractBackendProcessDescriptor, ProtocolError> {
        let validated = Self::parse(self.raw.clone())?;
        let endpoint = validated.endpoint()?;
        let control_endpoint = validated.control_endpoint()?;
        let descriptor = ContractBackendProcessDescriptor::try_new(
            validated.process_id()?,
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port()))
                .expect("validated endpoint retains host/port"),
            RuntimeEndpoint::new(control_endpoint.host(), i32::from(control_endpoint.port()))
                .expect("validated control endpoint retains host/port"),
            validated.deployment_id(),
            validated.build_identity(),
            validated.native_compatibility_id()?,
            usize::try_from(validated.raw.preparing_positions)
                .expect("validated capacity fits host"),
        )
        .expect("validated base descriptor retains its invariants");
        match validated.raw.bounded_root_support.as_ref() {
            None => Ok(descriptor),
            Some(support) => descriptor
                .with_bounded_root_support(decode_bounded_root_support(
                    support,
                    FieldPath::root("backend_process_descriptor").field("bounded_root_support"),
                )?)
                .map_err(|error| {
                    invalid(
                        FieldPath::root("backend_process_descriptor"),
                        error.to_string(),
                    )
                }),
        }
    }

    pub const fn as_proto(&self) -> &novarocks::BackendProcessDescriptor {
        &self.raw
    }

    pub fn process_id(&self) -> Result<DomainBackendProcessId, ProtocolError> {
        required_process_id(
            &self.raw.process_id,
            FieldPath::root("backend_process_descriptor").field("process_id"),
        )
    }

    pub fn endpoint(&self) -> Result<QueryControlEndpoint, ProtocolError> {
        let endpoint = self.raw.endpoint.clone().ok_or_else(|| {
            missing(
                FieldPath::root("backend_process_descriptor").field("endpoint"),
                "backend process endpoint is required",
            )
        })?;
        QueryControlEndpoint::parse(endpoint)
    }

    pub fn control_endpoint(&self) -> Result<QueryControlEndpoint, ProtocolError> {
        let endpoint = self.raw.control_endpoint.clone().ok_or_else(|| {
            missing(
                FieldPath::root("backend_process_descriptor").field("control_endpoint"),
                "backend control endpoint is required",
            )
        })?;
        QueryControlEndpoint::parse(endpoint)
    }

    pub fn deployment_id(&self) -> &str {
        &self.raw.deployment_id
    }

    pub fn build_identity(&self) -> &str {
        &self.raw.build_identity
    }

    pub fn native_compatibility_id(&self) -> Result<NativeCompatibilityId, ProtocolError> {
        required_native_compatibility_id(
            &self.raw.native_compatibility_id,
            FieldPath::root("backend_process_descriptor").field("native_compatibility_id"),
        )
    }
}

/// Validated announce request.
#[derive(Clone, Debug, PartialEq)]
pub struct BackendAnnounceRequest {
    raw: novarocks::AnnounceBackendRequest,
}

impl BackendAnnounceRequest {
    pub fn new(
        descriptor: BackendProcessDescriptor,
        reported_state: BackendReportedState,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::AnnounceBackendRequest {
            descriptor: Some(descriptor.as_proto().clone()),
            reported_state: reported_state_to_proto(reported_state),
        })
    }

    pub fn parse(raw: novarocks::AnnounceBackendRequest) -> Result<Self, ProtocolError> {
        let descriptor = raw.descriptor.clone().ok_or_else(|| {
            missing(
                FieldPath::root("announce_backend_request").field("descriptor"),
                "backend descriptor is required",
            )
        })?;
        BackendProcessDescriptor::parse(descriptor)?;
        parse_reported_state(raw.reported_state)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::AnnounceBackendRequest {
        &self.raw
    }

    pub fn descriptor(&self) -> Result<BackendProcessDescriptor, ProtocolError> {
        let descriptor = self.raw.descriptor.clone().ok_or_else(|| {
            missing(
                FieldPath::root("announce_backend_request").field("descriptor"),
                "backend descriptor is required",
            )
        })?;
        BackendProcessDescriptor::parse(descriptor)
    }

    pub fn reported_state(&self) -> Result<BackendReportedState, ProtocolError> {
        parse_reported_state(self.raw.reported_state)
    }
}

/// Closed announce rejection reason set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendAnnounceRejectionReason {
    DescriptorConflict,
    DeploymentMismatch,
}

impl BackendAnnounceRejectionReason {
    fn parse(raw: i32) -> Result<Self, ProtocolError> {
        match novarocks::BackendAnnounceRejectionReason::try_from(raw) {
            Ok(novarocks::BackendAnnounceRejectionReason::BackendAnnounceRejectionDescriptorConflict) => {
                Ok(Self::DescriptorConflict)
            }
            Ok(novarocks::BackendAnnounceRejectionReason::BackendAnnounceRejectionDeploymentMismatch) => {
                Ok(Self::DeploymentMismatch)
            }
            Ok(novarocks::BackendAnnounceRejectionReason::Unspecified) | Err(_) => Err(invalid(
                FieldPath::root("announce_backend_response")
                    .field("rejected")
                    .field("reason"),
                "unknown or unspecified backend announce rejection reason",
            )),
        }
    }

    fn as_proto(self) -> i32 {
        (match self {
            Self::DescriptorConflict => {
                novarocks::BackendAnnounceRejectionReason::BackendAnnounceRejectionDescriptorConflict
            }
            Self::DeploymentMismatch => {
                novarocks::BackendAnnounceRejectionReason::BackendAnnounceRejectionDeploymentMismatch
            }
        }) as i32
    }
}

/// Validated announce result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendAnnounceResult {
    Accepted {
        lease_ttl_ms: u64,
    },
    Rejected {
        reason: BackendAnnounceRejectionReason,
        safe_detail: String,
    },
}

impl BackendAnnounceResult {
    pub fn accepted(lease_ttl_ms: u64) -> Result<Self, ProtocolError> {
        Self::from_proto(novarocks::AnnounceBackendResponse {
            outcome: Some(novarocks::announce_backend_response::Outcome::Accepted(
                novarocks::BackendAnnounceLease { lease_ttl_ms },
            )),
        })
    }

    pub fn rejected(
        reason: BackendAnnounceRejectionReason,
        safe_detail: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::from_proto(novarocks::AnnounceBackendResponse {
            outcome: Some(novarocks::announce_backend_response::Outcome::Rejected(
                novarocks::BackendAnnounceRejected {
                    reason: reason.as_proto(),
                    safe_detail: safe_detail.into(),
                },
            )),
        })
    }

    pub fn from_proto(raw: novarocks::AnnounceBackendResponse) -> Result<Self, ProtocolError> {
        use novarocks::announce_backend_response::Outcome;

        match raw.outcome {
            Some(Outcome::Accepted(lease)) if lease.lease_ttl_ms > 0 => Ok(Self::Accepted {
                lease_ttl_ms: lease.lease_ttl_ms,
            }),
            Some(Outcome::Accepted(_)) => Err(invalid(
                FieldPath::root("announce_backend_response")
                    .field("accepted")
                    .field("lease_ttl_ms"),
                "backend announce lease ttl must be nonzero",
            )),
            Some(Outcome::Rejected(rejected)) => {
                let reason = BackendAnnounceRejectionReason::parse(rejected.reason)?;
                bounded_text(
                    &rejected.safe_detail,
                    MAX_SAFE_DETAIL_BYTES,
                    FieldPath::root("announce_backend_response")
                        .field("rejected")
                        .field("safe_detail"),
                    "safe detail",
                )?;
                Ok(Self::Rejected {
                    reason,
                    safe_detail: rejected.safe_detail,
                })
            }
            None => Err(missing(
                FieldPath::root("announce_backend_response").field("outcome"),
                "backend announce outcome is required",
            )),
        }
    }

    pub fn to_proto(&self) -> novarocks::AnnounceBackendResponse {
        use novarocks::announce_backend_response::Outcome;

        let outcome = match self {
            Self::Accepted { lease_ttl_ms } => Outcome::Accepted(novarocks::BackendAnnounceLease {
                lease_ttl_ms: *lease_ttl_ms,
            }),
            Self::Rejected {
                reason,
                safe_detail,
            } => Outcome::Rejected(novarocks::BackendAnnounceRejected {
                reason: reason.as_proto(),
                safe_detail: safe_detail.clone(),
            }),
        };
        novarocks::AnnounceBackendResponse {
            outcome: Some(outcome),
        }
    }
}

pub fn parse_reported_state(raw: i32) -> Result<BackendReportedState, ProtocolError> {
    match novarocks::BackendReportedState::try_from(raw) {
        Ok(novarocks::BackendReportedState::Running) => Ok(BackendReportedState::Running),
        Ok(novarocks::BackendReportedState::Draining) => Ok(BackendReportedState::Draining),
        Ok(novarocks::BackendReportedState::Unspecified) | Err(_) => Err(invalid(
            FieldPath::root("backend_reported_state"),
            "backend reported state must be running or draining",
        )),
    }
}

fn reported_state_to_proto(state: BackendReportedState) -> i32 {
    match state {
        BackendReportedState::Running => novarocks::BackendReportedState::Running as i32,
        BackendReportedState::Draining => novarocks::BackendReportedState::Draining as i32,
    }
}

fn required_process_id(
    raw: &Option<novarocks::BackendProcessId>,
    path: FieldPath,
) -> Result<DomainBackendProcessId, ProtocolError> {
    let raw = raw
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "backend process id is required"))?;
    BackendProcessId::parse(raw.clone())?
        .domain()
        .map_err(|error| ProtocolError::new(path, error.kind().clone(), error.detail().to_owned()))
}

pub(crate) fn required_native_compatibility_id(
    raw: &Option<novarocks::NativeCompatibilityId>,
    path: FieldPath,
) -> Result<NativeCompatibilityId, ProtocolError> {
    let raw = raw
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "native compatibility id is required"))?;
    NativeCompatibilityId::try_from_slice(&raw.value).map_err(|error| {
        invalid(
            path.field("value"),
            format!("native compatibility id must contain exactly 32 bytes: {error}"),
        )
    })
}

fn domain_process_id(raw: &[u8]) -> Result<DomainBackendProcessId, ProtocolError> {
    let value: [u8; 16] = raw.try_into().map_err(|_| {
        invalid(
            FieldPath::root("backend_process_id").field("value"),
            "backend process id must contain exactly 16 bytes",
        )
    })?;
    DomainBackendProcessId::try_from_bytes(value).map_err(|error| {
        invalid(
            FieldPath::root("backend_process_id").field("value"),
            match error {
                BackendProcessIdentityError::Nil => "backend process id must not be nil",
                BackendProcessIdentityError::NotUuidV7 => "backend process id must be UUIDv7",
            },
        )
    })
}

fn bounded_text(
    value: &str,
    maximum_bytes: usize,
    path: FieldPath,
    name: &'static str,
) -> Result<(), ProtocolError> {
    if value.trim().is_empty() {
        return Err(invalid(path, format!("{name} must not be empty")));
    }
    if value.len() > maximum_bytes {
        return Err(invalid(
            path,
            format!("{name} exceeds {maximum_bytes} bytes"),
        ));
    }
    Ok(())
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

fn prefix_path(prefix: FieldPath, error: ProtocolError) -> ProtocolError {
    ProtocolError::new(
        prefix.append_segments(error.path().segments().iter().skip(1).cloned()),
        error.kind().clone(),
        error.detail().to_owned(),
    )
}

fn encode_bounded_root_support(
    value: &novarocks_execution_contract::native_result_support::BoundedRootSupport,
) -> novarocks::BoundedRootSupport {
    novarocks::BoundedRootSupport {
        control_endpoint: Some(
            QueryControlEndpoint::new(
                value.control_endpoint().host(),
                value.control_endpoint().port() as u16,
            )
            .expect("validated control endpoint")
            .as_proto()
            .clone(),
        ),
        profile_id: value.profile().get(),
        geometry: Some(encode_support_geometry(value.geometry())),
    }
}
fn decode_bounded_root_support(
    value: &novarocks::BoundedRootSupport,
    path: FieldPath,
) -> Result<novarocks_execution_contract::native_result_support::BoundedRootSupport, ProtocolError>
{
    let profile =
        crate::root_result::decode_profile(value.profile_id, path.clone().field("profile_id"))?;
    let geometry = value.geometry.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("geometry"),
            "complete Native support geometry is required",
        )
    })?;
    if geometry
        != &encode_support_geometry(
            novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1,
        )
    {
        return Err(invalid(
            path.clone().field("geometry"),
            "Native support geometry does not match the frozen profile",
        ));
    }
    let control =
        QueryControlEndpoint::parse(value.control_endpoint.clone().ok_or_else(|| {
            missing(
                path.clone().field("control_endpoint"),
                "independent control endpoint is required",
            )
        })?)?;
    let control = RuntimeEndpoint::new(control.host(), i32::from(control.port()))
        .map_err(|error| invalid(path.field("control_endpoint"), error.to_string()))?;
    Ok(
        novarocks_execution_contract::native_result_support::BoundedRootSupport::new(
            control, profile,
        ),
    )
}
fn encode_support_geometry(
    value: novarocks_execution_contract::native_result_support::NativeResultSupportGeometry,
) -> novarocks::NativeResultSupportGeometry {
    novarocks::NativeResultSupportGeometry {
        frontend_client_compute_positions: value.frontend_client_compute_positions,
        frontend_short_tail_positions: value.frontend_short_tail_positions,
        frontend_client_window_positions: value.frontend_client_window_positions,
        frontend_local_positions: value.frontend_local_positions,
        frontend_internal_positions: value.frontend_internal_positions,
        frontend_supported_cancel_burst: value.frontend_supported_cancel_burst,
        frontend_sustained_cancels_per_second: value.frontend_sustained_cancels_per_second,
        transport_authenticated_live_frontends_per_backend: value
            .transport_authenticated_live_frontends_per_backend,
        transport_worst_case_roots_per_frontend_per_backend: value
            .transport_worst_case_roots_per_frontend_per_backend,
        transport_connections_per_frontend_backend_result: value
            .transport_connections_per_frontend_backend_result,
        transport_connections_per_frontend_backend_observation: value
            .transport_connections_per_frontend_backend_observation,
        transport_connections_per_frontend_backend_submission: value
            .transport_connections_per_frontend_backend_submission,
        transport_connections_per_frontend_backend_lifecycle_control: value
            .transport_connections_per_frontend_backend_lifecycle_control,
        transport_streams_per_connection: value.transport_streams_per_connection,
        transport_tonic_pending_per_connection: value.transport_tonic_pending_per_connection,
        transport_h2_frame_bytes: value.transport_h2_frame_bytes,
        transport_h2_header_bytes: value.transport_h2_header_bytes,
        transport_h2_send_buffer_bytes: value.transport_h2_send_buffer_bytes,
        transport_h2_adaptive_window: value.transport_h2_adaptive_window,
        transport_connect_deadline_ms: value.transport_connect_deadline_ms,
        transport_fetch_deadline_ms: value.transport_fetch_deadline_ms,
        transport_short_tail_exit_deadline_ms: value.transport_short_tail_exit_deadline_ms,
        transport_handshake_deadline_ms: value.transport_handshake_deadline_ms,
        transport_maximum_live_backends: value.transport_maximum_live_backends,
        transport_h2_connection_receive_window_bytes: value
            .transport_h2_connection_receive_window_bytes,
        transport_h2_stream_receive_window_bytes: value.transport_h2_stream_receive_window_bytes,
        transport_h2_pending_resets: value.transport_h2_pending_resets,
        transport_connection_all_independent_backings_bytes: value
            .transport_connection_all_independent_backings_bytes,
        transport_stream_bookkeeping_bytes: value.transport_stream_bookkeeping_bytes,
        transport_idle_decoder_bytes: value.transport_idle_decoder_bytes,
        transport_header_raw_and_expanded_bytes: value.transport_header_raw_and_expanded_bytes,
        transport_nonroot_producer_positions: value.transport_nonroot_producer_positions,
        transport_nonroot_encode_positions: value.transport_nonroot_encode_positions,
        transport_nonroot_decode_positions: value.transport_nonroot_decode_positions,
        transport_nonroot_producer_expanded_bytes: value.transport_nonroot_producer_expanded_bytes,
        transport_nonroot_wire_actual_backing_bytes: value
            .transport_nonroot_wire_actual_backing_bytes,
        transport_nonroot_decoded_expanded_bytes: value.transport_nonroot_decoded_expanded_bytes,
        transport_nonroot_wire_logical_bytes: value.transport_nonroot_wire_logical_bytes,
        transport_nonroot_compression: value.transport_nonroot_compression,
        transport_frontend_native_additional_bytes: value
            .transport_frontend_native_additional_bytes,
        transport_data_handshake_positions: value.transport_data_handshake_positions,
        transport_control_handshake_positions: value.transport_control_handshake_positions,
        transport_exchange_connections_per_peer: value.transport_exchange_connections_per_peer,
        transport_runtime_filter_connections_per_peer: value
            .transport_runtime_filter_connections_per_peer,
        transport_exchange_inflight_per_peer: value.transport_exchange_inflight_per_peer,
        transport_exchange_short_tails_per_peer: value.transport_exchange_short_tails_per_peer,
        transport_runtime_filter_inflight_per_peer: value
            .transport_runtime_filter_inflight_per_peer,
        transport_runtime_filter_short_tails_per_peer: value
            .transport_runtime_filter_short_tails_per_peer,
        root_joint_retained_bytes_per_root: value.root_joint_retained_bytes_per_root,
        root_joint_retained_bytes_per_process: value.root_joint_retained_bytes_per_process,
        root_maximum_root_drivers: value.root_maximum_root_drivers,
        root_shared_input_positions: value.root_shared_input_positions,
        root_shared_encoding_cursors: value.root_shared_encoding_cursors,
        root_original_input_backing_capacity_bytes: value
            .root_original_input_backing_capacity_bytes,
        root_additional_hydrate_backing_capacity_bytes: value
            .root_additional_hydrate_backing_capacity_bytes,
        root_scratch_capacity_bytes: value.root_scratch_capacity_bytes,
        root_small_row_staging_bytes: value.root_small_row_staging_bytes,
        root_active_segment_positions: value.root_active_segment_positions,
        root_queued_segment_positions: value.root_queued_segment_positions,
        root_terminal_positions: value.root_terminal_positions,
        root_segment_backing_capacity_bytes: value.root_segment_backing_capacity_bytes,
        root_live_send_holders: value.root_live_send_holders,
        root_independent_payload_copies_per_send: value.root_independent_payload_copies_per_send,
        root_fixed_schema_cursor_driver_capacity_bytes: value
            .root_fixed_schema_cursor_driver_capacity_bytes,
        root_actual_exit_deadline_ms: value.root_actual_exit_deadline_ms,
        root_retired_segment_tail_positions: value.root_retired_segment_tail_positions,
        frontend_window_all_objects_bytes: value.frontend_window_all_objects_bytes,
        frontend_minimum_open_file_limit: value.frontend_minimum_open_file_limit,
        backend_minimum_open_file_limit: value.backend_minimum_open_file_limit,
        transport_connecting_positions_per_lane: value.transport_connecting_positions_per_lane,
        transport_closing_positions_per_lane: value.transport_closing_positions_per_lane,
        transport_exchange_connecting_positions_per_peer: value
            .transport_exchange_connecting_positions_per_peer,
        transport_exchange_closing_positions_per_peer: value
            .transport_exchange_closing_positions_per_peer,
        transport_runtime_filter_connecting_positions_per_peer: value
            .transport_runtime_filter_connecting_positions_per_peer,
        transport_runtime_filter_closing_positions_per_peer: value
            .transport_runtime_filter_closing_positions_per_peer,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackendAnnounceRequest, BackendAnnounceResult, BackendProcessDescriptor, BackendProcessId,
        BackendReportedState,
    };
    use crate::lifecycle::QueryControlEndpoint;
    use novarocks_proto_models::novarocks;
    use novarocks_types::{BackendProcessId as DomainBackendProcessId, NativeCompatibilityId};

    fn descriptor() -> BackendProcessDescriptor {
        BackendProcessDescriptor::new(
            DomainBackendProcessId::new_v7(),
            QueryControlEndpoint::new("be-0.internal", 9090).expect("endpoint"),
            QueryControlEndpoint::new("be-0.internal", 9091).expect("control endpoint"),
            "warehouse-a",
            "build-identity",
            NativeCompatibilityId::new([7; 32]),
            4096,
        )
        .expect("descriptor")
    }

    #[test]
    fn process_id_requires_exact_non_nil_uuid_v7() {
        assert!(
            BackendProcessId::parse(novarocks::BackendProcessId { value: vec![1; 15] }).is_err()
        );
        assert!(
            BackendProcessId::parse(novarocks::BackendProcessId { value: vec![0; 16] }).is_err()
        );
        assert!(
            BackendProcessId::parse(novarocks::BackendProcessId {
                value: uuid::Uuid::new_v4().into_bytes().to_vec()
            })
            .is_err()
        );
        assert!(
            BackendProcessId::parse(
                BackendProcessId::from_domain(DomainBackendProcessId::new_v7())
                    .as_proto()
                    .clone()
            )
            .is_ok()
        );
    }

    #[test]
    fn announce_requires_descriptor_state_and_closed_outcome() {
        let request = BackendAnnounceRequest::new(descriptor(), BackendReportedState::Running)
            .expect("announce request");
        assert_eq!(
            request.reported_state().expect("state"),
            BackendReportedState::Running
        );
        assert!(BackendAnnounceResult::accepted(0).is_err());
        assert!(BackendAnnounceResult::accepted(1).is_ok());
    }

    #[test]
    fn descriptor_requires_an_exact_width_native_compatibility_id() {
        let mut missing = descriptor().as_proto().clone();
        missing.native_compatibility_id = None;
        let error = BackendProcessDescriptor::parse(missing).expect_err("missing id rejects");
        assert_eq!(error.kind().clone(), crate::ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "backend_process_descriptor.native_compatibility_id"
        );

        for width in [31, 33] {
            let mut malformed = descriptor().as_proto().clone();
            malformed.native_compatibility_id = Some(novarocks::NativeCompatibilityId {
                value: vec![7; width],
            });
            let error = BackendProcessDescriptor::parse(malformed).expect_err("bad id rejects");
            assert_eq!(error.kind().clone(), crate::ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "backend_process_descriptor.native_compatibility_id.value"
            );
        }

        assert_eq!(
            descriptor().native_compatibility_id().expect("exact id"),
            NativeCompatibilityId::new([7; 32])
        );
    }
    #[test]
    fn backend_preparation_capacity_is_exact_and_missing_is_rejected() {
        let descriptor = descriptor();
        assert_eq!(
            descriptor.to_contract().unwrap().preparing_positions(),
            4096
        );
        let mut raw = descriptor.as_proto().clone();
        raw.preparing_positions = 2;
        assert_eq!(
            BackendProcessDescriptor::parse(raw.clone())
                .unwrap()
                .to_contract()
                .unwrap()
                .preparing_positions(),
            2
        );
        raw.preparing_positions = 0;
        assert!(
            BackendProcessDescriptor::parse(raw).is_err(),
            "a missing hard-cut capability cannot be defaulted from another backend"
        );
    }

    #[test]
    fn descriptor_requires_valid_independent_control_even_without_root_support() {
        let original = descriptor();
        assert!(original.as_proto().bounded_root_support.is_none());
        let mut missing = original.as_proto().clone();
        missing.control_endpoint = None;
        let error = BackendProcessDescriptor::parse(missing).unwrap_err();
        assert_eq!(error.kind().clone(), crate::ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "backend_process_descriptor.control_endpoint"
        );
        for (endpoint, expected_kind) in [
            (
                novarocks::QueryControlEndpoint {
                    host: "be-0.internal".into(),
                    port: 0,
                },
                crate::ProtocolErrorKind::InvalidValue,
            ),
            (
                novarocks::QueryControlEndpoint {
                    host: "be-0.internal".into(),
                    port: 65536,
                },
                crate::ProtocolErrorKind::OutOfRange,
            ),
            (
                novarocks::QueryControlEndpoint {
                    host: String::new(),
                    port: 9091,
                },
                crate::ProtocolErrorKind::InvalidValue,
            ),
        ] {
            let mut malformed = original.as_proto().clone();
            malformed.control_endpoint = Some(endpoint);
            let error = BackendProcessDescriptor::parse(malformed).unwrap_err();
            assert_eq!(error.kind().clone(), expected_kind);
            assert!(
                error
                    .path()
                    .to_string()
                    .starts_with("backend_process_descriptor.control_endpoint")
            );
        }
        let mut shared_endpoint = original.as_proto().clone();
        shared_endpoint.control_endpoint = shared_endpoint.endpoint.clone();
        assert!(BackendProcessDescriptor::parse(shared_endpoint).is_err());
    }

    #[test]
    fn control_endpoint_roundtrip_and_change_are_exact_membership_facts() {
        let original = descriptor();
        let contract = original.to_contract().unwrap();
        assert_eq!(contract.control_endpoint().port(), 9091);
        assert!(contract.bounded_root_support().is_none());
        assert_eq!(
            BackendProcessDescriptor::from_contract(contract.clone()),
            original
        );
        let mut raw = original.as_proto().clone();
        raw.control_endpoint = Some(
            QueryControlEndpoint::new("be-0.internal", 9092)
                .unwrap()
                .as_proto()
                .clone(),
        );
        let changed = BackendProcessDescriptor::parse(raw).unwrap();
        let changed_contract = changed.to_contract().unwrap();
        assert_eq!(contract.process_id(), changed_contract.process_id());
        assert_eq!(contract.endpoint(), changed_contract.endpoint());
        assert_ne!(contract, changed_contract);
        assert_ne!(original, changed);
        let announce = BackendAnnounceRequest::new(changed, BackendReportedState::Running).unwrap();
        assert_eq!(
            announce.descriptor().unwrap().to_contract().unwrap(),
            changed_contract
        );
    }

    #[test]
    fn bounded_root_support_is_preserved_in_membership_and_requires_exact_geometry() {
        use novarocks_execution_contract::{
            RuntimeEndpoint, native_result_support::BoundedRootSupport,
        };
        use novarocks_result_contract::RootProfileId;

        let legacy = descriptor().to_contract().unwrap();
        assert!(legacy.require_bounded_root_support().is_err());
        let contract = legacy
            .with_bounded_root_support(BoundedRootSupport::new(
                RuntimeEndpoint::new("be-0.internal", 9091).unwrap(),
                RootProfileId::V1,
            ))
            .unwrap();
        let encoded = BackendProcessDescriptor::from_contract(contract.clone());
        assert_eq!(
            BackendProcessDescriptor::parse(encoded.as_proto().clone())
                .unwrap()
                .to_contract()
                .unwrap(),
            contract
        );
        let announce =
            BackendAnnounceRequest::new(encoded.clone(), BackendReportedState::Running).unwrap();
        assert_eq!(
            announce.descriptor().unwrap().to_contract().unwrap(),
            contract
        );

        let mut missing_control = encoded.as_proto().clone();
        missing_control
            .bounded_root_support
            .as_mut()
            .unwrap()
            .control_endpoint = None;
        assert!(BackendProcessDescriptor::parse(missing_control).is_err());
        let mut missing_mandatory_control = encoded.as_proto().clone();
        missing_mandatory_control.control_endpoint = None;
        let error = BackendProcessDescriptor::parse(missing_mandatory_control).unwrap_err();
        assert_eq!(error.kind().clone(), crate::ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "backend_process_descriptor.control_endpoint"
        );
        let mut drifting_support = encoded.as_proto().clone();
        drifting_support
            .bounded_root_support
            .as_mut()
            .unwrap()
            .control_endpoint = Some(
            QueryControlEndpoint::new("be-0.internal", 9092)
                .unwrap()
                .as_proto()
                .clone(),
        );
        let error = BackendProcessDescriptor::parse(drifting_support).unwrap_err();
        assert_eq!(error.kind().clone(), crate::ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.path().to_string(),
            "backend_process_descriptor.bounded_root_support"
        );
        let mut same_endpoint = encoded.as_proto().clone();
        same_endpoint
            .bounded_root_support
            .as_mut()
            .unwrap()
            .control_endpoint = same_endpoint.endpoint.clone();
        assert!(BackendProcessDescriptor::parse(same_endpoint).is_err());
        let mut missing_geometry = encoded.as_proto().clone();
        missing_geometry
            .bounded_root_support
            .as_mut()
            .unwrap()
            .geometry = None;
        assert!(BackendProcessDescriptor::parse(missing_geometry).is_err());
        let mut insufficient = encoded.as_proto().clone();
        insufficient
            .bounded_root_support
            .as_mut()
            .unwrap()
            .geometry
            .as_mut()
            .unwrap()
            .transport_streams_per_connection = 127;
        assert!(BackendProcessDescriptor::parse(insufficient).is_err());
        let mut excessive = encoded.as_proto().clone();
        excessive
            .bounded_root_support
            .as_mut()
            .unwrap()
            .geometry
            .as_mut()
            .unwrap()
            .root_live_send_holders = 3;
        assert!(BackendProcessDescriptor::parse(excessive).is_err());
        let mut unknown = encoded.as_proto().clone();
        unknown.bounded_root_support.as_mut().unwrap().profile_id = 2;
        assert!(BackendProcessDescriptor::parse(unknown).is_err());
    }
}
