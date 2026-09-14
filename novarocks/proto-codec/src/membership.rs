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
        deployment_id: impl Into<String>,
        build_identity: impl Into<String>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<Self, ProtocolError> {
        let endpoint =
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port())).map_err(|error| {
                invalid(
                    FieldPath::root("backend_process_descriptor").field("endpoint"),
                    format!("backend process endpoint is invalid: {error}"),
                )
            })?;
        ContractBackendProcessDescriptor::try_new(
            process_id,
            endpoint,
            deployment_id,
            build_identity,
            native_compatibility_id,
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
        let endpoint =
            RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port())).map_err(|error| {
                invalid(
                    FieldPath::root("backend_process_descriptor").field("endpoint"),
                    format!("backend process endpoint is invalid: {error}"),
                )
            })?;
        ContractBackendProcessDescriptor::try_new(
            process_id,
            endpoint,
            raw.deployment_id,
            raw.build_identity,
            native_compatibility_id,
        )
        .map(Self::from_contract)
        .map_err(|error| {
            invalid(
                FieldPath::root("backend_process_descriptor"),
                error.to_string(),
            )
        })
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
                deployment_id: descriptor.deployment_id().to_string(),
                build_identity: descriptor.build_identity().to_string(),
                native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                    value: descriptor.native_compatibility_id().as_bytes().to_vec(),
                }),
            },
        }
    }

    pub fn to_contract(&self) -> Result<ContractBackendProcessDescriptor, ProtocolError> {
        Self::parse(self.raw.clone()).map(|descriptor| {
            let process_id = descriptor
                .process_id()
                .expect("validated backend process descriptor retains its process id");
            let endpoint = descriptor
                .endpoint()
                .expect("validated backend process descriptor retains its endpoint");
            ContractBackendProcessDescriptor::try_new(
                process_id,
                RuntimeEndpoint::new(endpoint.host(), i32::from(endpoint.port()))
                    .expect("validated backend process descriptor retains a runtime endpoint"),
                descriptor.deployment_id(),
                descriptor.build_identity(),
                descriptor
                    .native_compatibility_id()
                    .expect("validated backend process descriptor retains compatibility"),
            )
            .expect("validated backend process descriptor retains contract invariants")
        })
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
        .map_err(|error| ProtocolError::new(path, error.kind(), error.detail().to_owned()))
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
        error.kind(),
        error.detail().to_owned(),
    )
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
            "warehouse-a",
            "build-identity",
            NativeCompatibilityId::new([7; 32]),
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
        assert_eq!(error.kind(), crate::ProtocolErrorKind::MissingField);
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
            assert_eq!(error.kind(), crate::ProtocolErrorKind::InvalidValue);
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
}
