//! Wire-level execution-binding helpers and typed control-plane outcomes.

use crate::{FieldPath, ProtocolError, ProtocolErrorKind, canonical, novarocks};

const DECLARATION_DIGEST_DOMAIN: &[u8] = b"novarocks.connector.execution-binding-declaration.v1\0";
const DECLARATION_MESSAGE_NAME: &str = "novarocks.ConnectorExecutionBindingDeclaration";
const MAX_SAFE_DETAIL_BYTES: usize = 512;
const MAX_SAFE_FIELD_PATH_BYTES: usize = 256;

/// Computes the domain-separated canonical digest for the generated wire DTO.
///
/// The caller owns structural and domain validation. This helper deliberately
/// accepts the generated message directly so Protocol does not become a second
/// declaration-domain authority.
pub fn connector_execution_binding_declaration_digest(
    declaration: &novarocks::ConnectorExecutionBindingDeclaration,
) -> Result<[u8; 32], ProtocolError> {
    canonical::digest_message(
        DECLARATION_DIGEST_DOMAIN,
        DECLARATION_MESSAGE_NAME,
        declaration,
    )
    .map_err(|error| {
        ProtocolError::new(
            FieldPath::root("connector_execution_binding"),
            ProtocolErrorKind::InvalidValue,
            format!("cannot canonicalize execution binding declaration: {error}"),
        )
    })
}

/// Closed Ensure rejection reason set validated at the Protocol boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnsureConnectorExecutionBindingRejectionReason {
    InvalidDeclaration,
    ConflictingDeclaration,
    QueryIncarnationConflict,
    Retiring,
    HostUnavailable,
    ActivationUnavailable,
    DeadlineExceeded,
    ResourceExhausted,
    InternalFailure,
}

impl EnsureConnectorExecutionBindingRejectionReason {
    fn try_from_proto(value: i32) -> Result<Self, ProtocolError> {
        use novarocks::EnsureConnectorExecutionBindingRejectionReason as ProtoReason;

        match ProtoReason::try_from(value) {
            Ok(ProtoReason::InvalidDeclaration) => Ok(Self::InvalidDeclaration),
            Ok(ProtoReason::ConflictingDeclaration) => Ok(Self::ConflictingDeclaration),
            Ok(ProtoReason::QueryIncarnationConflict) => Ok(Self::QueryIncarnationConflict),
            Ok(ProtoReason::Retiring) => Ok(Self::Retiring),
            Ok(ProtoReason::HostUnavailable) => Ok(Self::HostUnavailable),
            Ok(ProtoReason::ActivationUnavailable) => Ok(Self::ActivationUnavailable),
            Ok(ProtoReason::DeadlineExceeded) => Ok(Self::DeadlineExceeded),
            Ok(ProtoReason::ResourceExhausted) => Ok(Self::ResourceExhausted),
            Ok(ProtoReason::InternalFailure) => Ok(Self::InternalFailure),
            Ok(ProtoReason::Unspecified) | Err(_) => Err(ProtocolError::new(
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("reason"),
                ProtocolErrorKind::InvalidEnum,
                "unknown or unspecified execution binding rejection reason",
            )),
        }
    }

    fn to_proto(self) -> i32 {
        use novarocks::EnsureConnectorExecutionBindingRejectionReason as ProtoReason;

        (match self {
            Self::InvalidDeclaration => ProtoReason::InvalidDeclaration,
            Self::ConflictingDeclaration => ProtoReason::ConflictingDeclaration,
            Self::QueryIncarnationConflict => ProtoReason::QueryIncarnationConflict,
            Self::Retiring => ProtoReason::Retiring,
            Self::HostUnavailable => ProtoReason::HostUnavailable,
            Self::ActivationUnavailable => ProtoReason::ActivationUnavailable,
            Self::DeadlineExceeded => ProtoReason::DeadlineExceeded,
            Self::ResourceExhausted => ProtoReason::ResourceExhausted,
            Self::InternalFailure => ProtoReason::InternalFailure,
        }) as i32
    }

    fn allows_retryable_before_progress(self, value: bool) -> bool {
        match self {
            Self::InvalidDeclaration
            | Self::ConflictingDeclaration
            | Self::QueryIncarnationConflict
            | Self::Retiring
            | Self::HostUnavailable => !value,
            Self::DeadlineExceeded => value,
            Self::ActivationUnavailable | Self::ResourceExhausted | Self::InternalFailure => true,
        }
    }
}

/// A safe, application-produced Ensure rejection preserved across the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureConnectorExecutionBindingRejection {
    reason: EnsureConnectorExecutionBindingRejectionReason,
    retryable_before_progress: bool,
    safe_detail: String,
    safe_field_path: Option<String>,
}

impl EnsureConnectorExecutionBindingRejection {
    pub fn try_new(
        reason: EnsureConnectorExecutionBindingRejectionReason,
        retryable_before_progress: bool,
        safe_detail: impl Into<String>,
        safe_field_path: Option<String>,
    ) -> Result<Self, ProtocolError> {
        let safe_detail = safe_detail.into();
        validate_bounded_text(
            &safe_detail,
            MAX_SAFE_DETAIL_BYTES,
            FieldPath::root("ensure_connector_execution_binding_response")
                .field("rejection")
                .field("safe_detail"),
            "safe detail",
            true,
        )?;
        if let Some(path) = safe_field_path.as_deref() {
            validate_bounded_text(
                path,
                MAX_SAFE_FIELD_PATH_BYTES,
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("safe_field_path"),
                "safe field path",
                false,
            )?;
        }
        if !reason.allows_retryable_before_progress(retryable_before_progress) {
            return Err(ProtocolError::new(
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("retryable_before_progress"),
                ProtocolErrorKind::InconsistentFields,
                "execution binding rejection reason does not allow this retryability",
            ));
        }
        Ok(Self {
            reason,
            retryable_before_progress,
            safe_detail,
            safe_field_path,
        })
    }

    pub fn reason(&self) -> EnsureConnectorExecutionBindingRejectionReason {
        self.reason
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn safe_detail(&self) -> &str {
        &self.safe_detail
    }

    pub fn safe_field_path(&self) -> Option<&str> {
        self.safe_field_path.as_deref()
    }

    fn try_from_proto(
        raw: novarocks::EnsureConnectorExecutionBindingRejection,
    ) -> Result<Self, ProtocolError> {
        Self::try_new(
            EnsureConnectorExecutionBindingRejectionReason::try_from_proto(raw.reason)?,
            raw.retryable_before_progress,
            raw.safe_detail,
            raw.safe_field_path,
        )
    }

    fn to_proto(&self) -> novarocks::EnsureConnectorExecutionBindingRejection {
        novarocks::EnsureConnectorExecutionBindingRejection {
            reason: self.reason.to_proto(),
            retryable_before_progress: self.retryable_before_progress,
            safe_detail: self.safe_detail.clone(),
            safe_field_path: self.safe_field_path.clone(),
        }
    }
}

/// Validated Ensure outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsureConnectorExecutionBindingOutcome {
    Ensured,
    Rejected(EnsureConnectorExecutionBindingRejection),
}

/// Validated Protocol result wrapper for Ensure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureConnectorExecutionBindingResult {
    outcome: EnsureConnectorExecutionBindingOutcome,
}

impl EnsureConnectorExecutionBindingResult {
    pub const fn ensured() -> Self {
        Self {
            outcome: EnsureConnectorExecutionBindingOutcome::Ensured,
        }
    }

    pub const fn rejected(rejection: EnsureConnectorExecutionBindingRejection) -> Self {
        Self {
            outcome: EnsureConnectorExecutionBindingOutcome::Rejected(rejection),
        }
    }

    pub fn outcome(&self) -> &EnsureConnectorExecutionBindingOutcome {
        &self.outcome
    }

    pub fn try_from_proto(
        raw: novarocks::EnsureConnectorExecutionBindingResponse,
    ) -> Result<Self, ProtocolError> {
        use novarocks::ensure_connector_execution_binding_response::Outcome;

        let outcome = match raw.outcome {
            Some(Outcome::Ensured(_)) => EnsureConnectorExecutionBindingOutcome::Ensured,
            Some(Outcome::Rejection(rejection)) => {
                EnsureConnectorExecutionBindingOutcome::Rejected(
                    EnsureConnectorExecutionBindingRejection::try_from_proto(rejection)?,
                )
            }
            None => {
                return Err(ProtocolError::new(
                    FieldPath::root("ensure_connector_execution_binding_response").field("outcome"),
                    ProtocolErrorKind::MissingField,
                    "ensure execution binding outcome is required",
                ));
            }
        };
        Ok(Self { outcome })
    }

    pub fn to_proto(&self) -> novarocks::EnsureConnectorExecutionBindingResponse {
        use novarocks::ensure_connector_execution_binding_response::Outcome;

        let outcome = match &self.outcome {
            EnsureConnectorExecutionBindingOutcome::Ensured => {
                Outcome::Ensured(novarocks::EnsureConnectorExecutionBindingEnsured {})
            }
            EnsureConnectorExecutionBindingOutcome::Rejected(rejection) => {
                Outcome::Rejection(rejection.to_proto())
            }
        };
        novarocks::EnsureConnectorExecutionBindingResponse {
            outcome: Some(outcome),
        }
    }
}

/// Closed Retire result set.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RetireConnectorExecutionBindingOutcome {
    Accepted,
    NotFound,
    Unavailable,
    InvalidKey,
    Internal,
}

/// Validated Protocol result wrapper for Retire.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetireConnectorExecutionBindingResult {
    outcome: RetireConnectorExecutionBindingOutcome,
}

impl RetireConnectorExecutionBindingResult {
    pub const fn new(outcome: RetireConnectorExecutionBindingOutcome) -> Self {
        Self { outcome }
    }

    pub const fn outcome(self) -> RetireConnectorExecutionBindingOutcome {
        self.outcome
    }

    pub fn try_from_proto(
        raw: novarocks::RetireConnectorExecutionBindingResponse,
    ) -> Result<Self, ProtocolError> {
        use novarocks::retire_connector_execution_binding_response::Outcome;

        let outcome = match raw.outcome {
            Some(Outcome::Accepted(_)) => RetireConnectorExecutionBindingOutcome::Accepted,
            Some(Outcome::NotFound(_)) => RetireConnectorExecutionBindingOutcome::NotFound,
            Some(Outcome::Unavailable(_)) => RetireConnectorExecutionBindingOutcome::Unavailable,
            Some(Outcome::InvalidKey(_)) => RetireConnectorExecutionBindingOutcome::InvalidKey,
            Some(Outcome::Internal(_)) => RetireConnectorExecutionBindingOutcome::Internal,
            None => {
                return Err(ProtocolError::new(
                    FieldPath::root("retire_connector_execution_binding_response").field("outcome"),
                    ProtocolErrorKind::MissingField,
                    "retire execution binding outcome is required",
                ));
            }
        };
        Ok(Self { outcome })
    }

    pub fn to_proto(self) -> novarocks::RetireConnectorExecutionBindingResponse {
        use novarocks::retire_connector_execution_binding_response::Outcome;

        let outcome = match self.outcome {
            RetireConnectorExecutionBindingOutcome::Accepted => {
                Outcome::Accepted(novarocks::RetireConnectorExecutionBindingAccepted {})
            }
            RetireConnectorExecutionBindingOutcome::NotFound => {
                Outcome::NotFound(novarocks::RetireConnectorExecutionBindingNotFound {})
            }
            RetireConnectorExecutionBindingOutcome::Unavailable => {
                Outcome::Unavailable(novarocks::RetireConnectorExecutionBindingUnavailable {})
            }
            RetireConnectorExecutionBindingOutcome::InvalidKey => {
                Outcome::InvalidKey(novarocks::RetireConnectorExecutionBindingInvalidKey {})
            }
            RetireConnectorExecutionBindingOutcome::Internal => {
                Outcome::Internal(novarocks::RetireConnectorExecutionBindingInternal {})
            }
        };
        novarocks::RetireConnectorExecutionBindingResponse {
            outcome: Some(outcome),
        }
    }
}

fn validate_bounded_text(
    value: &str,
    max_bytes: usize,
    path: FieldPath,
    label: &str,
    allow_empty: bool,
) -> Result<(), ProtocolError> {
    if (!allow_empty && value.is_empty()) || value.len() > max_bytes {
        return Err(ProtocolError::new(
            path,
            ProtocolErrorKind::OutOfRange,
            format!(
                "{label} must {} and be at most {max_bytes} bytes",
                if allow_empty { "be" } else { "be non-empty" }
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost_reflect::DescriptorPool;

    use super::*;

    fn declaration() -> novarocks::ConnectorExecutionBindingDeclaration {
        novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: "catalog.analytics".into(),
            incarnation: vec![7; 16],
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                    novarocks::IcebergExecutionBindingDeclaration {
                        access_binding: "local-iceberg".into(),
                    },
                ),
            ),
        }
    }

    #[test]
    fn wire_declaration_digest_is_domain_separated_and_covers_presence_and_variant() {
        const ICEBERG_DECLARATION_DIGEST_GOLDEN_HEX: &str =
            "c443163cfd63a77ca3c489f407fee2e4ad08e97cd5d9d6ffdc7bd3ca596c5ee3";

        let iceberg = declaration();
        let same = declaration();
        let starrocks = novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: "catalog.analytics".into(),
            incarnation: vec![7; 16],
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Starrocks(
                    novarocks::StarRocksExecutionBindingDeclaration {
                        local_binding: "local-starrocks".into(),
                    },
                ),
            ),
        };
        let changed_binding = novarocks::ConnectorExecutionBindingDeclaration {
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                    novarocks::IcebergExecutionBindingDeclaration {
                        access_binding: "different-binding".into(),
                    },
                ),
            ),
            ..declaration()
        };
        let changed_instance = novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: "catalog.replacement".into(),
            ..declaration()
        };
        let changed_incarnation = novarocks::ConnectorExecutionBindingDeclaration {
            incarnation: vec![8; 16],
            ..declaration()
        };

        assert_eq!(
            connector_execution_binding_declaration_digest(&iceberg).expect("digest"),
            connector_execution_binding_declaration_digest(&same).expect("digest")
        );
        assert_ne!(
            connector_execution_binding_declaration_digest(&iceberg).expect("digest"),
            connector_execution_binding_declaration_digest(&starrocks).expect("digest")
        );
        assert_ne!(
            connector_execution_binding_declaration_digest(&iceberg).expect("digest"),
            connector_execution_binding_declaration_digest(&changed_binding).expect("digest")
        );
        assert_ne!(
            connector_execution_binding_declaration_digest(&iceberg).expect("digest"),
            connector_execution_binding_declaration_digest(&changed_instance).expect("digest")
        );
        assert_ne!(
            connector_execution_binding_declaration_digest(&iceberg).expect("digest"),
            connector_execution_binding_declaration_digest(&changed_incarnation).expect("digest")
        );
        assert_eq!(
            connector_execution_binding_declaration_digest(&iceberg)
                .expect("digest")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            ICEBERG_DECLARATION_DIGEST_GOLDEN_HEX,
        );
    }

    #[test]
    fn validates_the_closed_ensure_reason_retry_matrix() {
        for reason in [
            EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration,
            EnsureConnectorExecutionBindingRejectionReason::ConflictingDeclaration,
            EnsureConnectorExecutionBindingRejectionReason::QueryIncarnationConflict,
            EnsureConnectorExecutionBindingRejectionReason::Retiring,
            EnsureConnectorExecutionBindingRejectionReason::HostUnavailable,
        ] {
            assert!(
                EnsureConnectorExecutionBindingRejection::try_new(reason, false, "safe", None)
                    .is_ok()
            );
            assert!(
                EnsureConnectorExecutionBindingRejection::try_new(reason, true, "safe", None)
                    .is_err()
            );
        }
        assert!(
            EnsureConnectorExecutionBindingRejection::try_new(
                EnsureConnectorExecutionBindingRejectionReason::DeadlineExceeded,
                true,
                "safe",
                None,
            )
            .is_ok()
        );
        assert!(
            EnsureConnectorExecutionBindingRejection::try_new(
                EnsureConnectorExecutionBindingRejectionReason::DeadlineExceeded,
                false,
                "safe",
                None,
            )
            .is_err()
        );
        for reason in [
            EnsureConnectorExecutionBindingRejectionReason::ActivationUnavailable,
            EnsureConnectorExecutionBindingRejectionReason::ResourceExhausted,
            EnsureConnectorExecutionBindingRejectionReason::InternalFailure,
        ] {
            for retryable in [false, true] {
                assert!(
                    EnsureConnectorExecutionBindingRejection::try_new(
                        reason,
                        retryable,
                        "safe",
                        Some("binding.access".into())
                    )
                    .is_ok()
                );
            }
        }
    }

    #[test]
    fn ensure_and_retire_results_round_trip_and_reject_missing_or_unknown_outcomes() {
        let rejection = EnsureConnectorExecutionBindingRejection::try_new(
            EnsureConnectorExecutionBindingRejectionReason::Retiring,
            false,
            "generation is retiring",
            Some("declaration.incarnation".into()),
        )
        .expect("valid rejection");
        let result = EnsureConnectorExecutionBindingResult::rejected(rejection.clone());
        assert_eq!(
            EnsureConnectorExecutionBindingResult::try_from_proto(result.to_proto())
                .expect("round trip")
                .outcome(),
            result.outcome()
        );
        assert!(EnsureConnectorExecutionBindingResult::try_from_proto(Default::default()).is_err());

        let retire = RetireConnectorExecutionBindingResult::new(
            RetireConnectorExecutionBindingOutcome::Accepted,
        );
        assert_eq!(
            RetireConnectorExecutionBindingResult::try_from_proto(retire.to_proto())
                .expect("round trip"),
            retire
        );
        assert!(RetireConnectorExecutionBindingResult::try_from_proto(Default::default()).is_err());

        let unknown_reason = novarocks::EnsureConnectorExecutionBindingResponse {
            outcome: Some(
                novarocks::ensure_connector_execution_binding_response::Outcome::Rejection(
                    novarocks::EnsureConnectorExecutionBindingRejection {
                        reason: 99,
                        retryable_before_progress: false,
                        safe_detail: "safe".into(),
                        safe_field_path: None,
                    },
                ),
            ),
        };
        assert!(EnsureConnectorExecutionBindingResult::try_from_proto(unknown_reason).is_err());

        for outcome in [
            RetireConnectorExecutionBindingOutcome::Accepted,
            RetireConnectorExecutionBindingOutcome::NotFound,
            RetireConnectorExecutionBindingOutcome::Unavailable,
            RetireConnectorExecutionBindingOutcome::InvalidKey,
            RetireConnectorExecutionBindingOutcome::Internal,
        ] {
            let result = RetireConnectorExecutionBindingResult::new(outcome);
            assert_eq!(
                RetireConnectorExecutionBindingResult::try_from_proto(result.to_proto())
                    .expect("closed retire outcome round trip"),
                result
            );
        }
    }

    #[test]
    fn generated_descriptor_exposes_only_the_closed_provider_and_result_sets() {
        let pool = DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).expect("descriptor set");
        let declaration = pool
            .get_message_by_name("novarocks.ConnectorExecutionBindingDeclaration")
            .expect("declaration descriptor");
        let provider = declaration
            .oneofs()
            .find(|oneof| oneof.name() == "provider")
            .expect("provider oneof");
        let names = provider
            .fields()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, ["iceberg", "starrocks"]);

        let ensure_reason = pool
            .get_enum_by_name("novarocks.EnsureConnectorExecutionBindingRejectionReason")
            .expect("ensure reason descriptor");
        assert!(ensure_reason.get_value_by_name("CANCELLED").is_none());

        let ensure_request = pool
            .get_message_by_name("novarocks.EnsureConnectorExecutionBindingRequest")
            .expect("ensure request descriptor");
        assert_eq!(
            ensure_request
                .fields()
                .map(|field| (field.number(), field.name().to_string()))
                .collect::<Vec<_>>(),
            [
                (1, "execution_id".to_string()),
                (6, "declaration".to_string())
            ]
        );
        assert_eq!(
            ensure_request.reserved_ranges().collect::<Vec<_>>(),
            vec![2..3, 3..4, 4..5, 5..6]
        );
        for name in [
            "provider_id",
            "instance_id",
            "incarnation",
            "declaration_payload",
        ] {
            assert!(
                ensure_request
                    .reserved_names()
                    .any(|reserved| reserved == name)
            );
        }

        for response_name in [
            "novarocks.EnsureConnectorExecutionBindingResponse",
            "novarocks.RetireConnectorExecutionBindingResponse",
        ] {
            let response = pool
                .get_message_by_name(response_name)
                .expect("response descriptor");
            assert_eq!(
                response.reserved_ranges().collect::<Vec<_>>(),
                vec![1..2, 2..3]
            );
            assert!(response.reserved_names().any(|name| name == "status_code"));
            assert!(response.reserved_names().any(|name| name == "message"));
        }
    }
}
