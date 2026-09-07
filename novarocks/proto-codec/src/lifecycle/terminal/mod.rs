//! Validated participant-local terminal runtime-filter observation.
//!
//! Each public validated value owns exactly one generated protobuf message.
//! The Backend captures its per-query runtime-filter facts into those
//! messages and the query context host releases them; this module never
//! depends on their runtime representations.

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, novarocks};

pub const QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES: usize = 16_384;

/// A validated P2 runtime-filter contribution. The generated message is the
/// sole representation; keys and counters are not duplicated as Rust DTOs.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalProfileContributionV1 {
    raw: novarocks::QueryTerminalProfileContributionV1,
}

impl QueryTerminalProfileContributionV1 {
    pub fn parse(
        raw: novarocks::QueryTerminalProfileContributionV1,
    ) -> Result<Self, ProtocolError> {
        validate_profile_contribution(
            &raw,
            FieldPath::root("query_terminal_profile_contribution"),
        )?;
        Ok(Self { raw })
    }

    /// Establishes the wire's required key ordering before validation.
    pub fn seal(
        mut raw: novarocks::QueryTerminalProfileContributionV1,
    ) -> Result<Self, ProtocolError> {
        raw.channels.sort_by_key(channel_key);
        raw.producer_streams.sort_by_key(producer_stream_key);
        raw.transport_routes.sort_by_key(transport_route_key);
        raw.consumers.sort_by_key(consumer_key);
        Self::parse(raw)
    }

    pub const fn as_proto(&self) -> &novarocks::QueryTerminalProfileContributionV1 {
        &self.raw
    }

    pub const fn version(&self) -> u32 {
        self.raw.version
    }

    /// Wire leaves remain generated values because their role-local semantic
    /// interpretation belongs to the Frontend fold and Backend capture paths.
    pub fn channels(&self) -> &[novarocks::QueryTerminalRuntimeFilterChannelV1] {
        &self.raw.channels
    }

    pub fn producer_streams(&self) -> &[novarocks::QueryTerminalRuntimeFilterProducerStreamV1] {
        &self.raw.producer_streams
    }

    pub fn transport_routes(&self) -> &[novarocks::QueryTerminalRuntimeFilterTransportRouteV1] {
        &self.raw.transport_routes
    }

    pub fn consumers(&self) -> &[novarocks::QueryTerminalRuntimeFilterConsumerV1] {
        &self.raw.consumers
    }
}

/// A validated generated reason for unavailable terminal telemetry.
#[derive(Clone, Debug, PartialEq)]
pub struct TerminalTelemetryUnavailable {
    raw: novarocks::TerminalTelemetryUnavailable,
}

impl TerminalTelemetryUnavailable {
    pub fn parse(raw: novarocks::TerminalTelemetryUnavailable) -> Result<Self, ProtocolError> {
        validate_unavailable(&raw, FieldPath::root("terminal_telemetry_unavailable"))?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::TerminalTelemetryUnavailable {
        &self.raw
    }

    pub fn stage(&self) -> &str {
        &self.raw.stage
    }

    pub fn code(&self) -> &str {
        &self.raw.code
    }
}

/// A validated generated profile-contribution telemetry oneof.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalProfileContributionTelemetry {
    raw: novarocks::QueryTerminalProfileContributionTelemetry,
}

impl QueryTerminalProfileContributionTelemetry {
    pub fn parse(
        raw: novarocks::QueryTerminalProfileContributionTelemetry,
    ) -> Result<Self, ProtocolError> {
        validate_profile_contribution_telemetry(
            &raw,
            FieldPath::root("query_terminal_profile_contribution_telemetry"),
        )?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryTerminalProfileContributionTelemetry {
        &self.raw
    }

    pub fn available(&self) -> Option<QueryTerminalProfileContributionV1> {
        let novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
            contribution,
        ) = self.raw.telemetry.as_ref()?
        else {
            return None;
        };
        Some(
            QueryTerminalProfileContributionV1::parse(contribution.clone())
                .expect("validated profile telemetry always has a valid contribution"),
        )
    }

    pub fn unavailable(&self) -> Option<TerminalTelemetryUnavailable> {
        let novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
            reason,
        ) = self.raw.telemetry.as_ref()?
        else {
            return None;
        };
        Some(
            TerminalTelemetryUnavailable::parse(reason.clone())
                .expect("validated profile telemetry always has a valid reason"),
        )
    }
}

fn validate_profile_contribution_telemetry(
    raw: &novarocks::QueryTerminalProfileContributionTelemetry,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    use novarocks::query_terminal_profile_contribution_telemetry::Telemetry;
    match raw.telemetry.as_ref() {
        Some(Telemetry::Available(value)) => {
            validate_profile_contribution(value, path.field("telemetry").field("available"))
        }
        Some(Telemetry::Unavailable(reason)) => {
            validate_unavailable(reason, path.field("telemetry").field("unavailable"))
        }
        None => Err(error(
            path.field("telemetry"),
            ProtocolErrorKind::MissingField,
            "query terminal profile contribution telemetry is required",
        )),
    }
}

fn validate_unavailable(
    raw: &novarocks::TerminalTelemetryUnavailable,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if raw.stage.trim().is_empty() {
        return Err(error(
            path.field("stage"),
            ProtocolErrorKind::InvalidValue,
            "terminal telemetry unavailable stage and code must be nonempty",
        ));
    }
    if raw.code.trim().is_empty() {
        return Err(error(
            path.field("code"),
            ProtocolErrorKind::InvalidValue,
            "terminal telemetry unavailable stage and code must be nonempty",
        ));
    }
    Ok(())
}

fn validate_profile_contribution(
    raw: &novarocks::QueryTerminalProfileContributionV1,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if raw.version != QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1 {
        return Err(error(
            path.field("version"),
            ProtocolErrorKind::VersionMismatch,
            "unsupported query terminal profile contribution version",
        ));
    }
    for (field, label, len) in [
        ("channels", "channel", raw.channels.len()),
        (
            "producer_streams",
            "producer stream",
            raw.producer_streams.len(),
        ),
        (
            "transport_routes",
            "transport route",
            raw.transport_routes.len(),
        ),
        ("consumers", "consumer", raw.consumers.len()),
    ] {
        if len > QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES {
            return Err(error(
                path.field(field),
                ProtocolErrorKind::Capacity,
                format!("terminal runtime-filter {label} section exceeds the cardinality limit"),
            ));
        }
    }
    validate_channels(&raw.channels, path.field("channels"))?;
    validate_producer_streams(&raw.producer_streams, path.field("producer_streams"))?;
    validate_transport_routes(&raw.transport_routes, path.field("transport_routes"))?;
    validate_consumers(&raw.consumers, path.field("consumers"))
}

fn validate_channels(
    values: &[novarocks::QueryTerminalRuntimeFilterChannelV1],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for (index, value) in values.iter().enumerate() {
        let value_path = path.clone().index(index);
        let key = (value.channel_binding_id, value.channel_id);
        validate_channel_key(key, value_path.clone())?;
        require_known_enum(
            value.install_state,
            novarocks::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed as i32,
            value_path.field("install_state"),
            "invalid terminal runtime-filter channel install state",
        )?;
        match novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::try_from(
            value.terminal_state,
        ) {
            Ok(novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Open)
            | Ok(novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed)
            | Ok(novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable)
            | Ok(novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled) => {}
            _ => {
                return Err(error(
                    value_path.field("terminal_state"),
                    ProtocolErrorKind::InvalidEnum,
                    "invalid terminal runtime-filter channel terminal state",
                ));
            }
        }
        validate_optional_nonzero(
            value.latest_published_logical_version,
            value_path.field("latest_published_logical_version"),
            "terminal runtime-filter latest published logical version must be nonzero",
        )?;
    }
    Ok(())
}

fn channel_key(value: &novarocks::QueryTerminalRuntimeFilterChannelV1) -> (u32, u32) {
    (value.channel_binding_id, value.channel_id)
}

fn producer_stream_key(
    value: &novarocks::QueryTerminalRuntimeFilterProducerStreamV1,
) -> ((u32, u32), i64, i64, u32) {
    let id = value
        .producer_fragment_instance_id
        .as_ref()
        .map_or((i64::MIN, i64::MIN), |id| (id.hi, id.lo));
    (
        (value.channel_binding_id, value.channel_id),
        id.0,
        id.1,
        value.partition_id,
    )
}

fn transport_route_key(
    value: &novarocks::QueryTerminalRuntimeFilterTransportRouteV1,
) -> ((u32, u32), u64) {
    (
        (value.channel_binding_id, value.channel_id),
        value.route_edge_id,
    )
}

fn consumer_key(
    value: &novarocks::QueryTerminalRuntimeFilterConsumerV1,
) -> ((u32, u32), u32, i64, i64) {
    let id = value
        .fragment_instance_id
        .as_ref()
        .map_or((i64::MIN, i64::MIN), |id| (id.hi, id.lo));
    (
        (value.channel_binding_id, value.channel_id),
        value.consumer_binding_id,
        id.0,
        id.1,
    )
}

fn validate_producer_streams(
    values: &[novarocks::QueryTerminalRuntimeFilterProducerStreamV1],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for (index, value) in values.iter().enumerate() {
        let value_path = path
            .clone()
            .index(index)
            .field("producer_fragment_instance_id");
        let id = value
            .producer_fragment_instance_id
            .as_ref()
            .ok_or_else(|| {
                error(
                    value_path.clone(),
                    ProtocolErrorKind::MissingField,
                    "terminal runtime-filter producer fragment instance id is required",
                )
            })?;
        validate_nonzero_unique_id(
            id,
            value_path,
            "terminal runtime-filter producer fragment instance id must be nonzero",
        )?;
    }
    Ok(())
}

fn validate_transport_routes(
    values: &[novarocks::QueryTerminalRuntimeFilterTransportRouteV1],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for (index, value) in values.iter().enumerate() {
        if value.route_edge_id == 0 {
            return Err(error(
                path.clone().index(index).field("route_edge_id"),
                ProtocolErrorKind::InvalidValue,
                "terminal runtime-filter route edge id must be nonzero",
            ));
        }
    }
    Ok(())
}

fn validate_consumers(
    values: &[novarocks::QueryTerminalRuntimeFilterConsumerV1],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for (index, value) in values.iter().enumerate() {
        let value_path = path.clone().index(index);
        if value.consumer_binding_id == 0 {
            return Err(error(
                value_path.field("consumer_binding_id"),
                ProtocolErrorKind::InvalidValue,
                "terminal runtime-filter consumer binding id must be nonzero",
            ));
        }
        let id = value.fragment_instance_id.as_ref().ok_or_else(|| {
            error(
                value_path.field("fragment_instance_id"),
                ProtocolErrorKind::MissingField,
                "terminal runtime-filter consumer fragment instance id is required",
            )
        })?;
        validate_nonzero_unique_id(
            id,
            value_path.field("fragment_instance_id"),
            "terminal runtime-filter consumer fragment instance id must be nonzero",
        )?;
        validate_optional_nonzero(
            value.latest_delivered_logical_version,
            value_path.field("latest_delivered_logical_version"),
            "terminal runtime-filter latest delivered logical version must be nonzero",
        )?;
        validate_optional_nonzero(
            value.latest_applied_logical_version,
            value_path.field("latest_applied_logical_version"),
            "terminal runtime-filter latest applied logical version must be nonzero",
        )?;
        match novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::try_from(value.subscription_terminal) {
            Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact) => {}
            _ => return Err(error(value_path.field("subscription_terminal"), ProtocolErrorKind::InvalidEnum, "invalid terminal runtime-filter subscription terminal state")),
        }
        let _reasons = value.scan_not_evaluated_reasons.as_ref().ok_or_else(|| {
            error(
                value_path.field("scan_not_evaluated_reasons"),
                ProtocolErrorKind::MissingField,
                "terminal runtime-filter scan not-evaluated counters are required",
            )
        })?;
    }
    Ok(())
}

fn require_known_enum(
    value: i32,
    expected: i32,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    if value == expected {
        Ok(())
    } else {
        Err(error(path, ProtocolErrorKind::InvalidEnum, detail))
    }
}

fn validate_channel_key(key: (u32, u32), path: FieldPath) -> Result<(), ProtocolError> {
    if key.0 == 0 {
        return Err(error(
            path.field("channel_binding_id"),
            ProtocolErrorKind::InvalidValue,
            "terminal runtime-filter channel identity must be nonzero",
        ));
    }
    if key.1 == 0 {
        return Err(error(
            path.field("channel_id"),
            ProtocolErrorKind::InvalidValue,
            "terminal runtime-filter channel identity must be nonzero",
        ));
    }
    Ok(())
}

fn validate_nonzero_unique_id(
    raw: &common::UniqueId,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    if raw.hi == 0 && raw.lo == 0 {
        return Err(error(path, ProtocolErrorKind::InvalidValue, detail));
    }
    Ok(())
}

fn validate_optional_nonzero(
    value: Option<u64>,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    if value == Some(0) {
        Err(error(path, ProtocolErrorKind::InvalidValue, detail))
    } else {
        Ok(())
    }
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel(install_state: i32) -> novarocks::QueryTerminalRuntimeFilterChannelV1 {
        novarocks::QueryTerminalRuntimeFilterChannelV1 {
            channel_binding_id: 1,
            channel_id: 1,
            install_state,
            terminal_state: novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                as i32,
            ..Default::default()
        }
    }

    #[test]
    fn terminal_validation_reports_runtime_filter_repeated_field_paths() {
        let error = QueryTerminalProfileContributionV1::parse(
            novarocks::QueryTerminalProfileContributionV1 {
                version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                channels: vec![novarocks::QueryTerminalRuntimeFilterChannelV1 {
                    channel_binding_id: 1,
                    channel_id: 1,
                    install_state: 99,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .expect_err("invalid repeated channel enum");

        assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
        assert_eq!(
            error.path().to_string(),
            "query_terminal_profile_contribution.channels[0].install_state"
        );
    }

    #[test]
    fn profile_contribution_retains_wire_enum_validation() {
        let invalid = novarocks::QueryTerminalProfileContributionV1 {
            version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
            channels: vec![channel(99)],
            ..Default::default()
        };
        assert!(QueryTerminalProfileContributionV1::parse(invalid).is_err());
    }

    #[test]
    fn profile_contribution_preserves_backend_folded_terminal_counters_without_revalidating_them() {
        let contribution =
            QueryTerminalProfileContributionV1::seal(
                novarocks::QueryTerminalProfileContributionV1 {
                    version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                    channels: vec![novarocks::QueryTerminalRuntimeFilterChannelV1 {
                        channel_binding_id: 1,
                        channel_id: 1,
                        install_state:
                            novarocks::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                                as i32,
                        terminal_state:
                            novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                                as i32,
                        latest_published_logical_version: Some(1),
                        published_count: 1,
                        completed_count: 2,
                        unavailable_count: 1,
                        cancelled_count: 0,
                    }],
                    ..Default::default()
                },
            )
            .expect("joined terminal state preserves repeated and incomplete-coverage events");

        let channel = &contribution.as_proto().channels[0];
        assert_eq!(channel.completed_count, 2);
        assert_eq!(channel.unavailable_count, 1);

        let mut conflicting = contribution.as_proto().clone();
        conflicting.channels[0].cancelled_count = 1;
        assert!(QueryTerminalProfileContributionV1::parse(conflicting).is_ok());
    }

    /// The telemetry oneof is explicit in both directions: an absent oneof is
    /// refused rather than read as an empty contribution, and an unavailable
    /// reason must carry both of its fields.
    #[test]
    fn profile_contribution_telemetry_is_never_an_empty_payload() {
        let absent = novarocks::QueryTerminalProfileContributionTelemetry { telemetry: None };
        let error = QueryTerminalProfileContributionTelemetry::parse(absent)
            .expect_err("an absent telemetry oneof is not an empty contribution");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "query_terminal_profile_contribution_telemetry.telemetry"
        );

        let blank_stage = novarocks::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                    novarocks::TerminalTelemetryUnavailable {
                        stage: "  ".into(),
                        code: "BUDGET_EXHAUSTED".into(),
                    },
                ),
            ),
        };
        assert_eq!(
            QueryTerminalProfileContributionTelemetry::parse(blank_stage)
                .expect_err("a blank stage is not a reason")
                .kind(),
            ProtocolErrorKind::InvalidValue
        );

        let available = novarocks::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                    novarocks::QueryTerminalProfileContributionV1 {
                        version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                        ..Default::default()
                    },
                ),
            ),
        };
        let telemetry = QueryTerminalProfileContributionTelemetry::parse(available)
            .expect("a versioned contribution is available telemetry");
        assert!(telemetry.unavailable().is_none());
        assert_eq!(
            telemetry
                .available()
                .expect("available telemetry carries its contribution")
                .version(),
            QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1
        );
    }
}
