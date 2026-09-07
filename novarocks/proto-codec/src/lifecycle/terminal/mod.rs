//! Validated participant-local terminal lifecycle values.
//!
//! Each public validated value owns exactly one generated protobuf message.
//! The Backend encodes runtime profiles and sink facts into those messages;
//! this module never depends on their runtime representations.

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, novarocks};

use super::{
    identity::QueryExecutionId,
    manifest::{ParticipantAttemptRef, ParticipantBackendIdentity},
};

pub const QUERY_TERMINAL_SNAPSHOT_VERSION_V1: u32 = 1;
pub const PARTICIPANT_TERMINAL_OUTCOME_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES: usize = 128;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES: usize = 4096;
pub const QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES: usize = 16_384;

const TERMINALIZATION_PROOF_VERSION_V1: u32 = 1;

/// A validated P1/P2 participant terminal snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSnapshot {
    raw: novarocks::QueryTerminalSnapshot,
}

impl QueryTerminalSnapshot {
    pub fn parse(raw: novarocks::QueryTerminalSnapshot) -> Result<Self, ProtocolError> {
        validate_snapshot(&raw, FieldPath::root("query_terminal_snapshot"))?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryTerminalSnapshot {
        &self.raw
    }

    pub const fn version(&self) -> u32 {
        self.raw.version
    }

    pub fn participant(&self) -> ParticipantAttemptRef {
        required_participant_attempt_ref(
            self.raw.participant.as_ref(),
            FieldPath::root("query_terminal_snapshot").field("participant"),
            "terminal participant reference is required",
        )
        .expect("validated QueryTerminalSnapshot always has a participant reference")
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.participant()
            .execution_id()
            .expect("validated QueryTerminalSnapshot participant reference has execution id")
    }

    pub fn fragments(&self) -> Vec<FragmentTerminalSnapshot> {
        self.raw
            .fragments
            .iter()
            .cloned()
            .map(FragmentTerminalSnapshot::parse)
            .collect::<Result<Vec<_>, _>>()
            .expect("validated QueryTerminalSnapshot always has valid fragment snapshots")
    }

    pub fn profile_contribution_telemetry(&self) -> QueryTerminalProfileContributionTelemetry {
        QueryTerminalProfileContributionTelemetry::parse(
            self.raw
                .profile_contribution
                .clone()
                .expect("validated QueryTerminalSnapshot always has profile telemetry"),
        )
        .expect("validated QueryTerminalSnapshot always has valid profile telemetry")
    }
}

/// A validated, independently deliverable P0 terminalization proof.
#[derive(Clone, Debug, PartialEq)]
pub struct TerminalizationProof {
    raw: novarocks::TerminalizationProof,
}

impl TerminalizationProof {
    pub fn parse(raw: novarocks::TerminalizationProof) -> Result<Self, ProtocolError> {
        validate_proof(&raw, FieldPath::root("terminalization_proof"))?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::TerminalizationProof {
        &self.raw
    }

    pub const fn version(&self) -> u32 {
        self.raw.version
    }

    pub fn participant(&self) -> ParticipantAttemptRef {
        required_participant_attempt_ref(
            self.raw.participant.as_ref(),
            FieldPath::root("terminalization_proof").field("participant"),
            "terminalization proof participant reference is required",
        )
        .expect("validated TerminalizationProof always has a participant reference")
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.participant()
            .execution_id()
            .expect("validated TerminalizationProof participant reference has execution id")
    }

    pub fn fragments(&self) -> &[novarocks::TerminalizationProofFragment] {
        &self.raw.fragments
    }
}

/// A validated statement that P1 correctness evidence could not be formed.
#[derive(Clone, Debug, PartialEq)]
pub struct NegativeAttestation {
    raw: novarocks::NegativeAttestation,
}

impl NegativeAttestation {
    pub fn parse(raw: novarocks::NegativeAttestation) -> Result<Self, ProtocolError> {
        validate_attestation(&raw, FieldPath::root("negative_attestation"))?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::NegativeAttestation {
        &self.raw
    }

    pub fn participant(&self) -> ParticipantAttemptRef {
        required_participant_attempt_ref(
            self.raw.participant.as_ref(),
            FieldPath::root("negative_attestation").field("participant"),
            "negative attestation participant reference is required",
        )
        .expect("validated NegativeAttestation always has a participant reference")
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        self.participant()
            .execution_id()
            .expect("validated NegativeAttestation participant reference has execution id")
    }

    pub fn reason(&self) -> novarocks::NegativeAttestationReason {
        novarocks::NegativeAttestationReason::try_from(self.raw.reason)
            .expect("validated NegativeAttestation always has a known reason")
    }

    pub fn detail(&self) -> &str {
        &self.raw.detail
    }

    pub const fn detail_truncated(&self) -> bool {
        self.raw.detail_truncated
    }
}

/// The only participant terminal result: P0 proof plus P1/P2 snapshot, or a
/// negative attestation. It deliberately contains no FE convergence state.
#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantTerminalOutcome {
    raw: novarocks::ParticipantTerminalOutcome,
}

impl ParticipantTerminalOutcome {
    pub fn parse(raw: novarocks::ParticipantTerminalOutcome) -> Result<Self, ProtocolError> {
        match raw.outcome.as_ref().ok_or_else(|| {
            ProtocolError::new(
                FieldPath::root("participant_terminal_outcome").field("outcome"),
                ProtocolErrorKind::MissingField,
                "participant terminal outcome variant is required",
            )
        })? {
            novarocks::participant_terminal_outcome::Outcome::Proof(proof) => {
                validate_proof(
                    proof,
                    FieldPath::root("participant_terminal_outcome")
                        .field("outcome")
                        .field("proof"),
                )?;
                let snapshot = required_ref(
                    raw.snapshot.as_ref(),
                    FieldPath::root("participant_terminal_outcome").field("snapshot"),
                    "participant terminal proof requires its immutable snapshot",
                )?;
                validate_snapshot(
                    snapshot,
                    FieldPath::root("participant_terminal_outcome").field("snapshot"),
                )?;
                verify_proof_matches_snapshot(
                    proof,
                    snapshot,
                    FieldPath::root("participant_terminal_outcome"),
                )?;
            }
            novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(attestation) => {
                if raw.snapshot.is_some() {
                    return Err(ProtocolError::new(
                        FieldPath::root("participant_terminal_outcome").field("snapshot"),
                        ProtocolErrorKind::InconsistentFields,
                        "negative attestation must not carry a terminal snapshot",
                    ));
                }
                validate_attestation(
                    attestation,
                    FieldPath::root("participant_terminal_outcome")
                        .field("outcome")
                        .field("negative_attestation"),
                )?;
            }
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ParticipantTerminalOutcome {
        &self.raw
    }

    pub fn proof(&self) -> Option<TerminalizationProof> {
        let novarocks::participant_terminal_outcome::Outcome::Proof(proof) =
            self.raw.outcome.as_ref()?
        else {
            return None;
        };
        Some(
            TerminalizationProof::parse(proof.clone())
                .expect("validated ParticipantTerminalOutcome always has a valid proof"),
        )
    }

    pub fn snapshot(&self) -> Option<QueryTerminalSnapshot> {
        self.raw.snapshot.clone().map(|snapshot| {
            QueryTerminalSnapshot::parse(snapshot)
                .expect("validated ParticipantTerminalOutcome always has a valid snapshot")
        })
    }

    pub fn negative_attestation(&self) -> Option<NegativeAttestation> {
        let novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(attestation) =
            self.raw.outcome.as_ref()?
        else {
            return None;
        };
        Some(
            NegativeAttestation::parse(attestation.clone()).expect(
                "validated ParticipantTerminalOutcome always has a valid negative attestation",
            ),
        )
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        match self.raw.outcome.as_ref() {
            Some(novarocks::participant_terminal_outcome::Outcome::Proof(proof)) => {
                TerminalizationProof::parse(proof.clone())
                    .expect("validated ParticipantTerminalOutcome always has a valid proof")
                    .execution_id()
            }
            Some(novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation,
            )) => NegativeAttestation::parse(attestation.clone())
                .expect(
                    "validated ParticipantTerminalOutcome always has a valid negative attestation",
                )
                .execution_id(),
            None => unreachable!("validated ParticipantTerminalOutcome always has an outcome"),
        }
    }

    pub fn participant(&self) -> ParticipantAttemptRef {
        match self.raw.outcome.as_ref() {
            Some(novarocks::participant_terminal_outcome::Outcome::Proof(proof)) => {
                TerminalizationProof::parse(proof.clone())
                    .expect("validated ParticipantTerminalOutcome always has a valid proof")
                    .participant()
            }
            Some(novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation,
            )) => NegativeAttestation::parse(attestation.clone())
                .expect(
                    "validated ParticipantTerminalOutcome always has a valid negative attestation",
                )
                .participant(),
            None => unreachable!("validated ParticipantTerminalOutcome always has an outcome"),
        }
    }
}

/// A validated generated terminal outcome carried by a P0 proof fragment.
///
/// There is no standalone outcome message in the IDL. The proof fragment is
/// therefore the smallest generated carrier that owns its outcome, diagnostic,
/// and fragment identity without reconstructing a parallel Rust enum.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentTerminalOutcome {
    raw: novarocks::TerminalizationProofFragment,
}

impl FragmentTerminalOutcome {
    pub fn parse(raw: novarocks::TerminalizationProofFragment) -> Result<Self, ProtocolError> {
        validate_proof_fragment(&raw, FieldPath::root("terminalization_proof_fragment"))?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::TerminalizationProofFragment {
        &self.raw
    }

    pub fn fragment_instance_id(&self) -> common::UniqueId {
        self.raw
            .fragment_instance_id
            .expect("validated FragmentTerminalOutcome always has an instance id")
    }

    pub const fn backend_num(&self) -> i32 {
        self.raw.backend_num
    }

    pub fn kind(&self) -> novarocks::QueryTerminalFragmentOutcome {
        novarocks::QueryTerminalFragmentOutcome::try_from(self.raw.outcome)
            .expect("validated FragmentTerminalOutcome always has a known outcome")
    }

    pub fn is_success(&self) -> bool {
        self.kind() == novarocks::QueryTerminalFragmentOutcome::Succeeded
    }

    pub fn error_code(&self) -> &str {
        &self.raw.error_code
    }

    pub fn error_detail(&self) -> &str {
        &self.raw.error_detail
    }

    pub const fn error_detail_truncated(&self) -> bool {
        self.raw.error_detail_truncated
    }
}

/// A validated P1 fragment snapshot, useful to Backend terminal encoders.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentTerminalSnapshot {
    raw: novarocks::QueryTerminalFragmentSnapshot,
}

impl FragmentTerminalSnapshot {
    pub fn parse(raw: novarocks::QueryTerminalFragmentSnapshot) -> Result<Self, ProtocolError> {
        validate_fragment_snapshot(&raw, FieldPath::root("query_terminal_fragment_snapshot"))?;
        Ok(Self { raw })
    }

    /// Bounds UTF-8 diagnostics before validation while preserving the explicit
    /// `error_detail_truncated` P0 indicator.
    pub fn seal(mut raw: novarocks::QueryTerminalFragmentSnapshot) -> Result<Self, ProtocolError> {
        bound_fragment_diagnostics(&mut raw);
        Self::parse(raw)
    }

    pub const fn as_proto(&self) -> &novarocks::QueryTerminalFragmentSnapshot {
        &self.raw
    }

    pub fn fragment_instance_id(&self) -> common::UniqueId {
        self.raw
            .fragment_instance_id
            .expect("validated FragmentTerminalSnapshot always has an instance id")
    }

    pub const fn backend_num(&self) -> i32 {
        self.raw.backend_num
    }

    pub fn outcome(&self) -> novarocks::QueryTerminalFragmentOutcome {
        novarocks::QueryTerminalFragmentOutcome::try_from(self.raw.outcome)
            .expect("validated FragmentTerminalSnapshot always has a known outcome")
    }

    /// Returns the terminal-outcome semantic view without introducing a
    /// second, non-generated value representation.
    pub fn terminal_outcome(&self) -> FragmentTerminalOutcome {
        FragmentTerminalOutcome::parse(novarocks::TerminalizationProofFragment {
            fragment_instance_id: self.raw.fragment_instance_id,
            backend_num: self.raw.backend_num,
            outcome: self.raw.outcome,
            error_code: self.raw.error_code.clone(),
            error_detail: self.raw.error_detail.clone(),
            error_detail_truncated: self.raw.error_detail_truncated,
        })
        .expect("validated FragmentTerminalSnapshot always has a valid terminal outcome")
    }

    pub fn profile_telemetry(&self) -> FragmentTerminalProfileTelemetry {
        FragmentTerminalProfileTelemetry::parse(
            self.raw
                .profile
                .clone()
                .expect("validated FragmentTerminalSnapshot always has profile telemetry"),
        )
        .expect("validated FragmentTerminalSnapshot always has valid profile telemetry")
    }
}

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

/// A validated generated fragment-profile telemetry oneof.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentTerminalProfileTelemetry {
    raw: novarocks::FragmentTerminalProfileTelemetry,
}

impl FragmentTerminalProfileTelemetry {
    pub fn parse(raw: novarocks::FragmentTerminalProfileTelemetry) -> Result<Self, ProtocolError> {
        validate_fragment_profile_telemetry(
            &raw,
            FieldPath::root("fragment_terminal_profile_telemetry"),
        )?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::FragmentTerminalProfileTelemetry {
        &self.raw
    }

    pub fn available(&self) -> Option<&novarocks::RuntimeProfileTree> {
        let novarocks::fragment_terminal_profile_telemetry::Telemetry::Available(profile) =
            self.raw.telemetry.as_ref()?
        else {
            return None;
        };
        Some(profile)
    }

    pub fn unavailable(&self) -> Option<TerminalTelemetryUnavailable> {
        let novarocks::fragment_terminal_profile_telemetry::Telemetry::Unavailable(reason) =
            self.raw.telemetry.as_ref()?
        else {
            return None;
        };
        Some(
            TerminalTelemetryUnavailable::parse(reason.clone())
                .expect("validated fragment telemetry always has a valid reason"),
        )
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

/// Returns the reserve needed for a bounded P0 proof or negative attestation.
/// The manifest itself remains a generated Protocol message, avoiding a Core
/// dependency while retaining the former reservation calculation.
pub fn p0_max_encoded_len(
    manifest: &novarocks::ParticipantManifest,
) -> Result<usize, ProtocolError> {
    let backend = manifest.backend.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("participant_manifest").field("backend"),
            ProtocolErrorKind::MissingField,
            "terminal reservation backend is required",
        )
    })?;
    validate_backend(
        backend,
        FieldPath::root("participant_manifest").field("backend"),
    )?;
    let fixed_header: usize = 4
        + 8
        + 8
        + 8
        + 8
        + 8
        + backend
            .endpoint
            .as_ref()
            .expect("validated endpoint")
            .host
            .len()
        + 2
        + 8
        + 8
        + 32
        + 8;
    let max_outcome = 1
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES
        + 1;
    let proof_max = fixed_header.saturating_add(
        manifest
            .expected_fragment_instance_ids
            .len()
            .saturating_mul(16 + 4 + max_outcome),
    );
    let attestation_max = 8
        + 8
        + 8
        + 8
        + 8
        + backend
            .endpoint
            .as_ref()
            .expect("validated endpoint")
            .host
            .len()
        + 2
        + 8
        + 32
        + 1
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES
        + 1;
    Ok(proof_max.max(attestation_max))
}

fn validate_snapshot(
    raw: &novarocks::QueryTerminalSnapshot,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if raw.version != QUERY_TERMINAL_SNAPSHOT_VERSION_V1 {
        return Err(error(
            path.field("version"),
            ProtocolErrorKind::VersionMismatch,
            "unsupported query terminal snapshot version",
        ));
    }
    required_participant_attempt_ref(
        raw.participant.as_ref(),
        path.field("participant"),
        "terminal participant reference is required",
    )?;
    validate_sorted_unique_ids(
        &raw.fragments,
        |value| value.fragment_instance_id.as_ref(),
        path.field("fragments"),
        "query terminal snapshot contains duplicate or unsorted fragment facts",
    )?;
    for (index, fragment) in raw.fragments.iter().enumerate() {
        validate_fragment_snapshot(fragment, path.field("fragments").index(index))?;
    }
    validate_profile_contribution_telemetry(
        required_ref(
            raw.profile_contribution.as_ref(),
            path.field("profile_contribution"),
            "query terminal profile contribution telemetry is required",
        )?,
        path.field("profile_contribution"),
    )
}

fn validate_proof(
    raw: &novarocks::TerminalizationProof,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if raw.version != TERMINALIZATION_PROOF_VERSION_V1 {
        return Err(error(
            path.field("version"),
            ProtocolErrorKind::VersionMismatch,
            "unsupported terminalization proof version",
        ));
    }
    required_participant_attempt_ref(
        raw.participant.as_ref(),
        path.field("participant"),
        "terminalization proof participant reference is required",
    )?;
    validate_sorted_unique_ids(
        &raw.fragments,
        |value| value.fragment_instance_id.as_ref(),
        path.field("fragments"),
        "terminalization proof contains duplicate or unsorted fragment facts",
    )?;
    for (index, fragment) in raw.fragments.iter().enumerate() {
        validate_proof_fragment(fragment, path.field("fragments").index(index))?;
    }
    Ok(())
}

fn validate_attestation(
    raw: &novarocks::NegativeAttestation,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    required_participant_attempt_ref(
        raw.participant.as_ref(),
        path.field("participant"),
        "negative attestation participant reference is required",
    )?;
    validate_attestation_reason(raw.reason, path.field("reason"))?;
    validate_bounded_string(
        &raw.detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        path.field("detail"),
        "negative attestation detail exceeds the byte limit",
    )
}

fn validate_fragment_snapshot(
    raw: &novarocks::QueryTerminalFragmentSnapshot,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_nonzero_id(
        raw.fragment_instance_id.as_ref(),
        path.field("fragment_instance_id"),
        "terminal fragment instance id is required",
        "terminal fragment instance id must be nonzero",
    )?;
    if raw.backend_num < 0 {
        return Err(error(
            path.field("backend_num"),
            ProtocolErrorKind::OutOfRange,
            "terminal fragment backend number must be nonnegative",
        ));
    }
    validate_fragment_outcome(raw.outcome, &raw.error_code, path.clone())?;
    validate_bounded_string(
        &raw.error_code,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES,
        path.field("error_code"),
        "terminal fragment outcome code exceeds the byte limit",
    )?;
    validate_bounded_string(
        &raw.error_detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        path.field("error_detail"),
        "terminal fragment outcome detail exceeds the byte limit",
    )?;
    required_ref(
        raw.load_stats.as_ref(),
        path.field("load_stats"),
        "terminal fragment load stats are required",
    )?;
    validate_fragment_profile_telemetry(
        required_ref(
            raw.profile.as_ref(),
            path.field("profile"),
            "terminal fragment profile telemetry is required",
        )?,
        path.field("profile"),
    )?;
    Ok(())
}

fn validate_proof_fragment(
    raw: &novarocks::TerminalizationProofFragment,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_nonzero_id(
        raw.fragment_instance_id.as_ref(),
        path.field("fragment_instance_id"),
        "terminalization proof fragment instance id is required",
        "terminalization proof fragment instance id must be nonzero",
    )?;
    if raw.backend_num < 0 {
        return Err(error(
            path.field("backend_num"),
            ProtocolErrorKind::OutOfRange,
            "terminalization proof fragment backend number must be nonnegative",
        ));
    }
    validate_fragment_outcome(raw.outcome, &raw.error_code, path.clone())?;
    validate_bounded_string(
        &raw.error_code,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES,
        path.field("error_code"),
        "terminal fragment outcome code exceeds the byte limit",
    )?;
    validate_bounded_string(
        &raw.error_detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        path.field("error_detail"),
        "terminal fragment outcome detail exceeds the byte limit",
    )
}

fn validate_fragment_outcome(
    outcome: i32,
    code: &str,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    match novarocks::QueryTerminalFragmentOutcome::try_from(outcome) {
        Ok(novarocks::QueryTerminalFragmentOutcome::Succeeded)
        | Ok(novarocks::QueryTerminalFragmentOutcome::Cancelled)
        | Ok(novarocks::QueryTerminalFragmentOutcome::IncompleteDrain) => Ok(()),
        Ok(novarocks::QueryTerminalFragmentOutcome::Failed) if !code.trim().is_empty() => Ok(()),
        Ok(novarocks::QueryTerminalFragmentOutcome::Failed) => Err(error(
            path.field("error_code"),
            ProtocolErrorKind::InvalidValue,
            "invalid terminal fragment outcome",
        )),
        Ok(novarocks::QueryTerminalFragmentOutcome::Unspecified) | Err(_) => Err(error(
            path.field("outcome"),
            ProtocolErrorKind::InvalidEnum,
            "invalid terminal fragment outcome",
        )),
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

fn validate_fragment_profile_telemetry(
    raw: &novarocks::FragmentTerminalProfileTelemetry,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    use novarocks::fragment_terminal_profile_telemetry::Telemetry;
    match raw.telemetry.as_ref() {
        Some(Telemetry::Available(profile)) => {
            validate_runtime_profile(profile, path.field("telemetry").field("available"))
        }
        Some(Telemetry::Unavailable(reason)) => {
            validate_unavailable(reason, path.field("telemetry").field("unavailable"))
        }
        None => Err(error(
            path.field("telemetry"),
            ProtocolErrorKind::MissingField,
            "terminal fragment profile telemetry is required",
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

fn validate_runtime_profile(
    raw: &novarocks::RuntimeProfileTree,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let root = required_ref(
        raw.root.as_ref(),
        path.field("root"),
        "RuntimeProfileTree missing root",
    )?;
    validate_profile_node(root, path.field("root"))
}

fn validate_profile_node(
    raw: &novarocks::ProfileNode,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for (index, counter) in raw.counters.iter().enumerate() {
        match novarocks::ProfileUnit::try_from(counter.unit) {
            Ok(novarocks::ProfileUnit::Unspecified) | Err(_) => {
                return Err(error(
                    path.field("counters").index(index).field("unit"),
                    ProtocolErrorKind::InvalidEnum,
                    "invalid ProfileUnit in native runtime profile",
                ));
            }
            Ok(_) => {}
        }
    }
    for (index, child) in raw.children.iter().enumerate() {
        validate_profile_node(child, path.field("children").index(index))?;
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

fn verify_proof_matches_snapshot(
    proof: &novarocks::TerminalizationProof,
    snapshot: &novarocks::QueryTerminalSnapshot,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if proof.participant != snapshot.participant {
        return Err(error(
            path.field("outcome").field("proof"),
            ProtocolErrorKind::InconsistentFields,
            "terminalization proof does not match the immutable terminal snapshot",
        ));
    }
    if proof.fragments.len() != snapshot.fragments.len() {
        return Err(error(
            path.field("snapshot").field("fragments"),
            ProtocolErrorKind::InconsistentFields,
            "terminalization proof does not match the immutable terminal snapshot",
        ));
    }
    for (index, (proof_fragment, snapshot_fragment)) in
        proof.fragments.iter().zip(&snapshot.fragments).enumerate()
    {
        if proof_fragment.fragment_instance_id != snapshot_fragment.fragment_instance_id
            || proof_fragment.backend_num != snapshot_fragment.backend_num
            || proof_fragment.outcome != snapshot_fragment.outcome
            || proof_fragment.error_code != snapshot_fragment.error_code
            || proof_fragment.error_detail != snapshot_fragment.error_detail
            || proof_fragment.error_detail_truncated != snapshot_fragment.error_detail_truncated
        {
            return Err(error(
                path.field("outcome")
                    .field("proof")
                    .field("fragments")
                    .index(index),
                ProtocolErrorKind::InconsistentFields,
                "terminalization proof does not match the immutable terminal snapshot",
            ));
        }
    }
    Ok(())
}

fn validate_backend(
    raw: &novarocks::ParticipantBackendIdentity,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    ParticipantBackendIdentity::parse(raw.clone())
        .map(|_| ())
        .map_err(|error| {
            ProtocolError::new(
                path.append_segments(error.path().segments().iter().skip(1).cloned()),
                error.kind(),
                error.detail(),
            )
        })
}

fn required_participant_attempt_ref(
    raw: Option<&novarocks::ParticipantAttemptRef>,
    path: FieldPath,
    required: &'static str,
) -> Result<ParticipantAttemptRef, ProtocolError> {
    let raw = required_ref(raw, path, required)?;
    ParticipantAttemptRef::parse(raw.clone())
}

fn validate_attestation_reason(value: i32, path: FieldPath) -> Result<(), ProtocolError> {
    match novarocks::NegativeAttestationReason::try_from(value) {
        Ok(novarocks::NegativeAttestationReason::AttemptAborted)
        | Ok(novarocks::NegativeAttestationReason::AttemptTombstoned)
        | Ok(novarocks::NegativeAttestationReason::TerminalStateInvalid)
        | Ok(novarocks::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed)
        | Ok(novarocks::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted) => Ok(()),
        _ => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "invalid negative attestation reason",
        )),
    }
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

fn validate_nonzero_id(
    raw: Option<&common::UniqueId>,
    path: FieldPath,
    missing: &'static str,
    zero: &'static str,
) -> Result<(), ProtocolError> {
    validate_nonzero_unique_id(required_ref(raw, path.clone(), missing)?, path, zero)
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

fn validate_sorted_unique_ids<T>(
    values: &[T],
    id: impl Fn(&T) -> Option<&common::UniqueId>,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    let mut previous = None;
    for (index, value) in values.iter().enumerate() {
        let value_path = path.clone().index(index).field("fragment_instance_id");
        let id = required_ref(
            id(value),
            value_path.clone(),
            "terminal fragment instance id is required",
        )?;
        validate_nonzero_unique_id(
            id,
            value_path.clone(),
            "terminal fragment instance id must be nonzero",
        )?;
        validate_sorted_key(&mut previous, (id.hi, id.lo), value_path, detail)?;
    }
    Ok(())
}

fn validate_sorted_key<K: Ord + Copy>(
    previous: &mut Option<K>,
    current: K,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    if previous.is_some_and(|value| value >= current) {
        return Err(error(path, ProtocolErrorKind::InconsistentFields, detail));
    }
    *previous = Some(current);
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

fn validate_bounded_string(
    value: &str,
    limit: usize,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    if value.len() > limit {
        Err(error(path, ProtocolErrorKind::Capacity, detail))
    } else {
        Ok(())
    }
}

fn required_ref<'a, T>(
    raw: Option<&'a T>,
    path: FieldPath,
    detail: &'static str,
) -> Result<&'a T, ProtocolError> {
    raw.ok_or_else(|| error(path, ProtocolErrorKind::MissingField, detail))
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

fn bound_fragment_diagnostics(raw: &mut novarocks::QueryTerminalFragmentSnapshot) {
    truncate_utf8(
        &mut raw.error_code,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES,
    );
    bound_detail(&mut raw.error_detail, &mut raw.error_detail_truncated);
}

fn bound_detail(detail: &mut String, truncated: &mut bool) {
    *truncated = truncate_utf8(detail, QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
}

fn truncate_utf8(value: &mut String, max_bytes: usize) -> bool {
    if value.len() <= max_bytes {
        return false;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn execution() -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 1, lo: 2 }),
            attempt_id: 1,
        }
    }

    fn backend() -> novarocks::ParticipantBackendIdentity {
        novarocks::ParticipantBackendIdentity {
            endpoint: Some(novarocks::QueryControlEndpoint {
                host: "127.0.0.1".into(),
                port: 9030,
            }),
            process_id: Some(novarocks::BackendProcessId {
                value: vec![
                    0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1, 0x88,
                    0xad, 0x1f, 0x06,
                ],
            }),
        }
    }

    fn participant_ref() -> novarocks::ParticipantAttemptRef {
        novarocks::ParticipantAttemptRef {
            execution_id: Some(execution()),
            backend_process_id: backend().process_id,
        }
    }

    fn profile() -> novarocks::FragmentTerminalProfileTelemetry {
        novarocks::FragmentTerminalProfileTelemetry {
            telemetry: Some(
                novarocks::fragment_terminal_profile_telemetry::Telemetry::Unavailable(
                    novarocks::TerminalTelemetryUnavailable {
                        stage: "capture".into(),
                        code: "UNAVAILABLE".into(),
                    },
                ),
            ),
        }
    }

    fn fragment(id: i64) -> novarocks::QueryTerminalFragmentSnapshot {
        novarocks::QueryTerminalFragmentSnapshot {
            fragment_instance_id: Some(common::UniqueId { hi: 0, lo: id }),
            backend_num: 0,
            outcome: novarocks::QueryTerminalFragmentOutcome::Succeeded as i32,
            load_stats: Some(novarocks::QueryTerminalLoadStats::default()),
            profile: Some(profile()),
            ..Default::default()
        }
    }

    fn snapshot_raw() -> novarocks::QueryTerminalSnapshot {
        novarocks::QueryTerminalSnapshot {
            version: QUERY_TERMINAL_SNAPSHOT_VERSION_V1, participant: Some(participant_ref()), fragments: vec![fragment(1)],
            profile_contribution: Some(novarocks::QueryTerminalProfileContributionTelemetry { telemetry: Some(novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(novarocks::TerminalTelemetryUnavailable { stage: "observation".into(), code: "BUDGET_EXHAUSTED".into() })) }),
        }
    }

    #[test]
    fn terminal_snapshot_parse_round_trips_exact_generated_message() {
        let snapshot = QueryTerminalSnapshot::parse(snapshot_raw()).expect("valid P1/P2 snapshot");
        assert_eq!(
            QueryTerminalSnapshot::parse(snapshot.as_proto().clone()).expect("parse"),
            snapshot
        );
    }

    #[test]
    fn terminal_values_reject_unknown_enums() {
        let mut raw = snapshot_raw();
        raw.fragments[0].outcome = 99;
        let error = QueryTerminalSnapshot::parse(raw).expect_err("unknown fragment outcome");
        assert_eq!(error.detail(), "invalid terminal fragment outcome");
    }

    #[test]
    fn terminal_validation_reports_repeated_field_paths() {
        let mut raw = snapshot_raw();
        raw.fragments[0].backend_num = -1;

        let error = QueryTerminalSnapshot::parse(raw).expect_err("negative backend number");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "query_terminal_snapshot.fragments[0].backend_num"
        );
    }

    #[test]
    fn terminal_validation_reports_nested_outcome_paths() {
        let error = ParticipantTerminalOutcome::parse(novarocks::ParticipantTerminalOutcome {
            outcome: Some(novarocks::participant_terminal_outcome::Outcome::Proof(
                novarocks::TerminalizationProof {
                    version: 99,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .expect_err("unsupported nested proof version");

        assert_eq!(error.kind(), ProtocolErrorKind::VersionMismatch);
        assert_eq!(
            error.path().to_string(),
            "participant_terminal_outcome.outcome.proof.version"
        );
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
    fn bounds_utf8_details_without_touching_p1_or_p2() {
        let mut raw = fragment(1);
        raw.outcome = novarocks::QueryTerminalFragmentOutcome::Failed as i32;
        raw.error_code = "C".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES + 1);
        raw.error_detail = "测".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
        let bounded = FragmentTerminalSnapshot::seal(raw).expect("bounded fragment");
        assert_eq!(
            bounded.as_proto().error_code.len(),
            QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES
        );
        assert!(
            bounded.as_proto().error_detail.len()
                <= QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES
        );
        assert!(bounded.as_proto().error_detail_truncated);
    }

    #[test]
    fn p0_proof_remains_independent_of_p1_and_p2() {
        let snapshot = QueryTerminalSnapshot::parse(snapshot_raw()).expect("snapshot");
        let raw = novarocks::TerminalizationProof {
            version: TERMINALIZATION_PROOF_VERSION_V1,
            participant: Some(participant_ref()),
            fragments: vec![novarocks::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 0, lo: 1 }),
                backend_num: 0,
                outcome: 1,
                ..Default::default()
            }],
        };
        let proof = TerminalizationProof::parse(raw).expect("P0 proof");
        assert_eq!(snapshot.execution_id().query_id().high(), 1);
        assert!(snapshot.participant().backend_process_id().is_ok());
        assert_eq!(snapshot.fragments()[0].fragment_instance_id().lo, 1);
        assert_eq!(snapshot.fragments()[0].backend_num(), 0);
        assert_eq!(
            snapshot.fragments()[0].outcome(),
            novarocks::QueryTerminalFragmentOutcome::Succeeded
        );

        assert_eq!(proof.execution_id(), snapshot.execution_id());
        assert_eq!(proof.participant(), snapshot.participant());
        assert_eq!(proof.fragments().len(), 1);

        let outcome = ParticipantTerminalOutcome::parse(novarocks::ParticipantTerminalOutcome {
            outcome: Some(novarocks::participant_terminal_outcome::Outcome::Proof(
                proof.as_proto().clone(),
            )),
            snapshot: Some(snapshot.as_proto().clone()),
        })
        .expect("outcome");
        assert!(outcome.proof().is_some());
        assert!(outcome.snapshot().is_some());
        assert!(outcome.negative_attestation().is_none());
        assert_eq!(outcome.execution_id(), snapshot.execution_id());
        assert_eq!(outcome.participant(), snapshot.participant());
        let decoded = ParticipantTerminalOutcome::parse(
            novarocks::ParticipantTerminalOutcome::decode(
                outcome.as_proto().encode_to_vec().as_slice(),
            )
            .expect("generated outcome round trip"),
        )
        .expect("redecoded outcome");
        assert_eq!(outcome, decoded);

        let mut changed_raw = outcome.as_proto().clone();
        let snapshot = changed_raw
            .snapshot
            .as_mut()
            .expect("proof outcome has a snapshot");
        let telemetry = snapshot
            .profile_contribution
            .as_mut()
            .expect("snapshot has profile contribution telemetry");
        let novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
            unavailable,
        ) = telemetry.telemetry.as_mut().expect("profile telemetry")
        else {
            panic!("fixture has unavailable profile telemetry");
        };
        unavailable.code.push('X');
        let changed = ParticipantTerminalOutcome::parse(changed_raw).expect("changed outcome");
        assert_ne!(outcome, changed);
    }

    #[test]
    fn p0_p1_and_p2_negative_fixtures_fail_closed() {
        let invalid_p0 = novarocks::TerminalizationProof {
            version: TERMINALIZATION_PROOF_VERSION_V1,
            participant: Some(participant_ref()),
            fragments: vec![novarocks::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 0, lo: 1 }),
                backend_num: 0,
                outcome: 99,
                ..Default::default()
            }],
        };
        assert!(TerminalizationProof::parse(invalid_p0).is_err());

        let mut missing_p2 = snapshot_raw();
        missing_p2.profile_contribution = None;
        assert!(QueryTerminalSnapshot::parse(missing_p2).is_err());

        let attestation = NegativeAttestation::parse(novarocks::NegativeAttestation {
            participant: Some(participant_ref()),
            reason: novarocks::NegativeAttestationReason::AttemptAborted as i32,
            detail: "aborted".into(),
            ..Default::default()
        })
        .expect("negative attestation");
        assert_eq!(attestation.execution_id().query_id().low(), 2);
        assert!(attestation.participant().backend_process_id().is_ok());
        assert_eq!(
            attestation.reason(),
            novarocks::NegativeAttestationReason::AttemptAborted
        );
        assert_eq!(attestation.detail(), "aborted");
        assert!(!attestation.detail_truncated());
        assert!(
            ParticipantTerminalOutcome::parse(novarocks::ParticipantTerminalOutcome {
                outcome: Some(
                    novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                        attestation.as_proto().clone(),
                    ),
                ),
                snapshot: Some(
                    QueryTerminalSnapshot::parse(snapshot_raw())
                        .expect("P1")
                        .as_proto()
                        .clone()
                ),
            })
            .is_err()
        );
    }

    #[test]
    fn profile_contribution_retains_wire_enum_validation() {
        let invalid = novarocks::QueryTerminalProfileContributionV1 {
            version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
            channels: vec![novarocks::QueryTerminalRuntimeFilterChannelV1 {
                channel_binding_id: 1,
                channel_id: 1,
                install_state: 99,
                terminal_state: 1,
                ..Default::default()
            }],
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
}
