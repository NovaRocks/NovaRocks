//! Validated participant-local terminal lifecycle values.
//!
//! Each public validated value owns exactly one generated protobuf message.
//! The Backend encodes runtime profiles and sink facts into those messages;
//! this module never depends on their runtime representations.

use std::collections::BTreeSet;

use crate::{canonical, common, novarocks};

use super::{
    error::ContractError,
    identity::QueryExecutionId,
    manifest::{ParticipantBackendIdentity, ParticipantManifestDigest},
};

pub const QUERY_TERMINAL_SNAPSHOT_VERSION_V1: u32 = 1;
pub const PARTICIPANT_TERMINAL_OUTCOME_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES: usize = 128;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES: usize = 4096;
pub const QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES: usize = 16_384;
pub const QUERY_TERMINAL_STATISTICS_PAYLOAD_MAX_BYTES: usize = 64 * 1024;

const SNAPSHOT_DOMAIN: &[u8] = b"novarocks.query-lifecycle.terminal-snapshot.v1\0";
const PROOF_DOMAIN: &[u8] = b"novarocks.query-lifecycle.terminalization-proof.v1\0";
const ATTESTATION_DOMAIN: &[u8] = b"novarocks.query-lifecycle.negative-attestation.v1\0";
const TERMINALIZATION_PROOF_VERSION_V1: u32 = 1;

/// A validated P1/P2 participant terminal snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSnapshot {
    raw: novarocks::QueryTerminalSnapshot,
}

impl QueryTerminalSnapshot {
    pub fn parse(raw: novarocks::QueryTerminalSnapshot) -> Result<Self, ContractError> {
        validate_snapshot(&raw)?;
        verify_digest("novarocks.QueryTerminalSnapshot", &raw.digest, || {
            let mut projected = raw.clone();
            projected.digest.clear();
            canonical::digest_message(
                SNAPSHOT_DOMAIN,
                "novarocks.QueryTerminalSnapshot",
                &projected,
            )
        })?;
        Ok(Self { raw })
    }

    /// Validates, computes, and installs the canonical snapshot digest.
    pub fn seal(mut raw: novarocks::QueryTerminalSnapshot) -> Result<Self, ContractError> {
        raw.fragments.sort_by_key(fragment_key);
        raw.digest = vec![0; 32];
        validate_snapshot(&raw)?;
        raw.digest.clear();
        raw.digest =
            canonical_digest(SNAPSHOT_DOMAIN, "novarocks.QueryTerminalSnapshot", &raw)?.to_vec();
        Self::parse(raw)
    }

    pub const fn as_proto(&self) -> &novarocks::QueryTerminalSnapshot {
        &self.raw
    }

    pub const fn version(&self) -> u32 {
        self.raw.version
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(
            self.raw.execution_id.as_ref(),
            "terminal execution id is required",
        )
        .expect("validated QueryTerminalSnapshot always has an execution id")
    }

    pub fn backend(&self) -> ParticipantBackendIdentity {
        required_backend(
            self.raw.backend.as_ref(),
            "terminal backend identity is required",
        )
        .expect("validated QueryTerminalSnapshot always has a backend identity")
    }

    pub fn init_digest(&self) -> ParticipantManifestDigest {
        ParticipantManifestDigest::try_from_slice(&self.raw.init_digest)
            .expect("validated QueryTerminalSnapshot always has an init digest")
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

    pub fn digest(&self) -> [u8; 32] {
        digest_array(&self.raw.digest).expect("validated terminal snapshot digest")
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
    pub fn parse(raw: novarocks::TerminalizationProof) -> Result<Self, ContractError> {
        validate_proof(&raw)?;
        verify_digest("novarocks.TerminalizationProof", &raw.digest, || {
            let mut projected = raw.clone();
            projected.digest.clear();
            canonical::digest_message(PROOF_DOMAIN, "novarocks.TerminalizationProof", &projected)
        })?;
        Ok(Self { raw })
    }

    pub fn seal(mut raw: novarocks::TerminalizationProof) -> Result<Self, ContractError> {
        raw.fragments.sort_by_key(proof_fragment_key);
        raw.digest = vec![0; 32];
        validate_proof(&raw)?;
        raw.digest.clear();
        raw.digest =
            canonical_digest(PROOF_DOMAIN, "novarocks.TerminalizationProof", &raw)?.to_vec();
        Self::parse(raw)
    }

    pub const fn as_proto(&self) -> &novarocks::TerminalizationProof {
        &self.raw
    }

    pub const fn version(&self) -> u32 {
        self.raw.version
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(
            self.raw.execution_id.as_ref(),
            "terminalization proof execution id is required",
        )
        .expect("validated TerminalizationProof always has an execution id")
    }

    pub fn backend(&self) -> ParticipantBackendIdentity {
        required_backend(
            self.raw.backend.as_ref(),
            "terminalization proof backend is required",
        )
        .expect("validated TerminalizationProof always has a backend identity")
    }

    pub fn init_digest(&self) -> ParticipantManifestDigest {
        ParticipantManifestDigest::try_from_slice(&self.raw.init_digest)
            .expect("validated TerminalizationProof always has an init digest")
    }

    pub fn fragments(&self) -> &[novarocks::TerminalizationProofFragment] {
        &self.raw.fragments
    }

    pub fn digest(&self) -> [u8; 32] {
        digest_array(&self.raw.digest).expect("validated terminalization proof digest")
    }
}

/// A validated statement that P1 correctness evidence could not be formed.
#[derive(Clone, Debug, PartialEq)]
pub struct NegativeAttestation {
    raw: novarocks::NegativeAttestation,
}

impl NegativeAttestation {
    pub fn parse(raw: novarocks::NegativeAttestation) -> Result<Self, ContractError> {
        validate_attestation(&raw)?;
        verify_digest("novarocks.NegativeAttestation", &raw.digest, || {
            let mut projected = raw.clone();
            projected.digest.clear();
            canonical::digest_message(
                ATTESTATION_DOMAIN,
                "novarocks.NegativeAttestation",
                &projected,
            )
        })?;
        Ok(Self { raw })
    }

    pub fn seal(mut raw: novarocks::NegativeAttestation) -> Result<Self, ContractError> {
        bound_detail(&mut raw.detail, &mut raw.detail_truncated);
        raw.digest = vec![0; 32];
        validate_attestation(&raw)?;
        raw.digest.clear();
        raw.digest =
            canonical_digest(ATTESTATION_DOMAIN, "novarocks.NegativeAttestation", &raw)?.to_vec();
        Self::parse(raw)
    }

    pub const fn as_proto(&self) -> &novarocks::NegativeAttestation {
        &self.raw
    }

    pub fn execution_id(&self) -> QueryExecutionId {
        required_execution_id(
            self.raw.execution_id.as_ref(),
            "negative attestation execution id is required",
        )
        .expect("validated NegativeAttestation always has an execution id")
    }

    pub fn backend(&self) -> ParticipantBackendIdentity {
        required_backend(
            self.raw.backend.as_ref(),
            "negative attestation backend is required",
        )
        .expect("validated NegativeAttestation always has a backend identity")
    }

    pub fn init_digest(&self) -> ParticipantManifestDigest {
        ParticipantManifestDigest::try_from_slice(&self.raw.init_digest)
            .expect("validated NegativeAttestation always has an init digest")
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

    pub fn digest(&self) -> [u8; 32] {
        digest_array(&self.raw.digest).expect("validated negative attestation digest")
    }
}

/// The only participant terminal result: P0 proof plus P1/P2 snapshot, or a
/// negative attestation. It deliberately contains no FE convergence state.
#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantTerminalOutcome {
    raw: novarocks::ParticipantTerminalOutcome,
}

impl ParticipantTerminalOutcome {
    pub fn parse(raw: novarocks::ParticipantTerminalOutcome) -> Result<Self, ContractError> {
        match raw.outcome.as_ref().ok_or_else(|| {
            ContractError::invalid_value("participant terminal outcome variant is required")
        })? {
            novarocks::participant_terminal_outcome::Outcome::Proof(proof) => {
                let proof = TerminalizationProof::parse(proof.clone())?;
                let snapshot =
                    QueryTerminalSnapshot::parse(raw.snapshot.clone().ok_or_else(|| {
                        ContractError::invalid_value(
                            "participant terminal proof requires its immutable snapshot",
                        )
                    })?)?;
                verify_proof_matches_snapshot(proof.as_proto(), snapshot.as_proto())?;
            }
            novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(attestation) => {
                if raw.snapshot.is_some() {
                    return Err(ContractError::invalid_value(
                        "negative attestation must not carry a terminal snapshot",
                    ));
                }
                NegativeAttestation::parse(attestation.clone())?;
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

    pub fn backend(&self) -> ParticipantBackendIdentity {
        match self.raw.outcome.as_ref() {
            Some(novarocks::participant_terminal_outcome::Outcome::Proof(proof)) => {
                TerminalizationProof::parse(proof.clone())
                    .expect("validated ParticipantTerminalOutcome always has a valid proof")
                    .backend()
            }
            Some(novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation,
            )) => NegativeAttestation::parse(attestation.clone())
                .expect(
                    "validated ParticipantTerminalOutcome always has a valid negative attestation",
                )
                .backend(),
            None => unreachable!("validated ParticipantTerminalOutcome always has an outcome"),
        }
    }

    pub fn init_digest(&self) -> ParticipantManifestDigest {
        match self.raw.outcome.as_ref() {
            Some(novarocks::participant_terminal_outcome::Outcome::Proof(proof)) => {
                TerminalizationProof::parse(proof.clone())
                    .expect("validated ParticipantTerminalOutcome always has a valid proof")
                    .init_digest()
            }
            Some(novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation,
            )) => NegativeAttestation::parse(attestation.clone())
                .expect(
                    "validated ParticipantTerminalOutcome always has a valid negative attestation",
                )
                .init_digest(),
            None => unreachable!("validated ParticipantTerminalOutcome always has an outcome"),
        }
    }

    pub fn digest(&self) -> [u8; 32] {
        match self.raw.outcome.as_ref() {
            Some(novarocks::participant_terminal_outcome::Outcome::Proof(proof)) => {
                TerminalizationProof::parse(proof.clone())
                    .expect("validated ParticipantTerminalOutcome always has a valid proof")
                    .digest()
            }
            Some(novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation,
            )) => NegativeAttestation::parse(attestation.clone())
                .expect(
                    "validated ParticipantTerminalOutcome always has a valid negative attestation",
                )
                .digest(),
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
    pub fn parse(raw: novarocks::TerminalizationProofFragment) -> Result<Self, ContractError> {
        validate_proof_fragment(&raw)?;
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
    pub fn parse(raw: novarocks::QueryTerminalFragmentSnapshot) -> Result<Self, ContractError> {
        validate_fragment_snapshot(&raw)?;
        Ok(Self { raw })
    }

    /// Bounds UTF-8 diagnostics before validation while preserving the explicit
    /// `error_detail_truncated` P0 indicator.
    pub fn seal(mut raw: novarocks::QueryTerminalFragmentSnapshot) -> Result<Self, ContractError> {
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
    ) -> Result<Self, ContractError> {
        validate_profile_contribution(&raw)?;
        Ok(Self { raw })
    }

    /// Establishes the wire's required key ordering before validation.
    pub fn seal(
        mut raw: novarocks::QueryTerminalProfileContributionV1,
    ) -> Result<Self, ContractError> {
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
    pub fn parse(raw: novarocks::TerminalTelemetryUnavailable) -> Result<Self, ContractError> {
        validate_unavailable(&raw)?;
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
    pub fn parse(raw: novarocks::FragmentTerminalProfileTelemetry) -> Result<Self, ContractError> {
        validate_fragment_profile_telemetry(&raw)?;
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
    ) -> Result<Self, ContractError> {
        validate_profile_contribution_telemetry(&raw)?;
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
) -> Result<usize, ContractError> {
    let backend = manifest
        .backend
        .as_ref()
        .ok_or_else(|| ContractError::invalid_value("terminal reservation backend is required"))?;
    validate_backend(backend)?;
    let fixed_header = 4
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

fn validate_snapshot(raw: &novarocks::QueryTerminalSnapshot) -> Result<(), ContractError> {
    if raw.version != QUERY_TERMINAL_SNAPSHOT_VERSION_V1 {
        return Err(ContractError::version_mismatch(
            "unsupported query terminal snapshot version",
        ));
    }
    validate_execution(
        raw.execution_id.as_ref(),
        "terminal execution id is required",
    )?;
    validate_backend(
        raw.backend
            .as_ref()
            .ok_or_else(|| ContractError::invalid_value("terminal backend identity is required"))?,
    )?;
    require_digest_len(
        &raw.init_digest,
        "participant manifest digest must be 32 bytes",
    )?;
    require_digest_len(
        &raw.digest,
        "query terminal snapshot digest must be 32 bytes",
    )?;
    validate_sorted_unique_ids(
        &raw.fragments,
        |value| value.fragment_instance_id.as_ref(),
        "query terminal snapshot contains duplicate or unsorted fragment facts",
    )?;
    for fragment in &raw.fragments {
        validate_fragment_snapshot(fragment)?;
    }
    validate_profile_contribution_telemetry(raw.profile_contribution.as_ref().ok_or_else(|| {
        ContractError::invalid_value("query terminal profile contribution telemetry is required")
    })?)
}

fn fragment_key(value: &novarocks::QueryTerminalFragmentSnapshot) -> (i64, i64) {
    value
        .fragment_instance_id
        .as_ref()
        .map_or((i64::MIN, i64::MIN), |id| (id.hi, id.lo))
}

fn proof_fragment_key(value: &novarocks::TerminalizationProofFragment) -> (i64, i64) {
    value
        .fragment_instance_id
        .as_ref()
        .map_or((i64::MIN, i64::MIN), |id| (id.hi, id.lo))
}

fn validate_proof(raw: &novarocks::TerminalizationProof) -> Result<(), ContractError> {
    if raw.version != TERMINALIZATION_PROOF_VERSION_V1 {
        return Err(ContractError::version_mismatch(
            "unsupported terminalization proof version",
        ));
    }
    validate_execution(
        raw.execution_id.as_ref(),
        "terminalization proof execution id is required",
    )?;
    validate_backend(raw.backend.as_ref().ok_or_else(|| {
        ContractError::invalid_value("terminalization proof backend is required")
    })?)?;
    require_digest_len(
        &raw.init_digest,
        "participant manifest digest must be 32 bytes",
    )?;
    require_digest_len(
        &raw.digest,
        "query terminal snapshot digest must be 32 bytes",
    )?;
    validate_sorted_unique_ids(
        &raw.fragments,
        |value| value.fragment_instance_id.as_ref(),
        "terminalization proof contains duplicate or unsorted fragment facts",
    )?;
    for fragment in &raw.fragments {
        validate_proof_fragment(fragment)?;
    }
    Ok(())
}

fn validate_attestation(raw: &novarocks::NegativeAttestation) -> Result<(), ContractError> {
    validate_execution(
        raw.execution_id.as_ref(),
        "negative attestation execution id is required",
    )?;
    validate_backend(raw.backend.as_ref().ok_or_else(|| {
        ContractError::invalid_value("negative attestation backend is required")
    })?)?;
    require_digest_len(
        &raw.init_digest,
        "participant manifest digest must be 32 bytes",
    )?;
    require_digest_len(
        &raw.digest,
        "query terminal snapshot digest must be 32 bytes",
    )?;
    validate_attestation_reason(raw.reason)?;
    validate_bounded_string(
        &raw.detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        "negative attestation detail exceeds the byte limit",
    )
}

fn validate_fragment_snapshot(
    raw: &novarocks::QueryTerminalFragmentSnapshot,
) -> Result<(), ContractError> {
    validate_nonzero_id(
        raw.fragment_instance_id.as_ref(),
        "terminal fragment instance id is required",
        "terminal fragment instance id must be nonzero",
    )?;
    if raw.backend_num < 0 {
        return Err(ContractError::invalid_value(
            "terminal fragment backend number must be nonnegative",
        ));
    }
    validate_fragment_outcome(raw.outcome, &raw.error_code, &raw.error_detail)?;
    validate_bounded_string(
        &raw.error_code,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES,
        "terminal fragment outcome code exceeds the byte limit",
    )?;
    validate_bounded_string(
        &raw.error_detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        "terminal fragment outcome detail exceeds the byte limit",
    )?;
    if raw.load_stats.is_none() {
        return Err(ContractError::invalid_value(
            "terminal fragment load stats are required",
        ));
    }
    validate_fragment_profile_telemetry(raw.profile.as_ref().ok_or_else(|| {
        ContractError::invalid_value("terminal fragment profile telemetry is required")
    })?)?;
    if raw.statistics_payload.len() > QUERY_TERMINAL_STATISTICS_PAYLOAD_MAX_BYTES {
        return Err(ContractError::capacity(
            "terminal fragment statistics payload exceeds the connector statistics limit",
        ));
    }
    Ok(())
}

fn validate_proof_fragment(
    raw: &novarocks::TerminalizationProofFragment,
) -> Result<(), ContractError> {
    validate_nonzero_id(
        raw.fragment_instance_id.as_ref(),
        "terminalization proof fragment instance id is required",
        "terminalization proof fragment instance id must be nonzero",
    )?;
    if raw.backend_num < 0 {
        return Err(ContractError::invalid_value(
            "terminalization proof fragment backend number must be nonnegative",
        ));
    }
    validate_fragment_outcome(raw.outcome, &raw.error_code, &raw.error_detail)?;
    validate_bounded_string(
        &raw.error_code,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES,
        "terminal fragment outcome code exceeds the byte limit",
    )?;
    validate_bounded_string(
        &raw.error_detail,
        QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES,
        "terminal fragment outcome detail exceeds the byte limit",
    )
}

fn validate_fragment_outcome(outcome: i32, code: &str, _detail: &str) -> Result<(), ContractError> {
    match novarocks::QueryTerminalFragmentOutcome::try_from(outcome) {
        Ok(novarocks::QueryTerminalFragmentOutcome::Succeeded)
        | Ok(novarocks::QueryTerminalFragmentOutcome::Cancelled)
        | Ok(novarocks::QueryTerminalFragmentOutcome::IncompleteDrain) => Ok(()),
        Ok(novarocks::QueryTerminalFragmentOutcome::Failed) if !code.trim().is_empty() => Ok(()),
        Ok(novarocks::QueryTerminalFragmentOutcome::Failed)
        | Ok(novarocks::QueryTerminalFragmentOutcome::Unspecified)
        | Err(_) => Err(ContractError::invalid_value(
            "invalid terminal fragment outcome",
        )),
    }
}

fn validate_profile_contribution_telemetry(
    raw: &novarocks::QueryTerminalProfileContributionTelemetry,
) -> Result<(), ContractError> {
    use novarocks::query_terminal_profile_contribution_telemetry::Telemetry;
    match raw.telemetry.as_ref() {
        Some(Telemetry::Available(value)) => validate_profile_contribution(value),
        Some(Telemetry::Unavailable(reason)) => validate_unavailable(reason),
        None => Err(ContractError::invalid_value(
            "query terminal profile contribution telemetry is required",
        )),
    }
}

fn validate_fragment_profile_telemetry(
    raw: &novarocks::FragmentTerminalProfileTelemetry,
) -> Result<(), ContractError> {
    use novarocks::fragment_terminal_profile_telemetry::Telemetry;
    match raw.telemetry.as_ref() {
        Some(Telemetry::Available(profile)) => validate_runtime_profile(profile),
        Some(Telemetry::Unavailable(reason)) => validate_unavailable(reason),
        None => Err(ContractError::invalid_value(
            "terminal fragment profile telemetry is required",
        )),
    }
}

fn validate_unavailable(
    raw: &novarocks::TerminalTelemetryUnavailable,
) -> Result<(), ContractError> {
    if raw.stage.trim().is_empty() || raw.code.trim().is_empty() {
        return Err(ContractError::invalid_value(
            "terminal telemetry unavailable stage and code must be nonempty",
        ));
    }
    Ok(())
}

fn validate_runtime_profile(raw: &novarocks::RuntimeProfileTree) -> Result<(), ContractError> {
    let root = raw
        .root
        .as_ref()
        .ok_or_else(|| ContractError::invalid_value("RuntimeProfileTree missing root"))?;
    validate_profile_node(root)
}

fn validate_profile_node(raw: &novarocks::ProfileNode) -> Result<(), ContractError> {
    for counter in &raw.counters {
        match novarocks::ProfileUnit::try_from(counter.unit) {
            Ok(novarocks::ProfileUnit::Unspecified) | Err(_) => {
                return Err(ContractError::invalid_value(
                    "invalid ProfileUnit in native runtime profile",
                ));
            }
            Ok(_) => {}
        }
    }
    for child in &raw.children {
        validate_profile_node(child)?;
    }
    Ok(())
}

fn validate_profile_contribution(
    raw: &novarocks::QueryTerminalProfileContributionV1,
) -> Result<(), ContractError> {
    if raw.version != QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1 {
        return Err(ContractError::version_mismatch(
            "unsupported query terminal profile contribution version",
        ));
    }
    for (label, len) in [
        ("channel", raw.channels.len()),
        ("producer stream", raw.producer_streams.len()),
        ("transport route", raw.transport_routes.len()),
        ("consumer", raw.consumers.len()),
    ] {
        if len > QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES {
            return Err(ContractError::capacity(format!(
                "terminal runtime-filter {label} section exceeds the cardinality limit"
            )));
        }
    }
    validate_channels(&raw.channels)?;
    validate_producer_streams(&raw.producer_streams, &raw.channels)?;
    validate_transport_routes(&raw.transport_routes, &raw.channels)?;
    validate_consumers(&raw.consumers, &raw.channels)
}

fn validate_channels(
    values: &[novarocks::QueryTerminalRuntimeFilterChannelV1],
) -> Result<(), ContractError> {
    let mut previous = None;
    for value in values {
        let key = (value.channel_binding_id, value.channel_id);
        validate_channel_key(key)?;
        require_known_enum(
            value.install_state,
            novarocks::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed as i32,
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
                return Err(ContractError::invalid_value(
                    "invalid terminal runtime-filter channel terminal state",
                ));
            }
        }
        validate_optional_nonzero(
            value.latest_published_logical_version,
            "terminal runtime-filter latest published logical version must be nonzero",
        )?;
        if (value.published_count == 0) != value.latest_published_logical_version.is_none() {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter published count and latest version disagree",
            ));
        }
        // Terminal state is the joined semantic outcome, while these counters
        // retain every observed event. In particular, AnyOf completion joins
        // with IncompleteCoverage as Completed without erasing either event.
        let valid = match value.terminal_state {
            1 => {
                value.completed_count == 0
                    && value.unavailable_count == 0
                    && value.cancelled_count == 0
            }
            2 => value.completed_count != 0 && value.cancelled_count == 0,
            3 => {
                value.completed_count == 0
                    && value.unavailable_count != 0
                    && value.cancelled_count == 0
            }
            4 => {
                value.completed_count == 0
                    && value.unavailable_count == 0
                    && value.cancelled_count != 0
            }
            _ => false,
        };
        if !valid {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter channel state and terminal counters disagree",
            ));
        }
        validate_sorted_key(
            &mut previous,
            key,
            "query terminal profile contribution contains duplicate or unsorted channel identity",
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
    channels: &[novarocks::QueryTerminalRuntimeFilterChannelV1],
) -> Result<(), ContractError> {
    let known = channel_keys(channels);
    let mut previous = None;
    for value in values {
        let channel = (value.channel_binding_id, value.channel_id);
        validate_channel_reference(channel, &known)?;
        let id = value
            .producer_fragment_instance_id
            .as_ref()
            .ok_or_else(|| {
                ContractError::invalid_value(
                    "terminal runtime-filter producer fragment instance id is required",
                )
            })?;
        validate_nonzero_unique_id(
            id,
            "terminal runtime-filter producer fragment instance id must be nonzero",
        )?;
        if (value.accepted_count == 0) != value.latest_accepted_sequence.is_none() {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter accepted count and latest sequence disagree",
            ));
        }
        checked_sum(
            [
                value.accepted_count,
                value.duplicate_count,
                value.stale_count,
                value.conflict_count,
                value.resource_limit_count,
            ],
            "terminal runtime-filter producer counters overflow",
        )?;
        validate_sorted_key(
            &mut previous,
            (channel, id.hi, id.lo, value.partition_id),
            "query terminal profile contribution contains duplicate or unsorted producer stream identity",
        )?;
    }
    Ok(())
}

fn validate_transport_routes(
    values: &[novarocks::QueryTerminalRuntimeFilterTransportRouteV1],
    channels: &[novarocks::QueryTerminalRuntimeFilterChannelV1],
) -> Result<(), ContractError> {
    let known = channel_keys(channels);
    let mut previous = None;
    for value in values {
        let channel = (value.channel_binding_id, value.channel_id);
        validate_channel_reference(channel, &known)?;
        if value.route_edge_id == 0 {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter route edge id must be nonzero",
            ));
        }
        let delivery_count = value
            .sent_count
            .checked_add(value.retried_count)
            .ok_or_else(|| {
                ContractError::invalid_value(
                    "terminal runtime-filter transport delivery counter overflow",
                )
            })?;
        let delivery_bytes = value
            .sent_bytes
            .checked_add(value.retried_bytes)
            .ok_or_else(|| {
                ContractError::invalid_value(
                    "terminal runtime-filter transport delivery bytes overflow",
                )
            })?;
        if value.acked_count > delivery_count || value.acked_bytes > delivery_bytes {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter transport acknowledgement exceeds delivery totals",
            ));
        }
        validate_sorted_key(
            &mut previous,
            (channel, value.route_edge_id),
            "query terminal profile contribution contains duplicate or unsorted transport route identity",
        )?;
    }
    Ok(())
}

fn validate_consumers(
    values: &[novarocks::QueryTerminalRuntimeFilterConsumerV1],
    channels: &[novarocks::QueryTerminalRuntimeFilterChannelV1],
) -> Result<(), ContractError> {
    let known = channel_keys(channels);
    let mut previous = None;
    for value in values {
        let channel = (value.channel_binding_id, value.channel_id);
        validate_channel_reference(channel, &known)?;
        if value.consumer_binding_id == 0 {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter consumer binding id must be nonzero",
            ));
        }
        let id = value.fragment_instance_id.as_ref().ok_or_else(|| {
            ContractError::invalid_value(
                "terminal runtime-filter consumer fragment instance id is required",
            )
        })?;
        validate_nonzero_unique_id(
            id,
            "terminal runtime-filter consumer fragment instance id must be nonzero",
        )?;
        validate_optional_nonzero(
            value.latest_delivered_logical_version,
            "terminal runtime-filter latest delivered logical version must be nonzero",
        )?;
        validate_optional_nonzero(
            value.latest_applied_logical_version,
            "terminal runtime-filter latest applied logical version must be nonzero",
        )?;
        if let Some(applied) = value.latest_applied_logical_version {
            let delivered = value.latest_delivered_logical_version.ok_or_else(|| {
                ContractError::invalid_value(
                    "terminal runtime-filter applied version requires a delivered version",
                )
            })?;
            if applied > delivered {
                return Err(ContractError::invalid_value(
                    "terminal runtime-filter applied version exceeds delivered version",
                ));
            }
        }
        match novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::try_from(value.subscription_terminal) {
            Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed)
            | Ok(novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact) => {}
            _ => return Err(ContractError::invalid_value("invalid terminal runtime-filter subscription terminal state")),
        }
        if value.output_rows > value.input_rows
            || (value.row_evaluations == 0 && (value.input_rows != 0 || value.output_rows != 0))
        {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter row counters are inconsistent",
            ));
        }
        let evaluated = value
            .scan_kept
            .checked_add(value.scan_pruned)
            .ok_or_else(|| {
                ContractError::invalid_value(
                    "terminal runtime-filter scan evaluated counters overflow",
                )
            })?;
        let reasons = value.scan_not_evaluated_reasons.as_ref().ok_or_else(|| {
            ContractError::invalid_value(
                "terminal runtime-filter scan not-evaluated counters are required",
            )
        })?;
        if evaluated != value.scan_evaluated
            || checked_sum(
                [
                    reasons.unit_facts_missing,
                    reasons.column_facts_missing,
                    reasons.data_type_unsupported,
                    reasons.predicate_capability_unsupported,
                    reasons.resource_unavailable,
                    reasons.snapshot_unavailable,
                    reasons.snapshot_timed_out,
                    reasons.snapshot_not_published,
                ],
                "terminal runtime-filter scan not-evaluated counters overflow",
            )? != value.scan_not_evaluated
        {
            return Err(ContractError::invalid_value(
                "terminal runtime-filter scan counters are inconsistent",
            ));
        }
        validate_sorted_key(
            &mut previous,
            (channel, value.consumer_binding_id, id.hi, id.lo),
            "query terminal profile contribution contains duplicate or unsorted consumer identity",
        )?;
    }
    Ok(())
}

fn verify_proof_matches_snapshot(
    proof: &novarocks::TerminalizationProof,
    snapshot: &novarocks::QueryTerminalSnapshot,
) -> Result<(), ContractError> {
    if proof.execution_id != snapshot.execution_id
        || proof.backend != snapshot.backend
        || proof.init_digest != snapshot.init_digest
    {
        return Err(ContractError::conflict(
            "terminalization proof does not match the immutable terminal snapshot",
        ));
    }
    if proof.fragments.len() != snapshot.fragments.len() {
        return Err(ContractError::conflict(
            "terminalization proof does not match the immutable terminal snapshot",
        ));
    }
    for (proof_fragment, snapshot_fragment) in proof.fragments.iter().zip(&snapshot.fragments) {
        if proof_fragment.fragment_instance_id != snapshot_fragment.fragment_instance_id
            || proof_fragment.backend_num != snapshot_fragment.backend_num
            || proof_fragment.outcome != snapshot_fragment.outcome
            || proof_fragment.error_code != snapshot_fragment.error_code
            || proof_fragment.error_detail != snapshot_fragment.error_detail
            || proof_fragment.error_detail_truncated != snapshot_fragment.error_detail_truncated
        {
            return Err(ContractError::conflict(
                "terminalization proof does not match the immutable terminal snapshot",
            ));
        }
    }
    Ok(())
}

fn validate_execution(
    raw: Option<&novarocks::QueryExecutionId>,
    required: &'static str,
) -> Result<(), ContractError> {
    required_execution_id(raw, required).map(|_| ())
}

fn required_execution_id(
    raw: Option<&novarocks::QueryExecutionId>,
    required: &'static str,
) -> Result<QueryExecutionId, ContractError> {
    QueryExecutionId::try_from_proto(raw.ok_or_else(|| ContractError::invalid_value(required))?)
}

fn validate_backend(raw: &novarocks::ParticipantBackendIdentity) -> Result<(), ContractError> {
    let endpoint = raw
        .endpoint
        .as_ref()
        .ok_or_else(|| ContractError::invalid_value("query control endpoint is required"))?;
    if endpoint.host.trim().is_empty() {
        return Err(ContractError::invalid_value(
            "query control endpoint host must not be empty",
        ));
    }
    if endpoint.port == 0 || endpoint.port > u16::MAX as u32 {
        return Err(ContractError::invalid_value(
            "query control endpoint port must be a nonzero u16",
        ));
    }
    if raw.start_epoch == 0 {
        return Err(ContractError::invalid_value(
            "backend start epoch must be nonzero",
        ));
    }
    Ok(())
}

fn required_backend(
    raw: Option<&novarocks::ParticipantBackendIdentity>,
    required: &'static str,
) -> Result<ParticipantBackendIdentity, ContractError> {
    let raw = raw.ok_or_else(|| ContractError::invalid_value(required))?;
    ParticipantBackendIdentity::parse(raw.clone())
}

fn validate_attestation_reason(value: i32) -> Result<(), ContractError> {
    match novarocks::NegativeAttestationReason::try_from(value) {
        Ok(novarocks::NegativeAttestationReason::AttemptAborted)
        | Ok(novarocks::NegativeAttestationReason::AttemptTombstoned)
        | Ok(novarocks::NegativeAttestationReason::TerminalStateInvalid)
        | Ok(novarocks::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed)
        | Ok(novarocks::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted) => Ok(()),
        _ => Err(ContractError::invalid_value(
            "invalid negative attestation reason",
        )),
    }
}

fn require_known_enum(
    value: i32,
    expected: i32,
    detail: &'static str,
) -> Result<(), ContractError> {
    if value == expected {
        Ok(())
    } else {
        Err(ContractError::invalid_value(detail))
    }
}

fn validate_channel_key(key: (u32, u32)) -> Result<(), ContractError> {
    if key.0 == 0 || key.1 == 0 {
        return Err(ContractError::invalid_value(
            "terminal runtime-filter channel identity must be nonzero",
        ));
    }
    Ok(())
}

fn channel_keys(values: &[novarocks::QueryTerminalRuntimeFilterChannelV1]) -> BTreeSet<(u32, u32)> {
    values
        .iter()
        .map(|value| (value.channel_binding_id, value.channel_id))
        .collect()
}

fn validate_channel_reference(
    key: (u32, u32),
    known: &BTreeSet<(u32, u32)>,
) -> Result<(), ContractError> {
    validate_channel_key(key)?;
    if !known.contains(&key) {
        return Err(ContractError::invalid_value(
            "terminal runtime-filter section references an unknown channel",
        ));
    }
    Ok(())
}

fn validate_nonzero_id(
    raw: Option<&common::UniqueId>,
    missing: &'static str,
    zero: &'static str,
) -> Result<(), ContractError> {
    validate_nonzero_unique_id(
        raw.ok_or_else(|| ContractError::invalid_value(missing))?,
        zero,
    )
}

fn validate_nonzero_unique_id(
    raw: &common::UniqueId,
    detail: &'static str,
) -> Result<(), ContractError> {
    if raw.hi == 0 && raw.lo == 0 {
        return Err(ContractError::invalid_value(detail));
    }
    Ok(())
}

fn validate_sorted_unique_ids<T>(
    values: &[T],
    id: impl Fn(&T) -> Option<&common::UniqueId>,
    detail: &'static str,
) -> Result<(), ContractError> {
    let mut previous = None;
    for value in values {
        let id = id(value).ok_or_else(|| {
            ContractError::invalid_value("terminal fragment instance id is required")
        })?;
        validate_nonzero_unique_id(id, "terminal fragment instance id must be nonzero")?;
        validate_sorted_key(&mut previous, (id.hi, id.lo), detail)?;
    }
    Ok(())
}

fn validate_sorted_key<K: Ord + Copy>(
    previous: &mut Option<K>,
    current: K,
    detail: &'static str,
) -> Result<(), ContractError> {
    if previous.is_some_and(|value| value >= current) {
        return Err(ContractError::invalid_value(detail));
    }
    *previous = Some(current);
    Ok(())
}

fn validate_optional_nonzero(
    value: Option<u64>,
    detail: &'static str,
) -> Result<(), ContractError> {
    if value == Some(0) {
        Err(ContractError::invalid_value(detail))
    } else {
        Ok(())
    }
}

fn checked_sum<const N: usize>(
    values: [u64; N],
    detail: &'static str,
) -> Result<u64, ContractError> {
    values.into_iter().try_fold(0_u64, |sum, value| {
        sum.checked_add(value)
            .ok_or_else(|| ContractError::invalid_value(detail))
    })
}

fn validate_bounded_string(
    value: &str,
    limit: usize,
    detail: &'static str,
) -> Result<(), ContractError> {
    if value.len() > limit {
        Err(ContractError::capacity(detail))
    } else {
        Ok(())
    }
}

fn require_digest_len(value: &[u8], detail: &'static str) -> Result<(), ContractError> {
    if value.len() == 32 {
        Ok(())
    } else {
        Err(ContractError::invalid_value(detail))
    }
}

fn digest_array(value: &[u8]) -> Result<[u8; 32], ContractError> {
    value
        .try_into()
        .map_err(|_| ContractError::invalid_value("terminal digest must be 32 bytes"))
}

fn canonical_digest<M: prost::Message>(
    domain: &[u8],
    name: &str,
    raw: &M,
) -> Result<[u8; 32], ContractError> {
    canonical::digest_message(domain, name, raw)
        .map_err(|error| ContractError::invalid_value(error.detail()))
}

fn verify_digest(
    name: &str,
    supplied: &[u8],
    compute: impl FnOnce() -> Result<[u8; 32], canonical::CanonicalError>,
) -> Result<(), ContractError> {
    let supplied = digest_array(supplied)?;
    let computed = compute().map_err(|error| ContractError::invalid_value(error.detail()))?;
    if supplied == computed {
        Ok(())
    } else {
        Err(ContractError::digest_mismatch(format!(
            "{name} digest does not match canonical content"
        )))
    }
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

    fn execution() -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(common::UniqueId { hi: 1, lo: 2 }),
            attempt_id: 1,
        }
    }

    fn backend() -> novarocks::ParticipantBackendIdentity {
        novarocks::ParticipantBackendIdentity {
            backend_id: 1,
            endpoint: Some(novarocks::QueryControlEndpoint {
                host: "127.0.0.1".into(),
                port: 9030,
            }),
            start_epoch: 1,
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
            version: QUERY_TERMINAL_SNAPSHOT_VERSION_V1, execution_id: Some(execution()), backend: Some(backend()),
            init_digest: vec![7; 32], fragments: vec![fragment(1)],
            profile_contribution: Some(novarocks::QueryTerminalProfileContributionTelemetry { telemetry: Some(novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(novarocks::TerminalTelemetryUnavailable { stage: "observation".into(), code: "BUDGET_EXHAUSTED".into() })) }),
            ..Default::default()
        }
    }

    #[test]
    fn terminal_snapshot_seal_round_trips_exact_generated_message() {
        let snapshot = QueryTerminalSnapshot::seal(snapshot_raw()).expect("valid P1/P2 snapshot");
        assert_eq!(
            QueryTerminalSnapshot::parse(snapshot.as_proto().clone()).expect("parse"),
            snapshot
        );
    }

    #[test]
    fn terminal_values_reject_unknown_enums_and_bad_digest() {
        let mut raw = snapshot_raw();
        raw.fragments[0].outcome = 99;
        raw.digest = vec![0; 32];
        let error = QueryTerminalSnapshot::parse(raw).expect_err("unknown fragment outcome");
        assert_eq!(error.detail(), "invalid terminal fragment outcome");

        let mut sealed = QueryTerminalSnapshot::seal(snapshot_raw())
            .expect("sealed")
            .as_proto()
            .clone();
        sealed.digest[0] ^= 1;
        assert_eq!(
            QueryTerminalSnapshot::parse(sealed)
                .expect_err("digest mismatch")
                .code(),
            super::super::error::ContractErrorCode::DigestMismatch
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
        let snapshot = QueryTerminalSnapshot::seal(snapshot_raw()).expect("snapshot");
        let raw = novarocks::TerminalizationProof {
            version: TERMINALIZATION_PROOF_VERSION_V1,
            execution_id: Some(execution()),
            backend: Some(backend()),
            init_digest: vec![7; 32],
            fragments: vec![novarocks::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 0, lo: 1 }),
                backend_num: 0,
                outcome: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let proof = TerminalizationProof::seal(raw).expect("P0 proof");
        assert_eq!(snapshot.execution_id().query_id().high(), 1);
        assert_eq!(snapshot.backend().backend_id(), 1);
        assert_eq!(snapshot.init_digest().as_bytes(), &[7; 32]);
        assert_eq!(snapshot.fragments()[0].fragment_instance_id().lo, 1);
        assert_eq!(snapshot.fragments()[0].backend_num(), 0);
        assert_eq!(
            snapshot.fragments()[0].outcome(),
            novarocks::QueryTerminalFragmentOutcome::Succeeded
        );

        assert_eq!(proof.execution_id(), snapshot.execution_id());
        assert_eq!(proof.backend(), snapshot.backend());
        assert_eq!(proof.init_digest(), snapshot.init_digest());
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
        assert_eq!(outcome.backend(), snapshot.backend());
        assert_eq!(outcome.init_digest(), snapshot.init_digest());
        assert_eq!(outcome.digest(), proof.digest());
    }

    #[test]
    fn p0_p1_and_p2_negative_fixtures_fail_closed() {
        let invalid_p0 = novarocks::TerminalizationProof {
            version: TERMINALIZATION_PROOF_VERSION_V1,
            execution_id: Some(execution()),
            backend: Some(backend()),
            init_digest: vec![7; 32],
            fragments: vec![novarocks::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 0, lo: 1 }),
                backend_num: 0,
                outcome: 99,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(TerminalizationProof::seal(invalid_p0).is_err());

        let mut missing_p2 = snapshot_raw();
        missing_p2.profile_contribution = None;
        assert!(QueryTerminalSnapshot::seal(missing_p2).is_err());

        let attestation = NegativeAttestation::seal(novarocks::NegativeAttestation {
            execution_id: Some(execution()),
            backend: Some(backend()),
            init_digest: vec![7; 32],
            reason: novarocks::NegativeAttestationReason::AttemptAborted as i32,
            detail: "aborted".into(),
            ..Default::default()
        })
        .expect("negative attestation");
        assert_eq!(attestation.execution_id().query_id().low(), 2);
        assert_eq!(attestation.backend().start_epoch(), 1);
        assert_eq!(attestation.init_digest().as_bytes(), &[7; 32]);
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
                    QueryTerminalSnapshot::seal(snapshot_raw())
                        .expect("P1")
                        .as_proto()
                        .clone()
                ),
            })
            .is_err()
        );
    }

    #[test]
    fn profile_contribution_rejects_unknown_enum_and_orphan_channel() {
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
    fn profile_contribution_accepts_joined_terminal_with_truthful_event_counters() {
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
        assert!(QueryTerminalProfileContributionV1::parse(conflicting).is_err());
    }
}
