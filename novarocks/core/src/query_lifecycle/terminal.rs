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

//! Immutable, process-neutral terminal facts for one lifecycle participant.
//!
//! The lifecycle registry owns retention and delivery.  This module only owns
//! value semantics: canonical ordering, validation and the V1 digest.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::common::types::UniqueId;
use crate::runtime::sink_commit::SinkCommitReportSnapshot;
use novarocks_execution::runtime::fragment::fact::{FragmentOutcome, FragmentTerminalFact};
use novarocks_execution::runtime::profile::RuntimeProfileTree;
use novarocks_protocol::{common, novarocks};
use novarocks_spi::connector::ConnectorWriterTerminalState;

use super::{
    ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest, QueryExecutionId,
    QueryLifecycleError,
};

pub const QUERY_TERMINAL_SNAPSHOT_VERSION_V1: u32 = 1;
/// Version carried by terminal delivery acknowledgements for both the proof
/// and negative-attestation branches of `ParticipantTerminalOutcome`.
pub const PARTICIPANT_TERMINAL_OUTCOME_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1: u32 = 1;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES: usize = 128;
pub const QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES: usize = 4096;
const QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES: usize = 16_384;
const QUERY_TERMINAL_SNAPSHOT_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.terminal-snapshot.v1\0";
const TERMINALIZATION_PROOF_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.terminalization-proof.v1\0";
const NEGATIVE_ATTESTATION_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.negative-attestation.v1\0";
const TERMINALIZATION_PROOF_VERSION_V1: u32 = 1;
const CONNECTOR_WRITER_TERMINAL_STAGED: u32 = 0;

fn validated_endpoint(
    backend: &ParticipantBackendIdentity,
) -> novarocks_protocol::lifecycle::QueryControlEndpoint {
    backend
        .endpoint()
        .expect("validated lifecycle backend identity always has an endpoint")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTerminalSnapshotDigest([u8; 32]);

impl QueryTerminalSnapshotDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, QueryLifecycleError> {
        let bytes = bytes.try_into().map_err(|_| {
            QueryLifecycleError::invalid_manifest("query terminal snapshot digest must be 32 bytes")
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentTerminalOutcome {
    Succeeded,
    Failed {
        code: String,
        detail: String,
        detail_truncated: bool,
    },
    Cancelled {
        detail: String,
        detail_truncated: bool,
    },
    IncompleteDrain {
        detail: String,
        detail_truncated: bool,
    },
}

impl FragmentTerminalOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }

    fn bounded(self) -> Self {
        match self {
            Self::Succeeded => Self::Succeeded,
            Self::Failed { code, detail, .. } => {
                let (code, _) = truncate_utf8(code, QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES);
                let (detail, detail_truncated) =
                    truncate_utf8(detail, QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
                Self::Failed {
                    code,
                    detail,
                    detail_truncated,
                }
            }
            Self::Cancelled { detail, .. } => {
                let (detail, detail_truncated) =
                    truncate_utf8(detail, QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
                Self::Cancelled {
                    detail,
                    detail_truncated,
                }
            }
            Self::IncompleteDrain { detail, .. } => {
                let (detail, detail_truncated) =
                    truncate_utf8(detail, QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
                Self::IncompleteDrain {
                    detail,
                    detail_truncated,
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentTerminalSnapshot {
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: FragmentTerminalOutcome,
    sink: SinkCommitReportSnapshot,
    profile: TerminalTelemetry<RuntimeProfileTree>,
    statistics_payload: Vec<u8>,
}

impl FragmentTerminalSnapshot {
    pub fn new(
        fragment_instance_id: UniqueId,
        backend_num: i32,
        outcome: FragmentTerminalOutcome,
        sink: SinkCommitReportSnapshot,
        profile: Option<RuntimeProfileTree>,
    ) -> Result<Self, QueryLifecycleError> {
        let profile = match profile {
            Some(profile) => TerminalTelemetry::Available(profile),
            None => TerminalTelemetry::unavailable("fragment_profile", "PROFILE_UNAVAILABLE")?,
        };
        Self::new_with_profile_telemetry(fragment_instance_id, backend_num, outcome, sink, profile)
    }

    pub fn new_with_profile_telemetry(
        fragment_instance_id: UniqueId,
        backend_num: i32,
        outcome: FragmentTerminalOutcome,
        sink: SinkCommitReportSnapshot,
        profile: TerminalTelemetry<RuntimeProfileTree>,
    ) -> Result<Self, QueryLifecycleError> {
        if fragment_instance_id.high() == 0 && fragment_instance_id.low() == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment instance id must be nonzero",
            ));
        }
        if backend_num < 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment backend number must be nonnegative",
            ));
        }
        Ok(Self {
            fragment_instance_id,
            backend_num,
            outcome: outcome.bounded(),
            sink,
            profile,
            statistics_payload: Vec::new(),
        })
    }

    pub fn from_fact(
        fact: FragmentTerminalFact,
        backend_num: i32,
        sink: SinkCommitReportSnapshot,
    ) -> Result<Self, QueryLifecycleError> {
        let outcome = match fact.outcome() {
            FragmentOutcome::Succeeded => FragmentTerminalOutcome::Succeeded,
            FragmentOutcome::Failed(error) => FragmentTerminalOutcome::Failed {
                code: "FRAGMENT_EXECUTION_FAILED".to_string(),
                detail: error.to_string(),
                detail_truncated: false,
            },
            FragmentOutcome::Cancelled { reason } => FragmentTerminalOutcome::Cancelled {
                detail: reason.detail().to_string(),
                detail_truncated: false,
            },
        };
        Self::new(
            fact.fragment_instance_id(),
            backend_num,
            outcome,
            sink,
            fact.profile().cloned(),
        )
        .and_then(|snapshot| snapshot.with_statistics_payload(fact.statistics_payload().to_vec()))
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub const fn outcome(&self) -> &FragmentTerminalOutcome {
        &self.outcome
    }

    pub const fn sink(&self) -> &SinkCommitReportSnapshot {
        &self.sink
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.available()
    }

    pub const fn profile_telemetry(&self) -> &TerminalTelemetry<RuntimeProfileTree> {
        &self.profile
    }

    pub fn with_statistics_payload(
        mut self,
        statistics_payload: Vec<u8>,
    ) -> Result<Self, QueryLifecycleError> {
        if statistics_payload.len()
            > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment statistics payload exceeds the connector statistics limit",
            ));
        }
        self.statistics_payload = statistics_payload;
        Ok(self)
    }

    pub fn statistics_payload(&self) -> &[u8] {
        &self.statistics_payload
    }
}

/// A P0-only fragment terminal fact. It deliberately omits all P1 correctness
/// evidence and P2 telemetry so an outcome can be retained independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalizationProofFragment {
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: FragmentTerminalOutcome,
}

impl TerminalizationProofFragment {
    pub fn new(
        fragment_instance_id: UniqueId,
        backend_num: i32,
        outcome: FragmentTerminalOutcome,
    ) -> Result<Self, QueryLifecycleError> {
        if fragment_instance_id.high() == 0 && fragment_instance_id.low() == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminalization proof fragment instance id must be nonzero",
            ));
        }
        if backend_num < 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminalization proof fragment backend number must be nonnegative",
            ));
        }
        Ok(Self {
            fragment_instance_id,
            backend_num,
            outcome: outcome.bounded(),
        })
    }

    fn from_snapshot(snapshot: &FragmentTerminalSnapshot) -> Self {
        Self::new(
            snapshot.fragment_instance_id(),
            snapshot.backend_num(),
            snapshot.outcome().clone(),
        )
        .expect("fragment terminal snapshot is valid proof input")
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub const fn outcome(&self) -> &FragmentTerminalOutcome {
        &self.outcome
    }
}

/// The bounded P0 record proving a participant's terminal state was frozen.
#[derive(Clone, Debug, PartialEq)]
pub struct TerminalizationProof {
    version: u32,
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    init_digest: ParticipantManifestDigest,
    fragments: Vec<TerminalizationProofFragment>,
    digest: QueryTerminalSnapshotDigest,
}

impl TerminalizationProof {
    pub fn from_snapshot(snapshot: &QueryTerminalSnapshot) -> Result<Self, QueryLifecycleError> {
        Self::new(
            snapshot.execution_id(),
            snapshot.backend().clone(),
            snapshot.init_digest(),
            snapshot
                .fragments()
                .iter()
                .map(TerminalizationProofFragment::from_snapshot)
                .collect(),
        )
    }

    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        mut fragments: Vec<TerminalizationProofFragment>,
    ) -> Result<Self, QueryLifecycleError> {
        fragments.sort_by_key(TerminalizationProofFragment::fragment_instance_id);
        let mut ids = BTreeSet::new();
        for fragment in &fragments {
            if !ids.insert(fragment.fragment_instance_id) {
                return Err(QueryLifecycleError::invalid_manifest(
                    "terminalization proof contains duplicate fragment facts",
                ));
            }
        }
        let mut proof = Self {
            version: TERMINALIZATION_PROOF_VERSION_V1,
            execution_id,
            backend,
            init_digest,
            fragments,
            digest: QueryTerminalSnapshotDigest::new([0; 32]),
        };
        proof.digest = proof.compute_digest();
        Ok(proof)
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub fn fragments(&self) -> &[TerminalizationProofFragment] {
        &self.fragments
    }

    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        self.digest
    }

    pub fn validate(&self) -> Result<(), QueryLifecycleError> {
        if self.version != TERMINALIZATION_PROOF_VERSION_V1 {
            return Err(QueryLifecycleError::invalid_manifest(
                "unsupported terminalization proof version",
            ));
        }
        if self.compute_digest() != self.digest {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Conflict,
                "terminalization proof digest does not match canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, self.version);
        put_i64(&mut bytes, self.execution_id.query_id().high());
        put_i64(&mut bytes, self.execution_id.query_id().low());
        put_u64(&mut bytes, self.execution_id.attempt_id().get());
        put_u64(&mut bytes, self.backend.backend_id());
        let endpoint = validated_endpoint(&self.backend);
        put_string(&mut bytes, endpoint.host());
        put_u16(&mut bytes, endpoint.port());
        put_u64(&mut bytes, self.backend.start_epoch());
        put_bytes(&mut bytes, self.init_digest.as_bytes());
        put_u64(&mut bytes, self.fragments.len() as u64);
        for fragment in &self.fragments {
            put_i64(&mut bytes, fragment.fragment_instance_id.high());
            put_i64(&mut bytes, fragment.fragment_instance_id.low());
            put_i32(&mut bytes, fragment.backend_num);
            put_fragment_outcome(&mut bytes, &fragment.outcome);
        }
        bytes
    }

    fn compute_digest(&self) -> QueryTerminalSnapshotDigest {
        let mut hasher = Sha256::new();
        hasher.update(TERMINALIZATION_PROOF_V1_DOMAIN);
        hasher.update(self.canonical_bytes());
        QueryTerminalSnapshotDigest::new(hasher.finalize().into())
    }
}

/// Worst-case P0 canonical payload size for a participant manifest. Backend
/// QLC reserves this before ControlReady, independently of P1/P2 payloads.
pub fn p0_max_encoded_len(manifest: &ParticipantManifest) -> usize {
    let backend = manifest
        .backend()
        .expect("validated participant manifest has a backend");
    let endpoint = validated_endpoint(&backend);
    let fixed_header: usize = 4 + 8 + 8 + 8 + 8 + 8 + endpoint.host().len() + 2 + 8 + 8 + 32 + 8;
    let max_outcome = 1
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES
        + 1;
    let proof_max = fixed_header.saturating_add(
        manifest
            .expected_fragment_instance_ids()
            .len()
            .saturating_mul(16 + 4 + max_outcome),
    );
    // An attestation has no fragment list, so a coordinator-only participant
    // still needs a pre-ready reservation large enough for its bounded detail.
    let attestation_max = 8
        + 8
        + 8
        + 8
        + 8
        + endpoint.host().len()
        + 2
        + 8
        + 32
        + 1
        + 8
        + QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES
        + 1;
    proof_max.max(attestation_max)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NegativeAttestationReason {
    AttemptAborted,
    AttemptTombstoned,
    TerminalStateInvalid,
    CorrectnessEvidenceEncodingFailed,
    CorrectnessEvidenceRetentionExhausted,
}

impl NegativeAttestationReason {
    fn tag(self) -> u8 {
        match self {
            Self::AttemptAborted => 1,
            Self::AttemptTombstoned => 2,
            Self::TerminalStateInvalid => 3,
            Self::CorrectnessEvidenceEncodingFailed => 4,
            Self::CorrectnessEvidenceRetentionExhausted => 5,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NegativeAttestation {
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    init_digest: ParticipantManifestDigest,
    reason: NegativeAttestationReason,
    detail: String,
    detail_truncated: bool,
    digest: QueryTerminalSnapshotDigest,
}

impl NegativeAttestation {
    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        reason: NegativeAttestationReason,
        detail: String,
    ) -> Self {
        let (detail, detail_truncated) =
            truncate_utf8(detail, QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
        let mut attestation = Self {
            execution_id,
            backend,
            init_digest,
            reason,
            detail,
            detail_truncated,
            digest: QueryTerminalSnapshotDigest::new([0; 32]),
        };
        attestation.digest = attestation.compute_digest();
        attestation
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub const fn reason(&self) -> NegativeAttestationReason {
        self.reason
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub const fn detail_truncated(&self) -> bool {
        self.detail_truncated
    }

    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        self.digest
    }

    pub fn validate(&self) -> Result<(), QueryLifecycleError> {
        if self.compute_digest() != self.digest {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Conflict,
                "negative attestation digest does not match canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_i64(&mut bytes, self.execution_id.query_id().high());
        put_i64(&mut bytes, self.execution_id.query_id().low());
        put_u64(&mut bytes, self.execution_id.attempt_id().get());
        put_u64(&mut bytes, self.backend.backend_id());
        let endpoint = validated_endpoint(&self.backend);
        put_string(&mut bytes, endpoint.host());
        put_u16(&mut bytes, endpoint.port());
        put_u64(&mut bytes, self.backend.start_epoch());
        put_bytes(&mut bytes, self.init_digest.as_bytes());
        put_u8(&mut bytes, self.reason.tag());
        put_string(&mut bytes, &self.detail);
        put_u8(&mut bytes, u8::from(self.detail_truncated));
        bytes
    }

    fn compute_digest(&self) -> QueryTerminalSnapshotDigest {
        let mut hasher = Sha256::new();
        hasher.update(NEGATIVE_ATTESTATION_V1_DOMAIN);
        hasher.update(self.canonical_bytes());
        QueryTerminalSnapshotDigest::new(hasher.finalize().into())
    }
}

/// Stable, bounded reason for a P2 value that was intentionally omitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalTelemetryUnavailable {
    stage: String,
    code: String,
}

impl TerminalTelemetryUnavailable {
    pub fn new(
        stage: impl Into<String>,
        code: impl Into<String>,
    ) -> Result<Self, QueryLifecycleError> {
        let stage = stage.into();
        let code = code.into();
        if stage.trim().is_empty() || code.trim().is_empty() {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal telemetry unavailable stage and code must be nonempty",
            ));
        }
        Ok(Self { stage, code })
    }

    pub fn stage(&self) -> &str {
        &self.stage
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

/// Typed P2 availability. P1 values are intentionally not represented here.
#[derive(Clone, Debug, PartialEq)]
pub enum TerminalTelemetry<T> {
    Available(T),
    Unavailable(TerminalTelemetryUnavailable),
}

impl<T> TerminalTelemetry<T> {
    pub fn unavailable(
        stage: impl Into<String>,
        code: impl Into<String>,
    ) -> Result<Self, QueryLifecycleError> {
        Ok(Self::Unavailable(TerminalTelemetryUnavailable::new(
            stage, code,
        )?))
    }

    pub const fn available(&self) -> Option<&T> {
        match self {
            Self::Available(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }

    pub const fn unavailable_reason(&self) -> Option<&TerminalTelemetryUnavailable> {
        match self {
            Self::Available(_) => None,
            Self::Unavailable(reason) => Some(reason),
        }
    }
}

/// The only Backend-authored participant terminal result. A proof retains the
/// complete P1 snapshot alongside its independently deliverable P0 proof;
/// an attestation states that a valid snapshot could not be formed.
#[derive(Clone, Debug, PartialEq)]
pub enum ParticipantTerminalOutcome {
    Proof {
        proof: TerminalizationProof,
        snapshot: QueryTerminalSnapshot,
    },
    NegativeAttestation(NegativeAttestation),
}

impl ParticipantTerminalOutcome {
    pub fn proof(snapshot: QueryTerminalSnapshot) -> Result<Self, QueryLifecycleError> {
        let proof = TerminalizationProof::from_snapshot(&snapshot)?;
        Ok(Self::Proof { proof, snapshot })
    }

    pub fn negative_attestation(attestation: NegativeAttestation) -> Self {
        Self::NegativeAttestation(attestation)
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        match self {
            Self::Proof { proof, .. } => proof.execution_id(),
            Self::NegativeAttestation(attestation) => attestation.execution_id(),
        }
    }

    /// A negative attestation has no snapshot, so terminal delivery uses this
    /// outcome-level version rather than the snapshot version in its ACK
    /// identity. The value is shared by both variants deliberately.
    pub const fn version(&self) -> u32 {
        PARTICIPANT_TERMINAL_OUTCOME_VERSION_V1
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        match self {
            Self::Proof { proof, .. } => proof.backend(),
            Self::NegativeAttestation(attestation) => attestation.backend(),
        }
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        match self {
            Self::Proof { proof, .. } => proof.init_digest(),
            Self::NegativeAttestation(attestation) => attestation.init_digest(),
        }
    }

    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        match self {
            Self::Proof { proof, .. } => proof.digest(),
            Self::NegativeAttestation(attestation) => attestation.digest(),
        }
    }

    pub fn validate(&self) -> Result<(), QueryLifecycleError> {
        match self {
            Self::Proof { proof, snapshot } => {
                proof.validate()?;
                snapshot.validate()?;
                let expected = TerminalizationProof::from_snapshot(snapshot)?;
                if &expected != proof {
                    return Err(QueryLifecycleError::new(
                        super::QueryLifecycleErrorCode::Conflict,
                        "terminalization proof does not match the immutable terminal snapshot",
                    ));
                }
                Ok(())
            }
            Self::NegativeAttestation(attestation) => attestation.validate(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTerminalRuntimeFilterChannelKeyV1 {
    channel_binding_id: u32,
    channel_id: u32,
}

impl QueryTerminalRuntimeFilterChannelKeyV1 {
    pub const fn new(channel_binding_id: u32, channel_id: u32) -> Self {
        Self {
            channel_binding_id,
            channel_id,
        }
    }

    pub const fn channel_binding_id(self) -> u32 {
        self.channel_binding_id
    }

    pub const fn channel_id(self) -> u32 {
        self.channel_id
    }

    fn validate(self) -> Result<(), QueryLifecycleError> {
        if self.channel_binding_id == 0 || self.channel_id == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter channel identity must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalRuntimeFilterChannelInstallStateV1 {
    Installed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalRuntimeFilterChannelTerminalStateV1 {
    Open,
    Completed,
    Unavailable,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalRuntimeFilterChannelV1 {
    key: QueryTerminalRuntimeFilterChannelKeyV1,
    install_state: QueryTerminalRuntimeFilterChannelInstallStateV1,
    terminal_state: QueryTerminalRuntimeFilterChannelTerminalStateV1,
    latest_published_logical_version: Option<u64>,
    published_count: u64,
    completed_count: u64,
    unavailable_count: u64,
    cancelled_count: u64,
}

impl QueryTerminalRuntimeFilterChannelV1 {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        key: QueryTerminalRuntimeFilterChannelKeyV1,
        install_state: QueryTerminalRuntimeFilterChannelInstallStateV1,
        terminal_state: QueryTerminalRuntimeFilterChannelTerminalStateV1,
        latest_published_logical_version: Option<u64>,
        published_count: u64,
        completed_count: u64,
        unavailable_count: u64,
        cancelled_count: u64,
    ) -> Self {
        Self {
            key,
            install_state,
            terminal_state,
            latest_published_logical_version,
            published_count,
            completed_count,
            unavailable_count,
            cancelled_count,
        }
    }

    pub const fn key(&self) -> QueryTerminalRuntimeFilterChannelKeyV1 {
        self.key
    }
    pub const fn install_state(&self) -> QueryTerminalRuntimeFilterChannelInstallStateV1 {
        self.install_state
    }
    pub const fn terminal_state(&self) -> QueryTerminalRuntimeFilterChannelTerminalStateV1 {
        self.terminal_state
    }
    pub const fn latest_published_logical_version(&self) -> Option<u64> {
        self.latest_published_logical_version
    }
    pub const fn published_count(&self) -> u64 {
        self.published_count
    }
    pub const fn completed_count(&self) -> u64 {
        self.completed_count
    }
    pub const fn unavailable_count(&self) -> u64 {
        self.unavailable_count
    }
    pub const fn cancelled_count(&self) -> u64 {
        self.cancelled_count
    }

    fn validate(&self) -> Result<(), QueryLifecycleError> {
        self.key.validate()?;
        validate_optional_nonzero(
            self.latest_published_logical_version,
            "terminal runtime-filter latest published logical version must be nonzero",
        )?;
        if (self.published_count == 0) != self.latest_published_logical_version.is_none() {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter published count and latest version disagree",
            ));
        }
        let terminal_count = checked_sum(
            [
                self.completed_count,
                self.unavailable_count,
                self.cancelled_count,
            ],
            "terminal runtime-filter channel counters overflow",
        )?;
        let valid_terminal = match self.terminal_state {
            QueryTerminalRuntimeFilterChannelTerminalStateV1::Open => terminal_count == 0,
            QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed => {
                terminal_count == 1 && self.completed_count == 1
            }
            QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable => {
                terminal_count == 1 && self.unavailable_count == 1
            }
            QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled => {
                terminal_count == 1 && self.cancelled_count == 1
            }
        };
        if !valid_terminal {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter channel state and terminal counters disagree",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTerminalRuntimeFilterProducerStreamKeyV1 {
    channel: QueryTerminalRuntimeFilterChannelKeyV1,
    producer_fragment_instance_id: UniqueId,
    partition_id: u32,
}

impl QueryTerminalRuntimeFilterProducerStreamKeyV1 {
    pub const fn new(
        channel: QueryTerminalRuntimeFilterChannelKeyV1,
        producer_fragment_instance_id: UniqueId,
        partition_id: u32,
    ) -> Self {
        Self {
            channel,
            producer_fragment_instance_id,
            partition_id,
        }
    }

    pub const fn channel(self) -> QueryTerminalRuntimeFilterChannelKeyV1 {
        self.channel
    }
    pub const fn producer_fragment_instance_id(self) -> UniqueId {
        self.producer_fragment_instance_id
    }
    pub const fn partition_id(self) -> u32 {
        self.partition_id
    }

    fn validate(self) -> Result<(), QueryLifecycleError> {
        self.channel.validate()?;
        validate_unique_id(
            self.producer_fragment_instance_id,
            "terminal runtime-filter producer fragment instance id must be nonzero",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalRuntimeFilterProducerStreamV1 {
    key: QueryTerminalRuntimeFilterProducerStreamKeyV1,
    latest_accepted_sequence: Option<u64>,
    accepted_count: u64,
    duplicate_count: u64,
    stale_count: u64,
    conflict_count: u64,
    resource_limit_count: u64,
}

impl QueryTerminalRuntimeFilterProducerStreamV1 {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        key: QueryTerminalRuntimeFilterProducerStreamKeyV1,
        latest_accepted_sequence: Option<u64>,
        accepted_count: u64,
        duplicate_count: u64,
        stale_count: u64,
        conflict_count: u64,
        resource_limit_count: u64,
    ) -> Self {
        Self {
            key,
            latest_accepted_sequence,
            accepted_count,
            duplicate_count,
            stale_count,
            conflict_count,
            resource_limit_count,
        }
    }

    pub const fn key(&self) -> QueryTerminalRuntimeFilterProducerStreamKeyV1 {
        self.key
    }
    pub const fn latest_accepted_sequence(&self) -> Option<u64> {
        self.latest_accepted_sequence
    }
    pub const fn accepted_count(&self) -> u64 {
        self.accepted_count
    }
    pub const fn duplicate_count(&self) -> u64 {
        self.duplicate_count
    }
    pub const fn stale_count(&self) -> u64 {
        self.stale_count
    }
    pub const fn conflict_count(&self) -> u64 {
        self.conflict_count
    }
    pub const fn resource_limit_count(&self) -> u64 {
        self.resource_limit_count
    }

    fn validate(&self) -> Result<(), QueryLifecycleError> {
        self.key.validate()?;
        // ProducerSequence is zero-based. Presence, rather than a non-zero
        // value, proves that at least one contribution was accepted.
        if (self.accepted_count == 0) != self.latest_accepted_sequence.is_none() {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter accepted count and latest sequence disagree",
            ));
        }
        checked_sum(
            [
                self.accepted_count,
                self.duplicate_count,
                self.stale_count,
                self.conflict_count,
                self.resource_limit_count,
            ],
            "terminal runtime-filter producer counters overflow",
        )?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTerminalRuntimeFilterTransportRouteKeyV1 {
    channel: QueryTerminalRuntimeFilterChannelKeyV1,
    route_edge_id: u64,
}

impl QueryTerminalRuntimeFilterTransportRouteKeyV1 {
    pub const fn new(channel: QueryTerminalRuntimeFilterChannelKeyV1, route_edge_id: u64) -> Self {
        Self {
            channel,
            route_edge_id,
        }
    }

    pub const fn channel(self) -> QueryTerminalRuntimeFilterChannelKeyV1 {
        self.channel
    }
    pub const fn route_edge_id(self) -> u64 {
        self.route_edge_id
    }

    fn validate(self) -> Result<(), QueryLifecycleError> {
        self.channel.validate()?;
        if self.route_edge_id == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter route edge id must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalRuntimeFilterTransportRouteV1 {
    key: QueryTerminalRuntimeFilterTransportRouteKeyV1,
    sent_count: u64,
    sent_bytes: u64,
    retried_count: u64,
    retried_bytes: u64,
    acked_count: u64,
    acked_bytes: u64,
    fail_open_count: u64,
    fail_open_bytes: u64,
}

impl QueryTerminalRuntimeFilterTransportRouteV1 {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        key: QueryTerminalRuntimeFilterTransportRouteKeyV1,
        sent_count: u64,
        sent_bytes: u64,
        retried_count: u64,
        retried_bytes: u64,
        acked_count: u64,
        acked_bytes: u64,
        fail_open_count: u64,
        fail_open_bytes: u64,
    ) -> Self {
        Self {
            key,
            sent_count,
            sent_bytes,
            retried_count,
            retried_bytes,
            acked_count,
            acked_bytes,
            fail_open_count,
            fail_open_bytes,
        }
    }

    pub const fn key(&self) -> QueryTerminalRuntimeFilterTransportRouteKeyV1 {
        self.key
    }
    pub const fn sent_count(&self) -> u64 {
        self.sent_count
    }
    pub const fn sent_bytes(&self) -> u64 {
        self.sent_bytes
    }
    pub const fn retried_count(&self) -> u64 {
        self.retried_count
    }
    pub const fn retried_bytes(&self) -> u64 {
        self.retried_bytes
    }
    pub const fn acked_count(&self) -> u64 {
        self.acked_count
    }
    pub const fn acked_bytes(&self) -> u64 {
        self.acked_bytes
    }
    pub const fn fail_open_count(&self) -> u64 {
        self.fail_open_count
    }
    pub const fn fail_open_bytes(&self) -> u64 {
        self.fail_open_bytes
    }

    fn validate(&self) -> Result<(), QueryLifecycleError> {
        self.key.validate()?;
        let delivery_count = self
            .sent_count
            .checked_add(self.retried_count)
            .ok_or_else(|| {
                QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter transport delivery counter overflow",
                )
            })?;
        let delivery_bytes = self
            .sent_bytes
            .checked_add(self.retried_bytes)
            .ok_or_else(|| {
                QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter transport delivery bytes overflow",
                )
            })?;
        if self.acked_count > delivery_count || self.acked_bytes > delivery_bytes {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter transport acknowledgement exceeds delivery totals",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalRuntimeFilterSubscriptionTerminalV1 {
    Pending,
    Acquired,
    TimedOut,
    Unavailable,
    Unsupported,
    Cancelled,
    Completed,
    CompletedWithoutArtifact,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
    unit_facts_missing: u64,
    column_facts_missing: u64,
    data_type_unsupported: u64,
    predicate_capability_unsupported: u64,
    resource_unavailable: u64,
    snapshot_unavailable: u64,
    snapshot_timed_out: u64,
    snapshot_not_published: u64,
}

impl QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        unit_facts_missing: u64,
        column_facts_missing: u64,
        data_type_unsupported: u64,
        predicate_capability_unsupported: u64,
        resource_unavailable: u64,
        snapshot_unavailable: u64,
        snapshot_timed_out: u64,
        snapshot_not_published: u64,
    ) -> Self {
        Self {
            unit_facts_missing,
            column_facts_missing,
            data_type_unsupported,
            predicate_capability_unsupported,
            resource_unavailable,
            snapshot_unavailable,
            snapshot_timed_out,
            snapshot_not_published,
        }
    }

    pub const fn unit_facts_missing(self) -> u64 {
        self.unit_facts_missing
    }
    pub const fn column_facts_missing(self) -> u64 {
        self.column_facts_missing
    }
    pub const fn data_type_unsupported(self) -> u64 {
        self.data_type_unsupported
    }
    pub const fn predicate_capability_unsupported(self) -> u64 {
        self.predicate_capability_unsupported
    }
    pub const fn resource_unavailable(self) -> u64 {
        self.resource_unavailable
    }
    pub const fn snapshot_unavailable(self) -> u64 {
        self.snapshot_unavailable
    }
    pub const fn snapshot_timed_out(self) -> u64 {
        self.snapshot_timed_out
    }
    pub const fn snapshot_not_published(self) -> u64 {
        self.snapshot_not_published
    }

    fn total(self) -> Result<u64, QueryLifecycleError> {
        checked_sum(
            [
                self.unit_facts_missing,
                self.column_facts_missing,
                self.data_type_unsupported,
                self.predicate_capability_unsupported,
                self.resource_unavailable,
                self.snapshot_unavailable,
                self.snapshot_timed_out,
                self.snapshot_not_published,
            ],
            "terminal runtime-filter scan not-evaluated counters overflow",
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryTerminalRuntimeFilterConsumerKeyV1 {
    channel: QueryTerminalRuntimeFilterChannelKeyV1,
    consumer_binding_id: u32,
    fragment_instance_id: UniqueId,
}

impl QueryTerminalRuntimeFilterConsumerKeyV1 {
    pub const fn new(
        channel: QueryTerminalRuntimeFilterChannelKeyV1,
        consumer_binding_id: u32,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            channel,
            consumer_binding_id,
            fragment_instance_id,
        }
    }

    pub const fn channel(self) -> QueryTerminalRuntimeFilterChannelKeyV1 {
        self.channel
    }
    pub const fn consumer_binding_id(self) -> u32 {
        self.consumer_binding_id
    }
    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    fn validate(self) -> Result<(), QueryLifecycleError> {
        self.channel.validate()?;
        if self.consumer_binding_id == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter consumer binding id must be nonzero",
            ));
        }
        validate_unique_id(
            self.fragment_instance_id,
            "terminal runtime-filter consumer fragment instance id must be nonzero",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalRuntimeFilterConsumerV1 {
    key: QueryTerminalRuntimeFilterConsumerKeyV1,
    latest_delivered_logical_version: Option<u64>,
    latest_applied_logical_version: Option<u64>,
    subscription_terminal: QueryTerminalRuntimeFilterSubscriptionTerminalV1,
    row_evaluations: u64,
    input_rows: u64,
    output_rows: u64,
    scan_evaluated: u64,
    scan_kept: u64,
    scan_pruned: u64,
    scan_not_evaluated: u64,
    scan_not_evaluated_reasons: QueryTerminalRuntimeFilterScanNotEvaluatedV1,
}

impl QueryTerminalRuntimeFilterConsumerV1 {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        key: QueryTerminalRuntimeFilterConsumerKeyV1,
        latest_delivered_logical_version: Option<u64>,
        latest_applied_logical_version: Option<u64>,
        subscription_terminal: QueryTerminalRuntimeFilterSubscriptionTerminalV1,
        row_evaluations: u64,
        input_rows: u64,
        output_rows: u64,
        scan_evaluated: u64,
        scan_kept: u64,
        scan_pruned: u64,
        scan_not_evaluated: u64,
        scan_not_evaluated_reasons: QueryTerminalRuntimeFilterScanNotEvaluatedV1,
    ) -> Self {
        Self {
            key,
            latest_delivered_logical_version,
            latest_applied_logical_version,
            subscription_terminal,
            row_evaluations,
            input_rows,
            output_rows,
            scan_evaluated,
            scan_kept,
            scan_pruned,
            scan_not_evaluated,
            scan_not_evaluated_reasons,
        }
    }

    pub const fn key(&self) -> QueryTerminalRuntimeFilterConsumerKeyV1 {
        self.key
    }
    pub const fn latest_delivered_logical_version(&self) -> Option<u64> {
        self.latest_delivered_logical_version
    }
    pub const fn latest_applied_logical_version(&self) -> Option<u64> {
        self.latest_applied_logical_version
    }
    pub const fn subscription_terminal(&self) -> QueryTerminalRuntimeFilterSubscriptionTerminalV1 {
        self.subscription_terminal
    }
    pub const fn row_evaluations(&self) -> u64 {
        self.row_evaluations
    }
    pub const fn input_rows(&self) -> u64 {
        self.input_rows
    }
    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }
    pub const fn scan_evaluated(&self) -> u64 {
        self.scan_evaluated
    }
    pub const fn scan_kept(&self) -> u64 {
        self.scan_kept
    }
    pub const fn scan_pruned(&self) -> u64 {
        self.scan_pruned
    }
    pub const fn scan_not_evaluated(&self) -> u64 {
        self.scan_not_evaluated
    }
    pub const fn scan_not_evaluated_reasons(&self) -> QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
        self.scan_not_evaluated_reasons
    }

    fn validate(&self) -> Result<(), QueryLifecycleError> {
        self.key.validate()?;
        validate_optional_nonzero(
            self.latest_delivered_logical_version,
            "terminal runtime-filter latest delivered logical version must be nonzero",
        )?;
        validate_optional_nonzero(
            self.latest_applied_logical_version,
            "terminal runtime-filter latest applied logical version must be nonzero",
        )?;
        if let Some(applied) = self.latest_applied_logical_version {
            let Some(delivered) = self.latest_delivered_logical_version else {
                return Err(QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter applied version requires a delivered version",
                ));
            };
            if applied > delivered {
                return Err(QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter applied version exceeds delivered version",
                ));
            }
        }
        if self.output_rows > self.input_rows
            || (self.row_evaluations == 0 && (self.input_rows != 0 || self.output_rows != 0))
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter row counters are inconsistent",
            ));
        }
        let evaluated = self
            .scan_kept
            .checked_add(self.scan_pruned)
            .ok_or_else(|| {
                QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter scan evaluated counters overflow",
                )
            })?;
        if evaluated != self.scan_evaluated
            || self.scan_not_evaluated_reasons.total()? != self.scan_not_evaluated
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal runtime-filter scan counters are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalProfileContributionV1 {
    version: u32,
    channels: Vec<QueryTerminalRuntimeFilterChannelV1>,
    producer_streams: Vec<QueryTerminalRuntimeFilterProducerStreamV1>,
    transport_routes: Vec<QueryTerminalRuntimeFilterTransportRouteV1>,
    consumers: Vec<QueryTerminalRuntimeFilterConsumerV1>,
}

impl QueryTerminalProfileContributionV1 {
    pub const fn empty() -> Self {
        Self {
            version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
            channels: Vec::new(),
            producer_streams: Vec::new(),
            transport_routes: Vec::new(),
            consumers: Vec::new(),
        }
    }

    pub fn try_new(
        mut channels: Vec<QueryTerminalRuntimeFilterChannelV1>,
        mut producer_streams: Vec<QueryTerminalRuntimeFilterProducerStreamV1>,
        mut transport_routes: Vec<QueryTerminalRuntimeFilterTransportRouteV1>,
        mut consumers: Vec<QueryTerminalRuntimeFilterConsumerV1>,
    ) -> Result<Self, QueryLifecycleError> {
        channels.sort_by_key(QueryTerminalRuntimeFilterChannelV1::key);
        producer_streams.sort_by_key(QueryTerminalRuntimeFilterProducerStreamV1::key);
        transport_routes.sort_by_key(QueryTerminalRuntimeFilterTransportRouteV1::key);
        consumers.sort_by_key(QueryTerminalRuntimeFilterConsumerV1::key);

        validate_sorted_unique(
            &channels,
            QueryTerminalRuntimeFilterChannelV1::key,
            "channel",
        )?;
        validate_sorted_unique(
            &producer_streams,
            QueryTerminalRuntimeFilterProducerStreamV1::key,
            "producer stream",
        )?;
        validate_sorted_unique(
            &transport_routes,
            QueryTerminalRuntimeFilterTransportRouteV1::key,
            "transport route",
        )?;
        validate_sorted_unique(
            &consumers,
            QueryTerminalRuntimeFilterConsumerV1::key,
            "consumer",
        )?;

        let contribution = Self {
            version: QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
            channels,
            producer_streams,
            transport_routes,
            consumers,
        };
        contribution.validate()?;
        Ok(contribution)
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
    pub fn channels(&self) -> &[QueryTerminalRuntimeFilterChannelV1] {
        &self.channels
    }
    pub fn producer_streams(&self) -> &[QueryTerminalRuntimeFilterProducerStreamV1] {
        &self.producer_streams
    }
    pub fn transport_routes(&self) -> &[QueryTerminalRuntimeFilterTransportRouteV1] {
        &self.transport_routes
    }
    pub fn consumers(&self) -> &[QueryTerminalRuntimeFilterConsumerV1] {
        &self.consumers
    }
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
            && self.producer_streams.is_empty()
            && self.transport_routes.is_empty()
            && self.consumers.is_empty()
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.put_canonical(&mut bytes);
        bytes
    }

    fn validate(&self) -> Result<(), QueryLifecycleError> {
        if self.version != QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1 {
            return Err(QueryLifecycleError::invalid_manifest(
                "unsupported query terminal profile contribution version",
            ));
        }
        for (section, len) in [
            ("channel", self.channels.len()),
            ("producer stream", self.producer_streams.len()),
            ("transport route", self.transport_routes.len()),
            ("consumer", self.consumers.len()),
        ] {
            if len > QUERY_TERMINAL_PROFILE_SECTION_MAX_ENTRIES {
                return Err(QueryLifecycleError::invalid_manifest(format!(
                    "terminal runtime-filter {section} section exceeds the cardinality limit"
                )));
            }
        }
        validate_sorted_unique(
            &self.channels,
            QueryTerminalRuntimeFilterChannelV1::key,
            "channel",
        )?;
        validate_sorted_unique(
            &self.producer_streams,
            QueryTerminalRuntimeFilterProducerStreamV1::key,
            "producer stream",
        )?;
        validate_sorted_unique(
            &self.transport_routes,
            QueryTerminalRuntimeFilterTransportRouteV1::key,
            "transport route",
        )?;
        validate_sorted_unique(
            &self.consumers,
            QueryTerminalRuntimeFilterConsumerV1::key,
            "consumer",
        )?;
        for value in &self.channels {
            value.validate()?;
        }
        for value in &self.producer_streams {
            value.validate()?;
        }
        for value in &self.transport_routes {
            value.validate()?;
        }
        for value in &self.consumers {
            value.validate()?;
        }
        for channel in self
            .producer_streams
            .iter()
            .map(|value| value.key().channel())
            .chain(
                self.transport_routes
                    .iter()
                    .map(|value| value.key().channel()),
            )
            .chain(self.consumers.iter().map(|value| value.key().channel()))
        {
            if self
                .channels
                .binary_search_by_key(&channel, QueryTerminalRuntimeFilterChannelV1::key)
                .is_err()
            {
                return Err(QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter section references an unknown channel",
                ));
            }
        }
        Ok(())
    }

    fn put_canonical(&self, bytes: &mut Vec<u8>) {
        put_u32(bytes, self.version);
        put_u64(bytes, self.channels.len() as u64);
        for value in &self.channels {
            put_channel_key(bytes, value.key);
            put_u8(bytes, 1);
            put_u8(
                bytes,
                match value.terminal_state {
                    QueryTerminalRuntimeFilterChannelTerminalStateV1::Open => 1,
                    QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed => 2,
                    QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable => 3,
                    QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled => 4,
                },
            );
            put_optional_u64(bytes, value.latest_published_logical_version);
            put_u64(bytes, value.published_count);
            put_u64(bytes, value.completed_count);
            put_u64(bytes, value.unavailable_count);
            put_u64(bytes, value.cancelled_count);
        }
        put_u64(bytes, self.producer_streams.len() as u64);
        for value in &self.producer_streams {
            put_channel_key(bytes, value.key.channel);
            put_unique_id(bytes, value.key.producer_fragment_instance_id);
            put_u32(bytes, value.key.partition_id);
            put_optional_u64(bytes, value.latest_accepted_sequence);
            put_u64(bytes, value.accepted_count);
            put_u64(bytes, value.duplicate_count);
            put_u64(bytes, value.stale_count);
            put_u64(bytes, value.conflict_count);
            put_u64(bytes, value.resource_limit_count);
        }
        put_u64(bytes, self.transport_routes.len() as u64);
        for value in &self.transport_routes {
            put_channel_key(bytes, value.key.channel);
            put_u64(bytes, value.key.route_edge_id);
            put_u64(bytes, value.sent_count);
            put_u64(bytes, value.sent_bytes);
            put_u64(bytes, value.retried_count);
            put_u64(bytes, value.retried_bytes);
            put_u64(bytes, value.acked_count);
            put_u64(bytes, value.acked_bytes);
            put_u64(bytes, value.fail_open_count);
            put_u64(bytes, value.fail_open_bytes);
        }
        put_u64(bytes, self.consumers.len() as u64);
        for value in &self.consumers {
            put_channel_key(bytes, value.key.channel);
            put_u32(bytes, value.key.consumer_binding_id);
            put_unique_id(bytes, value.key.fragment_instance_id);
            put_optional_u64(bytes, value.latest_delivered_logical_version);
            put_optional_u64(bytes, value.latest_applied_logical_version);
            put_u8(
                bytes,
                match value.subscription_terminal {
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending => 1,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired => 2,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut => 3,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable => 4,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported => 5,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled => 6,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed => 7,
                    QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact => 8,
                },
            );
            put_u64(bytes, value.row_evaluations);
            put_u64(bytes, value.input_rows);
            put_u64(bytes, value.output_rows);
            put_u64(bytes, value.scan_evaluated);
            put_u64(bytes, value.scan_kept);
            put_u64(bytes, value.scan_pruned);
            put_u64(bytes, value.scan_not_evaluated);
            put_scan_not_evaluated(bytes, value.scan_not_evaluated_reasons);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSnapshot {
    version: u32,
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    init_digest: ParticipantManifestDigest,
    fragments: Vec<FragmentTerminalSnapshot>,
    profile_contribution: TerminalTelemetry<QueryTerminalProfileContributionV1>,
    digest: QueryTerminalSnapshotDigest,
}

impl QueryTerminalSnapshot {
    /// Compatibility entrypoint for existing explicitly RF-less callers.
    /// New lifecycle code should select one of the named constructors below.
    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        fragments: Vec<FragmentTerminalSnapshot>,
    ) -> Result<Self, QueryLifecycleError> {
        Self::new_without_runtime_filters(execution_id, backend, init_digest, fragments)
    }

    pub fn new_without_runtime_filters(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        fragments: Vec<FragmentTerminalSnapshot>,
    ) -> Result<Self, QueryLifecycleError> {
        Self::new_with_profile_contribution(
            execution_id,
            backend,
            init_digest,
            fragments,
            QueryTerminalProfileContributionV1::empty(),
        )
    }

    pub fn new_with_profile_contribution(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        mut fragments: Vec<FragmentTerminalSnapshot>,
        profile_contribution: QueryTerminalProfileContributionV1,
    ) -> Result<Self, QueryLifecycleError> {
        Self::new_with_profile_telemetry(
            execution_id,
            backend,
            init_digest,
            fragments,
            TerminalTelemetry::Available(profile_contribution),
        )
    }

    pub fn new_with_profile_telemetry(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        mut fragments: Vec<FragmentTerminalSnapshot>,
        profile_contribution: TerminalTelemetry<QueryTerminalProfileContributionV1>,
    ) -> Result<Self, QueryLifecycleError> {
        fragments.sort_by_key(|fact| fact.fragment_instance_id());
        let mut ids = BTreeSet::new();
        for fragment in &fragments {
            if !ids.insert(fragment.fragment_instance_id()) {
                return Err(QueryLifecycleError::invalid_manifest(
                    "query terminal snapshot contains duplicate fragment facts",
                ));
            }
        }
        if let TerminalTelemetry::Available(contribution) = &profile_contribution {
            contribution.validate()?;
        }
        let mut snapshot = Self {
            version: QUERY_TERMINAL_SNAPSHOT_VERSION_V1,
            execution_id,
            backend,
            init_digest,
            fragments,
            profile_contribution,
            digest: QueryTerminalSnapshotDigest::new([0; 32]),
        };
        snapshot.digest = snapshot.compute_digest();
        Ok(snapshot)
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub fn fragments(&self) -> &[FragmentTerminalSnapshot] {
        &self.fragments
    }

    pub const fn profile_contribution(&self) -> Option<&QueryTerminalProfileContributionV1> {
        self.profile_contribution.available()
    }

    pub const fn profile_contribution_telemetry(
        &self,
    ) -> &TerminalTelemetry<QueryTerminalProfileContributionV1> {
        &self.profile_contribution
    }

    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        self.digest
    }

    pub fn is_success(&self) -> bool {
        self.fragments
            .iter()
            .all(|fragment| fragment.outcome.is_success())
    }

    pub fn validate(&self) -> Result<(), QueryLifecycleError> {
        if self.version != QUERY_TERMINAL_SNAPSHOT_VERSION_V1 {
            return Err(QueryLifecycleError::invalid_manifest(
                "unsupported query terminal snapshot version",
            ));
        }
        if let TerminalTelemetry::Available(contribution) = &self.profile_contribution {
            contribution.validate()?;
        }
        if self.compute_digest() != self.digest {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Conflict,
                "query terminal snapshot digest does not match canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, self.version);
        put_i64(&mut bytes, self.execution_id.query_id().high());
        put_i64(&mut bytes, self.execution_id.query_id().low());
        put_u64(&mut bytes, self.execution_id.attempt_id().get());
        put_u64(&mut bytes, self.backend.backend_id());
        let endpoint = validated_endpoint(&self.backend);
        put_string(&mut bytes, endpoint.host());
        put_u16(&mut bytes, endpoint.port());
        put_u64(&mut bytes, self.backend.start_epoch());
        put_bytes(&mut bytes, self.init_digest.as_bytes());
        put_u64(&mut bytes, self.fragments.len() as u64);
        for fragment in &self.fragments {
            put_i64(&mut bytes, fragment.fragment_instance_id.high());
            put_i64(&mut bytes, fragment.fragment_instance_id.low());
            put_i32(&mut bytes, fragment.backend_num);
            match &fragment.outcome {
                FragmentTerminalOutcome::Succeeded => put_u8(&mut bytes, 1),
                FragmentTerminalOutcome::Failed {
                    code,
                    detail,
                    detail_truncated,
                } => {
                    put_u8(&mut bytes, 2);
                    put_string(&mut bytes, code);
                    put_string(&mut bytes, detail);
                    put_u8(&mut bytes, u8::from(*detail_truncated));
                }
                FragmentTerminalOutcome::Cancelled {
                    detail,
                    detail_truncated,
                } => {
                    put_u8(&mut bytes, 3);
                    put_string(&mut bytes, detail);
                    put_u8(&mut bytes, u8::from(*detail_truncated));
                }
                FragmentTerminalOutcome::IncompleteDrain {
                    detail,
                    detail_truncated,
                } => {
                    put_u8(&mut bytes, 4);
                    put_string(&mut bytes, detail);
                    put_u8(&mut bytes, u8::from(*detail_truncated));
                }
            }
            put_sink(&mut bytes, &fragment.sink);
            match &fragment.profile {
                TerminalTelemetry::Available(profile) => {
                    put_u8(&mut bytes, 1);
                    put_profile(&mut bytes, profile);
                }
                TerminalTelemetry::Unavailable(reason) => {
                    put_u8(&mut bytes, 2);
                    put_string(&mut bytes, reason.stage());
                    put_string(&mut bytes, reason.code());
                }
            }
            put_bytes(&mut bytes, &fragment.statistics_payload);
        }
        match &self.profile_contribution {
            TerminalTelemetry::Available(contribution) => {
                put_u8(&mut bytes, 1);
                contribution.put_canonical(&mut bytes);
            }
            TerminalTelemetry::Unavailable(reason) => {
                put_u8(&mut bytes, 2);
                put_string(&mut bytes, reason.stage());
                put_string(&mut bytes, reason.code());
            }
        }
        bytes
    }

    fn compute_digest(&self) -> QueryTerminalSnapshotDigest {
        let mut hasher = Sha256::new();
        hasher.update(QUERY_TERMINAL_SNAPSHOT_V1_DOMAIN);
        hasher.update(self.canonical_bytes());
        QueryTerminalSnapshotDigest::new(hasher.finalize().into())
    }
}

fn truncate_utf8(value: String, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value, false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), true)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ImmutableQueryTerminalRecord {
    snapshot: QueryTerminalSnapshot,
    encoded: Vec<u8>,
}

impl ImmutableQueryTerminalRecord {
    pub fn new(
        snapshot: QueryTerminalSnapshot,
        max_encoded_bytes: usize,
    ) -> Result<Self, QueryLifecycleError> {
        snapshot.validate()?;
        let encoded = snapshot.canonical_bytes();
        if encoded.len() > max_encoded_bytes {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Capacity,
                "query terminal snapshot exceeds configured encoded-byte limit",
            ));
        }
        Ok(Self { snapshot, encoded })
    }

    pub const fn snapshot(&self) -> &QueryTerminalSnapshot {
        &self.snapshot
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }
}

fn put_fragment_outcome(bytes: &mut Vec<u8>, outcome: &FragmentTerminalOutcome) {
    match outcome {
        FragmentTerminalOutcome::Succeeded => put_u8(bytes, 1),
        FragmentTerminalOutcome::Failed {
            code,
            detail,
            detail_truncated,
        } => {
            put_u8(bytes, 2);
            put_string(bytes, code);
            put_string(bytes, detail);
            put_u8(bytes, u8::from(*detail_truncated));
        }
        FragmentTerminalOutcome::Cancelled {
            detail,
            detail_truncated,
        } => {
            put_u8(bytes, 3);
            put_string(bytes, detail);
            put_u8(bytes, u8::from(*detail_truncated));
        }
        FragmentTerminalOutcome::IncompleteDrain {
            detail,
            detail_truncated,
        } => {
            put_u8(bytes, 4);
            put_string(bytes, detail);
            put_u8(bytes, u8::from(*detail_truncated));
        }
    }
}

fn put_sink(bytes: &mut Vec<u8>, sink: &SinkCommitReportSnapshot) {
    let mut connector = sink
        .connector_staged_report_frames
        .iter()
        .map(encode_connector_staged_report_frame)
        .map(|frame| canonical_connector_staged_report_frame(&frame))
        .collect::<Vec<_>>();
    connector.sort();
    put_u64(bytes, connector.len() as u64);
    for fact in connector {
        put_bytes(bytes, &fact);
    }
    let mut committed = sink
        .tablet_commit_infos
        .iter()
        .map(|fact| (fact.tablet_id, fact.backend_id))
        .collect::<Vec<_>>();
    committed.sort_unstable();
    put_u64(bytes, committed.len() as u64);
    for (tablet, backend) in committed {
        put_i64(bytes, tablet);
        put_i64(bytes, backend);
    }
    let mut failed = sink
        .tablet_fail_infos
        .iter()
        .map(|fact| (fact.tablet_id, fact.backend_id))
        .collect::<Vec<_>>();
    failed.sort_unstable();
    put_u64(bytes, failed.len() as u64);
    for (tablet, backend) in failed {
        put_i64(bytes, tablet);
        put_i64(bytes, backend);
    }
    put_i64(bytes, sink.load_stats.loaded_rows);
    put_i64(bytes, sink.load_stats.loaded_bytes);
    put_i64(bytes, sink.load_stats.filtered_rows);
}

/// Encodes an opaque provider staged-report frame for lifecycle terminal
/// values. The protocol adapter belongs with the terminal wire family so a
/// retained lifecycle value never depends on Frontend query assembly.
pub fn encode_connector_staged_report_frame(
    frame: &novarocks_spi::connector::ConnectorStagedReportFrame,
) -> novarocks::ConnectorStagedReportFrame {
    let writer = frame.writer();
    let fragment_instance_id = writer.fragment_instance_id();
    novarocks::ConnectorStagedReportFrame {
        contract_version: frame.version(),
        writer: Some(novarocks_protocol::plan::ConnectorWriterIdentity {
            operation_id: writer.operation_id().to_bytes().to_vec(),
            cohort_id: writer.cohort_id().to_bytes().to_vec(),
            execution_query_id: writer.execution_id().query_id().to_vec(),
            execution_attempt_id: writer.execution_id().attempt_id(),
            fragment_instance_id: Some(common::UniqueId {
                hi: i64::from_be_bytes(
                    fragment_instance_id[..8]
                        .try_into()
                        .expect("fixed UUID prefix"),
                ),
                lo: i64::from_be_bytes(
                    fragment_instance_id[8..]
                        .try_into()
                        .expect("fixed UUID suffix"),
                ),
            }),
            fragment_id: writer.fragment_id(),
            backend_num: writer.backend_num(),
            sink_ordinal: writer.sink_ordinal(),
            connector_instance_id: writer.binding_key().instance_id.as_str().to_string(),
            connector_incarnation: writer.binding_key().incarnation.to_bytes().to_vec(),
        }),
        terminal_state: match frame.state() {
            ConnectorWriterTerminalState::Staged => CONNECTOR_WRITER_TERMINAL_STAGED,
            ConnectorWriterTerminalState::Aborted => 1,
            ConnectorWriterTerminalState::Failed => 2,
        },
        input_rows: frame.summary().input_rows,
        staged_bytes: frame.summary().staged_bytes,
        artifact_count: frame.summary().artifact_count,
        part_index: frame.part_index(),
        part_count: frame.part_count(),
        logical_payload_len: frame.logical_payload_len(),
        logical_payload_sha256: frame.logical_payload_digest().to_vec(),
        frame_payload: frame.frame_payload().to_vec(),
        frame_payload_sha256: frame.frame_payload_digest().to_vec(),
    }
}

fn canonical_connector_staged_report_frame(
    frame: &novarocks::ConnectorStagedReportFrame,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_u32(&mut bytes, frame.contract_version);
    match &frame.writer {
        Some(writer) => {
            put_u8(&mut bytes, 1);
            put_bytes(&mut bytes, &writer.operation_id);
            put_bytes(&mut bytes, &writer.cohort_id);
            put_bytes(&mut bytes, &writer.execution_query_id);
            put_u64(&mut bytes, writer.execution_attempt_id);
            match &writer.fragment_instance_id {
                Some(id) => {
                    put_u8(&mut bytes, 1);
                    put_i64(&mut bytes, id.hi);
                    put_i64(&mut bytes, id.lo);
                }
                None => put_u8(&mut bytes, 0),
            }
            put_i32(&mut bytes, writer.fragment_id);
            put_i32(&mut bytes, writer.backend_num);
            put_u32(&mut bytes, writer.sink_ordinal);
            put_string(&mut bytes, &writer.connector_instance_id);
            put_bytes(&mut bytes, &writer.connector_incarnation);
        }
        None => put_u8(&mut bytes, 0),
    }
    put_u32(&mut bytes, frame.terminal_state);
    put_u64(&mut bytes, frame.input_rows);
    put_u64(&mut bytes, frame.staged_bytes);
    put_u64(&mut bytes, frame.artifact_count);
    put_u32(&mut bytes, frame.part_index);
    put_u32(&mut bytes, frame.part_count);
    put_u64(&mut bytes, frame.logical_payload_len);
    put_bytes(&mut bytes, &frame.logical_payload_sha256);
    put_bytes(&mut bytes, &frame.frame_payload);
    put_bytes(&mut bytes, &frame.frame_payload_sha256);
    bytes
}

/// Profile wire messages contain map fields, so their ordinary prost encoding
/// is not a deterministic digest input. Encode the typed tree directly:
/// repeated counters and children keep semantic order while BTreeMap-backed
/// info strings use key order.
fn put_profile(bytes: &mut Vec<u8>, profile: &RuntimeProfileTree) {
    put_profile_node(bytes, &profile.root);
}

fn put_profile_node(
    bytes: &mut Vec<u8>,
    node: &novarocks_execution::runtime::profile::ProfileNode,
) {
    put_string(bytes, &node.name);
    put_i32(bytes, node.node_id);
    put_u64(bytes, node.counters.len() as u64);
    for counter in &node.counters {
        put_string(bytes, &counter.name);
        put_string(bytes, &counter.parent_name);
        put_i32(
            bytes,
            crate::runtime::profile_codec::encode_profile_unit_value(counter.unit),
        );
        put_i64(bytes, counter.value);
        put_optional_i64(bytes, counter.min_value);
        put_optional_i64(bytes, counter.max_value);
    }
    put_u64(bytes, node.info_strings.len() as u64);
    for (key, value) in &node.info_strings {
        put_string(bytes, key);
        put_string(bytes, value);
    }
    put_u64(bytes, node.children.len() as u64);
    for child in &node.children {
        put_profile_node(bytes, child);
    }
}

fn put_optional_i64(bytes: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            put_u8(bytes, 1);
            put_i64(bytes, value);
        }
        None => put_u8(bytes, 0),
    }
}

fn put_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            put_u8(bytes, 1);
            put_u64(bytes, value);
        }
        None => put_u8(bytes, 0),
    }
}

fn put_unique_id(bytes: &mut Vec<u8>, value: UniqueId) {
    put_i64(bytes, value.high());
    put_i64(bytes, value.low());
}

fn put_channel_key(bytes: &mut Vec<u8>, value: QueryTerminalRuntimeFilterChannelKeyV1) {
    put_u32(bytes, value.channel_binding_id);
    put_u32(bytes, value.channel_id);
}

fn put_scan_not_evaluated(
    bytes: &mut Vec<u8>,
    value: QueryTerminalRuntimeFilterScanNotEvaluatedV1,
) {
    put_u64(bytes, value.unit_facts_missing);
    put_u64(bytes, value.column_facts_missing);
    put_u64(bytes, value.data_type_unsupported);
    put_u64(bytes, value.predicate_capability_unsupported);
    put_u64(bytes, value.resource_unavailable);
    put_u64(bytes, value.snapshot_unavailable);
    put_u64(bytes, value.snapshot_timed_out);
    put_u64(bytes, value.snapshot_not_published);
}

fn validate_optional_nonzero(
    value: Option<u64>,
    detail: &'static str,
) -> Result<(), QueryLifecycleError> {
    if value == Some(0) {
        return Err(QueryLifecycleError::invalid_manifest(detail));
    }
    Ok(())
}

fn validate_unique_id(value: UniqueId, detail: &'static str) -> Result<(), QueryLifecycleError> {
    if value.high() == 0 && value.low() == 0 {
        return Err(QueryLifecycleError::invalid_manifest(detail));
    }
    Ok(())
}

fn checked_sum<const N: usize>(
    values: [u64; N],
    detail: &'static str,
) -> Result<u64, QueryLifecycleError> {
    values.into_iter().try_fold(0_u64, |sum, value| {
        sum.checked_add(value)
            .ok_or_else(|| QueryLifecycleError::invalid_manifest(detail))
    })
}

fn validate_sorted_unique<T, K>(
    values: &[T],
    key: impl Fn(&T) -> K,
    label: &'static str,
) -> Result<(), QueryLifecycleError>
where
    K: Ord,
{
    for pair in values.windows(2) {
        let previous = key(&pair[0]);
        let current = key(&pair[1]);
        if previous >= current {
            return Err(QueryLifecycleError::invalid_manifest(format!(
                "query terminal profile contribution contains duplicate or unsorted {label} identity"
            )));
        }
    }
    Ok(())
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}
fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}
fn put_string(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn decode_terminal_telemetry_unavailable(
    value: &novarocks::TerminalTelemetryUnavailable,
) -> Result<TerminalTelemetryUnavailable, QueryLifecycleError> {
    TerminalTelemetryUnavailable::new(value.stage.clone(), value.code.clone())
}

pub fn decode_fragment_terminal_profile_telemetry(
    value: &novarocks::FragmentTerminalProfileTelemetry,
) -> Result<TerminalTelemetry<RuntimeProfileTree>, QueryLifecycleError> {
    use novarocks::fragment_terminal_profile_telemetry::Telemetry;

    match value.telemetry.as_ref().ok_or_else(|| {
        QueryLifecycleError::invalid_manifest("terminal fragment profile telemetry is required")
    })? {
        Telemetry::Available(profile) => {
            crate::runtime::profile_codec::decode_runtime_profile_tree(profile)
                .map(TerminalTelemetry::Available)
                .map_err(QueryLifecycleError::invalid_manifest)
        }
        Telemetry::Unavailable(reason) => {
            decode_terminal_telemetry_unavailable(reason).map(TerminalTelemetry::Unavailable)
        }
    }
}

pub fn decode_query_terminal_profile_contribution_telemetry(
    value: &novarocks::QueryTerminalProfileContributionTelemetry,
) -> Result<TerminalTelemetry<QueryTerminalProfileContributionV1>, QueryLifecycleError> {
    use novarocks::query_terminal_profile_contribution_telemetry::Telemetry;

    match value.telemetry.as_ref().ok_or_else(|| {
        QueryLifecycleError::invalid_manifest(
            "query terminal profile contribution telemetry is required",
        )
    })? {
        Telemetry::Available(contribution) => {
            decode_query_terminal_profile_contribution(contribution)
                .map(TerminalTelemetry::Available)
        }
        Telemetry::Unavailable(reason) => {
            decode_terminal_telemetry_unavailable(reason).map(TerminalTelemetry::Unavailable)
        }
    }
}

fn decode_query_terminal_profile_contribution(
    contribution: &novarocks::QueryTerminalProfileContributionV1,
) -> Result<QueryTerminalProfileContributionV1, QueryLifecycleError> {
    use {
        QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
        QueryTerminalRuntimeFilterChannelInstallStateV1, QueryTerminalRuntimeFilterChannelKeyV1,
        QueryTerminalRuntimeFilterChannelTerminalStateV1, QueryTerminalRuntimeFilterChannelV1,
        QueryTerminalRuntimeFilterConsumerKeyV1, QueryTerminalRuntimeFilterConsumerV1,
        QueryTerminalRuntimeFilterProducerStreamKeyV1, QueryTerminalRuntimeFilterProducerStreamV1,
        QueryTerminalRuntimeFilterScanNotEvaluatedV1,
        QueryTerminalRuntimeFilterSubscriptionTerminalV1,
        QueryTerminalRuntimeFilterTransportRouteKeyV1, QueryTerminalRuntimeFilterTransportRouteV1,
    };

    if contribution.version != QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1 {
        return Err(QueryLifecycleError::invalid_manifest(
            "unsupported query terminal profile contribution wire version",
        ));
    }
    let channel_key = |binding_id, channel_id| {
        QueryTerminalRuntimeFilterChannelKeyV1::new(binding_id, channel_id)
    };
    let channels = contribution
        .channels
        .iter()
        .map(|value| {
            let install_state = match value.install_state {
                1 => QueryTerminalRuntimeFilterChannelInstallStateV1::Installed,
                _ => {
                    return Err(QueryLifecycleError::invalid_manifest(
                        "invalid terminal runtime-filter channel install state",
                    ));
                }
            };
            let terminal_state = match value.terminal_state {
                1 => QueryTerminalRuntimeFilterChannelTerminalStateV1::Open,
                2 => QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed,
                3 => QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable,
                4 => QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled,
                _ => {
                    return Err(QueryLifecycleError::invalid_manifest(
                        "invalid terminal runtime-filter channel terminal state",
                    ));
                }
            };
            Ok(QueryTerminalRuntimeFilterChannelV1::new(
                channel_key(value.channel_binding_id, value.channel_id),
                install_state,
                terminal_state,
                value.latest_published_logical_version,
                value.published_count,
                value.completed_count,
                value.unavailable_count,
                value.cancelled_count,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let producer_streams = contribution
        .producer_streams
        .iter()
        .map(|value| {
            let fragment = value
                .producer_fragment_instance_id
                .as_ref()
                .ok_or_else(|| {
                    QueryLifecycleError::invalid_manifest(
                        "terminal runtime-filter producer fragment instance id is required",
                    )
                })?;
            Ok(QueryTerminalRuntimeFilterProducerStreamV1::new(
                QueryTerminalRuntimeFilterProducerStreamKeyV1::new(
                    channel_key(value.channel_binding_id, value.channel_id),
                    novarocks_types::UniqueId::new(fragment.hi, fragment.lo),
                    value.partition_id,
                ),
                value.latest_accepted_sequence,
                value.accepted_count,
                value.duplicate_count,
                value.stale_count,
                value.conflict_count,
                value.resource_limit_count,
            ))
        })
        .collect::<Result<Vec<_>, QueryLifecycleError>>()?;
    let transport_routes = contribution
        .transport_routes
        .iter()
        .map(|value| {
            QueryTerminalRuntimeFilterTransportRouteV1::new(
                QueryTerminalRuntimeFilterTransportRouteKeyV1::new(
                    channel_key(value.channel_binding_id, value.channel_id),
                    value.route_edge_id,
                ),
                value.sent_count,
                value.sent_bytes,
                value.retried_count,
                value.retried_bytes,
                value.acked_count,
                value.acked_bytes,
                value.fail_open_count,
                value.fail_open_bytes,
            )
        })
        .collect();
    let consumers = contribution
        .consumers
        .iter()
        .map(|value| {
            let fragment = value.fragment_instance_id.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter consumer fragment instance id is required",
                )
            })?;
            let reasons = value.scan_not_evaluated_reasons.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest(
                    "terminal runtime-filter scan not-evaluated counters are required",
                )
            })?;
            let terminal = match value.subscription_terminal {
                1 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending,
                2 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired,
                3 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut,
                4 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable,
                5 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported,
                6 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled,
                7 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed,
                8 => QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact,
                _ => {
                    return Err(QueryLifecycleError::invalid_manifest(
                        "invalid terminal runtime-filter subscription terminal state",
                    ));
                }
            };
            Ok(QueryTerminalRuntimeFilterConsumerV1::new(
                QueryTerminalRuntimeFilterConsumerKeyV1::new(
                    channel_key(value.channel_binding_id, value.channel_id),
                    value.consumer_binding_id,
                    novarocks_types::UniqueId::new(fragment.hi, fragment.lo),
                ),
                value.latest_delivered_logical_version,
                value.latest_applied_logical_version,
                terminal,
                value.row_evaluations,
                value.input_rows,
                value.output_rows,
                value.scan_evaluated,
                value.scan_kept,
                value.scan_pruned,
                value.scan_not_evaluated,
                QueryTerminalRuntimeFilterScanNotEvaluatedV1::new(
                    reasons.unit_facts_missing,
                    reasons.column_facts_missing,
                    reasons.data_type_unsupported,
                    reasons.predicate_capability_unsupported,
                    reasons.resource_unavailable,
                    reasons.snapshot_unavailable,
                    reasons.snapshot_timed_out,
                    reasons.snapshot_not_published,
                ),
            ))
        })
        .collect::<Result<Vec<_>, QueryLifecycleError>>()?;
    QueryTerminalProfileContributionV1::try_new(
        channels,
        producer_streams,
        transport_routes,
        consumers,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_lifecycle::{AttemptId, ParticipantRole, QueryControlEndpoint};
    use novarocks_execution::runtime::profile::{
        CounterStrategy, ProfileCounter, ProfileNode, ProfileUnit, RuntimeProfileTree,
    };
    use novarocks_protocol::{common, novarocks};
    use novarocks_types::QueryId;

    fn snapshot(fragment_ids: &[i64]) -> QueryTerminalSnapshot {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let facts = fragment_ids
            .iter()
            .map(|low| {
                FragmentTerminalSnapshot::new(
                    UniqueId::new(0, *low),
                    0,
                    FragmentTerminalOutcome::Succeeded,
                    SinkCommitReportSnapshot::default(),
                    None,
                )
                .unwrap()
            })
            .collect();
        QueryTerminalSnapshot::new(
            execution,
            backend,
            ParticipantManifestDigest::new([7; 32]),
            facts,
        )
        .unwrap()
    }

    #[test]
    fn terminal_outcome_bounds_utf8_diagnostics_without_touching_p1() {
        let detail = "测".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
        let snapshot = FragmentTerminalSnapshot::new(
            UniqueId::new(7, 8),
            1,
            FragmentTerminalOutcome::Failed {
                code: "C".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES + 1),
                detail,
                detail_truncated: false,
            },
            SinkCommitReportSnapshot::default(),
            None,
        )
        .expect("bounded terminal snapshot");
        match snapshot.outcome() {
            FragmentTerminalOutcome::Failed {
                code,
                detail,
                detail_truncated,
            } => {
                assert_eq!(code.len(), QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES);
                assert!(detail.len() <= QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES);
                assert!(detail.is_char_boundary(detail.len()));
                assert!(*detail_truncated);
            }
            outcome => panic!("expected failed outcome, got {outcome:?}"),
        }
    }

    #[test]
    fn proof_is_independent_of_snapshot_p1_and_has_a_stable_digest() {
        let snapshot = snapshot(&[3, 1]);
        let proof = TerminalizationProof::from_snapshot(&snapshot).expect("proof");
        assert_eq!(proof.fragments().len(), 2);
        assert!(proof.validate().is_ok());
        assert_eq!(
            ParticipantTerminalOutcome::proof(snapshot.clone())
                .expect("outcome")
                .digest(),
            proof.digest()
        );
    }

    #[test]
    fn p0_proof_bound_covers_the_worst_case_fragment_outcomes() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let ids = [UniqueId::new(0, 1), UniqueId::new(0, 2)];
        let manifest = ParticipantManifest::parse(novarocks::ParticipantManifest {
            execution_id: Some(execution.to_proto()),
            backend: Some(backend.as_proto().clone()),
            participant_roles: vec![i32::from(ParticipantRole::FragmentExecutor)],
            expected_fragment_instance_ids: ids
                .iter()
                .map(|id| common::UniqueId {
                    hi: id.high(),
                    lo: id.low(),
                })
                .collect(),
            query_options: Some(novarocks::QueryOptions::default()),
            query_deadline_unix_ms: 1_000,
            exchange_routes: Vec::new(),
            runtime_filter: None,
            pre_start_timeout_ms: 30_000,
            report_endpoint: Some(
                QueryControlEndpoint::new("127.0.0.1", 9031)
                    .unwrap()
                    .as_proto()
                    .clone(),
            ),
        })
        .unwrap();
        let facts = ids
            .into_iter()
            .map(|id| {
                FragmentTerminalSnapshot::new(
                    id,
                    0,
                    FragmentTerminalOutcome::Failed {
                        code: "C".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_CODE_MAX_BYTES),
                        detail: "D".repeat(QUERY_TERMINAL_FRAGMENT_OUTCOME_DETAIL_MAX_BYTES),
                        detail_truncated: false,
                    },
                    SinkCommitReportSnapshot::default(),
                    None,
                )
                .unwrap()
            })
            .collect();
        let snapshot = QueryTerminalSnapshot::new(
            execution,
            backend,
            ParticipantManifestDigest::new([7; 32]),
            facts,
        )
        .unwrap();
        assert!(
            TerminalizationProof::from_snapshot(&snapshot)
                .unwrap()
                .canonical_bytes()
                .len()
                <= p0_max_encoded_len(&manifest)
        );
    }

    #[test]
    fn p2_unavailable_is_explicit_and_does_not_block_proof_construction() {
        let fragment = FragmentTerminalSnapshot::new_with_profile_telemetry(
            UniqueId::new(7, 8),
            1,
            FragmentTerminalOutcome::Succeeded,
            SinkCommitReportSnapshot::default(),
            TerminalTelemetry::unavailable("profile_assembly", "BUDGET_EXHAUSTED").unwrap(),
        )
        .unwrap();
        let snapshot = QueryTerminalSnapshot::new_with_profile_telemetry(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            ParticipantBackendIdentity::new(
                1,
                QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
                1,
            )
            .unwrap(),
            ParticipantManifestDigest::new([7; 32]),
            vec![fragment],
            TerminalTelemetry::unavailable("observation_assembly", "BUDGET_EXHAUSTED").unwrap(),
        )
        .unwrap();
        assert_eq!(
            snapshot
                .profile_contribution_telemetry()
                .unavailable_reason()
                .unwrap()
                .code(),
            "BUDGET_EXHAUSTED"
        );
        assert!(TerminalizationProof::from_snapshot(&snapshot).is_ok());
    }

    #[test]
    fn terminal_snapshot_digest_is_order_independent_for_fragment_facts() {
        let first = snapshot(&[2, 1]);
        let second = snapshot(&[1, 2]);
        assert_eq!(first.digest(), second.digest());
        assert_eq!(first.fragments()[0].fragment_instance_id().low(), 1);
    }

    #[test]
    fn terminal_snapshot_rejects_duplicate_fragments() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let fact = FragmentTerminalSnapshot::new(
            UniqueId::new(0, 1),
            0,
            FragmentTerminalOutcome::Succeeded,
            SinkCommitReportSnapshot::default(),
            None,
        )
        .unwrap();
        assert!(
            QueryTerminalSnapshot::new(
                execution,
                backend,
                ParticipantManifestDigest::new([7; 32]),
                vec![fact.clone(), fact]
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_record_enforces_encoded_limit() {
        let snapshot = snapshot(&[1]);
        assert!(ImmutableQueryTerminalRecord::new(snapshot, 1).is_err());
    }

    fn channel(binding_id: u32, channel_id: u32) -> QueryTerminalRuntimeFilterChannelV1 {
        QueryTerminalRuntimeFilterChannelV1::new(
            QueryTerminalRuntimeFilterChannelKeyV1::new(binding_id, channel_id),
            QueryTerminalRuntimeFilterChannelInstallStateV1::Installed,
            QueryTerminalRuntimeFilterChannelTerminalStateV1::Open,
            Some(1),
            1,
            0,
            0,
            0,
        )
    }

    fn non_empty_contribution() -> QueryTerminalProfileContributionV1 {
        let channel_key = QueryTerminalRuntimeFilterChannelKeyV1::new(7, 9);
        QueryTerminalProfileContributionV1::try_new(
            vec![channel(7, 9)],
            vec![QueryTerminalRuntimeFilterProducerStreamV1::new(
                QueryTerminalRuntimeFilterProducerStreamKeyV1::new(
                    channel_key,
                    UniqueId::new(3, 4),
                    0,
                ),
                Some(2),
                2,
                1,
                1,
                0,
                0,
            )],
            vec![QueryTerminalRuntimeFilterTransportRouteV1::new(
                QueryTerminalRuntimeFilterTransportRouteKeyV1::new(channel_key, 11),
                2,
                20,
                1,
                10,
                2,
                20,
                1,
                10,
            )],
            vec![QueryTerminalRuntimeFilterConsumerV1::new(
                QueryTerminalRuntimeFilterConsumerKeyV1::new(channel_key, 13, UniqueId::new(5, 6)),
                Some(2),
                Some(1),
                QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired,
                1,
                10,
                4,
                2,
                1,
                1,
                1,
                QueryTerminalRuntimeFilterScanNotEvaluatedV1::new(1, 0, 0, 0, 0, 0, 0, 0),
            )],
        )
        .expect("valid non-empty contribution")
    }

    #[test]
    fn terminal_profile_contribution_sorts_sections_and_rejects_duplicate_identity() {
        let first = QueryTerminalProfileContributionV1::try_new(
            vec![channel(2, 2), channel(1, 1)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("canonical contribution");
        let second = QueryTerminalProfileContributionV1::try_new(
            vec![channel(1, 1), channel(2, 2)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("canonical contribution");
        assert_eq!(first, second);
        assert_eq!(first.canonical_bytes(), second.canonical_bytes());
        assert!(
            QueryTerminalProfileContributionV1::try_new(
                vec![channel(1, 1), channel(1, 1)],
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn transport_fail_open_before_send_is_a_valid_terminal_outcome() {
        let channel_key = QueryTerminalRuntimeFilterChannelKeyV1::new(7, 9);
        let contribution = QueryTerminalProfileContributionV1::try_new(
            vec![channel(7, 9)],
            Vec::new(),
            vec![QueryTerminalRuntimeFilterTransportRouteV1::new(
                QueryTerminalRuntimeFilterTransportRouteKeyV1::new(channel_key, 11),
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                128,
            )],
            Vec::new(),
        )
        .expect("pre-send fail-open is truthful without a synthetic send");

        assert_eq!(contribution.transport_routes()[0].sent_count(), 0);
        assert_eq!(contribution.transport_routes()[0].fail_open_count(), 1);
    }

    #[test]
    fn terminal_profile_contribution_validates_counter_invariants() {
        let invalid = QueryTerminalRuntimeFilterConsumerV1::new(
            QueryTerminalRuntimeFilterConsumerKeyV1::new(
                QueryTerminalRuntimeFilterChannelKeyV1::new(1, 1),
                2,
                UniqueId::new(0, 1),
            ),
            Some(1),
            Some(1),
            QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired,
            1,
            4,
            5,
            1,
            1,
            1,
            0,
            QueryTerminalRuntimeFilterScanNotEvaluatedV1::default(),
        );
        assert!(
            QueryTerminalProfileContributionV1::try_new(
                vec![channel(1, 1)],
                Vec::new(),
                Vec::new(),
                vec![invalid],
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_profile_contribution_accepts_zero_based_producer_sequence() {
        let channel_key = QueryTerminalRuntimeFilterChannelKeyV1::new(1, 1);
        let contribution = QueryTerminalProfileContributionV1::try_new(
            vec![channel(1, 1)],
            vec![QueryTerminalRuntimeFilterProducerStreamV1::new(
                QueryTerminalRuntimeFilterProducerStreamKeyV1::new(
                    channel_key,
                    UniqueId::new(2, 3),
                    0,
                ),
                Some(0),
                1,
                0,
                0,
                0,
                0,
            )],
            Vec::new(),
            Vec::new(),
        )
        .expect("producer sequences are zero-based");
        assert_eq!(
            contribution.producer_streams()[0].latest_accepted_sequence(),
            Some(0)
        );
    }

    #[test]
    fn terminal_profile_contribution_rejects_orphan_section_channel() {
        let orphan = QueryTerminalRuntimeFilterProducerStreamV1::new(
            QueryTerminalRuntimeFilterProducerStreamKeyV1::new(
                QueryTerminalRuntimeFilterChannelKeyV1::new(2, 2),
                UniqueId::new(3, 4),
                0,
            ),
            Some(0),
            1,
            0,
            0,
            0,
            0,
        );
        assert!(
            QueryTerminalProfileContributionV1::try_new(
                vec![channel(1, 1)],
                vec![orphan],
                Vec::new(),
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_snapshot_digest_and_wire_include_non_empty_profile_contribution() {
        let empty = snapshot(&[1]);
        let non_empty = QueryTerminalSnapshot::new_with_profile_contribution(
            empty.execution_id(),
            empty.backend().clone(),
            empty.init_digest(),
            empty.fragments().to_vec(),
            non_empty_contribution(),
        )
        .expect("terminal snapshot with contribution");
        assert_ne!(empty.digest(), non_empty.digest());
    }

    #[test]
    fn query_lifecycle_terminal_snapshot_profile_digest_is_canonical() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let profile = RuntimeProfileTree {
            root: ProfileNode {
                name: "fragment".to_string(),
                node_id: 7,
                counters: vec![ProfileCounter {
                    name: "Rows".to_string(),
                    parent_name: String::new(),
                    unit: ProfileUnit::Unit,
                    strategy: CounterStrategy::new(
                        novarocks_execution::runtime::profile::CounterAggregateType::Sum,
                    ),
                    value: 11,
                    min_value: Some(3),
                    max_value: Some(8),
                }],
                info_strings: [
                    ("alpha".to_string(), "first".to_string()),
                    ("omega".to_string(), "last".to_string()),
                ]
                .into_iter()
                .collect(),
                children: vec![ProfileNode {
                    name: "child".to_string(),
                    node_id: 8,
                    counters: Vec::new(),
                    info_strings: Default::default(),
                    children: Vec::new(),
                }],
            },
        };
        let fact = FragmentTerminalSnapshot::new(
            UniqueId::new(0, 1),
            0,
            FragmentTerminalOutcome::Succeeded,
            SinkCommitReportSnapshot::default(),
            Some(profile),
        )
        .unwrap();
        let snapshot = QueryTerminalSnapshot::new(
            execution,
            backend,
            ParticipantManifestDigest::new([7; 32]),
            vec![fact],
        )
        .unwrap();

        assert_eq!(
            snapshot.digest().as_bytes(),
            &[
                193, 68, 231, 25, 65, 110, 75, 9, 55, 1, 255, 153, 201, 135, 100, 91, 66, 217, 212,
                234, 246, 84, 168, 137, 146, 207, 33, 57, 151, 166, 40, 122,
            ]
        );
        snapshot.validate().expect("profile digest stays canonical");
    }
}
