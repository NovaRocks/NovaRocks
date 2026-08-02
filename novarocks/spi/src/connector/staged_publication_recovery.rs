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

//! Provider-neutral, cross-incarnation inspection for a staged publication.
//!
//! This is deliberately not a second write reconciliation API.  A current
//! connector generation may inspect durable lake truth for a historical
//! attempt and, after an application decision, remove only the exact staged
//! ref proven by that inspection.  It must never replay an old write or
//! publication operation.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorInstanceDescriptor, ConnectorMutationOperationId, ConnectorRequestContext,
    ConnectorTableIdentity, ExternalMutationEvidence, ExternalMutationOutcome,
};

pub const MAX_CONNECTOR_STAGED_PUBLICATION_PROOF_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS: usize = 4096;
pub const MAX_CONNECTOR_STAGED_PUBLICATION_COHORTS: usize = 4096;
pub const MAX_CONNECTOR_STAGED_PUBLICATION_LINEAGE_FACTS: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedPublicationPhase {
    StagingCreate,
    Write,
    Publication,
    StagingDrop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedPublicationPhaseState {
    Prepared,
    KnownUncommitted,
    KnownCommitted,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorHistoricalPublicationAction {
    pub phase: ConnectorStagedPublicationPhase,
    pub state: ConnectorStagedPublicationPhaseState,
    pub operation_id: ConnectorMutationOperationId,
    pub committed_version: Option<ConnectorCommittedVersion>,
    pub evidence_digest: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedPublicationBaseFact {
    pub table: Arc<str>,
    pub uuid: Arc<str>,
    pub from_version: Option<i64>,
    pub to_version: i64,
}

/// Complete value-only description of a historical staged publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedPublicationDescriptor {
    pub historical_binding: ConnectorExecutionBindingKey,
    pub table: ConnectorTableIdentity,
    pub staging_ref: Arc<str>,
    pub target_ref: Arc<str>,
    pub expected_target_version: Option<ConnectorCommittedVersion>,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub request_id: [u8; 16],
    pub marker_token: Arc<str>,
    pub cohort_ids: Vec<[u8; 32]>,
    pub cohort_set_digest: [u8; 32],
    pub actions: Vec<ConnectorHistoricalPublicationAction>,
    pub bases: Vec<ConnectorStagedPublicationBaseFact>,
    digest: [u8; 32],
}

impl ConnectorStagedPublicationDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        historical_binding: ConnectorExecutionBindingKey,
        table: ConnectorTableIdentity,
        staging_ref: impl Into<Arc<str>>,
        target_ref: impl Into<Arc<str>>,
        expected_target_version: Option<ConnectorCommittedVersion>,
        refresh_id: i64,
        mv_id: i64,
        request_id: [u8; 16],
        marker_token: impl Into<Arc<str>>,
        cohort_ids: Vec<[u8; 32]>,
        cohort_set_digest: [u8; 32],
        actions: Vec<ConnectorHistoricalPublicationAction>,
        bases: Vec<ConnectorStagedPublicationBaseFact>,
    ) -> Result<Self, ConnectorError> {
        let staging_ref = staging_ref.into();
        let target_ref = target_ref.into();
        let marker_token = marker_token.into();
        if refresh_id <= 0
            || mv_id <= 0
            || staging_ref.is_empty()
            || target_ref.is_empty()
            || marker_token.is_empty()
        {
            return Err(invalid(
                "staged publication descriptor has an invalid identity",
            ));
        }
        if cohort_ids.is_empty() || cohort_ids.len() > MAX_CONNECTOR_STAGED_PUBLICATION_COHORTS {
            return Err(invalid(
                "staged publication descriptor has an invalid cohort set",
            ));
        }
        if actions.len() != 4 || bases.len() > MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS {
            return Err(invalid(
                "staged publication descriptor has an invalid action or base set",
            ));
        }
        for action in &actions {
            if let Some(version) = &action.committed_version {
                version.validate()?;
            }
        }
        for base in &bases {
            if base.table.is_empty() || base.uuid.is_empty() || base.to_version <= 0 {
                return Err(invalid(
                    "staged publication descriptor has an invalid base fact",
                ));
            }
        }
        let digest = descriptor_digest(
            &historical_binding,
            &table,
            &staging_ref,
            &target_ref,
            expected_target_version.as_ref(),
            refresh_id,
            mv_id,
            request_id,
            &marker_token,
            &cohort_ids,
            cohort_set_digest,
            &actions,
            &bases,
        );
        Ok(Self {
            historical_binding,
            table,
            staging_ref,
            target_ref,
            expected_target_version,
            refresh_id,
            mv_id,
            request_id,
            marker_token,
            cohort_ids,
            cohort_set_digest,
            actions,
            bases,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.historical_binding.clone(),
            self.table.clone(),
            self.staging_ref.clone(),
            self.target_ref.clone(),
            self.expected_target_version.clone(),
            self.refresh_id,
            self.mv_id,
            self.request_id,
            self.marker_token.clone(),
            self.cohort_ids.clone(),
            self.cohort_set_digest,
            self.actions.clone(),
            self.bases.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "staged publication descriptor digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorStagedPublicationProof {
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorStagedPublicationProof {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        if payload.is_empty() || payload.len() > MAX_CONNECTOR_STAGED_PUBLICATION_PROOF_BYTES {
            return Err(invalid(
                "staged publication proof exceeds its bounded payload limit",
            ));
        }
        Ok(Self {
            digest: Sha256::digest(&payload).into(),
            payload,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(self.payload.clone())?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "staged publication proof digest does not match its payload",
            ));
        }
        Ok(())
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl fmt::Debug for ConnectorStagedPublicationProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorStagedPublicationProof")
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedPublicationDisposition {
    KnownUncommitted,
    Staged,
    Published,
    Superseded,
    CleanupPending,
    Ambiguous,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedPublicationObservation {
    pub disposition: ConnectorStagedPublicationDisposition,
    pub committed_version: Option<ConnectorCommittedVersion>,
    pub resulting_row_count: Option<u64>,
    pub bases: Vec<ConnectorStagedPublicationBaseFact>,
    pub definition_fingerprint: Option<Arc<str>>,
    pub staging_snapshot_id: Option<i64>,
    pub target_snapshot_id: Option<i64>,
    pub cleanup_required: bool,
    pub proof: ConnectorStagedPublicationProof,
    digest: [u8; 32],
}

impl ConnectorStagedPublicationObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        disposition: ConnectorStagedPublicationDisposition,
        committed_version: Option<ConnectorCommittedVersion>,
        resulting_row_count: Option<u64>,
        bases: Vec<ConnectorStagedPublicationBaseFact>,
        definition_fingerprint: Option<Arc<str>>,
        staging_snapshot_id: Option<i64>,
        target_snapshot_id: Option<i64>,
        cleanup_required: bool,
        proof: ConnectorStagedPublicationProof,
    ) -> Result<Self, ConnectorError> {
        if bases.len() > MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS
            || staging_snapshot_id.is_some_and(|id| id <= 0)
            || target_snapshot_id.is_some_and(|id| id <= 0)
        {
            return Err(invalid(
                "staged publication observation has invalid bounded facts",
            ));
        }
        if matches!(
            disposition,
            ConnectorStagedPublicationDisposition::Published
                | ConnectorStagedPublicationDisposition::Superseded
                | ConnectorStagedPublicationDisposition::CleanupPending
        ) && (committed_version.is_none()
            || resulting_row_count.is_none()
            || definition_fingerprint
                .as_ref()
                .is_none_or(|value| value.is_empty()))
        {
            return Err(invalid(
                "published staged publication observation is missing finalize facts",
            ));
        }
        if let Some(version) = &committed_version {
            version.validate()?;
        }
        proof.validate()?;
        let digest = observation_digest(
            disposition,
            committed_version.as_ref(),
            resulting_row_count,
            &bases,
            definition_fingerprint.as_deref(),
            staging_snapshot_id,
            target_snapshot_id,
            cleanup_required,
            proof.digest(),
        );
        Ok(Self {
            disposition,
            committed_version,
            resulting_row_count,
            bases,
            definition_fingerprint,
            staging_snapshot_id,
            target_snapshot_id,
            cleanup_required,
            proof,
            digest,
        })
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.disposition,
            self.committed_version.clone(),
            self.resulting_row_count,
            self.bases.clone(),
            self.definition_fingerprint.clone(),
            self.staging_snapshot_id,
            self.target_snapshot_id,
            self.cleanup_required,
            self.proof.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "staged publication observation digest does not match its contents",
            ));
        }
        Ok(())
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorStagedPublicationCleanupRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub descriptor_digest: [u8; 32],
    pub observation: ConnectorStagedPublicationObservation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedPublicationCleanupReceipt {
    pub descriptor_digest: [u8; 32],
    pub observation_digest: [u8; 32],
}

pub trait ConnectorStagedPublicationRecovery: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;
    fn inspect(
        &self,
        descriptor: ConnectorStagedPublicationDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorStagedPublicationObservation, ConnectorError>;
    fn cleanup(
        &self,
        request: ConnectorStagedPublicationCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>;
    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>;
}

pub fn validate_staged_publication_recovery_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ConnectorInstanceIncarnation,
    recovery: &dyn ConnectorStagedPublicationRecovery,
) -> Result<(), ConnectorError> {
    let key = recovery.binding_key();
    if key.instance_id != descriptor.instance_id || key.incarnation != incarnation {
        return Err(invalid(
            "staged publication recovery capability does not match its control binding generation",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn descriptor_digest(
    key: &ConnectorExecutionBindingKey,
    table: &ConnectorTableIdentity,
    staging: &str,
    target: &str,
    version: Option<&ConnectorCommittedVersion>,
    refresh_id: i64,
    mv_id: i64,
    request_id: [u8; 16],
    marker: &str,
    cohorts: &[[u8; 32]],
    cohort_digest: [u8; 32],
    actions: &[ConnectorHistoricalPublicationAction],
    bases: &[ConnectorStagedPublicationBaseFact],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"novarocks.staged-publication-descriptor.v1\0");
    h.update(key.instance_id.as_str());
    h.update(key.incarnation.to_bytes());
    h.update(table.instance_id.as_str());
    h.update(table.namespace.as_bytes());
    h.update(table.table.as_bytes());
    h.update(staging.as_bytes());
    h.update(target.as_bytes());
    if let Some(v) = version {
        h.update(v.digest());
    }
    h.update(refresh_id.to_be_bytes());
    h.update(mv_id.to_be_bytes());
    h.update(request_id);
    h.update(marker.as_bytes());
    for cohort in cohorts {
        h.update(cohort);
    }
    h.update(cohort_digest);
    for action in actions {
        h.update([action.phase as u8, action.state as u8]);
        h.update(action.operation_id.to_bytes());
        if let Some(v) = &action.committed_version {
            h.update(v.digest());
        }
        if let Some(d) = action.evidence_digest {
            h.update(d);
        }
    }
    for base in bases {
        h.update(base.table.as_bytes());
        h.update(base.uuid.as_bytes());
        h.update(base.from_version.unwrap_or_default().to_be_bytes());
        h.update(base.to_version.to_be_bytes());
    }
    h.finalize().into()
}

fn observation_digest(
    disposition: ConnectorStagedPublicationDisposition,
    version: Option<&ConnectorCommittedVersion>,
    rows: Option<u64>,
    bases: &[ConnectorStagedPublicationBaseFact],
    fingerprint: Option<&str>,
    staging: Option<i64>,
    target: Option<i64>,
    cleanup_required: bool,
    proof_digest: [u8; 32],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"novarocks.staged-publication-observation.v1\0");
    h.update([disposition as u8]);
    if let Some(v) = version {
        h.update(v.digest());
    }
    h.update(rows.unwrap_or_default().to_be_bytes());
    for base in bases {
        h.update(base.table.as_bytes());
        h.update(base.uuid.as_bytes());
        h.update(base.from_version.unwrap_or_default().to_be_bytes());
        h.update(base.to_version.to_be_bytes());
    }
    if let Some(value) = fingerprint {
        h.update(value.as_bytes());
    }
    h.update(staging.unwrap_or_default().to_be_bytes());
    h.update(target.unwrap_or_default().to_be_bytes());
    h.update([cleanup_required as u8]);
    h.update(proof_digest);
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{ConnectorInstanceId, ConnectorInstanceIncarnation};

    #[test]
    fn proof_rejects_empty_or_oversized_payloads_and_redacts_debug() {
        assert!(ConnectorStagedPublicationProof::try_new(Bytes::new()).is_err());
        assert!(
            ConnectorStagedPublicationProof::try_new(Bytes::from(vec![
                0;
                MAX_CONNECTOR_STAGED_PUBLICATION_PROOF_BYTES
                    + 1
            ]))
            .is_err()
        );
        let proof =
            ConnectorStagedPublicationProof::try_new(Bytes::from_static(b"opaque")).unwrap();
        let debug = format!("{proof:?}");
        assert!(debug.contains("payload_len"));
        assert!(!debug.contains("opaque"));
    }

    #[test]
    fn published_observation_requires_complete_finalize_facts() {
        let proof = ConnectorStagedPublicationProof::try_new(Bytes::from_static(b"proof")).unwrap();
        assert!(
            ConnectorStagedPublicationObservation::try_new(
                ConnectorStagedPublicationDisposition::Published,
                None,
                Some(1),
                vec![],
                Some(Arc::from("fingerprint")),
                Some(1),
                Some(1),
                true,
                proof,
            )
            .is_err()
        );
    }

    #[test]
    fn descriptor_digest_detects_mutation() {
        let instance = ConnectorInstanceId::parse("catalog.ice").unwrap();
        let descriptor = ConnectorStagedPublicationDescriptor::try_new(
            ConnectorExecutionBindingKey {
                instance_id: instance.clone(),
                incarnation: ConnectorInstanceIncarnation::new(),
            },
            ConnectorTableIdentity {
                instance_id: instance,
                namespace: Arc::from("db"),
                table: Arc::from("mv"),
            },
            "staging",
            "main",
            None,
            1,
            1,
            [1; 16],
            "marker",
            vec![[2; 32]],
            [3; 32],
            vec![
                ConnectorHistoricalPublicationAction {
                    phase: ConnectorStagedPublicationPhase::StagingCreate,
                    state: ConnectorStagedPublicationPhaseState::Prepared,
                    operation_id: ConnectorMutationOperationId::new(),
                    committed_version: None,
                    evidence_digest: None,
                },
                ConnectorHistoricalPublicationAction {
                    phase: ConnectorStagedPublicationPhase::Write,
                    state: ConnectorStagedPublicationPhaseState::Prepared,
                    operation_id: ConnectorMutationOperationId::new(),
                    committed_version: None,
                    evidence_digest: None,
                },
                ConnectorHistoricalPublicationAction {
                    phase: ConnectorStagedPublicationPhase::Publication,
                    state: ConnectorStagedPublicationPhaseState::Prepared,
                    operation_id: ConnectorMutationOperationId::new(),
                    committed_version: None,
                    evidence_digest: None,
                },
                ConnectorHistoricalPublicationAction {
                    phase: ConnectorStagedPublicationPhase::StagingDrop,
                    state: ConnectorStagedPublicationPhaseState::Prepared,
                    operation_id: ConnectorMutationOperationId::new(),
                    committed_version: None,
                    evidence_digest: None,
                },
            ],
            vec![],
        )
        .unwrap();
        descriptor.validate().unwrap();
        let mut corrupted = descriptor;
        corrupted.marker_token = Arc::from("other");
        assert!(corrupted.validate().is_err());
    }
}
