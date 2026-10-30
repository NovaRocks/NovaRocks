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

//! FE-only provider-neutral connector orphan-cleanup contract.
//! Design: ADR-0035 (docs/adr/ADR-0035-connector-orphan-cleanup-reconcile-contract.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorMetadata, ConnectorProcessLocalPayload,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableHandle,
    ProviderBindingEpoch,
};

pub const CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION: u16 = 2;
pub const MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS: usize = 1024;
pub const MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_CLEANUP_OWNED_REF_SELECTION_ITEMS: usize = 1024;
pub const MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS: u32 = 1024;
pub const MAX_CONNECTOR_CLEANUP_BATCHES: u32 = 256;

pub const REMOVE_UNREFERENCED_OBJECTS_KIND: &str = "remove-unreferenced-objects";

/// The exact Catalog identity that authorizes retiring an owned ref.
///
/// This deliberately excludes `created_at_ms`: that timestamp is evidence for
/// the frontend's durable age observation, not Catalog compare-and-swap
/// identity. A later planning request may select a ref only by this complete
/// immutable identity; a name alone is never a deletion capability.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCleanupOwnedRefIdentity {
    name: Arc<str>,
    head_snapshot_id: i64,
    provenance_version: u16,
    provenance_digest: [u8; 32],
}

impl ConnectorCleanupOwnedRefIdentity {
    pub fn try_new(
        name: Arc<str>,
        head_snapshot_id: i64,
        provenance_version: u16,
        provenance_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        let identity = Self {
            name,
            head_snapshot_id,
            provenance_version,
            provenance_digest,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn head_snapshot_id(&self) -> i64 {
        self.head_snapshot_id
    }

    pub const fn provenance_version(&self) -> u16 {
        self.provenance_version
    }

    pub const fn provenance_digest(&self) -> [u8; 32] {
        self.provenance_digest
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.name.is_empty() || self.head_snapshot_id <= 0 || self.provenance_version == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup owned ref exact identity is invalid",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.name.as_bytes());
        hash.update(self.head_snapshot_id.to_be_bytes());
        hash.update(self.provenance_version.to_be_bytes());
        hash.update(self.provenance_digest);
    }

    fn encoded_len(&self) -> usize {
        self.name.len() + 8 + 2 + 32
    }
}

/// A bounded canonical set of owned refs selected after a read-only discovery
/// pass. `Some(empty)` is meaningful: it proves that discovery found owned
/// refs but none survived the frontend age observation. Providers must return
/// an empty ref phase, never promote that request into an object sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCleanupOwnedRefSelection {
    identities: Vec<ConnectorCleanupOwnedRefIdentity>,
}

impl ConnectorCleanupOwnedRefSelection {
    pub fn try_new(
        identities: Vec<ConnectorCleanupOwnedRefIdentity>,
    ) -> Result<Self, ConnectorError> {
        let selection = Self { identities };
        selection.validate()?;
        Ok(selection)
    }

    pub fn identities(&self) -> &[ConnectorCleanupOwnedRefIdentity] {
        &self.identities
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.identities.len() > MAX_CONNECTOR_CLEANUP_OWNED_REF_SELECTION_ITEMS
            || self
                .identities
                .iter()
                .any(|identity| identity.validate().is_err())
            || self
                .identities
                .windows(2)
                .any(|pair| pair[0] >= pair[1] || pair[0].name() == pair[1].name())
            || self
                .identities
                .iter()
                .try_fold(0_usize, |total, identity| {
                    total.checked_add(identity.encoded_len())
                })
                .is_none_or(|bytes| bytes > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup owned ref selection is invalid or exceeds its hard limit",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        hash.update((self.identities.len() as u32).to_be_bytes());
        for identity in &self.identities {
            identity.digest_into(hash);
        }
    }
}

/// A provider-neutral, read-only projection of one cleanup candidate.
///
/// Object identity remains provider-private in the immutable cleanup manifest;
/// an owned ref instead exposes the exact Catalog compare-and-swap input so
/// callers can distinguish a ref-retirement pass from an object-sweep pass.
/// The closed form is deliberate: callers must not infer delete authority from
/// an arbitrary location or ref name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorCleanupCandidate {
    Object {
        location: Arc<str>,
    },
    OwnedRef {
        /// Provider-confirmed table identity used only for the frontend's
        /// durable GC observation key. It is not a ref deletion capability.
        table_uuid: Uuid,
        name: Arc<str>,
        head_snapshot_id: i64,
        provenance_version: u16,
        provenance_digest: [u8; 32],
        created_at_ms: i64,
    },
}

impl ConnectorCleanupCandidate {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Object { .. } => "object",
            Self::OwnedRef { .. } => "owned_ref",
        }
    }

    /// Prospective size of the exact original public spelling. This is not a
    /// plan/SDK graph invoice or permission; the local result author checks its
    /// real output contract before the report String is constructed.
    pub fn display_key_len(&self) -> Result<usize, ConnectorError> {
        match self {
            Self::Object { location } => Ok(location.len()),
            Self::OwnedRef { name, .. } => name.len().checked_add("ref:".len()).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "cleanup display length overflow",
                )
            }),
        }
    }
    pub fn display_key_string(&self) -> String {
        match self {
            Self::Object { location } => location.to_string(),
            Self::OwnedRef { name, .. } => format!("ref:{name}"),
        }
    }

    pub fn display_key(&self) -> Arc<str> {
        match self {
            Self::Object { location } => Arc::clone(location),
            Self::OwnedRef { .. } => Arc::from(self.display_key_string()),
        }
    }

    pub fn owned_ref_identity(&self) -> Option<ConnectorCleanupOwnedRefIdentity> {
        match self {
            Self::OwnedRef {
                name,
                head_snapshot_id,
                provenance_version,
                provenance_digest,
                ..
            } => ConnectorCleanupOwnedRefIdentity::try_new(
                Arc::clone(name),
                *head_snapshot_id,
                *provenance_version,
                *provenance_digest,
            )
            .ok(),
            Self::Object { .. } => None,
        }
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::Object { location } if !location.is_empty() => Ok(()),
            Self::OwnedRef {
                table_uuid,
                name,
                head_snapshot_id,
                provenance_version,
                provenance_digest: _,
                created_at_ms,
            } if !table_uuid.is_nil()
                && !name.is_empty()
                && *head_snapshot_id > 0
                && *provenance_version > 0
                && *created_at_ms > 0 =>
            {
                Ok(())
            }
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup candidate has an invalid exact identity",
            )),
        }
    }
}

const REQUEST_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.request.v1\0";
const PLAN_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.plan.v1\0";
const PREPARED_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.prepared.v1\0";
const RECEIPT_DOMAIN: &[u8] = b"novarocks.connector-cleanup-maintenance.receipt.v1\0";
const PREPARED_WIRE_MAGIC: &[u8; 8] = b"NRCLEAN1";

/// Durable identity selected by the frontend operation owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCleanupOperationId(Uuid);

impl ConnectorCleanupOperationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorCleanupOperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorCleanupOperation {
    RemoveUnreferencedObjects {
        table: ConnectorTableHandle,
        older_than_ms: i64,
    },
}

impl ConnectorCleanupOperation {
    pub fn remove_unreferenced_objects(
        table: ConnectorTableHandle,
        older_than_ms: i64,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::RemoveUnreferencedObjects {
            table,
            older_than_ms,
        };
        operation.validate()?;
        Ok(operation)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RemoveUnreferencedObjects { .. } => REMOVE_UNREFERENCED_OBJECTS_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RemoveUnreferencedObjects { table, .. } => table,
        }
    }

    pub const fn older_than_ms(&self) -> i64 {
        match self {
            Self::RemoveUnreferencedObjects { older_than_ms, .. } => *older_than_ms,
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.older_than_ms() <= 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "orphan cleanup older-than timestamp must be positive",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.kind().as_bytes());
        digest_bytes(hash, self.table().owner().as_str().as_bytes());
        digest_bytes(hash, self.table().payload());
        hash.update(self.older_than_ms().to_be_bytes());
        // Preserve the existing non-document cleanup digest framing.
        hash.update([0]);
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupPlanningRequest {
    operation_id: ConnectorCleanupOperationId,
    owner: ConnectorProviderBindingKey,
    operation: ConnectorCleanupOperation,
    owned_ref_selection: Option<ConnectorCleanupOwnedRefSelection>,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorCleanupPlanningRequest {
    pub fn try_new(
        operation_id: ConnectorCleanupOperationId,
        owner: ConnectorProviderBindingKey,
        operation: ConnectorCleanupOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup table handle does not match exact owner",
            ));
        }
        let request_digest = request_digest(operation_id, &owner, &operation, None);
        Ok(Self {
            operation_id,
            owner,
            operation,
            owned_ref_selection: None,
            request_digest,
            context,
        })
    }

    /// Build the second, selected ref-retirement planning request. Unlike the
    /// discovery request, this mode can never authorize an object sweep.
    pub fn try_new_selected_owned_refs(
        operation_id: ConnectorCleanupOperationId,
        owner: ConnectorProviderBindingKey,
        operation: ConnectorCleanupOperation,
        owned_ref_selection: ConnectorCleanupOwnedRefSelection,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        owned_ref_selection.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup table handle does not match exact owner",
            ));
        }
        let request_digest =
            request_digest(operation_id, &owner, &operation, Some(&owned_ref_selection));
        Ok(Self {
            operation_id,
            owner,
            operation,
            owned_ref_selection: Some(owned_ref_selection),
            request_digest,
            context,
        })
    }

    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub fn operation(&self) -> &ConnectorCleanupOperation {
        &self.operation
    }

    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// `None` denotes discovery. `Some`, including an empty selection,
    /// denotes the second ref-retirement plan and is never an object sweep.
    pub fn owned_ref_selection(&self) -> Option<&ConnectorCleanupOwnedRefSelection> {
        self.owned_ref_selection.as_ref()
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.operation.validate()?;
        if self.operation.table().owner() != &self.owner.instance_id
            || self
                .owned_ref_selection
                .as_ref()
                .is_some_and(|selection| selection.validate().is_err())
            || request_digest(
                self.operation_id,
                &self.owner,
                &self.operation,
                self.owned_ref_selection.as_ref(),
            ) != self.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup request owner or digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorCleanupPlanningRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCleanupPlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field(
                "owned_ref_selection_count",
                &self
                    .owned_ref_selection
                    .as_ref()
                    .map(|selection| selection.identities().len()),
            )
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorCleanupPlanSummary {
    candidate_count: u64,
    total_bytes: u64,
    batch_count: u32,
}

impl ConnectorCleanupPlanSummary {
    pub fn try_new(
        candidate_count: u64,
        total_bytes: u64,
        batch_count: u32,
    ) -> Result<Self, ConnectorError> {
        if batch_count > MAX_CONNECTOR_CLEANUP_BATCHES
            || (candidate_count == 0 && batch_count != 0)
            || (candidate_count > 0 && batch_count == 0)
            || candidate_count
                > u64::from(MAX_CONNECTOR_CLEANUP_BATCHES)
                    * u64::from(MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup plan exceeds its bounded manifest or batch budget",
            ));
        }
        Ok(Self {
            candidate_count,
            total_bytes,
            batch_count,
        })
    }

    pub const fn candidate_count(self) -> u64 {
        self.candidate_count
    }
    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }
    pub const fn batch_count(self) -> u32 {
        self.batch_count
    }

    fn digest_into(self, hash: &mut Sha256) {
        hash.update(self.candidate_count.to_be_bytes());
        hash.update(self.total_bytes.to_be_bytes());
        hash.update(self.batch_count.to_be_bytes());
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupPlan {
    schema_version: u16,
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorCleanupOperationId,
    request_digest: [u8; 32],
    base_state_digest: [u8; 32],
    manifest_digest: [u8; 32],
    summary: ConnectorCleanupPlanSummary,
    provider_payload: Bytes,
    plan_digest: [u8; 32],
    manifest: ConnectorProcessLocalPayload,
}

impl ConnectorCleanupPlan {
    pub fn try_new(
        request: &ConnectorCleanupPlanningRequest,
        base_state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorCleanupPlanSummary,
        provider_payload: Bytes,
        manifest: ConnectorProcessLocalPayload,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload(&provider_payload, "plan")?;
        let plan_digest = plan_digest(
            request.request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            schema_version: CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            request_digest: request.request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            provider_payload,
            plan_digest,
            manifest,
        })
    }

    pub fn try_restore(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorCleanupOperationId,
        request_digest: [u8; 32],
        base_state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorCleanupPlanSummary,
        provider_payload: Bytes,
        plan_digest: [u8; 32],
        manifest: ConnectorProcessLocalPayload,
    ) -> Result<Self, ConnectorError> {
        let plan = Self {
            schema_version: CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
            owner,
            operation_id,
            request_digest,
            base_state_digest,
            manifest_digest,
            summary,
            provider_payload,
            plan_digest,
            manifest,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn manifest(&self) -> &ConnectorProcessLocalPayload {
        &self.manifest
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    pub const fn base_state_digest(&self) -> [u8; 32] {
        self.base_state_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn summary(&self) -> ConnectorCleanupPlanSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        ConnectorCleanupPlanSummary::try_new(
            self.summary.candidate_count,
            self.summary.total_bytes,
            self.summary.batch_count,
        )?;
        validate_payload(&self.provider_payload, "plan")?;
        if self.schema_version != CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION
            || plan_digest(
                self.request_digest,
                self.base_state_digest,
                self.manifest_digest,
                self.summary,
                &self.provider_payload,
            ) != self.plan_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup plan digest is invalid",
            ));
        }
        Ok(())
    }
}

impl PartialEq for ConnectorCleanupPlan {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.owner == other.owner
            && self.operation_id == other.operation_id
            && self.request_digest == other.request_digest
            && self.base_state_digest == other.base_state_digest
            && self.manifest_digest == other.manifest_digest
            && self.summary == other.summary
            && self.provider_payload == other.provider_payload
            && self.plan_digest == other.plan_digest
    }
}

impl Eq for ConnectorCleanupPlan {}

impl fmt::Debug for ConnectorCleanupPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorCleanupPlan")
            .field("schema_version", &self.schema_version)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("request_digest", &self.request_digest)
            .field("base_state_digest", &self.base_state_digest)
            .field("manifest_digest", &self.manifest_digest)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PreparedBatch {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorCleanupOperationId,
    plan_digest: [u8; 32],
    manifest_digest: [u8; 32],
    batch_ordinal: u32,
    batch_digest: [u8; 32],
    evidence_payload: Bytes,
    evidence_digest: [u8; 32],
}

impl PreparedBatch {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorCleanupOperationId,
        plan_digest: [u8; 32],
        manifest_digest: [u8; 32],
        batch_ordinal: u32,
        batch_digest: [u8; 32],
        evidence_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup batch ordinal exceeds the hard limit",
            ));
        }
        validate_payload(&evidence_payload, "prepared-batch evidence")?;
        let evidence_digest = prepared_digest(
            &owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            &evidence_payload,
        );
        Ok(Self {
            owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            evidence_payload,
            evidence_digest,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn batch_ordinal(&self) -> u32 {
        self.batch_ordinal
    }
    pub const fn batch_digest(&self) -> [u8; 32] {
        self.batch_digest
    }
    pub fn evidence_payload(&self) -> &Bytes {
        &self.evidence_payload
    }
    pub const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup batch ordinal exceeds the hard limit",
            ));
        }
        validate_payload(&self.evidence_payload, "prepared-batch evidence")?;
        if prepared_digest(
            &self.owner,
            self.operation_id,
            self.plan_digest,
            self.manifest_digest,
            self.batch_ordinal,
            self.batch_digest,
            &self.evidence_payload,
        ) != self.evidence_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared-batch evidence digest is invalid",
            ));
        }
        Ok(())
    }

    /// Bounded opaque durable carrier for the frontend operation owner. It
    /// contains no candidate locations or object identities; only the exact
    /// binding, frozen digests, and provider-produced prepare evidence needed
    /// to reconcile the same batch after a restart.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        let instance = self.owner.instance_id.as_str().as_bytes();
        let instance_len = u16::try_from(instance.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup owner exceeds durable wire limit",
            )
        })?;
        let evidence_len = u32::try_from(self.evidence_payload.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup evidence exceeds durable wire limit",
            )
        })?;
        let mut output = Vec::with_capacity(
            8 + 2 + instance.len() + 16 + 16 + 32 * 4 + 4 + self.evidence_payload.len(),
        );
        output.extend_from_slice(PREPARED_WIRE_MAGIC);
        output.extend_from_slice(&CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
        output.extend_from_slice(&instance_len.to_be_bytes());
        output.extend_from_slice(instance);
        output.extend_from_slice(&self.owner.incarnation.to_bytes());
        output.extend_from_slice(&self.operation_id.to_bytes());
        output.extend_from_slice(&self.plan_digest);
        output.extend_from_slice(&self.manifest_digest);
        output.extend_from_slice(&self.batch_ordinal.to_be_bytes());
        output.extend_from_slice(&self.batch_digest);
        output.extend_from_slice(&self.evidence_digest);
        output.extend_from_slice(&evidence_len.to_be_bytes());
        output.extend_from_slice(&self.evidence_payload);
        if output.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup prepared wire exceeds the hard limit",
            ));
        }
        Ok(Bytes::from(output))
    }

    pub fn try_from_wire_v1(value: Bytes) -> Result<Self, ConnectorError> {
        if value.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES
            || value.len() < 8 + 2 + 2 + 16 + 16 + 32 * 4 + 4 + 4
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire length is invalid",
            ));
        }
        let mut cursor = 0usize;
        let take = |count: usize, cursor: &mut usize| -> Result<&[u8], ConnectorError> {
            let end = cursor.checked_add(count).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "cleanup prepared wire overflows",
                )
            })?;
            let part = value.get(*cursor..end).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "cleanup prepared wire is truncated",
                )
            })?;
            *cursor = end;
            Ok(part)
        };
        if take(8, &mut cursor)? != PREPARED_WIRE_MAGIC
            || take(2, &mut cursor)? != CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire version is invalid",
            ));
        }
        let instance_len = u16::from_be_bytes(take(2, &mut cursor)?.try_into().unwrap()) as usize;
        let instance = std::str::from_utf8(take(instance_len, &mut cursor)?).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared owner is not UTF-8",
            )
        })?;
        let incarnation =
            ProviderBindingEpoch::from_bytes(take(16, &mut cursor)?.try_into().unwrap());
        let operation_id =
            ConnectorCleanupOperationId::from_bytes(take(16, &mut cursor)?.try_into().unwrap());
        let plan_digest = take(32, &mut cursor)?.try_into().unwrap();
        let manifest_digest = take(32, &mut cursor)?.try_into().unwrap();
        let batch_ordinal = u32::from_be_bytes(take(4, &mut cursor)?.try_into().unwrap());
        let batch_digest = take(32, &mut cursor)?.try_into().unwrap();
        let evidence_digest: [u8; 32] = take(32, &mut cursor)?.try_into().unwrap();
        let evidence_len = u32::from_be_bytes(take(4, &mut cursor)?.try_into().unwrap()) as usize;
        let evidence_payload = Bytes::copy_from_slice(take(evidence_len, &mut cursor)?);
        if cursor != value.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire has trailing bytes",
            ));
        }
        let prepared = Self::try_new(
            ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse(instance)?,
                incarnation,
            },
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            evidence_payload,
        )?;
        if prepared.evidence_digest != evidence_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup prepared wire evidence digest is invalid",
            ));
        }
        Ok(prepared)
    }
}

impl fmt::Debug for PreparedBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedBatch")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("batch_ordinal", &self.batch_ordinal)
            .field("batch_digest", &self.batch_digest)
            .field("evidence_payload_len", &self.evidence_payload.len())
            .field("evidence_digest", &self.evidence_digest)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchReceiptSummary {
    deleted: u32,
    already_absent: u32,
    failed: u32,
    unknown: u32,
}

impl BatchReceiptSummary {
    pub const fn new(deleted: u32, already_absent: u32, failed: u32, unknown: u32) -> Self {
        Self {
            deleted,
            already_absent,
            failed,
            unknown,
        }
    }
    pub const fn deleted(self) -> u32 {
        self.deleted
    }
    pub const fn already_absent(self) -> u32 {
        self.already_absent
    }
    pub const fn failed(self) -> u32 {
        self.failed
    }
    pub const fn unknown(self) -> u32 {
        self.unknown
    }
    pub const fn total(self) -> u32 {
        self.deleted + self.already_absent + self.failed + self.unknown
    }

    fn digest_into(self, hash: &mut Sha256) {
        hash.update(self.deleted.to_be_bytes());
        hash.update(self.already_absent.to_be_bytes());
        hash.update(self.failed.to_be_bytes());
        hash.update(self.unknown.to_be_bytes());
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct BatchReceipt {
    descriptor: ConnectorInstanceDescriptor,
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorCleanupOperationId,
    plan_digest: [u8; 32],
    manifest_digest: [u8; 32],
    batch_ordinal: u32,
    batch_digest: [u8; 32],
    summary: BatchReceiptSummary,
    provider_payload: Bytes,
    receipt_digest: [u8; 32],
}

impl BatchReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorCleanupOperationId,
        plan_digest: [u8; 32],
        manifest_digest: [u8; 32],
        batch_ordinal: u32,
        batch_digest: [u8; 32],
        summary: BatchReceiptSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if descriptor.instance_id != owner.instance_id
            || batch_ordinal >= MAX_CONNECTOR_CLEANUP_BATCHES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup receipt owner or ordinal is invalid",
            ));
        }
        validate_payload(&provider_payload, "receipt")?;
        let receipt_digest = receipt_digest(
            &descriptor,
            &owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            summary,
            &provider_payload,
        );
        Ok(Self {
            descriptor,
            owner,
            operation_id,
            plan_digest,
            manifest_digest,
            batch_ordinal,
            batch_digest,
            summary,
            provider_payload,
            receipt_digest,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn batch_ordinal(&self) -> u32 {
        self.batch_ordinal
    }
    pub const fn batch_digest(&self) -> [u8; 32] {
        self.batch_digest
    }
    pub const fn summary(&self) -> BatchReceiptSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.descriptor.clone(),
            self.owner.clone(),
            self.operation_id,
            self.plan_digest,
            self.manifest_digest,
            self.batch_ordinal,
            self.batch_digest,
            self.summary,
            self.provider_payload.clone(),
        )?;
        if expected.receipt_digest != self.receipt_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "cleanup receipt digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for BatchReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchReceipt")
            .field("descriptor", &self.descriptor)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("batch_ordinal", &self.batch_ordinal)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("receipt_digest", &self.receipt_digest)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidatePage {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorCleanupOperationId,
    manifest_digest: [u8; 32],
    offset: u64,
    candidates: Vec<ConnectorCleanupCandidate>,
    complete: bool,
}

impl CandidatePage {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorCleanupOperationId,
        manifest_digest: [u8; 32],
        offset: u64,
        candidates: Vec<ConnectorCleanupCandidate>,
        complete: bool,
    ) -> Result<Self, ConnectorError> {
        let page = Self {
            owner,
            operation_id,
            manifest_digest,
            offset,
            candidates,
            complete,
        };
        page.validate()?;
        Ok(page)
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorCleanupOperationId {
        self.operation_id
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn offset(&self) -> u64 {
        self.offset
    }
    pub fn candidates(&self) -> &[ConnectorCleanupCandidate] {
        &self.candidates
    }
    /// Backward-compatible read-only display projection. It never carries
    /// delete authority and is intentionally unsuitable for dispatch.
    pub fn display_keys(&self) -> Vec<Arc<str>> {
        self.candidates
            .iter()
            .map(ConnectorCleanupCandidate::display_key)
            .collect()
    }
    pub const fn complete(&self) -> bool {
        self.complete
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let display_keys = self.display_keys();
        if self.candidates.len() > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS
            || self
                .candidates
                .iter()
                .any(|candidate| candidate.validate().is_err())
            || display_keys.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .candidates
                .iter()
                .map(|candidate| match candidate {
                    ConnectorCleanupCandidate::Object { location } => location.len(),
                    ConnectorCleanupCandidate::OwnedRef { name, .. } => {
                        16 + name.len() + 8 + 2 + 32 + 8
                    }
                })
                .sum::<usize>()
                > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "cleanup candidate page is invalid or exceeds its hard limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupPrepareRequest {
    pub plan: ConnectorCleanupPlan,
    pub batch_ordinal: u32,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupPrepareRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        batch_ordinal: u32,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if batch_ordinal >= plan.summary.batch_count {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup batch ordinal is outside the frozen plan",
            ));
        }
        Ok(Self {
            plan,
            batch_ordinal,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupExecuteRequest {
    pub plan: ConnectorCleanupPlan,
    pub prepared: PreparedBatch,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupExecuteRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        prepared: PreparedBatch,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        prepared.validate()?;
        validate_prepared_for_plan(&plan, &prepared)?;
        Ok(Self {
            plan,
            prepared,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupCandidatePageRequest {
    pub plan: ConnectorCleanupPlan,
    pub offset: u64,
    pub limit: u32,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupCandidatePageRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        offset: u64,
        limit: u32,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if limit == 0 || limit as usize > MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup candidate page limit is invalid",
            ));
        }
        Ok(Self {
            plan,
            offset,
            limit,
            context,
        })
    }
}

#[derive(Clone)]
pub struct ConnectorCleanupFinalizeRequest {
    pub plan: ConnectorCleanupPlan,
    pub context: ConnectorRequestContext,
}
impl ConnectorCleanupFinalizeRequest {
    pub fn try_new(
        plan: ConnectorCleanupPlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        Ok(Self { plan, context })
    }
}

/// Provider evidence for scanned-object reclamation, distinct from exact
/// operation-owned cleanup and exact Catalog reference retirement.
/// No current provider proves writer fencing, Unknown/unreceived protection
/// and restart recovery together. Age/current reachability is not that proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorCleanupScanSafety {
    Unproven,
}
impl ConnectorCleanupScanSafety {
    pub fn require_scanned_object_gc(self) -> Result<(), ConnectorError> {
        match self {
            Self::Unproven => Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "scanned orphan cleanup is unsupported: Provider writer fencing, Unknown/unreceived output protection and restart safety are unproven",
            )),
        }
    }
}

/// A cleanup provider never receives an opportunity to re-list candidates after
/// planning. `execute_batch` is invoked once per prepared batch.
pub trait ConnectorCleanupMaintenance: Send + Sync {
    /// Exact concrete author of this selected provider generation; no CPU grant.
    fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup has no governed metadata CPU author",
        ))
    }
    /// Silent construction only. The original application installs this source
    /// before polling; dropping a borrowed wait never drops its physical F.
    fn begin_async_in(
        &self,
        _start: &mut Option<ConnectorCleanupAsyncStart>,
        _metadata: Arc<dyn ConnectorMetadata>,
        _cpu: Arc<dyn super::ProviderMetadataCpuPort>,
        _target: &mut Option<ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "provider has no governed cleanup async source",
        ))
    }
    /// Initial routes selected by THIS original cleanup factory. This is
    /// neither a byte allowance nor an I/O/concurrency grant.
    fn initial_dependencies(&self) -> Result<Vec<super::SharedDependency>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup factory has no original selected route facts",
        ))
    }

    /// Must be checked before object discovery and before executing a frozen
    /// object manifest. It does not gate independently safe owned operations.
    fn scan_gc_safety(&self) -> ConnectorCleanupScanSafety {
        ConnectorCleanupScanSafety::Unproven
    }
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;
    fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError>;
    fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError>;
    fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError>;
    fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError>;
    fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError>;
}

pub trait ConnectorCleanupMaintenanceResolver: Send + Sync {
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError>;
    fn acquire_exact_cleanup_maintenance(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCleanupMaintenanceLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorCleanupMaintenanceLease {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    cleanup: Arc<dyn ConnectorCleanupMaintenance>,
    _release: Arc<CleanupRelease>,
}
struct CleanupRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCleanupMaintenanceLease {
    pub fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        let selected = self.cleanup.metadata_cpu_source_type_id()?;
        let (key, actual) = self
            .metadata
            .provider_metadata_cpu_author()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "selected cleanup metadata CPU author is unsupported",
                )
            })?;
        if key != self.provider_binding_key || actual != selected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup metadata CPU author belongs to another exact generation",
            ));
        }
        Ok(selected)
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.provider_binding_key.incarnation
    }
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        cleanup: Arc<dyn ConnectorCleanupMaintenance>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if metadata.instance_id() != &descriptor.instance_id
            || cleanup.descriptor() != &descriptor
            || cleanup.binding_key() != &provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_binding_key,
            metadata,
            cleanup,
            _release: Arc::new(CleanupRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }
    pub fn initial_dependencies(&self) -> Result<Vec<super::SharedDependency>, ConnectorError> {
        if self.cleanup.descriptor() != &self.descriptor
            || self.cleanup.binding_key() != &self.provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup route author belongs to another exact generation",
            ));
        }
        self.cleanup.initial_dependencies()
    }
    pub fn begin_async_in(
        &self,
        start: &mut Option<ConnectorCleanupAsyncStart>,
        cpu: Arc<dyn super::ProviderMetadataCpuPort>,
        target: &mut Option<ConnectorCleanupAsyncExecution>,
    ) -> Result<(), ConnectorError> {
        let input = start.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup original start is absent",
            )
        })?;
        if target.is_some() || input.table.instance_id != self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup original receiving or exact owner differs",
            ));
        }
        let operation = input.operation_id;
        self.cleanup
            .begin_async_in(start, self.metadata.clone(), cpu, target)?;
        let execution = target.as_mut().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "cleanup provider did not install its original source",
            )
        })?;
        execution.expected = Some((self.provider_binding_key.clone(), operation));
        Ok(())
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
    pub fn scan_gc_safety(&self) -> ConnectorCleanupScanSafety {
        self.cleanup.scan_gc_safety()
    }
    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }
    /// Builds a provider planning request behind the FE-owned control-runtime
    /// lease. The provider's legacy binding fence stays private to this
    /// capability boundary.
    pub fn plan_operation(
        &self,
        operation_id: ConnectorCleanupOperationId,
        operation: ConnectorCleanupOperation,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        let request = ConnectorCleanupPlanningRequest::try_new(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            context,
        )?;
        self.plan_cleanup(request)
    }
    /// Builds the selected-owned-refs plan behind the same exact lease.
    pub fn plan_selected_owned_refs(
        &self,
        operation_id: ConnectorCleanupOperationId,
        operation: ConnectorCleanupOperation,
        selection: ConnectorCleanupOwnedRefSelection,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        let request = ConnectorCleanupPlanningRequest::try_new_selected_owned_refs(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            selection,
            context,
        )?;
        self.plan_cleanup(request)
    }
    pub fn plan_cleanup(
        &self,
        request: ConnectorCleanupPlanningRequest,
    ) -> Result<ConnectorCleanupPlan, ConnectorError> {
        request.validate()?;
        if request.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup request does not match lease",
            ));
        }
        let plan = self.cleanup.plan_cleanup(request.clone())?;
        plan.validate()?;
        if plan.owner != self.provider_binding_key
            || plan.operation_id != request.operation_id
            || plan.request_digest != request.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup plan does not match request",
            ));
        }
        Ok(plan)
    }
    pub fn prepare_batch(
        &self,
        request: ConnectorCleanupPrepareRequest,
    ) -> Result<PreparedBatch, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let prepared = self.cleanup.prepare_batch(request.clone())?;
        prepared.validate()?;
        validate_prepared_for_plan(&request.plan, &prepared)?;
        if prepared.batch_ordinal != request.batch_ordinal {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup prepared batch does not match request ordinal",
            ));
        }
        Ok(prepared)
    }
    pub fn execute_batch(
        &self,
        request: ConnectorCleanupExecuteRequest,
    ) -> Result<BatchReceipt, ConnectorError> {
        self.validate_plan(&request.plan)?;
        validate_prepared_for_plan(&request.plan, &request.prepared)?;
        let receipt = self.cleanup.execute_batch(request.clone())?;
        self.validate_receipt(&request.plan, &request.prepared, &receipt)?;
        Ok(receipt)
    }
    pub fn read_candidate_page(
        &self,
        request: ConnectorCleanupCandidatePageRequest,
    ) -> Result<CandidatePage, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let page = self.cleanup.read_candidate_page(request.clone())?;
        page.validate()?;
        if page.owner != self.provider_binding_key
            || page.operation_id != request.plan.operation_id
            || page.manifest_digest != request.plan.manifest_digest
            || page.offset != request.offset
            || page.candidates.len() > request.limit as usize
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup candidate page does not match frozen plan",
            ));
        }
        Ok(page)
    }
    pub fn finalize_terminal(
        &self,
        request: ConnectorCleanupFinalizeRequest,
    ) -> Result<(), ConnectorError> {
        self.validate_plan(&request.plan)?;
        self.cleanup.finalize_terminal(request)
    }
    fn validate_plan(&self, plan: &ConnectorCleanupPlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup plan does not match lease",
            ));
        }
        Ok(())
    }
    fn validate_receipt(
        &self,
        plan: &ConnectorCleanupPlan,
        prepared: &PreparedBatch,
        receipt: &BatchReceipt,
    ) -> Result<(), ConnectorError> {
        receipt.validate()?;
        if receipt.descriptor != self.descriptor
            || receipt.owner != self.provider_binding_key
            || receipt.operation_id != plan.operation_id
            || receipt.plan_digest != plan.plan_digest
            || receipt.manifest_digest != plan.manifest_digest
            || receipt.batch_ordinal != prepared.batch_ordinal
            || receipt.batch_digest != prepared.batch_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cleanup receipt does not match prepared batch",
            ));
        }
        Ok(())
    }
}

impl Drop for CleanupRelease {
    fn drop(&mut self) {
        if let Ok(mut release) = self.release.lock()
            && let Some(release) = release.take()
        {
            release();
        }
    }
}

pub(crate) fn validate_cleanup_maintenance_owner(
    descriptor: &ConnectorInstanceDescriptor,
    key: &ConnectorProviderBindingKey,
    cleanup: &dyn ConnectorCleanupMaintenance,
) -> Result<(), ConnectorError> {
    if cleanup.descriptor() != descriptor || cleanup.binding_key() != key {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "cleanup capability owner does not match control binding",
        ));
    }
    Ok(())
}

fn validate_prepared_for_plan(
    plan: &ConnectorCleanupPlan,
    prepared: &PreparedBatch,
) -> Result<(), ConnectorError> {
    prepared.validate()?;
    if prepared.owner != plan.owner
        || prepared.operation_id != plan.operation_id
        || prepared.plan_digest != plan.plan_digest
        || prepared.manifest_digest != plan.manifest_digest
        || prepared.batch_ordinal >= plan.summary.batch_count
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "cleanup prepared batch does not match frozen plan",
        ));
    }
    Ok(())
}

fn request_digest(
    id: ConnectorCleanupOperationId,
    owner: &ConnectorProviderBindingKey,
    operation: &ConnectorCleanupOperation,
    owned_ref_selection: Option<&ConnectorCleanupOwnedRefSelection>,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(id.to_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hash);
    match owned_ref_selection {
        None => hash.update([0]),
        Some(selection) => {
            hash.update([1]);
            selection.digest_into(&mut hash);
        }
    }
    hash.finalize().into()
}
fn plan_digest(
    request: [u8; 32],
    base: [u8; 32],
    manifest: [u8; 32],
    summary: ConnectorCleanupPlanSummary,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLAN_DOMAIN);
    hash.update(CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION.to_be_bytes());
    hash.update(request);
    hash.update(base);
    hash.update(manifest);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn prepared_digest(
    owner: &ConnectorProviderBindingKey,
    id: ConnectorCleanupOperationId,
    plan: [u8; 32],
    manifest: [u8; 32],
    ordinal: u32,
    batch: [u8; 32],
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PREPARED_DOMAIN);
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    hash.update(id.to_bytes());
    hash.update(plan);
    hash.update(manifest);
    hash.update(ordinal.to_be_bytes());
    hash.update(batch);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn receipt_digest(
    descriptor: &ConnectorInstanceDescriptor,
    owner: &ConnectorProviderBindingKey,
    id: ConnectorCleanupOperationId,
    plan: [u8; 32],
    manifest: [u8; 32],
    ordinal: u32,
    batch: [u8; 32],
    summary: BatchReceiptSummary,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(RECEIPT_DOMAIN);
    digest_bytes(&mut hash, descriptor.provider_id.as_str().as_bytes());
    digest_bytes(&mut hash, descriptor.instance_id.as_str().as_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    hash.update(id.to_bytes());
    hash.update(plan);
    hash.update(manifest);
    hash.update(ordinal.to_be_bytes());
    hash.update(batch);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.finalize().into()
}
fn validate_payload(payload: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("cleanup {kind} payload exceeds the hard limit"),
        ));
    }
    Ok(())
}
fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::*;
    use crate::connector::{ConnectorInstanceId, ConnectorProviderId, ProviderBindingEpoch};

    fn owner() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.main").unwrap(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            crate::connector::ConnectorStopOwner::new().view(),
            1024,
            2048,
        )
        .expect("test connector context")
    }

    fn table_handle() -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(owner().instance_id, Bytes::from_static(b"table"))
            .expect("test table handle")
    }

    #[test]
    fn cleanup_plan_payload_shares_lifetime_and_does_not_change_identity() {
        let request = ConnectorCleanupPlanningRequest::try_new(
            ConnectorCleanupOperationId::new(),
            owner(),
            ConnectorCleanupOperation::remove_unreferenced_objects(table_handle(), 1).unwrap(),
            context(),
        )
        .unwrap();
        let content = Arc::new(String::from("private frozen candidates"));
        let weak = Arc::downgrade(&content);
        let plan = ConnectorCleanupPlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorCleanupPlanSummary::try_new(1, 10, 1).unwrap(),
            Bytes::from_static(b"plan"),
            ConnectorProcessLocalPayload::new(owner(), content),
        )
        .unwrap();
        let clone = plan.clone();
        assert!(Arc::ptr_eq(
            plan.manifest()
                .value::<String>(owner().instance_id())
                .unwrap(),
            clone
                .manifest()
                .value::<String>(owner().instance_id())
                .unwrap()
        ));
        let other_content = ConnectorCleanupPlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            plan.summary(),
            plan.provider_payload().clone(),
            ConnectorProcessLocalPayload::new(owner(), Arc::new(123u64)),
        )
        .unwrap();
        assert_eq!(plan, other_content);
        assert_eq!(plan.plan_digest(), other_content.plan_digest());
        assert!(!format!("{plan:?}").contains("private frozen candidates"));
        plan.validate().unwrap();
        drop(plan);
        assert!(weak.upgrade().is_some());
        drop(clone);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn rejects_corrupt_prepared_evidence() {
        let prepared = PreparedBatch::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [1; 32],
            [2; 32],
            0,
            [3; 32],
            Bytes::from_static(b"evidence"),
        )
        .unwrap();
        let mut corrupt = prepared.clone();
        corrupt.evidence_payload = Bytes::from_static(b"other");
        assert_eq!(
            corrupt.validate().unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn candidate_page_is_canonical_and_bounded() {
        let page = CandidatePage::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [0; 32],
            0,
            vec![
                ConnectorCleanupCandidate::Object {
                    location: Arc::from("s3://bucket/a"),
                },
                ConnectorCleanupCandidate::Object {
                    location: Arc::from("s3://bucket/b"),
                },
            ],
            true,
        );
        assert!(page.is_ok());
        let noncanonical = CandidatePage::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [0; 32],
            0,
            vec![
                ConnectorCleanupCandidate::Object {
                    location: Arc::from("b"),
                },
                ConnectorCleanupCandidate::Object {
                    location: Arc::from("a"),
                },
            ],
            true,
        );
        assert!(noncanonical.is_err());
    }

    #[test]
    fn owned_ref_candidate_keeps_exact_retirement_identity() {
        let page = CandidatePage::try_new(
            owner(),
            ConnectorCleanupOperationId::new(),
            [9; 32],
            0,
            vec![ConnectorCleanupCandidate::OwnedRef {
                table_uuid: Uuid::from_u128(7),
                name: Arc::from("__novarocks_mv_refresh_7"),
                head_snapshot_id: 41,
                provenance_version: 1,
                provenance_digest: [7; 32],
                created_at_ms: 1_700_000_000_000,
            }],
            true,
        )
        .expect("owned ref candidate");
        assert_eq!(page.candidates()[0].kind(), "owned_ref");
        assert_eq!(
            page.display_keys(),
            vec![Arc::from("ref:__novarocks_mv_refresh_7")]
        );
        let identity = page.candidates()[0]
            .owned_ref_identity()
            .expect("owned ref has an exact selection identity");
        assert_eq!(identity.name(), "__novarocks_mv_refresh_7");
        assert_eq!(identity.head_snapshot_id(), 41);
        assert_eq!(identity.provenance_version(), 1);
        assert_eq!(identity.provenance_digest(), [7; 32]);
    }

    #[test]
    fn selected_owned_refs_are_bounded_canonical_and_digest_bound() {
        let first = ConnectorCleanupOwnedRefIdentity::try_new(Arc::from("a"), 1, 1, [1; 32])
            .expect("first identity");
        let second = ConnectorCleanupOwnedRefIdentity::try_new(Arc::from("b"), 2, 1, [2; 32])
            .expect("second identity");
        let selection = ConnectorCleanupOwnedRefSelection::try_new(vec![first.clone(), second])
            .expect("canonical selection");
        let operation = ConnectorCleanupOperation::remove_unreferenced_objects(table_handle(), 1)
            .expect("cleanup operation");
        let selected = ConnectorCleanupPlanningRequest::try_new_selected_owned_refs(
            ConnectorCleanupOperationId::new(),
            owner(),
            operation.clone(),
            selection,
            context(),
        )
        .expect("selected request");
        let discovery = ConnectorCleanupPlanningRequest::try_new(
            selected.operation_id(),
            owner(),
            operation,
            context(),
        )
        .expect("discovery request");
        assert_ne!(selected.request_digest(), discovery.request_digest());
        assert!(ConnectorCleanupOwnedRefSelection::try_new(vec![first.clone(), first]).is_err());
        assert!(
            ConnectorCleanupOwnedRefSelection::try_new(vec![
                ConnectorCleanupOwnedRefIdentity::try_new(Arc::from("b"), 2, 1, [2; 32]).unwrap(),
                ConnectorCleanupOwnedRefIdentity::try_new(Arc::from("a"), 1, 1, [1; 32]).unwrap(),
            ])
            .is_err()
        );
    }

    #[test]
    fn receipt_keeps_unknown_explicit() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: owner().instance_id.clone(),
        };
        let receipt = BatchReceipt::try_new(
            descriptor,
            owner(),
            ConnectorCleanupOperationId::new(),
            [1; 32],
            [2; 32],
            0,
            [3; 32],
            BatchReceiptSummary::new(1, 2, 3, 4),
            Bytes::new(),
        )
        .unwrap();
        assert_eq!(receipt.summary().unknown(), 4);
        assert!(receipt.validate().is_ok());
    }
}

#[cfg(test)]
mod scan_safety_tests {
    use super::*;
    #[test]
    fn unproven_scan_safety_is_typed_unsupported_not_age_or_reachability() {
        let error = ConnectorCleanupScanSafety::Unproven
            .require_scanned_object_gc()
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(error.message().contains("Unknown/unreceived"));
        assert!(error.message().contains("restart"));
    }
}

/// Closed initial request. No object sweep authority is implied by an age.
pub struct ConnectorCleanupAsyncStart {
    pub original: super::ConnectorOriginalResultScope,
    pub table: super::ConnectorTableIdentity,
    pub operation_id: ConnectorCleanupOperationId,
    pub older_than_ms: i64,
    pub selection: Option<ConnectorCleanupOwnedRefSelection>,
    pub context: ConnectorRequestContext,
}
/// One original serial step; existing carrier validation remains authoritative.
pub enum ConnectorCleanupAsyncStep {
    Prepare(ConnectorCleanupPrepareRequest),
    Execute(ConnectorCleanupExecuteRequest),
    Page(ConnectorCleanupCandidatePageRequest),
    Finalize(ConnectorCleanupFinalizeRequest),
}
pub enum ConnectorCleanupAsyncOutput {
    Plan(ConnectorCleanupPlan),
    Prepared(PreparedBatch),
    Receipt(BatchReceipt),
    Page(CandidatePage),
    Finalized,
}
/// Provider original owns request, physical future, typed result and journal.
/// This is an observer/borrower API, never an execution or deletion grant.
pub trait ConnectorCleanupAsyncSource: Send + Sync {
    fn install_step_in(
        &self,
        step: &mut Option<ConnectorCleanupAsyncStep>,
    ) -> Result<(), ConnectorError>;
    fn poll_output(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<ConnectorCleanupAsyncOutput, ConnectorError>>;
    fn poll_closed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>>;
    fn publication_fact(&self) -> (Option<super::LakePublicationDisposition>, bool);
    /// One originating dispatch/required-finalization failure from the original
    /// journal. The boolean proves required finalization, never actual exit.
    fn original_failure(&self) -> Option<(ConnectorError, bool)>;
}
/// Read-only facts from the SAME original source; no dispatch/wait authority.
#[derive(Clone)]
pub struct ConnectorCleanupAsyncObservation {
    original: Arc<dyn ConnectorCleanupAsyncSource>,
}
impl ConnectorCleanupAsyncObservation {
    pub fn publication_fact(&self) -> (Option<super::LakePublicationDisposition>, bool) {
        self.original.publication_fact()
    }
    pub fn original_failure(&self) -> Option<(ConnectorError, bool)> {
        self.original.original_failure()
    }

    pub fn publication_disposition(&self) -> Option<super::LakePublicationDisposition> {
        self.original.publication_fact().0
    }
    pub fn has_applied(&self) -> bool {
        self.original.publication_fact().1
    }
}
pub struct ConnectorCleanupAsyncExecution {
    original: Arc<dyn ConnectorCleanupAsyncSource>,
    expected: Option<(ConnectorProviderBindingKey, ConnectorCleanupOperationId)>,
}
impl ConnectorCleanupAsyncExecution {
    pub fn observation(&self) -> ConnectorCleanupAsyncObservation {
        ConnectorCleanupAsyncObservation {
            original: self.original.clone(),
        }
    }

    pub fn from_original(original: Arc<dyn ConnectorCleanupAsyncSource>) -> Self {
        Self {
            original,
            expected: None,
        }
    }
    pub fn install_step_in(
        &self,
        step: &mut Option<ConnectorCleanupAsyncStep>,
    ) -> Result<(), ConnectorError> {
        if let Some((owner, operation)) = &self.expected {
            let plan = match step.as_ref().ok_or_else(|| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, "cleanup step is absent")
            })? {
                ConnectorCleanupAsyncStep::Prepare(x) => &x.plan,
                ConnectorCleanupAsyncStep::Execute(x) => &x.plan,
                ConnectorCleanupAsyncStep::Page(x) => &x.plan,
                ConnectorCleanupAsyncStep::Finalize(x) => &x.plan,
            };
            if plan.owner() != owner || plan.operation_id() != *operation {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup async step differs from original owner/operation",
                ));
            }
        }
        self.original.install_step_in(step)
    }
    pub fn poll_output(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<ConnectorCleanupAsyncOutput, ConnectorError>> {
        let value = std::task::ready!(self.original.poll_output(cx))?;
        if let Some((owner, operation)) = &self.expected {
            let matches = match &value {
                ConnectorCleanupAsyncOutput::Plan(x) => {
                    x.owner() == owner && x.operation_id() == *operation
                }
                ConnectorCleanupAsyncOutput::Prepared(x) => {
                    x.owner() == owner && x.operation_id() == *operation
                }
                ConnectorCleanupAsyncOutput::Receipt(x) => {
                    x.owner() == owner && x.operation_id() == *operation
                }
                ConnectorCleanupAsyncOutput::Page(x) => {
                    x.owner() == owner && x.operation_id() == *operation
                }
                ConnectorCleanupAsyncOutput::Finalized => true,
            };
            if !matches {
                return std::task::Poll::Ready(Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cleanup async output differs from original owner/operation",
                )));
            }
        }
        std::task::Poll::Ready(Ok(value))
    }
    pub fn poll_closed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        self.original.poll_closed(cx)
    }
    pub fn publication_disposition(&self) -> Option<super::LakePublicationDisposition> {
        self.original.publication_fact().0
    }
    pub fn has_applied(&self) -> bool {
        self.original.publication_fact().1
    }
}
