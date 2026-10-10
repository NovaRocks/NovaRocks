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

//! FE-only atomic staged-table publication contract.
//!
//! A provider prepares an invisible table, distributed writers stage data
//! against the returned opaque handle, and one provider commit publishes the
//! table initialization and the sealed writer aggregate. The handle payload
//! remains provider-private and is always fenced by the exact connector
//! incarnation that issued it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    CatalogHandle, ConnectorColumnDefinition, ConnectorControlRuntimeId,
    ConnectorDocumentCreatePublicationIntent, ConnectorDocumentManagementAdmission, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorMutationFailure,
    ConnectorMutationOperationId, ConnectorPartitionTransform, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableObjectId,
    ConnectorWriteLease, ConnectorWriteReceipt, CreatePolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, LakePublicationId,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MvExactPartitionField, MvExactPartitionTransform,
    ProviderBindingEpoch,
};

pub const CONNECTOR_STAGED_CREATE_CONTRACT_VERSION: u32 = 1;
pub const CONNECTOR_CTAS_UNANCHORED_CLEANUP_CONTRACT_VERSION: u16 = 1;
pub type ConnectorStagedCreateOperationId = ConnectorMutationOperationId;

const HANDLE_DOMAIN: &[u8] = b"novarocks.connector-staged-table-handle.v1\0";
const UNANCHORED_CTAS_PROVENANCE_DOMAIN: &[u8] =
    b"novarocks.connector-ctas-unanchored-provenance.v1\0";
const MAX_PREPARED_CREATE_FIELD_ID_BYTES: usize = 1024;
const MAX_PREPARED_CREATE_VERSION_ID_BYTES: usize = 1024;
const MAX_PREPARED_CREATE_DOCUMENT_FIELDS: usize = 4096;
const MAX_PREPARED_CREATE_PARTITION_FIELDS: usize = 4096;

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorPreparedCreateFieldBinding {
    request_ordinal: u32,
    provider_field_id: Bytes,
    name: String,
    type_signature: String,
    nullable: bool,
}

impl std::fmt::Debug for ConnectorPreparedCreateFieldBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedCreateFieldBinding")
            .field("request_ordinal", &self.request_ordinal)
            .field("provider_field_id_bytes", &self.provider_field_id.len())
            .field("name", &self.name)
            .field("type_signature", &self.type_signature)
            .field("nullable", &self.nullable)
            .finish()
    }
}

impl ConnectorPreparedCreateFieldBinding {
    pub fn try_new(
        request_ordinal: u32,
        provider_field_id: Bytes,
        name: String,
        type_signature: String,
        nullable: bool,
    ) -> Result<Self, ConnectorError> {
        if provider_field_id.is_empty()
            || provider_field_id.len() > MAX_PREPARED_CREATE_FIELD_ID_BYTES
            || name.trim().is_empty()
            || type_signature.trim().is_empty()
        {
            return Err(invalid(
                "prepared create field binding is empty or exceeds its byte limit",
            ));
        }
        Ok(Self {
            request_ordinal,
            provider_field_id,
            name,
            type_signature,
            nullable,
        })
    }

    pub const fn request_ordinal(&self) -> u32 {
        self.request_ordinal
    }

    pub const fn provider_field_id(&self) -> &Bytes {
        &self.provider_field_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_signature(&self) -> &str {
        &self.type_signature
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorPreparedCreateDocumentTarget {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    operation_id: ConnectorStagedCreateOperationId,
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    schema_version: Bytes,
    partition_spec_version: Bytes,
    fields: Vec<ConnectorPreparedCreateFieldBinding>,
    partition_fields: Vec<MvExactPartitionField>,
    provider_token: Bytes,
}

impl std::fmt::Debug for ConnectorPreparedCreateDocumentTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorPreparedCreateDocumentTarget")
            .field("owner", &self.owner)
            .field("catalog_handle", &self.catalog_handle)
            .field("operation_id", &self.operation_id)
            .field("target", &self.target)
            .field("object_id", &self.object_id)
            .field("schema_version_bytes", &self.schema_version.len())
            .field(
                "partition_spec_version_bytes",
                &self.partition_spec_version.len(),
            )
            .field("fields", &self.fields)
            .field("partition_fields", &self.partition_fields)
            .field("provider_token_bytes", &self.provider_token.len())
            .finish()
    }
}

impl ConnectorPreparedCreateDocumentTarget {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        operation_id: ConnectorStagedCreateOperationId,
        target: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        schema_version: Bytes,
        partition_spec_version: Bytes,
        mut fields: Vec<ConnectorPreparedCreateFieldBinding>,
        partition_fields: Vec<MvExactPartitionField>,
        provider_token: Bytes,
    ) -> Result<Self, ConnectorError> {
        if target.instance_id != owner.instance_id
            || catalog_handle.catalog_name() != &owner.instance_id
            || schema_version.is_empty()
            || partition_spec_version.is_empty()
            || schema_version.len() > MAX_PREPARED_CREATE_VERSION_ID_BYTES
            || partition_spec_version.len() > MAX_PREPARED_CREATE_VERSION_ID_BYTES
            || fields.is_empty()
            || fields.len() > MAX_PREPARED_CREATE_DOCUMENT_FIELDS
            || partition_fields.len() > MAX_PREPARED_CREATE_PARTITION_FIELDS
            || provider_token.is_empty()
            || provider_token.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        {
            return Err(invalid(
                "prepared create document target has an invalid owner, field set, or token",
            ));
        }
        let aggregate_bytes = 64usize
            .checked_add(owner.instance_id.as_str().len())
            .and_then(|bytes| bytes.checked_add(catalog_handle.catalog_name().as_str().len()))
            .and_then(|bytes| bytes.checked_add(target.instance_id.as_str().len()))
            .and_then(|bytes| bytes.checked_add(target.namespace.len()))
            .and_then(|bytes| bytes.checked_add(target.table.len()))
            .and_then(|bytes| bytes.checked_add(object_id.as_bytes().len()))
            .and_then(|bytes| bytes.checked_add(schema_version.len()))
            .and_then(|bytes| bytes.checked_add(partition_spec_version.len()))
            .and_then(|bytes| {
                fields.iter().try_fold(bytes, |bytes, field| {
                    bytes
                        .checked_add(std::mem::size_of::<u32>())
                        .and_then(|bytes| bytes.checked_add(field.provider_field_id().len()))
                        .and_then(|bytes| bytes.checked_add(field.name().len()))
                        .and_then(|bytes| bytes.checked_add(field.type_signature().len()))
                })
            })
            .and_then(|bytes| {
                partition_fields.iter().try_fold(bytes, |bytes, field| {
                    bytes
                        .checked_add(16)
                        .and_then(|bytes| bytes.checked_add(field.partition_field_id().len()))
                        .and_then(|bytes| bytes.checked_add(field.source_target_field_id().len()))
                })
            })
            .and_then(|bytes| bytes.checked_add(provider_token.len()))
            .ok_or_else(|| invalid("prepared create document target byte accounting overflowed"))?;
        if aggregate_bytes > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "prepared create document target exceeds the aggregate handle byte limit",
            ));
        }
        fields.sort_by_key(ConnectorPreparedCreateFieldBinding::request_ordinal);
        let mut identities = std::collections::HashSet::with_capacity(fields.len());
        if fields.iter().enumerate().any(|(expected_ordinal, field)| {
            field.request_ordinal() as usize != expected_ordinal
                || !identities.insert(field.provider_field_id().as_ref())
        }) {
            return Err(invalid(
                "prepared create document target requires dense unique field bindings",
            ));
        }
        let mut partition_identities =
            std::collections::HashSet::with_capacity(partition_fields.len());
        if partition_fields.iter().any(|field| {
            !partition_identities.insert(field.partition_field_id().as_ref())
                || !identities.contains(field.source_target_field_id().as_ref())
        }) {
            return Err(invalid(
                "prepared create document target has duplicate partition identities or a missing source field",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            operation_id,
            target,
            object_id,
            schema_version,
            partition_spec_version,
            fields,
            partition_fields,
            provider_token,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn schema_version(&self) -> &Bytes {
        &self.schema_version
    }

    pub const fn partition_spec_version(&self) -> &Bytes {
        &self.partition_spec_version
    }

    pub fn fields(&self) -> &[ConnectorPreparedCreateFieldBinding] {
        &self.fields
    }

    pub fn partition_fields(&self) -> &[MvExactPartitionField] {
        &self.partition_fields
    }

    pub const fn provider_token(&self) -> &Bytes {
        &self.provider_token
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        digest_bytes(hasher, self.owner.instance_id.as_str().as_bytes());
        hasher.update(self.owner.incarnation.to_bytes());
        hasher.update(self.catalog_handle.version().as_bytes());
        hasher.update(self.operation_id.to_bytes());
        digest_bytes(hasher, self.target.namespace.as_bytes());
        digest_bytes(hasher, self.target.table.as_bytes());
        digest_bytes(hasher, self.object_id.as_bytes());
        digest_bytes(hasher, &self.schema_version);
        digest_bytes(hasher, &self.partition_spec_version);
        hasher.update((self.fields.len() as u64).to_be_bytes());
        for field in &self.fields {
            hasher.update(field.request_ordinal.to_be_bytes());
            digest_bytes(hasher, &field.provider_field_id);
            digest_bytes(hasher, field.name().as_bytes());
            digest_bytes(hasher, field.type_signature().as_bytes());
            hasher.update([u8::from(field.nullable())]);
        }
        hasher.update((self.partition_fields.len() as u64).to_be_bytes());
        for field in &self.partition_fields {
            digest_bytes(hasher, field.partition_field_id());
            digest_bytes(hasher, field.source_target_field_id());
            match field.transform() {
                MvExactPartitionTransform::Identity => hasher.update([0]),
                MvExactPartitionTransform::Year => hasher.update([1]),
                MvExactPartitionTransform::Month => hasher.update([2]),
                MvExactPartitionTransform::Day => hasher.update([3]),
                MvExactPartitionTransform::Hour => hasher.update([4]),
                MvExactPartitionTransform::Bucket { num_buckets } => {
                    hasher.update([5]);
                    hasher.update(num_buckets.to_be_bytes());
                }
                MvExactPartitionTransform::Truncate { width } => {
                    hasher.update([6]);
                    hasher.update(width.to_be_bytes());
                }
                MvExactPartitionTransform::Void => hasher.update([7]),
            }
        }
        digest_bytes(hasher, &self.provider_token);
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorStagedTableHandle {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorStagedCreateOperationId,
    provider_payload: Bytes,
    document_target: Option<ConnectorPreparedCreateDocumentTarget>,
    digest: [u8; 32],
}

impl ConnectorStagedTableHandle {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_inner(owner, operation_id, provider_payload, None)
    }

    pub fn try_new_document_managed(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        provider_payload: Bytes,
        document_target: ConnectorPreparedCreateDocumentTarget,
    ) -> Result<Self, ConnectorError> {
        if document_target.owner() != &owner || document_target.operation_id() != operation_id {
            return Err(invalid(
                "prepared document target does not match its staged table handle",
            ));
        }
        Self::try_new_inner(owner, operation_id, provider_payload, Some(document_target))
    }

    fn try_new_inner(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        provider_payload: Bytes,
        document_target: Option<ConnectorPreparedCreateDocumentTarget>,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.is_empty()
            || provider_payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged table handle payload must fit the connector handle limit",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(HANDLE_DOMAIN);
        hasher.update(owner.instance_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(owner.incarnation.to_bytes());
        hasher.update(operation_id.to_bytes());
        digest_bytes(&mut hasher, provider_payload.as_ref());
        if let Some(document_target) = &document_target {
            hasher.update([1]);
            document_target.digest_into(&mut hasher);
        } else {
            hasher.update([0]);
        }
        let digest = hasher.finalize().into();
        Ok(Self {
            owner,
            operation_id,
            provider_payload,
            document_target,
            digest,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub const fn document_target(&self) -> Option<&ConnectorPreparedCreateDocumentTarget> {
        self.document_target.as_ref()
    }
}

#[derive(Clone)]
pub struct ConnectorStagedWritePlanningRequest {
    pub handle: ConnectorStagedTableHandle,
    pub context: ConnectorRequestContext,
}

/// The frozen facts a write session needs to open on one exact invisible
/// target.
///
/// This carries no write operation, cohort, or input shape: a staged target is
/// a *place to write*, and what is written there — the intent, the fields, the
/// branches — is decided by the session that opens on it. The table handle
/// stays opaque; only the provider that minted it can read it.
#[derive(Clone)]
pub struct ConnectorStagedWritePlanningBinding {
    owner: ConnectorProviderBindingKey,
    target_operation_id: ConnectorStagedCreateOperationId,
    target_handle_digest: [u8; 32],
    table: ConnectorTableHandle,
    provider_payload: Bytes,
    context: ConnectorRequestContext,
}

impl ConnectorStagedWritePlanningBinding {
    pub fn try_new(
        handle: &ConnectorStagedTableHandle,
        table: ConnectorTableHandle,
        provider_payload: Bytes,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
            return Err(invalid(
                "staged writer provider payload exceeds the handle limit",
            ));
        }
        if table.owner() != &handle.owner().instance_id {
            return Err(invalid(
                "staged writer table handle does not match the exact target owner",
            ));
        }
        Ok(Self {
            owner: handle.owner().clone(),
            target_operation_id: handle.operation_id(),
            target_handle_digest: handle.digest(),
            table,
            provider_payload,
            context,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn target_operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.target_operation_id
    }

    pub const fn target_handle_digest(&self) -> [u8; 32] {
        self.target_handle_digest
    }

    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
}

/// Everything a staged publication learns from the write that filled it.
///
/// A completed write session produces exactly one receipt, and the rows it
/// made durable are counted beside it. Those two facts are the whole boundary:
/// there is deliberately no operation id, no cohort set, no per-attempt
/// aggregate, and no writer manifest here, because a publication decides
/// nothing from any of them. If a provider needs more than this to publish, the
/// missing fact belongs *inside* the receipt its own session minted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedWriteProof {
    receipt: ConnectorWriteReceipt,
    row_count: u64,
}

impl ConnectorStagedWriteProof {
    pub fn try_new(receipt: ConnectorWriteReceipt, row_count: u64) -> Result<Self, ConnectorError> {
        receipt.validate()?;
        Ok(Self { receipt, row_count })
    }

    pub const fn receipt(&self) -> &ConnectorWriteReceipt {
        &self.receipt
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The identity two references to the same sealed write share. A staged
    /// target binds one of these and refuses to publish under another.
    pub const fn digest(&self) -> [u8; 32] {
        self.receipt.digest()
    }
}

impl std::fmt::Debug for ConnectorStagedTableHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStagedTableHandle")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone)]
pub enum ConnectorStagedCreateMode {
    Ordinary,
    ApplicationDocumentManaged {
        admission: ConnectorDocumentManagementAdmission,
    },
}

impl ConnectorStagedCreateMode {
    fn validate_for(
        &self,
        owner: &ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        table: &ConnectorTableIdentity,
    ) -> Result<(), ConnectorError> {
        if let Self::ApplicationDocumentManaged { admission } = self {
            admission.validate()?;
            if admission.owner() != owner
                || admission.operation_id() != operation_id
                || admission.target() != table
                || admission.expected_object_id().is_some()
                || admission.operation() != super::ConnectorDocumentManagementOperation::Create
            {
                return Err(invalid(
                    "staged create document admission does not match its exact operation",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorStagedCreatePrepareRequest {
    pub owner: ConnectorProviderBindingKey,
    /// The statement-level identity shared by the staged root, Iceberg marker
    /// and eventual unanchored-GC provenance.  `operation_id` remains the
    /// lease-local dispatch key; it must not become a second durable owner.
    pub publication_id: LakePublicationId,
    pub operation_id: ConnectorStagedCreateOperationId,
    pub table: ConnectorTableIdentity,
    pub columns: Vec<ConnectorColumnDefinition>,
    pub partitioning: Vec<ConnectorPartitionTransform>,
    pub properties: BTreeMap<Arc<str>, Arc<str>>,
    pub policy: CreatePolicy,
    pub mode: ConnectorStagedCreateMode,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateReceiptPhase {
    Prepared,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedCreateReceipt {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorStagedCreateOperationId,
    phase: ConnectorStagedCreateReceiptPhase,
    effect: ExternalMutationEffect,
    provider_payload: Bytes,
}

impl ConnectorStagedCreateReceipt {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
        effect: ExternalMutationEffect,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.len() > super::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "staged-create receipt exceeds the evidence limit",
            ));
        }
        Ok(Self {
            owner,
            operation_id,
            phase,
            effect,
            provider_payload,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorStagedCreateOperationId {
        self.operation_id
    }
    pub const fn phase(&self) -> ConnectorStagedCreateReceiptPhase {
        self.phase
    }
    pub const fn effect(&self) -> ExternalMutationEffect {
        self.effect
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreatePrepareOutcome {
    Prepared {
        handle: ConnectorStagedTableHandle,
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Conflict {
        failure: ConnectorMutationFailure,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone)]
pub enum ConnectorStagedCreatePublicationPayload {
    Ordinary,
    ApplicationDocuments(ConnectorDocumentCreatePublicationIntent),
}

#[derive(Clone)]
pub struct ConnectorStagedCreatePublishRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub handle: ConnectorStagedTableHandle,
    pub write: ConnectorStagedWriteProof,
    pub payload: ConnectorStagedCreatePublicationPayload,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreatePublishOutcome {
    Applied {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    NoOp {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    Conflict {
        failure: ConnectorMutationFailure,
        cleanup: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
        cleanup: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone)]
pub struct ConnectorStagedCreateAbortRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub handle: ConnectorStagedTableHandle,
    pub write: Option<ConnectorStagedWriteProof>,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreateAbortOutcome {
    Aborted {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone)]
pub struct ConnectorStagedCreatePublicationAdjudicationRequest {
    pub target_operation_id: ConnectorStagedCreateOperationId,
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorStagedCreatePublicationAdjudicationOutcome {
    Published {
        receipt: ConnectorStagedCreateReceipt,
        finalization: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub trait ConnectorStagedCreate: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;

    fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>;

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError>;

    /// Bind one sealed write to the exact opaque staged target. This is
    /// provider-local and must complete before the application records the
    /// write as ready for publication.
    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        write: ConnectorStagedWriteProof,
    ) -> Result<(), ConnectorError>;

    fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError>;

    fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError>;

    /// Read one exact previously-unknown publication frontier. This method
    /// cannot prepare, abort, clean up, or reopen a staged target.
    fn adjudicate_publication(
        &self,
        request: ConnectorStagedCreatePublicationAdjudicationRequest,
    ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError>;
}

/// Provenance frozen by CTAS staging and re-checked by unanchored cleanup.
///
/// A table UUID is optional only when the stage-create response was lost: in
/// that case the cleanup adapter must retain the prefix, rather than infer a
/// match from the target name or from a warehouse listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasUnanchoredProvenance {
    pub publication_id: LakePublicationId,
    pub target: ConnectorTableIdentity,
    pub expected_absent: bool,
    pub staged_table_uuid: Option<[u8; 16]>,
    pub created_at_ms: i64,
    pub digest: [u8; 32],
}

impl ConnectorCtasUnanchoredProvenance {
    pub fn try_new(
        publication_id: LakePublicationId,
        target: ConnectorTableIdentity,
        expected_absent: bool,
        staged_table_uuid: Option<[u8; 16]>,
        created_at_ms: i64,
    ) -> Result<Self, ConnectorError> {
        if target.namespace.is_empty()
            || target.table.is_empty()
            || !expected_absent
            || created_at_ms <= 0
        {
            return Err(invalid(
                "unanchored CTAS provenance must name an absent target and a positive creation time",
            ));
        }
        let digest = unanchored_provenance_digest(
            publication_id,
            &target,
            expected_absent,
            staged_table_uuid,
            created_at_ms,
        );
        Ok(Self {
            publication_id,
            target,
            expected_absent,
            staged_table_uuid,
            created_at_ms,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = unanchored_provenance_digest(
            self.publication_id,
            &self.target,
            self.expected_absent,
            self.staged_table_uuid,
            self.created_at_ms,
        );
        if self.digest != expected {
            return Err(invalid("unanchored CTAS provenance digest is invalid"));
        }
        Self::try_new(
            self.publication_id,
            self.target.clone(),
            self.expected_absent,
            self.staged_table_uuid,
            self.created_at_ms,
        )
        .map(|_| ())
    }
}

/// Exact cleanup input for a staged CTAS whose target table was never
/// registered. This is deliberately not a table-orphan cleanup request: its
/// root is a deterministic warehouse namespace and cannot be discovered from
/// a table handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasUnanchoredCleanupRequest {
    pub owner: ConnectorProviderBindingKey,
    pub warehouse_root: Arc<str>,
    pub cutoff_ms: i64,
    pub provenance: ConnectorCtasUnanchoredProvenance,
}

/// Catalog-wide discovery input for the deterministic CTAS staging namespace.
/// It carries the same age boundary as exact deletion, so a provider cannot
/// list young roots and leave filtering to a later, potentially skewed owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCtasUnanchoredDiscoveryRequest {
    pub owner: ConnectorProviderBindingKey,
    pub warehouse_root: Arc<str>,
    pub cutoff_ms: i64,
}

impl ConnectorCtasUnanchoredDiscoveryRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        warehouse_root: Arc<str>,
        cutoff_ms: i64,
    ) -> Result<Self, ConnectorError> {
        if warehouse_root.trim_end_matches('/').is_empty() || cutoff_ms <= 0 {
            return Err(invalid(
                "unanchored CTAS discovery requires an explicit warehouse root and positive cutoff",
            ));
        }
        Ok(Self {
            owner,
            warehouse_root,
            cutoff_ms,
        })
    }
}

impl ConnectorCtasUnanchoredCleanupRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        warehouse_root: Arc<str>,
        cutoff_ms: i64,
        provenance: ConnectorCtasUnanchoredProvenance,
    ) -> Result<Self, ConnectorError> {
        if warehouse_root.trim_end_matches('/').is_empty() || cutoff_ms <= 0 {
            return Err(invalid(
                "unanchored CTAS cleanup requires an explicit warehouse root and positive cutoff",
            ));
        }
        provenance.validate()?;
        if provenance.target.instance_id != owner.instance_id {
            return Err(invalid(
                "unanchored CTAS cleanup target has a foreign connector owner",
            ));
        }
        Ok(Self {
            owner,
            warehouse_root,
            cutoff_ms,
            provenance,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorCtasUnanchoredCleanupOutcome {
    Deleted,
    Retained,
    CommitUnknown { failure: ConnectorMutationFailure },
}

/// CTAS-specific cleanup capability.  It first performs an exact target
/// lookup, validates provenance at the deterministic prefix, and only then
/// deletes that prefix. Any negative or ambiguous observation is retained.
pub trait ConnectorUnanchoredCtasCleanup: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn warehouse_root(&self) -> Result<Arc<str>, ConnectorError>;

    /// Enumerate only provenance sidecars under the fixed warehouse prefix.
    /// Missing, malformed, young, or unverifiable roots are deliberately not
    /// candidates; the caller has no durable recovery index to compensate.
    fn discover_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError>;

    fn inspect_then_delete_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredCleanupRequest,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError>;
}

/// Lease for the catalog-wide CTAS staging namespace. Unlike a staged-create
/// lease it has no statement-local operation registry: the caller supplies a
/// fully frozen provenance record and the provider must independently re-read
/// both the target and sidecar before deleting one exact root.
#[derive(Clone)]
pub struct ConnectorUnanchoredCtasCleanupLease {
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    capability: Arc<dyn ConnectorUnanchoredCtasCleanup>,
    _release: Arc<StagedCreateLeaseRelease>,
}

impl ConnectorUnanchoredCtasCleanupLease {
    pub fn new_with_control_runtime(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        capability: Arc<dyn ConnectorUnanchoredCtasCleanup>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if capability.descriptor() != &descriptor
            || capability.incarnation() != provider_incarnation
        {
            return Err(invalid(
                "unanchored CTAS cleanup capability does not match its lease generation",
            ));
        }
        Ok(Self {
            control_runtime_id,
            provider_binding_key,
            capability,
            _release: Arc::new(StagedCreateLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    pub fn warehouse_root(&self) -> Result<Arc<str>, ConnectorError> {
        self.capability.warehouse_root()
    }

    /// Builds the provider-private request under this retained FE runtime
    /// lease. Callers never select a provider generation for cleanup.
    pub fn inspect_then_delete_unanchored_ctas(
        &self,
        warehouse_root: Arc<str>,
        cutoff_ms: i64,
        provenance: ConnectorCtasUnanchoredProvenance,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
        let request = ConnectorCtasUnanchoredCleanupRequest::try_new(
            self.provider_binding_key.clone(),
            warehouse_root,
            cutoff_ms,
            provenance,
        )?;
        self.capability
            .inspect_then_delete_unanchored_ctas(request, context)
    }

    pub fn discover_unanchored_ctas(
        &self,
        warehouse_root: Arc<str>,
        cutoff_ms: i64,
        context: ConnectorRequestContext,
    ) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError> {
        let request = ConnectorCtasUnanchoredDiscoveryRequest::try_new(
            self.provider_binding_key.clone(),
            warehouse_root,
            cutoff_ms,
        )?;
        self.capability.discover_unanchored_ctas(request, context)
    }
}

#[derive(Clone)]
pub struct ConnectorStagedCreateLease {
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    capability: Arc<dyn ConnectorStagedCreate>,
    operations: Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, LeaseOperationState>>>,
    _release: Arc<StagedCreateLeaseRelease>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseOperationState {
    Preparing,
    Unpublished {
        handle_digest: [u8; 32],
        bound_write: Option<BoundWriteProof>,
    },
    Published,
    Aborted,
    Unknown {
        handle_digest: Option<[u8; 32]>,
        bound_write: Option<BoundWriteProof>,
    },
    PublicationUnknown {
        evidence_digest: [u8; 32],
        handle_digest: [u8; 32],
        bound_write: BoundWriteProof,
    },
}

/// What the lease remembers about the one write bound to a staged target.
///
/// It is the sealed write's own identity — its receipt digest and the rows it
/// carries — so a publication offered a different write is refused without the
/// lease holding the write itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BoundWriteProof {
    receipt_digest: [u8; 32],
    row_count: u64,
}

impl BoundWriteProof {
    fn of(write: &ConnectorStagedWriteProof) -> Self {
        Self {
            receipt_digest: write.digest(),
            row_count: write.row_count(),
        }
    }
}

struct StagedCreateLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorStagedCreateLease {
    pub fn new_with_control_runtime(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        capability: Arc<dyn ConnectorStagedCreate>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if capability.descriptor() != &descriptor
            || capability.incarnation() != provider_incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged-create capability does not match its lease generation",
            ));
        }
        Ok(Self {
            control_runtime_id,
            provider_binding_key,
            capability,
            operations: Arc::new(Mutex::new(HashMap::new())),
            _release: Arc::new(StagedCreateLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    pub fn instance_id(&self) -> &super::ConnectorInstanceId {
        &self.provider_binding_key.instance_id
    }

    /// Verifies that the retained staged-create effect lease and the BE
    /// writer fence came from one provider generation, without exposing that
    /// provider-generation key to FE effect orchestration.
    pub fn matches_write_lease(&self, write_lease: &ConnectorWriteLease) -> bool {
        write_lease.matches_provider_binding_key(&self.provider_binding_key)
    }

    /// Constructs the provider-private prepare carrier from facts already
    /// frozen by the FE-owned control-runtime lease.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_request(
        &self,
        publication_id: LakePublicationId,
        operation_id: ConnectorStagedCreateOperationId,
        table: ConnectorTableIdentity,
        columns: Vec<ConnectorColumnDefinition>,
        partitioning: Vec<ConnectorPartitionTransform>,
        properties: BTreeMap<Arc<str>, Arc<str>>,
        policy: CreatePolicy,
        context: ConnectorRequestContext,
    ) -> ConnectorStagedCreatePrepareRequest {
        ConnectorStagedCreatePrepareRequest {
            owner: self.provider_binding_key.clone(),
            publication_id,
            operation_id,
            table,
            columns,
            partitioning,
            properties,
            policy,
            mode: ConnectorStagedCreateMode::Ordinary,
            context,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_document_managed_request(
        &self,
        publication_id: LakePublicationId,
        operation_id: ConnectorStagedCreateOperationId,
        table: ConnectorTableIdentity,
        columns: Vec<ConnectorColumnDefinition>,
        partitioning: Vec<ConnectorPartitionTransform>,
        properties: BTreeMap<Arc<str>, Arc<str>>,
        policy: CreatePolicy,
        admission: ConnectorDocumentManagementAdmission,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorStagedCreatePrepareRequest, ConnectorError> {
        let request = ConnectorStagedCreatePrepareRequest {
            owner: self.provider_binding_key.clone(),
            publication_id,
            operation_id,
            table,
            columns,
            partitioning,
            properties,
            policy,
            mode: ConnectorStagedCreateMode::ApplicationDocumentManaged { admission },
            context,
        };
        request
            .mode
            .validate_for(&request.owner, request.operation_id, &request.table)?;
        Ok(request)
    }

    #[cfg(test)]
    pub fn new(
        owner: ConnectorProviderBindingKey,
        capability: Arc<dyn ConnectorStagedCreate>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let descriptor = capability.descriptor().clone();
        Self::new_with_control_runtime(
            descriptor,
            ConnectorControlRuntimeId::new(),
            owner.incarnation,
            capability,
            release,
        )
    }

    #[cfg(test)]
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.provider_binding_key
    }

    pub fn prepare(
        &self,
        request: ConnectorStagedCreatePrepareRequest,
    ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
        if request.owner != self.provider_binding_key
            || request.table.instance_id != self.provider_binding_key.instance_id
        {
            return Err(invalid("staged-create prepare request has a foreign owner"));
        }
        if request.operation_id.to_bytes() != request.publication_id.to_bytes() {
            return Err(invalid(
                "staged-create operation ID must equal its statement publication ID",
            ));
        }
        request
            .mode
            .validate_for(&request.owner, request.operation_id, &request.table)?;
        let retained_request = request.clone();
        let operation_id = request.operation_id;
        {
            let mut operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            if operations.contains_key(&operation_id) {
                return Err(invalid(
                    "staged-create operation ID is already reserved by this lease",
                ));
            }
            operations.insert(operation_id, LeaseOperationState::Preparing);
        }
        let outcome = match self.capability.prepare(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.record_after_dispatch(
                    operation_id,
                    Some(LeaseOperationState::Unknown {
                        handle_digest: None,
                        bound_write: None,
                    }),
                );
                return Err(error);
            }
        };
        if let Err(error) = self.validate_prepare_outcome(&retained_request, &outcome) {
            self.record_after_dispatch(
                operation_id,
                Some(LeaseOperationState::Unknown {
                    handle_digest: None,
                    bound_write: None,
                }),
            );
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } => {
                Some(LeaseOperationState::Unpublished {
                    handle_digest: handle.digest(),
                    bound_write: None,
                })
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. } => {
                Some(LeaseOperationState::Unknown {
                    handle_digest: None,
                    bound_write: None,
                })
            }
            ConnectorStagedCreatePrepareOutcome::Conflict { .. }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { .. } => None,
        };
        self.record_after_dispatch(operation_id, state);
        Ok(outcome)
    }

    pub fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        write: ConnectorStagedWriteProof,
    ) -> Result<(), ConnectorError> {
        self.require_unpublished(&handle)?;
        let target_operation_id = handle.operation_id();
        let proof = BoundWriteProof::of(&write);
        if let Err(error) = self.capability.bind_write(handle.clone(), write) {
            self.record_after_dispatch(
                target_operation_id,
                Some(LeaseOperationState::Unknown {
                    handle_digest: Some(handle.digest()),
                    bound_write: Some(proof),
                }),
            );
            return Err(error);
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: handle.digest(),
                bound_write: Some(proof),
            }),
        );
        Ok(())
    }

    pub fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
        self.require_unpublished(&request.handle)?;
        if request.handle.owner() != &self.provider_binding_key {
            return Err(invalid("staged writer planning has a foreign owner"));
        }
        let target_operation_id = request.handle.operation_id();
        let target_handle_digest = request.handle.digest();
        let prior_bound_write = self.bound_write_for_abort(&request.handle, None)?;
        let binding = match self.capability.plan_write(request) {
            Ok(binding) => binding,
            Err(error) => {
                self.record_after_dispatch(
                    target_operation_id,
                    Some(LeaseOperationState::Unpublished {
                        handle_digest: target_handle_digest,
                        bound_write: prior_bound_write,
                    }),
                );
                return Err(error);
            }
        };
        if binding.owner() != &self.provider_binding_key
            || binding.target_operation_id() != target_operation_id
            || binding.target_handle_digest() != target_handle_digest
        {
            self.set_unknown_without_evidence(
                target_operation_id,
                Some(target_handle_digest),
                prior_bound_write,
            );
            return Err(invalid(
                "staged writer planning binding drifted from its exact target request",
            ));
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: target_handle_digest,
                bound_write: prior_bound_write,
            }),
        );
        Ok(binding)
    }

    pub fn publish(
        &self,
        request: ConnectorStagedCreatePublishRequest,
    ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
        let bound_write = self.require_bound_write(&request.handle, &request.write)?;
        match &request.payload {
            ConnectorStagedCreatePublicationPayload::Ordinary
                if request.handle.document_target().is_none() => {}
            ConnectorStagedCreatePublicationPayload::ApplicationDocuments(intent) => {
                intent.validate_for_handle(&request.handle)?;
            }
            _ => {
                return Err(invalid(
                    "staged create publication payload does not match its prepared target mode",
                ));
            }
        }
        let operation_id = request.handle.operation_id();
        let handle_digest = request.handle.digest();
        let dispatch_operation_id = request.operation_id;
        let outcome = match self.capability.publish(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.set_unknown_without_evidence(
                    operation_id,
                    Some(handle_digest),
                    Some(bound_write),
                );
                return Err(error);
            }
        };
        if let Err(error) = self.validate_publish_outcome(dispatch_operation_id, &outcome) {
            self.set_unknown_without_evidence(operation_id, Some(handle_digest), Some(bound_write));
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreatePublishOutcome::Applied { .. } => LeaseOperationState::Published,
            ConnectorStagedCreatePublishOutcome::NoOp { .. }
            | ConnectorStagedCreatePublishOutcome::Conflict { .. }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. } => {
                LeaseOperationState::Unpublished {
                    handle_digest,
                    bound_write: Some(bound_write),
                }
            }
            ConnectorStagedCreatePublishOutcome::CommitUnknown { evidence, .. } => {
                LeaseOperationState::PublicationUnknown {
                    evidence_digest: evidence.digest(),
                    handle_digest,
                    bound_write,
                }
            }
        };
        self.record_after_dispatch(operation_id, Some(state));
        Ok(outcome)
    }

    pub fn mark_write_unknown(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.provider_binding_key {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let mut operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        let bound_write = match operations.get(&handle.operation_id()) {
            Some(LeaseOperationState::Unpublished {
                handle_digest,
                bound_write,
            }) if handle_digest == &handle.digest() => *bound_write,
            Some(
                LeaseOperationState::Unknown { .. }
                | LeaseOperationState::PublicationUnknown { .. },
            ) => {
                return Err(invalid(
                    "staged table operation is unresolved; publish and abort are forbidden",
                ));
            }
            _ => {
                return Err(invalid(
                    "staged table handle was not issued by this retained lease",
                ));
            }
        };
        operations.insert(
            handle.operation_id(),
            LeaseOperationState::Unknown {
                handle_digest: Some(handle.digest()),
                bound_write,
            },
        );
        Ok(())
    }

    /// Restore an unpublished staged target after the write session proves its
    /// sealed write complete. This is an explicit recovery path for
    /// write-staging uncertainty; it never dispatches a catalog mutation and
    /// cannot substitute a different write.
    pub fn reconcile_write_proof(
        &self,
        handle: ConnectorStagedTableHandle,
        write: ConnectorStagedWriteProof,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.provider_binding_key {
            return Err(invalid("staged-create writer recovery has a foreign owner"));
        }
        let target_operation_id = handle.operation_id();
        let proof = BoundWriteProof::of(&write);
        {
            let operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            match operations.get(&target_operation_id) {
                Some(LeaseOperationState::Unknown {
                    handle_digest: Some(handle_digest),
                    bound_write,
                }) if *handle_digest == handle.digest()
                    && bound_write.is_none_or(|bound| bound == proof) => {}
                _ => {
                    return Err(invalid(
                        "staged-create writer recovery requires the exact unresolved write",
                    ));
                }
            }
        }
        if let Err(error) = self.capability.bind_write(handle.clone(), write) {
            self.record_after_dispatch(
                target_operation_id,
                Some(LeaseOperationState::Unknown {
                    handle_digest: Some(handle.digest()),
                    bound_write: Some(proof),
                }),
            );
            return Err(error);
        }
        self.record_after_dispatch(
            target_operation_id,
            Some(LeaseOperationState::Unpublished {
                handle_digest: handle.digest(),
                bound_write: Some(proof),
            }),
        );
        Ok(())
    }

    pub fn abort(
        &self,
        request: ConnectorStagedCreateAbortRequest,
    ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
        let bound_write = self.bound_write_for_abort(&request.handle, request.write.as_ref())?;
        let operation_id = request.handle.operation_id();
        let handle_digest = request.handle.digest();
        let dispatch_operation_id = request.operation_id;
        let outcome = match self.capability.abort(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.set_unknown_without_evidence(operation_id, Some(handle_digest), bound_write);
                return Err(error);
            }
        };
        if let Err(error) = self.validate_abort_outcome(dispatch_operation_id, &outcome) {
            self.set_unknown_without_evidence(operation_id, Some(handle_digest), bound_write);
            return Err(error);
        }
        let state = match &outcome {
            ConnectorStagedCreateAbortOutcome::Aborted { .. } => LeaseOperationState::Aborted,
            ConnectorStagedCreateAbortOutcome::KnownUncommitted { .. } => {
                LeaseOperationState::Unpublished {
                    handle_digest,
                    bound_write,
                }
            }
            ConnectorStagedCreateAbortOutcome::CommitUnknown { .. } => {
                LeaseOperationState::Unknown {
                    handle_digest: Some(handle_digest),
                    bound_write,
                }
            }
        };
        self.record_after_dispatch(operation_id, Some(state));
        Ok(outcome)
    }

    /// Consume the one retained exact publication observation for a prior
    /// publish-unknown outcome. Any result other than an exact published
    /// receipt permanently closes this lease to further mutation or reads.
    pub fn adjudicate_publication(
        &self,
        request: ConnectorStagedCreatePublicationAdjudicationRequest,
    ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
        let operation_id = request.target_operation_id;
        let dispatch_operation_id = request.evidence.operation_id();
        self.validate_evidence_for(
            &request.evidence,
            dispatch_operation_id,
            "staged-create-publish",
        )?;
        let (unresolved_handle_digest, unresolved_bound_write) = {
            let operations = self
                .operations
                .lock()
                .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
            match operations.get(&operation_id) {
                Some(LeaseOperationState::PublicationUnknown {
                    evidence_digest,
                    handle_digest,
                    bound_write,
                }) if *evidence_digest == request.evidence.digest() => {
                    (*handle_digest, *bound_write)
                }
                _ => {
                    return Err(invalid(
                        "staged-create publication adjudication requires the exact publish-unknown evidence",
                    ));
                }
            }
        };
        let outcome = match self.capability.adjudicate_publication(request) {
            Ok(outcome) => outcome,
            Err(error) => {
                // The sole observation attempt was consumed once the provider
                // call began. Do not retain evidence that could authorize a
                // second adjudication after a transport failure.
                self.record_after_dispatch(
                    operation_id,
                    Some(LeaseOperationState::Unknown {
                        handle_digest: Some(unresolved_handle_digest),
                        bound_write: Some(unresolved_bound_write),
                    }),
                );
                return Err(error);
            }
        };
        let state = match &outcome {
            ConnectorStagedCreatePublicationAdjudicationOutcome::Published { receipt, .. } => {
                self.validate_receipt(
                    receipt,
                    dispatch_operation_id,
                    ConnectorStagedCreateReceiptPhase::Published,
                )?;
                LeaseOperationState::Published
            }
            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                evidence, ..
            } => {
                self.validate_evidence_for(
                    evidence,
                    dispatch_operation_id,
                    "staged-create-publish",
                )?;
                LeaseOperationState::Unknown {
                    handle_digest: Some(unresolved_handle_digest),
                    bound_write: Some(unresolved_bound_write),
                }
            }
        };
        self.record_after_dispatch(operation_id, Some(state));
        Ok(outcome)
    }

    fn require_unpublished(
        &self,
        handle: &ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.provider_binding_key {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        match operations.get(&handle.operation_id()) {
            Some(LeaseOperationState::Unpublished { handle_digest, .. })
                if handle_digest == &handle.digest() =>
            {
                Ok(())
            }
            Some(
                LeaseOperationState::Unknown { .. }
                | LeaseOperationState::PublicationUnknown { .. },
            ) => Err(invalid(
                "staged table operation is unresolved; publish and abort are forbidden",
            )),
            Some(LeaseOperationState::Published) => Err(invalid(
                "published staged table cannot be aborted or republished",
            )),
            Some(LeaseOperationState::Aborted) => {
                Err(invalid("aborted staged table cannot be reused"))
            }
            Some(LeaseOperationState::Preparing) => {
                Err(invalid("staged table prepare is still in progress"))
            }
            _ => Err(invalid(
                "staged table handle was not issued by this retained lease",
            )),
        }
    }

    fn require_bound_write(
        &self,
        handle: &ConnectorStagedTableHandle,
        write: &ConnectorStagedWriteProof,
    ) -> Result<BoundWriteProof, ConnectorError> {
        let bound = self.bound_write_for_abort(handle, Some(write))?;
        bound.ok_or_else(|| invalid("staged-create publish requires an exact bound write"))
    }

    fn bound_write_for_abort(
        &self,
        handle: &ConnectorStagedTableHandle,
        write: Option<&ConnectorStagedWriteProof>,
    ) -> Result<Option<BoundWriteProof>, ConnectorError> {
        if handle.owner() != &self.provider_binding_key {
            return Err(invalid("staged table handle has a foreign owner"));
        }
        let operations = self
            .operations
            .lock()
            .map_err(|error| invalid(format!("staged-create lease lock: {error}")))?;
        let Some(LeaseOperationState::Unpublished {
            handle_digest,
            bound_write,
        }) = operations.get(&handle.operation_id())
        else {
            drop(operations);
            self.require_unpublished(handle)?;
            unreachable!("require_unpublished accepted a non-unpublished state")
        };
        if handle_digest != &handle.digest() {
            return Err(invalid("staged table handle digest mismatch"));
        }
        if let Some(write) = write {
            let observed = BoundWriteProof::of(write);
            if bound_write.as_ref() != Some(&observed) {
                return Err(invalid(
                    "staged-create write is not bound to this exact opaque target",
                ));
            }
        }
        Ok(*bound_write)
    }

    fn set_unknown_without_evidence(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        handle_digest: Option<[u8; 32]>,
        bound_write: Option<BoundWriteProof>,
    ) {
        self.record_after_dispatch(
            operation_id,
            Some(LeaseOperationState::Unknown {
                handle_digest,
                bound_write,
            }),
        );
    }

    fn record_after_dispatch(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        state: Option<LeaseOperationState>,
    ) {
        // Once provider dispatch has happened, a poisoned process-local lease
        // mutex must not downgrade an authoritative typed outcome or discard
        // exact unknown evidence. Recover the retained map and record the
        // result before returning it to the caller.
        let mut operations = match self.operations.lock() {
            Ok(operations) => operations,
            Err(poisoned) => {
                let operations = poisoned.into_inner();
                self.operations.clear_poison();
                operations
            }
        };
        if let Some(state) = state {
            operations.insert(operation_id, state);
        } else {
            operations.remove(&operation_id);
        }
    }

    fn validate_prepare_outcome(
        &self,
        request: &ConnectorStagedCreatePrepareRequest,
        outcome: &ConnectorStagedCreatePrepareOutcome,
    ) -> Result<(), ConnectorError> {
        let operation_id = request.operation_id;
        match outcome {
            ConnectorStagedCreatePrepareOutcome::Prepared {
                handle, receipt, ..
            } => {
                self.validate_handle(handle, operation_id)?;
                match (&request.mode, handle.document_target()) {
                    (ConnectorStagedCreateMode::Ordinary, None) => {}
                    (
                        ConnectorStagedCreateMode::ApplicationDocumentManaged { admission },
                        Some(target),
                    ) if target.owner() == &request.owner
                        && target.catalog_handle() == admission.catalog_handle()
                        && target.operation_id() == request.operation_id
                        && target.target() == &request.table
                        && target.fields().len() == request.columns.len() => {}
                    _ => {
                        return Err(invalid(
                            "staged create provider returned a target for a different request or management mode",
                        ));
                    }
                }
                self.validate_receipt(
                    receipt,
                    operation_id,
                    ConnectorStagedCreateReceiptPhase::Prepared,
                )
            }
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence_for(evidence, operation_id, "staged-create-prepare")
            }
            ConnectorStagedCreatePrepareOutcome::Conflict { .. }
            | ConnectorStagedCreatePrepareOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_publish_outcome(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        outcome: &ConnectorStagedCreatePublishOutcome,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ConnectorStagedCreatePublishOutcome::Applied { receipt, .. }
            | ConnectorStagedCreatePublishOutcome::NoOp { receipt, .. } => self.validate_receipt(
                receipt,
                operation_id,
                ConnectorStagedCreateReceiptPhase::Published,
            ),
            ConnectorStagedCreatePublishOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence_for(evidence, operation_id, "staged-create-publish")
            }
            ConnectorStagedCreatePublishOutcome::Conflict { .. }
            | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_abort_outcome(
        &self,
        operation_id: ConnectorStagedCreateOperationId,
        outcome: &ConnectorStagedCreateAbortOutcome,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ConnectorStagedCreateAbortOutcome::Aborted { receipt, .. } => self.validate_receipt(
                receipt,
                operation_id,
                ConnectorStagedCreateReceiptPhase::Aborted,
            ),
            ConnectorStagedCreateAbortOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence_for(evidence, operation_id, "staged-create-abort")
            }
            ConnectorStagedCreateAbortOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_handle(
        &self,
        handle: &ConnectorStagedTableHandle,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> Result<(), ConnectorError> {
        if handle.owner() != &self.provider_binding_key || handle.operation_id() != operation_id {
            return Err(invalid(
                "staged table handle does not match its prepare request",
            ));
        }
        Ok(())
    }

    fn validate_receipt(
        &self,
        receipt: &ConnectorStagedCreateReceipt,
        operation_id: ConnectorStagedCreateOperationId,
        phase: ConnectorStagedCreateReceiptPhase,
    ) -> Result<(), ConnectorError> {
        if receipt.owner() != &self.provider_binding_key
            || receipt.operation_id() != operation_id
            || receipt.phase() != phase
        {
            return Err(invalid("staged-create receipt does not match its request"));
        }
        Ok(())
    }

    fn validate_evidence_for(
        &self,
        evidence: &ExternalMutationEvidence,
        operation_id: ConnectorStagedCreateOperationId,
        operation_kind: &str,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor().instance_id != self.provider_binding_key.instance_id
            || evidence.incarnation() != self.provider_binding_key.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != operation_kind
        {
            return Err(invalid(
                "staged-create evidence does not match its lease generation",
            ));
        }
        Ok(())
    }
}

impl Drop for StagedCreateLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unanchored_provenance_digest(
    publication_id: LakePublicationId,
    target: &ConnectorTableIdentity,
    expected_absent: bool,
    staged_table_uuid: Option<[u8; 16]>,
    created_at_ms: i64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(UNANCHORED_CTAS_PROVENANCE_DOMAIN);
    hasher.update(publication_id.to_bytes());
    hasher.update(target.instance_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(target.namespace.as_bytes());
    hasher.update([0]);
    hasher.update(target.table.as_bytes());
    hasher.update([expected_absent as u8]);
    match staged_table_uuid {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value);
        }
        None => hasher.update([0]),
    }
    hasher.update(created_at_ms.to_be_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::connector::{CatalogVersion, ConnectorInstanceId, ConnectorProviderId};

    #[test]
    fn unanchored_ctas_cleanup_provenance_is_target_bound_and_tamper_evident() {
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let target = ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from("orders"),
        };
        let provenance = ConnectorCtasUnanchoredProvenance::try_new(
            LakePublicationId::new_v7(),
            target.clone(),
            true,
            Some([7; 16]),
            42,
        )
        .unwrap();
        let request = ConnectorCtasUnanchoredCleanupRequest::try_new(
            ConnectorProviderBindingKey {
                instance_id,
                incarnation: ProviderBindingEpoch::new(),
            },
            Arc::from("s3://warehouse/root"),
            100,
            provenance.clone(),
        );
        assert!(request.is_ok());

        let mut tampered = provenance;
        tampered.target = ConnectorTableIdentity {
            instance_id: target.instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("different"),
        };
        assert!(tampered.validate().is_err());
    }

    #[test]
    fn prepared_document_target_requires_dense_field_bindings() {
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let error = ConnectorPreparedCreateDocumentTarget::try_new(
            owner,
            CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([7; 32])),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            ConnectorTableObjectId::try_new(Bytes::from_static(b"table-object")).unwrap(),
            Bytes::from_static(b"schema-version"),
            Bytes::from_static(b"partition-spec-version"),
            vec![
                ConnectorPreparedCreateFieldBinding::try_new(
                    1,
                    Bytes::from_static(b"field-id"),
                    "field".to_string(),
                    "int".to_string(),
                    false,
                )
                .unwrap(),
            ],
            vec![],
            Bytes::from_static(b"provider-token"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn prepared_document_target_bounds_version_identities_independently() {
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let error = ConnectorPreparedCreateDocumentTarget::try_new(
            owner,
            CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([7; 32])),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            ConnectorTableObjectId::try_new(Bytes::from_static(b"table-object")).unwrap(),
            Bytes::from(vec![1; MAX_PREPARED_CREATE_VERSION_ID_BYTES + 1]),
            Bytes::from_static(b"partition-spec-version"),
            vec![
                ConnectorPreparedCreateFieldBinding::try_new(
                    0,
                    Bytes::from_static(b"field-id"),
                    "field".to_string(),
                    "int".to_string(),
                    false,
                )
                .unwrap(),
            ],
            vec![],
            Bytes::from_static(b"provider-token"),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn prepared_document_target_enforces_one_aggregate_carrier_limit() {
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let error = ConnectorPreparedCreateDocumentTarget::try_new(
            owner,
            CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([7; 32])),
            ConnectorMutationOperationId::new(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            ConnectorTableObjectId::try_new(Bytes::from_static(b"table-object")).unwrap(),
            Bytes::from_static(b"schema-version"),
            Bytes::from_static(b"partition-spec-version"),
            vec![
                ConnectorPreparedCreateFieldBinding::try_new(
                    0,
                    Bytes::from_static(b"field-id"),
                    "field".to_string(),
                    "int".to_string(),
                    false,
                )
                .unwrap(),
            ],
            vec![],
            Bytes::from(vec![7; MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn staged_handle_digest_covers_exact_schema_and_partition_facts() {
        let instance_id = ConnectorInstanceId::parse("ice").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance_id.clone(),
            incarnation: ProviderBindingEpoch::new(),
        };
        let operation_id = ConnectorMutationOperationId::new();
        let make_target =
            |schema: &'static [u8], spec: &'static [u8], transform: MvExactPartitionTransform| {
                ConnectorPreparedCreateDocumentTarget::try_new(
                    owner.clone(),
                    CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([7; 32])),
                    operation_id,
                    ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from("db"),
                        table: Arc::from("orders"),
                    },
                    ConnectorTableObjectId::try_new(Bytes::from_static(b"table-object")).unwrap(),
                    Bytes::from_static(schema),
                    Bytes::from_static(spec),
                    vec![
                        ConnectorPreparedCreateFieldBinding::try_new(
                            0,
                            Bytes::from_static(b"field-id"),
                            "field".to_string(),
                            "int".to_string(),
                            false,
                        )
                        .unwrap(),
                    ],
                    vec![
                        MvExactPartitionField::try_new(
                            Bytes::from_static(b"partition-id"),
                            Bytes::from_static(b"field-id"),
                            transform,
                        )
                        .unwrap(),
                    ],
                    Bytes::from_static(b"provider-token"),
                )
                .unwrap()
            };
        let baseline = ConnectorStagedTableHandle::try_new_document_managed(
            owner.clone(),
            operation_id,
            Bytes::from_static(b"handle"),
            make_target(b"schema-a", b"spec-a", MvExactPartitionTransform::Identity),
        )
        .unwrap();
        let schema_changed = ConnectorStagedTableHandle::try_new_document_managed(
            owner.clone(),
            operation_id,
            Bytes::from_static(b"handle"),
            make_target(b"schema-b", b"spec-a", MvExactPartitionTransform::Identity),
        )
        .unwrap();
        let spec_changed = ConnectorStagedTableHandle::try_new_document_managed(
            owner.clone(),
            operation_id,
            Bytes::from_static(b"handle"),
            make_target(b"schema-a", b"spec-b", MvExactPartitionTransform::Identity),
        )
        .unwrap();
        let transform_changed = ConnectorStagedTableHandle::try_new_document_managed(
            owner.clone(),
            operation_id,
            Bytes::from_static(b"handle"),
            make_target(b"schema-a", b"spec-a", MvExactPartitionTransform::Day),
        )
        .unwrap();

        assert_ne!(baseline.digest(), schema_changed.digest());
        assert_ne!(baseline.digest(), spec_changed.digest());
        assert_ne!(baseline.digest(), transform_changed.digest());
    }

    struct FakeCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        aborts: AtomicUsize,
        prepares: AtomicUsize,
        unknown_prepare: bool,
        fail_prepare: bool,
        malformed_prepare: bool,
        noop_publish: bool,
        conflict_publish: bool,
        unknown_abort: bool,
        binds: AtomicUsize,
        publishes: AtomicUsize,
    }

    impl FakeCapability {
        fn receipt(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
            phase: ConnectorStagedCreateReceiptPhase,
        ) -> ConnectorStagedCreateReceipt {
            ConnectorStagedCreateReceipt::try_new(
                ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.incarnation,
                },
                operation_id,
                phase,
                ExternalMutationEffect::Applied,
                Bytes::new(),
            )
            .unwrap()
        }

        fn evidence(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
        ) -> ExternalMutationEvidence {
            self.evidence_for(operation_id, "staged-create-prepare")
        }

        fn evidence_for(
            &self,
            operation_id: ConnectorStagedCreateOperationId,
            operation_kind: &str,
        ) -> ExternalMutationEvidence {
            ExternalMutationEvidence::try_new(
                1,
                self.descriptor.clone(),
                self.incarnation,
                operation_id,
                operation_kind,
                Bytes::from_static(b"unknown"),
            )
            .unwrap()
        }
    }

    impl ConnectorStagedCreate for FakeCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }
        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            if self.fail_prepare {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "prepare dispatch failed",
                ));
            }
            if self.unknown_prepare {
                return Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Unavailable,
                        "unknown",
                    ),
                    evidence: self.evidence(request.operation_id),
                });
            }
            let handle = ConnectorStagedTableHandle::try_new(
                request.owner,
                if self.malformed_prepare {
                    ConnectorMutationOperationId::new()
                } else {
                    request.operation_id
                },
                Bytes::from_static(b"provider-handle"),
            )?;
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared {
                handle,
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Prepared,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn publish(
            &self,
            request: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            self.publishes.fetch_add(1, Ordering::SeqCst);
            if self.conflict_publish {
                return Ok(ConnectorStagedCreatePublishOutcome::Conflict {
                    cleanup: ExternalMutationFinalization::Complete,
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::AlreadyExists,
                        "the staged-create target already exists",
                    ),
                });
            }
            if self.noop_publish {
                return Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                    receipt: self.receipt(
                        request.operation_id,
                        ConnectorStagedCreateReceiptPhase::Published,
                    ),
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            unreachable!("not used by these conformance tests")
        }
        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            let table = ConnectorTableHandle::try_new(
                request.handle.owner().instance_id.clone(),
                Bytes::new(),
            )?;
            ConnectorStagedWritePlanningBinding::try_new(
                &request.handle,
                table,
                Bytes::new(),
                request.context,
            )
        }
        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorStagedWriteProof,
        ) -> Result<(), ConnectorError> {
            self.binds.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn abort(
            &self,
            request: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            if self.unknown_abort {
                return Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Unavailable,
                        "unknown abort",
                    ),
                    evidence: self.evidence_for(request.operation_id, "staged-create-abort"),
                });
            }
            Ok(ConnectorStagedCreateAbortOutcome::Aborted {
                receipt: self.receipt(
                    request.operation_id,
                    ConnectorStagedCreateReceiptPhase::Aborted,
                ),
                finalization: ExternalMutationFinalization::Complete,
            })
        }
        fn adjudicate_publication(
            &self,
            request: ConnectorStagedCreatePublicationAdjudicationRequest,
        ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
            Ok(
                ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        super::super::ConnectorMutationFailureKind::Unavailable,
                        "publication remains unresolved",
                    ),
                    evidence: request.evidence,
                },
            )
        }
    }

    /// Shared operation table a poisoning fake re-enters to simulate a lease
    /// that observes its own in-flight state.
    type SharedOperations =
        Arc<Mutex<HashMap<ConnectorStagedCreateOperationId, LeaseOperationState>>>;

    struct PoisoningCapability {
        inner: FakeCapability,
        poison_on_plan_write: Mutex<Option<SharedOperations>>,
        poison_on_abort: Mutex<Option<SharedOperations>>,
    }

    impl ConnectorStagedCreate for PoisoningCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            self.inner.descriptor()
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.inner.incarnation()
        }

        fn prepare(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            self.inner.prepare(request)
        }

        fn publish(
            &self,
            request: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            self.inner.publish(request)
        }

        fn plan_write(
            &self,
            request: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            if let Some(operations) = self.poison_on_plan_write.lock().unwrap().take() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = operations.lock().unwrap();
                    panic!("poison staged-create lease during provider planning");
                }));
            }
            self.inner.plan_write(request)
        }

        fn bind_write(
            &self,
            handle: ConnectorStagedTableHandle,
            write: ConnectorStagedWriteProof,
        ) -> Result<(), ConnectorError> {
            self.inner.bind_write(handle, write)
        }

        fn abort(
            &self,
            request: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            if let Some(operations) = self.poison_on_abort.lock().unwrap().take() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = operations.lock().unwrap();
                    panic!("poison staged-create lease during provider abort");
                }));
            }
            self.inner.abort(request)
        }

        fn adjudicate_publication(
            &self,
            request: ConnectorStagedCreatePublicationAdjudicationRequest,
        ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
            self.inner.adjudicate_publication(request)
        }
    }

    fn owner() -> (ConnectorInstanceDescriptor, ProviderBindingEpoch) {
        (
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("rest").unwrap(),
            },
            ProviderBindingEpoch::new(),
        )
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            crate::connector::ConnectorStopOwner::new().view(),
            super::super::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            super::super::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    fn prepare_request(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorStagedCreateOperationId,
    ) -> ConnectorStagedCreatePrepareRequest {
        ConnectorStagedCreatePrepareRequest {
            table: ConnectorTableIdentity {
                instance_id: owner.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            owner,
            publication_id: LakePublicationId::try_from_bytes(operation_id.to_bytes())
                .expect("staged create operation ID must be UUIDv7"),
            operation_id,
            columns: Vec::new(),
            partitioning: Vec::new(),
            properties: BTreeMap::new(),
            policy: CreatePolicy::FailIfExists,
            mode: ConnectorStagedCreateMode::Ordinary,
            context: context(),
        }
    }

    /// A sealed write, as a session would hand it over: one receipt and the
    /// rows behind it.
    fn write_proof(marker: u8) -> ConnectorStagedWriteProof {
        ConnectorStagedWriteProof::try_new(
            super::super::ConnectorWriteReceipt::try_new(Bytes::from(vec![marker; 8]))
                .expect("receipt"),
            u64::from(marker),
        )
        .expect("write proof")
    }

    #[test]
    fn exact_staged_writer_plan_can_recover_unknown_staging_without_catalog_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let binding = lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: handle.clone(),
                context: context(),
            })
            .unwrap();
        assert_eq!(binding.target_handle_digest(), handle.digest());
        let write = write_proof(3);
        lease.mark_write_unknown(&handle).unwrap();
        lease
            .reconcile_write_proof(handle.clone(), write.clone())
            .unwrap();
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                write: Some(write),
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.binds.load(Ordering::SeqCst), 1);
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 0);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn handle_is_exact_instance_bound_and_abort_is_unpublished_only() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared")
        };
        let mut foreign_owner = lease.owner().clone();
        foreign_owner.incarnation = ProviderBindingEpoch::new();
        let foreign = ConnectorStagedTableHandle::try_new(
            foreign_owner,
            operation_id,
            handle.provider_payload().clone(),
        )
        .unwrap();
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: foreign,
                    write: None,
                    context: context(),
                })
                .is_err()
        );
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                write: None,
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle,
                    write: None,
                    context: context(),
                })
                .is_err()
        );
    }

    #[test]
    fn unknown_prepare_forbids_abort_without_provider_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: true,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let outcome = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }
        ));
        let forged = ConnectorStagedTableHandle::try_new(
            lease.owner().clone(),
            operation_id,
            Bytes::from_static(b"provider-handle"),
        )
        .unwrap();
        assert!(
            lease
                .abort(ConnectorStagedCreateAbortRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: forged,
                    write: None,
                    context: context(),
                })
                .is_err()
        );
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn duplicate_operation_id_is_rejected_before_provider_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn prepare_rejects_a_child_operation_identity_before_provider_dispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorStagedCreateOperationId::new();
        let mut request = prepare_request(lease.owner().clone(), operation_id);
        request.publication_id = LakePublicationId::new_v7();

        let error = lease.prepare(request).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "staged-create operation ID must equal its statement publication ID"
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepare_dispatch_error_locks_reservation_without_redispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: true,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn malformed_prepare_outcome_locks_reservation_without_redispatch() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: true,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert!(
            lease
                .prepare(prepare_request(lease.owner().clone(), operation_id))
                .is_err()
        );
        assert_eq!(capability.prepares.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_write_bound_to_one_target_cannot_publish_another_target() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle: first, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected first prepared target")
        };
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle: second, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected second prepared target")
        };
        let write = write_proof(5);
        lease.bind_write(first, write.clone()).unwrap();
        assert!(
            lease
                .publish(ConnectorStagedCreatePublishRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    handle: second,
                    write,
                    payload: ConnectorStagedCreatePublicationPayload::Ordinary,
                    context: context(),
                })
                .is_err()
        );
        assert_eq!(capability.binds.load(Ordering::SeqCst), 1);
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn publish_noop_retains_exact_target_for_explicit_abort() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: true,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let write = write_proof(6);
        lease.bind_write(handle.clone(), write.clone()).unwrap();
        let outcome = lease
            .publish(ConnectorStagedCreatePublishRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                write: write.clone(),
                payload: ConnectorStagedCreatePublicationPayload::Ordinary,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePublishOutcome::NoOp { .. }
        ));
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                write: Some(write),
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn post_dispatch_recording_recovers_poisoned_lease_state() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability,
            || {},
        )
        .unwrap();
        let operations = Arc::clone(&lease.operations);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = operations.lock().unwrap();
            panic!("poison staged-create lease state");
        }));
        let operation_id = ConnectorMutationOperationId::new();
        lease.record_after_dispatch(operation_id, Some(LeaseOperationState::Published));
        let operations = lease
            .operations
            .lock()
            .expect("post-dispatch recording clears poison");
        assert!(matches!(
            operations.get(&operation_id),
            Some(LeaseOperationState::Published)
        ));
    }

    #[test]
    fn abort_dispatch_records_authoritative_outcome_and_clears_poison() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(PoisoningCapability {
            inner: FakeCapability {
                descriptor: descriptor.clone(),
                incarnation,
                aborts: AtomicUsize::new(0),
                prepares: AtomicUsize::new(0),
                unknown_prepare: false,
                fail_prepare: false,
                malformed_prepare: false,
                noop_publish: false,
                conflict_publish: false,
                unknown_abort: false,
                binds: AtomicUsize::new(0),
                publishes: AtomicUsize::new(0),
            },
            poison_on_plan_write: Mutex::new(None),
            poison_on_abort: Mutex::new(None),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        *capability.poison_on_abort.lock().unwrap() = Some(Arc::clone(&lease.operations));
        let outcome = lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                write: None,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreateAbortOutcome::Aborted { .. }
        ));
        assert_eq!(capability.inner.aborts.load(Ordering::SeqCst), 1);
        let operations = lease
            .operations
            .lock()
            .expect("authoritative abort clears lease poison");
        assert!(matches!(
            operations.get(&operation_id),
            Some(LeaseOperationState::Aborted)
        ));
    }

    #[test]
    fn plan_write_dispatch_clears_poison_before_bind() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(PoisoningCapability {
            inner: FakeCapability {
                descriptor: descriptor.clone(),
                incarnation,
                aborts: AtomicUsize::new(0),
                prepares: AtomicUsize::new(0),
                unknown_prepare: false,
                fail_prepare: false,
                malformed_prepare: false,
                noop_publish: false,
                conflict_publish: false,
                unknown_abort: false,
                binds: AtomicUsize::new(0),
                publishes: AtomicUsize::new(0),
            },
            poison_on_plan_write: Mutex::new(None),
            poison_on_abort: Mutex::new(None),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let operation_id = ConnectorMutationOperationId::new();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(lease.owner().clone(), operation_id))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        *capability.poison_on_plan_write.lock().unwrap() = Some(Arc::clone(&lease.operations));
        lease
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle: handle.clone(),
                context: context(),
            })
            .unwrap();
        lease.bind_write(handle, write_proof(4)).unwrap();
        assert_eq!(capability.inner.binds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn publication_adjudication_consumes_exact_unknown_once() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: false,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let target_operation_id = ConnectorMutationOperationId::new();
        let dispatch_operation_id = ConnectorMutationOperationId::new();
        let evidence = capability.evidence_for(dispatch_operation_id, "staged-create-publish");
        lease.operations.lock().unwrap().insert(
            target_operation_id,
            LeaseOperationState::PublicationUnknown {
                evidence_digest: evidence.digest(),
                handle_digest: [7; 32],
                bound_write: BoundWriteProof {
                    receipt_digest: [8; 32],
                    row_count: 9,
                },
            },
        );

        let wrong_evidence =
            capability.evidence_for(dispatch_operation_id, "staged-create-prepare");
        assert!(
            lease
                .adjudicate_publication(ConnectorStagedCreatePublicationAdjudicationRequest {
                    target_operation_id,
                    evidence: wrong_evidence,
                    context: context(),
                })
                .is_err()
        );

        let outcome = lease
            .adjudicate_publication(ConnectorStagedCreatePublicationAdjudicationRequest {
                target_operation_id,
                evidence,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePublicationAdjudicationOutcome::CommitUnknown { .. }
        ));
        assert!(
            lease
                .adjudicate_publication(ConnectorStagedCreatePublicationAdjudicationRequest {
                    target_operation_id,
                    evidence: capability
                        .evidence_for(dispatch_operation_id, "staged-create-publish"),
                    context: context(),
                })
                .is_err()
        );
    }

    /// The absent-target fence, seen from the lease.
    ///
    /// A target that already exists is a refusal, not a publication: the write
    /// stays bound, the target stays unpublished, and the statement can still
    /// abort it to clean up what it staged. Reporting this as published — or as
    /// unknown — would either claim a table this statement did not create or
    /// strand the objects it wrote.
    #[test]
    fn a_publication_onto_an_existing_target_is_refused_and_stays_abortable() {
        let (descriptor, incarnation) = owner();
        let capability = Arc::new(FakeCapability {
            descriptor: descriptor.clone(),
            incarnation,
            aborts: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            unknown_prepare: false,
            fail_prepare: false,
            malformed_prepare: false,
            noop_publish: false,
            conflict_publish: true,
            unknown_abort: false,
            binds: AtomicUsize::new(0),
            publishes: AtomicUsize::new(0),
        });
        let lease = ConnectorStagedCreateLease::new(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            capability.clone(),
            || {},
        )
        .unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = lease
            .prepare(prepare_request(
                lease.owner().clone(),
                ConnectorMutationOperationId::new(),
            ))
            .unwrap()
        else {
            panic!("expected prepared target")
        };
        let write = write_proof(7);
        lease.bind_write(handle.clone(), write.clone()).unwrap();

        let outcome = lease
            .publish(ConnectorStagedCreatePublishRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle: handle.clone(),
                write: write.clone(),
                payload: ConnectorStagedCreatePublicationPayload::Ordinary,
                context: context(),
            })
            .unwrap();
        assert!(matches!(
            outcome,
            ConnectorStagedCreatePublishOutcome::Conflict { .. }
        ));

        // Still unpublished, so the exact bound write can still be aborted.
        lease
            .abort(ConnectorStagedCreateAbortRequest {
                operation_id: ConnectorMutationOperationId::new(),
                handle,
                write: Some(write),
                context: context(),
            })
            .unwrap();
        assert_eq!(capability.publishes.load(Ordering::SeqCst), 1);
        assert_eq!(capability.aborts.load(Ordering::SeqCst), 1);
    }
}
