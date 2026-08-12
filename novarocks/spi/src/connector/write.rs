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

//! Provider-neutral distributed writer contract.
//! Design: ADR-0048 (docs/adr/ADR-0048-connector-write-admission-and-terminal-facts.md)
//!
//! The frontend owns planning and external commit state. Backend execution
//! bindings can only stage Arrow batches and return bounded opaque reports.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorExecutionDistribution, ConnectorMutationFailure,
    ConnectorRequestContext, ConnectorTableHandle, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};

pub const CONNECTOR_WRITE_CONTRACT_VERSION: u32 = 1;
pub const MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECTOR_STAGED_REPORT_PARTS: u32 = 48;
pub const MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES: usize =
    MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES * MAX_CONNECTOR_STAGED_REPORT_PARTS as usize;
pub const MAX_CONNECTOR_WRITE_RECEIPT_BYTES: usize = MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES;
pub const MAX_CONNECTOR_WRITE_COHORTS: usize = 4096;
pub const MAX_CONNECTOR_WRITE_OPERATION_WRITERS: usize = 16_384;
pub const MAX_CONNECTOR_WRITE_OPERATION_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_CONNECTOR_WRITE_ACTIVATIONS: usize = 16_384;
pub const MAX_CONNECTOR_MANAGED_PUBLICATION_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS: usize = 4096;
pub const MAX_CONNECTOR_MANAGED_PARTITION_FIELD_TEXT_BYTES: usize = 4096;

const CONNECTOR_WRITE_COHORT_ID_DOMAIN: &[u8] = b"novarocks.connector-write-cohort.v1\0";
const CONNECTOR_WRITE_COHORT_SET_DOMAIN: &[u8] = b"novarocks.connector-write-cohort-set.v1\0";
const CONNECTOR_WRITE_ATTEMPT_DOMAIN: &[u8] = b"novarocks.connector-write-attempt.v1\0";
const CONNECTOR_WRITE_OPERATION_DOMAIN: &[u8] = b"novarocks.connector-write-operation.v1\0";
const CONNECTOR_MANAGED_PARTITION_SPEC_OBSERVATION_DOMAIN: &[u8] =
    b"novarocks.connector-managed-partition-spec-observation.v1\0";
const CONNECTOR_MANAGED_PARTITION_SPEC_REPLACEMENT_ID_DOMAIN: &[u8] =
    b"novarocks.connector-managed-partition-spec-replacement-id.v1\0";
const CONNECTOR_MANAGED_PARTITION_SPEC_REPLACEMENT_DOMAIN: &[u8] =
    b"novarocks.connector-managed-partition-spec-replacement.v1\0";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteOperationId(Uuid);

impl ConnectorWriteOperationId {
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

impl std::fmt::Display for ConnectorWriteOperationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for ConnectorWriteOperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

impl Default for ConnectorWriteOperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteCohortId([u8; 32]);

impl ConnectorWriteCohortId {
    pub fn derive(
        operation_id: ConnectorWriteOperationId,
        role_tag: &[u8],
        semantic_key_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        if role_tag.is_empty() || role_tag.len() > 256 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort role tag must contain 1..=256 bytes",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_WRITE_COHORT_ID_DOMAIN);
        hasher.update(operation_id.to_bytes());
        digest_bytes(&mut hasher, role_tag);
        hasher.update(semantic_key_digest);
        Ok(Self(hasher.finalize().into()))
    }

    pub fn primary(operation_id: ConnectorWriteOperationId) -> Self {
        let semantic_key_digest: [u8; 32] = Sha256::digest(b"primary").into();
        Self::derive(operation_id, b"primary", semantic_key_digest)
            .expect("the fixed primary cohort role is valid")
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteExecutionId {
    query_id: [u8; 16],
    attempt_id: u64,
}

impl ConnectorWriteExecutionId {
    pub const fn new(query_id: [u8; 16], attempt_id: u64) -> Self {
        Self {
            query_id,
            attempt_id,
        }
    }

    pub const fn query_id(self) -> [u8; 16] {
        self.query_id
    }

    pub const fn attempt_id(self) -> u64 {
        self.attempt_id
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriterIdentity {
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    fragment_instance_id: [u8; 16],
    fragment_id: i32,
    backend_num: i32,
    sink_ordinal: u32,
    binding_key: ConnectorExecutionBindingKey,
}

impl ConnectorWriterIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        fragment_instance_id: [u8; 16],
        fragment_id: i32,
        backend_num: i32,
        sink_ordinal: u32,
        binding_key: ConnectorExecutionBindingKey,
    ) -> Self {
        Self {
            operation_id,
            cohort_id,
            execution_id,
            fragment_instance_id,
            fragment_id,
            backend_num,
            sink_ordinal,
            binding_key,
        }
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub const fn fragment_instance_id(&self) -> [u8; 16] {
        self.fragment_instance_id
    }

    pub const fn fragment_id(&self) -> i32 {
        self.fragment_id
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub const fn sink_ordinal(&self) -> u32 {
        self.sink_ordinal
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorWriteIntent {
    Append,
    Overwrite,
    PartitionOverwrite,
    RowDelta,
}

/// The application semantic purpose presented to Provider write admission.
/// It is separate from the physical write intent so a managed target can deny
/// ordinary DML without granting a generic bypass to callers.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorWriteAdmissionPurpose {
    OrdinaryDml,
    MaterializedViewRefresh,
}

/// A provider-issued, preparation-local field identity. It is intentionally
/// neither a catalog field ID nor a table-format source ID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorWriteFieldToken([u8; 32]);

impl ConnectorWriteFieldToken {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteFieldRequest {
    field: Field,
}

impl ConnectorWriteFieldRequest {
    pub fn new(field: Field) -> Self {
        Self { field }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteFieldBinding {
    token: ConnectorWriteFieldToken,
    field: Field,
}

impl ConnectorWriteFieldBinding {
    pub fn new(token: ConnectorWriteFieldToken, field: Field) -> Self {
        Self { token, field }
    }

    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

/// SQL-owned input requirements submitted to the Provider during admission.
/// Each variant contains the entire required set so callers cannot represent a
/// row-delete mode with a missing descriptor or a conflicting optional field.
#[derive(Clone, Debug)]
pub enum ConnectorWriteInputRequest {
    Data {
        fields: Vec<ConnectorWriteFieldRequest>,
    },
    RowLineage {
        data_fields: Vec<ConnectorWriteFieldRequest>,
        row_identity_fields: Vec<ConnectorWriteFieldRequest>,
    },
    PositionDelete {
        identity_fields: Vec<ConnectorWriteFieldRequest>,
        partition_source_fields: Vec<ConnectorWriteFieldRequest>,
    },
    DeletionVector {
        identity_fields: Vec<ConnectorWriteFieldRequest>,
        partition_source_fields: Vec<ConnectorWriteFieldRequest>,
    },
    EqualityDelete {
        equality_fields: Vec<ConnectorWriteFieldRequest>,
    },
}

/// Provider-signed counterpart to [`ConnectorWriteInputRequest`].
#[derive(Clone, Debug)]
pub enum ConnectorWriteInputShape {
    Data {
        fields: Vec<ConnectorWriteFieldBinding>,
    },
    RowLineage {
        data_fields: Vec<ConnectorWriteFieldBinding>,
        row_identity_fields: Vec<ConnectorWriteFieldBinding>,
    },
    PositionDelete {
        identity_fields: Vec<ConnectorWriteFieldBinding>,
        partition_source_fields: Vec<ConnectorWriteFieldBinding>,
    },
    DeletionVector {
        identity_fields: Vec<ConnectorWriteFieldBinding>,
        partition_source_fields: Vec<ConnectorWriteFieldBinding>,
    },
    EqualityDelete {
        equality_fields: Vec<ConnectorWriteFieldBinding>,
    },
}

impl ConnectorWriteInputShape {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let mut tokens = HashSet::new();
        let mut names = HashSet::new();
        let fields: Vec<&ConnectorWriteFieldBinding> = match self {
            Self::Data { fields } => fields.iter().collect(),
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => data_fields.iter().chain(row_identity_fields).collect(),
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => identity_fields
                .iter()
                .chain(partition_source_fields)
                .collect(),
            Self::EqualityDelete { equality_fields } => equality_fields.iter().collect(),
        };
        if fields.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write input shape must contain at least one field",
            ));
        }
        for binding in fields {
            if !tokens.insert(binding.token) || !names.insert(binding.field.name().to_owned()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                ));
            }
        }
        Ok(())
    }

    pub fn fields(&self) -> Vec<&ConnectorWriteFieldBinding> {
        match self {
            Self::Data { fields } => fields.iter().collect(),
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => data_fields.iter().chain(row_identity_fields).collect(),
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => identity_fields
                .iter()
                .chain(partition_source_fields)
                .collect(),
            Self::EqualityDelete { equality_fields } => equality_fields.iter().collect(),
        }
    }
}

/// Opaque provider version captured during write admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteBaseVersion {
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorWriteBaseVersion {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        validate_handle_payload(&payload)?;
        Ok(Self {
            digest: sha256(&payload),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_handle_payload(&self.payload)?;
        if self.digest != sha256(&self.payload) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write base version digest does not match its payload",
            ));
        }
        Ok(())
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone)]
pub struct ConnectorWritePreparationRequest {
    pub table: ConnectorTableHandle,
    pub target_ref: ConnectorWriteTargetRef,
    pub intent: ConnectorWriteIntent,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub input: ConnectorWriteInputRequest,
    pub context: ConnectorRequestContext,
}

impl ConnectorWritePreparationRequest {
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        if self.table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write preparation table does not match the exact control owner",
            ));
        }
        self.target_ref.validate()?;
        validate_input_request(&self.input)
    }
}

/// SQL-visible write reference selected before Provider admission.
///
/// The name itself is semantic input (for example, an Iceberg branch); the
/// Provider remains the sole owner of resolving it to an external version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteTargetRef(Arc<str>);

impl ConnectorWriteTargetRef {
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self, ConnectorError> {
        let value = value.into();
        let target = Self(value);
        target.validate()?;
        Ok(target)
    }

    pub fn main() -> Self {
        Self(Arc::from("main"))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.0.is_empty() || self.0.len() > 256 || self.0.chars().any(char::is_control) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write target ref must contain 1..=256 non-control bytes",
            ));
        }
        Ok(())
    }
}

/// A sealed Provider admission result. The handle and base version are opaque
/// to application callers; only the provider may interpret them.
#[derive(Clone)]
pub struct ConnectorWritePreparation {
    owner: ConnectorExecutionBindingKey,
    table: ConnectorTableHandle,
    target_ref: ConnectorWriteTargetRef,
    intent: ConnectorWriteIntent,
    base_version: ConnectorWriteBaseVersion,
    input: ConnectorWriteInputShape,
    payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorWritePreparation {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        table: ConnectorTableHandle,
        target_ref: ConnectorWriteTargetRef,
        intent: ConnectorWriteIntent,
        base_version: ConnectorWriteBaseVersion,
        input: ConnectorWriteInputShape,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write preparation table does not match its owner",
            ));
        }
        target_ref.validate()?;
        base_version.validate()?;
        input.validate()?;
        validate_handle_payload(&payload)?;
        let digest = preparation_digest(
            &owner,
            &table,
            &target_ref,
            intent,
            &base_version,
            &input,
            &payload,
        );
        Ok(Self {
            owner,
            table,
            target_ref,
            intent,
            base_version,
            input,
            payload,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.owner.clone(),
            self.table.clone(),
            self.target_ref.clone(),
            self.intent,
            self.base_version.clone(),
            self.input.clone(),
            self.payload.clone(),
        )?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write preparation digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }
    pub fn target_ref(&self) -> &ConnectorWriteTargetRef {
        &self.target_ref
    }
    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }
    pub fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub fn base_version(&self) -> &ConnectorWriteBaseVersion {
        &self.base_version
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

// Boxing the prepared variant would only hide the cost behind a pointer on a
// control-plane value built once per write admission, and would change a frozen
// SPI shape that providers and Core both match on.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ConnectorWritePreparationOutcome {
    Prepared(ConnectorWritePreparation),
    Denied(ConnectorError),
}

/// The signed admission evidence consumed by the exact-generation activation
/// transition.  The tagged form prevents callers from smuggling a row plan
/// through the ordinary preparation path.
// The tagged shape is the contract: see the doc comment above. Boxing a variant
// to equalize sizes would obscure it for no runtime benefit on a per-admission
// value.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum ConnectorWriteActivationSource {
    Prepared(ConnectorWritePreparation),
    RowMutation(super::ConnectorRowMutationExecutionPlan),
}

impl ConnectorWriteActivationSource {
    fn validate(
        &self,
        owner: &ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<[u8; 32], ConnectorError> {
        match self {
            Self::Prepared(preparation) => {
                preparation.validate()?;
                if preparation.owner() != owner {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector write activation preparation does not match the exact control owner",
                    ));
                }
                Ok(preparation.digest())
            }
            Self::RowMutation(plan) => {
                plan.validate()?;
                if plan.owner() != owner || plan.operation_id() != operation_id {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector row-mutation activation does not match the exact operation owner",
                    ));
                }
                Ok(plan.digest())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorManagedPublicationTechnique {
    Full,
    Incremental,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorManagedPublicationEmptyInputDisposition {
    AbortWithoutExternalCommit,
    CommitEmptyWrite,
}

/// The closed, provider-neutral transform vocabulary for an atomic managed
/// partition-spec replacement. Providers remain responsible for validating
/// each transform against the retained exact table schema.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorManagedPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { buckets: u32 },
    Truncate { width: u32 },
    Void,
}

impl ConnectorManagedPartitionTransform {
    fn validate(self) -> Result<(), ConnectorError> {
        let parameter = match self {
            Self::Bucket { buckets } => Some(buckets),
            Self::Truncate { width } => Some(width),
            Self::Identity | Self::Year | Self::Month | Self::Day | Self::Hour | Self::Void => None,
        };
        if parameter.is_some_and(|value| value == 0 || value > i32::MAX as u32) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed partition transform parameter must be within 1..=i32::MAX",
            ));
        }
        Ok(())
    }

    fn digest_into(self, hasher: &mut Sha256) {
        match self {
            Self::Identity => hasher.update([1]),
            Self::Year => hasher.update([2]),
            Self::Month => hasher.update([3]),
            Self::Day => hasher.update([4]),
            Self::Hour => hasher.update([5]),
            Self::Bucket { buckets } => {
                hasher.update([6]);
                hasher.update(buckets.to_be_bytes());
            }
            Self::Truncate { width } => {
                hasher.update([7]);
                hasher.update(width.to_be_bytes());
            }
            Self::Void => hasher.update([8]),
        }
    }
}

/// One ordered field in a complete managed partition-spec replacement.
/// `source_field_id` is the stable application-visible source identity; the
/// provider assigns any physical partition field identity and new spec ID.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorManagedPartitionField {
    source_field_id: i32,
    position: u32,
    transform: ConnectorManagedPartitionTransform,
}

impl ConnectorManagedPartitionField {
    pub fn try_new(
        source_field_id: i32,
        position: u32,
        transform: ConnectorManagedPartitionTransform,
    ) -> Result<Self, ConnectorError> {
        if source_field_id <= 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed partition source field ID must be positive",
            ));
        }
        transform.validate()?;
        Ok(Self {
            source_field_id,
            position,
            transform,
        })
    }

    pub const fn source_field_id(&self) -> i32 {
        self.source_field_id
    }

    pub const fn position(&self) -> u32 {
        self.position
    }

    pub const fn transform(&self) -> ConnectorManagedPartitionTransform {
        self.transform
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        hasher.update(self.source_field_id.to_be_bytes());
        hasher.update(self.position.to_be_bytes());
        self.transform.digest_into(hasher);
    }
}

fn validate_managed_partition_fields(
    fields: &[ConnectorManagedPartitionField],
    allow_empty: bool,
) -> Result<(), ConnectorError> {
    if (!allow_empty && fields.is_empty())
        || fields.len() > MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector managed partition spec field count is invalid or exceeds its bound",
        ));
    }
    let mut field_transforms = BTreeSet::new();
    for (position, field) in fields.iter().enumerate() {
        if field.source_field_id <= 0
            || field.position as usize != position
            || field.transform.validate().is_err()
            || !field_transforms.insert((field.source_field_id, field.transform))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed partition spec has an invalid order, duplicate field transform, or transform",
            ));
        }
    }
    Ok(())
}

/// Canonical observation of the exact prior default partition spec. This is
/// logical, bounded evidence rather than a provider metadata location or a
/// caller-selected physical spec ID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorManagedPartitionSpecObservation {
    provider_spec_id: i32,
    layout_digest: [u8; 32],
}

impl ConnectorManagedPartitionSpecObservation {
    pub fn try_from_fields(
        provider_spec_id: i32,
        fields: &[ConnectorManagedPartitionField],
    ) -> Result<Self, ConnectorError> {
        if provider_spec_id < 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed prior partition spec ID must be non-negative",
            ));
        }
        validate_managed_partition_fields(fields, true)?;
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_MANAGED_PARTITION_SPEC_OBSERVATION_DOMAIN);
        hasher.update(provider_spec_id.to_be_bytes());
        hasher.update((fields.len() as u32).to_be_bytes());
        for field in fields {
            field.digest_into(&mut hasher);
        }
        Ok(Self {
            provider_spec_id,
            layout_digest: hasher.finalize().into(),
        })
    }

    pub const fn provider_spec_id(self) -> i32 {
        self.provider_spec_id
    }

    pub const fn layout_digest(self) -> [u8; 32] {
        self.layout_digest
    }
}

/// A deterministic identity proving that a replacement belongs to one write
/// operation. There is at most one replacement in a managed publication
/// intent, so no caller-provided identity or new physical spec ID is needed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorManagedPartitionSpecReplacementId([u8; 32]);

impl ConnectorManagedPartitionSpecReplacementId {
    pub fn derive(operation_id: ConnectorWriteOperationId) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_MANAGED_PARTITION_SPEC_REPLACEMENT_ID_DOMAIN);
        hasher.update(operation_id.to_bytes());
        Self(hasher.finalize().into())
    }

    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// The metadata transition is coupled to publication of the managed target,
/// never to creation or population of an internal staging reference.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorManagedPartitionSpecReplacementTarget {
    MainPublication,
}

/// Complete, signed logical facts for replacing the default partition spec in
/// the same external commit that publishes the managed snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorManagedPartitionSpecReplacement {
    operation_id: ConnectorWriteOperationId,
    replacement_id: ConnectorManagedPartitionSpecReplacementId,
    target: ConnectorManagedPartitionSpecReplacementTarget,
    expected_prior_default: ConnectorManagedPartitionSpecObservation,
    fields: Vec<ConnectorManagedPartitionField>,
}

impl ConnectorManagedPartitionSpecReplacement {
    pub fn try_new(
        operation_id: ConnectorWriteOperationId,
        expected_prior_default: ConnectorManagedPartitionSpecObservation,
        fields: Vec<ConnectorManagedPartitionField>,
    ) -> Result<Self, ConnectorError> {
        validate_managed_partition_fields(&fields, false)?;
        Ok(Self {
            operation_id,
            replacement_id: ConnectorManagedPartitionSpecReplacementId::derive(operation_id),
            target: ConnectorManagedPartitionSpecReplacementTarget::MainPublication,
            expected_prior_default,
            fields,
        })
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn replacement_id(&self) -> ConnectorManagedPartitionSpecReplacementId {
        self.replacement_id
    }

    pub const fn target(&self) -> ConnectorManagedPartitionSpecReplacementTarget {
        self.target
    }

    pub const fn expected_prior_default(&self) -> ConnectorManagedPartitionSpecObservation {
        self.expected_prior_default
    }

    pub fn fields(&self) -> &[ConnectorManagedPartitionField] {
        &self.fields
    }

    fn validate_for_operation(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        validate_managed_partition_fields(&self.fields, false)?;
        if self.operation_id != operation_id
            || self.replacement_id
                != ConnectorManagedPartitionSpecReplacementId::derive(operation_id)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed partition replacement does not match the write operation",
            ));
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(CONNECTOR_MANAGED_PARTITION_SPEC_REPLACEMENT_DOMAIN);
        hasher.update(self.operation_id.to_bytes());
        hasher.update(self.replacement_id.to_bytes());
        hasher.update([match self.target {
            ConnectorManagedPartitionSpecReplacementTarget::MainPublication => 1,
        }]);
        hasher.update(self.expected_prior_default.provider_spec_id().to_be_bytes());
        hasher.update(self.expected_prior_default.layout_digest());
        hasher.update((self.fields.len() as u32).to_be_bytes());
        for field in &self.fields {
            field.digest_into(&mut hasher);
        }
        hasher.finalize().into()
    }
}

/// One exact provider-assigned field in committed partitioning. Both physical
/// and source identities are application facts required to finalize or recover
/// the durable MV partition contract without interpreting provider metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCommittedPartitionField {
    partition_field_id: i32,
    partition_field_name: Arc<str>,
    source_field_id: i32,
    source_column_name: Arc<str>,
    position: u32,
    transform: ConnectorManagedPartitionTransform,
}

impl ConnectorCommittedPartitionField {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_field_id: i32,
        partition_field_name: impl Into<Arc<str>>,
        source_field_id: i32,
        source_column_name: impl Into<Arc<str>>,
        position: u32,
        transform: ConnectorManagedPartitionTransform,
    ) -> Result<Self, ConnectorError> {
        let partition_field_name = partition_field_name.into();
        let source_column_name = source_column_name.into();
        if partition_field_id <= 0
            || source_field_id <= 0
            || partition_field_name.is_empty()
            || source_column_name.is_empty()
            || partition_field_name.len() + source_column_name.len()
                > MAX_CONNECTOR_MANAGED_PARTITION_FIELD_TEXT_BYTES
            || partition_field_name.chars().any(char::is_control)
            || source_column_name.chars().any(char::is_control)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector committed partition field identity is invalid or exceeds its bound",
            ));
        }
        transform.validate()?;
        Ok(Self {
            partition_field_id,
            partition_field_name,
            source_field_id,
            source_column_name,
            position,
            transform,
        })
    }

    pub const fn partition_field_id(&self) -> i32 {
        self.partition_field_id
    }
    pub fn partition_field_name(&self) -> &str {
        self.partition_field_name.as_ref()
    }
    pub const fn source_field_id(&self) -> i32 {
        self.source_field_id
    }
    pub fn source_column_name(&self) -> &str {
        self.source_column_name.as_ref()
    }
    pub const fn position(&self) -> u32 {
        self.position
    }
    pub const fn transform(&self) -> ConnectorManagedPartitionTransform {
        self.transform
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        hasher.update(self.partition_field_id.to_be_bytes());
        digest_bytes(hasher, self.partition_field_name.as_bytes());
        hasher.update(self.source_field_id.to_be_bytes());
        digest_bytes(hasher, self.source_column_name.as_bytes());
        hasher.update(self.position.to_be_bytes());
        self.transform.digest_into(hasher);
    }
}

fn validate_committed_partition_fields(
    fields: &[ConnectorCommittedPartitionField],
) -> Result<(), ConnectorError> {
    if fields.is_empty() || fields.len() > MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector committed partition field count is invalid or exceeds its bound",
        ));
    }
    let mut partition_field_ids = BTreeSet::new();
    let mut partition_field_names = BTreeSet::new();
    let mut field_transforms = BTreeSet::new();
    for (position, field) in fields.iter().enumerate() {
        ConnectorCommittedPartitionField::try_new(
            field.partition_field_id,
            field.partition_field_name.clone(),
            field.source_field_id,
            field.source_column_name.clone(),
            field.position,
            field.transform,
        )?;
        if field.position as usize != position
            || !partition_field_ids.insert(field.partition_field_id)
            || !partition_field_names.insert(field.partition_field_name.as_ref())
            || !field_transforms.insert((field.source_field_id, field.transform))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector committed partitioning has an invalid order or duplicate field identity",
            ));
        }
    }
    Ok(())
}

/// Exact partitioning facts returned after an atomic managed publication.
/// The physical spec and partition field IDs are provider-assigned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCommittedPartitioning {
    spec_id: i32,
    fields: Vec<ConnectorCommittedPartitionField>,
    digest: [u8; 32],
}

impl ConnectorCommittedPartitioning {
    pub fn try_new(
        spec_id: i32,
        fields: Vec<ConnectorCommittedPartitionField>,
    ) -> Result<Self, ConnectorError> {
        if spec_id < 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector committed partition spec ID must be non-negative",
            ));
        }
        validate_committed_partition_fields(&fields)?;
        let digest = committed_partitioning_digest(spec_id, &fields);
        Ok(Self {
            spec_id,
            fields,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_committed_partition_fields(&self.fields)?;
        if self.spec_id < 0
            || self.digest != committed_partitioning_digest(self.spec_id, &self.fields)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector committed partitioning digest does not match its contents",
            ));
        }
        Ok(())
    }

    pub const fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub fn fields(&self) -> &[ConnectorCommittedPartitionField] {
        &self.fields
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

fn committed_partitioning_digest(
    spec_id: i32,
    fields: &[ConnectorCommittedPartitionField],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-committed-partitioning.v1\0");
    hasher.update(spec_id.to_be_bytes());
    hasher.update((fields.len() as u32).to_be_bytes());
    for field in fields {
        field.digest_into(&mut hasher);
    }
    hasher.finalize().into()
}

fn managed_partition_transform_wire(transform: ConnectorManagedPartitionTransform) -> (u8, u32) {
    match transform {
        ConnectorManagedPartitionTransform::Identity => (1, 0),
        ConnectorManagedPartitionTransform::Year => (2, 0),
        ConnectorManagedPartitionTransform::Month => (3, 0),
        ConnectorManagedPartitionTransform::Day => (4, 0),
        ConnectorManagedPartitionTransform::Hour => (5, 0),
        ConnectorManagedPartitionTransform::Bucket { buckets } => (6, buckets),
        ConnectorManagedPartitionTransform::Truncate { width } => (7, width),
        ConnectorManagedPartitionTransform::Void => (8, 0),
    }
}

fn managed_partition_transform_from_wire(
    tag: u8,
    parameter: u32,
) -> Result<ConnectorManagedPartitionTransform, ConnectorError> {
    let transform = match (tag, parameter) {
        (1, 0) => ConnectorManagedPartitionTransform::Identity,
        (2, 0) => ConnectorManagedPartitionTransform::Year,
        (3, 0) => ConnectorManagedPartitionTransform::Month,
        (4, 0) => ConnectorManagedPartitionTransform::Day,
        (5, 0) => ConnectorManagedPartitionTransform::Hour,
        (6, buckets) if buckets > 0 => ConnectorManagedPartitionTransform::Bucket { buckets },
        (7, width) if width > 0 => ConnectorManagedPartitionTransform::Truncate { width },
        (8, 0) => ConnectorManagedPartitionTransform::Void,
        _ => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown or invalid connector managed partition transform wire value",
            ));
        }
    };
    transform.validate()?;
    Ok(transform)
}

/// Bounded application facts that a provider may encode as its own managed
/// publication provenance. Target identity and target ref deliberately stay
/// in the signed preparation and cannot be duplicated here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorManagedPublicationIntent {
    refresh_id: i64,
    materialization_id: i64,
    marker: Arc<str>,
    technique: ConnectorManagedPublicationTechnique,
    bases: Vec<super::ConnectorStagedPublicationBaseFact>,
    definition_fingerprint: Arc<str>,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
    partition_spec_replacement: Option<ConnectorManagedPartitionSpecReplacement>,
}

impl ConnectorManagedPublicationIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        refresh_id: i64,
        materialization_id: i64,
        marker: impl Into<Arc<str>>,
        technique: ConnectorManagedPublicationTechnique,
        bases: Vec<super::ConnectorStagedPublicationBaseFact>,
        definition_fingerprint: impl Into<Arc<str>>,
        empty_input: ConnectorManagedPublicationEmptyInputDisposition,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_inner(
            refresh_id,
            materialization_id,
            marker.into(),
            technique,
            bases,
            definition_fingerprint.into(),
            empty_input,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_partition_spec_replacement(
        refresh_id: i64,
        materialization_id: i64,
        marker: impl Into<Arc<str>>,
        technique: ConnectorManagedPublicationTechnique,
        bases: Vec<super::ConnectorStagedPublicationBaseFact>,
        definition_fingerprint: impl Into<Arc<str>>,
        empty_input: ConnectorManagedPublicationEmptyInputDisposition,
        partition_spec_replacement: ConnectorManagedPartitionSpecReplacement,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_inner(
            refresh_id,
            materialization_id,
            marker.into(),
            technique,
            bases,
            definition_fingerprint.into(),
            empty_input,
            Some(partition_spec_replacement),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_new_inner(
        refresh_id: i64,
        materialization_id: i64,
        marker: Arc<str>,
        technique: ConnectorManagedPublicationTechnique,
        bases: Vec<super::ConnectorStagedPublicationBaseFact>,
        definition_fingerprint: Arc<str>,
        empty_input: ConnectorManagedPublicationEmptyInputDisposition,
        partition_spec_replacement: Option<ConnectorManagedPartitionSpecReplacement>,
    ) -> Result<Self, ConnectorError> {
        if refresh_id <= 0
            || materialization_id <= 0
            || marker.is_empty()
            || definition_fingerprint.is_empty()
            || bases.is_empty()
            || bases.len() > super::MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS
            || marker.len() + definition_fingerprint.len()
                > MAX_CONNECTOR_MANAGED_PUBLICATION_TEXT_BYTES
            || bases.iter().any(|base| {
                base.table.is_empty()
                    || base.uuid.is_empty()
                    || base.to_version < 0
                    || base.from_version.is_some_and(|version| version < 0)
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed publication intent is invalid or exceeds its bounds",
            ));
        }
        let mut tables = BTreeSet::new();
        let mut uuids = BTreeSet::new();
        if bases
            .iter()
            .any(|base| !tables.insert(base.table.as_ref()) || !uuids.insert(base.uuid.as_ref()))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed publication intent has duplicate base identities",
            ));
        }
        if partition_spec_replacement.is_some()
            && (technique != ConnectorManagedPublicationTechnique::Full
                || empty_input
                    != ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector managed partition replacement requires a full publication that commits empty input",
            ));
        }
        Ok(Self {
            refresh_id,
            materialization_id,
            marker,
            technique,
            bases,
            definition_fingerprint,
            empty_input,
            partition_spec_replacement,
        })
    }

    pub const fn refresh_id(&self) -> i64 {
        self.refresh_id
    }
    pub const fn materialization_id(&self) -> i64 {
        self.materialization_id
    }
    pub fn marker(&self) -> &str {
        self.marker.as_ref()
    }
    pub const fn technique(&self) -> ConnectorManagedPublicationTechnique {
        self.technique
    }
    pub fn bases(&self) -> &[super::ConnectorStagedPublicationBaseFact] {
        &self.bases
    }
    pub fn definition_fingerprint(&self) -> &str {
        self.definition_fingerprint.as_ref()
    }
    pub const fn empty_input(&self) -> ConnectorManagedPublicationEmptyInputDisposition {
        self.empty_input
    }
    pub fn partition_spec_replacement(&self) -> Option<&ConnectorManagedPartitionSpecReplacement> {
        self.partition_spec_replacement.as_ref()
    }

    fn validate_for_operation(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        if let Some(replacement) = &self.partition_spec_replacement {
            replacement.validate_for_operation(operation_id)?;
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector-managed-publication-intent.v1\0");
        hasher.update(self.refresh_id.to_be_bytes());
        hasher.update(self.materialization_id.to_be_bytes());
        digest_bytes(&mut hasher, self.marker.as_bytes());
        hasher.update([match self.technique {
            ConnectorManagedPublicationTechnique::Full => 1,
            ConnectorManagedPublicationTechnique::Incremental => 2,
        }]);
        for base in &self.bases {
            digest_bytes(&mut hasher, base.table.as_bytes());
            digest_bytes(&mut hasher, base.uuid.as_bytes());
            hasher.update(base.from_version.unwrap_or(-1).to_be_bytes());
            hasher.update(base.to_version.to_be_bytes());
        }
        digest_bytes(&mut hasher, self.definition_fingerprint.as_bytes());
        hasher.update([match self.empty_input {
            ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit => 1,
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite => 2,
        }]);
        // Preserve the frozen ordinary-publication digest exactly. The typed
        // suffix exists only when atomic repartition is explicitly requested.
        if let Some(replacement) = &self.partition_spec_replacement {
            hasher.update(b"partition-spec-replacement\0");
            hasher.update(replacement.digest());
        }
        hasher.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteActivationIntent {
    Ordinary,
    ManagedPublication(ConnectorManagedPublicationIntent),
}

impl ConnectorWriteActivationIntent {
    fn validate_for_operation(
        &self,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::Ordinary => Ok(()),
            Self::ManagedPublication(intent) => intent.validate_for_operation(operation_id),
        }
    }

    fn digest(&self) -> [u8; 32] {
        match self {
            Self::Ordinary => {
                Sha256::digest(b"novarocks.connector-write-activation-ordinary.v1\0").into()
            }
            Self::ManagedPublication(intent) => intent.digest(),
        }
    }
}

#[derive(Clone)]
pub struct ConnectorWriteActivationRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub source: ConnectorWriteActivationSource,
    pub intent: ConnectorWriteActivationIntent,
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteActivationRequest {
    pub fn validate(
        &self,
        owner: &ConnectorExecutionBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        self.intent.validate_for_operation(self.operation_id)?;
        self.source.validate(owner, self.operation_id)
    }
}

/// One provider-signed activated cohort. Only this value may enter planning.
#[derive(Clone)]
pub struct ConnectorActivatedWriteCohort {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    preparation: ConnectorWritePreparation,
    activation_digest: [u8; 32],
}

impl ConnectorActivatedWriteCohort {
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    pub fn preparation(&self) -> &ConnectorWritePreparation {
        &self.preparation
    }
    pub const fn activation_digest(&self) -> [u8; 32] {
        self.activation_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.preparation.validate()?;
        if self.preparation.owner() != &self.owner {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "activated write cohort owner does not match preparation",
            ));
        }
        Ok(())
    }
}

/// Operation-scoped result of exact-generation service reservation.
#[derive(Clone)]
pub struct ConnectorWriteActivation {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    source_digest: [u8; 32],
    activation_digest: [u8; 32],
    cohorts: Vec<ConnectorActivatedWriteCohort>,
    sealed_cohorts: ConnectorSealedWriteCohortSet,
}

impl ConnectorWriteActivation {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        request: &ConnectorWriteActivationRequest,
        mut cohorts: Vec<(ConnectorWriteCohortId, ConnectorWritePreparation)>,
    ) -> Result<Self, ConnectorError> {
        let source_digest = request.validate(&owner)?;
        if cohorts.is_empty() || cohorts.len() > MAX_CONNECTOR_WRITE_COHORTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector activation has an invalid cohort count",
            ));
        }
        cohorts.sort_by_key(|(cohort_id, _)| *cohort_id);
        let mut ids = BTreeSet::new();
        if cohorts.iter().any(|(cohort_id, preparation)| {
            preparation.validate().is_err()
                || preparation.owner() != &owner
                || !ids.insert(*cohort_id)
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector activation contains a foreign or duplicate cohort",
            ));
        }
        let intent_digest = request.intent.digest();
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector-write-activation.v1\0");
        digest_owner(&mut hasher, &owner);
        hasher.update(request.operation_id.to_bytes());
        hasher.update(source_digest);
        hasher.update(intent_digest);
        for (cohort_id, preparation) in &cohorts {
            hasher.update(cohort_id.to_bytes());
            hasher.update(preparation.digest());
        }
        let activation_digest = hasher.finalize().into();
        let cohorts = cohorts
            .into_iter()
            .map(|(cohort_id, preparation)| ConnectorActivatedWriteCohort {
                owner: owner.clone(),
                operation_id: request.operation_id,
                cohort_id,
                preparation,
                activation_digest,
            })
            .collect::<Vec<_>>();
        let sealed_cohorts = ConnectorSealedWriteCohortSet::try_new(
            request.operation_id,
            cohorts
                .iter()
                .map(|cohort| {
                    ConnectorWriteCohortDescriptor::new(
                        cohort.cohort_id(),
                        cohort.preparation().intent(),
                        write_planning_digest(
                            &owner,
                            cohort.operation_id(),
                            cohort.cohort_id(),
                            cohort.activation_digest(),
                        ),
                    )
                })
                .collect(),
        )?;
        Ok(Self {
            owner,
            operation_id: request.operation_id,
            source_digest,
            activation_digest,
            cohorts,
            sealed_cohorts,
        })
    }
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub const fn source_digest(&self) -> [u8; 32] {
        self.source_digest
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.activation_digest
    }
    pub fn cohorts(&self) -> &[ConnectorActivatedWriteCohort] {
        &self.cohorts
    }
    /// Exact operation authority reserved by this activation.
    ///
    /// Consumers retain this set before building any local planning carriers,
    /// so a local validation failure after provider activation can still be
    /// terminalized through the exact lease.
    pub fn sealed_cohorts(&self) -> &ConnectorSealedWriteCohortSet {
        &self.sealed_cohorts
    }
    pub fn cohort(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Option<ConnectorActivatedWriteCohort> {
        self.cohorts
            .iter()
            .find(|cohort| cohort.cohort_id == cohort_id)
            .cloned()
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.cohorts.is_empty() || self.cohorts.len() > MAX_CONNECTOR_WRITE_COHORTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector activation cohort count is invalid",
            ));
        }
        let mut ids = BTreeSet::new();
        if self.cohorts.iter().any(|cohort| {
            cohort.validate().is_err()
                || cohort.owner() != &self.owner
                || cohort.operation_id() != self.operation_id
                || cohort.activation_digest() != self.activation_digest
                || !ids.insert(cohort.cohort_id())
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector activation contains invalid cohorts",
            ));
        }
        let expected_sealed = ConnectorSealedWriteCohortSet::try_new(
            self.operation_id,
            self.cohorts
                .iter()
                .map(|cohort| {
                    ConnectorWriteCohortDescriptor::new(
                        cohort.cohort_id(),
                        cohort.preparation().intent(),
                        write_planning_digest(
                            &self.owner,
                            cohort.operation_id(),
                            cohort.cohort_id(),
                            cohort.activation_digest(),
                        ),
                    )
                })
                .collect(),
        )?;
        if expected_sealed != self.sealed_cohorts {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector activation sealed cohort set does not match its cohorts",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorWritePlanningRequest {
    pub operation_id: ConnectorWriteOperationId,
    pub cohort_id: ConnectorWriteCohortId,
    pub execution_id: ConnectorWriteExecutionId,
    pub activation: ConnectorActivatedWriteCohort,
    pub expected_writers: Vec<ConnectorWriterIdentity>,
    pub context: ConnectorRequestContext,
}

impl ConnectorWritePlanningRequest {
    pub fn validate(&self, owner: &ConnectorExecutionBindingKey) -> Result<(), ConnectorError> {
        self.activation.validate()?;
        if self.activation.owner() != owner
            || self.activation.operation_id() != self.operation_id
            || self.activation.cohort_id() != self.cohort_id
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write planning preparation does not match the exact control owner",
            ));
        }
        if self.expected_writers.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write planning requires at least one expected writer",
            ));
        }
        let mut writers = HashSet::with_capacity(self.expected_writers.len());
        for writer in &self.expected_writers {
            if writer.operation_id != self.operation_id
                || writer.cohort_id != self.cohort_id
                || writer.execution_id != self.execution_id
                || &writer.binding_key != owner
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write planning writer does not match the requested owner and operation",
                ));
            }
            if !writers.insert(writer.clone()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write planning contains a duplicate writer identity",
                ));
            }
        }
        Ok(())
    }

    pub fn stable_digest(
        &self,
        owner: &ConnectorExecutionBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        // This digest is also used while sealing a preparation, before the
        // placement-frozen writer set exists. Writer validation remains
        // mandatory in `validate`, which every provider planning request
        // invokes after `ConnectorWriteManifest::plan` fills that set.
        self.activation.validate()?;
        if self.activation.owner() != owner
            || self.activation.operation_id() != self.operation_id
            || self.activation.cohort_id() != self.cohort_id
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write planning preparation does not match the exact control owner",
            ));
        }
        Ok(write_planning_digest(
            owner,
            self.operation_id,
            self.cohort_id,
            self.activation.activation_digest(),
        ))
    }
}

fn write_planning_digest(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    activation_digest: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-write-planning.v1\0");
    digest_owner(&mut hasher, owner);
    hasher.update(operation_id.to_bytes());
    hasher.update(cohort_id.to_bytes());
    hasher.update(activation_digest);
    hasher.finalize().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriterHandle {
    owner: ConnectorExecutionBindingKey,
    writer: ConnectorWriterIdentity,
    version: u32,
    payload: Bytes,
    payload_digest: [u8; 32],
}

impl ConnectorWriterHandle {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        writer: ConnectorWriterIdentity,
        version: u32,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_handle_payload(&payload)?;
        if version == 0 || writer.binding_key != owner {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector writer handle owner or version is invalid",
            ));
        }
        Ok(Self {
            owner,
            writer,
            version,
            payload_digest: sha256(&payload),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.version == 0
            || self.writer.binding_key != self.owner
            || self.payload_digest != sha256(&self.payload)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector writer handle integrity validation failed",
            ));
        }
        validate_handle_payload(&self.payload)
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWritePlan {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    handles: Vec<ConnectorWriterHandle>,
    control_payload: Bytes,
}

impl ConnectorWritePlan {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        handles: Vec<ConnectorWriterHandle>,
        control_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_total_handle_payload(&handles, &control_payload)?;
        let mut writers = HashSet::with_capacity(handles.len());
        for handle in &handles {
            handle.validate()?;
            if handle.owner != owner
                || handle.writer.operation_id != operation_id
                || handle.writer.cohort_id != cohort_id
                || handle.writer.execution_id != execution_id
                || !writers.insert(handle.writer.clone())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write plan handles do not form one exact writer manifest",
                ));
            }
        }
        if handles.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write plan must contain at least one writer handle",
            ));
        }
        Ok(Self {
            owner,
            operation_id,
            cohort_id,
            execution_id,
            handles,
            control_payload,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub fn handles(&self) -> &[ConnectorWriterHandle] {
        &self.handles
    }

    pub fn control_payload(&self) -> &Bytes {
        &self.control_payload
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorStagedReportSummary {
    pub input_rows: u64,
    pub staged_bytes: u64,
    pub artifact_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriterTerminalState {
    Staged,
    Aborted,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedReport {
    writer: ConnectorWriterIdentity,
    version: u32,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    payload: Bytes,
    payload_digest: [u8; 32],
}

impl ConnectorStagedReport {
    pub fn try_new(
        writer: ConnectorWriterIdentity,
        version: u32,
        state: ConnectorWriterTerminalState,
        summary: ConnectorStagedReportSummary,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_report_payload(&payload)?;
        if version == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report version must be nonzero",
            ));
        }
        Ok(Self {
            writer,
            version,
            state,
            summary,
            payload_digest: sha256(&payload),
            payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.version == 0 || self.payload_digest != sha256(&self.payload) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report integrity validation failed",
            ));
        }
        validate_report_payload(&self.payload)
    }

    pub fn frames(&self) -> Vec<ConnectorStagedReportFrame> {
        let part_count = self
            .payload
            .len()
            .max(1)
            .div_ceil(MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES) as u32;
        self.payload
            .chunks(MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES)
            .enumerate()
            .map(|(part_index, payload)| {
                ConnectorStagedReportFrame::try_new(
                    self.writer.clone(),
                    self.version,
                    self.state,
                    self.summary,
                    part_index as u32,
                    part_count,
                    self.payload.len() as u64,
                    self.payload_digest,
                    Bytes::copy_from_slice(payload),
                )
                .expect("validated connector staged report must frame")
            })
            .collect()
    }

    /// Reassemble the complete, bounded frame sequence for one logical
    /// writer report. Identical duplicate frames are accepted because report
    /// delivery is retryable; conflicting duplicates and incomplete ranges
    /// are rejected before provider payloads become visible to a consumer.
    pub fn try_from_frames(
        frames: impl IntoIterator<Item = ConnectorStagedReportFrame>,
    ) -> Result<Self, ConnectorError> {
        let mut parts = BTreeMap::new();
        let mut first: Option<ConnectorStagedReportFrame> = None;
        for frame in frames {
            frame.validate()?;
            if let Some(expected) = &first {
                if frame.writer != expected.writer
                    || frame.version != expected.version
                    || frame.state != expected.state
                    || frame.summary != expected.summary
                    || frame.part_count != expected.part_count
                    || frame.logical_payload_len != expected.logical_payload_len
                    || frame.logical_payload_digest != expected.logical_payload_digest
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector staged report frames disagree on logical report identity",
                    ));
                }
            } else {
                first = Some(frame.clone());
            }
            match parts.entry(frame.part_index) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(frame);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &frame => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "connector staged report has conflicting duplicate frame",
                    ));
                }
            }
        }
        let first = first.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report has no frames",
            )
        })?;
        if parts.len() != first.part_count as usize || parts.keys().copied().ne(0..first.part_count)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report frame range is incomplete",
            ));
        }
        let payload_len = usize::try_from(first.logical_payload_len).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector staged report payload length does not fit usize",
            )
        })?;
        let mut payload = Vec::with_capacity(payload_len);
        for frame in parts.into_values() {
            payload.extend_from_slice(&frame.frame_payload);
        }
        if payload.len() != payload_len {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frames do not match the declared payload length",
            ));
        }
        let report = Self::try_new(
            first.writer,
            first.version,
            first.state,
            first.summary,
            Bytes::from(payload),
        )?;
        if report.payload_digest != first.logical_payload_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frames do not match the declared payload digest",
            ));
        }
        Ok(report)
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn state(&self) -> ConnectorWriterTerminalState {
        self.state
    }

    pub const fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStagedReportFrame {
    writer: ConnectorWriterIdentity,
    version: u32,
    state: ConnectorWriterTerminalState,
    summary: ConnectorStagedReportSummary,
    part_index: u32,
    part_count: u32,
    logical_payload_len: u64,
    logical_payload_digest: [u8; 32],
    frame_payload: Bytes,
    frame_payload_digest: [u8; 32],
}

impl ConnectorStagedReportFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        writer: ConnectorWriterIdentity,
        version: u32,
        state: ConnectorWriterTerminalState,
        summary: ConnectorStagedReportSummary,
        part_index: u32,
        part_count: u32,
        logical_payload_len: u64,
        logical_payload_digest: [u8; 32],
        frame_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if version == 0
            || part_count == 0
            || part_count > MAX_CONNECTOR_STAGED_REPORT_PARTS
            || part_index >= part_count
            || logical_payload_len as usize > MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES
            || frame_payload.len() > MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector staged report frame exceeds its contract bounds",
            ));
        }
        Ok(Self {
            writer,
            version,
            state,
            summary,
            part_index,
            part_count,
            logical_payload_len,
            logical_payload_digest,
            frame_payload_digest: sha256(&frame_payload),
            frame_payload,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(
            self.writer.clone(),
            self.version,
            self.state,
            self.summary,
            self.part_index,
            self.part_count,
            self.logical_payload_len,
            self.logical_payload_digest,
            self.frame_payload.clone(),
        )?;
        if expected.frame_payload_digest != self.frame_payload_digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector staged report frame digest does not match its payload",
            ));
        }
        Ok(())
    }

    pub fn writer(&self) -> &ConnectorWriterIdentity {
        &self.writer
    }
    pub const fn version(&self) -> u32 {
        self.version
    }
    pub const fn state(&self) -> ConnectorWriterTerminalState {
        self.state
    }
    pub const fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary
    }
    pub const fn part_index(&self) -> u32 {
        self.part_index
    }
    pub const fn part_count(&self) -> u32 {
        self.part_count
    }
    pub const fn logical_payload_len(&self) -> u64 {
        self.logical_payload_len
    }
    pub const fn logical_payload_digest(&self) -> [u8; 32] {
        self.logical_payload_digest
    }
    pub fn frame_payload(&self) -> &Bytes {
        &self.frame_payload
    }
    pub const fn frame_payload_digest(&self) -> [u8; 32] {
        self.frame_payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteReceipt {
    payload: Bytes,
    digest: [u8; 32],
    committed_version: Option<ConnectorCommittedVersion>,
    resulting_row_count: Option<u64>,
    committed_partitioning: Option<ConnectorCommittedPartitioning>,
}

impl ConnectorWriteReceipt {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        validate_receipt_payload(&payload)?;
        Ok(Self {
            digest: sha256(&payload),
            payload,
            committed_version: None,
            resulting_row_count: None,
            committed_partitioning: None,
        })
    }

    pub fn try_new_with_committed_version(
        payload: Bytes,
        committed_version: ConnectorCommittedVersion,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new(payload)?;
        committed_version.validate()?;
        receipt.committed_version = Some(committed_version);
        Ok(receipt)
    }

    pub fn try_new_with_committed_facts(
        payload: Bytes,
        committed_version: ConnectorCommittedVersion,
        resulting_row_count: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new_with_committed_version(payload, committed_version)?;
        receipt.resulting_row_count = resulting_row_count;
        Ok(receipt)
    }

    pub fn try_new_with_committed_facts_and_partitioning(
        payload: Bytes,
        committed_version: ConnectorCommittedVersion,
        resulting_row_count: Option<u64>,
        committed_partitioning: ConnectorCommittedPartitioning,
    ) -> Result<Self, ConnectorError> {
        let mut receipt =
            Self::try_new_with_committed_facts(payload, committed_version, resulting_row_count)?;
        committed_partitioning.validate()?;
        receipt.committed_partitioning = Some(committed_partitioning);
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_receipt_payload(&self.payload)?;
        if self.digest != sha256(&self.payload) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write receipt digest does not match its payload",
            ));
        }
        if let Some(version) = &self.committed_version {
            version.validate()?;
        }
        if let Some(partitioning) = &self.committed_partitioning {
            if self.committed_version.is_none() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector committed partitioning requires a committed version",
                ));
            }
            partitioning.validate()?;
        }
        Ok(())
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn committed_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.committed_version.as_ref()
    }
    pub const fn resulting_row_count(&self) -> Option<u64> {
        self.resulting_row_count
    }
    pub fn committed_partitioning(&self) -> Option<&ConnectorCommittedPartitioning> {
        self.committed_partitioning.as_ref()
    }

    /// Stable durable form for application journals. The provider payload is
    /// carried opaquely and never decoded outside the provider.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        const MAGIC: &[u8; 4] = b"CWR1";
        let payload_len = u32::try_from(self.payload.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector write receipt exceeds wire bound",
            )
        })?;
        let version = self.committed_version.as_ref();
        let version_payload = version.map_or(&[][..], |value| value.payload().as_ref());
        let version_len = u32::try_from(version_payload.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector committed version exceeds wire bound",
            )
        })?;
        let mut encoded = Vec::with_capacity(
            4 + 4 + self.payload.len() + 1 + 4 + version_payload.len() + 1 + 8 + 1 + 8,
        );
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(self.payload.as_ref());
        encoded.push(u8::from(version.is_some()));
        if let Some(version) = version {
            encoded.extend_from_slice(&version_len.to_be_bytes());
            encoded.extend_from_slice(version_payload);
            match version.snapshot_id() {
                Some(snapshot_id) => {
                    encoded.push(1);
                    encoded.extend_from_slice(&snapshot_id.to_be_bytes());
                }
                None => encoded.push(0),
            }
        }
        match self.resulting_row_count {
            Some(count) => {
                encoded.push(1);
                encoded.extend_from_slice(&count.to_be_bytes());
            }
            None => encoded.push(0),
        }
        if let Some(partitioning) = &self.committed_partitioning {
            encoded.push(1);
            encoded.extend_from_slice(&partitioning.spec_id().to_be_bytes());
            encoded.extend_from_slice(&partitioning.digest());
            encoded.extend_from_slice(&(partitioning.fields().len() as u32).to_be_bytes());
            for field in partitioning.fields() {
                encoded.extend_from_slice(&field.partition_field_id().to_be_bytes());
                encoded
                    .extend_from_slice(&(field.partition_field_name().len() as u32).to_be_bytes());
                encoded.extend_from_slice(field.partition_field_name().as_bytes());
                encoded.extend_from_slice(&field.source_field_id().to_be_bytes());
                encoded.extend_from_slice(&(field.source_column_name().len() as u32).to_be_bytes());
                encoded.extend_from_slice(field.source_column_name().as_bytes());
                encoded.extend_from_slice(&field.position().to_be_bytes());
                let (tag, parameter) = managed_partition_transform_wire(field.transform());
                encoded.push(tag);
                encoded.extend_from_slice(&parameter.to_be_bytes());
            }
        }
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        const MAGIC: &[u8; 4] = b"CWR1";
        let mut offset = 0usize;
        let read = |offset: &mut usize, len: usize| -> Result<&[u8], ConnectorError> {
            let end = offset.checked_add(len).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector write receipt wire overflow",
                )
            })?;
            let value = bytes.get(*offset..end).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "truncated connector write receipt wire",
                )
            })?;
            *offset = end;
            Ok(value)
        };
        if read(&mut offset, 4)? != MAGIC {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unsupported connector write receipt wire version",
            ));
        }
        let read_u32 = |offset: &mut usize| -> Result<usize, ConnectorError> {
            let raw: [u8; 4] = read(offset, 4)?
                .try_into()
                .expect("fixed-width receipt length");
            Ok(u32::from_be_bytes(raw) as usize)
        };
        let payload_len = read_u32(&mut offset)?;
        let payload = Bytes::copy_from_slice(read(&mut offset, payload_len)?);
        let version = match read(&mut offset, 1)?[0] {
            0 => None,
            1 => {
                let version_len = read_u32(&mut offset)?;
                let version_payload = Bytes::copy_from_slice(read(&mut offset, version_len)?);
                let snapshot_id = match read(&mut offset, 1)?[0] {
                    0 => None,
                    1 => Some(i64::from_be_bytes(
                        read(&mut offset, 8)?
                            .try_into()
                            .expect("fixed-width receipt snapshot"),
                    )),
                    _ => {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "invalid connector write receipt snapshot tag",
                        ));
                    }
                };
                Some(ConnectorCommittedVersion::try_new(
                    version_payload,
                    snapshot_id,
                )?)
            }
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "invalid connector write receipt version tag",
                ));
            }
        };
        let row_count = match read(&mut offset, 1)?[0] {
            0 => None,
            1 => Some(u64::from_be_bytes(
                read(&mut offset, 8)?
                    .try_into()
                    .expect("fixed-width receipt row count"),
            )),
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "invalid connector write receipt row-count tag",
                ));
            }
        };
        // The optional appendix preserves the byte-for-byte durable form of
        // all pre-repartition and ordinary receipts.
        let partitioning = if offset == bytes.len() {
            None
        } else {
            match read(&mut offset, 1)?[0] {
                1 => {
                    let spec_id = i32::from_be_bytes(
                        read(&mut offset, 4)?
                            .try_into()
                            .expect("fixed-width committed partition spec ID"),
                    );
                    let expected_digest: [u8; 32] = read(&mut offset, 32)?
                        .try_into()
                        .expect("fixed-width committed partition digest");
                    let field_count = read_u32(&mut offset)?;
                    if field_count == 0 || field_count > MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "connector committed partition field count is invalid",
                        ));
                    }
                    let mut fields = Vec::with_capacity(field_count);
                    for _ in 0..field_count {
                        let partition_field_id = i32::from_be_bytes(
                            read(&mut offset, 4)?
                                .try_into()
                                .expect("fixed-width partition field ID"),
                        );
                        let partition_field_name_len = read_u32(&mut offset)?;
                        let partition_field_name =
                            std::str::from_utf8(read(&mut offset, partition_field_name_len)?)
                                .map_err(|_| {
                                    ConnectorError::new(
                                        ConnectorErrorKind::CorruptData,
                                        "connector committed partition field name is not UTF-8",
                                    )
                                })?;
                        let source_field_id = i32::from_be_bytes(
                            read(&mut offset, 4)?
                                .try_into()
                                .expect("fixed-width partition source field ID"),
                        );
                        let source_column_name_len = read_u32(&mut offset)?;
                        let source_column_name =
                            std::str::from_utf8(read(&mut offset, source_column_name_len)?)
                                .map_err(|_| {
                                    ConnectorError::new(
                                        ConnectorErrorKind::CorruptData,
                                        "connector committed partition source name is not UTF-8",
                                    )
                                })?;
                        let position = u32::from_be_bytes(
                            read(&mut offset, 4)?
                                .try_into()
                                .expect("fixed-width partition field position"),
                        );
                        let tag = read(&mut offset, 1)?[0];
                        let parameter = u32::from_be_bytes(
                            read(&mut offset, 4)?
                                .try_into()
                                .expect("fixed-width partition transform parameter"),
                        );
                        fields.push(ConnectorCommittedPartitionField::try_new(
                            partition_field_id,
                            partition_field_name,
                            source_field_id,
                            source_column_name,
                            position,
                            managed_partition_transform_from_wire(tag, parameter)?,
                        )?);
                    }
                    let partitioning = ConnectorCommittedPartitioning::try_new(spec_id, fields)?;
                    if partitioning.digest() != expected_digest {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "connector committed partitioning wire digest does not match its facts",
                        ));
                    }
                    Some(partitioning)
                }
                _ => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "invalid connector committed partitioning tag",
                    ));
                }
            }
        };
        if offset != bytes.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write receipt wire has trailing bytes",
            ));
        }
        match (version, partitioning) {
            (Some(version), Some(partitioning)) => {
                Self::try_new_with_committed_facts_and_partitioning(
                    payload,
                    version,
                    row_count,
                    partitioning,
                )
            }
            (Some(version), None) => {
                Self::try_new_with_committed_facts(payload, version, row_count)
            }
            (None, _) if row_count.is_some() => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write receipt row count requires a committed version",
            )),
            (None, Some(_)) => Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write receipt partitioning requires a committed version",
            )),
            (None, None) => Self::try_new(payload),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteCohortDescriptor {
    cohort_id: ConnectorWriteCohortId,
    intent: ConnectorWriteIntent,
    planning_digest: [u8; 32],
}

impl ConnectorWriteCohortDescriptor {
    pub const fn new(
        cohort_id: ConnectorWriteCohortId,
        intent: ConnectorWriteIntent,
        planning_digest: [u8; 32],
    ) -> Self {
        Self {
            cohort_id,
            intent,
            planning_digest,
        }
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn intent(&self) -> ConnectorWriteIntent {
        self.intent
    }

    pub const fn planning_digest(&self) -> [u8; 32] {
        self.planning_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSealedWriteCohortSet {
    operation_id: ConnectorWriteOperationId,
    cohorts: Vec<ConnectorWriteCohortDescriptor>,
    digest: [u8; 32],
}

impl ConnectorSealedWriteCohortSet {
    pub fn try_new(
        operation_id: ConnectorWriteOperationId,
        cohorts: Vec<ConnectorWriteCohortDescriptor>,
    ) -> Result<Self, ConnectorError> {
        if cohorts.is_empty() || cohorts.len() > MAX_CONNECTOR_WRITE_COHORTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector write cohort set must contain 1..=4096 cohorts",
            ));
        }
        let mut cohorts = cohorts;
        cohorts.sort_by_key(ConnectorWriteCohortDescriptor::cohort_id);
        if cohorts
            .windows(2)
            .any(|pair| pair[0].cohort_id == pair[1].cohort_id)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort set contains a duplicate cohort ID",
            ));
        }
        let digest = cohort_set_digest(operation_id, &cohorts);
        Ok(Self {
            operation_id,
            cohorts,
            digest,
        })
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortDescriptor] {
        &self.cohorts
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteAttemptCompletion {
    owner: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    manifest_digest: [u8; 32],
    reports: Vec<ConnectorStagedReport>,
    control_payload: Bytes,
    digest: [u8; 32],
}

impl ConnectorWriteAttemptCompletion {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
        cohort_id: ConnectorWriteCohortId,
        execution_id: ConnectorWriteExecutionId,
        manifest_digest: [u8; 32],
        reports: Vec<ConnectorStagedReport>,
        control_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_handle_payload(&control_payload)?;
        if reports.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write attempt completion has no staged reports",
            ));
        }
        let mut writers = BTreeSet::new();
        for report in &reports {
            report.validate()?;
            let writer = report.writer();
            if writer.binding_key() != &owner
                || writer.operation_id() != operation_id
                || writer.cohort_id() != cohort_id
                || writer.execution_id() != execution_id
                || !writers.insert(writer.clone())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write attempt reports do not form one exact cohort attempt",
                ));
            }
        }
        let mut reports = reports;
        reports.sort_by(|left, right| left.writer().cmp(right.writer()));
        let digest = attempt_completion_digest(
            &owner,
            operation_id,
            cohort_id,
            execution_id,
            manifest_digest,
            &reports,
            &control_payload,
        );
        Ok(Self {
            owner,
            operation_id,
            cohort_id,
            execution_id,
            manifest_digest,
            reports,
            control_payload,
            digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub const fn execution_id(&self) -> ConnectorWriteExecutionId {
        self.execution_id
    }

    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }

    pub fn reports(&self) -> &[ConnectorStagedReport] {
        &self.reports
    }

    pub fn control_payload(&self) -> &Bytes {
        &self.control_payload
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteCohortCompletion {
    cohort_id: ConnectorWriteCohortId,
    accepted: Option<ConnectorWriteAttemptCompletion>,
    superseded: Vec<ConnectorWriteAttemptCompletion>,
}

impl ConnectorWriteCohortCompletion {
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        accepted: Option<ConnectorWriteAttemptCompletion>,
        superseded: Vec<ConnectorWriteAttemptCompletion>,
    ) -> Result<Self, ConnectorError> {
        if accepted.is_none() && superseded.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write cohort completion has no attempt reports",
            ));
        }
        let mut attempts = BTreeSet::new();
        for attempt in accepted.iter().chain(&superseded) {
            if attempt.cohort_id != cohort_id || !attempts.insert(attempt.execution_id) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write cohort completion contains a foreign or duplicate attempt",
                ));
            }
        }
        let mut superseded = superseded;
        superseded.sort_by_key(ConnectorWriteAttemptCompletion::execution_id);
        Ok(Self {
            cohort_id,
            accepted,
            superseded,
        })
    }

    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub fn accepted(&self) -> Option<&ConnectorWriteAttemptCompletion> {
        self.accepted.as_ref()
    }

    pub fn superseded(&self) -> &[ConnectorWriteAttemptCompletion] {
        &self.superseded
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteOperationCompletion {
    owner: ConnectorExecutionBindingKey,
    sealed: ConnectorSealedWriteCohortSet,
    cohorts: Vec<ConnectorWriteCohortCompletion>,
    aggregate_digest: [u8; 32],
}

impl ConnectorWriteOperationCompletion {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        sealed: ConnectorSealedWriteCohortSet,
        cohorts: Vec<ConnectorWriteCohortCompletion>,
    ) -> Result<Self, ConnectorError> {
        validate_operation_cohorts(&owner, &sealed, &cohorts, true)?;
        let aggregate_digest = operation_completion_digest(&owner, &sealed, &cohorts);
        Ok(Self {
            owner,
            sealed,
            cohorts,
            aggregate_digest,
        })
    }

    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        &self.owner
    }

    pub fn sealed(&self) -> &ConnectorSealedWriteCohortSet {
        &self.sealed
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortCompletion] {
        &self.cohorts
    }

    pub const fn aggregate_digest(&self) -> [u8; 32] {
        self.aggregate_digest
    }
}

#[derive(Clone)]
pub struct ConnectorOpenWriterRequest {
    pub handle: ConnectorWriterHandle,
    pub expected_schema: SchemaRef,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorWriteCommitRequest {
    pub completion: ConnectorWriteOperationCompletion,
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteCommitRequest {
    pub fn owner(&self) -> &ConnectorExecutionBindingKey {
        self.completion.owner()
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.completion.sealed.operation_id
    }

    pub fn sealed(&self) -> &ConnectorSealedWriteCohortSet {
        self.completion.sealed()
    }

    pub fn cohorts(&self) -> &[ConnectorWriteCohortCompletion] {
        self.completion.cohorts()
    }

    pub const fn aggregate_digest(&self) -> [u8; 32] {
        self.completion.aggregate_digest()
    }
}

#[derive(Clone)]
pub struct ConnectorWriteAbortRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub sealed: ConnectorSealedWriteCohortSet,
    pub cohorts: Vec<ConnectorWriteCohortCompletion>,
    pub aggregate_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteAbortRequest {
    pub fn try_new(
        owner: ConnectorExecutionBindingKey,
        sealed: ConnectorSealedWriteCohortSet,
        cohorts: Vec<ConnectorWriteCohortCompletion>,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_operation_cohorts(&owner, &sealed, &cohorts, false)?;
        let aggregate_digest = operation_completion_digest(&owner, &sealed, &cohorts);
        Ok(Self {
            owner,
            sealed,
            cohorts,
            aggregate_digest,
            context,
        })
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.sealed.operation_id
    }
}

#[derive(Clone)]
pub struct ConnectorWriteReconcileRequest {
    pub owner: ConnectorExecutionBindingKey,
    pub operation_id: ConnectorWriteOperationId,
    pub cohort_set_digest: [u8; 32],
    pub aggregate_digest: [u8; 32],
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteAbortOutcome {
    KnownUncommitted {
        cleanup: ExternalMutationFinalization,
    },
    KnownCommitted {
        receipt: ConnectorWriteReceipt,
        finalization: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

pub trait ConnectorWriteControl: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn prepare_write(
        &self,
        _request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not implement write preparation",
        ))
    }

    /// Plans a logical row mutation under this exact write control generation.
    /// This contract is intentionally separate from ordinary write preparation:
    /// providers own strategy, identity, opaque routes, and cohorts.
    fn prepare_row_mutation(
        &self,
        _request: super::ConnectorRowMutationPreparationRequest,
    ) -> Result<super::ConnectorRowMutationPreparationOutcome, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not implement row-mutation preparation",
        ))
    }

    /// Activates a sealed direct route set or COW preparation plus bounded
    /// selection. Implementations must not obtain a new control generation.
    fn activate_row_mutation(
        &self,
        _request: super::ConnectorRowMutationActivationRequest,
    ) -> Result<super::ConnectorRowMutationExecutionPlan, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not implement row-mutation activation",
        ))
    }

    /// Reserve the exact-generation writer/committer service after admission
    /// and before placement planning. Implementations must make identical
    /// requests idempotent and reject a conflicting request for one operation.
    fn activate_write(
        &self,
        _request: ConnectorWriteActivationRequest,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not implement write activation",
        ))
    }

    fn plan_write(
        &self,
        request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError>;

    fn commit(
        &self,
        request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;

    fn abort(
        &self,
        request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError>;

    fn reconcile(
        &self,
        request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorWriteLease {
    binding_key: ConnectorExecutionBindingKey,
    control: Arc<dyn ConnectorWriteControl>,
    execution_distribution: Option<Arc<dyn ConnectorExecutionDistribution>>,
    metadata: Option<Arc<dyn super::ConnectorMetadata>>,
    _release: Arc<ConnectorWriteLeaseRelease>,
}

struct ConnectorWriteLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorWriteLease {
    pub fn new(
        binding_key: ConnectorExecutionBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if control.binding_key() != &binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write lease control does not match its binding generation",
            ));
        }
        Ok(Self {
            binding_key,
            control,
            execution_distribution: None,
            metadata: None,
            _release: Arc::new(ConnectorWriteLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    /// Create an exact write lease that can also materialize the BE execution
    /// declaration from the same retained control generation. Production
    /// callers must use this constructor; the narrower `new` remains useful
    /// for isolated control-only conformance tests.
    pub fn new_with_execution_distribution(
        binding_key: ConnectorExecutionBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        execution_distribution: Arc<dyn ConnectorExecutionDistribution>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let mut lease = Self::new(binding_key, control, release)?;
        lease.execution_distribution = Some(execution_distribution);
        Ok(lease)
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    pub fn control(&self) -> &Arc<dyn ConnectorWriteControl> {
        &self.control
    }

    pub fn prepare_row_mutation(
        &self,
        request: super::ConnectorRowMutationPreparationRequest,
    ) -> Result<super::ConnectorRowMutationPreparationOutcome, ConnectorError> {
        request.validate(&self.binding_key)?;
        let outcome = self.control.prepare_row_mutation(request)?;
        if let super::ConnectorRowMutationPreparationOutcome::Prepared(preparation) = &outcome {
            preparation.validate()?;
            if preparation.owner() != &self.binding_key {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "row-mutation preparation does not retain the lease generation",
                ));
            }
        }
        Ok(outcome)
    }

    pub fn activate_row_mutation(
        &self,
        request: super::ConnectorRowMutationActivationRequest,
    ) -> Result<super::ConnectorRowMutationExecutionPlan, ConnectorError> {
        request.validate(&self.binding_key)?;
        let expected_request = request.clone();
        let preparation = request.preparation().clone();
        let plan = self.control.activate_row_mutation(request)?;
        let contract = preparation.match_contract();
        for route in plan.routes() {
            route.validate()?;
            if route.preparation().owner() != &self.binding_key {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "row-mutation route preparation does not retain the lease generation",
                ));
            }
            if route.preparation().table() != preparation.table()
                || route.preparation().base_version() != preparation.base_version()
                || route
                    .accepted_effects()
                    .iter()
                    .any(|effect| !preparation.intent().accepts(*effect))
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "row-mutation route does not match its sealed table, base version, or intent",
                ));
            }
            if route.input().fields().into_iter().any(|binding| {
                !contract
                    .identity_fields()
                    .iter()
                    .any(|field| field.token() == binding.token())
                    && !contract
                        .before_fields()
                        .iter()
                        .any(|field| field.token() == binding.token())
                    && !contract
                        .after_fields()
                        .iter()
                        .any(|field| field.token() == binding.token())
            }) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "row-mutation route has an input token foreign to its match contract",
                ));
            }
        }
        plan.validate_against_activation(&expected_request, &self.binding_key)?;
        Ok(plan)
    }

    pub fn activate_write(
        &self,
        request: ConnectorWriteActivationRequest,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        let source_digest = request.validate(&self.binding_key)?;
        let operation_id = request.operation_id;
        let activation = self.control.activate_write(request)?;
        activation.validate()?;
        if activation.owner() != &self.binding_key
            || activation.operation_id() != operation_id
            || activation.source_digest() != source_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector write activation does not retain the exact lease generation",
            ));
        }
        Ok(activation)
    }

    /// Return whether two leases retain the same provider control generation.
    /// Clones of one lease compare equal here; independently-derived leases
    /// compare equal only when they retain the same exact control capability.
    pub fn retains_same_generation(&self, other: &Self) -> bool {
        self.binding_key == other.binding_key && Arc::ptr_eq(&self.control, &other.control)
    }

    /// Retain metadata from the same control generation as this writer.
    /// Only `ConnectorControlPlanningLease::derive_write_lease` supplies this
    /// in production; standalone write-control tests may deliberately omit it.
    pub fn with_metadata(mut self, metadata: Arc<dyn super::ConnectorMetadata>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Resolve an opaque write target through the metadata capability retained
    /// from this exact generation. Core never constructs a table-handle payload.
    pub fn load_table(
        &self,
        request: super::ConnectorTableRequest,
    ) -> Result<super::ConnectorTableMetadata, ConnectorError> {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector write lease has no exact-generation metadata capability",
            )
        })?;
        metadata.load_table(request)
    }

    /// Materialize a declaration only through the exact generation held by
    /// this lease. A later active incarnation is deliberately unreachable.
    pub fn execution_declaration(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        let distribution = self.execution_distribution.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write lease has no execution distribution capability",
            )
        })?;
        let declaration = distribution.declaration(context)?;
        let key = declaration.binding_key();
        if key != self.binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write declaration does not match its retained binding generation",
            ));
        }
        Ok(declaration)
    }
}

impl Drop for ConnectorWriteLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

pub trait ConnectorWriteExecution: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError>;
}

pub trait ConnectorBatchWriter: Send {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError>;

    fn abort(&mut self) -> Result<(), ConnectorError>;

    fn summary(&self) -> ConnectorStagedReportSummary {
        ConnectorStagedReportSummary::default()
    }
}

fn validate_handle_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write handle payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_total_handle_payload(
    handles: &[ConnectorWriterHandle],
    control_payload: &Bytes,
) -> Result<(), ConnectorError> {
    validate_handle_payload(control_payload)?;
    let total = handles
        .iter()
        .try_fold(control_payload.len(), |total, handle| {
            handle.payload.len().checked_add(total).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector write plan payload accounting overflowed",
                )
            })
        })?;
    if total > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write plan payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_report_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector staged report payload exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_receipt_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_WRITE_RECEIPT_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write receipt exceeds the hard limit",
        ));
    }
    Ok(())
}

fn validate_operation_cohorts(
    owner: &ConnectorExecutionBindingKey,
    sealed: &ConnectorSealedWriteCohortSet,
    cohorts: &[ConnectorWriteCohortCompletion],
    require_complete: bool,
) -> Result<(), ConnectorError> {
    if cohorts.len() > sealed.cohorts.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write operation contains more completions than sealed cohorts",
        ));
    }
    let expected = sealed
        .cohorts
        .iter()
        .map(ConnectorWriteCohortDescriptor::cohort_id)
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    let mut writer_count = 0usize;
    let mut payload_bytes = 0usize;
    for cohort in cohorts {
        if !expected.contains(&cohort.cohort_id) || !actual.insert(cohort.cohort_id) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write operation contains an unknown or duplicate cohort completion",
            ));
        }
        if require_complete && cohort.accepted.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write commit is missing an accepted cohort attempt",
            ));
        }
        for attempt in cohort.accepted.iter().chain(&cohort.superseded) {
            if &attempt.owner != owner || attempt.operation_id != sealed.operation_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write operation attempt has a foreign owner or operation",
                ));
            }
            writer_count = writer_count
                .checked_add(attempt.reports.len())
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "connector write operation writer accounting overflowed",
                    )
                })?;
            payload_bytes = attempt.reports.iter().try_fold(
                payload_bytes
                    .checked_add(attempt.control_payload.len())
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "connector write operation payload accounting overflowed",
                        )
                    })?,
                |total, report| {
                    total.checked_add(report.payload.len()).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "connector write operation payload accounting overflowed",
                        )
                    })
                },
            )?;
        }
    }
    if require_complete && actual != expected {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write commit does not exactly cover the sealed cohort set",
        ));
    }
    if writer_count > MAX_CONNECTOR_WRITE_OPERATION_WRITERS
        || payload_bytes > MAX_CONNECTOR_WRITE_OPERATION_PAYLOAD_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write operation exceeds its aggregate writer or payload budget",
        ));
    }
    Ok(())
}

fn cohort_set_digest(
    operation_id: ConnectorWriteOperationId,
    cohorts: &[ConnectorWriteCohortDescriptor],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_COHORT_SET_DOMAIN);
    hasher.update(operation_id.to_bytes());
    hasher.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        hasher.update(cohort.cohort_id.to_bytes());
        hasher.update([write_intent_tag(cohort.intent)]);
        hasher.update(cohort.planning_digest);
    }
    hasher.finalize().into()
}

#[allow(clippy::too_many_arguments)]
fn attempt_completion_digest(
    owner: &ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
    cohort_id: ConnectorWriteCohortId,
    execution_id: ConnectorWriteExecutionId,
    manifest_digest: [u8; 32],
    reports: &[ConnectorStagedReport],
    control_payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_ATTEMPT_DOMAIN);
    digest_owner(&mut hasher, owner);
    hasher.update(operation_id.to_bytes());
    hasher.update(cohort_id.to_bytes());
    hasher.update(execution_id.query_id());
    hasher.update(execution_id.attempt_id().to_be_bytes());
    hasher.update(manifest_digest);
    digest_bytes(&mut hasher, control_payload);
    hasher.update((reports.len() as u64).to_be_bytes());
    for report in reports {
        digest_writer(&mut hasher, report.writer());
        hasher.update(report.version.to_be_bytes());
        hasher.update([writer_terminal_state_tag(report.state)]);
        hasher.update(report.summary.input_rows.to_be_bytes());
        hasher.update(report.summary.staged_bytes.to_be_bytes());
        hasher.update(report.summary.artifact_count.to_be_bytes());
        hasher.update(report.payload_digest);
    }
    hasher.finalize().into()
}

fn operation_completion_digest(
    owner: &ConnectorExecutionBindingKey,
    sealed: &ConnectorSealedWriteCohortSet,
    cohorts: &[ConnectorWriteCohortCompletion],
) -> [u8; 32] {
    let mut cohorts = cohorts.iter().collect::<Vec<_>>();
    cohorts.sort_by_key(|cohort| cohort.cohort_id);
    let mut hasher = Sha256::new();
    hasher.update(CONNECTOR_WRITE_OPERATION_DOMAIN);
    digest_owner(&mut hasher, owner);
    hasher.update(sealed.operation_id.to_bytes());
    hasher.update(sealed.digest);
    hasher.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        hasher.update(cohort.cohort_id.to_bytes());
        match &cohort.accepted {
            Some(accepted) => {
                hasher.update([1]);
                hasher.update(accepted.digest);
            }
            None => hasher.update([0]),
        }
        hasher.update((cohort.superseded.len() as u64).to_be_bytes());
        for superseded in &cohort.superseded {
            hasher.update(superseded.digest);
        }
    }
    hasher.finalize().into()
}

fn digest_owner(hasher: &mut Sha256, owner: &ConnectorExecutionBindingKey) {
    digest_bytes(hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
}

fn digest_writer(hasher: &mut Sha256, writer: &ConnectorWriterIdentity) {
    hasher.update(writer.operation_id.to_bytes());
    hasher.update(writer.cohort_id.to_bytes());
    hasher.update(writer.execution_id.query_id());
    hasher.update(writer.execution_id.attempt_id().to_be_bytes());
    hasher.update(writer.fragment_instance_id);
    hasher.update(writer.fragment_id.to_be_bytes());
    hasher.update(writer.backend_num.to_be_bytes());
    hasher.update(writer.sink_ordinal.to_be_bytes());
    digest_owner(hasher, &writer.binding_key);
}

fn digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn validate_input_request(request: &ConnectorWriteInputRequest) -> Result<(), ConnectorError> {
    let fields: Vec<&ConnectorWriteFieldRequest> = match request {
        ConnectorWriteInputRequest::Data { fields } => fields.iter().collect(),
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => data_fields.iter().chain(row_identity_fields).collect(),
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => identity_fields
            .iter()
            .chain(partition_source_fields)
            .collect(),
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            equality_fields.iter().collect()
        }
    };
    if fields.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write input request must contain at least one field",
        ));
    }
    let mut names = HashSet::new();
    if fields
        .iter()
        .any(|field| !names.insert(field.field.name().to_owned()))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write input request contains a duplicate field name",
        ));
    }
    Ok(())
}

fn preparation_digest(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    target_ref: &ConnectorWriteTargetRef,
    intent: ConnectorWriteIntent,
    base_version: &ConnectorWriteBaseVersion,
    input: &ConnectorWriteInputShape,
    payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-write-preparation.v1\0");
    digest_owner(&mut hasher, owner);
    digest_bytes(&mut hasher, table.owner().as_str().as_bytes());
    digest_bytes(&mut hasher, table.payload());
    digest_bytes(&mut hasher, target_ref.as_str().as_bytes());
    hasher.update([write_intent_tag(intent)]);
    hasher.update(base_version.digest);
    match input {
        ConnectorWriteInputShape::Data { fields } => {
            hasher.update([1]);
            digest_bound_fields(&mut hasher, fields);
        }
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => {
            hasher.update([2]);
            digest_bound_fields(&mut hasher, data_fields);
            digest_bound_fields(&mut hasher, row_identity_fields);
        }
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => {
            hasher.update([3]);
            digest_bound_fields(&mut hasher, identity_fields);
            digest_bound_fields(&mut hasher, partition_source_fields);
        }
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => {
            hasher.update([4]);
            digest_bound_fields(&mut hasher, identity_fields);
            digest_bound_fields(&mut hasher, partition_source_fields);
        }
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
            hasher.update([5]);
            digest_bound_fields(&mut hasher, equality_fields);
        }
    }
    digest_bytes(&mut hasher, payload);
    hasher.finalize().into()
}

fn digest_bound_fields(hasher: &mut Sha256, fields: &[ConnectorWriteFieldBinding]) {
    hasher.update((fields.len() as u64).to_be_bytes());
    for field in fields {
        hasher.update(field.token.to_bytes());
        digest_bytes(hasher, format!("{:?}", field.field).as_bytes());
    }
}

const fn write_intent_tag(intent: ConnectorWriteIntent) -> u8 {
    match intent {
        ConnectorWriteIntent::Append => 1,
        ConnectorWriteIntent::Overwrite => 2,
        ConnectorWriteIntent::PartitionOverwrite => 3,
        ConnectorWriteIntent::RowDelta => 4,
    }
}

const fn writer_terminal_state_tag(state: ConnectorWriterTerminalState) -> u8 {
    match state {
        ConnectorWriterTerminalState::Staged => 1,
        ConnectorWriterTerminalState::Aborted => 2,
        ConnectorWriterTerminalState::Failed => 3,
    }
}

fn sha256(payload: &Bytes) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
    };

    struct NotCancelled;
    impl ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn key() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("unit").expect("instance ID"),
            incarnation: ConnectorInstanceIncarnation::new(),
        }
    }

    fn writer() -> ConnectorWriterIdentity {
        let operation_id = ConnectorWriteOperationId::new();
        ConnectorWriterIdentity::new(
            operation_id,
            ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([1; 16], 2),
            [3; 16],
            4,
            5,
            0,
            key(),
        )
    }

    fn base_facts() -> Vec<super::super::ConnectorStagedPublicationBaseFact> {
        vec![super::super::ConnectorStagedPublicationBaseFact {
            table: Arc::from("db.base"),
            uuid: Arc::from("base-uuid"),
            from_version: Some(10),
            to_version: 11,
        }]
    }

    fn partition_fields() -> Vec<ConnectorManagedPartitionField> {
        vec![
            ConnectorManagedPartitionField::try_new(7, 0, ConnectorManagedPartitionTransform::Day)
                .expect("day field"),
            ConnectorManagedPartitionField::try_new(
                9,
                1,
                ConnectorManagedPartitionTransform::Bucket { buckets: 16 },
            )
            .expect("bucket field"),
        ]
    }

    fn committed_partition_fields() -> Vec<ConnectorCommittedPartitionField> {
        vec![
            ConnectorCommittedPartitionField::try_new(
                1000,
                "event_day",
                7,
                "event_time",
                0,
                ConnectorManagedPartitionTransform::Day,
            )
            .expect("committed day field"),
            ConnectorCommittedPartitionField::try_new(
                1001,
                "account_bucket",
                9,
                "account_id",
                1,
                ConnectorManagedPartitionTransform::Bucket { buckets: 16 },
            )
            .expect("committed bucket field"),
        ]
    }

    #[test]
    fn operation_id_round_trips_through_durable_attempt_text() {
        let operation_id = ConnectorWriteOperationId::new();
        let parsed: ConnectorWriteOperationId = operation_id
            .to_string()
            .parse()
            .expect("UUID v7 attempt text must round-trip");
        assert_eq!(parsed, operation_id);
    }

    #[test]
    fn managed_partition_replacement_is_bounded_ordered_and_operation_scoped() {
        assert!(
            ConnectorManagedPartitionField::try_new(
                1,
                0,
                ConnectorManagedPartitionTransform::Bucket { buckets: 0 },
            )
            .is_err()
        );
        let duplicate = vec![
            ConnectorManagedPartitionField::try_new(7, 0, ConnectorManagedPartitionTransform::Day)
                .unwrap(),
            ConnectorManagedPartitionField::try_new(7, 1, ConnectorManagedPartitionTransform::Day)
                .unwrap(),
        ];
        let prior = ConnectorManagedPartitionSpecObservation::try_from_fields(0, &[])
            .expect("unpartitioned prior spec is observable");
        assert!(
            ConnectorManagedPartitionSpecReplacement::try_new(
                ConnectorWriteOperationId::new(),
                prior,
                duplicate,
            )
            .is_err()
        );
        let same_source_different_transforms = vec![
            ConnectorManagedPartitionField::try_new(7, 0, ConnectorManagedPartitionTransform::Day)
                .unwrap(),
            ConnectorManagedPartitionField::try_new(7, 1, ConnectorManagedPartitionTransform::Hour)
                .unwrap(),
        ];
        assert!(
            ConnectorManagedPartitionSpecReplacement::try_new(
                ConnectorWriteOperationId::new(),
                prior,
                same_source_different_transforms,
            )
            .is_ok()
        );
        assert!(
            ConnectorManagedPartitionSpecReplacement::try_new(
                ConnectorWriteOperationId::new(),
                prior,
                Vec::new(),
            )
            .is_err()
        );

        let operation_id = ConnectorWriteOperationId::new();
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            operation_id,
            prior,
            partition_fields(),
        )
        .expect("replacement");
        assert_eq!(
            replacement.replacement_id(),
            ConnectorManagedPartitionSpecReplacementId::derive(operation_id)
        );
        assert_eq!(
            replacement.target(),
            ConnectorManagedPartitionSpecReplacementTarget::MainPublication
        );
        replacement
            .validate_for_operation(operation_id)
            .expect("exact operation");
        assert!(
            replacement
                .validate_for_operation(ConnectorWriteOperationId::new())
                .is_err()
        );
    }

    #[test]
    fn managed_partition_replacement_is_signed_without_changing_ordinary_digest() {
        let ordinary = ConnectorManagedPublicationIntent::try_new(
            1,
            2,
            "marker",
            ConnectorManagedPublicationTechnique::Full,
            base_facts(),
            "definition",
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        )
        .expect("ordinary managed publication");
        let mut legacy = Sha256::new();
        legacy.update(b"novarocks.connector-managed-publication-intent.v1\0");
        legacy.update(1_i64.to_be_bytes());
        legacy.update(2_i64.to_be_bytes());
        digest_bytes(&mut legacy, b"marker");
        legacy.update([1]);
        digest_bytes(&mut legacy, b"db.base");
        digest_bytes(&mut legacy, b"base-uuid");
        legacy.update(10_i64.to_be_bytes());
        legacy.update(11_i64.to_be_bytes());
        digest_bytes(&mut legacy, b"definition");
        legacy.update([2]);
        let legacy_digest: [u8; 32] = legacy.finalize().into();
        assert_eq!(ordinary.digest(), legacy_digest);
        assert!(ordinary.partition_spec_replacement().is_none());

        let operation_id = ConnectorWriteOperationId::new();
        let prior =
            ConnectorManagedPartitionSpecObservation::try_from_fields(3, &partition_fields())
                .expect("prior observation");
        assert_ne!(
            prior,
            ConnectorManagedPartitionSpecObservation::try_from_fields(4, &partition_fields())
                .expect("same layout under another prior spec")
        );
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            operation_id,
            prior,
            vec![
                ConnectorManagedPartitionField::try_new(
                    7,
                    0,
                    ConnectorManagedPartitionTransform::Month,
                )
                .unwrap(),
            ],
        )
        .expect("replacement");
        let repartition =
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                1,
                2,
                "marker",
                ConnectorManagedPublicationTechnique::Full,
                base_facts(),
                "definition",
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                replacement,
            )
            .expect("repartition intent");
        assert_ne!(repartition.digest(), ordinary.digest());
        repartition
            .validate_for_operation(operation_id)
            .expect("replacement operation is signed");
        assert!(
            repartition
                .validate_for_operation(ConnectorWriteOperationId::new())
                .is_err()
        );
    }

    #[test]
    fn managed_partition_replacement_rejects_non_atomic_publication_modes() {
        let operation_id = ConnectorWriteOperationId::new();
        let replacement = ConnectorManagedPartitionSpecReplacement::try_new(
            operation_id,
            ConnectorManagedPartitionSpecObservation::try_from_fields(0, &[]).unwrap(),
            partition_fields(),
        )
        .unwrap();
        assert!(
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                1,
                2,
                "marker",
                ConnectorManagedPublicationTechnique::Incremental,
                base_facts(),
                "definition",
                ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
                replacement.clone(),
            )
            .is_err()
        );
        assert!(
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                1,
                2,
                "marker",
                ConnectorManagedPublicationTechnique::Full,
                base_facts(),
                "definition",
                ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit,
                replacement,
            )
            .is_err()
        );
    }

    #[test]
    fn report_frames_are_bounded_and_digest_stable() {
        let report = ConnectorStagedReport::try_new(
            writer(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from(vec![9; MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES + 1]),
        )
        .expect("report");
        let frames = report.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].part_index(), 0);
        assert_eq!(frames[1].part_index(), 1);
        assert_eq!(frames[0].logical_payload_digest(), report.payload_digest());
        assert!(frames.iter().all(|frame| frame.validate().is_ok()));
    }

    #[test]
    fn report_reassembly_accepts_retry_duplicates_and_rejects_gaps() {
        let report = ConnectorStagedReport::try_new(
            writer(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from(vec![9; MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES + 1]),
        )
        .expect("report");
        let frames = report.frames();
        let reassembled = ConnectorStagedReport::try_from_frames(vec![
            frames[1].clone(),
            frames[0].clone(),
            frames[0].clone(),
        ])
        .expect("identical retry");
        assert_eq!(reassembled, report);

        let error = ConnectorStagedReport::try_from_frames(vec![frames[0].clone()])
            .expect_err("missing final frame");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn plan_rejects_conflicting_writer_manifest() {
        let writer = writer();
        let handle = ConnectorWriterHandle::try_new(
            writer.binding_key().clone(),
            writer.clone(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            Bytes::new(),
        )
        .expect("handle");
        let error = ConnectorWritePlan::try_new(
            writer.binding_key().clone(),
            writer.operation_id(),
            writer.cohort_id(),
            writer.execution_id(),
            vec![handle.clone(), handle],
            Bytes::new(),
        )
        .expect_err("duplicate writer must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn planning_request_requires_exact_writer_owner() {
        let writer = writer();
        let owner = writer.binding_key().clone();
        let table =
            ConnectorTableHandle::try_new(writer.binding_key().instance_id.clone(), Bytes::new())
                .expect("table handle");
        let preparation = ConnectorWritePreparation::try_new(
            owner.clone(),
            table,
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).expect("base version"),
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([7; 32]),
                    Field::new("x", DataType::Int64, true),
                )],
            },
            Bytes::new(),
        )
        .expect("preparation");
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NotCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("context");
        let activation_request = ConnectorWriteActivationRequest {
            operation_id: writer.operation_id(),
            source: ConnectorWriteActivationSource::Prepared(preparation.clone()),
            intent: ConnectorWriteActivationIntent::Ordinary,
            context: context.clone(),
        };
        let activation = ConnectorWriteActivation::try_new(
            owner.clone(),
            &activation_request,
            vec![(writer.cohort_id(), preparation)],
        )
        .expect("activation");
        let request = ConnectorWritePlanningRequest {
            operation_id: writer.operation_id(),
            cohort_id: writer.cohort_id(),
            execution_id: writer.execution_id(),
            activation: activation.cohort(writer.cohort_id()).expect("cohort"),
            expected_writers: vec![writer],
            context,
        };
        request.validate(&owner).expect("exact writer owner");
    }

    #[test]
    fn preparation_rejects_foreign_or_duplicate_field_tokens() {
        let owner = key();
        let foreign_table = ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("foreign").expect("foreign instance"),
            Bytes::new(),
        )
        .expect("foreign table");
        let error = ConnectorWritePreparation::try_new(
            owner.clone(),
            foreign_table,
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            ConnectorWriteBaseVersion::try_new(Bytes::new()).expect("base"),
            ConnectorWriteInputShape::Data {
                fields: vec![ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("x", DataType::Int64, true),
                )],
            },
            Bytes::new(),
        )
        .err()
        .expect("foreign table must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let shape = ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("x", DataType::Int64, true),
                ),
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("y", DataType::Int64, true),
                ),
            ],
        };
        assert_eq!(
            shape.validate().expect_err("duplicate token").kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn receipt_wire_round_trips_without_exposing_payload() {
        let receipt = ConnectorWriteReceipt::try_new_with_committed_facts(
            Bytes::from_static(b"provider receipt"),
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"version"), Some(7))
                .expect("version"),
            Some(13),
        )
        .expect("receipt");
        let decoded =
            ConnectorWriteReceipt::try_from_wire_v1(&receipt.try_to_wire_v1().expect("wire"))
                .expect("decode receipt");
        assert_eq!(decoded, receipt);

        let partitioning = ConnectorCommittedPartitioning::try_new(4, committed_partition_fields())
            .expect("committed partitioning");
        let receipt = ConnectorWriteReceipt::try_new_with_committed_facts_and_partitioning(
            Bytes::from_static(b"provider repartition receipt"),
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"version-2"), Some(8))
                .expect("version"),
            Some(14),
            partitioning.clone(),
        )
        .expect("repartition receipt");
        let decoded =
            ConnectorWriteReceipt::try_from_wire_v1(&receipt.try_to_wire_v1().expect("wire"))
                .expect("decode repartition receipt");
        assert_eq!(decoded, receipt);
        assert_eq!(decoded.committed_partitioning(), Some(&partitioning));
    }

    #[test]
    fn sealed_cohort_set_is_sorted_and_rejects_duplicates() {
        let operation_id = ConnectorWriteOperationId::new();
        let first = ConnectorWriteCohortId::derive(operation_id, b"rewrite", [1; 32])
            .expect("first cohort");
        let second = ConnectorWriteCohortId::derive(operation_id, b"rewrite", [2; 32])
            .expect("second cohort");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![
                ConnectorWriteCohortDescriptor::new(
                    second,
                    ConnectorWriteIntent::RowDelta,
                    [4; 32],
                ),
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
            ],
        )
        .expect("sealed cohorts");
        assert!(sealed.cohorts()[0].cohort_id() < sealed.cohorts()[1].cohort_id());

        let duplicate = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
                ConnectorWriteCohortDescriptor::new(first, ConnectorWriteIntent::RowDelta, [3; 32]),
            ],
        )
        .expect_err("duplicate cohort");
        assert_eq!(duplicate.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn operation_completion_rejects_missing_and_mixed_attempts() {
        let writer = writer();
        let operation_id = writer.operation_id();
        let cohort_id = writer.cohort_id();
        let owner = writer.binding_key().clone();
        let report = ConnectorStagedReport::try_new(
            writer.clone(),
            CONNECTOR_WRITE_CONTRACT_VERSION,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary::default(),
            Bytes::from_static(b"report"),
        )
        .expect("report");
        let accepted = ConnectorWriteAttemptCompletion::try_new(
            owner.clone(),
            operation_id,
            cohort_id,
            writer.execution_id(),
            [7; 32],
            vec![report],
            Bytes::new(),
        )
        .expect("accepted attempt");
        let sealed = ConnectorSealedWriteCohortSet::try_new(
            operation_id,
            vec![ConnectorWriteCohortDescriptor::new(
                cohort_id,
                ConnectorWriteIntent::Append,
                [8; 32],
            )],
        )
        .expect("sealed");
        let missing =
            ConnectorWriteOperationCompletion::try_new(owner.clone(), sealed.clone(), Vec::new())
                .expect_err("missing cohort");
        assert_eq!(missing.kind(), ConnectorErrorKind::InvalidRequest);

        let cohort = ConnectorWriteCohortCompletion::try_new(
            cohort_id,
            Some(accepted.clone()),
            vec![accepted],
        )
        .expect_err("same attempt cannot be accepted and superseded");
        assert_eq!(cohort.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn receipt_rejects_oversized_payload() {
        let error = ConnectorWriteReceipt::try_new(Bytes::from(vec![
            0;
            MAX_CONNECTOR_WRITE_RECEIPT_BYTES
                + 1
        ]))
        .expect_err("receipt limit");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
}
