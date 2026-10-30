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

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use arrow::datatypes::Field;
use bytes::Bytes;
use sha2::{Digest, Sha256};

pub use novarocks_connector_contract::ConnectorWriteFieldToken;
use uuid::Uuid;

use super::{
    CatalogProperties, ConnectorCommittedVersion, ConnectorControlRuntimeId, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionDistribution, ConnectorMutationFailure,
    ConnectorProviderBinding, ConnectorProviderBindingKey, ConnectorProviderId,
    ConnectorRequestContext, ConnectorTableHandle, ExternalMutationEvidence,
    ExternalMutationFinalization, LakePublicationFamily, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES, MvExactPartitionField, MvExactPartitionTransform,
};

pub const MAX_CONNECTOR_WRITE_RECEIPT_BYTES: usize = MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES;
pub const MAX_CONNECTOR_WRITE_COHORTS: usize = 4096;
pub const MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS: usize = 4096;
pub const MAX_CONNECTOR_MANAGED_PARTITION_FIELD_TEXT_BYTES: usize = 4096;
pub const DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_ENTRIES: usize = 16_384;

const CONNECTOR_WRITE_COHORT_ID_DOMAIN: &[u8] = b"novarocks.connector-write-cohort.v1\0";
const CONNECTOR_WRITE_COHORT_SET_DOMAIN: &[u8] = b"novarocks.connector-write-cohort-set.v1\0";
const CONNECTOR_MANAGED_PARTITION_SPEC_OBSERVATION_DOMAIN: &[u8] =
    b"novarocks.connector-managed-partition-spec-observation.v1\0";
const CONNECTOR_MANAGED_PARTITION_SPEC_REPLACEMENT_ID_DOMAIN: &[u8] =
    b"novarocks.connector-managed-partition-spec-replacement-id.v1\0";

#[path = "write_evidence_failure.rs"]
mod evidence_failure;
use evidence_failure::WriteEvidenceFailure;

/// Resolved per-fragment bounds for evidence returned by a distributed write.
///
/// This is intentionally an application input rather than a connector wire
/// limit. Individual reports retain their independent SPI framing bounds; the
/// ledger combines connector frames with tablet terminal facts for one
/// fragment instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteCommitEvidenceLimits {
    max_bytes: usize,
    max_entries: usize,
}

impl WriteCommitEvidenceLimits {
    pub fn try_new(max_bytes: usize, max_entries: usize) -> Result<Self, ConnectorError> {
        if max_bytes == 0 || max_entries == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write commit evidence byte and entry limits must be greater than zero",
            ));
        }
        Ok(Self {
            max_bytes,
            max_entries,
        })
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub const fn max_entries(self) -> usize {
        self.max_entries
    }
}

impl Default for WriteCommitEvidenceLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_BYTES,
            max_entries: DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_ENTRIES,
        }
    }
}

/// Fragment-local reserve-before-publish budget shared by all write evidence.
#[derive(Clone, Debug)]
pub struct WriteCommitEvidenceLedger {
    limits: WriteCommitEvidenceLimits,
    usage: Arc<Mutex<WriteCommitEvidenceUsage>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteCommitEvidenceUsage {
    bytes: usize,
    entries: usize,
}

impl WriteCommitEvidenceUsage {
    pub const fn bytes(self) -> usize {
        self.bytes
    }

    pub const fn entries(self) -> usize {
        self.entries
    }
}

impl WriteCommitEvidenceLedger {
    pub fn new(limits: WriteCommitEvidenceLimits) -> Self {
        Self {
            limits,
            usage: Arc::new(Mutex::new(WriteCommitEvidenceUsage::default())),
        }
    }

    /// Reserve before the caller makes evidence visible to a later terminal
    /// report. A failed reservation leaves the ledger unchanged.
    pub fn reserve(&self, bytes: usize, entries: usize) -> Result<(), ConnectorError> {
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| WriteEvidenceFailure::Poisoned.into_connector())?;
        let next_bytes = usage.bytes.checked_add(bytes).ok_or_else(|| {
            WriteEvidenceFailure::Arithmetic {
                dimension: novarocks_failure_contract::ResourceDimension::Bytes,
                requested: bytes,
                committed: usage.bytes,
                limit: self.limits.max_bytes,
            }
            .into_connector()
        })?;
        let next_entries = usage.entries.checked_add(entries).ok_or_else(|| {
            WriteEvidenceFailure::Arithmetic {
                dimension: novarocks_failure_contract::ResourceDimension::Items,
                requested: entries,
                committed: usage.entries,
                limit: self.limits.max_entries,
            }
            .into_connector()
        })?;
        if next_bytes > self.limits.max_bytes || next_entries > self.limits.max_entries {
            return Err(WriteEvidenceFailure::Budget {
                requested_bytes: bytes,
                requested_entries: entries,
                committed: *usage,
                limits: self.limits,
            }
            .into_connector());
        }
        *usage = WriteCommitEvidenceUsage {
            bytes: next_bytes,
            entries: next_entries,
        };
        Ok(())
    }

    pub const fn limits(&self) -> WriteCommitEvidenceLimits {
        self.limits
    }

    pub fn usage(&self) -> Result<WriteCommitEvidenceUsage, ConnectorError> {
        self.usage
            .lock()
            .map(|usage| *usage)
            .map_err(|_| WriteEvidenceFailure::Poisoned.into_connector())
    }

    pub fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.usage, &other.usage)
    }
}

impl Default for WriteCommitEvidenceLedger {
    fn default() -> Self {
        Self::new(WriteCommitEvidenceLimits::default())
    }
}

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

impl From<super::LakePublicationId> for ConnectorWriteOperationId {
    fn from(publication_id: super::LakePublicationId) -> Self {
        Self::from_bytes(publication_id.to_bytes())
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

#[derive(Clone, Debug)]
pub struct ConnectorWriteFieldRequest {
    field: Field,
    execution_type: Option<novarocks_type_contract::FunctionValueType>,
}

impl ConnectorWriteFieldRequest {
    pub fn new(field: Field) -> Self {
        Self {
            field,
            execution_type: None,
        }
    }

    /// Carry the SQL/provider author's complete value binding separately from
    /// the original provider field. The legacy constructor remains only while
    /// production request authors migrate to this explicit projection.
    pub fn try_new_typed(
        field: Field,
        execution_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        let matches = novarocks_connector_contract::write_field_matches_execution_type_observed::<
            novarocks_type_contract::ValueTypeError,
        >(&field, &execution_type, |_| Ok(()))
        .map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        if !matches {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "writer request source field conflicts with its explicit execution projection",
            ));
        }
        Ok(Self {
            field,
            execution_type: Some(execution_type),
        })
    }

    pub fn execution_type(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.execution_type.as_ref()
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

pub use novarocks_connector_contract::ConnectorWriteFieldBinding;

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

pub use novarocks_connector_contract::ConnectorWriteInputShape;

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
    pub fn validate(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
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

    pub(super) fn validate(&self) -> Result<(), ConnectorError> {
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
    owner: ConnectorProviderBindingKey,
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
        owner: ConnectorProviderBindingKey,
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

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
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
        owner: &ConnectorProviderBindingKey,
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
    /// A refresh whose inputs did not move, published as a new output version
    /// that carries the same rows.
    ///
    /// It is a real publication, not a skipped one: the watermark it advances
    /// is what later refreshes read back, so it has to be stated somewhere the
    /// lake keeps. Publishing it as `Incremental` would make the published
    /// provenance say a window was consumed when none was, and consumers
    /// already distinguish the two.
    MetadataOnly,
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

    pub(crate) fn validate_for_operation(
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
}

/// A read-only request to derive the provider-assigned physical identities
/// that an atomic managed partition replacement would produce. The request
/// carries an opaque target handle from the same retained control generation;
/// callers cannot manufacture provider metadata or physical field IDs.
#[derive(Clone)]
pub struct ConnectorManagedPartitionSpecPreviewRequest {
    operation_id: ConnectorWriteOperationId,
    table: super::ConnectorTableHandle,
    replacement: ConnectorManagedPartitionSpecReplacement,
    context: ConnectorRequestContext,
}

impl ConnectorManagedPartitionSpecPreviewRequest {
    pub fn new(
        operation_id: ConnectorWriteOperationId,
        table: super::ConnectorTableHandle,
        replacement: ConnectorManagedPartitionSpecReplacement,
        context: ConnectorRequestContext,
    ) -> Self {
        Self {
            operation_id,
            table,
            replacement,
            context,
        }
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn table(&self) -> &super::ConnectorTableHandle {
        &self.table
    }

    pub const fn replacement(&self) -> &ConnectorManagedPartitionSpecReplacement {
        &self.replacement
    }

    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub fn validate(&self, owner: &ConnectorProviderBindingKey) -> Result<(), ConnectorError> {
        if self.table.owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "managed partition preview table does not belong to the exact connector instance",
            ));
        }
        self.replacement.validate_for_operation(self.operation_id)
    }
}

/// The exact provider-assigned partitioning predicted from one frozen target.
/// This is a preparation fact, not a committed result: activation must derive
/// it again from the same admitted table and reject any mismatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorManagedPartitionSpecPreview {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorWriteOperationId,
    committed_partitioning: ConnectorCommittedPartitioning,
    exact_partition_spec_version: Bytes,
    exact_partition_fields: Vec<MvExactPartitionField>,
}

impl ConnectorManagedPartitionSpecPreview {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        operation_id: ConnectorWriteOperationId,
        committed_partitioning: ConnectorCommittedPartitioning,
        exact_partition_spec_version: Bytes,
        exact_partition_fields: Vec<MvExactPartitionField>,
    ) -> Result<Self, ConnectorError> {
        committed_partitioning.validate()?;
        validate_exact_preview_partitioning(
            &committed_partitioning,
            &exact_partition_spec_version,
            &exact_partition_fields,
        )?;
        Ok(Self {
            owner,
            operation_id,
            committed_partitioning,
            exact_partition_spec_version,
            exact_partition_fields,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub const fn committed_partitioning(&self) -> &ConnectorCommittedPartitioning {
        &self.committed_partitioning
    }

    pub const fn exact_partition_spec_version(&self) -> &Bytes {
        &self.exact_partition_spec_version
    }

    pub fn exact_partition_fields(&self) -> &[MvExactPartitionField] {
        &self.exact_partition_fields
    }

    fn validate_for_request(
        &self,
        owner: &ConnectorProviderBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<(), ConnectorError> {
        self.committed_partitioning.validate()?;
        validate_exact_preview_partitioning(
            &self.committed_partitioning,
            &self.exact_partition_spec_version,
            &self.exact_partition_fields,
        )?;
        if &self.owner != owner || self.operation_id != operation_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "managed partition preview does not retain the exact lease generation or operation",
            ));
        }
        Ok(())
    }
}

fn validate_exact_preview_partitioning(
    committed: &ConnectorCommittedPartitioning,
    spec_version: &Bytes,
    exact_fields: &[MvExactPartitionField],
) -> Result<(), ConnectorError> {
    if spec_version.is_empty()
        || spec_version.len() > 1024
        || exact_fields.len() != committed.fields().len()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "managed partition preview has an invalid exact partition binding",
        ));
    }
    let mut identities = HashSet::with_capacity(exact_fields.len());
    for (committed_field, exact_field) in committed.fields().iter().zip(exact_fields) {
        MvExactPartitionField::try_new(
            exact_field.partition_field_id().clone(),
            exact_field.source_target_field_id().clone(),
            exact_field.transform().clone(),
        )?;
        let expected_transform = match committed_field.transform() {
            ConnectorManagedPartitionTransform::Identity => MvExactPartitionTransform::Identity,
            ConnectorManagedPartitionTransform::Year => MvExactPartitionTransform::Year,
            ConnectorManagedPartitionTransform::Month => MvExactPartitionTransform::Month,
            ConnectorManagedPartitionTransform::Day => MvExactPartitionTransform::Day,
            ConnectorManagedPartitionTransform::Hour => MvExactPartitionTransform::Hour,
            ConnectorManagedPartitionTransform::Bucket { buckets } => {
                MvExactPartitionTransform::Bucket {
                    num_buckets: buckets,
                }
            }
            ConnectorManagedPartitionTransform::Truncate { width } => {
                MvExactPartitionTransform::Truncate { width }
            }
            ConnectorManagedPartitionTransform::Void => MvExactPartitionTransform::Void,
        };
        if !identities.insert(exact_field.partition_field_id().as_ref())
            || exact_field.transform() != &expected_transform
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "managed partition preview exact fields do not match committed partitioning",
            ));
        }
    }
    Ok(())
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteActivationIntent {
    Ordinary,
    Publication(LakePublicationFamily),
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
        owner: &ConnectorProviderBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        self.source.validate(owner, self.operation_id)
    }
}

/// Provider-owned request for a proof that the activation and placement-plan
/// steps for one write operation cannot create an external effect before every
/// participant reaches ControlReady. It is intentionally separate from a
/// write activation: Frontend must ask before it relies on a topology retry.
#[derive(Clone)]
pub struct ConnectorPreReadyWritePlanningRequest {
    activation: ConnectorWriteActivationRequest,
}

impl ConnectorPreReadyWritePlanningRequest {
    pub fn new(activation: ConnectorWriteActivationRequest) -> Self {
        Self { activation }
    }

    pub fn activation(&self) -> &ConnectorWriteActivationRequest {
        &self.activation
    }

    pub fn validate(
        &self,
        owner: &ConnectorProviderBindingKey,
    ) -> Result<[u8; 32], ConnectorError> {
        self.activation.validate(owner)
    }
}

/// A non-durable, exact-generation proof issued only by the provider control
/// that owns a write operation. The default Connector SPI implementation does
/// not issue this proof, so unknown providers fail closed for pre-ready DML
/// retry instead of inheriting an Iceberg-specific assumption.
#[derive(Clone)]
pub struct ConnectorPreReadyWritePlanningProof {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorWriteOperationId,
    activation_source_digest: [u8; 32],
}

impl ConnectorPreReadyWritePlanningProof {
    pub fn try_issue(
        owner: ConnectorProviderBindingKey,
        request: &ConnectorPreReadyWritePlanningRequest,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            operation_id: request.activation.operation_id,
            activation_source_digest: request.validate(&owner)?,
            owner,
        })
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }

    pub fn validates(
        &self,
        owner: &ConnectorProviderBindingKey,
        request: &ConnectorPreReadyWritePlanningRequest,
    ) -> Result<(), ConnectorError> {
        if &self.owner != owner || self.operation_id != request.activation.operation_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "pre-ready write-planning proof does not match its exact control owner",
            ));
        }
        if self.activation_source_digest != request.validate(owner)? {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "pre-ready write-planning proof does not match its activation request",
            ));
        }
        Ok(())
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
pub enum ConnectorWriteAbortOutcome {
    KnownUncommitted {
        cleanup: ExternalMutationFinalization,
    },
    KnownCommitted {
        /// The same exact business fact retained by the original session.
        needed: std::sync::Arc<crate::connector::write_stack::ConnectorWriteNeededReady>,
        cleanup: crate::connector::write_stack::ConnectorWriteCleanupState,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

/// Provider write *admission*, and nothing else.
///
/// The name is wider than the contract: this trait signs the contracts SQL
/// reads before a statement is planned, and runs the provider's own
/// write-support guards while doing so. It admits a write; it does not perform
/// one.
///
/// It therefore carries no writer placement planning, no commit, abort, or
/// reconciliation authority, and no staged-report vocabulary. The write session
/// (`write_stack::ConnectorWriteControl`) owns every one of those, and is the
/// only path by which an external effect is committed.
pub trait ConnectorWriteControl: Send + Sync {
    fn binding_key(&self) -> &ConnectorProviderBindingKey;

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

    /// Preview one managed partition replacement from the caller's exact
    /// admitted target. This must not reserve a writer, mutate catalog state,
    /// or resolve a later connector generation.
    fn preview_managed_partition_spec(
        &self,
        _request: ConnectorManagedPartitionSpecPreviewRequest,
    ) -> Result<ConnectorManagedPartitionSpecPreview, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not implement managed partition preview",
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

    /// Issue an operation-scoped proof only when this provider guarantees that
    /// the matching activation and all later `plan_write` calls remain free of
    /// staging, object, catalog, publication, and other external effects until
    /// ControlReady. Providers that cannot prove this must keep the default
    /// rejection; Frontend then reports `TopologyRetryUnsupported` for DML.
    fn certify_pre_ready_write_planning(
        &self,
        _request: ConnectorPreReadyWritePlanningRequest,
    ) -> Result<ConnectorPreReadyWritePlanningProof, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector write control does not prove effect-free pre-ready planning",
        ))
    }
}

#[derive(Clone)]
pub struct ConnectorWriteLease {
    /// FE-local effect owner.  This identity never crosses the native BE
    /// boundary and is deliberately independent of the BE-visible catalog
    /// handle and the provider-private generation key below.
    control_runtime_id: ConnectorControlRuntimeId,
    /// Provider-private proof used only to validate opaque provider facts.
    /// Frontend application code must use `control_runtime_id` for ownership;
    /// it must not select a provider generation directly.
    provider_binding_key: ConnectorProviderBindingKey,
    /// Exact immutable BE runtime materialization input. This remains separate
    /// from the FE effect-generation key that fences commit/abort.
    catalog_properties: Option<CatalogProperties>,
    control: Arc<dyn ConnectorWriteControl>,
    execution_provider_id: Option<ConnectorProviderId>,
    execution_distribution: Option<Arc<dyn ConnectorExecutionDistribution>>,
    metadata: Option<Arc<dyn super::ConnectorMetadata>>,
    _release: Arc<ConnectorWriteLeaseRelease>,
}

struct ConnectorWriteLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorWriteLease {
    pub fn new(
        binding_key: ConnectorProviderBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        Self::new_with_control_runtime_id(
            ConnectorControlRuntimeId::new(),
            binding_key,
            control,
            release,
        )
    }

    /// Construct a lease with an already-frozen FE control-runtime owner.
    /// Production acquisition paths must use this constructor (or the
    /// execution-distribution variant); `new` is retained for isolated
    /// provider-conformance fixtures only.
    pub fn new_with_control_runtime_id(
        control_runtime_id: ConnectorControlRuntimeId,
        binding_key: ConnectorProviderBindingKey,
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
            control_runtime_id,
            provider_binding_key: binding_key,
            catalog_properties: None,
            control,
            execution_provider_id: None,
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
        control_runtime_id: ConnectorControlRuntimeId,
        binding_key: ConnectorProviderBindingKey,
        control: Arc<dyn ConnectorWriteControl>,
        execution_provider_id: ConnectorProviderId,
        execution_distribution: Arc<dyn ConnectorExecutionDistribution>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let mut lease =
            Self::new_with_control_runtime_id(control_runtime_id, binding_key, control, release)?;
        lease.execution_provider_id = Some(execution_provider_id);
        lease.execution_distribution = Some(execution_distribution);
        Ok(lease)
    }

    /// Return the FE-local effect owner retained by this exact lease.
    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    /// Validate a provider-signed legacy fact without exposing the provider
    /// generation as an FE effect owner.
    pub fn matches_provider_binding_key(&self, key: &ConnectorProviderBindingKey) -> bool {
        key == &self.provider_binding_key
    }

    /// Validate a provider table handle without exposing an incarnation.
    pub fn matches_provider_instance(&self, instance_id: &super::ConnectorInstanceId) -> bool {
        instance_id == &self.provider_binding_key.instance_id
    }

    /// Return the provider instance name for FE-local identity construction.
    /// This deliberately does not disclose the provider incarnation.
    pub fn provider_instance_id(&self) -> &super::ConnectorInstanceId {
        &self.provider_binding_key.instance_id
    }

    /// Validate a pre-ready proof against this lease's private provider
    /// generation without requiring its caller to observe that generation.
    pub fn validate_pre_ready_write_planning_proof(
        &self,
        proof: &ConnectorPreReadyWritePlanningProof,
        request: &ConnectorPreReadyWritePlanningRequest,
    ) -> Result<(), ConnectorError> {
        proof.validates(&self.provider_binding_key, request)
    }

    /// Attach the desired-state-frozen runtime input retained by the FE
    /// control lease. Direct control-only fixtures may omit it, but a native
    /// write attachment must carry it into the query-wide Init CatalogSet.
    pub fn with_catalog_properties(
        mut self,
        catalog_properties: CatalogProperties,
    ) -> Result<Self, ConnectorError> {
        if catalog_properties.handle().catalog_name() != &self.provider_binding_key.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write catalog properties do not match the effect generation owner",
            ));
        }
        self.catalog_properties = Some(catalog_properties);
        Ok(self)
    }

    pub fn catalog_properties(&self) -> Option<&CatalogProperties> {
        self.catalog_properties.as_ref()
    }

    /// Call provider admission only after validating that the table handle
    /// belongs to this lease's private provider generation.
    pub fn prepare_write(
        &self,
        request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        request.validate(&self.provider_binding_key)?;
        let outcome = self.control.prepare_write(request)?;
        if let ConnectorWritePreparationOutcome::Prepared(preparation) = &outcome {
            preparation.validate()?;
            if preparation.owner() != &self.provider_binding_key {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector write preparation does not retain the exact lease generation",
                ));
            }
        }
        Ok(outcome)
    }

    pub fn prepare_row_mutation(
        &self,
        request: super::ConnectorRowMutationPreparationRequest,
    ) -> Result<super::ConnectorRowMutationPreparationOutcome, ConnectorError> {
        request.validate(&self.provider_binding_key)?;
        let outcome = self.control.prepare_row_mutation(request)?;
        if let super::ConnectorRowMutationPreparationOutcome::Prepared(preparation) = &outcome {
            preparation.validate()?;
            if preparation.owner() != &self.provider_binding_key {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "row-mutation preparation does not retain the lease generation",
                ));
            }
        }
        Ok(outcome)
    }

    pub fn preview_managed_partition_spec(
        &self,
        request: ConnectorManagedPartitionSpecPreviewRequest,
    ) -> Result<ConnectorManagedPartitionSpecPreview, ConnectorError> {
        request.validate(&self.provider_binding_key)?;
        let operation_id = request.operation_id();
        let preview = self.control.preview_managed_partition_spec(request)?;
        preview.validate_for_request(&self.provider_binding_key, operation_id)?;
        Ok(preview)
    }

    pub fn activate_row_mutation(
        &self,
        request: super::ConnectorRowMutationActivationRequest,
    ) -> Result<super::ConnectorRowMutationExecutionPlan, ConnectorError> {
        request.validate(&self.provider_binding_key)?;
        let expected_request = request.clone();
        let preparation = request.preparation().clone();
        let plan = self.control.activate_row_mutation(request)?;
        let contract = preparation.match_contract();
        for route in plan.routes() {
            route.validate()?;
            if route.preparation().owner() != &self.provider_binding_key {
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
        plan.validate_against_activation(&expected_request, &self.provider_binding_key)?;
        Ok(plan)
    }

    pub fn certify_pre_ready_write_planning(
        &self,
        request: ConnectorPreReadyWritePlanningRequest,
    ) -> Result<ConnectorPreReadyWritePlanningProof, ConnectorError> {
        request.validate(&self.provider_binding_key)?;
        let proof = self
            .control
            .certify_pre_ready_write_planning(request.clone())?;
        proof.validates(&self.provider_binding_key, &request)?;
        Ok(proof)
    }

    /// Return whether two leases retain the same provider control generation.
    /// Clones of one lease compare equal here; independently-derived leases
    /// compare equal only when they retain the same exact control capability.
    pub fn retains_same_generation(&self, other: &Self) -> bool {
        self.provider_binding_key == other.provider_binding_key
            && self.control_runtime_id == other.control_runtime_id
            && Arc::ptr_eq(&self.control, &other.control)
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
        creation: &mut super::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        super::ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector write lease has no exact-generation metadata capability",
            )
        })?;
        metadata.load_table(request, creation)
    }

    /// Materialize a declaration only through the exact generation held by
    /// this lease. A later active incarnation is deliberately unreachable.
    pub fn provider_binding(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        let distribution = self.execution_distribution.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write lease has no execution distribution capability",
            )
        })?;
        let provider_id = self.execution_provider_id.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write lease has no retained execution provider identity",
            )
        })?;
        let declaration = distribution.declaration(context)?;
        let key = declaration.binding_key();
        if declaration.provider_id() != provider_id || key != &self.provider_binding_key {
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

fn validate_handle_payload(payload: &Bytes) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write handle payload exceeds the hard limit",
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

fn digest_owner(hasher: &mut Sha256, owner: &ConnectorProviderBindingKey) {
    digest_bytes(hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
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
    owner: &ConnectorProviderBindingKey,
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
        hasher.update(field.token().to_bytes());
        digest_bytes(hasher, format!("{:?}", field.field()).as_bytes());
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

fn sha256(payload: &Bytes) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::connector::{ConnectorInstanceId, ProviderBindingEpoch};

    fn key() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("unit").expect("instance ID"),
            incarnation: ProviderBindingEpoch::new(),
        }
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
    fn managed_partition_preview_requires_exact_ordered_fields_and_generation() {
        let owner = key();
        let operation_id = ConnectorWriteOperationId::new();
        let committed = ConnectorCommittedPartitioning::try_new(4, committed_partition_fields())
            .expect("committed partitioning");
        let exact_fields = vec![
            MvExactPartitionField::try_new(
                Bytes::from_static(b"partition-1000"),
                Bytes::from_static(b"source-7"),
                MvExactPartitionTransform::Day,
            )
            .expect("exact day field"),
            MvExactPartitionField::try_new(
                Bytes::from_static(b"partition-1001"),
                Bytes::from_static(b"source-9"),
                MvExactPartitionTransform::Bucket { num_buckets: 16 },
            )
            .expect("exact bucket field"),
        ];
        let preview = ConnectorManagedPartitionSpecPreview::try_new(
            owner.clone(),
            operation_id,
            committed.clone(),
            Bytes::from_static(b"opaque-spec"),
            exact_fields.clone(),
        )
        .expect("exact preview");
        preview
            .validate_for_request(&owner, operation_id)
            .expect("same owner and operation");
        assert!(preview.validate_for_request(&key(), operation_id).is_err());
        assert!(
            preview
                .validate_for_request(&owner, ConnectorWriteOperationId::new())
                .is_err()
        );
        assert!(
            ConnectorManagedPartitionSpecPreview::try_new(
                owner.clone(),
                operation_id,
                committed.clone(),
                Bytes::new(),
                exact_fields.clone(),
            )
            .is_err()
        );
        let mut mismatched_transform = exact_fields.clone();
        mismatched_transform[0] = MvExactPartitionField::try_new(
            Bytes::from_static(b"partition-1000"),
            Bytes::from_static(b"source-7"),
            MvExactPartitionTransform::Month,
        )
        .expect("mismatched field");
        assert!(
            ConnectorManagedPartitionSpecPreview::try_new(
                owner.clone(),
                operation_id,
                committed.clone(),
                Bytes::from_static(b"opaque-spec"),
                mismatched_transform,
            )
            .is_err()
        );
        let mut duplicate_identity = exact_fields;
        duplicate_identity[1] = MvExactPartitionField::try_new(
            Bytes::from_static(b"partition-1000"),
            Bytes::from_static(b"source-9"),
            MvExactPartitionTransform::Bucket { num_buckets: 16 },
        )
        .expect("duplicate field");
        assert!(
            ConnectorManagedPartitionSpecPreview::try_new(
                owner,
                operation_id,
                committed,
                Bytes::from_static(b"opaque-spec"),
                duplicate_identity,
            )
            .is_err()
        );
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

    /// The sealed cohort set survives the write-operation aggregate because a
    /// row-mutation execution plan is still sealed against one, so its ordering
    /// and duplicate rules still have to hold.
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
