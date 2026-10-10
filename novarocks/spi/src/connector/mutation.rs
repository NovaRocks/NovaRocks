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

//! FE-only provider-neutral catalog mutation contract.
// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorCommittedPartitioning, ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorNamespaceIdentity,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
    ProviderBindingEpoch,
};

/// Largest provider-owned reconciliation payload accepted by the control plane.
pub const MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES: usize = 64 * 1024;

/// Bounded provider-produced commit fact that an application owner may persist
/// and pass back to the provider without understanding its payload.
/// Design: ADR-0036 (docs/adr/ADR-0036-frontend-mv-refresh-lifecycle.md)
///
/// `snapshot_id` is deliberately optional: snapshot-oriented providers can
/// expose the existing typed fact needed by MV bookkeeping, while other
/// providers keep the version entirely opaque.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCommittedVersion {
    payload: Bytes,
    digest: [u8; 32],
    snapshot_id: Option<i64>,
}

impl ConnectorCommittedVersion {
    pub fn try_new(payload: Bytes, snapshot_id: Option<i64>) -> Result<Self, ConnectorError> {
        if payload.len() > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector committed version exceeds the evidence limit",
            ));
        }
        if snapshot_id.is_some_and(|value| value <= 0) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector committed snapshot ID must be positive",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector-committed-version.v1\\0");
        hasher.update(snapshot_id.unwrap_or_default().to_be_bytes());
        hasher.update(payload.as_ref());
        Ok(Self {
            digest: hasher.finalize().into(),
            payload,
            snapshot_id,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let expected = Self::try_new(self.payload.clone(), self.snapshot_id)?;
        if expected.digest != self.digest {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector committed version digest does not match its payload",
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

    pub const fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
}

impl fmt::Debug for ConnectorCommittedVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorCommittedVersion")
            .field("payload_len", &self.payload.len())
            .field("digest", &self.digest)
            .field("snapshot_id", &self.snapshot_id)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMutationOperationId(Uuid);

impl ConnectorMutationOperationId {
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

impl Default for ConnectorMutationOperationId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreatePolicy {
    FailIfExists,
    NoOpIfExists,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateOrReplacePolicy {
    FailIfExists,
    NoOpIfExists,
    ReplaceIfExists,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropPolicy {
    FailIfMissing,
    NoOpIfMissing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorMutationFailureKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Unauthenticated,
    PermissionDenied,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    Unavailable,
    CorruptData,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMutationFailure {
    kind: ConnectorMutationFailureKind,
    message: Arc<str>,
}

impl ConnectorMutationFailure {
    pub fn new(kind: ConnectorMutationFailureKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> ConnectorMutationFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ConnectorMutationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalMutationEffect {
    Applied,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalMutationFinalization {
    Complete,
    Failed(ConnectorMutationFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalMutationOutcome<T> {
    KnownCommitted {
        effect: ExternalMutationEffect,
        receipt: T,
        finalization: ExternalMutationFinalization,
    },
    KnownUncommitted {
        failure: ConnectorMutationFailure,
        /// Cleanup completion is independent of the proven publication verdict.
        cleanup: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorViewIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
    pub view: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorViewDialect {
    StarRocks,
}

/// The durable source contract used by an engine-created connector view.
///
/// `None` is reserved for third-party provider metadata. It is not a legacy
/// NovaRocks representation: a NovaRocks writer must always supply the exact
/// format it wrote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorViewSourceFormat {
    EffectiveUserSourceV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorViewDefinition {
    pub dialect: ConnectorViewDialect,
    /// Exact user query source, after admission substitutions and before any
    /// normalization, qualification, or printing.
    pub raw_sql: Arc<str>,
    /// The catalog used to resolve unqualified names when the definition was
    /// created. Third-party metadata may omit this provider-specific fact.
    pub default_catalog: Option<Arc<str>>,
    /// The namespace used to resolve unqualified names when the definition was
    /// created.
    pub default_namespace: Arc<str>,
    /// NovaRocks-owned source provenance. Third-party provider metadata has no
    /// obligation to supply a NovaRocks format.
    pub source_format: Option<ConnectorViewSourceFormat>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectorDataType {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Decimal { precision: u8, scale: i8 },
    String,
    Binary,
    Json,
    Bitmap,
    Hll,
    Date,
    DateTime,
    DateTimeNs,
    Time,
    Array(Box<ConnectorDataType>),
    Map(Box<ConnectorDataType>, Box<ConnectorDataType>),
    Struct(Vec<ConnectorStructField>),
    Variant,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorStructField {
    pub name: Arc<str>,
    pub data_type: ConnectorDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectorDefaultValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: i8 },
    String(Arc<str>),
    Date(i32),
    DateTime(i64),
    Binary(Bytes),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorColumnAggregation {
    Sum,
    Min,
    Max,
    Replace,
    ReplaceIfNotNull,
    BitmapUnion,
    HllUnion,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectorColumnDefinition {
    pub name: Arc<str>,
    pub data_type: ConnectorDataType,
    pub nullable: bool,
    pub aggregation: Option<ConnectorColumnAggregation>,
    pub default: Option<ConnectorDefaultValue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorTableKeyKind {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableKey {
    pub kind: ConnectorTableKeyKind,
    pub columns: Vec<Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorPartitionTransform {
    Identity { column: Arc<str> },
    Year { column: Arc<str> },
    Month { column: Arc<str> },
    Day { column: Arc<str> },
    Hour { column: Arc<str> },
    Bucket { column: Arc<str>, num_buckets: u32 },
    Truncate { column: Arc<str>, width: u32 },
    Void { column: Arc<str> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorColumnPath {
    pub segments: Vec<Arc<str>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorColumnPosition {
    Default,
    First,
    After { column: Arc<str> },
    Before { column: Arc<str> },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectorSchemaChange {
    AddColumn {
        parent: ConnectorColumnPath,
        column: ConnectorColumnDefinition,
        position: ConnectorColumnPosition,
    },
    DropColumn {
        path: ConnectorColumnPath,
    },
    RenameColumn {
        path: ConnectorColumnPath,
        to: Arc<str>,
    },
    ModifyColumn {
        path: ConnectorColumnPath,
        data_type: ConnectorDataType,
    },
    SetColumnNullability {
        path: ConnectorColumnPath,
        nullable: bool,
    },
    ReorderColumn {
        path: ConnectorColumnPath,
        position: ConnectorColumnPosition,
    },
    SetColumnComment {
        path: ConnectorColumnPath,
        comment: Arc<str>,
    },
}

/// Who is requesting a table property mutation.
///
/// The distinction is a permission boundary, not a hint: providers reject
/// engine-reserved keys for `UserStatement` and accept them for `EngineOwned`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorPropertyAuthority {
    /// Reached from a user's DDL statement (e.g. ALTER TABLE SET TBLPROPERTIES).
    UserStatement,
    /// The engine writing metadata it owns, such as an MV descriptor.
    EngineOwned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorPropertyChange {
    Set { key: Arc<str>, value: Arc<str> },
    Unset { key: Arc<str>, if_exists: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorRefKind {
    Branch,
    Tag,
}

/// Exact publication identity that the provider validates against the source
/// snapshot summary immediately before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorRefreshPublicationGuard {
    publication_id: super::LakePublicationId,
}

impl ConnectorRefreshPublicationGuard {
    pub const fn new(publication_id: super::LakePublicationId) -> Self {
        Self { publication_id }
    }

    pub const fn publication_id(&self) -> super::LakePublicationId {
        self.publication_id
    }

    /// Stable redacted identity suitable for bounded provider evidence.
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector.refresh-publication-guard.v2");
        hasher.update(self.publication_id.to_bytes());
        hasher.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorRefAction {
    Create {
        kind: ConnectorRefKind,
        name: Arc<str>,
        snapshot_id: Option<i64>,
        policy: CreateOrReplacePolicy,
        /// Optional immutable table identity captured at planning. Internal
        /// staged publication supplies this so a DROP/recreate cannot receive
        /// a branch created for the prior table incarnation.
        expected_table_uuid: Option<Arc<str>>,
    },
    Drop {
        kind: ConnectorRefKind,
        name: Arc<str>,
        policy: DropPolicy,
    },
    /// Internal publication primitive. SQL grammar does not expose this action.
    FastForwardBranch {
        source_branch: Arc<str>,
        target_branch: Arc<str>,
        /// Provider-produced version of the staged write. Application owners
        /// persist and forward this fact without decoding it; the provider
        /// verifies that it still names the source branch snapshot before the
        /// guarded CAS publication.
        committed_version: ConnectorCommittedVersion,
        expected_target_snapshot_id: Option<i64>,
        /// Immutable target identity captured with the staged write. The
        /// provider validates it before and within the publication commit.
        expected_table_uuid: Arc<str>,
        guard: ConnectorRefreshPublicationGuard,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDropTableDataDisposition {
    Purge,
    Retain,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConnectorCatalogMutationOperation {
    CreateNamespace {
        namespace: ConnectorNamespaceIdentity,
        policy: CreatePolicy,
    },
    DropNamespace {
        namespace: ConnectorNamespaceIdentity,
        policy: DropPolicy,
    },
    CreateTable {
        table: ConnectorTableIdentity,
        columns: Vec<ConnectorColumnDefinition>,
        key: Option<ConnectorTableKey>,
        partitioning: Vec<ConnectorPartitionTransform>,
        properties: Vec<(Arc<str>, Arc<str>)>,
        policy: CreatePolicy,
    },
    /// Replace application-owned opaque documents in one exact table metadata
    /// commit. Providers must not reinterpret this as an arbitrary property
    /// patch or publish a new data snapshot.
    UpdateApplicationDocuments {
        intent: super::ConnectorDocumentUpdateIntent,
    },
    DropTable {
        table: ConnectorTableIdentity,
        policy: DropPolicy,
        data_disposition: ConnectorDropTableDataDisposition,
    },
    CreateView {
        view: ConnectorViewIdentity,
        columns: Vec<ConnectorColumnDefinition>,
        definition: ConnectorViewDefinition,
        comment: Option<Arc<str>>,
        properties: Vec<(Arc<str>, Arc<str>)>,
        policy: CreateOrReplacePolicy,
    },
    DropView {
        view: ConnectorViewIdentity,
        policy: DropPolicy,
    },
    AlterSchema {
        table: ConnectorTableIdentity,
        changes: Vec<ConnectorSchemaChange>,
    },
    AlterPartitionSpec {
        table: ConnectorTableIdentity,
        add: Vec<ConnectorPartitionTransform>,
        drop: Vec<ConnectorPartitionTransform>,
    },
    AlterProperties {
        table: ConnectorTableIdentity,
        changes: Vec<ConnectorPropertyChange>,
        /// Who is asking. A user statement may not touch engine-reserved
        /// property namespaces; the engine writing its own metadata may.
        ///
        /// Without this, "update the engine's own properties on an existing
        /// table" has no neutral expression at all, and the only way to do it
        /// is to bypass the SPI entirely (SPI-5I F6).
        authority: ConnectorPropertyAuthority,
        /// Optional exact committed default partitioning that must still be
        /// current when the provider applies the property changes.
        ///
        /// This closes the gap between an atomic managed publication and its
        /// later engine-owned descriptor projection without exposing a
        /// provider-specific partition spec. Ordinary property mutations do
        /// not require this precondition and pass `None`.
        expected_committed_partitioning: Option<ConnectorCommittedPartitioning>,
    },
    AlterRef {
        table: ConnectorTableIdentity,
        action: ConnectorRefAction,
    },
}

impl ConnectorCatalogMutationOperation {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::CreateNamespace { .. } => "create-namespace",
            Self::DropNamespace { .. } => "drop-namespace",
            Self::CreateTable { .. } => "create-table",
            Self::UpdateApplicationDocuments { .. } => "update-application-documents",
            Self::DropTable { .. } => "drop-table",
            Self::CreateView { .. } => "create-view",
            Self::DropView { .. } => "drop-view",
            Self::AlterSchema { .. } => "alter-schema",
            Self::AlterPartitionSpec { .. } => "alter-partition-spec",
            Self::AlterProperties { .. } => "alter-properties",
            Self::AlterRef { .. } => "alter-ref",
        }
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::AlterProperties {
            expected_committed_partitioning: Some(expected),
            ..
        } = self
        {
            expected.validate()?;
        }
        Ok(())
    }

    fn validate_for(
        &self,
        owner: &ConnectorProviderBindingKey,
        operation_id: ConnectorMutationOperationId,
    ) -> Result<(), ConnectorError> {
        self.validate()?;
        if let Self::UpdateApplicationDocuments { intent } = self {
            intent.validate_for(owner, operation_id)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConnectorCatalogMutationRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub target: ConnectorProviderBindingKey,
    pub operation: ConnectorCatalogMutationOperation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCatalogMutationReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    provider_version: Option<Bytes>,
    committed_version: Option<ConnectorCommittedVersion>,
    resulting_row_count: Option<u64>,
}

impl ConnectorCatalogMutationReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        provider_version: Option<Bytes>,
    ) -> Result<Self, ConnectorError> {
        if provider_version
            .as_ref()
            .is_some_and(|value| value.len() > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector mutation receipt version exceeds the evidence limit",
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            operation_id,
            operation_kind: operation_kind.into(),
            provider_version,
            committed_version: None,
            resulting_row_count: None,
        })
    }

    pub fn try_new_with_committed_version(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        provider_version: Option<Bytes>,
        committed_version: Option<ConnectorCommittedVersion>,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new(
            descriptor,
            incarnation,
            operation_id,
            operation_kind,
            provider_version,
        )?;
        if let Some(version) = &committed_version {
            version.validate()?;
        }
        receipt.committed_version = committed_version;
        Ok(receipt)
    }

    pub fn try_new_with_committed_facts(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        provider_version: Option<Bytes>,
        committed_version: ConnectorCommittedVersion,
        resulting_row_count: u64,
    ) -> Result<Self, ConnectorError> {
        let mut receipt = Self::try_new_with_committed_version(
            descriptor,
            incarnation,
            operation_id,
            operation_kind,
            provider_version,
            Some(committed_version),
        )?;
        receipt.resulting_row_count = Some(resulting_row_count);
        Ok(receipt)
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn operation_kind(&self) -> &str {
        &self.operation_kind
    }
    pub fn provider_version(&self) -> Option<&Bytes> {
        self.provider_version.as_ref()
    }

    pub fn committed_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.committed_version.as_ref()
    }
    pub const fn resulting_row_count(&self) -> Option<u64> {
        self.resulting_row_count
    }
}

impl fmt::Debug for ConnectorCatalogMutationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorCatalogMutationReceipt")
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field(
                "provider_version_len",
                &self.provider_version.as_ref().map(Bytes::len),
            )
            .field("committed_version", &self.committed_version)
            .field("resulting_row_count", &self.resulting_row_count)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalMutationEvidence {
    schema_version: u16,
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    provider_payload: Bytes,
}

impl ExternalMutationEvidence {
    pub fn try_new(
        schema_version: u16,
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if provider_payload.len() > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "external mutation evidence exceeds 64 KiB",
            ));
        }
        Ok(Self {
            schema_version,
            descriptor,
            incarnation,
            operation_id,
            operation_kind: operation_kind.into(),
            provider_payload,
        })
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn operation_kind(&self) -> &str {
        &self.operation_kind
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.schema_version.to_be_bytes());
        hasher.update(self.descriptor.provider_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.descriptor.instance_id.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.incarnation.to_bytes());
        hasher.update(self.operation_id.to_bytes());
        hasher.update(self.operation_kind.as_bytes());
        hasher.update([0]);
        hasher.update(self.provider_payload.as_ref());
        hasher.finalize().into()
    }

    /// Encodes only operation-specific reconciliation evidence.  This compact
    /// wire form is deliberately owned by SPI so a durable frontend never
    /// reimplements connector identity parsing.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        const MAGIC: &[u8; 4] = b"EME1";
        let provider_id = self.descriptor.provider_id.as_str().as_bytes();
        let instance_id = self.descriptor.instance_id.as_str().as_bytes();
        let operation_kind = self.operation_kind.as_bytes();
        let payload = self.provider_payload.as_ref();
        let bounded_u16 = |value: &[u8], field: &str| {
            u16::try_from(value.len()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("external mutation evidence {field} exceeds wire bound"),
                )
            })
        };
        let provider_id_len = bounded_u16(provider_id, "provider ID")?;
        let instance_id_len = bounded_u16(instance_id, "instance ID")?;
        let operation_kind_len = bounded_u16(operation_kind, "operation kind")?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "external mutation evidence payload exceeds wire bound",
            )
        })?;
        let mut encoded = Vec::with_capacity(
            MAGIC.len()
                + 2
                + 2
                + provider_id.len()
                + 2
                + instance_id.len()
                + 16
                + 16
                + 2
                + operation_kind.len()
                + 4
                + payload.len(),
        );
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.schema_version.to_be_bytes());
        encoded.extend_from_slice(&provider_id_len.to_be_bytes());
        encoded.extend_from_slice(provider_id);
        encoded.extend_from_slice(&instance_id_len.to_be_bytes());
        encoded.extend_from_slice(instance_id);
        encoded.extend_from_slice(&self.incarnation.to_bytes());
        encoded.extend_from_slice(&self.operation_id.to_bytes());
        encoded.extend_from_slice(&operation_kind_len.to_be_bytes());
        encoded.extend_from_slice(operation_kind);
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(payload);
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        const MAGIC: &[u8; 4] = b"EME1";
        let mut offset = 0usize;
        let read = |offset: &mut usize, len: usize| -> Result<&[u8], ConnectorError> {
            let end = offset.checked_add(len).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "external mutation evidence wire overflow",
                )
            })?;
            let slice = bytes.get(*offset..end).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "truncated external mutation evidence wire",
                )
            })?;
            *offset = end;
            Ok(slice)
        };
        if read(&mut offset, MAGIC.len())? != MAGIC {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unsupported external mutation evidence wire version",
            ));
        }
        let read_u16 = |offset: &mut usize| -> Result<u16, ConnectorError> {
            let raw = read(offset, 2)?;
            Ok(u16::from_be_bytes([raw[0], raw[1]]))
        };
        let schema_version = read_u16(&mut offset)?;
        let provider_id_len = read_u16(&mut offset)? as usize;
        let provider_id =
            std::str::from_utf8(read(&mut offset, provider_id_len)?).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "external mutation evidence provider ID is not UTF-8",
                )
            })?;
        let instance_id_len = read_u16(&mut offset)? as usize;
        let instance_id =
            std::str::from_utf8(read(&mut offset, instance_id_len)?).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "external mutation evidence instance ID is not UTF-8",
                )
            })?;
        let incarnation: [u8; 16] = read(&mut offset, 16)?
            .try_into()
            .expect("fixed-width evidence incarnation");
        let operation_id: [u8; 16] = read(&mut offset, 16)?
            .try_into()
            .expect("fixed-width evidence operation ID");
        let operation_kind_len = read_u16(&mut offset)? as usize;
        let operation_kind =
            std::str::from_utf8(read(&mut offset, operation_kind_len)?).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "external mutation evidence operation kind is not UTF-8",
                )
            })?;
        let payload_len_raw = read(&mut offset, 4)?;
        let payload_len = u32::from_be_bytes(
            payload_len_raw
                .try_into()
                .expect("fixed-width evidence payload length"),
        ) as usize;
        let payload = read(&mut offset, payload_len)?;
        if offset != bytes.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "external mutation evidence wire has trailing bytes",
            ));
        }
        Self::try_new(
            schema_version,
            ConnectorInstanceDescriptor {
                provider_id: super::ConnectorProviderId::parse(provider_id)?,
                instance_id: ConnectorInstanceId::parse(instance_id)?,
            },
            ProviderBindingEpoch::from_bytes(incarnation),
            ConnectorMutationOperationId::from_bytes(operation_id),
            operation_kind,
            Bytes::copy_from_slice(payload),
        )
    }
}

impl fmt::Debug for ExternalMutationEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalMutationEvidence")
            .field("schema_version", &self.schema_version)
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("digest", &self.digest())
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorCatalogMutationReconcileRequest {
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

/// FE-only external catalog mutation capability. It is never installed in a
/// BE execution binding.
pub trait ConnectorCatalogMutation: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    /// Admit a complete mutation before a consumer decomposes it into reads
    /// and child mutations. This method must perform no external side effect.
    /// Execution must repeat admission; this answer is not a reusable permit.
    fn admit(&self, _request: &ConnectorCatalogMutationRequest) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector catalog mutation does not support preflight admission",
        ))
    }
    fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>;
    fn reconcile(
        &self,
        request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>;
}

/// Narrow consumer port for FE application code. Core may acquire a lease but
/// cannot register, retire, or inspect control generations.
pub trait ConnectorCatalogMutationResolver: Send + Sync {
    fn acquire_current_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError>;

    fn acquire_exact_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorCatalogMutationLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorCatalogMutationLease {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_incarnation: ProviderBindingEpoch,
    mutation: Arc<dyn ConnectorCatalogMutation>,
    _release: Arc<MutationLeaseRelease>,
}

struct MutationLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCatalogMutationLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        mutation: Arc<dyn ConnectorCatalogMutation>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if mutation.descriptor() != &descriptor || mutation.incarnation() != provider_incarnation {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector mutation capability does not match its lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_incarnation,
            mutation,
            _release: Arc::new(MutationLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }

    /// Builds a provider request behind an FE-owned control-runtime lease.
    /// Provider incarnation remains internal to retain legacy external
    /// evidence validation without exposing it to FE application callers.
    pub fn admit_operation(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorCatalogMutationOperation,
        context: ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        let request = ConnectorCatalogMutationRequest {
            operation_id,
            target: ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.provider_incarnation,
            },
            operation,
            context,
        };
        self.validate_request(&request)?;
        self.mutation.admit(&request)
    }

    pub fn execute_operation(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorCatalogMutationOperation,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        self.execute(ConnectorCatalogMutationRequest {
            operation_id,
            target: ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.provider_incarnation,
            },
            operation,
            context,
        })
    }

    pub fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        self.validate_request(&request)?;
        let outcome = self.mutation.execute(request.clone())?;
        self.validate_outcome(request.operation_id, request.operation.kind(), &outcome)?;
        Ok(outcome)
    }

    pub fn reconcile(
        &self,
        request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        self.validate_evidence(&request.evidence)?;
        let operation_id = request.evidence.operation_id();
        let operation_kind = request.evidence.operation_kind().to_string();
        let outcome = self.mutation.reconcile(request)?;
        self.validate_outcome(operation_id, &operation_kind, &outcome)?;
        Ok(outcome)
    }

    fn validate_request(
        &self,
        request: &ConnectorCatalogMutationRequest,
    ) -> Result<(), ConnectorError> {
        if request.target.instance_id != self.descriptor.instance_id
            || request.target.incarnation != self.provider_incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector mutation request does not match its lease generation",
            ));
        }
        request
            .operation
            .validate_for(&request.target, request.operation_id)?;
        Ok(())
    }

    fn validate_evidence(&self, evidence: &ExternalMutationEvidence) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.provider_incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "external mutation evidence does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_outcome(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation_kind: &str,
        outcome: &ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                if receipt.descriptor() != &self.descriptor
                    || receipt.incarnation() != self.provider_incarnation
                    || receipt.operation_id() != operation_id
                    || receipt.operation_kind() != operation_kind
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "connector mutation receipt does not match its request",
                    ));
                }
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(evidence)?;
                if evidence.operation_id() != operation_id
                    || evidence.operation_kind() != operation_kind
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "external mutation evidence does not match its request",
                    ));
                }
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
        }
        Ok(())
    }
}

impl Drop for MutationLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{
        ConnectorCatalogMutationOperation, ConnectorMutationOperationId,
        ConnectorPropertyAuthority, ExternalMutationEvidence,
    };
    use crate::connector::{
        ConnectorCommittedPartitionField, ConnectorCommittedPartitioning, ConnectorErrorKind,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorManagedPartitionTransform,
        ConnectorProviderId, ConnectorTableIdentity, ProviderBindingEpoch,
    };

    #[test]
    fn catalog_mutation_preflight_defaults_to_unsupported_without_dispatch() {
        use super::*;
        struct Capability {
            descriptor: ConnectorInstanceDescriptor,
        }
        impl ConnectorCatalogMutation for Capability {
            fn descriptor(&self) -> &ConnectorInstanceDescriptor {
                &self.descriptor
            }
            fn incarnation(&self) -> ProviderBindingEpoch {
                ProviderBindingEpoch::from_bytes([1; 16])
            }
            fn execute(
                &self,
                _: ConnectorCatalogMutationRequest,
            ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
            {
                panic!("preflight must not dispatch a mutation")
            }
            fn reconcile(
                &self,
                _: ConnectorCatalogMutationReconcileRequest,
            ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
            {
                panic!("preflight must not reconcile a mutation")
            }
        }
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("analytics").unwrap(),
        };
        let lease = ConnectorCatalogMutationLease::new(
            descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            ProviderBindingEpoch::from_bytes([1; 16]),
            Arc::new(Capability {
                descriptor: descriptor.clone(),
            }),
            || {},
        )
        .unwrap();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            crate::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .unwrap();
        let error = lease
            .admit_operation(
                ConnectorMutationOperationId::new(),
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace: ConnectorNamespaceIdentity {
                        instance_id: descriptor.instance_id,
                        namespace: "db".into(),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                context,
            )
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("analytics").expect("instance ID"),
            namespace: Arc::from("db"),
            table: Arc::from("mv_target"),
        }
    }

    fn committed_partition_field() -> ConnectorCommittedPartitionField {
        ConnectorCommittedPartitionField::try_new(
            1000,
            "event_day",
            7,
            "event_time",
            0,
            ConnectorManagedPartitionTransform::Day,
        )
        .expect("committed partition field")
    }

    fn committed_partitioning() -> ConnectorCommittedPartitioning {
        ConnectorCommittedPartitioning::try_new(4, vec![committed_partition_field()])
            .expect("committed partitioning")
    }

    fn evidence() -> ExternalMutationEvidence {
        ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: ConnectorInstanceId::parse("analytics").expect("instance ID"),
            },
            ProviderBindingEpoch::new(),
            ConnectorMutationOperationId::new(),
            "statistics-publish",
            Bytes::from_static(b"operation-specific-evidence"),
        )
        .expect("evidence")
    }

    #[test]
    fn external_mutation_evidence_wire_round_trips_exactly() {
        let evidence = evidence();
        let wire = evidence.try_to_wire_v1().expect("encode evidence");
        let decoded = ExternalMutationEvidence::try_from_wire_v1(&wire).expect("decode evidence");
        assert_eq!(decoded, evidence);
        assert_eq!(decoded.digest(), evidence.digest());
    }

    #[test]
    fn external_mutation_evidence_wire_rejects_trailing_data() {
        let mut wire = evidence()
            .try_to_wire_v1()
            .expect("encode evidence")
            .to_vec();
        wire.push(0);
        let error = ExternalMutationEvidence::try_from_wire_v1(&wire)
            .expect_err("trailing bytes must not be accepted");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn guarded_alter_properties_accepts_valid_committed_partitioning() {
        let operation = ConnectorCatalogMutationOperation::AlterProperties {
            table: table(),
            changes: Vec::new(),
            authority: ConnectorPropertyAuthority::EngineOwned,
            expected_committed_partitioning: Some(committed_partitioning().clone()),
        };

        operation
            .validate()
            .expect("valid canonical committed partitioning guard");
        assert_eq!(operation.kind(), "alter-properties");
    }

    #[test]
    fn committed_partitioning_guard_construction_rejects_invalid_facts() {
        let invalid_spec =
            ConnectorCommittedPartitioning::try_new(-1, vec![committed_partition_field()])
                .expect_err("negative spec ID must fail closed");
        assert_eq!(invalid_spec.kind(), ConnectorErrorKind::InvalidRequest);

        let empty = ConnectorCommittedPartitioning::try_new(4, Vec::new())
            .expect_err("empty canonical partitioning must fail closed");
        assert_eq!(empty.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn unguarded_alter_properties_remains_compatible() {
        let operation = ConnectorCatalogMutationOperation::AlterProperties {
            table: table(),
            changes: Vec::new(),
            authority: ConnectorPropertyAuthority::UserStatement,
            expected_committed_partitioning: None,
        };

        operation
            .validate()
            .expect("ordinary unguarded property mutation");
        assert_eq!(operation.kind(), "alter-properties");
    }
}
