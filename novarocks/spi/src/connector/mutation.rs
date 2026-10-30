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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ConnectorCommittedPartitioning, ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorNamespaceIdentity,
    ConnectorProcessLocalPayload, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableIdentity, ProviderBindingEpoch,
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
    Interrupted,
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
    source: Option<ConnectorError>,
}

impl From<ConnectorErrorKind> for ConnectorMutationFailureKind {
    fn from(kind: ConnectorErrorKind) -> Self {
        match kind {
            ConnectorErrorKind::InvalidRequest => Self::InvalidRequest,
            ConnectorErrorKind::NotFound => Self::NotFound,
            ConnectorErrorKind::PermissionDenied => Self::PermissionDenied,
            ConnectorErrorKind::Unsupported => Self::Unsupported,
            ConnectorErrorKind::Cancelled => Self::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => Self::DeadlineExceeded,
            ConnectorErrorKind::ResourceExhausted => Self::ResourceExhausted,
            ConnectorErrorKind::Interrupted => Self::Interrupted,
            ConnectorErrorKind::Unavailable => Self::Unavailable,
            ConnectorErrorKind::CorruptData => Self::CorruptData,
            ConnectorErrorKind::Internal => Self::Internal,
        }
    }
}
impl ConnectorMutationFailure {
    pub fn from_connector(error: ConnectorError) -> Self {
        Self {
            kind: error.kind().into(),
            message: error.to_string().into(),
            source: Some(error),
        }
    }
    pub fn connector_source(&self) -> Option<&ConnectorError> {
        self.source.as_ref()
    }

    pub fn new(kind: ConnectorMutationFailureKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub const fn kind(&self) -> ConnectorMutationFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::error::Error for ConnectorMutationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
    }
}
impl novarocks_failure_contract::FailureProjection for ConnectorMutationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass as C, FailureFact, FailureReason, domain};
        if let Some(source) = &self.source {
            return source.failure_fact();
        }
        let (class, reason) = match self.kind {
            ConnectorMutationFailureKind::InvalidRequest => (C::Data, 6000),
            ConnectorMutationFailureKind::NotFound => (C::Dependency, 6001),
            ConnectorMutationFailureKind::AlreadyExists => (C::Data, 6002),
            ConnectorMutationFailureKind::Conflict => (C::Dependency, 6003),
            ConnectorMutationFailureKind::Unauthenticated => (C::Dependency, 6004),
            ConnectorMutationFailureKind::PermissionDenied => (C::Dependency, 6005),
            ConnectorMutationFailureKind::Unsupported => (C::Data, 6006),
            ConnectorMutationFailureKind::Cancelled => (C::Control, 6007),
            ConnectorMutationFailureKind::DeadlineExceeded => (C::Control, 6008),
            ConnectorMutationFailureKind::ResourceExhausted => (C::Resource, 6009),
            ConnectorMutationFailureKind::Unavailable => (C::Dependency, 6010),
            ConnectorMutationFailureKind::CorruptData => (C::Data, 6011),
            ConnectorMutationFailureKind::Internal => (C::Unexpected, 6012),
            ConnectorMutationFailureKind::Interrupted => (C::Invariant, 6013),
        };
        FailureFact::new(
            class,
            FailureReason::new(
                domain::CONNECTOR,
                std::num::NonZeroU16::new(reason).unwrap(),
            ),
        )
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
        /// Necessary business finalization; independent of safe cleanup.
        finalization: ExternalMutationFinalization,
        /// Operation-owned cleanup diagnostics do not undo a proven commit.
        /// Exact remaining artifacts and live obligations stay with their owner.
        cleanup: ExternalMutationFinalization,
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
#[expect(
    clippy::large_enum_variant,
    reason = "The public mutation union keeps each DDL request as its exact owned value; boxing the table-creation payload would change the shared SPI contract"
)]
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
    /// Typed marker, main-conditioned slot, or full-observation expiry intent.
    /// Each channel admits only the side effects specified by its own shape.
    KeyedDocuments { intent: super::KeyedMutationIntent },
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
            Self::KeyedDocuments { .. } => "keyed-documents",
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
        if let Self::KeyedDocuments { intent } = self {
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

#[derive(Clone)]
pub struct ExternalMutationEvidence {
    schema_version: u16,
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    provider_payload: Bytes,
    // An in-process owner is neither a wire fact nor an evidence identity.
    process_local_payload: Option<ConnectorProcessLocalPayload>,
}

impl PartialEq for ExternalMutationEvidence {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.descriptor == other.descriptor
            && self.incarnation == other.incarnation
            && self.operation_id == other.operation_id
            && self.operation_kind == other.operation_kind
            && self.provider_payload == other.provider_payload
    }
}
impl Eq for ExternalMutationEvidence {}

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
            process_local_payload: None,
        })
    }

    /// Retain a provider-owned in-process observation owner. Attachment must
    /// match the complete generation; it never changes serializable evidence.
    pub fn with_process_local_payload(
        mut self,
        payload: ConnectorProcessLocalPayload,
    ) -> Result<Self, ConnectorError> {
        payload.validate_owner(&ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        })?;
        if self.process_local_payload.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "external mutation evidence already retains a process-local owner",
            ));
        }
        self.process_local_payload = Some(payload);
        Ok(self)
    }

    pub fn process_local_payload(&self) -> Option<&ConnectorProcessLocalPayload> {
        self.process_local_payload.as_ref()
    }

    /// A journal stores the identity facts without a back-reference to itself.
    /// The returned value has identical digest, wire representation and equality.
    pub fn without_process_local_payload(&self) -> Self {
        let mut detached = self.clone();
        detached.process_local_payload = None;
        detached
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
    /// Execute through the original asynchronous Provider boundary. An absent
    /// implementation is a missing capability, never a synchronous fallback.
    fn execute_async<'a>(
        &'a self,
        _request: ConnectorCatalogMutationRequest,
        _receiving: &'a ConnectorCatalogMutationReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
                        ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector catalog mutation does not support asynchronous execution",
            ))
        })
    }
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

    /// Dispatch once while the caller retains the original receiving Source.
    pub async fn execute_operation_async_in(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorCatalogMutationOperation,
        context: ConnectorRequestContext,
        receiving: &ConnectorCatalogMutationReceiving,
    ) -> Result<(), ConnectorError> {
        self.execute_async_in(
            ConnectorCatalogMutationRequest {
                operation_id,
                target: ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.provider_incarnation,
                },
                operation,
                context,
            },
            receiving,
        )
        .await
    }

    /// Preserve the raw Provider verdict and its validation before foreign Drop.
    /// Cancellation of a borrowed observer must leave this SAME future owned
    /// by the caller; this method neither reconciles nor replays a mutation.
    pub async fn execute_async_in(
        &self,
        request: ConnectorCatalogMutationRequest,
        receiving: &ConnectorCatalogMutationReceiving,
    ) -> Result<(), ConnectorError> {
        receiving.install_request(self, &request)?;
        if let Err(error) = self.validate_request(&request) {
            receiving.reject_request(error.clone());
            return Err(error);
        }
        receiving.enter_factory();
        let future = self.mutation.execute_async(request, receiving);
        receiving.leave_pending();
        let mut child = CatalogMutationChild {
            future: Some(future),
            receiving,
        };
        let outcome = std::future::poll_fn(|cx| child.poll(cx)).await;
        receiving.publish_raw(outcome);
        receiving.validate_raw(self);
        drop(child);
        receiving.completed_result()
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
                validate_catalog_mutation_receipt(
                    &self.descriptor,
                    self.provider_incarnation,
                    operation_id,
                    operation_kind,
                    receipt,
                )?;
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

fn validate_catalog_mutation_receipt(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &str,
    receipt: &ConnectorCatalogMutationReceipt,
) -> Result<(), ConnectorError> {
    if receipt.descriptor() != descriptor
        || receipt.incarnation() != incarnation
        || receipt.operation_id() != operation_id
        || receipt.operation_kind() != operation_kind
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector mutation receipt does not match its request",
        ));
    }
    Ok(())
}

struct CatalogMutationIdentity {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    target: ConnectorProviderBindingKey,
    operation_id: ConnectorMutationOperationId,
    operation_kind: &'static str,
}

/// The Provider's necessary committed business frontier. Cleanup is not part
/// of this fact and may still be running in the SAME original Provider future.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCatalogMutationNeededCommitted {
    effect: ExternalMutationEffect,
    receipt: ConnectorCatalogMutationReceipt,
    finalization: ExternalMutationFinalization,
}
impl ConnectorCatalogMutationNeededCommitted {
    pub const fn effect(&self) -> ExternalMutationEffect {
        self.effect
    }
    pub fn receipt(&self) -> &ConnectorCatalogMutationReceipt {
        &self.receipt
    }
    pub fn finalization(&self) -> &ExternalMutationFinalization {
        &self.finalization
    }
}

#[derive(Default)]
struct CatalogMutationReceivingState {
    request: Option<CatalogMutationIdentity>,
    raw: Option<Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>>,
    validation: Option<Result<(), ConnectorError>>,
    applied_receipt: Option<ConnectorCatalogMutationReceipt>,
    needed_committed: Option<ConnectorCatalogMutationNeededCommitted>,
    needed_validation: Option<Result<(), ConnectorError>>,
    original_owner: Option<ConnectorProcessLocalPayload>,
    finalization_context: Option<Result<ConnectorRequestContext, ConnectorError>>,
    dispatched: bool,
    provider_exited: bool,
}

/// The original catalog operation's fixed receiving point. It grants no
/// dispatch, recovery, finalization or resource authority by observation.
#[derive(Default)]
pub struct ConnectorCatalogMutationReceiving {
    state: Mutex<CatalogMutationReceivingState>,
    in_foreign: AtomicBool,
}

/// A short borrow of the original facts. Do not retain it across foreign work.
pub struct ConnectorCatalogMutationObservation<'a> {
    state: std::sync::MutexGuard<'a, CatalogMutationReceivingState>,
}
impl ConnectorCatalogMutationObservation<'_> {
    pub fn descriptor(&self) -> Option<&ConnectorInstanceDescriptor> {
        self.state
            .request
            .as_ref()
            .map(|request| &request.descriptor)
    }
    pub fn control_runtime_id(&self) -> Option<ConnectorControlRuntimeId> {
        self.state
            .request
            .as_ref()
            .map(|request| request.control_runtime_id)
    }
    pub fn target(&self) -> Option<&ConnectorProviderBindingKey> {
        self.state.request.as_ref().map(|request| &request.target)
    }
    pub fn operation_id(&self) -> Option<ConnectorMutationOperationId> {
        self.state
            .request
            .as_ref()
            .map(|request| request.operation_id)
    }
    pub fn operation_kind(&self) -> Option<&'static str> {
        self.state
            .request
            .as_ref()
            .map(|request| request.operation_kind)
    }
    pub fn raw_outcome(
        &self,
    ) -> Option<&Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>>
    {
        self.state.raw.as_ref()
    }
    pub fn validation(&self) -> Option<&Result<(), ConnectorError>> {
        self.state.validation.as_ref()
    }
    /// An original Applied fact, independent of necessary finalization and exit.
    pub fn applied_receipt(&self) -> Option<&ConnectorCatalogMutationReceipt> {
        self.state.applied_receipt.as_ref()
    }
    /// Necessary business facts do not certify cleanup or actual child exit.
    pub fn needed_committed(&self) -> Option<&ConnectorCatalogMutationNeededCommitted> {
        self.state.needed_committed.as_ref()
    }
    pub fn needed_validation(&self) -> Option<&Result<(), ConnectorError>> {
        self.state.needed_validation.as_ref()
    }
    /// Provider operation/journal custody remains after the child F exits.
    pub fn original_owner(&self) -> Option<&ConnectorProcessLocalPayload> {
        self.state.original_owner.as_ref()
    }
    /// The original journal's finite necessary phase, not an effect or exit proof.
    /// A failed grant remains a failure and never grants continuation authority.
    pub fn finalization_context(&self) -> Option<&Result<ConnectorRequestContext, ConnectorError>> {
        self.state.finalization_context.as_ref()
    }
    pub fn dispatched(&self) -> bool {
        self.state.dispatched
    }
    pub fn provider_exited(&self) -> bool {
        self.state.provider_exited
    }
    /// A validated outcome is a fact, not proof of normal Provider child Drop.
    pub fn validated_outcome(
        &self,
    ) -> Option<Result<&ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, &ConnectorError>>
    {
        match self.state.validation.as_ref()? {
            Err(error) => Some(Err(error)),
            Ok(()) => match self.state.raw.as_ref()? {
                Ok(outcome) => Some(Ok(outcome)),
                Err(error) => Some(Err(error)),
            },
        }
    }
}

impl ConnectorCatalogMutationReceiving {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn observation(&self) -> ConnectorCatalogMutationObservation<'_> {
        ConnectorCatalogMutationObservation {
            state: self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }
    /// Sticky across foreign panic. The outer Source retains the raw panic.
    /// A false value alone never certifies actual exit.
    pub fn foreign_exit_unproven(&self) -> bool {
        self.in_foreign.load(Ordering::Acquire)
    }

    fn install_request(
        &self,
        lease: &ConnectorCatalogMutationLease,
        request: &ConnectorCatalogMutationRequest,
    ) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.request.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original receiving is already claimed",
            ));
        }
        state.request = Some(CatalogMutationIdentity {
            descriptor: lease.descriptor.clone(),
            control_runtime_id: lease.control_runtime_id,
            target: request.target.clone(),
            operation_id: request.operation_id,
            operation_kind: request.operation.kind(),
        });
        Ok(())
    }
    fn reject_request(&self, error: ConnectorError) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .validation = Some(Err(error));
    }
    fn enter_factory(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dispatched = true;
        self.in_foreign.store(true, Ordering::Release);
    }
    fn enter_poll(&self) -> bool {
        !self.in_foreign.swap(true, Ordering::AcqRel)
    }
    fn leave_pending(&self) {
        self.in_foreign.store(false, Ordering::Release);
    }
    fn publish_raw(
        &self,
        outcome: Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.raw.is_none(),
            "same catalog mutation outcome is received only once"
        );
        state.raw = Some(outcome);
    }
    fn validate_raw(&self, lease: &ConnectorCatalogMutationLease) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request = state
            .request
            .as_ref()
            .expect("installed original catalog request");
        let result = match state
            .raw
            .as_ref()
            .expect("received original catalog outcome")
        {
            Err(error) => Err(error.clone()),
            Ok(outcome) => lease
                .validate_outcome(request.operation_id, request.operation_kind, outcome)
                .and_then(|()| {
                    if let Some(Err(error)) = &state.needed_validation {
                        return Err(error.clone());
                    }
                    if state.applied_receipt.is_some()
                        && !matches!(
                            outcome,
                            ExternalMutationOutcome::KnownCommitted {
                                effect: ExternalMutationEffect::Applied,
                                ..
                            }
                        )
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "catalog mutation outcome contradicts its original Applied receipt",
                        ));
                    }
                    if let Some(needed) = &state.needed_committed {
                        match outcome {
                            ExternalMutationOutcome::KnownCommitted { effect, receipt, finalization, .. }
                                if *effect == needed.effect && receipt == &needed.receipt
                                    && finalization == &needed.finalization => {}
                            _ => return Err(ConnectorError::new(ConnectorErrorKind::InvalidRequest,
                                "catalog mutation outcome contradicts its necessary committed facts")),
                        }
                    }
                    Ok(())
                }),
        };
        if result.is_ok()
            && state.needed_committed.is_none()
            && state.needed_validation.is_none()
            && let Some(Ok(ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization,
                ..
            })) = &state.raw
        {
            let needed = ConnectorCatalogMutationNeededCommitted {
                effect: *effect,
                receipt: receipt.clone(),
                finalization: finalization.clone(),
            };
            state.needed_committed = Some(needed);
            state.needed_validation = Some(Ok(()));
        }
        assert!(
            state.validation.is_none(),
            "same catalog outcome is validated only once"
        );
        state.validation = Some(result);
    }
    /// Only the actual Provider's Applied journal branch may call this before
    /// necessary finalization. It neither completes the operation nor authorizes exit.
    pub fn record_applied_receipt(
        &self,
        receipt: &ConnectorCatalogMutationReceipt,
    ) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request = state.request.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original request is not installed",
            )
        })?;
        validate_catalog_mutation_receipt(
            &request.descriptor,
            request.target.incarnation,
            request.operation_id,
            request.operation_kind,
            receipt,
        )?;
        if !state.dispatched {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation Applied receipt precedes Provider dispatch",
            ));
        }
        if let Some(original) = &state.applied_receipt {
            if original != receipt {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "catalog mutation original Applied receipt cannot be overwritten",
                ));
            }
        } else {
            state.applied_receipt = Some(receipt.clone());
        }
        Ok(())
    }
    /// Record the actual necessary committed frontier before pure cleanup.
    /// A NoOp remains NoOp and does not create an Applied receipt.
    pub fn record_needed_committed(
        &self,
        effect: ExternalMutationEffect,
        receipt: &ConnectorCatalogMutationReceipt,
        finalization: &ExternalMutationFinalization,
    ) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request = state.request.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original request is not installed",
            )
        })?;
        let validation = validate_catalog_mutation_receipt(
            &request.descriptor,
            request.target.incarnation,
            request.operation_id,
            request.operation_kind,
            receipt,
        )
        .and_then(|()| {
            if !state.dispatched {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "catalog mutation necessary committed fact precedes Provider dispatch",
                ));
            }
            if state.applied_receipt.is_some() && effect != ExternalMutationEffect::Applied {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "catalog mutation necessary facts contradict its original Applied receipt",
                ));
            }
            Ok(())
        });
        if let Err(error) = validation {
            if state.needed_committed.is_none() && state.needed_validation.is_none() {
                state.needed_validation = Some(Err(error.clone()));
            }
            return Err(error);
        }
        if let Some(original) = &state.needed_committed {
            if original.effect != effect
                || &original.receipt != receipt
                || &original.finalization != finalization
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "catalog mutation original necessary committed facts cannot be overwritten",
                ));
            }
            return Ok(());
        }
        if let Some(Err(error)) = &state.needed_validation {
            return Err(error.clone());
        }
        state.needed_committed = Some(ConnectorCatalogMutationNeededCommitted {
            effect,
            receipt: receipt.clone(),
            finalization: finalization.clone(),
        });
        state.needed_validation = Some(Ok(()));
        Ok(())
    }
    /// Receive the SAME original journal's necessary finalization context once.
    /// Requires the separately received actual Applied receipt for this exact
    /// operation. It does not prove necessary readiness, cleanup, or actual exit.
    pub fn record_finalization_context(
        &self,
        receipt: &ConnectorCatalogMutationReceipt,
        context: Result<ConnectorRequestContext, ConnectorError>,
    ) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request = state.request.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original request is not installed",
            )
        })?;
        validate_catalog_mutation_receipt(
            &request.descriptor,
            request.target.incarnation,
            request.operation_id,
            request.operation_kind,
            receipt,
        )?;
        if !state.dispatched || state.original_owner.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation finalization context requires its original Provider owner",
            ));
        }
        if state.applied_receipt.as_ref() != Some(receipt) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation finalization context requires its same actual Applied receipt",
            ));
        }
        if state.finalization_context.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original finalization context is already installed",
            ));
        }
        state.finalization_context = Some(context);
        Ok(())
    }
    /// Retain this original operation's Provider owner before its actual work.
    /// The outer Source keeps this slot through cleanup/Unknown disposition;
    /// neither recording it nor observing it grants execution or capacity.
    pub fn retain_original_owner(
        &self,
        payload: ConnectorProcessLocalPayload,
    ) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request = state.request.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original request is not installed",
            )
        })?;
        payload.validate_owner(&ConnectorProviderBindingKey {
            instance_id: request.descriptor.instance_id.clone(),
            incarnation: request.target.incarnation,
        })?;
        if !state.dispatched {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original owner precedes Provider dispatch",
            ));
        }
        if state.original_owner.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original Provider owner is already installed",
            ));
        }
        state.original_owner = Some(payload);
        Ok(())
    }
    fn finish_provider_drop(&self, unwinding: bool) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .provider_exited = true;
        if !unwinding {
            self.in_foreign.store(false, Ordering::Release);
        }
    }
    fn completed_result(&self) -> Result<(), ConnectorError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.provider_exited || self.foreign_exit_unproven() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "catalog mutation original Provider exit is unproven",
            ));
        }
        state
            .validation
            .as_ref()
            .expect("validated original catalog outcome")
            .clone()
    }
}

type CatalogMutationFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
                    ConnectorError,
                >,
            > + Send
            + 'a,
    >,
>;

struct CatalogMutationChild<'a> {
    future: Option<CatalogMutationFuture<'a>>,
    receiving: &'a ConnectorCatalogMutationReceiving,
}
impl CatalogMutationChild<'_> {
    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>,
    > {
        if !self.receiving.enter_poll() {
            return std::task::Poll::Pending;
        }
        let result = self
            .future
            .as_mut()
            .expect("same original catalog child")
            .as_mut()
            .poll(cx);
        if result.is_pending() {
            self.receiving.leave_pending();
        }
        result
    }
}
impl Drop for CatalogMutationChild<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let unwinding = std::thread::panicking();
            self.receiving.in_foreign.store(true, Ordering::Release);
            drop(future);
            self.receiving.finish_provider_drop(unwinding);
        }
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
        ConnectorProcessLocalPayload, ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorTableIdentity, ProviderBindingEpoch,
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
    fn external_mutation_evidence_local_owner_preserves_wire_identity_and_exact_lifetime() {
        let bare = evidence();
        let wire = bare.try_to_wire_v1().unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: bare.descriptor().instance_id.clone(),
            incarnation: bare.incarnation(),
        };
        let content = Arc::new(String::from("provider-private-owner"));
        let weak = Arc::downgrade(&content);
        let attached = bare
            .clone()
            .with_process_local_payload(ConnectorProcessLocalPayload::new(owner, content.clone()))
            .unwrap();
        drop(content);
        assert_eq!(attached, bare);
        assert_eq!(attached.digest(), bare.digest());
        assert_eq!(attached.try_to_wire_v1().unwrap(), wire);
        let clone = attached.clone();
        let decoded = ExternalMutationEvidence::try_from_wire_v1(&wire).unwrap();
        let detached = attached.without_process_local_payload();
        assert_eq!(decoded, attached);
        assert!(decoded.process_local_payload().is_none());
        assert!(detached.process_local_payload().is_none());
        assert_eq!(
            clone
                .process_local_payload()
                .unwrap()
                .value::<String>("analytics")
                .unwrap()
                .as_str(),
            "provider-private-owner"
        );
        assert!(!format!("{attached:?}").contains("provider-private-owner"));
        drop(attached);
        assert!(weak.upgrade().is_some());
        drop(clone);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn external_mutation_evidence_rejects_foreign_or_replaced_local_owner() {
        let evidence = evidence();
        let exact = ConnectorProviderBindingKey {
            instance_id: evidence.descriptor().instance_id.clone(),
            incarnation: evidence.incarnation(),
        };
        for owner in [
            ConnectorProviderBindingKey {
                incarnation: ProviderBindingEpoch::new(),
                ..exact.clone()
            },
            ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::parse("other").unwrap(),
                ..exact.clone()
            },
        ] {
            assert_eq!(
                evidence
                    .clone()
                    .with_process_local_payload(ConnectorProcessLocalPayload::new(
                        owner,
                        Arc::new(1_u8)
                    ))
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
        let attached = evidence
            .with_process_local_payload(ConnectorProcessLocalPayload::new(
                exact.clone(),
                Arc::new(1_u8),
            ))
            .unwrap();
        assert_eq!(
            attached
                .with_process_local_payload(ConnectorProcessLocalPayload::new(
                    exact,
                    Arc::new(2_u8)
                ))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
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

    #[derive(Default)]
    struct AsyncGate {
        open: std::sync::atomic::AtomicBool,
        waiter: std::sync::Mutex<Option<std::task::Waker>>,
    }
    impl AsyncGate {
        fn open(&self) {
            self.open.store(true, std::sync::atomic::Ordering::Release);
            let waiter = self.waiter.lock().unwrap().take();
            if let Some(waiter) = waiter {
                waiter.wake();
            }
        }
    }
    struct AsyncCatalogCapability {
        descriptor: ConnectorInstanceDescriptor,
        epoch: ProviderBindingEpoch,
        gate: Arc<AsyncGate>,
        calls: std::sync::atomic::AtomicUsize,
        drops: std::sync::atomic::AtomicUsize,
        request: std::sync::Mutex<Option<super::ConnectorCatalogMutationRequest>>,
        drop_panic: bool,
        wrong_receipt: bool,
        early_applied: bool,
        panic_after_proof: bool,
        final_noop: bool,
        final_unknown: bool,
        enrich_receipt: bool,
        needed_before_cleanup: bool,
        cleanup_gate: Arc<AsyncGate>,
        cleanup_failed: bool,
    }
    impl super::ConnectorCatalogMutation for AsyncCatalogCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.epoch
        }
        fn execute(
            &self,
            _: super::ConnectorCatalogMutationRequest,
        ) -> Result<
            super::ExternalMutationOutcome<super::ConnectorCatalogMutationReceipt>,
            super::ConnectorError,
        > {
            panic!("asynchronous execution must never fall back to execute");
        }
        fn execute_async<'a>(
            &'a self,
            request: super::ConnectorCatalogMutationRequest,
            receiving: &'a super::ConnectorCatalogMutationReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            super::ExternalMutationOutcome<super::ConnectorCatalogMutationReceipt>,
                            super::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            assert_eq!(
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                0
            );
            *self.request.lock().unwrap() = Some(request.clone());
            Box::pin(AsyncCatalogChild {
                capability: self,
                request,
                receiving,
                needed_recorded: false,
            })
        }
        fn reconcile(
            &self,
            _: super::ConnectorCatalogMutationReconcileRequest,
        ) -> Result<
            super::ExternalMutationOutcome<super::ConnectorCatalogMutationReceipt>,
            super::ConnectorError,
        > {
            panic!("asynchronous execution must not reconcile");
        }
    }
    struct AsyncCatalogChild<'a> {
        capability: &'a AsyncCatalogCapability,
        request: super::ConnectorCatalogMutationRequest,
        receiving: &'a super::ConnectorCatalogMutationReceiving,
        needed_recorded: bool,
    }
    impl std::future::Future for AsyncCatalogChild<'_> {
        type Output = Result<
            super::ExternalMutationOutcome<super::ConnectorCatalogMutationReceipt>,
            super::ConnectorError,
        >;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            use super::*;
            let original = self.get_mut();
            let capability = original.capability;
            if !capability
                .gate
                .open
                .load(std::sync::atomic::Ordering::Acquire)
            {
                let previous = capability
                    .gate
                    .waiter
                    .lock()
                    .unwrap()
                    .replace(cx.waker().clone());
                drop(previous);
                return std::task::Poll::Pending;
            }
            let receipt = ConnectorCatalogMutationReceipt::try_new(
                capability.descriptor.clone(),
                capability.epoch,
                original.request.operation_id,
                original.request.operation.kind(),
                None,
            )
            .unwrap();
            if capability.early_applied {
                original.receiving.record_applied_receipt(&receipt).unwrap();
            }
            if capability.panic_after_proof {
                panic!("original catalog necessary finalization panic");
            }
            if capability.final_unknown {
                return std::task::Poll::Ready(Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "original unknown",
                    ),
                    evidence: ExternalMutationEvidence::try_new(
                        1,
                        capability.descriptor.clone(),
                        capability.epoch,
                        original.request.operation_id,
                        original.request.operation.kind(),
                        Bytes::from_static(b"same-operation-evidence"),
                    )
                    .unwrap(),
                }));
            }
            let receipt = if capability.wrong_receipt {
                ConnectorCatalogMutationReceipt::try_new(
                    capability.descriptor.clone(),
                    capability.epoch,
                    ConnectorMutationOperationId::new(),
                    original.request.operation.kind(),
                    None,
                )
                .unwrap()
            } else if capability.enrich_receipt {
                ConnectorCatalogMutationReceipt::try_new(
                    capability.descriptor.clone(),
                    capability.epoch,
                    original.request.operation_id,
                    original.request.operation.kind(),
                    Some(Bytes::from_static(b"observed-final-version")),
                )
                .unwrap()
            } else {
                receipt
            };
            if capability.needed_before_cleanup {
                if !original.needed_recorded {
                    original
                        .receiving
                        .record_needed_committed(
                            if capability.final_noop {
                                ExternalMutationEffect::NoOp
                            } else {
                                ExternalMutationEffect::Applied
                            },
                            &receipt,
                            &ExternalMutationFinalization::Complete,
                        )
                        .unwrap();
                    original.needed_recorded = true;
                }
                if !capability
                    .cleanup_gate
                    .open
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let previous = capability
                        .cleanup_gate
                        .waiter
                        .lock()
                        .unwrap()
                        .replace(cx.waker().clone());
                    drop(previous);
                    return std::task::Poll::Pending;
                }
            }
            std::task::Poll::Ready(Ok(ExternalMutationOutcome::KnownCommitted {
                effect: if capability.final_noop {
                    ExternalMutationEffect::NoOp
                } else {
                    ExternalMutationEffect::Applied
                },
                receipt,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: if capability.cleanup_failed {
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "original pure cleanup failure",
                    ))
                } else {
                    ExternalMutationFinalization::Complete
                },
            }))
        }
    }
    impl Drop for AsyncCatalogChild<'_> {
        fn drop(&mut self) {
            self.capability
                .drops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.capability.drop_panic {
                panic!("original catalog child Drop panic");
            }
        }
    }
    fn async_capability() -> AsyncCatalogCapability {
        AsyncCatalogCapability {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("analytics").unwrap(),
            },
            epoch: ProviderBindingEpoch::from_bytes([3; 16]),
            gate: Arc::new(AsyncGate::default()),
            calls: std::sync::atomic::AtomicUsize::new(0),
            drops: std::sync::atomic::AtomicUsize::new(0),
            request: std::sync::Mutex::new(None),
            drop_panic: false,
            wrong_receipt: false,
            early_applied: false,
            panic_after_proof: false,
            final_noop: false,
            final_unknown: false,
            enrich_receipt: false,
            needed_before_cleanup: false,
            cleanup_gate: Arc::new(AsyncGate::default()),
            cleanup_failed: false,
        }
    }
    fn async_lease(
        capability: &Arc<AsyncCatalogCapability>,
    ) -> super::ConnectorCatalogMutationLease {
        super::ConnectorCatalogMutationLease::new(
            capability.descriptor.clone(),
            crate::connector::ConnectorControlRuntimeId::new(),
            capability.epoch,
            capability.clone(),
            || {},
        )
        .unwrap()
    }
    fn async_request(
        lease: &super::ConnectorCatalogMutationLease,
    ) -> super::ConnectorCatalogMutationRequest {
        super::ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: ConnectorProviderBindingKey {
                instance_id: lease.descriptor.instance_id.clone(),
                incarnation: lease.provider_incarnation,
            },
            operation: ConnectorCatalogMutationOperation::DropNamespace {
                namespace: crate::connector::ConnectorNamespaceIdentity {
                    instance_id: lease.descriptor.instance_id.clone(),
                    namespace: "db".into(),
                },
                policy: super::DropPolicy::FailIfMissing,
            },
            context: crate::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                crate::connector::ConnectorStopOwner::new().view(),
                1024,
                4096,
            )
            .unwrap(),
        }
    }
    fn poll_async<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[test]
    fn async_catalog_cancelled_borrow_retains_same_pending_child_and_exact_request() {
        use super::*;
        use std::sync::atomic::Ordering;
        struct WakeCount(std::sync::atomic::AtomicUsize);
        impl std::task::Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let capability = Arc::new(async_capability());
        let lease = async_lease(&capability);
        let request = async_request(&lease);
        let same = request.clone();
        let marker = same
            .context
            .request_scope_extension_or_insert_with(|| 37_u64);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut original = Box::pin(lease.execute_async_in(request, &receiving));
        let wakes = Arc::new(WakeCount(std::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(wakes.clone());
        {
            let mut borrowed = Box::pin(std::future::poll_fn(|cx| original.as_mut().poll(cx)));
            assert!(
                borrowed
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert_eq!(capability.calls.load(Ordering::SeqCst), 1);
        assert_eq!(capability.drops.load(Ordering::SeqCst), 0);
        {
            let observed = receiving.observation();
            assert_eq!(observed.descriptor(), Some(lease.descriptor()));
            assert_eq!(
                observed.control_runtime_id(),
                Some(lease.control_runtime_id())
            );
            assert_eq!(observed.target(), Some(&same.target));
            assert_eq!(observed.operation_id(), Some(same.operation_id));
            assert_eq!(observed.operation_kind(), Some(same.operation.kind()));
            assert!(observed.dispatched());
            assert!(!observed.provider_exited());
            assert!(observed.raw_outcome().is_none());
        }
        let foreign_receipt = ConnectorCatalogMutationReceipt::try_new(
            lease.descriptor().clone(),
            lease.provider_incarnation,
            ConnectorMutationOperationId::new(),
            same.operation.kind(),
            None,
        )
        .unwrap();
        assert!(receiving.record_applied_receipt(&foreign_receipt).is_err());
        assert!(receiving.observation().applied_receipt().is_none());
        capability.gate.open();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            poll_async(original.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(capability.calls.load(Ordering::SeqCst), 1);
        assert_eq!(capability.drops.load(Ordering::SeqCst), 1);
        {
            let actual = capability.request.lock().unwrap();
            let actual = actual.as_ref().unwrap();
            assert_eq!(actual.operation_id, same.operation_id);
            assert_eq!(actual.target, same.target);
            assert_eq!(actual.operation, same.operation);
            assert_eq!(actual.context.deadline(), same.context.deadline());
            let actual_marker = actual
                .context
                .request_scope_extension_or_insert_with::<u64>(|| panic!("same context"));
            assert!(Arc::ptr_eq(&marker, &actual_marker));
        }
        let observed = receiving.observation();
        assert!(observed.provider_exited());
        assert!(matches!(
            observed.validated_outcome(),
            Some(Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                ..
            }))
        ));
        drop(observed);
        let mut duplicate = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
        assert!(matches!(
            poll_async(duplicate.as_mut()),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(capability.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            receiving.observation().operation_id(),
            Some(same.operation_id)
        );
    }

    #[test]
    fn async_catalog_default_is_unsupported_without_sync_fallback() {
        use super::*;
        struct SyncOnly(ConnectorInstanceDescriptor);
        impl ConnectorCatalogMutation for SyncOnly {
            fn descriptor(&self) -> &ConnectorInstanceDescriptor {
                &self.0
            }
            fn incarnation(&self) -> ProviderBindingEpoch {
                ProviderBindingEpoch::from_bytes([1; 16])
            }
            fn execute(
                &self,
                _: ConnectorCatalogMutationRequest,
            ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
            {
                panic!("no sync fallback");
            }
            fn reconcile(
                &self,
                _: ConnectorCatalogMutationReconcileRequest,
            ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
            {
                panic!("no reconcile");
            }
        }
        let capability = SyncOnly(async_capability().descriptor);
        let lease = ConnectorCatalogMutationLease::new(
            capability.0.clone(),
            ConnectorControlRuntimeId::new(),
            capability.incarnation(),
            Arc::new(capability),
            || {},
        )
        .unwrap();
        let receiving = ConnectorCatalogMutationReceiving::new();
        let request = async_request(&lease);
        let mut original = Box::pin(lease.execute_operation_async_in(
            request.operation_id,
            request.operation,
            request.context,
            &receiving,
        ));
        let std::task::Poll::Ready(Err(error)) = poll_async(original.as_mut()) else {
            panic!("explicit missing capability");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        let observed = receiving.observation();
        assert!(observed.provider_exited());
        assert!(
            matches!(observed.raw_outcome(), Some(Err(error)) if error.kind() == ConnectorErrorKind::Unsupported)
        );
    }

    #[test]
    fn async_catalog_raw_and_validated_commit_precede_child_drop_panic() {
        use super::*;
        let mut capability = async_capability();
        capability.drop_panic = true;
        capability.early_applied = true;
        capability.gate.open();
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut original = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_async(original.as_mut())
        }))
        .expect_err("actual child Drop panic must propagate");
        assert_eq!(
            panic.downcast_ref::<&'static str>(),
            Some(&"original catalog child Drop panic")
        );
        let observed = receiving.observation();
        assert!(matches!(
            observed.raw_outcome(),
            Some(Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                ..
            }))
        ));
        assert!(matches!(observed.validation(), Some(Ok(()))));
        assert!(observed.applied_receipt().is_some());
        assert!(!observed.provider_exited());
        assert!(receiving.foreign_exit_unproven());
        assert_eq!(
            capability.drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            capability.calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn async_catalog_wrong_scope_or_outcome_never_overwrites_original_facts() {
        use super::*;
        let capability = Arc::new(async_capability());
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut request = async_request(&lease);
        request.target.incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
        let same_id = request.operation_id;
        let mut original = Box::pin(lease.execute_async_in(request, &receiving));
        assert!(
            matches!(poll_async(original.as_mut()), std::task::Poll::Ready(Err(error)) if error.kind() == ConnectorErrorKind::InvalidRequest)
        );
        let observed = receiving.observation();
        assert_eq!(observed.operation_id(), Some(same_id));
        assert!(!observed.dispatched());
        assert!(!observed.provider_exited());
        assert!(observed.raw_outcome().is_none());
        assert_eq!(
            capability.calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(observed);
        let mut capability = async_capability();
        capability.wrong_receipt = true;
        capability.gate.open();
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let request = async_request(&lease);
        let same_id = request.operation_id;
        let mut original = Box::pin(lease.execute_async_in(request, &receiving));
        assert!(
            matches!(poll_async(original.as_mut()), std::task::Poll::Ready(Err(error)) if error.kind() == ConnectorErrorKind::InvalidRequest)
        );
        let observed = receiving.observation();
        assert!(
            matches!(observed.raw_outcome(), Some(Ok(ExternalMutationOutcome::KnownCommitted { receipt, .. })) if receipt.operation_id() != same_id)
        );
        assert!(matches!(observed.validated_outcome(), Some(Err(_))));
        assert!(observed.provider_exited());
        assert!(!receiving.foreign_exit_unproven());
    }

    #[test]
    fn async_catalog_early_applied_fact_survives_panic_before_provider_ready() {
        use super::*;
        let mut capability = async_capability();
        capability.early_applied = true;
        capability.panic_after_proof = true;
        capability.gate.open();
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut original = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_async(original.as_mut())
        }))
        .expect_err("actual necessary finalization panic must propagate");
        assert_eq!(
            panic.downcast_ref::<&'static str>(),
            Some(&"original catalog necessary finalization panic")
        );
        let observed = receiving.observation();
        assert!(observed.applied_receipt().is_some());
        assert!(observed.raw_outcome().is_none());
        assert!(observed.validation().is_none());
        assert!(receiving.foreign_exit_unproven());
        assert_eq!(
            capability.calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            capability.drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn async_catalog_applied_receipt_allows_enrichment_but_not_verdict_regression() {
        use super::*;
        let mut capability = async_capability();
        capability.early_applied = true;
        capability.enrich_receipt = true;
        capability.gate.open();
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut original = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
        assert!(matches!(
            poll_async(original.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        {
            let observed = receiving.observation();
            assert!(
                observed
                    .applied_receipt()
                    .unwrap()
                    .provider_version()
                    .is_none()
            );
            assert!(
                matches!(observed.validated_outcome(), Some(Ok(ExternalMutationOutcome::KnownCommitted { receipt, .. })) if receipt.provider_version().is_some())
            );
        }
        for unknown in [false, true] {
            let mut capability = async_capability();
            capability.early_applied = true;
            capability.final_unknown = unknown;
            capability.final_noop = !unknown;
            capability.gate.open();
            let capability = Arc::new(capability);
            let lease = async_lease(&capability);
            let receiving = ConnectorCatalogMutationReceiving::new();
            let mut original = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
            assert!(
                matches!(poll_async(original.as_mut()), std::task::Poll::Ready(Err(error)) if error.kind() == ConnectorErrorKind::InvalidRequest)
            );
            let observed = receiving.observation();
            assert!(observed.applied_receipt().is_some());
            assert!(observed.raw_outcome().is_some());
            assert!(matches!(observed.validation(), Some(Err(_))));
            assert!(observed.provider_exited());
        }
        let mut capability = async_capability();
        capability.final_noop = true;
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let request = async_request(&lease);
        let original_context = request.context.clone();
        let key = request.target.clone();
        let mut original = Box::pin(lease.execute_async_in(request, &receiving));
        assert!(poll_async(original.as_mut()).is_pending());
        receiving
            .retain_original_owner(ConnectorProcessLocalPayload::new(key, capability.clone()))
            .unwrap();
        capability.gate.open();
        assert!(matches!(
            poll_async(original.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        let observed = receiving.observation();
        assert!(observed.applied_receipt().is_none());
        let Some(Ok(ExternalMutationOutcome::KnownCommitted {
            effect, receipt, ..
        })) = observed.validated_outcome()
        else {
            panic!("the actual NoOp remains a successful outcome");
        };
        assert_eq!(*effect, ExternalMutationEffect::NoOp);
        let receipt = receipt.clone();
        drop(observed);
        assert!(
            receiving
                .record_finalization_context(&receipt, Ok(original_context))
                .is_err()
        );
        assert!(receiving.observation().finalization_context().is_none());
    }

    #[test]
    fn async_catalog_needed_committed_is_visible_while_same_child_cleanup_is_pending() {
        use super::*;
        let mut capability = async_capability();
        capability.early_applied = true;
        capability.needed_before_cleanup = true;
        capability.cleanup_failed = true;
        capability.gate.open();
        let capability = Arc::new(capability);
        let lease = async_lease(&capability);
        let receiving = ConnectorCatalogMutationReceiving::new();
        let mut original = Box::pin(lease.execute_async_in(async_request(&lease), &receiving));
        assert!(poll_async(original.as_mut()).is_pending());
        let needed_receipt = {
            let observed = receiving.observation();
            let needed = observed
                .needed_committed()
                .expect("actual necessary business frontier");
            assert_eq!(needed.effect(), ExternalMutationEffect::Applied);
            assert_eq!(
                needed.finalization(),
                &ExternalMutationFinalization::Complete
            );
            assert!(observed.raw_outcome().is_none());
            assert!(!observed.provider_exited());
            assert!(matches!(observed.needed_validation(), Some(Ok(()))));
            needed.receipt().clone()
        };
        assert_eq!(
            capability.drops.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        capability.cleanup_gate.open();
        assert!(matches!(
            poll_async(original.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        let observed = receiving.observation();
        let needed = observed.needed_committed().unwrap();
        assert_eq!(needed.receipt(), &needed_receipt);
        assert_eq!(
            needed.finalization(),
            &ExternalMutationFinalization::Complete
        );
        assert!(
            matches!(observed.raw_outcome(), Some(Ok(ExternalMutationOutcome::KnownCommitted {
            cleanup: ExternalMutationFinalization::Failed(failure), ..
        })) if failure.message() == "original pure cleanup failure")
        );
        assert!(observed.provider_exited());
        assert_eq!(
            capability.calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            capability.drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn async_catalog_original_owner_outlives_child_and_rejected_owner_drops_unlocked() {
        use super::*;
        use std::sync::atomic::Ordering;
        struct OwnerProbe {
            receiving: std::sync::Weak<ConnectorCatalogMutationReceiving>,
            drops: Arc<std::sync::atomic::AtomicUsize>,
            dropped_under_lock: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for OwnerProbe {
            fn drop(&mut self) {
                if let Some(receiving) = self.receiving.upgrade() {
                    let locked = receiving.state.try_lock().is_err();
                    self.dropped_under_lock.fetch_or(locked, Ordering::SeqCst);
                }
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let capability = Arc::new(async_capability());
        let lease = async_lease(&capability);
        let request = async_request(&lease);
        let key = request.target.clone();
        let receiving = Arc::new(ConnectorCatalogMutationReceiving::new());
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dropped_under_lock = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let make_owner = || {
            Arc::new(OwnerProbe {
                receiving: Arc::downgrade(&receiving),
                drops: drops.clone(),
                dropped_under_lock: dropped_under_lock.clone(),
            })
        };
        let mut original = Box::pin(lease.execute_async_in(request, &receiving));
        assert!(poll_async(original.as_mut()).is_pending());
        let owner = make_owner();
        let weak = Arc::downgrade(&owner);
        receiving
            .retain_original_owner(ConnectorProcessLocalPayload::new(key.clone(), owner))
            .unwrap();
        let mut wrong = key.clone();
        wrong.incarnation = ProviderBindingEpoch::from_bytes([9; 16]);
        assert!(
            receiving
                .retain_original_owner(ConnectorProcessLocalPayload::new(wrong, make_owner()))
                .is_err()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            receiving
                .retain_original_owner(ConnectorProcessLocalPayload::new(key.clone(), make_owner()))
                .is_err()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(!dropped_under_lock.load(Ordering::SeqCst));
        {
            let observed = receiving.observation();
            let stored = observed
                .original_owner()
                .unwrap()
                .value::<OwnerProbe>(key.instance_id())
                .unwrap();
            assert!(std::ptr::eq(Arc::as_ptr(stored), weak.as_ptr()));
        }
        capability.gate.open();
        assert!(matches!(
            poll_async(original.as_mut()),
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(receiving.observation().provider_exited());
        assert!(weak.upgrade().is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(original);
        drop(receiving);
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert!(!dropped_under_lock.load(Ordering::SeqCst));
    }

    #[test]
    fn async_catalog_finalization_context_is_same_original_once_and_keeps_refusal() {
        use super::*;
        for refuse in [false, true] {
            let capability = Arc::new(async_capability());
            let lease = async_lease(&capability);
            let request = async_request(&lease);
            let receipt = ConnectorCatalogMutationReceipt::try_new(
                lease.descriptor().clone(),
                request.target.incarnation,
                request.operation_id,
                request.operation.kind(),
                None,
            )
            .unwrap();
            let original_context = request.context.clone();
            let deadline = original_context.deadline();
            let marker = original_context.request_scope_extension_or_insert_with(|| 193_u64);
            let receiving = ConnectorCatalogMutationReceiving::new();
            let key = request.target.clone();
            let mut original = Box::pin(lease.execute_async_in(request, &receiving));
            assert!(poll_async(original.as_mut()).is_pending());
            receiving
                .retain_original_owner(ConnectorProcessLocalPayload::new(key, Arc::new(())))
                .unwrap();
            let foreign = ConnectorCatalogMutationReceipt::try_new(
                lease.descriptor().clone(),
                lease.provider_incarnation,
                ConnectorMutationOperationId::new(),
                receipt.operation_kind(),
                None,
            )
            .unwrap();
            assert!(
                receiving
                    .record_finalization_context(&foreign, Ok(original_context.clone()))
                    .is_err()
            );
            assert!(receiving.observation().finalization_context().is_none());
            assert!(
                receiving
                    .record_finalization_context(&receipt, Ok(original_context.clone()))
                    .is_err()
            );
            assert!(receiving.observation().finalization_context().is_none());
            receiving.record_applied_receipt(&receipt).unwrap();
            let refusal = ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "original finite phase refused",
            );
            let context = if refuse {
                Err(refusal.clone())
            } else {
                Ok(original_context.clone())
            };
            receiving
                .record_finalization_context(&receipt, context)
                .unwrap();
            assert!(
                receiving
                    .record_finalization_context(&receipt, Ok(original_context))
                    .is_err()
            );
            {
                let observed = receiving.observation();
                assert!(!observed.provider_exited());
                assert!(
                    observed.applied_receipt() == Some(&receipt),
                    "the original Applied fact precedes its continuation context"
                );
                assert!(observed.needed_committed().is_none());
                match observed.finalization_context().unwrap() {
                    Ok(context) => {
                        assert!(!refuse);
                        assert_eq!(context.deadline(), deadline);
                        let same = context.request_scope_extension_or_insert_with::<u64>(|| {
                            panic!("same original context")
                        });
                        assert!(Arc::ptr_eq(&same, &marker));
                    }
                    Err(error) => {
                        assert!(refuse);
                        assert_eq!(error, &refusal);
                    }
                }
            }
            capability.gate.open();
            assert!(matches!(
                poll_async(original.as_mut()),
                std::task::Poll::Ready(Ok(()))
            ));
            assert!(receiving.observation().provider_exited());
        }
    }
}
