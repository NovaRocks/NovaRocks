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
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorNamespaceIdentity,
    ConnectorRequestContext, ConnectorTableIdentity,
};

/// Largest provider-owned reconciliation payload accepted by the control plane.
pub const MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES: usize = 64 * 1024;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorViewDefinition {
    pub dialect: ConnectorViewDialect,
    pub sql: Arc<str>,
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

/// Bounded proof that an internal MV publication belongs to one refresh attempt.
///
/// The token intentionally has no `Debug` representation. It is validated by the
/// provider against the source snapshot summary immediately before publication.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorRefreshPublicationGuard {
    refresh_id: i64,
    materialized_view_id: i64,
    token: Arc<str>,
}

impl ConnectorRefreshPublicationGuard {
    pub const MAX_TOKEN_BYTES: usize = 256;

    pub fn try_new(
        refresh_id: i64,
        materialized_view_id: i64,
        token: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        if refresh_id <= 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV refresh publication guard refresh id must be positive",
            ));
        }
        if materialized_view_id <= 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV refresh publication guard materialized view id must be positive",
            ));
        }
        let token = token.into();
        if token.is_empty() || token.len() > Self::MAX_TOKEN_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV refresh publication guard token must be non-empty and at most 256 bytes",
            ));
        }
        Ok(Self {
            refresh_id,
            materialized_view_id,
            token,
        })
    }

    pub const fn refresh_id(&self) -> i64 {
        self.refresh_id
    }

    pub const fn materialized_view_id(&self) -> i64 {
        self.materialized_view_id
    }

    /// Provider-only input for authoritative snapshot-summary validation.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Stable redacted identity suitable for bounded provider evidence.
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.connector.refresh-publication-guard.v1");
        hasher.update(self.refresh_id.to_be_bytes());
        hasher.update(self.materialized_view_id.to_be_bytes());
        hasher.update(self.token.as_bytes());
        hasher.finalize().into()
    }
}

impl fmt::Debug for ConnectorRefreshPublicationGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorRefreshPublicationGuard")
            .field("refresh_id", &self.refresh_id)
            .field("materialized_view_id", &self.materialized_view_id)
            .field("token_len", &self.token.len())
            .field("digest", &self.digest())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorRefAction {
    Create {
        kind: ConnectorRefKind,
        name: Arc<str>,
        snapshot_id: Option<i64>,
        policy: CreateOrReplacePolicy,
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
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
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
            Self::DropTable { .. } => "drop-table",
            Self::CreateView { .. } => "create-view",
            Self::DropView { .. } => "drop-view",
            Self::AlterSchema { .. } => "alter-schema",
            Self::AlterPartitionSpec { .. } => "alter-partition-spec",
            Self::AlterProperties { .. } => "alter-properties",
            Self::AlterRef { .. } => "alter-ref",
        }
    }
}

#[derive(Clone)]
pub struct ConnectorCatalogMutationRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub target: ConnectorExecutionBindingKey,
    pub operation: ConnectorCatalogMutationOperation,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorCatalogMutationReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    provider_version: Option<Bytes>,
}

impl ConnectorCatalogMutationReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
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
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
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
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalMutationEvidence {
    schema_version: u16,
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    provider_payload: Bytes,
}

impl ExternalMutationEvidence {
    pub fn try_new(
        schema_version: u16,
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
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
    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
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
    fn incarnation(&self) -> ConnectorInstanceIncarnation;
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
}

#[derive(Clone)]
pub struct ConnectorCatalogMutationLease {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    mutation: Arc<dyn ConnectorCatalogMutation>,
    _release: Arc<MutationLeaseRelease>,
}

struct MutationLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorCatalogMutationLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        mutation: Arc<dyn ConnectorCatalogMutation>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if mutation.descriptor() != &descriptor || mutation.incarnation() != incarnation {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector mutation capability does not match its lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            mutation,
            _release: Arc::new(MutationLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
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
            || request.target.incarnation != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector mutation request does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_evidence(&self, evidence: &ExternalMutationEvidence) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor || evidence.incarnation() != self.incarnation {
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
                    || receipt.incarnation() != self.incarnation
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
