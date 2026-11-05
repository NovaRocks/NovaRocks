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

//! FE-only provider-neutral data mutation contract.
// Design: ADR-0024 (docs/adr/ADR-0024-connector-data-mutation-contract.md)

use std::fmt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorMetadata, ConnectorMutationOperationId,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableHandle,
    ExternalMutationEvidence, ExternalMutationOutcome, ProviderBindingEpoch,
};

pub const CONNECTOR_DATA_MUTATION_CONTRACT_VERSION: u16 = 3;
pub const CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION: u16 = 2;
pub const MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES: usize = 8 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES: usize = 256;
pub const MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_FILES: u32 = 4096;
pub const MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES: usize = 64 * 1024 * 1024;

const REQUEST_DIGEST_DOMAIN: &[u8] = b"novarocks.connector-data-mutation.request.v1\0";
const PLAN_DIGEST_DOMAIN: &[u8] = b"novarocks.connector-data-mutation.plan.v1\0";

pub const REGISTER_EXISTING_FILES_KIND: &str = "register-existing-files";
pub const TRUNCATE_KIND: &str = "truncate";
const RECEIPT_PAYLOAD_DIGEST_DOMAIN: &[u8] =
    b"novarocks.connector-data-mutation.receipt-payload.v1\0";

// Design: ADR-0046 (docs/adr/ADR-0046-frontend-add-files-source-ownership.md)
/// Provider-calculated, secret-free physical ownership scope for a bounded
/// existing-files plan.  It deliberately contains no operation, target, or
/// connector-instance identity, so the frontend can protect one physical
/// source location across retries and catalog instances without retaining the
/// provider-private manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDataMutationSourceScopeKind {
    Directory,
}

impl ConnectorDataMutationSourceScopeKind {
    const DIRECTORY_TAG: u8 = 1;

    const fn to_wire_tag(self) -> u8 {
        match self {
            Self::Directory => Self::DIRECTORY_TAG,
        }
    }

    fn try_from_wire_tag(tag: u8) -> Result<Self, ConnectorError> {
        match tag {
            Self::DIRECTORY_TAG => Ok(Self::Directory),
            _ => Err(corrupt(
                "unsupported connector data mutation source scope kind",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDataMutationSourceScope {
    version: u16,
    kind: ConnectorDataMutationSourceScopeKind,
    digest: [u8; 32],
}

impl ConnectorDataMutationSourceScope {
    pub const VERSION: u16 = 1;

    pub fn try_new_directory(digest: [u8; 32]) -> Result<Self, ConnectorError> {
        let scope = Self {
            version: Self::VERSION,
            kind: ConnectorDataMutationSourceScopeKind::Directory,
            digest,
        };
        scope.validate()?;
        Ok(scope)
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn kind(self) -> ConnectorDataMutationSourceScopeKind {
        self.kind
    }

    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub fn validate(self) -> Result<(), ConnectorError> {
        if self.version != Self::VERSION || self.digest == [0; 32] {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation source scope is invalid",
            ));
        }
        Ok(())
    }

    fn digest_into(self, hasher: &mut Sha256) {
        hasher.update(self.version.to_be_bytes());
        hasher.update([self.kind.to_wire_tag()]);
        hasher.update(self.digest);
    }
}

/// The source-side ownership contract for `register-existing-files`.
///
/// This is deliberately distinct from the source-scope digest. The digest is
/// only a stable identity useful for best-effort de-duplication of concurrent
/// work; this value records the provider's proof that source cleanup stays
/// outside NovaRocks' authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDataMutationSourceDomain {
    CallerManagedStable,
}

impl ConnectorDataMutationSourceDomain {
    const CALLER_MANAGED_STABLE_TAG: u8 = 1;

    const fn to_wire_tag(self) -> u8 {
        match self {
            Self::CallerManagedStable => Self::CALLER_MANAGED_STABLE_TAG,
        }
    }

    fn try_from_wire_tag(tag: u8) -> Result<Self, ConnectorError> {
        match tag {
            Self::CALLER_MANAGED_STABLE_TAG => Ok(Self::CallerManagedStable),
            _ => Err(corrupt("unsupported connector data mutation source domain")),
        }
    }
}

/// Provider-attested ADD FILES domain facts.
///
/// `target_cleanup_root_digest` identifies the exact target cleanup root whose
/// disjointness was proved during planning. It is not a source lease and never
/// authorizes NovaRocks to mutate the source root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDataMutationAddFilesDomain {
    version: u16,
    source_domain: ConnectorDataMutationSourceDomain,
    target_cleanup_root_digest: [u8; 32],
}

impl ConnectorDataMutationAddFilesDomain {
    pub const VERSION: u16 = 1;

    pub fn try_new_caller_managed_stable(
        target_cleanup_root_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        let domain = Self {
            version: Self::VERSION,
            source_domain: ConnectorDataMutationSourceDomain::CallerManagedStable,
            target_cleanup_root_digest,
        };
        domain.validate()?;
        Ok(domain)
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn source_domain(self) -> ConnectorDataMutationSourceDomain {
        self.source_domain
    }

    pub const fn target_cleanup_root_digest(self) -> [u8; 32] {
        self.target_cleanup_root_digest
    }

    pub fn validate(self) -> Result<(), ConnectorError> {
        if self.version != Self::VERSION || self.target_cleanup_root_digest == [0; 32] {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation ADD FILES source domain is invalid",
            ));
        }
        Ok(())
    }

    fn digest_into(self, hasher: &mut Sha256) {
        hasher.update(self.version.to_be_bytes());
        hasher.update([self.source_domain.to_wire_tag()]);
        hasher.update(self.target_cleanup_root_digest);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDataMutationOperation {
    RegisterExistingFiles {
        table: ConnectorTableHandle,
        source_location: Arc<str>,
    },
    Truncate {
        table: ConnectorTableHandle,
        target_ref: Arc<str>,
    },
}

impl ConnectorDataMutationOperation {
    pub fn register_existing_files(
        table: ConnectorTableHandle,
        source_location: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::RegisterExistingFiles {
            table,
            source_location: source_location.into(),
        };
        operation.validate()?;
        Ok(operation)
    }

    pub fn truncate(
        table: ConnectorTableHandle,
        target_ref: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let operation = Self::Truncate {
            table,
            target_ref: target_ref.into(),
        };
        operation.validate()?;
        Ok(operation)
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RegisterExistingFiles { .. } => REGISTER_EXISTING_FILES_KIND,
            Self::Truncate { .. } => TRUNCATE_KIND,
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RegisterExistingFiles { table, .. } | Self::Truncate { table, .. } => table,
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::RegisterExistingFiles {
                source_location, ..
            } => validate_bounded_text(
                source_location,
                MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES,
                "connector data mutation source location",
            ),
            Self::Truncate { target_ref, .. } => validate_bounded_text(
                target_ref,
                MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES,
                "connector data mutation target ref",
            ),
        }
    }

    fn digest_into(&self, hasher: &mut Sha256) {
        digest_bytes(hasher, self.kind().as_bytes());
        digest_bytes(hasher, self.table().owner().as_str().as_bytes());
        digest_bytes(hasher, self.table().payload());
        match self {
            Self::RegisterExistingFiles {
                source_location, ..
            } => digest_bytes(hasher, source_location.as_bytes()),
            Self::Truncate { target_ref, .. } => digest_bytes(hasher, target_ref.as_bytes()),
        }
    }
}

#[derive(Clone)]
pub struct ConnectorDataMutationPlanningRequest {
    operation_id: ConnectorMutationOperationId,
    owner: ConnectorProviderBindingKey,
    operation: ConnectorDataMutationOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationPlanningRequest {
    pub fn try_new(
        operation_id: ConnectorMutationOperationId,
        owner: ConnectorProviderBindingKey,
        operation: ConnectorDataMutationOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation table handle does not match the exact owner",
            ));
        }
        let request_digest = request_digest(operation_id, &owner, &operation);
        Ok(Self {
            operation_id,
            owner,
            operation,
            request_digest,
            context,
        })
    }

    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub fn operation(&self) -> &ConnectorDataMutationOperation {
        &self.operation
    }

    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.operation.validate()?;
        if self.operation.table().owner() != &self.owner.instance_id
            || request_digest(self.operation_id, &self.owner, &self.operation)
                != self.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation planning request digest or owner is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDataMutationPlanningRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationPlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("table_payload_len", &self.operation.table().payload().len())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDataMutationPlanSummary {
    file_count: u32,
    row_count: u64,
    total_bytes: u64,
}

impl ConnectorDataMutationPlanSummary {
    pub fn try_new(
        file_count: u32,
        row_count: u64,
        total_bytes: u64,
    ) -> Result<Self, ConnectorError> {
        if file_count > MAX_CONNECTOR_DATA_MUTATION_FILES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector data mutation file count exceeds the hard limit",
            ));
        }
        Ok(Self {
            file_count,
            row_count,
            total_bytes,
        })
    }

    pub const fn file_count(self) -> u32 {
        self.file_count
    }

    pub const fn row_count(self) -> u64 {
        self.row_count
    }

    pub const fn total_bytes(self) -> u64 {
        self.total_bytes
    }

    fn digest_into(self, hasher: &mut Sha256) {
        hasher.update(self.file_count.to_be_bytes());
        hasher.update(self.row_count.to_be_bytes());
        hasher.update(self.total_bytes.to_be_bytes());
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDataMutationPlan {
    schema_version: u16,
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    add_files_domain: Option<ConnectorDataMutationAddFilesDomain>,
    provider_payload: Bytes,
    plan_digest: [u8; 32],
}

impl ConnectorDataMutationPlan {
    pub fn try_new(
        request: &ConnectorDataMutationPlanningRequest,
        state_digest: [u8; 32],
        summary: ConnectorDataMutationPlanSummary,
        source_scope: Option<ConnectorDataMutationSourceScope>,
        add_files_domain: Option<ConnectorDataMutationAddFilesDomain>,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_provider_payload(&provider_payload, "plan")?;
        validate_add_files_facts(request.operation().kind(), source_scope, add_files_domain)?;
        let operation_kind: Arc<str> = request.operation.kind().into();
        let plan_digest = plan_digest(
            request.request_digest,
            state_digest,
            summary,
            source_scope,
            add_files_domain,
            &provider_payload,
        );
        Ok(Self {
            schema_version: CONNECTOR_DATA_MUTATION_CONTRACT_VERSION,
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            operation_kind,
            request_digest: request.request_digest,
            state_digest,
            summary,
            source_scope,
            add_files_domain,
            provider_payload,
            plan_digest,
        })
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }

    pub fn operation_kind(&self) -> &str {
        &self.operation_kind
    }

    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }

    pub const fn summary(&self) -> ConnectorDataMutationPlanSummary {
        self.summary
    }

    pub const fn source_scope(&self) -> Option<ConnectorDataMutationSourceScope> {
        self.source_scope
    }

    pub const fn add_files_domain(&self) -> Option<ConnectorDataMutationAddFilesDomain> {
        self.add_files_domain
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_operation_kind(&self.operation_kind)?;
        validate_provider_payload(&self.provider_payload, "plan")?;
        validate_add_files_facts(
            &self.operation_kind,
            self.source_scope,
            self.add_files_domain,
        )?;
        if self.schema_version != CONNECTOR_DATA_MUTATION_CONTRACT_VERSION
            || plan_digest(
                self.request_digest,
                self.state_digest,
                self.summary,
                self.source_scope,
                self.add_files_domain,
                &self.provider_payload,
            ) != self.plan_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan digest or version is invalid",
            ));
        }
        Ok(())
    }

    /// Canonical, bounded frontend-durable representation. The provider
    /// payload remains opaque; decoding revalidates immutable identity and
    /// digest before returning a plan.
    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        const MAGIC: &[u8; 4] = b"CDP1";
        let instance_id = self.owner.instance_id.as_str().as_bytes();
        let operation_kind = self.operation_kind.as_bytes();
        let payload = self.provider_payload.as_ref();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.schema_version.to_be_bytes());
        write_u16_bytes(&mut encoded, instance_id, "plan instance ID")?;
        encoded.extend_from_slice(&self.owner.incarnation.to_bytes());
        encoded.extend_from_slice(&self.operation_id.to_bytes());
        write_u16_bytes(&mut encoded, operation_kind, "plan operation kind")?;
        encoded.extend_from_slice(&self.request_digest);
        encoded.extend_from_slice(&self.state_digest);
        encoded.extend_from_slice(&self.summary.file_count.to_be_bytes());
        encoded.extend_from_slice(&self.summary.row_count.to_be_bytes());
        encoded.extend_from_slice(&self.summary.total_bytes.to_be_bytes());
        match self.source_scope {
            None => encoded.push(0),
            Some(scope) => {
                encoded.push(1);
                encoded.extend_from_slice(&scope.version.to_be_bytes());
                encoded.push(scope.kind.to_wire_tag());
                encoded.extend_from_slice(&scope.digest);
            }
        }
        match self.add_files_domain {
            None => encoded.push(0),
            Some(domain) => {
                encoded.push(1);
                encoded.extend_from_slice(&domain.version.to_be_bytes());
                encoded.push(domain.source_domain.to_wire_tag());
                encoded.extend_from_slice(&domain.target_cleanup_root_digest);
            }
        }
        write_u32_bytes(&mut encoded, payload, "plan provider payload")?;
        encoded.extend_from_slice(&self.plan_digest);
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        const MAGIC: &[u8; 4] = b"CDP1";
        let mut reader = WireReader::new(bytes, "connector data mutation plan");
        reader.expect(MAGIC, "magic")?;
        if reader.read_u16()? != CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION {
            return Err(corrupt(
                "unsupported connector data mutation plan wire version",
            ));
        }
        let schema_version = reader.read_u16()?;
        let instance_id = ConnectorInstanceId::parse(reader.read_utf8_u16("instance ID")?)?;
        let incarnation = ProviderBindingEpoch::from_bytes(reader.read_array()?);
        let operation_id = ConnectorMutationOperationId::from_bytes(reader.read_array()?);
        let operation_kind: Arc<str> = Arc::from(reader.read_utf8_u16("operation kind")?);
        let request_digest = reader.read_array()?;
        let state_digest = reader.read_array()?;
        let summary = ConnectorDataMutationPlanSummary::try_new(
            reader.read_u32()?,
            reader.read_u64()?,
            reader.read_u64()?,
        )?;
        let source_scope = match reader.read_u8()? {
            0 => None,
            1 => Some(ConnectorDataMutationSourceScope {
                version: reader.read_u16()?,
                kind: ConnectorDataMutationSourceScopeKind::try_from_wire_tag(reader.read_u8()?)?,
                digest: reader.read_array()?,
            }),
            _ => {
                return Err(corrupt(
                    "invalid connector data mutation plan source scope tag",
                ));
            }
        };
        let add_files_domain = match reader.read_u8()? {
            0 => None,
            1 => Some(ConnectorDataMutationAddFilesDomain {
                version: reader.read_u16()?,
                source_domain: ConnectorDataMutationSourceDomain::try_from_wire_tag(
                    reader.read_u8()?,
                )?,
                target_cleanup_root_digest: reader.read_array()?,
            }),
            _ => {
                return Err(corrupt(
                    "invalid connector data mutation plan ADD FILES source domain tag",
                ));
            }
        };
        let provider_payload = Bytes::copy_from_slice(reader.read_bytes_u32("provider payload")?);
        let plan_digest = reader.read_array()?;
        reader.finish()?;
        let plan = Self {
            schema_version,
            owner: ConnectorProviderBindingKey {
                instance_id,
                incarnation,
            },
            operation_id,
            operation_kind,
            request_digest,
            state_digest,
            summary,
            source_scope,
            add_files_domain,
            provider_payload,
            plan_digest,
        };
        plan.validate()?;
        Ok(plan)
    }
}

impl fmt::Debug for ConnectorDataMutationPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationPlan")
            .field("schema_version", &self.schema_version)
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("request_digest", &self.request_digest)
            .field("state_digest", &self.state_digest)
            .field("summary", &self.summary)
            .field("source_scope", &self.source_scope)
            .field("add_files_domain", &self.add_files_domain)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorDataMutationExecuteRequest {
    pub plan: ConnectorDataMutationPlan,
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationExecuteRequest {
    pub fn try_new(
        plan: ConnectorDataMutationPlan,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        Ok(Self { plan, context })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDataMutationReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    operation_kind: Arc<str>,
    request_digest: [u8; 32],
    plan_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    provider_payload: Bytes,
    provider_payload_digest: [u8; 32],
}

impl ConnectorDataMutationReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        operation_kind: impl Into<Arc<str>>,
        request_digest: [u8; 32],
        plan_digest: [u8; 32],
        state_digest: [u8; 32],
        summary: ConnectorDataMutationPlanSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        let operation_kind = operation_kind.into();
        validate_operation_kind(&operation_kind)?;
        validate_provider_payload(&provider_payload, "receipt")?;
        let provider_payload_digest =
            digest_with_domain(RECEIPT_PAYLOAD_DIGEST_DOMAIN, provider_payload.as_ref());
        Ok(Self {
            descriptor,
            incarnation,
            operation_id,
            operation_kind,
            request_digest,
            plan_digest,
            state_digest,
            summary,
            provider_payload,
            provider_payload_digest,
        })
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

    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }

    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }

    pub const fn summary(&self) -> ConnectorDataMutationPlanSummary {
        self.summary
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }

    pub const fn provider_payload_digest(&self) -> [u8; 32] {
        self.provider_payload_digest
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_operation_kind(&self.operation_kind)?;
        validate_provider_payload(&self.provider_payload, "receipt")?;
        if digest_with_domain(
            RECEIPT_PAYLOAD_DIGEST_DOMAIN,
            self.provider_payload.as_ref(),
        ) != self.provider_payload_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation receipt payload digest is invalid",
            ));
        }
        Ok(())
    }

    pub fn try_to_wire_v1(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        const MAGIC: &[u8; 4] = b"CDR1";
        let provider_id = self.descriptor.provider_id.as_str().as_bytes();
        let instance_id = self.descriptor.instance_id.as_str().as_bytes();
        let operation_kind = self.operation_kind.as_bytes();
        let payload = self.provider_payload.as_ref();
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
                + 32 * 4
                + 4
                + 8
                + 8
                + 4
                + payload.len(),
        );
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION.to_be_bytes());
        write_u16_bytes(&mut encoded, provider_id, "receipt provider ID")?;
        write_u16_bytes(&mut encoded, instance_id, "receipt instance ID")?;
        encoded.extend_from_slice(&self.incarnation.to_bytes());
        encoded.extend_from_slice(&self.operation_id.to_bytes());
        write_u16_bytes(&mut encoded, operation_kind, "receipt operation kind")?;
        encoded.extend_from_slice(&self.request_digest);
        encoded.extend_from_slice(&self.plan_digest);
        encoded.extend_from_slice(&self.state_digest);
        encoded.extend_from_slice(&self.summary.file_count.to_be_bytes());
        encoded.extend_from_slice(&self.summary.row_count.to_be_bytes());
        encoded.extend_from_slice(&self.summary.total_bytes.to_be_bytes());
        write_u32_bytes(&mut encoded, payload, "receipt provider payload")?;
        encoded.extend_from_slice(&self.provider_payload_digest);
        Ok(Bytes::from(encoded))
    }

    pub fn try_from_wire_v1(bytes: &[u8]) -> Result<Self, ConnectorError> {
        const MAGIC: &[u8; 4] = b"CDR1";
        let mut reader = WireReader::new(bytes, "connector data mutation receipt");
        reader.expect(MAGIC, "magic")?;
        if reader.read_u16()? != CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION {
            return Err(corrupt(
                "unsupported connector data mutation receipt wire version",
            ));
        }
        let provider_id = super::ConnectorProviderId::parse(reader.read_utf8_u16("provider ID")?)?;
        let instance_id = ConnectorInstanceId::parse(reader.read_utf8_u16("instance ID")?)?;
        let incarnation = ProviderBindingEpoch::from_bytes(reader.read_array()?);
        let operation_id = ConnectorMutationOperationId::from_bytes(reader.read_array()?);
        let operation_kind = reader.read_utf8_u16("operation kind")?;
        let request_digest = reader.read_array()?;
        let plan_digest = reader.read_array()?;
        let state_digest = reader.read_array()?;
        let summary = ConnectorDataMutationPlanSummary::try_new(
            reader.read_u32()?,
            reader.read_u64()?,
            reader.read_u64()?,
        )?;
        let provider_payload = Bytes::copy_from_slice(reader.read_bytes_u32("provider payload")?);
        let provider_payload_digest = reader.read_array()?;
        reader.finish()?;
        let receipt = Self::try_new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id,
            },
            incarnation,
            operation_id,
            operation_kind,
            request_digest,
            plan_digest,
            state_digest,
            summary,
            provider_payload,
        )?;
        if receipt.provider_payload_digest != provider_payload_digest {
            return Err(corrupt(
                "connector data mutation receipt payload digest does not match wire",
            ));
        }
        Ok(receipt)
    }
}

impl fmt::Debug for ConnectorDataMutationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorDataMutationReceipt")
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("request_digest", &self.request_digest)
            .field("plan_digest", &self.plan_digest)
            .field("state_digest", &self.state_digest)
            .field("summary", &self.summary)
            .field("provider_payload_len", &self.provider_payload.len())
            .field("provider_payload_digest", &self.provider_payload_digest)
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorDataMutationReconcileRequest {
    pub owner: ConnectorProviderBindingKey,
    pub operation_id: ConnectorMutationOperationId,
    pub operation_kind: Arc<str>,
    pub request_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

impl ConnectorDataMutationReconcileRequest {
    pub fn try_new(
        plan: &ConnectorDataMutationPlan,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        plan.validate()?;
        if evidence.operation_id() != plan.operation_id
            || evidence.operation_kind() != plan.operation_kind.as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation evidence does not match its plan",
            ));
        }
        Ok(Self {
            owner: plan.owner.clone(),
            operation_id: plan.operation_id,
            operation_kind: plan.operation_kind.clone(),
            request_digest: plan.request_digest,
            plan_digest: plan.plan_digest,
            state_digest: plan.state_digest,
            evidence,
            context,
        })
    }
}

/// FE-only external data mutation capability. It is never installed in a BE
/// execution binding and intentionally has no public abort method.
pub trait ConnectorDataMutation: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;

    fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError>;

    fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;

    fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError>;
}

pub trait ConnectorDataMutationResolver: Send + Sync {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDataMutationSession, ConnectorError>;

    fn acquire_exact_data_mutation(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDataMutationSession, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorDataMutationSession {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    metadata: Arc<dyn ConnectorMetadata>,
    mutation: Arc<dyn ConnectorDataMutation>,
    _release: Arc<DataMutationReleaseGuard>,
}

impl ConnectorDataMutationSession {}

struct DataMutationReleaseGuard {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDataMutationSession {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        metadata: Arc<dyn ConnectorMetadata>,
        mutation: Arc<dyn ConnectorDataMutation>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if metadata.instance_id() != &descriptor.instance_id
            || mutation.descriptor() != &descriptor
            || mutation.binding_key() != &provider_binding_key
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation capabilities do not match their lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_binding_key,
            metadata,
            mutation,
            _release: Arc::new(DataMutationReleaseGuard {
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

    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }

    /// Builds and validates a provider request behind the FE-owned runtime
    /// lease. The provider's legacy generation fence remains private to this
    /// capability boundary; callers retain only the control-runtime owner.
    pub fn plan_operation(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorDataMutationOperation,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        let request = ConnectorDataMutationPlanningRequest::try_new(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            context,
        )?;
        self.plan_mutation(request)
    }

    pub fn plan_mutation(
        &self,
        request: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        self.validate_planning_request(&request)?;
        let plan = self.mutation.plan_mutation(request.clone())?;
        plan.validate()?;
        if plan.owner != self.provider_binding_key
            || plan.operation_id != request.operation_id
            || plan.operation_kind.as_ref() != request.operation.kind()
            || plan.request_digest != request.request_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan does not match its request or lease generation",
            ));
        }
        Ok(plan)
    }

    pub fn execute(
        &self,
        request: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.validate_plan(&request.plan)?;
        let plan = request.plan.clone();
        let outcome = self.mutation.execute(request)?;
        self.validate_outcome(&plan, &outcome)?;
        Ok(outcome)
    }

    pub fn reconcile(
        &self,
        request: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        self.validate_reconcile_request(&request)?;
        let outcome = self.mutation.reconcile(request.clone())?;
        self.validate_reconcile_outcome(&request, &outcome)?;
        Ok(outcome)
    }

    fn validate_planning_request(
        &self,
        request: &ConnectorDataMutationPlanningRequest,
    ) -> Result<(), ConnectorError> {
        request.validate()?;
        if request.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation request does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_plan(&self, plan: &ConnectorDataMutationPlan) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.provider_binding_key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation plan does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_reconcile_request(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
    ) -> Result<(), ConnectorError> {
        validate_operation_kind(&request.operation_kind)?;
        if request.owner != self.provider_binding_key
            || request.evidence.descriptor() != &self.descriptor
            || request.evidence.incarnation() != self.provider_binding_key.incarnation
            || request.evidence.operation_id() != request.operation_id
            || request.evidence.operation_kind() != request.operation_kind.as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation reconcile evidence does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_outcome(
        &self,
        plan: &ConnectorDataMutationPlan,
        outcome: &ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                self.validate_receipt(
                    plan.operation_id,
                    plan.operation_kind(),
                    plan.request_digest,
                    plan.plan_digest,
                    plan.state_digest,
                    receipt,
                )?;
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(plan.operation_id, plan.operation_kind(), evidence)?;
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
        }
        Ok(())
    }

    fn validate_reconcile_outcome(
        &self,
        request: &ConnectorDataMutationReconcileRequest,
        outcome: &ExternalMutationOutcome<ConnectorDataMutationReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => self.validate_receipt(
                request.operation_id,
                &request.operation_kind,
                request.request_digest,
                request.plan_digest,
                request.state_digest,
                receipt,
            ),
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(request.operation_id, &request.operation_kind, evidence)
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => Ok(()),
        }
    }

    fn validate_receipt(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation_kind: &str,
        request_digest: [u8; 32],
        plan_digest: [u8; 32],
        state_digest: [u8; 32],
        receipt: &ConnectorDataMutationReceipt,
    ) -> Result<(), ConnectorError> {
        if receipt.descriptor != self.descriptor
            || receipt.incarnation != self.provider_binding_key.incarnation
            || receipt.operation_id != operation_id
            || receipt.operation_kind.as_ref() != operation_kind
            || receipt.request_digest != request_digest
            || receipt.plan_digest != plan_digest
            || receipt.state_digest != state_digest
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation receipt does not match its request",
            ));
        }
        Ok(())
    }

    fn validate_evidence(
        &self,
        operation_id: ConnectorMutationOperationId,
        operation_kind: &str,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.provider_binding_key.incarnation
            || evidence.operation_id() != operation_id
            || evidence.operation_kind() != operation_kind
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector data mutation evidence does not match its request",
            ));
        }
        Ok(())
    }
}

impl Drop for DataMutationReleaseGuard {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

pub(crate) fn validate_data_mutation_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    mutation: &dyn ConnectorDataMutation,
) -> Result<(), ConnectorError> {
    if mutation.descriptor() != descriptor
        || mutation.binding_key().instance_id != descriptor.instance_id
        || mutation.binding_key().incarnation != incarnation
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector data mutation capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}

fn request_digest(
    operation_id: ConnectorMutationOperationId,
    owner: &ConnectorProviderBindingKey,
    operation: &ConnectorDataMutationOperation,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(REQUEST_DIGEST_DOMAIN);
    hasher.update(CONNECTOR_DATA_MUTATION_CONTRACT_VERSION.to_be_bytes());
    hasher.update(operation_id.to_bytes());
    digest_bytes(&mut hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hasher);
    hasher.finalize().into()
}

fn plan_digest(
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    summary: ConnectorDataMutationPlanSummary,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    add_files_domain: Option<ConnectorDataMutationAddFilesDomain>,
    provider_payload: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PLAN_DIGEST_DOMAIN);
    hasher.update(CONNECTOR_DATA_MUTATION_CONTRACT_VERSION.to_be_bytes());
    hasher.update(request_digest);
    hasher.update(state_digest);
    summary.digest_into(&mut hasher);
    match source_scope {
        Some(scope) => {
            hasher.update([1]);
            scope.digest_into(&mut hasher);
        }
        None => hasher.update([0]),
    }
    match add_files_domain {
        Some(domain) => {
            hasher.update([1]);
            domain.digest_into(&mut hasher);
        }
        None => hasher.update([0]),
    }
    digest_bytes(&mut hasher, provider_payload);
    hasher.finalize().into()
}

fn validate_add_files_facts(
    operation_kind: &str,
    source_scope: Option<ConnectorDataMutationSourceScope>,
    add_files_domain: Option<ConnectorDataMutationAddFilesDomain>,
) -> Result<(), ConnectorError> {
    match (operation_kind, source_scope, add_files_domain) {
        (REGISTER_EXISTING_FILES_KIND, Some(scope), Some(domain)) => {
            scope.validate()?;
            domain.validate()
        }
        (REGISTER_EXISTING_FILES_KIND, None, _) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "register-existing-files plan requires a source ownership scope",
        )),
        (REGISTER_EXISTING_FILES_KIND, Some(_), None) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "register-existing-files plan requires caller-managed source domain proof",
        )),
        (TRUNCATE_KIND, None, None) => Ok(()),
        (TRUNCATE_KIND, _, _) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "truncate plan must not carry ADD FILES source facts",
        )),
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unsupported connector data mutation operation kind",
        )),
    }
}

fn write_u16_bytes(encoded: &mut Vec<u8>, value: &[u8], field: &str) -> Result<(), ConnectorError> {
    let len = u16::try_from(value.len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("connector data mutation {field} exceeds wire bound"),
        )
    })?;
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn write_u32_bytes(encoded: &mut Vec<u8>, value: &[u8], field: &str) -> Result<(), ConnectorError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("connector data mutation {field} exceeds wire bound"),
        )
    })?;
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

struct WireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    subject: &'static str,
}

impl<'a> WireReader<'a> {
    fn new(bytes: &'a [u8], subject: &'static str) -> Self {
        Self {
            bytes,
            offset: 0,
            subject,
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| corrupt(format!("{} wire offset overflow", self.subject)))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| corrupt(format!("truncated {} wire", self.subject)))?;
        self.offset = end;
        Ok(slice)
    }

    fn expect(&mut self, expected: &[u8], field: &str) -> Result<(), ConnectorError> {
        if self.take(expected.len())? != expected {
            return Err(corrupt(format!("invalid {} wire {field}", self.subject)));
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ConnectorError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed-width wire u16"),
        ))
    }

    fn read_u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed-width wire u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, ConnectorError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed-width wire u64"),
        ))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        Ok(self.take(N)?.try_into().expect("fixed-width wire array"))
    }

    fn read_utf8_u16(&mut self, field: &str) -> Result<&'a str, ConnectorError> {
        let len = self.read_u16()? as usize;
        let value = self.take(len)?;
        std::str::from_utf8(value)
            .map_err(|_| corrupt(format!("{} wire {field} is not UTF-8", self.subject)))
    }

    fn read_bytes_u32(&mut self, _field: &str) -> Result<&'a [u8], ConnectorError> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    fn finish(self) -> Result<(), ConnectorError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt(format!("{} wire has trailing bytes", self.subject)))
        }
    }
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn digest_with_domain(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    digest_bytes(&mut hasher, value);
    hasher.finalize().into()
}

fn validate_bounded_text(value: &str, max: usize, field: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("{field} must not be empty"),
        ));
    }
    if value.len() > max {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("{field} exceeds the hard limit"),
        ));
    }
    Ok(())
}

fn validate_provider_payload(payload: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if payload.len() > MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("connector data mutation {kind} payload exceeds the hard limit"),
        ));
    }
    Ok(())
}

fn validate_operation_kind(kind: &str) -> Result<(), ConnectorError> {
    if matches!(kind, REGISTER_EXISTING_FILES_KIND | TRUNCATE_KIND) {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unsupported connector data mutation operation kind",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::{
        ConnectorDataMutationAddFilesDomain, ConnectorDataMutationOperation,
        ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
        ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
        ConnectorDataMutationSourceScope,
    };
    use crate::connector::{
        ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorRequestContext, ConnectorTableHandle, ProviderBindingEpoch,
    };

    fn plan() -> ConnectorDataMutationPlan {
        let instance_id = ConnectorInstanceId::parse("analytics").expect("instance ID");
        let request = ConnectorDataMutationPlanningRequest::try_new(
            ConnectorMutationOperationId::from_bytes([3; 16]),
            ConnectorProviderBindingKey {
                instance_id: instance_id.clone(),
                incarnation: ProviderBindingEpoch::from_bytes([4; 16]),
            },
            ConnectorDataMutationOperation::register_existing_files(
                ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                    .expect("table"),
                "s3://bucket/source",
            )
            .expect("operation"),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                crate::connector::ConnectorStopOwner::new().view(),
                1024,
                2048,
            )
            .expect("context"),
        )
        .expect("request");
        ConnectorDataMutationPlan::try_new(
            &request,
            [5; 32],
            ConnectorDataMutationPlanSummary::try_new(1, 2, 3).expect("summary"),
            Some(
                ConnectorDataMutationSourceScope::try_new_directory([6; 32]).expect("source scope"),
            ),
            Some(
                ConnectorDataMutationAddFilesDomain::try_new_caller_managed_stable([7; 32])
                    .expect("source domain"),
            ),
            Bytes::from_static(b"opaque-plan"),
        )
        .expect("plan")
    }

    #[test]
    fn data_mutation_plan_wire_round_trips_and_binds_source_scope() {
        let plan = plan();
        let wire = plan.try_to_wire_v1().expect("encode plan");
        let decoded = ConnectorDataMutationPlan::try_from_wire_v1(&wire).expect("decode plan");
        assert_eq!(decoded, plan);
        assert_eq!(decoded.source_scope(), plan.source_scope());

        let mut tampered = wire.to_vec();
        let last = tampered.last_mut().expect("plan digest");
        *last ^= 1;
        assert!(ConnectorDataMutationPlan::try_from_wire_v1(&tampered).is_err());
    }

    #[test]
    fn data_mutation_receipt_wire_rejects_trailing_data() {
        let plan = plan();
        let receipt = ConnectorDataMutationReceipt::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: plan.owner().instance_id.clone(),
            },
            plan.owner().incarnation,
            plan.operation_id(),
            plan.operation_kind(),
            plan.request_digest(),
            plan.plan_digest(),
            plan.state_digest(),
            plan.summary(),
            Bytes::from_static(b"opaque-receipt"),
        )
        .expect("receipt");
        let mut wire = receipt.try_to_wire_v1().expect("encode receipt").to_vec();
        wire.push(0);
        assert_eq!(
            ConnectorDataMutationReceipt::try_from_wire_v1(&wire)
                .expect_err("trailing receipt bytes must fail")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
