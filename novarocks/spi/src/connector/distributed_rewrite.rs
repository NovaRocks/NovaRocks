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

//! FE-only provider-neutral distributed rewrite planning contract.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorControlPlanningLease, ConnectorControlRuntimeId, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMetadata, ConnectorPinnedFileSet, ConnectorProviderBinding,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorScanPlanning,
    ConnectorTableHandle, ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteIntent,
    ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWritePreparation,
    ConnectorWriteReceipt, ProviderBindingEpoch,
};

mod async_plan;
pub use async_plan::{
    ConnectorRewritePlanSource, ConnectorRewritePrepared, ConnectorRewriteSelection,
    ConnectorRewriteSourceRequest, ConnectorRewriteWriteBeginSource, RewriteCpuInput,
    RewriteCpuPort, RewritePlanCpuSource,
};

pub const CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION: u16 = 1;
pub const MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS: usize = 4096;

const REQUEST_DOMAIN: &[u8] = b"novarocks.connector-distributed-rewrite.request.v1\0";
const PLAN_DOMAIN: &[u8] = b"novarocks.connector-distributed-rewrite.plan.v1\0";

pub const REWRITE_DATA_FILES_KIND: &str = "rewrite-data-files";
pub const REWRITE_POSITION_DELETES_KIND: &str = "rewrite-position-deletes";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorDistributedRewriteOperation {
    RewriteDataFiles {
        table: ConnectorTableHandle,
        rewrite_all: bool,
    },
    RewritePositionDeletes {
        table: ConnectorTableHandle,
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

/// Which artifacts a distributed rewrite republishes, and the selection facts
/// that decide which of them it selected.
///
/// This is the frozen operation stripped of its target, so it can travel with a
/// write session that already names one. It exists because the two rewrites are
/// not distinguishable downstream by anything else: both reach `begin_write`
/// through the same neutral request, and a provider that read the writer input
/// shape instead would let a caller turn a data rewrite into a delete rewrite by
/// signing a different input.
///
/// The selection facts travel with the kind rather than being re-derived. The provider freezes groups once and the session consumes that selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDistributedRewriteShape {
    /// Republish the target's data files.
    ///
    /// `rewrite_all` is carried so the shape stays a faithful projection of the
    /// frozen operation. Whether a provider narrows its selection by it is that
    /// provider's decision; both sides read the same value, so they cannot
    /// disagree about the group set either way.
    DataFiles { rewrite_all: bool },
    /// Repack the position-delete artifacts attached to the target's data
    /// files. Both facts narrow the selection: `rewrite_all` waives the
    /// threshold, and `min_input_files` sets how many attached artifacts make a
    /// data file worth repacking.
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

impl ConnectorDistributedRewriteOperation {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RewriteDataFiles { .. } => REWRITE_DATA_FILES_KIND,
            Self::RewritePositionDeletes { .. } => REWRITE_POSITION_DELETES_KIND,
        }
    }

    /// What this operation rewrites, without its target.
    pub const fn shape(&self) -> ConnectorDistributedRewriteShape {
        match self {
            Self::RewriteDataFiles { rewrite_all, .. } => {
                ConnectorDistributedRewriteShape::DataFiles {
                    rewrite_all: *rewrite_all,
                }
            }
            Self::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => ConnectorDistributedRewriteShape::PositionDeletes {
                rewrite_all: *rewrite_all,
                min_input_files: *min_input_files,
            },
        }
    }

    pub const fn table(&self) -> &ConnectorTableHandle {
        match self {
            Self::RewriteDataFiles { table, .. } | Self::RewritePositionDeletes { table, .. } => {
                table
            }
        }
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Self::RewritePositionDeletes {
            min_input_files: Some(value),
            ..
        } = self
            && *value == 0
        {
            return Err(invalid(
                "rewrite position deletes min_input_files must be positive",
            ));
        }
        Ok(())
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.kind().as_bytes());
        digest_bytes(hash, self.table().owner().as_str().as_bytes());
        digest_bytes(hash, self.table().payload());
        match self {
            Self::RewriteDataFiles { rewrite_all, .. } => hash.update([u8::from(*rewrite_all)]),
            Self::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => {
                hash.update([u8::from(*rewrite_all), u8::from(min_input_files.is_some())]);
                hash.update(min_input_files.unwrap_or_default().to_be_bytes());
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectorDistributedRewritePlanningRequest {
    operation_id: ConnectorWriteOperationId,
    owner: ConnectorProviderBindingKey,
    operation: ConnectorDistributedRewriteOperation,
    request_digest: [u8; 32],
    pub context: ConnectorRequestContext,
}

impl ConnectorDistributedRewritePlanningRequest {
    pub fn try_new(
        operation_id: ConnectorWriteOperationId,
        owner: ConnectorProviderBindingKey,
        operation: ConnectorDistributedRewriteOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        operation.validate()?;
        if operation.table().owner() != &owner.instance_id {
            return Err(invalid(
                "distributed rewrite table handle does not match exact owner",
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
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub fn operation(&self) -> &ConnectorDistributedRewriteOperation {
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
            return Err(invalid(
                "distributed rewrite request owner or digest is invalid",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDistributedRewritePlanningRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewritePlanningRequest")
            .field("operation_id", &self.operation_id)
            .field("owner", &self.owner)
            .field("operation_kind", &self.operation.kind())
            .field("request_digest", &self.request_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDistributedRewritePlanSummary {
    pub groups: u64,
    pub input_data_files: u64,
    pub input_delete_files: u64,
    pub input_bytes: u64,
    /// A provider may know that a rewrite is a no-op, but a planned rewrite
    /// must not present an unavailable output cardinality as zero.
    pub expected_output_files: Option<u64>,
}

impl ConnectorDistributedRewritePlanSummary {
    fn digest_into(self, hash: &mut Sha256) {
        for value in [
            self.groups,
            self.input_data_files,
            self.input_delete_files,
            self.input_bytes,
        ] {
            hash.update(value.to_be_bytes());
        }
        digest_optional_u64(hash, self.expected_output_files);
    }
}

/// One cohort's attempt-local immutable input, owned by its provider.
#[derive(Clone, Debug)]
pub struct ConnectorFrozenRewriteGroup {
    schema_name: Arc<str>,
    table_name: Arc<str>,
    operation_id: ConnectorWriteOperationId,
    artifact_digest: [u8; 32],
    content: super::ConnectorProcessLocalPayload,
}
impl PartialEq for ConnectorFrozenRewriteGroup {
    fn eq(&self, other: &Self) -> bool {
        self.schema_name == other.schema_name
            && self.table_name == other.table_name
            && self.operation_id == other.operation_id
            && self.artifact_digest == other.artifact_digest
    }
}
impl Eq for ConnectorFrozenRewriteGroup {}
impl ConnectorFrozenRewriteGroup {
    pub fn try_new(
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        operation_id: ConnectorWriteOperationId,
        artifact_digest: [u8; 32],
        content: super::ConnectorProcessLocalPayload,
    ) -> Result<Self, ConnectorError> {
        if schema_name.as_ref().is_empty() || table_name.as_ref().is_empty() {
            return Err(invalid(
                "distributed rewrite cohort group requires a schema-qualified relation name",
            ));
        }
        Ok(Self {
            schema_name: Arc::from(schema_name.as_ref()),
            table_name: Arc::from(table_name.as_ref()),
            operation_id,
            artifact_digest,
            content,
        })
    }
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub const fn artifact_digest(&self) -> [u8; 32] {
        self.artifact_digest
    }
    pub fn content(&self) -> &super::ConnectorProcessLocalPayload {
        &self.content
    }
    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.schema_name.as_bytes());
        digest_bytes(hash, self.table_name.as_bytes());
        hash.update(self.operation_id.to_bytes());
        hash.update(self.artifact_digest);
    }
}

/// What one sealed cohort reads.
///
/// Both variants name their input exactly, and both name the same set the
/// cohort's commit replaces. The choice is a fact of the operation, not a
/// preference: a rewrite of table rows has a data file set to pin, and a
/// rewrite of delete artifacts has none, because it never reads table rows at
/// all. Modelling that as one optional pin would let a cohort with neither
/// reach execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorRewriteCohortRead {
    /// Exactly the data files this cohort rewrites.
    PinnedFileSet(ConnectorPinnedFileSet),
    /// Exactly the frozen group whose delete artifacts this cohort rewrites.
    DeleteArtifactGroup(ConnectorFrozenRewriteGroup),
}

impl ConnectorRewriteCohortRead {
    fn digest_into(&self, hash: &mut Sha256) {
        // Which files a cohort reads is part of what it is, so two cohorts
        // that differ only in their input never share a digest -- and the two
        // read families are tagged apart before their facts are folded in.
        match self {
            Self::PinnedFileSet(pinned) => {
                hash.update([0]);
                digest_bytes(hash, pinned.namespace().as_bytes());
                digest_bytes(hash, pinned.table().as_bytes());
                hash.update(pinned.version_ordinal().to_be_bytes());
                hash.update((pinned.files().len() as u64).to_be_bytes());
                for file in pinned.files() {
                    digest_bytes(hash, file.as_bytes());
                }
            }
            Self::DeleteArtifactGroup(group) => {
                hash.update([1]);
                group.digest_into(hash);
            }
        }
    }
}

fn complete_schema_digest(
    schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<[u8; 32], novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
    let mut hash = Sha256::new();
    let mut emit = |bytes: &[u8]| {
        creation.work(bytes.len())?;
        hash.update(bytes);
        Ok::<_, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>(())
    };
    emit(b"novarocks-rewrite-execution-schema-v1")?;
    emit(&(schema.bindings().len() as u64).to_le_bytes())?;
    for (field, binding) in schema.schema().fields().iter().zip(schema.bindings()) {
        emit(&(field.name().len() as u64).to_le_bytes())?;
        for name in field.name().as_bytes().chunks(1024) {
            emit(name)?;
        }
        novarocks_type_contract::write_value_type_identity_observed(
            binding.value_type(),
            &mut emit,
        )?;
        novarocks_type_contract::write_value_layout_identity_observed(
            binding.typed_binding().layout(),
            &mut emit,
        )?;
    }
    Ok(hash.finalize().into())
}

/// Storage and execution have independent type namespaces. The provider's
/// explicit declaration author owns conversions between them; these root
/// occurrence laws prevent signing another table or NULL policy by accident.
pub(crate) fn validate_scan_source_pair(
    source: &arrow::datatypes::Schema,
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<(), novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
    creation.work(1)?;
    if source.fields().len() != execution.bindings().len() {
        return Err(invalid("rewrite source/execution occurrence counts differ").into());
    }
    for (source, target) in source.fields().iter().zip(execution.schema().fields()) {
        creation.work(source.name().len() + target.name().len() + 1)?;
        if source.name() != target.name() || source.is_nullable() != target.is_nullable() {
            return Err(invalid("rewrite source/execution root names or NULL facts differ").into());
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct ConnectorDistributedRewriteCohortPlan {
    cohort_id: ConnectorWriteCohortId,
    /// Exactly what this cohort reads.  It is not a write target and must not
    /// be substituted for `preparation.table()`.
    read: ConnectorRewriteCohortRead,
    /// Schema of the frozen scan output.  SQL needs this to build the
    /// read-side physical carrier, but it is deliberately not the writer
    /// contract: the Provider-signed preparation below owns input shape.
    scan_schema: SchemaRef,
    scan_execution_schema: novarocks_connector_contract::ConnectorReadExecutionSchema,
    execution_schema_digest: [u8; 32],
    _source_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    scan_schema_digest: [u8; 32],
    preparation: ConnectorWritePreparation,
    group_digest: [u8; 32],
}

impl ConnectorDistributedRewriteCohortPlan {
    pub fn try_new_typed(
        cohort_id: ConnectorWriteCohortId,
        read: ConnectorRewriteCohortRead,
        scan_schema: SchemaRef,
        scan_execution_schema: novarocks_connector_contract::ConnectorReadExecutionSchema,
        scan_schema_digest: [u8; 32],
        preparation: ConnectorWritePreparation,
        group_digest: [u8; 32],
        creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<Self, novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
        preparation.validate()?;
        validate_scan_source_pair(&scan_schema, &scan_execution_schema, creation)?;
        let execution_schema_digest = complete_schema_digest(&scan_execution_schema, creation)?;
        Ok(Self {
            cohort_id,
            read,
            scan_schema,
            scan_execution_schema,
            execution_schema_digest,
            _source_storage: creation.storage(),
            scan_schema_digest,
            preparation,
            group_digest,
        })
    }
    pub const fn cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }
    /// Exactly what this cohort reads, and therefore exactly what its commit
    /// replaces.
    pub const fn read(&self) -> &ConnectorRewriteCohortRead {
        &self.read
    }
    /// Metadata-free execution projection. Complete value authority lives in
    /// scan_execution_schema; the original storage format is a separate fact.
    pub fn scan_schema(&self) -> &arrow::datatypes::Schema {
        self.scan_execution_schema.schema()
    }
    pub fn scan_execution_schema(
        &self,
    ) -> &novarocks_connector_contract::ConnectorReadExecutionSchema {
        &self.scan_execution_schema
    }
    pub fn scan_source_schema(&self) -> &arrow::datatypes::Schema {
        &self.scan_schema
    }
    /// Provider-owned canonical digest of the frozen scan Arrow schema.
    pub const fn scan_schema_digest(&self) -> [u8; 32] {
        self.scan_schema_digest
    }
    /// Provider-signed writer contract. Generic orchestration must pass this
    /// through unchanged and never infer a `Data` shape from the scan schema.
    pub fn preparation(&self) -> &ConnectorWritePreparation {
        &self.preparation
    }
    pub const fn group_digest(&self) -> [u8; 32] {
        self.group_digest
    }
    fn digest_into(&self, hash: &mut Sha256) {
        hash.update(self.cohort_id.to_bytes());
        hash.update(self.group_digest);
        hash.update(self.scan_schema_digest);
        hash.update(self.execution_schema_digest);
        self.read.digest_into(hash);
        hash.update(self.preparation.digest());
    }
}

impl fmt::Debug for ConnectorDistributedRewriteCohortPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewriteCohortPlan")
            .field("cohort_id", &self.cohort_id)
            .field("preparation_owner", self.preparation.owner())
            .field("preparation_digest", &self.preparation.digest())
            .field("group_digest", &self.group_digest)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ConnectorDistributedRewritePlan {
    owner: ConnectorProviderBindingKey,
    operation_id: ConnectorWriteOperationId,
    /// The exact operation this plan froze. It is kept whole rather than
    /// reduced to a kind and a target; downstream consumers share its frozen selection.
    operation: ConnectorDistributedRewriteOperation,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    manifest_digest: [u8; 32],
    summary: ConnectorDistributedRewritePlanSummary,
    provider_payload: Bytes,
    cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
    frozen: super::ConnectorProcessLocalPayload,
    plan_digest: [u8; 32],
}

impl ConnectorDistributedRewritePlan {
    pub fn try_new(
        request: &ConnectorDistributedRewritePlanningRequest,
        state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorDistributedRewritePlanSummary,
        provider_payload: Bytes,
        mut cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
        frozen: super::ConnectorProcessLocalPayload,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_payload(&provider_payload, "plan")?;
        if cohorts.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS {
            return Err(exhausted("distributed rewrite plan exceeds cohort limit"));
        }
        cohorts.sort_by_key(ConnectorDistributedRewriteCohortPlan::cohort_id);
        if cohorts
            .windows(2)
            .any(|pair| pair[0].cohort_id == pair[1].cohort_id)
            || cohorts.iter().any(|cohort| {
                cohort.preparation.owner() != &request.owner
                    || cohort.preparation.table() != request.operation.table()
                    || cohort.preparation.intent()
                        != rewrite_operation_intent(request.operation.kind())
            })
        {
            return Err(invalid(
                "distributed rewrite cohorts are invalid or foreign",
            ));
        }
        let plan_digest = plan_digest(
            request.request_digest,
            request.operation.table(),
            state_digest,
            manifest_digest,
            summary,
            &provider_payload,
            &cohorts,
        );
        Ok(Self {
            owner: request.owner.clone(),
            operation_id: request.operation_id,
            operation: request.operation.clone(),
            request_digest: request.request_digest,
            state_digest,
            manifest_digest,
            summary,
            provider_payload,
            cohorts,
            frozen,
            plan_digest,
        })
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
    pub const fn operation_kind(&self) -> &str {
        self.operation.kind()
    }
    /// The exact operation this plan froze.
    pub const fn operation(&self) -> &ConnectorDistributedRewriteOperation {
        &self.operation
    }
    /// What this plan rewrites, and how it selected it. A session sealed from
    /// this plan consumes its frozen groups; writer input cannot define the selection.
    pub const fn shape(&self) -> ConnectorDistributedRewriteShape {
        self.operation.shape()
    }
    /// Table that receives the C1 staged output.  Cohort sources may differ
    /// from this table in future providers, so callers must not substitute a
    /// source handle here.
    pub const fn target(&self) -> &ConnectorTableHandle {
        self.operation.table()
    }
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    pub const fn state_digest(&self) -> [u8; 32] {
        self.state_digest
    }
    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub const fn summary(&self) -> ConnectorDistributedRewritePlanSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub fn frozen(&self) -> &super::ConnectorProcessLocalPayload {
        &self.frozen
    }
    pub fn cohorts(&self) -> &[ConnectorDistributedRewriteCohortPlan] {
        &self.cohorts
    }
    pub const fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.provider_payload, "plan")?;
        self.operation.validate()?;
        if self.cohorts.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS
            || self
                .cohorts
                .windows(2)
                .any(|pair| pair[0].cohort_id >= pair[1].cohort_id)
        {
            return Err(invalid("distributed rewrite plan is invalid"));
        }
        if self.target().owner() != &self.owner.instance_id
            || self.cohorts.iter().any(|cohort| {
                cohort.preparation.validate().is_err()
                    || cohort.preparation.owner() != &self.owner
                    || cohort.preparation.table() != self.target()
                    || cohort.preparation.intent()
                        != rewrite_operation_intent(self.operation_kind())
            })
        {
            return Err(invalid("distributed rewrite plan contains foreign cohort"));
        }
        if self.plan_digest
            != plan_digest(
                self.request_digest,
                self.target(),
                self.state_digest,
                self.manifest_digest,
                self.summary,
                &self.provider_payload,
                &self.cohorts,
            )
        {
            return Err(invalid("distributed rewrite plan digest is invalid"));
        }
        Ok(())
    }
}

impl fmt::Debug for ConnectorDistributedRewritePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorDistributedRewritePlan")
            .field("owner", &self.owner)
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind())
            .field("target_owner", self.target().owner())
            .field("manifest_digest", &self.manifest_digest)
            .field("cohort_count", &self.cohorts.len())
            .field("provider_payload_len", &self.provider_payload.len())
            .field("plan_digest", &self.plan_digest)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorDistributedRewriteReceiptSummary {
    pub input_data_files: u64,
    pub input_delete_files: u64,
    /// `None` means the provider committed successfully but did not publish
    /// this fact. It is deliberately distinct from a known zero.
    pub output_data_files: Option<u64>,
    /// `None` means the provider committed successfully but did not publish
    /// this fact. It is deliberately distinct from a known zero.
    pub output_delete_files: Option<u64>,
    /// `None` means the provider committed successfully but did not publish
    /// this fact. It is deliberately distinct from a known zero.
    pub output_rows: Option<u64>,
    pub target_version: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDistributedRewriteReceipt {
    summary: ConnectorDistributedRewriteReceiptSummary,
    provider_payload: Bytes,
    provider_payload_digest: [u8; 32],
}

impl ConnectorDistributedRewriteReceipt {
    pub fn try_new(
        summary: ConnectorDistributedRewriteReceiptSummary,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        validate_payload(&provider_payload, "receipt")?;
        let provider_payload_digest = Sha256::digest(&provider_payload).into();
        Ok(Self {
            summary,
            provider_payload,
            provider_payload_digest,
        })
    }
    pub const fn summary(&self) -> ConnectorDistributedRewriteReceiptSummary {
        self.summary
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
    pub const fn provider_payload_digest(&self) -> [u8; 32] {
        self.provider_payload_digest
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_payload(&self.provider_payload, "receipt")?;
        let actual: [u8; 32] = Sha256::digest(&self.provider_payload).into();
        if self.provider_payload_digest != actual {
            return Err(invalid("distributed rewrite receipt digest is invalid"));
        }
        Ok(())
    }
}

pub trait ConnectorDistributedRewrite: Send + Sync {
    /// Initial selected routes of THIS original planning factory. Facts do
    /// not grant I/O, bytes or concurrency; discovered routes extend the SAME
    /// Root before their first actual dispatch.
    fn planning_initial_dependencies(
        &self,
    ) -> Result<Vec<crate::connector::SharedDependency>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "selected Rewrite planning factory has no original route facts",
        ))
    }

    /// Exact first-party planning source type for this selected generation.
    /// This observation grants no CPU, I/O or payload capacity.
    fn scoped_cpu_source_type_id(&self) -> Option<std::any::TypeId> {
        None
    }
    /// Install the stable original source before any foreign poll. An
    /// unsupported capability preserves the request and receiving slots;
    /// there is no synchronous planning fallback.
    fn prepare_plan_source_in(
        &self,
        _request: &mut Option<ConnectorRewriteSourceRequest>,
        _receiving: &mut Option<Box<dyn ConnectorRewritePlanSource>>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "asynchronous distributed rewrite source is unsupported",
        ))
    }
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;
    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
        creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    >;
    fn finalize_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError>;
}

pub trait ConnectorDistributedRewriteResolver: Send + Sync {
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError>;
    fn acquire_exact_distributed_rewrite(
        &self,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Result<ConnectorDistributedRewriteLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorDistributedRewriteLease {
    descriptor: ConnectorInstanceDescriptor,
    control_runtime_id: ConnectorControlRuntimeId,
    provider_binding_key: ConnectorProviderBindingKey,
    planning_lease: ConnectorControlPlanningLease,
    metadata: Arc<dyn ConnectorMetadata>,
    planning: Arc<dyn ConnectorScanPlanning>,
    rewrite: Arc<dyn ConnectorDistributedRewrite>,
    write: Arc<dyn ConnectorWriteControl>,
    distribution: Arc<dyn ConnectorExecutionDistribution>,
    _release: Arc<RewriteLeaseRelease>,
}
struct RewriteLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDistributedRewriteLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        provider_incarnation: ProviderBindingEpoch,
        planning_lease: ConnectorControlPlanningLease,
        metadata: Arc<dyn ConnectorMetadata>,
        planning: Arc<dyn ConnectorScanPlanning>,
        rewrite: Arc<dyn ConnectorDistributedRewrite>,
        write: Arc<dyn ConnectorWriteControl>,
        distribution: Arc<dyn ConnectorExecutionDistribution>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        let provider_binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: provider_incarnation,
        };
        if planning_lease.binding().descriptor() != &descriptor
            || planning_lease.binding().incarnation() != provider_incarnation
            || metadata.instance_id() != &descriptor.instance_id
            || planning.instance_id() != &descriptor.instance_id
            || rewrite.descriptor() != &descriptor
            || rewrite.binding_key() != &provider_binding_key
            || write.binding_key() != &provider_binding_key
        {
            return Err(invalid(
                "distributed rewrite capabilities do not match lease generation",
            ));
        }
        Ok(Self {
            descriptor,
            control_runtime_id,
            provider_binding_key,
            planning_lease,
            metadata,
            planning,
            rewrite,
            write,
            distribution,
            _release: Arc::new(RewriteLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }
    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.provider_binding_key.incarnation
    }
    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
    /// Retain the exact control generation through the generic execution
    /// binding barrier for a frozen rewrite read. This is derived from the
    /// composite rewrite lease; it never performs a current-generation lookup.
    pub fn planning_lease(&self) -> ConnectorControlPlanningLease {
        self.planning_lease.clone()
    }
    pub fn metadata(&self) -> &Arc<dyn ConnectorMetadata> {
        &self.metadata
    }
    /// The exact scan-planning generation retained for every frozen rewrite
    /// source.  A rewrite must never reopen a source through a later current
    /// control binding after its plan has been sealed.
    pub fn planning(&self) -> &Arc<dyn ConnectorScanPlanning> {
        &self.planning
    }
    /// Produce the typed BE installer declaration from the same exact
    /// generation that froze this rewrite.  This prevents a later active
    /// incarnation from silently serving a staged operation.
    pub fn provider_binding(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        let declaration = self.distribution.declaration(context)?;
        let key = declaration.binding_key();
        if declaration.provider_id() != &self.descriptor.provider_id
            || key.instance_id != self.descriptor.instance_id
            || key.incarnation != self.provider_binding_key.incarnation
        {
            return Err(invalid(
                "distributed rewrite provider binding does not match lease generation",
            ));
        }
        Ok(declaration)
    }
    /// The original metadata CPU author must belong to this exact lease,
    /// before any input can move into its selected closed CPU port.
    pub fn metadata_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        let metadata = self.metadata();
        let (key, source) = metadata.provider_metadata_cpu_author().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "selected metadata binding CPU author is unsupported",
            )
        })?;
        if key != self.provider_binding_key
            || metadata.instance_id() != &self.descriptor.instance_id
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "Rewrite metadata CPU author belongs to another exact generation",
            ));
        }
        Ok(source)
    }
    pub fn planning_initial_dependencies(
        &self,
    ) -> Result<Vec<crate::connector::SharedDependency>, ConnectorError> {
        if self.rewrite.descriptor() != &self.descriptor
            || self.rewrite.binding_key() != &self.provider_binding_key
        {
            return Err(invalid(
                "Rewrite planning route author belongs to another exact generation",
            ));
        }
        self.rewrite.planning_initial_dependencies()
    }
    pub fn scoped_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        self.rewrite.scoped_cpu_source_type_id().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "original rewrite CPU source is unsupported",
            )
        })
    }
    pub fn prepare_plan_source_in(
        &self,
        request: &mut Option<ConnectorRewriteSourceRequest>,
        receiving: &mut Option<Box<dyn ConnectorRewritePlanSource>>,
    ) -> Result<(), ConnectorError> {
        if receiving.is_some() {
            return Err(invalid("rewrite source receiver is occupied"));
        }
        let original = request
            .as_ref()
            .ok_or_else(|| invalid("rewrite source request is empty"))?;
        original.validate()?;
        if original.table.instance_id != self.provider_binding_key.instance_id {
            return Err(invalid("rewrite source request has a foreign instance"));
        }
        self.rewrite.prepare_plan_source_in(request, receiving)
    }
    pub fn rewrite(&self) -> &Arc<dyn ConnectorDistributedRewrite> {
        &self.rewrite
    }
    pub fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
        creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        request.validate()?;
        if request.owner != self.provider_binding_key {
            return Err(invalid("distributed rewrite request does not match lease").into());
        }
        let plan = self.rewrite.plan_rewrite(request.clone(), creation)?;
        plan.validate()?;
        if plan.owner != self.provider_binding_key
            || plan.operation_id != request.operation_id
            || plan.request_digest != request.request_digest
        {
            return Err(invalid("distributed rewrite plan does not match request").into());
        }
        Ok(plan)
    }
    /// Builds the provider request behind the FE-owned runtime lease. The
    /// legacy execution binding remains private to the provider plan fence.
    pub fn plan_operation(
        &self,
        operation_id: ConnectorWriteOperationId,
        operation: ConnectorDistributedRewriteOperation,
        context: ConnectorRequestContext,
        creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            context,
        )?;
        self.plan_rewrite(request, creation)
    }
    pub fn finalize_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.validate_plan(plan)?;
        receipt.validate()?;
        let rewrite_receipt = self.rewrite.finalize_rewrite(plan, receipt)?;
        rewrite_receipt.validate()?;
        Ok(rewrite_receipt)
    }
    pub fn derive_write_lease(&self) -> Result<ConnectorWriteLease, ConnectorError> {
        let catalog_properties = self.planning_lease.binding().catalog_properties()?.clone();
        let retained = self.clone();
        ConnectorWriteLease::new_with_execution_distribution(
            self.control_runtime_id,
            self.provider_binding_key.clone(),
            self.write.clone(),
            self.descriptor.provider_id.clone(),
            self.distribution.clone(),
            move || drop(retained),
        )
        .and_then(|lease| lease.with_catalog_properties(catalog_properties))
    }
    pub fn validate_plan(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner != self.provider_binding_key {
            return Err(invalid("distributed rewrite plan does not match lease"));
        }
        Ok(())
    }
}

impl Drop for RewriteLeaseRelease {
    fn drop(&mut self) {
        if let Ok(mut release) = self.release.lock()
            && let Some(release) = release.take()
        {
            release();
        }
    }
}

pub(crate) fn validate_distributed_rewrite_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: super::ProviderBindingEpoch,
    rewrite: &dyn ConnectorDistributedRewrite,
) -> Result<(), ConnectorError> {
    if rewrite.descriptor() != descriptor
        || rewrite.binding_key().instance_id != descriptor.instance_id
        || rewrite.binding_key().incarnation != incarnation
    {
        return Err(invalid(
            "distributed rewrite capability owner does not match control binding",
        ));
    }
    Ok(())
}

fn request_digest(
    operation_id: ConnectorWriteOperationId,
    owner: &ConnectorProviderBindingKey,
    operation: &ConnectorDistributedRewriteOperation,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(REQUEST_DOMAIN);
    hash.update(CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION.to_be_bytes());
    hash.update(operation_id.to_bytes());
    digest_bytes(&mut hash, owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    operation.digest_into(&mut hash);
    hash.finalize().into()
}

fn rewrite_operation_intent(kind: &str) -> ConnectorWriteIntent {
    match kind {
        REWRITE_DATA_FILES_KIND => ConnectorWriteIntent::Overwrite,
        REWRITE_POSITION_DELETES_KIND => ConnectorWriteIntent::RowDelta,
        _ => unreachable!("validated distributed rewrite operation kind"),
    }
}

fn plan_digest(
    request: [u8; 32],
    target: &ConnectorTableHandle,
    state: [u8; 32],
    manifest: [u8; 32],
    summary: ConnectorDistributedRewritePlanSummary,
    payload: &Bytes,
    cohorts: &[ConnectorDistributedRewriteCohortPlan],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(PLAN_DOMAIN);
    hash.update(CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION.to_be_bytes());
    hash.update(request);
    digest_bytes(&mut hash, target.owner().as_str().as_bytes());
    digest_bytes(&mut hash, target.payload());
    hash.update(state);
    hash.update(manifest);
    summary.digest_into(&mut hash);
    digest_bytes(&mut hash, payload);
    hash.update((cohorts.len() as u64).to_be_bytes());
    for cohort in cohorts {
        cohort.digest_into(&mut hash);
    }
    hash.finalize().into()
}
fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn digest_optional_u64(hash: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.to_be_bytes());
        }
        None => hash.update([0]),
    }
}
fn validate_payload(value: &Bytes, kind: &str) -> Result<(), ConnectorError> {
    if value.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES {
        Err(exhausted(format!(
            "distributed rewrite {kind} payload exceeds hard limit"
        )))
    } else {
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::connector::{
        ConnectorWriteBaseVersion, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ConnectorWriteInputShape,
    };

    fn execution_schema(
        schema: &Schema,
    ) -> novarocks_connector_contract::ConnectorReadExecutionSchema {
        use novarocks_type_contract::{
            FunctionValueType, ValueLogicalType, ValueTypeStorageLifetime,
        };
        // These fixture columns are explicitly declared physical Int64 values.
        let bindings = schema
            .fields()
            .iter()
            .map(|field| {
                assert_eq!(field.data_type(), &DataType::Int64);
                FunctionValueType::try_with_logical_type(
                    DataType::Int64,
                    field.is_nullable(),
                    ValueLogicalType::Physical,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
            Arc::new(schema.clone()),
            bindings.into_boxed_slice(),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap()
    }

    fn request() -> ConnectorDistributedRewritePlanningRequest {
        let instance = ConnectorInstanceId::parse("rewrite-contract-test").unwrap();
        let owner = ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: super::super::ProviderBindingEpoch::from_bytes([9; 16]),
        };
        let table = ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table")).unwrap();
        ConnectorDistributedRewritePlanningRequest::try_new(
            ConnectorWriteOperationId::new(),
            owner,
            ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table,
                rewrite_all: true,
            },
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                crate::connector::ConnectorStopOwner::new().view(),
                1024,
                1024,
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn cohort_read() -> ConnectorRewriteCohortRead {
        ConnectorRewriteCohortRead::DeleteArtifactGroup(
            ConnectorFrozenRewriteGroup::try_new(
                "db",
                "orders",
                ConnectorWriteOperationId::from_bytes([1; 16]),
                [5; 32],
                super::super::ConnectorProcessLocalPayload::new(
                    request().owner().clone(),
                    Arc::new(()),
                ),
            )
            .unwrap(),
        )
    }

    fn preparation(
        request: &ConnectorDistributedRewritePlanningRequest,
        table: ConnectorTableHandle,
        intent: ConnectorWriteIntent,
        schema: &SchemaRef,
    ) -> ConnectorWritePreparation {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                    field.as_ref().clone(),
                )
            })
            .collect();
        ConnectorWritePreparation::try_new(
            request.owner().clone(),
            table,
            crate::connector::ConnectorWriteTargetRef::main(),
            intent,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap(),
            ConnectorWriteInputShape::Data { fields },
            Bytes::from_static(b"prepared"),
        )
        .unwrap()
    }

    #[test]
    fn cohort_schema_digest_is_part_of_the_frozen_plan_digest() {
        let request = request();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let cohort_id =
            ConnectorWriteCohortId::derive(request.operation_id(), b"test", [7; 32]).unwrap();
        let cohort = |schema_digest| {
            let mut request_layout = |_| Ok(());
            let mut work = |_| Ok(());
            let mut creation = crate::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut request_layout,
                &mut work,
            );
            ConnectorDistributedRewriteCohortPlan::try_new_typed(
                cohort_id,
                cohort_read(),
                Arc::clone(&schema),
                execution_schema(&schema),
                schema_digest,
                preparation(
                    &request,
                    request.operation().table().clone(),
                    ConnectorWriteIntent::Overwrite,
                    &schema,
                ),
                [7; 32],
                &mut creation,
            )
            .unwrap()
        };
        let first = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary::default(),
            Bytes::new(),
            vec![cohort([3; 32])],
            super::super::ConnectorProcessLocalPayload::new(request.owner().clone(), Arc::new(())),
        )
        .unwrap();
        let second = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary::default(),
            Bytes::new(),
            vec![cohort([4; 32])],
            super::super::ConnectorProcessLocalPayload::new(request.owner().clone(), Arc::new(())),
        )
        .unwrap();

        assert_ne!(first.plan_digest(), second.plan_digest());
    }

    fn nested_execution_schema(
        domain: novarocks_type_contract::ValueLogicalType,
        large: bool,
        root_nullable: bool,
        child_nullable: bool,
        name: &str,
    ) -> novarocks_connector_contract::ConnectorReadExecutionSchema {
        use novarocks_type_contract::{
            FunctionValueType, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        let child =
            FunctionValueType::try_with_logical_type(DataType::Utf8, child_nullable, domain)
                .unwrap();
        let item = Arc::new(Field::new("item", DataType::Utf8, child_nullable));
        let layout = if large {
            DataType::LargeList(item)
        } else {
            DataType::List(item)
        };
        let binding = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::List(child.value_type().clone()),
                root_nullable,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&layout, storage.clone()).unwrap(),
        )
        .unwrap();
        novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
            Arc::new(Schema::new(vec![Field::new(name, layout, root_nullable)])),
            vec![binding].into_boxed_slice(),
            storage,
        )
        .unwrap()
    }

    #[test]
    fn cohort_execution_digest_covers_nested_domain_nulls_names_and_exact_offsets() {
        use novarocks_type_contract::{ValueLogicalType, ValueTypeStorageLifetime};
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = crate::connector::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let variants = [
            (ValueLogicalType::Json, false, true, true, "value"),
            (ValueLogicalType::Physical, false, true, true, "value"),
            (ValueLogicalType::Json, true, true, true, "value"),
            (ValueLogicalType::Json, false, false, true, "value"),
            (ValueLogicalType::Json, false, true, false, "value"),
            (ValueLogicalType::Json, false, true, true, "other"),
        ];
        let digests = variants.map(|(domain, large, root, child, name)| {
            complete_schema_digest(
                &nested_execution_schema(domain, large, root, child, name),
                &mut creation,
            )
            .unwrap()
        });
        for (index, digest) in digests.iter().enumerate() {
            assert!(!digests[..index].contains(digest));
        }
    }

    #[test]
    fn cohort_execution_digest_preserves_first_control_refusal() {
        use novarocks_type_contract::{
            CompileControlError, ValueLogicalType, ValueTypeStorageLifetime,
        };
        let schema = nested_execution_schema(ValueLogicalType::Json, false, true, true, "value");
        let mut request = |_| Ok(());
        let mut count = 0;
        let mut work = |_| {
            count += 1;
            if count == 3 {
                Err(CompileControlError::DeadlineExceeded)
            } else {
                Ok(())
            }
        };
        let mut creation = crate::connector::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        assert!(matches!(
            complete_schema_digest(&schema, &mut creation),
            Err(
                novarocks_connector_contract::PureProviderCompileError::Control(
                    CompileControlError::DeadlineExceeded
                )
            )
        ));
        assert_eq!(count, 3);
    }

    #[test]
    fn plan_rejects_cohort_preparation_with_wrong_target_or_intent() {
        let request = request();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let cohort_id =
            ConnectorWriteCohortId::derive(request.operation_id(), b"test", [7; 32]).unwrap();
        let cohort = |preparation| {
            let mut request_layout = |_| Ok(());
            let mut work = |_| Ok(());
            let mut creation = crate::connector::read_stack::ConnectorReadSchemaCreation::new(
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                &mut request_layout,
                &mut work,
            );
            ConnectorDistributedRewriteCohortPlan::try_new_typed(
                cohort_id,
                cohort_read(),
                Arc::clone(&schema),
                execution_schema(&schema),
                [3; 32],
                preparation,
                [7; 32],
                &mut creation,
            )
            .unwrap()
        };
        let wrong_target = ConnectorTableHandle::try_new(
            request.owner().instance_id.clone(),
            Bytes::from_static(b"other-table"),
        )
        .unwrap();
        for preparation in [
            preparation(
                &request,
                wrong_target,
                ConnectorWriteIntent::Overwrite,
                &schema,
            ),
            preparation(
                &request,
                request.operation().table().clone(),
                ConnectorWriteIntent::RowDelta,
                &schema,
            ),
        ] {
            assert!(
                ConnectorDistributedRewritePlan::try_new(
                    &request,
                    [1; 32],
                    [2; 32],
                    ConnectorDistributedRewritePlanSummary::default(),
                    Bytes::new(),
                    vec![cohort(preparation)],
                    super::super::ConnectorProcessLocalPayload::new(
                        request.owner().clone(),
                        Arc::new(())
                    ),
                )
                .is_err()
            );
        }
    }
}
