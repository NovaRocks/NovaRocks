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
/// The selection facts travel with the kind rather than being re-derived. A
/// provider cuts the same groups twice — once when it freezes the plan, once
/// when it seals the session — and which delete artifacts a rewrite selected is
/// not a function of the target alone.
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
    pub expected_output_files: u64,
}

impl ConnectorDistributedRewritePlanSummary {
    fn digest_into(self, hash: &mut Sha256) {
        for value in [
            self.groups,
            self.input_data_files,
            self.input_delete_files,
            self.input_bytes,
            self.expected_output_files,
        ] {
            hash.update(value.to_be_bytes());
        }
    }
}

/// The immutable external artifact one rewrite cohort's group lives in.
///
/// A distributed rewrite freezes its whole selection into one artifact before
/// any cohort exists, then cuts it into groups. A cohort whose input is not
/// table rows names its group here instead of pinning a data file set: the
/// group is resolved back to its exact artifact list by the provider that
/// wrote it, and the commit replaces precisely that list.
///
/// The artifact is named by location *and* content digest, because a
/// replacement written at the same location is a different plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorFrozenRewriteGroup {
    schema_name: Arc<str>,
    table_name: Arc<str>,
    artifact_location: Arc<str>,
    artifact_digest: [u8; 32],
}

impl ConnectorFrozenRewriteGroup {
    pub fn try_new(
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        artifact_location: impl AsRef<str>,
        artifact_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        let schema_name = schema_name.as_ref();
        let table_name = table_name.as_ref();
        let artifact_location = artifact_location.as_ref();
        if schema_name.is_empty() || table_name.is_empty() {
            return Err(invalid(
                "distributed rewrite cohort group requires a schema-qualified relation name",
            ));
        }
        if artifact_location.is_empty()
            || artifact_location.len() > MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES
            || artifact_location.ends_with('/')
        {
            return Err(invalid(
                "distributed rewrite cohort artifact location must be a non-empty bounded object location",
            ));
        }
        Ok(Self {
            schema_name: Arc::from(schema_name),
            table_name: Arc::from(table_name),
            artifact_location: Arc::from(artifact_location),
            artifact_digest,
        })
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn artifact_location(&self) -> &str {
        &self.artifact_location
    }

    pub const fn artifact_digest(&self) -> [u8; 32] {
        self.artifact_digest
    }

    fn digest_into(&self, hash: &mut Sha256) {
        digest_bytes(hash, self.schema_name.as_bytes());
        digest_bytes(hash, self.table_name.as_bytes());
        digest_bytes(hash, self.artifact_location.as_bytes());
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
    scan_schema_digest: [u8; 32],
    preparation: ConnectorWritePreparation,
    group_digest: [u8; 32],
}

impl ConnectorDistributedRewriteCohortPlan {
    pub fn try_new(
        cohort_id: ConnectorWriteCohortId,
        read: ConnectorRewriteCohortRead,
        scan_schema: SchemaRef,
        scan_schema_digest: [u8; 32],
        preparation: ConnectorWritePreparation,
        group_digest: [u8; 32],
    ) -> Result<Self, ConnectorError> {
        preparation.validate()?;
        Ok(Self {
            cohort_id,
            read,
            scan_schema,
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
    pub fn scan_schema(&self) -> &SchemaRef {
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
    /// reduced to a kind and a target, because everything downstream that must
    /// re-cut the same groups needs its selection facts too.
    operation: ConnectorDistributedRewriteOperation,
    request_digest: [u8; 32],
    state_digest: [u8; 32],
    manifest_digest: [u8; 32],
    summary: ConnectorDistributedRewritePlanSummary,
    provider_payload: Bytes,
    cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
    plan_digest: [u8; 32],
}

impl ConnectorDistributedRewritePlan {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        request: &ConnectorDistributedRewritePlanningRequest,
        state_digest: [u8; 32],
        manifest_digest: [u8; 32],
        summary: ConnectorDistributedRewritePlanSummary,
        provider_payload: Bytes,
        mut cohorts: Vec<ConnectorDistributedRewriteCohortPlan>,
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
    /// this plan must cut the same groups, so it is told the same facts rather
    /// than left to infer them from the writer input.
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
    pub output_data_files: u64,
    pub output_delete_files: u64,
    pub output_rows: u64,
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
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;
    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError>;
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
    #[allow(clippy::too_many_arguments)]
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
    pub fn rewrite(&self) -> &Arc<dyn ConnectorDistributedRewrite> {
        &self.rewrite
    }
    pub fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        request.validate()?;
        if request.owner != self.provider_binding_key {
            return Err(invalid("distributed rewrite request does not match lease"));
        }
        let plan = self.rewrite.plan_rewrite(request.clone())?;
        plan.validate()?;
        if plan.owner != self.provider_binding_key
            || plan.operation_id != request.operation_id
            || plan.request_digest != request.request_digest
        {
            return Err(invalid("distributed rewrite plan does not match request"));
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
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            operation_id,
            self.provider_binding_key.clone(),
            operation,
            context,
        )?;
        self.plan_rewrite(request)
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

    struct NotCancelled;

    impl super::super::ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
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
                Arc::new(NotCancelled),
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
                "s3://warehouse/db/orders/_rewrite/0199",
                [5; 32],
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
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                cohort_read(),
                Arc::clone(&schema),
                schema_digest,
                preparation(
                    &request,
                    request.operation().table().clone(),
                    ConnectorWriteIntent::Overwrite,
                    &schema,
                ),
                [7; 32],
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
        )
        .unwrap();
        let second = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary::default(),
            Bytes::new(),
            vec![cohort([4; 32])],
        )
        .unwrap();

        assert_ne!(first.plan_digest(), second.plan_digest());
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
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                cohort_read(),
                Arc::clone(&schema),
                [3; 32],
                preparation,
                [7; 32],
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
                )
                .is_err()
            );
        }
    }
}
