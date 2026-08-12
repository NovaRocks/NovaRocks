// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Exact-generation Iceberg distributed-rewrite control.
//!
//! Frozen file ownership, durable attempt artifacts, and write activation all
//! live in the provider generation that owns the catalog client.  No Core
//! registry, process-global runtime, or current-generation lookup participates
//! in this capability.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorDistributedRewrite, ConnectorDistributedRewriteAttemptCheckpoint,
    ConnectorDistributedRewriteAttemptDisposition, ConnectorDistributedRewriteCohortPlan,
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteReceipt, ConnectorDistributedRewriteReceiptSummary, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorRequestContext,
    ConnectorStagedReport, ConnectorStagedReportSummary, ConnectorTableHandle,
    ConnectorWriteActivation, ConnectorWriteAttemptCompletion, ConnectorWriteBaseVersion,
    ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
    ConnectorWritePreparation, ConnectorWriteReceipt, ConnectorWriteTargetRef,
    ConnectorWriterIdentity, ConnectorWriterTerminalState, REWRITE_DATA_FILES_KIND,
    REWRITE_POSITION_DELETES_KIND,
};

use crate::commit::write_control::{
    IcebergDistributedRewriteActivation, IcebergDistributedRewriteCohortActivation,
    IcebergDistributedRewriteKind, IcebergWriteControl,
};
use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::manifest::{DataFileWithStats, data_file_with_stats_to_iceberg_data_file_info};
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
use crate::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat};

pub(crate) const ARTIFACT_VERSION: u16 = 1;
pub(crate) const GROUP_PAYLOAD_VERSION: u16 = 1;
pub(crate) const REWRITE_ARTIFACT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_GROUPS: usize = 4096;
pub(crate) const REWRITE_ARTIFACT_MAX_PARTS: usize = 64;
pub(crate) const REWRITE_ARTIFACT_MAX_PART_BYTES: usize = 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_ROOT_BYTES: usize = 64 * 1024;

const ATTEMPT_ARTIFACT_MAGIC: &[u8; 8] = b"NRRWAT01";
const ATTEMPT_ARTIFACT_VERSION: u16 = 1;
const GROUP_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.group.v1\0";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.state.v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewritePlanPayloadV1 {
    version: u16,
    artifact_digest_hex: String,
    artifact_location: String,
    target_ref: String,
}

#[derive(Clone)]
struct PlannedRewrite {
    plan: ConnectorDistributedRewritePlan,
    artifact: IcebergFrozenRewriteArtifactV1,
    artifact_location: String,
}

/// Complete distributed-rewrite capability for one provider generation.
pub struct IcebergDistributedRewriteControl {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergControlRuntime>,
    provider: Arc<IcebergControlProvider>,
    write: Arc<IcebergWriteControl>,
    plans: Mutex<HashMap<novarocks_spi::connector::ConnectorWriteOperationId, PlannedRewrite>>,
}

impl IcebergDistributedRewriteControl {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        runtime: Arc<IcebergControlRuntime>,
        provider: Arc<IcebergControlProvider>,
        write: Arc<IcebergWriteControl>,
    ) -> Result<Self, ConnectorError> {
        let key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        if provider.descriptor() != &descriptor
            || provider.incarnation() != incarnation
            || write.binding_key() != &key
            || !Arc::ptr_eq(provider.runtime(), &runtime)
        {
            return Err(invalid(
                "Iceberg distributed rewrite capabilities do not share one exact generation",
            ));
        }
        Ok(Self {
            key,
            descriptor,
            runtime,
            provider,
            write,
            plans: Mutex::new(HashMap::new()),
        })
    }

    fn planned(
        &self,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    ) -> Result<PlannedRewrite, ConnectorError> {
        self.plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| not_found("Iceberg rewrite operation has no frozen plan"))
    }

    fn build_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
    ) -> Result<PlannedRewrite, ConnectorError> {
        validate_context(&request.context)?;
        let target = self.provider.table_payload(request.operation().table())?;
        if target.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg distributed rewrite requires a base table handle",
            ));
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
        let table = self
            .runtime
            .load_table(&target.namespace, &target.table)
            .map_err(unavailable)?
            .into_table();
        let metadata = table.metadata();
        let base_snapshot_id = metadata.current_snapshot_id();
        let table_for_files = table.clone();
        let files = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats(&table_for_files).await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let groups = match request.operation() {
            ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                let live_delete_paths = live_delete_file_paths(&self.runtime, &table)?;
                plan_data_file_groups(files, &live_delete_paths)?
            }
            ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => {
                let groups = plan_position_delete_groups(
                    files,
                    *rewrite_all,
                    min_input_files.unwrap_or(2) as usize,
                )?;
                if !groups.is_empty()
                    && metadata.format_version() != crate::iceberg::spec::FormatVersion::V3
                {
                    return Err(invalid(
                        "Iceberg rewrite position delete files requires a format v3 table",
                    ));
                }
                groups
            }
        };
        let artifact = IcebergFrozenRewriteArtifactV1 {
            version: ARTIFACT_VERSION,
            operation_kind: request.operation().kind().to_string(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id,
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            groups,
        };
        let artifact_bytes = artifact.canonical_bytes()?;
        let manifest_digest = artifact_digest(&artifact_bytes);
        let artifact_location = format!(
            "{}/_novarocks/maintenance/v2/distributed-rewrite/{}/{}",
            metadata.location().trim_end_matches('/'),
            hex::encode(request.operation_id().to_bytes()),
            hex::encode(manifest_digest),
        );
        write_frozen_artifact(
            &self.runtime,
            table.file_io().clone(),
            &artifact,
            manifest_digest,
            &artifact_location,
        )?;
        let physical_schema = Arc::new(
            crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
                .map_err(|error| internal(format!("convert Iceberg rewrite schema: {error}")))?,
        );
        let row_lineage = crate::schema_facts::row_lineage_enabled(metadata);
        let scan_schema = rewrite_input_schema(request.operation(), physical_schema, row_lineage);
        let cohorts = cohort_plans_from_artifact(
            &self.key,
            request,
            manifest_digest,
            &artifact_location,
            &artifact.groups,
            scan_schema,
            row_lineage,
            metadata,
        )?;
        let state_digest = rewrite_state_digest(
            metadata.uuid().to_string().as_bytes(),
            table
                .metadata_location()
                .ok_or_else(|| invalid("Iceberg rewrite table has no metadata location"))?,
            base_snapshot_id,
            metadata.current_schema_id(),
            metadata.default_partition_spec_id(),
        );
        let summary = ConnectorDistributedRewritePlanSummary {
            groups: artifact.groups.len() as u64,
            input_data_files: artifact
                .groups
                .iter()
                .map(|group| group.data_files.len() as u64)
                .sum(),
            input_delete_files: artifact
                .groups
                .iter()
                .map(|group| {
                    (group.selected_position_delete_files.len()
                        + group.owned_data_delete_files.len()) as u64
                })
                .sum(),
            input_bytes: artifact
                .groups
                .iter()
                .flat_map(|group| &group.data_files)
                .map(|file| file.size.max(0) as u64)
                .sum(),
            expected_output_files: 0,
        };
        let payload = canonical_json(&IcebergRewritePlanPayloadV1 {
            version: 1,
            artifact_digest_hex: hex::encode(manifest_digest),
            artifact_location: artifact_location.clone(),
            target_ref: "main".to_string(),
        })?;
        let plan = ConnectorDistributedRewritePlan::try_new(
            request,
            state_digest,
            manifest_digest,
            summary,
            payload,
            cohorts,
        )?;
        Ok(PlannedRewrite {
            plan,
            artifact,
            artifact_location,
        })
    }

    fn attempt_file_io(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<crate::iceberg::io::FileIO, ConnectorError> {
        let target = self.provider.table_payload(plan.target())?;
        self.runtime
            .load_table(&target.namespace, &target.table)
            .map(|table| table.table.file_io().clone())
            .map_err(unavailable)
    }
}

impl ConnectorDistributedRewrite for IcebergDistributedRewriteControl {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        request.validate()?;
        if request.owner() != &self.key {
            return Err(invalid(
                "Iceberg distributed rewrite request has a foreign generation",
            ));
        }
        if let Some(existing) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&request.operation_id())
            .cloned()
        {
            return if existing.plan.request_digest() == request.request_digest() {
                Ok(existing.plan)
            } else {
                Err(invalid(
                    "Iceberg distributed rewrite operation conflicts with cached plan",
                ))
            };
        }
        let planned = self.build_plan(&request)?;
        let mut plans = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?;
        match plans.get(&request.operation_id()) {
            Some(existing) if existing.plan.request_digest() == request.request_digest() => {
                Ok(existing.plan.clone())
            }
            Some(_) => Err(invalid(
                "Iceberg distributed rewrite operation conflicts with cached plan",
            )),
            None => {
                let plan = planned.plan.clone();
                plans.insert(request.operation_id(), planned);
                Ok(plan)
            }
        }
    }

    fn activate_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteActivation, ConnectorError> {
        validate_context(&context)?;
        plan.validate()?;
        if plan.owner() != &self.key {
            return Err(invalid(
                "Iceberg rewrite activation has a foreign generation",
            ));
        }
        let planned = self.planned(plan.operation_id())?;
        if planned.plan.plan_digest() != plan.plan_digest()
            || planned.plan.manifest_digest() != plan.manifest_digest()
        {
            return Err(invalid(
                "Iceberg rewrite activation does not match its frozen plan",
            ));
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&planned.artifact.namespace, &planned.artifact.table);
        let table = self
            .runtime
            .load_table(&planned.artifact.namespace, &planned.artifact.table)
            .map_err(unavailable)?
            .into_table();
        validate_frozen_rewrite_table(&planned.artifact, table.metadata())?;
        let kind = match plan.operation_kind() {
            REWRITE_DATA_FILES_KIND => IcebergDistributedRewriteKind::Data,
            REWRITE_POSITION_DELETES_KIND => IcebergDistributedRewriteKind::PositionDeletes,
            _ => return Err(invalid("unsupported Iceberg rewrite operation kind")),
        };
        let mut cohorts = Vec::with_capacity(plan.cohorts().len());
        for cohort in plan.cohorts() {
            let group = planned
                .artifact
                .groups
                .iter()
                .find(|group| {
                    decode_digest(&group.group_digest_hex).ok() == Some(cohort.group_digest())
                })
                .ok_or_else(|| invalid("Iceberg rewrite cohort names an unknown frozen group"))?;
            let expected = ConnectorWriteCohortId::derive(
                plan.operation_id(),
                b"iceberg-distributed-rewrite-group",
                cohort.group_digest(),
            )?;
            if expected != cohort.cohort_id() {
                return Err(invalid(
                    "Iceberg rewrite cohort ID does not match frozen group",
                ));
            }
            let data_paths = group
                .data_files
                .iter()
                .map(|file| file.path.clone())
                .collect::<BTreeSet<_>>();
            let delete_paths = match kind {
                IcebergDistributedRewriteKind::Data => {
                    group.owned_data_delete_files.iter().cloned().collect()
                }
                IcebergDistributedRewriteKind::PositionDeletes => group
                    .selected_position_delete_files
                    .iter()
                    .cloned()
                    .collect(),
            };
            cohorts.push(IcebergDistributedRewriteCohortActivation {
                cohort_id: cohort.cohort_id(),
                preparation: cohort.preparation().clone(),
                control_payload: group_payload(
                    group,
                    plan.manifest_digest(),
                    &planned.artifact_location,
                )?,
                data_paths,
                delete_paths,
            });
        }
        self.write.activate_distributed_rewrite(
            plan.operation_id(),
            IcebergDistributedRewriteActivation { kind, cohorts },
            context,
        )
    }

    fn checkpoint_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        validate_rewrite_attempt(plan, completion)?;
        let location = attempt_artifact_location(plan, completion, disposition)?;
        let bytes = encode_attempt_artifact(completion)?;
        let artifact_digest: [u8; 32] = Sha256::digest(&bytes).into();
        write_artifact_file(
            &self.runtime,
            &self.attempt_file_io(plan)?,
            &location,
            bytes,
        )?;
        ConnectorDistributedRewriteAttemptCheckpoint::try_new(
            completion.cohort_id(),
            completion.execution_id(),
            disposition,
            completion.digest(),
            artifact_digest,
            checkpoint_handle(plan, disposition, completion, &location, artifact_digest)?,
        )
    }

    fn restore_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        checkpoint.validate()?;
        let handle: IcebergRewriteAttemptHandleV1 = decode_canonical_json(
            &checkpoint.artifact_handle,
            "Iceberg rewrite attempt handle",
        )?;
        validate_checkpoint_handle(plan, checkpoint, &handle)?;
        let bytes = read_artifact_file(
            &self.runtime,
            &self.attempt_file_io(plan)?,
            &handle.location,
            REWRITE_ARTIFACT_MAX_BYTES,
        )?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != checkpoint.artifact_digest || actual != handle.artifact_digest()? {
            return Err(invalid(
                "Iceberg rewrite attempt artifact digest is invalid",
            ));
        }
        let completion = decode_attempt_artifact(&bytes)?;
        validate_rewrite_attempt(plan, &completion)?;
        if completion.execution_id() != checkpoint.execution_id
            || completion.cohort_id() != checkpoint.cohort_id
            || completion.digest() != checkpoint.attempt_digest
        {
            return Err(invalid(
                "Iceberg rewrite restored attempt conflicts with its checkpoint",
            ));
        }
        Ok(completion)
    }

    fn finalize_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        receipt.validate()?;
        plan.validate()?;
        if plan.owner() != &self.key {
            return Err(invalid("Iceberg rewrite receipt has a foreign generation"));
        }
        let target = self.provider.table_payload(plan.target())?;
        self.runtime
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
        ConnectorDistributedRewriteReceipt::try_new(
            ConnectorDistributedRewriteReceiptSummary {
                input_data_files: plan.summary().input_data_files,
                input_delete_files: plan.summary().input_delete_files,
                output_data_files: 0,
                output_delete_files: 0,
                output_rows: receipt.resulting_row_count().unwrap_or(0),
                target_version: receipt
                    .committed_version()
                    .and_then(|version| version.snapshot_id()),
            },
            canonical_json(&IcebergRewriteReceiptPayloadV1 {
                version: 1,
                operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
                plan_digest_hex: hex::encode(plan.plan_digest()),
                receipt_digest_hex: hex::encode(receipt.digest()),
            })?,
        )
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewriteReceiptPayloadV1 {
    version: u16,
    operation_id_hex: String,
    plan_digest_hex: String,
    receipt_digest_hex: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteArtifactV1 {
    pub version: u16,
    pub operation_kind: String,
    pub namespace: String,
    pub table: String,
    pub table_uuid: String,
    pub target_ref: String,
    pub base_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub default_spec_id: i32,
    pub groups: Vec<IcebergFrozenRewriteGroupV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteGroupV1 {
    pub group_digest_hex: String,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub data_files: Vec<IcebergDataFileInfo>,
    pub selected_position_delete_files: Vec<String>,
    #[serde(default)]
    pub owned_data_delete_files: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergRewriteGroupPayloadV1 {
    pub version: u16,
    pub group_digest_hex: String,
    pub artifact_digest_hex: String,
    pub artifact_location: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRootV1 {
    version: u16,
    logical_artifact_digest_hex: String,
    operation_kind: String,
    namespace: String,
    table: String,
    table_uuid: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
    parts: Vec<ArtifactPartRefV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactPartRefV1 {
    index: u16,
    digest_hex: String,
    location: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactPartV1 {
    version: u16,
    groups: Vec<IcebergFrozenRewriteGroupV1>,
}

impl IcebergFrozenRewriteArtifactV1 {
    fn canonical_bytes(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ARTIFACT_VERSION || self.groups.len() > REWRITE_ARTIFACT_MAX_GROUPS {
            return Err(invalid("Iceberg rewrite artifact is invalid"));
        }
        let bytes = canonical_json(self)?;
        if bytes.len() > REWRITE_ARTIFACT_MAX_BYTES {
            return Err(exhausted("Iceberg rewrite artifact exceeds 64 MiB"));
        }
        Ok(bytes)
    }
}

pub(crate) fn decode_group_payload(
    payload: &[u8],
) -> Result<IcebergRewriteGroupPayloadV1, ConnectorError> {
    let decoded: IcebergRewriteGroupPayloadV1 =
        decode_canonical_json(payload, "Iceberg rewrite group payload")?;
    if decoded.version != GROUP_PAYLOAD_VERSION
        || decoded.artifact_location.is_empty()
        || decoded.artifact_location.len() > 16 * 1024
        || decoded.artifact_location.ends_with('/')
    {
        return Err(invalid("Iceberg rewrite group payload is invalid"));
    }
    decode_digest(&decoded.group_digest_hex)?;
    decode_digest(&decoded.artifact_digest_hex)?;
    Ok(decoded)
}

pub(crate) fn load_frozen_rewrite_group(
    runtime: &IcebergControlRuntime,
    file_io: &crate::iceberg::io::FileIO,
    payload: &IcebergRewriteGroupPayloadV1,
) -> Result<IcebergFrozenRewriteGroupV1, ConnectorError> {
    let root_location = format!("{}/manifest.json", payload.artifact_location);
    let root_bytes = read_artifact_file(
        runtime,
        file_io,
        &root_location,
        REWRITE_ARTIFACT_MAX_ROOT_BYTES,
    )?;
    let root: ArtifactRootV1 = decode_canonical_json(&root_bytes, "Iceberg rewrite artifact root")?;
    if root.version != ARTIFACT_VERSION
        || root.parts.is_empty()
        || root.parts.len() > REWRITE_ARTIFACT_MAX_PARTS
        || root.logical_artifact_digest_hex != payload.artifact_digest_hex
    {
        return Err(invalid("Iceberg rewrite artifact root is invalid"));
    }
    let expected = decode_digest(&payload.artifact_digest_hex)?;
    let mut groups = Vec::new();
    for (index, reference) in root.parts.iter().enumerate() {
        let location = format!("{}/part-{index:04}.json", payload.artifact_location);
        if reference.index as usize != index || reference.location != location {
            return Err(invalid(
                "Iceberg rewrite artifact part reference is invalid",
            ));
        }
        let bytes =
            read_artifact_file(runtime, file_io, &location, REWRITE_ARTIFACT_MAX_PART_BYTES)?;
        if artifact_part_digest(&bytes) != decode_digest(&reference.digest_hex)? {
            return Err(invalid("Iceberg rewrite artifact part digest is invalid"));
        }
        let part: ArtifactPartV1 = decode_canonical_json(&bytes, "Iceberg rewrite artifact part")?;
        if part.version != ARTIFACT_VERSION || part.groups.is_empty() {
            return Err(invalid("Iceberg rewrite artifact part is invalid"));
        }
        groups.extend(part.groups);
    }
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups.len() > REWRITE_ARTIFACT_MAX_GROUPS
        || groups
            .windows(2)
            .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid("Iceberg rewrite artifact groups are invalid"));
    }
    let logical = IcebergFrozenRewriteArtifactV1 {
        version: root.version,
        operation_kind: root.operation_kind,
        namespace: root.namespace,
        table: root.table,
        table_uuid: root.table_uuid,
        target_ref: root.target_ref,
        base_snapshot_id: root.base_snapshot_id,
        schema_id: root.schema_id,
        default_spec_id: root.default_spec_id,
        groups,
    };
    if artifact_digest(&logical.canonical_bytes()?) != expected {
        return Err(invalid(
            "Iceberg rewrite logical artifact digest is invalid",
        ));
    }
    logical
        .groups
        .into_iter()
        .find(|group| group.group_digest_hex == payload.group_digest_hex)
        .ok_or_else(|| invalid("Iceberg rewrite artifact has no requested group"))
}

fn write_frozen_artifact(
    runtime: &IcebergControlRuntime,
    file_io: crate::iceberg::io::FileIO,
    artifact: &IcebergFrozenRewriteArtifactV1,
    logical_digest: [u8; 32],
    root_location: &str,
) -> Result<(), ConnectorError> {
    let parts = split_artifact_parts(&artifact.groups)?;
    let mut references = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical_json(part)?;
        let digest = artifact_part_digest(&bytes);
        let location = format!("{root_location}/part-{index:04}.json");
        write_artifact_file(runtime, &file_io, &location, bytes)?;
        references.push(ArtifactPartRefV1 {
            index: index as u16,
            digest_hex: hex::encode(digest),
            location,
        });
    }
    let root = ArtifactRootV1 {
        version: ARTIFACT_VERSION,
        logical_artifact_digest_hex: hex::encode(logical_digest),
        operation_kind: artifact.operation_kind.clone(),
        namespace: artifact.namespace.clone(),
        table: artifact.table.clone(),
        table_uuid: artifact.table_uuid.clone(),
        target_ref: artifact.target_ref.clone(),
        base_snapshot_id: artifact.base_snapshot_id,
        schema_id: artifact.schema_id,
        default_spec_id: artifact.default_spec_id,
        parts: references,
    };
    let bytes = canonical_json(&root)?;
    if bytes.len() > REWRITE_ARTIFACT_MAX_ROOT_BYTES {
        return Err(exhausted("Iceberg rewrite artifact root exceeds 64 KiB"));
    }
    write_artifact_file(
        runtime,
        &file_io,
        &format!("{root_location}/manifest.json"),
        bytes,
    )
}

fn split_artifact_parts(
    groups: &[IcebergFrozenRewriteGroupV1],
) -> Result<Vec<ArtifactPartV1>, ConnectorError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    for group in groups {
        let mut candidate = current.clone();
        candidate.push(group.clone());
        let candidate_part = ArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: candidate,
        };
        if canonical_json(&candidate_part)?.len() <= REWRITE_ARTIFACT_MAX_PART_BYTES {
            current = candidate_part.groups;
            continue;
        }
        if current.is_empty() {
            return Err(exhausted("Iceberg rewrite group exceeds 1 MiB"));
        }
        parts.push(ArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: std::mem::take(&mut current),
        });
        current.push(group.clone());
    }
    if !current.is_empty() {
        parts.push(ArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: current,
        });
    }
    if parts.len() > REWRITE_ARTIFACT_MAX_PARTS {
        return Err(exhausted("Iceberg rewrite artifact exceeds 64 parts"));
    }
    Ok(parts)
}

pub(crate) fn plan_data_file_groups(
    files: Vec<DataFileWithStats>,
    live_delete_paths: &BTreeSet<String>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    let mut groups = group_data_files(files, false, None)?;
    assign_unattached_delete_owners(&mut groups, live_delete_paths)?;
    Ok(groups)
}

pub(crate) fn plan_position_delete_groups(
    files: Vec<DataFileWithStats>,
    rewrite_all: bool,
    min_input_files: usize,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    group_data_files(
        files
            .into_iter()
            .map(data_file_with_stats_to_iceberg_data_file_info)
            .collect(),
        rewrite_all,
        Some(min_input_files),
    )
}

fn group_data_files(
    mut files: Vec<IcebergDataFileInfo>,
    rewrite_all: bool,
    position_min_inputs: Option<usize>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    files.sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(min_inputs) = position_min_inputs {
        let mut groups = Vec::new();
        for file in files {
            if file.delete_files.iter().any(|delete| {
                delete.file_content == IcebergDeleteFileContent::Position
                    && delete.file_format == IcebergDeleteFileFormat::Parquet
            }) {
                return Err(invalid("V2 Parquet position delete rewrite is unsupported"));
            }
            let selected = file
                .delete_files
                .iter()
                .filter(|delete| {
                    delete.file_content == IcebergDeleteFileContent::Position
                        && delete.file_format == IcebergDeleteFileFormat::Puffin
                })
                .map(|delete| delete.path.clone())
                .collect::<Vec<_>>();
            if selected.is_empty() || (!rewrite_all && selected.len() < min_inputs) {
                continue;
            }
            let digest = position_group_digest(&file.path, &selected);
            groups.push(IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(digest),
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key.clone(),
                data_files: vec![file],
                selected_position_delete_files: selected,
                owned_data_delete_files: Vec::new(),
            });
        }
        return bounded_groups(groups);
    }
    let mut by_partition =
        BTreeMap::<(Option<i32>, Option<String>), Vec<IcebergDataFileInfo>>::new();
    for file in files {
        by_partition
            .entry((file.partition_spec_id, file.partition_key.clone()))
            .or_default()
            .push(file);
    }
    let mut groups = by_partition
        .into_iter()
        .map(|((partition_spec_id, partition_key), mut data_files)| {
            data_files.sort_by(|left, right| left.path.cmp(&right.path));
            IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(data_group_digest(
                    partition_spec_id,
                    partition_key.as_deref(),
                    &data_files,
                )),
                partition_spec_id,
                partition_key,
                data_files,
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    assign_attached_delete_owners(&mut groups);
    bounded_groups(groups)
}

fn assign_attached_delete_owners(groups: &mut [IcebergFrozenRewriteGroupV1]) {
    let mut owners = BTreeMap::<String, usize>::new();
    for (index, group) in groups.iter().enumerate() {
        for path in group
            .data_files
            .iter()
            .flat_map(|file| file.delete_files.iter().map(|delete| delete.path.as_str()))
        {
            match owners.get(path) {
                Some(existing) if groups[*existing].group_digest_hex <= group.group_digest_hex => {}
                _ => {
                    owners.insert(path.to_string(), index);
                }
            }
        }
    }
    for (path, owner) in owners {
        groups[owner].owned_data_delete_files.push(path);
    }
    for group in groups {
        group.owned_data_delete_files.sort();
        group.owned_data_delete_files.dedup();
    }
}

fn assign_unattached_delete_owners(
    groups: &mut [IcebergFrozenRewriteGroupV1],
    live: &BTreeSet<String>,
) -> Result<(), ConnectorError> {
    let owned = groups
        .iter()
        .flat_map(|group| group.owned_data_delete_files.iter().cloned())
        .collect::<BTreeSet<_>>();
    if !owned.is_subset(live) {
        return Err(invalid(
            "Iceberg rewrite contains a non-live delete dependency",
        ));
    }
    let missing = live.difference(&owned).cloned().collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let owner = groups
        .iter_mut()
        .min_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex))
        .ok_or_else(|| invalid("Iceberg rewrite has delete files but no data cohort"))?;
    owner.owned_data_delete_files.extend(missing);
    owner.owned_data_delete_files.sort();
    owner.owned_data_delete_files.dedup();
    Ok(())
}

fn bounded_groups(
    mut groups: Vec<IcebergFrozenRewriteGroupV1>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups.len() > REWRITE_ARTIFACT_MAX_GROUPS {
        return Err(exhausted("Iceberg rewrite exceeds 4096 cohorts"));
    }
    if groups
        .windows(2)
        .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid("Iceberg rewrite group digest collision"));
    }
    Ok(groups)
}

#[allow(clippy::too_many_arguments)]
fn cohort_plans_from_artifact(
    owner: &ConnectorExecutionBindingKey,
    request: &ConnectorDistributedRewritePlanningRequest,
    artifact_digest: [u8; 32],
    artifact_location: &str,
    groups: &[IcebergFrozenRewriteGroupV1],
    scan_schema: SchemaRef,
    row_lineage: bool,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Vec<ConnectorDistributedRewriteCohortPlan>, ConnectorError> {
    let intent = match request.operation() {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
            ConnectorWriteIntent::Overwrite
        }
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            ConnectorWriteIntent::RowDelta
        }
    };
    groups
        .iter()
        .map(|group| {
            let group_digest = decode_digest(&group.group_digest_hex)?;
            let cohort_id = ConnectorWriteCohortId::derive(
                request.operation_id(),
                b"iceberg-distributed-rewrite-group",
                group_digest,
            )?;
            let source = frozen_rewrite_source_handle(
                request.operation().table(),
                request.operation().kind(),
                IcebergRewriteGroupPayloadV1 {
                    version: GROUP_PAYLOAD_VERSION,
                    group_digest_hex: group.group_digest_hex.clone(),
                    artifact_digest_hex: hex::encode(artifact_digest),
                    artifact_location: artifact_location.to_string(),
                },
            )?;
            let preparation = rewrite_preparation(
                owner,
                request,
                intent,
                scan_schema.as_ref(),
                row_lineage,
                metadata,
            )?;
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                source,
                scan_schema.clone(),
                arrow_schema_digest(&scan_schema),
                preparation,
                group_digest,
            )
        })
        .collect()
}

fn rewrite_preparation(
    owner: &ConnectorExecutionBindingKey,
    request: &ConnectorDistributedRewritePlanningRequest,
    intent: ConnectorWriteIntent,
    scan_schema: &Schema,
    row_lineage: bool,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<ConnectorWritePreparation, ConnectorError> {
    let fields = scan_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorWriteFieldBinding::new(
                rewrite_field_token(owner, request.operation().table(), intent, ordinal, field),
                field.as_ref().clone(),
            )
        })
        .collect::<Vec<_>>();
    let input = match request.operation() {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } if row_lineage => {
            let row_identity_fields = fields
                .iter()
                .filter(|field| {
                    matches!(
                        field.field().name().as_str(),
                        ICEBERG_ROW_ID_COL | ICEBERG_LAST_UPDATED_SEQ_COL
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let data_fields = fields
                .into_iter()
                .filter(|field| {
                    !matches!(
                        field.field().name().as_str(),
                        ICEBERG_ROW_ID_COL | ICEBERG_LAST_UPDATED_SEQ_COL
                    )
                })
                .collect();
            ConnectorWriteInputShape::RowLineage {
                data_fields,
                row_identity_fields,
            }
        }
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
            ConnectorWriteInputShape::Data { fields }
        }
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: fields,
                partition_source_fields: partition_source_bindings(
                    owner,
                    request.operation().table(),
                    intent,
                    metadata,
                )?,
            }
        }
    };
    let table_uuid = metadata.uuid().to_string();
    let snapshot = metadata
        .current_snapshot_id()
        .map_or_else(|| "none".to_string(), |value| value.to_string());
    ConnectorWritePreparation::try_new(
        owner.clone(),
        request.operation().table().clone(),
        ConnectorWriteTargetRef::main(),
        intent,
        ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
            "iceberg/write-base/v1/{table_uuid}/main/{snapshot}"
        )))?,
        input,
        Bytes::from(format!(
            "iceberg/distributed-rewrite-preparation/v1/{}/{}/{}/{}",
            owner.instance_id.as_str(),
            hex::encode(request.operation_id().to_bytes()),
            request.operation().kind(),
            snapshot,
        )),
    )
}

fn partition_source_bindings(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    intent: ConnectorWriteIntent,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Vec<ConnectorWriteFieldBinding>, ConnectorError> {
    let arrow = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
        .map_err(|error| internal(format!("convert Iceberg rewrite partition schema: {error}")))?;
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, partition)| {
            let source = metadata
                .current_schema()
                .field_by_id(partition.source_id)
                .ok_or_else(|| corrupt("Iceberg rewrite partition source field is missing"))?;
            let (_, field) = metadata
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, candidate)| candidate.id == source.id)
                .ok_or_else(|| corrupt("Iceberg rewrite partition source ordinal is missing"))?;
            let field_ordinal = metadata
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .position(|candidate| candidate.id == field.id)
                .ok_or_else(|| corrupt("Iceberg rewrite partition source is not top-level"))?;
            let exact = arrow.field(field_ordinal).clone();
            Ok(ConnectorWriteFieldBinding::new(
                rewrite_field_token(owner, table, intent, 10_000 + ordinal, &exact),
                exact.as_ref().clone(),
            ))
        })
        .collect()
}

fn rewrite_field_token(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    intent: ConnectorWriteIntent,
    ordinal: usize,
    field: &Field,
) -> ConnectorWriteFieldToken {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.write-field-token.v1\0");
    hash.update(owner.instance_id.as_str().as_bytes());
    hash.update(owner.incarnation.to_bytes());
    hash.update(table.payload());
    hash.update(format!("{intent:?}").as_bytes());
    hash.update([9]);
    hash.update((ordinal as u64).to_be_bytes());
    hash.update(format!("{field:?}").as_bytes());
    ConnectorWriteFieldToken::from_bytes(hash.finalize().into())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenRewriteSourceV1 {
    version: u16,
    operation_kind: String,
    group: IcebergRewriteGroupPayloadV1,
}

/// Decode the provider-only source facts carried by a rewrite scan handle.
pub(crate) fn decode_frozen_rewrite_source(
    payload: &[u8],
) -> Result<(String, String, String, IcebergRewriteGroupPayloadV1), ConnectorError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode Iceberg rewrite source: {error}")))?;
    let namespace = value
        .get("namespace")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("Iceberg rewrite source namespace is missing"))?
        .to_string();
    let table = value
        .get("table")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid("Iceberg rewrite source table is missing"))?
        .to_string();
    let frozen: FrozenRewriteSourceV1 = serde_json::from_value(
        value
            .get("frozen_rewrite")
            .cloned()
            .ok_or_else(|| invalid("Iceberg rewrite source facts are missing"))?,
    )
    .map_err(|error| invalid(format!("decode Iceberg rewrite source facts: {error}")))?;
    if frozen.version != 1
        || !matches!(
            frozen.operation_kind.as_str(),
            REWRITE_DATA_FILES_KIND | REWRITE_POSITION_DELETES_KIND
        )
    {
        return Err(invalid("Iceberg rewrite source facts are invalid"));
    }
    decode_group_payload(&canonical_json(&frozen.group)?)?;
    Ok((namespace, table, frozen.operation_kind, frozen.group))
}

fn frozen_rewrite_source_handle(
    original: &ConnectorTableHandle,
    operation_kind: &str,
    group: IcebergRewriteGroupPayloadV1,
) -> Result<ConnectorTableHandle, ConnectorError> {
    let mut table: Value = serde_json::from_slice(original.payload())
        .map_err(|error| invalid(format!("decode Iceberg rewrite target: {error}")))?;
    let object = table
        .as_object_mut()
        .ok_or_else(|| invalid("Iceberg rewrite target payload is not an object"))?;
    object.insert("table_info".to_string(), Value::Null);
    object.insert("metadata_columns".to_string(), Value::Array(Vec::new()));
    object.insert("prepared_files".to_string(), Value::Array(Vec::new()));
    object.insert("explicit_files".to_string(), Value::Null);
    object.insert(
        "logical_type_columns".to_string(),
        Value::Object(Default::default()),
    );
    object.insert("hidden_columns".to_string(), Value::Array(Vec::new()));
    object.insert(
        "frozen_rewrite".to_string(),
        serde_json::to_value(FrozenRewriteSourceV1 {
            version: 1,
            operation_kind: operation_kind.to_string(),
            group,
        })
        .map_err(|error| internal(format!("encode Iceberg rewrite source: {error}")))?,
    );
    let payload = canonical_json(&table)?;
    if payload.len()
        > novarocks_spi::connector::MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES
    {
        return Err(exhausted("Iceberg rewrite source handle exceeds 64 KiB"));
    }
    ConnectorTableHandle::try_new(original.owner().clone(), payload)
}

pub(crate) fn rewrite_input_schema(
    operation: &ConnectorDistributedRewriteOperation,
    physical_schema: SchemaRef,
    row_lineage: bool,
) -> SchemaRef {
    frozen_rewrite_scan_schema(operation.kind(), physical_schema, row_lineage)
}

/// The schema one frozen rewrite cohort reads.
///
/// Planning freezes this into the cohort plan and `begin_scan` has to reproduce
/// it field-for-field — the frozen read refuses a scan whose output schema
/// differs — so both sides resolve it here rather than deriving it twice.
pub(crate) fn frozen_rewrite_scan_schema(
    operation_kind: &str,
    physical_schema: SchemaRef,
    row_lineage: bool,
) -> SchemaRef {
    match operation_kind {
        REWRITE_POSITION_DELETES_KIND => Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
        ])),
        _ if row_lineage => {
            let mut fields = physical_schema.fields().to_vec();
            fields.extend([
                Arc::new(Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false)),
                Arc::new(Field::new(
                    ICEBERG_LAST_UPDATED_SEQ_COL,
                    DataType::Int64,
                    true,
                )),
            ]);
            Arc::new(Schema::new(fields))
        }
        _ => physical_schema,
    }
}

/// Decode the optional frozen-rewrite facts a rewrite source handle carries.
/// An ordinary table handle has none and yields `None`.
pub(crate) fn frozen_rewrite_source_facts(
    payload: &[u8],
) -> Result<Option<(String, IcebergRewriteGroupPayloadV1)>, ConnectorError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode Iceberg rewrite source: {error}")))?;
    if value.get("frozen_rewrite").is_none_or(Value::is_null) {
        return Ok(None);
    }
    let (_, _, operation_kind, group) = decode_frozen_rewrite_source(payload)?;
    Ok(Some((operation_kind, group)))
}

fn group_payload(
    group: &IcebergFrozenRewriteGroupV1,
    artifact_digest: [u8; 32],
    artifact_location: &str,
) -> Result<Bytes, ConnectorError> {
    canonical_json(&IcebergRewriteGroupPayloadV1 {
        version: GROUP_PAYLOAD_VERSION,
        group_digest_hex: group.group_digest_hex.clone(),
        artifact_digest_hex: hex::encode(artifact_digest),
        artifact_location: artifact_location.to_string(),
    })
}

fn live_delete_file_paths(
    runtime: &IcebergControlRuntime,
    table: &crate::iceberg::table::Table,
) -> Result<BTreeSet<String>, ConnectorError> {
    let Some(snapshot) = table.metadata().current_snapshot().cloned() else {
        return Ok(BTreeSet::new());
    };
    let file_io = table.file_io().clone();
    let metadata = table.metadata().clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            let manifest_list = snapshot
                .load_manifest_list(&file_io, &metadata)
                .await
                .map_err(|error| format!("load Iceberg rewrite manifest list: {error}"))?;
            let mut paths = BTreeSet::new();
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != crate::iceberg::spec::ManifestContentType::Deletes {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(&file_io)
                    .await
                    .map_err(|error| format!("load Iceberg rewrite delete manifest: {error}"))?;
                for entry in manifest.entries() {
                    if entry.is_alive() {
                        paths.insert(entry.data_file().file_path().to_string());
                    }
                }
            }
            Ok::<_, String>(paths)
        })
        .map_err(unavailable)?
        .map_err(unavailable)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewriteAttemptHandleV1 {
    version: u16,
    operation_id_hex: String,
    plan_digest_hex: String,
    cohort_id_hex: String,
    query_id_hex: String,
    attempt_id: u64,
    disposition: String,
    attempt_digest_hex: String,
    artifact_digest_hex: String,
    location: String,
}

impl IcebergRewriteAttemptHandleV1 {
    fn disposition(&self) -> Result<ConnectorDistributedRewriteAttemptDisposition, ConnectorError> {
        match self.disposition.as_str() {
            "accepted" => Ok(ConnectorDistributedRewriteAttemptDisposition::Accepted),
            "superseded" => Ok(ConnectorDistributedRewriteAttemptDisposition::Superseded),
            _ => Err(invalid("Iceberg rewrite checkpoint disposition is invalid")),
        }
    }

    fn artifact_digest(&self) -> Result<[u8; 32], ConnectorError> {
        decode_digest(&self.artifact_digest_hex)
    }
}

fn checkpoint_handle(
    plan: &ConnectorDistributedRewritePlan,
    disposition: ConnectorDistributedRewriteAttemptDisposition,
    completion: &ConnectorWriteAttemptCompletion,
    location: &str,
    artifact_digest: [u8; 32],
) -> Result<Bytes, ConnectorError> {
    canonical_json(&IcebergRewriteAttemptHandleV1 {
        version: 1,
        operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
        plan_digest_hex: hex::encode(plan.plan_digest()),
        cohort_id_hex: hex::encode(completion.cohort_id().to_bytes()),
        query_id_hex: hex::encode(completion.execution_id().query_id()),
        attempt_id: completion.execution_id().attempt_id(),
        disposition: match disposition {
            ConnectorDistributedRewriteAttemptDisposition::Accepted => "accepted".to_string(),
            ConnectorDistributedRewriteAttemptDisposition::Superseded => "superseded".to_string(),
        },
        attempt_digest_hex: hex::encode(completion.digest()),
        artifact_digest_hex: hex::encode(artifact_digest),
        location: location.to_string(),
    })
}

fn validate_checkpoint_handle(
    plan: &ConnectorDistributedRewritePlan,
    checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    handle: &IcebergRewriteAttemptHandleV1,
) -> Result<(), ConnectorError> {
    if handle.version != 1
        || handle.location.is_empty()
        || handle.location.len() > 16 * 1024
        || handle.operation_id_hex != hex::encode(plan.operation_id().to_bytes())
        || decode_digest(&handle.plan_digest_hex)? != plan.plan_digest()
        || decode_digest(&handle.cohort_id_hex)? != checkpoint.cohort_id.to_bytes()
        || handle.query_id_hex != hex::encode(checkpoint.execution_id.query_id())
        || handle.attempt_id != checkpoint.execution_id.attempt_id()
        || handle.disposition()? != checkpoint.disposition
        || decode_digest(&handle.attempt_digest_hex)? != checkpoint.attempt_digest
        || handle.artifact_digest()? != checkpoint.artifact_digest
    {
        return Err(invalid(
            "Iceberg rewrite checkpoint handle does not match checkpoint facts",
        ));
    }
    Ok(())
}

fn attempt_artifact_location(
    plan: &ConnectorDistributedRewritePlan,
    completion: &ConnectorWriteAttemptCompletion,
    disposition: ConnectorDistributedRewriteAttemptDisposition,
) -> Result<String, ConnectorError> {
    if !plan
        .cohorts()
        .iter()
        .any(|cohort| cohort.cohort_id() == completion.cohort_id())
    {
        return Err(invalid("Iceberg rewrite attempt names an unknown cohort"));
    }
    let payload: IcebergRewritePlanPayloadV1 = decode_canonical_json(
        plan.provider_payload(),
        "Iceberg distributed rewrite plan payload",
    )?;
    if payload.version != 1 {
        return Err(invalid(
            "Iceberg rewrite plan payload version is unsupported",
        ));
    }
    Ok(format!(
        "{}/attempts/{}-{}-{:020}-{}.bin",
        payload.artifact_location,
        hex::encode(completion.cohort_id().to_bytes()),
        hex::encode(completion.execution_id().query_id()),
        completion.execution_id().attempt_id(),
        match disposition {
            ConnectorDistributedRewriteAttemptDisposition::Accepted => "accepted",
            ConnectorDistributedRewriteAttemptDisposition::Superseded => "superseded",
        }
    ))
}

fn validate_rewrite_attempt(
    plan: &ConnectorDistributedRewritePlan,
    completion: &ConnectorWriteAttemptCompletion,
) -> Result<(), ConnectorError> {
    if completion.owner() != plan.owner()
        || completion.operation_id() != plan.operation_id()
        || !plan
            .cohorts()
            .iter()
            .any(|cohort| cohort.cohort_id() == completion.cohort_id())
    {
        return Err(invalid(
            "Iceberg distributed rewrite completion is foreign to its plan",
        ));
    }
    Ok(())
}

fn encode_attempt_artifact(
    completion: &ConnectorWriteAttemptCompletion,
) -> Result<Bytes, ConnectorError> {
    let mut out = Vec::new();
    out.extend_from_slice(ATTEMPT_ARTIFACT_MAGIC);
    put_u16(&mut out, ATTEMPT_ARTIFACT_VERSION);
    put_binding_key(&mut out, completion.owner())?;
    out.extend_from_slice(&completion.operation_id().to_bytes());
    out.extend_from_slice(&completion.cohort_id().to_bytes());
    put_execution_id(&mut out, completion.execution_id());
    out.extend_from_slice(&completion.manifest_digest());
    put_blob(&mut out, completion.control_payload())?;
    let report_count = u32::try_from(completion.reports().len())
        .map_err(|_| exhausted("Iceberg rewrite attempt has too many reports"))?;
    put_u32(&mut out, report_count);
    for report in completion.reports() {
        put_writer(&mut out, report.writer())?;
        put_u32(&mut out, report.version());
        out.push(match report.state() {
            ConnectorWriterTerminalState::Staged => 1,
            ConnectorWriterTerminalState::Aborted => 2,
            ConnectorWriterTerminalState::Failed => 3,
        });
        let summary = report.summary();
        put_u64(&mut out, summary.input_rows);
        put_u64(&mut out, summary.staged_bytes);
        put_u64(&mut out, summary.artifact_count);
        put_blob(&mut out, report.payload())?;
    }
    if out.len() > REWRITE_ARTIFACT_MAX_BYTES {
        return Err(exhausted("Iceberg rewrite attempt artifact exceeds 64 MiB"));
    }
    Ok(Bytes::from(out))
}

fn decode_attempt_artifact(
    payload: &[u8],
) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
    if payload.len() > REWRITE_ARTIFACT_MAX_BYTES {
        return Err(exhausted("Iceberg rewrite attempt artifact exceeds 64 MiB"));
    }
    let mut cursor = AttemptCursor::new(payload);
    if cursor.take_exact(ATTEMPT_ARTIFACT_MAGIC.len())? != ATTEMPT_ARTIFACT_MAGIC
        || cursor.take_u16()? != ATTEMPT_ARTIFACT_VERSION
    {
        return Err(invalid(
            "Iceberg rewrite attempt artifact version is invalid",
        ));
    }
    let owner = cursor.take_binding_key()?;
    let operation_id =
        novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(cursor.take_array()?);
    let cohort_id = ConnectorWriteCohortId::from_bytes(cursor.take_array()?);
    let execution_id = cursor.take_execution_id()?;
    let manifest_digest = cursor.take_array()?;
    let control_payload = cursor.take_blob()?;
    let report_count = cursor.take_u32()? as usize;
    if report_count == 0
        || report_count > novarocks_spi::connector::MAX_CONNECTOR_WRITE_OPERATION_WRITERS
    {
        return Err(invalid("Iceberg rewrite attempt report count is invalid"));
    }
    let mut reports = Vec::with_capacity(report_count);
    for _ in 0..report_count {
        let writer = cursor.take_writer()?;
        let version = cursor.take_u32()?;
        let state = match cursor.take_u8()? {
            1 => ConnectorWriterTerminalState::Staged,
            2 => ConnectorWriterTerminalState::Aborted,
            3 => ConnectorWriterTerminalState::Failed,
            _ => return Err(invalid("Iceberg rewrite attempt report state is invalid")),
        };
        let summary = ConnectorStagedReportSummary {
            input_rows: cursor.take_u64()?,
            staged_bytes: cursor.take_u64()?,
            artifact_count: cursor.take_u64()?,
        };
        reports.push(ConnectorStagedReport::try_new(
            writer,
            version,
            state,
            summary,
            cursor.take_blob()?,
        )?);
    }
    if !cursor.is_finished() {
        return Err(invalid(
            "Iceberg rewrite attempt artifact has trailing bytes",
        ));
    }
    ConnectorWriteAttemptCompletion::try_new(
        owner,
        operation_id,
        cohort_id,
        execution_id,
        manifest_digest,
        reports,
        control_payload,
    )
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_blob(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ConnectorError> {
    let length = u32::try_from(value.len())
        .map_err(|_| exhausted("Iceberg rewrite attempt blob exceeds u32"))?;
    put_u32(out, length);
    out.extend_from_slice(value);
    Ok(())
}

fn put_binding_key(
    out: &mut Vec<u8>,
    key: &ConnectorExecutionBindingKey,
) -> Result<(), ConnectorError> {
    let instance = key.instance_id.as_str().as_bytes();
    let length = u16::try_from(instance.len())
        .map_err(|_| invalid("Iceberg rewrite binding instance ID is too large"))?;
    put_u16(out, length);
    out.extend_from_slice(instance);
    out.extend_from_slice(&key.incarnation.to_bytes());
    Ok(())
}

fn put_execution_id(
    out: &mut Vec<u8>,
    execution_id: novarocks_spi::connector::ConnectorWriteExecutionId,
) {
    out.extend_from_slice(&execution_id.query_id());
    put_u64(out, execution_id.attempt_id());
}

fn put_writer(out: &mut Vec<u8>, writer: &ConnectorWriterIdentity) -> Result<(), ConnectorError> {
    out.extend_from_slice(&writer.operation_id().to_bytes());
    out.extend_from_slice(&writer.cohort_id().to_bytes());
    put_execution_id(out, writer.execution_id());
    out.extend_from_slice(&writer.fragment_instance_id());
    out.extend_from_slice(&writer.fragment_id().to_be_bytes());
    out.extend_from_slice(&writer.backend_num().to_be_bytes());
    put_u32(out, writer.sink_ordinal());
    put_binding_key(out, writer.binding_key())
}

struct AttemptCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> AttemptCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    fn take_exact(&mut self, length: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.payload.len())
            .ok_or_else(|| invalid("Iceberg rewrite attempt artifact is truncated"))?;
        let value = &self.payload[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        self.take_exact(N)?
            .try_into()
            .map_err(|_| invalid("Iceberg rewrite attempt array is invalid"))
    }

    fn take_u8(&mut self) -> Result<u8, ConnectorError> {
        Ok(self.take_exact(1)?[0])
    }

    fn take_u16(&mut self) -> Result<u16, ConnectorError> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn take_u32(&mut self) -> Result<u32, ConnectorError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn take_u64(&mut self) -> Result<u64, ConnectorError> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn take_blob(&mut self) -> Result<Bytes, ConnectorError> {
        let length = self.take_u32()? as usize;
        if length > REWRITE_ARTIFACT_MAX_BYTES {
            return Err(exhausted("Iceberg rewrite attempt blob exceeds 64 MiB"));
        }
        Ok(Bytes::copy_from_slice(self.take_exact(length)?))
    }

    fn take_binding_key(&mut self) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
        let length = self.take_u16()? as usize;
        let instance = std::str::from_utf8(self.take_exact(length)?)
            .map_err(|_| invalid("Iceberg rewrite instance ID is not UTF-8"))?;
        Ok(ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse(instance)?,
            incarnation: ConnectorInstanceIncarnation::from_bytes(self.take_array()?),
        })
    }

    fn take_execution_id(
        &mut self,
    ) -> Result<novarocks_spi::connector::ConnectorWriteExecutionId, ConnectorError> {
        Ok(novarocks_spi::connector::ConnectorWriteExecutionId::new(
            self.take_array()?,
            self.take_u64()?,
        ))
    }

    fn take_writer(&mut self) -> Result<ConnectorWriterIdentity, ConnectorError> {
        Ok(ConnectorWriterIdentity::new(
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(self.take_array()?),
            ConnectorWriteCohortId::from_bytes(self.take_array()?),
            self.take_execution_id()?,
            self.take_array()?,
            i32::from_be_bytes(self.take_array()?),
            i32::from_be_bytes(self.take_array()?),
            self.take_u32()?,
            self.take_binding_key()?,
        ))
    }
}

fn validate_frozen_rewrite_table(
    artifact: &IcebergFrozenRewriteArtifactV1,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<(), ConnectorError> {
    if metadata.uuid().to_string() != artifact.table_uuid
        || metadata.current_snapshot_id() != artifact.base_snapshot_id
        || metadata.current_schema_id() != artifact.schema_id
        || metadata.default_partition_spec_id() != artifact.default_spec_id
    {
        return Err(invalid(
            "Iceberg distributed rewrite frozen table state is no longer current",
        ));
    }
    Ok(())
}

fn write_artifact_file(
    runtime: &IcebergControlRuntime,
    file_io: &crate::iceberg::io::FileIO,
    location: &str,
    bytes: Bytes,
) -> Result<(), ConnectorError> {
    let output = file_io
        .new_output(location)
        .map_err(|error| unavailable(format!("create Iceberg rewrite artifact: {error}")))?;
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { output.write(bytes).await })
        .map_err(unavailable)?
        .map_err(|error| unavailable(format!("persist Iceberg rewrite artifact: {error}")))
}

fn read_artifact_file(
    runtime: &IcebergControlRuntime,
    file_io: &crate::iceberg::io::FileIO,
    location: &str,
    max_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let input = file_io
        .new_input(location)
        .map_err(|error| unavailable(format!("open Iceberg rewrite artifact: {error}")))?;
    let bytes = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { input.read().await })
        .map_err(unavailable)?
        .map_err(|error| unavailable(format!("read Iceberg rewrite artifact: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(exhausted(format!(
            "Iceberg rewrite artifact exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

fn artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact.v1\0");
    digest_bytes(&mut hash, bytes);
    hash.finalize().into()
}

fn artifact_part_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact-part.v1\0");
    digest_bytes(&mut hash, bytes);
    hash.finalize().into()
}

fn rewrite_state_digest(
    table_uuid: &[u8],
    metadata_location: &str,
    base_snapshot_id: Option<i64>,
    schema_id: i32,
    default_spec_id: i32,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(STATE_DOMAIN);
    digest_bytes(&mut hash, table_uuid);
    digest_bytes(&mut hash, metadata_location.as_bytes());
    hash.update(base_snapshot_id.unwrap_or(-1).to_be_bytes());
    hash.update(schema_id.to_be_bytes());
    hash.update(default_spec_id.to_be_bytes());
    hash.finalize().into()
}

fn data_group_digest(
    spec_id: Option<i32>,
    partition_key: Option<&str>,
    files: &[IcebergDataFileInfo],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"data\0");
    hash.update(spec_id.unwrap_or(-1).to_be_bytes());
    digest_bytes(&mut hash, partition_key.unwrap_or_default().as_bytes());
    for file in files {
        digest_bytes(&mut hash, file.path.as_bytes());
        hash.update(file.size.to_be_bytes());
        hash.update(file.row_count.unwrap_or(-1).to_be_bytes());
        for delete in &file.delete_files {
            digest_bytes(&mut hash, delete.path.as_bytes());
        }
    }
    hash.finalize().into()
}

fn position_group_digest(data_path: &str, delete_paths: &[String]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"position-delete\0");
    digest_bytes(&mut hash, data_path.as_bytes());
    for path in delete_paths {
        digest_bytes(&mut hash, path.as_bytes());
    }
    hash.finalize().into()
}

fn arrow_schema_digest(schema: &SchemaRef) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.arrow-schema.v1\0");
    digest_bytes(&mut hash, format!("{schema:?}").as_bytes());
    hash.finalize().into()
}

fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
    hex::decode(value)
        .map_err(|error| invalid(format!("decode Iceberg rewrite digest: {error}")))?
        .try_into()
        .map_err(|_| invalid("Iceberg rewrite digest has invalid length"))
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode Iceberg rewrite JSON: {error}")))?;
    let mut output = Vec::new();
    write_canonical_json(&value, &mut output)?;
    Ok(Bytes::from(output))
}

fn decode_canonical_json<T>(payload: &[u8], label: &str) -> Result<T, ConnectorError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let decoded = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode {label}: {error}")))?;
    if canonical_json(&decoded)?.as_ref() != payload {
        return Err(invalid(format!("{label} is not canonical JSON v1")));
    }
    Ok(decoded)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => serde_json::to_writer(output, value)
            .map_err(|error| internal(format!("encode Iceberg rewrite string: {error}")))?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut fields = values.iter().collect::<Vec<_>>();
            fields.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in fields.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|error| {
                    internal(format!("encode Iceberg rewrite object key: {error}"))
                })?;
                output.push(b':');
                write_canonical_json(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg distributed rewrite request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg distributed rewrite deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan_model::IcebergDeleteFileInfo;

    #[test]
    fn data_groups_are_partition_owned_and_stably_sorted() {
        let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
        a.partition_spec_id = Some(2);
        a.partition_key = Some("{\"day\":1}".to_string());
        let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 20, 2);
        b.partition_spec_id = Some(2);
        b.partition_key = Some("{\"day\":1}".to_string());
        let mut c = IcebergDataFileInfo::for_test("s3://bucket/c.parquet", 30, 3);
        c.partition_spec_id = Some(2);
        c.partition_key = Some("{\"day\":2}".to_string());
        let groups = group_data_files(vec![c, b, a], false, None).expect("groups");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.data_files.len())
                .sum::<usize>(),
            3
        );
        assert!(groups.iter().all(|group| {
            group
                .data_files
                .windows(2)
                .all(|pair| pair[0].path < pair[1].path)
        }));
    }

    #[test]
    fn shared_delete_file_has_one_canonical_owner() {
        let delete = IcebergDeleteFileInfo {
            path: "s3://bucket/shared-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(8),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(3),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: vec!["id".to_string()],
            equality_field_ids: vec![1],
        };
        let mut a = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
        a.partition_key = Some("a".to_string());
        a.delete_files.push(delete.clone());
        let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 10, 1);
        b.partition_key = Some("b".to_string());
        b.delete_files.push(delete);
        let groups = group_data_files(vec![b, a], false, None).expect("groups");
        assert_eq!(
            groups
                .iter()
                .flat_map(|group| &group.owned_data_delete_files)
                .filter(|path| path.as_str() == "s3://bucket/shared-delete.parquet")
                .count(),
            1
        );
    }

    #[test]
    fn orphan_live_delete_is_assigned_to_a_canonical_cohort() {
        let data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let mut groups = group_data_files(vec![data], false, None).expect("groups");
        assign_unattached_delete_owners(
            &mut groups,
            &BTreeSet::from(["s3://bucket/orphan.puffin".to_string()]),
        )
        .expect("owner");
        assert_eq!(
            groups[0].owned_data_delete_files,
            vec!["s3://bucket/orphan.puffin".to_string()]
        );
    }

    #[test]
    fn position_rewrite_rejects_v2_parquet_deletes() {
        let mut data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        data.delete_files.push(IcebergDeleteFileInfo {
            path: "s3://bucket/delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(8),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(3),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        });
        assert!(group_data_files(vec![data], true, Some(1)).is_err());
    }

    #[test]
    fn canonical_json_and_artifact_digest_are_deterministic() {
        let mut first = HashMap::new();
        first.insert("z".to_string(), vec![1_u8]);
        first.insert("a".to_string(), vec![2_u8]);
        let mut second = HashMap::new();
        second.insert("a".to_string(), vec![2_u8]);
        second.insert("z".to_string(), vec![1_u8]);
        assert_eq!(
            canonical_json(&first).unwrap(),
            canonical_json(&second).unwrap()
        );
        assert_eq!(artifact_digest(b"same"), artifact_digest(b"same"));
        assert_ne!(artifact_digest(b"same"), artifact_digest(b"different"));
    }

    #[test]
    fn frozen_source_round_trips_bounded_provider_facts() {
        let owner = ConnectorInstanceId::parse("iceberg.rewrite").unwrap();
        let original = ConnectorTableHandle::try_new(
            owner,
            Bytes::from_static(
                br#"{"explicit_files":null,"hidden_columns":[],"logical_type_columns":{},"metadata_columns":[],"metadata_table_type":null,"namespace":"ns","prepared_files":[],"table":"t","table_info":null}"#,
            ),
        )
        .unwrap();
        let handle = frozen_rewrite_source_handle(
            &original,
            REWRITE_DATA_FILES_KIND,
            IcebergRewriteGroupPayloadV1 {
                version: 1,
                group_digest_hex: "01".repeat(32),
                artifact_digest_hex: "02".repeat(32),
                artifact_location: "file:///warehouse/artifact".to_string(),
            },
        )
        .unwrap();
        let (namespace, table, kind, group) =
            decode_frozen_rewrite_source(handle.payload()).unwrap();
        assert_eq!(
            (namespace.as_str(), table.as_str(), kind.as_str()),
            ("ns", "t", REWRITE_DATA_FILES_KIND)
        );
        assert_eq!(group.group_digest_hex, "01".repeat(32));
    }

    #[test]
    fn attempt_artifact_round_trips_exact_writer_reports() {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.rewrite").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
        };
        let operation = novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([7; 16]);
        let cohort = ConnectorWriteCohortId::from_bytes([8; 32]);
        let execution = novarocks_spi::connector::ConnectorWriteExecutionId::new([6; 16], 3);
        let report = ConnectorStagedReport::try_new(
            ConnectorWriterIdentity::new(
                operation,
                cohort,
                execution,
                [5; 16],
                4,
                2,
                1,
                owner.clone(),
            ),
            1,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary {
                input_rows: 11,
                staged_bytes: 17,
                artifact_count: 1,
            },
            Bytes::from_static(b"opaque-report"),
        )
        .unwrap();
        let completion = ConnectorWriteAttemptCompletion::try_new(
            owner,
            operation,
            cohort,
            execution,
            [4; 32],
            vec![report],
            Bytes::from_static(b"opaque-control"),
        )
        .unwrap();
        let decoded = decode_attempt_artifact(&encode_attempt_artifact(&completion).unwrap())
            .expect("round trip");
        assert_eq!(decoded, completion);
    }

    #[test]
    fn attempt_artifact_rejects_trailing_or_truncated_bytes() {
        let mut artifact = ATTEMPT_ARTIFACT_MAGIC.to_vec();
        artifact.extend_from_slice(&ATTEMPT_ARTIFACT_VERSION.to_be_bytes());
        artifact.push(0);
        assert!(decode_attempt_artifact(&artifact).is_err());
    }
}

/// Tiny provider-private hex codec avoids introducing a crate dependency for
/// identities that are already bounded to 16 or 32 bytes.
mod hex {
    use std::fmt;

    #[derive(Debug)]
    pub struct DecodeError(&'static str);

    impl fmt::Display for DecodeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    pub fn encode(value: impl AsRef<[u8]>) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let value = value.as_ref();
        let mut output = String::with_capacity(value.len() * 2);
        for byte in value {
            output.push(DIGITS[(byte >> 4) as usize] as char);
            output.push(DIGITS[(byte & 0x0f) as usize] as char);
        }
        output
    }

    pub fn decode(value: &str) -> Result<Vec<u8>, DecodeError> {
        if !value.len().is_multiple_of(2) {
            return Err(DecodeError("hex input has odd length"));
        }
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| Ok((nibble(pair[0])? << 4) | nibble(pair[1])?))
            .collect()
    }

    fn nibble(value: u8) -> Result<u8, DecodeError> {
        match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            b'A'..=b'F' => Ok(value - b'A' + 10),
            _ => Err(DecodeError("hex input contains a non-hex digit")),
        }
    }
}
