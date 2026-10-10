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

//! Exact-generation Iceberg distributed-rewrite control.
//!
//! Frozen file ownership, durable attempt artifacts, and write activation all
//! live in the provider generation that owns the catalog client.  No Core
//! registry, process-global runtime, or current-generation lookup participates
//! in this capability.

use crate::commit::model::EntryIdentity;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use novarocks_spi::connector::{
    ConnectorDistributedRewrite, ConnectorDistributedRewriteCohortPlan,
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteReceipt, ConnectorDistributedRewriteReceiptSummary, ConnectorError,
    ConnectorErrorKind, ConnectorFrozenRewriteGroup, ConnectorInstanceDescriptor,
    ConnectorOperationControl, ConnectorPinnedFileSet, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorRewriteCohortRead, ConnectorTableHandle,
    ConnectorWriteBaseVersion, ConnectorWriteCohortId, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
    ConnectorWritePreparation, ConnectorWriteReceipt, ConnectorWriteTargetRef,
    ProviderBindingEpoch, REWRITE_POSITION_DELETES_KIND,
};

use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::manifest::{DataFileWithStats, data_file_with_stats_to_iceberg_data_file_info};
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
};

pub(crate) const ARTIFACT_VERSION: u16 = 3;
pub(crate) const GROUP_PAYLOAD_VERSION: u16 = 2;
pub(crate) const REWRITE_ARTIFACT_MAX_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_GROUPS: usize = 4096;
pub(crate) const REWRITE_ARTIFACT_MAX_PARTS: usize = 64;
pub(crate) const REWRITE_ARTIFACT_MAX_PART_BYTES: usize = 1024 * 1024;
pub(crate) const REWRITE_ARTIFACT_MAX_ROOT_BYTES: usize = 64 * 1024;

const GROUP_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.group.v2\0";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.state.v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewritePlanPayloadV1 {
    version: u16,
    artifact_digest_hex: String,
    artifact_location: String,
    target_ref: String,
}

/// Complete distributed-rewrite capability for one provider generation.
pub struct IcebergDistributedRewriteControl {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
    provider: Arc<IcebergMetadata>,
    plans: Mutex<
        HashMap<
            novarocks_spi::connector::ConnectorWriteOperationId,
            ConnectorDistributedRewritePlan,
        >,
    >,
}

impl IcebergDistributedRewriteControl {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
        provider: Arc<IcebergMetadata>,
    ) -> Result<Self, ConnectorError> {
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        if provider.descriptor() != &descriptor
            || provider.incarnation() != incarnation
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
            plans: Mutex::new(HashMap::new()),
        })
    }

    fn build_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
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
            .load_table_for_request(&target.namespace, &target.table, &request.context)
            .map_err(unavailable)?
            .into_table();
        let metadata = table.metadata();
        let base_snapshot_id = metadata.current_snapshot_id();
        let table_for_files = table.clone();
        let read_control = request.context.clone();
        let files_result = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::manifest::extract_data_files_with_stats_with_control(
                    &table_for_files,
                    Some(&read_control as &dyn ConnectorOperationControl),
                )
                .await
            });
        validate_context(&request.context)?;
        let files = files_result.map_err(unavailable)?.map_err(unavailable)?;
        let groups = match request.operation() {
            ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                let live_delete_paths =
                    live_delete_file_paths(&self.runtime, &table, &request.context)?;
                plan_data_file_groups(files, &live_delete_paths)?
            }
            ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                rewrite_all,
                min_input_files,
                ..
            } => {
                let groups = plan_position_delete_groups(files, *rewrite_all, *min_input_files)?;
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
            &artifact,
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
            // Planning proves the frozen inputs, not how many writer files a
            // future execution will publish. `None` is deliberately distinct
            // from a proven no-op's `Some(0)`.
            expected_output_files: None,
        };
        let payload = canonical_json(&IcebergRewritePlanPayloadV1 {
            version: 1,
            artifact_digest_hex: hex::encode(manifest_digest),
            artifact_location: artifact_location.clone(),
            target_ref: "main".to_string(),
        })?;
        ConnectorDistributedRewritePlan::try_new(
            request,
            state_digest,
            manifest_digest,
            summary,
            payload,
            cohorts,
        )
    }
}

impl ConnectorDistributedRewrite for IcebergDistributedRewriteControl {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
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
        let target = self.provider.table_payload(request.operation().table())?;
        if target.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg distributed rewrite requires a base table handle",
            ));
        }
        let operation = match request.operation() {
            ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                CatalogOperation::RewriteDataFiles
            }
            ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
                CatalogOperation::RewritePositionDeletes
            }
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                CatalogTableName::new(target.namespace, target.table),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        if let Some(existing) = self
            .plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&request.operation_id())
            .cloned()
        {
            return if existing.request_digest() == request.request_digest() {
                Ok(existing)
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
            Some(existing) if existing.request_digest() == request.request_digest() => {
                Ok(existing.clone())
            }
            Some(_) => Err(invalid(
                "Iceberg distributed rewrite operation conflicts with cached plan",
            )),
            None => {
                plans.insert(request.operation_id(), planned.clone());
                Ok(planned)
            }
        }
    }

    /// The write session seals a rewrite's branches from the same frozen
    /// groups this plan names, so a rewrite no longer activates a separate
    /// writer set. The trait still declares this entry; it has no caller.
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
        let output_facts =
            crate::write_codec::decode_write_receipt_output_facts(receipt).map_err(invalid)?;
        let (output_data_files, output_delete_files, output_rows) = match output_facts {
            Some(facts) => (
                Some(facts.data_files),
                Some(
                    facts
                        .position_delete_files
                        .checked_add(facts.deletion_vectors)
                        .and_then(|count| count.checked_add(facts.equality_delete_files))
                        .ok_or_else(|| {
                            invalid("Iceberg rewrite output delete file count overflow")
                        })?,
                ),
                Some(facts.data_rows),
            ),
            None => (None, None, None),
        };
        ConnectorDistributedRewriteReceipt::try_new(
            ConnectorDistributedRewriteReceiptSummary {
                input_data_files: plan.summary().input_data_files,
                input_delete_files: plan.summary().input_delete_files,
                output_data_files,
                output_delete_files,
                output_rows,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteGroupV1 {
    pub group_digest_hex: String,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub data_files: Vec<IcebergDataFileInfo>,
    pub selected_position_delete_files: Vec<EntryIdentity>,
    pub owned_data_delete_files: Vec<EntryIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergRewriteGroupPayloadV1 {
    pub version: u16,
    pub group_digest_hex: String,
    pub artifact_digest_hex: String,
    pub artifact_location: String,
}

/// One exact group together with the immutable artifact that authenticated it.
///
/// The group alone is insufficient at a typed TableExecute boundary: the root
/// owns the table identity and base-generation fences that prove which table
/// state the group was cut from. Keep those facts together until every caller
/// has validated them against its pinned relation.
#[derive(Clone, Debug)]
pub(crate) struct IcebergLoadedFrozenRewriteGroupV1 {
    pub artifact: IcebergFrozenRewriteArtifactV1,
    pub group: IcebergFrozenRewriteGroupV1,
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

pub(crate) fn load_frozen_rewrite_group(
    runtime: &IcebergMetadataContext,
    file_io: &crate::iceberg::io::FileIO,
    payload: &IcebergRewriteGroupPayloadV1,
) -> Result<IcebergLoadedFrozenRewriteGroupV1, ConnectorError> {
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
    let group = logical
        .groups
        .iter()
        .find(|group| group.group_digest_hex == payload.group_digest_hex)
        .cloned()
        .ok_or_else(|| invalid("Iceberg rewrite artifact has no requested group"))?;
    Ok(IcebergLoadedFrozenRewriteGroupV1 {
        artifact: logical,
        group,
    })
}

fn write_frozen_artifact(
    runtime: &IcebergMetadataContext,
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
    live_delete_paths: &BTreeSet<EntryIdentity>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    let mut groups = group_data_files(files, false, None)?;
    assign_unattached_delete_owners(&mut groups, live_delete_paths)?;
    refresh_data_group_digests(&mut groups)?;
    bounded_groups(groups)
}

pub(crate) fn plan_position_delete_groups(
    files: Vec<DataFileWithStats>,
    rewrite_all: bool,
    min_input_files: Option<u32>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    group_data_files(
        files
            .into_iter()
            .map(data_file_with_stats_to_iceberg_data_file_info)
            .collect(),
        rewrite_all,
        Some(position_delete_min_input_files(min_input_files)),
    )
}

/// How many attached Puffin deletion vectors make a data file worth repacking.
///
/// The default lives here and nowhere else: planning and the write session both
/// cut the same groups, and a default resolved twice could drift and leave the
/// two disagreeing about how many branches the rewrite has.
fn position_delete_min_input_files(min_input_files: Option<u32>) -> usize {
    min_input_files.unwrap_or(DEFAULT_POSITION_DELETE_MIN_INPUT_FILES) as usize
}

/// A data file with a single deletion vector is already packed; two is the
/// smallest input a repack can actually shrink.
const DEFAULT_POSITION_DELETE_MIN_INPUT_FILES: u32 = 2;

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
                return Err(invalid(
                    "V2 Parquet position delete rewrite is not supported",
                ));
            }
            let mut selected = file
                .delete_files
                .iter()
                .filter(|delete| {
                    delete.file_content == IcebergDeleteFileContent::Position
                        && delete.file_format == IcebergDeleteFileFormat::Puffin
                })
                .map(delete_entry_identity)
                .collect::<Result<Vec<_>, _>>()?;
            selected.sort();
            if selected.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(corrupt(
                    "Iceberg rewrite data file repeats a logical delete entry",
                ));
            }
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
            Ok(IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(data_group_digest(
                    partition_spec_id,
                    partition_key.as_deref(),
                    &data_files,
                    &[],
                )?),
                partition_spec_id,
                partition_key,
                data_files,
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    assign_attached_delete_owners(&mut groups)?;
    refresh_data_group_digests(&mut groups)?;
    bounded_groups(groups)
}

fn refresh_data_group_digests(
    groups: &mut [IcebergFrozenRewriteGroupV1],
) -> Result<(), ConnectorError> {
    for group in groups {
        group.group_digest_hex = hex::encode(data_group_digest(
            group.partition_spec_id,
            group.partition_key.as_deref(),
            &group.data_files,
            &group.owned_data_delete_files,
        )?);
    }
    Ok(())
}

fn assign_attached_delete_owners(
    groups: &mut [IcebergFrozenRewriteGroupV1],
) -> Result<(), ConnectorError> {
    let mut owners = BTreeMap::<EntryIdentity, usize>::new();
    for (index, group) in groups.iter().enumerate() {
        for delete in group.data_files.iter().flat_map(|file| &file.delete_files) {
            let identity = delete_entry_identity(delete)?;
            match owners.get(&identity) {
                Some(existing) if groups[*existing].group_digest_hex <= group.group_digest_hex => {}
                _ => {
                    owners.insert(identity, index);
                }
            }
        }
    }
    for (identity, owner) in owners {
        groups[owner].owned_data_delete_files.push(identity);
    }
    for group in groups {
        group.owned_data_delete_files.sort();
        group.owned_data_delete_files.dedup();
    }
    Ok(())
}

fn assign_unattached_delete_owners(
    groups: &mut [IcebergFrozenRewriteGroupV1],
    live: &BTreeSet<EntryIdentity>,
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
    owner: &ConnectorProviderBindingKey,
    request: &ConnectorDistributedRewritePlanningRequest,
    artifact_digest: [u8; 32],
    artifact_location: &str,
    artifact: &IcebergFrozenRewriteArtifactV1,
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
    artifact
        .groups
        .iter()
        .map(|group| {
            let group_digest = decode_digest(&group.group_digest_hex)?;
            let cohort_id = ConnectorWriteCohortId::derive(
                request.operation_id(),
                b"iceberg-distributed-rewrite-group",
                group_digest,
            )?;
            let preparation = rewrite_preparation(
                owner,
                request,
                intent,
                scan_schema.as_ref(),
                row_lineage,
                metadata,
            )?;
            // A data-file rewrite reads table rows, and its commit replaces
            // exactly the group's files, so the read is pinned to that same
            // set. Rewriting position deletes reads delete artifacts rather
            // than rows, so it has no data file set to pin and names its
            // frozen group instead -- the same group its commit resolves the
            // replaced Puffin files from.
            let read = match request.operation() {
                ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                    let base_snapshot_id = artifact.base_snapshot_id.ok_or_else(|| {
                        invalid("Iceberg rewrite group has no base snapshot to pin its files to")
                    })?;
                    ConnectorRewriteCohortRead::PinnedFileSet(ConnectorPinnedFileSet::try_new(
                        &artifact.namespace,
                        &artifact.table,
                        base_snapshot_id,
                        group.data_files.iter().map(|file| file.path.as_str()),
                    )?)
                }
                ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
                    ConnectorRewriteCohortRead::DeleteArtifactGroup(
                        ConnectorFrozenRewriteGroup::try_new(
                            &artifact.namespace,
                            &artifact.table,
                            artifact_location,
                            artifact_digest,
                        )?,
                    )
                }
            };
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                read,
                scan_schema.clone(),
                arrow_schema_digest(&scan_schema),
                preparation,
                group_digest,
            )
        })
        .collect()
}

fn rewrite_preparation(
    owner: &ConnectorProviderBindingKey,
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
    owner: &ConnectorProviderBindingKey,
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
    owner: &ConnectorProviderBindingKey,
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
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
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

/// Resolve one frozen rewrite group back to the delete artifacts it names, as
/// the splits that re-encode them.
///
/// The group is the authority on which artifacts this procedure instance
/// rewrites, and the commit replaces exactly that set. Nothing here reselects:
/// the artifact is read back by location and content digest, the named group is
/// looked up inside it, and the only judgement made is whether the pinned
/// snapshot still holds every artifact the group named. It if does not, the
/// cohort's commit could no longer replace what this read would consume, so the
/// read fails rather than producing a delete file for artifacts somebody else
/// already replaced.
pub(crate) fn plan_rewrite_position_delete_splits(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
    snapshot_id: i64,
    group_payload: &IcebergRewriteGroupPayloadV1,
    context: &ConnectorRequestContext,
) -> Result<(IcebergDataFileInfo, Vec<IcebergDeleteFileInfo>), ConnectorError> {
    validate_context(context)?;
    let loaded = load_frozen_rewrite_group(runtime, table.file_io(), group_payload)?;
    validate_context(context)?;
    // The table can advance after the TableExecute relation was frozen. Its
    // later current snapshot must never become a substitute generation for the
    // immutable rewrite artifact, so fail before split dispatch if that fence
    // no longer holds.
    validate_frozen_rewrite_table(&loaded.artifact, table.metadata())?;
    let live = live_delete_file_paths_at_with_control(runtime, table, snapshot_id, Some(context))?;
    validate_context(context)?;
    select_rewrite_position_delete_artifacts(&loaded.group, &live, snapshot_id)
}

/// Pick out exactly the delete artifacts one frozen group names.
///
/// Everything this decides is a comparison against the group. It selects no
/// artifact the group did not name -- a data file may carry vectors this
/// procedure instance does not own -- and refuses rather than shrinking when an
/// artifact the group named is missing from the file or has stopped being live.
/// Both refusals matter for the same reason: the cohort's commit replaces
/// exactly the named set, so a read over a different set would leave the
/// relation describing deletes that no artifact holds.
fn select_rewrite_position_delete_artifacts(
    group: &IcebergFrozenRewriteGroupV1,
    live_delete_paths: &BTreeSet<EntryIdentity>,
    snapshot_id: i64,
) -> Result<(IcebergDataFileInfo, Vec<IcebergDeleteFileInfo>), ConnectorError> {
    // A position-delete group is cut one data file at a time, because the
    // rewritten artifact addresses exactly one data file's rows.
    let [data_file] = group.data_files.as_slice() else {
        return Err(invalid(
            "Iceberg rewrite position delete group must name exactly one data file",
        ));
    };
    if group.selected_position_delete_files.is_empty() {
        return Err(invalid(
            "Iceberg rewrite position delete group names no delete artifact",
        ));
    }
    let named = group
        .selected_position_delete_files
        .iter()
        .collect::<BTreeSet<_>>();
    let selected = data_file
        .delete_files
        .iter()
        .map(|delete| Ok((delete_entry_identity(delete)?, delete)))
        .collect::<Result<Vec<_>, ConnectorError>>()?
        .into_iter()
        .filter(|(identity, _)| named.contains(identity))
        .map(|(_, delete)| delete.clone())
        .collect::<Vec<_>>();
    if selected.len() != named.len() {
        return Err(invalid(
            "Iceberg rewrite position delete group names a delete artifact its data file does not carry",
        ));
    }
    if let Some(missing) = named
        .iter()
        .find(|identity| !live_delete_paths.contains(*identity))
    {
        return Err(corrupt(format!(
            "Iceberg rewrite position delete artifact {missing:?} is no longer live at snapshot {snapshot_id}"
        )));
    }
    Ok((data_file.clone(), selected))
}

fn live_delete_file_paths(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
    context: &ConnectorRequestContext,
) -> Result<BTreeSet<EntryIdentity>, ConnectorError> {
    let Some(snapshot) = table.metadata().current_snapshot().cloned() else {
        return Ok(BTreeSet::new());
    };
    live_delete_file_paths_of(runtime, table, snapshot, Some(context))
}

/// The delete files alive at one exact snapshot of a relation.
pub(crate) fn live_delete_file_paths_at(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
    snapshot_id: i64,
) -> Result<BTreeSet<EntryIdentity>, ConnectorError> {
    live_delete_file_paths_at_with_control(runtime, table, snapshot_id, None)
}

fn live_delete_file_paths_at_with_control(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
    snapshot_id: i64,
    context: Option<&ConnectorRequestContext>,
) -> Result<BTreeSet<EntryIdentity>, ConnectorError> {
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .cloned()
        .ok_or_else(|| {
            corrupt(format!(
                "Iceberg relation no longer holds snapshot {snapshot_id}, which a rewrite was frozen at"
            ))
        })?;
    live_delete_file_paths_of(runtime, table, snapshot, context)
}

fn live_delete_file_paths_of(
    runtime: &IcebergMetadataContext,
    table: &crate::iceberg::table::Table,
    snapshot: std::sync::Arc<crate::iceberg::spec::Snapshot>,
    context: Option<&ConnectorRequestContext>,
) -> Result<BTreeSet<EntryIdentity>, ConnectorError> {
    if let Some(context) = context {
        validate_context(context)?;
    }
    let file_io = table.file_io().clone();
    let metadata = table.metadata().clone();
    let control = context.cloned();
    let result = runtime.resources().catalog_runtime().block_on(async move {
        let check_active = || -> Result<(), String> {
            if let Some(control) = &control {
                ConnectorOperationControl::check_active(control)
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        };
        check_active()?;
        let manifest_list = snapshot
            .load_manifest_list(&file_io, &metadata)
            .await
            .map_err(|error| format!("load Iceberg rewrite manifest list: {error}"))?;
        check_active()?;
        let mut paths = BTreeSet::new();
        for manifest_file in manifest_list.entries() {
            check_active()?;
            if manifest_file.content != crate::iceberg::spec::ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(&file_io)
                .await
                .map_err(|error| format!("load Iceberg rewrite delete manifest: {error}"))?;
            check_active()?;
            for entry in manifest.entries() {
                check_active()?;
                if entry.is_alive() {
                    let identity = EntryIdentity::try_from(entry.data_file())
                        .map_err(|error| error.to_string())?;
                    if !paths.insert(identity) {
                        return Err(
                            "Iceberg rewrite snapshot repeats a logical delete entry".to_string()
                        );
                    }
                }
            }
        }
        check_active()?;
        Ok::<_, String>(paths)
    });
    if let Some(context) = context {
        validate_context(context)?;
    }
    result.map_err(unavailable)?.map_err(unavailable)
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
    runtime: &IcebergMetadataContext,
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
    runtime: &IcebergMetadataContext,
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
    owned_deletes: &[EntryIdentity],
) -> Result<[u8; 32], ConnectorError> {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"data\0");
    hash.update(spec_id.unwrap_or(-1).to_be_bytes());
    digest_bytes(&mut hash, partition_key.unwrap_or_default().as_bytes());
    for file in files {
        digest_bytes(&mut hash, file.path.as_bytes());
        hash.update(file.size.to_be_bytes());
        hash.update(file.row_count.unwrap_or(-1).to_be_bytes());
        let mut deletes = file
            .delete_files
            .iter()
            .map(delete_entry_identity)
            .collect::<Result<Vec<_>, _>>()?;
        deletes.sort();
        if deletes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corrupt(
                "Iceberg rewrite data file repeats a logical delete entry",
            ));
        }
        for identity in &deletes {
            digest_entry_identity(&mut hash, identity);
        }
    }
    // Include the assigned retirement set, including deletes with no attached live data.
    hash.update(b"owned-delete-entries\0");
    hash.update((owned_deletes.len() as u64).to_be_bytes());
    for identity in owned_deletes {
        identity
            .validate()
            .map_err(|error| corrupt(error.to_string()))?;
        digest_entry_identity(&mut hash, identity);
    }
    Ok(hash.finalize().into())
}

pub(crate) fn delete_entry_identity(
    delete: &IcebergDeleteFileInfo,
) -> Result<EntryIdentity, ConnectorError> {
    let identity = if delete.file_content == IcebergDeleteFileContent::Position
        && delete.file_format == IcebergDeleteFileFormat::Puffin
    {
        EntryIdentity::DeletionVector {
            path: delete.path.clone(),
            offset: delete
                .content_offset
                .ok_or_else(|| corrupt("Frozen DV is missing content_offset"))?,
            length: delete
                .content_size_in_bytes
                .ok_or_else(|| corrupt("Frozen DV is missing content_size_in_bytes"))?,
            referenced_data_file: delete
                .referenced_data_file
                .clone()
                .ok_or_else(|| corrupt("Frozen DV is missing referenced data file"))?,
        }
    } else {
        EntryIdentity::DeleteFile {
            path: delete.path.clone(),
        }
    };
    identity
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    Ok(identity)
}

fn digest_entry_identity(hash: &mut Sha256, identity: &EntryIdentity) {
    match identity {
        EntryIdentity::DataFile { path } => {
            hash.update([0]);
            digest_bytes(hash, path.as_bytes());
        }
        EntryIdentity::DeleteFile { path } => {
            hash.update([1]);
            digest_bytes(hash, path.as_bytes());
        }
        EntryIdentity::DeletionVector {
            path,
            offset,
            length,
            referenced_data_file,
        } => {
            hash.update([2]);
            digest_bytes(hash, path.as_bytes());
            hash.update(offset.to_be_bytes());
            hash.update(length.to_be_bytes());
            digest_bytes(hash, referenced_data_file.as_bytes());
        }
    }
}

fn position_group_digest(data_path: &str, delete_paths: &[EntryIdentity]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(GROUP_DOMAIN);
    hash.update(b"position-delete\0");
    digest_bytes(&mut hash, data_path.as_bytes());
    for path in delete_paths {
        digest_entry_identity(&mut hash, path);
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
    if context.is_cancelled() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dv(path: &str) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            record_count: Some(1),
            partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Puffin,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(1024),
            content_offset: Some(4),
            content_size_in_bytes: Some(32),
            sequence_number: Some(7),
            partition_spec_id: Some(0),
            partition_key: None,
            referenced_data_file: Some("s3://bucket/data-1.parquet".to_string()),
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        }
    }

    /// One position-delete group whose data file carries `carried` vectors and
    /// whose frozen selection names `named`.
    fn position_delete_group(carried: &[&str], named: &[&str]) -> IcebergFrozenRewriteGroupV1 {
        let mut data_file = IcebergDataFileInfo::for_test("s3://bucket/data-1.parquet", 64, 4);
        data_file.delete_files = carried.iter().map(|path| dv(path)).collect();
        IcebergFrozenRewriteGroupV1 {
            group_digest_hex: hex::encode([1_u8; 32]),
            partition_spec_id: Some(0),
            partition_key: None,
            data_files: vec![data_file],
            selected_position_delete_files: named
                .iter()
                .map(|path| delete_entry_identity(&dv(path)).unwrap())
                .collect(),
            owned_data_delete_files: Vec::new(),
        }
    }

    fn live(paths: &[&str]) -> BTreeSet<EntryIdentity> {
        paths
            .iter()
            .map(|path| delete_entry_identity(&dv(path)).unwrap())
            .collect()
    }

    /// The group is the authority on what this procedure instance rewrites, and
    /// its commit replaces exactly that set. A vector the data file carries but
    /// the group did not name belongs to some other cohort -- reading it here
    /// would repack deletes this commit never removes.
    #[test]
    fn a_rewrite_reads_exactly_the_delete_artifacts_its_group_names() {
        let group = position_delete_group(
            &[
                "s3://bucket/dv-a.puffin",
                "s3://bucket/dv-b.puffin",
                "s3://bucket/dv-foreign.puffin",
            ],
            &["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"],
        );
        let (data_file, selected) = select_rewrite_position_delete_artifacts(
            &group,
            &live(&[
                "s3://bucket/dv-a.puffin",
                "s3://bucket/dv-b.puffin",
                "s3://bucket/dv-foreign.puffin",
            ]),
            42,
        )
        .expect("the group names two live artifacts its data file carries");
        assert_eq!(data_file.path, "s3://bucket/data-1.parquet");
        assert_eq!(
            selected
                .iter()
                .map(|delete| delete.path.as_str())
                .collect::<Vec<_>>(),
            ["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"]
        );

        // And an artifact the group names that its data file does not carry is
        // a refusal, never a shorter read.
        let error = select_rewrite_position_delete_artifacts(
            &position_delete_group(
                &["s3://bucket/dv-a.puffin"],
                &["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"],
            ),
            &live(&["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"]),
            42,
        )
        .expect_err("a named artifact the data file does not carry");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    /// A named artifact the pinned snapshot no longer holds means somebody else
    /// already replaced it. Rewriting it anyway would commit a delete file for
    /// artifacts this commit cannot remove, so the read fails closed.
    #[test]
    fn a_rewrite_refuses_a_delete_artifact_the_snapshot_no_longer_holds() {
        let group = position_delete_group(
            &["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"],
            &["s3://bucket/dv-a.puffin", "s3://bucket/dv-b.puffin"],
        );
        let error = select_rewrite_position_delete_artifacts(
            &group,
            &live(&["s3://bucket/dv-a.puffin"]),
            42,
        )
        .expect_err("dv-b is no longer live at the pinned snapshot");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            error.message().contains("s3://bucket/dv-b.puffin"),
            "the refusal must name the missing artifact: {}",
            error.message()
        );
        assert!(
            error.message().contains("42"),
            "the refusal must name the pinned snapshot: {}",
            error.message()
        );
    }

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
    fn shared_puffin_vectors_keep_distinct_identity_and_digest() {
        let path = "s3://bucket/shared.puffin";
        let first = dv(path);
        let mut second = first.clone();
        second.content_offset = Some(first.content_offset.unwrap() + 128);
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
        file.delete_files = vec![second.clone(), first.clone()];
        let groups = group_data_files(vec![file.clone()], true, Some(2)).unwrap();
        assert_eq!(groups[0].selected_position_delete_files.len(), 2);
        let encoded = serde_json::to_vec(&groups[0]).unwrap();
        let decoded: IcebergFrozenRewriteGroupV1 = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            decoded.selected_position_delete_files,
            groups[0].selected_position_delete_files
        );
        let mut old_shape = serde_json::to_value(&groups[0]).unwrap();
        old_shape["selected_position_delete_files"] = serde_json::json!([path]);
        assert!(serde_json::from_value::<IcebergFrozenRewriteGroupV1>(old_shape).is_err());
        file.delete_files.reverse();
        let reverse = group_data_files(vec![file.clone()], true, Some(2)).unwrap();
        assert_eq!(groups[0].group_digest_hex, reverse[0].group_digest_hex);
        file.delete_files[0].content_size_in_bytes =
            Some(second.content_size_in_bytes.unwrap() + 1);
        let changed = group_data_files(vec![file], true, Some(2)).unwrap();
        assert_ne!(groups[0].group_digest_hex, changed[0].group_digest_hex);
        let data_groups = group_data_files(
            vec![{
                let mut data = IcebergDataFileInfo::for_test("s3://bucket/a.parquet", 10, 1);
                data.delete_files = vec![first, second];
                data
            }],
            false,
            None,
        )
        .unwrap();
        assert_eq!(data_groups[0].owned_data_delete_files.len(), 2);
    }

    #[test]
    fn shared_delete_file_has_one_canonical_owner() {
        let delete = IcebergDeleteFileInfo {
            record_count: Some(1),
            partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
            path: "s3://bucket/shared-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(8),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(3),
            partition_spec_id: Some(0),
            partition_key: None,
            referenced_data_file: None,
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
                .filter(|path| path.path() == "s3://bucket/shared-delete.parquet")
                .count(),
            1
        );
    }

    #[test]
    fn data_group_digest_covers_the_unattached_retirement_identity() {
        let data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let first = dv("s3://bucket/orphan.puffin");
        let mut second = first.clone();
        second.content_offset = Some(first.content_offset.unwrap() + 128);
        let build = |delete: &IcebergDeleteFileInfo| {
            let mut groups = group_data_files(vec![data.clone()], false, None).unwrap();
            assign_unattached_delete_owners(
                &mut groups,
                &BTreeSet::from([delete_entry_identity(delete).unwrap()]),
            )
            .unwrap();
            refresh_data_group_digests(&mut groups).unwrap();
            groups[0].group_digest_hex.clone()
        };
        assert_ne!(build(&first), build(&second));
    }

    #[test]
    fn orphan_live_delete_is_assigned_to_a_canonical_cohort() {
        let data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let mut groups = group_data_files(vec![data], false, None).expect("groups");
        assign_unattached_delete_owners(
            &mut groups,
            &BTreeSet::from([delete_entry_identity(&dv("s3://bucket/orphan.puffin")).unwrap()]),
        )
        .expect("owner");
        assert_eq!(
            groups[0].owned_data_delete_files,
            vec![delete_entry_identity(&dv("s3://bucket/orphan.puffin")).unwrap()]
        );
    }

    #[test]
    fn position_rewrite_rejects_v2_parquet_deletes() {
        let mut data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        data.delete_files.push(IcebergDeleteFileInfo {
            record_count: Some(1),
            partition_data_json: Some(r#"{"version":1,"values":[]}"#.to_string()),
            path: "s3://bucket/delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(8),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(3),
            partition_spec_id: Some(0),
            partition_key: None,
            referenced_data_file: None,
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
    fn admission_runtime(
        catalog_type: &str,
    ) -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        Arc<IcebergMetadataContext>,
        std::net::TcpListener,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HMS probe listener");
        listener.set_nonblocking(true).expect("nonblocking probe");
        let mut properties = vec![
            ("iceberg.catalog.type".to_string(), catalog_type.to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            ),
        ];
        if catalog_type == "hive" {
            properties.push((
                "hive.metastore.uris".to_string(),
                format!(
                    "thrift://{}",
                    listener.local_addr().expect("listener address")
                ),
            ));
        }
        let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
            .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        (executor, warehouse, runtime, listener)
    }

    fn admission_context(catalog_type: &str) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("context")
        .with_initiation(if catalog_type == "hive" {
            novarocks_spi::connector::ConnectorRequestInitiation::Statement
        } else {
            novarocks_spi::connector::ConnectorRequestInitiation::Background
        })
    }

    fn admission_table(
        instance: novarocks_spi::connector::ConnectorInstanceId,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(instance, Bytes::from_static(br#"{"namespace":"db","table":"absent","metadata_location":null,"table_info":null,"metadata_columns":[],"metadata_table_type":null,"prepared_files":[],"explicit_files":null}"#)).expect("table handle")
    }

    fn assert_admission_left_no_io(
        warehouse: &tempfile::TempDir,
        listener: &std::net::TcpListener,
    ) {
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse inventory")
                .count(),
            0
        );
        let error = listener
            .accept()
            .expect_err("admission must not connect to HMS");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn planning_refuses_hms_and_background_hadoop_before_rewrite_artifacts() {
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorProviderId, ConnectorWriteOperationId,
        };
        for catalog_type in ["hive", "hadoop"] {
            let (_executor, warehouse, runtime, listener) = admission_runtime(catalog_type);
            let descriptor = ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            };
            let incarnation = ProviderBindingEpoch::from_bytes([8; 16]);
            let provider = Arc::new(IcebergMetadata::new(
                descriptor.clone(),
                incarnation,
                runtime.clone(),
            ));
            let adapter =
                IcebergDistributedRewriteControl::new(descriptor, incarnation, runtime, provider)
                    .expect("adapter");
            for operation in [
                ConnectorDistributedRewriteOperation::RewriteDataFiles {
                    table: admission_table(adapter.key.instance_id.clone()),
                    rewrite_all: true,
                },
                ConnectorDistributedRewriteOperation::RewritePositionDeletes {
                    table: admission_table(adapter.key.instance_id.clone()),
                    rewrite_all: true,
                    min_input_files: None,
                },
            ] {
                let request = ConnectorDistributedRewritePlanningRequest::try_new(
                    ConnectorWriteOperationId::new(),
                    adapter.key.clone(),
                    operation,
                    admission_context(catalog_type),
                )
                .expect("request");
                let error = adapter
                    .plan_rewrite(request)
                    .expect_err("catalog admission must refuse planning");
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.to_string().contains(if catalog_type == "hive" {
                    "read-only compatibility entry"
                } else {
                    "background"
                }));
                assert!(adapter.plans.lock().expect("plans").is_empty());
                assert_admission_left_no_io(&warehouse, &listener);
            }
        }
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
