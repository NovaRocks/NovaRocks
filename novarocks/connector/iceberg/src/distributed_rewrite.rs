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
//! Frozen file ownership, attempt-local frozen inputs, and write activation all
//! live in the provider generation that owns the catalog client.  No Core
//! registry, process-global runtime, or current-generation lookup participates
//! in this capability.

// Design: ADR-0177 (docs/adr/ADR-0177-attempt-local-maintenance-inputs-and-single-rewrite-plan.md)
use crate::commit::model::{EntryIdentity, FrozenEntry};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorPinnedFileSet,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorRewriteCohortRead,
    ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteCohortId,
    ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
    ConnectorWriteIntent, ConnectorWritePreparation, ConnectorWriteReceipt,
    ConnectorWriteTargetRef, ProviderBindingEpoch,
};

use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::manifest::{DataFileWithStats, data_file_with_stats_to_iceberg_data_file_info};
use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;
use crate::read_snapshot::rewrite_entries::RewriteEntries;
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
};

pub(crate) const ARTIFACT_VERSION: u16 = 4;

const GROUP_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.group.v2\0";
const STATE_DOMAIN: &[u8] = b"novarocks.iceberg.distributed-rewrite.state.v1\0";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergRewritePlanPayloadV1 {
    version: u16,
    artifact_digest_hex: String,
    operation_id: String,
    target_ref: String,
}

/// Complete distributed-rewrite capability for one provider generation.
#[derive(Clone)]
pub struct IcebergDistributedRewriteControl {
    key: ConnectorProviderBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergMetadataContext>,
    provider: Arc<IcebergMetadata>,
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
        })
    }

    fn build_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        validate_context(&request.context)?;
        let target = self.provider.table_payload(request.operation().table())?;
        if target.metadata_table_type.is_some() {
            return Err(invalid("Iceberg distributed rewrite requires a base table handle").into());
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&target.namespace, &target.table);
        let table = self
            .runtime
            .load_table_for_request(&target.namespace, &target.table, &request.context)
            .map_err(unavailable)?
            .into_table();
        let table_for_files = table.clone();
        let read_control = request.context.clone();
        let (files, removals) = match table.metadata().current_snapshot_id() {
            Some(snapshot_id) => self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    let (snapshot, removals) =
                        crate::read_snapshot::build_rewrite_read_snapshot_at(
                            &table_for_files,
                            snapshot_id,
                            &read_control,
                        )
                        .await?;
                    let files = crate::manifest::finish_data_files_with_stats_from_original_files(
                        table_for_files.metadata(),
                        snapshot_id,
                        snapshot.files,
                        Some(&read_control),
                        |stats, _, _| stats,
                    )
                    .map_err(unavailable)?;
                    Ok::<_, ConnectorError>((files, removals))
                })
                .map_err(unavailable)??,
            None => (Vec::new(), RewriteEntries::default()),
        };
        validate_context(&request.context)?;
        self.build_loaded_plan(request, &table, files, removals, None, creation)
    }

    /// Pure group/schema/artifact author, called only by the bound closed CPU source on the async route.
    fn build_loaded_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
        table: &crate::iceberg::table::Table,
        files: Vec<DataFileWithStats>,
        observed_entries: RewriteEntries,
        removal_retention: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        validate_context(&request.context)?;
        let target = self.provider.table_payload(request.operation().table())?;
        let metadata = table.metadata();
        crate::commit::validation::deny_unwritable_data_files(
            metadata.current_schema(),
            &format!("{}.{}", target.namespace, target.table),
        )?;
        let base_snapshot_id = metadata.current_snapshot_id();
        let base_sequence_number = match base_snapshot_id {
            Some(id) => metadata
                .snapshot_by_id(id)
                .ok_or_else(|| internal("Iceberg rewrite planning snapshot is absent"))?
                .sequence_number(),
            None => 0,
        };
        let ConnectorDistributedRewriteOperation::OptimizeDataFiles {
            file_size_threshold_bytes,
            ..
        } = request.operation();
        let groups = plan_optimize_groups(
            files
                .into_iter()
                .map(data_file_with_stats_to_iceberg_data_file_info)
                .collect(),
            metadata.default_partition_spec_id(),
            *file_size_threshold_bytes,
        )?;
        let mut artifact = IcebergFrozenRewriteArtifactV1 {
            version: ARTIFACT_VERSION,
            operation_kind: request.operation().kind().to_string(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id,
            base_sequence_number,
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            groups,
        };
        let data_count = artifact
            .groups
            .iter()
            .map(|group| group.data_files.len())
            .sum();
        let delete_count = artifact
            .groups
            .iter()
            .map(|group| group.owned_data_delete_files.len())
            .sum();
        let mut data_paths = Vec::with_capacity(data_count);
        let mut delete_identities = Vec::with_capacity(delete_count);
        for group in &artifact.groups {
            validate_context(&request.context)?;
            data_paths.extend(group.data_files.iter().map(|file| file.path.as_str()));
            delete_identities.extend(group.owned_data_delete_files.iter());
        }
        let removals = observed_entries.select(data_paths, delete_identities, || {
            validate_context(&request.context)
        })?;
        let artifact_bytes = artifact.canonical_bytes()?;
        let manifest_digest = artifact_digest(&artifact_bytes);
        // The content digest uses the canonical group-digest order; execution uses cohort order.
        artifact.groups.sort_by_key(|group| {
            ConnectorWriteCohortId::derive(
                request.operation_id(),
                b"iceberg-distributed-rewrite-group",
                decode_digest(&group.group_digest_hex).expect("validated group digest"),
            )
            .expect("valid cohort domain")
        });
        let artifact = Arc::new(FrozenRewritePlanContent {
            #[cfg(test)]
            operation_id: request.operation_id(),
            #[cfg(test)]
            manifest_digest,
            metadata: table.metadata_ref(),
            removals,
            removal_retention: removal_retention.map(Arc::new),
            artifact,
        });
        let frozen = novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            self.key.clone(),
            artifact.clone(),
        );
        let physical_schema = Arc::new(
            crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
                .map_err(|error| internal(format!("convert Iceberg rewrite schema: {error}")))?,
        );
        let row_lineage = crate::schema_facts::row_lineage_enabled(metadata);
        let scan_schema = rewrite_input_schema(physical_schema, row_lineage);
        let cohorts = cohort_plans_from_artifact(
            &self.key,
            request,
            &artifact,
            scan_schema,
            row_lineage,
            metadata,
            creation,
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
                .map(|group| group.owned_data_delete_files.len() as u64)
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
            operation_id: hex::encode(request.operation_id().to_bytes()),
            target_ref: "main".to_string(),
        })?;
        ConnectorDistributedRewritePlan::try_new(
            request,
            state_digest,
            manifest_digest,
            summary,
            payload,
            cohorts,
            frozen,
        )
        .map_err(Into::into)
    }
}

pub(crate) mod async_plan;
impl ConnectorDistributedRewrite for IcebergDistributedRewriteControl {
    fn planning_initial_dependencies(
        &self,
    ) -> Result<Vec<novarocks_spi::connector::SharedDependency>, ConnectorError> {
        self.runtime.metadata_action_initial_dependencies()
    }

    fn scoped_cpu_source_type_id(&self) -> Option<std::any::TypeId> {
        Some(std::any::TypeId::of::<async_plan::RewritePlanOriginal>())
    }
    fn prepare_plan_source_in(
        &self,
        request: &mut Option<novarocks_spi::connector::ConnectorRewriteSourceRequest>,
        receiving: &mut Option<Box<dyn novarocks_spi::connector::ConnectorRewritePlanSource>>,
    ) -> Result<(), ConnectorError> {
        async_plan::prepare_source_in(self, request, receiving)
    }
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorDistributedRewritePlan,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        request.validate()?;
        if request.owner() != &self.key {
            return Err(
                invalid("Iceberg distributed rewrite request has a foreign generation").into(),
            );
        }
        let target = self.provider.table_payload(request.operation().table())?;
        if target.metadata_table_type.is_some() {
            return Err(invalid("Iceberg distributed rewrite requires a base table handle").into());
        }
        let operation = CatalogOperation::RewriteDataFiles;
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                CatalogTableName::new(target.namespace, target.table),
                request.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        self.build_plan(&request, creation)
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
    pub base_sequence_number: i64,
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
    pub owned_data_delete_files: Vec<EntryIdentity>,
}

/// Immutable attempt-owned input and the identity computed before cohort ordering.
/// Content bytes stay in FE; wire contains only this identity.
#[derive(Clone)]
pub(crate) struct FrozenRewritePlanContent {
    #[cfg(test)]
    pub(crate) operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    #[cfg(test)]
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) artifact: IcebergFrozenRewriteArtifactV1,
    pub(crate) metadata: Arc<crate::iceberg::spec::TableMetadata>,
    pub(crate) removals: Arc<[FrozenEntry]>,
    pub(crate) removal_retention:
        Option<Arc<novarocks_spi::connector::ConnectorPayloadRetentionGuard>>,
}
impl std::ops::Deref for FrozenRewritePlanContent {
    type Target = IcebergFrozenRewriteArtifactV1;
    fn deref(&self) -> &Self::Target {
        &self.artifact
    }
}
impl FrozenRewritePlanContent {
    /// Verify the author copied the exact facts of its retained planning observation.
    pub(crate) fn validate_planning_observation(&self) -> Result<(), ConnectorError> {
        let artifact = &self.artifact;
        let metadata = &self.metadata;
        let snapshot = metadata.snapshot_for_ref(&artifact.target_ref);
        let mut uuid_bytes = [0; 36];
        let table_uuid = metadata.uuid().hyphenated().encode_lower(&mut uuid_bytes);
        if artifact.version != ARTIFACT_VERSION
            || artifact.table_uuid.as_str() != table_uuid
            || artifact.schema_id != metadata.current_schema_id()
            || artifact.default_spec_id != metadata.default_partition_spec_id()
            || artifact.base_snapshot_id != snapshot.map(|s| s.snapshot_id())
            || artifact.base_sequence_number != snapshot.map_or(0, |s| s.sequence_number())
        {
            return Err(invalid(
                "Iceberg frozen rewrite artifact contradicts its planning observation",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
impl FrozenRewritePlanContent {
    pub(crate) fn validate_identity(
        &self,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
        digest_hex: &str,
    ) -> Result<(), ConnectorError> {
        if self.operation_id != operation_id || hex::encode(self.manifest_digest) != digest_hex {
            return Err(internal(
                "Iceberg frozen rewrite content identity differs from its handle",
            ));
        }
        Ok(())
    }
}

impl IcebergFrozenRewriteArtifactV1 {
    fn canonical_bytes(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ARTIFACT_VERSION {
            return Err(invalid("Iceberg rewrite artifact is invalid"));
        }
        canonical_json(self)
    }
}

#[cfg(test)]
impl IcebergFrozenRewriteArtifactV1 {
    pub(crate) fn group(
        &self,
        digest: &str,
    ) -> Result<&IcebergFrozenRewriteGroupV1, ConnectorError> {
        self.groups
            .iter()
            .find(|group| group.group_digest_hex == digest)
            .ok_or_else(|| internal("Iceberg frozen rewrite plan has no requested group"))
    }
}

/// Trino optimize selects candidates before eliminating singleton partitions.
/// A candidate from an older spec causes every candidate to be rewritten.
fn plan_optimize_groups(
    files: Vec<IcebergDataFileInfo>,
    current_spec_id: i32,
    threshold_bytes: u64,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .filter(|file| {
            !file.delete_files.is_empty()
                || u64::try_from(file.size).is_ok_and(|size| size <= threshold_bytes)
        })
        .collect::<Vec<_>>();
    let old_spec = files
        .iter()
        .any(|file| file.partition_spec_id != Some(current_spec_id));
    let groups = group_data_files(files)?;
    // Attached delete ownership is assigned after selection, so one logical
    // delete entry has exactly one selected owner even across partitions.
    let mut groups = groups
        .into_iter()
        .filter(|group| {
            old_spec
                || group.data_files.len() > 1
                || group
                    .data_files
                    .iter()
                    .any(|file| !file.delete_files.is_empty())
        })
        .collect::<Vec<_>>();
    for group in &mut groups {
        group.owned_data_delete_files.clear();
    }
    assign_attached_delete_owners(&mut groups)?;
    refresh_data_group_digests(&mut groups)?;
    bounded_groups(groups)
}

pub(crate) fn group_data_files(
    mut files: Vec<IcebergDataFileInfo>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    files.sort_by(|left, right| left.path.cmp(&right.path));
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

fn bounded_groups(
    mut groups: Vec<IcebergFrozenRewriteGroupV1>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups
        .windows(2)
        .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid("Iceberg rewrite group digest collision"));
    }
    Ok(groups)
}

fn cohort_plans_from_artifact(
    owner: &ConnectorProviderBindingKey,
    request: &ConnectorDistributedRewritePlanningRequest,
    artifact: &IcebergFrozenRewriteArtifactV1,
    scan_schema: SchemaRef,
    row_lineage: bool,
    metadata: &crate::iceberg::spec::TableMetadata,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    Vec<ConnectorDistributedRewriteCohortPlan>,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    let intent = ConnectorWriteIntent::Overwrite;
    let execution = rewrite_execution_schema(metadata, row_lineage, creation)?;
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
                &execution,
                row_lineage,
                metadata,
            )?;
            let base_snapshot_id = artifact.base_snapshot_id.ok_or_else(|| {
                invalid("Iceberg rewrite group has no base snapshot to pin its files to")
            })?;
            let read = ConnectorRewriteCohortRead::PinnedFileSet(ConnectorPinnedFileSet::try_new(
                &artifact.namespace,
                &artifact.table,
                base_snapshot_id,
                group.data_files.iter().map(|file| file.path.as_str()),
            )?);
            ConnectorDistributedRewriteCohortPlan::try_new_typed(
                cohort_id,
                read,
                scan_schema.clone(),
                execution.clone(),
                arrow_schema_digest(&scan_schema),
                preparation,
                group_digest,
                creation,
            )
        })
        .collect()
}

fn rewrite_execution_schema(
    metadata: &crate::iceberg::spec::TableMetadata,
    row_lineage: bool,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    novarocks_connector_contract::ConnectorReadExecutionSchema,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    let mut pseudo = creation.reserve::<String>(if row_lineage { 2 } else { 0 })?;
    if row_lineage {
        pseudo.push(creation.text(ICEBERG_ROW_ID_COL)?);
        pseudo.push(creation.text(ICEBERG_LAST_UPDATED_SEQ_COL)?);
    }
    crate::schema_mapping::catalog_value::table(
        metadata,
        metadata.current_schema(),
        &pseudo,
        creation,
    )
}

fn rewrite_preparation(
    owner: &ConnectorProviderBindingKey,
    request: &ConnectorDistributedRewritePlanningRequest,
    intent: ConnectorWriteIntent,
    scan_schema: &Schema,
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    row_lineage: bool,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<
    ConnectorWritePreparation,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    let fields = scan_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let value = execution.binding(ordinal).ok_or_else(|| {
                internal("Iceberg rewrite input has no complete execution declaration")
            })?;
            // Provider annotations remain source facts; the already authored
            // execution declaration chooses the writer's actual value carrier.
            ConnectorWriteFieldBinding::try_new_typed(
                rewrite_field_token(owner, request.operation().table(), intent, ordinal, field),
                field
                    .as_ref()
                    .clone()
                    .with_data_type(value.data_type().clone()),
                value.clone(),
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let input = if row_lineage {
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
    } else {
        ConnectorWriteInputShape::Data { fields }
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
    .map_err(Into::into)
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

/// The schema one frozen rewrite cohort reads.
///
/// Planning freezes this into the cohort plan and `begin_scan` has to reproduce
/// it field-for-field — the frozen read refuses a scan whose output schema
/// differs — so both sides resolve it here rather than deriving it twice.
fn rewrite_input_schema(physical_schema: SchemaRef, row_lineage: bool) -> SchemaRef {
    if row_lineage {
        let mut fields = physical_schema.fields().to_vec();
        fields.extend([
            Arc::new(Field::new(
                ICEBERG_ROW_ID_COL,
                DataType::Int64,
                crate::typed_read::IcebergMetadataColumn::RowId.nullable(),
            )),
            Arc::new(Field::new(
                ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                crate::typed_read::IcebergMetadataColumn::LastUpdatedSequenceNumber.nullable(),
            )),
        ]);
        Arc::new(Schema::new(fields))
    } else {
        physical_schema
    }
}

fn artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact.v1\0");
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
    // Include the logical delete entries this cohort owns.
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

pub(crate) fn decode_digest(value: &str) -> Result<[u8; 32], ConnectorError> {
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
            fields.sort_by_key(|(left, _)| *left);
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

#[cfg(test)]
mod tests;

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
            .as_chunks::<2>()
            .0
            .iter()
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
