// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Iceberg-owned planning facts for C1-backed distributed rewrites.
//!
//! This module freezes the input file ownership before a frontend asks C1 to
//! place a writer.  It deliberately serializes the detailed file list only to
//! a provider artifact: generic SPI transports its digest and a bounded group
//! handle, never Iceberg files or catalog state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_connector_iceberg::iceberg::{NamespaceIdent, TableIdent};
use novarocks_spi::connector::{
    ConnectorDistributedRewrite, ConnectorDistributedRewriteAttemptCheckpoint,
    ConnectorDistributedRewriteAttemptDisposition, ConnectorDistributedRewriteCohortPlan,
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteReceipt, ConnectorDistributedRewriteReceiptSummary, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorStagedReport,
    ConnectorStagedReportSummary, ConnectorWriteAdmissionPurpose, ConnectorWriteAttemptCompletion,
    ConnectorWriteCohortId, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    ConnectorWriteIntent, ConnectorWritePreparation, ConnectorWritePreparationOutcome,
    ConnectorWritePreparationRequest, ConnectorWriteReceipt, ConnectorWriterIdentity,
    ConnectorWriterTerminalState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info;
use super::catalog::registry::{
    DataFileWithStats, IcebergCatalogEntry, IcebergCatalogRegistry, block_on_iceberg,
    build_iceberg_catalog, extract_data_files_with_stats, load_table,
};
use super::commit::{IcebergCommitCollector, SelectedRewriteKind};
use super::sink::build_position_delete_data_file_partition_index;
use super::sink_plan::IcebergSinkObjectStoreConfig;
use super::write_commit::IcebergWriteCommitExecutor;
use super::write_contract::{
    encode_frozen_data_rewrite_handle_payload, encode_frozen_deletion_vector_rewrite_handle_payload,
};
use super::write_service::{
    IcebergDistributedRewriteMarkerBase, IcebergDistributedRewriteReportCommitter,
    IcebergWriteCohortContext, IcebergWriteControlService, IcebergWriteControlServiceContext,
    IcebergWriteReportCommitter, IcebergWriteServiceRegistry,
};
use crate::common::types::UniqueId;
use crate::connector::iceberg::commit::CommitOpKind;
use crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry;
use novarocks_connector_iceberg::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
};

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
pub(crate) struct PlannedIcebergDistributedRewrite {
    pub(crate) plan: ConnectorDistributedRewritePlan,
    pub(crate) artifact: IcebergFrozenRewriteArtifactV1,
    pub(crate) artifact_location: String,
}

/// Exact-generation FE planner.  Its cache is intentionally operation scoped:
/// the same request may replay, while a different request under the same
/// operation ID is rejected before any BE staging is possible.
pub(crate) struct IcebergDistributedRewritePlanner {
    key: ConnectorExecutionBindingKey,
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    plans: Mutex<
        HashMap<
            novarocks_spi::connector::ConnectorWriteOperationId,
            PlannedIcebergDistributedRewrite,
        >,
    >,
}

impl IcebergDistributedRewritePlanner {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        if instance_id != key.instance_id {
            return Err(invalid(
                "Iceberg distributed rewrite planner instance does not match key",
            ));
        }
        Ok(Self {
            key,
            descriptor,
            instance_id,
            registry,
            plans: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    pub(crate) fn plan(
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
            if existing.plan.request_digest() == request.request_digest() {
                return Ok(existing.plan);
            }
            return Err(invalid(
                "Iceberg distributed rewrite operation conflicts with cached plan",
            ));
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

    pub(crate) fn planned(
        &self,
        operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    ) -> Result<PlannedIcebergDistributedRewrite, ConnectorError> {
        self.plans
            .lock()
            .map_err(|_| internal("Iceberg distributed rewrite plan cache lock poisoned"))?
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg rewrite operation has no frozen plan",
                )
            })
    }

    pub(crate) fn entry(&self) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|_| internal("Iceberg distributed rewrite registry lock poisoned"))?
            .get(self.instance_id.as_str())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })
    }

    fn build_plan(
        &self,
        request: &ConnectorDistributedRewritePlanningRequest,
    ) -> Result<PlannedIcebergDistributedRewrite, ConnectorError> {
        let (namespace, table_name) =
            super::provider::decode_data_mutation_table_target(request.operation().table())?;
        let entry = self.entry()?;
        entry.invalidate_table_cache(&namespace, &table_name);
        let loaded = load_table(&entry, &namespace, &table_name).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        })?;
        let table = loaded.table;
        let metadata = table.metadata();
        let base_snapshot_id = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        let files = extract_data_files_with_stats(&table)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error))?;
        let groups = match request.operation() {
            ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
                let live_delete_paths = live_delete_file_paths(&table)?;
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
                    && metadata.format_version()
                        != novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3
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
            namespace: namespace.clone(),
            table: table_name.clone(),
            table_uuid: metadata.uuid().to_string(),
            target_ref: "main".to_string(),
            base_snapshot_id,
            schema_id: metadata.current_schema_id(),
            default_spec_id: metadata.default_partition_spec_id(),
            groups,
        };
        let artifact_bytes = artifact.canonical_bytes()?;
        let artifact_digest = artifact_digest(&artifact_bytes);
        let artifact_location = format!(
            "{}/_novarocks/maintenance/v2/distributed-rewrite/{}/{}",
            metadata.location(),
            hex::encode(request.operation_id().to_bytes()),
            hex::encode(artifact_digest),
        );
        write_frozen_artifact(
            table.file_io().clone(),
            &artifact,
            artifact_digest,
            &artifact_location,
        )?;

        let physical_schema = Arc::new(
            novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(
                metadata.current_schema(),
            )
            .map_err(|error| {
                internal(format!("convert Iceberg rewrite schema to Arrow: {error}"))
            })?,
        );
        let input_schema = rewrite_input_schema(
            request.operation(),
            physical_schema,
            super::catalog::backend::row_lineage_enabled(metadata),
        );
        let cohorts = cohort_plans_from_artifact(
            request,
            artifact_digest,
            &artifact_location,
            &artifact.groups,
            input_schema,
            super::catalog::backend::row_lineage_enabled(metadata),
        )?;
        let state_digest = rewrite_state_digest(
            metadata.uuid().to_string().as_bytes(),
            table.metadata_location().ok_or_else(|| {
                invalid("Iceberg distributed rewrite table has no metadata location")
            })?,
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
                .flat_map(|group| group.data_files.iter())
                .map(|file| file.size.max(0) as u64)
                .sum(),
            expected_output_files: 0,
        };
        let payload = canonical_payload(&IcebergRewritePlanPayloadV1 {
            version: 1,
            artifact_digest_hex: hex::encode(artifact_digest),
            artifact_location: artifact_location.clone(),
            target_ref: "main".to_string(),
        })?;
        let plan = ConnectorDistributedRewritePlan::try_new(
            request,
            state_digest,
            artifact_digest,
            summary,
            payload,
            cohorts,
        )?;
        Ok(PlannedIcebergDistributedRewrite {
            plan,
            artifact,
            artifact_location,
        })
    }
}

/// Exact-generation implementation of the provider-neutral rewrite capability.
/// Planning owns the frozen Iceberg artifact; activation derives the C1 writer
/// service from that immutable artifact and never asks a current generation to
/// re-plan or rediscover the selected files.
pub(crate) struct IcebergDistributedRewriteAdapter {
    planner: Arc<IcebergDistributedRewritePlanner>,
    services: IcebergWriteServiceRegistry,
    activated: Mutex<HashMap<novarocks_spi::connector::ConnectorWriteOperationId, [u8; 32]>>,
}

impl IcebergDistributedRewriteAdapter {
    pub(crate) fn new_registered(
        key: ConnectorExecutionBindingKey,
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
        services: IcebergWriteServiceRegistry,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            planner: Arc::new(IcebergDistributedRewritePlanner::new_registered(
                key,
                instance_id,
                registry,
            )?),
            services,
            activated: Mutex::new(HashMap::new()),
        })
    }

    fn build_service(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<Arc<dyn super::write_control::IcebergWriteControlBackend>, ConnectorError> {
        let planned = self.planner.planned(plan.operation_id())?;
        if planned.plan.plan_digest() != plan.plan_digest()
            || planned.plan.manifest_digest() != plan.manifest_digest()
        {
            return Err(invalid(
                "Iceberg distributed rewrite activation does not match its frozen plan",
            ));
        }
        let artifact = planned.artifact;
        let entry = self.planner.entry()?;
        entry.invalidate_table_cache(&artifact.namespace, &artifact.table);
        let table = load_table(&entry, &artifact.namespace, &artifact.table)
            .map_err(|error| unavailable(format!("reload frozen Iceberg rewrite table: {error}")))?
            .table;
        validate_frozen_rewrite_table(&artifact, table.metadata())?;
        let catalog = build_iceberg_catalog(&entry)
            .map_err(|error| unavailable(format!("build Iceberg rewrite catalog: {error}")))?;
        let table_ident = TableIdent::new(
            NamespaceIdent::new(artifact.namespace.clone()),
            artifact.table.clone(),
        );
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::SelectedRewrite,
                table_ident,
                artifact.base_snapshot_id,
                table.metadata().last_sequence_number(),
                table.metadata().current_schema().clone(),
                table.metadata().default_partition_spec().clone(),
                format!(
                    "{}/data/_staging/{}",
                    table.metadata().location(),
                    plan.operation_id()
                ),
                UniqueId::new(0, 0),
            )
            .with_table_metadata(table.metadata().clone()),
        );
        let cleanup = build_abort_cleanup_for_catalog_entry(&entry)
            .map_err(|error| unavailable(format!("prepare rewrite cleanup: {error}")))?;
        let executor = Arc::new(IcebergWriteCommitExecutor {
            catalog,
            table: table.clone(),
            collector,
            fs: cleanup.fs,
            cleanup_path_mapper: cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: artifact.target_ref.clone(),
            snapshot_properties: BTreeMap::new(),
        });
        let mut contexts = BTreeMap::new();
        for cohort in plan.cohorts() {
            let group = artifact
                .groups
                .iter()
                .find(|candidate| {
                    decode_digest(&candidate.group_digest_hex, "Iceberg rewrite group")
                        .is_ok_and(|digest| digest == cohort.group_digest())
                })
                .ok_or_else(|| invalid("Iceberg rewrite cohort names an unknown frozen group"))?;
            let group_digest = decode_digest(&group.group_digest_hex, "Iceberg rewrite group")?;
            let expected_cohort = ConnectorWriteCohortId::derive(
                plan.operation_id(),
                b"iceberg-distributed-rewrite-group",
                group_digest,
            )?;
            if expected_cohort != cohort.cohort_id() {
                return Err(invalid(
                    "Iceberg rewrite cohort ID does not match frozen group",
                ));
            }
            let data_paths = group
                .data_files
                .iter()
                .map(|file| file.path.clone())
                .collect::<BTreeSet<_>>();
            let delete_paths = match plan.operation_kind() {
                novarocks_spi::connector::REWRITE_DATA_FILES_KIND => group
                    .owned_data_delete_files
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND => group
                    .selected_position_delete_files
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                _ => {
                    return Err(invalid(
                        "Iceberg rewrite plan has an unsupported operation kind",
                    ));
                }
            };
            let (kind, handle) = match plan.operation_kind() {
                novarocks_spi::connector::REWRITE_DATA_FILES_KIND => (
                    SelectedRewriteKind::Data,
                    encode_frozen_data_rewrite_handle_payload(
                        table.metadata(),
                        artifact.base_snapshot_id,
                        super::catalog::backend::row_lineage_enabled(table.metadata()),
                    )
                    .map_err(|error| {
                        invalid(format!("encode data rewrite writer handle: {error}"))
                    })?,
                ),
                novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND => {
                    let partitions = rewrite_position_partitions(
                        &entry,
                        table.metadata(),
                        artifact.base_snapshot_id,
                        &data_paths,
                    )?;
                    (
                        SelectedRewriteKind::PositionDeletes,
                        encode_frozen_deletion_vector_rewrite_handle_payload(
                            table.metadata(),
                            artifact.base_snapshot_id,
                            &partitions,
                        )
                        .map_err(|error| {
                            invalid(format!("encode position rewrite writer handle: {error}"))
                        })?,
                    )
                }
                _ => unreachable!("validated Iceberg rewrite operation kind"),
            };
            let context = IcebergWriteCohortContext::distributed_rewrite(
                handle,
                group_payload_from_artifact(
                    group,
                    plan.manifest_digest(),
                    &planned.artifact_location,
                )?,
                kind,
                data_paths,
                delete_paths,
            )?;
            if contexts.insert(cohort.cohort_id(), context).is_some() {
                return Err(invalid("Iceberg rewrite plan contains a duplicate cohort"));
            }
        }
        let marker = IcebergDistributedRewriteMarkerBase {
            operation_id: plan.operation_id(),
            plan_digest: plan.plan_digest(),
            manifest_digest: plan.manifest_digest(),
            target_ref: artifact.target_ref,
            incarnation: self.planner.binding_key().incarnation,
            operation_kind: plan.operation_kind().to_string(),
        };
        let committer: Arc<dyn IcebergWriteReportCommitter> = Arc::new(
            IcebergDistributedRewriteReportCommitter::new(executor, marker),
        );
        let service = IcebergWriteControlService::new(
            IcebergWriteControlServiceContext::new_with_cohorts(contexts, committer)?,
        );
        Ok(Arc::new(service))
    }

    fn attempt_file_io(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<novarocks_connector_iceberg::iceberg::io::FileIO, ConnectorError> {
        let (namespace, table) = super::provider::decode_data_mutation_table_target(plan.target())?;
        let entry = self.planner.entry()?;
        let table = load_table(&entry, &namespace, &table)
            .map_err(|error| {
                unavailable(format!("load Iceberg rewrite checkpoint table: {error}"))
            })?
            .table;
        Ok(table.file_io().clone())
    }
}

impl ConnectorDistributedRewrite for IcebergDistributedRewriteAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.planner.descriptor()
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        self.planner.binding_key()
    }

    fn plan_rewrite(
        &self,
        request: ConnectorDistributedRewritePlanningRequest,
    ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
        self.planner.plan(request)
    }

    fn activate_rewrite(
        &self,
        plan: &ConnectorDistributedRewritePlan,
    ) -> Result<(), ConnectorError> {
        plan.validate()?;
        if plan.owner() != self.binding_key() {
            return Err(invalid(
                "Iceberg rewrite activation has a foreign generation",
            ));
        }
        let mut activated = self
            .activated
            .lock()
            .map_err(|_| internal("Iceberg rewrite activation cache lock poisoned"))?;
        match activated.get(&plan.operation_id()) {
            Some(existing) if existing == &plan.plan_digest() => return Ok(()),
            Some(_) => {
                return Err(invalid(
                    "Iceberg rewrite activation conflicts with frozen plan",
                ));
            }
            None => {}
        }
        let service = self.build_service(plan)?;
        let operation_id = plan.operation_id();
        let digest = plan.plan_digest();
        self.services
            .register_lazy(operation_id, digest, move || Ok(Arc::clone(&service)))?;
        activated.insert(operation_id, digest);
        Ok(())
    }

    fn checkpoint_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        disposition: ConnectorDistributedRewriteAttemptDisposition,
        completion: &ConnectorWriteAttemptCompletion,
    ) -> Result<ConnectorDistributedRewriteAttemptCheckpoint, ConnectorError> {
        validate_rewrite_attempt(plan, completion)?;
        let file_io = self.attempt_file_io(plan)?;
        let location = attempt_artifact_location(plan, completion, disposition)?;
        let bytes = encode_attempt_artifact(completion)?;
        let artifact_digest: [u8; 32] = Sha256::digest(&bytes).into();
        let output = file_io.new_output(&location).map_err(|error| {
            unavailable(format!("create Iceberg rewrite attempt artifact: {error}"))
        })?;
        block_on_iceberg(async move { output.write(bytes).await })
            .map_err(|error| {
                unavailable(format!("write Iceberg rewrite attempt artifact: {error}"))
            })?
            .map_err(|error| {
                unavailable(format!("persist Iceberg rewrite attempt artifact: {error}"))
            })?;
        let artifact_handle =
            checkpoint_handle(plan, disposition, completion, &location, artifact_digest)?;
        ConnectorDistributedRewriteAttemptCheckpoint::try_new(
            completion.cohort_id(),
            completion.execution_id(),
            disposition,
            completion.digest(),
            artifact_digest,
            artifact_handle,
        )
    }

    fn restore_attempt(
        &self,
        plan: &ConnectorDistributedRewritePlan,
        checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    ) -> Result<ConnectorWriteAttemptCompletion, ConnectorError> {
        checkpoint.validate()?;
        let handle = decode_checkpoint_handle(&checkpoint.artifact_handle)?;
        validate_checkpoint_handle(plan, checkpoint, &handle)?;
        let file_io = self.attempt_file_io(plan)?;
        let bytes = read_artifact_file(&file_io, &handle.location, REWRITE_ARTIFACT_MAX_BYTES)?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != checkpoint.artifact_digest || actual != handle.artifact_digest()? {
            return Err(invalid(
                "Iceberg rewrite attempt artifact digest conflicts with checkpoint",
            ));
        }
        let completion = decode_attempt_artifact(&bytes)?;
        validate_rewrite_attempt(plan, &completion)?;
        if completion.execution_id() != checkpoint.execution_id
            || completion.cohort_id() != checkpoint.cohort_id
            || completion.digest() != checkpoint.attempt_digest
        {
            return Err(invalid(
                "Iceberg rewrite restored attempt digest conflicts with checkpoint",
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
        let target_version = receipt
            .committed_version()
            .and_then(|version| version.snapshot_id());
        let payload = canonical_payload(&IcebergRewriteReceiptPayloadV1 {
            version: 1,
            operation_id_hex: hex::encode(plan.operation_id().to_bytes()),
            plan_digest_hex: hex::encode(plan.plan_digest()),
            receipt_digest_hex: hex::encode(receipt.digest()),
        })?;
        let rewrite_receipt = ConnectorDistributedRewriteReceipt::try_new(
            ConnectorDistributedRewriteReceiptSummary {
                input_data_files: plan.summary().input_data_files,
                input_delete_files: plan.summary().input_delete_files,
                output_data_files: 0,
                output_delete_files: 0,
                output_rows: receipt.resulting_row_count().unwrap_or(0),
                target_version,
            },
            payload,
        )?;
        let (namespace, table) = super::provider::decode_data_mutation_table_target(plan.target())?;
        self.planner
            .entry()?
            .invalidate_table_cache(&namespace, &table);
        Ok(rewrite_receipt)
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

fn validate_frozen_rewrite_table(
    artifact: &IcebergFrozenRewriteArtifactV1,
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
) -> Result<(), ConnectorError> {
    if metadata.uuid().to_string() != artifact.table_uuid
        || metadata
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id())
            != artifact.base_snapshot_id
        || metadata.current_schema_id() != artifact.schema_id
        || metadata.default_partition_spec_id() != artifact.default_spec_id
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg distributed rewrite frozen table state is no longer current",
        ));
    }
    Ok(())
}

fn rewrite_position_partitions(
    entry: &IcebergCatalogEntry,
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    base_snapshot_id: Option<i64>,
    selected_data_paths: &BTreeSet<String>,
) -> Result<HashMap<String, super::sink_plan::PositionDeleteDataFilePartition>, ConnectorError> {
    let storage = position_delete_index_storage_config(entry, metadata.location())?;
    let all = build_position_delete_data_file_partition_index(
        metadata,
        base_snapshot_id,
        metadata.location(),
        storage.as_ref(),
    )
    .map_err(|error| {
        unavailable(format!(
            "build frozen position-delete rewrite partition index: {error}"
        ))
    })?;
    let mut selected = HashMap::with_capacity(selected_data_paths.len());
    for path in selected_data_paths {
        let partition = all.get(path).cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg frozen position-delete rewrite data file is no longer live",
            )
        })?;
        selected.insert(path.clone(), partition);
    }
    Ok(selected)
}

fn position_delete_index_storage_config(
    entry: &IcebergCatalogEntry,
    table_location: &str,
) -> Result<Option<IcebergSinkObjectStoreConfig>, ConnectorError> {
    let Some(bucket) =
        super::changes::expected_object_store_bucket_from_location(table_location)
            .map_err(|error| invalid(format!("resolve rewrite position-delete bucket: {error}")))?
    else {
        return Ok(None);
    };
    let config = entry.object_store_config().ok_or_else(|| {
        unavailable(format!(
            "Iceberg position-delete rewrite requires object-store credentials for bucket {bucket}"
        ))
    })?;
    Ok(Some(IcebergSinkObjectStoreConfig {
        endpoint: config.endpoint.clone(),
        bucket,
        access_key_id: config.access_key_id.clone(),
        access_key_secret: config.access_key_secret.clone(),
        session_token: config.session_token.clone(),
        region: config.region.clone(),
        enable_path_style_access: config.enable_path_style_access,
        retry_max_times: config.retry_max_times,
        retry_min_delay_ms: config.retry_min_delay_ms,
        retry_max_delay_ms: config.retry_max_delay_ms,
        timeout_ms: config.timeout_ms,
        io_timeout_ms: config.io_timeout_ms,
    }))
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
            "Iceberg distributed rewrite checkpoint completion is foreign to its plan",
        ));
    }
    Ok(())
}

fn checkpoint_handle(
    plan: &ConnectorDistributedRewritePlan,
    disposition: ConnectorDistributedRewriteAttemptDisposition,
    completion: &ConnectorWriteAttemptCompletion,
    location: &str,
    artifact_digest: [u8; 32],
) -> Result<Bytes, ConnectorError> {
    canonical_payload(&IcebergRewriteAttemptHandleV1 {
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
        location: location.to_string(),
        artifact_digest_hex: hex::encode(artifact_digest),
    })
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
    location: String,
    artifact_digest_hex: String,
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
        decode_digest(
            &self.artifact_digest_hex,
            "Iceberg rewrite attempt artifact",
        )
    }
}

fn decode_checkpoint_handle(
    payload: &[u8],
) -> Result<IcebergRewriteAttemptHandleV1, ConnectorError> {
    let handle: IcebergRewriteAttemptHandleV1 =
        decode_canonical_json(payload, "Iceberg rewrite attempt checkpoint handle")?;
    if handle.version != 1
        || handle.location.is_empty()
        || handle.location.len() > 16 * 1024
        || !handle
            .location
            .contains("/_novarocks/maintenance/v2/distributed-rewrite/")
        || handle.operation_id_hex.len() != 32
        || handle.plan_digest_hex.len() != 64
        || handle.cohort_id_hex.len() != 64
        || handle.query_id_hex.len() != 32
        || handle.attempt_digest_hex.len() != 64
    {
        return Err(invalid("Iceberg rewrite checkpoint handle is invalid"));
    }
    let _ = handle.disposition()?;
    let _ = decode_digest(&handle.plan_digest_hex, "Iceberg rewrite checkpoint plan")?;
    let _ = decode_digest(
        &handle.attempt_digest_hex,
        "Iceberg rewrite checkpoint attempt",
    )?;
    let _ = handle.artifact_digest()?;
    Ok(handle)
}

fn validate_checkpoint_handle(
    plan: &ConnectorDistributedRewritePlan,
    checkpoint: &ConnectorDistributedRewriteAttemptCheckpoint,
    handle: &IcebergRewriteAttemptHandleV1,
) -> Result<(), ConnectorError> {
    let operation: [u8; 16] = hex::decode(&handle.operation_id_hex)
        .map_err(|error| {
            invalid(format!(
                "decode Iceberg rewrite checkpoint operation: {error}"
            ))
        })?
        .try_into()
        .map_err(|_| invalid("Iceberg rewrite checkpoint operation has invalid length"))?;
    let cohort = decode_digest(&handle.cohort_id_hex, "Iceberg rewrite checkpoint cohort")?;
    let query: [u8; 16] = hex::decode(&handle.query_id_hex)
        .map_err(|error| invalid(format!("decode Iceberg rewrite checkpoint query: {error}")))?
        .try_into()
        .map_err(|_| invalid("Iceberg rewrite checkpoint query has invalid length"))?;
    if operation != plan.operation_id().to_bytes()
        || decode_digest(&handle.plan_digest_hex, "Iceberg rewrite checkpoint plan")?
            != plan.plan_digest()
        || cohort != checkpoint.cohort_id.to_bytes()
        || query != checkpoint.execution_id.query_id()
        || handle.attempt_id != checkpoint.execution_id.attempt_id()
        || handle.disposition()? != checkpoint.disposition
        || decode_digest(
            &handle.attempt_digest_hex,
            "Iceberg rewrite checkpoint attempt",
        )? != checkpoint.attempt_digest
        || handle.artifact_digest()? != checkpoint.artifact_digest
    {
        return Err(invalid(
            "Iceberg rewrite checkpoint handle does not match durable checkpoint facts",
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
            "Iceberg distributed rewrite plan payload version is unsupported",
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
    let reports = completion.reports();
    let count = u32::try_from(reports.len())
        .map_err(|_| exhausted("Iceberg rewrite attempt has too many reports"))?;
    put_u32(&mut out, count);
    for report in reports {
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
        return Err(exhausted(
            "Iceberg rewrite attempt artifact exceeds the 64 MiB operation limit",
        ));
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
        return Err(invalid(
            "Iceberg rewrite attempt artifact report count is invalid",
        ));
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
        let report =
            ConnectorStagedReport::try_new(writer, version, state, summary, cursor.take_blob()?)?;
        reports.push(report);
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
        .map_err(|_| exhausted("Iceberg rewrite attempt artifact blob exceeds u32"))?;
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
            .map_err(|_| invalid("Iceberg rewrite attempt artifact array is invalid"))
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
            return Err(exhausted(
                "Iceberg rewrite attempt artifact blob exceeds 64 MiB",
            ));
        }
        Ok(Bytes::copy_from_slice(self.take_exact(length)?))
    }

    fn take_binding_key(&mut self) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
        let length = self.take_u16()? as usize;
        let instance = std::str::from_utf8(self.take_exact(length)?)
            .map_err(|_| invalid("Iceberg rewrite binding instance ID is not UTF-8"))?;
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
        let operation_id =
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(self.take_array()?);
        let cohort_id = ConnectorWriteCohortId::from_bytes(self.take_array()?);
        let execution_id = self.take_execution_id()?;
        let fragment_instance_id = self.take_array()?;
        let fragment_id = i32::from_be_bytes(self.take_array()?);
        let backend_num = i32::from_be_bytes(self.take_array()?);
        let sink_ordinal = self.take_u32()?;
        let binding_key = self.take_binding_key()?;
        Ok(ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            fragment_instance_id,
            fragment_id,
            backend_num,
            sink_ordinal,
            binding_key,
        ))
    }
}

/// The immutable, provider-private plan.  It is intentionally canonical JSON
/// so the artifact itself can be content-addressed and verified after an FE
/// restart without making its file list public SPI state.
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

/// Bounded root record.  It intentionally carries no selected files: those
/// remain in content-addressed parts below the provider-private root.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactRootV1 {
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
    parts: Vec<IcebergFrozenRewriteArtifactPartRefV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactPartRefV1 {
    index: u16,
    digest_hex: String,
    location: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergFrozenRewriteArtifactPartV1 {
    version: u16,
    groups: Vec<IcebergFrozenRewriteGroupV1>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergFrozenRewriteGroupV1 {
    pub group_digest_hex: String,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub data_files: Vec<IcebergDataFileInfo>,
    /// Puffin deletion-vector inputs selected for a position-delete rewrite.
    /// Data rewrite groups leave this empty; any delete dependency remains
    /// attached to its data file as read-only scan input.
    pub selected_position_delete_files: Vec<String>,
    /// Delete files removed by a data rewrite. A shared dependency has exactly
    /// one canonical owner, while readers retain it until aggregate commit.
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

impl IcebergFrozenRewriteArtifactV1 {
    pub(crate) fn canonical_bytes(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ARTIFACT_VERSION || self.groups.len() > REWRITE_ARTIFACT_MAX_GROUPS {
            return Err(invalid(
                "Iceberg rewrite artifact version or group count is invalid",
            ));
        }
        let bytes = canonical_json(self, "frozen Iceberg rewrite artifact")?;
        if bytes.len() > REWRITE_ARTIFACT_MAX_BYTES {
            return Err(exhausted("frozen Iceberg rewrite artifact exceeds 64 MiB"));
        }
        Ok(bytes)
    }
}

pub(crate) fn decode_group_payload(
    payload: &[u8],
) -> Result<IcebergRewriteGroupPayloadV1, ConnectorError> {
    let decoded: IcebergRewriteGroupPayloadV1 =
        decode_canonical_json(payload, "Iceberg distributed rewrite group payload")?;
    if decoded.version != GROUP_PAYLOAD_VERSION {
        return Err(invalid(
            "Iceberg distributed rewrite group payload version is unsupported",
        ));
    }
    decode_digest(&decoded.group_digest_hex, "Iceberg rewrite group")?;
    decode_digest(&decoded.artifact_digest_hex, "Iceberg rewrite artifact")?;
    if decoded.artifact_location.is_empty()
        || decoded.artifact_location.len() > 16 * 1024
        || decoded.artifact_location.ends_with('/')
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact location is invalid",
        ));
    }
    Ok(decoded)
}

/// Reassemble and verify the bounded provider artifact before exposing a
/// group to the Iceberg scan planner. Generic core only carries the opaque
/// group payload; this function is the single Iceberg-only decoder.
pub(crate) fn load_frozen_rewrite_group(
    file_io: &novarocks_connector_iceberg::iceberg::io::FileIO,
    payload: &IcebergRewriteGroupPayloadV1,
) -> Result<IcebergFrozenRewriteGroupV1, ConnectorError> {
    let root_location = format!("{}/manifest.json", payload.artifact_location);
    let root_bytes = read_artifact_file(file_io, &root_location, REWRITE_ARTIFACT_MAX_ROOT_BYTES)?;
    let root: IcebergFrozenRewriteArtifactRootV1 =
        decode_canonical_json(&root_bytes, "Iceberg distributed rewrite artifact root")?;
    if root.version != ARTIFACT_VERSION
        || root.parts.is_empty()
        || root.parts.len() > REWRITE_ARTIFACT_MAX_PARTS
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact root is invalid",
        ));
    }
    let expected_digest = decode_digest(&payload.artifact_digest_hex, "Iceberg rewrite artifact")?;
    if root.logical_artifact_digest_hex != payload.artifact_digest_hex {
        return Err(invalid(
            "Iceberg distributed rewrite root has a foreign digest",
        ));
    }
    let mut groups = Vec::new();
    for (expected_index, part_ref) in root.parts.iter().enumerate() {
        if part_ref.index as usize != expected_index
            || part_ref.location
                != format!(
                    "{}/part-{expected_index:04}.json",
                    payload.artifact_location
                )
        {
            return Err(invalid(
                "Iceberg distributed rewrite part reference is invalid",
            ));
        }
        let bytes =
            read_artifact_file(file_io, &part_ref.location, REWRITE_ARTIFACT_MAX_PART_BYTES)?;
        if artifact_part_digest(&bytes)
            != decode_digest(&part_ref.digest_hex, "Iceberg rewrite part")?
        {
            return Err(invalid(
                "Iceberg distributed rewrite part digest is invalid",
            ));
        }
        let part: IcebergFrozenRewriteArtifactPartV1 =
            decode_canonical_json(&bytes, "Iceberg distributed rewrite artifact part")?;
        if part.version != ARTIFACT_VERSION || part.groups.is_empty() {
            return Err(invalid(
                "Iceberg distributed rewrite artifact part is invalid",
            ));
        }
        groups.extend(part.groups);
    }
    groups.sort_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex));
    if groups.len() > REWRITE_ARTIFACT_MAX_GROUPS
        || groups
            .windows(2)
            .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid(
            "Iceberg distributed rewrite artifact groups are invalid",
        ));
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
    if artifact_digest(&logical.canonical_bytes()?) != expected_digest {
        return Err(invalid(
            "Iceberg distributed rewrite artifact digest is invalid",
        ));
    }
    logical
        .groups
        .into_iter()
        .find(|group| group.group_digest_hex == payload.group_digest_hex)
        .ok_or_else(|| invalid("Iceberg distributed rewrite artifact has no requested group"))
}

fn read_artifact_file(
    file_io: &novarocks_connector_iceberg::iceberg::io::FileIO,
    location: &str,
    max_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let input = file_io
        .new_input(location)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    let bytes = block_on_iceberg(async move { input.read().await })
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    if bytes.len() > max_bytes {
        return Err(exhausted(format!(
            "Iceberg distributed rewrite artifact file exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

/// Store the immutable provider artifact in a small root plus bounded parts.
/// The digest intentionally covers the logical, reassembled artifact, not
/// storage paths or process-local catalog state.  A single oversized group is
/// rejected rather than silently widening the carrier or flattening groups.
fn write_frozen_artifact(
    file_io: novarocks_connector_iceberg::iceberg::io::FileIO,
    artifact: &IcebergFrozenRewriteArtifactV1,
    logical_digest: [u8; 32],
    root_location: &str,
) -> Result<(), ConnectorError> {
    let parts = split_artifact_parts(&artifact.groups)?;
    let mut refs = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        let bytes = canonical_json(part, "frozen Iceberg rewrite artifact part")?;
        debug_assert!(bytes.len() <= REWRITE_ARTIFACT_MAX_PART_BYTES);
        let digest = artifact_part_digest(&bytes);
        let location = format!("{root_location}/part-{index:04}.json");
        let output = file_io.new_output(&location).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        })?;
        block_on_iceberg(async move { output.write(bytes).await })
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })?
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
            })?;
        refs.push(IcebergFrozenRewriteArtifactPartRefV1 {
            index: index as u16,
            digest_hex: hex::encode(digest),
            location,
        });
    }
    let root = IcebergFrozenRewriteArtifactRootV1 {
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
        parts: refs,
    };
    let root_bytes = canonical_json(&root, "frozen Iceberg rewrite artifact root")?;
    if root_bytes.len() > REWRITE_ARTIFACT_MAX_ROOT_BYTES {
        return Err(exhausted("Iceberg rewrite artifact root exceeds 64 KiB"));
    }
    let output = file_io
        .new_output(&format!("{root_location}/manifest.json"))
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    block_on_iceberg(async move { output.write(root_bytes).await })
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string()))?;
    Ok(())
}

fn split_artifact_parts(
    groups: &[IcebergFrozenRewriteGroupV1],
) -> Result<Vec<IcebergFrozenRewriteArtifactPartV1>, ConnectorError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    for group in groups {
        let mut candidate = current.clone();
        candidate.push(group.clone());
        let candidate_part = IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: candidate,
        };
        if canonical_json(&candidate_part, "frozen Iceberg rewrite artifact part")?.len()
            <= REWRITE_ARTIFACT_MAX_PART_BYTES
        {
            current = candidate_part.groups;
            continue;
        }
        if current.is_empty() {
            return Err(exhausted(
                "Iceberg rewrite group exceeds the 1 MiB artifact-part limit",
            ));
        }
        parts.push(IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: std::mem::take(&mut current),
        });
        current.push(group.clone());
    }
    if !current.is_empty() {
        parts.push(IcebergFrozenRewriteArtifactPartV1 {
            version: ARTIFACT_VERSION,
            groups: current,
        });
    }
    if parts.len() > REWRITE_ARTIFACT_MAX_PARTS {
        return Err(exhausted(
            "Iceberg rewrite artifact exceeds the 64-part storage limit",
        ));
    }
    Ok(parts)
}

/// Build deterministic data-file rewrite groups.  A group owns every data
/// file it lists; delete dependencies remain nested under their owner file so
/// no delete file can cause cross-group ownership in a later aggregate commit.
pub(crate) fn plan_data_file_groups(
    files: Vec<DataFileWithStats>,
    live_delete_paths: &BTreeSet<String>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    let mut groups = group_data_files(files, false, None)?;
    assign_unattached_data_delete_owners(&mut groups, live_delete_paths)?;
    Ok(groups)
}

fn live_delete_file_paths(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<BTreeSet<String>, ConnectorError> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(BTreeSet::new());
    };
    let file_io = table.file_io();
    block_on_iceberg(async {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|error| format!("load Iceberg rewrite manifest list: {error}"))?;
        let mut paths = BTreeSet::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content
                != novarocks_connector_iceberg::iceberg::spec::ManifestContentType::Deletes
            {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|error| {
                    format!(
                        "load Iceberg rewrite delete manifest {}: {error}",
                        manifest_file.manifest_path
                    )
                })?;
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

/// Build deterministic deletion-vector rewrite groups.  Each group is keyed
/// by its referenced data file, and only V3 Puffin position-delete inputs are
/// selected.  V2 position deletes are intentionally not smuggled through this
/// route: the caller/provider must reject that table before staging.
pub(crate) fn plan_position_delete_groups(
    files: Vec<DataFileWithStats>,
    rewrite_all: bool,
    min_input_files: usize,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    let files = files
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    group_data_files(files, rewrite_all, Some(min_input_files))
}

fn group_data_files(
    mut files: Vec<IcebergDataFileInfo>,
    rewrite_all: bool,
    position_delete_min_inputs: Option<usize>,
) -> Result<Vec<IcebergFrozenRewriteGroupV1>, ConnectorError> {
    files.sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(min_inputs) = position_delete_min_inputs {
        let mut groups = Vec::new();
        for file in files {
            if file.delete_files.iter().any(|delete| {
                matches!(delete.file_content, IcebergDeleteFileContent::Position)
                    && matches!(delete.file_format, IcebergDeleteFileFormat::Parquet)
            }) {
                return Err(invalid(
                    "V2 Parquet position delete rewrite is not supported",
                ));
            }
            let selected = file
                .delete_files
                .iter()
                .filter(|delete| {
                    matches!(delete.file_content, IcebergDeleteFileContent::Position)
                        && matches!(delete.file_format, IcebergDeleteFileFormat::Puffin)
                })
                .map(|delete| delete.path.clone())
                .collect::<Vec<_>>();
            if selected.is_empty() || (!rewrite_all && selected.len() < min_inputs) {
                continue;
            }
            let group_digest = position_group_digest(&file.path, &selected);
            groups.push(IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(group_digest),
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
            let group_digest =
                data_group_digest(partition_spec_id, partition_key.as_deref(), &data_files);
            IcebergFrozenRewriteGroupV1 {
                group_digest_hex: hex::encode(group_digest),
                partition_spec_id,
                partition_key,
                data_files,
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    assign_data_delete_owners(&mut groups);
    bounded_groups(groups)
}

/// Assign every live delete file to one deterministic data-rewrite group.
/// Read sources retain all applicable delete dependencies; this map is used
/// only by the single aggregate replacement snapshot.
fn assign_data_delete_owners(groups: &mut [IcebergFrozenRewriteGroupV1]) {
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

/// A read snapshot attaches only delete files that still apply to a live data
/// file. A whole-table Replace must additionally remove every live delete
/// manifest entry, including an orphan left behind after COW replaced its
/// referenced data file. Assign those paths to the canonical first cohort so
/// the aggregate commit owns the exact provider-frozen live set.
fn assign_unattached_data_delete_owners(
    groups: &mut [IcebergFrozenRewriteGroupV1],
    live_delete_paths: &BTreeSet<String>,
) -> Result<(), ConnectorError> {
    let owned = groups
        .iter()
        .flat_map(|group| group.owned_data_delete_files.iter().cloned())
        .collect::<BTreeSet<_>>();
    if !owned.is_subset(live_delete_paths) {
        return Err(invalid(
            "Iceberg data rewrite dependencies include a non-live delete file",
        ));
    }
    let missing = live_delete_paths
        .difference(&owned)
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let owner = groups
        .iter_mut()
        .min_by(|left, right| left.group_digest_hex.cmp(&right.group_digest_hex))
        .ok_or_else(|| invalid("Iceberg data rewrite has live delete files but no data cohort"))?;
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
        return Err(exhausted("Iceberg rewrite exceeds the 4096 cohort limit"));
    }
    if groups
        .windows(2)
        .any(|pair| pair[0].group_digest_hex == pair[1].group_digest_hex)
    {
        return Err(invalid("Iceberg rewrite group digest collision"));
    }
    Ok(groups)
}

pub(crate) fn cohort_plans_from_artifact(
    request: &ConnectorDistributedRewritePlanningRequest,
    artifact_digest: [u8; 32],
    artifact_location: &str,
    groups: &[IcebergFrozenRewriteGroupV1],
    input_schema: SchemaRef,
    row_lineage_data: bool,
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
            let group_digest = decode_digest(&group.group_digest_hex, "Iceberg rewrite group")?;
            let cohort_id = ConnectorWriteCohortId::derive(
                request.operation_id(),
                b"iceberg-distributed-rewrite-group",
                group_digest,
            )?;
            let source_payload = IcebergRewriteGroupPayloadV1 {
                version: GROUP_PAYLOAD_VERSION,
                group_digest_hex: group.group_digest_hex.clone(),
                artifact_digest_hex: hex::encode(artifact_digest),
                artifact_location: artifact_location.to_string(),
            };
            let source = super::provider::frozen_rewrite_source_table_handle(
                request.operation().table(),
                request.operation(),
                source_payload,
            )?;
            let preparation = rewrite_cohort_preparation(
                request,
                intent,
                input_schema.as_ref(),
                row_lineage_data,
            )?;
            ConnectorDistributedRewriteCohortPlan::try_new(
                cohort_id,
                source,
                input_schema.clone(),
                arrow_schema_digest(&input_schema),
                preparation,
                group_digest,
            )
        })
        .collect()
}

fn rewrite_cohort_preparation(
    request: &ConnectorDistributedRewritePlanningRequest,
    intent: ConnectorWriteIntent,
    input_schema: &Schema,
    row_lineage_data: bool,
) -> Result<ConnectorWritePreparation, ConnectorError> {
    let fields = input_schema
        .fields()
        .iter()
        .map(|field| ConnectorWriteFieldRequest::new((**field).clone()))
        .collect::<Vec<_>>();
    let input = match request.operation() {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } if row_lineage_data => {
            let row_identity_fields = fields
                .iter()
                .filter(|field| {
                    matches!(
                        field.field().name().as_str(),
                        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL
                            | novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let data_fields = fields
                .into_iter()
                .filter(|field| {
                    !matches!(
                        field.field().name().as_str(),
                        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL
                            | novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL
                    )
                })
                .collect();
            ConnectorWriteInputRequest::RowLineage {
                data_fields,
                row_identity_fields,
            }
        }
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => {
            ConnectorWriteInputRequest::Data { fields }
        }
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            ConnectorWriteInputRequest::PositionDelete {
                identity_fields: fields,
                partition_source_fields: Vec::new(),
            }
        }
    };
    match super::provider::prepare_iceberg_write(
        ConnectorWritePreparationRequest {
            table: request.operation().table().clone(),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent,
            purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input,
            context: request.context.clone(),
        },
        request.owner(),
    )? {
        ConnectorWritePreparationOutcome::Prepared(preparation) => Ok(preparation),
        ConnectorWritePreparationOutcome::Denied(error) => Err(error),
    }
}

fn group_payload_from_artifact(
    group: &IcebergFrozenRewriteGroupV1,
    artifact_digest: [u8; 32],
    artifact_location: &str,
) -> Result<Bytes, ConnectorError> {
    canonical_payload(&IcebergRewriteGroupPayloadV1 {
        version: GROUP_PAYLOAD_VERSION,
        group_digest_hex: group.group_digest_hex.clone(),
        artifact_digest_hex: hex::encode(artifact_digest),
        artifact_location: artifact_location.to_string(),
    })
}

pub(crate) fn rewrite_input_schema(
    operation: &ConnectorDistributedRewriteOperation,
    physical_schema: SchemaRef,
    row_lineage_data: bool,
) -> SchemaRef {
    match operation {
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } if row_lineage_data => {
            let mut fields = physical_schema.fields().to_vec();
            fields.extend([
                Arc::new(Field::new(
                    novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
                    DataType::Int64,
                    true,
                )),
            ]);
            Arc::new(Schema::new(fields))
        }
        ConnectorDistributedRewriteOperation::RewriteDataFiles { .. } => physical_schema,
        ConnectorDistributedRewriteOperation::RewritePositionDeletes { .. } => {
            Arc::new(Schema::new(vec![
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
            ]))
        }
    }
}

pub(crate) fn artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn artifact_part_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.artifact-part.v1\0");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
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
    for delete in delete_paths {
        digest_bytes(&mut hash, delete.as_bytes());
    }
    hash.finalize().into()
}

fn arrow_schema_digest(schema: &SchemaRef) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"novarocks.iceberg.distributed-rewrite.arrow-schema.v1\0");
    digest_bytes(&mut hash, format!("{schema:?}").as_bytes());
    hash.finalize().into()
}

pub(crate) fn canonical_payload<T: Serialize>(value: &T) -> Result<Bytes, ConnectorError> {
    canonical_json(value, "Iceberg rewrite payload")
}

/// Canonical JSON v1 used for persisted provider artifacts and bounded SPI
/// envelopes. `serde_json` may inherit insertion order from a map source, so
/// it is not sufficient by itself for a digest-bearing artifact. Sorting at
/// every object level also covers `IcebergDataFileInfo::column_stats`.
fn canonical_json<T: Serialize>(value: &T, label: &str) -> Result<Bytes, ConnectorError> {
    let value = serde_json::to_value(value)
        .map_err(|error| internal(format!("encode {label}: {error}")))?;
    let mut out = Vec::new();
    write_canonical_json(&value, &mut out)?;
    Ok(Bytes::from(out))
}

fn decode_canonical_json<T>(payload: &[u8], label: &str) -> Result<T, ConnectorError>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let decoded = serde_json::from_slice(payload)
        .map_err(|error| invalid(format!("decode {label}: {error}")))?;
    if canonical_json(&decoded, label)?.as_ref() != payload {
        return Err(invalid(format!("{label} is not canonical JSON v1")));
    }
    Ok(decoded)
}

fn write_canonical_json(value: &Value, out: &mut Vec<u8>) -> Result<(), ConnectorError> {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        Value::String(string) => {
            let encoded = serde_json::to_string(string)
                .map_err(|error| internal(format!("encode canonical JSON string: {error}")))?;
            out.extend_from_slice(encoded.as_bytes());
        }
        Value::Array(values) => {
            out.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical_json(value, out)?;
            }
            out.push(b']');
        }
        Value::Object(values) => {
            out.push(b'{');
            let mut sorted = values.iter().collect::<Vec<_>>();
            sorted.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                let encoded = serde_json::to_string(key)
                    .map_err(|error| internal(format!("encode canonical JSON key: {error}")))?;
                out.extend_from_slice(encoded.as_bytes());
                out.push(b':');
                write_canonical_json(value, out)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}

fn decode_digest(value: &str, context: &str) -> Result<[u8; 32], ConnectorError> {
    let bytes =
        hex::decode(value).map_err(|error| invalid(format!("decode {context} digest: {error}")))?;
    bytes
        .try_into()
        .map_err(|_| invalid(format!("{context} digest has invalid length")))
}

fn digest_bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message)
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_connector_iceberg::scan_model::{
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    };

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

        let groups = group_data_files(vec![c, b, a], false, None).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.data_files.len())
                .sum::<usize>(),
            3
        );
        assert!(
            groups
                .iter()
                .all(|group| group.selected_position_delete_files.is_empty())
        );
        for group in groups {
            assert!(
                group
                    .data_files
                    .windows(2)
                    .all(|pair| pair[0].path < pair[1].path)
            );
        }
    }

    #[test]
    fn data_rewrite_assigns_each_shared_delete_file_one_canonical_owner() {
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
        a.partition_key = Some("{\"day\":1}".to_string());
        a.delete_files.push(delete.clone());
        let mut b = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 10, 1);
        b.partition_key = Some("{\"day\":2}".to_string());
        b.delete_files.push(delete);

        let groups = group_data_files(vec![b, a], false, None).expect("frozen groups");
        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .flat_map(|group| group.owned_data_delete_files.iter())
                .filter(|path| path.as_str() == "s3://bucket/shared-delete.parquet")
                .count(),
            1
        );
    }

    #[test]
    fn data_rewrite_assigns_orphan_live_delete_to_a_canonical_cohort() {
        let data = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let mut groups = group_data_files(vec![data], false, None).expect("frozen groups");
        assign_unattached_data_delete_owners(
            &mut groups,
            &BTreeSet::from(["s3://bucket/orphan.puffin".to_string()]),
        )
        .expect("orphan owner");

        assert_eq!(
            groups[0].owned_data_delete_files,
            vec!["s3://bucket/orphan.puffin".to_string()]
        );
    }

    #[test]
    fn position_rewrite_rejects_v2_parquet_delete_before_puffin_planning() {
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

        let error = group_data_files(vec![data], true, Some(1)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("V2 Parquet position delete rewrite is not supported")
        );
    }

    #[test]
    fn artifact_digest_is_content_addressed() {
        assert_eq!(artifact_digest(b"same"), artifact_digest(b"same"));
        assert_ne!(artifact_digest(b"same"), artifact_digest(b"different"));
    }

    #[test]
    fn canonical_json_sorts_nested_map_keys() {
        let mut first = HashMap::new();
        first.insert("z".to_string(), vec![1_u8]);
        first.insert("a".to_string(), vec![2_u8]);
        let mut second = HashMap::new();
        second.insert("a".to_string(), vec![2_u8]);
        second.insert("z".to_string(), vec![1_u8]);
        assert_eq!(
            canonical_json(&first, "test").unwrap(),
            canonical_json(&second, "test").unwrap()
        );
    }

    #[test]
    fn artifact_parts_never_cross_the_fixed_part_limit() {
        let groups = (0..3)
            .map(|index| IcebergFrozenRewriteGroupV1 {
                group_digest_hex: format!("{index:064x}"),
                partition_spec_id: None,
                partition_key: None,
                data_files: vec![IcebergDataFileInfo::for_test(
                    &format!("file:///warehouse/{index}.parquet"),
                    1,
                    1,
                )],
                selected_position_delete_files: Vec::new(),
                owned_data_delete_files: Vec::new(),
            })
            .collect::<Vec<_>>();
        let parts = split_artifact_parts(&groups).unwrap();
        assert_eq!(parts.len(), 1);
        assert!(
            canonical_json(&parts[0], "test").unwrap().len() <= REWRITE_ARTIFACT_MAX_PART_BYTES
        );
    }

    #[test]
    fn rewrite_attempt_artifact_round_trips_exact_writer_reports() {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.rewrite").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
        };
        let operation_id = novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([7; 16]);
        let cohort_id = ConnectorWriteCohortId::from_bytes([8; 32]);
        let execution_id = novarocks_spi::connector::ConnectorWriteExecutionId::new([6; 16], 3);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            cohort_id,
            execution_id,
            [5; 16],
            4,
            2,
            1,
            owner.clone(),
        );
        let report = ConnectorStagedReport::try_new(
            writer,
            1,
            ConnectorWriterTerminalState::Staged,
            ConnectorStagedReportSummary {
                input_rows: 11,
                staged_bytes: 17,
                artifact_count: 1,
            },
            Bytes::from_static(b"opaque-iceberg-report"),
        )
        .unwrap();
        let completion = ConnectorWriteAttemptCompletion::try_new(
            owner,
            operation_id,
            cohort_id,
            execution_id,
            [4; 32],
            vec![report],
            Bytes::from_static(b"opaque-control"),
        )
        .unwrap();
        let encoded = encode_attempt_artifact(&completion).unwrap();
        let decoded = decode_attempt_artifact(&encoded).unwrap();
        assert_eq!(decoded, completion);
    }

    #[test]
    fn rewrite_attempt_artifact_rejects_trailing_bytes() {
        let mut artifact = ATTEMPT_ARTIFACT_MAGIC.to_vec();
        artifact.extend_from_slice(&ATTEMPT_ARTIFACT_VERSION.to_be_bytes());
        artifact.push(0);
        assert!(decode_attempt_artifact(&artifact).is_err());
    }
}
