// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Generation-local Iceberg control capabilities.
//!
//! This module is the provider implementation behind one frontend control
//! binding. It owns opaque table payloads and uses only the catalog client and
//! runtime injected into that exact generation.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::{Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorCatalogMutation, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationRequest, ConnectorCommittedVersion, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorListNamespacesRequest, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorNamespaceIdentity, ConnectorNamespaceRequest,
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorReadNamedReference,
    ConnectorReadPurpose, ConnectorReadReferenceFacts, ConnectorReadReferenceFactsRequest,
    ConnectorReadReferenceKind, ConnectorReadSelector, ConnectorReadSnapshotLogEntry,
    ConnectorRefAction, ConnectorRefKind, ConnectorScalarType, ConnectorScalarValue, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorScanSelection, ConnectorSplit,
    ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStagedPublicationBaseFact, ConnectorStagedPublicationCleanupReceipt,
    ConnectorStagedPublicationCleanupRequest, ConnectorStagedPublicationDescriptor,
    ConnectorStagedPublicationDisposition, ConnectorStagedPublicationObservation,
    ConnectorStagedPublicationProof, ConnectorStagedPublicationRecovery,
    ConnectorStaticComparisonOp, ConnectorStaticPredicate, ConnectorStaticPredicateKind,
    ConnectorTableDefinitionFacts, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTablePlanningFacts, ConnectorTableRequest,
    ConnectorTableResolution, DropPolicy, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, validate_static_predicates,
};
use serde::{Deserialize, Serialize};

use crate::commit::write_control::operation_marker_partitioning;
use crate::control_runtime::IcebergControlRuntime;
use crate::file_reader::execution_payload::{
    ICEBERG_SPLIT_V5, IcebergFrozenScanUnitPayload, IcebergMetadataSplitPayloadV1,
    IcebergScanFactColumnV1, SplitPayload, canonical_split_name_mapping,
    materialize_local_scan_units, scan_fact_scalar_type,
};
use crate::manifest::{
    data_file_with_stats_to_iceberg_data_file_info, extract_data_files_with_stats_at,
};
use crate::metadata_batch_reader::{
    MetadataTableType, metadata_output_schema, metadata_table_output_columns,
};
use crate::planning_facts::{IcebergTablePlanningFactsInput, table_planning_facts};
use crate::reconcile_payload::{
    ICEBERG_STAGED_PUBLICATION_PROOF_VERSION, IcebergStagedPublicationProofV1,
    decode_staged_publication_proof, encode_staged_publication_proof,
};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain,
    IcebergPhysicalPredicateOp, IcebergPhysicalPredicateValue, IcebergTableInfo,
};
use crate::schema_facts::{iceberg_schema_def, row_lineage_enabled};

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
const APPLY_KEY_COLUMN_PROPERTY: &str = "novarocks.mv.apply-key-column";
const HIDDEN_COLUMNS_PROPERTY: &str = "novarocks.mv.hidden-columns";

#[derive(Clone)]
pub struct IcebergControlProvider {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    binding_key: ConnectorExecutionBindingKey,
    runtime: Arc<IcebergControlRuntime>,
    recovery_cleanup_outcomes:
        Arc<Mutex<HashMap<ConnectorMutationOperationId, IcebergRecoveryCleanupRecord>>>,
}

#[derive(Clone)]
struct IcebergRecoveryCleanupRecord {
    outcome: ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>,
    proof: IcebergStagedPublicationProofV1,
    descriptor_digest: [u8; 32],
    observation_digest: [u8; 32],
}

impl IcebergControlProvider {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Self {
        let binding_key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        Self {
            descriptor,
            incarnation,
            binding_key,
            runtime,
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    pub(crate) fn runtime(&self) -> &Arc<IcebergControlRuntime> {
        &self.runtime
    }

    fn validate_context(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn ensure_owner(&self, instance_id: &ConnectorInstanceId) -> Result<(), ConnectorError> {
        if instance_id != &self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg control request belongs to another connector instance",
            ));
        }
        Ok(())
    }

    pub(crate) fn table_payload(
        &self,
        table: &ConnectorTableHandle,
    ) -> Result<IcebergTablePayload, ConnectorError> {
        self.ensure_owner(table.owner())?;
        decode_payload(table.payload(), "table handle")
    }

    pub(crate) fn staged_write_table_handle(
        &self,
        table: &crate::iceberg::table::Table,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorTableHandle, ConnectorError> {
        self.validate_context(context)?;
        let metadata = table.metadata();
        let ident = table.identifier();
        let payload = IcebergTablePayload {
            namespace: ident.namespace.to_url_string(),
            table: ident.name.clone(),
            table_info: Some(IcebergTableInfo {
                catalog: self.descriptor.instance_id.as_str().to_string(),
                namespace: ident.namespace.to_url_string(),
                table: ident.name.clone(),
                table_uuid: Some(metadata.uuid().to_string()),
                current_snapshot_id: metadata.current_snapshot_id(),
                schema_id: metadata.current_schema_id(),
                location: metadata.location().to_string(),
                schema: iceberg_schema_def(metadata.current_schema()),
                serialized_metadata: Some(serde_json::to_string(metadata).map_err(|error| {
                    corrupt(format!("serialize staged Iceberg table metadata: {error}"))
                })?),
                serialized_metadata_rows: None,
            }),
            metadata_columns: metadata_column_names(metadata),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: logical_type_columns(metadata.properties()),
            hidden_columns: hidden_internal_columns(metadata.properties()),
        };
        ConnectorTableHandle::try_new(
            self.descriptor.instance_id.clone(),
            encode_payload(
                &payload,
                "staged write table handle",
                context.max_handle_payload_bytes(),
            )?,
        )
    }

    fn scan_payload(
        &self,
        scan: &ConnectorScanHandle,
    ) -> Result<IcebergScanPayload, ConnectorError> {
        self.ensure_owner(scan.owner())?;
        decode_payload(scan.payload(), "scan handle")
    }
}

impl ConnectorMetadata for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn list_namespaces(
        &self,
        request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.instance_id)?;
        self.runtime
            .list_namespaces()
            .map_err(unavailable)?
            .into_iter()
            .map(|namespace| {
                Ok(ConnectorNamespaceIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: Arc::from(namespace),
                })
            })
            .collect()
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.namespace.instance_id)?;
        self.runtime
            .namespace_exists(&request.namespace.namespace)
            .map_err(unavailable)
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let (table, metadata_type) =
            resolve_table_request(&request.table.table, request.resolution)?;
        if metadata_type.is_some() {
            return self
                .runtime
                .table_exists(&request.table.namespace, &table)
                .map_err(unavailable);
        }
        self.runtime
            .table_exists(&request.table.namespace, &table)
            .map_err(unavailable)
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.namespace.instance_id)?;
        self.runtime
            .list_tables(&request.namespace.namespace)
            .map_err(unavailable)?
            .into_iter()
            .map(|table| {
                Ok(ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
            })
            .collect()
    }

    fn read_reference_facts(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let loaded = self
            .runtime
            .load_table(&request.table.namespace, &request.table.table)
            .map_err(unavailable)?;
        read_reference_facts(loaded.table.metadata(), &request.context)
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let (table_name, metadata_table_type) =
            resolve_table_request(&request.table.table, request.resolution)?;
        let loaded = self
            .runtime
            .load_table(&request.table.namespace, &table_name)
            .map_err(unavailable)?;
        let metadata = loaded.table.metadata();
        let definition_schema = metadata.current_schema().clone();
        let table_comment = metadata.properties().get("comment").cloned();
        let mut base_schema = Arc::new(
            crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
                .map_err(|error| corrupt(format!("convert Iceberg schema to Arrow: {error}")))?,
        );
        let hidden_columns = hidden_internal_columns(metadata.properties());
        base_schema = annotate_hidden_fields(base_schema, &hidden_columns);
        let logical_type_columns = logical_type_columns(metadata.properties());
        let metadata_columns = metadata_column_names(metadata);
        let mut table_info =
            IcebergTableInfo {
                catalog: self.descriptor.instance_id.as_str().to_string(),
                namespace: request.table.namespace.to_string(),
                table: table_name.clone(),
                table_uuid: Some(metadata.uuid().to_string()),
                current_snapshot_id: metadata.current_snapshot_id(),
                schema_id: metadata.current_schema_id(),
                location: metadata.location().to_string(),
                schema: iceberg_schema_def(metadata.current_schema()),
                serialized_metadata: Some(serde_json::to_string(metadata).map_err(|error| {
                    corrupt(format!("serialize Iceberg table metadata: {error}"))
                })?),
                serialized_metadata_rows: None,
            };
        let mut prepared_files = Vec::new();
        if matches!(metadata_table_type, Some(MetadataTableType::Partitions)) {
            if let Some(snapshot_id) = metadata.current_snapshot_id() {
                let table = loaded.table.clone();
                prepared_files = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(
                        async move { extract_data_files_with_stats_at(&table, snapshot_id).await },
                    )
                    .map_err(unavailable)?
                    .map_err(unavailable)?
                    .into_iter()
                    .map(data_file_with_stats_to_iceberg_data_file_info)
                    .collect();
            }
        }
        if matches!(
            metadata_table_type,
            Some(
                MetadataTableType::Files
                    | MetadataTableType::Manifests
                    | MetadataTableType::LogicalIcebergMetadata
            )
        ) {
            let table = loaded.table.clone();
            let file_io = table.file_io().clone();
            let metadata_read_type =
                metadata_read_type(metadata_table_type.expect("metadata table type is present"))?;
            table_info.serialized_metadata_rows = Some(
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        crate::metadata_read::read_metadata_table_rows(
                            &table,
                            &file_io,
                            metadata_read_type,
                        )
                        .await
                    })
                    .map_err(unavailable)?
                    .map_err(unavailable)?,
            );
        }
        let payload = IcebergTablePayload {
            namespace: request.table.namespace.to_string(),
            table: table_name.clone(),
            table_info: Some(table_info),
            metadata_columns,
            metadata_table_type,
            prepared_files,
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns,
            hidden_columns,
        };
        let schema = if let Some(metadata_table_type) = payload.metadata_table_type {
            let columns =
                metadata_table_output_columns(metadata_table_type, metadata).map_err(corrupt)?;
            metadata_output_schema(&columns).map_err(corrupt)?
        } else {
            base_schema
        };
        let planning_facts = if payload.metadata_table_type.is_some() {
            ConnectorTablePlanningFacts::empty()
        } else {
            table_planning_facts(IcebergTablePlanningFactsInput {
                schema: &schema,
                iceberg_schema: Some(definition_schema.as_ref()),
                metadata_columns: &payload.metadata_columns,
                hidden_columns: &payload.hidden_columns,
                logical_type_columns: &payload.logical_type_columns,
                serialized_metadata: payload
                    .table_info
                    .as_ref()
                    .and_then(|table| table.serialized_metadata.as_deref()),
                namespace: &request.table.namespace,
                instance_id: &self.descriptor.instance_id,
                context: &request.context,
            })?
        };
        let definition_facts = if payload.metadata_table_type.is_some() {
            ConnectorTableDefinitionFacts::empty()
        } else {
            crate::table_definition::table_definition_facts(
                &definition_schema,
                &schema,
                &planning_facts,
                table_comment.as_deref(),
                &request.context,
            )?
        };
        let statistics_data_version = crate::statistics_codec::statistics_data_version(
            &metadata.uuid().to_string(),
            metadata.current_snapshot_id(),
        )?;
        Ok(ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: self.descriptor.instance_id.clone(),
                namespace: request.table.namespace,
                table: Arc::from(table_name),
            },
            schema,
            planning_facts,
            definition_facts,
            version: Some(Bytes::copy_from_slice(
                &metadata.current_schema_id().to_le_bytes(),
            )),
            statistics_data_version: Some(statistics_data_version),
            table: ConnectorTableHandle::try_new(
                self.descriptor.instance_id.clone(),
                encode_payload(
                    &payload,
                    "table handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
        })
    }
}

impl ConnectorScanPlanning for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.validate_context(&request.context)?;
        validate_static_predicates(&request.static_predicates)?;
        let table = self.table_payload(table)?;
        let output_schema = if table.metadata_table_type.is_some() {
            projected_metadata_schema(&table, &request.projection)?
        } else {
            projected_schema(&table, &request.projection)?
        };
        if let ConnectorScanSelection::ChangeWindow(window) = request.selection {
            if table.metadata_table_type.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg metadata aliases do not support change-window scans",
                ));
            }
            let table_info = table.table_info.as_ref().ok_or_else(|| {
                corrupt("Iceberg change-window scan is missing its resolved table pin")
            })?;
            if table_info.current_snapshot_id != Some(window.to_inclusive()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg change-window upper endpoint does not match the exact table pin",
                ));
            }
            let physical = self
                .runtime
                .load_table(&table.namespace, &table.table)
                .map_err(unavailable)?;
            let metadata = physical.table.metadata();
            let metadata_uuid = metadata.uuid().to_string();
            if metadata.current_snapshot_id() != Some(window.to_inclusive())
                || table_info.table_uuid.as_deref() != Some(metadata_uuid.as_str())
                || table_info.location != metadata.location()
            {
                return Err(corrupt(
                    "Iceberg change-window scan drifted from its exact table pin",
                ));
            }
            let (admission, batch) = crate::change_planning::plan_change_window(
                &physical.table,
                window.from_exclusive(),
                window.to_inclusive(),
                self.runtime.resources().catalog_runtime(),
                &request.context,
            )?;
            let delta = if matches!(
                admission,
                novarocks_spi::connector::ConnectorChangeWindowAdmission::FullRebuild(_)
            ) {
                None
            } else {
                Some(crate::change_planning::freeze_delta_scan_plan(
                    &physical.table,
                    &batch,
                    self.runtime.resources().catalog_runtime(),
                    self.runtime.resources().planning_binding(),
                    &request.context,
                )?)
            };
            let predicate_dispositions = request
                .static_predicates
                .iter()
                .map(|predicate| ConnectorPredicateDisposition {
                    predicate_id: predicate.id,
                    kind: ConnectorPredicateDispositionKind::Unsupported,
                })
                .collect();
            let fact_columns = scan_fact_columns(&output_schema, &request.projection, &table)?;
            let payload = IcebergScanPayload {
                table,
                snapshot_id: Some(window.to_inclusive()),
                table_uuid: Some(metadata_uuid),
                projection: request.projection,
                limit: request.limit,
                purpose: request.purpose.into(),
                fact_columns,
                physical_predicates: Vec::new(),
                mode: IcebergScanModeV1::ChangeWindow { delta },
            };
            return ConnectorScan::try_new_change_window(
                ConnectorExecutionBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.incarnation,
                },
                window,
                admission,
                ConnectorScanHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    encode_payload(
                        &payload,
                        "scan handle",
                        request.context.max_handle_payload_bytes(),
                    )?,
                )?,
                output_schema,
                predicate_dispositions,
                &request.context,
            );
        }

        let ConnectorScanSelection::Snapshot(selector) = request.selection else {
            unreachable!("change-window scans return above")
        };
        let (snapshot_id, table_uuid) = match selector {
            ConnectorReadSelector::Current => {
                let table_info = table.table_info.as_ref().ok_or_else(|| {
                    corrupt("Iceberg current scan is missing its resolved table pin")
                })?;
                (
                    table_info.current_snapshot_id,
                    table_info.table_uuid.clone(),
                )
            }
            selector => {
                let physical = self
                    .runtime
                    .load_table(&table.namespace, &table.table)
                    .map_err(unavailable)?;
                (
                    select_snapshot(physical.table.metadata(), selector)?,
                    Some(physical.table.metadata().uuid().to_string()),
                )
            }
        };
        let (physical_predicates, predicate_dispositions) =
            negotiate_static_predicates(&table, &request.static_predicates);
        let fact_columns = if table.metadata_table_type.is_some() {
            Vec::new()
        } else {
            scan_fact_columns(&output_schema, &request.projection, &table)?
        };
        let payload = IcebergScanPayload {
            table,
            snapshot_id,
            table_uuid,
            projection: request.projection,
            limit: request.limit,
            purpose: request.purpose.into(),
            fact_columns,
            physical_predicates,
            mode: IcebergScanModeV1::Snapshot,
        };
        ConnectorScan::try_new_snapshot(
            ConnectorExecutionBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            },
            selector,
            ConnectorScanHandle::try_new(
                self.descriptor.instance_id.clone(),
                encode_payload(
                    &payload,
                    "scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
            output_schema,
            predicate_dispositions,
        )
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        self.validate_context(&request.context)?;
        let scan = self.scan_payload(scan)?;
        if scan.table.metadata_table_type.is_some() {
            return self.plan_metadata_splits(scan, request);
        }
        if let IcebergScanModeV1::ChangeWindow { delta } = &scan.mode {
            return self.plan_change_window_splits(&scan, delta.as_ref(), request);
        }
        let files = self.scan_files(&scan)?;
        if !matches!(scan.purpose, IcebergReadPurposeV1::Query)
            && files.iter().any(|file| {
                file.delete_files.iter().any(|delete| {
                    delete.file_content == crate::scan_model::IcebergDeleteFileContent::Equality
                })
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg MV target scan does not support equality deletes yet",
            ));
        }
        crate::planning_facts::validate_planned_files(scan.table.table_info.as_ref(), &files)?;
        let candidate_units_considered = u64::try_from(files.len()).unwrap_or(u64::MAX);
        // Prune only once the pinned snapshot is fully assembled. Delete-file
        // applicability was resolved above and must never be derived from a
        // predicate-selected view of the snapshot.
        let files = files
            .into_iter()
            .filter(|file| {
                crate::file_pruning::file_may_satisfy_physical_predicates(
                    file,
                    &scan.physical_predicates,
                )
            })
            .collect::<Vec<_>>();
        let candidate_units_pruned = candidate_units_considered
            .saturating_sub(u64::try_from(files.len()).unwrap_or(u64::MAX));
        let name_mapping = split_name_mapping(&scan.table)?;
        let mut remaining = scan.limit;
        let mut leaves = Vec::new();
        for file in files {
            if let Some(rows) = remaining.as_mut() {
                if *rows == 0 {
                    break;
                }
                if let Some(row_count) = file.row_count.and_then(|value| u64::try_from(value).ok())
                {
                    *rows = rows.saturating_sub(row_count);
                }
            }
            let estimated_bytes = u64::try_from(file.size).map_err(|_| {
                corrupt(format!(
                    "Iceberg data file {} has a negative size",
                    file.path
                ))
            })?;
            leaves.push(IcebergFrozenScanUnitPayload {
                data_file: file,
                row_groups: None,
                estimated_bytes: Some(estimated_bytes),
            });
        }
        let leaves = materialize_local_scan_units(
            self.runtime.resources().planning_binding(),
            leaves,
            false,
            &novarocks_spi::connector::ConnectorPrepareSplitRequest {
                context: request.context.clone(),
            },
        )?;
        let scan_units_planned = u64::try_from(leaves.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg scan unit count overflows u64",
            )
        })?;
        let total_leaf_bytes = leaves
            .iter()
            .try_fold(0_u64, |total, leaf| {
                total.checked_add(leaf.estimated_bytes.unwrap_or(0))
            })
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg split cost overflowed",
                )
            })?;
        let target_bytes = request
            .max_split_bytes
            .map(NonZeroU64::get)
            .unwrap_or_else(|| {
                total_leaf_bytes
                    .checked_add(request.target_parallelism.get() as u64 - 1)
                    .and_then(|bytes| bytes.checked_div(request.target_parallelism.get() as u64))
                    .unwrap_or(u64::MAX)
                    .max(1)
            });
        let hard_limit = request.max_split_bytes.map(NonZeroU64::get);
        let mut splits = Vec::new();
        let mut total_payload_bytes = 0_usize;
        let mut pending = Vec::new();
        let mut pending_bytes = 0_u64;
        for leaf in leaves {
            let leaf_bytes = leaf.estimated_bytes.unwrap_or(0);
            if hard_limit.is_some_and(|limit| leaf_bytes > limit) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!(
                        "Iceberg physical leaf {} exceeds split byte limit",
                        leaf.data_file.path
                    ),
                ));
            }
            let full = pending.len()
                >= novarocks_spi::connector::MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT;
            let over_bytes = !pending.is_empty()
                && pending_bytes
                    .checked_add(leaf_bytes)
                    .is_none_or(|value| value > target_bytes);
            if full || over_bytes {
                push_data_split(
                    self,
                    &scan,
                    &name_mapping,
                    &mut splits,
                    &mut total_payload_bytes,
                    std::mem::take(&mut pending),
                    pending_bytes,
                    &request.context,
                )?;
                pending_bytes = 0;
            }
            pending_bytes = pending_bytes.checked_add(leaf_bytes).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg split cost overflowed",
                )
            })?;
            pending.push(leaf);
        }
        if !pending.is_empty() {
            push_data_split(
                self,
                &scan,
                &name_mapping,
                &mut splits,
                &mut total_payload_bytes,
                pending,
                pending_bytes,
                &request.context,
            )?;
        }
        let composite_splits_planned = u64::try_from(splits.len()).unwrap_or(u64::MAX);
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered,
                candidate_units_pruned,
                composite_splits_planned,
                scan_units_planned,
            },
        )
    }
}

impl ConnectorStagedPublicationRecovery for IcebergControlProvider {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn inspect(
        &self,
        descriptor: ConnectorStagedPublicationDescriptor,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorStagedPublicationObservation, ConnectorError> {
        self.validate_context(&context)?;
        descriptor.validate()?;
        if descriptor.table.instance_id != self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged publication descriptor belongs to another Iceberg instance",
            ));
        }
        self.runtime
            .control_state()
            .invalidate_table_cache(&descriptor.table.namespace, &descriptor.table.table);
        let loaded = self
            .runtime
            .load_table(&descriptor.table.namespace, &descriptor.table.table)
            .map_err(unavailable)?;
        let metadata = loaded.table.metadata();
        let staging_snapshot_id = metadata
            .refs()
            .get(descriptor.staging_ref.as_ref())
            .map(|reference| reference.snapshot_id);
        let target_snapshot_id = if descriptor.target_ref.as_ref() == "main" {
            metadata.current_snapshot_id()
        } else {
            metadata
                .refs()
                .get(descriptor.target_ref.as_ref())
                .map(|reference| reference.snapshot_id)
        };
        let marker = crate::commit::MvRefreshSnapshotMarker {
            refresh_id: descriptor.refresh_id,
            mv_id: descriptor.mv_id,
            token: descriptor.marker_token.to_string(),
        };
        let marker_for = |snapshot_id: i64| {
            metadata
                .snapshot_by_id(snapshot_id)
                .is_some_and(|snapshot| {
                    crate::commit::snapshot_matches_refresh_marker(snapshot, &marker)
                })
        };
        let target_ancestors = staged_publication_target_ancestors(metadata, target_snapshot_id);
        let target_marker_snapshot_id = target_ancestors
            .iter()
            .copied()
            .filter(|snapshot_id| marker_for(*snapshot_id))
            .try_fold(None, |matched, snapshot_id| match matched {
                None => Ok(Some(snapshot_id)),
                Some(_) => Err(corrupt(
                    "Iceberg target lineage contains multiple matching MV refresh markers",
                )),
            })?;
        let disposition = staged_publication_disposition(
            staging_snapshot_id,
            target_snapshot_id,
            target_marker_snapshot_id,
            staging_snapshot_id.is_some_and(marker_for),
            staging_snapshot_id.is_some_and(|staging| target_ancestors.contains(&staging)),
        );
        let observed_snapshot = match disposition {
            ConnectorStagedPublicationDisposition::Published
            | ConnectorStagedPublicationDisposition::CleanupPending => target_marker_snapshot_id,
            ConnectorStagedPublicationDisposition::Superseded => {
                target_marker_snapshot_id.or(staging_snapshot_id)
            }
            ConnectorStagedPublicationDisposition::Staged => staging_snapshot_id,
            ConnectorStagedPublicationDisposition::KnownUncommitted
            | ConnectorStagedPublicationDisposition::Ambiguous => None,
        };
        let (
            committed_version,
            resulting_row_count,
            bases,
            definition_fingerprint,
            committed_partitioning,
        ) = if matches!(
            disposition,
            ConnectorStagedPublicationDisposition::Published
                | ConnectorStagedPublicationDisposition::Superseded
                | ConnectorStagedPublicationDisposition::CleanupPending
        ) {
            let snapshot_id = observed_snapshot
                .ok_or_else(|| corrupt("published MV recovery observation has no snapshot"))?;
            let snapshot = metadata
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| corrupt("published MV recovery snapshot is missing"))?;
            let provenance = crate::commit::MvProvenanceV1::from_snapshot_summary(snapshot)
                .map_err(corrupt)?
                .filter(|provenance| {
                    provenance.refresh_id == descriptor.refresh_id
                        && provenance.mv_id == descriptor.mv_id
                        && provenance.token == descriptor.marker_token.as_ref()
                })
                .ok_or_else(|| {
                    corrupt("published MV recovery snapshot lacks matching provenance")
                })?;
            let total_records = snapshot
                .summary()
                .additional_properties
                .get("total-records")
                .ok_or_else(|| corrupt("published MV recovery snapshot lacks total-records"))?
                .parse::<u64>()
                .map_err(|error| {
                    corrupt(format!(
                        "published MV recovery has invalid total-records: {error}"
                    ))
                })?;
            if provenance.rows < 0
                || (provenance.rows != 0 && provenance.rows as u64 != total_records)
            {
                return Err(corrupt(
                    "published MV recovery provenance rows conflict with total-records",
                ));
            }
            let bases = provenance
                .bases
                .into_iter()
                .map(|base| ConnectorStagedPublicationBaseFact {
                    table: Arc::from(base.table_fqn),
                    uuid: Arc::from(base.uuid),
                    from_version: base.from_snapshot,
                    to_version: base.to_snapshot,
                })
                .collect::<Vec<_>>();
            let version = ConnectorCommittedVersion::try_new(
                Bytes::from(format!("iceberg/recovery/v1/{snapshot_id}")),
                Some(snapshot_id),
            )?;
            let committed_partitioning = operation_marker_partitioning(snapshot, metadata)?;
            (
                Some(version),
                Some(total_records),
                bases,
                Some(Arc::from(provenance.definition_fingerprint)),
                committed_partitioning,
            )
        } else {
            (None, None, Vec::new(), None, None)
        };
        let proof = IcebergStagedPublicationProofV1 {
            version: ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            namespace: descriptor.table.namespace.to_string(),
            table: descriptor.table.table.to_string(),
            table_uuid: metadata.uuid().to_string(),
            staging_ref: descriptor.staging_ref.to_string(),
            staging_snapshot_id,
            target_ref: descriptor.target_ref.to_string(),
            target_snapshot_id,
            refresh_id: descriptor.refresh_id,
            mv_id: descriptor.mv_id,
            marker_token: descriptor.marker_token.to_string(),
        };
        let proof = encode_staged_publication_proof(&proof)
            .map(Bytes::from)
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        let proof = ConnectorStagedPublicationProof::try_new(proof)?;
        match committed_partitioning {
            Some(partitioning) => {
                ConnectorStagedPublicationObservation::try_new_with_committed_partitioning(
                    disposition,
                    committed_version,
                    resulting_row_count,
                    bases,
                    definition_fingerprint,
                    staging_snapshot_id,
                    target_snapshot_id,
                    partitioning,
                    staging_snapshot_id.is_some(),
                    proof,
                )
            }
            None => ConnectorStagedPublicationObservation::try_new(
                disposition,
                committed_version,
                resulting_row_count,
                bases,
                definition_fingerprint,
                staging_snapshot_id,
                target_snapshot_id,
                staging_snapshot_id.is_some(),
                proof,
            ),
        }
    }

    fn cleanup(
        &self,
        request: ConnectorStagedPublicationCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&request.context)?;
        request.observation.validate()?;
        if let Some(outcome) = self
            .recovery_cleanup_outcomes
            .lock()
            .map_err(recovery_cleanup_lock_error)?
            .get(&request.operation_id)
            .map(|record| record.outcome.clone())
        {
            return Ok(outcome);
        }
        let proof = decode_staged_publication_proof(request.observation.proof.payload()).map_err(
            |error| {
                corrupt(format!(
                    "invalid Iceberg staged publication cleanup proof: {error}"
                ))
            },
        )?;
        if proof.version != ICEBERG_STAGED_PUBLICATION_PROOF_VERSION
            || proof.descriptor_digest.as_slice() != request.descriptor_digest
            || proof.staging_snapshot_id != request.observation.staging_snapshot_id
            || proof.staging_snapshot_id.is_none()
        {
            return Err(corrupt(
                "Iceberg staged publication cleanup proof conflicts with observation",
            ));
        }
        let table = ConnectorTableIdentity {
            instance_id: self.descriptor.instance_id.clone(),
            namespace: Arc::from(proof.namespace.clone()),
            table: Arc::from(proof.table.clone()),
        };
        self.runtime
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let loaded = self
            .runtime
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        if loaded.table.metadata().uuid().to_string() != proof.table_uuid {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg staged publication cleanup table UUID drifted",
            ));
        }
        if let Some(reference) = loaded.table.metadata().refs().get(&proof.staging_ref) {
            if reference.snapshot_id != proof.staging_snapshot_id.expect("checked above")
                || !reference.is_branch()
            {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "Iceberg staged publication ref drifted before cleanup",
                    ),
                });
            }
        } else {
            let outcome = ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: ConnectorStagedPublicationCleanupReceipt {
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
                finalization: ExternalMutationFinalization::Complete,
            };
            self.recovery_cleanup_outcomes
                .lock()
                .map_err(recovery_cleanup_lock_error)?
                .insert(
                    request.operation_id,
                    IcebergRecoveryCleanupRecord {
                        outcome: outcome.clone(),
                        proof,
                        descriptor_digest: request.descriptor_digest,
                        observation_digest: request.observation.digest(),
                    },
                );
            return Ok(outcome);
        }
        let outcome = ConnectorCatalogMutation::execute(
            self,
            ConnectorCatalogMutationRequest {
                operation_id: request.operation_id,
                target: self.binding_key.clone(),
                operation: ConnectorCatalogMutationOperation::AlterRef {
                    table,
                    action: ConnectorRefAction::Drop {
                        kind: ConnectorRefKind::Branch,
                        name: Arc::from(proof.staging_ref.clone()),
                        policy: DropPolicy::NoOpIfMissing,
                    },
                },
                context: request.context,
            },
        )?;
        let outcome = match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect,
                finalization,
                ..
            } => ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt: ConnectorStagedPublicationCleanupReceipt {
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
                finalization,
            },
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                ExternalMutationOutcome::KnownUncommitted { failure }
            }
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                ExternalMutationOutcome::CommitUnknown { failure, evidence }
            }
        };
        self.recovery_cleanup_outcomes
            .lock()
            .map_err(recovery_cleanup_lock_error)?
            .insert(
                request.operation_id,
                IcebergRecoveryCleanupRecord {
                    outcome: outcome.clone(),
                    proof,
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
            );
        Ok(outcome)
    }

    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&context)?;
        if evidence.operation_id() != operation_id
            || evidence.descriptor() != &self.descriptor
            || evidence.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg staged publication cleanup evidence does not match this generation",
            ));
        }
        let record = self
            .recovery_cleanup_outcomes
            .lock()
            .map_err(recovery_cleanup_lock_error)?
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "Iceberg staged publication cleanup has no retained outcome to reconcile",
                )
            })?;
        if !matches!(
            record.outcome,
            ExternalMutationOutcome::CommitUnknown { .. }
        ) {
            return Ok(record.outcome);
        }
        let table = ConnectorTableIdentity {
            instance_id: self.descriptor.instance_id.clone(),
            namespace: Arc::from(record.proof.namespace.clone()),
            table: Arc::from(record.proof.table.clone()),
        };
        self.runtime
            .control_state()
            .invalidate_table_cache(&table.namespace, &table.table);
        let loaded = self
            .runtime
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        let outcome = if loaded.table.metadata().uuid().to_string() != record.proof.table_uuid {
            ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg staged publication cleanup table UUID drifted during reconciliation",
                ),
            }
        } else {
            match loaded
                .table
                .metadata()
                .refs()
                .get(&record.proof.staging_ref)
            {
                None => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: ConnectorStagedPublicationCleanupReceipt {
                        descriptor_digest: record.descriptor_digest,
                        observation_digest: record.observation_digest,
                    },
                    finalization: ExternalMutationFinalization::Complete,
                },
                Some(reference)
                    if reference.is_branch()
                        && Some(reference.snapshot_id) == record.proof.staging_snapshot_id =>
                {
                    ExternalMutationOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "Iceberg staged publication cleanup ref still points at the inspected snapshot",
                        ),
                    }
                }
                Some(_) => ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "Iceberg staged publication cleanup ref drifted during reconciliation",
                    ),
                },
            }
        };
        self.recovery_cleanup_outcomes
            .lock()
            .map_err(recovery_cleanup_lock_error)?
            .insert(
                operation_id,
                IcebergRecoveryCleanupRecord {
                    outcome: outcome.clone(),
                    ..record
                },
            );
        Ok(outcome)
    }
}

fn staged_publication_target_ancestors(
    metadata: &crate::iceberg::spec::TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Vec<i64> {
    let mut ancestors = Vec::new();
    let mut cursor = target_snapshot_id;
    while let Some(snapshot_id) = cursor {
        if ancestors.len()
            == novarocks_spi::connector::MAX_CONNECTOR_STAGED_PUBLICATION_LINEAGE_FACTS
        {
            break;
        }
        ancestors.push(snapshot_id);
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    ancestors
}

fn staged_publication_disposition(
    staging_snapshot_id: Option<i64>,
    target_snapshot_id: Option<i64>,
    target_marker_snapshot_id: Option<i64>,
    staging_has_marker: bool,
    staging_is_target_ancestor: bool,
) -> ConnectorStagedPublicationDisposition {
    // Atomic managed repartition publishes directly to the target ref in the
    // same commit as the partition-spec transition. A staging ref may still
    // witness the old parent and must not hide this stronger target marker.
    if let Some(marker_snapshot_id) = target_marker_snapshot_id {
        if target_snapshot_id == Some(marker_snapshot_id) {
            return if staging_snapshot_id == Some(marker_snapshot_id) {
                ConnectorStagedPublicationDisposition::CleanupPending
            } else {
                ConnectorStagedPublicationDisposition::Published
            };
        }
        return ConnectorStagedPublicationDisposition::Superseded;
    }
    match (staging_snapshot_id, target_snapshot_id) {
        (Some(staging), Some(target)) if staging == target => {
            if staging_has_marker {
                ConnectorStagedPublicationDisposition::CleanupPending
            } else {
                ConnectorStagedPublicationDisposition::Ambiguous
            }
        }
        (Some(_), _) if staging_is_target_ancestor => {
            if staging_has_marker {
                ConnectorStagedPublicationDisposition::Superseded
            } else {
                ConnectorStagedPublicationDisposition::Ambiguous
            }
        }
        (Some(_), _) if staging_has_marker => ConnectorStagedPublicationDisposition::Staged,
        (Some(_), _) => ConnectorStagedPublicationDisposition::Ambiguous,
        (None, _) => ConnectorStagedPublicationDisposition::KnownUncommitted,
    }
}

fn recovery_cleanup_lock_error<T>(error: std::sync::PoisonError<T>) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("Iceberg recovery cleanup outcome lock: {error}"),
    )
}

impl IcebergControlProvider {
    fn plan_change_window_splits(
        &self,
        scan: &IcebergScanPayload,
        delta: Option<&crate::change_planning::IcebergDeltaScanPlan>,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        let delta = delta.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg full-rebuild change-window admission cannot plan incremental splits",
            )
        })?;
        let name_mapping = split_name_mapping(&scan.table)?;
        let mut total_payload_bytes = 0_usize;
        let mut splits = Vec::with_capacity(delta.sources.len());
        for source in delta.sources.iter().cloned() {
            self.validate_context(&request.context)?;
            let estimated_bytes = u64::try_from(source.size).map_err(|_| {
                corrupt(format!(
                    "Iceberg delta source {} has a negative size",
                    source.path
                ))
            })?;
            let data_file = IcebergDataFileInfo {
                path: source.path.clone(),
                size: source.size,
                row_count: None,
                column_stats: None,
                partition_spec_id: source.partition_spec_id,
                partition_key: source.partition_key.clone(),
                first_row_id: source.first_row_id,
                data_sequence_number: source.data_sequence_number,
                ivm_change_op: None,
                included_positions: None,
                delete_files: Vec::new(),
                manifest_path: None,
                partition_values: Vec::new(),
            };
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V5,
                owner_instance_id: self.descriptor.instance_id.as_str().to_string(),
                incarnation: self.incarnation.to_bytes(),
                namespace: scan.table.namespace.clone(),
                table: scan.table.table.clone(),
                snapshot_id: scan.snapshot_id,
                table_uuid: scan.table_uuid.clone(),
                schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
                units: vec![IcebergFrozenScanUnitPayload {
                    data_file,
                    row_groups: None,
                    estimated_bytes: Some(estimated_bytes),
                }],
                projection: scan.projection.clone(),
                limit: scan.limit,
                physical_predicates: Vec::new(),
                fact_columns: scan.fact_columns.clone(),
                name_mapping: name_mapping.clone(),
                delta: Some(crate::delta::IcebergDeltaSplitPayload {
                    source,
                    delete_side: delta.delete_side.clone(),
                }),
                distributed_rewrite_position: None,
                metadata: None,
            };
            let payload = encode_payload(
                &payload,
                "delta split",
                request.context.max_handle_payload_bytes(),
            )?;
            total_payload_bytes = total_payload_bytes
                .checked_add(payload.len())
                .filter(|total| *total <= request.context.max_total_payload_bytes())
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg delta split payloads exceed the request budget",
                    )
                })?;
            splits.push(ConnectorSplit::try_new(
                self.descriptor.instance_id.clone(),
                format!("delta-{}", splits.len()),
                payload,
                Some(estimated_bytes),
            )?);
        }
        let count = u64::try_from(splits.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg delta split count overflows u64",
            )
        })?;
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: count,
                candidate_units_pruned: 0,
                composite_splits_planned: count,
                scan_units_planned: count,
            },
        )
    }

    fn plan_metadata_splits(
        &self,
        scan: IcebergScanPayload,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        let metadata_table_type = scan.table.metadata_table_type.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata split planning requires a metadata table type",
            )
        })?;
        let table =
            scan.table.table_info.as_ref().ok_or_else(|| {
                corrupt("Iceberg metadata split is missing frozen table information")
            })?;
        let serialized_table = table.serialized_metadata.clone().ok_or_else(|| {
            corrupt("Iceberg metadata split is missing serialized table metadata")
        })?;
        let serialized_payload = match metadata_table_type {
            MetadataTableType::Files
            | MetadataTableType::Manifests
            | MetadataTableType::LogicalIcebergMetadata => {
                table.serialized_metadata_rows.clone().ok_or_else(|| {
                    corrupt("Iceberg metadata split is missing frozen metadata rows")
                })?
            }
            MetadataTableType::Snapshots | MetadataTableType::History | MetadataTableType::Refs => {
                String::new()
            }
            MetadataTableType::Partitions => {
                partition_metadata_payload(&scan.table.prepared_files).map_err(corrupt)?
            }
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: self.descriptor.instance_id.as_str().to_string(),
            incarnation: self.incarnation.to_bytes(),
            namespace: scan.table.namespace,
            table: scan.table.table,
            snapshot_id: scan.snapshot_id,
            table_uuid: scan.table_uuid,
            schema_id: Some(table.schema_id),
            units: Vec::new(),
            projection: scan.projection,
            limit: scan.limit,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: None,
            distributed_rewrite_position: None,
            metadata: Some(IcebergMetadataSplitPayloadV1 {
                metadata_table_type,
                serialized_table,
                serialized_payload,
            }),
        };
        let encoded = encode_payload(
            &payload,
            "metadata split",
            request.context.max_handle_payload_bytes(),
        )?;
        if encoded.len() > request.context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg metadata split payload exceeds the request budget",
            ));
        }
        let split = ConnectorSplit::try_new(
            self.descriptor.instance_id.clone(),
            "iceberg-metadata-0".to_string(),
            encoded,
            None,
        )?;
        ConnectorSplitPlanningResult::try_new(
            vec![split],
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 1,
                candidate_units_pruned: 0,
                composite_splits_planned: 1,
                scan_units_planned: 1,
            },
        )
    }

    fn scan_files(
        &self,
        scan: &IcebergScanPayload,
    ) -> Result<Vec<IcebergDataFileInfo>, ConnectorError> {
        if scan.table.row_mutation_frozen_source {
            match scan.table.explicit_files.as_deref() {
                Some([_]) => {}
                Some(_) => {
                    return Err(corrupt(
                        "Iceberg frozen row-mutation source must carry exactly one explicit data file",
                    ));
                }
                None => {
                    return Err(corrupt(
                        "Iceberg frozen row-mutation source is missing its explicit data file",
                    ));
                }
            }
        }
        match (&scan.table.explicit_files, scan.snapshot_id) {
            (Some(files), _) => Ok(files.clone()),
            (None, None) => Ok(Vec::new()),
            (None, Some(snapshot_id)) => {
                let physical = self
                    .runtime
                    .load_table(&scan.table.namespace, &scan.table.table)
                    .map_err(unavailable)?;
                let expected_uuid = scan.table_uuid.as_deref().ok_or_else(|| {
                    corrupt("Iceberg snapshot scan is missing its table incarnation")
                })?;
                if physical.table.metadata().uuid().to_string() != expected_uuid {
                    return Err(corrupt(
                        "Iceberg scan belongs to a different table incarnation",
                    ));
                }
                let table = physical.table;
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(
                        async move { extract_data_files_with_stats_at(&table, snapshot_id).await },
                    )
                    .map_err(unavailable)?
                    .map_err(unavailable)
                    .map(|files| {
                        files
                            .into_iter()
                            .map(data_file_with_stats_to_iceberg_data_file_info)
                            .collect()
                    })
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct IcebergTablePayload {
    pub namespace: String,
    pub table: String,
    pub table_info: Option<IcebergTableInfo>,
    pub metadata_columns: Vec<String>,
    pub metadata_table_type: Option<MetadataTableType>,
    pub prepared_files: Vec<IcebergDataFileInfo>,
    pub explicit_files: Option<Vec<IcebergDataFileInfo>>,
    /// Provider-private exact-base COW source. Such a handle carries a complete
    /// explicit file set and must never fall back to a catalog lookup.
    #[serde(default)]
    pub row_mutation_frozen_source: bool,
    #[serde(default)]
    pub logical_type_columns: BTreeMap<String, String>,
    #[serde(default)]
    pub hidden_columns: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct IcebergScanPayload {
    table: IcebergTablePayload,
    snapshot_id: Option<i64>,
    table_uuid: Option<String>,
    projection: Vec<usize>,
    limit: Option<u64>,
    purpose: IcebergReadPurposeV1,
    fact_columns: Vec<IcebergScanFactColumnV1>,
    physical_predicates: Vec<IcebergPhysicalPredicate>,
    mode: IcebergScanModeV1,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum IcebergScanModeV1 {
    Snapshot,
    ChangeWindow {
        delta: Option<crate::change_planning::IcebergDeltaScanPlan>,
    },
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum IcebergReadPurposeV1 {
    Query,
    MvTargetState,
    MvTargetLocator,
}

impl From<ConnectorReadPurpose> for IcebergReadPurposeV1 {
    fn from(value: ConnectorReadPurpose) -> Self {
        match value {
            ConnectorReadPurpose::Query => Self::Query,
            ConnectorReadPurpose::MvTargetState => Self::MvTargetState,
            ConnectorReadPurpose::MvTargetLocator => Self::MvTargetLocator,
        }
    }
}

fn projected_schema(
    table: &IcebergTablePayload,
    projection: &[usize],
) -> Result<SchemaRef, ConnectorError> {
    let serialized = table
        .table_info
        .as_ref()
        .and_then(|table| table.serialized_metadata.as_deref())
        .ok_or_else(|| corrupt("Iceberg table handle has no serialized metadata"))?;
    let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| corrupt(format!("decode Iceberg table metadata: {error}")))?;
    let storage_schema = if table.row_mutation_frozen_source {
        let snapshot_id = table
            .table_info
            .as_ref()
            .and_then(|table| table.current_snapshot_id)
            .ok_or_else(|| corrupt("Iceberg frozen row-mutation source has no base snapshot"))?;
        metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| corrupt("Iceberg frozen row-mutation base snapshot is absent"))?
            .schema(&metadata)
            .map_err(|error| corrupt(format!("resolve frozen row-mutation schema: {error}")))?
    } else {
        metadata.current_schema().clone()
    };
    let storage = crate::iceberg::arrow::schema_to_arrow_schema(&storage_schema)
        .map_err(|error| corrupt(format!("convert Iceberg schema to Arrow: {error}")))?;
    let mut fields = storage.fields().to_vec();
    let mut metadata_fields = metadata_arrow_fields(&table.metadata_columns)?;
    if table.row_mutation_frozen_source {
        metadata_fields = metadata_fields
            .into_iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(false)))
            .collect();
    }
    fields.extend(metadata_fields);
    let indexes = if projection.is_empty() {
        (0..fields.len()).collect::<Vec<_>>()
    } else {
        projection.to_vec()
    };
    let projected = indexes
        .into_iter()
        .map(|index| {
            fields.get(index).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("Iceberg projection index {index} is outside the table schema"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(projected)))
}

fn projected_metadata_schema(
    table: &IcebergTablePayload,
    projection: &[usize],
) -> Result<SchemaRef, ConnectorError> {
    let metadata_table_type = table.metadata_table_type.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg metadata schema requires a metadata table type",
        )
    })?;
    let serialized = table
        .table_info
        .as_ref()
        .and_then(|table| table.serialized_metadata.as_deref())
        .ok_or_else(|| corrupt("Iceberg metadata alias has no serialized table metadata"))?;
    let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| corrupt(format!("decode Iceberg table metadata: {error}")))?;
    let columns = metadata_table_output_columns(metadata_table_type, &metadata).map_err(corrupt)?;
    let schema = metadata_output_schema(&columns).map_err(corrupt)?;
    if projection.is_empty() {
        return Ok(schema);
    }
    let fields = projection
        .iter()
        .map(|index| {
            schema.fields().get(*index).cloned().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("metadata projection index {index} is outside the visible schema"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn metadata_read_type(
    metadata_table_type: MetadataTableType,
) -> Result<crate::metadata_read::MetadataTableType, ConnectorError> {
    match metadata_table_type {
        MetadataTableType::Files => Ok(crate::metadata_read::MetadataTableType::Files),
        MetadataTableType::Manifests => Ok(crate::metadata_read::MetadataTableType::Manifests),
        MetadataTableType::LogicalIcebergMetadata => {
            Ok(crate::metadata_read::MetadataTableType::LogicalIcebergMetadata)
        }
        _ => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg metadata manifest walk does not support this alias",
        )),
    }
}

fn partition_metadata_payload(files: &[IcebergDataFileInfo]) -> Result<String, String> {
    use std::collections::BTreeSet;

    let mut groups =
        BTreeMap::<(i32, String), (i64, i64, BTreeSet<String>, BTreeSet<String>)>::new();
    for file in files {
        let spec_id = file.partition_spec_id.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires partition spec id for data file {}",
                file.path
            )
        })?;
        let rows = file.row_count.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires record_count for data file {}",
                file.path
            )
        })?;
        let entry = groups
            .entry((
                spec_id,
                file.partition_key
                    .clone()
                    .unwrap_or_else(|| "Struct([])".to_string()),
            ))
            .or_default();
        entry.0 = entry
            .0
            .checked_add(rows)
            .ok_or_else(|| "iceberg partitions metadata record_count overflow".to_string())?;
        entry.1 = entry
            .1
            .checked_add(1)
            .ok_or_else(|| "iceberg partitions metadata file_count overflow".to_string())?;
        for delete in &file.delete_files {
            match delete.file_content {
                crate::scan_model::IcebergDeleteFileContent::Position => {
                    entry.2.insert(delete.path.clone());
                }
                crate::scan_model::IcebergDeleteFileContent::Equality => {
                    entry.3.insert(delete.path.clone());
                }
            }
        }
    }
    let rows = groups
        .into_iter()
        .map(
            |((_spec_id, _partition), (record_count, file_count, position, equality))| {
                Ok(serde_json::json!({
                    "record_count": record_count,
                    "file_count": file_count,
                    "position_delete_file_count": i64::try_from(position.len()).map_err(|_| "iceberg partitions metadata position delete count overflow".to_string())?,
                    "equality_delete_file_count": i64::try_from(equality.len()).map_err(|_| "iceberg partitions metadata equality delete count overflow".to_string())?,
                }))
            },
        )
        .collect::<Result<Vec<_>, String>>()?;
    serde_json::to_string(&serde_json::json!({ "version": 1, "rows": rows }))
        .map_err(|error| format!("serialize Iceberg partitions metadata payload: {error}"))
}

pub(crate) fn metadata_arrow_fields(names: &[String]) -> Result<Vec<Arc<Field>>, ConnectorError> {
    names
        .iter()
        .map(|name| {
            let (data_type, nullable) = match name.as_str() {
                "_file" => (arrow::datatypes::DataType::Utf8, false),
                "_pos" | "_row_id" => (arrow::datatypes::DataType::Int64, false),
                "_last_updated_sequence_number" => (arrow::datatypes::DataType::Int64, true),
                other => {
                    return Err(corrupt(format!(
                        "unknown Iceberg metadata column `{other}`"
                    )));
                }
            };
            Ok(Arc::new(
                Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                    novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                    "true".to_string(),
                )])),
            ))
        })
        .collect()
}

fn scan_fact_columns(
    output_schema: &SchemaRef,
    projection: &[usize],
    table: &IcebergTablePayload,
) -> Result<Vec<IcebergScanFactColumnV1>, ConnectorError> {
    let Some(table_info) = table.table_info.as_ref() else {
        return Ok(Vec::new());
    };
    let indexes = if projection.is_empty() {
        (0..output_schema.fields().len()).collect::<Vec<_>>()
    } else {
        projection.to_vec()
    };
    if indexes.len() != output_schema.fields().len() {
        return Err(corrupt(
            "Iceberg output schema does not match its frozen projection",
        ));
    }
    let mut columns = indexes
        .into_iter()
        .zip(output_schema.fields())
        .filter_map(|(ordinal, field)| {
            if is_metadata_column(field.name()) {
                return None;
            }
            Some((ordinal, field))
        })
        .map(|(ordinal, field)| {
            let field_ordinal = u32::try_from(ordinal).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg table-schema ordinal does not fit u32",
                )
            })?;
            let frozen = table_info.schema.fields.get(ordinal).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("Iceberg projection index {ordinal} is outside the frozen schema"),
                )
            })?;
            if !frozen.name.eq_ignore_ascii_case(field.name()) {
                return Err(corrupt(
                    "Iceberg frozen table schema disagrees with its output schema",
                ));
            }
            Ok(IcebergScanFactColumnV1 {
                field_ordinal,
                field_id: frozen.field_id,
                canonical_name: frozen.name.to_ascii_lowercase(),
                scalar_type: scan_fact_scalar_type(field.data_type()),
                nullable: field.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    columns.sort_by_key(|column| column.field_ordinal);
    Ok(columns)
}

fn split_name_mapping(table: &IcebergTablePayload) -> Result<Option<String>, ConnectorError> {
    let Some(serialized) = table
        .table_info
        .as_ref()
        .and_then(|table| table.serialized_metadata.as_deref())
    else {
        return Ok(None);
    };
    let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| corrupt(format!("decode Iceberg name mapping metadata: {error}")))?;
    metadata
        .properties()
        .get(crate::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|mapping| canonical_split_name_mapping(mapping))
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn push_data_split(
    provider: &IcebergControlProvider,
    scan: &IcebergScanPayload,
    name_mapping: &Option<String>,
    splits: &mut Vec<ConnectorSplit>,
    total_payload_bytes: &mut usize,
    units: Vec<IcebergFrozenScanUnitPayload>,
    estimated_bytes: u64,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    let payload = SplitPayload {
        version: ICEBERG_SPLIT_V5,
        owner_instance_id: provider.descriptor.instance_id.as_str().to_string(),
        incarnation: provider.incarnation.to_bytes(),
        namespace: scan.table.namespace.clone(),
        table: scan.table.table.clone(),
        snapshot_id: scan.snapshot_id,
        table_uuid: scan.table_uuid.clone(),
        schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
        units,
        projection: scan.projection.clone(),
        limit: scan.limit,
        physical_predicates: scan.physical_predicates.clone(),
        fact_columns: scan.fact_columns.clone(),
        name_mapping: name_mapping.clone(),
        delta: None,
        distributed_rewrite_position: None,
        metadata: None,
    };
    let payload = encode_payload(&payload, "split", context.max_handle_payload_bytes())?;
    let next_total = total_payload_bytes
        .checked_add(payload.len())
        .filter(|total| *total <= context.max_total_payload_bytes())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg split payloads exceed the request budget",
            )
        })?;
    let split = ConnectorSplit::try_new(
        provider.descriptor.instance_id.clone(),
        format!(
            "{}-{}",
            scan.snapshot_id
                .map(|snapshot| snapshot.to_string())
                .unwrap_or_else(|| "explicit".to_string()),
            splits.len()
        ),
        payload,
        Some(estimated_bytes),
    )?;
    splits.push(split);
    *total_payload_bytes = next_total;
    Ok(())
}

fn select_snapshot(
    metadata: &crate::iceberg::spec::TableMetadata,
    selector: ConnectorReadSelector,
) -> Result<Option<i64>, ConnectorError> {
    match selector {
        ConnectorReadSelector::Current => Ok(metadata.current_snapshot_id()),
        ConnectorReadSelector::SnapshotId(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .map(|_| Some(snapshot_id))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    format!("Iceberg snapshot {snapshot_id} does not exist"),
                )
            }),
        ConnectorReadSelector::TimestampMicros(timestamp) => {
            let millis = timestamp.div_euclid(1_000);
            metadata
                .history()
                .iter()
                .filter(|entry| entry.timestamp_ms() <= millis)
                .max_by_key(|entry| entry.timestamp_ms())
                .map(|entry| Some(entry.snapshot_id))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "no Iceberg snapshot exists at the requested timestamp",
                    )
                })
        }
    }
}

fn is_metadata_column(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "_file" | "_pos" | "_row_id" | "_last_updated_sequence_number"
    )
}

fn resolve_table_request(
    requested: &str,
    resolution: ConnectorTableResolution,
) -> Result<(String, Option<MetadataTableType>), ConnectorError> {
    let alias = requested.rsplit_once('$').and_then(|(table, suffix)| {
        parse_metadata_table_type(suffix).map(|metadata_type| (table.to_string(), metadata_type))
    });
    match (resolution, alias) {
        (ConnectorTableResolution::StrictBaseTable, Some(_)) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "strict Iceberg table resolution does not accept metadata aliases",
        )),
        (ConnectorTableResolution::StrictBaseTable, None) => Ok((requested.to_string(), None)),
        (ConnectorTableResolution::ProviderReadAlias, Some(alias)) => Ok((alias.0, Some(alias.1))),
        (ConnectorTableResolution::ProviderReadAlias, None) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider read alias must use `<table>$<metadata-type>`",
        )),
    }
}

fn parse_metadata_table_type(value: &str) -> Option<MetadataTableType> {
    match value.trim().to_ascii_uppercase().as_str() {
        "FILES" => Some(MetadataTableType::Files),
        "MANIFESTS" => Some(MetadataTableType::Manifests),
        "LOGICAL_ICEBERG_METADATA" | "ENTRIES" => Some(MetadataTableType::LogicalIcebergMetadata),
        "SNAPSHOTS" => Some(MetadataTableType::Snapshots),
        "HISTORY" => Some(MetadataTableType::History),
        "REFS" => Some(MetadataTableType::Refs),
        "PARTITIONS" => Some(MetadataTableType::Partitions),
        _ => None,
    }
}

fn negotiate_static_predicates(
    table: &IcebergTablePayload,
    predicates: &[ConnectorStaticPredicate],
) -> (
    Vec<IcebergPhysicalPredicate>,
    Vec<ConnectorPredicateDisposition>,
) {
    let table_info = table
        .metadata_table_type
        .is_none()
        .then_some(table.table_info.as_ref())
        .flatten();
    let mut physical_predicates = Vec::new();
    let mut dispositions = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let physical = table_info.and_then(|table_info| {
            let field = table_info
                .schema
                .fields
                .get(predicate.column.field_ordinal as usize)?;
            static_predicate_to_physical(predicate, field.field_id, &field.name)
        });
        let kind = if let Some(predicate) = physical {
            physical_predicates.push(predicate);
            ConnectorPredicateDispositionKind::PruningOnly
        } else {
            ConnectorPredicateDispositionKind::Unsupported
        };
        dispositions.push(ConnectorPredicateDisposition {
            predicate_id: predicate.id,
            kind,
        });
    }
    (physical_predicates, dispositions)
}

fn static_predicate_to_physical(
    predicate: &ConnectorStaticPredicate,
    field_id: i32,
    column: &str,
) -> Option<IcebergPhysicalPredicate> {
    use ConnectorScalarType::{Boolean, Date32, Int32, Int64};

    let value = |literal: &ConnectorScalarValue| match literal {
        ConnectorScalarValue::Boolean(value) if predicate.column.data_type == Boolean => {
            Some(IcebergPhysicalPredicateValue::Boolean(*value))
        }
        ConnectorScalarValue::Int32(value) if predicate.column.data_type == Int32 => {
            Some(IcebergPhysicalPredicateValue::Int32(*value))
        }
        ConnectorScalarValue::Int64(value) if predicate.column.data_type == Int64 => {
            Some(IcebergPhysicalPredicateValue::Int64(*value))
        }
        ConnectorScalarValue::Date32(value) if predicate.column.data_type == Date32 => {
            Some(IcebergPhysicalPredicateValue::Date32(*value))
        }
        _ => None,
    };
    let domain = match &predicate.kind {
        ConnectorStaticPredicateKind::Comparison { op, literal } => {
            let op = match op {
                ConnectorStaticComparisonOp::Eq => IcebergPhysicalPredicateOp::Eq,
                ConnectorStaticComparisonOp::Lt => IcebergPhysicalPredicateOp::Lt,
                ConnectorStaticComparisonOp::Le => IcebergPhysicalPredicateOp::Le,
                ConnectorStaticComparisonOp::Gt => IcebergPhysicalPredicateOp::Gt,
                ConnectorStaticComparisonOp::Ge => IcebergPhysicalPredicateOp::Ge,
                ConnectorStaticComparisonOp::Ne => return None,
                _ => return None,
            };
            IcebergPhysicalPredicateDomain::Range {
                op,
                value: value(literal)?,
            }
        }
        ConnectorStaticPredicateKind::In { literals } => {
            let values = literals.iter().map(value).collect::<Option<Vec<_>>>()?;
            if values.is_empty() {
                return None;
            }
            IcebergPhysicalPredicateDomain::DiscreteSet { values }
        }
        ConnectorStaticPredicateKind::IsNull | ConnectorStaticPredicateKind::IsNotNull => {
            return None;
        }
        _ => return None,
    };
    Some(IcebergPhysicalPredicate {
        field_id,
        column: column.to_string(),
        domain,
    })
}

fn read_reference_facts(
    metadata: &crate::iceberg::spec::TableMetadata,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
    ConnectorReadReferenceFacts::try_new(
        metadata
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect(),
        metadata
            .history()
            .iter()
            .map(|entry| ConnectorReadSnapshotLogEntry {
                snapshot_id: entry.snapshot_id,
                timestamp_millis: entry.timestamp_ms(),
            })
            .collect(),
        metadata
            .refs()
            .iter()
            .map(|(name, reference)| ConnectorReadNamedReference {
                name: Arc::from(name.as_str()),
                kind: if reference.is_branch() {
                    ConnectorReadReferenceKind::Branch
                } else {
                    ConnectorReadReferenceKind::Tag
                },
                snapshot_id: reference.snapshot_id,
            })
            .collect(),
        metadata.current_snapshot_id(),
        context,
    )
}

fn logical_type_columns(properties: &HashMap<String, String>) -> BTreeMap<String, String> {
    properties
        .iter()
        .filter_map(|(key, value)| {
            let column = key.strip_prefix(LOGICAL_TYPE_PROPERTY_PREFIX)?;
            matches!(value.to_ascii_lowercase().as_str(), "bitmap" | "hll")
                .then(|| (column.to_ascii_lowercase(), value.to_ascii_lowercase()))
        })
        .collect()
}

fn hidden_internal_columns(properties: &HashMap<String, String>) -> Vec<String> {
    let mut hidden = Vec::new();
    for value in properties
        .get(APPLY_KEY_COLUMN_PROPERTY)
        .into_iter()
        .chain(properties.get(HIDDEN_COLUMNS_PROPERTY))
    {
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if !hidden
                .iter()
                .any(|current: &String| current.eq_ignore_ascii_case(name))
            {
                hidden.push(name.to_string());
            }
        }
    }
    hidden
}

fn annotate_hidden_fields(schema: SchemaRef, hidden: &[String]) -> SchemaRef {
    if hidden.is_empty() {
        return schema;
    }
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|field| {
            if !hidden
                .iter()
                .any(|name| name.eq_ignore_ascii_case(field.name()))
            {
                return field.clone();
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(
                novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                "true".to_string(),
            );
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn metadata_column_names(metadata: &crate::iceberg::spec::TableMetadata) -> Vec<String> {
    let mut columns = vec!["_file".to_string(), "_pos".to_string()];
    if row_lineage_enabled(metadata) {
        columns.push("_row_id".to_string());
        columns.push("_last_updated_sequence_number".to_string());
    }
    columns
}

fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    let payload = serde_json::to_vec(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("serialize Iceberg {subject}: {error}"),
        )
    })?;
    if payload.len() > max_payload_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("Iceberg {subject} exceeds the request payload budget"),
        ));
    }
    Ok(Bytes::from(payload))
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg {subject}: {error}"),
        )
    })
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
        .with_retryable_before_progress()
}

#[cfg(test)]
mod staged_publication_recovery_tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use base64::Engine;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorHistoricalPublicationAction, ConnectorInstanceId,
        ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
        ConnectorStagedPublicationPhase, ConnectorStagedPublicationPhaseState,
    };

    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, Summary, Transform, Type, UnboundPartitionSpecBuilder,
    };
    use crate::iceberg::{
        NamespaceIdent, TableCommit, TableCreation, TableRequirement, TableUpdate,
    };
    use crate::resources::IcebergControlResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            64 * 1024,
            256 * 1024,
        )
        .expect("request context")
    }

    fn provider_with_empty_table() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlProvider,
        crate::iceberg::table::Table,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = IcebergControlProvider::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            Arc::clone(&runtime),
        );
        let catalog = Arc::clone(runtime.catalog());
        let table = executor.block_on(async move {
            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("create namespace");
            let schema = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("t".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                )
                .await
                .expect("create table")
        });
        (executor, warehouse, provider, table)
    }

    fn historical_actions() -> Vec<ConnectorHistoricalPublicationAction> {
        [
            ConnectorStagedPublicationPhase::StagingCreate,
            ConnectorStagedPublicationPhase::Write,
            ConnectorStagedPublicationPhase::Publication,
            ConnectorStagedPublicationPhase::StagingDrop,
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, phase)| ConnectorHistoricalPublicationAction {
            phase,
            state: ConnectorStagedPublicationPhaseState::Prepared,
            operation_id: ConnectorMutationOperationId::from_bytes([ordinal as u8 + 1; 16]),
            committed_version: None,
            evidence_digest: None,
        })
        .collect()
    }

    fn recovery_descriptor(
        provider: &IcebergControlProvider,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorStagedPublicationDescriptor {
        ConnectorStagedPublicationDescriptor::try_new(
            provider.binding_key.clone(),
            ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            "__novarocks_staging_41",
            "main",
            None,
            41,
            7,
            [8; 16],
            "refresh-41",
            vec![[9; 32]],
            [10; 32],
            historical_actions(),
            Vec::new(),
        )
        .expect("recovery descriptor")
    }

    #[test]
    fn empty_table_inspection_is_known_uncommitted_and_exact_generation_owned() {
        let (_executor, _warehouse, provider, _table) = provider_with_empty_table();
        let descriptor = recovery_descriptor(&provider, provider.descriptor.instance_id.clone());
        let observation = provider
            .inspect(descriptor, context())
            .expect("inspect empty table");
        assert_eq!(
            observation.disposition,
            ConnectorStagedPublicationDisposition::KnownUncommitted
        );
        assert!(observation.committed_version.is_none());
        assert!(observation.staging_snapshot_id.is_none());
        assert!(!observation.cleanup_required);
        assert_eq!(
            ConnectorStagedPublicationRecovery::binding_key(&provider),
            &provider.binding_key
        );
    }

    #[test]
    fn inspection_rejects_a_table_from_another_instance() {
        let (_executor, _warehouse, provider, _table) = provider_with_empty_table();
        let descriptor = recovery_descriptor(
            &provider,
            ConnectorInstanceId::parse("other").expect("other instance"),
        );
        let error = provider
            .inspect(descriptor, context())
            .expect_err("instance mismatch");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn cleanup_is_idempotent_when_the_exact_staging_ref_is_already_absent() {
        let (_executor, _warehouse, provider, table) = provider_with_empty_table();
        let descriptor = recovery_descriptor(&provider, provider.descriptor.instance_id.clone());
        let proof = IcebergStagedPublicationProofV1 {
            version: ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: table.metadata().uuid().to_string(),
            staging_ref: descriptor.staging_ref.to_string(),
            staging_snapshot_id: Some(17),
            target_ref: "main".to_string(),
            target_snapshot_id: None,
            refresh_id: descriptor.refresh_id,
            mv_id: descriptor.mv_id,
            marker_token: descriptor.marker_token.to_string(),
        };
        let observation = ConnectorStagedPublicationObservation::try_new(
            ConnectorStagedPublicationDisposition::Staged,
            None,
            None,
            Vec::new(),
            None,
            Some(17),
            None,
            true,
            ConnectorStagedPublicationProof::try_new(Bytes::from(
                encode_staged_publication_proof(&proof).expect("proof"),
            ))
            .expect("sealed proof"),
        )
        .expect("observation");
        let request = ConnectorStagedPublicationCleanupRequest {
            operation_id: ConnectorMutationOperationId::from_bytes([22; 16]),
            descriptor_digest: descriptor.digest(),
            observation,
            context: context(),
        };
        for outcome in [
            provider.cleanup(request.clone()).expect("first cleanup"),
            provider.cleanup(request).expect("replayed cleanup"),
        ] {
            assert!(matches!(
                outcome,
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::NoOp,
                    finalization: ExternalMutationFinalization::Complete,
                    ..
                }
            ));
        }
    }

    #[test]
    fn atomic_main_marker_wins_over_old_staging_ancestor() {
        let disposition = staged_publication_disposition(Some(10), Some(11), Some(11), false, true);
        assert_eq!(
            disposition,
            ConnectorStagedPublicationDisposition::Published
        );
        assert!(Some(10_i64).is_some());
    }

    #[derive(serde::Serialize)]
    struct TestAtomicOperationMarker {
        version: u8,
        instance_id: String,
        incarnation_base64: String,
        operation_id_base64: String,
        target_ref: String,
        cohort_set_digest_base64: String,
        aggregate_digest_base64: String,
        partition_replacement_id_base64: Option<String>,
        expected_prior_partition_spec_id: Option<i32>,
        expected_prior_partition_observation_base64: Option<String>,
        committed_partition_spec_id: Option<i32>,
        committed_partitioning_digest_base64: Option<String>,
    }

    #[test]
    fn historical_inspection_accepts_later_main_head_but_rejects_default_spec_drift() {
        let (executor, _warehouse, provider, table) = provider_with_empty_table();
        let metadata = table.metadata().clone();
        let unbound = UnboundPartitionSpecBuilder::new()
            .add_partition_field(1, "value", Transform::Identity)
            .expect("partition field")
            .build();
        let prospective =
            crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(metadata.clone(), None)
                .add_default_partition_spec(unbound)
                .expect("add default spec")
                .build()
                .expect("prospective metadata");
        let spec_id = prospective.metadata.default_partition_spec_id();
        let committed = crate::commit::write_control::committed_partitioning_from_metadata(
            &prospective.metadata,
            spec_id,
        )
        .expect("committed partitioning");
        let write_operation_id =
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([12; 16]);
        let prior_observation =
            novarocks_spi::connector::ConnectorManagedPartitionSpecObservation::try_from_fields(
                metadata.default_partition_spec_id(),
                &[],
            )
            .expect("prior observation");
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let marker = TestAtomicOperationMarker {
            version: 1,
            instance_id: provider.descriptor.instance_id.as_str().to_string(),
            incarnation_base64: b64(&provider.incarnation.to_bytes()),
            operation_id_base64: b64(&write_operation_id.to_bytes()),
            target_ref: "main".to_string(),
            cohort_set_digest_base64: b64(&[13; 32]),
            aggregate_digest_base64: b64(&[14; 32]),
            partition_replacement_id_base64: Some(b64(
                &novarocks_spi::connector::ConnectorManagedPartitionSpecReplacementId::derive(
                    write_operation_id,
                )
                .to_bytes(),
            )),
            expected_prior_partition_spec_id: Some(metadata.default_partition_spec_id()),
            expected_prior_partition_observation_base64: Some(b64(
                &prior_observation.layout_digest()
            )),
            committed_partition_spec_id: Some(spec_id),
            committed_partitioning_digest_base64: Some(b64(&committed.digest())),
        };
        let mut properties = crate::commit::MvProvenanceV1 {
            provenance_version: crate::commit::MV_PROVENANCE_VERSION,
            refresh_id: 41,
            mv_id: 7,
            token: "refresh-41".to_string(),
            technique: crate::commit::RefreshTechnique::Full,
            bases: Vec::new(),
            definition_fingerprint: "definition-fingerprint".to_string(),
            rows: 3,
        }
        .to_summary_properties()
        .expect("provenance properties");
        properties.insert("total-records".to_string(), "3".to_string());
        properties.insert(
            "novarocks.write.operation.v1".to_string(),
            serde_json::to_string(&marker).expect("operation marker"),
        );
        let published_snapshot_id = 101;
        let later_snapshot_id = 102;
        let published = Snapshot::builder()
            .with_snapshot_id(published_snapshot_id)
            .with_sequence_number(1)
            .with_timestamp_ms(metadata.last_updated_ms() + 1)
            .with_manifest_list("file:///tmp/published.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: properties.into_iter().collect(),
            })
            .with_schema_id(metadata.current_schema_id())
            .build();
        let later = Snapshot::builder()
            .with_snapshot_id(later_snapshot_id)
            .with_parent_snapshot_id(Some(published_snapshot_id))
            .with_sequence_number(2)
            .with_timestamp_ms(metadata.last_updated_ms() + 2)
            .with_manifest_list("file:///tmp/later.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .build();
        let mut updates = prospective.changes;
        updates.extend([
            TableUpdate::AddSnapshot {
                snapshot: published,
            },
            TableUpdate::AddSnapshot { snapshot: later },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                reference: SnapshotReference {
                    snapshot_id: later_snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ]);
        let table_ident = table.identifier().clone();
        let table_uuid = metadata.uuid();
        let original_default_spec_id = metadata.default_partition_spec_id();
        let catalog = Arc::clone(provider.runtime.catalog());
        let publication_catalog = Arc::clone(&catalog);
        let publication_ident = table_ident.clone();
        executor.block_on(async move {
            publication_catalog
                .update_table(
                    TableCommit::builder()
                        .ident(publication_ident)
                        .requirements(vec![
                            TableRequirement::UuidMatch { uuid: table_uuid },
                            TableRequirement::DefaultSpecIdMatch {
                                default_spec_id: original_default_spec_id,
                            },
                            TableRequirement::RefSnapshotIdMatch {
                                r#ref: "main".to_string(),
                                snapshot_id: None,
                            },
                        ])
                        .updates(updates)
                        .build(),
                )
                .await
                .expect("atomic publication plus later main snapshot");
        });
        let descriptor = recovery_descriptor(&provider, provider.descriptor.instance_id.clone());
        let observation = provider
            .inspect(descriptor, context())
            .expect("historical inspection");
        assert_eq!(
            observation.disposition,
            ConnectorStagedPublicationDisposition::Superseded
        );
        assert_eq!(
            observation
                .committed_version
                .as_ref()
                .and_then(ConnectorCommittedVersion::snapshot_id),
            Some(published_snapshot_id)
        );
        assert_eq!(observation.committed_partitioning, Some(committed));

        provider
            .runtime
            .control_state()
            .invalidate_table_cache("db", "t");
        let current = provider
            .runtime
            .load_table("db", "t")
            .expect("load atomic publication before external spec drift");
        let current_metadata = current.table.metadata().clone();
        let drifted = crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(
            current_metadata.clone(),
            None,
        )
        .add_default_partition_spec(
            UnboundPartitionSpecBuilder::new()
                .add_partition_field(1, "value_bucket_16", Transform::Bucket(16))
                .expect("drifted partition field")
                .build(),
        )
        .expect("add externally drifted default spec")
        .build()
        .expect("build externally drifted metadata");
        assert_ne!(drifted.metadata.default_partition_spec_id(), spec_id);
        executor.block_on(async move {
            catalog
                .update_table(
                    TableCommit::builder()
                        .ident(table_ident)
                        .requirements(vec![
                            TableRequirement::UuidMatch {
                                uuid: current_metadata.uuid(),
                            },
                            TableRequirement::DefaultSpecIdMatch {
                                default_spec_id: current_metadata.default_partition_spec_id(),
                            },
                        ])
                        .updates(drifted.changes)
                        .build(),
                )
                .await
                .expect("externally drift default partition spec");
        });
        let error = provider
            .inspect(
                recovery_descriptor(&provider, provider.descriptor.instance_id.clone()),
                context(),
            )
            .expect_err("historical inspection must reject default spec drift");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("atomic repartition default partition spec drifted from committed spec")
        );
    }

    #[test]
    fn cleanup_rejects_a_malformed_provider_proof_before_catalog_mutation() {
        let (_executor, _warehouse, provider, _table) = provider_with_empty_table();
        let descriptor = recovery_descriptor(&provider, provider.descriptor.instance_id.clone());
        let observation = ConnectorStagedPublicationObservation::try_new(
            ConnectorStagedPublicationDisposition::Staged,
            None,
            None,
            Vec::new(),
            None,
            Some(17),
            None,
            true,
            ConnectorStagedPublicationProof::try_new(Bytes::from_static(b"{}"))
                .expect("generic proof"),
        )
        .expect("observation");
        let error = provider
            .cleanup(ConnectorStagedPublicationCleanupRequest {
                operation_id: ConnectorMutationOperationId::from_bytes([23; 16]),
                descriptor_digest: descriptor.digest(),
                observation,
                context: context(),
            })
            .expect_err("malformed proof");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}

#[cfg(test)]
mod plan_splits_pruning_tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorProviderId, ConnectorRequestContext,
        ConnectorSplitPlanningMetrics,
    };

    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;
    use crate::scan_model::{
        IcebergColumnStats, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
        IcebergPhysicalPredicateValue,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            256 * 1024,
            1024 * 1024,
        )
        .expect("request context")
    }

    fn provider() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlProvider,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(executor.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(executor.handle().clone())),
        );
        let resources = IcebergControlResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let provider = IcebergControlProvider::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ConnectorInstanceIncarnation::from_bytes([7; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    /// ORC rather than Parquet so split materialization does not try to read a
    /// footer that this fixture has no file for. Pruning reads the manifest, not
    /// the data file, so the physical format is irrelevant to what is tested.
    fn file_with_bounds(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test(path, 128, 10);
        file.column_stats = Some(HashMap::from([(
            "id".to_string(),
            IcebergColumnStats {
                field_id: Some(7),
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: Some(min.to_le_bytes().to_vec()),
                upper_bound: Some(max.to_le_bytes().to_vec()),
            },
        )]));
        file
    }

    fn id_eq(value: i32) -> IcebergPhysicalPredicate {
        IcebergPhysicalPredicate {
            column: "id".to_string(),
            field_id: 7,
            domain: IcebergPhysicalPredicateDomain::Range {
                op: IcebergPhysicalPredicateOp::Eq,
                value: IcebergPhysicalPredicateValue::Int32(value),
            },
        }
    }

    fn plan(
        provider: &IcebergControlProvider,
        files: Vec<IcebergDataFileInfo>,
        predicates: Vec<IcebergPhysicalPredicate>,
    ) -> ConnectorSplitPlanningMetrics {
        let payload = IcebergScanPayload {
            table: IcebergTablePayload {
                namespace: "ns".to_string(),
                table: "t".to_string(),
                table_info: None,
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: Some(files),
                row_mutation_frozen_source: false,
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
            },
            snapshot_id: None,
            table_uuid: None,
            projection: vec![0],
            limit: None,
            purpose: IcebergReadPurposeV1::Query,
            fact_columns: Vec::new(),
            physical_predicates: predicates,
            mode: IcebergScanModeV1::Snapshot,
        };
        let context = context();
        let handle = ConnectorScanHandle::try_new(
            ConnectorInstanceId::parse("ice").expect("instance"),
            encode_payload(&payload, "scan handle", context.max_handle_payload_bytes())
                .expect("encode scan handle"),
        )
        .expect("scan handle");
        provider
            .plan_splits(
                &handle,
                ConnectorSplitPlanningRequest {
                    target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                    max_split_bytes: None,
                    context,
                },
            )
            .expect("plan splits")
            .metrics
    }

    #[test]
    fn planning_reports_the_files_it_pruned() {
        let (_executor, _warehouse, provider) = provider();
        let metrics = plan(
            &provider,
            vec![
                file_with_bounds("s3://bucket/a.orc", 1, 5),
                file_with_bounds("s3://bucket/b.orc", 10, 20),
                file_with_bounds("s3://bucket/c.orc", 100, 200),
            ],
            vec![id_eq(12)],
        );
        assert_eq!(metrics.candidate_units_considered, 3);
        assert_eq!(metrics.candidate_units_pruned, 2);
        assert_eq!(metrics.scan_units_planned, 1);
    }

    #[test]
    fn frozen_row_mutation_source_never_falls_back_to_the_current_catalog() {
        let (_executor, _warehouse, provider) = provider();
        let scan = IcebergScanPayload {
            table: IcebergTablePayload {
                namespace: "ns".to_string(),
                table: "t".to_string(),
                table_info: None,
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: None,
                row_mutation_frozen_source: true,
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
            },
            snapshot_id: Some(7),
            table_uuid: Some("admitted-table-uuid".to_string()),
            projection: vec![0],
            limit: None,
            purpose: IcebergReadPurposeV1::Query,
            fact_columns: Vec::new(),
            physical_predicates: Vec::new(),
            mode: IcebergScanModeV1::Snapshot,
        };

        let error = provider
            .scan_files(&scan)
            .expect_err("frozen source must not reload the current catalog");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.to_string().contains("missing its explicit data file"));
    }

    /// A zero count must mean "nothing was prunable", never "pruning did not
    /// run" -- otherwise the metric cannot be read at all.
    #[test]
    fn planning_reports_zero_pruned_when_every_file_may_match() {
        let (_executor, _warehouse, provider) = provider();
        let metrics = plan(
            &provider,
            vec![
                file_with_bounds("s3://bucket/a.orc", 10, 20),
                file_with_bounds("s3://bucket/b.orc", 11, 13),
            ],
            vec![id_eq(12)],
        );
        assert_eq!(metrics.candidate_units_considered, 2);
        assert_eq!(metrics.candidate_units_pruned, 0);
        assert_eq!(metrics.scan_units_planned, 2);
    }

    #[test]
    fn planning_without_predicates_prunes_nothing() {
        let (_executor, _warehouse, provider) = provider();
        let metrics = plan(
            &provider,
            vec![
                file_with_bounds("s3://bucket/a.orc", 1, 5),
                file_with_bounds("s3://bucket/b.orc", 100, 200),
            ],
            Vec::new(),
        );
        assert_eq!(metrics.candidate_units_considered, 2);
        assert_eq!(metrics.candidate_units_pruned, 0);
        assert_eq!(metrics.scan_units_planned, 2);
    }

    /// `candidate_units_considered` counts the pinned snapshot, so it must not
    /// shrink when pruning removes every file.
    #[test]
    fn considered_counts_the_snapshot_even_when_everything_is_pruned() {
        let (_executor, _warehouse, provider) = provider();
        let metrics = plan(
            &provider,
            vec![
                file_with_bounds("s3://bucket/a.orc", 1, 5),
                file_with_bounds("s3://bucket/b.orc", 100, 200),
            ],
            vec![id_eq(12)],
        );
        assert_eq!(metrics.candidate_units_considered, 2);
        assert_eq!(metrics.candidate_units_pruned, 2);
        assert_eq!(metrics.scan_units_planned, 0);
    }
}
