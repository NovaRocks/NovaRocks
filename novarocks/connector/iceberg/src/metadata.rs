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

//! Generation-local Iceberg control capabilities.
//!
//! This module is the provider implementation behind one frontend control
//! binding. It owns opaque table payloads and uses only the catalog client and
//! runtime injected into that exact generation.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow::datatypes::{Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorError, ConnectorErrorKind, ConnectorExactSemanticRevision,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorListNamespacesRequest,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorMutationOperationId,
    ConnectorNamespaceIdentity, ConnectorNamespaceRequest, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorProviderBindingKey, ConnectorReadNamedReference,
    ConnectorReadPurpose, ConnectorReadReferenceFacts, ConnectorReadReferenceFactsRequest,
    ConnectorReadReferenceKind, ConnectorReadSelector, ConnectorReadSnapshotLogEntry,
    ConnectorScalarType, ConnectorScalarValue, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorScanSelection, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateKind, ConnectorTableDefinitionFacts,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTableObjectBinding, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectId, ConnectorTableObjectRebindRequest,
    ConnectorTableObjectSelector, ConnectorTablePlanningFacts, ConnectorTableRequest,
    ConnectorTableResolution, ProviderBindingEpoch, validate_static_predicates,
};
use serde::{Deserialize, Serialize};

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
use crate::metadata_context::IcebergMetadataContext;
use crate::planning_facts::{IcebergTablePlanningFactsInput, table_planning_facts};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain,
    IcebergPhysicalPredicateOp, IcebergPhysicalPredicateValue, IcebergTableInfo,
};
use crate::schema_facts::{iceberg_schema_def, row_lineage_enabled};

const LOGICAL_TYPE_PROPERTY_PREFIX: &str = "novarocks.logical_type.";
use novarocks_spi::connector::{
    CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY as APPLY_KEY_COLUMN_PROPERTY,
    CONNECTOR_MV_HIDDEN_COLUMNS_PROPERTY as HIDDEN_COLUMNS_PROPERTY,
};

/// Owns the frontend-local read registration for one Iceberg generation.
///
/// The registry sees only a weak edge.  Every clone of `IcebergMetadata`
/// shares this owner, including the metadata/planning instances that cross a
/// control binding's `into_parts` boundary, so the exact registry slot lives
/// for precisely as long as this generation's control capabilities do.
struct GenerationOwner {
    registration_lease: OnceLock<Arc<dyn ConnectorReadRegistrationLease>>,
}

impl GenerationOwner {
    fn install(
        &self,
        lease: Arc<dyn ConnectorReadRegistrationLease>,
    ) -> Result<(), ConnectorError> {
        self.registration_lease.set(lease).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg generation read registration lease was installed twice",
            )
        })
    }
}

#[derive(Clone)]
pub struct IcebergMetadata {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    binding_key: ConnectorProviderBindingKey,
    runtime: Arc<IcebergMetadataContext>,
    generation_owner: Arc<GenerationOwner>,
}

impl IcebergMetadata {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        let binding_key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        Self {
            descriptor,
            incarnation,
            binding_key,
            runtime,
            generation_owner: Arc::new(GenerationOwner {
                registration_lease: OnceLock::new(),
            }),
        }
    }

    pub(crate) fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub(crate) fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }

    pub(crate) fn runtime(&self) -> &Arc<IcebergMetadataContext> {
        &self.runtime
    }

    /// Retain the exact frontend registration only after the caller has
    /// assembled and validated the complete control binding.  The strong edge
    /// remains private to this provider generation; roles retain weak edges
    /// and have no authority to renew or retire it.
    pub(crate) fn install_read_registration_lease(
        &self,
        lease: Arc<dyn ConnectorReadRegistrationLease>,
    ) -> Result<(), ConnectorError> {
        self.generation_owner.install(lease)
    }

    pub(crate) fn validate_context(
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

    fn require_current_table_object_selector(
        &self,
        selector: ConnectorTableObjectSelector,
    ) -> Result<(), ConnectorError> {
        match selector {
            ConnectorTableObjectSelector::Current => Ok(()),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table object binding selector is unsupported",
            )),
        }
    }

    fn current_table_object_binding(
        &self,
        table: ConnectorTableIdentity,
        resolution: ConnectorTableResolution,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        let metadata = self.load_table(ConnectorTableRequest {
            table,
            resolution,
            context,
        })?;
        // Design: ADR-0085 (docs/adr/ADR-0085-connector-physical-table-object-bindings.md)
        // Reuse the UUID frozen into this exact metadata observation instead of
        // issuing a second catalog lookup or deriving identity from a name.
        let table_payload = self.table_payload(&metadata.table)?;
        let table_uuid = table_payload
            .table_info
            .as_ref()
            .and_then(|table_info| table_info.table_uuid.as_deref())
            .ok_or_else(|| corrupt("Iceberg table metadata is missing its physical UUID"))?;
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))?;
        Ok(ConnectorTableObjectBinding {
            metadata,
            object_id,
        })
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
        staging_operation: ConnectorMutationOperationId,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorTableHandle, ConnectorError> {
        self.validate_context(context)?;
        let table_metadata = table.metadata();
        let data_prefix = crate::catalog_control::staged_create::staged_write_data_prefix(
            table_metadata.location(),
            staging_operation,
        );
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(
            table_metadata.clone(),
            None,
        )
        .set_properties(HashMap::from([(
            "write.data.path".to_string(),
            data_prefix,
        )]))
        .and_then(crate::iceberg::spec::TableMetadataBuilder::build)
        .map(|result| result.metadata)
        .map_err(|error| corrupt(format!("bind staged Iceberg data prefix: {error}")))?;
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
                serialized_metadata: Some(serde_json::to_string(&metadata).map_err(|error| {
                    corrupt(format!("serialize staged Iceberg table metadata: {error}"))
                })?),
                serialized_metadata_rows: None,
            }),
            metadata_columns: metadata_column_names(&metadata),
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

impl ConnectorMetadata for IcebergMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn exact_semantic_revision(
        &self,
        table: &ConnectorTableHandle,
        selector: ConnectorReadSelector,
    ) -> Result<ConnectorExactSemanticRevision, ConnectorError> {
        let payload = self.table_payload(table)?;
        let table_info = payload.table_info.as_ref().ok_or_else(|| {
            corrupt("Iceberg exact semantic revision is missing its frozen table descriptor")
        })?;
        let table_uuid = table_info.table_uuid.as_deref().ok_or_else(|| {
            corrupt("Iceberg exact semantic revision is missing its physical UUID")
        })?;
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))?;
        let snapshot_id = match selector {
            ConnectorReadSelector::Current => table_info.current_snapshot_id,
            selector => {
                let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
                    corrupt("Iceberg exact semantic revision is missing its frozen table metadata")
                })?;
                let metadata: crate::iceberg::spec::TableMetadata =
                    serde_json::from_str(serialized).map_err(|error| {
                        corrupt(format!(
                            "decode Iceberg exact semantic revision metadata: {error}"
                        ))
                    })?;
                select_snapshot(&metadata, selector)?
            }
        };
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            self.descriptor.provider_id.clone(),
            &object_id,
            snapshot_id,
        )
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
            .load_table_for_request(
                &request.table.namespace,
                &request.table.table,
                &request.context,
            )
            .map_err(unavailable)?;
        read_reference_facts(loaded.table.metadata(), &request.context)
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.require_current_table_object_selector(request.selector)?;
        self.current_table_object_binding(request.table, request.resolution, request.context)
    }

    fn rebind_table_object_binding(
        &self,
        request: ConnectorTableObjectRebindRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.require_current_table_object_selector(request.selector)?;
        let expected_object_id = request.expected_object_id;
        let binding = self
            .current_table_object_binding(request.table, request.resolution, request.context)
            .map_err(|error| {
                if error.kind() == ConnectorErrorKind::NotFound {
                    ConnectorError::table_object_binding(
                        ConnectorTableObjectBindingFailure::Missing,
                        "Iceberg table no longer resolves for its durable object binding",
                    )
                } else {
                    error
                }
            })?;
        if binding.object_id != expected_object_id {
            return Err(ConnectorError::table_object_binding(
                ConnectorTableObjectBindingFailure::Replaced,
                "Iceberg table no longer matches its durable object binding",
            ));
        }
        Ok(binding)
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let (table_name, metadata_table_type) =
            resolve_table_request(&request.table.table, request.resolution)?;
        // A metadata load is the Provider's observation boundary for catalog
        // truth. External engines can evolve a REST/Hadoop table without going
        // through this process, so a process-lifetime physical cache entry must
        // not seal an obsolete schema or snapshot into a new SQL statement.
        self.runtime
            .control_state()
            .invalidate_table(&request.table.namespace, &table_name);
        let loaded = self
            .runtime
            .load_table_classified_with_credential_lease_collection(
                &request.table.namespace,
                &table_name,
                request.context.vended_credential_lease_collection(),
                Some(&request.context),
            )
            .map_err(classified_control_error)?;
        let metadata = loaded.table.metadata();
        let definition_schema = metadata.current_schema().clone();
        let table_comment = metadata.properties().get("comment").cloned();
        let mut base_schema =
            crate::schema_mapping::sql_read_schema_from_iceberg(metadata.current_schema())
                .map_err(corrupt)?;
        let hidden_columns = hidden_internal_columns(metadata.properties());
        base_schema = annotate_hidden_fields(base_schema, &hidden_columns);
        // Carry the same frozen field facts a scan output schema carries, so the
        // admitted projection and the scan the provider later returns stay
        // field-for-field identical.
        base_schema = crate::schema_mapping::annotate_read_schema_from_scan_model(
            &base_schema,
            &iceberg_schema_def(metadata.current_schema()),
        )
        .map_err(corrupt)?;
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
        if matches!(metadata_table_type, Some(MetadataTableType::Partitions))
            && let Some(snapshot_id) = metadata.current_snapshot_id()
        {
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
            // The table schema exposed to SQL carries the Iceberg metadata
            // pseudo-columns after the storage fields, in exactly the order
            // `projected_schema` uses for scans. Their planning facts are what
            // marks them hidden and row-lineage-owned, so omitting them here
            // would make `_file`, `_pos`, `_row_id` and
            // `_last_updated_sequence_number` unresolvable.
            let mut fields = base_schema.fields().to_vec();
            fields.extend(metadata_arrow_fields(&payload.metadata_columns)?);
            Arc::new(Schema::new_with_metadata(
                fields,
                base_schema.metadata().clone(),
            ))
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

impl ConnectorScanPlanning for IcebergMetadata {
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
                .load_table_for_request(&table.namespace, &table.table, &request.context)
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
                mode: IcebergScanModeV1::ChangeWindow {
                    delta: Box::new(delta),
                },
            };
            return ConnectorScan::try_new_change_window(
                ConnectorProviderBindingKey {
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
                    .load_table_for_request(&table.namespace, &table.table, &request.context)
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
            ConnectorProviderBindingKey {
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
            return self.plan_change_window_splits(&scan, delta.as_ref().as_ref(), request);
        }
        let files = self.scan_files(&scan, &request.context)?;
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
        // Footer reads are FE-side object-store I/O during candidate planning.
        // Bind this exact request before inspecting a file so a vended catalog
        // resolves only through the attempt-local capability rather than the
        // generation-scoped startup binding.
        let planning_binding = self
            .runtime
            .resources()
            .planning_binding()
            .for_request(request.context.clone());
        let leaves = materialize_local_scan_units(
            &planning_binding,
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

impl IcebergMetadata {
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

    /// Resolve one frozen rewrite cohort's source into the exact file set its
    /// artifact froze. The handle deliberately carries no table metadata: the
    /// artifact, not the current snapshot, is the authority for what a rewrite
    /// reads. Returns the position-delete facts when the cohort rewrites
    /// position deletes.
    fn scan_files(
        &self,
        scan: &IcebergScanPayload,
        request_context: &novarocks_spi::connector::ConnectorRequestContext,
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
                    .load_table_for_request(
                        &scan.table.namespace,
                        &scan.table.table,
                        request_context,
                    )
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

/// Read back the frozen facts a staged write target carries.
///
/// This is the exact inverse of [`IcebergMetadata::staged_write_table_handle`]:
/// a staged CTAS target has no catalog entry, so the metadata a write session
/// would otherwise have loaded travels inside the handle the staged-create
/// capability minted. Decoding it here is the only way that metadata is ever
/// read back, and the handle's owner is checked first so one catalog's staged
/// target cannot be opened against another's generation.
pub(crate) fn staged_target_metadata(
    owner: &ConnectorInstanceId,
    table: &ConnectorTableHandle,
) -> Result<IcebergStagedTargetMetadata, ConnectorError> {
    if table.owner() != owner {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "staged Iceberg write target belongs to another connector instance",
        ));
    }
    let payload: IcebergTablePayload = decode_payload(table.payload(), "staged table handle")?;
    if payload.metadata_table_type.is_some() {
        return Err(corrupt(
            "Iceberg metadata tables cannot be staged write targets",
        ));
    }
    let table_info = payload.table_info.ok_or_else(|| {
        corrupt("staged Iceberg write target is missing its frozen table descriptor")
    })?;
    let serialized = table_info
        .serialized_metadata
        .as_deref()
        .ok_or_else(|| corrupt("staged Iceberg write target is missing frozen metadata"))?;
    let metadata: crate::iceberg::spec::TableMetadata =
        serde_json::from_str(serialized).map_err(|error| {
            corrupt(format!(
                "decode staged Iceberg write target metadata: {error}"
            ))
        })?;
    Ok(IcebergStagedTargetMetadata {
        namespace: payload.namespace,
        table: payload.table,
        metadata,
    })
}

/// The provider-private table one copy-on-write branch re-reads.
///
/// A copy-on-write branch replaces one whole data file, so the read that
/// produces its replacement rows names exactly that file and pins the base
/// snapshot the session froze. It is built from the session's own loaded
/// metadata rather than from an admitted handle, because a session's target is
/// resolved by name and never arrives as one.
pub(crate) fn frozen_copy_on_write_source_payload(
    catalog: &ConnectorInstanceId,
    namespace: &str,
    table_name: &str,
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file: IcebergDataFileInfo,
) -> Result<IcebergTablePayload, ConnectorError> {
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        corrupt("Iceberg copy-on-write base snapshot is absent from its metadata")
    })?;
    let snapshot_schema = snapshot.schema(metadata).map_err(|error| {
        corrupt(format!(
            "resolve Iceberg copy-on-write snapshot schema: {error}"
        ))
    })?;
    Ok(IcebergTablePayload {
        namespace: namespace.to_string(),
        table: table_name.to_string(),
        table_info: Some(IcebergTableInfo {
            catalog: catalog.as_str().to_string(),
            namespace: namespace.to_string(),
            table: table_name.to_string(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: Some(snapshot_id),
            schema_id: snapshot_schema.schema_id(),
            location: metadata.location().to_string(),
            schema: iceberg_schema_def(&snapshot_schema),
            serialized_metadata: Some(serde_json::to_string(metadata).map_err(|error| {
                corrupt(format!(
                    "serialize Iceberg copy-on-write source metadata: {error}"
                ))
            })?),
            serialized_metadata_rows: None,
        }),
        metadata_columns: metadata_column_names(metadata),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: Some(vec![file]),
        // A frozen source carries a complete explicit file set and must never
        // fall back to a catalog lookup.
        row_mutation_frozen_source: true,
        logical_type_columns: logical_type_columns(metadata.properties()),
        hidden_columns: hidden_internal_columns(metadata.properties()),
    })
}

/// The frozen target one staged write session opens on.
///
/// A registered table is loaded from the catalog on every session; a staged one
/// cannot be, so this is what a staged handle stands in for.
pub(crate) struct IcebergStagedTargetMetadata {
    pub namespace: String,
    pub table: String,
    pub metadata: crate::iceberg::spec::TableMetadata,
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
        delta: Box<Option<crate::change_planning::IcebergDeltaScanPlan>>,
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

pub(crate) fn projected_schema(
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
    let table_info = table
        .table_info
        .as_ref()
        .ok_or_else(|| corrupt("Iceberg table handle has no frozen table descriptor"))?;
    let storage_schema = if table.row_mutation_frozen_source
        || table_info.schema_id != metadata.current_schema_id()
    {
        let snapshot_id = table_info.current_snapshot_id.ok_or_else(|| {
            corrupt("Iceberg exact table source has no snapshot for its frozen schema")
        })?;
        metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| corrupt("Iceberg exact table source snapshot is absent"))?
            .schema(&metadata)
            .map_err(|error| corrupt(format!("resolve exact table source schema: {error}")))?
    } else {
        metadata.current_schema().clone()
    };
    let storage =
        crate::schema_mapping::sql_read_schema_from_iceberg(&storage_schema).map_err(corrupt)?;
    // Field IDs survive the Arrow conversion but initial defaults do not, so the
    // frozen schema has to re-stamp them before the scan schema leaves the
    // provider. Readers backfill a missing column from that metadata.
    let storage = match table.table_info.as_ref() {
        Some(table_info) => crate::schema_mapping::annotate_read_schema_from_scan_model(
            &storage,
            &table_info.schema,
        )
        .map_err(corrupt)?,
        None => storage,
    };
    // Hidden columns are annotated exactly as `load_table` annotates them: the
    // admitted projection is compared against this schema field for field.
    let storage = annotate_hidden_fields(storage, &table.hidden_columns);
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
    provider: &IcebergMetadata,
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

/// Rebuild a control-runtime error that kept its classification. Only
/// `Unavailable` is retryable before progress; a classified absence or
/// rejection is terminal for the request that raised it.
fn classified_control_error((kind, message): (ConnectorErrorKind, String)) -> ConnectorError {
    match kind {
        ConnectorErrorKind::Unavailable => unavailable(message),
        kind => ConnectorError::new(kind, message),
    }
}

#[cfg(test)]
mod hidden_column_tests {
    use std::collections::HashMap;

    use super::{APPLY_KEY_COLUMN_PROPERTY, HIDDEN_COLUMNS_PROPERTY, hidden_internal_columns};

    #[test]
    fn an_mv_targets_apply_key_and_state_columns_are_hidden_from_sql() {
        let properties = HashMap::from([
            (
                APPLY_KEY_COLUMN_PROPERTY.to_string(),
                "__nova_base_row_id".to_string(),
            ),
            (
                HIDDEN_COLUMNS_PROPERTY.to_string(),
                "__sum_state_v1, __count_state_v1".to_string(),
            ),
            ("comment".to_string(), "not hidden".to_string()),
        ]);

        let hidden = hidden_internal_columns(&properties);

        assert_eq!(
            hidden,
            vec!["__nova_base_row_id", "__sum_state_v1", "__count_state_v1"],
            "the apply key comes first and both property lists contribute; missing \
             either one leaks an engine-owned column into every SELECT * on the target"
        );
    }

    #[test]
    fn a_plain_table_hides_nothing() {
        assert!(hidden_internal_columns(&HashMap::new()).is_empty());
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
    use crate::resources::IcebergMetadataResources;
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

    fn provider() -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
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
        let resources = IcebergMetadataResources::new(binding, executor.handle().clone());
        let runtime = Arc::new(
            IcebergMetadataContext::try_new(
                IcebergCatalogControlState::new(configuration),
                resources,
            )
            .expect("control runtime"),
        );
        let provider = IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            },
            ProviderBindingEpoch::from_bytes([7; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    fn exact_revision_handle(
        provider: &IcebergMetadata,
        table_uuid: &str,
        snapshot_id: i64,
    ) -> ConnectorTableHandle {
        let payload = IcebergTablePayload {
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_info: Some(IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: Some(table_uuid.to_string()),
                current_snapshot_id: Some(snapshot_id),
                schema_id: 1,
                location: "s3://warehouse/db/orders".to_string(),
                schema: crate::scan_model::IcebergSchemaDef { fields: Vec::new() },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            }),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        };
        ConnectorTableHandle::try_new(
            provider.descriptor.instance_id.clone(),
            encode_payload(&payload, "test exact revision", 64 * 1024).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn exact_semantic_revision_comes_from_the_same_frozen_handle() {
        let (_runtime, _warehouse, provider) = provider();
        let first = provider
            .exact_semantic_revision(
                &exact_revision_handle(&provider, "uuid-a", 101),
                ConnectorReadSelector::Current,
            )
            .unwrap();
        let newer = provider
            .exact_semantic_revision(
                &exact_revision_handle(&provider, "uuid-a", 102),
                ConnectorReadSelector::Current,
            )
            .unwrap();
        let replacement = provider
            .exact_semantic_revision(
                &exact_revision_handle(&provider, "uuid-b", 101),
                ConnectorReadSelector::Current,
            )
            .unwrap();
        assert_ne!(first, newer);
        assert_ne!(first, replacement);
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
        provider: &IcebergMetadata,
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
            .scan_files(&scan, &context())
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
