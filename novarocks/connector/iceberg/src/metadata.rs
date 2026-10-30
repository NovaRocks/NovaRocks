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

pub(crate) mod existing_serde_projection;

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow::datatypes::{Field, Schema, SchemaRef};
use bytes::Bytes;
use novarocks_spi::connector::ConnectorProviderBindingKey;
use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorCanonicalReadPoint, ConnectorError, ConnectorErrorKind,
    ConnectorExactSemanticRevision, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListNamespacesRequest, ConnectorListTablesRequest, ConnectorMetadata,
    ConnectorMutationOperationId, ConnectorNamespaceIdentity, ConnectorNamespaceRequest,
    ConnectorOperationControl, ConnectorPredicateDisposition, ConnectorPredicateDispositionKind,
    ConnectorReadNamedReference, ConnectorReadPurpose, ConnectorReadReferenceFacts,
    ConnectorReadReferenceFactsRequest, ConnectorReadReferenceKind, ConnectorReadSelector,
    ConnectorReadSnapshotLogEntry, ConnectorScalarType, ConnectorScalarValue, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorScanSelection, ConnectorSplit,
    ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStaticComparisonOp, ConnectorStaticPredicate, ConnectorStaticPredicateKind,
    ConnectorTableDefinitionFacts, ConnectorTableHandle, ConnectorTableIdentity,
    ConnectorTableMetadata, ConnectorTableObjectBinding, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectId, ConnectorTableObjectRebindRequest,
    ConnectorTableObjectSelector, ConnectorTablePlanningFacts, ConnectorTableRequest,
    ConnectorTableResolution, ProviderBindingEpoch, validate_static_predicates,
};
use serde::{Deserialize, Serialize};

use crate::file_reader::execution_payload::{
    ICEBERG_SPLIT_V5, IcebergFrozenScanUnitPayload, IcebergScanFactColumnV1, SplitPayload,
    canonical_split_name_mapping, materialize_local_scan_units, scan_fact_scalar_type,
};
use crate::manifest::{
    data_file_with_stats_to_iceberg_data_file_info, extract_data_files_with_stats_at_with_control,
};
use crate::metadata_batch_reader::{
    MetadataTableType, metadata_output_schema, metadata_table_output_columns,
};
use crate::metadata_context::IcebergMetadataContext;
use crate::planning_facts::{
    IcebergTablePlanningFactsInput, snapshot_source_planning_facts, table_planning_facts,
};
use crate::scan_model::{
    IcebergDataFileInfo, IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain,
    IcebergPhysicalPredicateOp, IcebergPhysicalPredicateValue, IcebergTableInfo,
};
use crate::schema_facts::{iceberg_schema_def, row_lineage_enabled};

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
    runtime: Arc<IcebergMetadataContext>,
    generation_owner: Arc<GenerationOwner>,
}

impl IcebergMetadata {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        runtime
            .novarocks_catalog()
            .bind_publication_generation(incarnation)
            .expect("one materialized catalog client belongs to exactly one provider generation");
        if let Some(activity) = runtime.resources().catalog_runtime().activity() {
            activity.bind_epoch(incarnation);
        }
        Self {
            descriptor,
            incarnation,
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
        if context.is_cancelled() {
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
        // Physical-object capture is FE catalog preparation. It retains the
        // original stop/deadline checks, with no memory grant or byte quota.
        let mut request_observer = |_: std::alloc::Layout| pure_catalog_control(&context);
        let mut work_observer = |_: usize| pure_catalog_control(&context);
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request_observer,
            &mut work_observer,
        );
        let metadata = self
            .load_table(
                ConnectorTableRequest {
                    table,
                    resolution,
                    context: context.clone(),
                },
                &mut creation,
            )
            .map_err(metadata_creation_error)?;
        self.table_object_binding_from_metadata(metadata)
    }

    fn table_object_binding_from_metadata(
        &self,
        metadata: ConnectorTableMetadata,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
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

    async fn current_table_object_binding_async(
        &self,
        table: ConnectorTableIdentity,
        resolution: ConnectorTableResolution,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        let preparation = self
            .prepare_table(ConnectorTableRequest {
                table,
                resolution,
                context: context.clone(),
            })
            .await?;
        let mut request_observer = |_: std::alloc::Layout| pure_catalog_control(&context);
        let mut work_observer = |_: usize| pure_catalog_control(&context);
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request_observer,
            &mut work_observer,
        );
        let metadata = self
            .create_prepared_table(preparation, &mut creation)
            .map_err(metadata_creation_error)?;
        self.table_object_binding_from_metadata(metadata)
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
            metadata_location: None,
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
            }),
            metadata_columns: metadata_column_names(&metadata),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
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
    fn provider_metadata_cpu_author(
        &self,
    ) -> Option<(
        novarocks_spi::connector::ConnectorProviderBindingKey,
        std::any::TypeId,
    )> {
        Some((
            novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            },
            std::any::TypeId::of::<
                crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
            >(),
        ))
    }

    fn admit_table_job(
        &self,
        request: novarocks_spi::connector::ConnectorTableJobAdmissionRequest,
    ) -> Result<novarocks_spi::connector::ConnectorTableJobAdmission, ConnectorError> {
        use crate::catalog::admission::{
            CatalogAdmission, CatalogAdmissionRequest, CatalogInitiation, CatalogOperation,
            connector_unsupported,
        };
        use novarocks_spi::connector::{ConnectorTableJobAdmission, ConnectorTableJobKind};
        if request.table.instance_id != self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table job has a foreign connector instance",
            ));
        }
        self.validate_context(&request.context)?;
        let operation = match request.job {
            ConnectorTableJobKind::Statistics => CatalogOperation::Statistics,
            ConnectorTableJobKind::RewriteDataFiles => CatalogOperation::RewriteDataFiles,
        };
        self.runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                operation,
                crate::catalog::CatalogTableName::new(request.table.namespace, request.table.table),
                CatalogInitiation::StatementJob,
            ))
            .map(|admission| match admission {
                CatalogAdmission::Admitted => ConnectorTableJobAdmission::Detached,
                CatalogAdmission::AdmittedAwaitingCompletion => {
                    ConnectorTableJobAdmission::AwaitTerminal
                }
            })
            .map_err(connector_unsupported)
    }

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

    fn read_selector_from_exact_revision(
        &self,
        table: &ConnectorTableHandle,
        revision: &ConnectorExactSemanticRevision,
    ) -> Result<ConnectorReadSelector, ConnectorError> {
        let payload = self.table_payload(table)?;
        if payload.metadata_table_type.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata aliases cannot admit MV exact revisions",
            ));
        }
        let table_info = payload.table_info.as_ref().ok_or_else(|| {
            corrupt("Iceberg exact revision selector is missing its frozen table descriptor")
        })?;
        let table_uuid = table_info.table_uuid.as_deref().ok_or_else(|| {
            corrupt("Iceberg exact revision selector is missing its physical UUID")
        })?;
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::copy_from_slice(table_uuid.as_bytes()))?;
        let Some(ConnectorCanonicalReadPoint::Snapshot(Some(snapshot_id))) =
            revision.canonical_read_point()
        else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg exact revision names no snapshot read point",
            ));
        };
        let expected = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            self.descriptor.provider_id.clone(),
            &object_id,
            Some(snapshot_id),
        )?;
        if revision != &expected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg exact revision does not belong to this provider and table object",
            ));
        }
        let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
            corrupt("Iceberg exact revision selector is missing frozen table metadata")
        })?;
        let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
            .map_err(|error| {
                corrupt(format!(
                    "decode Iceberg exact revision selector metadata: {error}"
                ))
            })?;
        select_snapshot(&metadata, ConnectorReadSelector::SnapshotId(snapshot_id))?;
        Ok(ConnectorReadSelector::SnapshotId(snapshot_id))
    }

    fn list_namespaces(
        &self,
        request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.instance_id)?;
        let namespaces = self
            .runtime
            .list_namespaces_for_request(&request.context, request.bound);
        self.validate_context(&request.context)?;
        namespaces
            .map_err(classified_control_error)?
            .into_iter()
            .map(|namespace| {
                self.validate_context(&request.context)?;
                Ok(ConnectorNamespaceIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: Arc::from(namespace),
                })
            })
            .collect()
    }

    fn list_namespaces_async(
        &self,
        request: ConnectorListNamespacesRequest,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorNamespaceIdentity>, ConnectorError>>
    {
        Box::pin(async move {
            self.validate_context(&request.context)?;
            self.ensure_owner(&request.instance_id)?;
            let namespaces = self
                .runtime
                .list_namespaces_for_request_async(&request.context, request.bound)
                .await;
            self.validate_context(&request.context)?;
            namespaces
                .map_err(classified_control_error)?
                .into_iter()
                .map(|namespace| {
                    self.validate_context(&request.context)?;
                    Ok(ConnectorNamespaceIdentity {
                        instance_id: self.descriptor.instance_id.clone(),
                        namespace: Arc::from(namespace),
                    })
                })
                .collect()
        })
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.namespace.instance_id)?;
        let result = self
            .runtime
            .namespace_exists_for_request(&request.namespace.namespace, &request.context);
        self.validate_context(&request.context)?;
        result.map_err(unavailable)
    }

    fn namespace_exists_async(
        &self,
        request: ConnectorNamespaceRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<bool, ConnectorError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.validate_context(&request.context)?;
            self.ensure_owner(&request.namespace.instance_id)?;
            let result = self
                .runtime
                .namespace_exists_for_request_async(&request.namespace.namespace, &request.context)
                .await;
            self.validate_context(&request.context)?;
            result.map_err(unavailable)
        })
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let (table, metadata_type) =
            resolve_table_request(&request.table.table, request.resolution)?;
        if metadata_type.is_some() {
            let result = self.runtime.table_exists_for_request(
                &request.table.namespace,
                &table,
                &request.context,
            );
            self.validate_context(&request.context)?;
            return result.map_err(unavailable);
        }
        let result = self.runtime.table_exists_for_request(
            &request.table.namespace,
            &table,
            &request.context,
        );
        self.validate_context(&request.context)?;
        result.map_err(unavailable)
    }

    fn table_exists_async(
        &self,
        request: ConnectorTableRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<bool, ConnectorError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.validate_context(&request.context)?;
            self.ensure_owner(&request.table.instance_id)?;
            let (table, metadata_type) =
                resolve_table_request(&request.table.table, request.resolution)?;
            if metadata_type.is_some() {
                let result = self
                    .runtime
                    .table_exists_for_request_async(
                        &request.table.namespace,
                        &table,
                        &request.context,
                    )
                    .await;
                self.validate_context(&request.context)?;
                return result.map_err(unavailable);
            }
            let result = self
                .runtime
                .table_exists_for_request_async(&request.table.namespace, &table, &request.context)
                .await;
            self.validate_context(&request.context)?;
            result.map_err(unavailable)
        })
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.namespace.instance_id)?;
        let tables = self.runtime.list_tables_for_request(
            &request.namespace.namespace,
            &request.context,
            request.bound,
        );
        self.validate_context(&request.context)?;
        tables
            .map_err(classified_control_error)?
            .into_iter()
            .map(|table| {
                self.validate_context(&request.context)?;
                Ok(ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
            })
            .collect()
    }

    fn list_tables_async(
        &self,
        request: ConnectorListTablesRequest,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorTableIdentity>, ConnectorError>> {
        Box::pin(async move {
            self.validate_context(&request.context)?;
            self.ensure_owner(&request.namespace.instance_id)?;
            let tables = self
                .runtime
                .list_tables_for_request_async(
                    &request.namespace.namespace,
                    &request.context,
                    request.bound,
                )
                .await;
            self.validate_context(&request.context)?;
            tables
                .map_err(classified_control_error)?
                .into_iter()
                .map(|table| {
                    self.validate_context(&request.context)?;
                    Ok(ConnectorTableIdentity {
                        instance_id: self.descriptor.instance_id.clone(),
                        namespace: request.namespace.namespace.clone(),
                        table: Arc::from(table),
                    })
                })
                .collect()
        })
    }

    fn read_reference_facts(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        // A new statement must resolve a named ref against current catalog
        // metadata. Keep the request-local snapshot frozen after this load.
        self.runtime
            .control_state()
            .invalidate_table(&request.table.namespace, &request.table.table);
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

    fn read_reference_facts_async(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorReadReferenceFacts, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.validate_context(&request.context)?;
            self.ensure_owner(&request.table.instance_id)?;
            // A new statement must resolve a named ref against current catalog
            // metadata. Keep the request-local snapshot frozen after this load.
            self.runtime
                .control_state()
                .invalidate_table(&request.table.namespace, &request.table.table);
            let loaded = self
                .runtime
                .load_table_for_request_async(
                    &request.table.namespace,
                    &request.table.table,
                    &request.context,
                )
                .await
                .map_err(unavailable)?;
            read_reference_facts(loaded.table.metadata(), &request.context)
        })
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.require_current_table_object_selector(request.selector)?;
        self.current_table_object_binding(request.table, request.resolution, request.context)
    }

    fn capture_table_object_binding_async(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.require_current_table_object_selector(request.selector)?;
            self.current_table_object_binding_async(
                request.table,
                request.resolution,
                request.context,
            )
            .await
        })
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

    fn rebind_table_object_binding_async(
        &self,
        request: ConnectorTableObjectRebindRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.require_current_table_object_selector(request.selector)?;
            let expected_object_id = request.expected_object_id;
            let binding = self
                .current_table_object_binding_async(
                    request.table,
                    request.resolution,
                    request.context,
                )
                .await
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
        })
    }

    fn capture_table_object_with_snapshot_cpu(
        &self,
        request: ConnectorTableObjectCaptureRequest,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.require_current_table_object_selector(request.selector)?;
            let preparation = self
                .prepare_table(ConnectorTableRequest {
                    table: request.table,
                    resolution: request.resolution,
                    context: request.context,
                })
                .await?;
            let facts = preparation
                .into_facts::<IcebergPreparedTableFacts>(&self.descriptor.instance_id)?;
            self.validate_metadata_binding_generation(&facts)?;
            let source = crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal::metadata_binding(self.clone(), facts, None);
            let mut input = Some(novarocks_spi::connector::ProviderMetadataCpuInput(
                source.clone(),
            ));
            cpu.submit_metadata_in(&mut input)?;
            std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
            source.take_binding_with_snapshot_result()
        })
    }

    fn capture_table_object_with_snapshot_cpu_in<'a>(
        &'a self,
        request: ConnectorTableObjectCaptureRequest,
        cpu: &'a Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        owner: &'a mut Option<novarocks_spi::connector::ConnectorProcessLocalPayload>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>,
                > + Send
                + 'a,
        >,
    > {
        if owner.is_some() {
            return Box::pin(async {
                Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "metadata capture original owner receiver is occupied",
                ))
            });
        }
        if let Err(error) = self.require_current_table_object_selector(request.selector) {
            return Box::pin(async move { Err(error) });
        }
        let source = crate::catalog_control::statistics::scoped_cpu::scoped_capture::MetadataCaptureOriginal::new(
            self.clone(), request, Arc::clone(cpu),
        );
        *owner = Some(novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.incarnation,
            },
            Arc::clone(&source),
        ));
        Box::pin(async move { source.drive().await })
    }

    fn capture_table_object_binding_for_statistics_async(
        &self,
        request: ConnectorTableObjectCaptureRequest,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.statistics_table_object_binding_original(
            request.table,
            request.resolution,
            request.selector,
            request.context,
            None,
            cpu,
        ))
    }

    fn rebind_table_object_binding_for_statistics_async(
        &self,
        request: ConnectorTableObjectRebindRequest,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.statistics_table_object_binding_original(
            request.table,
            request.resolution,
            request.selector,
            request.context,
            Some(request.expected_object_id),
            cpu,
        ))
    }

    fn prepare_table(
        &self,
        request: ConnectorTableRequest,
    ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
        Box::pin(self.prepare_table_original(request))
    }

    fn prepare_source_table(
        &self,
        request: ConnectorTableRequest,
    ) -> novarocks_spi::connector::ConnectorTablePreparationFuture<'_> {
        Box::pin(self.prepare_source_table_original(request))
    }

    fn create_prepared_table(
        &self,
        preparation: novarocks_spi::connector::ConnectorTablePreparation,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let facts =
            preparation.into_facts::<IcebergPreparedTableFacts>(&self.descriptor.instance_id)?;
        if facts.incarnation != self.incarnation
            || !Arc::ptr_eq(&facts.generation_owner, &self.generation_owner)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table preparation belongs to another exact generation",
            )
            .into());
        }
        creation.work(1)?;
        self.validate_context(&facts.request.context)?;
        self.create_table_from_observation(facts, creation)
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        creation.work(1)?;
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
        let snapshots = if metadata_table_type.is_none() {
            crate::catalog::SnapshotLoading::Refs
        } else {
            crate::catalog::SnapshotLoading::All
        };
        let loaded = self
            .runtime
            .observe_table_classified_for_request_with_snapshots(
                &request.table.namespace,
                &table_name,
                &request.context,
                snapshots,
            )
            .map_err(classified_control_error)?;
        self.create_table_from_observation(
            IcebergPreparedTableFacts {
                request,
                loaded,
                table_name,
                metadata_table_type,
                snapshots,
                incarnation: self.incarnation,
                generation_owner: Arc::clone(&self.generation_owner),
            },
            creation,
        )
    }
}

#[derive(Clone)]
pub(crate) struct IcebergPreparedTableFacts {
    request: ConnectorTableRequest,
    loaded: crate::loaded_table::IcebergPhysicalTable,
    table_name: String,
    metadata_table_type: Option<MetadataTableType>,
    snapshots: crate::catalog::SnapshotLoading,
    incarnation: ProviderBindingEpoch,
    generation_owner: Arc<GenerationOwner>,
}

impl IcebergPreparedTableFacts {
    pub(crate) fn rewrite_physical_table(&self) -> &crate::iceberg::table::Table {
        &self.loaded.table
    }
}

impl IcebergMetadata {
    async fn statistics_table_object_binding_original(
        &self,
        table: ConnectorTableIdentity,
        resolution: ConnectorTableResolution,
        selector: ConnectorTableObjectSelector,
        context: novarocks_spi::connector::ConnectorRequestContext,
        expected: Option<ConnectorTableObjectId>,
        cpu: Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.require_current_table_object_selector(selector)?;
        let preparation = self
            .prepare_table(ConnectorTableRequest {
                table,
                resolution,
                context,
            })
            .await
            .map_err(|error| {
                if expected.is_some() && error.kind() == ConnectorErrorKind::NotFound {
                    ConnectorError::table_object_binding(
                        ConnectorTableObjectBindingFailure::Missing,
                        "Iceberg table no longer resolves for its durable object binding",
                    )
                } else {
                    error
                }
            })?;
        // The one physical preparation is transferred, never cloned out of
        // an opaque Any. The worker only borrows this stable typed source.
        let facts =
            preparation.into_facts::<IcebergPreparedTableFacts>(&self.descriptor.instance_id)?;
        self.validate_metadata_binding_generation(&facts)?;
        let source =
            crate::catalog_control::statistics::scoped_cpu::StatisticsCpuOriginal::metadata_binding(
                self.clone(),
                facts,
                expected,
            );
        crate::catalog_control::statistics::scoped_cpu::run_binding(&cpu, &source).await?;
        source.take_binding_result()
    }
    fn validate_metadata_binding_generation(
        &self,
        facts: &IcebergPreparedTableFacts,
    ) -> Result<(), ConnectorError> {
        if facts.incarnation != self.incarnation
            || !Arc::ptr_eq(&facts.generation_owner, &self.generation_owner)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics table preparation belongs to another exact generation",
            ));
        }
        Ok(())
    }
    pub(crate) fn create_binding_with_snapshot_borrowed(
        &self,
        facts: &IcebergPreparedTableFacts,
        expected: &Option<ConnectorTableObjectId>,
    ) -> Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError> {
        self.validate_metadata_binding_generation(facts)?;
        let metadata = with_pure_schema_creation(&facts.request.context, |creation| {
            self.create_table_from_observation(facts.clone(), creation)
                .map_err(metadata_creation_error)
        })?;
        let binding = self.table_object_binding_from_metadata(metadata)?;
        if expected.as_ref().is_some_and(|id| id != &binding.object_id) {
            return Err(ConnectorError::table_object_binding(
                ConnectorTableObjectBindingFailure::Replaced,
                "Iceberg table no longer matches its durable object binding",
            ));
        }
        // Same immutable loaded observation, read on this SAME original CPU
        // phase. No handle-token interpretation or second Catalog observation.
        Ok((binding, facts.loaded.table.metadata().current_snapshot_id()))
    }
    async fn prepare_table_original(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<novarocks_spi::connector::ConnectorTablePreparation, ConnectorError> {
        self.prepare_table_observation_original(request, false)
            .await
    }
    async fn prepare_source_table_original(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<novarocks_spi::connector::ConnectorTablePreparation, ConnectorError> {
        if !matches!(
            request.resolution,
            ConnectorTableResolution::StrictBaseTable
        ) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "source observation requires an exact base table",
            ));
        }
        self.prepare_table_observation_original(request, true).await
    }
    async fn prepare_table_observation_original(
        &self,
        request: ConnectorTableRequest,
        source_observation: bool,
    ) -> Result<novarocks_spi::connector::ConnectorTablePreparation, ConnectorError> {
        self.validate_context(&request.context)?;
        self.ensure_owner(&request.table.instance_id)?;
        let (table_name, metadata_table_type) =
            resolve_table_request(&request.table.table, request.resolution)?;
        self.runtime
            .control_state()
            .invalidate_table(&request.table.namespace, &table_name);
        let snapshots = if source_observation || metadata_table_type.is_some() {
            crate::catalog::SnapshotLoading::All
        } else {
            crate::catalog::SnapshotLoading::Refs
        };
        let loaded = self
            .runtime
            .observe_table_original_for_request_with_snapshots_async(
                &request.table.namespace,
                &table_name,
                &request.context,
                snapshots,
            )
            .await?;
        Ok(novarocks_spi::connector::ConnectorTablePreparation::new(
            self.descriptor.instance_id.clone(),
            IcebergPreparedTableFacts {
                request,
                loaded,
                table_name,
                metadata_table_type,
                snapshots,
                incarnation: self.incarnation,
                generation_owner: Arc::clone(&self.generation_owner),
            },
        ))
    }
    pub(crate) fn statistics_read_preparation_bytes() -> Result<usize, ConnectorError> {
        fn bytes<A, B, F>(_: fn(A, B) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        bytes(Self::prepare_table_original)
            .checked_add(std::mem::size_of::<IcebergPreparedTableFacts>())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "statistics preparation layout overflow",
                )
            })
    }
    pub(crate) fn statistics_read_metadata(
        &self,
        facts: &IcebergPreparedTableFacts,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        if facts.incarnation != self.incarnation
            || !Arc::ptr_eq(&facts.generation_owner, &self.generation_owner)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics read preparation has a foreign exact generation",
            ));
        }
        with_pure_schema_creation(&facts.request.context, |creation| {
            self.create_table_from_observation(facts.clone(), creation)
                .map_err(metadata_creation_error)
        })
    }
    pub(crate) fn create_rewrite_table_borrowed(
        &self,
        facts: &IcebergPreparedTableFacts,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        if facts.incarnation != self.incarnation
            || !Arc::ptr_eq(&facts.generation_owner, &self.generation_owner)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite preparation has a foreign exact generation",
            )
            .into());
        }
        self.create_table_from_observation(facts.clone(), creation)
    }
    pub(crate) fn statistics_read_physical(
        facts: &IcebergPreparedTableFacts,
    ) -> &crate::iceberg::table::Table {
        &facts.loaded.table
    }
    pub(crate) fn statistics_binding_future_bytes() -> usize {
        fn bytes<A, B, C, D, E, F, G, H: std::future::Future>(
            _: fn(A, B, C, D, E, F, G) -> H,
        ) -> usize {
            std::mem::size_of::<H>()
        }
        bytes(Self::statistics_table_object_binding_original)
    }
    fn create_table_from_observation(
        &self,
        facts: IcebergPreparedTableFacts,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let IcebergPreparedTableFacts {
            request,
            loaded,
            table_name,
            metadata_table_type,
            snapshots,
            ..
        } = facts;
        build_table_metadata(
            TableMetadataBuildInput {
                descriptor: &self.descriptor,
                incarnation: self.incarnation,
                request,
                loaded,
                table_name,
                metadata_table_type,
                snapshots,
                projection: TableMetadataProjection::Current,
            },
            creation,
        )
    }
}

/// The read projection is separate from the real metadata used by control
/// handles. Snapshot sources never supply Current write partition membership.
#[derive(Clone, Copy)]
pub(crate) enum TableMetadataProjection {
    Current,
    SnapshotTo(i64),
}

pub(crate) struct TableMetadataBuildInput<'a> {
    pub(crate) descriptor: &'a ConnectorInstanceDescriptor,
    pub(crate) incarnation: ProviderBindingEpoch,
    pub(crate) request: ConnectorTableRequest,
    pub(crate) loaded: crate::loaded_table::IcebergPhysicalTable,
    pub(crate) table_name: String,
    pub(crate) metadata_table_type: Option<MetadataTableType>,
    pub(crate) snapshots: crate::catalog::SnapshotLoading,
    pub(crate) projection: TableMetadataProjection,
}

/// Project the full public metadata once in the caller's original CPU turn.
/// The opaque control payload always describes the actual observed metadata.
pub(crate) fn build_table_metadata(
    input: TableMetadataBuildInput<'_>,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    ConnectorTableMetadata,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    let TableMetadataBuildInput {
        descriptor,
        incarnation,
        request,
        loaded,
        table_name,
        metadata_table_type,
        snapshots,
        projection,
    } = input;
    let metadata = loaded.table.metadata();
    let (definition_schema, projected_snapshot, snapshot_source) = match projection {
        TableMetadataProjection::Current => (
            metadata.current_schema().clone(),
            metadata.current_snapshot_id(),
            false,
        ),
        TableMetadataProjection::SnapshotTo(snapshot_id) => {
            if metadata_table_type.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Snapshot-source metadata requires a base table",
                )
                .into());
            }
            let schema = crate::change_planning::resolved_window_schema(metadata, snapshot_id)?;
            (schema, Some(snapshot_id), true)
        }
    };
    let table_comment = metadata.properties().get("comment").cloned();
    let nominal_document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let mut base_schema = crate::scalar_integer_domain::sql_schema_from_document(
        definition_schema.as_ref(),
        &nominal_document,
    )?;
    let hidden_columns = hidden_internal_columns(metadata.properties());
    base_schema = annotate_hidden_fields(base_schema, &hidden_columns);
    // Carry the same frozen field facts a scan output schema carries, so the
    // admitted projection and the scan the provider later returns stay
    // field-for-field identical.
    base_schema = crate::schema_mapping::annotate_read_schema_from_scan_model(
        &base_schema,
        &iceberg_schema_def(definition_schema.as_ref()),
    )
    .map_err(corrupt)?;
    let metadata_columns = metadata_column_names(metadata);
    let table_info = IcebergTableInfo {
        catalog: descriptor.instance_id.as_str().to_string(),
        namespace: request.table.namespace.to_string(),
        table: table_name.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def(metadata.current_schema()),
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|error| corrupt(format!("serialize Iceberg table metadata: {error}")))?,
        ),
    };
    let payload = IcebergTablePayload {
        namespace: request.table.namespace.to_string(),
        table: table_name.clone(),
        metadata_location: loaded.table.metadata_location().map(str::to_owned),
        table_info: Some(table_info),
        metadata_columns,
        metadata_table_type,
        prepared_files: Vec::new(),
        explicit_files: None,
        row_mutation_frozen_source: false,
        hidden_columns,
    };
    let (schema, execution_schema) = if let Some(metadata_table_type) = payload.metadata_table_type
    {
        let columns = metadata_table_output_columns(metadata_table_type, metadata, creation)?;
        let schema = metadata_output_schema(&columns).map_err(corrupt)?;
        let execution =
            crate::metadata_batch_reader::metadata_output_execution_schema(&columns, creation)?;
        (schema, execution)
    } else {
        // The table schema exposed to SQL carries the Iceberg metadata
        // pseudo-columns after the storage fields, in exactly the order
        // `projected_schema` uses for scans. Their planning facts are what
        // marks them hidden and row-lineage-owned, so omitting them here
        // would make `_file`, `_pos`, `_row_id` and
        // `_last_updated_sequence_number` unresolvable.
        let mut fields = base_schema.fields().to_vec();
        fields.extend(metadata_arrow_fields(&payload.metadata_columns)?);
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            base_schema.metadata().clone(),
        ));
        let execution = crate::schema_mapping::catalog_value::table_with_document(
            &nominal_document,
            definition_schema.as_ref(),
            &payload.metadata_columns,
            creation,
        )?;
        (schema, execution)
    };
    let planning_facts = if payload.metadata_table_type.is_some() {
        ConnectorTablePlanningFacts::empty()
    } else {
        let input = IcebergTablePlanningFactsInput {
            schema: &schema,
            iceberg_schema: Some(definition_schema.as_ref()),
            metadata_columns: &payload.metadata_columns,
            hidden_columns: &payload.hidden_columns,
            nominal_declarations: &nominal_document.nominals,
            serialized_metadata: payload
                .table_info
                .as_ref()
                .and_then(|table| table.serialized_metadata.as_deref()),
            namespace: &request.table.namespace,
            instance_id: &descriptor.instance_id,
            context: &request.context,
        };
        if snapshot_source {
            snapshot_source_planning_facts(input)?
        } else {
            table_planning_facts(input)?
        }
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
        projected_snapshot,
    )?;
    let observed_source = creation.arc(
        crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts {
            access: Arc::new(crate::loaded_table::IcebergAttemptTableAccess::freeze(loaded.clone())),
            snapshots,
            projection,
        },
    )?;
    let source_facts =
        novarocks_spi::connector::ConnectorProcessLocalPayload::try_new_with_creation(
            ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id.clone(),
                incarnation,
            },
            observed_source,
            creation,
        )?;
    ConnectorTableMetadata::try_new_typed(
        novarocks_spi::connector::ConnectorTableMetadataHeader {
            identity: ConnectorTableIdentity {
                instance_id: descriptor.instance_id.clone(),
                namespace: request.table.namespace,
                table: Arc::from(table_name),
            },
            planning_facts,
            definition_facts,
            version: Some(Bytes::copy_from_slice(
                &definition_schema.schema_id().to_le_bytes(),
            )),
            statistics_data_version: Some(statistics_data_version),
            table: ConnectorTableHandle::try_new(
                descriptor.instance_id.clone(),
                encode_payload(
                    &payload,
                    "table handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
        },
        schema,
        execution_schema,
        creation,
    )
    .map(|metadata| metadata.with_source_read_facts(source_facts))
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
            projected_metadata_schema(&table, &request.projection, &request.context)?
        } else {
            projected_schema(&table, &request.projection)?
        };
        let ConnectorScanSelection::Snapshot(selector) = request.selection;
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
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata relations read only through the typed provider split source",
            ));
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
                let control = request_context.clone();
                let result = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        extract_data_files_with_stats_at_with_control(
                            &table,
                            snapshot_id,
                            Some(&control as &dyn ConnectorOperationControl),
                        )
                        .await
                    });
                self.validate_context(request_context)?;
                result
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
        metadata_location: None,
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
        }),
        metadata_columns: metadata_column_names(metadata),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: Some(vec![file]),
        // A frozen source carries a complete explicit file set and must never
        // fall back to a catalog lookup.
        row_mutation_frozen_source: true,
        hidden_columns: hidden_internal_columns(metadata.properties()),
    })
}

/// The same frozen payload constructor, using the original checked serializer
/// for Nova's metadata JSON/default/schema outputs. The returned guard moves
/// into the source's encoded backing before any handle can escape.
pub(crate) fn frozen_copy_on_write_source_payload_with_original_scope(
    catalog: &ConnectorInstanceId,
    namespace: &str,
    table_name: &str,
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file: IcebergDataFileInfo,
    scope: &crate::commit::write_stack::control::cow_begin::CowBeginScope<'_>,
) -> Result<
    (
        IcebergTablePayload,
        novarocks_spi::connector::ConnectorPayloadRetentionGuard,
    ),
    novarocks_spi::connector::ConnectorCowBeginCause,
> {
    use crate::commit::write_stack::control::cow_begin::projection_error;
    scope.active()?;
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        corrupt("Iceberg copy-on-write base snapshot is absent from its metadata")
    })?;
    let schema = snapshot.schema(metadata).map_err(|e| {
        corrupt(format!(
            "resolve Iceberg copy-on-write snapshot schema: {e}"
        ))
    })?;
    let plan = existing_serde_projection::Plan::inspect(metadata, &schema, scope)
        .map_err(projection_error)?;
    let mut checked = plan.for_copies(1, scope).map_err(projection_error)?;
    let output = checked.build_one().map_err(projection_error)?;
    let (serialized_metadata, schema, guard) = output.into_guarded_parts();
    let payload = IcebergTablePayload {
        namespace: namespace.to_string(),
        table: table_name.to_string(),
        metadata_location: None,
        table_info: Some(IcebergTableInfo {
            catalog: catalog.as_str().to_string(),
            namespace: namespace.to_string(),
            table: table_name.to_string(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: Some(snapshot_id),
            schema_id: snapshot
                .schema(metadata)
                .map_err(|_| corrupt("Iceberg copy-on-write snapshot schema changed"))?
                .schema_id(),
            location: metadata.location().to_string(),
            schema,
            serialized_metadata: Some(serialized_metadata),
        }),
        metadata_columns: metadata_column_names(metadata),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: Some(vec![file]),
        row_mutation_frozen_source: true,
        hidden_columns: hidden_internal_columns(metadata.properties()),
    };
    scope.active()?;
    Ok((payload, guard))
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
    /// Exact catalog metadata pointer captured with `table_info`. Staged and
    /// synthetic handles have no committed Current generation.
    pub metadata_location: Option<String>,
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
    projected_schema_with_metadata(table, projection, &metadata)
}

/// COW uses its SAME loaded, wire-normalized immutable metadata owner.
/// The ordinary entry above keeps its original decode and error ordering.
pub(crate) fn projected_schema_with_metadata(
    table: &IcebergTablePayload,
    projection: &[usize],
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<SchemaRef, ConnectorError> {
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
            .schema(metadata)
            .map_err(|error| corrupt(format!("resolve exact table source schema: {error}")))?
    } else {
        metadata.current_schema().clone()
    };
    let storage = crate::scalar_integer_domain::metadata_sql_schema(metadata, &storage_schema)?;
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
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<SchemaRef, ConnectorError> {
    // This is the legacy FE scan-negotiation format projection. It performs
    // pure preparation with the original cancellation/deadline control; no
    // memory grant or FE byte budget is minted by this source author.
    let mut request = |_: std::alloc::Layout| {
        if context.is_cancelled() {
            Err(novarocks_type_contract::CompileControlError::Cancelled)
        } else if Instant::now() >= context.deadline() {
            Err(novarocks_type_contract::CompileControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    };
    let mut work = |_: usize| {
        if context.is_cancelled() {
            Err(novarocks_type_contract::CompileControlError::Cancelled)
        } else if Instant::now() >= context.deadline() {
            Err(novarocks_type_contract::CompileControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    };
    let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
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
    let columns = metadata_table_output_columns(metadata_table_type, &metadata, &mut creation)
        .map_err(metadata_creation_error)?;
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

pub(crate) fn metadata_arrow_fields(names: &[String]) -> Result<Vec<Arc<Field>>, ConnectorError> {
    use crate::typed_read::{
        ALWAYS_BOUND_METADATA_COLUMNS, IcebergMetadataColumn, ROW_LINEAGE_METADATA_COLUMNS,
    };
    names
        .iter()
        .map(|name| {
            let metadata = ALWAYS_BOUND_METADATA_COLUMNS
                .into_iter()
                .chain(ROW_LINEAGE_METADATA_COLUMNS)
                .find(|metadata| metadata.column_name() == name)
                .ok_or_else(|| corrupt(format!("unknown Iceberg metadata column `{name}`")))?;
            let data_type = match metadata {
                IcebergMetadataColumn::Path => arrow::datatypes::DataType::Utf8,
                IcebergMetadataColumn::RowPosition
                | IcebergMetadataColumn::RowId
                | IcebergMetadataColumn::LastUpdatedSequenceNumber => {
                    arrow::datatypes::DataType::Int64
                }
                IcebergMetadataColumn::IsDeleted => {
                    return Err(corrupt(format!(
                        "Iceberg metadata column `{name}` is not stated to SQL"
                    )));
                }
            };
            // The NULL contract is the column's own: the read binding mints the
            // handle a frozen read publishes from the same owner, so the type a
            // scan is planned with is exactly the type its read publishes.
            Ok(Arc::new(
                Field::new(name, data_type, metadata.nullable()).with_metadata(HashMap::from([(
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
        .get(crate::iceberg::spec::TableProperties::PROPERTY_DEFAULT_NAME_MAPPING)
        .map(|mapping| canonical_split_name_mapping(mapping))
        .transpose()
}

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

pub(crate) fn metadata_column_names(metadata: &crate::iceberg::spec::TableMetadata) -> Vec<String> {
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
        ConnectorInstanceId, ConnectorProviderId, ConnectorRequestContext,
        ConnectorSplitPlanningMetrics,
    };

    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use crate::resources::IcebergMetadataResources;
    use crate::scan_model::{
        IcebergColumnStats, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
        IcebergPhysicalPredicateValue,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            256 * 1024,
            1024 * 1024,
        )
        .expect("request context")
    }

    fn provider() -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
        provider_with_catalog("hadoop", "127.0.0.1:1")
    }

    fn provider_with_catalog(
        catalog_type: &str,
        address: &str,
    ) -> (tokio::runtime::Runtime, tempfile::TempDir, IcebergMetadata) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
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
                format!("thrift://{address}"),
            ));
        } else if catalog_type == "rest" {
            properties.push((
                "iceberg.catalog.uri".to_string(),
                format!("http://{address}"),
            ));
        }
        let configuration = crate::catalog_config::parse_catalog_configuration("ice", &properties)
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

    #[test]
    fn async_namespace_exists_preserves_real_catalog_observation_and_owner_fence() {
        use crate::catalog::{
            CatalogNamespaceName, catalog_request::CatalogPublicationRequest, error::CatalogOutcome,
        };
        let (executor, _warehouse, metadata) = provider();
        executor.block_on(async {
            let request = ConnectorNamespaceRequest {
                namespace: ConnectorNamespaceIdentity {
                    instance_id: metadata.descriptor.instance_id.clone(),
                    namespace: Arc::from("prepared_test"),
                },
                context: context(),
            };
            assert!(
                !metadata
                    .namespace_exists_async(request.clone())
                    .await
                    .unwrap()
            );
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &metadata.runtime,
                metadata.incarnation,
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("prepared_test"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            assert!(
                metadata
                    .namespace_exists_async(request.clone())
                    .await
                    .unwrap()
            );
            let mut foreign = request;
            foreign.namespace.instance_id = ConnectorInstanceId::parse("foreign").unwrap();
            let error = metadata.namespace_exists_async(foreign).await.unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        });
    }

    #[test]
    fn async_object_rebind_keeps_the_real_frozen_uuid_and_typed_replacement() {
        use crate::catalog::CatalogNamespaceName;
        use crate::catalog::catalog_request::CatalogPublicationRequest;
        use crate::catalog::error::CatalogOutcome;
        use crate::iceberg::TableCreation;
        let (executor, _warehouse, metadata) = provider();
        executor.block_on(async {
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &metadata.runtime,
                metadata.incarnation,
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("prepared_test"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &metadata.runtime,
                metadata.incarnation,
                CatalogPublicationRequest::CreateTable {
                    namespace: CatalogNamespaceName::new("prepared_test"),
                    creation: TableCreation::builder()
                        .name("t".into())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let request = ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: metadata.descriptor.instance_id.clone(),
                    namespace: "prepared_test".into(),
                    table: "t".into(),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context(),
            };
            // Source capture requests complete history through its own port;
            // ordinary catalog projection retains its existing refs request.
            for (source, expected) in [(false, crate::catalog::SnapshotLoading::Refs), (true, crate::catalog::SnapshotLoading::All)] {
                let preparation = if source { metadata.prepare_source_table(request.clone()).await }
                    else { metadata.prepare_table(request.clone()).await }.unwrap();
                let observed = with_pure_schema_creation(&request.context, |creation| metadata.create_prepared_table(preparation, creation).map_err(metadata_creation_error)).unwrap();
                let facts = observed.source_read_facts().unwrap()
                    .value::<crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts>(metadata.descriptor.instance_id.as_str()).unwrap();
                assert_eq!(facts.snapshots, expected);
                assert!(matches!(facts.projection, TableMetadataProjection::Current));
                assert_eq!(facts.access.identifier().name(), "t");
            }
            let mut alias = request.clone();
            alias.resolution = ConnectorTableResolution::ProviderReadAlias;
            let Err(error) = metadata.prepare_source_table(alias).await else { panic!("source observation cannot use a catalog alias"); };
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            let first = metadata
                .capture_table_object_binding_async(ConnectorTableObjectCaptureRequest {
                    table: request.table.clone(),
                    resolution: request.resolution,
                    selector: ConnectorTableObjectSelector::Current,
                    context: request.context.clone(),
                })
                .await
                .unwrap();
            let mut foreign = request.table.clone();
            foreign.instance_id = ConnectorInstanceId::parse("foreign").unwrap();
            let Err(error) = metadata
                .capture_table_object_binding_async(ConnectorTableObjectCaptureRequest {
                    table: foreign,
                    resolution: request.resolution,
                    selector: ConnectorTableObjectSelector::Current,
                    context: request.context.clone(),
                })
                .await
            else {
                panic!("foreign instance must not capture a physical object");
            };
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            let mut missing = request.table.clone();
            missing.table = Arc::from("missing");
            let Err(error) = metadata
                .capture_table_object_binding_async(ConnectorTableObjectCaptureRequest {
                    table: missing,
                    resolution: request.resolution,
                    selector: ConnectorTableObjectSelector::Current,
                    context: request.context.clone(),
                })
                .await
            else {
                panic!("missing table must not capture a physical object");
            };
            assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
            let retained = first.object_id.clone();
            let rebound = metadata
                .rebind_table_object_binding_async(ConnectorTableObjectRebindRequest {
                    table: request.table.clone(),
                    expected_object_id: retained.clone(),
                    resolution: request.resolution,
                    selector: ConnectorTableObjectSelector::Current,
                    context: request.context.clone(),
                })
                .await
                .unwrap();
            assert_eq!(rebound.object_id, retained);
            assert_eq!(rebound.metadata.identity, first.metadata.identity);
            let refused = metadata
                .rebind_table_object_binding_async(ConnectorTableObjectRebindRequest {
                    table: request.table,
                    expected_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(
                        b"replaced",
                    ))
                    .unwrap(),
                    resolution: request.resolution,
                    selector: ConnectorTableObjectSelector::Current,
                    context: request.context,
                })
                .await;
            let Err(error) = refused else {
                panic!("foreign physical UUID must be rejected");
            };
            assert_eq!(
                error.table_object_binding_failure(),
                Some(novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced)
            );
        });
    }

    // Executes the real closed Provider CPU input after removing its original
    // catalog files. This fixture tests phase/input semantics, not thread-pool
    // capacity or the physical worker's exit protocol.
    struct CaptureAfterOriginalIo {
        path: std::path::PathBuf,
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl novarocks_spi::connector::ProviderMetadataCpuPort for CaptureAfterOriginalIo {
        fn submit_metadata_in(
            &self,
            input: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            use std::sync::atomic::Ordering;
            let original = input.as_ref().expect("original metadata input");
            assert_eq!(original.descriptor(), &self.descriptor);
            assert_eq!(original.incarnation(), self.incarnation);
            assert_eq!(
                original.original_source_type_id(),
                std::any::TypeId::of::<
                    crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                >()
            );
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            std::fs::remove_dir_all(&self.path).unwrap();
            input.take().unwrap().run_original_phase();
            Ok(())
        }
        fn poll_finished(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_closed(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }

    #[test]
    fn snapshot_metadata_projects_to_schema_without_current_partition_membership() {
        use crate::iceberg::spec::{Literal, Transform};
        use novarocks_spi::connector::ConnectorColumnDefault;

        let (_executor, _warehouse, provider) = provider();
        let old_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("old-read"))
                        .with_write_default(Literal::string("old-write")),
                ),
            ])
            .build()
            .unwrap();
        let base = TableMetadataBuilder::new(
            old_schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///to-metadata".to_string(),
            FormatVersion::V3,
            HashMap::from([
                ("write.row-lineage".to_string(), "false".to_string()),
                ("unique_constraints".to_string(), "id".to_string()),
                (
                    "foreign_key_constraints".to_string(),
                    "(id) REFERENCES db.parent(id)".to_string(),
                ),
            ]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let old_schema_id = base.current_schema_id();
        let snapshot_base_ms = base.last_updated_ms();
        let snapshot = |id, parent, schema_id| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(id)
                .with_timestamp_ms(snapshot_base_ms + id)
                .with_manifest_list(format!("file:///to-metadata/snap-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .with_schema_id(schema_id)
                .with_row_range(0, 0)
                .build()
        };
        let base = base
            .into_builder(None)
            .add_snapshot(snapshot(101, None, old_schema_id))
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(101, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let current_schema = Schema::builder()
            .with_schema_id(old_schema_id + 1)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    3,
                    "future",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(
                    NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("old-read"))
                        .with_write_default(Literal::string("new-write")),
                ),
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let future_spec = PartitionSpec::builder(Arc::new(current_schema.clone()))
            .add_partition_field("future", "future_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let evolved = base
            .into_builder(None)
            .add_current_schema(current_schema)
            .unwrap()
            .add_partition_spec(future_spec.into_unbound())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let current_schema_id = evolved.current_schema_id();
        let evolved = evolved
            .into_builder(None)
            .add_snapshot(snapshot(102, Some(101), current_schema_id))
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(102, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_ne!(old_schema_id, evolved.current_schema_id());
        assert_eq!(evolved.default_partition_spec().fields()[0].source_id, 3);
        let uuid = evolved.uuid().to_string();
        let io = crate::fs_io::build_file_io_for_location(
            evolved.location(),
            provider.runtime.resources().planning_binding().clone(),
        );
        let table = crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .metadata(Arc::new(evolved))
            .file_io(io)
            .runtime(
                provider
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .iceberg_runtime()
                    .clone(),
            )
            .build()
            .unwrap();
        let physical = crate::loaded_table::IcebergPhysicalTable::new(table);
        let request = ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: provider.descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("t"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        };
        let build = |projection| {
            with_pure_schema_creation(&request.context, |creation| {
                build_table_metadata(
                    TableMetadataBuildInput {
                        descriptor: &provider.descriptor,
                        incarnation: provider.incarnation,
                        request: request.clone(),
                        loaded: physical.clone(),
                        table_name: "t".to_string(),
                        metadata_table_type: None,
                        snapshots: crate::catalog::SnapshotLoading::All,
                        projection,
                    },
                    creation,
                )
                .map_err(metadata_creation_error)
            })
            .unwrap()
        };
        let to = build(TableMetadataProjection::SnapshotTo(101));
        let current = build(TableMetadataProjection::Current);
        let owner = ConnectorProviderBindingKey {
            instance_id: provider.descriptor.instance_id.clone(),
            incarnation: provider.incarnation,
        };
        let inspect_source = |metadata: &ConnectorTableMetadata| {
            crate::storage_inspector::source_schema_observation(
                physical.table.metadata(),
                metadata,
                &owner,
                &request.context,
            )
        };
        let selected_source = inspect_source(&to).unwrap();
        let current_source = inspect_source(&current).unwrap();
        assert_eq!(
            selected_source
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "label"]
        );
        assert_eq!(
            current_source
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
            ["future", "label", "id"]
        );
        assert_eq!(selected_source.schema_version, to.version.clone().unwrap());
        assert_eq!(
            current_source.schema_version,
            current.version.clone().unwrap()
        );
        assert!(selected_source.format_v3);
        assert!(!selected_source.stored_row_lineage_enabled);
        assert_eq!(selected_source.object_id, current_source.object_id);
        // The default Current spec uses future field 3, absent from To. Source
        // inspection must not call the target partition-author under To schema.
        assert_eq!(
            physical.table.metadata().default_partition_spec().fields()[0].source_id,
            3
        );
        let mut wrong_version = to.clone();
        wrong_version.version = Some(Bytes::copy_from_slice(&(old_schema_id + 19).to_le_bytes()));
        assert_eq!(
            inspect_source(&wrong_version).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        let original_marker = to.source_read_facts().unwrap()
            .value::<crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts>(provider.descriptor.instance_id.as_str()).unwrap();
        assert!(matches!(
            original_marker.projection,
            TableMetadataProjection::SnapshotTo(101)
        ));
        let foreign = to.clone().with_source_read_facts(novarocks_spi::connector::ConnectorProcessLocalPayload::new(
            ConnectorProviderBindingKey {
                instance_id: provider.descriptor.instance_id.clone(),
                incarnation: ProviderBindingEpoch::from_bytes([0x79;16]),
            },
            Arc::new(crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts {
                access: Arc::clone(&original_marker.access), snapshots: original_marker.snapshots,
                projection: original_marker.projection,
            }),
        ));
        assert_eq!(
            inspect_source(&foreign).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        for missing_field in [false, true] {
            let mut frame = serde_json::to_value(physical.table.metadata()).unwrap();
            let selected_schema = frame["schemas"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|schema| schema["schema-id"].as_i64() == Some(i64::from(old_schema_id)))
                .unwrap();
            if missing_field {
                selected_schema["fields"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|field| field["id"] != serde_json::json!(2));
            } else {
                selected_schema["fields"][0]["type"] = serde_json::json!("string");
            }
            let altered: crate::iceberg::spec::TableMetadata =
                serde_json::from_value(frame).unwrap();
            let changed_table = crate::iceberg::table::Table::builder()
                .identifier(physical.table.identifier().clone())
                .metadata(Arc::new(altered.clone()))
                .file_io(physical.table.file_io().clone())
                .runtime(
                    provider
                        .runtime
                        .resources()
                        .catalog_runtime()
                        .iceberg_runtime()
                        .clone(),
                )
                .build()
                .unwrap();
            let malformed = to.clone().with_source_read_facts(novarocks_spi::connector::ConnectorProcessLocalPayload::new(
                owner.clone(), Arc::new(crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts {
                    access: Arc::new(crate::loaded_table::IcebergAttemptTableAccess::freeze(crate::loaded_table::IcebergPhysicalTable::new(changed_table))),
                    snapshots: original_marker.snapshots, projection: original_marker.projection,
                }),
            ));
            // Model the same decoded frozen frame and private marker disagreeing
            // with the already-admitted typed schema. Same schema ID is not proof.
            assert_eq!(
                crate::storage_inspector::source_schema_observation(
                    &altered,
                    &malformed,
                    &owner,
                    &request.context,
                )
                .unwrap_err()
                .kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        let names = |metadata: &ConnectorTableMetadata| {
            metadata
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&to), ["id", "label", "_file", "_pos"]);
        assert_eq!(names(&current), ["future", "label", "id", "_file", "_pos"]);
        assert_eq!(
            to.execution_schema()
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "label", "_file", "_pos"]
        );
        assert_eq!(to.planning_facts.column_facts().len(), 4);
        assert_eq!(
            to.planning_facts
                .column_facts()
                .iter()
                .map(|f| f.field_ordinal())
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(
            to.planning_facts.column_facts()[1].write_default(),
            Some(&ConnectorColumnDefault::String(Arc::from("old-write")))
        );
        assert_eq!(
            current.planning_facts.column_facts()[1].write_default(),
            Some(&ConnectorColumnDefault::String(Arc::from("new-write")))
        );
        let initial = crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY;
        assert!(to.schema().field(1).metadata()[initial].contains("old-read"));
        assert!(current.schema().field(1).metadata()[initial].contains("old-read"));
        assert_eq!(to.planning_facts.partition_source_column_ordinals(), None);
        assert_eq!(
            current.planning_facts.partition_source_column_ordinals(),
            Some([0].as_slice())
        );
        assert_eq!(
            to.planning_facts.unique_constraints()[0].column_ordinals(),
            &[0]
        );
        assert_eq!(
            current.planning_facts.unique_constraints()[0].column_ordinals(),
            &[2]
        );
        assert_eq!(
            to.planning_facts.foreign_key_constraints()[0].local_column_ordinals(),
            &[0]
        );
        assert_eq!(
            to.version.as_deref(),
            Some(old_schema_id.to_le_bytes().as_slice())
        );
        assert_eq!(
            current.version.as_deref(),
            Some(current_schema_id.to_le_bytes().as_slice())
        );
        assert_eq!(
            to.statistics_data_version,
            Some(crate::statistics_codec::statistics_data_version(&uuid, Some(101)).unwrap())
        );
        assert_eq!(
            current.statistics_data_version,
            Some(crate::statistics_codec::statistics_data_version(&uuid, Some(102)).unwrap())
        );
        let current_revision = provider
            .exact_semantic_revision(&to.table, ConnectorReadSelector::Current)
            .unwrap();
        assert_eq!(
            current_revision.canonical_read_point(),
            Some(ConnectorCanonicalReadPoint::Snapshot(Some(102)))
        );
        let payload = provider.table_payload(&to.table).unwrap();
        let info = payload.table_info.unwrap();
        assert_eq!(info.current_snapshot_id, Some(102));
        assert_eq!(info.schema_id, current_schema_id);
        let real: crate::iceberg::spec::TableMetadata =
            serde_json::from_str(info.serialized_metadata.as_deref().unwrap()).unwrap();
        assert_eq!(real.current_snapshot_id(), Some(102));
        assert_eq!(real.current_schema_id(), current_schema_id);
        let error = with_pure_schema_creation(&request.context, |creation| {
            build_table_metadata(
                TableMetadataBuildInput {
                    descriptor: &provider.descriptor,
                    incarnation: provider.incarnation,
                    request: request.clone(),
                    loaded: physical.clone(),
                    table_name: "t".to_string(),
                    metadata_table_type: Some(MetadataTableType::Files),
                    snapshots: crate::catalog::SnapshotLoading::All,
                    projection: TableMetadataProjection::SnapshotTo(101),
                },
                creation,
            )
            .map_err(metadata_creation_error)
        })
        .err()
        .expect("metadata aliases cannot masquerade as snapshot sources");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn completed_async_observation_creates_the_original_schema_without_further_io() {
        use crate::catalog::CatalogNamespaceName;
        use crate::catalog::catalog_request::CatalogPublicationRequest;
        use crate::catalog::error::CatalogOutcome;
        use crate::iceberg::TableCreation;
        let (executor, warehouse, metadata) = provider();
        let (_other_executor, _other_warehouse, other_generation) = provider();
        let (prepared, foreign) = executor.block_on(async {
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &metadata.runtime,
                metadata.incarnation,
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("prepared_test"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                &metadata.runtime,
                metadata.incarnation,
                CatalogPublicationRequest::CreateTable {
                    namespace: CatalogNamespaceName::new("prepared_test"),
                    creation: TableCreation::builder()
                        .name("t".into())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let request = ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: metadata.descriptor.instance_id.clone(),
                    namespace: "prepared_test".into(),
                    table: "t".into(),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context(),
            };
            let prepared = metadata.prepare_table(request.clone()).await.unwrap();
            let foreign = metadata.prepare_table(request.clone()).await.unwrap();
            let cpu = Arc::new(CaptureAfterOriginalIo {
                path: warehouse.path().to_path_buf(),
                descriptor: metadata.descriptor.clone(),
                incarnation: metadata.incarnation,
                calls: std::sync::atomic::AtomicUsize::new(0),
            });
            let (binding, snapshot) = metadata
                .capture_table_object_with_snapshot_cpu(
                    ConnectorTableObjectCaptureRequest {
                        table: request.table.clone(),
                        resolution: request.resolution,
                        selector: ConnectorTableObjectSelector::Current,
                        context: request.context,
                    },
                    cpu.clone(),
                )
                .await
                .unwrap();
            assert_eq!(cpu.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(binding.metadata.identity, request.table);
            assert_eq!(snapshot, None, "the real new table has no snapshot");
            assert_eq!(
                metadata
                    .table_payload(&binding.metadata.table)
                    .unwrap()
                    .table_info
                    .unwrap()
                    .current_snapshot_id,
                snapshot
            );
            (prepared, foreign)
        });
        // The actual tuple capture removed storage before its original CPU
        // phase. These earlier observations also remain usable without I/O.
        assert!(!warehouse.path().exists());
        let mut request = |_: std::alloc::Layout| Ok(());
        let mut work = |_: usize| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        assert!(
            other_generation
                .create_prepared_table(foreign, &mut creation)
                .is_err()
        );
        let table = metadata
            .create_prepared_table(prepared, &mut creation)
            .unwrap();
        assert_eq!(table.identity.namespace.as_ref(), "prepared_test");
        assert_eq!(table.identity.table.as_ref(), "t");
        assert_eq!(table.schema().field(0).name(), "id");
        let payload = metadata.table_payload(&table.table).unwrap();
        assert_eq!(payload.namespace, "prepared_test");
        assert_eq!(payload.table, "t");
        assert!(payload.table_info.unwrap().table_uuid.is_some());
    }

    #[test]
    fn table_job_admission_uses_the_exact_owner_without_any_external_io() {
        use novarocks_spi::connector::{
            ConnectorTableJobAdmission, ConnectorTableJobAdmissionRequest, ConnectorTableJobKind,
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        for catalog_type in ["hive", "hadoop", "rest"] {
            let (_executor, warehouse, provider) = provider_with_catalog(catalog_type, &address);
            for job in [
                ConnectorTableJobKind::Statistics,
                ConnectorTableJobKind::RewriteDataFiles,
            ] {
                let answer = provider.admit_table_job(ConnectorTableJobAdmissionRequest {
                    table: ConnectorTableIdentity {
                        instance_id: provider.descriptor.instance_id.clone(),
                        namespace: Arc::from("db"),
                        table: Arc::from("orders"),
                    },
                    job,
                    context: context(),
                });
                match catalog_type {
                    "hive" => {
                        let error = answer.unwrap_err();
                        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                        assert!(error.message().contains("read-only compatibility entry"));
                    }
                    "hadoop" => {
                        assert_eq!(answer.unwrap(), ConnectorTableJobAdmission::AwaitTerminal)
                    }
                    "rest" => assert_eq!(answer.unwrap(), ConnectorTableJobAdmission::Detached),
                    _ => unreachable!(),
                }
                assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), 0);
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
            }
        }
    }

    fn exact_revision_handle(
        provider: &IcebergMetadata,
        table_uuid: &str,
        snapshot_id: i64,
    ) -> ConnectorTableHandle {
        let payload = IcebergTablePayload {
            namespace: "db".to_string(),
            table: "orders".to_string(),
            metadata_location: None,
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
            }),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
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

    #[test]
    fn exact_revision_selectors_reject_foreign_and_unreadable_endpoints() {
        let (_runtime, _warehouse, provider) = provider();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let snapshot = |id| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_sequence_number(id)
                .with_timestamp_ms(id)
                .with_manifest_list(format!("file:///exact-revision/snap-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build()
        };
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///exact-revision".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_snapshot(snapshot(101))
        .unwrap()
        .add_snapshot(snapshot(102))
        .unwrap()
        .set_ref(
            "main",
            SnapshotReference::new(102, SnapshotRetention::branch(None, None, None)),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table_uuid = metadata.uuid().to_string();
        let mut payload = provider
            .table_payload(&exact_revision_handle(&provider, &table_uuid, 102))
            .unwrap();
        payload.table_info.as_mut().unwrap().serialized_metadata =
            Some(serde_json::to_string(&metadata).unwrap());
        let table = ConnectorTableHandle::try_new(
            provider.descriptor.instance_id.clone(),
            encode_payload(&payload, "test exact selector", 64 * 1024).unwrap(),
        )
        .unwrap();
        let from = provider
            .exact_semantic_revision(&table, ConnectorReadSelector::SnapshotId(101))
            .unwrap();
        let to = provider
            .exact_semantic_revision(&table, ConnectorReadSelector::Current)
            .unwrap();
        assert_eq!(
            provider
                .read_selector_from_exact_revision(&table, &from)
                .unwrap(),
            ConnectorReadSelector::SnapshotId(101)
        );
        assert_eq!(
            provider
                .read_selector_from_exact_revision(&table, &to)
                .unwrap(),
            ConnectorReadSelector::SnapshotId(102)
        );
        let object_id = ConnectorTableObjectId::try_new(Bytes::from(table_uuid)).unwrap();
        let replaced_object =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"replaced-object")).unwrap();
        for revision in [
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                ConnectorProviderId::parse("paimon").unwrap(),
                &object_id,
                Some(101),
            )
            .unwrap(),
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.descriptor.provider_id.clone(),
                &replaced_object,
                Some(101),
            )
            .unwrap(),
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.descriptor.provider_id.clone(),
                &object_id,
                Some(99),
            )
            .unwrap(),
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.descriptor.provider_id.clone(),
                &object_id,
                None,
            )
            .unwrap(),
        ] {
            assert!(
                provider
                    .read_selector_from_exact_revision(&table, &revision)
                    .is_err()
            );
        }
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
                metadata_location: None,
                table_info: None,
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: Some(files),
                row_mutation_frozen_source: false,
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
                metadata_location: None,
                table_info: None,
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: None,
                row_mutation_frozen_source: true,
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
    mod prepared_source_phase_support {
        use super::*;
        use crate::iceberg::spec::{DataContentType, DataFileFormat, Struct};
        use novarocks_spi::connector::{
            ConnectorIoAdmissionPort, ConnectorIoCleanupFuture, ConnectorIoProgressSource,
            ConnectorIoRequest, ConnectorIoRequestSource, ConnectorOriginalResultScopeCheck,
            ConnectorPayloadRetentionGuard, OriginalResultCheckError, ProviderMetadataCpuInput,
            ProviderMetadataCpuPort, SharedDependency,
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::task::{Context, Poll, Waker};
        pub(super) struct OriginalScope(pub(super) Instant);
        impl ConnectorOriginalResultScopeCheck for OriginalScope {
            fn original_deadline(&self) -> Instant {
                self.0
            }
            fn check_active(&self) -> Result<(), OriginalResultCheckError> {
                Ok(())
            }
        }
        // Explicit SPI resource-cell fixture. It observes the real FileIO
        // requests; it does not certify a WC Root, a CPU offer or worker exit.
        pub(super) struct IoFixture {
            pub(super) in_cpu: Arc<AtomicBool>,
            pub(super) owned_reads: AtomicUsize,
        }
        struct IoCell;
        impl ConnectorIoRequestSource for IoCell {
            fn exit_waker(&self) -> Waker {
                Waker::noop().clone()
            }
            fn reserve_stream(&self) -> Result<(), ConnectorError> {
                panic!("snapshot whole-object reads do not open streams")
            }
            fn stream_closed(&self) {
                panic!("no snapshot stream")
            }
            fn install_stream_cleanup_in(
                &self,
                _: &mut Option<ConnectorIoCleanupFuture>,
            ) -> Result<(), ConnectorError> {
                panic!("no snapshot stream cleanup")
            }
            fn stream_custody_lost(&self) {
                panic!("no snapshot stream custody")
            }
            fn record_cleanup_failure(
                &self,
                original: Box<dyn std::any::Any + Send>,
            ) -> Result<(), Box<dyn std::any::Any + Send>> {
                Err(original)
            }
            fn attach_original_progress(
                &self,
                _: Arc<dyn ConnectorIoProgressSource>,
            ) -> Result<(), ConnectorError> {
                Ok(())
            }
            fn poll_admitted(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
                Poll::Ready(Ok(()))
            }
            fn poll_include_dependency(
                &self,
                _: &SharedDependency,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), ConnectorError>> {
                Poll::Ready(Ok(()))
            }
        }
        impl ConnectorIoAdmissionPort for IoFixture {
            fn request_owned(
                &self,
                _: std::num::NonZeroU64,
            ) -> Result<ConnectorIoRequest, ConnectorError> {
                assert!(
                    !self.in_cpu.load(Ordering::SeqCst),
                    "raw snapshot read must be driven outside the pure CPU phase"
                );
                self.owned_reads.fetch_add(1, Ordering::SeqCst);
                Ok(ConnectorIoRequest::from_original(Arc::new(IoCell)))
            }
            fn request(&self, _: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError> {
                assert!(
                    !self.in_cpu.load(Ordering::SeqCst),
                    "selected physical I/O must not be polled by the pure CPU phase"
                );
                Ok(ConnectorIoRequest::from_original(Arc::new(IoCell)))
            }
            fn publication_obligation_started(&self) {
                panic!("scan cannot publish")
            }
            fn retain_payload(&self) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
                Ok(ConnectorPayloadRetentionGuard::new(()))
            }
        }
        pub(super) struct PhasePort {
            pub(super) in_cpu: Arc<AtomicBool>,
            pub(super) descriptor: ConnectorInstanceDescriptor,
            pub(super) incarnation: ProviderBindingEpoch,
            pub(super) calls: AtomicUsize,
        }
        impl ProviderMetadataCpuPort for PhasePort {
            fn submit_metadata_in(
                &self,
                input: &mut Option<ProviderMetadataCpuInput>,
            ) -> Result<(), ConnectorError> {
                let source = input.as_ref().unwrap();
                assert_eq!(source.descriptor(), &self.descriptor);
                assert_eq!(source.incarnation(), self.incarnation);
                assert_eq!(
                    source.original_source_type_id(),
                    std::any::TypeId::of::<
                        crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                    >()
                );
                assert!(!self.in_cpu.swap(true, Ordering::SeqCst));
                self.calls.fetch_add(1, Ordering::SeqCst);
                input.take().unwrap().run_original_phase();
                self.in_cpu.store(false, Ordering::SeqCst);
                Ok(())
            }
            fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
                Poll::Ready(Ok(()))
            }
            fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
                Poll::Ready(Ok(()))
            }
            fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
                None
            }
        }
        pub(super) fn written(
            table: &crate::iceberg::table::Table,
            label: &str,
        ) -> crate::commit::WrittenFile {
            crate::commit::WrittenFile {
                path: format!("{}/data/{label}.parquet", table.metadata().location()),
                format: DataFileFormat::Parquet,
                content: DataContentType::Data,
                partition_values: Struct::empty(),
                partition_spec_id: table.metadata().default_partition_spec_id(),
                record_count: 3,
                file_size_in_bytes: 128,
                split_offsets: Vec::new(),
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                key_metadata: None,
                referenced_data_file: None,
                equality_ids: None,
                first_row_id: None,
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            }
        }
    }
    mod prepared_split_execution_tests;

    fn prepared_source_fixture_control(
        provider: &IcebergMetadata,
        context: &ConnectorRequestContext,
    ) -> novarocks_spi::connector::read_stack::ConnectorReadRequestControl {
        use novarocks_spi::connector::read_stack::ConnectorReadRequestControlFactory;
        let template = Arc::new(crate::typed_boundary::IcebergTypedBoundary::new(
            provider.descriptor.clone(),
            provider.incarnation,
            novarocks_spi::connector::CatalogHandle::new(
                provider.descriptor.instance_id.clone(),
                novarocks_spi::connector::CatalogVersion::from_bytes([0x65; 32]),
            ),
            crate::typed_read::HiveTransactionHandle::new(true, [0x71; 16]),
            Arc::clone(&provider.runtime),
        ));
        crate::typed_boundary::IcebergTypedRequestControlFactory::new(template)
            .for_planning(
                &novarocks_spi::connector::ConnectorPlanningContext::try_from_request(
                    context.clone().without_attempt_capabilities(),
                )
                .unwrap(),
            )
            .unwrap()
    }

    async fn prepared_source_fixture_metadata(
        provider: &IcebergMetadata,
        namespace: &str,
        context: &ConnectorRequestContext,
    ) -> ConnectorTableMetadata {
        let preparation = provider
            .prepare_source_table(ConnectorTableRequest {
                table: ConnectorTableIdentity {
                    instance_id: provider.descriptor.instance_id.clone(),
                    namespace: namespace.into(),
                    table: "t".into(),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: context.clone(),
            })
            .await
            .unwrap();
        with_pure_schema_creation(context, |creation| {
            provider
                .create_prepared_table(preparation, creation)
                .map_err(metadata_creation_error)
        })
        .unwrap()
    }

    fn prepared_source_fixture_window(
        provider: &IcebergMetadata,
        metadata: &ConnectorTableMetadata,
        namespace: &str,
        from: i64,
        to: i64,
    ) -> novarocks_spi::connector::read_stack::ConnectorReadSourcePreparationRequest {
        let facts = metadata.source_read_facts().unwrap().value::<
            crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts
        >(provider.descriptor.instance_id.as_str()).unwrap();
        let object = ConnectorTableObjectId::try_new(Bytes::from(
            facts.access.metadata().uuid().to_string(),
        ))
        .unwrap();
        let revision = |snapshot| {
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.descriptor.provider_id.clone(),
                &object,
                Some(snapshot),
            )
            .unwrap()
        };
        novarocks_spi::connector::read_stack::ConnectorReadSourcePreparationRequest::change_window(
            novarocks_spi::connector::read_stack::SchemaTableName::try_new(namespace, "t").unwrap(),
            metadata.table.clone(),
            metadata.source_read_facts().unwrap().clone(),
            object.clone(),
            novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(from, to),
            revision(from),
            revision(to),
        )
    }

    #[test]
    fn original_async_change_window_uses_loaded_exact_pin_without_cpu_catalog_io() {
        use crate::catalog::{
            CatalogNamespaceName, catalog_request::CatalogPublicationRequest, error::CatalogOutcome,
        };
        use novarocks_spi::connector::{
            ConnectorChangeWindowAdmission, ConnectorChangeWindowFullRebuildReason,
            ConnectorOriginalResultScope, ConnectorOriginalResultScopeCheck,
            OriginalResultCheckError, ProviderMetadataCpuInput, ProviderMetadataCpuPort,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct OriginalScope(Instant);
        impl ConnectorOriginalResultScopeCheck for OriginalScope {
            fn original_deadline(&self) -> Instant {
                self.0
            }
            fn check_active(&self) -> Result<(), OriginalResultCheckError> {
                Ok(())
            }
        }
        // Like CaptureAfterOriginalIo above, this is a real-input phase fixture,
        // not a physical CPU admission/exit oracle. The typed pure phase must
        // finish from the exact already-captured facts after the catalog is gone.
        struct LoadedPhasePort {
            path: std::path::PathBuf,
            descriptor: ConnectorInstanceDescriptor,
            incarnation: ProviderBindingEpoch,
            calls: AtomicUsize,
        }
        impl ProviderMetadataCpuPort for LoadedPhasePort {
            fn submit_metadata_in(
                &self,
                input: &mut Option<ProviderMetadataCpuInput>,
            ) -> Result<(), ConnectorError> {
                let original = input.as_ref().unwrap();
                assert_eq!(original.descriptor(), &self.descriptor);
                assert_eq!(original.incarnation(), self.incarnation);
                assert_eq!(
                    original.original_source_type_id(),
                    std::any::TypeId::of::<
                        crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                    >()
                );
                let turn = self.calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    turn, 0,
                    "resolved lineage needs one typed preparation phase"
                );
                std::fs::remove_dir_all(&self.path).unwrap();
                input.take().unwrap().run_original_phase();
                Ok(())
            }
            fn poll_finished(
                &self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), ConnectorError>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_closed(
                &self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), ConnectorError>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
                None
            }
        }
        for missing_from in [false, true] {
            let (executor, warehouse, provider) = provider();
            executor.block_on(async {
                let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                    provider.runtime(),
                    provider.incarnation,
                    CatalogPublicationRequest::CreateNamespace {
                        namespace: CatalogNamespaceName::new("async_window"),
                        properties: HashMap::new(),
                        no_op_if_exists: false,
                    },
                )
                .await;
                assert!(
                    matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                    "{outcome:?}"
                );
                let schema = Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .unwrap();
                let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                    provider.runtime(),
                    provider.incarnation,
                    CatalogPublicationRequest::CreateTable {
                        namespace: CatalogNamespaceName::new("async_window"),
                        creation: crate::iceberg::TableCreation::builder()
                            .name("t".into())
                            .schema(schema)
                            .format_version(FormatVersion::V2)
                            .build(),
                        no_op_if_exists: false,
                    },
                )
                .await;
                assert!(
                    matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                    "{outcome:?}"
                );
                let loaded = provider
                    .runtime()
                    .load_table_for_request_async("async_window", "t", &context())
                    .await
                    .unwrap();
                let snapshot = crate::commit::run::append_snapshot_for_test(
                    provider.clone(),
                    loaded.table,
                    Vec::new(),
                    "main".into(),
                    std::collections::BTreeMap::new(),
                )
                .await
                .unwrap();
                provider
                    .runtime()
                    .control_state()
                    .invalidate_table_cache("async_window", "t");
                let request_context = context();
                let table =
                    prepared_source_fixture_metadata(&provider, "async_window", &request_context)
                        .await;
                let from = if missing_from {
                    snapshot.checked_add(1).unwrap_or(snapshot - 1)
                } else {
                    snapshot
                };
                let request = prepared_source_fixture_window(
                    &provider,
                    &table,
                    "async_window",
                    from,
                    snapshot,
                );
                let original = ConnectorOriginalResultScope::from_original(Arc::new(
                    OriginalScope(request_context.deadline()),
                ));
                let read = prepared_source_fixture_control(&provider, &request_context);
                let metadata = read.metadata();
                let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
                    "q",
                    "u",
                    "UTC",
                    "en_US",
                    std::time::SystemTime::UNIX_EPOCH,
                )
                .unwrap();
                let port = Arc::new(LoadedPhasePort {
                    path: warehouse.path().to_path_buf(),
                    descriptor: provider.descriptor.clone(),
                    incarnation: provider.incarnation,
                    calls: AtomicUsize::new(0),
                });
                let cpu: Arc<dyn ProviderMetadataCpuPort> = port.clone();
                let mut owner = None;
                let result = metadata
                    .prepare_source_read(&session, &request, &cpu, &original, &mut owner)
                    .await
                    .unwrap();
                let key = ConnectorProviderBindingKey {
                    instance_id: provider.descriptor.instance_id.clone(),
                    incarnation: provider.incarnation,
                };
                owner.as_ref().unwrap().validate_owner(&key).unwrap();
                let weak = Arc::downgrade(owner.as_ref().unwrap().value::<
                    crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                >(provider.descriptor.instance_id.as_str()).unwrap());
                assert!(
                    weak.upgrade().is_some(),
                    "actual Source outlives the completed Provider F"
                );
                let rejected = match metadata
                    .prepare_source_read(&session, &request, &cpu, &original, &mut owner)
                    .await
                {
                    Err(error) => error,
                    Ok(_) => panic!("same owner receiver cannot be overwritten"),
                };
                assert_eq!(rejected.kind(), ConnectorErrorKind::InvalidRequest);
                assert_eq!(
                    rejected.message(),
                    "source preparation owner slot is occupied"
                );
                assert!(weak.upgrade().is_some());
                assert_eq!(port.calls.load(Ordering::SeqCst), 1);
                assert_eq!(result.request().to_revision(), request.to_revision());
                assert_eq!(result.request().from_revision(), request.from_revision());
                let expected = if missing_from {
                    ConnectorChangeWindowAdmission::FullRebuild(
                        ConnectorChangeWindowFullRebuildReason::LineageBroken {
                            from_snapshot_id: from,
                        },
                    )
                } else {
                    ConnectorChangeWindowAdmission::MetadataOnly
                };
                assert_eq!(result.admission(), Some(&expected));
                // This fixture's port reports its explicit close. It is not a
                // physical worker exit oracle; actual Source custody stays in
                // the original receiver until the caller closes and drops it.
                std::future::poll_fn(|cx| cpu.poll_closed(cx))
                    .await
                    .unwrap();
                drop(owner.take());
                assert!(weak.upgrade().is_none());
                assert_eq!(result.to_metadata().schema().field(0).name(), "id");
                assert_eq!(result.to_metadata().schema().fields().len(), 3);
            });
        }
    }
    #[test]
    fn original_async_change_window_reads_both_manifest_endpoints_across_a_rename() {
        use crate::catalog::{
            CatalogNamespaceName, catalog_request::CatalogPublicationRequest, error::CatalogOutcome,
        };
        use novarocks_spi::connector::{
            ConnectorCatalogMutation, ConnectorCatalogMutationOperation,
            ConnectorCatalogMutationRequest, ConnectorChangeWindowAdmission,
            ConnectorChangeWindowPartitionImpact, ConnectorColumnPath,
            ConnectorOriginalResultScope, ConnectorSchemaChange, ExternalMutationEffect,
            ExternalMutationOutcome, ProviderMetadataCpuPort,
        };
        use prepared_source_phase_support::{IoFixture, OriginalScope, PhasePort, written};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let (executor, _warehouse, provider) = provider();
        let from = executor.block_on(async {
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                provider.runtime(),
                provider.incarnation,
                CatalogPublicationRequest::CreateNamespace {
                    namespace: CatalogNamespaceName::new("endpoint_window"),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(
                provider.runtime(),
                provider.incarnation,
                CatalogPublicationRequest::CreateTable {
                    namespace: CatalogNamespaceName::new("endpoint_window"),
                    creation: crate::iceberg::TableCreation::builder()
                        .name("t".into())
                        .schema(schema)
                        .format_version(FormatVersion::V2)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
            assert!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                "{outcome:?}"
            );
            let loaded = provider
                .runtime()
                .load_table_for_request_async("endpoint_window", "t", &context())
                .await
                .unwrap();
            let file = written(&loaded.table, "a");
            crate::commit::run::append_snapshot_for_test(
                provider.clone(),
                loaded.table,
                vec![file],
                "main".into(),
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap()
        });
        // Seed the rename through the existing catalog mutation author outside
        // the async driver. The scan below never calls this legacy sync route.
        let outcome = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: ConnectorProviderBindingKey {
                    instance_id: provider.descriptor.instance_id.clone(),
                    incarnation: provider.incarnation,
                },
                operation: ConnectorCatalogMutationOperation::AlterSchema {
                    table: ConnectorTableIdentity {
                        instance_id: provider.descriptor.instance_id.clone(),
                        namespace: "endpoint_window".into(),
                        table: "t".into(),
                    },
                    changes: vec![ConnectorSchemaChange::RenameColumn {
                        path: ConnectorColumnPath {
                            segments: vec!["id".into()],
                        },
                        to: "renamed".into(),
                    }],
                },
                context: context(),
            })
            .unwrap();
        assert!(
            matches!(
                outcome,
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ),
            "{outcome:?}"
        );
        executor.block_on(async {
                    provider.runtime().control_state().invalidate_table_cache("endpoint_window", "t");
                    let loaded = provider.runtime().load_table_for_request_async("endpoint_window", "t", &context()).await.unwrap();
                    let file = written(&loaded.table, "b");
                    let to = crate::commit::run::append_snapshot_for_test(provider.clone(), loaded.table, vec![file], "main".into(), std::collections::BTreeMap::new()).await.unwrap();
                    provider.runtime().control_state().invalidate_table_cache("endpoint_window", "t");
                    let physical = provider.runtime().load_table_for_request_async("endpoint_window", "t", &context()).await.unwrap();
                    let from_schema = physical.table.metadata().snapshot_by_id(from).unwrap().schema(physical.table.metadata()).unwrap();
                    let to_schema = physical.table.metadata().snapshot_by_id(to).unwrap().schema(physical.table.metadata()).unwrap();
                    assert_eq!(from_schema.field_by_id(1).unwrap().name, "id");
                    assert_eq!(to_schema.field_by_id(1).unwrap().name, "renamed");
                    assert_ne!(from_schema.schema_id(), to_schema.schema_id());
                    let active = Arc::new(AtomicBool::new(false));
                    let io = Arc::new(IoFixture { in_cpu: active.clone(), owned_reads: AtomicUsize::new(0) });
                    let phase = Arc::new(PhasePort { in_cpu: active, descriptor: provider.descriptor.clone(), incarnation: provider.incarnation, calls: AtomicUsize::new(0) });
                    let cpu: Arc<dyn ProviderMetadataCpuPort> = phase.clone();
                    let request_context = context().with_io_admission(io.clone());
                    let original = ConnectorOriginalResultScope::from_original(Arc::new(OriginalScope(request_context.deadline())));
                    let table = prepared_source_fixture_metadata(&provider, "endpoint_window", &request_context).await;
                    let initial_owned_reads = io.owned_reads.load(Ordering::SeqCst);
                    let request = prepared_source_fixture_window(&provider, &table, "endpoint_window", from, to);
                    let read = prepared_source_fixture_control(&provider, &request_context);
                    let metadata = read.metadata();
                    let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
                        "q", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH,
                    ).unwrap();
                    // An unpolled typed borrower dispatches no work and installs
                    // no opaque Source. The caller's original gate owns its inputs;
                    // first poll installs Source custody before any worker/foreign poll.
                    let mut unpolled_owner = None;
                    let borrower = metadata.prepare_source_read(&session, &request, &cpu, &original, &mut unpolled_owner);
                    drop(borrower);
                    assert!(unpolled_owner.is_none());
                    assert_eq!(phase.calls.load(Ordering::SeqCst), 0);
                    drop(unpolled_owner);
                    let mut owner = None;
                    let scan = metadata.prepare_source_read(&session, &request, &cpu, &original, &mut owner).await.unwrap();
                    assert_eq!(scan.to_metadata().schema().field(0).name(), "renamed");
                    owner.as_ref().unwrap().validate_owner(&ConnectorProviderBindingKey { instance_id: provider.descriptor.instance_id.clone(), incarnation: provider.incarnation }).unwrap();
                    assert_eq!(scan.admission(), Some(&ConnectorChangeWindowAdmission::Incremental {
                        has_inserts: true, has_deletes: false, partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
                    }));
                    assert!(phase.calls.load(Ordering::SeqCst) > 3, "both endpoint readers suspend and resume before the common Finish phase");
                    assert!(io.owned_reads.load(Ordering::SeqCst) - initial_owned_reads >= 4, "both nonempty manifest lists and their data manifests were actually read");
                    let weak = Arc::downgrade(owner.as_ref().unwrap().value::<crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal>(provider.descriptor.instance_id.as_str()).unwrap());
                    assert!(weak.upgrade().is_some());
                    let endpoints = owner.as_ref().unwrap().value::<crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal>(provider.descriptor.instance_id.as_str()).unwrap().prepared_source_endpoint_facts_for_test();
                    assert_eq!(endpoints[0].len(), 1);
                    assert_eq!(endpoints[1].len(), 2);
                    assert!(endpoints[0][0].path.ends_with("/a.parquet"));
                    let mut to_names = endpoints[1].iter().map(|file| file.path.rsplit('/').next().unwrap()).collect::<Vec<_>>();
                    to_names.sort_unstable();
                    assert_eq!(to_names, ["a.parquet", "b.parquet"]);
                    for (index, files) in endpoints.iter().enumerate() {
                        for file in files {
                            assert_eq!(file.record_count, Some(3));
                            let endpoint = file.read_domain().endpoint();
                            assert_eq!(endpoint.snapshot_id(), [from, to][index]);
                            assert_eq!(endpoint.schema().unwrap().field_by_id(1).unwrap().name, "renamed", "both real readers use the original To schema");
                        }
                    }
                    drop(endpoints);
                    std::future::poll_fn(|cx| cpu.poll_closed(cx)).await.unwrap();
                    drop(owner);
                    assert!(weak.upgrade().is_none(), "the received value does not hide a runtime Source alias");
                });
    }
    #[derive(Debug)]
    struct OriginalCaptureCatalogFailure {
        inner: Arc<dyn crate::catalog::NovaRocksCatalog>,
        failure: ConnectorError,
        reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl crate::catalog::NovaRocksCatalog for OriginalCaptureCatalogFailure {
        fn implementation_name(&self) -> &'static str {
            "original-capture-failure"
        }
        fn listing_admission(&self) -> Arc<crate::catalog::listing_admission::ListingAdmission> {
            self.inner.listing_admission()
        }
        async fn list_namespaces(
            &self,
            bound: novarocks_spi::connector::ConnectorListingBound,
        ) -> Result<Vec<String>, ConnectorError> {
            self.inner.list_namespaces(bound).await
        }
        async fn namespace_exists(
            &self,
            name: crate::catalog::CatalogNamespaceName,
        ) -> Result<bool, ConnectorError> {
            self.inner.namespace_exists(name).await
        }
        async fn list_tables(
            &self,
            name: crate::catalog::CatalogNamespaceName,
            bound: novarocks_spi::connector::ConnectorListingBound,
        ) -> Result<Vec<String>, ConnectorError> {
            self.inner.list_tables(name, bound).await
        }
        async fn table_exists(
            &self,
            name: crate::catalog::CatalogTableName,
        ) -> Result<bool, ConnectorError> {
            self.inner.table_exists(name).await
        }
        async fn load_table(
            &self,
            _: crate::catalog::CatalogTableName,
        ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(self.failure.clone())
        }
        async fn view_exists(
            &self,
            name: crate::catalog::CatalogTableName,
        ) -> Result<bool, ConnectorError> {
            self.inner.view_exists(name).await
        }
        async fn list_views(
            &self,
            name: crate::catalog::CatalogNamespaceName,
            bound: novarocks_spi::connector::ConnectorListingBound,
        ) -> Result<Vec<String>, ConnectorError> {
            self.inner.list_views(name, bound).await
        }
        async fn load_view(
            &self,
            name: crate::catalog::CatalogTableName,
        ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
            self.inner.load_view(name).await
        }
        async fn anchor_written_metadata(
            &self,
            name: crate::catalog::CatalogTableName,
            location: Arc<str>,
        ) -> crate::catalog::error::CatalogOutcome<crate::catalog::CatalogTableName> {
            self.inner.anchor_written_metadata(name, location).await
        }
        fn admit_operation(
            &self,
            operation: &crate::catalog::admission::CatalogOperation,
            target: &crate::catalog::admission::CatalogAdmissionTarget,
        ) -> Result<(), crate::catalog::error::CatalogUnsupported> {
            self.inner.admit_operation(operation, target)
        }
    }

    struct CaptureFooterPort {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        calls: std::sync::atomic::AtomicUsize,
        footer: std::sync::Mutex<tokio::sync::oneshot::Receiver<()>>,
        closed: std::sync::atomic::AtomicBool,
        started: tokio::sync::Notify,
    }
    // Explicit component-only inline CPU author: the real private input runs,
    // while a separate footer gate tests handoff ordering, not physical QB exit.
    impl novarocks_spi::connector::ProviderMetadataCpuPort for CaptureFooterPort {
        fn submit_metadata_in(
            &self,
            input: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            use std::sync::atomic::Ordering;
            let source = input.as_ref().expect("same original capture input");
            assert_eq!(source.descriptor(), &self.descriptor);
            assert_eq!(source.incarnation(), self.incarnation);
            assert_eq!(
                source.original_source_type_id(),
                std::any::TypeId::of::<
                    crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                >()
            );
            assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
            input.take().unwrap().run_original_phase();
            self.started.notify_one();
            Ok(())
        }
        fn poll_finished(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            assert_eq!(self.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_closed(
            &self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            use std::sync::atomic::Ordering;
            if self.closed.load(Ordering::SeqCst) {
                return std::task::Poll::Ready(Ok(()));
            }
            let result = std::future::Future::poll(
                std::pin::Pin::new(&mut *self.footer.lock().unwrap()),
                cx,
            );
            std::task::ready!(result).expect("the original footer gate is released");
            self.closed.store(true, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }
    fn original_capture_request(
        provider: &IcebergMetadata,
        namespace: &str,
    ) -> ConnectorTableObjectCaptureRequest {
        ConnectorTableObjectCaptureRequest {
            table: ConnectorTableIdentity {
                instance_id: provider.descriptor.instance_id.clone(),
                namespace: namespace.into(),
                table: "t".into(),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context(),
        }
    }
    fn footer_port(
        provider: &IcebergMetadata,
    ) -> (tokio::sync::oneshot::Sender<()>, Arc<CaptureFooterPort>) {
        let (release, footer) = tokio::sync::oneshot::channel();
        (
            release,
            Arc::new(CaptureFooterPort {
                descriptor: provider.descriptor.clone(),
                incarnation: provider.incarnation,
                calls: std::sync::atomic::AtomicUsize::new(0),
                footer: std::sync::Mutex::new(footer),
                closed: std::sync::atomic::AtomicBool::new(false),
                started: tokio::sync::Notify::new(),
            }),
        )
    }

    #[test]
    fn actual_catalog_original_failure_survives_prepare_current_and_stable_cpu_capture() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use std::error::Error;
        use std::sync::atomic::Ordering;
        let (executor, _warehouse, original) = provider();
        // The actual NovaRocksCatalog callback produces this error. It is not
        // injected into the metadata cache or disguised as CompileControl.
        let failure = ConnectorError::from_domain_static(
            ConnectorErrorKind::Unavailable,
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "catalog read original refusal",
            ),
            "catalog original read failed",
        );
        let cause = failure
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let fact = failure.failure_fact();
        let catalog = Arc::new(OriginalCaptureCatalogFailure {
            inner: Arc::clone(original.runtime.novarocks_catalog()),
            failure,
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let runtime = Arc::new(IcebergMetadataContext::with_catalog_for_test(
            original.runtime.control_state().clone(),
            original.runtime.resources().clone(),
            catalog.clone(),
        ));
        let metadata =
            IcebergMetadata::new(original.descriptor.clone(), original.incarnation, runtime);
        executor.block_on(async {
            let request = original_capture_request(&metadata, "catalog_failure");
            let preparation = metadata
                .prepare_table(ConnectorTableRequest {
                    table: request.table.clone(),
                    resolution: request.resolution,
                    context: request.context.clone(),
                })
                .await
                .expect_err("actual catalog failure");
            let current = metadata
                .capture_table_object_binding_async(request.clone())
                .await
                .err()
                .expect("actual Current loader failure");
            let (_release, port) = footer_port(&metadata);
            let port: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort> = port;
            let mut owner = None;
            let capture = metadata
                .capture_table_object_with_snapshot_cpu_in(request, &port, &mut owner)
                .await
                .err()
                .expect("actual stable capture loader failure");
            assert!(
                owner.is_some(),
                "the original Source stays in caller custody on refusal"
            );
            for error in [&preparation, &current, &capture] {
                assert_eq!(error.failure_fact(), fact);
                assert_eq!(
                    error.source().unwrap().downcast_ref::<SharedFailureCause>(),
                    Some(&cause)
                );
                let original = cause
                    .downcast_ref::<ConnectorError>()
                    .expect("actual catalog error type");
                assert_eq!(original.kind(), ConnectorErrorKind::Unavailable);
            }
            assert_eq!(
                catalog.reads.load(Ordering::SeqCst),
                3,
                "each request reached the real catalog loader; no negative-cache shortcut"
            );
        });
    }

    #[test]
    fn stable_metadata_capture_keeps_original_owner_and_waits_for_cpu_footer() {
        use crate::catalog::catalog_request::CatalogPublicationRequest;
        use crate::catalog::error::CatalogOutcome;
        use std::sync::atomic::Ordering;
        let (executor, _warehouse, metadata) = provider();
        executor.block_on(async {
            for request in [
                CatalogPublicationRequest::CreateNamespace {
                    namespace: crate::catalog::CatalogNamespaceName::new("capture_original"),
                    properties: HashMap::new(), no_op_if_exists: false,
                },
                CatalogPublicationRequest::CreateTable {
                    namespace: crate::catalog::CatalogNamespaceName::new("capture_original"),
                    creation: crate::iceberg::TableCreation::builder().name("t".into()).schema(
                        Schema::builder().with_fields(vec![Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)))]).build().unwrap()
                    ).format_version(FormatVersion::V2).build(),
                    no_op_if_exists: false,
                },
            ] {
                let outcome = crate::catalog::admission_test_support::publish_catalog_fixture(&metadata.runtime, metadata.incarnation, request).await;
                assert!(matches!(outcome, CatalogOutcome::KnownCommitted { .. }), "{outcome:?}");
            }
            let (release, concrete) = footer_port(&metadata);
            let port: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort> = concrete.clone();
            // The synchronous _in author installs its actual Source even if
            // the returned original F has never been polled.
            let mut unpolled_owner = None;
            let unpolled = metadata.capture_table_object_with_snapshot_cpu_in(original_capture_request(&metadata, "capture_original"), &port, &mut unpolled_owner);
            drop(unpolled);
            assert!(unpolled_owner.is_some());
            assert_eq!(concrete.calls.load(Ordering::SeqCst), 0);
            drop(unpolled_owner);
            let mut owner = None;
            let mut original = metadata.capture_table_object_with_snapshot_cpu_in(original_capture_request(&metadata, "capture_original"), &port, &mut owner);
            // Real filesystem preparation may itself yield. Wait for the actual
            // CPU submission, without assuming one loader poll completes I/O.
            let started = concrete.started.notified();
            tokio::pin!(started);
            started.as_mut().enable();
            tokio::select! {
                result = original.as_mut() => panic!("binding handed off before footer: {}", result.is_ok()),
                _ = &mut started => {},
            }
            // Retain the SAME original Future while a temporary borrower exits.
            let mut borrower = Box::pin(std::future::poll_fn(|cx| original.as_mut().poll(cx)));
            assert!(futures::poll!(&mut borrower).is_pending());
            assert_eq!(concrete.calls.load(Ordering::SeqCst), 1);
            assert!(!concrete.closed.load(Ordering::SeqCst));
            drop(borrower);
            release.send(()).unwrap();
            let (binding, snapshot) = original.await.unwrap();
            assert_eq!(binding.metadata.identity.table.as_ref(), "t");
            assert_eq!(snapshot, None);
            assert!(concrete.closed.load(Ordering::SeqCst));
            assert_eq!(concrete.calls.load(Ordering::SeqCst), 1);
            let weak = Arc::downgrade(owner.as_ref().unwrap().value::<crate::catalog_control::statistics::scoped_cpu::scoped_capture::MetadataCaptureOriginal>(metadata.descriptor.instance_id.as_str()).unwrap());
            let rejected = metadata.capture_table_object_with_snapshot_cpu_in(original_capture_request(&metadata, "capture_original"), &port, &mut owner).await.err().expect("occupied original receiver");
            assert_eq!(rejected.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(weak.upgrade().is_some());
            drop(owner);
            assert!(weak.upgrade().is_none(), "binding result does not hide the original runtime header");
        });
    }
}

#[cfg(test)]
#[path = "metadata/cow_borrowed_schema_tests.rs"]
mod cow_borrowed_schema_tests;

pub(crate) fn metadata_creation_error(
    cause: novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
) -> ConnectorError {
    match cause {
        novarocks_connector_contract::PureProviderCompileError::Provider(cause) => cause,
        novarocks_connector_contract::PureProviderCompileError::Control(cause) => {
            ConnectorError::compile_interruption(cause)
        }
    }
}

pub(crate) fn pure_catalog_control(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), novarocks_type_contract::CompileControlError> {
    if context.is_cancelled() {
        Err(novarocks_type_contract::CompileControlError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(novarocks_type_contract::CompileControlError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Use the original FE/provider cancellation and deadline observations. This
/// pure preparation creates no capacity authority or retained-memory invoice.
pub(crate) fn with_pure_schema_creation<T>(
    context: &novarocks_spi::connector::ConnectorRequestContext,
    operation: impl FnOnce(
        &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<T, ConnectorError>,
) -> Result<T, ConnectorError> {
    let mut request = |_: std::alloc::Layout| pure_catalog_control(context);
    let mut work = |_: usize| pure_catalog_control(context);
    let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    creation.work(0).map_err(metadata_creation_error)?;
    let result = operation(&mut creation)?;
    creation.work(0).map_err(metadata_creation_error)?;
    Ok(result)
}
