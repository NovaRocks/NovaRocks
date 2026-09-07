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

//! BE-only installation and execution of one exact Iceberg generation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogProviderKind, CatalogRuntime,
    CatalogRuntimeMaterializer, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceId, ConnectorOpenReaderRequest, ConnectorPrepareSplitRequest,
    ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor, ConnectorPreparedScanUnitSet,
    ConnectorProviderBindingKey, ConnectorReadExecution, ConnectorRequestContext,
    ConnectorScanUnitDomainFacts, ConnectorSplit,
};

use crate::access_binding::IcebergReadBinding;
use crate::file_reader::batch_reader::IcebergBatchReader;
use crate::file_reader::delta_reader::IcebergDeltaBatchReader;
use crate::file_reader::execution_payload::{
    ICEBERG_PREPARED_SCAN_UNIT_V1, ICEBERG_PREPARED_SPLIT_SHARED_V2,
    IcebergPreparedMetadataUnitPayloadV1, IcebergPreparedSplitSharedPayload,
    IcebergPreparedUnitPayload, SplitPayload, decode_payload, encode_payload,
    iceberg_unit_domain_facts, materialize_local_scan_units, validate_prepared_payload,
    validate_split_payload,
};
use crate::metadata_batch_reader::open_metadata_connector_reader;
use crate::resources::IcebergExecutionResources;

/// Startup-composed materializer for immutable catalog properties received by
/// a BE query lifecycle.  The filesystem binding is process-local and is
/// deliberately not derived from, or returned through, `CatalogProperties`.
pub struct IcebergCatalogRuntimeMaterializer {
    binding: IcebergReadBinding,
}

impl IcebergCatalogRuntimeMaterializer {
    pub fn new(resources: IcebergExecutionResources) -> Self {
        Self {
            binding: resources.binding().clone(),
        }
    }

    pub fn from_binding(binding: IcebergReadBinding) -> Self {
        Self { binding }
    }
}

struct IcebergCatalogRuntime {
    handle: CatalogHandle,
    _binding: IcebergReadBinding,
}

impl CatalogRuntime for IcebergCatalogRuntime {
    fn handle(&self) -> &CatalogHandle {
        &self.handle
    }

    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }
}

impl CatalogRuntimeMaterializer for IcebergCatalogRuntimeMaterializer {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError> {
        if properties.provider_kind() != CatalogProviderKind::Iceberg {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg catalog materializer received another provider kind",
            ));
        }
        let binding = self.binding.bind_catalog(properties)?;
        Ok(Arc::new(IcebergCatalogRuntime {
            handle: properties.handle().clone(),
            _binding: binding,
        }))
    }
}

/// Materializes only FE-frozen membership into local read units. Catalog
/// access and all planning remain outside this BE execution object.
struct IcebergReadOnlyConnectorInstance {
    key: ConnectorProviderBindingKey,
    binding: IcebergReadBinding,
}

impl IcebergReadOnlyConnectorInstance {
    fn validate_context(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
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

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        request.check_active()?;
        ensure_owner(split.owner(), &self.key.instance_id)?;
        let payload: SplitPayload = decode_payload(split.payload(), "Iceberg split")?;
        validate_split_payload(&payload)?;
        if payload.owner_instance_id != self.key.instance_id.as_str()
            || payload.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg split does not belong to this installed instance incarnation",
            ));
        }
        if let Some(metadata) = payload.metadata {
            let shared_payload = encode_payload(
                &IcebergPreparedSplitSharedPayload {
                    version: ICEBERG_PREPARED_SPLIT_SHARED_V2,
                    owner_instance_id: payload.owner_instance_id,
                    incarnation: payload.incarnation,
                    namespace: payload.namespace,
                    table: payload.table,
                    snapshot_id: payload.snapshot_id,
                    table_uuid: payload.table_uuid,
                    schema_id: payload.schema_id,
                    projection: payload.projection,
                    limit: payload.limit,
                    physical_predicates: payload.physical_predicates,
                    fact_columns: payload.fact_columns,
                    name_mapping: payload.name_mapping,
                    delta: payload.delta,
                    metadata: Some(metadata),
                },
                "prepared metadata split shared payload",
                request.context.max_handle_payload_bytes(),
            )?;
            let descriptor = ConnectorPreparedScanUnitDescriptor::try_new(
                encode_payload(
                    &IcebergPreparedMetadataUnitPayloadV1 { version: 1 },
                    "prepared metadata scan unit payload",
                    request.context.max_handle_payload_bytes(),
                )?,
                None,
                ConnectorScanUnitDomainFacts::missing(
                    novarocks_spi::connector::ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
                ),
            )?;
            return ConnectorPreparedScanUnitSet::try_new_with_preparation_evidence(
                self.key.clone(),
                split,
                shared_payload,
                vec![descriptor],
                Some("metadata"),
                &request,
            );
        }
        if payload.units.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg split has no frozen scan units",
            ));
        }
        if payload.delta.is_some() && payload.units.len() != 1 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg special scan split must carry exactly one frozen unit",
            ));
        }
        let special_unit = payload.delta.is_some();
        let fact_columns = payload.fact_columns.clone();
        let facts_are_conservative =
            payload.limit.is_some() || !payload.physical_predicates.is_empty() || special_unit;
        let shared_payload = encode_payload(
            &IcebergPreparedSplitSharedPayload {
                version: ICEBERG_PREPARED_SPLIT_SHARED_V2,
                owner_instance_id: payload.owner_instance_id,
                incarnation: payload.incarnation,
                namespace: payload.namespace,
                table: payload.table,
                snapshot_id: payload.snapshot_id,
                table_uuid: payload.table_uuid,
                schema_id: payload.schema_id,
                projection: payload.projection,
                limit: payload.limit,
                physical_predicates: payload.physical_predicates,
                fact_columns: payload.fact_columns,
                name_mapping: payload.name_mapping,
                delta: payload.delta,
                metadata: None,
            },
            "prepared split shared payload",
            request.context.max_handle_payload_bytes(),
        )?;
        let binding = self.binding.for_request(request.context.clone());
        let units = materialize_local_scan_units(&binding, payload.units, special_unit, &request)?;
        let leaf_kind = if units.iter().any(|unit| unit.row_groups.is_some()) {
            "row_group"
        } else {
            "file"
        };
        let mut inspections = HashMap::new();
        let descriptors = units
            .into_iter()
            .map(|unit| {
                request.check_active()?;
                let facts = iceberg_unit_domain_facts(
                    &binding,
                    &mut inspections,
                    &unit,
                    &fact_columns,
                    facts_are_conservative
                        || !unit.data_file.delete_files.is_empty()
                        || unit.data_file.included_positions.is_some(),
                    special_unit,
                    &request,
                )?;
                ConnectorPreparedScanUnitDescriptor::try_new(
                    encode_payload(
                        &IcebergPreparedUnitPayload {
                            version: ICEBERG_PREPARED_SCAN_UNIT_V1,
                            data_file: unit.data_file,
                            row_groups: unit.row_groups,
                        },
                        "prepared scan unit payload",
                        request.context.max_handle_payload_bytes(),
                    )?,
                    unit.estimated_bytes,
                    facts,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        ConnectorPreparedScanUnitSet::try_new_with_preparation_evidence(
            self.key.clone(),
            split,
            shared_payload,
            descriptors,
            Some(leaf_kind),
            &request,
        )
    }

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.validate_context(&request.context)?;
        if unit.binding_key() != &self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg prepared scan unit belongs to another installed instance incarnation",
            ));
        }
        let shared: IcebergPreparedSplitSharedPayload = decode_payload(
            unit.shared_payload(),
            "Iceberg prepared split shared payload",
        )?;
        if let Some(metadata) = shared.metadata {
            if shared.owner_instance_id != self.key.instance_id.as_str()
                || shared.incarnation != self.key.incarnation.to_bytes()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg metadata prepared unit does not belong to this installed instance incarnation",
                ));
            }
            return open_metadata_connector_reader(
                metadata.metadata_table_type,
                metadata.serialized_table,
                metadata.serialized_payload,
                request.expected_schema.clone(),
                request.batch,
                request.context,
            );
        }
        let prepared: IcebergPreparedUnitPayload =
            decode_payload(unit.payload(), "Iceberg prepared scan unit")?;
        validate_prepared_payload(&shared, &prepared)?;
        if shared.owner_instance_id != self.key.instance_id.as_str()
            || shared.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg prepared scan unit does not belong to this installed instance incarnation",
            ));
        }
        if let Some(delta) = shared.delta {
            let binding = self.binding.for_request(request.context.clone());
            return IcebergDeltaBatchReader::try_new(
                delta.source,
                delta.delete_side,
                binding,
                request,
            )
            .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>);
        }
        let binding = self.binding.for_request(request.context.clone());
        let file_context = binding.file_read_context(
            novarocks_fs::FileCancellation::new(),
            request.context.deadline(),
        )?;
        let access = binding.resolve_access_for_locations(
            std::iter::once(prepared.data_file.path.as_str()).chain(
                prepared
                    .data_file
                    .delete_files
                    .iter()
                    .map(|delete| delete.path.as_str()),
            ),
        )?;
        IcebergBatchReader::try_new_with_name_mapping_and_row_groups(
            &prepared.data_file,
            &shared.physical_predicates,
            shared.name_mapping.as_deref(),
            prepared.row_groups.as_deref(),
            access,
            request,
            file_context,
        )
        .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>)
    }
}

impl ConnectorReadExecution for IcebergReadOnlyConnectorInstance {
    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        self.prepare_split(split, request)
    }

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open_unit_reader(unit, request)
    }
}

fn ensure_owner(
    owner: &ConnectorInstanceId,
    expected: &ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if owner == expected {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector handle belongs to a different instance",
        ))
    }
}
