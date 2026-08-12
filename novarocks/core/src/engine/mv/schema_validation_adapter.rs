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

use crate::mv::persistence::descriptor::MvDescriptorV1;
use crate::mv::persistence::schema::{
    MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
};
use crate::mv::storage_observation::{
    MvLakePackageObservation, MvLakePublication, MvMaintenanceMetadataObservation,
    MvObservedMaintenancePolicy, MvObservedRefreshMarker, MvObservedSnapshot,
    MvObservedTargetField, MvPublishedBaseFact, MvPublishedLakeFacts, MvPublishedRefreshTechnique,
    MvRefreshBaseObservation, MvRefreshTargetObservation, MvSchemaValidationObservation,
    MvSchemaValidationPartitionContract, MvSchemaValidationPartitionField,
    MvSchemaValidationPartitionTransform, MvStorageObservationPort, MvTargetCreationObservation,
};
use novarocks_connector_iceberg::storage_inspector::{
    IcebergStorageInspector, IcebergStorageLakePublication, IcebergStoragePartitionContract,
    IcebergStoragePartitionTransform, IcebergStorageRefreshTechnique,
    IcebergStorageTargetObservation,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorTableMetadata,
};

const ICEBERG_ROW_LINEAGE_PROP: &str = "write.row-lineage";

/// Test-only equivalent of the Server composition adapter.
///
/// Core production keeps this module disabled. Iceberg MV integration fixtures
/// still construct the legacy Core control binding directly in Phase 1, so
/// they must explicitly install the same exact-lease storage observation
/// boundary that Server production installs.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TestIcebergMvStorageObservationAdapter {
    inspector: IcebergStorageInspector,
}

impl MvStorageObservationPort for TestIcebergMvStorageObservationAdapter {
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError> {
        let observed = self
            .inspector
            .observe_created_target(exact_lease, metadata, context)?;
        let fields = observed_fields(&observed);
        let partition = created_partition_contract(&observed);
        MvTargetCreationObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.schema_id,
            fields,
            partition,
        )
    }

    fn observe_schema_validation(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_created_target(exact_lease, metadata, context.clone())?;
        MvSchemaValidationObservation::try_new(
            observed.table_uuid.clone(),
            observed.schema_id,
            observed.format_v3,
            observed.explicit_row_lineage_enabled,
            observed_fields(&observed),
            validation_partition_contract(&observed),
            &context,
        )
    }

    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        let Some(observed) = self
            .inspector
            .observe_lake_package(exact_lease, metadata, context)?
        else {
            return Ok(None);
        };
        let properties = observed
            .descriptor_properties
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let descriptor = MvDescriptorV1::from_storage_properties(&properties).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg MV storage descriptor: {error}"),
            )
        })?;
        let publication = match observed.publication {
            IcebergStorageLakePublication::NeverPublished => MvLakePublication::NeverPublished,
            IcebergStorageLakePublication::Published(facts) => {
                let technique = match facts.technique {
                    IcebergStorageRefreshTechnique::Incremental => {
                        MvPublishedRefreshTechnique::Incremental
                    }
                    IcebergStorageRefreshTechnique::Full => MvPublishedRefreshTechnique::Full,
                    IcebergStorageRefreshTechnique::MetadataOnly => {
                        MvPublishedRefreshTechnique::MetadataOnly
                    }
                };
                let bases = facts
                    .bases
                    .into_iter()
                    .map(|base| MvPublishedBaseFact {
                        table_fqn: base.table_fqn,
                        table_uuid: base.table_uuid,
                        from_snapshot: base.from_snapshot,
                        to_snapshot: base.to_snapshot,
                    })
                    .collect();
                MvLakePublication::Published(MvPublishedLakeFacts::try_new(
                    facts.target_snapshot_id,
                    facts.refresh_id,
                    facts.mv_id,
                    facts.token,
                    technique,
                    bases,
                    facts.definition_fingerprint,
                    facts.rows,
                    facts.provenance_hash,
                    facts.waterline_hash,
                )?)
            }
        };
        MvLakePackageObservation::try_new(metadata.identity.clone(), descriptor, publication)
            .map(Some)
    }

    fn observe_refresh_base(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_refresh_base(exact_lease, metadata, context.clone())?;
        MvRefreshBaseObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.current_snapshot_id,
            &context,
        )
    }

    fn observe_refresh_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_refresh_target(exact_lease, metadata, context.clone())?;
        MvRefreshTargetObservation::try_new(
            metadata.identity.clone(),
            observed.table_uuid,
            observed.schema_id,
            refresh_partition_contract(&observed.partition),
            observed.current_snapshot_id,
            observed.ref_snapshot_ids,
            observed.field_ids,
            observed.main_ancestor_snapshot_ids,
            observed.current_snapshot_is_empty_bootstrap,
            observed
                .snapshot_markers
                .into_iter()
                .map(|(snapshot_id, marker)| {
                    (
                        snapshot_id,
                        MvObservedRefreshMarker {
                            refresh_id: marker.refresh_id,
                            mv_id: marker.mv_id,
                            token: marker.token,
                        },
                    )
                })
                .collect(),
            &context,
        )
    }

    fn observe_maintenance_metadata(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        let observed =
            self.inspector
                .observe_maintenance_metadata(exact_lease, metadata, context.clone())?;
        MvMaintenanceMetadataObservation::try_new(
            observed.current_snapshot_id,
            observed
                .snapshots
                .into_iter()
                .map(|snapshot| MvObservedSnapshot {
                    snapshot_id: snapshot.snapshot_id,
                    timestamp_ms: snapshot.timestamp_ms,
                })
                .collect(),
            observed.non_default_reference_count,
            observed.total_data_files,
            observed.total_delete_files,
            observed.total_files_size_bytes,
            MvObservedMaintenancePolicy {
                maintenance_enabled: observed.policy.maintenance_enabled,
                expire_max_snapshot_age_ms: observed.policy.expire_max_snapshot_age_ms,
                expire_min_snapshots_to_keep: observed.policy.expire_min_snapshots_to_keep,
                target_file_size_bytes: observed.policy.target_file_size_bytes,
            },
            &context,
        )
    }
}

fn refresh_partition_contract(observed: &IcebergStoragePartitionContract) -> MvPartitionContract {
    MvPartitionContract {
        target_spec_id: observed.target_spec_id,
        fields: observed
            .fields
            .iter()
            .map(|field| MvPartitionFieldContract {
                partition_field_id: field.partition_field_id,
                partition_field_name: field.partition_field_name.clone(),
                source_target_field_id: field.source_target_field_id,
                source_column_name: field.source_column_name.clone(),
                transform: created_partition_transform(field.transform.clone()),
            })
            .collect(),
    }
}

fn observed_fields(observed: &IcebergStorageTargetObservation) -> Vec<MvObservedTargetField> {
    observed
        .fields
        .iter()
        .map(|field| MvObservedTargetField {
            field_id: field.field_id,
            name: field.name.clone(),
            type_signature: field.type_signature.clone(),
            nullable: field.nullable,
        })
        .collect()
}

fn created_partition_contract(observed: &IcebergStorageTargetObservation) -> MvPartitionContract {
    MvPartitionContract {
        target_spec_id: observed.partition.target_spec_id,
        fields: observed
            .partition
            .fields
            .iter()
            .map(|field| MvPartitionFieldContract {
                partition_field_id: field.partition_field_id,
                partition_field_name: field.partition_field_name.clone(),
                source_target_field_id: field.source_target_field_id,
                source_column_name: field.source_column_name.clone(),
                transform: created_partition_transform(field.transform.clone()),
            })
            .collect(),
    }
}

fn validation_partition_contract(
    observed: &IcebergStorageTargetObservation,
) -> MvSchemaValidationPartitionContract {
    MvSchemaValidationPartitionContract::new(
        observed.partition.target_spec_id,
        observed
            .partition
            .fields
            .iter()
            .map(|field| {
                MvSchemaValidationPartitionField::new(
                    field.partition_field_id,
                    field.partition_field_name.clone(),
                    field.source_target_field_id,
                    field.source_column_name.clone(),
                    validation_partition_transform(field.transform.clone()),
                )
            })
            .collect(),
    )
}

fn created_partition_transform(
    transform: IcebergStoragePartitionTransform,
) -> MvPartitionTransformContract {
    match transform {
        IcebergStoragePartitionTransform::Identity => MvPartitionTransformContract::Identity,
        IcebergStoragePartitionTransform::Year => MvPartitionTransformContract::Year,
        IcebergStoragePartitionTransform::Month => MvPartitionTransformContract::Month,
        IcebergStoragePartitionTransform::Day => MvPartitionTransformContract::Day,
        IcebergStoragePartitionTransform::Hour => MvPartitionTransformContract::Hour,
        IcebergStoragePartitionTransform::Bucket { num_buckets } => {
            MvPartitionTransformContract::Bucket { num_buckets }
        }
        IcebergStoragePartitionTransform::Truncate { width } => {
            MvPartitionTransformContract::Truncate { width }
        }
        IcebergStoragePartitionTransform::Void => MvPartitionTransformContract::Void,
    }
}

fn validation_partition_transform(
    transform: IcebergStoragePartitionTransform,
) -> MvSchemaValidationPartitionTransform {
    match transform {
        IcebergStoragePartitionTransform::Identity => {
            MvSchemaValidationPartitionTransform::Identity
        }
        IcebergStoragePartitionTransform::Year => MvSchemaValidationPartitionTransform::Year,
        IcebergStoragePartitionTransform::Month => MvSchemaValidationPartitionTransform::Month,
        IcebergStoragePartitionTransform::Day => MvSchemaValidationPartitionTransform::Day,
        IcebergStoragePartitionTransform::Hour => MvSchemaValidationPartitionTransform::Hour,
        IcebergStoragePartitionTransform::Bucket { num_buckets } => {
            MvSchemaValidationPartitionTransform::Bucket { num_buckets }
        }
        IcebergStoragePartitionTransform::Truncate { width } => {
            MvSchemaValidationPartitionTransform::Truncate { width }
        }
        IcebergStoragePartitionTransform::Void => MvSchemaValidationPartitionTransform::Void,
    }
}

pub(crate) fn current_iceberg_table_observation(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<MvSchemaValidationObservation, String> {
    current_iceberg_table_observation_with_schema(table, table.metadata().current_schema())
}

fn current_iceberg_table_observation_with_schema(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
) -> Result<MvSchemaValidationObservation, String> {
    let metadata = table.metadata();
    MvSchemaValidationObservation::try_new_with_maximum_payload(
        metadata.uuid().to_string(),
        schema.schema_id(),
        metadata.format_version() == novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3,
        row_lineage_enabled(metadata.properties()),
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                MvObservedTargetField::new(
                    field.id,
                    field.name.clone(),
                    field.field_type.to_string(),
                    !field.required,
                )
            })
            .collect(),
        partition_contract(metadata.default_partition_spec(), schema)?,
    )
    .map_err(|error| error.to_string())
}

fn partition_contract(
    spec: &novarocks_connector_iceberg::iceberg::spec::PartitionSpec,
    schema: &novarocks_connector_iceberg::iceberg::spec::Schema,
) -> Result<MvSchemaValidationPartitionContract, String> {
    let fields = spec
        .fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "partition field {} references missing source field ID {}",
                    field.name, field.source_id
                )
            })?;
            let transform = match &field.transform {
                novarocks_connector_iceberg::iceberg::spec::Transform::Identity => {
                    MvSchemaValidationPartitionTransform::Identity
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Year => {
                    MvSchemaValidationPartitionTransform::Year
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Month => {
                    MvSchemaValidationPartitionTransform::Month
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Day => {
                    MvSchemaValidationPartitionTransform::Day
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Hour => {
                    MvSchemaValidationPartitionTransform::Hour
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(num_buckets) => {
                    MvSchemaValidationPartitionTransform::Bucket {
                        num_buckets: *num_buckets,
                    }
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(width) => {
                    MvSchemaValidationPartitionTransform::Truncate { width: *width }
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Void => {
                    MvSchemaValidationPartitionTransform::Void
                }
                novarocks_connector_iceberg::iceberg::spec::Transform::Unknown => {
                    MvSchemaValidationPartitionTransform::Unsupported(format!(
                        "{:?}",
                        field.transform
                    ))
                }
            };
            Ok(MvSchemaValidationPartitionField::new(
                field.field_id,
                field.name.clone(),
                field.source_id,
                source.name.clone(),
                transform,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MvSchemaValidationPartitionContract::new(
        spec.spec_id(),
        fields,
    ))
}

fn row_lineage_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get(ICEBERG_ROW_LINEAGE_PROP)
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_lineage_enabled_recognizes_case_insensitive_true() {
        let mut properties = std::collections::HashMap::new();
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "TRUE".to_string());
        assert!(row_lineage_enabled(&properties));
        properties.insert(ICEBERG_ROW_LINEAGE_PROP.to_string(), "false".to_string());
        assert!(!row_lineage_enabled(&properties));
        properties.clear();
        assert!(!row_lineage_enabled(&properties));
    }
}
