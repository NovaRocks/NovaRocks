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

//! Preparation of one atomic managed partition replacement.
//!
//! A managed publication may replace the target's default partition spec, but
//! only inside the *same* external commit that publishes its snapshot: a
//! separate spec commit would leave a window in which the table is partitioned
//! one way and its data another. This module turns the neutral replacement
//! facts into the exact metadata updates that commit must carry ahead of its
//! snapshot updates, plus the prospective metadata every writer and every
//! staged artifact is then interpreted against.
//!
//! The prior partition observation is checked against the original table. When
//! ordinary storage changes accompany the transition, the same provider
//! preparation also freezes the storage projection used by the writers.

use novarocks_spi::connector::{
    ConnectorCommittedPartitionField, ConnectorCommittedPartitioning, ConnectorError,
    ConnectorErrorKind, ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
    ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionTransform,
    ConnectorOperationControl, ConnectorRequestContext,
};

use sha2::{Digest, Sha256};

use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};
use crate::commit::staging::PreparedChange;
use crate::iceberg::TableUpdate;
use crate::iceberg::spec::TableMetadata;

use crate::commit::write_stack::domain::{corrupt, invalid};

/// One prepared atomic replacement: declarative updates for common staging,
/// exact prospective metadata for writer binding, and committed partitioning.
#[derive(Clone, Debug)]
pub(crate) struct IcebergPreparedRepartition {
    metadata_updates: Vec<crate::iceberg::TableUpdate>,
    prospective_metadata: crate::iceberg::spec::TableMetadata,
    metadata_requirements: Vec<crate::iceberg::TableRequirement>,
    committed: ConnectorCommittedPartitioning,
    storage_update_count: Option<usize>,
    storage_projection_digest: Option<[u8; 32]>,
}

impl IcebergPreparedRepartition {
    #[cfg(test)]
    pub(crate) fn metadata_updates(&self) -> &[crate::iceberg::TableUpdate] {
        &self.metadata_updates
    }

    #[cfg(test)]
    pub(crate) fn metadata_requirements(&self) -> &[crate::iceberg::TableRequirement] {
        &self.metadata_requirements
    }

    pub(crate) fn prepared_change(&self) -> crate::commit::staging::PreparedChange {
        crate::commit::staging::PreparedChange {
            updates: self.metadata_updates.clone(),
            requirements: self.metadata_requirements.clone(),
        }
    }

    pub(crate) fn storage_updates(&self) -> Option<&[TableUpdate]> {
        self.storage_update_count
            .map(|count| &self.metadata_updates[..count])
    }

    pub(crate) const fn storage_projection_digest(&self) -> Option<[u8; 32]> {
        self.storage_projection_digest
    }

    pub(crate) fn prospective_metadata(&self) -> &crate::iceberg::spec::TableMetadata {
        &self.prospective_metadata
    }

    pub(crate) fn committed(&self) -> &ConnectorCommittedPartitioning {
        &self.committed
    }
}

pub(crate) fn admit_storage_alter(
    catalog: &dyn crate::catalog::NovaRocksCatalog,
    namespace: &str,
    table: &str,
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    catalog
        .admit(&CatalogAdmissionRequest::new(
            CatalogOperation::AlterProperties,
            crate::catalog::CatalogTableName::new(namespace, table),
            context.initiation(),
        ))
        .map(|_| ())
        .map_err(connector_unsupported)
}

/// Prepare the atomic partition transition and the effective writer metadata.
pub(crate) fn prepare_managed_repartition(
    metadata: &TableMetadata,
    replacement: &ConnectorManagedPartitionSpecReplacement,
    control: &dyn ConnectorOperationControl,
) -> Result<IcebergPreparedRepartition, ConnectorError> {
    let current_fields = managed_partition_fields(metadata.default_partition_spec())?;
    let current_observation = ConnectorManagedPartitionSpecObservation::try_from_fields(
        metadata.default_partition_spec_id(),
        &current_fields,
    )?;
    if current_observation != replacement.expected_prior_default() {
        return Err(invalid(
            "Iceberg default partition spec does not match the exact managed observation",
        ));
    }

    let mut builder_metadata = metadata.clone().into_builder(None);
    let mut storage_update_count = None;
    let mut metadata_updates = Vec::new();
    let mut metadata_requirements = Vec::new();
    if let Some(properties) = replacement.storage_properties() {
        let PreparedChange {
            updates,
            requirements,
        } = crate::catalog_control::catalog_mutation::prepare_set_table_property_entries(
            metadata, properties, control,
        )?;
        for update in &updates {
            control.check_active()?;
            if !matches!(
                update,
                TableUpdate::SetProperties { .. }
                    | TableUpdate::RemoveProperties { .. }
                    | TableUpdate::UpgradeFormatVersion { .. }
            ) {
                return Err(corrupt(
                    "Iceberg managed storage preparation returned a non-storage update",
                ));
            }
            builder_metadata = update.clone().apply(builder_metadata).map_err(|error| {
                invalid(format!("prepare Iceberg managed storage change: {error}"))
            })?;
        }
        storage_update_count = Some(updates.len());
        metadata_updates = updates;
        metadata_requirements = requirements;
    }

    let schema = metadata.current_schema();
    let mut builder = crate::iceberg::spec::UnboundPartitionSpecBuilder::new();
    for field in replacement.fields() {
        let source = schema.field_by_id(field.source_field_id()).ok_or_else(|| {
            invalid(format!(
                "Iceberg managed partition source field {} is absent from the exact schema",
                field.source_field_id()
            ))
        })?;
        let transform = iceberg_partition_transform(field.transform());
        transform
            .result_type(source.field_type.as_ref())
            .map_err(|error| {
                invalid(format!(
                    "Iceberg managed partition transform is invalid for source field {}: {error}",
                    field.source_field_id()
                ))
            })?;
        builder = builder
            .add_partition_fields([crate::iceberg::spec::UnboundPartitionField {
                source_id: field.source_field_id(),
                field_id: None,
                name: managed_partition_field_name(source.name.as_str(), field.transform()),
                transform,
            }])
            .map_err(|error| {
                invalid(format!("build Iceberg replacement partition spec: {error}"))
            })?;
    }
    let requested_spec = builder.build();
    let build = builder_metadata
        .add_default_partition_spec(requested_spec.clone())
        .map_err(|error| invalid(format!("bind Iceberg replacement partition spec: {error}")))?
        .build()
        .map_err(|error| {
            invalid(format!(
                "finalize Iceberg replacement partition spec: {error}"
            ))
        })?;
    let spec_id = build.metadata.default_partition_spec_id();
    if spec_id == metadata.default_partition_spec_id() {
        return Err(invalid(
            "Iceberg managed partition replacement is identical to the current default spec",
        ));
    }
    let committed_spec = build
        .metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| corrupt("Iceberg prospective default partition spec is missing"))?;
    // Declarative updates are normalized by the common staging engine. Reusing
    // a historical spec is valid even when the builder emits only a selector.
    metadata_updates.extend([
        TableUpdate::AddSpec {
            spec: requested_spec,
        },
        TableUpdate::SetDefaultSpec { spec_id: -1 },
    ]);
    let committed_fields = committed_spec
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let source = build
                .metadata
                .current_schema()
                .field_by_id(field.source_id)
                .ok_or_else(|| {
                    corrupt(format!(
                        "Iceberg prospective partition source field {} is missing",
                        field.source_id
                    ))
                })?;
            ConnectorCommittedPartitionField::try_new(
                field.field_id,
                field.name.clone(),
                field.source_id,
                source.name.clone(),
                u32::try_from(position)
                    .map_err(|_| corrupt("Iceberg committed partition position exceeds u32"))?,
                connector_partition_transform(&field.transform)?,
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let committed = ConnectorCommittedPartitioning::try_new(spec_id, committed_fields)?;
    metadata_requirements.extend([
        crate::iceberg::TableRequirement::DefaultSpecIdMatch {
            default_spec_id: metadata.default_partition_spec_id(),
        },
        crate::iceberg::TableRequirement::LastAssignedPartitionIdMatch {
            last_assigned_partition_id: metadata.last_partition_id(),
        },
    ]);
    let mut prepared = IcebergPreparedRepartition {
        prospective_metadata: build.metadata,
        metadata_updates,
        metadata_requirements,
        committed,
        storage_update_count,
        storage_projection_digest: None,
    };
    if let Some(updates) = prepared.storage_updates() {
        control.check_active()?;
        let effective = prepared.prospective_metadata();
        let base = (
            metadata.uuid(),
            metadata.location(),
            metadata.format_version(),
            metadata.current_schema().as_ref(),
            metadata.current_snapshot_id(),
            metadata.last_sequence_number(),
            metadata.default_partition_spec_id(),
            metadata.last_partition_id(),
            metadata.properties(),
        );
        let writer = (
            effective.format_version(),
            effective.default_partition_spec().as_ref(),
            effective.properties(),
        );
        // The prepared target and Begin can load different snapshot history.
        // Use the original immutable base facts and the exact writer projection
        // rather than serializing their full metadata envelopes.
        let value = serde_json::to_value((
            replacement.replacement_id().to_bytes(),
            base,
            updates,
            writer,
        ))
        .map_err(|error| corrupt(format!("encode managed storage projection: {error}")))?;
        let bytes = serde_json::to_vec(&crate::commit::model::canonical_json(value))
            .map_err(|error| corrupt(format!("serialize managed storage projection: {error}")))?;
        let mut digest = Sha256::new();
        digest.update(b"novarocks.iceberg.managed-repartition-storage-projection.v1\0");
        digest.update(bytes);
        prepared.storage_projection_digest = Some(digest.finalize().into());
        control.check_active()?;
    }
    Ok(prepared)
}

/// Observe a live partition spec as the neutral field vocabulary, so the prior
/// default can be compared against the replacement's expectation without either
/// side reading provider metadata.
fn managed_partition_fields(
    spec: &crate::iceberg::spec::PartitionSpec,
) -> Result<Vec<ConnectorManagedPartitionField>, ConnectorError> {
    spec.fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            ConnectorManagedPartitionField::try_new(
                field.source_id,
                u32::try_from(position)
                    .map_err(|_| corrupt("Iceberg partition field position exceeds u32"))?,
                connector_partition_transform(&field.transform)?,
            )
        })
        .collect()
}

fn connector_partition_transform(
    transform: &crate::iceberg::spec::Transform,
) -> Result<ConnectorManagedPartitionTransform, ConnectorError> {
    use crate::iceberg::spec::Transform;
    match transform {
        Transform::Identity => Ok(ConnectorManagedPartitionTransform::Identity),
        Transform::Year => Ok(ConnectorManagedPartitionTransform::Year),
        Transform::Month => Ok(ConnectorManagedPartitionTransform::Month),
        Transform::Day => Ok(ConnectorManagedPartitionTransform::Day),
        Transform::Hour => Ok(ConnectorManagedPartitionTransform::Hour),
        Transform::Bucket(buckets) => {
            Ok(ConnectorManagedPartitionTransform::Bucket { buckets: *buckets })
        }
        Transform::Truncate(width) => {
            Ok(ConnectorManagedPartitionTransform::Truncate { width: *width })
        }
        Transform::Void => Ok(ConnectorManagedPartitionTransform::Void),
        Transform::Unknown => Err(unsupported(
            "Iceberg managed partition replacement cannot observe an unknown transform",
        )),
    }
}

fn iceberg_partition_transform(
    transform: ConnectorManagedPartitionTransform,
) -> crate::iceberg::spec::Transform {
    use crate::iceberg::spec::Transform;
    match transform {
        ConnectorManagedPartitionTransform::Identity => Transform::Identity,
        ConnectorManagedPartitionTransform::Year => Transform::Year,
        ConnectorManagedPartitionTransform::Month => Transform::Month,
        ConnectorManagedPartitionTransform::Day => Transform::Day,
        ConnectorManagedPartitionTransform::Hour => Transform::Hour,
        ConnectorManagedPartitionTransform::Bucket { buckets } => Transform::Bucket(buckets),
        ConnectorManagedPartitionTransform::Truncate { width } => Transform::Truncate(width),
        ConnectorManagedPartitionTransform::Void => Transform::Void,
    }
}

fn managed_partition_field_name(
    source: &str,
    transform: ConnectorManagedPartitionTransform,
) -> String {
    match transform {
        ConnectorManagedPartitionTransform::Identity => source.to_string(),
        ConnectorManagedPartitionTransform::Year => format!("{source}_year"),
        ConnectorManagedPartitionTransform::Month => format!("{source}_month"),
        ConnectorManagedPartitionTransform::Day => format!("{source}_day"),
        ConnectorManagedPartitionTransform::Hour => format!("{source}_hour"),
        ConnectorManagedPartitionTransform::Bucket { buckets } => {
            format!("{source}_bucket_{buckets}")
        }
        ConnectorManagedPartitionTransform::Truncate { width } => {
            format!("{source}_truncate_{width}")
        }
        ConnectorManagedPartitionTransform::Void => format!("{source}_void"),
    }
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_spi::connector::{
        ConnectorErrorKind, ConnectorManagedPartitionField,
        ConnectorManagedPartitionSpecObservation, ConnectorManagedPartitionSpecReplacement,
        ConnectorManagedPartitionTransform, ConnectorWriteOperationId,
    };

    use super::*;

    /// An unpartitioned table with one `id` column, built without a catalog so
    /// each test perturbs exactly one replacement fact.
    fn unpartitioned_metadata() -> crate::iceberg::spec::TableMetadata {
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "s3://b/wh/db/t".to_string(),
            crate::iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    /// The observation a replacement must carry to match `metadata`'s live
    /// default spec.
    fn prior_observation(
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> ConnectorManagedPartitionSpecObservation {
        let fields =
            managed_partition_fields(metadata.default_partition_spec()).expect("prior fields");
        ConnectorManagedPartitionSpecObservation::try_from_fields(
            metadata.default_partition_spec_id(),
            &fields,
        )
        .expect("prior observation")
    }

    fn identity_on_id() -> Vec<ConnectorManagedPartitionField> {
        vec![
            ConnectorManagedPartitionField::try_new(
                1,
                0,
                ConnectorManagedPartitionTransform::Identity,
            )
            .expect("requested partition field"),
        ]
    }

    fn replacement(
        expected_prior_default: ConnectorManagedPartitionSpecObservation,
        fields: Vec<ConnectorManagedPartitionField>,
    ) -> ConnectorManagedPartitionSpecReplacement {
        ConnectorManagedPartitionSpecReplacement::try_new(
            ConnectorWriteOperationId::new(),
            expected_prior_default,
            fields,
        )
        .expect("replacement")
    }

    fn control() -> ConnectorRequestContext {
        use std::time::{Duration, Instant};

        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    fn preview_managed_repartition(
        metadata: &TableMetadata,
        replacement: &ConnectorManagedPartitionSpecReplacement,
    ) -> Result<IcebergPreparedRepartition, ConnectorError> {
        prepare_managed_repartition(metadata, replacement, &control())
    }

    #[test]
    fn combined_storage_freezes_writer_settings_and_leaves_the_original_base() {
        use novarocks_spi::connector::{DdlPropertyChange as Change, DdlPropertyValue as Value};
        use std::collections::HashMap;

        let original = unpartitioned_metadata()
            .into_builder(None)
            .set_properties(HashMap::from([
                ("commit.retry.num-retries".to_string(), "4".to_string()),
                ("read.split.target-size".to_string(), "1024".to_string()),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let replace = replacement(prior_observation(&original), identity_on_id())
            .with_storage_properties(
                Arc::from([
                    (
                        Arc::from("data_location"),
                        Change::Set(Value::Varchar("s3://b/new-data".into())),
                    ),
                    (
                        Arc::from("partitioning"),
                        Change::Set(Value::ArrayVarchar(vec![Some("id".into())])),
                    ),
                    (
                        Arc::from("parquet_writer_row_group_size"),
                        Change::Set(Value::Varchar("1MB".into())),
                    ),
                    (
                        Arc::from("statistics_collect_on_write"),
                        Change::Set(Value::Boolean(false)),
                    ),
                    (Arc::from("max_commit_retry"), Change::Reset(Value::Null)),
                ]),
                1,
            )
            .unwrap();
        let forecast = prepare_managed_repartition(&original, &replace, &control()).unwrap();
        let effective = forecast.prospective_metadata();
        assert_eq!(
            super::super::control::iceberg_data_location(effective),
            "s3://b/new-data"
        );
        assert_eq!(
            crate::commit::data_writer::parquet_row_group_size_bytes(effective.properties())
                .unwrap(),
            Some(1_048_576)
        );
        let statistics = &effective.properties()[crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY];
        assert!(!super::super::control::collect_on_write_property_enabled(
            statistics
        ));
        assert!(
            !effective
                .properties()
                .contains_key("commit.retry.num-retries")
        );
        assert_eq!(effective.properties()["read.split.target-size"], "1024");
        assert_eq!(original.properties()["commit.retry.num-retries"], "4");
        assert_eq!(
            super::super::control::iceberg_data_location(&original),
            "s3://b/wh/db/t/data"
        );
        assert!(original.default_partition_spec().is_unpartitioned());
        assert_eq!(original.uuid(), effective.uuid());
        assert_eq!(
            original.current_snapshot_id(),
            effective.current_snapshot_id()
        );
        assert!(matches!(
            forecast.storage_updates().unwrap(),
            [
                TableUpdate::SetProperties { .. },
                TableUpdate::RemoveProperties { .. }
            ]
        ));
        assert!(matches!(
            &forecast.metadata_updates()[2..],
            [
                TableUpdate::AddSpec { .. },
                TableUpdate::SetDefaultSpec { .. }
            ]
        ));
        let digest = forecast.storage_projection_digest().unwrap();
        let repeated =
            prepare_managed_repartition(&original, &replace.clone(), &control()).unwrap();
        assert_eq!(repeated.storage_projection_digest(), Some(digest));

        let altered = original
            .into_builder(None)
            .set_properties(HashMap::from([(
                "read.split.target-size".into(),
                "2048".into(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let later = prepare_managed_repartition(&altered, &replace, &control()).unwrap();
        assert_eq!(later.committed(), forecast.committed());
        assert_ne!(later.storage_projection_digest(), Some(digest));
    }

    #[test]
    fn invalid_combined_storage_preserves_the_property_error() {
        use novarocks_spi::connector::{
            DdlPropertyChange as Change, DdlPropertyValue as Value, InvalidTableProperty,
        };

        let original = unpartitioned_metadata();
        let replace = replacement(prior_observation(&original), identity_on_id())
            .with_storage_properties(
                Arc::from([
                    (
                        Arc::from("partitioning"),
                        Change::Set(Value::ArrayVarchar(vec![Some("id".into())])),
                    ),
                    (
                        Arc::from("parquet_writer_row_group_size"),
                        Change::Set(Value::Varchar("bad".into())),
                    ),
                ]),
                0,
            )
            .unwrap();
        let error = prepare_managed_repartition(&original, &replace, &control()).unwrap_err();
        assert!(InvalidTableProperty::from_connector_error(&error).is_some());
        assert!(original.default_partition_spec().is_unpartitioned());
    }

    #[test]
    fn a_prepared_repartition_is_add_spec_then_set_default_spec() {
        let metadata = unpartitioned_metadata();
        let replacement = replacement(prior_observation(&metadata), identity_on_id());

        let prepared =
            preview_managed_repartition(&metadata, &replacement).expect("prepare the repartition");

        assert_eq!(prepared.metadata_updates().len(), 2);
        assert!(matches!(
            prepared.metadata_updates()[1],
            crate::iceberg::TableUpdate::SetDefaultSpec { .. }
        ));
        let crate::iceberg::TableUpdate::AddSpec { spec } = &prepared.metadata_updates()[0] else {
            panic!("partition replacement must start with AddSpec");
        };
        assert!(spec.fields().iter().all(|field| field.field_id.is_none()));
        assert!(prepared.metadata_requirements().iter().any(|r| matches!(
            r,
            crate::iceberg::TableRequirement::LastAssignedPartitionIdMatch { .. }
        )));

        let new_spec_id = prepared.committed().spec_id();
        assert_ne!(new_spec_id, metadata.default_partition_spec_id());
        assert_eq!(
            prepared.prospective_metadata().default_partition_spec_id(),
            new_spec_id
        );
        let committed_field = &prepared.committed().fields()[0];
        assert_eq!(committed_field.source_field_id(), 1);
        assert_eq!(committed_field.source_column_name(), "id");
        assert_eq!(committed_field.partition_field_name(), "id");
        assert_eq!(
            committed_field.transform(),
            ConnectorManagedPartitionTransform::Identity
        );
    }

    #[test]
    fn a_preview_contains_only_the_partition_transition() {
        let metadata = unpartitioned_metadata();
        let replacement = replacement(prior_observation(&metadata), identity_on_id());

        let prepared =
            preview_managed_repartition(&metadata, &replacement).expect("preview the repartition");

        assert_eq!(prepared.metadata_updates().len(), 2);
        assert!(matches!(
            prepared.metadata_updates()[0],
            crate::iceberg::TableUpdate::AddSpec { .. }
        ));
        assert!(matches!(
            prepared.metadata_updates()[1],
            crate::iceberg::TableUpdate::SetDefaultSpec { .. }
        ));
        assert_ne!(
            prepared.committed().spec_id(),
            metadata.default_partition_spec_id()
        );
    }

    #[test]
    fn a_replacement_whose_expected_prior_default_disagrees_is_refused() {
        let metadata = unpartitioned_metadata();
        // The table is unpartitioned, so claiming the prior default already
        // partitioned by `id` is a stale observation: another writer would have
        // had to repartition it first.
        let stale = ConnectorManagedPartitionSpecObservation::try_from_fields(
            metadata.default_partition_spec_id(),
            &identity_on_id(),
        )
        .expect("stale observation");
        let replacement = replacement(stale, identity_on_id());

        let error = preview_managed_repartition(&metadata, &replacement)
            .expect_err("a stale prior observation must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn a_replacement_whose_expected_prior_spec_id_disagrees_is_refused() {
        let metadata = unpartitioned_metadata();
        let fields =
            managed_partition_fields(metadata.default_partition_spec()).expect("prior fields");
        let wrong_spec_id = ConnectorManagedPartitionSpecObservation::try_from_fields(
            metadata.default_partition_spec_id() + 1,
            &fields,
        )
        .expect("observation of another spec ID");
        let replacement = replacement(wrong_spec_id, identity_on_id());

        let error = preview_managed_repartition(&metadata, &replacement)
            .expect_err("a prior observation of another spec ID must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn a_no_op_replacement_is_refused() {
        // Replaying the table's existing partitioning would change nothing
        // while still claiming a partition transition, so it must not reach the
        // commit. Binding an equivalent spec resolves to the current ID, so
        // the exact prospective default-spec comparison refuses the no-op.
        let metadata = {
            let base = unpartitioned_metadata();
            let replacement = replacement(prior_observation(&base), identity_on_id());
            preview_managed_repartition(&base, &replacement)
                .expect("partition the table once")
                .prospective_metadata
        };
        let replacement = replacement(prior_observation(&metadata), identity_on_id());

        let error = preview_managed_repartition(&metadata, &replacement)
            .expect_err("a no-op replacement must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .message()
                .contains("identical to the current default spec"),
            "a no-op replacement must be refused as an absent partition transition: {error}"
        );
    }

    #[test]
    fn a_replacement_naming_an_absent_source_field_is_refused() {
        let metadata = unpartitioned_metadata();
        let absent = vec![
            ConnectorManagedPartitionField::try_new(
                42,
                0,
                ConnectorManagedPartitionTransform::Identity,
            )
            .expect("requested partition field"),
        ];
        let replacement = replacement(prior_observation(&metadata), absent);

        let error = preview_managed_repartition(&metadata, &replacement)
            .expect_err("an absent source field must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn a_transform_the_source_type_cannot_carry_is_refused() {
        // `id` is a long, and a year transform is defined only over date and
        // timestamp sources.
        let metadata = unpartitioned_metadata();
        let mistyped = vec![
            ConnectorManagedPartitionField::try_new(1, 0, ConnectorManagedPartitionTransform::Year)
                .expect("requested partition field"),
        ];
        let replacement = replacement(prior_observation(&metadata), mistyped);

        let error = preview_managed_repartition(&metadata, &replacement)
            .expect_err("a transform the source type cannot carry must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn managed_partition_fields_observe_a_bound_spec_as_neutral_fields() {
        let metadata = unpartitioned_metadata();
        assert!(
            managed_partition_fields(metadata.default_partition_spec())
                .expect("observe the unpartitioned default")
                .is_empty()
        );

        let replacement = replacement(prior_observation(&metadata), identity_on_id());
        let prepared =
            preview_managed_repartition(&metadata, &replacement).expect("prepare the repartition");
        let observed =
            managed_partition_fields(prepared.prospective_metadata().default_partition_spec())
                .expect("observe the prospective default");
        assert_eq!(observed, identity_on_id());
    }
    #[tokio::test]
    async fn full_replacement_can_publish_an_unpartitioned_spec_with_its_new_snapshot() {
        use crate::commit::fast_append::FastAppendPreparer;
        use crate::commit::model::AddedContent;
        use crate::commit::overwrite::preparer_tests::{
            Fixture, data, raw_entries, raw_snapshot_entries,
        };
        use crate::commit::staging::{StagingBase, StagingEngine};
        use crate::iceberg::{
            TableRequirement, TableUpdate,
            spec::{FormatVersion, Literal, ManifestStatus, Struct},
        };

        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let unpartitioned_spec_id = base.default_partition_spec_id();
        let partitioned = preview_managed_repartition(
            &base,
            &replacement(prior_observation(&base), identity_on_id()),
        )
        .unwrap()
        .prospective_metadata;
        let original_spec_id = partitioned.default_partition_spec_id();
        let old = "s3://b/partitioned.parquet";
        let seed = fixture.intent(
            &partitioned,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data(old, 3, Struct::from_iter([Some(Literal::long(7))])),
                    original_spec_id,
                )
                .unwrap(),
            ],
        );
        let (partitioned, _) = fixture.stage(partitioned, &seed, &FastAppendPreparer).await;
        let parent = partitioned.current_snapshot().unwrap().snapshot_id();
        let clear = replacement(prior_observation(&partitioned), vec![]);
        // The original exact prior fence also applies to an empty input.
        let stale = preview_managed_repartition(&base, &clear).unwrap_err();
        assert!(stale.message().contains("exact managed observation"));
        let prepared = preview_managed_repartition(&partitioned, &clear).unwrap();
        assert_eq!(prepared.committed().spec_id(), unpartitioned_spec_id);
        assert!(prepared.committed().fields().is_empty());
        assert!(
            prepared
                .prospective_metadata()
                .default_partition_spec()
                .is_unpartitioned()
        );

        let next = "s3://b/unpartitioned.parquet";
        let intent = fixture.intent(
            &partitioned,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data(next, 2, Struct::empty()),
                    prepared.committed().spec_id(),
                )
                .unwrap(),
            ],
        );
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: partitioned.clone(),
                metadata_location: "s3://b/partitioned.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        engine.stage_change(prepared.prepared_change()).unwrap();
        engine
            .stage(&crate::commit::filter_delete::FilterDeletePreparer::for_full_refresh())
            .await
            .unwrap();
        engine
            .stage(&crate::commit::fast_append::FullRefreshAppendFixture(None))
            .await
            .unwrap();
        let after = engine.metadata().clone();
        let frozen = engine.freeze().unwrap();
        assert_eq!(after.default_partition_spec_id(), unpartitioned_spec_id);
        assert_eq!(
            after.partition_spec_by_id(original_spec_id),
            partitioned.partition_spec_by_id(original_spec_id),
        );
        let appended = after.current_snapshot().unwrap();
        let deleted = after
            .snapshot_by_id(appended.parent_snapshot_id().unwrap())
            .unwrap();
        assert_eq!(deleted.parent_snapshot_id(), Some(parent));
        assert_eq!(
            deleted.summary().operation,
            crate::iceberg::spec::Operation::Delete
        );
        assert_eq!(
            appended.summary().operation,
            crate::iceberg::spec::Operation::Append
        );
        assert!(
            frozen
                .requirements()
                .contains(&TableRequirement::DefaultSpecIdMatch {
                    default_spec_id: original_spec_id,
                })
        );
        assert!(frozen.updates().contains(&TableUpdate::SetDefaultSpec {
            spec_id: unpartitioned_spec_id,
        }));
        assert!(
            frozen
                .updates()
                .iter()
                .any(|update| matches!(update, TableUpdate::AddSnapshot { .. }))
        );
        let deleted_entries = raw_snapshot_entries(&after, deleted, &attempt).await;
        assert!(
            deleted_entries
                .iter()
                .any(|entry| entry.data_file().file_path() == old
                    && entry.status == ManifestStatus::Deleted)
        );
        let entries = raw_entries(&after, "main", &attempt).await;
        assert!(
            entries
                .iter()
                .any(|entry| entry.data_file().file_path() == next
                    && entry.status == ManifestStatus::Added)
        );
        let no_op = replacement(prior_observation(&after), vec![]);
        assert!(
            preview_managed_repartition(&after, &no_op)
                .unwrap_err()
                .message()
                .contains("identical to the current default spec")
        );
    }

    #[tokio::test]
    async fn repartition_staging_binds_new_ids_and_reuses_an_exact_historical_spec() {
        use crate::commit::model::{
            FileChanges, OperationIntent, OperationIntentParts, RequestShape, TableTarget,
        };
        use crate::commit::staging::{StagingBase, StagingEngine};
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let base = fixture.metadata(crate::iceberg::spec::FormatVersion::V2);
        let intent = |metadata: &crate::iceberg::spec::TableMetadata| {
            OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: crate::iceberg::TableIdent::new(
                        crate::iceberg::NamespaceIdent::new("db".into()),
                        "t".into(),
                    ),
                    uuid: Some(metadata.uuid()),
                },
                target_ref: "main".into(),
                start: None,
                changes: FileChanges::default(),
                dependencies: Vec::new(),
                protocol: crate::commit::model::CommitProtocol::Legacy,
                shape: RequestShape::MetadataOnly,
                summary: std::collections::BTreeMap::new(),
                token: fixture.operation.token(),
            })
            .unwrap()
        };
        let first = preview_managed_repartition(
            &base,
            &replacement(prior_observation(&base), identity_on_id()),
        )
        .unwrap();
        let operation = intent(&base);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base.clone(),
                metadata_location: "s3://b/base.metadata.json".into(),
            },
            &operation,
            &attempt,
        )
        .unwrap();
        engine.stage_change(first.prepared_change()).unwrap();
        let partitioned = engine.metadata().clone();
        let request = engine.freeze().unwrap();
        let json = request.to_rest_json().unwrap();
        let fields = json["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["action"] == "add-spec")
            .unwrap()["spec"]["fields"]
            .as_array()
            .unwrap()
            .clone();
        assert!(fields.iter().all(|f| f["field-id"].is_i64()));
        assert!(request.requirements().contains(
            &crate::iceberg::TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: base.last_partition_id(),
            }
        ));
        let middle = preview_managed_repartition(
            &partitioned,
            &replacement(
                prior_observation(&partitioned),
                vec![
                    ConnectorManagedPartitionField::try_new(
                        1,
                        0,
                        ConnectorManagedPartitionTransform::Bucket { buckets: 8 },
                    )
                    .unwrap(),
                ],
            ),
        )
        .unwrap();
        let operation = intent(&partitioned);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: partitioned.clone(),
                metadata_location: "s3://b/partitioned.metadata.json".into(),
            },
            &operation,
            &attempt,
        )
        .unwrap();
        engine.stage_change(middle.prepared_change()).unwrap();
        let bucketed = engine.metadata().clone();
        engine.freeze().unwrap();
        let back = preview_managed_repartition(
            &bucketed,
            &replacement(prior_observation(&bucketed), identity_on_id()),
        )
        .unwrap();
        assert_eq!(
            back.committed().spec_id(),
            partitioned.default_partition_spec_id()
        );
        let operation = intent(&bucketed);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: bucketed.clone(),
                metadata_location: "s3://b/partitioned.metadata.json".into(),
            },
            &operation,
            &attempt,
        )
        .unwrap();
        engine.stage_change(back.prepared_change()).unwrap();
        assert_eq!(
            engine.metadata().default_partition_spec_id(),
            partitioned.default_partition_spec_id()
        );
        let request = engine.freeze().unwrap();
        assert!(
            request
                .updates()
                .iter()
                .all(|u| !matches!(u, crate::iceberg::TableUpdate::AddSpec { .. }))
        );
        assert!(
            request
                .updates()
                .contains(&crate::iceberg::TableUpdate::SetDefaultSpec {
                    spec_id: partitioned.default_partition_spec_id(),
                })
        );
    }
}
