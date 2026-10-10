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
//! Nothing here carries a write-operation identity. The replacement is bound to
//! the session that prepares it by the expected-prior-default observation
//! check, which is an optimistic-concurrency fence against the live table.

use std::collections::HashMap;

use novarocks_spi::connector::{
    ConnectorCommittedPartitionField, ConnectorCommittedPartitioning, ConnectorError,
    ConnectorErrorKind, ConnectorManagedDescriptorProperties, ConnectorManagedPartitionField,
    ConnectorManagedPartitionSpecObservation, ConnectorManagedPartitionSpecReplacement,
    ConnectorManagedPartitionTransform,
};

use crate::commit::write_stack::control::ICEBERG_WRITE_SESSION_MARKER_PROPERTY;
use crate::commit::write_stack::domain::{corrupt, invalid};

/// One prepared atomic replacement: declarative updates for common staging,
/// exact prospective metadata for writer binding, and committed partitioning.
#[derive(Clone, Debug)]
pub(crate) struct IcebergPreparedRepartition {
    metadata_updates: Vec<crate::iceberg::TableUpdate>,
    prospective_metadata: crate::iceberg::spec::TableMetadata,
    metadata_requirements: Vec<crate::iceberg::TableRequirement>,
    committed: novarocks_spi::connector::ConnectorCommittedPartitioning,
}

impl IcebergPreparedRepartition {
    pub(crate) fn metadata_updates(&self) -> &[crate::iceberg::TableUpdate] {
        &self.metadata_updates
    }

    pub(crate) fn metadata_requirements(&self) -> &[crate::iceberg::TableRequirement] {
        &self.metadata_requirements
    }

    pub(crate) fn prepared_change(&self) -> crate::commit::staging::PreparedChange {
        crate::commit::staging::PreparedChange {
            updates: self.metadata_updates.clone(),
            requirements: self.metadata_requirements.clone(),
        }
    }

    pub(crate) fn prospective_metadata(&self) -> &crate::iceberg::spec::TableMetadata {
        &self.prospective_metadata
    }

    pub(crate) fn committed(&self) -> &ConnectorCommittedPartitioning {
        &self.committed
    }
}

/// Prepare the full atomic replacement: the partition transition followed by
/// the managed descriptor properties, so `metadata_updates` is exactly
/// `[AddSpec, SetDefaultSpec, SetProperties]`. That shape is what the single
/// external commit requires ahead of its snapshot updates.
pub(crate) fn prepare_managed_repartition(
    metadata: &crate::iceberg::spec::TableMetadata,
    replacement: &ConnectorManagedPartitionSpecReplacement,
    descriptor_properties: &ConnectorManagedDescriptorProperties,
) -> Result<IcebergPreparedRepartition, ConnectorError> {
    let mut prepared = preview_managed_repartition(metadata, replacement)?;
    let descriptor_properties = managed_descriptor_property_updates(descriptor_properties)?;
    let build = crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(
        prepared.prospective_metadata.clone(),
        None,
    )
    .set_properties(descriptor_properties.clone())
    .map_err(|error| invalid(format!("bind managed MV descriptor properties: {error}")))?
    .build()
    .map_err(|error| {
        invalid(format!(
            "finalize managed MV descriptor properties: {error}"
        ))
    })?;
    prepared.prospective_metadata = build.metadata;
    prepared
        .metadata_updates
        .push(crate::iceberg::TableUpdate::SetProperties {
            updates: descriptor_properties,
        });
    Ok(prepared)
}

/// Prepare the partition transition alone, without the descriptor properties.
///
/// This is the read-only preview: it answers "which physical partitioning would
/// this replacement establish" without proposing the application's descriptor
/// change, so its `metadata_updates` stop at `[AddSpec, SetDefaultSpec]`.
pub(crate) fn preview_managed_repartition(
    metadata: &crate::iceberg::spec::TableMetadata,
    replacement: &ConnectorManagedPartitionSpecReplacement,
) -> Result<IcebergPreparedRepartition, ConnectorError> {
    // The write session has no write-operation identity to compare the
    // replacement against, so there is no id equality check here. The
    // replacement is bound to this session by the expected-prior-default
    // observation check immediately below: it is a real optimistic-concurrency
    // fence against the live default spec, not a weaker substitute for an id.
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
    let build =
        crate::iceberg::spec::TableMetadataBuilder::new_from_metadata(metadata.clone(), None)
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
    let metadata_updates = vec![
        crate::iceberg::TableUpdate::AddSpec {
            spec: requested_spec,
        },
        crate::iceberg::TableUpdate::SetDefaultSpec { spec_id: -1 },
    ];
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
    Ok(IcebergPreparedRepartition {
        prospective_metadata: build.metadata,
        metadata_updates,
        metadata_requirements: vec![
            crate::iceberg::TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
            crate::iceberg::TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: metadata.last_partition_id(),
            },
        ],
        committed,
    })
}

/// Turn the opaque application descriptor carrier into the property map the
/// `SetProperties` update carries. The provider owns publication metadata, so a
/// descriptor key that collides with one of those keys fails closed rather than
/// silently overwriting the provider's own record of the publication.
fn managed_descriptor_property_updates(
    descriptor: &ConnectorManagedDescriptorProperties,
) -> Result<HashMap<String, String>, ConnectorError> {
    let mut updates = HashMap::with_capacity(descriptor.entries().len());
    for (key, value) in descriptor.entries() {
        if matches!(
            key.as_ref(),
            crate::commit::MV_PUBLICATION_ID_PROP
                | crate::commit::MV_PUBLICATION_PROVENANCE_PROP
                | crate::commit::MV_REFRESH_ROW_COUNT_PROP
                | ICEBERG_WRITE_SESSION_MARKER_PROPERTY
        ) {
            return Err(invalid(
                "managed MV descriptor properties conflict with provider-owned publication metadata",
            ));
        }
        if updates.insert(key.to_string(), value.to_string()).is_some() {
            return Err(invalid(
                "managed MV descriptor properties contain a duplicate key",
            ));
        }
    }
    Ok(updates)
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
        ConnectorErrorKind, ConnectorManagedDescriptorProperties, ConnectorManagedPartitionField,
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

    fn descriptor_properties() -> ConnectorManagedDescriptorProperties {
        ConnectorManagedDescriptorProperties::try_new(vec![
            (
                Arc::from("novarocks.mv.descriptor.hash"),
                Arc::from("descriptor-hash"),
            ),
            (
                Arc::from("novarocks.mv.descriptor.inline"),
                Arc::from("descriptor-inline"),
            ),
            (
                Arc::from("novarocks.mv.descriptor.package-id"),
                Arc::from("db.mv"),
            ),
        ])
        .expect("descriptor properties")
    }

    #[test]
    fn a_prepared_repartition_is_add_spec_then_set_default_spec_then_set_properties() {
        let metadata = unpartitioned_metadata();
        let replacement = replacement(prior_observation(&metadata), identity_on_id());
        let descriptor = descriptor_properties();

        let prepared = prepare_managed_repartition(&metadata, &replacement, &descriptor)
            .expect("prepare the repartition");

        assert_eq!(prepared.metadata_updates().len(), 3);
        assert!(matches!(
            prepared.metadata_updates()[1],
            crate::iceberg::TableUpdate::SetDefaultSpec { .. }
        ));
        assert!(matches!(
            prepared.metadata_updates()[2],
            crate::iceberg::TableUpdate::SetProperties { .. }
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
        for (key, value) in descriptor.entries() {
            assert_eq!(
                prepared
                    .prospective_metadata()
                    .properties()
                    .get(key.as_ref())
                    .map(String::as_str),
                Some(value.as_ref())
            );
        }
    }

    #[test]
    fn a_preview_stops_before_the_descriptor_properties() {
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

        let error = prepare_managed_repartition(&metadata, &replacement, &descriptor_properties())
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

        let error = prepare_managed_repartition(&metadata, &replacement, &descriptor_properties())
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
    fn managed_descriptor_properties_reject_provider_owned_keys() {
        for reserved in [
            crate::commit::MV_PUBLICATION_ID_PROP,
            crate::commit::MV_PUBLICATION_PROVENANCE_PROP,
            crate::commit::MV_REFRESH_ROW_COUNT_PROP,
            ICEBERG_WRITE_SESSION_MARKER_PROPERTY,
        ] {
            let descriptor = ConnectorManagedDescriptorProperties::try_new(vec![(
                Arc::from(reserved),
                Arc::from("01890f3c-4e70-7cc0-8000-000000000012"),
            )])
            .expect("opaque descriptor carrier");

            let error = managed_descriptor_property_updates(&descriptor)
                .expect_err("provider-owned descriptor key must fail closed");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn a_reserved_descriptor_key_refuses_the_whole_repartition() {
        let metadata = unpartitioned_metadata();
        let replacement = replacement(prior_observation(&metadata), identity_on_id());
        let descriptor = ConnectorManagedDescriptorProperties::try_new(vec![(
            Arc::from(crate::commit::MV_PUBLICATION_ID_PROP),
            Arc::from("01890f3c-4e70-7cc0-8000-000000000012"),
        )])
        .expect("opaque descriptor carrier");

        let error = prepare_managed_repartition(&metadata, &replacement, &descriptor)
            .expect_err("a provider-owned descriptor key must fail the whole preparation");
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
    async fn repartition_staging_binds_new_ids_and_reuses_an_exact_historical_spec() {
        use crate::commit::model::{
            FileChanges, IsolationLevel, OperationIntent, OperationIntentParts, RequestShape,
            TableTarget,
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
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::MetadataOnly,
                summary: std::collections::BTreeMap::new(),
                token: fixture.operation.token(),
            })
            .unwrap()
        };
        let first = prepare_managed_repartition(
            &base,
            &replacement(prior_observation(&base), identity_on_id()),
            &descriptor_properties(),
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
        let request = engine.freeze(&[]).unwrap();
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
        engine.freeze(&[]).unwrap();
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
        let request = engine.freeze(&[]).unwrap();
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
