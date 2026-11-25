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

use crate::mv::domain::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
use crate::mv::domain::persistence::source_bindings::MvDirectOutputSources;
use crate::mv::domain::storage_observation::MvSourceSchemaObservation;
use novarocks_mv_application::persistence::codec::TargetPartitionTransform;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::persistence::schema::{
    MvPartitionContract, MvPartitionTransformContract,
};
use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionTransform, ConnectorChangePartitionValue,
};

use crate::mv::domain::model::AffectedTargetPartitions;
use novarocks_spi::connector::read_stack::ConnectorPreparedSourceRead;
use novarocks_spi::connector::{
    ConnectorChangeWindowFacts, ConnectorChangeWindowPartitionImpact, ConnectorControlPlanningPin,
    ConnectorError, ConnectorErrorKind,
};

#[derive(Debug)]
enum PartitionMappingFailure {
    Connector(ConnectorError),
    NotDerived(&'static str),
}
impl From<&str> for PartitionMappingFailure {
    fn from(message: &str) -> Self {
        Self::Connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            message,
        ))
    }
}
impl From<String> for PartitionMappingFailure {
    fn from(message: String) -> Self {
        Self::Connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            message,
        ))
    }
}
impl std::fmt::Display for PartitionMappingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connector(error) => std::fmt::Display::fmt(error, f),
            Self::NotDerived(message) => f.write_str(message),
        }
    }
}

/// The only production impact entry: a copied admission cannot claim a source/spec author.
pub(crate) fn plan_prepared_source_partitions(
    projection: &StoredKeyedMvProjection,
    source_occurrence_id: u32,
    source: &MvSourceSchemaObservation,
    planning: &ConnectorControlPlanningPin,
    prepared: &ConnectorPreparedSourceRead,
    target_partition: &MvPartitionContract,
    direct: Option<&MvDirectOutputSources>,
) -> Result<AffectedTargetPartitions, ConnectorError> {
    prepared.request().observed_source_facts().validate_owner(
        &novarocks_spi::connector::ConnectorProviderBindingKey {
            instance_id: planning.binding().descriptor().instance_id.clone(),
            incarnation: planning.binding().incarnation(),
        },
    )?;
    if prepared.binding().descriptor() != planning.binding().descriptor()
        || prepared.binding().catalog_handle() != planning.binding().catalog_handle()?
        || prepared.request().object_id() != source.table_object_id()
        || &prepared.to_metadata().identity != source.table()
        || prepared.to_metadata().version.as_ref() != Some(source.schema_version())
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "partition impact differs from its original prepared source generation",
        ));
    }
    let occurrence = projection
        .facts
        .binding()
        .occurrences
        .iter()
        .find(|value| value.occurrence_id == source_occurrence_id)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "partition impact names no D occurrence",
            )
        })?;
    if occurrence.catalog_at_binding != source.table().instance_id.as_str()
        || !novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            &occurrence.object_id,
            source.table_object_id(),
        )?
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "partition impact does not belong to the original D source occurrence",
        ));
    }
    match prepared.window_facts() {
        Some(ConnectorChangeWindowFacts::NetZero(_)) => {
            Ok(AffectedTargetPartitions::known(std::iter::empty()))
        }
        Some(ConnectorChangeWindowFacts::NotIncremental(_)) => {
            Ok(AffectedTargetPartitions::not_derived(
                "connector change window requires a full rebuild",
            ))
        }
        Some(ConnectorChangeWindowFacts::Difference { .. })
            if target_partition.fields.is_empty() =>
        {
            Ok(AffectedTargetPartitions::Unpartitioned)
        }
        Some(ConnectorChangeWindowFacts::Difference {
            partition_impact, ..
        }) => derive_prepared_partition_impact(
            projection,
            source_occurrence_id,
            source,
            target_partition,
            partition_impact,
            direct,
        ),
        None => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "snapshot preparation cannot authorize a change-window partition impact",
        )),
    }
}

/// Pure derivation kernel; its production caller borrows impact only from the validated receipt above.
fn derive_prepared_partition_impact(
    projection: &StoredKeyedMvProjection,
    source_occurrence_id: u32,
    source: &MvSourceSchemaObservation,
    target_partition: &MvPartitionContract,
    impact: &ConnectorChangeWindowPartitionImpact,
    direct: Option<&MvDirectOutputSources>,
) -> Result<AffectedTargetPartitions, ConnectorError> {
    Ok(match impact {
        ConnectorChangeWindowPartitionImpact::Unavailable => AffectedTargetPartitions::not_derived(
            "connector change-window partition impact is unavailable",
        ),
        ConnectorChangeWindowPartitionImpact::Unpartitioned => {
            if target_partition.fields.is_empty() {
                AffectedTargetPartitions::Unpartitioned
            } else {
                AffectedTargetPartitions::not_derived(
                    "unpartitioned source changes cannot identify target partitions",
                )
            }
        }
        ConnectorChangeWindowPartitionImpact::Exact(partitions) => {
            if partitions.has_row_deletes() {
                return Ok(AffectedTargetPartitions::not_derived(
                    "row-level delete affected partitions require row-evaluation fallback",
                ));
            }
            let added = partitions.added();
            let removed = partitions.removed();
            let mut partitions = Vec::with_capacity(added.len() + removed.len());
            for partition in added.iter().chain(removed) {
                match map_partition_fields(
                    projection,
                    source_occurrence_id,
                    source,
                    target_partition,
                    partition,
                    direct,
                ) {
                    Ok(Some(key)) => partitions.push(key),
                    Ok(None) => return Ok(AffectedTargetPartitions::Unpartitioned),
                    Err(PartitionMappingFailure::NotDerived(reason)) => {
                        return Ok(AffectedTargetPartitions::not_derived(reason));
                    }
                    Err(PartitionMappingFailure::Connector(error)) => return Err(error),
                }
            }
            AffectedTargetPartitions::known(partitions)
        }
    })
}

/// Derive one target key only when D's output lineage and L's exact partition
/// binding prove that each target field is the same source field and transform
/// as the provider's per-file impact. Any weaker relation requires a full scan.
fn map_partition_fields(
    projection: &StoredKeyedMvProjection,
    source_occurrence_id: u32,
    source_observation: &MvSourceSchemaObservation,
    target_partition: &MvPartitionContract,
    connector_partition: &ConnectorChangePartition,
    direct: Option<&MvDirectOutputSources>,
) -> Result<Option<MvPartitionKey>, PartitionMappingFailure> {
    let occurrence = projection
        .facts
        .binding()
        .occurrences
        .iter()
        .find(|occurrence| occurrence.occurrence_id == source_occurrence_id)
        .ok_or("connector partition impact names no D source occurrence")?;
    if occurrence.catalog_at_binding != source_observation.table().instance_id.as_str()
        || !novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            &occurrence.object_id,
            source_observation.table_object_id(),
        )
        .map_err(PartitionMappingFailure::Connector)?
    {
        return Err(
            "connector partition impact does not belong to an exact D source occurrence".into(),
        );
    }
    for field in connector_partition.fields() {
        if !source_observation
            .fields()
            .iter()
            .any(|observed| observed.field_id.as_bytes() == field.source_field_identity().as_ref())
        {
            return Err(format!(
                "connector partition impact references source field {} outside the exact schema observation",
                field.source_column()
            ).into());
        }
    }
    let interpretation = projection.facts.interpretation();
    let canonical = &interpretation.target.partition_fields;
    if canonical.len() != target_partition.fields.len() || canonical.is_empty() {
        return Err("target partition observation disagrees with canonical L".into());
    }
    let mut fields = Vec::with_capacity(canonical.len());
    for (binding, observed) in canonical.iter().zip(&target_partition.fields) {
        if !target_transform_matches(&binding.transform, &observed.transform) {
            return Err("target partition transform disagrees with canonical L".into());
        }
        let output_binding = interpretation
            .outputs
            .iter()
            .find(|output| output.target_field_id == binding.source_target_field_id)
            .ok_or("target partition source has no output binding in canonical L")?;
        let source = direct
            .and_then(|sources| sources.get(&output_binding.output_id))
            .filter(|source| source.occurrence_id == source_occurrence_id)
            .ok_or(PartitionMappingFailure::NotDerived(
                "target partition output has no analyzed direct source field",
            ))?;
        let source_id = &source.field_id;
        let source_field = source_observation
            .fields()
            .iter()
            .find(|field| field.field_id == *source_id)
            .ok_or("direct target partition source is absent from the exact observation")?;
        if source_field.source_type != output_binding.source_type {
            return Err("direct target partition source changes its value type".into());
        }
        let impact = connector_partition
            .fields()
            .iter()
            .find(|field| {
                field.source_field_identity().as_ref() == source_id.as_bytes()
                    && impact_transform_matches(&binding.transform, field.transform())
            })
            .ok_or(PartitionMappingFailure::NotDerived(
                "source partition impact cannot derive the target transform",
            ))?;
        fields.push(MvPartitionKeyField::new(
            binding.partition_field_id.clone(),
            match impact.value() {
                ConnectorChangePartitionValue::Null => MvPartitionValue::Null,
                ConnectorChangePartitionValue::String(value) => {
                    MvPartitionValue::String(value.to_string())
                }
            },
        ));
    }
    Ok(Some(MvPartitionKey::new(
        interpretation.target.partition_spec_version.clone(),
        fields,
    )))
}

fn target_transform_matches(
    binding: &TargetPartitionTransform,
    observed: &MvPartitionTransformContract,
) -> bool {
    match (binding, observed) {
        (TargetPartitionTransform::Identity, MvPartitionTransformContract::Identity)
        | (TargetPartitionTransform::Year, MvPartitionTransformContract::Year)
        | (TargetPartitionTransform::Month, MvPartitionTransformContract::Month)
        | (TargetPartitionTransform::Day, MvPartitionTransformContract::Day)
        | (TargetPartitionTransform::Hour, MvPartitionTransformContract::Hour)
        | (TargetPartitionTransform::Void, MvPartitionTransformContract::Void) => true,
        (
            TargetPartitionTransform::Bucket { num_buckets: left },
            MvPartitionTransformContract::Bucket { num_buckets: right },
        ) => left == right,
        (
            TargetPartitionTransform::Truncate { width: left },
            MvPartitionTransformContract::Truncate { width: right },
        ) => left == right,
        _ => false,
    }
}

fn impact_transform_matches(
    binding: &TargetPartitionTransform,
    impact: ConnectorChangePartitionTransform,
) -> bool {
    match (binding, impact) {
        (TargetPartitionTransform::Identity, ConnectorChangePartitionTransform::Identity)
        | (TargetPartitionTransform::Year, ConnectorChangePartitionTransform::Year)
        | (TargetPartitionTransform::Month, ConnectorChangePartitionTransform::Month)
        | (TargetPartitionTransform::Day, ConnectorChangePartitionTransform::Day)
        | (TargetPartitionTransform::Hour, ConnectorChangePartitionTransform::Hour) => true,
        (
            TargetPartitionTransform::Bucket { num_buckets },
            ConnectorChangePartitionTransform::Bucket { buckets },
        ) => *num_buckets == buckets.get(),
        (
            TargetPartitionTransform::Truncate { width },
            ConnectorChangePartitionTransform::Truncate {
                width: impact_width,
            },
        ) => *width == impact_width.get(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::codec::{
        ApplyKeyKind, PhysicalFieldLogicalIdentity, SourceFieldReference,
        TargetPartitionFieldBinding,
    };
    use novarocks_mv_application::persistence::identity::FieldIdentity;
    use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
    use novarocks_mv_application::persistence::schema::MvPartitionFieldContract;
    use novarocks_mv_application::persistence::test_support::{fixture_mapping, fixture_type};
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
        ConnectorChangePartitionValue, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableObjectId, MvObservedSourceField,
    };
    use std::sync::Arc;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            16 * 1024,
        )
        .unwrap()
    }

    fn projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .unwrap(),
        }
    }

    fn observation(object: &'static [u8]) -> MvSourceSchemaObservation {
        let context = context();
        crate::mv::domain::storage_observation::source_schema_from_spi(
            novarocks_spi::connector::MvSourceSchemaObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                    namespace: Arc::from("sales"),
                    table: Arc::from("orders"),
                },
                ConnectorTableObjectId::try_new(Bytes::from_static(object)).unwrap(),
                Bytes::from_static(&[1]),
                true,
                Some(1),
                true,
                vec![
                    MvObservedSourceField::try_new(
                        Bytes::from_static(&[1]),
                        "order_id".to_string(),
                        fixture_type("bigint"),
                        false,
                    )
                    .unwrap(),
                ],
                &context,
            )
            .unwrap(),
            &context,
        )
        .unwrap()
    }

    fn partition() -> ConnectorChangePartition {
        ConnectorChangePartition::try_new(
            Bytes::from_static(&[4]),
            vec![
                ConnectorChangePartitionField::try_new(
                    Bytes::from_static(&[1]),
                    "order_id",
                    ConnectorChangePartitionTransform::Identity,
                    ConnectorChangePartitionValue::String("42".into()),
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    fn partitioned_projection() -> StoredKeyedMvProjection {
        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), None);
        fixture.binding.occurrences.truncate(1);
        let output = &mut fixture.binding.outputs[0];
        output.name = "order_id".into();
        output.source_fields = vec![SourceFieldReference {
            occurrence_id: 7,
            field_id: FieldIdentity::try_new(vec![1]).unwrap(),
        }];
        fixture.interpretation.outputs[0].source_type = fixture_type("bigint");
        fixture.interpretation.outputs[0].nullable = false;
        fixture.interpretation.aggregates.clear();
        fixture.interpretation.state_slots.clear();
        fixture.interpretation.branches.clear();
        fixture.interpretation.apply_key.kind = ApplyKeyKind::BaseRowId;
        fixture.interpretation.target.fields.retain(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::Output(_) | PhysicalFieldLogicalIdentity::ApplyKey(_)
            )
        });
        fixture.interpretation.target.fields[0].source_type = fixture_type("bigint");
        fixture.interpretation.target.fields[0].nullable = false;
        fixture.interpretation.target.partition_fields = vec![TargetPartitionFieldBinding {
            partition_field_id: FieldIdentity::try_new(vec![90]).unwrap(),
            source_target_field_id: fixture.interpretation.outputs[0].target_field_id.clone(),
            transform: TargetPartitionTransform::Identity,
        }];
        fixture.mapping = fixture_mapping(&fixture.binding, &fixture.interpretation);
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().unwrap(),
        }
    }

    fn target_partition() -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: 4,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 1000,
                partition_field_name: "order_id".into(),
                source_target_field_id: 31,
                source_column_name: "order_id".into(),
                transform: MvPartitionTransformContract::Identity,
            }],
        }
    }

    fn direct_source(projection: &StoredKeyedMvProjection) -> MvDirectOutputSources {
        let output = &projection.facts.binding().outputs[0];
        [(output.identity(), output.source_fields[0].clone())].into()
    }

    #[test]
    fn exact_partition_impact_derives_direct_bound_target_key() {
        let projection = partitioned_projection();
        assert_eq!(
            map_partition_fields(
                &projection,
                7,
                &observation(&[11]),
                &target_partition(),
                &partition(),
                Some(&direct_source(&projection)),
            )
            .unwrap(),
            Some(MvPartitionKey::new(
                projection
                    .facts
                    .interpretation()
                    .target
                    .partition_spec_version
                    .clone(),
                vec![MvPartitionKeyField::new(
                    FieldIdentity::try_new(vec![90]).unwrap(),
                    MvPartitionValue::String("42".into()),
                )],
            ))
        );

        let changed_spec = ConnectorChangePartition::try_new(
            Bytes::from_static(&[5]),
            partition().fields().to_vec(),
        )
        .unwrap();
        let original = map_partition_fields(
            &projection,
            7,
            &observation(&[11]),
            &target_partition(),
            &partition(),
            Some(&direct_source(&projection)),
        )
        .unwrap();
        let changed = map_partition_fields(
            &projection,
            7,
            &observation(&[11]),
            &target_partition(),
            &changed_spec,
            Some(&direct_source(&projection)),
        )
        .unwrap();
        assert_eq!(
            changed, original,
            "field derivation must not compare an opaque prepared spec token with Current default spec"
        );
    }

    #[test]
    fn exact_partition_impact_never_reconstructs_a_retired_numeric_mapping() {
        let projection = projection();
        let target_partition = MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        };
        let error = map_partition_fields(
            &projection,
            7,
            &observation(&[11]),
            &target_partition,
            &partition(),
            Some(&direct_source(&projection)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("canonical L"), "{error}");

        let error = map_partition_fields(
            &projection,
            7,
            &observation(&[12]),
            &target_partition,
            &partition(),
            Some(&direct_source(&projection)),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("exact D source occurrence"),
            "{error}"
        );
    }
    #[test]
    fn prepared_unpartitioned_impact_is_preserved_without_decoding_spec_identity() {
        let projection = projection();
        let target = MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        };
        assert_eq!(
            derive_prepared_partition_impact(
                &projection,
                7,
                &observation(&[11]),
                &target,
                &ConnectorChangeWindowPartitionImpact::Unpartitioned,
                None,
            )
            .unwrap(),
            AffectedTargetPartitions::Unpartitioned
        );
    }

    #[test]
    fn prepared_row_deletes_remain_non_derivable() {
        let projection = projection();
        let target = MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        };
        let impact = novarocks_spi::connector::ConnectorChangeWindowPartitionImpactBuilder::new(
            true,
            &context(),
        )
        .finish();
        let result = derive_prepared_partition_impact(
            &projection,
            7,
            &observation(&[11]),
            &target,
            &impact,
            None,
        )
        .unwrap();
        assert_eq!(
            result.not_derived_reason(),
            Some("row-level delete affected partitions require row-evaluation fallback")
        );
    }
}
