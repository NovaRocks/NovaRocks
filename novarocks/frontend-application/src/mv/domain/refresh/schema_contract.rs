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

//! Exact D/L schema validation; no legacy descriptor or schema-contract recovery.

use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::storage_observation::{
    MvSchemaValidationObservation, MvSourceSchemaObservation,
};
use novarocks_mv_application::persistence::codec::{InterpretationDocument, RelationOccurrence};
use novarocks_mv_application::persistence::identity::FieldIdentity;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::persistence::runtime_bindings::{
    MvRuntimeBindings, reconstruct_keyed_runtime_bindings,
};

/// A query-local rename, identified by D occurrence and opaque provider field.
/// The SQL owner must apply these facts with lexical occurrence scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvOccurrenceFieldRebind {
    pub occurrence_id: u32,
    pub field_id: FieldIdentity,
    pub qualifier_at_binding: String,
    pub name_at_binding: String,
    pub current_name: String,
}

pub(crate) fn validate_projection_target(
    projection: &StoredKeyedMvProjection,
    observation: &MvSchemaValidationObservation,
) -> Result<MvRuntimeBindings, String> {
    let target = projection.facts.target();
    let observed = observation.table();
    if target.catalog() != Some(observed.instance_id.as_str())
        || target.namespace() != observed.namespace.as_ref()
        || target.name() != observed.table.as_ref()
    {
        return Err(
            "MV target schema observation belongs to another requested locator".to_string(),
        );
    }
    reconstruct_keyed_runtime_bindings(&projection.facts, observation.exact_schema())
}

/// Schema version may advance after a rename or an unrelated added column.
/// Every bound field must still have the same exact identity, type and nullability.
pub(crate) fn validate_relation_occurrence_schema(
    occurrence: &RelationOccurrence,
    observation: &MvSourceSchemaObservation,
) -> Result<Vec<MvOccurrenceFieldRebind>, String> {
    // D records the source object inside the canonical exact-fact envelope;
    // the observation reports the provider's raw identity. Comparing the two
    // encodings directly would report every source as replaced.
    let names_same_object =
        novarocks_mv_application::persistence::exact_revision::persisted_object_names(
            &occurrence.object_id,
            observation.table_object_id(),
        )
        .map_err(|error| {
            format!(
                "restore MV source object identity for relation occurrence {}: {error}",
                occurrence.occurrence_id
            )
        })?;
    if occurrence.catalog_at_binding != observation.table().instance_id.as_str()
        || !names_same_object
    {
        return Err(format!(
            "MV source object changed for relation occurrence {}",
            occurrence.occurrence_id
        ));
    }
    let mut renames = Vec::new();
    for bound in &occurrence.fields {
        let current = observation
            .fields()
            .iter()
            .find(|field| field.field_id == bound.field_id)
            .ok_or_else(|| {
                format!(
                    "MV source field is missing for relation occurrence {}",
                    occurrence.occurrence_id
                )
            })?;
        if current.type_signature != bound.type_signature || current.nullable != bound.nullable {
            return Err(format!(
                "MV source field type or nullability changed for relation occurrence {}",
                occurrence.occurrence_id
            ));
        }
        if current.name != bound.name_at_binding {
            renames.push(MvOccurrenceFieldRebind {
                occurrence_id: occurrence.occurrence_id,
                field_id: bound.field_id.clone(),
                qualifier_at_binding: occurrence.qualifier_at_binding.clone(),
                name_at_binding: bound.name_at_binding.clone(),
                current_name: current.name.clone(),
            });
        }
    }
    Ok(renames)
}

/// Aggregate state is interpreted only by validated L, never by a schema-contract version.
pub fn validate_aggregate_schema_contract_metadata<'a>(
    target: &IcebergMvTarget,
    projection: &'a StoredKeyedMvProjection,
) -> Result<&'a InterpretationDocument, String> {
    let interpretation = projection.facts.interpretation();
    if interpretation.aggregates.is_empty() {
        return Err(format!(
            "Iceberg aggregate MV {}.{}.{} has no aggregate interpretation",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(interpretation)
}

pub(crate) fn validate_aggregate_schema_contract_for_base(
    projection: &StoredKeyedMvProjection,
    occurrence: &RelationOccurrence,
    base_observation: &MvSourceSchemaObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<Vec<MvOccurrenceFieldRebind>, String> {
    validate_projection_target(projection, target_observation)?;
    validate_relation_occurrence_schema(occurrence, base_observation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use bytes::Bytes;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableIdentity, ConnectorTableObjectId, MvObservedSourceField,
        MvSchemaValidationObservation as SpiObservation,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            16384,
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

    fn target_observation(
        projection: &StoredKeyedMvProjection,
        metadata_version: ConnectorCommittedVersion,
    ) -> MvSchemaValidationObservation {
        let facts = &projection.facts;
        let target = &facts.interpretation().target;
        let mut seen = std::collections::BTreeSet::new();
        let fields = target
            .fields
            .iter()
            .filter(|field| seen.insert(field.target_field_id.clone()))
            .enumerate()
            .map(|(ordinal, field)| {
                (
                    ordinal as u32,
                    MvObservedSourceField::try_new(
                        Bytes::copy_from_slice(field.target_field_id.as_bytes()),
                        format!("physical_{ordinal}"),
                        field.type_signature.clone(),
                        field.nullable,
                    )
                    .unwrap(),
                )
            })
            .collect();
        crate::mv::domain::storage_observation::schema_validation_from_spi(
            SpiObservation::try_new(
                facts.source_revision().target.clone(),
                facts.source_revision().target_object_id.clone(),
                metadata_version,
                Bytes::copy_from_slice(target.schema_version.as_bytes()),
                Bytes::copy_from_slice(target.partition_spec_version.as_bytes()),
                true,
                true,
                fields,
                vec![],
                &context(),
            )
            .unwrap(),
            &context(),
        )
        .unwrap()
    }

    fn source_observation(
        occurrence: &RelationOccurrence,
        object: &[u8],
        rename: bool,
    ) -> MvSourceSchemaObservation {
        let fields = occurrence
            .fields
            .iter()
            .map(|field| {
                MvObservedSourceField::try_new(
                    Bytes::copy_from_slice(field.field_id.as_bytes()),
                    if rename {
                        format!("new_{}", field.name_at_binding)
                    } else {
                        field.name_at_binding.clone()
                    },
                    field.type_signature.clone(),
                    field.nullable,
                )
                .unwrap()
            })
            .collect();
        crate::mv::domain::storage_observation::source_schema_from_spi(
            novarocks_spi::connector::MvSourceSchemaObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse(&occurrence.catalog_at_binding)
                        .unwrap(),
                    namespace: occurrence.namespace_at_binding.clone().into(),
                    table: occurrence.relation_at_binding.clone().into(),
                },
                ConnectorTableObjectId::try_new(Bytes::copy_from_slice(object)).unwrap(),
                Bytes::from_static(b"source-schema-after-rename"),
                true,
                true,
                fields,
                &context(),
            )
            .unwrap(),
            &context(),
        )
        .unwrap()
    }

    #[test]
    fn exact_target_observation_reconstructs_l_bindings_and_rejects_generation_drift() {
        let projection = projection();
        let observed = target_observation(&projection, projection.facts.metadata_version().clone());
        let bindings = validate_projection_target(&projection, &observed).unwrap();
        assert_eq!(bindings.outputs.len(), 1);
        assert_eq!(bindings.aggregates[0].states.len(), 2);
        assert_eq!(bindings.apply_key.len(), 1);
        assert_eq!(bindings.branches.len(), 2);
        let changed = target_observation(
            &projection,
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"later-metadata"), Some(11))
                .unwrap(),
        );
        assert!(
            validate_projection_target(&projection, &changed)
                .unwrap_err()
                .contains("exact document generation")
        );
    }

    #[test]
    fn repeated_objects_keep_distinct_occurrence_rename_facts() {
        let projection = projection();
        let occurrences = &projection.facts.definition().relation_occurrences;
        let object =
            novarocks_mv_application::persistence::exact_revision::restore_persisted_object(
                &occurrences[0].object_id,
            )
            .expect("restore the fixture source object");
        let observed = source_observation(&occurrences[0], object.as_bytes().as_ref(), true);
        let first = validate_relation_occurrence_schema(&occurrences[0], &observed).unwrap();
        let second = validate_relation_occurrence_schema(&occurrences[1], &observed).unwrap();
        assert!(!first.is_empty());
        assert_eq!(first[0].field_id, second[0].field_id);
        assert_ne!(first[0].occurrence_id, second[0].occurrence_id);
        assert_ne!(
            first[0].qualifier_at_binding,
            second[0].qualifier_at_binding
        );
        assert_eq!(
            first[0].current_name,
            format!("new_{}", first[0].name_at_binding)
        );
    }

    #[test]
    fn same_locator_cannot_rebind_a_replaced_source_object() {
        let projection = projection();
        let occurrence = &projection.facts.definition().relation_occurrences[0];
        let observed = source_observation(occurrence, b"replacement-object", false);
        assert!(
            validate_relation_occurrence_schema(occurrence, &observed)
                .unwrap_err()
                .contains("source object changed")
        );
    }
}
