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

//! CREATE-time source-field bindings for durable MV documents.
//!
//! SQL owns occurrence and field coordinates. The request-local catalog
//! binding owns the exact provider metadata that analysis admitted. This
//! module joins those two facts only after the provider projects opaque field
//! identities from that same metadata generation.

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_spi::connector::{
    ConnectorReadSelector, ConnectorRequestContext, MvSourceSchemaObservation,
    MvStorageObservationPort,
};
use novarocks_sql::planning::mv::{
    SqlMvCreatePersistenceFacts, SqlMvPersistenceRelationOccurrenceFacts,
};

use novarocks_mv_application::persistence::aggregate_bindings::{
    MvCreateRelationObservation, MvCreateSourceFieldObservation,
};

/// Freeze the exact provider source-field identities that a CREATE analysis
/// already used. Repeated SQL references to one relation produce distinct
/// occurrence records, even though they deliberately reuse one request-local
/// source metadata binding.
pub(crate) fn observe_mv_create_source_bindings_original(
    observer: &dyn MvStorageObservationPort,
    sql_facts: &SqlMvCreatePersistenceFacts,
    bindings: &QueryTableBindingStore,
    context: ConnectorRequestContext,
) -> Result<
    (
        Vec<MvCreateRelationObservation>,
        novarocks_mv_application::persistence::codec::keyed::BindingDocument,
    ),
    crate::mv::domain::application::MvApplicationError,
> {
    let mut formats = Vec::new();
    let observations = sql_facts.relation_occurrences().iter().map(|relation| {
        let binding = bindings.strict_base_binding(relation.catalog(), relation.namespace(), relation.relation())
            .ok_or_else(|| invalid(format!("CREATE source occurrence {} has no admitted connector binding", relation.occurrence_id().get())))?;
        let metadata = binding.source_metadata.as_ref().ok_or_else(|| invalid(format!(
            "CREATE source occurrence {} was not admitted from exact provider metadata", relation.occurrence_id().get())))?;
        let lease = binding.admission.exact_planning_lease().map_err(invalid)?;
        let observed = observer.observe_source_schema(&lease, metadata, context.clone())
            .map_err(|error| provider(error, format!("observe CREATE source fields for {}.{}.{}",
                relation.catalog(), relation.namespace(), relation.relation())))?;
        validate_same_generation(metadata, &observed).map_err(invalid)?;
        let revision = lease.binding().metadata().exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
            .map_err(|error| provider(error, format!("observe exact CREATE source revision for {}.{}.{}",
                relation.catalog(), relation.namespace(), relation.relation())))?;
        if revision.object_identity().value().as_ref() != observed.object_id().as_bytes().as_ref() {
            return Err(invalid("CREATE source field observation and exact revision identify different objects"));
        }
        formats.push(novarocks_mv_application::persistence::codec::keyed::DataVersionFormat::from_issued_fact(revision.data_version())
            .map_err(codec_failure)?);
        let (object_id, _data_version) = persist_exact_connector_revision(&revision)
            .map_err(|error| provider(error, "persist exact CREATE source revision"))?;
        bind_relation_fields(relation, &observed, bytes::Bytes::copy_from_slice(object_id.as_bytes()))
            .map_err(invalid)
    }).collect::<Result<Vec<_>, _>>()?;
    Ok((
        observations,
        novarocks_mv_application::persistence::codec::keyed::BindingDocument {
            data_versions: formats,
        },
    ))
}
fn invalid(message: impl Into<String>) -> crate::mv::domain::application::MvApplicationError {
    crate::mv::domain::application::MvApplicationError::new(
        crate::mv::domain::application::MvApplicationErrorKind::InvalidRequest,
        message,
    )
}
fn provider(
    error: novarocks_spi::connector::ConnectorError,
    context: impl std::fmt::Display,
) -> crate::mv::domain::application::MvApplicationError {
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    let message = format!("{context}: {error}");
    let kind = if error.kind() == novarocks_spi::connector::ConnectorErrorKind::Unavailable {
        MvApplicationErrorKind::Unavailable
    } else {
        MvApplicationErrorKind::Engine
    };
    let original = MvApplicationError::from_connector(kind, error);
    MvApplicationError::new(kind, message)
        .with_original_failure(original.original_failure().unwrap().clone())
}
fn codec_failure(
    error: novarocks_mv_application::persistence::codec::PersistenceCodecError,
) -> crate::mv::domain::application::MvApplicationError {
    let message = error.to_string();
    invalid(message)
        .with_original_failure(novarocks_failure_contract::SharedFailureCause::new(error))
}

fn bind_relation_fields(
    relation: &SqlMvPersistenceRelationOccurrenceFacts,
    observed: &MvSourceSchemaObservation,
    provider_object_id: bytes::Bytes,
) -> Result<MvCreateRelationObservation, String> {
    let mut source_ordinals = std::collections::BTreeSet::new();
    let fields = relation
        .referenced_fields()
        .iter()
        .map(|reference| {
            if !source_ordinals.insert(reference.field_ordinal()) {
                return Err(
                    "CREATE source observation has a duplicate SQL field coordinate".to_string(),
                );
            }
            let ordinal = usize::try_from(reference.field_ordinal()).map_err(|_| {
                "CREATE source field ordinal exceeds platform address space".to_string()
            })?;
            let field = observed.fields().get(ordinal).ok_or_else(|| {
                format!(
                    "CREATE source observation is missing SQL field ordinal {}",
                    reference.field_ordinal()
                )
            })?;
            if !field.name().eq_ignore_ascii_case(reference.name()) {
                return Err(format!(
                    "CREATE source field ordinal {} resolves to a different field name",
                    reference.field_ordinal()
                ));
            }
            Ok(MvCreateSourceFieldObservation {
                field_ordinal: reference.field_ordinal(),
                field_name: field.name().to_string(),
                provider_field_id: field.provider_field_id().clone(),
                type_signature: field.type_signature().to_string(),
                nullable: field.nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MvCreateRelationObservation {
        occurrence_id: relation.occurrence_id().get(),
        provider_object_id,
        provider_schema_version: observed.schema_version().clone(),
        fields,
    })
}

fn validate_same_generation(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    observed: &MvSourceSchemaObservation,
) -> Result<(), String> {
    if observed.table() != &metadata.identity {
        return Err("CREATE source observation table does not match admitted metadata".to_string());
    }
    if metadata.version.as_ref() != Some(observed.schema_version()) {
        return Err(
            "CREATE source observation schema version does not match admitted metadata".to_string(),
        );
    }
    // Connector metadata freezes the read schema, which on a row-lineage table
    // appends synthetic columns such as `_file`, `_pos` and `_row_id`. Those
    // are query inputs, not declared fields, and the provider's source
    // observation reports only the declared ones.
    let declared = declared_metadata_fields(metadata)?;
    if declared.len() != observed.fields().len() {
        return Err(format!(
            "CREATE source observation reports {} fields but admitted metadata declares {}",
            observed.fields().len(),
            declared.len()
        ));
    }
    for (metadata_field, observed_field) in declared.iter().zip(observed.fields()) {
        if !metadata_field
            .name()
            .eq_ignore_ascii_case(observed_field.name())
            || metadata_field.is_nullable() != observed_field.nullable()
        {
            return Err(
                "CREATE source observation schema facts do not match admitted metadata".to_string(),
            );
        }
    }
    Ok(())
}

/// The declared columns of an admitted source, in schema order.
fn declared_metadata_fields(
    metadata: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Result<Vec<arrow::datatypes::FieldRef>, String> {
    let column_facts = metadata.planning_facts.column_facts();
    if !column_facts.is_empty() && column_facts.len() != metadata.schema().fields().len() {
        return Err(format!(
            "CREATE source planning facts cover {} columns but its read schema has {}",
            column_facts.len(),
            metadata.schema().fields().len()
        ));
    }
    Ok(metadata
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| {
            !column_facts.get(*ordinal).is_some_and(|fact| {
                fact.role() == novarocks_spi::connector::ConnectorTableColumnRole::RowLineageSystem
            })
        })
        .map(|(_, field)| field.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::validate_same_generation;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
        ConnectorTableObjectId, MvObservedSourceField, MvSourceSchemaObservation,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            4096,
            16 * 1024,
        )
        .expect("context")
    }

    #[test]
    fn generation_validation_rejects_misaligned_source_schema() {
        let identity = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            namespace: Arc::from("db"),
            table: Arc::from("source"),
        };
        let metadata = crate::connector::schema_creation::declared_test_metadata(
            novarocks_spi::connector::ConnectorTableMetadataHeader {
                identity: identity.clone(),
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: Some(Bytes::from_static(b"schema-1")),
                statistics_data_version: None,
                table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                    identity.instance_id.clone(),
                    Bytes::from_static(b"provider-table"),
                )
                .expect("table handle"),
            },
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            )],
        );
        let observed = MvSourceSchemaObservation::try_new(
            identity,
            ConnectorTableObjectId::try_new(Bytes::from_static(b"object-1")).expect("object"),
            Bytes::from_static(b"schema-2"),
            true,
            true,
            vec![
                MvObservedSourceField::try_new(
                    Bytes::from_static(b"field-1"),
                    "id".to_string(),
                    "long".to_string(),
                    false,
                )
                .expect("field"),
            ],
            &context(),
        )
        .expect("observation");
        assert!(
            validate_same_generation(&metadata, &observed)
                .expect_err("schema version mismatch")
                .contains("schema version")
        );
    }
}

#[cfg(test)]
mod original_codec_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn actual_binding_document_decode_failure_keeps_its_codec_source_through_create() {
        let error = novarocks_mv_application::persistence::codec::keyed::decode_binding(
            &[0xff],
            novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
        )
        .unwrap_err();
        assert!(matches!(&error, novarocks_mv_application::persistence::codec::PersistenceCodecError::ProtobufDecode(_) | novarocks_mv_application::persistence::codec::PersistenceCodecError::MalformedWire(_)));
        let expected = error.failure_fact();
        let application = codec_failure(error);
        let cause = application.original_failure().unwrap().clone();
        assert!(matches!(cause.downcast_ref::<novarocks_mv_application::persistence::codec::PersistenceCodecError>(),
            Some(novarocks_mv_application::persistence::codec::PersistenceCodecError::ProtobufDecode(_) | novarocks_mv_application::persistence::codec::PersistenceCodecError::MalformedWire(_))));
        let selected = crate::mv::command::project_product_failure_for_test(application);
        assert_eq!(selected.failure_cause(), Some(&cause));
        assert_eq!(cause.failure_fact(), expected);
        assert_eq!(expected.class(), FailureClass::Data);
        assert!(selected.compile_control_error().is_none());
    }
}
