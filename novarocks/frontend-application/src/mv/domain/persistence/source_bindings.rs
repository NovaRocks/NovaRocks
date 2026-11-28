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
use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::mv::domain::storage_observation as storage;
use bytes::Bytes;
use novarocks_mv_application::persistence::{
    aggregate_bindings::{MvCreateRelationObservation, MvCreateSourceFieldObservation},
    codec::{
        BindingDependency, BindingDocument, BindingOccurrence, BoundSourceNode, BoundSourcePath,
        QueryDialect, ReadSelection, SourceBindingFacts, SourceNodeKind, keyed::DataVersionFormat,
    },
    create_documents::build_binding_outputs,
    exact_revision::persist_exact_connector_revision,
    identity::{FieldIdentity, ObjectIdentity},
};
use novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection;
use novarocks_spi::connector::{
    ConnectorExactSemanticRevision, ConnectorReadSelector, ConnectorRequestContext,
    MvSourceFieldCoordinate, MvSourceFieldKind, MvSourceFieldPath, MvSourceObjectKind,
    MvSourceSchemaObservation, MvStorageObservationPort,
};
use novarocks_sql::planning::mv::{
    SqlMvCreatePersistenceFacts, SqlMvPersistenceRelationOccurrenceFacts, SqlMvReadSelection,
};
use novarocks_type_contract::CanonicalFactWriter;
use std::collections::BTreeMap;

pub(crate) type MvDirectOutputSources = BTreeMap<
    novarocks_mv_application::persistence::identity::OutputIdentity,
    novarocks_mv_application::persistence::codec::SourceFieldReference,
>;

pub(crate) struct MvObservedBinding {
    pub(crate) relations: Vec<MvCreateRelationObservation>,
    pub(crate) document: BindingDocument,
    pub(crate) revisions: BTreeMap<u32, ConnectorExactSemanticRevision>,
    pub(crate) direct_output_sources: MvDirectOutputSources,
    pub(crate) replacement_output_sources: Option<BTreeMap<u32, MvDirectOutputSources>>,
    pub(crate) schemas: BTreeMap<String, storage::MvSourceSchemaObservation>,
}

/// Resolve each occurrence through the original SQL binding ID. A repeated
/// object may share immutable metadata while preserving the address of each scan.
pub(crate) fn view_binding_dependency(
    view: &novarocks_sql::planning::catalog::SqlMvViewDependency,
) -> BindingDependency {
    let mut identity = CanonicalFactWriter::new("novarocks.mv.observed-view", 1);
    identity.text(&view.identity().catalog);
    if let Some((uuid, _)) = view.iceberg_version() {
        identity.bytes(&uuid);
    } else {
        identity.text(&view.identity().namespace);
        identity.text(&view.identity().table);
    }
    BindingDependency::View {
        identity: identity.finish().into(),
        depth: view.depth(),
        exact_declaration: Bytes::copy_from_slice(view.version().as_bytes()),
        iceberg_version: view.iceberg_version(),
        dialect: QueryDialect::Trino,
        canonical: Bytes::copy_from_slice(view.canonical_bytes()),
    }
}

pub(crate) fn observe_mv_binding(
    observer: &dyn MvStorageObservationPort,
    sql_facts: &SqlMvCreatePersistenceFacts,
    bindings: &QueryTableBindingStore,
    upstream: &[LoadedKeyedMvProjection],
    context: ConnectorRequestContext,
) -> Result<MvObservedBinding, MvApplicationError> {
    let mut dependencies = sql_facts
        .view_dependencies()
        .iter()
        .map(view_binding_dependency)
        .collect::<Vec<_>>();
    let mut relations = Vec::new();
    let mut occurrences = Vec::new();
    let mut revisions = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    for relation in sql_facts.relation_occurrences() {
        let occurrence_id = relation.occurrence_id().get();
        if relation.read_selection() != SqlMvReadSelection::Current {
            return Err(MvApplicationError::from_compile(
                novarocks_sql::compiler::SqlCompileError::Analyze(
                    novarocks_sql::analyze_error::AnalyzeError::unsupported_query_shape(
                        "Materialized view source scans must read Current",
                        novarocks_parser::Span::new(0, 0),
                    ),
                ),
            ));
        }
        let binding = bindings.binding(relation.binding()).map_err(invalid)?;
        let metadata = binding.source_metadata.as_ref().ok_or_else(|| {
            invalid(format!(
                "MV source occurrence {occurrence_id} has no original provider metadata"
            ))
        })?;
        let lease = binding.admission.exact_planning_pin().map_err(invalid)?;
        let request = relation
            .field_paths()
            .iter()
            .map(|path| MvSourceFieldPath {
                fields: path
                    .fields()
                    .iter()
                    .map(|field| MvSourceFieldCoordinate {
                        ordinal: field.ordinal(),
                        name: field.name().to_owned(),
                    })
                    .collect(),
                whole_subtree: path.whole_subtree(),
            })
            .collect::<Vec<_>>();
        let fact = observer
            .observe_source_binding(&lease, metadata, &request, context.clone())
            .map_err(|error| provider(error, "observe exact MV source binding"))?;
        let schema = observer
            .observe_source_schema(&lease, metadata, context.clone())
            .map_err(|error| provider(error, "observe exact MV source schema"))?;
        validate_same_generation(metadata, &schema).map_err(invalid)?;
        let revision = lease
            .binding()
            .metadata()
            .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
            .map_err(|error| provider(error, "freeze MV source data version"))?;
        if fact.catalog_instance != metadata.identity.instance_id
            || fact.object_id != *schema.object_id()
            || revision.object_identity().value() != fact.object_id.as_bytes()
            || fact.paths.len() != request.len()
        {
            return Err(invalid(
                "MV source facts differ from the original query binding",
            ));
        }
        let format =
            DataVersionFormat::from_issued_fact(revision.data_version()).map_err(codec_failure)?;
        let (object_id, _) = persist_exact_connector_revision(&revision)
            .map_err(|error| provider(error, "persist exact MV source revision"))?;
        relations.push(
            bind_relation_fields(
                relation,
                &schema,
                Bytes::copy_from_slice(object_id.as_bytes()),
            )
            .map_err(invalid)?,
        );
        if fact.object_kind == MvSourceObjectKind::MaterializedView {
            dependencies.push(nested_dependency(upstream, &fact, &schema, &object_id)?);
        }
        let mut paths = fact
            .paths
            .into_iter()
            .zip(request)
            .map(|(path, sql_path)| {
                if path.fields.len() != sql_path.fields.len() {
                    return Err(invalid(
                        "MV provider paths do not cover the analyzed field coordinates",
                    ));
                }
                let nodes = path
                    .fields
                    .into_iter()
                    .zip(sql_path.fields)
                    .map(|(node, sql_field)| {
                        Ok(BoundSourceNode {
                            field_id: FieldIdentity::try_new(node.provider_field_id.to_vec())
                                .map_err(|e| invalid(e.to_string()))?,
                            name_at_binding: sql_field.name,
                            required: node.required,
                            kind: match node.kind {
                                MvSourceFieldKind::Scalar => SourceNodeKind::Scalar,
                                MvSourceFieldKind::Struct => SourceNodeKind::Struct,
                                MvSourceFieldKind::List => SourceNodeKind::List,
                                MvSourceFieldKind::Map => SourceNodeKind::Map,
                            },
                        })
                    })
                    .collect::<Result<_, MvApplicationError>>()?;
                Ok(BoundSourcePath {
                    nodes,
                    leaf_type: path.leaf_type,
                    whole_subtree: path.whole_subtree,
                })
            })
            .collect::<Result<Vec<_>, MvApplicationError>>()?;
        paths.sort_by(|left, right| {
            left.nodes
                .iter()
                .map(|node| &node.field_id)
                .cmp(right.nodes.iter().map(|node| &node.field_id))
        });
        occurrences.push(BindingOccurrence {
            occurrence_id,
            catalog_at_binding: metadata.identity.instance_id.as_str().to_owned(),
            namespace_at_binding: metadata.identity.namespace.to_string(),
            relation_at_binding: metadata.identity.table.to_string(),
            qualifier_at_binding: relation.qualifier().to_owned(),
            selection: ReadSelection::Current,
            object_id,
            data_version: format,
            facts: SourceBindingFacts {
                occurrence: relation.canonical_bytes().into(),
                object: fact.object_fact.canonical_bytes().clone(),
                field_paths: fact.path_fact.canonical_bytes().clone(),
                field_types: fact.type_fact.canonical_bytes().clone(),
                read_declarations: fact.read_declarations_fact.canonical_bytes().clone(),
            },
            paths,
        });
        let identity = format!(
            "{}.{}.{}",
            metadata.identity.instance_id.as_str(),
            metadata.identity.namespace,
            metadata.identity.table
        );
        if let std::collections::btree_map::Entry::Vacant(entry) = schemas.entry(identity) {
            entry.insert(
                storage::source_schema_from_spi(schema, &context)
                    .map_err(|error| provider(error, "project MV base schema"))?,
            );
        }
        if revisions.insert(occurrence_id, revision).is_some() {
            return Err(invalid("MV analysis repeats a source occurrence identity"));
        }
    }
    dependencies.sort_by(|left, right| left.canonical_bytes().cmp(right.canonical_bytes()));
    dependencies.dedup_by(|left, right| left.canonical_bytes() == right.canonical_bytes());
    let outputs = build_binding_outputs(sql_facts, &relations).map_err(invalid)?;
    let mut direct_output_sources = BTreeMap::new();
    for (sql, output) in sql_facts.outputs().iter().zip(&outputs) {
        if let (Some(direct), [provider]) = (sql.direct_source(), output.source_fields.as_slice()) {
            if direct.occurrence_id().get() != provider.occurrence_id {
                return Err(invalid(
                    "MV direct field belongs to another source occurrence",
                ));
            }
            direct_output_sources.insert(output.identity(), provider.clone());
        }
    }
    let replacement_output_sources = sql_facts
        .partition_replacement_sources()
        .map(|branches| {
            branches
                .iter()
                .map(|(occurrence, direct)| {
                    let occurrence = occurrence.get();
                    let fields = direct
                        .iter()
                        .zip(&outputs)
                        .filter(|(is_direct, _)| **is_direct)
                        .map(|(_, output)| {
                            let mut providers = output
                                .source_fields
                                .iter()
                                .filter(|field| field.occurrence_id == occurrence);
                            match (providers.next(), providers.next()) {
                                (Some(field), None) => Ok((output.identity(), field.clone())),
                                _ => Err(invalid(
                                    "MV replacement output lacks one exact source field",
                                )),
                            }
                        })
                        .collect::<Result<_, _>>()?;
                    Ok((occurrence, fields))
                })
                .collect::<Result<_, _>>()
        })
        .transpose()?;
    Ok(MvObservedBinding {
        relations,
        document: BindingDocument {
            occurrences,
            outputs,
            dependencies,
        },
        revisions,
        direct_output_sources,
        replacement_output_sources,
        schemas,
    })
}

fn nested_dependency(
    candidates: &[LoadedKeyedMvProjection],
    fact: &novarocks_spi::connector::MvSourceBindingObservation,
    schema: &MvSourceSchemaObservation,
    object_id: &ObjectIdentity,
) -> Result<BindingDependency, MvApplicationError> {
    let matches = candidates
        .iter()
        .filter(|candidate| {
            let source = candidate.projection.facts.source_revision();
            source.target.instance_id == fact.catalog_instance
                && source.target_object_id == fact.object_id
                && candidate.projection.facts.metadata_version().snapshot_id()
                    == schema.selected_snapshot_id()
        })
        .collect::<Vec<_>>();
    let [upstream] = matches.as_slice() else {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::Unavailable,
            "An observed upstream materialized view has no unambiguous current definition",
        ));
    };
    let definition_revision = upstream
        .projection
        .facts
        .source_revision()
        .definition_revision;
    let mut canonical = CanonicalFactWriter::new("novarocks.mv.nested-mv-binding", 1);
    canonical.text(fact.catalog_instance.as_str());
    canonical.bytes(object_id.as_bytes());
    canonical.bytes(definition_revision.as_bytes());
    canonical.text("trino");
    Ok(BindingDependency::NestedMv {
        object_id: object_id.clone(),
        definition_revision,
        dialect: QueryDialect::Trino,
        canonical: canonical.finish().into(),
    })
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
                source_type: field.source_type().clone(),
                nullable: field.nullable(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MvCreateRelationObservation {
        occurrence_id: relation.occurrence_id().get(),
        provider_object_id,
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
            novarocks_spi::connector::MvSourceFormatVersion::V3,
            Some(1),
            true,
            vec![
                MvObservedSourceField::try_new(
                    Bytes::from_static(b"field-1"),
                    "id".to_string(),
                    novarocks_mv_application::persistence::test_support::fixture_type("long"),
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
        let error = novarocks_mv_application::persistence::codec::keyed::decode_binding(&[0xff])
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
